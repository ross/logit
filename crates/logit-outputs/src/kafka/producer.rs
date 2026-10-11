//! The producer: cluster metadata, the idempotent producer's identity and sequences, and one
//! attempt at a set of partition batches with a verdict per partition.
//!
//! A sink drives it in two calls per `logit` batch. [`Producer::prepare`] partitions the records
//! once and cuts each partition's share into partition batches under `max_request_bytes`, into a
//! [`Pending`] the sink keeps until the batch is final. [`Producer::attempt`] sends every
//! unsettled partition batch of a [`Pending`] once and returns the send's verdict
//! (`docs/adr/kafka-output.md`, item 4). Keeping the [`Pending`] across attempts is what keeps
//! each unkeyed record on its first partition and each partition batch on its first sequence.
//!
//! What an attempt does, in order:
//!
//! 1. With idempotence on, gets a producer id (PID) from `InitProducerId` when it has none or the
//!    broker fenced its epoch, and bumps the epoch on the client when an earlier attempt left a
//!    sequence gap: the same PID at epoch + 1, every partition's sequence back to 0, and
//!    `InitProducerId` again only at `i16::MAX`. A new epoch renumbers every unsettled partition
//!    batch; within one epoch a partition batch keeps its base sequence across attempts.
//! 2. Refreshes metadata when it's older than [`METADATA_MAX_AGE`] or a response asked for it.
//! 3. Sends each leader its partition batches, one produce request at a time per broker, with
//!    the brokers in parallel. A request holds at most one batch per partition, because a broker
//!    takes one record batch per partition per request. Once a partition's batch fails, its later
//!    batches in the same attempt aren't sent: they'd land behind a gap.
//! 4. Reads each partition's answer through [`super::fault::classify`].
//!
//! A sequence gap follows every partition batch settled without being appended: a rejection,
//! the named-record drop, the `MESSAGE_TOO_LARGE` split, and a [`Pending`] the sink abandons
//! ([`Producer::abandon`]) with a numbered partition batch unsettled. Each sets the bump for the
//! next attempt.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use bytes::Bytes;
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{
    InitProducerIdRequest, MetadataRequest, MetadataResponse, ProduceRequest, ProducerId, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::Compression;
use logit_pipeline::Fault;
use tokio::task::JoinSet;
use tokio::time::Instant;

use super::batch::{self, ProducerRecord, Stamp, BATCH_OVERHEAD};
use super::conn::{Conn, Dialer, PRODUCE_RECORD_ERRORS_MIN, PRODUCE_ZSTD_MIN};
use super::fault::{self, code, Response};
use super::partitioner;

/// Java's `metadata.max.age.ms`.
pub(crate) const METADATA_MAX_AGE: Duration = Duration::from_secs(300);

/// The transaction timeout an idempotent producer's `InitProducerId` carries; the broker ignores
/// it without a transactional id. Java's `transaction.timeout.ms` default.
const TRANSACTION_TIMEOUT_MS: i32 = 60_000;

/// Which replicas must have a batch before the broker answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Acks {
    All,
    Leader,
    /// The broker never answers; a written request counts as accepted.
    None,
}

impl Acks {
    fn wire(self) -> i16 {
        match self {
            Acks::All => -1,
            Acks::Leader => 1,
            Acks::None => 0,
        }
    }
}

/// Everything a [`Producer`] is built from.
#[derive(Clone)]
pub(crate) struct ProducerConfig {
    /// `host:port` entries to bootstrap metadata from.
    pub(crate) brokers: Vec<String>,
    pub(crate) topic: String,
    pub(crate) acks: Acks,
    /// Requires [`Acks::All`].
    pub(crate) idempotent: bool,
    pub(crate) compression: Compression,
    /// Bounds every connect, handshake, write, and response wait.
    pub(crate) timeout: Duration,
    /// The cap on one record's uncompressed size estimate, one partition batch's, and one
    /// produce request's encoded batches.
    pub(crate) max_request_bytes: usize,
    pub(crate) tls: Option<Arc<rustls::ClientConfig>>,
    pub(crate) client_id: String,
}

/// A connection's key: a bootstrap entry by its index, or a broker by its node id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Node {
    Bootstrap(usize),
    Broker(i32),
}

/// The cached answer to the last `Metadata` request for the topic.
#[derive(Debug, Clone)]
pub(crate) struct Metadata {
    brokers: HashMap<i32, (String, u16)>,
    /// Each partition's leader, by partition index; `-1` for none.
    leaders: Vec<i32>,
    fetched: Instant,
}

impl Metadata {
    fn leader(&self, partition: i32) -> Option<i32> {
        let leader = *self.leaders.get(usize::try_from(partition).ok()?)?;
        (leader >= 0 && self.brokers.contains_key(&leader)).then_some(leader)
    }

    fn led_partitions(&self) -> Vec<i32> {
        (0..self.leaders.len() as i32).filter(|&p| self.leader(p).is_some()).collect()
    }

    fn is_stale(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.fetched) >= METADATA_MAX_AGE
    }
}

/// The idempotent producer's identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Identity {
    pub(crate) producer_id: i64,
    pub(crate) epoch: i16,
}

/// Where a partition batch stands within its [`Pending`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PartState {
    /// Not yet settled; the fault and reason of its last attempt, `Clean` before any.
    Unsettled {
        fault: Fault,
        why: String,
    },
    Accepted,
    Rejected,
}

/// One partition batch: a partition's records, sent as one record batch.
#[derive(Debug, Clone)]
pub(crate) struct Part {
    pub(crate) partition: i32,
    pub(crate) records: Vec<ProducerRecord>,
    /// The generation and base sequence it was numbered under.
    sequence: Option<(u64, i32)>,
    pub(crate) state: PartState,
    /// A split half or a named-record resend: a second split or rejection settles it.
    resend: bool,
}

impl Part {
    fn new(partition: i32, records: Vec<ProducerRecord>, resend: bool) -> Self {
        let why = "not yet sent".to_string();
        Self {
            partition,
            records,
            sequence: None,
            state: PartState::Unsettled { fault: Fault::Clean, why },
            resend,
        }
    }

    fn is_unsettled(&self) -> bool {
        matches!(self.state, PartState::Unsettled { .. })
    }

    fn unsettle(&mut self, fault: Fault, why: String) {
        self.state = PartState::Unsettled { fault, why };
    }
}

/// One `logit` batch's records, partitioned and cut into partition batches, with what the broker
/// settled so far. The sink keeps it across attempts at the batch.
#[derive(Debug, Default)]
pub(crate) struct Pending {
    pub(crate) parts: Vec<Part>,
    /// Records over `max_request_bytes` on their own, dropped before any request.
    pub(crate) oversize: usize,
    accepted: bool,
    first_rejection: Option<String>,
}

/// What a partition batch settled as on one attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Settlement {
    /// Appended at `base_offset`, `-1` under `acks: none`.
    Accepted { base_offset: i64 },
    /// Dropped whole, with the broker's code.
    Rejected { code: i16 },
    /// The records `record_errors` named, dropped; the rest stay in the pending batch.
    Dropped { code: i16 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Settled {
    pub(crate) partition: i32,
    pub(crate) records: usize,
    pub(crate) settlement: Settlement,
}

/// One attempt's result: the send's verdict, and each settlement it reached, for the sink to
/// count.
#[derive(Debug)]
pub(crate) struct Attempt {
    /// `Ok` once no partition batch is unsettled and one was accepted, on this attempt or an
    /// earlier one. Otherwise the strongest unsettled class (`Refused`, `Ambiguous`, `Clean`),
    /// or, with every partition batch settled and none accepted, the first rejection as
    /// `Rejected`.
    pub(crate) result: anyhow::Result<()>,
    pub(crate) settled: Vec<Settled>,
}

/// One partition's answer, or why it has none.
#[derive(Debug)]
enum Outcome {
    Unanswered { fault: Fault, why: String },
    Answered { code: i16, base_offset: i64, record_errors: Vec<i32>, why: String, version: i16 },
}

/// A Kafka producer for one topic. Not `Clone`: its sequences are the broker's view of it.
pub(crate) struct Producer {
    config: ProducerConfig,
    dialer: Dialer,
    bootstrap: Vec<(String, u16)>,
    topic: StrBytes,
    conns: HashMap<Node, Conn>,
    metadata: Option<Metadata>,
    refresh_due: bool,
    identity: Option<Identity>,
    reinit_due: bool,
    bump_due: bool,
    /// Counts epochs, PIDs included: a partition batch numbered under an older generation is
    /// renumbered.
    generation: u64,
    next_sequence: HashMap<i32, i32>,
}

/// `seq + count` on the broker's sequence ring, which wraps from `i32::MAX` to 0
/// (`DefaultRecordBatch.incrementSequence`).
pub(crate) fn next_sequence(seq: i32, count: i32) -> i32 {
    if i32::MAX - seq < count {
        count - (i32::MAX - seq) - 1
    } else {
        seq + count
    }
}

/// Splits a bootstrap `host:port`; a bracketed IPv6 literal loses its brackets.
fn host_port(entry: &str) -> anyhow::Result<(String, u16)> {
    let (host, port) =
        entry.rsplit_once(':').ok_or_else(|| anyhow!("Kafka broker {entry:?} has no port"))?;
    let port = port.parse().with_context(|| format!("Kafka broker {entry:?} has no valid port"))?;
    let host = host.trim_start_matches('[').trim_end_matches(']');
    anyhow::ensure!(!host.is_empty(), "Kafka broker {entry:?} has no host");
    Ok((host.to_string(), port))
}

impl Producer {
    pub(crate) fn new(config: ProducerConfig) -> anyhow::Result<Self> {
        anyhow::ensure!(!config.brokers.is_empty(), "no Kafka broker to bootstrap from");
        anyhow::ensure!(
            !config.idempotent || config.acks == Acks::All,
            "an idempotent producer needs acks: all"
        );
        let bootstrap = config.brokers.iter().map(|b| host_port(b)).collect::<Result<_, _>>()?;
        let dialer = Dialer {
            tls: config.tls.clone(),
            timeout: config.timeout,
            client_id: StrBytes::from_string(config.client_id.clone()),
        };
        Ok(Self {
            topic: StrBytes::from_string(config.topic.clone()),
            config,
            dialer,
            bootstrap,
            conns: HashMap::new(),
            metadata: None,
            refresh_due: false,
            identity: None,
            reinit_due: false,
            bump_due: false,
            generation: 0,
            next_sequence: HashMap::new(),
        })
    }

    /// Sets the identity as if `InitProducerId` had answered it, with a bump pending when `bump`.
    #[cfg(test)]
    pub(crate) fn force_identity(&mut self, identity: Identity, bump: bool) {
        self.identity = Some(identity);
        self.restart_sequences();
        self.bump_due = bump;
    }

    /// The current identity, once `InitProducerId` answered.
    pub(crate) fn identity(&self) -> Option<Identity> {
        self.identity
    }

    /// Partitions `records` and cuts each partition's share into partition batches whose size
    /// estimate stays under `max_request_bytes`. A record whose own estimate is over it is
    /// dropped and counted in [`Pending::oversize`]. Fails, with nothing partitioned, when
    /// metadata can't be had (`Clean`), the topic is refused (`Refused`), or unkeyed records have
    /// no led partition to go to (`Clean`).
    pub(crate) async fn prepare(
        &mut self,
        records: Vec<ProducerRecord>,
    ) -> anyhow::Result<Pending> {
        self.ensure_metadata().await?;
        let meta = self.metadata.as_ref().expect("ensure_metadata leaves metadata");
        let count = meta.leaders.len();
        let cap = self.config.max_request_bytes;
        let mut sticky = None;
        let mut by_partition: BTreeMap<i32, Vec<ProducerRecord>> = BTreeMap::new();
        let mut oversize = 0;
        for record in records {
            if BATCH_OVERHEAD + record.size_upper_bound() > cap {
                oversize += 1;
                continue;
            }
            let partition = match &record.key {
                Some(key) => partitioner::partition_for_key(key, count),
                None => match sticky {
                    Some(p) => p,
                    None => {
                        let Some(p) = partitioner::sticky_partition(&meta.led_partitions()) else {
                            self.refresh_due = true;
                            return Err(anyhow!(
                                "no partition of Kafka topic {:?} has a leader",
                                self.config.topic
                            ))
                            .context(Fault::Clean);
                        };
                        *sticky.insert(p)
                    }
                },
            };
            by_partition.entry(partition).or_default().push(record);
        }
        let mut parts = Vec::new();
        for (partition, records) in by_partition {
            let mut current = Vec::new();
            let mut size = BATCH_OVERHEAD;
            for record in records {
                let estimate = record.size_upper_bound();
                if !current.is_empty() && size + estimate > cap {
                    parts.push(Part::new(partition, std::mem::take(&mut current), false));
                    size = BATCH_OVERHEAD;
                }
                size += estimate;
                current.push(record);
            }
            if !current.is_empty() {
                parts.push(Part::new(partition, current, false));
            }
        }
        Ok(Pending { parts, oversize, ..Pending::default() })
    }

    /// Marks the sequence gap a [`Pending`] leaves when the sink drops it with a numbered
    /// partition batch unsettled (an `Ambiguous` attempt under `at_most_once`, or shutdown): the
    /// next attempt bumps the epoch.
    pub(crate) fn abandon(&mut self, pending: &Pending) {
        let numbered = |p: &Part| matches!(p.sequence, Some((g, _)) if g == self.generation);
        if self.config.idempotent && pending.parts.iter().any(|p| p.is_unsettled() && numbered(p)) {
            self.bump_due = true;
        }
    }

    /// Sends every unsettled partition batch of `pending` once.
    pub(crate) async fn attempt(&mut self, pending: &mut Pending) -> Attempt {
        let mut settled = Vec::new();
        let result = match self.send_unsettled(pending, &mut settled).await {
            Ok(()) => pending.verdict(),
            Err(err) => Err(err),
        };
        Attempt { result, settled }
    }

    async fn send_unsettled(
        &mut self,
        pending: &mut Pending,
        settled: &mut Vec<Settled>,
    ) -> anyhow::Result<()> {
        if !pending.parts.iter().any(Part::is_unsettled) {
            return Ok(());
        }
        if self.config.idempotent {
            self.ensure_identity().await?;
        }
        self.ensure_metadata().await?;
        let meta = self.metadata.clone().expect("ensure_metadata leaves metadata");

        let mut outcomes: Vec<Option<Outcome>> = pending.parts.iter().map(|_| None).collect();
        let mut jobs: BTreeMap<i32, Vec<(usize, i32, Bytes)>> = BTreeMap::new();
        for (i, part) in pending.parts.iter_mut().enumerate().filter(|(_, p)| p.is_unsettled()) {
            let stamp = self.stamp(part);
            let Some(leader) = meta.leader(part.partition) else {
                self.refresh_due = true;
                let why = format!("partition {} has no leader", part.partition);
                outcomes[i] = Some(Outcome::Unanswered { fault: Fault::Clean, why });
                continue;
            };
            match batch::encode(&part.records, stamp, self.config.compression) {
                Ok(bytes) => jobs.entry(leader).or_default().push((i, part.partition, bytes)),
                Err(err) => {
                    // A batch `kafka-protocol` can't encode is this batch's own content.
                    let why = format!("encoding partition {}: {err:#}", part.partition);
                    outcomes[i] = Some(Outcome::Answered {
                        code: -1,
                        base_offset: -1,
                        record_errors: vec![],
                        why,
                        version: 0,
                    });
                }
            }
        }

        let mut tasks = JoinSet::new();
        for (broker, parts) in jobs {
            let (host, port) = meta.brokers[&broker].clone();
            let job = BrokerJob {
                broker,
                host,
                port,
                conn: self.conns.remove(&Node::Broker(broker)),
                dialer: self.dialer.clone(),
                topic: self.topic.clone(),
                acks: self.config.acks,
                timeout_ms: i32::try_from(self.config.timeout.as_millis()).unwrap_or(i32::MAX),
                compression: self.config.compression,
                max_request_bytes: self.config.max_request_bytes,
                parts,
            };
            tasks.spawn(job.run());
        }
        while let Some(joined) = tasks.join_next().await {
            let (broker, conn, answers) = match joined {
                Ok(done) => done,
                Err(err) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
                Err(err) => {
                    return Err(anyhow!("a Kafka produce task ended: {err}")).context(Fault::Clean)
                }
            };
            if let Some(conn) = conn {
                self.conns.insert(Node::Broker(broker), conn);
            }
            for (i, outcome) in answers {
                outcomes[i] = Some(outcome);
            }
        }

        let parts = std::mem::take(&mut pending.parts);
        for (part, outcome) in parts.into_iter().zip(outcomes) {
            match outcome {
                None => pending.parts.push(part),
                Some(outcome) => self.apply(pending, part, outcome, settled),
            }
        }
        Ok(())
    }

    /// Folds one partition batch's outcome into `pending`, which it's pushed back onto, halved,
    /// or trimmed.
    fn apply(
        &mut self,
        pending: &mut Pending,
        mut part: Part,
        outcome: Outcome,
        settled: &mut Vec<Settled>,
    ) {
        let (code, base_offset, record_errors, why, version) = match outcome {
            Outcome::Unanswered { fault, why } => {
                part.unsettle(fault, why);
                pending.parts.push(part);
                return;
            }
            Outcome::Answered { code, base_offset, record_errors, why, version } => {
                (code, base_offset, record_errors, why, version)
            }
        };
        let partition = part.partition;
        match fault::classify(code) {
            Response::Accepted => {
                settled.push(Settled {
                    partition,
                    records: part.records.len(),
                    settlement: Settlement::Accepted { base_offset },
                });
                part.state = PartState::Accepted;
                pending.accepted = true;
            }
            Response::Unsettled { fault, refresh } => {
                self.refresh_due |= refresh;
                part.unsettle(fault, why);
            }
            Response::OutOfOrder => {
                self.bump_due = true;
                part.unsettle(Fault::Ambiguous, why);
            }
            Response::StaleEpoch => {
                self.reinit_due = true;
                part.unsettle(Fault::Clean, why);
            }
            Response::TooLarge if part.records.len() > 1 && !part.resend => {
                self.gap();
                let back = part.records.split_off(part.records.len() / 2);
                let halves = [part.records, back];
                for records in halves {
                    let mut half = Part::new(partition, records, true);
                    half.unsettle(Fault::Clean, format!("{why}; halved for a resend"));
                    pending.parts.push(half);
                }
                return;
            }
            Response::NamedRecords
                if version >= PRODUCE_RECORD_ERRORS_MIN
                    && !part.resend
                    && !record_errors.is_empty() =>
            {
                self.gap();
                let named: HashSet<usize> = record_errors
                    .iter()
                    .filter_map(|&i| usize::try_from(i).ok())
                    .filter(|&i| i < part.records.len())
                    .collect();
                if named.is_empty() {
                    return self.reject(pending, part, code, why, settled);
                }
                settled.push(Settled {
                    partition,
                    records: named.len(),
                    settlement: Settlement::Dropped { code },
                });
                let rest: Vec<ProducerRecord> = std::mem::take(&mut part.records)
                    .into_iter()
                    .enumerate()
                    .filter(|(i, _)| !named.contains(i))
                    .map(|(_, r)| r)
                    .collect();
                if rest.is_empty() {
                    pending.first_rejection.get_or_insert(why);
                    part.state = PartState::Rejected;
                } else {
                    part = Part::new(partition, rest, true);
                    part.unsettle(Fault::Clean, format!("{why}; resending the records not named"));
                }
            }
            Response::Rejected | Response::TooLarge | Response::NamedRecords => {
                return self.reject(pending, part, code, why, settled);
            }
        }
        pending.parts.push(part);
    }

    fn reject(
        &mut self,
        pending: &mut Pending,
        mut part: Part,
        code: i16,
        why: String,
        settled: &mut Vec<Settled>,
    ) {
        self.gap();
        settled.push(Settled {
            partition: part.partition,
            records: part.records.len(),
            settlement: Settlement::Rejected { code },
        });
        pending.first_rejection.get_or_insert(why);
        part.state = PartState::Rejected;
        pending.parts.push(part);
    }

    /// Records a sequence gap: the next attempt bumps the epoch.
    fn gap(&mut self) {
        if self.config.idempotent {
            self.bump_due = true;
        }
    }

    /// The stamp `part` goes out under, numbering it first when it has no sequence from the
    /// current generation.
    fn stamp(&mut self, part: &mut Part) -> Stamp {
        let Some(identity) = self.identity.filter(|_| self.config.idempotent) else {
            return Stamp::NONE;
        };
        let base = match part.sequence {
            Some((generation, base)) if generation == self.generation => base,
            _ => {
                let next = self.next_sequence.entry(part.partition).or_insert(0);
                let base = *next;
                *next = next_sequence(base, part.records.len() as i32);
                part.sequence = Some((self.generation, base));
                base
            }
        };
        Stamp {
            producer_id: identity.producer_id,
            producer_epoch: identity.epoch,
            base_sequence: base,
        }
    }

    async fn ensure_identity(&mut self) -> anyhow::Result<()> {
        match self.identity {
            None => self.init_producer_id().await,
            Some(_) if self.reinit_due => self.init_producer_id().await,
            Some(identity) if self.bump_due => {
                if identity.epoch == i16::MAX {
                    return self.init_producer_id().await;
                }
                self.identity = Some(Identity { epoch: identity.epoch + 1, ..identity });
                self.restart_sequences();
                Ok(())
            }
            Some(_) => Ok(()),
        }
    }

    fn restart_sequences(&mut self) {
        self.generation += 1;
        self.next_sequence.clear();
        self.bump_due = false;
        self.reinit_due = false;
    }

    async fn init_producer_id(&mut self) -> anyhow::Result<()> {
        let request = InitProducerIdRequest::default()
            .with_transactional_id(None)
            .with_transaction_timeout_ms(TRANSACTION_TIMEOUT_MS)
            .with_producer_id(ProducerId(-1))
            .with_producer_epoch(-1);
        let (node, mut conn) = self.control_conn().await?;
        let version = conn.versions().init_producer_id;
        let response = conn
            .request(&request, version)
            .await
            .map_err(|err| err.error.context("InitProducerId").context(Fault::Clean))?;
        self.conns.insert(node, conn);
        if response.error_code != code::NONE {
            let fault = fault::classify_control(response.error_code);
            return Err(anyhow!("InitProducerId answered {}", fault::name(response.error_code)))
                .context(fault);
        }
        self.identity =
            Some(Identity { producer_id: response.producer_id.0, epoch: response.producer_epoch });
        self.restart_sequences();
        Ok(())
    }

    async fn ensure_metadata(&mut self) -> anyhow::Result<()> {
        let fresh = self.metadata.as_ref().is_some_and(|m| !m.is_stale(Instant::now()));
        if fresh && !self.refresh_due {
            return Ok(());
        }
        self.refresh_metadata().await
    }

    /// Asks for the topic's metadata with `allow_auto_topic_creation` set, so the broker's
    /// `auto.create.topics.enable` decides whether a missing topic is created.
    async fn refresh_metadata(&mut self) -> anyhow::Result<()> {
        let topic = MetadataRequestTopic::default().with_name(Some(TopicName(self.topic.clone())));
        let request = MetadataRequest::default()
            .with_topics(Some(vec![topic]))
            .with_allow_auto_topic_creation(true);
        let (node, mut conn) = self.control_conn().await?;
        let version = conn.versions().metadata;
        let response = conn
            .request(&request, version)
            .await
            .map_err(|err| err.error.context("Metadata").context(Fault::Clean))?;
        self.conns.insert(node, conn);
        self.apply_metadata(response)
    }

    fn apply_metadata(&mut self, response: MetadataResponse) -> anyhow::Result<()> {
        let topic_name = &self.config.topic;
        let Some(topic) = response
            .topics
            .iter()
            .find(|t| t.name.as_ref().is_some_and(|n| n.0.as_str() == topic_name.as_str()))
        else {
            self.refresh_due = true;
            return Err(anyhow!("Metadata has no answer for Kafka topic {topic_name:?}"))
                .context(Fault::Clean);
        };
        if topic.error_code != code::NONE {
            self.refresh_due = true;
            let fault = fault::classify_control(topic.error_code);
            return Err(anyhow!(
                "Metadata for Kafka topic {topic_name:?} answered {}",
                fault::name(topic.error_code)
            ))
            .context(fault);
        }
        let brokers: HashMap<i32, (String, u16)> = response
            .brokers
            .iter()
            .filter_map(|b| {
                let port = u16::try_from(b.port).ok()?;
                Some((b.node_id.0, (b.host.to_string(), port)))
            })
            .collect();
        let count = topic
            .partitions
            .iter()
            .filter_map(|p| usize::try_from(p.partition_index).ok())
            .map(|i| i + 1)
            .max()
            .unwrap_or(0);
        let mut leaders = vec![-1; count];
        for p in &topic.partitions {
            if let Ok(i) = usize::try_from(p.partition_index) {
                leaders[i] = if p.error_code == code::NONE { p.leader_id.0 } else { -1 };
            }
        }
        if count == 0 {
            self.refresh_due = true;
            return Err(anyhow!("Kafka topic {topic_name:?} has no partitions"))
                .context(Fault::Clean);
        }
        self.metadata = Some(Metadata { brokers, leaders, fetched: Instant::now() });
        self.refresh_due = false;
        Ok(())
    }

    /// A connection for a request any broker answers: an open pooled one, else a fresh dial to
    /// each bootstrap entry in turn, then to each broker the last metadata named. Taken out of
    /// the pool; the caller puts it back once its request succeeds.
    async fn control_conn(&mut self) -> anyhow::Result<(Node, Conn)> {
        let pooled: Vec<Node> = self.conns.keys().copied().collect();
        for node in pooled {
            let mut conn = self.conns.remove(&node).expect("a pooled key");
            if conn.is_open().await {
                return Ok((node, conn));
            }
        }
        let mut targets: Vec<(Node, String, u16)> = self
            .bootstrap
            .iter()
            .enumerate()
            .map(|(i, (h, p))| (Node::Bootstrap(i), h.clone(), *p))
            .collect();
        if let Some(meta) = &self.metadata {
            let mut known: Vec<_> = meta.brokers.iter().collect();
            known.sort_by_key(|(id, _)| **id);
            targets
                .extend(known.into_iter().map(|(id, (h, p))| (Node::Broker(*id), h.clone(), *p)));
        }
        let mut last = None;
        for (node, host, port) in targets {
            match self.dialer.connect(&host, port).await {
                Ok(conn) => return Ok((node, conn)),
                // Every broker answers with the same versions; another can't do better.
                Err(err) if logit_pipeline::classify(&err) == Fault::Refused => return Err(err),
                Err(err) => last = Some(err),
            }
        }
        Err(last.expect("at least one bootstrap entry"))
    }
}

impl Pending {
    /// The send's verdict once no request is outstanding ([`Attempt::result`]).
    fn verdict(&self) -> anyhow::Result<()> {
        let rank = |f: Fault| match f {
            Fault::Refused => 3,
            Fault::Ambiguous => 2,
            _ => 1,
        };
        let unsettled: Vec<(Fault, &str)> = self
            .parts
            .iter()
            .filter_map(|p| match &p.state {
                PartState::Unsettled { fault, why } => Some((*fault, why.as_str())),
                _ => None,
            })
            .collect();
        if let Some(&(fault, why)) = unsettled.iter().max_by_key(|(f, _)| rank(*f)) {
            return Err(anyhow!(
                "{} of {} Kafka partition batches unsettled: {why}",
                unsettled.len(),
                self.parts.len()
            ))
            .context(fault);
        }
        match &self.first_rejection {
            Some(why) if !self.accepted => {
                Err(anyhow!("Kafka rejected the batch: {why}")).context(Fault::Rejected)
            }
            _ => Ok(()),
        }
    }
}

/// One broker's share of an attempt, run on its own task so brokers proceed in parallel.
struct BrokerJob {
    broker: i32,
    host: String,
    port: u16,
    conn: Option<Conn>,
    dialer: Dialer,
    topic: StrBytes,
    acks: Acks,
    timeout_ms: i32,
    compression: Compression,
    max_request_bytes: usize,
    /// `(index in the pending batch, partition, encoded batch)`, in send order.
    parts: Vec<(usize, i32, Bytes)>,
}

impl BrokerJob {
    async fn run(self) -> (i32, Option<Conn>, Vec<(usize, Outcome)>) {
        let BrokerJob {
            broker,
            host,
            port,
            conn,
            dialer,
            topic,
            acks,
            timeout_ms,
            compression,
            max_request_bytes,
            parts,
        } = self;
        let params = ProduceParams { topic, acks, timeout_ms };
        let mut answers = Vec::with_capacity(parts.len());
        let mut reused = conn;
        if let Some(conn) = &mut reused {
            if !conn.is_open().await {
                reused = None;
            }
        }
        let mut conn = match reused {
            Some(conn) => conn,
            None => match dialer.connect(&host, port).await {
                Ok(conn) => conn,
                Err(err) => {
                    let fault = logit_pipeline::classify(&err);
                    let why = format!("{err:#}");
                    for (i, _, _) in parts {
                        answers.push((i, Outcome::Unanswered { fault, why: why.clone() }));
                    }
                    return (broker, None, answers);
                }
            },
        };
        let version = conn.versions().produce;
        if compression == Compression::Zstd && version < PRODUCE_ZSTD_MIN {
            let why = format!(
                "Kafka broker {} offers Produce up to v{version}; zstd needs v{PRODUCE_ZSTD_MIN}",
                broker
            );
            for (i, _, _) in parts {
                answers.push((i, Outcome::Unanswered { fault: Fault::Refused, why: why.clone() }));
            }
            return (broker, Some(conn), answers);
        }

        let mut failed: HashSet<i32> = HashSet::new();
        let mut alive = true;
        for request in pack(parts, max_request_bytes) {
            let (sendable, behind): (Vec<_>, Vec<_>) =
                request.into_iter().partition(|(_, p, _)| alive && !failed.contains(p));
            for (i, partition, _) in behind {
                let why = if alive {
                    format!("not sent behind an earlier batch of partition {partition} that failed")
                } else {
                    "not sent: an earlier request on the connection failed".to_string()
                };
                failed.insert(partition);
                answers.push((i, Outcome::Unanswered { fault: Fault::Clean, why }));
            }
            if sendable.is_empty() {
                continue;
            }
            match params.produce(&mut conn, &sendable, version).await {
                Ok(outcomes) => {
                    for ((i, partition, _), outcome) in sendable.iter().zip(outcomes) {
                        if !matches!(outcome, Outcome::Answered { code: 0 | 46, .. }) {
                            failed.insert(*partition);
                        }
                        answers.push((*i, outcome));
                    }
                }
                Err(err) => {
                    alive = false;
                    let fault = err.fault();
                    let why = format!("{:#}", err.error);
                    for (i, partition, _) in sendable {
                        failed.insert(partition);
                        answers.push((i, Outcome::Unanswered { fault, why: why.clone() }));
                    }
                }
            }
        }
        (broker, alive.then_some(conn), answers)
    }
}

/// What every produce request of one broker job carries.
struct ProduceParams {
    topic: StrBytes,
    acks: Acks,
    timeout_ms: i32,
}

impl ProduceParams {
    /// Sends one produce request and reads each partition's answer, in `parts` order.
    async fn produce(
        &self,
        conn: &mut Conn,
        parts: &[(usize, i32, Bytes)],
        version: i16,
    ) -> Result<Vec<Outcome>, super::conn::RequestError> {
        let data = parts
            .iter()
            .map(|(_, p, bytes)| {
                PartitionProduceData::default().with_index(*p).with_records(Some(bytes.clone()))
            })
            .collect();
        let topic = TopicProduceData::default()
            .with_name(TopicName(self.topic.clone()))
            .with_partition_data(data);
        let request = ProduceRequest::default()
            .with_transactional_id(None)
            .with_acks(self.acks.wire())
            .with_timeout_ms(self.timeout_ms)
            .with_topic_data(vec![topic]);
        if self.acks == Acks::None {
            conn.send_only(&request, version).await?;
            let accepted = || Outcome::Answered {
                code: code::NONE,
                base_offset: -1,
                record_errors: vec![],
                why: String::new(),
                version,
            };
            return Ok(parts.iter().map(|_| accepted()).collect());
        }
        let response = conn.request(&request, version).await?;
        let mut answers: HashMap<i32, Outcome> = HashMap::new();
        for topic in response.responses.iter().filter(|t| t.name.0 == self.topic) {
            for p in &topic.partition_responses {
                let why = match &p.error_message {
                    Some(message) => format!(
                        "partition {} answered {}: {message}",
                        p.index,
                        fault::name(p.error_code)
                    ),
                    None => format!("partition {} answered {}", p.index, fault::name(p.error_code)),
                };
                answers.insert(
                    p.index,
                    Outcome::Answered {
                        code: p.error_code,
                        base_offset: p.base_offset,
                        record_errors: p.record_errors.iter().map(|e| e.batch_index).collect(),
                        why,
                        version,
                    },
                );
            }
        }
        Ok(parts
            .iter()
            .map(|(_, p, _)| {
                answers.remove(p).unwrap_or_else(|| Outcome::Unanswered {
                    fault: Fault::Ambiguous,
                    why: format!("the produce response has no answer for partition {p}"),
                })
            })
            .collect())
    }
}

/// Cuts `parts` into produce requests in order: at most one batch per partition per request,
/// and encoded batches summing to at most `cap`, except a lone batch over it.
fn pack(parts: Vec<(usize, i32, Bytes)>, cap: usize) -> Vec<Vec<(usize, i32, Bytes)>> {
    let mut requests: Vec<Vec<(usize, i32, Bytes)>> = Vec::new();
    let mut size = 0;
    for part in parts {
        let starts_new = match requests.last() {
            None => true,
            Some(current) => {
                current.iter().any(|(_, p, _)| *p == part.1) || size + part.2.len() > cap
            }
        };
        if starts_new {
            requests.push(Vec::new());
            size = 0;
        }
        size += part.2.len();
        requests.last_mut().expect("pushed above").push(part);
    }
    requests
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequences_wrap_from_i32_max_to_zero() {
        assert_eq!(next_sequence(0, 3), 3);
        assert_eq!(next_sequence(i32::MAX - 3, 3), i32::MAX);
        assert_eq!(next_sequence(i32::MAX - 2, 3), 0);
        assert_eq!(next_sequence(i32::MAX, 1), 0);
        assert_eq!(next_sequence(i32::MAX - 1, 5), 3);
    }

    #[test]
    fn metadata_is_stale_after_five_minutes() {
        let fetched = Instant::now();
        let meta = Metadata { brokers: HashMap::new(), leaders: vec![], fetched };
        assert!(!meta.is_stale(fetched + METADATA_MAX_AGE - Duration::from_millis(1)));
        assert!(meta.is_stale(fetched + METADATA_MAX_AGE));
    }

    #[test]
    fn a_partition_without_a_known_leader_has_none() {
        let meta = Metadata {
            brokers: HashMap::from([(1, ("b1".to_string(), 9092))]),
            leaders: vec![1, -1, 7],
            fetched: Instant::now(),
        };
        assert_eq!(meta.leader(0), Some(1));
        assert_eq!(meta.leader(1), None);
        assert_eq!(meta.leader(2), None, "a leader the broker list doesn't name");
        assert_eq!(meta.leader(3), None);
        assert_eq!(meta.led_partitions(), vec![0]);
    }

    #[test]
    fn pack_keeps_one_batch_per_partition_and_the_size_cap() {
        let part = |i: usize, p: i32, n: usize| (i, p, Bytes::from(vec![0u8; n]));
        let requests = pack(
            vec![part(0, 0, 40), part(1, 1, 40), part(2, 0, 10), part(3, 2, 30), part(4, 3, 200)],
            100,
        );
        let shape: Vec<Vec<usize>> =
            requests.iter().map(|r| r.iter().map(|(i, _, _)| *i).collect()).collect();
        assert_eq!(shape, vec![vec![0, 1], vec![2, 3], vec![4]]);
    }

    #[test]
    fn bootstrap_entries_split_into_host_and_port() {
        assert_eq!(host_port("kafka:9092").unwrap(), ("kafka".to_string(), 9092));
        assert_eq!(host_port("[::1]:9093").unwrap(), ("::1".to_string(), 9093));
        assert!(host_port("kafka").is_err());
        assert!(host_port(":9092").is_err());
        assert!(host_port("kafka:x").is_err());
    }
}
