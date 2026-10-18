//! The multi-topic facade the sink drives in place of a [`Producer`]: records carry their topic,
//! and the facade prepares, attempts, and settles per topic, with one verdict across them
//! (`docs/adr/kafka-otlp-formats.md`, items 3 and 4).
//!
//! Inside, each topic has its own [`Producer`], opened on the topic's first record from the
//! [`ProducerConfig`] template, so a topic no record names is never asked about. Two records
//! naming one topic share its producer. Its API is the one a producer holding several topics
//! under one identity would expose, so replacing the internals with one changes nothing above
//! this module; nothing outside it constructs a [`Producer`].
//!
//! An attempt runs every topic's share concurrently, each preparing first when its last
//! `prepare` failed, so one topic's metadata wait doesn't stack on another's. [`verdict`] reads
//! the topics' partition states, never a verdict per topic: `Rejected` and `Clean` rank alike in
//! a per-topic verdict, so merging them would drop a batch another topic still has unsettled.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use anyhow::{anyhow, Context};
use logit_core::Telemetry;
use logit_pipeline::Fault;

use super::batch::ProducerRecord;
use super::producer::{PartState, Pending, Producer, ProducerConfig, Settled};

/// One attempt across every topic of a [`MultiPending`].
#[derive(Debug)]
pub(crate) struct Attempt {
    /// [`verdict`]'s reading of every topic.
    pub(crate) result: anyhow::Result<()>,
    /// Every topic's settlements, for the sink to count.
    pub(crate) settled: Vec<Settled>,
    /// Records the topics prepared on this attempt dropped as over `max_request_bytes`.
    pub(crate) oversize: usize,
}

/// One `logit` batch's records across their topics, kept by the sink until the batch is final.
#[derive(Default)]
pub(crate) struct MultiPending {
    topics: Vec<TopicPending>,
}

struct TopicPending {
    topic: String,
    state: TopicState,
}

enum TopicState {
    /// Not yet partitioned: no attempt has prepared the topic.
    Unprepared(Vec<ProducerRecord>),
    Prepared(Pending),
}

impl MultiPending {
    /// Groups `records` by topic, each topic's in their order.
    pub(crate) fn new<'t>(records: impl IntoIterator<Item = (&'t str, ProducerRecord)>) -> Self {
        let mut topics: Vec<TopicPending> = Vec::new();
        for (topic, record) in records {
            match topics.iter_mut().find(|t| t.topic == topic) {
                Some(TopicPending { state: TopicState::Unprepared(records), .. }) => {
                    records.push(record)
                }
                _ => topics.push(TopicPending {
                    topic: topic.to_string(),
                    state: TopicState::Unprepared(vec![record]),
                }),
            }
        }
        Self { topics }
    }

    /// Records not settled: every record of a topic not yet prepared, and those of each
    /// unsettled partition batch.
    pub(crate) fn unsettled_records(&self) -> usize {
        self.topics
            .iter()
            .map(|t| match &t.state {
                TopicState::Unprepared(records) => records.len(),
                TopicState::Prepared(pending) => pending
                    .parts
                    .iter()
                    .filter(|p| matches!(p.state, PartState::Unsettled { .. }))
                    .map(|p| p.records.len())
                    .sum(),
            })
            .sum()
    }

    /// Each topic's prepared [`Pending`], for tests to inspect.
    #[cfg(test)]
    pub(crate) fn prepared(&self, topic: &str) -> Option<&Pending> {
        self.topics.iter().find(|t| t.topic == topic).and_then(|t| match &t.state {
            TopicState::Prepared(pending) => Some(pending),
            TopicState::Unprepared(_) => None,
        })
    }
}

/// A producer per topic, opened lazily from one template.
pub(crate) struct MultiProducer {
    template: ProducerConfig,
    producers: HashMap<String, Producer>,
}

impl MultiProducer {
    /// Fails as [`Producer::new`] fails on the template: a broker entry that isn't a
    /// `host:port`, no broker, or idempotence without `acks: all`.
    pub(crate) fn new(template: ProducerConfig) -> anyhow::Result<Self> {
        Producer::new(template.clone())?;
        Ok(Self { template, producers: HashMap::new() })
    }

    pub(crate) fn template(&self) -> &ProducerConfig {
        &self.template
    }

    /// Replaces the template's TLS and drops every producer: the next record for each topic
    /// opens it again under the new template.
    pub(crate) fn set_tls(&mut self, tls: Option<Arc<rustls::ClientConfig>>) {
        self.template.tls = tls;
        self.producers.clear();
    }

    /// Replaces the template's telemetry handle, and each open producer's.
    pub(crate) fn set_telemetry(&mut self, telemetry: &Telemetry) {
        self.template.telemetry = telemetry.clone();
        for producer in self.producers.values_mut() {
            producer.set_telemetry(telemetry);
        }
    }

    /// The topics a producer has been opened for, in name order.
    #[cfg(test)]
    pub(crate) fn opened(&self) -> Vec<String> {
        let mut topics: Vec<String> = self.producers.keys().cloned().collect();
        topics.sort();
        topics
    }

    /// Sends every unsettled partition batch of `pending` once, preparing each topic not yet
    /// prepared first, with every topic's share running concurrently.
    pub(crate) async fn attempt(&mut self, pending: &mut MultiPending) -> Attempt {
        for t in &pending.topics {
            if !self.producers.contains_key(&t.topic) {
                let config = ProducerConfig { topic: t.topic.clone(), ..self.template.clone() };
                // The template passed `Producer::new` in `MultiProducer::new`, and the topic
                // isn't among what it checks.
                let producer = Producer::new(config).expect("the template is a valid config");
                self.producers.insert(t.topic.clone(), producer);
            }
        }
        let mut producers: HashMap<&str, &mut Producer> =
            self.producers.iter_mut().map(|(topic, p)| (topic.as_str(), p)).collect();
        let jobs: Vec<_> = pending
            .topics
            .iter_mut()
            .map(|t| {
                let producer = producers.remove(t.topic.as_str()).expect("opened above");
                attempt_topic(producer, t)
            })
            .collect();
        let outcomes = join_all(jobs).await;

        let mut settled = Vec::new();
        let mut oversize = 0;
        let mut views = Vec::with_capacity(outcomes.len());
        for (t, outcome) in pending.topics.iter().zip(outcomes) {
            settled.extend(outcome.settled);
            oversize += outcome.oversize;
            let prepared = match &t.state {
                TopicState::Prepared(pending) => Some(pending),
                TopicState::Unprepared(_) => None,
            };
            views.push((prepared, outcome.error));
        }
        let result = verdict(&mut views);
        Attempt { result, settled, oversize }
    }

    /// Settles each partition batch, on every topic, whose last attempt was `Ambiguous` as
    /// dropped ([`Producer::drop_ambiguous`]), and returns how many records they held.
    pub(crate) fn drop_ambiguous(&mut self, pending: &mut MultiPending) -> usize {
        let mut dropped = 0;
        for t in &mut pending.topics {
            if let (TopicState::Prepared(p), Some(producer)) =
                (&mut t.state, self.producers.get_mut(&t.topic))
            {
                dropped += producer.drop_ambiguous(p);
            }
        }
        dropped
    }

    /// Marks, on each topic's producer, the sequence gap `pending` leaves when the sink drops it
    /// ([`Producer::abandon`]).
    pub(crate) fn abandon(&mut self, pending: &MultiPending) {
        for t in &pending.topics {
            if let (TopicState::Prepared(p), Some(producer)) =
                (&t.state, self.producers.get_mut(&t.topic))
            {
                producer.abandon(p);
            }
        }
    }
}

/// One topic's share of an attempt.
struct TopicOutcome {
    /// Why the topic didn't prepare, or why its attempt stopped before any partition answered.
    error: Option<anyhow::Error>,
    settled: Vec<Settled>,
    oversize: usize,
}

async fn attempt_topic(producer: &mut Producer, t: &mut TopicPending) -> TopicOutcome {
    let mut oversize = 0;
    if let TopicState::Unprepared(records) = &mut t.state {
        match producer.prepare(records).await {
            Ok(prepared) => {
                oversize = prepared.oversize;
                t.state = TopicState::Prepared(prepared);
            }
            Err(err) => return TopicOutcome { error: Some(err), settled: Vec::new(), oversize },
        }
    }
    let TopicState::Prepared(pending) = &mut t.state else { unreachable!("prepared above") };
    let sent = producer.send(pending).await;
    TopicOutcome { error: sent.error, settled: sent.settled, oversize }
}

/// The verdict on an attempt across topics, each given as its prepared [`Pending`], if it has
/// one, and the error that stopped its attempt, if one did. A topic with an error counts as one
/// unsettled entry of that error's class, whatever its partition batches hold; a topic without
/// counts each unsettled partition batch.
///
/// - With anything unsettled, the strongest class among it: `Refused` over `Ambiguous` over
///   `Clean`, the topic's own error when that's the strongest.
/// - With nothing unsettled, success when any partition batch of any topic was accepted, on this
///   attempt or an earlier one; otherwise `Rejected` with the first rejection, or success when
///   nothing was rejected either.
pub(crate) fn verdict(
    topics: &mut [(Option<&Pending>, Option<anyhow::Error>)],
) -> anyhow::Result<()> {
    let rank = |f: Fault| match f {
        Fault::Refused => 3,
        Fault::Ambiguous => 2,
        _ => 1,
    };
    enum Strongest {
        Topic(usize),
        Part(Fault, String),
    }
    let mut strongest: Option<(u8, Strongest)> = None;
    let (mut unsettled, mut parts) = (0, 0);
    for (i, (pending, error)) in topics.iter().enumerate() {
        if let Some(error) = error {
            let r = rank(logit_pipeline::classify(error));
            // Of equals, the last, as `Iterator::max_by_key` picks.
            if strongest.as_ref().is_none_or(|(best, _)| r >= *best) {
                strongest = Some((r, Strongest::Topic(i)));
            }
            continue;
        }
        let Some(pending) = pending else { continue };
        parts += pending.parts.len();
        for part in &pending.parts {
            if let PartState::Unsettled { fault, why } = &part.state {
                unsettled += 1;
                let r = rank(*fault);
                if strongest.as_ref().is_none_or(|(best, _)| r >= *best) {
                    strongest = Some((r, Strongest::Part(*fault, why.clone())));
                }
            }
        }
    }
    match strongest {
        Some((_, Strongest::Topic(i))) => {
            return Err(topics[i].1.take().expect("a topic error was ranked"))
        }
        Some((_, Strongest::Part(fault, why))) => {
            return Err(anyhow!("{unsettled} of {parts} Kafka partition batches unsettled: {why}"))
                .context(fault)
        }
        None => {}
    }
    if topics.iter().any(|(pending, _)| pending.is_some_and(|p| p.accepted)) {
        return Ok(());
    }
    match topics.iter().find_map(|(pending, _)| pending.and_then(|p| p.first_rejection.as_ref())) {
        Some(why) => Err(anyhow!("Kafka rejected the batch: {why}")).context(Fault::Rejected),
        None => Ok(()),
    }
}

/// Polls every future in `futures` on the caller's task until all finish, and returns their
/// outputs in order. Each borrows its own topic's producer and records, which a spawned task
/// can't, and a cancelled attempt leaves both where they were.
async fn join_all<F: Future>(futures: Vec<F>) -> Vec<F::Output> {
    let mut futures: Vec<Pin<Box<F>>> = futures.into_iter().map(Box::pin).collect();
    let mut outputs: Vec<Option<F::Output>> = futures.iter().map(|_| None).collect();
    std::future::poll_fn(|cx| {
        let mut done = true;
        for (future, output) in futures.iter_mut().zip(outputs.iter_mut()) {
            if output.is_none() {
                match future.as_mut().poll(cx) {
                    Poll::Ready(value) => *output = Some(value),
                    Poll::Pending => done = false,
                }
            }
        }
        if done {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    outputs.into_iter().map(|o| o.expect("every future finished")).collect()
}
