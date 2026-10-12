//! `kafka_out`: the `Output` that drives [`Producer`], with the records [`KafkaEncoder`] writes:
//! one per event, or under `record: batch` as many events as fit in each.
//!
//! One `logit` batch becomes one [`Pending`]: [`KafkaEncoder`] writes every record, and
//! [`Producer::prepare`] partitions them and cuts the partition batches once. The sink keeps that
//! `Pending` across every attempt `write_loop` makes at the batch, so a retry resends only the
//! partition batches the broker hasn't settled, with each unkeyed record on its first partition
//! and each partition batch under its first sequence (`docs/adr/kafka-output.md`, "Amendment:
//! what W1 settled", item 1). It lets go of the `Pending` when a send is final (`Ok`, or a fault
//! `write_loop` won't retry under the posture), and calls [`Producer::abandon`] on one dropped
//! with a partition batch unsettled: when the send was final, or when `Output::observe_batch`
//! starts another batch first, as after a shutdown cut. A batch that encodes to no record (every
//! event skipped or dropped) succeeds without a request. A caller that never calls
//! `observe_batch` gets a fresh `Pending` on every `send`. Under `at_most_once`, a `Refused`
//! send first drops each partition batch left `Ambiguous`, so the retry doesn't resend it
//! (`docs/adr/kafka-output.md`, "Amendment: what W2 settled", item 5).
//!
//! How each response is classified is the module doc's table (`super`, "Response classes").
//!
//! ## Telemetry
//!
//! | Point | Meaning |
//! |---|---|
//! | `logit.output.requests{class}` | one per attempt, `class` its result (`crate::count_request`) |
//! | `logit.output.batch.bytes` | the record values' bytes, before compression, once per batch |
//! | `logit.output.records` | records the broker accepted |
//! | `logit.output.records.dropped{reason}` | `oversize` (one record over `max_request_bytes`, or under `record: batch` one event whose value alone is over it, never sent), `rejected` (a partition batch the broker rejected), `invalid_timestamp` or `invalid_record` (records the broker named), or `ambiguous_at_most_once` (records left unsettled when `at_most_once` drops an `Ambiguous` batch, or an `Ambiguous` partition batch beside a `Refused` one) |
//! | `logit.output.records.unkeyed` | records sent without a key because the configured key had no value on their event |
//! | `logit.output.events.skipped{reason="no_log"}` | events `format: text` skipped for having no log record |
//!
//! `oversize`, `records.unkeyed`, and `events.skipped` are encode-side, counted once per batch; the others follow
//! the broker's answer, which a retry never meets twice for one partition batch. Each drop but
//! `ambiguous_at_most_once` raises a throttled diagnostic of the same name: `oversize` names the
//! record count and the `max_request_bytes` cap, and `rejected`, `invalid_timestamp`, and
//! `invalid_record` carry the broker's error code and message.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::Compression;
use logit_core::{Diagnostics, EventBatch, Telemetry};
use logit_pipeline::{BatchContext, DeliveryPosture, Fault, Output, SeqId};
use logit_proto::{FramedEncoder, MessageBuf};

use logit_proto::Signal;

use super::batch::{value_budget, ProducerRecord};
use super::encode::{KafkaEncoder, RecordFormat, RecordKey, RecordMeta, RecordMode};
use super::fault;
use super::multi::{MultiPending, MultiProducer};
use super::producer::{Acks, ProducerConfig, Settlement};
use super::sasl::{self, KafkaSasl};
use crate::accounting::BatchAccounting;
pub use crate::tls::TlsClientSettings;

/// The default `max_request_bytes:`, the Java producer's `max.request.size`.
pub const DEFAULT_MAX_REQUEST_BYTES: usize = 1024 * 1024;

/// The default `timeout:`, the Java producer's `request.timeout.ms`.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

const BATCH_BYTES: &str = "logit.output.batch.bytes";
const RECORDS: &str = "logit.output.records";
const RECORDS_DROPPED: &str = "logit.output.records.dropped";
const RECORDS_UNKEYED: &str = "logit.output.records.unkeyed";
const EVENTS_SKIPPED: &str = "logit.output.events.skipped";

/// Which replicas must have a batch before the broker answers. Mirrors `logit_config::KafkaAcks`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KafkaAcks {
    #[default]
    All,
    Leader,
    None,
}

/// The record batch codec. Mirrors `logit_config::KafkaCompression`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KafkaCompression {
    #[default]
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

/// The topic each signal's records go to under an OTLP format. Mirrors
/// `logit_config::KafkaTopics` with each unset key at its default, the OpenTelemetry Collector
/// Kafka exporter's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalTopics {
    pub logs: String,
    pub metrics: String,
    pub traces: String,
}

impl Default for SignalTopics {
    fn default() -> Self {
        Self {
            logs: "otlp_logs".to_string(),
            metrics: "otlp_metrics".to_string(),
            traces: "otlp_spans".to_string(),
        }
    }
}

/// Everything `kafka_out` is configured with but TLS, which [`KafkaOutput::with_tls`] adds.
#[derive(Debug, Clone)]
pub struct KafkaSettings {
    pub brokers: Vec<String>,
    /// Every record's topic, unless `topics` is set.
    pub topic: String,
    /// Under an OTLP format, each signal's topic in place of `topic`.
    pub topics: Option<SignalTopics>,
    pub key: RecordKey,
    pub format: RecordFormat,
    /// `None` takes the format's own default ([`RecordFormat::default_record`]).
    pub record: Option<RecordMode>,
    pub compression: KafkaCompression,
    pub acks: KafkaAcks,
    pub idempotent: bool,
    pub max_request_bytes: usize,
    pub timeout: Duration,
    /// Sent on every record, in this order.
    pub headers: Vec<(String, String)>,
    /// The `client.id` brokers log and apply quotas by.
    pub client_id: String,
    /// Authenticates every broker connection when set.
    pub sasl: Option<KafkaSasl>,
}

impl KafkaSettings {
    /// The defaults for `brokers` and `topic`, as config leaves every other field.
    pub fn new(brokers: Vec<String>, topic: impl Into<String>) -> Self {
        Self {
            brokers,
            topic: topic.into(),
            topics: None,
            key: RecordKey::None,
            format: RecordFormat::Json,
            record: None,
            compression: KafkaCompression::None,
            acks: KafkaAcks::All,
            idempotent: true,
            max_request_bytes: DEFAULT_MAX_REQUEST_BYTES,
            timeout: DEFAULT_TIMEOUT,
            headers: Vec::new(),
            client_id: "logit".to_string(),
            sasl: None,
        }
    }
}

pub struct KafkaOutput {
    producer: MultiProducer,
    /// Every record's topic, or under `topics` each signal's.
    topic: String,
    topics: Option<SignalTopics>,
    encoder: KafkaEncoder,
    headers: IndexMap<StrBytes, Option<Bytes>>,
    /// Reused across batches.
    messages: MessageBuf<RecordMeta>,
    /// The current batch's records and what the broker settled of them.
    pending: Option<MultiPending>,
    accounting: BatchAccounting,
    diag: Diagnostics,
    telemetry: Telemetry,
}

impl KafkaOutput {
    /// Fails on a broker entry that isn't a `host:port`, idempotence without `acks: all`, a key
    /// with `record: batch`, whose records have none, or a `record:`, an attribute key, or no
    /// topic under an OTLP format, or `topics` under another.
    pub fn new(settings: KafkaSettings) -> anyhow::Result<Self> {
        let otlp = settings.format.is_otlp();
        let record = settings.record.unwrap_or(settings.format.default_record());
        anyhow::ensure!(
            record != RecordMode::Batch || settings.key == RecordKey::None,
            "a kafka_out key needs 'record: event'; a batch record carries no key"
        );
        anyhow::ensure!(
            !otlp || settings.record.is_none(),
            "an OTLP kafka_out format takes no 'record': each record is one export request"
        );
        anyhow::ensure!(
            !otlp || !matches!(settings.key, RecordKey::Attribute(_)),
            "an OTLP kafka_out format can't key by an event attribute: a request holds many events"
        );
        anyhow::ensure!(
            otlp || settings.topics.is_none(),
            "kafka_out 'topics' needs an OTLP format; the other formats write one 'topic'"
        );
        anyhow::ensure!(
            settings.topics.is_some() || !settings.topic.is_empty(),
            "kafka_out has no topic to write to"
        );
        let config = ProducerConfig {
            brokers: settings.brokers,
            topic: String::new(),
            acks: match settings.acks {
                KafkaAcks::All => Acks::All,
                KafkaAcks::Leader => Acks::Leader,
                KafkaAcks::None => Acks::None,
            },
            idempotent: settings.idempotent,
            compression: match settings.compression {
                KafkaCompression::None => Compression::None,
                KafkaCompression::Gzip => Compression::Gzip,
                KafkaCompression::Snappy => Compression::Snappy,
                KafkaCompression::Lz4 => Compression::Lz4,
                KafkaCompression::Zstd => Compression::Zstd,
            },
            timeout: settings.timeout,
            max_request_bytes: settings.max_request_bytes,
            tls: None,
            client_id: settings.client_id,
            sasl: settings.sasl.as_ref().map(|sasl| sasl::authenticator(sasl, settings.timeout)),
        };
        let headers = settings
            .headers
            .into_iter()
            .map(|(k, v)| (StrBytes::from_string(k), Some(Bytes::from(v))))
            .collect();
        let budget = value_budget(settings.max_request_bytes, &headers);
        let accounting = BatchAccounting::default();
        let (telemetry, diag) = (Telemetry::default(), Diagnostics::default());
        Ok(Self {
            producer: MultiProducer::new(config)?,
            topic: settings.topic,
            topics: settings.topics,
            encoder: KafkaEncoder::new(&settings.key, settings.format, record, budget)
                .with_counters(&telemetry, &diag, accounting.gate()),
            headers,
            messages: MessageBuf::default(),
            pending: None,
            accounting,
            diag,
            telemetry,
        })
    }

    /// Attaches the diagnostics handle, also threaded, gated, into the encoder's OTLP mapping.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self.encoder =
            self.encoder.with_counters(&self.telemetry, &self.diag, self.accounting.gate());
        self
    }

    /// Attaches the telemetry handle, also threaded, gated, into the encoder's OTLP mapping.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self.encoder =
            self.encoder.with_counters(&self.telemetry, &self.diag, self.accounting.gate());
        self
    }

    /// The topic a record of `signal` goes to.
    fn topic_of(&self, signal: Option<Signal>) -> &str {
        match (&self.topics, signal) {
            (Some(topics), Some(Signal::Logs)) => &topics.logs,
            (Some(topics), Some(Signal::Metrics)) => &topics.metrics,
            (Some(topics), Some(Signal::Traces)) => &topics.traces,
            _ => &self.topic,
        }
    }

    /// Turns on TLS to every broker (`tls:`; its presence turns TLS on, so an empty block means
    /// the bundled Mozilla roots). The files load and validate here, and register with `reloader`
    /// under this sink's diagnostics and telemetry as they are when this runs, so call it after
    /// `with_diagnostics` and `with_telemetry`.
    pub fn with_tls(
        mut self,
        settings: &TlsClientSettings,
        base_dir: &Path,
        reloader: &logit_pipeline::tls::TlsReloader,
    ) -> anyhow::Result<Self> {
        if settings.insecure_skip_verify {
            self.diag.warn(
                "tls.insecure_skip_verify is set -- the connection is encrypted, but this output \
                 will accept any certificate a broker presents, self-signed or otherwise",
            );
        }
        let config = logit_pipeline::tls::build_client_config(
            settings,
            base_dir,
            reloader,
            &self.diag,
            &self.telemetry,
        )?;
        self.producer.set_tls(Some(Arc::new(config)));
        Ok(self)
    }

    /// Encodes `batch` into a new [`MultiPending`], or `None` when it encodes to no record. The
    /// encode is unit 0 of the batch accounting, so a repeat for a batch already encoded counts
    /// nothing twice.
    fn prepare(&mut self, batch: &EventBatch) -> Option<MultiPending> {
        let mut messages = std::mem::take(&mut self.messages);
        let (first, stats) =
            self.accounting.encode(0, || self.encoder.encode_into(batch, &mut messages));
        if first {
            self.telemetry.count(BATCH_BYTES, messages.total_bytes() as f64, &[]);
            if stats.missing_key > 0 {
                self.telemetry.count(RECORDS_UNKEYED, stats.missing_key as f64, &[]);
            }
            if stats.no_log > 0 {
                self.telemetry.count(EVENTS_SKIPPED, stats.no_log as f64, &[("reason", "no_log")]);
            }
            self.count_oversize(stats.oversize);
        }
        if messages.is_empty() {
            self.messages = messages;
            return None;
        }
        let pending = MultiPending::new(messages.iter_with().map(|(value, meta)| {
            let record = ProducerRecord {
                key: meta.key.clone(),
                value: Some(Bytes::copy_from_slice(value)),
                headers: self.headers.clone(),
                timestamp: meta.timestamp,
            };
            (self.topic_of(meta.signal), record)
        }));
        self.messages = messages;
        Some(pending)
    }

    fn count_oversize(&mut self, n: usize) {
        if n == 0 {
            return;
        }
        self.telemetry.count(RECORDS_DROPPED, n as f64, &[("reason", "oversize")]);
        self.diag.warn_throttled(
            "oversize",
            format_args!(
                "dropped {n} record(s) over 'max_request_bytes' ({})",
                self.producer.template().max_request_bytes
            ),
        );
    }

    async fn attempt(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        if !self.accounting.armed() {
            self.release();
        }
        if self.pending.is_none() {
            if batch.events.is_empty() {
                return Ok(());
            }
            match self.prepare(batch) {
                Some(pending) => self.pending = Some(pending),
                None => return Ok(()),
            }
        }
        let pending = self.pending.as_mut().expect("set above");
        let attempt = self.producer.attempt(pending).await;
        // `Refused` outranks `Ambiguous` in the verdict and is retried under either posture, so
        // under `at_most_once` an `Ambiguous` partition batch, on any topic, is dropped here or
        // the retry resends it.
        if self.accounting.posture() == DeliveryPosture::AtMostOnce {
            if let Err(err) = &attempt.result {
                if logit_pipeline::classify(err) == Fault::Refused {
                    let lost = self.producer.drop_ambiguous(pending);
                    if lost > 0 {
                        self.telemetry.count(
                            RECORDS_DROPPED,
                            lost as f64,
                            &[("reason", "ambiguous_at_most_once")],
                        );
                    }
                }
            }
        }
        self.count_oversize(attempt.oversize);
        for settled in &attempt.settled {
            let n = settled.records as f64;
            match settled.settlement {
                Settlement::Accepted { .. } => self.telemetry.count(RECORDS, n, &[]),
                Settlement::Rejected { .. } => {
                    self.telemetry.count(RECORDS_DROPPED, n, &[("reason", "rejected")]);
                    self.diag.warn_throttled(
                        "rejected",
                        format_args!(
                            "Kafka rejected {} record(s): {}",
                            settled.records, settled.why
                        ),
                    );
                }
                Settlement::Dropped { code } => {
                    let reason = if code == fault::code::INVALID_TIMESTAMP {
                        "invalid_timestamp"
                    } else {
                        "invalid_record"
                    };
                    self.telemetry.count(RECORDS_DROPPED, n, &[("reason", reason)]);
                    self.diag.warn_throttled(
                        reason,
                        format_args!(
                            "Kafka refused {} record(s) it named, and the rest are resent: {}",
                            settled.records, settled.why
                        ),
                    );
                }
            }
        }
        attempt.result
    }

    /// Drops the current [`Pending`], marking the sequence gap any unsettled partition batch
    /// leaves.
    fn release(&mut self) {
        if let Some(pending) = self.pending.take() {
            self.producer.abandon(&pending);
        }
    }
}

#[async_trait::async_trait]
impl Output for KafkaOutput {
    /// Starts a batch: a `Pending` still held belongs to the batch before, which `write_loop`
    /// let go of without a final send.
    fn observe_batch(&mut self, _ctx: BatchContext, _seq: SeqId) {
        self.release();
        self.accounting.observe();
    }

    fn observe_posture(&mut self, posture: DeliveryPosture) {
        self.accounting.observe_posture(posture);
    }

    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let result = self.attempt(batch).await;
        crate::count_request(&self.telemetry, &result);
        if self.accounting.is_final(&result) {
            if let (Err(err), Some(pending)) = (&result, &self.pending) {
                if logit_pipeline::classify(err) == Fault::Ambiguous {
                    let lost = pending.unsettled_records();
                    self.telemetry.count(
                        RECORDS_DROPPED,
                        lost as f64,
                        &[("reason", "ambiguous_at_most_once")],
                    );
                }
            }
            self.release();
        }
        self.accounting.finish(result)
    }
}

#[cfg(test)]
#[path = "sink_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "otlp_tests.rs"]
mod otlp_tests;
