//! `kafka_out`: the `Output` that drives [`Producer`], one record per event.
//!
//! One `logit` batch becomes one [`Pending`]: [`KafkaEncoder`] writes every record, and
//! [`Producer::prepare`] partitions them and cuts the partition batches once. The sink keeps that
//! `Pending` across every attempt `write_loop` makes at the batch, so a retry resends only the
//! partition batches the broker hasn't settled, with each unkeyed record on its first partition
//! and each partition batch under its first sequence (`docs/adr/kafka-output.md`, "Amendment:
//! what W1 settled", item 1). It lets go of the `Pending` when a send is final (`Ok`, or a fault
//! `write_loop` won't retry under the posture), and calls [`Producer::abandon`] on one dropped
//! with a partition batch unsettled: when the send was final, or when `Output::observe_batch`
//! starts another batch first, as after a shutdown cut. A caller that never calls
//! `observe_batch` gets a fresh `Pending` on every `send`.
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
//! | `logit.output.records.dropped{reason}` | `oversize` (one record over `max_request_bytes`, never sent), `rejected` (a partition batch the broker rejected), `invalid_timestamp` or `invalid_record` (records the broker named), or `ambiguous_at_most_once` (records left unsettled when `at_most_once` drops an `Ambiguous` batch) |
//! | `logit.output.records.unkeyed` | records sent without a key because the configured key had no value on their event |
//!
//! `oversize` and `records.unkeyed` are encode-side, counted once per batch; the others follow
//! the broker's answer, which a retry never meets twice for one partition batch. Each drop but
//! `ambiguous_at_most_once` raises a throttled diagnostic of the same name, with the broker's
//! error code and message.

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

use super::batch::ProducerRecord;
use super::encode::{KafkaEncoder, RecordFormat, RecordKey, RecordMeta};
use super::fault;
use super::producer::{Acks, PartState, Pending, Producer, ProducerConfig, Settlement};
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

/// Everything `kafka_out` is configured with but TLS, which [`KafkaOutput::with_tls`] adds.
#[derive(Debug, Clone)]
pub struct KafkaSettings {
    pub brokers: Vec<String>,
    pub topic: String,
    pub key: RecordKey,
    pub format: RecordFormat,
    pub compression: KafkaCompression,
    pub acks: KafkaAcks,
    pub idempotent: bool,
    pub max_request_bytes: usize,
    pub timeout: Duration,
    /// Sent on every record, in this order.
    pub headers: Vec<(String, String)>,
    /// The `client.id` brokers log and apply quotas by.
    pub client_id: String,
}

impl KafkaSettings {
    /// The defaults for `brokers` and `topic`, as config leaves every other field.
    pub fn new(brokers: Vec<String>, topic: impl Into<String>) -> Self {
        Self {
            brokers,
            topic: topic.into(),
            key: RecordKey::None,
            format: RecordFormat::Json,
            compression: KafkaCompression::None,
            acks: KafkaAcks::All,
            idempotent: true,
            max_request_bytes: DEFAULT_MAX_REQUEST_BYTES,
            timeout: DEFAULT_TIMEOUT,
            headers: Vec::new(),
            client_id: "logit".to_string(),
        }
    }
}

pub struct KafkaOutput {
    config: ProducerConfig,
    producer: Producer,
    encoder: KafkaEncoder,
    headers: IndexMap<StrBytes, Option<Bytes>>,
    /// Reused across batches.
    messages: MessageBuf<RecordMeta>,
    /// The current batch's records and what the broker settled of them.
    pending: Option<Pending>,
    accounting: BatchAccounting,
    diag: Diagnostics,
    telemetry: Telemetry,
}

impl KafkaOutput {
    /// Fails on a broker entry that isn't a `host:port`, or idempotence without `acks: all`.
    pub fn new(settings: KafkaSettings) -> anyhow::Result<Self> {
        let config = ProducerConfig {
            brokers: settings.brokers,
            topic: settings.topic,
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
        };
        let headers = settings
            .headers
            .into_iter()
            .map(|(k, v)| (StrBytes::from_string(k), Some(Bytes::from(v))))
            .collect();
        Ok(Self {
            producer: Producer::new(config.clone())?,
            config,
            encoder: KafkaEncoder::new(&settings.key, settings.format),
            headers,
            messages: MessageBuf::default(),
            pending: None,
            accounting: BatchAccounting::default(),
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
        })
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
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
        self.config.tls = Some(Arc::new(config));
        self.producer = Producer::new(self.config.clone())?;
        Ok(self)
    }

    /// Encodes and partitions `batch` into a new [`Pending`]. The encode is unit 0 of the batch
    /// accounting, so a repeat after a failed `prepare` counts nothing twice.
    async fn prepare(&mut self, batch: &EventBatch) -> anyhow::Result<Pending> {
        let mut messages = std::mem::take(&mut self.messages);
        let (first, stats) =
            self.accounting.encode(0, || self.encoder.encode_into(batch, &mut messages));
        if first {
            self.telemetry.count(BATCH_BYTES, messages.total_bytes() as f64, &[]);
            if stats.missing_key > 0 {
                self.telemetry.count(RECORDS_UNKEYED, stats.missing_key as f64, &[]);
            }
        }
        let records: Vec<ProducerRecord> = messages
            .iter_with()
            .map(|(value, meta)| ProducerRecord {
                key: meta.key.clone(),
                value: Some(Bytes::copy_from_slice(value)),
                headers: self.headers.clone(),
                timestamp: meta.timestamp,
            })
            .collect();
        self.messages = messages;
        let pending = self.producer.prepare(records).await?;
        if pending.oversize > 0 {
            self.telemetry.count(
                RECORDS_DROPPED,
                pending.oversize as f64,
                &[("reason", "oversize")],
            );
            self.diag.warn_throttled(
                "oversize",
                format_args!(
                    "dropped {} record(s) over 'max_request_bytes' ({})",
                    pending.oversize, self.config.max_request_bytes
                ),
            );
        }
        Ok(pending)
    }

    async fn attempt(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        if !self.accounting.armed() {
            self.release();
        }
        if self.pending.is_none() {
            if batch.events.is_empty() {
                return Ok(());
            }
            let pending = self.prepare(batch).await?;
            self.pending = Some(pending);
        }
        let pending = self.pending.as_mut().expect("set above");
        let attempt = self.producer.attempt(pending).await;
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
                    let lost: usize = pending
                        .parts
                        .iter()
                        .filter(|p| matches!(p.state, PartState::Unsettled { .. }))
                        .map(|p| p.records.len())
                        .sum();
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
