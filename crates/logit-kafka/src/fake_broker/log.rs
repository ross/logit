//! A partition's log as a broker keeps it, and the two APIs that read it, `Fetch` and
//! `ListOffsets`.
//!
//! A batch is stored as it arrived, with bytes 0..8 rewritten to the base offset the broker
//! assigns, and served as stored. The rules a consumer depends on:
//!
//! - **Offsets.** The high watermark is the next offset. The last stable offset (LSO) is the
//!   first offset of the earliest open transaction, else the high watermark. A fetch below the
//!   log start or above the high watermark is `OFFSET_OUT_OF_RANGE`; at the high watermark it's an
//!   empty answer.
//! - **Transactions.** A transactional batch opens its producer's transaction on the partition
//!   if none is open; a control batch closes it, and an abort marker adds `(producer id, first
//!   offset, marker offset)` to the partition's aborted list.
//! - **Reading.** A fetch starts at the stored batch holding its offset and returns whole
//!   batches: under `read_uncommitted` up to the high watermark, under `read_committed` only
//!   batches wholly below the LSO, with every aborted transaction overlapping the offsets
//!   returned. As `ReplicaManager.readFromLog` does, each partition reads at most
//!   `min(partition_max_bytes, what's left of max_bytes)`, except that the first partition to
//!   return data always gets its first batch whole (KIP-74); a later partition whose first
//!   batch is over its limit returns nothing.
//! - **Waiting.** An answer holding fewer than `min_bytes` bytes, and no error, waits up to
//!   `max_wait_ms`; every append re-reads it, and it goes out once it reaches `min_bytes` or at
//!   the deadline.
//! - **ListOffsets.** Timestamp -2 is the log start, -1 the high watermark (the LSO under
//!   `read_committed`), -3 the record with the largest timestamp, and any other timestamp the
//!   first record at or after it, else offset -1.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::fetch_response::{
    AbortedTransaction, FetchableTopicResponse, PartitionData,
};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::{
    ApiKey, FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse, ProducerId,
};
use kafka_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use logit_proto::kafka::batch::{self, ProducerRecord, Stamp};
use tokio::time::Instant;

use super::{answer, Action, Appended, Partition, Reply, State, Taken, MAX_RECORDS_BYTES};
use crate::error::code;

/// The isolation level that reads only committed transactions.
const READ_COMMITTED: i8 = 1;

/// One `Fetch` request as it arrived.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchSeen {
    pub node: i32,
    pub version: i16,
    pub isolation_level: i8,
    pub session_id: i32,
    pub session_epoch: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub max_wait_ms: i32,
    /// `(topic, partition, fetch_offset, partition_max_bytes)`, in request order.
    pub partitions: Vec<(String, i32, i64, i32)>,
}

/// A batch's base offset rewritten to `base`, as a broker assigns offsets.
fn rebase(raw: &[u8], base: i64) -> Bytes {
    assert!(raw.len() >= 8, "a stored batch starts with its 8-byte base offset");
    let mut out = BytesMut::from(raw);
    out[..8].copy_from_slice(&base.to_be_bytes());
    out.freeze()
}

/// A magic-2 batch's records, decompressed with the producer's own codecs.
fn decode_records(raw: Bytes) -> Vec<Record> {
    let mut buf = raw;
    let set = RecordBatchDecoder::decode_with_custom_compression(
        &mut buf,
        Some(|payload: &mut Bytes, codec: Compression| {
            logit_proto::kafka::compression::decompress(codec, payload, MAX_RECORDS_BYTES)
                .map(Bytes::from)
        }),
    )
    .expect("a stored batch decodes");
    set.records
}

impl Partition {
    /// Stores one magic-2 batch at the next offset; returns its base offset.
    pub(super) fn push_batch(&mut self, raw: Bytes) -> i64 {
        let base = self.next_offset;
        let raw = rebase(&raw, base);
        let info = RecordBatchDecoder::decode_batch_info(&mut raw.clone())
            .expect("a stored batch's header decodes")
            .remove(0);
        let last_delta = i32::from_be_bytes(raw[23..27].try_into().unwrap());
        let last = base + i64::from(last_delta);
        let records = decode_records(raw.clone());
        let pid = info.producer_id;
        if info.control {
            // A control record's key is a version, then the type: 0 is ABORT, 1 is COMMIT.
            let abort = records
                .first()
                .and_then(|r| r.key.as_ref())
                .is_some_and(|key| key.len() >= 4 && key[2..4] == [0, 0]);
            if let Some(first) = self.open_transactions.remove(&pid) {
                if abort {
                    self.aborted.push((pid, first, base));
                }
            }
        } else if info.transactional {
            self.open_transactions.entry(pid).or_insert(base);
        }
        self.log.push(Appended {
            records,
            raw,
            base_offset: base,
            last_offset: last,
            transactional: info.transactional,
            control: info.control,
            producer_id: pid,
        });
        self.next_offset = last + 1;
        base
    }

    /// The stored batches a fetch at `offset` reads, stopping before `upper`, within `limit`
    /// bytes unless `min_one` lets the first batch past it.
    fn read(&self, offset: i64, upper: i64, limit: i64, min_one: bool) -> Vec<&Appended> {
        let mut out = Vec::new();
        let mut total = 0i64;
        let start = self.log.iter().position(|b| b.last_offset >= offset);
        for batch in start.map_or(&self.log[..0], |at| &self.log[at..]) {
            if batch.last_offset >= upper {
                break;
            }
            let size = batch.raw.len() as i64;
            if total + size > limit && !(out.is_empty() && min_one) {
                break;
            }
            total += size;
            out.push(batch);
        }
        out
    }
}

impl State {
    /// Appends `records` as one batch with no producer id, compressed with `codec`; returns its
    /// base offset.
    pub fn append(
        &mut self,
        topic: &str,
        partition: i32,
        records: &[ProducerRecord],
        codec: Compression,
    ) -> i64 {
        let raw = batch::encode(records, Stamp::NONE, codec).expect("the batch encodes");
        let base = self.partition_mut(topic, partition).push_batch(raw);
        self.appended.notify_waiters();
        base
    }

    /// Appends `records` as one transactional batch (attributes bit 4) of `producer_id` at
    /// `epoch`, opening the producer's transaction on the partition if none is open; returns its
    /// base offset.
    pub fn append_transactional(
        &mut self,
        topic: &str,
        partition: i32,
        producer_id: i64,
        epoch: i16,
        records: &[ProducerRecord],
    ) -> i64 {
        let records: Vec<Record> = records
            .iter()
            .enumerate()
            .map(|(i, r)| Record {
                transactional: true,
                control: false,
                delete_horizon: false,
                partition_leader_epoch: -1,
                producer_id,
                producer_epoch: epoch,
                timestamp_type: TimestampType::Creation,
                offset: i as i64,
                sequence: i as i32,
                timestamp: r.timestamp,
                key: r.key.clone(),
                value: r.value.clone(),
                headers: r.headers.clone(),
            })
            .collect();
        let base = self.partition_mut(topic, partition).push_batch(encode(&records));
        self.appended.notify_waiters();
        base
    }

    /// Ends `producer_id`'s transaction on the partition with a control batch (attributes bits 4
    /// and 5) holding one marker, Kafka's `EndTransactionMarker`: key version 0 and type 0 for
    /// ABORT or 1 for COMMIT, value version 0 and coordinator epoch 0. An abort adds the
    /// transaction to the aborted list. Returns the marker's offset.
    pub fn end_transaction(
        &mut self,
        topic: &str,
        partition: i32,
        producer_id: i64,
        epoch: i16,
        commit: bool,
    ) -> i64 {
        let marker = Record {
            transactional: true,
            control: true,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id,
            producer_epoch: epoch,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence: -1,
            timestamp: 0,
            key: Some(Bytes::from(vec![0, 0, 0, u8::from(commit)])),
            value: Some(Bytes::from_static(&[0, 0, 0, 0, 0, 0])),
            headers: Default::default(),
        };
        let base = self.partition_mut(topic, partition).push_batch(encode(&[marker]));
        self.appended.notify_waiters();
        base
    }

    /// Stores `raw` at the next offset without decoding it, rewriting its bytes 0..8 to that
    /// offset, and advances the next offset by `offsets`: a magic 0 or 1 message set, say.
    /// Returns its base offset.
    pub fn append_raw(&mut self, topic: &str, partition: i32, raw: Bytes, offsets: i64) -> i64 {
        assert!(offsets > 0, "a stored blob covers at least one offset");
        let p = self.partition_mut(topic, partition);
        let base = p.next_offset;
        p.log.push(Appended {
            records: Vec::new(),
            raw: rebase(&raw, base),
            base_offset: base,
            last_offset: base + offsets - 1,
            transactional: false,
            control: false,
            producer_id: -1,
        });
        p.next_offset = base + offsets;
        self.appended.notify_waiters();
        base
    }

    /// Raises the partition's log start to `log_start`, as retention does: every batch whose
    /// last offset is below it is dropped, and a fetch below it is out of range.
    pub fn truncate(&mut self, topic: &str, partition: i32, log_start: i64) {
        let p = self.partition_mut(topic, partition);
        assert!(log_start <= p.next_offset, "a log start past the high watermark");
        p.log_start = p.log_start.max(log_start);
        let start = p.log_start;
        p.log.retain(|b| b.last_offset >= start);
        self.appended.notify_waiters();
    }
}

fn encode(records: &[Record]) -> Bytes {
    let mut buf = BytesMut::new();
    let options = RecordEncodeOptions { version: 2, compression: Compression::None };
    RecordBatchEncoder::encode(&mut buf, records, &options).expect("the batch encodes");
    buf.freeze()
}

/// One read of a fetch request against the log.
struct Read {
    response: FetchResponse,
    bytes: usize,
    error: bool,
}

fn read_fetch(state: &State, node: i32, request: &FetchRequest, taken: &Option<Taken>) -> Read {
    let mut remaining = i64::from(request.max_bytes);
    let mut min_one = true;
    let mut bytes = 0;
    let mut error = false;
    let mut responses = Vec::new();
    for topic in &request.topics {
        let name = topic.topic.0.to_string();
        let mut partitions = Vec::new();
        for asked in &topic.partitions {
            let index = asked.partition;
            let data = PartitionData::default()
                .with_partition_index(index)
                .with_high_watermark(-1)
                .with_last_stable_offset(-1)
                .with_log_start_offset(-1)
                .with_records(Some(Bytes::new()));
            let found = state.topics.get(&name).and_then(|parts| parts.get(index as usize));
            let refused = match (Taken::error_for(taken, &name, index), found) {
                (Some(code), _) => Some(code),
                (None, None) => Some(code::UNKNOWN_TOPIC_OR_PARTITION),
                (None, Some(p)) if p.leader != node => Some(code::NOT_LEADER_OR_FOLLOWER),
                (None, Some(p))
                    if asked.fetch_offset < p.log_start
                        || asked.fetch_offset > p.high_watermark() =>
                {
                    Some(code::OFFSET_OUT_OF_RANGE)
                }
                _ => None,
            };
            if let Some(code) = refused {
                error |= code != code::NONE;
                partitions.push(data.with_error_code(code));
                continue;
            }
            let p = found.expect("a partition with no refusal exists");
            let committed = request.isolation_level == READ_COMMITTED;
            let upper = if committed { p.last_stable_offset() } else { p.high_watermark() };
            let limit = i64::from(asked.partition_max_bytes).min(remaining);
            let batches = p.read(asked.fetch_offset, upper, limit, min_one);
            let mut records = BytesMut::new();
            for b in &batches {
                records.extend_from_slice(&b.raw);
            }
            if !batches.is_empty() {
                min_one = false;
                remaining -= records.len() as i64;
            }
            bytes += records.len();
            let aborted = committed.then(|| {
                let last = batches.last().map_or(asked.fetch_offset - 1, |b| b.last_offset);
                p.aborted
                    .iter()
                    .filter(|&&(_, first, end)| first <= last && end >= asked.fetch_offset)
                    .map(|&(pid, first, _)| {
                        AbortedTransaction::default()
                            .with_producer_id(ProducerId(pid))
                            .with_first_offset(first)
                    })
                    .collect()
            });
            partitions.push(
                data.with_high_watermark(p.high_watermark())
                    .with_last_stable_offset(p.last_stable_offset())
                    .with_log_start_offset(p.log_start)
                    .with_aborted_transactions(aborted)
                    .with_records(Some(records.freeze())),
            );
        }
        responses.push(
            FetchableTopicResponse::default()
                .with_topic(topic.topic.clone())
                .with_partitions(partitions),
        );
    }
    let response = FetchResponse::default()
        .with_throttle_time_ms(Taken::throttle(taken))
        .with_session_id(0)
        .with_responses(responses);
    Read { response, bytes, error }
}

/// Takes the first `CorruptCrc` step covering a partition `response` serves a batch for, and
/// flips the low bit of that batch's CRC.
fn corrupt(state: &mut State, response: &mut FetchResponse) {
    for topic in &mut response.responses {
        let name = topic.topic.0.to_string();
        for data in &mut topic.partitions {
            let Some(records) = data.records.as_ref().filter(|r| r.len() > 21) else { continue };
            let step = state.script.iter().position(|s| {
                s.api == ApiKey::Fetch
                    && matches!(s.action, Action::CorruptCrc)
                    && s.covers(&name, data.partition_index)
            });
            if let Some(at) = step {
                state.script.remove(at);
                let mut flipped = BytesMut::from(&records[..]);
                flipped[17] ^= 1;
                data.records = Some(flipped.freeze());
                return;
            }
        }
    }
}

/// Handles one `Fetch`: steps and the request record now, then the answer now or once an append
/// or the deadline releases it.
pub(super) fn fetch(
    shared: &Arc<Mutex<State>>,
    node: i32,
    correlation: i32,
    version: i16,
    request: FetchRequest,
) -> Reply {
    let mut state = shared.lock().unwrap();
    let asked: Vec<(String, i32, i64, i32)> = request
        .topics
        .iter()
        .flat_map(|t| {
            t.partitions.iter().map(|p| {
                (t.topic.0.to_string(), p.partition, p.fetch_offset, p.partition_max_bytes)
            })
        })
        .collect();
    state.fetches.push(FetchSeen {
        node,
        version,
        isolation_level: request.isolation_level,
        session_id: request.session_id,
        session_epoch: request.session_epoch,
        min_bytes: request.min_bytes,
        max_bytes: request.max_bytes,
        max_wait_ms: request.max_wait_ms,
        partitions: asked.clone(),
    });
    let names: Vec<(&str, i32)> = asked.iter().map(|(t, p, _, _)| (t.as_str(), *p)).collect();
    let taken = state.take_step_in(ApiKey::Fetch, &names);
    if let Some(reply) = Taken::connection(&taken) {
        return reply;
    }
    let read = read_fetch(&state, node, &request, &taken);
    let min_bytes = request.min_bytes.max(0) as usize;
    let enough = move |read: &Read| read.error || read.bytes >= min_bytes;
    if enough(&read) || request.max_wait_ms <= 0 {
        let mut response = read.response;
        corrupt(&mut state, &mut response);
        return answer(correlation, ApiKey::Fetch, version, &response);
    }
    drop(state);
    let shared = shared.clone();
    let deadline = Instant::now() + Duration::from_millis(request.max_wait_ms as u64);
    Reply::Later(Box::pin(async move {
        let appended = shared.lock().unwrap().appended.clone();
        loop {
            let notified = appended.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = shared.lock().unwrap();
                let read = read_fetch(&state, node, &request, &taken);
                if enough(&read) || Instant::now() >= deadline {
                    let mut response = read.response;
                    corrupt(&mut state, &mut response);
                    return answer(correlation, ApiKey::Fetch, version, &response);
                }
            }
            tokio::select! {
                () = &mut notified => {}
                () = tokio::time::sleep_until(deadline) => {}
            }
        }
    }))
}

/// Answers one `ListOffsets`.
pub(super) fn list_offsets(
    state: &mut State,
    node: i32,
    version: i16,
    request: &ListOffsetsRequest,
    taken: &Option<Taken>,
) -> ListOffsetsResponse {
    let mut topics = Vec::new();
    for topic in &request.topics {
        let name = topic.name.0.to_string();
        let mut partitions = Vec::new();
        for asked in &topic.partitions {
            let index = asked.partition_index;
            state.list_offsets.push((
                node,
                version,
                name.clone(),
                index,
                asked.timestamp,
                request.isolation_level,
            ));
            let answer = ListOffsetsPartitionResponse::default().with_partition_index(index);
            let found = state.topics.get(&name).and_then(|parts| parts.get(index as usize));
            let answer = match (Taken::error_for(taken, &name, index), found) {
                (Some(code), _) => answer.with_error_code(code),
                (None, None) => answer.with_error_code(code::UNKNOWN_TOPIC_OR_PARTITION),
                (None, Some(p)) if p.leader != node => {
                    answer.with_error_code(code::NOT_LEADER_OR_FOLLOWER)
                }
                (None, Some(p)) => {
                    let (offset, timestamp) = match asked.timestamp {
                        -2 => (p.log_start, -1),
                        -1 if request.isolation_level == READ_COMMITTED => {
                            (p.last_stable_offset(), -1)
                        }
                        -1 => (p.high_watermark(), -1),
                        -3 => p
                            .records()
                            .into_iter()
                            .max_by_key(|r| r.timestamp)
                            .map_or((-1, -1), |r| (r.offset, r.timestamp)),
                        at => p
                            .records()
                            .into_iter()
                            .find(|r| r.timestamp >= at && r.offset >= p.log_start)
                            .map_or((-1, -1), |r| (r.offset, r.timestamp)),
                    };
                    answer.with_offset(offset).with_timestamp(timestamp)
                }
            };
            partitions.push(answer);
        }
        topics.push(
            ListOffsetsTopicResponse::default()
                .with_name(topic.name.clone())
                .with_partitions(partitions),
        );
    }
    ListOffsetsResponse::default().with_throttle_time_ms(Taken::throttle(taken)).with_topics(topics)
}
