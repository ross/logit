//! The fetch path: one loop per leader broker, each sending full Fetch requests for the assigned
//! partitions it leads whose buffer is empty, and the reading of a partition's records blob into
//! records for the caller (`docs/adr/kafka-input.md`, items 5, 7, 9, and 10).
//!
//! - **Full fetches only.** Every request sends session id 0 and epoch -1 (the crate's
//!   defaults), which opens no KIP-227 session; the wait is `max_wait` plus the timeout.
//! - **Backpressure.** A partition holds at most one fetch's records ([`Part::buffer`]); while
//!   they're undelivered it's left out of the next request, as Java pauses a partition with
//!   buffered records. One record batch decompresses at a time, bounded by `max_batch_bytes`.
//! - **Out of range.** `OFFSET_OUT_OF_RANGE` lists the earliest and latest offsets (two
//!   requests: a broker refuses one partition twice in one ListOffsets). A position below the
//!   log start moves to it, and the offsets between are counted
//!   `logit.input.records.dropped{reason="retention"}` and marked done; one above the log end
//!   moves back to it. `start:` never applies: it isn't permission to skip retained records.
//! - **`read_committed`.** A partition's `aborted_transactions` and the abort markers decide
//!   which transactional batches to skip, as the Java client's `CompletedFetch` does: a producer
//!   joins the aborted set once a batch of its reaches its transaction's first offset, and leaves
//!   it at its abort marker. Skipped records count `records.skipped{reason="aborted"}`; control
//!   records aren't counted.
//! - **Bad batches.** A CRC mismatch or an unparsable batch refetches the partition once from
//!   the same offset; a second failure at the same batch skips it, counted
//!   `records.dropped{reason="corrupt"}` per offset skipped. The skip ends at the base offset of
//!   the next batch in the answer whose CRC passes; else at the base offset in the first 12 bytes
//!   of a batch the answer cut short, within the failed batch's base and the high watermark; else,
//!   after one refetch giving the partition `max_batch_bytes` of room so a batch
//!   `max_partition_bytes` kept out arrives beside it, at the high watermark (the last stable
//!   offset under `read_committed`), never past it. No
//!   CRC covers a batch's length, so nothing decoded after a failed batch is trusted until a CRC
//!   passes. A batch whose length reaches past the answer's end at the fetch position is corrupt
//!   the same way. An oversize batch (`oversize`) and a magic 0 or 1 message set
//!   (`legacy_format`) are skipped at once. Each skip has a throttled diagnostic
//!   naming the topic, partition, and offset.
//! - **A loop's response is applied only to partitions still at the requested offset, led by
//!   that loop, in the same assignment epoch**, so a response that crosses a revoke or a leader
//!   move changes nothing.

use std::collections::{BTreeMap, HashSet};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::{ApiKey, FetchRequest, TopicName};
use kafka_protocol::protocol::StrBytes;
use logit_proto::kafka::{BatchErrorKind, Record};
use tokio::sync::Notify;
use tokio::time::Instant;

use super::group::Failure;
use super::offsets::{self, Tracker, EARLIEST, LATEST};
use super::pool::{CallError, Link, Pool, Slot, Target};
use super::{
    intern, partition_code, Backoff, FetchBounds, Isolation, PartitionCode, TopicPartition,
};

/// A run of records from one partition, in offset order, and the offset span they cover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fetched {
    pub tp: TopicPartition,
    /// The partition's reset epoch when these were fetched, for
    /// [`super::Consumer::mark_accepted`]: a range from before a reset is ignored.
    pub epoch: u64,
    pub records: Vec<Record>,
    /// Every offset this fetch consumed: the records, and any control record, aborted record,
    /// skipped batch, or compaction gap between them.
    pub span: Range<i64>,
}

impl Fetched {
    /// The offset range a batch of `records[range]` covers, for
    /// [`super::Consumer::mark_accepted`]. Consecutive ranges of records tile the span: the
    /// first starts at the span's start, each later one where the one before ended, and the
    /// last ends at the span's end, so the offsets between records are accepted with them.
    pub fn cut(&self, records: Range<usize>) -> Range<i64> {
        let start = match records.start {
            0 => self.span.start,
            i => self.records[i - 1].offset + 1,
        };
        let end = if records.end >= self.records.len() {
            self.span.end
        } else {
            self.records[records.end - 1].offset + 1
        };
        start..end
    }
}

/// Where a partition reads from next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Position {
    At(i64),
    /// Unknown until ListOffsets answers this timestamp: a partition with no commit.
    Start(i64),
    /// `OFFSET_OUT_OF_RANGE` at this offset; ListOffsets decides where to go.
    OutOfRange(i64),
}

/// A process-unique reset epoch, so no two partition states, or two resets of one, share one.
pub(crate) fn next_epoch() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// One assigned partition's fetch state.
#[derive(Debug)]
pub(crate) struct Part {
    pub(crate) leader: Option<i32>,
    /// Changes with every tracker reset; [`Fetched::epoch`] carries it.
    pub(crate) epoch: u64,
    /// The epoch the state started with: an older one belongs to another assignment.
    first_epoch: u64,
    /// Per reset, the epoch it replaced and the lowest offset any reset at or after it moved
    /// the tracker to, so `reset_at` rises with the epoch.
    resets: Vec<(u64, i64)>,
    pub(crate) position: Position,
    pub(crate) buffer: Option<Fetched>,
    /// No fetch before this, after an error.
    pub(crate) retry_at: Option<Instant>,
    pub(crate) backoff: Backoff,
    /// The base offset of a batch that failed once and is being refetched.
    pub(crate) corrupt_at: Option<i64>,
    /// The base offset of a batch that failed twice with nothing verified after it: the next
    /// fetch gives the partition `max_batch_bytes` of room.
    pub(crate) widen_at: Option<i64>,
    pub(crate) tracker: Tracker,
}

impl Part {
    pub(crate) fn new(position: Position, tracker: Tracker) -> Self {
        let epoch = next_epoch();
        Self {
            leader: None,
            epoch,
            first_epoch: epoch,
            resets: Vec::new(),
            position,
            buffer: None,
            retry_at: None,
            backoff: Backoff::new(),
            corrupt_at: None,
            widen_at: None,
            tracker,
        }
    }

    /// Resets the tracker to `position`, the end of a log that no longer holds the offsets
    /// fetched before it, and starts a new epoch.
    pub(crate) fn reset(&mut self, position: i64) {
        self.tracker.reset(position);
        for (_, at) in &mut self.resets {
            *at = (*at).min(position);
        }
        self.resets.retain(|&(_, at)| at != position);
        self.resets.push((self.epoch, position));
        self.epoch = next_epoch();
    }

    /// Marks `range`, fetched in `epoch`, done. A range from before a reset is cut at the lowest
    /// offset a reset since then moved to: offsets below it named the same records then as now,
    /// and leaving them unmarked would hold the commit below them forever. A range from another
    /// assignment is ignored.
    pub(crate) fn accept(&mut self, epoch: u64, range: Range<i64>) {
        if epoch == self.epoch {
            self.tracker.mark(range);
        } else if (self.first_epoch..self.epoch).contains(&epoch) {
            if let Some(&(_, at)) = self.resets.iter().find(|&&(replaced, _)| replaced >= epoch) {
                self.tracker.mark(range.start..range.end.min(at));
            }
        }
    }

    fn back_off(&mut self) {
        self.retry_at = Some(Instant::now() + self.backoff.step());
    }

    fn ready(&self, now: Instant) -> bool {
        self.buffer.is_none() && self.retry_at.is_none_or(|at| at <= now)
    }
}

/// What the consumer and its fetch loops share.
#[derive(Debug, Default)]
pub(crate) struct Shared {
    pub(crate) parts: BTreeMap<TopicPartition, Part>,
    /// Bumped by every revoke and assignment, so a response from before it is dropped.
    pub(crate) epoch: u64,
    /// A loop met a leader error or lost its connection: refresh metadata.
    pub(crate) refresh: bool,
}

/// The two wake-ups between the consumer and its loops.
#[derive(Debug, Default)]
pub(crate) struct Wakes {
    /// A buffer filled, or the consumer should look again.
    pub(crate) data: Notify,
    /// A buffer drained or a position changed: a loop may have work.
    pub(crate) loops: Notify,
}

/// What every fetch loop is configured with.
#[derive(Clone)]
pub(crate) struct LoopSettings {
    pub(crate) bounds: FetchBounds,
    pub(crate) isolation: Isolation,
    pub(crate) max_batch_bytes: usize,
}

/// What a Fetch answer adds beyond its records: the response, topic, and partition headers and
/// the aborted-transaction lists.
const FETCH_ANSWER_SLACK: usize = 1 << 20;

impl LoopSettings {
    /// The largest Fetch answer to read: `max_bytes` of records, plus one batch past it, which
    /// KIP-74 returns whole and `max_batch_bytes` bounds, plus the answer's own framing. A
    /// larger answer is refused as `response_too_large`.
    pub(crate) fn max_response(&self) -> usize {
        usize::try_from(self.bounds.max_bytes.max(0)).unwrap_or(0)
            + self.max_batch_bytes
            + FETCH_ANSWER_SLACK
    }
}

/// Runs the fetch loop for broker `node` until aborted.
pub(crate) async fn run(
    node: i32,
    pool: Pool,
    shared: Arc<Mutex<Shared>>,
    wakes: Arc<Wakes>,
    settings: LoopSettings,
) {
    let mut link =
        Link::new(Slot::Fetch(node), Target::Node(node)).with_max_response(settings.max_response());
    let mut backoff = Backoff::new();
    loop {
        let wake = wakes.loops.notified();
        tokio::pin!(wake);
        wake.as_mut().enable();
        let now = Instant::now();
        let (epoch, resets, ready, next_retry) = {
            let state = shared.lock().unwrap();
            let mine = state.parts.iter().filter(|(_, p)| p.leader == Some(node));
            let mut resets = Vec::new();
            let mut ready = Vec::new();
            let mut next_retry: Option<Instant> = None;
            for (tp, part) in mine {
                if !part.ready(now) {
                    if let Some(at) = part.retry_at.filter(|_| part.buffer.is_none()) {
                        next_retry = Some(next_retry.map_or(at, |n| n.min(at)));
                    }
                    continue;
                }
                match part.position {
                    Position::At(offset) => ready.push((tp.clone(), offset)),
                    other => resets.push((tp.clone(), other)),
                }
            }
            (state.epoch, resets, ready, next_retry)
        };
        if !resets.is_empty() {
            if resolve(&mut link, &pool, &shared, &settings, epoch, resets).await.is_err() {
                backoff.wait().await;
            } else {
                backoff.reset();
            }
            continue;
        }
        if ready.is_empty() {
            match next_retry {
                Some(at) => {
                    tokio::select! {
                        _ = wake => {}
                        _ = tokio::time::sleep_until(at) => {}
                    }
                }
                None => wake.await,
            }
            continue;
        }
        match fetch(&mut link, &pool, &shared, &wakes, &settings, node, epoch, ready).await {
            Ok(()) => backoff.reset(),
            Err(()) => backoff.wait().await,
        }
    }
}

/// Lists offsets for partitions with no position; `Err` after a failure the loop backs off on.
async fn resolve(
    link: &mut Link,
    pool: &Pool,
    shared: &Arc<Mutex<Shared>>,
    settings: &LoopSettings,
    epoch: u64,
    resets: Vec<(TopicPartition, Position)>,
) -> Result<(), ()> {
    let starts: Vec<(TopicPartition, i64)> = resets
        .iter()
        .filter_map(|(tp, p)| match p {
            Position::Start(ts) => Some((tp.clone(), *ts)),
            _ => None,
        })
        .collect();
    let ranged: Vec<(TopicPartition, i64)> = resets
        .iter()
        .filter_map(|(tp, p)| match p {
            Position::OutOfRange(at) => Some((tp.clone(), *at)),
            _ => None,
        })
        .collect();
    if !starts.is_empty() {
        let answers = list(link, pool, shared, settings.isolation, &starts).await?;
        let mut state = shared.lock().unwrap();
        if state.epoch != epoch {
            return Ok(());
        }
        for (tp, offset) in answers {
            if let Some(part) = state.parts.get_mut(&tp) {
                if matches!(part.position, Position::Start(_)) {
                    part.position = Position::At(offset);
                    part.tracker = Tracker::new(offset, None);
                }
            }
        }
    }
    if !ranged.is_empty() {
        let at = |ts| ranged.iter().map(|(tp, _)| (tp.clone(), ts)).collect::<Vec<_>>();
        let earliest = list(link, pool, shared, settings.isolation, &at(EARLIEST)).await?;
        let latest = list(link, pool, shared, settings.isolation, &at(LATEST)).await?;
        let mut state = shared.lock().unwrap();
        if state.epoch != epoch {
            return Ok(());
        }
        for (tp, from) in &ranged {
            let start = earliest.iter().find(|(t, _)| t == tp).map(|(_, o)| *o);
            let end = latest.iter().find(|(t, _)| t == tp).map(|(_, o)| *o);
            let (Some(start), Some(end)) = (start, end) else { continue };
            let Some(part) = state.parts.get_mut(tp) else { continue };
            if part.position != Position::OutOfRange(*from) {
                continue;
            }
            let counters = &pool.counters;
            counters.count("logit.input.offsets.reset", 1.0, &[("reason", "out_of_range")]);
            let to = if *from < start {
                let gap = (start - from) as f64;
                counters.count("logit.input.records", gap, &[]);
                counters.count("logit.input.records.dropped", gap, &[("reason", "retention")]);
                counters.warn(
                    "records_dropped",
                    format_args!(
                        "Kafka {tp}: offsets {from} to {} were deleted by retention before they \
                         were read",
                        start - 1
                    ),
                );
                part.tracker.mark(*from..start);
                start
            } else if *from > end {
                part.reset(end);
                end
            } else {
                *from
            };
            part.position = Position::At(to);
        }
    }
    Ok(())
}

/// One ListOffsets; a partition answered with an error backs off (and asks for a refresh when
/// its leader may have moved), and is left out of the answer.
async fn list(
    link: &mut Link,
    pool: &Pool,
    shared: &Arc<Mutex<Shared>>,
    isolation: Isolation,
    asks: &[(TopicPartition, i64)],
) -> Result<Vec<(TopicPartition, i64)>, ()> {
    let answers = match offsets::list(link, pool, isolation, asks).await {
        Ok(answers) => answers,
        Err(failure) => {
            if let Failure::Code(c) = failure {
                pool.counters.broker_code(ApiKey::ListOffsets, c);
            }
            shared.lock().unwrap().refresh = true;
            return Err(());
        }
    };
    let mut out = Vec::new();
    let mut state = shared.lock().unwrap();
    for (tp, error, offset) in answers {
        if error == crate::error::code::NONE {
            out.push((tp, offset));
            continue;
        }
        pool.counters.broker_code(ApiKey::ListOffsets, error);
        let refresh = settle_error(pool, &mut state, &tp, error, "ListOffsets");
        state.refresh |= refresh;
    }
    Ok(out)
}

/// Backs a partition off after `error`; returns whether metadata should be refreshed.
fn settle_error(
    pool: &Pool,
    state: &mut Shared,
    tp: &TopicPartition,
    error: i16,
    api: &str,
) -> bool {
    let Some(part) = state.parts.get_mut(tp) else { return false };
    part.back_off();
    match partition_code(error) {
        PartitionCode::Refresh => true,
        PartitionCode::BackOff(Some(key)) => {
            pool.counters.warn(
                key,
                format_args!("Kafka {tp}: {api} answered {}", crate::error::name(error)),
            );
            false
        }
        _ => false,
    }
}

/// One Fetch for `ready` and the reading of its answer; `Err` after a failure the loop backs
/// off on.
#[allow(clippy::too_many_arguments)]
async fn fetch(
    link: &mut Link,
    pool: &Pool,
    shared: &Arc<Mutex<Shared>>,
    wakes: &Wakes,
    settings: &LoopSettings,
    node: i32,
    epoch: u64,
    ready: Vec<(TopicPartition, i64)>,
) -> Result<(), ()> {
    let bounds = settings.bounds;
    let widened = i32::try_from(settings.max_batch_bytes).unwrap_or(i32::MAX);
    let mut max_bytes = bounds.max_bytes;
    let mut topics: BTreeMap<Arc<str>, Vec<FetchPartition>> = BTreeMap::new();
    {
        let state = shared.lock().unwrap();
        for (tp, offset) in &ready {
            let widen = state.parts.get(tp).is_some_and(|p| p.widen_at.is_some());
            let limit = if widen {
                // The response cap already allows `max_bytes` plus `max_batch_bytes`.
                max_bytes = bounds.max_bytes.saturating_add(widened);
                bounds.max_partition_bytes.max(widened)
            } else {
                bounds.max_partition_bytes
            };
            topics.entry(tp.topic.clone()).or_default().push(
                FetchPartition::default()
                    .with_partition(tp.partition)
                    .with_fetch_offset(*offset)
                    .with_partition_max_bytes(limit),
            );
        }
    }
    let wait = bounds.max_wait + pool.timeout();
    let max_wait_ms = i32::try_from(bounds.max_wait.as_millis()).unwrap_or(i32::MAX);
    let result = link
        .call(pool, ApiKey::Fetch, Some(wait), |_| {
            FetchRequest::default()
                .with_max_wait_ms(max_wait_ms)
                .with_min_bytes(bounds.min_bytes)
                .with_max_bytes(max_bytes)
                .with_isolation_level(settings.isolation.wire())
                .with_topics(
                    topics
                        .into_iter()
                        .map(|(t, partitions)| {
                            FetchTopic::default()
                                .with_topic(TopicName(StrBytes::from_string(t.to_string())))
                                .with_partitions(partitions)
                        })
                        .collect(),
                )
        })
        .await;
    let response = match result {
        Ok((_, response)) => response,
        // A batch the answer can't skip without reading: the same fetch draws the same answer.
        Err(CallError::TooLarge(err)) => {
            pool.counters.warn(
                "response_too_large",
                format_args!(
                    "Kafka broker {node}: a Fetch answer declares {} bytes, over the {}-byte cap \
                     of fetch.max_bytes plus max_batch_bytes plus 1 MiB; a record batch this \
                     large holds every partition fetched from this broker until \
                     max_batch_bytes is raised",
                    err.declared, err.cap
                ),
            );
            return Err(());
        }
        Err(_) => {
            shared.lock().unwrap().refresh = true;
            return Err(());
        }
    };
    if response.error_code != crate::error::code::NONE {
        pool.counters.broker_code(ApiKey::Fetch, response.error_code);
        pool.counters.warn(
            "broker_error",
            format_args!(
                "Kafka broker {node}: Fetch answered {}",
                crate::error::name(response.error_code)
            ),
        );
        return Err(());
    }
    let asked: HashSet<(TopicPartition, i64)> = ready.into_iter().collect();
    let mut state = shared.lock().unwrap();
    if state.epoch != epoch {
        return Ok(());
    }
    let mut filled = false;
    for topic in &response.responses {
        for data in &topic.partitions {
            let tp = TopicPartition::new(topic.topic.0.as_str(), data.partition_index);
            let Some(part) = state.parts.get(&tp) else { continue };
            let Position::At(from) = part.position else { continue };
            if part.leader != Some(node)
                || part.buffer.is_some()
                || !asked.contains(&(tp.clone(), from))
            {
                continue;
            }
            match partition_code(data.error_code) {
                PartitionCode::Ok => {}
                PartitionCode::OutOfRange => {
                    pool.counters.broker_code(ApiKey::Fetch, data.error_code);
                    state.parts.get_mut(&tp).expect("checked").position =
                        Position::OutOfRange(from);
                    continue;
                }
                _ => {
                    pool.counters.broker_code(ApiKey::Fetch, data.error_code);
                    let refresh = settle_error(pool, &mut state, &tp, data.error_code, "Fetch");
                    state.refresh |= refresh;
                    continue;
                }
            }
            let part = state.parts.get_mut(&tp).expect("checked");
            let aborted = match settings.isolation {
                Isolation::ReadCommitted => data
                    .aborted_transactions
                    .iter()
                    .flatten()
                    .map(|t| (t.producer_id.0, t.first_offset))
                    .collect(),
                Isolation::ReadUncommitted => Vec::new(),
            };
            let blob = data.records.clone().unwrap_or_default();
            let stable = match settings.isolation {
                Isolation::ReadCommitted => data.last_stable_offset,
                Isolation::ReadUncommitted => data.high_watermark,
            };
            let bad = (part.corrupt_at, part.widen_at);
            let read = read(&blob, from, &aborted, settings.max_batch_bytes, bad, stable);
            part.corrupt_at = read.corrupt_at;
            part.widen_at = read.widen_at;
            part.backoff.reset();
            part.retry_at = None;
            count_read(pool, &tp, &read);
            if stable >= 0 {
                pool.counters.telemetry.gauge(
                    "logit.input.lag",
                    (stable - read.position).max(0) as f64,
                    &[
                        ("topic", intern(&tp.topic)),
                        ("partition", intern(&tp.partition.to_string())),
                    ],
                );
            }
            part.position = Position::At(read.position);
            if read.records.is_empty() {
                part.tracker.mark(from..read.position);
            } else {
                part.buffer = Some(Fetched {
                    tp: tp.clone(),
                    epoch: part.epoch,
                    records: read.records,
                    span: from..read.position,
                });
                filled = true;
            }
        }
    }
    drop(state);
    if filled {
        wakes.data.notify_waiters();
    }
    Ok(())
}

/// What reading one partition's records blob gave.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Read {
    pub(crate) records: Vec<Record>,
    /// The next offset to fetch.
    pub(crate) position: i64,
    /// Set when a bad batch at this base offset is to be refetched once.
    pub(crate) corrupt_at: Option<i64>,
    /// Set when the refetch for a bad batch at this base offset is to be widened.
    pub(crate) widen_at: Option<i64>,
    /// `(reason, records, base offset)` of each skipped batch.
    pub(crate) dropped: Vec<(&'static str, i64, i64)>,
    pub(crate) aborted: i64,
}

/// What the answer shows after a batch at `base` that failed twice.
struct AfterCorrupt {
    base: i64,
    /// The base of the first later entry whose CRC passed.
    verified: Option<i64>,
    /// The base in the first 12 bytes of a batch the answer cut short.
    partial: Option<i64>,
    /// Whether this answer was the widened refetch for `base`.
    widened: bool,
}

impl AfterCorrupt {
    /// Skips the batch, or asks for one widened refetch: `None` when `out` is to be returned as
    /// it stands, else whether the skip landed on the verified base. The skip lands on the
    /// verified base, else on the partial base, each only within `(base, end]`; with neither, and
    /// offsets past `base + 1` that a batch could hold, the partition is fetched once more with
    /// `max_batch_bytes` of room so the next batch arrives whole beside this one; after that, the
    /// skip lands on `end`. It never passes `end` when the answer has one, and always moves past
    /// the position.
    fn skip(self, out: &mut Read, end: i64) -> Option<bool> {
        let Self { base, verified, partial, widened } = self;
        let within = |b: &i64| *b > base && (end < 0 || *b <= end);
        let (next, on_verified) = match (verified.filter(within), partial.filter(within)) {
            (Some(next), _) => (next, true),
            (None, Some(next)) => (next, false),
            (None, None) if !widened && end > base + 1 => {
                out.corrupt_at = Some(base);
                out.widen_at = Some(base);
                return None;
            }
            (None, None) => (end, false),
        };
        let next = next.max(out.position + 1);
        out.dropped.push(("corrupt", next - out.position, base));
        out.position = next;
        Some(on_verified)
    }
}

/// Reads a partition's records blob from offset `from`. `aborted` is the answer's aborted
/// transactions, `(producer id, first offset)`, empty under `read_uncommitted`; `corrupt_at` is
/// the base offset of a batch that already failed once, and `widened_at` the one this answer's
/// widened refetch was for; `end` is the answer's high watermark, or its last stable offset under
/// `read_committed`.
///
/// A corrupt batch's skip reads nothing from its header but the base offset, since no CRC covers
/// its length and the CRC failed over `last_offset_delta` and `record_count`
/// ([`AfterCorrupt::skip`]). A batch whose length passes the blob's end at `from` is corrupt the
/// same way. Each skip counts the offsets it passes.
pub(crate) fn read(
    blob: &Bytes,
    from: i64,
    aborted: &[(i64, i64)],
    max_batch_bytes: usize,
    (corrupt_at, widened_at): (Option<i64>, Option<i64>),
    end: i64,
) -> Read {
    let mut pending: Vec<(i64, i64)> = aborted.to_vec();
    pending.sort_by_key(|&(_, first)| first);
    let mut pending = pending.into_iter().peekable();
    let mut aborting: HashSet<i64> = HashSet::new();
    let mut out = Read { position: from, ..Read::default() };
    let decoded = logit_proto::kafka::decode(blob, max_batch_bytes);
    let partial_base = decoded.partial_base;
    let entries = decoded.entries;
    // A batch whose length passes the blob's end, at the position: the broker returns the first
    // batch whole, so its length field is corrupt and nothing after it can be found.
    if entries.is_empty() {
        if let Some(base) = partial_base.filter(|&b| b <= from) {
            if corrupt_at != Some(base) {
                out.corrupt_at = Some(base);
                return out;
            }
            let widened = widened_at == Some(base);
            AfterCorrupt { base, verified: None, partial: None, widened }.skip(&mut out, end);
            return out;
        }
    }
    // Only a CRC-verified entry shows decoding is aligned with the batches again.
    let verified: Vec<Option<i64>> =
        entries.iter().map(|e| e.as_ref().ok().map(|b| b.base_offset)).collect();
    let count = entries.len();
    // Entries before this index follow a corrupt batch with no verified entry between: decoding
    // may have resumed inside its bytes, so their headers mean nothing.
    let mut resume = 0;
    for (i, entry) in entries.into_iter().enumerate() {
        if i < resume {
            continue;
        }
        let batch = match entry {
            Ok(batch) => batch,
            Err(err) if err.kind == BatchErrorKind::Corrupt => {
                if corrupt_at != Some(err.base_offset) {
                    out.corrupt_at = Some(err.base_offset);
                    return out;
                }
                let base = err.base_offset;
                let next_ok = (i + 1..count).find(|&j| verified[j].is_some());
                let after = AfterCorrupt {
                    base,
                    verified: next_ok.and_then(|j| verified[j]),
                    partial: partial_base,
                    widened: widened_at == Some(base),
                };
                match after.skip(&mut out, end) {
                    None => return out,
                    Some(true) => resume = next_ok.unwrap_or(count),
                    Some(false) => resume = count,
                }
                continue;
            }
            Err(err) => {
                let last = err.last_offset.unwrap_or(err.base_offset).max(err.base_offset);
                let next = last + 1;
                if next <= out.position {
                    continue;
                }
                let reason = match err.kind {
                    BatchErrorKind::Oversize => "oversize",
                    _ => "legacy_format",
                };
                // A legacy entry declares no records: its offsets stand in for them.
                let records = match err.record_count {
                    n if n > 0 => i64::from(n),
                    _ => next - out.position,
                };
                out.dropped.push((reason, records, err.base_offset));
                out.position = next;
                continue;
            }
        };
        let last = batch.last_offset();
        if last < out.position {
            continue;
        }
        if !aborted.is_empty() && batch.producer_id >= 0 {
            while let Some(&(pid, _)) = pending.peek().filter(|&&(_, first)| first <= last) {
                aborting.insert(pid);
                pending.next();
            }
            if batch.is_control() {
                if is_abort_marker(&batch) {
                    aborting.remove(&batch.producer_id);
                }
            } else if batch.is_transactional() && aborting.contains(&batch.producer_id) {
                out.aborted +=
                    batch.records.iter().filter(|r| r.offset >= out.position).count() as i64;
                out.position = last + 1;
                continue;
            }
        }
        if !batch.is_control() {
            let from = out.position;
            out.records.extend(batch.records.into_iter().filter(|r| r.offset >= from));
        }
        out.position = last + 1;
    }
    out
}

/// Whether a control batch holds an abort marker: a control record whose key is version `i16`
/// then type `i16`, type 0 ABORT (`ControlRecordType`).
fn is_abort_marker(batch: &logit_proto::kafka::RecordBatch) -> bool {
    batch
        .records
        .first()
        .and_then(|r| r.key.as_ref())
        .is_some_and(|k| k.len() >= 4 && k[2..4] == [0, 0])
}

fn count_read(pool: &Pool, tp: &TopicPartition, read: &Read) {
    let counters = &pool.counters;
    for &(reason, records, base) in &read.dropped {
        counters.count("logit.input.records", records as f64, &[]);
        counters.count("logit.input.records.dropped", records as f64, &[("reason", reason)]);
        counters.warn(
            "records_dropped",
            format_args!("Kafka {tp}: skipped the record batch at offset {base} ({reason})"),
        );
    }
    if read.aborted > 0 {
        counters.count("logit.input.records", read.aborted as f64, &[]);
        counters.count(
            "logit.input.records.skipped",
            read.aborted as f64,
            &[("reason", "aborted")],
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use kafka_protocol::records::Compression;
    use logit_proto::kafka::batch::{encode, ProducerRecord, Stamp};

    /// One batch of `n` records at `base`, its CRC valid.
    fn batch_at(base: i64, n: usize) -> Vec<u8> {
        let records: Vec<ProducerRecord> = (0..n)
            .map(|i| ProducerRecord {
                key: None,
                value: Some(Bytes::from(format!("value {i}"))),
                headers: Default::default(),
                timestamp: 1_000,
            })
            .collect();
        let mut raw = encode(&records, Stamp::NONE, Compression::None).unwrap().to_vec();
        raw[..8].copy_from_slice(&base.to_be_bytes());
        raw
    }

    fn with_length(mut raw: Vec<u8>, length: i32) -> Vec<u8> {
        raw[8..12].copy_from_slice(&length.to_be_bytes());
        raw
    }

    fn length(raw: &[u8]) -> i32 {
        i32::from_be_bytes(raw[8..12].try_into().unwrap())
    }

    /// `read` for a batch at 0 that already failed and had its widened refetch.
    fn read_final(blob: Vec<u8>, end: i64) -> Read {
        read(&Bytes::from(blob), 0, &[], 1 << 20, (Some(0), Some(0)), end)
    }

    /// A shrunk length makes decoding resume inside the failed batch's records, where any header
    /// is garbage: the skip may not land on one, nor past `end`.
    #[test]
    fn a_shrunk_length_never_lands_on_an_offset_decoded_inside_the_batch() {
        let bad = batch_at(0, 3);
        for shrink in [8, 20, 40] {
            let bad = with_length(bad.clone(), length(&bad) - shrink);
            let blob = [bad.clone(), batch_at(3, 1)].concat();
            let read = read_final(blob, 4);
            assert!((1..=4).contains(&read.position), "shrink {shrink}: {}", read.position);
            let dropped: i64 = read.dropped.iter().map(|d| d.1).sum();
            assert_eq!(dropped + read.records.len() as i64, read.position, "shrink {shrink}");
        }
    }

    /// An entry after the failed batch whose own CRC fails names a base inside the failed batch's
    /// offsets, as one decoded from a misaligned position does: only the verified batch after it
    /// is a landing.
    #[test]
    fn only_a_crc_verified_entry_is_a_landing() {
        let mut bad = batch_at(0, 3);
        bad[17] ^= 1;
        let mut garbage = batch_at(2, 1);
        garbage[17] ^= 1;
        let read = read_final([bad, garbage, batch_at(3, 1)].concat(), 4);
        assert_eq!(read.position, 4);
        assert_eq!(read.records.len(), 1);
        assert_eq!(read.dropped, vec![("corrupt", 3, 0)]);
    }

    /// A length grown past the answer leaves no entry at all: the batch at the position is
    /// refetched, refetched widened, then skipped to `end`, counted.
    #[test]
    fn a_length_past_the_answer_at_the_position_is_corrupt() {
        let bad = batch_at(0, 3);
        let blob =
            Bytes::from([with_length(bad.clone(), length(&bad) + 1_000), batch_at(3, 1)].concat());
        let first = read(&blob, 0, &[], 1 << 20, (None, None), 4);
        assert_eq!((first.position, first.corrupt_at, first.widen_at), (0, Some(0), None));
        let second = read(&blob, 0, &[], 1 << 20, (Some(0), None), 4);
        assert_eq!((second.position, second.widen_at), (0, Some(0)));
        assert!(second.dropped.is_empty());
        let last = read(&blob, 0, &[], 1 << 20, (Some(0), Some(0)), 4);
        assert_eq!(last.position, 4);
        assert_eq!(last.dropped, vec![("corrupt", 4, 0)]);
    }

    /// A cut batch's base counts only within the failed batch's base and `end`.
    #[test]
    fn a_partial_base_outside_the_range_lands_on_end() {
        let mut bad = batch_at(0, 3);
        bad[17] ^= 1;
        for (partial, landing) in [(0, 10), (-5, 10), (11, 10), (5, 5)] {
            let cut = batch_at(partial, 1)[..20].to_vec();
            let read = read_final([bad.clone(), cut].concat(), 10);
            assert_eq!(read.position, landing, "partial base {partial}");
            assert_eq!(read.dropped, vec![("corrupt", landing, 0)], "partial base {partial}");
        }
    }

    /// A verified entry past `end` is no landing: the skip lands on a cut batch's base within the
    /// range, else on `end`.
    #[test]
    fn a_verified_base_past_end_is_no_landing() {
        let mut bad = batch_at(0, 3);
        bad[17] ^= 1;
        let cut = batch_at(5, 1)[..20].to_vec();
        for (blob, landing) in [
            ([bad.clone(), batch_at(20, 1), cut].concat(), 5),
            ([bad.clone(), batch_at(20, 1)].concat(), 10),
        ] {
            let read = read_final(blob, 10);
            assert_eq!(read.position, landing);
            assert_eq!(read.dropped, vec![("corrupt", landing, 0)]);
            assert!(read.records.is_empty(), "{:?}", read.records);
        }
    }

    /// The widened refetch for a lone failed batch: with nothing verified after it, and offsets
    /// left before `end`, the second failure asks for room rather than skipping to `end`.
    #[test]
    fn a_lone_failed_batch_is_refetched_widened_before_skipping_to_end() {
        let mut bad = batch_at(0, 3);
        bad[17] ^= 1;
        let blob = Bytes::from(bad);
        let second = read(&blob, 0, &[], 1 << 20, (Some(0), None), 10);
        assert_eq!((second.position, second.widen_at, second.corrupt_at), (0, Some(0), Some(0)));
        let last = read(&blob, 0, &[], 1 << 20, (Some(0), Some(0)), 10);
        assert_eq!((last.position, last.widen_at), (10, None));
        // With no offset past the batch's own, there's nothing to widen for.
        let alone = read(&blob, 0, &[], 1 << 20, (Some(0), None), 1);
        assert_eq!((alone.position, alone.widen_at), (1, None));
    }

    fn part(position: i64, committed: i64) -> Part {
        Part::new(Position::At(position), Tracker::new(position, Some(committed)))
    }

    /// A range handed out before a reset stays markable below the reset point, so the commit it
    /// held back can move once the reset's own records are done.
    #[test]
    fn a_range_from_before_a_reset_is_cut_at_the_reset_point() {
        let mut p = part(2, 2);
        let before = p.epoch;
        p.reset(3);
        assert_eq!(p.tracker.candidate(), 2);
        p.accept(before, 2..6);
        assert_eq!(p.tracker.candidate(), 3);
        p.accept(p.epoch, 3..4);
        assert_eq!(p.tracker.candidate(), 4);
    }

    /// Two resets: a range from between them is cut at the second's point, and one from before
    /// both at the lower of the two.
    #[test]
    fn each_epoch_is_cut_at_the_lowest_reset_since_it() {
        let mut p = part(0, 0);
        let first = p.epoch;
        p.reset(8);
        let second = p.epoch;
        p.reset(10);
        p.accept(second, 8..12);
        assert_eq!(p.tracker.candidate(), 0);
        p.accept(first, 0..9);
        assert_eq!(p.tracker.candidate(), 10);

        let mut p = part(0, 0);
        let first = p.epoch;
        p.reset(8);
        p.reset(4);
        p.accept(first, 0..9);
        assert_eq!(p.tracker.candidate(), 4);
    }

    /// Even after a reset, whose cut would otherwise apply to every older epoch.
    #[test]
    fn a_range_from_another_assignment_is_ignored() {
        let old = part(0, 0);
        let mut p = part(0, 0);
        p.reset(5);
        p.accept(old.epoch, 0..5);
        assert_eq!(p.tracker.candidate(), 0);
    }
}
