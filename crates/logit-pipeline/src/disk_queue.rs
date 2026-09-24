//! A disk-backed, crash-recoverable alternative to [`crate::queue::SinkQueue`]
//! (`docs/adr/disk-backed-sink-buffer.md`). [`DiskQueue`] has `SinkQueue`'s
//! `push`/`peek`/`commit`/`close` shape (both sit behind `crate::queue::SinkStore`), but every
//! batch is appended to a segment file before it is eligible for delivery, and a restart resumes
//! from the last checkpointed read cursor.
//!
//! **On-disk layout.** `<dir>/` holds:
//!
//! - `segment-<seq:016>.lgit` files, each a plain concatenation of records, oldest lowest.
//! - `cursor.json`, the read cursor (segment and byte offset), written tmp+rename.
//! - `lock`, held with `std::fs::File::try_lock` for the queue's lifetime. The OS releases it on
//!   any exit, `SIGKILL` included, so a restart needs no stale-lock cleanup.
//!
//! A **record** is 24 raw bytes of [`TraceContext`] (16-byte `trace_id`, 8-byte `span_id`;
//! unversioned, see `CONTEXT_LEN`) followed by one `logit_proto::frame` native frame. The frame's
//! codec byte (`CODEC_NATIVE_V1` or `_V2`) tells [`parse_record`] whether a
//! [`logit_core::Provenance`] trailer follows the batch. `frame::resync` can recover past a
//! corrupt record because `MAGIC` always immediately follows a record's 24 context bytes.
//!
//! **Only the read cursor is persisted.** The write side resumes at the end of the
//! highest-numbered segment, validated frame by frame at [`DiskQueue::open`]. Every other segment
//! was complete before a newer one became active (one producer, one write in flight), so only the
//! active segment can have a torn tail.
//!
//! **`commit` is synchronous**, like `SinkQueue::commit`: it mutates in-memory cursor state and
//! occasionally makes one small blocking cursor write and one file deletion, never a segment read
//! or write.

use std::collections::VecDeque;
use std::fs::File as StdFile;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::fanout::{BatchContext, TraceContext};
use crate::queue::{OverflowPolicy, SINK_QUEUE_METRICS};
use logit_core::{Diagnostics, EventBatch, Provenance, Telemetry};
use logit_proto::frame::{self, Compression, MAX_SANE_UNCOMPRESSED_LEN};
use logit_proto::native;
use logit_proto::CodecError;

/// `[trace_id: 16][span_id: 8]`, ahead of the frame. Never widen it: a record carries no version,
/// so a wider prefix would misparse every already-spooled record. Anything new rides inside the
/// frame under a new codec byte, as `Provenance` does with `CODEC_NATIVE_V2`: a `V1` record still
/// replays with empty provenance, and a binary that doesn't know `V2` resyncs past it
/// (`docs/adr/batch-provenance-on-delivered.md`).
const CONTEXT_LEN: usize = 24;

const LOCK_FILE_NAME: &str = "lock";
const CURSOR_FILE_NAME: &str = "cursor.json";
const CURSOR_VERSION: u32 = 1;

/// The first read size for an uncached peek: larger than any fixture batch
/// (`docs/design/memory.md`), so the common case is one disk read. A record that doesn't fit grows
/// the read by [`CodecError::Truncated`]'s `needed` hint rather than reading the whole segment.
const READ_CHUNK_INITIAL: usize = 8 * 1024;

const DISK_SEGMENTS: &str = "logit.component.buffer.disk.segments";
const DISK_REPLAYED: &str = "logit.component.buffer.disk.replayed";
const DISK_TRUNCATED: &str = "logit.component.buffer.disk.truncated";

/// Bounds and behavior for one sink's disk spool. Disk replaces the in-memory queue, so there is
/// no `max_batches`: `max_bytes` alone bounds the sum of segment sizes.
#[derive(Debug, Clone)]
pub struct DiskQueueConfig {
    pub dir: PathBuf,
    pub max_bytes: u64,
    pub segment_bytes: u64,
    pub overflow: OverflowPolicy,
    pub compression: Compression,
    pub checkpoint_interval: Duration,
}

fn encode_context(ctx: TraceContext) -> [u8; CONTEXT_LEN] {
    let mut buf = [0u8; CONTEXT_LEN];
    buf[..16].copy_from_slice(&ctx.trace_id);
    buf[16..].copy_from_slice(&ctx.span_id);
    buf
}

fn decode_context(buf: &[u8]) -> TraceContext {
    let mut trace_id = [0u8; 16];
    let mut span_id = [0u8; 8];
    trace_id.copy_from_slice(&buf[..16]);
    span_id.copy_from_slice(&buf[16..CONTEXT_LEN]);
    TraceContext { trace_id, span_id }
}

fn segment_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("segment-{seq:016}.lgit"))
}

/// Every `segment-<seq>.lgit` in `dir`, ascending by sequence number. Other files are ignored.
fn list_segments(dir: &Path) -> io::Result<Vec<u64>> {
    let mut seqs = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(seq_str) = name.strip_prefix("segment-").and_then(|s| s.strip_suffix(".lgit")) {
            if let Ok(seq) = seq_str.parse::<u64>() {
                seqs.push(seq);
            }
        }
    }
    seqs.sort_unstable();
    Ok(seqs)
}

/// Parses one record (`[24-byte trace context][native frame]`) off the front of `buf`, returning
/// its context, batch, and byte length. `Truncated` means `buf` doesn't hold a whole record yet;
/// any other error means the bytes are wrong and the caller should resync
/// (`docs/design/wire-protocol.md`).
///
/// `CODEC_NATIVE_V1` decodes with `Provenance::default()`, `CODEC_NATIVE_V2` with its trailer;
/// any other codec byte is an error.
fn parse_record(buf: &[u8]) -> Result<(BatchContext, Arc<EventBatch>, usize), CodecError> {
    if buf.len() < CONTEXT_LEN {
        return Err(CodecError::Truncated { needed: CONTEXT_LEN - buf.len() });
    }
    let trace = decode_context(&buf[..CONTEXT_LEN]);
    let mut rest = Bytes::copy_from_slice(&buf[CONTEXT_LEN..]);
    let before = rest.len();
    let (codec, mut payload) = frame::read_frame(&mut rest)?;
    let (batch, provenance) = match codec {
        native::CODEC_NATIVE_V1 => (native::decode_batch(&mut payload)?, Provenance::default()),
        native::CODEC_NATIVE_V2 => native::decode_batch_v2(&mut payload)?,
        other => {
            return Err(CodecError::Malformed(format!(
                "disk record declares codec {other}, expected native v1 ({}) or v2 ({})",
                native::CODEC_NATIVE_V1,
                native::CODEC_NATIVE_V2
            )))
        }
    };
    let consumed_frame = before - rest.len();
    let ctx = BatchContext { trace, provenance };
    Ok((ctx, Arc::new(batch), CONTEXT_LEN + consumed_frame))
}

/// The result of walking every record in a byte range.
struct WalkOutcome {
    /// Segment offset up to which data is confirmed good. Everything after it is a torn tail or
    /// corruption with no further resync target.
    good_len: u64,
    valid_count: u64,
    corrupt_skipped: u64,
}

/// Walks every record in `bytes` from `start_offset` (`bytes[0]` is the segment's byte 0),
/// calling `on_record(offset, ctx, batch, len)` for each clean one.
///
/// Stops at the first short read. A torn tail and a clean end look the same here; the caller
/// tells them apart by comparing `good_len` with the file's length. Any other parse failure
/// resyncs forward with `frame::resync`, backing up over the 24-byte context prefix it doesn't
/// know about. A spurious `MAGIC` match (say, inside a `trace_id`) is tried and skipped if it
/// doesn't parse.
fn walk_segment(
    bytes: &[u8],
    start_offset: u64,
    mut on_record: impl FnMut(u64, BatchContext, Arc<EventBatch>, u64),
) -> WalkOutcome {
    let mut pos = start_offset as usize;
    let mut valid_count = 0u64;
    let mut corrupt_skipped = 0u64;
    loop {
        if pos >= bytes.len() {
            break;
        }
        match parse_record(&bytes[pos..]) {
            Ok((ctx, batch, consumed)) => {
                on_record(pos as u64, ctx, batch, consumed as u64);
                valid_count += 1;
                pos += consumed;
            }
            Err(CodecError::Truncated { .. }) => break,
            Err(_corrupt) => {
                let mut scan_from = pos + 1;
                let mut recovered = false;
                while scan_from < bytes.len() {
                    match frame::resync(&bytes[scan_from..]) {
                        Some(rel) => {
                            let magic_at = scan_from + rel;
                            if magic_at >= CONTEXT_LEN {
                                let candidate = magic_at - CONTEXT_LEN;
                                if parse_record(&bytes[candidate..]).is_ok() {
                                    corrupt_skipped += 1;
                                    pos = candidate;
                                    recovered = true;
                                    break;
                                }
                            }
                            scan_from = magic_at + 1;
                        }
                        None => break,
                    }
                }
                if !recovered {
                    corrupt_skipped += 1;
                    pos = bytes.len();
                    break;
                }
            }
        }
    }
    WalkOutcome { good_len: pos as u64, valid_count, corrupt_skipped }
}

#[derive(Serialize, Deserialize)]
struct CursorFile {
    version: u32,
    segment: u64,
    offset: u64,
}

/// Loads the persisted read cursor. Missing is the ordinary first run (no diagnostic); a version
/// mismatch or malformed file is diagnosed and treated as absent, never fatal.
fn load_cursor(dir: &Path, diag: &mut Diagnostics) -> Option<(u64, u64)> {
    let path = dir.join(CURSOR_FILE_NAME);
    match std::fs::read(&path) {
        Ok(bytes) => match serde_json::from_slice::<CursorFile>(&bytes) {
            Ok(cursor) if cursor.version == CURSOR_VERSION => Some((cursor.segment, cursor.offset)),
            Ok(cursor) => {
                diag.warn_throttled(
                    "cursor_error",
                    format!(
                        "unsupported cursor version {} in {} -- resuming from the oldest segment",
                        cursor.version,
                        path.display()
                    ),
                );
                None
            }
            Err(err) => {
                diag.warn_throttled(
                    "cursor_error",
                    format!("malformed cursor at {}: {err}", path.display()),
                );
                None
            }
        },
        Err(err) if err.kind() == io::ErrorKind::NotFound => None,
        Err(err) => {
            diag.warn_throttled(
                "cursor_error",
                format!("reading cursor {}: {err}", path.display()),
            );
            None
        }
    }
}

/// Persists the read cursor via tmp+rename.
fn persist_cursor(dir: &Path, segment: u64, offset: u64, diag: &mut Diagnostics) {
    let path = dir.join(CURSOR_FILE_NAME);
    let doc = CursorFile { version: CURSOR_VERSION, segment, offset };
    let bytes = match serde_json::to_vec(&doc) {
        Ok(bytes) => bytes,
        Err(err) => {
            diag.warn_throttled("cursor_error", format!("encoding cursor: {err}"));
            return;
        }
    };
    let tmp = path.with_extension("tmp");
    let result = std::fs::write(&tmp, &bytes).and_then(|()| std::fs::rename(&tmp, &path));
    if let Err(err) = result {
        diag.warn_throttled("cursor_error", format!("writing cursor {}: {err}", path.display()));
    }
}

async fn fsync_path(path: &Path) -> io::Result<()> {
    tokio::fs::File::open(path).await?.sync_data().await
}

/// Whether `err` is `ENOSPC` (errno 28 on Linux, the only platform `logit` targets), checked on
/// the raw errno rather than relying on `io::ErrorKind::StorageFull`'s mapping.
fn is_disk_full(err: &io::Error) -> bool {
    err.raw_os_error() == Some(28)
}

#[derive(Clone, Copy)]
struct Segment {
    seq: u64,
    /// The confirmed-good length: a closed segment's on-disk size (only the active segment can
    /// be torn), or the active segment's length validated at [`DiskQueue::open`] and advanced by
    /// each successful [`DiskQueue::push`].
    len: u64,
}

struct HeadCache {
    batch: Arc<EventBatch>,
    ctx: BatchContext,
    record_len: u64,
}

struct State {
    segments: VecDeque<Segment>,
    read_seq: u64,
    read_offset: u64,
    total_bytes: u64,
    queued_records: u64,
    head_cache: Option<HeadCache>,
    write_file: Option<tokio::fs::File>,
    /// Set just before a write, cleared once it completes. A `push` future dropped mid-write
    /// (`run_output`'s `select!` drops `drain_inbox` when `write_loop` finishes first) or a
    /// failed write leaves it `true`, and the next [`DiskQueue::push`] truncates the tail back to
    /// `write_len_before_flight` before writing. A crash is the same case, repaired at the next
    /// [`DiskQueue::open`].
    write_in_flight: bool,
    write_len_before_flight: u64,
    /// Whether the last failed write was `ENOSPC`, so `push` can tag the drop it counts.
    last_write_error_disk_full: bool,
    read_file: Option<(u64, tokio::fs::File)>,
    last_checkpoint: Instant,
    diag: Diagnostics,
}

/// A disk-backed sink queue. See the module doc for the on-disk layout and
/// `docs/adr/disk-backed-sink-buffer.md` for the design decisions.
pub struct DiskQueue {
    dir: PathBuf,
    max_bytes: u64,
    segment_bytes: u64,
    overflow: OverflowPolicy,
    compression: Compression,
    checkpoint_interval: Duration,
    inner: Mutex<State>,
    not_empty: tokio::sync::Notify,
    not_full: tokio::sync::Notify,
    closed: AtomicBool,
    telemetry: Telemetry,
    /// An OS advisory lock held for the queue's lifetime. Catches two sinks sharing a spool
    /// through an aliased path (`./spool` vs `spool`); graph validation rule 35 compares only
    /// literal `disk.path` strings.
    _lock: StdFile,
}

impl DiskQueue {
    /// Opens or creates the spool at `config.dir`, recovering whatever a previous process left:
    /// truncates a torn active segment, clamps a stale cursor, and counts replayed and corrupt
    /// records. Fails if another process holds the lock. Blocking (`std::fs`): it runs once,
    /// before delivery starts.
    pub fn open(
        config: DiskQueueConfig,
        telemetry: Telemetry,
        mut diag: Diagnostics,
    ) -> anyhow::Result<Self> {
        use anyhow::Context;

        std::fs::create_dir_all(&config.dir)
            .with_context(|| format!("creating disk buffer directory {}", config.dir.display()))?;

        let lock_path = config.dir.join(LOCK_FILE_NAME);
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .with_context(|| format!("opening lock file {}", lock_path.display()))?;
        match lock_file.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => anyhow::bail!(
                "disk buffer directory {} is already in use by another component or process -- \
                 two sinks must not share a `disk.path`",
                config.dir.display()
            ),
            Err(std::fs::TryLockError::Error(err)) => {
                return Err(err).with_context(|| format!("locking {}", lock_path.display()))
            }
        }

        let seqs = list_segments(&config.dir)
            .with_context(|| format!("listing segments in {}", config.dir.display()))?;

        let mut replayed = 0u64;
        let mut corrupt_skipped_total = 0u64;
        let mut truncated = false;

        let (segments, read_seq, read_offset): (VecDeque<Segment>, u64, u64) = if seqs.is_empty() {
            let path = segment_path(&config.dir, 0);
            std::fs::File::create(&path)
                .with_context(|| format!("creating segment {}", path.display()))?;
            (VecDeque::from([Segment { seq: 0, len: 0 }]), 0, 0)
        } else {
            let mut segments = VecDeque::new();
            let active_seq = *seqs.last().expect("checked non-empty above");
            for &seq in &seqs {
                let path = segment_path(&config.dir, seq);
                let on_disk_len = std::fs::metadata(&path)
                    .with_context(|| format!("stat-ing segment {}", path.display()))?
                    .len();
                if seq == active_seq {
                    let bytes = std::fs::read(&path)
                        .with_context(|| format!("reading segment {}", path.display()))?;
                    let outcome = walk_segment(&bytes, 0, |_, _, _, _| {});
                    if outcome.good_len < on_disk_len {
                        std::fs::File::options()
                            .write(true)
                            .open(&path)
                            .and_then(|f| f.set_len(outcome.good_len))
                            .with_context(|| {
                                format!("truncating torn segment {}", path.display())
                            })?;
                        truncated = true;
                    }
                    segments.push_back(Segment { seq, len: outcome.good_len });
                } else {
                    segments.push_back(Segment { seq, len: on_disk_len });
                }
            }
            let oldest_seq = seqs[0];

            let cursor = load_cursor(&config.dir, &mut diag);
            let (mut cur_seq, mut cur_offset) = cursor.unwrap_or((oldest_seq, 0));
            if !seqs.contains(&cur_seq) || cur_seq < oldest_seq {
                if cursor.is_some() {
                    diag.warn_throttled(
                        "cursor_error",
                        format!(
                            "cursor referenced segment {cur_seq}, which no longer exists -- \
                             resuming from the oldest surviving segment {oldest_seq}"
                        ),
                    );
                }
                cur_seq = oldest_seq;
                cur_offset = 0;
            } else {
                let seg_len = segments.iter().find(|s| s.seq == cur_seq).expect("just checked").len;
                if cur_offset > seg_len {
                    diag.warn_throttled(
                        "cursor_error",
                        format!(
                            "cursor offset {cur_offset} in segment {cur_seq} is past its \
                             recovered length {seg_len} -- clamping"
                        ),
                    );
                    cur_offset = seg_len;
                }
            }
            (segments, cur_seq, cur_offset)
        };

        // Replay count: walk every record from the resume point to the end of the spool.
        {
            let mut first = true;
            for seg in segments.iter() {
                if seg.seq < read_seq {
                    continue;
                }
                let start = if first { read_offset } else { 0 };
                first = false;
                let path = segment_path(&config.dir, seg.seq);
                let bytes = std::fs::read(&path)
                    .with_context(|| format!("reading segment {}", path.display()))?;
                let usable = bytes.len().min(seg.len as usize);
                let outcome = walk_segment(&bytes[..usable], start, |_, _, _, _| {});
                replayed += outcome.valid_count;
                corrupt_skipped_total += outcome.corrupt_skipped;
            }
        }

        if truncated {
            telemetry.count(DISK_TRUNCATED, 1.0, &[]);
        }
        if corrupt_skipped_total > 0 {
            telemetry.count(
                SINK_QUEUE_METRICS.items_dropped,
                corrupt_skipped_total as f64,
                &[("reason", "disk_corrupt")],
            );
            telemetry.count(
                SINK_QUEUE_METRICS.units_dropped,
                corrupt_skipped_total as f64,
                &[("reason", "disk_corrupt")],
            );
        }
        if replayed > 0 {
            telemetry.count(DISK_REPLAYED, replayed as f64, &[]);
        }

        // Persist any clamping above, regardless of `checkpoint_interval`.
        persist_cursor(&config.dir, read_seq, read_offset, &mut diag);

        let total_bytes: u64 = segments.iter().map(|s| s.len).sum();
        let segment_count = segments.len();
        let write_len = segments.back().expect("always at least one segment").len;

        let state = State {
            segments,
            read_seq,
            read_offset,
            total_bytes,
            queued_records: replayed,
            head_cache: None,
            write_file: None,
            write_in_flight: false,
            write_len_before_flight: write_len,
            last_write_error_disk_full: false,
            read_file: None,
            last_checkpoint: Instant::now(),
            diag,
        };

        let queue = Self {
            dir: config.dir,
            max_bytes: config.max_bytes,
            segment_bytes: config.segment_bytes,
            overflow: config.overflow,
            compression: config.compression,
            checkpoint_interval: config.checkpoint_interval,
            inner: Mutex::new(state),
            not_empty: tokio::sync::Notify::new(),
            not_full: tokio::sync::Notify::new(),
            closed: AtomicBool::new(false),
            telemetry,
            _lock: lock_file,
        };
        queue.update_gauges(total_bytes, replayed, segment_count);
        Ok(queue)
    }

    fn update_gauges(&self, total_bytes: u64, queued_records: u64, segment_count: usize) {
        self.telemetry.gauge(SINK_QUEUE_METRICS.depth, queued_records as f64, &[]);
        self.telemetry.gauge(SINK_QUEUE_METRICS.bytes, total_bytes as f64, &[]);
        let ratio =
            if self.max_bytes == 0 { 0.0 } else { total_bytes as f64 / self.max_bytes as f64 };
        self.telemetry.gauge(SINK_QUEUE_METRICS.utilization, ratio, &[]);
        self.telemetry.gauge(DISK_SEGMENTS, segment_count as f64, &[]);
    }

    fn count_dropped(&self, reason: &'static str, units: u64) {
        self.telemetry.count(SINK_QUEUE_METRICS.items_dropped, 1.0, &[("reason", reason)]);
        self.telemetry.count(SINK_QUEUE_METRICS.units_dropped, units as f64, &[("reason", reason)]);
    }

    fn after_change(&self) {
        let (total_bytes, queued_records, segment_count) = {
            let state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            (state.total_bytes, state.queued_records, state.segments.len())
        };
        self.update_gauges(total_bytes, queued_records, segment_count);
    }

    fn closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Appends `item` to the active segment, rotating first if the segment has reached
    /// `segment_bytes`. That is a soft trigger checked before writing, not a cap, so a record
    /// larger than it still lands whole.
    ///
    /// Applies the overflow policy against `max_bytes` first; a record larger than `max_bytes`,
    /// or a push to a closed queue, is written over-bound rather than blocked. A batch whose
    /// encoded payload exceeds `MAX_SANE_UNCOMPRESSED_LEN`, or whose write fails, is dropped and
    /// counted, never counted as queued.
    ///
    /// **Cancellation safety.** The record is encoded in memory first; the write-path `.await`s
    /// run under `write_in_flight` (see [`State::write_in_flight`]), so a dropped future leaves a
    /// tail the next push repairs.
    pub async fn push(&self, item: (Arc<EventBatch>, BatchContext)) {
        let (batch, ctx) = item;

        let payload = native::encode_batch_v2(&batch, ctx.provenance);
        if payload.len() > MAX_SANE_UNCOMPRESSED_LEN as usize {
            self.count_dropped("frame_too_large", batch.events.len() as u64);
            return;
        }
        // `write_frame` fails only for `Compression::Zstd`, which `logit_config::Compression`
        // (where `disk.compression` comes from) cannot express.
        let framed = frame::write_frame(native::CODEC_NATIVE_V2, self.compression, &payload)
            .expect("logit_config::Compression excludes Zstd; write_frame only fails for Zstd");
        let mut record = Vec::with_capacity(CONTEXT_LEN + framed.len());
        record.extend_from_slice(&encode_context(ctx.trace));
        record.extend_from_slice(&framed);
        let record_len = record.len() as u64;
        let events = batch.events.len() as u64;

        let impossible_to_ever_fit = record_len > self.max_bytes;
        let mut waited = false;
        let mut blocked_timer: Option<logit_core::telemetry::Timer> = None;

        enum Action {
            Write,
            Block,
            Evict,
            DropNewest,
        }

        loop {
            // Roll the cursor off a rotated-away segment before deciding what to evict (see
            // `roll_read_cursor`). Only under `DropOldest`, so the `Block` hot path
            // (`disk_queue_push_one_batch`) takes no extra lock.
            if matches!(self.overflow, OverflowPolicy::DropOldest) {
                self.roll_read_cursor();
            }
            let notified = self.not_full.notified();
            let action = {
                let state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
                let full = state.total_bytes + record_len > self.max_bytes;
                if !full || impossible_to_ever_fit || self.closed() {
                    Action::Write
                } else {
                    match self.overflow {
                        OverflowPolicy::Block => Action::Block,
                        OverflowPolicy::DropNewest => Action::DropNewest,
                        // Three outcomes. Accepting over-bound while the head is reserved would
                        // leave `disk.max_bytes` unenforced for most of a destination outage, the
                        // case the bound exists for.
                        OverflowPolicy::DropOldest => {
                            let seg_len = state
                                .segments
                                .iter()
                                .find(|s| s.seq == state.read_seq)
                                .map(|s| s.len);
                            if state.head_cache.is_some() {
                                // The head is reserved (peeked, mid-delivery). A file-backed
                                // FIFO can't evict behind it the way the in-memory buffer can,
                                // so reject the new push, as `DropNewest` would.
                                Action::DropNewest
                            } else if seg_len.map(|l| state.read_offset < l).unwrap_or(false) {
                                // Unreserved and something is queued: evict it.
                                Action::Evict
                            } else {
                                // Nothing queued. The spool can still read as full
                                // (`total_bytes` shrinks only when a whole segment is deleted),
                                // so dropping here would drop every future push. Accept
                                // over-bound instead.
                                Action::Write
                            }
                        }
                    }
                }
            };
            match action {
                Action::Write => break,
                Action::DropNewest => {
                    self.count_dropped("overflow_newest", events);
                    return;
                }
                Action::Evict => {
                    if !self.evict_oldest().await {
                        // A concurrent peek reserved the head, or it was consumed, since the
                        // check. Accept anyway.
                        break;
                    }
                }
                Action::Block => {
                    if !waited {
                        waited = true;
                        blocked_timer = Some(self.telemetry.timer(SINK_QUEUE_METRICS.push_blocked));
                    }
                    notified.await;
                }
            }
        }
        drop(blocked_timer);

        if !self.write_record(&record).await {
            // Never durably written, so count it dropped, not queued. `disk_full` is the one
            // cause the overflow policies can't prevent, so it gets its own reason.
            let reason =
                if self.last_write_error_was_disk_full() { "disk_full" } else { "disk_io_error" };
            self.count_dropped(reason, events);
            return;
        }

        {
            let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            let active = state.segments.back_mut().expect("always at least one segment");
            active.len += record_len;
            state.total_bytes += record_len;
            state.queued_records += 1;
        }
        self.not_empty.notify_one();
        self.after_change();
    }

    fn last_write_error_was_disk_full(&self) -> bool {
        let state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        state.last_write_error_disk_full
    }

    /// Repairs a torn tail left by a cancelled or failed write, rotates if the active segment has
    /// reached `segment_bytes`, then appends and flushes `record`. Returns whether the record
    /// reached the kernel.
    async fn write_record(&self, record: &[u8]) -> bool {
        let (needs_repair, repair_len, repair_seq) = {
            let state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            (
                state.write_in_flight,
                state.write_len_before_flight,
                state.segments.back().expect("always at least one segment").seq,
            )
        };
        if needs_repair {
            let path = segment_path(&self.dir, repair_seq);
            if let Ok(f) = tokio::fs::File::options().write(true).open(&path).await {
                let _ = f.set_len(repair_len).await;
            }
            let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(active) = state.segments.back_mut() {
                if active.seq == repair_seq {
                    let old_len = active.len;
                    active.len = repair_len;
                    state.total_bytes =
                        state.total_bytes.saturating_sub(old_len.saturating_sub(repair_len));
                }
            }
            state.write_in_flight = false;
            state.write_file = None;
        }

        let needs_rotate = {
            let state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            state.segments.back().expect("always at least one segment").len >= self.segment_bytes
        };
        if needs_rotate {
            self.rotate_segment().await;
        }

        let file_and_seq = {
            let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            let (seq, len) = {
                let active = state.segments.back().expect("always at least one segment");
                (active.seq, active.len)
            };
            state.write_in_flight = true;
            state.write_len_before_flight = len;
            state.write_file.take().map(|f| (f, seq))
        };
        let (mut file, seq) = match file_and_seq {
            Some(pair) => pair,
            None => {
                let seq = {
                    let state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
                    state.segments.back().expect("always at least one segment").seq
                };
                match self.open_append(seq).await {
                    Ok(f) => (f, seq),
                    Err(err) => {
                        let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
                        state.diag.warn_throttled(
                            "disk_io_error",
                            format!("opening segment {seq} for append: {err}"),
                        );
                        // Nothing was written, so there is no torn tail to repair.
                        state.write_in_flight = false;
                        state.last_write_error_disk_full = is_disk_full(&err);
                        return false;
                    }
                }
            }
        };

        // `write_all` returning `Ok` means only that the bytes reached `tokio::fs::File`'s
        // buffer; `flush()` hands them to the kernel. Without it, a batch counted as queued is
        // lost on an ordinary process crash, not only on power loss. A failed flush is repaired
        // like a failed write.
        let mut result = file.write_all(record).await;
        if result.is_ok() {
            result = file.flush().await;
        }

        let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        match result {
            Ok(()) => {
                state.write_file = Some(file);
                state.write_in_flight = false;
                true
            }
            Err(err) => {
                state.diag.warn_throttled("disk_io_error", format!("writing segment {seq}: {err}"));
                state.last_write_error_disk_full = is_disk_full(&err);
                // Leave `write_in_flight` set for the next push to repair.
                false
            }
        }
    }

    async fn open_append(&self, seq: u64) -> io::Result<tokio::fs::File> {
        tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(segment_path(&self.dir, seq))
            .await
    }

    /// Closes out the active segment (flush and `fsync` it, `fsync` the directory after creating
    /// the next) and starts a new one.
    async fn rotate_segment(&self) {
        let (old_seq, file) = {
            let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            let old_seq = state.segments.back().expect("always at least one segment").seq;
            (old_seq, state.write_file.take())
        };
        // Flush after the guard drops: awaiting under a `std::sync::Mutex` guard trips
        // `clippy::await_holding_lock`.
        if let Some(mut f) = file {
            let _ = f.flush().await;
        }
        let _ = fsync_path(&segment_path(&self.dir, old_seq)).await;
        let new_seq = old_seq + 1;
        let new_path = segment_path(&self.dir, new_seq);
        if tokio::fs::File::create(&new_path).await.is_ok() {
            let _ = fsync_path(&self.dir).await;
            let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            state.segments.push_back(Segment { seq: new_seq, len: 0 });
        }
    }

    /// `DropOldest` under a full queue: advances the read cursor past the head record without
    /// delivering it. After the async read, re-checks under the lock that the head is the same
    /// unreserved record, since a concurrent `peek` may have reserved it. Returns whether it
    /// evicted anything.
    async fn evict_oldest(&self) -> bool {
        let (seq, offset) = {
            let state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            if state.head_cache.is_some() {
                return false;
            }
            (state.read_seq, state.read_offset)
        };
        let Some((_ctx, batch, len)) = self.read_record_at(seq, offset).await else { return false };
        {
            let state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            if state.head_cache.is_some() || state.read_seq != seq || state.read_offset != offset {
                return false;
            }
        }
        self.advance_read_cursor(len);
        self.count_dropped("overflow_oldest", batch.events.len() as u64);
        {
            let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            state.queued_records = state.queued_records.saturating_sub(1);
        }
        self.not_full.notify_one();
        true
    }

    /// Reads one record at `(seq, offset)`, growing the read by [`CodecError::Truncated`]'s
    /// `needed` hint past [`READ_CHUNK_INITIAL`]. Returns the record and how far the cursor must
    /// advance to pass it. Resyncs past live corruption as defense in depth; the primary recovery
    /// is [`DiskQueue::open`].
    async fn read_record_at(
        &self,
        seq: u64,
        offset: u64,
    ) -> Option<(BatchContext, Arc<EventBatch>, u64)> {
        let mut chunk_len = READ_CHUNK_INITIAL;
        loop {
            let buf = match self.read_at(seq, offset, chunk_len).await {
                Ok(buf) => buf,
                Err(_) => return None,
            };
            if buf.is_empty() {
                return None;
            }
            match parse_record(&buf) {
                Ok((ctx, batch, consumed)) => return Some((ctx, batch, consumed as u64)),
                Err(CodecError::Truncated { needed }) => {
                    if buf.len() < chunk_len {
                        // End of segment.
                        return None;
                    }
                    chunk_len = buf.len() + needed;
                }
                Err(_corrupt) => {
                    // Live corruption: resync within what's readable of this segment.
                    let seg_len = {
                        let state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
                        state.segments.iter().find(|s| s.seq == seq).map(|s| s.len)
                    };
                    let seg_len = seg_len?;
                    let remaining = seg_len.saturating_sub(offset) as usize;
                    let whole = match self.read_at(seq, offset, remaining).await {
                        Ok(b) => b,
                        Err(_) => return None,
                    };
                    // The returned length is a delta from the read cursor, not the record's
                    // size: `pos` counts the corrupt bytes skipped before the record, so the
                    // delta is `pos + len`. `len` alone would land the cursor inside the record.
                    let mut found = None;
                    let outcome = walk_segment(&whole, 0, |pos, ctx, batch, len| {
                        if found.is_none() {
                            found = Some((ctx, batch, pos + len));
                        }
                    });
                    self.count_dropped("disk_corrupt", outcome.corrupt_skipped.max(1));
                    return found;
                }
            }
        }
    }

    async fn read_at(&self, seq: u64, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let cached = {
            let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            state.read_file.take()
        };
        let mut file = match cached {
            Some((cached_seq, f)) if cached_seq == seq => f,
            _ => tokio::fs::File::open(segment_path(&self.dir, seq)).await?,
        };
        file.seek(io::SeekFrom::Start(offset)).await?;
        let mut buf = vec![0u8; len];
        let mut filled = 0usize;
        while filled < buf.len() {
            let n = file.read(&mut buf[filled..]).await?;
            if n == 0 {
                buf.truncate(filled);
                break;
            }
            filled += n;
        }
        let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        state.read_file = Some((seq, file));
        Ok(buf)
    }

    /// While the segment `read_seq` names is fully consumed and is no longer the active one,
    /// moves the cursor to the next surviving segment (the next larger `seq`, since a failed
    /// deletion can leave a gap) and deletes the one left behind. Loops, so one call resolves an
    /// advance past several boundaries.
    ///
    /// `peek`, `push`'s `DropOldest` arm, and `advance_read_cursor` all call it. A reader that
    /// caught up to the writer while its segment was active has nothing else to re-evaluate
    /// `read_seq` once a later `push` rotates that segment away, and `peek` would wait on
    /// `not_empty` forever against a segment that never grows again.
    ///
    /// Persists the cursor once per roll, then deletes files and notifies `not_full` outside the
    /// lock. Allocates nothing when no boundary is crossed. Returns whether the cursor now points
    /// at readable bytes.
    fn roll_read_cursor(&self) -> bool {
        let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let mut to_delete: Vec<u64> = Vec::new();
        loop {
            let seq = state.read_seq;
            let offset = state.read_offset;
            let is_active = state.segments.back().map(|s| s.seq) == Some(seq);
            let Some(len) = state.segments.iter().find(|s| s.seq == seq).map(|s| s.len) else {
                break;
            };
            if is_active || offset < len {
                break;
            }
            let Some(next_seq) = state.segments.iter().map(|s| s.seq).find(|&s| s > seq) else {
                break;
            };
            to_delete.push(seq);
            state.read_seq = next_seq;
            state.read_offset = offset - len;
        }

        if !to_delete.is_empty() {
            persist_cursor(&self.dir, state.read_seq, state.read_offset, &mut state.diag);
            state.last_checkpoint = Instant::now();
            for &seq in &to_delete {
                if let Some(pos) = state.segments.iter().position(|s| s.seq == seq) {
                    if let Some(removed) = state.segments.remove(pos) {
                        state.total_bytes = state.total_bytes.saturating_sub(removed.len);
                    }
                }
                if let Some((cached_seq, _)) = &state.read_file {
                    if *cached_seq == seq {
                        state.read_file = None;
                    }
                }
            }
        }

        let seq = state.read_seq;
        let offset = state.read_offset;
        let readable =
            state.segments.iter().find(|s| s.seq == seq).map(|s| offset < s.len).unwrap_or(false);
        drop(state);
        // The no-crossing case must not allocate: it is on `peek`'s cached-hit path
        // (`disk_queue_peek_cached_costs_nothing`).
        if !to_delete.is_empty() {
            for seq in to_delete {
                let _ = std::fs::remove_file(segment_path(&self.dir, seq));
            }
            self.not_full.notify_one();
        }
        readable
    }

    /// Advances the read cursor past `record_len` bytes, clears the head reservation, rolls
    /// across any segment boundary crossed, and persists the cursor if `checkpoint_interval` has
    /// elapsed. Sync: its only I/O is a small cursor write and occasional file removals.
    fn advance_read_cursor(&self, record_len: u64) {
        {
            let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            state.head_cache = None;
            state.read_offset += record_len;
        }
        self.roll_read_cursor();
        let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if state.last_checkpoint.elapsed() >= self.checkpoint_interval {
            persist_cursor(&self.dir, state.read_seq, state.read_offset, &mut state.diag);
            state.last_checkpoint = Instant::now();
        }
    }

    /// The head, without removing it. Cached, and reserved against `DropOldest` eviction, until
    /// [`DiskQueue::commit`], so a retry (`write_loop` peeks once per delivery attempt) costs
    /// nothing after the first. `None` once closed and empty.
    pub async fn peek(&self) -> Option<(Arc<EventBatch>, BatchContext)> {
        loop {
            // Roll past a segment the reader finished while it was active and that has since
            // rotated away (see `roll_read_cursor`).
            self.roll_read_cursor();
            let (cached, seq, offset, has_data) = {
                let state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
                if let Some(cache) = &state.head_cache {
                    (Some((Arc::clone(&cache.batch), cache.ctx)), 0, 0, true)
                } else {
                    let seg_len =
                        state.segments.iter().find(|s| s.seq == state.read_seq).map(|s| s.len);
                    let available = seg_len.map(|l| state.read_offset < l).unwrap_or(false);
                    (None, state.read_seq, state.read_offset, available)
                }
            };
            if let Some(item) = cached {
                return Some(item);
            }
            if !has_data {
                if self.closed() {
                    return None;
                }
                let notified = self.not_empty.notified();
                self.roll_read_cursor();
                let still_nothing = {
                    let state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
                    let seg_len =
                        state.segments.iter().find(|s| s.seq == state.read_seq).map(|s| s.len);
                    !seg_len.map(|l| state.read_offset < l).unwrap_or(false)
                };
                if still_nothing && !self.closed() {
                    notified.await;
                }
                continue;
            }

            match self.read_record_at(seq, offset).await {
                Some((ctx, batch, record_len)) => {
                    let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
                    if state.read_seq == seq && state.read_offset == offset {
                        state.head_cache = Some(HeadCache { batch, ctx, record_len });
                    }
                }
                None => {
                    // Nothing readable where accounting said there should be (an I/O error, a
                    // short read, or corruption with no resync target). Back off briefly rather
                    // than spin, then re-evaluate.
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }
    }

    /// Advances the read cursor past the cached head, returning it. `None`, and a no-op, with
    /// nothing peeked.
    pub fn commit(&self) -> Option<(Arc<EventBatch>, BatchContext)> {
        let (item, record_len) = {
            let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            let cache = state.head_cache.take()?;
            state.queued_records = state.queued_records.saturating_sub(1);
            ((cache.batch, cache.ctx), cache.record_len)
        };
        self.advance_read_cursor(record_len);
        self.after_change();
        Some(item)
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.not_empty.notify_waiters();
        self.not_full.notify_waiters();
    }

    /// Persists the cursor regardless of `checkpoint_interval`, `fsync`s it, the active segment,
    /// and the directory, and closes files. Drops nothing: what is queued delivers after the next
    /// open.
    pub async fn finish(&self) {
        let (active_seq, file) = {
            let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            persist_cursor(&self.dir, state.read_seq, state.read_offset, &mut state.diag);
            let file = state.write_file.take();
            state.read_file = None;
            (state.segments.back().map(|s| s.seq), file)
        };
        // Flush outside the lock, as in `rotate_segment`.
        if let Some(mut f) = file {
            let _ = f.flush().await;
        }
        let _ = fsync_path(&self.dir.join(CURSOR_FILE_NAME)).await;
        if let Some(active_seq) = active_seq {
            let _ = fsync_path(&segment_path(&self.dir, active_seq)).await;
        }
        let _ = fsync_path(&self.dir).await;
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    pub(crate) fn scratch_dir(label: &str) -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir()
            .join(format!("logit-disk-queue-test-{label}-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create scratch dir");
        dir
    }

    /// The on-disk length `DiskQueue::push` writes for `batch` under `Compression::None`, for
    /// tests in other modules that size a spool around one record.
    pub(crate) fn encoded_record_len(
        batch: &logit_core::EventBatch,
        provenance: logit_core::Provenance,
    ) -> u64 {
        use super::CONTEXT_LEN;
        use logit_proto::{frame, native};

        let payload = native::encode_batch_v2(batch, provenance);
        let framed =
            frame::write_frame(native::CODEC_NATIVE_V2, frame::Compression::None, &payload)
                .expect("None compression never fails");
        (CONTEXT_LEN + framed.len()) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::scratch_dir;
    use super::*;
    use logit_core::{AttrMap, Event, Registry, Resource, Value};

    fn ctx() -> BatchContext {
        BatchContext { trace: TraceContext::new_root(), provenance: Provenance::default() }
    }

    fn batch(marker: &str) -> Arc<EventBatch> {
        let mut attrs = AttrMap::new();
        attrs.insert("marker", Value::str(marker));
        Arc::new(EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![Event::empty(0, attrs)],
        })
    }

    fn marker_of(batch: &EventBatch) -> String {
        batch.events[0].attributes.get("marker").and_then(Value::as_str).unwrap().to_string()
    }

    /// Sums every point named `name` in drained telemetry, only those carrying `tag` if given.
    /// Tags are on the drained `Event`'s attributes; the name is an interned `MetricRecord::name`.
    fn metric_sum(events: &[logit_core::Event], name: &str, tag: Option<(&str, &str)>) -> f64 {
        let name_sym = logit_core::interner::intern(name);
        events
            .iter()
            .filter(|e| match tag {
                Some((k, v)) => e.attributes.get(k).and_then(Value::as_str) == Some(v),
                None => true,
            })
            .flat_map(|e| e.metrics.iter())
            .filter(|m| m.name == name_sym)
            .map(|m| match &m.kind {
                logit_core::MetricKind::Sum(s) => s.value,
                logit_core::MetricKind::Gauge(v) => *v,
                _ => 0.0,
            })
            .sum()
    }

    fn config(dir: PathBuf) -> DiskQueueConfig {
        DiskQueueConfig {
            dir,
            max_bytes: 10 * 1024 * 1024,
            // Small enough that a few test batches force rotation.
            segment_bytes: 200,
            overflow: OverflowPolicy::Block,
            compression: Compression::None,
            checkpoint_interval: Duration::from_secs(3600),
        }
    }

    fn open(dir: PathBuf) -> DiskQueue {
        DiskQueue::open(config(dir), Telemetry::default(), Diagnostics::new("test")).unwrap()
    }

    fn open_with(config: DiskQueueConfig) -> DiskQueue {
        DiskQueue::open(config, Telemetry::default(), Diagnostics::new("test")).unwrap()
    }

    /// The on-disk bytes `DiskQueue::push` writes for one record, for hand-built segment files.
    fn raw_record(batch: &EventBatch, ctx: BatchContext) -> Vec<u8> {
        let payload = native::encode_batch_v2(batch, ctx.provenance);
        let framed = frame::write_frame(native::CODEC_NATIVE_V2, Compression::None, &payload)
            .expect("None compression never fails");
        let mut record = Vec::with_capacity(CONTEXT_LEN + framed.len());
        record.extend_from_slice(&encode_context(ctx.trace));
        record.extend_from_slice(&framed);
        record
    }

    /// A `CODEC_NATIVE_V1` record, with no provenance trailer.
    fn raw_record_v1(batch: &EventBatch, trace: TraceContext) -> Vec<u8> {
        let payload = native::encode_batch(batch);
        let framed = frame::write_frame(native::CODEC_NATIVE_V1, Compression::None, &payload)
            .expect("None compression never fails");
        let mut record = Vec::with_capacity(CONTEXT_LEN + framed.len());
        record.extend_from_slice(&encode_context(trace));
        record.extend_from_slice(&framed);
        record
    }

    #[tokio::test]
    async fn push_then_peek_then_commit_round_trips_fifo_across_a_segment_boundary() {
        let dir = scratch_dir("fifo");
        let q = open(dir.clone());

        // Each batch is well under `segment_bytes: 200`, so several force a rotation.
        for label in ["a", "b", "c", "d", "e", "f", "g", "h"] {
            q.push((batch(label), ctx())).await;
        }
        assert!(
            list_segments(&dir).unwrap().len() > 1,
            "small segment_bytes should have forced a rotation"
        );

        for label in ["a", "b", "c", "d", "e", "f", "g", "h"] {
            let (peeked, _) = q.peek().await.expect("should peek the next batch");
            assert_eq!(marker_of(&peeked), label, "delivery order must be FIFO");
            let (committed, _) = q.commit().expect("should commit what was just peeked");
            assert_eq!(marker_of(&committed), label);
        }
        q.close();
        assert!(q.peek().await.is_none(), "closed and empty should return None");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Provenance survives a spool round trip alongside the trace context.
    #[tokio::test]
    async fn provenance_survives_a_spool_round_trip() {
        let dir = scratch_dir("provenance-round-trip");
        let q = open(dir.clone());
        let sent = BatchContext {
            trace: TraceContext::new_root(),
            provenance: Provenance {
                origin: Some(logit_core::interner::intern("disk_queue_test_nginx_in")),
                previous: Some(logit_core::interner::intern("disk_queue_test_enrich")),
            },
        };

        q.push((batch("a"), sent)).await;

        let (_peeked, peeked_ctx) = q.peek().await.expect("should peek the pushed batch");
        assert_eq!(peeked_ctx, sent, "trace and provenance should both come back unchanged");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A `CODEC_NATIVE_V1` record on disk replays with empty provenance.
    #[tokio::test]
    async fn a_v1_codec_record_spooled_before_this_change_still_replays_with_empty_provenance() {
        let dir = scratch_dir("v1-record-compat");
        std::fs::create_dir_all(&dir).unwrap();
        let trace = TraceContext::new_root();
        let record = raw_record_v1(&batch("pre-provenance"), trace);
        std::fs::write(segment_path(&dir, 0), &record).unwrap();

        let q = open(dir.clone());
        let (peeked, peeked_ctx) = q.peek().await.expect("should find the pre-existing record");
        assert_eq!(marker_of(&peeked), "pre-provenance");
        assert_eq!(peeked_ctx.trace, trace);
        assert_eq!(peeked_ctx.provenance, Provenance::default());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn peek_is_cached_across_repeated_calls_until_commit() {
        let dir = scratch_dir("peek-cached");
        let q = open(dir.clone());
        q.push((batch("only"), ctx())).await;

        let (first, _) = q.peek().await.unwrap();
        let (second, _) = q.peek().await.unwrap();
        assert!(Arc::ptr_eq(&first, &second), "repeated peeks before commit should be cached");

        let (committed, _) = q.commit().unwrap();
        assert!(Arc::ptr_eq(&committed, &first));
        assert!(q.commit().is_none(), "nothing left to commit");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn reopen_after_uncommitted_pushes_replays_exactly_those() {
        let dir = scratch_dir("replay");
        {
            let q = open(dir.clone());
            q.push((batch("a"), ctx())).await;
            q.push((batch("b"), ctx())).await;
            q.push((batch("c"), ctx())).await;
            // Commit only the first.
            let (peeked, _) = q.peek().await.unwrap();
            assert_eq!(marker_of(&peeked), "a");
            q.commit().unwrap();
            q.finish().await;
        }

        let q2 = open(dir.clone());
        for label in ["b", "c"] {
            let (peeked, _) = q2.peek().await.unwrap();
            assert_eq!(marker_of(&peeked), label);
            q2.commit().unwrap();
        }
        assert!(q2.commit().is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_segment_truncated_mid_record_recovers_to_the_last_good_frame_and_counts_truncated() {
        let dir = scratch_dir("torn-tail");
        let path = segment_path(&dir, 0);
        std::fs::create_dir_all(&dir).unwrap();
        let good = raw_record(&batch("good"), ctx());
        let torn = raw_record(&batch("torn"), ctx());
        let mut bytes = good.clone();
        // Half of the second record: a torn write.
        bytes.extend_from_slice(&torn[..torn.len() / 2]);
        std::fs::write(&path, &bytes).unwrap();

        let registry = Registry::new();
        let telemetry = registry.telemetry_for("test", "output", "sink");
        let q = DiskQueue::open(config(dir.clone()), telemetry, Diagnostics::new("test")).unwrap();

        let on_disk = std::fs::metadata(&path).unwrap().len();
        assert_eq!(on_disk, good.len() as u64, "the torn tail should have been truncated away");

        let (peeked, _) = q.peek().await.expect("the one good record should still be there");
        assert_eq!(marker_of(&peeked), "good");
        q.commit().unwrap();

        let events = registry.drain(0);
        assert_eq!(
            metric_sum(&events, DISK_TRUNCATED, None),
            1.0,
            "the torn tail should have counted exactly one truncation"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_crc_corrupted_record_mid_segment_is_skipped_via_resync_and_counted() {
        let dir = scratch_dir("crc-corrupt");
        let path = segment_path(&dir, 0);
        std::fs::create_dir_all(&dir).unwrap();
        let first = raw_record(&batch("first"), ctx());
        let mut corrupted = raw_record(&batch("corrupt"), ctx());
        // Flip a payload byte so the header parses but the checksum fails.
        let last = corrupted.len() - 1;
        corrupted[last] ^= 0xFF;
        let third = raw_record(&batch("third"), ctx());

        let mut bytes = first.clone();
        bytes.extend_from_slice(&corrupted);
        bytes.extend_from_slice(&third);
        std::fs::write(&path, &bytes).unwrap();

        let q = open(dir.clone());
        let (peeked, _) = q.peek().await.unwrap();
        assert_eq!(marker_of(&peeked), "first");
        q.commit().unwrap();

        let (peeked, _) = q.peek().await.expect("should resync past the corrupt record to third");
        assert_eq!(marker_of(&peeked), "third");
        q.commit().unwrap();
        assert!(q.commit().is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_trace_id_containing_magic_does_not_derail_recovery() {
        let dir = scratch_dir("magic-in-trace-id");
        let path = segment_path(&dir, 0);
        std::fs::create_dir_all(&dir).unwrap();

        // A trace_id containing `MAGIC`: the spurious match `frame::resync` warns about.
        let mut spurious_ctx = ctx();
        spurious_ctx.trace.trace_id[4..8].copy_from_slice(&frame::MAGIC);
        let record = raw_record(&batch("real"), spurious_ctx);
        std::fs::write(&path, &record).unwrap();

        let q = open(dir.clone());
        let (peeked, peeked_ctx) = q.peek().await.expect("should still find the real record");
        assert_eq!(marker_of(&peeked), "real");
        assert_eq!(peeked_ctx, spurious_ctx);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn drop_oldest_advances_past_whole_records_and_reports_units() {
        let dir = scratch_dir("drop-oldest");
        let mut cfg = config(dir.clone());
        cfg.overflow = OverflowPolicy::DropOldest;
        // Small enough that pushing a second batch always evicts the first.
        let one = raw_record(&batch("x"), ctx()).len() as u64;
        cfg.max_bytes = one + one / 2;
        let q = open_with(cfg);

        q.push((batch("first"), ctx())).await;
        q.push((batch("second"), ctx())).await;

        let (peeked, _) = q.peek().await.expect("second should have survived, first evicted");
        assert_eq!(marker_of(&peeked), "second");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Under `DropOldest` with the head peeked, a push that doesn't fit is rejected and counted
    /// `overflow_newest`; the reserved head is never evicted and `max_bytes` still holds.
    #[tokio::test]
    async fn drop_oldest_drops_the_newest_rather_than_growing_past_max_bytes_while_the_head_is_peeked(
    ) {
        let dir = scratch_dir("drop-oldest-reserved");
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("test", "output", "sink");
        let mut cfg = config(dir.clone());
        cfg.overflow = OverflowPolicy::DropOldest;
        let one = raw_record(&batch("x"), ctx()).len() as u64;
        cfg.max_bytes = one + one / 2;
        let q = DiskQueue::open(cfg, telemetry, Diagnostics::new("test")).unwrap();

        q.push((batch("first"), ctx())).await;
        let (peeked, _) = q.peek().await.expect("should peek the only batch");
        assert_eq!(marker_of(&peeked), "first");

        // Nothing but the reserved head to evict, so this push is rejected.
        q.push((batch("second"), ctx())).await;

        let (still_first, _) = q.commit().expect("the peeked batch must still be first's");
        assert_eq!(marker_of(&still_first), "first");

        let events = registry.drain(0);
        let dropped = metric_sum(
            &events,
            SINK_QUEUE_METRICS.items_dropped,
            Some(("reason", "overflow_newest")),
        );
        assert_eq!(dropped, 1.0, "the second push should have been rejected and counted");

        q.close();
        assert!(q.peek().await.is_none(), "second was never admitted at all");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn drop_newest_rejects_the_push() {
        let dir = scratch_dir("drop-newest");
        let mut cfg = config(dir.clone());
        cfg.overflow = OverflowPolicy::DropNewest;
        let one = raw_record(&batch("x"), ctx()).len() as u64;
        cfg.max_bytes = one + one / 2;
        let q = open_with(cfg);

        q.push((batch("first"), ctx())).await;
        q.push((batch("second"), ctx())).await;

        let (peeked, _) = q.peek().await.unwrap();
        assert_eq!(marker_of(&peeked), "first", "the new push should have been rejected outright");
        q.commit().unwrap();
        q.close();
        assert!(q.peek().await.is_none(), "second was never admitted");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_missing_cursor_starts_at_the_oldest_segment() {
        let dir = scratch_dir("missing-cursor");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(segment_path(&dir, 0), raw_record(&batch("a"), ctx())).unwrap();
        std::fs::write(segment_path(&dir, 1), raw_record(&batch("b"), ctx())).unwrap();
        // No cursor.json at all.

        let q = open(dir.clone());
        let (peeked, _) = q.peek().await.expect("should resume from the oldest segment");
        assert_eq!(marker_of(&peeked), "a");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_cursor_past_the_end_restarts_at_the_oldest_surviving_segment() {
        let dir = scratch_dir("stale-cursor");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(segment_path(&dir, 5), raw_record(&batch("a"), ctx())).unwrap();
        // References a segment that doesn't exist (already deleted, in a real run).
        let mut diag = Diagnostics::new("test");
        persist_cursor(&dir, 2, 0, &mut diag);

        let q = open(dir.clone());
        let (peeked, _) = q.peek().await.expect("should clamp to the oldest surviving segment");
        assert_eq!(marker_of(&peeked), "a");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_fully_consumed_segment_is_deleted_once_commit_crosses_it() {
        let dir = scratch_dir("segment-deletion");
        let mut cfg = config(dir.clone());
        cfg.segment_bytes = 1; // rotate on every push
        let q = open_with(cfg);

        q.push((batch("a"), ctx())).await;
        q.push((batch("b"), ctx())).await;
        assert_eq!(list_segments(&dir).unwrap().len(), 2, "each push should have rotated");

        let (peeked, _) = q.peek().await.unwrap();
        assert_eq!(marker_of(&peeked), "a");
        q.commit().unwrap();

        // `commit` deletes synchronously; the yield guards against a future scheduling change.
        tokio::task::yield_now().await;
        assert_eq!(
            list_segments(&dir).unwrap(),
            vec![1],
            "segment 0 should be deleted once its only record is committed"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn the_utilization_gauge_tracks_max_bytes() {
        let dir = scratch_dir("utilization");
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("test", "output", "sink");
        let one = raw_record(&batch("x"), ctx()).len() as u64;
        let mut cfg = config(dir.clone());
        cfg.max_bytes = one * 4;
        let q = DiskQueue::open(cfg, telemetry, Diagnostics::new("test")).unwrap();

        q.push((batch("a"), ctx())).await;

        let events = registry.drain(0);
        let utilization = metric_sum(&events, SINK_QUEUE_METRICS.utilization, None);
        let expected = one as f64 / (one * 4) as f64;
        assert!(
            (utilization - expected).abs() < 1e-9,
            "expected utilization near {expected}, got {utilization}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn an_oversized_batch_is_dropped_and_counted_rather_than_written() {
        let dir = scratch_dir("oversized");
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("test", "output", "sink");
        let q = DiskQueue::open(config(dir.clone()), telemetry, Diagnostics::new("test")).unwrap();

        let mut attrs = AttrMap::new();
        attrs.insert("payload", Value::str("x".repeat(MAX_SANE_UNCOMPRESSED_LEN as usize + 1)));
        let huge = Arc::new(EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![Event::empty(0, attrs)],
        });
        q.push((huge, ctx())).await;

        q.close();
        assert!(q.peek().await.is_none(), "the oversized batch should never have been written");
        let events = registry.drain(0);
        let dropped = metric_sum(
            &events,
            SINK_QUEUE_METRICS.items_dropped,
            Some(("reason", "frame_too_large")),
        );
        assert_eq!(dropped, 1.0, "frame_too_large should have been counted exactly once");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_failed_write_drops_and_counts_the_batch_rather_than_silently_counting_it_queued() {
        let dir = scratch_dir("write-fails");
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("test", "output", "sink");
        let q = DiskQueue::open(config(dir.clone()), telemetry, Diagnostics::new("test")).unwrap();

        // Replace the active segment with a directory: opening it for append fails with
        // `EISDIR`, for root too (a read-only file would not fail for root, as in CI containers).
        std::fs::remove_file(segment_path(&dir, 0)).unwrap();
        std::fs::create_dir(segment_path(&dir, 0)).unwrap();

        q.push((batch("never-lands"), ctx())).await;

        // Restore a real file so the queue can be inspected/closed cleanly.
        std::fs::remove_dir(segment_path(&dir, 0)).unwrap();
        std::fs::File::create(segment_path(&dir, 0)).unwrap();

        q.close();
        assert!(
            q.peek().await.is_none(),
            "a batch whose write failed must never be counted as queued"
        );
        let events = registry.drain(0);
        let dropped = metric_sum(
            &events,
            SINK_QUEUE_METRICS.items_dropped,
            Some(("reason", "disk_io_error")),
        );
        assert_eq!(dropped, 1.0, "the failed write should have been counted dropped");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn close_then_peek_returns_none_when_empty() {
        let dir = scratch_dir("close-empty");
        let q = open(dir.clone());
        q.close();
        assert!(q.peek().await.is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    // -----------------------------------------------------------------------------------------
    // The read cursor rolls past a segment the reader finished while it was active, once that
    // segment rotates away.
    // -----------------------------------------------------------------------------------------

    #[tokio::test]
    async fn the_cursor_rolls_forward_when_a_segment_the_reader_caught_up_to_later_rotates_away() {
        let dir = scratch_dir("roll-forward");
        let mut cfg = config(dir.clone());
        // Every single-character-marker batch encodes to the same length.
        let one = raw_record(&batch("x"), ctx()).len() as u64;
        cfg.segment_bytes = 3 * one;
        let q = open_with(cfg);

        // `segment_bytes` is checked before a write, so all three land in segment 0.
        q.push((batch("a"), ctx())).await;
        q.push((batch("b"), ctx())).await;
        q.push((batch("c"), ctx())).await;
        assert_eq!(
            list_segments(&dir).unwrap(),
            vec![0],
            "all three should still fit in segment 0"
        );

        // Catch the reader up to the end of the still-active segment 0.
        for label in ["a", "b", "c"] {
            let (peeked, _) = q.peek().await.expect("should peek the next batch");
            assert_eq!(marker_of(&peeked), label);
            q.commit().unwrap();
        }

        // Forces rotation to segment 1.
        q.push((batch("d"), ctx())).await;
        assert_eq!(list_segments(&dir).unwrap(), vec![0, 1], "the push above should have rotated");

        // Without the roll, this parks forever against a segment that never grows again.
        let (peeked, _) = tokio::time::timeout(Duration::from_secs(5), q.peek())
            .await
            .expect("peek must not hang once the segment it was waiting on has rotated away")
            .expect("d should be delivered");
        assert_eq!(marker_of(&peeked), "d");
        q.commit().unwrap();
        assert_eq!(
            list_segments(&dir).unwrap(),
            vec![1],
            "segment 0 should have been deleted once the cursor rolled past it"
        );

        // The queue keeps flowing afterward.
        q.push((batch("e"), ctx())).await;
        let (peeked, _) = tokio::time::timeout(Duration::from_secs(5), q.peek())
            .await
            .expect("peek must not hang")
            .expect("e should be delivered");
        assert_eq!(marker_of(&peeked), "e");
        q.commit().unwrap();

        std::fs::remove_dir_all(&dir).ok();
    }

    // -----------------------------------------------------------------------------------------
    // A pushed record is flushed to the OS before `push` returns.
    // -----------------------------------------------------------------------------------------

    #[tokio::test]
    async fn a_pushed_record_is_on_disk_before_push_returns() {
        let dir = scratch_dir("flush-on-push");
        let q = open(dir.clone());
        q.push((batch("a"), ctx())).await;

        // Read through an independent path: visible only after `flush()`, not a bare `write_all`.
        let on_disk = std::fs::metadata(segment_path(&dir, 0)).unwrap().len();
        assert_eq!(on_disk, raw_record(&batch("a"), ctx()).len() as u64);
        std::fs::remove_dir_all(&dir).ok();
    }

    // -----------------------------------------------------------------------------------------
    // Live corruption resync advances the cursor past the skipped bytes too, not only the found
    // record's length.
    // -----------------------------------------------------------------------------------------

    #[tokio::test]
    async fn live_resync_past_corruption_advances_the_cursor_past_the_skipped_bytes() {
        let dir = scratch_dir("live-resync-cursor");
        let path = segment_path(&dir, 0);
        std::fs::create_dir_all(&dir).unwrap();
        let good = raw_record(&batch("good"), ctx());
        let mut corrupted = raw_record(&batch("corrupt"), ctx());
        let last = corrupted.len() - 1;
        corrupted[last] ^= 0xFF; // CRC-corrupt, same idiom as `rejects_corrupt_crc`.
        let next = raw_record(&batch("next"), ctx());
        let last_record = raw_record(&batch("last"), ctx());

        let mut bytes = good.clone();
        bytes.extend_from_slice(&corrupted);
        bytes.extend_from_slice(&next);
        bytes.extend_from_slice(&last_record);
        std::fs::write(&path, &bytes).unwrap();

        let registry = Registry::new();
        let telemetry = registry.telemetry_for("test", "output", "sink");
        let q = DiskQueue::open(config(dir.clone()), telemetry, Diagnostics::new("test")).unwrap();
        // Discard `open`'s own recovery count so the drain below reflects only the live path.
        registry.drain(0);

        // The clean first record.
        let (peeked, _) = q.peek().await.expect("good should be delivered");
        assert_eq!(marker_of(&peeked), "good");
        q.commit().unwrap();

        // This peek must take `read_record_at`'s *live* corruption-resync branch specifically.
        let (peeked, _) = tokio::time::timeout(Duration::from_secs(5), q.peek())
            .await
            .expect("peek must not hang")
            .expect("should resync live past the corrupt record to next");
        assert_eq!(marker_of(&peeked), "next");
        q.commit().unwrap();

        // Reaching `last` proves the cursor didn't land inside `next`'s bytes.
        let (peeked, _) = tokio::time::timeout(Duration::from_secs(5), q.peek())
            .await
            .expect("peek must not hang")
            .expect("should deliver last");
        assert_eq!(marker_of(&peeked), "last");
        q.commit().unwrap();

        q.close();
        assert!(q.peek().await.is_none(), "queue should be empty after last");

        let events = registry.drain(0);
        assert_eq!(
            metric_sum(&events, SINK_QUEUE_METRICS.items_dropped, Some(("reason", "disk_corrupt"))),
            1.0,
            "exactly one live corruption event should have been counted by the live read path"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    // -----------------------------------------------------------------------------------------
    // A corrupted `compressed_len` is not mistaken for a torn tail, which `DiskQueue::open` would
    // truncate away along with everything after it.
    // -----------------------------------------------------------------------------------------

    #[tokio::test]
    async fn a_corrupted_length_field_does_not_silently_discard_the_rest_of_the_segment() {
        let dir = scratch_dir("corrupted-length-field");
        let path = segment_path(&dir, 0);
        std::fs::create_dir_all(&dir).unwrap();
        let good = raw_record(&batch("good"), ctx());
        let mut corrupted = raw_record(&batch("corrupt"), ctx());
        // Set `compressed_len` (frame header bytes 16..20, after the 24-byte context) oversized.
        // Were it read as `Truncated`, `walk_segment` would stop and `open` would drop `after`.
        let compressed_len_at = CONTEXT_LEN + 16;
        corrupted[compressed_len_at..compressed_len_at + 4]
            .copy_from_slice(&u32::MAX.to_le_bytes());
        let after = raw_record(&batch("after"), ctx());

        let mut bytes = good.clone();
        bytes.extend_from_slice(&corrupted);
        bytes.extend_from_slice(&after);
        std::fs::write(&path, &bytes).unwrap();

        let q = open(dir.clone());

        let on_disk = std::fs::metadata(&path).unwrap().len();
        assert_eq!(
            on_disk,
            bytes.len() as u64,
            "the segment must not have been truncated -- the oversized compressed_len should have \
             been resynced past, not mistaken for a torn tail"
        );

        let (peeked, _) = q.peek().await.expect("good should still be delivered");
        assert_eq!(marker_of(&peeked), "good");
        q.commit().unwrap();

        let (peeked, _) = tokio::time::timeout(Duration::from_secs(5), q.peek())
            .await
            .expect("peek must not hang")
            .expect("after should still be delivered -- pre-fix this would silently vanish");
        assert_eq!(marker_of(&peeked), "after");
        q.commit().unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }
}
