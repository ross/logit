//! A disk-backed, crash-recoverable alternative to [`crate::queue::SinkQueue`]
//! (`docs/adr/disk-backed-sink-buffer.md`). [`DiskQueue`] has `SinkQueue`'s
//! `push`/`peek`/`commit`/`close` shape (both sit behind `crate::queue::SinkStore`), but every
//! batch is appended to a segment file before it is eligible for delivery, and a restart resumes
//! from the last checkpointed read cursor.
//!
//! **On-disk layout.** `<dir>/` holds:
//!
//! - `segment-<seq:016>.lgit` files, each a plain concatenation of records, oldest lowest.
//! - `cursor.json`, the read cursor (segment and byte offset), written through
//!   [`crate::atomic_write::write_file_durably`]: tmp, `fsync`, rename, directory `fsync`.
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
//! was complete before a newer one became active (one producer, one write in flight, and no
//! rotation while the active segment has unrepaired bytes), so only the active segment can have a
//! torn tail.
//!
//! **A cancelled `push` leaves its write running.** `tokio::fs::File::poll_write` hands the bytes
//! to a blocking thread and returns `Ready` at once; dropping the `push` future doesn't recall
//! them. Only an operation on the *same* `File` waits for that write to finish, so the active
//! segment has exactly one write handle, which outlives a cancelled push (see
//! [`HeldWriteFile`]), and the next push repairs through it (see [`State::needs_repair`]).
//!
//! **`commit` is synchronous**, like `SinkQueue::commit`: it mutates in-memory cursor state and
//! occasionally makes one small blocking cursor write (with two `fsync`s) and one file deletion,
//! never a segment read or write.
//!
//! **Every filesystem failure is observed.** A failed cursor write, segment `create`, `flush`,
//! `fsync`, torn-tail `truncate`, or unlink counts `logit.component.buffer.disk.errors{op}` and is
//! diagnosed (`cursor_error` for the cursor, `disk_fs_error` for the rest). A failed truncate also
//! drops the push that attempted it. Each mutating operation is preceded by a [`crate::fault`]
//! check, so tests can fail or freeze it.

use std::collections::VecDeque;
use std::fmt;
use std::fs::File as StdFile;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::atomic_write::{self, AtomicWriteError};
use crate::fanout::{BatchContext, TraceContext};
use crate::fault::{sites, Op, Point};
use crate::fault_io;
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
pub(crate) const CONTEXT_LEN: usize = 24;

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
/// Tagged `op`: `cursor`, `flush`, `fsync`, `create`, `truncate`, or `unlink`.
const DISK_ERRORS: &str = "logit.component.buffer.disk.errors";

const SEGMENT_CREATE: Point = Point::new(sites::SPOOL_SEGMENT, Op::Create);
const SEGMENT_OPEN: Point = Point::new(sites::SPOOL_SEGMENT, Op::Open);
const SEGMENT_WRITE: Point = Point::new(sites::SPOOL_SEGMENT, Op::Write);
const SEGMENT_FLUSH: Point = Point::new(sites::SPOOL_SEGMENT, Op::Flush);
const SEGMENT_SET_LEN: Point = Point::new(sites::SPOOL_SEGMENT, Op::SetLen);
const SEGMENT_SYNC: Point = Point::new(sites::SPOOL_SEGMENT, Op::SyncFile);
const SEGMENT_UNLINK: Point = Point::new(sites::SPOOL_SEGMENT, Op::Unlink);
const DIR_CREATE: Point = Point::new(sites::SPOOL_DIR, Op::Create);
const DIR_SYNC: Point = Point::new(sites::SPOOL_DIR, Op::SyncDir);

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

pub(crate) fn segment_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("segment-{seq:016}.lgit"))
}

/// Every `segment-<seq>.lgit` in `dir` whose `seq` is exactly 16 ASCII digits (the form
/// [`segment_path`] writes), ascending by sequence number. Other files are ignored, including an
/// unpadded twin such as `segment-0.lgit`: it would parse to a `seq` that already names a
/// different file, and be counted twice.
pub(crate) fn list_segments(dir: &Path) -> io::Result<Vec<u64>> {
    let mut seqs = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(seq_str) = name.strip_prefix("segment-").and_then(|s| s.strip_suffix(".lgit")) {
            if seq_str.len() != 16 || !seq_str.bytes().all(|b| b.is_ascii_digit()) {
                continue;
            }
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
pub(crate) struct WalkOutcome {
    /// Where the walk stopped: the end of the bytes, or the start of a torn tail (a record that
    /// reads as `Truncated` with nothing parseable after it).
    pub(crate) good_len: u64,
    pub(crate) valid_count: u64,
    pub(crate) corrupt_skipped: u64,
}

/// Walks every record in `bytes` from `start_offset` (`bytes[0]` is the segment's byte 0),
/// calling `on_record(offset, ctx, batch, len)` for each clean one.
///
/// A record that fails to parse resyncs forward with `frame::resync`, backing up over the
/// 24-byte context prefix it doesn't know about. The scan starts `CONTEXT_LEN + 1` bytes past
/// the failed record, because the next real record's `MAGIC` can be no nearer (the failed record
/// is at least one byte, and the next one's context precedes its `MAGIC`). So every candidate
/// lies past the failed record's start, and the walk never moves backwards. A spurious `MAGIC`
/// match (say, inside a `trace_id`) is tried and skipped if it doesn't parse.
///
/// A `Truncated` failure is a torn tail only if nothing after it parses: `frame::read_frame`
/// reports any in-cap `compressed_len` longer than the bytes present as `Truncated`
/// (`crates/logit-proto/tests/frame_fixed_point.rs`'s
/// `a_compressed_len_corrupted_below_the_cap_reads_as_truncated`, on `dur/w2`, #323), so a corrupt
/// length mid-segment reads the same as a torn write. A torn tail stops the walk at the failed record
/// (`good_len` is its start); a clean end and a torn tail look the same here, and the caller
/// tells them apart by comparing `good_len` with the file's length.
///
/// `corrupt_skipped` counts skipped regions, not records: one resync, or one unrecoverable run to
/// the end of `bytes`, counts 1 however many records it spanned. It's a lower bound.
pub(crate) fn walk_segment(
    bytes: &[u8],
    start_offset: u64,
    mut on_record: impl FnMut(u64, BatchContext, Arc<EventBatch>, u64),
) -> WalkOutcome {
    let mut pos = start_offset as usize;
    let mut valid_count = 0u64;
    let mut corrupt_skipped = 0u64;
    while pos < bytes.len() {
        let err = match parse_record(&bytes[pos..]) {
            Ok((ctx, batch, consumed)) => {
                on_record(pos as u64, ctx, batch, consumed as u64);
                valid_count += 1;
                pos += consumed;
                continue;
            }
            Err(err) => err,
        };
        match resync_after(bytes, pos) {
            Some(next) => {
                corrupt_skipped += 1;
                pos = next;
            }
            None if matches!(err, CodecError::Truncated { .. }) => break,
            None => {
                corrupt_skipped += 1;
                pos = bytes.len();
            }
        }
    }
    WalkOutcome { good_len: pos as u64, valid_count, corrupt_skipped }
}

/// The start of the first record after the one at `pos` that parses, if any. Always `> pos`: see
/// [`walk_segment`] for why the scan starts `CONTEXT_LEN + 1` bytes on.
fn resync_after(bytes: &[u8], pos: usize) -> Option<usize> {
    let mut scan_from = pos + CONTEXT_LEN + 1;
    while scan_from < bytes.len() {
        let magic_at = scan_from + frame::resync(&bytes[scan_from..])?;
        let candidate = magic_at - CONTEXT_LEN;
        if parse_record(&bytes[candidate..]).is_ok() {
            return Some(candidate);
        }
        scan_from = magic_at + 1;
    }
    None
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

/// Persists the read cursor through [`atomic_write::write_file_durably`]: two `fsync`s, so never
/// call it holding the state lock. On failure the previous cursor stays in place (unless only the
/// directory `fsync` failed), so the cost is replay, never loss. Callers report a failure with
/// [`report_cursor_error`].
fn persist_cursor(dir: &Path, segment: u64, offset: u64) -> Result<(), AtomicWriteError> {
    let doc = CursorFile { version: CURSOR_VERSION, segment, offset };
    let bytes = serde_json::to_vec(&doc).expect("a CursorFile of plain integers always serializes");
    atomic_write::write_file_durably(&dir.join(CURSOR_FILE_NAME), &bytes, sites::SPOOL_CURSOR)
}

/// Counts (`op="cursor"`) and diagnoses (`cursor_error`) a failed [`persist_cursor`].
fn report_cursor_error(
    err: &AtomicWriteError,
    dir: &Path,
    diag: &mut Diagnostics,
    telemetry: &Telemetry,
) {
    telemetry.count(DISK_ERRORS, 1.0, &[("op", "cursor")]);
    let path = dir.join(CURSOR_FILE_NAME);
    diag.warn_throttled("cursor_error", format!("writing cursor {}: {err}", path.display()));
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

/// What [`DiskQueue::read_record_at`] found at the read cursor.
enum ReadOutcome {
    /// A record, and how far the cursor must advance to pass it (past any corrupt bytes skipped
    /// to reach it).
    Record(BatchContext, Arc<EventBatch>, u64),
    /// Corruption with no record after it before the end of the segment: advance this many bytes
    /// without delivering.
    Skip(u64),
    /// Nothing readable now: an I/O error, a segment already rolled away, or a file shorter than
    /// its recorded length. The caller re-evaluates.
    Unavailable,
}

/// The active segment's write handle, out of [`State::write_file`] for one operation. Dropping
/// the guard puts the handle back, so a `push` cancelled at any `.await` keeps the handle, and
/// with it tokio's record of any write still running on a blocking thread, for the next push's
/// repair to wait on. Its `Drop` locks the state only briefly and allocates nothing.
struct HeldWriteFile<'a> {
    queue: &'a DiskQueue,
    file: Option<tokio::fs::File>,
}

impl Drop for HeldWriteFile<'_> {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            let mut state = self.queue.inner.lock().unwrap_or_else(|p| p.into_inner());
            // One producer: no other guard can have put a handle back meanwhile.
            debug_assert!(state.write_file.is_none(), "two write handles on one spool");
            state.write_file = Some(file);
        }
    }
}

/// Why [`DiskQueue::write_record`] appended nothing. `push` counts the batch dropped with
/// `reason="disk_full"` or `"disk_io_error"`.
struct WriteError {
    disk_full: bool,
}

impl WriteError {
    fn from_io(err: &io::Error) -> Self {
        Self { disk_full: is_disk_full(err) }
    }
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
    /// The active segment's one write handle, while no [`HeldWriteFile`] has it out. `None`
    /// until the first write after [`DiskQueue::open`].
    write_file: Option<tokio::fs::File>,
    /// The active segment's length before a write that hasn't been confirmed. Set just before
    /// the write and cleared once it's flushed. A failed write, or a `push` future dropped
    /// mid-write (`run_output`'s `select!` drops `drain_inbox` when `write_loop` finishes first),
    /// leaves it set, and the next [`DiskQueue::push`] truncates back to it before anything else.
    /// While it's set, nothing is written and nothing rotates. A crash is the same case, repaired
    /// at the next [`DiskQueue::open`].
    needs_repair: Option<u64>,
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
    /// Held across a cursor write (see [`DiskQueue::checkpoint_cursor`]). Always taken before
    /// `inner`, never while holding it.
    cursor_write: Mutex<()>,
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

        fault_io!(DIR_CREATE, &config.dir, 0, std::fs::create_dir_all(&config.dir))
            .with_context(|| format!("creating disk buffer directory {}", config.dir.display()))?;

        let lock_path = config.dir.join(LOCK_FILE_NAME);
        let lock_file = fault_io!(
            DIR_CREATE,
            &lock_path,
            0,
            std::fs::OpenOptions::new().create(true).truncate(false).write(true).open(&lock_path)
        )
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

        let (mut segments, read_seq, read_offset): (VecDeque<Segment>, u64, u64) = if seqs
            .is_empty()
        {
            let path = segment_path(&config.dir, 0);
            fault_io!(SEGMENT_CREATE, &path, 0, std::fs::File::create(&path))
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
                        fault_io!(
                            SEGMENT_SET_LEN,
                            &path,
                            seq,
                            std::fs::File::options()
                                .write(true)
                                .open(&path)
                                .and_then(|f| f.set_len(outcome.good_len))
                        )
                        .with_context(|| format!("truncating torn segment {}", path.display()))?;
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

        // A segment behind the cursor was left by an unlink that failed after the cursor moved
        // past it. It holds nothing to deliver, so it never counts toward `max_bytes`; it's
        // unlinked below, once the cursor is persisted.
        let mut leaked: Vec<u64> = Vec::new();
        while segments.front().is_some_and(|s| s.seq < read_seq) {
            leaked.extend(segments.pop_front().map(|s| s.seq));
        }

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

        // Persist any clamping above, regardless of `checkpoint_interval`. A failure isn't fatal:
        // the next open recomputes the same clamp.
        if let Err(err) = persist_cursor(&config.dir, read_seq, read_offset) {
            report_cursor_error(&err, &config.dir, &mut diag, &telemetry);
        }
        // Even after a failed persist: the cursor on disk named `read_seq` already (a fallback
        // to the oldest segment leaves nothing behind it), so these hold nothing to replay.
        for seq in leaked {
            let path = segment_path(&config.dir, seq);
            if let Err(err) = fault_io!(SEGMENT_UNLINK, &path, seq, std::fs::remove_file(&path)) {
                telemetry.count(DISK_ERRORS, 1.0, &[("op", "unlink")]);
                diag.warn_throttled(
                    "disk_fs_error",
                    format!("deleting segment {seq}, already behind the cursor: {err}"),
                );
            }
        }

        let total_bytes: u64 = segments.iter().map(|s| s.len).sum();
        let segment_count = segments.len();

        let state = State {
            segments,
            read_seq,
            read_offset,
            total_bytes,
            queued_records: replayed,
            head_cache: None,
            write_file: None,
            needs_repair: None,
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
            cursor_write: Mutex::new(()),
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

    /// Counts and diagnoses a failed non-cursor filesystem operation. Takes the state lock, so
    /// never call it while holding one.
    fn count_fs_error(&self, op: &'static str, what: fmt::Arguments<'_>, err: &io::Error) {
        self.telemetry.count(DISK_ERRORS, 1.0, &[("op", op)]);
        let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        state.diag.warn_throttled("disk_fs_error", format!("{what}: {err}"));
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
    /// or a push to a closed queue, is written over-bound rather than blocked. A spool that reads
    /// as full with nothing queued, under any policy, first makes room by rotating its consumed
    /// active segment away (see [`DiskQueue::rotate_consumed_active_segment`]). A batch whose
    /// encoded payload exceeds `MAX_SANE_UNCOMPRESSED_LEN`, or whose write fails, is dropped and
    /// counted, never counted as queued.
    ///
    /// **Cancellation safety.** The record is encoded in memory first. A future dropped at any
    /// write-path `.await` keeps the write handle (see [`HeldWriteFile`]) and leaves at most a
    /// tail past [`State::needs_repair`], which the next push waits out and truncates before it
    /// writes anything.
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
        let mut made_room = false;

        enum Action {
            Write,
            Block,
            Evict,
            DropNewest,
            MakeRoom,
        }

        loop {
            // Roll the cursor off a consumed segment that has rotated away, so its bytes leave
            // `total_bytes` before the full check (see `roll_read_cursor`).
            self.roll_read_cursor();
            let notified = self.not_full.notified();
            let action = {
                let state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
                let full = state.total_bytes + record_len > self.max_bytes;
                let active = state.segments.back().expect("always at least one segment");
                // After the roll above, a cursor at the end of the active segment means every
                // other segment is gone and nothing is queued. The spool can still read as full:
                // `total_bytes` shrinks only when a whole segment is deleted, and the active
                // segment never is.
                let nothing_queued = state.head_cache.is_none()
                    && state.read_seq == active.seq
                    && state.read_offset >= active.len;
                if !full || impossible_to_ever_fit || self.closed() {
                    Action::Write
                } else if nothing_queued && !made_room {
                    // No consumer will ever free these bytes: rotate the consumed active segment
                    // away and delete it.
                    Action::MakeRoom
                } else if nothing_queued {
                    // Making room failed (a failed rotation, say). Waiting or evicting would wait
                    // on a consumer with nothing to consume, so accept over-bound, except under
                    // `DropNewest`, which drops.
                    match self.overflow {
                        OverflowPolicy::DropNewest => Action::DropNewest,
                        OverflowPolicy::Block | OverflowPolicy::DropOldest => Action::Write,
                    }
                } else {
                    match self.overflow {
                        OverflowPolicy::Block => Action::Block,
                        OverflowPolicy::DropNewest => Action::DropNewest,
                        // Accepting over-bound while the head is reserved would leave
                        // `disk.max_bytes` unenforced for most of a destination outage, the case
                        // the bound exists for.
                        OverflowPolicy::DropOldest if state.head_cache.is_some() => {
                            // The head is reserved (peeked, mid-delivery). A file-backed FIFO
                            // can't evict behind it the way the in-memory buffer can, so reject
                            // the new push, as `DropNewest` would.
                            Action::DropNewest
                        }
                        OverflowPolicy::DropOldest => Action::Evict,
                    }
                }
            };
            match action {
                Action::Write => break,
                Action::DropNewest => {
                    self.count_dropped("overflow_newest", events);
                    return;
                }
                Action::MakeRoom => {
                    made_room = true;
                    if let Err(err) = self.rotate_consumed_active_segment().await {
                        let reason = if err.disk_full { "disk_full" } else { "disk_io_error" };
                        self.count_dropped(reason, events);
                        return;
                    }
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

        if let Err(err) = self.write_record(&record).await {
            // Never durably written, so count it dropped, not queued. `disk_full` is the one
            // cause the overflow policies can't prevent, so it gets its own reason.
            let reason = if err.disk_full { "disk_full" } else { "disk_io_error" };
            self.count_dropped(reason, events);
            return;
        }
        self.not_empty.notify_one();
        self.after_change();
    }

    /// Takes the active segment's write handle out of the state until the guard drops.
    fn hold_write_file(&self) -> HeldWriteFile<'_> {
        let file = self.inner.lock().unwrap_or_else(|p| p.into_inner()).write_file.take();
        HeldWriteFile { queue: self, file }
    }

    /// Frees a full spool that has nothing queued: rotates the fully consumed active segment
    /// into a closed one, then rolls the read cursor off it, which deletes it. `push` calls it
    /// only when the read cursor sits at the end of the active segment, and it's the only
    /// producer, so nothing can be queued in between.
    ///
    /// Repairs a torn tail first, since nothing rotates past unrepaired bytes; a failed repair is
    /// returned for `push` to count the batch dropped. A failed rotation isn't: `push` sees the
    /// spool still full and doesn't try again.
    async fn rotate_consumed_active_segment(&self) -> Result<(), WriteError> {
        let mut held = self.hold_write_file();
        let (seq, needs_repair) = {
            let state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            (state.segments.back().expect("always at least one segment").seq, state.needs_repair)
        };
        if let Some(before) = needs_repair {
            self.repair_torn_tail(&mut held, seq, before).await?;
        }
        self.rotate_segment(&mut held).await;
        drop(held);
        self.roll_read_cursor();
        Ok(())
    }

    /// Repairs a torn tail left by a cancelled or failed write, rotates if the active segment has
    /// reached `segment_bytes`, then appends and flushes `record` and counts it queued. Every
    /// `.await` on the active segment goes through the one retained handle.
    ///
    /// The accounting happens in the same poll as the flush completing, so a cancelled push
    /// either counted its record or left `needs_repair` set for the next push to truncate.
    async fn write_record(&self, record: &[u8]) -> Result<(), WriteError> {
        let mut held = self.hold_write_file();
        let (seq, needs_repair) = {
            let state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            (state.segments.back().expect("always at least one segment").seq, state.needs_repair)
        };
        if let Some(before) = needs_repair {
            self.repair_torn_tail(&mut held, seq, before).await?;
        }

        let needs_rotate = {
            let state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            state.segments.back().expect("always at least one segment").len >= self.segment_bytes
        };
        if needs_rotate {
            self.rotate_segment(&mut held).await;
        }

        let seq = {
            let state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            state.segments.back().expect("always at least one segment").seq
        };
        if held.file.is_none() {
            match self.open_append(seq).await {
                Ok(file) => held.file = Some(file),
                Err(err) => {
                    let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
                    state.diag.warn_throttled(
                        "disk_io_error",
                        format!("opening segment {seq} for append: {err}"),
                    );
                    // Nothing was written, so there is no torn tail to repair.
                    return Err(WriteError::from_io(&err));
                }
            }
        }
        let file = held.file.as_mut().expect("opened above");

        {
            let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            let len = state.segments.back().expect("always at least one segment").len;
            state.needs_repair = Some(len);
        }
        // `write_all` returning `Ok` means only that the bytes reached `tokio::fs::File`'s
        // buffer; `flush()` hands them to the kernel. Without it, a batch counted as queued is
        // lost on an ordinary process crash, not only on power loss. A failed flush is repaired
        // like a failed write.
        let mut result = fault_io!(SEGMENT_WRITE, &self.dir, seq, file.write_all(record).await);
        if result.is_ok() {
            result = fault_io!(SEGMENT_FLUSH, &self.dir, seq, file.flush().await);
        }

        let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        match result {
            Ok(()) => {
                let record_len = record.len() as u64;
                state.needs_repair = None;
                state.segments.back_mut().expect("always at least one segment").len += record_len;
                state.total_bytes += record_len;
                state.queued_records += 1;
                Ok(())
            }
            Err(err) => {
                state.diag.warn_throttled("disk_io_error", format!("writing segment {seq}: {err}"));
                Err(WriteError::from_io(&err))
            }
        }
    }

    /// Truncates the active segment back to `before`, its length ahead of an unconfirmed write,
    /// through the retained handle (opening one only if none is retained).
    ///
    /// The `flush` first waits for a cancelled push's write that is still running on a blocking
    /// thread, and surfaces and clears the error tokio stores from a failed one
    /// (`last_write_err`, tokio 1.53.1 `src/fs/file.rs:1096`, `:1104`), which would otherwise
    /// fail the next write. A failed flush is counted `op="flush"` and diagnosed, and the repair
    /// goes on: the truncate discards those bytes either way, and `set_len`'s own
    /// `complete_inflight` still waits for the orphaned write. Truncating through any other
    /// handle wouldn't wait, and the write could land after it.
    ///
    /// Untested: the error-clearing role. The `fault` seam fails an operation instead of running
    /// it, so no test makes a real write fail inside tokio; that role rests on tokio's source.
    ///
    /// A failed truncate is counted `op="truncate"` and diagnosed, fails this push, and leaves
    /// `needs_repair` set, so nothing is written or rotated until a later push repairs.
    async fn repair_torn_tail(
        &self,
        held: &mut HeldWriteFile<'_>,
        seq: u64,
        before: u64,
    ) -> Result<(), WriteError> {
        let path = segment_path(&self.dir, seq);
        let truncated = match held.file.as_mut() {
            Some(file) => {
                if let Err(err) = fault_io!(SEGMENT_FLUSH, &path, seq, file.flush().await) {
                    self.count_fs_error("flush", format_args!("flushing segment {seq}"), &err);
                }
                fault_io!(SEGMENT_SET_LEN, &path, seq, file.set_len(before).await)
            }
            None => {
                // `append`, like every other handle on the segment: a positioned handle would
                // write the next record at offset 0.
                let opened = fault_io!(
                    SEGMENT_OPEN,
                    &path,
                    seq,
                    tokio::fs::OpenOptions::new().append(true).open(&path).await
                );
                match opened {
                    Ok(file) => {
                        let file = held.file.insert(file);
                        fault_io!(SEGMENT_SET_LEN, &path, seq, file.set_len(before).await)
                    }
                    Err(err) => Err(err),
                }
            }
        };
        match truncated {
            Ok(()) => {
                self.inner.lock().unwrap_or_else(|p| p.into_inner()).needs_repair = None;
                Ok(())
            }
            Err(err) => {
                self.count_fs_error(
                    "truncate",
                    format_args!("truncating segment {seq}'s torn tail to {before} bytes"),
                    &err,
                );
                Err(WriteError::from_io(&err))
            }
        }
    }

    async fn open_append(&self, seq: u64) -> io::Result<tokio::fs::File> {
        let path = segment_path(&self.dir, seq);
        fault_io!(
            SEGMENT_OPEN,
            &path,
            seq,
            tokio::fs::OpenOptions::new().create(true).append(true).open(&path).await
        )
    }

    /// Closes out the active segment and starts the next: flushes and `fsync`s the old segment
    /// through the retained handle, creates the new one, `fsync`s the directory, then makes the
    /// new segment active with its handle retained. Each failure is counted and diagnosed. A
    /// failed flush or `fsync` doesn't stop the rotation. A failed create leaves the old segment
    /// active, and the next push retries.
    ///
    /// The new segment opens without truncating, so a file left by a rotation cancelled after
    /// its create (always empty: nothing writes to a segment before it's active) is reused.
    async fn rotate_segment(&self, held: &mut HeldWriteFile<'_>) {
        let old_seq = {
            let state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            state.segments.back().expect("always at least one segment").seq
        };
        let old_path = segment_path(&self.dir, old_seq);
        let synced = match held.file.as_mut() {
            Some(file) => {
                if let Err(err) = fault_io!(SEGMENT_FLUSH, &old_path, old_seq, file.flush().await) {
                    self.count_fs_error("flush", format_args!("flushing segment {old_seq}"), &err);
                }
                fault_io!(SEGMENT_SYNC, &old_path, old_seq, file.sync_data().await)
            }
            None => fault_io!(SEGMENT_SYNC, &old_path, old_seq, fsync_path(&old_path).await),
        };
        if let Err(err) = synced {
            self.count_fs_error("fsync", format_args!("syncing segment {old_seq}"), &err);
        }
        let new_seq = old_seq + 1;
        let new_path = segment_path(&self.dir, new_seq);
        let created = fault_io!(
            SEGMENT_CREATE,
            &new_path,
            new_seq,
            tokio::fs::OpenOptions::new().create(true).append(true).open(&new_path).await
        );
        let new_file = match created {
            Ok(file) => file,
            Err(err) => {
                self.count_fs_error("create", format_args!("creating segment {new_seq}"), &err);
                return;
            }
        };
        if let Err(err) = fault_io!(DIR_SYNC, &self.dir, new_seq, fsync_path(&self.dir).await) {
            self.count_fs_error("fsync", format_args!("syncing {}", self.dir.display()), &err);
        }
        // No `.await` from here on, so a cancelled push can't separate the new handle from the
        // new segment.
        let old_file = held.file.replace(new_file);
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .segments
            .push_back(Segment { seq: new_seq, len: 0 });
        drop(old_file);
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
        let (batch, len) = match self.read_record_at(seq, offset).await {
            ReadOutcome::Record(_ctx, batch, len) => (batch, len),
            ReadOutcome::Skip(delta) => return self.skip_corrupt(seq, offset, delta),
            ReadOutcome::Unavailable => return false,
        };
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
    /// `needed` hint past [`READ_CHUNK_INITIAL`]. Reads no further than the segment's in-memory
    /// length, which covers only whole, flushed records: an in-progress write past it is never
    /// read, and a record that claims to run past it is corrupt, not waiting on a write. Resyncs
    /// past live corruption as defense in depth; the primary recovery is [`DiskQueue::open`].
    async fn read_record_at(&self, seq: u64, offset: u64) -> ReadOutcome {
        let seg_len = {
            let state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            state.segments.iter().find(|s| s.seq == seq).map(|s| s.len)
        };
        let Some(seg_len) = seg_len else { return ReadOutcome::Unavailable };
        let Some(remaining) = seg_len.checked_sub(offset).filter(|&r| r > 0) else {
            return ReadOutcome::Unavailable;
        };
        let remaining = remaining as usize;
        let mut chunk_len = READ_CHUNK_INITIAL.min(remaining);
        let buf = loop {
            let buf = match self.read_at(seq, offset, chunk_len).await {
                Ok(buf) => buf,
                Err(_) => return ReadOutcome::Unavailable,
            };
            if buf.len() < chunk_len {
                // The file is shorter than the accounting says: nothing to parse or skip.
                return ReadOutcome::Unavailable;
            }
            match parse_record(&buf) {
                Ok((ctx, batch, consumed)) => {
                    return ReadOutcome::Record(ctx, batch, consumed as u64)
                }
                Err(CodecError::Truncated { needed }) if buf.len() < remaining => {
                    chunk_len = (buf.len() + needed).min(remaining);
                }
                // Corrupt, or `Truncated` with every byte up to the segment's length in hand.
                Err(_) => break buf,
            }
        };

        let whole = if buf.len() == remaining {
            buf
        } else {
            match self.read_at(seq, offset, remaining).await {
                Ok(b) if b.len() == remaining => b,
                _ => return ReadOutcome::Unavailable,
            }
        };
        // The returned length is a delta from the read cursor, not the record's size: `pos`
        // counts the corrupt bytes skipped before the record, so the delta is `pos + len`. `len`
        // alone would land the cursor inside the record.
        let mut found = None;
        let outcome = walk_segment(&whole, 0, |pos, ctx, batch, len| {
            if found.is_none() {
                found = Some((ctx, batch, pos + len));
            }
        });
        match found {
            Some((ctx, batch, delta)) => {
                self.count_dropped("disk_corrupt", outcome.corrupt_skipped.max(1));
                ReadOutcome::Record(ctx, batch, delta)
            }
            None => ReadOutcome::Skip(remaining as u64),
        }
    }

    /// Advances the read cursor past `delta` bytes of corruption at `(seq, offset)` without
    /// delivering, as a commit would, and counts one `batches.dropped{reason="disk_corrupt"}`
    /// with zero events: how many events a run of undecodable bytes held is unknowable. Returns
    /// whether it skipped: `false` if the head moved or was reserved since the read.
    ///
    /// Leaves `queued_records` alone. `open` seeds it from the records that parse, so corruption
    /// already there at open was never counted, and decrementing for it would under-report
    /// records still queued. A record corrupted after its push was counted, so it over-reports
    /// by one until the next `open` re-derives the count.
    fn skip_corrupt(&self, seq: u64, offset: u64, delta: u64) -> bool {
        {
            let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            if state.head_cache.is_some() || state.read_seq != seq || state.read_offset != offset {
                return false;
            }
            state.read_offset += delta;
        }
        self.count_dropped("disk_corrupt", 0);
        self.after_cursor_advance();
        self.after_change();
        self.not_full.notify_one();
        true
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
    /// `peek`, `push` (under every policy), and `advance_read_cursor` all call it. A reader that
    /// caught up to the writer while its segment was active has nothing else to re-evaluate
    /// `read_seq` once a later `push` rotates that segment away, and `peek` would wait on
    /// `not_empty` forever against a segment that never grows again.
    ///
    /// After a roll, and outside the lock, persists the cursor, then deletes the segments it left
    /// and notifies `not_full`. The cursor is durable before any segment it left is unlinked. A
    /// segment whose unlink fails still leaves memory and `total_bytes` (the cursor has left it,
    /// so it holds nothing to deliver), and [`DiskQueue::open`] removes it next time.
    /// Allocates nothing when no boundary is crossed. Returns whether the cursor now points at
    /// readable bytes.
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
            self.checkpoint_cursor();
            for seq in to_delete {
                let path = segment_path(&self.dir, seq);
                if let Err(err) = fault_io!(SEGMENT_UNLINK, &path, seq, std::fs::remove_file(&path))
                {
                    self.count_fs_error("unlink", format_args!("deleting segment {seq}"), &err);
                }
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
        self.after_cursor_advance();
    }

    /// Rolls across any segment boundary the cursor just crossed, and persists the cursor if
    /// `checkpoint_interval` has elapsed.
    fn after_cursor_advance(&self) {
        if !self.roll_read_cursor() {
            // Nothing left queued. A push parked on a full spool must re-check now: it can make
            // room by rotating the consumed active segment away, and no later commit or roll
            // will wake it.
            self.not_full.notify_one();
        }
        let due = {
            let state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            state.last_checkpoint.elapsed() >= self.checkpoint_interval
        };
        if due {
            self.checkpoint_cursor();
        }
    }

    /// Persists the current read cursor, records the checkpoint time, and reports a failure. The
    /// write runs outside the state lock, so its two `fsync`s never stall a `push` or `peek`.
    ///
    /// `cursor_write` serializes concurrent calls (the consumer's `commit`, and a `DropOldest`
    /// producer's roll or eviction), and each reads the cursor only once the previous write has
    /// finished. The cursor only moves forward, so what lands on disk never moves backward, and a
    /// roll's call persists a cursor at or past the one it computed.
    fn checkpoint_cursor(&self) {
        let _serial = self.cursor_write.lock().unwrap_or_else(|p| p.into_inner());
        let (seq, offset) = {
            let state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            (state.read_seq, state.read_offset)
        };
        let result = persist_cursor(&self.dir, seq, offset);
        let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        state.last_checkpoint = Instant::now();
        if let Err(err) = result {
            report_cursor_error(&err, &self.dir, &mut state.diag, &self.telemetry);
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
                ReadOutcome::Record(ctx, batch, record_len) => {
                    let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
                    if state.read_seq == seq && state.read_offset == offset {
                        state.head_cache = Some(HeadCache { batch, ctx, record_len });
                    }
                }
                ReadOutcome::Skip(delta) => {
                    self.skip_corrupt(seq, offset, delta);
                }
                ReadOutcome::Unavailable => {
                    // Nothing readable where accounting said there should be (an I/O error, or a
                    // file shorter than its recorded length). Back off briefly rather than spin,
                    // then re-evaluate.
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

    /// Persists the cursor durably regardless of `checkpoint_interval`, flushes and `fsync`s the
    /// active segment, `fsync`s the directory, and closes files. Each failure is counted and
    /// diagnosed. Drops nothing: what is queued delivers after the next open.
    ///
    /// Doesn't repair a torn tail. The flush, through the retained handle, waits for any write a
    /// cancelled push left running, and whatever it leaves past the last whole record is
    /// [`DiskQueue::open`]'s to truncate.
    pub async fn finish(&self) {
        self.checkpoint_cursor();
        let mut held = self.hold_write_file();
        let active_seq = {
            let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            state.read_file = None;
            state.segments.back().expect("always at least one segment").seq
        };
        let active_path = segment_path(&self.dir, active_seq);
        let synced = match held.file.as_mut() {
            Some(file) => {
                let flushed =
                    fault_io!(SEGMENT_FLUSH, &active_path, active_seq, file.flush().await);
                if let Err(err) = flushed {
                    self.count_fs_error(
                        "flush",
                        format_args!("flushing segment {active_seq}"),
                        &err,
                    );
                }
                fault_io!(SEGMENT_SYNC, &active_path, active_seq, file.sync_data().await)
            }
            None => {
                fault_io!(SEGMENT_SYNC, &active_path, active_seq, fsync_path(&active_path).await)
            }
        };
        if let Err(err) = synced {
            self.count_fs_error("fsync", format_args!("syncing segment {active_seq}"), &err);
        }
        if let Err(err) = fault_io!(DIR_SYNC, &self.dir, 0, fsync_path(&self.dir).await) {
            self.count_fs_error("fsync", format_args!("syncing {}", self.dir.display()), &err);
        }
        // Closes the handle rather than returning it to the state.
        drop(held.file.take());
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use super::{encode_context, DiskQueueConfig, CONTEXT_LEN};
    use crate::fanout::{BatchContext, TraceContext};
    use crate::queue::OverflowPolicy;
    use logit_core::{AttrMap, Event, EventBatch, Provenance, Resource, Value};
    use logit_proto::frame::{self, Compression};
    use logit_proto::native;

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
        let payload = native::encode_batch_v2(batch, provenance);
        let framed =
            frame::write_frame(native::CODEC_NATIVE_V2, frame::Compression::None, &payload)
                .expect("None compression never fails");
        (CONTEXT_LEN + framed.len()) as u64
    }

    pub(crate) fn ctx() -> BatchContext {
        BatchContext { trace: TraceContext::new_root(), provenance: Provenance::default() }
    }

    pub(crate) fn batch(marker: &str) -> Arc<EventBatch> {
        let mut attrs = AttrMap::new();
        attrs.insert("marker", Value::str(marker));
        Arc::new(EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![Event::empty(0, attrs)],
        })
    }

    pub(crate) fn marker_of(batch: &EventBatch) -> String {
        batch.events[0].attributes.get("marker").and_then(Value::as_str).unwrap().to_string()
    }

    /// Sums every point named `name` in drained telemetry, only those carrying `tag` if given.
    /// Tags are on the drained `Event`'s attributes; the name is an interned `MetricRecord::name`.
    pub(crate) fn metric_sum(
        events: &[logit_core::Event],
        name: &str,
        tag: Option<(&str, &str)>,
    ) -> f64 {
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

    pub(crate) fn config(dir: PathBuf) -> DiskQueueConfig {
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

    /// The on-disk bytes `DiskQueue::push` writes for one record, for hand-built segment files.
    pub(crate) fn raw_record(batch: &EventBatch, ctx: BatchContext) -> Vec<u8> {
        let payload = native::encode_batch_v2(batch, ctx.provenance);
        let framed = frame::write_frame(native::CODEC_NATIVE_V2, Compression::None, &payload)
            .expect("None compression never fails");
        let mut record = Vec::with_capacity(CONTEXT_LEN + framed.len());
        record.extend_from_slice(&encode_context(ctx.trace));
        record.extend_from_slice(&framed);
        record
    }

    /// A `CODEC_NATIVE_V1` record, with no provenance trailer.
    pub(crate) fn raw_record_v1(batch: &EventBatch, trace: TraceContext) -> Vec<u8> {
        let payload = native::encode_batch(batch);
        let framed = frame::write_frame(native::CODEC_NATIVE_V1, Compression::None, &payload)
            .expect("None compression never fails");
        let mut record = Vec::with_capacity(CONTEXT_LEN + framed.len());
        record.extend_from_slice(&encode_context(trace));
        record.extend_from_slice(&framed);
        record
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{
        batch, config, ctx, marker_of, metric_sum, raw_record, raw_record_v1, scratch_dir,
    };
    use super::*;
    use crate::fault::{self, errno};
    use logit_core::{AttrMap, Event, Registry, Resource, Value};
    use std::future::Future;

    fn open(dir: PathBuf) -> DiskQueue {
        DiskQueue::open(config(dir), Telemetry::default(), Diagnostics::new("test")).unwrap()
    }

    fn open_with(config: DiskQueueConfig) -> DiskQueue {
        DiskQueue::open(config, Telemetry::default(), Diagnostics::new("test")).unwrap()
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
        persist_cursor(&dir, 2, 0).unwrap();

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

    // -----------------------------------------------------------------------------------------
    // The cursor write is durable, and every spool fsync, create, and unlink failure is counted
    // and diagnosed (DISK-04).
    // -----------------------------------------------------------------------------------------

    /// A queue with live telemetry, plus a clone of its `Diagnostics` (clones share occurrence
    /// counts) for asserting what was diagnosed.
    fn open_observed(cfg: DiskQueueConfig) -> (DiskQueue, Arc<Registry>, Diagnostics) {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("test", "output", "sink");
        let diag = Diagnostics::new("test");
        let q = DiskQueue::open(cfg, telemetry, diag.clone()).unwrap();
        (q, registry, diag)
    }

    fn disk_errors(registry: &Registry, op: &str) -> f64 {
        metric_sum(&registry.drain(0), DISK_ERRORS, Some(("op", op)))
    }

    async fn deliver(q: &DiskQueue, labels: &[&str]) {
        for label in labels {
            let (peeked, _) = tokio::time::timeout(Duration::from_secs(5), q.peek())
                .await
                .expect("peek must not stop responding")
                .expect("a batch should be queued");
            assert_eq!(marker_of(&peeked), *label);
            q.commit().unwrap();
        }
    }

    #[tokio::test]
    async fn a_cursor_persist_is_fsynced_before_its_rename_and_the_directory_after() {
        let dir = scratch_dir("cursor-fsync-order");
        let mut cfg = config(dir.clone());
        cfg.segment_bytes = 1; // rotate on every push
        let q = open_with(cfg);
        q.push((batch("a"), ctx())).await;
        q.push((batch("b"), ctx())).await;

        let scope = fault::scope(&dir);
        scope.record();
        // Crossing out of segment 0 persists the cursor, then deletes the segment.
        deliver(&q, &["a"]).await;

        let hits = scope.hits();
        let cursor_ops: Vec<Op> = hits
            .iter()
            .filter(|h| h.point.site == sites::SPOOL_CURSOR)
            .map(|h| h.point.op)
            .collect();
        assert_eq!(cursor_ops, vec![Op::Write, Op::SyncFile, Op::Rename, Op::SyncDir]);
        let cursor_durable_at = hits
            .iter()
            .position(|h| h.point == Point::new(sites::SPOOL_CURSOR, Op::SyncDir))
            .unwrap();
        let unlinks: Vec<usize> = hits
            .iter()
            .enumerate()
            .filter(|(_, h)| h.point == SEGMENT_UNLINK)
            .map(|(i, _)| i)
            .collect();
        assert_eq!(unlinks.len(), 1, "segment 0 is deleted: {hits:?}");
        assert!(
            unlinks.iter().all(|&i| cursor_durable_at < i),
            "a segment must be unlinked only once the cursor leaving it is durable: {hits:?}"
        );
        drop(scope);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_failed_segment_fsync_at_rotation_is_counted_and_diagnosed() {
        let dir = scratch_dir("rotation-fsync-fails");
        let mut cfg = config(dir.clone());
        cfg.segment_bytes = 1;
        let (q, registry, diag) = open_observed(cfg);
        let scope = fault::scope(&dir);
        scope.fail(SEGMENT_SYNC, errno::EIO);

        q.push((batch("a"), ctx())).await;
        q.push((batch("b"), ctx())).await; // rotates, failing segment 0's fsync

        assert_eq!(disk_errors(&registry, "fsync"), 1.0);
        assert_eq!(diag.occurrences("disk_fs_error"), 1);
        assert_eq!(list_segments(&dir).unwrap(), vec![0, 1], "the rotation still completes");
        deliver(&q, &["a", "b"]).await;
        drop(scope);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_failed_rotation_create_is_counted_and_the_next_push_retries_rotation() {
        let dir = scratch_dir("rotation-create-fails");
        let mut cfg = config(dir.clone());
        cfg.segment_bytes = 1;
        let (q, registry, diag) = open_observed(cfg);
        let scope = fault::scope(&dir);
        scope.fail_nth(SEGMENT_CREATE, 1, errno::ENOSPC);

        q.push((batch("a"), ctx())).await;
        q.push((batch("b"), ctx())).await; // the create fails, so b lands in segment 0
        assert_eq!(list_segments(&dir).unwrap(), vec![0]);
        q.push((batch("c"), ctx())).await; // retries the rotation, which now succeeds

        assert_eq!(list_segments(&dir).unwrap(), vec![0, 1]);
        assert_eq!(disk_errors(&registry, "create"), 1.0);
        assert_eq!(diag.occurrences("disk_fs_error"), 1);
        deliver(&q, &["a", "b", "c"]).await;
        drop(scope);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_failed_directory_fsync_is_counted() {
        let dir = scratch_dir("dir-fsync-fails");
        let mut cfg = config(dir.clone());
        cfg.segment_bytes = 1;
        let (q, registry, diag) = open_observed(cfg);
        let scope = fault::scope(&dir);
        scope.fail(DIR_SYNC, errno::EIO);

        q.push((batch("a"), ctx())).await;
        q.push((batch("b"), ctx())).await; // rotation's directory fsync
        q.finish().await; // finish's directory fsync

        assert_eq!(disk_errors(&registry, "fsync"), 2.0);
        assert_eq!(diag.occurrences("disk_fs_error"), 2);
        assert_eq!(diag.occurrences("cursor_error"), 0, "the cursor syncs under its own site");
        drop(scope);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_failed_segment_unlink_is_counted() {
        let dir = scratch_dir("unlink-fails");
        let mut cfg = config(dir.clone());
        cfg.segment_bytes = 1;
        let (q, registry, diag) = open_observed(cfg);
        q.push((batch("a"), ctx())).await;
        q.push((batch("b"), ctx())).await;
        let scope = fault::scope(&dir);
        scope.fail(SEGMENT_UNLINK, errno::EACCES);

        deliver(&q, &["a"]).await; // crosses out of segment 0 and tries to delete it

        assert_eq!(disk_errors(&registry, "unlink"), 1.0);
        assert_eq!(diag.occurrences("disk_fs_error"), 1);
        assert!(segment_path(&dir, 0).exists(), "the failed unlink left segment 0 behind");
        drop(scope);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_persistently_failing_cursor_write_is_counted_every_time() {
        let dir = scratch_dir("cursor-write-fails");
        let mut cfg = config(dir.clone());
        cfg.segment_bytes = 1024 * 1024; // no rotation, so every persist is a commit's
        cfg.checkpoint_interval = Duration::ZERO; // persist on every commit
        let (q, registry, diag) = open_observed(cfg.clone());
        for label in ["a", "b", "c"] {
            q.push((batch(label), ctx())).await;
        }
        let scope = fault::scope(&dir);
        scope.fail(Point::new(sites::SPOOL_CURSOR, Op::Rename), errno::EIO);

        deliver(&q, &["a", "b", "c"]).await;

        assert_eq!(disk_errors(&registry, "cursor"), 3.0);
        assert_eq!(diag.occurrences("cursor_error"), 3);
        drop(scope);

        // With no cursor ever persisted past the start, a restart replays everything: duplicates,
        // never loss.
        drop(q);
        let reopened = open_with(cfg);
        deliver(&reopened, &["a", "b", "c"]).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    // -----------------------------------------------------------------------------------------
    // Recovery and the read path treat an in-cap corrupt length as corruption, never walk
    // backwards, and never stall on a closed segment (DISK-01, DISK-02, DISK-07's read path).
    // -----------------------------------------------------------------------------------------

    /// `raw_record(batch(marker))` with its frame's `compressed_len` rewritten to `declared`.
    fn record_with_compressed_len(marker: &str, declared: u32) -> Vec<u8> {
        let mut record = raw_record(&batch(marker), ctx());
        let at = CONTEXT_LEN + 16;
        record[at..at + 4].copy_from_slice(&declared.to_le_bytes());
        record
    }

    /// A length past everything written after it, but far under the frame layer's sanity cap, so
    /// `frame::read_frame` reports `Truncated` rather than `Malformed`
    /// (`crates/logit-proto/tests/frame_fixed_point.rs`'s
    /// `a_compressed_len_corrupted_below_the_cap_reads_as_truncated`, on `dur/w2`, #323).
    const IN_CAP_CORRUPT_LEN: u32 = 1024 * 1024;

    async fn peek_within(q: &DiskQueue) -> Option<(Arc<EventBatch>, BatchContext)> {
        tokio::time::timeout(Duration::from_secs(5), q.peek())
            .await
            .expect("peek must not stop responding")
    }

    #[tokio::test]
    async fn a_corrupted_length_field_below_the_sanity_cap_does_not_truncate_the_records_after_it()
    {
        let dir = scratch_dir("in-cap-length-active");
        let path = segment_path(&dir, 0);
        let mut bytes = raw_record(&batch("good"), ctx());
        bytes.extend_from_slice(&record_with_compressed_len("corrupt", IN_CAP_CORRUPT_LEN));
        bytes.extend_from_slice(&raw_record(&batch("after"), ctx()));
        std::fs::write(&path, &bytes).unwrap();

        let (q, registry, _diag) = open_observed(config(dir.clone()));

        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            bytes.len() as u64,
            "an in-cap corrupt length is corruption with a record after it, not a torn tail"
        );
        let events = registry.drain(0);
        assert_eq!(metric_sum(&events, DISK_TRUNCATED, None), 0.0);
        assert_eq!(
            metric_sum(&events, SINK_QUEUE_METRICS.items_dropped, Some(("reason", "disk_corrupt"))),
            1.0
        );
        assert_eq!(metric_sum(&events, DISK_REPLAYED, None), 2.0);
        deliver(&q, &["good", "after"]).await;
        q.close();
        assert!(peek_within(&q).await.is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_closed_segment_with_an_in_cap_corrupt_length_does_not_stall_peek() {
        let dir = scratch_dir("in-cap-length-closed");
        let mut closed = raw_record(&batch("good"), ctx());
        closed.extend_from_slice(&record_with_compressed_len("corrupt", IN_CAP_CORRUPT_LEN));
        closed.extend_from_slice(&raw_record(&batch("after"), ctx()));
        std::fs::write(segment_path(&dir, 0), &closed).unwrap();
        std::fs::write(segment_path(&dir, 1), raw_record(&batch("next"), ctx())).unwrap();

        let q = open(dir.clone());
        deliver(&q, &["good", "after", "next"]).await;
        q.close();
        assert!(peek_within(&q).await.is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn unrecoverable_garbage_at_the_end_of_a_closed_segment_is_skipped_and_counted() {
        for garbage in [
            // Reads as `Truncated`: a length past the segment's end.
            record_with_compressed_len("corrupt", IN_CAP_CORRUPT_LEN),
            // Reads as `Malformed`: no MAGIC anywhere to resync to.
            vec![0xA5; 64],
        ] {
            let dir = scratch_dir("closed-garbage-tail");
            let mut closed = raw_record(&batch("good"), ctx());
            closed.extend_from_slice(&garbage);
            std::fs::write(segment_path(&dir, 0), &closed).unwrap();
            std::fs::write(segment_path(&dir, 1), raw_record(&batch("next"), ctx())).unwrap();

            let (q, registry, _diag) = open_observed(config(dir.clone()));
            // Only the read path's count below: `open` counts the same region once itself.
            registry.drain(0);
            let mut events = Vec::new();

            deliver(&q, &["good"]).await;
            // Skips the garbage, crossing into segment 1, then reads `next`.
            let (peeked, _) = peek_within(&q).await.expect("next is still queued");
            assert_eq!(marker_of(&peeked), "next");
            let drained = registry.drain(0);
            assert_eq!(
                metric_sum(&drained, SINK_QUEUE_METRICS.depth, None),
                1.0,
                "the skip must not count `next`, which open queued, as gone"
            );
            events.extend(drained);
            q.commit().unwrap();
            let drained = registry.drain(0);
            assert_eq!(metric_sum(&drained, SINK_QUEUE_METRICS.depth, None), 0.0);
            events.extend(drained);
            q.close();
            assert!(peek_within(&q).await.is_none());
            events.extend(registry.drain(0));

            let corrupt = |metric| metric_sum(&events, metric, Some(("reason", "disk_corrupt")));
            assert_eq!(corrupt(SINK_QUEUE_METRICS.items_dropped), 1.0, "one skipped region");
            assert_eq!(
                corrupt(SINK_QUEUE_METRICS.units_dropped),
                0.0,
                "undecodable bytes have no knowable event count"
            );
            assert!(
                !segment_path(&dir, 0).exists(),
                "the skip crossed out of segment 0, which is deleted as a commit would"
            );
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn a_record_one_byte_after_a_corrupt_byte_is_still_recovered() {
        // The nearest a real record's `MAGIC` can sit after a failed parse at `pos`: one corrupt
        // byte, then the record's context. `resync_after` must start its scan no later.
        let a = raw_record(&batch("a"), ctx());
        let b = raw_record(&batch("b"), ctx());
        let mut bytes = a.clone();
        bytes.push(0x5A);
        bytes.extend_from_slice(&b);

        let mut emitted: Vec<(u64, u64)> = Vec::new();
        let outcome = walk_segment(&bytes, 0, |offset, _, _, len| emitted.push((offset, len)));

        let b_at = a.len() as u64 + 1;
        assert_eq!(emitted, vec![(0, a.len() as u64), (b_at, b.len() as u64)]);
        assert_eq!(outcome.corrupt_skipped, 1);
        assert_eq!(outcome.good_len, bytes.len() as u64);
    }

    #[test]
    fn a_spurious_frame_inside_a_corrupt_records_context_never_moves_the_walk_backwards() {
        // Record A, then fewer than `CONTEXT_LEN` filler bytes, then a frame with no context of
        // its own. Parsing at A's end fails (its "frame" starts inside the real one). The MAGIC
        // just past A's end, backed up by `CONTEXT_LEN`, lands inside A, and a record parses
        // there: the last bytes of A plus the filler as its context, then the real frame.
        let a = raw_record(&batch("a"), ctx());
        let payload = native::encode_batch_v2(&batch("phantom"), Provenance::default());
        let bare_frame =
            frame::write_frame(native::CODEC_NATIVE_V2, Compression::None, &payload).unwrap();
        for filler in 1..CONTEXT_LEN {
            let mut bytes = a.clone();
            bytes.extend(std::iter::repeat_n(0x5A, filler));
            bytes.extend_from_slice(&bare_frame);

            let mut emitted: Vec<(u64, u64)> = Vec::new();
            let outcome = walk_segment(&bytes, 0, |offset, _, _, len| emitted.push((offset, len)));

            assert_eq!(emitted, vec![(0, a.len() as u64)], "filler {filler}: only A is a record");
            assert_eq!(outcome.corrupt_skipped, 1, "filler {filler}");
            assert_eq!(outcome.good_len, bytes.len() as u64, "filler {filler}");
        }
    }

    #[tokio::test]
    async fn a_segment_file_whose_name_is_not_zero_padded_is_ignored() {
        let dir = scratch_dir("unpadded-segment-name");
        std::fs::write(segment_path(&dir, 0), raw_record(&batch("real"), ctx())).unwrap();
        // Each parses as a `u64`: two alias segment 0, one names a segment that isn't there.
        for name in ["segment-0.lgit", "segment-+000000000000000.lgit", "segment-7.lgit"] {
            std::fs::write(dir.join(name), raw_record(&batch("alias"), ctx())).unwrap();
        }

        assert_eq!(list_segments(&dir).unwrap(), vec![0]);
        let (q, registry, _diag) = open_observed(config(dir.clone()));
        let events = registry.drain(0);
        assert_eq!(metric_sum(&events, DISK_REPLAYED, None), 1.0, "segment 0 counts once");
        deliver(&q, &["real"]).await;
        q.close();
        assert!(peek_within(&q).await.is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_second_open_of_the_same_spool_directory_fails_at_the_lock() {
        let dir = scratch_dir("second-open");
        let first = open(dir.clone());
        first.push((batch("a"), ctx())).await;
        let segment_before = std::fs::read(segment_path(&dir, 0)).unwrap();

        let err = DiskQueue::open(config(dir.clone()), Telemetry::default(), Diagnostics::new("t"))
            .err()
            .expect("a second open of a locked spool must fail");
        assert!(err.to_string().contains("already in use"), "{err:#}");
        assert_eq!(
            std::fs::read(segment_path(&dir, 0)).unwrap(),
            segment_before,
            "the refused open must not have truncated or rewritten anything"
        );

        drop(first);
        let reopened = open(dir.clone());
        deliver(&reopened, &["a"]).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    // -----------------------------------------------------------------------------------------
    // A cancelled or failed write never leaves a segment longer on disk than in memory, and a
    // failed repair never writes past the torn bytes (DISK-03, DISK-05).
    // -----------------------------------------------------------------------------------------

    /// Every segment's in-memory length beside its length on disk, oldest first.
    fn segment_lengths(q: &DiskQueue, dir: &Path) -> Vec<(u64, u64, u64)> {
        let segments: Vec<Segment> = {
            let state = q.inner.lock().unwrap();
            state.segments.iter().copied().collect()
        };
        segments
            .into_iter()
            .map(|s| (s.seq, s.len, std::fs::metadata(segment_path(dir, s.seq)).unwrap().len()))
            .collect()
    }

    fn assert_segments_match_disk(q: &DiskQueue, dir: &Path) {
        for (seq, in_memory, on_disk) in segment_lengths(q, dir) {
            assert_eq!(on_disk, in_memory, "segment {seq}: on-disk length vs in-memory length");
        }
    }

    /// Polls `fut` once inside `rt`, so any blocking work it spawns goes to `rt`'s pool. Returns
    /// whether it's still pending.
    fn poll_once_in<F: std::future::Future>(
        rt: &tokio::runtime::Runtime,
        mut fut: std::pin::Pin<&mut F>,
    ) -> bool {
        rt.block_on(std::future::poll_fn(|cx| {
            std::task::Poll::Ready(fut.as_mut().poll(cx).is_pending())
        }))
    }

    #[tokio::test]
    async fn a_push_cancelled_after_its_bytes_landed_is_truncated_by_the_next_push() {
        let dir = scratch_dir("cancel-after-landing");
        let mut cfg = config(dir.clone());
        cfg.segment_bytes = 1024 * 1024;
        let q = open_with(cfg);
        q.push((batch("a"), ctx())).await;
        let path = segment_path(&dir, 0);
        let before = std::fs::metadata(&path).unwrap().len();
        let record_len = raw_record(&batch("cancelled"), ctx()).len() as u64;

        {
            let mut push = std::pin::pin!(q.push((batch("cancelled"), ctx())));
            // Poll only while the bytes haven't landed. The write goes to the blocking pool on
            // the first poll, which then parks at the `flush` await until it completes.
            let deadline = Instant::now() + Duration::from_secs(5);
            while std::fs::metadata(&path).unwrap().len() < before + record_len {
                assert!(Instant::now() < deadline, "the write never landed");
                let pending = std::future::poll_fn(|cx| {
                    std::task::Poll::Ready(push.as_mut().poll(cx).is_pending())
                })
                .await;
                assert!(pending, "the push must still be parked at its flush");
                std::thread::sleep(Duration::from_millis(1));
            }
        } // dropped at the `flush` await, its bytes on disk

        q.push((batch("b"), ctx())).await;
        assert_segments_match_disk(&q, &dir);
        deliver(&q, &["a", "b"]).await;
        q.close();
        assert!(peek_within(&q).await.is_none(), "the cancelled record was truncated away");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The orphaned write lands after the next push has repaired and appended: the order a
    /// fresh-fd repair can't prevent, because nothing makes it wait for a write issued on another
    /// handle. A second runtime, whose one blocking thread the test holds, parks the orphan.
    #[test]
    fn an_orphaned_write_that_lands_after_the_next_push_began_never_desynchronizes_the_segment() {
        let dir = scratch_dir("orphan-lands-late");
        let mut cfg = config(dir.clone());
        cfg.segment_bytes = 1024 * 1024;
        let main = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let stalled = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        let (release, gate) = std::sync::mpsc::channel::<()>();
        stalled.spawn_blocking(move || {
            let _ = gate.recv();
        });
        let q = open_with(cfg);
        main.block_on(q.push((batch("a"), ctx())));

        {
            let mut push = std::pin::pin!(q.push((batch("orphan"), ctx())));
            assert!(poll_once_in(&stalled, push.as_mut()), "the write is parked behind the gate");
        } // cancelled at the `flush` await, its write queued behind the gate

        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            let _ = release.send(());
        });
        main.block_on(q.push((batch("b"), ctx())));
        releaser.join().unwrap();
        drop(stalled); // waits for the orphaned write to land

        assert_segments_match_disk(&q, &dir);
        main.block_on(async {
            deliver(&q, &["a", "b"]).await;
            q.close();
            assert!(peek_within(&q).await.is_none(), "the orphan never becomes a record");
        });
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Cancels a push after `k` polls, for `k` in 1..=8 over 20 rounds, then pushes a clean
    /// record. The cancelled push runs on a second runtime whose one blocking thread first sleeps
    /// a varying amount, so its orphaned work (a write, a flush, a rotation's `fsync` or create, a
    /// repair's truncate) lands before, during, or after the next push's own.
    #[test]
    fn cancelling_pushes_at_every_await_never_desynchronizes_the_segment() {
        let dir = scratch_dir("cancel-every-await");
        let mut cfg = config(dir.clone());
        // Three records a segment, so cancellations land inside rotations too.
        cfg.segment_bytes = 3 * raw_record(&batch("c00-0"), ctx()).len() as u64;
        let main = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let side = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        let q = open_with(cfg);

        let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut jitter = move |max_us: u64| {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            Duration::from_micros((seed >> 33) % max_us)
        };
        let mut expected: Vec<String> = Vec::new();
        for round in 0..20 {
            for k in 1..=8u32 {
                let delay = jitter(3000);
                side.spawn_blocking(move || std::thread::sleep(delay));
                let cancelled = format!("x{round:02}-{k}");
                let completed = {
                    let mut push = std::pin::pin!(q.push((batch(&cancelled), ctx())));
                    let mut completed = false;
                    for _ in 0..k {
                        if !poll_once_in(&side, push.as_mut()) {
                            completed = true;
                            break;
                        }
                        std::thread::sleep(jitter(500));
                    }
                    completed
                }; // a push still pending is cancelled here, before the next one starts
                if completed {
                    expected.push(cancelled);
                }

                let clean = format!("c{round:02}-{k}");
                main.block_on(q.push((batch(&clean), ctx())));
                expected.push(clean);
                assert_segments_match_disk(&q, &dir);
            }
        }
        drop(side); // waits for every orphaned operation to land
        assert_segments_match_disk(&q, &dir);

        let labels: Vec<&str> = expected.iter().map(String::as_str).collect();
        main.block_on(async {
            deliver(&q, &labels).await;
            q.close();
            assert!(peek_within(&q).await.is_none(), "only completed pushes are ever delivered");
        });
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_failed_torn_tail_truncate_drops_and_counts_the_batch_and_never_writes_past_the_torn_bytes(
    ) {
        let dir = scratch_dir("repair-truncate-fails");
        let mut cfg = config(dir.clone());
        cfg.segment_bytes = 1024 * 1024;
        let (q, registry, diag) = open_observed(cfg);
        q.push((batch("a"), ctx())).await;
        let path = segment_path(&dir, 0);
        let a_len = std::fs::metadata(&path).unwrap().len();
        let torn_len = raw_record(&batch("torn"), ctx()).len() as u64;
        registry.drain(0);

        let scope = fault::scope(&dir);
        scope.fail_nth(SEGMENT_FLUSH, 1, errno::ENOSPC);
        q.push((batch("torn"), ctx())).await; // written, then the flush "fails"
        scope.fail_nth(SEGMENT_SET_LEN, 1, errno::EIO);
        q.push((batch("blocked"), ctx())).await; // its repair's truncate fails

        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            a_len + torn_len,
            "nothing may be appended after unrepaired bytes"
        );
        assert_eq!(segment_lengths(&q, &dir)[0].1, a_len, "the in-memory length never moved");
        let events = registry.drain(0);
        let dropped = |reason| {
            metric_sum(&events, SINK_QUEUE_METRICS.items_dropped, Some(("reason", reason)))
        };
        assert_eq!(dropped("disk_full"), 1.0, "the torn push");
        assert_eq!(dropped("disk_io_error"), 1.0, "the push whose repair failed");
        assert_eq!(metric_sum(&events, DISK_ERRORS, Some(("op", "truncate"))), 1.0);
        assert_eq!(diag.occurrences("disk_fs_error"), 1);

        q.push((batch("ok"), ctx())).await; // repairs, then lands
        assert_segments_match_disk(&q, &dir);
        deliver(&q, &["a", "ok"]).await;
        q.close();
        assert!(peek_within(&q).await.is_none());
        drop(scope);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_failed_repair_never_rotates_so_only_the_active_segment_can_be_torn() {
        let dir = scratch_dir("repair-fails-no-rotate");
        let mut cfg = config(dir.clone());
        cfg.segment_bytes = 1; // rotate on every push
        let (q, registry, _diag) = open_observed(cfg);
        q.push((batch("a"), ctx())).await;

        let scope = fault::scope(&dir);
        scope.fail_nth(SEGMENT_FLUSH, 2, errno::EIO); // the first is the rotation's flush
        q.push((batch("torn"), ctx())).await; // rotates to segment 1, then the flush "fails"
        assert_eq!(list_segments(&dir).unwrap(), vec![0, 1]);
        scope.fail(SEGMENT_SET_LEN, errno::EIO);
        q.push((batch("blocked-1"), ctx())).await;
        q.push((batch("blocked-2"), ctx())).await;

        assert_eq!(list_segments(&dir).unwrap(), vec![0, 1], "no rotation past a torn tail");
        let lengths = segment_lengths(&q, &dir);
        assert_eq!(lengths[0].1, lengths[0].2, "the closed segment is whole");
        assert_eq!(lengths[1].1, 0, "segment 1 holds only torn bytes");
        assert_eq!(
            lengths[1].2,
            raw_record(&batch("torn"), ctx()).len() as u64,
            "and nothing after them"
        );
        assert_eq!(disk_errors(&registry, "truncate"), 2.0);

        drop(scope);
        q.push((batch("ok"), ctx())).await;
        assert_segments_match_disk(&q, &dir);
        deliver(&q, &["a", "ok"]).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `push` `expect`s `write_frame` to succeed: it fails only for `Compression::Zstd`, which no
    /// `disk.compression` value maps to. The exhaustive match fails to compile if
    /// `logit_config::Compression` gains a variant, so the new one gets checked here.
    #[tokio::test]
    async fn every_configurable_disk_compression_is_encodable_by_write_frame() {
        for configured in [logit_config::Compression::None, logit_config::Compression::Lz4] {
            let compression = match configured {
                logit_config::Compression::None => Compression::None,
                logit_config::Compression::Lz4 => Compression::Lz4,
            };
            let payload = native::encode_batch_v2(&batch("x"), Provenance::default());
            frame::write_frame(native::CODEC_NATIVE_V2, compression, &payload)
                .unwrap_or_else(|err| panic!("{configured:?} must encode: {err}"));

            let dir = scratch_dir("every-compression");
            let mut cfg = config(dir.clone());
            cfg.compression = compression;
            let q = open_with(cfg);
            q.push((batch("round-trip"), ctx())).await;
            deliver(&q, &["round-trip"]).await;
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[tokio::test]
    async fn drop_oldest_reclaims_space_a_whole_head_segment_at_a_time_and_counts_every_eviction() {
        let dir = scratch_dir("drop-oldest-whole-segment");
        let one = raw_record(&batch("x"), ctx()).len() as u64;
        let mut cfg = config(dir.clone());
        cfg.overflow = OverflowPolicy::DropOldest;
        cfg.segment_bytes = 3 * one;
        cfg.max_bytes = 6 * one;
        let (q, registry, _diag) = open_observed(cfg);
        for label in ["a", "b", "c", "d", "e", "f"] {
            q.push((batch(label), ctx())).await;
        }
        assert_eq!(list_segments(&dir).unwrap(), vec![0, 1]);
        registry.drain(0);

        // Evicting `a` or `b` frees nothing: space comes back only when segment 0 is deleted.
        q.push((batch("g"), ctx())).await;

        let events = registry.drain(0);
        let evicted = |metric| metric_sum(&events, metric, Some(("reason", "overflow_oldest")));
        assert_eq!(evicted(SINK_QUEUE_METRICS.items_dropped), 3.0, "a, b, and c, one push");
        assert_eq!(evicted(SINK_QUEUE_METRICS.units_dropped), 3.0);
        assert_eq!(list_segments(&dir).unwrap(), vec![1, 2]);
        assert!(segment_lengths(&q, &dir).iter().map(|s| s.1).sum::<u64>() <= 6 * one);
        deliver(&q, &["d", "e", "f", "g"]).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The worst case of whole-segment reclamation: with `segment_bytes == max_bytes`, the head
    /// segment is the active one, so evicting frees nothing until every queued record is gone.
    /// The push then rotates the consumed segment away and deletes it, and lands within the
    /// bound. The eviction burst is inherent to reclaiming by segment (see the ADR's rejected
    /// deferred-skip-cursor design); `docs/deploying.md` advises `segment_bytes` well under
    /// `max_bytes` to keep it small.
    #[tokio::test]
    async fn drop_oldest_with_one_active_segment_evicts_every_queued_record_then_rotates_to_make_room(
    ) {
        let dir = scratch_dir("drop-oldest-one-segment");
        let one = raw_record(&batch("x"), ctx()).len() as u64;
        let mut cfg = config(dir.clone());
        cfg.overflow = OverflowPolicy::DropOldest;
        cfg.segment_bytes = 3 * one;
        cfg.max_bytes = 3 * one;
        let (q, registry, _diag) = open_observed(cfg);
        for label in ["a", "b", "c"] {
            q.push((batch(label), ctx())).await;
        }
        registry.drain(0);

        q.push((batch("d"), ctx())).await;

        let events = registry.drain(0);
        assert_eq!(
            metric_sum(
                &events,
                SINK_QUEUE_METRICS.items_dropped,
                Some(("reason", "overflow_oldest"))
            ),
            3.0,
            "every queued record is evicted by one push"
        );
        assert_eq!(list_segments(&dir).unwrap(), vec![1], "the evicted segment is reclaimed");
        let total: u64 = segment_lengths(&q, &dir).iter().map(|s| s.1).sum();
        assert_eq!(total, one, "then the push lands within max_bytes");
        deliver(&q, &["d"]).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    // -----------------------------------------------------------------------------------------
    // Cursor rollover, segment deletion, and `finish` under crashes and failures (DISK-06), and
    // a full spool with nothing queued (F5).
    // -----------------------------------------------------------------------------------------

    /// Closes `q` and drains it, returning every delivered marker in order.
    async fn drain_all(q: &DiskQueue) -> Vec<String> {
        q.close();
        let mut delivered = Vec::new();
        while let Some((batch, _)) = peek_within(q).await {
            delivered.push(marker_of(&batch));
            q.commit().unwrap();
        }
        delivered
    }

    /// Each recorded hit as the `(point, n)` that [`fault::Scope::crash_at`] needs to stop at it:
    /// `n` counts that point's hits up to and including this one.
    fn crash_points(hits: &[fault::Hit]) -> Vec<(Point, u64)> {
        hits.iter()
            .enumerate()
            .map(|(i, hit)| {
                let n = hits[..=i].iter().filter(|h| h.point == hit.point).count() as u64;
                (hit.point, n)
            })
            .collect()
    }

    /// Asserts `delivered` ends with `expected` and that anything before it is a suffix of
    /// `replayable`: every uncommitted record arrives, in order, and only committed ones repeat.
    fn assert_no_loss(delivered: &[String], replayable: &[&str], expected: &[&str], at: &str) {
        let tail = &delivered[delivered.len().saturating_sub(expected.len())..];
        assert_eq!(tail, expected, "crash at {at}: every uncommitted record, in order");
        let head: Vec<&str> =
            delivered[..delivered.len() - tail.len()].iter().map(String::as_str).collect();
        assert!(
            replayable.ends_with(&head),
            "crash at {at}: only committed records may replay, got {delivered:?}"
        );
    }

    /// Three single-record segments with `a` committed and `b` peeked: committing `b` rolls the
    /// cursor out of segment 1.
    async fn roll_setup(dir: &Path) -> DiskQueue {
        let mut cfg = config(dir.to_path_buf());
        cfg.segment_bytes = 1;
        let q = open_with(cfg);
        for label in ["a", "b", "c"] {
            q.push((batch(label), ctx())).await;
        }
        deliver(&q, &["a"]).await;
        let (peeked, _) = peek_within(&q).await.unwrap();
        assert_eq!(marker_of(&peeked), "b");
        q
    }

    #[tokio::test]
    async fn a_crash_at_any_point_of_a_segment_roll_loses_no_uncommitted_record() {
        // Committing `b` persists the cursor (write, fsync, rename, directory fsync), then
        // unlinks segment 1.
        let probe = scratch_dir("roll-crash-probe");
        let q = roll_setup(&probe).await;
        let scope = fault::scope(&probe);
        scope.record();
        q.commit().unwrap();
        let points = crash_points(&scope.hits());
        drop(scope);
        drop(q);
        std::fs::remove_dir_all(&probe).ok();
        assert!(points.iter().any(|(p, _)| *p == SEGMENT_UNLINK), "the roll unlinks: {points:?}");
        assert!(points.iter().any(|(p, _)| p.site == sites::SPOOL_CURSOR), "{points:?}");

        for (point, n) in points {
            let dir = scratch_dir("roll-crash");
            let q = roll_setup(&dir).await;
            let scope = fault::scope(&dir);
            scope.crash_at(point, n);
            q.commit().unwrap();
            assert!(scope.crashed(), "{point:?} #{n} was reached");
            drop(q);
            drop(scope);

            let reopened = open(dir.clone());
            let delivered = drain_all(&reopened).await;
            assert_no_loss(&delivered, &["a", "b"], &["c"], &format!("{point:?} #{n}"));
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    /// Asserts every segment unlink in `hits` comes after a cursor directory `fsync`, so the
    /// cursor leaving that segment is durable first.
    fn assert_unlinks_follow_a_durable_cursor(hits: &[fault::Hit], scenario: &str) {
        let unlinks: Vec<usize> = hits
            .iter()
            .enumerate()
            .filter(|(_, h)| h.point == SEGMENT_UNLINK)
            .map(|(i, _)| i)
            .collect();
        assert!(!unlinks.is_empty(), "{scenario}: a segment is unlinked: {hits:?}");
        let cursor_synced = Point::new(sites::SPOOL_CURSOR, Op::SyncDir);
        for i in unlinks {
            assert!(
                hits[..i].iter().any(|h| h.point == cursor_synced),
                "{scenario}: unlink #{i} precedes any durable cursor: {hits:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_segment_is_unlinked_only_after_the_cursor_leaving_it_is_durable() {
        let one = raw_record(&batch("x"), ctx()).len() as u64;

        // A commit crossing a segment boundary.
        let dir = scratch_dir("unlink-order-commit");
        let q = roll_setup(&dir).await;
        let scope = fault::scope(&dir);
        scope.record();
        q.commit().unwrap();
        assert_unlinks_follow_a_durable_cursor(&scope.hits(), "commit");
        drop(scope);
        std::fs::remove_dir_all(&dir).ok();

        // `drop_oldest` evicting a whole head segment.
        let dir = scratch_dir("unlink-order-evict");
        let mut cfg = config(dir.clone());
        cfg.overflow = OverflowPolicy::DropOldest;
        cfg.segment_bytes = 2 * one;
        cfg.max_bytes = 4 * one;
        let q = open_with(cfg);
        for label in ["a", "b", "c", "d"] {
            q.push((batch(label), ctx())).await;
        }
        let scope = fault::scope(&dir);
        scope.record();
        q.push((batch("e"), ctx())).await;
        assert_unlinks_follow_a_durable_cursor(&scope.hits(), "drop_oldest eviction");
        drop(scope);
        std::fs::remove_dir_all(&dir).ok();

        // A full `block` spool with nothing queued, rotating to make room (F5).
        let dir = scratch_dir("unlink-order-make-room");
        let mut cfg = config(dir.clone());
        cfg.segment_bytes = 3 * one;
        cfg.max_bytes = 3 * one;
        let q = open_with(cfg);
        for label in ["a", "b", "c"] {
            q.push((batch(label), ctx())).await;
        }
        deliver(&q, &["a", "b", "c"]).await;
        let scope = fault::scope(&dir);
        scope.record();
        tokio::time::timeout(Duration::from_secs(5), q.push((batch("d"), ctx())))
            .await
            .expect("a push with nothing queued must not wait for a consumer");
        assert_unlinks_follow_a_durable_cursor(&scope.hits(), "rotation to make room");
        drop(scope);
        std::fs::remove_dir_all(&dir).ok();

        // `open` removing a segment a failed unlink left behind (F4).
        let dir = scratch_dir("unlink-order-open");
        let mut cfg = config(dir.clone());
        cfg.segment_bytes = 1;
        let q = open_with(cfg.clone());
        q.push((batch("a"), ctx())).await;
        q.push((batch("b"), ctx())).await;
        let scope = fault::scope(&dir);
        scope.fail(SEGMENT_UNLINK, errno::EIO);
        deliver(&q, &["a"]).await;
        drop(scope);
        drop(q);
        let scope = fault::scope(&dir);
        scope.record();
        let reopened = open_with(cfg);
        assert_unlinks_follow_a_durable_cursor(&scope.hits(), "open");
        drop(scope);
        drop(reopened);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_failed_cursor_persist_before_an_unlink_loses_nothing_on_reopen() {
        let dir = scratch_dir("cursor-fails-before-unlink");
        let mut cfg = config(dir.clone());
        cfg.segment_bytes = 1;
        let (q, registry, _diag) = open_observed(cfg.clone());
        for label in ["a", "b", "c"] {
            q.push((batch(label), ctx())).await;
        }
        let scope = fault::scope(&dir);
        scope.fail(Point::new(sites::SPOOL_CURSOR, Op::Rename), errno::EIO);

        // Each commit rolls out of a segment: the persist fails, and the unlink still runs.
        deliver(&q, &["a", "b"]).await;
        assert!(disk_errors(&registry, "cursor") >= 2.0);
        assert_eq!(list_segments(&dir).unwrap(), vec![2], "segments 0 and 1 are unlinked");
        drop(scope);
        drop(q);

        // The cursor on disk still names segment 0, which is gone, so `open` falls back to the
        // oldest surviving segment, where `c` waits.
        let reopened = open_with(cfg);
        assert_eq!(drain_all(&reopened).await, vec!["c"]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_segment_left_behind_by_a_failed_unlink_is_removed_at_the_next_open() {
        let dir = scratch_dir("leaked-segment");
        let mut cfg = config(dir.clone());
        cfg.segment_bytes = 1;
        let q = open_with(cfg.clone());
        q.push((batch("a"), ctx())).await;
        q.push((batch("b"), ctx())).await;
        let scope = fault::scope(&dir);
        scope.fail(SEGMENT_UNLINK, errno::EACCES);
        deliver(&q, &["a"]).await; // the cursor leaves segment 0, whose unlink fails
        drop(scope);
        drop(q);
        assert!(segment_path(&dir, 0).exists(), "the failed unlink leaked segment 0");

        // A reopen whose own unlink fails counts it and still leaves the leak out of the bound.
        let scope = fault::scope(&dir);
        scope.fail(SEGMENT_UNLINK, errno::EACCES);
        let (reopened, registry, diag) = open_observed(cfg.clone());
        let events = registry.drain(0);
        assert_eq!(metric_sum(&events, DISK_ERRORS, Some(("op", "unlink"))), 1.0);
        assert_eq!(diag.occurrences("disk_fs_error"), 1);
        let b_len = std::fs::metadata(segment_path(&dir, 1)).unwrap().len() as f64;
        assert_eq!(
            metric_sum(&events, SINK_QUEUE_METRICS.bytes, None),
            b_len,
            "a segment behind the cursor never counts toward max_bytes"
        );
        assert_eq!(metric_sum(&events, DISK_SEGMENTS, None), 1.0);
        drop(scope);
        drop(reopened);

        let (reopened, registry, _diag) = open_observed(cfg);
        assert!(!segment_path(&dir, 0).exists(), "the next open removes it");
        assert_eq!(metric_sum(&registry.drain(0), DISK_SEGMENTS, None), 1.0);
        assert_eq!(drain_all(&reopened).await, vec!["b"]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn finish_after_a_peek_without_commit_replays_the_peeked_head_on_reopen() {
        let dir = scratch_dir("finish-after-peek");
        let q = open(dir.clone());
        for label in ["a", "b", "c"] {
            q.push((batch(label), ctx())).await;
        }
        deliver(&q, &["a"]).await;
        let (peeked, _) = peek_within(&q).await.unwrap();
        assert_eq!(marker_of(&peeked), "b");
        // As when shutdown grace expires mid-delivery: `b` was peeked, never committed.
        q.close();
        q.finish().await;
        drop(q);

        let reopened = open(dir.clone());
        assert_eq!(drain_all(&reopened).await, vec!["b", "c"]);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `a` committed, `b` and `c` queued across two segments, and the queue closed.
    async fn finish_setup(dir: &Path) -> DiskQueue {
        let one = raw_record(&batch("x"), ctx()).len() as u64;
        let mut cfg = config(dir.to_path_buf());
        cfg.segment_bytes = 2 * one;
        let q = open_with(cfg);
        for label in ["a", "b", "c"] {
            q.push((batch(label), ctx())).await;
        }
        deliver(&q, &["a"]).await;
        q.close();
        q
    }

    #[tokio::test]
    async fn a_crash_at_any_point_of_finish_loses_nothing() {
        let probe = scratch_dir("finish-crash-probe");
        let q = finish_setup(&probe).await;
        let scope = fault::scope(&probe);
        scope.record();
        q.finish().await;
        let points = crash_points(&scope.hits());
        drop(scope);
        drop(q);
        std::fs::remove_dir_all(&probe).ok();
        assert!(points.iter().any(|(p, _)| p.site == sites::SPOOL_CURSOR), "{points:?}");
        assert!(points.iter().any(|(p, _)| *p == SEGMENT_SYNC), "{points:?}");
        assert!(points.iter().any(|(p, _)| *p == DIR_SYNC), "{points:?}");

        for (point, n) in points {
            let dir = scratch_dir("finish-crash");
            let q = finish_setup(&dir).await;
            let scope = fault::scope(&dir);
            scope.crash_at(point, n);
            q.finish().await;
            assert!(scope.crashed(), "{point:?} #{n} was reached");
            drop(q);
            drop(scope);

            let reopened = open(dir.clone());
            let delivered = drain_all(&reopened).await;
            assert_no_loss(&delivered, &["a"], &["b", "c"], &format!("{point:?} #{n}"));
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    /// A spool bounded at one segment of three records, filled and fully delivered: the active
    /// segment alone reads as full, with nothing queued.
    async fn consumed_full_spool(
        dir: &Path,
        overflow: OverflowPolicy,
    ) -> (DiskQueue, Arc<Registry>) {
        let one = raw_record(&batch("x"), ctx()).len() as u64;
        let mut cfg = config(dir.to_path_buf());
        cfg.overflow = overflow;
        cfg.segment_bytes = 3 * one;
        cfg.max_bytes = 3 * one;
        let (q, registry, _diag) = open_observed(cfg);
        for label in ["a", "b", "c"] {
            q.push((batch(label), ctx())).await;
        }
        deliver(&q, &["a", "b", "c"]).await;
        registry.drain(0);
        (q, registry)
    }

    #[tokio::test]
    async fn a_blocked_push_makes_room_by_rotating_a_fully_consumed_active_segment() {
        let dir = scratch_dir("block-make-room");
        let (q, registry) = consumed_full_spool(&dir, OverflowPolicy::Block).await;

        let ((), peeked) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(q.push((batch("d"), ctx())), q.peek())
        })
        .await
        .expect("neither the push nor the peek may wait on the other forever");
        assert_eq!(marker_of(&peeked.unwrap().0), "d");
        q.commit().unwrap();

        assert_eq!(list_segments(&dir).unwrap(), vec![1], "the consumed segment is reclaimed");
        let events = registry.drain(0);
        assert_eq!(metric_sum(&events, SINK_QUEUE_METRICS.items_dropped, None), 0.0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_drop_newest_push_makes_room_by_rotating_a_fully_consumed_active_segment() {
        let dir = scratch_dir("drop-newest-make-room");
        let (q, registry) = consumed_full_spool(&dir, OverflowPolicy::DropNewest).await;

        for label in ["d", "e"] {
            q.push((batch(label), ctx())).await;
        }

        let events = registry.drain(0);
        assert_eq!(
            metric_sum(&events, SINK_QUEUE_METRICS.items_dropped, None),
            0.0,
            "a spool with nothing queued has room once its consumed segment is gone"
        );
        assert_eq!(drain_all(&q).await, vec!["d", "e"]);
        std::fs::remove_dir_all(&dir).ok();
    }
}
