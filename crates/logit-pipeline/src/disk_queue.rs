//! A disk-backed, crash-recoverable alternative to [`crate::queue::SinkQueue`]'s in-memory
//! buffer -- see `docs/plans/durable-sink-buffer.md` and `docs/adr/disk-backed-sink-buffer.md`.
//! [`DiskQueue`] presents the same `push`/`peek`/`commit`/`close` shape `SinkQueue` does (wired
//! together behind `crate::queue::SinkStore`), but every batch is appended as a native frame to a
//! segment file before it is eligible for delivery, and a restart resumes from the last
//! checkpointed read cursor.
//!
//! **On-disk layout.** `<dir>/` holds `segment-<seq:016>.lgit` files (a plain concatenation of
//! records, oldest segment lowest sequence number), `cursor.json` (the read cursor: which segment
//! and byte offset the next delivery should read from, tmp+rename like
//! `crates/logit-inputs/src/tail/checkpoint.rs`), and a `lock` file held exclusively
//! (`std::fs::File::try_lock`) for this queue's lifetime -- released automatically on process
//! exit, including `SIGKILL`, so a restart of the same component reopens its own spool without
//! any stale-lock cleanup. One **record** per batch: 24 raw bytes (16-byte `trace_id` + 8-byte
//! `span_id`, [`TraceContext`] inline, no framing of its own) followed by one
//! `logit_proto::frame` native frame. `frame::resync` still works to recover past a corrupt
//! record because it scans for `MAGIC`, which always immediately follows a record's 24 context
//! bytes.
//!
//! **The write cursor is never persisted.** Only the read cursor needs to survive a restart --
//! the write side always resumes at the end of the highest-numbered segment, re-derived by
//! validating that segment frame by frame at [`DiskQueue::open`]. Every *other* segment on disk
//! was already complete and closed before a new one became active (single producer, one write in
//! flight at a time), so only the active segment can ever have a torn tail.
//!
//! **`commit` stays synchronous**, exactly like `SinkQueue::commit` -- it only ever mutates
//! in-memory cursor state plus, occasionally, one small blocking cursor-file write and one file
//! deletion (never a segment read or write), the same brief-blocking trade-off
//! `crates/logit-inputs/src/tail/checkpoint.rs`'s own `write` already makes from call sites that
//! are themselves async elsewhere in this codebase.

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

use crate::fanout::TraceContext;
use crate::queue::{OverflowPolicy, SINK_QUEUE_METRICS};
use logit_core::{Diagnostics, EventBatch, Telemetry};
use logit_proto::frame::{self, Compression, MAX_SANE_UNCOMPRESSED_LEN};
use logit_proto::native;
use logit_proto::CodecError;

/// `[trace_id: 16][span_id: 8]`, ahead of the frame -- see the module doc.
const CONTEXT_LEN: usize = 24;

const LOCK_FILE_NAME: &str = "lock";
const CURSOR_FILE_NAME: &str = "cursor.json";
const CURSOR_VERSION: u32 = 1;

/// The first read attempt per fresh (uncached) peek -- comfortably larger than any batch this
/// project's own fixtures produce (`docs/design/memory.md`), so the common case is one disk read.
/// Grown by exactly [`CodecError::Truncated`]'s `needed` hint on the rare record that doesn't
/// fit, rather than reading a whole segment (up to `segment_bytes`) for one record.
const READ_CHUNK_INITIAL: usize = 8 * 1024;

const DISK_SEGMENTS: &str = "logit.component.buffer.disk.segments";
const DISK_REPLAYED: &str = "logit.component.buffer.disk.replayed";
const DISK_TRUNCATED: &str = "logit.component.buffer.disk.truncated";

/// Bounds and behavior for one sink's disk spool. Disk *replaces* memory for that sink rather
/// than sizing beside it (`docs/adr/disk-backed-sink-buffer.md`) -- there is no `max_batches`
/// analogue here; `max_bytes` alone bounds the sum of on-disk segment sizes.
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

/// Every `segment-<seq>.lgit` currently in `dir`, ascending by sequence number. A file that
/// doesn't match the naming pattern is silently skipped -- nothing this queue writes should ever
/// produce one, and an operator poking around the directory is not this queue's problem to crash
/// over.
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

/// Parses one record -- `[24-byte context][native frame]` -- off the front of `buf`. `Truncated`
/// means `buf` simply doesn't hold a whole record yet (ran out of bytes, benign); every other
/// error means the bytes present are provably wrong and the caller should resync
/// (`docs/design/wire-protocol.md`).
fn parse_record(buf: &[u8]) -> Result<(TraceContext, Arc<EventBatch>, usize), CodecError> {
    if buf.len() < CONTEXT_LEN {
        return Err(CodecError::Truncated { needed: CONTEXT_LEN - buf.len() });
    }
    let ctx = decode_context(&buf[..CONTEXT_LEN]);
    let mut rest = Bytes::copy_from_slice(&buf[CONTEXT_LEN..]);
    let before = rest.len();
    let (codec, mut payload) = frame::read_frame(&mut rest)?;
    if codec != native::CODEC_NATIVE_V1 {
        return Err(CodecError::Malformed(format!(
            "disk record declares codec {codec}, expected native v1 ({})",
            native::CODEC_NATIVE_V1
        )));
    }
    let consumed_frame = before - rest.len();
    let batch = native::decode_batch(&mut payload)?;
    Ok((ctx, Arc::new(batch), CONTEXT_LEN + consumed_frame))
}

/// The result of walking every record in a byte range: how far into it good data reaches, how
/// many records parsed cleanly, and how many were skipped for corruption.
struct WalkOutcome {
    /// Absolute offset (from byte 0 of the segment) up to which data is confirmed good --
    /// everything from here to the end of the walked bytes is either a torn tail (nothing more
    /// to parse) or unrecoverable corruption with no further resync target.
    good_len: u64,
    valid_count: u64,
    corrupt_skipped: u64,
}

/// Walks every record in `bytes` starting at `start_offset` (an absolute offset into `bytes`,
/// i.e. `bytes[0]` is the segment's own byte 0), invoking `on_record` for each one parsed
/// cleanly. Stops at the first short read (a torn tail or a clean end -- indistinguishable from
/// the bytes alone; the caller decides which by comparing `good_len` against the file's actual
/// on-disk length). A parse failure that isn't simply "ran out of bytes" resyncs forward past it
/// (`frame::resync`, corrected for the 24-byte context prefix it doesn't know about) and keeps
/// going, tolerating a spurious `MAGIC` match by trying the candidate and continuing the scan
/// past it if that candidate doesn't parse either (`frame::resync`'s own documented caveat).
fn walk_segment(
    bytes: &[u8],
    start_offset: u64,
    mut on_record: impl FnMut(u64, TraceContext, Arc<EventBatch>, u64),
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

/// Loads the persisted read cursor, tolerating everything the way
/// `crates/logit-inputs/src/tail/checkpoint.rs::CheckpointStore::load` does: missing is the
/// ordinary first-run case (no diagnostic), a version mismatch or malformed file is diagnosed and
/// treated as absent -- never fatal to opening the queue.
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

/// Persists the read cursor via tmp+rename, exactly `checkpoint.rs`'s idiom.
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

/// Whether `err` means the filesystem is out of space -- checked via the raw OS errno (`ENOSPC`
/// is `28` on Linux, the only platform this project targets, `docs/adr/containerized-development.md`)
/// rather than `io::ErrorKind::StorageFull` alone, since that variant's exact stabilization and
/// exhaustiveness across platforms is not something to depend on here.
fn is_disk_full(err: &io::Error) -> bool {
    err.raw_os_error() == Some(28)
}

#[derive(Clone, Copy)]
struct Segment {
    seq: u64,
    /// The confirmed-good length: for every segment but the active one, its actual on-disk size
    /// (trusted -- see the module doc on why only the active segment can be torn); for the active
    /// one, the length validated at [`DiskQueue::open`], kept current by every successful
    /// [`DiskQueue::push`] afterward.
    len: u64,
}

struct HeadCache {
    batch: Arc<EventBatch>,
    ctx: TraceContext,
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
    /// Set just before a write starts, cleared just after it completes -- a `push` future dropped
    /// mid-write (`run_output`'s `select!` can drop `drain_inbox`, and with it whatever `push`
    /// call it was mid-`.await` on, when `write_loop` finishes first) leaves this `true`, and the
    /// *next* call to [`DiskQueue::push`] repairs the tail (truncates back to
    /// `write_len_before_flight`) before writing anything new. A crash is the same situation
    /// discovered at the next [`DiskQueue::open`] instead.
    write_in_flight: bool,
    write_len_before_flight: u64,
    /// Set alongside `write_in_flight` staying `true` on a failed write, so `push` can classify
    /// the drop it counts for that batch. Meaningless when `write_in_flight` is `false`.
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
    /// Held for this queue's whole lifetime -- an OS-level advisory lock
    /// (`std::fs::File::try_lock`), released automatically when this file closes, including on
    /// `SIGKILL`. What catches two sinks (mistakenly) configured with the same `disk.path`: graph
    /// validation only compares literal path strings (`crates/logit-pipeline/src/graph.rs` rule
    /// 34), so an aliased path (`./spool` vs `spool`) reaches here instead.
    _lock: StdFile,
}

impl DiskQueue {
    /// Opens (or creates) the spool at `config.dir`, recovering from whatever a previous process
    /// left behind. Blocking (`std::fs` throughout) -- a one-time startup cost, the same
    /// trade-off `crates/logit-outputs/src/file.rs::FileTarget::open` and
    /// `checkpoint.rs::CheckpointStore::load` already make for the same reason: this runs once,
    /// before the pipeline starts delivering anything, not on any hot path.
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

        // Replay count: walk every record from the resume point to the end of every segment,
        // across segment boundaries. The active segment's bytes were already loaded above when
        // it needed validating; every other segment in range is read fresh here.
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

        // Persist once more so cursor.json reflects any clamping done above, forced regardless of
        // `checkpoint_interval` -- this is a one-time startup write, not the hot path that gate
        // protects.
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

    /// Appends `item` to the active segment, rotating first if it already exceeds
    /// `segment_bytes` (a soft trigger checked *before* writing, not a hard cap -- see the module
    /// doc: this is what lets a single record larger than the trigger still land whole in a
    /// fresh segment rather than being unwritable).
    ///
    /// **Cancellation safety.** The record is encoded fully in memory first; the only `.await` in
    /// the write path is one `write_all` under `write_in_flight = true`
    /// (see [`State::write_in_flight`]). This mirrors
    /// `crates/logit-outputs/src/syslog.rs::send_tcp`'s take-before-write shape, adapted for a
    /// single always-appending file rather than a reconnectable stream.
    pub async fn push(&self, item: (Arc<EventBatch>, TraceContext)) {
        let (batch, ctx) = item;

        let payload = native::encode_batch(&batch);
        if payload.len() > MAX_SANE_UNCOMPRESSED_LEN as usize {
            self.count_dropped("frame_too_large", batch.events.len() as u64);
            return;
        }
        // `write_frame` only ever fails for `Compression::Zstd`, which nothing on the path from
        // config to here can produce: `logit_config::Compression` (the only place an operator's
        // `disk.compression` is read from) has no `Zstd` variant at all.
        let framed = frame::write_frame(native::CODEC_NATIVE_V1, self.compression, &payload)
            .expect("logit_config::Compression excludes Zstd; write_frame only fails for Zstd");
        let mut record = Vec::with_capacity(CONTEXT_LEN + framed.len());
        record.extend_from_slice(&encode_context(ctx));
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
                        OverflowPolicy::DropOldest => {
                            let seg_len = state
                                .segments
                                .iter()
                                .find(|s| s.seq == state.read_seq)
                                .map(|s| s.len);
                            let evictable = state.head_cache.is_none()
                                && seg_len.map(|l| state.read_offset < l).unwrap_or(false);
                            if evictable {
                                Action::Evict
                            } else {
                                // Nothing evictable ahead of the reserved (peeked) head, or
                                // nothing evictable at all -- accept over-bound rather than block
                                // or loop forever, the same escape hatch
                                // `crate::queue::BoundedQueue::push` documents for the
                                // impossible-to-ever-fit case.
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
                        // Raced: a concurrent peek reserved the head, or it was already
                        // consumed, between the check above and now. Accept anyway.
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
            // The write itself failed (e.g. `ENOSPC`) -- `write_record` has already left
            // `write_in_flight` set for the next call to repair, and logged why. The batch was
            // never durably written, so it must not be counted as queued: drop it here, under
            // `disk_full` when the cause was actually running out of space (the one case the
            // block/drop_oldest/drop_newest policies above can't have prevented, since none of
            // them can free real disk space) and a generic reason otherwise.
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

    /// The write path proper: repair a torn tail from a previously-cancelled call, rotate if the
    /// active segment is already over `segment_bytes`, then append `record`. Returns whether the
    /// record actually landed durably in the active segment.
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
                        // Nothing was written -- no torn state to repair, unlike a failed
                        // `write_all` below.
                        state.write_in_flight = false;
                        state.last_write_error_disk_full = is_disk_full(&err);
                        return false;
                    }
                }
            }
        };

        let result = file.write_all(record).await;

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
                // Leave `write_in_flight = true` and `write_file = None` -- the next push
                // repairs by truncating back to the length recorded before this attempt.
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

    /// Closes out the current active segment (fsync it and the directory, so its directory entry
    /// and every byte written to it are durable) and starts a new one.
    async fn rotate_segment(&self) {
        let old_seq = {
            let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            let old_seq = state.segments.back().expect("always at least one segment").seq;
            state.write_file = None;
            old_seq
        };
        let _ = fsync_path(&segment_path(&self.dir, old_seq)).await;
        let new_seq = old_seq + 1;
        let new_path = segment_path(&self.dir, new_seq);
        if tokio::fs::File::create(&new_path).await.is_ok() {
            let _ = fsync_path(&self.dir).await;
            let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            state.segments.push_back(Segment { seq: new_seq, len: 0 });
        }
    }

    /// `DropOldest` under a full queue: advances the read cursor past the whole head record
    /// without delivering it, exactly like an in-memory `SinkQueue`'s own eviction. Re-checks
    /// (after the async read, under the lock) that the head is still the same, unreserved record
    /// it started with before actually advancing -- a concurrent `peek` could have reserved it
    /// in the meantime. Returns whether it actually evicted anything.
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

    /// Reads one record at `(seq, offset)`, growing the read buffer using
    /// [`CodecError::Truncated`]'s `needed` hint rather than reading the rest of the segment --
    /// most records fit in [`READ_CHUNK_INITIAL`]. Resyncs past corruption discovered live (a
    /// live segment should never actually be corrupt in ordinary operation -- see the module
    /// doc -- so this is defense in depth, not the primary recovery path, which is
    /// [`DiskQueue::open`]).
    async fn read_record_at(
        &self,
        seq: u64,
        offset: u64,
    ) -> Option<(TraceContext, Arc<EventBatch>, u64)> {
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
                        // Real end of segment -- nothing more to read here.
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
                    let mut found = None;
                    let outcome = walk_segment(&whole, 0, |_, ctx, batch, len| {
                        if found.is_none() {
                            found = Some((ctx, batch, len));
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

    /// Advances the read cursor past one record of `record_len` bytes, in-memory only, rolling
    /// into the next segment (and deleting the one just fully consumed) if that lands exactly at
    /// a segment boundary. Force-persists the cursor before deleting a segment -- never the other
    /// order -- so `cursor.json` can never reference a file that no longer exists. Sync: the only
    /// I/O is one small blocking JSON write and (occasionally) one blocking file removal.
    fn advance_read_cursor(&self, record_len: u64) {
        let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        state.head_cache = None;
        let old_seq = state.read_seq;
        let mut offset = state.read_offset + record_len;
        let mut seq = old_seq;
        let is_active = state.segments.back().map(|s| s.seq) == Some(old_seq);
        let seg_len = state.segments.iter().find(|s| s.seq == old_seq).map(|s| s.len);
        let mut to_delete = None;
        if !is_active && seg_len.map(|l| offset >= l).unwrap_or(false) {
            to_delete = Some(old_seq);
            seq += 1;
            offset = 0;
        }
        state.read_seq = seq;
        state.read_offset = offset;

        if let Some(deleted_seq) = to_delete {
            persist_cursor(&self.dir, seq, offset, &mut state.diag);
            state.last_checkpoint = Instant::now();
            if let Some(pos) = state.segments.iter().position(|s| s.seq == deleted_seq) {
                if let Some(removed) = state.segments.remove(pos) {
                    state.total_bytes = state.total_bytes.saturating_sub(removed.len);
                }
            }
            if let Some((cached_seq, _)) = &state.read_file {
                if *cached_seq == deleted_seq {
                    state.read_file = None;
                }
            }
            let dir = self.dir.clone();
            drop(state);
            let _ = std::fs::remove_file(segment_path(&dir, deleted_seq));
            self.not_full.notify_one();
        } else if state.last_checkpoint.elapsed() >= self.checkpoint_interval {
            persist_cursor(&self.dir, seq, offset, &mut state.diag);
            state.last_checkpoint = Instant::now();
        }
    }

    /// The head, without removing it -- cached until [`DiskQueue::commit`] so a retry
    /// (`write_loop` calls this once per delivery attempt) costs nothing after the first. `None`
    /// once closed and empty.
    pub async fn peek(&self) -> Option<(Arc<EventBatch>, TraceContext)> {
        loop {
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
                    // Nothing readable where accounting said there should be -- a genuinely
                    // exceptional case (see the module doc's cancellation-safety notes); brief
                    // backoff rather than a tight spin, then re-evaluate from scratch.
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }
    }

    /// Advances the read cursor past the currently-cached head, returning it. A no-op (`None`)
    /// with nothing cached -- mirrors `SinkQueue::commit`'s contract exactly, including staying
    /// synchronous (see the module doc).
    pub fn commit(&self) -> Option<(Arc<EventBatch>, TraceContext)> {
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

    /// Persists the cursor (forced, regardless of `checkpoint_interval`), `fsync`s it, the active
    /// segment, and the directory, and closes files. Drops nothing -- a disk-backed sink's
    /// shutdown grace only bounds how long delivery keeps running, never what's still queued.
    pub async fn finish(&self) {
        let active_seq = {
            let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            persist_cursor(&self.dir, state.read_seq, state.read_offset, &mut state.diag);
            state.write_file = None;
            state.read_file = None;
            state.segments.back().map(|s| s.seq)
        };
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
}

#[cfg(test)]
mod tests {
    use super::test_support::scratch_dir;
    use super::*;
    use logit_core::{AttrMap, Event, Registry, Resource, Value};

    fn ctx() -> TraceContext {
        TraceContext::new_root()
    }

    fn batch(marker: &str) -> Arc<EventBatch> {
        let mut attrs = AttrMap::new();
        attrs.insert("marker", Value::str(marker));
        Arc::new(EventBatch {
            resource: Arc::new(Resource::default()),
            events: vec![Event::empty(0, attrs)],
        })
    }

    fn marker_of(batch: &EventBatch) -> String {
        batch.events[0].attributes.get("marker").and_then(Value::as_str).unwrap().to_string()
    }

    /// Sums every counter point named `name` across drained telemetry events, optionally
    /// filtered to those carrying `(tag_key, tag_value)` -- `ComponentBuffer::drain` puts a
    /// metric's tags on the drained `Event`'s attributes and its own name on `MetricRecord::name`
    /// (an interned `Symbol`, not a string attribute), so both have to be checked this way.
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
                logit_core::MetricKind::Counter(v) | logit_core::MetricKind::Gauge(v) => *v,
                _ => 0.0,
            })
            .sum()
    }

    fn config(dir: PathBuf) -> DiskQueueConfig {
        DiskQueueConfig {
            dir,
            max_bytes: 10 * 1024 * 1024,
            // Small enough that a handful of tiny test batches force real rotation.
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

    /// Builds the exact on-disk bytes `DiskQueue::push` would write for one record -- reused by
    /// tests that hand-construct or hand-corrupt segment files directly.
    fn raw_record(batch: &EventBatch, ctx: TraceContext) -> Vec<u8> {
        let payload = native::encode_batch(batch);
        let framed = frame::write_frame(native::CODEC_NATIVE_V1, Compression::None, &payload)
            .expect("None compression never fails");
        let mut record = Vec::with_capacity(CONTEXT_LEN + framed.len());
        record.extend_from_slice(&encode_context(ctx));
        record.extend_from_slice(&framed);
        record
    }

    #[tokio::test]
    async fn push_then_peek_then_commit_round_trips_fifo_across_a_segment_boundary() {
        let dir = scratch_dir("fifo");
        let q = open(dir.clone());

        // `segment_bytes: 200` and each of these batches encodes to well under that, so pushing
        // several forces at least one real rotation.
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
            // Commit only the first -- the rest should still be there after a reopen.
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
        // Half of the second record only -- a torn write.
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
        // Flip a payload byte without touching the frame header, so the header parses fine but
        // the checksum fails -- `frame::read_frame`'s own `rejects_corrupt_crc` test does the
        // same thing.
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

        // A trace_id whose bytes happen to contain `MAGIC` -- `frame::resync`'s own doc warns a
        // reader must expect and tolerate exactly this spurious match.
        let mut spurious_ctx = ctx();
        spurious_ctx.trace_id[4..8].copy_from_slice(&frame::MAGIC);
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

    #[tokio::test]
    async fn drop_oldest_never_evicts_a_record_currently_peeked() {
        let dir = scratch_dir("drop-oldest-reserved");
        let mut cfg = config(dir.clone());
        cfg.overflow = OverflowPolicy::DropOldest;
        let one = raw_record(&batch("x"), ctx()).len() as u64;
        cfg.max_bytes = one + one / 2;
        let q = open_with(cfg);

        q.push((batch("first"), ctx())).await;
        let (peeked, _) = q.peek().await.expect("should peek the only batch");
        assert_eq!(marker_of(&peeked), "first");

        // Nothing else exists to evict ahead of the reserved head, so this push is accepted
        // over-bound rather than evicting the batch a caller (write_loop, here just this test)
        // is currently holding.
        q.push((batch("second"), ctx())).await;

        let (still_first, _) = q.commit().expect("the peeked batch must still be first's");
        assert_eq!(marker_of(&still_first), "first");
        let (second, _) = q.peek().await.expect("second should still be there too");
        assert_eq!(marker_of(&second), "second");
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

        // Give the (synchronous, but internal) deletion a moment to land -- `commit` itself is
        // sync and returns only after the delete already happened, so this should already be
        // true immediately, but a tiny yield keeps this robust to any future scheduling change.
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

        // Replace the active segment with a directory of the same name -- opening it for append
        // then fails with `EISDIR`, a type mismatch rather than a permission check, so it fails
        // the same way whether the test runs as an ordinary user or as root (a `chmod`-readonly
        // file would silently succeed for root, e.g. CI's containers, which is exactly what a
        // first version of this test got wrong). Exercises the same "batch never actually
        // landed" path a real write failure (permissions, `ENOSPC`, the device gone) would.
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
}
