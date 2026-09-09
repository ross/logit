---
created: 2026-09-09
updated: 2026-09-09
---

# Disk-backed durable buffering for a sink's delivery queue

## Status
Accepted

## Context

ADR [`buffered-sink-delivery`](buffered-sink-delivery.md) put every sink behind a bounded,
in-memory `SinkQueue` and explicitly rejected disk backing *for that ADR* because it needed an
`EventBatch` serialization that hadn't been decided yet. That blocker is gone: ADR
[`native-wire-format-encoding`](native-wire-format-encoding.md) shipped `logit_proto::native`,
whose own module doc says "this is the same format for a socket and a file … a file is a plain
concatenation of frames — append, sequential read, `frame::resync` past a torn write," and ADR
[`file-output-native-format`](file-output-native-format.md) already writes those frames to disk
from `stdio_out`/`file_out`. `docs/known-gaps.md`'s "No durable (disk-backed) buffering" named
this as real, unblocked follow-up work. This ADR designs and closes it for the sink side; the
listener side (`ReceiveQueue`) stays in-memory and is a separate, still-open gap.

`docs/plans/durable-sink-buffer.md` is the original design sketch this ADR implements, with five
corrections made against the actual code (see "Corrections to the sketch" below) and one seam
(`crate::queue::SinkStore`) that replaced the sketch's proposed `pub enum SinkStore` shape with a
slightly different, narrower method surface once the implementation showed what `write_loop` and
`drain_inbox` actually call.

## Decision

**Scope.** An opt-in `buffer.disk:` block on any sink (`logit_config::DiskBufferConfig`). When
set, the sink's queue *is* a disk-backed spool (`crates/logit-pipeline/src/disk_queue.rs`,
`DiskQueue`): every batch is appended as a native frame to a segment file before it is eligible
for delivery, the writer reads from a persisted cursor, and a process restart (clean or
`SIGKILL`) resumes delivery from the last committed cursor, replaying at most the batches
committed since the last checkpoint — at-least-once, the same trade `tail_in`'s own checkpoint
already makes.

**Memory vs disk.** Disk *replaces* memory for that sink, not a spill tier. A two-tier "memory
until full, then disk" design has to answer how ordering survives a batch sitting in memory while
older ones are on disk, and makes "what survives a crash" depend on timing. Vector's model (a
buffer is memory *or* disk) is the precedent. `buffer.max_batches`/`max_bytes` are rejected when
non-default alongside `buffer.disk` (graph rule 34) — a knob that would silently do nothing is a
config error, the same reasoning rule 33 already applies to `stdio_out`/`file_out`'s
`compression:`.

**What is persisted.** One record per batch: `[TraceContext: 16-byte trace_id, 8-byte
span_id][native frame]`. The context rides inline so a batch replayed within the same process
keeps its sink-span parent; after a restart it is still valid (a trace id is just bytes).
`frame::resync` still works because it scans for `MAGIC`, which always immediately follows the
fixed 24-byte context prefix.

**File layout.** `<path>/` is a directory: `segment-<seq:016>.lgit` files, `cursor.json`
(`{version, segment, offset}` — see "the write cursor is never persisted" below for why this is
narrower than the sketch's `{version, segment, offset, committed_seq}`), written tmp+rename
exactly like `crates/logit-inputs/src/tail/checkpoint.rs`, and a `lock` file held exclusively
(`std::fs::File::try_lock`, stable since well before this project's pinned toolchain) for the
queue's process lifetime — released automatically on `SIGKILL`, so a restart of the same
component reopens its own spool with no stale-lock cleanup, and two sinks aliasing the same
`disk.path` (`./spool` vs `spool`, which graph validation's literal-string comparison can't see)
fail at open instead of corrupting each other. A new segment starts once the active one already
exceeds `segment_bytes` (checked before writing, not a hard cap — so a single record larger than
the trigger still lands whole in a fresh segment). A segment is deleted once the read cursor has
fully crossed it.

**Bound and overflow.** `disk.max_bytes` (default 1 GiB) over the sum of on-disk segment sizes.
`buffer.overflow` keeps its meaning: `block` (default) awaits room; `drop_oldest` advances the
read cursor past whole head records (counted `batches.dropped{reason="overflow_oldest"}`, the
same reason the in-memory queue uses) and never evicts a record a concurrent `peek` has reserved;
`drop_newest` rejects the push outright. A write that fails outright — the filesystem genuinely
out of space, a permissions error, the device gone — never leaves the batch silently counted as
queued: it is dropped and counted (`reason="disk_full"` when the OS reports `ENOSPC` specifically,
`reason="disk_io_error"` otherwise), under every overflow policy, since none of them can free real
disk space. `write_in_flight` still gets set so the next push repairs any partial bytes the failed
attempt left behind, exactly as a cancelled push does.

**Durability.** `fdatasync` (`tokio::fs::File::sync_data`) on segment rotation, on the cursor
file, and at shutdown — not per push. Power loss can lose the tail of the active segment; process
death cannot lose anything already `write`n, since the kernel page cache survives the process.
Per-push fsync is a possible later `disk.sync: every_push` knob, not built here.

**Recovery.** Only the highest-numbered (active) segment can ever be torn — every other segment
was already complete and closed before a new one became active, since there is exactly one
producer and one write in flight at a time. `DiskQueue::open` validates that one segment frame by
frame, truncating at the last good boundary (counted `logit.component.buffer.disk.truncated`) and
resyncing forward past mid-file corruption (`batches.dropped{reason="disk_corrupt"}`), tolerating
a spurious `MAGIC` match inside a `trace_id`'s bytes the way `frame::resync`'s own doc already
warns a caller must. A missing, corrupt, or stale cursor (referencing a segment that no longer
exists) falls back to the oldest surviving segment at offset `0`, diagnosed under a `cursor_error`
key, never fatal — the same posture `checkpoint.rs::CheckpointStore::load` already takes. Records
found between the resume point and the end of all segments at open count
`logit.component.buffer.disk.replayed`.

**Push cost.** Push = encode (`native::encode_batch` + `frame::write_frame`) + one `write_all` to
the active segment. This breaks the `buffered-sink-delivery` ADR's zero-clone `Arc<EventBatch>`
property for disk-backed sinks *by design* — durability is what the operator opted into. An
encoded frame over `logit_proto::frame::MAX_SANE_UNCOMPRESSED_LEN` is dropped and counted
(`batches.dropped{reason="frame_too_large"}`) rather than written unreadably — `frame.rs`'s own
doc comment on that constant names exactly this case as "worth an encode-side assertion once the
durable-buffer work starts."

**Peek cost.** Peek = read + decode the head record, cached until `commit()` so `write_loop`'s
per-attempt retry loop re-decodes nothing. The first (uncached) read grows its buffer using
`CodecError::Truncated`'s `needed` hint rather than reading a whole segment for one record.

**Runtime seam.** `pub enum SinkStore { Memory(SinkQueue), Disk(Box<DiskQueue>) }` in `queue.rs`
(boxed: `DiskQueue` is far larger than `SinkQueue`, and `clippy::large_enum_variant` is right that
leaving it unboxed would pad the common `Memory` case out to `DiskQueue`'s size), with inherent
`push`/`peek`/`commit`/`close`/`finish` methods `crate::runtime`'s `drain_inbox`/`write_loop` call
identically regardless of variant. Enum dispatch, no `dyn`, no generic over `run_output`.
`NodeSpec::Output` carries a `SinkStoreConfig`, and `logit-cli::pipeline::queue_config` resolves
`disk.path` against the config's own `base_dir`, exactly like `StdioTarget::Path`/`FileOut::path`.

**Shutdown.** With disk, `SinkStore::finish` persists the cursor and closes files — it drops
**nothing** and counts nothing as `reason="shutdown"`; the shutdown grace only bounds how long
`write_loop` keeps *delivering*. The abandoned-inbox sweep in `run_output` still applies (those
batches never reached the spool) — with `disk:` on, it appends them to the spool instead of
counting them dropped.

**Frame compression.** `disk.compression: none | lz4` (reuses `logit_config::Compression`,
already added for `stdio_out`/`file_out`'s own `format: native`), default `none` — disk is cheap,
decode latency on the hot path matters more.

**Not a `Buffer<T>` impl.** `logit_proto::buffer::Buffer<T>` is sync, `&mut self`, and generic
over `T`; a disk queue is async (file I/O) and concrete over `(Arc<EventBatch>, TraceContext)`.
`DiskQueue` implements its own async surface directly rather than that trait. The trait stays for
`InMemoryBuffer`; this ADR records that the "trait boundary that was cheap to add" turned out to
be the wrong seam for disk, which is exactly the risk `buffered-sink-delivery`'s own Alternatives
section flagged when it deferred disk backing rather than force-fitting it through `Buffer<T>`.

## Corrections to the design sketch

`docs/plans/durable-sink-buffer.md` proposed five things that didn't survive contact with the
actual code; each is resolved as follows.

1. **No `len`/`is_empty` on the seam.** `BoundedQueue` (the in-memory queue's own async wrapper)
   has neither, and nothing in `drain_inbox`/`write_loop`/`finish_and_flush` calls them. The
   `SinkStore` surface is exactly `push`/`peek`/`commit`/`close`/`finish`.
2. **`commit` stays synchronous.** The sketch implied a disk `commit` might do I/O. It doesn't:
   `commit` only ever advances an in-memory read cursor, persisting the cursor file (a small,
   brief blocking write, the same trade-off `checkpoint.rs`'s own `write` already makes from async
   call sites) only on a segment crossing or when `checkpoint_interval` has elapsed. This means
   `drain_inbox`/`write_loop` need no signature change beyond `Arc<SinkQueue>` becoming
   `Arc<SinkStore>` — no new `.await` points.
3. **Rule 34 compares literal paths, not canonical ones.** `graph::resolve(config)` never receives
   `base_dir` (it runs before any path resolution), so the graph-time uniqueness check compares
   `disk.path` strings as written, sorted for a deterministic error message the way rule 13
   already does for `internal` components. `DiskQueue::open`'s exclusive lock catches the aliased
   case (`./spool` vs `spool`) at startup instead.
4. **A torn write is repaired on the next push, not left to `resync` alone.** There is exactly one
   producer per spool, so `DiskQueue` tracks a `write_in_flight` flag and the last known-good
   segment length; a push that finds the flag set truncates back to that length before appending.
   `frame::resync` remains the crash-time fallback, where no in-memory state survived.
5. **An oversized frame is rejected at push, never written.** See "Push cost" above.

## Alternatives considered

- **A memory-then-disk spill tier.** Rejected: ordering and crash semantics become
  timing-dependent (what was in memory vs. already spilled at the moment of a crash), and Vector's
  memory-*or*-disk precedent avoids that class of bug entirely.
- **Implementing `logit_proto::buffer::Buffer<T>` for the disk queue.** Rejected: that trait is
  sync/`&mut self`/generic, the wrong seam for async, concrete, file-backed state. See "Not a
  `Buffer<T>` impl" above.
- **Per-push `fsync`.** Rejected for now: real durability at a real throughput cost, better left
  as an opt-in `disk.sync: every_push` knob once there's a concrete deployment asking for it —
  filed in `docs/known-gaps.md`.
- **A memory-mapped segment file.** Rejected: no new dependency and no real benefit over
  sequential `write_all`/`read` for an append-then-sequential-read access pattern; memmap's value
  is random access, which this workload never does.
- **Storing the write cursor alongside the read cursor in `cursor.json`.** Rejected: the write side
  never needs to survive a restart on its own — it always resumes at the true end of the highest-
  numbered segment, re-derived by validating that one segment at open (see "Recovery" above). A
  second persisted cursor would be one more thing that could drift from reality, for no benefit.

## Consequences

- `crates/logit-proto`: `CodecError::Truncated { needed }`, `FrameHeader::read` and
  `MAX_SANE_UNCOMPRESSED_LEN` now `pub` — shared prerequisite with `docs/plans/native-transport.md`
  workstream A.
- `crates/logit-pipeline`: new `disk_queue.rs` (`DiskQueue`, `DiskQueueConfig`); `queue.rs` gains
  `SinkStore`/`SinkStoreConfig`; `runtime.rs`'s `NodeSpec::Output`, `run_output`, `drain_inbox`,
  `write_loop`, `finish_and_flush` operate on `SinkStore` instead of `SinkQueue` directly;
  `crates/logit-bench`'s allocation tests updated to the `Memory` arm.
- `crates/logit-config`: `BufferConfig.disk: Option<DiskBufferConfig>`; new `DiskBufferConfig`
  (presence of `path` is the on-switch, following `TlsServerConfig`'s precedent);
  `schema/logit.schema.json` regenerated.
- `crates/logit-pipeline/src/graph.rs`: rule 34 (the shape above);
  `docs/design/pipeline-graph.md`'s rule list gains it.
- `crates/logit-cli/src/pipeline.rs`: `queue_config` returns `SinkStoreConfig` and resolves
  `disk.path` against `base_dir`.
- Measured cost: `disk_queue_push_one_batch` (encode + write) and
  `disk_queue_peek_cached_costs_nothing` in `crates/logit-bench/tests/allocations.rs`;
  `docs/design/memory.md` §2/§5 updated in the same commit, per this repo's exact-equality
  discipline.
- `docs/known-gaps.md`: the durable-buffering entry narrows to receive-side-only.
- `docs/deploying.md`: a new "Durable buffering" subsection under "Sink delivery buffering."
- Explicitly out of scope, filed as new `docs/known-gaps.md` entries: receive-side (`ReceiveQueue`)
  disk backing, per-push fsync, encryption at rest, a spool shared across sinks, segment
  compaction/rewrite, out-of-order acknowledgement (window > 1).
