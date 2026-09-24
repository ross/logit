---
created: 2026-09-24
updated: 2026-09-24
---

# Durable checkpoint writes, observed spool I/O failures, and a feature-gated fault-injection seam

## Status
Accepted

## Context

Four pieces of code decide what survives a `SIGKILL` or a power loss: the disk spool
([ADR `disk-backed-sink-buffer`](disk-backed-sink-buffer.md),
`crates/logit-pipeline/src/disk_queue.rs`), its read cursor, the tail checkpoint
([ADR `file-tailing-and-docker-json-logs`](file-tailing-and-docker-json-logs.md),
`crates/logit-inputs/src/tail/checkpoint.rs`), and `file_out` rotation
([ADR `rotating-file-output`](rotating-file-output.md), `crates/logit-outputs/src/file.rs`).
`docs/plans/critical-sections-inventory.md` groups them as cluster 2, "Durability" (DISK-01..06,
DISK-09, DISK-10, DISK-13, TAIL-05). Reading them against their ADRs found this:

- **Neither checkpoint write is durable.** `disk_queue::persist_cursor` and
  `tail::checkpoint::CheckpointStore::write` both write a tmp file with `std::fs::write` and then
  `std::fs::rename` it over the target. Neither fsyncs the tmp file before the rename or the
  directory after it. Both name the tmp file with `Path::with_extension("tmp")`, so two targets
  that differ only in extension share one tmp path. The spool's `DiskQueue::finish` fsyncs the
  cursor at shutdown; nothing else does, although the disk ADR's "Durability" section and
  `docs/known-gaps.md` both say the cursor file is `fdatasync`ed.
- **The two checkpoints fail differently after a power loss.** A spool cursor that is missing,
  unparseable, or names a segment that no longer exists falls back to the oldest surviving segment
  at offset 0 (`DiskQueue::open`), so a bad cursor costs duplicates, never loss. A tail checkpoint
  that is present but unreadable, malformed, or the wrong version is treated as missing
  (`CheckpointStore::load`), so every file present at the first scan starts at `read_from`, whose
  default is `end`. That skips everything the files gained while `logit` was down: silent loss,
  against the tailing ADR's "strictly duplicates on restart".
- **Spool I/O failures are invisible.** `DiskQueue::rotate_segment` discards the results of its
  flush, the old segment's fsync, the directory fsync, and (through `is_ok()`) the new segment's
  create. `DiskQueue::finish` discards its flush and all three fsyncs. `DiskQueue::roll_read_cursor`
  discards the segment unlink's result and removes the segment from memory either way.
  `persist_cursor` only reports through a throttled `cursor_error` diagnostic. None of these is
  counted.
- **`file_out` fsyncs nothing.** Nothing in `file.rs` calls `sync_all` or `sync_data`: not the
  active file, not the `.rotating` staging file, and not the directory after a rename.
- **A dropped batch leaves the spool.** `runtime::write_loop` calls `store.commit()` on
  `Delivery::Dropped` exactly as on `Delivery::Delivered`, whichever store backs the sink. Neither
  the disk ADR nor `docs/deploying.md` says so.
- **A leaked spool segment is never deleted.** `roll_read_cursor` drops a segment from memory
  whether or not its unlink succeeds. The next `DiskQueue::open` re-lists the file, counts it in
  `total_bytes`, and never deletes it, so enough of them fill `max_bytes`. Closed by #333.
- **A full `block` spool with nothing queued waits forever.** Once the reader has consumed
  everything, the active segment's bytes still count toward `max_bytes`, and the active segment
  is never deleted. With `segment_bytes` close to `max_bytes`, a push that doesn't fit parks on
  `not_full` while the reader parks on `not_empty`, and nothing wakes either; `drop_newest` drops
  every later push. Closed by #333.
- **A batch parked in a `block` push is lost uncounted at shutdown.** `run_output` drops
  `drain_inbox` when `write_loop` finishes first, and the batch `drain_inbox` was pushing goes
  with the future: not spooled, not counted `reason="shutdown"`. Closed by #333.
- **None of the crash paths above has a test.** A test can't make `fsync` fail or stop a process
  between a `rename` and the next syscall. `crates/logit-cli/tests/durable_buffer_restart.rs`
  covers one `SIGKILL` at one point. `FileTarget::rotate_with` injects a failing opener and
  nothing else. The inventory's rule is that each entry ends in a committed, executable
  artifact, so the cluster needs a way to fail or freeze any single filesystem operation from an
  ordinary test.

Planning the `dur` stack also found four spool defects, each reproducible from the code. They're
labelled here so later records can cite them; the workstream named on each closes it.

- **F1: an in-cap corrupt `compressed_len` reads as a torn tail.** A `compressed_len` below
  `logit_proto::frame`'s sanity cap (just over 64 MiB) but past the bytes present reads as
  `CodecError::Truncated`. That's correct at the frame layer, where it's indistinguishable from a
  short read, but `DiskQueue` mishandles it twice. `walk_segment` stops there, so
  `DiskQueue::open` truncates every real record after it in the active segment. On a closed
  segment, `read_record_at` returns nothing at end of file, so `peek` retries forever. The disk
  ADR's "Recovery" section credits the cap with preventing this; it only covers lengths above it.
  Closed by `dur/w3`.
- **F2: a cancelled `push` can land its bytes after the repair.** A `push` future dropped at its
  `flush` await leaves tokio's already-spawned blocking write running: `poll_write` hands the
  bytes to the blocking pool through `spawn_mandatory_blocking` and returns `Poll::Ready(Ok(n))`,
  and nothing cancels that task (tokio 1.53.1, `src/fs/file.rs`, `File::poll_write`). The next
  `write_record`'s torn-tail repair truncates through a fresh file descriptor that doesn't wait
  for it, so the orphaned bytes can land after the truncate and the file gets ahead of the
  in-memory segment length. Closed by `dur/w4`.
- **F3: a batch parked in a blocked `push` is lost uncounted at shutdown.** Under
  `overflow: block`, `drain_inbox` can hold a batch it already took from the inbox inside a
  pending `store.push`. When `run_output` drops `drain_inbox` at shutdown, that batch is in
  neither the inbox (so the abandoned-inbox sweep never sees it) nor the store, and nothing counts
  it dropped. The memory store has the same hole. Closed by `dur/w5`.
- **F4: a failed segment unlink leaks the segment for good.** `roll_read_cursor` removes a
  segment from memory whether or not its unlink succeeded. At the next `DiskQueue::open`,
  `list_segments` lists it again and counts it in `total_bytes`, and nothing deletes it, because
  only a segment the read cursor leaves is ever unlinked. Enough leaks fill `disk.max_bytes`.
  Closed by `dur/w5`.

## Decision

Every checkpoint write goes through one shared write → fsync(tmp) → rename → fsync(dir) helper;
every ignored spool I/O result becomes counted and diagnosed; an unusable tail checkpoint starts
from the beginning; and a feature-gated fault-injection seam in `logit-pipeline` puts every
filesystem mutation on these paths under test control. Each numbered item below is one decision a
reviewer can check.

1. **One durable-write helper.** `crates/logit-pipeline/src/atomic_write.rs` provides
   `write_file_durably(path, bytes, site)`. It runs, in this order: write the bytes to the tmp
   path, fsync the tmp file, rename it over `path`, fsync `path`'s parent directory (`.` when
   `path` names none). A failure returns `AtomicWriteError { step, source }`, where `step` is one
   of `Write`, `SyncFile`, `Rename`, or `SyncDir`. A failure at `Write`, `SyncFile`, or `Rename`
   leaves the previous document at `path` untouched. A failure at `SyncDir` happens after the
   rename, so `path` already holds the new document, not yet durable;
   `AtomicWriteError::replaced()` says which case a caller is in. The tmp path is the full file name with `.tmp` appended (`cursor.json.tmp`), never
   `with_extension`. `persist_cursor` and `CheckpointStore::write` both call it, and neither keeps
   its own tmp+rename code. The helper is synchronous.

2. **Where each caller runs it, and how spool persists are serialized.** The spool calls the
   helper inline from `DiskQueue::checkpoint_cursor`, never under the queue's state lock. Two
   tasks can persist the cursor: the write-loop task through `commit` and `peek`, and, under
   `overflow: drop_oldest`, the drain task through `push` (`push` → `roll_read_cursor`, and
   `push` → `evict_oldest` → `advance_read_cursor`). Unserialized, two persists would share
   `cursor.json.tmp`: one rename could fail with `ENOENT`, a truncated tmp could be renamed into
   place, or an older cursor could land last. So `checkpoint_cursor` holds a dedicated
   `cursor_write` mutex across the helper. It always takes `cursor_write` before the state lock
   and never while holding it; after taking it, it locks state only long enough to read the
   current cursor, releases state, runs the helper, then locks state again to record the result.
   Because each persist reads the cursor after taking `cursor_write`, and the cursor only moves
   forward, the cursor on disk never moves backwards. A persist's two fsyncs therefore stall only
   the task persisting (and another persist waiting behind it), never a `push` or `peek` waiting
   on the state mutex alone. It stays inline because it runs at most once per
   `checkpoint_interval` (1s by default) and once per segment roll, and because keeping `commit`
   synchronous is what keeps `write_loop`'s delivery path free of a new `.await` (the disk ADR's
   correction 2). The tail calls the helper through `tokio::task::spawn_blocking`, and
   `CheckpointStore::write` becomes `async`.

3. **Observed failures, with these names.**
   - Spool: `logit.component.buffer.disk.errors` (count), tagged `op` = `cursor`, `flush`,
     `fsync`, `create`, `truncate`, or `unlink`. `op="cursor"` reports under the existing
     `cursor_error` diagnostic key. Every other `op` reports under a new `disk_fs_error` key, kept
     separate because `Diagnostics::warn_throttled` throttles per key. A failed torn-tail repair
     truncate also drops the batch, counted `batches.dropped{reason="disk_io_error"|"disk_full"}`.
   - Tail: `logit.input.checkpoint.errors` (count), tagged `op` = `load` or `write`, under the
     existing `checkpoint_error` key.
   - No result of a flush, fsync, create, truncate, or unlink on the spool paths is discarded with
     `let _ =` or `is_ok()`.

4. **An unusable tail checkpoint starts from the beginning.** A checkpoint that is present but
   unreadable, malformed, empty, or the wrong version, or that is missing while its tmp file
   exists, makes every file present at the first scan start at offset 0. It is counted
   `op="load"` and diagnosed. A missing checkpoint with no tmp file beside it is the first-run
   case and keeps `read_from`. Files discovered after the first scan already start at the
   beginning and are unaffected.

5. **The spool unlinks even after a failed cursor persist.** `roll_read_cursor` counts the failed
   persist and still unlinks the segments the cursor left. This is safe because the cursor on disk
   is then an older one, or the new one not yet durable (a `SyncDir` failure, decision 1). At the
   next `DiskQueue::open` an older cursor either still names a surviving segment (replaying from
   there) or names a deleted one (falling back to the oldest surviving segment at offset 0), and
   the new one resumes where it says. Every case costs duplicates, never loss.

6. **`file_out` makes no durability promise.** It doesn't fsync the active file, the staging
   file, or the directory. Its renames are atomic against a process crash, not a power loss. This
   is recorded as an amendment to [ADR `rotating-file-output`](rotating-file-output.md) and as a
   `docs/known-gaps.md` entry, not changed.

7. **`Delivery::Dropped` commits the batch for a disk-backed sink too.** `write_loop` keeps
   calling `store.commit()` on a drop, whatever the store. The spool bounds loss across a process
   restart; `buffer.retry_budget` bounds loss across a destination outage, for the faults the
   sink's delivery posture retries. Per `output::is_retryable`, a batch is dropped when its fault
   isn't retryable under that posture (a permanent fault, or an ambiguous one such as a timeout
   under `at_most_once`), or when a retryable fault is still failing once `buffer.retry_budget`
   runs out. Either way it's counted `batches.dropped{reason="send_failed"}` and doesn't replay
   after a restart. This is
   [ADR `buffered-sink-delivery`](buffered-sink-delivery.md#delivery-posture-is-a-per-sink-policy-chosen-in-three-layers)'s
   rule inherited unchanged, recorded as an amendment to the disk ADR and in `docs/deploying.md`.

8. **A `fault-injection` seam, compiled out of the release binary.**
   `crates/logit-pipeline/src/fault.rs` (`pub mod fault`) defines
   `enum Op { Create, Open, Write, Flush, SetLen, SyncFile, SyncDir, Rename, Unlink }`,
   `struct Point { site: &'static str, op: Op }`, and a `sites` module naming each caller
   (`spool.segment`, `spool.cursor`, `spool.dir`, `tail.checkpoint`, `file_out.active`,
   `file_out.staging`, `file_out.retained`).
   - **Call rule.** `fault::check(point, path, arg) -> io::Result<()>` runs immediately before
     every filesystem mutation on the spool, tail-checkpoint, and `file_out` paths, including
     every step of the helper in decision 1. `path` is one the caller already holds, never one
     built for the call. `arg` is the segment sequence number, or `0`.
   - **Off.** Without the cargo feature, `check` is `#[inline(always)]` and returns `Ok(())`.
     `Op`, `Point`, and `sites` are always compiled, so call sites carry no `cfg`.
   - **On but disarmed.** With the feature (or under `cfg(test)`), `check` first loads one
     `static ARMED: AtomicBool` and returns `Ok(())` if it's clear. That load is the whole cost
     the allocation pins in `crates/logit-bench/tests/allocations.rs` see, and it allocates
     nothing.
   - **Armed.** Rules live in one global `Mutex` registry, each scoped to a directory prefix.
     `fault::scope(dir)` returns a drop guard that removes its rules and recomputes `ARMED`. A
     scope can fail every hit or only the nth hit of a `Point` with an errno, record every hit,
     or crash at the nth hit.
   - **Crash model: freeze.** At a crash point, the operation doesn't happen, and every later
     `check` under that scope returns an error until the test calls `revive()`. The test then
     drops the component, reopens it, and asserts. Given the call rule, the disk is exactly what a
     `kill -9` at that point would leave. Known limit: a tokio blocking write already spawned
     before the crash point still lands.
   - **Wiring.** `logit-pipeline` declares `[features] fault-injection = []`. `logit-inputs` and
     `logit-outputs` enable it only in `[dev-dependencies]`. With `resolver = "2"`,
     `cargo build --release -p logit-cli` never sees it. `script/lint` fails if
     `cargo tree -p logit-cli -e normal,features` mentions `fault-injection`.

9. **`file_out`'s `max_files` has a ceiling of 1000.** `logit_config::MAX_ROTATE_FILES = 1000`
   sits next to `RotateConfig`, and graph rule 29 rejects a larger value. `max_files` counts the
   active file, so 1000 retains 999 rotated files: about 2.7 years of daily files, or about 41
   days of hourly ones. One rotation makes about two syscalls per retained file, so 1000 keeps a
   rotation in the milliseconds.

## Alternatives considered

- **Per-push fsync on the spool.** Out of scope. It closes the power-loss window on the active
  segment at a real throughput cost, and `docs/known-gaps.md` already tracks it as a possible
  `disk.sync: every_push` knob. This ADR makes the checkpoint writes durable, not every append.
- **A panic at the crash point.** Rejected. A panic can fire while the spool holds its state
  lock, which the spool recovers from a poisoned mutex with `into_inner`, so the component would
  carry on over half-updated state. It also unwinds through tokio tasks that may be mid-operation
  on other threads, and makes what's on disk depend on which destructors ran. Freezing leaves the
  disk as `kill -9` would and lets the test decide when to stop.
- **A thread-local rule registry.** Rejected. Tokio runs `tokio::fs` operations and
  `spawn_blocking` work on its blocking pool, not the thread that armed the rule, so a
  thread-local rule would never see them. A global registry scoped by directory isolates tests
  instead: nextest runs each test in its own process, and every test uses its own scratch
  directory under plain `cargo test`.
- **An `Fs` trait threaded through every type.** Rejected. Every type on these paths
  (`DiskQueue`, `CheckpointStore`, `FileTarget`, the helper) would gain a generic parameter or a
  `dyn` field that production never varies, and the release binary would carry the indirection.
  A free function with a compiled-out body costs nothing and changes no signature.
- **`strace -e inject=` or an `LD_PRELOAD` shim.** Rejected for this cluster.
  `script/unsafe-check inject` already runs `strace -e inject=` out of CI
  ([ADR `out-of-ci-unsafe-verification`](out-of-ci-unsafe-verification.md)), and it fits a
  syscall wrapper with no seam of its own. Here it would target a syscall by number across the
  whole process, not one file's operation, couldn't freeze every later operation in one
  directory, and would keep these tests out of `script/cibuild`. These paths need ordinary tests
  that run on every build.
- **dm-flakey or a qemu power-loss simulation.** Rejected for now. Either one tests what the
  kernel and filesystem do with unsynced data, which is a property of ext4 or XFS, not of `logit`.
  The freeze model tests what `logit` controls: the order of its operations and what it does with
  each failure. Both need root or a VM, so neither can run in CI.
- **A separate ADR for the seam.** Rejected. The seam exists to verify the durability decisions
  in this ADR, and its crash model defines what those tests prove. Split across two records,
  either half would read as unmotivated.

## Running it

The harness is the set of ordinary tests listed here. Every workspace test build enables
`fault-injection`, so `script/test` and `script/cibuild` run them all; no separate image or
script is needed. This list is filled in as each workstream lands.

- **`dur/w1`, the seam, the helper, and spool I/O observability (DISK-04):**
  - `crates/logit-pipeline/src/fault.rs`:
    - `a_disarmed_seam_passes_every_point_through`: no rule means every `Op` at every site passes.
    - `an_armed_failure_fires_only_under_its_scope_directory`: matching is by path component, and
      only the armed `Point` fails.
    - `fail_nth_fires_once_on_the_nth_hit`: the nth hit fails and the hits either side pass.
    - `a_crash_freezes_every_later_operation_under_the_scope_until_revived`: the freeze model,
      including paths outside the scope staying live.
    - `dropping_a_scope_disarms_it`: a dropped scope leaves no rule or frozen state behind.
  - `crates/logit-pipeline/src/atomic_write.rs`:
    - `a_durable_write_runs_write_sync_rename_then_directory_sync_in_that_order`: decision 1's
      step order and paths, from recorded hits.
    - `the_tmp_name_appends_to_the_full_file_name`: no `with_extension` collision.
    - `a_failure_at_any_step_leaves_the_previous_document_readable`: old document unless
      `replaced()`, which is true only at `SyncDir`.
    - `a_crash_at_any_step_leaves_either_the_old_or_the_new_document`: a freeze at each step.
    - `a_stray_longer_tmp_is_overwritten_not_appended`: a leftover tmp never leaks into the target.
  - `crates/logit-pipeline/src/disk_queue.rs`:
    - `a_cursor_persist_is_fsynced_before_its_rename_and_the_directory_after`: the cursor's four
      steps, and every segment unlink after the cursor's directory `fsync`.
    - `a_failed_segment_fsync_at_rotation_is_counted_and_diagnosed`: `op="fsync"` plus
      `disk_fs_error`, and the rotation still completes.
    - `a_failed_rotation_create_is_counted_and_the_next_push_retries_rotation`: `op="create"`, and
      the next push self-heals with nothing lost.
    - `a_failed_directory_fsync_is_counted`: at rotation and at `finish`.
    - `a_failed_segment_unlink_is_counted`: `op="unlink"`, with the segment left on disk.
    - `a_persistently_failing_cursor_write_is_counted_every_time`: `op="cursor"` and `cursor_error`
      on every persist, and a restart replays rather than loses.
- **`dur/w2`, frame fixed-point properties (DISK-13):**
  `crates/logit-proto/tests/frame_fixed_point.rs`:
  - `write_then_read_round_trips_every_payload_under_both_compressions` — write/read is the
    identity on codec, flags, compression, and payload, over generated payloads up to 256 KiB.
  - `concatenated_frames_read_back_in_order_with_nothing_left_over` — one to eight frames read
    back in the order they were written, with nothing left in the buffer.
  - `lz4_expansion_on_incompressible_payloads_stays_within_n_plus_n_over_255_plus_16` — checks
    that lz4's real worst-case output stays within the `n + n/255 + 16` bound
    `MAX_SANE_COMPRESSED_LEN` is built on (it doesn't pin the constant itself: changing it leaves
    this test green; only the over-the-cap test below would notice).
  - `a_payload_at_the_uncompressed_cap_round_trips_under_lz4_and_none` — a full 64 MiB payload
    round-trips at the cap; one byte past it is rejected.
  - `a_compressed_len_corrupted_below_the_cap_reads_as_truncated` — pins finding F1's premise: a
    `compressed_len` corrupted below the sanity cap reads as `Truncated`, not `Malformed`.
  - `a_compressed_len_corrupted_over_the_cap_reads_as_malformed` — the complement: over the cap
    is always `Malformed`.
- **`dur/w3`, spool recovery and the read path (DISK-01, DISK-02):**
  - `crates/logit-pipeline/src/disk_queue_verification.rs`:
    - `walk_segment_recovers_every_record_outside_the_mutated_range`: a proptest over real
      segments (some `trace_id`s containing `MAGIC`) with one bit flip, overwrite, insert,
      truncation, or in-cap length rewrite. The walk terminates, never overlaps or goes
      backwards, recovers every untouched record exactly once at its own offset, and counts
      corruption exactly when there is some.
    - `open_never_truncates_a_record_that_would_have_parsed`: the same generator through
      `DiskQueue::open` and a peek/commit drain, checking `disk.truncated` and `disk_corrupt`.
  - `crates/logit-pipeline/src/disk_queue.rs`:
    - `a_corrupted_length_field_below_the_sanity_cap_does_not_truncate_the_records_after_it`:
      `open` keeps and delivers the record after an in-cap corrupt length.
    - `a_closed_segment_with_an_in_cap_corrupt_length_does_not_stall_peek`: `peek` resyncs past
      it on a closed segment instead of retrying forever.
    - `unrecoverable_garbage_at_the_end_of_a_closed_segment_is_skipped_and_counted`: `peek` skips
      to the next segment, counting one `disk_corrupt` batch with zero events.
    - `a_spurious_frame_inside_a_corrupt_records_context_never_moves_the_walk_backwards`: no
      phantom record inside the record before the corruption.
    - `a_segment_file_whose_name_is_not_zero_padded_is_ignored`: unpadded and signed names.
    - `a_second_open_of_the_same_spool_directory_fails_at_the_lock`: and leaves the spool
      untouched.
- **`dur/w4`, the spool write path (DISK-03, DISK-05):**
  - `crates/logit-pipeline/src/disk_queue.rs`:
    - `a_push_cancelled_after_its_bytes_landed_is_truncated_by_the_next_push`: a push dropped at
      its `flush` await once its bytes are on disk is truncated away, and never delivered.
    - `an_orphaned_write_that_lands_after_the_next_push_began_never_desynchronizes_the_segment`:
      a second runtime whose one blocking thread is held parks a cancelled push's write until the
      next push is repairing, the order a fresh-descriptor truncate can't survive.
    - `cancelling_pushes_at_every_await_never_desynchronizes_the_segment`: pushes cancelled after
      1 to 8 polls, their blocking work delayed by a varying amount, across rotations. Every
      segment's on-disk length matches its in-memory one after each push.
    - `a_failed_torn_tail_truncate_drops_and_counts_the_batch_and_never_writes_past_the_torn_bytes`:
      `op="truncate"`, a `disk_io_error` drop, nothing appended, and the next push repairs.
    - `a_failed_repair_never_rotates_so_only_the_active_segment_can_be_torn`: no new segment while
      a repair keeps failing.
    - `every_configurable_disk_compression_is_encodable_by_write_frame`: pins the `expect` on
      `write_frame` to `logit_config::Compression`'s variants.
    - `drop_oldest_reclaims_space_a_whole_head_segment_at_a_time_and_counts_every_eviction` and
      `drop_oldest_with_one_active_segment_evicts_every_queued_record_then_rotates_to_make_room`:
      `drop_oldest`'s whole-segment reclamation and its worst case.
  - `crates/logit-pipeline/src/disk_queue_verification.rs`:
    - `spool_model_every_push_is_delivered_dropped_or_queued`: a model-based proptest over pushes,
      cancelled pushes, peeks, commits, injected failures, and crash-reopens. Every push counted
      queued is delivered, each push counts exactly one of queued or dropped, duplicates and
      unconfirmed pushes appear only after a reopen, first deliveries are FIFO, no peek stops
      responding, and the depth gauge matches.
- **`dur/w5`, cursor rollover and shutdown (DISK-06, DISK-09):**
  - `crates/logit-pipeline/src/disk_queue.rs`:
    - `a_crash_at_any_point_of_a_segment_roll_loses_no_uncommitted_record`: a freeze at every
      recorded step of a roll (the cursor's four steps, the unlink), then a reopen and a drain:
      every uncommitted record arrives, and only committed ones repeat.
    - `a_segment_is_unlinked_only_after_the_cursor_leaving_it_is_durable`: from recorded hits, for
      a commit, a `drop_oldest` eviction, a rotation to make room, and `open`'s cleanup.
    - `a_failed_cursor_persist_before_an_unlink_loses_nothing_on_reopen`: decision 5, pinned.
    - `a_segment_left_behind_by_a_failed_unlink_is_removed_at_the_next_open`: out of the bound
      even when `open`'s own unlink fails (counted `op="unlink"`), and gone at the next `open`.
    - `finish_after_a_peek_without_commit_replays_the_peeked_head_on_reopen`: shutdown grace
      expiring mid-delivery replays, never loses.
    - `a_crash_at_any_point_of_finish_loses_nothing`: a freeze at every step of `finish`.
    - `a_blocked_push_makes_room_by_rotating_a_fully_consumed_active_segment` and
      `a_drop_newest_push_makes_room_by_rotating_a_fully_consumed_active_segment`: with
      `max_bytes == segment_bytes`, a push after everything was delivered neither waits forever
      nor drops.
    - `a_rotation_cancelled_after_its_create_is_finished_by_the_next_write_never_appending_to_the_old_segment`:
      a make-room rotation cancelled between its create and the new segment becoming active is
      finished by the next write, even on a closed store, and a failed retry of the create drops
      the batch rather than append to the old segment.
  - `crates/logit-pipeline/src/disk_queue_verification.rs`:
    - `spool_model_a_bounded_block_spool_never_parks_a_push_that_nothing_will_wake`: the spool
      model under `block` with a `max_bytes` a few records fill, plus a consume-everything op. A
      parked push is woken by the consumer's commits, and a push to a full spool with nothing
      queued finishes on its own. It also closes the spool and cancels pushes inside a
      rotation, and checks no segment newer than the active one survives a queued push.
  - `crates/logit-pipeline/src/runtime.rs`:
    - `every_run_output_exit_path_reconciles_received_against_delivered_dropped_and_spooled`:
      drain first, grace expiry, permanent error, and closed and empty, under both stores.
      Batches sent equal delivered plus `send_failed` plus `shutdown` plus spooled, and a disk
      store counts no `shutdown`.
    - `a_batch_parked_in_a_blocked_push_when_the_drain_is_abandoned_is_spooled_by_the_sweep`: the
      sweep takes the batch `drain_inbox`'s dropped push held.
    - `a_disk_sink_commits_a_batch_dropped_after_its_retry_budget_so_it_never_replays`:
      decision 7, pinned.
- **`dur/w6`, the tail checkpoint (TAIL-05):**
  - `crates/logit-inputs/src/tail/checkpoint.rs`:
    - `an_empty_checkpoint_is_unusable_not_missing`,
      `a_truncated_checkpoint_document_is_unusable_and_counted`,
      `an_unreadable_checkpoint_is_unusable`, `a_wrong_version_checkpoint_is_unusable`,
      `a_missing_checkpoint_with_a_stray_tmp_beside_it_is_unusable`, and
      `an_unreadable_tmp_beside_a_missing_checkpoint_is_unusable`: decision 4's unusable shapes,
      each counted `op="load"` and diagnosed once.
    - `a_missing_checkpoint_alone_is_missing`: the first-run case stays silent.
    - `checkpoints_differing_only_in_extension_never_share_a_tmp`: `state.json` and `state.yaml`
      write distinct tmp files.
    - `a_failed_write_at_any_step_leaves_the_store_dirty_and_the_next_write_lands`: an `EIO` at
      each of the helper's four steps counts `op="write"`, keeps the store dirty, and the next
      unforced write lands.
    - `a_crash_at_any_step_of_a_write_leaves_the_previous_checkpoint_loadable`: a freeze at each
      step, then a reload, finds the old checkpoint (the new one after the directory `fsync`).
    - `a_checkpoint_write_fsyncs_the_file_before_the_rename_and_the_directory_after`: decision 1's
      order at the `tail.checkpoint` site, from recorded hits.
  - `crates/logit-inputs/src/tail/driver.rs`:
    - `an_unusable_checkpoint_starts_every_preexisting_file_at_the_beginning_even_under_read_from_end`:
      empty, truncated, wrong-version, and stray-tmp checkpoints each replay every pre-existing
      line, and shutdown's forced write replaces the unusable document.
    - `a_missing_checkpoint_still_honours_read_from_end`: no checkpoint and no tmp still skips.
    - `a_crash_between_checkpoint_write_and_rename_resumes_from_the_previous_checkpoint_with_duplicates_only`:
      a freeze at the rename, then a restart under `read_from: end`, redelivers the line read
      since the last checkpoint and skips nothing.
  - `crates/logit-pipeline/src/graph.rs` (rule 62):
    `two_tailing_listeners_sharing_a_checkpoint_path_are_rejected`,
    `a_checkpoint_path_equal_to_another_components_tmp_path_is_rejected`, and
    `tailing_listeners_with_distinct_or_no_checkpoint_paths_validate_fine`.
- **`dur/w7`, `file_out` rotation (DISK-10):**
  - `crates/logit-outputs/src/file.rs`:
    - `a_crash_at_any_rotation_step_loses_no_line_and_duplicates_none_after_restart`: for
      `max_files` 2 and 3 with full retained history, pins the recorded operation order of one
      rotation (commit-point rename before any retained file), then freezes at each operation in
      turn, restarts, and writes on. No retained file changes before the commit point, and
      afterwards the lines across `.N`, `.rotating`, and the active file are an in-order suffix of
      everything written, with no duplicate, no orphan left, and every generation present.
    - `promote_staged_keeps_every_generation_in_suffix_order_for_max_files_two_through_six`: the
      cascade leaves `.N` holding the Nth-newest file for each `max_files` from 2 to 6.
    - `a_failed_truncate_under_max_files_one_is_not_rotated_and_keeps_writing_to_the_existing_file`:
      `rotate_failure` and `NotRotated`; later batches land in the existing file and retry the
      truncate.
  - `crates/logit-pipeline/src/graph.rs`: `file_out_with_max_files_over_the_ceiling_is_rejected`
    and `file_out_with_max_files_at_the_ceiling_is_accepted` (decision 9).

## Consequences

- **The `dur/w1`–`dur/w7` stack implements this record.** Each PR closes the inventory entries
  named in its line under "Running it": `dur/w1` DISK-04, `dur/w2` DISK-13, `dur/w3` DISK-01 and
  DISK-02, `dur/w4` DISK-03 and DISK-05, `dur/w5` DISK-06 and DISK-09, `dur/w6` TAIL-05, and
  `dur/w7` DISK-10. Each sets its rows to `findings → #N` or `reviewed @<sha>`.
- **The allocation pins measure the disarmed seam.** Cargo unifies features across a workspace
  test build, so `crates/logit-bench/tests/allocations.rs` runs with `fault-injection` on even
  though `logit-bench` never asks for it. `disk_queue_push_one_batch` and
  `disk_queue_peek_cached_costs_nothing` must stay exact with the seam present. A change to the
  disarmed path that allocates or takes a lock fails those pins; that is the pins working.
- **Each cursor persist costs two fsyncs more than today,** inline in the persisting task,
  serialized by `cursor_write` and outside the state lock, at most once per
  `checkpoint_interval` plus once per segment roll. Today's persist runs under the state lock, so
  moving it out, and adding `cursor_write`, is part of the change. A `logit-perf` disk-spool scenario is the
  follow-up if it shows up in delivery latency.
- **The tail checkpoint write moves off the runtime thread** (decision 2); `CheckpointStore::load`
  stays a blocking read at bind, an accepted startup cost.
- **An unusable tail checkpoint now replays** every file present at startup from byte 0: a
  duplicate burst in place of silent loss, visible as `logit.input.checkpoint.errors{op="load"}`.
- **New operator-visible names:** `logit.component.buffer.disk.errors`,
  `logit.input.checkpoint.errors`, and the `disk_fs_error` diagnostic key, each documented in
  `docs/design/internal-telemetry.md` by the workstream that emits it.
- **Every new filesystem mutation on these paths needs a `fault::check` before it.** An
  unchecked one makes the freeze model's "exactly a `kill -9`" claim false for any test that
  crosses it.
