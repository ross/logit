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
- **None of the crash paths above has a test.** A test can't make `fsync` fail or stop a process
  between a `rename` and the next syscall. `crates/logit-cli/tests/durable_buffer_restart.rs`
  covers one `SIGKILL` at one point. `FileTarget::rotate_with` injects a failing opener and
  nothing else. The inventory's rule is that each entry ends in a committed, executable
  artifact, so the cluster needs a way to fail or freeze any single filesystem operation from an ordinary test.

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
   of `Write`, `SyncFile`, `Rename`, or `SyncDir`, and leaves the previous document at `path`
   untouched. The tmp path is the full file name with `.tmp` appended (`cursor.json.tmp`), never
   `with_extension`. `persist_cursor` and `CheckpointStore::write` both call it, and neither keeps
   its own tmp+rename code. The helper is synchronous.

2. **Where each caller runs it.** The spool calls the helper inline from `persist_cursor`, which
   `DiskQueue::commit` reaches through `advance_read_cursor` and `roll_read_cursor`, but never
   under the queue's state lock: it copies the cursor under the lock, releases the lock, runs the
   helper, then locks again to record the result. A persist's two fsyncs therefore stall only the
   committing sink task, never a concurrent `push` or `peek` waiting on the state mutex. That is
   the blocking write `commit` already makes, now with two fsyncs. It stays inline because it runs
   at most once per `checkpoint_interval` (1s by default) and once per segment roll, and because
   keeping `commit` synchronous is what keeps `write_loop`'s delivery path free of a new `.await`
   (the disk ADR's correction 2). The tail calls the helper through
   `tokio::task::spawn_blocking`, and `CheckpointStore::write` becomes `async`.

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
   is then an older one, and at the next `DiskQueue::open` an older cursor either still names a
   surviving segment (replaying from there) or names a deleted one (falling back to the oldest
   surviving segment at offset 0). Both cost duplicates, never loss.

6. **`file_out` makes no durability promise.** It doesn't fsync the active file, the staging
   file, or the directory. Its renames are atomic against a process crash, not a power loss. This
   is recorded as an amendment to [ADR `rotating-file-output`](rotating-file-output.md) and as a
   `docs/known-gaps.md` entry, not changed.

7. **`Delivery::Dropped` commits the batch for a disk-backed sink too.** `write_loop` keeps
   calling `store.commit()` on a drop, whatever the store. The spool bounds loss across a process
   restart; `buffer.retry_budget` bounds loss across a destination outage. A destination down
   longer than the retry budget loses the spooled batches it couldn't accept, counted
   `batches.dropped{reason="send_failed"}`, as does a batch whose failure is permanent. This is
   [ADR `buffered-sink-delivery`](buffered-sink-delivery.md)'s rule inherited unchanged, recorded
   as an amendment to the disk ADR and in `docs/deploying.md`.

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
     scope can fail the next or the nth hit of a `Point` with an errno, record every hit, or
     crash at a hit.
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
   sits next to `RotateConfig`, and graph rule 29 rejects a larger value. One rotation makes
   about two syscalls per retained file, so 1000 keeps a rotation in the milliseconds and still
   covers more than two years of daily files or six weeks of hourly ones.

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

- **`dur/w1`, the seam, the helper, and spool I/O observability (DISK-04):** to be listed when
  `dur/w1` lands.
- **`dur/w2`, frame fixed-point properties (DISK-13):** to be listed when `dur/w2` lands.
- **`dur/w3`, spool recovery and the read path (DISK-01, DISK-02):** to be listed when `dur/w3`
  lands.
- **`dur/w4`, the spool write path (DISK-03, DISK-05):** to be listed when `dur/w4` lands.
- **`dur/w5`, cursor rollover and shutdown (DISK-06, DISK-09):** to be listed when `dur/w5`
  lands.
- **`dur/w6`, the tail checkpoint (TAIL-05):** to be listed when `dur/w6` lands.
- **`dur/w7`, `file_out` rotation (DISK-10):** to be listed when `dur/w7` lands.

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
- **Each cursor persist costs two fsyncs more than today,** inline in `commit`, outside the state
  lock, at most once per `checkpoint_interval` plus once per segment roll. Today's persist runs
  under that lock, so moving it out is part of the change. A `logit-perf` disk-spool scenario is the
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
