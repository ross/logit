---
created: 2026-09-28
updated: 2026-09-28
---

# Tail discovery failure and resume identity: a failed listing is no information, and a resume verifies the file's head

## Status
Accepted

## Context

`docs/plans/critical-sections-inventory.md` groups six entries as cluster 4, "Tail bookkeeping":
TAIL-01 (rotation, truncation, and removal reconciliation in `Tailer::scan`), TAIL-02 (start
offset, inode rebinding, and the `resume` map), TAIL-03 (the read, split, decode, and batch loop),
TAIL-04 (`LineSplitter`), TAIL-09 (Docker json-file decode and reassembly), and TAIL-10
(`config.v2.json` identity cache). TAIL-11 (pattern discovery) and TAIL-12 (telemetry accounting)
share the same code and are verified with it. [ADR
`file-tailing-and-docker-json-logs`](file-tailing-and-docker-json-logs.md) is the design they
verify.

Read-only passes over the entries, against the code as of `af557d90`, found most of it correct:
`reap_drained` reaps only `Draining` and `Deselected` files that reached EOF in the current pass,
the rebind branch checks `by_path` ownership, `read_from` applies on the first scan only, an
`Unusable` checkpoint starts every file at `0`, and `LineSplitter` holds every framing invariant.
Two findings break the ADR's promise that a tail restart is "strictly duplicates, never loss",
or its cost model for a transient error:

- **F1: a failed listing retires every file it would have named.** A failed `read_dir` of a
  pattern's directory (`PathPattern::scan`) or a failed `stat` of a discovered path
  (`Tailer::scan`) drops the path from `discovered`. The stale pass then marks every tracked file
  under it `Draining`, `drain` reaps each at EOF, and the next good scan reopens it at
  `StartOffset::Beginning`. One transient `EACCES`, `EIO`, or `EMFILE` costs a full replay of
  every file under that pattern. It has no diagnostic and no counter.
- **F2: `resume` is keyed on `(dev, ino)` alone, is never evicted, and seeks mid-file.** A
  `Resume(off)` with `off <= len` seeks to `off`. A recycled inode (a churning Docker host, a
  restored volume) that lands under a tracked pattern resumes at the previous file's offset and
  skips the new file's first bytes. That is loss, the one mode "strictly duplicates" doesn't
  cover. [Amendment 2026-09-26](file-tailing-and-docker-json-logs.md) closed the crash window for
  a reaped file's entry, but not an entry that outlives its file by other routes: an entry
  nothing consumes, or a de-selection retention whose file is gone.

Three more findings shape the contract without changing a rule here:

- **F3: truncation detection is size-only.** `reconcile_truncation` tests `len < offset`. A
  `copytruncate` that the writer refills past the old offset before the next check isn't seen.
- **F7 and F8: Docker reassembly.** A `Malformed` entry leaves a held fragment to splice onto the
  next logical line, and `docker_in` applies `max_line_bytes` to the JSON envelope, so a cap under
  about 16.1 KiB drops fragment envelopes before the decoder sees them. The [2026-09-28
  amendment](file-tailing-and-docker-json-logs.md) to the tailing ADR decides both.
- **F11: a read error on a `Draining` file counts as its EOF.** The reap loses the unread tail.

The verification gap is structural too. Tests drive `scan`, `drain`, and `open_tracked` by hand
with `Watcher::Poll` against a real scratch directory. The fault seam ([ADR
`durable-checkpoint-writes-and-fault-injection`](durable-checkpoint-writes-and-fault-injection.md),
decision 8) has mutation ops only and one tail site, `tail.checkpoint`, so a listing failure
can't be produced by a test, least of all as root in the dev container. A file's inode can't be
made to recycle on demand on tmpfs.

## Decision

A failed listing retires nothing. A resume trusts an inode only after the file's head matches what
the checkpoint recorded. The verification is a state-machine proptest on the real filesystem, with
the fault seam extended to reads. Each numbered item is one decision a reviewer can check.

1. **A failed listing is no information.** `PathPattern::scan` returns `Result<Scan, io::Error>`.
   The rule, per operation:

   - **`read_dir` of a pattern's directory fails (either matcher).** `scan` returns `Err`.
     `Tailer::scan` records the pattern's directory in `failed_dirs`. No path under that
     directory (`tail_in`) or that `root` (`docker_in`) is retired in this scan.
   - **`docker_in`'s per-entry work fails.** In `scan_docker_containers`, a `file_type()` error
     on a `root` entry, or a `metadata` error other than `NotFound` on the built log path, pushes
     the path to `Scan::unknown` instead of skipping it. A `NotFound` `metadata` is an ordinary
     absence: the container has no log file, and the path is in neither list.
   - **`stat` of a discovered path fails in `Tailer::scan`.** A `NotFound` is an ordinary
     absence and the path is treated as not discovered. Any other error puts the path in
     `unknown`.

   `Tailer::scan` builds `discovered` as before and an `unknown: HashSet<PathBuf>` holding every
   path from a pattern's `Scan::unknown` plus every discovered path whose `stat` failed with
   anything but `NotFound`. The stale pass retires a `by_path` key only if it isn't in `unknown`
   and its parent (`tail_in`) or its `root` prefix (`docker_in`) isn't in `failed_dirs`. A file
   that isn't retired stays in its current state: `Active` keeps being read, and `Draining` keeps
   draining.

   Each failure is counted once per scan per operation, as
   `logit.input.scan.errors{op="read_dir"|"stat"}`, and diagnosed `scan_error` through
   `warn_throttled`. A scan that had any failure doesn't prune `resume` (decision 4).

   The rule is per pattern and per path, not "skip the whole stale pass on any error". One
   persistently unreadable directory under one pattern would then block retirement under every
   other pattern, and a removed file under a healthy pattern would stay open for as long as
   the other directory failed.

2. **Resume identity is `(dev, ino)` plus a head fingerprint.** A checkpoint entry and a
   de-selection retention each carry a `Head { len, hash }`: `len` is the number of leading bytes
   hashed, `min(256, file length)`, and `hash` is XXH64 with seed 0 (`twox-hash`, already
   pinned in the workspace) over them. `head_of(&std::fs::File)` reads them with one
   `read_at(0, 256)`; a positional read doesn't move the file's cursor.

   - **Where the head comes from.** `TrackedFile` keeps a `std::fs::File` from `try_clone()`
     taken at open, so `write_checkpoint` and the `Deselected` branch of `reap_drained` compute
     the head without touching the read handle.
   - **Where it's checked.** `open_tracked`, for `StartOffset::Resume`, computes `head_of` on
     the file it opened and compares it with the retained head before the seek. Equal
     means resume at the offset. Not equal means start at `0`, count
     `logit.input.files.resume_rejected`, and diagnose `resume_rejected`. An offset past the
     file's length stays a plain restart at `0`, unchanged.
   - **What it proves.** The fingerprint covers `min(offset, 256)` bytes of the skipped range.
     For a file at most 256 bytes long, every byte a resume would skip is verified. Past that,
     two different files that share 256 identical leading bytes on a recycled inode are the
     accepted residual, recorded in `docs/known-gaps.md`. Log files that start with a
     timestamp or a per-file header make the case unlikely, and the failure is bounded to one
     file's skipped range.
   - **Why the head.** A file that only grows never changes its first bytes, so an appended
     file keeps its fingerprint across every checkpoint and the check has no false rejection.
     The head changes only when the file's content was replaced or truncated and refilled,
     which is a resume that must not seek.
   - **Why not the path.** A rotation while `logit` is down, under `app.log*`, is a supported
     resume: the inode's path is now `app.log.1`, and its offset is still right ([ADR
     `file-tailing-and-docker-json-logs`](file-tailing-and-docker-json-logs.md)'s "Rotation and
     truncation" section). A path-must-match rule would replay it.

3. **Checkpoint format version 2, with no version 1 reader.** `CheckpointEntry` gains `head_len`
   and `head_hash`, and `CHECKPOINT_VERSION` becomes `2`. The in-memory `resume` becomes a map
   from `FileId` to `Retained { path, offset, head, source }`, where `source` is `Checkpoint` or
   `Deselected`. A version 1 file is `Loaded::Unusable`: every file present at the first scan
   replays from `0`, the path the 2026-09-24 amendment to the tailing ADR already defines,
   counted `logit.input.checkpoint.errors{op="load"}`. Logit is pre-release, so no reader
   for version 1 is kept; one release replays once.

4. **Eviction.** A retained entry lives only as long as something can still consume it.

   - **Checkpoint entries.** `Tailer` carries `checkpoint_pruned: bool`. At the end of the
     first scan in which every listing succeeded (`failed_dirs` is empty, the first scan has
     run, and `checkpoint_pruned` is clear), every `Retained` with `source: Checkpoint` still in
     `resume` is removed and the flag is set. Such an entry names a file that no pattern
     discovered, so nothing can consume it, and a later inode reuse must not find it. A
     permanently failing directory keeps the entries unpruned, which is bounded by the
     checkpoint's size at startup and never grows.
   - **De-selection retentions.** Every scan removes each `Retained` with `source: Deselected`
     whose `path` is in neither `discovered` nor `unknown`. A container that is renamed out of
     `containers:` and back within the same process still resumes; one whose path is gone
     (removed, or its directory failed to list and later listed clean without it) doesn't
     leave an entry behind for a recycled inode.
   - **Spent entries.** An entry is removed when `accept` takes it, before `File::open`. An
     open failure therefore costs a replay, not a loss, and this ADR keeps it.

5. **Verification method.**

   - **A state-machine proptest on the real filesystem.** Operations (append, create, rename
     rotate, `copytruncate`, delete, listing failure, stat failure, scan, drain, checkpoint tick,
     restart, crash, shutdown) run against a scratch directory under `Watcher::Poll` and
     hand-driven `scan`/`drain`, and a model of the written lines is the oracle. There is no
     filesystem trait: the real filesystem is what decides an inode, a rename, and a truncate,
     and a model of it would only be a second copy of the assumptions under test.
   - **Fault-seam read ops.** `Op::ReadDir` and `Op::Stat`, at the new site `tail.scan`
     (`crates/logit-pipeline/src/fault.rs`), fail a listing or a `stat` on demand. The
     [amendment](durable-checkpoint-writes-and-fault-injection.md) to the fault-injection ADR
     records why the seam grows reads.
   - **A fingerprint mismatch stands in for inode reuse.** Tmpfs and overlayfs don't recycle an
     inode on demand, so a test writes a version 2 checkpoint that names a live file's
     `(dev, ino)` with a wrong hash. That is the state a recycled inode leaves, produced
     without the recycle.
   - **A checkpoint never lands inside a dropped line.** The proptests assert it for the plain
     splitter (`pending_bytes` covers a dropped line's consumed bytes while it is dropping) and
     for the Docker decoder (`holds_entry` stays true while a stream is dropping), so a restart
     re-drops the line whole instead of emitting its tail. The cost is a checkpoint pinned at
     that line's start until its newline arrives.
   - **A recorded manual `logrotate` run.** `logrotate` is installed once in the running dev
     container (not the image), run against a real `logit run` in each of its modes, the
     syscall sequence and the line multiset recorded in TAIL-01's inventory entry, and the
     proptest's rotation operations reproduce that sequence.

## Alternatives considered

- **Skip the whole stale pass when any listing fails.** Rejected. One persistently failing
  directory would block retirement under every other pattern (decision 1).
- **Resume only if the path matches.** Rejected. A rotation while `logit` is down under
  `app.log*` is a supported resume, and path-must-match would replay it (decision 2).
- **A whole-file or offset-dependent fingerprint.** Rejected. Hashing the bytes up to the
  offset re-reads the file at every checkpoint, and the hash changes on every append. A
  `min(256, len)` head is one positional read and stable for an append-only file.
- **A filesystem trait for tests.** Rejected. The oracle for rotation and inode behavior is the
  real filesystem; `Watcher::Poll` and hand-driven `scan`/`drain` already are the seam.
- **A committed test that shells out to `logrotate`.** Rejected. It adds a package to the dev
  image and a process to every test run for a tool whose behavior a one-time recording captures.
  The recording is what the proptest reproduces, and a version bump of `logrotate` is a reason
  to re-record, not to fail CI.
- **Keep the silent status quo.** Rejected. A replay per transient error, with no counter, is
  the failure operators can't diagnose (F1), and the ADR's central promise is false for a
  recycled inode (F2).
- **Keep a version 1 reader.** Rejected. Pre-release, and the fallback already exists: an
  unusable checkpoint replays.

## Consequences

- **Operator-visible names.** `logit.input.scan.errors{op="read_dir"|"stat"}`,
  `logit.input.files.resume_rejected`, and the diagnostic keys `scan_error` and
  `resume_rejected` are documented in `docs/design/internal-telemetry.md`. The same section
  gains the `truncated` key the driver already emits.
- **A transient listing error costs one diagnostic, not a replay.** A file whose path is removed
  while its directory can't be listed stays open until a listing succeeds and no longer names
  it. A tail that was `Active` is still read.
- **A recycled inode is a replay, not a skip.** Except for the 256-byte residual.
- **One release replays once.** The checkpoint version bump makes every existing checkpoint
  `Unusable`, so every file present at the first scan after the upgrade replays from `0`.
- **`write_checkpoint` does one more positional read per tracked file per write.** It runs on
  the checkpoint interval and only when the store is dirty; a measurement on many thousands of
  tracked files belongs to the perf follow-up.
- **The fault seam is no longer mutation-only.** Every new `read_dir` or `metadata` call on the
  tail scan path needs a `fault::check` before it.
- **Documented gaps, not fixes.** Copytruncate fast refill, the 256-byte residual, the
  `Draining` read-error reap, and a container name of 12 or more hex characters matching as an
  id prefix are in `docs/known-gaps.md`. The perf items (a 64 KiB read buffer allocated per
  call, per-pass `Vec`s, byte-by-byte newline search) wait for a perf session with a tail
  scenario, because an allocation or sizing change needs a measurement on the perf VM
  ([ADR `event-sizing-and-allocation-strategy`](event-sizing-and-allocation-strategy.md)).

## Running it

Each workstream updates its inventory rows in the PR that lands its artifact, and fills in its
subsection here.

### `tailbk/w0`: this ADR

Docs only. It records the decisions above, the amendments to the tailing and fault-injection
ADRs, four entries in `docs/known-gaps.md`, the telemetry names in
`docs/design/internal-telemetry.md`, and eight inventory rows marked `in-progress`.

### `tailbk/w1`: `LineSplitter` model proptest (TAIL-04)

To be filled by the PR that lands it.

### `tailbk/w2`: Docker decode, held-fragment flush and cap decoupling (TAIL-09)

To be filled by the PR that lands it.

### `tailbk/w3`: scan failure is no information (TAIL-01, TAIL-11)

To be filled by the PR that lands it.

### `tailbk/w4`: head fingerprint and eviction (TAIL-02)

To be filled by the PR that lands it.

### `tailbk/w5`: driver state machine, `logrotate` run, close-out (TAIL-01..03, TAIL-10, TAIL-12)

To be filled by the PR that lands it.
