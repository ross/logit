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

1. **A failed listing is no information.** `PathPattern::scan` returns `Result<Scan, io::Error>`,
   and `pattern.rs`'s module doc holds the canonical error-to-outcome table. The rule, in prose:

   - **A directory that isn't there is an empty listing, not an error.** A `read_dir` that fails
     `NotFound` or `NotADirectory` is the "not there yet" case `docs/deploying.md` describes. It
     is a successful empty listing and counts nothing.
   - **Any other `read_dir` failure fails that pattern's listing.** So does an error from the
     `ReadDir` iterator part-way through. `scan` returns `Err`, and no path that pattern could
     name is retired in this scan.
   - **A per-path `stat` has three outcomes.** `NotFound` or `NotADirectory` means absent. Any
     other error means unknown, and an unknown path retires nothing.
   - **`docker_in`'s per-entry work.** In `scan_docker_containers`, a `file_type()` error on a
     `root` entry, or a `metadata` error other than absent on the built log path, pushes the
     path to `Scan::unknown` instead of skipping it. `ELOOP` on a container directory is
     unknown: the file stays tracked and the error is diagnosed.

   `Tailer::scan` retires a tracked path only if its `stat` wasn't unknown and no pattern whose
   listing failed covers it (`PathPattern::covers`); `pattern.rs`'s table has the per-operation
   detail. Each failure is counted once per scan per operation, as
   `logit.input.scan.errors{op="read_dir"|"stat"}`, and diagnosed `scan_error` through
   `warn_throttled`.

   - **A file kept only because its listing failed or its stat was unknown is still checked
     through its open handle.** The driver runs `fstat` on it: a link count of 0 means it was
     removed, so it drains; otherwise its length is checked for truncation. Without the check, a
     `copytruncate` under an unreadable directory goes unseen and the refilled file's first bytes
     are skipped.
   - **`docker_in` keeps the metadata-cache entry of a container kept this way**
     (`DecoderFactory::retain`), so recovery reports no spurious identity change.
   - **`read_from: end` applies only to files found by the bind-time scan.** A file first listed
     after that listing failed starts at its beginning: duplicates over loss, the choice the
     2026-09-24 amendment to the tailing ADR makes for an unusable checkpoint.

   The rule is per pattern and per path, not "skip the whole stale pass on any error". One
   persistently unreadable directory under one pattern would then block retirement under every
   other pattern, and a removed file under a healthy pattern would stay open for as long as
   the other directory failed.

2. **Resume identity is `(dev, ino)` plus a head fingerprint.** `TrackedFile` holds the file's
   first `min(256, offset)` bytes and clears them on a truncation. A checkpoint entry and a
   de-selection retention carry `head_len` (at most 256) and `head_hash`, XXH64 with seed 0
   (`twox-hash`, already pinned in the workspace) over those bytes.

   - **Where the head comes from.** A file opened at `0` captures its head as the tailer reads
     it. A file opened at `End` or at a `Resume` offset has not read its own first bytes, so
     `open_tracked` reads the first `min(256, target offset)` bytes once, from position 0, and
     then seeks. For `Resume` it hashes them against the retained head; either way they seed
     `TrackedFile`'s head. The head and the offset are from the same generation of the file
     unless a `copytruncate` and refill went undetected: the old head then stays paired with a
     newer offset, and that mismatch is what the restart check catches. A checkpoint write reads
     nothing and needs no second descriptor.

   - **Resume accepts iff all three hold:** the current file is at least `head_len` long, its
     first `head_len` bytes hash to `head_hash` (the open-time read above), and the offset is at most the current
     length.
     Any one failing is a single `logit.input.files.resume_rejected`, diagnosed
     `resume_rejected`, and the file starts at `0`. An offset past the file's length is
     therefore counted. The check runs in `open_tracked` before the seek.
   - **What it proves.** An append-only file never changes its first bytes and only grows, so
     it is never falsely rejected. A recycled inode is accepted only if its new content shares
     the first `min(256, offset)` bytes with the old file. For an offset of at most 256, that
     means every skipped byte is identical content. Past 256, two files that share their first
     256 bytes on a recycled inode are the accepted residual in `docs/known-gaps.md`.
   - **A copytruncate-and-refill that the size check missed fails this check at restart.** The
     refilled file's head no longer matches, so it replays instead of skipping.
   - **A resume entry is spent only once the file is tracked.** That is after the open, the
     inode check, the head check, the seek, and the decoder open all succeed, so a transient
     open failure doesn't replay. The opened descriptor's `(dev, ino)` is compared with the
     scanned one, and a mismatch (a rotation between `stat` and `open`) leaves the entry for the
     next scan.
   - **Why not the path.** A rotation while `logit` is down, under `app.log*`, is a supported
     resume: the inode's path is now `app.log.1`, and its offset is still right ([ADR
     `file-tailing-and-docker-json-logs`](file-tailing-and-docker-json-logs.md)'s "Rotation and
     truncation" section). A path-must-match rule would replay it.

3. **Checkpoint format version 2, with no version 1 reader.** An entry's fields are `dev`, `ino`,
   `path`, `offset`, `head_len`, and `head_hash`, and `CHECKPOINT_VERSION` becomes `2`. The hash
   and `HEAD_BYTES` (256) are part of the format: changing either is a version bump. The loader
   probes the version before the full parse, so a version 1 file reads as "unsupported version
   1", not "malformed"; a `head_len` over 256 is malformed. Either is `Loaded::Unusable`: every
   file present at the first scan replays from `0`, the path the 2026-09-24 amendment defines,
   counted `logit.input.checkpoint.errors{op="load"}`. Logit is pre-release, so no version 1
   reader is kept, and upgrading replays every file once. The in-memory `resume` becomes a map
   from `FileId` to `Retained { path, offset, head, source }`, where `source` is `Checkpoint` or
   `Deselected`.

4. **Eviction is per entry.** A retained entry lives only as long as something can still
   consume it, and a listing that failed can't say that it can't.

   - **Unconsumed checkpoint entries persist.** Every checkpoint write persists them until they
     are pruned, alongside the tracked files. A write that persisted only tracked files would
     lose the entry of a file whose listing failed at startup, and a restart under
     `read_from: end` would then skip data.
   - **A checkpoint entry is dropped** when its stored path's listing succeeded, its `stat`
     wasn't unknown, and its inode wasn't discovered in this scan. Checkpoint entries are pruned
     after the discovery loop.
   - **A de-selection retention is dropped** when its path is neither unknown nor under a failed
     listing, and is either not discovered or discovered with a different inode. Retentions are
     pruned before the discovery loop, so a container renamed out of `containers:` and back
     within the process still resumes.

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
  offset re-reads the file at every checkpoint, and the hash changes on every append. A head
  capped at 256 bytes is stable for an append-only file.
- **A fingerprint read from the file at checkpoint time (`pread` on a cloned descriptor).**
  Rejected. It costs a second descriptor per tracked file and a blocking read per dirty tick,
  and it can pair a head from one generation of the file with an offset from another. The one
  read at open, for `End` and `Resume`, is the accepted cost.
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
  while its directory can't be listed stays open until a listing succeeds and no longer names it,
  or until its handle shows a link count of 0. An `Active` file is still read.
- **A recycled inode, or a truncate-and-refill, is a replay, not a skip.** Except for the
  256-byte residual.
- **One release replays once.** The checkpoint version bump makes every existing checkpoint
  `Unusable`, so every file present at the first scan after the upgrade replays from `0`.
- **A file's head costs a small buffer per tracked file,** one read of at most 256 bytes when a
  file opens at `End` or `Resume`, and one hash per checkpoint write. A measurement on many
  thousands of tracked files belongs to the perf follow-up.
- **The fault seam is no longer mutation-only.** Every discovery syscall on the tail scan path
  (`read_dir`, each iteration step, `file_type`, and `metadata`) needs a `fault::check` before
  it.
- **Documented gaps, not fixes.** Eleven entries in `docs/known-gaps.md`'s "File tailing and
  Docker logs" record what this stream leaves open, so a later workstream that closes one can
  see it was expected:
  - copytruncate fast refill;
  - the 256-byte fingerprint residual;
  - a `Draining` read error losing the unread tail;
  - a container name of 12 or more hex characters matching as an id prefix;
  - a directory unreadable at startup replaying under `read_from: end` once it recovers;
  - a file kept under a directory that stays unreadable;
  - a dropped closing fragment splicing the next same-stream line onto the held partial;
  - `attrs` past the envelope cap's slack dropping an entry;
  - the checkpoint pinned by the oldest held line across both streams;
  - the checkpoint pinned by a line that never ends;
  - the shutdown tail emitted as a `bad_line`.

  The perf items (a 64 KiB read buffer allocated per
  call, per-pass `Vec`s, byte-by-byte newline search) wait for a perf session with a tail
  scenario, because an allocation or sizing change needs a measurement on the perf VM
  ([ADR `event-sizing-and-allocation-strategy`](event-sizing-and-allocation-strategy.md)).

## Running it

Each workstream updates its inventory rows in the PR that lands its artifact, and fills in its
subsection here.

### `tailbk/w0`: this ADR

Docs only. It records the decisions above, the amendments to the tailing and fault-injection
ADRs, eleven entries in `docs/known-gaps.md`, the telemetry names in
`docs/design/internal-telemetry.md`, and nine inventory items marked `in-progress` (eight TAIL
rows and top lead 7).

### `tailbk/w1`: `LineSplitter` model proptest (TAIL-04)

To be filled by the PR that lands it.

### `tailbk/w2`: Docker decode, held-fragment flush and cap decoupling (TAIL-09)

To be filled by the PR that lands it.

### `tailbk/w3`: scan failure is no information (TAIL-01, TAIL-11)

Decision 1 as written. Because a failed pattern is tested with `PathPattern::covers` rather than
by directory, two patterns sharing a directory, one of them failing, still retire what only the
listed one names. `Tailer::scan`'s listing step returns a `Listing` (`discovered`,
`unknown`, `failed`, with `is_complete` for the resume pruning in decision 4). The fault seam
checks three points at `tail.scan`: the `read_dir` in `PathPattern::scan`, the two per-container
stats in `docker_in`'s walk, and the per-path `metadata` in `Tailer::scan`, which runs once per
distinct matched path.

Tests, all against a real scratch directory with the failure forced through the seam:
`pattern.rs` covers each row of its module doc's table, `covers` against `scan` for both
matchers, and a model proptest of `matches_name`; `driver.rs` covers a failed listing and a
failed `stat` keeping the file with no replay, `ENOENT` still retiring it, a missing or removed
directory, a deletion and a truncation seen through the handle while the listing fails, two
patterns with one failing (separate and shared directories), one `stat` per distinct path, and a
bind-time failure starting the file at `0` later; `docker.rs` covers an unreadable container
directory and an unreadable `root`, each recovering with no identity change.

### `tailbk/w4`: head fingerprint and eviction (TAIL-02)

To be filled by the PR that lands it.

### `tailbk/w5`: driver state machine, `logrotate` run, close-out (TAIL-01..03, TAIL-10, TAIL-12)

To be filled by the PR that lands it.
