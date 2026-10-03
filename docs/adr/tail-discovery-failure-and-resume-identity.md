---
created: 2026-09-28
updated: 2026-10-02
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
the checkpoint recorded. A draining file's first EOF isn't final. The verification is a state-machine proptest on the real filesystem, with
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
   - **`docker_in`'s per-entry work.** In `PathPattern::scan_container`, a `file_type()` error
     on a `root` entry, or a `metadata` error other than absent on the built log path, pushes the
     path to `Scan::unknown` instead of skipping it. `ELOOP` (or any other non-absent error) on
     the log path's `metadata` is unknown: the file stays tracked and the error is diagnosed. A
     symlinked container directory isn't followed (`file_type()` doesn't follow it) and is
     absent.

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

6. **A draining file is reaped only after it has been draining for at least one
   `poll_interval`, a scan that could have rebound it has run, and it is at EOF.** Decided from
   the recorded `logrotate` run (TAIL-01's inventory entry). `TrackedFile` records when, and in
   which scan, it became `Draining` (the stale pass, the rotation arm, and a kept file whose
   handle shows a link count of 0), and a rebind back to `Active` clears both.
   `Tailer::reap_drained` reaps a `Draining` file only when all three hold:
   - a `drain` pass found it at EOF;
   - at least one `poll_interval` passed between it starting to drain and the start of that
     pass, before the pass's reads, so a pass parked on the downstream can't reap on an EOF it
     saw before the grace ran out;
   - a scan after the one that retired it has completed with a listing that could name its path
     (the path wasn't an unknown `stat` and no failed listing covers it), because a `drain`
     also follows a data wake or a flush tick, and time alone doesn't order the rebinding scan
     first.

   Its doc comment is the canonical statement of the rule. A `Deselected` file is still reaped at
   its first EOF.

   - **Why.** A first EOF isn't final for two reasons the run showed:
     - logrotate's `create` mode renames the file and HUPs the writer, which keeps appending to
       the renamed inode for 4.5 to 36 ms before it reopens. Under an exact pattern, that inode is
       reachable only through the tracked handle, and a reap at the first EOF loses those lines.
       A writer that never reopens loses everything after the first rotation.
     - A `read_dir`, then a rename, then an `ENOENT` from the `stat` makes a scan see a tracked
       path as absent. Its inode starts draining, and the next scan finds it under its new name.
       Reaped in between, it's reopened there as a new file and replayed from `0`.

     With the grace, the next scan (a `Discover` wake or the poll tick) rebinds the renamed inode
     through `open_tracked`, and a writer that reopens within the grace has its late lines read.
     The poll tick runs under every `WatchMode`, so a draining file with no other wake is reaped
     within about two poll intervals. One whose every later listing fails stays pinned until one
     succeeds.
   - **Cost.** A rotated or removed file's descriptor is held at least one `poll_interval` longer. A
     writer that reopens later than that still loses what it writes after the reap. The shutdown
     path is unchanged: a file still draining at shutdown stays in the checkpoint, and the known
     orphan gap applies.

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
- **A separate, configurable reap grace.** Rejected. `poll_interval` already bounds when the next
  scan runs, and that scan is what rebinds a renamed inode, so a second knob would only let the
  two disagree.

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
- **A draining file's descriptor is held at least one `poll_interval` longer** (decision 6). A
  `Deselected` file is unaffected.
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
- **Follow-ups, not started.**
  - TAIL-03 perf: reuse one read buffer per `Tailer` (no 64 KiB zeroing and copy per call),
    `memchr` in `LineSplitter::push`, and `drain`'s per-pass `Vec`s hoisted. **Implemented and
    confirmed on the perf VM (2026-10-02)**: the `tail` and `tail-rotate` scenarios measure it, and
    CPU µs/event falls 38.7% and 37.3% (`docs/design/performance.md` §1). No allocation pin
    covers the read path yet: `crates/logit-bench` calls components directly, and `read_one` is
    reachable only through a running `Tailer`.
  - Per-stream drop state in the checkpoint, which removes the pin a never-ending line holds on
    the checkpoint.
  - A `TailDecoder` hook the splitter calls for an envelope it drops, so `docker_in` can release
    a held fragment the drop would otherwise splice onto the next line.
  - A decoder opt-out from `take_partial` at shutdown, so `docker_in` checkpoints before a torn
    envelope instead of emitting it as a `bad_line`.

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
listed one names. `Tailer::scan`'s listing step returns a `Listing` (`discovered`, `unknown`,
`failed`, and `listed`, which the resume pruning in decision 4 also uses). The fault seam checks
three points at `tail.scan`: the `read_dir` in `PathPattern::scan` and each entry it yields, the
two per-container stats
in `docker_in`'s walk, and the per-path `metadata` in `Tailer::scan`, which runs once per
distinct matched path.

Tests, all against a real scratch directory with the failure forced through the seam: `pattern.rs`
covers each row of its module doc's table but the iterator-item one, `covers` against `scan` for
both matchers, and a model proptest of `matches_name`; `driver.rs` covers a failed listing (at the call, or part-way through: the iterator-item row)
and a failed `stat` keeping the file with no replay, `ENOENT` still retiring it, a missing or
removed directory, a deletion and a truncation seen through the handle while the listing fails, two
patterns with one failing (separate and shared directories), one `stat` per distinct path, and a
bind-time failure starting the file at `0` later; `docker.rs` covers an unreadable container
directory and an unreadable `root`, each recovering with no identity change.

### `tailbk/w4`: head fingerprint and eviction (TAIL-02)

Decisions 2 to 4 as written. `checkpoint.rs`'s module doc is the canonical statement of format 2,
the capture rule, the accept rule, and the residual. Three details the decisions leave open:

- **A resume reads `min(256, max(offset, head_len))` bytes from `0`** before the seek, so the
  tracked file's head satisfies its invariant (`head.len() >= min(256, offset)`) without a second
  read, and the file is always sought afterwards, to `0` on a rejection. A file opened at its end
  reads its first `min(256, len)` bytes once, at open; every other head byte comes from
  `read_one`'s own chunks. A head read that fails other than as a short file (`EIO` on a flaky
  mount) is an open error, not a rejection: the entry stays for the next scan.
- **The resume entry is removed once the file is tracked, whatever the start was**, so an entry
  never outlives its inode being tracked. The rotation arm resumes from an unspent entry as the
  unbound arm does (a `tailbk/w6` fix; before it, the rotation arm opened at `0` and spent it).
- **The fault seam's `tail.scan` site gains `Op::Open` and `Op::Read`**, checked before
  `open_tracked` opens a discovered file and before it reads a resumed file's head, so a test can
  fail either after a good `stat`.

Tests, in `driver.rs` unless noted, drive `bind`/`scan`/`drain` by hand under `Watcher::Poll`
against a real scratch directory: a hand-written format 2 entry with a wrong hash for a live
inode (the recycled-inode stand-in), an offset past the file's length, a file under 256 bytes
checked in full and still resumed after it grew, a head that crosses 256 bytes between two
checkpoints, the seek back to `0` after a rejection, a `copytruncate` refilled past the offset
while stopped, a truncation clearing the head, `read_from: end` capturing it at open, a
de-selection retention rejected after a rewrite and pruned when its path is gone or another
inode's, checkpoint entries pruned after a clean scan and kept under a failed listing or an
unknown `stat` (and persisted by a write meanwhile, so a `read_from: end` restart replays), an
entry spent only once the file is tracked (a failed open or decoder open), and an open that finds
another inode than the one scanned. `checkpoint.rs` covers the version probe (format 1 reads as
"unsupported version 1"), a `head_len` over 256, a format 2 entry without its head, `Head::matches`,
and XXH64 test vectors. `checkpoint_is_written_on_interval_only_when_dirty_and_resumes_by_inode`
and `a_reaped_files_stale_checkpoint_entry_is_gone_before_an_inode_reuse_can_resume_from_it` pass
unchanged.

### `tailbk/w5`: reap grace and tail accounting fixes (TAIL-01, TAIL-03, TAIL-10, TAIL-12)

Decision 6, from the recorded `logrotate` run, and five fixes found reviewing the driver for the
state-machine proptest:

- **`logit.input.files.rotated` is counted before any discovered path is reconciled**: one per
  discovered path whose binding names another inode. Counted in the rotation arm, it depended on
  `discovered`'s order under a wildcard, since a rebind that ran first removed the old binding.
- **A truncation dirties the checkpoint**, so an interval write with no read since doesn't keep
  the pre-truncation offset.
- **`logit.input.lines` and `line.bytes` count the unterminated line `close_decoder` offers the
  decoder**, as they count every other line the decoder is offered.
- **`open_tracked` calls `DecoderFactory::retain` on its rebind branch and its `Deselected`
  return**, so `end_scan` keeps `docker_in`'s cached identity for both. `docker_in` never rebinds:
  a rotated `<id>-json.log.1` matches no pattern.
- **The fault seam gains `tail.read`**, checked before `read_one` reads a chunk. The head read
  `open_tracked` does for a resume stays at `tail.scan`.

Tests, in `driver.rs` unless noted, hand-drive `scan` and `drain` under `Watcher::Poll`, with
`start_paused` and `tokio::time::advance` where the grace matters: a writer appending to the
renamed inode after the rotation, a `stat` that races a rename rebinding a full poll interval
later, a `drain` with no scan since draining started not reaping, a pass parked on the downstream
past the grace not reaping on its earlier EOF, a file deleted under a failing listing pinned until
the listing recovers, the reap at the grace boundary, a `Deselected` file reaped with no time passing,
`files.rotated` for a two-inode swap and a rotation chain in every discovery order, a truncation
dirtying the checkpoint, `lines` for the close-time partial and against a decoder that rejects
lines, every discovered path offered to the factory once per scan, and a read error on an
`Active` file (kept) and on a `Draining` one (reaped, its unread tail lost). `docker.rs` covers a
de-selected container's cache entry surviving until its reap. Five tests that expected a reap in
the same pass as the retiring scan advance the clock past the grace and scan again first, and so
does `the_live_kernel_watch_count_matches_this_watchers_own_bookkeeping`, so it still reaps.

### `tailbk/w6`: driver state-machine proptest, close-out (TAIL-01..03, TAIL-10, TAIL-12)

`crates/logit-inputs/src/tail/driver/verification.rs` is a state-machine proptest of `Tailer` on
the real filesystem; its module doc is the canonical description. Each case tails one to three
files under one pattern kind (`app.log` or `app.log*`), with `max_events` 1 or 3 and
`max_line_bytes` 40 or 1 MiB, through 8 to 64 random ops on a current-thread runtime with paused
time:

- **Writes:** complete lines, a line's first bytes (completed by the slot's next file op), a
  create, a delete, and a delete of the oldest rotated file.
- **Rotations, as the recorded `logrotate` run did them:** `RenameRotate` (shift `.N` to `.N+1`,
  rename to `.1`, create, with a scan free to land between steps), an append to the renamed
  inode (the writer before its reopen), and `CopyTruncate` (a new `.1` copy, a truncation in
  place, and a scan, whose listing may fail).
- **Tailer ops:** a scan (clean, a failed listing, an `EIO` `stat`, or an `ENOENT` `stat` standing
  in for a rename between the listing and the `stat`), a drain, a flush, a checkpoint tick, a
  `poll_interval` of clock, a clean restart, and a crash, each restart's first scan free to fail.

An independent model restates the driver's rules and, after every op, the test checks the batches
received (content, boundaries, and per-inode order), every tracked file's offset, pending bytes,
state, path, and head, `by_path`, the resume entries, the checkpoint on disk (and that each
persisted offset is a line start, or the end of a prefix a clean stop emitted), and every counter
and diagnostic key. At the end, every message is a run of a written line between two points a
clean stop split it at, including a split a later generation of the inode inherits, and every
complete line of an inode a pattern still reaches was received. 64 cases
run in about 1.4 s, and 1000 in about 23 s. Seven driver mutations (the reap grace, the truncation
dirtying the checkpoint, the rotation count, the head check, the pending-bytes subtraction, the
rebind's ownership check, the link-count check) each fail it.

The model found three driver bugs, all fixed here, each with a hand-driven test in `driver.rs`;
the first and second are also replayed through the harness:

- **A rebind never checked for truncation.** A file retired by a `stat` that raced a rename, then
  truncated in place (`copytruncate`) and still shorter than its old offset at the scan that
  rebinds it, kept that offset: once refilled past it, it was read from mid-line, a fragment
  emitted as a line and the refill's first lines lost. The rebind now runs the same-path arm's
  truncation check against the scan's `stat`.
- **Nothing checked a draining file for truncation.** A `Draining` file is bound to no path, so
  neither the scan's truncation checks nor a data wake reached it, and a refill after the
  truncation but before a rebind (or before the reap, with no rebind) was read from the old
  offset. Every scan now `fstat`s each `Draining` file's handle, and `drain` does before each read
  of one; only a refill past the offset before the first such check stays unseen, the documented
  size-detection gap. The model's `CopyTruncate` always scans before any append, so that gap
  can't occur in it.
- **The rotation arm ignored an unspent checkpoint entry.** An inode whose entry survived a
  restart unspent (its `stat` failed), then rotated onto a path still bound to another inode,
  was opened at `0` and replayed, but only in the scan order where that path was reached before
  the old inode's rebind released it. The rotation arm now resumes from the entry as the unbound
  arm does.

Named tests pin the refuter's cases one at a time: a rotation chain rebinding in all six scan
orders, a two-inode swap in both, a `copytruncate` copy replayed under a wildcard, a clean stop
splitting an unterminated line, a deleted file and a rotated file under an exact pattern losing
their unread tails at a restart and a crash, a `copytruncate` before any read counting no
truncation, and `files.open` sampled at the scan before a reap. `docker.rs` pins TAIL-10 at the
factory: an in-place rewrite that keeps `config.v2.json`'s length and mtime is never re-read (a
new mtime is), `end_scan` keeps only the containers a scan reached, and a 12-hex-character
name matches as an id prefix (not uppercase, not 11 characters).
