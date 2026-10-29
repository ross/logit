---
created: 2026-10-29
updated: 2026-10-29
---

# Tail content identity: the inode stays primary, content claims an unmatched inode, and what a restart or rotation still loses is decided or counted

## Status
Accepted and built: `tailid/w1` implements decisions 2 and 7, and `tailid/w2` implements
decisions 4, 5, and 6. Decisions 4, 5, and 7 say how the build refined them: where the marker's
bytes come from, when an inode waits before it claims, and when a truncation's counts are
recorded.

## Context

[ADR `file-tailing-and-docker-json-logs`](file-tailing-and-docker-json-logs.md) promises that a
tail restart is "strictly duplicates on restart, never loss", and [ADR
`tail-discovery-failure-and-resume-identity`](tail-discovery-failure-and-resume-identity.md)
made a resume verify the file's head before it trusts an inode. Several ways to lose or replay
lines stayed open, each listed in `docs/known-gaps/tailing.md`:

- **Shutdown.** The checkpoint covers lines handed into a downstream inbox. A sink that drops
  that batch when its shutdown grace runs out (`logit.component.batches.dropped{reason="shutdown"}`)
  loses it, and the restart resumes past it.
- **`compress`.** Under `app.log*`, a rotated `app.log.2.gz` is read as lines of binary,
  diagnosed `invalid_utf8`.
- **`copytruncate` under a pattern that matches rotated names.** The copy is a new inode, so
  `app.log*` reads it from `0` and re-emits the whole file on every rotation.
- **`copytruncate` under an exact pattern.** The lines written after the tailer's last read and
  before the truncate exist only in the copy, which no pattern matches. Nothing counts them.
- **The 256-byte residual.** A resume checks only the first `min(256, offset)` bytes, so a
  recycled inode whose new content shares them resumes at a stale offset and skips bytes.
- **Migration.** Identity is `(dev, ino)`, so after a restored backup or a volume moved to other
  storage, no checkpoint entry matches a file, and each file starts where `read_from` puts it.
  Under the default `read_from: end`, that skips each file's unread tail, counted
  `logit.input.bytes.dropped{reason="gone"}` when the scan prunes the unmatched entries. Under
  `read_from: beginning`, every file replays from `0`.
- **A reused inode hides a gone file.** When a new file reuses a checkpointed inode while
  `tail_in` is down, the head check rejects the resume (`logit.input.files.resume_rejected`), and
  the original file's unread tail is lost with no `*.dropped` count, against the rule in [ADR
  `telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md).

### Prior art

Four tailers were read from source at their main branches on 2026-10-29:

- **Vector's `file` source** (`lib/file-source-common/src/fingerprinter.rs`,
  `lib/file-source/src/file_watcher/mod.rs`) identifies a file by a CRC-64 of its first line,
  with no inode and no truncation check. Under `app.log*` a `copytruncate` copy matches the
  fingerprint and resumes at the stored offset. It decompresses gzip, detected by magic bytes,
  and reads a compressed file only from `0`.
- **OpenTelemetry's `filelog` receiver** (`pkg/stanza/fileconsumer/internal/fingerprint/fingerprint.go`,
  `pkg/stanza/fileconsumer/design.md`) identifies a file by its first 1,000 raw bytes, matched by
  prefix while the file is shorter, and ignores the inode. Its design doc says an exact-pattern
  `copytruncate` "can cause data loss." It decompresses gzip under `compression: auto`.
- **Filebeat's `filestream` input** (`filebeat/input/filestream/identifier_fingerprint.go`)
  defaults to a SHA-256 over a fixed byte range of the file (the first 1,024 bytes, at least 64).
  On a collision it ingests only the first file. Its docs advise against `copytruncate`. It
  decompresses gzip under `compression: auto`.
- **Fluent Bit's `tail` input** (`plugins/in_tail/tail_file.c`) identifies a file by inode and
  stores a 64-bit hash of the up to 32 bytes before the offset. On restore, an offset past the
  file's size or a hash mismatch reads the file from `0`. That catches a reused inode and a
  truncate-and-refill while it was stopped.

None of them recovers or counts the exact-pattern `copytruncate` loss, and none counts the unread
bytes of a file that vanished while it was down. Fluent Bit's offset hash is the only check of
the bytes at the offset. The three content-keyed tools each follow a `copytruncate` copy under a
rotated-name pattern because their identity is the content, at the cost of every collision on
their common path.

## Decision

The inode stays the primary identity. A content claim is the fallback for an inode no checkpoint
entry matches, and an offset marker checks the bytes at the resume point. What the tailer can't
recover is counted or recorded here as a decided limit. Each numbered item is one decision a
reviewer can check.

1. **The shutdown checkpoint loss is a decided limit.** The tail checkpoint means what an input's
   acknowledgment means in [ADR `delivery-semantics`](delivery-semantics.md), item 3: the lines
   are in every open downstream inbox. A sink that drops that batch at its shutdown grace, counted
   `logit.component.batches.dropped{reason="shutdown"}`, loses it, because the restart resumes
   past it. A crash loses every checkpointed line still in memory: in a sink's queue, in a
   transform's inbox, on an edge, or in an `aggregate` window. There is no end-to-end
   acknowledgment (Alternatives considered). The remedy is a `buffer.disk:` spool on every sink
   downstream of the tail source. On a clean shutdown it keeps what the grace would drop. On a
   crash it keeps only what reached the sinks' spools, and lines still upstream of them are lost.
   `docs/deploying.md`'s tailing section says so.

2. **A compressed file is skipped and counted, never tailed as text.** The tailer detects one by
   the magic number at the file's head:

   | Format | Magic bytes |
   |---|---|
   | gzip | `1f 8b 08`: the ID bytes and deflate, gzip's one defined method |
   | zstd | `28 b5 2f fd` |
   | xz | `fd 37 7a 58 5a 00` |
   | bzip2 | `42 5a 68` (`BZh`), a block-size digit `1` to `9`, then the first block's magic `31 41 59 26 53 59` or an empty stream's end-of-stream magic `17 72 45 38 50 90` |

   bzip2's `BZh` alone is printable, and a text log can start with it, so the match takes the
   whole 10-byte stream header. A file whose bytes so far are shorter than a magic they're a prefix
   of, an empty file included, is read no further until more bytes decide it.

   A skipped file counts `logit.input.files.skipped{reason="compressed"}`, with a throttled
   `compressed` diagnostic naming the path. It's `skipped`, not `dropped`: under the pattern that
   reaches a compressed file (`app.log*`), its content was already read under its uncompressed
   name, so the file carries nothing new, and the pattern is the operator's choice. Not
   decompressing is a decided limit: the three content-keyed tools above decompress gzip, and
   `logit` doesn't.

3. **The inode stays the primary identity.** A file whose `(dev, ino)` names a checkpoint entry
   or a de-selection retention resumes by inode, under decision 4's check. Content identity is
   used only for an inode with no such match (decision 5). The common path, a file that keeps its
   inode, is unchanged.

4. **An offset marker checks the bytes before the resume point.** A checkpoint entry carries a
   marker beside the head hash: XXH64 with seed 0, as the head hash uses, over the up to 256 bytes
   that end at the checkpointed offset. For `tail_in` the tailer captures those bytes from what
   it has already read: the up to 256 bytes before its read offset, and the same before its last
   line boundary, which is the offset a checkpoint persists while a line is pending. Each read
   copies at most 256 bytes into each, and steady-state tailing reads nothing extra from the
   file. Under `docker_in`, a held reassembly pins the persisted offset at the start of its oldest
   held line, which neither buffer covers, so a checkpoint write reads the 256 bytes there back
   from the open descriptor, once per distinct offset, and seeks back. A failed read-back is
   diagnosed `read_error`, and the checkpoint persists that file's last position whose marker it
   took, else offset `0` with the empty marker, never an offset whose marker it couldn't take: a
   restart resumes from there, a replay with no `resume_rejected` and no `gone` count. A resume by inode accepts iff the head
   matches, the marker matches, and the offset is at most the file's length; anything else is a
   `resume_rejected` and a start at `0`, as today. That closes the 256-byte residual: a recycled
   inode that shares the first 256 bytes now also has to share the 256 bytes before the offset. It
   also catches a `copytruncate` refilled past the old offset while `logit` was down whose new
   content shares the old head, which the head check alone accepts. The marker makes this
   checkpoint format 4. There's no format 3 reader: a format 3 file is unusable, as an unknown
   version already is, so every file replays once on upgrade (`logit` is pre-release).

5. **A content claim resumes an inode no entry matches.** A newly discovered inode with no
   inode-matched resume entry may claim one candidate:

   - an unclaimed checkpoint entry, including one its own inode's content rejected; or
   - a file tracked in this process, by its in-memory head and the marker at its emitted offset,
     including a truncated file (its state from before the truncation).

   A tracked file's candidate is its last line boundary, the offset a checkpoint would persist,
   so a claim resumes at a line start and a line half read at a truncation is read whole from the
   copy. A truncated file's candidate lasts until a scan offers it with no claimant (decision 7).
   A rejected entry is a candidate only in the scan that rejected it.

   It claims a candidate iff the candidate's head covers at least 64 bytes, the file's length is
   at least the candidate's offset, and both the head hash and the marker match. A truncated
   file's candidate also needs the file to reach the original's read offset before the
   truncation, so a copy cut inside the pending line, which lacks bytes the tailer read, can't
   claim it: it waits as a copy still being written, then starts where it would have, and the
   truncation's counts stand. It resumes at the
   candidate's offset, overriding `read_from` as a resume does, counted
   `logit.input.files.claimed`. If more than one file matches one candidate, or one file matches
   more than one candidate, nothing is claimed and a `claim_ambiguous` diagnostic says so: the
   file starts where it would have without the claim, which favors duplicates over loss. Each scan
   reconciles every inode with an entry or a tracked file first, so every candidate the scan can
   see exists before any unmatched inode is probed, whatever order the listing yields.

   A scan after the first leaves an unmatched inode unopened, for at most two scans in a row,
   when claiming now could duplicate or a later scan could still find a match:

   - It matches a tracked file that hasn't been truncated: `copytruncate`'s copy, found before
     the truncate. Claimed now, the copy and the original would both read what the original reads
     before its truncation. Once the truncation is seen, the copy claims the original's state from
     before it. A copy whose original is never truncated (a plain `cp`) claims the original as it
     stands once the wait runs out.
   - It's shorter than a candidate whose head bytes are in memory and agrees with every byte it
     has: a copy still being written. An empty file never waits, so a rename rotation's new file
     isn't delayed.

   logrotate's `copytruncate` creates its copy empty and then fills it, so a scan can open the
   copy empty. A file opened empty, not from a resume or a claim, stays claimable until its first
   non-empty read, since it has emitted nothing. Before reading it, a drain probes it against the
   truncated and tracked files' candidates: if its bytes match one or may grow into one, the drain
   leaves it unread and the next scan's claim pass decides it as it does a new inode; otherwise
   it's read from `0` at once, as a rename rotation's new file is.

   The first scan never waits, since `read_from` decides only files it opens. The cost falls only
   on an unmatched inode the decoder factory accepts (under `docker_in`, a selected container's
   log; a rejected one is never opened): one open, a read of at most 256 bytes for the head, one
   hash per distinct candidate head length, and a read of at most 256 bytes per distinct
   candidate offset whose head matches. An inode that waits pays it again at each scan, and a
   file opened empty pays it once more when a drain first finds it holding bytes; any other is
   opened in the scan that probed it and never probed again. What it recovers:

   - a `copytruncate` copy under `app.log*` resumes at the original's offset instead of replaying
     from `0`;
   - a restored backup or a migrated volume resumes, instead of skipping each file's unread tail
     (under `read_from: end`) or replaying it from `0` (under `read_from: beginning`);
   - a `copytruncate` while `logit` was down is recovered under a pattern that matches the copy.

   The 64-byte floor is Filebeat's minimum fingerprint size. A shorter head is too likely to be a
   timestamp prefix or header that unrelated files share.

6. **A rejected entry nothing claims is counted as gone.** A checkpoint entry its inode's content
   rejected, and that no content claim takes by the end of that scan, counts its recorded unread
   bytes as `logit.input.bytes.dropped{reason="gone"}` with a `file_gone` diagnostic. It goes
   through the path a pruned entry already takes, counted only once a checkpoint write without
   the entry lands ([ADR
   `tail-discovery-failure-and-resume-identity`](tail-discovery-failure-and-resume-identity.md)'s
   2026-10-18 amendment). A file created and removed wholly while `tail_in` was down can't be
   counted, because no checkpoint recorded it. That's a decided limit.

7. **An exact-pattern `copytruncate` is counted, not recovered.** At a truncation, the tailer
   counts the bytes it knew existed past its read offset, the last size it saw before the
   truncation less that offset, as `logit.input.bytes.dropped{reason="truncated"}`. That's usually
   `0`, since the lost lines are writes no scan saw. The `truncated` diagnostic says that lines
   written before the truncate may be lost unless a pattern also matches the copy. The count is
   an estimate; `logit.input.files.truncated` is the signal that a truncation happened, and what
   the operator needs is to know it happened, not the exact count. The tailer doesn't search for
   the copy outside its patterns. `docs/deploying.md` steers to rename rotation, and, under
   `copytruncate`, to a pattern that matches the rotated names. A claim of the truncated file's
   pre-truncation state (decision 5) cancels the discarded line's
   `logit.input.frames.dropped{reason="truncated"}`, because the copy then resumes where the
   original stopped and holds the line whole. Of the bytes count it keeps only what the copy
   lacks: the last size the tailer saw less the larger of the copy's length and the
   pre-truncation read offset, when that's positive, since those bytes are in neither file. So
   that neither is counted and then retracted, both wait: they're recorded at
   the end of the first scan after the truncation whose listing failed nowhere and no waiting
   inode could still claim the state, or at a stop. A truncated file whose head is under 64 bytes
   can't be claimed, so its counts are recorded at once. `logit.input.files.truncated` and the
   `truncated` diagnostic are recorded at once either way.

8. **A refill past the old offset while running stays undetected.** A `copytruncate` that the
   writer refills past the tailer's offset before the next size check still looks like an append
   while `logit` runs: the tailer reads on from the old offset, and the refilled bytes before it
   aren't emitted. Detecting it would take a head re-read on every read. This is a decided limit,
   and rotation by rename avoids it (`docs/deploying.md`'s "Rotation" paragraph). At the next
   restart, the head check that exists today rejects the resume and replays the file, because the
   tailer still holds the old file's head. If the refilled file shares the old head, the resume
   is accepted, and the skipped bytes are lost. Decision 4's marker doesn't help here: the tailer
   takes it from the refilled bytes it read, so it matches them.

## Alternatives considered

- **Content as the primary identity, as Vector, OpenTelemetry's `filelog`, and Filebeat use.**
  Rejected. It changes the common path, a file that keeps its inode, and puts every content
  collision on that path: two files with the same first line or the same first kilobyte share an
  identity, and those tools warn, follow one, or ingest only the first. A fallback consulted only
  for an unmatched inode leaves the common path as it is and meets a collision only where the
  inode already failed.
- **Following the `copytruncate` copy outside the configured patterns.** Rejected. It would mean
  searching the directory for a file the operator didn't select, guessing at the rotator's naming,
  and reading files no `paths:` entry names. Counting the truncation and steering the pattern
  (decision 7) keeps `tail_in` reading only what its config names.
- **Decompressing compressed files.** Rejected. Under the patterns that reach a rotated `.gz`, its
  content was already read under its uncompressed name, so decompressing it would replay the
  file. Reading archives once is `replay_in`'s shape, not a tailer's, and a decompressor per
  format adds dependencies for no lines the tailer lacks.
- **End-to-end acknowledgment, advancing the checkpoint only once every sink delivered.**
  Rejected. [ADR `delivery-semantics`](delivery-semantics.md) already rejects it (item 3, and
  "An opt-in durable acknowledgment" under its alternatives). A token can't pass through
  `aggregate` or a Lua `flush()`, and most tail pipelines have one: each emits batches that
  correspond to no input batch. `BatchContext`, which an edge carries, is `Copy` and
  pinned at 32 bytes, so it has no room for an acknowledgment handle. `buffer.disk:` closes the
  window on the sink side with what exists.
- **Fluent Bit's 32-byte marker.** Rejected in favor of 256 bytes, the head's size, so one rule
  and one hash cover both ends of what a resume skips.

## Consequences

- **Operator-visible names.** `logit.input.files.skipped{reason="compressed"}` and the
  `compressed` diagnostic (decision 2), `logit.input.bytes.dropped{reason="truncated"}`
  (decision 7), `logit.input.files.claimed` and the `claim_ambiguous` diagnostic (decision 5),
  and `bytes.dropped{reason="gone"}` for a rejected, unclaimed entry (decision 6), each in
  `docs/design/internal-telemetry.md`'s tail table.
- **One replay on upgrade.** Checkpoint format 4 makes every format 3 checkpoint unusable, so
  every file present at the first scan after the upgrade replays from `0`.
- **Each tracked file holds two more buffers of at most 256 bytes**, the bytes before its read
  offset and before its line boundary, so the marker costs no read in steady state (decision 4).
- **A truncation keeps the file's pre-truncation head and marker** beside the tailer, not on the
  file, until a copy claims them or decision 7 counts them; the tracked file's own head still
  clears ([ADR `tail-discovery-failure-and-resume-identity`](tail-discovery-failure-and-resume-identity.md),
  decision 2).
- **What a claim still duplicates or loses.** Each is a decided limit:
  - Under `copytruncate`, the bytes the writer appends after the copy and before the truncate are
    in neither file. The original reads them only if a read lands in that window. Those a size
    observation saw are counted `bytes.dropped{reason="truncated"}` when the copy claims
    (decision 7); those written after the last observation are lost uncounted, the same floor as
    an exact pattern's.
  - A copy shorter than what the tailer read from the original (the original read appends the
    copy missed) can't claim, and once its wait runs out it's read from `0`, duplicating the
    original's lines.
  - A copy whose head is under 64 bytes, or that matches ambiguously, is read from `0`.
  - A plain `cp` of a live file claims the original as it stands once its wait runs out, so the
    lines the original reads after that are read again from the copy.
  - A truncation held at a stop is counted, though a copy the restart claims by the original's
    rejected entry may then deliver those bytes. A crash records no held truncation's counts.
  - Under `docker_in`, a truncated file whose decoder holds a reassembly offers no candidate:
    its boundary is the held line's start, whose bytes the truncation removed. The copy is read
    from `0`, and the decoder's reset counts its own drops as before.
  - A line being dropped for `max_line_bytes` at a truncation leaves the boundary at its start,
    so a copy that claims there drops it again, counted `oversize` a second time.
- **Decided limits, not gaps.** The shutdown loss (decision 1), not decompressing (decision 2), a
  file created and removed while `tail_in` was down (decision 6), a refill past the offset while
  running (decision 8), and what a claim still duplicates or loses (above) are recorded here. The
  refill's known-gaps entry moved to `docs/deploying.md`'s "Rotation" paragraph with this record,
  and `docs/known-gaps/tailing.md` no longer lists the wildcard `copytruncate` replay, the
  256-byte residual, or the `(dev, ino)` migration.
- **`tailid/w1`** implements decisions 2 and 7: the compressed skip and the truncation count.
- **`tailid/w2`** implements decisions 4, 5, and 6: the format 4 offset marker, the content
  claim, and gone counting for a rejected entry nothing claims, and narrows decision 7's counts on
  a claim to what the copy lacks. `crates/logit-inputs/src/tail/driver.rs`'s `Tailer::claim_unmatched` and
  `crates/logit-inputs/src/tail/checkpoint.rs`'s module doc are the code's statement of both.
