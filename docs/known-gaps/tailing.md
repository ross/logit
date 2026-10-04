# Known gaps: File tailing and Docker logs

Entry format and the other areas: [the known-gaps index](README.md).

- **`docker_in`'s identity refresh, and its offset retention across a de-selecting rename, are
  both bounded by `poll_interval`, and the retention doesn't survive a `logit` restart.** The next
  poll tick picks up a `config.v2.json` change (a rename, or a metadata read recovering from an
  earlier failure), so a rename and a rename back within one tick is never observed. A container
  renamed out of `containers:` keeps its offset in memory, so a rename back resumes instead of
  replaying, but only within this process. If `logit` restarts between the two renames, the
  container falls back to `read_from` like any file the process has never seen. See [ADR
  `docker-container-identity-and-minimal-watches`](../adr/docker-container-identity-and-minimal-watches.md).
- **`docker_in` only watches `root` and the files it currently has open, so a log file's own
  first appearance inside an already-existing container directory, a rotation, and a
  `config.v2.json` change are all discovered on the next `poll_interval` tick, not instantly.**
  Only a container directory arriving or leaving under `root` is `inotify`-fast, because Docker's
  per-container state directories are direct children of `root`.
  - **Consequence:** latency only; none of the three poll-bound cases loses data.
  - **Workaround:** a shorter `poll_interval` is the only way to tighten them. A
    per-container-directory watch would catch them near-instantly, but would cost
    O(containers on the host) work per log line written anywhere on the host. See [ADR
    `docker-container-identity-and-minimal-watches`](../adr/docker-container-identity-and-minimal-watches.md).
- **`docker_in`'s timestamps are the one exception among the tailing decoders to "stamp receipt
  time."** It uses the json-file envelope's own `time` field (the daemon's same-host clock),
  because replaying a backlog (`read_from: beginning`, or a fresh container's already-written
  history) as "now" would misstate when those lines happened. See the "docker_in timestamps"
  section of [ADR `file-tailing-and-docker-json-logs`](../adr/file-tailing-and-docker-json-logs.md).
  - `tail_in` keeps the general rule (read time, matching `syslog_in`), because a plain text line
    carries no timestamp to trust.
  - Receipt time isn't a repo-wide invariant either: `otlp_in` prefers a record's own
    `time_unix_nano` when set, falls back to `observed_time_unix_nano` only for the zero "unknown"
    sentinel, and preserves `observed_time_unix_nano` both ways (`otlp/logs.rs`'s module doc,
    [ADR `metrics-model-v2`](../adr/metrics-model-v2.md)).
- **`tail_in`/`docker_in`'s checkpoint identity is `(dev, ino)`, which doesn't survive a bind
  mount or filesystem migration that preserves content but not inode numbers.** A restored backup,
  a volume moved to different storage, or a bind mount re-created from a snapshot resumes from the
  beginning instead of the checkpointed offset. That's safe (at-least-once still holds), but not
  the seamless resume of the common case.
- **The tail checkpoint is at-least-once only up to the downstream in-memory queues.** Shutdown
  flushes every file's accumulator, then force-writes the checkpoint, so the offset covers every
  line flushed into a sink's inbox. A sink that then drops that batch when its own grace runs out
  (`logit.component.batches.dropped{reason="shutdown"}`) loses it for good, because the restart
  resumes past it. A `buffer.disk:` sink spools the batch instead. See [ADR
  `file-tailing-and-docker-json-logs`](../adr/file-tailing-and-docker-json-logs.md)'s 2026-09-26
  amendment.
- **The tail driver notices shutdown only between two files' reads.** One read is at most 64 KiB
  (`READ_CHUNK_BYTES`), but the driver emits its lines before the next check, and each `emit`
  waits on the downstream. Against a slow or stalled downstream, `run_input`'s grace backstop can
  drop the task first, with no final flush and no final checkpoint. Nothing is lost: the restart
  resumes from the last interval checkpoint, which lands between passes even under a backlog, so
  it replays at most one `checkpoint_interval` or one 64 KiB chunk per file.
- **A file removed or rotated out of every pattern loses its unread tail at a clean stop or a
  crash**, whether or not a scan noticed first.
  - After a clean stop, the checkpoint records its inode and offset, but the restart's scan never
    finds the file, so the entry is never used.
  - After a crash, the lines it had read but not yet flushed are gone too.

  A pattern that also matches the rotated name (`app.log*`) avoids it for a rename, but not for a
  removal. See [ADR
  `tail-discovery-failure-and-resume-identity`](../adr/tail-discovery-failure-and-resume-identity.md),
  decision 6.
- **Under a pattern that matches rotated names, `copytruncate` re-emits the whole file on every
  rotation, and `compress` tails `app.log.N.gz` as text.**
  - The copy `copytruncate` writes is a new inode, so `app.log*` reads it from `0`; a recorded run
    re-emitted about 1,600 to 2,000 lines per three rotations.
  - A `.gz` file is read as lines of binary, diagnosed `invalid_utf8`.

  `docs/deploying.md`'s "What to watch for file tailing" has the guidance. See [ADR
  `file-tailing-and-docker-json-logs`](../adr/file-tailing-and-docker-json-logs.md)'s "Rotation and
  truncation" section.
- **Under an exact pattern, `copytruncate` loses the lines written after the tailer's last read
  and before the truncate.** They exist only in the copy, which the pattern doesn't match. The
  window is up to one `poll_interval` of writes (or one wake). See [ADR
  `file-tailing-and-docker-json-logs`](../adr/file-tailing-and-docker-json-logs.md)'s 2026-09-28
  amendment.
- **`logit.input.files.rotated` misses a rotation when a scan lands in logrotate `create`'s
  rename-to-create gap.** logrotate renames the file away and creates its replacement afterward. A
  scan between the two finds nothing at the path, so it retires the old inode (which drains to its
  end), and the next scan opens the replacement at offset 0 as a new file. No line is lost; only
  the counter misses that rotation. The `tail-rotate` perf scenario rotates by a hard link, then a
  rename, so its rotation count can't hit this window (`crates/logit-perf/src/file_load.rs`'s
  module doc).
- **`tail_in` splits a line held unterminated at a clean stop into two events.** Shutdown emits the
  partial line as it stands, and the checkpoint records the end of what was read, so the rest of
  the line, written later, arrives after the restart as a line of its own. The exception is a line
  being dropped for `max_line_bytes`: its checkpoint stays at its start, and it's dropped whole
  again. See
  [ADR `file-tailing-and-docker-json-logs`](../adr/file-tailing-and-docker-json-logs.md)'s
  "Checkpoints" section.
- **A `copytruncate` that the writer refills past the old offset before the next check goes
  undetected.** Truncation is `len < offset`, seen at a scan or a read. A file truncated in place
  and grown beyond the tailer's offset within one `poll_interval` (or one wake) looks like an
  ordinary append: the tailer reads the new content from the old offset, and the bytes before it
  aren't emitted during the run. The inode doesn't change under `copytruncate`, so nothing
  signals it while `logit` runs.
  - With a checkpoint, a restart replays the file through the head fingerprint, so the bytes are
    recovered late, not lost.
  - A writer that rotates by rename has no such window.

  See [ADR `file-tailing-and-docker-json-logs`](../adr/file-tailing-and-docker-json-logs.md)'s
  2026-09-28 amendment.
- **A resume verifies only the first `min(256, offset)` bytes of a file, so a recycled inode
  whose new content shares them still resumes at a stale offset.** A checkpoint entry and a
  de-selection retention carry a hash of the bytes the tailer read from the file's head. A resume
  starts at `0` if the file is shorter than that head, differs in those bytes, or is shorter than
  the offset.
  - For an offset of at most 256, every skipped byte is identical content.
  - Beyond that, a new file with the same first 256 bytes skips the bytes between there and the
    stale offset. Files that start with a timestamp or a per-file header make this unlikely.
  - A clean stop's mid-line offset, resumed this way into a file truncated and refilled while the
    entry was unspent, splits the new generation's line at the same point it split the old one.
    The halves concatenate to the new line, and the old line is the copy's loss.

  See [ADR
  `tail-discovery-failure-and-resume-identity`](../adr/tail-discovery-failure-and-resume-identity.md),
  decision 2.
- **A directory unreadable at startup replays its files under `read_from: end` once it becomes
  readable.** `read_from: end` applies only to files the bind-time scan found. A file first listed
  after that listing failed starts at its beginning, which favors duplicates over loss.
- **A file under a directory that stays unreadable stays tracked** until the listing recovers or
  its inode is unlinked. A failed listing retires nothing, and the handle check catches removal
  and truncation but not a rename. Even an unlinked file stays open until a listing covering its
  path succeeds, because a draining file is reaped only after a scan that could have rebound it.
  `ELOOP` on a `docker_in` container's log path (a looping `<id>-json.log` symlink) gets the same
  treatment: unknown, kept, and diagnosed. See [ADR
  `tail-discovery-failure-and-resume-identity`](../adr/tail-discovery-failure-and-resume-identity.md),
  decision 1.
- **A read error on a `Draining` file loses its unread tail.** The driver reports a read error as
  EOF so a handle that keeps failing is reaped, and the reap drops whatever the file still held,
  diagnosed `read_error`. An `Active` file is never reaped on a read error.
  `a_read_error_on_a_draining_file_reaps_it_and_loses_its_unread_tail` pins it through the fault
  seam's `tail.read` site. See [ADR
  `file-tailing-and-docker-json-logs`](../adr/file-tailing-and-docker-json-logs.md)'s 2026-09-28
  amendment.
- **A `containers:` entry that is a container's *name* and also 12 or more hex characters selects
  any container whose id starts with it.** An entry matches a container if it equals the
  container's name, or if it's an id prefix: at least 12 hex characters that start the
  container's id. So naming a container `deadbeefcafe` and listing that name also selects a
  different container whose id begins `deadbeefcafe`.
  - **Workaround:** don't give a container a name shaped like an id prefix. See [ADR
    `docker-container-identity-and-minimal-watches`](../adr/docker-container-identity-and-minimal-watches.md).
- **An envelope over the cap is dropped by the splitter without the decoder seeing it, so a dropped
  *closing* fragment lets the next line on that stream splice onto the held partial.**
  `docker_in`'s `LineSplitter` drops a json-file line longer than its envelope bound
  (`envelope_cap` in `crates/logit-inputs/src/docker.rs`) whole and counts it `long_line`, and
  `DockerDecoder` never learns a line went missing. If the dropped line was a message's closing
  fragment, the held partial stays open and the next same-stream line joins it.
  - **To close:** the splitter must report drops in sequence, and a `TailDecoder` hook must
    receive them. See [ADR
    `file-tailing-and-docker-json-logs`](../adr/file-tailing-and-docker-json-logs.md)'s 2026-09-28
    amendment.
- **An `attrs` object larger than the envelope bound's slack can drop entries as `long_line`.**
  The bound leaves fixed room for `attrs` beside a worst-case-escaped fragment. An envelope whose
  `attrs` outgrows that room can exceed the bound, and the splitter drops it unseen. `attrs` comes
  from `--log-opt labels`, `env`, and `tag`. The slack and the bound are `envelope_cap` in
  `crates/logit-inputs/src/docker.rs`.
- **`held_from` is the oldest held line across both streams, so a long reassembly on one stream
  pins the checkpoint.** The other stream's lines after that offset are already emitted, and a
  crash replays them. Replay, not loss.
- **A line that never ends pins the checkpoint while it is being dropped.** A `\r`-only progress
  bar is the case: dockerd writes it as 16 KiB partial entries but never a closing one, because
  only `\n` ends a message. A drop for `max_line_bytes` therefore runs until a newline that may
  never come, and a crash replays everything since the drop began. Replay, not loss, but
  unbounded.
  - **To close:** persist per-stream drop state in the checkpoint.
- **At shutdown, `docker_in`'s unterminated tail is almost always a dockerd write in progress,
  and it is emitted as a `bad_line`.** `close_decoder`'s `take_partial` turns the tail into a
  rejected line, and the final checkpoint then skips past it, so that line's content never becomes
  an event.
  - **To close:** let a decoder opt out of `take_partial` at shutdown.
- **`inotify` doesn't reliably fire over network or FUSE-backed mounts** (NFS chief among them),
  and `watch: auto` falls back to polling only on outright setup failure, not on a mount type it
  can't detect in advance.
  - **Workaround:** for a config on such a mount, set `watch: poll` explicitly. `poll_interval`
    is the only mechanism proven to work everywhere.
- **`tail_in`/`docker_in`'s `inotify` wake source is Linux-only.** Every other platform runs
  `watch: poll` regardless of config, and an explicit `watch: inotify` is a startup error, not a
  silent downgrade.
- **A *file* watch that fails to register is never retried for that file.** `Watcher::watch_file`
  runs once per tracked inode, when `Tailer::open_tracked` opens it. A failure (realistically
  `ENOSPC` against `fs.inotify.max_user_watches` on a host tailing many files) is diagnosed
  `watch_error` with the errno. That file then relies on `poll_interval` for data wakes, as under
  `watch: poll`, until it's rotated or re-opened.
  - The *directory* watch self-heals: it's re-armed on every `scan`
    ([ADR `file-tailing-and-docker-json-logs`](../adr/file-tailing-and-docker-json-logs.md)'s
    2026-09-21 amendment).
  - A per-file retry would re-attempt one syscall per unwatched file per scan, with nothing
    suggesting the limit moved; the diagnostic points at the sysctl instead.
- **Two spellings of one directory (a symlink, a `.` component) share a single kernel watch, and
  `logit.input.watch.watches` counts them twice.** `inotify_add_watch` follows symlinks, and the
  desired set is keyed on the configured path string, so `paths: [/var/log/app/*.log,
  /srv/app/logs/*.log]` over a symlink registers one watch and reports two.
  - **Consequence:** only the gauge over-reports, and `docs/deploying.md`'s "What to watch for
    file tailing" says so. A `Wake::Discover` may name the other spelling, but the driver discards
    its payload before rescanning, and the `IN_IGNORED` purge drops both entries together.
  - **To close:** normalizing the desired set (or passing `IN_DONT_FOLLOW`) would change which
    paths a config can name. That's a config-surface decision, not a bug fix.
- **`parse_events` discards the rest of a `read` buffer after a malformed event**, rather than
  resynchronizing. That's unreachable from a real inotify fd: the kernel never returns a partial
  event, and `len` is always 0 or a multiple of 16, both pinned in the ADR. It's acceptable
  because the poll tick and the unconditional `drain` reconcile whatever a discarded event would
  have said.
- **`docker_in` only speaks the json-file log driver.** Docker's other logging drivers (`local`,
  `journald`, `syslog`, and more) don't write a per-container file this driver could tail. Each
  would be separate work, not a parameter on this one.
- **No per-input stream filter on `docker_in`.**
  - **Workaround:** to keep only `stdout` (or only `stderr`), add a downstream stage that reads
    `log.iostream`. `demo/logit.yaml`'s `nginx_stdout`, an inline `lua` component, is the worked
    example.

  A config field on `docker_in` was set aside together with named output ports (the next entry).
  See [ADR `file-tailing-and-docker-json-logs`](../adr/file-tailing-and-docker-json-logs.md)'s
  "Alternatives considered".
- **Named output ports on a component (a listener publishing separate named streams other
  components subscribe to individually, e.g. `docker_in` publishing `stdout`/`stderr` as two
  distinct sources) don't exist.** They touch the component graph's core arity and wiring model
  broadly enough to need their own design, not a `docker_in`-sized increment.
  - **Revisit trigger:** a second, unrelated need for the same shape. The other motivating case
    was a `splitter`-style component fanning a multi-signal event out into separate
    logs/metrics/traces streams. See [ADR
    `file-tailing-and-docker-json-logs`](../adr/file-tailing-and-docker-json-logs.md)'s
    "Alternatives considered".
- **`config.v2.json` is an internal Docker daemon format, not a documented public API.**
  `docker_in` reads it directly (no socket, no HTTP client) because it sits next to the log file
  already being read, but a Docker version bump could change its shape without notice. A missing
  or unparseable file degrades gracefully: a `container.id`-only resource, diagnosed
  `metadata_error`. The residual risk is a *silently reshaped* file that still parses but means
  something different.
