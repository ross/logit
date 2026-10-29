# Known gaps: File tailing, Docker logs, and replay

Entry format and the other areas: [the known-gaps index](README.md).

- **`docker_in`'s identity refresh, and its offset retention across a de-selecting rename, are
  both bounded by `poll_interval`, and the retention doesn't survive a `logit` restart.** The next
  poll tick picks up a `config.v2.json` change (a rename, or a metadata read recovering from an
  earlier failure), so a rename and a rename back within one tick is never observed. A container
  renamed out of `containers:` keeps its offset in memory, so a rename back resumes instead of
  replaying, but only within this process. If `logit` restarts between the two renames, the
  container falls back to `read_from` like any file the process has never seen. See [ADR
  `docker-container-identity-and-minimal-watches`](../adr/docker-container-identity-and-minimal-watches.md).
- **The tail driver notices shutdown only between two files' reads.** One read is at most 64 KiB
  (`READ_CHUNK_BYTES`), but the driver emits its lines before the next check, and each `emit`
  waits on the downstream. Against a slow or stalled downstream, `run_input`'s grace backstop can
  drop the task first, with no final flush and no final checkpoint. Nothing is lost: the restart
  resumes from the last interval checkpoint, which lands between passes even under a backlog, so
  it replays at most one `checkpoint_interval` or one 64 KiB chunk per file.
- **A file removed or rotated out of every pattern before `tail_in` read it to its end, at a stop
  or while `tail_in` is down, loses its unread tail, and the loss is counted only as far as the
  last checkpoint knew.** The checkpoint records the file's inode, offset, and length, but the
  restart's scan never finds the file. It drops the entry
  (`crates/logit-inputs/src/tail/driver.rs`, `Tailer::prune_checkpoint_entries`) and counts the
  bytes the entry recorded as unread as `logit.input.bytes.dropped{reason="gone"}`, with a
  `file_gone` diagnostic, once a checkpoint write without the entry lands. The count is exact
  only when no line was held or being dropped at the stop and a scan or a read to the end saw the
  file's final size. Otherwise it isn't the loss:
  - What the file gained after `tail_in` last saw its size isn't counted, so for a file still
    being written the count is short.
  - The count runs from the resume offset, not the read position, so it includes bytes read after
    a held or dropping line's start: an oversize line's consumed prefix, already counted in
    `logit.input.frames.dropped{reason="oversize"}`, and under `docker_in`, the other stream's
    entries delivered after a held partial.
  - After a kill, the count comes from the last interval checkpoint, not the process's last
    read. It can include bytes the killed process had already delivered, and miss bytes written
    after that checkpoint.
  - A file created and removed wholly while `tail_in` was down isn't counted: no checkpoint
    names it.
  - A file whose inode a new file reused while `tail_in` was down is counted the same way: the
    new file's content rejects the entry, counted `logit.input.files.resume_rejected`, and if no
    new file claims the entry by content, its unread bytes are counted gone ([ADR
    `tail-content-identity`](../adr/tail-content-identity.md), decision 6).

  A pattern that also matches the rotated name (`app.log*`) avoids the loss for a rename, but not
  for a removal, such as a rotator's deletion of its oldest file while `tail_in` is down. See
  [ADR
  `tail-discovery-failure-and-resume-identity`](../adr/tail-discovery-failure-and-resume-identity.md),
  decision 6 and its 2026-10-18 amendment.
- **A rotated file stays open, read to its end, for as long as a pattern matches its new
  name.** A scan that finds a tracked inode under another matched name rebinds it and keeps it
  `Active`, so under `app.log*` each rename of `file_out`'s or logrotate's cascade moves the open
  file to its next name. `tail_in` closes a file only once no pattern matches any of its names:
  renamed out of every pattern, or removed. It reads the file to its end first and closes it one
  `poll_interval` later (`crates/logit-inputs/src/tail/driver.rs`, `Tailer::open_tracked` and
  `Tailer::reap_drained`).
  - **Consequence:** one descriptor and one file watch per retained rotated file, shown in
    `logit.input.files.open`. A rotator that deletes its oldest file (`file_out`'s `max_files`,
    logrotate's `rotate`) bounds them at its retention. One that keeps every file grows them by
    one per rotation, with no bound but the process's descriptor limit. The soak harness's
    `tail-rotate`, keeping every file, saw the SUT's descriptors grow from 16 to 34 across 18
    rotations (`docs/plans/soak-harness.md`, "W11b: file tailing"). Its `tail-rotate-delete`,
    whose writer keeps eight files, saw `logit.input.files.open` level off at its retention
    (`docs/plans/soak-harness.md`, "W18: a deleting rotation").
  - **Workaround:** bound the rotator's retention. A pattern that matches only the active name
    (`app.log`) closes each rotated file once it's read, but loses the file's unread tail at a
    stop or a crash, counted only as far as the last checkpoint knew (the entry above).
- **`logit.input.files.rotated` misses a rotation when a scan lands in logrotate `create`'s
  rename-to-create gap.** logrotate renames the file away and creates its replacement afterward. A
  scan between the two finds nothing at the path, so it retires the old inode (which drains to its
  end), and the next scan opens the replacement at offset 0 as a new file. No line is lost; only
  the counter misses that rotation. The `tail-rotate` perf scenario rotates by a hard link, then a
  rename, so its rotation count can't hit this window (`crates/logit-perf/src/file_load.rs`'s
  module doc).
- **`tail_in` splits a line held unterminated at a clean stop into two events.** Shutdown emits the
  partial line as it stands (`docker_in` holds it instead), and the checkpoint records the end of what was read, so the rest of
  the line, written later, arrives after the restart as a line of its own. The exception is a line
  being dropped for `max_line_bytes`: its checkpoint stays at its start, and it's dropped whole
  again. See
  [ADR `file-tailing-and-docker-json-logs`](../adr/file-tailing-and-docker-json-logs.md)'s
  "Checkpoints" section.
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
  (`envelope_cap` in `crates/logit-inputs/src/docker.rs`) whole and counts it (`long_line` and
  `logit.input.frames.dropped{reason="oversize"}`), and
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
- **A truncation counts at most one of a held entry and the unterminated line behind it.**
  When `docker_in`'s file is truncated in place, the decoder counts each stream's held reassembly
  `logit.input.frames.dropped{reason="truncated"}` (a drop in progress was counted `oversize`
  when it began), and the driver counts the unterminated line only when the decoder holds
  neither, because that line most often continues the held message. An unterminated line that
  starts a message on the other stream, beside a held reassembly or a drop in progress, is lost
  uncounted. The partial
  line can't name its stream: json-file writes `stream` after `log`.
  - **To close:** parse the stream from the partial line when its envelope got that far.
- **`held_from` is the oldest held line across both streams, so a long reassembly on one stream
  pins the checkpoint.** The other stream's lines after that offset are already emitted, and a
  crash or a clean stop replays them. Replay, not loss.
- **A line that never ends pins the checkpoint while it is being dropped.** A `\r`-only progress
  bar is the case: dockerd writes it as 16 KiB partial entries but never a closing one, because
  only `\n` ends a message. A drop for `max_line_bytes` therefore runs until a newline that may
  never come, and a crash replays everything since the drop began. Replay, not loss, but
  unbounded.
  - **To close:** persist per-stream drop state in the checkpoint.
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

## `replay_in`

- **`replay_in` reads one finished file once, with no follow mode, no directory or glob, no rate
  pacing, and no checkpoint** ([ADR `replay-input`](../adr/replay-input.md)). It stops at the end
  of the file even if a writer is still appending, sends events as fast as downstream takes them,
  and a restart reads the file from its start.
  - **Consequence:** a restarted replay sends every event again, and a replay into a rate-limited
    destination meets that destination's limits at full speed.
  - **Workaround:** follow a growing file with `tail_in` and a `json` stage. Run one `replay_in`
    per file, and put the restart decision in whatever starts `logit`.
  - **Revisit trigger:** an operator replaying archives too large to resend after a failure, or a
    flow that needs pacing.
- **Neither side of the archive pair handles a compressed JSON file.** `file_out` compresses only
  `format: native`, as lz4 inside each frame, and never compresses a `format: json` file, rotated
  or not. `replay_in` reads no compressed file: no `.gz`, and no lz4-framed JSON. A
  `format: native` archive written with `compression: lz4` replays, since its compression is per
  frame.
  - **Consequence:** a JSON archive takes its full size on disk, and one an external tool gzipped
    after rotation has to be decompressed before `replay_in` can read it.
  - **Workaround:** archive as `format: native` with `compression: lz4`, or decompress to a file
    (or a named pipe) before replaying.
  - **Remedy:** one follow-up that adds both sides together, gzip on `file_out`'s `format: json`
    rotation and `replay_in` reading a `.gz` file by its extension, so archiving and replaying
    stay one story.
