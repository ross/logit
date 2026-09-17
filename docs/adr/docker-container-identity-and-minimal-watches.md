---
created: 2026-09-17
updated: 2026-09-17
---

# `docker_in`: live container identity, and a minimal `inotify` watch set

## Status
Accepted

## Context

[ADR `file-tailing-and-docker-json-logs`](file-tailing-and-docker-json-logs.md) built `docker_in`
on the assumption that container identity is fixed for the life of a log file's open handle, and
that watching every container subdirectory under `root` is the right price for near-immediate
discovery. Both break down on a real, churning host:

**Identity is frozen at open.** `DockerDecoderFactory::open` reads the sibling `config.v2.json`
once, when a container's log file is first opened, and never again. A `docker rename` is therefore
invisible until the container is recreated — a documented gap in the original ADR's Alternatives
section and in `docs/known-gaps.md`. Worse, a metadata read that fails even once — the container
directory existing a moment before `config.v2.json` does, a torn read racing the daemon's own
atomic rewrite — permanently degrades that container to a `container.id`-only resource for the
life of the handle, with no retry.

**The watch set doesn't scale with the host, only with `root`'s existence.** `PathPattern::
watch_dirs` returns `root` plus every container subdirectory that currently exists, all under one
`inotify` mask that includes `IN_MODIFY`, and any wake — from any container, selected or not —
triggers a full `scan()`. On a host running many containers, a log line written by *any* container
wakes the driver and costs a readdir of `root`, a stat per container, a second readdir for `
watch_dirs` itself, and (via `DecoderFactory::accept`, which every not-yet-tracked path goes
through on every scan) an uncached read-and-parse of every unselected container's `config.v2.json`.
That is O(containers on the host) work per log line written anywhere on the host — not proportional
to what `docker_in` was actually configured to follow, and not proportional to time.

This ADR closes both. It deliberately reverses two things the original ADR decided: the "watch
every container subdirectory" wake-source design, and the "a `docker rename` handled by watching
for it explicitly" alternative, which that record considered and set aside as out of scope. Neither
reversal touches `config.v2.json`-as-metadata-source or the "no docker socket/API" decision, which
both stand.

## Decision

### Watch set: `root`, plus one watch per file actually being tailed — nothing else

`InotifyWatcher`'s single `WATCH_MASK` splits into `DIR_MASK` (`IN_CREATE | IN_MOVED_TO |
IN_MOVED_FROM | IN_DELETE | IN_DELETE_SELF` — appearance and departure, no content events) for
`root`, and `FILE_MASK` (`IN_MODIFY`) for each currently-open log file, registered at the moment
`Tailer::open_tracked` opens it and removed when that file closes for any reason. `PathPattern::
watch_dirs` for `docker_in`'s `Matcher::DockerContainers` collapses back to `vec![root]` — no more
per-scan readdir of `root` just to compute the watch set, and no more directory watch on a
container this listener was never asked to follow.

**Why `root` alone still catches container add/remove near-instantly.** Docker's per-container
state directories are direct children of `root` (`<root>/<id>/...`), so `docker create`/`docker
rm` fires `IN_CREATE`/`IN_DELETE` on `root`'s own watch — no per-container directory watch is
needed to notice a container arriving or leaving. What a directory watch bought beyond that was
noticing the *log file* appearing inside an already-existing container directory a moment later,
and noticing that container's `config.v2.json` change — both now handled below.

**A `Wake::Data` (a tailed file's own `IN_MODIFY`) never triggers a `scan`.** It runs a cheap
truncation check for the one file it names and falls through to the driver's existing per-iteration
drain. A `Wake::Discover` (something changed under `root` itself) or `Wake::Overflow` still runs a
full `scan`, exactly as before. This is what makes the cost of one log write O(1) rather than
O(containers) — the actual complaint this ADR exists to fix, and it holds identically whether
`docker_in` is configured with an explicit, short `containers:` list or `discover: true` (every
container on the host): watch count is `root` plus the number of files currently open, which is
proportional to what is running, never to how much any of them write.

**What moves to the poll tick instead of `inotify`.** A log file first appearing inside an
already-existing container directory, rotation, `config.v2.json` changing, and in-place truncation
are all no longer watched directly — they ride the existing `poll_interval` tick (1s default),
same as `watch: poll` always has. None of the four lose data doing this: a newly discovered file
still opens at its own beginning, and a rotated-away handle still drains to EOF before closing.
The cost is bounded latency (at most one `poll_interval`), not correctness, traded for not doing
per-write, per-container work host-wide.

**File watches are cleaned up on kernel invalidation, not only on explicit `unwatch`.** An
`inotify` file watch follows the inode; when that inode is deleted — Docker's own log-rotation
cleanup removing an old `.2`/`.3` file, or an operator deleting a log directly — the kernel emits
`IN_IGNORED` on that watch descriptor unconditionally, regardless of the requested mask. `
parse_events` treats this as "the watch is gone" and drops the bookkeeping entry; without it, a
long-running host under `discover: true` leaks one stale descriptor per rotated-or-deleted file
for the life of the process. This wasn't visible under directory watches, since a directory
normally outlives the files rotated inside it.

### Container identity refreshes on the poll tick, cached by `config.v2.json`'s own stat

`DockerDecoderFactory` gains a cache, keyed by container directory, holding the last successfully
parsed identity (name, image, labels — folded into the `Arc<Resource>` `docker_in` stamps onto
every event) and the `(dev, ino, len, mtime)` of the `config.v2.json` it came from. Both `accept`
(deciding whether an as-yet-untracked path is selected) and a new per-scan `refresh` (re-checking
an already-tracked file) read through this cache: a `stat` every tick, a `read` + `serde_json`
parse only when that stat has actually changed. This is what keeps the poll tick's cost at one
syscall per container rather than a full metadata read for every one of them, selected or not.

**A rebuilt resource is compared by value before being installed**, not swapped in on every stat
change. `config.v2.json` is rewritten by the daemon for reasons that have nothing to do with
identity (restart counts, healthcheck results) far more often than an operator actually renames a
container. Keeping the existing `Arc<Resource>` when the freshly-parsed one is equal is what keeps
`docker_in`'s downstream batching stable — see the next section for why an unnecessary swap isn't
just wasted work, it actively breaks batching.

**A failed read is cached as a failure and retried on the next stat change, not stuck for the life
of the handle.** The race of a container directory existing a moment before its `config.v2.json`
does, or a read landing mid-rewrite, now self-heals on the very next tick that sees the stat settle,
instead of permanently degrading that container to a `container.id`-only resource. The existing
`metadata_error` diagnostic still fires exactly once per failure — on the *transition* into
failure, not once per poll tick for as long as it persists, since `Diagnostics::warn_throttled`
counts every call even while it throttles the log line itself.

### Identity changes flush cleanly through the existing resource-change machinery

`docker_in`'s decoder already carries an `Arc<Resource>` per tracked file, and `BatchAccumulator::
absorb` already flushes whatever batch it's holding, tagged `FlushReason::ResourceChange`, the
moment an incoming `Arc` isn't pointer-equal to the one it's accumulating under. Refreshed identity
uses exactly this: the cache installs the new `Arc` on the decoder, and the very next decoded line
flushes the old batch intact before any line carries the new identity. No new `FlushReason`, no
new batch-boundary logic — the mechanism that already exists for "this listener's resource changed
mid-stream" is the correct mechanism for "this container was renamed mid-stream" too, and reusing
it is what makes the value-equality check above load-bearing rather than cosmetic: without it,
every unrelated `config.v2.json` rewrite would flush a batch that never needed to split.

### De-selection is symmetric with selection, and offsets survive it

A tracked container renamed so it no longer matches an explicit `containers:` list stops being
read, flushes whatever it's already accumulated, and closes — the same outcome a container that's
been `docker rm`'d gets, but reached by a different path, since the file itself hasn't gone
anywhere. It does *not* drain to EOF first: unlike removal, the container is still running and
still writing, so there is no EOF to reach, and every byte written after the rename belongs to a
name this listener was told not to follow. `Tailer`'s existing `resume` map — already "the offset
this inode should start from when next discovered," populated from the checkpoint file at startup
— retains the closed file's offset by inode, so a rename back into the selection resumes from
where it left off instead of replaying the whole log from byte 0. This retention is process-local:
it does not survive a `logit` restart, since the checkpoint file only reproduces entries for files
still being tracked. `discover: true` can never de-select anything, by construction — every
container matches.

## Alternatives considered

- **The docker socket/API, to get renames as first-class events instead of polling a stat.**
  Rejected for the same reasons the original ADR gave: broader privilege than a read-only bind
  mount, an API version to negotiate, a client dependency, for a decision this record shows doesn't
  need it — `config.v2.json`'s own stat, checked on the poll tick, already catches a rename within
  one `poll_interval`, and `root`'s own directory watch already catches add/remove immediately with
  no socket at all.
- **Keeping per-container directory watches, just narrowing their mask.** Considered, since it
  would restore near-instant discovery of a new log file, rotation, and identity changes.
  Rejected: the watch count itself is still O(containers), which is the more expensive resource on
  a large host (`fs.inotify.max_user_watches`/`max_user_instances` are finite, and each running
  container would cost one), and the latency this buys is bounded to one `poll_interval` — a
  parameter the operator already controls — either way.
- **Reusing `FileState::Draining` for a de-selected file instead of a new `Deselected` state.**
  Rejected: `Draining` means "read every remaining byte, then close," which is correct for an
  inode that's genuinely going away but wrong for a container still running under a name this
  listener no longer wants — it would keep a busy de-selected container emitting under its old
  identity indefinitely, the precise failure this decision exists to close.
- **A `Deselected`-specific `FlushReason` tag.** Rejected in favor of the existing `Closed` reason
  (already documented as covering "a single tracked file... is ending" for any reason short of the
  whole component shutting down) plus dedicated counters
  (`logit.input.files.deselected`/`.identity_changed`) — `FlushReason`'s variants are about batch
  identity and bounds, not about why a source ended, and the counters give an operator the more
  precise signal without growing that enum's purpose.

## Consequences

- `docs/adr/file-tailing-and-docker-json-logs.md`'s "Wake source" section and its "A `docker
  rename` handled by watching for it explicitly" alternative are both superseded by this record —
  marked inline there, not rewritten, per this repo's convention for a later ADR revising an
  earlier one.
- `docs/known-gaps.md`'s `docker_in`-never-notices-a-rename entry is replaced by what's actually
  still true: identity refresh is bounded by `poll_interval`, a rename-then-rename-back inside one
  tick is never observed at all, and de-selection's offset retention doesn't survive a restart.
- New `crates/logit-inputs/src/tail/driver.rs` state: `FileState::Deselected`, a `DecoderFactory::
  refresh`/`end_scan` pair (both defaulted, so `tail_in`'s own factory is untouched).
- New telemetry: `logit.input.watch.watches` (gauge), `logit.input.files.identity_changed`,
  `logit.input.files.deselected` (counters), and `container_renamed`/`container_deselected`
  diagnostics (`Diagnostics::info`, not `warn_throttled` — a rename is normal operation).
- No config surface changes. `poll_interval` is the refresh cadence for everything this record
  moves off `inotify`; `schema/logit.schema.json` is untouched.
- `docs/plans/docker-container-identity.md` tracks the workstreams landing this.
