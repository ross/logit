---
created: 2026-09-17
updated: 2026-09-17
---

# Enabling plan: `docker_in` live container identity and a minimal `inotify` watch set

## Context

[ADR `docker-container-identity-and-minimal-watches`](../adr/docker-container-identity-and-minimal-watches.md)
decides the shape: `docker_in` refreshes container identity from `config.v2.json`'s own stat on
every poll tick instead of freezing it at open, and holds an `inotify` watch set of `root` plus
only the files it actually has open, instead of every container subdirectory under `root`. This
plan is the build-out: what lands in which order, in which files, and how it is verified. Read the
ADR first — this document doesn't repeat its reasoning, only its consequences.

Stream key **`dkr`**: branches `dkr/w0`…`dkr/w4`, a strictly linear stack, each PR based on and
targeting its parent's branch, brought up to date with `git merge origin/main` (never rebase).

## Decisions already settled

| Question | Decision |
|---|---|
| Watch set | `root` (`IN_CREATE`/`IN_MOVED_TO`/`IN_MOVED_FROM`/`IN_DELETE`/`IN_DELETE_SELF`) plus one `IN_MODIFY` watch per currently-open tailed file — no per-container directory watches |
| A data wake | Never triggers a `scan`; runs a truncation check for the one file it names, then falls through to the existing per-iteration drain |
| Discovery / rotation / config changes / truncation | All ride the existing `poll_interval` tick (1s default), not `inotify` |
| Metadata cache | Keyed by container directory, keyed on `config.v2.json`'s `(dev, ino, len, mtime)`; a stat every tick, a read+parse only on a stat change |
| Resource swap | Compared by value before installing — an unrelated `config.v2.json` rewrite (restart counts, healthcheck results) must not force a flush |
| Failed metadata read | Cached as a failure, retried on the next stat change; diagnosed once on the transition into failure, not once per tick |
| Identity change flush | Reuses existing `BatchAccumulator::absorb` / `FlushReason::ResourceChange` — no new `FlushReason` variant |
| De-selection state | New `FileState::Deselected`, not `Draining` — stops reading immediately rather than draining to EOF, since the container is still running |
| De-selection offset retention | Reuses the existing `resume: HashMap<FileId, (PathBuf, u64)>` map — no new field; process-local, does not survive a `logit` restart |
| `discover: true` | Can never de-select (selection always matches); watch/refresh cost scales with containers currently running, not host-wide write volume |
| Docker socket/API | Out of scope, unchanged — `config.v2.json` remains the only metadata source |
| Config surface | No new fields; `poll_interval` is the refresh cadence. `schema/logit.schema.json` is untouched |
| Landing | PR stack only. Nothing merged by this workstream; Ross directs merging |

## Workstreams

### `dkr/w0` — ADR and plan (this PR)

- New ADR `docs/adr/docker-container-identity-and-minimal-watches.md`.
- `docs/adr/file-tailing-and-docker-json-logs.md`: inline "Superseded (2026-09-17)" notes in its
  "Wake source" section and its "A `docker rename` handled by watching for it explicitly"
  alternative, both forward-linking to the new ADR; `updated` frontmatter bumped.
- `docs/adr/README.md`, this plan's own entry in `docs/plans/README.md`.

### `dkr/w1` — the watch set: `root` plus the files actually being tailed

- `crates/logit-inputs/src/tail/watch.rs`: split `WATCH_MASK` into `DIR_MASK`/`FILE_MASK`;
  `InotifyWatcher` gains `watch_file`/`unwatch` alongside `watch_dir`/`unwatch_dir`, keyed by watch
  descriptor; `Wake` becomes `Discover(PathBuf) | Data(PathBuf) | Overflow`, with bursts of
  `IN_MODIFY` for one file collapsed to a single pending wake; `IN_IGNORED` (the kernel's
  unconditional signal that a watched inode is gone — rotation cleanup, a direct delete) clears the
  watch's own bookkeeping rather than leaking it.
- `crates/logit-inputs/src/tail/pattern.rs`: `watch_dirs()` for `Matcher::DockerContainers` drops
  its `read_dir` over `root` and returns `vec![self.dir]`, like every other matcher.
- `crates/logit-inputs/src/tail/driver.rs`: `TrackedFile` gains a watch handle, registered at open
  and removed at close; the `select!` match routes `Wake::Data` to a truncation-only reconcile
  (extracted from `scan`'s inline branch into a shared `reconcile_truncation`) with no `scan` call,
  while `Discover`/`Overflow` still `scan` as today; new `logit.input.watch.watches` gauge.
- `tail_in` inherits this for free (a directory with many files no longer wakes on writes to ones
  it isn't tailing).
- Tests: mask/kind unit tests in `watch.rs`; driver tests that a write to an unwatched sibling file
  produces no scan, that a tracked file's write still delivers promptly, that truncation is still
  caught via the file's own wake, and that rotation moves the watch to the new inode. The existing
  `under_inotify_a_container_log_created_after_its_directory_is_discovered_before_the_poll_interval`
  (`docker.rs`, added for the old per-container-directory-watch design) is replaced with a test
  pinning the new contract: such discovery is now poll-bound by design. Its sibling
  `under_inotify_a_truncated_container_log_is_noticed_before_the_poll_interval` is expected to keep
  passing unchanged, since `O_TRUNC` fires `IN_MODIFY` on the file's own new watch — only its doc
  comment needs rewriting.

### `dkr/w2` — live container identity

- `crates/logit-inputs/src/docker.rs`: a metadata cache on `DockerDecoderFactory`; `accept` and a
  new `DecoderFactory::refresh`/`end_scan` pair (both defaulted no-ops, so `tail_in`'s
  `LineDecoderFactory` is untouched) read through it. `refresh` takes `&mut D` and installs a
  refreshed `Arc<Resource>` on the decoder directly rather than handing one back, keeping
  `TailDecoder` itself untouched.
- Value-equality check before installing a rebuilt resource (see Decisions table).
- Failed-read caching and retry-on-stat-change (see Decisions table); `metadata_error` diagnosed
  once per failure transition, not once per tick.
- New counter `logit.input.files.identity_changed`; `container_renamed` diagnostic via
  `Diagnostics::info`.
- Tests: a rewritten `config.v2.json` changes `container.name` on subsequent events, not
  already-emitted ones, with the batch boundary landing as `FlushReason::ResourceChange`; a
  metadata read that fails then succeeds recovers the full resource; a stat-preserving no-op
  rewrite produces no new `Arc` (`Arc::ptr_eq` across lines); `accept` does not re-parse an
  unchanged config across two scans.

### `dkr/w3` — selection follows the rename

- `crates/logit-inputs/src/tail/driver.rs`: `FileState::Deselected`; `read_one` returns `false`
  for it immediately; `reap_drained`'s predicate widens to close it the same loop iteration;
  `open_tracked`'s rebind branch gets a guard so a `Deselected` entry is never silently revived
  before `reap_drained` removes it; `reap_drained` inserts the closed file's offset into the
  existing `resume` map (only for `Deselected`, never `Draining`, whose inode may be recycled).
- New counter `logit.input.files.deselected`; `container_deselected` diagnostic via
  `Diagnostics::info`.
- Tests: a de-selected container stops emitting immediately even while still being written to; a
  de-selected-then-re-selected inode resumes at the retained offset rather than replaying from
  byte 0; a de-selected file is not revived by `open_tracked` before it's reaped.

### `dkr/w4` — docs, and live verification against a real stack

- `docs/known-gaps.md`: the `docker_in` rename gap replaced with what's still true (bounded by
  `poll_interval`; retention doesn't survive a restart).
- `docs/deploying.md`'s "Tailing files and Docker logs" section: what's watched vs. poll-bound;
  `metadata_error` now retries.
- `docs/design/internal-telemetry.md`: the new counters and diagnostic keys.
- Live run against `script/demo up --build`: `docker rename` on the demo's redis container,
  confirm the leg drains and stops, rename back and confirm resume with no duplicate lines;
  `docker restart`/recreate on nginx, confirm continuity and pickup by name; record the
  watch-count and wake-rate before/after (`logit.input.watch.watches`) as the concrete proof of
  W1's goal.

## Verification

Per PR, `script/cibuild` green (format, clippy `-D warnings`, nextest, `script/validate`, schema
drift, audit). End to end, W4's live demo run is the real proof, matching the precedent
`docs/plans/file-tailing.md`'s own workstream D set.
