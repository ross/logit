---
created: 2026-09-08
updated: 2026-09-08
---

# `file_out`: a rotating file sink, sharing `stdio_out`'s implementation

## Status
Accepted

## Context

`stdio_out`'s file target is opened once, in append mode, and held for the process's lifetime
(`crates/logit-outputs/src/stdio.rs`, `docs/known-gaps.md`): it grows without bound, and an
external log rotator that renames the file leaves `logit` writing into the unlinked inode until
restart. That's an accepted trade-off for a debugging/dev-loop sink, which is what `stdio_out`'s
own module doc says it is -- but it's not adequate for writing events to disk as a real
operational destination, which needs a bounded file, a rotation policy, and bounded retention.

An early version of this design added a `file_out` `ComponentKind` with its own parallel sink
implementation next to `stdio_out`'s. That duplicated the encode/short-circuit/write/flush/
count-bytes loop to express what is actually a subset relationship: `stdio_out`'s file target *is*
`file_out` with an empty rotation policy. It also named the shared sink `TextOutput`, baking a text
assumption into the wrong layer -- the destination half (open, append, count, rotate, retain,
flush) is byte-oriented and encoding-agnostic; the actual text assumption lives in
`EventDump::render` returning `String`, and `logit_proto::Encoder` (`&EventBatch -> Bytes`) already
exists as the seam that generalizes it, with `InfluxLineEncoder` as its only implementor before
this record.

## Decision

### One sink, generic over its encoder; rotation as a property of the destination

```rust
pub struct StreamOutput<E> {
    target: Target,          // Stdout | Stderr | File(FileTarget)
    encoder: E,
    telemetry: Telemetry,
    diagnostics: Diagnostics,
}
```

`stdio_out` and `file_out` are both a `StreamOutput<EventDump>`, differing only in `target`:
`stdio_out` never rotates (`RotatePolicy::never()`), `file_out` always carries a real policy.
`EventDump` (`crates/logit-outputs/src/stdio.rs`) now implements `logit_proto::Encoder`, joining
`InfluxLineEncoder` on the trait its own doc comment already claimed every output implemented. The
inherent `EventDump::encode(&self, ...) -> String` is renamed `render` so it doesn't silently keep
resolving over the new trait method of the same name. `render`'s two existing contracts (never
fails, no scratch buffers, `&self` suffices) are unchanged; the trait impl simply wraps it and
always returns `Ok`.

The generic is monomorphized -- no new indirection. Both kinds still box their sink as
`dyn Output + Send` at the `NodeSpec::Output` boundary, exactly as before. A future binary or
NDJSON encoder plugs into the same sink with no change to the destination half; none is built here
(see "Alternatives considered").

### `Target::File` is always a `FileTarget`; `RotatePolicy::never()` is the unrotated case

```rust
enum Target {
    Stdout(io::Stdout),
    Stderr(io::Stderr),
    File(FileTarget),   // policy may be RotatePolicy::never()
}
```

There is exactly one place a file is opened and written, for both kinds. `StreamOutput::open_path`
(`stdio_out`) and `StreamOutput::rotating` (`file_out`) both go through `FileTarget::open`, which
reproduces the original `open_path`'s behavior exactly: sync `OpenOptions::new().create(true).
append(true)`, wrapped via `tokio::fs::File::from_std`, called eagerly from `build_spec` so a bad
path or permissions error is a config error that fails before anything starts listening.

### Rotation state is pure and separated from the file handle

`crates/logit-outputs/src/file.rs`'s `RotationState` holds only `written`/`period`/`policy` -- no
file handle, no path. `FileTarget` composes one alongside its `tokio::fs::File`. This is what makes
every trigger decision (`RotationState::should_rotate`) a plain synchronous unit test: the clock
(`now_unix: i64`, Unix seconds UTC) is an injected argument, mirroring
`logit_pipeline::Transform::flush(now)`'s precedent, rather than a real clock the test has to race.

`should_rotate` is true when either:
- **`interval`** is set and `now_unix` falls in a different calendar period (`now_unix.div_euclid
  (period_seconds)`) than the one the active file was last written in, or
- **`max_bytes`** is set and `written > 0 && written + incoming > max_bytes`.

Two consequences, both deliberate:

- **`max_bytes` is a threshold, not a hard cap.** A batch larger than `max_bytes` is written whole
  into its own file rather than split -- tearing an event block across two files would produce a
  file no reader can parse, which is strictly worse than one oversized file. The `written > 0`
  guard is what makes this work: an empty active file never rotates before its first (possibly
  oversized) batch lands.
- **Time rotation is write-triggered, not boundary-triggered.** `Output` gets no periodic tick --
  `write_loop`'s `select!` (`crates/logit-pipeline/src/runtime.rs`) has only a queue-peek arm and a
  shutdown-grace arm. An idle target rolls on its *next* write after a calendar boundary, not at
  it. The rolled file still holds exactly the previous period's events, so this delays when a file
  appears, not what ends up in it.

`period` starts `None` and is only set on the first `note_written` call after open or rotation
*unless* `FileTarget::open` can seed it from an existing file's mtime (see below), so a freshly
opened, genuinely empty file's very first batch never spuriously rotates just because no period was
known yet. `written` is seeded from the existing file's length at `open` time, not `0` --
restarting against an already-large file must not get another full `max_bytes` for free.

Calendar periods (`Hourly`/`Daily`), not a `Duration`: a duration measured from an arbitrary start
(process start, first write) drifts against the wall clock, which is the opposite of what a daily
log file is for. UTC only, never the host's local zone, matching the reasoning already recorded for
`syslog_in`'s timestamp resolution (`docs/known-gaps.md`).

**`period` is seeded from an existing file's mtime too, not just learned fresh after a restart.**
`FileTarget::open` already seeded `written` from the file's length; `RotationState::seed_period`
does the same for `period`, from the file's last-modified time, when `interval` is set and
`written > 0` (an empty file has no previous period's events to protect, mirroring
`should_rotate`'s own guard). Without this, a restart under an `interval` policy would forget which
calendar period the active file was last written in and either merge two periods' events into the
same file (breaking "the rolled file still holds exactly the previous period's events" across a
restart) or never notice a boundary already crossed while the process was down until the *next*
one. The residual: an unreadable mtime (a failed `metadata()` call) or a backwards clock jump across
the restart both fall back to the pre-seeding behavior -- `period` stays `None` until the first
write sets it fresh, exactly as before this existed.

### Retention: logrotate's own numbered-suffix cascade, commit-point first

`rotate()` is **commit-point first**: the active file is renamed to a transient staging path
(`path.rotating`) *before* anything retained is touched, not after the cascade the way an
interrupted middle step could previously have left retained files shifted with no active file
renamed in to replace them. Order:

1. Flush the active handle, if any.
2. `max_files == 1`: no room to keep any rotated file at all, so "rotating" means truncating the
   active file in place rather than ever creating a `.1` -- no staging path, no cascade, done here.
3. Otherwise, promote any staging file orphaned by a process killed mid-rotation on a *previous*
   run (see below), before this rotation stages a new one.
4. **Commit point:** rename the active file to `path.rotating`.
5. Drop the handle and reset rotation state.
6. Re-open `path` fresh.
7. Promote the just-staged file to `.1`: drop `path.{max_files-1}` if present, shift every
   remaining `path.{n}` up to `path.{n+1}` (oldest first, so no rename overwrites a file not yet
   moved), then rename `path.rotating` to `path.1` -- unconditionally, regardless of whether step 6
   succeeded, so the previous period's events reach `.1` either way.

`.1` is always the most recent rotated file -- no timestamped naming is available this pass (see
Alternatives). `max_files` counts *every* file the sink maintains, active plus rotated, so
`max_files * max_bytes` reads directly as a disk budget.

**`path.rotating` is transient, and an orphan is self-healing.** It exists only between step 4's
rename and step 7's promotion of that same rotation -- a process killed in that narrow window
leaves it behind, and the very next rotation's step 3 recovers it before doing anything else, so no
rotation is ever lost, just delayed by however long the process stayed down. Like a `.1`-suffixed
file, `path.rotating` is never matched by `tail_in`'s anchored wildcard (ADR
`file-tailing-and-docker-json-logs`, "Rotation and truncation") -- a `.rotating` suffix is just as
much a non-match as a numeric one, so a `tail_in` on `*.log` never mistakes a staging file for a
rotated one worth reading. Numbered suffixes get the same treatment for the same reason -- a
`tail_in` on `*.log` follows a `file_out`-fed active file across rotation and never re-reads a
rotated one.

**Failure policy, deliberately asymmetric** -- this is why `StreamOutput` now carries `Diagnostics`,
which `StdioOutput` previously did not need:

| Failure | Handling |
|---|---|
| Flushing the active file before rotating | Fatal -- bubbles as `Err` |
| Renaming the active file to its staging path (or truncating under `max_files: 1`) | `rotate_failure`, `RotateOutcome::NotRotated` -- nothing on disk touched, rotation state left unchanged so the next write retries safely |
| Re-opening `path` after a committed rename (or truncate) | `Err`, classified `Fault::Clean` -- the active handle is dropped and rotation state reset regardless, and a later write self-heals via a lazy re-open |
| Cascading/promoting a *retained* file | `retention_failure`, continue |

A retention-cascade/promotion failure only risks losing history, not correctness, so it's reported
(throttled) and skipped. A failure renaming the active file to its staging path means rotation
didn't happen at all -- the safest response is to keep the existing, already-flushed handle open
and retry on the next write (the throttled diagnostic bounds how often this actually prints), not
to fail the whole sink over what might be a transient permissions issue; `RotateOutcome::NotRotated`
is how `StreamOutput::send` knows not to count `logit.output.file.rotations` for a rotation that
didn't actually happen.

**Why a failed re-open is `Fault::Clean`, not left to default to `Permanent`.** `FileTarget.file` is
`Option<tokio::fs::File>`, `None` only in the narrow window between the commit-point rename and a
successful re-open -- never a handle to an already-rotated-away file, since the handle is dropped at
exactly that rename, not discovered stale later. A failed re-open therefore means the batch that
triggered rotation provably never reached any file: safe to retry under either delivery posture.
Per `logit_pipeline::output`'s `is_explicitly_permanent` doc comment, only an *explicit*
`Fault::Permanent` classification should ever count toward `write_loop`'s sustained-permanent-
failure exit window -- a transient re-open failure (most likely ENOSPC/EMFILE-class, since the
directory itself was just proven writable by the rename that preceded it) is a very different
situation from a real configuration error, and must never be mistaken for one just because it
wasn't explicitly classified `Clean`. `FileTarget::write_all`/`FileTarget::flush` (replacing the old
`file_mut()` accessor) lazily re-open the handle on the next write via the same `ensure_open` path,
so the sink self-heals with no rotation ever needing to succeed synchronously. `stdio_out`/an
unrotated `file_out` target never exercises any of these keys (`should_rotate` never fires), so
attaching `Diagnostics` remains additive, not a behavior change, for the target this module was
originally built around.

**Testing the re-open failure needs an injection seam, not a filesystem trick.** Once `path` has
been renamed away by the commit-point rename, a `create` at that now-freed name in a writable
directory can't be made to fail through any path or permission trick available to a test -- the
real failure mode there is ENOSPC/EMFILE-class, which a unit test can't induce on demand either.
`FileTarget::rotate` delegates to a private `rotate_inner` parameterized over a plain
`fn(&Path, bool) -> std::io::Result<std::fs::File>` opener (a plain `fn` pointer, not `impl Fn`, so
the returned future stays `Send`); a `#[cfg(test)]`-only `rotate_with` exposes that seam so tests
can inject a failing opener, while `rotate` itself always passes the real `open_active`.

### Config: a sibling `ComponentKind`, not a `rotate:` block on `stdio_out`

```yaml
- id: disk_out
  type: file_out
  sources: [enrich]
  path: /var/log/logit/events.log   # required
  rotate:
    max_bytes: "64MiB"              # optional, human_bytes codec
    interval: daily                 # optional: hourly | daily
    max_files: 5                    # default 5
```

`path` resolves against the config file's own directory when relative, exactly like
`StdioTarget::Path`. `rotate: RotateConfig` defaults every field; graph validation (rule 29,
`crates/logit-pipeline/src/graph.rs`) rejects a `file_out` whose `rotate:` sets neither
`max_bytes` nor `interval` -- that would silently never rotate at all, and `stdio_out` already
covers "no rotation" on purpose, so this is a config error rather than a quiet no-op. Rule 29 also
rejects `rotate.max_bytes: "0"` (every batch would rotate) and `rotate.max_files: 0` (would delete
the file it just rotated) -- the same "0 is impossible, not just small" instinct as the existing
tail/receive rules.

Format is unchanged from `stdio_out`'s human-readable `EventDump` render; no `format:` field lands
in this pass (see Alternatives). Retention is `max_files` alone -- no `max_age`, no compression.

## Alternatives considered

- **A `rotate:` block added directly to `stdio_out`, no new kind.** Rejected: couples an
  operational feature to a documented debug sink, and needs a graph rule rejecting `rotate:` under
  `target: stdout`/`stderr`. A sibling kind keeps `stdio_out`'s config surface exactly as small as
  its own doc comment says it is.
- **`log4rs` or `simplelog` for the rotation engine.** Both are `log`-facade backends whose unit of
  work is a `log::Record` -- a formatted message string -- structurally unable to carry an
  `Event`'s independent log/metric/span payloads, and unable to carry a binary encoding at all even
  in principle. Both are also blocking `std::io` against an async `Output::send`, and sit outside
  this codebase's `Fault`/`Diagnostics`/`Telemetry` plumbing. Neither is in `Cargo.lock` today, and
  `deny.toml`'s strict license allowlist already cost the `notify` crate its place in the tailing
  work (ADR `file-tailing-and-docker-json-logs`) -- the same bar a new dependency has to clear here.
- **`tracing-appender`'s `RollingFileAppender`.** Closer -- a plain `io::Write`, no facade -- but
  time-based only, so `max_bytes` would still be hand-rolled on top, and it doesn't share this
  codebase's config/telemetry/diagnostics conventions. Hand-rolling the whole (small) rotation
  engine matches this repo's standing pattern (hand-rolled `inotify` over `notify`, hand-rolled
  gRPC over `tonic`, hand-rolled duration/byte-size serde) more closely than pulling in a crate for
  half the feature.
- **A binary/NDJSON encoder alongside this change.** Out of scope for this pass: `EventDump`
  joining `logit_proto::Encoder` is the seam a future encoder would plug into, but the native wire
  encoding is an open, benchmark-gated decision (`docs/design/wire-protocol.md`) not to be settled
  in passing, and `logit_out` is endpoint-based today, not file-based. Tracked in
  `docs/known-gaps.md`.
- **Timestamped rotated-file names (`events-2026-09-07.log`) instead of numbered suffixes.**
  Rejected for this pass: numbered suffixes are what let one retention rule (`max_files`) cover
  both triggers uniformly, and match `tail_in`'s existing "a `.1`-suffixed file is never matched by
  an anchored wildcard" precedent exactly. A timestamped scheme is a reasonable future addition,
  tracked in `docs/known-gaps.md`.
- **`max_age`-based retention, or compressing rotated files.** Both deferred: a count alone is the
  single knob most operators reach for first, and gzip compression would add background CPU work
  inside (or spawned from) a sink's write path for a feature an external tool already does well.
  Tracked in `docs/known-gaps.md`.
- **SIGHUP/external-rotator reopen.** Not addressed here -- `file_out` still holds its file handle
  for the process's lifetime between its own rotations, same as `stdio_out` always has. An external
  tool rotating a `file_out`-managed file out from under it remains the same known gap `stdio_out`
  already documents.

## Consequences

- `StdioOutput` (`crates/logit-outputs/src/stdio.rs`) is renamed `StreamOutput<E>`; `Sink` is
  renamed `Target`, with `File` now carrying a `FileTarget` instead of a bare `tokio::fs::File`.
  Every call site (`crates/logit-cli/src/pipeline.rs`, and doc-comment mentions in
  `logit-pipeline::output`, `logit-outputs::otlp`/`syslog`) is updated to the new name.
  `EventDump::encode` is renamed `render`; `EventDump` gains `impl logit_proto::Encoder`.
- New `crates/logit-outputs/src/file.rs`: `FileTarget`, `RotationState`, `RotatePolicy`,
  `RotateInterval`, and their tests -- the rotation-trigger tests are pure/synchronous, the
  rotate/retention tests use real scratch-directory files (`std::env::temp_dir()`-based, no
  `tempfile` dependency, matching `crates/logit-inputs/src/tail/mod.rs`'s precedent).
- New `ComponentKind::FileOut`, `RotateConfig`, `RotateInterval` (`crates/logit-config/src/lib.rs`).
- `crates/logit-pipeline/src/graph.rs`: `FileOut` added to `role`/`kind_name`/`is_implemented`, new
  rule 29.
- `crates/logit-cli/src/pipeline.rs`: a `FileOut` arm in `build_spec`, `to_rotate_policy`/
  `to_rotate_interval` converters (the `logit-outputs`-doesn't-depend-on-`logit-config` crate-layout
  rule, same reasoning as `overflow_policy`/`syslog_format`).
- `crates/logit-bench/tests/allocations.rs`'s `stdio_encode_100_events` now measures through
  `Encoder::encode` (what production calls) rather than the inherent `render` directly; the
  allocation count is unchanged (`Bytes::from(String)` reuses the `String`'s existing heap buffer,
  it doesn't copy).
- `schema/logit.schema.json` regenerated (`script/schema`) for `FileOut`/`RotateConfig`/
  `RotateInterval`.
- `docs/known-gaps.md`'s `stdio_out` rotation entry narrows to what's still true (`stdio_out` itself
  still has no reopen, no user-controlled format); new entries record what `file_out` still doesn't
  do (no SIGHUP/external-rotator reopen, no compression, no `max_age`, no timestamped naming,
  write- not boundary-triggered time rotation, no `format:`).
- **Rotation is now commit-point first** (this record's own correction, landing in the same PR as
  everything above): a new public `RotateOutcome` (`Rotated`/`NotRotated`) is `FileTarget::rotate`'s
  return type, replacing the old bare `Ok(())`; `StreamOutput::send` only counts
  `logit.output.file.rotations` on `RotateOutcome::Rotated`. `FileTarget.file` becomes
  `Option<tokio::fs::File>`, `None` only between the commit-point rename and a successful re-open;
  the old `pub fn file_mut` accessor is gone, replaced by `FileTarget::write_all`/
  `FileTarget::flush`, which lazily re-open the handle via a private `ensure_open` when it's `None`.
  A new private `FileTarget::staging_path` (`path.rotating`) and `FileTarget::promote_staged` (the
  cascade-then-promote logic, now shared between recovering an orphan and promoting a fresh
  rotation) replace the old inline single-pass cascade. The old `async fn reopen` (and its now-
  inaccurate "failure here is fatal" doc claim) is gone, inlined into `rotate_inner`'s two re-open
  sites. A new `#[cfg(test)]` `rotate_with` seam (a private `rotate_inner` parameterized over a
  plain `fn(&Path, bool) -> std::io::Result<std::fs::File>` opener) lets tests inject a failing
  re-open, since no real filesystem trick can make one fail once `path` has already been renamed
  away. `RotationState::seed_period` and the `unix_seconds`/`now_unix` mtime-conversion path are
  new; `FileTarget::open` now calls both `open_active` (also new, shared with `rotate_inner`'s two
  re-open sites) and `seed_period`.
