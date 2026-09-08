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

`period` starts `None` and is only set on the first `note_written` call after open or rotation, so
a freshly opened file's very first batch never spuriously rotates just because no period was known
yet. `written` is seeded from the existing file's length at `open` time, not `0` -- restarting
against an already-large file must not get another full `max_bytes` for free.

Calendar periods (`Hourly`/`Daily`), not a `Duration`: a duration measured from an arbitrary start
(process start, first write) drifts against the wall clock, which is the opposite of what a daily
log file is for. UTC only, never the host's local zone, matching the reasoning already recorded for
`syslog_in`'s timestamp resolution (`docs/known-gaps.md`).

### Retention: logrotate's own numbered-suffix cascade

`rotate()`: drop `path.{max_files-1}` if present, shift every remaining `path.{n}` up to
`path.{n+1}` (oldest first, so no rename overwrites a file not yet moved), rename the active file to
`path.1`, then open a fresh active file. `.1` is always the most recent rotated file -- no
timestamped naming is available this pass (see Alternatives). `max_files` counts *every* file the
sink maintains, active plus rotated, so `max_files * max_bytes` reads directly as a disk budget.
`max_files: 1` is a special case with no room to keep any rotated file at all: rotating truncates
the active file in place rather than ever creating a `.1`.

Numbered suffixes are also exactly what `tail_in`'s anchored wildcard already refuses to match (ADR
`file-tailing-and-docker-json-logs`, "Rotation and truncation") -- a `tail_in` on `*.log` follows a
`file_out`-fed active file across rotation and never re-reads a rotated one.

**Failure policy, deliberately asymmetric** -- this is why `StreamOutput` now carries `Diagnostics`,
which `StdioOutput` previously did not need:

| Failure | Handling |
|---|---|
| Flushing the active file before rotating | Fatal -- bubbles as `Err` |
| Deleting/renaming a *retained* file (the cascade) | `retention_failure`, continue |
| Renaming the *active* file to `.1` | `rotate_failure`, keep writing the current file |
| Re-opening `path` after a successful rename | Fatal -- there is no file left to write to |

A retention-cascade failure only risks losing history, not correctness, so it's reported (throttled)
and skipped. A failure renaming the active file itself means rotation didn't happen at all -- the
safest response is to keep the existing, already-flushed handle open and retry on the next write
(the throttled diagnostic bounds how often this actually prints), not to fail the whole sink over
what might be a transient permissions issue. `stdio_out`/an unrotated `file_out` target never
exercises either key, so attaching `Diagnostics` is additive, not a behavior change, for the target
this module was originally built around.

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
