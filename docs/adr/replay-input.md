---
created: 2026-10-19
updated: 2026-10-19
---

# `replay_in`: a finite input that reads a `format: json` or `format: native` file once and ends

## Status
Accepted

## Context

`stdio_out` and `file_out` write archives in two machine-readable forms: `format: json`, one
event per line ([ADR `stream-json-format`](stream-json-format.md)), and `format: native`, one
bare-batch frame per batch ([ADR `file-output-native-format`](file-output-native-format.md)).
Nothing in `logit` reads either back. `logit-perf`'s `decode_dump` reads a native dump for the
load-test harness, and the `format: json` readers are Python scripts outside the pipeline.

The json form became lossless in the stream-json-format ADR's 2026-10-19 amendment, with
`logit_proto::event_json` as its reader. That makes three uses possible, in order of how often an
operator meets them:

1. Replaying an archive `file_out` wrote, into the same pipeline or a different one.
2. Injecting hand-written events into a pipeline, to try a config or reproduce a report.
3. Driving the transit spec's flows ([ADR `transit-spec`](transit-spec.md)), whose cases are
   authored as lossless event JSON and need a source that feeds them to real components.

Each needs a source that reads a finished file once and stops. `generate_in` under `count` is the
one finite input today.

## Decision

**A new input kind, `replay_in`, reads one file from start to end, sends its events downstream,
and returns `Ok(())`.** It's `crates/logit-inputs/src/replay.rs`.

```yaml
replay:
  type: replay_in
  path: archive/events.json   # relative to the config file
  format: json                # json (the default) | native
  batch: 1000                 # the most events in one batch (the default)
```

1. **Config.** `path:` resolves against the config file's directory, as `lua_file` and a
   `stdio_out` path target do. `format:` is `json` or `native`, `json` by default because it's the
   form a person writes by hand. `batch:` bounds the events in one batch, `1000` by default, the
   socket drivers' `batch_max_events` default. Graph rule 93 rejects an empty `path` and
   `batch: 0`.
2. **Timestamps are the file's.** Nothing is re-stamped, and no receive time, peer, or sender
   attribute is added. There's no `peer:`: a file has no sender, and what produced it is whatever
   its resource says.
3. **Batching.** Under `json`, consecutive lines with equal resource and scope form one batch, the
   runs `EventJsonDecoder` already shares one `Arc` across, cut at `batch` events. Under `native`,
   one frame is one batch as the sink wrote it, cut at `batch` events only when a frame holds
   more. A batch boundary in the file is the writer's batch boundary in both forms, as far as
   `batch` allows, so a replay reproduces the batch shape downstream components saw.
4. **Finite.** At the end of the file `run` returns `Ok(())`, dropping its `Fanout`. This is the
   path `generate_in` takes at its `count`:
   - `run_input` (`crates/logit-pipeline/src/runtime.rs`) returns the input's `Ok`, and
     `run_with_telemetry`'s join loop marks the node `Finished` and keeps joining. Only an `Err`
     triggers shutdown.
   - Dropping the `Fanout` drops the input's senders. Each downstream inbox closes once every
     sender into it is gone, and a node whose inbox closes flushes and exits, so the close
     cascades to the sinks, each of which drains its queue before it exits.
   - The process exits when the join loop has no task left, with exit code 0.
   - With two inputs, the other input keeps running, and the process with it. A node fed by both
     keeps its inbox open until both have ended. A node fed only by `replay_in` flushes and
     finishes as soon as the file ends.
   - An `internal` input never ends on its own: a config with `internal` beside `replay_in` runs
     until a signal, which is what a replay that should also report its telemetry wants.
5. **Bind.** `Input::bind` opens the file in the startup pre-pass, so a missing or unreadable
   file, or a directory, fails `logit run` with exit code 1 before any task starts. `logit
   validate` doesn't open it.
6. **Loss.** An unreadable unit is skipped, counted, and diagnosed, and the rest of the file is
   still read. Every count is `logit.input.frames.dropped{reason}`, the name the line and frame
   drivers already use ([ADR `telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md)):
   - A JSON line `EventJsonDecoder` refuses is `undecodable`, the reason the stream driver uses
     for a frame its decoder rejected whole. Its `bad_line` diagnostic names the line number.
   - A native frame is `magic`, `version`, `too_large`, `crc`, `codec`, `decode_budget`, or
     `malformed`, the reasons `logit_in` counts under `logit.proto.errors`. A `CODEC_HOP_BATCH`
     frame, the disk spool's shape and not a file's, is `codec`.
   - A frame whose declared length runs past the end of the file is `truncated`.
   - A JSON line cut short fails to parse and is `undecodable`.

   A frame's declared length isn't trusted until its CRC passes. `file_out` opens its file to
   append, so a writer killed mid-frame leaves a torn frame with the next run's whole frames
   behind it, and the torn frame's length reaches into them. After a header fault, a CRC fault,
   or a `truncated` frame, the read resumes at the next frame magic past the faulty frame's own,
   not past its length. Only a `truncated` frame with no magic behind it ends the read, as
   `decode_dump` tolerates a torn final frame. A false magic inside a payload costs one more CRC
   check. A frame that passes its CRC and then fails, a body that doesn't decompress included, is
   counted under its own reason and skipped by its length. One fault is counted per unreadable
   run, from a counted fault until a frame passes its CRC, except that a frame or header the end
   of the file cuts short, magic included, is counted `truncated` once.

   The module doc's table is the canonical list.
7. **A batch no consumer took** is counted `logit.input.batches.dropped{reason="closed_consumer"}`
   and ends the read, as `tail_in` does.

## Alternatives considered

- **A `format:` on `tail_in`.** `tail_in` follows a growing file, survives rotation, and
  checkpoints its offset so a restart resumes. A replay reads a finished file once, and none of
  that machinery applies; a "read once, then stop" mode would add a second lifecycle to the most
  intricate input in the tree. `tail_in` also stamps read time, the opposite of decision 2.
- **Naming it `file_in`.** It would sit beside `tail_in` as a confusable sibling: both read files,
  and the name says nothing about which one follows. `replay_in` names what it's for.
- **Extending `generate_in`.** `generate_in` renders a template for load tests, with rate pacing
  and placeholders. A file source shares only "finite" with it.
- **`logit.input.lines.dropped{reason="malformed"}` for a refused JSON line.** No such name
  exists; the line-framed listeners already count a lost line under `logit.input.frames.dropped`,
  and `undecodable` is the existing reason for a unit its decoder refused. Reusing both keeps one
  name to alert on.
- **Grouping JSON lines by `batch:` alone.** A batch carries one resource and scope, so lines
  under different resources can't share one, and the file's own runs are the batches its writer
  sent.

## Consequences

- An archive `file_out` writes in `format: json` or `format: native` can be replayed, and the
  transit spec's flows have a source.
- A `format: native` archive written with `compression: lz4` is read, since the compression is
  inside each frame. No other compressed file is: a gzipped rotation and lz4-framed JSON aren't
  read, and `file_out` writes neither ([known gaps](../known-gaps/tailing.md)).
- There's no follow mode, no directory or glob, no rate pacing, and no checkpoint. A restart reads
  the file from the start, and events go out as fast as downstream takes them. Each is an entry in
  `docs/known-gaps/tailing.md`.
- Replayed events carry the file's timestamps, so an `aggregate` downstream windows them by wall
  clock, not by their timestamps, and a `drop_stale` drops an old archive's events.
