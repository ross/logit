---
created: 2026-09-28
updated: 2026-10-02
---

# `stdio_out`/`file_out`'s human render: an exhaustive, sectioned block per event

## Status
Accepted

## Context

`stdio_out` is the sink for seeing what a pipeline carries without a backend, and `file_out`
writes the same text under its default `format: human`
([`rotating-file-output`](rotating-file-output.md),
[`file-output-native-format`](file-output-native-format.md)). The render they shared showed one
line per section, terse enough to tail: a timestamp with the log message folded onto it, a merged
attribute line, one line per metric, and a span line.

That render dropped a good part of the model in [`docs/design/data-model.md`](../design/data-model.md):
the batch `Scope` entirely; a `Resource`'s identity, merged invisibly into the event's attributes;
a log record's `body_format`, `event_name`, `observed_timestamp`, and dropped-attribute count; a
metric's `description`, `start_timestamp`, exemplars, and every flag bit but one; a histogram's
temporality and an exponential histogram's buckets; and a span's `flags`, status message, trace
state, and dropped counts. A `Distribution` was three fixed quantiles. Every one of those is a
field a decoder went to the trouble of keeping, and a debug sink that hides it sends a maintainer
to a packet capture to learn what `logit` holds.

Nothing parses the human text any more: [`stream-json-format`](stream-json-format.md) moved
every out-of-CI reader onto `format: json`, so the render can change for a person's benefit
alone.

## Decision

The human render is a **block per event, exhaustive over the event model**, in a YAML-shaped
layout that is not a YAML document. `crates/logit-outputs/src/human.rs`'s module doc is the
canonical grammar; the decisions it follows:

1. **A fixed divider line opens every event**, then `timestamp:`, then sections at column 0 in
   payload-first order: `log:`, `metrics:`, `span:`, `attributes:`, `resource:`, `scope:`. What
   a person tails for is at the top of the block; the context it came with is below.
2. **Exhaustive by omission, not by `null`.** Every populated field of `Event`, `LogRecord`,
   `MetricRecord` (every kind's own fields, exemplars, flags), `SpanRecord` (its `SpanExt`, events,
   links), `Resource`, and `Scope` is written. A field that is `None`, empty, or `0` is omitted
   rather than written as `null`/`0`, so a statsd counter stays five lines and an OTLP span with
   everything set shows everything. The divider and `timestamp:` always appear, so an empty event
   is still visible.
3. **`resource:` and `scope:` are their own sections, repeated in every block.** A block stands
   alone when grepped out of a file, and an event/resource key collision shows both values instead
   of one winning silently.
4. **Nesting is indentation.** A `Map` value nests as a block; an `Array` of scalars stays inline
   as `[a, b]`; an `Array` holding a container becomes a `- ` list, as `metrics:`, `events:`,
   `links:`, and `exemplars:` are. Keys are bare when identifier-shaped and quoted otherwise;
   strings are quoted and escaped; `Bytes` is `b"..."` with each invalid byte as `\xHH`, so the
   content is shown rather than a byte count.
5. **The log message is unquoted, and its line breaks are a mode.** `message: escaped` (the
   default) keeps it on one line with every control character escaped; `message: multiline`
   writes a newline as a real line break with continuation lines aligned under the message's
   first character, and a tab as a tab. Every other control character, ESC above all, is escaped
   in both modes: any string the sink writes can be a peer's bytes, and a raw ESC would drive
   the viewer's terminal. The mode is a field on both kinds, and graph rule 72 rejects
   `multiline` under any format but `human`, as rule 33 rejects `compression` under any format but
   `native`.
6. **Sketches render as summary statistics.** A `Distribution` shows count, sum, min, max, mean,
   and p50/p90/p95/p99; a `Set` shows its estimate. Bins and registers are the sketch's internals,
   not observations, and a hundred bin lines per metric would bury the event.
7. **`syslog_out`'s container fallback is pinned.** It rendered a `Map`/`Array`/`Bytes` message
   through the old inline value grammar, which is now `render_value_inline`, unchanged, used only
   by syslog. The human render's own value grammar is free to move without touching a wire.

## Alternatives considered

- **Make the default NDJSON, or real YAML.** Rejected for the default: a person at a terminal
  reads a sectioned block far better than a JSON line, and `format: json` already exists for a
  program. YAML would promise a parseability the escaping rules below don't keep.
- **Keep resource attributes merged into `attributes:`.** Shorter, and what `influxdb_out` does
  for tags. Rejected: the merge hides which values are batch identity and loses a colliding
  resource value, both things an exhaustive render exists to show.
- **Write every field, absent ones as `null`.** Uniform, but a five-line statsd counter becomes
  thirty lines of `null`, and the fields that matter drown.
- **Drop the multi-line mode and always escape.** One grammar to parse. Rejected: a stack trace
  read as one line with `\n` in it is the case a debug sink is most often opened for, and the
  mode is opt-in with `escaped` the default.
- **Dump DDSketch bins.** The only fully lossless rendering, but it is the sketch's storage
  format, not a reading a person can act on; `format: native` carries a sketch bit-for-bit for
  the case that needs it.

## Consequences

- `crates/logit-outputs/src/human.rs` holds `EventDump`, `Format`, `MessageMode`, and the render;
  `stdio.rs` keeps `StreamOutput`/`StreamEncoder` and re-exports them.
  `logit_core::time::write_rfc3339_utc` formats a timestamp into an existing buffer, so a block's
  several timestamps don't each allocate.
- `logit_config::MessageMode` and a `message:` field on `StdioOut`/`FileOut`; graph rule 72;
  `schema/logit.schema.json` regenerated.
- `crates/logit-bench/tests/allocations.rs`'s `stdio_encode_100_events` pin and
  `docs/design/memory.md`'s row move to the new count. The perf VM measured the render on
  2026-10-02: `encode-human-devnull` costs 1.025 µs/event with the block render against 1.039
  before it, and `docs/design/memory.md` §2's `stdio_out` timing row is +5.3% against 2026-09-20
  (`docs/design/performance.md` §1).
- Batch provenance (`origin`/`previous`) is the one thing the render does not show: it reaches a
  sink through `Output::observe_batch`, which `StreamOutput` doesn't implement, and
  [`batch-provenance-on-delivered`](batch-provenance-on-delivered.md) keeps it off the event.
  Tracked in `docs/known-gaps.md`.
