---
created: 2026-09-28
updated: 2026-09-28
---

# `stdio_out`/`file_out` gain `format: json`, and every machine reader of the text render moves to it

## Status
Accepted

## Context

`stdio_out` and `file_out` write a human-facing text render by default
([`rotating-file-output`](rotating-file-output.md),
[`file-output-native-format`](file-output-native-format.md)). It was built for a person at a
terminal and documented as not an export format, with an NDJSON `Format` variant named as the
extension point if a machine consumer ever needed one.

Five did. The human render is the one sink that carries raw `Samples` and every `internal`
counter to a plain file with no service in the way, so the out-of-CI harnesses read it:
`tools/shape-survey/summarize.py` and `check_interop.py` (the data-shape survey's readout and its
acceptance test, [`docs/plans/data-shape-survey.md`](../plans/data-shape-survey.md)), the Python
in `tools/shape-survey/producers/oteldemo.sh`, and the `telemetry_sum` readers in
`tools/splunk-interop/check.py` and `tools/victoria-interop/check.py`. Each carried a hand parser
for the text grammar, three of them independent by design, and any change to the human render
had to move all five. [`docs/design/data-shapes.md`](../design/data-shapes.md) §7 lists the
NDJSON format as the survey's first follow-up for this reason.

## Decision

**`format: json` is a third `StreamFormat`: one JSON object per event per line, exhaustive over
the event model, and the only form a program reads.** `crates/logit-outputs/src/ndjson.rs`'s
module doc is the canonical grammar. The rules it follows:

1. **The object mirrors the model, section by section**: `timestamp`, then `log`, `metrics[]`,
   `span`, `attributes`, `resource`, `scope`, with the field names of
   [`docs/design/data-model.md`](../design/data-model.md) and the model's own enum spellings
   (`Severity::as_str`, `BodyFormat::as_str`, `Temporality::as_str`, ...). Resource and scope are
   their own objects, repeated per line, so a line stands alone when grepped out of a file.
2. **Exhaustive by omission.** Every populated field is written; a field that is `None`, empty,
   or `0` is omitted rather than written as `null`, `0`, or `[]`. A reader tests for a key's
   presence, and a statsd counter stays one short line.
3. **Three things JSON can't carry get a fixed spelling.** A non-finite `f64` is the string
   `"NaN"`, `"inf"`, or `"-inf"`, since JSON has no number for it. `Bytes` is a string holding
   the text `b"..."`, valid UTF-8 runs as text and each invalid byte as `\xHH`, so the content is
   visible without a base64 step. That spelling is for reading, not round-tripping: a literal
   `\x` in the data reads the same as an escaped byte, and `format: native` is the lossless form. A timestamp is an RFC 3339 UTC string with nine fractional
   digits, the same form the human render uses.
4. **Hand-written, into the sink's one output buffer**, like every other encoder in
   `logit-outputs` (`docs/design/memory.md`). No `serde` derive on `Event`: the model's types
   are shaped for the pipeline, not for a serializer, and the omission rule above is not what a
   derive would produce.
5. **The harness readers consume `format: json` and nothing else.** Their capture configs set
   it, and their parsers are `json.loads`. The human render has no machine consumer, so it can
   change for a person's benefit without a Python change.

## Alternatives considered

- **Keep the readers on the human text and make its grammar stricter.** Rejected: a human
  render that has to stay parseable stops being a human render, and every one of the five
  parsers had its own copy of the grammar.
- **`serde_json` over a `Serialize` derive on the model.** Less code. Rejected: the derive
  would write every field, `null`s included, would need `#[serde]` attributes on core types for
  the omission and non-finite rules, and would put `serde_json` on the sink's hot path where
  every other encoder writes into one buffer.
- **Base64 for `Bytes`.** The conventional JSON spelling. Rejected: the value is almost always a
  syslog MSG or a tag that failed UTF-8 by a byte or two, and a reader wants to see it.
- **A `format:` template string.** The other extension the original design named. Not built:
  no reader has asked for a shape the fixed object doesn't give.

## Consequences

- `logit_config::StreamFormat::Json`, `Format::Json` in `crates/logit-outputs/src/stdio.rs`,
  `StreamEncoder::json()`, `crates/logit-outputs/src/ndjson.rs`, and a regenerated
  `schema/logit.schema.json`. Rule 33 keeps `compression:` `native`-only.
- `logit_core::time::write_rfc3339_utc` formats a timestamp into an existing buffer, so a line's
  several timestamps don't each allocate.
- `crates/logit-bench/tests/allocations.rs` pins `stdio_json_encode_100_events`, and
  `docs/design/memory.md` records it. `perf/scenarios/encode-json-devnull.yaml` exists for the
  next perf-VM session; `docs/design/performance.md` has no number for it yet.
- The five readers, their embedded self-test fixtures, and the capture configs under
  `tools/shape-survey/configs/`, `tools/splunk-interop/`, and `tools/victoria-interop/` are on
  `format: json`. A change to `ndjson.rs`'s grammar is mirrored there; the module doc names them.
- The human render is free to change. Its redesign is the next record.
