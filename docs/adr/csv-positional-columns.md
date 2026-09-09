---
created: 2026-09-07
updated: 2026-09-07
---

# `csv`: positional columns from config, not a header row, and no type coercion

## Status
Accepted

## Context

`ComponentKind::Csv` has existed as a config variant since the component-graph work landed, carried
in `crates/logit-config/src/lib.rs`'s "not implemented yet" block alongside `logfmt`, `kv`, `regex`,
`rename`, `filter`, `sample`, `throttle`, and `dedup` — referenced in `docs/OVERVIEW.md` and
`docs/design/lua-api.md` as one of the "built-in native processors ... meant to sit in front of user
Lua," specifically for line shapes too slow or too tedious to hand-parse in Lua. `docs/design/
pipeline-graph.md`'s arity table already lists it as a transform. None of that settles how a CSV
line actually becomes attributes: where the schema (column names) comes from, what happens to a
line with the wrong number of fields, what type a field lands as, or how RFC 4180 quoting is
handled. Per `AGENTS.md`'s "a new design decision worth remembering gets an ADR," this record
settles those questions, the same way `docs/adr/json-parsing-into-attributes.md` settled them for
`json`.

## Decision

**Explicit `columns: Vec<String>`, required and non-empty, positional — no header-row mode.**
`columns[i]` names the attribute the row's `i`-th field is written to. There is no `has_header:
bool` and no positional fallback naming (`column_0`, `column_1`, ...); see "Alternatives
considered" for why both were rejected.

**A row byte-identical to the configured header line is recognized *by value*, not position, and
passed through unparsed.** `CsvParser` builds `header_line = columns.join(delimiter)` once at
construction; a message equal to it produces a throttled `header_row` diagnostic and the event
continues downstream untouched (never dropped — a pipeline that wants the header line gone drops it
with a `lua` component). This check is unconditional, with no config field to disable it — a real
header row parsing as if it were data would silently populate every attribute with its own name as
a string, which is never useful.

**`delimiter: char`, default `,`.** TSV is in scope (`delimiter: "\t"`, double-quoted in YAML so
the escape survives YAML's own parsing). Rejected at graph-validation time: `"` (RFC 4180's quote
character, which this parser reads as field framing, not data), `\n`/`\r` (already consumed as line
framing by every input), and any non-ASCII character.

**RFC 4180 quoting within one line only.** A quoted field (`"a,b"`) may contain the delimiter as
data; `""` inside a quoted field is one literal `"`; an empty quoted field (`""`) is an empty
string; a `"` inside an *unquoted* field is data, not an error (real producers emit it, and
rejecting the row on this alone loses more than it protects). **Embedded newlines are explicitly
out of scope**: `LineSplitter::push` (`crates/logit-inputs/src/tail/line.rs`) scans for `b'\n'`
unconditionally with no quote-awareness, so a CSV record with a newline inside a quoted field
already arrives as two separate `Event`s by the time this transform sees either half — reassembling
them would need cross-event state that belongs in the input's line-framing layer, not in a
stateless per-event transform. See Consequences for what actually happens to such a record.

**No type coercion — every field is `Value::Str`, including an empty field (`Value::Str("")`, not
`Value::Null`)**. CSV carries no type grammar the way JSON syntax does (`json`'s ADR types by
syntax, never by string-parsing), so inventing one here would manufacture instability
`docs/adr/scale-transform.md` already warns against for a different transform. `numeric` (shared by
`kv_metrics`/`scale`, `crates/logit-transforms/src/lib.rs`) already coerces a `Value::Str` that
parses cleanly to a finite `f64` and returns `None` for `""`, so `csv → kv_metrics`/`scale` works
correctly with zero coercion in `csv` itself.

**A row with the wrong number of fields is a whole-line parse failure**: pass the event through
unchanged, attributes untouched, with a throttled `field_count` diagnostic naming both the expected
and actual counts. Not truncate (a misaligned prefix looks plausible but is wrong) and not pad
(needs an invented sentinel value with no real meaning).

**Flat namespace, no prefix.** `columns[i]` is the attribute name verbatim, written via
`AttrMap::insert_sym` (the caller already holds an interned `Symbol`, so this skips the
`resolve`-then-`intern` round trip `json.rs`'s own follow-up note flags). Last-writer-wins on
collision with a pre-existing attribute — the same posture `json-parsing-into-attributes` already
settled and every other attribute-writing path in this codebase already has. **Duplicate names
within `columns` are rejected at graph-validation time**: the later column would silently win over
the earlier one on every event, leaving the earlier one permanently unreachable — not a runtime
condition worth discovering event-by-event.

**Everything else inherited unchanged from `docs/adr/json-parsing-into-attributes.md`**: additive
(the message and `body_format` are left untouched), pass-through-never-drop on any failure, and
only a `log` event with a `Value::Str`/`Value::Bytes` message is a candidate (a metric- or
span-only event, or a log with a non-string message, passes through untouched). The one place this
transform diverges from `json` is types: `json` types by JSON syntax; `csv` has no syntax to type
by, so it deliberately makes no attempt to guess. A `Value::Bytes` message carries no UTF-8
guarantee (an OTLP body's `bytes_value` decodes straight into one), while every field this
transform produces is handed to `Value::Str`, whose invariant *is* valid UTF-8 -- so the whole
message is validated as UTF-8 once, up front, before any field is sliced out of it. One check
suffices for every field: `delimiter` is a single ASCII byte and `"` is ASCII (rule 29), so every
boundary `split_row` computes lands on an ASCII byte and never inside a multi-byte sequence, and
`unescape` only ever deletes an ASCII `"` -- both keep a valid whole valid in its parts.

**Diagnostic keys are distinct and independently throttled** (`Diagnostics::warn_throttled`, each
auto-mirrored to `logit.component.diagnostics{key=...}`): `field_count` (schema drift — actionable),
`parse_failure` (malformed quoting), `header_row` (the recognized header line), `invalid_utf8` (the
message failed the up-front UTF-8 check). An empty message is a silent, un-throttled skip
(`logit.transform.rows.skipped{reason="empty"}`) — routine, not exceptional, the same way an empty
line from a tailed file needs no diagnostic at all.

**No new `Cargo.toml` dependency — a hand-rolled ~60-line state machine**, not the `csv` or
`csv_core` crate. `csv::Reader` copies every record into its own buffer regardless; `csv_core::
Reader` writes into a caller-supplied buffer with its own resize loop — more code at the call site
for the same result, and neither gives an unquoted field a zero-copy `Bytes::slice` the way this
transform's own splitter does. A single-line, single-delimiter splitter with RFC 4180 quoting is a
small, exhaustively-testable four-state machine over a fully-specified grammar; no crate earns its
weight here.

## Alternatives considered

- **`has_header: bool` (first event defines the schema).** Rejected on five independent grounds,
  any one of which is disqualifying on its own: `tail_in`'s `read_from` defaults to `end`, so on a
  pre-existing file the header row is never seen and the first *data* row becomes the schema;
  checkpointed restart resumes mid-file, so a restart can silently start reading whatever line sits
  at the checkpoint offset as the header; file rotation delivers a new file's header row mid-stream
  as an ordinary event, parsed as data; fan-in (multiple sources, or a `paths:` glob tailing several
  files) means whichever line arrives first defines the schema for all of them, an ordering with no
  stable meaning; and the resulting config is not self-describing — `logit graph` has nothing to
  show for it, and validation can't check anything about the schema it implies.
- **Positional fallback names (`column_0`, `column_1`, ...) when `columns` is absent or short.**
  Rejected: pushes the coupling downstream (`kv_metrics`/`keep` would need to know `field:
  column_5`), a config change that adds a column earlier in the row silently shifts every fallback
  name after it, defeats the "empty/absent list is a certain no-op" validation shape every other
  transform here uses, and produces attribute names nobody would ever choose to key logic on.
- **A separate `Tsv` sibling kind.** Rejected — one parameterized kind (`delimiter:`) matches the
  precedent already set by `syslog_out`'s `format:` and `otlp_out`'s `protocol:`: a format
  difference is a field, not a new `ComponentKind`.
- **`delimiter` fixed to `,`.** Rejected outright — TSV access logs and pipe-delimited exports are
  common enough in practice that hand-writing a Lua splitter for anything but a comma would defeat
  the point of shipping this transform at all.
- **Truncate or pad on a wrong field count.** Rejected — see Decision above: truncation produces a
  plausible-looking but misaligned result, and padding requires inventing a sentinel value with no
  real meaning to any downstream consumer.
- **Multi-line record reassembly inside this transform.** Rejected — `LineSplitter` has already
  split the record in two by the time any transform sees it; reassembly needs state that spans
  events and belongs in the input's own line-framing layer, not a stateless per-event transform
  positioned arbitrarily far downstream of it.
- **A `csv`/`csv_core` crate dependency.** Rejected — see Decision's closing paragraph.

## Consequences

- A CSV record with an embedded newline (inside a quoted field) produces **two** pass-throughs and
  **two** throttled diagnostics, never one corrupt merged attribute set: the first half ends at
  end-of-input inside an open quote (`UnterminatedQuote` → `parse_failure`), and the second half
  parses to some field count that is almost certainly wrong for the configured schema (→
  `field_count`). This is the intended, documented behavior of leaving newline reassembly out of
  scope, not a partial failure to fix later.
- A source that starts adding (or removing) a column upstream silently fails every subsequent row's
  `field_count` check until the config is updated — loud in telemetry (`logit.component.
  diagnostics{key="field_count"}`), but not something `logit validate` can catch ahead of time, the
  same posture `docs/adr/kv-metrics-semantics.md` already accepts for a typo'd `field:`.
- The header row itself, when it appears in the data stream, reaches every downstream component as
  an ordinary log line with unparsed attributes — dropping it, if a pipeline wants that, is a `lua`
  or `filter`-shaped job, not something this transform does on the config author's behalf.
- Every field is a string end to end unless something downstream (`kv_metrics`/`scale`, via
  `numeric`) chooses to coerce it — a pipeline that wants a numeric attribute type on the wire
  needs one of those in the chain; `csv` alone will never produce `Value::I64`/`Value::F64`.
