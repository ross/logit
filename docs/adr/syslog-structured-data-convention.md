---
created: 2026-09-11
updated: 2026-09-11
---

# RFC 5424 structured-data convention: nested `syslog.sd`, strict parsing, opt-in PEN-qualified emission

## Status
Accepted

## Context

[ADR `lossless-transit`](lossless-transit.md) commits `syslog_in -> syslog_out` to a lossless
relay, modulo a named list of permitted normalizations. Before this decision, `syslog_in`
(`crates/logit-inputs/src/syslog.rs`) only balanced-and-skipped RFC 5424 STRUCTURED-DATA
(`skip_structured_data`) — its contents never reached an attribute — and `syslog_out`
(`crates/logit-outputs/src/syslog.rs`) always wrote the NILVALUE `-`, regardless of what an event
carried. [`docs/plans/lossless-transit.md`](../plans/lossless-transit.md)'s W5 workstream closes
that gap: a real, quote-aware STRUCTURED-DATA parser and its exact encoder inverse, plus an opt-in
element for attributes that didn't originate as `syslog.sd` at all.

That plan's own "Attribute conventions" section originally justified the nested `syslog.sd` shape
partly on interning grounds — "nesting also interns one bounded key (`syslog.sd`) instead of
interning attacker-chosen SD-ID/PARAM-NAME strings directly." **That reasoning is wrong and this
ADR corrects it.** `AttrMap::insert` interns every key it's given, at any nesting depth — the inner
`Value::Map`s under `syslog.sd` intern their own SD-ID and PARAM-NAME keys exactly as if they were
top-level attribute names. Nesting buys nothing on the interning axis. The real reasons for the
shape are disambiguation, free round-trip through the native codec, and Lua ergonomics — see
Decision below.

Three more design questions had to be settled alongside the parser/encoder pair: what `syslog.pid`
and `syslog.timestamp` become when they can't be represented as before (a non-numeric PROCID, a
nil TIMESTAMP), what happens to a non-UTF-8 MSG, and how an attribute that never came from
`syslog_in` reaches the wire on a relay.

## Decision

### `syslog.sd`: a nested `Value::Map`

```
syslog.sd = Value::Map {
    "<SD-ID>" -> Value::Map {
        "<PARAM-NAME>" -> Value::Str | Value::Array<Value::Str>
    }
}
```

One outer map entry per SD-ELEMENT, one inner map entry per distinct PARAM-NAME. A PARAM-NAME
repeated within one SD-ELEMENT becomes a `Value::Array` of `Value::Str`, in the order encountered
(`insert_param`) — RFC 5424 doesn't forbid repetition, and there's no defined merge for two values
under one name, so every occurrence is kept rather than last-write-wins. The nil STRUCTURED-DATA
marker (`-`) produces no `syslog.sd` attribute at all — never an empty map — so "no structured
data" and "an empty element" (which the grammar doesn't allow anyway) stay distinguishable from
"the attribute wasn't looked at."

### Parse rules (`crates/logit-inputs/src/syslog.rs`'s `parse_structured_data`, `parse_sd_name`, `parse_param_value`)

Faithful to RFC 5424 §6.3, with the same strictness the rest of this dialect's parsing already
has:

- **`SD-NAME`** (both `SD-ID` and `PARAM-NAME`): 1 to 32 bytes of PRINTUSASCII (`%d33-126`)
  excluding `=`, SP, `]`, and `"`. Zero bytes or more than 32 is a grammar violation.
- **Escapes.** Exactly three two-character sequences inside a `PARAM-VALUE` are escapes: `\"`,
  `\\`, `\]`. A backslash before any other byte is kept literally, along with that byte, rather
  than treated as an unrecognized escape or a no-op — `parse_param_value` never mis-reads a
  legitimate literal backslash as the start of an escape it doesn't recognize.
  `PARAM-VALUE` is then required to be valid UTF-8 once unescaped (it's defined as
  `UTF-8-STRING`).
- **Duplicate `SD-ID` rejects the line.** An `SD-ID` repeated within one message has no defined
  merge between the two elements sharing it, so this parser rejects the whole line rather than
  silently picking one (last-wins or first-wins would both be a guess the RFC doesn't license).
- **Any other grammar violation rejects the whole line** — a missing `=`, a missing opening or
  closing quote, an unterminated element, an SD-NAME outside 1..=32 PRINTUSASCII-minus-`=SP]"` —
  with a `bad_line` diagnostic naming what was violated and its byte offset. This is the same
  strictness every other malformed RFC 5424 field on the line already gets (a bad PRI, a bad
  TIMESTAMP): STRUCTURED-DATA is not a special, more-tolerant case.

### Emit rules (`crates/logit-outputs/src/syslog.rs`'s `write_structured_data`, `write_sd_element`, `write_sd_param`, `push_sd_escaped`)

- **Exact inverse escaping.** `push_sd_escaped` emits only `"` → `\"`, `\` → `\\`, `]` → `\]` — the
  precise inverse of the parser's unescaping, so a value the parser accepted always re-encodes to
  the same three sequences (never a fourth "recognized" escape the parser doesn't produce).
- **Invalid names are skipped, not fatal.** An SD-ID or PARAM-NAME that fails `is_valid_sd_name`
  (RFC 5424 §6.3.2's `SD-NAME` grammar) causes the whole SD-ELEMENT (for an invalid SD-ID) or just
  that one param (for an invalid PARAM-NAME) to be dropped — counted in
  [`EncodeStats::dropped_invalid_sd`] and reported via a throttled `invalid_structured_data`
  diagnostic, rather than corrupting the line with an unparseable element.
- **RFC 3164 output never emits STRUCTURED-DATA.** 3164 has no such field; `syslog.sd` and the
  opt-in element below are both silently dropped on a `5424 -> 3164` relay. This is one of the
  plan's permitted normalizations (a sink-configured dialect change), not data loss this sink is
  expected to work around.
- **Opt-in `structured_data: { sd_id: "<name>@<PEN>" }`.** When `syslog_out` is configured with
  this (5424 output only), every event attribute whose key does *not* start with `syslog.` is
  emitted as one extra SD-ELEMENT under `sd_id`, PARAM-NAME = attribute key, same
  validate/skip/count rule as above; the element is omitted entirely when no attribute qualifies.
  This is what closes the `syslog_in -> json -> syslog_out` gap — an attribute a transform added
  along the way still reaches the wire when an operator opts in. `syslog_in` decodes this element
  back into `syslog.sd` like any other on the way back in; there is no automatic re-lifting of it
  into top-level attributes — that stays a transform's job.

  **No default private enterprise number is shipped.** `sd_id` must be a valid `SD-NAME`
  containing exactly one `@` (`with_structured_data`'s validation). RFC 5424's own `32473` example
  PEN, used throughout the RFC's spec text, is documentation only — it was never assigned to this
  project and shipping it as a silent default would mint SD-ELEMENTs under an enterprise number
  `logit` has no right to. Registering a real PEN with IANA, or reusing one an operator already
  holds, is a decision for whoever turns this feature on.

  **SD-ID collision.** RFC 5424's grammar permits `@` inside an ordinary `SD-ID`, so a peer that
  happens to use the same PEN-qualified id `syslog_out` was configured with would otherwise make
  the sink emit two SD-ELEMENTs sharing one SD-ID, which §6.3.1 forbids and which `syslog_in`
  rejects outright. The encoder guards against it: when `structured_data.sd_id` already appears as
  a key of the event's own `syslog.sd`, the opt-in element is skipped for that event, counted under
  `dropped_invalid_sd`, and reported through the throttled `invalid_structured_data` diagnostic
  naming the collision. The origin's element wins because it is real data; the opt-in element is
  a convenience the operator can rename.

### Timestamp precedence

Per event, per `syslog.timestamp`'s shape, independent of the *input* dialect that produced it:

| `syslog.timestamp` | RFC 5424 output | RFC 3164 output |
|---|---|---|
| `Value::Timestamp` (5424's parsed, unambiguous instant) | renders directly (RFC 3339) | renders directly (`Mmm dd hh:mm:ss`) |
| `Value::Str` (3164's raw 15-byte token, no year/timezone) | falls through to `event.timestamp` | renders verbatim, but only when it's exactly that 15-byte shape (`is_rfc3164_timestamp_shape`); otherwise falls through |
| `Value::Null` (5424's nil `-` TIMESTAMP) | renders as `-` (NILVALUE) | falls through to `event.timestamp` (3164 has no NILVALUE concept for TIMESTAMP) |
| absent, or any other `Value` | falls through to `event.timestamp` | falls through to `event.timestamp` |

`event.timestamp` itself is always receipt time (`docs/adr/decoupled-listener-io.md`) and this sink
never resolves `syslog.timestamp` onto it — the opt-in `syslog_timestamp` transform
`docs/known-gaps.md` sketches remains the place that would do that explicitly, for either
direction, before an event reaches `syslog_out`.

### `syslog.pid`

`Value::U64` when PROCID (5424) or a `tag[pid]` bracket (3164) parses as one; `Value::Str` of the
raw token otherwise. RFC 5424's PROCID is free-form PRINTUSASCII, not necessarily numeric, and this
project now keeps a non-numeric one rather than dropping it. `resolve_pid` mirrors this on encode
([`Pid::U64`]/[`Pid::Str`]): a `Pid::Str` is sanitized and capped at 128 bytes (5424's own PROCID
maximum) on 5424 output, and rendered as `tag[pid]` after the same cap on 3164 output (3164 defines
no PROCID length limit of its own; 128 is reused for consistency, not because the RFC requires it).

### Non-UTF-8 MSG

Header fields (PRI, the 3164 timestamp token, HOSTNAME, TAG/APP-NAME, PROCID, MSGID,
STRUCTURED-DATA) are parsed and validated off the line's raw bytes, independent of MSG. MSG alone
is UTF-8-validated on its own: valid UTF-8 becomes `Value::Str` as always; invalid UTF-8 becomes
`Value::Bytes` (`message_value`) instead of rejecting the line — RFC 5424's `MSG-ANY` explicitly
permits arbitrary octets. On encode, a `Value::Bytes` message is sanitized at the byte level
(`sanitize_msg_bytes`) and written raw, never lossy-UTF-8-decoded.

### BOM handling

A leading RFC 5424 §6.4 UTF-8 BOM (`EF BB BF`) on MSG is stripped on decode (`message_value`),
only when the whole MSG including the BOM is valid UTF-8 — it's a `MSG-UTF8` signal, not payload.
It is **never emitted** on encode: an earlier version of `syslog_out` did emit one (the symmetric
choice), but Loki's `| json` LogQL stage (Go's `encoding/json`) does not skip a leading BOM, so
every relayed line silently failed to parse; see [ADR `syslog-output`](syslog-output.md). Stripping
on the way in and never re-adding it on the way out is a permitted normalization — the BOM carries
no information beyond "this MSG is UTF-8," which is already implied by MSG decoding to `Value::Str`
at all.

## Alternatives considered

- **Flattened `syslog.sd.<id>.<param>` keys.** Rejected: `SD-NAME` (both SD-ID and PARAM-NAME) may
  itself contain `.`, which makes a flattened key ambiguous to reassemble — `syslog.sd.a.b.c`
  can't tell whether `a.b` is the SD-ID and `c` the PARAM-NAME, or `a` and `b.c`, without
  re-parsing against a namespace no such key encodes.
- **`Array` of `{sd_id, name, value}` triples.** Rejected: no lookup ergonomics — every read
  becomes a linear scan instead of a map access — and awkward in Lua, which has no destructuring
  convenient for triples the way it has for nested tables.
- **Raw `syslog.raw` passthrough** (the whole line or datagram as `Value::Bytes` alongside the
  normalized attributes). Already rejected in [ADR `lossless-transit`](lossless-transit.md)'s own
  Alternatives: it goes stale the instant a transform touches the event, doubles memory for every
  event carrying it, and answers "what bytes arrived" rather than "does the information survive."
- **Lenient SD parsing that tolerates grammar errors** (skip a malformed element rather than
  reject the line, or accept an SD-NAME outside the 32-byte limit). Rejected on two grounds:
  consistency with every other RFC 5424 field this dialect already parses strictly (a bad PRI or
  TIMESTAMP rejects the line, not just the field), and a lenient parse can't be inverted exactly —
  `syslog_out` would have no faithful way to re-emit whatever was tolerated, breaking the
  byte-faithful relay this convention exists to enable.
- **Shipping a default private enterprise number** (e.g. RFC 5424's own `32473` example) for the
  opt-in `structured_data` element. Rejected: `32473` was never assigned to this project — silently
  minting SD-ELEMENTs under it would misrepresent their origin to any receiver that looks the PEN
  up. Picking a real one is an operator decision, not a default `logit` should make silently.

## Consequences

- **Corrected interning statement.** `syslog.sd`'s inner `Value::Map`s intern their SD-ID and
  PARAM-NAME keys exactly like any other `AttrMap` key — nesting doesn't bound that. The growth is
  bounded instead by RFC 5424's own grammar: an SD-ID or PARAM-NAME is at most 32 PRINTUSASCII
  bytes, so an adversarial or careless sender can grow the interner at most as fast as a stream of
  distinct, RFC-legal 1-to-32-byte tokens — the same exposure `json`
  ([ADR `json-parsing-into-attributes`](json-parsing-into-attributes.md)) already has today, where
  an arbitrary JSON object's keys are interned unconditionally via `AttrMap::insert` with no length
  bound at all. `docs/known-gaps.md`'s interner entry is rewritten to name `syslog.sd` instead of
  claiming syslog's field names are "bounded by construction."
- **`max_message_bytes` now counts STRUCTURED-DATA as header.** `write_rfc5424_header` appends
  STRUCTURED-DATA before `encode_event`'s length check runs, so a `syslog.sd`/`structured_data`
  element that pushes the header over `max_message_bytes` drops the whole message
  (`EncodeStats::dropped_oversize_header`) exactly like an oversize hostname would, rather than
  being truncated or silently omitted.
- **`syslog_in -> syslog_out` is byte-faithful for 5424→5424 and 3164→3164**, modulo the following
  enumerated normalizations:
  - A leading MSG BOM is stripped on decode and never re-emitted on encode.
  - A configured sink `hostname`/`app_name` fills only an *absent* `syslog.hostname`/`syslog.tag`
    attribute — it never overrides one the origin actually sent, but it does mean a relay through a
    sink configured with defaults is not byte-identical for a message that omitted those fields.
  - A 3164 output drops `syslog.sd` and the opt-in `structured_data` element entirely (3164 has no
    STRUCTURED-DATA field).
  - A 3164-shaped `Value::Str` timestamp falls through to `event.timestamp` (receipt time) when the
    *output* format is 5424 — there's no year or timezone in the raw token to build an RFC 3339
    stamp from.
  - **SD-ELEMENT and SD-PARAM order is canonicalized by name, not preserved from the wire.**
    `write_structured_data`/`write_sd_element` sort SD-IDs and PARAM-NAMEs by name bytes before
    writing, since `AttrMap`/attribute iteration order is process-global intern order, not wire
    order — a relay that saw `[b@2 ..][a@1 ..]` re-emits `[a@1 ..][b@2 ..]`. A repeated
    PARAM-NAME's occurrences are emitted grouped (already grouped under one `Value::Array` by the
    decoder), so a wire `a b a` interleaving is not preserved — see `docs/known-gaps.md`.
  - **A PARAM-VALUE's bare (non-escape) backslash is re-emitted in canonical escaped form.** RFC
    5424 section 6.3.3 declares only `\"`, `\\`, `\]` as escapes; a backslash before any other
    byte is a literal backslash followed by that byte, which `parse_param_value` keeps literally
    rather than rejecting. `syslog_out` then re-emits that literal backslash the canonical way
    (`\` → `\\`), so `p="a\xb"` relays as `p="a\\xb"` — the same PARAM-VALUE, per the RFC's own
    equivalence, just spelled the canonical way.
- **The `syslog_timestamp` transform sketch (`docs/known-gaps.md`) remains the way to resolve
  `event.timestamp` from `syslog.timestamp` explicitly**, for either direction — this ADR changes
  what `syslog_out` does with `syslog.timestamp` when present, not what sets `event.timestamp`
  itself, which stays receipt time throughout the pipeline.
- `crates/logit-config/src/lib.rs` gains `SyslogStructuredData` and `SyslogOut::structured_data`;
  `crates/logit-cli/src/pipeline.rs`'s `SyslogOut` arm is the sole place a config `sd_id` crosses
  into `SyslogEncoder::with_structured_data` and its `anyhow::Result` becomes a config-time error;
  `schema/logit.schema.json` regenerated.
