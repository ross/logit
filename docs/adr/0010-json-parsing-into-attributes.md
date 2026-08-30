# 0010 — `json` transform: structured attributes, additive, pass-through on failure

## Status
Accepted

## Context

`ComponentKind::Json` (`crates/logit-config/src/lib.rs`) has existed as a config variant since the
component-graph work landed, rejected at `logit validate`/`logit run` as "not implemented yet."
`docs/design/lua-api.md` fixes this transform's role in advance — one of the "built-in native
processors ... meant to sit in front of user Lua," specifically "parse the JSON body, then run my
logic" — but leaves open how a parsed JSON object actually lands on the event, what happens to the
message it was parsed from, and what a parse failure does. Per `AGENTS.md`'s "a new design decision
worth remembering gets an ADR," those are what this record settles.

## Decision

**Structured, not flattened.** A JSON object becomes `Value::Map(Box<AttrMap>)`; a JSON array
becomes `Value::Array`. Both are first-class `Value` variants already, and `logit-script/src/value.rs`
already round-trips both to real Lua tables, so a downstream script reads `event.attributes.http.status`
directly — no dotted-key convention to invent or document, and no information lost the way flattening
an array into `tags.0`/`tags.1` would.

**Additive: `message` and `body_format` are left untouched.** `LogRecord::body_format` is documented
as *how the body was found* — a decode-time hint flowing *into* this transform (a decoder noticing a
body looks like JSON), not something this transform produces. The parsed key/values are new
information layered on top of the existing event, not a replacement for it; a later `remove`/Lua
stage can drop the message if a particular pipeline wants that.

**A parsed key overwrites an existing attribute of the same name** — plain `AttrMap::insert`
semantics (last-writer-wins), with no special collision handling. Same rule for a duplicate key
within one JSON object. Consistent with how every other attribute-writing path in this codebase
already behaves (`AttrsProxy::__newindex` in `logit-script`, `Aggregator`'s own `AttrMap::insert`
calls) — inventing a different collision policy here would be a new, undocumented special case for
no concrete requirement.

**Only a top-level JSON *object* is merged.** A syntactically valid `[1,2]` or `"hi"` has no
key/values to merge into attributes, so it's treated the same as a parse failure: pass the event
through untouched, with a diagnostic. This is enforced structurally, not as a post-parse check — the
top-level deserializer only ever asks for a JSON object, so a top-level scalar or array is a decode
error by construction.

**Pass through on any parse failure, never drop.** Malformed JSON, a non-object top level, or (in
`skip_to_brace` mode) no `{` found in the message at all — every failure path returns the event
unchanged, attributes untouched, with an `eprintln!` diagnostic (`docs/known-gaps.md`'s existing
`eprintln!` gap, not a new one). Matches this codebase's consistent stance (`aggregate` forwards
metric kinds it can't merge rather than dropping them, per
[ADR 0008](0008-aggregation-window-semantics.md)) — losing telemetry over one malformed line is a
worse failure than a no-op.

**Only `Payload::Log` events with a string/bytes message are candidates.** A metric or span event
has no message to parse and passes through untouched; so does a log event whose `message` isn't
`Value::Str`/`Value::Bytes` (there's nothing meaningful to feed a JSON parser).

**`skip_to_brace: bool`, default `false`.** Off, the whole line is assumed to be the JSON data, and
trailing non-whitespace after the value is a parse failure — "the whole line is the JSON data" is a
real assertion this mode makes, not just "parse a prefix of it." On, everything before the first `{`
is skipped and parsing starts there, tolerating trailing content after the object closes — this is
what makes a line like `2026-08-29 INFO {"a":1} took=3ms` parse at all. The two modes deliberately
differ on trailing-content strictness, not just on where they start.

**Values are decoded directly into `logit_core::Value`**, via a `serde::de::DeserializeSeed`/
`Visitor` pair (`crates/logit-transforms/src/json.rs`), rather than through an intermediate
`serde_json::Value` tree and a separate conversion. Two reasons: one fewer allocation-and-walk per
line, and it lets an *unescaped* JSON string decode as a zero-copy `Bytes` slice of the original
message buffer rather than a fresh allocation — the "`bytes::Bytes` everywhere strings and blobs
appear" rule in `docs/design/data-model.md`. An escaped string (`"a\nb"`) still copies, since
serde_json has already unescaped it into a scratch buffer with no connection to the original bytes
by the time the visitor sees it. Object keys are decoded as plain `String`s, not zero-copy: every
key is interned by `AttrMap::insert` regardless, so a zero-copy key would only save a `String`
allocation, not the interning itself — not worth a second seed type for.

## Alternatives considered

- **Dotted-key flattening** (`http.status`, `tags.0`) instead of structured `Value::Map`/`Array`.
  Rejected: lossy (a real attribute literally named `http.status` becomes indistinguishable from a
  flattened `http: {status: ...}`), and `AttrMap`/`Value` already represent nesting natively with no
  extra work needed downstream.
- **Replace `message` with the parsed data, or clear it to `Value::Null`.** Rejected: throws away the
  raw line for good. A sink or later stage that wants the original text (shipping the raw line
  alongside structured fields, say) can no longer get it; the additive approach costs nothing and a
  pipeline that *does* want the message gone can drop it explicitly downstream.
- **Set `body_format = Structured` after a successful parse.** Rejected: `body_format` is documented
  as describing how the body was *found*, i.e. an input/decoder-side signal to downstream parsers —
  not an output this transform should be writing. Repurposing it would blur that meaning for every
  other reader of `body_format` in the codebase.
- **Drop an event that fails to parse** (`process` returns `None`). Rejected outright, for the same
  reason ADR 0008 rejected it for `aggregate`: silently losing telemetry over a malformed line is a
  worse failure mode than forwarding it unchanged with a diagnostic.
- **A `field`-style config option naming where to nest the parsed result** (e.g. all keys go under
  `event.attributes.json` instead of merging at the top level). Not requested and adds config
  surface with no concrete need driving it; can be added later as a strictly additive config field if
  someone hits real key collisions in practice.

## Consequences

- A pipeline that mixes JSON and non-JSON log lines through one `json` component gets attributes on
  the JSON lines and untouched events (plus one stderr line each) for the rest — this is the
  intended behavior, not a partial failure to fix.
- A high-volume source of malformed lines produces one `eprintln!` per event, same as `aggregate`'s
  existing kind-conflict diagnostic — cosmetic today, tracked under `docs/known-gaps.md`'s existing
  "real diagnostics facility" gap rather than a new `json`-specific one.
- No log-producing `ComponentKind` is implemented yet (`syslog_in`/`file_tail` are still
  "not implemented" stubs), so `json` cannot be exercised in a real running pipeline today — every
  event it sees in practice is a pass-through. Unit tests (`crates/logit-transforms/src/json.rs`)
  are the real coverage until a log-producing input lands; not a defect in this component.
- A parsed attribute can silently overwrite one set earlier in the pipeline (by the decoder, or by
  an upstream transform) if the JSON object happens to use the same key. No new failure mode this
  codebase doesn't already have elsewhere (a Lua script can do the same via `event.attributes.x =
  ...`) — a config author who cares about this orders/names things to avoid it.

## Amendment: "log events" means "events carrying a log"

[ADR 0012](0012-multi-payload-events.md) replaces `Event`'s one-of `Payload` with independent
`log`/`metrics`/`span` fields, so "Only `Payload::Log` events... are candidates" above no longer
parses — that type is gone. Reworded without changing the decision: only an event whose `log` field
is present, and whose message is `Value::Str`/`Value::Bytes`, is a candidate; an event with no log at
all passes through untouched, exactly as a metric- or span-only event did under the old model.

**An event carrying both a log and metrics (or a span) is a new, previously-unrepresentable shape,
and behaves exactly as the additive design above already implies**: `json` only ever reads
`log.message` and writes `attributes`, so any metrics or span already on the event ride through
completely unaffected, whether the log half parses successfully or not. No new code path was needed
for this — see `a_log_event_that_also_carries_a_metric_is_parsed_and_keeps_its_metric`
(`crates/logit-transforms/src/json.rs`) for the regression test proving it.
