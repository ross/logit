---
created: 2026-09-04
updated: 2026-09-04
---

# `trace_context` grows a `span:` block, and a native `traceparent` parser

## Status
Accepted

## Context

[ADR `log-record-trace-context`](log-record-trace-context.md) gave `LogRecord` a native
`trace`/`span` reference and a `trace_context` transform to lift `trace_id`/`span_id`/`flags` off
an event's attributes onto it. That closes "this log line belongs to trace X" — it says nothing
about turning the line itself into a span. An access log line (haproxy, nginx, any reverse proxy
or web server) carries everything `crate::span::SpanRecord` needs: an id, a start, an end or
duration, a status. `docs/plans/demo-tracing-stack.md`'s HAProxy → nginx → app chain already mints
and propagates a W3C `traceparent` end to end, and the app already emits real OTel spans — but
haproxy and nginx, which do all the same work of receiving a request and forwarding it, produce
only logs. The goal here is the same one `docs/OVERVIEW.md` states for the project generally: see
a request pass through the stack, server to server to app, as one trace.

Two things block that today. First, `trace_context` has nowhere to put a span even if it wanted
to — it only ever writes `LogRecord.trace`. Second, and more specifically: the transform's own doc
comment (`crates/logit-transforms/src/trace_context.rs`, before this change) explains that it
deliberately refuses to parse a `traceparent` header, because the header's flags octet is hex and
the standalone `flags` field is decimal, and conflating them would silently parse `"08"` as either
8 or 0x08 depending on where it came from. That reasoning is sound for the two-values-collide
case, but it argued for refusing the whole header, not just its flags octet — the trace id and
parent id inside a `traceparent` are unambiguous regardless. `docs/plans/demo-tracing-stack.md`'s
own workaround (every tier logs `trace_id`/`span_id`/`trace_flags` as separate decimal-flags JSON
fields, split by hand in HAProxy vars or an nginx `map`) exists entirely because of that gap —
config-and-server-side plumbing standing in for four lines for a parser inside `logit`.

`docs/design/data-model.md` had no table of well-known attribute names at all. `syslog_in`'s
`syslog.*` prefix and the OTLP codec's `otel.status_message` are precedent for the *pattern* (a
dotted attribute name standing in for a field the core model doesn't carry), but nothing wrote
down a convention an operator — or another `logit` component — could target on purpose. A span
needs several such names (id, parent, kind, status, timing) at once, which is reason enough to
name them together rather than growing them one at a time as separate, uncoordinated config
fields the way `trace_context`'s original `trace_id`/`span_id`/`flags` did.

## Decision

**`trace_context` gains an opt-in `span:` block.** Absent (the default), the component behaves
exactly as it does today. Present, a successful lift also sets `event.span` to a freshly built
`SpanRecord` and rewrites `event.timestamp` to the span's start — `Event` already permits a log
and a span on the same event ([ADR `multi-payload-events`](multi-payload-events.md)), and
`docs/design/data-model.md` already uses exactly this shape as its own illustration ("an access
log line is a log record and ... a source of several metrics at once — the same event, not two
related-but-separate ones"). A span is the same idea one signal further.

**`trace_context` now parses a `traceparent` header natively.** `crates/logit_core::trace` gains
`parse_traceparent`, returning `(trace_id, parent_id, flags)` — `parent_id`, not `span_id`,
because the id inside a `traceparent` names the *caller's* span, never the receiving service's
own; nothing here ever writes it into `LogRecord.trace.span_id`. Its flags come back as the raw
hex octet, used only as a fallback when no standalone `trace.flags` attribute is present — the two
never combine, and an explicit field always wins. This narrows, rather than reverses, the original
transform's reasoning: the problem was never "a `traceparent` exists," it was "two representations
of flags must never be treated as interchangeable." Splitting a `traceparent` string by hand in a
proxy config, as `docs/plans/demo-tracing-stack.md`'s workstream A did, is no longer necessary —
see the follow-up note in that plan.

**A well-known attribute table, in `docs/design/data-model.md`.** `traceparent`, `trace.id`,
`trace.flags`, `span.id`, `span.parent_id`, `span.name`, `span.kind`, `span.status`, and
`span.start`/`span.end`/`span.duration` — full table there. `trace_context`'s three original
config fields (`trace_id`, `span_id`, `flags`) become optional, defaulting to `trace.id`/
`span.id`/`trace.flags`; a config that names them explicitly (the pre-existing shape) still works
unchanged.

**Timing is integer nanoseconds, with unit-suffixed forms for coarser producers, never a float in
an integer-denominated field.** OTLP's `start_time_unix_nano`/`end_time_unix_nano` are the model;
matching them means a producer with true nanosecond resolution (a real OTel SDK; this project's
own Django demo app) round-trips exactly. But haproxy's finest available instant is microseconds
(`request_date(us)`) and nginx's is milliseconds (`$msec`), so the convention adds
`span.{start,end}_{us,ms,s}` and `span.duration_{us,ms,s}` — the unit lives in the attribute's
*name*, never inferred from its value's shape. A JSON float in the base nanosecond form is
rejected outright rather than rounded: an `f64`'s 53-bit mantissa can't represent an
epoch-nanosecond instant exactly (2^53 ≈ 9×10¹⁵, epoch-now nanoseconds is ~1.7×10¹⁸), so a float
there is a producer bug worth surfacing, not a value worth guessing at. The `_s` (seconds) form is
the one place a float is legitimate — it's what nginx's JSON-encoded `$msec` and `$request_time`
actually are — and a quoted decimal string in that form is parsed digit-by-digit
(`logit_core::parse_decimal_nanos`), not through `f64`, so a producer that prints more significant
digits than a float can hold (`"1725400000.123456789"`) still round-trips exactly. Any two of
start/end/duration determine the third; a lone start or duration borrows the event's own (receipt)
timestamp as the missing end, specifically so an *unchanged* nginx line carrying only
`request_time` still produces a span once `trace_context` is placed after it.

**A resolved start/end further from the event's receipt time than `max_skew` (default one hour)
is rejected, not written.** The same reasoning `docs/known-gaps.md`'s sketched `syslog_timestamp`
transform already gives for exactly this class of risk: one sender with a badly wrong clock must
not be able to write spans years away and quietly poison a trace store.

**`mint_id: false` by default; `logit` never mints a trace id.** A missing span id, with `span:`
configured and `mint_id: true`, is minted via the same SplitMix64 generator `logit`'s own internal
pipeline `TraceContext` already used — moved from `crates/logit-pipeline::fanout` into
`logit_core::trace` (`random_id_bytes`) now that it has a second caller, so exactly one module
decides what a fresh id looks like. A trace id is never minted here under any configuration: an
access log line's whole point is correlating to a trace that began somewhere else (or, at the true
edge, that the edge tier itself minted and is now forwarding as a header) — inventing one inside
`logit` when the line has none would silently fork the trace rather than report the gap. This is
the same "operator's explicit choice, never `logit`'s own initiative" posture
[ADR `log-record-trace-context`](log-record-trace-context.md) already established, applied to a
span id specifically.

**All-or-nothing per event, same as before.** Every attribute is parsed before anything is
mutated; a failure — missing trace id, an unparseable id or timing value, two forms of the same
timing quantity, a resolved span outside `max_skew` — leaves `event.attributes`,
`event.timestamp`, `event.log.trace`, and `event.span` exactly as they arrived, and counts one
`.skipped{reason}`. `reason` is `missing`, `invalid`, or, only with a `span:` block, `span_id`
(no id and no minting), `timing`, or `skew`.

**`keep_source: false` (the default) now removes every convention attribute the lift actually
read**, `traceparent` included — the same Loki-structured-metadata-collision reasoning
[ADR `log-record-trace-context`](log-record-trace-context.md) already gives for the original three
fields, extended to the rest of the convention.

## Producer timing model

Verified against the HAProxy 3.4 configuration manual, §8.4 "Timing events" and its §7.3 sample
fetch reference, and nginx's `$msec`/`$request_time` documentation — recorded here because
inter-span timing (not just each span's own duration) is the entire point of this feature, and
picking the wrong pair of fields silently produces a span that starts before it should or drifts
across hops.

HAProxy, HTTP mode (every named timer is milliseconds; the manual's own diagram):

```
  t(accept)   tr(request_date)                                  log emitted
  |--- Th ---|-- Ti --|-- TR --|-- Tw --|-- Tc --|-- Tr --|-- Td --|
                      |<---------------------- Ta ---------------->|
```

- `Ta` (`%Ta`, `txn.timer.total`): "total active time for the HTTP request, between the moment the
  proxy received the first byte of the request header and the emission of the last byte of the
  response body."
- `request_date([unit])` (`%tr`): "the exact date when the first byte of the HTTP request was
  received ... computed from accept_date + handshake time (%Th) + idle time (%Ti)," `unit` one of
  `s`/`ms`/`us`. Same anchor instant as `Ta`'s start — **the pair this ADR recommends is
  `span.start_us = request_date(us)`, `span.duration_ms = %Ta`.**
- `accept_date`/`%Ts`/`%t`/`%ms` are the *connection's* accept time, reset per request only to
  "the end of the previous response" on a keep-alive connection — it already includes `Th`+`Ti`
  (handshake and idle time), so a span anchored there would begin before the request that produced
  it existed. Not used.
- Precision caveat: `request_date`'s own doc says it derives from `accept_date` (seconds) plus
  `%Th`/`%Ti` (both millisecond timers), so its sub-millisecond digits are exact only when both
  are zero (the first request on a freshly accepted, non-TLS connection). `%Ta` is a millisecond
  timer outright. A haproxy-derived span is therefore millisecond-accurate in general, with a
  microsecond-precise *start* specifically on that common first-request case — `logit` preserves
  whatever precision arrives; it does not manufacture precision the source didn't have.
- `option logasap` must stay off: it prefixes `%Ta`/`%Tt` with `+` and reports them before the
  response body has actually finished, which would make every derived span's duration too short
  and, worse, inconsistently so.
- The finer-grained timers (`%TR`/`%Tw`/`%Tc`/`%Tr`/`%Td`) are worth logging as plain attributes
  (`haproxy.timer.request_ms`, etc.) even though this ADR doesn't turn them into a second span —
  see the Known Gaps entry below.

nginx (both millisecond-resolution): `$request_time` is "time elapsed since the first bytes were
read from the client" through to when the log line is written; `$msec` is that log-write instant
itself. The consistent pair is **`span.end_s = $msec`, `span.duration_s = $request_time`** — not
`$msec` as a start, which would place the span's start at the moment it *ended*.
`$upstream_connect_time`/`$upstream_header_time`/`$upstream_response_time` give the same kind of
breakdown haproxy's finer timers do, for the same not-built-here reason.

## Alternatives considered

- **A separate `span_from_log` component instead of extending `trace_context`.** Rejected: the two
  operations share every piece of parsing (ids, the `traceparent`), and an operator lifting a
  trace context onto a log while also wanting a span from the same line would otherwise configure
  two components reading the same attributes with two independent id-precedence rules to keep in
  sync.
- **Underscore names (`trace_id`, `span_id`, `parent_span_id`, ...) instead of dotted
  (`trace.id`, `span.id`, `span.parent_id`).** Rejected: the repo's own convention is dotted
  (`service.name`, `syslog.facility`, `otel.status_message`); underscore was only ever
  `trace_context`'s own original, one-off field-name choice, matching the JSON body example in
  [ADR `log-record-trace-context`](log-record-trace-context.md), not a house style to preserve.
- **Resolving a partial timing failure by falling back to receipt time for everything, rather than
  skipping the event.** Rejected: silently substituting a wrong instant for a span's start or end
  is worse than a visible, countable skip — the same "silently wrong is worse than visibly
  incomplete" instinct `docs/known-gaps.md`'s internal-spans section already states for a
  different case (picking an arbitrary contributing batch as a flush's parent).
- **Minting a trace id when none is present, not just a span id.** Rejected outright, not just
  deferred — see the Decision section; this would fork traces silently rather than report a real
  gap in propagation.
- **Deriving a second, CLIENT-side span for the upstream hop from the same timers this ADR reads
  anyway (haproxy's `%TR`/`%Tw`/`%Tc`/`%Tr`/`%Td`, nginx's `$upstream_*`).** Deferred, not
  designed: `Transform::process` is one-in-one-out, so a second span per line needs either a trait
  change or a second, upstream-flavored lift mode, and no concrete config needs it yet. Filed in
  `docs/known-gaps.md` with the exact derivation so it isn't re-discovered from scratch.

## Consequences

- `crates/logit-config`: `TraceContext`'s `trace_id`/`span_id`/`flags` fields gain defaults
  (`trace.id`, `span.id`, `trace.flags`); a config omitting them entirely is now valid and uses
  the convention. `span_id`/`flags` are disabled with `null`, not `""` — an empty string is
  rejected as a graph-validation error, same reasoning as an empty `trace_id`. New
  `SpanLiftConfig`/`SpanKindConfig` types, `#[serde(deny_unknown_fields)]` on the former. Schema
  regenerated (`script/schema`).
- `crates/logit-pipeline::graph`: rule 19 extended to the two new optional fields; new rule 25 for
  `span.name`/`span.max_skew`.
- `crates/logit-core`: `trace.rs` gains `parse_traceparent` and `random_id_bytes` (moved from
  `crates/logit-pipeline::fanout`, which now calls the core function); `time.rs` gains
  `parse_rfc3339_to_nanos` (hoisted from `crates/logit-inputs::syslog`, which now imports it) and
  `parse_decimal_nanos` (new). `syslog_in`'s own RFC 3339 acceptance widens from 6 to 9 fractional
  digits as a side effect of sharing the parser with `span.*_rfc3339` — strictly more permissive,
  never a rejection that used to succeed.
- No `Event`/`LogRecord`/`SpanRecord` field changes — everything here is attribute convention plus
  transform behavior. `Event`/`SpanRecord`/`TraceRef` sizes are unchanged; the allocation cost of a
  successful lift with a `span:` block stays at the same **1** allocation the log-only lift already
  had (`crates/logit-bench/tests/allocations.rs`'s `trace_context_mints_a_span_from_the_convention`).
- Lua gains no new capability here — `event.has_span` remains the entire span surface. Recorded as
  its own `docs/known-gaps.md` entry rather than folded into the existing trace-context one, since
  the constraint (no accumulator, no proxy type) is different from that entry's.
- `docs/plans/demo-tracing-stack.md`'s workstream A description of `logit` having no `traceparent`
  parser, and its per-tier decimal-flags-JSON-fields workaround, are now historical rather than
  current — a follow-up note there points here rather than rewriting that plan's own account of
  what it built at the time.
