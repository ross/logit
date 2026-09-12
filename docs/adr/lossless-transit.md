---
created: 2026-09-10
updated: 2026-09-12
---

# Lossless like-protocol transit: the internal model is a superset of every supported wire protocol

## Status
Accepted

Realized as of 2026-09-12: W1-W7 closed every named loss this ADR requires closed for like-to-like
transit; see [`docs/plans/lossless-transit.md`](../plans/lossless-transit.md)'s closing assessment.

## Context

[ADR `statsd-output`](statsd-output.md) shipped `statsd_out` with a v1 metric-kind deferral: a
`statsd_in -> aggregate -> statsd_out` relay drops every timer metric today, because the aggregator's
merged `DdSketch` no longer holds the samples it combined and "how does a merged sketch become
statsd lines" was left as an open design question. Working through that question surfaced a bigger
one `logit` has never answered directly: **should an operator be able to point `statsd_in` at
`statsd_out`, `otlp_in` at `otlp_out`, or `syslog_in` at `syslog_out`, and trust that nothing
between them was quietly dropped?** Today the honest answer is no for all three pairs, and each gap
was accepted independently, one sink at a time, with no shared principle saying whether that's fine
or debt.

The nearest existing statement of intent is [ADR `native-wire-format-with-otlp-bridge`](native-wire-format-with-otlp-bridge.md)'s
Consequences: "the internal event model must be a superset of what OTLP can express, or the OTLP
codec becomes lossy." That claim is scoped to OTLP alone, and [ADR `committed-pregenerated-otlp-protobuf`](committed-pregenerated-otlp-protobuf.md)
already had to qualify it once `MetricKind::Distribution` → OTLP `Summary` and `MetricKind::Set` →
skip showed the loss can run in the *other* direction too — `logit`'s own model, not OTLP's, can be
the narrower one.

`docs/known-gaps.md` separately catalogs a dozen losses that are each individually "deliberate,
already-identified... accepted": `statsd_out` drops `Distribution`/`Set`/`Histogram`/`Summary`
entirely; `syslog_out` emits no RFC 5424 STRUCTURED-DATA and re-stamps the relayed timestamp; the
"Cross-protocol semantic gaps" table lists `Distribution`→`Summary` degradation, `Set` skip,
`U64`/`Timestamp` collapse to `I64`, and more. None of these is wrong on its own terms — every one
is counted and documented, per this project's own conventions — but nothing says which of them are
acceptable forever versus which are a stated goal not yet met. This ADR settles that.

## Decision

**Like-to-like transit is lossless.** For every protocol P with both a `P_in` and a `P_out`, a
pipeline `P_in -> P_out` — with no transforms, or only transforms that don't themselves summarize —
produces output that a consumer speaking P cannot distinguish from the original input, modulo the
permitted normalizations below. Concretely and immediately in scope: `statsd_in -> statsd_out`,
`otlp_in -> otlp_out`, `syslog_in -> syslog_out`.

**Permitted normalizations** — the things a relay may do and still count as lossless, so a
round-trip test can assert equality against a concrete expectation rather than "close enough":

- Batching/regrouping, and reordering within a batch.
- Splitting a multi-value statsd line (`a:1:2:3|c`) into several lines, or the reverse.
- Summing or merging increments/samples belonging to the same series (a counter's value, or two
  same-series metrics landing on one event).
- Attribute/tag reordering — `AttrMap` is already sorted by interned key, so this costs nothing to
  state.
- Number formatting and escaping that the protocol's own grammar declares equivalent.
- Sanitizer substitutions required for injection safety (an embedded newline in a syslog message,
  a `:`/`|`/`\n` in a statsd name or tag) — safety wins over byte-identity here, and this project
  already treats it that way.
- A sink-configured dialect change the operator explicitly asked for (RFC 3164 in, RFC 5424 out;
  plain statsd in, DogStatsD out) — the operator chose to normalize, so this isn't loss.
- Counter sample-rate folding: `hits:1|c|@0.1` arriving as `Counter(10)` and leaving as `hits:10|c`
  is permitted — a statsd counter has no wire concept of "this was extrapolated." **Timer/histogram
  sample rate is not folded away**: the raw value and its `@rate` both survive until something
  chooses to summarize (see below), because unlike a counter's value, a timer's individual samples
  are real values a lossless relay has to be able to hand back one-for-one.

**The internal model is a superset of every supported protocol's data model, not only OTLP's.**
A field or semantic a protocol can carry that `Event`/`EventBatch` cannot represent at all is a
model gap, tracked as debt against this ADR (in [`docs/plans/lossless-transit.md`](../plans/lossless-transit.md)
and `docs/known-gaps.md`), not something a codec is free to accept quietly. This generalizes and
amends `native-wire-format-with-otlp-bridge`'s Consequences from "must be a superset of OTLP" to
"must be a superset of every protocol this project ships an `_in`/`_out` pair for."

**Summarization is opt-in and named.** Only a component whose stated purpose is to summarize —
`aggregate`, or a Lua script doing the equivalent — may discard information that a lossless relay
would otherwise have to preserve. A codec (a `Decoder`/`Encoder` implementation) never summarizes on
its own, and a decoder never pre-summarizes what a later, explicit stage should be the one to
decide about. Concretely: statsd `ms`/`h`/`d` values and `s` set members stay in the model as raw
observations after decode; only `aggregate` turns them into a sketch or a cardinality estimate, and
only when asked to.

**A concept the model can't represent at all becomes a typed field; a protocol's own raw encoding
of something the model already normalizes rides as a protocol-namespaced attribute that outranks
the normalized field on that protocol's own egress.** Two existing conventions, both already load-
bearing, generalized into one rule:

- (a) OTLP's aggregation temporality, monotonicity, exemplars, `event_name`, span `trace_state` and
  `flags`, and a status message have no field on today's `Event`/`MetricRecord`/`SpanRecord` at
  all — each is a genuine model gap, not something to smuggle onto an attribute where it would leak
  into every other sink's tag set the way `otel.status_message` does today.
- (b) A protocol's own raw encoding of something the model already normalizes rides as a
  protocol-namespaced attribute that outranks the normalized field on that protocol's own egress —
  already the rule `syslog_in`/`syslog_out` follow for severity: [ADR `syslog-output`](syslog-output.md)
  ("Header-field precedence") has `syslog.severity` deliberately outrank the normalized
  `log.severity` on `syslog_out`, precisely because `syslog_in`'s PRI-to-`Severity` mapping is lossy
  by construction. The same shape applies to OTLP severity (24 raw levels collapsing onto `logit`'s
  6): `otlp_in` stamps `otel.severity_number`/`otel.severity_text` attributes that `otlp_out`
  prefers over the normalized `Severity`, the same way `syslog.severity` already works.
- (c) `logit.*`-prefixed attributes stay reserved for a `logit`-specific concept crossing a foreign
  wire with nowhere else to go (`logit.body_format` is the standing example) — not for a concept two
  protocols both need, which belongs in (a) instead. `otel.temporality` is retired under this rule
  once temporality is a real field (see the companion plan); `otel.status_message` is retired once
  span status message is a real field.

**Cross-protocol egress stays best-effort, but every degradation is counted and documented.**
`P_in -> Q_out` for two different protocols may still degrade — a raw sample list has no OTLP
metric type, a DDSketch has no statsd wire form — and that is explicitly *not* what this ADR
requires to be lossless. What it does require: every such degradation increments a
`logit.output.metrics.{degraded,skipped}`-style counter (the existing convention), is named in the
receiving codec's own module doc, and appears in `docs/known-gaps.md`'s cross-protocol table. Loss
is acceptable exactly there, and nowhere else.

**Round-trip fixed point is the test that proves this, not a design review.** Each like pair gets a
`decode -> encode -> decode` equality test over a fixture corpus that exercises every wire feature
[`docs/design/telemetry-landscape.md`](../design/telemetry-landscape.md) lists for that protocol. A
wire feature with no fixture is a feature this ADR doesn't yet cover; a new one lands with its
fixture, not after.

## Alternatives considered

- **Leave every gap as an independently accepted `known-gaps.md` entry (status quo).** Rejected:
  each new sink re-litigates the same question from scratch (this ADR exists because `statsd_out`
  just did), and nothing stops the list from growing indefinitely in a direction no one chose on
  purpose. An operator relaying statsd through `logit` has a reasonable expectation of fidelity that
  the current state doesn't meet and doesn't say it's trying to meet.
- **Make OTLP the internal model.** Already rejected in `native-wire-format-with-otlp-bridge` for
  performance and log-model-fit reasons; also fails this goal directly, since OTLP itself cannot
  carry a mergeable sketch, statsd's relative-gauge delta, raw unaggregated samples, or syslog
  structured data without its own lossy workarounds.
- **Raw wire passthrough** (a `syslog.raw`/`statsd.raw` `Value::Bytes` holding the original
  datagram or line, alongside the normalized model). Byte-faithful by construction, but rejected
  even as a complement: it goes stale the instant any transform touches the event's normalized
  fields, doubles memory for every event that carries it, and answers a different question
  ("what bytes arrived") than the one this ADR is about ("does the *information* survive"). A
  transform-derived event has no raw bytes to begin with, so this couldn't be the general
  mechanism regardless.
- **Best-effort-with-counters everywhere, including like-to-like.** This is what exists today.
  Rejected as the permanent state for exactly the reason in Context: it's indistinguishable from
  not having tried, from the perspective of an operator who expected a relay to be transparent.
  Kept, deliberately, for the cross-protocol case, where an exact mapping frequently doesn't exist.

## Consequences

- `MetricKind`, `MetricRecord`, `LogRecord`, and `SpanRecord` are reshaped to close the gaps this
  ADR names as debt rather than accepted loss — the concrete target shapes, sizes, and ordered
  workstreams are in [`docs/plans/lossless-transit.md`](../plans/lossless-transit.md), informed by
  the field-by-field survey in [`docs/design/telemetry-landscape.md`](../design/telemetry-landscape.md).
  `Event`'s exact-size test (`crates/logit-core/tests/type_sizes.rs`) and
  [`docs/design/memory.md`](../design/memory.md)'s table move as a result — expected and accepted,
  per [AGENTS.md](../../AGENTS.md)'s "when one fails, that's the test working."
- The native wire format (`crates/logit-proto/src/native/`) has to carry whatever the model gains;
  since `logit` is pre-release, this is a straight reshape of the record framing, not a version
  negotiation or a dual-read compatibility path.
- `docs/known-gaps.md`'s statsd/syslog/cross-protocol entries this ADR covers are reclassified from
  "accepted" to "tracked debt against this ADR," not removed — nothing is fixed by writing this
  record.
- `statsd-output`, `aggregation-window-semantics`, `syslog-output`, and `relative-gauge-adjustments`
  each get an amendment when their corresponding workstream lands, the same way
  `aggregation-window-semantics` already carries two amendments from later decisions.
- The OTLP integration test's current fidelity check
  (`crates/logit-cli/tests/otlp_round_trip.rs`'s `assert_round_tripped`) only checks that a log, a
  metric, and a span each exist with roughly the right shape — it becomes a per-field fixed-point
  assertion once the model changes it's meant to protect exist.
