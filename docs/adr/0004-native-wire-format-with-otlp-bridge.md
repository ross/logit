# 0004 — Service-to-service protocol: native wire format, OTLP as a bridge

## Status
Accepted

## Context
`logit` is designed to let collection and processing run on separate nodes (an edge collector
forwarding to a central processor, for example). That link needs to be efficient — it's the
project's most differentiating piece of architecture — while `logit` also needs to interoperate with
the existing OpenTelemetry ecosystem at its edges.

## Decision
Design a compact native frame format for `logit`-to-`logit` hops (see
[docs/design/wire-protocol.md](../design/wire-protocol.md)), and support OTLP as a first-class
ingest/egress **codec** alongside statsd, syslog, etc. — not as the internal transport.

## Alternatives considered
- **OTLP/protobuf everywhere**, as both the internal model and the inter-node wire format. Zero
  interop work and an existing published spec. Rejected as the *primary* transport: protobuf
  encode/decode on every hop is real overhead where both ends are `logit` and full control is
  available, and OTLP's log data model doesn't fit arbitrary structured/unstructured logs well.
  Kept as a first-class *codec* for interop, since that need is real.
- **Native only, no special-cased OTLP.** Keeps the core maximally simple. Rejected because OTLP
  interop is a stated scope requirement, not a nice-to-have, and treating it as just another codec
  (per [docs/design/data-model.md](../design/data-model.md)) costs little.

## Consequences
- Two format concerns to maintain: the native frame format for efficiency, and an OTLP codec for
  interop. Both are pluggable codecs against the same internal event model, so this is additive
  complexity, not duplicated logic.
- The internal event model ([docs/design/data-model.md](../design/data-model.md)) must be a superset
  of what OTLP can express, or the OTLP codec becomes lossy.
