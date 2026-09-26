---
created: 2026-09-25
updated: 2026-09-25
---

# Deployment threat model: accidental data inside a trust boundary, not a malicious peer

## Status
Accepted

## Context

`logit` is a relay and multiplexer. It sits between the producers that send it telemetry and the
backends it forwards to, on the operator's private network: a sidecar, a host agent, or a central
aggregator that other `logit` nodes and agents forward to ([OVERVIEW.md](../OVERVIEW.md)). Its
listeners are never exposed to an untrusted network.

How far a decoder or listener should defend itself has been decided case by case until now:

- `docs/known-gaps.md`'s interner entry accepts a never-evicting interner on the premise that
  listeners are private, and cited no decision for that premise.
- [ADR `otlp-compression-and-decompression-bounds`](otlp-compression-and-decompression-bounds.md)
  caps a decompressed body at the compressed-body limit, the "compression bomb" case, without
  saying whose bomb it defends against.
- [ADR `untrusted-input-bounds`](untrusted-input-bounds.md) had to settle the question again for
  every decoder and listener a peer can reach.

Each time, the same question came up in a different form. This ADR answers it once for the project.

## Decision

- **The bar is accidental data.** Every listener, decoder, and transform must handle a
  misconfigured or buggy sender, a producer `logit` hasn't seen before, a wedged peer, and a
  corrupt file. No such input may crash the process, make it allocate without bound, or corrupt
  what it relays.
- **Crafted input is defended only when the defense is free.** A problem that only crafted input
  can trigger is defended when the defense is a branch, a counter, or a timeout wrapper, with no
  hot-path or complexity cost. Otherwise it is recorded as a documented non-goal in
  `docs/known-gaps.md`, citing this ADR.
- **The operator owns the trust boundary.** Network policy, TLS with client certificates, and a
  proxy in front of a listener are the operator's tools for keeping untrusted peers out, not
  `logit`'s. [`docs/deploying.md`](../deploying.md) says so.
- **Revisit trigger.** A listener that stops being private, such as a public or multi-tenant
  ingest endpoint or a hosted aggregator, reopens every non-goal recorded against this ADR,
  starting with the interner and the compression amplifiers.

## Alternatives considered

- **Defend every listener as if it faced the internet.** Rejected. It puts a cost on every hot
  path, and more code on every decoder, for a deployment shape the project doesn't target.
- **A per-listener "untrusted" mode.** Rejected for now: two code paths per decoder to keep
  correct and tested, for a use case with no user today. It may return with the revisit trigger.

## Consequences

- A reviewer asks two questions of a proposed bound: can accidental data reach this, and is the
  defense free? A yes to the first means the bound is required. A no to the first and a no to the
  second means a known-gaps entry instead of code.
- Every non-goal in `docs/known-gaps.md` that rests on this premise cites this ADR, so the revisit
  trigger can find them all.
- `docs/deploying.md` tells operators to keep listeners behind their own trust boundary.
