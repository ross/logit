---
created: 2026-09-25
updated: 2026-10-25
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

- The interner's posture ([`memory.md`](../design/memory.md#what-feeds-it)'s "What feeds it")
  accepts a never-evicting interner on the premise that listeners are private, and cited no
  decision for that premise.
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
  `docs/known-gaps/`, citing this ADR.
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
- Every non-goal in `docs/known-gaps/` that rests on this premise cites this ADR, so the revisit
  trigger can find them all.
- `docs/deploying.md` tells operators to keep listeners behind their own trust boundary.

## Amendment (2026-10-25): a credential is one of the operator's tools

[ADR `listener-authentication`](listener-authentication.md) gives `otlp_in`, `prometheus_in`'s
remote-write receiver, and `prometheus_out`'s exposition static bearer and basic credentials,
beside the protocol tokens `datadog_in`, `splunk_hec_in`, and `influxdb_in` already check. The
trust boundary is still the operator's: a credential decides who may send, and it doesn't make a
listener fit to face the internet. The bar for what a listener survives stays accidental data,
and an authenticated peer is held to it, not trusted further. The revisit trigger is unchanged,
because a credential on a public endpoint doesn't defend the non-goals recorded against this ADR
from a peer that holds one.

## Amendment (2026-10-25): the non-goals are listed here, not in the known-gaps lists

`docs/known-gaps/` holds only rough edges that are still open, and a non-goal this ADR decides
isn't one. So a non-goal is recorded in an ADR or design doc, not in `docs/known-gaps/`, and this
list replaces the decision's "recorded as a documented non-goal in `docs/known-gaps/`" and the
consequence that each such entry cites this ADR. The revisit trigger reopens every item below.
[`docs/deploying.md`](../deploying.md)'s "Trust boundary" section carries what an operator needs.

- **The interner never frees.** A string in key or metric-name position that never repeats stays
  for the life of the process. [`docs/design/memory.md`](../design/memory.md)'s "Interning: the
  bargain, and its bounds" states the bargain and lists every component that feeds the interner a
  peer's or a script's strings, among them `logit_in` interning a dictionary from a batch it then
  rejects ([ADR `untrusted-input-bounds`](untrusted-input-bounds.md)'s "Documented non-goals").
- **Lua-authored telemetry goes through the interner.** A script that builds a metric name or tag
  value from event data grows it one entry per value; a disabled telemetry handle costs nothing,
  whatever the script passes
  ([ADR `lua-authored-telemetry-cardinality`](lua-authored-telemetry-cardinality.md)).
- **A Lua script has no time bound, and its residuals stand.** Interner growth from
  script-derived strings, a shared-table DAG converting at 2^k nodes, a 128-deep value that
  doesn't survive `otlp_out -> otlp_in`, recursion through Rust/C frames past the 8 MiB stack, an
  `Event.new` loop that discards what it builds, and `max_memory` counting the Lua heap only
  ([ADR `lua-runaway-script-bounds`](lua-runaway-script-bounds.md)'s decision 3 and
  Consequences), and not the machine code `jit: true` compiles, up to 512 KB per VM
  ([ADR `lua-jit-opt-in`](lua-jit-opt-in.md)). `docs/design/lua-api.md`'s "Limits and costs" is
  the operator's account.
- **A stalled or dribbled body holds its connection permit.** A body read's only time bound is a
  per-frame stall bound, and that bound is the listener's `idle_timeout`, off by default
  ([ADR `untrusted-input-bounds`](untrusted-input-bounds.md)'s "A total body deadline";
  [ADR `idle-connection-timeout`](idle-connection-timeout.md)'s 2026-09-25 amendment). With enough
  connections, such a peer holds the whole `max_connections` cap. If the trigger fires, the
  candidates are a total deadline, a minimum transfer rate, or a per-peer connection cap, each
  worth its false positives only on an untrusted network.
- **A native attribute map with keys in descending dictionary order inserts in quadratic time.**
  `read_attr_map` inserts each key into `AttrMap`'s sorted storage as it reads it. Measured:
  80,000 keys take 3.4 s in descending order against 20 ms ascending, and real maps are far too
  small to show it. The fix, collecting the keys and sorting once, changes the ordinary decode
  path for a shape only crafted input produces
  ([ADR `untrusted-input-bounds`](untrusted-input-bounds.md)'s "Documented non-goals").
- **An OTLP/JSON body of crafted tiny objects peaks at about 98 bytes of heap per input byte.**
  Ordinary OTLP/JSON (`testdata/interop/otlp/logs.json`, a real SDK export) peaks at about 19, and
  the same batch as protobuf at about 17 (debug build, measured 2026-09-25), because the JSON path
  builds a `serde_json::Value` tree first. At 4 MiB the crafted shape is about 400 MiB
  for one request. `crates/logit-proto/tests/robustness.rs`'s
  `otlp_json_peak_memory_per_input_byte_is_documented` pins ceilings of 24 and 128, and no cap or
  streaming parser is built
  ([ADR `untrusted-input-bounds`](untrusted-input-bounds.md)'s "Documented non-goals").
- **A decoded sketch's or HyperLogLog's summary fields are taken as written.** `from_bytes`
  bounds what a blob can allocate and rejects a zero-register count past the register count, and
  trusts the rest. None of these panics: a `DdSketch` with `min > max` answers non-monotonic
  quantiles; an infinite or `NaN` `min` or `max` lets a quantile answer `±∞`; `merge` skips a
  sketch whose count and zero count are both 0 over populated bins, without a counter; and a
  `HyperLogLog` with a wrong harmonic sum estimates wrong. No encoder `logit` ships writes any of
  these, and recomputing a summary on decode wouldn't reproduce the bytes
  ([ADR `untrusted-input-bounds`](untrusted-input-bounds.md)'s "Documented non-goals"). A real
  producer's sketch carrying bins under a zero count would reopen the `merge` case.
- **No cap on syslog STRUCTURED-DATA elements or PARAMs per line.** `parse_structured_data` reads
  a line, bounded by the 64 KiB frame cap or the datagram, in one forward pass, but folding names
  into `AttrMap`'s sorted map is quadratic in distinct names arriving in an order the interner
  hasn't seen: a 64 KiB line of about 10,000 distinct SD-IDs costs about 17 ms in a release
  build. No real sender writes that many, so a count cap would stop only crafted input.
  `crates/logit-proto/tests/robustness.rs`'s syslog section pins it.
- **`forwarded:` trusts the header it names.** A client that reaches a listener or a web server
  past the proxy, or through a proxy that appends to its header, can name any address as
  `client.address` ([ADR `forwarded-header-parsing`](forwarded-header-parsing.md)). If the trigger
  fires, the fix is a list of trusted proxy addresses with the rightmost untrusted entry taken as
  the client. `proxy_protocol:`'s missing allowlist of trusted PROXY sources isn't part of this
  non-goal: it's deferred work in `docs/known-gaps/intake.md`.
