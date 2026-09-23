---
created: 2026-09-22
updated: 2026-09-22
---

# `sample`: consistent, keyed event sampling with an operator override

## Status
Accepted. Supersedes, in part, [ADR `routing-by-condition-is-lua`](routing-by-condition-is-lua.md):
its `sample` clause only. That ADR's core holding — no native predicate language, no
`filter`/`rename`/`throttle`/`dedup` kinds — stands unchanged.

## Context

`logit` has no way to keep a fraction of a stream. [ADR
`routing-by-condition-is-lua`](routing-by-condition-is-lua.md) retired the unimplemented
`ComponentKind::Sample` on the reasoning that per-event sampling is `math.random() < rate` in a
`lua` component — one line, no new kind. That reasoning is correct for *random* sampling and wrong
for the sampling that actually matters once traces are in the picture.

A trace is many spans, emitted by many services, arriving through many listeners — and, in the
split-collection topology `docs/OVERVIEW.md` describes, through many `logit` processes. Keeping
10% of *spans* at random is worthless: nearly every surviving trace is missing most of its spans.
Keeping 10% of *traces* — every span of a chosen trace, none of an unchosen one — requires that
every sampler that sees any part of the trace reach the same verdict independently, with nothing
propagated between them. The standard construction is to hash a key shared by every event of the
group (the trace id) to a uniformly distributed value and compare it against the rate: same key,
same hash, same verdict, in every process, with no coordination. OTel's `TraceIdRatioBased`
sampler is this construction, and `logit` already uses it for its *own* spans — `trace_is_sampled`
(`crates/logit-core/src/telemetry.rs`, [ADR
`internal-span-emission-and-deterministic-sampling`](internal-span-emission-and-deterministic-sampling.md))
takes the top 53 bits of a trace id's low 8 bytes and compares them against `rate · 2^53`, so every
`logit` process in a chain keeps the same internal traces without a propagated bit.

`lua` cannot express this. The scripting surface (`docs/design/lua-api.md`) exposes no hash
function; LuaJIT's `math.random` is seeded per VM, so two `lua` nodes — let alone two processes —
never agree; and a hand-rolled hash in Lua is per-event script work of exactly the kind
`routing-by-condition-is-lua` accepted paying for *conditions* because the alternative was a
predicate grammar. Here the alternative is one fixed function. The retiring ADR's revisit trigger
was "sustained central-collector throughput pressure"; this is not that trigger firing a fourth
time — it is the retiring premise (that Lua already does the job) being false for this one kind.

Two more requirements come with the use case. An operator debugging a specific request wants
*that* request's events to survive whatever the rate is — a flag on the event that the sampler
honours unconditionally, the only way to make a 1% sampler testable by hand. And a sampled stream
is rarely homogeneous: a log line without a trace id sitting in a trace-keyed stream needs a defined
fate, not an accident.

## Decision

**`sample` is a native `Transform` kind**, and the `sample` clause of
`routing-by-condition-is-lua` is superseded by this ADR. Config:

```yaml
sampled:
  type: sample
  sources: [spans_in]
  rate: 0.1                    # fraction kept, in [0, 1]
  key: trace_id                # optional: trace_id | {attribute: <name>} | {resource: <name>}
  missing: random              # optional, only with key:. random (default) | keep | drop
  always_keep:                 # optional: kept unconditionally when the field is present
    attribute: sampling.keep   #   exactly one of attribute: / resource:
    value: true                #   optional literal; absent means any value
```

**Evaluation order, per event: `always_keep`, then the key, then the rate.**

1. If `always_keep` names a field the event (or its resource) carries — and, when `value:` is
   given, carries with that value, under `has_attributes`' equality rules (numeric coercion across
   `I64`/`U64`/`F64`/`Str`, no coercion for `Bool`) — the event is kept. The override is a
   per-event decision, not propagated: an event flagged on one leg is kept on that leg only.
2. With `key:` set, the value at the key is canonicalized to bytes and hashed (below); with no
   `key:`, a per-instance counter mixed through the same hash stands in for a random draw. A
   configured key the event doesn't carry — no span, no `log.trace`, no such attribute, or a
   `Null`/`Array`/`Map` value there — falls to `missing:`: `random` (the default) draws as if no
   key were configured, `keep` forwards, `drop` drops.
3. The event is kept iff `(h >> 11) < (rate · 2^53) as u64`, the same 53-bit-exact compare
   `trace_is_sampled` already uses, moved to `logit_core::sampling::keep` so both callers share it.

**`key: trace_id`** reads `event.span.trace_id`, else `event.log.trace.trace_id` (the `TraceRef` of
[ADR `log-record-trace-context`](log-record-trace-context.md)), so a stream where `trace_context`
lifted some events to real spans and left others as logs with a trace reference samples both by the
same trace. A trace id is canonicalized to its 32 lowercase hex characters before hashing, so
`{attribute: trace_id}` on an *unlifted* hex-string attribute reaches the same verdict as
`trace_id` on the lifted event — the two spellings of the same trace agree by construction.

**Canonical bytes of a `Value`** are: `Str`/`Bytes` as-is; `I64`/`U64` as decimal text; `F64` as
Rust `Display` (`200`, `0.5`, `-0`); `Bool` as `true`/`false`; `Timestamp` as decimal nanoseconds;
`Null`/`Array`/`Map` treated as missing. So `I64(200)`, `U64(200)`, `F64(200.0)`, and `Str("200")`
hash identically — a decoder's choice of numeric type never changes a verdict, matching [ADR
`kv-metrics-semantics`](kv-metrics-semantics.md)' identity commitment. Case is *not* folded: an
uppercase-hex trace id in an attribute does not agree with a lifted one (W3C `traceparent` mandates
lowercase, so that disagreement only arises for a producer already off-spec).

**The hash is XXH64, seed 0, from the `twox-hash` crate, and it is frozen.** The hash function,
its seed, and the canonicalization table above are a cross-process contract: two `logit` versions
in one topology must reach the same verdict for the same key, so changing any of the three is a
wire-breaking change, gated by pinned test vectors in `logit_core::sampling`, and gets its own ADR.
`twox-hash` is pure Rust, MIT, on `deny.toml`'s allowlist, and already the workspace's shape of
dependency (a named, stable algorithm rather than one whose output is allowed to change between
releases).

**Rate edges follow the graph's "a config that can only ever be a no-op is an error" precedent**
(rules 7, 12, 54, 59): `rate: 1` is rejected, and `rate: 0` is rejected *unless* `always_keep` is
set — `rate: 0` with an override is the "only flagged events" debugging mode, and worth having.
Non-finite and out-of-`[0, 1]` rates are rejected the way `span_sample_rate`'s are (rule 16).
`missing:` without `key:` is rejected as meaningless.

**The internal-span sampler is untouched.** `trace_is_sampled` keeps taking raw trace-id bits with
no hash — those ids are `logit`'s own pipeline trace ids, random by construction and never an
application's — and `sample` hashes canonical text. The two reach different verdicts for the same
16 bytes, on purpose: an operator's `sample` at 10% and `internal`'s `span_sample_rate` at 10%
are sampling different populations and owe each other nothing.

**Telemetry**: `logit.transform.events.filtered` (the filter family's shared counter) and
`logit.transform.sample.decisions{outcome="kept"|"dropped", by="key"|"random"|"override"|"missing"}`,
tallied in integers per event and emitted once per batch from `end_batch`, since this transform
exists for the central-collector rates where per-event telemetry calls are the cost. No
`Diagnostics`: nothing here can fail.

## Alternatives considered

- **Keep sampling in `lua`, as retired.** `math.random() < rate` is per-VM and per-process; it
  cannot keep whole traces. Rejected because the premise the retirement rested on doesn't hold for
  keyed sampling.
- **Expose a hash function to Lua instead of a component.** Would make consistent sampling
  *possible* in a script at Lua's per-event cost (9 allocations and 1.61 µs versus 1 and 525 ns —
  `routing-by-condition-is-lua`'s own table), on a stream where a sampler is by definition the
  node every event passes through. And it is a second API surface for the same decision. Rejected;
  a Lua-side hash may still be worth adding later for other reasons, and nothing here precludes it.
- **Take raw bits from the key like `trace_is_sampled` does, no hash.** Correct only for a value
  that is already uniformly random — true of a spec-compliant trace id, false of every other key
  (`request_id: "{seq}"`, a `service.name`, a numeric attribute). One rule for every key is worth
  one hash over sixteen bytes.
- **Propagate a sampled bit instead of hashing.** Already rejected for internal spans by
  `internal-span-emission-and-deterministic-sampling`, for the same reasons: there is nothing to
  propagate *through* across `logit_in`/`logit_out` hops that isn't a wire-format change, and it
  doesn't help two listeners in one process that never see each other's batches.
- **OTEP 235 / W3C `tracestate` `th:` consistent-probability sampling.** The emerging OTel
  standard encodes the threshold in `tracestate` and compares it against the trace id's low 56
  bits, so an SDK sampler and a collector sampler agree and downstream can recover the sampling
  probability. Worth interoperating with eventually; deferred, because it only applies to the
  `trace_id` key (nothing else has a `tracestate`), `logit`'s compare uses different bits (top 53
  of the low 64, inherited from `trace_is_sampled`), and adopting it means committing to a
  specific bit convention this ADR would rather not freeze on a first pass. Tracked in
  `docs/known-gaps.md`.
- **Hash choices.** `std`'s `DefaultHasher` documents that its algorithm may change between Rust
  releases — fatal for a cross-version contract. `ahash` says the same of itself. `xxhash-rust`
  is BSL-1.0, not on `deny.toml`'s allowlist. A hand-rolled FNV-1a with a finalizer would be
  stable by construction and dependency-free, but "our own 20-line hash" is a weaker story than a
  named algorithm with published test vectors when the property being sold is agreement between
  binaries. XXH64 via `twox-hash` (MIT) it is.
- **A `percent:` field instead of `rate:`.** `internal` already spells the same idea
  `span_sample_rate: 0.1`; one convention.

## Consequences

- `ComponentKind::Sample` returns, implemented in the same PR (never as an unimplemented
  placeholder — `routing-by-condition-is-lua` retired it precisely to stop the schema advertising
  what the binary can't run). `logit-core` gains a `sampling` module and its first hashing
  dependency, `twox-hash`.
- **The hash contract is frozen.** XXH64 seed 0 over the canonicalization table above, pinned by
  test vectors. A change is a wire-breaking change between `logit` versions and needs its own ADR.
- `routing-by-condition-is-lua`'s Status, Context, and Decision carry "superseded in part" markers
  pointing here. `filter`, `rename`, `throttle`, and `dedup` stay retired; `docs/known-gaps.md`'s
  entry narrows to throttling, dedup, and operator-shaped conditions.
- `always_keep` is per leg. An operator who wants a flagged request kept end to end places the
  same override on every sampler in the path — there is no propagated bit, by the same reasoning
  that there is none for the rate. Recorded in `docs/known-gaps.md`.
- No OTEP 235 interop; recorded in `docs/known-gaps.md` with the bit-convention difference, so a
  future adoption knows what it is changing.
- `sample` is a per-event decision and never looks at a batch, so a resource-keyed sampler keeps
  or drops every event of a resource — which is the point of keying on one.
- [`examples/sample-traces.yaml`](../../examples/sample-traces.yaml) is the runnable shape:
  `generate_in` → `trace_context` → `sample` (`key: trace_id`, an `always_keep` flag) →
  `stdio_out`, so "the same traces survive on every run, every span of each" can be seen rather
  than trusted. `demo/` is unchanged: it keeps every trace on purpose.
