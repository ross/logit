# 0019 — Lua-authored telemetry: cardinality is convention-enforced, not type-system-enforced

## Status
Accepted

## Context

ADR 0018 built `logit_core::telemetry::Telemetry` around one hard rule: every metric name and tag
is `&'static str` — a compile-time-constant string, not a runtime value. That's what bounds
cardinality by *code* rather than by traffic, and it matters more here than in most systems: the
process-wide attribute interner (`logit_core::interner`) never evicts (`docs/known-gaps.md`), so a
runtime-derived string reaching it leaks for the life of the process.

Extending telemetry to Lua scripts (`crates/logit-script/src/telemetry.rs`, letting a `process()`/
`flush()` call `telemetry.count(...)`/`.gauge(...)`) runs straight into that rule: a Lua script
hands over an `mlua::String`, not a Rust string literal. There is no way to prove at compile time
that a value crossing the Lua/Rust boundary is `'static` — the type system's enforcement mechanism
simply doesn't reach across that boundary.

## Decision

**Round-trip every Lua-provided name and tag value through the existing interner**:
`interner::resolve(interner::intern(s))` genuinely returns a `&'static str` — the interner's own
permanent storage — and re-interning a string it already holds allocates nothing (measured and
documented in `docs/known-gaps.md`'s interner section). This satisfies `Telemetry`'s type signature
using infrastructure the codebase already has and already accepts the tradeoff on, rather than
building a second, bespoke leak mechanism just for this path.

**The honest consequence: cardinality safety for Lua-authored telemetry is convention-enforced,
not type-system-enforced.** A script that writes `telemetry.count("orders.total", 1)` — a fixed
literal, called repeatedly — costs one interner entry, ever, exactly like any Rust call site. A
script that writes `telemetry.count(event.attributes.order_id, 1)` compiles and runs fine, and
leaks one interner entry per distinct order id, forever. Nothing in the type system distinguishes
these two cases the way it does for Rust code, where only a `&'static str` literal type-checks at
all. This is the same status `kv_metrics`'s config-driven `MetricSpec.name: String` already has —
bounded in practice because it comes from config, not event data, but bounded by where it comes
from, not by what the compiler will accept.

## Alternatives considered

- **Restrict Lua telemetry to metric names only, no tags.** Removes half the surface area for
  misuse, at real cost to what the capability is actually for — most of the motivating cases
  (`telemetry.count("orders.total", n, {status = "completed"})`) want a dimension, not just a
  count. Rejected: the interner round-trip already solves the *name* problem, and a tag value is no
  more dangerous than a tag key or a metric name once that mechanism is in place — restricting only
  tags doesn't remove the underlying risk, it just narrows where a script author would hit it.
- **A separate, bounded cache instead of the process-wide interner** (e.g. a fixed-size LRU per
  `ScriptWorker`, evicting old entries). Would give a real cap instead of an accepted-but-unbounded
  tradeoff. Rejected for now: it's a second cardinality-bounding mechanism to build, test, and
  reason about, when the codebase already has one it accepts the growth characteristics of; revisit
  if Lua-authored telemetry in practice turns out to need a harder bound than "the same thing the
  rest of the system already lives with."
- **Reject any Lua telemetry call whose name/tag wasn't already interned** (i.e., require a
  script's metric names to be declared somewhere ahead of time, like `kv_metrics`'s config-time
  `MetricSpec` list). Rejected: it would mean a script can't emit a metric it decides to add without
  a corresponding config change, defeating the actual value of exposing this to Lua at all — the
  point is a script author can add a metric the moment they realize they want one.
- **Do nothing — no Lua-facing telemetry API.** Was the state before this ADR. Rejected because it
  was explicitly requested: a user's transform script often knows something about the domain (an
  order value, a custom business counter) that no amount of Rust-side instrumentation could infer,
  and the framework's whole premise is "components add what only they know" — a script is a
  component too.

## Consequences

- `crates/logit-script/src/telemetry.rs`'s module doc states this tradeoff plainly, right next to
  the code that makes it, not just here.
- `docs/design/lua-api.md`'s "Emitting telemetry from a script" section carries the same warning
  for the audience that actually writes scripts — the ADR records the decision, the design doc is
  what a script author is expected to read.
- If Lua-authored telemetry cardinality ever becomes a real operational problem (unlike the
  Rust-side interner growth risk, which `docs/known-gaps.md` argues is unlikely to be hit first),
  the fix is either the bounded-cache alternative above or a lint/review convention for scripts —
  not a change to `logit_core::telemetry` itself, which stays correctly agnostic about where a
  `&'static str` came from.
- No change to `Telemetry`'s public API or its `&'static str` signature — this ADR is entirely
  about how `crates/logit-script` satisfies that signature, not about relaxing it.
