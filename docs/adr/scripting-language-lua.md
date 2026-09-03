---
created: 2026-08-28
updated: 2026-08-28
---

# User scripting language: Lua (LuaJIT)

## Status
Accepted

## Context
The core value of `logit` is letting users express real processing logic — reshaping, enriching,
routing, aggregating — without a rebuild or a separate processing tier. That logic needs to run
inline in a high-throughput pipeline, so the scripting layer's performance and embeddability are as
important as its ergonomics.

## Decision
Lua, via LuaJIT, embedded through [`mlua`](https://github.com/mlua-rs/mlua) with the `vendored`
feature (statically linked, no system Lua dependency). Scripts are written inline in YAML or in
referenced `.lua` files, and are pinned to LuaJIT's Lua 5.1 dialect.

## Alternatives considered
- **Lua + a WASM plugin escape hatch.** Keeps Lua as the ergonomic default and adds a WASM ABI for
  users who need heavier or proprietary logic in Rust/Go/etc. Real option for later — noted as a
  possible v2 extension — but doubles the surface area to design and maintain for a v1, and Lua
  alone hasn't yet proven insufficient.
- **A purpose-built DSL** (in the spirit of Vector's VRL). Fully sandboxed and statically checkable
  by construction. Rejected for v1: it means owning an entire language — parser, type system,
  stdlib, editor tooling, docs — instead of inheriting Lua's.
- **Starlark.** Deterministic, hermetic, Python-flavored syntax. Rejected: no I/O and no unbounded
  loops by design conflicts with things users plausibly want in a transform (bounded loops over
  event fields are fine, but the restriction is a frequent point of friction in practice), and the
  embedding and operator-community story is weaker than Lua's in this domain.

## Consequences
- LuaJIT's frozen Lua 5.1 semantics is a real, permanent constraint (no goto in older forms, no
  integer division operator, etc.) — acceptable for the kind of scripts this project expects.
- `mlua::Lua` is neither `Send` nor `Sync`. The pipeline runs one Lua VM per worker; there is no
  implicit shared mutable state between workers. See
  [docs/design/lua-api.md](../design/lua-api.md).
- Events are exposed to scripts through a proxy (userdata + metamethods) rather than converted to
  plain Lua tables on every stage, to avoid per-event allocation. See
  [docs/design/lua-api.md](../design/lua-api.md) for the design and the benchmark this needs before
  the API is frozen.
