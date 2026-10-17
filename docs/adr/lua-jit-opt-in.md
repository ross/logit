---
created: 2026-10-16
updated: 2026-10-16
---

# LuaJIT's trace compiler is off by default, and `jit: true` turns it on per component

## Status
Accepted

## Context

Every `lua`/`lua_file` script runs in LuaJIT's interpreter. `sandbox_libs()`
(`crates/logit-script/src/lib.rs`) opens `table`, `string`, and `math`, and never the `jit`
library. `luaopen_jit` (its `jit_init`) is the only code in LuaJIT that sets the compiler's on
flag and its default optimization parameters, so without it no trace ever compiles. Nothing
recorded that as a choice: [ADR `lua-runaway-script-bounds`](lua-runaway-script-bounds.md) reasons
about compiled traces as if they ran, and its rejection of a count hook rests partly on them.

Measured on 2026-10-16 on a developer machine (a Ryzen AI MAX+ 395, pinned cores, the
`performance` governor), not the perf VM, so read the numbers as relative only. End to end, through
`logit-perf` with five interleaved rounds per configuration, the median CPU µs per event was:

| Scenario | JIT off | JIT on | Change |
|---|---|---|---|
| `perf/scenarios/lua.yaml` | 1.324 | 1.305 | -1.4% |
| a logfmt parse in Lua through `gmatch` | 8.505 | 8.391 | -1.3% |
| 8 attribute writes and a concatenation | 3.102 | 3.112 | +0.3% (noise) |
| a pure-Lua helper (hashing and table work) | 2.282 | 1.733 | -24.0% |
| `examples/prusa/prusa.lua` at `63fd8238` | 33.679 | 28.349 | -15.8% |

Nothing ran slower. The compiler helps only work that stays in plain Lua. A proxy's
`__index`/`__newindex` is an mlua C function, and `string.match`/`gmatch` are C, so a trace ends
at each call and stitches around it, and that work runs at interpreter speed either way. Over a
pure-Lua loop, a concatenation loop ran 378.7 ns per iteration interpreted and 32.4 ns compiled,
while a `gmatch` parse ran 5,236 ns and 5,218 ns.

Other measurements from the same session:

- **A count hook can't bound a compiled loop.** LuaJIT still compiles under a hook, and a
  compiled trace never checks it, because this build doesn't set `LUAJIT_ENABLE_CHECKHOOK`. With
  the compiler on, `while true do x = x + 1 end` was not interrupted within 3 s at a hook every
  1,000 or every 1,000,000 instructions, whether the hook was set before the loop or after it
  warmed up. A loop that calls a proxy each iteration was interrupted, after about 200 ms.
- **With the compiler off, a hook bounds every loop**, at a cost set by the hook dispatch mode
  rather than by how often the hook fires. A hook every 1,000 or every 10,000 instructions added
  0.4% to 2.2% CPU per event on the three light scripts above and 9.2% to 13.6% on the two
  Lua-heavy ones (prusa +11.2% at 1,000).
- **Memory.** A compiled VM held about 72 KB more RSS after its work. About 45 KB of that is
  trace data on the Lua heap, which `used_memory()` and `max_memory` count. Machine code is
  mapped outside the Lua allocator (`lj_mcode.c`), reserved 64 KB at a time up to the `maxmcode`
  default of 512 KB per VM, and `max_memory` doesn't count it.
- **Failure paths don't change.** A Lua `error`, an error from a Rust callback, an error from a
  proxy's `__newindex`, and a `max_memory` trip in `Event.new`, each raised inside a hot loop,
  failed the call with the same message compiled or interpreted, and the VM stayed usable.
- **Allocations don't change.** All 18 Lua cases in `crates/logit-bench/tests/allocations.rs`
  counted the same allocations with the compiler on.

`logit` ships for amd64 only, where LuaJIT's compiler is mature.

## Decision

The trace compiler stays off by default. A `lua` or `lua_file` component that sets `jit: true`
(default `false`) gets a VM with it on:

1. `ScriptWorker::new_with_jit` builds the VM with `sandbox_libs() | StdLib::JIT`, so
   `luaopen_jit` sets the on flag and LuaJIT's default parameters.
2. It then sets the `jit` global to `nil`, so a script can't turn the compiler off, flush traces,
   or attach a trace handler. That global is the only copy a script can reach: the library is
   also in the registry's `_LOADED` table, but the sandbox has nothing that reads the registry or
   that table (no `debug`, `package`, or `require`, and `getfenv` is removed).
   `the_global_table_is_exactly_the_allowlist` checks the global set in both modes.
3. No tuning knob, and no count hook or time limit in either mode. A runaway loop is still caught
   only by stall detection ([ADR `lua-runaway-script-bounds`](lua-runaway-script-bounds.md),
   decision 2).

## Alternatives considered

- **Compiler on by default.** Rejected. Most scripts are attribute access and pattern matching,
  where it gained nothing measurable, and it would move every Lua component onto memory that
  `max_memory` doesn't count and onto loops a future count hook couldn't bound.
- **`luaJIT_setmode` from Rust instead of opening the library.** Rejected. Without `luaopen_jit`,
  the optimization parameters stay zeroed, so the compiler would run with every limit at zero
  rather than LuaJIT's defaults.
- **Tuning knobs (`maxmcode`, `hotloop`, and the other `jit.opt` parameters).** Not added. Nothing
  measured needed one, and each is one more setting an operator can get wrong.
- **Building LuaJIT with `LUAJIT_ENABLE_CHECKHOOK`.** Not now. It would make compiled traces check
  a hook, which only matters once a count hook exists, and no decision adds one.

## Consequences

- A component with `jit: true` accepts three costs. No count hook could bound its compiled loops.
  Its machine code, up to 512 KB per VM, is outside `max_memory`. Its trace data grows the heap
  `max_memory` does count, by about 45 KB in the measurement above.
- A `pairs` loop call site that has once seen an attribute proxy loses its table-specialized
  iteration bytecode for good. With the compiler on, that site then runs plain-table loops about
  4x slower than a tables-only one, against about 60% slower interpreted
  ([ADR `lua-in-place-edits`](lua-in-place-edits.md)'s amendment).
- The default path is unchanged: a component without `jit:` builds the same VM it did before.
- `docs/known-gaps/transforms.md` records that `max_memory` doesn't count compiled machine code.
