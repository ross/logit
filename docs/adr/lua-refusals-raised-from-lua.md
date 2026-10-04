---
created: 2026-10-03
updated: 2026-10-03
---

# Lua refusals: `Event.new` raises a refusal from a Lua shim, not as an mlua callback error

## Status
Accepted

## Context

`Event.new(t)` ([ADR `lua-event-constructor`](lua-event-constructor.md)) is a Rust function a
script calls. It refuses a malformed table, and once `max_memory`'s in-call check trips
([ADR `lua-runaway-script-bounds`](lua-runaway-script-bounds.md), decision 3) it refuses every
later call in that `process()` or `flush()`. A script can catch a refusal with
`pcall(Event.new, t)` and keep going, and the sticky trip exists because scripts do.

The `logit-script` test `a_pcall_wrapped_event_new_loop_stays_tripped` pcall-loops `Event.new`
200,000 times against a 4 MiB cap. It took 2–3.5 s in most runs and 64–90 s in about a third of
them. Two upstream facts explain the slow runs:

- **mlua builds a traceback for every callback error.** In mlua 0.9.9, `callback_error_ext`
  (`src/lua.rs`) turns every `Err` a `Lua::create_function` callback returns into
  `Error::CallbackError { traceback, .. }`, with the traceback built through `luaL_traceback`
  before the error is raised. A `pcall` that discards the error has already paid for it. mlua
  0.12.2 still does this for every user callback, so an mlua upgrade doesn't change it.
- **mlua-sys's LuaJIT `luaL_traceback` searches the global tables.** mlua-sys 0.6.8 binds its own
  compat53 port of `luaL_traceback` (`src/lua51/compat.rs`) in place of LuaJIT's. For a C frame
  with no name, the port calls `compat53_pushglobalfuncname`, which runs
  `compat53_findfield(_G, 2)`: a `lua_next` walk over every global and every field of every
  global table, stopping at the first table that holds the function. LuaJIT's own
  `luaL_traceback` (`lj_debug.c`) prints the C function's address and walks nothing.

Under `pcall(Event.new, t)`, the `Event.new` frame has no name, so every refusal walks `_G`. The
test keeps its ~20,000 retained events in a global table, and when `next` reaches that table
before `Event`, each refusal walks it: about 0.5 ms per refusal, about 90 s for the run. `_G`'s
iteration order follows LuaJIT's per-VM string hash seed, so which runs are slow is random.

The cost isn't specific to the test or to the memory cap. Any script that holds large global state
and pcall-loops a refused call spends time in proportion to that state on every refusal, in about
half of its worker VMs. Because `Event.new` ticked the stall heartbeat before its memory check,
the stall watcher read such a loop as progress.

## Decision

1. **A refusal a script can catch is raised from Lua, never returned as an mlua callback
   error.** `Event.new` is a Lua function, loaded once per VM from a chunk named `=Event.new`,
   that wraps the Rust constructor. The constructor returns the event on success and the
   refusal's message as a Lua string on refusal; the shim raises that string with
   `error(msg, 2)`. A Lua `error` call builds no traceback, and `pcall` installs no message
   handler, so a caught refusal costs no global-table search. The shim holds `type` and `error`
   as upvalues, so a script that reassigns either global doesn't change what a refusal does.
2. **What the script sees.** A refusal is a plain string. A direct call's message carries the
   calling line, `script:12: Event.new: timestamp is required`. Under `pcall(Event.new, t)` the
   caller is a C function, so the message is the bare `Event.new: timestamp is required`. A tail
   call (`return Event.new{...}`) leaves no calling frame to name, so its message is bare too. A
   refusal that escapes `process()` or `flush()` gets a traceback from mlua's message handler at
   the top-level call, as any script error does.
3. **Internal failures stay on mlua's path.** Only an `mlua::Error::RuntimeError`, the variant
   every refusal is built as, becomes a returned string. Any other error (an allocation failure or
   a collection that fails) stays an `Err` from the callback, with mlua's
   traceback.
4. **The `max_memory` trip message is fixed for the rest of the call.** `MemoryCap::check`
   formats `Event.new: over max_memory (<used> > <cap>)` once, when the in-call check trips, and
   every later refusal in that call repeats it. A pcall loop over a tripped cap then interns one
   Lua string instead of one per call. The runtime's trip reset clears it.
5. **A refused call is not progress.** `Event.new` ticks the stall heartbeat only when it
   constructs an event. A refusal ticks nothing, whether a tripped memory cap or a malformed table
   caused it, so a `pcall` loop of refusals reads to the stall watcher as a stall.

## Alternatives considered

- **Upgrade mlua.** mlua 0.12.2 still builds the traceback eagerly for every user callback error,
  so the upgrade alone doesn't remove the cost.
- **Patch mlua-sys to bind LuaJIT's native `luaL_traceback`.** A `[patch.crates-io]` fork removes
  the global-table walk, but it's a fork to maintain, and every caught callback error would still
  build a traceback it then discards. Filing the change upstream is worth doing regardless.
- **Make only the `over_cap` message constant.** That shortens the memory-trip loop's strings but
  leaves every refusal, including a malformed table, on the traceback path.

## Consequences

- `Event.new`'s allocation pins (`docs/design/memory.md` §2) don't move: the success path
  returns the same userdata through the same conversion, and the shim's Lua call frame allocates
  nothing on the Rust heap.
- The error text changes shape. A direct call's refusal gains the `script:<line>:` prefix, and a
  caught refusal is the bare message rather than mlua's `CallbackError` wrapping. Tests and
  `docs/design/lua-api.md` match on the `Event.new: ...` part.
- Every other Rust callback a script can reach (the proxies' `__index`/`__newindex`, `event:to`,
  `telemetry.*`, `print`) still returns its errors through mlua, so each one a script catches
  builds a traceback, with the same global-table walk for an anonymous frame. None of them is a
  call a script loops on to probe for success, and each costs one walk per failed call.
  `docs/known-gaps/transforms.md` records it. Moving another callback onto this
  pattern follows this record.
- A script that pcall-loops refused `Event.new` calls, after a memory trip or on a malformed
  table, shows as `stalled` once the loop runs past the stall threshold. This amends
  [ADR `lua-runaway-script-bounds`](lua-runaway-script-bounds.md)'s "a loop that keeps calling
  `Event.new` is progress" consequence: only a loop that keeps constructing events is progress.
