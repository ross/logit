---
created: 2026-09-26
updated: 2026-09-26
---

# Lua scripts: stall detection, a bounded drain, opt-in `max_memory`, and a table-depth cap

## Status
Accepted

## Context

`docs/plans/critical-sections-inventory.md` groups CORE-15..19 and RT-11 as cluster 7, "Lua
boundary": the Rust↔LuaJIT boundary in `crates/logit-script` and the OS-thread hosting of a Lua
node in `crates/logit-pipeline/src/runtime.rs`. Under [ADR
`deployment-threat-model`](deployment-threat-model.md), a `lua`/`lua_file` script is trusted, like
the network: the bar for a defense is accidental misuse by an operator script, not a crafted one.
The accidental cases this record answers are a handle held past its lifetime, a cyclic or very
deep table, an infinite loop, unbounded accumulation across `flush()` calls, a metric or tag name
derived from per-event data, and a leftover `print`.

Three read-only explorer passes plus one design pass, reading the vendored mlua 0.9.9 and LuaJIT
sources, found:

- No bound of any kind exists on a script today: no `set_memory_limit`, no hook, no time budget.
  `ScriptWorker::used_memory` feeds only the `logit.script.vm.memory` gauge; nothing reads it back
  to act.
- An infinite-loop `process()` hangs process exit indefinitely: `watch_lua_thread` awaits only
  `done_rx`, and the join loop in `run_with_telemetry` has no deadline. A wedged thread also keeps
  its `Fanout` senders open, so a downstream native node never sees its inbox close either.
- `lua_to_value` → `lua_table_to_value` → `lua_table_to_attrmap` recurse with no depth bound, so a
  self-referencing table (`t.self = t`) overflows the Rust stack and aborts the process. Every
  other producer of a nested `Value` is already bounded at or under 128 levels (native's
  `MAX_VALUE_DEPTH`, serde_json's 128, OTLP's 41/49, `syslog.sd`'s 3); only the Lua-to-Rust
  direction is unbounded.
- `lua_table_to_value`'s array branch reads through `Table::get`, which honors a metatable's
  `__index`, while the map branch and `events_from_table` already read raw (`lua_next`). Not
  reachable as re-entrancy today, since a raw `pairs` walk finds every key present before `get`
  would ever consult `__index` on a nil raw value, but the two branches take different paths for
  no reason tied to correctness.
- Lifetime tests cover a stashed `event`, `event.attributes`, and `event.metrics[i]` used in
  `flush()`, but not a stashed `event.log` or `event.span`, a stash from `process()` call N used in
  call N+1, returning the same handle twice, or `return {e}` beside a live alias to `e`.
  `into_inner`'s `Err(rc)` clone fallback looks unreachable by analysis, but is silent if a path
  ever reaches it.
- The sandbox exposes `coroutine` (registered by `luaopen_base` itself), `print` (writes to
  process stdout, which corrupts `stdio_out` on stdout and is fatal under `format: native`),
  `collectgarbage`, `newproxy`, and `gcinfo`. No test enumerates `_G`. `bit`, `jit`, `debug`,
  `package`, `ffi`, `os`, and `io` are absent.
- LuaJIT's compiled traces never check a count hook unless the runtime is built with
  `LUAJIT_ENABLE_CHECKHOOK`, which this project's build does not set, and interpreted code pays a
  hook-dispatch cost on every instruction (`lj_dispatch.c`, `lj_record.c`). `Lua::set_memory_limit`
  does work on LuaJIT x64, since GC64 is on and `lua_newstate` takes mlua's allocator, but it trips
  on uncollected garbage and surfaces as a script error rather than failing the node.
- `to_table` emits every count (`bucket_counts`, `count`, dropped counts) as a Lua number; on x64
  LuaJIT that's an `f64`, so a count above 2^53 loses precision through
  `Event.new(e:to_table())`. Attributes already cross the boundary as decimal strings past
  ±2^53 (`exact_i64_to_lua`); counts do not yet follow that convention.
- Script strings intern with no guard from `MetricProxy`'s `name`/`unit`/`description`,
  `LogProxy`'s `event_name`, `Event.new`'s same fields, every attribute key a script writes
  (nested-table keys included), and `telemetry` tag keys and values. Only `telemetry` documents the
  hazard.

## Decision

1. **Runaway CPU gets a heartbeat, not a hook.** `ScriptWorker` marks a per-call heartbeat before
   and after each `process()`/`flush()` call. A watcher task polls it; a busy bit that stops
   advancing for `stall_after` (default 10s) diagnoses `script_stalled` and moves the node to a
   `NodeState::Stalled` state. `Stalled` is reversible: the heartbeat advancing again moves the
   node back to `Running` and diagnoses `script_resumed`. `/readyz` maps a `Ready` phase with any
   stalled node to `503 degraded`, the same status a failed node already reports. `/healthz` stays
   `200`, because the admin task itself is unaffected: an orchestrator restart would not help a
   node that is not making progress for reasons internal to a Lua VM.
2. **Shutdown is bounded even with a wedged script.** The drain deadline for a run with a Lua node
   is `max(listener shutdown_grace, sink shutdown_grace) + Lua shutdown_grace` (default 5s for the
   Lua term). Past that bound, the join loop abandons the drain: every node still running is marked
   `Failed`, readiness reports failure, and the process exits `2`, naming the wedged Lua node (and,
   where relevant, the nodes downstream of it) in the diagnostic. A run with no Lua node keeps
   today's unbounded drain, so no existing paused-time test changes.
3. **Runaway memory is opt-in.** `lua`/`lua_file` gain an optional `max_memory` field (a byte
   count, `human_bytes`-shaped like `RotateConfig::max_bytes`). After each batch and each
   `flush()`, a VM over the cap runs a full garbage collection before the verdict: still over the
   cap after that collection fails the node the way a Rust panic already does (`Failed`, `/readyz`
   `503`, exit `2`). Off by default; `0` is rejected by config validation.
4. **A depth cap on Lua-to-Rust table conversion.** `MAX_TABLE_DEPTH = 128`, local to
   `logit-script`, matches native's `MAX_VALUE_DEPTH` so a value a script builds always decodes on
   a `logit_in` peer. A table nested past that depth is a clear conversion error, naming the
   attribute or field it came from, not a stack overflow. 128 levels convert; 129 fail.
5. **Table reads are raw everywhere in the conversion path.** The array branch of
   `lua_table_to_value` moves to `raw_get`, matching the map branch and `events_from_table`, so no
   Lua-to-Rust conversion ever runs a metamethod. The borrow on the event's `RefCell` is still
   released before conversion runs, because a GC finalizer can run script code at any allocation
   point, conversion or not.
6. **`print` goes to the self-log.** `ScriptWorker::new` replaces the global `print` with a Rust
   closure that `tostring`s its arguments, joins them the way Lua's own `print` does, and emits a
   `tracing::info!` line tagged with the component id. It never touches process stdout. A leftover
   debug `print` is the accidental case this cluster's threat model names, and a self-log
   line is more useful to an operator than corrupted `stdio_out` output.
7. **`collectgarbage`, `setmetatable`, `newproxy`, and `coroutine` stay in the sandbox.** None of
   the four reaches the host filesystem, network, or process, and each has an ordinary use in a
   transform script (freeing memory early, building a read-only wrapper table, closing over a
   resource with a finalizer, or structuring control flow). Removing them would narrow the
   scripting surface for no bound gained; a script that misuses one is covered by the memory cap
   (`collectgarbage`), the depth cap (`setmetatable`-driven aliasing still converts through the
   same raw path), or is left as a documented non-goal below (`newproxy` and `coroutine`'s crafted
   cases).

## Alternatives considered

- **A count or time hook.** Rejected. LuaJIT's compiled traces never check a hook unless the
  runtime is built with `LUAJIT_ENABLE_CHECKHOOK` (off here), so a hot loop would compile straight
  past it; interpreted code would pay a per-instruction dispatch cost for a bound that only catches
  the case where JIT compilation fails to kick in. Neither trade is worth taking for an accidental
  hang, which the heartbeat already surfaces without touching the VM's execution path.
- **`Lua::set_memory_limit`.** Rejected as the primary mechanism. It works on this build (GC64,
  LuaJIT x64), but a VM sitting on uncollected garbage trips it as an ordinary script error, which
  leaves the node running in a state the operator can't distinguish from a real script bug. A full
  GC before the verdict, decided separately after the cap is crossed, gives the same protection
  without that false-positive class.
- **Killing the OS thread hosting a wedged script.** Rejected: Rust has no supported way to
  terminate a running thread. The bounded drain accepts that a wedged thread outlives the process
  that abandons it, and lets `process::exit` reclaim it at exit.
- **A terminal `Phase::Failed` on a stall.** Rejected. A stall is not evidence the script (or the
  process) is broken beyond recovery — a slow but progressing script, a large batch, or a
  temporarily blocked downstream sink can all look the same for a while. `Stalled` recovers to
  `Running` on its own; a hard failure is reserved for the cases that are terminal (a
  panic, an over-cap VM after a full GC, an abandoned drain).
- **Removing `print` instead of redirecting it.** Rejected. A leftover debug `print` is the
  accidental case this record is about, not a misuse to design out; a self-log line under the
  component id is strictly more useful to an operator than deleting the primitive would be.
- **Bounding only the Lua watcher, leaving the drain unbounded.** Rejected. A wedged Lua thread
  keeps its `Fanout` senders open, so a downstream native node's own inbox never closes and it
  never finishes its own shutdown. Watching the Lua node alone would report the problem without
  ever letting the process exit.

## Consequences

- Each workstream below closes the inventory rows named under "Running it," updating their status
  in the PR that lands its artifact.
- The following stay documented non-goals in `docs/known-gaps.md`, citing [ADR
  `deployment-threat-model`](deployment-threat-model.md): `collectgarbage("stop")` run from a
  script defeats the memory cap's full-GC step; a `newproxy(true)` finalizer that touches a
  stashed handle during collection is a crafted re-entrancy case, not an accidental one; a
  no-allocation infinite loop (a pure numeric spin) is invisible to `max_memory`, which only the
  heartbeat catches; and interner growth from script-derived strings (`Event.new`'s and the proxy
  setters' `name`/`unit`/`description`/`event_name` fields, and nested attribute keys) is accepted
  the way `telemetry`'s own tag values already are.
- A downstream `aggregate` window in flight when a drain is abandoned is lost: the process exits
  before that window's flush would fire. This is the same at-most-once cost an abandoned drain
  already has for any node kind.
- `/healthz` stays `200` on a stalled node, unchanged from [ADR
  `admin-readiness-endpoint`](admin-readiness-endpoint.md): the admin server itself is healthy, and
  an orchestrator restart would not clear a script wedge.

## Running it

Filled in as each workstream lands.

- `luab/w1` (CORE-17, CORE-18): pending.
- `luab/w2` (CORE-16, CORE-15 sandbox half): pending.
- `luab/w3` (RT-11, CORE-15 limits half): pending.
- `luab/w4` (CORE-15 close, `max_memory`): pending.
- `luab/w5` (CORE-19 close, docs): pending.
