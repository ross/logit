---
created: 2026-09-26
updated: 2026-09-26
---

# Lua scripts: stall detection, a progress-based wedge check, opt-in `max_memory`, and a table-depth cap

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
  `into_inner`'s `Err(rc)` clone fallback is reachable, not merely theoretical: `lua_newuserdata`
  (used by `attrs_userdata`, `log_userdata`, `metrics_userdata`, and `span_userdata` alike) runs
  `lj_gc_check`, which can run a script's `__gc` finalizer while an mlua call is mid-allocation.
  `newproxy` is the only way a script gets a finalizer on LuaJIT — `setmetatable({}, {__gc = ..})`
  never fires, and `ffi` is absent — so a probe built on `newproxy(true)` (a re-armed `__gc`,
  `collectgarbage("setpause", 0)`/`setstepmul`) is what found it: a finalizer that runs during
  `AttrsProxy::__index`'s `value_to_lua` allocation SIGSEGVs reproducibly (3/3 in a debug build,
  2/3 in release), and one that runs during `EventProxy::to_table`'s hits an "already borrowed"
  `RefCell` panic, caught and silently lost, instead.
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

1. **Runaway CPU gets a heartbeat, not a hook.** The heartbeat lives in `logit-script`
   (`logit_script::Heartbeat`, an `AtomicU64`: bit 0 is a busy flag, the upper bits a
   call/progress count; `Relaxed` load and store, one writer). `ScriptWorker::with_heartbeat`
   ticks it around each `process()`/`flush()` call, and inside a call too — once per `Event.new`
   and once per element `events_from_table` builds from a returned table — so a `flush()` that
   emits many events stays a run of progress ticks, never a stall, however long it takes. A
   watcher task polls the heartbeat; a busy bit that stops advancing for `stall_after` (default
   10s) diagnoses `script_stalled` and moves the node to a `NodeState::Stalled` state. `Stalled`
   is reversible: the heartbeat advancing again moves the node back to `Running` and diagnoses
   `script_resumed`. `/readyz` maps a `Ready` phase with any stalled node to `503 degraded`, the
   same status a failed node already reports; the shipped image's `HEALTHCHECK` probes `/readyz`,
   so a stalled script makes the container unhealthy — Docker Swarm restarts it, Kubernetes pulls
   the pod from every Service's endpoints — without `logit` itself doing anything orchestrator
   specific. `/healthz` stays `200`, because the admin task itself is unaffected: an orchestrator
   restart would not help a node that is not making progress for reasons internal to a Lua VM.
2. **A wedge is detected by lack of progress, never by a wall-clock drain bound.** The same
   heartbeat is the wedge signal at shutdown. `shutdown_rx` only arms the check: once shutdown has
   been seen, a node whose heartbeat is busy and unchanged for at least its `shutdown_grace`
   (default 5s), measured from `max(shutdown_at, last_change)`, is wedged. A node the heartbeat
   never marks busy — parked in `blocking_send` against a full downstream inbox, say — is never
   blamed here; that belongs to RT-03. On a wedge the watcher revokes the Lua thread's I/O: its
   `inbox`, `fanout`, and `target_fanouts` live behind an `Arc<Mutex<Option<LuaIo>>>` the thread
   locks only around a receive or a send, never between entering and leaving a call, and the
   watcher `try_lock`s the mutex and drops what it holds. Every downstream node then sees its
   inbox close and drains on its own grace (a window flushed, a drop counted); an upstream send
   against the revoked fanout fails and counts `closed_consumer`. Nothing is aborted and no thread
   is cancelled: if the wedged call ever returns, it finds `None` and the thread exits on its own.
   The watcher then fails the node the way a Rust panic already does — `Failed`, readiness
   failure, the join loop's existing first-error cascade, exit `2` naming the node — through the
   unchanged first-error path. A run with no Lua node is unaffected: the join loop,
   `shutdown_grace_expired`, and the "drain complete" log are untouched.
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
   closure that calls the script-visible global `tostring` on each argument in turn — so a
   script's own `__tostring` metamethod or a redefined `tostring` renders the same way it would
   under Lua's real `print` (`luaB_print`) — tab-joins the results, and emits a `tracing::info!`
   line tagged with the component id. The id is shared with `with_component` through the same
   `Rc<RefCell<String>>` the component's provenance state already uses, holding a placeholder
   until `with_component` sets it (only top-level code during `.exec()` can print before then). It
   never touches process stdout. A leftover debug `print` is the accidental case this cluster's
   threat model names, and a self-log line is more useful to an operator than corrupted
   `stdio_out` output.
7. **`collectgarbage`, `setmetatable`, and `coroutine` stay in the sandbox, and `print` stays
   rerouted rather than removed (decision 6).** None of the three reaches the host filesystem,
   network, or process, and each has an ordinary use in a transform script (freeing memory early,
   building a read-only wrapper table, or structuring control flow). Removing them would narrow
   the scripting surface for no bound gained; a script that misuses one is covered by the memory
   cap (`collectgarbage`) or the depth cap (`setmetatable`-driven aliasing still converts through
   the same raw path), or is left as a documented non-goal below (`coroutine`'s crafted cases).
8. **`newproxy` is removed from the sandbox.** `remove_unsandboxed_base_globals` nils it, and the
   `_G` allowlist test pins it absent. It was the only way a script could reach the SIGSEGV and
   the borrow panic in the Context section above, because it is the only way a script gets a
   `__gc` finalizer on LuaJIT at all. Removing it establishes the invariant the rest of the
   boundary depends on: **no script code runs during an mlua allocation.** That invariant is what
   makes `EventProxy::to_table` and `AttrsProxy::__index` sound holding a `RefCell` borrow across
   a call that allocates — a re-entrant write reaching either during that borrow has no path left
   to fire from, rather than a path defended at each site. `into_inner`'s `Err(rc)` arm keeps its
   `debug_assert!(false)` in a debug build and its clone fallback in release, for whatever else
   might reach it.
9. **The registry is expired on a schedule, not left to mlua.** mlua 0.9.9's `RegistryKey::drop`
   only queues its id for reuse; the slot, and the `Rc<RefCell<Event>>` a sub-proxy handle holds
   through it, stays live until `create_registry_value` reuses the id or `expire_registry_values`
   runs. Left alone, that gap is bounded only by the size of the next GC batch and invisible to
   `used_memory()`. `ScriptWorker::expire_registry_values` runs after each batch and each
   `flush()`, and again before the full garbage collection that decides a `max_memory` verdict
   (decision 3).
10. **A malformed return value gets one wording, from the script's own name.** `take_event` on a
    non-event userdata and `events_from_table`'s conversion error both report "process() must
    return nil, an event, or a table of events; got …" (and the `flush()` analogue), rather than
    two different messages for the same mistake. The script is loaded with
    `Lua::load(source).set_name("=script")`, so a traceback cites the script, not a
    `crates/logit-script/src/lib.rs` line. A script's own `pcall` still sees mlua's raw
    destructed-userdata wording for a stale handle; documented, not changed.
11. **A count round-trips past 2^53, like an attribute already does.** `to_table` and the
    `MetricProxy` read arms (`count`, `zero_count`, and each bucket row) emit a `u64` count as an
    ordinary Lua integer up to 2^53 and as a decimal-digit string above it, the same convention
    `exact_i64_to_lua` already uses for an attribute; `Event.new`'s `count` field accepts either
    form. [ADR `lua-event-constructor`](lua-event-constructor.md)'s residual list, which names
    this gap, is corrected in `luab/w1`.
12. **The Lua OS thread gets an 8 MiB stack.** Pure-Lua recursion through Rust/C frames — a
    `string.gsub` callback recursing into itself — aborts the default 2 MiB thread at around 233
    levels, regardless of build profile. The thread `run_lua` spawns sets `std::thread::Builder::
    stack_size` to 8 MiB. The extra is virtual address space, committed only as the stack grows,
    so an ordinary script pays nothing for it.

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
- **A wall-clock drain bound with `JoinSet` abort.** The first design measured against a real
  workload instead of dropped: a `flush()` emitting 1M events through `Event.new`, a legitimate
  and healthy call, took 7.8s on the reference machine, tripping a bound sized for a wedge. Along
  a chain of more than one Lua node the graces would add, so the bound a single node needs is not
  the bound a chain needs. Worse, aborting the `JoinSet` entry hosting a Lua node would drop its
  `Fanout` senders at once, so every downstream `aggregate` window in flight and any in-memory
  sink state would be lost with no chance to flush. Rejected in favor of the progress-based,
  per-node wedge check in decision 2.
- **Killing the OS thread hosting a wedged script.** Rejected: Rust has no supported way to
  terminate a running thread. Revoking its I/O instead lets a wedged thread keep running
  unobserved after the node around it has failed, and `process::exit` reclaims it at exit.
- **A terminal `Phase::Failed` on a stall.** Rejected. A stall is not evidence the script (or the
  process) is broken beyond recovery — a slow but progressing script, a large batch, or a
  temporarily blocked downstream sink can all look the same for a while. `Stalled` recovers to
  `Running` on its own; a hard failure is reserved for the cases that are terminal (a
  panic, an over-cap VM after a full GC, a wedge past its `shutdown_grace`).
- **Removing `print` instead of redirecting it.** Rejected. A leftover debug `print` is the
  accidental case this record is about, not a misuse to design out; a self-log line under the
  component id is strictly more useful to an operator than deleting the primitive would be.
- **Watching the Lua node without revoking its I/O.** Rejected. A wedged Lua thread keeps its
  `Fanout` senders open, so a downstream native node's own inbox never closes and it never
  finishes its own shutdown. Diagnosing the wedge without acting on its I/O would report the
  problem without ever letting the process exit.
- **Keeping `newproxy` and defending only the returned-event cache.** Rejected: the SIGSEGV and
  the borrow panic both happen inside the mlua allocation call that runs the finalizer, before any
  accessor cache exists to re-check. A defense placed at the cache could not have prevented either
  one; only removing the one path to a finalizer closes both.

## Consequences

- Each workstream below closes the inventory rows named under "Running it," updating their status
  in the PR that lands its artifact.
- The following stay documented non-goals in `docs/known-gaps.md`, citing [ADR
  `deployment-threat-model`](deployment-threat-model.md): `collectgarbage("stop")` run from a
  script defeats the memory cap's full-GC step; a no-allocation infinite loop (a pure numeric
  spin) is invisible to `max_memory`, which only the heartbeat catches; and interner growth from
  script-derived strings (`Event.new`'s and the proxy setters' `name`/`unit`/`description`/
  `event_name` fields, and nested attribute keys) is accepted the way `telemetry`'s own tag values
  already are. A `newproxy(true)` finalizer touching a stashed handle during collection is no
  longer possible to write at all: decision 8 removes `newproxy`, closing the class rather than
  defending each site against it.
- A downstream `aggregate` window in flight when a Lua node wedges is not lost: revoking the
  wedged node's I/O (decision 2) closes every downstream inbox, so each downstream node drains on
  its own `shutdown_grace` the way it would on an ordinary shutdown, flushing its own window and
  counting its own drops. A send from upstream of the wedged node, against its now-revoked
  fanout, counts `closed_consumer` rather than reaching it.
- `/healthz` stays `200` on a stalled node, unchanged from [ADR
  `admin-readiness-endpoint`](admin-readiness-endpoint.md): the admin server itself is healthy, and
  an orchestrator restart would not clear a script wedge on its own — the container's
  `HEALTHCHECK` failing `/readyz` is what prompts one.
- Three residuals the depth cap and the larger stack don't close. A 128-deep value a script builds
  does not survive a relay through `otlp_out → otlp_in`: OTLP's own JSON and protobuf nesting
  limits are 41 and 49 levels, both under native's 128. The depth cap bounds a table's nesting,
  not its size, so a DAG a script builds by sharing table references (`t = {a = t, b = t}`
  repeated k times) still converts, at 2^k nodes. And pure-Lua recursion through Rust/C frames can
  still abort the process past the larger stack from decision 12, at a higher level than 233. The
  last two are recorded in `docs/known-gaps.md`'s Lua entry, citing [ADR
  `deployment-threat-model`](deployment-threat-model.md).

## Running it

Filled in as each workstream lands.

- `luab/w1` (CORE-17, CORE-18): pending.
- `luab/w2` (CORE-16, CORE-15 sandbox half): pending.
- `luab/w3` (RT-11, CORE-15 limits half): pending.
- `luab/w4` (CORE-15 close, `max_memory`): pending.
- `luab/w5` (CORE-19 close, docs): pending.
