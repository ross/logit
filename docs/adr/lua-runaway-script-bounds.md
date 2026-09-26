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
  `__index`, and so does `events_from_table`. Only the map branch and `validated_sequence_len`
  already read raw (`lua_next`). Not reachable as re-entrancy today, since `validated_sequence_len`'s
  raw `pairs` walk finds every key present before `get` would ever consult `__index` on a nil raw
  value, but the paths differ for no reason tied to correctness.
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
   watcher task polls the heartbeat on the interval decision 2 sets; a busy bit that stops
   advancing for `stall_after` (default 10s) diagnoses `script_stalled` and moves the node to a
   `NodeState::Stalled` state. `Stalled` is reversible: the heartbeat advancing again moves the
   node back to `Running` and diagnoses `script_resumed`. `/readyz` reports a stalled node as its
   own wire state, `503 stalled`, distinct from a failed node's `503 degraded`: `luab/w3` amends
   [ADR `admin-readiness-endpoint`](admin-readiness-endpoint.md) to add `stalled` beside
   `degraded` rather than widen `degraded` to cover it. The shipped image's `HEALTHCHECK`
   (`logit ready` → `/readyz`) makes the container unhealthy under Docker or Swarm; a Kubernetes
   `readinessProbe` configured against `/readyz`, per `docs/deploying.md`, pulls the pod from
   Service endpoints — Kubernetes takes no action on an image `HEALTHCHECK` by itself. `/healthz`
   stays `200`: liveness answers "is the process alive," which it is, while readiness answers
   "should traffic route here," which a stalled node honestly answers no to. Whether an
   orchestrator restarts on that signal is the operator's own probe policy, not something this
   record decides.
2. **A wedge is detected by lack of progress, never by a wall-clock drain bound.** The same
   heartbeat is the wedge signal at shutdown. A new, internal `LuaRuntimeConfig` (not
   config-exposed, landing in `luab/w3`) supplies both thresholds: `stall_after` defaults to 10s,
   matching decision 1, and `shutdown_grace` defaults to **2s**, shorter than a sink's
   `buffer.shutdown_grace` (5s default) so a downstream node still has time to flush before a
   sink's `write_loop` leaves. The watcher's tick interval is `min(stall_after, shutdown_grace) /
   4` (at least 10 ms). `shutdown_rx` only arms the check: once shutdown has been seen, a node
   whose heartbeat is busy and unchanged for at least its `shutdown_grace`, measured from
   `max(shutdown_at, last_change)`, is wedged; a node already `Stalled` when shutdown arrives —
   its last change already predates shutdown by more than the grace — is revoked on the first
   tick after shutdown begins. A node the heartbeat never marks busy is never blamed here: a
   `blocking_send` park against a full downstream inbox is one of the concerns RT-11's own
   inventory entry already lists, and the unbounded `output.flush()` that `finish_and_flush` can
   leave running past its own sink's shutdown is RT-03's. On a wedge the watcher revokes the Lua
   thread's I/O: its `inbox`, `fanout`, and `target_fanouts` live behind an
   `Arc<Mutex<Option<LuaIo>>>` the thread locks only around a receive or a send, never between
   entering and leaving a call, and the watcher `try_lock`s the mutex and drops what it holds. A
   downstream transform then flushes once its inbox closes, the way `run_transform` already does;
   a downstream sink drains under its own `buffer.shutdown_grace`, as on an ordinary shutdown. An
   upstream send against the revoked fanout fails and counts `closed_consumer`. Nothing is
   aborted and no thread is cancelled: if the wedged call ever returns, it finds `None` and the
   thread exits on its own. The watcher then fails the node the way a Rust panic already does —
   `Failed`, readiness failure, the join loop's existing first-error cascade, exit `2` naming the
   node — through the unchanged first-error path. A run with no Lua node is unaffected: the join
   loop, `shutdown_grace_expired`, and the "drain complete" log are untouched.
3. **Runaway memory is opt-in.** `lua`/`lua_file` gain an optional `max_memory` field (a byte
   count, `human_bytes`-shaped like `RotateConfig::max_bytes`). A VM over the cap runs a full
   garbage collection before the verdict: still over the cap after that collection fails the node
   the way a Rust panic already does (`Failed`, `/readyz` `503`, exit `2`). The check runs after
   each batch and each `flush()`, and, when `max_memory` is set, inside a call too — every 1024
   `Event.new` constructions — so a call that keeps building and retaining events is bounded
   without waiting for it to return: a Lua error naming `max_memory` ends such a call there, as a
   counted script error, ahead of the post-call check that then decides the node's fate. Off by
   default; `0` is rejected by config validation.

   **Amended 2026-09-26 (`luab/w4`), after a refuter pass measured the first design:**
   - *The verdict is a collection loop, not one collection.* One LuaJIT full cycle halves the
     string table and frees a finalized userdata only on the cycle after, so a million transient
     strings over 100 KiB of live data read 48.8 MB, then 4.2, 1.1, 0.3, 0.1, and 0.05 MB across
     successive cycles. The verdict runs `expire_registry_values` then a full collection while the
     VM is over the cap and the previous pass freed at least an eighth of what it started from, at
     most eight passes (`ScriptWorker::collect_until_under`). A collection that itself errors fails
     the node with its message.
   - *It is rate-limited, counted, and runs after the send.* A cap under about twice the working
     set forced a full collection on every batch, 76% of one measured run. Forced verdicts run at
     most once per second, or ten times the last one's duration if longer, and count as
     `logit.script.vm.gc.forced` with a `.gc.duration` timing. An over-cap reading inside that
     window is skipped and the verdict deferred to the window's end, which the loop wakes for even
     with no batch arriving. The check runs after the batch's or `flush()`'s send, with the
     heartbeat idle, so the batch that crossed the cap reaches downstream rather than vanishing
     uncounted; the batches still in the node's inbox are counted as a revoked inbox's are. A
     memory failure is a returned error, not a panic, so it logs `memory_limit_exceeded` and never
     `thread_panicked`.
   - *The in-call check runs on every `Event.new` and is sticky.* Reading the VM's byte count is a
     field read, so every call compares; over the cap, a collection runs at most once per 1024
     calls, and one that leaves the VM still over trips a flag under which every later `Event.new`
     raises until the runtime clears it after the call. Without the flag,
     `pcall(Event.new, ..)` swallowed each error: 200,000 wrapped constructions reached 29 MB
     against a 4 MiB cap.
   - *Residual: the cap bounds the Lua VM heap only.* A retained event costs the VM about 149
     bytes while its payload stays in the Rust heap (10,000 retained 1 KiB events: 1.5 MB of VM,
     about 10 MB of Rust). Size the cap at least twice the script's steady working set, read from
     `logit.script.vm.memory`. A script that hoards events rather than Lua values is visible in
     process RSS, not bounded by this cap; `docs/known-gaps.md`'s Lua entry records it.
4. **A depth cap on Lua-to-Rust table conversion.** `MAX_TABLE_DEPTH = 128`, local to
   `logit-script`, matches native's `MAX_VALUE_DEPTH` so a value a script builds always decodes on
   a `logit_in` peer. A table nested past that depth is a clear conversion error, naming the
   attribute or field it came from, not a stack overflow. 128 levels convert; 129 fail.
5. **Table reads move to raw everywhere in the conversion path, in `luab/w1`.** The array branch
   of `lua_table_to_value`, and `events_from_table`, move to `raw_get`, matching the map branch
   and `validated_sequence_len`, so no Lua-to-Rust conversion runs a metamethod. The borrow on the
   event's `RefCell` is still released before conversion runs, because a GC finalizer can run
   script code at any allocation point, conversion or not.
6. **`print` goes to the self-log.** `ScriptWorker::new` replaces the global `print` with a Rust
   closure that calls the script-visible global `tostring` on each argument in turn — so a
   script's own `__tostring` metamethod or a redefined `tostring` renders the same way it would
   under Lua's real `print` (`luaB_print`) — tab-joins the results, and emits a `tracing::info!`
   line tagged with the component id. The id is shared with `with_component` through an
   `Rc<RefCell<Option<String>>>`, the `ProvenanceState` pattern (`provenance.rs`'s `component:
   Option<String>`, starting `None`), holding no id until `with_component` sets it (only
   top-level code during `.exec()` can print before then). It never touches process stdout. A
   leftover debug `print` is the accidental case this cluster's threat model names, and a
   self-log line is more useful to an operator than corrupted `stdio_out` output.
7. **`collectgarbage`, `setmetatable`, and `coroutine` stay in the sandbox, and `print` stays
   rerouted rather than removed (decision 6).** None of the three reaches the host filesystem,
   network, or process, and each has an ordinary use in a transform script (freeing memory early,
   building a read-only wrapper table, or structuring control flow). Removing them would narrow
   the scripting surface for no bound gained; a script that misuses one is covered by the memory
   cap (`collectgarbage`) or the depth cap (`setmetatable`-driven aliasing still converts through
   the same raw path).
8. **`newproxy` is removed from the sandbox.** `remove_unsandboxed_base_globals` nils it, and the
   `_G` allowlist test pins it absent. It was the only way a script could reach the SIGSEGV and
   the borrow panic in the Context section above, because it is the only way a script gets a
   `__gc` finalizer on LuaJIT at all. Removing it establishes the invariant the rest of the
   boundary depends on: **no script code runs during an mlua allocation.** That invariant is what
   makes `EventProxy::to_table` and `AttrsProxy::__index` sound holding a `RefCell` borrow across
   a call that allocates — a re-entrant write reaching either during that borrow has no path left
   to fire from, rather than a path defended at each site. `into_inner`'s `Err(rc)` arm, a bare
   clone today, gains a `debug_assert!(false)` in a debug build in `luab/w2`, so a debug build
   catches whatever else might reach it while a release build keeps the clone fallback.
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
    levels, regardless of build profile. `run_with_telemetry` spawns the thread that runs
    `run_lua` through `std::thread::Builder::stack_size`, set to 8 MiB. The extra is virtual
    address space, committed only as the stack grows, so an ordinary script pays nothing for it.

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
  process) is broken beyond recovery — a slow but progressing script or a large batch can look
  the same for a while. `Stalled` recovers to `Running` on its own; a hard failure is reserved for
  the cases that are terminal (a panic, an over-cap VM after a full GC, a wedge past its
  `shutdown_grace`).
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
- Interner growth from script-derived strings (`Event.new`'s and the proxy setters'
  `name`/`unit`/`description`/`event_name` fields, and nested attribute keys) stays a documented,
  accepted residual in `docs/known-gaps.md`, citing [ADR
  `deployment-threat-model`](deployment-threat-model.md), the way `telemetry`'s own tag values
  already are. A `newproxy(true)` finalizer touching a stashed handle during collection is no
  longer possible to write at all: decision 8 removes `newproxy`, closing the class rather than
  defending each site against it.
- A downstream `aggregate` window in flight when a Lua node wedges is not lost, and only because
  the wedge grace is shorter than a sink's: revoking the wedged node's I/O (decision 2) closes the
  downstream transform's inbox at `shutdown_at + shutdown_grace` (2s default) at the latest, so it
  flushes once — the way `run_transform` already does on inbox close — well before a downstream
  sink's own `write_loop` leaves at `shutdown_at + buffer.shutdown_grace` (5s default). A send
  from upstream of the wedged node, against its now-revoked fanout, counts `closed_consumer`
  rather than reaching it.
- **A loop that keeps calling `Event.new` is progress, not a stall.** Its heartbeat tick (decision
  1) advances on every construction, so it is never `Stalled` and never wedged. The variant that
  retains what it builds is bounded by `max_memory`'s in-call check (decision 3); the variant that
  allocates and drops without retaining stays undetected. Telling either apart from a large,
  legitimate `flush()` needs a time limit, which this record declines for the reasons decision 1
  already gives.
- `/healthz` stays `200` on a stalled node: the admin server itself is alive, and liveness and
  readiness answer different questions (decision 1). `/readyz`'s `503 stalled` is what an
  operator's own probe — the image's `HEALTHCHECK` under Docker or Swarm, or a Kubernetes
  `readinessProbe` pointed at `/readyz` — acts on, and [ADR
  `admin-readiness-endpoint`](admin-readiness-endpoint.md) gains `stalled` as a wire state beside
  `degraded`, not folded into it.
- Three residuals the depth cap and the larger stack don't close. A 128-deep value a script builds
  does not survive a relay through `otlp_out → otlp_in`: OTLP's own JSON and protobuf nesting
  limits are 41 and 49 levels, both under native's 128. The depth cap bounds a table's nesting,
  not its size, so a DAG a script builds by sharing table references (`t = {a = t, b = t}`
  repeated k times) still converts, at 2^k nodes. And pure-Lua recursion through Rust/C frames can
  still abort the process past the larger stack from decision 12, at a higher level than 233. The
  last two, and the `Event.new`-loop residual above, are recorded in `docs/known-gaps.md`'s Lua
  entry, citing [ADR `deployment-threat-model`](deployment-threat-model.md).

## Running it

Filled in as each workstream lands.

- `luab/w1` (CORE-17, CORE-18): #385. `crates/logit-script/src/value.rs`'s `depth_tests`
  (`a_table_nested_128_deep_converts`, `a_table_nested_129_deep_is_an_error_naming_the_cap`,
  `a_self_referencing_table_is_the_depth_error_not_a_stack_overflow`,
  `a_self_referencing_array_is_the_depth_error`,
  `an_array_tables_index_metamethod_never_fires_during_conversion`,
  `a_metamethod_that_writes_back_into_the_event_never_runs_during_an_attribute_write`,
  `a_bad_key_inside_a_nested_attribute_table_names_the_attribute`,
  `a_resource_and_a_scope_attribute_write_get_the_same_cap`); `construct.rs`'s
  `event_new_with_a_cyclic_attribute_table_is_a_clear_error`,
  `event_new_with_a_nested_metatable_array_reads_raw`, and
  `a_count_above_two_to_the_53_round_trips_through_to_table`; `proxy.rs`'s
  `a_metric_count_above_two_to_the_53_reads_as_a_decimal_string`; and
  `crates/logit-script/tests/event_new_fixed_point.rs`
  (`event_new_of_to_table_is_a_fixed_point_over_generated_events`,
  `a_fixed_point_event_survives_two_round_trips`). Both self-reference tests aborted the test
  process before the cap existed.
- `luab/w2` (CORE-16, CORE-15 sandbox half): #388. `crates/logit-script/src/lib.rs`'s
  `the_global_table_is_exactly_the_allowlist` and `print_writes_to_the_self_log_not_stdout`;
  `crates/logit-script/src/lifetime_tests.rs`'s six
  `a_stashed_{event,attributes,log,span,metrics,metric}_fails_clearly_in_the_next_process_and_in_flush`
  tests, `returning_the_same_event_twice_in_one_table_is_an_error_and_emits_nothing`,
  `returning_a_table_holding_an_aliased_event_invalidates_the_alias`,
  `returning_the_original_after_clone_leaves_the_clone_usable_in_flush`,
  `returning_a_sub_handle_is_the_contract_error`,
  `returning_a_non_userdata_in_a_table_is_the_contract_error`,
  `stashed_resource_scope_trace_and_provenance_read_the_fresh_root_in_flush`,
  `stashed_resource_and_scope_read_the_next_batchs_values`,
  `reading_every_sub_proxy_then_returning_keeps_the_no_clone_path`,
  `ten_thousand_dropped_events_that_touched_every_sub_proxy_keep_memory_flat` (Lua heap and Rust
  live bytes, the latter through a counting allocator local to the test module), and
  `a_script_error_traceback_names_the_script_not_a_rust_file`. The allowlist, `print`, both
  contract-error tests, and the traceback test failed before the change.
- `luab/w3` (RT-11, CORE-15 limits half): `logit_script::Heartbeat`, `NodeState::Stalled`, and the
  watcher's revocable `LuaIo`, pinned in `crates/logit-pipeline/src/runtime.rs` by
  `an_infinite_loop_script_is_reported_stalled_and_degrades_readyz`,
  `shutdown_with_a_wedged_script_revokes_its_io_and_returns_runtime_naming_it`,
  `batches_queued_in_a_revoked_inbox_are_counted_not_silently_lost`,
  `a_progressing_flush_emitting_many_events_is_never_stalled`,
  `a_loop_that_keeps_constructing_events_is_progress_not_a_stall`,
  `a_lua_node_blocked_on_a_full_sink_inbox_unparks_within_the_sinks_grace_and_returns_ok`,
  `a_later_script_failing_to_load_returns_startup_promptly`,
  `an_interval_tick_runs_flush_through_the_elapsed_branch`, and the paused-time watcher tests
  `watch_lua_thread_maps_each_outcome`, `a_busy_heartbeat_that_stops_advancing_is_stalled_and_resumes`,
  `an_idle_heartbeat_is_never_stalled`, `a_wedged_node_after_shutdown_has_its_io_revoked_and_fails`,
  `a_node_already_stalled_at_shutdown_is_revoked_on_the_next_tick`,
  `a_permit_reserved_before_revocation_is_drained_and_counted`;
  `readiness.rs`'s `has_stalled_node_reflects_any_stalled_component`; `admin.rs`'s
  `readyz_wire_matches_the_spec_table` and `a_stalled_node_turns_ready_into_stalled_and_back`;
  `heartbeat.rs`'s `enter_tick_leave_keep_the_busy_bit_and_advance`. RT-11 findings; CORE-15's time
  half addended.
- `luab/w4` (CORE-15 close, `max_memory`): #391. `lua`/`lua_file` `max_memory` and graph
  rule 71, pinned by `crates/logit-config`'s `lua_max_memory_parses_a_byte_count_string` and
  `lua_file_max_memory_is_optional`, `graph.rs`'s `a_lua_max_memory_of_zero_is_rejected` and
  `a_lua_max_memory_validates`, and `logit-cli`'s
  `build_spec_carries_max_memory_into_the_lua_runtime_config`; `crates/logit-script/src/memory.rs`'s
  `collect_until_under_keeps_collecting_while_a_pass_frees_enough`,
  `collect_until_under_stops_once_a_pass_frees_little`,
  `a_retaining_event_new_loop_trips_the_in_call_check`,
  `a_pcall_wrapped_event_new_loop_stays_tripped`, `a_discarding_event_new_loop_never_trips`, and
  `with_no_cap_event_new_never_checks_memory`; `runtime.rs`'s
  `a_lua_node_over_max_memory_fails_the_run_as_runtime_naming_it`,
  `garbage_over_max_memory_is_collected_before_the_node_is_failed`,
  `max_memory_is_checked_after_flush_too`, `a_memory_verdict_is_rate_limited`,
  `a_skipped_verdict_runs_once_the_window_ends_with_no_batch_arriving`, and
  `thread_outcome_reports_a_panic_payload_as_a_message`. Each new test failed first against a
  mutation of the behavior it pins (no verdict, no collection before it, no rate limit, no
  sticky trip, one pass only, no inbox sweep, a memory failure logged as a panic). CORE-15
  findings.
- `luab/w5` (CORE-19 close, docs): pending.
