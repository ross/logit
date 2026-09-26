//! Handle-lifetime tests: what a script sees when it keeps a handle past the call that gave it one,
//! returns a handle it shouldn't, or drops many events without returning them.
//!
//! The rule under test is `crate::proxy::EventProxy`'s: an event handle, and every sub-handle from
//! it, is consumed once the event is returned from `process()` or included in a `flush()` table.
//! `docs/design/lua-api.md`'s "Script contract" is the operator-facing copy.

use crate::proxy::tests::{
    log_event, log_record_with_everything, metric_event, metric_record, span_event_full, sum_kind,
};
use crate::proxy::EventProxy;
use crate::tests::{emitted, process_err, worker};
use crate::{ProcessOutcome, ScriptWorker};
use logit_core::interner::intern;
use logit_core::{AttrMap, Event, Provenance, Resource, Scope};
use std::sync::Arc;

/// Counts the bytes this thread holds live through the global allocator, so a test can see Rust
/// memory a leak would pin but `ScriptWorker::used_memory` can't (an `Rc<RefCell<Event>>` held by
/// a registry slot).
///
/// `logit-bench`'s `CountingAlloc` does the same, but `logit-bench` depends on this crate. The
/// counter is a `const`-initialized, destructor-free thread-local: any other kind allocates on
/// first access, and allocating from inside the allocator recurses.
mod counting {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    thread_local! {
        static LIVE: Cell<i64> = const { Cell::new(0) };
    }

    pub(super) fn live_bytes() -> i64 {
        LIVE.with(Cell::get)
    }

    fn bump(delta: i64) {
        // `try_with`: an allocation during thread teardown, after the local is gone, is dropped
        // from the count rather than panicking.
        let _ = LIVE.try_with(|live| live.set(live.get() + delta));
    }

    struct Counting;

    // SAFETY: every method forwards to `System`, which upholds `GlobalAlloc`'s contract; the
    // counting touches only a thread-local `Cell` and never allocates.
    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            bump(layout.size() as i64);
            System.alloc(layout)
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            bump(-(layout.size() as i64));
            System.dealloc(ptr, layout)
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            bump(layout.size() as i64);
            System.alloc_zeroed(layout)
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            bump(new_size as i64 - layout.size() as i64);
            System.realloc(ptr, layout, new_size)
        }
    }

    #[global_allocator]
    static ALLOCATOR: Counting = Counting;
}

/// An event carrying a log, one metric, and a span, so a script can reach every sub-proxy.
fn event_with_every_payload() -> Event {
    let mut event = span_event_full();
    event.log = Some(log_record_with_everything());
    event.metrics.push(metric_record(sum_kind()));
    event.attributes.insert("k", "v");
    event
}

/// `flush()`'s result as an error string; `flush` returns a `Vec`, which is `Debug`, but the
/// message reads better than `unwrap_err`'s panic.
fn flush_err(w: &ScriptWorker) -> String {
    match w.flush(0) {
        Err(err) => err.to_string(),
        Ok(events) => panic!("expected flush() to fail, got {} events", events.len()),
    }
}

/// Stashes `stash` (an expression over `event`) in a global and in an upvalue during the first
/// `process()`, which returns the event, then runs `use_` against the handle `h` from a later
/// `process()` and from `flush()`, once for each stash. Returns the four errors, in the order
/// global/process, upvalue/process, global/flush, upvalue/flush.
fn stash_errors(stash: &str, use_: &str, event: fn() -> Event) -> [String; 4] {
    let w = worker(&format!(
        r#"
        local up = nil
        function process(event)
            if not stashed then
                stashed = true
                g = {stash}
                up = {stash}
                return event
            end
            local h = (which == "global") and g or up
            {use_}
            return event
        end
        function flush()
            local h = (which == "global") and g or up
            {use_}
            return {{}}
        end
        "#
    ));
    emitted(w.process(event()).unwrap());
    let set_which = |which: &str| w.lua.globals().set("which", which).unwrap();
    set_which("global");
    let global_process = process_err(&w, event());
    set_which("upvalue");
    let upvalue_process = process_err(&w, event());
    set_which("global");
    let global_flush = flush_err(&w);
    set_which("upvalue");
    let upvalue_flush = flush_err(&w);
    [global_process, upvalue_process, global_flush, upvalue_flush]
}

/// The consumed-handle wording from `proxy::clarify_destructed_handle_use`, never mlua's own.
fn assert_consumed_wording(errors: &[String; 4]) {
    for err in errors {
        assert!(
            err.contains("was already returned/emitted elsewhere and can no longer be used"),
            "expected the consumed-handle wording, got: {err}"
        );
        assert!(!err.contains("destructed"), "mlua's own wording leaked through: {err}");
    }
}

#[test]
fn a_stashed_event_fails_clearly_in_the_next_process_and_in_flush() {
    assert_consumed_wording(&stash_errors("event", "local _ = h.timestamp", || {
        metric_event(sum_kind())
    }));
}

#[test]
fn a_stashed_attributes_fails_clearly_in_the_next_process_and_in_flush() {
    assert_consumed_wording(&stash_errors("event.attributes", r#"h.env = "prod""#, || {
        metric_event(sum_kind())
    }));
}

#[test]
fn a_stashed_log_fails_clearly_in_the_next_process_and_in_flush() {
    assert_consumed_wording(&stash_errors("event.log", "local _ = h.trace_id", log_event));
}

#[test]
fn a_stashed_span_fails_clearly_in_the_next_process_and_in_flush() {
    assert_consumed_wording(&stash_errors("event.span", "local _ = h.name", span_event_full));
}

#[test]
fn a_stashed_metrics_fails_clearly_in_the_next_process_and_in_flush() {
    assert_consumed_wording(&stash_errors("event.metrics", "local _ = #h", || {
        metric_event(sum_kind())
    }));
}

/// `event.metrics[i]` holds a `Weak`, not a cached registry entry, so it reports the mistake
/// itself (`proxy::metric_handle_consumed_error`) rather than through mlua's destructed marker.
#[test]
fn a_stashed_metric_fails_clearly_in_the_next_process_and_in_flush() {
    let errors = stash_errors("event.metrics[1]", "h.value = 99", || metric_event(sum_kind()));
    for err in &errors {
        assert!(
            err.contains(
                "event.metrics[1] belongs to an event that has already been returned from \
                 process() or included in a flush() table"
            ),
            "expected the consumed metric-handle wording, got: {err}"
        );
    }
}

#[test]
fn returning_the_same_event_twice_in_one_table_is_an_error_and_emits_nothing() {
    let w = worker("function process(event) return {event, event} end");
    let err = process_err(&w, metric_event(sum_kind()));
    assert!(err.contains("this event was already returned/emitted elsewhere"), "got: {err}");
}

#[test]
fn returning_a_table_holding_an_aliased_event_invalidates_the_alias() {
    let w = worker(
        r#"
        local pending = nil
        function process(event)
            pending = event
            return {event}
        end
        function flush()
            pending.attributes.late = "yes"
            return {}
        end
        "#,
    );
    match w.process(metric_event(sum_kind())).unwrap() {
        ProcessOutcome::EmitMany(events) => assert_eq!(events.len(), 1),
        _ => panic!("expected EmitMany"),
    }
    let err = flush_err(&w);
    assert!(err.contains("was already returned/emitted elsewhere"), "got: {err}");
}

#[test]
fn returning_the_original_after_clone_leaves_the_clone_usable_in_flush() {
    let w = worker(
        r#"
        local pending = nil
        function process(event)
            pending = event:clone()
            pending.attributes.copy = "yes"
            local _ = pending.log.trace_id
            local _ = pending.span.name
            local _ = pending.metrics[1].value
            return event
        end
        function flush()
            pending.attributes.flushed = "yes"
            local e = pending
            pending = nil
            return {e}
        end
        "#,
    );
    let returned = emitted(w.process(event_with_every_payload()).unwrap());
    assert!(returned.attributes.get("copy").is_none(), "the clone is independent of the original");

    let flushed = w.flush(0).unwrap();
    assert_eq!(flushed.len(), 1);
    let clone = &flushed[0].0;
    assert_eq!(clone.attributes.get("copy").and_then(|v| v.as_str()), Some("yes"));
    assert_eq!(clone.attributes.get("flushed").and_then(|v| v.as_str()), Some("yes"));
    assert!(clone.log.is_some() && clone.span.is_some() && clone.metrics.len() == 1);
}

const PROCESS_CONTRACT: &str = "process() must return nil, an event, or a table of events; got";
const FLUSH_CONTRACT: &str = "flush() must return nil or a table of events; got";

#[test]
fn returning_a_sub_handle_is_the_contract_error() {
    let not_an_event = "a userdata that isn't an event (e.g. event.attributes)";
    for body in ["return event.attributes", "return resource", "return event.metrics"] {
        let w = worker(&format!("function process(event) {body} end"));
        let err = process_err(&w, metric_event(sum_kind()));
        assert!(err.contains(&format!("{PROCESS_CONTRACT} {not_an_event}")), "{body}: {err}");
    }

    let w = worker("function process(event) return {event:clone(), event.log} end");
    let err = process_err(&w, log_event());
    assert!(err.contains(&format!("{PROCESS_CONTRACT} {not_an_event} at index 2")), "got: {err}");

    let w = worker(
        r#"
        function process(event) return nil end
        function flush() return {scope} end
        "#,
    );
    let err = flush_err(&w);
    assert!(err.contains(&format!("{FLUSH_CONTRACT} {not_an_event} at index 1")), "got: {err}");

    let w = worker(
        r#"
        function process(event) return nil end
        function flush() return resource end
        "#,
    );
    let err = flush_err(&w);
    assert!(err.contains(&format!("{FLUSH_CONTRACT} {not_an_event}")), "got: {err}");
}

/// A stale event returned bare from `flush()` is named as consumed, not as some other userdata.
#[test]
fn a_stale_event_returned_bare_from_flush_gets_the_consumed_handle_wording() {
    let w = worker(
        r#"
        local stashed = nil
        function process(event)
            stashed = event
            return event
        end
        function flush() return stashed end
        "#,
    );
    emitted(w.process(metric_event(sum_kind())).unwrap());
    let err = flush_err(&w);
    assert!(err.contains("this event was already returned/emitted elsewhere"), "got: {err}");
    assert!(!err.contains("isn't an event"), "got: {err}");
}

#[test]
fn returning_a_non_userdata_in_a_table_is_the_contract_error() {
    let w = worker("function process(event) return {1} end");
    let err = process_err(&w, metric_event(sum_kind()));
    assert!(err.contains(&format!("{PROCESS_CONTRACT} a number at index 1")), "got: {err}");

    let w = worker(r#"function process(event) return {event, "x"} end"#);
    let err = process_err(&w, metric_event(sum_kind()));
    assert!(err.contains(&format!("{PROCESS_CONTRACT} a string at index 2")), "got: {err}");

    let w = worker(
        r#"
        function process(event) return nil end
        function flush() return {{}} end
        "#,
    );
    let err = flush_err(&w);
    assert!(err.contains(&format!("{FLUSH_CONTRACT} a table at index 1")), "got: {err}");

    // A non-table, non-event return keeps the same prefix.
    let w = worker("function process(event) return 5 end");
    let err = process_err(&w, metric_event(sum_kind()));
    assert!(err.contains(&format!("{PROCESS_CONTRACT} a number")), "got: {err}");

    // A bare event from `flush()` is named as one, with the fix.
    let w = worker(
        r#"
        function process(event) return nil end
        function flush(now) return Event.new{timestamp = now} end
        "#,
    );
    let err = flush_err(&w);
    assert!(err.contains(&format!("{FLUSH_CONTRACT} an event (return {{event}})")), "got: {err}");
}

/// The script that reads the batch-scoped globals through stashes taken in the first `process()`
/// and writes what it read onto the event it returns: `process()` for the second call, and an
/// `Event.new` in `flush()`.
const STASHED_GLOBALS_SCRIPT: &str = r#"
    local r, s, t, p = nil, nil, nil, nil
    local function read_into(attrs)
        attrs.service = tostring(r["service.name"])
        attrs.scope_name = s.name
        attrs.trace_id = t.trace_id
        attrs.origin = tostring(p.origin)
    end
    function process(event)
        if r == nil then
            r, s, t, p = resource, scope, trace, provenance
            return nil
        end
        read_into(event.attributes)
        return event
    end
    function flush(now)
        local attrs = {}
        read_into(attrs)
        return {Event.new{timestamp = now, attributes = attrs}}
    end
"#;

/// Sets the batch-scoped globals the way `run_lua_loop` does before a batch's `process()` calls.
fn set_batch(w: &ScriptWorker, service: &str, scope_name: &str, trace_byte: u8, origin: &str) {
    let mut resource = Resource::default();
    resource.attributes.insert("service.name", service);
    w.set_resource(&Arc::new(resource));
    let scope = Scope {
        name: bytes::Bytes::copy_from_slice(scope_name.as_bytes()),
        version: bytes::Bytes::new(),
        attributes: AttrMap::new(),
        dropped_attributes_count: 0,
        schema_url: None,
    };
    w.set_scope(&Some(Arc::new(scope)));
    w.set_trace_context([trace_byte; 16], [trace_byte; 8]).unwrap();
    w.set_provenance(Provenance { origin: Some(intern(origin)), previous: Some(intern(origin)) });
}

fn attr(event: &Event, key: &str) -> String {
    event.attributes.get(key).and_then(|v| v.as_str()).unwrap_or("<missing>").to_string()
}

#[test]
fn stashed_resource_scope_trace_and_provenance_read_the_fresh_root_in_flush() {
    let w = worker(STASHED_GLOBALS_SCRIPT).with_component("enrich");
    set_batch(&w, "api", "lib", 0x11, "upstream");
    assert!(matches!(w.process(metric_event(sum_kind())).unwrap(), ProcessOutcome::Drop));

    // The flush root, as `run_lua_loop` sets it: a fresh trace, this component as provenance,
    // an empty resource, and no scope.
    w.set_trace_context([0x22; 16], [0x22; 8]).unwrap();
    w.set_provenance(Provenance {
        origin: Some(intern("enrich")),
        previous: Some(intern("enrich")),
    });
    w.set_resource(&Arc::new(Resource::default()));
    w.set_scope(&None);

    let flushed = w.flush(0).unwrap();
    assert_eq!(flushed.len(), 1);
    let event = &flushed[0].0;
    assert_eq!(attr(event, "service"), "nil");
    assert_eq!(attr(event, "scope_name"), "");
    assert_eq!(attr(event, "trace_id"), "22".repeat(16));
    assert_eq!(attr(event, "origin"), "enrich");
}

#[test]
fn stashed_resource_and_scope_read_the_next_batchs_values() {
    let w = worker(STASHED_GLOBALS_SCRIPT);
    set_batch(&w, "api", "lib", 0x11, "first");
    assert!(matches!(w.process(metric_event(sum_kind())).unwrap(), ProcessOutcome::Drop));

    set_batch(&w, "billing", "lib2", 0x33, "second");
    let event = emitted(w.process(metric_event(sum_kind())).unwrap());
    assert_eq!(attr(&event, "service"), "billing");
    assert_eq!(attr(&event, "scope_name"), "lib2");
    assert_eq!(attr(&event, "trace_id"), "33".repeat(16));
    assert_eq!(attr(&event, "origin"), "second");
}

/// Reads every sub-proxy (and a field through each), then hands the returned handle to
/// `EventProxy`'s own teardown by hand, so the strong count can be seen between teardown and
/// `Rc::try_unwrap`. `into_inner`'s `debug_assert!` guards the same path in every other test.
#[test]
fn reading_every_sub_proxy_then_returning_keeps_the_no_clone_path() {
    let w = worker(
        r#"
        function process(event)
            local _ = event.attributes.k
            local _ = event.log.trace_id
            local _ = #event.metrics
            local _ = event.metrics[1].value
            local _ = event.span.name
            local _ = event:to_table()
            return event
        end
        "#,
    );
    let process: mlua::Function = w.lua.registry_value(&w.process).unwrap();
    let returned: mlua::AnyUserData =
        process.call(EventProxy::new(event_with_every_payload())).unwrap();
    let proxy = returned.take::<EventProxy>().unwrap();
    assert_eq!(proxy.strong_count(), 5, "the four cached sub-proxies each hold the event");
    proxy.release_sub_proxies(&w.lua);
    assert_eq!(proxy.strong_count(), 1, "teardown must leave `Rc::try_unwrap` the only owner");
    let (event, _) = proxy.into_inner(&w.lua);
    assert_eq!(event.metrics.len(), 1);
}

/// Bytes of Lua heap a run may end above its warm-up after a full collection: LuaJIT's string
/// table and the registry's free list can each settle at a slightly different size.
const LUA_HEAP_SLACK: usize = 64 * 1024;
/// Rust bytes the same run may end above its warm-up. One pinned `Event` from
/// `event_with_every_payload` is a few KiB, so a leak of one per call over 10k calls is far past
/// this.
const RUST_LIVE_SLACK: i64 = 64 * 1024;

#[test]
fn ten_thousand_dropped_events_that_touched_every_sub_proxy_keep_memory_flat() {
    let w = worker(
        r#"
        function process(event)
            local _ = event.attributes.k
            local _ = event.log.trace_id
            local _ = #event.metrics
            local _ = event.metrics[1].value
            local _ = event.span.name
            local _ = event:to_table()
            return nil
        end
        "#,
    );
    let run = |n: usize| {
        for _ in 0..n {
            assert!(matches!(w.process(event_with_every_payload()).unwrap(), ProcessOutcome::Drop));
        }
        w.lua.expire_registry_values();
        w.lua.load(r#"collectgarbage("collect") collectgarbage("collect")"#).exec().unwrap();
    };

    run(1_000);
    let lua_after_warm_up = w.used_memory();
    let rust_after_warm_up = counting::live_bytes();

    run(10_000);
    let lua_after = w.used_memory();
    let rust_after = counting::live_bytes();

    assert!(
        lua_after <= lua_after_warm_up + LUA_HEAP_SLACK,
        "Lua heap grew from {lua_after_warm_up} to {lua_after} bytes over 10k dropped events"
    );
    assert!(
        rust_after - rust_after_warm_up <= RUST_LIVE_SLACK,
        "Rust live bytes grew from {rust_after_warm_up} to {rust_after} over 10k dropped events"
    );
}

#[test]
fn a_script_error_traceback_names_the_script_not_a_rust_file() {
    let w = worker(
        r#"
        function process(event)
            local missing = nil
            return missing.field
        end
        "#,
    );
    let err = process_err(&w, metric_event(sum_kind()));
    assert!(err.contains("script:4:"), "expected the script's own line, got: {err}");
    assert!(!err.contains(".rs"), "a traceback must not cite a Rust source file: {err}");
}
