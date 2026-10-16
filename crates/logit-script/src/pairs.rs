//! The sandbox's `pairs`: a Lua wrapper over the original that also enumerates the three
//! attribute proxies, `event.attributes`, `resource`, and `scope.attributes`. LuaJIT has no
//! `__pairs`, so this is that metamethod done once in the sandbox. See `docs/design/lua-api.md`'s
//! "Enumerating attributes with `pairs`" section and `docs/adr/lua-in-place-edits.md`.
//!
//! Installed in [`crate::ScriptWorker::new`] before the script's source runs, so a top-level
//! `local pairs = pairs` captures the wrapper (`crate::trace`'s module doc has the reason).
//!
//! - **The wrapper is Lua, not Rust.** For a table it tail-calls the original, which returns the
//!   builtin `next`, so the loop keeps its `ITERN` specialization; with the JIT on, a Rust `pairs`
//!   would also stop the loop compiling. It costs about 13 ns per call on a table in the
//!   interpreted sandbox, and nothing per step. A call site that ever sees a proxy loses `ITERN`
//!   for good, and its plain-table loops run about 60% slower.
//! - **The iterator is stateless**: one Rust function, created here and shared by every loop. A
//!   step receives `({proxy, bound}, last_key)`, resolves the key without interning it, and seeks
//!   the first entry after it ([`logit_core::AttrMap::entry_after`]). Deleting or overwriting the
//!   current key during the loop is safe.
//! - **The loop is bounded when `pairs` is called**, at the interning index of the map's last key
//!   ([`bound_of`]), so it always ends. A key added during the loop is never visited if it's new
//!   to the process, and may be visited if it was interned before.
//! - **It holds no `Rc` to the event.** The loop's state holds the proxy userdata, so
//!   [`crate::EventProxy::into_inner`]'s `Rc::try_unwrap` still finds no clone, and a stashed
//!   iterator used after the event is returned gets the destructed-handle error.

use crate::{proxy, resource, scope};
use logit_core::interner::{index, lookup, resolve};
use logit_core::AttrMap;
use mlua::{AnyUserData, Function, Lua, Value as LuaValue};

/// Takes the original `pairs`, `type`, and `error`, the bound lookup, and the iterator, so a
/// script that reassigns any of those globals doesn't change what the wrapper does.
/// `error(.., 2)` puts the caller's line on a refusal, as the original's own argument error does.
///
/// `bound_of` returns `false` for a userdata that isn't an attribute proxy. The loop's state is
/// `{proxy, bound}`, a Lua table, so it holds the proxy and never the event.
const WRAPPER: &str = r#"
local base_pairs, type, error, bound_of, step = ...
return function(t)
  local ty = type(t)
  if ty == "table" then
    return base_pairs(t)
  end
  if ty == "userdata" then
    local bound = bound_of(t)
    if bound > -2 then
      return step, {t, bound}, nil
    end
  end
  error("bad argument #1 to 'pairs' (table expected, got " .. ty .. ")", 2)
end
"#;

/// Replaces the `pairs` global with the wrapper.
pub(crate) fn install(lua: &Lua) -> mlua::Result<()> {
    let globals = lua.globals();
    let base_pairs: Function = globals.get("pairs")?;
    let type_fn: Function = globals.get("type")?;
    let error_fn: Function = globals.get("error")?;
    let bound_of = lua.create_function(bound_of)?;
    let step = lua.create_function(step)?;
    let wrapper: Function = lua
        .load(WRAPPER)
        .set_name("=pairs")
        .call((base_pairs, type_fn, error_fn, bound_of, step))?;
    globals.set("pairs", wrapper)
}

/// Runs `f` on the attributes of whichever attribute proxy `ud` is; `Ok(None)` for any other
/// userdata. `proxy::with_attrs` runs first because it's the one that reports a torn-down handle.
fn with_proxy_attrs<R>(
    ud: &AnyUserData,
    f: impl FnOnce(&AttrMap) -> mlua::Result<R>,
) -> mlua::Result<Option<R>> {
    // Each helper takes its closure by value, so `f` rides in an `Option` the matching one empties.
    let mut f = Some(f);
    let mut call = |attrs: &AttrMap| (f.take().expect("only one helper matches"))(attrs);
    if let Some(r) = proxy::with_attrs(ud, &mut call)? {
        return Ok(Some(r));
    }
    if let Some(r) = resource::with_attrs(ud, &mut call)? {
        return Ok(Some(r));
    }
    scope::with_attrs(ud, &mut call)
}

/// The loop's bound: the interning index of the map's last key when `pairs` is called, `-1` for
/// an empty map, or `false` if `ud` isn't an attribute proxy.
///
/// Without it, a loop that adds a key each step never ends: a key interned for the first time
/// sorts after every existing one, so each step would find a new entry past the last. The
/// copy-on-write copy of `resource` or `scope` keeps every key's `Symbol`, so the bound holds when
/// a write mid-loop switches the step from the base to the copy.
fn bound_of<'lua>(_: &'lua Lua, ud: AnyUserData<'lua>) -> mlua::Result<i64> {
    Ok(match with_proxy_attrs(&ud, |attrs| Ok(attrs.last_key()))? {
        None => -2,
        Some(None) => -1,
        Some(Some(last)) => index(last) as i64,
    })
}

/// One step of a `pairs` loop over a proxy: the entry after `last`, or `nil` to end the loop once
/// the next entry sorts past the bound. A `Null` value reads as `nil`, which a generic `for`
/// passes through, because only a `nil` key ends it.
fn step<'lua>(
    lua: &'lua Lua,
    (state, last): (LuaValue<'lua>, Option<mlua::String<'lua>>),
) -> mlua::Result<(LuaValue<'lua>, LuaValue<'lua>)> {
    let LuaValue::Table(state) = state else {
        return Err(mlua::Error::RuntimeError(
            "the pairs iterator takes the state pairs returned with it".to_string(),
        ));
    };
    let ud: AnyUserData = state.raw_get(1)?;
    let bound: i64 = state.raw_get(2)?;
    let after = match &last {
        None => None,
        // A key the interner has never seen is in no map, and the iterator only ever hands back
        // keys it read from one, so a miss means the caller passed its own control value.
        Some(key) => match lookup(key.to_str()?) {
            Some(sym) => Some(sym),
            None => return Ok((LuaValue::Nil, LuaValue::Nil)),
        },
    };
    let entry = with_proxy_attrs(&ud, |attrs| match attrs.entry_after(after) {
        Some((key, value)) if index(key) as i64 <= bound => Ok((
            LuaValue::String(lua.create_string(resolve(key))?),
            crate::value::value_to_lua(lua, value)?,
        )),
        _ => Ok((LuaValue::Nil, LuaValue::Nil)),
    })?;
    entry.ok_or_else(|| {
        mlua::Error::RuntimeError(
            "the pairs iterator takes event.attributes, resource, or scope.attributes".to_string(),
        )
    })
}

#[cfg(test)]
mod tests {
    use crate::proxy::tests::log_record_with_everything;
    use crate::tests::{emitted, process_err, worker};
    use logit_core::interner::resolve;
    use logit_core::{AttrMap, Event, Resource, Scope, Value};
    use std::sync::Arc;

    fn attrs(pairs: &[(&str, Value)]) -> AttrMap {
        let mut map = AttrMap::new();
        for (k, v) in pairs {
            map.insert(k, v.clone());
        }
        map
    }

    fn log_event(attributes: AttrMap) -> Event {
        Event::log(1, attributes, log_record_with_everything())
    }

    /// The `k=v` list a script's loop should produce for `map`, in its iteration order.
    fn expected_listing(map: &AttrMap) -> String {
        map.iter()
            .map(|(k, v)| {
                let v = match v {
                    Value::Null => "nil".to_string(),
                    Value::Bool(b) => b.to_string(),
                    Value::I64(n) => n.to_string(),
                    other => other.as_str().expect("a string value").to_string(),
                };
                format!("{}={v}", resolve(k))
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Lists what a loop over `iterable` yields, as `k=v` joined by `,`, into the event's `seen`
    /// attribute after the loop, since a key added during one may or may not be visited.
    fn listing_script(iterable: &str) -> String {
        format!(
            r#"
            function process(event)
                local out = {{}}
                for k, v in pairs({iterable}) do
                    out[#out + 1] = k .. "=" .. tostring(v)
                end
                event.attributes.seen = table.concat(out, ",")
                return event
            end
            "#
        )
    }

    fn seen(event: &Event) -> &str {
        event.attributes.get("seen").and_then(Value::as_str).expect("the script sets `seen`")
    }

    #[test]
    fn pairs_over_event_attributes_yields_every_entry_in_order() {
        let map = attrs(&[
            ("host", Value::str("web-1")),
            ("retries", Value::I64(3)),
            ("cached", Value::Bool(true)),
            ("gone", Value::Null),
            ("env", Value::str("prod")),
        ]);
        let expected = expected_listing(&map);
        let w = worker(&listing_script("event.attributes"));
        let out = emitted(w.process(log_event(map)).unwrap());
        assert_eq!(seen(&out), expected);
    }

    #[test]
    fn pairs_over_resource_yields_every_entry_in_order() {
        let map = attrs(&[("service.name", Value::str("nginx")), ("host.name", Value::str("h"))]);
        let expected = expected_listing(&map);
        let w = worker(&listing_script("resource"));
        w.set_resource(&Arc::new(Resource { attributes: map, ..Resource::default() }));
        let out = emitted(w.process(log_event(AttrMap::new())).unwrap());
        assert_eq!(seen(&out), expected);
    }

    #[test]
    fn pairs_over_scope_attributes_yields_every_entry_in_order() {
        let map = attrs(&[("library", Value::str("otel")), ("level", Value::I64(2))]);
        let expected = expected_listing(&map);
        let w = worker(&listing_script("scope.attributes"));
        w.set_scope(&Some(Arc::new(Scope { attributes: map, ..Scope::default() })));
        let out = emitted(w.process(log_event(AttrMap::new())).unwrap());
        assert_eq!(seen(&out), expected);
    }

    #[test]
    fn pairs_over_a_batch_with_no_scope_yields_nothing() {
        let w = worker(&listing_script("scope.attributes"));
        w.set_scope(&None);
        let out = emitted(w.process(log_event(AttrMap::new())).unwrap());
        assert_eq!(seen(&out), "");
    }

    /// `examples/prusa/prusa.lua`'s loop: deleting the current key keeps the iterator's place.
    #[test]
    fn deleting_the_current_key_during_the_loop_visits_every_key() {
        let w = worker(
            r#"
            function process(event)
                for key in pairs(event.attributes) do
                    if string.sub(key, 1, 7) == "syslog." then
                        event.attributes[key] = nil
                    end
                end
                return event
            end
            "#,
        );
        let map = attrs(&[
            ("syslog.facility", Value::str("user")),
            ("syslog.severity", Value::str("info")),
            ("host", Value::str("web-1")),
            ("syslog.hostname", Value::str("printer")),
            ("syslog.appname", Value::str("prusa")),
            ("mac_address", Value::str("aa:bb")),
        ]);
        let out = emitted(w.process(log_event(map)).unwrap());
        let mut keys: Vec<&str> = out.attributes.iter().map(|(k, _)| resolve(k)).collect();
        keys.sort();
        assert_eq!(keys, ["host", "mac_address"]);
    }

    #[test]
    fn overwriting_the_current_key_during_the_loop_visits_every_key_once() {
        let w = worker(
            r#"
            function process(event)
                local a = event.attributes
                for k, v in pairs(a) do
                    a[k] = v .. "!"
                end
                return event
            end
            "#,
        );
        let map = attrs(&[("a", Value::str("1")), ("b", Value::str("2")), ("c", Value::str("3"))]);
        let out = emitted(w.process(log_event(map)).unwrap());
        for (key, want) in [("a", "1!"), ("b", "2!"), ("c", "3!")] {
            assert_eq!(out.attributes.get(key).and_then(Value::as_str), Some(want), "{key}");
        }
    }

    /// A loop that adds a key each step ends: a key interned for the first time sorts after every
    /// existing one, and the loop is bounded at the map's last key when `pairs` is called. The
    /// guard turns an unbounded loop into a failure instead of a hang.
    #[test]
    fn a_rename_loop_adding_a_new_key_each_step_ends() {
        let w = worker(
            r#"
            function process(event)
                local a, steps = event.attributes, 0
                for k, v in pairs(a) do
                    steps = steps + 1
                    if steps > 100 then
                        error("the loop didn't end")
                    end
                    a["renamed." .. k] = v
                    a[k] = nil
                end
                return event
            end
            "#,
        );
        let map = attrs(&[
            ("rename_loop.a", Value::str("1")),
            ("rename_loop.b", Value::str("2")),
            ("rename_loop.c", Value::str("3")),
        ]);
        let out = emitted(w.process(log_event(map)).unwrap());
        let mut keys: Vec<(&str, &str)> = out
            .attributes
            .iter()
            .map(|(k, v)| (resolve(k), v.as_str().expect("a string value")))
            .collect();
        keys.sort();
        assert_eq!(
            keys,
            [
                ("renamed.rename_loop.a", "1"),
                ("renamed.rename_loop.b", "2"),
                ("renamed.rename_loop.c", "3")
            ]
        );
    }

    /// A `Null` entry yields `(key, nil)` without ending the loop, and the loop can delete it.
    #[test]
    fn a_null_entry_yields_nil_and_can_be_deleted() {
        let w = worker(
            r#"
            function process(event)
                local count = 0
                for k, v in pairs(event.attributes) do
                    count = count + 1
                    if v == nil then
                        event.attributes[k] = nil
                    end
                end
                event.attributes.count = count
                return event
            end
            "#,
        );
        let map = attrs(&[
            ("first", Value::Null),
            ("kept", Value::str("x")),
            ("middle", Value::Null),
            ("last", Value::str("y")),
        ]);
        let out = emitted(w.process(log_event(map)).unwrap());
        assert_eq!(out.attributes.get("count"), Some(&Value::I64(4)));
        assert_eq!(out.attributes.get("first"), None);
        assert_eq!(out.attributes.get("middle"), None);
        assert_eq!(out.attributes.get("kept").and_then(Value::as_str), Some("x"));
        assert_eq!(out.attributes.get("last").and_then(Value::as_str), Some("y"));
    }

    /// The first and last keys in `map`'s iteration order.
    fn first_and_last(map: &AttrMap) -> (&'static str, &'static str) {
        let keys: Vec<&'static str> = map.iter().map(|(k, _)| resolve(k)).collect();
        (keys[0], keys[keys.len() - 1])
    }

    /// The loop's first step overwrites the last key, so the step that reaches it must read the
    /// copy-on-write copy, not the batch's base.
    fn mid_loop_write_script(iterable: &str, first: &str, last: &str) -> String {
        format!(
            r#"
            function process(event)
                local out = {{}}
                for k, v in pairs({iterable}) do
                    if k == "{first}" then
                        {iterable}["{last}"] = "changed"
                    end
                    out[#out + 1] = k .. "=" .. tostring(v)
                end
                event.attributes.seen = table.concat(out, ",")
                return event
            end
            "#
        )
    }

    #[test]
    fn a_resource_write_earlier_in_the_loop_is_seen_by_a_later_step() {
        let map = attrs(&[("r.one", Value::str("1")), ("r.two", Value::str("2"))]);
        let (first, last) = first_and_last(&map);
        let w = worker(&mid_loop_write_script("resource", first, last));
        w.set_resource(&Arc::new(Resource { attributes: map, ..Resource::default() }));
        let out = emitted(w.process(log_event(AttrMap::new())).unwrap());
        assert!(seen(&out).ends_with(&format!("{last}=changed")), "{}", seen(&out));
        assert!(w.take_resource().is_some(), "the write commits");
    }

    #[test]
    fn a_scope_write_earlier_in_the_loop_is_seen_by_a_later_step() {
        let map = attrs(&[("s.one", Value::str("1")), ("s.two", Value::str("2"))]);
        let (first, last) = first_and_last(&map);
        let w = worker(&mid_loop_write_script("scope.attributes", first, last));
        w.set_scope(&Some(Arc::new(Scope { attributes: map, ..Scope::default() })));
        let out = emitted(w.process(log_event(AttrMap::new())).unwrap());
        assert!(seen(&out).ends_with(&format!("{last}=changed")), "{}", seen(&out));
        assert!(w.take_scope().is_some(), "the write commits");
    }

    /// An attribute named like one of `resource`'s fixed fields or methods is enumerated, though
    /// `resource[k]` reads the field; `= nil` on a method's name deletes the attribute.
    #[test]
    fn pairs_over_resource_yields_attributes_a_fixed_name_shadows() {
        let w = worker(
            r#"
            function process(event)
                local out = {}
                for k, v in pairs(resource) do
                    out[#out + 1] = k .. "=" .. v
                end
                table.sort(out)
                event.attributes.seen = table.concat(out, ",")
                resource.to_table = nil
                return event
            end
            "#,
        );
        let map = attrs(&[("schema_url", Value::str("attr")), ("to_table", Value::str("attr"))]);
        w.set_resource(&Arc::new(Resource { attributes: map, ..Resource::default() }));
        let out = emitted(w.process(log_event(AttrMap::new())).unwrap());
        assert_eq!(seen(&out), "schema_url=attr,to_table=attr");
        let resource = w.take_resource().expect("the delete commits");
        assert_eq!(resource.attributes.get("to_table"), None);
        assert!(resource.attributes.get("schema_url").is_some());
    }

    #[test]
    fn pairs_over_a_plain_table_is_unchanged() {
        let w = worker(
            r#"
            function process(event)
                local t = setmetatable({b = 2, a = 1, [1] = "x"}, {__index = function() return 0 end})
                local out = {}
                for k, v in pairs(t) do
                    out[#out + 1] = tostring(k) .. "=" .. tostring(v)
                end
                table.sort(out)
                local f, s, c = pairs(t)
                event.attributes.seen = table.concat(out, ",")
                event.attributes.next_is_builtin = f == next and s == t and c == nil
                return event
            end
            "#,
        );
        let out = emitted(w.process(log_event(AttrMap::new())).unwrap());
        assert_eq!(seen(&out), "1=x,a=1,b=2");
        assert_eq!(out.attributes.get("next_is_builtin"), Some(&Value::Bool(true)));
    }

    /// The wrapper is in `_G` before the chunk runs, so a top-level alias captures it.
    #[test]
    fn a_top_level_alias_of_pairs_captures_the_wrapper() {
        let w = worker(
            r#"
            local pairs = pairs
            function process(event)
                local n = 0
                for _ in pairs(event.attributes) do
                    n = n + 1
                end
                event.attributes.count = n
                return event
            end
            "#,
        );
        let map = attrs(&[("a", Value::str("1")), ("b", Value::str("2"))]);
        let out = emitted(w.process(log_event(map)).unwrap());
        assert_eq!(out.attributes.get("count"), Some(&Value::I64(2)));
    }

    /// A refusal names the script's line, not the wrapper's, as the original `pairs` does.
    #[test]
    fn pairs_of_a_non_table_names_the_script_line() {
        for (arg, got) in [("42", "number"), ("event.log", "userdata"), ("event", "userdata")] {
            let w = worker(&format!(
                "function process(event)\n  for k in pairs({arg}) do end\n  return event\nend\n"
            ));
            let err = process_err(&w, log_event(AttrMap::new()));
            assert!(
                err.contains(&format!(
                    "script:2: bad argument #1 to 'pairs' (table expected, got {got})"
                )),
                "{arg}: {err}"
            );
        }
    }

    /// The wrapper covers `pairs` only.
    #[test]
    fn next_and_ipairs_of_a_proxy_still_raise() {
        for call in ["next(event.attributes)", "ipairs(event.attributes)"] {
            let w = worker(&format!("function process(event)\n  {call}\n  return event\nend\n"));
            let err = process_err(&w, log_event(AttrMap::new()));
            assert!(err.contains("table expected, got userdata"), "{call}: {err}");
        }
    }

    /// The loop's state is the proxy, not a clone of the event's `Rc`: with the iterator triple
    /// still reachable from a global, a held `Rc` would trip `into_inner`'s no-clone
    /// `debug_assert` on return. Once the event is returned, the stashed iterator fails as any
    /// torn-down handle does.
    #[test]
    fn the_iterator_holds_no_reference_to_the_event() {
        let w = worker(
            r#"
            function process(event)
                stashed = {pairs(event.attributes)}
                for _ in pairs(event.attributes) do end
                return event
            end

            function flush()
                local step, state = stashed[1], stashed[2]
                step(state, nil)
                return {}
            end
            "#,
        );
        let map = attrs(&[("a", Value::str("1"))]);
        let out = emitted(w.process(log_event(map)).unwrap());
        assert_eq!(out.attributes.get("a").and_then(Value::as_str), Some("1"));
        let err = match w.flush(0) {
            Err(err) => err.to_string(),
            Ok(_) => panic!("expected the stashed iterator to fail"),
        };
        assert!(err.contains("was already returned/emitted elsewhere"), "{err}");
    }

    /// `pairs` on a stashed, torn-down `event.attributes` reports it consumed, not a type error.
    #[test]
    fn pairs_of_a_torn_down_proxy_reports_it_consumed() {
        let w = worker(
            r#"
            function process(event)
                stashed = event.attributes
                return event
            end

            function flush()
                for _ in pairs(stashed) do end
                return {}
            end
            "#,
        );
        emitted(w.process(log_event(AttrMap::new())).unwrap());
        let err = match w.flush(0) {
            Err(err) => err.to_string(),
            Ok(_) => panic!("expected pairs on a torn-down proxy to fail"),
        };
        assert!(err.contains("was already returned/emitted elsewhere"), "{err}");
    }
}
