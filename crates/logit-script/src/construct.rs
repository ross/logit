//! `Event.new(t)`: constructs an [`Event`] from a plain Lua table in exactly the shape
//! `event:to_table()` returns (`crate::proxy`'s `to_table`/`log_to_table`), and hands it back as
//! an ordinary [`EventProxy`] handle. The inverse of `to_table()`, by design -- same keys, same
//! encodings (decimal-digit strings for nanosecond timestamps, lowercase hex for ids, lowercase
//! enum names), same nesting -- so `Event.new(e:to_table())` round-trips every lossless shape.
//! See `docs/adr/lua-event-constructor.md` for the decision and `docs/design/lua-api.md`'s
//! "Constructing events" for the script-facing contract.
//!
//! Three rules every parser in this module holds to, so a mistake in a script is a clear error
//! at the call rather than a silently different event:
//!
//! - **Strict keys.** An unknown key anywhere -- top level or a sub-table -- is
//!   `Event.new: <path> is not a field`, the same posture the proxies take for an unknown field
//!   on read or write. `has_log`/`has_metrics`/`has_span` are accepted (they are in `to_table()`'s
//!   output) and must be booleans, but their values are ignored: the payload keys are the truth.
//! - **Raw table access.** Keys are read with `Table::raw_get` and enumerated with `Table::pairs`,
//!   which in mlua 0.9 walks the table with `lua_next` (`TablePairs::next`) -- raw, so neither
//!   `__index` nor `__pairs` (which LuaJIT lacks anyway) can make the key check and the field
//!   reads disagree about what the table holds.
//! - **Defaults only where core already documents one** (`BodyFormat::Raw`, `observed_timestamp`
//!   and `dropped_attributes_count` of `0`, empty `attributes`); `timestamp` and `log.message`
//!   are required, exactly as the ADR lists.
//!
//! Every error is an `mlua::Error::RuntimeError` prefixed `Event.new: <path> ...`; the shared
//! helpers at the bottom take the path as an argument so the metric (W3/W4) and span (W5)
//! parsers of `docs/plans/lua-event-constructor.md` can reuse them unchanged. `metrics` and
//! `span` are recognized here but not yet constructible -- each says so.

use crate::proxy::{EventProxy, TargetTable};
use crate::value::{lua_table_to_attrmap, lua_to_value, validated_sequence_len};
use logit_core::interner::intern;
use logit_core::trace::{parse_span_id, parse_trace_id, TraceRef};
use logit_core::{AttrMap, BodyFormat, Event, LogRecord, Severity, Value};
use mlua::{Lua, Table, Value as LuaValue};
use std::cell::RefCell;
use std::rc::Rc;

/// The keys `to_table()` emits at the top level -- and therefore the only keys `Event.new`
/// accepts there.
const EVENT_KEYS: &[&str] =
    &["timestamp", "attributes", "log", "metrics", "span", "has_log", "has_metrics", "has_span"];

/// The keys `log_to_table` emits.
const LOG_KEYS: &[&str] = &[
    "trace_id",
    "span_id",
    "trace_flags",
    "message",
    "severity",
    "body_format",
    "event_name",
    "observed_timestamp",
    "dropped_attributes_count",
];

/// Installs the `Event` global -- a table holding one function, `new` -- following
/// `telemetry::install`'s shape. `targets` is the worker's routing table *cell*
/// (`ScriptWorker::targets`): the closure reads through it on every call rather than capturing
/// the table itself, so a constructed event resolves `event:to(id)` against whatever
/// `ScriptWorker::with_targets` installed by the time `Event.new` actually runs -- inside
/// `process()`/`flush()`, the list the component declared; at script top level (during
/// `ScriptWorker::new`'s `.exec()`, before `with_targets` can have been called), the empty list.
pub(crate) fn install(lua: &Lua, targets: Rc<RefCell<Rc<TargetTable>>>) -> mlua::Result<()> {
    let table = lua.create_table()?;
    let new = lua.create_function(move |_, arg: LuaValue| {
        let LuaValue::Table(t) = arg else {
            return Err(runtime_error(format!(
                "Event.new(t) takes a table, got {}",
                arg.type_name()
            )));
        };
        // An `Rc` bump, never an allocation -- and released before `event_from_table` runs so a
        // constructor error can't leave the cell borrowed.
        let targets = targets.borrow().clone();
        event_from_table(t).map(|event| EventProxy::with_targets(event, targets))
    })?;
    table.set("new", new)?;
    lua.globals().set("Event", table)
}

/// Builds an [`Event`] from a table in `to_table()`'s top-level shape. See the module doc for
/// the rules; the per-field contract is `docs/design/lua-api.md`'s "Constructing events".
pub(crate) fn event_from_table(t: Table) -> mlua::Result<Event> {
    expect_keys(&t, EVENT_KEYS, "")?;
    let timestamp = match t.raw_get::<_, LuaValue>("timestamp")? {
        LuaValue::Nil => return Err(required("", "timestamp")),
        value => nanos_string(value, "", "timestamp")?,
    };
    let attributes = match t.raw_get::<_, LuaValue>("attributes")? {
        LuaValue::Nil => AttrMap::new(),
        LuaValue::Table(attrs) => attributes_from_table(attrs, "attributes")?,
        other => {
            return Err(runtime_error(format!(
                "Event.new: attributes must be a table (or nil), got {}",
                other.type_name()
            )))
        }
    };
    for key in ["has_log", "has_metrics", "has_span"] {
        // Accepted because `to_table()` emits them; ignored because the payload keys below are
        // the truth (ADR `lua-event-constructor`). Still type-checked, so a script that wrote
        // `has_log = "yes"` hears about it.
        boolean_field(&t, "", key)?;
    }
    match t.raw_get::<_, LuaValue>("metrics")? {
        LuaValue::Nil => {}
        LuaValue::Table(metrics) => match validated_sequence_len(&metrics)? {
            // `to_table()` always emits `metrics`, empty when the event carries none, so the
            // empty sequence must be accepted for the round-trip to hold.
            Some(0) => {}
            // W3/W4 of `docs/plans/lua-event-constructor.md` replace this arm with
            // `metric_from_table` per kind.
            Some(_) => {
                return Err(runtime_error(
                    "Event.new: metrics[1] is not constructible yet".to_string(),
                ))
            }
            None => return Err(metrics_not_a_sequence()),
        },
        _ => return Err(metrics_not_a_sequence()),
    }
    match t.raw_get::<_, LuaValue>("span")? {
        LuaValue::Nil => {}
        // W5 of `docs/plans/lua-event-constructor.md` replaces this arm with `span_from_table`.
        LuaValue::Table(_) => {
            return Err(runtime_error("Event.new: span is not constructible yet".to_string()))
        }
        other => {
            return Err(runtime_error(format!(
                "Event.new: span must be a table (or nil), got {}",
                other.type_name()
            )))
        }
    }
    let log = match t.raw_get::<_, LuaValue>("log")? {
        LuaValue::Nil => None,
        LuaValue::Table(log) => Some(log_from_table(log, "log")?),
        other => {
            return Err(runtime_error(format!(
                "Event.new: log must be a table (or nil), got {}",
                other.type_name()
            )))
        }
    };
    let mut event = Event::empty(timestamp, attributes);
    event.log = log;
    Ok(event)
}

/// Builds a [`LogRecord`] from a table in `log_to_table`'s shape. `path` is the table's own
/// path for error messages (`log` from the top level).
fn log_from_table(t: Table, path: &str) -> mlua::Result<LogRecord> {
    expect_keys(&t, LOG_KEYS, path)?;
    let message = match t.raw_get::<_, LuaValue>("message")? {
        // `to_table()` emits a `Value::Null` message as an absent key, so it comes back as
        // missing here -- the ADR's recorded residual, not fixed by inventing a default.
        LuaValue::Nil => return Err(required(path, "message")),
        value => value_field(value, path, "message")?,
    };
    let severity = enum_field(
        t.raw_get("severity")?,
        path,
        "severity",
        &Severity::NAMES,
        Severity::from_name,
    )?;
    let body_format = enum_field(
        t.raw_get("body_format")?,
        path,
        "body_format",
        &BodyFormat::NAMES,
        BodyFormat::from_name,
    )?
    .unwrap_or(BodyFormat::Raw);
    let trace = trace_ref_from_fields(
        t.raw_get("trace_id")?,
        t.raw_get("span_id")?,
        t.raw_get("trace_flags")?,
        path,
    )?;
    let event_name = match t.raw_get::<_, LuaValue>("event_name")? {
        LuaValue::Nil => None,
        LuaValue::String(s) => Some(intern(s.to_str()?)),
        other => {
            return Err(runtime_error(format!(
                "Event.new: {} must be a string or nil, got {}",
                dotted(path, "event_name"),
                other.type_name()
            )))
        }
    };
    let observed_timestamp = match t.raw_get::<_, LuaValue>("observed_timestamp")? {
        LuaValue::Nil => 0,
        value => nanos_string(value, path, "observed_timestamp")?,
    };
    let dropped_attributes_count =
        u32_field(t.raw_get("dropped_attributes_count")?, path, "dropped_attributes_count")?;
    Ok(LogRecord {
        message,
        severity,
        body_format,
        trace,
        event_name,
        observed_timestamp,
        dropped_attributes_count,
    })
}

// -- shared helpers -----------------------------------------------------------------------------
//
// Each takes the `path` of the table it's reading (`""` at the top level) and the `key` within
// it separately, and only joins them (`dotted`) on the error path: a constructed event's
// success path allocates nothing for messages it never raises (the `lua: Event.new ..` pin in
// `crates/logit-bench/tests/allocations.rs`), and W3-W5's metric/exemplar/span parsers can pass
// `metrics[i]`, `metrics[i].exemplars[j]`, `span`, ... as `path` unchanged.

fn runtime_error(message: String) -> mlua::Error {
    mlua::Error::RuntimeError(message)
}

fn required(path: &str, key: &str) -> mlua::Error {
    runtime_error(format!("Event.new: {} is required", dotted(path, key)))
}

fn metrics_not_a_sequence() -> mlua::Error {
    runtime_error("Event.new: metrics must be a contiguous array-like table".to_string())
}

/// `<path>.<key>`, or just `<key>` at the top level (`path == ""`).
fn dotted(path: &str, key: &str) -> String {
    match path.is_empty() {
        true => key.to_string(),
        false => format!("{path}.{key}"),
    }
}

/// How a table is named in a message about the table itself (as opposed to one of its fields):
/// its path, or "the table" for the top level.
fn describe(path: &str) -> &str {
    match path.is_empty() {
        true => "the table",
        false => path,
    }
}

/// Rejects any key of `t` that isn't in `allowed` -- `Event.new: <path>.<key> is not a field`
/// -- and any non-string key at all. Iterates with `Table::pairs`, which is raw in mlua 0.9
/// (`lua_next` under the hood, see the module doc), matching the `raw_get` reads that follow.
fn expect_keys(t: &Table, allowed: &[&str], path: &str) -> mlua::Result<()> {
    for pair in t.clone().pairs::<LuaValue, LuaValue>() {
        let (key, _) = pair?;
        let LuaValue::String(key) = key else {
            return Err(runtime_error(format!(
                "Event.new: {} has a non-string key ({})",
                describe(path),
                key.type_name()
            )));
        };
        let key = key.to_string_lossy();
        if !allowed.contains(&key.as_ref()) {
            return Err(runtime_error(format!("Event.new: {} is not a field", dotted(path, &key))));
        }
    }
    Ok(())
}

/// The `event.timestamp` rule and wording (`crate::proxy`'s `__newindex`): a string of decimal
/// digits, never a Lua number, since an IEEE-754 double can't hold a unix-nanos value exactly.
/// The caller handles `nil` (required or defaulted, per field).
fn nanos_string(value: LuaValue, path: &str, key: &str) -> mlua::Result<i64> {
    let LuaValue::String(s) = value else {
        return Err(runtime_error(format!(
            "Event.new: {} must be a string of decimal digits (a Lua number can't represent \
             full nanosecond precision), got {}",
            dotted(path, key),
            value.type_name()
        )));
    };
    s.to_str().ok().and_then(|s| s.parse().ok()).ok_or_else(|| {
        runtime_error(format!(
            "Event.new: {} must be a string of decimal digits",
            dotted(path, key)
        ))
    })
}

/// A `nil`-or-boolean field whose value is ignored (`has_log`/`has_metrics`/`has_span`).
fn boolean_field(t: &Table, path: &str, key: &str) -> mlua::Result<()> {
    match t.raw_get::<_, LuaValue>(key)? {
        LuaValue::Nil | LuaValue::Boolean(_) => Ok(()),
        other => Err(runtime_error(format!(
            "Event.new: {} must be a boolean, got {}",
            dotted(path, key),
            other.type_name()
        ))),
    }
}

/// A non-negative integer that fits a `u32`, defaulting to `0` for `nil`. A Lua integer or an
/// integral float (LuaJIT's dual-number mode usually canonicalizes the latter to the former
/// already, but a value that arrives as a `Number` is still accepted when it's whole).
fn u32_field(value: LuaValue, path: &str, key: &str) -> mlua::Result<u32> {
    let reject = |got: String| {
        runtime_error(format!(
            "Event.new: {} must be a non-negative integer, got {got}",
            dotted(path, key)
        ))
    };
    match value {
        LuaValue::Nil => Ok(0),
        LuaValue::Integer(n) => u32::try_from(n).map_err(|_| reject(n.to_string())),
        LuaValue::Number(n) if n.fract() == 0.0 && n >= 0.0 && n <= f64::from(u32::MAX) => {
            Ok(n as u32)
        }
        LuaValue::Number(n) => Err(reject(n.to_string())),
        other => Err(reject(other.type_name().to_string())),
    }
}

/// An optional enum-valued field named by its lowercase name: `nil` is `None`, a string is looked
/// up through `parse` (one of the `from_name`s `logit-core` provides), anything else is an error
/// listing `names` (the matching `NAMES` table) so the message can't drift from the enum.
fn enum_field<T>(
    value: LuaValue,
    path: &str,
    key: &str,
    names: &[&str],
    parse: fn(&str) -> Option<T>,
) -> mlua::Result<Option<T>> {
    match value {
        LuaValue::Nil => Ok(None),
        LuaValue::String(s) => {
            let s = s.to_string_lossy();
            parse(&s).map(Some).ok_or_else(|| {
                runtime_error(format!(
                    "Event.new: {} must be one of {} (or nil), got \"{s}\"",
                    dotted(path, key),
                    names.join(", ")
                ))
            })
        }
        other => Err(runtime_error(format!(
            "Event.new: {} must be a string or nil, got {}",
            dotted(path, key),
            other.type_name()
        ))),
    }
}

/// The log proxy's trace-context rule, applied to three fields read at once: `trace_id` is the
/// gate (absent means no [`TraceRef`] at all), `span_id`/`trace_flags` without it are the same
/// error `event.log.span_id = ..` raises before a `trace_id` is set, and each id goes through the
/// parser `logit_core::trace` already has (exact length, hex, not all-zero). `path` is the
/// record's path (`log`, or W3's `metrics[i].exemplars[j]`), not a field's.
fn trace_ref_from_fields(
    trace_id: LuaValue,
    span_id: LuaValue,
    trace_flags: LuaValue,
    path: &str,
) -> mlua::Result<Option<TraceRef>> {
    let trace_id = match trace_id {
        LuaValue::Nil => {
            for (key, value) in [("span_id", &span_id), ("trace_flags", &trace_flags)] {
                if !matches!(value, LuaValue::Nil) {
                    return Err(runtime_error(format!(
                        "Event.new: {} can't be set without a trace_id",
                        dotted(path, key)
                    )));
                }
            }
            return Ok(None);
        }
        LuaValue::String(s) => s.to_str().ok().and_then(parse_trace_id).ok_or_else(|| {
            runtime_error(format!(
                "Event.new: {} must be a 32-character hex string (or nil), and not all-zero",
                dotted(path, "trace_id")
            ))
        })?,
        other => {
            return Err(runtime_error(format!(
                "Event.new: {} must be a hex string or nil, got {}",
                dotted(path, "trace_id"),
                other.type_name()
            )))
        }
    };
    let span_id = match span_id {
        LuaValue::Nil => None,
        LuaValue::String(s) => Some(s.to_str().ok().and_then(parse_span_id).ok_or_else(|| {
            runtime_error(format!(
                "Event.new: {} must be a 16-character hex string (or nil), and not all-zero",
                dotted(path, "span_id")
            ))
        })?),
        other => {
            return Err(runtime_error(format!(
                "Event.new: {} must be a hex string or nil, got {}",
                dotted(path, "span_id"),
                other.type_name()
            )))
        }
    };
    let flags = match trace_flags {
        LuaValue::Nil => 0,
        LuaValue::Integer(n) => u8::try_from(n).map_err(|_| {
            runtime_error(format!(
                "Event.new: {} must be an integer 0-255, got {n}",
                dotted(path, "trace_flags")
            ))
        })?,
        other => {
            return Err(runtime_error(format!(
                "Event.new: {} must be an integer 0-255, got {}",
                dotted(path, "trace_flags"),
                other.type_name()
            )))
        }
    };
    Ok(Some(TraceRef { trace_id, span_id, flags }))
}

/// A field holding an arbitrary [`Value`] (`log.message`; W5's span and span-event `name`s),
/// converted the way a fresh attribute write is (`lua_to_value`) -- so a Lua string becomes
/// `Str`, an integer `I64`, an empty table an empty `Map`: the flattening ADR
/// `lua-event-constructor` records. The one thing checked up front is the Lua *type*, so the
/// error names this field rather than `lua_to_value`'s generic "attribute value" wording.
fn value_field(value: LuaValue, path: &str, key: &str) -> mlua::Result<Value> {
    match value {
        LuaValue::Nil
        | LuaValue::Boolean(_)
        | LuaValue::Integer(_)
        | LuaValue::Number(_)
        | LuaValue::String(_)
        | LuaValue::Table(_) => lua_to_value(value),
        other => Err(runtime_error(format!(
            "Event.new: {} can't be a Lua {}",
            dotted(path, key),
            other.type_name()
        ))),
    }
}

/// An [`AttrMap`] from a table of string keys. Stricter than `event.attributes.x = {..}`'s
/// `lua_table_to_attrmap` alone, which coerces a numeric key to its decimal string the way
/// mlua's `String` conversion does: a constructor's input is `to_table()`'s output, where every
/// attribute key is a string, so a non-string key here is a mistake worth naming. The key check
/// is its own raw pass; the values then convert through the shared helper.
fn attributes_from_table(t: Table, path: &str) -> mlua::Result<AttrMap> {
    for pair in t.clone().pairs::<LuaValue, LuaValue>() {
        let (key, _) = pair?;
        if !matches!(key, LuaValue::String(_)) {
            return Err(runtime_error(format!(
                "Event.new: {path} has a non-string key ({})",
                key.type_name()
            )));
        }
    }
    lua_table_to_attrmap(t)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ProcessOutcome, ScriptWorker};
    use logit_core::{AttrMap, BodyFormat, Severity, Value};

    fn worker(source: &str) -> ScriptWorker {
        ScriptWorker::new(source).expect("script should load")
    }

    /// As `lib.rs`'s own helper: a worker whose component declares `targets:`, in slot order.
    fn routing_worker(source: &str, targets: &[&str]) -> ScriptWorker {
        let targets: Vec<String> = targets.iter().map(|t| (*t).to_string()).collect();
        worker(source).with_targets(&targets)
    }

    fn emitted(outcome: ProcessOutcome) -> Event {
        match outcome {
            ProcessOutcome::Emit(e, _) => *e,
            _ => panic!("expected Emit"),
        }
    }

    fn emitted_with_mark(outcome: ProcessOutcome) -> (Event, Option<u16>) {
        match outcome {
            ProcessOutcome::Emit(e, target) => (*e, target),
            _ => panic!("expected Emit"),
        }
    }

    /// As `lib.rs`'s own `process_err` helper -- `ProcessOutcome` isn't `Debug`, so
    /// `Result::unwrap_err` doesn't work directly on `ScriptWorker::process`'s return value.
    fn process_err(w: &ScriptWorker, event: Event) -> String {
        match w.process(event) {
            Err(err) => err.to_string(),
            Ok(_) => panic!("expected process() to reject this script"),
        }
    }

    /// The error `process()` raises for a script whose body is `return Event.new<args>`.
    fn new_err(args: &str) -> String {
        let w = worker(&format!("function process(event) return Event.new{args} end"));
        process_err(&w, Event::empty(0, AttrMap::new()))
    }

    // -- fixtures -----------------------------------------------------------------------------

    /// `proxy.rs`'s `log_record_with_everything`: every `LogRecord` field non-default.
    fn log_record_with_everything() -> LogRecord {
        LogRecord {
            message: Value::str("GET /widgets"),
            severity: Some(Severity::Warn),
            body_format: BodyFormat::Json,
            trace: Some(TraceRef { trace_id: [0x11; 16], span_id: Some([0x22; 8]), flags: 1 }),
            event_name: Some(intern("request.completed")),
            observed_timestamp: 9_007_199_254_740_993, // one past 2^53
            dropped_attributes_count: 3,
        }
    }

    /// Attributes whose every `Value` variant survives `to_table()` and back unchanged: `Str`,
    /// `I64`, a *fractional* `F64` (an integral one is canonicalized to a Lua integer by LuaJIT
    /// and comes back `I64`), `Bool`. The variants that flatten (`U64`, `Timestamp`, UTF-8
    /// `Bytes`) are the ADR's recorded residual -- see the test named for it below.
    fn round_trippable_attributes() -> AttrMap {
        let mut attrs = AttrMap::new();
        attrs.insert("host", "web-01");
        attrs.insert("retries", 3i64);
        attrs.insert("ratio", 0.25f64);
        attrs.insert("ok", true);
        attrs
    }

    fn log_event() -> Event {
        Event::log(
            1_700_000_000_000_000_000,
            round_trippable_attributes(),
            log_record_with_everything(),
        )
    }

    const REBUILD: &str = "function process(event) return Event.new(event:to_table()) end";

    // -- construction -------------------------------------------------------------------------

    #[test]
    fn new_of_to_table_round_trips_a_log_event_whole() {
        let w = worker(REBUILD);
        let out = emitted(w.process(log_event()).unwrap());
        assert_eq!(out, log_event());
    }

    /// The flattening residual ADR `lua-event-constructor` records: a constructed value has no
    /// existing `Value` to compare against, so the no-op-assignment identity rule
    /// (`docs/adr/lua-value-identity-preservation.md`) can't apply -- a `U64` attribute comes
    /// back as the `I64` any fresh Lua integer becomes.
    #[test]
    fn new_of_to_table_flattens_a_u64_attribute_to_i64() {
        let w = worker(REBUILD);
        let mut event = log_event();
        event.attributes.insert("count", Value::U64(5));
        let out = emitted(w.process(event).unwrap());
        assert_eq!(out.attributes.get("count"), Some(&Value::I64(5)));
    }

    #[test]
    fn timestamp_alone_yields_an_empty_event() {
        let w = worker(r#"function process(event) return Event.new{timestamp = "1"} end"#);
        let out = emitted(w.process(Event::empty(0, AttrMap::new())).unwrap());
        assert_eq!(out, Event::empty(1, AttrMap::new()));
    }

    #[test]
    fn a_minimal_log_gets_cores_documented_defaults() {
        let w = worker(
            r#"function process(event) return Event.new{timestamp = "1", log = {message = "hi"}} end"#,
        );
        let out = emitted(w.process(Event::empty(0, AttrMap::new())).unwrap());
        assert_eq!(
            out,
            Event::log(
                1,
                AttrMap::new(),
                LogRecord {
                    message: Value::str("hi"),
                    severity: None,
                    body_format: BodyFormat::Raw,
                    trace: None,
                    event_name: None,
                    observed_timestamp: 0,
                    dropped_attributes_count: 0,
                }
            )
        );
    }

    #[test]
    fn has_flags_are_accepted_and_ignored() {
        // `has_log = true` with no `log` key: the payload keys are the truth.
        let w = worker(
            r#"function process(event) return Event.new{timestamp = "1", has_log = true, has_metrics = false, has_span = false, metrics = {}} end"#,
        );
        let out = emitted(w.process(Event::empty(0, AttrMap::new())).unwrap());
        assert_eq!(out, Event::empty(1, AttrMap::new()));
    }

    #[test]
    fn a_constructed_event_can_be_returned_alongside_the_incoming_one() {
        let w = worker(
            r#"
            function process(event)
                return {event, Event.new{timestamp = "2", attributes = {minted = true}}}
            end
            "#,
        );
        let outcome = w.process(Event::empty(1, AttrMap::new())).unwrap();
        let ProcessOutcome::EmitMany(events) = outcome else { panic!("expected EmitMany") };
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].0, Event::empty(1, AttrMap::new()));
        assert_eq!(events[1].0.timestamp, 2);
        assert_eq!(events[1].0.attributes.get("minted"), Some(&Value::Bool(true)));
    }

    #[test]
    fn a_constructed_event_is_an_ordinary_handle() {
        // Mutation, `clone`, and sub-proxy access all work on the returned handle.
        let w = worker(
            r#"
            function process(event)
                local e = Event.new{timestamp = "1", log = {message = "hi"}}
                e.attributes.env = "prod"
                e.log.trace_id = "4bf92f3577b34da6a3ce929d0e0e4736"
                local c = e:clone()
                c.timestamp = "2"
                return {e, c}
            end
            "#,
        );
        let ProcessOutcome::EmitMany(events) = w.process(Event::empty(0, AttrMap::new())).unwrap()
        else {
            panic!("expected EmitMany")
        };
        assert_eq!(events[0].0.timestamp, 1);
        assert_eq!(events[1].0.timestamp, 2);
        for (event, _) in &events {
            assert_eq!(event.attributes.get("env"), Some(&Value::str("prod")));
            assert!(event.log.as_ref().unwrap().trace.is_some());
        }
    }

    // -- targets ------------------------------------------------------------------------------

    #[test]
    fn to_on_a_constructed_event_resolves_against_the_workers_targets_in_process() {
        let w = routing_worker(
            r#"function process(event) return Event.new{timestamp = "1"}:to("b") end"#,
            &["a", "b"],
        );
        let (event, mark) = emitted_with_mark(w.process(Event::empty(0, AttrMap::new())).unwrap());
        assert_eq!(event.timestamp, 1);
        assert_eq!(mark, Some(1));
    }

    #[test]
    fn to_on_a_constructed_event_resolves_against_the_workers_targets_in_flush() {
        let w = routing_worker(
            r#"
            function process(event) return event end
            function flush(now) return {Event.new{timestamp = now}:to("a")} end
            "#,
            &["a", "b"],
        );
        let flushed = w.flush(1_700_000_000_000_000_000).unwrap();
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].0.timestamp, 1_700_000_000_000_000_000);
        assert_eq!(flushed[0].1, Some(0));
    }

    /// The documented top-level caveat: `Event.new` during the script's own top-level run sees
    /// the empty target list `with_targets` has not yet replaced, so the event it built can't be
    /// routed later even on a router -- the same "declares no targets" wording an unrouted
    /// component gives.
    #[test]
    fn a_top_level_constructed_event_sees_the_empty_target_list() {
        let w = routing_worker(
            r#"
            local e = Event.new{timestamp = "1"}
            function process(event) return e:to("a") end
            "#,
            &["a", "b"],
        );
        let err = process_err(&w, Event::empty(0, AttrMap::new()));
        assert!(err.contains("this component declares no targets"), "got: {err}");
    }

    // -- flush(now) ---------------------------------------------------------------------------

    #[test]
    fn flush_receives_now_as_a_decimal_string() {
        let w = worker(
            r#"
            function process(event) return event end
            function flush(now)
                return {Event.new{timestamp = now, attributes = {tick = now, kind = type(now)}}}
            end
            "#,
        );
        let flushed = w.flush(123).unwrap();
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].0.timestamp, 123);
        assert_eq!(flushed[0].0.attributes.get("tick"), Some(&Value::str("123")));
        assert_eq!(flushed[0].0.attributes.get("kind"), Some(&Value::str("string")));
    }

    #[test]
    fn a_flush_declaring_no_parameter_still_works() {
        let w = worker(
            r#"
            function process(event) return event end
            function flush() return {Event.new{timestamp = "7"}} end
            "#,
        );
        let flushed = w.flush(0).unwrap();
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].0, Event::empty(7, AttrMap::new()));
    }

    // -- errors -------------------------------------------------------------------------------

    #[test]
    fn a_non_table_argument_is_a_clear_error() {
        let err = new_err(r#"("nope")"#);
        assert!(err.contains("Event.new(t) takes a table, got string"), "got: {err}");
    }

    #[test]
    fn a_numeric_timestamp_is_rejected_like_the_proxy_write() {
        let err = new_err("{timestamp = 1}");
        assert!(
            err.contains(
                "Event.new: timestamp must be a string of decimal digits (a Lua number can't \
                 represent full nanosecond precision), got integer"
            ),
            "got: {err}"
        );
    }

    #[test]
    fn a_non_digit_timestamp_is_rejected() {
        let err = new_err(r#"{timestamp = "abc"}"#);
        assert!(
            err.contains("Event.new: timestamp must be a string of decimal digits"),
            "got: {err}"
        );
    }

    #[test]
    fn a_missing_timestamp_is_required() {
        let err = new_err("{}");
        assert!(err.contains("Event.new: timestamp is required"), "got: {err}");
    }

    #[test]
    fn an_unknown_top_level_key_is_not_a_field() {
        let err = new_err(r#"{timestamp = "1", foo = 1}"#);
        assert!(err.contains("Event.new: foo is not a field"), "got: {err}");
    }

    #[test]
    fn an_unknown_log_key_is_not_a_field_with_its_dotted_path() {
        let err = new_err(r#"{timestamp = "1", log = {message = "hi", severty = "warn"}}"#);
        assert!(err.contains("Event.new: log.severty is not a field"), "got: {err}");
    }

    #[test]
    fn a_log_without_a_message_is_rejected() {
        let err = new_err(r#"{timestamp = "1", log = {severity = "warn"}}"#);
        assert!(err.contains("Event.new: log.message is required"), "got: {err}");
    }

    #[test]
    fn an_unknown_severity_lists_the_names() {
        let err = new_err(r#"{timestamp = "1", log = {message = "hi", severity = "warning"}}"#);
        assert!(
            err.contains(
                "Event.new: log.severity must be one of trace, debug, info, warn, error, fatal \
                 (or nil), got \"warning\""
            ),
            "got: {err}"
        );
    }

    #[test]
    fn an_unknown_body_format_lists_the_names() {
        let err = new_err(r#"{timestamp = "1", log = {message = "hi", body_format = "text"}}"#);
        assert!(
            err.contains(
                "Event.new: log.body_format must be one of raw, json, structured (or nil), got \
                 \"text\""
            ),
            "got: {err}"
        );
    }

    #[test]
    fn a_span_id_without_a_trace_id_is_rejected() {
        let err =
            new_err(r#"{timestamp = "1", log = {message = "hi", span_id = "2222222222222222"}}"#);
        assert!(
            err.contains("Event.new: log.span_id can't be set without a trace_id"),
            "got: {err}"
        );
    }

    #[test]
    fn an_invalid_trace_id_is_rejected() {
        let err = new_err(r#"{timestamp = "1", log = {message = "hi", trace_id = "zz"}}"#);
        assert!(
            err.contains(
                "Event.new: log.trace_id must be a 32-character hex string (or nil), and not \
                 all-zero"
            ),
            "got: {err}"
        );
    }

    #[test]
    fn out_of_range_trace_flags_are_rejected() {
        let err = new_err(
            r#"{timestamp = "1", log = {message = "hi", trace_id = "11111111111111111111111111111111", trace_flags = 256}}"#,
        );
        assert!(
            err.contains("Event.new: log.trace_flags must be an integer 0-255, got 256"),
            "got: {err}"
        );
    }

    #[test]
    fn a_negative_dropped_attributes_count_is_rejected() {
        let err =
            new_err(r#"{timestamp = "1", log = {message = "hi", dropped_attributes_count = -1}}"#);
        assert!(
            err.contains(
                "Event.new: log.dropped_attributes_count must be a non-negative integer, got -1"
            ),
            "got: {err}"
        );
    }

    #[test]
    fn a_numeric_attribute_key_is_rejected() {
        let err = new_err(r#"{timestamp = "1", attributes = {[1] = "a"}}"#);
        assert!(err.contains("Event.new: attributes has a non-string key (integer)"), "got: {err}");
    }

    #[test]
    fn a_non_boolean_has_flag_is_rejected() {
        let err = new_err(r#"{timestamp = "1", has_log = "yes"}"#);
        assert!(err.contains("Event.new: has_log must be a boolean, got string"), "got: {err}");
    }

    #[test]
    fn a_non_empty_metrics_list_is_not_constructible_yet() {
        let err = new_err(r#"{timestamp = "1", metrics = {{}}}"#);
        assert!(err.contains("Event.new: metrics[1] is not constructible yet"), "got: {err}");
    }

    #[test]
    fn a_span_is_not_constructible_yet() {
        let err = new_err(r#"{timestamp = "1", span = {}}"#);
        assert!(err.contains("Event.new: span is not constructible yet"), "got: {err}");
    }

    #[test]
    fn a_non_sequence_metrics_table_is_rejected() {
        let err = new_err(r#"{timestamp = "1", metrics = {[2] = {}}}"#);
        assert!(
            err.contains("Event.new: metrics must be a contiguous array-like table"),
            "got: {err}"
        );
    }
}
