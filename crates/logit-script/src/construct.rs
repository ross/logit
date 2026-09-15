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
//!   and `dropped_attributes_count` of `0`, empty `attributes`; a metric's `start_timestamp`/
//!   `flags` of `0`, `MetricKind::counter`'s temporality and monotonicity for a bare `sum`,
//!   `Samples::new`'s `sample_rate` of `1.0`); `timestamp`, `log.message`, a metric's `name`/
//!   `kind` and its kind's own payload are required, exactly as the ADR lists.
//!
//! Every error is an `mlua::Error::RuntimeError` prefixed `Event.new: <path> ...` down to the
//! field; the one exception is a malformed value *inside* a nested attribute table, which reports
//! the shared attribute-conversion error (`lua_to_value`'s, unprefixed), since nested tables
//! convert through the same helper the proxy write path uses. The shared helpers at the bottom
//! take the path as an argument so the metric and span parsers of
//! `docs/plans/lua-event-constructor.md` reuse them unchanged. `metrics` builds the four raw
//! kinds (`sum`, `gauge`, `samples`, `set_members`) with their exemplars; the three
//! pre-aggregated kinds are recognized but not yet constructible (W4), the two sketches and
//! `gauge_delta` never will be, and `span` is not yet constructible (W5) -- each says so.

use crate::proxy::{EventProxy, TargetTable};
use crate::value::{lua_to_value, validated_sequence_len};
use bytes::Bytes;
use logit_core::interner::{intern, Symbol};
use logit_core::trace::{parse_span_id, parse_trace_id, TraceRef};
use logit_core::{
    AttrMap, BodyFormat, Event, Exemplar, LogRecord, MetricKind, MetricList, MetricRecord, Samples,
    Severity, Sum, Temporality, Value,
};
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

/// The keys `metric_to_table` emits for every kind. A kind's own payload keys
/// ([`RawKind::keys`]) are added to these for the key check, once the kind is known.
const METRIC_KEYS: &[&str] = &[
    "name",
    "kind",
    "unit",
    "description",
    "start_timestamp",
    "flags",
    "is_no_recorded_value",
    "exemplars",
];

/// The keys `exemplar_to_table` emits.
const EXEMPLAR_KEYS: &[&str] =
    &["timestamp", "value", "trace_id", "span_id", "trace_flags", "attributes"];

/// The kinds the unknown-`kind` error names: the ones `Event.new` builds, plus the three W4 of
/// `docs/plans/lua-event-constructor.md` adds -- deliberately *not* the sketches or
/// `gauge_delta`, which are never constructible and get their own message each.
const CONSTRUCTIBLE_KINDS: &str =
    "sum, gauge, samples, set_members, histogram, exponential_histogram, summary";

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
    let attributes = attributes_field(t.raw_get("attributes")?, "", "attributes")?;
    for key in ["has_log", "has_metrics", "has_span"] {
        // Accepted because `to_table()` emits them; ignored because the payload keys below are
        // the truth (ADR `lua-event-constructor`). Still type-checked, so a script that wrote
        // `has_log = "yes"` hears about it.
        boolean_field(&t, "", key)?;
    }
    let metrics = match t.raw_get::<_, LuaValue>("metrics")? {
        LuaValue::Nil => MetricList::new(),
        LuaValue::Table(metrics) => match validated_sequence_len(&metrics)? {
            // `to_table()` always emits `metrics`, empty when the event carries none, so the
            // empty sequence is accepted (and costs nothing: `with_capacity(0)` stays inline,
            // as does the one-record case `MetricList`'s inline slot is sized for).
            Some(len) => {
                let mut list = MetricList::with_capacity(len);
                for i in 1..=len {
                    match metrics.raw_get::<_, LuaValue>(i)? {
                        // One small `String` per metric for its path: every field error
                        // beneath needs `metrics[i]` as a prefix, and building it once here is
                        // cheaper than threading the index through every helper. Accounted
                        // for in the `lua: Event.new gauge event ..` pin.
                        LuaValue::Table(m) => {
                            list.push(metric_from_table(m, &format!("metrics[{i}]"))?)
                        }
                        other => {
                            return Err(runtime_error(format!(
                                "Event.new: metrics[{i}] must be a table, got {}",
                                other.type_name()
                            )))
                        }
                    }
                }
                list
            }
            None => return Err(not_a_sequence("", "metrics")),
        },
        _ => return Err(not_a_sequence("", "metrics")),
    };
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
    event.metrics = metrics;
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
    let event_name = symbol_field(t.raw_get("event_name")?, path, "event_name")?;
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

/// Builds a [`MetricRecord`] from a table in `metric_to_table`'s shape. `path` is the table's
/// own path (`metrics[i]`).
fn metric_from_table(t: Table, path: &str) -> mlua::Result<MetricRecord> {
    // `kind` first: it decides which payload keys are fields at all, so an unknown kind is
    // reported as such rather than as its payload keys "not being fields", and a `sum`'s
    // `monotonic` is a field while a `gauge`'s is not. `to_string_lossy` borrows a valid UTF-8
    // Lua string, so the name costs nothing on the success path.
    let kind_name = match t.raw_get::<_, LuaValue>("kind")? {
        LuaValue::Nil => return Err(required(path, "kind")),
        LuaValue::String(s) => s,
        other => {
            return Err(runtime_error(format!(
                "Event.new: {} must be a string, got {}",
                dotted(path, "kind"),
                other.type_name()
            )))
        }
    };
    let kind = RawKind::parse(&kind_name.to_string_lossy(), path)?;
    expect_keys_of(&t, &[METRIC_KEYS, kind.keys()], path)?;
    let name = match t.raw_get::<_, LuaValue>("name")? {
        LuaValue::Nil => return Err(required(path, "name")),
        LuaValue::String(s) => intern(s.to_str()?),
        other => {
            return Err(runtime_error(format!(
                "Event.new: {} must be a string, got {}",
                dotted(path, "name"),
                other.type_name()
            )))
        }
    };
    let unit = symbol_field(t.raw_get("unit")?, path, "unit")?;
    let description = symbol_field(t.raw_get("description")?, path, "description")?;
    let start_timestamp = match t.raw_get::<_, LuaValue>("start_timestamp")? {
        LuaValue::Nil => 0,
        value => nanos_string(value, path, "start_timestamp")?,
    };
    let mut flags = u32_field(t.raw_get("flags")?, path, "flags")?;
    // `is_no_recorded_value` is sugar for the flag bit (ADR `lua-event-constructor`): `true`
    // ORs it on, `false` is a no-op rather than a clear, so `to_table()`'s `{flags = 1,
    // is_no_recorded_value = true}` rebuilds as exactly `flags == 1` and a script that sets
    // `flags` by hand isn't second-guessed.
    match t.raw_get::<_, LuaValue>("is_no_recorded_value")? {
        LuaValue::Nil | LuaValue::Boolean(false) => {}
        LuaValue::Boolean(true) => flags |= MetricRecord::FLAG_NO_RECORDED_VALUE,
        other => {
            return Err(runtime_error(format!(
                "Event.new: {} must be a boolean (or nil), got {}",
                dotted(path, "is_no_recorded_value"),
                other.type_name()
            )))
        }
    }
    let exemplars = match t.raw_get::<_, LuaValue>("exemplars")? {
        LuaValue::Nil => Vec::new(),
        LuaValue::Table(list) => match validated_sequence_len(&list)? {
            // `with_capacity(0)` is `Vec::new()`: the empty list `to_table()` always emits
            // costs nothing to rebuild.
            Some(len) => {
                let mut exemplars = Vec::with_capacity(len);
                for j in 1..=len {
                    match list.raw_get::<_, LuaValue>(j)? {
                        LuaValue::Table(e) => exemplars
                            .push(exemplar_from_table(e, &format!("{path}.exemplars[{j}]"))?),
                        other => {
                            return Err(runtime_error(format!(
                                "Event.new: {path}.exemplars[{j}] must be a table, got {}",
                                other.type_name()
                            )))
                        }
                    }
                }
                exemplars
            }
            None => return Err(not_a_sequence(path, "exemplars")),
        },
        _ => return Err(not_a_sequence(path, "exemplars")),
    };
    let kind = match kind {
        RawKind::Sum => {
            // The defaults are `MetricKind::counter`'s -- delta, monotonic -- so
            // `{kind = "sum", value = 1}` is exactly the counter `kv_metrics`/`statsd_in` emit.
            let value = finite_field(t.raw_get("value")?, path, "value")?;
            let temporality = enum_field(
                t.raw_get("temporality")?,
                path,
                "temporality",
                &Temporality::NAMES,
                Temporality::from_name,
            )?
            .unwrap_or(Temporality::Delta);
            let monotonic = match t.raw_get::<_, LuaValue>("monotonic")? {
                LuaValue::Nil => true,
                LuaValue::Boolean(b) => b,
                other => {
                    return Err(runtime_error(format!(
                        "Event.new: {} must be a boolean (or nil), got {}",
                        dotted(path, "monotonic"),
                        other.type_name()
                    )))
                }
            };
            MetricKind::Sum(Sum { value, temporality, monotonic })
        }
        RawKind::Gauge => MetricKind::Gauge(finite_field(t.raw_get("value")?, path, "value")?),
        RawKind::Samples => {
            // `Samples::new`'s `sample_rate` of `1.0` is the default core documents; `values`
            // are pushed straight onto the record's own `SmallVec`, so up to `SAMPLES_INLINE`
            // of them allocate nothing beyond the record.
            let mut samples = Samples::new(std::iter::empty());
            match t.raw_get::<_, LuaValue>("values")? {
                LuaValue::Nil => {}
                LuaValue::Table(values) => match validated_sequence_len(&values)? {
                    Some(len) => {
                        samples.values.reserve(len);
                        for k in 1..=len {
                            samples.values.push(finite(values.raw_get(k)?, || {
                                format!("{}[{k}]", dotted(path, "values"))
                            })?);
                        }
                    }
                    None => return Err(not_a_sequence(path, "values")),
                },
                _ => return Err(not_a_sequence(path, "values")),
            }
            match t.raw_get::<_, LuaValue>("sample_rate")? {
                LuaValue::Nil => {}
                value => samples.sample_rate = finite(value, || dotted(path, "sample_rate"))?,
            }
            MetricKind::Samples(samples)
        }
        RawKind::SetMembers => {
            let members = match t.raw_get::<_, LuaValue>("members")? {
                LuaValue::Nil => Vec::new(),
                LuaValue::Table(list) => match validated_sequence_len(&list)? {
                    Some(len) => {
                        let mut members = Vec::with_capacity(len);
                        for k in 1..=len {
                            match list.raw_get::<_, LuaValue>(k)? {
                                // A member is opaque bytes on the record (`statsd_in`'s `s`
                                // payload), so a Lua string copies over as-is, UTF-8 or not.
                                LuaValue::String(s) => {
                                    members.push(Bytes::copy_from_slice(s.as_bytes()))
                                }
                                other => {
                                    return Err(runtime_error(format!(
                                        "Event.new: {}[{k}] must be a string, got {}",
                                        dotted(path, "members"),
                                        other.type_name()
                                    )))
                                }
                            }
                        }
                        members
                    }
                    None => return Err(not_a_sequence(path, "members")),
                },
                _ => return Err(not_a_sequence(path, "members")),
            };
            MetricKind::SetMembers(members)
        }
    };
    Ok(MetricRecord { name, unit, description, start_timestamp, exemplars, flags, kind })
}

/// The metric kinds `Event.new` builds today -- the raw, pre-aggregation ones -- and the payload
/// keys each adds to [`METRIC_KEYS`]. [`RawKind::parse`] is where every kind that *isn't* one
/// of these gets its own message.
#[derive(Clone, Copy)]
enum RawKind {
    Sum,
    Gauge,
    Samples,
    SetMembers,
}

impl RawKind {
    fn parse(name: &str, path: &str) -> mlua::Result<Self> {
        Ok(match name {
            "sum" => RawKind::Sum,
            "gauge" => RawKind::Gauge,
            "samples" => RawKind::Samples,
            "set_members" => RawKind::SetMembers,
            // W4 of `docs/plans/lua-event-constructor.md` replaces this arm.
            "histogram" | "exponential_histogram" | "summary" => {
                return Err(runtime_error(format!(
                    "Event.new: {} \"{name}\" is not constructible yet",
                    dotted(path, "kind")
                )))
            }
            // `to_table()` is deliberately lossy for the two sketches (a `count`, an `estimate`
            // -- never the DDSketch or HyperLogLog state), so there is no shape to invert; the
            // raw kind `aggregate` folds into each is the way to get one.
            "distribution" => {
                return Err(not_constructible(
                    path,
                    name,
                    "a merged sketch; build a \"samples\" metric and let aggregate summarize it",
                ))
            }
            "set" => {
                return Err(not_constructible(
                    path,
                    name,
                    "a merged sketch; build a \"set_members\" metric and let aggregate \
                     summarize it",
                ))
            }
            "gauge_delta" => {
                return Err(not_constructible(
                    path,
                    name,
                    "aggregate's private intermediate, never valid at a sink",
                ))
            }
            _ => {
                return Err(runtime_error(format!(
                    "Event.new: {} must be one of {CONSTRUCTIBLE_KINDS}, got \"{name}\"",
                    dotted(path, "kind")
                )))
            }
        })
    }

    fn keys(self) -> &'static [&'static str] {
        match self {
            RawKind::Sum => &["value", "temporality", "monotonic"],
            RawKind::Gauge => &["value"],
            RawKind::Samples => &["values", "sample_rate"],
            RawKind::SetMembers => &["members"],
        }
    }
}

fn not_constructible(path: &str, name: &str, why: &str) -> mlua::Error {
    runtime_error(format!(
        "Event.new: {} \"{name}\" is not constructible from Lua -- {why}",
        dotted(path, "kind")
    ))
}

/// Builds an [`Exemplar`] from a table in `exemplar_to_table`'s shape. `path` is the table's
/// own path (`metrics[i].exemplars[j]`).
fn exemplar_from_table(t: Table, path: &str) -> mlua::Result<Exemplar> {
    expect_keys(&t, EXEMPLAR_KEYS, path)?;
    let timestamp = match t.raw_get::<_, LuaValue>("timestamp")? {
        LuaValue::Nil => return Err(required(path, "timestamp")),
        value => nanos_string(value, path, "timestamp")?,
    };
    let value = finite_field(t.raw_get("value")?, path, "value")?;
    let trace = trace_ref_from_fields(
        t.raw_get("trace_id")?,
        t.raw_get("span_id")?,
        t.raw_get("trace_flags")?,
        path,
    )?;
    let filtered_attributes = attributes_field(t.raw_get("attributes")?, path, "attributes")?;
    Ok(Exemplar { timestamp, value, trace, filtered_attributes })
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

/// A field that must be a sequence (`validated_sequence_len`'s contiguous-from-one rule) but is
/// either a non-table or a table with other keys: `metrics`, a metric's `exemplars`/`values`/
/// `members`.
fn not_a_sequence(path: &str, key: &str) -> mlua::Error {
    runtime_error(format!("Event.new: {} must be a contiguous array-like table", dotted(path, key)))
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
    expect_keys_of(t, &[allowed], path)
}

/// [`expect_keys`] against the union of several key lists, without building the union: a
/// metric's common keys plus the payload keys of its own kind.
fn expect_keys_of(t: &Table, allowed: &[&[&str]], path: &str) -> mlua::Result<()> {
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
        if !allowed.iter().any(|set| set.contains(&key.as_ref())) {
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

/// An optional interned-string field (`log.event_name`, a metric's `unit`/`description`):
/// `nil` is `None`, a string is interned -- the same cardinality caution the proxy's writes to
/// these fields carry.
fn symbol_field(value: LuaValue, path: &str, key: &str) -> mlua::Result<Option<Symbol>> {
    match value {
        LuaValue::Nil => Ok(None),
        LuaValue::String(s) => Ok(Some(intern(s.to_str()?))),
        other => Err(runtime_error(format!(
            "Event.new: {} must be a string or nil, got {}",
            dotted(path, key),
            other.type_name()
        ))),
    }
}

/// A required finite number (a `sum`/`gauge`/exemplar `value`): `nil` is "is required", and
/// anything else goes through [`finite`].
fn finite_field(value: LuaValue, path: &str, key: &str) -> mlua::Result<f64> {
    match value {
        LuaValue::Nil => Err(required(path, key)),
        value => finite(value, || dotted(path, key)),
    }
}

/// The rule `crate::proxy`'s `require_finite_number` applies to a `value` write, with a lazily
/// built field name so a `values[k]` element costs no `format!` on the success path: a Lua
/// integer or number that is finite. NaN and the infinities are rejected as loudly as a string
/// is, rather than stored into a metric a sink or `aggregate` would then have to defend against.
fn finite(value: LuaValue, field: impl FnOnce() -> String) -> mlua::Result<f64> {
    let v = match value {
        LuaValue::Integer(i) => i as f64,
        LuaValue::Number(n) => n,
        other => {
            return Err(runtime_error(format!(
                "Event.new: {} must be a number, got {}",
                field(),
                other.type_name()
            )))
        }
    };
    if !v.is_finite() {
        return Err(runtime_error(format!(
            "Event.new: {} must be a finite number, got {v}",
            field()
        )));
    }
    Ok(v)
}

/// An optional attributes table (the top level's `attributes`, an exemplar's): `nil` is empty,
/// a table goes through [`attributes_from_table`].
fn attributes_field(value: LuaValue, path: &str, key: &str) -> mlua::Result<AttrMap> {
    match value {
        LuaValue::Nil => Ok(AttrMap::new()),
        LuaValue::Table(attrs) => attributes_from_table(attrs, path, key),
        other => Err(runtime_error(format!(
            "Event.new: {} must be a table (or nil), got {}",
            dotted(path, key),
            other.type_name()
        ))),
    }
}

/// An [`AttrMap`] from a table of string keys. Stricter than `event.attributes.x = {..}`'s
/// `lua_table_to_attrmap`, which coerces a numeric key to its decimal string the way mlua's
/// `String` conversion does and surfaces a non-UTF-8 key as mlua's own conversion error: a
/// constructor's input is `to_table()`'s output, where every attribute key is a UTF-8 string,
/// so either is a mistake worth naming with the table's path. One raw pass: each key is checked,
/// then its value goes through [`value_field`] so a bad value is `Event.new: <path>.<key>.<k>
/// ...` too. A nested table still converts through the shared `lua_to_value`, so a malformed
/// value *inside* one reports that helper's unprefixed attribute-conversion error.
fn attributes_from_table(t: Table, path: &str, key: &str) -> mlua::Result<AttrMap> {
    let table_path = dotted(path, key);
    let mut map = AttrMap::new();
    for pair in t.pairs::<LuaValue, LuaValue>() {
        let (k, value) = pair?;
        let LuaValue::String(k) = k else {
            return Err(runtime_error(format!(
                "Event.new: {table_path} has a non-string key ({})",
                k.type_name()
            )));
        };
        let Ok(k) = k.to_str() else {
            return Err(runtime_error(format!("Event.new: {table_path} has a non-UTF-8 key")));
        };
        map.insert(k, value_field(value, &table_path, k)?);
    }
    Ok(map)
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
    /// and comes back `I64`), `Bool`. The shapes that flatten (`U64`, `Timestamp`, UTF-8
    /// `Bytes`, an `I64` past 2^53, an integral `F64`) are the ADR's recorded residuals -- see
    /// the two tests named for them below.
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

    /// `proxy.rs`'s `metric_record`: non-default values on every kind-independent field (unit,
    /// description, start_timestamp, flags, one exemplar carrying a full trace context --
    /// `flags` included, which is what `exemplar_to_table`'s `trace_flags` exists for -- and an
    /// attribute) -- callers fill in `kind`.
    fn metric_record(kind: MetricKind) -> MetricRecord {
        let mut record = MetricRecord::new(intern("test.metric"), kind);
        record.unit = Some(intern("ms"));
        record.description = Some(intern("a test metric"));
        record.start_timestamp = 1_700_000_000_000_000_000;
        record.flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
        record.exemplars = vec![Exemplar {
            timestamp: 1_700_000_000_500_000_000,
            value: 42.0,
            trace: Some(TraceRef { trace_id: [0x33; 16], span_id: Some([0x44; 8]), flags: 1 }),
            filtered_attributes: {
                let mut m = AttrMap::new();
                m.insert("dropped", "yes");
                m
            },
        }];
        record
    }

    fn metric_event(kind: MetricKind) -> Event {
        Event::metric(1_700_000_000_000_000_000, round_trippable_attributes(), metric_record(kind))
    }

    fn sum_kind() -> MetricKind {
        MetricKind::Sum(Sum { value: 12.5, temporality: Temporality::Cumulative, monotonic: false })
    }

    fn gauge_kind() -> MetricKind {
        MetricKind::Gauge(3.25)
    }

    fn samples_kind() -> MetricKind {
        let mut samples = Samples::new([1.0, 2.0, 3.5]);
        samples.sample_rate = 0.5;
        MetricKind::Samples(samples)
    }

    fn set_members_kind() -> MetricKind {
        MetricKind::SetMembers(vec![Bytes::from_static(b"alice"), Bytes::from_static(b"bob")])
    }

    /// A one-record metric event with `name = "m"` and the given literal fields, minted from
    /// `timestamp = "1"` and no attributes -- the shape every "this literal yields this record"
    /// test below asserts against.
    fn minted_metric(record: MetricRecord) -> Event {
        Event::metric(1, AttrMap::new(), record)
    }

    /// `process()`'s result for `Event.new{timestamp = "1", metrics = {{name = "m", <fields>}}}`.
    fn mint_metric(fields: &str) -> Event {
        let w = worker(&format!(
            r#"function process(event) return Event.new{{timestamp = "1", metrics = {{{{name = "m", {fields}}}}}}} end"#
        ));
        emitted(w.process(Event::empty(0, AttrMap::new())).unwrap())
    }

    /// The error for `Event.new{timestamp = "1", metrics = {{<fields>}}}`.
    fn metric_err(fields: &str) -> String {
        new_err(&format!(r#"{{timestamp = "1", metrics = {{{{{fields}}}}}}}"#))
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

    /// The other two attribute residuals `docs/design/lua-api.md` lists: an `I64` past 2^53 is
    /// emitted by `to_table()` as a decimal string (`exact_i64_to_lua`'s fallback, the branch
    /// `Timestamp` takes) and comes back `Str`; an integral `F64` is a Lua number LuaJIT's
    /// dual-number mode canonicalizes to an integer, so it comes back `I64`.
    #[test]
    fn new_of_to_table_flattens_a_wide_i64_to_str_and_an_integral_f64_to_i64() {
        let w = worker(REBUILD);
        let mut event = log_event();
        event.attributes.insert("wide", Value::I64(9_007_199_254_740_993));
        event.attributes.insert("whole", Value::F64(3.0));
        let out = emitted(w.process(event).unwrap());
        assert_eq!(out.attributes.get("wide"), Some(&Value::str("9007199254740993")));
        assert_eq!(out.attributes.get("whole"), Some(&Value::I64(3)));
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
    fn a_non_utf8_attribute_key_is_rejected() {
        let err = new_err(r#"{timestamp = "1", attributes = {["\255"] = 1}}"#);
        assert!(err.contains("Event.new: attributes has a non-UTF-8 key"), "got: {err}");
    }

    /// A bad attribute *value* is prefixed and located like every other mistake, rather than
    /// surfacing `lua_to_value`'s bare "can't use a Lua function as an event attribute value".
    #[test]
    fn a_bad_attribute_value_names_its_dotted_path() {
        let err = new_err(r#"{timestamp = "1", attributes = {cb = tostring}}"#);
        assert!(err.contains("Event.new: attributes.cb can't be a Lua function"), "got: {err}");
    }

    #[test]
    fn a_non_boolean_has_flag_is_rejected() {
        let err = new_err(r#"{timestamp = "1", has_log = "yes"}"#);
        assert!(err.contains("Event.new: has_log must be a boolean, got string"), "got: {err}");
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

    // -- metrics ------------------------------------------------------------------------------

    #[test]
    fn new_of_to_table_round_trips_a_sum_metric_event_whole() {
        let w = worker(REBUILD);
        let out = emitted(w.process(metric_event(sum_kind())).unwrap());
        assert_eq!(out, metric_event(sum_kind()));
    }

    #[test]
    fn new_of_to_table_round_trips_a_gauge_metric_event_whole() {
        let w = worker(REBUILD);
        let out = emitted(w.process(metric_event(gauge_kind())).unwrap());
        assert_eq!(out, metric_event(gauge_kind()));
    }

    #[test]
    fn new_of_to_table_round_trips_a_samples_metric_event_whole() {
        let w = worker(REBUILD);
        let out = emitted(w.process(metric_event(samples_kind())).unwrap());
        assert_eq!(out, metric_event(samples_kind()));
    }

    #[test]
    fn new_of_to_table_round_trips_a_set_members_metric_event_whole() {
        let w = worker(REBUILD);
        let out = emitted(w.process(metric_event(set_members_kind())).unwrap());
        assert_eq!(out, metric_event(set_members_kind()));
    }

    #[test]
    fn new_of_to_table_round_trips_a_multi_metric_event_in_order() {
        let mut event = metric_event(sum_kind());
        event.metrics.push(metric_record(gauge_kind()));
        let w = worker(REBUILD);
        let out = emitted(w.process(event.clone()).unwrap());
        assert_eq!(out, event);
        assert_eq!(out.metrics[0].kind, sum_kind());
        assert_eq!(out.metrics[1].kind, gauge_kind());
    }

    /// The `exemplar_to_table` addition this workstream makes: without `trace_flags` in the
    /// table, a flagged exemplar would rebuild with `flags: 0` and the whole-event round-trips
    /// above would fail. Proven from a literal too, so the field is known to be *read*, not
    /// just emitted.
    #[test]
    fn an_exemplar_round_trips_its_trace_flags() {
        let out = mint_metric(
            r#"kind = "gauge", value = 1, exemplars = {{timestamp = "5", value = 2, trace_id = string.rep("33", 16), span_id = string.rep("44", 8), trace_flags = 1}}"#,
        );
        let mut record = MetricRecord::new(intern("m"), MetricKind::Gauge(1.0));
        record.exemplars = vec![Exemplar {
            timestamp: 5,
            value: 2.0,
            trace: Some(TraceRef { trace_id: [0x33; 16], span_id: Some([0x44; 8]), flags: 1 }),
            filtered_attributes: AttrMap::new(),
        }];
        assert_eq!(out, minted_metric(record));
    }

    #[test]
    fn a_bare_sum_is_a_counter() {
        // `MetricKind::counter`'s defaults: delta, monotonic.
        let out = mint_metric(r#"kind = "sum", value = 1"#);
        assert_eq!(out, minted_metric(MetricRecord::new(intern("m"), MetricKind::counter(1.0))));
    }

    #[test]
    fn a_sum_takes_its_temporality_and_monotonicity() {
        let out = mint_metric(
            r#"kind = "sum", value = 1, temporality = "cumulative", monotonic = false"#,
        );
        assert_eq!(
            out.metrics[0].kind,
            MetricKind::Sum(Sum {
                value: 1.0,
                temporality: Temporality::Cumulative,
                monotonic: false
            })
        );
    }

    #[test]
    fn a_bare_samples_metric_gets_cores_defaults() {
        // No `values` is empty; no `sample_rate` is `Samples::new`'s 1.0.
        let out = mint_metric(r#"kind = "samples""#);
        assert_eq!(out.metrics[0].kind, MetricKind::Samples(Samples::new(std::iter::empty())));
    }

    #[test]
    fn a_bare_set_members_metric_is_empty() {
        let out = mint_metric(r#"kind = "set_members""#);
        assert_eq!(out.metrics[0].kind, MetricKind::SetMembers(Vec::new()));
    }

    #[test]
    fn is_no_recorded_value_true_sets_the_flag_bit() {
        let out =
            mint_metric(r#"kind = "gauge", value = 1, flags = 0, is_no_recorded_value = true"#);
        assert!(out.metrics[0].is_no_recorded_value());
        assert_eq!(out.metrics[0].flags, MetricRecord::FLAG_NO_RECORDED_VALUE);
    }

    #[test]
    fn is_no_recorded_value_false_leaves_flags_alone() {
        let out =
            mint_metric(r#"kind = "gauge", value = 1, flags = 1, is_no_recorded_value = false"#);
        assert!(out.metrics[0].is_no_recorded_value());
        assert_eq!(out.metrics[0].flags, 1);
    }

    #[test]
    fn an_unknown_metric_kind_lists_the_constructible_ones() {
        let err = metric_err(r#"name = "m", kind = "counter", value = 1"#);
        assert!(
            err.contains(
                "Event.new: metrics[1].kind must be one of sum, gauge, samples, set_members, \
                 histogram, exponential_histogram, summary, got \"counter\""
            ),
            "got: {err}"
        );
    }

    #[test]
    fn an_unknown_kind_is_reported_before_its_payload_keys() {
        // `value` isn't a field of any known kind's key set here, but the kind error wins.
        let err = metric_err(r#"name = "m", kind = "nope", value = 1"#);
        assert!(err.contains("metrics[1].kind must be one of"), "got: {err}");
    }

    #[test]
    fn the_sketch_kinds_name_their_raw_kind() {
        let err = metric_err(r#"name = "m", kind = "distribution", count = 3"#);
        assert!(
            err.contains(
                "Event.new: metrics[1].kind \"distribution\" is not constructible from Lua -- a \
                 merged sketch; build a \"samples\" metric and let aggregate summarize it"
            ),
            "got: {err}"
        );
        let err = metric_err(r#"name = "m", kind = "set", estimate = 3"#);
        assert!(
            err.contains(
                "Event.new: metrics[1].kind \"set\" is not constructible from Lua -- a merged \
                 sketch; build a \"set_members\" metric and let aggregate summarize it"
            ),
            "got: {err}"
        );
    }

    #[test]
    fn gauge_delta_is_never_constructible() {
        let err = metric_err(r#"name = "m", kind = "gauge_delta", value = 1"#);
        assert!(
            err.contains(
                "Event.new: metrics[1].kind \"gauge_delta\" is not constructible from Lua -- \
                 aggregate's private intermediate, never valid at a sink"
            ),
            "got: {err}"
        );
    }

    #[test]
    fn the_pre_aggregated_kinds_are_not_constructible_yet() {
        for kind in ["histogram", "exponential_histogram", "summary"] {
            let err = metric_err(&format!(r#"name = "m", kind = "{kind}""#));
            assert!(
                err.contains(&format!(
                    "Event.new: metrics[1].kind \"{kind}\" is not constructible yet"
                )),
                "got: {err}"
            );
        }
    }

    #[test]
    fn a_non_numeric_value_is_rejected() {
        let err = metric_err(r#"name = "m", kind = "gauge", value = "x""#);
        assert!(
            err.contains("Event.new: metrics[1].value must be a number, got string"),
            "got: {err}"
        );
    }

    #[test]
    fn a_missing_value_is_required() {
        let err = metric_err(r#"name = "m", kind = "sum""#);
        assert!(err.contains("Event.new: metrics[1].value is required"), "got: {err}");
    }

    #[test]
    fn a_non_finite_value_is_rejected() {
        let err = metric_err(r#"name = "m", kind = "gauge", value = 0/0"#);
        assert!(
            err.contains("Event.new: metrics[1].value must be a finite number, got NaN"),
            "got: {err}"
        );
        let err = metric_err(r#"name = "m", kind = "sum", value = math.huge"#);
        assert!(
            err.contains("Event.new: metrics[1].value must be a finite number, got inf"),
            "got: {err}"
        );
    }

    #[test]
    fn a_non_sequence_values_table_is_rejected() {
        let err = metric_err(r#"name = "m", kind = "samples", values = {[2] = 1}"#);
        assert!(
            err.contains("Event.new: metrics[1].values must be a contiguous array-like table"),
            "got: {err}"
        );
    }

    #[test]
    fn a_non_numeric_sample_is_rejected_by_index() {
        let err = metric_err(r#"name = "m", kind = "samples", values = {1, "two"}"#);
        assert!(
            err.contains("Event.new: metrics[1].values[2] must be a number, got string"),
            "got: {err}"
        );
    }

    #[test]
    fn a_non_string_member_is_rejected_by_index() {
        let err = metric_err(r#"name = "m", kind = "set_members", members = {1}"#);
        assert!(
            err.contains("Event.new: metrics[1].members[1] must be a string, got integer"),
            "got: {err}"
        );
    }

    #[test]
    fn a_non_numeric_sample_rate_is_rejected() {
        let err = metric_err(r#"name = "m", kind = "samples", sample_rate = "fast""#);
        assert!(
            err.contains("Event.new: metrics[1].sample_rate must be a number, got string"),
            "got: {err}"
        );
    }

    #[test]
    fn an_exemplar_span_id_without_a_trace_id_is_rejected_with_its_path() {
        let err = metric_err(
            r#"name = "m", kind = "gauge", value = 1, exemplars = {{timestamp = "1", value = 1, span_id = "4444444444444444"}}"#,
        );
        assert!(
            err.contains(
                "Event.new: metrics[1].exemplars[1].span_id can't be set without a trace_id"
            ),
            "got: {err}"
        );
    }

    #[test]
    fn an_unknown_exemplar_key_is_not_a_field() {
        let err = metric_err(
            r#"name = "m", kind = "gauge", value = 1, exemplars = {{timestamp = "1", value = 1, flags = 1}}"#,
        );
        assert!(
            err.contains("Event.new: metrics[1].exemplars[1].flags is not a field"),
            "got: {err}"
        );
    }

    #[test]
    fn an_exemplar_without_a_timestamp_is_required() {
        let err = metric_err(r#"name = "m", kind = "gauge", value = 1, exemplars = {{value = 1}}"#);
        assert!(
            err.contains("Event.new: metrics[1].exemplars[1].timestamp is required"),
            "got: {err}"
        );
    }

    #[test]
    fn a_non_table_exemplar_is_rejected() {
        let err = metric_err(r#"name = "m", kind = "gauge", value = 1, exemplars = {1}"#);
        assert!(
            err.contains("Event.new: metrics[1].exemplars[1] must be a table, got integer"),
            "got: {err}"
        );
    }

    #[test]
    fn an_unknown_metric_key_is_not_a_field() {
        let err = metric_err(r#"name = "m", kind = "gauge", value = 1, bogus = 1"#);
        assert!(err.contains("Event.new: metrics[1].bogus is not a field"), "got: {err}");
    }

    #[test]
    fn a_payload_key_of_another_kind_is_not_a_field() {
        // `monotonic` is a `sum` field; on a `gauge` it's unknown.
        let err = metric_err(r#"name = "m", kind = "gauge", value = 1, monotonic = true"#);
        assert!(err.contains("Event.new: metrics[1].monotonic is not a field"), "got: {err}");
    }

    #[test]
    fn a_metric_without_a_name_is_required() {
        let err = metric_err(r#"kind = "gauge", value = 1"#);
        assert!(err.contains("Event.new: metrics[1].name is required"), "got: {err}");
    }

    #[test]
    fn a_metric_without_a_kind_is_required() {
        let err = metric_err(r#"name = "m", value = 1"#);
        assert!(err.contains("Event.new: metrics[1].kind is required"), "got: {err}");
    }

    #[test]
    fn a_non_table_metric_is_rejected() {
        let err = new_err(r#"{timestamp = "1", metrics = {5}}"#);
        assert!(err.contains("Event.new: metrics[1] must be a table, got integer"), "got: {err}");
    }

    #[test]
    fn an_unknown_temporality_lists_the_names() {
        let err = metric_err(r#"name = "m", kind = "sum", value = 1, temporality = "total""#);
        assert!(
            err.contains(
                "Event.new: metrics[1].temporality must be one of delta, cumulative (or nil), \
                 got \"total\""
            ),
            "got: {err}"
        );
    }
}
