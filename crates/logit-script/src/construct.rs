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
//!   `Samples::new`'s `sample_rate` of `1.0`; a span's `SpanKind::Internal`/`SpanStatus::Unset`,
//!   an `end_timestamp` equal to its start, `SpanExt`'s zeros, empty `events`/`links`);
//!   `timestamp`, `log.message`, a metric's `name`/`kind` and its kind's own payload, and a
//!   span's `trace_id`/`span_id`/`name` are required, exactly as the ADR lists.
//!
//! Every error is an `mlua::Error::RuntimeError` prefixed `Event.new: <path> ...` down to the
//! field; the one exception is a malformed value *inside* a nested attribute table, which reports
//! the shared attribute-conversion error (`lua_to_value`'s, unprefixed), since nested tables
//! convert through the same helper the proxy write path uses. The shared helpers at the bottom
//! take the path as an argument so the log, metric and span parsers reuse them unchanged.
//! `metrics` builds the four raw kinds (`sum`, `gauge`, `samples`, `set_members`) and the three
//! pre-aggregated ones (`histogram`, `exponential_histogram`, `summary`), exemplars included; the
//! two sketches and `gauge_delta` are never constructible, and each says so. `span` builds a
//! whole [`SpanRecord`], its `events` and `links` included -- the one way a script mints a span,
//! since `event.span` itself stays read-only in place (`crate::proxy`'s `SpanProxy`).

use crate::proxy::{EventProxy, TargetTable};
use crate::value::{lua_to_value, validated_sequence_len};
use bytes::Bytes;
use logit_core::interner::{intern, Symbol};
use logit_core::trace::{parse_span_id, parse_trace_id, TraceRef};
use logit_core::{
    AttrMap, BodyFormat, Event, Exemplar, ExpHistogram, Histogram, LogRecord, MetricKind,
    MetricList, MetricRecord, Samples, Severity, SpanEvent, SpanExt, SpanKind, SpanLink,
    SpanRecord, SpanStatus, Sum, Summary, Temporality, Value,
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
/// ([`Kind::keys`]) are added to these for the key check, once the kind is known.
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

/// The keys of one row of a `histogram`'s `buckets`, as `metric_to_table` emits them.
const BUCKET_KEYS: &[&str] = &["bound", "count"];

/// The keys of an `exponential_histogram`'s `positive`/`negative` table (`exp_buckets_table`).
const EXP_BUCKETS_KEYS: &[&str] = &["offset", "counts"];

/// The keys of one row of a `summary`'s `quantiles`.
const QUANTILE_KEYS: &[&str] = &["quantile", "value"];

/// The keys `span_to_table` emits.
const SPAN_KEYS: &[&str] = &[
    "trace_id",
    "span_id",
    "parent_span_id",
    "name",
    "kind",
    "status",
    "status_message",
    "trace_state",
    "end_timestamp",
    "flags",
    "dropped_attributes_count",
    "dropped_events_count",
    "dropped_links_count",
    "events",
    "links",
];

/// The keys `span_event_to_table` emits.
const SPAN_EVENT_KEYS: &[&str] = &["timestamp", "name", "attributes", "dropped_attributes_count"];

/// The keys `span_link_to_table` emits.
const SPAN_LINK_KEYS: &[&str] =
    &["trace_id", "span_id", "trace_state", "flags", "dropped_attributes_count", "attributes"];

/// The kinds the unknown-`kind` error names: every kind `Event.new` builds -- deliberately *not*
/// the sketches or `gauge_delta`, which are never constructible and get their own message each.
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
    let span = match t.raw_get::<_, LuaValue>("span")? {
        LuaValue::Nil => None,
        // The event's `timestamp` is the span's start (`SpanRecord::end_timestamp`'s doc), so the
        // span parser gets it: it is the `end_timestamp` default and the floor `end_timestamp`
        // is checked against.
        LuaValue::Table(span) => Some(span_from_table(span, "span", timestamp)?),
        other => {
            return Err(runtime_error(format!(
                "Event.new: span must be a table (or nil), got {}",
                other.type_name()
            )))
        }
    };
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
    event.span = span;
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
        true,
    )?;
    let body_format = enum_field(
        t.raw_get("body_format")?,
        path,
        "body_format",
        &BodyFormat::NAMES,
        BodyFormat::from_name,
        true,
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
    let kind = Kind::parse(&kind_name.to_string_lossy(), path)?;
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
        Kind::Sum => {
            // The defaults are `MetricKind::counter`'s -- delta, monotonic -- so
            // `{kind = "sum", value = 1}` is exactly the counter `kv_metrics`/`statsd_in` emit.
            let value = finite_field(t.raw_get("value")?, path, "value")?;
            let temporality = enum_field(
                t.raw_get("temporality")?,
                path,
                "temporality",
                &Temporality::NAMES,
                Temporality::from_name,
                true,
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
        Kind::Gauge => MetricKind::Gauge(finite_field(t.raw_get("value")?, path, "value")?),
        Kind::Samples => {
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
        Kind::SetMembers => {
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
        Kind::Histogram => {
            // `buckets` is required even when empty: `to_table()` always emits it, and an empty
            // histogram round-trips as one. Each row is `{bound, count}`, checked strictly.
            let buckets = sequence_field(t.raw_get("buckets")?, path, "buckets", bucket_from_row)?;
            MetricKind::Histogram(Histogram {
                buckets,
                temporality: temporality_field(&t, path)?,
                sum: optional_finite_field(t.raw_get("sum")?, path, "sum")?,
                min: optional_finite_field(t.raw_get("min")?, path, "min")?,
                max: optional_finite_field(t.raw_get("max")?, path, "max")?,
            })
        }
        Kind::ExponentialHistogram => MetricKind::ExponentialHistogram(ExpHistogram {
            scale: i32_field(t.raw_get("scale")?, path, "scale")?,
            zero_count: count_field(t.raw_get("zero_count")?, path, "zero_count")?,
            zero_threshold: finite_field(t.raw_get("zero_threshold")?, path, "zero_threshold")?,
            positive: exp_buckets_from_table(t.raw_get("positive")?, path, "positive")?,
            negative: exp_buckets_from_table(t.raw_get("negative")?, path, "negative")?,
            temporality: temporality_field(&t, path)?,
            count: count_field(t.raw_get("count")?, path, "count")?,
            sum: optional_finite_field(t.raw_get("sum")?, path, "sum")?,
            min: optional_finite_field(t.raw_get("min")?, path, "min")?,
            max: optional_finite_field(t.raw_get("max")?, path, "max")?,
        }),
        Kind::Summary => MetricKind::Summary(Summary {
            quantiles: sequence_field(
                t.raw_get("quantiles")?,
                path,
                "quantiles",
                quantile_from_row,
            )?,
            count: count_field(t.raw_get("count")?, path, "count")?,
            sum: finite_field(t.raw_get("sum")?, path, "sum")?,
        }),
    };
    Ok(MetricRecord { name, unit, description, start_timestamp, exemplars, flags, kind })
}

/// The metric kinds `Event.new` builds -- the four raw, pre-aggregation ones and the three
/// pre-aggregated ones a scrape or OTLP input carries whole -- and the payload keys each adds to
/// [`METRIC_KEYS`]. [`Kind::parse`] is where every kind that *isn't* one of these gets its own
/// message.
#[derive(Clone, Copy)]
enum Kind {
    Sum,
    Gauge,
    Samples,
    SetMembers,
    Histogram,
    ExponentialHistogram,
    Summary,
}

impl Kind {
    fn parse(name: &str, path: &str) -> mlua::Result<Self> {
        Ok(match name {
            "sum" => Kind::Sum,
            "gauge" => Kind::Gauge,
            "samples" => Kind::Samples,
            "set_members" => Kind::SetMembers,
            "histogram" => Kind::Histogram,
            "exponential_histogram" => Kind::ExponentialHistogram,
            "summary" => Kind::Summary,
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
            Kind::Sum => &["value", "temporality", "monotonic"],
            Kind::Gauge => &["value"],
            Kind::Samples => &["values", "sample_rate"],
            Kind::SetMembers => &["members"],
            Kind::Histogram => &["buckets", "temporality", "sum", "min", "max"],
            Kind::ExponentialHistogram => &[
                "scale",
                "zero_count",
                "zero_threshold",
                "positive",
                "negative",
                "temporality",
                "count",
                "sum",
                "min",
                "max",
            ],
            Kind::Summary => &["quantiles", "count", "sum"],
        }
    }
}

fn not_constructible(path: &str, name: &str, why: &str) -> mlua::Error {
    runtime_error(format!(
        "Event.new: {} \"{name}\" is not constructible from Lua -- {why}",
        dotted(path, "kind")
    ))
}

/// A `histogram`'s or `exponential_histogram`'s `temporality`: the one enum field that is
/// *required*, because core documents no default for it (ADR `lua-event-constructor` -- a `sum`
/// has `MetricKind::counter`'s delta, these have nothing to fall back on).
fn temporality_field(t: &Table, path: &str) -> mlua::Result<Temporality> {
    enum_field(
        t.raw_get("temporality")?,
        path,
        "temporality",
        &Temporality::NAMES,
        Temporality::from_name,
        false,
    )?
    .ok_or_else(|| required(path, "temporality"))
}

/// One `{bound=, count=}` row of a `histogram`'s `buckets` (`metric_to_table`'s `Histogram`
/// arm). `path` yields the row's own path (`metrics[i].buckets[k]`); it is built once here since
/// `expect_keys` needs it up front -- the same one-`String`-per-row cost the exemplar loop pays.
fn bucket_from_row(value: LuaValue, path: &dyn Fn() -> String) -> mlua::Result<(f64, u64)> {
    let row = row_table(value, path)?;
    let path = path();
    expect_keys(&row, BUCKET_KEYS, &path)?;
    let bound = bound_field(row.raw_get("bound")?, &path, "bound")?;
    let count = count_field(row.raw_get("count")?, &path, "count")?;
    Ok((bound, count))
}

/// One `{quantile=, value=}` row of a `summary`'s `quantiles`. A `quantile` outside `[0, 1]` is
/// accepted as-is: `Summary` doesn't constrain it, and neither does any producer, so the
/// constructor doesn't invent a rule the model lacks.
fn quantile_from_row(value: LuaValue, path: &dyn Fn() -> String) -> mlua::Result<(f64, f64)> {
    let row = row_table(value, path)?;
    let path = path();
    expect_keys(&row, QUANTILE_KEYS, &path)?;
    let quantile = finite_field(row.raw_get("quantile")?, &path, "quantile")?;
    let value = finite_field(row.raw_get("value")?, &path, "value")?;
    Ok((quantile, value))
}

/// An entry of a sequence that must itself be a table (a bucket or quantile row).
fn row_table<'lua>(value: LuaValue<'lua>, path: &dyn Fn() -> String) -> mlua::Result<Table<'lua>> {
    match value {
        LuaValue::Table(row) => Ok(row),
        other => Err(runtime_error(format!(
            "Event.new: {} must be a table, got {}",
            path(),
            other.type_name()
        ))),
    }
}

/// An `exponential_histogram`'s `positive`/`negative` table, `exp_buckets_table`'s
/// `{offset=, counts=[...]}`: required, strict about its keys, and `counts` is required even
/// when empty (an empty side is what `to_table()` emits for one).
fn exp_buckets_from_table(value: LuaValue, path: &str, key: &str) -> mlua::Result<(i32, Vec<u64>)> {
    let table = match value {
        LuaValue::Nil => return Err(required(path, key)),
        LuaValue::Table(table) => table,
        other => {
            return Err(runtime_error(format!(
                "Event.new: {} must be a table, got {}",
                dotted(path, key),
                other.type_name()
            )))
        }
    };
    let path = dotted(path, key);
    expect_keys(&table, EXP_BUCKETS_KEYS, &path)?;
    let offset = i32_field(table.raw_get("offset")?, &path, "offset")?;
    // A closure rather than `count` itself: `count` is generic over its path closure, which
    // can't coerce to the higher-ranked `fn` pointer `sequence_field` takes.
    let counts =
        sequence_field(table.raw_get("counts")?, &path, "counts", |v, field| count(v, field))?;
    Ok((offset, counts))
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

/// Builds a [`SpanRecord`] from a table in `span_to_table`'s shape. `path` is the table's own
/// path (`span`); `start` is the event's already-parsed `timestamp`, which is the span's start
/// (`SpanRecord::end_timestamp`'s doc) -- the default for `end_timestamp` and the floor it is
/// checked against, the same `end < start` rule `trace_context`'s `span:` block applies to a
/// lifted span (`crates/logit-transforms/src/trace_context.rs`).
fn span_from_table(t: Table, path: &str, start: i64) -> mlua::Result<SpanRecord> {
    expect_keys(&t, SPAN_KEYS, path)?;
    let trace_id = required_hex_id(t.raw_get("trace_id")?, path, "trace_id", parse_trace_id)?;
    let span_id = required_hex_id(t.raw_get("span_id")?, path, "span_id", parse_span_id)?;
    let parent_span_id =
        hex_id_field(t.raw_get("parent_span_id")?, path, "parent_span_id", parse_span_id, true)?;
    let name = match t.raw_get::<_, LuaValue>("name")? {
        // As `log.message`: a `Value::Null` name is an absent key in `to_table()`'s output and
        // comes back as missing -- the ADR's recorded residual.
        LuaValue::Nil => return Err(required(path, "name")),
        value => value_field(value, path, "name")?,
    };
    let kind =
        enum_field(t.raw_get("kind")?, path, "kind", &SpanKind::NAMES, SpanKind::from_name, true)?
            .unwrap_or(SpanKind::Internal);
    let status = enum_field(
        t.raw_get("status")?,
        path,
        "status",
        &SpanStatus::NAMES,
        SpanStatus::from_name,
        true,
    )?
    .unwrap_or(SpanStatus::Unset);
    let end_timestamp = match t.raw_get::<_, LuaValue>("end_timestamp")? {
        LuaValue::Nil => start,
        value => nanos_string(value, path, "end_timestamp")?,
    };
    if end_timestamp < start {
        return Err(runtime_error(format!(
            "Event.new: {} precedes timestamp",
            dotted(path, "end_timestamp")
        )));
    }
    let flags = u32_field(t.raw_get("flags")?, path, "flags")?;
    // `SpanExt` is boxed only when something in it is non-default -- `crates/logit-proto`'s
    // `ext_from_wire` rule -- so a minimal constructed span costs what a minimal decoded one
    // does, and `to_table()`'s `nil`/`0` for an `ext`-less span rebuilds as `ext: None`.
    let ext = SpanExt {
        status_message: bytes_field(t.raw_get("status_message")?, path, "status_message")?,
        trace_state: bytes_field(t.raw_get("trace_state")?, path, "trace_state")?,
        dropped_attributes_count: u32_field(
            t.raw_get("dropped_attributes_count")?,
            path,
            "dropped_attributes_count",
        )?,
        dropped_events_count: u32_field(
            t.raw_get("dropped_events_count")?,
            path,
            "dropped_events_count",
        )?,
        dropped_links_count: u32_field(
            t.raw_get("dropped_links_count")?,
            path,
            "dropped_links_count",
        )?,
    };
    let ext = (ext != SpanExt::default()).then(|| Box::new(ext));
    // Each row's parser needs its `span.events[i]`/`span.links[i]` path up front for
    // `expect_keys`, so it is built once per row -- the same one-`String`-per-row cost the
    // exemplar and bucket loops pay -- while every scalar beneath stays lazy.
    let events = optional_sequence_field(t.raw_get("events")?, path, "events", |value, path| {
        span_event_from_table(row_table(value, path)?, &path())
    })?;
    let links = optional_sequence_field(t.raw_get("links")?, path, "links", |value, path| {
        span_link_from_table(row_table(value, path)?, &path())
    })?;
    Ok(SpanRecord {
        trace_id,
        span_id,
        parent_span_id,
        name,
        kind,
        status,
        events,
        links,
        end_timestamp,
        flags,
        ext,
    })
}

/// Builds a [`SpanEvent`] from a table in `span_event_to_table`'s shape. `path` is the row's
/// own path (`span.events[i]`).
fn span_event_from_table(t: Table, path: &str) -> mlua::Result<SpanEvent> {
    expect_keys(&t, SPAN_EVENT_KEYS, path)?;
    let timestamp = match t.raw_get::<_, LuaValue>("timestamp")? {
        LuaValue::Nil => return Err(required(path, "timestamp")),
        value => nanos_string(value, path, "timestamp")?,
    };
    let name = match t.raw_get::<_, LuaValue>("name")? {
        LuaValue::Nil => return Err(required(path, "name")),
        value => value_field(value, path, "name")?,
    };
    let attributes = attributes_field(t.raw_get("attributes")?, path, "attributes")?;
    let dropped_attributes_count =
        u32_field(t.raw_get("dropped_attributes_count")?, path, "dropped_attributes_count")?;
    Ok(SpanEvent { timestamp, name, attributes, dropped_attributes_count })
}

/// Builds a [`SpanLink`] from a table in `span_link_to_table`'s shape. `path` is the row's own
/// path (`span.links[i]`). Unlike a log's or exemplar's trace context, a link's `trace_id` and
/// `span_id` are both required: a link *is* a reference to another span, so there is no
/// "trace only" shape for it.
fn span_link_from_table(t: Table, path: &str) -> mlua::Result<SpanLink> {
    expect_keys(&t, SPAN_LINK_KEYS, path)?;
    let trace_id = required_hex_id(t.raw_get("trace_id")?, path, "trace_id", parse_trace_id)?;
    let span_id = required_hex_id(t.raw_get("span_id")?, path, "span_id", parse_span_id)?;
    let trace_state = bytes_field(t.raw_get("trace_state")?, path, "trace_state")?;
    let flags = u32_field(t.raw_get("flags")?, path, "flags")?;
    let dropped_attributes_count =
        u32_field(t.raw_get("dropped_attributes_count")?, path, "dropped_attributes_count")?;
    let attributes = attributes_field(t.raw_get("attributes")?, path, "attributes")?;
    Ok(SpanLink { trace_id, span_id, attributes, flags, trace_state, dropped_attributes_count })
}

// -- shared helpers -----------------------------------------------------------------------------
//
// Each takes the `path` of the table it's reading (`""` at the top level) and the `key` within
// it separately, and only joins them (`dotted`) on the error path: a constructed event's
// success path allocates nothing for messages it never raises (the `lua: Event.new ..` pins in
// `crates/logit-bench/tests/allocations.rs`), and the metric/exemplar/span parsers pass
// `metrics[i]`, `metrics[i].exemplars[j]`, `span`, `span.events[i]`, ... as `path` unchanged.

fn runtime_error(message: String) -> mlua::Error {
    mlua::Error::RuntimeError(message)
}

fn required(path: &str, key: &str) -> mlua::Error {
    runtime_error(format!("Event.new: {} is required", dotted(path, key)))
}

/// A field that must be a sequence (`validated_sequence_len`'s contiguous-from-one rule) but is
/// either a non-table or a table with other keys: `metrics`, a metric's `exemplars`/`values`/
/// `members`/`buckets`/`quantiles`, an exponential histogram side's `counts`.
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

/// [`dotted`] with a one-based index appended: `<path>.<key>[k]`.
fn indexed(path: &str, key: &str, k: usize) -> String {
    match path.is_empty() {
        true => format!("{key}[{k}]"),
        false => format!("{path}.{key}[{k}]"),
    }
}

/// A *required* sequence field (a histogram's `buckets`, a summary's `quantiles`, an exponential
/// histogram side's `counts` -- each of which `to_table()` always emits, empty or not), every
/// entry parsed by `entry` from its value and a closure yielding its path (`<path>.<key>[k]`).
/// The closure keeps a scalar entry's path off the success path (`count` only calls it to build
/// an error); a row parser calls it once up front because `expect_keys` needs the path eagerly.
fn sequence_field<T>(
    value: LuaValue,
    path: &str,
    key: &str,
    entry: fn(LuaValue, &dyn Fn() -> String) -> mlua::Result<T>,
) -> mlua::Result<Vec<T>> {
    let list = match value {
        LuaValue::Nil => return Err(required(path, key)),
        LuaValue::Table(list) => list,
        _ => return Err(not_a_sequence(path, key)),
    };
    let Some(len) = validated_sequence_len(&list)? else {
        return Err(not_a_sequence(path, key));
    };
    let mut out = Vec::with_capacity(len);
    for k in 1..=len {
        out.push(entry(list.raw_get(k)?, &|| indexed(path, key, k))?);
    }
    Ok(out)
}

/// [`sequence_field`] for a sequence that may be absent (a span's `events`/`links`, which
/// `to_table()` always emits but a script minting a span needn't): `nil` is empty -- and free,
/// `Vec::new()` allocates nothing -- anything else goes through [`sequence_field`].
fn optional_sequence_field<T>(
    value: LuaValue,
    path: &str,
    key: &str,
    entry: fn(LuaValue, &dyn Fn() -> String) -> mlua::Result<T>,
) -> mlua::Result<Vec<T>> {
    match value {
        LuaValue::Nil => Ok(Vec::new()),
        value => sequence_field(value, path, key, entry),
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

/// A *required* non-negative integer that fits a `u64` (a histogram bucket's or a summary's
/// `count`, an exponential histogram's `zero_count`/`count`): `nil` is "is required", and
/// anything else goes through [`count`]. Unlike [`u32_field`], no default: core documents none
/// for any of these.
fn count_field(value: LuaValue, path: &str, key: &str) -> mlua::Result<u64> {
    match value {
        LuaValue::Nil => Err(required(path, key)),
        value => count(value, || dotted(path, key)),
    }
}

/// [`u32_field`]'s rule widened to `u64` and with a lazily built field name (an exponential
/// histogram's `counts[k]` element costs no `format!` on the success path): a Lua integer that
/// is `>= 0`, or an integral float in `[0, 2^64)`. `to_table()` emits every count `as i64`, so
/// a round-trip arrives as a Lua integer; the float arm is for a script that computed one.
fn count(value: LuaValue, field: impl FnOnce() -> String) -> mlua::Result<u64> {
    let reject = |got: String| {
        runtime_error(format!("Event.new: {} must be a non-negative integer, got {got}", field()))
    };
    match value {
        LuaValue::Integer(n) => u64::try_from(n).map_err(|_| reject(n.to_string())),
        LuaValue::Number(n)
            if n.fract() == 0.0 && (0.0..18_446_744_073_709_551_616.0).contains(&n) =>
        {
            Ok(n as u64)
        }
        LuaValue::Number(n) => Err(reject(n.to_string())),
        other => Err(reject(other.type_name().to_string())),
    }
}

/// A *required* integer that fits an `i32` (an exponential histogram's `scale`, a side's
/// `offset`): a Lua integer or an integral float within `i32`'s range, negatives included.
fn i32_field(value: LuaValue, path: &str, key: &str) -> mlua::Result<i32> {
    let reject = |got: String| {
        runtime_error(format!(
            "Event.new: {} must be an integer between {} and {}, got {got}",
            dotted(path, key),
            i32::MIN,
            i32::MAX
        ))
    };
    match value {
        LuaValue::Nil => Err(required(path, key)),
        LuaValue::Integer(n) => i32::try_from(n).map_err(|_| reject(n.to_string())),
        LuaValue::Number(n)
            if n.fract() == 0.0 && (f64::from(i32::MIN)..=f64::from(i32::MAX)).contains(&n) =>
        {
            Ok(n as i32)
        }
        LuaValue::Number(n) => Err(reject(n.to_string())),
        other => Err(reject(other.type_name().to_string())),
    }
}

/// An enum-valued field named by its lowercase name: `nil` is `None` (the caller decides whether
/// that is a default or `required`), a string is looked up through `parse` (one of the
/// `from_name`s `logit-core` provides), anything else is an error listing `names` (the matching
/// `NAMES` table) so the message can't drift from the enum. `optional` says whether the messages
/// offer `nil`: a *required* field ([`temporality_field`]) must not tell the author nil is
/// allowed and then reject it as missing.
fn enum_field<T>(
    value: LuaValue,
    path: &str,
    key: &str,
    names: &[&str],
    parse: fn(&str) -> Option<T>,
    optional: bool,
) -> mlua::Result<Option<T>> {
    match value {
        LuaValue::Nil => Ok(None),
        LuaValue::String(s) => {
            let s = s.to_string_lossy();
            parse(&s).map(Some).ok_or_else(|| {
                runtime_error(format!(
                    "Event.new: {} must be one of {}{}, got \"{s}\"",
                    dotted(path, key),
                    names.join(", "),
                    if optional { " (or nil)" } else { "" }
                ))
            })
        }
        other => Err(runtime_error(format!(
            "Event.new: {} must be a string{}, got {}",
            dotted(path, key),
            if optional { " or nil" } else { "" },
            other.type_name()
        ))),
    }
}

/// The log proxy's trace-context rule, applied to three fields read at once: `trace_id` is the
/// gate (absent means no [`TraceRef`] at all), `span_id`/`trace_flags` without it are the same
/// error `event.log.span_id = ..` raises before a `trace_id` is set, and each id goes through
/// [`hex_id_field`]. `path` is the record's path (`log`, `metrics[i].exemplars[j]`), not a
/// field's.
fn trace_ref_from_fields(
    trace_id: LuaValue,
    span_id: LuaValue,
    trace_flags: LuaValue,
    path: &str,
) -> mlua::Result<Option<TraceRef>> {
    let Some(trace_id) = hex_id_field(trace_id, path, "trace_id", parse_trace_id, true)? else {
        for (key, value) in [("span_id", &span_id), ("trace_flags", &trace_flags)] {
            if !matches!(value, LuaValue::Nil) {
                return Err(runtime_error(format!(
                    "Event.new: {} can't be set without a trace_id",
                    dotted(path, key)
                )));
            }
        }
        return Ok(None);
    };
    let span_id = hex_id_field(span_id, path, "span_id", parse_span_id, true)?;
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

/// A hex id field -- a trace id or a span id, `parse` being `logit_core::trace`'s
/// `parse_trace_id`/`parse_span_id` (exact length, hex, not all-zero): `nil` is `None`, and the
/// caller decides whether that is a default (a log's trace context, a span's `parent_span_id`)
/// or "is required" ([`required_hex_id`]). `optional` only shapes the wording, so a required
/// id's error doesn't offer `nil` as a choice.
fn hex_id_field<const N: usize>(
    value: LuaValue,
    path: &str,
    key: &str,
    parse: fn(&str) -> Option<[u8; N]>,
    optional: bool,
) -> mlua::Result<Option<[u8; N]>> {
    match value {
        LuaValue::Nil => Ok(None),
        LuaValue::String(s) => s.to_str().ok().and_then(parse).map(Some).ok_or_else(|| {
            runtime_error(format!(
                "Event.new: {} must be a {}-character hex string{}, and not all-zero",
                dotted(path, key),
                N * 2,
                if optional { " (or nil)" } else { "" }
            ))
        }),
        other => Err(runtime_error(format!(
            "Event.new: {} must be a hex string{}, got {}",
            dotted(path, key),
            if optional { " or nil" } else { "" },
            other.type_name()
        ))),
    }
}

/// [`hex_id_field`] for a *required* id (a span's or a link's `trace_id`/`span_id`).
fn required_hex_id<const N: usize>(
    value: LuaValue,
    path: &str,
    key: &str,
    parse: fn(&str) -> Option<[u8; N]>,
) -> mlua::Result<[u8; N]> {
    hex_id_field(value, path, key, parse, false)?.ok_or_else(|| required(path, key))
}

/// An optional opaque-bytes field (a span's `status_message`/`trace_state`, a link's
/// `trace_state`): `nil` is `None`, a Lua string is copied as-is, UTF-8 or not -- `to_table()`
/// emits these straight from the record's `Bytes`, so this is the exact inverse.
fn bytes_field(value: LuaValue, path: &str, key: &str) -> mlua::Result<Option<Bytes>> {
    match value {
        LuaValue::Nil => Ok(None),
        LuaValue::String(s) => Ok(Some(Bytes::copy_from_slice(s.as_bytes()))),
        other => Err(runtime_error(format!(
            "Event.new: {} must be a string or nil, got {}",
            dotted(path, key),
            other.type_name()
        ))),
    }
}

/// A field holding an arbitrary [`Value`] (`log.message`, a span's and a span event's `name`),
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

/// A required finite number (a `sum`/`gauge`/exemplar `value`, a summary's `sum`, an
/// exponential histogram's `zero_threshold`): `nil` is "is required", and anything else goes
/// through [`finite`].
fn finite_field(value: LuaValue, path: &str, key: &str) -> mlua::Result<f64> {
    match value {
        LuaValue::Nil => Err(required(path, key)),
        value => finite(value, || dotted(path, key)),
    }
}

/// An optional finite number (a histogram's or exponential histogram's `sum`/`min`/`max`, which
/// `to_table()` emits as `nil` when the record has `None`): `nil` is `None`, anything else goes
/// through [`finite`].
fn optional_finite_field(value: LuaValue, path: &str, key: &str) -> mlua::Result<Option<f64>> {
    match value {
        LuaValue::Nil => Ok(None),
        value => finite(value, || dotted(path, key)).map(Some),
    }
}

/// A histogram bucket's `bound`: required, and the one field anywhere in `Event.new` that may
/// be non-finite -- but only as `+inf`. `Histogram`'s last bucket is conventionally
/// `(f64::INFINITY, n)` (Prometheus's `+Inf` bucket, OTLP's implicit overflow bucket), and
/// `to_table()` emits that bound as the Lua number `math.huge`, so it has to come back in for
/// the round-trip to hold. NaN and `-math.huge` are rejected as everywhere else.
fn bound_field(value: LuaValue, path: &str, key: &str) -> mlua::Result<f64> {
    match value {
        LuaValue::Nil => Err(required(path, key)),
        LuaValue::Number(n) if n == f64::INFINITY => Ok(n),
        LuaValue::Number(n) if !n.is_finite() => Err(runtime_error(format!(
            "Event.new: {} must be a finite number or math.huge, got {n}",
            dotted(path, key)
        ))),
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

    /// `proxy.rs`'s `histogram_kind` plus the trailing `+Inf` bucket a Prometheus/OTLP
    /// histogram carries: `to_table()` emits that bound as `math.huge`, and the round-trip
    /// below is what proves `bound_field` lets it back in.
    fn histogram_kind() -> MetricKind {
        MetricKind::Histogram(Histogram {
            buckets: vec![(1.0, 3), (5.0, 7), (f64::INFINITY, 2)],
            temporality: Temporality::Cumulative,
            sum: Some(15.0),
            min: Some(0.5),
            max: Some(9.0),
        })
    }

    fn exp_histogram_kind() -> MetricKind {
        MetricKind::ExponentialHistogram(ExpHistogram {
            scale: 3,
            zero_count: 2,
            zero_threshold: 0.001,
            positive: (1, vec![1, 2, 3]),
            negative: (0, vec![4, 5]),
            temporality: Temporality::Delta,
            count: 11,
            sum: Some(20.0),
            min: Some(-1.0),
            max: Some(9.5),
        })
    }

    fn summary_kind() -> MetricKind {
        MetricKind::Summary(Summary {
            quantiles: vec![(0.5, 10.0), (0.99, 99.0)],
            count: 42,
            sum: 500.0,
        })
    }

    /// `proxy.rs`'s `span_record_with_everything`: every `SpanRecord` field non-default, `ext`
    /// fully populated, one event and one link each carrying attributes. Every value in it is
    /// one `to_table()` emits losslessly (`Value::str` names; `Str`/`Bool` attributes), so the
    /// whole-event round-trip below can `assert_eq!` against it.
    fn span_record_with_everything() -> SpanRecord {
        SpanRecord {
            trace_id: [1; 16],
            span_id: [2; 8],
            parent_span_id: Some([3; 8]),
            name: Value::str("GET /"),
            kind: SpanKind::Server,
            status: SpanStatus::Error,
            events: vec![SpanEvent {
                timestamp: 1_700_000_000_100_000_000,
                name: Value::str("exception"),
                attributes: {
                    let mut m = AttrMap::new();
                    m.insert("type", "Timeout");
                    m
                },
                dropped_attributes_count: 2,
            }],
            links: vec![SpanLink {
                trace_id: [4; 16],
                span_id: [5; 8],
                attributes: {
                    let mut m = AttrMap::new();
                    m.insert("linked", true);
                    m
                },
                flags: 1,
                trace_state: Some(Bytes::from_static(b"vendor=value")),
                dropped_attributes_count: 1,
            }],
            end_timestamp: 1_700_000_000_900_000_000,
            flags: 1,
            ext: Some(Box::new(SpanExt {
                status_message: Some(Bytes::from_static(b"boom")),
                trace_state: Some(Bytes::from_static(b"vendor=xyz")),
                dropped_attributes_count: 3,
                dropped_events_count: 4,
                dropped_links_count: 5,
            })),
        }
    }

    fn span_event() -> Event {
        Event::span(
            1_700_000_000_000_000_000,
            round_trippable_attributes(),
            span_record_with_everything(),
        )
    }

    /// `proxy.rs`'s `minimal_span_record`, started at `5` (the `timestamp` [`mint_span`] uses):
    /// what a `{trace_id, span_id, name}` literal must rebuild as once every default applies.
    fn minimal_span_record() -> SpanRecord {
        SpanRecord {
            trace_id: [9; 16],
            span_id: [8; 8],
            parent_span_id: None,
            name: Value::str("minimal"),
            kind: SpanKind::Internal,
            status: SpanStatus::Unset,
            events: vec![],
            links: vec![],
            end_timestamp: 5,
            flags: 0,
            ext: None,
        }
    }

    /// `process()`'s result for `Event.new{timestamp = "5", span = {<the minimal ids and name>,
    /// <fields>}}` -- the span-side twin of [`mint_metric`].
    fn mint_span(fields: &str) -> Event {
        let w = worker(&format!(
            r#"function process(event) return Event.new{{timestamp = "5", span = {{trace_id = string.rep("09", 16), span_id = string.rep("08", 8), name = "minimal", {fields}}}}} end"#
        ));
        emitted(w.process(Event::empty(0, AttrMap::new())).unwrap())
    }

    /// The error for `Event.new{timestamp = "5", span = {<fields>}}` -- no ids or name filled
    /// in, so a test names exactly the fields it means to.
    fn span_err(fields: &str) -> String {
        new_err(&format!(r#"{{timestamp = "5", span = {{{fields}}}}}"#))
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

    /// The non-finite residual `docs/design/lua-api.md` records: `prometheus_in` and `otlp_in`
    /// both admit a NaN/infinite point, `to_table()` emits the raw float, and the finiteness
    /// rule refuses it on the way back -- a rebuild must fix or drop the value.
    #[test]
    fn new_of_to_table_rejects_a_non_finite_gauge_value() {
        let w = worker(REBUILD);
        let err = process_err(&w, metric_event(MetricKind::Gauge(f64::NAN)));
        assert!(
            err.contains("Event.new: metrics[1].value must be a finite number, got NaN"),
            "got: {err}"
        );
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

    // -- histogram / exponential_histogram / summary ------------------------------------------

    #[test]
    fn new_of_to_table_round_trips_a_histogram_metric_event_whole() {
        // The fixture's last bucket is `(f64::INFINITY, _)`, so this also proves `math.huge`
        // makes it back in as a bound.
        let w = worker(REBUILD);
        let out = emitted(w.process(metric_event(histogram_kind())).unwrap());
        assert_eq!(out, metric_event(histogram_kind()));
    }

    #[test]
    fn new_of_to_table_round_trips_an_exponential_histogram_metric_event_whole() {
        let w = worker(REBUILD);
        let out = emitted(w.process(metric_event(exp_histogram_kind())).unwrap());
        assert_eq!(out, metric_event(exp_histogram_kind()));
    }

    #[test]
    fn new_of_to_table_round_trips_a_summary_metric_event_whole() {
        let w = worker(REBUILD);
        let out = emitted(w.process(metric_event(summary_kind())).unwrap());
        assert_eq!(out, metric_event(summary_kind()));
    }

    #[test]
    fn a_histogram_with_no_sum_min_max_and_no_buckets_rebuilds_as_none_and_empty() {
        // `sum`/`min`/`max` absent are `None`; an empty `buckets` sequence is the empty
        // histogram `to_table()` emits for one.
        let out = mint_metric(r#"kind = "histogram", buckets = {}, temporality = "delta""#);
        assert_eq!(
            out.metrics[0].kind,
            MetricKind::Histogram(Histogram {
                buckets: Vec::new(),
                temporality: Temporality::Delta,
                sum: None,
                min: None,
                max: None,
            })
        );
    }

    #[test]
    fn a_histogram_takes_explicit_sum_min_max_and_a_math_huge_bound() {
        let out = mint_metric(
            r#"kind = "histogram", buckets = {{bound = 2.5, count = 4}, {bound = math.huge, count = 1}}, temporality = "cumulative", sum = 6, min = 0.5, max = 3"#,
        );
        assert_eq!(
            out.metrics[0].kind,
            MetricKind::Histogram(Histogram {
                buckets: vec![(2.5, 4), (f64::INFINITY, 1)],
                temporality: Temporality::Cumulative,
                sum: Some(6.0),
                min: Some(0.5),
                max: Some(3.0),
            })
        );
    }

    #[test]
    fn an_exponential_histogram_with_no_sum_min_max_rebuilds_as_none() {
        let out = mint_metric(
            r#"kind = "exponential_histogram", scale = -2, zero_count = 0, zero_threshold = 0, positive = {offset = -1, counts = {}}, negative = {offset = 0, counts = {7}}, temporality = "delta", count = 7"#,
        );
        assert_eq!(
            out.metrics[0].kind,
            MetricKind::ExponentialHistogram(ExpHistogram {
                scale: -2,
                zero_count: 0,
                zero_threshold: 0.0,
                positive: (-1, Vec::new()),
                negative: (0, vec![7]),
                temporality: Temporality::Delta,
                count: 7,
                sum: None,
                min: None,
                max: None,
            })
        );
    }

    #[test]
    fn an_exponential_histogram_takes_explicit_sum_min_max() {
        let out = mint_metric(
            r#"kind = "exponential_histogram", scale = 0, zero_count = 0, zero_threshold = 0, positive = {offset = 0, counts = {}}, negative = {offset = 0, counts = {}}, temporality = "cumulative", count = 0, sum = 1.5, min = -2, max = 2"#,
        );
        let MetricKind::ExponentialHistogram(e) = &out.metrics[0].kind else {
            panic!("expected an exponential histogram")
        };
        assert_eq!((e.sum, e.min, e.max), (Some(1.5), Some(-2.0), Some(2.0)));
    }

    #[test]
    fn a_summary_quantile_outside_the_unit_interval_is_accepted_as_is() {
        // The model doesn't constrain `quantile`, so neither does the constructor.
        let out = mint_metric(
            r#"kind = "summary", quantiles = {{quantile = 1.5, value = 3}}, count = 1, sum = 3"#,
        );
        assert_eq!(
            out.metrics[0].kind,
            MetricKind::Summary(Summary { quantiles: vec![(1.5, 3.0)], count: 1, sum: 3.0 })
        );
    }

    #[test]
    fn the_pre_aggregated_kinds_are_constructible_and_name_their_first_missing_field() {
        // The W3 "is not constructible yet" arm is gone: a bare kind now gets as far as its
        // own required payload.
        for (kind, field) in
            [("histogram", "buckets"), ("exponential_histogram", "scale"), ("summary", "quantiles")]
        {
            let err = metric_err(&format!(r#"name = "m", kind = "{kind}""#));
            assert!(!err.contains("not constructible"), "got: {err}");
            assert!(
                err.contains(&format!("Event.new: metrics[1].{field} is required")),
                "got: {err}"
            );
        }
    }

    #[test]
    fn a_bucket_row_with_an_extra_key_is_not_a_field_with_its_indexed_path() {
        let err = metric_err(
            r#"name = "m", kind = "histogram", buckets = {{bound = 1, count = 1, foo = 2}}, temporality = "delta""#,
        );
        assert!(err.contains("Event.new: metrics[1].buckets[1].foo is not a field"), "got: {err}");
    }

    #[test]
    fn a_non_table_bucket_row_is_rejected() {
        let err =
            metric_err(r#"name = "m", kind = "histogram", buckets = {1}, temporality = "delta""#);
        assert!(
            err.contains("Event.new: metrics[1].buckets[1] must be a table, got integer"),
            "got: {err}"
        );
    }

    #[test]
    fn a_negative_bucket_count_is_rejected() {
        let err = metric_err(
            r#"name = "m", kind = "histogram", buckets = {{bound = 1, count = -1}}, temporality = "delta""#,
        );
        assert!(
            err.contains(
                "Event.new: metrics[1].buckets[1].count must be a non-negative integer, got -1"
            ),
            "got: {err}"
        );
    }

    #[test]
    fn a_nan_bound_is_rejected_while_math_huge_is_not() {
        let err = metric_err(
            r#"name = "m", kind = "histogram", buckets = {{bound = 0/0, count = 1}}, temporality = "delta""#,
        );
        assert!(
            err.contains(
                "Event.new: metrics[1].buckets[1].bound must be a finite number or math.huge, \
                 got NaN"
            ),
            "got: {err}"
        );
        let err = metric_err(
            r#"name = "m", kind = "histogram", buckets = {{bound = -math.huge, count = 1}}, temporality = "delta""#,
        );
        assert!(err.contains("must be a finite number or math.huge, got -inf"), "got: {err}");
        let out = mint_metric(
            r#"kind = "histogram", buckets = {{bound = math.huge, count = 1}}, temporality = "delta""#,
        );
        let MetricKind::Histogram(h) = &out.metrics[0].kind else { panic!("expected a histogram") };
        assert_eq!(h.buckets, vec![(f64::INFINITY, 1)]);
    }

    #[test]
    fn a_histogram_sum_must_still_be_finite() {
        // `math.huge` is a bound's privilege only.
        let err = metric_err(
            r#"name = "m", kind = "histogram", buckets = {}, temporality = "delta", sum = math.huge"#,
        );
        assert!(
            err.contains("Event.new: metrics[1].sum must be a finite number, got inf"),
            "got: {err}"
        );
    }

    #[test]
    fn a_histogram_without_a_temporality_is_required() {
        let err = metric_err(r#"name = "m", kind = "histogram", buckets = {}"#);
        assert!(err.contains("Event.new: metrics[1].temporality is required"), "got: {err}");
    }

    /// A required field's errors must not offer `nil` and then reject it as missing.
    #[test]
    fn a_histograms_bad_temporality_does_not_offer_nil() {
        let err =
            metric_err(r#"name = "m", kind = "histogram", buckets = {}, temporality = "total""#);
        assert!(
            err.contains(
                "Event.new: metrics[1].temporality must be one of delta, cumulative, got \"total\""
            ),
            "got: {err}"
        );
        assert!(!err.contains("or nil"), "got: {err}");
        let err = metric_err(r#"name = "m", kind = "histogram", buckets = {}, temporality = 5"#);
        assert!(
            err.contains("Event.new: metrics[1].temporality must be a string, got integer"),
            "got: {err}"
        );
        assert!(!err.contains("or nil"), "got: {err}");
    }

    #[test]
    fn an_exponential_histogram_without_a_temporality_is_required() {
        let err = metric_err(
            r#"name = "m", kind = "exponential_histogram", scale = 0, zero_count = 0, zero_threshold = 0, positive = {offset = 0, counts = {}}, negative = {offset = 0, counts = {}}, count = 0"#,
        );
        assert!(err.contains("Event.new: metrics[1].temporality is required"), "got: {err}");
    }

    #[test]
    fn an_exponential_histogram_side_without_counts_is_required_with_its_path() {
        let err = metric_err(
            r#"name = "m", kind = "exponential_histogram", scale = 0, zero_count = 0, zero_threshold = 0, positive = {offset = 0}"#,
        );
        assert!(err.contains("Event.new: metrics[1].positive.counts is required"), "got: {err}");
    }

    #[test]
    fn an_exponential_histogram_count_element_is_checked_by_index() {
        let err = metric_err(
            r#"name = "m", kind = "exponential_histogram", scale = 0, zero_count = 0, zero_threshold = 0, positive = {offset = 0, counts = {1, -2}}"#,
        );
        assert!(
            err.contains(
                "Event.new: metrics[1].positive.counts[2] must be a non-negative integer, got -2"
            ),
            "got: {err}"
        );
    }

    #[test]
    fn an_unknown_exponential_histogram_side_key_is_not_a_field() {
        let err = metric_err(
            r#"name = "m", kind = "exponential_histogram", scale = 0, zero_count = 0, zero_threshold = 0, positive = {offset = 0, counts = {}, bogus = 1}"#,
        );
        assert!(err.contains("Event.new: metrics[1].positive.bogus is not a field"), "got: {err}");
    }

    #[test]
    fn a_scale_that_does_not_fit_an_i32_is_rejected() {
        let err = metric_err(r#"name = "m", kind = "exponential_histogram", scale = 2^40"#);
        assert!(
            err.contains(
                "Event.new: metrics[1].scale must be an integer between -2147483648 and \
                 2147483647, got 1099511627776"
            ),
            "got: {err}"
        );
    }

    #[test]
    fn a_quantile_row_without_a_value_is_required_with_its_indexed_path() {
        let err = metric_err(
            r#"name = "m", kind = "summary", quantiles = {{quantile = 0.5}}, count = 1, sum = 1"#,
        );
        assert!(err.contains("Event.new: metrics[1].quantiles[1].value is required"), "got: {err}");
    }

    #[test]
    fn a_payload_key_of_a_pre_aggregated_kind_is_not_a_field_on_another() {
        // `buckets` belongs to `histogram`; on a `summary` it's unknown.
        let err = metric_err(
            r#"name = "m", kind = "summary", quantiles = {}, count = 0, sum = 0, buckets = {}"#,
        );
        assert!(err.contains("Event.new: metrics[1].buckets is not a field"), "got: {err}");
    }

    // -- span ---------------------------------------------------------------------------------

    #[test]
    fn new_of_to_table_round_trips_a_span_event_whole() {
        // Every `ext` field, `parent_span_id`, a non-default kind/status, an event and a link
        // with attributes and a `trace_state` -- all of it back, field for field.
        let w = worker(REBUILD);
        let out = emitted(w.process(span_event()).unwrap());
        assert_eq!(out, span_event());
    }

    #[test]
    fn a_minimal_span_gets_cores_documented_defaults() {
        // `kind` internal, `status` unset, `end_timestamp` the event's own `timestamp`,
        // `flags` 0, no `ext` at all, empty `events`/`links`.
        let out = mint_span("");
        assert_eq!(out, Event::span(5, AttrMap::new(), minimal_span_record()));
    }

    #[test]
    fn a_status_message_alone_boxes_ext_with_only_that_set() {
        let out = mint_span(r#"status_message = "boom""#);
        let mut expected = minimal_span_record();
        expected.ext = Some(Box::new(SpanExt {
            status_message: Some(Bytes::from_static(b"boom")),
            ..SpanExt::default()
        }));
        assert_eq!(out, Event::span(5, AttrMap::new(), expected));
    }

    #[test]
    fn an_explicit_default_dropped_count_leaves_ext_none() {
        // `ext_from_wire`'s rule: a field that is present but default doesn't earn a box.
        let out = mint_span("dropped_events_count = 0");
        assert_eq!(out.span.as_ref().unwrap().ext, None);
        assert_eq!(out, Event::span(5, AttrMap::new(), minimal_span_record()));
    }

    #[test]
    fn a_span_takes_its_parent_kind_status_end_and_flags() {
        let out = mint_span(
            r#"parent_span_id = string.rep("07", 8), kind = "client", status = "ok", end_timestamp = "9", flags = 257"#,
        );
        let span = out.span.as_ref().unwrap();
        assert_eq!(span.parent_span_id, Some([7; 8]));
        assert_eq!(span.kind, SpanKind::Client);
        assert_eq!(span.status, SpanStatus::Ok);
        assert_eq!(span.end_timestamp, 9);
        assert_eq!(span.flags, 257);
        assert_eq!(span.ext, None);
    }

    #[test]
    fn an_end_timestamp_equal_to_the_start_is_accepted() {
        // A zero-duration span is legal (`trace_context` accepts `end == start` too); only an
        // end *before* the start is rejected.
        let out = mint_span(r#"end_timestamp = "5""#);
        assert_eq!(out.span.as_ref().unwrap().end_timestamp, 5);
    }

    #[test]
    fn a_span_event_and_link_from_literals_rebuild_whole() {
        let out = mint_span(
            r#"events = {{timestamp = "6", name = "exception", attributes = {type = "Timeout"}, dropped_attributes_count = 2}}, links = {{trace_id = string.rep("04", 16), span_id = string.rep("05", 8), trace_state = "vendor=value", flags = 1, dropped_attributes_count = 1, attributes = {linked = true}}}"#,
        );
        let span = out.span.as_ref().unwrap();
        let everything = span_record_with_everything();
        assert_eq!(span.events.len(), 1);
        assert_eq!(span.events[0].timestamp, 6);
        assert_eq!(span.events[0].name, everything.events[0].name);
        assert_eq!(span.events[0].attributes, everything.events[0].attributes);
        assert_eq!(span.events[0].dropped_attributes_count, 2);
        assert_eq!(span.links, everything.links);
    }

    #[test]
    fn a_bare_span_event_gets_empty_attributes_and_a_zero_count() {
        let out = mint_span(r#"events = {{timestamp = "6", name = "tick"}}"#);
        assert_eq!(
            out.span.as_ref().unwrap().events,
            vec![SpanEvent {
                timestamp: 6,
                name: Value::str("tick"),
                attributes: AttrMap::new(),
                dropped_attributes_count: 0,
            }]
        );
    }

    #[test]
    fn a_bare_span_link_gets_no_trace_state_and_zeros() {
        let out = mint_span(
            r#"links = {{trace_id = string.rep("04", 16), span_id = string.rep("05", 8)}}"#,
        );
        assert_eq!(
            out.span.as_ref().unwrap().links,
            vec![SpanLink {
                trace_id: [4; 16],
                span_id: [5; 8],
                attributes: AttrMap::new(),
                flags: 0,
                trace_state: None,
                dropped_attributes_count: 0,
            }]
        );
    }

    #[test]
    fn new_of_to_table_round_trips_a_mixed_event_whole() {
        // A log, a gauge and a span on one event (`docs/adr/multi-payload-events.md`): the
        // three parsers compose, and the event's `timestamp` serves as the span's start.
        let mut event = log_event();
        event.metrics.push(metric_record(gauge_kind()));
        event.span = Some(span_record_with_everything());
        let w = worker(REBUILD);
        let out = emitted(w.process(event.clone()).unwrap());
        assert_eq!(out, event);
    }

    #[test]
    fn an_end_timestamp_before_the_timestamp_is_rejected() {
        let err = span_err(
            r#"trace_id = string.rep("09", 16), span_id = string.rep("08", 8), name = "x", end_timestamp = "4""#,
        );
        assert!(err.contains("Event.new: span.end_timestamp precedes timestamp"), "got: {err}");
    }

    #[test]
    fn an_all_zero_span_trace_id_is_rejected_without_offering_nil() {
        let err = span_err(
            r#"trace_id = string.rep("00", 16), span_id = string.rep("08", 8), name = "x""#,
        );
        assert!(
            err.contains(
                "Event.new: span.trace_id must be a 32-character hex string, and not all-zero"
            ),
            "got: {err}"
        );
        assert!(!err.contains("or nil"), "got: {err}");
    }

    #[test]
    fn a_bad_parent_span_id_is_rejected() {
        let err = span_err(
            r#"trace_id = string.rep("09", 16), span_id = string.rep("08", 8), name = "x", parent_span_id = "zz""#,
        );
        assert!(
            err.contains(
                "Event.new: span.parent_span_id must be a 16-character hex string (or nil), and \
                 not all-zero"
            ),
            "got: {err}"
        );
    }

    #[test]
    fn a_span_event_without_a_name_is_required_with_its_indexed_path() {
        let err = span_err(
            r#"trace_id = string.rep("09", 16), span_id = string.rep("08", 8), name = "x", events = {{timestamp = "1"}}"#,
        );
        assert!(err.contains("Event.new: span.events[1].name is required"), "got: {err}");
    }

    #[test]
    fn a_span_link_without_a_span_id_is_required_with_its_indexed_path() {
        let err = span_err(
            r#"trace_id = string.rep("09", 16), span_id = string.rep("08", 8), name = "x", links = {{trace_id = string.rep("04", 16)}}"#,
        );
        assert!(err.contains("Event.new: span.links[1].span_id is required"), "got: {err}");
    }

    #[test]
    fn an_unknown_span_link_key_is_not_a_field() {
        let err = span_err(
            r#"trace_id = string.rep("09", 16), span_id = string.rep("08", 8), name = "x", links = {{trace_id = string.rep("04", 16), span_id = string.rep("05", 8), bogus = 1}}"#,
        );
        assert!(err.contains("Event.new: span.links[1].bogus is not a field"), "got: {err}");
    }

    #[test]
    fn an_unknown_span_key_is_not_a_field() {
        // `attributes` in particular: a span has none of its own (`event.attributes` is the
        // span's), so it isn't a field here any more than on `event.span`.
        let err = span_err(
            r#"trace_id = string.rep("09", 16), span_id = string.rep("08", 8), name = "x", attributes = {}"#,
        );
        assert!(err.contains("Event.new: span.attributes is not a field"), "got: {err}");
    }

    #[test]
    fn a_span_kind_is_matched_exactly_and_lists_the_names() {
        let err = span_err(
            r#"trace_id = string.rep("09", 16), span_id = string.rep("08", 8), name = "x", kind = "SERVER""#,
        );
        assert!(
            err.contains(
                "Event.new: span.kind must be one of internal, server, client, producer, \
                 consumer (or nil), got \"SERVER\""
            ),
            "got: {err}"
        );
    }

    #[test]
    fn an_unknown_span_status_lists_the_names() {
        let err = span_err(
            r#"trace_id = string.rep("09", 16), span_id = string.rep("08", 8), name = "x", status = "failed""#,
        );
        assert!(
            err.contains(
                "Event.new: span.status must be one of unset, ok, error (or nil), got \"failed\""
            ),
            "got: {err}"
        );
    }

    #[test]
    fn an_empty_span_table_is_missing_its_trace_id() {
        // The W2 "is not constructible yet" arm is gone: a bare `span = {}` now gets as far as
        // its own first required field.
        let err = span_err("");
        assert!(!err.contains("not constructible"), "got: {err}");
        assert!(err.contains("Event.new: span.trace_id is required"), "got: {err}");
    }

    #[test]
    fn a_span_without_a_name_is_required() {
        let err = span_err(r#"trace_id = string.rep("09", 16), span_id = string.rep("08", 8)"#);
        assert!(err.contains("Event.new: span.name is required"), "got: {err}");
    }

    #[test]
    fn a_non_table_span_is_rejected() {
        let err = new_err(r#"{timestamp = "1", span = 5}"#);
        assert!(
            err.contains("Event.new: span must be a table (or nil), got integer"),
            "got: {err}"
        );
    }

    #[test]
    fn a_non_table_span_event_row_is_rejected_by_index() {
        let err = span_err(
            r#"trace_id = string.rep("09", 16), span_id = string.rep("08", 8), name = "x", events = {1}"#,
        );
        assert!(
            err.contains("Event.new: span.events[1] must be a table, got integer"),
            "got: {err}"
        );
    }

    #[test]
    fn a_non_sequence_links_table_is_rejected() {
        let err = span_err(
            r#"trace_id = string.rep("09", 16), span_id = string.rep("08", 8), name = "x", links = {[2] = {}}"#,
        );
        assert!(
            err.contains("Event.new: span.links must be a contiguous array-like table"),
            "got: {err}"
        );
    }

    #[test]
    fn a_non_string_status_message_is_rejected() {
        let err = span_err(
            r#"trace_id = string.rep("09", 16), span_id = string.rep("08", 8), name = "x", status_message = 1"#,
        );
        assert!(
            err.contains("Event.new: span.status_message must be a string or nil, got integer"),
            "got: {err}"
        );
    }
}
