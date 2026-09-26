//! `Event.new(e:to_table())` is a fixed point over generated events, through the public API only.
//!
//! The generator covers every constructible payload: logs, the seven constructible metric kinds
//! with exemplars, and spans with events and links. It leaves out the kinds the constructor
//! refuses (the two sketches and `gauge_delta`) and every shape `docs/adr/lua-event-constructor.md`
//! lists as not round-tripping: the "Residual, recorded rather than fixed" consequence and the
//! count amendment. Counts draw from the full `u64` range, boundary values first.

use bytes::Bytes;
use logit_core::interner::intern;
use logit_core::{
    AttrMap, BodyFormat, Event, Exemplar, ExpHistogram, Histogram, LogRecord, MetricKind,
    MetricList, MetricRecord, Samples, Severity, SpanEvent, SpanExt, SpanKind, SpanLink,
    SpanRecord, SpanStatus, Sum, Summary, Temporality, TraceRef, Value,
};
use logit_script::{ProcessOutcome, ScriptWorker};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};

/// The largest integer a Lua number holds without rounding; an `I64` attribute past it reads back
/// `Str`.
const MAX_EXACT: i64 = 1 << 53;

/// A finite number a script reads back unchanged. mlua 0.9.9's LuaJIT conversion
/// (`Lua::pop_value`/`stack_value`) truncates toward zero with `num_traits::cast` and keeps the
/// integer when `(n - i as f64).abs() < f64::EPSILON`, so only `0 < |x| < 2^-52` (and `-0.0`)
/// collapse to `0`; `1 - 2^-53` doesn't. The generator excludes those by construction, shrinking
/// included: every value is mapped through [`outside_the_collapse`].
fn finite() -> impl Strategy<Value = f64> {
    prop_oneof![Just(0.0), -1000.0f64..1000.0, prop::num::f64::NORMAL]
        .prop_map(outside_the_collapse)
}

/// `-0.0` becomes `0.0`, and a nonzero magnitude below `f64::EPSILON` (2^-52) becomes `±1.0`.
/// Not the reciprocal: a subnormal's reciprocal is infinite.
fn outside_the_collapse(f: f64) -> f64 {
    if f == 0.0 {
        0.0
    } else if f.abs() < f64::EPSILON {
        f.signum()
    } else {
        f
    }
}

/// A count, boundary values first: 2^53 is the last one a Lua number holds, `i64::MAX + 1` the
/// first a signed cast turns negative.
fn count() -> impl Strategy<Value = u64> {
    prop_oneof![
        Just(0u64),
        Just(1u64 << 53),
        Just((1u64 << 53) + 1),
        Just(i64::MAX as u64),
        Just(i64::MAX as u64 + 1),
        Just(u64::MAX),
        any::<u64>(),
        0u64..1000,
    ]
}

fn symbol() -> impl Strategy<Value = logit_core::Symbol> {
    "[a-z][a-z0-9_.]{0,7}".prop_map(|s| intern(&s))
}

fn key() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9_.]{0,8}"
}

/// A Lua string that isn't UTF-8, the one `Bytes` a Lua string can carry back.
fn non_utf8_bytes() -> impl Strategy<Value = Bytes> {
    prop::collection::vec(any::<u8>(), 1..8)
        .prop_map(|mut b| {
            b.push(0xff);
            b
        })
        .prop_map(Bytes::from)
}

fn leaf_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        any::<bool>().prop_map(Value::Bool),
        (-MAX_EXACT..=MAX_EXACT).prop_map(Value::I64),
        (-1e12f64..1e12)
            .prop_filter("fractional", |f| f.fract() != 0.0 && f.abs() >= f64::EPSILON)
            .prop_map(Value::F64),
        "\\PC{0,8}".prop_map(Value::str),
        non_utf8_bytes().prop_map(Value::Bytes),
    ]
}

fn value() -> impl Strategy<Value = Value> {
    leaf_value().prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 1..4).prop_map(Value::Array),
            prop::collection::vec((key(), inner), 0..4).prop_map(|pairs| {
                let mut map = AttrMap::new();
                for (k, v) in pairs {
                    map.insert(&k, v);
                }
                Value::Map(Box::new(map))
            }),
        ]
    })
}

fn attributes() -> impl Strategy<Value = AttrMap> {
    prop::collection::vec((key(), value()), 0..4).prop_map(|pairs| {
        let mut map = AttrMap::new();
        for (k, v) in pairs {
            map.insert(&k, v);
        }
        map
    })
}

fn trace_id() -> impl Strategy<Value = [u8; 16]> {
    any::<[u8; 16]>().prop_filter("not all-zero", |id| id.iter().any(|b| *b != 0))
}

fn span_id() -> impl Strategy<Value = [u8; 8]> {
    any::<[u8; 8]>().prop_filter("not all-zero", |id| id.iter().any(|b| *b != 0))
}

fn trace_ref() -> impl Strategy<Value = TraceRef> {
    (trace_id(), prop::option::of(span_id()), any::<u8>())
        .prop_map(|(trace_id, span_id, flags)| TraceRef { trace_id, span_id, flags })
}

fn temporality() -> impl Strategy<Value = Temporality> {
    prop::sample::select(Temporality::NAMES.to_vec())
        .prop_map(|n| Temporality::from_name(n).expect("a listed name"))
}

fn log_record() -> impl Strategy<Value = LogRecord> {
    (
        value(),
        prop::option::of(
            prop::sample::select(Severity::NAMES.to_vec())
                .prop_map(|n| Severity::from_name(n).expect("a listed name")),
        ),
        prop::sample::select(BodyFormat::NAMES.to_vec())
            .prop_map(|n| BodyFormat::from_name(n).expect("a listed name")),
        prop::option::of(trace_ref()),
        prop::option::of(symbol()),
        any::<i64>(),
        any::<u32>(),
    )
        .prop_map(
            |(
                message,
                severity,
                body_format,
                trace,
                event_name,
                observed_timestamp,
                dropped_attributes_count,
            )| LogRecord {
                message,
                severity,
                body_format,
                trace,
                event_name,
                observed_timestamp,
                dropped_attributes_count,
            },
        )
}

fn histogram() -> impl Strategy<Value = Histogram> {
    (
        prop::collection::vec((finite(), count()), 0..4),
        any::<bool>(),
        count(),
        temporality(),
        prop::option::of(finite()),
        prop::option::of(finite()),
        prop::option::of(finite()),
    )
        .prop_map(
            |(mut finite_buckets, with_overflow, overflow, temporality, sum, min, max)| {
                // `Event.new` needs strictly increasing bounds and appends a `+Inf` bucket to a
                // finite-tailed list, so a generated list that must be a fixed point carries one.
                finite_buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
                finite_buckets.dedup_by(|a, b| a.0 == b.0);
                let mut buckets = finite_buckets;
                if with_overflow || !buckets.is_empty() {
                    buckets.push((f64::INFINITY, overflow));
                }
                Histogram { buckets, temporality, sum, min, max }
            },
        )
}

fn exp_histogram() -> impl Strategy<Value = ExpHistogram> {
    (
        -10i32..=20,
        count(),
        finite(),
        (any::<i32>(), prop::collection::vec(count(), 0..4)),
        (any::<i32>(), prop::collection::vec(count(), 0..4)),
        temporality(),
        count(),
        prop::option::of(finite()),
        prop::option::of(finite()),
        prop::option::of(finite()),
    )
        .prop_map(
            |(
                scale,
                zero_count,
                zero_threshold,
                positive,
                negative,
                temporality,
                count,
                sum,
                min,
                max,
            )| {
                ExpHistogram {
                    scale,
                    zero_count,
                    zero_threshold,
                    positive,
                    negative,
                    temporality,
                    count,
                    sum,
                    min,
                    max,
                }
            },
        )
}

fn metric_kind() -> impl Strategy<Value = MetricKind> {
    prop_oneof![
        (finite(), temporality(), any::<bool>()).prop_map(|(value, temporality, monotonic)| {
            MetricKind::Sum(Sum { value, temporality, monotonic })
        }),
        finite().prop_map(MetricKind::Gauge),
        (prop::collection::vec(finite(), 0..5), finite()).prop_map(|(values, sample_rate)| {
            let mut samples = Samples::new(values);
            samples.sample_rate = sample_rate;
            MetricKind::Samples(samples)
        }),
        prop::collection::vec(prop::collection::vec(any::<u8>(), 0..6), 0..4).prop_map(|members| {
            MetricKind::SetMembers(members.into_iter().map(Bytes::from).collect())
        }),
        histogram().prop_map(MetricKind::Histogram),
        exp_histogram().prop_map(MetricKind::ExponentialHistogram),
        (prop::collection::vec((0.0f64..=1.0, finite()), 0..4), count(), finite()).prop_map(
            |(quantiles, count, sum)| MetricKind::Summary(Summary { quantiles, count, sum })
        ),
    ]
}

fn exemplar() -> impl Strategy<Value = Exemplar> {
    (any::<i64>(), finite(), prop::option::of(trace_ref()), attributes()).prop_map(
        |(timestamp, value, trace, filtered_attributes)| Exemplar {
            timestamp,
            value,
            trace,
            filtered_attributes,
        },
    )
}

fn metric_record() -> impl Strategy<Value = MetricRecord> {
    (
        symbol(),
        prop::option::of(symbol()),
        prop::option::of(symbol()),
        any::<i64>(),
        prop::collection::vec(exemplar(), 0..3),
        any::<u32>(),
        metric_kind(),
    )
        .prop_map(|(name, unit, description, start_timestamp, exemplars, flags, kind)| {
            MetricRecord { name, unit, description, start_timestamp, exemplars, flags, kind }
        })
}

fn span_event() -> impl Strategy<Value = SpanEvent> {
    (any::<i64>(), value(), attributes(), any::<u32>()).prop_map(
        |(timestamp, name, attributes, dropped_attributes_count)| SpanEvent {
            timestamp,
            name,
            attributes,
            dropped_attributes_count,
        },
    )
}

fn span_link() -> impl Strategy<Value = SpanLink> {
    (
        trace_id(),
        span_id(),
        attributes(),
        any::<u32>(),
        prop::option::of(non_utf8_bytes()),
        any::<u32>(),
    )
        .prop_map(
            |(trace_id, span_id, attributes, flags, trace_state, dropped_attributes_count)| {
                SpanLink {
                    trace_id,
                    span_id,
                    attributes,
                    flags,
                    trace_state,
                    dropped_attributes_count,
                }
            },
        )
}

fn span_ext() -> impl Strategy<Value = Option<Box<SpanExt>>> {
    (
        prop::option::of("\\PC{0,8}".prop_map(Bytes::from)),
        prop::option::of("\\PC{0,8}".prop_map(Bytes::from)),
        any::<u32>(),
        any::<u32>(),
        any::<u32>(),
    )
        .prop_map(
            |(status_message, trace_state, dropped_attrs, dropped_events, dropped_links)| {
                let ext = SpanExt {
                    status_message,
                    trace_state,
                    dropped_attributes_count: dropped_attrs,
                    dropped_events_count: dropped_events,
                    dropped_links_count: dropped_links,
                };
                // `Event.new` boxes `ext` only when a field is non-default, as the decoders do.
                (ext != SpanExt::default()).then(|| Box::new(ext))
            },
        )
}

/// A span for an event at `timestamp`; `Event.new` rejects an end before it.
fn span_record(timestamp: i64) -> impl Strategy<Value = SpanRecord> {
    (
        (trace_id(), span_id(), prop::option::of(span_id()), value()),
        prop::sample::select(SpanKind::NAMES.to_vec())
            .prop_map(|n| SpanKind::from_name(n).expect("a listed name")),
        prop::sample::select(SpanStatus::NAMES.to_vec())
            .prop_map(|n| SpanStatus::from_name(n).expect("a listed name")),
        prop::collection::vec(span_event(), 0..3),
        prop::collection::vec(span_link(), 0..3),
        any::<u32>(),
        any::<u32>(),
        span_ext(),
    )
        .prop_map(
            move |(
                (trace_id, span_id, parent_span_id, name),
                kind,
                status,
                events,
                links,
                dur,
                flags,
                ext,
            )| {
                SpanRecord {
                    trace_id,
                    span_id,
                    parent_span_id,
                    name,
                    kind,
                    status,
                    events,
                    links,
                    end_timestamp: timestamp.saturating_add(i64::from(dur)),
                    flags,
                    ext,
                }
            },
        )
}

fn event() -> impl Strategy<Value = Event> {
    any::<i64>().prop_flat_map(|timestamp| {
        (
            attributes(),
            prop::option::of(log_record()),
            prop::collection::vec(metric_record(), 0..3),
            prop::option::of(span_record(timestamp)),
        )
            .prop_map(move |(attributes, log, metrics, span)| Event {
                timestamp,
                attributes,
                log,
                metrics: MetricList::from_vec(metrics),
                span,
            })
    })
}

/// Runs `script`'s `process` over generated events and checks each comes back unchanged.
fn assert_fixed_point(script: &str) {
    let worker = ScriptWorker::new(script).expect("script should load");
    let mut runner = TestRunner::new(Config::with_cases(512));
    runner
        .run(&event(), |event| {
            let out = match worker.process(event.clone()) {
                Ok(ProcessOutcome::Emit(out, _)) => *out,
                Ok(_) => return Err(TestCaseError::fail("expected one emitted event")),
                Err(err) => return Err(TestCaseError::fail(format!("{err}"))),
            };
            prop_assert_eq!(out, event);
            Ok(())
        })
        .unwrap();
}

#[test]
fn event_new_of_to_table_is_a_fixed_point_over_generated_events() {
    assert_fixed_point("function process(e) return Event.new(e:to_table()) end");
}

#[test]
fn a_fixed_point_event_survives_two_round_trips() {
    assert_fixed_point(
        "function process(e) return Event.new(Event.new(e:to_table()):to_table()) end",
    );
}
