//! Built-in native transform components -- no Lua VM involved, per `docs/design/lua-api.md`'s
//! "built-in native processors ... meant to sit in front of user Lua" split. Each implements
//! `logit_pipeline::Transform`, letting the node runtime run it as an ordinary tokio task (no
//! dedicated OS thread, unlike a Lua component -- `docs/design/pipeline-graph.md`'s "Node kinds"
//! section). `aggregate`, `json`, `csv`, `kv_metrics`, `keep`, `remove`, `set`, `trace_context`,
//! `scale`, `has_signal`, `keep_signals`, `drop_signals`, `has_attributes`, `drop_attributes`,
//! `has_provenance`, `drop_provenance`, `logfmt`, `kv`, and `regex` are implemented
//! (`rename`/`filter`/`sample`/`throttle`/`dedup` were retired rather than landing --
//! `docs/adr/routing-by-condition-is-lua.md`).

mod aggregate;
mod attributes;
mod csv;
mod json;
mod keep;
mod kv_metrics;
mod logfmt;
mod provenance;
mod regex;
mod scale;
mod set;
mod signals;
mod trace_context;

use logit_core::Value;

pub use aggregate::Aggregator;
pub use attributes::{DropAttributes, HasAttributes};
pub use csv::CsvParser;
pub use json::JsonParser;
pub use keep::{Keep, Remove};
pub use kv_metrics::{KvMetrics, MetricSpec};
pub use logfmt::{Kv, Logfmt};
pub use provenance::{DropProvenance, HasProvenance};
pub use regex::RegexParser;
pub use scale::Scale;
pub use set::Set;
pub use signals::{DropSignals, HasSignal, KeepSignals, MatchMode, SignalSet};
pub use trace_context::{SpanLift, TraceContext};

/// Coerces a `Value` to a finite `f64`: `I64`/`U64`/`F64` directly, or a `Str` that parses
/// cleanly to a finite `f64` (so it works whether the source JSON quoted the value or not).
/// `Bool`, `Null`, `Bytes`, `Timestamp`, `Array`, and `Map` never coerce. Deliberately *not* a
/// general `Value::as_f64` on `logit-core`: a general method that silently parses strings would be
/// a surprising API for every other caller of `Value` (`docs/adr/kv-metrics-semantics.md`), so
/// this stays `pub(crate)` here, shared by `kv_metrics` and `scale` -- the two transforms that
/// read a numeric attribute off an event.
pub(crate) fn numeric(value: &Value) -> Option<f64> {
    let v = match value {
        Value::I64(n) => *n as f64,
        Value::U64(n) => *n as f64,
        Value::F64(n) => *n,
        Value::Str(_) => value.as_str().and_then(|s| s.parse::<f64>().ok())?,
        _ => return None,
    };
    v.is_finite().then_some(v)
}

/// Whether an event's (or resource's) `Value` matches an operator-configured one. Coercing,
/// modelled on [`numeric`] and `logit-script`'s `lua_value_matches`, deliberately *not* on
/// `aggregate`'s `value_key_eq` -- that one is a *keying* equality (variant-exact, `f64` by bit
/// pattern), correct for hash-map identity and wrong here: config `status: 200` must match a
/// `Value::Str("200")` off a logfmt line just as it matches a `Value::I64(200)` off JSON, per
/// ADR `kv-metrics-semantics`' identity commitment.
///
/// **Symmetric** in its arguments -- every arm below is -- so a caller can't get the order wrong.
/// **Total**: never panics, never allocates, never fails; anything it can't compare is `false`.
///
/// Three rules worth stating plainly, because each is a deliberate asymmetry with a precedent
/// elsewhere in this module rather than an oversight:
/// - **String-to-string is never coerced numerically.** `Str("01")` and `Str("1")` do not match --
///   id-shaped tags are routinely numeric-looking, and treating them as numbers would be a
///   surprise. Coercion only happens *across* a numeric variant and a string.
/// - **`Bool` never coerces to anything else, including a string.** A `logfmt` line's
///   `sampled=true` is `Value::Str("true")` (logfmt always emits `Str`), so it will not match a
///   configured `sampled: true` -- write `sampled: "true"` instead. Both models this function
///   follows (`numeric`, `lua_value_matches`) refuse bool coercion, and there is no demand for it.
/// - **A non-finite configured or actual value matches nothing**, inherited from `numeric`'s
///   `is_finite` filter -- see `docs/adr/attribute-filtering-components.md`, rule 36's finiteness
///   check exists precisely because of this.
///
/// One divergence from `lua_value_matches` worth flagging so nobody "fixes" it later:
/// `Timestamp` never compares equal to a bare number here, even though the Lua bridge lets an
/// integer match one -- filtering on an exact nanosecond value is not a tag-shaped use case, and
/// `numeric` (which this delegates to for the general numeric case) already excludes `Timestamp`.
pub(crate) fn value_matches(configured: &Value, actual: &Value) -> bool {
    match (configured, actual) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
        (Value::Str(a) | Value::Bytes(a), Value::Str(b) | Value::Bytes(b)) => a == b,
        (Value::I64(a), Value::I64(b)) => a == b,
        (Value::U64(a), Value::U64(b)) => a == b,
        (Value::I64(a), Value::U64(b)) | (Value::U64(b), Value::I64(a)) => {
            *a >= 0 && *a as u64 == *b
        }
        (Value::Bool(_), _) | (_, Value::Bool(_)) => false,
        (Value::Timestamp(_), _) | (_, Value::Timestamp(_)) => false,
        (Value::I64(_) | Value::U64(_) | Value::F64(_) | Value::Str(_), _)
            if matches!(actual, Value::I64(_) | Value::U64(_) | Value::F64(_) | Value::Str(_)) =>
        {
            matches!((numeric(configured), numeric(actual)), (Some(a), Some(b)) if a == b)
        }
        _ => false,
    }
}

#[cfg(test)]
mod value_matches_tests {
    use super::*;

    #[test]
    fn bool_matches_bool_by_equality() {
        assert!(value_matches(&Value::Bool(true), &Value::Bool(true)));
        assert!(!value_matches(&Value::Bool(true), &Value::Bool(false)));
    }

    #[test]
    fn a_bool_never_matches_a_string() {
        assert!(!value_matches(&Value::Bool(true), &Value::str("true")));
        assert!(!value_matches(&Value::str("true"), &Value::Bool(true)));
    }

    #[test]
    fn a_bool_never_matches_a_number() {
        assert!(!value_matches(&Value::Bool(true), &Value::I64(1)));
    }

    #[test]
    fn exact_integer_variants_match_by_equality() {
        assert!(value_matches(&Value::I64(200), &Value::I64(200)));
        assert!(value_matches(&Value::U64(200), &Value::U64(200)));
        assert!(!value_matches(&Value::I64(200), &Value::I64(201)));
    }

    #[test]
    fn i64_and_u64_compare_exactly_across_variants() {
        assert!(value_matches(&Value::I64(200), &Value::U64(200)));
        assert!(value_matches(&Value::U64(200), &Value::I64(200)));
        assert!(
            !value_matches(&Value::I64(-1), &Value::U64(u64::MAX)),
            "a negative I64 never matches a U64"
        );
    }

    #[test]
    fn numeric_variants_coerce_across_i64_u64_f64_and_str() {
        let hundred = [Value::I64(200), Value::U64(200), Value::F64(200.0), Value::str("200")];
        for configured in &hundred {
            for actual in &hundred {
                assert!(
                    value_matches(configured, actual),
                    "{configured:?} should match {actual:?}"
                );
            }
        }
    }

    #[test]
    fn two_numeric_looking_strings_are_not_coerced() {
        assert!(
            !value_matches(&Value::str("01"), &Value::str("1")),
            "id-shaped strings must not be numerically coerced against each other"
        );
        assert!(
            value_matches(&Value::str("01"), &Value::str("01")),
            "exact byte match still works"
        );
    }

    #[test]
    fn bytes_and_str_compare_alike() {
        assert!(value_matches(
            &Value::str("web-1"),
            &Value::Bytes(bytes::Bytes::from_static(b"web-1"))
        ));
        assert!(value_matches(
            &Value::Bytes(bytes::Bytes::from_static(b"web-1")),
            &Value::str("web-1")
        ));
    }

    #[test]
    fn timestamp_matches_timestamp_but_never_a_bare_number() {
        assert!(value_matches(&Value::Timestamp(5), &Value::Timestamp(5)));
        assert!(!value_matches(&Value::Timestamp(5), &Value::I64(5)));
        assert!(!value_matches(&Value::I64(5), &Value::Timestamp(5)));
    }

    #[test]
    fn a_non_finite_value_matches_nothing() {
        assert!(!value_matches(&Value::F64(f64::NAN), &Value::F64(f64::NAN)));
        assert!(!value_matches(&Value::F64(f64::INFINITY), &Value::F64(f64::INFINITY)));
        assert!(!value_matches(&Value::F64(f64::INFINITY), &Value::I64(1)));
    }

    #[test]
    fn null_matches_only_null() {
        assert!(value_matches(&Value::Null, &Value::Null));
        assert!(!value_matches(&Value::Null, &Value::str("")));
    }

    #[test]
    fn array_and_map_never_match_anything() {
        let arr = Value::Array(vec![Value::I64(1)]);
        assert!(!value_matches(&arr, &arr));
        let map = Value::Map(Box::new(logit_core::AttrMap::new()));
        assert!(!value_matches(&map, &map));
    }

    #[test]
    fn value_matches_is_symmetric() {
        let values = [
            Value::Null,
            Value::Bool(true),
            Value::Bool(false),
            Value::I64(200),
            Value::U64(200),
            Value::F64(200.0),
            Value::str("200"),
            Value::Bytes(bytes::Bytes::from_static(b"200")),
            Value::Timestamp(200),
            Value::Array(vec![Value::I64(1)]),
        ];
        for a in &values {
            for b in &values {
                assert_eq!(
                    value_matches(a, b),
                    value_matches(b, a),
                    "value_matches must be symmetric for {a:?} vs {b:?}"
                );
            }
        }
    }
}

/// Integration coverage across module boundaries -- each transform above is unit-tested in its
/// own module; this proves they compose the way a real pipeline actually wires them.
#[cfg(test)]
mod chained_pipeline_test {
    use super::*;
    use logit_core::interner::resolve;
    use logit_core::{AttrMap, BodyFormat, Event, LogRecord, MetricKind, Resource, Value};
    use logit_pipeline::Transform;
    use std::sync::Arc;
    use std::time::Duration;

    /// The workstream's headline test: a `json -> scale -> kv_metrics -> keep -> aggregate` chain
    /// fed one synthetic nginx-shaped log event produces correctly-tagged counter/gauge/
    /// distribution metrics and nothing else -- specifically, that the tags surviving into
    /// `aggregate`'s `SeriesKey` are exactly what `keep` named, and that `scale`'s unit
    /// conversion (seconds -> milliseconds, matching `demo/logit.yaml`'s `nginx_scale`) has
    /// already happened by the time `kv_metrics` reads `request_time`. This is what proves
    /// `keep`'s documented placement ahead of `aggregate` (`crate::keep`'s module doc comment,
    /// `docs/adr/kv-metrics-semantics.md`) actually bounds series cardinality end to end,
    /// not just in isolation.
    #[test]
    fn json_scale_kv_metrics_keep_aggregate_chain_produces_correctly_tagged_metrics() {
        let resource = Arc::new(Resource::default());

        let raw = r#"{"status":200,"body_bytes_sent":512,"request_time":0.012,
                       "client_ip":"10.0.0.1","user_agent":"curl/8.0"}"#;
        let event = Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::str(raw),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );

        // json: the raw body becomes attributes.
        let mut json = JsonParser::new(false);
        let event = json.process(&resource, event).expect("json always forwards");
        assert_eq!(event.attributes.len(), 5, "every top-level JSON key should have landed");

        // scale: request_time converts from seconds to milliseconds before kv_metrics ever
        // reads it.
        let mut scale = Scale::new(vec![("request_time".to_string(), 1000.0)]);
        let event = scale.process(&resource, event).expect("scale always forwards");
        assert_eq!(event.attributes.get("request_time"), Some(&Value::F64(12.0)));

        // kv_metrics: two counters (one no-field, one field-backed) and a distribution.
        let mut kv = KvMetrics::new(
            vec![
                MetricSpec { name: "nginx.requests".to_string(), field: None, unit: None },
                MetricSpec {
                    name: "nginx.bytes_sent".to_string(),
                    field: Some("body_bytes_sent".to_string()),
                    unit: None,
                },
            ],
            vec![],
            vec![MetricSpec {
                name: "nginx.request_time".to_string(),
                field: Some("request_time".to_string()),
                unit: Some("ms".to_string()),
            }],
        );
        let event = kv.process(&resource, event).expect("kv_metrics always forwards");
        assert_eq!(event.metrics.len(), 3, "two counters and one distribution should be derived");

        // keep: only `status` is allowed to survive as a tag -- client_ip/user_agent (and the
        // now-redundant body_bytes_sent/request_time) must not reach aggregate.
        let mut keep = Keep::new(vec!["status".to_string()]);
        let event = keep.process(&resource, event).expect("keep always forwards");
        let kept: Vec<&str> = event.attributes.iter().map(|(k, _)| resolve(k)).collect();
        assert_eq!(kept, vec!["status"], "only the kept attribute should survive");

        // aggregate: every metric here is mergeable, so it's fully absorbed -- the log half
        // (still present) is forwarded on its own as the remainder.
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let passed = agg.process(&resource, event).expect("the log half should be forwarded");
        assert!(passed.metrics.is_empty(), "every metric should have been absorbed");
        assert_eq!(passed.log.as_ref().unwrap().message, Value::str(raw));

        let flushed = agg.flush(1_000_000_000);
        assert_eq!(flushed.len(), 1, "one resource group");
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 3, "three distinct series -- nothing else");

        for (series_event, _links) in events {
            let tags: Vec<&str> = series_event.attributes.iter().map(|(k, _)| resolve(k)).collect();
            assert_eq!(
                tags,
                vec!["status"],
                "every series' tags must be exactly what keep named, no more and no less"
            );
            assert_eq!(series_event.metrics.len(), 1);

            let record = &series_event.metrics[0];
            match resolve(record.name) {
                "nginx.requests" => {
                    assert!(
                        matches!(record.kind, MetricKind::Sum(logit_core::Sum { value: v, .. }) if v == 1.0)
                    );
                }
                "nginx.bytes_sent" => {
                    assert!(
                        matches!(record.kind, MetricKind::Sum(logit_core::Sum { value: v, .. }) if v == 512.0)
                    );
                }
                "nginx.request_time" => match &record.kind {
                    MetricKind::Distribution(sketch) => {
                        assert_eq!(sketch.count(), 1);
                        let q = sketch.quantile(0.5).expect("single-sample sketch has a median");
                        assert!((q - 12.0).abs() < 0.1, "got {q}, scale should have run first");
                        assert_eq!(record.unit.map(resolve), Some("ms"));
                    }
                    other => panic!("expected Distribution, got {other:?}"),
                },
                other => panic!("unexpected series name: {other}"),
            }
        }
    }

    /// The `logfmt` mirror of [`json_scale_kv_metrics_keep_aggregate_chain_produces_correctly_tagged_metrics`]:
    /// same chain, fed a logfmt-shaped line instead of JSON, proving `logfmt`'s always-`Value::Str`
    /// output (`request_time="0.012"`, never a number) still flows correctly through `scale` ->
    /// `Value::F64(12.0)` -> `kv_metrics`'s `numeric` coercion, exactly as `crate::numeric`'s own
    /// doc comment promises.
    #[test]
    fn logfmt_scale_kv_metrics_keep_aggregate_chain_produces_correctly_tagged_metrics() {
        let resource = Arc::new(Resource::default());

        let raw = "status=200 body_bytes_sent=512 request_time=0.012 client_ip=10.0.0.1 \
                    user_agent=curl/8.0";
        let event = Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::str(raw),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );

        // logfmt: the raw body becomes attributes, always as Value::Str.
        let mut logfmt = Logfmt::new(false);
        let event = logfmt.process(&resource, event).expect("logfmt always forwards");
        assert_eq!(event.attributes.len(), 5, "every logfmt field should have landed");
        assert_eq!(event.attributes.get("status"), Some(&Value::str("200")), "never coerced");

        // scale: request_time converts from seconds to milliseconds before kv_metrics ever
        // reads it -- `numeric` parses logfmt's Value::Str("0.012") just fine.
        let mut scale = Scale::new(vec![("request_time".to_string(), 1000.0)]);
        let event = scale.process(&resource, event).expect("scale always forwards");
        assert_eq!(event.attributes.get("request_time"), Some(&Value::F64(12.0)));

        // kv_metrics: two counters (one no-field, one field-backed) and a distribution.
        let mut kv = KvMetrics::new(
            vec![
                MetricSpec { name: "nginx.requests".to_string(), field: None, unit: None },
                MetricSpec {
                    name: "nginx.bytes_sent".to_string(),
                    field: Some("body_bytes_sent".to_string()),
                    unit: None,
                },
            ],
            vec![],
            vec![MetricSpec {
                name: "nginx.request_time".to_string(),
                field: Some("request_time".to_string()),
                unit: Some("ms".to_string()),
            }],
        );
        let event = kv.process(&resource, event).expect("kv_metrics always forwards");
        assert_eq!(event.metrics.len(), 3, "two counters and one distribution should be derived");

        // keep: only `status` is allowed to survive as a tag.
        let mut keep = Keep::new(vec!["status".to_string()]);
        let event = keep.process(&resource, event).expect("keep always forwards");
        let kept: Vec<&str> = event.attributes.iter().map(|(k, _)| resolve(k)).collect();
        assert_eq!(kept, vec!["status"], "only the kept attribute should survive");

        // aggregate: every metric here is mergeable, so it's fully absorbed.
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let passed = agg.process(&resource, event).expect("the log half should be forwarded");
        assert!(passed.metrics.is_empty(), "every metric should have been absorbed");
        assert_eq!(passed.log.as_ref().unwrap().message, Value::str(raw));

        let flushed = agg.flush(1_000_000_000);
        assert_eq!(flushed.len(), 1, "one resource group");
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 3, "three distinct series -- nothing else");

        for (series_event, _links) in events {
            let tags: Vec<&str> = series_event.attributes.iter().map(|(k, _)| resolve(k)).collect();
            assert_eq!(
                tags,
                vec!["status"],
                "every series' tags must be exactly what keep named, no more and no less"
            );
            assert_eq!(series_event.metrics.len(), 1);

            let record = &series_event.metrics[0];
            match resolve(record.name) {
                "nginx.requests" => {
                    assert!(
                        matches!(record.kind, MetricKind::Sum(logit_core::Sum { value: v, .. }) if v == 1.0)
                    );
                }
                "nginx.bytes_sent" => {
                    assert!(
                        matches!(record.kind, MetricKind::Sum(logit_core::Sum { value: v, .. }) if v == 512.0)
                    );
                }
                "nginx.request_time" => match &record.kind {
                    MetricKind::Distribution(sketch) => {
                        assert_eq!(sketch.count(), 1);
                        let q = sketch.quantile(0.5).expect("single-sample sketch has a median");
                        assert!((q - 12.0).abs() < 0.1, "got {q}, scale should have run first");
                        assert_eq!(record.unit.map(resolve), Some("ms"));
                    }
                    other => panic!("expected Distribution, got {other:?}"),
                },
                other => panic!("unexpected series name: {other}"),
            }
        }
    }

    /// The workstream B (Loki-direct) shape from `docs/plans/otlp-logs-and-resource-identity.md`:
    /// a `json -> kv_metrics -> keep_signals[logs]` chain proves the derived metrics are stripped
    /// off before a logs-only sink would see the event, while the log body itself survives
    /// untouched -- `keep_signals` mutates the payload, unlike `has_signal`.
    #[test]
    fn json_kv_metrics_keep_signals_chain_strips_derived_metrics_and_keeps_the_log() {
        let resource = Arc::new(Resource::default());

        let raw = r#"{"status":200,"body_bytes_sent":512}"#;
        let event = Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::str(raw),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );

        let mut json = JsonParser::new(false);
        let event = json.process(&resource, event).expect("json always forwards");

        let mut kv = KvMetrics::new(
            vec![MetricSpec {
                name: "nginx.bytes_sent".to_string(),
                field: Some("body_bytes_sent".to_string()),
                unit: None,
            }],
            vec![],
            vec![],
        );
        let event = kv.process(&resource, event).expect("kv_metrics always forwards");
        assert_eq!(event.metrics.len(), 1, "one derived counter");

        let mut keep_logs =
            KeepSignals::new(SignalSet { logs: true, metrics: false, traces: false });
        let event = keep_logs.process(&resource, event).expect("the log half survives");
        assert!(
            event.metrics.is_empty(),
            "derived metrics must be stripped before a logs-only sink"
        );
        assert_eq!(
            event.log.as_ref().unwrap().message,
            Value::str(raw),
            "the log body is untouched"
        );
    }

    /// Proves `csv`'s all-`Str` output (`docs/adr/csv-positional-columns.md`) feeds
    /// `kv_metrics`/`scale` correctly through `numeric`'s string branch -- exactly the same
    /// coercion `json`'s ADR relies on `numeric` for, but starting from a value that was *never*
    /// anything but a string, unlike JSON's own numeric syntax.
    #[test]
    fn csv_kv_metrics_keep_aggregate_chain_produces_correctly_tagged_metrics() {
        let resource = Arc::new(Resource::default());

        let event = Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::str("10.0.0.1,GET,200,612,0.012"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );

        let mut csv = CsvParser::new(
            vec![
                "client_ip".to_string(),
                "request_method".to_string(),
                "status".to_string(),
                "body_bytes_sent".to_string(),
                "request_time".to_string(),
            ],
            b',',
        );
        let event = csv.process(&resource, event).expect("csv always forwards");
        for (key, expected) in [
            ("client_ip", "10.0.0.1"),
            ("request_method", "GET"),
            ("status", "200"),
            ("body_bytes_sent", "612"),
            ("request_time", "0.012"),
        ] {
            assert_eq!(
                event.attributes.get(key),
                Some(&Value::str(expected)),
                "every csv field should be a Value::Str"
            );
        }

        let mut scale = Scale::new(vec![("request_time".to_string(), 1000.0)]);
        let event = scale.process(&resource, event).expect("scale always forwards");
        assert_eq!(event.attributes.get("request_time"), Some(&Value::F64(12.0)));

        let mut kv = KvMetrics::new(
            vec![MetricSpec {
                name: "nginx.bytes_sent".to_string(),
                field: Some("body_bytes_sent".to_string()),
                unit: None,
            }],
            vec![],
            vec![MetricSpec {
                name: "nginx.request_time".to_string(),
                field: Some("request_time".to_string()),
                unit: Some("ms".to_string()),
            }],
        );
        let event = kv.process(&resource, event).expect("kv_metrics always forwards");
        assert_eq!(event.metrics.len(), 2, "one counter (from a string) and one distribution");

        let mut keep = Keep::new(vec!["status".to_string()]);
        let event = keep.process(&resource, event).expect("keep always forwards");
        let kept: Vec<&str> = event.attributes.iter().map(|(k, _)| resolve(k)).collect();
        assert_eq!(kept, vec!["status"], "only the kept attribute should survive");

        let mut agg = Aggregator::new(Duration::from_secs(10));
        let passed = agg.process(&resource, event).expect("the log half should be forwarded");
        assert!(passed.metrics.is_empty(), "every metric should have been absorbed");

        let flushed = agg.flush(1_000_000_000);
        assert_eq!(flushed.len(), 1, "one resource group");
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 2, "two distinct series");
        for (series_event, _links) in events {
            let tags: Vec<&str> = series_event.attributes.iter().map(|(k, _)| resolve(k)).collect();
            assert_eq!(tags, vec!["status"]);
            let record = &series_event.metrics[0];
            match resolve(record.name) {
                "nginx.bytes_sent" => {
                    assert!(
                        matches!(record.kind, MetricKind::Sum(logit_core::Sum { value: v, .. }) if v == 612.0)
                    );
                }
                "nginx.request_time" => match &record.kind {
                    MetricKind::Distribution(sketch) => {
                        let q = sketch.quantile(0.5).expect("single-sample sketch has a median");
                        assert!((q - 12.0).abs() < 0.1, "got {q}, scale should have run first");
                    }
                    other => panic!("expected Distribution, got {other:?}"),
                },
                other => panic!("unexpected series name: {other}"),
            }
        }
    }

    /// The headline claim `docs/adr/attribute-filtering-components.md` exists to make: a `set`
    /// stage stamping a distinguishing tag, and a `has_attributes`/`drop_attributes` stage
    /// downstream matching on exactly that tag, are inverses -- `has_attributes` forwards what
    /// `set` stamped and `drop_attributes` forwards everything else, on the identical config.
    #[test]
    fn set_then_has_attributes_round_trips_the_stamped_tag() {
        let resource = Arc::new(Resource::default());
        let event = Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::str("line"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );

        let mut tag_web = Set::new(vec![], vec![("stream".to_string(), Value::str("web"))]);
        let tagged = tag_web.process(&resource, event).expect("set always forwards");

        let config = vec![("stream".to_string(), Value::str("web"))];
        let mut has_web = HasAttributes::new(vec![], config.clone());
        let mut drop_web = DropAttributes::new(vec![], config);

        assert!(
            has_web.process(&resource, tagged.clone()).is_some(),
            "has_attributes must forward exactly what set stamped"
        );
        assert!(
            drop_web.process(&resource, tagged).is_none(),
            "drop_attributes must drop exactly what set stamped"
        );
    }
}
