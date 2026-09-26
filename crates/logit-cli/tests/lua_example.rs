//! Runs `examples/lua/process.lua`, the Lua stage `examples/lua/logit.yaml` documents, through a
//! real `ScriptWorker`. `logit validate` resolves that config's graph but never reads a
//! `lua_file`, so this is the only thing that checks the script compiles and behaves as its own
//! comments describe.

use logit_core::{AttrMap, Event, LogRecord, Resource, Severity, Value};
use logit_pipeline::Transform;
use logit_script::{ProcessOutcome, ScriptWorker};
use logit_transforms::Logfmt;
use std::fs;
use std::path::Path;
use std::sync::Arc;

/// `flush()`'s tick time, and what every flushed event's timestamp must equal.
const NOW: i64 = 1_700_000_010_000_000_000;

fn script_source() -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/lua/process.lua");
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()))
}

fn worker() -> ScriptWorker {
    ScriptWorker::new(&script_source())
        .expect("examples/lua/process.lua should load")
        .with_targets(&["errors".to_string(), "stats".to_string()])
}

/// Builds one event the way `examples/lua/logit.yaml`'s pipeline does before `shop` sees it: a raw
/// logfmt line arriving as a log record with the syslog attributes `syslog_in` would have set, run
/// through the real `logfmt` transform.
fn request_event(line: &str) -> Event {
    let mut attrs = AttrMap::new();
    attrs.insert("syslog.hostname", "web-1");
    attrs.insert("syslog.tag", "shop-api");
    let log = LogRecord {
        message: Value::str(line),
        severity: None,
        body_format: logit_core::BodyFormat::Raw,
        trace: None,
        event_name: None,
        observed_timestamp: 0,
        dropped_attributes_count: 0,
    };
    let mut event = Event::log(0, attrs, log);
    let resource = Arc::new(Resource::default());
    assert!(Logfmt::new(false).process(&resource, &mut event), "logfmt should forward {line:?}");
    event
}

/// A Lua number arrives as `Value::I64` or `Value::F64` depending on whether it's integral
/// (LuaJIT's dual-number mode); callers must accept either rather than assert one variant.
fn number(event: &Event, key: &str) -> Option<f64> {
    match event.attributes.get(key)? {
        Value::I64(v) => Some(*v as f64),
        Value::F64(v) => Some(*v),
        _ => None,
    }
}

fn message(event: &Event) -> &str {
    event.log.as_ref().expect("event should carry a log").message.as_str().expect("UTF-8 message")
}

fn severity(event: &Event) -> Option<Severity> {
    event.log.as_ref().expect("event should carry a log").severity
}

fn emit(outcome: ProcessOutcome) -> (Event, Option<u16>) {
    match outcome {
        ProcessOutcome::Emit(event, mark) => (*event, mark),
        _ => panic!("expected a single Emit outcome"),
    }
}

fn emit_many(outcome: ProcessOutcome) -> Vec<(Event, Option<u16>)> {
    match outcome {
        ProcessOutcome::EmitMany(events) => events,
        _ => panic!("expected an EmitMany outcome"),
    }
}

/// A metric named `name` on `event`, resolving its interned `Symbol`.
fn metric<'a>(event: &'a Event, name: &str) -> &'a logit_core::MetricRecord {
    event
        .metrics
        .iter()
        .find(|m| logit_core::interner::resolve(m.name) == name)
        .unwrap_or_else(|| panic!("expected a {name} metric, got: {:?}", event.metrics))
}

fn samples(record: &logit_core::MetricRecord) -> &logit_core::Samples {
    match &record.kind {
        logit_core::MetricKind::Samples(s) => s,
        other => panic!("expected Samples, got: {other:?}"),
    }
}

fn set_members(record: &logit_core::MetricRecord) -> Vec<String> {
    match &record.kind {
        logit_core::MetricKind::SetMembers(members) => members
            .iter()
            .map(|m| std::str::from_utf8(m).expect("member is UTF-8").to_string())
            .collect(),
        other => panic!("expected SetMembers, got: {other:?}"),
    }
}

fn sum_value(record: &logit_core::MetricRecord) -> f64 {
    match &record.kind {
        logit_core::MetricKind::Sum(sum) => sum.value,
        other => panic!("expected Sum, got: {other:?}"),
    }
}

/// The `http.route` events flushed to `stats` are grouped by, resolving the Lua-set attribute.
/// `None` for an event that carries no route at all (the `shop.users` metric event).
fn route_of(event: &Event) -> Option<&str> {
    event.attributes.get("http.route").and_then(Value::as_str)
}

fn events_for_route<'a>(flushed: &'a [(Event, Option<u16>)], route: &str) -> Vec<&'a Event> {
    flushed.iter().filter(|(e, _)| route_of(e) == Some(route)).map(|(e, _)| e).collect()
}

#[test]
fn the_config_declares_the_targets_and_script_this_test_assumes() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/lua/logit.yaml");
    let text =
        fs::read_to_string(&path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    assert!(text.contains("targets: [errors, stats]"), "logit.yaml should declare both targets");
    assert!(text.contains("lua_file: process.lua"), "logit.yaml should point at process.lua");
}

#[test]
fn a_request_line_is_archived_with_derived_fields() {
    let w = worker();
    let event = request_event(
        "level=info msg=request method=GET path=/api/orders/42?expand=items status=200 dur=12ms user=alice",
    );
    let original_message = message(&event).to_string();
    let (out, mark) = emit(w.process(event).unwrap());
    assert_eq!(mark, None);
    assert_eq!(out.attributes.get("http.route").and_then(Value::as_str), Some("/api/orders/{id}"));
    assert_eq!(number(&out, "http.response.status_code"), Some(200.0));
    assert_eq!(number(&out, "duration_ms"), Some(12.0));
    assert_eq!(out.attributes.get("slow"), Some(&Value::Bool(false)));
    assert_eq!(message(&out), original_message);
}

#[test]
fn durations_convert_to_milliseconds() {
    let w = worker();

    let (fast, _) = emit(
        w.process(request_event(
            "level=info msg=request method=GET path=/api/orders/1 status=200 dur=1.5s user=alice",
        ))
        .unwrap(),
    );
    assert_eq!(number(&fast, "duration_ms"), Some(1500.0));
    assert_eq!(fast.attributes.get("slow"), Some(&Value::Bool(true)));

    let (micros, _) = emit(
        w.process(request_event(
            "level=info msg=request method=GET path=/api/orders/2 status=200 dur=850us user=bob",
        ))
        .unwrap(),
    );
    let ms = number(&micros, "duration_ms").expect("duration_ms should be set");
    assert!((ms - 0.85).abs() < 1e-9, "expected ~0.85ms, got {ms}");

    let (unrecognized, _) = emit(
        w.process(request_event(
            "level=info msg=request method=GET path=/api/orders/3 status=200 dur=soon user=carol",
        ))
        .unwrap(),
    );
    assert_eq!(unrecognized.attributes.get("duration_ms"), None);
}

#[test]
fn a_non_request_line_passes_through_unchanged() {
    let w = worker();
    let (out, mark) =
        emit(w.process(request_event("level=info msg=listening addr=:8080")).unwrap());
    assert_eq!(mark, None);
    assert_eq!(out.attributes.get("http.route"), None);
}

#[test]
fn a_server_error_fans_out_to_an_alert_and_the_archive() {
    let w = worker();
    let events = emit_many(
        w.process(request_event(
            "level=error msg=request method=POST path=/api/checkout status=502 dur=2.3s user=bob err=\"payment gateway timeout\"",
        ))
        .unwrap(),
    );
    assert_eq!(events.len(), 2);

    let (alert, alert_mark) = &events[0];
    assert_eq!(*alert_mark, Some(0));
    assert_eq!(severity(alert), Some(Severity::Error));
    assert_eq!(message(alert), "POST /api/checkout returned 502: payment gateway timeout");
    assert_eq!(alert.attributes.get("syslog.hostname").and_then(Value::as_str), Some("web-1"));
    assert_eq!(alert.attributes.get("http.route").and_then(Value::as_str), Some("/api/checkout"));

    let (archived, archived_mark) = &events[1];
    assert_eq!(*archived_mark, None);
    assert_eq!(
        message(archived),
        "level=error msg=request method=POST path=/api/checkout status=502 dur=2.3s user=bob err=\"payment gateway timeout\""
    );
}

#[test]
fn a_repeated_server_error_is_archived_and_summarized_at_flush() {
    let w = worker();
    let line = "level=error msg=request method=POST path=/api/checkout status=502 dur=2.3s user=bob err=\"timeout\"";

    emit_many(w.process(request_event(line)).unwrap());
    let (out, mark) = emit(w.process(request_event(line)).unwrap());
    assert_eq!(mark, None);
    assert_eq!(message(&out), line);

    let flushed = w.flush(NOW).unwrap();
    let (summary, summary_mark) = flushed
        .iter()
        .find(|(e, _)| e.log.is_some() && severity(e) == Some(Severity::Warn))
        .unwrap_or_else(|| {
            panic!("expected a warn-severity summary event in flush(), got: {flushed:?}")
        });
    assert_eq!(*summary_mark, Some(0));
    assert!(message(summary).contains("1 more times"), "got: {}", message(summary));
    assert_eq!(number(summary, "repeats"), Some(1.0));
    assert_eq!(summary.attributes.get("syslog.hostname").and_then(Value::as_str), Some("web-1"));
}

#[test]
fn health_checks_are_dropped_but_counted() {
    let w = worker();
    let outcome = w
        .process(request_event(
            "level=info msg=request method=GET path=/healthz status=200 dur=1ms",
        ))
        .unwrap();
    assert!(matches!(outcome, ProcessOutcome::Drop));

    let flushed = w.flush(NOW).unwrap();
    let healthz = events_for_route(&flushed, "/healthz");
    assert_eq!(healthz.len(), 1, "expected one stats event for /healthz, got: {flushed:?}");
    assert_eq!(sum_value(metric(healthz[0], "http.server.requests")), 1.0);
}

#[test]
fn a_failing_health_check_is_alerted_and_counted() {
    let w = worker();
    let events = emit_many(
        w.process(request_event(
            "level=error msg=request method=GET path=/healthz status=503 dur=1ms",
        ))
        .unwrap(),
    );
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].1, Some(0));
    assert_eq!(message(&events[0].0), "GET /healthz returned 503");
    assert_eq!(events[1].1, None);

    let flushed = w.flush(NOW).unwrap();
    let healthz = events_for_route(&flushed, "/healthz");
    assert_eq!(healthz.len(), 1);
    assert_eq!(sum_value(metric(healthz[0], "http.server.requests")), 1.0);
    assert_eq!(sum_value(metric(healthz[0], "http.server.errors")), 1.0);
}

#[test]
fn flush_emits_per_route_metrics_to_stats() {
    let w = worker();
    for line in [
        "level=info msg=request method=GET path=/api/orders/42 status=200 dur=12ms user=alice",
        "level=info msg=request method=GET path=/api/orders/7 status=200 dur=30ms user=bob",
        "level=error msg=request method=POST path=/api/checkout status=502 dur=2.3s user=bob err=\"x\"",
        "level=info msg=request method=GET path=/healthz status=200 dur=1ms",
    ] {
        let _ = w.process(request_event(line));
    }

    let flushed = w.flush(NOW).unwrap();
    assert!(!flushed.is_empty());
    for (event, mark) in &flushed {
        assert_eq!(*mark, Some(1), "every flushed metrics event should be marked for stats");
        assert_eq!(event.timestamp, NOW);
    }

    let orders = events_for_route(&flushed, "/api/orders/{id}");
    assert_eq!(orders.len(), 1);
    assert_eq!(sum_value(metric(orders[0], "http.server.requests")), 2.0);
    assert_eq!(sum_value(metric(orders[0], "http.server.errors")), 0.0);
    let duration = samples(metric(orders[0], "http.server.request.duration"));
    let mut values: Vec<f64> = duration.values.to_vec();
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(values, vec![12.0, 30.0]);
    assert_eq!(duration.sample_rate, 1.0);
    assert_eq!(
        metric(orders[0], "http.server.request.duration").unit.map(logit_core::interner::resolve),
        Some("ms")
    );

    let checkout = events_for_route(&flushed, "/api/checkout");
    assert_eq!(checkout.len(), 1);
    assert_eq!(sum_value(metric(checkout[0], "http.server.requests")), 1.0);
    assert_eq!(sum_value(metric(checkout[0], "http.server.errors")), 1.0);

    let users_event = flushed
        .iter()
        .find(|(e, _)| {
            e.metrics.iter().any(|m| logit_core::interner::resolve(m.name) == "shop.users")
        })
        .map(|(e, _)| e)
        .expect("expected a shop.users metric event");
    let mut members = set_members(metric(users_event, "shop.users"));
    members.sort();
    assert_eq!(members, vec!["alice".to_string(), "bob".to_string()]);

    let resource = w.take_resource().expect("flush() should have written the resource");
    assert_eq!(resource.attributes.get("service.name").and_then(Value::as_str), Some("shop-api"));

    assert!(w.flush(NOW).unwrap().is_empty(), "a second flush should have nothing left to emit");
}

#[test]
fn routes_past_the_cap_collapse_into_other() {
    let w = worker();
    for i in 0..60 {
        let line = format!("level=info msg=request method=GET path=/p{i} status=200 dur=1ms");
        let _ = w.process(request_event(&line));
    }

    let flushed = w.flush(NOW).unwrap();
    let route_events: Vec<&Event> = flushed
        .iter()
        .filter(|(e, _)| {
            e.metrics
                .iter()
                .any(|m| logit_core::interner::resolve(m.name) == "http.server.requests")
        })
        .map(|(e, _)| e)
        .collect();
    assert_eq!(route_events.len(), 51, "expected 50 named routes plus /{{other}}");

    let other = events_for_route(&flushed, "/{other}");
    assert_eq!(other.len(), 1);
    assert_eq!(sum_value(metric(other[0], "http.server.requests")), 10.0);
}

/// Only the counters are bucketed past the route cap: the event, the health-check drop, and the
/// alert keep the real route.
#[test]
fn the_route_cap_buckets_counters_only() {
    let w = worker();
    for i in 0..50 {
        let line = format!("level=info msg=request method=GET path=/p{i} status=200 dur=1ms");
        let _ = w.process(request_event(&line));
    }

    let (archived, mark) = emit(
        w.process(request_event(
            "level=info msg=request method=GET path=/overflow status=200 dur=1ms",
        ))
        .unwrap(),
    );
    assert_eq!(mark, None);
    assert_eq!(archived.attributes.get("http.route").and_then(Value::as_str), Some("/overflow"));

    let outcome = w
        .process(request_event(
            "level=info msg=request method=GET path=/healthz status=200 dur=1ms",
        ))
        .unwrap();
    assert!(matches!(outcome, ProcessOutcome::Drop));

    let events = emit_many(
        w.process(request_event(
            "level=error msg=request method=POST path=/overflow status=502 dur=1ms",
        ))
        .unwrap(),
    );
    assert_eq!(message(&events[0].0), "POST /overflow returned 502");

    let flushed = w.flush(NOW).unwrap();
    let other = events_for_route(&flushed, "/{other}");
    assert_eq!(other.len(), 1);
    assert_eq!(sum_value(metric(other[0], "http.server.requests")), 3.0);
    assert_eq!(sum_value(metric(other[0], "http.server.errors")), 1.0);
    assert!(events_for_route(&flushed, "/overflow").is_empty());
}

#[test]
fn alerts_past_the_cap_are_archived_only() {
    let w = worker();
    for i in 0..50 {
        let line = format!("level=error msg=request method=GET path=/e{i} status=500 dur=1ms");
        assert_eq!(emit_many(w.process(request_event(&line)).unwrap()).len(), 2);
    }

    let (archived, mark) = emit(
        w.process(request_event("level=error msg=request method=GET path=/e50 status=500 dur=1ms"))
            .unwrap(),
    );
    assert_eq!(mark, None);
    assert_eq!(archived.attributes.get("http.route").and_then(Value::as_str), Some("/e50"));

    let flushed = w.flush(NOW).unwrap();
    assert!(flushed.iter().all(|(e, _)| e.log.is_none()), "no repeats, so no summary lines");
}

#[test]
fn durations_past_the_cap_are_reservoir_sampled() {
    let w = worker();
    for _ in 0..600 {
        let _ = w.process(request_event(
            "level=info msg=request method=GET path=/one status=200 dur=1ms",
        ));
    }

    let flushed = w.flush(NOW).unwrap();
    let one = events_for_route(&flushed, "/one");
    assert_eq!(one.len(), 1);
    let duration = samples(metric(one[0], "http.server.request.duration"));
    assert_eq!(duration.values.len(), 500);
    let expected_rate = 500.0 / 600.0;
    assert!(
        (duration.sample_rate - expected_rate).abs() < 1e-9,
        "expected sample_rate ~{expected_rate}, got {}",
        duration.sample_rate
    );
}
