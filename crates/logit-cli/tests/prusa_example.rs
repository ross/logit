//! Runs a real Prusa Buddy datagram through `examples/prusa/logit.yaml`'s pipeline without a
//! socket: `syslog_in`'s decoder under `framing: datagram`, `examples/prusa/prusa.lua` in a real
//! `ScriptWorker`, and the `line_protocol` transform through the runtime's own `process_batch`.
//! `logit validate` never reads a `lua_file`, so this is what checks the script compiles and
//! produces the points its comments and the example's README describe.
//!
//! The fixtures are two consecutive datagrams captured from one printer on firmware 7.0.0+16903.

use bytes::Bytes;
use logit_core::{interner, Event, EventBatch, MetricKind, MetricValue, Resource, Value};
use logit_pipeline::runtime::process_batch;
use logit_pipeline::test_util::TelemetryProbe;
use logit_proto::influxdb::Precision;
use logit_proto::syslog::SyslogDecoder;
use logit_proto::Decoder;
use logit_script::{ProcessOutcome, ScriptWorker};
use logit_transforms::LineProtocol;
use std::fs;
use std::path::Path;
use std::sync::Arc;

/// The first datagram's receipt time in the capture, `1792167827.7678285`, in nanoseconds.
const RECEIVED_AT: i64 = 1_792_167_827_767_828_500;

/// `msg=920`, 984 bytes, 37 points.
const DATAGRAM_1: &[u8] = include_bytes!("fixtures/prusa-datagram-1.txt");
/// `msg=921`, the next datagram the printer sent, 977 bytes.
const DATAGRAM_2: &[u8] = include_bytes!("fixtures/prusa-datagram-2.txt");

const MAC: &str = "10:9c:70:2d:d5:c4";

/// The newest offset in `DATAGRAM_1`, `esp_out`'s, in microseconds.
const NEWEST_OFFSET_US: i64 = 676_222;

fn script_source() -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/prusa/prusa.lua");
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()))
}

fn worker(probe: &TelemetryProbe) -> ScriptWorker {
    ScriptWorker::new(&script_source())
        .expect("examples/prusa/prusa.lua should load")
        .with_telemetry(probe.telemetry("prusa", "lua_file", "transform"))
        .expect("telemetry should install")
}

/// One datagram decoded as `syslog_in` decodes it under `framing: datagram`.
fn decode(datagram: &'static [u8], received_at: i64) -> (Arc<Resource>, Event) {
    let mut decoder = SyslogDecoder::new(Arc::new(Resource::default())).with_line_splitting(false);
    let mut out = Vec::new();
    let (resource, _scope) = decoder
        .decode_into(Bytes::from_static(datagram), received_at, &mut out)
        .expect("the datagram should decode");
    assert_eq!(out.len(), 1, "framing: datagram makes one datagram one event");
    (resource, out.pop().unwrap())
}

/// Runs one event through the script as `run_lua` does, returning the batch resource the
/// script left and the event it returned.
fn run_script(w: &ScriptWorker, resource: Arc<Resource>, event: Event) -> (Arc<Resource>, Event) {
    w.set_resource(&resource);
    let event = match w.process(event).expect("process() should succeed") {
        ProcessOutcome::Emit(event, None) => *event,
        _ => panic!("expected one unmarked event from process()"),
    };
    (w.take_resource().unwrap_or(resource), event)
}

fn message(event: &Event) -> &str {
    event.log.as_ref().expect("event should carry a log").message.as_str().expect("UTF-8")
}

fn attr<'a>(event: &'a Event, key: &str) -> Option<&'a str> {
    event.attributes.get(key).and_then(Value::as_str)
}

fn gauge(event: &Event, name: &str) -> MetricValue {
    let record = event
        .metrics
        .iter()
        .find(|m| interner::resolve(m.name) == name)
        .unwrap_or_else(|| panic!("expected a {name} metric, got: {:?}", event.metrics));
    match &record.kind {
        MetricKind::Gauge(v) => *v,
        other => panic!("expected {name} to be a Gauge, got: {other:?}"),
    }
}

/// Each point line of `DATAGRAM_1`, as the measurement and the offset the firmware wrote,
/// read straight from the fixture as the oracle for every point's order and timestamp.
fn fixture_points() -> Vec<(String, i64)> {
    let text = std::str::from_utf8(DATAGRAM_1).unwrap();
    let (_, msg) = text.split_once("msg=920,tm=1730850936,v=4 ").expect("the fixture's header");
    msg.lines()
        .map(|line| {
            let measurement = line.split([',', ' ']).next().unwrap().to_string();
            let offset = line.rsplit(' ').next().unwrap().parse().unwrap();
            (measurement, offset)
        })
        .collect()
}

#[test]
fn the_config_runs_the_pipeline_this_test_assumes() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/prusa/logit.yaml");
    let text =
        fs::read_to_string(&path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    assert!(text.contains("framing: datagram"), "printer_in should take one datagram per event");
    assert!(text.contains("lua_file: prusa.lua"), "logit.yaml should point at prusa.lua");
    assert!(text.contains("type: line_protocol"), "logit.yaml should parse the points");
    assert!(
        !text.lines().any(|line| line.trim_start().starts_with("bucket:")),
        "influx_out should follow the bucket the script carries"
    );
}

#[test]
fn a_datagram_becomes_one_anchored_point_per_line() {
    let mut probe = TelemetryProbe::new();
    let w = worker(&probe);
    let (resource, event) = decode(DATAGRAM_1, RECEIVED_AT);
    assert_eq!(attr(&event, "syslog.hostname"), Some(MAC));

    let (resource, rewritten) = run_script(&w, resource, event);
    assert_eq!(
        resource.attributes.get("_logit.influxdb.bucket").and_then(Value::as_str),
        Some("prusa"),
        "the script should name the bucket on the batch resource"
    );
    assert!(!message(&rewritten).contains("msg="), "the header should be stripped");

    let transform_telemetry = probe.telemetry("points", "line_protocol", "transform");
    let mut transform = LineProtocol::new(Precision::Ns, false);
    let batch = EventBatch { resource, scope: None, events: vec![rewritten] };
    let out = process_batch(&mut transform, batch, &mut Vec::new(), &transform_telemetry)
        .expect("line_protocol should emit the points");
    assert_eq!(
        out.resource.attributes.get("_logit.influxdb.bucket").and_then(Value::as_str),
        Some("prusa")
    );
    assert_eq!(
        probe.sum("logit.component.events.discarded", &[("component", "points")]),
        1.0,
        "the datagram's own event should be discarded once its points are out"
    );
    assert_eq!(probe.sum("logit.transform.lines.dropped", &[]), 0.0);
    assert_eq!(probe.sum("logit.transform.lines.skipped", &[]), 0.0);

    let expected = fixture_points();
    assert_eq!(expected.len(), 37);
    assert_eq!(out.events.len(), 37, "one child per point, and no parent");
    for (i, (child, (measurement, offset))) in out.events.iter().zip(&expected).enumerate() {
        assert!(child.log.is_none(), "point {i} should carry no log");
        assert_eq!(
            attr(child, "_logit.influxdb.measurement"),
            Some(measurement.as_str()),
            "point {i}'s measurement"
        );
        assert_eq!(attr(child, "mac_address"), Some(MAC), "point {i}'s mac_address");
        for (key, _) in child.attributes.iter() {
            let key = interner::resolve(key);
            assert!(!key.starts_with("syslog."), "point {i} still carries {key}");
        }
        assert_eq!(
            child.timestamp,
            RECEIVED_AT - (NEWEST_OFFSET_US - offset) * 1_000,
            "point {i} ({measurement}) should keep its offset from the newest point"
        );
    }

    let first = &out.events[0];
    assert_eq!(attr(first, "_logit.influxdb.measurement"), Some("ttemp_noz"));
    assert_eq!((attr(first, "n"), attr(first, "a")), (Some("0"), Some("1")));
    assert_eq!(gauge(first, "ttemp_noz.value"), MetricValue::I64(0));
    assert_eq!(first.timestamp, RECEIVED_AT - 676_380_000);

    let stall = &out.events[1];
    assert_eq!(attr(stall, "_logit.influxdb.measurement"), Some("stp_stall"));
    assert_eq!(gauge(stall, "stp_stall.value"), MetricValue::I64(1), "v renamed to value");
    assert_eq!(stall.timestamp, RECEIVED_AT - (676_222 - 38_950) * 1_000);

    let heap = &out.events[6];
    assert_eq!(attr(heap, "_logit.influxdb.measurement"), Some("heap"));
    assert_eq!(gauge(heap, "heap.free"), MetricValue::I64(61_088));
    assert_eq!(gauge(heap, "heap.total"), MetricValue::I64(79_012));

    let fans: Vec<&Event> = out
        .events
        .iter()
        .filter(|e| attr(e, "_logit.influxdb.measurement") == Some("fan"))
        .collect();
    assert_eq!(
        fans.iter().map(|e| attr(e, "fan")).collect::<Vec<_>>(),
        [Some("print"), Some("heatbreak")]
    );
    for fan in fans {
        assert_eq!(fan.metrics.len(), 3);
        for field in ["fan.state", "fan.pwm", "fan.measured"] {
            assert_eq!(gauge(fan, field), MetricValue::F64(0.0));
        }
    }

    let last = out.events.last().unwrap();
    assert_eq!(attr(last, "_logit.influxdb.measurement"), Some("esp_out"));
    assert_eq!(gauge(last, "esp_out.sent"), MetricValue::I64(4_171_237));
    assert_eq!(last.timestamp, RECEIVED_AT, "the newest point takes the receipt time");
}

#[test]
fn consecutive_datagrams_count_no_gap() {
    let mut probe = TelemetryProbe::new();
    let w = worker(&probe);
    for (datagram, at) in [(DATAGRAM_1, RECEIVED_AT), (DATAGRAM_2, 1_792_167_828_553_567_000)] {
        let (resource, event) = decode(datagram, at);
        run_script(&w, resource, event);
    }
    assert_eq!(probe.sum("prusa.datagrams.missed", &[]), 0.0);
    assert_eq!(probe.sum("prusa.datagrams.resync", &[]), 0.0);
}

#[test]
fn a_gap_in_the_sequence_counts_the_missed_datagrams() {
    let mut probe = TelemetryProbe::new();
    let w = worker(&probe);
    let (resource, event) = decode(DATAGRAM_1, RECEIVED_AT);
    run_script(&w, resource, event);
    let (resource, event) = decode(
        b"<14>1 - 10:9c:70:2d:d5:c4 buddy - - - msg=925,tm=1734000000,v=4 cpu_usage v=73i 0\n",
        RECEIVED_AT + 2_500_000_000,
    );
    run_script(&w, resource, event);
    assert_eq!(probe.sum("prusa.datagrams.missed", &[("mac_address", MAC)]), 4.0);
    assert_eq!(probe.sum("prusa.datagrams.resync", &[]), 0.0);
}

#[test]
fn an_unsupported_version_is_forwarded_unchanged_and_counted() {
    let mut probe = TelemetryProbe::new();
    let w = worker(&probe);
    let (resource, event) = decode(
        b"<14>1 - 10:9c:70:2d:d5:c4 buddy - - - msg=1,tm=5,v=9 cpu_usage v=73i 0\n",
        RECEIVED_AT,
    );
    let original = event.clone();
    let (after, out) = run_script(&w, resource.clone(), event);
    assert_eq!(out, original);
    assert!(Arc::ptr_eq(&after, &resource), "no bucket for a datagram the script didn't rewrite");
    assert_eq!(probe.sum("prusa.datagrams.skipped", &[("reason", "version")]), 1.0);
}

#[test]
fn a_non_buddy_syslog_event_passes_through_unchanged() {
    let probe = TelemetryProbe::new();
    let w = worker(&probe);
    let (resource, event) =
        decode(b"<134>1 - web-1 nginx - - - GET /index.html 200\n", RECEIVED_AT);
    let original = event.clone();
    w.set_resource(&resource);
    let out = match w.process(event).expect("process() should succeed") {
        ProcessOutcome::Emit(event, None) => *event,
        _ => panic!("expected one unmarked event from process()"),
    };
    assert_eq!(out, original);
    assert!(w.take_resource().is_none(), "the script should leave the resource alone");
}
