//! Runs a real Prusa Buddy datagram through `examples/prusa/logit.yaml`'s pipeline without a
//! socket: `syslog_in`'s decoder under `framing: datagram`, `examples/prusa/prusa.lua` in a real
//! `ScriptWorker`, and the `line_protocol` transform through the runtime's own `process_batch`.
//! `logit validate` never reads a `lua_file`, so this is what checks the script compiles and
//! produces the points its comments and the example's README describe.
//!
//! The fixtures are two consecutive datagrams captured from one printer on firmware 7.0.0+16903.
//! The script runs with the `vars:` `logit.yaml` gives it, which names that printer.

use bytes::Bytes;
use logit_config::{LuaVar, LuaVars};
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

/// A receipt time for the first datagram. Its sub-second part, 100 µs, is less than the
/// datagram's span of 676,380 µs, so anchoring every point but the newest crosses a second
/// boundary, the case a decimal-string timestamp gets wrong if the borrow is.
const RECEIVED_AT: i64 = 1_792_167_828_000_100_000;

/// `msg=920`, 984 bytes, 37 points.
const DATAGRAM_1: &[u8] = include_bytes!("fixtures/prusa-datagram-1.txt");
/// `msg=921`, the next datagram the printer sent, 977 bytes.
const DATAGRAM_2: &[u8] = include_bytes!("fixtures/prusa-datagram-2.txt");

const MAC: &str = "10:9c:70:2d:d5:c4";

/// The name `logit.yaml`'s `vars.names` gives `MAC`.
const NAME: &str = "mk4";

/// The newest offset in `DATAGRAM_1`, `esp_out`'s, in microseconds.
const NEWEST_OFFSET_US: i64 = 676_222;

fn script_source() -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/prusa/prusa.lua");
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()))
}

fn config_path() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/prusa/logit.yaml")
}

/// The `prusa` component's `vars:` from `logit.yaml`. Read through a plain `Value`, because the
/// file's `!env` tag needs `logit`'s own loader to resolve.
fn shipped_vars() -> LuaVars {
    let path = config_path();
    let text =
        fs::read_to_string(&path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    let config: serde_norway::Value = serde_norway::from_str(&text).unwrap();
    serde_norway::from_value(config["components"]["prusa"]["vars"].clone())
        .expect("the prusa component's vars should parse")
}

fn worker(probe: &TelemetryProbe) -> ScriptWorker {
    worker_with(probe, &shipped_vars())
}

fn worker_with(probe: &TelemetryProbe, vars: &LuaVars) -> ScriptWorker {
    ScriptWorker::new_with_vars(&script_source(), false, vars)
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

/// Runs the script's output through `line_protocol` as the `points` node, reporting to `probe`.
fn line_protocol(probe: &TelemetryProbe, resource: Arc<Resource>, event: Event) -> EventBatch {
    let telemetry = probe.telemetry("points", "line_protocol", "transform");
    let mut transform = LineProtocol::new(Precision::Ns, false).with_telemetry(telemetry.clone());
    let batch = EventBatch { resource, scope: None, events: vec![event] };
    process_batch(&mut transform, batch, &mut Vec::new(), &telemetry)
        .expect("line_protocol should emit the points")
}

/// One datagram through the decoder, the script, and `line_protocol`, received at `RECEIVED_AT`.
fn pipeline(w: &ScriptWorker, probe: &TelemetryProbe, datagram: &'static [u8]) -> Vec<Event> {
    let (resource, event) = decode(datagram, RECEIVED_AT);
    let (resource, rewritten) = run_script(w, resource, event);
    line_protocol(probe, resource, rewritten).events
}

/// Each child's measurement and timestamp, for comparing a whole datagram at once.
fn measured_at(events: &[Event]) -> Vec<(&str, i64)> {
    events.iter().map(|e| (attr(e, "_logit.influxdb.measurement").unwrap(), e.timestamp)).collect()
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
    let path = config_path();
    let text =
        fs::read_to_string(&path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    assert!(text.contains("framing: datagram"), "printer_in should take one datagram per event");
    assert!(text.contains("lua_file: prusa.lua"), "logit.yaml should point at prusa.lua");
    assert!(text.contains("type: line_protocol"), "logit.yaml should parse the points");
    let config: serde_norway::Value = serde_norway::from_str(&text).unwrap();
    assert!(
        config["components"]["influx_out"].get("bucket").is_none(),
        "influx_out should follow the bucket the script carries"
    );
    let vars = shipped_vars();
    assert_eq!(vars.get("bucket"), Some(&LuaVar::String("prusa".into())));
    match vars.get("names") {
        Some(LuaVar::Map(names)) => assert_eq!(names.get(MAC), Some(&LuaVar::String(NAME.into()))),
        other => panic!("vars.names should name the fixtures' printer, got {other:?}"),
    }
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

    let out = line_protocol(&probe, resource, rewritten);
    assert_eq!(
        out.resource.attributes.get("_logit.influxdb.bucket").and_then(Value::as_str),
        Some("prusa")
    );
    assert_eq!(
        probe.sum("logit.component.events.discarded", &[("component", "points")]),
        1.0,
        "the datagram's own event should be discarded once its points are out"
    );
    assert_eq!(
        probe.sum("logit.transform.lines.dropped", &[]),
        0.0,
        "every rewritten line should parse as a point"
    );
    assert_eq!(probe.sum("logit.transform.lines.skipped", &[]), 0.0);
    assert_eq!(probe.sum("prusa.points.skipped", &[]), 0.0);

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
        assert_eq!(attr(child, "printer"), Some(NAME), "point {i}'s printer");
        assert_eq!(attr(child, "mac_address"), None, "point {i} carries no mac_address");
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
    assert_eq!(probe.sum("prusa.datagrams.missed", &[("printer", NAME)]), 4.0);
    assert_eq!(probe.sum("prusa.datagrams.resync", &[]), 0.0);
}

#[test]
fn an_unsupported_version_is_discarded_and_counted_as_dropped() {
    let mut probe = TelemetryProbe::new();
    let w = worker(&probe);
    let (resource, event) = decode(
        b"<14>1 - 10:9c:70:2d:d5:c4 buddy - - - msg=1,tm=5,v=9 cpu_usage v=73i 0\n",
        RECEIVED_AT,
    );
    w.set_resource(&resource);
    let outcome = w.process(event).expect("process() should succeed");
    assert!(matches!(outcome, ProcessOutcome::Drop), "the datagram should not reach line_protocol");
    assert!(w.take_resource().is_none(), "no bucket for a datagram the script dropped");
    assert_eq!(probe.sum("prusa.datagrams.dropped", &[("reason", "version")]), 1.0);
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

#[test]
fn a_v3_datagram_counts_milliseconds_and_skips_seq_points() {
    let mut probe = TelemetryProbe::new();
    let w = worker(&probe);
    let points = pipeline(
        &w,
        &probe,
        b"<14>1 - 10:9c:70:2d:d5:c4 buddy - - - msg=1,tm=5000,v=3 cpu_usage v=73i 10\n\
          rec,_seq=4 v=1i 20\n\
          temp_bed v=19.5 40\n",
    );
    assert_eq!(
        measured_at(&points),
        [("cpu_usage", RECEIVED_AT - 30_000_000), ("temp_bed", RECEIVED_AT)]
    );
    assert_eq!(probe.sum("prusa.points.skipped", &[("reason", "seq")]), 1.0);
}

#[test]
fn a_v2_datagram_sums_each_offset_onto_the_one_before() {
    let probe = TelemetryProbe::new();
    let w = worker(&probe);
    // Offsets -5, 15, and 20 ms relative to the previous point are -5, 10, and 30 ms from `tm`.
    let points = pipeline(
        &w,
        &probe,
        b"<14>1 - 10:9c:70:2d:d5:c4 buddy - - - msg=1,tm=5000,v=2 a v=1i -5\n\
          b v=2i 15\n\
          c v=3i 20\n",
    );
    assert_eq!(
        measured_at(&points),
        [("a", RECEIVED_AT - 35_000_000), ("b", RECEIVED_AT - 20_000_000), ("c", RECEIVED_AT)]
    );
}

#[test]
fn a_nan_value_is_skipped_and_counted() {
    let mut probe = TelemetryProbe::new();
    let w = worker(&probe);
    let points = pipeline(
        &w,
        &probe,
        b"<14>1 - 10:9c:70:2d:d5:c4 buddy - - - msg=1,tm=5000,v=4 x value=nan 10\n\
          y v=NaN 20\n\
          z v=1i 30\n",
    );
    assert_eq!(measured_at(&points), [("z", RECEIVED_AT)]);
    assert_eq!(probe.sum("prusa.points.skipped", &[("reason", "nan")]), 2.0);
    assert_eq!(probe.sum("logit.transform.lines.dropped", &[]), 0.0);
}

#[test]
fn only_a_lone_v_field_is_renamed_to_value() {
    let probe = TelemetryProbe::new();
    let w = worker(&probe);
    let points = pipeline(
        &w,
        &probe,
        b"<14>1 - 10:9c:70:2d:d5:c4 buddy - - - msg=1,tm=5000,v=4 fw_version v=\"7.0.0+16903\" 0\n\
          pair v=1i,w=2i 5\n",
    );
    assert_eq!(measured_at(&points), [("fw_version", RECEIVED_AT - 5_000), ("pair", RECEIVED_AT)]);
    assert_eq!(attr(&points[0], "_logit.influxdb.field.value"), Some("7.0.0+16903"));
    assert_eq!(gauge(&points[1], "pair.v"), MetricValue::I64(1));
    assert_eq!(gauge(&points[1], "pair.w"), MetricValue::I64(2));
}

/// A printer `vars.names` doesn't list is tagged with its MAC address, on its points and its
/// sequence counters, so it shows up before anyone names it.
#[test]
fn an_unnamed_printer_is_tagged_with_its_mac_address() {
    const OTHER: &str = "10:9c:70:00:00:01";
    let mut probe = TelemetryProbe::new();
    let w = worker(&probe);
    let datagram = |msg: u32| {
        format!("<14>1 - {OTHER} buddy - - - msg={msg},tm=5000,v=4 cpu_usage v=73i 0\n")
            .into_bytes()
    };
    let (resource, event) = decode(datagram(1).leak(), RECEIVED_AT);
    let (resource, rewritten) = run_script(&w, resource, event);
    let points = line_protocol(&probe, resource, rewritten).events;
    assert_eq!(points.len(), 1);
    assert_eq!(attr(&points[0], "printer"), Some(OTHER));

    let (resource, event) = decode(datagram(4).leak(), RECEIVED_AT + 1_500_000_000);
    run_script(&w, resource, event);
    assert_eq!(probe.sum("prusa.datagrams.missed", &[("printer", OTHER)]), 2.0);
}

/// With no `vars:` at all, every printer is tagged with its MAC address and the bucket is
/// `prusa`.
#[test]
fn with_no_vars_the_mac_address_and_the_default_bucket_apply() {
    let probe = TelemetryProbe::new();
    let w = worker_with(&probe, &LuaVars::new());
    let (resource, event) = decode(DATAGRAM_1, RECEIVED_AT);
    let (resource, rewritten) = run_script(&w, resource, event);
    assert_eq!(
        resource.attributes.get("_logit.influxdb.bucket").and_then(Value::as_str),
        Some("prusa")
    );
    assert_eq!(attr(&rewritten, "printer"), Some(MAC));
}

/// `vars.bucket` names the bucket the batch carries.
#[test]
fn vars_bucket_names_the_batch_bucket() {
    let probe = TelemetryProbe::new();
    let vars = LuaVars::from([("bucket".to_string(), LuaVar::String("shop".into()))]);
    let w = worker_with(&probe, &vars);
    let (resource, event) = decode(DATAGRAM_1, RECEIVED_AT);
    let (resource, _) = run_script(&w, resource, event);
    assert_eq!(
        resource.attributes.get("_logit.influxdb.bucket").and_then(Value::as_str),
        Some("shop")
    );
}
