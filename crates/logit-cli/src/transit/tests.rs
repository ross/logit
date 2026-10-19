use super::*;
use logit_pipeline::test_util::scratch_dir;
use serde_json::{json, Value};
use std::path::PathBuf;

/// `statsd-kinds.json`: what `statsd_in` decodes from a counter, a gauge, a timer, and a set
/// member (`requests:3|c|#route:/api`, `temperature:21.5|g|#room:lab`,
/// `latency:12|ms|#route:/api`, `users:alice|s`) received at `2027-01-15T08:00:00Z`.
const CASE: &str = "statsd-kinds.json";

/// The default clock: the case's one timestamp plus one second. A statsd line carries no
/// timestamp, so every re-decoded event reads back at it.
const AT: &str = "2027-01-15T08:00:01.000000000Z";

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/transit").join(name)
}

/// The run's document, parsed, so a test compares values rather than spelling.
fn document(flow: &Path) -> Value {
    let outcome = run(flow, &fixture(CASE), Options::default()).expect("the flow runs");
    serde_json::from_str(&outcome.document()).expect("the document is JSON")
}

/// A flow written to a scratch directory, for a config the fixtures don't hold.
fn flow(label: &str, yaml: &str) -> PathBuf {
    let path = scratch_dir(label).join("flow.yaml");
    std::fs::write(&path, yaml).expect("write the flow");
    path
}

fn metric_event(metric: Value, attributes: Option<Value>) -> Value {
    let mut event = json!({"timestamp": AT, "metrics": [metric]});
    if let Some(attributes) = attributes {
        event["attributes"] = attributes;
    }
    event
}

/// A like pair under DogStatsD: every kind `statsd_in` produces reads back as the same kind and
/// value, the timer's `ms` carrier included, and nothing is counted.
#[test]
fn a_statsd_relay_reads_every_kind_back_unchanged() {
    let route = Some(json!({"route": "/api"}));
    let want = json!({
        "events": [
            metric_event(
                json!({"name": "latency", "kind": "samples", "values": [12.0], "sample_rate": 1.0}),
                Some(json!({"route": "/api", "_logit.statsd.type": "ms"})),
            ),
            metric_event(
                json!({"name": "requests", "kind": "sum", "value": 3.0,
                       "temporality": "delta", "monotonic": true}),
                route,
            ),
            metric_event(
                json!({"name": "temperature", "kind": "gauge", "value": 21.5}),
                Some(json!({"room": "lab"})),
            ),
            metric_event(json!({"name": "users", "kind": "set_members", "members": ["alice"]}), None),
        ],
        "counters": {},
    });
    assert_eq!(document(&fixture("statsd-relay.yaml")), want);
}

/// Through a 10 s `aggregate`, the timer becomes a distribution and the set member a set, and
/// `multi_value: expand` (the default) writes each as dotted counter and gauge lines, counted
/// degraded once per record. The counter and the gauge pass through unchanged.
#[test]
fn aggregate_then_statsd_out_expands_a_distribution_and_a_set_counted_degraded() {
    let route = || Some(json!({"route": "/api"}));
    let gauge = |name: &str, value: f64, attributes| {
        metric_event(json!({"name": name, "kind": "gauge", "value": value}), attributes)
    };
    let sum = |name: &str, value: f64, attributes| {
        metric_event(
            json!({"name": name, "kind": "sum", "value": value,
                   "temporality": "delta", "monotonic": true}),
            attributes,
        )
    };
    let want = json!({
        "events": [
            sum("latency.count", 1.0, route()),
            gauge("latency.max", 12.0, route()),
            gauge("latency.min", 12.0, route()),
            gauge("latency.q0_5", 12.0, route()),
            gauge("latency.q0_75", 12.0, route()),
            gauge("latency.q0_9", 12.0, route()),
            gauge("latency.q0_95", 12.0, route()),
            gauge("latency.q0_99", 12.0, route()),
            sum("latency.sum", 12.0, route()),
            sum("requests", 3.0, route()),
            gauge("temperature", 21.5, Some(json!({"room": "lab"}))),
            gauge("users.count", 1.0, None),
        ],
        "counters": {
            "logit.component.events.discarded{component=window,kind=aggregate,role=transform}": 4,
            "logit.output.metrics.degraded{component=out,kind=statsd_out,metric_kind=distribution,role=sink}": 1,
            "logit.output.metrics.degraded{component=out,kind=statsd_out,metric_kind=set,role=sink}": 1,
        },
    });
    let flow = fixture("statsd-aggregate.yaml");
    assert_eq!(document(&flow), want);
    assert_eq!(document(&flow), want, "a second run reads the same");
}

/// `statsd_out`'s `format:` and `multi_value:` reach the encoder the driver builds: plain statsd
/// drops every tag, counted, and `skip` writes no line for a distribution or a set, counted.
#[test]
fn statsd_out_options_reach_the_driver_encoder() {
    let flow = flow(
        "transit-statsd-options",
        "components:
  case: {type: replay_in, path: case.json}
  window: {type: aggregate, sources: [case], interval: 10s}
  out:
    type: statsd_out
    sources: [window]
    endpoint: 127.0.0.1:8125
    format: statsd
    multi_value: skip
",
    );
    let want = json!({
        "events": [
            metric_event(
                json!({"name": "requests", "kind": "sum", "value": 3.0,
                       "temporality": "delta", "monotonic": true}),
                None,
            ),
            metric_event(json!({"name": "temperature", "kind": "gauge", "value": 21.5}), None),
        ],
        "counters": {
            "logit.component.events.discarded{component=window,kind=aggregate,role=transform}": 4,
            "logit.output.metrics.skipped{component=out,kind=statsd_out,metric_kind=distribution,role=sink}": 1,
            "logit.output.metrics.skipped{component=out,kind=statsd_out,metric_kind=set,role=sink}": 1,
            // The encoder walks a record's tags before it skips the record, so the distribution's
            // `route` counts beside the two written records' tags.
            "logit.output.tags.omitted{component=out,kind=statsd_out,reason=dialect,role=sink}": 3,
        },
    });
    assert_eq!(document(&flow), want);
}

/// Each shape the driver can't run fails with an error naming the component that breaks it.
#[test]
fn the_driver_rejects_a_flow_it_cant_run_naming_the_component() {
    let sink = "type: statsd_out, endpoint: 127.0.0.1:8125";
    let cases = [
        (
            "fan-out",
            format!(
                "components:
  case: {{type: replay_in, path: case.json}}
  a: {{{sink}, sources: [case]}}
  b: {{{sink}, sources: [case]}}
"
            ),
            "component 'case': a flow can't fan out",
        ),
        (
            "lua",
            format!(
                "components:
  case: {{type: replay_in, path: case.json}}
  script: {{type: lua, sources: [case], script: 'function process(event) return event end'}}
  out: {{{sink}, sources: [script]}}
"
            ),
            "component 'script': the driver doesn't run a lua",
        ),
        (
            "native",
            format!(
                "components:
  case: {{type: replay_in, path: case.bin, format: native}}
  out: {{{sink}, sources: [case]}}
"
            ),
            "component 'case': a flow's replay_in reads format: json",
        ),
        (
            "listener",
            format!(
                "components:
  case: {{type: statsd_in, bind: '127.0.0.1:0'}}
  out: {{{sink}, sources: [case]}}
"
            ),
            "component 'case': a flow's input is a replay_in, not statsd_in",
        ),
        (
            "sink",
            "components:
  case: {type: replay_in, path: case.json}
  out: {type: null_out, sources: [case]}
"
            .to_string(),
            "null_out has no encoder this driver reaches",
        ),
    ];
    for (label, yaml, want) in cases {
        let flow = flow(&format!("transit-reject-{label}"), &yaml);
        let err = run(&flow, &fixture(CASE), Options::default()).expect_err(label);
        assert!(format!("{err:#}").contains(want), "{label}: {err:#}");
    }
}

/// Every other encoder shape runs end to end: the case's gauge reads back as a gauge holding
/// 21.5 through each sink and its pair's decoder.
#[test]
fn every_sink_shape_round_trips_the_gauge() {
    let sinks = [
        (
            "influxdb",
            "type: influxdb_out, url: 'http://127.0.0.1:8086', bucket: b, org: o, token: t",
        ),
        ("otlp", "type: otlp_out, endpoint: 'http://127.0.0.1:4318'"),
        ("expose", "type: prometheus_out, bind: '127.0.0.1:0'"),
        ("remote-write", "type: prometheus_out, endpoint: 'http://127.0.0.1:9090/api/v1/write'"),
        (
            "remote-write-2",
            "type: prometheus_out, endpoint: 'http://127.0.0.1:9090/api/v1/write', version: 2",
        ),
    ];
    for (label, sink) in sinks {
        let yaml = format!(
            "components:\n  case: {{type: replay_in, path: case.json}}\n  out: {{{sink}, sources: [case]}}\n"
        );
        let flow = flow(&format!("transit-shape-{label}"), &yaml);
        let outcome = run(&flow, &fixture(CASE), Options::default()).expect(label);
        let doc: Value = serde_json::from_str(&outcome.document()).expect("JSON");
        let gauge = doc["events"].as_array().expect("events").iter().any(|event| {
            event["metrics"][0]["kind"] == "gauge" && event["metrics"][0]["value"] == 21.5
        });
        assert!(gauge, "{label}: {doc}");
    }
}
