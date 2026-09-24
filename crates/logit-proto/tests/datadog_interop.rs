//! Recorded interop fixtures: what a real Datadog Agent 7.83 sent its intake and what a real
//! dd-trace-py 4.15 tracer sent an Agent, replayed through `logit_proto::datadog`'s decoders.
//!
//! **Real bytes from real producers**, not this codec's own encoder, which agrees with itself
//! even where both sides share a misreading of the wire. See
//! `testdata/interop/datadog/README.md` for the provenance table and what each capture settled,
//! and `testdata/interop/README.md` for why the corpus exists; regenerate with
//! `script/record-fixtures datadog`.
//!
//! Replayed at the codec entry points, as `prometheus_remote_write_interop.rs` does, because half
//! of what arrived is in the `.headers` sidecar beside each body: the route is its `path:`, the
//! body form its `Content-Type`, and the compression its `Content-Encoding`, which this file undoes
//! with `ruzstd`/`flate2` as `datadog_in` does. `crates/logit-cli/tests/datadog_in_round_trip.rs`
//! and `datadog_trace_in_round_trip.rs` replay the same requests over a socket.
//!
//! Every capture must decode with nothing `Malformed`, skipped, degraded, or diagnosed, and then
//! sit on the codec's fixed point: `decode(encode(decode(b))) == decode(b)` and
//! `encode(decode(encode(d))) == encode(d)`, the rule `datadog_*_fixed_point.rs` holds hand-written
//! vectors to. Assertions are on decoded, identifiable values (metric names and kinds, a
//! sketch's count and sum, an event's title, a check's message, a log line, span names, stats
//! groups), never on bytes, which change on every re-record.

use bytes::Bytes;
use logit_core::interner::resolve;
use logit_core::{Diagnostics, Event, EventBatch, MetricKind, Registry, Value};
use logit_proto::datadog::events::EventFormat;
use logit_proto::datadog::stats::ATTR_STATS_NAME;
use logit_proto::datadog::{
    DatadogDecoder, DatadogEncoder, ATTR_HOST_NAME, ATTR_INTERVAL, ATTR_TYPE, METRIC_TOP_LEVEL,
};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::{Path, PathBuf};

const RECEIVED_AT: i64 = 1_790_000_000_000_000_000;

fn interop_dir() -> PathBuf {
    // `crates/logit-proto/` -> repository root.
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/interop/datadog")
}

/// One recorded request: its header sidecar and its body, decompressed per `Content-Encoding`.
struct Capture {
    name: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

impl Capture {
    fn read(name: &str) -> Self {
        let dir = interop_dir();
        let raw = std::fs::read(dir.join(format!("{name}.bin")))
            .unwrap_or_else(|e| panic!("reading interop fixture {name}.bin: {e}"));
        let sidecar = std::fs::read_to_string(dir.join(format!("{name}.headers")))
            .unwrap_or_else(|e| panic!("reading interop fixture {name}.headers: {e}"));
        let headers: BTreeMap<String, String> = sidecar
            .lines()
            .filter_map(|line| line.split_once(": "))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let body = match headers.get("content-encoding").map(String::as_str) {
            None | Some("identity") => raw,
            Some("zstd") => {
                let mut out = Vec::new();
                ruzstd::decoding::StreamingDecoder::new(&raw[..])
                    .unwrap_or_else(|e| panic!("{name}: zstd frame: {e}"))
                    .read_to_end(&mut out)
                    .unwrap_or_else(|e| panic!("{name}: zstd body: {e}"));
                out
            }
            Some("gzip") => {
                let mut out = Vec::new();
                flate2::read::GzDecoder::new(&raw[..])
                    .read_to_end(&mut out)
                    .unwrap_or_else(|e| panic!("{name}: gzip: {e}"));
                out
            }
            Some(other) => panic!("{name}: a recorded sender used Content-Encoding {other:?}"),
        };
        Capture { name: name.to_string(), headers, body }
    }

    fn header(&self, name: &str) -> &str {
        self.headers.get(name).map(String::as_str).unwrap_or("")
    }

    /// The request path, query string dropped.
    fn path(&self) -> &str {
        self.header("path").split('?').next().unwrap_or("")
    }
}

/// Every capture whose stem starts with `prefix` and has a body file, in name order.
fn captures(prefix: &str) -> Vec<Capture> {
    let dir = interop_dir();
    let mut stems: Vec<String> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("reading {}: {e}", dir.display()))
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with(prefix) && name.ends_with(".bin"))
        .map(|name| name.trim_end_matches(".bin").to_string())
        .collect();
    stems.sort();
    assert!(!stems.is_empty(), "no capture matching `{prefix}*` under {}", dir.display());
    stems.iter().map(|stem| Capture::read(stem)).collect()
}

/// An `/intake/` body in the Agent's `events` envelope. The recorder keeps no other kind
/// (testdata/interop/datadog/README.md's "Privacy" section), so every recorded one is.
fn is_events_envelope(body: &[u8]) -> bool {
    let value: serde_json::Value = serde_json::from_slice(body).expect("an /intake/ body is JSON");
    let object = value.as_object().expect("an /intake/ body is an object");
    object.contains_key("events")
}

/// A decoder reporting into its own registry, and what that registry says afterwards.
struct Probe {
    registry: std::sync::Arc<Registry>,
    decoder: DatadogDecoder,
}

impl Probe {
    fn new() -> Self {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("dd", "datadog_in", "listener");
        let decoder = DatadogDecoder::new()
            .with_telemetry(telemetry.clone())
            .with_diagnostics(Diagnostics::new("dd").with_telemetry(telemetry));
        Probe { registry, decoder }
    }

    /// Every drop, degrade, or diagnostic the decode recorded: a conforming sender's request
    /// should produce none.
    fn problems(&self) -> Vec<String> {
        self.registry
            .drain(0)
            .iter()
            .filter_map(|event| {
                let key = event.attributes.get("key").and_then(Value::as_str);
                let bad = event.metrics.iter().map(|m| resolve(m.name)).find(|name| {
                    name.contains("skipped")
                        || name.contains("degraded")
                        || name.contains("dropped")
                });
                match (key, bad) {
                    (Some(key), _) => Some(format!("diagnostic {key}")),
                    (None, Some(name)) => Some(format!(
                        "{name}{{reason={:?}}}",
                        event.attributes.get("reason").and_then(Value::as_str)
                    )),
                    (None, None) => None,
                }
            })
            .collect()
    }
}

/// The routes a recorded request can be for, chosen by its `path:` and `Content-Type`.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Route {
    SeriesV2Protobuf,
    Sketches,
    ServiceChecks,
    Events,
    Logs,
    AgentTraces,
    AgentStats,
    TracesV04,
    TracesV05,
    ClientStats,
}

impl Route {
    fn of(capture: &Capture) -> Route {
        match capture.path() {
            "/api/v2/series" => {
                assert_eq!(capture.header("content-type"), "application/x-protobuf");
                Route::SeriesV2Protobuf
            }
            "/api/beta/sketches" => Route::Sketches,
            "/api/v1/check_run" => Route::ServiceChecks,
            "/intake/" => Route::Events,
            "/api/v2/logs" => Route::Logs,
            "/api/v0.2/traces" => Route::AgentTraces,
            "/api/v0.2/stats" => Route::AgentStats,
            "/v0.4/traces" => Route::TracesV04,
            "/v0.5/traces" => Route::TracesV05,
            "/v0.6/stats" => Route::ClientStats,
            other => panic!("{}: no decoder for {other}", capture.name),
        }
    }

    fn decode(self, decoder: &mut DatadogDecoder, body: &[u8]) -> Vec<EventBatch> {
        let one = |r: Result<EventBatch, _>| r.map(|batch| vec![batch]);
        let result = match self {
            Route::SeriesV2Protobuf => one(decoder.decode_series_v2_protobuf(body, RECEIVED_AT)),
            Route::Sketches => one(decoder.decode_sketches(body, RECEIVED_AT)),
            Route::ServiceChecks => one(decoder.decode_service_checks(body, RECEIVED_AT)),
            Route::Events => one(decoder.decode_events(body, RECEIVED_AT)),
            Route::Logs => one(decoder.decode_logs(body, RECEIVED_AT)),
            Route::AgentTraces => decoder.decode_agent_payload(body, RECEIVED_AT),
            Route::AgentStats => decoder.decode_stats_payload(body, RECEIVED_AT),
            Route::TracesV04 => one(decoder.decode_traces_v04(body, RECEIVED_AT)),
            Route::TracesV05 => one(decoder.decode_traces_v05(body, RECEIVED_AT)),
            Route::ClientStats => one(decoder.decode_client_stats_v06(body, RECEIVED_AT)),
        };
        result.unwrap_or_else(|e| panic!("{self:?}: a recorded body must decode: {e}"))
    }

    /// One batch re-encoded on this route; `None` when nothing in it belongs here.
    fn encode(self, batch: &EventBatch) -> Option<Bytes> {
        let mut e = DatadogEncoder::new();
        match self {
            Route::SeriesV2Protobuf => e.encode_series_v2_protobuf(batch),
            Route::Sketches => e.encode_sketches(batch),
            Route::ServiceChecks => e.encode_service_checks(batch),
            Route::Events => {
                let mut bodies = e.encode_events(batch, EventFormat::AgentEnvelope);
                assert!(bodies.len() <= 1, "the Agent envelope is one body");
                bodies.pop()
            }
            Route::Logs => e.encode_logs(batch),
            Route::AgentTraces => e.encode_agent_payload(batch),
            Route::AgentStats => e.encode_stats_payload(batch),
            Route::TracesV04 => e.encode_traces_v04(batch),
            Route::TracesV05 => e.encode_traces_v05(batch),
            Route::ClientStats => e.encode_client_stats_v06(batch),
        }
    }
}

/// Decodes `capture` on its route with nothing dropped, degraded, or diagnosed, and asserts the
/// codec's fixed point from its bytes. Returns the first decode.
fn replay(capture: &Capture) -> Vec<EventBatch> {
    let route = Route::of(capture);
    let mut probe = Probe::new();
    let first = route.decode(&mut probe.decoder, &capture.body);
    let problems = probe.problems();
    assert!(problems.is_empty(), "{}: {problems:?}", capture.name);

    for batch in &first {
        let Some(encoded) = route.encode(batch) else {
            assert!(batch.events.is_empty(), "{}: decoded events didn't re-encode", capture.name);
            continue;
        };
        let second = route.decode(&mut DatadogDecoder::new(), &encoded);
        assert_eq!(
            second,
            std::slice::from_ref(batch),
            "{}: decode(encode(decode(b))) != decode(b)",
            capture.name
        );
        let again = route.encode(&second[0]).expect("the same batch encodes again");
        assert_eq!(again, encoded, "{}: encode(decode(encode(d))) != encode(d)", capture.name);
    }
    first
}

fn events(batches: &[EventBatch]) -> impl Iterator<Item = &Event> {
    batches.iter().flat_map(|b| b.events.iter())
}

fn str_attr<'a>(event: &'a Event, key: &str) -> Option<&'a str> {
    event.attributes.get(key).and_then(Value::as_str)
}

fn metric_name(event: &Event) -> String {
    resolve(event.metrics[0].name).to_string()
}

// -------------------------------------------------------------------------------------------------
// The Agent's intake traffic
// -------------------------------------------------------------------------------------------------

/// The capture holds every route this file replays, so a re-record that loses one fails here
/// rather than passing vacuously.
#[test]
fn every_recorded_agent_request_decodes_cleanly_and_sits_on_the_fixed_point() {
    let mut routes = BTreeSet::new();
    for capture in captures("agent-") {
        match capture.path() {
            // Probes: no body, answered by `datadog_in` without a decode.
            "/api/v1/validate" | "/api/v2/validate" | "/_health" => {
                assert!(capture.body.is_empty(), "{}", capture.name)
            }
            "/intake/" => {
                assert!(is_events_envelope(&capture.body), "{}", capture.name);
                replay(&capture);
                routes.insert(capture.path().to_string());
            }
            _ => {
                replay(&capture);
                routes.insert(capture.path().to_string());
            }
        }
    }
    let expected = [
        "/api/v2/series",
        "/api/beta/sketches",
        "/api/v1/check_run",
        "/intake/",
        "/api/v2/logs",
        "/api/v0.2/traces",
        "/api/v0.2/stats",
    ];
    assert_eq!(routes, expected.iter().map(|s| s.to_string()).collect());
}

/// Every DogStatsD construct the clients sent reached the series route, with its type: a counter
/// is a `rate` over the Agent's 10 s flush interval, a histogram or timer is its aggregates, and
/// a set is a gauge of its size. A gauge with `|T` is a series of its own, one point per
/// transport, at the client's timestamp.
#[test]
fn the_agent_series_carry_the_dogstatsd_clients_metrics() {
    let series: Vec<EventBatch> =
        captures("agent-api-v2-series-").iter().flat_map(replay).collect();
    let by_name: BTreeMap<String, &Event> =
        events(&series).map(|event| (metric_name(event), event)).collect();

    let rate = by_name["record.requests.count"];
    assert!(matches!(rate.metrics[0].kind, MetricKind::Gauge(_)), "{rate:?}");
    assert_eq!(str_attr(rate, ATTR_TYPE), Some("rate"));
    assert_eq!(rate.attributes.get(ATTR_INTERVAL), Some(&Value::I64(10)));
    assert_eq!(str_attr(rate, ATTR_HOST_NAME), Some("record-fixtures"));
    assert_eq!(str_attr(rate, "endpoint"), Some("/checkout"));
    // The client's `env:record` twice (its own tag and `DD_ENV`'s) is one tag by the time the
    // Agent sends it; the decoder's own exact-duplicate dedupe agrees.
    assert_eq!(str_attr(rate, "env"), Some("record"));

    let gauge = by_name["record.queue.depth"];
    assert_eq!(gauge.metrics[0].kind, MetricKind::Gauge(17.0));
    assert_eq!(str_attr(gauge, ATTR_TYPE), None, "a plain gauge carries no type carrier");

    for aggregate in ["avg", "count", "max", "median", "95percentile"] {
        for timer in ["record.request.duration", "record.db.query.duration"] {
            let name = format!("{timer}.{aggregate}");
            assert!(by_name.contains_key(&name), "{name} missing: {:?}", by_name.keys());
        }
    }
    assert_eq!(by_name["record.users.active"].metrics[0].kind, MetricKind::Gauge(1.0));

    let lagged: Vec<&Event> =
        events(&series).filter(|e| metric_name(e) == "record.batch.lag").collect();
    assert_eq!(lagged.len(), 3, "one `|T` gauge per transport");
    for event in lagged {
        assert_eq!(event.timestamp, 1_790_000_000_000_000_000, "the client's own timestamp");
        assert_eq!(event.metrics[0].kind, MetricKind::Gauge(2.5));
    }
}

/// The distribution, sent once over each of the three transports, is one sketch of three.
#[test]
fn the_agent_sketch_carries_the_distribution() {
    let batches: Vec<EventBatch> =
        captures("agent-api-beta-sketches-").iter().flat_map(replay).collect();
    let sketch = events(&batches)
        .find(|e| metric_name(e) == "record.response.size")
        .expect("the distribution's sketch");
    let MetricKind::Distribution(sketch) = &sketch.metrics[0].kind else {
        panic!("a sketch decodes to a Distribution: {sketch:?}");
    };
    assert_eq!(sketch.count(), 3);
    assert_eq!(sketch.sum(), 3.0 * 1830.0);
    assert_eq!(sketch.min(), Some(1830.0));
}

/// The service check's message is `slow upstream`, not `slow upstream|c:...|card:low`: the
/// Agent ends `m:` at the next `|`, which is what `statsd_in` now does.
#[test]
fn the_agent_service_check_ends_its_message_at_the_next_pipe() {
    let batches: Vec<EventBatch> =
        captures("agent-api-v1-check-run-").iter().flat_map(replay).collect();
    let checks: Vec<&Event> =
        events(&batches).filter(|e| metric_name(e) == "record.can_connect").collect();
    assert_eq!(checks.len(), 3, "one per transport");
    for check in checks {
        assert_eq!(check.metrics[0].kind, MetricKind::Gauge(1.0), "status WARNING");
        assert_eq!(str_attr(check, "statsd.service_check.message"), Some("slow upstream"));
        assert_eq!(str_attr(check, "statsd.service_check.host"), Some("record-host"));
    }
    assert!(events(&batches).any(|e| metric_name(e) == "datadog.agent.up"));
}

/// DogStatsD events travel in `/intake/`'s envelope, grouped under their source type; so does
/// the Agent's own startup event.
#[test]
fn the_agent_events_arrive_in_the_intake_envelope() {
    let batches: Vec<EventBatch> = captures("agent-intake-").iter().flat_map(replay).collect();
    let titles: Vec<&str> =
        events(&batches).filter_map(|e| str_attr(e, "statsd.event.title")).collect();
    assert!(titles.contains(&"Agent Startup"), "{titles:?}");
    let deploys = titles.iter().filter(|t| **t == "Deploy finished").count();
    assert_eq!(deploys, 3, "one per transport: {titles:?}");
    let deploy = events(&batches)
        .find(|e| str_attr(e, "statsd.event.title") == Some("Deploy finished"))
        .unwrap();
    let log = deploy.log.as_ref().expect("an event is a log");
    assert_eq!(log.message.as_str(), Some("record-fixtures 1.2.3 is live"));
    assert_eq!(str_attr(deploy, "statsd.event.aggregation_key"), Some("deploy-123"));
}

/// The tailed file's three lines, the JSON one kept as a string: the Agent parses nothing.
#[test]
fn the_agent_logs_carry_the_tailed_lines() {
    let batches: Vec<EventBatch> = captures("agent-api-v2-logs-").iter().flat_map(replay).collect();
    let messages: Vec<&str> =
        events(&batches).filter_map(|e| e.log.as_ref().and_then(|l| l.message.as_str())).collect();
    assert_eq!(messages.len(), 3, "{messages:?}");
    assert!(messages[0].ends_with("record-fixtures started on port 5050"));
    assert!(messages[1].starts_with(r#"{"timestamp":"2026-09-24T10:00:01Z""#));
    assert!(messages[2].contains("request failed: status=500"));
}

/// The Agent's trace payload: three Flask traces, each root marked `_top_level` by the Agent,
/// and the app's own child span among them.
#[test]
fn the_agent_trace_payload_carries_the_app_s_traces_marked_top_level() {
    let batches = replay(&Capture::read("agent-api-v0-2-traces-000"));
    let spans: Vec<&Event> = events(&batches).filter(|e| e.span.is_some()).collect();
    let roots: Vec<&Event> = spans
        .iter()
        .copied()
        .filter(|e| e.span.as_ref().unwrap().parent_span_id.is_none())
        .collect();
    assert_eq!(roots.len(), 3, "one trace per request");
    for root in &roots {
        assert_eq!(root.span.as_ref().unwrap().name.as_str(), Some("flask.request"));
        assert_eq!(root.attributes.get(METRIC_TOP_LEVEL), Some(&Value::F64(1.0)));
    }
    assert!(spans.iter().any(|e| e.span.as_ref().unwrap().name.as_str() == Some("orders.lookup")));
    assert!(spans.len() > roots.len() * 5, "a Flask request is many spans: {}", spans.len());
}

/// The Agent computed the stats itself: one group per request, all `flask.request`.
#[test]
fn the_agent_stats_payload_carries_one_group_per_request() {
    let batches = replay(&Capture::read("agent-api-v0-2-stats-000"));
    let names: Vec<&str> = events(&batches).filter_map(|e| str_attr(e, ATTR_STATS_NAME)).collect();
    assert_eq!(names, ["flask.request"; 3]);
}

// -------------------------------------------------------------------------------------------------
// A tracer talking to an Agent
// -------------------------------------------------------------------------------------------------

/// What dd-trace-py 4.15 sent: v0.5 by default, v0.4 when asked, and `/v0.6/stats` only against
/// an `/info` that allows priority-0 dropping. Every body decodes cleanly and re-encodes.
#[test]
fn every_recorded_tracer_request_decodes_cleanly_and_sits_on_the_fixed_point() {
    let mut paths = BTreeSet::new();
    for capture in captures("tracer-") {
        paths.insert(capture.path().to_string());
        if capture.path() == "/info" {
            assert_eq!(capture.header("method"), "GET");
            continue;
        }
        let batches = replay(&capture);
        assert!(events(&batches).next().is_some(), "{} decoded to nothing", capture.name);
    }
    let expected = ["/info", "/v0.4/traces", "/v0.5/traces", "/v0.6/stats"];
    assert_eq!(paths, expected.iter().map(|s| s.to_string()).collect());
}

/// `X-Datadog-Trace-Count` agrees with the traces on the wire, and the tracer identifies itself
/// only in headers on the trace routes.
#[test]
fn the_tracer_s_trace_count_matches_its_traces() {
    for (name, traces) in [("tracer-v0-5-traces-000", 3), ("tracer-v04-v0-4-traces-000", 1)] {
        let capture = Capture::read(name);
        assert_eq!(capture.header("x-datadog-trace-count"), traces.to_string(), "{name}");
        assert_eq!(capture.header("datadog-meta-lang"), "python", "{name}");
        assert_eq!(capture.header("content-type"), "application/msgpack", "{name}");
        let batches = replay(&capture);
        let roots = events(&batches)
            .filter(|e| e.span.as_ref().is_some_and(|s| s.parent_span_id.is_none()))
            .count();
        assert_eq!(roots, traces, "{name}");
    }
}

/// libdatadog's client stats leave `Lang` and `TracerVersion` empty in the payload and send them
/// as headers; `datadog_trace_in` fills them from the headers as the Agent does. The sketches use
/// libdatadog's own mapping (gamma 1.015625 with an index offset), not the Agent's 1.0202.
#[test]
fn the_tracer_s_client_stats_name_the_tracer_only_in_headers() {
    let capture = Capture::read("tracer-v04-v0-6-stats-000");
    assert_eq!(capture.header("datadog-meta-lang"), "python");
    assert!(!capture.header("datadog-meta-tracer-version").is_empty());
    let batches = replay(&capture);
    let batch = &batches[0];
    assert_eq!(batch.resource.attributes.get("datadog.tracer.language_name"), None);
    let group = events(&batches).find(|e| str_attr(e, ATTR_STATS_NAME).is_some()).unwrap();
    assert_eq!(str_attr(group, ATTR_STATS_NAME), Some("flask.request"));
    let summary = group
        .metrics
        .iter()
        .find_map(|m| match &m.kind {
            MetricKind::Distribution(sketch) if sketch.count() > 0 => Some(sketch),
            _ => None,
        })
        .expect("the ok summary");
    assert_ne!(summary.mapping().gamma(), 1.0202020202020203, "not the Agent's mapping");
}
