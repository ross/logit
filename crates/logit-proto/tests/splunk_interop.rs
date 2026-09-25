//! Recorded interop fixtures: what four real HEC clients sent (the OpenTelemetry Collector
//! contrib 0.161.0 `splunk_hec` exporter, Docker 29.8.1's `splunk` log driver, Splunk Connect for
//! Syslog 3.40.0, and splunk-library-javalogging 1.11.11's Logback appenders), replayed through
//! `logit_proto::splunk`'s decoders.
//!
//! **Real bytes from real producers**, not this codec's own encoder, which agrees with itself
//! even where both sides share a misreading of the wire. See
//! `testdata/interop/splunk/README.md` for the provenance table and `testdata/interop/README.md`
//! for why the corpus exists; regenerate with `script/record-fixtures splunk`.
//!
//! Replayed at the codec entry points, as `datadog_interop.rs` replays its own corpus: the route
//! is each capture's `path:` (query string included for `/raw`), decompressed per its
//! `Content-Encoding` sidecar line. `crates/logit-cli/tests/splunk_hec_in_round_trip.rs` replays
//! the same requests over a socket.
//!
//! Every capture must decode with nothing skipped, degraded, or diagnosed, and every `/event`
//! capture then sits on the codec's fixed point:
//! `decode(encode(decode(b))) == decode(b)` and `encode` stable on the second hop, the rule
//! `splunk_fixed_point.rs` holds a hand-written and generated corpus to. Assertions are on
//! decoded, identifiable values (span kinds and names, a log's severity and trace reference, a
//! metric's name and kind, an attribute), never on whole-file bytes.

use logit_core::interner::resolve;
use logit_core::trace::to_hex;
use logit_core::{
    Diagnostics, Event, EventBatch, MetricKind, Registry, Severity, SpanKind, SpanStatus, Sum,
    Temporality, Value,
};
use logit_proto::splunk::time::parse_hec_time;
use logit_proto::splunk::{Envelope, SplunkDecoder, SplunkEncoder};
use logit_proto::Encoder;
use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};

const RECEIVED_AT: i64 = 1_790_000_000_000_000_000;

fn interop_dir() -> PathBuf {
    // `crates/logit-proto/` -> repository root.
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/interop/splunk")
}

/// One recorded request: its header sidecar (`method`, `path`, and every request header, all
/// lowercased keys) and its body, decompressed per `Content-Encoding`.
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

    fn method(&self) -> &str {
        self.header("method")
    }

    /// The request path, query string dropped.
    fn route_path(&self) -> &str {
        self.header("path").split('?').next().unwrap_or("")
    }

    /// The query string, when the recorded path had one.
    fn query(&self) -> Option<&str> {
        self.header("path").split_once('?').map(|(_, q)| q)
    }
}

/// Every capture under the corpus, in name order.
fn every_capture() -> Vec<Capture> {
    let dir = interop_dir();
    let mut stems: Vec<String> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("reading {}: {e}", dir.display()))
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".bin"))
        .map(|name| name.trim_end_matches(".bin").to_string())
        .collect();
    stems.sort();
    stems.iter().map(|stem| Capture::read(stem)).collect()
}

fn decode_file(name: &str) -> Vec<EventBatch> {
    let capture = Capture::read(name);
    SplunkDecoder::new()
        .decode_events(&capture.body, RECEIVED_AT)
        .unwrap_or_else(|e| panic!("{name}: a recorded body must decode: {e}"))
}

/// A `/raw` capture's query string, minimally percent-decoded (only `%XX` occurs in this corpus).
fn envelope_of(capture: &Capture) -> Envelope {
    let mut envelope = Envelope::default();
    let Some(query) = capture.query() else { return envelope };
    for pair in query.split('&') {
        let Some((key, value)) = pair.split_once('=') else { continue };
        let value = percent_decode(value);
        match key {
            "host" => envelope.host = Some(value),
            "source" => envelope.source = Some(value),
            "sourcetype" => envelope.sourcetype = Some(value),
            "index" => envelope.index = Some(value),
            _ => {}
        }
    }
    envelope
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(byte) =
                u8::from_str_radix(std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or(""), 16)
            {
                out.push(byte);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// A body's top-level objects (concatenated or, unused in this corpus, an array), as
/// `serde_json::Value`, in wire order.
fn concatenated_objects(body: &[u8]) -> Vec<serde_json::Value> {
    serde_json::Deserializer::from_slice(body)
        .into_iter::<serde_json::Value>()
        .map(|r| r.expect("a recorded body is valid JSON"))
        .collect()
}

/// `value`'s `key` member, when `value` is a `Map`.
fn map_field<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    match value {
        Value::Map(map) => map.get(key),
        _ => None,
    }
}

// -------------------------------------------------------------------------------------------------
// A decoder reporting into its own registry
// -------------------------------------------------------------------------------------------------

struct Probe {
    registry: std::sync::Arc<Registry>,
    decoder: SplunkDecoder,
}

impl Probe {
    fn new() -> Self {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("splunk", "splunk_hec_in", "listener");
        let decoder = SplunkDecoder::new()
            .with_telemetry(telemetry.clone())
            .with_diagnostics(Diagnostics::new("splunk").with_telemetry(telemetry));
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
                let bad = event
                    .metrics
                    .iter()
                    .map(|m| resolve(m.name))
                    .find(|name| name.contains("skipped") || name.contains("degraded"));
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

/// The two routes a `POST` capture in this corpus can be for.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Route {
    Events,
    Raw,
}

impl Route {
    fn of(capture: &Capture) -> Route {
        if capture.route_path().starts_with("/services/collector/raw") {
            Route::Raw
        } else {
            Route::Events
        }
    }

    fn decode(self, decoder: &mut SplunkDecoder, capture: &Capture) -> Vec<EventBatch> {
        match self {
            Route::Events => decoder
                .decode_events(&capture.body, RECEIVED_AT)
                .unwrap_or_else(|e| panic!("{}: a recorded body must decode: {e}", capture.name)),
            Route::Raw => {
                vec![decoder.decode_raw(&capture.body, &envelope_of(capture), RECEIVED_AT)]
            }
        }
    }
}

// -------------------------------------------------------------------------------------------------
// Every capture decodes cleanly and, on `/event`, sits on the fixed point
// -------------------------------------------------------------------------------------------------

/// The whole corpus, so a re-record that loses a file fails here rather than passing vacuously.
#[test]
fn the_corpus_has_every_recorded_capture() {
    assert_eq!(every_capture().len(), 32, "testdata/interop/splunk/*.bin");
}

/// Every `POST` body decodes through its route with nothing skipped, degraded, or diagnosed. A
/// `GET`/`OPTIONS` capture (the exporter's health probe, Docker's driver-startup preflight) has no
/// body to decode.
#[test]
fn every_recorded_post_request_decodes_with_nothing_skipped_or_degraded() {
    let mut posts = 0;
    for capture in every_capture() {
        if capture.method() != "POST" {
            assert!(capture.body.is_empty(), "{}: a GET/OPTIONS capture has no body", capture.name);
            continue;
        }
        posts += 1;
        let route = Route::of(&capture);
        let mut probe = Probe::new();
        let batches = route.decode(&mut probe.decoder, &capture);
        assert!(!batches.is_empty(), "{}: decoded to nothing", capture.name);
        let problems = probe.problems();
        assert!(problems.is_empty(), "{}: {problems:?}", capture.name);
    }
    assert_eq!(posts, 28, "every POST capture in the corpus");
}

/// Every `/event` capture (not `/raw`, which has no encoder counterpart of its own) sits on the
/// codec's fixed point: re-encoding and decoding again gives the same batches back, and the
/// second encode is byte-identical to the first.
#[test]
fn every_event_capture_sits_on_the_fixed_point() {
    let mut checked = 0;
    for capture in every_capture() {
        if capture.method() != "POST" || Route::of(&capture) != Route::Events {
            continue;
        }
        checked += 1;
        let d1 = SplunkDecoder::new().decode_events(&capture.body, RECEIVED_AT).unwrap();
        let mut e1 = Vec::new();
        for batch in &d1 {
            e1.extend_from_slice(&SplunkEncoder::new().encode(batch).unwrap());
        }
        let d2 = SplunkDecoder::new().decode_events(&e1, RECEIVED_AT).unwrap();
        assert_eq!(d1, d2, "{}: decode(encode(decode(b))) != decode(b)", capture.name);
        let mut e2 = Vec::new();
        for batch in &d2 {
            e2.extend_from_slice(&SplunkEncoder::new().encode(batch).unwrap());
        }
        assert_eq!(e1, e2, "{}: encode(decode(encode(d))) != encode(d)", capture.name);
    }
    assert_eq!(checked, 24, "every /event capture in the corpus");
}

// -------------------------------------------------------------------------------------------------
// The OpenTelemetry Collector contrib 0.161.0 `splunk_hec` exporter
// -------------------------------------------------------------------------------------------------

/// The exporter's span batch: 3 traces, each a `SPAN_KIND_CLIENT` root `lets-go` with no parent
/// and a `SPAN_KIND_SERVER` child `okey-dokey-0` whose parent is the root, both `STATUS_CODE_UNSET`.
/// Re-encoding writes each object's `event` and `fields` members back equal, as parsed JSON, to
/// the recorded ones: the exporter's own `SPAN_KIND_*`/`STATUS_CODE_*` spellings and member set.
#[test]
fn otel_exporter_spans_decode_to_six_records_across_three_traces() {
    let capture = Capture::read("otel-services-collector-000");
    let batches = SplunkDecoder::new().decode_events(&capture.body, RECEIVED_AT).unwrap();
    assert_eq!(batches.len(), 1, "one resource");
    let batch = &batches[0];
    assert_eq!(batch.events.len(), 6);

    let resource = &batch.resource.attributes;
    assert_eq!(resource.get("service.name"), Some(&Value::str("splunk-otel-fixture")));
    assert_eq!(resource.get("host.name"), Some(&Value::str("unknown")));
    assert_eq!(resource.get("com.splunk.source"), Some(&Value::str("otel:telemetrygen")));
    assert_eq!(resource.get("com.splunk.sourcetype"), Some(&Value::str("otel:traces")));
    assert_eq!(resource.get("com.splunk.index"), Some(&Value::str("main")));

    let mut by_trace: BTreeMap<String, Vec<&Event>> = BTreeMap::new();
    for event in &batch.events {
        let span = event.span.as_ref().expect("every event in this capture is a span");
        by_trace.entry(to_hex(&span.trace_id)).or_default().push(event);
    }
    assert_eq!(by_trace.len(), 3, "three traces");
    for events in by_trace.values() {
        assert_eq!(events.len(), 2);
        let root = events
            .iter()
            .find(|e| e.span.as_ref().unwrap().parent_span_id.is_none())
            .expect("a root");
        let child = events
            .iter()
            .find(|e| e.span.as_ref().unwrap().parent_span_id.is_some())
            .expect("a child");
        let root_span = root.span.as_ref().unwrap();
        let child_span = child.span.as_ref().unwrap();
        assert_eq!(root_span.kind, SpanKind::Client);
        assert_eq!(root_span.name, Value::str("lets-go"));
        assert_eq!(child_span.kind, SpanKind::Server);
        assert_eq!(child_span.name, Value::str("okey-dokey-0"));
        assert_eq!(
            child_span.parent_span_id,
            Some(root_span.span_id),
            "the child's parent is the root"
        );
        for event in events {
            assert_eq!(event.span.as_ref().unwrap().status, SpanStatus::Unset);
        }
    }

    let recorded = concatenated_objects(&capture.body);
    assert_eq!(recorded.len(), 6);
    let mut encoder = SplunkEncoder::new();
    let encoded_body = encoder.encode(batch).unwrap();
    let encoded = concatenated_objects(&encoded_body);
    assert_eq!(encoded.len(), 6);
    for (want, got) in recorded.iter().zip(encoded.iter()) {
        assert_eq!(want["event"], got["event"], "the exporter's span object comes back equal");
        assert_eq!(want.get("fields"), got.get("fields"));
    }
}

/// Both metric forms (`otel:metrics`, one object per record; `otel:metrics:multi`, `_sum` and
/// `_count` sharing an object) decode to the same records; `Sum` decodes cumulative monotonic;
/// `Histogram` keeps `metric_type` as an attribute, with `le` on every bucket record.
#[test]
fn otel_exporter_metrics_decode_in_both_forms() {
    fn gauge_records(batches: &[EventBatch]) -> Vec<(String, f64)> {
        let mut out: Vec<(String, f64)> = batches
            .iter()
            .flat_map(|b| &b.events)
            .flat_map(|e| &e.metrics)
            .map(|m| match &m.kind {
                MetricKind::Gauge(v) => (resolve(m.name).to_string(), *v),
                other => panic!("expected a Gauge, got {other:?}"),
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.partial_cmp(&b.1).unwrap()));
        out
    }

    let default_form = decode_file("otel-services-collector-002");
    let multi_form = decode_file("otel-services-collector-003");
    assert_eq!(gauge_records(&default_form), gauge_records(&multi_form), "the Gauge records agree");
    assert_eq!(
        gauge_records(&default_form),
        vec![("gen".into(), 0.0), ("gen".into(), 1.0), ("gen".into(), 2.0)]
    );

    for name in ["otel-services-collector-004", "otel-services-collector-005"] {
        let batches = decode_file(name);
        for event in batches.iter().flat_map(|b| &b.events) {
            for metric in &event.metrics {
                assert!(
                    matches!(
                        metric.kind,
                        MetricKind::Sum(Sum {
                            temporality: Temporality::Cumulative,
                            monotonic: true,
                            ..
                        })
                    ),
                    "{name}: {:?}",
                    metric.kind
                );
            }
        }
    }

    for name in ["otel-services-collector-006", "otel-services-collector-007"] {
        let batches = decode_file(name);
        let mut saw_bucket = false;
        for event in batches.iter().flat_map(|b| &b.events) {
            assert_eq!(
                event.attributes.get("metric_type"),
                Some(&Value::str("Histogram")),
                "{name}: metric_type stays an attribute"
            );
            if event.attributes.get("le").is_some() {
                saw_bucket = true;
            }
        }
        assert!(saw_bucket, "{name}: at least one bucket record carries le");
    }
}

/// The exporter's log: `Warn` from `otel.log.severity.number` 13's band, both severity attributes
/// kept, the trace reference consumed, `fixture.tags` an untouched array.
#[test]
fn otel_exporter_log_decodes_with_warn_severity_and_a_trace_ref() {
    let batches = decode_file("otel-services-collector-001");
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.events.len(), 3);
    for event in &batch.events {
        let log = event.log.as_ref().expect("a log event");
        assert_eq!(log.message, Value::str("the message"));
        assert_eq!(log.severity, Some(Severity::Warn));
        let trace = log.trace.expect("a trace ref");
        assert_eq!(to_hex(&trace.trace_id), "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(trace.span_id.map(|s| to_hex(&s)), Some("b7ad6b7169203331".to_string()));
        assert_eq!(event.attributes.get("otel.log.severity.text"), Some(&Value::str("Warn")));
        assert_eq!(event.attributes.get("otel.log.severity.number"), Some(&Value::I64(13)));
        assert_eq!(event.attributes.get("app"), Some(&Value::str("server")));
        assert!(
            matches!(event.attributes.get("fixture.tags"), Some(Value::Array(_))),
            "fixture.tags stays an array, not flattened"
        );
        assert_eq!(event.attributes.get("trace_id"), None, "consumed into the trace ref");
        assert_eq!(event.attributes.get("span_id"), None, "consumed into the trace ref");
    }
}

/// The exporter's `/raw` body: three lines, each `the message`.
#[test]
fn otel_exporter_raw_decodes_to_three_logs() {
    let capture = Capture::read("otel-services-collector-raw-000");
    let batch = SplunkDecoder::new().decode_raw(&capture.body, &Envelope::default(), RECEIVED_AT);
    assert_eq!(batch.events.len(), 3);
    for event in &batch.events {
        assert_eq!(event.log.as_ref().unwrap().message, Value::str("the message"));
    }
}

// -------------------------------------------------------------------------------------------------
// Docker 29.8.1's `splunk` log driver
// -------------------------------------------------------------------------------------------------

/// Docker's three `splunk-format` settings: `inline`'s first line is JSON text kept as a string,
/// its second a plain string; `json`'s first line is a `Map`; `raw`'s event is a string starting
/// with the container's short id (Docker's own tag). The string `time` decodes to the recorded
/// instant.
#[test]
fn docker_log_driver_formats_decode() {
    let inline = Capture::read("docker-services-collector-event-1-0-001");
    let inline_batches = SplunkDecoder::new().decode_events(&inline.body, RECEIVED_AT).unwrap();
    assert_eq!(inline_batches.len(), 1);
    let events = &inline_batches[0].events;
    assert_eq!(events.len(), 2);
    let line0 = map_field(&events[0].log.as_ref().unwrap().message, "line").unwrap();
    let text0 = line0.as_str().expect("inline's first line is a Str");
    assert!(text0.starts_with('{'), "holding JSON text: {text0}");
    let line1 = map_field(&events[1].log.as_ref().unwrap().message, "line").unwrap();
    assert_eq!(line1, &Value::str("splunk-docker fixture plain line (inline)"));
    assert_eq!(
        inline_batches[0].resource.attributes.get("host.name"),
        Some(&Value::str("splunk-docker-fixture"))
    );
    let recorded = concatenated_objects(&inline.body);
    let want_time = parse_hec_time(recorded[0]["time"].as_str().unwrap()).unwrap();
    assert_eq!(events[0].timestamp, want_time);

    let json = Capture::read("docker-services-collector-event-1-0-003");
    let json_batches = SplunkDecoder::new().decode_events(&json.body, RECEIVED_AT).unwrap();
    assert_eq!(json_batches.len(), 1);
    let json_events = &json_batches[0].events;
    let json_line0 = map_field(&json_events[0].log.as_ref().unwrap().message, "line").unwrap();
    assert!(matches!(json_line0, Value::Map(_)), "json's first line is a Map: {json_line0:?}");
    let json_resource = &json_batches[0].resource.attributes;
    assert_eq!(json_resource.get("host.name"), Some(&Value::str("splunk-docker-fixture")));
    assert_eq!(json_resource.get("com.splunk.source"), Some(&Value::str("docker:fixture")));
    assert_eq!(json_resource.get("com.splunk.sourcetype"), Some(&Value::str("docker:json")));
    assert_eq!(json_resource.get("com.splunk.index"), Some(&Value::str("main")));

    let raw = Capture::read("docker-services-collector-event-1-0-005");
    let raw_batches = SplunkDecoder::new().decode_events(&raw.body, RECEIVED_AT).unwrap();
    assert_eq!(raw_batches.len(), 1);
    let raw_events = &raw_batches[0].events;
    let raw_text = raw_events[0].log.as_ref().unwrap().message.as_str().unwrap();
    assert!(raw_text.starts_with("298ce556c084 "), "raw's event starts with the tag: {raw_text}");
}

// -------------------------------------------------------------------------------------------------
// Splunk Connect for Syslog 3.40.0
// -------------------------------------------------------------------------------------------------

/// The three forwarded syslog lines: `nix:syslog`/`osnix`/`sc4s-fixture-host` from the envelope,
/// and `sc4s_syslog_severity` `info`, `err`, `notice` in order.
#[test]
fn sc4s_syslog_lines_carry_the_syslog_severity_attribute() {
    for (name, severity) in [
        ("sc4s-services-collector-event-004", "info"),
        ("sc4s-services-collector-event-005", "err"),
        ("sc4s-services-collector-event-006", "notice"),
    ] {
        let batches = decode_file(name);
        assert_eq!(batches.len(), 1, "{name}");
        let resource = &batches[0].resource.attributes;
        assert_eq!(
            resource.get("com.splunk.sourcetype"),
            Some(&Value::str("nix:syslog")),
            "{name}"
        );
        assert_eq!(resource.get("com.splunk.index"), Some(&Value::str("osnix")), "{name}");
        assert_eq!(resource.get("host.name"), Some(&Value::str("sc4s-fixture-host")), "{name}");
        assert_eq!(
            batches[0].events[0].attributes.get("sc4s_syslog_severity"),
            Some(&Value::str(severity)),
            "{name}"
        );
    }
}

// -------------------------------------------------------------------------------------------------
// splunk-library-javalogging 1.11.11's Logback appenders
// -------------------------------------------------------------------------------------------------

/// The `/event` objects' messages are `Map`s carrying `severity` INFO/WARN/ERROR, one text and one
/// json appender per level; the WARN ones carry `properties.request_id`. The `/raw` captures
/// decode through their query-string envelope.
#[test]
fn java_appender_events_and_raw_captures_decode() {
    let mut severities = Vec::new();
    for i in 0..=5 {
        let name = format!("java-services-collector-event-1-0-{i:03}");
        let batches = decode_file(&name);
        assert_eq!(batches.len(), 1, "{name}");
        let event = &batches[0].events[0];
        let message = &event.log.as_ref().expect("a log event").message;
        assert!(matches!(message, Value::Map(_)), "{name}: {message:?}");
        let severity = map_field(message, "severity")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("{name}: no severity"))
            .to_string();
        if severity == "WARN" {
            let request_id = map_field(message, "properties")
                .and_then(|p| map_field(p, "request_id"))
                .and_then(Value::as_str);
            assert_eq!(request_id, Some("req-0001"), "{name}");
        }
        severities.push(severity);
    }
    for level in ["INFO", "WARN", "ERROR"] {
        assert_eq!(
            severities.iter().filter(|s| s.as_str() == level).count(),
            2,
            "{level} from one text and one json appender: {severities:?}"
        );
    }

    for i in 0..=2 {
        let name = format!("java-services-collector-raw-{i:03}");
        let capture = Capture::read(&name);
        let envelope = envelope_of(&capture);
        assert_eq!(envelope.sourcetype.as_deref(), Some("java:raw"), "{name}");
        assert_eq!(envelope.host.as_deref(), Some("splunk-java-fixture"), "{name}");
        assert_eq!(envelope.index.as_deref(), Some("main"), "{name}");
        assert_eq!(envelope.source.as_deref(), Some("java:logback"), "{name}");
        let batch = SplunkDecoder::new().decode_raw(&capture.body, &envelope, RECEIVED_AT);
        assert_eq!(batch.events.len(), 1, "{name}");
        assert_eq!(
            batch.resource.attributes.get("host.name"),
            Some(&Value::str("splunk-java-fixture"))
        );
    }
}
