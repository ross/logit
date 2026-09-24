//! Route ownership across Datadog's encoders: a batch fanned out to every route leaves each item
//! on exactly the route(s) that own it. A service check's first record (its `Gauge(status)`) is
//! `encode_service_checks`'s alone, and a Datadog event (a `log` carrying `statsd.event.title`) is
//! `encode_events`'s alone; every other route skips them silently. Any later metric on a service
//! check still leaves as an ordinary series, as `statsd_out` sends `metrics[1..]` as ordinary
//! lines. APM stats (an event carrying `datadog.stats.name`) are the stats routes' alone, whole:
//! their `Sum`s and summary `Distribution`s never leave as series or sketches. The fixed-point
//! suites can't see this: each decodes and encodes on one route.

use bytes::Bytes;
use logit_core::interner::{intern, resolve};
use logit_core::{EventBatch, MetricKind, MetricRecord};
use logit_proto::datadog::events::EventFormat;
use logit_proto::datadog::{DatadogDecoder, DatadogEncoder};

const RECEIVED_AT: i64 = 1_690_000_000_000_000_000;

const CHECKS: &[u8] = br#"[{"check":"db.up","host_name":"db1","timestamp":1700000000,"status":2,"message":"connection refused","tags":["env:prod"]},{"check":"app.ok","host_name":"","timestamp":1700000000,"status":0,"message":"","tags":[]}]"#;

const EVENTS: &[u8] = br#"{"apiKey":"","events":{"api":[{"msg_title":"deploy","msg_text":"v2 out","timestamp":1700000000,"host":"web1","tags":["env:prod"],"alert_type":"warning"}]},"internalHostname":"agent-host"}"#;

fn checks() -> EventBatch {
    DatadogDecoder::new().decode_service_checks(CHECKS, RECEIVED_AT).unwrap()
}

/// Every series route's body for `batch`, labelled.
fn series_routes(batch: &EventBatch) -> [(&'static str, Option<Bytes>); 3] {
    let mut e = DatadogEncoder::new();
    [
        ("v2 protobuf", e.encode_series_v2_protobuf(batch)),
        ("v2 json", e.encode_series_v2_json(batch)),
        ("v1", e.encode_series_v1(batch)),
    ]
}

fn decode_series(route: &str, body: &[u8]) -> EventBatch {
    let mut d = DatadogDecoder::new();
    match route {
        "v2 protobuf" => d.decode_series_v2_protobuf(body, RECEIVED_AT),
        "v2 json" => d.decode_series_v2_json(body, RECEIVED_AT),
        _ => d.decode_series_v1(body, RECEIVED_AT),
    }
    .unwrap()
}

#[test]
fn a_service_check_never_leaves_as_a_series() {
    let batch = checks();
    assert_eq!(batch.events.len(), 2);
    for (route, body) in series_routes(&batch) {
        assert!(body.is_none(), "{route}: a service check leaked onto a series route");
    }
    let mut e = DatadogEncoder::new();
    assert!(e.encode_distribution_points(&batch).is_none());
    assert!(e.encode_sketches(&batch).is_none());
    assert!(e.encode_service_checks(&batch).is_some(), "the check still has its own route");
}

#[test]
fn a_datadog_event_never_leaves_as_a_log() {
    let events = DatadogDecoder::new().decode_events(EVENTS, RECEIVED_AT).unwrap();
    assert_eq!(events.events.len(), 1);
    let e = DatadogEncoder::new();
    assert!(e.encode_logs(&events).is_none(), "a Datadog event leaked onto the logs route");
    assert_eq!(e.encode_events(&events, EventFormat::AgentEnvelope).len(), 1);

    // Mixed with an ordinary log, only the ordinary log goes to the logs route, and only the
    // event to the events route.
    let logs = DatadogDecoder::new().decode_logs(br#"[{"message":"plain"}]"#, RECEIVED_AT).unwrap();
    let mut mixed = events.clone();
    mixed.events.extend(logs.events);
    let body = e.encode_logs(&mixed).expect("the plain log");
    let relayed = DatadogDecoder::new().decode_logs(&body, RECEIVED_AT).unwrap();
    assert_eq!(relayed.events.len(), 1);
    let log = relayed.events[0].log.as_ref().unwrap();
    assert_eq!(log.message.as_str(), Some("plain"));
    let envelope = e.encode_events(&mixed, EventFormat::AgentEnvelope);
    let relayed = DatadogDecoder::new().decode_events(&envelope[0], RECEIVED_AT).unwrap();
    assert_eq!(relayed.events, events.events);
}

#[test]
fn a_service_checks_later_metrics_still_leave_as_series() {
    let mut batch = checks();
    batch.events[0].metrics.push(MetricRecord::new(intern("db.latency"), MetricKind::Gauge(3.5)));
    for (route, body) in series_routes(&batch) {
        let body = body.unwrap_or_else(|| panic!("{route}: the appended metric was dropped"));
        let relayed = decode_series(route, &body);
        assert_eq!(relayed.events.len(), 1, "{route}: exactly the appended metric");
        let metrics = &relayed.events[0].metrics;
        assert_eq!(metrics.len(), 1);
        assert_eq!(resolve(metrics[0].name), "db.latency", "{route}");
        assert_eq!(metrics[0].kind, MetricKind::Gauge(3.5), "{route}");
        // The check's own carriers stay with the check: rendered as tags, they would make the
        // relayed series decode as a service check and vanish from the series route next hop.
        let attrs = &relayed.events[0].attributes;
        assert_eq!(attrs.get("statsd.service_check.name"), None, "{route}");
        assert_eq!(attrs.get("env").and_then(|v| v.as_str()), Some("prod"), "{route}");
        let (_, again) =
            series_routes(&relayed).into_iter().find(|(r, _)| *r == route).expect("the same route");
        assert_eq!(again.as_deref(), Some(&body[..]), "{route}: relays again unchanged");
    }
    // And the check itself still leaves on its own route, once per check.
    let body = DatadogEncoder::new().encode_service_checks(&batch).unwrap();
    let relayed = DatadogDecoder::new().decode_service_checks(&body, RECEIVED_AT).unwrap();
    assert_eq!(relayed.events.len(), 2);
    assert_eq!(resolve(relayed.events[0].metrics[0].name), "db.up");
}

/// A tracer's `/v0.6/stats` body: one bucket, one group with an ok summary. Its events are
/// `Sum`s and a `Distribution`, every one of which a metrics route would otherwise send.
fn client_stats() -> EventBatch {
    use logit_proto::msgpack::Writer;
    let summary = logit_proto::datadog::generated::ddsketch::DdSketch {
        mapping: Some(logit_proto::datadog::generated::ddsketch::IndexMapping {
            gamma: 1.0202020202020203,
            index_offset: 0.0,
            interpolation: 0,
        }),
        positive_values: Some(logit_proto::datadog::generated::ddsketch::Store {
            bin_counts: [(700, 3.0)].into_iter().collect(),
            ..Default::default()
        }),
        negative_values: None,
        zero_count: 0.0,
    };
    let mut w = Writer::new();
    w.write_map_len(2);
    w.write_str("Hostname");
    w.write_str("web-1");
    w.write_str("Stats");
    w.write_array_len(1);
    w.write_map_len(3);
    w.write_str("Start");
    w.write_u64(1_700_000_000_000_000_000);
    w.write_str("Duration");
    w.write_u64(10_000_000_000);
    w.write_str("Stats");
    w.write_array_len(1);
    w.write_map_len(5);
    w.write_str("Service");
    w.write_str("web");
    w.write_str("Name");
    w.write_str("http.request");
    w.write_str("Hits");
    w.write_u64(3);
    w.write_str("Duration");
    w.write_u64(3_000_000);
    w.write_str("OkSummary");
    w.write_bin(&prost::Message::encode_to_vec(&summary));
    DatadogDecoder::new().decode_client_stats_v06(w.as_slice(), RECEIVED_AT).unwrap()
}

#[test]
fn apm_stats_never_leave_on_a_metrics_or_logs_route() {
    let batch = client_stats();
    assert_eq!(batch.events.len(), 1);
    assert_eq!(batch.events[0].metrics.len(), 5, "four sums and the ok summary");
    for (route, body) in series_routes(&batch) {
        assert!(body.is_none(), "{route}: APM stats leaked onto a series route");
    }
    let mut e = DatadogEncoder::new();
    assert!(e.encode_distribution_points(&batch).is_none());
    assert!(e.encode_sketches(&batch).is_none(), "the ok summary leaked onto sketches");
    assert!(e.encode_service_checks(&batch).is_none());
    assert!(e.encode_logs(&batch).is_none());
    assert!(e.encode_events(&batch, EventFormat::AgentEnvelope).is_empty());
    assert!(e.encode_client_stats_v06(&batch).is_some(), "stats keep their own routes");
    assert!(e.encode_stats_payload(&batch).is_some());
}

#[test]
fn apm_stats_beside_a_series_leave_only_the_series_on_a_metrics_route() {
    let mut batch = client_stats();
    let checks = checks();
    let mut gauge = checks.events[1].clone();
    gauge.attributes = Default::default();
    gauge.metrics[0] = MetricRecord::new(intern("queue.depth"), MetricKind::Gauge(4.0));
    batch.events.push(gauge);
    for (route, body) in series_routes(&batch) {
        let relayed = decode_series(route, &body.expect("the gauge"));
        assert_eq!(relayed.events.len(), 1, "{route}");
        assert_eq!(resolve(relayed.events[0].metrics[0].name), "queue.depth", "{route}");
    }
    let body = DatadogEncoder::new().encode_client_stats_v06(&batch).unwrap();
    let relayed = DatadogDecoder::new().decode_client_stats_v06(&body, RECEIVED_AT).unwrap();
    assert_eq!(relayed.events.len(), 1, "the gauge is not a stats group");
}
