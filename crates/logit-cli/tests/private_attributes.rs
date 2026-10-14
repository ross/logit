//! The private `_logit.` attribute namespace end to end: every protocol decoder's carriers reach
//! no other protocol's wire, and no encoder counts them as anything.
//!
//! Each case decodes one carrier-bearing input with the real decoder, then encodes the result
//! through every protocol encoder. No wire output may contain the bytes `_logit.` or, sanitized,
//! `_logit_`, and each cross-protocol encoder's counts (its stats struct and the telemetry it
//! emits) must equal what it counts for the same batch with every top-level private key removed. `datadog_out` reads
//! statsd's event carriers, as the ADR permits, and its counts still match; a sink whose reading of
//! another protocol's carriers changes a count needs an exemption here. The whole-event renders,
//! `stdio_out`'s `format: json` and the native frame, must keep every private key. The rule is
//! [ADR `private-attribute-namespace`](../../../docs/adr/private-attribute-namespace.md)'s "The
//! ignore rule".

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use logit_core::attrs::is_private;
use logit_core::interner::resolve;
use logit_core::telemetry::Registry;
use logit_core::{AttrMap, Diagnostics, EventBatch, MetricKind, Resource, Telemetry, Value};
use logit_outputs::{ndjson, statsd as statsd_out, syslog as syslog_out};
use logit_proto::datadog::events::EventFormat;
use logit_proto::datadog::{
    DatadogDecoder, DatadogEncoder, TRACER_FLAG_HEADERS, TRACER_STR_HEADERS, TRACER_U64_HEADERS,
};
use logit_proto::graphite::{GraphiteEncoder, Protocol};
use logit_proto::influxdb::{InfluxDecoder, Precision, RESOURCE_ATTR_BUCKET, RESOURCE_ATTR_ORG};
use logit_proto::otlp::{OtlpDecoder, OtlpEncoder, OtlpEncoding};
use logit_proto::prometheus::remote_write::{self, Version};
use logit_proto::prometheus::{
    events_to_families, families_to_events, text, Destination, Dialect, PrometheusDecoder,
    PrometheusEncoder, ATTR_TARGET,
};
use logit_proto::splunk::SplunkEncoder;
use logit_proto::{
    collectd, influxdb, native, statsd, syslog, Decoder, Encoder, FramedEncoder, MessageBuf,
    Signal, SignalEncoder,
};

/// A private key as it can reach a wire: verbatim, or with its dots sanitized to `_`, as
/// `prometheus_out` writes a label name (`_logit_statsd_type`).
const PRIVATE: [&[u8]; 2] = [b"_logit.", b"_logit_"];

fn interop(path: &str) -> Vec<u8> {
    let full = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/interop").join(path);
    std::fs::read(&full).unwrap_or_else(|e| panic!("reading {}: {e}", full.display()))
}

fn empty_resource() -> Arc<Resource> {
    Arc::new(Resource::default())
}

fn resource_with(pairs: &[(&str, Value)]) -> Arc<Resource> {
    let mut resource = Resource::default();
    for (key, value) in pairs {
        resource.attributes.insert(key, value.clone());
    }
    Arc::new(resource)
}

// --- the decoded inputs ------------------------------------------------------------------------

/// One decoded input: a case name, the protocol whose decoder made it, and the batch.
struct Case {
    name: &'static str,
    protocol: &'static str,
    batch: EventBatch,
}

fn cases() -> Vec<Case> {
    let mut out = Vec::new();
    let mut case = |name, protocol, batch| out.push(Case { name, protocol, batch });

    let mut statsd = statsd::StatsdDecoder::new(empty_resource());
    case(
        "statsd timer and gauge",
        "statsd",
        statsd
            .decode(Bytes::from_static(
                b"req.time:12|ms|#env:prod|c:abc123|T1700000000\n\
                  conns:3|g|#env:prod|c:abc123|T1700000000",
            ))
            .unwrap(),
    );
    case(
        "statsd event and service check",
        "statsd",
        statsd
            .decode(Bytes::from_static(
                b"_e{6,5}:deploy|hello|p:low|t:info|#env:prod\n_sc|db.up|0|#env:prod|m:fine",
            ))
            .unwrap(),
    );

    let mut collectd = collectd::CollectdDecoder::new(empty_resource());
    case(
        "collectd values",
        "collectd",
        collectd.decode(Bytes::from(interop("collectd/collectd-000.raw"))).unwrap(),
    );
    case(
        "collectd notification",
        "collectd",
        collectd.decode(Bytes::from(interop("collectd/collectd-notification-000.raw"))).unwrap(),
    );

    let influx = InfluxDecoder::new()
        .decode_body(
            Bytes::from_static(b"cpu,host=a usage=1.5,note=\"hi\" 1700000000000000000"),
            Precision::Ns,
            1_700_000_000_000_000_000,
            resource_with(&[
                (RESOURCE_ATTR_ORG, Value::from("acme")),
                (RESOURCE_ATTR_BUCKET, Value::from("metrics")),
            ]),
        )
        .unwrap();
    case("influxdb line", "influxdb", influx);

    let families = text::parse(
        b"# TYPE temp untyped\ntemp{room=\"a\"} 21.5 1700000000000\n",
        Dialect::Text0_0_4,
    )
    .unwrap();
    let events =
        families_to_events(&families, 1_700_000_000_000_000_000, &mut PrometheusDecoder::new());
    let resource = resource_with(&[(ATTR_TARGET, Value::from("http://node:9100/metrics"))]);
    case("prometheus untyped", "prometheus", EventBatch { resource, scope: None, events });

    let series = br#"{"series":[{"metric":"req.rate","type":2,"interval":10,
        "points":[{"timestamp":1700000000,"value":1.5}],"tags":["env:prod"],
        "resources":[{"type":"host","name":"web-1"}],"source_type_name":"src"}]}"#;
    case(
        "datadog rate series",
        "datadog",
        DatadogDecoder::new().decode_series_v2_json(series, 0).unwrap(),
    );
    case("datadog v0.4 trace", "datadog", datadog_trace());

    let mut syslog = syslog::SyslogDecoder::new(empty_resource());
    case(
        "syslog rfc5424",
        "syslog",
        syslog
            .decode(Bytes::from_static(
                b"<34>1 2023-11-14T22:13:20.000Z web-1 app 123 ID47 [ex@32473 k=\"v\"] hello",
            ))
            .unwrap(),
    );

    let log = br#"{"resourceLogs":[{"resource":{"attributes":[
        {"key":"service.name","value":{"stringValue":"api"}}]},
        "scopeLogs":[{"scope":{"name":"s"},"logRecords":[{"timeUnixNano":"1700000000000000000",
        "severityNumber":10,"severityText":"INFO2","body":{"stringValue":"hello"},
        "attributes":[{"key":"env","value":{"stringValue":"prod"}}]}]}]}]}"#;
    let mut otlp =
        OtlpDecoder::new().decode_signal_json(Signal::Logs, Bytes::from_static(log)).unwrap();
    case("otlp log", "otlp", otlp.remove(0));

    out
}

/// A recorded dd-trace v0.4 request, with its request headers stamped on the resource as
/// `datadog_trace_in` stamps them.
fn datadog_trace() -> EventBatch {
    let body = interop("datadog/tracer-v04-v0-4-traces-000.bin");
    let mut batch = DatadogDecoder::new().decode_traces_v04(&body, 0).unwrap();
    let headers = String::from_utf8(interop("datadog/tracer-v04-v0-4-traces-000.headers")).unwrap();
    let header = |name: &str| {
        headers.lines().find_map(|line| {
            let (key, value) = line.split_once(": ")?;
            (key == name).then(|| value.trim().to_string())
        })
    };
    let mut resource = (*batch.resource).clone();
    for (name, attr) in TRACER_STR_HEADERS {
        if let Some(value) = header(name) {
            resource.attributes.insert(attr, Value::str(&value));
        }
    }
    for (name, attr) in TRACER_FLAG_HEADERS {
        if header(name).is_some() {
            resource.attributes.insert(attr, Value::Bool(true));
        }
    }
    for (name, attr) in TRACER_U64_HEADERS {
        if let Some(n) = header(name).and_then(|v| v.parse::<u64>().ok()) {
            resource.attributes.insert(attr, Value::U64(n));
        }
    }
    batch.resource = Arc::new(resource);
    batch
}

// --- private keys ------------------------------------------------------------------------------

fn private_keys(attrs: &AttrMap) -> Vec<String> {
    attrs.iter().map(|(k, _)| resolve(k)).filter(|k| is_private(k)).map(str::to_string).collect()
}

/// Every top-level private key with its value, as `"resource <key> = <value>"` or
/// `"event <n> <key> = <value>"`.
fn private_pairs(batch: &EventBatch) -> Vec<String> {
    let mut out = Vec::new();
    let mut push = |place: String, attrs: &AttrMap| {
        for key in private_keys(attrs) {
            out.push(format!("{place} {key} = {:?}", attrs.get(&key).unwrap()));
        }
    };
    push("resource".to_string(), &batch.resource.attributes);
    for (n, event) in batch.events.iter().enumerate() {
        push(format!("event {n}"), &event.attributes);
    }
    out.sort();
    out
}

fn strip_attrs(attrs: &mut AttrMap) {
    for key in private_keys(attrs) {
        attrs.remove(&key);
    }
}

/// `batch` with every top-level private key removed from its resource and its events.
fn stripped(batch: &EventBatch) -> EventBatch {
    let mut resource = (*batch.resource).clone();
    strip_attrs(&mut resource.attributes);
    let mut events = batch.events.clone();
    for event in &mut events {
        strip_attrs(&mut event.attributes);
    }
    EventBatch { resource: Arc::new(resource), scope: batch.scope.clone(), events }
}

// --- the encoders ------------------------------------------------------------------------------

/// What one encode produced: its wire messages, and its stats struct rendered with `{:?}` (empty
/// for an encoder that counts only through telemetry).
type Encoded = (Vec<Vec<u8>>, String);

/// One protocol encoder, built fresh per encode over the given telemetry and diagnostics.
struct Sink {
    name: &'static str,
    protocol: &'static str,
    encode: fn(&EventBatch, Telemetry, Diagnostics) -> Encoded,
}

fn framed<M>(buf: &MessageBuf<M>) -> Vec<Vec<u8>> {
    buf.iter().map(<[u8]>::to_vec).collect()
}

fn graphite(batch: &EventBatch, t: Telemetry, d: Diagnostics, protocol: Protocol) -> Encoded {
    let mut encoder =
        GraphiteEncoder::new().with_telemetry(t).with_diagnostics(d).with_protocol(protocol);
    let mut out = MessageBuf::default();
    let stats = encoder.encode_into(batch, &mut out);
    (framed(&out), format!("{stats:?}"))
}

fn encode_statsd(batch: &EventBatch, d: Diagnostics, format: statsd_out::Format) -> Encoded {
    let mut encoder = statsd_out::StatsdEncoder::new(format).with_diagnostics(d);
    let mut out = MessageBuf::default();
    let stats = encoder.encode_into(batch, &mut out);
    (framed(&out), format!("{stats:?}"))
}

fn encode_syslog(batch: &EventBatch, d: Diagnostics, format: syslog_out::Format) -> Encoded {
    let mut encoder = syslog_out::SyslogEncoder::new(format, 1)
        .with_diagnostics(d)
        .with_structured_data("logit@32473")
        .unwrap();
    let mut out = MessageBuf::default();
    let stats = encoder.encode_into(batch, &mut out);
    (framed(&out), format!("{stats:?}"))
}

fn exposition(batch: &EventBatch, t: Telemetry, d: Diagnostics, dialect: Dialect) -> Encoded {
    let mut encoder = PrometheusEncoder::new().with_telemetry(t).with_diagnostics(d);
    let families =
        events_to_families(batch.events.iter().map(|e| (&*batch.resource, e)), &mut encoder);
    let mut out = Vec::new();
    text::write_with(&families, dialect, &mut out, &mut encoder);
    (vec![out], String::new())
}

/// Remote write as `prometheus_out` builds it: one family list per distinct timestamp, every
/// series timestamped, encoded uncompressed.
fn remote_write(batch: &EventBatch, t: Telemetry, d: Diagnostics, version: Version) -> Encoded {
    let mut encoder = PrometheusEncoder::new()
        .with_telemetry(t)
        .with_diagnostics(d)
        .with_timestamps_always(true)
        .with_stale_markers(true)
        .with_destination(Destination::RemoteWrite);
    let mut by_time: BTreeMap<i64, Vec<_>> = BTreeMap::new();
    for event in &batch.events {
        by_time.entry(event.timestamp).or_default().push((&*batch.resource, event));
    }
    let groups: Vec<_> = by_time
        .into_values()
        .map(|pairs| events_to_families(pairs.into_iter(), &mut encoder))
        .collect();
    (vec![remote_write::encode(&groups, version, &mut encoder)], String::new())
}

/// Every Datadog intake encoder; each returns nothing for a batch with nothing of its route.
fn datadog(batch: &EventBatch, t: Telemetry, d: Diagnostics) -> Encoded {
    let mut encoder = DatadogEncoder::new().with_telemetry(t).with_diagnostics(d);
    let mut out: Vec<Option<Bytes>> = vec![
        encoder.encode_series_v2_protobuf(batch),
        encoder.encode_series_v2_json(batch),
        encoder.encode_series_v1(batch),
        encoder.encode_distribution_points(batch),
        encoder.encode_sketches(batch),
        encoder.encode_service_checks(batch),
        encoder.encode_logs(batch),
        encoder.encode_agent_payload(batch),
        encoder.encode_stats_payload(batch),
    ];
    out.extend(encoder.encode_events(batch, EventFormat::AgentEnvelope).into_iter().map(Some));
    out.extend(encoder.encode_events(batch, EventFormat::PublicV1).into_iter().map(Some));
    (out.into_iter().flatten().map(|b| b.to_vec()).collect(), String::new())
}

fn otlp(batch: &EventBatch, t: Telemetry, d: Diagnostics, encoding: OtlpEncoding) -> Encoded {
    let mut encoder =
        OtlpEncoder::new().with_encoding(encoding).with_telemetry(t).with_diagnostics(d);
    let payloads = encoder.encode_signals(batch).unwrap();
    (payloads.into_iter().map(|p| p.bytes.to_vec()).collect(), String::new())
}

fn sinks() -> Vec<Sink> {
    vec![
        Sink {
            name: "influxdb line protocol",
            protocol: "influxdb",
            encode: |b, _, d| {
                let mut encoder = influxdb::encode::InfluxLineEncoder::new().with_diagnostics(d);
                let body = encoder.encode(b).unwrap();
                (vec![body.to_vec()], format!("{:?}", encoder.stats()))
            },
        },
        Sink {
            name: "graphite plaintext",
            protocol: "graphite",
            encode: |b, t, d| graphite(b, t, d, Protocol::Plaintext),
        },
        Sink {
            name: "graphite pickle",
            protocol: "graphite",
            encode: |b, t, d| graphite(b, t, d, Protocol::Pickle),
        },
        Sink {
            name: "collectd",
            protocol: "collectd",
            encode: |b, t, d| {
                // A fallback host, so an event with no `collectd.host` still reaches the wire.
                let mut encoder = collectd::CollectdEncoder::new()
                    .with_telemetry(t)
                    .with_diagnostics(d)
                    .with_hostname("fallback-host");
                let mut out = MessageBuf::default();
                let stats = encoder.encode_into(b, &mut out);
                (framed(&out), format!("{stats:?}"))
            },
        },
        Sink {
            name: "statsd dogstatsd",
            protocol: "statsd",
            encode: |b, _, d| encode_statsd(b, d, statsd_out::Format::DogStatsd),
        },
        Sink {
            name: "statsd plain",
            protocol: "statsd",
            encode: |b, _, d| encode_statsd(b, d, statsd_out::Format::Statsd),
        },
        Sink {
            name: "syslog rfc5424",
            protocol: "syslog",
            encode: |b, _, d| encode_syslog(b, d, syslog_out::Format::Rfc5424),
        },
        Sink {
            name: "syslog rfc3164",
            protocol: "syslog",
            encode: |b, _, d| encode_syslog(b, d, syslog_out::Format::Rfc3164),
        },
        Sink {
            name: "prometheus text 0.0.4",
            protocol: "prometheus",
            encode: |b, t, d| exposition(b, t, d, Dialect::Text0_0_4),
        },
        Sink {
            name: "prometheus openmetrics",
            protocol: "prometheus",
            encode: |b, t, d| exposition(b, t, d, Dialect::OpenMetrics1_0),
        },
        Sink {
            name: "prometheus remote write 1.0",
            protocol: "prometheus",
            encode: |b, t, d| remote_write(b, t, d, Version::V1),
        },
        Sink {
            name: "prometheus remote write 2.0",
            protocol: "prometheus",
            encode: |b, t, d| remote_write(b, t, d, Version::V2),
        },
        Sink { name: "datadog intake", protocol: "datadog", encode: datadog },
        Sink {
            name: "datadog v0.4 traces",
            protocol: "datadog",
            encode: |b, t, d| {
                let mut encoder = DatadogEncoder::new().with_telemetry(t).with_diagnostics(d);
                (
                    encoder.encode_traces_v04(b).into_iter().map(|b| b.to_vec()).collect(),
                    String::new(),
                )
            },
        },
        Sink {
            name: "splunk hec",
            protocol: "splunk",
            encode: |b, t, d| {
                let mut encoder = SplunkEncoder::new().with_telemetry(t).with_diagnostics(d);
                let mut out = MessageBuf::default();
                encoder.encode_objects(b, &mut out);
                (framed(&out), String::new())
            },
        },
        Sink {
            name: "otlp protobuf",
            protocol: "otlp",
            encode: |b, t, d| otlp(b, t, d, OtlpEncoding::Protobuf),
        },
        Sink {
            name: "otlp json",
            protocol: "otlp",
            encode: |b, t, d| otlp(b, t, d, OtlpEncoding::Json),
        },
    ]
}

/// Runs `sink` over `batch`, returning its wire messages and every count it made: the stats
/// struct, then each counter and gauge its telemetry and diagnostics recorded, sorted.
fn run(sink: &Sink, batch: &EventBatch) -> (Vec<Vec<u8>>, Vec<String>) {
    let registry = Registry::new();
    let telemetry = registry.telemetry_for("sink", "sink", "sink");
    let diagnostics = Diagnostics::new("sink").with_telemetry(telemetry.clone());
    let (wire, stats) = (sink.encode)(batch, telemetry, diagnostics);
    let mut counts = Vec::new();
    for event in registry.drain(0) {
        let mut tags: Vec<String> =
            event.attributes.iter().map(|(k, v)| format!("{}={v:?}", resolve(k))).collect();
        tags.sort();
        for record in &event.metrics {
            if matches!(record.kind, MetricKind::Sum(_) | MetricKind::Gauge(_)) {
                counts.push(format!("{} {tags:?} {:?}", resolve(record.name), record.kind));
            }
        }
    }
    counts.sort();
    counts.insert(0, stats);
    (wire, counts)
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

// --- the tests ---------------------------------------------------------------------------------

#[test]
fn every_decoder_stamps_a_private_key() {
    for case in cases() {
        assert!(
            !private_pairs(&case.batch).is_empty(),
            "{}: the decoded batch carries no `_logit.` key, so nothing below is tested",
            case.name
        );
    }
}

#[test]
fn no_protocol_encoder_writes_or_counts_a_private_key() {
    let mut failures = Vec::new();
    for case in cases() {
        let mut written = 0;
        for sink in sinks() {
            let pair = format!("{} -> {}", case.name, sink.name);
            let (wire, counts) = run(&sink, &case.batch);
            written += wire.iter().filter(|m| !m.is_empty()).count();
            for message in &wire {
                if let Some(at) = PRIVATE.iter().find_map(|needle| find(message, needle)) {
                    let end = message.len().min(at + 60);
                    failures.push(format!(
                        "{pair}: the wire carries a private key: {:?}",
                        String::from_utf8_lossy(&message[at..end])
                    ));
                }
            }
            // A sink reads its own protocol's private keys, so stripping them changes its counts.
            if case.protocol == sink.protocol {
                continue;
            }
            let (_, baseline) = run(&sink, &stripped(&case.batch));
            if counts != baseline {
                failures.push(format!(
                    "{pair}: counts differ from the same batch without its private keys\n  \
                     with:    {counts:?}\n  without: {baseline:?}"
                ));
            }
        }
        assert!(written > 0, "{}: no encoder wrote anything", case.name);
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn whole_event_renders_keep_every_private_key() {
    for case in cases() {
        let pairs = private_pairs(&case.batch);

        let mut json = String::new();
        ndjson::render_ndjson(&mut json, &case.batch);
        let mut keys: Vec<String> = private_keys(&case.batch.resource.attributes);
        for event in &case.batch.events {
            keys.extend(private_keys(&event.attributes));
        }
        for key in keys {
            assert!(
                json.contains(&format!("\"{key}\"")),
                "{}: stdio_out json lost {key}",
                case.name
            );
        }

        let mut frame = native::encode_batch(&case.batch);
        let budget = native::DecodeBudget::for_frame_cap(u32::MAX);
        let decoded = native::decode_batch(&mut frame, &budget).unwrap();
        assert_eq!(
            private_pairs(&decoded),
            pairs,
            "{}: the native frame lost a private key",
            case.name
        );
    }
}
