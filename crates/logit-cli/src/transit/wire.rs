//! A sink's encoder to its pair's decoder: the bytes each [`SinkEncoder`] shape writes, read back
//! as the pair's listener reads them, with the resource that listener would decode under.
//!
//! | Encoder | Bytes | Decoder |
//! |---|---|---|
//! | `statsd_out` | each `MessageBuf` entry: one line, or an event's indivisible lines | `StatsdDecoder`, one entry at a time |
//! | `influxdb_out` | one body per batch, to the org and bucket the sink resolves for it, `precision=ns`; a batch with no bucket is dropped and counted as the sink drops it | `InfluxDecoder::decode_body`, under the resource `influxdb_in` builds from that org and bucket; a write naming no org or an empty bucket is refused as `influxdb_in` refuses it, counted as the sink's write loop counts a `Rejected` batch (`batches.dropped`/`events.dropped{reason="rejected"}`), since the listener's own count needs its server |
//! | `otlp_out` | one protobuf request per signal per batch; a log with no observed time is stamped with the wall clock, which the driver reads back as the run's clock | `OtlpDecoder::decode_signal` |
//! | `prometheus_out`, `bind:` | one scrape after the last batch, in the run's dialect, held to `max_series:` (no expiry: it reads a clock) | `text::parse_with`, then `families_to_events` |
//! | `prometheus_out`, `endpoint:` | one compressed request per batch | `decompress_bounded`, `remote_write::decode`, then `families_to_events` per timestamp group |
//! | `datadog_out` | one uncompressed request per route per batch, one per event on the events route, with the sink's routing and trace readiness gate (`encode_routes`), a span the gate holds back counted as the sink counts it, and none of its stale filter, tag limit, or size cuts, which read the clock or apply an intake limit | `datadog_in`'s `decode_request`, by the request's path and `Content-Type` |
//! | `graphite_out` | each `MessageBuf` entry: one plaintext line, or one length-prefixed pickle frame, line-capped as the sink's transport caps it | `GraphiteDecoder` of the encoder's protocol, one entry at a time, a pickle frame's length prefix stripped as `graphite_in`'s framer strips it |
//! | `syslog_out` | each `MessageBuf` entry: one message, capped as the sink's transport caps it | `SyslogDecoder`, one entry at a time: a UDP datagram with line splitting on, `syslog_in`'s default `framing: lines`, or under `transport: tcp` one octet-counted frame's payload with line splitting off |
//! | `splunk_hec_out` | each `MessageBuf` entry: one HEC object (`encode_objects`), an object over `max_body_bytes` dropped and counted as the sink drops it, the rest joined into one `/event` body per batch | `SplunkDecoder::decode_events` |
//! | `datadog_trace_out` | one uncompressed trace request per batch at `version:`'s path (`/v0.4/traces` or `/v0.7/traces`), with the tracer headers the sink restores from the batch resource and `X-Datadog-Trace-Count`, and one `/v0.6/stats` request for the batch's APM stats events (`encode_requests`), with none of the sink's size cuts | `datadog_trace_in`'s `decode_request`, by the request's path, with its headers |
//! | `kafka_out` | each `MessageBuf` entry: one record's value, key, and timestamp, written with the sink's headers into one record batch per topic under its `compression:` (`logit_outputs::kafka::record_batches`), with none of the producer's partitioning, request packing, or per-record size check; under an OTLP format, a record and a log with no observed time are stamped with the wall clock, which the driver reads back as the run's clock | `kafka_in`'s `decode_record_batches`, on partition 0 of each topic, in the sink's `format:` (under `record: batch`, `text`'s escaped lines; under an OTLP format, each topic's signal from `topics:`), which stamps each event's `_logit.kafka.*` carriers |
//!
//! The statsd, InfluxDB, syslog, and Kafka encoders return their counts, so this counts them through the
//! sink's own reporting functions, and the Kafka encoder's OTLP mapping through the handle it holds; the OTLP, Prometheus, Datadog, Datadog trace, Graphite, and
//! Splunk encoders count through the handle they hold.
//! A decode error fails the run: the pair's decoder refusing its own sink's bytes is a finding.

use crate::pipeline::SinkEncoder;
use anyhow::Context;
use bytes::Bytes;
use http::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use logit_core::{Diagnostics, EventBatch, Resource, Telemetry};
use logit_inputs::kafka::KafkaInFormat;
use logit_outputs::kafka::{KafkaSettings, RecordFormat, RecordMode};
use logit_pipeline::test_util::TelemetryProbe;
use logit_proto::datadog::DatadogDecoder;
use logit_proto::graphite::pickle::LENGTH_PREFIX_BYTES;
use logit_proto::graphite::{GraphiteDecoder, Protocol as GraphiteProtocol};
use logit_proto::influxdb::{write_resource, InfluxDecoder, Precision};
use logit_proto::otlp::OtlpDecoder;
use logit_proto::prometheus::compression::decompress_bounded;
use logit_proto::prometheus::{families_to_events, remote_write, text, Dialect, PrometheusDecoder};
use logit_proto::splunk::SplunkDecoder;
use logit_proto::statsd::StatsdDecoder;
use logit_proto::syslog::SyslogDecoder;
use logit_proto::{
    Decoder, Encoder, FramedEncoder, MessageBuf, Signal, SignalDecoder, SignalEncoder,
};
use std::sync::Arc;

/// The decoder's telemetry id.
const DECODE_ID: &str = "transit.decode";

/// The most bytes a remote-write body decompresses to, as a receiver bounds it.
const MAX_BODY_BYTES: usize = 1 << 30;

/// The most bytes a record batch's records decompress to: `kafka_in`'s default `max_batch_bytes`.
pub(super) const MAX_RECORDS_BYTES: usize = 64 << 20;

/// How the pair's decoder reads.
pub(super) struct Decode<'a> {
    pub probe: &'a TelemetryProbe,
    pub received_at: i64,
    pub dialect: Dialect,
}

impl Decode<'_> {
    /// The decoder's telemetry and diagnostics, under `kind`, its pair listener's.
    fn handles(&self, kind: &'static str) -> (Telemetry, Diagnostics) {
        let telemetry = self.probe.telemetry(DECODE_ID, kind, "listener");
        let diag = Diagnostics::new(DECODE_ID).with_telemetry(telemetry.clone());
        (telemetry, diag)
    }
}

/// The wall clock in Unix nanoseconds.
fn wall_clock() -> i64 {
    let since = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH);
    since.map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
}

/// Encodes `batches` with `encoder` and decodes the bytes back (module doc).
pub(super) fn round_trip(
    encoder: SinkEncoder,
    batches: &[EventBatch],
    sink: &Telemetry,
    sink_diag: &Diagnostics,
    decode: &Decode<'_>,
) -> anyhow::Result<Vec<EventBatch>> {
    let empty = || Arc::new(Resource::default());
    let mut out = Vec::new();
    match encoder {
        SinkEncoder::Framed(mut encoder) => {
            let (telemetry, diag) = decode.handles("statsd_in");
            let mut decoder =
                StatsdDecoder::new(empty()).with_telemetry(telemetry).with_diagnostics(diag);
            let mut lines = MessageBuf::default();
            for batch in batches {
                let stats = encoder.encode_into(batch, &mut lines);
                logit_outputs::statsd::report_encode_stats(sink, &stats);
                for line in lines.iter() {
                    let mut events = Vec::new();
                    let (resource, scope) = decoder
                        .decode_into(Bytes::copy_from_slice(line), decode.received_at, &mut events)
                        .with_context(|| {
                            format!("statsd_in refused {:?}", String::from_utf8_lossy(line))
                        })?;
                    out.push(EventBatch { resource, scope, events });
                }
            }
        }
        SinkEncoder::Blob { mut encoder, organization, destination } => {
            let mut sink_diag = sink_diag.clone();
            let (telemetry, _) = decode.handles("influxdb_in");
            let mut decoder = InfluxDecoder::new().with_telemetry(telemetry);
            for batch in batches {
                let Some((org, org_id, bucket)) = logit_outputs::influxdb::destination(
                    &organization,
                    &destination,
                    &batch.resource,
                ) else {
                    logit_outputs::influxdb::drop_no_bucket(sink, &mut sink_diag, batch);
                    continue;
                };
                let resource = Arc::new(write_resource(org, org_id, bucket));
                let body = encoder.encode(batch)?;
                logit_outputs::influxdb::emit_encode_stats(sink, encoder.stats());
                if body.is_empty() {
                    continue;
                }
                // `influxdb_in` answers a write naming no org, or an empty value it reads as
                // none, with influxd's `400`, which the sink reads as `Rejected`.
                let named = |value: Option<&str>| value.is_some_and(|v| !v.is_empty());
                if (!named(org) && !named(org_id)) || bucket.is_empty() {
                    sink.count("logit.component.batches.dropped", 1.0, &[("reason", "rejected")]);
                    let events = batch.events.len() as f64;
                    sink.count("logit.component.events.dropped", events, &[("reason", "rejected")]);
                    continue;
                }
                let batch = decoder
                    .decode_body(body, Precision::Ns, decode.received_at, resource)
                    .map_err(|err| anyhow::anyhow!("influxdb_in refused the body: {err}"))?;
                out.push(batch);
            }
        }
        SinkEncoder::Signal(mut encoder) => {
            let (telemetry, diag) = decode.handles("otlp_in");
            let mut decoder = OtlpDecoder::new().with_telemetry(telemetry).with_diagnostics(diag);
            for batch in batches {
                let before = wall_clock();
                let payloads = encoder.encode_signals(batch)?;
                let after = wall_clock();
                for payload in payloads {
                    let signal = payload.signal.as_str();
                    let mut decoded = decoder
                        .decode_signal(payload.signal, payload.bytes)
                        .with_context(|| format!("otlp_in refused a {signal} request"))?;
                    // The encoder stamps a log with no observed time with the wall clock
                    // (`crates/logit-proto/src/otlp/logs.rs`); the run's clock stands in for it.
                    for event in decoded.iter_mut().flat_map(|b| b.events.iter_mut()) {
                        if let Some(log) = event.log.as_mut() {
                            if (before..=after).contains(&log.observed_timestamp) {
                                log.observed_timestamp = decode.received_at;
                            }
                        }
                    }
                    out.extend(decoded);
                }
            }
        }
        SinkEncoder::PrometheusExpose { mut encoder, max_series } => {
            let body = logit_outputs::prometheus::render_batches(
                batches,
                decode.dialect,
                max_series,
                &mut encoder,
                sink,
            );
            let (telemetry, diag) = decode.handles("prometheus_in");
            let mut decoder =
                PrometheusDecoder::new().with_telemetry(telemetry).with_diagnostics(diag);
            let families = text::parse_with(&body, decode.dialect, &mut decoder)
                .context("prometheus_in refused the scrape")?;
            let events = families_to_events(&families, decode.received_at, &mut decoder);
            if !events.is_empty() {
                out.push(EventBatch { resource: empty(), scope: None, events });
            }
        }
        SinkEncoder::Datadog(mut encoder) => {
            for batch in batches {
                for request in logit_outputs::datadog::encode_routes(&mut encoder, batch, sink) {
                    let (telemetry, diag) = decode.handles("datadog_in");
                    let decoder =
                        DatadogDecoder::new().with_telemetry(telemetry).with_diagnostics(diag);
                    let mut headers = HeaderMap::new();
                    headers.insert(CONTENT_TYPE, HeaderValue::from_static(request.content_type));
                    let decoded = logit_inputs::datadog::decode_request(
                        request.path,
                        &headers,
                        request.body,
                        decode.received_at,
                        decoder,
                    )
                    .map_err(|err| {
                        anyhow::anyhow!("datadog_in refused a {} request: {err}", request.path)
                    })?;
                    out.extend(decoded);
                }
            }
        }
        SinkEncoder::DatadogTrace { mut encoder, version } => {
            let mut sink_diag = sink_diag.clone();
            for batch in batches {
                let requests = logit_outputs::datadog_trace::encode_requests(
                    &mut encoder,
                    batch,
                    version,
                    &mut sink_diag,
                );
                for request in requests {
                    let (telemetry, diag) = decode.handles("datadog_trace_in");
                    let decoder = DatadogDecoder::new()
                        .with_telemetry(telemetry)
                        .with_diagnostics(diag.clone());
                    let mut headers = request.headers;
                    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/msgpack"));
                    let decoded = logit_inputs::datadog_trace::decode_request(
                        request.path,
                        &headers,
                        request.body,
                        decode.received_at,
                        decoder,
                        &diag,
                    )
                    .map_err(|err| {
                        anyhow::anyhow!(
                            "datadog_trace_in refused a {} request: {err}",
                            request.path
                        )
                    })?;
                    out.extend(decoded);
                }
            }
        }
        SinkEncoder::Graphite(mut encoder) => {
            let (telemetry, diag) = decode.handles("graphite_in");
            let protocol = encoder.protocol();
            let mut decoder = GraphiteDecoder::new(empty())
                .with_protocol(protocol)
                .with_telemetry(telemetry)
                .with_diagnostics(diag);
            let mut entries = MessageBuf::default();
            for batch in batches {
                // The encoder counts through the handle it holds; `EncodeStats` repeats them.
                encoder.encode_into(batch, &mut entries);
                let mut events = Vec::new();
                let mut resource = empty();
                for entry in entries.iter() {
                    // `graphite_in`'s framer strips a pickle frame's length prefix.
                    let payload = match protocol {
                        GraphiteProtocol::Plaintext => entry,
                        GraphiteProtocol::Pickle => &entry[LENGTH_PREFIX_BYTES..],
                    };
                    (resource, _) = decoder
                        .decode_into(
                            Bytes::copy_from_slice(payload),
                            decode.received_at,
                            &mut events,
                        )
                        .with_context(|| {
                            format!("graphite_in refused {:?}", String::from_utf8_lossy(entry))
                        })?;
                }
                if !events.is_empty() {
                    out.push(EventBatch { resource, scope: None, events });
                }
            }
        }
        SinkEncoder::Syslog { mut encoder, tcp } => {
            let (telemetry, diag) = decode.handles("syslog_in");
            let mut decoder = SyslogDecoder::new(empty())
                .with_line_splitting(!tcp)
                .with_telemetry(telemetry)
                .with_diagnostics(diag);
            let mut messages = MessageBuf::default();
            for batch in batches {
                let stats = encoder.encode_into(batch, &mut messages);
                logit_outputs::syslog::report_encode_stats(sink, &stats);
                let mut events = Vec::new();
                let mut resource = empty();
                for message in messages.iter() {
                    (resource, _) = decoder
                        .decode_into(
                            Bytes::copy_from_slice(message),
                            decode.received_at,
                            &mut events,
                        )
                        .with_context(|| {
                            format!("syslog_in refused {:?}", String::from_utf8_lossy(message))
                        })?;
                }
                if !events.is_empty() {
                    out.push(EventBatch { resource, scope: None, events });
                }
            }
        }
        SinkEncoder::Splunk { mut encoder, max_body_bytes } => {
            let (telemetry, diag) = decode.handles("splunk_hec_in");
            let mut decoder = SplunkDecoder::new().with_telemetry(telemetry).with_diagnostics(diag);
            let mut sink_diag = sink_diag.clone();
            let mut objects = MessageBuf::default();
            for batch in batches {
                encoder.encode_objects(batch, &mut objects);
                logit_outputs::splunk::count_oversize(
                    &objects,
                    max_body_bytes,
                    sink,
                    &mut sink_diag,
                );
                let mut body = Vec::new();
                for object in objects.iter().filter(|object| object.len() <= max_body_bytes) {
                    body.extend_from_slice(object);
                }
                if body.is_empty() {
                    continue;
                }
                let decoded = decoder.decode_events(&body, decode.received_at).map_err(|err| {
                    anyhow::anyhow!(
                        "splunk_hec_in refused {:?}: {err}",
                        String::from_utf8_lossy(&body)
                    )
                })?;
                out.extend(decoded);
            }
        }
        SinkEncoder::Kafka { mut encoder, settings } => {
            let mut records = MessageBuf::default();
            let mut all = MessageBuf::default();
            let otlp = settings.format.is_otlp();
            let before = wall_clock();
            for batch in batches {
                let stats = encoder.encode_into(batch, &mut records);
                logit_outputs::kafka::report_encode_stats(sink, &stats);
                let after = wall_clock() / 1_000_000;
                for (value, meta) in records.iter_with() {
                    let mut meta = meta.clone();
                    // An OTLP format's record is stamped with the wall clock, as the Collector's
                    // exporter stamps it; the run's clock stands in for it.
                    if otlp && (before / 1_000_000..=after).contains(&meta.timestamp) {
                        meta.timestamp = decode.received_at / 1_000_000;
                    }
                    all.push_with(value, meta);
                }
            }
            let after = wall_clock();
            for (topic, bytes) in logit_outputs::kafka::record_batches(&settings, &all)? {
                let (telemetry, _) = decode.handles("kafka_in");
                let mut decoded = logit_inputs::kafka::decode_record_batches(
                    kafka_in_format(&settings, &topic)?,
                    &topic,
                    0,
                    &bytes,
                    MAX_RECORDS_BYTES,
                    decode.received_at,
                    telemetry,
                )
                .with_context(|| format!("kafka_in refused the records of topic {topic:?}"))?;
                // The OTLP encoder stamps a log with no observed time with the wall clock
                // (`crates/logit-proto/src/otlp/logs.rs`); the run's clock stands in for it.
                if otlp {
                    for event in decoded.iter_mut().flat_map(|b| b.events.iter_mut()) {
                        if let Some(log) = event.log.as_mut() {
                            if (before..=after).contains(&log.observed_timestamp) {
                                log.observed_timestamp = decode.received_at;
                            }
                        }
                    }
                }
                out.extend(decoded);
            }
        }
        SinkEncoder::PrometheusRemoteWrite { mut encoder, version, compression } => {
            let (telemetry, diag) = decode.handles("prometheus_in");
            // A remote-write sample always carries a timestamp, so `prometheus_in` stamps no
            // marker for one.
            let mut decoder = PrometheusDecoder::new()
                .with_timestamp_marker(false)
                .with_telemetry(telemetry)
                .with_diagnostics(diag);
            for batch in batches {
                let encoded = logit_outputs::prometheus::encode_remote_write(
                    batch,
                    version,
                    compression,
                    &mut encoder,
                )?;
                let Some((compressed, _samples)) = encoded else {
                    continue;
                };
                let body = decompress_bounded(compression, &compressed, MAX_BODY_BYTES)?;
                let decoded = remote_write::decode(&body, version, &mut decoder)
                    .context("prometheus_in refused the request")?;
                let mut events = Vec::new();
                for group in &decoded.groups {
                    events.extend(families_to_events(group, decode.received_at, &mut decoder));
                }
                out.push(EventBatch { resource: empty(), scope: None, events });
            }
        }
    }
    Ok(out)
}

/// The `kafka_in` format that reads `settings`' records on `topic`: the sink's own `format:`, a
/// `text` batch record's lines under `record: batch`, and under an OTLP format the signal each
/// topic of `topics:` carries.
fn kafka_in_format(settings: &KafkaSettings, topic: &str) -> anyhow::Result<KafkaInFormat> {
    let batch = settings.record == Some(RecordMode::Batch);
    Ok(match settings.format {
        RecordFormat::Json => KafkaInFormat::Json,
        RecordFormat::Msgpack => KafkaInFormat::Msgpack,
        RecordFormat::Native => KafkaInFormat::Native,
        RecordFormat::Text => KafkaInFormat::Text { batch },
        RecordFormat::OtlpProto | RecordFormat::OtlpJson => {
            let topics = settings.topics.as_ref().with_context(|| {
                format!("kafka_out's topic {topic:?} carries every signal, which no reader can tell apart")
            })?;
            let topics = vec![
                (topics.logs.clone(), Signal::Logs),
                (topics.metrics.clone(), Signal::Metrics),
                (topics.traces.clone(), Signal::Traces),
            ];
            KafkaInFormat::Otlp { json: settings.format == RecordFormat::OtlpJson, topics }
        }
    })
}
