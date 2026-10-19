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
//!
//! The statsd and InfluxDB encoders return their counts, so this counts them through the sink's
//! own reporting functions; the OTLP and Prometheus encoders count through the handle they hold.
//! A decode error fails the run: the pair's decoder refusing its own sink's bytes is a finding.

use crate::pipeline::SinkEncoder;
use anyhow::Context;
use bytes::Bytes;
use logit_core::{Diagnostics, EventBatch, Resource, Telemetry};
use logit_pipeline::test_util::TelemetryProbe;
use logit_proto::influxdb::{write_resource, InfluxDecoder, Precision};
use logit_proto::otlp::OtlpDecoder;
use logit_proto::prometheus::compression::decompress_bounded;
use logit_proto::prometheus::{families_to_events, remote_write, text, Dialect, PrometheusDecoder};
use logit_proto::statsd::StatsdDecoder;
use logit_proto::{Decoder, Encoder, FramedEncoder, MessageBuf, SignalDecoder, SignalEncoder};
use std::sync::Arc;

/// The decoder's telemetry id.
const DECODE_ID: &str = "transit.decode";

/// The most bytes a remote-write body decompresses to, as a receiver bounds it.
const MAX_BODY_BYTES: usize = 1 << 30;

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
