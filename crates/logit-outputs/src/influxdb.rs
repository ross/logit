//! InfluxDB 2.x line-protocol output -- the output side of the v0.1 vertical slice
//! (`docs/OVERVIEW.md`), writing to `/api/v2/write` with org/bucket query params and a
//! `Token` auth header. Matches the `influxdb` service seeded in `compose.yaml` for local testing.

use crate::Output;
use bytes::Bytes;
use logit_core::interner::resolve;
use logit_core::{Event, EventBatch, MetricKind, MetricRecord, Payload, Resource, Value};
use logit_proto::{CodecError, Encoder};

pub struct InfluxDbOutput {
    url: String,
    org: String,
    bucket: String,
    token: String,
    client: reqwest::Client,
    encoder: InfluxLineEncoder,
}

impl InfluxDbOutput {
    pub fn new(url: String, org: String, bucket: String, token: String) -> Self {
        Self { url, org, bucket, token, client: reqwest::Client::new(), encoder: InfluxLineEncoder }
    }
}

#[async_trait::async_trait]
impl Output for InfluxDbOutput {
    async fn send(&mut self, batch: EventBatch) -> anyhow::Result<()> {
        let body = self.encoder.encode(&batch)?;
        if body.is_empty() {
            // Nothing in this batch had a line-protocol encoding (e.g. all logs/traces, or all
            // Set metrics -- see `metric_fields` below). Not an error; nothing to write.
            return Ok(());
        }

        let write_url = format!("{}/api/v2/write", self.url.trim_end_matches('/'));
        let resp = self
            .client
            .post(&write_url)
            .query(&[
                ("org", self.org.as_str()),
                ("bucket", self.bucket.as_str()),
                ("precision", "ns"),
            ])
            .header("Authorization", format!("Token {}", self.token))
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("InfluxDB write failed ({status}): {text}");
        }
        Ok(())
    }
}

/// Encodes an [`EventBatch`] as InfluxDB line protocol. Split out from [`InfluxDbOutput`] so the
/// encoding logic is directly unit-testable without an HTTP server.
///
/// Only [`Payload::Metric`] events have a line-protocol mapping; logs and traces are skipped
/// (`docs/OVERVIEW.md`'s v0.1 slice is metrics-only -- there's no established convention yet for
/// what a log line or span becomes in InfluxDB, and guessing one isn't this output's job).
struct InfluxLineEncoder;

impl Encoder for InfluxLineEncoder {
    fn encode(&mut self, batch: &EventBatch) -> Result<Bytes, CodecError> {
        let mut buf = String::new();
        for event in &batch.events {
            let Payload::Metric(metric) = &event.payload else {
                continue;
            };
            // One bad metric (today, only a `Set` -- see `metric_fields`) shouldn't drop every
            // other metric in the batch, so this logs and skips rather than propagating via `?`.
            // TODO: route through a proper diagnostics facility once one exists, instead of
            // stderr -- same gap noted in logit-inputs::statsd.
            if let Err(err) = encode_metric_line(&mut buf, &batch.resource, event, metric) {
                eprintln!("influxdb output: {err}");
            }
        }
        Ok(Bytes::from(buf.into_bytes()))
    }
}

fn encode_metric_line(
    buf: &mut String,
    resource: &Resource,
    event: &Event,
    metric: &MetricRecord,
) -> Result<(), CodecError> {
    let fields = metric_fields(&metric.kind)?;
    if fields.is_empty() {
        // Every field was non-finite (see `format_float`) or otherwise unrepresentable -- an
        // empty line is invalid line protocol, so skip rather than write one.
        return Ok(());
    }

    buf.push_str(&escape_measurement(resolve(metric.name)));

    // Tags: resource attributes (host, service, ...) first, event attributes override on key
    // collision -- an event-level tag is more specific than the batch-wide resource it came from.
    let mut tags = resource.attributes.clone();
    for (key, value) in event.attributes.iter() {
        tags.insert(resolve(key), value.clone());
    }
    for (key, value) in tags.iter() {
        if let Some(v) = value_as_tag_string(value) {
            buf.push(',');
            buf.push_str(&escape_tag(resolve(key)));
            buf.push('=');
            buf.push_str(&escape_tag(&v));
        }
    }

    buf.push(' ');
    for (i, (key, value)) in fields.iter().enumerate() {
        if i > 0 {
            buf.push(',');
        }
        buf.push_str(&escape_tag(key));
        buf.push('=');
        buf.push_str(value);
    }

    buf.push(' ');
    buf.push_str(&event.timestamp.to_string());
    buf.push('\n');
    Ok(())
}

/// The line-protocol field set for one metric. `Counter`/`Gauge` are a single `value` field;
/// `Distribution` writes `count` plus a few fixed percentiles (meaningful today even for the
/// single-sample sketches `logit-inputs::statsd` currently produces -- p50/p90/p99 of one sample
/// are all just that sample). `Histogram`/`Summary` map their buckets/quantiles onto fields
/// directly. `Set` has no encoding yet: it needs a real `HyperLogLog`
/// (`logit_core::metric::HyperLogLog` is still a stub), so this returns an error rather than
/// inventing a meaningless one.
fn metric_fields(kind: &MetricKind) -> Result<Vec<(String, String)>, CodecError> {
    match kind {
        MetricKind::Counter(v) | MetricKind::Gauge(v) => {
            Ok(format_float(*v).map(|v| vec![("value".to_string(), v)]).unwrap_or_default())
        }
        MetricKind::Distribution(sketch) => {
            let mut fields = vec![("count".to_string(), sketch.count().to_string())];
            for q in [0.5, 0.9, 0.99] {
                if let Some(v) = sketch.quantile(q).and_then(format_float) {
                    fields.push((format!("p{}", (q * 100.0).round() as u32), v));
                }
            }
            Ok(fields)
        }
        MetricKind::Histogram { buckets } => Ok(buckets
            .iter()
            .filter_map(|(bound, count)| {
                format_float(*bound).map(|b| (format!("bucket_{b}"), count.to_string()))
            })
            .collect()),
        MetricKind::Summary { quantiles } => Ok(quantiles
            .iter()
            .filter_map(|(q, v)| {
                format_float(*v).map(|v| (format!("p{}", (q * 100.0).round() as u32), v))
            })
            .collect()),
        MetricKind::Set(_) => {
            Err(CodecError::Malformed("Set metrics have no line-protocol encoding yet".to_string()))
        }
    }
}

/// Line protocol has no representation for non-finite floats; guard here rather than writing
/// `value=NaN` and letting InfluxDB reject the whole write. (A client can produce this today --
/// statsd's `f64::parse` accepts the literal text "NaN"/"inf" -- so this isn't a purely
/// theoretical guard.)
fn format_float(v: f64) -> Option<String> {
    v.is_finite().then(|| v.to_string())
}

fn value_as_tag_string(v: &Value) -> Option<String> {
    match v {
        Value::Bool(b) => Some(b.to_string()),
        Value::I64(i) => Some(i.to_string()),
        Value::U64(u) => Some(u.to_string()),
        Value::F64(f) => Some(f.to_string()),
        Value::Str(s) => std::str::from_utf8(s).ok().map(str::to_string),
        // Null, Bytes, Timestamp, Array, and Map have no sensible plain-text tag representation.
        Value::Null | Value::Bytes(_) | Value::Timestamp(_) | Value::Array(_) | Value::Map(_) => {
            None
        }
    }
}

fn escape_measurement(s: &str) -> String {
    s.replace('\\', "\\\\").replace(',', "\\,").replace(' ', "\\ ")
}

fn escape_tag(s: &str) -> String {
    s.replace('\\', "\\\\").replace(',', "\\,").replace('=', "\\=").replace(' ', "\\ ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, BodyFormat, LogRecord, MetricKind};
    use std::sync::Arc;

    fn batch_with(events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(Resource::default()), events }
    }

    fn metric_event(name: &str, kind: MetricKind, attrs: &[(&str, &str)]) -> Event {
        let mut attributes = AttrMap::new();
        for (k, v) in attrs {
            attributes.insert(k, *v);
        }
        Event {
            timestamp: 1_700_000_000_000_000_000,
            attributes,
            payload: Payload::Metric(MetricRecord {
                name: logit_core::interner::intern(name),
                kind,
                unit: None,
            }),
        }
    }

    fn encode(events: Vec<Event>) -> String {
        let mut encoder = InfluxLineEncoder;
        let bytes = encoder.encode(&batch_with(events)).expect("encode should succeed");
        String::from_utf8(bytes.to_vec()).expect("output should be valid utf-8")
    }

    #[test]
    fn counter_line() {
        let out =
            encode(vec![metric_event("page.views", MetricKind::Counter(3.0), &[("env", "prod")])]);
        assert_eq!(out, "page.views,env=prod value=3 1700000000000000000\n");
    }

    #[test]
    fn gauge_line_with_no_tags() {
        let out = encode(vec![metric_event("cpu.load", MetricKind::Gauge(0.5), &[])]);
        assert_eq!(out, "cpu.load value=0.5 1700000000000000000\n");
    }

    #[test]
    fn distribution_line_has_count_and_percentiles() {
        let mut sketch = logit_core::DdSketch::new();
        sketch.add(120.0);
        let out = encode(vec![metric_event("latency", MetricKind::Distribution(sketch), &[])]);
        assert!(out.starts_with("latency count=1,"));
        assert!(out.contains("p50="));
        assert!(out.contains("p90="));
        assert!(out.contains("p99="));
    }

    #[test]
    fn tag_values_with_special_characters_are_escaped() {
        let out = encode(vec![metric_event(
            "page.views",
            MetricKind::Counter(1.0),
            &[("path", "a,b c=d")],
        )]);
        assert!(out.contains("path=a\\,b\\ c\\=d"), "got: {out}");
    }

    #[test]
    fn resource_attributes_become_tags_and_event_attrs_override() {
        let mut resource = Resource::default();
        resource.attributes.insert("host", "web1");
        resource.attributes.insert("env", "staging");
        let mut event = metric_event("page.views", MetricKind::Counter(1.0), &[("env", "prod")]);
        event.attributes.insert("env", "prod"); // already set by metric_event; explicit for clarity

        let mut encoder = InfluxLineEncoder;
        let batch = EventBatch { resource: Arc::new(resource), events: vec![event] };
        let out = String::from_utf8(encoder.encode(&batch).unwrap().to_vec()).unwrap();

        assert!(out.contains("host=web1"), "got: {out}");
        assert!(out.contains("env=prod"), "resource's env=staging should be overridden: {out}");
        assert!(!out.contains("env=staging"), "got: {out}");
    }

    #[test]
    fn set_metrics_are_skipped_not_fatal() {
        let out = encode(vec![
            metric_event("unique.users", MetricKind::Set(logit_core::HyperLogLog::default()), &[]),
            metric_event("page.views", MetricKind::Counter(1.0), &[]),
        ]);
        // The Set line is dropped; the Counter line alongside it still comes through.
        assert!(!out.contains("unique.users"), "got: {out}");
        assert!(out.contains("page.views value=1"), "got: {out}");
    }

    #[test]
    fn non_finite_values_are_skipped_not_written_as_invalid_line_protocol() {
        let out = encode(vec![metric_event("bad", MetricKind::Counter(f64::NAN), &[])]);
        assert_eq!(out, "", "a NaN-only line should produce no output, not `value=NaN`");
    }

    #[test]
    fn non_metric_payloads_are_skipped() {
        let log_event = Event {
            timestamp: 0,
            attributes: AttrMap::new(),
            payload: Payload::Log(LogRecord {
                message: Value::str("hello"),
                severity: None,
                body_format: BodyFormat::Raw,
            }),
        };
        assert_eq!(encode(vec![log_event]), "");
    }
}
