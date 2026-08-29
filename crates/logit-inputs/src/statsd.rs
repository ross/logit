//! statsd / DogStatsD-tagged metrics over UDP -- the input side of the v0.1 vertical slice
//! (`docs/OVERVIEW.md`: statsd -> transform -> InfluxDB).
//!
//! Grammar (superset covering both plain statsd and the DogStatsD tag extension):
//!
//! ```text
//! <name>:<value>[:<value>...]|<type>[|@<sample-rate>][|#<tag>[:<value>],...][|<ignored>]
//! ```
//!
//! `<type>` is one of `c` (counter), `g` (gauge), `ms`/`h`/`d` (timing/histogram/distribution --
//! all decoded as a single-sample [`logit_core::DdSketch`]), or `s` (set, not yet implemented --
//! see the note on [`HyperLogLog`](logit_core::HyperLogLog)). Multiple `:`-separated values share
//! one type/sample-rate/tags and become one [`Event`] each. A datagram may contain multiple
//! newline-separated lines.

use crate::Input;
use bytes::Bytes;
use logit_core::{
    interner::intern, AttrMap, DdSketch, Event, EventBatch, MetricKind, MetricRecord, Payload,
    Resource,
};
use logit_proto::{CodecError, Decoder};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::Sender;

pub struct StatsdInput {
    pub bind: String,
}

#[async_trait::async_trait]
impl Input for StatsdInput {
    async fn run(&mut self, sink: Sender<EventBatch>) -> anyhow::Result<()> {
        let socket = UdpSocket::bind(&self.bind).await?;
        let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()));
        // The largest possible UDP payload (65535 minus the 8-byte UDP header).
        let mut buf = vec![0u8; 65_507];
        loop {
            let (n, _peer) = socket.recv_from(&mut buf).await?;
            let bytes = Bytes::copy_from_slice(&buf[..n]);
            match decoder.decode(bytes) {
                Ok(batch) if !batch.events.is_empty() => {
                    if sink.send(batch).await.is_err() {
                        // Receiver gone: the pipeline is shutting down, not an input error.
                        return Ok(());
                    }
                }
                Ok(_) => {} // empty datagram
                Err(err) => {
                    // A malformed line from one client shouldn't take the whole listener down.
                    // TODO: route through a proper diagnostics facility once one exists, instead
                    // of stderr.
                    eprintln!("statsd: {err}");
                }
            }
        }
    }
}

/// Decodes raw statsd/DogStatsD datagram bytes into an [`EventBatch`]. Split out from
/// [`StatsdInput`] so the parsing logic is directly unit-testable without a socket.
pub struct StatsdDecoder {
    resource: Arc<Resource>,
}

impl StatsdDecoder {
    pub fn new(resource: Arc<Resource>) -> Self {
        Self { resource }
    }
}

impl Decoder for StatsdDecoder {
    fn decode(&mut self, bytes: Bytes) -> Result<EventBatch, CodecError> {
        let text = std::str::from_utf8(&bytes)
            .map_err(|e| CodecError::Malformed(format!("invalid utf-8: {e}")))?;
        let timestamp = now_nanos();
        let mut events = Vec::new();
        for line in text.split('\n') {
            let line = line.trim_end_matches('\r').trim();
            if line.is_empty() {
                continue;
            }
            events.extend(parse_line(line, timestamp)?);
        }
        Ok(EventBatch { resource: self.resource.clone(), events })
    }
}

fn now_nanos() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as i64
}

fn parse_line(line: &str, timestamp: i64) -> Result<Vec<Event>, CodecError> {
    let malformed = || CodecError::Malformed(format!("malformed statsd line: {line:?}"));

    let (name, rest) = line.split_once(':').ok_or_else(malformed)?;
    if name.is_empty() {
        return Err(malformed());
    }

    let mut segments = rest.split('|');
    let values_part = segments.next().ok_or_else(malformed)?;
    let type_part = segments.next().ok_or_else(malformed)?;

    let mut sample_rate = 1.0f64;
    let mut attributes = AttrMap::new();
    for extra in segments {
        if let Some(rate) = extra.strip_prefix('@') {
            sample_rate = rate.parse().map_err(|_| malformed())?;
        } else if let Some(tags) = extra.strip_prefix('#') {
            for tag in tags.split(',').filter(|t| !t.is_empty()) {
                match tag.split_once(':') {
                    Some((k, v)) => attributes.insert(k, v),
                    // A valueless tag (`#urgent`) marks presence, not a key/value pair.
                    None => attributes.insert(tag, true),
                }
            }
        }
        // Anything else (e.g. DogStatsD's `|c:<container-id>`) is accepted and ignored --
        // forward-compatible with segment kinds this decoder doesn't know about yet, rather than
        // a hard error on something benign.
    }

    values_part
        .split(':')
        .map(|raw_value| {
            build_event(name, raw_value, type_part, sample_rate, &attributes, timestamp, line)
        })
        .collect()
}

fn build_event(
    name: &str,
    raw_value: &str,
    type_part: &str,
    sample_rate: f64,
    attributes: &AttrMap,
    timestamp: i64,
    line: &str,
) -> Result<Event, CodecError> {
    let malformed = |what: &str| CodecError::Malformed(format!("{what}: {line:?}"));

    let kind = match type_part {
        "c" => {
            let value: f64 = raw_value.parse().map_err(|_| malformed("invalid counter value"))?;
            MetricKind::Counter(value / sample_rate)
        }
        "g" => {
            // TODO: a leading '+'/'-' marks a *relative* adjustment to the gauge's previous
            // value, per the DogStatsD spec -- that needs state this decoder doesn't have, and
            // belongs to the `aggregate` processor (docs/design/lua-api.md's `flush()` contract),
            // not here. For now the magnitude decodes as an absolute value; `str::parse` rejects
            // a leading '+' outright, so it's stripped uniformly first.
            let value: f64 = raw_value
                .strip_prefix('+')
                .unwrap_or(raw_value)
                .parse()
                .map_err(|_| malformed("invalid gauge value"))?;
            MetricKind::Gauge(value)
        }
        "ms" | "h" | "d" => {
            let value: f64 =
                raw_value.parse().map_err(|_| malformed("invalid timing/histogram value"))?;
            // TODO: DDSketch has no native weighted-add, so a sample rate < 1 here is decoded as
            // a single unweighted sample rather than extrapolated -- a smaller gap in practice
            // than for counters, since timings/histograms are rarely sampled in DogStatsD
            // clients, but a gap nonetheless.
            let mut sketch = DdSketch::new();
            sketch.add(value);
            MetricKind::Distribution(sketch)
        }
        "s" => {
            // See the note on `HyperLogLog` in logit-core::metric: not implemented yet.
            return Err(malformed("set metrics ('s') are not implemented yet"));
        }
        other => return Err(malformed(&format!("unknown metric type '{other}'"))),
    };

    Ok(Event {
        timestamp,
        attributes: attributes.clone(),
        payload: Payload::Metric(MetricRecord { name: intern(name), kind, unit: None }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(line: &str) -> Vec<Event> {
        let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()));
        decoder.decode(Bytes::from(line.to_string())).expect("decode should succeed").events
    }

    fn only_metric(events: Vec<Event>) -> MetricRecord {
        assert_eq!(events.len(), 1, "expected exactly one event");
        match events.into_iter().next().unwrap().payload {
            Payload::Metric(m) => m,
            _ => panic!("expected a Metric payload"),
        }
    }

    #[test]
    fn counter() {
        let metric = only_metric(decode("page.views:1|c"));
        assert_eq!(intern("page.views"), metric.name);
        assert!(matches!(metric.kind, MetricKind::Counter(v) if v == 1.0));
    }

    #[test]
    fn counter_with_sample_rate_extrapolates() {
        let metric = only_metric(decode("page.views:2|c|@0.5"));
        assert!(matches!(metric.kind, MetricKind::Counter(v) if (v - 4.0).abs() < 1e-9));
    }

    #[test]
    fn gauge() {
        let metric = only_metric(decode("cpu.load:0.75|g"));
        assert!(matches!(metric.kind, MetricKind::Gauge(v) if v == 0.75));
    }

    #[test]
    fn gauge_leading_plus_is_stripped() {
        let metric = only_metric(decode("cpu.load:+5|g"));
        assert!(matches!(metric.kind, MetricKind::Gauge(v) if v == 5.0));
    }

    #[test]
    fn timer_becomes_a_single_sample_distribution() {
        let metric = only_metric(decode("request.latency:120|ms"));
        match metric.kind {
            MetricKind::Distribution(sketch) => {
                assert_eq!(sketch.count(), 1);
                let q = sketch.quantile(0.5).expect("quantile should be present");
                assert!((q - 120.0).abs() < 1.0, "quantile {q} should be close to 120");
            }
            other => panic!("expected Distribution, got {other:?}"),
        }
    }

    #[test]
    fn dogstatsd_tags_become_attributes() {
        let events = decode("page.views:1|c|#env:prod,host:web1,urgent");
        let event = &events[0];
        assert_eq!(event.attributes.get("env").and_then(|v| v.as_str()), Some("prod"));
        assert_eq!(event.attributes.get("host").and_then(|v| v.as_str()), Some("web1"));
        assert!(matches!(event.attributes.get("urgent"), Some(logit_core::Value::Bool(true))));
    }

    #[test]
    fn multi_value_shares_type_and_tags() {
        let events = decode("page.views:1:2:3|c|#env:prod");
        assert_eq!(events.len(), 3);
        for event in &events {
            assert_eq!(event.attributes.get("env").and_then(|v| v.as_str()), Some("prod"));
        }
    }

    #[test]
    fn multiple_lines_in_one_datagram() {
        let events = decode("a:1|c\nb:2|c\n");
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn unknown_type_is_rejected() {
        let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()));
        let err = decoder.decode(Bytes::from_static(b"x:1|zz")).unwrap_err();
        assert!(matches!(err, CodecError::Malformed(_)));
    }

    #[test]
    fn missing_colon_is_rejected() {
        let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()));
        let err = decoder.decode(Bytes::from_static(b"nocolon|c")).unwrap_err();
        assert!(matches!(err, CodecError::Malformed(_)));
    }

    #[test]
    fn set_type_is_a_clear_not_implemented_error() {
        let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()));
        let err = decoder.decode(Bytes::from_static(b"unique.users:abc123|s")).unwrap_err();
        assert!(matches!(err, CodecError::Malformed(_)));
    }

    #[test]
    fn blank_lines_are_skipped() {
        let events = decode("\n\na:1|c\n\n");
        assert_eq!(events.len(), 1);
    }
}
