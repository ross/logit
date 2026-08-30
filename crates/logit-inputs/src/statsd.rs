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
use logit_pipeline::Fanout;
use logit_proto::{CodecError, Decoder};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;

pub struct StatsdInput {
    pub bind: String,
}

#[async_trait::async_trait]
impl Input for StatsdInput {
    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        let socket = UdpSocket::bind(&self.bind).await?;
        let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()));
        // The largest possible UDP payload (65535 minus the 8-byte UDP header).
        let mut buf = vec![0u8; 65_507];
        loop {
            let (n, _peer) = socket.recv_from(&mut buf).await?;
            let bytes = Bytes::copy_from_slice(&buf[..n]);
            match decoder.decode(bytes) {
                Ok(batch) if !batch.events.is_empty() => {
                    // `Fanout::send` has no per-consumer failure signal to react to -- a closed
                    // consumer is silently skipped (`docs/design/pipeline-graph.md`'s backpressure
                    // section notes this as a named open question, not solved here).
                    sink.send(batch).await;
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
            // One malformed line must not discard unrelated valid metrics elsewhere in the same
            // datagram -- StatsD clients routinely pack several independent metrics into one
            // packet, so treating the datagram as atomic would let a single bad line take down
            // everything alongside it. Isolate per line: keep what parsed, report what didn't.
            match parse_line(line, timestamp) {
                Ok(mut line_events) => events.append(&mut line_events),
                // TODO: route through a proper diagnostics facility once one exists, instead of
                // stderr -- same gap noted in StatsdInput::run.
                Err(err) => eprintln!("statsd: {err}"),
            }
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
            let parsed: f64 = rate.parse().map_err(|_| malformed())?;
            // A sample rate is a probability: it must be finite and in (0, 1]. `f64::parse`
            // happily accepts "NaN"/"inf"/negative/zero/>1 text, any of which would turn into a
            // non-finite or negative Counter (or a divide-by-zero) below -- reject them here
            // rather than let bad input poison a value that later gets merged and shipped.
            if !parsed.is_finite() || parsed <= 0.0 || parsed > 1.0 {
                return Err(malformed());
            }
            sample_rate = parsed;
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
            let value = parse_finite_value(raw_value, "counter", line)?;
            MetricKind::Counter(value / sample_rate)
        }
        "g" => {
            // A leading '+'/'-' marks a *relative* adjustment to the gauge's previous value, per
            // the DogStatsD spec -- and per that same spec, a plain negative number is
            // indistinguishable from a relative decrement, so any leading sign is ambiguous, not
            // just '+'. Applying a relative update needs state this decoder doesn't have (it
            // belongs to the future `aggregate` processor, docs/design/lua-api.md's `flush()`
            // contract); silently reinterpreting a delta as an absolute value would produce a
            // wrong number that looks correct, so this rejects it the same way `s` (set) is
            // rejected below: a clear not-implemented error instead of quietly wrong data.
            if raw_value.starts_with('+') || raw_value.starts_with('-') {
                return Err(malformed(
                    "relative gauge adjustments ('+'/'-') are not implemented yet",
                ));
            }
            let value = parse_finite_value(raw_value, "gauge", line)?;
            MetricKind::Gauge(value)
        }
        "ms" | "h" | "d" => {
            let value = parse_finite_value(raw_value, "timing/histogram", line)?;
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

/// Parses a metric value and rejects it unless finite. `f64::parse` accepts the literal text
/// "NaN"/"inf"/"-inf", which would otherwise become `Counter(NaN)`, `Gauge(inf)`, or -- worse --
/// get inserted into a `DdSketch`, where a NaN sample corrupts the sketch's summary state rather
/// than just producing one bad data point. Shared by the counter/gauge/timing-histogram-
/// distribution branches in `build_event`, which differ only in the value's name for the error.
fn parse_finite_value(raw_value: &str, what: &str, line: &str) -> Result<f64, CodecError> {
    let value: f64 = raw_value
        .parse()
        .map_err(|_| CodecError::Malformed(format!("invalid {what} value: {line:?}")))?;
    if !value.is_finite() {
        return Err(CodecError::Malformed(format!("{what} value must be finite: {line:?}")));
    }
    Ok(value)
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

    /// For asserting a specific line is rejected: `decode()` itself now isolates per-line errors
    /// (a malformed line must not discard unrelated valid metrics in the same datagram -- see
    /// `malformed_line_does_not_drop_other_valid_metrics_in_same_datagram` below), so it no
    /// longer surfaces one. `parse_line` is where that rejection actually happens.
    fn parse_err(line: &str) -> CodecError {
        parse_line(line, 0).expect_err("expected this line to be rejected")
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
    fn invalid_sample_rates_are_rejected() {
        // Zero would divide-by-zero into an infinite counter; negative and >1 aren't valid
        // probabilities; NaN/inf parse successfully as f64 but aren't finite. Any of these would
        // otherwise poison a Counter value that later gets merged and shipped downstream.
        for rate in ["0", "-0.5", "1.5", "NaN", "inf", "-inf"] {
            let line = format!("hits:1|c|@{rate}");
            assert!(
                matches!(parse_err(&line), CodecError::Malformed(_)),
                "expected @{rate} to be rejected"
            );
        }
    }

    #[test]
    fn non_finite_counter_values_are_rejected() {
        // `f64::parse` accepts the literal text "NaN"/"inf"/"-inf" -- unguarded, these would
        // become Counter(NaN) or Counter(inf) rather than being caught at decode time.
        for value in ["NaN", "inf", "-inf"] {
            let line = format!("hits:{value}|c");
            assert!(
                matches!(parse_err(&line), CodecError::Malformed(_)),
                "expected {value} to be rejected"
            );
        }
    }

    #[test]
    fn non_finite_gauge_values_are_rejected() {
        for value in ["NaN", "inf", "-inf"] {
            let line = format!("load:{value}|g");
            assert!(
                matches!(parse_err(&line), CodecError::Malformed(_)),
                "expected {value} to be rejected"
            );
        }
    }

    #[test]
    fn non_finite_distribution_values_are_rejected() {
        // Worse than a bad Counter/Gauge: a NaN sample inserted into a DdSketch corrupts the
        // sketch's summary state rather than just producing one bad data point.
        for value in ["NaN", "inf", "-inf"] {
            let line = format!("latency:{value}|ms");
            assert!(
                matches!(parse_err(&line), CodecError::Malformed(_)),
                "expected {value} to be rejected"
            );
        }
    }

    #[test]
    fn gauge() {
        let metric = only_metric(decode("cpu.load:0.75|g"));
        assert!(matches!(metric.kind, MetricKind::Gauge(v) if v == 0.75));
    }

    #[test]
    fn gauge_relative_adjustments_are_rejected_not_silently_reinterpreted() {
        // Per the DogStatsD spec, a leading '+' or '-' means "adjust the previous value by this
        // much", which this decoder can't apply (no state). Silently treating '+5' or '-5' as an
        // absolute value would produce a wrong number that looks like a correct one, so both are
        // rejected rather than guessed at.
        assert!(matches!(parse_err("cpu.load:+5|g"), CodecError::Malformed(_)));
        assert!(matches!(parse_err("cpu.load:-5|g"), CodecError::Malformed(_)));
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
    fn malformed_line_does_not_drop_other_valid_metrics_in_same_datagram() {
        // A datagram is not atomic: StatsD clients routinely pack several independent metrics
        // into one packet, so one bad line must not discard unrelated valid ones alongside it.
        let events = decode("a:1|c\nbad\nb:2|c");
        assert_eq!(
            events.len(),
            2,
            "expected both 'a' and 'b' to survive the malformed middle line"
        );
        assert_eq!(intern("a"), only_metric(vec![events[0].clone()]).name);
        assert_eq!(intern("b"), only_metric(vec![events[1].clone()]).name);
    }

    #[test]
    fn unknown_type_is_rejected() {
        assert!(matches!(parse_err("x:1|zz"), CodecError::Malformed(_)));
    }

    #[test]
    fn missing_colon_is_rejected() {
        assert!(matches!(parse_err("nocolon|c"), CodecError::Malformed(_)));
    }

    #[test]
    fn set_type_is_a_clear_not_implemented_error() {
        assert!(matches!(parse_err("unique.users:abc123|s"), CodecError::Malformed(_)));
    }

    #[test]
    fn blank_lines_are_skipped() {
        let events = decode("\n\na:1|c\n\n");
        assert_eq!(events.len(), 1);
    }
}
