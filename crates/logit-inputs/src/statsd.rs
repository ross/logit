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
//!
//! **DogStatsD tag values are zero-copy slices of the datagram**, exactly like every field
//! [`crate::syslog`] extracts: `slice_of` reconstructs each tag value's `Bytes` by pointer
//! arithmetic back into the datagram passed to [`StatsdDecoder::decode`], rather than going
//! through `impl From<&str> for Value` (`Bytes::from(String)`, a fresh copy). Tag *keys* and the
//! metric name don't need this treatment -- both only ever reach [`logit_core::interner::intern`],
//! which hashes/copies into its own table regardless of where the `&str` it's given points.

use crate::udp::{UdpListener, UdpListenerConfig};
use crate::Input;
use bytes::Bytes;
use logit_core::{
    interner::intern, AttrMap, DdSketch, Diagnostics, Event, MetricKind, MetricRecord, Resource,
    Telemetry, Value,
};
use logit_pipeline::Fanout;
use logit_proto::{CodecError, Decoder};
use std::sync::Arc;
use tokio::sync::watch;

/// Thin wrapper over [`UdpListener<StatsdDecoder>`] -- the read/decode split and datagram-\>batch
/// assembly all live there (`docs/adr/0022-decoupled-listener-io.md`); this type is just the
/// decoder choice plus the public constructor/builder surface `logit-cli::pipeline` and this
/// module's own tests already depend on.
pub struct StatsdInput {
    inner: UdpListener<StatsdDecoder>,
}

impl StatsdInput {
    pub fn new(bind: impl Into<String>) -> Self {
        Self {
            inner: UdpListener::new(
                bind,
                StatsdDecoder::new(Arc::new(Resource::default())),
                UdpListenerConfig::default(),
            ),
        }
    }

    /// Attaches a component id to this listener's diagnostics -- and to the [`StatsdDecoder`] it
    /// wraps, so both report under the same id.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.inner = self.inner.with_diagnostics(diag);
        self
    }

    /// Attaches a telemetry handle -- component-specific detail beyond the runtime's uniform
    /// layer-2 metrics (`docs/design/internal-telemetry.md`'s "layer 3"): how many datagrams and
    /// bytes actually arrived on the wire, which `Fanout`-level `events.sent` can't tell apart
    /// from a single busy client.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.inner = self.inner.with_telemetry(telemetry);
        self
    }

    /// Overrides the receive-queue/batching/shutdown-grace knobs a `receive:` config block sets
    /// (`docs/adr/0022-decoupled-listener-io.md`). Defaults to [`UdpListenerConfig::default`] --
    /// today's behaviour -- when never called.
    pub fn with_receive(mut self, config: UdpListenerConfig) -> Self {
        self.inner = self.inner.with_config(config);
        self
    }

    /// The currently-configured receive-queue/batching/shutdown-grace knobs -- for test
    /// introspection (`logit-cli::pipeline`'s `build_spec` wiring tests).
    pub fn receive_config(&self) -> UdpListenerConfig {
        self.inner.config()
    }
}

#[async_trait::async_trait]
impl Input for StatsdInput {
    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        self.inner.run(sink).await
    }

    async fn run_until_shutdown(
        &mut self,
        sink: Fanout,
        shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        self.inner.run_until_shutdown(sink, shutdown).await
    }
}

/// Decodes raw statsd/DogStatsD datagram bytes into an [`EventBatch`]. Split out from
/// [`StatsdInput`] so the parsing logic is directly unit-testable without a socket.
pub struct StatsdDecoder {
    resource: Arc<Resource>,
    diag: Diagnostics,
}

impl StatsdDecoder {
    pub fn new(resource: Arc<Resource>) -> Self {
        Self { resource, diag: Diagnostics::default() }
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }
}

impl Decoder for StatsdDecoder {
    fn decode_into(
        &mut self,
        bytes: Bytes,
        received_at: i64,
        out: &mut Vec<Event>,
    ) -> Result<Arc<Resource>, CodecError> {
        let text = std::str::from_utf8(&bytes)
            .map_err(|e| CodecError::Malformed(format!("invalid utf-8: {e}")))?;
        for line in text.split('\n') {
            let line = line.trim_end_matches('\r').trim();
            if line.is_empty() {
                continue;
            }
            // One malformed line must not discard unrelated valid metrics elsewhere in the same
            // datagram -- StatsD clients routinely pack several independent metrics into one
            // packet, so treating the datagram as atomic would let a single bad line take down
            // everything alongside it. Isolate per line: keep what parsed, report what didn't.
            match parse_line(&bytes, text, line, received_at) {
                Ok(mut line_events) => out.append(&mut line_events),
                Err(err) => {
                    self.diag.warn_throttled("bad_line", err);
                }
            }
        }
        Ok(self.resource.clone())
    }
}

/// Reconstructs a `Bytes` sharing the datagram's underlying allocation for `sub`, a substring
/// derived (through ordinary `&str` slicing -- `split`, `split_once`, `trim_end_matches`/`trim`,
/// indexing) from `text`, which in turn was parsed directly out of `bytes` via `str::from_utf8`.
/// Mirrors `syslog.rs`'s `slice_of` exactly; see that function's doc comment for the full
/// reasoning. The short version: because `sub` is always obtained by slicing `text` rather than by
/// copying or reconstructing it, the pointer-arithmetic round-trip always lands inside `bytes`'s
/// allocation. Unlike `logit-transforms::json::borrowed_str_bytes`, there is no fallback copy here
/// -- a DogStatsD tag value is never unescaped, so there's no case where `sub` could legitimately
/// live outside `bytes`.
fn slice_of(bytes: &Bytes, text: &str, sub: &str) -> Bytes {
    let text_start = text.as_ptr() as usize;
    let sub_start = sub.as_ptr() as usize;
    let start = sub_start - text_start;
    bytes.slice(start..start + sub.len())
}

/// `bytes`/`text` are the *whole datagram* -- the same `Bytes` (and its `&str` view) passed into
/// [`StatsdDecoder::decode`] -- threaded down so [`slice_of`] can reconstruct each tag value as a
/// zero-copy slice of it. `line` is one line of that datagram (already isolated by `decode`, and
/// itself a genuine `&str` slice of `text`), used for parsing and error messages.
fn parse_line(
    bytes: &Bytes,
    text: &str,
    line: &str,
    timestamp: i64,
) -> Result<Vec<Event>, CodecError> {
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
                    // `v` is a genuine `&str` slice of `text` (`split_once` on `tags`, itself
                    // sliced out of `line`/`text`), so `slice_of` shares the datagram's
                    // allocation instead of `Value::from(&str)`'s `Bytes::from(String)` copy.
                    Some((k, v)) => attributes.insert(k, Value::Str(slice_of(bytes, text, v))),
                    // A valueless tag (`#urgent`) marks presence, not a key/value pair -- not a
                    // string value, so there's nothing to slice.
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

    // Cheap for the multi-value form (`name:1:2:3|c`), where this runs once per shared value:
    // every `Value::Str` in `attributes` is already a slice of the datagram's one shared
    // allocation (see `slice_of`), so cloning the map is a `SmallVec` memcpy plus a refcount
    // bump per tag, not a fresh copy of the tag bytes.
    Ok(Event::metric(
        timestamp,
        attributes.clone(),
        MetricRecord { name: intern(name), kind, unit: None },
    ))
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

    /// `decode_into` must stamp every event with the caller's `received_at`, not a fresh
    /// call-time clock read -- the property `docs/adr/0022-decoupled-listener-io.md` exists for:
    /// once decode runs on its own loop, "now" at decode time can be arbitrarily later than
    /// arrival under backlog.
    #[test]
    fn decode_into_stamps_events_with_the_callers_received_at_not_the_current_time() {
        let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()));
        let deliberately_not_now: i64 = 123;
        let mut out = Vec::new();
        decoder
            .decode_into(Bytes::from_static(b"hits:1|c"), deliberately_not_now, &mut out)
            .expect("decode should succeed");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].timestamp, deliberately_not_now);
    }

    /// `decode_into` appends to `out` rather than replacing it -- the property that lets a caller
    /// accumulate several datagrams' events into one reused buffer
    /// (`logit_pipeline::BatchAccumulator`) instead of allocating fresh per datagram.
    #[test]
    fn decode_into_appends_to_an_already_populated_out_buffer_rather_than_replacing_it() {
        let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()));
        let mut out = vec![Event::empty(0, AttrMap::new())];
        decoder
            .decode_into(Bytes::from_static(b"hits:1|c"), 1, &mut out)
            .expect("decode should succeed");
        assert_eq!(out.len(), 2, "the pre-existing event must survive, plus the newly decoded one");
    }

    fn only_metric(events: Vec<Event>) -> MetricRecord {
        assert_eq!(events.len(), 1, "expected exactly one event");
        let mut event = events.into_iter().next().unwrap();
        assert_eq!(event.metrics.len(), 1, "expected exactly one metric on that event");
        // statsd is a metrics-only input: pinning this down here means a future attempt to fold
        // a multi-value line into one multi-metric event (rather than one event per value, as
        // today) fails loudly in this helper rather than silently changing 17 tests' meaning.
        assert!(event.log.is_none() && event.span.is_none(), "statsd emits metric-only events");
        event.metrics.pop().unwrap()
    }

    /// For asserting a specific line is rejected: `decode()` itself now isolates per-line errors
    /// (a malformed line must not discard unrelated valid metrics in the same datagram -- see
    /// `malformed_line_does_not_drop_other_valid_metrics_in_same_datagram` below), so it no
    /// longer surfaces one. `parse_line` is where that rejection actually happens.
    fn parse_err(line: &str) -> CodecError {
        let bytes = Bytes::from(line.to_string());
        let text = std::str::from_utf8(&bytes).unwrap();
        parse_line(&bytes, text, text, 0).expect_err("expected this line to be rejected")
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
    fn dogstatsd_tag_value_is_a_zero_copy_slice_of_the_datagram() {
        // Structural companion to `syslog.rs`'s `emitted_message_is_a_zero_copy_slice_of_the_datagram`
        // -- pins the property this module's `slice_of` exists for, not just its resulting value.
        let datagram = Bytes::from("page.views:1|c|#env:prod".to_string());
        let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()));
        let event = only_metric_event(decoder.decode(datagram.clone()).unwrap().events);
        let tag = event.attributes.get("env").expect("env tag");
        let logit_core::Value::Str(tag) = tag else { panic!("expected Value::Str, got {tag:?}") };

        let base_start = datagram.as_ptr() as usize;
        let base_end = base_start + datagram.len();
        let tag_start = tag.as_ptr() as usize;
        let tag_end = tag_start + tag.len();
        assert!(
            tag_start >= base_start && tag_end <= base_end,
            "tag value should be a slice of the original datagram, not a copy"
        );
    }

    fn only_metric_event(events: Vec<Event>) -> Event {
        assert_eq!(events.len(), 1, "expected exactly one event");
        events.into_iter().next().unwrap()
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
