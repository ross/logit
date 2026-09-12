//! statsd / DogStatsD-tagged metrics over UDP -- the input side of the v0.1 vertical slice
//! (`docs/OVERVIEW.md`: statsd -> transform -> InfluxDB) and, since W3, the input half of
//! [`docs/adr/lossless-transit.md`]'s `statsd_in -> statsd_out` lossless-relay pair
//! (`docs/plans/lossless-transit.md`'s W3).
//!
//! Grammar (superset covering plain statsd and the DogStatsD tag/container-id/timestamp
//! extensions):
//!
//! ```text
//! <name>:<value>[:<value>...]|<type>[|@<sample-rate>][|#<tag>[:<value>],...][|c:<container-id>][|T<unix-seconds>][|<ignored>]
//! ```
//!
//! `<type>` is one of:
//!
//! - `c` (counter) -- one [`Event`] per value, sample-rate-extrapolated (`value / sample_rate`)
//!   into [`logit_core::MetricKind::Sum`], as always.
//! - `g` (gauge) -- one `Event` per value: unsigned into [`logit_core::MetricKind::Gauge`], a
//!   leading `+`/`-` into an unresolved [`logit_core::MetricKind::GaugeDelta`]
//!   (`docs/adr/relative-gauge-adjustments.md`). Sample rate is ignored -- a gauge value is not a
//!   count to extrapolate.
//! - `ms`/`h`/`d` (timing/histogram/distribution) -- **one `Event` per line, not per value**:
//!   every `:`-separated value on the line lands in one [`logit_core::MetricKind::Samples`],
//!   `sample_rate` carried verbatim, with no extrapolation and no sketching at decode time.
//!   [`docs/adr/lossless-transit.md`]'s "summarization is opt-in and named" rule: only
//!   `aggregate` decides whether/how to turn raw samples into a sketch
//!   (`docs/adr/aggregation-window-semantics.md`'s amendment), never this decoder. This replaces
//!   this module's pre-W3 behaviour of sketching straight into a [`logit_core::DdSketch`] at
//!   decode time (extrapolating to `(1.0 / sample_rate).round()` weighted samples, clamped to a
//!   `MAX_SAMPLE_WEIGHT` of 1000) -- both the sketch and the clamp diagnostic moved to
//!   `aggregate`, which owns `Samples::MAX_WEIGHT`/the `samples_cap_exceeded`-style diagnostics
//!   now. The wire type letter survives as the `statsd.type` attribute (rule (b),
//!   `docs/adr/lossless-transit.md`: a protocol-namespaced carrier for something the model
//!   normalizes) since `ms`/`h`/`d` all land on the same `Samples` shape.
//! - `s` (set) -- **one `Event` per line**: every `:`-separated value on the line lands in one
//!   [`logit_core::MetricKind::SetMembers`], each member a zero-copy `Bytes` slice of the
//!   datagram, in wire order. `aggregate` is the only component that turns these into a real
//!   [`logit_core::HyperLogLog`] estimate ([`logit_core::MetricKind::Set`]); sample rate is
//!   ignored, same reasoning as `g`.
//!
//! Multiple `:`-separated values on a `c`/`g` line share one type/sample-rate/tags and become
//! independent events (gauge sign semantics are per value, so folding them into one event would
//! lose which value was which sign); a `ms`/`h`/`d`/`s` line's values stay together on one event
//! instead, matching the shape statsd itself hands them over in. A datagram may contain multiple
//! newline-separated lines.
//!
//! **`|c:<container-id>` and `|T<unix-seconds>` apply to every metric type here**, not only the
//! `c`/`g` the DogStatsD spec itself restricts them to (`docs/design/telemetry-landscape.md`) --
//! a forward-compatible superset, the same stance this decoder already takes toward unrecognized
//! `|` segments generally. `|c:<id>` (v1.2+; v1.4+'s `ci-`/`in-`-prefixed variants land in the
//! same slot verbatim) stamps `statsd.container_id: Value::Str`, a zero-copy datagram slice.
//! `|T<secs>` sets [`Event::timestamp`] to `secs * 1_000_000_000` (checked -- a non-digit or
//! overflowing value rejects *only that line*, as a `CodecError::Malformed`, leaving the rest of
//! the datagram unaffected) instead of the receipt-time timestamp `decode_into`'s `received_at`
//! would otherwise stamp, and stamps `statsd.timestamp: Value::U64(secs)` -- the raw parsed
//! seconds, not just a marker bit -- so a consumer can both tell a wire-supplied timestamp from a
//! receipt-time one *and* read back the exact wire value, independent of whatever
//! `Event::timestamp` becomes downstream (a summarizing stage like `aggregate` rebuilds
//! `Event::timestamp` at flush time; the carrier attribute is what survives that rebuild
//! unchanged). Both attributes are rule-(b) protocol-namespaced carriers
//! (`docs/adr/lossless-transit.md`) for a concept this model has no normalized field for at all.
//! Every other unrecognized `|` segment (DogStatsD events/service checks use different leading
//! sigils entirely) is accepted and silently ignored -- forward-compatible with segment kinds
//! this decoder doesn't know about yet, rather than a hard error on something benign.
//!
//! **DogStatsD tag values, `|c:<id>`, and `s`'s set members are all zero-copy slices of the
//! datagram**, exactly like every field [`crate::syslog`] extracts: `slice_of` reconstructs each
//! one's `Bytes` by pointer arithmetic back into the datagram passed to
//! [`StatsdDecoder::decode`], rather than going through `impl From<&str> for Value`
//! (`Bytes::from(String)`, a fresh copy). Tag *keys* and the metric name don't need this
//! treatment -- both only ever reach [`logit_core::interner::intern`], which hashes/copies into
//! its own table regardless of where the `&str` it's given points.

use crate::udp::{UdpListener, UdpListenerConfig};
use crate::Input;
use bytes::Bytes;
use logit_core::{
    interner::intern, AttrMap, Diagnostics, Event, MetricKind, MetricRecord, Resource, Samples,
    Scope, Telemetry, Value,
};
use logit_pipeline::Fanout;
use logit_proto::{CodecError, Decoder};
use std::sync::Arc;
use tokio::sync::watch;

/// Thin wrapper over [`UdpListener<StatsdDecoder>`] -- the read/decode split and datagram-\>batch
/// assembly all live there (`docs/adr/decoupled-listener-io.md`); this type is just the
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
    /// wraps, so both report under the same id. Both halves matter: `UdpListener`'s own
    /// `diag` is what a whole-datagram decode failure reports through
    /// (`decode_loop`'s `bad_datagram`); the decoder's own `diag` field is what a malformed
    /// *line* inside an otherwise-valid datagram reports through (`bad_line`) -- two distinct
    /// `Diagnostics` values that must both carry the same id and telemetry handle, or one class
    /// of decode failure silently reports under no component id and with telemetry disabled.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.inner =
            self.inner.with_diagnostics(diag.clone()).map_decoder(|d| d.with_diagnostics(diag));
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
    /// (`docs/adr/decoupled-listener-io.md`). Defaults to [`UdpListenerConfig::default`] --
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

    /// Passthrough to the wrapped [`UdpListener::local_addr`] -- mirrors
    /// [`crate::syslog::SyslogInput::local_addr`]: lets a caller (a round-trip test) learn the
    /// real ephemeral port after `bind()`, with no bind-drop race.
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        self.inner.local_addr()
    }
}

#[async_trait::async_trait]
impl Input for StatsdInput {
    async fn bind(&mut self) -> anyhow::Result<()> {
        self.inner.bind().await
    }

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

    /// Test-only: confirms `StatsdInput::with_diagnostics` actually reached this decoder's own
    /// `diag`, not just `UdpListener`'s.
    #[cfg(test)]
    pub(crate) fn diag(&self) -> &Diagnostics {
        &self.diag
    }
}

impl Decoder for StatsdDecoder {
    fn decode_into(
        &mut self,
        bytes: Bytes,
        received_at: i64,
        out: &mut Vec<Event>,
    ) -> Result<(Arc<Resource>, Option<Arc<Scope>>), CodecError> {
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
        // statsd datagrams carry no OTLP instrumentation-scope concept -- `None`, always.
        Ok((self.resource.clone(), None))
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
    // Overridden by `|T<secs>` below; otherwise every event on this line keeps the receipt-time
    // timestamp `decode_into` was called with.
    let mut line_timestamp = timestamp;
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
        } else if let Some(container_id) = extra.strip_prefix("c:") {
            // DogStatsD container id (`|c:<id>`, v1.2+; v1.4+'s `ci-`/`in-`-prefixed variants
            // land in the same slot verbatim -- this decoder carries whatever follows `c:`
            // unchanged, it doesn't parse the prefixed forms specially). Applied to every metric
            // type here, not only `c`/`g` as the spec restricts it to -- see the module doc's
            // forward-compatibility note. Rule (b) (`docs/adr/lossless-transit.md`): a
            // protocol-namespaced carrier for a concept this model has no normalized field for.
            attributes
                .insert("statsd.container_id", Value::Str(slice_of(bytes, text, container_id)));
        } else if let Some(secs) = extra.strip_prefix('T') {
            // DogStatsD point timestamp (`|T<unix-seconds>`, v1.3+, spec-restricted to `c`/`g`
            // but accepted here on every type -- see the module doc). A non-digit or
            // seconds-to-nanoseconds-overflowing value rejects only this line, rather than
            // silently falling back to receipt time.
            let secs: u64 = secs.parse().map_err(|_| malformed())?;
            let nanos = secs
                .checked_mul(1_000_000_000)
                .and_then(|n| i64::try_from(n).ok())
                .ok_or_else(malformed)?;
            line_timestamp = nanos;
            // The carrier holds the parsed wire value itself, not just a marker bit -- so a
            // stage downstream that rebuilds `Event::timestamp` (`aggregate`'s flush, notably)
            // can't fabricate a `|T` value the wire never sent: `statsd_out` reads this attribute
            // directly rather than trusting `event.timestamp`.
            attributes.insert("statsd.timestamp", Value::U64(secs));
        }
        // Anything else (DogStatsD events/service checks use different leading sigils entirely)
        // is accepted and ignored -- forward-compatible with segment kinds this decoder doesn't
        // know about yet, rather than a hard error on something benign.
    }

    match type_part {
        "c" | "g" => values_part
            .split(':')
            .map(|raw_value| {
                build_event(
                    name,
                    raw_value,
                    type_part,
                    sample_rate,
                    &attributes,
                    line_timestamp,
                    line,
                )
            })
            .collect(),
        "ms" | "h" | "d" => {
            // One `Event` per *line*, not per value -- every value on the line shares one
            // `Samples` record (`docs/adr/lossless-transit.md`'s "summarization is opt-in and
            // named": no sketching, no sample-rate extrapolation here; `sample_rate` rides
            // verbatim for `aggregate` to decide about). Pushed straight into `Samples::default`'s
            // own inline `SmallVec` (`SAMPLES_INLINE = 19`, `crates/logit-core/src/metric.rs`)
            // rather than collected into an intermediate `Vec<f64>` first -- an owned `Vec` would
            // be a real allocation even for a single value, which `SmallVec`'s inline storage
            // avoids up to 19 of them.
            let mut samples = Samples::default();
            for raw_value in values_part.split(':') {
                samples.values.push(parse_finite_value(raw_value, "timing/histogram", line)?);
            }
            samples.sample_rate = sample_rate;
            let mut attrs = attributes.clone();
            // The wire type letter survives as `statsd.type` (rule (b)) since `ms`/`h`/`d` all
            // land on the same `Samples` shape -- a zero-copy slice of the datagram, like every
            // other string-valued attribute this decoder stamps.
            attrs.insert("statsd.type", Value::Str(slice_of(bytes, text, type_part)));
            let kind = MetricKind::Samples(samples);
            Ok(vec![Event::metric(line_timestamp, attrs, MetricRecord::new(intern(name), kind))])
        }
        "s" => {
            // One `Event` per line, mirroring `ms`/`h`/`d` above: every member on the line is a
            // zero-copy `Bytes` slice of the datagram, in wire order. `sample_rate` is ignored,
            // same reasoning as `g` -- a set member is not a count to extrapolate.
            let members: Vec<Bytes> =
                values_part.split(':').map(|raw_value| slice_of(bytes, text, raw_value)).collect();
            Ok(vec![Event::metric(
                line_timestamp,
                attributes.clone(),
                MetricRecord::new(intern(name), MetricKind::SetMembers(members)),
            )])
        }
        other => Err(CodecError::Malformed(format!("unknown metric type '{other}': {line:?}"))),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_event(
    name: &str,
    raw_value: &str,
    type_part: &str,
    sample_rate: f64,
    attributes: &AttrMap,
    timestamp: i64,
    line: &str,
) -> Result<Event, CodecError> {
    let kind = match type_part {
        "c" => {
            let value = parse_finite_value(raw_value, "counter", line)?;
            MetricKind::counter(value / sample_rate)
        }
        "g" => {
            // Any leading '+'/'-' means a *relative* adjustment to the gauge's previous value,
            // per the statsd/DogStatsD spec -- and per that same spec there is no wire syntax for
            // setting a gauge to a negative absolute value at all, so a leading '-' is just as
            // unambiguous as '+', not a case needing its own guess. No config escape hatch: this
            // decoder used to reject any signed value outright, so there is no prior working
            // "absolute negative gauge" behavior a `negative_gauge: delta|absolute` toggle could
            // ever have been preserving (see docs/adr/relative-gauge-adjustments.md's
            // Alternatives). `f64::from_str` accepts a leading '+' the same as '-' (pinned by
            // `plus_prefixed_gauge_values_parse_via_from_str`), so `parse_finite_value` handles
            // both signs identically; only the *choice* between `Gauge`/`GaugeDelta` is decided
            // here. Resolution belongs to `aggregate` (docs/design/data-model.md is explicit that
            // aggregation state lives there, not in the wire decoder) -- this decoder only marks
            // the value unresolved and hands it off; a `GaugeDelta` that reaches a sink with no
            // `aggregate` on its path is that component's problem to report, not this one's to
            // guess around.
            //
            // `sample_rate` is deliberately ignored here: a gauge value is absolute (or, for a
            // delta, an adjustment), not a count of occurrences, so there is nothing to
            // extrapolate -- unlike `c`/`ms`/`h`/`d`, "1 in N samples reported this value"
            // doesn't imply anything about the other N-1, and pretending otherwise would be
            // meaningless, not just a missed opportunity.
            let value = parse_finite_value(raw_value, "gauge", line)?;
            if raw_value.starts_with('+') || raw_value.starts_with('-') {
                MetricKind::GaugeDelta(value)
            } else {
                MetricKind::Gauge(value)
            }
        }
        // `ms`/`h`/`d`/`s` never reach here -- `parse_line` handles them itself, one `Event` per
        // line rather than per value.
        other => unreachable!("build_event only handles c/g, got {other:?}"),
    };

    // Cheap for the multi-value form (`name:1:2:3|c`), where this runs once per shared value:
    // every `Value::Str` in `attributes` is already a slice of the datagram's one shared
    // allocation (see `slice_of`), so cloning the map is a `SmallVec` memcpy plus a refcount
    // bump per tag, not a fresh copy of the tag bytes.
    Ok(Event::metric(timestamp, attributes.clone(), MetricRecord::new(intern(name), kind)))
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

    /// Regression: `StatsdInput::with_diagnostics` used to only set `UdpListener`'s own `diag`,
    /// never reaching the wrapped `StatsdDecoder`'s -- so a malformed *line* (as opposed to a
    /// whole malformed datagram) reported through a permanently unnamed, telemetry-disabled
    /// `Diagnostics::default()`, regardless of what the component was actually configured with.
    #[test]
    fn with_diagnostics_reaches_the_wrapped_decoder_too() {
        let input = StatsdInput::new("127.0.0.1:0").with_diagnostics(Diagnostics::new("my-id"));
        assert_eq!(input.inner.decoder().diag().component_id(), "my-id");
    }

    /// `decode_into` must stamp every event with the caller's `received_at`, not a fresh
    /// call-time clock read -- the property `docs/adr/decoupled-listener-io.md` exists for:
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
        assert!(
            matches!(metric.kind, MetricKind::Sum(logit_core::Sum { value, .. }) if value == 1.0)
        );
    }

    #[test]
    fn counter_with_sample_rate_extrapolates() {
        let metric = only_metric(decode("page.views:2|c|@0.5"));
        assert!(matches!(
            metric.kind,
            MetricKind::Sum(logit_core::Sum { value, .. }) if (value - 4.0).abs() < 1e-9
        ));
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

    /// `f64::from_str`'s grammar accepts a leading `+` the same as `-` -- pinned directly, since
    /// `build_event`'s `"g"` arm relies on this to make `parse_finite_value` handle both signs
    /// identically and let only the `starts_with` check decide `Gauge` vs. `GaugeDelta`.
    #[test]
    fn plus_prefixed_gauge_values_parse_via_from_str() {
        assert_eq!("+5".parse::<f64>(), Ok(5.0));
        assert_eq!("+0".parse::<f64>(), Ok(0.0));
    }

    #[test]
    fn a_leading_plus_decodes_as_a_gauge_delta() {
        let metric = only_metric(decode("conns:+5|g"));
        assert!(matches!(metric.kind, MetricKind::GaugeDelta(v) if v == 5.0));
    }

    #[test]
    fn a_leading_minus_decodes_as_a_gauge_delta() {
        let metric = only_metric(decode("conns:-5|g"));
        assert!(matches!(metric.kind, MetricKind::GaugeDelta(v) if v == -5.0));
    }

    /// The unsigned case is unchanged by this workstream -- pinned directly, not just implied by
    /// the pre-existing `gauge` test, since it's the regression that matters most here.
    #[test]
    fn an_unsigned_gauge_value_still_decodes_as_an_absolute_gauge() {
        let metric = only_metric(decode("cpu.load:5|g"));
        assert!(matches!(metric.kind, MetricKind::Gauge(v) if v == 5.0));
    }

    /// `+0` is a legal no-op delta, not an error -- distinct from an *unsigned* `0`, which is
    /// (and stays) an ordinary absolute `Gauge(0.0)`.
    #[test]
    fn a_leading_plus_zero_is_a_legal_no_op_delta_not_an_error() {
        let metric = only_metric(decode("conns:+0|g"));
        assert!(matches!(metric.kind, MetricKind::GaugeDelta(v) if v == 0.0));
    }

    /// A signed non-finite value is still rejected by `parse_finite_value`, same as an unsigned
    /// one -- the sign only decides `Gauge` vs. `GaugeDelta`, never bypasses the finiteness check.
    #[test]
    fn signed_non_finite_gauge_values_are_still_rejected() {
        for value in ["+NaN", "+inf", "-inf"] {
            let line = format!("load:{value}|g");
            assert!(
                matches!(parse_err(&line), CodecError::Malformed(_)),
                "expected {value} to be rejected"
            );
        }
    }

    #[test]
    fn a_signed_gauge_with_tags_and_a_sample_rate_decodes() {
        let events = decode("conns:-5|g|@0.5|#host:web1");
        let event = &events[0];
        assert!(matches!(event.metrics[0].kind, MetricKind::GaugeDelta(v) if v == -5.0));
        assert_eq!(event.attributes.get("host").and_then(|v| v.as_str()), Some("web1"));
    }

    /// Multi-value grammar (`name:v1:v2|type`) applied to signed gauge values: each value is
    /// decoded independently, so a mix of signs on one line yields two independent deltas, not
    /// one merged value or a decode error.
    #[test]
    fn multi_value_signed_gauges_yield_two_independent_deltas() {
        let events = decode("conns:+1:-2|g");
        assert_eq!(events.len(), 2);
        assert!(
            matches!(only_metric(vec![events[0].clone()]).kind, MetricKind::GaugeDelta(v) if v == 1.0)
        );
        assert!(
            matches!(only_metric(vec![events[1].clone()]).kind, MetricKind::GaugeDelta(v) if v == -2.0)
        );
    }

    /// `ms`/`h`/`d` decode straight to raw [`MetricKind::Samples`] now -- no sketching, no
    /// extrapolation (`docs/adr/lossless-transit.md`'s "summarization is opt-in and named": only
    /// `aggregate` sketches). This replaces this test's pre-W3 assertion that a single `ms` value
    /// became a one-count `DdSketch`.
    #[test]
    fn timer_becomes_a_single_sample_distribution() {
        let metric = only_metric(decode("request.latency:120|ms"));
        match metric.kind {
            MetricKind::Samples(samples) => {
                assert_eq!(samples.values.as_slice(), &[120.0]);
                assert_eq!(samples.sample_rate, 1.0);
            }
            other => panic!("expected Samples, got {other:?}"),
        }
    }

    /// Pre-W3 this asserted the `@0.5` rate got extrapolated into two weighted `DdSketch`
    /// samples at decode time. `docs/adr/lossless-transit.md`'s "summarization is opt-in and
    /// named" moves that extrapolation to `aggregate` -- the raw rate now rides verbatim on the
    /// decoded `Samples` instead.
    #[test]
    fn sampled_distribution_at_half_rate_preserves_the_rate_without_extrapolating() {
        let metric = only_metric(decode("x:100|ms|@0.5"));
        match metric.kind {
            MetricKind::Samples(samples) => {
                assert_eq!(samples.values.as_slice(), &[100.0]);
                assert_eq!(samples.sample_rate, 0.5);
            }
            other => panic!("expected Samples, got {other:?}"),
        }
    }

    /// Same shift as the half-rate test above, at `@0.1`.
    #[test]
    fn sampled_distribution_at_tenth_rate_preserves_the_rate_without_extrapolating() {
        let metric = only_metric(decode("x:100|ms|@0.1"));
        match metric.kind {
            MetricKind::Samples(samples) => {
                assert_eq!(samples.values.as_slice(), &[100.0]);
                assert_eq!(samples.sample_rate, 0.1);
            }
            other => panic!("expected Samples, got {other:?}"),
        }
    }

    /// An explicit `@1` (the default, unsampled rate) still decodes to one raw value at rate
    /// `1.0` -- unchanged in spirit from this test's pre-W3 "one sample, not extrapolated"
    /// claim, just against `Samples` instead of a `DdSketch`. `statsd_decode_one_line` in
    /// `crates/logit-bench/tests/allocations.rs` pins the same claim at the allocation level.
    #[test]
    fn unsampled_distribution_still_inserts_exactly_one_sample() {
        let metric = only_metric(decode("x:100|ms|@1"));
        match metric.kind {
            MetricKind::Samples(samples) => {
                assert_eq!(samples.values.as_slice(), &[100.0]);
                assert_eq!(samples.sample_rate, 1.0);
            }
            other => panic!("expected Samples, got {other:?}"),
        }
    }

    /// `ms`/`h`/`d` share one `Samples` record per *line*, not per value -- every `:`-separated
    /// value on the line lands in `values`, in wire order, on a single `Event`.
    #[test]
    fn multi_value_timer_produces_one_event_with_all_values() {
        let events = decode("request.latency:100:200:300|ms");
        assert_eq!(events.len(), 1, "ms/h/d lines are one event per line, not per value");
        let metric = only_metric(events);
        match metric.kind {
            MetricKind::Samples(samples) => {
                assert_eq!(samples.values.as_slice(), &[100.0, 200.0, 300.0]);
            }
            other => panic!("expected Samples, got {other:?}"),
        }
    }

    /// The wire type letter survives as `statsd.type` (rule (b), `docs/adr/lossless-transit.md`)
    /// on every one of the three types that normalize onto the same `Samples` shape.
    #[test]
    fn statsd_type_is_stamped_for_each_timer_type() {
        for (line, expected) in [("x:1|ms", "ms"), ("x:1|h", "h"), ("x:1|d", "d")] {
            let events = decode(line);
            assert_eq!(
                events[0].attributes.get("statsd.type").and_then(|v| v.as_str()),
                Some(expected),
                "statsd.type should be stamped for {line:?}"
            );
        }
    }

    // The weight-clamping and decode-time-sketch-quantile-accuracy coverage that used to live
    // here (an extreme `@rate` clamping to `MAX_SAMPLE_WEIGHT`, and a sampled distribution's
    // quantile staying within the configured relative error bound) moved to `aggregate`, the
    // only component that sketches a `Samples` record now
    // (`docs/adr/aggregation-window-semantics.md`'s amendment) -- see
    // `crates/logit-transforms/src/aggregate.rs`'s
    // `samples_sketch_mode_merges_weighted_values_and_counts_weight_clamp` for the clamp, and
    // `logit_core::metric`'s `Samples::sketch` tests for the quantile-accuracy claim. This
    // decoder no longer builds a `DdSketch` at all.

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

    /// `s` decodes to raw `SetMembers` now -- one member, a zero-copy slice of the datagram.
    /// Replaces this test's pre-W3 "not implemented" assertion.
    #[test]
    fn set_type_becomes_set_members() {
        let metric = only_metric(decode("unique.users:abc123|s"));
        match metric.kind {
            MetricKind::SetMembers(members) => {
                assert_eq!(members, vec![Bytes::from_static(b"abc123")])
            }
            other => panic!("expected SetMembers, got {other:?}"),
        }
    }

    /// `s` shares one `SetMembers` record per line, same as `ms`/`h`/`d`: every `:`-separated
    /// value on the line lands in the same event, in wire order.
    #[test]
    fn multi_value_set_produces_one_event_with_all_members() {
        let events = decode("unique.users:abc123:def456|s");
        assert_eq!(events.len(), 1, "s lines are one event per line, not per value");
        let metric = only_metric(events);
        match metric.kind {
            MetricKind::SetMembers(members) => {
                assert_eq!(
                    members,
                    vec![Bytes::from_static(b"abc123"), Bytes::from_static(b"def456")]
                );
            }
            other => panic!("expected SetMembers, got {other:?}"),
        }
    }

    #[test]
    fn blank_lines_are_skipped() {
        let events = decode("\n\na:1|c\n\n");
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn container_id_segment_becomes_an_attribute() {
        let events = decode("hits:1|c|c:abcdef0123456789");
        assert_eq!(
            events[0].attributes.get("statsd.container_id").and_then(|v| v.as_str()),
            Some("abcdef0123456789")
        );
    }

    /// `|c:<id>` applies to every metric type here, not only `c`/`g` as the DogStatsD spec
    /// itself restricts it to -- see the module doc's forward-compatibility note.
    #[test]
    fn container_id_segment_applies_to_every_metric_type() {
        for line in ["x:1|ms|c:cid", "x:1|s|c:cid", "x:1|g|c:cid"] {
            let events = decode(line);
            assert_eq!(
                events[0].attributes.get("statsd.container_id").and_then(|v| v.as_str()),
                Some("cid"),
                "expected statsd.container_id on {line:?}"
            );
        }
    }

    #[test]
    fn timestamp_segment_sets_the_event_timestamp_and_marker() {
        let events = decode("hits:1|c|T1700000000");
        let event = &events[0];
        assert_eq!(event.timestamp, 1_700_000_000 * 1_000_000_000);
        assert_eq!(event.attributes.get("statsd.timestamp"), Some(&Value::U64(1_700_000_000)));
    }

    #[test]
    fn malformed_timestamp_segment_rejects_only_that_line() {
        assert!(matches!(parse_err("hits:1|c|Tabc"), CodecError::Malformed(_)), "non-digit");
        assert!(matches!(parse_err("hits:1|c|T-5"), CodecError::Malformed(_)), "negative");
        assert!(
            matches!(parse_err("hits:1|c|T18446744073709551615"), CodecError::Malformed(_)),
            "seconds-to-nanoseconds overflow"
        );

        // A malformed |T must not take down the rest of the datagram -- same isolation contract
        // as any other malformed line.
        let events = decode("a:1|c|Tbad\nb:2|c");
        assert_eq!(events.len(), 1, "only the malformed-T line should be dropped");
        assert_eq!(intern("b"), only_metric(events).name);
    }

    /// `|c:`/`|T`/`@rate`/`#tags` combine freely, in any order, on the same line.
    #[test]
    fn container_id_timestamp_rate_and_tags_combine_in_any_order() {
        let orderings = [
            "x:100|ms|@0.5|#env:prod|c:abc123|T1700000000",
            "x:100|ms|c:abc123|T1700000000|@0.5|#env:prod",
            "x:100|ms|T1700000000|#env:prod|c:abc123|@0.5",
            "x:100|ms|#env:prod|@0.5|T1700000000|c:abc123",
        ];
        for line in orderings {
            let events = decode(line);
            assert_eq!(events.len(), 1, "expected one event for {line:?}");
            let event = &events[0];
            assert_eq!(event.timestamp, 1_700_000_000 * 1_000_000_000, "line: {line:?}");
            assert_eq!(
                event.attributes.get("statsd.container_id").and_then(|v| v.as_str()),
                Some("abc123"),
                "line: {line:?}"
            );
            assert_eq!(
                event.attributes.get("env").and_then(|v| v.as_str()),
                Some("prod"),
                "line: {line:?}"
            );
            assert_eq!(
                event.attributes.get("statsd.timestamp"),
                Some(&Value::U64(1_700_000_000)),
                "line: {line:?}"
            );
            match &event.metrics[0].kind {
                MetricKind::Samples(samples) => {
                    assert_eq!(samples.sample_rate, 0.5, "line: {line:?}")
                }
                other => panic!("expected Samples, got {other:?}"),
            }
        }
    }

    /// Mirrors `syslog.rs`'s own `local_addr`-after-`bind` property (`crates/logit-inputs/src/
    /// udp.rs`'s `bind_then_run_delivers_a_real_datagram`): no address before `bind()`, a real
    /// one after.
    #[tokio::test]
    async fn local_addr_is_available_after_bind() {
        let mut input = StatsdInput::new("127.0.0.1:0");
        assert_eq!(input.local_addr(), None, "no address before bind()");

        input.bind().await.expect("binding an ephemeral port should succeed");
        let addr = input.local_addr().expect("bind() should leave a real address behind");
        assert_eq!(addr.ip().to_string(), "127.0.0.1");
    }
}
