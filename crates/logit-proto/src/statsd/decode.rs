//! Decoding statsd and DogStatsD text into events: the code behind [`super`]'s module doc, which
//! is the spec for everything here.
//!
//! [`StatsdDecoder::decode_into`] splits its input on `\n` and hands each line to `parse_line`,
//! which picks the metric, event, or service-check parser by the line's leading sigil. Framing is
//! the listener's job: `statsd_in` hands this decoder a whole datagram or one framed packet.

use crate::{CodecError, Decoder};
use bytes::Bytes;
use logit_core::subslice;
use logit_core::{
    interner::{intern, KeyCache},
    AttrMap, BodyFormat, Diagnostics, Event, LogRecord, MetricKind, MetricRecord, Resource,
    Samples, Scope, Severity, Symbol, Value,
};
use std::sync::{Arc, LazyLock};

/// Decodes statsd/DogStatsD bytes into events; testable without a socket.
///
/// `Clone` because `statsd_in`'s TCP driver gives every connection its own decoder
/// (`crates/logit-inputs/src/tcp.rs`'s module doc). A clone shares the one
/// `Arc<Resource>`, which must stay shared: `logit_pipeline::BatchAccumulator::absorb` keys on
/// `Arc::ptr_eq`, so a resource per connection would stop two connections' events sharing a batch.
/// It also shares its `Diagnostics` throttle counts, so `bad_line` throttles listener-wide.
///
/// On TCP the driver hands this one already-delimited line, so [`Self::decode_into`]'s `\n` split
/// is a single iteration. A UDP or Unix datagram, and a `unix_stream` length-prefixed packet, may
/// hold many lines, and the split is their only line framing. Either way no `with_line_splitting`
/// switch is needed, unlike `syslog_in`'s `SyslogDecoder`: an octet-counted syslog frame may
/// contain a `\n` that is message content, and a statsd line never can.
#[derive(Clone)]
pub struct StatsdDecoder {
    resource: Arc<Resource>,
    diag: Diagnostics,
    /// Tag keys memoised `&str -> Symbol`: a client's tag names repeat on every line, so after the
    /// first each is a `memcmp`, not an interner probe. The `statsd.*` carrier keys are in `KEYS`.
    keys: KeyCache,
}

impl StatsdDecoder {
    pub fn new(resource: Arc<Resource>) -> Self {
        Self { resource, diag: Diagnostics::default(), keys: KeyCache::new() }
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    /// This decoder's diagnostics handle, public for
    /// [`crate::collectd::CollectdDecoder::diag`]'s reason.
    pub fn diag(&self) -> &Diagnostics {
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
            // Trailing whitespace is payload on an `_e{`/`_sc|` line (`super`'s module doc,
            // "DogStatsD events and service checks"), so only other lines are trimmed at the end.
            let line = line.trim_end_matches('\r').trim_start();
            let line = if line.starts_with("_e{") || line.starts_with("_sc|") {
                line
            } else {
                line.trim_end()
            };
            if line.is_empty() {
                continue;
            }
            // Per-line isolation: clients pack independent metrics into one datagram, so a bad
            // line is reported and skipped without discarding the others.
            match parse_line(&bytes, line, received_at, &mut self.keys) {
                Ok(mut line_events) => out.append(&mut line_events),
                Err(err) => {
                    self.diag.warn_throttled("bad_line", err);
                }
            }
        }
        // statsd has no instrumentation scope.
        Ok((self.resource.clone(), None))
    }
}

/// The `statsd.*` carrier keys, interned once per process so each line pays a sorted
/// `insert_sym`, not an interner hash and shard lock. A `LazyLock` rather than a decoder field
/// because the parsers are free functions; `KEYS.x` is one acquire load after first use.
static KEYS: LazyLock<StatsdKeys> = LazyLock::new(|| StatsdKeys {
    container_id: intern("statsd.container_id"),
    external_data: intern("statsd.external_data"),
    cardinality: intern("statsd.cardinality"),
    timestamp: intern("statsd.timestamp"),
    type_: intern("statsd.type"),
    event_title: intern("statsd.event.title"),
    event_host: intern("statsd.event.host"),
    event_priority: intern("statsd.event.priority"),
    event_aggregation_key: intern("statsd.event.aggregation_key"),
    event_source_type: intern("statsd.event.source_type"),
    service_check_name: intern("statsd.service_check.name"),
    service_check_status: intern("statsd.service_check.status"),
    service_check_host: intern("statsd.service_check.host"),
});

struct StatsdKeys {
    container_id: Symbol,
    external_data: Symbol,
    cardinality: Symbol,
    timestamp: Symbol,
    type_: Symbol,
    event_title: Symbol,
    event_host: Symbol,
    event_priority: Symbol,
    event_aggregation_key: Symbol,
    event_source_type: Symbol,
    service_check_name: Symbol,
    service_check_status: Symbol,
    service_check_host: Symbol,
}

/// Folds a `#<tag>[:<value>],...` payload (the text after `#`) into `attributes`, for a metric
/// line, an event, or a service check alike.
///
/// A valued tag is a zero-copy slice of the datagram; a bare one is `Value::Bool(true)`. A
/// repeated key folds into a `Value::Array` in wire order and an exact duplicate token is deduped;
/// `super`'s module doc, "DogStatsD tags", has the full rule and what it leaves alone. The
/// remove-then-insert per token is two binary searches over the sorted map, the same two-step
/// `syslog_in`'s `insert_param` uses.
fn insert_tags(attributes: &mut AttrMap, bytes: &Bytes, tags: &str, keys: &mut KeyCache) {
    for tag in tags.split(',').filter(|t| !t.is_empty()) {
        let (key, value) = match tag.split_once(':') {
            Some((k, v)) => (k, Value::Str(subslice::share(bytes, v.as_bytes()))),
            None => (tag, Value::Bool(true)),
        };
        let key = keys.get_or_intern(key);
        let merged = match attributes.remove_sym(key) {
            None => value,
            Some(Value::Array(mut arr)) => {
                if !arr.iter().any(|e| tag_element_eq(e, &value)) {
                    arr.push(value);
                }
                Value::Array(arr)
            }
            Some(existing) if tag_element_eq(&existing, &value) => existing,
            Some(existing) => Value::Array(vec![existing, value]),
        };
        attributes.insert_sym(key, merged);
    }
}

/// Exact-token equality for [`insert_tags`]'s dedupe: `Str`/`Str` by bytes (so two offsets into
/// one datagram compare on content), `Bool`/`Bool` by value, anything else unequal. The catch-all
/// is what keeps both forms of `#urgent,urgent:1`. Not `Value`'s `PartialEq`, because the contract
/// is the agent's exact-token rule, not general value equality.
fn tag_element_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Str(a), Value::Str(b)) => a == b,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        _ => false,
    }
}

/// Stamps `statsd.container_id`, a protocol-namespaced carrier (`docs/adr/lossless-transit.md`),
/// as a zero-copy datagram slice, for a metric line's `|c:` and an event's or service check's `c:`.
fn insert_container_id(attributes: &mut AttrMap, bytes: &Bytes, container_id: &str) {
    attributes
        .insert_sym(KEYS.container_id, Value::Str(subslice::share(bytes, container_id.as_bytes())));
}

/// Stamps `statsd.external_data` (`|e:`) or `statsd.cardinality` (`|card:`) when `field` is one
/// of them, returning whether it was. The metric, event, and service-check parsers share it: both
/// are protocol-namespaced carriers (`docs/adr/lossless-transit.md`), zero-copy datagram slices
/// kept verbatim, last segment wins. `card:` values aren't checked against the Agent's
/// `none|low|orchestrator|high`: a relay carries what the client sent.
fn insert_origin_field(attributes: &mut AttrMap, bytes: &Bytes, field: &str) -> bool {
    if let Some(external_data) = field.strip_prefix("e:") {
        attributes.insert_sym(
            KEYS.external_data,
            Value::Str(subslice::share(bytes, external_data.as_bytes())),
        );
        true
    } else if let Some(cardinality) = field.strip_prefix("card:") {
        attributes.insert_sym(
            KEYS.cardinality,
            Value::Str(subslice::share(bytes, cardinality.as_bytes())),
        );
        true
    } else {
        false
    }
}

/// Parses a `|T<unix-seconds>`/`d:<unix-seconds>` value into `(nanos, secs)`: `nanos` for
/// `Event::timestamp`, `secs` for the `statsd.timestamp` carrier.
///
/// Anything `u64::from_str` refuses, or seconds whose nanosecond conversion overflows `i64`,
/// rejects the line rather than falling back to receipt time. `from_str` takes a leading `+`
/// (`super`'s module doc has why that stays).
fn parse_wire_seconds(secs: &str, line: &str) -> Result<(i64, u64), CodecError> {
    let malformed = || CodecError::Malformed(format!("malformed statsd line: {line:?}"));
    let secs: u64 = secs.parse().map_err(|_| malformed())?;
    let nanos = secs
        .checked_mul(1_000_000_000)
        .and_then(|n| i64::try_from(n).ok())
        .ok_or_else(malformed)?;
    Ok((nanos, secs))
}

/// Parses one line. `bytes` is the whole datagram, threaded down so every field shares it;
/// `line` borrows from it.
fn parse_line(
    bytes: &Bytes,
    line: &str,
    timestamp: i64,
    keys: &mut KeyCache,
) -> Result<Vec<Event>, CodecError> {
    // Only these two sigils are special; a legal `_`-prefixed metric name falls through.
    if line.starts_with("_e{") {
        return parse_event(bytes, line, timestamp, keys).map(|event| vec![event]);
    }
    if line.starts_with("_sc|") {
        return parse_service_check(bytes, line, timestamp, keys).map(|event| vec![event]);
    }

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
    // Receipt time unless `|T<secs>` overrides it.
    let mut line_timestamp = timestamp;
    for extra in segments {
        if let Some(rate) = extra.strip_prefix('@') {
            let parsed: f64 = rate.parse().map_err(|_| malformed())?;
            // A probability: finite and in (0, 1]. `f64::parse` accepts NaN/inf/zero/negative
            // text, which would become a non-finite or negative counter (or divide by zero).
            if !parsed.is_finite() || parsed <= 0.0 || parsed > 1.0 {
                return Err(malformed());
            }
            sample_rate = parsed;
        } else if let Some(tags) = extra.strip_prefix('#') {
            insert_tags(&mut attributes, bytes, tags, keys);
        } else if let Some(container_id) = extra.strip_prefix("c:") {
            // Carried verbatim, `ci-`/`in-` prefixes included, on every metric type.
            insert_container_id(&mut attributes, bytes, container_id);
        } else if insert_origin_field(&mut attributes, bytes, extra) {
            // `|e:`/`|card:`, stamped by the call itself.
        } else if let Some(secs) = extra.strip_prefix('T') {
            // DogStatsD v1.3+ point timestamp, accepted on every type.
            let (nanos, secs) = parse_wire_seconds(secs, line)?;
            line_timestamp = nanos;
            // `statsd_out` emits `|T` from this carrier, not `event.timestamp`, which a stage
            // like `aggregate` rebuilds; so a `|T` the wire never sent can't be fabricated.
            attributes.insert_sym(KEYS.timestamp, Value::U64(secs));
        }
        // Any other segment is ignored, for forward compatibility.
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
            // One raw `Samples` per line, unsketched, `sample_rate` verbatim (`super`'s module
            // doc). Pushed straight into its inline `SmallVec` (`SAMPLES_INLINE`, 19 values) so a
            // line up to that size doesn't allocate an intermediate `Vec`.
            let mut samples = Samples::default();
            for raw_value in values_part.split(':') {
                samples.values.push(parse_finite_value(raw_value, "timing/histogram", line)?);
            }
            samples.sample_rate = sample_rate;
            let mut attrs = attributes.clone();
            // Stamped after the tags, so it overwrites a `#statsd.type` tag.
            attrs.insert_sym(KEYS.type_, Value::Str(subslice::share(bytes, type_part.as_bytes())));
            let kind = MetricKind::Samples(samples);
            Ok(vec![Event::metric(line_timestamp, attrs, MetricRecord::new(intern(name), kind))])
        }
        "s" => {
            // `sample_rate` is ignored: a set member is not a count to extrapolate.
            let members: Vec<Bytes> = values_part
                .split(':')
                .map(|raw_value| subslice::share(bytes, raw_value.as_bytes()))
                .collect();
            Ok(vec![Event::metric(
                line_timestamp,
                attributes.clone(),
                MetricRecord::new(intern(name), MetricKind::SetMembers(members)),
            )])
        }
        other => Err(CodecError::Malformed(format!("unknown metric type '{other}': {line:?}"))),
    }
}

/// Parses a DogStatsD event line starting `_e{` (`super`'s module doc, "DogStatsD events and
/// service checks").
fn parse_event(
    bytes: &Bytes,
    line: &str,
    timestamp: i64,
    keys: &mut KeyCache,
) -> Result<Event, CodecError> {
    let malformed = || CodecError::Malformed(format!("malformed dogstatsd event: {line:?}"));

    let header_rest = line.strip_prefix("_e{").ok_or_else(malformed)?;
    let (header, after_header) = header_rest.split_once('}').ok_or_else(malformed)?;
    let (title_len, text_len) = header.split_once(',').ok_or_else(malformed)?;
    let title_len: usize = title_len.parse().map_err(|_| malformed())?;
    let text_len: usize = text_len.parse().map_err(|_| malformed())?;
    let after_header = after_header.strip_prefix(':').ok_or_else(malformed)?;

    // `str::get` is `None` both past the end and off a char boundary, so a bad length rejects the
    // line; the later indexing reuses the lengths `get` already validated and cannot panic.
    let title = after_header.get(..title_len).ok_or_else(malformed)?;
    let after_title = after_header[title_len..].strip_prefix('|').ok_or_else(malformed)?;
    let raw_text = after_title.get(..text_len).ok_or_else(malformed)?;
    let after_text = &after_title[text_len..];

    let mut attributes = AttrMap::new();
    attributes.insert_sym(KEYS.event_title, Value::Str(subslice::share(bytes, title.as_bytes())));

    let mut line_timestamp = timestamp;
    let mut severity = None;

    if !after_text.is_empty() {
        let fields = after_text.strip_prefix('|').ok_or_else(malformed)?;
        for field in fields.split('|') {
            if let Some(tags) = field.strip_prefix('#') {
                insert_tags(&mut attributes, bytes, tags, keys);
            } else if let Some(container_id) = field.strip_prefix("c:") {
                insert_container_id(&mut attributes, bytes, container_id);
            } else if insert_origin_field(&mut attributes, bytes, field) {
                // `e:`/`card:`, stamped by the call itself.
            } else if let Some(secs) = field.strip_prefix("d:") {
                let (nanos, secs) = parse_wire_seconds(secs, line)?;
                line_timestamp = nanos;
                attributes.insert_sym(KEYS.timestamp, Value::U64(secs));
            } else if let Some(host) = field.strip_prefix("h:") {
                attributes.insert_sym(
                    KEYS.event_host,
                    Value::Str(subslice::share(bytes, host.as_bytes())),
                );
            } else if let Some(priority) = field.strip_prefix("p:") {
                if priority != "normal" && priority != "low" {
                    return Err(malformed());
                }
                attributes.insert_sym(
                    KEYS.event_priority,
                    Value::Str(subslice::share(bytes, priority.as_bytes())),
                );
            } else if let Some(alert_type) = field.strip_prefix("t:") {
                severity = Some(match alert_type {
                    "error" => Severity::Error,
                    "warning" => Severity::Warn,
                    "success" | "info" => Severity::Info,
                    _ => return Err(malformed()),
                });
                attributes.insert(
                    "statsd.event.alert_type",
                    Value::Str(subslice::share(bytes, alert_type.as_bytes())),
                );
            } else if let Some(key) = field.strip_prefix("k:") {
                attributes.insert_sym(
                    KEYS.event_aggregation_key,
                    Value::Str(subslice::share(bytes, key.as_bytes())),
                );
            } else if let Some(source) = field.strip_prefix("s:") {
                attributes.insert_sym(
                    KEYS.event_source_type,
                    Value::Str(subslice::share(bytes, source.as_bytes())),
                );
            }
            // Any other field, `|T` included, is ignored.
        }
    }

    Ok(Event::log(
        line_timestamp,
        attributes,
        LogRecord {
            message: unescape_event_text(bytes, raw_text),
            severity,
            body_format: BodyFormat::Raw,
            trace: None,
            // Not the title: interning free text would grow the global interner without bound.
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    ))
}

/// A DogStatsD event's `TEXT` with its `\n` (backslash, `n`) escape turned into a newline.
///
/// Zero-copy when there is nothing to unescape. Otherwise one allocation: each escape shrinks by
/// one byte, so the buffer is sized to fit and `Bytes::from(Vec)` takes its no-copy
/// `len == capacity` path, where `String::replace`'s slack would cost a second allocation.
fn unescape_event_text(bytes: &Bytes, raw: &str) -> Value {
    let escapes = raw.matches("\\n").count();
    if escapes == 0 {
        return Value::Str(subslice::share(bytes, raw.as_bytes()));
    }
    let mut out = Vec::with_capacity(raw.len() - escapes);
    let mut rest = raw;
    while let Some(at) = rest.find("\\n") {
        out.extend_from_slice(&rest.as_bytes()[..at]);
        out.push(b'\n');
        rest = &rest[at + 2..];
    }
    out.extend_from_slice(rest.as_bytes());
    debug_assert_eq!(out.len(), out.capacity());
    Value::Str(Bytes::from(out))
}

/// Parses a DogStatsD service check line starting `_sc|` (`super`'s module doc, "DogStatsD
/// events and service checks").
fn parse_service_check(
    bytes: &Bytes,
    line: &str,
    timestamp: i64,
    keys: &mut KeyCache,
) -> Result<Event, CodecError> {
    let malformed =
        || CodecError::Malformed(format!("malformed dogstatsd service check: {line:?}"));

    let rest = line.strip_prefix("_sc|").ok_or_else(malformed)?;
    // NAME, STATUS, and the optional fields, each up to the next `|`.
    let mut parts = rest.splitn(3, '|');
    let name = parts.next().ok_or_else(malformed)?;
    if name.is_empty() {
        return Err(malformed());
    }
    let status: u8 = parts.next().ok_or_else(malformed)?.parse().map_err(|_| malformed())?;
    if status > 3 {
        return Err(malformed());
    }

    let mut attributes = AttrMap::new();
    // Always stamped: `MetricRecord` has nowhere else to carry the raw name.
    attributes
        .insert_sym(KEYS.service_check_name, Value::Str(subslice::share(bytes, name.as_bytes())));
    attributes.insert_sym(KEYS.service_check_status, Value::U64(status as u64));

    let mut line_timestamp = timestamp;

    if let Some(fields) = parts.next() {
        for field in fields.split('|') {
            if let Some(message) = field.strip_prefix("m:") {
                attributes.insert(
                    "statsd.service_check.message",
                    Value::Str(subslice::share(bytes, message.as_bytes())),
                );
            } else if let Some(tags) = field.strip_prefix('#') {
                insert_tags(&mut attributes, bytes, tags, keys);
            } else if let Some(container_id) = field.strip_prefix("c:") {
                insert_container_id(&mut attributes, bytes, container_id);
            } else if insert_origin_field(&mut attributes, bytes, field) {
                // `e:`/`card:`, stamped by the call itself.
            } else if let Some(secs) = field.strip_prefix("d:") {
                let (nanos, secs) = parse_wire_seconds(secs, line)?;
                line_timestamp = nanos;
                attributes.insert_sym(KEYS.timestamp, Value::U64(secs));
            } else if let Some(host) = field.strip_prefix("h:") {
                attributes.insert_sym(
                    KEYS.service_check_host,
                    Value::Str(subslice::share(bytes, host.as_bytes())),
                );
            }
            // Any other field, `|T` included, is ignored.
        }
    }

    Ok(Event::metric(
        line_timestamp,
        attributes,
        MetricRecord::new(intern(name), MetricKind::Gauge(status as f64)),
    ))
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
            let value = parse_finite_value(raw_value, "counter", line)? / sample_rate;
            // A rate below 1 multiplies, so a finite value near `f64::MAX` can extrapolate to
            // infinity, which this decoder rejects as it does a literal `inf` counter.
            if !value.is_finite() {
                return Err(CodecError::Malformed(format!(
                    "counter value overflows when extrapolated by its sample rate: {line:?}"
                )));
            }
            MetricKind::counter(value)
        }
        "g" => {
            // A leading '+' or '-' is a relative adjustment: the spec has no syntax for a negative
            // absolute gauge, so '-' is as unambiguous as '+' and there is no config toggle
            // (`docs/adr/relative-gauge-adjustments.md`'s Alternatives). `f64::from_str` accepts
            // both signs (`plus_prefixed_gauge_values_parse_via_from_str`), so only the
            // `Gauge`/`GaugeDelta` choice is made here; `aggregate` resolves a delta, and a sink
            // reached without one reports it.
            //
            // `sample_rate` is ignored: a gauge value is not a count of occurrences.
            let value = parse_finite_value(raw_value, "gauge", line)?;
            if raw_value.starts_with('+') || raw_value.starts_with('-') {
                MetricKind::GaugeDelta(value)
            } else {
                MetricKind::Gauge(value)
            }
        }
        // `parse_line` handles `ms`/`h`/`d`/`s` itself, one event per line.
        other => unreachable!("build_event only handles c/g, got {other:?}"),
    };

    // Runs once per value on a multi-value line. Cloning scalar tags is a `SmallVec` memcpy plus a
    // refcount bump each; a repeated-key tag's `Value::Array` costs one `Vec` spine allocation per
    // event, its elements still refcounted datagram slices.
    Ok(Event::metric(timestamp, attributes.clone(), MetricRecord::new(intern(name), kind)))
}

/// Parses a metric value, rejecting it unless finite. `f64::parse` accepts "NaN"/"inf"/"-inf",
/// which would become a non-finite `Sum` or `Gauge`, or a `Samples` value that corrupts the
/// `DdSketch` `aggregate` later builds from it. `what` names the value in the error.
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
    use crate::framing::{Framer, FramingMode, MAX_FRAME_BYTES};

    fn decode(line: &str) -> Vec<Event> {
        let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()));
        decoder.decode(Bytes::from(line.to_string())).expect("decode should succeed").events
    }

    /// Events carry the caller's `received_at`, not decode time, which can lag arrival under
    /// backlog (`docs/adr/decoupled-listener-io.md`).
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

    /// `decode_into` appends to `out`, so `logit_pipeline::BatchAccumulator` can reuse one buffer.
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
        // Folding a multi-value line into one multi-metric event must fail here, not quietly
        // change what every caller of this helper asserts.
        assert!(event.log.is_none() && event.span.is_none(), "statsd emits metric-only events");
        event.metrics.pop().unwrap()
    }

    /// A line's rejection, straight from `parse_line`: `decode()` isolates per-line errors and
    /// never surfaces one.
    fn parse_err(line: &str) -> CodecError {
        let bytes = Bytes::from(line.to_string());
        let text = std::str::from_utf8(&bytes).unwrap();
        parse_line(&bytes, text, 0, &mut KeyCache::new())
            .expect_err("expected this line to be rejected")
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
        // Zero divides by zero; negative and >1 aren't probabilities; NaN/inf parse but aren't
        // finite.
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
        // `f64::parse` accepts "NaN"/"inf"/"-inf".
        for value in ["NaN", "inf", "-inf"] {
            let line = format!("hits:{value}|c");
            assert!(
                matches!(parse_err(&line), CodecError::Malformed(_)),
                "expected {value} to be rejected"
            );
        }
    }

    /// A finite value divided by a small rate can pass `f64::MAX`: `1e308 / 0.1` is `inf`.
    #[test]
    fn a_counter_whose_extrapolation_overflows_is_rejected() {
        for line in ["hits:1e308|c|@0.1", "hits:-1e308|c|@0.5", "hits:1:1e308|c|@0.01"] {
            let err = parse_err(line);
            assert!(
                matches!(&err, CodecError::Malformed(msg) if msg.starts_with("counter value overflows")),
                "{line}: {err:?}"
            );
        }
        let metric = only_metric(decode("hits:1e307|c|@0.5"));
        assert!(
            matches!(metric.kind, MetricKind::Sum(logit_core::Sum { value, .. }) if value == 2e307)
        );
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
        // A NaN sample would corrupt the `DdSketch` `aggregate` builds, not only one point.
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

    /// `f64::from_str` accepts a leading `+` as it does `-`, which `build_event`'s `"g"` arm relies
    /// on so that only its `starts_with` check decides `Gauge` vs. `GaugeDelta`.
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

    /// Sign detection must not turn an unsigned gauge into a delta.
    #[test]
    fn an_unsigned_gauge_value_still_decodes_as_an_absolute_gauge() {
        let metric = only_metric(decode("cpu.load:5|g"));
        assert!(matches!(metric.kind, MetricKind::Gauge(v) if v == 5.0));
    }

    /// `+0` is a legal no-op delta, distinct from an unsigned `0` (`Gauge(0.0)`).
    #[test]
    fn a_leading_plus_zero_is_a_legal_no_op_delta_not_an_error() {
        let metric = only_metric(decode("conns:+0|g"));
        assert!(matches!(metric.kind, MetricKind::GaugeDelta(v) if v == 0.0));
    }

    /// A sign never bypasses the finiteness check.
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

    /// Mixed signs on one multi-value gauge line decode as independent deltas.
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

    /// `ms` decodes to a raw [`MetricKind::Samples`], never a sketch.
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

    /// A sample rate rides verbatim on `Samples`; extrapolating is `aggregate`'s job.
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

    /// The same at `@0.1`.
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

    /// An explicit `@1` decodes to one raw value at rate `1.0`; `statsd_decode_one_line` in
    /// `crates/logit-bench/tests/allocations.rs` pins the same at the allocation level.
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

    /// A `ms`/`h`/`d` line's values share one `Samples`, in wire order, on one event.
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

    /// Each of `ms`/`h`/`d` keeps its type letter as `statsd.type`.
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

    // Weight clamping to `Samples::MAX_WEIGHT` and sketch accuracy are tested where sketching
    // happens: `crates/logit-transforms/src/aggregate.rs`'s
    // `samples_sketch_mode_merges_weighted_values_and_counts_weight_clamp`, and
    // `logit_core::metric`'s `Samples::sketch` tests.

    #[test]
    fn dogstatsd_tags_become_attributes() {
        let events = decode("page.views:1|c|#env:prod,host:web1,urgent");
        let event = &events[0];
        assert_eq!(event.attributes.get("env").and_then(|v| v.as_str()), Some("prod"));
        assert_eq!(event.attributes.get("host").and_then(|v| v.as_str()), Some("web1"));
        assert!(matches!(event.attributes.get("urgent"), Some(logit_core::Value::Bool(true))));
    }

    /// A repeat line with the same tag names in another order (one repeated, so the merge runs)
    /// interns nothing new, and the `KeyCache` holds only the three tag names. `nextest` runs
    /// each test in its own process, so `interner::len()` reflects only this test.
    #[test]
    fn repeat_tag_keys_are_cache_hits() {
        let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()));
        let line = |s: &str| Bytes::from(s.to_string());
        // `|c:` here too: the first touch of `KEYS` interns every carrier key at once.
        drop(
            decoder.decode(line("tc.views:1|c|#tc_env:prod,tc_host:web1,tc_urgent|c:abc")).unwrap(),
        );
        assert_eq!(decoder.keys.len(), 3);

        let before = logit_core::interner::len();
        let events = decoder
            .decode(line("tc.views:2|c|#tc_host:web2,tc_urgent,tc_env:dev,tc_env:qa|c:abc"))
            .expect("decode should succeed")
            .events;
        assert_eq!(logit_core::interner::len(), before, "same tag keys, same carrier keys");
        assert_eq!(decoder.keys.len(), 3);

        let event = &events[0];
        assert_eq!(event.attributes.get("tc_host").and_then(|v| v.as_str()), Some("web2"));
        assert_eq!(
            event.attributes.get("tc_env"),
            Some(&Value::Array(vec![Value::str("dev"), Value::str("qa")]))
        );
        assert!(matches!(event.attributes.get("tc_urgent"), Some(Value::Bool(true))));
        assert_eq!(
            event.attributes.get("statsd.container_id").and_then(|v| v.as_str()),
            Some("abc")
        );
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
        // The structural pin for the zero-copy tag value, like `syslog_in`'s
        // `emitted_message_is_a_zero_copy_slice_of_the_datagram`. The repeated `team` key checks
        // that each `Value::Array` element is a datagram slice too, not a copy.
        let datagram = Bytes::from("page.views:1|c|#env:prod,team:a,team:b".to_string());
        let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()));
        let event = only_metric_event(decoder.decode(datagram.clone()).unwrap().events);

        let tag = event.attributes.get("env").expect("env tag");
        let Value::Str(tag) = tag else { panic!("expected Value::Str, got {tag:?}") };
        assert_shares_datagram_allocation(&datagram, tag, "a scalar tag value");

        let team = event.attributes.get("team").expect("team tag");
        let Value::Array(elements) = team else { panic!("expected Value::Array, got {team:?}") };
        assert_eq!(elements.len(), 2, "both wire values should survive the fold");
        for element in elements {
            let Value::Str(element) = element else {
                panic!("expected Value::Str element, got {element:?}")
            };
            assert_shares_datagram_allocation(&datagram, element, "an array tag element");
        }
    }

    /// Asserts `slice` points inside `datagram`'s allocation rather than at a copy.
    fn assert_shares_datagram_allocation(datagram: &Bytes, slice: &Bytes, what: &str) {
        assert!(
            logit_core::subslice::within(datagram, slice),
            "{what} should be a slice of the original datagram, not a copy"
        );
    }

    /// A repeated tag key folds into a `Value::Array` in wire order; the last token doesn't win.
    #[test]
    fn a_repeated_tag_key_folds_into_an_array_in_wire_order() {
        let events = decode("page.views:1|c|#team:a,team:b");
        assert_eq!(
            events[0].attributes.get("team"),
            Some(&Value::Array(vec![Value::str("a"), Value::str("b")]))
        );
    }

    #[test]
    fn three_occurrences_of_a_tag_key_fold_into_three_array_elements() {
        let events = decode("page.views:1|c|#team:a,team:b,team:c");
        assert_eq!(
            events[0].attributes.get("team"),
            Some(&Value::Array(vec![Value::str("a"), Value::str("b"), Value::str("c")]))
        );
    }

    /// An exact duplicate token is deduped, so no one-element `Array` is produced.
    #[test]
    fn an_exact_duplicate_tag_is_deduped_instead_of_becoming_an_array() {
        let events = decode("page.views:1|c|#team:a,team:a");
        assert_eq!(events[0].attributes.get("team"), Some(&Value::str("a")));
    }

    #[test]
    fn an_exact_duplicate_bare_tag_is_deduped_instead_of_becoming_an_array() {
        let events = decode("page.views:1|c|#urgent,urgent");
        assert_eq!(events[0].attributes.get("urgent"), Some(&Value::Bool(true)));
    }

    /// A duplicate among distinct values drops only the duplicate -- `a,b,a` is two live tags.
    #[test]
    fn a_duplicate_among_distinct_tag_values_drops_only_the_duplicate() {
        let events = decode("page.views:1|c|#team:a,team:b,team:a");
        assert_eq!(
            events[0].attributes.get("team"),
            Some(&Value::Array(vec![Value::str("a"), Value::str("b")]))
        );
    }

    /// A bare token and a valued one sharing a key are not duplicates; both survive, in order.
    #[test]
    fn a_bare_and_a_valued_tag_sharing_a_key_keep_both_forms_in_wire_order() {
        let events = decode("page.views:1|c|#urgent,urgent:1");
        assert_eq!(
            events[0].attributes.get("urgent"),
            Some(&Value::Array(vec![Value::Bool(true), Value::str("1")]))
        );

        let events = decode("page.views:1|c|#urgent:1,urgent");
        assert_eq!(
            events[0].attributes.get("urgent"),
            Some(&Value::Array(vec![Value::str("1"), Value::Bool(true)]))
        );
    }

    /// An event line's `#` field folds a repeated key as a metric line's does.
    #[test]
    fn a_repeated_tag_key_on_an_event_line_folds_into_an_array() {
        let event = only_log_event(decode("_e{5,4}:title|text|#k:a,k:b"));
        assert_eq!(
            event.attributes.get("k"),
            Some(&Value::Array(vec![Value::str("a"), Value::str("b")]))
        );
    }

    /// So does a service check's.
    #[test]
    fn a_repeated_tag_key_on_a_service_check_line_folds_into_an_array() {
        let events = decode("_sc|check|0|#k:a,k:b");
        assert_eq!(
            events[0].attributes.get("k"),
            Some(&Value::Array(vec![Value::str("a"), Value::str("b")]))
        );
    }

    /// A tag literally named `statsd.type` folds like any other repeated key. A `c` line, because
    /// on `ms`/`h`/`d` the decoder's own `statsd.type` stamp overwrites it.
    #[test]
    fn a_tag_literally_named_statsd_type_folds_into_an_array_like_any_other() {
        let events = decode("page.views:1|c|#statsd.type:ms,statsd.type:h");
        assert_eq!(
            events[0].attributes.get("statsd.type"),
            Some(&Value::Array(vec![Value::str("ms"), Value::str("h")]))
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

    /// `s` decodes to raw `SetMembers`: one member, a zero-copy slice of the datagram.
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

    /// A `s` line's values share one `SetMembers`, in wire order, on one event.
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

    /// `|c:<id>` applies to every metric type, not only the spec's `c`/`g`.
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

        // A malformed |T rejects only its own line.
        let events = decode("a:1|c|Tbad\nb:2|c");
        assert_eq!(events.len(), 1, "only the malformed-T line should be dropped");
        assert_eq!(intern("b"), only_metric(events).name);
    }

    /// The integer fields take what `u64::from_str` takes, a leading `+` and leading zeros
    /// included (`super`'s module doc, under the rejection rules): each names the same number.
    #[test]
    fn integer_fields_accept_a_leading_plus_and_leading_zeros() {
        for line in ["hits:1|c|T+1700000000", "hits:1|c|T01700000000"] {
            let events = decode(line);
            assert_eq!(events.len(), 1, "{line}");
            assert_eq!(
                events[0].attributes.get("statsd.timestamp"),
                Some(&Value::U64(1_700_000_000)),
                "{line}"
            );
        }
        let event = &decode("_e{+2,+4}:hi|body|d:+5")[0];
        assert_eq!(event.attributes.get("statsd.timestamp"), Some(&Value::U64(5)));
        assert_eq!(event.log.as_ref().unwrap().message.as_str(), Some("body"));
        let check = &decode("_sc|check|+1")[0];
        assert_eq!(check.attributes.get("statsd.service_check.status"), Some(&Value::U64(1)));
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

    /// Asserts `events` is a single log-only `Event` and returns it whole, attributes and timestamp
    /// included.
    fn only_log_event(events: Vec<Event>) -> Event {
        assert_eq!(events.len(), 1, "expected exactly one event");
        let event = events.into_iter().next().unwrap();
        assert!(
            event.metrics.is_empty() && event.span.is_none(),
            "expected a log-only event, got {event:?}"
        );
        assert!(event.log.is_some(), "expected a log body");
        event
    }

    /// The DogStatsD docs' own canonical event example.
    #[test]
    fn dogstatsd_docs_example_event_decodes() {
        let events = decode(
            "_e{21,36}:An exception occurred|Cannot parse CSV file from 10.0.0.17|t:warning|#err_type:bad_file",
        );
        let event = only_log_event(events);
        let log = event.log.as_ref().unwrap();
        assert_eq!(log.message.as_str(), Some("Cannot parse CSV file from 10.0.0.17"));
        assert_eq!(log.severity, Some(Severity::Warn));
        assert_eq!(log.body_format, BodyFormat::Raw);
        assert_eq!(log.event_name, None);
        assert_eq!(
            event.attributes.get("statsd.event.title").and_then(|v| v.as_str()),
            Some("An exception occurred")
        );
        assert_eq!(
            event.attributes.get("statsd.event.alert_type").and_then(|v| v.as_str()),
            Some("warning")
        );
        assert_eq!(event.attributes.get("err_type").and_then(|v| v.as_str()), Some("bad_file"));
    }

    /// The DogStatsD docs' own canonical service check example.
    #[test]
    fn dogstatsd_docs_example_service_check_decodes() {
        let events =
            decode("_sc|Redis connection|2|#env:dev|m:Redis connection timed out after 10s");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert!(event.log.is_none() && event.span.is_none());
        assert_eq!(event.metrics.len(), 1);
        assert_eq!(intern("Redis connection"), event.metrics[0].name);
        assert!(matches!(event.metrics[0].kind, MetricKind::Gauge(v) if v == 2.0));
        assert_eq!(
            event.attributes.get("statsd.service_check.name").and_then(|v| v.as_str()),
            Some("Redis connection")
        );
        assert_eq!(event.attributes.get("statsd.service_check.status"), Some(&Value::U64(2)));
        assert_eq!(
            event.attributes.get("statsd.service_check.message").and_then(|v| v.as_str()),
            Some("Redis connection timed out after 10s")
        );
        assert_eq!(event.attributes.get("env").and_then(|v| v.as_str()), Some("dev"));
    }

    #[test]
    fn event_with_every_optional_field_decodes() {
        let events =
            decode("_e{5,4}:title|text|d:1700000000|h:host1|p:low|t:success|k:agg1|s:src1|#env:prod|c:cid1");
        let event = only_log_event(events);
        assert_eq!(event.timestamp, 1_700_000_000 * 1_000_000_000);
        let log = event.log.as_ref().unwrap();
        assert_eq!(log.message.as_str(), Some("text"));
        // `t:success` maps to `Severity::Info`, same as `t:info`.
        assert_eq!(log.severity, Some(Severity::Info));
        assert_eq!(
            event.attributes.get("statsd.event.title").and_then(|v| v.as_str()),
            Some("title")
        );
        assert_eq!(
            event.attributes.get("statsd.event.priority").and_then(|v| v.as_str()),
            Some("low")
        );
        assert_eq!(
            event.attributes.get("statsd.event.alert_type").and_then(|v| v.as_str()),
            Some("success")
        );
        assert_eq!(
            event.attributes.get("statsd.event.aggregation_key").and_then(|v| v.as_str()),
            Some("agg1")
        );
        assert_eq!(
            event.attributes.get("statsd.event.source_type").and_then(|v| v.as_str()),
            Some("src1")
        );
        assert_eq!(
            event.attributes.get("statsd.event.host").and_then(|v| v.as_str()),
            Some("host1")
        );
        assert_eq!(event.attributes.get("env").and_then(|v| v.as_str()), Some("prod"));
        assert_eq!(
            event.attributes.get("statsd.container_id").and_then(|v| v.as_str()),
            Some("cid1")
        );
        assert_eq!(event.attributes.get("statsd.timestamp"), Some(&Value::U64(1_700_000_000)));
    }

    /// TEXT may contain `|` and `:`, and its `\n` escape becomes a newline; the title's doesn't.
    #[test]
    fn event_text_containing_pipe_colon_and_an_escaped_newline_decodes() {
        // Wire bytes: `a|b:c\nd` where `\n` is the two-byte escape sequence -- 8 bytes total.
        let events = decode("_e{1,8}:T|a|b:c\\nd");
        let event = only_log_event(events);
        let message = event.log.as_ref().unwrap().message.as_str().expect("message should be str");
        assert_eq!(message, "a|b:c\nd", "the escape sequence should become a real newline");
        assert!(message.contains('\n'), "expected a real newline byte in the decoded message");
    }

    /// Trailing whitespace (a space, a tab) on an `_e{` line is kept as `TEXT`.
    #[test]
    fn event_text_ending_in_whitespace_is_kept() {
        let events = decode("_e{1,2}:a|b ");
        let event = only_log_event(events);
        let message = event.log.as_ref().unwrap().message.as_str().expect("message should be str");
        assert_eq!(message, "b ", "the trailing space is real TEXT, not packet padding");

        let events = decode("_e{1,2}:a|b\t");
        let event = only_log_event(events);
        let message = event.log.as_ref().unwrap().message.as_str().expect("message should be str");
        assert_eq!(message, "b\t", "a trailing tab is kept the same way");
    }

    #[test]
    fn event_title_length_running_past_the_line_is_rejected() {
        assert!(matches!(parse_err("_e{100,4}:title|text"), CodecError::Malformed(_)));
    }

    #[test]
    fn event_missing_pipe_after_title_is_rejected() {
        // TITLE_LEN=5 correctly covers "title", but nothing separates it from "text".
        assert!(matches!(parse_err("_e{5,4}:titletext"), CodecError::Malformed(_)));
    }

    #[test]
    fn event_title_length_landing_mid_char_boundary_is_rejected() {
        // 'é' is 2 UTF-8 bytes; TITLE_LEN=1 lands inside it, not on a char boundary.
        assert!(matches!(parse_err("_e{1,4}:\u{e9}|text"), CodecError::Malformed(_)));
    }

    #[test]
    fn event_unknown_priority_or_alert_type_is_rejected() {
        assert!(matches!(parse_err("_e{5,4}:title|text|p:bogus"), CodecError::Malformed(_)));
        assert!(matches!(parse_err("_e{5,4}:title|text|t:bogus"), CodecError::Malformed(_)));
    }

    #[test]
    fn event_d_field_sets_the_timestamp_and_the_carrier() {
        let events = decode("_e{5,4}:title|text|d:1700000000");
        let event = only_log_event(events);
        assert_eq!(event.timestamp, 1_700_000_000 * 1_000_000_000);
        assert_eq!(event.attributes.get("statsd.timestamp"), Some(&Value::U64(1_700_000_000)));
    }

    #[test]
    fn event_container_id_becomes_an_attribute() {
        let events = decode("_e{5,4}:title|text|c:cid1");
        let event = only_log_event(events);
        assert_eq!(
            event.attributes.get("statsd.container_id").and_then(|v| v.as_str()),
            Some("cid1")
        );
    }

    #[test]
    fn service_check_d_field_sets_the_timestamp_and_the_carrier() {
        let events = decode("_sc|check|0|d:1700000000");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.timestamp, 1_700_000_000 * 1_000_000_000);
        assert_eq!(event.attributes.get("statsd.timestamp"), Some(&Value::U64(1_700_000_000)));
    }

    #[test]
    fn service_check_container_id_becomes_an_attribute() {
        let events = decode("_sc|check|0|c:cid2");
        assert_eq!(
            events[0].attributes.get("statsd.container_id").and_then(|v| v.as_str()),
            Some("cid2")
        );
    }

    fn attr_str<'a>(event: &'a Event, key: &str) -> Option<&'a str> {
        event.attributes.get(key).and_then(|v| v.as_str())
    }

    /// `|e:`/`|card:` on a metric line, alongside `|c:` and `|T`, on every metric type.
    #[test]
    fn external_data_and_cardinality_become_attributes_on_a_metric_line() {
        for line in [
            "x:1|c|#env:prod|c:cid|e:it-false,cn-web,pu-abc|card:high|T1700000000",
            "x:1|g|card:high|e:it-false,cn-web,pu-abc",
            "x:1:2|ms|e:it-false,cn-web,pu-abc|card:high",
            "x:a:b|s|e:it-false,cn-web,pu-abc|card:high",
        ] {
            let events = decode(line);
            assert_eq!(events.len(), 1, "line: {line:?}");
            let event = &events[0];
            assert_eq!(
                attr_str(event, "statsd.external_data"),
                Some("it-false,cn-web,pu-abc"),
                "line: {line:?}"
            );
            assert_eq!(attr_str(event, "statsd.cardinality"), Some("high"), "line: {line:?}");
        }
    }

    /// `card:` carries what the client sent; the Agent's four values aren't enforced.
    #[test]
    fn an_unrecognized_cardinality_is_carried_verbatim() {
        let events = decode("x:1|c|card:bogus");
        assert_eq!(attr_str(&events[0], "statsd.cardinality"), Some("bogus"));
    }

    #[test]
    fn external_data_and_cardinality_become_attributes_on_an_event() {
        let event = only_log_event(decode("_e{5,4}:title|text|c:cid|e:ext1|card:low"));
        assert_eq!(attr_str(&event, "statsd.container_id"), Some("cid"));
        assert_eq!(attr_str(&event, "statsd.external_data"), Some("ext1"));
        assert_eq!(attr_str(&event, "statsd.cardinality"), Some("low"));
    }

    #[test]
    fn external_data_and_cardinality_become_attributes_on_a_service_check() {
        let events = decode("_sc|check|0|e:ext2|card:orchestrator|m:all good");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(attr_str(event, "statsd.external_data"), Some("ext2"));
        assert_eq!(attr_str(event, "statsd.cardinality"), Some("orchestrator"));
        assert_eq!(attr_str(event, "statsd.service_check.message"), Some("all good"));
    }

    #[test]
    fn absent_external_data_and_cardinality_leave_no_attribute() {
        for line in ["x:1|c|c:cid", "_e{5,4}:title|text", "_sc|check|0"] {
            let events = decode(line);
            let event = &events[0];
            assert!(event.attributes.get("statsd.external_data").is_none(), "line: {line:?}");
            assert!(event.attributes.get("statsd.cardinality").is_none(), "line: {line:?}");
        }
    }

    /// An empty `|e:`/`|card:` stamps an empty string, as an empty `|c:` does.
    #[test]
    fn an_empty_external_data_or_cardinality_is_an_empty_string_like_an_empty_container_id() {
        let events = decode("x:1|c|c:|e:|card:");
        let event = &events[0];
        assert_eq!(attr_str(event, "statsd.container_id"), Some(""));
        assert_eq!(attr_str(event, "statsd.external_data"), Some(""));
        assert_eq!(attr_str(event, "statsd.cardinality"), Some(""));
    }

    /// Both carriers are zero-copy slices of the datagram, like `|c:`.
    #[test]
    fn external_data_is_a_zero_copy_slice_of_the_datagram() {
        let datagram = Bytes::from_static(b"x:1|c|e:ext");
        let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()));
        let mut out = Vec::new();
        decoder.decode_into(datagram.clone(), 0, &mut out).unwrap();
        let Some(Value::Str(value)) = out[0].attributes.get("statsd.external_data") else {
            panic!("expected a Str");
        };
        assert!(logit_core::subslice::within(&datagram, value), "expected a slice of the datagram");
    }

    /// `m:` ends at the next `|`, as every other field does and as the Agent reads it: a real
    /// client writes `c:`/`card:` after `m:` (`testdata/interop/datadog/dogstatsd-unix-008.raw`).
    #[test]
    fn service_check_message_ends_at_the_next_pipe() {
        let events = decode("_sc|check|0|m:slow upstream|c:in-7|card:low");
        let event = &events[0];
        assert_eq!(attr_str(event, "statsd.service_check.message"), Some("slow upstream"));
        assert_eq!(attr_str(event, "statsd.container_id"), Some("in-7"));
        assert_eq!(attr_str(event, "statsd.cardinality"), Some("low"));
        let events = decode("_sc|check|0|m:a|b|c");
        assert_eq!(attr_str(&events[0], "statsd.service_check.message"), Some("a"));
    }

    /// A trailing space on an `_sc|` line is kept as message content.
    #[test]
    fn service_check_message_trailing_whitespace_is_kept() {
        let events = decode("_sc|check|0|m:disk almost full ");
        assert_eq!(
            events[0].attributes.get("statsd.service_check.message").and_then(|v| v.as_str()),
            Some("disk almost full ")
        );
    }

    #[test]
    fn service_check_out_of_range_or_non_numeric_status_is_rejected() {
        for status in ["4", "-1", "abc"] {
            let line = format!("_sc|check|{status}");
            assert!(
                matches!(parse_err(&line), CodecError::Malformed(_)),
                "expected status {status:?} to be rejected"
            );
        }
    }

    #[test]
    fn service_check_empty_name_is_rejected() {
        assert!(matches!(parse_err("_sc||0"), CodecError::Malformed(_)));
    }

    /// Any `_`-prefixed line other than `_e{`/`_sc|` is an ordinary metric line.
    #[test]
    fn an_underscore_prefixed_metric_name_still_decodes_as_a_metric() {
        let metric = only_metric(decode("_total.count:1|c"));
        assert_eq!(intern("_total.count"), metric.name);
        assert!(
            matches!(metric.kind, MetricKind::Sum(logit_core::Sum { value, .. }) if value == 1.0)
        );
    }

    /// `_x|1` falls through to the metric grammar and is rejected there for having no `:`.
    #[test]
    fn an_underscore_prefixed_line_without_a_colon_is_rejected_for_missing_colon() {
        assert!(matches!(parse_err("_x|1"), CodecError::Malformed(_)));
    }

    /// A counter, an event, and a service check in one datagram decode in wire order.
    #[test]
    fn a_packed_datagram_mixing_a_counter_an_event_and_a_service_check_decodes_all_three_in_order()
    {
        let events = decode("hits:1|c\n_e{5,4}:title|text\n_sc|check|0");
        assert_eq!(events.len(), 3);
        assert!(events[0].metrics.len() == 1 && events[0].log.is_none(), "expected the counter");
        assert!(events[1].log.is_some(), "expected the event");
        assert!(
            events[2].metrics.len() == 1 && events[2].log.is_none(),
            "expected the service check"
        );
    }

    fn metric_name(event: &Event) -> &'static str {
        logit_core::interner::resolve(event.metrics[0].name)
    }

    // -------------------------------------------------------------------------------------------
    // Recorded interop fixtures (testdata/interop/statsd/, docs/plans/recorded-interop-fixtures.md)
    //
    // Real UDP datagrams from two real clients, Datadog's `datadog` package and the plain-statsd
    // `statsd` package, recorded by `script/record-fixtures statsd`. The tests above check this
    // repo's reading of the grammar; these check what real clients send, which is how a shared
    // misunderstanding surfaces.
    //
    // Asserted on decoded values, never bytes: a re-record changes the container id and flush
    // boundaries (that directory's README).
    // -------------------------------------------------------------------------------------------

    fn interop_dir() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/interop/statsd")
    }

    fn interop_fixture(name: &str) -> Bytes {
        let path = interop_dir().join(name);
        let raw = std::fs::read(&path)
            .unwrap_or_else(|e| panic!("reading interop fixture {}: {e}", path.display()));
        Bytes::from(raw)
    }

    /// Every captured datagram whose filename starts with `prefix`, in name order.
    fn interop_fixtures(prefix: &str) -> Vec<(String, Bytes)> {
        let dir = interop_dir();
        let mut names: Vec<String> = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("reading {}: {e}", dir.display()))
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with(prefix) && name.ends_with(".raw"))
            .collect();
        names.sort();
        assert!(!names.is_empty(), "no fixture matching `{prefix}*` under {}", dir.display());
        names
            .into_iter()
            .map(|name| {
                let bytes = interop_fixture(&name);
                (name, bytes)
            })
            .collect()
    }

    /// Decodes one captured datagram with drainable diagnostics, so a test can assert zero of
    /// them; a length check alone would pass a datagram whose every line was rejected.
    fn decode_interop(datagram: &Bytes) -> (Vec<Event>, Vec<String>) {
        let registry = logit_core::telemetry::Registry::new();
        let telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()))
            .with_diagnostics(Diagnostics::new("statsd_in").with_telemetry(telemetry));
        let mut events = Vec::new();
        decoder
            .decode_into(datagram.clone(), 0, &mut events)
            .expect("a captured datagram must decode as a whole");
        let keys = registry
            .drain(0)
            .into_iter()
            .filter_map(|event| match event.attributes.get("key") {
                Some(Value::Str(key)) => Some(String::from_utf8_lossy(key).into_owned()),
                _ => None,
            })
            .collect();
        (events, keys)
    }

    #[test]
    fn interop_fixture_every_captured_datagram_decodes_with_no_diagnostics() {
        let mut datagrams = 0usize;
        let mut events = 0usize;
        for (name, bytes) in interop_fixtures("statsd-") {
            let (decoded, diagnostics) = decode_interop(&bytes);
            assert!(diagnostics.is_empty(), "{name} raised {diagnostics:?}");
            assert!(!decoded.is_empty(), "{name} decoded to no events at all");
            datagrams += 1;
            events += decoded.len();
        }
        assert!(datagrams >= 50, "expected the whole capture, got {datagrams} datagrams");
        assert!(events >= 100, "expected a real workload, got {events} events");
    }

    #[test]
    fn interop_fixture_a_buffered_dogstatsd_datagram_carries_a_whole_packed_batch() {
        // The client packed many metrics into one datagram, cut on a line boundary.
        let bytes = interop_fixture("statsd-dogstatsd-buffered-000.raw");
        assert!(bytes.len() > 1_000, "the client packs close to its 1432-byte UDP ceiling");
        assert!(bytes.len() <= 1_432, "and never past it");
        let (events, diagnostics) = decode_interop(&bytes);
        assert!(diagnostics.is_empty(), "{diagnostics:?}");
        assert!(
            events.len() >= 8,
            "a packed DogStatsD datagram holds many metrics, got {}",
            events.len()
        );

        // Only these two shapes are asserted: which types land in one datagram depends on the
        // client's flush boundary.
        let mut sums = 0;
        let mut samples = 0;
        for event in &events {
            for metric in &event.metrics {
                match &metric.kind {
                    MetricKind::Sum(_) => sums += 1,
                    MetricKind::Samples(_) => samples += 1,
                    MetricKind::SetMembers(_) | MetricKind::Gauge(_) => {}
                    other => panic!("unexpected metric kind from a real client: {other:?}"),
                }
            }
        }
        assert!(sums > 0 && samples > 0, "got {sums} counters, {samples} timings");
    }

    #[test]
    fn interop_fixture_a_real_dogstatsd_line_carries_its_tags_and_container_id() {
        // The client detected its own container and sent `|c:<id>` unprompted, which a
        // hand-written fixture wouldn't have included.
        let (events, diagnostics) =
            decode_interop(&interop_fixture("statsd-dogstatsd-unbuffered-000.raw"));
        assert!(diagnostics.is_empty(), "{diagnostics:?}");
        let event = &events[0];
        assert_eq!(
            event.attributes.get("env").and_then(Value::as_str),
            Some("prod"),
            "the workload's own `env:prod` tag"
        );
        assert_eq!(event.attributes.get("service").and_then(Value::as_str), Some("checkout-api"));
        assert!(
            event.attributes.get("endpoint").is_some(),
            "a per-request tag the client hung off the line"
        );
        assert!(
            event.attributes.get("statsd.container_id").is_some(),
            "DogStatsd volunteers `|c:<id>` when it can see its own container"
        );
    }

    #[test]
    fn interop_fixture_plain_statsd_carries_its_cardinality_in_the_name_and_no_tags() {
        // Plain statsd: no tags, so what the tagged client put in tags is in the metric name.
        let (events, diagnostics) =
            decode_interop(&interop_fixture("statsd-plain-unbuffered-000.raw"));
        assert!(diagnostics.is_empty(), "{diagnostics:?}");
        assert_eq!(events.len(), 1, "one metric per datagram, unbuffered");
        let event = &events[0];
        let name = metric_name(event);
        assert!(name.starts_with("app.checkout_api."), "got {name:?}");
        assert!(
            name.len() > 40,
            "a tagless name carries the cardinality: {name:?} is {} bytes",
            name.len()
        );
        assert!(
            event.attributes.get("env").is_none()
                && event.attributes.get("statsd.container_id").is_none(),
            "the plain-statsd dialect has no tag or container-id syntax at all"
        );
    }

    // -------------------------------------------------------------------------------------------
    // Recorded DogStatsD over the Agent's Unix sockets (testdata/interop/datadog/,
    // `script/record-fixtures datadog-dogstatsd-unix`)
    //
    // The `datadog` Python client's nine constructs, each carrying `|c:`, `|e:` (from
    // `DD_EXTERNAL_ENV`), and `|card:`, over a Unix datagram socket (one file per datagram) and a
    // Unix stream socket (one file per connection, length prefixes and all).
    // -------------------------------------------------------------------------------------------

    /// The `DD_EXTERNAL_ENV` `script/record-fixtures` gives every DogStatsD client.
    const RECORDED_EXTERNAL_ENV: &str =
        "it-false,cn-record-fixtures,pu-00000000-0000-4000-8000-000000000001";

    fn datadog_interop_fixture(name: &str) -> Bytes {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../testdata/interop/datadog")
            .join(name);
        Bytes::from(
            std::fs::read(&path)
                .unwrap_or_else(|e| panic!("reading interop fixture {}: {e}", path.display())),
        )
    }

    /// The nine packets of one unbuffered run, checked construct by construct: the capture holds
    /// the client's calls in `python_dogstatsd_producer.py`'s order.
    fn assert_the_nine_recorded_constructs(packets: &[Bytes]) {
        assert_eq!(packets.len(), 9);
        let mut decoded = Vec::new();
        for (i, packet) in packets.iter().enumerate() {
            let (events, diagnostics) = decode_interop(packet);
            assert!(diagnostics.is_empty(), "packet {i} raised {diagnostics:?}");
            assert_eq!(events.len(), 1, "packet {i}: one construct per unbuffered packet");
            let event = events.into_iter().next().unwrap();
            assert!(attr_str(&event, "statsd.container_id").is_some_and(|c| c.starts_with("in-")));
            decoded.push(event);
        }
        let kinds: Vec<&str> =
            decoded.iter().take(7).map(|e| attr_str(e, "statsd.type").unwrap_or("-")).collect();
        assert_eq!(kinds, ["-", "-", "h", "d", "-", "ms", "-"], "the type carriers of the metrics");
        assert!(matches!(decoded[0].metrics[0].kind, MetricKind::Sum(_)));
        assert!(matches!(decoded[4].metrics[0].kind, MetricKind::SetMembers(_)));
        for event in &decoded[..7] {
            assert_eq!(attr_str(event, "statsd.external_data"), Some(RECORDED_EXTERNAL_ENV));
        }
        let cards: Vec<&str> =
            decoded.iter().map(|e| attr_str(e, "statsd.cardinality").unwrap()).collect();
        assert_eq!(
            cards,
            ["low", "high", "low", "low", "low", "low", "low", "orchestrator", "low"]
        );
        // `|T` last on the line, and its value the client's own.
        assert_eq!(decoded[6].attributes.get("statsd.timestamp"), Some(&Value::U64(1_790_000_000)));
        assert_eq!(decoded[6].timestamp, 1_790_000_000_000_000_000);
        // The event and the service check: no `e:` on either from this client.
        assert_eq!(attr_str(&decoded[7], "statsd.event.title"), Some("Deploy finished"));
        assert_eq!(attr_str(&decoded[7], "statsd.external_data"), None);
        let check = &decoded[8];
        assert_eq!(attr_str(check, "statsd.service_check.name"), Some("record.can_connect"));
        assert_eq!(attr_str(check, "statsd.service_check.message"), Some("slow upstream"));
        assert_eq!(attr_str(check, "statsd.external_data"), None);
    }

    #[test]
    fn interop_fixture_unix_datagrams_carry_every_origin_field() {
        let packets: Vec<Bytes> = (0..9)
            .map(|i| datadog_interop_fixture(&format!("dogstatsd-unix-{i:03}.raw")))
            .collect();
        assert_the_nine_recorded_constructs(&packets);
    }

    /// The stream capture, fed to the `unix_stream` framer as it arrived: every frame is a
    /// little-endian length and one packet, and the buffered connection packs several lines
    /// into one packet.
    #[test]
    fn interop_fixture_a_unix_stream_capture_frames_as_le_length_prefixed_packets() {
        let frames_of = |name: &str| {
            let mut framer = Framer::new(FramingMode::LengthPrefixedLe, MAX_FRAME_BYTES);
            framer.push(&datadog_interop_fixture(name));
            let mut frames = Vec::new();
            while let Some(frame) = framer.next_frame().expect("a recorded frame is well formed") {
                frames.push(frame);
            }
            frames
        };
        let unbuffered = frames_of("dogstatsd-unix-stream-000.raw");
        assert_the_nine_recorded_constructs(&unbuffered);

        let buffered = frames_of("dogstatsd-unix-stream-001.raw");
        assert!(buffered.len() < 9, "the buffered client packs lines: {} frames", buffered.len());
        let mut constructs = 0;
        for frame in &buffered {
            let (events, diagnostics) = decode_interop(frame);
            assert!(diagnostics.is_empty(), "{diagnostics:?}");
            constructs += events.len();
        }
        assert_eq!(constructs, 9, "every call arrived");
        assert!(
            buffered.iter().any(|f| f.iter().filter(|b| **b == b'\n').count() > 1),
            "at least one frame holds several lines"
        );
    }
}
