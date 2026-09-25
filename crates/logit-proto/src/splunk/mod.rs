//! Splunk's HTTP Event Collector (HEC): the codec behind `splunk_hec_in` and `splunk_hec_out`
//! ([ADR `splunk-hec-relay`](../../../../docs/adr/splunk-hec-relay.md),
//! [`docs/plans/splunk-relay.md`](../../../../docs/plans/splunk-relay.md)). This doc is the
//! canonical mapping table and permitted-normalization list; the submodules hold one signal each
//! ([`logs`], [`metrics`], [`spans`]), the timestamp rule ([`time`]), and the response bodies
//! ([`response`]).
//!
//! The vocabulary is the OpenTelemetry Collector's `splunk_hec` exporter's, not a `splunk.*`
//! namespace, so `otlp_out` to a Splunk Collector and `splunk_hec_out` put the same names in
//! Splunk. HTTP concerns (routes, auth, `Content-Encoding`, channels, acknowledgment) belong to the
//! listener and sink, never here.
//!
//! # API
//!
//! [`SplunkDecoder::decode_events`] takes one decompressed `/services/collector/event` body
//! (concatenated objects or a JSON array) and returns one [`EventBatch`] per distinct resource, in
//! first-appearance order, wire order kept within each. It isn't a [`crate::Decoder`]: one request
//! carries several resources. A body that isn't HEC JSON is a [`HecError`] carrying the status
//! and, for a syntax error, the index of the first bad object, so the listener answers what Splunk
//! would and delivers nothing. [`SplunkDecoder::decode_raw`] takes a `/raw` body and the
//! [`Envelope`] the listener read from the query string.
//!
//! [`SplunkEncoder::encode_objects`] writes one HEC JSON object per entry of a
//! [`MessageBuf<ObjectMeta>`](crate::MessageBuf), in batch order, and never fails: every loss is
//! counted. The sink packs objects greedily into bodies under its size cap and can drop object `N`
//! by slicing the same buffer, with no re-encode. Its [`crate::Encoder`] impl is the
//! concatenation, one body per batch.
//!
//! # Envelope
//!
//! Every object carries its own `time`, `host`, `source`, `sourcetype`, and `index`. The last
//! four are the batch [`Resource`]'s carriers:
//!
//! | Wire | Model |
//! |---|---|
//! | `host` | resource [`RESOURCE_ATTR_HOST_NAME`] (`Str`) |
//! | `source` | resource [`RESOURCE_ATTR_SOURCE`] |
//! | `sourcetype` | resource [`RESOURCE_ATTR_SOURCETYPE`] |
//! | `index` | resource [`RESOURCE_ATTR_INDEX`] |
//!
//! ## Decode: HEC → events
//!
//! | Wire | Model | Counter |
//! |---|---|---|
//! | body empty, whitespace, or `[]` | `HecError` code 5 (`No data`) | -- |
//! | object *i* not valid JSON, or not a JSON object | `HecError` code 6 with `invalid-event-number` *i*; nothing delivered | -- |
//! | object *i*'s `event`, `fields`, or a carrier holding a number outside `f64`'s range (`1e400`) or nesting past 127 levels, which the model can't hold | the same `HecError` code 6 | -- |
//! | a carrier `null` | absent | -- |
//! | a carrier that isn't a string | its JSON text | `logit.input.events.degraded{reason="non_string_envelope"}` |
//! | any other envelope key | dropped | `logit.input.events.degraded{reason="unknown_envelope_key"}` |
//! | `time`: a number, or a string holding one | `Event::timestamp`, every digit kept ([`time::parse_hec_time`]); `0` is the epoch | -- |
//! | `time` absent or `null` | `received_at` | -- |
//! | `time` another type, or a number or string that doesn't parse (`1e400`) | `received_at` | `degraded{reason="bad_time"}` |
//! | `fields` not an object | ignored | `degraded{reason="bad_fields"}` |
//! | `fields` object | event attributes (a span's: resource attributes), each value typed as [`crate::json`] converts it, a nested map flattened to dotted keys by [`crate::json::flatten_into`] (an empty map dropped, an array holding a container as its JSON text) | -- |
//! | objects with an equal resource | one batch, in first-appearance order | -- |
//! | `event` absent or `null`, with a `metric_name:<n>` field or both `metric_name` and `_value` | a metric event ([`metrics`]), as Splunk indexes it | see [`metrics`] |
//! | `event` absent or `null` otherwise | skipped; the rest delivered | `logit.input.events.skipped{reason="no_event"}` |
//! | `event` `""` | skipped; the rest delivered | `skipped{reason="blank_event"}` |
//! | `event` `"metric"` with a `metric_name:<n>` field, or both `metric_name` and `_value` | a metric event ([`metrics`]) | see [`metrics`] |
//! | `event` an object carrying valid `trace_id`, `span_id`, `start_time`, `end_time` | a span event ([`spans`]) | see [`spans`] |
//! | any other `event` | a log event ([`logs`]) | see [`logs`] |
//!
//! `/raw` ([`SplunkDecoder::decode_raw`]): one batch whose resource is the query-string
//! [`Envelope`]; one [`logit_core::Event::log`] per LF-delimited line, a trailing CR stripped,
//! empty lines skipped, each a `Str` message with `BodyFormat::Raw` stamped `received_at`.
//! Invalid UTF-8 is replaced, counted `degraded{reason="invalid_utf8"}` once per line.
//!
//! ## Encode: events → HEC
//!
//! One object per payload: an event carrying a log, metrics, and a span writes a log object, one
//! or more metric objects, and a span object, in that order. Every object writes `time`, then
//! each carrier present as a `Str` on the resource, then `event`, then `fields` when non-empty.
//!
//! | Model | Wire | Counter |
//! |---|---|---|
//! | `Event::timestamp` (a span's: its start) | `time`, seconds with up to nine decimals ([`time::write_hec_time`]) | -- |
//! | a carrier that isn't a `Str` | omitted | `logit.output.tags.dropped{reason="unrepresentable"}`, once per batch |
//! | every other resource attribute, then every event attribute (the event's winning on a key) | `fields`, a `Map` flattened to dotted keys by [`crate::json::flatten_into`] (a span's event attributes go in its `attributes` instead) | -- |
//! | an event with no log, metric, or span | nothing | `logit.output.events.skipped{reason="no_payload"}` |
//!
//! [`logs`], [`metrics`], and [`spans`] each carry their own rows.
//!
//! # Permitted normalizations
//!
//! `splunk_hec_in -> splunk_hec_out` is a fixed point modulo this list
//! (`tests/splunk_fixed_point.rs` pins it):
//!
//! 1. batching and framing: an array body leaves as concatenated objects, objects regroup into one
//!    batch per resource (first-appearance order, wire order within), and a sink splits them
//!    across requests by size;
//! 2. JSON formatting: whitespace and key order within an object, a number's spelling (`1` read as
//!    a metric value leaves as `1.0`, `1.50` as `1.5`), a string `time` leaves as a number, and
//!    `time` is written with trailing zeros trimmed;
//! 3. an absent `time` leaves as the receipt time, a non-string carrier as its JSON text, and a
//!    span's `fields` key spelled like a carrier the envelope lacks as that carrier;
//! 4. `fields` leaves flat ([`crate::json::flatten_into`]): a nested map as dotted keys, an empty
//!    map dropped, and an array holding a map or array as its JSON text;
//! 5. a metric event's single-metric form (`metric_name` + `_value`) leaves in multi-metric form
//!    (`metric_name:<n>`), its records in name order, a metric object with no `metric_type`
//!    leaves with `metric_type` `Gauge`, the OpenTelemetry exporter's form, one with no `event`
//!    leaves with `"event":"metric"`, and a measurement written as a numeric string leaves as a
//!    number;
//! 6. a span's `kind` spelled without the `SPAN_KIND_` prefix or as a number leaves as the
//!    exporter's `SPAN_KIND_*` name (unspecified as `SPAN_KIND_INTERNAL`), a status code the same
//!    way (`STATUS_CODE_*`), an absent `name` as `""`, and an absent `status` as
//!    `{"message":"","code":"STATUS_CODE_UNSET"}`;
//! 7. `/raw` splits a body into one event per line, as Splunk's line breaker does, and a relayed
//!    line leaves through `/event`;
//! 8. an unknown envelope key, a `fields` that isn't an object, an event with no `event` or a
//!    blank one, and a span's `fields` key spelled like a carrier the envelope also sets are
//!    dropped and counted;
//! 9. a hex id (a span's, parent's, or link's `trace_id`/`span_id`, and a log's `trace_id`/`span_id`
//!    fields when they parse) is read in either case and leaves lowercase.
//!
//! The model-side losses (a resource attribute returning as an event attribute, `Sum`
//! temporality, the multi-number kinds under `MultiValue`) are in [`metrics`], [`logs`], and
//! [`spans`].

pub mod logs;
pub mod metrics;
pub mod response;
pub mod spans;
pub mod time;

pub use response::{HecReply, HecStatus};

use crate::json::{flatten_into, json_text, json_to_value, write_str, JsonObject};
use crate::{CodecError, Encoder, MessageBuf, MultiValue, Signal};
use logit_core::interner::{intern, resolve};
use logit_core::{AttrMap, Diagnostics, Event, EventBatch, Resource, Symbol, Telemetry, Value};
use serde_json::value::RawValue;
use serde_json::{Map, Value as Json};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, LazyLock};

/// `host.name`: the envelope's `host`.
pub const RESOURCE_ATTR_HOST_NAME: &str = "host.name";
/// `com.splunk.source`: the envelope's `source`.
pub const RESOURCE_ATTR_SOURCE: &str = "com.splunk.source";
/// `com.splunk.sourcetype`: the envelope's `sourcetype`.
pub const RESOURCE_ATTR_SOURCETYPE: &str = "com.splunk.sourcetype";
/// `com.splunk.index`: the envelope's `index`.
pub const RESOURCE_ATTR_INDEX: &str = "com.splunk.index";
/// `otel.log.severity.text` / `.number`: the OpenTelemetry exporter's severity fields, decoded
/// into `LogRecord::severity` and kept as attributes.
pub const ATTR_SEVERITY_TEXT: &str = "otel.log.severity.text";
pub const ATTR_SEVERITY_NUMBER: &str = "otel.log.severity.number";
/// `otel.log.name`: the exporter's log event name, `LogRecord::event_name`.
pub const ATTR_LOG_NAME: &str = "otel.log.name";
/// `trace_id` / `span_id`: a log's hex trace reference, `LogRecord::trace`.
pub const ATTR_TRACE_ID: &str = "trace_id";
pub const ATTR_SPAN_ID: &str = "span_id";
/// `metric_type`: a metric object's kind dimension (`Gauge`, `Sum`, or a verbatim other value).
pub const ATTR_METRIC_TYPE: &str = "metric_type";

/// The four carrier symbols, interned once.
struct Carriers {
    host: Symbol,
    source: Symbol,
    sourcetype: Symbol,
    index: Symbol,
}

static CARRIERS: LazyLock<Carriers> = LazyLock::new(|| Carriers {
    host: intern(RESOURCE_ATTR_HOST_NAME),
    source: intern(RESOURCE_ATTR_SOURCE),
    sourcetype: intern(RESOURCE_ATTR_SOURCETYPE),
    index: intern(RESOURCE_ATTR_INDEX),
});

fn is_carrier(key: Symbol) -> bool {
    let c = &*CARRIERS;
    key == c.host || key == c.source || key == c.sourcetype || key == c.index
}

/// An object's `host`, `source`, `sourcetype`, and `index`; for `/raw`, the query string's.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Envelope {
    pub host: Option<String>,
    pub source: Option<String>,
    pub sourcetype: Option<String>,
    pub index: Option<String>,
}

impl Envelope {
    /// The batch [`Resource`] this envelope names: each present field as its carrier, a `Str`.
    pub fn to_resource(&self) -> Resource {
        let mut resource = Resource::default();
        self.insert_into(&mut resource.attributes);
        resource
    }

    fn insert_into(&self, attrs: &mut AttrMap) {
        let c = &*CARRIERS;
        for (key, value) in [
            (c.host, &self.host),
            (c.source, &self.source),
            (c.sourcetype, &self.sourcetype),
            (c.index, &self.index),
        ] {
            if let Some(value) = value {
                attrs.insert_sym(key, Value::str(value.as_str()));
            }
        }
    }

    /// The carriers `resource` holds as `Str`, and how many it holds as something else.
    fn from_resource(resource: &Resource) -> (Envelope, usize) {
        let c = &*CARRIERS;
        let mut unrepresentable = 0;
        let mut read = |key: Symbol| match resource.attributes.get_sym(key) {
            None => None,
            Some(value) => match value.as_str() {
                Some(s) => Some(s.to_string()),
                None => {
                    unrepresentable += 1;
                    None
                }
            },
        };
        let envelope = Envelope {
            host: read(c.host),
            source: read(c.source),
            sourcetype: read(c.sourcetype),
            index: read(c.index),
        };
        (envelope, unrepresentable)
    }
}

/// Why a whole `/event` body was rejected: the status to answer and, for a syntax error, the
/// zero-based index of the first object that failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("HEC code {}: {}", .status.code, .status.text)]
pub struct HecError {
    pub status: HecStatus,
    pub invalid_event_number: Option<u64>,
}

impl HecError {
    fn no_data() -> Self {
        HecError { status: HecStatus::NO_DATA, invalid_event_number: None }
    }

    fn invalid_at(index: usize) -> Self {
        HecError {
            status: HecStatus::INVALID_DATA_FORMAT,
            invalid_event_number: Some(index as u64),
        }
    }

    /// The response body Splunk sends for this error.
    pub fn body(&self) -> Vec<u8> {
        match self.invalid_event_number {
            Some(n) => response::encode_invalid_event(self.status, n),
            None => response::encode_status(self.status),
        }
    }
}

/// What one [`SplunkEncoder::encode_objects`] entry carries beyond its bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectMeta {
    /// The index in the batch of the event this object came from.
    pub event_index: usize,
    pub signal: Signal,
    /// The records the object carries: `1` for a log or span, the `metric_name:` count for a
    /// metric object.
    pub records: usize,
}

/// Decodes HEC request bodies. Every drop and degrade is counted through its [`Telemetry`]; a
/// default-constructed handle makes that a no-op.
#[derive(Default)]
pub struct SplunkDecoder {
    telemetry: Telemetry,
    diagnostics: Diagnostics,
}

/// One decoded object, before grouping.
type Object<'a> = BTreeMap<String, &'a RawValue>;

impl SplunkDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn with_diagnostics(mut self, diagnostics: Diagnostics) -> Self {
        self.diagnostics = diagnostics;
        self
    }

    /// Decodes one `/services/collector/event` body into one batch per distinct resource. Every
    /// object is parsed before any is decoded, so a syntax error anywhere rejects the whole body.
    pub fn decode_events(
        &mut self,
        body: &[u8],
        received_at: i64,
    ) -> Result<Vec<EventBatch>, HecError> {
        let objects = split_objects(body)?;
        let parsed = objects
            .iter()
            .enumerate()
            .map(|(i, object)| ParsedObject::parse(object).ok_or_else(|| HecError::invalid_at(i)))
            .collect::<Result<Vec<_>, _>>()?;
        let mut groups = Groups::default();
        for object in parsed {
            self.decode_object(object, received_at, &mut groups);
        }
        Ok(groups.into_batches())
    }

    /// Decodes one `/raw` body: one log event per line, all under `envelope`'s resource.
    pub fn decode_raw(&mut self, body: &[u8], envelope: &Envelope, received_at: i64) -> EventBatch {
        let mut events = Vec::new();
        for line in body.split(|&b| b == b'\n') {
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            if line.is_empty() {
                continue;
            }
            let text = match std::str::from_utf8(line) {
                Ok(text) => text.to_string(),
                Err(_) => {
                    self.degrade_event("invalid_utf8");
                    String::from_utf8_lossy(line).into_owned()
                }
            };
            events.push(logs::raw_line(Value::str(text), received_at));
        }
        EventBatch { resource: Arc::new(envelope.to_resource()), scope: None, events }
    }

    fn decode_object(&mut self, object: ParsedObject<'_>, received_at: i64, groups: &mut Groups) {
        for _ in 0..object.unknown_keys {
            self.degrade_event("unknown_envelope_key");
        }
        let envelope = Envelope {
            host: self.read_carrier(object.host),
            source: self.read_carrier(object.source),
            sourcetype: self.read_carrier(object.sourcetype),
            index: self.read_carrier(object.index),
        };
        let timestamp = object.time.and_then(|raw| self.read_time(raw)).unwrap_or(received_at);
        let fields = match object.fields {
            None | Some(Json::Null) => Map::new(),
            Some(Json::Object(map)) => map,
            Some(_) => {
                self.degrade_event("bad_fields");
                Map::new()
            }
        };
        let event = match object.event {
            // Splunk indexes an object with no `event` whose `fields` carry a measurement as a
            // metric; SC4S sends its own metrics that way.
            None | Some(Json::Null) if metrics::is_metric_fields(&fields) => {
                if let Some(decoded) = self.decode_metric(fields, timestamp) {
                    groups.push(envelope.to_resource(), decoded);
                }
                return;
            }
            None | Some(Json::Null) => return self.skip_event("no_event"),
            Some(Json::String(s)) if s.is_empty() => return self.skip_event("blank_event"),
            Some(event) => event,
        };

        if event.as_str() == Some(metrics::METRIC_EVENT) && metrics::is_metric_fields(&fields) {
            if let Some(decoded) = self.decode_metric(fields, timestamp) {
                groups.push(envelope.to_resource(), decoded);
            }
            return;
        }
        if let Json::Object(span) = &event {
            if spans::is_span(span) {
                match self.decode_span(span) {
                    Some(decoded) => {
                        let resource = self.span_resource(&envelope, &fields);
                        groups.push(resource, decoded);
                        return;
                    }
                    None => {
                        self.telemetry.count(
                            "logit.input.spans.degraded",
                            1.0,
                            &[("reason", "malformed_span")],
                        );
                        self.diagnostics.warn_throttled(
                            "malformed_span",
                            "HEC span object has a field that doesn't parse; kept as a log",
                        );
                    }
                }
            }
        }
        let decoded = self.decode_log(event, fields, timestamp);
        groups.push(envelope.to_resource(), decoded);
    }

    /// A span's resource: its carriers plus its `fields`, which the OpenTelemetry exporter fills
    /// from the span's resource attributes. A field spelled like a carrier the envelope also sets
    /// loses to the envelope.
    fn span_resource(&self, envelope: &Envelope, fields: &Map<String, Json>) -> Resource {
        let mut resource = Resource::default();
        for (key, value) in fields {
            if is_carrier(intern(key)) && envelope_has(envelope, key) {
                self.telemetry.count(
                    "logit.input.spans.degraded",
                    1.0,
                    &[("reason", "carrier_collision")],
                );
                continue;
            }
            flatten_into(key, &json_to_value(value), &mut resource.attributes);
        }
        envelope.insert_into(&mut resource.attributes);
        resource
    }

    /// `None` for `null`; an unparseable number or string, or any other type, is counted.
    fn read_time(&self, raw: &RawValue) -> Option<i64> {
        let text = raw.get();
        let nanos = match text.as_bytes().first() {
            _ if text == "null" => return None,
            Some(b'-' | b'0'..=b'9') => time::parse_hec_time(text),
            Some(b'"') => serde_json::from_str::<String>(text)
                .ok()
                .and_then(|s| time::parse_hec_time(s.trim())),
            _ => None,
        };
        if nanos.is_none() {
            self.degrade_event("bad_time");
        }
        nanos
    }

    fn read_carrier(&self, value: Option<Json>) -> Option<String> {
        match value? {
            Json::Null => None,
            Json::String(s) => Some(s),
            other => {
                self.degrade_event("non_string_envelope");
                Some(other.to_string())
            }
        }
    }

    fn degrade_event(&self, reason: &'static str) {
        self.telemetry.count("logit.input.events.degraded", 1.0, &[("reason", reason)]);
    }

    fn skip_event(&self, reason: &'static str) {
        self.telemetry.count("logit.input.events.skipped", 1.0, &[("reason", reason)]);
    }
}

fn envelope_has(envelope: &Envelope, carrier: &str) -> bool {
    match carrier {
        RESOURCE_ATTR_HOST_NAME => envelope.host.is_some(),
        RESOURCE_ATTR_SOURCE => envelope.source.is_some(),
        RESOURCE_ATTR_SOURCETYPE => envelope.sourcetype.is_some(),
        RESOURCE_ATTR_INDEX => envelope.index.is_some(),
        _ => false,
    }
}

/// One object's envelope members, parsed out of their `RawValue`s. `time` stays raw so its
/// number text keeps every digit.
struct ParsedObject<'a> {
    time: Option<&'a RawValue>,
    host: Option<Json>,
    source: Option<Json>,
    sourcetype: Option<Json>,
    index: Option<Json>,
    event: Option<Json>,
    fields: Option<Json>,
    unknown_keys: usize,
}

impl<'a> ParsedObject<'a> {
    /// `None` when a member is syntactically valid JSON that `serde_json::Value` can't hold: a
    /// number outside `f64`'s range (`1e400`), or nesting past `serde_json`'s recursion limit.
    /// The object is unrepresentable, so the body is rejected as Splunk rejects invalid data.
    fn parse(object: &Object<'a>) -> Option<Self> {
        let parse = |raw: &RawValue| serde_json::from_str::<Json>(raw.get()).ok();
        let mut parsed = ParsedObject {
            time: None,
            host: None,
            source: None,
            sourcetype: None,
            index: None,
            event: None,
            fields: None,
            unknown_keys: 0,
        };
        for (key, raw) in object {
            let raw: &'a RawValue = raw;
            match key.as_str() {
                "time" => parsed.time = Some(raw),
                "host" => parsed.host = Some(parse(raw)?),
                "source" => parsed.source = Some(parse(raw)?),
                "sourcetype" => parsed.sourcetype = Some(parse(raw)?),
                "index" => parsed.index = Some(parse(raw)?),
                "event" => parsed.event = Some(parse(raw)?),
                "fields" => parsed.fields = Some(parse(raw)?),
                _ => parsed.unknown_keys += 1,
            }
        }
        Some(parsed)
    }
}

/// Splits a body into its objects: a JSON array's elements, or concatenated top-level objects.
fn split_objects(body: &[u8]) -> Result<Vec<Object<'_>>, HecError> {
    let start = body.iter().position(|b| !b.is_ascii_whitespace()).ok_or_else(HecError::no_data)?;
    let body = &body[start..];
    let objects = if body[0] == b'[' {
        serde_json::from_slice::<Vec<Object<'_>>>(body)
            .map_err(|_| HecError::invalid_at(first_bad_element(body)))?
    } else {
        let mut objects = Vec::new();
        for (i, item) in
            serde_json::Deserializer::from_slice(body).into_iter::<Object<'_>>().enumerate()
        {
            objects.push(item.map_err(|_| HecError::invalid_at(i))?);
        }
        objects
    };
    if objects.is_empty() {
        return Err(HecError::no_data());
    }
    Ok(objects)
}

/// The index of the first element of a JSON array body (which failed to parse as a whole) that
/// isn't a valid object: a scan for the array's top-level commas, tracking strings and nesting,
/// then a parse of each element. When every element parses, the fault is in the array's own
/// syntax after the last one (a missing `]`, trailing bytes), and the index is the element count.
fn first_bad_element(body: &[u8]) -> usize {
    let mut elements = Vec::new();
    let (mut depth, mut in_string, mut escaped, mut from) = (0usize, false, false, 1usize);
    let mut closed = false;
    for (i, &b) in body.iter().enumerate() {
        if in_string {
            match b {
                _ if escaped => escaped = false,
                b'\\' => escaped = true,
                b'"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'[' | b'{' => depth += 1,
            b']' | b'}' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    elements.push(&body[from..i]);
                    closed = true;
                    break;
                }
            }
            b',' if depth == 1 => {
                elements.push(&body[from..i]);
                from = i + 1;
            }
            _ => {}
        }
    }
    if !closed {
        elements.push(&body[from.min(body.len())..]);
    }
    for (i, element) in elements.iter().enumerate() {
        if serde_json::from_slice::<Object<'_>>(element).is_err() {
            // `[]` scans as one empty element; an empty element anywhere else is a stray comma.
            return i;
        }
    }
    elements.len()
}

/// Decoded events grouped by resource, in first-appearance order.
#[derive(Default)]
struct Groups {
    batches: Vec<(Resource, Vec<Event>)>,
    /// The resource's attributes as JSON text → its index in `batches`.
    index: HashMap<String, usize>,
}

impl Groups {
    fn push(&mut self, resource: Resource, event: Event) {
        let key = json_text(&Value::Map(Box::new(resource.attributes.clone())));
        let slot = *self.index.entry(key).or_insert_with(|| {
            self.batches.push((resource, Vec::new()));
            self.batches.len() - 1
        });
        self.batches[slot].1.push(event);
    }

    fn into_batches(self) -> Vec<EventBatch> {
        self.batches
            .into_iter()
            .map(|(resource, events)| EventBatch {
                resource: Arc::new(resource),
                scope: None,
                events,
            })
            .collect()
    }
}

/// Encodes event batches into HEC `/event` objects. Every skip and degrade is counted through its
/// [`Telemetry`]; a disabled handle costs nothing.
#[derive(Default)]
pub struct SplunkEncoder {
    telemetry: Telemetry,
    diagnostics: Diagnostics,
    multi_value: MultiValue,
    /// One object's bytes, reused across objects.
    scratch: Vec<u8>,
}

/// What every object of one batch shares.
struct BatchContext<'a> {
    envelope: Envelope,
    resource: &'a Resource,
}

impl SplunkEncoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn with_diagnostics(mut self, diagnostics: Diagnostics) -> Self {
        self.diagnostics = diagnostics;
        self
    }

    /// What a metric kind with no single number does: skipped (the default) or expanded into the
    /// series [`metrics`]'s module doc lists.
    pub fn with_multi_value(mut self, multi_value: MultiValue) -> Self {
        self.multi_value = multi_value;
        self
    }

    /// Writes one HEC object per entry of `out` (cleared first), in batch order.
    pub fn encode_objects(&mut self, batch: &EventBatch, out: &mut MessageBuf<ObjectMeta>) {
        out.clear();
        let (envelope, unrepresentable) = Envelope::from_resource(&batch.resource);
        if unrepresentable > 0 {
            self.telemetry.count(
                "logit.output.tags.dropped",
                unrepresentable as f64,
                &[("reason", "unrepresentable")],
            );
        }
        let ctx = BatchContext { envelope, resource: &batch.resource };
        for (event_index, event) in batch.events.iter().enumerate() {
            if event.log.is_none() && event.metrics.is_empty() && event.span.is_none() {
                self.telemetry.count(
                    "logit.output.events.skipped",
                    1.0,
                    &[("reason", "no_payload")],
                );
                continue;
            }
            if let Some(log) = &event.log {
                self.encode_log(&ctx, event, log, event_index, out);
            }
            if !event.metrics.is_empty() {
                self.encode_metrics(&ctx, event, event_index, out);
            }
            if let Some(span) = &event.span {
                self.encode_span(&ctx, event, span, event_index, out);
            }
        }
    }

    /// `fields` for a log or metric object: every non-carrier resource attribute, then every
    /// event attribute, flattened, the event's value winning on a key.
    fn object_fields(resource: &Resource, event: &Event) -> AttrMap {
        let mut fields = AttrMap::new();
        for (key, value) in resource.attributes.iter() {
            if !is_carrier(key) {
                flatten_into(resolve(key), value, &mut fields);
            }
        }
        for (key, value) in event.attributes.iter() {
            flatten_into(resolve(key), value, &mut fields);
        }
        fields
    }

    fn reserved_key_dropped(&self, n: usize) {
        if n > 0 {
            self.telemetry.count(
                "logit.output.tags.dropped",
                n as f64,
                &[("reason", "reserved_key")],
            );
        }
    }
}

/// Writes an object's envelope (`time` and every carrier present) and returns it open, for the
/// caller to add `event` and `fields`.
fn begin_object<'a>(out: &'a mut Vec<u8>, timestamp: i64, envelope: &Envelope) -> JsonObject<'a> {
    let mut obj = JsonObject::begin(out);
    time::write_hec_time(obj.key("time"), timestamp);
    for (key, value) in [
        ("host", &envelope.host),
        ("source", &envelope.source),
        ("sourcetype", &envelope.sourcetype),
        ("index", &envelope.index),
    ] {
        if let Some(value) = value {
            write_str(obj.key(key), value);
        }
    }
    obj
}

/// Writes `fields` as a JSON object member, when non-empty.
fn write_fields(
    obj: &mut JsonObject<'_>,
    fields: &AttrMap,
    trailer: impl FnOnce(&mut JsonObject<'_>),
) {
    let mut inner = JsonObject::begin(obj.key("fields"));
    for (key, value) in fields.iter() {
        crate::json::write_value(inner.key(resolve(key)), value);
    }
    trailer(&mut inner);
    inner.finish();
}

impl Encoder for SplunkEncoder {
    /// The batch's objects concatenated, one `/event` body.
    fn encode(&mut self, batch: &EventBatch) -> Result<bytes::Bytes, CodecError> {
        let mut objects = MessageBuf::default();
        self.encode_objects(batch, &mut objects);
        let mut body = Vec::with_capacity(objects.total_bytes());
        for object in objects.iter() {
            body.extend_from_slice(object);
        }
        Ok(bytes::Bytes::from(body))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use logit_core::telemetry::Registry;
    use logit_core::MetricKind;

    pub(crate) const RECEIVED_AT: i64 = 1_690_000_000_000_000_000;

    thread_local! {
        /// Every telemetry point drained so far, per registry, so [`counted`] can be called more
        /// than once on one registry.
        static DRAINED: std::cell::RefCell<HashMap<usize, Vec<Event>>> = Default::default();
    }

    /// The summed value of every `metric` point tagged `tag` that `registry` has recorded.
    pub(crate) fn counted(registry: &Registry, metric: &str, tag: (&str, &str)) -> f64 {
        DRAINED.with(|drained| {
            let mut drained = drained.borrow_mut();
            let events = drained.entry(registry as *const Registry as usize).or_default();
            events.extend(registry.drain(0));
            let mut total = 0.0;
            for event in events.iter() {
                if event.attributes.get(tag.0).and_then(|v| v.as_str()) != Some(tag.1) {
                    continue;
                }
                for m in &event.metrics {
                    if resolve(m.name) == metric {
                        if let MetricKind::Sum(sum) = &m.kind {
                            total += sum.value;
                        }
                    }
                }
            }
            total
        })
    }

    pub(crate) fn decoder(registry: &Registry) -> SplunkDecoder {
        SplunkDecoder::new().with_telemetry(registry.telemetry_for(
            "splunk_hec_in",
            "splunk_hec_in",
            "listener",
        ))
    }

    pub(crate) fn encoder(registry: &Registry) -> SplunkEncoder {
        SplunkEncoder::new().with_telemetry(registry.telemetry_for(
            "splunk_hec_out",
            "splunk_hec_out",
            "sink",
        ))
    }

    pub(crate) fn decode(body: &str) -> Vec<EventBatch> {
        SplunkDecoder::new().decode_events(body.as_bytes(), RECEIVED_AT).expect("decodes")
    }

    pub(crate) fn encode(batches: &[EventBatch]) -> String {
        let mut encoder = SplunkEncoder::new();
        let mut out = String::new();
        for batch in batches {
            out.push_str(std::str::from_utf8(&encoder.encode(batch).unwrap()).unwrap());
        }
        out
    }

    #[test]
    fn empty_bodies_are_no_data() {
        let mut decoder = SplunkDecoder::new();
        for body in ["", "  \n", "[]", " [ ] "] {
            let err = decoder.decode_events(body.as_bytes(), 0).unwrap_err();
            assert_eq!(err.status, HecStatus::NO_DATA, "{body:?}");
            assert_eq!(err.body(), br#"{"text":"No data","code":5}"#.to_vec());
        }
    }

    #[test]
    fn a_syntax_error_names_the_first_bad_object() {
        let mut decoder = SplunkDecoder::new();
        for (body, index) in [
            (r#"{"event":"a"}{"event":"b""#, 1),
            (r#"{"event":"a"}{"event":"b"}x"#, 2),
            (r#"{"event":"a"} 7 {"event":"b"}"#, 1),
            (r#"[{"event":"a"},{"event":"b",},{"event":"c"}]"#, 1),
            (r#"[{"event":"a,]"},{"event":"b"}"#, 2),
            (r#"[{"event":"a"},"x"]"#, 1),
            (r#"[{"event":"a"}] {"#, 1),
            (r#"not json"#, 0),
        ] {
            let err = decoder.decode_events(body.as_bytes(), 0).unwrap_err();
            assert_eq!(err.status, HecStatus::INVALID_DATA_FORMAT, "{body}");
            assert_eq!(err.invalid_event_number, Some(index), "{body}");
        }
        let err = decoder.decode_events(br#"{"event":"a"}{"#, 0).unwrap_err();
        assert_eq!(
            err.body(),
            br#"{"text":"Invalid data format","code":6,"invalid-event-number":1}"#.to_vec()
        );
    }

    #[test]
    fn a_member_the_model_cant_hold_rejects_the_body_with_its_index() {
        let deep = format!(r#"{{"event":{}1{}}}"#, "[".repeat(130), "]".repeat(130));
        let cases = [
            (r#"{"event":"a"}{"event":"x","fields":{"a":1e400,"b":"keep"}}"#.to_string(), 1),
            (r#"{"event":{"n":1e400}}"#.to_string(), 0),
            (r#"{"event":"metric","fields":{"metric_name:x":1e400}}"#.to_string(), 0),
            (r#"{"event":"x","host":1e400}"#.to_string(), 0),
            (format!(r#"{{"event":"a"}}{{"event":"b"}}{deep}"#), 2),
        ];
        for (body, index) in cases {
            let err = SplunkDecoder::new().decode_events(body.as_bytes(), 0).unwrap_err();
            assert_eq!(err.status, HecStatus::INVALID_DATA_FORMAT, "{body}");
            assert_eq!(err.invalid_event_number, Some(index), "{body}");
        }
    }

    #[test]
    fn an_unparseable_time_number_is_counted() {
        let registry = Registry::new();
        let batches = decoder(&registry)
            .decode_events(br#"{"event":"a","time":1e400}{"event":"b","time":"x"}"#, RECEIVED_AT)
            .unwrap();
        assert!(batches[0].events.iter().all(|e| e.timestamp == RECEIVED_AT));
        assert_eq!(counted(&registry, "logit.input.events.degraded", ("reason", "bad_time")), 2.0);
    }

    #[test]
    fn objects_group_by_resource_in_first_appearance_order() {
        let batches = decode(
            r#"{"event":"a","host":"h1"}{"event":"b","host":"h2","index":"i"}{"event":"c","host":"h1"}"#,
        );
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].resource.attributes.get("host.name"), Some(&Value::str("h1")));
        assert_eq!(batches[0].events.len(), 2);
        let messages: Vec<_> =
            batches[0].events.iter().map(|e| e.log.as_ref().unwrap().message.clone()).collect();
        assert_eq!(messages, vec![Value::str("a"), Value::str("c")]);
        assert_eq!(batches[1].resource.attributes.get("com.splunk.index"), Some(&Value::str("i")));
        assert_eq!(batches[1].resource.attributes.len(), 2);
    }

    #[test]
    fn an_array_body_and_concatenated_objects_decode_alike() {
        let concatenated = decode(r#"{"event":"a","time":1} {"event":"b","time":2}"#);
        let array = decode(r#"[{"event":"a","time":1},{"event":"b","time":2}]"#);
        assert_eq!(concatenated, array);
    }

    #[test]
    fn envelope_oddities_are_counted() {
        let registry = Registry::new();
        let batches = decoder(&registry)
            .decode_events(
                br#"{"event":"a","host":7,"extra":1,"fields":[1],"time":true}{"event":null}{"event":""}{"time":1}"#,
                RECEIVED_AT,
            )
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].resource.attributes.get("host.name"), Some(&Value::str("7")));
        assert_eq!(batches[0].events[0].timestamp, RECEIVED_AT);
        for reason in ["non_string_envelope", "unknown_envelope_key", "bad_fields", "bad_time"] {
            assert_eq!(counted(&registry, "logit.input.events.degraded", ("reason", reason)), 1.0);
        }
        assert_eq!(counted(&registry, "logit.input.events.skipped", ("reason", "no_event")), 2.0);
        assert_eq!(
            counted(&registry, "logit.input.events.skipped", ("reason", "blank_event")),
            1.0
        );
    }

    #[test]
    fn time_forms_decode_to_nanoseconds() {
        let batches = decode(
            r#"{"event":"a","time":1700000000.123456789}{"event":"b","time":"1700000000.5"}{"event":"c","time":0}{"event":"d","time":null}"#,
        );
        let times: Vec<i64> = batches[0].events.iter().map(|e| e.timestamp).collect();
        assert_eq!(
            times,
            vec![1_700_000_000_123_456_789, 1_700_000_000_500_000_000, 0, RECEIVED_AT]
        );
    }

    #[test]
    fn raw_bodies_split_on_lines_under_the_query_envelope() {
        let registry = Registry::new();
        let envelope = Envelope {
            host: Some("h".into()),
            sourcetype: Some("syslog".into()),
            ..Envelope::default()
        };
        let batch =
            decoder(&registry).decode_raw(b"one\r\ntwo\n\n\xffthree\n", &envelope, RECEIVED_AT);
        assert_eq!(batch.resource.attributes.get("host.name"), Some(&Value::str("h")));
        assert_eq!(
            batch.resource.attributes.get("com.splunk.sourcetype"),
            Some(&Value::str("syslog"))
        );
        let messages: Vec<_> =
            batch.events.iter().map(|e| e.log.as_ref().unwrap().message.clone()).collect();
        assert_eq!(
            messages,
            vec![Value::str("one"), Value::str("two"), Value::str("\u{fffd}three")]
        );
        assert!(batch.events.iter().all(|e| e.timestamp == RECEIVED_AT));
        assert_eq!(
            counted(&registry, "logit.input.events.degraded", ("reason", "invalid_utf8")),
            1.0
        );

        // A relayed `/raw` line leaves through `/event` and comes back the same.
        let again = decode(&encode(std::slice::from_ref(&batch)));
        assert_eq!(again, vec![batch]);
    }

    #[test]
    fn a_non_string_carrier_is_omitted_and_counted() {
        let registry = Registry::new();
        let mut batch = decode(r#"{"event":"a","time":1,"host":"h"}"#).remove(0);
        let mut resource = (*batch.resource).clone();
        resource.attributes.insert("com.splunk.index", Value::I64(3));
        batch.resource = Arc::new(resource);
        let mut out = MessageBuf::default();
        encoder(&registry).encode_objects(&batch, &mut out);
        assert_eq!(out.iter().next().unwrap(), br#"{"time":1,"host":"h","event":"a"}"#);
        assert_eq!(
            counted(&registry, "logit.output.tags.dropped", ("reason", "unrepresentable")),
            1.0
        );
    }

    #[test]
    fn an_event_with_no_payload_is_skipped_and_counted() {
        let registry = Registry::new();
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![Event::empty(0, AttrMap::new())],
        };
        let mut out = MessageBuf::default();
        encoder(&registry).encode_objects(&batch, &mut out);
        assert!(out.is_empty());
        assert_eq!(
            counted(&registry, "logit.output.events.skipped", ("reason", "no_payload")),
            1.0
        );
    }
}
