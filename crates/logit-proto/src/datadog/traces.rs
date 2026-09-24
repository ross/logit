//! Datadog spans to and from [`SpanRecord`]s: the mapping every trace form shares. The wire forms
//! themselves live in [`super::traces_msgpack`] (the tracer API's v0.4, v0.5, and v0.7 msgpack)
//! and [`super::traces_proto`] (the intake's `AgentPayload` protobuf); both parse into, and build
//! from, the `Wire*` structs here, so one span means one thing on every route. The mapping tables
//! are in [`super`]'s module doc, under "Traces".
//!
//! The `Wire*` structs mirror `span.proto`/`tracer_payload.proto`/`agent_payload.proto` field for
//! field, except that every map is an ordered `Vec` of pairs: prost's generated types hold
//! `HashMap`s, whose iteration order is random per instance, so encoding through them would make
//! two encodes of one batch differ byte for byte. The encoders sort every map by key instead.

use super::logs::value_text;
use super::{
    merged_get, DatadogDecoder, DatadogEncoder, ATTR_RESOURCE_NAME, ATTR_SERVICE_NAME,
    ATTR_SPAN_KIND, ATTR_SPAN_TYPE, RESOURCE_ATTR_AGENT_ENV, RESOURCE_ATTR_AGENT_HOSTNAME,
    RESOURCE_ATTR_AGENT_VERSION, RESOURCE_ATTR_TRACER_APP_VERSION,
    RESOURCE_ATTR_TRACER_CONTAINER_ID, RESOURCE_ATTR_TRACER_ENV, RESOURCE_ATTR_TRACER_HOSTNAME,
    RESOURCE_ATTR_TRACER_LANGUAGE_NAME, RESOURCE_ATTR_TRACER_RUNTIME_ID,
    RESOURCE_ATTR_TRACER_VERSION,
};
use bytes::Bytes;
use logit_core::interner::resolve;
use logit_core::trace::{parse_trace_id_high, trace_id_bytes, trace_id_halves};
use logit_core::{
    format_rfc3339_utc, AttrMap, Event, EventBatch, Resource, SpanEvent, SpanKind, SpanLink,
    SpanRecord, SpanStatus, Value,
};
use std::collections::HashMap;

/// `_dd.p.tid`: the `meta` key holding a 128-bit trace id's high 64 bits as hex.
pub const META_TRACE_ID_HIGH: &str = "_dd.p.tid";
/// `datadog.span.error`: a span's `error` when it is neither 0 nor 1 (`I64`), so it re-encodes
/// exactly; 0 and 1 are carried by [`SpanRecord::status`] alone.
pub const ATTR_SPAN_ERROR: &str = "datadog.span.error";
/// `datadog.chunk.priority` / `.origin` / `.dropped_trace` / `.tags`: a v0.7 or `AgentPayload`
/// `TraceChunk`'s fields, on every span of the chunk (`I64`, `Str`, `Bool`, `Map` of `Str`).
pub const ATTR_CHUNK_PRIORITY: &str = "datadog.chunk.priority";
pub const ATTR_CHUNK_ORIGIN: &str = "datadog.chunk.origin";
pub const ATTR_CHUNK_DROPPED_TRACE: &str = "datadog.chunk.dropped_trace";
pub const ATTR_CHUNK_TAGS: &str = "datadog.chunk.tags";
/// `datadog.tracer.language_version` / `.tags` / `.container_debug`: `TracerPayload` fields with
/// no APM-stats counterpart, as batch resource attributes; the carriers shared with
/// [`super::stats`] (`container_id`, `language_name`, `tracer_version`, `runtime_id`, `env`,
/// `hostname`, `app_version`) live in [`super`].
pub const RESOURCE_ATTR_TRACER_LANGUAGE_VERSION: &str = "datadog.tracer.language_version";
pub const RESOURCE_ATTR_TRACER_TAGS: &str = "datadog.tracer.tags";
pub const RESOURCE_ATTR_TRACER_CONTAINER_DEBUG: &str = "datadog.tracer.container_debug";
/// `datadog.agent.target_tps` / `.error_tps` / `.rare_sampler_enabled` / `.tags`: `AgentPayload`
/// fields with no APM-stats counterpart, as batch resource attributes; `hostname`, `env`, and
/// `version` live in [`super`] ([`RESOURCE_ATTR_AGENT_HOSTNAME`], [`RESOURCE_ATTR_AGENT_ENV`],
/// [`RESOURCE_ATTR_AGENT_VERSION`]).
pub const RESOURCE_ATTR_AGENT_TARGET_TPS: &str = "datadog.agent.target_tps";
pub const RESOURCE_ATTR_AGENT_ERROR_TPS: &str = "datadog.agent.error_tps";
pub const RESOURCE_ATTR_AGENT_RARE_SAMPLER_ENABLED: &str = "datadog.agent.rare_sampler_enabled";
pub const RESOURCE_ATTR_AGENT_TAGS: &str = "datadog.agent.tags";

/// `sampler.PriorityNone`: the chunk priority the Agent's own v0.4/v0.5 decoders stamp, meaning
/// "not decided yet". Decodes to no [`ATTR_CHUNK_PRIORITY`] and is what a span without one
/// encodes to.
pub const PRIORITY_NONE: i32 = -128;

const TRACER_STR_CARRIERS: [&str; 8] = [
    RESOURCE_ATTR_TRACER_CONTAINER_ID,
    RESOURCE_ATTR_TRACER_LANGUAGE_NAME,
    RESOURCE_ATTR_TRACER_LANGUAGE_VERSION,
    RESOURCE_ATTR_TRACER_VERSION,
    RESOURCE_ATTR_TRACER_RUNTIME_ID,
    RESOURCE_ATTR_TRACER_ENV,
    RESOURCE_ATTR_TRACER_HOSTNAME,
    RESOURCE_ATTR_TRACER_APP_VERSION,
];

/// Every `TracerPayload` carrier, the order [`DatadogEncoder::resource_carriers_lost`] counts in.
const TRACER_CARRIERS: [&str; 10] = [
    RESOURCE_ATTR_TRACER_CONTAINER_ID,
    RESOURCE_ATTR_TRACER_LANGUAGE_NAME,
    RESOURCE_ATTR_TRACER_LANGUAGE_VERSION,
    RESOURCE_ATTR_TRACER_VERSION,
    RESOURCE_ATTR_TRACER_RUNTIME_ID,
    RESOURCE_ATTR_TRACER_ENV,
    RESOURCE_ATTR_TRACER_HOSTNAME,
    RESOURCE_ATTR_TRACER_APP_VERSION,
    RESOURCE_ATTR_TRACER_TAGS,
    RESOURCE_ATTR_TRACER_CONTAINER_DEBUG,
];

const AGENT_CARRIERS: [&str; 7] = [
    RESOURCE_ATTR_AGENT_HOSTNAME,
    RESOURCE_ATTR_AGENT_ENV,
    RESOURCE_ATTR_AGENT_VERSION,
    RESOURCE_ATTR_AGENT_TARGET_TPS,
    RESOURCE_ATTR_AGENT_ERROR_TPS,
    RESOURCE_ATTR_AGENT_RARE_SAMPLER_ENABLED,
    RESOURCE_ATTR_AGENT_TAGS,
];

/// `container_debug`'s five fields, as the keys of the [`RESOURCE_ATTR_TRACER_CONTAINER_DEBUG`]
/// map (the msgpack keys, which are also the proto field names).
const DEBUG_ERROR: &str = "error";
const DEBUG_LATENCY_MS: &str = "latency_ms";
const DEBUG_WAS_BUFFERED: &str = "was_buffered";
const DEBUG_BUFFER_MS: &str = "buffer_ms";
const DEBUG_BUFFER_EVICTION_REASON: &str = "buffer_eviction_reason";

/// A string-to-string wire map, in wire order on decode and key order on encode.
pub(super) type StrMap = Vec<(String, String)>;

/// `span.proto`'s `Span`.
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct WireSpan {
    pub service: String,
    pub name: String,
    pub resource: String,
    pub trace_id: u64,
    pub span_id: u64,
    pub parent_id: u64,
    pub start: i64,
    pub duration: i64,
    pub error: i32,
    pub meta: StrMap,
    pub metrics: Vec<(String, f64)>,
    pub r#type: String,
    pub meta_struct: Vec<(String, Vec<u8>)>,
    pub span_links: Vec<WireLink>,
    pub span_events: Vec<WireSpanEvent>,
}

/// `span.proto`'s `SpanLink`.
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct WireLink {
    pub trace_id: u64,
    pub trace_id_high: u64,
    pub span_id: u64,
    pub attributes: StrMap,
    pub tracestate: String,
    pub flags: u32,
}

/// `span.proto`'s `SpanEvent`.
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct WireSpanEvent {
    pub time_unix_nano: u64,
    pub name: String,
    pub attributes: Vec<(String, WireAny)>,
}

/// `AttributeAnyValue` (and, inside `Array`, `AttributeArrayValue`), resolved by its `type`.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum WireAny {
    Str(String),
    Bool(bool),
    Int(i64),
    Double(f64),
    Array(Vec<WireAny>),
    /// A `type` outside the enum (or `ARRAY_VALUE` inside an array): dropped on decode, counted.
    Unknown,
}

/// `AttributeAnyValueType`'s codes, shared by `AttributeArrayValueType` for 0 to 3.
pub(super) const ANY_STRING: i64 = 0;
pub(super) const ANY_BOOL: i64 = 1;
pub(super) const ANY_INT: i64 = 2;
pub(super) const ANY_DOUBLE: i64 = 3;
pub(super) const ANY_ARRAY: i64 = 4;

impl WireAny {
    /// Resolves an `AttributeAnyValue`'s manual union from its fields, as the Agent does: the
    /// `type` picks which field counts. `array` is `None` for an `AttributeArrayValue`, which has
    /// no `ARRAY_VALUE`.
    pub(super) fn from_parts(
        kind: i64,
        string: String,
        boolean: bool,
        int: i64,
        double: f64,
        array: Option<Vec<WireAny>>,
    ) -> WireAny {
        match kind {
            ANY_STRING => WireAny::Str(string),
            ANY_BOOL => WireAny::Bool(boolean),
            ANY_INT => WireAny::Int(int),
            ANY_DOUBLE => WireAny::Double(double),
            ANY_ARRAY => array.map_or(WireAny::Unknown, WireAny::Array),
            _ => WireAny::Unknown,
        }
    }

    /// The `type` code this value encodes under.
    pub(super) fn kind(&self) -> i64 {
        match self {
            WireAny::Str(_) | WireAny::Unknown => ANY_STRING,
            WireAny::Bool(_) => ANY_BOOL,
            WireAny::Int(_) => ANY_INT,
            WireAny::Double(_) => ANY_DOUBLE,
            WireAny::Array(_) => ANY_ARRAY,
        }
    }
}

/// `tracer_payload.proto`'s `TraceChunk`.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct WireChunk {
    pub priority: i32,
    pub origin: String,
    pub spans: Vec<WireSpan>,
    pub tags: StrMap,
    pub dropped_trace: bool,
}

impl Default for WireChunk {
    fn default() -> Self {
        WireChunk {
            priority: PRIORITY_NONE,
            origin: String::new(),
            spans: Vec::new(),
            tags: Vec::new(),
            dropped_trace: false,
        }
    }
}

/// `tracer_payload.proto`'s `TracerPayload`.
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct WireTracer {
    pub container_id: String,
    pub language_name: String,
    pub language_version: String,
    pub tracer_version: String,
    pub runtime_id: String,
    pub chunks: Vec<WireChunk>,
    pub tags: StrMap,
    pub env: String,
    pub hostname: String,
    pub app_version: String,
    pub container_debug: Option<WireContainerDebug>,
}

/// `tracer_payload.proto`'s `ContainerDebug`.
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct WireContainerDebug {
    pub error: String,
    pub latency_ms: i64,
    pub was_buffered: bool,
    pub buffer_ms: i64,
    pub buffer_eviction_reason: String,
}

/// `agent_payload.proto`'s `AgentPayload`, minus `idxTracerPayloads` (the unimplemented v1.0
/// form), which decode only counts.
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct WireAgent {
    pub host_name: String,
    pub env: String,
    pub tracer_payloads: Vec<WireTracer>,
    pub tags: StrMap,
    pub agent_version: String,
    pub target_tps: f64,
    pub error_tps: f64,
    pub rare_sampler_enabled: bool,
}

/// Which wire an encode is for: decides which fields have nowhere to go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Form {
    V04,
    V05,
    V07,
    Agent,
}

impl Form {
    /// v0.4 and v0.5 have no chunk envelope: `datadog.chunk.*` has nowhere to go.
    fn has_chunks(self) -> bool {
        matches!(self, Form::V07 | Form::Agent)
    }
}

fn str_attrs(map: &StrMap) -> AttrMap {
    let mut attrs = AttrMap::new();
    for (k, v) in map {
        attrs.insert(k, Value::str(v.as_str()));
    }
    attrs
}

fn str_map_value(map: &StrMap) -> Value {
    Value::Map(Box::new(str_attrs(map)))
}

/// Inserts `key`, counting a key already present (a `meta` and `metrics` key of one name, or a
/// carrier spelled like a `meta` key): the later write wins.
fn put(attrs: &mut AttrMap, collisions: &mut usize, key: &str, value: Value) {
    if attrs.get(key).is_some() {
        *collisions += 1;
    }
    attrs.insert(key, value);
}

impl DatadogDecoder {
    pub(super) fn span_skipped(&self, reason: &'static str, n: usize) {
        if n > 0 {
            self.telemetry.count("logit.input.spans.skipped", n as f64, &[("reason", reason)]);
        }
    }

    pub(super) fn span_degraded(&self, reason: &'static str, n: usize) {
        if n > 0 {
            self.telemetry.count("logit.input.spans.degraded", n as f64, &[("reason", reason)]);
        }
    }

    /// Drops one span (or a whole chunk or trace, `n` spans) whose shape is wrong, keeping the
    /// rest of the request.
    pub(super) fn malformed_span(&mut self, n: usize, why: impl std::fmt::Display) {
        self.span_skipped("malformed", n.max(1));
        self.diagnostics
            .warn_throttled("malformed_span", format!("datadog: malformed span dropped: {why}"));
    }

    /// One chunk's spans as events, in wire order. `carriers` is false for v0.4/v0.5, which have
    /// no chunk on the wire (the Agent invents a `PriorityNone` one), so nothing chunk-level is
    /// recorded.
    ///
    /// `_dd.p.tid` is set on one span per chunk (by tracer convention), but it names the whole
    /// trace's high bits, so every span of the chunk with the same low 64 bits gets them; a span
    /// carrying its own parseable `_dd.p.tid` uses that. Only the carrying span keeps the
    /// attribute, which is what lets the encoder put it back on that span alone.
    pub(super) fn chunk_events(&mut self, chunk: WireChunk, carriers: bool, out: &mut Vec<Event>) {
        let mut chunk_high: HashMap<u64, u64> = HashMap::new();
        for span in &chunk.spans {
            if let Some(high) = span
                .meta
                .iter()
                .rev()
                .find(|(k, _)| k == META_TRACE_ID_HIGH)
                .and_then(|(_, v)| parse_trace_id_high(v))
            {
                chunk_high.entry(span.trace_id).or_insert(high);
            }
        }
        let mut chunk_attrs: Vec<(&'static str, Value)> = Vec::new();
        if carriers {
            if chunk.priority != PRIORITY_NONE {
                chunk_attrs.push((ATTR_CHUNK_PRIORITY, Value::I64(chunk.priority.into())));
            }
            if !chunk.origin.is_empty() {
                chunk_attrs.push((ATTR_CHUNK_ORIGIN, Value::str(chunk.origin.as_str())));
            }
            if chunk.dropped_trace {
                chunk_attrs.push((ATTR_CHUNK_DROPPED_TRACE, Value::Bool(true)));
            }
            if !chunk.tags.is_empty() {
                chunk_attrs.push((ATTR_CHUNK_TAGS, str_map_value(&chunk.tags)));
            }
        }
        for span in chunk.spans {
            let fallback = chunk_high.get(&span.trace_id).copied().unwrap_or(0);
            out.push(self.span_event(span, fallback, &chunk_attrs));
        }
    }

    /// One wire span as one `Event::span`. `fallback_high` is the chunk's `_dd.p.tid`, for a
    /// span that carries no parseable one of its own.
    fn span_event(
        &mut self,
        span: WireSpan,
        fallback_high: u64,
        chunk_attrs: &[(&'static str, Value)],
    ) -> Event {
        let mut attrs = AttrMap::new();
        let mut collisions = 0;
        let mut high = None;
        for (k, v) in &span.meta {
            if k == META_TRACE_ID_HIGH {
                high = parse_trace_id_high(v);
                if high.is_none() {
                    self.span_degraded("bad_tid", 1);
                }
            }
            put(&mut attrs, &mut collisions, k, Value::str(v.as_str()));
        }
        for (k, v) in &span.metrics {
            put(&mut attrs, &mut collisions, k, Value::F64(*v));
        }
        for (k, v) in span.meta_struct {
            put(&mut attrs, &mut collisions, &k, Value::Bytes(Bytes::from(v)));
        }
        for (key, value) in [
            (ATTR_SERVICE_NAME, &span.service),
            (ATTR_RESOURCE_NAME, &span.resource),
            (ATTR_SPAN_TYPE, &span.r#type),
        ] {
            if !value.is_empty() {
                put(&mut attrs, &mut collisions, key, Value::str(value.as_str()));
            }
        }
        if span.error != 0 && span.error != 1 {
            put(&mut attrs, &mut collisions, ATTR_SPAN_ERROR, Value::I64(span.error.into()));
        }
        for (key, value) in chunk_attrs {
            put(&mut attrs, &mut collisions, key, value.clone());
        }
        self.span_degraded("key_collision", collisions);
        // Read back after every write, so a `metrics` or `meta_struct` entry that displaced the
        // `meta` one decides, exactly as it will on the next hop.
        let kind = attrs
            .get(ATTR_SPAN_KIND)
            .and_then(Value::as_str)
            .and_then(SpanKind::from_name)
            .unwrap_or(SpanKind::Internal);

        let duration = if span.duration < 0 {
            self.span_degraded("negative_duration", 1);
            0
        } else {
            span.duration
        };
        let links = span
            .span_links
            .into_iter()
            .map(|l| SpanLink {
                trace_id: trace_id_bytes(l.trace_id_high, l.trace_id),
                span_id: l.span_id.to_be_bytes(),
                attributes: str_attrs(&l.attributes),
                flags: l.flags,
                trace_state: (!l.tracestate.is_empty()).then(|| Bytes::from(l.tracestate)),
                dropped_attributes_count: 0,
            })
            .collect();
        let events = span.span_events.into_iter().map(|e| self.span_event_record(e)).collect();
        let record = SpanRecord {
            trace_id: trace_id_bytes(high.unwrap_or(fallback_high), span.trace_id),
            span_id: span.span_id.to_be_bytes(),
            parent_span_id: (span.parent_id != 0).then(|| span.parent_id.to_be_bytes()),
            name: Value::str(span.name),
            kind,
            status: if span.error != 0 { SpanStatus::Error } else { SpanStatus::Unset },
            events,
            links,
            end_timestamp: span.start.saturating_add(duration),
            flags: 0,
            ext: None,
        };
        Event::span(span.start, attrs, record)
    }

    fn span_event_record(&mut self, e: WireSpanEvent) -> SpanEvent {
        let timestamp = i64::try_from(e.time_unix_nano).unwrap_or_else(|_| {
            self.span_degraded("timestamp_range", 1);
            i64::MAX
        });
        let mut attributes = AttrMap::new();
        for (k, v) in e.attributes {
            match any_to_value(v) {
                Some(value) => attributes.insert(&k, value),
                None => self.span_degraded("bad_attribute_type", 1),
            }
        }
        SpanEvent { timestamp, name: Value::str(e.name), attributes, dropped_attributes_count: 0 }
    }

    /// A `TracerPayload`'s fields (and, from the intake, its `AgentPayload`'s) as the batch
    /// resource, each only when non-empty, nonzero, or true.
    pub(super) fn tracer_resource(
        &self,
        tracer: &WireTracer,
        agent: Option<&WireAgent>,
    ) -> Resource {
        let mut resource = Resource::default();
        let attrs = &mut resource.attributes;
        for (key, value) in TRACER_STR_CARRIERS.into_iter().zip([
            &tracer.container_id,
            &tracer.language_name,
            &tracer.language_version,
            &tracer.tracer_version,
            &tracer.runtime_id,
            &tracer.env,
            &tracer.hostname,
            &tracer.app_version,
        ]) {
            if !value.is_empty() {
                attrs.insert(key, Value::str(value.as_str()));
            }
        }
        if !tracer.tags.is_empty() {
            attrs.insert(RESOURCE_ATTR_TRACER_TAGS, str_map_value(&tracer.tags));
        }
        if let Some(debug) = &tracer.container_debug {
            let mut map = AttrMap::new();
            if !debug.error.is_empty() {
                map.insert(DEBUG_ERROR, Value::str(debug.error.as_str()));
            }
            if debug.latency_ms != 0 {
                map.insert(DEBUG_LATENCY_MS, Value::I64(debug.latency_ms));
            }
            if debug.was_buffered {
                map.insert(DEBUG_WAS_BUFFERED, Value::Bool(true));
            }
            if debug.buffer_ms != 0 {
                map.insert(DEBUG_BUFFER_MS, Value::I64(debug.buffer_ms));
            }
            if !debug.buffer_eviction_reason.is_empty() {
                map.insert(
                    DEBUG_BUFFER_EVICTION_REASON,
                    Value::str(debug.buffer_eviction_reason.as_str()),
                );
            }
            attrs.insert(RESOURCE_ATTR_TRACER_CONTAINER_DEBUG, Value::Map(Box::new(map)));
        }
        let Some(agent) = agent else { return resource };
        for (key, value) in [
            (RESOURCE_ATTR_AGENT_HOSTNAME, &agent.host_name),
            (RESOURCE_ATTR_AGENT_ENV, &agent.env),
            (RESOURCE_ATTR_AGENT_VERSION, &agent.agent_version),
        ] {
            if !value.is_empty() {
                attrs.insert(key, Value::str(value.as_str()));
            }
        }
        if agent.target_tps != 0.0 {
            attrs.insert(RESOURCE_ATTR_AGENT_TARGET_TPS, Value::F64(agent.target_tps));
        }
        if agent.error_tps != 0.0 {
            attrs.insert(RESOURCE_ATTR_AGENT_ERROR_TPS, Value::F64(agent.error_tps));
        }
        if agent.rare_sampler_enabled {
            attrs.insert(RESOURCE_ATTR_AGENT_RARE_SAMPLER_ENABLED, Value::Bool(true));
        }
        if !agent.tags.is_empty() {
            attrs.insert(RESOURCE_ATTR_AGENT_TAGS, str_map_value(&agent.tags));
        }
        resource
    }
}

/// An `AttributeAnyValue` as a `Value`; `None` for an unknown `type`.
fn any_to_value(v: WireAny) -> Option<Value> {
    Some(match v {
        WireAny::Str(s) => Value::str(s),
        WireAny::Bool(b) => Value::Bool(b),
        WireAny::Int(i) => Value::I64(i),
        WireAny::Double(f) => Value::F64(f),
        WireAny::Array(items) => {
            Value::Array(items.into_iter().map(any_to_value).collect::<Option<Vec<_>>>()?)
        }
        WireAny::Unknown => return None,
    })
}

/// Where a non-carrier attribute lands on a span: Datadog's three typed maps.
enum Slot {
    Meta(String),
    Metric(f64),
    MetaStruct(Vec<u8>),
}

/// The chunk-level carriers read off one span.
#[derive(Default)]
struct ChunkFields {
    priority: Option<i32>,
    origin: Option<String>,
    dropped_trace: bool,
    tags: StrMap,
}

/// `i` as an `f64`, and whether that is exact.
fn i64_as_f64(i: i64) -> (f64, bool) {
    let f = i as f64;
    // `as i64` saturates, so 2^63 (the nearest double to `i64::MAX`) needs the bound check.
    (f, f < 9_223_372_036_854_775_808.0 && f as i64 == i)
}

fn u64_as_f64(u: u64) -> (f64, bool) {
    let f = u as f64;
    (f, f < 18_446_744_073_709_551_616.0 && f as u64 == u)
}

fn sort_map<V>(map: &mut [(String, V)]) {
    map.sort_by(|a, b| a.0.cmp(&b.0));
}

/// Whether `key` is a carrier [`DatadogEncoder::wire_span`] consumes rather than sends as a
/// `meta`/`metrics`/`meta_struct` entry. A carrier of the wrong `Value` type isn't one: it goes
/// out by its type like any attribute, so a Datadog span whose own `meta` happens to use a
/// carrier's name still relays. A payload carrier, or a tracer header's
/// ([`super::is_tracer_header_attr`]), counts only on the resource.
fn is_carrier(key: &str, value: &Value, event: &Event) -> bool {
    match (key, value) {
        // Empty isn't a carrier either: decode never makes one (the wire's `""` is no attribute),
        // so an empty one came from a `meta` entry and goes back there.
        (
            ATTR_SERVICE_NAME | ATTR_RESOURCE_NAME | ATTR_SPAN_TYPE | ATTR_CHUNK_ORIGIN,
            Value::Str(s),
        ) => !s.is_empty(),
        (ATTR_CHUNK_DROPPED_TRACE, Value::Bool(_)) | (ATTR_CHUNK_TAGS, Value::Map(_)) => true,
        (ATTR_SPAN_ERROR | ATTR_CHUNK_PRIORITY, Value::I64(i)) => i32::try_from(*i).is_ok(),
        (k, _) => {
            (TRACER_CARRIERS.contains(&k)
                || AGENT_CARRIERS.contains(&k)
                || super::is_tracer_header_attr(k))
                && event.attributes.get(k).is_none()
        }
    }
}

impl DatadogEncoder {
    pub(super) fn span_out_degraded(&self, reason: &'static str, n: usize) {
        if n > 0 {
            self.telemetry.count("logit.output.spans.degraded", n as f64, &[("reason", reason)]);
        }
    }

    /// A value for a Datadog string field (a `meta` value, a link attribute, a tag): `Str` as-is,
    /// a `Timestamp` as RFC 3339, a `Map`/`Array`/`Null` as JSON text (counted `json_text`), any
    /// other scalar as its JSON text (`true`, `42`, a base64 `Bytes`).
    fn text(&self, value: &Value) -> String {
        match value {
            Value::Str(_) => value.as_str().unwrap_or_default().to_string(),
            Value::Timestamp(ns) => format_rfc3339_utc(*ns),
            Value::Array(_) | Value::Map(_) | Value::Null => {
                self.span_out_degraded("json_text", 1);
                value_text(value).into_owned()
            }
            Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
            other => value_text(other).into_owned(),
        }
    }

    fn slot(&self, value: &Value) -> Slot {
        match value {
            Value::F64(f) => Slot::Metric(*f),
            Value::I64(i) => {
                let (f, exact) = i64_as_f64(*i);
                if !exact {
                    self.span_out_degraded("int_as_f64", 1);
                }
                Slot::Metric(f)
            }
            Value::U64(u) => {
                let (f, exact) = u64_as_f64(*u);
                if !exact {
                    self.span_out_degraded("int_as_f64", 1);
                }
                Slot::Metric(f)
            }
            Value::Bool(b) => Slot::Meta(if *b { "true" } else { "false" }.to_string()),
            Value::Bytes(b) => Slot::MetaStruct(b.to_vec()),
            other => Slot::Meta(self.text(other)),
        }
    }

    fn str_map(&self, value: &Value) -> StrMap {
        let Value::Map(map) = value else {
            return Vec::new();
        };
        let mut out: StrMap =
            map.iter().map(|(k, v)| (resolve(k).to_string(), self.text(v))).collect();
        sort_map(&mut out);
        out
    }

    /// Every span in `batch` as chunks, one per 128-bit trace id in first-appearance order,
    /// spans in batch order within each (the permitted "chunk regrouping by trace id"). Chunk
    /// fields come from each chunk's first span. A chunk whose spans carry no `_dd.p.tid` but a
    /// nonzero high half gets one synthesized on its first span.
    pub(super) fn batch_chunks(&self, batch: &EventBatch, form: Form) -> Vec<WireChunk> {
        let mut chunks: Vec<WireChunk> = Vec::new();
        let mut index: HashMap<[u8; 16], usize> = HashMap::new();
        let mut group_ids: Vec<[u8; 16]> = Vec::new();
        for event in &batch.events {
            let Some(span) = &event.span else { continue };
            let (wire, fields) = self.wire_span(&batch.resource, event, span, form);
            let i = *index.entry(span.trace_id).or_insert_with(|| {
                chunks.push(WireChunk {
                    priority: fields.priority.unwrap_or(PRIORITY_NONE),
                    origin: fields.origin.clone().unwrap_or_default(),
                    spans: Vec::new(),
                    tags: fields.tags.clone(),
                    dropped_trace: fields.dropped_trace,
                });
                group_ids.push(span.trace_id);
                chunks.len() - 1
            });
            chunks[i].spans.push(wire);
        }
        for (chunk, id) in chunks.iter_mut().zip(&group_ids) {
            let (high, _) = trace_id_halves(id);
            let carried =
                chunk.spans.iter().any(|s| s.meta.iter().any(|(k, _)| k == META_TRACE_ID_HIGH));
            if high != 0 && !carried {
                let first = &mut chunk.spans[0];
                first.meta.push((META_TRACE_ID_HIGH.to_string(), format!("{high:016x}")));
                sort_map(&mut first.meta);
            }
        }
        chunks
    }

    /// One span, and the chunk fields it carries. Attributes are the resource merged with the
    /// event's; the carriers are consumed, everything else lands in `meta`/`metrics`/`meta_struct`
    /// by its type.
    fn wire_span(
        &self,
        resource: &Resource,
        event: &Event,
        span: &SpanRecord,
        form: Form,
    ) -> (WireSpan, ChunkFields) {
        let (_, low) = trace_id_halves(&span.trace_id);
        let mut wire = WireSpan {
            trace_id: low,
            span_id: u64::from_be_bytes(span.span_id),
            parent_id: span.parent_span_id.map_or(0, u64::from_be_bytes),
            start: event.timestamp,
            ..WireSpan::default()
        };
        wire.duration = if span.end_timestamp < event.timestamp {
            self.span_out_degraded("negative_duration", 1);
            0
        } else {
            span.end_timestamp.saturating_sub(event.timestamp)
        };
        wire.name = match &span.name {
            Value::Str(_) => span.name.as_str().unwrap_or_default().to_string(),
            other => self.text(other),
        };
        let mut fields = ChunkFields::default();
        let mut error_attr = None;
        let mut lost = 0;
        for (sym, value) in logit_core::attrs::merged(resource, event) {
            let key = resolve(sym);
            if !is_carrier(key, value, event) {
                match self.slot(value) {
                    Slot::Meta(s) => wire.meta.push((key.to_string(), s)),
                    Slot::Metric(f) => wire.metrics.push((key.to_string(), f)),
                    Slot::MetaStruct(b) => {
                        if form == Form::V05 {
                            lost += 1;
                        } else {
                            wire.meta_struct.push((key.to_string(), b));
                        }
                    }
                }
                continue;
            }
            match (key, value) {
                (ATTR_SERVICE_NAME, _) => wire.service = self.text(value),
                (ATTR_RESOURCE_NAME, _) => wire.resource = self.text(value),
                (ATTR_SPAN_TYPE, _) => wire.r#type = self.text(value),
                (ATTR_SPAN_ERROR, Value::I64(i)) => error_attr = i32::try_from(*i).ok(),
                (
                    ATTR_CHUNK_PRIORITY
                    | ATTR_CHUNK_ORIGIN
                    | ATTR_CHUNK_DROPPED_TRACE
                    | ATTR_CHUNK_TAGS,
                    _,
                ) if !form.has_chunks() => {
                    lost += 1;
                }
                (ATTR_CHUNK_PRIORITY, Value::I64(i)) => fields.priority = i32::try_from(*i).ok(),
                (ATTR_CHUNK_ORIGIN, _) => fields.origin = Some(self.text(value)),
                (ATTR_CHUNK_DROPPED_TRACE, Value::Bool(b)) => fields.dropped_trace = *b,
                (ATTR_CHUNK_TAGS, _) => fields.tags = self.str_map(value),
                // A batch-level carrier: `resource_carriers_lost` counts it once per batch.
                _ => {}
            }
        }
        wire.error = error_attr.unwrap_or(if span.status == SpanStatus::Error { 1 } else { 0 });
        if span.kind != SpanKind::Internal && merged_get(resource, event, ATTR_SPAN_KIND).is_none()
        {
            wire.meta.push((ATTR_SPAN_KIND.to_string(), span.kind.as_str().to_string()));
        }
        sort_map(&mut wire.meta);
        sort_map(&mut wire.metrics);
        sort_map(&mut wire.meta_struct);

        if span.status == SpanStatus::Ok {
            lost += 1;
        }
        if span.flags != 0 {
            lost += 1;
        }
        if let Some(ext) = &span.ext {
            lost += usize::from(ext.status_message.is_some())
                + usize::from(ext.trace_state.is_some())
                + usize::from(ext.dropped_attributes_count != 0)
                + usize::from(ext.dropped_events_count != 0)
                + usize::from(ext.dropped_links_count != 0);
        }
        if form == Form::V05 {
            lost += span.links.len() + span.events.len();
        } else {
            for link in &span.links {
                let (link_high, link_low) = trace_id_halves(&link.trace_id);
                let mut attributes: StrMap = link
                    .attributes
                    .iter()
                    .map(|(k, v)| (resolve(k).to_string(), self.text(v)))
                    .collect();
                sort_map(&mut attributes);
                lost += usize::from(link.dropped_attributes_count != 0);
                wire.span_links.push(WireLink {
                    trace_id: link_low,
                    trace_id_high: link_high,
                    span_id: u64::from_be_bytes(link.span_id),
                    attributes,
                    tracestate: link
                        .trace_state
                        .as_ref()
                        .map(|t| String::from_utf8_lossy(t).into_owned())
                        .unwrap_or_default(),
                    flags: link.flags,
                });
            }
            for e in &span.events {
                lost += usize::from(e.dropped_attributes_count != 0);
                let time_unix_nano = u64::try_from(e.timestamp).unwrap_or_else(|_| {
                    self.span_out_degraded("timestamp_range", 1);
                    0
                });
                let name = match &e.name {
                    Value::Str(_) => e.name.as_str().unwrap_or_default().to_string(),
                    other => self.text(other),
                };
                let mut attributes: Vec<(String, WireAny)> = e
                    .attributes
                    .iter()
                    .map(|(k, v)| (resolve(k).to_string(), self.any(v, true)))
                    .collect();
                sort_map(&mut attributes);
                wire.span_events.push(WireSpanEvent { time_unix_nano, name, attributes });
            }
        }
        self.span_out_degraded("no_wire_form", lost);
        (wire, fields)
    }

    /// A span event attribute as an `AttributeAnyValue`: scalars natively, an array of scalars
    /// as `ARRAY_VALUE` (only at the top level: `AttributeArrayValue` is scalar-only), anything
    /// else as a string, JSON text for a `Map`/`Array`/`Null`.
    fn any(&self, value: &Value, top: bool) -> WireAny {
        match value {
            Value::Str(_) => WireAny::Str(value.as_str().unwrap_or_default().to_string()),
            Value::Bool(b) => WireAny::Bool(*b),
            Value::I64(i) => WireAny::Int(*i),
            Value::U64(u) => match i64::try_from(*u) {
                Ok(i) => WireAny::Int(i),
                Err(_) => {
                    self.span_out_degraded("int_as_f64", 1);
                    WireAny::Double(*u as f64)
                }
            },
            Value::F64(f) => WireAny::Double(*f),
            Value::Array(items)
                if top
                    && items.iter().all(|v| {
                        matches!(
                            v,
                            Value::Str(_)
                                | Value::Bool(_)
                                | Value::I64(_)
                                | Value::U64(_)
                                | Value::F64(_)
                        )
                    }) =>
            {
                WireAny::Array(items.iter().map(|v| self.any(v, false)).collect())
            }
            other => WireAny::Str(self.text(other)),
        }
    }

    /// Counts, once per batch, every batch-resource carrier `form` has no field for: the
    /// `datadog.agent.*` ones below `AgentPayload`, the `datadog.tracer.*` ones below v0.7, and the
    /// tracer headers' carriers no payload has a field for. With `headers`, the caller sends every
    /// tracer header as a request header, so no header's carrier is lost.
    pub(super) fn resource_carriers_lost(&self, resource: &Resource, form: Form, headers: bool) {
        let attrs = &resource.attributes;
        let lost_here =
            |key: &str| attrs.get(key).is_some() && !(headers && super::is_tracer_header_attr(key));
        let mut lost = 0;
        if form != Form::Agent {
            lost += AGENT_CARRIERS.iter().filter(|k| attrs.get(k).is_some()).count();
        }
        if !form.has_chunks() {
            lost += TRACER_CARRIERS.iter().filter(|k| lost_here(k)).count();
        }
        let header_only = super::TRACER_STR_HEADERS
            .iter()
            .chain(&super::TRACER_FLAG_HEADERS)
            .chain(&super::TRACER_U64_HEADERS)
            .map(|&(_, attr)| attr)
            .filter(|attr| !TRACER_CARRIERS.contains(attr));
        lost += header_only.filter(|attr| lost_here(attr)).count();
        self.span_out_degraded("no_wire_form", lost);
    }

    /// The batch resource's `datadog.tracer.*` carriers as a `TracerPayload` around `chunks`.
    pub(super) fn wire_tracer(&self, resource: &Resource, chunks: Vec<WireChunk>) -> WireTracer {
        let attrs = &resource.attributes;
        let text = |key: &str| attrs.get(key).map(|v| self.text(v)).unwrap_or_default();
        let container_debug = attrs.get(RESOURCE_ATTR_TRACER_CONTAINER_DEBUG).map(|v| {
            let Value::Map(map) = v else {
                return WireContainerDebug::default();
            };
            let int = |key: &str| match map.get(key) {
                Some(Value::I64(i)) => *i,
                _ => 0,
            };
            WireContainerDebug {
                error: map.get(DEBUG_ERROR).map(|v| self.text(v)).unwrap_or_default(),
                latency_ms: int(DEBUG_LATENCY_MS),
                was_buffered: matches!(map.get(DEBUG_WAS_BUFFERED), Some(Value::Bool(true))),
                buffer_ms: int(DEBUG_BUFFER_MS),
                buffer_eviction_reason: map
                    .get(DEBUG_BUFFER_EVICTION_REASON)
                    .map(|v| self.text(v))
                    .unwrap_or_default(),
            }
        });
        WireTracer {
            container_id: text(RESOURCE_ATTR_TRACER_CONTAINER_ID),
            language_name: text(RESOURCE_ATTR_TRACER_LANGUAGE_NAME),
            language_version: text(RESOURCE_ATTR_TRACER_LANGUAGE_VERSION),
            tracer_version: text(RESOURCE_ATTR_TRACER_VERSION),
            runtime_id: text(RESOURCE_ATTR_TRACER_RUNTIME_ID),
            chunks,
            tags: attrs.get(RESOURCE_ATTR_TRACER_TAGS).map(|v| self.str_map(v)).unwrap_or_default(),
            env: text(RESOURCE_ATTR_TRACER_ENV),
            hostname: text(RESOURCE_ATTR_TRACER_HOSTNAME),
            app_version: text(RESOURCE_ATTR_TRACER_APP_VERSION),
            container_debug,
        }
    }

    /// The batch resource's `datadog.agent.*` carriers as an `AgentPayload` around one tracer
    /// payload.
    pub(super) fn wire_agent(&self, resource: &Resource, tracer: WireTracer) -> WireAgent {
        let attrs = &resource.attributes;
        let text = |key: &str| attrs.get(key).map(|v| self.text(v)).unwrap_or_default();
        let float = |key: &str| match attrs.get(key) {
            Some(Value::F64(f)) => *f,
            _ => 0.0,
        };
        WireAgent {
            host_name: text(RESOURCE_ATTR_AGENT_HOSTNAME),
            env: text(RESOURCE_ATTR_AGENT_ENV),
            tracer_payloads: vec![tracer],
            tags: attrs.get(RESOURCE_ATTR_AGENT_TAGS).map(|v| self.str_map(v)).unwrap_or_default(),
            agent_version: text(RESOURCE_ATTR_AGENT_VERSION),
            target_tps: float(RESOURCE_ATTR_AGENT_TARGET_TPS),
            error_tps: float(RESOURCE_ATTR_AGENT_ERROR_TPS),
            rare_sampler_enabled: matches!(
                attrs.get(RESOURCE_ATTR_AGENT_RARE_SAMPLER_ENABLED),
                Some(Value::Bool(true))
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datadog::logs::tests::counted;
    use logit_core::{Registry, SpanExt};
    use std::sync::Arc;

    fn encoder() -> (DatadogEncoder, Arc<Registry>) {
        let registry = Registry::new();
        let t = registry.telemetry_for("dd", "datadog", "sink");
        (DatadogEncoder::new().with_telemetry(t), registry)
    }

    fn otel_span(trace_id: [u8; 16]) -> SpanRecord {
        SpanRecord {
            trace_id,
            span_id: [0, 0, 0, 0, 0, 0, 0, 9],
            parent_span_id: None,
            name: Value::str("GET /users"),
            kind: SpanKind::Server,
            status: SpanStatus::Unset,
            events: Vec::new(),
            links: Vec::new(),
            end_timestamp: 1_500,
            flags: 0,
            ext: None,
        }
    }

    fn batch(resource: Resource, events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(resource), scope: None, events }
    }

    fn only_chunk(e: &DatadogEncoder, b: &EventBatch, form: Form) -> WireChunk {
        let mut chunks = e.batch_chunks(b, form);
        assert_eq!(chunks.len(), 1);
        chunks.remove(0)
    }

    #[test]
    fn tid_parsing_follows_go_parse_uint_base_16() {
        assert_eq!(parse_trace_id_high("64f5a1b200000000"), Some(0x64f5_a1b2_0000_0000));
        assert_eq!(parse_trace_id_high("ABC"), Some(0xabc));
        assert_eq!(parse_trace_id_high(""), None);
        assert_eq!(parse_trace_id_high("0x12"), None);
        assert_eq!(parse_trace_id_high("11112222333344445"), None, "17 digits");
    }

    #[test]
    fn attributes_land_in_meta_metrics_and_meta_struct_by_type() {
        let (e, registry) = encoder();
        let mut attrs = AttrMap::new();
        attrs.insert("s", Value::str("x"));
        attrs.insert("f", Value::F64(0.5));
        attrs.insert("i", Value::I64(3));
        attrs.insert("big", Value::I64(i64::MAX));
        attrs.insert("u", Value::U64(u64::MAX));
        attrs.insert("yes", Value::Bool(true));
        attrs.insert("raw", Value::Bytes(Bytes::from_static(b"\x01\x02")));
        attrs.insert("at", Value::Timestamp(0));
        attrs.insert("list", Value::Array(vec![Value::I64(1), Value::str("a")]));
        let mut resource = Resource::default();
        resource.attributes.insert(ATTR_SERVICE_NAME, Value::str("checkout"));
        resource.attributes.insert(RESOURCE_ATTR_TRACER_ENV, Value::str("prod"));
        let b = batch(resource, vec![Event::span(1_000, attrs, otel_span([0; 16]))]);
        let s = &only_chunk(&e, &b, Form::V07).spans[0];
        let meta: HashMap<_, _> = s.meta.iter().cloned().collect();
        assert_eq!(meta["s"], "x");
        assert_eq!(meta["yes"], "true");
        assert_eq!(meta["at"], "1970-01-01T00:00:00.000000000Z");
        assert_eq!(meta["list"], r#"[1,"a"]"#);
        assert_eq!(meta["span.kind"], "server", "synthesized from a non-internal kind");
        assert!(!meta.contains_key(RESOURCE_ATTR_TRACER_ENV), "a payload carrier, not meta");
        let metrics: HashMap<_, _> = s.metrics.iter().cloned().collect();
        assert_eq!(metrics["f"], 0.5);
        assert_eq!(metrics["i"], 3.0);
        assert_eq!(s.meta_struct, vec![("raw".to_string(), vec![1, 2])]);
        assert_eq!(s.service, "checkout", "from the resource's `service.name`");
        assert_eq!(s.duration, 500);
        let keys: Vec<_> = s.meta.iter().map(|(k, _)| k.as_str()).collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "maps are key-sorted");
        let deg = |r| counted(&registry, "logit.output.spans.degraded", ("reason", r));
        assert_eq!(deg("int_as_f64"), 2.0, "i64::MAX and u64::MAX");
    }

    #[test]
    fn json_text_is_counted() {
        let (e, registry) = encoder();
        let mut attrs = AttrMap::new();
        attrs.insert("m", Value::Map(Box::new(AttrMap::new())));
        let b = batch(Resource::default(), vec![Event::span(0, attrs, otel_span([0; 16]))]);
        let s = &only_chunk(&e, &b, Form::V04).spans[0];
        assert!(s.meta.contains(&("m".to_string(), "{}".to_string())));
        assert_eq!(counted(&registry, "logit.output.spans.degraded", ("reason", "json_text")), 1.0);
    }

    #[test]
    fn an_otel_128_bit_id_gets_a_synthesized_tid_on_its_first_span_only() {
        let (e, _) = encoder();
        let mut id = [0u8; 16];
        id[0] = 0xab;
        id[15] = 1;
        let events = vec![
            Event::span(0, AttrMap::new(), otel_span(id)),
            Event::span(0, AttrMap::new(), otel_span(id)),
        ];
        let c = only_chunk(&e, &batch(Resource::default(), events), Form::V04);
        let tid = |s: &WireSpan| {
            s.meta.iter().find(|(k, _)| k == META_TRACE_ID_HIGH).map(|(_, v)| v.clone())
        };
        assert_eq!(tid(&c.spans[0]).as_deref(), Some("ab00000000000000"));
        assert_eq!(tid(&c.spans[1]), None);
        assert_eq!(c.spans[0].trace_id, 1);
        assert_eq!(c.priority, PRIORITY_NONE);
    }

    #[test]
    fn spans_regroup_by_trace_id_in_first_appearance_order() {
        let (e, _) = encoder();
        let id = |n| {
            let mut id = [0u8; 16];
            id[15] = n;
            id
        };
        let events = [2, 1, 2].map(|n| Event::span(0, AttrMap::new(), otel_span(id(n))));
        let chunks = e.batch_chunks(&batch(Resource::default(), events.to_vec()), Form::V07);
        let ids: Vec<Vec<u64>> =
            chunks.iter().map(|c| c.spans.iter().map(|s| s.trace_id).collect()).collect();
        assert_eq!(ids, vec![vec![2, 2], vec![1]]);
    }

    #[test]
    fn model_only_fields_are_counted_no_wire_form() {
        let (e, registry) = encoder();
        let mut span = otel_span([0; 16]);
        span.status = SpanStatus::Ok;
        span.flags = 1;
        span.ext = Some(Box::new(SpanExt {
            status_message: Some(Bytes::from_static(b"fine")),
            trace_state: Some(Bytes::from_static(b"a=b")),
            dropped_attributes_count: 1,
            dropped_events_count: 0,
            dropped_links_count: 0,
        }));
        let b = batch(Resource::default(), vec![Event::span(0, AttrMap::new(), span)]);
        let s = &only_chunk(&e, &b, Form::V07).spans[0];
        assert_eq!(s.error, 0, "`Ok` has no Datadog spelling");
        let lost = counted(&registry, "logit.output.spans.degraded", ("reason", "no_wire_form"));
        assert_eq!(lost, 5.0, "status Ok, flags, message, trace_state, dropped count");
    }

    #[test]
    fn links_and_events_map_both_ways() {
        let (e, _) = encoder();
        let mut span = otel_span([0; 16]);
        let mut link_id = [0u8; 16];
        link_id[7] = 2;
        link_id[15] = 1;
        let mut link_attrs = AttrMap::new();
        link_attrs.insert("n", Value::I64(4));
        span.links.push(SpanLink {
            trace_id: link_id,
            span_id: [0, 0, 0, 0, 0, 0, 0, 3],
            attributes: link_attrs,
            flags: 0x8000_0001,
            trace_state: Some(Bytes::from_static(b"dd=s:1")),
            dropped_attributes_count: 0,
        });
        let mut ev_attrs = AttrMap::new();
        ev_attrs.insert("big", Value::U64(u64::MAX));
        ev_attrs.insert("nested", Value::Array(vec![Value::Array(vec![])]));
        ev_attrs.insert("ok", Value::Array(vec![Value::Bool(true), Value::F64(1.5)]));
        span.events.push(SpanEvent {
            timestamp: 7,
            name: Value::str("boom"),
            attributes: ev_attrs,
            dropped_attributes_count: 0,
        });
        let b = batch(Resource::default(), vec![Event::span(0, AttrMap::new(), span)]);
        let s = &only_chunk(&e, &b, Form::Agent).spans[0];
        let l = &s.span_links[0];
        assert_eq!((l.trace_id, l.trace_id_high, l.span_id), (1, 2, 3));
        assert_eq!(l.attributes, vec![("n".to_string(), "4".to_string())]);
        assert_eq!((l.tracestate.as_str(), l.flags), ("dd=s:1", 0x8000_0001));
        let ev = &s.span_events[0];
        assert_eq!((ev.time_unix_nano, ev.name.as_str()), (7, "boom"));
        let attrs: HashMap<_, _> = ev.attributes.iter().cloned().collect();
        assert_eq!(attrs["big"], WireAny::Double(u64::MAX as f64));
        assert_eq!(attrs["nested"], WireAny::Str("[[]]".into()));
        assert_eq!(attrs["ok"], WireAny::Array(vec![WireAny::Bool(true), WireAny::Double(1.5)]));
    }

    #[test]
    fn an_unknown_attribute_type_is_dropped_and_counted() {
        let registry = Registry::new();
        let t = registry.telemetry_for("dd", "datadog", "listener");
        let mut d = DatadogDecoder::new().with_telemetry(t);
        let span = WireSpan {
            span_events: vec![WireSpanEvent {
                time_unix_nano: u64::MAX,
                name: "e".into(),
                attributes: vec![("x".into(), WireAny::Unknown)],
            }],
            ..WireSpan::default()
        };
        let mut out = Vec::new();
        d.chunk_events(WireChunk { spans: vec![span], ..WireChunk::default() }, false, &mut out);
        let ev = &out[0].span.as_ref().unwrap().events[0];
        assert!(ev.attributes.is_empty());
        assert_eq!(ev.timestamp, i64::MAX);
        let deg = |r| counted(&registry, "logit.input.spans.degraded", ("reason", r));
        assert_eq!(deg("bad_attribute_type"), 1.0);
    }

    #[test]
    fn chunk_carriers_decode_only_where_the_wire_has_a_chunk() {
        let mut d = DatadogDecoder::new();
        let chunk = WireChunk {
            priority: 1,
            origin: "rum".into(),
            spans: vec![WireSpan::default(), WireSpan::default()],
            tags: vec![("_dd.p.dm".into(), "-4".into())],
            dropped_trace: true,
        };
        let mut out = Vec::new();
        d.chunk_events(chunk.clone(), true, &mut out);
        for e in &out {
            assert_eq!(e.attributes.get(ATTR_CHUNK_PRIORITY), Some(&Value::I64(1)));
            assert_eq!(e.attributes.get(ATTR_CHUNK_ORIGIN), Some(&Value::str("rum")));
            assert_eq!(e.attributes.get(ATTR_CHUNK_DROPPED_TRACE), Some(&Value::Bool(true)));
        }
        let mut v04 = Vec::new();
        d.chunk_events(chunk.clone(), false, &mut v04);
        assert!(v04[0].attributes.is_empty(), "v0.4/v0.5 have no chunk");
        let mut none = Vec::new();
        d.chunk_events(WireChunk { priority: PRIORITY_NONE, ..chunk }, true, &mut none);
        assert_eq!(none[0].attributes.get(ATTR_CHUNK_PRIORITY), None, "-128 is no priority");
    }
}
