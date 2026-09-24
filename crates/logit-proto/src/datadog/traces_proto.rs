//! The intake's `/api/v0.2/traces` body: a protobuf `AgentPayload` of `TracerPayload`s. The span
//! mapping is [`super::traces`]'s; the tables are in [`super`]'s module doc, under "Traces".
//!
//! Decoding goes through prost's generated types. Encoding is hand-written protobuf instead: those
//! types hold every map as a `HashMap`, whose iteration order is random per instance, so encoding
//! through them would write one batch as different bytes on every call. The writer here emits
//! fields in tag order, skips proto3 defaults exactly as prost does, and sorts every map by key.

use super::generated::trace::{
    AgentPayload, AttributeAnyValue, ContainerDebug, Span, SpanEvent, SpanLink, TraceChunk,
    TracerPayload,
};
use super::logs::new_batch;
use super::traces::{
    Form, StrMap, WireAgent, WireAny, WireChunk, WireContainerDebug, WireLink, WireSpan,
    WireSpanEvent, WireTracer, ANY_ARRAY,
};
use super::{DatadogDecoder, DatadogEncoder};
use crate::CodecError;
use bytes::Bytes;
use logit_core::EventBatch;
use prost::Message;
use std::collections::HashMap;

fn str_map(map: HashMap<String, String>) -> StrMap {
    let mut out: StrMap = map.into_iter().collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn sorted<V>(map: HashMap<String, V>) -> Vec<(String, V)> {
    let mut out: Vec<_> = map.into_iter().collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn any(v: AttributeAnyValue) -> WireAny {
    let array = v.array_value.map(|a| {
        a.values
            .into_iter()
            .map(|x| {
                WireAny::from_parts(
                    x.r#type.into(),
                    x.string_value,
                    x.bool_value,
                    x.int_value,
                    x.double_value,
                    None,
                )
            })
            .collect()
    });
    let array = match (i64::from(v.r#type), array) {
        (ANY_ARRAY, None) => Some(Vec::new()),
        (_, array) => array,
    };
    WireAny::from_parts(
        v.r#type.into(),
        v.string_value,
        v.bool_value,
        v.int_value,
        v.double_value,
        array,
    )
}

fn span(s: Span) -> WireSpan {
    WireSpan {
        service: s.service,
        name: s.name,
        resource: s.resource,
        trace_id: s.trace_id,
        span_id: s.span_id,
        parent_id: s.parent_id,
        start: s.start,
        duration: s.duration,
        error: s.error,
        meta: str_map(s.meta),
        metrics: sorted(s.metrics),
        r#type: s.r#type,
        meta_struct: sorted(s.meta_struct),
        span_links: s.span_links.into_iter().map(link).collect(),
        span_events: s.span_events.into_iter().map(event).collect(),
    }
}

fn link(l: SpanLink) -> WireLink {
    WireLink {
        trace_id: l.trace_id,
        trace_id_high: l.trace_id_high,
        span_id: l.span_id,
        attributes: str_map(l.attributes),
        tracestate: l.tracestate,
        flags: l.flags,
    }
}

fn event(e: SpanEvent) -> WireSpanEvent {
    WireSpanEvent {
        time_unix_nano: e.time_unix_nano,
        name: e.name,
        attributes: sorted(e.attributes).into_iter().map(|(k, v)| (k, any(v))).collect(),
    }
}

fn chunk(c: TraceChunk) -> WireChunk {
    WireChunk {
        priority: c.priority,
        origin: c.origin,
        spans: c.spans.into_iter().map(span).collect(),
        tags: str_map(c.tags),
        dropped_trace: c.dropped_trace,
    }
}

fn tracer(t: TracerPayload) -> WireTracer {
    WireTracer {
        container_id: t.container_id,
        language_name: t.language_name,
        language_version: t.language_version,
        tracer_version: t.tracer_version,
        runtime_id: t.runtime_id,
        chunks: t.chunks.into_iter().map(chunk).collect(),
        tags: str_map(t.tags),
        env: t.env,
        hostname: t.hostname,
        app_version: t.app_version,
        container_debug: t.container_debug.map(|d: ContainerDebug| WireContainerDebug {
            error: d.error,
            latency_ms: d.latency_ms,
            was_buffered: d.was_buffered,
            buffer_ms: d.buffer_ms,
            buffer_eviction_reason: d.buffer_eviction_reason,
        }),
    }
}

impl DatadogDecoder {
    /// `/api/v0.2/traces`: one batch per `TracerPayload`, each resource carrying that payload's
    /// `datadog.tracer.*` and the envelope's `datadog.agent.*`. An `AgentPayload` with no tracer
    /// payloads decodes to no batches. `idxTracerPayloads` (the v1.0 string-table form, not
    /// implemented) are skipped and counted `idx_payload`, one per payload. `received_at` is
    /// unused: a span's `start` is its timestamp.
    pub fn decode_agent_payload(
        &mut self,
        body: &[u8],
        received_at: i64,
    ) -> Result<Vec<EventBatch>, CodecError> {
        let _ = received_at;
        let payload = AgentPayload::decode(body).map_err(|e| {
            CodecError::Malformed(format!("datadog AgentPayload does not decode: {e}"))
        })?;
        self.span_skipped("idx_payload", payload.idx_tracer_payloads.len());
        let mut agent = WireAgent {
            host_name: payload.host_name,
            env: payload.env,
            tracer_payloads: Vec::new(),
            tags: str_map(payload.tags),
            agent_version: payload.agent_version,
            target_tps: payload.target_tps,
            error_tps: payload.error_tps,
            rare_sampler_enabled: payload.rare_sampler_enabled,
        };
        agent.tracer_payloads = payload.tracer_payloads.into_iter().map(tracer).collect();
        let mut batches = Vec::with_capacity(agent.tracer_payloads.len());
        for mut t in std::mem::take(&mut agent.tracer_payloads) {
            let resource = self.tracer_resource(&t, Some(&agent));
            let mut events = Vec::new();
            for c in std::mem::take(&mut t.chunks) {
                self.chunk_events(c, true, &mut events);
            }
            batches.push(new_batch(resource, events));
        }
        Ok(batches)
    }
}

/// A minimal protobuf writer: proto3 scalars skipped at their default, as prost encodes them.
#[derive(Default)]
struct Pb(Vec<u8>);

const VARINT: u32 = 0;
const FIXED64: u32 = 1;
const LEN: u32 = 2;

impl Pb {
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.0.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.0.push(v as u8);
    }

    fn key(&mut self, field: u32, wire_type: u32) {
        self.varint(u64::from(field << 3 | wire_type));
    }

    fn uint(&mut self, field: u32, v: u64) {
        if v != 0 {
            self.key(field, VARINT);
            self.varint(v);
        }
    }

    /// `int32`/`int64`/an enum: a negative value is its 64-bit two's complement, 10 bytes.
    fn int(&mut self, field: u32, v: i64) {
        self.uint(field, v as u64);
    }

    fn boolean(&mut self, field: u32, v: bool) {
        self.uint(field, u64::from(v));
    }

    fn double(&mut self, field: u32, v: f64) {
        if v != 0.0 {
            self.key(field, FIXED64);
            self.0.extend_from_slice(&v.to_le_bytes());
        }
    }

    fn fixed64(&mut self, field: u32, v: u64) {
        if v != 0 {
            self.key(field, FIXED64);
            self.0.extend_from_slice(&v.to_le_bytes());
        }
    }

    fn bytes(&mut self, field: u32, v: &[u8]) {
        if !v.is_empty() {
            self.len_delimited(field, v);
        }
    }

    fn string(&mut self, field: u32, v: &str) {
        self.bytes(field, v.as_bytes());
    }

    /// An embedded message (or map entry), written even when empty.
    fn message(&mut self, field: u32, build: impl FnOnce(&mut Pb)) {
        let mut inner = Pb::default();
        build(&mut inner);
        self.len_delimited(field, &inner.0);
    }

    fn len_delimited(&mut self, field: u32, v: &[u8]) {
        self.key(field, LEN);
        self.varint(v.len() as u64);
        self.0.extend_from_slice(v);
    }

    fn str_map(&mut self, field: u32, map: &StrMap) {
        for (k, v) in map {
            self.message(field, |e| {
                e.string(1, k);
                e.string(2, v);
            });
        }
    }
}

fn pb_any(p: &mut Pb, v: &WireAny) {
    p.int(1, v.kind());
    match v {
        WireAny::Str(s) => p.string(2, s),
        WireAny::Unknown => {}
        WireAny::Bool(b) => p.boolean(3, *b),
        WireAny::Int(i) => p.int(4, *i),
        WireAny::Double(f) => p.double(5, *f),
        WireAny::Array(items) => p.message(6, |a| {
            for item in items {
                a.message(1, |x| pb_any(x, item));
            }
        }),
    }
}

fn pb_span(p: &mut Pb, s: &WireSpan) {
    p.string(1, &s.service);
    p.string(2, &s.name);
    p.string(3, &s.resource);
    p.uint(4, s.trace_id);
    p.uint(5, s.span_id);
    p.uint(6, s.parent_id);
    p.int(7, s.start);
    p.int(8, s.duration);
    p.int(9, s.error.into());
    p.str_map(10, &s.meta);
    for (k, v) in &s.metrics {
        p.message(11, |e| {
            e.string(1, k);
            e.double(2, *v);
        });
    }
    p.string(12, &s.r#type);
    for (k, v) in &s.meta_struct {
        p.message(13, |e| {
            e.string(1, k);
            e.bytes(2, v);
        });
    }
    for l in &s.span_links {
        p.message(14, |m| {
            m.uint(1, l.trace_id);
            m.uint(2, l.trace_id_high);
            m.uint(3, l.span_id);
            m.str_map(4, &l.attributes);
            m.string(5, &l.tracestate);
            m.uint(6, l.flags.into());
        });
    }
    for e in &s.span_events {
        p.message(15, |m| {
            m.fixed64(1, e.time_unix_nano);
            m.string(2, &e.name);
            for (k, v) in &e.attributes {
                m.message(3, |entry| {
                    entry.string(1, k);
                    entry.message(2, |x| pb_any(x, v));
                });
            }
        });
    }
}

fn pb_tracer(p: &mut Pb, t: &WireTracer) {
    p.string(1, &t.container_id);
    p.string(2, &t.language_name);
    p.string(3, &t.language_version);
    p.string(4, &t.tracer_version);
    p.string(5, &t.runtime_id);
    for c in &t.chunks {
        p.message(6, |m| {
            m.int(1, c.priority.into());
            m.string(2, &c.origin);
            for s in &c.spans {
                m.message(3, |x| pb_span(x, s));
            }
            m.str_map(4, &c.tags);
            m.boolean(5, c.dropped_trace);
        });
    }
    p.str_map(7, &t.tags);
    p.string(8, &t.env);
    p.string(9, &t.hostname);
    p.string(10, &t.app_version);
    if let Some(d) = &t.container_debug {
        p.message(11, |m| {
            m.string(1, &d.error);
            m.int(2, d.latency_ms);
            m.boolean(3, d.was_buffered);
            m.int(4, d.buffer_ms);
            m.string(5, &d.buffer_eviction_reason);
        });
    }
}

fn pb_agent(a: &WireAgent) -> Vec<u8> {
    let mut p = Pb::default();
    p.string(1, &a.host_name);
    p.string(2, &a.env);
    for t in &a.tracer_payloads {
        p.message(5, |m| pb_tracer(m, t));
    }
    p.str_map(6, &a.tags);
    p.string(7, &a.agent_version);
    p.double(8, a.target_tps);
    p.double(9, a.error_tps);
    p.boolean(10, a.rare_sampler_enabled);
    p.0
}

impl DatadogEncoder {
    /// `/api/v0.2/traces`: one `AgentPayload` with one `TracerPayload`, both from the batch
    /// resource, one chunk per trace id. `None` when the batch has no span.
    pub fn encode_agent_payload(&mut self, batch: &EventBatch) -> Option<Bytes> {
        let chunks = self.batch_chunks(batch, Form::Agent);
        if chunks.is_empty() {
            return None;
        }
        self.resource_carriers_lost(&batch.resource, Form::Agent);
        let tracer = self.wire_tracer(&batch.resource, chunks);
        let agent = self.wire_agent(&batch.resource, tracer);
        Some(Bytes::from(pb_agent(&agent)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datadog::generated::trace::{AttributeArray, AttributeArrayValue};
    use crate::datadog::traces::{
        ATTR_CHUNK_PRIORITY, RESOURCE_ATTR_AGENT_RARE_SAMPLER_ENABLED,
        RESOURCE_ATTR_AGENT_TARGET_TPS,
    };
    use crate::datadog::{RESOURCE_ATTR_AGENT_HOSTNAME, RESOURCE_ATTR_TRACER_ENV};
    use logit_core::{Registry, Value};

    fn full_span() -> Span {
        Span {
            service: "web".into(),
            name: "http.request".into(),
            resource: "GET /".into(),
            trace_id: 0xdead_beef,
            span_id: 7,
            parent_id: 3,
            start: 1_700_000_000_000_000_000,
            duration: -1, // clamped, not rejected
            error: 1,
            meta: HashMap::from([
                ("env".to_string(), "prod".to_string()),
                ("_dd.p.tid".to_string(), "00000000000000ff".to_string()),
            ]),
            metrics: HashMap::from([("_top_level".to_string(), 1.0)]),
            r#type: "web".into(),
            meta_struct: HashMap::from([("appsec".to_string(), vec![0x81, 0xa1, 0x61, 0x01])]),
            span_links: vec![SpanLink {
                trace_id: 1,
                trace_id_high: 2,
                span_id: 3,
                attributes: HashMap::from([("link.kind".to_string(), "follows".to_string())]),
                tracestate: "dd=s:1".into(),
                flags: 0x8000_0001,
            }],
            span_events: vec![SpanEvent {
                time_unix_nano: 1_700_000_000_000_000_001,
                name: "exception".into(),
                attributes: HashMap::from([
                    (
                        "s".to_string(),
                        AttributeAnyValue {
                            r#type: 0,
                            string_value: "x".into(),
                            ..Default::default()
                        },
                    ),
                    (
                        "b".to_string(),
                        AttributeAnyValue { r#type: 1, bool_value: true, ..Default::default() },
                    ),
                    (
                        "i".to_string(),
                        AttributeAnyValue { r#type: 2, int_value: -4, ..Default::default() },
                    ),
                    (
                        "d".to_string(),
                        AttributeAnyValue { r#type: 3, double_value: 0.25, ..Default::default() },
                    ),
                    (
                        "a".to_string(),
                        AttributeAnyValue {
                            r#type: 4,
                            array_value: Some(AttributeArray {
                                values: vec![
                                    AttributeArrayValue {
                                        r#type: 2,
                                        int_value: 1,
                                        ..Default::default()
                                    },
                                    AttributeArrayValue {
                                        r#type: 0,
                                        string_value: "y".into(),
                                        ..Default::default()
                                    },
                                ],
                            }),
                            ..Default::default()
                        },
                    ),
                ]),
            }],
        }
    }

    fn payload() -> AgentPayload {
        AgentPayload {
            host_name: "agent-host".into(),
            env: "prod".into(),
            tracer_payloads: vec![
                TracerPayload {
                    env: "staging".into(),
                    chunks: vec![TraceChunk {
                        priority: 2,
                        origin: "".into(),
                        spans: vec![full_span()],
                        tags: HashMap::new(),
                        dropped_trace: false,
                    }],
                    ..Default::default()
                },
                TracerPayload { language_name: "go".into(), ..Default::default() },
            ],
            tags: HashMap::new(),
            agent_version: "7.83.3".into(),
            target_tps: 10.0,
            error_tps: 0.0,
            rare_sampler_enabled: true,
            idx_tracer_payloads: vec![Default::default()],
        }
    }

    #[test]
    fn an_agent_payload_decodes_one_batch_per_tracer_payload() {
        let registry = Registry::new();
        let t = registry.telemetry_for("dd", "datadog", "listener");
        let mut d = DatadogDecoder::new().with_telemetry(t);
        let batches = d.decode_agent_payload(&payload().encode_to_vec(), 0).unwrap();
        assert_eq!(batches.len(), 2);
        for b in &batches {
            let r = &b.resource.attributes;
            assert_eq!(r.get(RESOURCE_ATTR_AGENT_HOSTNAME), Some(&Value::str("agent-host")));
            assert_eq!(r.get(RESOURCE_ATTR_AGENT_TARGET_TPS), Some(&Value::F64(10.0)));
            assert_eq!(r.get(RESOURCE_ATTR_AGENT_RARE_SAMPLER_ENABLED), Some(&Value::Bool(true)));
        }
        assert_eq!(
            batches[0].resource.attributes.get(RESOURCE_ATTR_TRACER_ENV),
            Some(&Value::str("staging"))
        );
        assert!(batches[1].events.is_empty());
        let e = &batches[0].events[0];
        assert_eq!(e.attributes.get(ATTR_CHUNK_PRIORITY), Some(&Value::I64(2)));
        let s = e.span.as_ref().unwrap();
        assert_eq!(s.trace_id[7], 0xff, "`_dd.p.tid` is the high half");
        assert_eq!(s.parent_span_id, Some(3u64.to_be_bytes()));
        assert_eq!(s.end_timestamp, e.timestamp);
        assert_eq!(s.links[0].trace_id[7], 2);
        assert_eq!(s.links[0].trace_state.as_deref(), Some(&b"dd=s:1"[..]));
        let attrs = &s.events[0].attributes;
        assert_eq!(attrs.get("s"), Some(&Value::str("x")));
        assert_eq!(attrs.get("b"), Some(&Value::Bool(true)));
        assert_eq!(attrs.get("i"), Some(&Value::I64(-4)));
        assert_eq!(attrs.get("d"), Some(&Value::F64(0.25)));
        assert_eq!(attrs.get("a"), Some(&Value::Array(vec![Value::I64(1), Value::str("y")])));
        assert!(matches!(e.attributes.get("appsec"), Some(Value::Bytes(_))));
        let reasons: Vec<String> = registry
            .drain(0)
            .iter()
            .filter_map(|e| e.attributes.get("reason").and_then(Value::as_str).map(String::from))
            .collect();
        assert!(reasons.contains(&"idx_payload".to_string()));
        assert!(reasons.contains(&"negative_duration".to_string()));
    }

    #[test]
    fn the_hand_written_encoding_is_what_prost_reads_back() {
        let mut d = DatadogDecoder::new();
        let batch = d.decode_agent_payload(&payload().encode_to_vec(), 0).unwrap().remove(0);
        let bytes = DatadogEncoder::new().encode_agent_payload(&batch).unwrap();
        let back = AgentPayload::decode(bytes.as_ref()).unwrap();
        let mut want = payload();
        want.tracer_payloads.truncate(1);
        want.idx_tracer_payloads.clear();
        want.tracer_payloads[0].chunks[0].spans[0].duration = 0;
        assert_eq!(back, want);
        let again = d.decode_agent_payload(&bytes, 0).unwrap();
        assert_eq!(again, vec![batch]);
    }

    #[test]
    fn negative_int32_priority_is_a_ten_byte_varint() {
        let mut p = Pb::default();
        p.int(1, -128);
        assert_eq!(p.0.len(), 11);
        let back = TraceChunk::decode(p.0.as_slice()).unwrap();
        assert_eq!(back.priority, -128);
    }

    #[test]
    fn a_batch_without_spans_encodes_to_nothing() {
        let batch = new_batch(Default::default(), Vec::new());
        assert!(DatadogEncoder::new().encode_agent_payload(&batch).is_none());
    }
}
