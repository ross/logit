//! The tracer API's msgpack trace forms, over [`crate::msgpack`]: v0.4 (an array of traces, each
//! an array of span maps), v0.5 (a string dictionary plus 12-element span arrays), and v0.7 (one
//! `TracerPayload` map). The span mapping itself is [`super::traces`]'s; this module only parses
//! and writes the wire. The mapping tables are in [`super`]'s module doc, under "Traces".
//!
//! Decoding follows the Agent's generated `UnmarshalMsg` (`span_gen.go`, `tracer_payload_gen.go`)
//! and `decoder_v05.go`: an unknown map key is skipped, an absent or `nil` field is its zero
//! value, any int format satisfies an integer field (a negative one read as a `uint64` wraps, as
//! Go's cast does), and a string field also accepts `bin`. One malformed span (or trace, or
//! chunk) is dropped and counted while the rest of the body decodes, because each is first
//! skipped structurally and then parsed from its own slice; a body whose structure itself is
//! broken is `CodecError::Malformed`.
//!
//! Encoding writes the Agent's own key sets and orders (`EncodeMsg`), omitting exactly what its
//! `omitempty` tags omit, with every map sorted by key.

use super::logs::new_batch;
use super::traces::{
    Form, StrMap, WireAny, WireChunk, WireContainerDebug, WireLink, WireSpan, WireSpanEvent,
    WireTracer, ANY_ARRAY,
};
use super::{DatadogDecoder, DatadogEncoder};
use crate::msgpack::{MsgpackError, Reader, Type, Writer};
use crate::CodecError;
use bytes::Bytes;
use logit_core::{EventBatch, Resource};
use std::collections::HashMap;
use std::fmt;

/// v0.5's fixed span arity (`spanPropertyCount`).
const V05_SPAN_FIELDS: usize = 12;

/// Why one span (or chunk) didn't parse.
#[derive(Debug)]
pub(super) enum SpanError {
    Msgpack(MsgpackError),
    Shape(String),
}

impl From<MsgpackError> for SpanError {
    fn from(e: MsgpackError) -> Self {
        SpanError::Msgpack(e)
    }
}

impl fmt::Display for SpanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SpanError::Msgpack(e) => write!(f, "msgpack: {e}"),
            SpanError::Shape(s) => f.write_str(s),
        }
    }
}

impl From<SpanError> for CodecError {
    fn from(e: SpanError) -> Self {
        CodecError::Malformed(format!("datadog traces: {e}"))
    }
}

type Res<T> = Result<T, SpanError>;

/// Counts what a parse had to repair, for the decoder to report once per body.
#[derive(Default)]
struct Repairs {
    invalid_utf8: usize,
}

/// The next whole value's bytes, skipped structurally. `buf` is the slice `r` reads.
fn next_value<'a>(r: &mut Reader<'a>, buf: &'a [u8]) -> Result<&'a [u8], MsgpackError> {
    let start = buf.len() - r.remaining();
    r.skip_value()?;
    Ok(&buf[start..buf.len() - r.remaining()])
}

fn is_nil(r: &mut Reader<'_>) -> Result<bool, MsgpackError> {
    if r.peek_type()? == Type::Nil {
        r.read_nil()?;
        Ok(true)
    } else {
        Ok(false)
    }
}

/// A string field: `str` or `bin` (lossy UTF-8, counted), `nil` as `""`.
fn string(r: &mut Reader<'_>, repairs: &mut Repairs) -> Res<String> {
    let bytes = match r.peek_type()? {
        Type::Nil => {
            r.read_nil()?;
            return Ok(String::new());
        }
        Type::Bin => r.read_bin()?,
        _ => r.read_str_bytes()?,
    };
    Ok(match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        Err(_) => {
            repairs.invalid_utf8 += 1;
            String::from_utf8_lossy(bytes).into_owned()
        }
    })
}

/// A `bytes` field: `bin` or `str`, `nil` as empty.
fn byte_string(r: &mut Reader<'_>) -> Res<Vec<u8>> {
    Ok(match r.peek_type()? {
        Type::Nil => {
            r.read_nil()?;
            Vec::new()
        }
        Type::Str => r.read_str_bytes()?.to_vec(),
        _ => r.read_bin()?.to_vec(),
    })
}

fn u64_any(r: &mut Reader<'_>) -> Res<u64> {
    if is_nil(r)? {
        return Ok(0);
    }
    Ok(match r.read_u64() {
        Ok(v) => v,
        Err(MsgpackError::Type { .. }) => r.read_i64()? as u64,
        Err(e) => return Err(e.into()),
    })
}

fn i64_any(r: &mut Reader<'_>) -> Res<i64> {
    if is_nil(r)? {
        return Ok(0);
    }
    Ok(match r.read_i64() {
        Ok(v) => v,
        Err(MsgpackError::Type { .. }) => r.read_u64()? as i64,
        Err(e) => return Err(e.into()),
    })
}

fn i32_any(r: &mut Reader<'_>) -> Res<i32> {
    let v = i64_any(r)?;
    i32::try_from(v).map_err(|_| SpanError::Shape(format!("{v} overflows an int32 field")))
}

fn u32_any(r: &mut Reader<'_>) -> Res<u32> {
    let v = u64_any(r)?;
    u32::try_from(v).map_err(|_| SpanError::Shape(format!("{v} overflows a uint32 field")))
}

fn f64_any(r: &mut Reader<'_>) -> Res<f64> {
    if is_nil(r)? {
        return Ok(0.0);
    }
    Ok(r.read_f64()?)
}

fn bool_any(r: &mut Reader<'_>) -> Res<bool> {
    if is_nil(r)? {
        return Ok(false);
    }
    Ok(r.read_bool()?)
}

fn map_len(r: &mut Reader<'_>) -> Res<usize> {
    if is_nil(r)? {
        return Ok(0);
    }
    Ok(r.read_map_len()?)
}

fn array_len(r: &mut Reader<'_>) -> Res<usize> {
    if is_nil(r)? {
        return Ok(0);
    }
    Ok(r.read_array_len()?)
}

fn str_map(r: &mut Reader<'_>, repairs: &mut Repairs) -> Res<StrMap> {
    let n = map_len(r)?;
    let mut out = Vec::with_capacity(n.min(1024));
    for _ in 0..n {
        let k = string(r, repairs)?;
        let v = string(r, repairs)?;
        out.push((k, v));
    }
    Ok(out)
}

/// One v0.4/v0.7 span map (`span_gen.go`'s `UnmarshalMsg`).
fn span_map(r: &mut Reader<'_>, repairs: &mut Repairs) -> Res<WireSpan> {
    let mut s = WireSpan::default();
    let n = r.read_map_len()?;
    for _ in 0..n {
        let key = string(r, repairs)?;
        match key.as_str() {
            "service" => s.service = string(r, repairs)?,
            "name" => s.name = string(r, repairs)?,
            "resource" => s.resource = string(r, repairs)?,
            "trace_id" => s.trace_id = u64_any(r)?,
            "span_id" => s.span_id = u64_any(r)?,
            "parent_id" => s.parent_id = u64_any(r)?,
            "start" => s.start = i64_any(r)?,
            "duration" => s.duration = i64_any(r)?,
            "error" => s.error = i32_any(r)?,
            "meta" => s.meta = str_map(r, repairs)?,
            "metrics" => {
                let n = map_len(r)?;
                s.metrics.clear();
                for _ in 0..n {
                    let k = string(r, repairs)?;
                    s.metrics.push((k, f64_any(r)?));
                }
            }
            "type" => s.r#type = string(r, repairs)?,
            "meta_struct" => {
                let n = map_len(r)?;
                s.meta_struct.clear();
                for _ in 0..n {
                    let k = string(r, repairs)?;
                    s.meta_struct.push((k, byte_string(r)?));
                }
            }
            "span_links" => {
                let n = array_len(r)?;
                s.span_links.clear();
                for _ in 0..n {
                    s.span_links.push(link_map(r, repairs)?);
                }
            }
            "span_events" => {
                let n = array_len(r)?;
                s.span_events.clear();
                for _ in 0..n {
                    s.span_events.push(event_map(r, repairs)?);
                }
            }
            _ => r.skip_value()?,
        }
    }
    Ok(s)
}

fn link_map(r: &mut Reader<'_>, repairs: &mut Repairs) -> Res<WireLink> {
    let mut l = WireLink::default();
    if is_nil(r)? {
        return Ok(l);
    }
    let n = r.read_map_len()?;
    for _ in 0..n {
        let key = string(r, repairs)?;
        match key.as_str() {
            "trace_id" => l.trace_id = u64_any(r)?,
            "trace_id_high" => l.trace_id_high = u64_any(r)?,
            "span_id" => l.span_id = u64_any(r)?,
            "attributes" => l.attributes = str_map(r, repairs)?,
            "tracestate" => l.tracestate = string(r, repairs)?,
            "flags" => l.flags = u32_any(r)?,
            _ => r.skip_value()?,
        }
    }
    Ok(l)
}

fn event_map(r: &mut Reader<'_>, repairs: &mut Repairs) -> Res<WireSpanEvent> {
    let mut e = WireSpanEvent::default();
    if is_nil(r)? {
        return Ok(e);
    }
    let n = r.read_map_len()?;
    for _ in 0..n {
        let key = string(r, repairs)?;
        match key.as_str() {
            "time_unix_nano" => e.time_unix_nano = u64_any(r)?,
            "name" => e.name = string(r, repairs)?,
            "attributes" => {
                let n = map_len(r)?;
                e.attributes.clear();
                for _ in 0..n {
                    let k = string(r, repairs)?;
                    e.attributes.push((k, any_map(r, repairs, true)?));
                }
            }
            _ => r.skip_value()?,
        }
    }
    Ok(e)
}

/// An `AttributeAnyValue` map (`top`), or an `AttributeArrayValue` one (no `array_value`).
fn any_map(r: &mut Reader<'_>, repairs: &mut Repairs, top: bool) -> Res<WireAny> {
    let (mut kind, mut string_value, mut bool_value, mut int_value, mut double_value) =
        (0, String::new(), false, 0, 0.0);
    let mut array = None;
    if !is_nil(r)? {
        let n = r.read_map_len()?;
        for _ in 0..n {
            let key = string(r, repairs)?;
            match key.as_str() {
                "type" => kind = i64_any(r)?,
                "string_value" => string_value = string(r, repairs)?,
                "bool_value" => bool_value = bool_any(r)?,
                "int_value" => int_value = i64_any(r)?,
                "double_value" => double_value = f64_any(r)?,
                "array_value" if top => {
                    let mut values = Vec::new();
                    if !is_nil(r)? {
                        let m = r.read_map_len()?;
                        for _ in 0..m {
                            let k = string(r, repairs)?;
                            if k == "values" {
                                let len = array_len(r)?;
                                values.clear();
                                for _ in 0..len {
                                    values.push(any_map(r, repairs, false)?);
                                }
                            } else {
                                r.skip_value()?;
                            }
                        }
                    }
                    array = Some(values);
                }
                _ => r.skip_value()?,
            }
        }
    }
    if kind == ANY_ARRAY && top && array.is_none() {
        array = Some(Vec::new());
    }
    let array = if top { array } else { None };
    Ok(WireAny::from_parts(kind, string_value, bool_value, int_value, double_value, array))
}

/// One v0.5 span: a 12-element array whose strings are dictionary indices.
fn span_v05(r: &mut Reader<'_>, dict: &[String]) -> Res<WireSpan> {
    let n = r.read_array_len()?;
    if n != V05_SPAN_FIELDS {
        return Err(SpanError::Shape(format!("a v0.5 span has {n} elements, not 12")));
    }
    let lookup = |r: &mut Reader<'_>| -> Res<String> {
        let i = u32_any(r)? as usize;
        dict.get(i).cloned().ok_or_else(|| {
            SpanError::Shape(format!("dictionary index {i} out of range ({})", dict.len()))
        })
    };
    let mut s = WireSpan {
        service: lookup(r)?,
        name: lookup(r)?,
        resource: lookup(r)?,
        trace_id: u64_any(r)?,
        span_id: u64_any(r)?,
        parent_id: u64_any(r)?,
        start: i64_any(r)?,
        duration: i64_any(r)?,
        error: i32_any(r)?,
        ..WireSpan::default()
    };
    let meta = map_len(r)?;
    for _ in 0..meta {
        let k = lookup(r)?;
        let v = lookup(r)?;
        s.meta.push((k, v));
    }
    let metrics = map_len(r)?;
    for _ in 0..metrics {
        let k = lookup(r)?;
        s.metrics.push((k, f64_any(r)?));
    }
    s.r#type = lookup(r)?;
    Ok(s)
}

fn container_debug(r: &mut Reader<'_>, repairs: &mut Repairs) -> Res<Option<WireContainerDebug>> {
    if is_nil(r)? {
        return Ok(None);
    }
    let mut d = WireContainerDebug::default();
    let n = r.read_map_len()?;
    for _ in 0..n {
        let key = string(r, repairs)?;
        match key.as_str() {
            "error" => d.error = string(r, repairs)?,
            "latency_ms" => d.latency_ms = i64_any(r)?,
            "was_buffered" => d.was_buffered = bool_any(r)?,
            "buffer_ms" => d.buffer_ms = i64_any(r)?,
            "buffer_eviction_reason" => d.buffer_eviction_reason = string(r, repairs)?,
            _ => r.skip_value()?,
        }
    }
    Ok(Some(d))
}

impl DatadogDecoder {
    fn repaired(&self, repairs: &Repairs) {
        self.span_degraded("invalid_utf8", repairs.invalid_utf8);
    }

    /// Parses the spans of one trace array (v0.4) or chunk `spans` array (v0.7), dropping each
    /// malformed span on its own.
    fn span_array(&mut self, buf: &[u8], repairs: &mut Repairs) -> Res<Vec<WireSpan>> {
        let mut r = Reader::new(buf);
        let n = array_len(&mut r)?;
        let mut spans = Vec::with_capacity(n.min(1024));
        for _ in 0..n {
            let one = next_value(&mut r, buf)?;
            match span_map(&mut Reader::new(one), repairs) {
                Ok(s) => spans.push(s),
                Err(e) => self.malformed_span(1, e),
            }
        }
        Ok(spans)
    }

    /// `/v0.4/traces` (and `/v0.3`'s msgpack): an array of traces, each an array of span maps.
    /// One event per span, in wire order. v0.4 has no chunk, so no `datadog.chunk.*`, and no
    /// payload, so the batch resource is empty. `received_at` is unused: a span's `start` is its
    /// timestamp.
    pub fn decode_traces_v04(
        &mut self,
        body: &[u8],
        received_at: i64,
    ) -> Result<EventBatch, CodecError> {
        let _ = received_at;
        let mut r = Reader::new(body);
        let n = r.read_array_len()?;
        let mut repairs = Repairs::default();
        let mut events = Vec::new();
        for _ in 0..n {
            let trace = next_value(&mut r, body)?;
            match self.span_array(trace, &mut repairs) {
                Ok(spans) => self.chunk_events(
                    WireChunk { spans, ..WireChunk::default() },
                    false,
                    &mut events,
                ),
                Err(e) => self.malformed_span(1, e),
            }
        }
        self.repaired(&repairs);
        Ok(new_batch(Resource::default(), events))
    }

    /// `/v0.5/traces`: `[dictionary, traces]`, each span a 12-element array of dictionary
    /// indices and raw integers. A span with the wrong arity or an index past the dictionary is
    /// dropped on its own.
    pub fn decode_traces_v05(
        &mut self,
        body: &[u8],
        received_at: i64,
    ) -> Result<EventBatch, CodecError> {
        let _ = received_at;
        let mut r = Reader::new(body);
        let n = r.read_array_len()?;
        if n != 2 {
            return Err(CodecError::Malformed(format!(
                "datadog v0.5 traces: a {n}-element top level, not [dictionary, traces]"
            )));
        }
        let mut repairs = Repairs::default();
        let dict_len = r.read_array_len()?;
        let mut dict = Vec::with_capacity(dict_len.min(1 << 16));
        for _ in 0..dict_len {
            dict.push(string(&mut r, &mut repairs)?);
        }
        let traces = r.read_array_len()?;
        let mut events = Vec::new();
        for _ in 0..traces {
            let trace = next_value(&mut r, body)?;
            let mut tr = Reader::new(trace);
            let spans_len = match array_len(&mut tr) {
                Ok(n) => n,
                Err(e) => {
                    self.malformed_span(1, e);
                    continue;
                }
            };
            let mut spans = Vec::with_capacity(spans_len.min(1024));
            for _ in 0..spans_len {
                let one = next_value(&mut tr, trace)?;
                match span_v05(&mut Reader::new(one), &dict) {
                    Ok(s) => spans.push(s),
                    Err(e) => self.malformed_span(1, e),
                }
            }
            self.chunk_events(WireChunk { spans, ..WireChunk::default() }, false, &mut events);
        }
        self.repaired(&repairs);
        Ok(new_batch(Resource::default(), events))
    }

    /// `/v0.7/traces`: one msgpack `TracerPayload` map. Its fields become the batch resource, its
    /// chunks' fields `datadog.chunk.*` on every span of the chunk.
    pub fn decode_tracer_payload_v07(
        &mut self,
        body: &[u8],
        received_at: i64,
    ) -> Result<EventBatch, CodecError> {
        let _ = received_at;
        let mut r = Reader::new(body);
        let mut repairs = Repairs::default();
        let mut t = WireTracer::default();
        let n = r.read_map_len()?;
        for _ in 0..n {
            let key = string(&mut r, &mut repairs)?;
            match key.as_str() {
                "container_id" => t.container_id = string(&mut r, &mut repairs)?,
                "language_name" => t.language_name = string(&mut r, &mut repairs)?,
                "language_version" => t.language_version = string(&mut r, &mut repairs)?,
                "tracer_version" => t.tracer_version = string(&mut r, &mut repairs)?,
                "runtime_id" => t.runtime_id = string(&mut r, &mut repairs)?,
                "chunks" => {
                    let chunks = array_len(&mut r)?;
                    t.chunks.clear();
                    for _ in 0..chunks {
                        let one = next_value(&mut r, body)?;
                        match self.chunk_v07(one, &mut repairs) {
                            Ok(c) => t.chunks.push(c),
                            Err(e) => self.malformed_span(1, e),
                        }
                    }
                }
                "tags" => t.tags = str_map(&mut r, &mut repairs)?,
                "env" => t.env = string(&mut r, &mut repairs)?,
                "hostname" => t.hostname = string(&mut r, &mut repairs)?,
                "app_version" => t.app_version = string(&mut r, &mut repairs)?,
                "container_debug" => t.container_debug = container_debug(&mut r, &mut repairs)?,
                _ => r.skip_value()?,
            }
        }
        self.repaired(&repairs);
        let resource = self.tracer_resource(&t, None);
        let mut events = Vec::new();
        for chunk in std::mem::take(&mut t.chunks) {
            self.chunk_events(chunk, true, &mut events);
        }
        Ok(new_batch(resource, events))
    }

    /// One v0.7 `TraceChunk` map (`tracer_payload_gen.go`).
    fn chunk_v07(&mut self, buf: &[u8], repairs: &mut Repairs) -> Res<WireChunk> {
        let mut r = Reader::new(buf);
        // The Go zero value, not `PriorityNone`: a v0.7 chunk with no `priority` key is 0.
        let mut c = WireChunk { priority: 0, ..WireChunk::default() };
        if is_nil(&mut r)? {
            return Ok(c);
        }
        let n = r.read_map_len()?;
        for _ in 0..n {
            let key = string(&mut r, repairs)?;
            match key.as_str() {
                "priority" => c.priority = i32_any(&mut r)?,
                "origin" => c.origin = string(&mut r, repairs)?,
                "spans" => {
                    let spans = next_value(&mut r, buf)?;
                    c.spans = self.span_array(spans, repairs)?;
                }
                "tags" => c.tags = str_map(&mut r, repairs)?,
                "dropped_trace" => c.dropped_trace = bool_any(&mut r)?,
                _ => r.skip_value()?,
            }
        }
        Ok(c)
    }
}

fn write_str_map(w: &mut Writer, map: &StrMap) {
    w.write_map_len(map.len());
    for (k, v) in map {
        w.write_str(k);
        w.write_str(v);
    }
}

/// One span map, in `span_gen.go`'s `EncodeMsg` key order, `meta`/`metrics`/`meta_struct`/
/// `span_links`/`span_events` omitted when empty (their `omitempty`).
fn write_span(w: &mut Writer, s: &WireSpan) {
    let optional = [
        !s.meta.is_empty(),
        !s.metrics.is_empty(),
        !s.meta_struct.is_empty(),
        !s.span_links.is_empty(),
        !s.span_events.is_empty(),
    ];
    w.write_map_len(10 + optional.iter().filter(|&&b| b).count());
    w.write_str("service");
    w.write_str(&s.service);
    w.write_str("name");
    w.write_str(&s.name);
    w.write_str("resource");
    w.write_str(&s.resource);
    w.write_str("trace_id");
    w.write_u64(s.trace_id);
    w.write_str("span_id");
    w.write_u64(s.span_id);
    w.write_str("parent_id");
    w.write_u64(s.parent_id);
    w.write_str("start");
    w.write_i64(s.start);
    w.write_str("duration");
    w.write_i64(s.duration);
    w.write_str("error");
    w.write_i64(s.error.into());
    if !s.meta.is_empty() {
        w.write_str("meta");
        write_str_map(w, &s.meta);
    }
    if !s.metrics.is_empty() {
        w.write_str("metrics");
        w.write_map_len(s.metrics.len());
        for (k, v) in &s.metrics {
            w.write_str(k);
            w.write_f64(*v);
        }
    }
    w.write_str("type");
    w.write_str(&s.r#type);
    if !s.meta_struct.is_empty() {
        w.write_str("meta_struct");
        w.write_map_len(s.meta_struct.len());
        for (k, v) in &s.meta_struct {
            w.write_str(k);
            w.write_bin(v);
        }
    }
    if !s.span_links.is_empty() {
        w.write_str("span_links");
        w.write_array_len(s.span_links.len());
        for l in &s.span_links {
            write_link(w, l);
        }
    }
    if !s.span_events.is_empty() {
        w.write_str("span_events");
        w.write_array_len(s.span_events.len());
        for e in &s.span_events {
            w.write_map_len(3);
            w.write_str("time_unix_nano");
            w.write_u64(e.time_unix_nano);
            w.write_str("name");
            w.write_str(&e.name);
            w.write_str("attributes");
            w.write_map_len(e.attributes.len());
            for (k, v) in &e.attributes {
                w.write_str(k);
                write_any(w, v);
            }
        }
    }
}

fn write_link(w: &mut Writer, l: &WireLink) {
    let optional =
        [l.trace_id_high != 0, !l.attributes.is_empty(), !l.tracestate.is_empty(), l.flags != 0];
    w.write_map_len(2 + optional.iter().filter(|&&b| b).count());
    w.write_str("trace_id");
    w.write_u64(l.trace_id);
    if l.trace_id_high != 0 {
        w.write_str("trace_id_high");
        w.write_u64(l.trace_id_high);
    }
    w.write_str("span_id");
    w.write_u64(l.span_id);
    if !l.attributes.is_empty() {
        w.write_str("attributes");
        write_str_map(w, &l.attributes);
    }
    if !l.tracestate.is_empty() {
        w.write_str("tracestate");
        w.write_str(&l.tracestate);
    }
    if l.flags != 0 {
        w.write_str("flags");
        w.write_u64(l.flags.into());
    }
}

/// `{type, <the one value field>}`: the Agent's decoder reads any subset of the union's fields,
/// so the unused ones aren't written.
fn write_any(w: &mut Writer, v: &WireAny) {
    w.write_map_len(2);
    w.write_str("type");
    w.write_i64(v.kind());
    match v {
        WireAny::Str(s) => {
            w.write_str("string_value");
            w.write_str(s);
        }
        WireAny::Unknown => {
            w.write_str("string_value");
            w.write_str("");
        }
        WireAny::Bool(b) => {
            w.write_str("bool_value");
            w.write_bool(*b);
        }
        WireAny::Int(i) => {
            w.write_str("int_value");
            w.write_i64(*i);
        }
        WireAny::Double(f) => {
            w.write_str("double_value");
            w.write_f64(*f);
        }
        WireAny::Array(items) => {
            w.write_str("array_value");
            w.write_map_len(1);
            w.write_str("values");
            w.write_array_len(items.len());
            for item in items {
                write_any(w, item);
            }
        }
    }
}

fn write_chunk(w: &mut Writer, c: &WireChunk) {
    w.write_map_len(5);
    w.write_str("priority");
    w.write_i64(c.priority.into());
    w.write_str("origin");
    w.write_str(&c.origin);
    w.write_str("spans");
    w.write_array_len(c.spans.len());
    for s in &c.spans {
        write_span(w, s);
    }
    w.write_str("tags");
    write_str_map(w, &c.tags);
    w.write_str("dropped_trace");
    w.write_bool(c.dropped_trace);
}

fn write_tracer(w: &mut Writer, t: &WireTracer) {
    w.write_map_len(10 + usize::from(t.container_debug.is_some()));
    for (key, value) in [
        ("container_id", &t.container_id),
        ("language_name", &t.language_name),
        ("language_version", &t.language_version),
        ("tracer_version", &t.tracer_version),
        ("runtime_id", &t.runtime_id),
    ] {
        w.write_str(key);
        w.write_str(value);
    }
    w.write_str("chunks");
    w.write_array_len(t.chunks.len());
    for c in &t.chunks {
        write_chunk(w, c);
    }
    w.write_str("tags");
    write_str_map(w, &t.tags);
    for (key, value) in
        [("env", &t.env), ("hostname", &t.hostname), ("app_version", &t.app_version)]
    {
        w.write_str(key);
        w.write_str(value);
    }
    if let Some(d) = &t.container_debug {
        w.write_str("container_debug");
        let present = [
            !d.error.is_empty(),
            d.latency_ms != 0,
            d.was_buffered,
            d.buffer_ms != 0,
            !d.buffer_eviction_reason.is_empty(),
        ];
        w.write_map_len(present.iter().filter(|&&b| b).count());
        if !d.error.is_empty() {
            w.write_str("error");
            w.write_str(&d.error);
        }
        if d.latency_ms != 0 {
            w.write_str("latency_ms");
            w.write_i64(d.latency_ms);
        }
        if d.was_buffered {
            w.write_str("was_buffered");
            w.write_bool(true);
        }
        if d.buffer_ms != 0 {
            w.write_str("buffer_ms");
            w.write_i64(d.buffer_ms);
        }
        if !d.buffer_eviction_reason.is_empty() {
            w.write_str("buffer_eviction_reason");
            w.write_str(&d.buffer_eviction_reason);
        }
    }
}

/// v0.5's string table: `""` at index 0, then every string in first-use order.
struct Dictionary {
    strings: Vec<String>,
    index: HashMap<String, u32>,
}

impl Dictionary {
    fn new() -> Self {
        let mut d = Dictionary { strings: Vec::new(), index: HashMap::new() };
        d.id("");
        d
    }

    fn id(&mut self, s: &str) -> u32 {
        if let Some(&i) = self.index.get(s) {
            return i;
        }
        let i = self.strings.len() as u32;
        self.strings.push(s.to_string());
        self.index.insert(s.to_string(), i);
        i
    }
}

impl DatadogEncoder {
    /// `/v0.4/traces`: one trace array per 128-bit trace id. `None` when the batch has no span.
    /// Chunk and payload carriers have no v0.4 field: dropped, counted `no_wire_form`.
    pub fn encode_traces_v04(&mut self, batch: &EventBatch) -> Option<Bytes> {
        let chunks = self.batch_chunks(batch, Form::V04);
        if chunks.is_empty() {
            return None;
        }
        self.resource_carriers_lost(&batch.resource, Form::V04);
        let mut w = Writer::new();
        w.write_array_len(chunks.len());
        for c in &chunks {
            w.write_array_len(c.spans.len());
            for s in &c.spans {
                write_span(&mut w, s);
            }
        }
        Some(Bytes::from(w.into_inner()))
    }

    /// `/v0.5/traces`: `[dictionary, traces]`. v0.5 spans have no `meta_struct`, links, or events
    /// either: each is dropped, counted `no_wire_form`.
    pub fn encode_traces_v05(&mut self, batch: &EventBatch) -> Option<Bytes> {
        let chunks = self.batch_chunks(batch, Form::V05);
        if chunks.is_empty() {
            return None;
        }
        self.resource_carriers_lost(&batch.resource, Form::V05);
        let mut dict = Dictionary::new();
        let mut body = Writer::new();
        body.write_array_len(chunks.len());
        for c in &chunks {
            body.write_array_len(c.spans.len());
            for s in &c.spans {
                body.write_array_len(V05_SPAN_FIELDS);
                body.write_u64(dict.id(&s.service).into());
                body.write_u64(dict.id(&s.name).into());
                body.write_u64(dict.id(&s.resource).into());
                body.write_u64(s.trace_id);
                body.write_u64(s.span_id);
                body.write_u64(s.parent_id);
                body.write_i64(s.start);
                body.write_i64(s.duration);
                body.write_i64(s.error.into());
                body.write_map_len(s.meta.len());
                for (k, v) in &s.meta {
                    body.write_u64(dict.id(k).into());
                    body.write_u64(dict.id(v).into());
                }
                body.write_map_len(s.metrics.len());
                for (k, v) in &s.metrics {
                    body.write_u64(dict.id(k).into());
                    body.write_f64(*v);
                }
                body.write_u64(dict.id(&s.r#type).into());
            }
        }
        let mut w = Writer::new();
        w.write_array_len(2);
        w.write_array_len(dict.strings.len());
        for s in &dict.strings {
            w.write_str(s);
        }
        let mut out = w.into_inner();
        out.extend_from_slice(&body.into_inner());
        Some(Bytes::from(out))
    }

    /// `/v0.7/traces`: one `TracerPayload` from the batch resource, one chunk per trace id.
    pub fn encode_tracer_payload_v07(&mut self, batch: &EventBatch) -> Option<Bytes> {
        let chunks = self.batch_chunks(batch, Form::V07);
        if chunks.is_empty() {
            return None;
        }
        self.resource_carriers_lost(&batch.resource, Form::V07);
        let tracer = self.wire_tracer(&batch.resource, chunks);
        let mut w = Writer::new();
        write_tracer(&mut w, &tracer);
        Some(Bytes::from(w.into_inner()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datadog::logs::tests::counted;
    use crate::datadog::traces::{
        ATTR_CHUNK_DROPPED_TRACE, ATTR_CHUNK_ORIGIN, ATTR_CHUNK_PRIORITY, ATTR_CHUNK_TAGS,
        ATTR_SPAN_ERROR, RESOURCE_ATTR_TRACER_CONTAINER_DEBUG, RESOURCE_ATTR_TRACER_TAGS,
    };
    use crate::datadog::{
        ATTR_RESOURCE_NAME, ATTR_SERVICE_NAME, ATTR_SPAN_TYPE, RESOURCE_ATTR_TRACER_LANGUAGE_NAME,
    };
    use logit_core::{Registry, SpanKind, SpanStatus, Value};
    use std::sync::Arc;

    fn with_registry() -> (DatadogDecoder, DatadogEncoder, Arc<Registry>) {
        let registry = Registry::new();
        let t = registry.telemetry_for("dd", "datadog", "listener");
        (
            DatadogDecoder::new().with_telemetry(t.clone()),
            DatadogEncoder::new().with_telemetry(t),
            registry,
        )
    }

    fn reasons(registry: &Registry) -> Vec<String> {
        registry
            .drain(0)
            .iter()
            .filter_map(|e| e.attributes.get("reason").and_then(Value::as_str).map(String::from))
            .collect()
    }

    fn span_bytes(f: impl FnOnce(&mut Writer)) -> Vec<u8> {
        let mut w = Writer::new();
        f(&mut w);
        w.into_inner()
    }

    /// `[[{..span..}]]` with the given key/value writer.
    fn v04(pairs: usize, f: impl FnOnce(&mut Writer)) -> Vec<u8> {
        span_bytes(|w| {
            w.write_array_len(1);
            w.write_array_len(1);
            w.write_map_len(pairs);
            f(w);
        })
    }

    #[test]
    fn a_v04_span_decodes_field_by_field() {
        let (mut d, _, _) = with_registry();
        let body = v04(13, |w| {
            for (k, v) in [("service", "web"), ("name", "http.request"), ("resource", "GET /")] {
                w.write_str(k);
                w.write_str(v);
            }
            w.write_str("trace_id");
            w.write_u64(1);
            w.write_str("span_id");
            w.write_u64(2);
            w.write_str("parent_id");
            w.write_u64(0);
            w.write_str("start");
            w.write_i64(1_690_000_000_000_000_000);
            w.write_str("duration");
            w.write_i64(500_000);
            w.write_str("error");
            w.write_i64(1);
            w.write_str("meta");
            w.write_map_len(2);
            w.write_str("env");
            w.write_str("prod");
            w.write_str("span.kind");
            w.write_str("server");
            w.write_str("metrics");
            w.write_map_len(1);
            w.write_str("_sampling_priority_v1");
            w.write_i64(1); // an int where a float belongs, which the Agent accepts
            w.write_str("type");
            w.write_str("web");
            w.write_str("unknown_future_key");
            w.write_array_len(1);
            w.write_nil();
        });
        let batch = d.decode_traces_v04(&body, 0).unwrap();
        let e = &batch.events[0];
        let s = e.span.as_ref().unwrap();
        assert_eq!(e.timestamp, 1_690_000_000_000_000_000);
        assert_eq!(s.end_timestamp, 1_690_000_000_000_500_000);
        assert_eq!(s.trace_id, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
        assert_eq!(s.span_id, 2u64.to_be_bytes());
        assert_eq!(s.parent_span_id, None);
        assert_eq!(s.name, Value::str("http.request"));
        assert_eq!(s.kind, SpanKind::Server);
        assert_eq!(s.status, SpanStatus::Error);
        assert_eq!(e.attributes.get(ATTR_SERVICE_NAME), Some(&Value::str("web")));
        assert_eq!(e.attributes.get(ATTR_RESOURCE_NAME), Some(&Value::str("GET /")));
        assert_eq!(e.attributes.get(ATTR_SPAN_TYPE), Some(&Value::str("web")));
        assert_eq!(e.attributes.get("span.kind"), Some(&Value::str("server")));
        assert_eq!(e.attributes.get("_sampling_priority_v1"), Some(&Value::F64(1.0)));
        assert_eq!(e.attributes.get(ATTR_SPAN_ERROR), None, "1 is carried by the status alone");
        assert!(batch.resource.attributes.is_empty());
    }

    #[test]
    fn absent_and_nil_fields_are_zero_values() {
        let (mut d, _, _) = with_registry();
        let body = v04(3, |w| {
            w.write_str("span_id");
            w.write_u64(7);
            w.write_str("meta");
            w.write_nil();
            w.write_str("service");
            w.write_nil();
        });
        let batch = d.decode_traces_v04(&body, 0).unwrap();
        let e = &batch.events[0];
        assert!(e.attributes.is_empty());
        let s = e.span.as_ref().unwrap();
        assert_eq!(s.trace_id, [0; 16]);
        assert_eq!(s.name, Value::str(""));
        assert_eq!(s.status, SpanStatus::Unset);
        assert_eq!(s.kind, SpanKind::Internal);
    }

    #[test]
    fn dd_p_tid_sets_the_high_half_for_the_whole_chunk_and_stays_on_its_span() {
        let (mut d, _, _) = with_registry();
        let span = |w: &mut Writer, id: u64, tid: Option<&str>| {
            w.write_map_len(if tid.is_some() { 3 } else { 2 });
            w.write_str("trace_id");
            w.write_u64(5);
            w.write_str("span_id");
            w.write_u64(id);
            if let Some(tid) = tid {
                w.write_str("meta");
                w.write_map_len(1);
                w.write_str("_dd.p.tid");
                w.write_str(tid);
            }
        };
        let body = span_bytes(|w| {
            w.write_array_len(1);
            w.write_array_len(2);
            span(w, 1, None);
            span(w, 2, Some("64f5a1b200000000"));
        });
        let batch = d.decode_traces_v04(&body, 0).unwrap();
        let want = {
            let mut id = [0u8; 16];
            id[..8].copy_from_slice(&0x64f5_a1b2_0000_0000u64.to_be_bytes());
            id[15] = 5;
            id
        };
        for e in &batch.events {
            assert_eq!(e.span.as_ref().unwrap().trace_id, want);
        }
        assert_eq!(batch.events[0].attributes.get("_dd.p.tid"), None);
        assert_eq!(
            batch.events[1].attributes.get("_dd.p.tid"),
            Some(&Value::str("64f5a1b200000000"))
        );

        let mut e = DatadogEncoder::new();
        let again = d.decode_traces_v04(&e.encode_traces_v04(&batch).unwrap(), 0).unwrap();
        assert_eq!(again, batch, "the tid goes back on its own span only");
    }

    #[test]
    fn an_unparseable_tid_keeps_its_attribute_and_counts() {
        let (mut d, _, registry) = with_registry();
        let body = v04(2, |w| {
            w.write_str("trace_id");
            w.write_u64(9);
            w.write_str("meta");
            w.write_map_len(1);
            w.write_str("_dd.p.tid");
            w.write_str("not-hex");
        });
        let batch = d.decode_traces_v04(&body, 0).unwrap();
        let e = &batch.events[0];
        assert_eq!(e.span.as_ref().unwrap().trace_id[..8], [0; 8]);
        assert_eq!(e.attributes.get("_dd.p.tid"), Some(&Value::str("not-hex")));
        assert!(reasons(&registry).contains(&"bad_tid".to_string()));
    }

    #[test]
    fn odd_errors_and_negative_durations() {
        let (mut d, _, registry) = with_registry();
        let body = v04(3, |w| {
            w.write_str("error");
            w.write_i64(2);
            w.write_str("start");
            w.write_i64(100);
            w.write_str("duration");
            w.write_i64(-5);
        });
        let batch = d.decode_traces_v04(&body, 0).unwrap();
        let e = &batch.events[0];
        let s = e.span.as_ref().unwrap();
        assert_eq!(s.status, SpanStatus::Error);
        assert_eq!(s.end_timestamp, 100);
        assert_eq!(e.attributes.get(ATTR_SPAN_ERROR), Some(&Value::I64(2)));
        assert!(reasons(&registry).contains(&"negative_duration".to_string()));
        let body = DatadogEncoder::new().encode_traces_v04(&batch).unwrap();
        let again = d.decode_traces_v04(&body, 0).unwrap();
        assert_eq!(again, batch, "error 2 re-encodes exactly");
    }

    #[test]
    fn a_malformed_span_is_dropped_and_the_rest_decodes() {
        let (mut d, _, registry) = with_registry();
        let body = span_bytes(|w| {
            w.write_array_len(1);
            w.write_array_len(2);
            w.write_map_len(1);
            w.write_str("trace_id");
            w.write_str("not a number");
            w.write_map_len(1);
            w.write_str("span_id");
            w.write_u64(3);
        });
        let batch = d.decode_traces_v04(&body, 0).unwrap();
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.events[0].span.as_ref().unwrap().span_id, 3u64.to_be_bytes());
        assert!(reasons(&registry).contains(&"malformed".to_string()));
        assert!(d.decode_traces_v04(b"\x91", 0).is_err(), "a truncated body is malformed");
        assert!(d.decode_traces_v04(b"\x80", 0).is_err(), "a map body is malformed");
    }

    #[test]
    fn v05_resolves_the_dictionary_and_checks_bounds() {
        let (mut d, _, registry) = with_registry();
        let body = span_bytes(|w| {
            w.write_array_len(2);
            w.write_array_len(4);
            for s in ["", "web", "env", "prod"] {
                w.write_str(s);
            }
            w.write_array_len(1);
            w.write_array_len(2);
            // A good span.
            w.write_array_len(12);
            for v in [1u64, 1, 0, 10, 11, 0, 5, 6, 0] {
                w.write_u64(v);
            }
            w.write_map_len(1);
            w.write_u64(2);
            w.write_u64(3);
            w.write_map_len(1);
            w.write_u64(2);
            w.write_f64(0.5);
            w.write_u64(1);
            // An index past the dictionary.
            w.write_array_len(12);
            w.write_u64(99);
            for _ in 0..8 {
                w.write_u64(0);
            }
            w.write_map_len(0);
            w.write_map_len(0);
            w.write_u64(0);
        });
        let batch = d.decode_traces_v05(&body, 0).unwrap();
        assert_eq!(batch.events.len(), 1);
        let e = &batch.events[0];
        assert_eq!(e.attributes.get(ATTR_SERVICE_NAME), Some(&Value::str("web")));
        assert_eq!(e.attributes.get(ATTR_SPAN_TYPE), Some(&Value::str("web")));
        assert_eq!(e.attributes.get(ATTR_RESOURCE_NAME), None, "index 0 is the empty string");
        // `env` is both a meta key and a metrics key here: the metric wins, counted.
        assert_eq!(e.attributes.get("env"), Some(&Value::F64(0.5)));
        let r = reasons(&registry);
        assert!(r.contains(&"malformed".to_string()));
        assert!(r.contains(&"key_collision".to_string()));
        let wrong_arity = span_bytes(|w| {
            w.write_array_len(3);
        });
        assert!(d.decode_traces_v05(&wrong_arity, 0).is_err());
    }

    #[test]
    fn v05_encode_builds_a_first_use_dictionary() {
        let (mut d, mut e, _) = with_registry();
        let body = v04(4, |w| {
            w.write_str("service");
            w.write_str("svc");
            w.write_str("name");
            w.write_str("svc");
            w.write_str("trace_id");
            w.write_u64(1);
            w.write_str("type");
            w.write_str("db");
        });
        let batch = d.decode_traces_v04(&body, 0).unwrap();
        let out = e.encode_traces_v05(&batch).unwrap();
        let mut r = Reader::new(&out);
        assert_eq!(r.read_array_len().unwrap(), 2);
        assert_eq!(r.read_array_len().unwrap(), 3);
        let dict: Vec<_> = (0..3).map(|_| r.read_str().unwrap().to_string()).collect();
        assert_eq!(dict, ["", "svc", "db"]);
        assert_eq!(d.decode_traces_v05(&out, 0).unwrap(), batch);
    }

    #[test]
    fn v07_chunk_and_payload_fields_become_carriers() {
        let (mut d, _, _) = with_registry();
        let body = span_bytes(|w| {
            w.write_map_len(4);
            w.write_str("language_name");
            w.write_str("python");
            w.write_str("tags");
            w.write_map_len(1);
            w.write_str("k");
            w.write_str("v");
            w.write_str("container_debug");
            w.write_map_len(1);
            w.write_str("latency_ms");
            w.write_i64(12);
            w.write_str("chunks");
            w.write_array_len(1);
            w.write_map_len(5);
            w.write_str("priority");
            w.write_i64(2);
            w.write_str("origin");
            w.write_str("lambda");
            w.write_str("dropped_trace");
            w.write_bool(true);
            w.write_str("tags");
            w.write_map_len(1);
            w.write_str("_dd.p.dm");
            w.write_str("-4");
            w.write_str("spans");
            w.write_array_len(1);
            w.write_map_len(1);
            w.write_str("span_id");
            w.write_u64(1);
        });
        let batch = d.decode_tracer_payload_v07(&body, 0).unwrap();
        let res = &batch.resource.attributes;
        assert_eq!(res.get(RESOURCE_ATTR_TRACER_LANGUAGE_NAME), Some(&Value::str("python")));
        let Some(Value::Map(tags)) = res.get(RESOURCE_ATTR_TRACER_TAGS) else { panic!() };
        assert_eq!(tags.get("k"), Some(&Value::str("v")));
        let Some(Value::Map(debug)) = res.get(RESOURCE_ATTR_TRACER_CONTAINER_DEBUG) else {
            panic!()
        };
        assert_eq!(debug.get("latency_ms"), Some(&Value::I64(12)));
        let e = &batch.events[0].attributes;
        assert_eq!(e.get(ATTR_CHUNK_PRIORITY), Some(&Value::I64(2)));
        assert_eq!(e.get(ATTR_CHUNK_ORIGIN), Some(&Value::str("lambda")));
        assert_eq!(e.get(ATTR_CHUNK_DROPPED_TRACE), Some(&Value::Bool(true)));
        assert!(matches!(e.get(ATTR_CHUNK_TAGS), Some(Value::Map(_))));

        let mut enc = DatadogEncoder::new();
        let out = enc.encode_tracer_payload_v07(&batch).unwrap();
        assert_eq!(d.decode_tracer_payload_v07(&out, 0).unwrap(), batch);
    }

    #[test]
    fn v04_drops_chunk_carriers_and_counts_them() {
        let (mut d, mut e, registry) = with_registry();
        let body = span_bytes(|w| {
            w.write_map_len(2);
            w.write_str("language_name");
            w.write_str("go");
            w.write_str("chunks");
            w.write_array_len(1);
            w.write_map_len(2);
            w.write_str("priority");
            w.write_i64(1);
            w.write_str("spans");
            w.write_array_len(1);
            w.write_map_len(0);
        });
        let batch = d.decode_tracer_payload_v07(&body, 0).unwrap();
        reasons(&registry);
        let out = e.encode_traces_v04(&batch).unwrap();
        let again = d.decode_traces_v04(&out, 0).unwrap();
        assert!(again.events[0].attributes.is_empty());
        let lost = counted(&registry, "logit.output.spans.degraded", ("reason", "no_wire_form"));
        assert_eq!(lost, 2.0, "the chunk priority and the tracer's language name");
    }

    #[test]
    fn a_batch_without_spans_encodes_to_nothing() {
        let mut e = DatadogEncoder::new();
        let batch = new_batch(Resource::default(), Vec::new());
        assert!(e.encode_traces_v04(&batch).is_none());
        assert!(e.encode_traces_v05(&batch).is_none());
        assert!(e.encode_tracer_payload_v07(&batch).is_none());
    }
}
