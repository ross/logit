//! `kafka_out`'s record encoder: one [`MessageBuf`] entry per record, its bytes the record's value
//! and its [`RecordMeta`] the key and timestamp (`docs/adr/kafka-output.md`, item 11 and
//! "Amendment: what W3 settled").
//!
//! | `format:` | `record: event` (default but for `native`) | `record: batch` (`native`'s default) |
//! |---|---|---|
//! | `json` | `crate::ndjson`'s object, without the newline | those objects as NDJSON, each line newline-terminated |
//! | `text` | the log body: a `Str` or `Bytes` as it is, a `Map` or `Array` in `crate::ndjson`'s value form, a timestamp as RFC 3339, another scalar as its `Display` text, `Null` as nothing | those bodies joined by `\n`, with none after the last |
//! | `msgpack` | `crate::msgpack`'s map | those maps back to back |
//! | `native` | a `CODEC_BATCH` frame of a one-event batch under the batch's resource and scope | one `CODEC_BATCH` frame of the events |
//! | `otlp_proto` | takes no `record:`: one signal's OTLP export request in protobuf, below | |
//! | `otlp_json` | takes no `record:`: the same request as OTLP/JSON, below | |
//!
//! `crate::ndjson`'s doc is the grammar of the JSON object and `crate::msgpack`'s the
//! MessagePack value forms. A native frame is uncompressed: `compression:` compresses the record
//! batch it travels in.
//!
//! `format: text` skips an event with no log record and counts it in
//! [`EncodeStats::no_log`]. An event whose body it writes leaves its metrics and span behind,
//! counted in [`EncodeStats::metrics_omitted`] and [`EncodeStats::spans_omitted`] at encode, so
//! a record the producer later drops as oversize, or the broker rejects or names, has still
//! counted them. An event dropped as `oversize` under `record: batch` counts only there. A text body that contains `\n`
//! can't be told from two bodies in a batch record.
//!
//! Under `record: batch`, events go into a record in batch order until the next would push its
//! value over the encoder's `budget`, which the sink sets so a value that fits never trips the
//! producer's `max_request_bytes` check ([`super::batch::value_budget`]); that event starts the
//! next record. An event whose own value is over the budget is dropped and counted in
//! [`EncodeStats::oversize`]. A native frame's size isn't the sum of its events' (the dictionary
//! is shared), so `native` finds each record's longest prefix of the remaining events that fits by
//! encoding candidates: the whole batch once, then for each record by doubling and bisecting. A
//! frame can't skip an event, so an event over the budget ends the open `native` record where the
//! other formats keep filling it. A batch record has no key, and its timestamp is its first
//! event's.
//!
//! Under `record: event`, a key is text, as UTF-8 bytes (`docs/adr/kafka-output.md`, item 3), the
//! same canonical bytes `sample` hashes for a key (`logit_core::sampling::hash_value`):
//!
//! | Value at the key | Key bytes |
//! |---|---|
//! | `trace_id`: the span's, else the log's `TraceRef` | 32 lowercase hex characters; an all-zero id is no value |
//! | `Str`, `Bytes` | as they are |
//! | `I64`, `U64` | decimal text |
//! | `F64` | Rust `Display`: the shortest decimal text that round-trips, never an exponent |
//! | `Bool` | `true` or `false` |
//! | `Timestamp` | decimal nanoseconds |
//! | `Null`, `Array`, `Map`, or no value | no key: the record goes unkeyed, counted in [`EncodeStats::missing_key`] |
//!
//! A record's timestamp is the event's, in milliseconds since the Unix epoch, rounded down.
//!
//! # OTLP formats
//!
//! `otlp_proto` and `otlp_json` write one record per OTLP export request
//! (`docs/adr/kafka-otlp-formats.md`); they differ only in the serialization of the request, and
//! the size budget measures that serialization's length.
//! A batch becomes one request per signal that has records, in the order logs, traces, metrics,
//! each built by `OtlpEncoder::request` from the batch's own events under its resource and
//! scope, and tagged with its signal in [`RecordMeta::signal`], which picks its topic. Two
//! things split a signal's request further, and nothing else does:
//!
//! - **The key.** `trace_id` splits logs and spans into one request per trace id, in order of
//!   first appearance, keyed as the per-event table's first row; each signal reads its own
//!   field, a log its `TraceRef` and a span its own id, so an event carrying both can key its
//!   two records differently. The events with none form one unkeyed request, which counts once
//!   per record in [`EncodeStats::missing_key`]. Metrics stay one unkeyed request and count
//!   nothing. `{resource: NAME}` keys every request of the batch with that value's text, or
//!   sends each unkeyed, counted. `{attribute: NAME}` is refused by the sink.
//! - **The size.** A request's budget is the encoder's less its key's length, the record overhead
//!   the producer's cap check adds for a key. A request over it is cut by events, each record the
//!   longest run of the remaining events that fits, found as `native` finds one. A single event
//!   whose request is over the budget alone is dropped, counted once in
//!   [`EncodeStats::oversize`] for each signal record it would have formed.
//!
//! The first encode of each key group is the one that counts the OTLP mapping's side effects
//! (`logit.output.metrics.skipped`, `dropped`, and `degraded`); every probe of the cut runs with
//! the gate [`KafkaEncoder::with_counters`] takes muted. A record's timestamp is the time the
//! encoder wrote it: a request holds many events, and one future-stamped event would otherwise
//! lose the whole request to Kafka's rule against records stamped more than an hour ahead.

use std::collections::HashMap;
use std::fmt::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use logit_core::interner::intern;
use logit_core::time::write_rfc3339_utc;
use logit_core::trace::push_hex;
use logit_core::{
    CountGate, Diagnostics, Event, EventBatch, Resource, Scope, Symbol, Telemetry, Value,
};
use logit_proto::frame::{write_frame, Compression};
use logit_proto::msgpack::Writer;
use logit_proto::native::{encode_events, CODEC_BATCH};
use logit_proto::otlp::{OtlpEncoder, OtlpRequest};
use logit_proto::{FramedEncoder, MessageBuf, Signal};

/// What each record's key is. Mirrors `logit_config::KafkaKey`, which `logit-cli` converts.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum RecordKey {
    #[default]
    None,
    TraceId,
    /// A top-level event attribute, named literally.
    Attribute(String),
    /// A resource attribute.
    Resource(String),
}

/// What each record's value holds. Mirrors `logit_config::KafkaFormat`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RecordFormat {
    #[default]
    Json,
    Text,
    Msgpack,
    Native,
    OtlpProto,
    OtlpJson,
}

impl RecordFormat {
    /// The record granularity a format has when `record:` isn't set.
    pub fn default_record(self) -> RecordMode {
        match self {
            RecordFormat::Json | RecordFormat::Text | RecordFormat::Msgpack => RecordMode::Event,
            RecordFormat::Native => RecordMode::Batch,
            RecordFormat::OtlpProto | RecordFormat::OtlpJson => RecordMode::Request,
        }
    }

    /// Whether each record holds one OTLP export request, a unit `record:` can't change.
    pub fn is_otlp(self) -> bool {
        self.default_record() == RecordMode::Request
    }
}

/// How many events one record holds. Mirrors `logit_config::KafkaRecord`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordMode {
    /// One event per record.
    Event,
    /// As many of a batch's events as fit under `max_request_bytes`.
    Batch,
    /// One OTLP export request of one signal: the OTLP formats' own unit, which `record:` can't
    /// select.
    Request,
}

/// [`RecordKey`] with its attribute name interned once.
#[derive(Debug, Clone, Copy)]
enum Compiled {
    None,
    TraceId,
    Attribute(Symbol),
    Resource(Symbol),
}

/// A record's key, timestamp, and signal; its value is the message's bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordMeta {
    pub key: Option<Bytes>,
    /// `CreateTime`, in milliseconds since the Unix epoch.
    pub timestamp: i64,
    /// The signal of an OTLP format's request, which picks its topic; `None` for the other
    /// formats, whose records all go to `topic:`.
    pub signal: Option<Signal>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct EncodeStats {
    /// Events sent unkeyed because a configured key had no value on them.
    pub missing_key: usize,
    /// Events `format: text` skipped for having no log record.
    pub no_log: usize,
    /// Metric records `format: text` left out of a record it wrote from the event's log.
    pub metrics_omitted: usize,
    /// Spans `format: text` left out of a record it wrote from the event's log.
    pub spans_omitted: usize,
    /// Events dropped under `record: batch` because their value alone is over the budget; under
    /// an OTLP format, one per signal of an event whose request alone is over it.
    pub oversize: usize,
}

/// `kafka_out`'s [`FramedEncoder`]. Public for the allocation pins in `logit-bench`.
pub struct KafkaEncoder {
    key: Compiled,
    format: RecordFormat,
    record: RecordMode,
    /// The most bytes one `record: batch` value may hold.
    budget: usize,
    /// A key's text; reused across events and batches.
    scratch: String,
    render: Render,
    /// The `record: batch` value being filled; reused.
    value: Vec<u8>,
    /// The OTLP mapping, on handles gated by `gate`.
    otlp: OtlpEncoder,
    /// Muted around every OTLP encode that repeats an event's mapping, so the mapping's
    /// side-effect counters count each event once.
    gate: CountGate,
}

/// One event's value for the per-event formats, in buffers reused across events and batches.
#[derive(Default)]
struct Render {
    text: String,
    body: Vec<u8>,
    msgpack: Writer,
    msgpack_scratch: String,
}

impl Render {
    /// `event`'s value under `format`, or `None` for an event `format: text` skips. `newline`
    /// ends a JSON object with `\n`, as an NDJSON line.
    fn value(
        &mut self,
        format: RecordFormat,
        newline: bool,
        resource: &Resource,
        scope: Option<&Scope>,
        event: &Event,
    ) -> Option<&[u8]> {
        match format {
            RecordFormat::Json => {
                self.text.clear();
                crate::ndjson::render_event(&mut self.text, resource, scope, event);
                if newline {
                    self.text.push('\n');
                }
                Some(self.text.as_bytes())
            }
            RecordFormat::Msgpack => {
                self.msgpack.clear();
                crate::msgpack::render_event(
                    &mut self.msgpack,
                    &mut self.msgpack_scratch,
                    resource,
                    scope,
                    event,
                );
                Some(self.msgpack.as_slice())
            }
            RecordFormat::Text => {
                let log = event.log.as_ref()?;
                self.body.clear();
                push_body(&mut self.body, &mut self.text, &log.message);
                Some(&self.body)
            }
            RecordFormat::Native => unreachable!("native frames go through `native_frame`"),
            RecordFormat::OtlpProto | RecordFormat::OtlpJson => {
                unreachable!("OTLP requests go through `otlp`")
            }
        }
    }
}

/// A log body's text, by the module doc's table.
fn push_body(out: &mut Vec<u8>, text: &mut String, message: &Value) {
    text.clear();
    match message {
        Value::Null => {}
        Value::Str(b) | Value::Bytes(b) => return out.extend_from_slice(b),
        Value::Bool(b) => {
            let _ = write!(text, "{b}");
        }
        Value::I64(n) => {
            let _ = write!(text, "{n}");
        }
        Value::U64(n) => {
            let _ = write!(text, "{n}");
        }
        Value::F64(n) => {
            let _ = write!(text, "{n}");
        }
        Value::Timestamp(ns) => write_rfc3339_utc(text, *ns),
        Value::Array(_) | Value::Map(_) => crate::ndjson::render_value(text, message),
    }
    out.extend_from_slice(text.as_bytes());
}

/// One `CODEC_BATCH` frame of `events`, or `None` past the frame's own payload cap.
fn native_frame(resource: &Resource, scope: Option<&Scope>, events: &[Event]) -> Option<Bytes> {
    write_frame(CODEC_BATCH, Compression::None, &encode_events(resource, scope, events)).ok()
}

fn millis(nanos: i64) -> i64 {
    nanos.div_euclid(1_000_000)
}

impl KafkaEncoder {
    /// `budget` caps a `record: batch` value; `record: event` leaves the cap to the producer.
    pub fn new(key: &RecordKey, format: RecordFormat, record: RecordMode, budget: usize) -> Self {
        let key = match key {
            RecordKey::None => Compiled::None,
            RecordKey::TraceId => Compiled::TraceId,
            RecordKey::Attribute(name) => Compiled::Attribute(intern(name)),
            RecordKey::Resource(name) => Compiled::Resource(intern(name)),
        };
        Self {
            key,
            format,
            record,
            budget,
            scratch: String::new(),
            render: Render::default(),
            value: Vec::new(),
            otlp: OtlpEncoder::new(),
            gate: CountGate::new(),
        }
    }

    /// Counts the OTLP mapping's side effects (`logit.output.metrics.degraded`, `skipped`, and
    /// `dropped`, and their warnings) through views of `telemetry` and `diag` gated by `gate`, the sink's
    /// batch accounting gate.
    pub fn with_counters(
        mut self,
        telemetry: &Telemetry,
        diag: &Diagnostics,
        gate: &CountGate,
    ) -> Self {
        self.otlp = OtlpEncoder::new()
            .with_telemetry(telemetry.gated(gate))
            .with_diagnostics(diag.gated(gate));
        self.gate = gate.clone();
        self
    }

    /// The key bytes for `event`; `Err(())` when a configured key has no value on it.
    fn key(&mut self, resource: &Resource, event: &Event) -> Result<Option<Bytes>, ()> {
        let value = match self.key {
            Compiled::None => return Ok(None),
            Compiled::TraceId => {
                let id = event
                    .span
                    .as_ref()
                    .map(|span| &span.trace_id)
                    .or_else(|| event.log.as_ref()?.trace.as_ref().map(|t| &t.trace_id))
                    .filter(|id| id.iter().any(|&b| b != 0))
                    .ok_or(())?;
                self.scratch.clear();
                push_hex(&mut self.scratch, id);
                return Ok(Some(Bytes::copy_from_slice(self.scratch.as_bytes())));
            }
            Compiled::Attribute(sym) => event.attributes.get_sym(sym),
            Compiled::Resource(sym) => resource.attributes.get_sym(sym),
        };
        key_bytes(value.ok_or(())?, &mut self.scratch).map(Some)
    }

    fn keyed_meta(
        &mut self,
        resource: &Resource,
        event: &Event,
        stats: &mut EncodeStats,
    ) -> RecordMeta {
        let key = self.key(resource, event).unwrap_or_else(|()| {
            stats.missing_key += 1;
            None
        });
        RecordMeta { key, timestamp: millis(event.timestamp), signal: None }
    }

    /// `record: event`: one record per event.
    fn per_event(
        &mut self,
        batch: &EventBatch,
        out: &mut MessageBuf<RecordMeta>,
        stats: &mut EncodeStats,
    ) {
        let scope = batch.scope.as_deref();
        for event in &batch.events {
            if self.format == RecordFormat::Text && event.log.is_none() {
                stats.no_log += 1;
                continue;
            }
            let meta = self.keyed_meta(&batch.resource, event, stats);
            if self.format == RecordFormat::Native {
                // A frame past its payload cap is far over any `max_request_bytes`: dropped, as
                // the producer would drop it.
                match native_frame(&batch.resource, scope, std::slice::from_ref(event)) {
                    Some(frame) => out.push_with(&frame, meta),
                    None => stats.oversize += 1,
                }
                continue;
            }
            let value = self
                .render
                .value(self.format, false, &batch.resource, scope, event)
                .expect("a text event here has a log record");
            out.push_with(value, meta);
            self.count_omitted(event, stats);
        }
    }

    /// Under `format: text`, counts the metrics and span `event` carries beside the log body
    /// its record holds.
    fn count_omitted(&self, event: &Event, stats: &mut EncodeStats) {
        if self.format == RecordFormat::Text {
            stats.metrics_omitted += event.metrics.len();
            stats.spans_omitted += usize::from(event.span.is_some());
        }
    }

    /// `record: batch` for `json`, `text`, and `msgpack`: values appended in batch order until
    /// the next would push the record over the budget.
    fn packed(
        &mut self,
        batch: &EventBatch,
        out: &mut MessageBuf<RecordMeta>,
        stats: &mut EncodeStats,
    ) {
        let scope = batch.scope.as_deref();
        let joined = self.format == RecordFormat::Text;
        let mut count = 0;
        let mut timestamp = 0;
        self.value.clear();
        for event in &batch.events {
            let Some(piece) = self.render.value(self.format, true, &batch.resource, scope, event)
            else {
                stats.no_log += 1;
                continue;
            };
            if piece.len() > self.budget {
                stats.oversize += 1;
                continue;
            }
            let separator = usize::from(joined && count > 0);
            if count > 0 && self.value.len() + separator + piece.len() > self.budget {
                out.push_with(&self.value, RecordMeta { key: None, timestamp, signal: None });
                self.value.clear();
                count = 0;
            }
            if count == 0 {
                timestamp = millis(event.timestamp);
            } else if joined {
                self.value.push(b'\n');
            }
            self.value.extend_from_slice(piece);
            count += 1;
            self.count_omitted(event, stats);
        }
        if count > 0 {
            out.push_with(&self.value, RecordMeta { key: None, timestamp, signal: None });
        }
    }

    /// `record: batch` for `native`: each record the longest run of the remaining events whose
    /// frame fits the budget. A frame only grows as events are added, so the search is sound.
    fn native_packed(
        &self,
        batch: &EventBatch,
        out: &mut MessageBuf<RecordMeta>,
        stats: &mut EncodeStats,
    ) {
        let scope = batch.scope.as_deref();
        let fits = |events: &[Event]| {
            native_frame(&batch.resource, scope, events).filter(|f| f.len() <= self.budget)
        };
        let mut rest = &batch.events[..];
        let Some(first) = rest.first() else { return };
        if let Some(frame) = fits(rest) {
            let meta = RecordMeta { key: None, timestamp: millis(first.timestamp), signal: None };
            out.push_with(&frame, meta);
            return;
        }
        // The first pass starts knowing the whole batch doesn't fit. Later passes find their
        // bound by doubling, because probing the whole rest again would cost every record an
        // encode of the remaining batch.
        let mut known_over = Some(rest.len());
        while let Some(first) = rest.first() {
            let meta = RecordMeta { key: None, timestamp: millis(first.timestamp), signal: None };
            // A run length that doesn't fit, or one past the end when none is known.
            let over_at_start = known_over.take().unwrap_or(rest.len() + 1);
            let Some(mut best) = fits(&rest[..1]) else {
                stats.oversize += 1;
                rest = &rest[1..];
                continue;
            };
            // `fit` events fit and `over` don't: double, then bisect between them.
            let (mut fit, mut over) = (1, over_at_start);
            let mut probe = 2;
            while probe < over && fit < rest.len() {
                let n = probe.min(rest.len());
                match fits(&rest[..n]) {
                    Some(frame) => {
                        (fit, best) = (n, frame);
                        probe *= 2;
                    }
                    None => over = n,
                }
            }
            while over - fit > 1 {
                let mid = fit + (over - fit) / 2;
                match fits(&rest[..mid]) {
                    Some(frame) => (fit, best) = (mid, frame),
                    None => over = mid,
                }
            }
            out.push_with(&best, meta);
            rest = &rest[fit..];
        }
    }
}

/// One OTLP request's worth of a signal's events, before the size cap cuts it.
struct Group<'e> {
    key: Option<Bytes>,
    /// Unkeyed because the configured key has no value here: each record counts in
    /// [`EncodeStats::missing_key`].
    missing: bool,
    events: Vec<&'e Event>,
}

/// What one candidate encode of a run of events gave.
enum Fit {
    /// The request fits the budget.
    /// The request, and its OTLP/JSON when the format is `otlp_json`, written once to measure it.
    Fits(OtlpRequest, Option<Vec<u8>>),
    /// No event of the run yields a record of the signal: a metric the mapping skips.
    Empty,
    Over,
}

/// The order an OTLP format writes a batch's signals in, `SignalEncoder::encode_signals`'s.
const SIGNALS: [Signal; 3] = [Signal::Logs, Signal::Traces, Signal::Metrics];

fn carries(signal: Signal, event: &Event) -> bool {
    match signal {
        Signal::Logs => event.log.is_some(),
        Signal::Traces => event.span.is_some(),
        Signal::Metrics => !event.metrics.is_empty(),
    }
}

/// The trace id `signal`'s record of `event` carries: a span's own, or the log record's
/// `TraceRef`. An all-zero id is none.
fn signal_trace_id(signal: Signal, event: &Event) -> Option<[u8; 16]> {
    let id = match signal {
        Signal::Logs => event.log.as_ref()?.trace.as_ref()?.trace_id,
        Signal::Traces => event.span.as_ref()?.trace_id,
        Signal::Metrics => return None,
    };
    id.iter().any(|&b| b != 0).then_some(id)
}

impl KafkaEncoder {
    /// The OTLP formats: one request per signal, split by key and then by the budget
    /// (`docs/adr/kafka-otlp-formats.md`, items 1, 5, 6, and 7).
    fn otlp(
        &mut self,
        batch: &EventBatch,
        out: &mut MessageBuf<RecordMeta>,
        stats: &mut EncodeStats,
    ) {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
        let timestamp = millis(i64::try_from(nanos).unwrap_or(i64::MAX));
        for signal in SIGNALS {
            let groups = self.groups(signal, batch);
            for group in groups {
                self.otlp_group(signal, batch, group, timestamp, out, stats);
            }
        }
    }

    /// `signal`'s events of `batch`, grouped by their key: one group per trace id, in order of
    /// first appearance, then the events with none; or one group for the batch.
    fn groups<'e>(&mut self, signal: Signal, batch: &'e EventBatch) -> Vec<Group<'e>> {
        let events = batch.events.iter().filter(|e| carries(signal, e));
        let one = |key: Option<Bytes>, missing: bool, events: Vec<&'e Event>| {
            if events.is_empty() {
                Vec::new()
            } else {
                vec![Group { key, missing, events }]
            }
        };
        match self.key {
            Compiled::TraceId if signal != Signal::Metrics => {
                let mut groups: Vec<Group<'e>> = Vec::new();
                let mut index: HashMap<[u8; 16], usize> = HashMap::new();
                let mut unkeyed = Vec::new();
                for event in events {
                    let Some(id) = signal_trace_id(signal, event) else {
                        unkeyed.push(event);
                        continue;
                    };
                    let at = *index.entry(id).or_insert_with(|| {
                        self.scratch.clear();
                        push_hex(&mut self.scratch, &id);
                        let key = Some(Bytes::copy_from_slice(self.scratch.as_bytes()));
                        groups.push(Group { key, missing: false, events: Vec::new() });
                        groups.len() - 1
                    });
                    groups[at].events.push(event);
                }
                groups.extend(one(None, true, unkeyed));
                groups
            }
            Compiled::Resource(sym) => {
                let key = batch
                    .resource
                    .attributes
                    .get_sym(sym)
                    .and_then(|value| key_bytes(value, &mut self.scratch).ok());
                one(key.clone(), key.is_none(), events.collect())
            }
            // `key: {attribute: ..}` is refused under the OTLP formats.
            _ => one(None, false, events.collect()),
        }
    }

    /// Writes `group` as records of `signal`: the whole request when it fits the budget less
    /// its key, else each the longest run of the remaining events that fits, found as
    /// `native_packed` finds one. The whole request's encode is the one that counts the
    /// mapping's side effects; every probe after it runs with the gate muted.
    fn otlp_group(
        &mut self,
        signal: Signal,
        batch: &EventBatch,
        group: Group<'_>,
        timestamp: i64,
        out: &mut MessageBuf<RecordMeta>,
        stats: &mut EncodeStats,
    ) {
        let Group { key, missing, events } = group;
        let budget = self.budget.saturating_sub(key.as_ref().map_or(0, Bytes::len));
        let (resource, scope) = (&batch.resource, batch.scope.as_deref());
        let emit = |(request, json): (OtlpRequest, Option<Vec<u8>>),
                    out: &mut MessageBuf<RecordMeta>,
                    stats: &mut EncodeStats| {
            let meta = RecordMeta { key: key.clone(), timestamp, signal: Some(signal) };
            match json {
                Some(json) => out.push_with(&json, meta),
                None => out.push_with(&request.encode_to_vec(), meta),
            };
            stats.missing_key += usize::from(missing);
        };
        let json = self.format == RecordFormat::OtlpJson;
        let otlp = &mut self.otlp;
        let mut fits =
            |events: &[&Event]| match otlp.request(signal, resource, scope, events.iter().copied())
            {
                None => Fit::Empty,
                Some(request) if json => {
                    let mut body = Vec::new();
                    request.write_json(&mut body);
                    if body.len() <= budget {
                        Fit::Fits(request, Some(body))
                    } else {
                        Fit::Over
                    }
                }
                Some(request) if request.encoded_len() <= budget => Fit::Fits(request, None),
                Some(_) => Fit::Over,
            };
        match fits(&events) {
            Fit::Fits(request, json) => return emit((request, json), out, stats),
            Fit::Empty => return,
            Fit::Over => {}
        }
        let gate = self.gate.clone();
        gate.muted(|| {
            let mut rest = &events[..];
            // The first pass starts knowing the whole group doesn't fit.
            let mut known_over = Some(rest.len());
            while !rest.is_empty() {
                let over_at_start = known_over.take().unwrap_or(rest.len() + 1);
                let mut best = match fits(&rest[..1]) {
                    Fit::Fits(request, json) => (request, json),
                    Fit::Empty => {
                        rest = &rest[1..];
                        continue;
                    }
                    Fit::Over => {
                        stats.oversize += 1;
                        rest = &rest[1..];
                        continue;
                    }
                };
                // `fit` events fit and `over` don't: double, then bisect between them. A run
                // starting with an event that yields a record never encodes empty.
                let (mut fit, mut over) = (1, over_at_start);
                let mut probe = 2;
                while probe < over && fit < rest.len() {
                    let n = probe.min(rest.len());
                    match fits(&rest[..n]) {
                        Fit::Fits(request, json) => {
                            (fit, best) = (n, (request, json));
                            probe *= 2;
                        }
                        Fit::Empty | Fit::Over => over = n,
                    }
                }
                while over - fit > 1 {
                    let mid = fit + (over - fit) / 2;
                    match fits(&rest[..mid]) {
                        Fit::Fits(request, json) => (fit, best) = (mid, (request, json)),
                        Fit::Empty | Fit::Over => over = mid,
                    }
                }
                emit(best, out, stats);
                rest = &rest[fit..];
            }
        });
    }
}

/// A value's key bytes, by the module doc's table; `Err(())` for one that can't be a key.
fn key_bytes(value: &Value, scratch: &mut String) -> Result<Bytes, ()> {
    scratch.clear();
    match value {
        Value::Str(b) | Value::Bytes(b) => return Ok(b.clone()),
        Value::I64(n) => write!(scratch, "{n}"),
        Value::U64(n) => write!(scratch, "{n}"),
        Value::F64(n) => write!(scratch, "{n}"),
        Value::Bool(b) => write!(scratch, "{b}"),
        Value::Timestamp(ns) => write!(scratch, "{ns}"),
        Value::Null | Value::Array(_) | Value::Map(_) => return Err(()),
    }
    .expect("writing to a String never fails");
    Ok(Bytes::copy_from_slice(scratch.as_bytes()))
}

impl FramedEncoder for KafkaEncoder {
    type Meta = RecordMeta;
    type Stats = EncodeStats;

    fn encode_into(&mut self, batch: &EventBatch, out: &mut MessageBuf<RecordMeta>) -> EncodeStats {
        out.clear();
        let mut stats = EncodeStats::default();
        match (self.record, self.format) {
            (RecordMode::Event, _) => self.per_event(batch, out, &mut stats),
            (RecordMode::Batch, RecordFormat::Native) => self.native_packed(batch, out, &mut stats),
            (RecordMode::Batch, _) => self.packed(batch, out, &mut stats),
            (RecordMode::Request, _) => self.otlp(batch, out, &mut stats),
        }
        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, LogRecord, TraceRef};

    fn batch(resource: AttrMap, events: Vec<Event>) -> EventBatch {
        let resource = Resource { attributes: resource, ..Resource::default() };
        EventBatch { resource: std::sync::Arc::new(resource), scope: None, events }
    }

    fn event(attributes: AttrMap) -> Event {
        Event::empty(1_700_000_000_123_456_789, attributes)
    }

    fn encode(key: RecordKey, batch: &EventBatch) -> (Vec<(String, RecordMeta)>, EncodeStats) {
        let mut encoder =
            KafkaEncoder::new(&key, RecordFormat::Json, RecordMode::Event, usize::MAX);
        let mut out = MessageBuf::default();
        let stats = encoder.encode_into(batch, &mut out);
        let records = out
            .iter_with()
            .map(|(v, m)| (String::from_utf8(v.to_vec()).unwrap(), m.clone()))
            .collect();
        (records, stats)
    }

    fn key_of(key: RecordKey, value: Value) -> Option<Bytes> {
        let mut attrs = AttrMap::new();
        attrs.insert("k", value);
        let (records, _) = encode(key, &batch(AttrMap::new(), vec![event(attrs)]));
        records[0].1.key.clone()
    }

    #[test]
    fn a_value_is_the_ndjson_object_without_its_newline() {
        let mut attrs = AttrMap::new();
        attrs.insert("a", Value::I64(1));
        let b = batch(AttrMap::new(), vec![event(attrs)]);
        let (records, stats) = encode(RecordKey::None, &b);
        let mut line = String::new();
        crate::ndjson::render_ndjson(&mut line, &b);
        assert_eq!(format!("{}\n", records[0].0), line);
        assert_eq!(
            records[0].1,
            RecordMeta { key: None, timestamp: 1_700_000_000_123, signal: None }
        );
        assert_eq!(stats.missing_key, 0, "no key configured is not a missing key");
    }

    #[test]
    fn a_timestamp_before_the_epoch_rounds_down() {
        let b = batch(AttrMap::new(), vec![Event::empty(-1, AttrMap::new())]);
        assert_eq!(encode(RecordKey::None, &b).0[0].1.timestamp, -1);
    }

    #[test]
    fn each_value_type_becomes_its_key_text() {
        let attr = || RecordKey::Attribute("k".into());
        let cases = [
            (Value::str("abc"), Some("abc")),
            (Value::Bytes(Bytes::from_static(b"\xff\x00")), Some("\u{0}")),
            (Value::I64(-7), Some("-7")),
            (Value::U64(200), Some("200")),
            (Value::F64(200.0), Some("200")),
            (Value::F64(0.1), Some("0.1")),
            (Value::Bool(true), Some("true")),
            (Value::Timestamp(42), Some("42")),
            (Value::Null, None),
            (Value::Array(vec![Value::I64(1)]), None),
            (Value::Map(Box::default()), None),
        ];
        for (value, want) in cases {
            let got = key_of(attr(), value.clone());
            match (value, want) {
                (Value::Bytes(b), _) => assert_eq!(got, Some(b)),
                (_, want) => assert_eq!(got.as_deref(), want.map(str::as_bytes), "{want:?}"),
            }
        }
    }

    #[test]
    fn a_missing_key_is_counted_and_the_record_goes_unkeyed() {
        let b = batch(AttrMap::new(), vec![event(AttrMap::new()), event(AttrMap::new())]);
        let (records, stats) = encode(RecordKey::Attribute("absent".into()), &b);
        assert_eq!(stats.missing_key, 2);
        assert!(records.iter().all(|(_, m)| m.key.is_none()));
    }

    #[test]
    fn a_resource_key_reads_the_batch_resource() {
        let mut resource = AttrMap::new();
        resource.insert("service.name", Value::str("api"));
        let b = batch(resource, vec![event(AttrMap::new())]);
        let (records, _) = encode(RecordKey::Resource("service.name".into()), &b);
        assert_eq!(records[0].1.key.as_deref(), Some(&b"api"[..]));
    }

    #[test]
    fn a_trace_id_key_is_lowercase_hex_and_all_zero_is_missing() {
        let id = [0xab; 16];
        let mut e = event(AttrMap::new());
        e.log = Some(LogRecord {
            message: Value::str("m"),
            severity: None,
            body_format: logit_core::BodyFormat::Raw,
            trace: Some(TraceRef { trace_id: id, span_id: None, flags: 0 }),
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        });
        let mut zero = e.clone();
        zero.log.as_mut().unwrap().trace.as_mut().unwrap().trace_id = [0; 16];
        let (records, stats) = encode(RecordKey::TraceId, &batch(AttrMap::new(), vec![e, zero]));
        assert_eq!(records[0].1.key.as_deref(), Some("ab".repeat(16).as_bytes()));
        assert_eq!(records[1].1.key, None);
        assert_eq!(stats.missing_key, 1);
    }

    // ---- formats and record modes -------------------------------------------------------

    use crate::msgpack::tests::{every_field_batch, json_tree, read_tree, same};
    use logit_proto::msgpack::Reader;

    fn encoder(format: RecordFormat, record: RecordMode, budget: usize) -> KafkaEncoder {
        KafkaEncoder::new(&RecordKey::None, format, record, budget)
    }

    /// Every record `encoder` writes for `batch`, and its stats.
    fn records(
        encoder: &mut KafkaEncoder,
        batch: &EventBatch,
    ) -> (Vec<(Vec<u8>, RecordMeta)>, EncodeStats) {
        let mut out = MessageBuf::default();
        let stats = encoder.encode_into(batch, &mut out);
        (out.iter_with().map(|(v, m)| (v.to_vec(), m.clone())).collect(), stats)
    }

    /// `event`'s NDJSON object, the reference every format is decoded back against.
    fn ndjson(batch: &EventBatch, event: &Event) -> String {
        let mut line = String::new();
        crate::ndjson::render_event(&mut line, &batch.resource, batch.scope.as_deref(), event);
        line
    }

    /// The events of every native frame in `records`, in order, each frame checked to be one
    /// `CODEC_BATCH` frame under the batch's resource and scope.
    fn native_events(batch: &EventBatch, records: &[(Vec<u8>, RecordMeta)]) -> Vec<Event> {
        let mut events = Vec::new();
        for (value, _) in records {
            let mut bytes = Bytes::copy_from_slice(value);
            let (codec, mut payload) = logit_proto::frame::read_frame(&mut bytes).unwrap();
            assert_eq!(codec, CODEC_BATCH);
            assert!(bytes.is_empty(), "one frame per record");
            let decoded =
                logit_proto::native::decode_batch(&mut payload, &Default::default()).unwrap();
            assert_eq!(decoded.resource, batch.resource);
            assert_eq!(decoded.scope, batch.scope);
            events.extend(decoded.events);
        }
        events
    }

    /// The events `format`'s records decode back to, as their NDJSON objects, so a NaN compares
    /// equal to itself. `text` decodes to bodies instead, which [`text_round_trips`] checks.
    fn decoded(
        format: RecordFormat,
        batch: &EventBatch,
        records: &[(Vec<u8>, RecordMeta)],
        record: RecordMode,
    ) -> Vec<String> {
        match format {
            RecordFormat::Json => records
                .iter()
                .flat_map(|(value, _)| {
                    let text = std::str::from_utf8(value).unwrap().to_string();
                    match record {
                        RecordMode::Event => vec![text],
                        RecordMode::Batch => {
                            assert!(text.ends_with('\n'), "NDJSON lines end with a newline");
                            text.lines().map(str::to_string).collect()
                        }
                        RecordMode::Request => unreachable!("json writes no request"),
                    }
                })
                .inspect(|line| {
                    serde_json::from_str::<serde_json::Value>(line).expect("each object parses");
                })
                .collect(),
            RecordFormat::Msgpack => {
                let mut trees = Vec::new();
                for (value, _) in records {
                    let mut r = Reader::new(value);
                    let before = trees.len();
                    while !r.is_empty() {
                        trees.push(read_tree(&mut r));
                    }
                    if record == RecordMode::Event {
                        assert_eq!(trees.len() - before, 1, "one map per event record");
                    }
                }
                // Each map carries what the event's NDJSON object does; the reference string
                // stands in for it once that holds.
                assert_eq!(trees.len(), batch.events.len());
                trees
                    .iter()
                    .zip(&batch.events)
                    .map(|(tree, event)| {
                        let line = ndjson(batch, event);
                        assert!(same(&json_tree(&line), tree), "{line}\n{tree:#?}");
                        line
                    })
                    .collect()
            }
            RecordFormat::Native => {
                native_events(batch, records).iter().map(|e| ndjson(batch, e)).collect()
            }
            RecordFormat::Text | RecordFormat::OtlpProto | RecordFormat::OtlpJson => unreachable!(),
        }
    }

    /// The `kafka_in` round-trip property, in-process: each format's records, in both record
    /// modes, decode back to the events that went in.
    #[test]
    fn each_format_round_trips_in_both_record_modes() {
        let batch = every_field_batch();
        let want: Vec<String> = batch.events.iter().map(|e| ndjson(&batch, e)).collect();
        for format in [RecordFormat::Json, RecordFormat::Msgpack, RecordFormat::Native] {
            for record in [RecordMode::Event, RecordMode::Batch] {
                let mut encoder = encoder(format, record, usize::MAX);
                let (records, stats) = records(&mut encoder, &batch);
                assert_eq!(stats, EncodeStats::default(), "{format:?} {record:?}");
                let expected_records = match record {
                    RecordMode::Event => batch.events.len(),
                    RecordMode::Batch | RecordMode::Request => 1,
                };
                assert_eq!(records.len(), expected_records, "{format:?} {record:?}");
                assert_eq!(
                    decoded(format, &batch, &records, record),
                    want,
                    "{format:?} {record:?}"
                );
            }
        }
    }

    fn log_event(timestamp: i64, message: Value) -> Event {
        Event::log(
            timestamp,
            AttrMap::new(),
            LogRecord {
                message,
                severity: None,
                body_format: logit_core::BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        )
    }

    /// `format: text` writes each log body by the module doc's table, joins them with newlines
    /// under `record: batch`, and skips and counts an event with no log record in both modes.
    #[test]
    fn text_round_trips() {
        let mut map = AttrMap::new();
        map.insert("text_test.k", Value::str("v"));
        let bodies = [
            (Value::str("GET / 200"), &b"GET / 200"[..]),
            (Value::Bytes(Bytes::from_static(b"raw\xff")), b"raw\xff"),
            (Value::I64(-3), b"-3"),
            (Value::F64(0.5), b"0.5"),
            (Value::Bool(false), b"false"),
            (Value::Null, b""),
            (Value::Timestamp(1_000_000_001), b"1970-01-01T00:00:01.000000001Z"),
            (Value::Map(Box::new(map)), br#"{"text_test.k":"v"}"#),
            (Value::Array(vec![Value::I64(1), Value::str("a")]), br#"[1,"a"]"#),
        ];
        let mut events: Vec<Event> = bodies
            .iter()
            .enumerate()
            .map(|(i, (v, _))| log_event(i as i64 * 1_000_000, v.clone()))
            .collect();
        events.insert(2, Event::empty(0, AttrMap::new()));
        let b = batch(AttrMap::new(), events);

        let (records_out, stats) =
            records(&mut encoder(RecordFormat::Text, RecordMode::Event, usize::MAX), &b);
        assert_eq!(stats.no_log, 1, "the event with no log record");
        let got: Vec<&[u8]> = records_out.iter().map(|(v, _)| &v[..]).collect();
        let want: Vec<&[u8]> = bodies.iter().map(|(_, text)| *text).collect();
        assert_eq!(got, want);
        assert_eq!(records_out[1].1.timestamp, 1, "each record keeps its event's timestamp");

        let (batched, stats) =
            records(&mut encoder(RecordFormat::Text, RecordMode::Batch, usize::MAX), &b);
        assert_eq!(stats.no_log, 1);
        assert_eq!(batched.len(), 1);
        assert_eq!(batched[0].0, want.join(&b'\n'));
        assert_eq!(batched[0].1, RecordMeta { key: None, timestamp: 0, signal: None });
    }

    /// A log event that also carries two metric records and a span.
    fn mixed_event(message: &str) -> Event {
        let mut event = log_event(0, Value::str(message));
        for name in ["a", "b"] {
            event.metrics.push(logit_core::MetricRecord::new(
                intern(name),
                logit_core::MetricKind::Gauge(1.0.into()),
            ));
        }
        event.span = Some(logit_core::SpanRecord {
            trace_id: [1; 16],
            span_id: [1; 8],
            parent_span_id: None,
            name: Value::str("s"),
            kind: logit_core::SpanKind::Internal,
            status: logit_core::SpanStatus::Unset,
            events: vec![],
            links: vec![],
            end_timestamp: 1,
            flags: 0,
            ext: None,
        });
        event
    }

    /// `format: text` counts the metrics and span of each event whose body it writes, in both
    /// record modes; an event skipped for no log or dropped as `oversize` counts none, and no
    /// other format counts any.
    #[test]
    fn text_counts_the_metrics_and_span_it_leaves_out_of_a_written_record() {
        let mut metric_only = log_event(0, Value::Null);
        metric_only.log = None;
        metric_only.metrics.push(logit_core::MetricRecord::new(
            intern("m"),
            logit_core::MetricKind::Gauge(1.0.into()),
        ));
        let b = batch(
            AttrMap::new(),
            vec![mixed_event("one"), metric_only, mixed_event(&"x".repeat(64)), mixed_event("two")],
        );

        let (out, stats) =
            records(&mut encoder(RecordFormat::Text, RecordMode::Event, usize::MAX), &b);
        assert_eq!(out.len(), 3);
        assert_eq!((stats.no_log, stats.metrics_omitted, stats.spans_omitted), (1, 6, 3));

        let (out, stats) = records(&mut encoder(RecordFormat::Text, RecordMode::Batch, 32), &b);
        assert_eq!(out.len(), 1);
        assert_eq!(stats.oversize, 1, "the 64-byte body");
        assert_eq!((stats.no_log, stats.metrics_omitted, stats.spans_omitted), (1, 4, 2));

        for format in [RecordFormat::Json, RecordFormat::Msgpack] {
            let (_, stats) = records(&mut encoder(format, RecordMode::Event, usize::MAX), &b);
            assert_eq!((stats.metrics_omitted, stats.spans_omitted), (0, 0), "{format:?}");
        }
    }

    /// Under `record: batch`, each record takes events until the next would push it over the
    /// budget, an event over the budget alone is dropped and counted, and a record's timestamp
    /// is its first event's.
    #[test]
    fn a_batch_record_fills_to_the_budget_and_drops_an_event_over_it_alone() {
        let line = |e: &Event, b: &EventBatch| ndjson(b, e).len() + 1;
        let events: Vec<Event> =
            (0..5).map(|i| log_event(i * 1_000_000, Value::str("abcd"))).collect();
        let mut events_with_big = events.clone();
        events_with_big.insert(2, log_event(99_000_000, Value::str("x".repeat(500))));
        let b = batch(AttrMap::new(), events_with_big);
        let one = line(&b.events[0], &b);
        // Room for two lines, one byte short of three.
        let budget = 3 * one - 1;
        let (out, stats) = records(&mut encoder(RecordFormat::Json, RecordMode::Batch, budget), &b);
        assert_eq!(stats.oversize, 1, "the 500-byte event");
        let lines: Vec<usize> =
            out.iter().map(|(v, _)| v.iter().filter(|&&c| c == b'\n').count()).collect();
        assert_eq!(lines, vec![2, 2, 1]);
        assert!(out.iter().all(|(v, _)| v.len() <= budget));
        let stamps: Vec<i64> = out.iter().map(|(_, m)| m.timestamp).collect();
        assert_eq!(stamps, vec![0, 2, 4], "each record's first event");
        assert!(out.iter().all(|(_, m)| m.key.is_none()));

        // A value of the budget's length fits: two whole lines.
        let (out, _) = records(&mut encoder(RecordFormat::Json, RecordMode::Batch, 2 * one), &b);
        assert_eq!(out[0].0.len(), 2 * one);

        // Text counts the newline between bodies; msgpack values abut.
        let (out, _) = records(
            &mut encoder(RecordFormat::Text, RecordMode::Batch, 9),
            &batch(AttrMap::new(), events.clone()),
        );
        let values: Vec<&[u8]> = out.iter().map(|(v, _)| &v[..]).collect();
        assert_eq!(values, vec![&b"abcd\nabcd"[..], b"abcd\nabcd", b"abcd"]);
        let (out, _) = records(
            &mut encoder(RecordFormat::Text, RecordMode::Batch, 8),
            &batch(AttrMap::new(), events.clone()),
        );
        assert_eq!(out.len(), 5, "8 bytes hold one body, not two and a newline");
    }

    /// `native` under `record: batch` cuts the events into frames that each fit the budget and
    /// hold as many events as do: one more would not fit.
    #[test]
    fn a_native_batch_record_holds_the_longest_run_that_fits() {
        let events: Vec<Event> = (0..37)
            .map(|i| log_event(i * 1_000_000, Value::str("y".repeat(10 + (i as usize * 7) % 40))))
            .collect();
        let b = batch(AttrMap::new(), events);
        let frame = |events: &[Event]| native_frame(&b.resource, None, events).unwrap().len();
        let budget = frame(&b.events[..5]);
        let (out, stats) =
            records(&mut encoder(RecordFormat::Native, RecordMode::Batch, budget), &b);
        assert_eq!(stats, EncodeStats::default());
        assert!(out.len() > 1);
        let mut start = 0;
        for (value, meta) in &out {
            assert!(value.len() <= budget);
            let n = native_events(&b, std::slice::from_ref(&(value.clone(), meta.clone()))).len();
            assert_eq!(meta.timestamp, millis(b.events[start].timestamp));
            if start + n < b.events.len() {
                assert!(
                    frame(&b.events[start..start + n + 1]) > budget,
                    "record at {start} stopped short"
                );
            }
            start += n;
        }
        assert_eq!(start, b.events.len());
        let got: Vec<String> = native_events(&b, &out).iter().map(|e| ndjson(&b, e)).collect();
        let want: Vec<String> = b.events.iter().map(|e| ndjson(&b, e)).collect();
        assert_eq!(got, want, "every event, in order");

        // One event over the budget alone is dropped and counted; the rest still go.
        let mut big = b.events[..3].to_vec();
        big.insert(1, log_event(0, Value::str("z".repeat(2 * budget))));
        let b2 = batch(AttrMap::new(), big);
        let (out, stats) =
            records(&mut encoder(RecordFormat::Native, RecordMode::Batch, budget), &b2);
        assert_eq!(stats.oversize, 1);
        assert_eq!(native_events(&b2, &out).len(), 3);
    }

    /// An event over the budget mid-batch ends a `native` record, because a frame can't skip an
    /// event, while the in-order formats drop it and keep filling the open record.
    #[test]
    fn an_oversize_event_mid_batch_ends_a_native_record_but_not_a_json_one() {
        let small = |i: i64| log_event(i * 1_000_000, Value::str("abcd"));
        let big = log_event(9_000_000, Value::str("x".repeat(4096)));
        let b = batch(AttrMap::new(), vec![small(0), small(1), big, small(2)]);
        let abc = [b.events[0].clone(), b.events[1].clone(), b.events[3].clone()];
        let groups = |format, budget| {
            let (out, stats) = records(&mut encoder(format, RecordMode::Batch, budget), &b);
            assert_eq!(stats.oversize, 1);
            out.iter()
                .map(|r| decoded(format, &b, std::slice::from_ref(r), RecordMode::Batch))
                .collect::<Vec<_>>()
        };
        let objects = |events: &[Event]| events.iter().map(|e| ndjson(&b, e)).collect::<Vec<_>>();

        let native_budget = native_frame(&b.resource, None, &abc).unwrap().len();
        assert_eq!(
            groups(RecordFormat::Native, native_budget),
            vec![objects(&abc[..2]), objects(&abc[2..])],
        );
        let json_budget: usize = abc.iter().map(|e| ndjson(&b, e).len() + 1).sum();
        assert_eq!(groups(RecordFormat::Json, json_budget), vec![objects(&abc)]);
    }

    /// `record: event` keys each record in every format; a batch record never has one.
    #[test]
    fn every_format_keys_its_event_records() {
        let mut attrs = AttrMap::new();
        attrs.insert("k", Value::str("abc"));
        let mut e = log_event(0, Value::str("m"));
        e.attributes = attrs;
        let b = batch(AttrMap::new(), vec![e]);
        for format in
            [RecordFormat::Json, RecordFormat::Text, RecordFormat::Msgpack, RecordFormat::Native]
        {
            let mut encoder = KafkaEncoder::new(
                &RecordKey::Attribute("k".into()),
                format,
                RecordMode::Event,
                usize::MAX,
            );
            let (out, _) = records(&mut encoder, &b);
            assert_eq!(out[0].1.key.as_deref(), Some(&b"abc"[..]), "{format:?}");
        }
    }

    /// The OTLP formats share `otlp_out`'s encoder, so they read the neutral type flag: an `info`
    /// gauge is a non-monotonic `Sum` named `_info`, a flagged gauge histogram is skipped, and
    /// `prometheus_in`'s own carrier changes nothing.
    #[test]
    fn the_otlp_formats_read_the_neutral_type_flag_and_not_the_prometheus_carrier() {
        use logit_core::attrs::METRICS_TYPE;
        use logit_core::{Histogram, MetricKind, MetricRecord, Temporality};
        let metric = |name: &str, key: &str, kind: MetricKind| {
            let mut attributes = AttrMap::new();
            attributes.insert(key, Value::str(if name == "h" { "gaugehistogram" } else { "info" }));
            Event::metric(0, attributes, MetricRecord::new(intern(name), kind))
        };
        let histogram = MetricKind::Histogram(Histogram {
            buckets: vec![(1.0, 2), (f64::INFINITY, 1)],
            temporality: Temporality::Cumulative,
            sum: Some(3.0),
            min: None,
            max: None,
        });
        let b = batch(
            AttrMap::new(),
            vec![
                metric("flagged", METRICS_TYPE, MetricKind::gauge(1.0)),
                metric("carried", logit_proto::prometheus::ATTR_TYPE, MetricKind::gauge(1.0)),
                metric("h", METRICS_TYPE, histogram),
            ],
        );
        let (out, _) =
            records(&mut encoder(RecordFormat::OtlpJson, RecordMode::Request, usize::MAX), &b);
        assert_eq!(out.len(), 1, "one metrics request");
        let request: serde_json::Value = serde_json::from_slice(&out[0].0).unwrap();
        let metrics = request["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| {
                let data = ["gauge", "sum", "histogram"].into_iter().find(|k| m.get(*k).is_some());
                (m["name"].as_str().unwrap().to_string(), data.unwrap())
            })
            .collect::<Vec<_>>();
        assert_eq!(
            metrics,
            [("flagged_info".to_string(), "sum"), ("carried".to_string(), "gauge")],
            "the gauge histogram is skipped"
        );
    }

    /// The soak harness's protobuf walker (`tools/soak/soaklib/otlp.py`) is checked against
    /// this batch's `otlp_proto` record, committed as hex. The hex must be what the encoder
    /// writes; on a mismatch the message holds the encoder's hex to commit in its place.
    #[test]
    fn the_soak_otlp_metrics_vector_is_what_the_encoder_writes() {
        use logit_core::{Histogram, MetricKind, MetricRecord, MetricValue, Sum, Temporality};
        const TS: i64 = 1_791_720_000_250_000_000;
        let metric = |name: &str, attributes: AttrMap, kind: MetricKind| {
            Event::metric(TS, attributes, MetricRecord::new(intern(name), kind))
        };
        let sum = |value, monotonic| {
            MetricKind::Sum(Sum { value, temporality: Temporality::Delta, monotonic })
        };
        let mut shard = AttrMap::new();
        shard.insert("shard", Value::str("b"));
        let mut resource = AttrMap::new();
        resource.insert("host.name", Value::str("soak"));
        let b = batch(
            resource,
            vec![
                metric("soak_requests_1", AttrMap::new(), sum(MetricValue::I64(30), true)),
                metric("soak_requests_2", shard, sum(MetricValue::F64(2.5), true)),
                metric("soak_gauge", AttrMap::new(), MetricKind::gauge(9.0)),
                metric(
                    "soak_latency",
                    AttrMap::new(),
                    MetricKind::Histogram(Histogram {
                        buckets: vec![(1.0, 2), (f64::INFINITY, 3)],
                        temporality: Temporality::Delta,
                        sum: Some(4.0),
                        min: None,
                        max: None,
                    }),
                ),
                metric("soak_requests_3", AttrMap::new(), sum(MetricValue::I64(-4), false)),
            ],
        );
        let mut encoder = encoder(RecordFormat::OtlpProto, RecordMode::Request, usize::MAX);
        let (out, _) = records(&mut encoder, &b);
        assert_eq!(out.len(), 1, "one metrics request");
        let hex: String = out[0].0.iter().map(|byte| format!("{byte:02x}")).collect();
        let committed: String =
            include_str!("../../../../tools/soak/vectors/kafka-otlp-metrics.hex")
                .lines()
                .filter(|line| !line.starts_with('#'))
                .flat_map(str::split_whitespace)
                .collect();
        assert_eq!(hex, committed, "commit the encoder's hex: {hex}");
    }
}
