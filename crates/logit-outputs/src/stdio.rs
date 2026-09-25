//! `stdio_out`: a human-facing debug sink that writes a pipeline's events as readable text to
//! stdout (default), stderr, or a file. Also home of [`StreamOutput`], which `file_out`
//! (`crate::file`) builds on: `stdio_out`'s file target is `file_out` with an empty rotation
//! policy, not a second implementation (`docs/adr/rotating-file-output.md`).
//!
//! **The render is a readable text block per event, not JSON.** It's for a person at a terminal,
//! not an export format; an NDJSON [`Format`] variant is the extension point if a consumer needs
//! one. See `docs/plans/nginx-integration.md`'s workstream D and `docs/known-gaps.md` for the
//! accepted consequences. `tools/shape-survey/summarize.py` parses this render, [`render_value`]'s
//! arrays and maps included, so a change to how anything renders must be mirrored there.
//!
//! Split like `InfluxDbOutput`/`InfluxLineEncoder`: a pure [`EventDump`] encoder with no file
//! descriptor (every format test runs against it alone), and the thin [`StreamOutput`] that owns
//! the target. [`StreamEncoder`] picks `EventDump` or `logit_proto::native::NativeEncoder`
//! (`format: native`, `docs/adr/file-output-native-format.md`); `Target`/`FileTarget` never see
//! which.
//!
//! [`Format`] has one variant, and `render_value`/`render_metric` stay free functions, so a future
//! `format:` template or NDJSON variant is a new `Format` arm calling the same renderers.
//!
//! Every string rendered here (a value, an attribute/map key, a metric or unit name) can come from
//! attacker-influenced input such as a syslog line or a JSON body, so each goes through
//! [`render_quoted_str`] or [`render_key`]. They escape every C0 control character and DEL, so an
//! embedded ESC can't drive the viewer's terminal, and quote any key that isn't identifier-shaped,
//! so a space, `=`, or newline can't forge extra tokens or a fake line.

use crate::file::{FileTarget, RotateOutcome, RotatePolicy};
use crate::Output;
use anyhow::Context;
use bytes::Bytes;
use logit_core::interner::resolve;
use logit_core::time::format_rfc3339_utc;
use logit_core::trace::push_hex;
use logit_core::{
    AttrMap, Diagnostics, Event, EventBatch, MetricKind, MetricRecord, Resource, Severity,
    SpanEvent, SpanLink, SpanRecord, Telemetry, Value,
};
use logit_proto::frame::Compression as NativeCompression;
use logit_proto::native::NativeEncoder;
use logit_proto::{CodecError, Encoder};
use std::cmp::Ordering;
// For `write!` straight into the output buffer, never a per-number `to_string()`/`format!`
// (`docs/design/memory.md`).
use std::fmt::Write;
use std::path::Path;
use tokio::io::{self, AsyncWriteExt};

/// The output format [`EventDump`] renders. One variant; the module doc says why it's an enum.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    #[default]
    Human,
}

/// Renders an [`EventBatch`] as readable text. Pure: no file descriptor, no I/O.
#[derive(Debug, Default, Clone, Copy)]
pub struct EventDump {
    format: Format,
}

impl EventDump {
    pub fn new(format: Format) -> Self {
        Self { format }
    }

    /// Renders `batch` as one readable block per event, in batch order.
    ///
    /// Never fails and never panics: a debug sink has to stay up when everything else is falling
    /// over, so even a non-finite numeric value renders something.
    ///
    /// `&self` with no scratch buffers on [`EventDump`], unlike `InfluxLineEncoder`
    /// (`docs/design/memory.md`): that encoder stages a line before committing it, but every
    /// `render_*` here writes straight into `out`, so there is nothing to hoist. `out` is a fresh
    /// `String` per call, as `InfluxLineEncoder::encode`'s `buf` is.
    ///
    /// Named `render`, not `encode`: Rust resolves an inherent method over a same-named trait
    /// method with no ambiguity error, so an inherent `encode` would keep every `dump.encode(..)`
    /// call site on this `String`-returning method instead of `Encoder::encode`.
    pub fn render(&self, batch: &EventBatch) -> String {
        match self.format {
            Format::Human => {
                let mut out = String::new();
                for (i, event) in batch.events.iter().enumerate() {
                    if i > 0 {
                        out.push('\n');
                    }
                    render_event_block(&mut out, &batch.resource, event);
                }
                out
            }
        }
    }
}

/// What lets [`StreamOutput`] be generic over its encoder. Always `Ok`: [`EventDump::render`]
/// never fails.
impl Encoder for EventDump {
    fn encode(&mut self, batch: &EventBatch) -> Result<Bytes, CodecError> {
        Ok(Bytes::from(self.render(batch).into_bytes()))
    }
}

/// Which encoder [`StreamOutput`] writes through: [`EventDump`]'s text render, or
/// `logit_proto::native::NativeEncoder` (`docs/adr/file-output-native-format.md`).
///
/// An enum, not `Box<dyn Encoder>`, so `StreamOutput<StreamEncoder>` is the one concrete type
/// `build_spec` constructs. Both encoders are `Copy` and carry no state across calls:
/// `NativeEncoder` rebuilds its dictionary inside every `encode()`, so each frame decodes on its
/// own, which is what lets `file_out` rotate mid-stream without stranding a reader.
#[derive(Debug, Clone, Copy)]
pub enum StreamEncoder {
    Human(EventDump),
    Native(NativeEncoder),
}

impl StreamEncoder {
    pub fn human() -> Self {
        StreamEncoder::Human(EventDump::default())
    }

    pub fn native(compression: NativeCompression) -> Self {
        StreamEncoder::Native(NativeEncoder::new(compression))
    }
}

impl Encoder for StreamEncoder {
    fn encode(&mut self, batch: &EventBatch) -> Result<Bytes, CodecError> {
        match self {
            StreamEncoder::Human(e) => e.encode(batch),
            StreamEncoder::Native(e) => e.encode(batch),
        }
    }
}

/// One event's block: a timestamp/log line, then `attrs`/`metric`/`span` lines, each omitted when
/// the event carries nothing for that section. Always ends with `\n`, and always emits the
/// timestamp line, so an empty event (legal under `docs/adr/multi-payload-events.md`) is still
/// visible.
///
/// `attrs` merges `resource`'s attributes underneath the event's own, as influxdb's
/// `render_tag_suffix` does; otherwise events from different hosts or services would render
/// byte-identically.
fn render_event_block(out: &mut String, resource: &Resource, event: &Event) {
    out.push_str(&format_rfc3339_utc(event.timestamp));
    if let Some(log) = &event.log {
        let severity = log.severity.map(Severity::as_str).unwrap_or("-");
        out.push_str(" log[");
        out.push_str(severity);
        out.push_str("] ");
        render_value(out, &log.message);
        // The log's application trace context (`docs/adr/log-record-trace-context.md`), not the
        // `span` section below; absent unless a decoder, `trace_context`, or a script set it.
        if let Some(trace) = log.trace {
            out.push_str(" trace_id=");
            push_hex(out, &trace.trace_id);
            if let Some(span_id) = trace.span_id {
                out.push_str(" span_id=");
                push_hex(out, &span_id);
            }
            if trace.flags != 0 {
                let _ = write!(out, " flags={}", trace.flags);
            }
        }
    }
    out.push('\n');

    if !resource.attributes.is_empty() || !event.attributes.is_empty() {
        out.push_str("  attrs   ");
        render_merged_attrs(out, &resource.attributes, &event.attributes);
        out.push('\n');
    }

    for metric in &event.metrics {
        out.push_str("  metric  ");
        render_metric(out, metric);
        out.push('\n');
    }

    if let Some(span) = &event.span {
        out.push_str("  span    ");
        render_span(out, event.timestamp, span);
        out.push('\n');
        for span_event in &span.events {
            out.push_str("  span_event ");
            render_span_event(out, span_event);
            out.push('\n');
        }
        for link in &span.links {
            out.push_str("  span_link ");
            render_span_link(out, link);
            out.push('\n');
        }
    }
}

/// Space-separated `key=value` pairs in `AttrMap`'s own (sorted-by-`Symbol`, deterministic)
/// order; don't re-sort.
fn render_attrs(out: &mut String, attrs: &AttrMap) {
    for (i, (key, value)) in attrs.iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        render_key(out, resolve(key));
        out.push('=');
        render_value(out, value);
    }
}

/// Renders `resource`'s attributes merged with `event`'s as space-separated `key=value` pairs,
/// the event's value winning on a key collision.
///
/// A merge-join, not a clone of `resource` with `event` inserted over it: both iterate in
/// sorted-`Symbol` order, so walking them in lockstep gives the same sequence with no per-event
/// `AttrMap` copy and no `resolve` -> `intern` round trip. Mirrors influxdb's `render_tag_suffix`.
fn render_merged_attrs(out: &mut String, resource: &AttrMap, event: &AttrMap) {
    let mut resource_attrs = resource.iter().peekable();
    let mut event_attrs = event.iter().peekable();
    let mut first = true;

    loop {
        let next =
            match (resource_attrs.peek().map(|(k, _)| *k), event_attrs.peek().map(|(k, _)| *k)) {
                (Some(r), Some(e)) => match r.cmp(&e) {
                    Ordering::Less => resource_attrs.next(),
                    Ordering::Greater => event_attrs.next(),
                    // Same key on both: the event's value wins, and the resource's is discarded.
                    Ordering::Equal => {
                        resource_attrs.next();
                        event_attrs.next()
                    }
                },
                (Some(_), None) => resource_attrs.next(),
                (None, Some(_)) => event_attrs.next(),
                (None, None) => break,
            };
        let Some((key, value)) = next else { break };

        if !first {
            out.push(' ');
        }
        first = false;
        render_key(out, resolve(key));
        out.push('=');
        render_value(out, value);
    }
}

/// Trailing ` sum=`/` min=`/` max=` for whichever of the three is `Some`; an absent field renders
/// as nothing, never `sum=None`.
fn render_optional_sum_min_max(
    out: &mut String,
    sum: Option<f64>,
    min: Option<f64>,
    max: Option<f64>,
) {
    if let Some(sum) = sum {
        let _ = write!(out, " sum={sum}");
    }
    if let Some(min) = min {
        let _ = write!(out, " min={min}");
    }
    if let Some(max) = max {
        let _ = write!(out, " max={max}");
    }
}

/// `<name> <kind-specific fields>`, plus a trailing ` unit=<unit>` when the metric has one.
/// Most kinds lead with `<kind>=` (`sum=`, `gauge=`, `gauge_delta=`, `set=`, `samples=[..]`,
/// `set_members=[..]`); `distribution`, `histogram`, `exp_histogram`, and `summary` lead with the
/// bare kind name. Any further fields follow as space-separated `field=value` pairs.
fn render_metric(out: &mut String, metric: &MetricRecord) {
    render_key(out, resolve(metric.name));
    out.push(' ');
    if metric.is_no_recorded_value() {
        // OTLP `NO_RECORDED_VALUE`: never dropped, since a debug sink must show it. Render the
        // flag, not the kind's meaningless default value (`docs/known-gaps.md`'s cross-protocol
        // table, `crates/logit-core/src/metric.rs`'s `flags` doc).
        out.push_str("no_recorded_value");
        if let Some(unit) = metric.unit {
            out.push_str(" unit=");
            render_key(out, resolve(unit));
        }
        return;
    }
    match &metric.kind {
        MetricKind::Sum(s) => {
            let _ = write!(
                out,
                "sum={} temporality={} monotonic={}",
                s.value,
                s.temporality.as_str(),
                s.monotonic
            );
        }
        MetricKind::Gauge(v) => {
            out.push_str("gauge=");
            let _ = write!(out, "{v}");
        }
        MetricKind::GaugeDelta(v) => {
            // `gauge_delta`, with an explicit sign, so an unresolved relative adjustment never
            // reads as an absolute value (`docs/adr/relative-gauge-adjustments.md`).
            out.push_str("gauge_delta=");
            // `is_sign_positive`, not `*v >= 0.0`: `-0.0 >= 0.0` is true, but `Display` prints
            // `-0`, so the comparison would produce `+-0`.
            if v.is_sign_positive() {
                out.push('+');
            }
            let _ = write!(out, "{v}");
        }
        MetricKind::Distribution(sketch) => {
            out.push_str("distribution count=");
            let _ = write!(out, "{}", sketch.count());
            for q in [0.5, 0.9, 0.99] {
                if let Some(v) = sketch.quantile(q) {
                    let _ = write!(out, " p{}={v}", (q * 100.0).round() as u32);
                }
            }
        }
        MetricKind::Samples(s) => {
            out.push_str("samples=[");
            for (i, v) in s.values.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                let _ = write!(out, "{v}");
            }
            let _ = write!(out, "] rate={}", s.sample_rate);
        }
        MetricKind::SetMembers(members) => {
            // Members are arbitrary wire bytes, so each is quoted and escaped like any other
            // string (module doc); an embedded newline would otherwise forge an output line.
            out.push_str("set_members=[");
            for (i, m) in members.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                render_quoted_str(out, &String::from_utf8_lossy(m));
            }
            out.push(']');
        }
        MetricKind::Histogram(h) => {
            out.push_str("histogram");
            for (bound, count) in &h.buckets {
                let _ = write!(out, " bucket_{bound}={count}");
            }
            render_optional_sum_min_max(out, h.sum, h.min, h.max);
        }
        MetricKind::ExponentialHistogram(e) => {
            let _ = write!(
                out,
                "exp_histogram scale={} count={} zero={} pos={} neg={}",
                e.scale,
                e.count,
                e.zero_count,
                e.positive.1.len(),
                e.negative.1.len()
            );
            render_optional_sum_min_max(out, e.sum, e.min, e.max);
        }
        MetricKind::Summary(s) => {
            out.push_str("summary");
            for (q, v) in &s.quantiles {
                let _ = write!(out, " q{q}={v}");
            }
            let _ = write!(out, " count={} sum={}", s.count, s.sum);
        }
        MetricKind::Set(hll) => {
            // The `HyperLogLog` estimate: one representative number, as `Distribution`'s `count=`.
            out.push_str("set=");
            let _ = write!(out, "{}", hll.estimate());
        }
    }
    if let Some(unit) = metric.unit {
        out.push_str(" unit=");
        render_key(out, resolve(unit));
    }
}

/// `name=... trace_id=<hex> span_id=<hex> [parent_span_id=<hex>] kind=... status=... duration=...`.
/// `event_timestamp` is the span's start time (a `SpanRecord` has none of its own), so duration
/// is computed here.
fn render_span(out: &mut String, event_timestamp: i64, span: &SpanRecord) {
    out.push_str("name=");
    render_value(out, &span.name);
    out.push_str(" trace_id=");
    push_hex(out, &span.trace_id);
    out.push_str(" span_id=");
    push_hex(out, &span.span_id);
    if let Some(parent) = &span.parent_span_id {
        out.push_str(" parent_span_id=");
        push_hex(out, parent);
    }
    out.push_str(" kind=");
    out.push_str(span.kind.as_str());
    out.push_str(" status=");
    out.push_str(span.status.as_str());
    out.push_str(" duration=");
    // `saturating_sub`: an `end_timestamp` before the start must render, not panic or wrap.
    let _ = write!(out, "{}", span.end_timestamp.saturating_sub(event_timestamp));
    out.push_str("ns");
}

/// `name=... at=<rfc3339> [attrs ...]` for one of a span's `events` (`SpanRecord::events`).
fn render_span_event(out: &mut String, span_event: &SpanEvent) {
    out.push_str("name=");
    render_value(out, &span_event.name);
    out.push_str(" at=");
    out.push_str(&format_rfc3339_utc(span_event.timestamp));
    if !span_event.attributes.is_empty() {
        out.push_str(" attrs ");
        render_attrs(out, &span_event.attributes);
    }
}

/// `trace_id=<hex> span_id=<hex> [attrs ...]` for one of a span's `links` (`SpanRecord::links`).
fn render_span_link(out: &mut String, link: &SpanLink) {
    out.push_str("trace_id=");
    push_hex(out, &link.trace_id);
    out.push_str(" span_id=");
    push_hex(out, &link.span_id);
    if !link.attributes.is_empty() {
        out.push_str(" attrs ");
        render_attrs(out, &link.attributes);
    }
}

/// Renders one [`Value`]: `Str` quoted and escaped; `Bytes` as `<N bytes>`, never a lossy UTF-8
/// decode; `Timestamp` as RFC 3339 ([`format_rfc3339_utc`]); `Array` as `[a, b]` and `Map` as
/// `{k=v, k2=v2}`, recursively. `tools/shape-survey/summarize.py`'s `parse_value` scans exactly
/// these forms.
///
/// `pub(crate)` for `syslog_out`'s container fallback (a `Map` or `Array` value); syslog never
/// routes a `Str` here, since the quoting would mangle a raw MSG body (`syslog.rs`'s module doc).
pub(crate) fn render_value(out: &mut String, value: &Value) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => {
            let _ = write!(out, "{b}");
        }
        Value::I64(i) => {
            let _ = write!(out, "{i}");
        }
        Value::U64(u) => {
            let _ = write!(out, "{u}");
        }
        Value::F64(f) => {
            let _ = write!(out, "{f}");
        }
        Value::Bytes(b) => {
            let _ = write!(out, "<{} bytes>", b.len());
        }
        Value::Str(s) => {
            // `Value::Str` is constructed only from valid UTF-8, so this cannot panic.
            let text = std::str::from_utf8(s).expect("Value::Str is always valid UTF-8");
            render_quoted_str(out, text);
        }
        Value::Timestamp(ns) => out.push_str(&format_rfc3339_utc(*ns)),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                render_value(out, item);
            }
            out.push(']');
        }
        Value::Map(map) => {
            out.push('{');
            for (i, (key, value)) in map.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                render_key(out, resolve(key));
                out.push('=');
                render_value(out, value);
            }
            out.push('}');
        }
    }
}

/// Renders a map/attribute key or a metric/unit name: bare when identifier-shaped (letters,
/// digits, `.`, `_`, `-`), quoted and escaped otherwise. A `json`-parsed body can key an attribute
/// on attacker-influenced text, and written bare, `a b=1` reads as two tokens and an embedded
/// newline forges a line.
fn render_key(out: &mut String, key: &str) {
    if is_plain_key(key) {
        out.push_str(key);
    } else {
        render_quoted_str(out, key);
    }
}

fn is_plain_key(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

/// Quotes and escapes `s`. Beyond `"`/`\`/`\n`/`\r`/`\t`, every other C0 control (`0x00..=0x1F`)
/// and DEL (`0x7F`) becomes `\xHH`: a raw ESC (`0x1B`) would otherwise emit a real OSC/CSI
/// sequence that takes over the viewer's terminal. This must hold for every string the sink
/// writes, since any of them can come from attacker-influenced input.
///
/// Copies each unescaped run with one `push_str`, like influxdb's `push_escaped`
/// (`docs/design/memory.md`). Every escaped character is ASCII, so the byte offset `find` returns
/// is one character wide, never a UTF-8 continuation byte.
fn render_quoted_str(out: &mut String, s: &str) {
    out.push('"');
    let mut rest = s;
    while let Some(i) = rest.find(needs_str_escape) {
        out.push_str(&rest[..i]);
        push_escaped_char(out, rest.as_bytes()[i]);
        rest = &rest[i + 1..];
    }
    out.push_str(rest);
    out.push('"');
}

fn needs_str_escape(c: char) -> bool {
    matches!(c, '"' | '\\' | '\n' | '\r' | '\t') || (c as u32) < 0x20 || c as u32 == 0x7f
}

/// Escapes the one byte `needs_str_escape` matched, formatting `\xHH` straight into `out`.
fn push_escaped_char(out: &mut String, b: u8) {
    match b {
        b'"' => out.push_str("\\\""),
        b'\\' => out.push_str("\\\\"),
        b'\n' => out.push_str("\\n"),
        b'\r' => out.push_str("\\r"),
        b'\t' => out.push_str("\\t"),
        _ => {
            let _ = write!(out, "\\x{b:02x}");
        }
    }
}

/// The open destination [`StreamOutput`] writes to. `File` always carries a [`FileTarget`] with a
/// [`RotatePolicy`] ([`RotatePolicy::never`] for `stdio_out`), which is what makes `stdio_out`'s
/// file target `file_out` without rotation in the type system.
#[derive(Debug)]
enum Target {
    Stdout(io::Stdout),
    Stderr(io::Stderr),
    File(FileTarget),
}

/// The `stdio_out` and `file_out` sink, generic over its [`Encoder`]
/// (`docs/adr/rotating-file-output.md`). The two differ only in `target`: `stdio_out` never
/// rotates, `file_out` carries a real policy. `build_spec` picks the constructor from the config.
#[derive(Debug)]
pub struct StreamOutput<E> {
    target: Target,
    encoder: E,
    telemetry: Telemetry,
    /// Only for a rotating file target's two non-fatal failures, `FileTarget::rotate`'s
    /// `rotate_failure`/`retention_failure`; every other failure returns `Err` from `send`.
    diagnostics: Diagnostics,
}

impl StreamOutput<StreamEncoder> {
    pub fn stdout() -> Self {
        Self {
            target: Target::Stdout(io::stdout()),
            encoder: StreamEncoder::human(),
            telemetry: Telemetry::default(),
            diagnostics: Diagnostics::default(),
        }
    }

    pub fn stderr() -> Self {
        Self {
            target: Target::Stderr(io::stderr()),
            encoder: StreamEncoder::human(),
            telemetry: Telemetry::default(),
            diagnostics: Diagnostics::default(),
        }
    }

    /// Opens (creating if needed) `path` for append, never rotating ([`RotatePolicy::never`]).
    ///
    /// Eager, at config-build time, so a bad path or permissions error fails startup before
    /// anything listens. `path` is used as given; `build_spec` resolves a relative one against the
    /// config file's directory.
    pub fn open_path(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Self::rotating(path, RotatePolicy::never())
    }

    /// The `file_out` constructor: [`Self::open_path`] under a real [`RotatePolicy`].
    pub fn rotating(path: impl AsRef<Path>, policy: RotatePolicy) -> anyhow::Result<Self> {
        let file = FileTarget::open(path, policy)?;
        Ok(Self {
            target: Target::File(file),
            encoder: StreamEncoder::human(),
            telemetry: Telemetry::default(),
            diagnostics: Diagnostics::default(),
        })
    }

    /// Replaces the `human()` default every constructor starts with; `build_spec` calls this for
    /// `format: native` (`docs/adr/file-output-native-format.md`).
    pub fn with_format(mut self, encoder: StreamEncoder) -> Self {
        self.encoder = encoder;
        self
    }
}

impl<E> StreamOutput<E> {
    /// Attaches a telemetry handle -- see `send`'s `logit.output.batch.bytes`.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Attaches a diagnostics handle for the two rotation keys the `diagnostics` field names.
    pub fn with_diagnostics(mut self, diagnostics: Diagnostics) -> Self {
        self.diagnostics = diagnostics;
        self
    }
}

#[async_trait::async_trait]
impl<E: Encoder + Send> Output for StreamOutput<E> {
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        // Check the input, not the encoded bytes: `NativeEncoder` emits a non-empty frame
        // (header, dictionary, resource) even for zero events.
        if batch.events.is_empty() {
            return Ok(());
        }
        let bytes = self.encoder.encode(batch).context("encoding batch")?;

        // Rotation is decided before the write, so a batch is never split across files
        // (`FileTarget::should_rotate` on a batch bigger than `max_bytes`).
        if let Target::File(file) = &mut self.target {
            let now = crate::file::now_unix();
            if file.should_rotate(now, bytes.len()) {
                let outcome = file.rotate(&mut self.diagnostics).await?;
                // Counted here because `FileTarget` holds no `Telemetry`. `NotRotated` means the
                // active file's rename or truncate failed and nothing on disk changed, so it
                // isn't a rotation.
                if outcome == RotateOutcome::Rotated {
                    self.telemetry.count("logit.output.file.rotations", 1.0, &[]);
                }
            }
            file.note_written(now, bytes.len());
        }

        self.telemetry.count("logit.output.batch.bytes", bytes.len() as f64, &[]);
        // One `write_all` and one `flush` per batch, so nothing sits in tokio's buffer between
        // batches. `flush` is not `fsync`: the OS page cache still holds the bytes. A write error
        // carries no `Fault`, so the runtime doesn't retry the batch, and it doesn't count toward
        // the permanent-failure exit either (`logit_pipeline::output::is_explicitly_permanent`).
        // A failed re-open after rotation is the exception: `Fault::Clean` (`FileTarget::rotate`).
        match &mut self.target {
            Target::Stdout(w) => {
                w.write_all(&bytes).await?;
                w.flush().await?;
            }
            Target::Stderr(w) => {
                w.write_all(&bytes).await?;
                w.flush().await?;
            }
            Target::File(f) => {
                f.write_all(&bytes).await?;
                f.flush().await?;
            }
        }
        Ok(())
    }

    /// Not the default no-op: `send` flushes every batch anyway, but this keeps the runtime's
    /// shutdown flush (`finish_and_flush`) meaningful if that ever changes.
    async fn flush(&mut self) -> anyhow::Result<()> {
        match &mut self.target {
            Target::Stdout(w) => w.flush().await.context("flushing stdout")?,
            Target::Stderr(w) => w.flush().await.context("flushing stderr")?,
            Target::File(f) => f.flush().await.context("flushing file target")?,
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{
        BodyFormat, DdSketch, HyperLogLog, LogRecord, Resource, SpanKind, SpanStatus,
    };
    use std::sync::Arc;

    fn batch_with(events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
    }

    fn encode(events: Vec<Event>) -> String {
        EventDump::default().render(&batch_with(events))
    }

    fn log_event(ts: i64, message: &str, severity: Option<Severity>) -> Event {
        Event::log(
            ts,
            AttrMap::new(),
            LogRecord {
                message: Value::str(message),
                severity,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        )
    }

    fn metric_event(ts: i64, name: &str, kind: MetricKind) -> Event {
        Event::metric(
            ts,
            AttrMap::new(),
            MetricRecord::new(logit_core::interner::intern(name), kind),
        )
    }

    fn span_event(ts: i64, end: i64) -> Event {
        Event::span(
            ts,
            AttrMap::new(),
            SpanRecord {
                trace_id: [0xAB; 16],
                span_id: [0xCD; 8],
                parent_span_id: None,
                name: Value::str("handle_request"),
                kind: SpanKind::Server,
                status: SpanStatus::Ok,
                events: vec![],
                links: vec![],
                end_timestamp: end,
                flags: 0,
                ext: None,
            },
        )
    }

    #[test]
    fn log_only_event_renders_timestamp_and_log_line_only() {
        let out = encode(vec![log_event(0, "GET /index.html HTTP/1.1", Some(Severity::Info))]);
        assert_eq!(out, "1970-01-01T00:00:00.000000000Z log[info] \"GET /index.html HTTP/1.1\"\n");
    }

    #[test]
    fn log_with_no_severity_renders_a_dash() {
        let out = encode(vec![log_event(0, "hello", None)]);
        assert!(out.contains("log[-] \"hello\""), "got: {out}");
    }

    #[test]
    fn a_log_with_no_trace_context_renders_no_trace_suffix_at_all() {
        let out = encode(vec![log_event(0, "hello", None)]);
        assert!(!out.contains("trace_id="), "got: {out}");
    }

    #[test]
    fn a_logs_trace_context_renders_after_the_message() {
        let mut event = log_event(0, "hello", Some(Severity::Info));
        event.log.as_mut().unwrap().trace =
            Some(logit_core::TraceRef { trace_id: [0xab; 16], span_id: Some([0xcd; 8]), flags: 1 });
        let out = encode(vec![event]);
        assert_eq!(
            out,
            format!(
                "1970-01-01T00:00:00.000000000Z log[info] \"hello\" trace_id={} span_id={} flags=1\n",
                "ab".repeat(16),
                "cd".repeat(8)
            )
        );
    }

    #[test]
    fn a_logs_trace_context_with_no_span_id_and_zero_flags_omits_both() {
        let mut event = log_event(0, "hello", None);
        event.log.as_mut().unwrap().trace =
            Some(logit_core::TraceRef { trace_id: [0xab; 16], span_id: None, flags: 0 });
        let out = encode(vec![event]);
        assert!(out.contains(&format!("trace_id={}\n", "ab".repeat(16))), "got: {out}");
        assert!(!out.contains("span_id="), "got: {out}");
        assert!(!out.contains("flags="), "got: {out}");
    }

    #[test]
    fn metrics_only_event_renders_a_metric_line_and_no_log_prefix() {
        let out = encode(vec![metric_event(0, "nginx.requests", MetricKind::counter(1.0))]);
        assert_eq!(
            out,
            "1970-01-01T00:00:00.000000000Z\n  metric  nginx.requests sum=1 temporality=delta monotonic=true\n"
        );
    }

    #[test]
    fn span_only_event_renders_a_span_line() {
        let out = encode(vec![span_event(1_000_000_000, 1_500_000_000)]);
        assert!(out.contains("  span    "), "got: {out}");
        assert!(out.contains("name=\"handle_request\""), "got: {out}");
        assert!(out.contains("trace_id=abababababababababababababababab"), "got: {out}");
        assert!(out.contains("span_id=cdcdcdcdcdcdcdcd"), "got: {out}");
        assert!(out.contains("kind=server"), "got: {out}");
        assert!(out.contains("status=ok"), "got: {out}");
        assert!(out.contains("duration=500000000ns"), "got: {out}");
        assert!(!out.contains("parent_span_id"), "no parent should render nothing for it: {out}");
    }

    #[test]
    fn span_events_and_links_render_on_their_own_lines() {
        let mut event = span_event(1_000_000_000, 1_500_000_000);
        let mut event_attrs = AttrMap::new();
        event_attrs.insert("retry", 1_i64);
        let span_evt = SpanEvent {
            timestamp: 1_200_000_000,
            name: Value::str("retrying"),
            attributes: event_attrs,
            dropped_attributes_count: 0,
        };
        let mut link_attrs = AttrMap::new();
        link_attrs.insert("relation", "follows_from");
        let link = SpanLink {
            trace_id: [0xEF; 16],
            span_id: [0x12; 8],
            attributes: link_attrs,
            flags: 0,
            trace_state: None,
            dropped_attributes_count: 0,
        };
        match &mut event.span {
            Some(span) => {
                span.events.push(span_evt);
                span.links.push(link);
            }
            None => unreachable!("span_event always builds a span"),
        }

        let out = encode(vec![event]);
        assert!(out.contains("  span_event "), "got: {out}");
        assert!(out.contains("name=\"retrying\""), "got: {out}");
        assert!(out.contains("at=1970-01-01T00:00:01.200000000Z"), "got: {out}");
        assert!(out.contains("retry=1"), "got: {out}");
        assert!(out.contains("  span_link "), "got: {out}");
        assert!(out.contains("trace_id=efefefefefefefefefefefefefefefef"), "got: {out}");
        assert!(out.contains("span_id=1212121212121212"), "got: {out}");
        assert!(out.contains(r#"relation="follows_from""#), "got: {out}");
    }

    #[test]
    fn mixed_log_and_metric_event_renders_both_sections() {
        let mut event = log_event(0, "GET /", Some(Severity::Info));
        event.metrics.push(MetricRecord::new(
            logit_core::interner::intern("nginx.requests"),
            MetricKind::counter(1.0),
        ));
        let out = encode(vec![event]);
        assert!(out.contains("log[info] \"GET /\""), "got: {out}");
        assert!(
            out.contains("  metric  nginx.requests sum=1 temporality=delta monotonic=true"),
            "got: {out}"
        );
    }

    #[test]
    fn a_completely_empty_event_renders_just_its_timestamp_line_and_does_not_panic() {
        let out = encode(vec![Event::empty(0, AttrMap::new())]);
        assert_eq!(out, "1970-01-01T00:00:00.000000000Z\n");
    }

    /// Resource attributes merge underneath the event's own; the event wins a key collision.
    #[test]
    fn resource_attributes_are_included_and_the_event_overrides_on_collision() {
        let mut resource = Resource::default();
        resource.attributes.insert("host", "web-1");
        resource.attributes.insert("env", "staging");
        let mut event = Event::empty(0, AttrMap::new());
        event.attributes.insert("env", "prod");
        let batch = EventBatch { resource: Arc::new(resource), scope: None, events: vec![event] };
        let out = EventDump::default().render(&batch);

        assert!(out.contains(r#"host="web-1""#), "got: {out}");
        assert!(out.contains(r#"env="prod""#), "event's own env should win over resource's: {out}");
        assert!(!out.contains("staging"), "got: {out}");
    }

    /// Events differing only in their resource must not render identically.
    #[test]
    fn two_batches_from_different_resources_render_differently() {
        let mut resource_a = Resource::default();
        resource_a.attributes.insert("host", "web-1");
        let mut resource_b = Resource::default();
        resource_b.attributes.insert("host", "web-2");

        let out_a = EventDump::default().render(&EventBatch {
            resource: Arc::new(resource_a),
            scope: None,
            events: vec![Event::empty(0, AttrMap::new())],
        });
        let out_b = EventDump::default().render(&EventBatch {
            resource: Arc::new(resource_b),
            scope: None,
            events: vec![Event::empty(0, AttrMap::new())],
        });

        assert_ne!(out_a, out_b, "different resources must produce distinguishable output");
        assert!(out_a.contains(r#"host="web-1""#), "got: {out_a}");
        assert!(out_b.contains(r#"host="web-2""#), "got: {out_b}");
    }

    #[test]
    fn gauge_renders_as_kind_equals_value() {
        let out = encode(vec![metric_event(0, "cpu.load", MetricKind::Gauge(0.5))]);
        assert!(out.contains("cpu.load gauge=0.5"), "got: {out}");
    }

    #[test]
    fn distribution_renders_count_and_percentiles() {
        let mut sketch = DdSketch::new();
        sketch.add(120.0);
        let out = encode(vec![metric_event(0, "latency", MetricKind::Distribution(sketch))]);
        assert!(out.contains("distribution count=1"), "got: {out}");
        assert!(out.contains("p50="), "got: {out}");
        assert!(out.contains("p90="), "got: {out}");
        assert!(out.contains("p99="), "got: {out}");
    }

    #[test]
    fn histogram_renders_each_bucket() {
        let out = encode(vec![metric_event(
            0,
            "resp.size",
            MetricKind::Histogram(logit_core::Histogram {
                buckets: vec![(100.0, 5), (500.0, 2)],
                temporality: logit_core::Temporality::Cumulative,
                sum: None,
                min: None,
                max: None,
            }),
        )]);
        assert!(out.contains("bucket_100=5"), "got: {out}");
        assert!(out.contains("bucket_500=2"), "got: {out}");
        assert!(!out.contains("sum="), "no sum/min/max should render when absent: {out}");
    }

    #[test]
    fn histogram_renders_sum_min_max_when_present() {
        let out = encode(vec![metric_event(
            0,
            "resp.size",
            MetricKind::Histogram(logit_core::Histogram {
                buckets: vec![(100.0, 5)],
                temporality: logit_core::Temporality::Cumulative,
                sum: Some(42.0),
                min: Some(1.0),
                max: Some(99.0),
            }),
        )]);
        assert!(out.contains("sum=42"), "got: {out}");
        assert!(out.contains("min=1"), "got: {out}");
        assert!(out.contains("max=99"), "got: {out}");
    }

    #[test]
    fn summary_renders_each_quantile() {
        let out = encode(vec![metric_event(
            0,
            "req.latency",
            MetricKind::Summary(logit_core::Summary {
                quantiles: vec![(0.99, 12.5)],
                count: 3,
                sum: 40.0,
            }),
        )]);
        assert!(out.contains("q0.99=12.5"), "got: {out}");
        assert!(out.contains("count=3"), "got: {out}");
        assert!(out.contains("sum=40"), "got: {out}");
    }

    #[test]
    fn samples_renders_values_and_rate() {
        let out = encode(vec![metric_event(
            0,
            "latency",
            MetricKind::Samples(logit_core::Samples::new([1.0, 2.0])),
        )]);
        assert!(out.contains("samples=[1,2] rate=1"), "got: {out}");
    }

    #[test]
    fn set_members_render_as_quoted_lossy_utf8_strings() {
        let out = encode(vec![metric_event(
            0,
            "unique_visitors",
            MetricKind::SetMembers(vec![
                bytes::Bytes::from_static(b"a"),
                bytes::Bytes::from_static(b"b"),
            ]),
        )]);
        assert!(out.contains("set_members=[\"a\",\"b\"]"), "got: {out}");
    }

    /// A set member's newline can't forge a second `metric` line, and a raw ESC never reaches the
    /// output.
    #[test]
    fn set_members_are_escaped_not_emitted_raw() {
        let out = encode(vec![metric_event(
            0,
            "uniq",
            MetricKind::SetMembers(vec![bytes::Bytes::from_static(
                b"a\n  metric  forged sum=9\x1b]0;x\x07",
            )]),
        )]);
        assert!(!out.contains('\x1b'), "a raw ESC byte must never reach the output: {out:?}");
        assert_eq!(
            out.lines().filter(|l| l.starts_with("  metric  ")).count(),
            1,
            "a member must not forge a second metric line: {out:?}"
        );
        assert!(
            out.contains("set_members=[\"a\\n  metric  forged sum=9\\x1b]0;x\\x07\"]"),
            "got: {out:?}"
        );
    }

    #[test]
    fn exponential_histogram_renders_shape_and_optional_sum_min_max() {
        let out = encode(vec![metric_event(
            0,
            "resp.size",
            MetricKind::ExponentialHistogram(logit_core::ExpHistogram {
                scale: 3,
                zero_count: 2,
                zero_threshold: 0.0,
                positive: (0, vec![1, 2, 3]),
                negative: (0, vec![4, 5]),
                temporality: logit_core::Temporality::Cumulative,
                count: 10,
                sum: Some(11.0),
                min: Some(0.0),
                max: Some(9.0),
            }),
        )]);
        assert!(out.contains("exp_histogram scale=3 count=10 zero=2 pos=3 neg=2"), "got: {out}");
        assert!(out.contains("sum=11"), "got: {out}");
        assert!(out.contains("min=0"), "got: {out}");
        assert!(out.contains("max=9"), "got: {out}");
    }

    #[test]
    fn set_renders_its_hyperloglog_estimate() {
        let mut hll = HyperLogLog::default();
        hll.insert(b"a");
        hll.insert(b"b");
        hll.insert(b"a"); // idempotent -- must not inflate the estimate
        let out = encode(vec![metric_event(0, "unique.users", MetricKind::Set(hll))]);
        assert!(out.contains("unique.users set=2"), "got: {out}");
    }

    /// A `NO_RECORDED_VALUE` point renders as the flag, never dropped and never as the kind's
    /// default value (`docs/known-gaps.md`'s cross-protocol table).
    #[test]
    fn a_no_recorded_value_point_renders_the_flag_instead_of_a_value() {
        let mut event = metric_event(0, "conns", MetricKind::Gauge(0.0));
        event.metrics[0].flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
        let out = encode(vec![event]);
        assert!(out.contains("conns no_recorded_value"), "got: {out}");
        assert!(!out.contains("gauge="), "must not also render a value: {out}");
    }

    /// A `GaugeDelta` renders as `gauge_delta` with an explicit sign, distinct from a `Gauge`.
    #[test]
    fn gauge_delta_renders_distinguishably_with_an_explicit_sign() {
        let out = encode(vec![metric_event(0, "conns", MetricKind::GaugeDelta(5.0))]);
        assert!(out.contains("conns gauge_delta=+5"), "got: {out}");

        let out = encode(vec![metric_event(0, "conns", MetricKind::GaugeDelta(-5.0))]);
        assert!(out.contains("conns gauge_delta=-5"), "got: {out}");
    }

    /// Negative zero renders `-0`, not `+-0`.
    #[test]
    fn gauge_delta_negative_zero_does_not_double_the_sign() {
        let out = encode(vec![metric_event(0, "conns", MetricKind::GaugeDelta(-0.0))]);
        assert!(out.contains("conns gauge_delta=-0"), "got: {out}");
        assert!(!out.contains("+-0"), "got: {out}");
    }

    #[test]
    fn unit_appears_when_present_and_is_absent_otherwise() {
        let with_unit = encode(vec![Event::metric(0, AttrMap::new(), {
            let mut record = MetricRecord::new(
                logit_core::interner::intern("request_time"),
                MetricKind::Gauge(0.5),
            );
            record.unit = Some(logit_core::interner::intern("s"));
            record
        })]);
        assert!(with_unit.contains("unit=s"), "got: {with_unit}");

        let without_unit = encode(vec![metric_event(0, "request_time", MetricKind::Gauge(0.5))]);
        assert!(!without_unit.contains("unit="), "got: {without_unit}");
    }

    #[test]
    fn bytes_value_renders_as_a_byte_count_not_lossy_utf8() {
        let mut event = Event::empty(0, AttrMap::new());
        event
            .attributes
            .insert("payload", Value::Bytes(bytes::Bytes::from_static(b"\xff\xfe\x00")));
        let out = encode(vec![event]);
        assert!(out.contains("payload=<3 bytes>"), "got: {out}");
    }

    #[test]
    fn timestamp_value_renders_as_rfc3339() {
        let mut event = Event::empty(0, AttrMap::new());
        event.attributes.insert("seen_at", Value::Timestamp(0));
        let out = encode(vec![event]);
        assert!(out.contains("seen_at=1970-01-01T00:00:00.000000000Z"), "got: {out}");
    }

    #[test]
    fn null_bool_and_numeric_values_render_plainly() {
        let mut event = Event::empty(0, AttrMap::new());
        event.attributes.insert("a", Value::Null);
        event.attributes.insert("b", Value::Bool(true));
        event.attributes.insert("c", Value::I64(-5));
        event.attributes.insert("d", Value::U64(5));
        event.attributes.insert("e", Value::F64(1.5));
        let out = encode(vec![event]);
        assert!(out.contains("a=null"), "got: {out}");
        assert!(out.contains("b=true"), "got: {out}");
        assert!(out.contains("c=-5"), "got: {out}");
        assert!(out.contains("d=5"), "got: {out}");
        assert!(out.contains("e=1.5"), "got: {out}");
    }

    #[test]
    fn array_and_map_values_render_compactly() {
        let mut event = Event::empty(0, AttrMap::new());
        event.attributes.insert("tags", Value::Array(vec![Value::str("a"), Value::str("b")]));
        let mut inner = AttrMap::new();
        inner.insert("k", "v");
        event.attributes.insert("nested", Value::Map(Box::new(inner)));
        let out = encode(vec![event]);
        assert!(out.contains(r#"tags=["a", "b"]"#), "got: {out}");
        assert!(out.contains(r#"nested={k="v"}"#), "got: {out}");
    }

    /// A multi-valued statsd tag renders as one bracketed list in array order.
    #[test]
    fn a_multi_valued_statsd_tag_attribute_renders_as_a_bracketed_list() {
        let mut event = Event::empty(0, AttrMap::new());
        event.attributes.insert("team", Value::Array(vec![Value::str("a"), Value::str("b")]));
        let out = encode(vec![event]);
        assert!(out.contains(r#"team=["a", "b"]"#), "got: {out}");
    }

    #[test]
    fn a_quoted_string_escapes_special_characters() {
        let mut event = Event::empty(0, AttrMap::new());
        event.attributes.insert("msg", Value::str("line1\nline2\t\"quoted\"\\backslash"));
        let out = encode(vec![event]);
        assert!(out.contains(r#"msg="line1\nline2\t\"quoted\"\\backslash""#), "got: {out}");
    }

    /// A raw ESC in a value never reaches the terminal unescaped.
    #[test]
    fn escape_and_other_control_characters_in_a_value_are_escaped_not_emitted_raw() {
        let mut event = Event::empty(0, AttrMap::new());
        event.attributes.insert("payload", Value::str("clear\x1b[2Jscreen\x07bell\x00nul"));
        let out = encode(vec![event]);
        assert!(out.contains(r"clear\x1b[2Jscreen"), "ESC should be escaped, got: {out}");
        assert!(out.contains(r"\x07bell"), "BEL should be escaped, got: {out}");
        assert!(out.contains(r"\x00nul"), "NUL should be escaped, got: {out}");
        assert!(!out.contains('\x1b'), "a raw ESC byte must never reach the output: {out:?}");
        assert!(!out.contains('\x07'), "a raw BEL byte must never reach the output: {out:?}");
    }

    /// A key containing a space, `=`, or a newline is quoted, not written bare.
    #[test]
    fn a_key_that_is_not_a_plain_identifier_is_quoted_and_escaped() {
        let mut event = Event::empty(0, AttrMap::new());
        event.attributes.insert("weird key\nwith=stuff", "value");
        let out = encode(vec![event]);
        assert!(out.contains(r#""weird key\nwith=stuff"="value""#), "got: {out}");
    }

    #[test]
    fn attributes_come_out_in_attrmaps_sorted_order() {
        // `Symbol` order depends on process-wide intern history, so assert only that the render
        // follows `AttrMap::iter`'s order, not any alphabetical one.
        let mut attrs = AttrMap::new();
        attrs.insert("zebra", "z");
        attrs.insert("apple", "a");
        attrs.insert("mango", "m");
        let expected_order: Vec<&str> = attrs.iter().map(|(k, _)| resolve(k)).collect();

        let out = encode(vec![Event::empty(0, attrs)]);
        let attrs_line = out.lines().find(|l| l.contains("attrs")).expect("should have attrs");

        let positions: Vec<usize> =
            expected_order.iter().map(|key| attrs_line.find(key).unwrap()).collect();
        let mut sorted_positions = positions.clone();
        sorted_positions.sort_unstable();
        assert_eq!(
            positions, sorted_positions,
            "attrs line should preserve AttrMap's own iteration order, got: {attrs_line}"
        );
    }

    #[test]
    fn a_multi_event_batch_renders_one_block_per_event_in_batch_order() {
        let out = encode(vec![
            metric_event(0, "first", MetricKind::counter(1.0)),
            metric_event(1, "second", MetricKind::counter(2.0)),
            metric_event(2, "third", MetricKind::counter(3.0)),
        ]);
        let first = out.find("first").unwrap();
        let second = out.find("second").unwrap();
        let third = out.find("third").unwrap();
        assert!(first < second && second < third, "expected batch order, got: {out}");
    }

    #[tokio::test]
    async fn send_writes_the_encoded_batch_to_a_file_target_and_flushes() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("logit-stdio-out-test-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let mut output = StreamOutput::open_path(&path).expect("path should open");
        output
            .send(&batch_with(vec![metric_event(0, "x", MetricKind::counter(1.0))]))
            .await
            .expect("send should succeed");

        let contents = std::fs::read_to_string(&path).expect("file should exist and be readable");
        assert!(contents.contains("x sum=1 temporality=delta monotonic=true"), "got: {contents}");
        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn send_appends_across_multiple_batches_rather_than_truncating() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("logit-stdio-out-test-append-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let mut output = StreamOutput::open_path(&path).expect("path should open");
        output
            .send(&batch_with(vec![metric_event(0, "first", MetricKind::counter(1.0))]))
            .await
            .expect("send should succeed");
        output
            .send(&batch_with(vec![metric_event(0, "second", MetricKind::counter(2.0))]))
            .await
            .expect("send should succeed");

        let contents = std::fs::read_to_string(&path).expect("file should exist and be readable");
        assert!(
            contents.contains("first sum=1 temporality=delta monotonic=true"),
            "got: {contents}"
        );
        assert!(
            contents.contains("second sum=2 temporality=delta monotonic=true"),
            "got: {contents}"
        );
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn open_path_reports_a_clear_path_naming_error_for_an_unopenable_path() {
        // A missing parent directory fails regardless of permissions.
        let path = std::env::temp_dir().join("logit-stdio-out-test-no-such-dir").join("x.log");
        let err = StreamOutput::open_path(&path).expect_err("expected an error");
        assert!(format!("{err:?}").contains(&path.display().to_string()), "got: {err:?}");
    }

    #[tokio::test]
    async fn send_on_an_empty_batch_writes_nothing_and_does_not_error() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("logit-stdio-out-test-empty-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let mut output = StreamOutput::open_path(&path).expect("path should open");
        output.send(&batch_with(vec![])).await.expect("send should succeed");

        let contents = std::fs::read_to_string(&path).unwrap_or_default();
        assert_eq!(contents, "", "an empty batch should write nothing");
        std::fs::remove_file(&path).ok();
    }

    /// `logit.output.batch.bytes` equals the encoded length written to the file.
    #[tokio::test]
    async fn send_records_batch_bytes_matching_the_actual_encoded_length() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("logit-stdio-out-test-telemetry-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("tap", "stdio_out", "sink");
        let mut output =
            StreamOutput::open_path(&path).expect("path should open").with_telemetry(telemetry);
        output
            .send(&batch_with(vec![metric_event(0, "x", MetricKind::counter(1.0))]))
            .await
            .expect("send should succeed");

        let contents = std::fs::read(&path).expect("file should exist and be readable");
        std::fs::remove_file(&path).ok();

        let events = registry.drain(0);
        let recorded = events
            .iter()
            .find_map(|e| {
                e.metrics.iter().find_map(|m| match &m.kind {
                    MetricKind::Sum(s)
                        if logit_core::interner::resolve(m.name) == "logit.output.batch.bytes" =>
                    {
                        Some(s.value)
                    }
                    _ => None,
                })
            })
            .expect("logit.output.batch.bytes should have been recorded");
        assert_eq!(recorded, contents.len() as f64);
    }

    /// `stdio_out`'s file target never rotates through `send`, however much is written.
    #[tokio::test]
    async fn open_path_never_rotates_no_matter_how_much_is_written() {
        let dir = std::env::temp_dir();
        let path =
            dir.join(format!("logit-stdio-out-test-never-rotate-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let mut output = StreamOutput::open_path(&path).expect("path should open");
        for i in 0..20 {
            output
                .send(&batch_with(vec![metric_event(0, "x", MetricKind::counter(i as f64))]))
                .await
                .expect("send should succeed");
        }

        assert!(
            !dir.join(format!("logit-stdio-out-test-never-rotate-{}.log.1", std::process::id()))
                .exists(),
            "an unrotated target must never create a .1"
        );
        std::fs::remove_file(&path).ok();
    }

    /// `send`'s should_rotate/rotate/note_written sequencing and `logit.output.file.rotations`.
    #[tokio::test]
    async fn rotating_via_stream_output_rotates_and_counts_the_rotation() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("logit-stdio-out-test-rotating-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let rotated =
            dir.join(format!("logit-stdio-out-test-rotating-{}.log.1", std::process::id()));
        let _ = std::fs::remove_file(&rotated);

        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("tap", "file_out", "sink");
        let policy = RotatePolicy { max_bytes: Some(1), interval: None, max_files: 5 };
        let mut output = StreamOutput::rotating(&path, policy)
            .expect("path should open")
            .with_telemetry(telemetry);

        output
            .send(&batch_with(vec![metric_event(0, "first", MetricKind::counter(1.0))]))
            .await
            .expect("send should succeed");
        output
            .send(&batch_with(vec![metric_event(0, "second", MetricKind::counter(2.0))]))
            .await
            .expect("send should succeed");

        assert!(rotated.exists(), "the first batch should have been rotated out to .1");
        let rotated_contents = std::fs::read_to_string(&rotated).unwrap();
        assert!(rotated_contents.contains("first"), "got: {rotated_contents}");
        let active_contents = std::fs::read_to_string(&path).unwrap();
        assert!(active_contents.contains("second"), "got: {active_contents}");

        let events = registry.drain(0);
        let rotations = events
            .iter()
            .find_map(|e| {
                e.metrics.iter().find_map(|m| match &m.kind {
                    MetricKind::Sum(s)
                        if logit_core::interner::resolve(m.name)
                            == "logit.output.file.rotations" =>
                    {
                        Some(s.value)
                    }
                    _ => None,
                })
            })
            .expect("logit.output.file.rotations should have been recorded");
        assert_eq!(rotations, 1.0, "exactly one rotation should have happened");

        std::fs::remove_file(&path).ok();
        std::fs::remove_file(&rotated).ok();
    }

    /// A failed active-file rename (`RotateOutcome::NotRotated`) isn't counted as a rotation.
    #[tokio::test]
    async fn a_rotation_that_could_not_rename_the_active_file_is_never_counted_as_a_rotation() {
        let dir = std::env::temp_dir();
        let path =
            dir.join(format!("logit-stdio-out-test-failed-rotate-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("tap", "file_out", "sink");
        let policy = RotatePolicy { max_bytes: Some(1), interval: None, max_files: 5 };
        let mut output = StreamOutput::rotating(&path, policy)
            .expect("path should open")
            .with_telemetry(telemetry);

        output
            .send(&batch_with(vec![metric_event(0, "first", MetricKind::counter(1.0))]))
            .await
            .expect("send should succeed");

        // The fd stays valid, but `rotate`'s rename now has nothing at `path`.
        std::fs::remove_file(&path).ok();

        output
            .send(&batch_with(vec![metric_event(0, "second", MetricKind::counter(2.0))]))
            .await
            .expect("send should still succeed even though the rename underneath it failed");

        let events = registry.drain(0);
        let rotations = events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(s)
                    if logit_core::interner::resolve(m.name) == "logit.output.file.rotations" =>
                {
                    Some(s.value)
                }
                _ => None,
            })
        });
        assert!(rotations.is_none(), "a failed rotation must never be counted, got: {rotations:?}");

        std::fs::remove_file(&path).ok();
    }

    // --- StreamEncoder ---

    #[test]
    fn stream_encoder_human_delegates_to_event_dump() {
        let mut encoder = StreamEncoder::human();
        let bytes = encoder
            .encode(&batch_with(vec![metric_event(0, "x", MetricKind::counter(1.0))]))
            .expect("should encode");
        let text = String::from_utf8(bytes.to_vec()).expect("human output is always valid utf-8");
        assert!(text.contains("x sum=1 temporality=delta monotonic=true"), "got: {text}");
    }

    /// `StreamEncoder::Native`'s output decodes through `read_frame` + `decode_batch`.
    #[test]
    fn stream_encoder_native_round_trips_through_the_real_native_decoder() {
        let mut encoder = StreamEncoder::native(NativeCompression::None);
        let mut bytes = encoder
            .encode(&batch_with(vec![metric_event(0, "x", MetricKind::counter(1.0))]))
            .expect("native encode should succeed");

        let (codec_id, mut payload) =
            logit_proto::frame::read_frame(&mut bytes).expect("frame should read");
        assert_eq!(codec_id, logit_proto::native::CODEC_NATIVE_V1);
        let decoded =
            logit_proto::native::decode_batch(&mut payload).expect("payload should decode");
        assert_eq!(decoded.events.len(), 1);
        match &decoded.events[0].metrics[0].kind {
            MetricKind::Sum(s) => assert_eq!(s.value, 1.0),
            other => panic!("expected Sum, got {other:?}"),
        }
    }

    /// An empty batch writes nothing under `format: native`, whose encoder emits a frame anyway.
    #[tokio::test]
    async fn send_on_an_empty_batch_writes_nothing_under_native_format_either() {
        let dir = std::env::temp_dir();
        let path =
            dir.join(format!("logit-stdio-out-test-native-empty-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let mut output = StreamOutput::open_path(&path)
            .expect("path should open")
            .with_format(StreamEncoder::native(NativeCompression::None));
        output.send(&batch_with(vec![])).await.expect("send should succeed");

        let contents = std::fs::read(&path).unwrap_or_default();
        assert!(contents.is_empty(), "an empty batch under format: native should write nothing");
        std::fs::remove_file(&path).ok();
    }

    /// Under `format: native`, the rotated `.1` and the fresh active file each decode on their own
    /// (`docs/adr/file-output-native-format.md`).
    #[tokio::test]
    async fn rotating_under_native_format_leaves_both_files_independently_decodable() {
        let dir = std::env::temp_dir();
        let path =
            dir.join(format!("logit-stdio-out-test-native-rotate-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let rotated =
            dir.join(format!("logit-stdio-out-test-native-rotate-{}.log.1", std::process::id()));
        let _ = std::fs::remove_file(&rotated);

        let policy = RotatePolicy { max_bytes: Some(1), interval: None, max_files: 5 };
        let mut output = StreamOutput::rotating(&path, policy)
            .expect("path should open")
            .with_format(StreamEncoder::native(NativeCompression::None));

        output
            .send(&batch_with(vec![metric_event(0, "first", MetricKind::counter(1.0))]))
            .await
            .expect("send should succeed");
        output
            .send(&batch_with(vec![metric_event(0, "second", MetricKind::counter(2.0))]))
            .await
            .expect("send should succeed");
        assert!(rotated.exists(), "the first batch should have been rotated out to .1");

        for (file_path, expected_name) in [(&rotated, "first"), (&path, "second")] {
            let mut bytes = Bytes::from(std::fs::read(file_path).unwrap());
            let (codec_id, mut payload) =
                logit_proto::frame::read_frame(&mut bytes).expect("frame should read");
            assert_eq!(codec_id, logit_proto::native::CODEC_NATIVE_V1);
            let decoded =
                logit_proto::native::decode_batch(&mut payload).expect("payload should decode");
            assert_eq!(
                logit_core::interner::resolve(decoded.events[0].metrics[0].name),
                expected_name,
                "got the wrong events out of {}",
                file_path.display()
            );
        }

        std::fs::remove_file(&path).ok();
        std::fs::remove_file(&rotated).ok();
    }
}
