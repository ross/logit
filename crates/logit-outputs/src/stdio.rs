//! A general-purpose, human-facing debug sink: dumps a whole pipeline's events as readable text
//! to stdout (default), stderr, or a file -- the dev loop for this project, and the first thing
//! anyone getting started with `logit` reaches for before standing up a real backend like
//! InfluxDB.
//!
//! **This deliberately renders a readable text block, not one JSON object per event.** The
//! original `docs/plans/nginx-integration.md` sketch called for JSON before workstream A
//! landed (`Event` carrying `log`/`metrics`/`span` independently, ADR `multi-payload-events`); once building this
//! for real, a block a person can read at a glance in a terminal won -- this is a debugging/
//! dev-loop sink for a human, not a machine-parseable export format (that's what
//! `logit-outputs::influxdb`'s line protocol is for, and an NDJSON `Format` variant remains a
//! reasonable future addition if a real consumer needs one). See that plan document's workstream D
//! section (marked superseded there) and `docs/known-gaps.md` for the accepted consequences.
//!
//! Split the way `InfluxDbOutput`/`InfluxLineEncoder` are (`crates/logit-outputs/src/influxdb.rs`):
//! a pure [`EventDump`] encoder (`&EventBatch` -> `String`, no file descriptor anywhere) plus the
//! thin [`StdioOutput`] that owns the open target and writes/flushes it. Every format test below
//! runs against the encoder alone.
//!
//! The encoder is deliberately built around a [`Format`] enum with a single variant today
//! (`Format::Human`), and the per-value/per-metric rendering (`render_value`/`render_metric`) is
//! kept as free functions rather than inlined into one big match -- a future user-supplied
//! `format:` template string (or an NDJSON variant) is explicitly designed *for* here (a new
//! `Format` variant plus a renderer that calls the same free functions) but not built now.
//!
//! Every string rendered here -- a value, but also an attribute/map key or a metric/unit name, all
//! of which can originate from attacker-influenced input (a syslog line, a JSON body) rather than
//! trusted local config -- goes through [`render_quoted_str`] or [`render_key`], which escape
//! every C0 control character (including ESC, so an embedded terminal escape/OSC sequence can't
//! repaint or otherwise hijack the viewer's terminal) and DEL, and quote any key that isn't a
//! plain identifier-shaped string (so a key containing a space, `=`, or newline can't be
//! misread as extra tokens or an injected fake line).

use crate::Output;
use anyhow::Context;
use logit_core::interner::resolve;
use logit_core::time::format_rfc3339_utc;
use logit_core::{
    AttrMap, Event, EventBatch, MetricKind, MetricRecord, Resource, Severity, SpanEvent, SpanKind,
    SpanLink, SpanRecord, SpanStatus, Telemetry, Value,
};
use std::cmp::Ordering;
// `std::fmt::Write`, for `write!` into a `String` -- formatting straight into the output buffer
// instead of building an intermediate `String` per number via `to_string()`/`format!`
// (`docs/design/memory.md`), the same reason `logit-outputs::influxdb` uses it.
use std::fmt::Write;
use std::path::Path;
use tokio::io::{self, AsyncWriteExt};

/// The output format [`EventDump`] renders. One variant today; see the module doc comment for why
/// this is a `match`-ready enum rather than a single hardcoded function.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    #[default]
    Human,
}

/// Renders an [`EventBatch`] as readable text. Pure -- no file descriptor, no I/O -- so every
/// format test runs directly against this, with no target of any kind involved.
#[derive(Debug, Default, Clone, Copy)]
pub struct EventDump {
    format: Format,
}

impl EventDump {
    pub fn new(format: Format) -> Self {
        Self { format }
    }

    /// Encodes `batch` as one readable block per event, in batch order. Never fails and never
    /// panics -- a debug sink's whole job is staying up when everything else is falling over, so
    /// even a `Set` metric (no real encoding yet, see `logit_core::HyperLogLog`) or a
    /// non-finite/absurd numeric value renders *something* rather than erroring.
    ///
    /// Deliberately still `&self`, with no scratch buffers held on [`EventDump`] the way
    /// `InfluxLineEncoder` holds `line`/`fields`/`tag_suffix`/`scratch` (`docs/design/memory.md`).
    /// Those exist because that encoder builds a line into a *separate* buffer before committing
    /// it (so a later rejection can't leave a half-written line in the output) and formats a
    /// non-string tag value into its own scratch space before borrowing it back out. Every
    /// `render_*` function below writes straight into `out` -- the same buffer this returns --
    /// with no intermediate buffer at any point, so there is nothing left to hoist onto the
    /// struct; the fix here was removing the per-event `AttrMap` clone (see
    /// [`render_merged_attrs`]) and the per-value `format!`/`to_string()` calls, not adding
    /// reusable state. `out` itself is still a fresh `String` per call, exactly as
    /// `InfluxLineEncoder::encode`'s `buf` is -- see that function's doc comment for why the
    /// per-batch output buffer is left as is.
    pub fn encode(&self, batch: &EventBatch) -> String {
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

/// One event's block: a timestamp/log line, then `attrs`/`metric`/`span` lines, each omitted when
/// the event carries nothing for that section. Always ends with `\n` after its last line, and
/// always emits at least the timestamp line -- a completely empty event (legal under
/// `docs/adr/multi-payload-events.md`) still gets one, since silently printing nothing would
/// be worse for a sink whose whole purpose is visibility.
///
/// `attrs` merges `resource`'s attributes underneath the event's own, the same precedence
/// `logit-outputs::influxdb`'s `render_tag_suffix` uses: without this, two batches from different
/// resources (different hosts/services) whose events otherwise match produce byte-identical debug
/// output, defeating a big part of what a human reads this sink's output to tell apart.
fn render_event_block(out: &mut String, resource: &Resource, event: &Event) {
    out.push_str(&format_rfc3339_utc(event.timestamp));
    if let Some(log) = &event.log {
        let severity = log.severity.map(severity_label).unwrap_or("-");
        out.push_str(" log[");
        out.push_str(severity);
        out.push_str("] ");
        render_value(out, &log.message);
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

fn severity_label(severity: Severity) -> &'static str {
    match severity {
        Severity::Trace => "trace",
        Severity::Debug => "debug",
        Severity::Info => "info",
        Severity::Warn => "warn",
        Severity::Error => "error",
        Severity::Fatal => "fatal",
    }
}

/// `AttrMap`'s own iteration order (sorted by interned `Symbol`) is deterministic already -- don't
/// re-sort, just render `key=value` pairs space-separated in that order.
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
/// the event's value winning on a key collision -- see [`render_event_block`]'s doc comment for
/// why the merge happens at all.
///
/// **Merge-joined rather than combined by cloning `resource`'s `AttrMap` and inserting `event`'s
/// over the top.** Both already iterate in sorted-`Symbol` order (`AttrMap::iter`'s doc comment),
/// so walking them in lockstep and preferring the event's value on an equal key produces exactly
/// the same sequence the clone-and-insert did -- without copying an `AttrMap` per event, and
/// without the `resolve` -> `intern` round trip re-inserting every key required. Mirrors
/// `logit-outputs::influxdb`'s `render_tag_suffix`, which fixed the same pattern there first.
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

/// `<name> <kind-specific fields>`, plus a trailing ` unit=<unit>` when the metric has one.
/// Single-valued kinds (`counter`/`gauge`) render as `kind=value`; multi-field kinds render the
/// kind name followed by space-separated `field=value` pairs, matching the module doc comment's
/// example block.
fn render_metric(out: &mut String, metric: &MetricRecord) {
    render_key(out, resolve(metric.name));
    out.push(' ');
    match &metric.kind {
        MetricKind::Counter(v) => {
            out.push_str("counter=");
            let _ = write!(out, "{v}");
        }
        MetricKind::Gauge(v) => {
            out.push_str("gauge=");
            let _ = write!(out, "{v}");
        }
        MetricKind::GaugeDelta(v) => {
            // Rendered distinguishably from a resolved `Gauge` (`gauge_delta`, not `gauge`), and
            // with an explicit sign, so an operator can see at a glance that this is an
            // *unresolved* relative adjustment (`docs/adr/relative-gauge-adjustments.md`) --
            // a debug sink must never silently print it as though it were an absolute value.
            out.push_str("gauge_delta=");
            // `is_sign_positive`, not `*v >= 0.0` -- `-0.0 >= 0.0` is true in IEEE-754 comparison,
            // but `f64`'s `Display` still renders `-0.0` as `"-0"`, so `>= 0.0` would double the
            // sign into the malformed `+-0`. `is_sign_positive` reads the sign bit directly and
            // excludes negative zero, matching what `Display` is about to print.
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
        MetricKind::Histogram { buckets } => {
            out.push_str("histogram");
            for (bound, count) in buckets {
                let _ = write!(out, " bucket_{bound}={count}");
            }
        }
        MetricKind::Summary { quantiles } => {
            out.push_str("summary");
            for (q, v) in quantiles {
                let _ = write!(out, " q{q}={v}");
            }
        }
        MetricKind::Set(_) => {
            // `HyperLogLog` is still a stub (`logit_core::metric::HyperLogLog`,
            // `docs/known-gaps.md`) -- there's no real value to print. A debug sink must never be
            // the thing that fails on that, unlike `logit-outputs::influxdb`, which can afford to
            // reject the metric outright and let the rest of the batch through.
            out.push_str("set=<unrepresentable>");
        }
    }
    if let Some(unit) = metric.unit {
        out.push_str(" unit=");
        render_key(out, resolve(unit));
    }
}

/// `name=... trace_id=<hex> span_id=<hex> [parent_span_id=<hex>] kind=... status=... duration=...`.
/// `event_timestamp` is the span's start time (`Event::timestamp` -- a span has no start time of
/// its own; see `logit_core::SpanRecord`'s doc comment), so duration is computed here rather than
/// stored anywhere.
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
    out.push_str(span_kind_label(span.kind));
    out.push_str(" status=");
    out.push_str(span_status_label(span.status));
    out.push_str(" duration=");
    // `saturating_sub`: a span with a corrupt/out-of-order `end_timestamp` before its own start
    // must still render *something* rather than panicking or wrapping to a nonsense huge value.
    let _ = write!(out, "{}", span.end_timestamp.saturating_sub(event_timestamp));
    out.push_str("ns");
}

/// `name=... at=<rfc3339> [attrs ...]` for one of a span's `events` (`SpanRecord::events`) --
/// omitted previously, which meant two spans differing only in their annotations rendered
/// identically.
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

/// `trace_id=<hex> span_id=<hex> [attrs ...]` for one of a span's `links` (`SpanRecord::links`) --
/// same omission as `render_span_event`, same fix.
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

fn push_hex(out: &mut String, bytes: &[u8]) {
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
}

fn span_kind_label(kind: SpanKind) -> &'static str {
    match kind {
        SpanKind::Internal => "internal",
        SpanKind::Server => "server",
        SpanKind::Client => "client",
        SpanKind::Producer => "producer",
        SpanKind::Consumer => "consumer",
    }
}

fn span_status_label(status: SpanStatus) -> &'static str {
    match status {
        SpanStatus::Unset => "unset",
        SpanStatus::Ok => "ok",
        SpanStatus::Error => "error",
    }
}

/// Renders one [`Value`]. `Bytes` renders as a byte count, never a lossy UTF-8 decode (arbitrary
/// bytes may not be valid text at all); `Str` is quoted with escapes; `Timestamp` goes through the
/// same [`format_rfc3339_utc`] the event's own timestamp line uses; `Array`/`Map` render compactly
/// and recursively.
///
/// `pub(crate)`, not private: `logit_outputs::syslog`'s message-body rendering reuses this for a
/// `Value::Map`/`Value::Array` log message (its own container-encoding fallback, not a case worth
/// a second implementation) -- but deliberately does **not** route `Value::Str` through it, since
/// this function quotes and escapes a string for a human reading a terminal
/// ([`render_quoted_str`]), which would wrap a syslog MSG's raw JSON body in quotes and double
/// its backslashes. See `syslog.rs`'s module doc for the full reasoning.
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
            // `Value::Str` is constructed only from valid UTF-8 (see its own doc comment and
            // `Value::as_str`), so this cannot panic.
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

/// Renders a map/attribute key, or a metric/unit name: bare when it's a "plain" identifier-shaped
/// string (letters, digits, `.`, `_`, `-` -- everything every built-in producer today actually
/// emits), quoted and escaped like any other string otherwise. Keys reaching this sink aren't
/// necessarily trusted local config -- a `json`-parsed access-log body can hand an event an
/// attribute keyed on arbitrary attacker-influenced text -- so a key containing a space, `=`, or
/// newline must be quoted rather than written bare: written bare, it would either misparse
/// visually (`a b=1` reads as two space-separated tokens) or, with an embedded newline, inject a
/// fake extra output line.
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

/// Quotes and escapes `s`. Beyond the usual `"`/`\`/`\n`/`\r`/`\t`, every other C0 control
/// character (`0x00..=0x1F`) and DEL (`0x7F`) is escaped as `\xHH` too -- this is a human-facing
/// *terminal* sink, and a value or key holding a raw ESC (`0x1B`) can otherwise emit a real
/// OSC/CSI escape sequence that repaints or otherwise takes over the viewer's terminal, not just
/// garbled text. Since a value can come from attacker-influenced input (a syslog line, a
/// `json`-parsed body) rather than trusted local config, this has to hold for every string this
/// sink ever writes, not just the visibly obvious ones.
///
/// Copies runs of characters that need no escaping directly into `out` with one `push_str`,
/// rather than pushing one character at a time -- the same `push_escaped` shape
/// `logit-outputs::influxdb` uses (`docs/design/memory.md`). Every character this escapes is
/// ASCII (a C0 control, DEL, or one of `"`/`\`/newline/CR/tab), so the byte offset `find` returns
/// is always exactly one character wide, never a UTF-8 continuation byte.
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

/// Escapes the one byte `needs_str_escape` matched. `write!`'s `\xHH` fallback formats straight
/// into `out` -- no intermediate `String` the way `format!` would build one.
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

/// The open sink `StdioOutput` writes to. An enum over the three concrete `tokio` I/O types rather
/// than a boxed `dyn AsyncWrite`: there are exactly three, known up front, and a `match` in `send`
/// costs nothing an indirect call wouldn't also cost.
#[derive(Debug)]
enum Sink {
    Stdout(io::Stdout),
    Stderr(io::Stderr),
    File(tokio::fs::File),
}

/// `logit_pipeline::Output` for `stdio_out`. Built via [`StdioOutput::stdout`],
/// [`StdioOutput::stderr`], or [`StdioOutput::open_path`] -- never a bare constructor, since which
/// one is legal to call depends on which `StdioTarget` config resolved to
/// (`crates/logit-cli/src/pipeline.rs::build_spec`).
#[derive(Debug)]
pub struct StdioOutput {
    sink: Sink,
    encoder: EventDump,
    /// No `Diagnostics` here, unlike most other shipped outputs: a write error propagates as an
    /// `anyhow::Error` today (matching `InfluxDbOutput`'s hard-failure stance for a non-transient
    /// sink error), so there's no `warn_throttled` call site for one to bridge -- see
    /// `docs/design/internal-telemetry.md`'s `keep`/`remove` note for the same reasoning.
    telemetry: Telemetry,
}

impl StdioOutput {
    pub fn stdout() -> Self {
        Self {
            sink: Sink::Stdout(io::stdout()),
            encoder: EventDump::default(),
            telemetry: Telemetry::default(),
        }
    }

    pub fn stderr() -> Self {
        Self {
            sink: Sink::Stderr(io::stderr()),
            encoder: EventDump::default(),
            telemetry: Telemetry::default(),
        }
    }

    /// Opens (creating if necessary) `path` in append mode, eagerly -- called from `build_spec` at
    /// config-build time, not lazily on the first `send`, so a bad path or a permissions error is a
    /// config error that fails before anything starts listening, exactly as an unset `!env`
    /// variable or a missing `lua_file` already do. `path` is used exactly as given -- resolving a
    /// relative `StdioTarget::Path` against the config file's directory (rather than the process's
    /// current working directory) is `build_spec`'s job, the same way it resolves `LuaFile`'s
    /// script path, not this constructor's.
    pub fn open_path(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("opening stdio_out target {}", path.display()))?;
        Ok(Self {
            sink: Sink::File(tokio::fs::File::from_std(file)),
            encoder: EventDump::default(),
            telemetry: Telemetry::default(),
        })
    }

    /// Attaches a telemetry handle -- see `send`'s `logit.output.batch.bytes`.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

#[async_trait::async_trait]
impl Output for StdioOutput {
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let text = self.encoder.encode(batch);
        if text.is_empty() {
            // An empty batch (`batch.events` is empty). Every *non-empty* batch always produces
            // at least a timestamp line per event -- see `render_event_block` -- so this can only
            // happen here, never because a real event encoded to nothing.
            return Ok(());
        }
        let bytes = text.into_bytes();
        self.telemetry.count("logit.output.batch.bytes", bytes.len() as f64, &[]);
        // One `write_all` plus one `flush` per batch: `Output` has no close/flush hook of its own
        // (a documented known gap, `docs/known-gaps.md`), so flushing every batch immediately is
        // what guarantees nothing sits buffered in `tokio`'s (or the OS's) write path at shutdown.
        // A write error propagates as an `anyhow::Error`, matching `InfluxDbOutput` -- a sink
        // whose file has gone away should fail the process, not silently discard. No retry: unlike
        // an HTTP 5xx, a broken stdio/file target isn't a transient condition worth waiting out.
        match &mut self.sink {
            Sink::Stdout(w) => {
                w.write_all(&bytes).await?;
                w.flush().await?;
            }
            Sink::Stderr(w) => {
                w.write_all(&bytes).await?;
                w.flush().await?;
            }
            Sink::File(w) => {
                w.write_all(&bytes).await?;
                w.flush().await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{BodyFormat, DdSketch, HyperLogLog, LogRecord, Resource};
    use std::sync::Arc;

    fn batch_with(events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(Resource::default()), events }
    }

    fn encode(events: Vec<Event>) -> String {
        EventDump::default().encode(&batch_with(events))
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
            },
        )
    }

    fn metric_event(ts: i64, name: &str, kind: MetricKind) -> Event {
        Event::metric(
            ts,
            AttrMap::new(),
            MetricRecord { name: logit_core::interner::intern(name), kind, unit: None },
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
    fn metrics_only_event_renders_a_metric_line_and_no_log_prefix() {
        let out = encode(vec![metric_event(0, "nginx.requests", MetricKind::Counter(1.0))]);
        assert_eq!(out, "1970-01-01T00:00:00.000000000Z\n  metric  nginx.requests counter=1\n");
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
        };
        let mut link_attrs = AttrMap::new();
        link_attrs.insert("relation", "follows_from");
        let link = SpanLink { trace_id: [0xEF; 16], span_id: [0x12; 8], attributes: link_attrs };
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
        event.metrics.push(MetricRecord {
            name: logit_core::interner::intern("nginx.requests"),
            kind: MetricKind::Counter(1.0),
            unit: None,
        });
        let out = encode(vec![event]);
        assert!(out.contains("log[info] \"GET /\""), "got: {out}");
        assert!(out.contains("  metric  nginx.requests counter=1"), "got: {out}");
    }

    #[test]
    fn a_completely_empty_event_renders_just_its_timestamp_line_and_does_not_panic() {
        let out = encode(vec![Event::empty(0, AttrMap::new())]);
        assert_eq!(out, "1970-01-01T00:00:00.000000000Z\n");
    }

    /// Without merging the batch's `Resource` in, two batches from different resources whose
    /// events otherwise match would render byte-identical output -- defeating a big part of what
    /// a human reads a debug sink's output to tell apart. `render_tag_suffix`
    /// (`logit-outputs::influxdb`) established the precedent this follows: resource attributes
    /// underneath the event's own, event wins on a key collision.
    #[test]
    fn resource_attributes_are_included_and_the_event_overrides_on_collision() {
        let mut resource = Resource::default();
        resource.attributes.insert("host", "web-1");
        resource.attributes.insert("env", "staging");
        let mut event = Event::empty(0, AttrMap::new());
        event.attributes.insert("env", "prod");
        let batch = EventBatch { resource: Arc::new(resource), events: vec![event] };
        let out = EventDump::default().encode(&batch);

        assert!(out.contains(r#"host="web-1""#), "got: {out}");
        assert!(out.contains(r#"env="prod""#), "event's own env should win over resource's: {out}");
        assert!(!out.contains("staging"), "got: {out}");
    }

    /// Two otherwise-identical events differing only in which resource produced them must not
    /// render identically -- the concrete regression this guards against.
    #[test]
    fn two_batches_from_different_resources_render_differently() {
        let mut resource_a = Resource::default();
        resource_a.attributes.insert("host", "web-1");
        let mut resource_b = Resource::default();
        resource_b.attributes.insert("host", "web-2");

        let out_a = EventDump::default().encode(&EventBatch {
            resource: Arc::new(resource_a),
            events: vec![Event::empty(0, AttrMap::new())],
        });
        let out_b = EventDump::default().encode(&EventBatch {
            resource: Arc::new(resource_b),
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
            MetricKind::Histogram { buckets: vec![(100.0, 5), (500.0, 2)] },
        )]);
        assert!(out.contains("bucket_100=5"), "got: {out}");
        assert!(out.contains("bucket_500=2"), "got: {out}");
    }

    #[test]
    fn summary_renders_each_quantile() {
        let out = encode(vec![metric_event(
            0,
            "req.latency",
            MetricKind::Summary { quantiles: vec![(0.99, 12.5)] },
        )]);
        assert!(out.contains("q0.99=12.5"), "got: {out}");
    }

    #[test]
    fn set_renders_unrepresentably_rather_than_erroring() {
        let out =
            encode(vec![metric_event(0, "unique.users", MetricKind::Set(HyperLogLog::default()))]);
        assert!(out.contains("unique.users set=<unrepresentable>"), "got: {out}");
    }

    /// `gauge_delta`, not `gauge` -- an unresolved relative adjustment must be visually
    /// distinguishable from a resolved absolute value (`docs/adr/relative-gauge-adjustments.md`),
    /// with an explicit sign so a positive delta doesn't read as a bare number.
    #[test]
    fn gauge_delta_renders_distinguishably_with_an_explicit_sign() {
        let out = encode(vec![metric_event(0, "conns", MetricKind::GaugeDelta(5.0))]);
        assert!(out.contains("conns gauge_delta=+5"), "got: {out}");

        let out = encode(vec![metric_event(0, "conns", MetricKind::GaugeDelta(-5.0))]);
        assert!(out.contains("conns gauge_delta=-5"), "got: {out}");
    }

    /// `-0.0 >= 0.0` is true (IEEE-754), but `Display` still renders negative zero as `"-0"` -- a
    /// naive `>= 0.0` sign check would double the sign into `+-0`. Regression for that.
    #[test]
    fn gauge_delta_negative_zero_does_not_double_the_sign() {
        let out = encode(vec![metric_event(0, "conns", MetricKind::GaugeDelta(-0.0))]);
        assert!(out.contains("conns gauge_delta=-0"), "got: {out}");
        assert!(!out.contains("+-0"), "got: {out}");
    }

    #[test]
    fn unit_appears_when_present_and_is_absent_otherwise() {
        let with_unit = encode(vec![Event::metric(
            0,
            AttrMap::new(),
            MetricRecord {
                name: logit_core::interner::intern("request_time"),
                kind: MetricKind::Gauge(0.5),
                unit: Some(logit_core::interner::intern("s")),
            },
        )]);
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

    #[test]
    fn a_quoted_string_escapes_special_characters() {
        let mut event = Event::empty(0, AttrMap::new());
        event.attributes.insert("msg", Value::str("line1\nline2\t\"quoted\"\\backslash"));
        let out = encode(vec![event]);
        assert!(out.contains(r#"msg="line1\nline2\t\"quoted\"\\backslash""#), "got: {out}");
    }

    /// A raw ESC byte in a value must never reach the terminal unescaped -- a real
    /// `\x1b[2J` (clear-screen) or other OSC/CSI sequence embedded in attacker-influenced input
    /// (a syslog line, a `json`-parsed body) would otherwise be interpreted by the viewer's
    /// terminal, not just displayed as text.
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

    /// A key containing a space, `=`, or a newline must be quoted rather than written bare --
    /// bare, it would either misparse visually or, with a newline, inject a fake extra line.
    #[test]
    fn a_key_that_is_not_a_plain_identifier_is_quoted_and_escaped() {
        let mut event = Event::empty(0, AttrMap::new());
        event.attributes.insert("weird key\nwith=stuff", "value");
        let out = encode(vec![event]);
        assert!(out.contains(r#""weird key\nwith=stuff"="value""#), "got: {out}");
    }

    #[test]
    fn attributes_come_out_in_attrmaps_sorted_order() {
        // `AttrMap` sorts by *interned `Symbol`*, not lexicographically by string content (see its
        // own doc comment) -- and `Symbol` order depends on process-wide intern history, which a
        // unit test can't pin to a specific alphabetical outcome without coupling to global state.
        // What this test actually needs to prove is narrower and robust to that: the encoder
        // renders attributes in whatever order `AttrMap::iter` already gives, rather than
        // re-sorting (or scrambling) them itself.
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
            metric_event(0, "first", MetricKind::Counter(1.0)),
            metric_event(1, "second", MetricKind::Counter(2.0)),
            metric_event(2, "third", MetricKind::Counter(3.0)),
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

        let mut output = StdioOutput::open_path(&path).expect("path should open");
        output
            .send(&batch_with(vec![metric_event(0, "x", MetricKind::Counter(1.0))]))
            .await
            .expect("send should succeed");

        let contents = std::fs::read_to_string(&path).expect("file should exist and be readable");
        assert!(contents.contains("x counter=1"), "got: {contents}");
        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn send_appends_across_multiple_batches_rather_than_truncating() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("logit-stdio-out-test-append-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let mut output = StdioOutput::open_path(&path).expect("path should open");
        output
            .send(&batch_with(vec![metric_event(0, "first", MetricKind::Counter(1.0))]))
            .await
            .expect("send should succeed");
        output
            .send(&batch_with(vec![metric_event(0, "second", MetricKind::Counter(2.0))]))
            .await
            .expect("send should succeed");

        let contents = std::fs::read_to_string(&path).expect("file should exist and be readable");
        assert!(contents.contains("first counter=1"), "got: {contents}");
        assert!(contents.contains("second counter=2"), "got: {contents}");
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn open_path_reports_a_clear_path_naming_error_for_an_unopenable_path() {
        // A path inside a directory that doesn't exist can never be opened, regardless of
        // permissions -- a reliable, environment-independent way to trigger the open failure.
        let path = std::env::temp_dir().join("logit-stdio-out-test-no-such-dir").join("x.log");
        let err = StdioOutput::open_path(&path).expect_err("expected an error");
        assert!(format!("{err:?}").contains(&path.display().to_string()), "got: {err:?}");
    }

    #[tokio::test]
    async fn send_on_an_empty_batch_writes_nothing_and_does_not_error() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("logit-stdio-out-test-empty-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let mut output = StdioOutput::open_path(&path).expect("path should open");
        output.send(&batch_with(vec![])).await.expect("send should succeed");

        let contents = std::fs::read_to_string(&path).unwrap_or_default();
        assert_eq!(contents, "", "an empty batch should write nothing");
        std::fs::remove_file(&path).ok();
    }

    /// The layer-3 example (`docs/design/internal-telemetry.md`): `logit.output.batch.bytes`
    /// should match the actual encoded length written to the file, not just be present.
    #[tokio::test]
    async fn send_records_batch_bytes_matching_the_actual_encoded_length() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("logit-stdio-out-test-telemetry-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("tap", "stdio_out", "sink");
        let mut output =
            StdioOutput::open_path(&path).expect("path should open").with_telemetry(telemetry);
        output
            .send(&batch_with(vec![metric_event(0, "x", MetricKind::Counter(1.0))]))
            .await
            .expect("send should succeed");

        let contents = std::fs::read(&path).expect("file should exist and be readable");
        std::fs::remove_file(&path).ok();

        let events = registry.drain(0);
        let recorded = events
            .iter()
            .find_map(|e| {
                e.metrics.iter().find_map(|m| match &m.kind {
                    MetricKind::Counter(v)
                        if logit_core::interner::resolve(m.name) == "logit.output.batch.bytes" =>
                    {
                        Some(*v)
                    }
                    _ => None,
                })
            })
            .expect("logit.output.batch.bytes should have been recorded");
        assert_eq!(recorded, contents.len() as f64);
    }
}
