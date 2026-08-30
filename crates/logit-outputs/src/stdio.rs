//! A general-purpose, human-facing debug sink: dumps a whole pipeline's events as readable text
//! to stdout (default), stderr, or a file -- the dev loop for this project, and the first thing
//! anyone getting started with `logit` reaches for before standing up a real backend like
//! InfluxDB.
//!
//! Split the way `InfluxDbOutput`/`InfluxLineEncoder` are (`crates/logit-outputs/src/influxdb.rs`):
//! a pure [`EventDump`] encoder (`&EventBatch` -> `String`, no file descriptor anywhere) plus the
//! thin [`StdioOutput`] that owns the open target and writes/flushes it. Every format test below
//! runs against the encoder alone.
//!
//! The encoder is deliberately built around a [`Format`] enum with a single variant today
//! (`Format::Human`), and the per-value/per-metric rendering (`render_value`/`render_metric`) is
//! kept as free functions rather than inlined into one big match -- a future user-supplied
//! `format:` template string is explicitly designed *for* here (a `Format::Template(String)`
//! variant plus a renderer that calls the same free functions) but not built now.

use crate::Output;
use anyhow::Context;
use logit_core::interner::resolve;
use logit_core::time::format_rfc3339_utc;
use logit_core::{
    AttrMap, Event, EventBatch, MetricKind, MetricRecord, Severity, SpanKind, SpanRecord,
    SpanStatus, Value,
};
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
    pub fn encode(&self, batch: &EventBatch) -> String {
        match self.format {
            Format::Human => {
                let mut out = String::new();
                for (i, event) in batch.events.iter().enumerate() {
                    if i > 0 {
                        out.push('\n');
                    }
                    render_event_block(&mut out, event);
                }
                out
            }
        }
    }
}

/// One event's block: a timestamp/log line, then `attrs`/`metric`/`span` lines, each omitted when
/// the event carries nothing for that section. Always ends with `\n` after its last line, and
/// always emits at least the timestamp line -- a completely empty event (legal under
/// `docs/adr/0012-multi-payload-events.md`) still gets one, since silently printing nothing would
/// be worse for a sink whose whole purpose is visibility.
fn render_event_block(out: &mut String, event: &Event) {
    out.push_str(&format_rfc3339_utc(event.timestamp));
    if let Some(log) = &event.log {
        let severity = log.severity.map(severity_label).unwrap_or("-");
        out.push_str(&format!(" log[{severity}] "));
        render_value(out, &log.message);
    }
    out.push('\n');

    if !event.attributes.is_empty() {
        out.push_str("  attrs   ");
        render_attrs(out, &event.attributes);
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
        out.push_str(resolve(key));
        out.push('=');
        render_value(out, value);
    }
}

/// `<name> <kind-specific fields>`, plus a trailing ` unit=<unit>` when the metric has one.
/// Single-valued kinds (`counter`/`gauge`) render as `kind=value`; multi-field kinds render the
/// kind name followed by space-separated `field=value` pairs, matching the module doc comment's
/// example block.
fn render_metric(out: &mut String, metric: &MetricRecord) {
    out.push_str(resolve(metric.name));
    out.push(' ');
    match &metric.kind {
        MetricKind::Counter(v) => {
            out.push_str("counter=");
            out.push_str(&v.to_string());
        }
        MetricKind::Gauge(v) => {
            out.push_str("gauge=");
            out.push_str(&v.to_string());
        }
        MetricKind::Distribution(sketch) => {
            out.push_str("distribution count=");
            out.push_str(&sketch.count().to_string());
            for q in [0.5, 0.9, 0.99] {
                if let Some(v) = sketch.quantile(q) {
                    out.push_str(&format!(" p{}={v}", (q * 100.0).round() as u32));
                }
            }
        }
        MetricKind::Histogram { buckets } => {
            out.push_str("histogram");
            for (bound, count) in buckets {
                out.push_str(&format!(" bucket_{bound}={count}"));
            }
        }
        MetricKind::Summary { quantiles } => {
            out.push_str("summary");
            for (q, v) in quantiles {
                out.push_str(&format!(" q{q}={v}"));
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
        out.push_str(resolve(unit));
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
    out.push_str(&span.end_timestamp.saturating_sub(event_timestamp).to_string());
    out.push_str("ns");
}

fn push_hex(out: &mut String, bytes: &[u8]) {
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
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
fn render_value(out: &mut String, value: &Value) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(&b.to_string()),
        Value::I64(i) => out.push_str(&i.to_string()),
        Value::U64(u) => out.push_str(&u.to_string()),
        Value::F64(f) => out.push_str(&f.to_string()),
        Value::Bytes(b) => out.push_str(&format!("<{} bytes>", b.len())),
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
                out.push_str(resolve(key));
                out.push('=');
                render_value(out, value);
            }
            out.push('}');
        }
    }
}

fn render_quoted_str(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out.push('"');
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
}

impl StdioOutput {
    pub fn stdout() -> Self {
        Self { sink: Sink::Stdout(io::stdout()), encoder: EventDump::default() }
    }

    pub fn stderr() -> Self {
        Self { sink: Sink::Stderr(io::stderr()), encoder: EventDump::default() }
    }

    /// Opens (creating if necessary) `path` in append mode, eagerly -- called from `build_spec` at
    /// config-build time, not lazily on the first `send`, so a bad path or a permissions error is a
    /// config error that fails before anything starts listening, exactly as an unset `!env`
    /// variable or a missing `lua_file` already do.
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
        })
    }
}

#[async_trait::async_trait]
impl Output for StdioOutput {
    async fn send(&mut self, batch: EventBatch) -> anyhow::Result<()> {
        let text = self.encoder.encode(&batch);
        if text.is_empty() {
            // An empty batch (`batch.events` is empty). Every *non-empty* batch always produces
            // at least a timestamp line per event -- see `render_event_block` -- so this can only
            // happen here, never because a real event encoded to nothing.
            return Ok(());
        }
        let bytes = text.into_bytes();
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
            LogRecord { message: Value::str(message), severity, body_format: BodyFormat::Raw },
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
            .send(batch_with(vec![metric_event(0, "x", MetricKind::Counter(1.0))]))
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
            .send(batch_with(vec![metric_event(0, "first", MetricKind::Counter(1.0))]))
            .await
            .expect("send should succeed");
        output
            .send(batch_with(vec![metric_event(0, "second", MetricKind::Counter(2.0))]))
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
        output.send(batch_with(vec![])).await.expect("send should succeed");

        let contents = std::fs::read_to_string(&path).unwrap_or_default();
        assert_eq!(contents, "", "an empty batch should write nothing");
        std::fs::remove_file(&path).ok();
    }
}
