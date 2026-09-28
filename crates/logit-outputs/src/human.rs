//! The human render `stdio_out` and `file_out` write by default: one exhaustive, indented block
//! per event, for a person at a terminal (`docs/adr/human-render-block-format.md`). It isn't an
//! export format and nothing parses it; a program reads [`Format::Json`], `crate::ndjson`.
//!
//! This module doc is the canonical copy of the grammar.
//!
//! ## Block
//!
//! Every event starts with [`EVENT_DIVIDER`], a line of 80 `-`, with no blank line between events.
//! Sections follow in this order, indented two spaces per level:
//!
//! ```text
//! --------------------------------------------------------------------------------
//! timestamp: <ts>
//! log:
//!   severity: info
//!   format: raw
//!   message: GET /index.html HTTP/1.1
//!   event_name: "http.request"
//!   observed_timestamp: <ts>
//!   trace_id: <32 hex>
//!   span_id: <16 hex>
//!   trace_flags: 1
//!   dropped_attributes_count: 2
//! metrics:
//!   - name: http.server.requests
//!     kind: sum
//!     <the kind's fields, per the table below>
//!     unit: ms
//!     description: "Requests served"
//!     start_timestamp: <ts>
//!     flags: 3
//!     no_recorded_value: true
//!     exemplars:
//!       - timestamp: <ts>
//!         value: 0.25
//!         trace_id: <32 hex>
//!         span_id: <16 hex>
//!         trace_flags: 0
//!         attributes:
//!           k: "v"
//! span:
//!   name: "GET /index.html"
//!   trace_id: <32 hex>
//!   span_id: <16 hex>
//!   parent_span_id: <16 hex>
//!   kind: server
//!   status: ok
//!   status_message: "boom"
//!   trace_state: "vendor=x"
//!   flags: 1
//!   start: <ts>
//!   end: <ts>
//!   duration: 12300000ns
//!   dropped_attributes_count: 1
//!   dropped_events_count: 1
//!   dropped_links_count: 1
//!   events:
//!     - name: "retrying"
//!       timestamp: <ts>
//!       attributes:
//!         retry: 1
//!       dropped_attributes_count: 1
//!   links:
//!     - trace_id: <32 hex>
//!       span_id: <16 hex>
//!       flags: 1
//!       trace_state: "vendor=x"
//!       attributes:
//!         k: "v"
//!       dropped_attributes_count: 1
//! attributes:
//!   http.request.method: "GET"
//! resource:
//!   attributes:
//!     host.name: "web-1"
//!   schema_url: "https://opentelemetry.io/schemas/1.26.0"
//!   dropped_attributes_count: 1
//! scope:
//!   name: "io.opentelemetry.nginx"
//!   version: "1.2.0"
//!   attributes:
//!     k: "v"
//!   schema_url: "https://opentelemetry.io/schemas/1.26.0"
//!   dropped_attributes_count: 1
//! ```
//!
//! `<ts>` is RFC 3339 UTC with nine fractional digits (`logit_core::time::write_rfc3339_utc`).
//! `span.start` is the event's own timestamp, repeated so the span block reads alone, and
//! `duration` is `end - start` in integer nanoseconds, saturating. `resource` and `scope` are the
//! batch's, repeated in every event's block.
//!
//! ## Omission
//!
//! A section or optional field that is absent, empty, or zero is omitted, never rendered as
//! `null` or `0`. The divider and `timestamp:` always appear, so an empty event is still visible.
//! Always present once their section is: `log.format`, `log.trace_flags` (when `trace_id` is),
//! a metric's `name` and `kind`, and the span's identity, kind, status, and times. `log` appears
//! when the event has one, `metrics` when non-empty, `span` when present, `attributes` when
//! non-empty, `resource` when it has attributes, a `schema_url`, or a non-zero dropped count, and
//! `scope` whenever the batch has one, even with every field inside it empty.
//!
//! ## Metric kinds
//!
//! `kind:` is `MetricKind::name`, then that kind's fields:
//!
//! | kind | fields |
//! |---|---|
//! | `sum` | `value`, `temporality`, `monotonic` |
//! | `gauge` | `value` |
//! | `gauge_delta` | `delta`, with an explicit sign (`+5`, `-2`, `+0`, `-0`) |
//! | `samples` | `values: [1, 2.5]`, `sample_rate` |
//! | `set_members` | `members: ["a", "b"]`, each member a quoted string, lossily decoded |
//! | `distribution` | `count`, `sum`, then `min`, `max`, `avg` (`sum / count`), `p50`, `p90`, `p95`, `p99`, each only when the sketch has observations |
//! | `set` | `estimate` |
//! | `histogram` | `temporality`, `buckets:` a block of `<bound>: <count>` in wire order (each count that bucket's own, the last bound `inf`), then `sum`, `min`, `max` when present |
//! | `exponential_histogram` | `scale`, `temporality`, `count`, `zero_count`, `zero_threshold`, `positive:`/`negative:` each a block of `offset` and `counts: [..]` (omitted when its counts are empty), then `sum`, `min`, `max` when present |
//! | `summary` | `count`, `sum`, `quantiles:` a block of `<q>: <value>` in wire order |
//!
//! A metric flagged `no_recorded_value` (OTLP's `NO_RECORDED_VALUE`) omits every field of its
//! kind: the kind's default payload is not a reading (`MetricRecord::flags`). `flags` shows the
//! raw bitmask whenever it's non-zero. An empty `buckets`/`quantiles` renders as `{}`.
//!
//! ## Values and keys
//!
//! - Scalars: `null`, `true`/`false`, integers and floats through `Display` (`NaN`, `inf`,
//!   `-inf`), `Str` quoted and escaped, `Timestamp` as `<ts>`.
//! - `Bytes` render as `b"..."`: each valid UTF-8 run escaped as a string is, each invalid byte
//!   as `\xHH`. The same walk, without the `b`, renders the byte-string fields that are text by
//!   convention (`status_message`, `trace_state`, `schema_url`, a scope's `name`/`version`).
//! - A `Map` nests as a block under its key, one `key: value` line per entry in `AttrMap` order.
//! - An `Array` whose elements are all scalars stays inline, `[a, b]`. An `Array` holding any
//!   `Map` or `Array` becomes a block of `- ` items, each item's first line after its `- ` and
//!   the rest two columns further in.
//! - An empty `Map` or `Array` renders inline as `{}` or `[]`.
//! - A key, a metric name, and a unit render bare when identifier-shaped (ASCII letters, digits,
//!   `.`, `_`, `-`) and quoted and escaped otherwise.
//!
//! A quoted string escapes `"`, `\`, `\n`, `\r`, `\t`, and every other C0 control and DEL as
//! `\xHH`. Every string here can come from a peer's bytes (a syslog line, a JSON body), and a raw
//! ESC would emit an OSC/CSI sequence that drives the viewer's terminal, while a bare key holding
//! a space, `:`, or newline would forge extra fields or lines.
//!
//! ## Message
//!
//! A `log.message` that is a `Str` renders unquoted, in one of two [`MessageMode`]s:
//!
//! - [`MessageMode::Escaped`] (the default): every C0 control and DEL is escaped (`\n`, `\r`,
//!   `\t`, `\x1b`), so the message stays on one line. `"` and `\` pass through.
//! - [`MessageMode::Multiline`]: `\n` becomes a line break followed by 11 spaces (the width of
//!   `  message: `), `\t` passes through raw, and everything else is escaped as in `Escaped`.
//!
//! An empty message renders as `message: ` in both modes. A `log.message` of any other `Value`
//! uses the value grammar above.

use bytes::Bytes;
use logit_core::interner::resolve;
use logit_core::time::write_rfc3339_utc;
use logit_core::trace::push_hex;
use logit_core::{
    AttrMap, Event, EventBatch, Exemplar, MetricKind, MetricRecord, Resource, Scope, SpanRecord,
    TraceRef, Value,
};
use logit_proto::{CodecError, Encoder};
use std::fmt::Display;
// For `write!` straight into the output buffer, never a per-field `to_string()`/`format!`
// (`docs/design/memory.md`).
use std::fmt::Write;

/// The line every event's block starts with: 80 `-`.
pub const EVENT_DIVIDER: &str =
    "--------------------------------------------------------------------------------";

/// The output format [`EventDump`] renders.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// This module's block per event.
    #[default]
    Human,
    /// One JSON object per event per line ([`crate::ndjson`]).
    Json,
}

/// How a `Str` `log.message` renders; see the module doc's "Message" section.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum MessageMode {
    /// One line: every control character escaped.
    #[default]
    Escaped,
    /// `\n` breaks the line, continuation lines aligned under the message's first character.
    Multiline,
}

/// Renders an [`EventBatch`] as readable text. Pure: no file descriptor, no I/O.
#[derive(Debug, Default, Clone, Copy)]
pub struct EventDump {
    format: Format,
    message: MessageMode,
}

impl EventDump {
    pub fn new(format: Format) -> Self {
        Self { format, message: MessageMode::default() }
    }

    pub fn with_message_mode(mut self, message: MessageMode) -> Self {
        self.message = message;
        self
    }

    /// Renders `batch` as one block, or under [`Format::Json`] one JSON line, per event, in
    /// batch order.
    ///
    /// Never fails and never panics: a debug sink has to stay up when everything else is falling
    /// over, so even a non-finite numeric value renders something.
    ///
    /// `&self` with no scratch buffers, unlike `InfluxLineEncoder` (`docs/design/memory.md`):
    /// every renderer writes straight into `out`, so there is nothing to hoist. `out` is a fresh
    /// `String` per call, as `InfluxLineEncoder::encode`'s `buf` is.
    ///
    /// Named `render`, not `encode`: Rust resolves an inherent method over a same-named trait
    /// method with no ambiguity error, so an inherent `encode` would keep every `dump.encode(..)`
    /// call site on this `String`-returning method instead of `Encoder::encode`.
    pub fn render(&self, batch: &EventBatch) -> String {
        match self.format {
            Format::Human => {
                let mut out = String::new();
                for event in &batch.events {
                    render_event_block(&mut out, batch, event, self.message);
                }
                out
            }
            Format::Json => {
                let mut out = String::new();
                crate::ndjson::render_ndjson(&mut out, batch);
                out
            }
        }
    }
}

/// What lets `StreamOutput` be generic over its encoder. Always `Ok`: [`EventDump::render`]
/// never fails.
impl Encoder for EventDump {
    fn encode(&mut self, batch: &EventBatch) -> Result<Bytes, CodecError> {
        Ok(Bytes::from(self.render(batch).into_bytes()))
    }
}

/// One event's block, in the module doc's section order. Always ends with `\n`.
fn render_event_block(out: &mut String, batch: &EventBatch, event: &Event, mode: MessageMode) {
    out.push_str(EVENT_DIVIDER);
    out.push('\n');
    out.push_str("timestamp: ");
    write_rfc3339_utc(out, event.timestamp);
    out.push('\n');

    if let Some(log) = &event.log {
        header(out, 0, "log");
        if let Some(severity) = log.severity {
            field(out, 2, "severity");
            out.push_str(severity.as_str());
            out.push('\n');
        }
        field(out, 2, "format");
        out.push_str(log.body_format.as_str());
        out.push('\n');
        match &log.message {
            Value::Str(s) => {
                field(out, 2, "message");
                push_message(out, utf8(s), mode);
                out.push('\n');
            }
            other => value_field(out, 2, "message", other),
        }
        if let Some(name) = log.event_name {
            field(out, 2, "event_name");
            render_quoted_str(out, resolve(name));
            out.push('\n');
        }
        if log.observed_timestamp != 0 {
            ts_field(out, 2, "observed_timestamp", log.observed_timestamp);
        }
        if let Some(trace) = &log.trace {
            trace_fields(out, 2, trace);
        }
        nonzero_field(out, 2, "dropped_attributes_count", log.dropped_attributes_count);
    }

    if !event.metrics.is_empty() {
        header(out, 0, "metrics");
        for metric in &event.metrics {
            render_metric(out, metric);
        }
    }

    if let Some(span) = &event.span {
        render_span(out, event.timestamp, span);
    }

    if !event.attributes.is_empty() {
        header(out, 0, "attributes");
        write_map_entries(out, &event.attributes, 2, false);
    }

    render_resource(out, &batch.resource);

    if let Some(scope) = &batch.scope {
        render_scope(out, scope);
    }
}

/// One `- name: ...` item of the `metrics:` list; its other fields sit at column 4.
fn render_metric(out: &mut String, metric: &MetricRecord) {
    const COL: usize = 4;
    push_indent(out, 2);
    out.push_str("- name: ");
    render_key(out, resolve(metric.name));
    out.push('\n');
    field(out, COL, "kind");
    out.push_str(metric.kind.name());
    out.push('\n');

    let no_recorded_value = metric.is_no_recorded_value();
    if !no_recorded_value {
        render_metric_kind(out, COL, &metric.kind);
    }

    if let Some(unit) = metric.unit {
        field(out, COL, "unit");
        render_key(out, resolve(unit));
        out.push('\n');
    }
    if let Some(description) = metric.description {
        field(out, COL, "description");
        render_quoted_str(out, resolve(description));
        out.push('\n');
    }
    if metric.start_timestamp != 0 {
        ts_field(out, COL, "start_timestamp", metric.start_timestamp);
    }
    nonzero_field(out, COL, "flags", metric.flags);
    if no_recorded_value {
        display_field(out, COL, "no_recorded_value", true);
    }
    if !metric.exemplars.is_empty() {
        header(out, COL, "exemplars");
        for exemplar in &metric.exemplars {
            render_exemplar(out, COL + 2, exemplar);
        }
    }
}

/// The kind-specific fields of the module doc's metric-kinds table, at column `col`.
fn render_metric_kind(out: &mut String, col: usize, kind: &MetricKind) {
    match kind {
        MetricKind::Sum(s) => {
            display_field(out, col, "value", s.value);
            str_field(out, col, "temporality", s.temporality.as_str());
            display_field(out, col, "monotonic", s.monotonic);
        }
        MetricKind::Gauge(v) => display_field(out, col, "value", v),
        MetricKind::GaugeDelta(v) => {
            // An explicit sign, so an unresolved relative adjustment never reads as an absolute
            // value (`docs/adr/relative-gauge-adjustments.md`). `is_sign_positive`, not
            // `*v >= 0.0`: `-0.0 >= 0.0` is true, but `Display` prints `-0`, giving `+-0`.
            field(out, col, "delta");
            if v.is_sign_positive() {
                out.push('+');
            }
            let _ = write!(out, "{v}");
            out.push('\n');
        }
        MetricKind::Samples(s) => {
            field(out, col, "values");
            out.push('[');
            for (i, v) in s.values.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                let _ = write!(out, "{v}");
            }
            out.push_str("]\n");
            display_field(out, col, "sample_rate", s.sample_rate);
        }
        MetricKind::SetMembers(members) => {
            field(out, col, "members");
            out.push('[');
            for (i, m) in members.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                // `Cow::Borrowed` for valid UTF-8, so only an invalid member allocates.
                render_quoted_str(out, &String::from_utf8_lossy(m));
            }
            out.push_str("]\n");
        }
        MetricKind::Distribution(sketch) => {
            let count = sketch.count();
            display_field(out, col, "count", count);
            display_field(out, col, "sum", sketch.sum());
            if let Some(min) = sketch.min() {
                display_field(out, col, "min", min);
            }
            if let Some(max) = sketch.max() {
                display_field(out, col, "max", max);
            }
            if count > 0 {
                display_field(out, col, "avg", sketch.sum() / count as f64);
            }
            for (name, q) in [("p50", 0.5), ("p90", 0.9), ("p95", 0.95), ("p99", 0.99)] {
                if let Some(v) = sketch.quantile(q) {
                    display_field(out, col, name, v);
                }
            }
        }
        MetricKind::Set(hll) => display_field(out, col, "estimate", hll.estimate()),
        MetricKind::Histogram(h) => {
            str_field(out, col, "temporality", h.temporality.as_str());
            pairs_field(out, col, "buckets", h.buckets.iter().map(|(b, c)| (b, c)));
            optional_sum_min_max(out, col, h.sum, h.min, h.max);
        }
        MetricKind::ExponentialHistogram(e) => {
            display_field(out, col, "scale", e.scale);
            str_field(out, col, "temporality", e.temporality.as_str());
            display_field(out, col, "count", e.count);
            display_field(out, col, "zero_count", e.zero_count);
            display_field(out, col, "zero_threshold", e.zero_threshold);
            for (name, (offset, counts)) in [("positive", &e.positive), ("negative", &e.negative)] {
                if counts.is_empty() {
                    continue;
                }
                header(out, col, name);
                display_field(out, col + 2, "offset", offset);
                field(out, col + 2, "counts");
                out.push('[');
                for (i, c) in counts.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    let _ = write!(out, "{c}");
                }
                out.push_str("]\n");
            }
            optional_sum_min_max(out, col, e.sum, e.min, e.max);
        }
        MetricKind::Summary(s) => {
            display_field(out, col, "count", s.count);
            display_field(out, col, "sum", s.sum);
            pairs_field(out, col, "quantiles", s.quantiles.iter().map(|(q, v)| (q, v)));
        }
    }
}

/// `name:` then one `<k>: <v>` line per pair at `col + 2`, or `name: {}` when there are none.
fn pairs_field<'a, K: Display + 'a, V: Display + 'a>(
    out: &mut String,
    col: usize,
    name: &str,
    pairs: impl ExactSizeIterator<Item = (&'a K, &'a V)>,
) {
    if pairs.len() == 0 {
        field(out, col, name);
        out.push_str("{}\n");
        return;
    }
    header(out, col, name);
    for (k, v) in pairs {
        push_indent(out, col + 2);
        let _ = writeln!(out, "{k}: {v}");
    }
}

fn optional_sum_min_max(
    out: &mut String,
    col: usize,
    sum: Option<f64>,
    min: Option<f64>,
    max: Option<f64>,
) {
    for (name, v) in [("sum", sum), ("min", min), ("max", max)] {
        if let Some(v) = v {
            display_field(out, col, name, v);
        }
    }
}

/// One `- timestamp: ...` item of a metric's `exemplars:` list, its item marker at `col`.
fn render_exemplar(out: &mut String, col: usize, exemplar: &Exemplar) {
    push_indent(out, col);
    out.push_str("- timestamp: ");
    write_rfc3339_utc(out, exemplar.timestamp);
    out.push('\n');
    let col = col + 2;
    display_field(out, col, "value", exemplar.value);
    if let Some(trace) = &exemplar.trace {
        trace_fields(out, col, trace);
    }
    attributes_field(out, col, &exemplar.filtered_attributes);
}

/// `trace_id`, `span_id` when present, and `trace_flags`, for a log's or an exemplar's
/// [`TraceRef`].
fn trace_fields(out: &mut String, col: usize, trace: &TraceRef) {
    hex_field(out, col, "trace_id", &trace.trace_id);
    if let Some(span_id) = &trace.span_id {
        hex_field(out, col, "span_id", span_id);
    }
    display_field(out, col, "trace_flags", trace.flags);
}

/// The `span:` section. `start` is `event_timestamp`: a `SpanRecord` has no start of its own.
fn render_span(out: &mut String, event_timestamp: i64, span: &SpanRecord) {
    header(out, 0, "span");
    value_field(out, 2, "name", &span.name);
    hex_field(out, 2, "trace_id", &span.trace_id);
    hex_field(out, 2, "span_id", &span.span_id);
    if let Some(parent) = &span.parent_span_id {
        hex_field(out, 2, "parent_span_id", parent);
    }
    str_field(out, 2, "kind", span.kind.as_str());
    str_field(out, 2, "status", span.status.as_str());
    if let Some(ext) = &span.ext {
        if let Some(message) = &ext.status_message {
            text_bytes_field(out, 2, "status_message", message);
        }
        if let Some(state) = &ext.trace_state {
            text_bytes_field(out, 2, "trace_state", state);
        }
    }
    nonzero_field(out, 2, "flags", span.flags);
    ts_field(out, 2, "start", event_timestamp);
    ts_field(out, 2, "end", span.end_timestamp);
    field(out, 2, "duration");
    // `saturating_sub`: an `end_timestamp` before the start must render, not panic or wrap.
    let _ = writeln!(out, "{}ns", span.end_timestamp.saturating_sub(event_timestamp));
    if let Some(ext) = &span.ext {
        nonzero_field(out, 2, "dropped_attributes_count", ext.dropped_attributes_count);
        nonzero_field(out, 2, "dropped_events_count", ext.dropped_events_count);
        nonzero_field(out, 2, "dropped_links_count", ext.dropped_links_count);
    }
    if !span.events.is_empty() {
        header(out, 2, "events");
        for span_event in &span.events {
            push_indent(out, 4);
            out.push_str("- name:");
            write_value_after_key(out, &span_event.name, 6);
            ts_field(out, 6, "timestamp", span_event.timestamp);
            attributes_field(out, 6, &span_event.attributes);
            nonzero_field(out, 6, "dropped_attributes_count", span_event.dropped_attributes_count);
        }
    }
    if !span.links.is_empty() {
        header(out, 2, "links");
        for link in &span.links {
            push_indent(out, 4);
            out.push_str("- trace_id: ");
            push_hex(out, &link.trace_id);
            out.push('\n');
            hex_field(out, 6, "span_id", &link.span_id);
            nonzero_field(out, 6, "flags", link.flags);
            if let Some(state) = &link.trace_state {
                text_bytes_field(out, 6, "trace_state", state);
            }
            attributes_field(out, 6, &link.attributes);
            nonzero_field(out, 6, "dropped_attributes_count", link.dropped_attributes_count);
        }
    }
}

fn render_resource(out: &mut String, resource: &Resource) {
    if resource.attributes.is_empty()
        && resource.schema_url.is_none()
        && resource.dropped_attributes_count == 0
    {
        return;
    }
    header(out, 0, "resource");
    attributes_field(out, 2, &resource.attributes);
    if let Some(url) = &resource.schema_url {
        text_bytes_field(out, 2, "schema_url", url);
    }
    nonzero_field(out, 2, "dropped_attributes_count", resource.dropped_attributes_count);
}

fn render_scope(out: &mut String, scope: &Scope) {
    header(out, 0, "scope");
    if !scope.name.is_empty() {
        text_bytes_field(out, 2, "name", &scope.name);
    }
    if !scope.version.is_empty() {
        text_bytes_field(out, 2, "version", &scope.version);
    }
    attributes_field(out, 2, &scope.attributes);
    if let Some(url) = &scope.schema_url {
        text_bytes_field(out, 2, "schema_url", url);
    }
    nonzero_field(out, 2, "dropped_attributes_count", scope.dropped_attributes_count);
}

// --- line primitives ---

/// 64 spaces, one `push_str` per indent; a deeper nested value loops over it.
const SPACES: &str = "                                                                ";

fn push_indent(out: &mut String, mut col: usize) {
    while col > SPACES.len() {
        out.push_str(SPACES);
        col -= SPACES.len();
    }
    out.push_str(&SPACES[..col]);
}

/// `<indent>name: `; the caller writes the value and the trailing `\n`.
fn field(out: &mut String, col: usize, name: &str) {
    push_indent(out, col);
    out.push_str(name);
    out.push_str(": ");
}

/// `<indent>name:\n`, opening a nested block.
fn header(out: &mut String, col: usize, name: &str) {
    push_indent(out, col);
    out.push_str(name);
    out.push_str(":\n");
}

fn display_field(out: &mut String, col: usize, name: &str, value: impl Display) {
    field(out, col, name);
    let _ = writeln!(out, "{value}");
}

fn nonzero_field(out: &mut String, col: usize, name: &str, value: u32) {
    if value != 0 {
        display_field(out, col, name, value);
    }
}

fn str_field(out: &mut String, col: usize, name: &str, value: &str) {
    field(out, col, name);
    out.push_str(value);
    out.push('\n');
}

fn ts_field(out: &mut String, col: usize, name: &str, nanos: i64) {
    field(out, col, name);
    write_rfc3339_utc(out, nanos);
    out.push('\n');
}

fn hex_field(out: &mut String, col: usize, name: &str, bytes: &[u8]) {
    field(out, col, name);
    push_hex(out, bytes);
    out.push('\n');
}

/// A byte-string field that is text by convention: quoted, with invalid UTF-8 as `\xHH`.
fn text_bytes_field(out: &mut String, col: usize, name: &str, bytes: &[u8]) {
    field(out, col, name);
    out.push('"');
    push_escaped_bytes(out, bytes);
    out.push_str("\"\n");
}

/// `attributes:` and its entries one level in, or nothing for an empty map.
fn attributes_field(out: &mut String, col: usize, attrs: &AttrMap) {
    if !attrs.is_empty() {
        header(out, col, "attributes");
        write_map_entries(out, attrs, col + 2, false);
    }
}

// --- values ---

/// `<indent>name:` then `value` by the module doc's value grammar.
fn value_field(out: &mut String, col: usize, name: &str, value: &Value) {
    push_indent(out, col);
    out.push_str(name);
    out.push(':');
    write_value_after_key(out, value, col);
}

/// Finishes a `key:` line whose key starts at column `col`: ` <inline value>\n`, or a line break
/// and a nested block at `col + 2` for a non-empty `Map` or an `Array` holding a container.
fn write_value_after_key(out: &mut String, value: &Value, col: usize) {
    match value {
        Value::Map(map) if !map.is_empty() => {
            out.push('\n');
            write_map_entries(out, map, col + 2, false);
        }
        Value::Array(items) if is_block_array(items) => {
            out.push('\n');
            write_array_items(out, items, col + 2, false);
        }
        _ => {
            out.push(' ');
            write_inline(out, value);
            out.push('\n');
        }
    }
}

/// One `key: value` line per entry at `col`, in `AttrMap` order. `first_inline` means the cursor
/// already sits at `col` after a `- ` item marker, so the first entry writes no indent.
fn write_map_entries(out: &mut String, map: &AttrMap, col: usize, first_inline: bool) {
    for (i, (key, value)) in map.iter().enumerate() {
        if i > 0 || !first_inline {
            push_indent(out, col);
        }
        render_key(out, resolve(key));
        out.push(':');
        write_value_after_key(out, value, col);
    }
}

/// One `- item` per element with the marker at `col`; `first_inline` as in
/// [`write_map_entries`].
fn write_array_items(out: &mut String, items: &[Value], col: usize, first_inline: bool) {
    for (i, item) in items.iter().enumerate() {
        if i > 0 || !first_inline {
            push_indent(out, col);
        }
        out.push_str("- ");
        match item {
            Value::Map(map) if !map.is_empty() => write_map_entries(out, map, col + 2, true),
            Value::Array(inner) if is_block_array(inner) => {
                write_array_items(out, inner, col + 2, true)
            }
            _ => {
                write_inline(out, item);
                out.push('\n');
            }
        }
    }
}

fn is_block_array(items: &[Value]) -> bool {
    items.iter().any(|v| matches!(v, Value::Map(_) | Value::Array(_)))
}

/// A value that fits on one line: a scalar, an empty `Map`, or an `Array` of scalars.
/// [`write_value_after_key`] and [`write_array_items`] route every other value to a block.
fn write_inline(out: &mut String, value: &Value) {
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
            out.push_str("b\"");
            push_escaped_bytes(out, b);
            out.push('"');
        }
        Value::Str(s) => render_quoted_str(out, utf8(s)),
        Value::Timestamp(ns) => write_rfc3339_utc(out, *ns),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_inline(out, item);
            }
            out.push(']');
        }
        Value::Map(map) if map.is_empty() => out.push_str("{}"),
        // Unreachable from the block renderers, which nest a non-empty map; kept total so a
        // future caller can't panic the sink.
        Value::Map(_) => render_value_inline(out, value),
    }
}

/// `Value::Str` is constructed only from valid UTF-8, so this cannot panic.
fn utf8(s: &[u8]) -> &str {
    std::str::from_utf8(s).expect("Value::Str is always valid UTF-8")
}

/// The compact single-line render: `Str` quoted and escaped; `Bytes` as `<N bytes>`, never a lossy
/// UTF-8 decode; `Timestamp` as RFC 3339; `Array` as `[a, b]` and `Map` as `{k=v, k2=v2}`,
/// recursively.
///
/// For `syslog_out`'s container fallback (a `Map` or `Array` value); syslog never routes a `Str`
/// here, since the quoting would mangle a raw MSG body (`syslog.rs`'s module doc). Its output is
/// part of `syslog_out`'s wire, independent of the block grammar above.
pub(crate) fn render_value_inline(out: &mut String, value: &Value) {
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
        Value::Str(s) => render_quoted_str(out, utf8(s)),
        Value::Timestamp(ns) => write_rfc3339_utc(out, *ns),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                render_value_inline(out, item);
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
                render_value_inline(out, value);
            }
            out.push('}');
        }
    }
}

// --- escaping ---

/// Renders a map/attribute key or a metric/unit name: bare when identifier-shaped (letters,
/// digits, `.`, `_`, `-`), quoted and escaped otherwise. A `json`-parsed body can key an attribute
/// on a peer's text, and written bare, `a b: 1` reads as a forged field and an embedded newline
/// forges a line.
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
/// sequence that takes over the viewer's terminal.
fn render_quoted_str(out: &mut String, s: &str) {
    out.push('"');
    push_escaped_str(out, s);
    out.push('"');
}

/// [`render_quoted_str`]'s body, unquoted. Copies each unescaped run with one `push_str`, like
/// influxdb's `push_escaped` (`docs/design/memory.md`). Every escaped character is ASCII, so the
/// byte offset `find` returns is one character wide, never a UTF-8 continuation byte.
fn push_escaped_str(out: &mut String, s: &str) {
    let mut rest = s;
    while let Some(i) = rest.find(needs_str_escape) {
        out.push_str(&rest[..i]);
        push_escaped_char(out, rest.as_bytes()[i]);
        rest = &rest[i + 1..];
    }
    out.push_str(rest);
}

/// Bytes that need not be UTF-8: each valid run as [`push_escaped_str`] writes it, each invalid
/// byte as `\xHH`. An invalid byte is always `0x80` or above, so it never reads as an escaped
/// control character.
fn push_escaped_bytes(out: &mut String, bytes: &[u8]) {
    for chunk in bytes.utf8_chunks() {
        push_escaped_str(out, chunk.valid());
        for b in chunk.invalid() {
            let _ = write!(out, "\\x{b:02x}");
        }
    }
}

fn needs_str_escape(c: char) -> bool {
    matches!(c, '"' | '\\') || is_control(c)
}

/// A C0 control or DEL: what a message escapes in both [`MessageMode`]s.
fn is_control(c: char) -> bool {
    (c as u32) < 0x20 || c as u32 == 0x7f
}

/// Escapes the one byte a `find` predicate matched, formatting `\xHH` straight into `out`.
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

/// The width of `  message: `: where a [`MessageMode::Multiline`] continuation line starts.
const MESSAGE_CONTINUATION: &str = "\n           ";

/// A `Str` message, unquoted, per [`MessageMode`].
fn push_message(out: &mut String, text: &str, mode: MessageMode) {
    let mut rest = text;
    while let Some(i) = rest.find(is_control) {
        out.push_str(&rest[..i]);
        let b = rest.as_bytes()[i];
        match (mode, b) {
            (MessageMode::Multiline, b'\n') => out.push_str(MESSAGE_CONTINUATION),
            (MessageMode::Multiline, b'\t') => out.push('\t'),
            _ => push_escaped_char(out, b),
        }
        rest = &rest[i + 1..];
    }
    out.push_str(rest);
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{
        BodyFormat, DdSketch, ExpHistogram, Histogram, HyperLogLog, LogRecord, Samples, Severity,
        SpanEvent, SpanExt, SpanKind, SpanLink, SpanStatus, Summary, Temporality,
    };
    use std::sync::Arc;

    const T0: &str = "timestamp: 1970-01-01T00:00:00.000000000Z";

    /// The expected render: each line followed by `\n`, so a test reads as the block itself.
    fn block(lines: &[&str]) -> String {
        let mut out = String::new();
        for line in lines {
            out.push_str(line);
            out.push('\n');
        }
        out
    }

    fn batch(events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
    }

    fn render(events: Vec<Event>) -> String {
        EventDump::default().render(&batch(events))
    }

    fn render_multiline(events: Vec<Event>) -> String {
        EventDump::default().with_message_mode(MessageMode::Multiline).render(&batch(events))
    }

    fn log_record(message: Value) -> LogRecord {
        LogRecord {
            message,
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        }
    }

    fn log_event(message: &str) -> Event {
        Event::log(0, AttrMap::new(), log_record(Value::str(message)))
    }

    fn metric(name: &str, kind: MetricKind) -> MetricRecord {
        MetricRecord::new(logit_core::interner::intern(name), kind)
    }

    fn metric_event(name: &str, kind: MetricKind) -> Event {
        Event::metric(0, AttrMap::new(), metric(name, kind))
    }

    fn span_record(end: i64) -> SpanRecord {
        SpanRecord {
            trace_id: [0xab; 16],
            span_id: [0xcd; 8],
            parent_span_id: None,
            name: Value::str("GET /index.html"),
            kind: SpanKind::Server,
            status: SpanStatus::Ok,
            events: vec![],
            links: vec![],
            end_timestamp: end,
            flags: 0,
            ext: None,
        }
    }

    fn attrs_event(key: &str, value: Value) -> Event {
        let mut attrs = AttrMap::new();
        attrs.insert(key, value);
        Event::empty(0, attrs)
    }

    /// Inserts in slice order; tests use keys no other test interns, so `AttrMap`'s
    /// interning-order iteration follows the slice.
    fn map(entries: &[(&str, Value)]) -> Value {
        let mut m = AttrMap::new();
        for (k, v) in entries {
            m.insert(k, v.clone());
        }
        Value::Map(Box::new(m))
    }

    /// Divider, timestamp, and `metrics:`, then `lines`.
    fn metric_block(lines: &[&str]) -> String {
        let mut all = vec![EVENT_DIVIDER, T0, "metrics:"];
        all.extend_from_slice(lines);
        block(&all)
    }

    /// Divider, timestamp, and `attributes:`, then `lines`.
    fn attrs_block(lines: &[&str]) -> String {
        let mut all = vec![EVENT_DIVIDER, T0, "attributes:"];
        all.extend_from_slice(lines);
        block(&all)
    }

    // --- sections ---

    #[test]
    fn the_divider_is_80_dashes() {
        assert_eq!(EVENT_DIVIDER.len(), 80);
        assert!(EVENT_DIVIDER.bytes().all(|b| b == b'-'));
    }

    #[test]
    fn an_empty_event_renders_its_divider_and_timestamp_only() {
        assert_eq!(render(vec![Event::empty(0, AttrMap::new())]), block(&[EVENT_DIVIDER, T0]));
    }

    #[test]
    fn a_log_with_only_a_message_renders_format_and_message() {
        assert_eq!(
            render(vec![log_event("GET /index.html HTTP/1.1")]),
            block(&[
                EVENT_DIVIDER,
                T0,
                "log:",
                "  format: raw",
                "  message: GET /index.html HTTP/1.1",
            ])
        );
    }

    #[test]
    fn a_log_with_every_optional_field_renders_them_all_in_order() {
        let mut record = log_record(Value::str("hello"));
        record.severity = Some(Severity::Warn);
        record.body_format = BodyFormat::Json;
        record.event_name = Some(logit_core::interner::intern("http.request"));
        record.observed_timestamp = 1_000_000_000;
        record.trace =
            Some(logit_core::TraceRef { trace_id: [0xab; 16], span_id: Some([0xcd; 8]), flags: 1 });
        record.dropped_attributes_count = 2;
        assert_eq!(
            render(vec![Event::log(0, AttrMap::new(), record)]),
            block(&[
                EVENT_DIVIDER,
                T0,
                "log:",
                "  severity: warn",
                "  format: json",
                "  message: hello",
                "  event_name: \"http.request\"",
                "  observed_timestamp: 1970-01-01T00:00:01.000000000Z",
                "  trace_id: abababababababababababababababab",
                "  span_id: cdcdcdcdcdcdcdcd",
                "  trace_flags: 1",
                "  dropped_attributes_count: 2",
            ])
        );
    }

    #[test]
    fn a_logs_trace_without_a_span_id_still_shows_zero_trace_flags() {
        let mut record = log_record(Value::str("hi"));
        record.trace = Some(logit_core::TraceRef { trace_id: [0xab; 16], span_id: None, flags: 0 });
        assert_eq!(
            render(vec![Event::log(0, AttrMap::new(), record)]),
            block(&[
                EVENT_DIVIDER,
                T0,
                "log:",
                "  format: raw",
                "  message: hi",
                "  trace_id: abababababababababababababababab",
                "  trace_flags: 0",
            ])
        );
    }

    /// Every section at once, with a resource and a scope: the format's reference block.
    #[test]
    fn a_mixed_log_metric_and_span_event_renders_every_section_in_order() {
        let mut attrs = AttrMap::new();
        attrs.insert("http.request.method", "GET");
        let record = LogRecord {
            severity: Some(Severity::Info),
            ..log_record(Value::str("GET /index.html HTTP/1.1"))
        };
        let mut event = Event::log(1_000_000_000, attrs, record);
        let mut requests = metric("http.server.requests", MetricKind::counter(1.0));
        requests.unit = Some(logit_core::interner::intern("1"));
        event.metrics.push(requests);
        event.span = Some(span_record(1_012_300_000));

        let mut resource = Resource::default();
        resource.attributes.insert("host.name", "web-1");
        let scope = logit_core::Scope {
            name: Bytes::from_static(b"io.opentelemetry.nginx"),
            version: Bytes::from_static(b"1.2.0"),
            ..Default::default()
        };
        let batch = EventBatch {
            resource: Arc::new(resource),
            scope: Some(Arc::new(scope)),
            events: vec![event],
        };

        assert_eq!(
            EventDump::default().render(&batch),
            block(&[
                "--------------------------------------------------------------------------------",
                "timestamp: 1970-01-01T00:00:01.000000000Z",
                "log:",
                "  severity: info",
                "  format: raw",
                "  message: GET /index.html HTTP/1.1",
                "metrics:",
                "  - name: http.server.requests",
                "    kind: sum",
                "    value: 1",
                "    temporality: delta",
                "    monotonic: true",
                "    unit: 1",
                "span:",
                "  name: \"GET /index.html\"",
                "  trace_id: abababababababababababababababab",
                "  span_id: cdcdcdcdcdcdcdcd",
                "  kind: server",
                "  status: ok",
                "  start: 1970-01-01T00:00:01.000000000Z",
                "  end: 1970-01-01T00:00:01.012300000Z",
                "  duration: 12300000ns",
                "attributes:",
                "  http.request.method: \"GET\"",
                "resource:",
                "  attributes:",
                "    host.name: \"web-1\"",
                "scope:",
                "  name: \"io.opentelemetry.nginx\"",
                "  version: \"1.2.0\"",
            ])
        );
    }

    #[test]
    fn a_multi_event_batch_renders_one_block_per_event_in_batch_order() {
        let out = render(vec![
            metric_event("first", MetricKind::Gauge(1.0)),
            metric_event("second", MetricKind::Gauge(2.0)),
        ]);
        assert_eq!(
            out,
            block(&[
                EVENT_DIVIDER,
                T0,
                "metrics:",
                "  - name: first",
                "    kind: gauge",
                "    value: 1",
                EVENT_DIVIDER,
                T0,
                "metrics:",
                "  - name: second",
                "    kind: gauge",
                "    value: 2",
            ])
        );
    }

    #[test]
    fn several_metrics_on_one_event_each_get_an_item() {
        let mut event = metric_event("m.one", MetricKind::Gauge(1.0));
        event.metrics.push(metric("m.two", MetricKind::Gauge(2.0)));
        assert_eq!(
            render(vec![event]),
            metric_block(&[
                "  - name: m.one",
                "    kind: gauge",
                "    value: 1",
                "  - name: m.two",
                "    kind: gauge",
                "    value: 2",
            ])
        );
    }

    // --- metric kinds ---

    #[test]
    fn sum_renders_value_temporality_and_monotonic() {
        let kind = MetricKind::Sum(logit_core::Sum {
            value: 2.5,
            temporality: Temporality::Cumulative,
            monotonic: false,
        });
        assert_eq!(
            render(vec![metric_event("bytes", kind)]),
            metric_block(&[
                "  - name: bytes",
                "    kind: sum",
                "    value: 2.5",
                "    temporality: cumulative",
                "    monotonic: false",
            ])
        );
    }

    #[test]
    fn gauge_renders_its_value() {
        assert_eq!(
            render(vec![metric_event("cpu.load", MetricKind::Gauge(0.5))]),
            metric_block(&["  - name: cpu.load", "    kind: gauge", "    value: 0.5"])
        );
    }

    #[test]
    fn gauge_delta_always_carries_an_explicit_sign() {
        for (v, expected) in [(5.0, "+5"), (-2.0, "-2"), (0.0, "+0"), (-0.0, "-0")] {
            let delta = format!("    delta: {expected}");
            assert_eq!(
                render(vec![metric_event("g", MetricKind::GaugeDelta(v))]),
                metric_block(&["  - name: g", "    kind: gauge_delta", &delta]),
                "for {v:?}"
            );
        }
    }

    #[test]
    fn samples_render_inline_values_and_the_sample_rate() {
        let mut samples = Samples::new([1.0, 2.5]);
        samples.sample_rate = 0.5;
        assert_eq!(
            render(vec![metric_event("latency", MetricKind::Samples(samples))]),
            metric_block(&[
                "  - name: latency",
                "    kind: samples",
                "    values: [1, 2.5]",
                "    sample_rate: 0.5",
            ])
        );
    }

    /// Members are a peer's bytes: each is quoted and escaped, an invalid one lossily decoded.
    #[test]
    fn set_members_render_as_quoted_escaped_strings() {
        let kind = MetricKind::SetMembers(vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b\nc\x1b"),
            Bytes::from_static(b"\xff"),
        ]);
        let out = render(vec![metric_event("uniques", kind)]);
        assert_eq!(
            out,
            metric_block(&[
                "  - name: uniques",
                "    kind: set_members",
                "    members: [\"a\", \"b\\nc\\x1b\", \"\u{fffd}\"]",
            ])
        );
        assert!(!out.contains('\x1b'), "a raw ESC must never reach the output: {out:?}");
    }

    #[test]
    fn distribution_renders_its_stats_and_quantiles() {
        let mut sketch = DdSketch::new();
        sketch.add(120.0);
        assert_eq!(
            render(vec![metric_event("latency", MetricKind::Distribution(sketch))]),
            metric_block(&[
                "  - name: latency",
                "    kind: distribution",
                "    count: 1",
                "    sum: 120",
                "    min: 120",
                "    max: 120",
                "    avg: 120",
                "    p50: 120",
                "    p90: 120",
                "    p95: 120",
                "    p99: 120",
            ])
        );
    }

    #[test]
    fn an_empty_distribution_renders_count_and_sum_only() {
        assert_eq!(
            render(vec![metric_event("latency", MetricKind::Distribution(DdSketch::new()))]),
            metric_block(&[
                "  - name: latency",
                "    kind: distribution",
                "    count: 0",
                "    sum: 0"
            ])
        );
    }

    #[test]
    fn set_renders_its_hyperloglog_estimate() {
        let mut hll = HyperLogLog::default();
        hll.insert(b"a");
        hll.insert(b"b");
        hll.insert(b"a");
        assert_eq!(
            render(vec![metric_event("unique.users", MetricKind::Set(hll))]),
            metric_block(&["  - name: unique.users", "    kind: set", "    estimate: 2"])
        );
    }

    #[test]
    fn histogram_renders_buckets_in_wire_order_with_an_inf_bound_and_stats() {
        let kind = MetricKind::Histogram(Histogram {
            buckets: vec![(100.0, 5), (500.0, 2), (f64::INFINITY, 1)],
            temporality: Temporality::Cumulative,
            sum: Some(42.0),
            min: Some(1.0),
            max: Some(99.5),
        });
        assert_eq!(
            render(vec![metric_event("resp.size", kind)]),
            metric_block(&[
                "  - name: resp.size",
                "    kind: histogram",
                "    temporality: cumulative",
                "    buckets:",
                "      100: 5",
                "      500: 2",
                "      inf: 1",
                "    sum: 42",
                "    min: 1",
                "    max: 99.5",
            ])
        );
    }

    #[test]
    fn a_histogram_with_no_buckets_or_stats_renders_empty_buckets_inline() {
        let kind = MetricKind::Histogram(Histogram {
            buckets: vec![],
            temporality: Temporality::Delta,
            sum: None,
            min: None,
            max: None,
        });
        assert_eq!(
            render(vec![metric_event("h", kind)]),
            metric_block(&[
                "  - name: h",
                "    kind: histogram",
                "    temporality: delta",
                "    buckets: {}",
            ])
        );
    }

    #[test]
    fn exponential_histogram_renders_both_offsets_and_its_present_stats() {
        let kind = MetricKind::ExponentialHistogram(ExpHistogram {
            scale: 3,
            zero_count: 2,
            zero_threshold: 0.001,
            positive: (-2, vec![1, 2, 3]),
            negative: (1, vec![4]),
            temporality: Temporality::Cumulative,
            count: 12,
            sum: Some(11.5),
            min: None,
            max: Some(9.0),
        });
        assert_eq!(
            render(vec![metric_event("resp.size", kind)]),
            metric_block(&[
                "  - name: resp.size",
                "    kind: exponential_histogram",
                "    scale: 3",
                "    temporality: cumulative",
                "    count: 12",
                "    zero_count: 2",
                "    zero_threshold: 0.001",
                "    positive:",
                "      offset: -2",
                "      counts: [1, 2, 3]",
                "    negative:",
                "      offset: 1",
                "      counts: [4]",
                "    sum: 11.5",
                "    max: 9",
            ])
        );
    }

    #[test]
    fn exponential_histogram_omits_an_empty_side_and_absent_stats() {
        let kind = MetricKind::ExponentialHistogram(ExpHistogram {
            scale: 0,
            zero_count: 0,
            zero_threshold: 0.0,
            positive: (0, vec![7]),
            negative: (5, vec![]),
            temporality: Temporality::Delta,
            count: 7,
            sum: None,
            min: None,
            max: None,
        });
        assert_eq!(
            render(vec![metric_event("e", kind)]),
            metric_block(&[
                "  - name: e",
                "    kind: exponential_histogram",
                "    scale: 0",
                "    temporality: delta",
                "    count: 7",
                "    zero_count: 0",
                "    zero_threshold: 0",
                "    positive:",
                "      offset: 0",
                "      counts: [7]",
            ])
        );
    }

    #[test]
    fn summary_renders_count_sum_and_quantiles_in_wire_order() {
        let kind = MetricKind::Summary(Summary {
            quantiles: vec![(0.99, 12.5), (0.5, 1.5)],
            count: 3,
            sum: 40.0,
        });
        assert_eq!(
            render(vec![metric_event("req.latency", kind)]),
            metric_block(&[
                "  - name: req.latency",
                "    kind: summary",
                "    count: 3",
                "    sum: 40",
                "    quantiles:",
                "      0.99: 12.5",
                "      0.5: 1.5",
            ])
        );
    }

    #[test]
    fn unit_description_and_start_timestamp_render_after_the_kind_fields() {
        let mut record = metric("req.time", MetricKind::Gauge(0.5));
        record.unit = Some(logit_core::interner::intern("ms"));
        record.description = Some(logit_core::interner::intern("Time \"served\""));
        record.start_timestamp = 1_000_000_000;
        assert_eq!(
            render(vec![Event::metric(0, AttrMap::new(), record)]),
            metric_block(&[
                "  - name: req.time",
                "    kind: gauge",
                "    value: 0.5",
                "    unit: ms",
                "    description: \"Time \\\"served\\\"\"",
                "    start_timestamp: 1970-01-01T00:00:01.000000000Z",
            ])
        );
    }

    #[test]
    fn a_unit_that_is_not_identifier_shaped_is_quoted() {
        let mut record = metric("reqs", MetricKind::Gauge(1.0));
        record.unit = Some(logit_core::interner::intern("{request}/s"));
        assert_eq!(
            render(vec![Event::metric(0, AttrMap::new(), record)]),
            metric_block(&[
                "  - name: reqs",
                "    kind: gauge",
                "    value: 1",
                "    unit: \"{request}/s\"",
            ])
        );
    }

    /// The kind's default payload isn't a reading, so none of its fields render.
    #[test]
    fn no_recorded_value_suppresses_the_kind_fields_and_shows_the_raw_flags() {
        let mut record = metric(
            "up",
            MetricKind::Sum(logit_core::Sum {
                value: 0.0,
                temporality: Temporality::Cumulative,
                monotonic: true,
            }),
        );
        record.flags = MetricRecord::FLAG_NO_RECORDED_VALUE | 0b10;
        record.unit = Some(logit_core::interner::intern("s"));
        assert_eq!(
            render(vec![Event::metric(0, AttrMap::new(), record)]),
            metric_block(&[
                "  - name: up",
                "    kind: sum",
                "    unit: s",
                "    flags: 3",
                "    no_recorded_value: true",
            ])
        );
    }

    #[test]
    fn a_flag_other_than_no_recorded_value_keeps_the_kind_fields() {
        let mut record = metric("up", MetricKind::Gauge(1.0));
        record.flags = 0b10;
        assert_eq!(
            render(vec![Event::metric(0, AttrMap::new(), record)]),
            metric_block(&["  - name: up", "    kind: gauge", "    value: 1", "    flags: 2"])
        );
    }

    #[test]
    fn exemplars_render_with_their_trace_and_filtered_attributes() {
        let mut filtered = AttrMap::new();
        filtered.insert("exemplar.user", "u-1");
        let mut record = metric("latency", MetricKind::Gauge(0.3));
        record.exemplars = vec![
            Exemplar {
                timestamp: 1_000_000_000,
                value: 0.25,
                trace: Some(logit_core::TraceRef {
                    trace_id: [0xab; 16],
                    span_id: Some([0xcd; 8]),
                    flags: 0,
                }),
                filtered_attributes: filtered,
            },
            Exemplar { timestamp: 0, value: 1.0, trace: None, filtered_attributes: AttrMap::new() },
        ];
        assert_eq!(
            render(vec![Event::metric(0, AttrMap::new(), record)]),
            metric_block(&[
                "  - name: latency",
                "    kind: gauge",
                "    value: 0.3",
                "    exemplars:",
                "      - timestamp: 1970-01-01T00:00:01.000000000Z",
                "        value: 0.25",
                "        trace_id: abababababababababababababababab",
                "        span_id: cdcdcdcdcdcdcdcd",
                "        trace_flags: 0",
                "        attributes:",
                "          exemplar.user: \"u-1\"",
                "      - timestamp: 1970-01-01T00:00:00.000000000Z",
                "        value: 1",
            ])
        );
    }

    // --- span ---

    #[test]
    fn a_minimal_span_renders_identity_times_and_duration() {
        let event = Event::span(1_000_000_000, AttrMap::new(), span_record(1_500_000_000));
        assert_eq!(
            render(vec![event]),
            block(&[
                EVENT_DIVIDER,
                "timestamp: 1970-01-01T00:00:01.000000000Z",
                "span:",
                "  name: \"GET /index.html\"",
                "  trace_id: abababababababababababababababab",
                "  span_id: cdcdcdcdcdcdcdcd",
                "  kind: server",
                "  status: ok",
                "  start: 1970-01-01T00:00:01.000000000Z",
                "  end: 1970-01-01T00:00:01.500000000Z",
                "  duration: 500000000ns",
            ])
        );
    }

    #[test]
    fn a_span_duration_saturates_instead_of_wrapping() {
        let event = Event::span(i64::MIN, AttrMap::new(), span_record(i64::MAX));
        let out = render(vec![event]);
        assert!(out.contains(&format!("  duration: {}ns\n", i64::MAX)), "got: {out}");
    }

    #[test]
    fn a_full_span_renders_parent_ext_flags_events_and_links() {
        let mut span = span_record(1_500_000_000);
        span.parent_span_id = Some([0x12; 8]);
        span.status = SpanStatus::Error;
        span.flags = 0x101;
        span.ext = Some(Box::new(SpanExt {
            status_message: Some(Bytes::from_static(b"boom")),
            trace_state: Some(Bytes::from_static(b"vendor=x")),
            dropped_attributes_count: 1,
            dropped_events_count: 2,
            dropped_links_count: 3,
        }));
        let mut event_attrs = AttrMap::new();
        event_attrs.insert("span.event.retry", 1_i64);
        span.events.push(SpanEvent {
            timestamp: 1_200_000_000,
            name: Value::str("retrying"),
            attributes: event_attrs,
            dropped_attributes_count: 4,
        });
        span.events.push(SpanEvent {
            timestamp: 1_300_000_000,
            name: Value::str("done"),
            attributes: AttrMap::new(),
            dropped_attributes_count: 0,
        });
        let mut link_attrs = AttrMap::new();
        link_attrs.insert("span.link.relation", "follows_from");
        span.links.push(SpanLink {
            trace_id: [0xef; 16],
            span_id: [0x34; 8],
            attributes: link_attrs,
            flags: 1,
            trace_state: Some(Bytes::from_static(b"k=v")),
            dropped_attributes_count: 5,
        });
        span.links.push(SpanLink {
            trace_id: [0x01; 16],
            span_id: [0x02; 8],
            attributes: AttrMap::new(),
            flags: 0,
            trace_state: None,
            dropped_attributes_count: 0,
        });
        let event = Event::span(1_000_000_000, AttrMap::new(), span);
        assert_eq!(
            render(vec![event]),
            block(&[
                EVENT_DIVIDER,
                "timestamp: 1970-01-01T00:00:01.000000000Z",
                "span:",
                "  name: \"GET /index.html\"",
                "  trace_id: abababababababababababababababab",
                "  span_id: cdcdcdcdcdcdcdcd",
                "  parent_span_id: 1212121212121212",
                "  kind: server",
                "  status: error",
                "  status_message: \"boom\"",
                "  trace_state: \"vendor=x\"",
                "  flags: 257",
                "  start: 1970-01-01T00:00:01.000000000Z",
                "  end: 1970-01-01T00:00:01.500000000Z",
                "  duration: 500000000ns",
                "  dropped_attributes_count: 1",
                "  dropped_events_count: 2",
                "  dropped_links_count: 3",
                "  events:",
                "    - name: \"retrying\"",
                "      timestamp: 1970-01-01T00:00:01.200000000Z",
                "      attributes:",
                "        span.event.retry: 1",
                "      dropped_attributes_count: 4",
                "    - name: \"done\"",
                "      timestamp: 1970-01-01T00:00:01.300000000Z",
                "  links:",
                "    - trace_id: efefefefefefefefefefefefefefefef",
                "      span_id: 3434343434343434",
                "      flags: 1",
                "      trace_state: \"k=v\"",
                "      attributes:",
                "        span.link.relation: \"follows_from\"",
                "      dropped_attributes_count: 5",
                "    - trace_id: 01010101010101010101010101010101",
                "      span_id: 0202020202020202",
            ])
        );
    }

    // --- resource and scope ---

    #[test]
    fn resource_renders_attributes_schema_url_and_dropped_count() {
        let mut resource = Resource::default();
        resource.attributes.insert("service.name", "nginx");
        resource.schema_url = Some(Bytes::from_static(b"https://opentelemetry.io/schemas/1.26.0"));
        resource.dropped_attributes_count = 2;
        let batch = EventBatch {
            resource: Arc::new(resource),
            scope: None,
            events: vec![Event::empty(0, AttrMap::new())],
        };
        assert_eq!(
            EventDump::default().render(&batch),
            block(&[
                EVENT_DIVIDER,
                T0,
                "resource:",
                "  attributes:",
                "    service.name: \"nginx\"",
                "  schema_url: \"https://opentelemetry.io/schemas/1.26.0\"",
                "  dropped_attributes_count: 2",
            ])
        );
    }

    #[test]
    fn a_resource_with_only_a_dropped_count_still_renders() {
        let resource = Resource { dropped_attributes_count: 1, ..Default::default() };
        let batch = EventBatch {
            resource: Arc::new(resource),
            scope: None,
            events: vec![Event::empty(0, AttrMap::new())],
        };
        assert_eq!(
            EventDump::default().render(&batch),
            block(&[EVENT_DIVIDER, T0, "resource:", "  dropped_attributes_count: 1"])
        );
    }

    /// The resource is its own section, never merged into the event's attributes: a key on both
    /// renders twice.
    #[test]
    fn resource_and_event_attributes_render_in_separate_sections() {
        let mut resource = Resource::default();
        resource.attributes.insert("env", "staging");
        let batch = EventBatch {
            resource: Arc::new(resource),
            scope: None,
            events: vec![attrs_event("env", Value::str("prod"))],
        };
        assert_eq!(
            EventDump::default().render(&batch),
            attrs_block(&["  env: \"prod\"", "resource:", "  attributes:", "    env: \"staging\""])
        );
    }

    #[test]
    fn scope_renders_every_field() {
        let mut scope_attrs = AttrMap::new();
        scope_attrs.insert("scope.attr", "v");
        let scope = logit_core::Scope {
            name: Bytes::from_static(b"io.opentelemetry.nginx"),
            version: Bytes::from_static(b"1.2.0"),
            attributes: scope_attrs,
            dropped_attributes_count: 3,
            schema_url: Some(Bytes::from_static(b"https://example.test/schema")),
        };
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: Some(Arc::new(scope)),
            events: vec![Event::empty(0, AttrMap::new())],
        };
        assert_eq!(
            EventDump::default().render(&batch),
            block(&[
                EVENT_DIVIDER,
                T0,
                "scope:",
                "  name: \"io.opentelemetry.nginx\"",
                "  version: \"1.2.0\"",
                "  attributes:",
                "    scope.attr: \"v\"",
                "  schema_url: \"https://example.test/schema\"",
                "  dropped_attributes_count: 3",
            ])
        );
    }

    #[test]
    fn an_empty_scope_renders_its_header_alone() {
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: Some(Arc::new(logit_core::Scope::default())),
            events: vec![Event::empty(0, AttrMap::new())],
        };
        assert_eq!(EventDump::default().render(&batch), block(&[EVENT_DIVIDER, T0, "scope:"]));
    }

    #[test]
    fn a_byte_string_field_shows_invalid_utf8_as_hex() {
        let scope =
            logit_core::Scope { name: Bytes::from_static(b"lib\xff\n"), ..Default::default() };
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: Some(Arc::new(scope)),
            events: vec![Event::empty(0, AttrMap::new())],
        };
        assert_eq!(
            EventDump::default().render(&batch),
            block(&[EVENT_DIVIDER, T0, "scope:", r#"  name: "lib\xff\n""#])
        );
    }

    // --- values ---

    #[test]
    fn scalar_values_render_plainly() {
        for (value, expected) in [
            (Value::Null, "null"),
            (Value::Bool(true), "true"),
            (Value::I64(-5), "-5"),
            (Value::U64(5), "5"),
            (Value::F64(1.5), "1.5"),
            (Value::F64(f64::NAN), "NaN"),
            (Value::F64(f64::NEG_INFINITY), "-inf"),
            (Value::Timestamp(1_000_000_000), "1970-01-01T00:00:01.000000000Z"),
        ] {
            let line = format!("  v: {expected}");
            assert_eq!(render(vec![attrs_event("v", value)]), attrs_block(&[&line]));
        }
    }

    /// A raw ESC in a value never reaches the terminal unescaped.
    #[test]
    fn a_string_value_is_quoted_and_every_control_character_escaped() {
        let out = render(vec![attrs_event("s", Value::str("a\nb\t\"q\"\\\x1b[2J\x07\x7f\r"))]);
        assert_eq!(out, attrs_block(&[r#"  s: "a\nb\t\"q\"\\\x1b[2J\x07\x7f\r""#]));
        assert!(!out.contains('\x1b') && !out.contains('\x07'), "raw control byte: {out:?}");
    }

    #[test]
    fn a_nested_map_renders_as_a_block() {
        let value = map(&[(
            "exampleSDID@32473",
            map(&[("nested.iut", Value::str("3")), ("nested.level", Value::I64(2))]),
        )]);
        assert_eq!(
            render(vec![attrs_event("syslog.sd", value)]),
            attrs_block(&[
                "  syslog.sd:",
                "    \"exampleSDID@32473\":",
                "      nested.iut: \"3\"",
                "      nested.level: 2",
            ])
        );
    }

    #[test]
    fn an_array_of_scalars_stays_inline() {
        let value = Value::Array(vec![Value::str("a"), Value::I64(1), Value::Null]);
        assert_eq!(
            render(vec![attrs_event("tags", value)]),
            attrs_block(&["  tags: [\"a\", 1, null]"])
        );
    }

    #[test]
    fn an_array_of_maps_renders_as_a_dash_block() {
        let value = Value::Array(vec![
            map(&[("item.name", Value::str("x")), ("item.qty", Value::I64(1))]),
            map(&[("item.name", Value::str("y"))]),
            Value::I64(7),
        ]);
        assert_eq!(
            render(vec![attrs_event("items", value)]),
            attrs_block(&[
                "  items:",
                "    - item.name: \"x\"",
                "      item.qty: 1",
                "    - item.name: \"y\"",
                "    - 7",
            ])
        );
    }

    #[test]
    fn nested_arrays_and_a_map_holding_a_block_array_nest_consistently() {
        let value = Value::Array(vec![
            Value::Array(vec![Value::I64(1), Value::I64(2)]),
            Value::Array(vec![map(&[("deep.k", Value::Bool(true))])]),
            map(&[("deep.list", Value::Array(vec![map(&[("deep.x", Value::I64(0))])]))]),
        ]);
        assert_eq!(
            render(vec![attrs_event("grid", value)]),
            attrs_block(&[
                "  grid:",
                "    - [1, 2]",
                "    - - deep.k: true",
                "    - deep.list:",
                "        - deep.x: 0",
            ])
        );
    }

    #[test]
    fn empty_containers_render_inline() {
        let value = map(&[
            ("empty.map", Value::Map(Box::default())),
            ("empty.array", Value::Array(vec![])),
            ("empty.wrapped", Value::Array(vec![Value::Map(Box::default()), Value::Array(vec![])])),
        ]);
        assert_eq!(
            render(vec![attrs_event("c", value)]),
            attrs_block(&[
                "  c:",
                "    empty.map: {}",
                "    empty.array: []",
                "    empty.wrapped:",
                "      - {}",
                "      - []",
            ])
        );
    }

    #[test]
    fn valid_utf8_bytes_render_their_content_escaped() {
        let value = Value::Bytes(Bytes::from_static(b"ok \"x\"\n"));
        assert_eq!(
            render(vec![attrs_event("raw", value)]),
            attrs_block(&[r#"  raw: b"ok \"x\"\n""#])
        );
    }

    #[test]
    fn invalid_utf8_bytes_render_each_invalid_byte_as_hex() {
        let value = Value::Bytes(Bytes::from_static(b"a\xff\xfe\x00z\xc3\xa9"));
        assert_eq!(
            render(vec![attrs_event("raw", value)]),
            attrs_block(&["  raw: b\"a\\xff\\xfe\\x00z\u{e9}\""])
        );
    }

    /// A key containing a space, `:`, or a newline is quoted, not written bare.
    #[test]
    fn a_key_that_is_not_identifier_shaped_is_quoted_and_escaped() {
        assert_eq!(
            render(vec![attrs_event("weird key\n: x", Value::I64(1))]),
            attrs_block(&[r#"  "weird key\n: x": 1"#])
        );
    }

    #[test]
    fn attributes_come_out_in_attrmap_order() {
        // `Symbol` order is interning order, so assert against `AttrMap::iter`, not the alphabet.
        let mut attrs = AttrMap::new();
        attrs.insert("order.zebra", "z");
        attrs.insert("order.apple", "a");
        attrs.insert("order.mango", "m");
        let lines: Vec<String> = attrs
            .iter()
            .map(|(k, v)| format!("  {}: \"{}\"", resolve(k), v.as_str().unwrap()))
            .collect();
        let lines: Vec<&str> = lines.iter().map(String::as_str).collect();
        assert_eq!(render(vec![Event::empty(0, attrs)]), attrs_block(&lines));
    }

    // --- message modes ---

    /// Everything after `  message:` to the end of the render.
    fn after_message_key(out: &str) -> &str {
        out.split_once("  message:").map(|(_, rest)| rest).unwrap()
    }

    #[test]
    fn escaped_mode_keeps_the_message_on_one_line_and_unquoted() {
        let out = render(vec![log_event("a\nb\tc\x1b[31m\rd \"q\" \\")]);
        assert_eq!(after_message_key(&out), " a\\nb\\tc\\x1b[31m\\rd \"q\" \\\n");
        assert!(!out.contains('\x1b'), "a raw ESC must never reach the output: {out:?}");
    }

    #[test]
    fn multiline_mode_breaks_lines_and_passes_tabs_through() {
        let out = render_multiline(vec![log_event("first\n\tsecond\x1b\r\nthird")]);
        assert_eq!(
            out,
            block(&[
                EVENT_DIVIDER,
                T0,
                "log:",
                "  format: raw",
                "  message: first",
                "           \tsecond\\x1b\\r",
                "           third",
            ])
        );
    }

    #[test]
    fn an_empty_or_space_led_message_renders_the_same_in_both_modes() {
        for message in ["", " leading"] {
            let escaped = render(vec![log_event(message)]);
            assert_eq!(escaped, render_multiline(vec![log_event(message)]));
            assert_eq!(after_message_key(&escaped), format!(" {message}\n"));
        }
    }

    #[test]
    fn a_non_string_message_uses_the_value_grammar_in_either_mode() {
        let body = map(&[("msg.level", Value::str("info")), ("msg.n", Value::I64(3))]);
        let expected = block(&[
            EVENT_DIVIDER,
            T0,
            "log:",
            "  format: raw",
            "  message:",
            "    msg.level: \"info\"",
            "    msg.n: 3",
        ]);
        let event = || Event::log(0, AttrMap::new(), log_record(body.clone()));
        assert_eq!(render(vec![event()]), expected);
        assert_eq!(render_multiline(vec![event()]), expected);

        let bytes = log_record(Value::Bytes(Bytes::from_static(b"\xff\n")));
        let out = render_multiline(vec![Event::log(0, AttrMap::new(), bytes)]);
        assert_eq!(after_message_key(&out), " b\"\\xff\\n\"\n");
    }

    // --- render_value_inline, syslog_out's container fallback ---

    fn inline(value: &Value) -> String {
        let mut out = String::new();
        render_value_inline(&mut out, value);
        out
    }

    #[test]
    fn render_value_inline_renders_bytes_as_a_byte_count_not_lossy_utf8() {
        assert_eq!(inline(&Value::Bytes(Bytes::from_static(b"\xff\xfe\x00"))), "<3 bytes>");
    }

    #[test]
    fn render_value_inline_renders_a_timestamp_as_rfc3339() {
        assert_eq!(inline(&Value::Timestamp(0)), "1970-01-01T00:00:00.000000000Z");
    }

    #[test]
    fn render_value_inline_renders_scalars_plainly() {
        assert_eq!(inline(&Value::Null), "null");
        assert_eq!(inline(&Value::Bool(true)), "true");
        assert_eq!(inline(&Value::I64(-5)), "-5");
        assert_eq!(inline(&Value::U64(5)), "5");
        assert_eq!(inline(&Value::F64(1.5)), "1.5");
    }

    #[test]
    fn render_value_inline_renders_arrays_and_maps_compactly() {
        assert_eq!(inline(&Value::Array(vec![Value::str("a"), Value::str("b")])), r#"["a", "b"]"#);
        let nested = map(&[
            ("inline.k", Value::str("v")),
            ("inline.m", map(&[("inline.x", Value::I64(1))])),
            ("inline.e", Value::Map(Box::default())),
        ]);
        assert_eq!(inline(&nested), r#"{inline.k="v", inline.m={inline.x=1}, inline.e={}}"#);
    }

    #[test]
    fn render_value_inline_quotes_and_escapes_strings_and_keys() {
        assert_eq!(
            inline(&Value::str("line1\nline2\t\"quoted\"\\backslash\x1b")),
            r#""line1\nline2\t\"quoted\"\\backslash\x1b""#
        );
        assert_eq!(
            inline(&map(&[("weird key\nwith=stuff", Value::str("value"))])),
            r#"{"weird key\nwith=stuff"="value"}"#
        );
    }
}
