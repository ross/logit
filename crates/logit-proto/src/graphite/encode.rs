//! Encoding a batch of events back into carbon plaintext lines or pickle frames -- the
//! `| Model | Wire |` half of [`super`]'s module doc, which is the spec for everything here.
//!
//! Pure: no socket anywhere, so every grammar, sanitization, packing and counting test runs
//! directly against [`GraphiteEncoder`] (the split `crates/logit-outputs/src/statsd.rs` and
//! [`crate::collectd::encode`] already use). [`GraphiteEncoder`] implements [`FramedEncoder`]
//! rather than [`crate::Encoder`] -- see [`super`]'s "`Protocol` and `Meta`" section for why, and
//! for what each entry's `usize` meta means.
//!
//! The codec emits **every one of its own** `logit.output.*` counters and diagnostics directly, at
//! each drop site (see [`Ctx`]), through the handles
//! [`GraphiteEncoder::with_telemetry`]/[`GraphiteEncoder::with_diagnostics`] install -- collectd's
//! model, not statsd's. [`EncodeStats`] is returned for tests and benches; `graphite_out` (W3)
//! discards it.
//!
//! **Every per-record buffer is a struct field**, cleared and refilled rather than reallocated:
//! the tag suffix, its arena and slot table, the sanitized path, the rendered line, the in-progress
//! pickle frame and the one-datapoint scratch. That is what makes a warm encode allocation-free
//! (`docs/design/memory.md` §3, and the allocation rows W3 pins). The two deliberate exceptions
//! are named at their own call sites: `Samples::sketch()` builds a sketch per record (inherent, and
//! shared with `influxdb_out`), and a `SetMembers` expansion needs a de-duplication buffer.

use super::pickle;
use super::{MultiValue, Protocol, Tags};
use crate::otlp::metrics::DISTRIBUTION_QUANTILES;
use crate::{FramedEncoder, MessageBuf};
use logit_core::interner::resolve;
use logit_core::{
    DdSketch, Diagnostics, Event, EventBatch, ExpHistogram, Histogram, MetricKind, MetricRecord,
    Resource, Summary, Telemetry, Value,
};
use std::fmt::Write as _;
use std::ops::Range;

/// Nanoseconds per second -- the divisor an egress timestamp floors by.
const NANOS_PER_SECOND: i64 = 1_000_000_000;

/// Attribute namespaces that belong to another protocol's codec and have no business appearing as
/// carbon tags. Skipped **uncounted**, exactly as `crates/logit-outputs/src/influxdb.rs`'s
/// `render_tag_suffix` skips `statsd.`: these are consumed carriers, not tags anybody asked to see
/// on this wire, so counting them as dropped would report a loss that never happened.
const FOREIGN_CARRIER_PREFIXES: [&str; 2] = ["statsd.", "collectd."];

/// Per-batch outcome counts from [`GraphiteEncoder::encode_into`] -- the aggregate this module's
/// own tests and `crates/logit-bench/tests/allocations.rs` assert on exactly. Production telemetry
/// comes from the counters the encoder emits itself, not from this struct.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EncodeStats {
    /// Events carrying no metrics at all -- a log- or span-only event, legal under
    /// `docs/adr/multi-payload-events.md`. Not a loss: there was nothing carbon could carry.
    pub skipped_no_metrics: usize,
    /// A multi-value metric kind under [`MultiValue::Skip`] (the default). Counted once per record,
    /// with the kind as the counter's tag.
    pub dropped_unsupported_kind: usize,
    /// A multi-value metric kind under [`MultiValue::Expand`]. Counted once per **record**, however
    /// many sub-paths that record produced -- a degradation is a thing that happened to one metric,
    /// not to each of its pieces.
    pub degraded_expanded_kind: usize,
    /// A `Gauge`/`Sum` whose value is NaN or ±inf. Carbon drops a NaN on receipt itself, and there
    /// is no wire spelling for an infinity.
    pub dropped_unencodable_value: usize,
    /// A record flagged [`MetricRecord::FLAG_NO_RECORDED_VALUE`]. Carbon has no "no reading this
    /// interval" concept at all (unlike collectd's GAUGE NaN), so emitting the flag's default
    /// numeric payload would fabricate a sample nobody sent.
    pub dropped_no_recorded_value: usize,
    /// A [`MetricKind::GaugeDelta`], which means a missing `aggregate` stage rather than a bad
    /// metric.
    pub dropped_gauge_delta: usize,
    /// An event whose timestamp floors to a non-positive second. Counted once per record the event
    /// would have produced, so this number means the same thing as every other sink's
    /// `metrics.skipped`.
    pub dropped_unencodable_timestamp: usize,
    /// A record whose name sanitized to nothing -- carbon rejects an empty path, and a bare tag
    /// segment is not a series.
    pub dropped_empty_name: usize,
    /// A plaintext line longer than `max_packet_bytes`, dropped whole. Never split: half a line is
    /// a corrupt series, not a partial one.
    pub dropped_oversize_line: usize,
    /// A single pickle datapoint that cannot fit an empty `max_frame_bytes` frame, dropped whole.
    pub dropped_oversize_datapoint: usize,
    /// An attribute dropped because [`Tags::Drop`] is configured -- the operator's dialect choice,
    /// counted so it is visible rather than silent.
    pub tags_dropped_dialect: usize,
    /// An attribute whose `Value` has no faithful carbon tag spelling (`Null`, `Bytes`, a
    /// `Timestamp`, a `Map`, or an `Array` with no representable element).
    pub tags_dropped_unrepresentable: usize,
    /// A tag whose name or value was empty after sanitizing. Carbon's own parser rejects both, so
    /// emitting one would take the whole line down at the far end.
    pub tags_dropped_empty: usize,
    /// A tag whose rendered name collided with another's; the one whose **original** name sorts
    /// first survives.
    pub tags_dropped_collision: usize,
    /// A [`Value::Array`] rendered as its last representable element. **Lossy**, unlike every other
    /// `*.normalized` reason: the non-last elements are genuinely discarded (the same caveat
    /// `influxdb_out`'s identical row carries).
    pub tags_normalized_multi_value: usize,
    /// Records whose path had at least one byte substituted. Counted once per record, not once per
    /// byte.
    pub paths_sanitized: usize,
    /// Records whose tag segment had at least one byte substituted, in a name or a value. Counted
    /// once per record, for the same reason.
    pub tags_sanitized: usize,
    /// Datapoints actually written -- exactly Σ of every entry's `Meta`, which is what makes the
    /// sink's `logit.output.datapoints` and this agree by construction.
    pub datapoints: usize,
}

/// Encodes events as carbon plaintext lines or pickle frames. Pure -- no socket -- so
/// `graphite_out` (W3) is only a transport wrapper over this.
#[derive(Debug)]
pub struct GraphiteEncoder {
    telemetry: Telemetry,
    diag: Diagnostics,
    protocol: Protocol,
    tags: Tags,
    multi_value: MultiValue,
    /// The longest single plaintext **line** this encoder will emit. `usize::MAX` (the default) is
    /// effectively uncapped, which is what a TCP sink wants; a UDP sink passes its own
    /// `max_packet_bytes:`. Encoder state rather than a per-call argument -- [`FramedEncoder`] has
    /// one signature.
    max_packet_bytes: usize,
    /// The longest pickle **payload** (the bytes after carbon's 4-byte length prefix) this encoder
    /// will pack into one frame. [`super::DEFAULT_MAX_FRAME_BYTES`] is Twisted's own
    /// `Int32StringReceiver.MAX_LENGTH`, so a relay never writes a frame the far end refuses.
    max_frame_bytes: usize,

    // -- per-record scratch, reused across every call; see this module's doc comment --
    /// `;k=v;k=v`, rebuilt once per event.
    tag_suffix: String,
    /// Arena backing [`TagSlot`]'s three ranges.
    tag_text: String,
    tag_slots: Vec<TagSlot>,
    /// One rendered attribute value, before sanitizing.
    tag_value: String,
    /// The sanitized record name.
    path: String,
    /// `path + sub-path suffix + tag suffix` -- what actually goes on the wire as the series name.
    full_path: String,
    /// The current sub-path suffix under [`MultiValue::Expand`] (`.count`, `.q0_99`, ...); empty
    /// for a scalar record.
    suffix: String,
    /// One formatted number, before its `.` → `_` substitution.
    number: String,
    /// One rendered plaintext line.
    line: String,
    /// The pickle frame being packed, **including** its 4-byte length prefix (patched in place when
    /// the frame closes, so no second buffer and no copy).
    frame: Vec<u8>,
    /// One encoded pickle datapoint, measured against the frame cap before being appended.
    datapoint: Vec<u8>,
}

impl Default for GraphiteEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphiteEncoder {
    pub fn new() -> Self {
        Self {
            telemetry: Telemetry::default(),
            diag: Diagnostics::default(),
            protocol: Protocol::default(),
            tags: Tags::default(),
            multi_value: MultiValue::default(),
            max_packet_bytes: usize::MAX,
            max_frame_bytes: super::DEFAULT_MAX_FRAME_BYTES,
            tag_suffix: String::new(),
            tag_text: String::new(),
            tag_slots: Vec::new(),
            tag_value: String::new(),
            path: String::new(),
            full_path: String::new(),
            suffix: String::new(),
            number: String::new(),
            line: String::new(),
            frame: Vec::new(),
            datapoint: Vec::new(),
        }
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    /// Which carbon wire protocol to write -- see [`super`]'s "`Protocol` and `Meta`" section.
    pub fn with_protocol(mut self, protocol: Protocol) -> Self {
        self.protocol = protocol;
        self
    }

    /// Whether to render attributes as carbon tags at all.
    pub fn with_tags(mut self, tags: Tags) -> Self {
        self.tags = tags;
        self
    }

    /// What to do with a metric kind carbon's one-number datapoint cannot carry.
    pub fn with_multi_value(mut self, multi_value: MultiValue) -> Self {
        self.multi_value = multi_value;
        self
    }

    /// Caps one plaintext line -- see the field's own doc comment. `usize::MAX` means uncapped.
    pub fn with_max_packet_bytes(mut self, max_packet_bytes: usize) -> Self {
        self.max_packet_bytes = max_packet_bytes;
        self
    }

    /// Caps one pickle payload -- see the field's own doc comment.
    pub fn with_max_frame_bytes(mut self, max_frame_bytes: usize) -> Self {
        self.max_frame_bytes = max_frame_bytes;
        self
    }

    pub fn protocol(&self) -> Protocol {
        self.protocol
    }
}

impl FramedEncoder for GraphiteEncoder {
    /// The number of datapoints each message carries: always `1` for a plaintext line, and the
    /// frame's own datapoint count for a pickle frame. See [`super`]'s "`Protocol` and `Meta`".
    type Meta = usize;
    type Stats = EncodeStats;

    /// Encodes every event in `batch` into `out` (cleared first). Never fails: a per-record problem
    /// is a counted drop, not an error, and there is nothing for a caller to react to beyond the
    /// returned [`EncodeStats`].
    fn encode_into(&mut self, batch: &EventBatch, out: &mut MessageBuf<usize>) -> EncodeStats {
        out.clear();
        let mut stats = EncodeStats::default();
        // Destructured rather than reached through `self`: the packing loop borrows most of these
        // at once, which a chain of `&mut self` methods could not express (`collectd/encode.rs`
        // does the same).
        let Self {
            telemetry,
            diag,
            protocol,
            tags,
            multi_value,
            max_packet_bytes,
            max_frame_bytes,
            tag_suffix,
            tag_text,
            tag_slots,
            tag_value,
            path,
            full_path,
            suffix,
            number,
            line,
            frame,
            datapoint,
        } = self;
        let mut ctx = Ctx { telemetry, diag, stats: &mut stats };
        let mut sink = Sink {
            protocol: *protocol,
            max_packet_bytes: *max_packet_bytes,
            max_frame_bytes: *max_frame_bytes,
            full_path,
            suffix,
            number,
            line,
            frame,
            datapoint,
            datapoints_in_frame: 0,
            out,
        };
        sink.frame.clear();

        for event in &batch.events {
            if event.metrics.is_empty() {
                ctx.stats.skipped_no_metrics += 1;
                continue;
            }

            // Whole seconds, floored -- `div_euclid` rather than `/`, so a pre-epoch instant floors
            // downward instead of toward zero (normalization 6). Carbon has no sub-second
            // resolution and no pre-epoch second worth writing.
            let seconds = event.timestamp.div_euclid(NANOS_PER_SECOND);
            if seconds <= 0 {
                ctx.drop_unencodable_timestamp(event.timestamp, event.metrics.len());
                continue;
            }

            // The tag segment is the same for every record on the event, so it is built once.
            let tags_sanitized = build_tag_suffix(
                tag_suffix,
                tag_text,
                tag_slots,
                tag_value,
                &batch.resource,
                event,
                *tags,
                &mut ctx,
            );

            for record in &event.metrics {
                encode_record(
                    record,
                    seconds,
                    tag_suffix,
                    tags_sanitized,
                    path,
                    *multi_value,
                    &mut sink,
                    &mut ctx,
                );
            }
        }

        sink.close_frame();
        stats
    }
}

/// What a record's kind resolves to before its path is rendered. Deciding this first means a
/// dropped kind never counts a path sanitization that was not going to reach the wire.
enum Plan {
    /// One datapoint carrying this value.
    Scalar(f64),
    /// Several dotted sub-paths -- [`expand`] walks the kind again to emit them.
    Expand,
}

#[allow(clippy::too_many_arguments)]
fn encode_record(
    record: &MetricRecord,
    seconds: i64,
    tag_suffix: &str,
    tags_sanitized: bool,
    path: &mut String,
    multi_value: MultiValue,
    sink: &mut Sink,
    ctx: &mut Ctx,
) {
    let name = resolve(record.name);

    // Carbon's wire has no "no reading this interval" marker, so the flag's default numeric payload
    // must not be written as though it were a real sample (`logit_core::MetricRecord::flags`).
    if record.is_no_recorded_value() {
        ctx.drop_no_recorded_value(name);
        return;
    }

    // Exhaustive, one arm per variant, no wildcard -- AGENTS.md's rule, so a new `MetricKind`
    // fails to compile here rather than silently taking a default path.
    let plan = match &record.kind {
        // Both temporalities and both monotonicities write the bare value: carbon's wire has no
        // opinion about either, so this is normalization 12, a named drop of the model's extra
        // facts rather than a skipped metric the way `prometheus_out`'s delta arm is.
        MetricKind::Sum(sum) => Some(Plan::Scalar(sum.value)),
        MetricKind::Gauge(v) => Some(Plan::Scalar(*v)),
        MetricKind::GaugeDelta(_) => {
            ctx.drop_gauge_delta();
            return;
        }
        MetricKind::Samples(_) => multi(multi_value, "samples", name, ctx),
        MetricKind::Distribution(_) => multi(multi_value, "distribution", name, ctx),
        MetricKind::SetMembers(_) => multi(multi_value, "set_members", name, ctx),
        MetricKind::Set(_) => multi(multi_value, "set", name, ctx),
        MetricKind::Histogram(_) => multi(multi_value, "histogram", name, ctx),
        MetricKind::ExponentialHistogram(_) => {
            multi(multi_value, "exponential_histogram", name, ctx)
        }
        MetricKind::Summary(_) => multi(multi_value, "summary", name, ctx),
    };
    // `None` means [`multi`] already counted the drop under `multi_value: skip`.
    let Some(plan) = plan else { return };

    if let Plan::Scalar(value) = plan {
        if !value.is_finite() {
            ctx.drop_unencodable_value(name, value);
            return;
        }
    }

    path.clear();
    if sanitize_into(path, name, is_forbidden_in_path) {
        ctx.path_sanitized();
    }
    if path.is_empty() {
        ctx.drop_empty_name(name);
        return;
    }
    if tags_sanitized {
        ctx.tag_sanitized();
    }

    match plan {
        Plan::Scalar(value) => {
            sink.suffix.clear();
            sink.emit(path, tag_suffix, value, seconds, ctx);
        }
        Plan::Expand => {
            ctx.degrade_expanded(metric_kind_tag(&record.kind), name);
            expand(&record.kind, path, tag_suffix, seconds, sink, ctx);
        }
    }
}

/// [`MultiValue::Skip`] counts the drop and returns `None` (the caller returns);
/// [`MultiValue::Expand`] returns [`Plan::Expand`].
fn multi(
    multi_value: MultiValue,
    metric_kind: &'static str,
    name: &str,
    ctx: &mut Ctx,
) -> Option<Plan> {
    match multi_value {
        MultiValue::Skip => {
            ctx.drop_kind(metric_kind, name);
            None
        }
        MultiValue::Expand => Some(Plan::Expand),
    }
}

/// The `metric_kind` counter tag for a multi-value kind -- `&'static str`, as every tag must be.
/// Exhaustive over the same variants [`encode_record`] matches; the scalar kinds are unreachable
/// here because they never reach [`Plan::Expand`].
fn metric_kind_tag(kind: &MetricKind) -> &'static str {
    match kind {
        MetricKind::Samples(_) => "samples",
        MetricKind::Distribution(_) => "distribution",
        MetricKind::SetMembers(_) => "set_members",
        MetricKind::Set(_) => "set",
        MetricKind::Histogram(_) => "histogram",
        MetricKind::ExponentialHistogram(_) => "exponential_histogram",
        MetricKind::Summary(_) => "summary",
        MetricKind::Sum(_) | MetricKind::Gauge(_) | MetricKind::GaugeDelta(_) => {
            unreachable!("a scalar kind never expands")
        }
    }
}

/// The [`MultiValue::Expand`] sub-path table from [`super`]'s module doc, emitted in the order it
/// lists. Every arm adds at least one dotted suffix, which is what makes an expanded path
/// unable to collide with the scalar path the same record would have had.
fn expand(
    kind: &MetricKind,
    path: &str,
    tag_suffix: &str,
    seconds: i64,
    sink: &mut Sink,
    ctx: &mut Ctx,
) {
    match kind {
        // `sketch()` allocates a `DdSketch` per record -- inherent to re-summarizing raw
        // observations, and exactly what `influxdb_out`/`otlp_out` already pay
        // (`logit_core::Samples::sketch`).
        MetricKind::Samples(samples) => {
            expand_sketch(&samples.sketch(), path, tag_suffix, seconds, sink, ctx)
        }
        MetricKind::Distribution(sketch) => {
            expand_sketch(sketch, path, tag_suffix, seconds, sink, ctx)
        }
        MetricKind::Set(hll) => {
            sub(sink, ctx, path, tag_suffix, seconds, ".count", hll.estimate() as f64)
        }
        MetricKind::SetMembers(members) => {
            // One de-duplication buffer per record. The only allocation in this function that is
            // not inherent to the kind, and it is bounded by the record's own member count; a
            // `SetMembers` reaching a sink unsummarized already means no `aggregate` stage ran.
            let mut distinct: Vec<&[u8]> = members.iter().map(|m| m.as_ref()).collect();
            distinct.sort_unstable();
            distinct.dedup();
            sub(sink, ctx, path, tag_suffix, seconds, ".count", distinct.len() as f64);
        }
        MetricKind::Histogram(histogram) => {
            expand_histogram(histogram, path, tag_suffix, seconds, sink, ctx)
        }
        MetricKind::ExponentialHistogram(histogram) => {
            expand_exp_histogram(histogram, path, tag_suffix, seconds, sink, ctx)
        }
        MetricKind::Summary(summary) => {
            expand_summary(summary, path, tag_suffix, seconds, sink, ctx)
        }
        MetricKind::Sum(_) | MetricKind::Gauge(_) | MetricKind::GaugeDelta(_) => {
            unreachable!("a scalar kind never expands")
        }
    }
}

/// `.count`, `.sum`, then one `.q<q>` per [`DISTRIBUTION_QUANTILES`].
///
/// `.sum` is emitted because [`DdSketch::sum`] is **exact** -- the inner crate accumulates it
/// alongside the bins rather than deriving it from them -- unlike a quantile, which carries the
/// sketch's relative-error bound. A quantile the sketch cannot answer (an empty sketch) or that
/// comes back non-finite is simply not emitted: the record is already counted degraded, and a
/// fabricated `inf` datapoint would be worse than a missing one.
fn expand_sketch(
    sketch: &DdSketch,
    path: &str,
    tag_suffix: &str,
    seconds: i64,
    sink: &mut Sink,
    ctx: &mut Ctx,
) {
    sub(sink, ctx, path, tag_suffix, seconds, ".count", sketch.count() as f64);
    sub(sink, ctx, path, tag_suffix, seconds, ".sum", sketch.sum());
    for q in DISTRIBUTION_QUANTILES {
        let Some(v) = sketch.quantile(q).filter(|v| v.is_finite()) else { continue };
        sink.suffix.clear();
        sink.suffix.push_str(".q");
        push_number_token(sink.suffix, sink.number, q);
        sink.emit(path, tag_suffix, v, seconds, ctx);
    }
}

/// `.count` (Σ bucket counts), `.sum`/`.min`/`.max` when present, then `.bucket_<bound>` per
/// bucket. Each bucket carries its **own** count, not a cumulative running total
/// (`logit_core::Histogram`'s own doc) -- re-deriving a cumulative series here would be a
/// reinterpretation, not a rendering.
fn expand_histogram(
    histogram: &Histogram,
    path: &str,
    tag_suffix: &str,
    seconds: i64,
    sink: &mut Sink,
    ctx: &mut Ctx,
) {
    let count: u64 = histogram.buckets.iter().map(|(_, c)| *c).sum();
    sub(sink, ctx, path, tag_suffix, seconds, ".count", count as f64);
    optional(sink, ctx, path, tag_suffix, seconds, ".sum", histogram.sum);
    optional(sink, ctx, path, tag_suffix, seconds, ".min", histogram.min);
    optional(sink, ctx, path, tag_suffix, seconds, ".max", histogram.max);
    for (bound, bucket_count) in &histogram.buckets {
        sink.suffix.clear();
        sink.suffix.push_str(".bucket_");
        push_number_token(sink.suffix, sink.number, *bound);
        sink.emit(path, tag_suffix, *bucket_count as f64, seconds, ctx);
    }
}

/// `.count`, `.sum`/`.min`/`.max` when present, `.zero_count` -- and deliberately **no buckets**.
///
/// An exponential histogram's buckets are a `(scale, offset, counts)` encoding whose bounds are
/// `base^i` for `base = 2^(2^-scale)`; materializing them as explicit `.bucket_<b>` sub-paths would
/// be exactly the lossy conversion [`MetricKind::ExponentialHistogram`] exists to avoid, and would
/// mint an unbounded number of wire paths from one record besides.
fn expand_exp_histogram(
    histogram: &ExpHistogram,
    path: &str,
    tag_suffix: &str,
    seconds: i64,
    sink: &mut Sink,
    ctx: &mut Ctx,
) {
    sub(sink, ctx, path, tag_suffix, seconds, ".count", histogram.count as f64);
    optional(sink, ctx, path, tag_suffix, seconds, ".sum", histogram.sum);
    optional(sink, ctx, path, tag_suffix, seconds, ".min", histogram.min);
    optional(sink, ctx, path, tag_suffix, seconds, ".max", histogram.max);
    sub(sink, ctx, path, tag_suffix, seconds, ".zero_count", histogram.zero_count as f64);
}

/// `.count`, `.sum`, then one `.q<q>` per quantile the summary itself carries -- keyed on the raw
/// quantile rather than a rounded percentage, since rounding is not collision-free (`0.991` and
/// `0.994` would both become `p99`, the argument `influxdb_out`'s `render_fields` makes).
fn expand_summary(
    summary: &Summary,
    path: &str,
    tag_suffix: &str,
    seconds: i64,
    sink: &mut Sink,
    ctx: &mut Ctx,
) {
    sub(sink, ctx, path, tag_suffix, seconds, ".count", summary.count as f64);
    sub(sink, ctx, path, tag_suffix, seconds, ".sum", summary.sum);
    for (q, v) in &summary.quantiles {
        if !v.is_finite() {
            continue;
        }
        sink.suffix.clear();
        sink.suffix.push_str(".q");
        push_number_token(sink.suffix, sink.number, *q);
        sink.emit(path, tag_suffix, *v, seconds, ctx);
    }
}

/// One sub-path with a literal suffix.
fn sub(
    sink: &mut Sink,
    ctx: &mut Ctx,
    path: &str,
    tag_suffix: &str,
    seconds: i64,
    suffix: &str,
    value: f64,
) {
    if !value.is_finite() {
        return;
    }
    sink.suffix.clear();
    sink.suffix.push_str(suffix);
    sink.emit(path, tag_suffix, value, seconds, ctx);
}

/// [`sub`] for a field the model carries as an `Option` -- absent means "the producer did not
/// report it", which is not the same as zero and must not be written as one.
#[allow(clippy::too_many_arguments)]
fn optional(
    sink: &mut Sink,
    ctx: &mut Ctx,
    path: &str,
    tag_suffix: &str,
    seconds: i64,
    suffix: &str,
    value: Option<f64>,
) {
    if let Some(value) = value {
        sub(sink, ctx, path, tag_suffix, seconds, suffix, value);
    }
}

/// The wire-writing half: everything that knows about lines, frames and size caps, so the kind
/// walkers above never do. Holds the reusable buffers by reference; `datapoints_in_frame` is the
/// only state that outlives one datapoint.
struct Sink<'a> {
    protocol: Protocol,
    max_packet_bytes: usize,
    max_frame_bytes: usize,
    full_path: &'a mut String,
    suffix: &'a mut String,
    number: &'a mut String,
    line: &'a mut String,
    frame: &'a mut Vec<u8>,
    datapoint: &'a mut Vec<u8>,
    datapoints_in_frame: usize,
    out: &'a mut MessageBuf<usize>,
}

impl Sink<'_> {
    /// Writes one datapoint: `path` + the current [`Sink::suffix`] + `tag_suffix`, carrying
    /// `value` at `seconds`.
    fn emit(&mut self, path: &str, tag_suffix: &str, value: f64, seconds: i64, ctx: &mut Ctx) {
        self.full_path.clear();
        self.full_path.push_str(path);
        self.full_path.push_str(self.suffix.as_str());
        self.full_path.push_str(tag_suffix);

        match self.protocol {
            Protocol::Plaintext => {
                self.line.clear();
                self.line.push_str(self.full_path.as_str());
                // Rust's `{}` for `f64` is the shortest round-trip rendering (normalization 8):
                // `3.0` writes as `3`, `1.50` as `1.5`, and re-parsing gives back the same bits.
                let _ = write!(self.line, " {value} {seconds}");
                if self.line.len() > self.max_packet_bytes {
                    ctx.drop_oversize_line(self.line.len(), self.max_packet_bytes);
                    return;
                }
                self.out.push_with(self.line.as_bytes(), 1);
                ctx.stats.datapoints += 1;
            }
            Protocol::Pickle => {
                self.datapoint.clear();
                pickle::write_datapoint(self.datapoint, self.full_path.as_str(), seconds, value);
                // A datapoint that cannot fit an *empty* frame will never fit any frame, so it is
                // dropped rather than opening a frame nothing can close under the cap.
                if pickle::HEADER_BYTES + self.datapoint.len() + pickle::TRAILER_BYTES
                    > self.max_frame_bytes
                {
                    ctx.drop_oversize_datapoint(self.datapoint.len(), self.max_frame_bytes);
                    return;
                }
                if self.frame.is_empty() {
                    self.open_frame();
                }
                if self.payload_len() + self.datapoint.len() + pickle::TRAILER_BYTES
                    > self.max_frame_bytes
                {
                    self.close_frame();
                    self.open_frame();
                }
                self.frame.extend_from_slice(self.datapoint.as_slice());
                self.datapoints_in_frame += 1;
                ctx.stats.datapoints += 1;
            }
        }
    }

    /// Bytes of pickle payload currently in `frame` -- the frame buffer also carries the 4-byte
    /// length prefix, and `max_frame_bytes` bounds the payload (Twisted's `Int32StringReceiver`
    /// applies `MAX_LENGTH` to the declared length, not to the declaration plus the body).
    fn payload_len(&self) -> usize {
        self.frame.len() - pickle::LENGTH_PREFIX_BYTES
    }

    /// Starts a frame: four placeholder bytes for the length prefix, then the pickle header. The
    /// prefix is patched in place by [`Sink::close_frame`], so a frame is assembled once with no
    /// second buffer and no copy.
    fn open_frame(&mut self) {
        self.frame.clear();
        self.frame.extend_from_slice(&[0u8; pickle::LENGTH_PREFIX_BYTES]);
        pickle::write_header(self.frame);
        self.datapoints_in_frame = 0;
    }

    /// Closes the frame in progress, if it carries anything, and pushes it as one message whose
    /// meta is its datapoint count.
    fn close_frame(&mut self) {
        if self.datapoints_in_frame == 0 {
            self.frame.clear();
            return;
        }
        pickle::write_trailer(self.frame);
        let payload_len = self.payload_len() as u32;
        self.frame[..pickle::LENGTH_PREFIX_BYTES].copy_from_slice(&payload_len.to_be_bytes());
        self.out.push_with(self.frame.as_slice(), self.datapoints_in_frame);
        self.frame.clear();
        self.datapoints_in_frame = 0;
    }
}

// -- tags -----------------------------------------------------------------------------------------

/// One attribute that survived far enough to be a candidate tag. The three ranges point into the
/// shared `tag_text` arena, so an event's whole tag set costs no per-tag allocation.
#[derive(Debug)]
struct TagSlot {
    /// The sanitized tag name, as it will appear on the wire.
    rendered: Range<usize>,
    /// The sanitized tag value.
    value: Range<usize>,
    /// The attribute's original, unsanitized name -- the collision tie-break, and the only thing
    /// that makes that tie-break independent of interner order.
    original: Range<usize>,
}

/// Builds `;name=value…` into `suffix` for one event, returning whether any name or value had a
/// byte substituted.
///
/// Order is ascending **rendered** name (normalization 4 -- carbon's own `TaggedSeries.format`
/// sorts too), with the original name as the tie-break. Two attributes whose names sanitize onto
/// one wire name are a collision: the one whose **original** name sorts first survives and the rest
/// are dropped and counted. Resolving on the rendered and original *names* rather than on interner
/// order is ADR `prometheus-scrape-and-exposition`'s rule, and is what makes the choice reproducible
/// across processes.
#[allow(clippy::too_many_arguments)]
fn build_tag_suffix(
    suffix: &mut String,
    text: &mut String,
    slots: &mut Vec<TagSlot>,
    scratch: &mut String,
    resource: &Resource,
    event: &Event,
    tags: Tags,
    ctx: &mut Ctx,
) -> bool {
    suffix.clear();
    text.clear();
    slots.clear();
    let mut sanitized = false;

    for (key, value) in logit_core::attrs::merged(resource, event) {
        let key = resolve(key);
        if FOREIGN_CARRIER_PREFIXES.iter().any(|prefix| key.starts_with(prefix)) {
            continue;
        }
        if tags == Tags::Drop {
            ctx.tag_dropped_dialect();
            continue;
        }

        // An `Array` renders its *last* representable element, walked backwards so a trailing
        // unrepresentable one falls through to the element before it -- `influxdb_out`'s rule,
        // byte for byte, so one repeated DogStatsD tag key reaches both sinks the same way.
        scratch.clear();
        let is_multi_value = matches!(value, Value::Array(_));
        let rendered = match value {
            Value::Array(elements) => elements.iter().rev().any(|element| {
                scratch.clear();
                render_scalar(scratch, element)
            }),
            scalar => render_scalar(scratch, scalar),
        };
        if !rendered {
            ctx.tag_dropped_unrepresentable();
            continue;
        }
        if is_multi_value {
            ctx.tag_normalized_multi_value();
        }

        let name_start = text.len();
        let name_substituted = sanitize_into(text, key, is_forbidden_in_tag_name);
        let name_end = text.len();
        let value_substituted = sanitize_tag_value_into(text, scratch);
        let value_end = text.len();
        if name_end == name_start || value_end == name_end {
            // Carbon's own parser rejects an empty tag name or value, taking the whole line with
            // it -- so the tag goes rather than the metric.
            text.truncate(name_start);
            ctx.tag_dropped_empty();
            continue;
        }
        let original_start = text.len();
        text.push_str(key);
        sanitized |= name_substituted || value_substituted;
        slots.push(TagSlot {
            rendered: name_start..name_end,
            value: name_end..value_end,
            original: original_start..text.len(),
        });
    }

    if slots.is_empty() {
        return sanitized;
    }

    let arena = &*text;
    slots.sort_by(|a, b| {
        arena[a.rendered.clone()]
            .cmp(&arena[b.rendered.clone()])
            .then_with(|| arena[a.original.clone()].cmp(&arena[b.original.clone()]))
    });

    let mut previous: Option<Range<usize>> = None;
    for slot in slots.iter() {
        if let Some(previous) = &previous {
            if arena[previous.clone()] == arena[slot.rendered.clone()] {
                ctx.tag_dropped_collision();
                continue;
            }
        }
        suffix.push(';');
        suffix.push_str(&arena[slot.rendered.clone()]);
        suffix.push('=');
        suffix.push_str(&arena[slot.value.clone()]);
        previous = Some(slot.rendered.clone());
    }

    sanitized
}

/// Renders one non-`Array` [`Value`] as a tag value, returning whether it has a faithful spelling
/// at all. Carbon tags are strings, so every kind with an honest string form gets one and the type
/// is lost; the rest are dropped rather than given an invented syntax (`Bytes` need not be UTF-8,
/// and flattening a `Map` into one tag value would produce something nothing parses back).
fn render_scalar(out: &mut String, value: &Value) -> bool {
    match value {
        Value::Str(_) => {
            // `Value::Str` is always valid UTF-8 by construction (`logit_core::Value::str`).
            out.push_str(value.as_str().unwrap_or_default());
            true
        }
        Value::I64(v) => {
            let _ = write!(out, "{v}");
            true
        }
        Value::U64(v) => {
            let _ = write!(out, "{v}");
            true
        }
        Value::F64(v) => {
            let _ = write!(out, "{v}");
            true
        }
        Value::Bool(v) => {
            out.push_str(if *v { "true" } else { "false" });
            true
        }
        Value::Null | Value::Bytes(_) | Value::Timestamp(_) | Value::Map(_) | Value::Array(_) => {
            false
        }
    }
}

// -- sanitization ---------------------------------------------------------------------------------

/// Appends `s` to `out` (does **not** clear it first), replacing every character `forbidden`
/// rejects with `_`, and reports whether it replaced any. Substitution, not deletion, so distinct
/// inputs stay distinct -- `crates/logit-outputs/src/statsd.rs`'s `sanitize_into`, which this is
/// modelled on directly.
fn sanitize_into(out: &mut String, s: &str, forbidden: impl Fn(char) -> bool) -> bool {
    let mut substituted = false;
    for c in s.chars() {
        if forbidden(c) {
            out.push('_');
            substituted = true;
        } else {
            out.push(c);
        }
    }
    substituted
}

/// [`sanitize_into`] for a tag value, whose rule has one position-dependent case: a **leading** `~`
/// is reserved by carbon's tag grammar, while a `~` anywhere else is an ordinary byte and rides
/// through untouched.
fn sanitize_tag_value_into(out: &mut String, s: &str) -> bool {
    let mut substituted = false;
    for (i, c) in s.chars().enumerate() {
        if is_forbidden_in_tag_value(c) || (i == 0 && c == '~') {
            out.push('_');
            substituted = true;
        } else {
            out.push(c);
        }
    }
    substituted
}

/// Forbidden in a path: `;` opens the tag segment, whitespace ends the field, a control byte would
/// corrupt line framing, and `/`/`\` are whisper's **directory separators** -- a path component
/// carrying one would create a nested directory rather than a series segment. Whitespace is
/// [`char::is_whitespace`] rather than ASCII-only because carbon splits a decoded `str` with
/// Python's `str.split()`, which does the same (see [`super`]'s sanitization section).
fn is_forbidden_in_path(c: char) -> bool {
    matches!(c, ';' | '/' | '\\') || c.is_whitespace() || c.is_control()
}

/// Forbidden in a tag name -- carbon's own `TaggedSeries` grammar reserves `;`, `!`, `^` and `=`
/// (the first separates tags, the last separates a name from its value, and `!`/`^` are its
/// query-syntax operators), plus the whitespace and control bytes every field forbids.
fn is_forbidden_in_tag_name(c: char) -> bool {
    matches!(c, ';' | '!' | '^' | '=') || c.is_whitespace() || c.is_control()
}

/// Forbidden anywhere in a tag value. Narrower than a tag name's set: `=` is legal in a value
/// (carbon splits on the *first* `=`, so `k=a=b` round-trips as `k` → `a=b`), and so are `!`/`^`.
/// The leading-`~` rule lives in [`sanitize_tag_value_into`], since it is positional.
fn is_forbidden_in_tag_value(c: char) -> bool {
    c == ';' || c.is_whitespace() || c.is_control()
}

/// Appends `v` as a sub-path token: Rust's `{}` rendering with every `.` substituted by `_`.
///
/// **Injective**, which is what makes an expanded sub-path collision-free. `Display` for `f64`
/// emits only `-`, decimal digits, at most one `.`, and the literals `inf`/`-inf`/`NaN`, so
/// substituting `.` is a bijection on that alphabet: `0.5 → 0_5`, `5 → 5`, `0.05 → 0_05`,
/// `-0.5 → -0_5`, `inf → inf` are five distinct tokens for five distinct bounds. Contrast a
/// *rounded* percentile, which `influxdb_out`'s `render_fields` rejects precisely because it is
/// not.
fn push_number_token(out: &mut String, scratch: &mut String, v: f64) {
    scratch.clear();
    let _ = write!(scratch, "{v}");
    for c in scratch.chars() {
        out.push(if c == '.' { '_' } else { c });
    }
}

// -- counters -------------------------------------------------------------------------------------

/// The telemetry/diagnostics/stats triple every drop site needs, carried together so a drop reports
/// itself in all three places at once and can never be counted in one but not the others
/// ([`crate::collectd::encode`]'s `Ctx`, and `crates/logit-outputs/src/statsd.rs`'s `EncodeCtx`).
struct Ctx<'a> {
    telemetry: &'a Telemetry,
    diag: &'a mut Diagnostics,
    stats: &'a mut EncodeStats,
}

impl Ctx<'_> {
    fn drop_kind(&mut self, metric_kind: &'static str, name: &str) {
        self.stats.dropped_unsupported_kind += 1;
        self.telemetry.count("logit.output.metrics.skipped", 1.0, &[("metric_kind", metric_kind)]);
        self.diag.warn_throttled(
            "unsupported_metric_kind",
            format_args!(
                "graphite_out: {metric_kind} (metric {name:?}) is not one number at one second, \
                 which is all a carbon datapoint is; dropping. Set `multi_value: expand` to emit \
                 dotted sub-paths instead"
            ),
        );
    }

    fn degrade_expanded(&mut self, metric_kind: &'static str, name: &str) {
        self.stats.degraded_expanded_kind += 1;
        self.telemetry.count("logit.output.metrics.degraded", 1.0, &[("metric_kind", metric_kind)]);
        self.diag.warn_throttled(
            "multi_value_expanded",
            format_args!(
                "graphite_out: {metric_kind} (metric {name:?}) expanded into dotted sub-paths; \
                 the aggregate's mergeability is lost on the wire"
            ),
        );
    }

    fn drop_gauge_delta(&mut self) {
        self.stats.dropped_gauge_delta += 1;
        self.telemetry.count(
            "logit.output.metrics.skipped",
            1.0,
            &[("metric_kind", "gauge_delta")],
        );
        self.diag.warn_throttled(
            "gauge_delta_unresolved",
            "a relative gauge adjustment reached a sink unresolved -- add an `aggregate` component \
             between the statsd input and this output",
        );
    }

    fn drop_unencodable_value(&mut self, name: &str, value: f64) {
        self.stats.dropped_unencodable_value += 1;
        self.telemetry.count(
            "logit.output.metrics.skipped",
            1.0,
            &[("reason", "unencodable_value")],
        );
        self.diag.warn_throttled(
            "unencodable_value",
            format_args!(
                "graphite_out: {value} on metric {name:?} has no carbon wire form (carbon drops a \
                 NaN on receipt itself); dropping"
            ),
        );
    }

    fn drop_no_recorded_value(&mut self, name: &str) {
        self.stats.dropped_no_recorded_value += 1;
        self.telemetry.count(
            "logit.output.metrics.skipped",
            1.0,
            &[("reason", "no_recorded_value")],
        );
        self.diag.warn_throttled(
            "no_recorded_value",
            format_args!(
                "graphite_out: metric {name:?} has no recorded value (OTLP NO_RECORDED_VALUE) and \
                 carbon has no marker for one; dropping rather than writing its default payload"
            ),
        );
    }

    /// `records` is how many records the dropped event carried -- `logit.output.metrics.skipped` is
    /// a per-record figure at every other sink, and an operator summing it across sinks needs this
    /// one to mean the same thing.
    fn drop_unencodable_timestamp(&mut self, timestamp: i64, records: usize) {
        self.stats.dropped_unencodable_timestamp += records;
        self.telemetry.count(
            "logit.output.metrics.skipped",
            records as f64,
            &[("reason", "unencodable_timestamp")],
        );
        self.diag.warn_throttled(
            "unencodable_timestamp",
            format_args!(
                "graphite_out: timestamp {timestamp} floors to a non-positive second; dropping the \
                 event rather than stamping one nothing upstream reported"
            ),
        );
    }

    fn drop_empty_name(&mut self, name: &str) {
        self.stats.dropped_empty_name += 1;
        self.telemetry.count("logit.output.metrics.skipped", 1.0, &[("reason", "empty_name")]);
        self.diag.warn_throttled(
            "empty_metric_name",
            format_args!(
                "graphite_out: metric {name:?} sanitizes to an empty path; dropping (carbon's own \
                 receiver rejects the same line)"
            ),
        );
    }

    fn drop_oversize_line(&mut self, len: usize, max_packet_bytes: usize) {
        self.stats.dropped_oversize_line += 1;
        self.telemetry.count("logit.output.metrics.skipped", 1.0, &[("reason", "oversize_line")]);
        self.diag.warn_throttled(
            "oversize_line",
            format_args!(
                "graphite_out: a {len}-byte line exceeds max_packet_bytes ({max_packet_bytes}); \
                 dropping it whole rather than splitting it across datagrams"
            ),
        );
    }

    fn drop_oversize_datapoint(&mut self, len: usize, max_frame_bytes: usize) {
        self.stats.dropped_oversize_datapoint += 1;
        self.telemetry.count(
            "logit.output.metrics.skipped",
            1.0,
            &[("reason", "oversize_datapoint")],
        );
        self.diag.warn_throttled(
            "oversize_datapoint",
            format_args!(
                "graphite_out: a single {len}-byte pickle datapoint cannot fit a \
                 {max_frame_bytes}-byte frame; dropping it whole"
            ),
        );
    }

    fn tag_dropped_dialect(&mut self) {
        self.stats.tags_dropped_dialect += 1;
        self.telemetry.count("logit.output.tags.dropped", 1.0, &[("reason", "dialect")]);
    }

    fn tag_dropped_unrepresentable(&mut self) {
        self.stats.tags_dropped_unrepresentable += 1;
        self.telemetry.count("logit.output.tags.dropped", 1.0, &[("reason", "unrepresentable")]);
    }

    fn tag_dropped_empty(&mut self) {
        self.stats.tags_dropped_empty += 1;
        self.telemetry.count("logit.output.tags.dropped", 1.0, &[("reason", "empty")]);
    }

    fn tag_dropped_collision(&mut self) {
        self.stats.tags_dropped_collision += 1;
        self.telemetry.count("logit.output.tags.dropped", 1.0, &[("reason", "collision")]);
    }

    fn tag_normalized_multi_value(&mut self) {
        self.stats.tags_normalized_multi_value += 1;
        self.telemetry.count("logit.output.tags.normalized", 1.0, &[("reason", "multi_value")]);
    }

    fn path_sanitized(&mut self) {
        self.stats.paths_sanitized += 1;
        self.telemetry.count(
            "logit.output.metrics.normalized",
            1.0,
            &[("reason", "path_sanitized")],
        );
    }

    fn tag_sanitized(&mut self) {
        self.stats.tags_sanitized += 1;
        self.telemetry.count(
            "logit.output.metrics.normalized",
            1.0,
            &[("reason", "tag_sanitized")],
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner::intern;
    use logit_core::{
        AttrMap, ExpHistogram, HyperLogLog, MetricList, Registry, Samples, Sum, Temporality,
    };
    use std::sync::Arc;

    const TS: i64 = 1_700_000_000_000_000_000;
    const SECONDS: i64 = 1_700_000_000;

    fn event(kind: MetricKind) -> Event {
        Event::metric(TS, AttrMap::new(), MetricRecord::new(intern("sys.cpu"), kind))
    }

    fn batch(events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
    }

    /// Encodes `batch` through a fresh encoder built by `build`, returning the raw wire messages,
    /// their metas, the stats and the telemetry registry. **Raw bytes**, not text: a pickle frame
    /// is binary, and a lossy UTF-8 round trip through `String` would silently change its length.
    fn encode_raw_with(
        batch: &EventBatch,
        build: impl FnOnce(GraphiteEncoder) -> GraphiteEncoder,
    ) -> (Vec<Vec<u8>>, Vec<usize>, EncodeStats, Arc<Registry>) {
        let registry = Registry::new();
        let mut encoder = build(GraphiteEncoder::new().with_telemetry(registry.telemetry_for(
            "graphite_out",
            "graphite_out",
            "sink",
        )));
        let mut out = MessageBuf::default();
        let stats = encoder.encode_into(batch, &mut out);
        let messages = out.iter().map(|m| m.to_vec()).collect::<Vec<_>>();
        let metas = out.iter_with().map(|(_, meta)| *meta).collect::<Vec<_>>();
        (messages, metas, stats, registry)
    }

    /// [`encode_raw_with`] for the plaintext tests, whose messages are always valid UTF-8 lines.
    fn encode_with(
        batch: &EventBatch,
        build: impl FnOnce(GraphiteEncoder) -> GraphiteEncoder,
    ) -> (Vec<String>, Vec<usize>, EncodeStats, Arc<Registry>) {
        let (messages, metas, stats, registry) = encode_raw_with(batch, build);
        let lines = messages
            .iter()
            .map(|m| String::from_utf8(m.clone()).expect("a plaintext line is utf-8"))
            .collect();
        (lines, metas, stats, registry)
    }

    fn encode(batch: &EventBatch) -> Vec<String> {
        encode_with(batch, |e| e).0
    }

    fn counted(registry: &Registry, metric: &str, tag: (&str, &str)) -> f64 {
        let mut total = 0.0;
        for event in registry.drain(0) {
            if event.attributes.get(tag.0).and_then(|v| v.as_str()) != Some(tag.1) {
                continue;
            }
            for m in &event.metrics {
                if logit_core::interner::resolve(m.name) != metric {
                    continue;
                }
                match &m.kind {
                    MetricKind::Sum(sum) => total += sum.value,
                    other => panic!("expected a Sum counter, got {other:?}"),
                }
            }
        }
        total
    }

    // -- scalar kinds ----------------------------------------------------------------------------

    #[test]
    fn a_gauge_becomes_one_line() {
        let (lines, metas, stats, _) =
            encode_with(&batch(vec![event(MetricKind::Gauge(0.5))]), |e| e);
        assert_eq!(lines, vec!["sys.cpu 0.5 1700000000"]);
        assert_eq!(metas, vec![1], "a plaintext entry is always one datapoint");
        assert_eq!(stats.datapoints, 1);
    }

    /// Normalization 12: carbon's wire has no temporality or monotonicity, so all four `Sum`
    /// shapes write the bare value. Not a skip -- the number itself is carried faithfully.
    #[test]
    fn every_sum_shape_writes_its_bare_value() {
        for temporality in [Temporality::Delta, Temporality::Cumulative] {
            for monotonic in [true, false] {
                let kind = MetricKind::Sum(Sum { value: 7.0, temporality, monotonic });
                let lines = encode(&batch(vec![event(kind)]));
                assert_eq!(
                    lines,
                    vec!["sys.cpu 7 1700000000"],
                    "{temporality:?}/{monotonic} must write the bare value"
                );
            }
        }
    }

    /// Normalization 8: Rust's `{}` is the shortest round-trip rendering.
    #[test]
    fn values_render_as_the_shortest_round_trip_f64() {
        for (value, rendered) in
            [(3.0, "3"), (1.5, "1.5"), (-0.25, "-0.25"), (1e5, "100000"), (1e-7, "0.0000001")]
        {
            let lines = encode(&batch(vec![event(MetricKind::Gauge(value))]));
            assert_eq!(lines, vec![format!("sys.cpu {rendered} 1700000000")], "{value}");
        }
    }

    #[test]
    fn a_non_finite_value_is_dropped_and_counted() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let (lines, _, stats, registry) =
                encode_with(&batch(vec![event(MetricKind::Gauge(value))]), |e| e);
            assert!(lines.is_empty(), "{value}");
            assert_eq!(stats.dropped_unencodable_value, 1, "{value}");
            assert_eq!(
                counted(&registry, "logit.output.metrics.skipped", ("reason", "unencodable_value")),
                1.0
            );
        }
    }

    #[test]
    fn a_flagged_record_is_dropped_and_counted() {
        let mut ev = event(MetricKind::Gauge(0.0));
        ev.metrics[0].flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
        let (lines, _, stats, registry) = encode_with(&batch(vec![ev]), |e| e);
        assert!(lines.is_empty());
        assert_eq!(stats.dropped_no_recorded_value, 1);
        assert_eq!(
            counted(&registry, "logit.output.metrics.skipped", ("reason", "no_recorded_value")),
            1.0
        );
    }

    #[test]
    fn a_gauge_delta_is_dropped_under_the_shared_key() {
        let (lines, _, stats, registry) =
            encode_with(&batch(vec![event(MetricKind::GaugeDelta(1.0))]), |e| e);
        assert!(lines.is_empty());
        assert_eq!(stats.dropped_gauge_delta, 1);
        assert_eq!(
            counted(&registry, "logit.output.metrics.skipped", ("metric_kind", "gauge_delta")),
            1.0
        );
    }

    #[test]
    fn an_event_with_no_metrics_is_skipped_without_a_counter() {
        let (lines, _, stats, _) =
            encode_with(&batch(vec![Event::empty(TS, AttrMap::new())]), |e| e);
        assert!(lines.is_empty());
        assert_eq!(stats.skipped_no_metrics, 1);
    }

    // -- timestamps ------------------------------------------------------------------------------

    #[test]
    fn a_non_positive_second_drops_every_record_on_the_event() {
        let mut ev = event(MetricKind::Gauge(1.0));
        ev.metrics.push(MetricRecord::new(intern("sys.mem"), MetricKind::Gauge(2.0)));
        for timestamp in [0i64, -1, -1_000_000_000] {
            ev.timestamp = timestamp;
            let (lines, _, stats, registry) = encode_with(&batch(vec![ev.clone()]), |e| e);
            assert!(lines.is_empty(), "{timestamp}");
            assert_eq!(stats.dropped_unencodable_timestamp, 2, "counted per record, not per event");
            assert_eq!(
                counted(
                    &registry,
                    "logit.output.metrics.skipped",
                    ("reason", "unencodable_timestamp")
                ),
                2.0
            );
        }
    }

    /// Normalization 6 floors, and `div_euclid` floors *downward* -- a sub-second positive instant
    /// floors to 0 and is therefore dropped, which is the same rule stated from the other side.
    #[test]
    fn timestamps_floor_to_whole_seconds() {
        let mut ev = event(MetricKind::Gauge(1.0));
        ev.timestamp = 1_700_000_000_999_999_999;
        assert_eq!(encode(&batch(vec![ev.clone()])), vec!["sys.cpu 1 1700000000"]);

        ev.timestamp = 999_999_999;
        let (lines, _, stats, _) = encode_with(&batch(vec![ev]), |e| e);
        assert!(lines.is_empty(), "a sub-second instant floors to second 0, which carbon rejects");
        assert_eq!(stats.dropped_unencodable_timestamp, 1);
    }

    // -- tags -------------------------------------------------------------------------------------

    fn tagged(attrs: &[(&str, Value)]) -> Event {
        let mut attributes = AttrMap::new();
        for (key, value) in attrs {
            attributes.insert(key, value.clone());
        }
        Event::metric(TS, attributes, MetricRecord::new(intern("sys.cpu"), MetricKind::Gauge(1.0)))
    }

    #[test]
    fn tags_render_in_ascending_rendered_name_order() {
        let ev = tagged(&[
            ("zulu", Value::from("z")),
            ("alpha", Value::from("a")),
            ("mike", Value::from("m")),
        ]);
        assert_eq!(
            encode(&batch(vec![ev])),
            vec!["sys.cpu;alpha=a;mike=m;zulu=z 1 1700000000"],
            "carbon's own TaggedSeries.format sorts too (normalization 4)"
        );
    }

    #[test]
    fn a_resource_attribute_becomes_a_tag_and_an_event_attribute_wins_a_tie() {
        let mut resource_attrs = AttrMap::new();
        resource_attrs.insert("env", "staging");
        resource_attrs.insert("region", "us-east");
        let resource = Arc::new(Resource { attributes: resource_attrs, ..Resource::default() });
        let ev = tagged(&[("env", Value::from("prod"))]);
        let batch = EventBatch { resource, scope: None, events: vec![ev] };
        assert_eq!(encode(&batch), vec!["sys.cpu;env=prod;region=us-east 1 1700000000"]);
    }

    #[test]
    fn every_value_kind_renders_or_is_dropped_and_counted() {
        let ev = tagged(&[
            ("s", Value::from("text")),
            ("i", Value::I64(-3)),
            ("u", Value::U64(7)),
            ("f", Value::F64(1.5)),
            ("b", Value::Bool(true)),
        ]);
        assert_eq!(
            encode(&batch(vec![ev])),
            vec!["sys.cpu;b=true;f=1.5;i=-3;s=text;u=7 1 1700000000"]
        );

        let ev = tagged(&[
            ("nul", Value::Null),
            ("byt", Value::Bytes(bytes::Bytes::from_static(b"\xff"))),
            ("ts", Value::Timestamp(1)),
            ("map", Value::Map(Box::new(AttrMap::new()))),
            ("keep", Value::from("yes")),
        ]);
        let (lines, _, stats, registry) = encode_with(&batch(vec![ev]), |e| e);
        assert_eq!(lines, vec!["sys.cpu;keep=yes 1 1700000000"]);
        assert_eq!(stats.tags_dropped_unrepresentable, 4);
        assert_eq!(
            counted(&registry, "logit.output.tags.dropped", ("reason", "unrepresentable")),
            4.0
        );
    }

    /// `influxdb_out`'s rule, byte for byte: the last representable element, walked backwards so a
    /// trailing unrepresentable one falls through.
    #[test]
    fn an_array_renders_its_last_representable_element_and_is_counted() {
        let ev = tagged(&[("team", Value::Array(vec![Value::from("a"), Value::from("b")]))]);
        let (lines, _, stats, registry) = encode_with(&batch(vec![ev]), |e| e);
        assert_eq!(lines, vec!["sys.cpu;team=b 1 1700000000"]);
        assert_eq!(stats.tags_normalized_multi_value, 1);
        assert_eq!(
            counted(&registry, "logit.output.tags.normalized", ("reason", "multi_value")),
            1.0
        );

        let ev = tagged(&[("team", Value::Array(vec![Value::from("a"), Value::Null]))]);
        assert_eq!(encode(&batch(vec![ev])), vec!["sys.cpu;team=a 1 1700000000"]);

        let ev = tagged(&[("team", Value::Array(vec![])), ("keep", Value::from("y"))]);
        let (lines, _, stats, _) = encode_with(&batch(vec![ev]), |e| e);
        assert_eq!(lines, vec!["sys.cpu;keep=y 1 1700000000"]);
        assert_eq!(stats.tags_dropped_unrepresentable, 1, "an empty array is unrepresentable");
    }

    #[test]
    fn tags_drop_emits_no_tag_segment_and_counts_every_tag() {
        let ev = tagged(&[("env", Value::from("prod")), ("host", Value::from("web-1"))]);
        let (lines, _, stats, registry) =
            encode_with(&batch(vec![ev]), |e| e.with_tags(Tags::Drop));
        assert_eq!(lines, vec!["sys.cpu 1 1700000000"]);
        assert_eq!(stats.tags_dropped_dialect, 2);
        assert_eq!(counted(&registry, "logit.output.tags.dropped", ("reason", "dialect")), 2.0);
    }

    /// Another protocol's consumed carriers are not tags anybody asked to see here, so they are
    /// skipped **uncounted** -- `influxdb_out`'s `statsd.` rule, widened to `collectd.` too.
    #[test]
    fn foreign_protocol_carriers_are_skipped_uncounted() {
        let ev = tagged(&[
            ("statsd.type", Value::from("ms")),
            ("collectd.plugin", Value::from("cpu")),
            ("env", Value::from("prod")),
        ]);
        let (lines, _, stats, _) = encode_with(&batch(vec![ev]), |e| e);
        assert_eq!(lines, vec!["sys.cpu;env=prod 1 1700000000"]);
        assert_eq!(stats.tags_dropped_dialect, 0);
        assert_eq!(stats.tags_dropped_unrepresentable, 0);
    }

    // -- sanitization -----------------------------------------------------------------------------

    /// Substitution, never deletion: `a b` and `ab` must not become one series.
    #[test]
    fn a_forbidden_path_byte_is_substituted_not_deleted() {
        for (name, rendered) in [
            ("a b", "a_b"),
            ("a\tb", "a_b"),
            ("a;b", "a_b"),
            ("a/b", "a_b"),
            ("a\\b", "a_b"),
            ("a\nb", "a_b"),
            ("a\u{00a0}b", "a_b"),
        ] {
            let ev = Event::metric(
                TS,
                AttrMap::new(),
                MetricRecord::new(intern(name), MetricKind::Gauge(1.0)),
            );
            let (lines, _, stats, registry) = encode_with(&batch(vec![ev]), |e| e);
            assert_eq!(lines, vec![format!("{rendered} 1 1700000000")], "{name:?}");
            assert_eq!(stats.paths_sanitized, 1, "{name:?}");
            assert_eq!(
                counted(&registry, "logit.output.metrics.normalized", ("reason", "path_sanitized")),
                1.0
            );
        }
    }

    #[test]
    fn a_tag_name_forbids_carbons_four_reserved_characters() {
        for (key, rendered) in
            [("a;b", "a_b"), ("a!b", "a_b"), ("a^b", "a_b"), ("a=b", "a_b"), ("a b", "a_b")]
        {
            let ev = tagged(&[(key, Value::from("v"))]);
            let (lines, _, stats, _) = encode_with(&batch(vec![ev]), |e| e);
            assert_eq!(lines, vec![format!("sys.cpu;{rendered}=v 1 1700000000")], "{key:?}");
            assert_eq!(stats.tags_sanitized, 1, "{key:?}");
        }
    }

    /// A tag *value* forbids less than a name: `=`, `!` and `^` are all legal in it, `~` is legal
    /// anywhere but the first position, and `;`/whitespace are not.
    #[test]
    fn a_tag_value_forbids_only_semicolons_whitespace_and_a_leading_tilde() {
        for (value, rendered, sanitized) in [
            ("a;b", "a_b", 1),
            ("a b", "a_b", 1),
            ("~lead", "_lead", 1),
            ("mid~tilde", "mid~tilde", 0),
            ("a=b", "a=b", 0),
            ("a!b^c", "a!b^c", 0),
        ] {
            let ev = tagged(&[("k", Value::from(value))]);
            let (lines, _, stats, _) = encode_with(&batch(vec![ev]), |e| e);
            assert_eq!(lines, vec![format!("sys.cpu;k={rendered} 1 1700000000")], "{value:?}");
            assert_eq!(stats.tags_sanitized, sanitized, "{value:?}");
        }
    }

    #[test]
    fn a_tag_empty_after_sanitizing_is_dropped_and_counted() {
        let ev =
            tagged(&[("", Value::from("v")), ("k", Value::from("")), ("ok", Value::from("y"))]);
        let (lines, _, stats, registry) = encode_with(&batch(vec![ev]), |e| e);
        assert_eq!(lines, vec!["sys.cpu;ok=y 1 1700000000"]);
        assert_eq!(stats.tags_dropped_empty, 2);
        assert_eq!(counted(&registry, "logit.output.tags.dropped", ("reason", "empty")), 2.0);
    }

    #[test]
    fn a_path_empty_after_sanitizing_drops_the_record() {
        let ev = Event::metric(
            TS,
            AttrMap::new(),
            MetricRecord::new(intern(""), MetricKind::Gauge(1.0)),
        );
        let (lines, _, stats, registry) = encode_with(&batch(vec![ev]), |e| e);
        assert!(lines.is_empty());
        assert_eq!(stats.dropped_empty_name, 1);
        assert_eq!(
            counted(&registry, "logit.output.metrics.skipped", ("reason", "empty_name")),
            1.0
        );
    }

    /// Two names sanitizing onto one wire name: the one whose **original** name sorts first
    /// survives, deterministically and independent of interner order.
    #[test]
    fn a_rendered_name_collision_keeps_the_one_whose_original_name_sorts_first() {
        let ev = tagged(&[("a;b", Value::from("semi")), ("a!b", Value::from("bang"))]);
        let (lines, _, stats, registry) = encode_with(&batch(vec![ev]), |e| e);
        assert_eq!(
            lines,
            vec!["sys.cpu;a_b=bang 1 1700000000"],
            "'a!b' sorts before 'a;b', so its value is the one kept"
        );
        assert_eq!(stats.tags_dropped_collision, 1);
        assert_eq!(counted(&registry, "logit.output.tags.dropped", ("reason", "collision")), 1.0);
    }

    // -- multi-value kinds -------------------------------------------------------------------------

    fn sketch(values: &[f64]) -> DdSketch {
        let mut sketch = DdSketch::new();
        for v in values {
            sketch.add(*v);
        }
        sketch
    }

    fn histogram() -> Histogram {
        Histogram {
            buckets: vec![(0.5, 1), (5.0, 2), (f64::INFINITY, 3)],
            temporality: Temporality::Delta,
            sum: Some(12.0),
            min: Some(0.25),
            max: Some(9.0),
        }
    }

    fn exp_histogram() -> ExpHistogram {
        ExpHistogram {
            scale: 0,
            zero_count: 2,
            zero_threshold: 0.0,
            positive: (0, vec![1, 2]),
            negative: (0, vec![]),
            temporality: Temporality::Delta,
            count: 5,
            sum: Some(10.0),
            min: Some(1.0),
            max: Some(4.0),
        }
    }

    fn set_members() -> MetricKind {
        MetricKind::SetMembers(vec![
            bytes::Bytes::from_static(b"a"),
            bytes::Bytes::from_static(b"b"),
            bytes::Bytes::from_static(b"a"),
        ])
    }

    fn hyper_log_log() -> HyperLogLog {
        let mut hll = HyperLogLog::new();
        hll.insert(b"a");
        hll.insert(b"b");
        hll
    }

    /// All seven multi-value kinds, one table: skipped by default, each under its own
    /// `metric_kind` tag.
    #[test]
    fn every_multi_value_kind_is_skipped_by_default() {
        for (kind, tag) in [
            (MetricKind::Samples(Samples::new([1.0])), "samples"),
            (MetricKind::Distribution(sketch(&[1.0])), "distribution"),
            (set_members(), "set_members"),
            (MetricKind::Set(hyper_log_log()), "set"),
            (MetricKind::Histogram(histogram()), "histogram"),
            (MetricKind::ExponentialHistogram(exp_histogram()), "exponential_histogram"),
            (
                MetricKind::Summary(Summary { quantiles: vec![(0.5, 1.0)], count: 1, sum: 1.0 }),
                "summary",
            ),
        ] {
            let (lines, _, stats, registry) = encode_with(&batch(vec![event(kind)]), |e| e);
            assert!(lines.is_empty(), "{tag}");
            assert_eq!(stats.dropped_unsupported_kind, 1, "{tag}");
            assert_eq!(
                counted(&registry, "logit.output.metrics.skipped", ("metric_kind", tag)),
                1.0,
                "{tag}"
            );
        }
    }

    /// The sub-path table from `mod.rs`, checked kind by kind against the exact paths it lists.
    #[test]
    fn expand_produces_the_documented_sub_paths_for_every_kind() {
        let cases: Vec<(MetricKind, &str, Vec<&str>)> = vec![
            (
                MetricKind::Distribution(sketch(&[1.0, 2.0, 3.0])),
                "distribution",
                vec![
                    "sys.cpu.count",
                    "sys.cpu.sum",
                    "sys.cpu.q0_5",
                    "sys.cpu.q0_75",
                    "sys.cpu.q0_9",
                    "sys.cpu.q0_95",
                    "sys.cpu.q0_99",
                ],
            ),
            (
                MetricKind::Samples(Samples::new([1.0, 2.0, 3.0])),
                "samples",
                vec![
                    "sys.cpu.count",
                    "sys.cpu.sum",
                    "sys.cpu.q0_5",
                    "sys.cpu.q0_75",
                    "sys.cpu.q0_9",
                    "sys.cpu.q0_95",
                    "sys.cpu.q0_99",
                ],
            ),
            (
                MetricKind::Histogram(histogram()),
                "histogram",
                vec![
                    "sys.cpu.count",
                    "sys.cpu.sum",
                    "sys.cpu.min",
                    "sys.cpu.max",
                    "sys.cpu.bucket_0_5",
                    "sys.cpu.bucket_5",
                    "sys.cpu.bucket_inf",
                ],
            ),
            (
                MetricKind::ExponentialHistogram(exp_histogram()),
                "exponential_histogram",
                vec![
                    "sys.cpu.count",
                    "sys.cpu.sum",
                    "sys.cpu.min",
                    "sys.cpu.max",
                    "sys.cpu.zero_count",
                ],
            ),
            (
                MetricKind::Summary(Summary {
                    quantiles: vec![(0.5, 1.0), (0.99, 9.0)],
                    count: 4,
                    sum: 12.0,
                }),
                "summary",
                vec!["sys.cpu.count", "sys.cpu.sum", "sys.cpu.q0_5", "sys.cpu.q0_99"],
            ),
            (MetricKind::Set(hyper_log_log()), "set", vec!["sys.cpu.count"]),
            (set_members(), "set_members", vec!["sys.cpu.count"]),
        ];

        for (kind, tag, expected) in cases {
            let (lines, _, stats, registry) =
                encode_with(&batch(vec![event(kind)]), |e| e.with_multi_value(MultiValue::Expand));
            let paths: Vec<&str> =
                lines.iter().map(|line| line.split(' ').next().unwrap()).collect();
            assert_eq!(paths, expected, "{tag}");
            assert_eq!(stats.degraded_expanded_kind, 1, "{tag}: counted once per record");
            assert_eq!(
                counted(&registry, "logit.output.metrics.degraded", ("metric_kind", tag)),
                1.0,
                "{tag}"
            );
        }
    }

    #[test]
    fn a_set_members_count_is_the_distinct_member_count() {
        let (lines, _, _, _) = encode_with(&batch(vec![event(set_members())]), |e| {
            e.with_multi_value(MultiValue::Expand)
        });
        assert_eq!(lines, vec!["sys.cpu.count 2 1700000000"], "three members, two distinct");
    }

    /// A sketch's `.sum` is exact (`DdSketch::sum`), so it is emitted rather than omitted the way
    /// `prometheus_out`'s summary path omits `_sum`.
    #[test]
    fn a_sketch_emits_an_exact_sum() {
        let (lines, _, _, _) = encode_with(
            &batch(vec![event(MetricKind::Distribution(sketch(&[1.0, 2.0, 3.0])))]),
            |e| e.with_multi_value(MultiValue::Expand),
        );
        assert!(lines.contains(&"sys.cpu.sum 6 1700000000".to_string()), "{lines:?}");
        assert!(lines.contains(&"sys.cpu.count 3 1700000000".to_string()), "{lines:?}");
    }

    /// The injectivity claim from `mod.rs`, pinned on the five bounds most likely to collide under
    /// a rounding scheme.
    #[test]
    fn bucket_tokens_are_injective_over_distinct_bounds() {
        let histogram = Histogram {
            buckets: vec![
                (0.5, 1),
                (5.0, 1),
                (0.05, 1),
                (-0.5, 1),
                (f64::INFINITY, 1),
                (f64::NEG_INFINITY, 1),
            ],
            temporality: Temporality::Delta,
            sum: None,
            min: None,
            max: None,
        };
        let (lines, _, _, _) =
            encode_with(&batch(vec![event(MetricKind::Histogram(histogram))]), |e| {
                e.with_multi_value(MultiValue::Expand)
            });
        let mut paths: Vec<&str> = lines
            .iter()
            .map(|line| line.split(' ').next().unwrap())
            .filter(|path| path.contains(".bucket_"))
            .collect();
        assert_eq!(
            paths,
            vec![
                "sys.cpu.bucket_0_5",
                "sys.cpu.bucket_5",
                "sys.cpu.bucket_0_05",
                "sys.cpu.bucket_-0_5",
                "sys.cpu.bucket_inf",
                "sys.cpu.bucket_-inf",
            ]
        );
        let before = paths.len();
        paths.sort_unstable();
        paths.dedup();
        assert_eq!(paths.len(), before, "distinct bounds must render to distinct tokens");
    }

    #[test]
    fn an_expanded_sub_path_carries_the_events_tags() {
        let mut attributes = AttrMap::new();
        attributes.insert("env", "prod");
        let ev = Event::metric(
            TS,
            attributes,
            MetricRecord::new(intern("sys.cpu"), MetricKind::Set(hyper_log_log())),
        );
        let (lines, _, _, _) =
            encode_with(&batch(vec![ev]), |e| e.with_multi_value(MultiValue::Expand));
        assert_eq!(lines, vec!["sys.cpu.count;env=prod 2 1700000000"]);
    }

    // -- framing ------------------------------------------------------------------------------------

    #[test]
    fn a_line_over_max_packet_bytes_is_dropped_whole() {
        let ev = Event::metric(
            TS,
            AttrMap::new(),
            MetricRecord::new(intern("a.very.long.metric.name.indeed"), MetricKind::Gauge(1.0)),
        );
        let (lines, _, stats, registry) =
            encode_with(&batch(vec![ev]), |e| e.with_max_packet_bytes(16));
        assert!(lines.is_empty(), "never split -- half a line is a corrupt series");
        assert_eq!(stats.dropped_oversize_line, 1);
        assert_eq!(
            counted(&registry, "logit.output.metrics.skipped", ("reason", "oversize_line")),
            1.0
        );
    }

    #[test]
    fn encode_into_clears_its_output_first() {
        let mut encoder = GraphiteEncoder::new();
        let mut out = MessageBuf::default();
        out.push_with(b"stale", 9);
        encoder.encode_into(&batch(vec![event(MetricKind::Gauge(1.0))]), &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out.iter().next().unwrap(), b"sys.cpu 1 1700000000");
    }

    // -- pickle -------------------------------------------------------------------------------------

    fn decode_frames(messages: &[Vec<u8>]) -> Vec<Vec<(String, f64, f64)>> {
        messages
            .iter()
            .map(|raw| {
                let declared = u32::from_be_bytes(raw[..4].try_into().unwrap()) as usize;
                assert_eq!(declared, raw.len() - 4, "the prefix must describe the payload");
                let mut reader = pickle::PickleReader::new();
                let mut points = Vec::new();
                reader
                    .read_datapoints(&raw[4..], |path, ts, value| {
                        points.push((path.to_string(), ts, value));
                    })
                    .expect("our own frames must read back");
                points
            })
            .collect()
    }

    /// A pickle entry is a complete, already length-prefixed frame, so a sink's send path is one
    /// `write_all` per entry -- and its meta is the frame's datapoint count, not `1`.
    #[test]
    fn a_pickle_entry_is_one_prefixed_frame_whose_meta_is_its_datapoint_count() {
        let events = (0..3)
            .map(|i| {
                Event::metric(
                    TS,
                    AttrMap::new(),
                    MetricRecord::new(intern("sys.cpu"), MetricKind::Gauge(i as f64)),
                )
            })
            .collect();
        let (messages, metas, stats, _) =
            encode_raw_with(&batch(events), |e| e.with_protocol(Protocol::Pickle));
        assert_eq!(messages.len(), 1);
        assert_eq!(metas, vec![3]);
        assert_eq!(stats.datapoints, 3);
        assert_eq!(
            decode_frames(&messages)[0],
            vec![
                ("sys.cpu".to_string(), SECONDS as f64, 0.0),
                ("sys.cpu".to_string(), SECONDS as f64, 1.0),
                ("sys.cpu".to_string(), SECONDS as f64, 2.0),
            ]
        );
    }

    #[test]
    fn pickle_opens_a_new_frame_when_the_next_datapoint_would_exceed_the_cap() {
        let events: Vec<Event> = (0..20)
            .map(|i| {
                Event::metric(
                    TS,
                    AttrMap::new(),
                    MetricRecord::new(intern("sys.cpu"), MetricKind::Gauge(i as f64)),
                )
            })
            .collect();
        let (messages, metas, stats, _) = encode_raw_with(&batch(events), |e| {
            e.with_protocol(Protocol::Pickle).with_max_frame_bytes(64)
        });
        assert!(messages.len() > 1, "a 64-byte cap must split 20 datapoints across frames");
        assert_eq!(metas.iter().sum::<usize>(), 20);
        assert_eq!(stats.datapoints, 20);
        for message in &messages {
            assert!(
                message.len() - 4 <= 64,
                "every frame's payload must fit the cap, got {}",
                message.len() - 4
            );
        }
        let decoded: Vec<(String, f64, f64)> =
            decode_frames(&messages).into_iter().flatten().collect();
        assert_eq!(decoded.len(), 20);
    }

    #[test]
    fn a_pickle_datapoint_that_cannot_fit_an_empty_frame_is_dropped_whole() {
        let ev = Event::metric(
            TS,
            AttrMap::new(),
            MetricRecord::new(intern("a.very.long.metric.name.indeed"), MetricKind::Gauge(1.0)),
        );
        let (messages, _, stats, registry) = encode_raw_with(&batch(vec![ev]), |e| {
            e.with_protocol(Protocol::Pickle).with_max_frame_bytes(16)
        });
        assert!(messages.is_empty());
        assert_eq!(stats.dropped_oversize_datapoint, 1);
        assert_eq!(
            counted(&registry, "logit.output.metrics.skipped", ("reason", "oversize_datapoint")),
            1.0
        );
    }

    #[test]
    fn a_pickle_datapoint_carries_the_same_tagged_path_plaintext_would() {
        let ev = tagged(&[("env", Value::from("prod")), ("host", Value::from("web-1"))]);
        let plaintext = encode(&batch(vec![ev.clone()]));
        let (messages, _, _, _) =
            encode_raw_with(&batch(vec![ev]), |e| e.with_protocol(Protocol::Pickle));
        let decoded = decode_frames(&messages);
        assert_eq!(decoded[0][0].0, plaintext[0].split(' ').next().unwrap());
        assert_eq!(decoded[0][0].0, "sys.cpu;env=prod;host=web-1");
    }

    /// The reusable-buffer contract: a second batch of the same shape must not grow any of the
    /// encoder's scratch buffers.
    #[test]
    fn a_warm_encoder_reuses_its_buffers() {
        let ev = tagged(&[("env", Value::from("prod")), ("host", Value::from("web-1"))]);
        let events: Vec<Event> = std::iter::repeat_n(ev, 32).collect();
        let batch = batch(events);
        let mut encoder = GraphiteEncoder::new();
        let mut out = MessageBuf::default();
        encoder.encode_into(&batch, &mut out);
        let caps = (
            encoder.tag_suffix.capacity(),
            encoder.tag_text.capacity(),
            encoder.tag_slots.capacity(),
            encoder.path.capacity(),
            encoder.full_path.capacity(),
            encoder.line.capacity(),
        );
        encoder.encode_into(&batch, &mut out);
        assert_eq!(
            (
                encoder.tag_suffix.capacity(),
                encoder.tag_text.capacity(),
                encoder.tag_slots.capacity(),
                encoder.path.capacity(),
                encoder.full_path.capacity(),
                encoder.line.capacity(),
            ),
            caps,
            "a warm encode must not reallocate"
        );
    }

    /// `MetricList` order is wire order -- a batch's datapoints must not be reshuffled within one
    /// event.
    #[test]
    fn records_are_written_in_metric_list_order() {
        let mut ev = event(MetricKind::Gauge(1.0));
        ev.metrics = MetricList::from_iter([
            MetricRecord::new(intern("z.last"), MetricKind::Gauge(1.0)),
            MetricRecord::new(intern("a.first"), MetricKind::Gauge(2.0)),
        ]);
        assert_eq!(encode(&batch(vec![ev])), vec!["z.last 1 1700000000", "a.first 2 1700000000"]);
    }
}
