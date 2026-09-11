//! statsd / DogStatsD egress over UDP or TCP -- the mirror of `logit_inputs::statsd`, and a real
//! relay: names, values, and tags round-trip through the real `StatsdDecoder` (pinned by this
//! module's own tests), not just through hand-checked example lines.
//!
//! Split the way `syslog.rs`/`influxdb.rs` are: a pure [`StatsdEncoder`] (no socket anywhere,
//! every grammar/sanitization/packing test runs against it directly) plus the thin
//! [`StatsdOutput`] that owns the socket.
//!
//! **This does not implement `logit_proto::Encoder`.** That trait is `fn encode(&mut self,
//! &EventBatch) -> Result<Bytes, CodecError>` -- one opaque buffer per batch, with no framing
//! metadata -- and this sink genuinely needs per-message boundaries (one line per metric, packed
//! into datagrams up to a size cap on UDP). [`crate::syslog::SyslogEncoder`] is the in-tree
//! precedent for a sink whose encoder sidesteps the trait for the same class of reason. See
//! `docs/known-gaps.md` for this recorded as an open gap in `logit_proto::Encoder`'s shape, not a
//! defect in this module.
//!
//! ## Grammar and round-trip contract
//!
//! `<name>:<value>|<type>[|#<tag>[:<value>],...]` -- the same grammar `logit_inputs::statsd`
//! parses, minus the two segments this sink never emits (see "No sample rate, no timestamp"
//! below). Every sanitization rule exists because of a specific way `StatsdDecoder::parse_line`
//! would otherwise misparse the result; see that module's grammar doc comment for the decoder side
//! of each rule cited here.
//!
//! ## Dialects
//!
//! `Format::DogStatsd` (default) emits the `|#k:v,k:v` tag segment; `Format::Statsd` omits it
//! entirely -- not an empty `|#`, which some plain-statsd receivers reject outright -- and counts
//! every tag it drops (`EncodeStats::tags_dropped_dialect`).
//!
//! ## Sanitization
//!
//! A metric name has every one of `: | @ # , \n \r \0`, ASCII control characters, and whitespace
//! replaced with `_` (substitution, not deletion, so distinct names stay distinct -- following
//! `syslog.rs::sanitize_5424_field`'s approach). Each forbidden character earns its place against
//! the decoder's own grammar: `:` splits name from values, `|` splits segments, `@`/`#` open the
//! sample-rate/tag segments, `,` separates tags, `\n` separates lines; whitespace is stripped
//! because the decoder trims every line before parsing it.
//!
//! Tag *keys* forbid the same set as a name, plus nothing extra -- **and forbid `:`** for a
//! different reason than the name does: `parse_line` splits a tag on its *first* colon
//! (`tag.split_once(':')`), so a `:` inside a key would silently reparse as a shorter key with the
//! remainder folded into the value. Tag *values* forbid the same set **except `:`, which is
//! deliberately allowed**: since only the first colon is significant, `env:a:b` round-trips as key
//! `env`, value `a:b` -- an asymmetry between key and value sanitization that is easy to get
//! backwards, so it has its own test.
//!
//! ## Metric-kind coverage and the v1 deferral
//!
//! Only `Counter` (`|c`) and `Gauge`/`GaugeDelta` (`|g`) are encoded. `Distribution`, `Set`,
//! `Histogram`, and `Summary` are dropped with a clear "not implemented yet" message
//! (`EncodeStats::dropped_unsupported_kind`) -- recorded in `docs/known-gaps.md`.
//!
//! **This means a `statsd_in -> aggregate -> statsd_out` relay drops every timer metric today.**
//! `ms`/`h`/`d` on the wire all decode to `MetricKind::Distribution`
//! (`logit_inputs::statsd::build_event`), so the single most common statsd workload -- timers --
//! makes it through the input and the aggregator and then dies at this sink, loudly counted but
//! dropped. Adding it later is a localized change to one `match` arm in [`render_metric`] plus its
//! stats field; the real design question it defers -- how a merged `DdSketch`, which no longer
//! holds the original samples, should become one or more statsd lines -- deserves its own ADR
//! rather than a guess made in passing here.
//!
//! ## Relative gauges
//!
//! statsd is the one protocol that natively expresses a *relative* gauge adjustment
//! (`name:+5|g`/`name:-5|g`) -- exactly [`logit_core::MetricKind::GaugeDelta`]'s wire origin. By
//! default a `GaugeDelta` reaching this sink is dropped with the same message
//! `influxdb_out` uses (`gauge_delta_unresolved`): it means the pipeline is missing an `aggregate`
//! component, not that this metric is malformed. Setting `relative_gauges: true` opts into
//! encoding it natively instead -- the one sink that *can* round-trip a delta losslessly, since
//! every other sink's wire format has no such concept at all.
//!
//! A positive delta needs an explicit `+`: `write!("{}", 5.0)` yields `"5"`, which the decoder
//! reads back as an *absolute* `Gauge`, not a delta -- silently corrupting the round-trip this
//! sink exists to preserve.
//!
//! ## Negative absolute gauges
//!
//! The statsd/DogStatsD grammar has no wire syntax for setting a gauge to a negative absolute
//! value at all (`logit_inputs::statsd::build_event`'s `"g"` arm reads *any* leading `-` as a
//! delta) -- so a naive `Gauge(-5.0)` would render as `name:-5|g` and decode back as
//! `GaugeDelta(-5.0)`, a silent semantic corruption. This sink instead emits the idiom both Etsy
//! statsd and DogStatsD document for exactly this case: `name:0|g` immediately followed by
//! `name:-5|g`. The two lines are pushed into the [`MessageBuf`] as **one indivisible entry**
//! (joined by an embedded `\n`) so the packer can never split them across two datagrams -- a lost
//! first datagram would otherwise apply `-5` to whatever stale value the gauge already held at the
//! receiver. This is the only place an entry contains a newline; every sanitizer above exists
//! precisely to guarantee nothing else ever does.
//!
//! `Gauge(-0.0)` is deliberately *not* a pair: it is numerically zero, so its sign is normalized
//! away and it renders as the plain `name:0|g` -- the naive `name:-0|g` would decode as a no-op
//! `GaugeDelta`, since the decoder dispatches on the leading `-` without parsing the value.
//!
//! ## Packing and framing
//!
//! UDP **packs** several lines into one datagram, up to `max_packet_bytes`
//! (`\n`-joined, no trailing `\n`, since the datagram boundary itself ends the last line). This is
//! a deliberate divergence from `syslog_out`, which refuses to pack because packing there would
//! depend on the receiver splitting on a delimiter its whole "injection safety" section exists to
//! avoid relying on. The reasoning inverts here: splitting on `\n` **is** the statsd grammar
//! (every statsd client packs a buffered send this way, and `StatsdDecoder::decode_into` splits on
//! it directly), and the sanitizers above make an embedded `\n` unrepresentable in a name, key, or
//! value -- so a packed datagram cannot forge an extra metric the way a packed syslog datagram
//! could forge an extra log line. A line that would overflow the cap starts a new datagram rather
//! than being split; a single line longer than the cap is **dropped whole**, never truncated
//! (unlike `syslog_out`) -- a truncated statsd line decodes as a different metric or a parse
//! error, never a shorter version of the same one.
//!
//! TCP terminates **every** line with `\n`, including the last -- a stream has no per-batch EOF,
//! so without a trailing separator the last line of one batch would glue onto the first line of
//! the next. No octet-counting: statsd has no such framing convention and no receiver auto-detects
//! one, unlike syslog's `go-syslog`.
//!
//! ## No sample rate, no timestamp, no unit
//!
//! Never `@<rate>`: `logit_inputs::statsd` already extrapolated at decode time (a `Counter`'s
//! value already has the sample rate divided out; a `Distribution`'s samples are already
//! replicated to the extrapolated weight), so emitting `@1` would be a no-op at best and anything
//! else would double-extrapolate downstream. Never `|T<ts>`: the classic grammar has no timestamp
//! segment at all, and `logit_inputs::statsd::parse_line` would silently ignore one if emitted, so
//! it wouldn't even round-trip through this repo's own input -- a receiver stamps with its own
//! receipt time instead (`docs/known-gaps.md`). `MetricRecord::unit` has no statsd wire
//! representation either and is dropped the same way.

use crate::influxdb::{push_float, tag_value};
use crate::msgbuf::MessageBuf;
use crate::Output;
use anyhow::Context;
use logit_core::{
    Diagnostics, Event, EventBatch, MetricKind, MetricRecord, Resource, Telemetry, Temporality,
    Value,
};
use logit_pipeline::Fault;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{lookup_host, TcpStream, UdpSocket};

/// Etsy statsd's own "commodity Ethernet LAN" recommendation, and also DataDog's documented
/// DogStatsD client default -- 1500 MTU minus IPv4/UDP headers minus ~40 bytes of headroom for
/// VXLAN/IPsec encapsulation, exactly the case where a 1472-byte datagram would silently fragment
/// or `EMSGSIZE`. Unlike DataDog's loopback/UDS figure (8192, matching `syslog_out`'s
/// `DEFAULT_MAX_MESSAGE_BYTES`) this doesn't assume the destination is local. Bounds one
/// **datagram** (several packed lines), not a single line.
pub const DEFAULT_MAX_PACKET_BYTES: usize = 1432;

/// TCP only -- mirrors `syslog::DEFAULT_CONNECT_TIMEOUT`'s reasoning and value exactly: `logit-
/// config`'s own `default_statsd_connect_timeout` hardcodes the same 5 seconds, kept in sync by
/// hand.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Which statsd dialect [`StatsdEncoder`] emits. Deliberately its own tiny enum rather than
/// `logit_config::StatsdFormat` -- `logit-outputs` never depends on `logit-config`
/// (`docs/design/pipeline-graph.md`'s crate layout); `logit-cli::pipeline::build_spec` is the sole
/// place a config value crosses into this type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    DogStatsd,
    Statsd,
}

/// Per-batch outcome counts from [`StatsdEncoder::encode_into`] -- what `StatsdOutput::send` turns
/// into `logit.output.*` telemetry (`docs/design/internal-telemetry.md`).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EncodeStats {
    /// Events with no metrics (a log-only or span-only event, legal under
    /// `docs/adr/multi-payload-events.md`) -- the same "nothing to render" skip `influxdb_out`
    /// makes for the same shape of event.
    pub skipped_no_metrics: usize,
    pub dropped_gauge_delta: usize,
    /// A `NO_RECORDED_VALUE`-flagged point -- statsd has no wire concept of "no value here," so
    /// unlike `otlp_out` this sink can't keep the point flagged; it drops it rather than write its
    /// default value as a fabricated real sample (`docs/adr/lossless-transit.md`,
    /// `docs/known-gaps.md`'s cross-protocol table).
    pub dropped_no_recorded_value: usize,
    pub dropped_unsupported_kind: usize,
    pub dropped_unencodable_value: usize,
    pub dropped_empty_name: usize,
    pub dropped_oversize_line: usize,
    pub tags_dropped_dialect: usize,
    pub tags_dropped_unrepresentable: usize,
}

/// Encodes events as statsd lines. Pure -- no socket anywhere -- so every grammar/sanitization/
/// packing test runs directly against this, with no transport of any kind involved.
pub struct StatsdEncoder {
    format: Format,
    relative_gauges: bool,
    diag: Diagnostics,
    /// The `|#k:v,k:v` tag segment for the event currently being encoded -- built once per event,
    /// shared across that event's metrics (`influxdb.rs::render_tag_suffix`'s same split).
    tag_suffix: String,
    /// One rendered line (or, for a negative absolute gauge, the two-line pair joined by `\n`).
    /// Cleared per metric, never reallocated -- `syslog.rs::SyslogEncoder::line`'s discipline.
    line: String,
    /// The sanitized metric name for the metric currently being encoded. Its own field, not a
    /// local in `render_metric`, for the same reason `line` is -- a function-local `String::new()`
    /// would reallocate on every single metric.
    name: String,
    /// Scratch for [`tag_value`]'s non-`Str` formatting only -- every use within one event is
    /// read-immediately-into-`tag_suffix`-then-cleared before the next, never overlapping in time.
    scratch: String,
}

impl StatsdEncoder {
    pub fn new(format: Format) -> Self {
        Self {
            format,
            relative_gauges: false,
            diag: Diagnostics::default(),
            tag_suffix: String::new(),
            line: String::new(),
            name: String::new(),
            scratch: String::new(),
        }
    }

    pub fn with_relative_gauges(mut self, relative_gauges: bool) -> Self {
        self.relative_gauges = relative_gauges;
        self
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    /// Encodes every event in `batch` into `out` (cleared first). Never fails -- a per-metric
    /// problem (an unsupported kind, an unresolved delta, a non-finite value, an oversize line) is
    /// a drop counted in the returned [`EncodeStats`], not an error; there is nothing for a caller
    /// to react to beyond what the stats already report. `max_packet_bytes` bounds a UDP
    /// datagram's worth of packed lines; pass `usize::MAX` for TCP, which has no such cap (the
    /// per-line oversize drop still applies).
    pub fn encode_into(
        &mut self,
        batch: &EventBatch,
        max_packet_bytes: usize,
        out: &mut MessageBuf,
    ) -> EncodeStats {
        out.clear();
        let mut stats = EncodeStats::default();
        for event in &batch.events {
            if event.metrics.is_empty() {
                stats.skipped_no_metrics += 1;
                continue;
            }
            build_tag_suffix(
                &mut self.tag_suffix,
                &mut self.scratch,
                self.format,
                &batch.resource,
                event,
                &mut stats,
            );
            for metric in &event.metrics {
                self.line.clear();
                if !render_metric(
                    &mut self.line,
                    &mut self.name,
                    &self.tag_suffix,
                    self.relative_gauges,
                    metric,
                    &mut stats,
                    &mut self.diag,
                ) {
                    continue;
                }
                if self.line.len() > max_packet_bytes {
                    stats.dropped_oversize_line += 1;
                    self.diag.warn_throttled(
                        "oversize_line",
                        format_args!(
                            "statsd_out: a single metric line exceeds max_packet_bytes ({}); \
                             dropping it whole rather than truncating",
                            max_packet_bytes
                        ),
                    );
                    continue;
                }
                out.push(&self.line);
            }
        }
        stats
    }
}

/// Builds this event's DogStatsD tag segment into `suffix` (cleared first, **no** leading `|#` --
/// [`render_metric`]/[`append_tags`] add that only if `suffix` ends up non-empty). Resource
/// attributes first, event attributes overriding on key collision, merge-joined via
/// [`crate::attrs::merged`]. Under [`Format::Statsd`] this always leaves `suffix` empty and counts
/// every attribute that would otherwise have become a tag into `stats.tags_dropped_dialect`.
fn build_tag_suffix(
    suffix: &mut String,
    scratch: &mut String,
    format: Format,
    resource: &Resource,
    event: &Event,
    stats: &mut EncodeStats,
) {
    suffix.clear();
    for (key, value) in crate::attrs::merged(resource, event) {
        if format == Format::Statsd {
            stats.tags_dropped_dialect += 1;
            continue;
        }

        let key_str = logit_core::interner::resolve(key);
        if key_str.is_empty() {
            stats.tags_dropped_unrepresentable += 1;
            continue;
        }
        // `Bool(true)` is DogStatsD's own bare-tag idiom (`#urgent`, no `:value`) -- exactly what
        // `logit_inputs::statsd::parse_line` produces for a valueless tag. Emitting `key:true`
        // instead would round-trip as `Value::Str("true")`, silently changing the value's type.
        let bare = matches!(value, Value::Bool(true));
        let rendered_value = if bare { None } else { tag_value(scratch, value) };
        if !bare && rendered_value.is_none() {
            stats.tags_dropped_unrepresentable += 1;
            continue;
        }

        if !suffix.is_empty() {
            suffix.push(',');
        }
        let key_start = suffix.len();
        sanitize_into(suffix, key_str, is_forbidden_in_tag_key);
        if suffix.len() == key_start {
            // Sanitized to nothing (only possible if `key_str` itself was empty, already handled
            // above, but kept as a defensive no-op-key guard) -- undo any separator just pushed.
            if suffix.ends_with(',') {
                suffix.pop();
            }
            stats.tags_dropped_unrepresentable += 1;
            continue;
        }
        if let Some(v) = rendered_value {
            suffix.push(':');
            sanitize_into(suffix, v, is_forbidden_in_tag_value_only);
        }
    }
}

/// Appends `line`'s `|#`-prefixed tag segment, if `tag_suffix` is non-empty. Shared by every
/// metric-kind arm in [`render_metric`], including both lines of a negative-gauge pair.
fn append_tags(line: &mut String, tag_suffix: &str) {
    if !tag_suffix.is_empty() {
        line.push_str("|#");
        line.push_str(tag_suffix);
    }
}

/// Encodes one metric into `line` (already cleared by the caller). `name` is a reused scratch
/// buffer (cleared here), not a local -- a fresh `String::new()` per metric would reallocate on
/// every single call, the same reasoning `line`/`tag_suffix`/`scratch` are struct fields for.
/// Returns whether it produced a line at all (a dropped metric returns `false` having already
/// recorded why in `stats`).
fn render_metric(
    line: &mut String,
    name: &mut String,
    tag_suffix: &str,
    relative_gauges: bool,
    metric: &MetricRecord,
    stats: &mut EncodeStats,
    diag: &mut Diagnostics,
) -> bool {
    name.clear();
    sanitize_into(name, logit_core::interner::resolve(metric.name), is_forbidden_in_name);
    if name.is_empty() {
        stats.dropped_empty_name += 1;
        diag.warn_throttled(
            "empty_metric_name",
            format_args!(
                "statsd_out: metric name {:?} sanitizes to nothing; dropping",
                logit_core::interner::resolve(metric.name)
            ),
        );
        return false;
    }

    if metric.is_no_recorded_value() {
        stats.dropped_no_recorded_value += 1;
        diag.warn_throttled(
            "no_recorded_value",
            format_args!(
                "statsd_out: metric {name:?} has no recorded value (OTLP NO_RECORDED_VALUE); \
                 dropping"
            ),
        );
        return false;
    }

    match &metric.kind {
        // A delta, monotonic `Sum` is what `MetricKind::Counter` used to mean -- encodes exactly
        // as it did, `name:v|c`. Any other `Sum` (cumulative, or non-monotonic) has no `|c`
        // meaning statsd can represent and falls through to the unsupported-kind arm below --
        // real encoding support is W3's (`docs/plans/lossless-transit.md`).
        MetricKind::Sum(s) if s.temporality == Temporality::Delta && s.monotonic => {
            if !s.value.is_finite() {
                stats.dropped_unencodable_value += 1;
                diag.warn_throttled(
                    "unencodable_value",
                    format_args!("statsd_out: non-finite counter value on {name:?}; dropping"),
                );
                return false;
            }
            line.push_str(name);
            line.push(':');
            push_float(line, s.value);
            line.push_str("|c");
            append_tags(line, tag_suffix);
            true
        }
        MetricKind::Sum(s) => {
            let kind_name = if s.temporality == Temporality::Cumulative {
                "cumulative Sum"
            } else {
                "non-monotonic Sum"
            };
            dropped_unsupported_kind(stats, diag, name, kind_name)
        }
        MetricKind::Gauge(v) => {
            if !v.is_finite() {
                stats.dropped_unencodable_value += 1;
                diag.warn_throttled(
                    "unencodable_value",
                    format_args!("statsd_out: non-finite gauge value on {name:?}; dropping"),
                );
                return false;
            }
            // `-0.0` is numerically zero, and `0` *is* representable as an absolute gauge -- but
            // `f64`'s `Display` renders it `"-0"`, and `StatsdDecoder::build_event`'s `"g"` arm
            // decides `Gauge` vs `GaugeDelta` on the leading `-` alone, without parsing the float
            // first, so the naive rendering would decode back as a no-op `GaugeDelta(-0.0)`
            // instead of an absolute reset to zero. Normalizing the sign away here emits the
            // plain `name:0|g` that says exactly that, and keeps the two-line idiom below for
            // values that really are negative. `stdio_out`'s
            // `gauge_delta_negative_zero_does_not_double_the_sign` is the same
            // `Display`-of-negative-zero trap in that sink.
            let v = if *v == 0.0 { 0.0 } else { *v };
            if v.is_sign_negative() {
                // No wire syntax for a negative absolute gauge -- emit the documented two-line
                // idiom as one indivisible entry (module doc's "Negative absolute gauges").
                write_gauge_line(line, name, 0.0, tag_suffix);
                line.push('\n');
                write_gauge_line(line, name, v, tag_suffix);
            } else {
                write_gauge_line(line, name, v, tag_suffix);
            }
            true
        }
        MetricKind::GaugeDelta(v) => {
            if !relative_gauges {
                stats.dropped_gauge_delta += 1;
                diag.warn_throttled(
                    "gauge_delta_unresolved",
                    "a relative gauge adjustment reached a sink unresolved -- add an `aggregate` \
                     component between the statsd input and this output",
                );
                return false;
            }
            if !v.is_finite() {
                stats.dropped_unencodable_value += 1;
                diag.warn_throttled(
                    "unencodable_value",
                    format_args!("statsd_out: non-finite gauge delta on {name:?}; dropping"),
                );
                return false;
            }
            line.push_str(name);
            line.push(':');
            if v.is_sign_positive() {
                line.push('+');
            }
            push_float(line, *v);
            line.push_str("|g");
            append_tags(line, tag_suffix);
            true
        }
        MetricKind::Distribution(_) => dropped_unsupported_kind(stats, diag, name, "Distribution"),
        MetricKind::Set(_) => dropped_unsupported_kind(stats, diag, name, "Set"),
        MetricKind::Histogram(_) => dropped_unsupported_kind(stats, diag, name, "Histogram"),
        MetricKind::Summary(_) => dropped_unsupported_kind(stats, diag, name, "Summary"),
        // Raw, unsummarized data (statsd's own `ms`/`h`/`d`/`s` shapes, decoded losslessly by
        // `statsd_in` -- `docs/plans/lossless-transit.md`) -- W3 owns real `|ms`/`|h`/`|d`/`|s`
        // encoding for these; W1 only has to keep them from panicking.
        MetricKind::Samples(_) => dropped_unsupported_kind(stats, diag, name, "Samples"),
        MetricKind::SetMembers(_) => dropped_unsupported_kind(stats, diag, name, "SetMembers"),
        MetricKind::ExponentialHistogram(_) => {
            dropped_unsupported_kind(stats, diag, name, "ExponentialHistogram")
        }
    }
}

/// Shared by every `MetricKind` arm `render_metric` can't encode -- counts the drop and logs a
/// throttled warning naming exactly which kind was unencodable, then returns `false` the same way
/// every other early-return drop path in `render_metric` does. Extracted so the match above can
/// stay one arm per variant (fully exhaustive, no wildcard) without repeating these three lines
/// per arm -- the exhaustiveness itself is the point: a future `MetricKind` variant is a compile
/// error here, not a silent `unreachable!` panic at runtime.
fn dropped_unsupported_kind(
    stats: &mut EncodeStats,
    diag: &mut Diagnostics,
    name: &str,
    kind_name: &str,
) -> bool {
    stats.dropped_unsupported_kind += 1;
    diag.warn_throttled(
        "unsupported_metric_kind",
        format_args!(
            "statsd_out: {kind_name} metrics are not implemented yet (metric {name:?}); dropping"
        ),
    );
    false
}

fn write_gauge_line(line: &mut String, name: &str, v: f64, tag_suffix: &str) {
    line.push_str(name);
    line.push(':');
    push_float(line, v);
    line.push_str("|g");
    append_tags(line, tag_suffix);
}

/// Appends `s` to `out` (does **not** clear it first -- callers that want a fresh buffer clear
/// explicitly), replacing every character `forbidden` rejects with `_`. Substitution, not
/// deletion, so distinct inputs stay distinct.
fn sanitize_into(out: &mut String, s: &str, forbidden: impl Fn(char) -> bool) {
    for c in s.chars() {
        out.push(if forbidden(c) { '_' } else { c });
    }
}

/// Forbidden in a metric name -- see the module doc's "Sanitization" section for why each
/// character earns its place. Also the tag-*key* rule: `:` is forbidden there too, because
/// `parse_line` splits a tag on its first colon, so a `:` in a key would silently reparse as a
/// shorter key with the remainder folded into the value.
fn is_forbidden_in_name(c: char) -> bool {
    matches!(c, ':' | '|' | '@' | '#' | ',' | '\n' | '\r' | '\0')
        || c.is_control()
        || c.is_whitespace()
}

/// Alias for [`is_forbidden_in_name`], named for its other use site (tag keys) -- see that
/// function's doc comment.
fn is_forbidden_in_tag_key(c: char) -> bool {
    is_forbidden_in_name(c)
}

/// Tag *values* forbid the same set as a name **except `:`**, which is deliberately preserved:
/// since `parse_line` only looks at the first colon, `env:a:b` round-trips as key `env`, value
/// `a:b`.
fn is_forbidden_in_tag_value_only(c: char) -> bool {
    c != ':' && is_forbidden_in_name(c)
}

/// The live half of a `statsd_out` sink: `Udp` binds eagerly (a bad local bind is a config error);
/// `Tcp` connects lazily inside `send`, since a not-yet-up downstream receiver must not block
/// `logit` from starting. Mirrors `syslog::Conn` exactly.
enum Conn {
    Udp(UdpSocket),
    Tcp { stream: Option<TcpStream>, connect_timeout: Duration },
}

/// `logit_pipeline::Output` for `statsd_out`. Built via [`StatsdOutput::udp`] or
/// [`StatsdOutput::tcp`] -- never a bare constructor, mirroring `SyslogOutput`.
pub struct StatsdOutput {
    endpoint: String,
    conn: Conn,
    encoder: StatsdEncoder,
    max_packet_bytes: usize,
    lines: MessageBuf,
    /// Reused across `send` calls: the packed UDP datagram, or the whole TCP frame.
    packet_buf: Vec<u8>,
    diag: Diagnostics,
    telemetry: Telemetry,
}

impl StatsdOutput {
    /// Binds an ephemeral local UDP socket eagerly -- see `SyslogOutput::udp`'s doc comment for
    /// why `endpoint` itself is resolved per `send`, not here.
    pub fn udp(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        let socket = std::net::UdpSocket::bind("0.0.0.0:0")
            .context("binding statsd_out's local UDP socket")?;
        socket.set_nonblocking(true).context("configuring statsd_out's UDP socket")?;
        let socket = UdpSocket::from_std(socket).context("registering statsd_out's UDP socket")?;
        Ok(Self::new(endpoint, Conn::Udp(socket)))
    }

    /// Never connects here -- see [`Conn`]'s doc comment.
    pub fn tcp(endpoint: impl Into<String>, connect_timeout: Duration) -> Self {
        Self::new(endpoint, Conn::Tcp { stream: None, connect_timeout })
    }

    fn new(endpoint: impl Into<String>, conn: Conn) -> Self {
        Self {
            endpoint: endpoint.into(),
            conn,
            encoder: StatsdEncoder::new(Format::DogStatsd),
            max_packet_bytes: DEFAULT_MAX_PACKET_BYTES,
            lines: MessageBuf::default(),
            packet_buf: Vec::new(),
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
        }
    }

    pub fn with_encoder(mut self, encoder: StatsdEncoder) -> Self {
        self.encoder = encoder;
        self
    }

    pub fn with_max_packet_bytes(mut self, max_packet_bytes: usize) -> Self {
        self.max_packet_bytes = max_packet_bytes;
        self
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag.clone();
        self.encoder = self.encoder.with_diagnostics(diag);
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

#[async_trait::async_trait]
impl Output for StatsdOutput {
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        // TCP has no datagram to overflow -- only the per-line oversize drop applies there.
        let cap =
            if matches!(self.conn, Conn::Udp(_)) { self.max_packet_bytes } else { usize::MAX };
        let stats = self.encoder.encode_into(batch, cap, &mut self.lines);
        self.telemetry.count("logit.output.events.skipped", stats.skipped_no_metrics as f64, &[]);
        self.telemetry.count(
            "logit.output.messages.dropped",
            stats.dropped_gauge_delta as f64,
            &[("reason", "unresolved_gauge_delta")],
        );
        self.telemetry.count(
            "logit.output.messages.dropped",
            stats.dropped_unsupported_kind as f64,
            &[("reason", "unsupported_kind")],
        );
        self.telemetry.count(
            "logit.output.messages.dropped",
            stats.dropped_no_recorded_value as f64,
            &[("reason", "no_recorded_value")],
        );
        self.telemetry.count(
            "logit.output.messages.dropped",
            stats.dropped_unencodable_value as f64,
            &[("reason", "unencodable_value")],
        );
        self.telemetry.count(
            "logit.output.messages.dropped",
            stats.dropped_empty_name as f64,
            &[("reason", "empty_name")],
        );
        self.telemetry.count(
            "logit.output.messages.dropped",
            stats.dropped_oversize_line as f64,
            &[("reason", "oversize_line")],
        );
        self.telemetry.count(
            "logit.output.tags.dropped",
            stats.tags_dropped_dialect as f64,
            &[("reason", "dialect")],
        );
        self.telemetry.count(
            "logit.output.tags.dropped",
            stats.tags_dropped_unrepresentable as f64,
            &[("reason", "unrepresentable")],
        );

        if self.lines.is_empty() {
            return Ok(());
        }

        self.telemetry.count("logit.output.batch.bytes", self.lines.total_bytes() as f64, &[]);
        let request_timer = self.telemetry.timer("logit.output.request.duration");
        let result = match &mut self.conn {
            Conn::Udp(socket) => {
                Self::send_udp(
                    socket,
                    &self.endpoint,
                    &self.lines,
                    self.max_packet_bytes,
                    &mut self.packet_buf,
                    &mut self.diag,
                    &self.telemetry,
                )
                .await
            }
            Conn::Tcp { stream, connect_timeout } => {
                Self::send_tcp(
                    stream,
                    &self.endpoint,
                    *connect_timeout,
                    &self.lines,
                    &mut self.packet_buf,
                )
                .await
            }
        };
        drop(request_timer);

        match &result {
            Ok((messages, datagrams)) => {
                self.telemetry.count("logit.output.messages", *messages as f64, &[]);
                if matches!(self.conn, Conn::Udp(_)) {
                    self.telemetry.count("logit.output.datagrams", *datagrams as f64, &[]);
                }
                self.telemetry.count("logit.output.requests", 1.0, &[("class", "ok")]);
            }
            Err(_) => {
                self.telemetry.count("logit.output.requests", 1.0, &[("class", "error")]);
            }
        }
        result.map(|_| ())
    }

    /// Implemented explicitly for the same reason `syslog_out` does: `send` performs one write per
    /// batch and retains nothing between calls, so there's nothing buffered here at shutdown --
    /// for TCP, this simply flushes the underlying stream.
    async fn flush(&mut self) -> anyhow::Result<()> {
        if let Conn::Tcp { stream: Some(stream), .. } = &mut self.conn {
            stream.flush().await.context("flushing statsd_out TCP stream")?;
        }
        Ok(())
    }

    /// `false`: a redelivered `hits:5|c` **increments the destination counter a second time**,
    /// silently corrupting the value with no trace at the receiver -- a stronger reason than
    /// `syslog_out`'s (which only duplicates a log line). Under the derived `AtMostOnce` posture
    /// this still lets a `Fault::Clean` retry succeed, covering the common receiver-restart
    /// outage with zero duplicate risk.
    fn duplicate_safe(&self) -> bool {
        false
    }
}

/// Running totals for one [`StatsdOutput::send_udp`] call. `entries_in_packet` is the count that
/// cannot be recovered from `packet_buf`'s bytes: a negative-absolute-gauge pair is **one**
/// [`MessageBuf`] entry containing an embedded `\n` (module doc's "Negative absolute gauges"), so
/// counting `\n` bytes in a packed datagram would report it as two messages over UDP where
/// `send_tcp` (and `syslog_out`, on both transports) reports one.
#[derive(Default)]
struct UdpSendCounts {
    /// [`MessageBuf`] entries actually written to the socket -- `logit.output.messages`.
    messages: usize,
    /// Datagrams actually written to the socket -- `logit.output.datagrams`.
    datagrams: usize,
    /// Entries appended to `packet_buf` since the last flush; reset by every flush.
    entries_in_packet: usize,
}

impl StatsdOutput {
    /// Packs `lines` into as few UDP datagrams as fit under `max_packet_bytes` (newline-joined, no
    /// trailing newline), then sends one `send_to` per datagram. See the module doc's "Packing and
    /// framing" section for why packing is correct here where `syslog_out` refuses it. A single
    /// line already longer than `max_packet_bytes` was dropped by the encoder, so every line seen
    /// here fits in its own datagram at minimum. Returns `(messages sent, datagrams sent)`.
    async fn send_udp(
        socket: &UdpSocket,
        endpoint: &str,
        lines: &MessageBuf,
        max_packet_bytes: usize,
        packet_buf: &mut Vec<u8>,
        diag: &mut Diagnostics,
        telemetry: &Telemetry,
    ) -> anyhow::Result<(usize, usize)> {
        // Resolved once per batch, not once per datagram -- see `syslog::send_udp`'s doc comment
        // for why a non-numeric host must not be re-resolved on every call.
        let mut addrs = lookup_host(endpoint)
            .await
            .context("resolving statsd_out endpoint")
            .context(Fault::Clean)?;
        let addr = addrs
            .next()
            .context("statsd_out endpoint resolved to no addresses")
            .context(Fault::Clean)?;

        let mut counts = UdpSendCounts::default();
        packet_buf.clear();
        for msg in lines.iter() {
            let needs_sep = !packet_buf.is_empty();
            let extra = msg.len() + usize::from(needs_sep);
            if !packet_buf.is_empty() && packet_buf.len() + extra > max_packet_bytes {
                Self::flush_datagram(socket, addr, packet_buf, &mut counts, diag, telemetry)
                    .await?;
            }
            if needs_sep && !packet_buf.is_empty() {
                packet_buf.push(b'\n');
            }
            packet_buf.extend_from_slice(msg);
            counts.entries_in_packet += 1;
        }
        if !packet_buf.is_empty() {
            Self::flush_datagram(socket, addr, packet_buf, &mut counts, diag, telemetry).await?;
        }
        Ok((counts.messages, counts.datagrams))
    }

    /// Sends one packed datagram, clearing `packet_buf` and `counts.entries_in_packet` after.
    /// Counts the datagram's [`MessageBuf`] **entries** -- not its `\n` bytes -- toward
    /// `counts.messages`: one entry may itself be a negative-gauge pair (two statsd lines joined
    /// by an embedded `\n`, module doc's "Negative absolute gauges"), and that is still one unit
    /// of "messages", the same convention [`Self::send_tcp`] (`lines.len()`) and `syslog_out`
    /// count by on both transports. The same count is what an oversize drop reports under
    /// `logit.output.messages.dropped{reason="oversize_datagram"}`.
    async fn flush_datagram(
        socket: &UdpSocket,
        addr: std::net::SocketAddr,
        packet_buf: &mut Vec<u8>,
        counts: &mut UdpSendCounts,
        diag: &mut Diagnostics,
        telemetry: &Telemetry,
    ) -> anyhow::Result<()> {
        match socket.send_to(packet_buf, addr).await {
            Ok(_) => {
                counts.messages += counts.entries_in_packet;
                counts.datagrams += 1;
            }
            Err(err) if is_message_too_large(&err) => {
                telemetry.count(
                    "logit.output.messages.dropped",
                    counts.entries_in_packet as f64,
                    &[("reason", "oversize_datagram")],
                );
                diag.warn_throttled(
                    "oversize_datagram",
                    format_args!("statsd_out: packed datagram too large for one send: {err}"),
                );
            }
            Err(err) => {
                let fault = if counts.datagrams > 0 { Fault::Ambiguous } else { Fault::Clean };
                packet_buf.clear();
                counts.entries_in_packet = 0;
                return Err(anyhow::Error::new(err).context(fault));
            }
        }
        packet_buf.clear();
        counts.entries_in_packet = 0;
        Ok(())
    }

    /// One newline-terminated frame (every line, including the last) per **batch**, written with
    /// at most one internal reconnect-and-retry -- a near-verbatim port of `syslog::send_tcp`,
    /// including both correctness properties documented on that function (cancellation safety via
    /// `stream.take()`, and never resending once a byte has left this host). Returns `(messages
    /// sent, 0)` -- there's no datagram count on TCP.
    async fn send_tcp(
        stream: &mut Option<TcpStream>,
        endpoint: &str,
        connect_timeout: Duration,
        lines: &MessageBuf,
        frame_buf: &mut Vec<u8>,
    ) -> anyhow::Result<(usize, usize)> {
        frame_buf.clear();
        for msg in lines.iter() {
            frame_buf.extend_from_slice(msg);
            frame_buf.push(b'\n');
        }

        let mut retried_after_a_zero_byte_failure = false;
        loop {
            let mut conn = match stream.take() {
                Some(conn) => conn,
                None => tokio::time::timeout(connect_timeout, TcpStream::connect(endpoint))
                    .await
                    .context("connecting to statsd_out endpoint timed out")
                    .and_then(|r| r.context("connecting to statsd_out endpoint"))
                    .context(Fault::Clean)?,
            };

            let first_write = match conn.write(frame_buf).await {
                Ok(0) if !frame_buf.is_empty() => {
                    Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "wrote zero bytes"))
                }
                Ok(n) => Ok(n),
                Err(err) => Err(err),
            };

            match first_write {
                Ok(n) => {
                    let rest_result = if n < frame_buf.len() {
                        conn.write_all(&frame_buf[n..]).await
                    } else {
                        Ok(())
                    };
                    return match rest_result {
                        Ok(()) => {
                            *stream = Some(conn);
                            Ok((lines.len(), 0))
                        }
                        Err(err) => Err(anyhow::Error::new(err).context(Fault::Ambiguous)),
                    };
                }
                Err(_) if !retried_after_a_zero_byte_failure => {
                    retried_after_a_zero_byte_failure = true;
                    continue;
                }
                Err(err) => return Err(anyhow::Error::new(err).context(Fault::Clean)),
            }
        }
    }
}

/// `90` is `EMSGSIZE` on Linux specifically -- see `syslog::is_message_too_large`'s doc comment;
/// this repo only ever ships/runs inside the Linux containers it builds.
fn is_message_too_large(err: &std::io::Error) -> bool {
    matches!(err.raw_os_error(), Some(errno) if errno == 90 /* EMSGSIZE, Linux */)
        || err.kind() == std::io::ErrorKind::InvalidInput
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{interner::intern, AttrMap, MetricRecord, Resource};
    use logit_inputs::statsd::StatsdDecoder;
    use logit_proto::Decoder;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;

    fn batch_with(events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
    }

    fn metric_event(name: &str, kind: MetricKind, attrs: &[(&str, Value)]) -> Event {
        let mut attributes = AttrMap::new();
        for (k, v) in attrs {
            attributes.insert(k, v.clone());
        }
        Event::metric(0, attributes, MetricRecord::new(intern(name), kind))
    }

    fn log_event(ts: i64) -> Event {
        Event::log(
            ts,
            AttrMap::new(),
            logit_core::LogRecord {
                message: Value::str("x"),
                severity: None,
                body_format: logit_core::BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        )
    }

    fn encode(events: Vec<Event>) -> (Vec<String>, EncodeStats) {
        encode_with_format(events, Format::DogStatsd)
    }

    fn encode_with_format(events: Vec<Event>, format: Format) -> (Vec<String>, EncodeStats) {
        let mut encoder = StatsdEncoder::new(format);
        let mut out = MessageBuf::default();
        let stats = encoder.encode_into(&batch_with(events), usize::MAX, &mut out);
        let msgs = out.iter().map(|b| String::from_utf8_lossy(b).into_owned()).collect();
        (msgs, stats)
    }

    fn encode_with(encoder: &mut StatsdEncoder, events: Vec<Event>) -> (Vec<String>, EncodeStats) {
        let mut out = MessageBuf::default();
        let stats = encoder.encode_into(&batch_with(events), usize::MAX, &mut out);
        let msgs = out.iter().map(|b| String::from_utf8_lossy(b).into_owned()).collect();
        (msgs, stats)
    }

    // -- Grammar ----------------------------------------------------------------------------

    #[test]
    fn a_counter_encodes_as_name_colon_value_pipe_c() {
        let (msgs, stats) = encode(vec![metric_event("hits", MetricKind::counter(3.0), &[])]);
        assert_eq!(stats, EncodeStats::default());
        assert_eq!(msgs, vec!["hits:3|c"]);
    }

    #[test]
    fn a_gauge_encodes_as_name_colon_value_pipe_g() {
        let (msgs, _) = encode(vec![metric_event("load", MetricKind::Gauge(0.75), &[])]);
        assert_eq!(msgs, vec!["load:0.75|g"]);
    }

    #[test]
    fn an_event_with_no_metrics_is_skipped_and_counted_rather_than_encoded_as_an_empty_line() {
        let (msgs, stats) = encode(vec![log_event(0)]);
        assert!(msgs.is_empty());
        assert_eq!(stats.skipped_no_metrics, 1);
    }

    #[test]
    fn no_sample_rate_segment_is_ever_emitted() {
        let (msgs, _) = encode(vec![metric_event("hits", MetricKind::counter(3.0), &[])]);
        assert!(!msgs[0].contains('@'));
    }

    #[test]
    fn no_timestamp_segment_is_ever_emitted() {
        let (msgs, _) = encode(vec![metric_event("hits", MetricKind::counter(3.0), &[])]);
        assert!(!msgs[0].contains('T'));
    }

    #[test]
    fn encoding_the_same_batch_twice_produces_byte_identical_output() {
        let events =
            vec![metric_event("hits", MetricKind::counter(3.0), &[("env", "prod".into())])];
        let (first, _) = encode_with_format(events.clone(), Format::DogStatsd);
        let (second, _) = encode_with_format(events, Format::DogStatsd);
        assert_eq!(first, second);
    }

    // -- Tags ---------------------------------------------------------------------------------

    #[test]
    fn dogstatsd_tags_render_as_one_hash_prefixed_comma_separated_segment() {
        let (msgs, _) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[("env", "prod".into()), ("host", "web1".into())],
        )]);
        assert_eq!(msgs[0], "hits:1|c|#env:prod,host:web1");
    }

    #[test]
    fn a_bool_true_attribute_encodes_as_a_bare_tag_not_key_colon_true() {
        let (msgs, _) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[("urgent", true.into())],
        )]);
        assert_eq!(msgs[0], "hits:1|c|#urgent");
    }

    #[test]
    fn a_bool_false_attribute_encodes_as_key_colon_false() {
        let (msgs, _) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[("verified", false.into())],
        )]);
        assert_eq!(msgs[0], "hits:1|c|#verified:false");
    }

    #[test]
    fn plain_statsd_format_omits_the_tag_segment_entirely() {
        let (msgs, stats) = encode_with_format(
            vec![metric_event("hits", MetricKind::counter(1.0), &[("env", "prod".into())])],
            Format::Statsd,
        );
        assert_eq!(msgs[0], "hits:1|c");
        assert_eq!(stats.tags_dropped_dialect, 1);
    }

    // -- Sanitization ---------------------------------------------------------------------------

    #[test]
    fn a_colon_in_a_tag_value_survives() {
        let (msgs, _) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[("range", "a:b".into())],
        )]);
        assert_eq!(msgs[0], "hits:1|c|#range:a:b");
    }

    #[test]
    fn a_colon_in_a_tag_key_is_replaced() {
        let mut attrs = AttrMap::new();
        attrs.insert("a:b", "x");
        let event =
            Event::metric(0, attrs, MetricRecord::new(intern("hits"), MetricKind::counter(1.0)));
        let (msgs, _) = encode(vec![event]);
        assert_eq!(msgs[0], "hits:1|c|#a_b:x");
    }

    #[test]
    fn an_embedded_newline_in_a_metric_name_cannot_forge_a_second_metric_line() {
        let (msgs, _) = encode(vec![metric_event("a\nb", MetricKind::counter(1.0), &[])]);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], "a_b:1|c");
    }

    #[test]
    fn a_metric_name_that_sanitizes_to_nothing_drops_the_metric_and_counts_it() {
        // Substitution never produces an empty result except from an already-empty input --
        // every forbidden character becomes `_`, not nothing (`:` alone sanitizes to `"_"`, a
        // perfectly good one-character name, not a drop).
        let (msgs, stats) = encode(vec![metric_event("", MetricKind::counter(1.0), &[])]);
        assert!(msgs.is_empty());
        assert_eq!(stats.dropped_empty_name, 1);
    }

    // -- Values and kinds -----------------------------------------------------------------------

    #[test]
    fn a_non_finite_counter_value_is_dropped_rather_than_written_as_the_text_nan() {
        let (msgs, stats) = encode(vec![metric_event("hits", MetricKind::counter(f64::NAN), &[])]);
        assert!(msgs.is_empty());
        assert_eq!(stats.dropped_unencodable_value, 1);
    }

    /// A `NO_RECORDED_VALUE`-flagged point must be dropped and counted, not written as a
    /// fabricated `name:0|g` -- fix 3 in PR #123's review (`docs/adr/lossless-transit.md`,
    /// `docs/known-gaps.md`'s cross-protocol table).
    #[test]
    fn a_no_recorded_value_point_is_dropped_and_counted() {
        let mut flagged = metric_event("conns", MetricKind::Gauge(0.0), &[]);
        flagged.metrics[0].flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
        let (msgs, stats) = encode(vec![flagged]);
        assert!(msgs.is_empty(), "a flagged point must not be written at all: {msgs:?}");
        assert_eq!(stats.dropped_no_recorded_value, 1);
    }

    #[test]
    fn a_gauge_delta_is_dropped_by_default() {
        let (msgs, stats) = encode(vec![metric_event("conns", MetricKind::GaugeDelta(5.0), &[])]);
        assert!(msgs.is_empty());
        assert_eq!(stats.dropped_gauge_delta, 1);
    }

    #[test]
    fn a_gauge_delta_encodes_as_a_signed_value_only_under_relative_gauges_true() {
        let mut encoder = StatsdEncoder::new(Format::DogStatsd).with_relative_gauges(true);
        let (msgs, stats) = encode_with(
            &mut encoder,
            vec![metric_event("conns", MetricKind::GaugeDelta(5.0), &[])],
        );
        assert_eq!(stats.dropped_gauge_delta, 0);
        assert_eq!(msgs[0], "conns:+5|g");
    }

    #[test]
    fn a_positive_gauge_delta_carries_an_explicit_plus_sign() {
        let mut encoder = StatsdEncoder::new(Format::DogStatsd).with_relative_gauges(true);
        let (msgs, _) = encode_with(
            &mut encoder,
            vec![metric_event("conns", MetricKind::GaugeDelta(5.0), &[])],
        );
        assert!(msgs[0].starts_with("conns:+5"));
    }

    #[test]
    fn a_negative_gauge_delta_needs_no_extra_sign_from_float_formatting() {
        let mut encoder = StatsdEncoder::new(Format::DogStatsd).with_relative_gauges(true);
        let (msgs, _) = encode_with(
            &mut encoder,
            vec![metric_event("conns", MetricKind::GaugeDelta(-5.0), &[])],
        );
        assert_eq!(msgs[0], "conns:-5|g");
    }

    #[test]
    fn a_negative_absolute_gauge_never_renders_as_a_bare_minus() {
        let (msgs, _) = encode(vec![metric_event("free", MetricKind::Gauge(-5.0), &[])]);
        assert_eq!(msgs.len(), 1, "the pair is one MessageBuf entry");
        assert_eq!(msgs[0], "free:0|g\nfree:-5|g");
    }

    #[test]
    fn a_negative_zero_gauge_renders_as_a_plain_zero_not_a_pair() {
        let (msgs, _) = encode(vec![metric_event("free", MetricKind::Gauge(-0.0), &[])]);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], "free:0|g");
    }

    #[test]
    fn distribution_set_histogram_and_summary_each_drop_with_a_clear_message() {
        let events = vec![
            metric_event("d", MetricKind::Distribution(logit_core::DdSketch::new()), &[]),
            metric_event("s", MetricKind::Set(logit_core::HyperLogLog::default()), &[]),
            metric_event(
                "h",
                MetricKind::Histogram(logit_core::Histogram {
                    buckets: vec![],
                    temporality: Temporality::Cumulative,
                    sum: None,
                    min: None,
                    max: None,
                }),
                &[],
            ),
            metric_event(
                "q",
                MetricKind::Summary(logit_core::Summary { quantiles: vec![], count: 0, sum: 0.0 }),
                &[],
            ),
        ];
        let (msgs, stats) = encode(events);
        assert!(msgs.is_empty());
        assert_eq!(stats.dropped_unsupported_kind, 4);
    }

    /// The new-in-v2 variants (`Samples`/`SetMembers`/`ExponentialHistogram`, raw or lossless data
    /// no producer emits until W3/W4) all fall through to the same unsupported-kind drop path as
    /// `Distribution`/`Set`/`Histogram`/`Summary` above -- not a panic, and not silently dropped
    /// uncounted.
    #[test]
    fn samples_set_members_and_exponential_histogram_each_drop_with_a_clear_message() {
        let events = vec![
            metric_event("s", MetricKind::Samples(logit_core::Samples::new([1.0])), &[]),
            metric_event("m", MetricKind::SetMembers(vec![bytes::Bytes::from_static(b"a")]), &[]),
            metric_event(
                "e",
                MetricKind::ExponentialHistogram(logit_core::ExpHistogram {
                    scale: 0,
                    zero_count: 0,
                    zero_threshold: 0.0,
                    positive: (0, vec![]),
                    negative: (0, vec![]),
                    temporality: Temporality::Cumulative,
                    count: 0,
                    sum: None,
                    min: None,
                    max: None,
                }),
                &[],
            ),
        ];
        let (msgs, stats) = encode(events);
        assert!(msgs.is_empty());
        assert_eq!(stats.dropped_unsupported_kind, 3);
    }

    /// A cumulative (or non-monotonic) `Sum` has no `|c` statsd can represent and is dropped, same
    /// as any other unsupported kind -- only a delta, monotonic `Sum` still encodes as `|c`.
    #[test]
    fn a_cumulative_sum_is_dropped_and_a_delta_monotonic_sum_still_encodes_as_c() {
        let (msgs, stats) = encode(vec![
            metric_event(
                "cumulative",
                MetricKind::Sum(logit_core::Sum {
                    value: 5.0,
                    temporality: Temporality::Cumulative,
                    monotonic: true,
                }),
                &[],
            ),
            metric_event("delta", MetricKind::counter(3.0), &[]),
        ]);
        assert_eq!(msgs, vec!["delta:3|c"]);
        assert_eq!(stats.dropped_unsupported_kind, 1);
    }

    #[test]
    fn a_dropped_distribution_does_not_take_a_healthy_counter_on_the_same_event_with_it() {
        let mut event = metric_event("ok", MetricKind::counter(1.0), &[]);
        event.metrics.push(MetricRecord::new(
            intern("bad"),
            MetricKind::Distribution(logit_core::DdSketch::new()),
        ));
        let (msgs, stats) = encode(vec![event]);
        assert_eq!(msgs, vec!["ok:1|c"]);
        assert_eq!(stats.dropped_unsupported_kind, 1);
    }

    // -- Packing and framing ------------------------------------------------------------------

    #[tokio::test]
    async fn udp_packs_several_lines_into_one_newline_separated_datagram() {
        let (addr, collector) = udp_collector().await;
        let mut output = StatsdOutput::udp(addr.to_string()).unwrap();
        let batch = batch_with(vec![
            metric_event("a", MetricKind::counter(1.0), &[]),
            metric_event("b", MetricKind::counter(2.0), &[]),
        ]);
        let recv_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            let (n, _) = collector.recv_from(&mut buf).await.unwrap();
            String::from_utf8_lossy(&buf[..n]).into_owned()
        });
        output.send(&batch).await.expect("send should succeed");
        let received =
            tokio::time::timeout(Duration::from_secs(2), recv_task).await.unwrap().unwrap();
        assert_eq!(received, "a:1|c\nb:2|c");
    }

    #[tokio::test]
    async fn a_line_that_would_overflow_the_cap_starts_a_new_datagram() {
        let (addr, collector) = udp_collector().await;
        let mut output = StatsdOutput::udp(addr.to_string()).unwrap();
        output = output.with_max_packet_bytes(10); // "a:1|c" is 5 bytes; two won't fit with a sep
        let batch = batch_with(vec![
            metric_event("a", MetricKind::counter(1.0), &[]),
            metric_event("b", MetricKind::counter(2.0), &[]),
        ]);
        let recv_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            let mut got = Vec::new();
            for _ in 0..2 {
                let (n, _) = collector.recv_from(&mut buf).await.unwrap();
                got.push(String::from_utf8_lossy(&buf[..n]).into_owned());
            }
            got
        });
        output.send(&batch).await.expect("send should succeed");
        let received =
            tokio::time::timeout(Duration::from_secs(2), recv_task).await.unwrap().unwrap();
        assert_eq!(received, vec!["a:1|c", "b:2|c"]);
    }

    #[test]
    fn a_single_line_longer_than_max_packet_bytes_is_dropped_whole() {
        let mut encoder = StatsdEncoder::new(Format::DogStatsd);
        let mut out = MessageBuf::default();
        let stats = encoder.encode_into(
            &batch_with(vec![metric_event("hits", MetricKind::counter(1.0), &[])]),
            3, // "hits:1|c" is longer than this
            &mut out,
        );
        assert!(out.is_empty());
        assert_eq!(stats.dropped_oversize_line, 1);
    }

    #[test]
    fn a_negative_absolute_gauges_two_lines_are_never_split_across_datagrams() {
        // The pair is pushed as one MessageBuf entry (an embedded '\n'), so the packer can never
        // split it -- verified indirectly: it either fits whole in a datagram, or the *whole* pair
        // is dropped as one oversize line, never half of it.
        let mut encoder = StatsdEncoder::new(Format::DogStatsd);
        let mut out = MessageBuf::default();
        let stats = encoder.encode_into(
            &batch_with(vec![metric_event("free", MetricKind::Gauge(-5.0), &[])]),
            usize::MAX,
            &mut out,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(stats.dropped_oversize_line, 0);
    }

    #[tokio::test]
    async fn a_udp_datagram_carries_no_trailing_newline() {
        let (addr, collector) = udp_collector().await;
        let mut output = StatsdOutput::udp(addr.to_string()).unwrap();
        let batch = batch_with(vec![metric_event("a", MetricKind::counter(1.0), &[])]);
        let recv_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            let (n, _) = collector.recv_from(&mut buf).await.unwrap();
            buf[..n].to_vec()
        });
        output.send(&batch).await.expect("send should succeed");
        let received =
            tokio::time::timeout(Duration::from_secs(2), recv_task).await.unwrap().unwrap();
        assert!(!received.ends_with(b"\n"));
    }

    #[tokio::test]
    async fn tcp_terminates_every_line_with_a_newline_including_the_last_one() {
        let (addr, received, _accepts) = tcp_collector().await;
        let mut output = StatsdOutput::tcp(addr.to_string(), Duration::from_secs(2));
        let batch = batch_with(vec![
            metric_event("a", MetricKind::counter(1.0), &[]),
            metric_event("b", MetricKind::counter(2.0), &[]),
        ]);
        output.send(&batch).await.expect("send should succeed");
        drop(output);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let got = received.lock().unwrap();
        assert_eq!(String::from_utf8_lossy(&got[0]), "a:1|c\nb:2|c\n");
    }

    // -- Socket ---------------------------------------------------------------------------------

    async fn udp_collector() -> (SocketAddr, Arc<UdpSocket>) {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        (addr, Arc::new(socket))
    }

    async fn tcp_collector() -> (SocketAddr, Arc<Mutex<Vec<Vec<u8>>>>, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let received: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
        let accepts = Arc::new(AtomicUsize::new(0));
        {
            let received = Arc::clone(&received);
            let accepts = Arc::clone(&accepts);
            tokio::spawn(async move {
                loop {
                    let Ok((mut stream, _)) = listener.accept().await else { break };
                    accepts.fetch_add(1, Ordering::SeqCst);
                    use tokio::io::AsyncReadExt;
                    let mut buf = Vec::new();
                    let _ = stream.read_to_end(&mut buf).await;
                    received.lock().unwrap().push(buf);
                }
            });
        }
        (addr, received, accepts)
    }

    #[tokio::test]
    async fn a_batch_with_nothing_encodable_performs_no_io_at_all() {
        let mut output = StatsdOutput::udp("127.0.0.1:1").unwrap();
        let batch = batch_with(vec![log_event(0)]);
        output.send(&batch).await.expect("an all-skipped batch must not attempt any I/O");
    }

    #[tokio::test]
    async fn tcp_sends_one_newline_delimited_frame_per_batch() {
        let (addr, received, accepts) = tcp_collector().await;
        let mut output = StatsdOutput::tcp(addr.to_string(), Duration::from_secs(2));
        let batch = batch_with(vec![metric_event("a", MetricKind::counter(1.0), &[])]);
        output.send(&batch).await.expect("send should succeed");
        drop(output);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(accepts.load(Ordering::SeqCst), 1);
        let got = received.lock().unwrap();
        assert_eq!(String::from_utf8_lossy(&got[0]), "a:1|c\n");
    }

    #[tokio::test]
    async fn tcp_reconnects_exactly_once_after_the_peer_resets_an_inherited_connection() {
        let (addr, received, accepts) = tcp_collector().await;
        let mut output = StatsdOutput::tcp(addr.to_string(), Duration::from_secs(2));

        let batch = batch_with(vec![metric_event("first", MetricKind::counter(1.0), &[])]);
        output.send(&batch).await.expect("first send should succeed against a fresh connection");

        if let Conn::Tcp { stream: Some(stream), .. } = &mut output.conn {
            stream.shutdown().await.expect("local shutdown should succeed");
        }

        let batch2 = batch_with(vec![metric_event("second", MetricKind::counter(1.0), &[])]);
        output
            .send(&batch2)
            .await
            .expect("second send should reconnect once and succeed, not surface the failure");

        drop(output);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(accepts.load(Ordering::SeqCst), 2);
        let got = received.lock().unwrap();
        assert!(got.iter().any(|b| String::from_utf8_lossy(b).contains("first")));
        assert!(got.iter().any(|b| String::from_utf8_lossy(b).contains("second")));
    }

    // -- Faults ---------------------------------------------------------------------------------

    #[tokio::test]
    async fn tcp_connect_refused_is_classified_as_a_clean_fault() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let mut output = StatsdOutput::tcp(addr.to_string(), Duration::from_millis(500));
        let batch = batch_with(vec![metric_event("a", MetricKind::counter(1.0), &[])]);
        let err = output.send(&batch).await.expect_err("connect should fail");
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
    }

    #[tokio::test]
    async fn duplicate_safe_is_false() {
        let output = StatsdOutput::udp("127.0.0.1:0").unwrap();
        assert!(!output.duplicate_safe());
    }

    // -- Telemetry --------------------------------------------------------------------------

    #[tokio::test]
    async fn an_unresolved_gauge_delta_reports_under_its_own_diagnostic_key() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("out", "statsd_out", "sink");
        let diag = Diagnostics::new("out").with_telemetry(telemetry);
        let mut output = StatsdOutput::udp("127.0.0.1:1").unwrap().with_diagnostics(diag);
        let batch = batch_with(vec![metric_event("conns", MetricKind::GaugeDelta(5.0), &[])]);
        output.send(&batch).await.expect("nothing encodable means no I/O");

        let found = registry.drain(0).into_iter().any(|e| {
            e.attributes.get("key").and_then(|v| v.as_str()) == Some("gauge_delta_unresolved")
        });
        assert!(found, "expected a gauge_delta_unresolved diagnostic");
    }

    /// The identical batch must report the identical `logit.output.messages` on both transports.
    /// A negative-absolute-gauge pair is one `MessageBuf` entry holding two statsd lines; UDP
    /// used to count its embedded `\n` as a second message where TCP counted the entry.
    #[tokio::test]
    async fn udp_and_tcp_report_the_same_message_count_for_the_same_batch() {
        fn messages(registry: &logit_core::Registry) -> f64 {
            registry
                .drain(0)
                .into_iter()
                .flat_map(|e| e.metrics)
                .filter(|m| logit_core::interner::resolve(m.name) == "logit.output.messages")
                .map(|m| match m.kind {
                    MetricKind::Sum(s) => s.value,
                    _ => panic!("logit.output.messages must be a counter"),
                })
                .sum()
        }
        let batch = || {
            batch_with(vec![
                metric_event("free", MetricKind::Gauge(-5.0), &[]),
                metric_event("hits", MetricKind::counter(1.0), &[]),
            ])
        };

        let (udp_addr, _collector) = udp_collector().await;
        let udp_registry = logit_core::Registry::new();
        let mut udp_out = StatsdOutput::udp(udp_addr.to_string())
            .unwrap()
            .with_telemetry(udp_registry.telemetry_for("out", "statsd_out", "sink"));
        udp_out.send(&batch()).await.expect("udp send should succeed");

        let (tcp_addr, _received, _accepts) = tcp_collector().await;
        let tcp_registry = logit_core::Registry::new();
        let mut tcp_out = StatsdOutput::tcp(tcp_addr.to_string(), Duration::from_secs(2))
            .with_telemetry(tcp_registry.telemetry_for("out", "statsd_out", "sink"));
        tcp_out.send(&batch()).await.expect("tcp send should succeed");

        assert_eq!(
            messages(&udp_registry),
            2.0,
            "one message per MessageBuf entry: the pair counts once"
        );
        assert_eq!(messages(&tcp_registry), 2.0);
    }

    // -- Round-trip through the real StatsdDecoder -----------------------------------------

    fn decode_one(line: &str) -> Vec<Event> {
        let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()));
        decoder.decode(bytes::Bytes::from(line.to_string())).expect("decode should succeed").events
    }

    #[test]
    fn a_counter_with_tags_round_trips_through_the_real_statsd_decoder() {
        let (msgs, _) =
            encode(vec![metric_event("hits", MetricKind::counter(3.0), &[("env", "prod".into())])]);
        let events = decode_one(&msgs[0]);
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0].metrics[0].kind, MetricKind::Sum(s) if s.value == 3.0));
        assert_eq!(events[0].attributes.get("env").and_then(|v| v.as_str()), Some("prod"));
    }

    #[test]
    fn a_bare_tag_round_trips_as_value_bool_true_through_the_real_statsd_decoder() {
        let (msgs, _) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[("urgent", true.into())],
        )]);
        let events = decode_one(&msgs[0]);
        assert!(matches!(events[0].attributes.get("urgent"), Some(Value::Bool(true))));
    }

    #[test]
    fn a_packed_multi_line_datagram_round_trips_as_several_events() {
        let mut encoder = StatsdEncoder::new(Format::DogStatsd);
        let mut out = MessageBuf::default();
        encoder.encode_into(
            &batch_with(vec![
                metric_event("a", MetricKind::counter(1.0), &[]),
                metric_event("b", MetricKind::counter(2.0), &[]),
            ]),
            usize::MAX,
            &mut out,
        );
        let packed: Vec<&str> = out.iter().map(|b| std::str::from_utf8(b).unwrap()).collect();
        let datagram = packed.join("\n");
        let events = decode_one(&datagram);
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn a_gauge_delta_round_trips_as_a_gauge_delta_under_relative_gauges() {
        let mut encoder = StatsdEncoder::new(Format::DogStatsd).with_relative_gauges(true);
        let (msgs, _) = encode_with(
            &mut encoder,
            vec![metric_event("conns", MetricKind::GaugeDelta(-5.0), &[])],
        );
        let events = decode_one(&msgs[0]);
        assert!(matches!(events[0].metrics[0].kind, MetricKind::GaugeDelta(v) if v == -5.0));
    }

    #[test]
    fn a_negative_absolute_gauge_round_trips_to_the_same_effective_value_not_a_delta() {
        let (msgs, _) = encode(vec![metric_event("free", MetricKind::Gauge(-5.0), &[])]);
        // The single MessageBuf entry contains two lines; decode both, in order.
        let events = decode_one(&msgs[0]);
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0].metrics[0].kind, MetricKind::Gauge(v) if v == 0.0));
        assert!(matches!(events[1].metrics[0].kind, MetricKind::GaugeDelta(v) if v == -5.0));
        // Applied in order against a starting gauge of 0, this reaches -5 -- the value we encoded.
    }

    /// `f64`'s `Display` renders `-0.0` as `"-0"`, and the decoder's `"g"` arm dispatches on the
    /// leading `-` without parsing the value -- so a naive rendering would come back as a no-op
    /// `GaugeDelta`, not the absolute reset to zero it was. Regression for that.
    #[test]
    fn a_negative_zero_gauge_does_not_decode_as_a_gauge_delta() {
        let (msgs, _) = encode(vec![metric_event("free", MetricKind::Gauge(-0.0), &[])]);
        assert!(!msgs[0].contains('-'), "no minus may reach the wire: {}", msgs[0]);
        let events = decode_one(&msgs[0]);
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0].metrics[0].kind, MetricKind::Gauge(v) if v == 0.0));
    }

    #[test]
    fn a_tag_value_containing_a_colon_round_trips_with_its_colon_intact() {
        let (msgs, _) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[("range", "a:b".into())],
        )]);
        let events = decode_one(&msgs[0]);
        assert_eq!(events[0].attributes.get("range").and_then(|v| v.as_str()), Some("a:b"));
    }

    #[test]
    fn a_full_statsd_in_to_statsd_out_relay_preserves_every_name_value_and_tag() {
        let original = decode_one("api.hits:3|c|#env:prod,host:web1");
        let name = logit_core::interner::resolve(original[0].metrics[0].name).to_string();
        let (msgs, _) = encode(original.clone());
        let relayed = decode_one(&msgs[0]);
        assert_eq!(logit_core::interner::resolve(relayed[0].metrics[0].name), name);
        assert!(matches!(relayed[0].metrics[0].kind, MetricKind::Sum(s) if s.value == 3.0));
        assert_eq!(relayed[0].attributes.get("env").and_then(|v| v.as_str()), Some("prod"));
        assert_eq!(relayed[0].attributes.get("host").and_then(|v| v.as_str()), Some("web1"));
    }
}
