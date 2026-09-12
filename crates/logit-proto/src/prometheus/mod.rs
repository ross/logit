//! The Prometheus codec: the exposition/OpenMetrics *semantic* model (`MetricFamily`/`Series`/
//! `Point`) and its two conversions to and from `logit`'s own event model. The text syntax for both
//! dialects lives next door in [`text`]; nothing in this file knows what a line looks like.
//!
//! **This module doc is the mapping table** (house convention, see `crate::otlp`'s module doc).
//!
//! **Why the split.** [`MetricFamily`] is the seam: `text.rs` maps bytes ↔ families, this module
//! maps families ↔ [`Event`]s. A future `remote_write.rs` (prompb ↔ families, see
//! [ADR `prometheus-scrape-and-exposition`](../../../../docs/adr/prometheus-scrape-and-exposition.md)'s
//! forward-compatibility section) plugs into the same seam and reuses every rule below unchanged --
//! remote-write is a transport for exactly the semantics the exposition format already describes
//! (`docs/design/telemetry-landscape.md`'s remote-write section), so the model mapping must not
//! depend on text syntax, and doesn't.
//!
//! **No [`crate::Encoder`]/[`crate::Decoder`]/[`crate::SignalEncoder`]/[`crate::SignalDecoder`]
//! implementation here**, deliberately, for the reason `statsd_out`/`syslog_out` don't have one
//! either (ADR `statsd-output` §"No `logit_proto::Encoder`"): `prometheus_out` is a *stateful*
//! registry an HTTP handler renders on demand -- there is no `EventBatch -> bytes` call at all, and
//! `prometheus_in` is an HTTP *client* that already holds the response body, not a framed stream a
//! `Decoder` gets fed. Both would have to lie about their shape to fit those traits. Plain
//! functions plus the two builder-configured handle types below ([`PrometheusDecoder`],
//! [`PrometheusEncoder`], `with_telemetry`/`with_diagnostics` like [`crate::otlp::OtlpEncoder`])
//! give the counters a home without inventing a trait relationship.
//!
//! ## Family naming
//!
//! [`MetricFamily::name`] is **the name the model uses** -- what a [`logit_core::MetricRecord`]'s
//! `name` is, verbatim, in both directions. That is not always the name on the family's `# TYPE`
//! line, and the difference is entirely `text.rs`'s business:
//!
//! | Family type | `# TYPE` name | Sample names |
//! |---|---|---|
//! | `counter` | text 0.0.4: `name`, with `_total` appended if it lacks it; OpenMetrics: `name` minus a trailing `_total` | the value sample always ends in `_total`; OM adds `<base>_created` |
//! | `histogram`/`gaugehistogram` | `name` | `name_bucket`, `name_sum`/`name_gsum`, `name_count`/`name_gcount`, `name_created` |
//! | `summary` | `name` | `name` (quantiles), `name_sum`, `name_count`, `name_created` |
//! | `info` | `name` | `name_info` |
//! | `stateset` | `name` | `name` (one per state) |
//! | `gauge`/`unknown`/`untyped` | `name` | `name` |
//!
//! So a text 0.0.4 counter family `http_requests_total` and an OpenMetrics counter family
//! `http_requests` (samples `http_requests_total`) both give `name = "http_requests_total"`: the
//! *sample* name is what a scraped series is actually called, so that is what the model keeps
//! ("name kept verbatim, `_total` included"). The `_total` dance is a dialect concern, not a model
//! one.
//!
//! ## Decode: families → events ([`families_to_events`])
//!
//! One [`Event`] per [`Series`], each carrying exactly one [`logit_core::MetricRecord`]; the
//! series' labels become `Value::Str` event attributes verbatim (no sanitization -- they arrived
//! valid).
//!
//! | Wire | Model |
//! |---|---|
//! | `counter` sample (name verbatim, `_total` included) | `Sum { value, Cumulative, monotonic: true }` |
//! | `gauge` | `Gauge(v)` |
//! | `histogram` (`_bucket{le}` cumulative, `_sum`, `_count`) | `Histogram { buckets: per-bucket counts by successive difference, trailing `(+Inf, n)`, Cumulative, sum, min/max: None }` |
//! | `gaugehistogram` (OM, `_gsum`/`_gcount`) | the same `Histogram`, plus `prometheus.type: "gaugehistogram"` |
//! | `summary` (`{quantile}`, `_sum`, `_count`) | `Summary { quantiles, count, sum }` |
//! | `untyped` (text) / `unknown` (OM) / no `# TYPE` | `Gauge(v)` + `prometheus.type: "untyped"` \| `"unknown"` |
//! | `info` (OM, `X_info{...} 1`) | `Gauge(1)` + `prometheus.type: "info"`, `name = X` (`_info` stripped) |
//! | `stateset` (OM) | one `Gauge(0\|1)` per state line + `prometheus.type: "stateset"` |
//! | `# HELP` / `# UNIT` | `description` / `unit` (interned) |
//! | `_created` (OM) | `start_timestamp` (seconds → nanos) |
//! | sample timestamp | `Event::timestamp` + `prometheus.timestamp: true`; absent → `received_at_nanos`, no marker |
//! | OM exemplar | `Exemplar { value, timestamp, trace, filtered_attributes }` -- `trace_id`/`span_id` labels become a [`logit_core::TraceRef`] when both are valid hex (consumed); every other exemplar label, and an invalid id, stays in `filtered_attributes`. Bucket exemplars all collect onto the one record. |
//!
//! ## Encode: events → families ([`events_to_families`])
//!
//! The inverse, plus the rules for model kinds Prometheus has no wire type for. Labels are
//! `resource` attributes merged with event attributes (event wins), `prometheus.*` skipped.
//!
//! | Model | Wire |
//! |---|---|
//! | `Sum{Cumulative, monotonic}` | `counter` |
//! | `Sum{Cumulative, !monotonic}` | `gauge` -- Prometheus has no non-monotonic counter; `logit.output.metrics.degraded{metric_kind="non_monotonic_sum"}` |
//! | `Sum{Delta}` / `Histogram{Delta}` | **skipped**, `logit.output.metrics.skipped{metric_kind="delta_sum"\|"delta_histogram"}` + `warn_throttled("delta_temporality_unresolved")` -- put an `aggregate` with `temporality: cumulative` in front of the sink |
//! | `Gauge` | `gauge`, or whatever `prometheus.type` says (consumed): `untyped`/`unknown`/`info`/`stateset` |
//! | `GaugeDelta` | **skipped**, `logit.output.metrics.skipped{metric_kind="gauge_delta"}` + `warn_throttled("gauge_delta_unresolved")` (the same greppable key every other sink uses) |
//! | `Histogram{Cumulative}` | `histogram`: running-sum buckets, `+Inf` = total, `_sum` only when `Some`; `min`/`max` dropped (known-gaps row); `prometheus.type: "gaugehistogram"` → `_gsum`/`_gcount` |
//! | `Summary` | `summary` + `_created` (OM) |
//! | `Distribution(sketch)` | `summary` of [`DISTRIBUTION_QUANTILES`] + `_count`, **no `_sum`** (a sketch has no sum; OpenMetrics permits omitting it) -- `logit.output.metrics.degraded{metric_kind="distribution"}` |
//! | `Samples` | `Samples::sketch()`, then exactly as above -- `degraded{metric_kind="samples"}` |
//! | `Set` / `SetMembers` | `gauge` of `estimate()` / the distinct member count -- `degraded{metric_kind="set"\|"set_members"}` |
//! | `ExponentialHistogram` | **skipped**, `logit.output.metrics.skipped{metric_kind="exponential_histogram"}` -- neither text dialect has native-histogram syntax |
//! | `flags & NO_RECORDED_VALUE` | **skipped**, `logit.output.metrics.skipped{reason="no_recorded_value"}` (`MetricRecord::flags`' own doc: every non-OTLP sink must treat a flagged point as carrying no reading) |
//! | exemplars | carried onto the family; [`text`] emits them on `_total`/`_bucket` lines in OpenMetrics only, at most one per line, each on the bucket its own value falls in. One that has no line left to sit on -- a counter's second exemplar, two in one bucket's range, one over OpenMetrics' 128-code-point label budget -- is dropped, `logit.output.metrics.degraded{reason="exemplar_dropped"}` (text 0.0.4 drops all of them uncounted: that is the operator's dialect choice, not a lossy mapping) |
//! | `Event::timestamp` | emitted only when `prometheus.timestamp: true` is present (consumed) |
//! | labels | `Value::Str/I64/U64/F64/Bool` stringified; `Null/Bytes/Timestamp/Array/Map` dropped -- `logit.output.labels.dropped{reason="unrepresentable"}` |
//! | names | sanitized ([`sanitize_metric_name`], [`sanitize_label_name`]); labels are ordered and collision-checked on their **rendered** names, and on a collision the one whose *original* attribute name sorts first wins -- `logit.output.labels.dropped{reason="collision"}`; a label sanitizing onto a generated one (`le` on a histogram, `quantile` on a summary) is dropped -- `logit.output.labels.dropped{reason="reserved"}` |
//! | two names sanitizing onto one wire name | the family whose *model* name sorts first is exposed, the rest are **skipped** -- `logit.output.metrics.skipped{reason="name_collision"}`. Both cannot be exposed: a second `# TYPE` line for one name makes Prometheus reject the whole scrape, so one clash would poison every other metric in the body |
//! | `unit` / `description` | `# UNIT` (OM only, and only when `_<unit>` suffixes the family name and the unit is `[a-zA-Z0-9_]+` -- the OpenMetrics spec requires it and Prometheus's parser fails the entire body otherwise; dropped counted `logit.output.metrics.degraded{reason="unit_not_suffix"}`) / `# HELP` |
//! | two records, one name, different family types | the first record's type wins, the rest are **skipped**, `logit.output.metrics.skipped{reason="type_conflict"}` -- one name cannot carry two `# TYPE` lines |
//! | `EventBatch::scope`, `Resource::schema_url`, `dropped_attributes_count` | dropped (known-gaps rows) |
//!
//! ## Well-known attributes (`prometheus.*`) and target identity
//!
//! Rule (b) of [ADR `lossless-transit`](../../../../docs/adr/lossless-transit.md) -- the raw,
//! protocol-native fact rides alongside the normalized model field and wins on the way back out.
//! Every `prometheus.*` attribute is *consumed* by the encode side (never rendered as a label) and
//! appears as an ordinary tag at every other sink. `docs/design/data-model.md`'s well-known-attribute
//! table is the canonical list; in short: [`ATTR_TYPE`] (`Value::Str`) names the wire family type the
//! model has no distinct kind for; [`ATTR_TIMESTAMP`] (`Value::Bool(true)`) means "this sample
//! carried its own timestamp"; [`ATTR_TARGET`] (`Value::Str`, a **resource** attribute stamped by
//! `prometheus_in`) is the full scrape URL.
//!
//! Target identity is the deliberate exception: `prometheus_in` also stamps an **unprefixed**
//! [`LABEL_INSTANCE`] (`instance`, `host:port`) on the resource, which this encoder therefore
//! renders as a label like any other resource attribute -- exactly the `instance` label Prometheus's
//! own scrape adds, so two targets exposing the same exporter don't collapse onto one series through
//! a relay. A relay's exposition is thus its input plus `instance`, which the fixed-point tests and
//! the ADR name as a permitted normalization. `job` stays an operator concern (a downstream `set`).
//!
//! ## Permitted normalizations
//!
//! `prometheus_in -> prometheus_out` is a fixed point modulo exactly this list (the round-trip
//! tests in `crates/logit-proto/tests/prometheus_fixed_point.rs` pin it):
//!
//! - family and series reordering: families sorted by name, series by label set, labels by rendered
//!   name;
//! - the scraper's own `instance` label, added the way Prometheus's own scrape adds it (above);
//! - float formatting: shortest round-trip (`1.0` → `1`, `1.458255915e9` → `1458255915`);
//! - a counter's value sample gains `_total` when the model name lacks it (both dialects), and the
//!   OpenMetrics family name loses it -- see [`text`]'s own table;
//! - `# TYPE x untyped`/`unknown` emitted for a family that arrived with no metadata at all;
//! - `_created`, `# UNIT` and exemplars dropped when the *output* dialect is text 0.0.4, and the
//!   OpenMetrics-only family types rendered as their nearest text 0.0.4 shape (see [`text`]);
//! - `# EOF` present per dialect; blank lines, non-`HELP`/`TYPE`/`UNIT` comments dropped;
//! - a histogram's `+Inf` bucket is authoritative for the total: a `_count` line that disagrees is
//!   ignored rather than kept as a second, conflicting total the model has nowhere to put;
//! - a histogram exemplar sits on the bucket its own *value* falls in, which for a conforming
//!   producer is the bucket it arrived on and for a non-conforming one is a relocation;
//! - a wire `summary` with no `_sum`/`_count` re-emits `_sum 0`/`_count 0`: [`logit_core::Summary`]
//!   holds `sum: f64`/`count: u64`, not `Option`s, so "absent" and "zero" are the same model value.
//!   (The reverse direction is exact: a `Distribution`'s sum-less summary stays sum-less, because
//!   that path builds the [`Point`] directly.)
//!
//! Everything else is an error or a counted skip, never a silent reinterpretation.

pub mod text;

pub use text::Dialect;

use crate::otlp::metrics::DISTRIBUTION_QUANTILES;
use logit_core::interner::{intern, resolve};
use logit_core::{
    AttrMap, DdSketch, Diagnostics, Event, Exemplar, Histogram, MetricKind, MetricRecord, Resource,
    Sum, Summary, Telemetry, Temporality, Value,
};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

/// The wire family type the model has no distinct kind for (`untyped`, `unknown`, `info`,
/// `stateset`, `gaugehistogram`) -- see the module doc's well-known-attribute section.
pub const ATTR_TYPE: &str = "prometheus.type";
/// `Value::Bool(true)`: this sample carried its own timestamp on the wire, so the encode side
/// re-emits one on that line (and only on that line).
pub const ATTR_TIMESTAMP: &str = "prometheus.timestamp";
/// The full scrape URL -- a **resource** attribute stamped by `prometheus_in`, consumed here like
/// every other `prometheus.*` attribute.
pub const ATTR_TARGET: &str = "prometheus.target";
/// `host:port` of the scraped target: an **unprefixed** resource attribute stamped by
/// `prometheus_in`, deliberately *outside* the consumed `prometheus.*` namespace so it renders as
/// the `instance` label Prometheus's own scrape adds. Two targets exposing the same exporter stay
/// distinct series through a relay because of it; an event-level `instance` wins over the
/// resource's (`honor_labels` semantics), which falls out of the resource/event merge for free.
pub const LABEL_INSTANCE: &str = "instance";

/// The `prometheus.` prefix every well-known attribute above shares; the encode side skips the
/// whole namespace when building labels rather than matching the four names individually, so a
/// later addition is automatically not leaked onto the wire as a label.
const ATTR_PREFIX: &str = "prometheus.";

/// A family's wire type. `Untyped` and `Unknown` are the same *semantics* (an unannotated sample)
/// under two dialect spellings, kept apart so `prometheus_in -> prometheus_out` re-emits the
/// spelling it received (`prometheus.type` carries it through the model). Each dialect writes the
/// other's spelling as its own -- see [`text`]'s module doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FamilyType {
    Counter,
    Gauge,
    Histogram,
    /// OpenMetrics only: a histogram of a quantity that can decrease (`_gsum`/`_gcount`).
    GaugeHistogram,
    Summary,
    /// OpenMetrics only: an always-`1` sample whose labels are the payload.
    Info,
    /// OpenMetrics only: one boolean sample per state.
    StateSet,
    /// OpenMetrics' spelling of "no type given".
    Unknown,
    /// Text 0.0.4's spelling of the same thing.
    Untyped,
}

impl FamilyType {
    /// The `# TYPE` keyword for this type in its own dialect -- also the `prometheus.type`
    /// attribute value for the five types the model has no distinct kind for.
    pub fn as_str(self) -> &'static str {
        match self {
            FamilyType::Counter => "counter",
            FamilyType::Gauge => "gauge",
            FamilyType::Histogram => "histogram",
            FamilyType::GaugeHistogram => "gaugehistogram",
            FamilyType::Summary => "summary",
            FamilyType::Info => "info",
            FamilyType::StateSet => "stateset",
            FamilyType::Unknown => "unknown",
            FamilyType::Untyped => "untyped",
        }
    }

    /// The inverse of [`FamilyType::as_str`] over a `# TYPE` keyword (`None` for an unrecognized
    /// one, which [`text`] treats as malformed metadata).
    pub fn from_keyword(s: &str) -> Option<FamilyType> {
        Some(match s {
            "counter" => FamilyType::Counter,
            "gauge" => FamilyType::Gauge,
            "histogram" => FamilyType::Histogram,
            "gaugehistogram" => FamilyType::GaugeHistogram,
            "summary" => FamilyType::Summary,
            "info" => FamilyType::Info,
            "stateset" => FamilyType::StateSet,
            "unknown" => FamilyType::Unknown,
            "untyped" => FamilyType::Untyped,
            _ => return None,
        })
    }

    /// Whether this type needs a `prometheus.type` marker attribute to survive the model: the five
    /// types that all decode onto a plain `Gauge`/`Histogram` and would otherwise be
    /// indistinguishable from one on the way back out.
    fn needs_marker(self) -> bool {
        matches!(
            self,
            FamilyType::Untyped
                | FamilyType::Unknown
                | FamilyType::Info
                | FamilyType::StateSet
                | FamilyType::GaugeHistogram
        )
    }

    /// Whether this type carries a `_created` sample (OpenMetrics): counter, histogram and summary
    /// only, per the OpenMetrics spec -- a `gaugehistogram` explicitly has none.
    pub fn has_created(self) -> bool {
        matches!(self, FamilyType::Counter | FamilyType::Histogram | FamilyType::Summary)
    }
}

/// One sample point of a series, in wire terms: bucket counts are *cumulative* (`le`-style), the
/// way both dialects carry them, not the per-bucket counts [`logit_core::Histogram`] holds.
#[derive(Debug, Clone, PartialEq)]
pub enum Point {
    Counter(f64),
    Gauge(f64),
    /// `buckets` is cumulative and ends with a `(f64::INFINITY, total)` entry; `count` is that
    /// total restated (the `_count`/`_gcount` sample), never an independent number -- see the
    /// module doc's normalization list.
    Histogram {
        buckets: Vec<(f64, u64)>,
        sum: Option<f64>,
        count: u64,
    },
    /// `sum`/`count` are `Option` because OpenMetrics permits omitting them, which is exactly what
    /// the `Distribution`/`Samples` encode path needs (a sketch has no sum to report).
    Summary {
        quantiles: Vec<(f64, f64)>,
        sum: Option<f64>,
        count: Option<u64>,
    },
    /// `X_info{...} 1` -- the value is always `1`, so there is nothing to carry.
    Info,
    /// One state of a stateset: `X{X="state"} 0|1`.
    StateSet(bool),
    /// An `untyped`/`unknown` sample.
    Unknown(f64),
}

/// One series of a family: a label set and its single point, plus the three per-series wire extras.
#[derive(Debug, Clone, PartialEq)]
pub struct Series {
    /// Sorted by label name, `le`/`quantile` excluded (those are part of [`Point`]).
    pub labels: Vec<(String, String)>,
    pub point: Point,
    /// Unix nanoseconds, when the sample carried its own timestamp.
    pub timestamp: Option<i64>,
    /// Unix nanoseconds, from an OpenMetrics `_created` sample.
    pub created: Option<i64>,
    /// OpenMetrics exemplars, in bucket order for a histogram. The writer places each one on the
    /// bucket its own value falls in.
    pub exemplars: Vec<Exemplar>,
}

impl Series {
    /// A series with no timestamp, no `_created`, and no exemplars -- what most producers and
    /// nearly every test want.
    pub fn new(labels: Vec<(String, String)>, point: Point) -> Self {
        Series { labels, point, timestamp: None, created: None, exemplars: Vec::new() }
    }
}

/// A metric family: one name, one type, one metadata pair, N series. The syntax-independent seam
/// between [`text`] (and, later, remote-write) and the model mapping in this module -- see the
/// module doc's "Family naming" table for what `name` means.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricFamily {
    pub name: String,
    pub kind: FamilyType,
    /// `# HELP`, unescaped. An empty `# HELP` decodes as `None`, not `Some("")`.
    pub help: Option<String>,
    /// `# UNIT` (OpenMetrics only).
    pub unit: Option<String>,
    /// Sorted by label set.
    pub series: Vec<Series>,
}

impl MetricFamily {
    /// An empty family -- `help`/`unit` `None`, no series.
    pub fn new(name: impl Into<String>, kind: FamilyType) -> Self {
        MetricFamily { name: name.into(), kind, help: None, unit: None, series: Vec::new() }
    }
}

/// Counts the decode side's skips (and carries the throttled-diagnostics handle a parse needs).
/// `Telemetry::default()`/`Diagnostics::default()` make every call a no-op, so a decoder used as a
/// pure codec -- [`text::parse`] with no component attached -- costs nothing to carry.
#[derive(Default)]
pub struct PrometheusDecoder {
    telemetry: Telemetry,
    diagnostics: Diagnostics,
}

impl PrometheusDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn with_diagnostics(mut self, diagnostics: Diagnostics) -> Self {
        self.diagnostics = diagnostics;
        self
    }

    /// `logit.input.metrics.skipped{reason}` -- one greppable counter for every "this input is
    /// wrong in a way we can step over" path, with the reason as a compile-time tag value.
    pub(crate) fn skipped(&self, reason: &'static str) {
        self.telemetry.count("logit.input.metrics.skipped", 1.0, &[("reason", reason)]);
    }

    /// `logit.input.metrics.degraded{reason}` -- the input was kept, but something in it was
    /// normalized away (today: a histogram whose `_count` disagreed with its `+Inf` bucket).
    pub(crate) fn degraded(&self, reason: &'static str) {
        self.telemetry.count("logit.input.metrics.degraded", 1.0, &[("reason", reason)]);
    }

    pub(crate) fn diagnostics(&mut self) -> &mut Diagnostics {
        &mut self.diagnostics
    }
}

/// Counts the encode side's lossy paths -- the mirror of [`PrometheusDecoder`], same builders, same
/// disabled-by-default no-op behavior as [`crate::otlp::OtlpEncoder`].
#[derive(Default)]
pub struct PrometheusEncoder {
    telemetry: Telemetry,
    diagnostics: Diagnostics,
}

impl PrometheusEncoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn with_diagnostics(mut self, diagnostics: Diagnostics) -> Self {
        self.diagnostics = diagnostics;
        self
    }

    fn skipped_kind(&self, metric_kind: &'static str) {
        self.telemetry.count("logit.output.metrics.skipped", 1.0, &[("metric_kind", metric_kind)]);
    }

    fn skipped_reason(&self, reason: &'static str) {
        self.telemetry.count("logit.output.metrics.skipped", 1.0, &[("reason", reason)]);
    }

    fn degraded(&self, metric_kind: &'static str) {
        self.telemetry.count("logit.output.metrics.degraded", 1.0, &[("metric_kind", metric_kind)]);
    }

    /// `logit.output.metrics.degraded{reason}` -- the same counter keyed by *why* rather than by
    /// which model kind, for a degradation the wire format forces on a metric of any kind (a
    /// `# UNIT` the format won't accept, an exemplar with nowhere to sit).
    fn degraded_reason(&self, reason: &'static str) {
        self.telemetry.count("logit.output.metrics.degraded", 1.0, &[("reason", reason)]);
    }

    fn label_dropped(&self, reason: &'static str) {
        self.telemetry.count("logit.output.labels.dropped", 1.0, &[("reason", reason)]);
    }
}

// -------------------------------------------------------------------------------------------------
// Decode: families -> events
// -------------------------------------------------------------------------------------------------

/// Families → events, one [`Event`] per [`Series`] (see the module doc's decode table).
/// `received_at_nanos` is the scrape's start instant: every series that carried no timestamp of its
/// own is stamped with it, exactly the contract [`crate::Decoder::decode_into`]'s `received_at`
/// documents for every other input.
pub fn families_to_events(
    families: &[MetricFamily],
    received_at_nanos: i64,
    decoder: &mut PrometheusDecoder,
) -> Vec<Event> {
    let mut out = Vec::new();
    for family in families {
        let name = intern(&family.name);
        let unit = family.unit.as_deref().map(intern);
        let description = family.help.as_deref().map(intern);
        for series in &family.series {
            let Some(kind) = point_to_kind(&series.point, decoder) else { continue };
            let mut attributes = AttrMap::new();
            for (label, value) in &series.labels {
                attributes.insert(label, Value::str(value.as_str()));
            }
            if family.kind.needs_marker() {
                attributes.insert(ATTR_TYPE, Value::str(family.kind.as_str()));
            }
            let timestamp = match series.timestamp {
                Some(ts) => {
                    attributes.insert(ATTR_TIMESTAMP, Value::Bool(true));
                    ts
                }
                None => received_at_nanos,
            };
            let record = MetricRecord {
                name,
                unit,
                description,
                start_timestamp: series.created.unwrap_or(0),
                exemplars: series.exemplars.clone(),
                flags: 0,
                kind,
            };
            out.push(Event::metric(timestamp, attributes, record));
        }
    }
    out
}

/// One [`Point`] → one [`MetricKind`], or `None` for a series this codec steps over (counted).
/// The cumulative → per-bucket conversion lives here rather than in [`text`] on purpose: it is a
/// model rule, not a syntax one, so a future remote-write path gets it for free.
fn point_to_kind(point: &Point, decoder: &mut PrometheusDecoder) -> Option<MetricKind> {
    Some(match point {
        Point::Counter(v) => MetricKind::Sum(Sum {
            value: *v,
            temporality: Temporality::Cumulative,
            monotonic: true,
        }),
        Point::Gauge(v) | Point::Unknown(v) => MetricKind::Gauge(*v),
        Point::Info => MetricKind::Gauge(1.0),
        Point::StateSet(on) => MetricKind::Gauge(if *on { 1.0 } else { 0.0 }),
        Point::Histogram { buckets, sum, .. } => {
            let buckets = per_bucket_counts(buckets, decoder)?;
            MetricKind::Histogram(Histogram {
                buckets,
                temporality: Temporality::Cumulative,
                sum: *sum,
                min: None,
                max: None,
            })
        }
        Point::Summary { quantiles, sum, count } => MetricKind::Summary(Summary {
            quantiles: quantiles.clone(),
            count: count.unwrap_or(0),
            sum: sum.unwrap_or(0.0),
        }),
    })
}

/// Cumulative `le` counts → [`logit_core::Histogram`]'s per-bucket counts, by successive
/// difference. A family with no buckets at all, or whose cumulative counts decrease (which no
/// conforming exposition produces and which would yield a negative bucket count), is skipped:
/// `logit.input.metrics.skipped{reason="empty_histogram"|"non_monotonic_buckets"}`. Bucket bounds
/// are assumed sorted ascending -- [`text::parse`] sorts them, and so does the encode side.
fn per_bucket_counts(
    cumulative: &[(f64, u64)],
    decoder: &mut PrometheusDecoder,
) -> Option<Vec<(f64, u64)>> {
    if cumulative.is_empty() {
        decoder.skipped("empty_histogram");
        return None;
    }
    let mut buckets = Vec::with_capacity(cumulative.len());
    let mut running = 0u64;
    for (bound, cumulative_count) in cumulative {
        let Some(delta) = cumulative_count.checked_sub(running) else {
            decoder.skipped("non_monotonic_buckets");
            decoder.diagnostics().warn_throttled(
                "prometheus_non_monotonic_buckets",
                format_args!(
                    "histogram bucket counts decrease at le={bound} -- the series is not \
                     cumulative; skipping it"
                ),
            );
            return None;
        };
        running = *cumulative_count;
        buckets.push((*bound, delta));
    }
    Some(buckets)
}

// -------------------------------------------------------------------------------------------------
// Encode: events -> families
// -------------------------------------------------------------------------------------------------

/// Events → families (see the module doc's encode table). Takes `(resource, event)` pairs rather
/// than an `EventBatch` so a stateful sink can feed several batches' worth of events -- or a
/// registry's worth of retained ones -- through one call without rebuilding a batch it doesn't
/// have.
///
/// The result is canonically ordered: families sorted by name, series by label set, labels by name.
pub fn events_to_families<'a>(
    events: impl Iterator<Item = (&'a Resource, &'a Event)>,
    encoder: &mut PrometheusEncoder,
) -> Vec<MetricFamily> {
    // Keyed by the *emitted* (sanitized) name, not the model name: two model names that sanitize
    // onto one cannot both be exposed -- a second `# TYPE` line for one name makes Prometheus reject
    // the whole scrape, so one naming clash would poison every other metric in the body.
    let mut families: BTreeMap<String, FamilyEntry> = BTreeMap::new();
    for (resource, event) in events {
        for record in &event.metrics {
            if record.is_no_recorded_value() {
                encoder.skipped_reason("no_recorded_value");
                continue;
            }
            let Some((kind, point)) = record_to_point(record, event, encoder) else { continue };
            let name = resolve(record.name);
            let key = sanitize_metric_name(name);
            let fresh = || FamilyEntry {
                origin: name,
                family: MetricFamily {
                    name: key.clone(),
                    kind,
                    help: record.description.map(|s| resolve(s).to_string()),
                    unit: record.unit.map(|s| resolve(s).to_string()),
                    series: Vec::new(),
                },
            };
            match families.get(&key) {
                // The family this record belongs to, already started.
                Some(existing) if existing.origin == name => {}
                // A different model name sanitizing onto the same wire name: the one whose original
                // name sorts first wins, so the outcome is the data's, not the arrival order's.
                Some(existing) if name < existing.origin => {
                    let displaced = families.insert(key.clone(), fresh()).expect("just probed");
                    for _ in 0..displaced.family.series.len().max(1) {
                        encoder.skipped_reason("name_collision");
                    }
                }
                Some(_) => {
                    encoder.skipped_reason("name_collision");
                    continue;
                }
                None => {
                    families.insert(key.clone(), fresh());
                }
            }
            let entry = &mut families.get_mut(&key).expect("inserted above").family;
            if entry.kind != kind {
                // Two records sharing one name but disagreeing on type: the wire has exactly one
                // `# TYPE` line per name, so the second one has nowhere to go.
                encoder.skipped_reason("type_conflict");
                continue;
            }
            let labels = build_labels(resource, event, kind, encoder);
            let timestamp = match event.attributes.get(ATTR_TIMESTAMP) {
                Some(Value::Bool(true)) => Some(event.timestamp),
                _ => None,
            };
            let created = if kind.has_created() && record.start_timestamp != 0 {
                Some(record.start_timestamp)
            } else {
                None
            };
            entry.series.push(Series {
                labels,
                point,
                timestamp,
                created,
                exemplars: record.exemplars.clone(),
            });
        }
    }

    // The map is keyed by the emitted name, so draining it in key order is already the canonical
    // family ordering -- no re-sort needed.
    let mut out: Vec<MetricFamily> = families.into_values().map(|entry| entry.family).collect();
    for family in &mut out {
        // Stable, so the last of a run of equal label sets is the last one pushed -- latest wins,
        // the cumulative-series semantics `prometheus_out`'s registry upsert also uses.
        family.series.sort_by(|a, b| a.labels.cmp(&b.labels));
        let mut deduped: Vec<Series> = Vec::with_capacity(family.series.len());
        for series in family.series.drain(..) {
            match deduped.last_mut() {
                Some(last) if last.labels == series.labels => *last = series,
                _ => deduped.push(series),
            }
        }
        family.series = deduped;
    }
    out
}

/// A family under construction plus the model name that claimed it -- the two together are what
/// resolves a post-sanitization name collision deterministically (see [`events_to_families`]).
struct FamilyEntry {
    origin: &'static str,
    family: MetricFamily,
}

/// One [`MetricRecord`] → its family type and wire point, or `None` for a kind Prometheus can't
/// carry (counted, see the module doc's encode table).
fn record_to_point(
    record: &MetricRecord,
    event: &Event,
    encoder: &mut PrometheusEncoder,
) -> Option<(FamilyType, Point)> {
    let declared = match event.attributes.get(ATTR_TYPE).and_then(|v| v.as_str()) {
        Some(s) => FamilyType::from_keyword(s),
        None => None,
    };
    Some(match &record.kind {
        MetricKind::Sum(s) => match (s.temporality, s.monotonic) {
            (Temporality::Cumulative, true) => (FamilyType::Counter, Point::Counter(s.value)),
            (Temporality::Cumulative, false) => {
                encoder.degraded("non_monotonic_sum");
                (FamilyType::Gauge, Point::Gauge(s.value))
            }
            (Temporality::Delta, _) => {
                encoder.skipped_kind("delta_sum");
                warn_delta(encoder, resolve(record.name));
                return None;
            }
        },
        MetricKind::Gauge(v) => match declared {
            Some(FamilyType::Untyped) => (FamilyType::Untyped, Point::Unknown(*v)),
            Some(FamilyType::Unknown) => (FamilyType::Unknown, Point::Unknown(*v)),
            Some(FamilyType::Info) => (FamilyType::Info, Point::Info),
            Some(FamilyType::StateSet) => (FamilyType::StateSet, Point::StateSet(*v != 0.0)),
            _ => (FamilyType::Gauge, Point::Gauge(*v)),
        },
        MetricKind::GaugeDelta(_) => {
            encoder.skipped_kind("gauge_delta");
            encoder.diagnostics.warn_throttled(
                "gauge_delta_unresolved",
                format_args!(
                    "a relative gauge adjustment reached a sink unresolved -- add an `aggregate` \
                     component between the statsd input and this output"
                ),
            );
            return None;
        }
        MetricKind::Histogram(h) => {
            if h.temporality == Temporality::Delta {
                encoder.skipped_kind("delta_histogram");
                warn_delta(encoder, resolve(record.name));
                return None;
            }
            let kind = if declared == Some(FamilyType::GaugeHistogram) {
                FamilyType::GaugeHistogram
            } else {
                FamilyType::Histogram
            };
            let (buckets, count) = cumulative_counts(&h.buckets);
            (kind, Point::Histogram { buckets, sum: h.sum, count })
        }
        MetricKind::ExponentialHistogram(_) => {
            encoder.skipped_kind("exponential_histogram");
            encoder.diagnostics.warn_throttled(
                "prometheus_exponential_histogram_skipped",
                format_args!(
                    "metric '{}' is an ExponentialHistogram, which neither Prometheus text 0.0.4 \
                     nor OpenMetrics 1.0 can express -- skipped",
                    resolve(record.name)
                ),
            );
            return None;
        }
        MetricKind::Summary(s) => (
            FamilyType::Summary,
            Point::Summary {
                quantiles: s.quantiles.clone(),
                sum: Some(s.sum),
                count: Some(s.count),
            },
        ),
        MetricKind::Distribution(sketch) => {
            encoder.degraded("distribution");
            (FamilyType::Summary, sketch_summary(sketch))
        }
        MetricKind::Samples(samples) => {
            encoder.degraded("samples");
            (FamilyType::Summary, sketch_summary(&samples.sketch()))
        }
        MetricKind::Set(hll) => {
            encoder.degraded("set");
            (FamilyType::Gauge, Point::Gauge(hll.estimate() as f64))
        }
        MetricKind::SetMembers(members) => {
            encoder.degraded("set_members");
            let distinct = members.iter().collect::<BTreeSet<_>>().len();
            (FamilyType::Gauge, Point::Gauge(distinct as f64))
        }
    })
}

/// The one diagnostic both delta arms report under -- a single greppable key naming the fix, the
/// way `gauge_delta_unresolved` does for its own missing-`aggregate` case.
fn warn_delta(encoder: &mut PrometheusEncoder, name: &str) {
    encoder.diagnostics.warn_throttled(
        "delta_temporality_unresolved",
        format_args!(
            "metric '{name}' has delta temporality, which Prometheus exposition cannot express -- \
             add an `aggregate` component with `temporality: cumulative` before this output"
        ),
    );
}

/// A sketch → the summary point the module doc's `Distribution`/`Samples` rows describe: the five
/// shared [`DISTRIBUTION_QUANTILES`] and a count, with **no** `_sum` -- a sketch has none, and
/// OpenMetrics permits omitting it.
fn sketch_summary(sketch: &DdSketch) -> Point {
    let quantiles = DISTRIBUTION_QUANTILES
        .iter()
        .filter_map(|q| sketch.quantile(*q).map(|v| (*q, v)))
        .collect();
    Point::Summary { quantiles, sum: None, count: Some(sketch.count() as u64) }
}

/// [`logit_core::Histogram`]'s per-bucket counts → the cumulative `le` counts both dialects carry,
/// plus the total. Bounds are sorted ascending first (the model doesn't promise an order) and a
/// `+Inf` bucket is appended when the highest bound is finite -- every conforming exposition has
/// one, and the total has to live somewhere.
fn cumulative_counts(buckets: &[(f64, u64)]) -> (Vec<(f64, u64)>, u64) {
    let mut sorted: Vec<(f64, u64)> = buckets.to_vec();
    sorted.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
    let mut out = Vec::with_capacity(sorted.len() + 1);
    let mut running = 0u64;
    for (bound, count) in sorted {
        running = running.saturating_add(count);
        out.push((bound, running));
    }
    if out.last().map(|(bound, _)| bound.is_finite()).unwrap_or(true) {
        out.push((f64::INFINITY, running));
    }
    (out, running)
}

/// Resource attributes merged with event attributes (event wins on a collision), `prometheus.*`
/// skipped, each name sanitized, each value stringified -- the label set for one series.
///
/// Ordering and collision resolution are decided on the **rendered** label names, not on `Symbol`
/// order: the emitted label set is sorted by the name that appears on the wire, and when two
/// attributes sanitize onto one label name the one whose *original* attribute name sorts first wins,
/// so the outcome depends on the data rather than on process-wide interning history.
fn build_labels(
    resource: &Resource,
    event: &Event,
    kind: FamilyType,
    encoder: &mut PrometheusEncoder,
) -> Vec<(String, String)> {
    // The label name this family type generates itself, which an attribute must not collide with:
    // a duplicate label name is invalid exposition, not merely confusing.
    let generated = match kind {
        FamilyType::Histogram | FamilyType::GaugeHistogram => Some("le"),
        FamilyType::Summary => Some("quantile"),
        _ => None,
    };
    let mut candidates: Vec<(&str, String, String)> = Vec::new();
    for (key, value) in logit_core::attrs::merged(resource, event) {
        let key = resolve(key);
        if key.starts_with(ATTR_PREFIX) {
            continue;
        }
        let Some(rendered) = label_value(value) else {
            encoder.label_dropped("unrepresentable");
            continue;
        };
        let name = sanitize_label_name(key);
        if Some(name.as_str()) == generated {
            encoder.label_dropped("reserved");
            continue;
        }
        candidates.push((key, name, rendered));
    }
    candidates.sort_by(|a, b| a.0.cmp(b.0));
    let mut labels: Vec<(String, String)> = Vec::with_capacity(candidates.len());
    for (_, name, value) in candidates {
        if labels.iter().any(|(existing, _)| *existing == name) {
            encoder.label_dropped("collision");
            continue;
        }
        labels.push((name, value));
    }
    labels.sort_by(|a, b| a.0.cmp(&b.0));
    labels
}

/// A [`Value`] as a Prometheus label value (always a string on the wire), or `None` for the kinds
/// with no faithful string form -- see the module doc's encode table.
fn label_value(value: &Value) -> Option<String> {
    Some(match value {
        Value::Str(_) => value.as_str()?.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::I64(i) => i.to_string(),
        Value::U64(u) => u.to_string(),
        Value::F64(f) => f.to_string(),
        Value::Null | Value::Bytes(_) | Value::Timestamp(_) | Value::Array(_) | Value::Map(_) => {
            return None
        }
    })
}

// -------------------------------------------------------------------------------------------------
// Sanitization
// -------------------------------------------------------------------------------------------------

/// A metric name forced into `[a-zA-Z_:][a-zA-Z0-9_:]*`: every other byte becomes `_`
/// (substitution, not deletion, so distinct inputs stay distinct -- the `statsd_out` precedent,
/// `crates/logit-outputs/src/statsd.rs`'s `sanitize_into`), and a leading digit gains a `_` prefix
/// rather than being replaced, which would fold `5xx_total` and `_xx_total` together. An empty name
/// becomes `_`. Two names that sanitize onto one produce two families with the same name; the
/// exposition then carries a duplicate `# TYPE`, which is the operator's own naming collision
/// (known-gaps row) rather than something this codec can resolve.
pub fn sanitize_metric_name(name: &str) -> String {
    sanitize(name, true)
}

/// A label name forced into `[a-zA-Z_][a-zA-Z0-9_]*` -- the same rules as
/// [`sanitize_metric_name`] minus `:`, which is legal in a metric name and not in a label name.
pub fn sanitize_label_name(name: &str) -> String {
    sanitize(name, false)
}

fn sanitize(name: &str, colon_ok: bool) -> String {
    let mut out = String::with_capacity(name.len() + 1);
    for c in name.chars() {
        let ok = c.is_ascii_alphanumeric() || c == '_' || (colon_ok && c == ':');
        out.push(if ok { c } else { '_' });
    }
    match out.chars().next() {
        None => out.push('_'),
        Some(c) if c.is_ascii_digit() => out.insert(0, '_'),
        Some(_) => {}
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::telemetry::Registry;
    use logit_core::{ExpHistogram, HyperLogLog, Samples, Sum, TraceRef};
    use std::sync::Arc;

    const RECEIVED_AT: i64 = 1_700_000_000_000_000_000;

    fn family(name: &str, kind: FamilyType, series: Vec<Series>) -> MetricFamily {
        MetricFamily { series, ..MetricFamily::new(name, kind) }
    }

    fn labels(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    fn decode(families: &[MetricFamily]) -> Vec<Event> {
        families_to_events(families, RECEIVED_AT, &mut PrometheusDecoder::new())
    }

    /// The one metric kind a single-series family decodes to.
    fn decode_kind(families: &[MetricFamily]) -> MetricKind {
        let events = decode(families);
        assert_eq!(events.len(), 1, "expected exactly one event");
        events[0].metrics[0].kind.clone()
    }

    fn telemetry() -> (Arc<Registry>, Telemetry) {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("prometheus", "prometheus_out", "sink");
        (registry, telemetry)
    }

    /// Whether `registry` recorded a point named `metric` carrying `tag`. Drains, so call once.
    fn counted(registry: &Registry, metric: &str, tag: (&str, &str)) -> bool {
        registry.drain(0).iter().any(|event| {
            event.metrics.iter().any(|m| resolve(m.name) == metric)
                && event.attributes.get(tag.0).and_then(|v| v.as_str()) == Some(tag.1)
        })
    }

    fn resource_with(attrs: &[(&str, Value)]) -> Resource {
        let mut attributes = AttrMap::new();
        for (key, value) in attrs {
            attributes.insert(key, value.clone());
        }
        Resource { attributes, ..Default::default() }
    }

    fn event_with(attrs: &[(&str, Value)], record: MetricRecord) -> Event {
        let mut attributes = AttrMap::new();
        for (key, value) in attrs {
            attributes.insert(key, value.clone());
        }
        Event::metric(RECEIVED_AT, attributes, record)
    }

    fn record(name: &str, kind: MetricKind) -> MetricRecord {
        MetricRecord::new(intern(name), kind)
    }

    fn encode(resource: &Resource, event: &Event) -> Vec<MetricFamily> {
        events_to_families(std::iter::once((resource, event)), &mut PrometheusEncoder::new())
    }

    fn encode_counted(
        resource: &Resource,
        event: &Event,
    ) -> (Vec<MetricFamily>, Arc<Registry>, Arc<Registry>) {
        let (registry, telemetry) = telemetry();
        let diag_registry = Registry::new();
        let diagnostics = Diagnostics::new("prometheus_out")
            .with_telemetry(diag_registry.telemetry_for("prometheus", "prometheus_out", "diag"));
        let mut encoder =
            PrometheusEncoder::new().with_telemetry(telemetry).with_diagnostics(diagnostics);
        let families = events_to_families(std::iter::once((resource, event)), &mut encoder);
        (families, registry, diag_registry)
    }

    fn encode_kind(kind: MetricKind) -> Vec<MetricFamily> {
        encode(&Resource::default(), &event_with(&[], record("m", kind)))
    }

    // --- decode: one test per row of the module doc's decode table ------------------------------

    #[test]
    fn a_counter_decodes_as_a_cumulative_monotonic_sum_keeping_its_name_verbatim() {
        let families = vec![family(
            "http_requests_total",
            FamilyType::Counter,
            vec![Series::new(labels(&[("code", "200")]), Point::Counter(1027.0))],
        )];
        let events = decode(&families);
        assert_eq!(resolve(events[0].metrics[0].name), "http_requests_total");
        assert_eq!(
            events[0].metrics[0].kind,
            MetricKind::Sum(Sum {
                value: 1027.0,
                temporality: Temporality::Cumulative,
                monotonic: true
            })
        );
        assert_eq!(events[0].attributes.get("code"), Some(&Value::from("200")));
    }

    #[test]
    fn a_gauge_decodes_as_a_gauge_with_no_type_marker() {
        let families = vec![family(
            "go_goroutines",
            FamilyType::Gauge,
            vec![Series::new(vec![], Point::Gauge(69.0))],
        )];
        let events = decode(&families);
        assert_eq!(events[0].metrics[0].kind, MetricKind::Gauge(69.0));
        assert_eq!(events[0].attributes.get(ATTR_TYPE), None, "a plain gauge needs no marker");
    }

    /// The cumulative-to-per-bucket successive difference, `+Inf` bucket included.
    #[test]
    fn a_histogram_decodes_with_per_bucket_counts_by_successive_difference() {
        let point = Point::Histogram {
            buckets: vec![(0.05, 24054), (0.1, 33444), (f64::INFINITY, 144320)],
            sum: Some(53423.0),
            count: 144320,
        };
        let families = vec![family("h", FamilyType::Histogram, vec![Series::new(vec![], point)])];
        assert_eq!(
            decode_kind(&families),
            MetricKind::Histogram(Histogram {
                buckets: vec![(0.05, 24054), (0.1, 9390), (f64::INFINITY, 110876)],
                temporality: Temporality::Cumulative,
                sum: Some(53423.0),
                min: None,
                max: None,
            })
        );
    }

    #[test]
    fn a_gaugehistogram_decodes_as_a_histogram_plus_a_type_marker() {
        let point = Point::Histogram {
            buckets: vec![(0.01, 20), (f64::INFINITY, 42)],
            sum: Some(3289.3),
            count: 42,
        };
        let families =
            vec![family("h", FamilyType::GaugeHistogram, vec![Series::new(vec![], point)])];
        let events = decode(&families);
        assert!(matches!(events[0].metrics[0].kind, MetricKind::Histogram(_)));
        assert_eq!(events[0].attributes.get(ATTR_TYPE), Some(&Value::from("gaugehistogram")));
    }

    #[test]
    fn a_summary_decodes_with_its_quantiles_count_and_sum() {
        let point = Point::Summary {
            quantiles: vec![(0.5, 4773.0), (0.99, 76656.0)],
            sum: Some(1.7560473e7),
            count: Some(2693),
        };
        let families = vec![family("s", FamilyType::Summary, vec![Series::new(vec![], point)])];
        assert_eq!(
            decode_kind(&families),
            MetricKind::Summary(Summary {
                quantiles: vec![(0.5, 4773.0), (0.99, 76656.0)],
                count: 2693,
                sum: 1.7560473e7,
            })
        );
    }

    /// A wire `summary` with neither `_sum` nor `_count` still decodes -- both become the model's
    /// own zero, which is the one place this mapping cannot tell "absent" from "zero" (see the
    /// module doc's normalization list).
    #[test]
    fn a_summary_with_no_sum_or_count_decodes_as_zero_for_both() {
        let point = Point::Summary { quantiles: vec![(0.5, 1.0)], sum: None, count: None };
        let families = vec![family("s", FamilyType::Summary, vec![Series::new(vec![], point)])];
        assert_eq!(
            decode_kind(&families),
            MetricKind::Summary(Summary { quantiles: vec![(0.5, 1.0)], count: 0, sum: 0.0 })
        );
    }

    #[test]
    fn untyped_and_unknown_decode_as_a_gauge_carrying_their_own_spelling() {
        for (kind, expected) in [(FamilyType::Untyped, "untyped"), (FamilyType::Unknown, "unknown")]
        {
            let families =
                vec![family("m", kind, vec![Series::new(vec![], Point::Unknown(12.47))])];
            let events = decode(&families);
            assert_eq!(events[0].metrics[0].kind, MetricKind::Gauge(12.47));
            assert_eq!(events[0].attributes.get(ATTR_TYPE), Some(&Value::from(expected)));
        }
    }

    #[test]
    fn an_info_family_decodes_as_a_gauge_of_one_named_without_the_info_suffix() {
        let families = vec![family(
            "target",
            FamilyType::Info,
            vec![Series::new(labels(&[("env", "prod")]), Point::Info)],
        )];
        let events = decode(&families);
        assert_eq!(resolve(events[0].metrics[0].name), "target");
        assert_eq!(events[0].metrics[0].kind, MetricKind::Gauge(1.0));
        assert_eq!(events[0].attributes.get(ATTR_TYPE), Some(&Value::from("info")));
        assert_eq!(events[0].attributes.get("env"), Some(&Value::from("prod")));
    }

    #[test]
    fn a_stateset_decodes_as_one_gauge_per_state() {
        let families = vec![family(
            "foo",
            FamilyType::StateSet,
            vec![
                Series::new(labels(&[("foo", "a")]), Point::StateSet(false)),
                Series::new(labels(&[("foo", "bb")]), Point::StateSet(true)),
            ],
        )];
        let events = decode(&families);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].metrics[0].kind, MetricKind::Gauge(0.0));
        assert_eq!(events[1].metrics[0].kind, MetricKind::Gauge(1.0));
        assert_eq!(events[1].attributes.get(ATTR_TYPE), Some(&Value::from("stateset")));
    }

    #[test]
    fn help_and_unit_decode_onto_description_and_unit() {
        let mut f =
            family("m", FamilyType::Counter, vec![Series::new(vec![], Point::Counter(1.0))]);
        f.help = Some("a help string".to_string());
        f.unit = Some("seconds".to_string());
        let events = decode(&[f]);
        assert_eq!(events[0].metrics[0].description.map(resolve), Some("a help string"));
        assert_eq!(events[0].metrics[0].unit.map(resolve), Some("seconds"));
    }

    #[test]
    fn created_decodes_onto_start_timestamp_and_is_zero_when_absent() {
        let mut series = Series::new(vec![], Point::Counter(1.0));
        series.created = Some(1_605_281_325_000_000_000);
        let with = vec![family("m", FamilyType::Counter, vec![series])];
        assert_eq!(decode(&with)[0].metrics[0].start_timestamp, 1_605_281_325_000_000_000);

        let without =
            vec![family("m", FamilyType::Counter, vec![Series::new(vec![], Point::Counter(1.0))])];
        assert_eq!(decode(&without)[0].metrics[0].start_timestamp, 0);
    }

    #[test]
    fn a_sample_timestamp_becomes_the_events_timestamp_and_is_marked() {
        let mut series = Series::new(vec![], Point::Gauge(1.0));
        series.timestamp = Some(1_395_066_363_000_000_000);
        let events = decode(&[family("m", FamilyType::Gauge, vec![series])]);
        assert_eq!(events[0].timestamp, 1_395_066_363_000_000_000);
        assert_eq!(events[0].attributes.get(ATTR_TIMESTAMP), Some(&Value::Bool(true)));
    }

    #[test]
    fn a_sample_with_no_timestamp_takes_the_scrape_time_and_no_marker() {
        let events =
            decode(&[family("m", FamilyType::Gauge, vec![Series::new(vec![], Point::Gauge(1.0))])]);
        assert_eq!(events[0].timestamp, RECEIVED_AT);
        assert_eq!(events[0].attributes.get(ATTR_TIMESTAMP), None);
    }

    #[test]
    fn exemplars_ride_onto_the_metric_record() {
        let exemplar = Exemplar {
            timestamp: 1_520_879_607_789_000_000,
            value: 0.67,
            trace: Some(TraceRef { trace_id: [7; 16], span_id: Some([6; 8]), flags: 0 }),
            filtered_attributes: AttrMap::new(),
        };
        let mut series = Series::new(vec![], Point::Counter(17.0));
        series.exemplars = vec![exemplar.clone()];
        let events = decode(&[family("m", FamilyType::Counter, vec![series])]);
        assert_eq!(events[0].metrics[0].exemplars, vec![exemplar]);
    }

    #[test]
    fn a_histogram_whose_cumulative_counts_decrease_is_skipped_and_counted() {
        let (registry, telemetry) = telemetry();
        let mut decoder = PrometheusDecoder::new().with_telemetry(telemetry);
        let point = Point::Histogram {
            buckets: vec![(1.0, 5), (2.0, 3), (f64::INFINITY, 3)],
            sum: None,
            count: 3,
        };
        let families = vec![family("h", FamilyType::Histogram, vec![Series::new(vec![], point)])];
        let events = families_to_events(&families, RECEIVED_AT, &mut decoder);
        assert!(events.is_empty(), "a non-cumulative bucket list must not decode");
        assert!(counted(
            &registry,
            "logit.input.metrics.skipped",
            ("reason", "non_monotonic_buckets")
        ));
    }

    #[test]
    fn a_histogram_with_no_buckets_at_all_is_skipped_and_counted() {
        let (registry, telemetry) = telemetry();
        let mut decoder = PrometheusDecoder::new().with_telemetry(telemetry);
        let point = Point::Histogram { buckets: vec![], sum: None, count: 0 };
        let families = vec![family("h", FamilyType::Histogram, vec![Series::new(vec![], point)])];
        assert!(families_to_events(&families, RECEIVED_AT, &mut decoder).is_empty());
        assert!(counted(&registry, "logit.input.metrics.skipped", ("reason", "empty_histogram")));
    }

    // --- encode: one test per row of the module doc's encode table ------------------------------

    #[test]
    fn a_cumulative_monotonic_sum_encodes_as_a_counter() {
        let kind = MetricKind::Sum(Sum {
            value: 5.0,
            temporality: Temporality::Cumulative,
            monotonic: true,
        });
        let families = encode_kind(kind);
        assert_eq!(families[0].kind, FamilyType::Counter);
        assert_eq!(families[0].series[0].point, Point::Counter(5.0));
    }

    #[test]
    fn a_non_monotonic_cumulative_sum_encodes_as_a_gauge_and_is_counted_degraded() {
        let kind = MetricKind::Sum(Sum {
            value: 5.0,
            temporality: Temporality::Cumulative,
            monotonic: false,
        });
        let (families, registry, _) =
            encode_counted(&Resource::default(), &event_with(&[], record("m", kind)));
        assert_eq!(families[0].kind, FamilyType::Gauge);
        assert!(counted(
            &registry,
            "logit.output.metrics.degraded",
            ("metric_kind", "non_monotonic_sum")
        ));
    }

    #[test]
    fn a_delta_sum_is_skipped_and_names_aggregates_cumulative_mode() {
        let (families, registry, diag) = encode_counted(
            &Resource::default(),
            &event_with(&[], record("m", MetricKind::counter(1.0))),
        );
        assert!(families.is_empty());
        assert!(counted(&registry, "logit.output.metrics.skipped", ("metric_kind", "delta_sum")));
        assert!(counted(
            &diag,
            "logit.component.diagnostics",
            ("key", "delta_temporality_unresolved")
        ));
    }

    #[test]
    fn a_delta_histogram_is_skipped_and_counted() {
        let kind = MetricKind::Histogram(Histogram {
            buckets: vec![(1.0, 1), (f64::INFINITY, 0)],
            temporality: Temporality::Delta,
            sum: None,
            min: None,
            max: None,
        });
        let (families, registry, _) =
            encode_counted(&Resource::default(), &event_with(&[], record("m", kind)));
        assert!(families.is_empty());
        assert!(counted(
            &registry,
            "logit.output.metrics.skipped",
            ("metric_kind", "delta_histogram")
        ));
    }

    #[test]
    fn a_gauges_family_type_follows_its_prometheus_type_attribute() {
        for (marker, expected_kind, expected_point) in [
            ("untyped", FamilyType::Untyped, Point::Unknown(2.0)),
            ("unknown", FamilyType::Unknown, Point::Unknown(2.0)),
            ("info", FamilyType::Info, Point::Info),
            ("stateset", FamilyType::StateSet, Point::StateSet(true)),
        ] {
            let event = event_with(
                &[(ATTR_TYPE, Value::from(marker))],
                record("m", MetricKind::Gauge(2.0)),
            );
            let families = encode(&Resource::default(), &event);
            assert_eq!(families[0].kind, expected_kind, "marker {marker}");
            assert_eq!(families[0].series[0].point, expected_point, "marker {marker}");
            assert!(
                families[0].series[0].labels.is_empty(),
                "`prometheus.*` attributes are consumed, never rendered as labels"
            );
        }
    }

    #[test]
    fn a_gauge_delta_is_skipped_under_the_shared_diagnostic_key() {
        let (families, registry, diag) = encode_counted(
            &Resource::default(),
            &event_with(&[], record("m", MetricKind::GaugeDelta(1.0))),
        );
        assert!(families.is_empty());
        assert!(counted(&registry, "logit.output.metrics.skipped", ("metric_kind", "gauge_delta")));
        assert!(counted(&diag, "logit.component.diagnostics", ("key", "gauge_delta_unresolved")));
    }

    #[test]
    fn a_cumulative_histogram_encodes_with_running_sum_buckets_and_drops_min_max() {
        let kind = MetricKind::Histogram(Histogram {
            buckets: vec![(1.0, 2), (5.0, 3), (f64::INFINITY, 1)],
            temporality: Temporality::Cumulative,
            sum: Some(12.5),
            min: Some(0.5),
            max: Some(9.9),
        });
        let families = encode_kind(kind);
        assert_eq!(families[0].kind, FamilyType::Histogram);
        assert_eq!(
            families[0].series[0].point,
            Point::Histogram {
                buckets: vec![(1.0, 2), (5.0, 5), (f64::INFINITY, 6)],
                sum: Some(12.5),
                count: 6,
            }
        );
    }

    /// `min`/`max` have no exposition representation at all -- the known-gaps row. Nothing in the
    /// emitted family can carry them, which this pins by encoding the same histogram twice, once
    /// with and once without, and demanding identical output.
    #[test]
    fn a_histograms_min_and_max_are_dropped_without_changing_anything_else() {
        let with = MetricKind::Histogram(Histogram {
            buckets: vec![(1.0, 2), (f64::INFINITY, 1)],
            temporality: Temporality::Cumulative,
            sum: Some(3.0),
            min: Some(0.5),
            max: Some(9.9),
        });
        let without = MetricKind::Histogram(Histogram {
            buckets: vec![(1.0, 2), (f64::INFINITY, 1)],
            temporality: Temporality::Cumulative,
            sum: Some(3.0),
            min: None,
            max: None,
        });
        assert_eq!(encode_kind(with), encode_kind(without));
    }

    #[test]
    fn a_histogram_with_a_gaugehistogram_marker_encodes_as_a_gaugehistogram() {
        let kind = MetricKind::Histogram(Histogram {
            buckets: vec![(1.0, 2), (f64::INFINITY, 1)],
            temporality: Temporality::Cumulative,
            sum: Some(3.0),
            min: None,
            max: None,
        });
        let event = event_with(&[(ATTR_TYPE, Value::from("gaugehistogram"))], record("m", kind));
        assert_eq!(encode(&Resource::default(), &event)[0].kind, FamilyType::GaugeHistogram);
    }

    /// A model histogram whose highest bound is finite (nothing forbids it) still gets a `+Inf`
    /// bucket on the way out: both formats require one, and the total has to live somewhere.
    #[test]
    fn a_histogram_with_no_infinite_bound_gains_an_inf_bucket() {
        let kind = MetricKind::Histogram(Histogram {
            buckets: vec![(1.0, 2), (5.0, 3)],
            temporality: Temporality::Cumulative,
            sum: None,
            min: None,
            max: None,
        });
        assert_eq!(
            encode_kind(kind)[0].series[0].point,
            Point::Histogram {
                buckets: vec![(1.0, 2), (5.0, 5), (f64::INFINITY, 5)],
                sum: None,
                count: 5,
            }
        );
    }

    #[test]
    fn a_summary_encodes_with_its_sum_and_count() {
        let kind =
            MetricKind::Summary(Summary { quantiles: vec![(0.5, 1.0)], count: 7, sum: 42.0 });
        assert_eq!(
            encode_kind(kind)[0].series[0].point,
            Point::Summary { quantiles: vec![(0.5, 1.0)], sum: Some(42.0), count: Some(7) }
        );
    }

    #[test]
    fn a_distribution_encodes_as_a_five_quantile_summary_with_no_sum_and_is_counted_degraded() {
        let mut sketch = DdSketch::new();
        for v in [1.0, 2.0, 3.0, 4.0, 5.0] {
            sketch.add(v);
        }
        let (families, registry, _) = encode_counted(
            &Resource::default(),
            &event_with(&[], record("m", MetricKind::Distribution(sketch))),
        );
        assert_eq!(families[0].kind, FamilyType::Summary);
        match &families[0].series[0].point {
            Point::Summary { quantiles, sum, count } => {
                assert_eq!(quantiles.len(), DISTRIBUTION_QUANTILES.len());
                assert_eq!(*sum, None, "a sketch has no sum to report");
                assert_eq!(*count, Some(5));
            }
            other => panic!("expected a summary, got {other:?}"),
        }
        assert!(counted(
            &registry,
            "logit.output.metrics.degraded",
            ("metric_kind", "distribution")
        ));
    }

    #[test]
    fn samples_are_sketched_into_the_same_summary_shape_and_counted_degraded() {
        let (families, registry, _) = encode_counted(
            &Resource::default(),
            &event_with(&[], record("m", MetricKind::Samples(Samples::new([1.0, 2.0, 3.0])))),
        );
        assert_eq!(families[0].kind, FamilyType::Summary);
        assert!(counted(&registry, "logit.output.metrics.degraded", ("metric_kind", "samples")));
    }

    #[test]
    fn a_set_encodes_as_a_gauge_of_its_estimate_and_is_counted_degraded() {
        let mut hll = HyperLogLog::new();
        hll.insert(b"alice");
        hll.insert(b"bob");
        let (families, registry, _) = encode_counted(
            &Resource::default(),
            &event_with(&[], record("m", MetricKind::Set(hll))),
        );
        assert_eq!(families[0].kind, FamilyType::Gauge);
        assert_eq!(families[0].series[0].point, Point::Gauge(2.0));
        assert!(counted(&registry, "logit.output.metrics.degraded", ("metric_kind", "set")));
    }

    #[test]
    fn set_members_encode_as_a_gauge_of_the_distinct_count_and_are_counted_degraded() {
        let members = vec![
            bytes::Bytes::from_static(b"alice"),
            bytes::Bytes::from_static(b"bob"),
            bytes::Bytes::from_static(b"alice"),
        ];
        let (families, registry, _) = encode_counted(
            &Resource::default(),
            &event_with(&[], record("m", MetricKind::SetMembers(members))),
        );
        assert_eq!(families[0].series[0].point, Point::Gauge(2.0));
        assert!(counted(
            &registry,
            "logit.output.metrics.degraded",
            ("metric_kind", "set_members")
        ));
    }

    #[test]
    fn an_exponential_histogram_is_skipped_and_counted() {
        let kind = MetricKind::ExponentialHistogram(ExpHistogram {
            scale: 1,
            zero_count: 0,
            zero_threshold: 0.0,
            positive: (0, vec![1]),
            negative: (0, vec![]),
            temporality: Temporality::Cumulative,
            count: 1,
            sum: None,
            min: None,
            max: None,
        });
        let (families, registry, _) =
            encode_counted(&Resource::default(), &event_with(&[], record("m", kind)));
        assert!(families.is_empty());
        assert!(counted(
            &registry,
            "logit.output.metrics.skipped",
            ("metric_kind", "exponential_histogram")
        ));
    }

    #[test]
    fn a_no_recorded_value_point_is_skipped_and_counted() {
        let mut rec = record("m", MetricKind::Gauge(0.0));
        rec.flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
        let (families, registry, _) = encode_counted(&Resource::default(), &event_with(&[], rec));
        assert!(families.is_empty());
        assert!(counted(
            &registry,
            "logit.output.metrics.skipped",
            ("reason", "no_recorded_value")
        ));
    }

    #[test]
    fn a_timestamp_is_emitted_only_when_the_marker_attribute_is_present() {
        let marked =
            event_with(&[(ATTR_TIMESTAMP, Value::Bool(true))], record("m", MetricKind::Gauge(1.0)));
        assert_eq!(encode(&Resource::default(), &marked)[0].series[0].timestamp, Some(RECEIVED_AT));

        let plain = event_with(&[], record("m", MetricKind::Gauge(1.0)));
        assert_eq!(encode(&Resource::default(), &plain)[0].series[0].timestamp, None);
    }

    #[test]
    fn created_is_emitted_only_for_the_types_that_have_one_and_only_when_known() {
        let mut counter = record(
            "m",
            MetricKind::Sum(Sum {
                value: 1.0,
                temporality: Temporality::Cumulative,
                monotonic: true,
            }),
        );
        counter.start_timestamp = 1_605_281_325_000_000_000;
        let event = event_with(&[], counter);
        assert_eq!(
            encode(&Resource::default(), &event)[0].series[0].created,
            Some(1_605_281_325_000_000_000)
        );

        // A gauge has no `_created` sample in either format, so a `start_timestamp` on one has
        // nowhere to go.
        let mut gauge = record("m", MetricKind::Gauge(1.0));
        gauge.start_timestamp = 1_605_281_325_000_000_000;
        let event = event_with(&[], gauge);
        assert_eq!(encode(&Resource::default(), &event)[0].series[0].created, None);
    }

    #[test]
    fn resource_and_event_attributes_both_become_labels_with_the_event_winning() {
        let resource =
            resource_with(&[("instance", Value::from("host:9100")), ("env", Value::from("dev"))]);
        let event =
            event_with(&[("env", Value::from("prod"))], record("m", MetricKind::Gauge(1.0)));
        assert_eq!(
            encode(&resource, &event)[0].series[0].labels,
            labels(&[("env", "prod"), ("instance", "host:9100")])
        );
    }

    /// Target identity is deliberately *not* in the consumed namespace: `instance` renders,
    /// `prometheus.target` does not (the PR #127 review's delta 1).
    #[test]
    fn the_instance_resource_attribute_renders_while_prometheus_target_is_consumed() {
        let resource = resource_with(&[
            (LABEL_INSTANCE, Value::from("node-exporter:9100")),
            (ATTR_TARGET, Value::from("http://node-exporter:9100/metrics")),
        ]);
        let event = event_with(&[], record("m", MetricKind::Gauge(1.0)));
        assert_eq!(
            encode(&resource, &event)[0].series[0].labels,
            labels(&[("instance", "node-exporter:9100")])
        );
    }

    #[test]
    fn every_representable_value_kind_stringifies_and_the_rest_are_counted_dropped() {
        let event = event_with(
            &[
                ("s", Value::from("text")),
                ("b", Value::Bool(true)),
                ("i", Value::I64(-3)),
                ("u", Value::U64(7)),
                ("f", Value::F64(1.5)),
                ("null", Value::Null),
                ("bytes", Value::Bytes(bytes::Bytes::from_static(b"\xff"))),
                ("ts", Value::Timestamp(1)),
                ("arr", Value::Array(vec![Value::I64(1)])),
                ("map", Value::Map(Box::new(AttrMap::new()))),
            ],
            record("m", MetricKind::Gauge(1.0)),
        );
        let (families, registry, _) = encode_counted(&Resource::default(), &event);
        assert_eq!(
            families[0].series[0].labels,
            labels(&[("b", "true"), ("f", "1.5"), ("i", "-3"), ("s", "text"), ("u", "7")])
        );
        assert!(counted(&registry, "logit.output.labels.dropped", ("reason", "unrepresentable")));
    }

    /// Two attribute names that sanitize onto one label: the one whose *original* name sorts first
    /// wins, so the outcome depends on the data rather than on interning order.
    #[test]
    fn a_post_sanitization_label_collision_keeps_the_first_original_name_and_is_counted() {
        let event = event_with(
            &[("a.b", Value::from("first")), ("a-b", Value::from("second"))],
            record("m", MetricKind::Gauge(1.0)),
        );
        let (families, registry, _) = encode_counted(&Resource::default(), &event);
        assert_eq!(families[0].series[0].labels, labels(&[("a_b", "second")]), "`a-b` < `a.b`");
        assert!(counted(&registry, "logit.output.labels.dropped", ("reason", "collision")));
    }

    #[test]
    fn an_attribute_colliding_with_a_generated_label_is_dropped_and_counted() {
        let histogram = MetricKind::Histogram(Histogram {
            buckets: vec![(1.0, 1), (f64::INFINITY, 0)],
            temporality: Temporality::Cumulative,
            sum: None,
            min: None,
            max: None,
        });
        let event = event_with(&[("le", Value::from("0.5"))], record("m", histogram));
        let (families, registry, _) = encode_counted(&Resource::default(), &event);
        assert!(families[0].series[0].labels.is_empty(), "`le` belongs to the bucket lines");
        assert!(counted(&registry, "logit.output.labels.dropped", ("reason", "reserved")));

        let summary =
            MetricKind::Summary(Summary { quantiles: vec![(0.5, 1.0)], count: 1, sum: 1.0 });
        let event = event_with(&[("quantile", Value::from("0.5"))], record("m", summary));
        let (families, registry, _) = encode_counted(&Resource::default(), &event);
        assert!(families[0].series[0].labels.is_empty());
        assert!(counted(&registry, "logit.output.labels.dropped", ("reason", "reserved")));
    }

    #[test]
    fn description_and_unit_become_help_and_unit() {
        let mut rec = record("m", MetricKind::Gauge(1.0));
        rec.description = Some(intern("what it measures"));
        rec.unit = Some(intern("seconds"));
        let families = encode(&Resource::default(), &event_with(&[], rec));
        assert_eq!(families[0].help.as_deref(), Some("what it measures"));
        assert_eq!(families[0].unit.as_deref(), Some("seconds"));
    }

    #[test]
    fn two_records_of_one_name_disagreeing_on_type_keep_the_first_and_count_the_rest() {
        let (registry, telemetry) = telemetry();
        let mut encoder = PrometheusEncoder::new().with_telemetry(telemetry);
        let resource = Resource::default();
        let gauge = event_with(&[], record("m", MetricKind::Gauge(1.0)));
        let counter = event_with(
            &[],
            record(
                "m",
                MetricKind::Sum(Sum {
                    value: 2.0,
                    temporality: Temporality::Cumulative,
                    monotonic: true,
                }),
            ),
        );
        let families = events_to_families(
            [(&resource, &gauge), (&resource, &counter)].into_iter(),
            &mut encoder,
        );
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].kind, FamilyType::Gauge);
        assert!(counted(&registry, "logit.output.metrics.skipped", ("reason", "type_conflict")));
    }

    #[test]
    fn one_name_and_one_label_set_twice_keeps_the_later_series() {
        let resource = Resource::default();
        let first = event_with(&[("k", Value::from("v"))], record("m", MetricKind::Gauge(1.0)));
        let second = event_with(&[("k", Value::from("v"))], record("m", MetricKind::Gauge(2.0)));
        let families = events_to_families(
            [(&resource, &first), (&resource, &second)].into_iter(),
            &mut PrometheusEncoder::new(),
        );
        assert_eq!(families[0].series.len(), 1, "one label set is one series");
        assert_eq!(families[0].series[0].point, Point::Gauge(2.0), "latest wins");
    }

    #[test]
    fn families_come_back_sorted_by_name_and_series_by_label_set() {
        let resource = Resource::default();
        let z = event_with(&[("k", Value::from("z"))], record("z_metric", MetricKind::Gauge(1.0)));
        let a2 = event_with(&[("k", Value::from("b"))], record("a_metric", MetricKind::Gauge(1.0)));
        let a1 = event_with(&[("k", Value::from("a"))], record("a_metric", MetricKind::Gauge(1.0)));
        let families = events_to_families(
            [(&resource, &z), (&resource, &a2), (&resource, &a1)].into_iter(),
            &mut PrometheusEncoder::new(),
        );
        assert_eq!(
            families.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
            vec!["a_metric", "z_metric"]
        );
        assert_eq!(
            families[0].series.iter().map(|s| s.labels[0].1.as_str()).collect::<Vec<_>>(),
            vec!["a", "b"]
        );
    }

    #[test]
    fn an_unsanitary_metric_name_is_substituted_not_deleted() {
        let families = encode(
            &Resource::default(),
            &event_with(&[], record("some.metric-name", MetricKind::Gauge(1.0))),
        );
        assert_eq!(families[0].name, "some_metric_name");
    }

    // --- sanitization ---------------------------------------------------------------------------

    #[test]
    fn metric_name_sanitization_substitutes_and_keeps_colons() {
        assert_eq!(sanitize_metric_name("job:rate5m"), "job:rate5m");
        assert_eq!(sanitize_metric_name("some.metric name"), "some_metric_name");
        // One `_` per *character*, not per byte -- `statsd_out`'s `sanitize_into` precedent.
        assert_eq!(sanitize_metric_name("ünïcode"), "_n_code");
    }

    #[test]
    fn a_leading_digit_gains_an_underscore_rather_than_replacing_the_digit() {
        assert_eq!(sanitize_metric_name("5xx_total"), "_5xx_total");
        assert_eq!(sanitize_label_name("2fa"), "_2fa");
    }

    #[test]
    fn an_empty_name_sanitizes_to_a_single_underscore() {
        assert_eq!(sanitize_metric_name(""), "_");
        assert_eq!(sanitize_label_name(""), "_");
    }

    #[test]
    fn a_colon_is_forbidden_in_a_label_name_though_legal_in_a_metric_name() {
        assert_eq!(sanitize_label_name("job:rate"), "job_rate");
    }

    // --- review follow-up: post-sanitization metric-name collisions -------------------------------

    /// Two model names that sanitize onto one wire name cannot both be exposed: a second `# TYPE`
    /// line for one name makes Prometheus reject the whole scrape, so one clash would poison every
    /// other metric in the body. The family whose *model* name sorts first wins.
    #[test]
    fn two_metric_names_sanitizing_onto_one_expose_the_first_and_count_the_rest() {
        let (registry, telemetry) = telemetry();
        let mut encoder = PrometheusEncoder::new().with_telemetry(telemetry);
        let resource = Resource::default();
        let dotted = event_with(&[], record("a.b", MetricKind::Gauge(1.0)));
        let dashed = event_with(&[], record("a-b", MetricKind::Gauge(2.0)));
        let families = events_to_families(
            [(&resource, &dotted), (&resource, &dashed)].into_iter(),
            &mut encoder,
        );
        assert_eq!(families.len(), 1, "one wire name is one family");
        assert_eq!(families[0].name, "a_b");
        // `a-b` < `a.b`, so the dashed one wins however the events arrive.
        assert_eq!(families[0].series[0].point, Point::Gauge(2.0));
        assert!(counted(&registry, "logit.output.metrics.skipped", ("reason", "name_collision")));
    }

    /// The same, with the winner arriving *second*: the already-started family is displaced rather
    /// than the outcome depending on arrival order.
    #[test]
    fn a_colliding_name_that_sorts_first_displaces_the_family_already_started() {
        let (registry, telemetry) = telemetry();
        let mut encoder = PrometheusEncoder::new().with_telemetry(telemetry);
        let resource = Resource::default();
        let dotted = event_with(&[], record("a.b", MetricKind::Gauge(1.0)));
        let dashed = event_with(&[], record("a-b", MetricKind::Gauge(2.0)));
        // Reverse arrival order from the test above; the result must be identical.
        let families = events_to_families(
            [(&resource, &dashed), (&resource, &dotted)].into_iter(),
            &mut encoder,
        );
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].series[0].point, Point::Gauge(2.0));
        assert!(counted(&registry, "logit.output.metrics.skipped", ("reason", "name_collision")));
    }

    /// A name that needs sanitizing but collides with nothing is simply renamed, not counted.
    #[test]
    fn a_sanitized_name_with_no_collision_is_not_counted() {
        let (registry, telemetry) = telemetry();
        let mut encoder = PrometheusEncoder::new().with_telemetry(telemetry);
        let resource = Resource::default();
        let event = event_with(&[], record("a.b", MetricKind::Gauge(1.0)));
        let families = events_to_families(std::iter::once((&resource, &event)), &mut encoder);
        assert_eq!(families[0].name, "a_b");
        assert!(!counted(&registry, "logit.output.metrics.skipped", ("reason", "name_collision")));
    }
}
