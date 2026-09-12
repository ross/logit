//! Prometheus text 0.0.4 and OpenMetrics 1.0 -- the syntax layer, both directions.
//!
//! [`parse`] turns a scrape body into [`MetricFamily`]s; [`write`] turns them back into bytes.
//! Nothing here knows about [`logit_core::Event`] -- that mapping is the parent module's, and
//! keeping the two apart is what lets a future remote-write module reuse the model mapping
//! unchanged (see [`super`]'s module doc).
//!
//! References: <https://prometheus.io/docs/instrumenting/exposition_formats/> and
//! <https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md>;
//! `docs/design/telemetry-landscape.md` summarizes both.
//!
//! ## What differs between the dialects
//!
//! | | text 0.0.4 | OpenMetrics 1.0 |
//! |---|---|---|
//! | timestamps | integer milliseconds | decimal **seconds** |
//! | trailing `# EOF` | not part of the format: a plain comment wherever it appears, with ordinary exposition allowed after it | **required** -- its absence, or any content after it, is [`CodecError::Malformed`] |
//! | counter samples | `<family>` -- and the family is named `<name>_total`, so the sample carries the suffix either way | `<family>_total` (+ `<family>_created`), family named without it |
//! | `# UNIT` | not part of the format (accepted on parse, never written) | written when a family has a unit **and** `_<unit>` suffixes the family name, which OpenMetrics requires and Prometheus enforces by failing the whole scrape; otherwise dropped, counted `logit.output.metrics.degraded{reason="unit_not_suffix"}` |
//! | `_created` | not part of the format (accepted on parse, never written) | written for counter/histogram/summary |
//! | exemplars | none (parsed and discarded, never written) | ` # {labels} value [ts]` on `_total`/`_bucket` lines |
//! | unannotated samples | `# TYPE x untyped` | `# TYPE x unknown` |
//! | `info`/`stateset`/`gaugehistogram` | no such type -- written as their nearest text shape (below) | native |
//! | metadata order | `# HELP` then `# TYPE` (Prometheus's own output) | `# TYPE`, `# UNIT`, `# HELP` (the spec's own examples) |
//!
//! **A counter's value sample always ends in `_total`, in both dialects.** Only the *family* name
//! differs: OpenMetrics names it without the suffix and text 0.0.4 with it, so a family whose model
//! name lacks `_total` gains it on the way out (a permitted normalization -- the wire name is then
//! what every Prometheus consumer expects, and re-scraping it is a fixed point).
//!
//! **OpenMetrics-only types in text 0.0.4.** `unknown` writes as `untyped` (and `untyped` writes as
//! `unknown` in OpenMetrics); `info` writes as a `gauge` named `<name>_info` whose single sample is
//! `1`; `stateset` writes as a `gauge` with the same per-state sample lines; `gaugehistogram` writes
//! as an ordinary `histogram` (`_gsum`/`_gcount` become `_sum`/`_count`). Each is a dialect *choice*
//! by whoever configured the output, so none is counted -- see [`super`]'s permitted-normalization
//! list.
//!
//! **Escaping.** Label values, both dialects: `\\`, `\"`, `\n`. `# HELP` text: `\\` and `\n` in text
//! 0.0.4, plus `\"` in OpenMetrics (whose `escaped-string` production forbids a bare `"`). On the
//! way in, an undefined escape (`\t`, say) stays literal -- backslash *and* the character -- which
//! is what Prometheus's own parser does; on the way out that literal backslash is then re-escaped,
//! so `\"` in a text 0.0.4 `# HELP` comes back as `\\"`. Same string, different bytes: a
//! canonicalization, listed with the other permitted normalizations.
//!
//! **Values** are Go-`ParseFloat` floats plus `NaN`, `+Inf`, `-Inf` (parsed case-insensitively,
//! written in exactly that spelling -- Rust's own `inf`/`NaN` rendering is not valid exposition).
//! Every finite value is written shortest-round-trip, so `1.0` comes back as `1` and
//! `1.458255915e9` as `1458255915`.
//!
//! **Timestamps are parsed digit by digit** ([`logit_core::parse_decimal_nanos`]), never through an
//! `f64`: an epoch-nanosecond instant needs 19 significant digits and an `f64` holds 15-16, so a
//! float round trip would silently move a sample in time. That covers the plain
//! `[SIGN] DIGIT+ ["." DIGIT*]` form every real exposition emits, sign included (the exposition
//! format's own example carries `something_weird{...} +Inf -3982045`). OpenMetrics' `realnumber`
//! production also permits an exponent (`1.605281325e9`), which has no digit-exact reading at all,
//! so that form alone falls back to `f64` -- ~1µs resolution at epoch magnitude, which beats
//! rejecting a legal timestamp. `_created` is read the same way, from the sample's own digits rather
//! than from its already-parsed value.
//!
//! ## Malformed input: what is skipped, and what fails the whole body
//!
//! [`CodecError::Malformed`] is reserved for framing this codec cannot step over:
//!
//! - an OpenMetrics body with no trailing `# EOF`;
//! - any content after `# EOF`.
//!
//! Everything else is skipped at line or series granularity and counted
//! `logit.input.metrics.skipped{reason=...}` on the [`PrometheusDecoder`]'s telemetry -- a scrape of
//! a mostly-good endpoint is worth keeping:
//!
//! | Reason | What it counts |
//! |---|---|
//! | `malformed_line` | a sample line this grammar rejects: a bad name, an unterminated label set, an unparsable value or timestamp, non-UTF-8 bytes, or Prometheus 3's quoted UTF-8 name syntax (`{"my.dotted.metric"} 1`), which this codec does not implement (`docs/known-gaps.md`) |
//! | `malformed_metadata` | a `# HELP`/`# TYPE`/`# UNIT` line with a bad name, a missing field (a bare `# TYPE` included), or an unrecognized type keyword -- the family stays untyped rather than the body failing. Fields are separated by a run of spaces or tabs, either way |
//! | `duplicate_type` | a second, conflicting `# TYPE` for one family; the first wins |
//! | `duplicate_metadata` | a second `# HELP`/`# UNIT` for one family; the first wins |
//! | `duplicate_series` | one sample repeated: the same label set twice for a family's primary/`_sum`/`_count`/`_created` sample, or the same `le`/`quantile` twice. Both formats require "a unique combination of a metric name and labels"; the first wins |
//! | `duplicate_label` | one line naming the same label twice -- an invalid label set, so the whole sample goes |
//! | `unknown_suffix` | a sample whose name is a declared family's name plus a suffix that type has no meaning for (`foo_sum` under `# TYPE foo counter`) |
//! | `incomplete_series` | a series with no value at all for its type: a counter with only a `_created`, a histogram with no buckets |
//!
//! One further skip reason (`non_monotonic_buckets`, with `empty_histogram` for a bucket-less
//! histogram) comes from the model mapping in [`super::families_to_events`] rather than from
//! parsing: a bucket list that decreases is only *detectable* as the cumulative → per-bucket
//! conversion runs, and that conversion is deliberately dialect-independent. One *degradation* is
//! counted here: `logit.input.metrics.degraded{reason="histogram_count_mismatch"}`, when a
//! histogram's `_count`/`_gcount` line disagrees with its `+Inf` bucket -- the `+Inf` line wins,
//! since the model holds one total, not two.
//!
//! **Deliberate leniencies**, each a place a strict reading of either spec would fail the body and
//! this parser does not, because a scraper's job is to keep what it can:
//!
//! - a sample for a family with no `# TYPE` at all becomes an `untyped` (text) / `unknown` (OM)
//!   family -- the OpenMetrics spec mandates exactly this, and text 0.0.4 implies it;
//! - metadata may follow its family's samples: a `# TYPE` for an already-implicit family retypes it
//!   in place. Samples already *routed* elsewhere are not re-homed, so a `# TYPE foo histogram`
//!   after a bare `foo_bucket{le="1"} 1` leaves that line in its own implicit family;
//! - families need not be contiguous (OpenMetrics requires it; this parser groups by name);
//! - `# UNIT` and `_created` are accepted in text 0.0.4, where they are not part of the format;
//! - a `_count`/`_gcount` that disagrees with the `+Inf` bucket loses to it (counted, above), and a
//!   histogram missing its `+Inf` bucket gains one from `_count` (or from its highest bucket) -- see
//!   [`super`]'s normalization list;
//! - buckets and quantiles are sorted on parse; both formats require increasing order anyway.
//!
//! ## Writing is deterministic
//!
//! Families sorted by name, series by label set, labels by name, the generated `le`/`quantile`
//! label written last. [`write`]/[`write_with`] never fail and never allocate per line (reused
//! scratch `String`s for number formatting).
//!
//! One exemplar per `_total`/`_bucket` line, as OpenMetrics requires ("a bucket MUST NOT have more
//! than one exemplar"), each placed on the bucket **its own value falls in** -- for a conforming
//! producer that is the bucket it arrived on, and for a non-conforming one it is a relocation, which
//! is on [`super`]'s permitted-normalization list. Everything with no line left to sit on is dropped
//! and counted `logit.output.metrics.degraded{reason="exemplar_dropped"}` (a counter's second
//! exemplar, two in one bucket's range, one over OpenMetrics' 128-code-point label budget). In text
//! 0.0.4 every exemplar is dropped and none of it is counted: that is the operator's dialect choice,
//! not a lossy mapping.

use super::{FamilyType, MetricFamily, Point, PrometheusDecoder, PrometheusEncoder, Series};
use crate::CodecError;
use logit_core::interner::resolve;
use logit_core::trace::{parse_span_id, parse_trace_id, to_hex};
use logit_core::{parse_decimal_nanos, AttrMap, Exemplar, TraceRef, Value};
use std::collections::HashMap;
use std::fmt::Write as _;

/// Which exposition dialect a body is in. The parser and writer differ in the ways the module
/// doc's table lists; everything else is shared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Text0_0_4,
    OpenMetrics1_0,
}

impl Dialect {
    /// The `Content-Type` a server sends (or a scraper's `Accept` asks for) for this dialect --
    /// dialect knowledge, so it lives here rather than being re-spelled by `prometheus_in`'s
    /// request builder and `prometheus_out`'s response builder independently.
    pub fn content_type(self) -> &'static str {
        match self {
            Dialect::Text0_0_4 => "text/plain; version=0.0.4; charset=utf-8",
            Dialect::OpenMetrics1_0 => "application/openmetrics-text; version=1.0.0; charset=utf-8",
        }
    }

    /// The dialect a scrape response's `Content-Type` selects: OpenMetrics only when it actually
    /// says so, text 0.0.4 for everything else (including a missing or unrecognized type) -- the
    /// same default Prometheus itself applies.
    pub fn from_content_type(value: &str) -> Dialect {
        const OM: &[u8] = b"application/openmetrics-text";
        let bytes = value.trim_start().as_bytes();
        if bytes.len() >= OM.len() && bytes[..OM.len()].eq_ignore_ascii_case(OM) {
            Dialect::OpenMetrics1_0
        } else {
            Dialect::Text0_0_4
        }
    }

    fn is_openmetrics(self) -> bool {
        self == Dialect::OpenMetrics1_0
    }

    /// Target nanoseconds per source unit for a timestamp in this dialect: milliseconds in text
    /// 0.0.4, seconds in OpenMetrics.
    fn timestamp_scale(self) -> i64 {
        match self {
            Dialect::Text0_0_4 => 1_000_000,
            Dialect::OpenMetrics1_0 => 1_000_000_000,
        }
    }
}

// -------------------------------------------------------------------------------------------------
// Parsing
// -------------------------------------------------------------------------------------------------

/// Parses a scrape body into families, canonically ordered (families by name, series by label set,
/// labels by name) -- the same order [`write`] emits and [`super::events_to_families`] produces, so
/// the three compose without a normalization pass in between.
///
/// Counters go nowhere: this is the no-telemetry convenience over [`parse_with`], for a caller with
/// no component attached (a test, a benchmark). Production callers hold a [`PrometheusDecoder`].
pub fn parse(bytes: &[u8], dialect: Dialect) -> Result<Vec<MetricFamily>, CodecError> {
    parse_with(bytes, dialect, &mut PrometheusDecoder::new())
}

/// [`parse`], reporting every skipped line/series on `decoder`'s telemetry and diagnostics -- see
/// the module doc's skip table.
pub fn parse_with(
    bytes: &[u8],
    dialect: Dialect,
    decoder: &mut PrometheusDecoder,
) -> Result<Vec<MetricFamily>, CodecError> {
    let mut parser = Parser::new(dialect);
    for line in bytes.split(|b| *b == b'\n') {
        parser.line(line, decoder)?;
    }
    parser.finish(decoder)
}

/// Which sample of a family a line carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    /// The family's own value sample: a counter's `_total`, a gauge/unknown/info/stateset line.
    Primary,
    Bucket,
    Sum,
    Count,
    Created,
    Quantile,
}

/// Every suffix either dialect gives meaning to. Order matters only in that a longer suffix must be
/// tried before a shorter one it ends with; none of these overlap that way today.
const SUFFIXES: [(&str, Role); 8] = [
    ("_total", Role::Primary),
    ("_info", Role::Primary),
    ("_created", Role::Created),
    ("_bucket", Role::Bucket),
    ("_gcount", Role::Count),
    ("_gsum", Role::Sum),
    ("_count", Role::Count),
    ("_sum", Role::Sum),
];

/// Whether `role` means anything for a family of type `kind`, and -- for a `_gsum`/`_gcount` vs
/// `_sum`/`_count` pair -- which spelling that type uses.
fn suffix_applies(kind: FamilyType, suffix: &str, role: Role) -> bool {
    match role {
        Role::Primary => match kind {
            FamilyType::Counter => suffix == "_total",
            FamilyType::Info => suffix == "_info",
            _ => false,
        },
        Role::Created => kind.has_created(),
        Role::Bucket => matches!(kind, FamilyType::Histogram | FamilyType::GaugeHistogram),
        Role::Sum | Role::Count => match kind {
            FamilyType::Histogram | FamilyType::Summary => suffix == "_sum" || suffix == "_count",
            FamilyType::GaugeHistogram => suffix == "_gsum" || suffix == "_gcount",
            _ => false,
        },
        Role::Quantile => false,
    }
}

/// The role a bare family name plays for its own type: a summary's quantile lines and a stateset's
/// state lines are named exactly the family name, a histogram's samples never are.
fn bare_name_role(kind: FamilyType) -> Option<Role> {
    match kind {
        FamilyType::Counter
        | FamilyType::Gauge
        | FamilyType::StateSet
        | FamilyType::Unknown
        | FamilyType::Untyped => Some(Role::Primary),
        FamilyType::Summary => Some(Role::Quantile),
        FamilyType::Histogram | FamilyType::GaugeHistogram | FamilyType::Info => None,
    }
}

/// One family under construction. `base` is the name as it appeared on the `# TYPE` line (or the
/// bare sample name that implied the family); `total_suffix` records that a counter's value sample
/// arrived as `<base>_total`, which is what decides the family's model-facing name -- see
/// [`super`]'s "Family naming" table.
struct FamilyAccum {
    base: String,
    kind: FamilyType,
    typed: bool,
    help: Option<String>,
    unit: Option<String>,
    total_suffix: bool,
    series: Vec<SeriesAccum>,
    series_index: HashMap<Vec<(String, String)>, usize>,
}

#[derive(Default)]
struct SeriesAccum {
    labels: Vec<(String, String)>,
    value: Option<f64>,
    buckets: Vec<(f64, u64)>,
    sum: Option<f64>,
    count: Option<u64>,
    quantiles: Vec<(f64, f64)>,
    timestamp: Option<i64>,
    created: Option<i64>,
    exemplars: Vec<Exemplar>,
}

struct Parser {
    dialect: Dialect,
    families: Vec<FamilyAccum>,
    index: HashMap<String, usize>,
    saw_eof: bool,
}

impl Parser {
    fn new(dialect: Dialect) -> Self {
        Parser { dialect, families: Vec::new(), index: HashMap::new(), saw_eof: false }
    }

    fn line(&mut self, raw: &[u8], decoder: &mut PrometheusDecoder) -> Result<(), CodecError> {
        let raw = trim(raw);
        if raw.is_empty() {
            return Ok(());
        }
        if self.saw_eof {
            return Err(CodecError::Malformed("content after `# EOF`".into()));
        }
        let Ok(text) = std::str::from_utf8(raw) else {
            decoder.skipped("malformed_line");
            return Ok(());
        };
        match text.strip_prefix('#') {
            Some(rest) => self.comment(rest.trim_start(), decoder),
            None => self.sample(text, decoder),
        }
        Ok(())
    }

    /// A `#` line: `HELP`/`TYPE`/`UNIT` metadata, the `EOF` terminator, or a comment to ignore.
    fn comment(&mut self, rest: &str, decoder: &mut PrometheusDecoder) {
        // `# EOF` terminates the body in OpenMetrics only. Text 0.0.4 has no terminator at all, so
        // there a `# EOF` is an ordinary comment and whatever follows it is ordinary exposition --
        // treating it as a terminator would discard a scrape Prometheus itself accepts.
        if rest == "EOF" && self.dialect.is_openmetrics() {
            self.saw_eof = true;
            return;
        }
        // Both formats separate metadata fields with spaces or tabs interchangeably; a metadata
        // keyword with nothing after it is a malformed line, not a comment to ignore.
        let (keyword, remainder) = split_field(rest);
        match keyword {
            "HELP" => {
                let (name, help) = split_field(remainder);
                let Some(idx) = self.family_for_metadata(name, decoder) else { return };
                if self.families[idx].help.is_some() {
                    decoder.skipped("duplicate_metadata");
                    return;
                }
                self.families[idx].help =
                    if help.is_empty() { None } else { Some(unescape_help(help, self.dialect)) };
            }
            "TYPE" => {
                let (name, keyword) = split_field(remainder);
                let Some(kind) = FamilyType::from_keyword(keyword) else {
                    decoder.skipped("malformed_metadata");
                    return;
                };
                let Some(idx) = self.family_for_metadata(name, decoder) else { return };
                if self.families[idx].typed {
                    if self.families[idx].kind != kind {
                        decoder.skipped("duplicate_type");
                    }
                    return;
                }
                self.families[idx].kind = kind;
                self.families[idx].typed = true;
            }
            "UNIT" => {
                let (name, unit) = split_field(remainder);
                let Some(idx) = self.family_for_metadata(name, decoder) else { return };
                if self.families[idx].unit.is_some() {
                    decoder.skipped("duplicate_metadata");
                    return;
                }
                self.families[idx].unit =
                    if unit.is_empty() { None } else { Some(unit.to_string()) };
            }
            // Any other `#` line is a plain comment, which both formats allow and neither gives
            // meaning to.
            _ => {}
        }
    }

    /// The family a metadata line names, creating it untyped if this is the first mention.
    fn family_for_metadata(
        &mut self,
        name: &str,
        decoder: &mut PrometheusDecoder,
    ) -> Option<usize> {
        if name.is_empty() || !is_metric_name(name) {
            decoder.skipped("malformed_metadata");
            return None;
        }
        Some(self.family_index(name))
    }

    fn family_index(&mut self, name: &str) -> usize {
        if let Some(idx) = self.index.get(name) {
            return *idx;
        }
        let idx = self.families.len();
        self.families.push(FamilyAccum {
            base: name.to_string(),
            kind: self.untyped(),
            typed: false,
            help: None,
            unit: None,
            total_suffix: false,
            series: Vec::new(),
            series_index: HashMap::new(),
        });
        self.index.insert(name.to_string(), idx);
        idx
    }

    /// The "no type given" family type in this dialect.
    fn untyped(&self) -> FamilyType {
        if self.dialect.is_openmetrics() {
            FamilyType::Unknown
        } else {
            FamilyType::Untyped
        }
    }

    fn sample(&mut self, line: &str, decoder: &mut PrometheusDecoder) {
        let Some(sample) = parse_sample(line, self.dialect) else {
            decoder.skipped("malformed_line");
            return;
        };
        let Some((idx, role, total_suffix)) = self.route(sample.name, decoder) else { return };
        let mut labels = sample.labels;
        // `le`/`quantile` are part of the point, not the series identity: strip them out before the
        // label set becomes the series key.
        let extra = match role {
            Role::Bucket => match take_label(&mut labels, "le").map(|v| parse_number(&v)) {
                Some(Some(bound)) => Some(bound),
                _ => {
                    decoder.skipped("malformed_line");
                    return;
                }
            },
            Role::Quantile => match take_label(&mut labels, "quantile").map(|v| parse_number(&v)) {
                Some(Some(q)) => Some(q),
                _ => {
                    decoder.skipped("malformed_line");
                    return;
                }
            },
            _ => None,
        };
        labels.sort_by(|a, b| a.0.cmp(&b.0));
        if labels.windows(2).any(|w| w[0].0 == w[1].0) {
            decoder.skipped("duplicate_label");
            return;
        }

        if total_suffix {
            self.families[idx].total_suffix = true;
        }
        let family = &mut self.families[idx];
        let series_idx = match family.series_index.get(&labels) {
            Some(i) => *i,
            None => {
                let i = family.series.len();
                family.series_index.insert(labels.clone(), i);
                family.series.push(SeriesAccum { labels, ..SeriesAccum::default() });
                i
            }
        };
        let series = &mut family.series[series_idx];
        let duplicate = match role {
            Role::Primary => replace_once(&mut series.value, sample.value),
            Role::Sum => replace_once(&mut series.sum, sample.value),
            Role::Count => match count_value(sample.value) {
                Some(c) => replace_once(&mut series.count, c),
                None => {
                    decoder.skipped("malformed_line");
                    return;
                }
            },
            Role::Created => match parse_created(sample.value_text) {
                Some(ts) => replace_once(&mut series.created, ts),
                None => {
                    decoder.skipped("malformed_line");
                    return;
                }
            },
            Role::Bucket => {
                let bound = extra.unwrap_or(f64::NAN);
                match count_value(sample.value) {
                    Some(c) if !bound.is_nan() => {
                        if series.buckets.iter().any(|(b, _)| *b == bound) {
                            true
                        } else {
                            series.buckets.push((bound, c));
                            false
                        }
                    }
                    _ => {
                        decoder.skipped("malformed_line");
                        return;
                    }
                }
            }
            Role::Quantile => {
                let q = extra.unwrap_or(f64::NAN);
                if q.is_nan() {
                    decoder.skipped("malformed_line");
                    return;
                }
                if series.quantiles.iter().any(|(existing, _)| *existing == q) {
                    true
                } else {
                    series.quantiles.push((q, sample.value));
                    false
                }
            }
        };
        if duplicate {
            decoder.skipped("duplicate_series");
            return;
        }
        // A timestamp rides the family's value-bearing samples; a `_created` line's own "value" is
        // the creation instant, so it never sets one.
        if role != Role::Created {
            if let Some(ts) = sample.timestamp {
                series.timestamp = Some(ts);
            }
        }
        if matches!(role, Role::Primary | Role::Bucket) {
            if let Some(exemplar) = sample.exemplar {
                series.exemplars.push(exemplar);
            }
        }
    }

    /// Which family and role a sample name belongs to: an exact family-name match first (so a gauge
    /// genuinely called `foo_sum` beats a histogram called `foo`), then a known suffix over a
    /// declared family, then a fresh implicit untyped family.
    fn route(
        &mut self,
        name: &str,
        decoder: &mut PrometheusDecoder,
    ) -> Option<(usize, Role, bool)> {
        if let Some(idx) = self.index.get(name).copied() {
            return match bare_name_role(self.families[idx].kind) {
                Some(role) => Some((idx, role, false)),
                None => {
                    decoder.skipped("unknown_suffix");
                    None
                }
            };
        }
        for (suffix, role) in SUFFIXES {
            let Some(base) = name.strip_suffix(suffix) else { continue };
            let Some(idx) = self.index.get(base).copied() else { continue };
            let kind = self.families[idx].kind;
            if suffix_applies(kind, suffix, role) {
                return Some((idx, role, suffix == "_total"));
            }
            decoder.skipped("unknown_suffix");
            return None;
        }
        let idx = self.family_index(name);
        Some((idx, Role::Primary, false))
    }

    fn finish(self, decoder: &mut PrometheusDecoder) -> Result<Vec<MetricFamily>, CodecError> {
        if self.dialect.is_openmetrics() && !self.saw_eof {
            return Err(CodecError::Malformed(
                "openmetrics exposition is missing its trailing `# EOF`".into(),
            ));
        }
        let mut out = Vec::with_capacity(self.families.len());
        for family in self.families {
            let name = match family.kind {
                FamilyType::Counter if family.total_suffix => format!("{}_total", family.base),
                _ => family.base.clone(),
            };
            let mut series: Vec<Series> = family
                .series
                .into_iter()
                .filter_map(|accum| finish_series(accum, family.kind, decoder))
                .collect();
            if series.is_empty() {
                continue;
            }
            series.sort_by(|a, b| a.labels.cmp(&b.labels));
            out.push(MetricFamily {
                name,
                kind: family.kind,
                help: family.help,
                unit: family.unit,
                series,
            });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }
}

/// Sets `slot` unless it already holds a value; returns whether this was a duplicate (the first
/// value always wins, which is what a strict parser rejecting the body would effectively have
/// kept).
fn replace_once<T>(slot: &mut Option<T>, value: T) -> bool {
    if slot.is_some() {
        return true;
    }
    *slot = Some(value);
    false
}

/// An accumulated series → its [`Point`], or `None` (counted) when the lines seen don't add up to a
/// value of the family's type.
fn finish_series(
    accum: SeriesAccum,
    kind: FamilyType,
    decoder: &mut PrometheusDecoder,
) -> Option<Series> {
    let SeriesAccum {
        labels,
        value,
        mut buckets,
        sum,
        count,
        mut quantiles,
        timestamp,
        created,
        exemplars,
    } = accum;
    let point = match kind {
        FamilyType::Counter => Point::Counter(value_or_skip(value, decoder)?),
        FamilyType::Gauge => Point::Gauge(value_or_skip(value, decoder)?),
        FamilyType::Unknown | FamilyType::Untyped => Point::Unknown(value_or_skip(value, decoder)?),
        FamilyType::Info => {
            value_or_skip(value, decoder)?;
            Point::Info
        }
        FamilyType::StateSet => Point::StateSet(value_or_skip(value, decoder)? != 0.0),
        FamilyType::Histogram | FamilyType::GaugeHistogram => {
            if buckets.is_empty() {
                decoder.skipped("incomplete_series");
                return None;
            }
            buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
            // Every conforming exposition has a `+Inf` bucket; when one is missing the total has to
            // come from somewhere, and `_count` (else the highest bucket) is that somewhere.
            let highest = buckets.last().map(|(_, c)| *c).unwrap_or(0);
            if buckets.last().map(|(b, _)| b.is_finite()).unwrap_or(true) {
                buckets.push((f64::INFINITY, count.unwrap_or(highest).max(highest)));
            }
            let total = buckets.last().map(|(_, c)| *c).unwrap_or(0);
            // The `+Inf` bucket is the total; a `_count`/`_gcount` line claiming otherwise is a
            // producer bug, and the model has exactly one place to put a total.
            if count.is_some_and(|c| c != total) {
                decoder.degraded("histogram_count_mismatch");
            }
            Point::Histogram { buckets, sum, count: total }
        }
        FamilyType::Summary => {
            if quantiles.is_empty() && sum.is_none() && count.is_none() && created.is_none() {
                decoder.skipped("incomplete_series");
                return None;
            }
            quantiles.sort_by(|a, b| a.0.total_cmp(&b.0));
            Point::Summary { quantiles, sum, count }
        }
    };
    Some(Series { labels, point, timestamp, created, exemplars })
}

fn value_or_skip(value: Option<f64>, decoder: &mut PrometheusDecoder) -> Option<f64> {
    match value {
        Some(v) => Some(v),
        None => {
            decoder.skipped("incomplete_series");
            None
        }
    }
}

/// Removes and returns a label by name -- how `le`/`quantile` leave the series' own label set.
fn take_label(labels: &mut Vec<(String, String)>, name: &str) -> Option<String> {
    let pos = labels.iter().position(|(k, _)| k == name)?;
    Some(labels.remove(pos).1)
}

/// One parsed sample line. Borrowed name, owned label strings -- the only allocations parsing makes
/// beyond the accumulators themselves.
struct Sample<'a> {
    name: &'a str,
    labels: Vec<(String, String)>,
    value: f64,
    /// The value token exactly as it appeared -- a `_created` sample's "value" is an instant, and
    /// reading it back out of `value` would already have rounded it (see [`parse_timestamp`]).
    value_text: &'a str,
    timestamp: Option<i64>,
    exemplar: Option<Exemplar>,
}

fn parse_sample(line: &str, dialect: Dialect) -> Option<Sample<'_>> {
    let bytes = line.as_bytes();
    let name_end = scan_metric_name(bytes, 0)?;
    let name = &line[..name_end];
    let mut i = name_end;
    let mut labels = Vec::new();
    if bytes.get(i) == Some(&b'{') {
        i = parse_labels(line, i, &mut labels)?;
    }
    let value_start = skip_ws(bytes, i);
    if value_start == i {
        // A sample's value must be separated from its name/labels by whitespace.
        return None;
    }
    let value_end = token_end(bytes, value_start);
    if value_end == value_start {
        return None;
    }
    let value_text = &line[value_start..value_end];
    let value = parse_number(value_text)?;
    i = skip_ws(bytes, value_end);

    let mut timestamp = None;
    if i < bytes.len() && bytes[i] != b'#' {
        let end = token_end(bytes, i);
        timestamp = Some(parse_timestamp(&line[i..end], dialect)?);
        i = skip_ws(bytes, end);
    }

    let mut exemplar = None;
    if i < bytes.len() {
        if bytes[i] != b'#' {
            return None;
        }
        let parsed = parse_exemplar(line, i + 1, dialect)?;
        // Text 0.0.4 has no exemplars: the trailing section still has to parse (trailing junk is a
        // malformed line either way), it just goes nowhere.
        if dialect.is_openmetrics() {
            exemplar = Some(parsed);
        }
    }
    Some(Sample { name, labels, value, value_text, timestamp, exemplar })
}

/// `{a="1",b="2"}` starting at `i` (which must be the `{`), appending each pair to `out`. Returns
/// the index just past the closing `}`.
fn parse_labels(line: &str, mut i: usize, out: &mut Vec<(String, String)>) -> Option<usize> {
    let bytes = line.as_bytes();
    i += 1; // the `{`
    loop {
        i = skip_ws(bytes, i);
        if bytes.get(i) == Some(&b'}') {
            return Some(i + 1);
        }
        let name_end = scan_label_name(bytes, i)?;
        let name = &line[i..name_end];
        i = skip_ws(bytes, name_end);
        if bytes.get(i) != Some(&b'=') {
            return None;
        }
        i = skip_ws(bytes, i + 1);
        if bytes.get(i) != Some(&b'"') {
            return None;
        }
        let (value, next) = parse_quoted(line, i + 1)?;
        out.push((name.to_string(), value));
        i = skip_ws(bytes, next);
        match bytes.get(i) {
            Some(b',') => i += 1,
            Some(b'}') => return Some(i + 1),
            _ => return None,
        }
    }
}

/// ` # {labels} value [timestamp]` starting just past the `#`.
fn parse_exemplar(line: &str, i: usize, dialect: Dialect) -> Option<Exemplar> {
    let bytes = line.as_bytes();
    let mut i = skip_ws(bytes, i);
    if bytes.get(i) != Some(&b'{') {
        return None;
    }
    let mut labels = Vec::new();
    i = parse_labels(line, i, &mut labels)?;
    let value_start = skip_ws(bytes, i);
    if value_start == i {
        return None;
    }
    let value_end = token_end(bytes, value_start);
    let value = parse_number(&line[value_start..value_end])?;
    i = skip_ws(bytes, value_end);
    let mut timestamp = 0;
    if i < bytes.len() {
        let end = token_end(bytes, i);
        timestamp = parse_timestamp(&line[i..end], dialect)?;
        i = skip_ws(bytes, end);
        if i < bytes.len() {
            return None;
        }
    }

    // `trace_id`/`span_id` become a real trace reference only when both are valid hex, the same
    // all-zero-is-invalid rule `TraceRef::from_bytes` applies everywhere else; an invalid one stays
    // an ordinary exemplar attribute rather than being silently dropped.
    let trace_id = labels.iter().position(|(k, _)| k == "trace_id").and_then(|i| {
        let id = parse_trace_id(&labels[i].1)?;
        Some((i, id))
    });
    let trace = trace_id.map(|(pos, trace_id)| {
        labels.remove(pos);
        let span = labels.iter().position(|(k, _)| k == "span_id").and_then(|i| {
            let id = parse_span_id(&labels[i].1)?;
            Some((i, id))
        });
        let span_id = span.map(|(pos, id)| {
            labels.remove(pos);
            id
        });
        TraceRef { trace_id, span_id, flags: 0 }
    });
    let mut filtered_attributes = AttrMap::new();
    for (key, value) in labels {
        filtered_attributes.insert(&key, Value::str(value));
    }
    Some(Exemplar { timestamp, value, trace, filtered_attributes })
}

/// A double-quoted, escaped label value starting just past the opening quote. Returns the unescaped
/// string and the index just past the closing quote.
fn parse_quoted(line: &str, start: usize) -> Option<(String, usize)> {
    let bytes = line.as_bytes();
    let mut out = String::new();
    let mut i = start;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => return Some((out, i + 1)),
            b'\\' => {
                let next = *bytes.get(i + 1)?;
                match next {
                    b'n' => out.push('\n'),
                    b'"' => out.push('"'),
                    b'\\' => out.push('\\'),
                    // An undefined escape stays literal, backslash included -- what Prometheus's
                    // own parser does.
                    _ => {
                        out.push('\\');
                        push_char_at(&mut out, line, i + 1);
                    }
                }
                i += 1 + char_len(bytes, i + 1);
            }
            _ => {
                push_char_at(&mut out, line, i);
                i += char_len(bytes, i);
            }
        }
    }
    None
}

/// The byte length of the UTF-8 character starting at `i` -- the scanner walks bytes, but a label
/// value is arbitrary UTF-8 and must not be sliced mid-character.
fn char_len(bytes: &[u8], i: usize) -> usize {
    match bytes.get(i) {
        None => 0,
        Some(b) if *b < 0x80 => 1,
        Some(b) if *b >> 5 == 0b110 => 2,
        Some(b) if *b >> 4 == 0b1110 => 3,
        Some(_) => 4,
    }
}

fn push_char_at(out: &mut String, line: &str, i: usize) {
    let len = char_len(line.as_bytes(), i);
    out.push_str(&line[i..i + len]);
}

/// Splits one metadata field off the front: `# HELP <name> <text>` / `# TYPE <name> <keyword>` /
/// `# UNIT <name> <unit>` all separate their fields with a run of spaces **or tabs** (both formats
/// treat the two interchangeably, as the sample path's `skip_ws` already does). Returns the field
/// and whatever follows the separator, each empty when there is nothing there.
fn split_field(s: &str) -> (&str, &str) {
    let bytes = s.as_bytes();
    let end = token_end(bytes, 0);
    (&s[..end], &s[skip_ws(bytes, end)..])
}

fn is_metric_name(s: &str) -> bool {
    scan_metric_name(s.as_bytes(), 0) == Some(s.len()) && !s.is_empty()
}

/// The end index of a metric name (`[a-zA-Z_:][a-zA-Z0-9_:]*`) starting at `i`, or `None` when
/// there isn't one.
fn scan_metric_name(bytes: &[u8], i: usize) -> Option<usize> {
    scan_name(bytes, i, true)
}

/// The same for a label name (`[a-zA-Z_][a-zA-Z0-9_]*` -- no `:`).
fn scan_label_name(bytes: &[u8], i: usize) -> Option<usize> {
    scan_name(bytes, i, false)
}

fn scan_name(bytes: &[u8], i: usize, colon_ok: bool) -> Option<usize> {
    let first = *bytes.get(i)?;
    if !(first.is_ascii_alphabetic() || first == b'_' || (colon_ok && first == b':')) {
        return None;
    }
    let mut end = i + 1;
    while let Some(b) = bytes.get(end) {
        if b.is_ascii_alphanumeric() || *b == b'_' || (colon_ok && *b == b':') {
            end += 1;
        } else {
            break;
        }
    }
    Some(end)
}

fn skip_ws(bytes: &[u8], mut i: usize) -> usize {
    while matches!(bytes.get(i), Some(b' ') | Some(b'\t')) {
        i += 1;
    }
    i
}

fn token_end(bytes: &[u8], mut i: usize) -> usize {
    while let Some(b) = bytes.get(i) {
        if *b == b' ' || *b == b'\t' {
            break;
        }
        i += 1;
    }
    i
}

fn trim(mut bytes: &[u8]) -> &[u8] {
    while matches!(bytes.first(), Some(b' ') | Some(b'\t')) {
        bytes = &bytes[1..];
    }
    while matches!(bytes.last(), Some(b' ') | Some(b'\t') | Some(b'\r')) {
        bytes = &bytes[..bytes.len() - 1];
    }
    bytes
}

/// A sample value: Rust's own `f64` grammar already covers Go's `ParseFloat` shapes plus
/// case-insensitive `nan`/`inf`/`infinity` with an optional sign, which is exactly what both
/// exposition formats allow.
fn parse_number(s: &str) -> Option<f64> {
    if s.is_empty() {
        return None;
    }
    s.parse::<f64>().ok()
}

/// A count sample (`_count`, `_gcount`, a bucket) as the `u64` the model holds. OpenMetrics writes
/// these as floats (`42.0`), and a `gaugehistogram`'s may genuinely be fractional -- rounded here,
/// since [`logit_core::Histogram`]'s bucket counts are integers.
fn count_value(v: f64) -> Option<u64> {
    if v.is_finite() && v >= 0.0 {
        Some(v.round() as u64)
    } else {
        None
    }
}

/// A sample timestamp, or an OpenMetrics `_created` value. The plain `[SIGN] DIGIT+ ["." DIGIT*]`
/// form -- everything a real exposition emits -- goes through [`parse_decimal_nanos`] digit by
/// digit, because an epoch-nanosecond instant needs 19 significant digits and an `f64` holds
/// 15-16: a float round trip would silently move the sample in time. OpenMetrics' `realnumber`
/// production also permits an exponent (`1.605281325e9`), which has no digit-exact reading at all,
/// so that form -- and only that form -- falls back to `f64`, accepting its ~1µs resolution at
/// epoch magnitude rather than rejecting a legal timestamp. `scale` comes from the dialect
/// (milliseconds in text 0.0.4, seconds in OpenMetrics).
fn parse_timestamp(s: &str, dialect: Dialect) -> Option<i64> {
    let scale = dialect.timestamp_scale();
    let (negative, digits) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    if let Some(nanos) = parse_decimal_nanos(digits, scale) {
        return Some(if negative { -nanos } else { nanos });
    }
    let value: f64 = digits.parse().ok()?;
    if !value.is_finite() {
        return None;
    }
    let nanos = value * scale as f64;
    if nanos.abs() >= i64::MAX as f64 {
        return None;
    }
    let nanos = nanos.round() as i64;
    Some(if negative { -nanos } else { nanos })
}

/// An OpenMetrics `_created` value: always decimal *seconds*, in both dialects (text 0.0.4 has no
/// `_created` of its own, so a `_created` line found there is read the same way), parsed from the
/// sample's own digits rather than from the already-parsed `f64` -- see [`parse_timestamp`].
fn parse_created(text: &str) -> Option<i64> {
    parse_timestamp(text, Dialect::OpenMetrics1_0)
}

// -------------------------------------------------------------------------------------------------
// Writing
// -------------------------------------------------------------------------------------------------

/// Renders `families` into `out` (appended, never cleared), deterministically -- see the module
/// doc's "Writing is deterministic" note. Infallible: every family is representable in both
/// dialects, if only as its nearest text 0.0.4 shape.
///
/// Counters go nowhere: this is the no-telemetry convenience over [`write_with`], the mirror of
/// [`parse`]'s relationship to [`parse_with`]. A sink renders through `write_with` so the two
/// OpenMetrics-only degradations this pass can make -- an unusable `# UNIT`, an exemplar with no line
/// to sit on -- are counted rather than silent.
pub fn write(families: &[MetricFamily], dialect: Dialect, out: &mut Vec<u8>) {
    write_with(families, dialect, out, &mut PrometheusEncoder::new());
}

/// [`write`], counting what it has to drop on `encoder`'s telemetry:
/// `logit.output.metrics.degraded{reason="unit_not_suffix"|"exemplar_dropped"}`. Text 0.0.4's
/// wholesale exemplar and `_created`/`# UNIT` drops are *not* counted -- those are the operator's
/// dialect choice, listed with the permitted normalizations rather than as lossy mappings.
pub fn write_with(
    families: &[MetricFamily],
    dialect: Dialect,
    out: &mut Vec<u8>,
    encoder: &mut PrometheusEncoder,
) {
    let mut order: Vec<&MetricFamily> = families.iter().collect();
    order.sort_by(|a, b| a.name.cmp(&b.name));
    let mut writer = Writer {
        out,
        dialect,
        encoder,
        number: String::new(),
        bound: String::new(),
        name: String::new(),
        base: String::new(),
        used: Vec::new(),
    };
    for family in order {
        writer.family(family);
    }
    if dialect.is_openmetrics() {
        writer.out.extend_from_slice(b"# EOF\n");
    }
}

/// The writer's reusable buffers, so rendering allocates per *family* (its `# TYPE` name) rather
/// than per line: `number`/`bound` hold formatted floats, `name` a suffixed sample name, `base` the
/// family's own `# TYPE` name, `used` the per-bucket exemplar claims.
struct Writer<'a> {
    out: &'a mut Vec<u8>,
    dialect: Dialect,
    encoder: &'a mut PrometheusEncoder,
    number: String,
    bound: String,
    name: String,
    base: String,
    used: Vec<bool>,
}

impl Writer<'_> {
    fn family(&mut self, family: &MetricFamily) {
        let om = self.dialect.is_openmetrics();
        self.base.clear();
        match (family.kind, om) {
            // A counter's value sample always ends in `_total`; only the family name differs.
            // OpenMetrics names the family without the suffix, text 0.0.4 with it.
            (FamilyType::Counter, true) => {
                // A counter named exactly `_total` is legal (`_` is a valid leading character), and
                // stripping the suffix there would leave a nameless `# TYPE  counter` line.
                let stripped = family.name.strip_suffix("_total").filter(|s| !s.is_empty());
                self.base.push_str(stripped.unwrap_or(&family.name));
            }
            (FamilyType::Counter, false) => {
                self.base.push_str(&family.name);
                if !family.name.ends_with("_total") {
                    self.base.push_str("_total");
                }
            }
            // Text 0.0.4 has no `info` type, so the family is named after the sample it renders as.
            (FamilyType::Info, false) => {
                self.base.push_str(&family.name);
                self.base.push_str("_info");
            }
            _ => self.base.push_str(&family.name),
        }
        let keyword = type_keyword(family.kind, self.dialect);
        // Prometheus's own output puts `# HELP` first; the OpenMetrics spec's examples put `# TYPE`
        // first, then `# UNIT`, then `# HELP`.
        if om {
            push_metadata(self.out, "TYPE", &self.base, keyword);
            if let Some(unit) = &family.unit {
                if unit_suffixes(&self.base, unit) {
                    push_metadata(self.out, "UNIT", &self.base, unit);
                } else {
                    self.encoder.degraded_reason("unit_not_suffix");
                }
            }
            if let Some(help) = &family.help {
                push_help(self.out, &self.base, help, self.dialect);
            }
        } else {
            if let Some(help) = &family.help {
                push_help(self.out, &self.base, help, self.dialect);
            }
            push_metadata(self.out, "TYPE", &self.base, keyword);
        }

        let mut series: Vec<&Series> = family.series.iter().collect();
        series.sort_by(|a, b| a.labels.cmp(&b.labels));
        for s in series {
            self.series(family.kind, s);
        }
    }

    fn series(&mut self, kind: FamilyType, series: &Series) {
        let om = self.dialect.is_openmetrics();
        match &series.point {
            Point::Counter(v) => {
                // `self.base` already ends in `_total` in text 0.0.4 (the family name carries it
                // there); OpenMetrics keeps the suffix off the family and on the sample.
                self.suffixed(if om { "_total" } else { "" });
                let exemplar = series.exemplars.first();
                if om {
                    // One line, so one exemplar: OpenMetrics has nowhere to put the rest.
                    for _ in series.exemplars.iter().skip(1) {
                        self.encoder.degraded_reason("exemplar_dropped");
                    }
                }
                self.sample(&series.labels, None, *v, series, exemplar);
                self.created(series);
            }
            Point::Gauge(v) | Point::Unknown(v) => {
                self.drop_exemplars(series);
                self.suffixed("");
                self.sample(&series.labels, None, *v, series, None);
            }
            Point::Info => {
                self.drop_exemplars(series);
                self.suffixed(if om { "_info" } else { "" });
                self.sample(&series.labels, None, 1.0, series, None);
            }
            Point::StateSet(on) => {
                self.drop_exemplars(series);
                self.suffixed("");
                self.sample(&series.labels, None, if *on { 1.0 } else { 0.0 }, series, None);
            }
            Point::Histogram { buckets, sum, count } => {
                let gauge_histogram = om && kind == FamilyType::GaugeHistogram;
                self.used.clear();
                self.used.resize(series.exemplars.len(), false);
                let mut previous = f64::NEG_INFINITY;
                for (le, cumulative) in buckets {
                    self.bound.clear();
                    push_float_str(&mut self.bound, *le);
                    let exemplar = self.claim_exemplar(series, previous, *le);
                    previous = *le;
                    self.suffixed("_bucket");
                    self.count_sample(&series.labels, Some("le"), *cumulative, series, exemplar);
                }
                if om {
                    // At most one exemplar per bucket (OpenMetrics), each on the bucket its own
                    // value falls in: a second one in the same range, or one whose value is `NaN`,
                    // has no line left to sit on.
                    let unplaced = self.used.iter().filter(|claimed| !**claimed).count();
                    for _ in 0..unplaced {
                        self.encoder.degraded_reason("exemplar_dropped");
                    }
                }
                // A gaugehistogram's `_gcount` exists if and only if its `_gsum` does
                // (OpenMetrics); an ordinary histogram always reports `_count`, which is what every
                // scraper expects to find.
                if let Some(sum) = sum {
                    self.suffixed(if gauge_histogram { "_gsum" } else { "_sum" });
                    self.sample(&series.labels, None, *sum, series, None);
                }
                if sum.is_some() || !gauge_histogram {
                    self.suffixed(if gauge_histogram { "_gcount" } else { "_count" });
                    self.count_sample(&series.labels, None, *count, series, None);
                }
                self.created(series);
            }
            Point::Summary { quantiles, sum, count } => {
                // A summary has no line OpenMetrics allows an exemplar on -- only `_total` and
                // `_bucket` carry them -- which is the same structural reason OTLP's
                // `SummaryDataPoint` has no exemplars field at all.
                self.drop_exemplars(series);
                for (q, v) in quantiles {
                    self.bound.clear();
                    push_float_str(&mut self.bound, *q);
                    self.suffixed("");
                    self.sample(&series.labels, Some("quantile"), *v, series, None);
                }
                if let Some(sum) = sum {
                    self.suffixed("_sum");
                    self.sample(&series.labels, None, *sum, series, None);
                }
                if let Some(count) = count {
                    self.suffixed("_count");
                    self.count_sample(&series.labels, None, *count, series, None);
                }
                self.created(series);
            }
        }
    }

    /// Counts every exemplar on a series whose point type has no line that can carry one:
    /// OpenMetrics permits exemplars on `_total` and `_bucket` samples only, so a gauge's, an
    /// `info`'s, a `stateset`'s or a summary's are dropped -- and `events_to_families` attaches
    /// whatever the record carried regardless of kind, so an `otlp_in`-sourced `Gauge` or `Summary`
    /// really does arrive here with exemplars on it. Text 0.0.4 drops every exemplar anyway, which
    /// is the operator's dialect choice and deliberately uncounted.
    fn drop_exemplars(&mut self, series: &Series) {
        if !self.dialect.is_openmetrics() {
            return;
        }
        for _ in &series.exemplars {
            self.encoder.degraded_reason("exemplar_dropped");
        }
    }

    /// Loads `self.name` with this family's base name plus `suffix` -- the sample name the next
    /// line uses.
    fn suffixed(&mut self, suffix: &str) {
        self.name.clear();
        self.name.push_str(&self.base);
        self.name.push_str(suffix);
    }

    /// One float-valued sample line. `extra_name` is the generated label (`le`/`quantile`) whose
    /// value is already formatted into `self.bound`.
    fn sample(
        &mut self,
        labels: &[(String, String)],
        extra_name: Option<&'static str>,
        value: f64,
        series: &Series,
        exemplar: Option<&Exemplar>,
    ) {
        self.open(labels, extra_name);
        self.number.clear();
        push_float_str(&mut self.number, value);
        self.out.extend_from_slice(self.number.as_bytes());
        self.trailer(series, exemplar);
    }

    /// One integer-valued sample line (a bucket, `_count`, `_gcount`): counts are `u64` in the
    /// model and both formats accept an integer literal where they ask for a float.
    fn count_sample(
        &mut self,
        labels: &[(String, String)],
        extra_name: Option<&'static str>,
        count: u64,
        series: &Series,
        exemplar: Option<&Exemplar>,
    ) {
        self.open(labels, extra_name);
        push_count(self.out, count);
        self.trailer(series, exemplar);
    }

    /// `<name>{<labels>[,<extra>]} ` -- everything up to and including the space before the value.
    fn open(&mut self, labels: &[(String, String)], extra_name: Option<&'static str>) {
        self.out.extend_from_slice(self.name.as_bytes());
        if !labels.is_empty() || extra_name.is_some() {
            self.out.push(b'{');
            for (i, (key, value)) in labels.iter().enumerate() {
                if i > 0 {
                    self.out.push(b',');
                }
                push_label(self.out, key, value);
            }
            // The generated label goes last rather than in sorted position: that is where
            // Prometheus's own exposition puts `le`, and the parser strips it wherever it appears.
            if let Some(key) = extra_name {
                if !labels.is_empty() {
                    self.out.push(b',');
                }
                push_label(self.out, key, &self.bound);
            }
            self.out.push(b'}');
        }
        self.out.push(b' ');
    }

    /// The trailing `[ <timestamp>][ # <exemplar>]` plus the newline -- shared by every sample line
    /// so the dialect rules live in exactly one place.
    fn trailer(&mut self, series: &Series, exemplar: Option<&Exemplar>) {
        if let Some(ts) = series.timestamp {
            self.out.push(b' ');
            self.number.clear();
            push_instant(&mut self.number, ts, self.dialect);
            self.out.extend_from_slice(self.number.as_bytes());
        }
        if self.dialect.is_openmetrics() {
            if exemplar.is_some_and(|e| !exemplar_fits(e)) {
                // Over OpenMetrics' 128-code-point exemplar label budget: emitting it would make the
                // line invalid, and truncating a trace id would make it a lie.
                self.encoder.degraded_reason("exemplar_dropped");
            }
            if let Some(exemplar) = exemplar.filter(|e| exemplar_fits(e)) {
                self.out.extend_from_slice(b" # ");
                push_exemplar_labels(self.out, exemplar);
                self.out.push(b' ');
                self.number.clear();
                push_float_str(&mut self.number, exemplar.value);
                self.out.extend_from_slice(self.number.as_bytes());
                if exemplar.timestamp != 0 {
                    self.out.push(b' ');
                    self.number.clear();
                    push_instant(&mut self.number, exemplar.timestamp, Dialect::OpenMetrics1_0);
                    self.out.extend_from_slice(self.number.as_bytes());
                }
            }
        }
        self.out.push(b'\n');
    }

    /// The OpenMetrics-only `<base>_created` line.
    fn created(&mut self, series: &Series) {
        if !self.dialect.is_openmetrics() {
            return;
        }
        let Some(created) = series.created else { return };
        self.suffixed("_created");
        self.open(&series.labels, None);
        self.number.clear();
        push_instant(&mut self.number, created, Dialect::OpenMetrics1_0);
        self.out.extend_from_slice(self.number.as_bytes());
        self.out.push(b'\n');
    }

    /// The first not-yet-placed exemplar whose value falls in `(previous, le]` -- OpenMetrics allows
    /// at most one per bucket, and an exemplar belongs on the bucket its own value lands in.
    fn claim_exemplar<'e>(
        &mut self,
        series: &'e Series,
        previous: f64,
        le: f64,
    ) -> Option<&'e Exemplar> {
        for (i, exemplar) in series.exemplars.iter().enumerate() {
            if self.used.get(i).copied().unwrap_or(true) || exemplar.value.is_nan() {
                continue;
            }
            if exemplar.value > previous && exemplar.value <= le {
                self.used[i] = true;
                return Some(exemplar);
            }
        }
        None
    }
}

/// Whether `# UNIT <name> <unit>` is legal for this family name. OpenMetrics 1.0: "an underscore
/// and the unit MUST be the suffix of the MetricFamily name", and Prometheus's own OpenMetrics
/// parser fails the *entire* scrape when it isn't (`unit %q not a suffix of metric %q`) -- so a unit
/// that doesn't fit, or one carrying anything outside `[a-zA-Z0-9_]` (`{requests}`, which would also
/// break the line grammar), is dropped rather than emitted. Appending the unit to the metric name
/// instead, the way Prometheus's own OTLP translation does, is a follow-up rather than something to
/// do silently here.
fn unit_suffixes(name: &str, unit: &str) -> bool {
    if unit.is_empty() || !unit.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        return false;
    }
    // `>`, not `> unit.len() + 1`: a family named exactly `_<unit>` satisfies the rule, and
    // Prometheus's own check rejects only `len(name) < len(unit) + 1`.
    name.len() > unit.len()
        && name.as_bytes()[name.len() - unit.len() - 1] == b'_'
        && name.ends_with(unit)
}

/// The `# TYPE` keyword: this family's own type in OpenMetrics, its nearest text 0.0.4 equivalent
/// otherwise.
fn type_keyword(kind: FamilyType, dialect: Dialect) -> &'static str {
    if dialect.is_openmetrics() {
        match kind {
            FamilyType::Untyped => FamilyType::Unknown.as_str(),
            other => other.as_str(),
        }
    } else {
        match kind {
            FamilyType::Counter => "counter",
            // `info` and `stateset` are both a plain `1`/`0`-valued gauge in text 0.0.4, which is
            // what Prometheus's own OpenMetrics-to-text degradation produces.
            FamilyType::Gauge | FamilyType::Info | FamilyType::StateSet => "gauge",
            FamilyType::Histogram | FamilyType::GaugeHistogram => "histogram",
            FamilyType::Summary => "summary",
            FamilyType::Unknown | FamilyType::Untyped => "untyped",
        }
    }
}

fn push_metadata(out: &mut Vec<u8>, keyword: &str, name: &str, value: &str) {
    out.extend_from_slice(b"# ");
    out.extend_from_slice(keyword.as_bytes());
    out.push(b' ');
    out.extend_from_slice(name.as_bytes());
    out.push(b' ');
    out.extend_from_slice(value.as_bytes());
    out.push(b'\n');
}

fn push_help(out: &mut Vec<u8>, name: &str, help: &str, dialect: Dialect) {
    out.extend_from_slice(b"# HELP ");
    out.extend_from_slice(name.as_bytes());
    out.push(b' ');
    push_escaped_help(out, help, dialect);
    out.push(b'\n');
}

fn push_label(out: &mut Vec<u8>, key: &str, value: &str) {
    out.extend_from_slice(key.as_bytes());
    out.extend_from_slice(b"=\"");
    push_escaped_label(out, value);
    out.push(b'"');
}

/// OpenMetrics caps an exemplar's label set at 128 UTF-8 code points, names and values together.
/// One that doesn't fit is dropped rather than emitted invalid.
fn exemplar_fits(exemplar: &Exemplar) -> bool {
    let mut total = 0usize;
    if let Some(trace) = &exemplar.trace {
        total += "trace_id".len() + trace.trace_id.len() * 2;
        if let Some(span) = trace.span_id {
            total += "span_id".len() + span.len() * 2;
        }
    }
    for (key, value) in exemplar.filtered_attributes.iter() {
        if let Some(rendered) = super::label_value(value) {
            total += resolve(key).chars().count() + rendered.chars().count();
        }
    }
    total <= 128
}

/// `{trace_id="...",span_id="...",<other>}`, sorted by label name -- a [`logit_core::TraceRef`]
/// becomes the two id labels it arrived as, and every other exemplar attribute renders like any
/// label (an unrepresentable one is dropped, the way the encode side's own labels are).
fn push_exemplar_labels(out: &mut Vec<u8>, exemplar: &Exemplar) {
    let mut labels: Vec<(String, String)> = Vec::new();
    if let Some(trace) = &exemplar.trace {
        labels.push(("trace_id".to_string(), to_hex(&trace.trace_id)));
        if let Some(span) = trace.span_id {
            labels.push(("span_id".to_string(), to_hex(&span)));
        }
    }
    for (key, value) in exemplar.filtered_attributes.iter() {
        if let Some(rendered) = super::label_value(value) {
            labels.push((resolve(key).to_string(), rendered));
        }
    }
    labels.sort_by(|a, b| a.0.cmp(&b.0));
    out.push(b'{');
    for (i, (key, value)) in labels.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        push_label(out, key, value);
    }
    out.push(b'}');
}

/// A unix-nanosecond instant in this dialect's own unit: integer milliseconds for text 0.0.4,
/// decimal seconds (trailing zeros trimmed) for OpenMetrics.
fn push_instant(buf: &mut String, nanos: i64, dialect: Dialect) {
    match dialect {
        Dialect::Text0_0_4 => {
            let _ = write!(buf, "{}", nanos / 1_000_000);
        }
        Dialect::OpenMetrics1_0 => {
            if nanos < 0 {
                buf.push('-');
            }
            let abs = nanos.unsigned_abs();
            let _ = write!(buf, "{}", abs / 1_000_000_000);
            let frac = abs % 1_000_000_000;
            if frac != 0 {
                buf.push('.');
                let _ = write!(buf, "{frac:09}");
                while buf.ends_with('0') {
                    buf.pop();
                }
            }
        }
    }
}

/// A `u64` count, without the formatting machinery `write!` would drag in per line.
fn push_count(out: &mut Vec<u8>, count: u64) {
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    let mut n = count;
    loop {
        i -= 1;
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    out.extend_from_slice(&buf[i..]);
}

/// A float in exposition spelling: `NaN`/`+Inf`/`-Inf` (Rust's own `NaN`/`inf`/`-inf` is not valid
/// exposition), otherwise shortest-round-trip decimal.
fn push_float_str(buf: &mut String, v: f64) {
    if v.is_nan() {
        buf.push_str("NaN");
    } else if v == f64::INFINITY {
        buf.push_str("+Inf");
    } else if v == f64::NEG_INFINITY {
        buf.push_str("-Inf");
    } else {
        let _ = write!(buf, "{v}");
    }
}

fn push_escaped_label(out: &mut Vec<u8>, value: &str) {
    for b in value.bytes() {
        match b {
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'"' => out.extend_from_slice(b"\\\""),
            b'\n' => out.extend_from_slice(b"\\n"),
            other => out.push(other),
        }
    }
}

/// `# HELP` escaping: backslash and line feed in both dialects, plus the double quote in
/// OpenMetrics, whose `escaped-string` production has no room for a bare one.
fn push_escaped_help(out: &mut Vec<u8>, help: &str, dialect: Dialect) {
    for b in help.bytes() {
        match b {
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'"' if dialect.is_openmetrics() => out.extend_from_slice(b"\\\""),
            other => out.push(other),
        }
    }
}

/// The inverse of [`push_escaped_help`].
fn unescape_help(help: &str, dialect: Dialect) -> String {
    let bytes = help.as_bytes();
    let mut out = String::with_capacity(help.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' {
            match bytes.get(i + 1) {
                Some(b'n') => {
                    out.push('\n');
                    i += 2;
                    continue;
                }
                Some(b'\\') => {
                    out.push('\\');
                    i += 2;
                    continue;
                }
                Some(b'"') if dialect.is_openmetrics() => {
                    out.push('"');
                    i += 2;
                    continue;
                }
                _ => {}
            }
        }
        push_char_at(&mut out, help, i);
        i += char_len(bytes, i);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::telemetry::Registry;
    use logit_core::Telemetry;
    use std::sync::Arc;

    /// The complete example from <https://prometheus.io/docs/instrumenting/exposition_formats/>,
    /// minus its two Prometheus-3 quoted-UTF-8-name lines (their own test below covers those).
    const EXPOSITION_FORMAT_EXAMPLE: &str = concat!(
        "# HELP http_requests_total The total number of HTTP requests.\n",
        "# TYPE http_requests_total counter\n",
        "http_requests_total{method=\"post\",code=\"200\"} 1027 1395066363000\n",
        "http_requests_total{method=\"post\",code=\"400\"}    3 1395066363000\n",
        "\n",
        "# Escaping in label values:\n",
        "msdos_file_access_time_seconds{path=\"C:\\\\DIR\\\\FILE.TXT\",error=\"Cannot find \
         file:\\n\\\"FILE.TXT\\\"\"} 1.458255915e9\n",
        "\n",
        "# Minimalistic line:\n",
        "metric_without_timestamp_and_labels 12.47\n",
        "\n",
        "# A weird metric from before the epoch:\n",
        "something_weird{problem=\"division by zero\"} +Inf -3982045\n",
        "\n",
        "# A histogram, which has a pretty complex representation in the text format:\n",
        "# HELP http_request_duration_seconds A histogram of the request duration.\n",
        "# TYPE http_request_duration_seconds histogram\n",
        "http_request_duration_seconds_bucket{le=\"0.05\"} 24054\n",
        "http_request_duration_seconds_bucket{le=\"0.1\"} 33444\n",
        "http_request_duration_seconds_bucket{le=\"0.2\"} 100392\n",
        "http_request_duration_seconds_bucket{le=\"0.5\"} 129389\n",
        "http_request_duration_seconds_bucket{le=\"1\"} 133988\n",
        "http_request_duration_seconds_bucket{le=\"+Inf\"} 144320\n",
        "http_request_duration_seconds_sum 53423\n",
        "http_request_duration_seconds_count 144320\n",
        "\n",
        "# Finally a summary, which has a complex representation, too:\n",
        "# HELP rpc_duration_seconds A summary of the RPC duration in seconds.\n",
        "# TYPE rpc_duration_seconds summary\n",
        "rpc_duration_seconds{quantile=\"0.01\"} 3102\n",
        "rpc_duration_seconds{quantile=\"0.05\"} 3272\n",
        "rpc_duration_seconds{quantile=\"0.5\"} 4773\n",
        "rpc_duration_seconds{quantile=\"0.9\"} 9001\n",
        "rpc_duration_seconds{quantile=\"0.99\"} 76656\n",
        "rpc_duration_seconds_sum 1.7560473e+07\n",
        "rpc_duration_seconds_count 2693\n",
    );

    /// That example's canonical form: families by name, series by label set, labels by name, floats
    /// shortest-round-trip, comments and blank lines gone, `# TYPE ... untyped` supplied for the
    /// three families that arrived with no metadata.
    const EXPOSITION_FORMAT_CANONICAL: &str = concat!(
        "# HELP http_request_duration_seconds A histogram of the request duration.\n",
        "# TYPE http_request_duration_seconds histogram\n",
        "http_request_duration_seconds_bucket{le=\"0.05\"} 24054\n",
        "http_request_duration_seconds_bucket{le=\"0.1\"} 33444\n",
        "http_request_duration_seconds_bucket{le=\"0.2\"} 100392\n",
        "http_request_duration_seconds_bucket{le=\"0.5\"} 129389\n",
        "http_request_duration_seconds_bucket{le=\"1\"} 133988\n",
        "http_request_duration_seconds_bucket{le=\"+Inf\"} 144320\n",
        "http_request_duration_seconds_sum 53423\n",
        "http_request_duration_seconds_count 144320\n",
        "# HELP http_requests_total The total number of HTTP requests.\n",
        "# TYPE http_requests_total counter\n",
        "http_requests_total{code=\"200\",method=\"post\"} 1027 1395066363000\n",
        "http_requests_total{code=\"400\",method=\"post\"} 3 1395066363000\n",
        "# TYPE metric_without_timestamp_and_labels untyped\n",
        "metric_without_timestamp_and_labels 12.47\n",
        "# TYPE msdos_file_access_time_seconds untyped\n",
        "msdos_file_access_time_seconds{error=\"Cannot find file:\\n\\\"FILE.TXT\\\"\",\
         path=\"C:\\\\DIR\\\\FILE.TXT\"} 1458255915\n",
        "# HELP rpc_duration_seconds A summary of the RPC duration in seconds.\n",
        "# TYPE rpc_duration_seconds summary\n",
        "rpc_duration_seconds{quantile=\"0.01\"} 3102\n",
        "rpc_duration_seconds{quantile=\"0.05\"} 3272\n",
        "rpc_duration_seconds{quantile=\"0.5\"} 4773\n",
        "rpc_duration_seconds{quantile=\"0.9\"} 9001\n",
        "rpc_duration_seconds{quantile=\"0.99\"} 76656\n",
        "rpc_duration_seconds_sum 17560473\n",
        "rpc_duration_seconds_count 2693\n",
        "# TYPE something_weird untyped\n",
        "something_weird{problem=\"division by zero\"} +Inf -3982045\n",
    );

    /// The OpenMetrics specification's own complete exposition example (its "Example" section).
    const OPENMETRICS_EXAMPLE: &str = concat!(
        "# TYPE acme_http_router_request_seconds summary\n",
        "# UNIT acme_http_router_request_seconds seconds\n",
        "# HELP acme_http_router_request_seconds Latency though all of ACME's HTTP request \
         router.\n",
        "acme_http_router_request_seconds_sum{path=\"/api/v1\",method=\"GET\"} 9036.32\n",
        "acme_http_router_request_seconds_count{path=\"/api/v1\",method=\"GET\"} 807283.0\n",
        "acme_http_router_request_seconds_created{path=\"/api/v1\",method=\"GET\"} 1605281325.0\n",
        "acme_http_router_request_seconds_sum{path=\"/api/v2\",method=\"POST\"} 479.3\n",
        "acme_http_router_request_seconds_count{path=\"/api/v2\",method=\"POST\"} 34.0\n",
        "acme_http_router_request_seconds_created{path=\"/api/v2\",method=\"POST\"} 1605281325.0\n",
        "# TYPE go_goroutines gauge\n",
        "# HELP go_goroutines Number of goroutines that currently exist.\n",
        "go_goroutines 69\n",
        "# TYPE process_cpu_seconds counter\n",
        "# UNIT process_cpu_seconds seconds\n",
        "# HELP process_cpu_seconds Total user and system CPU time spent in seconds.\n",
        "process_cpu_seconds_total 4.20072246e+06\n",
        "# EOF\n",
    );

    const OPENMETRICS_CANONICAL: &str = concat!(
        "# TYPE acme_http_router_request_seconds summary\n",
        "# UNIT acme_http_router_request_seconds seconds\n",
        "# HELP acme_http_router_request_seconds Latency though all of ACME's HTTP request \
         router.\n",
        "acme_http_router_request_seconds_sum{method=\"GET\",path=\"/api/v1\"} 9036.32\n",
        "acme_http_router_request_seconds_count{method=\"GET\",path=\"/api/v1\"} 807283\n",
        "acme_http_router_request_seconds_created{method=\"GET\",path=\"/api/v1\"} 1605281325\n",
        "acme_http_router_request_seconds_sum{method=\"POST\",path=\"/api/v2\"} 479.3\n",
        "acme_http_router_request_seconds_count{method=\"POST\",path=\"/api/v2\"} 34\n",
        "acme_http_router_request_seconds_created{method=\"POST\",path=\"/api/v2\"} 1605281325\n",
        "# TYPE go_goroutines gauge\n",
        "# HELP go_goroutines Number of goroutines that currently exist.\n",
        "go_goroutines 69\n",
        "# TYPE process_cpu_seconds counter\n",
        "# UNIT process_cpu_seconds seconds\n",
        "# HELP process_cpu_seconds Total user and system CPU time spent in seconds.\n",
        "process_cpu_seconds_total 4200722.46\n",
        "# EOF\n",
    );

    fn parsed(body: &str, dialect: Dialect) -> Vec<MetricFamily> {
        parse(body.as_bytes(), dialect).expect("body must parse")
    }

    fn written(families: &[MetricFamily], dialect: Dialect) -> String {
        let mut out = Vec::new();
        write(families, dialect, &mut out);
        String::from_utf8(out).expect("written exposition must be utf-8")
    }

    /// `write(parse(body)) == expected`, byte for byte.
    fn assert_canonical(body: &str, dialect: Dialect, expected: &str) {
        let families = parsed(body, dialect);
        assert_eq!(written(&families, dialect), expected);
    }

    fn telemetry() -> (Arc<Registry>, Telemetry) {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("prometheus", "prometheus_in", "source");
        (registry, telemetry)
    }

    /// Parses with counters live, returning whichever `logit.input.metrics.*` reasons fired.
    fn parse_reasons(body: &str, dialect: Dialect) -> (Vec<MetricFamily>, Vec<String>) {
        let (registry, telemetry) = telemetry();
        let mut decoder = PrometheusDecoder::new().with_telemetry(telemetry);
        let families = parse_with(body.as_bytes(), dialect, &mut decoder).expect("must parse");
        let reasons = registry
            .drain(0)
            .iter()
            .filter(|event| {
                event.metrics.iter().any(|m| {
                    let name = logit_core::interner::resolve(m.name);
                    name == "logit.input.metrics.skipped" || name == "logit.input.metrics.degraded"
                })
            })
            .filter_map(|event| {
                event.attributes.get("reason").and_then(|v| v.as_str()).map(str::to_string)
            })
            .collect();
        (families, reasons)
    }

    fn family_names(families: &[MetricFamily]) -> Vec<&str> {
        families.iter().map(|f| f.name.as_str()).collect()
    }

    // --- the two specifications' own examples ---------------------------------------------------

    #[test]
    fn the_exposition_format_documentations_example_parses_into_the_expected_families() {
        let families = parsed(EXPOSITION_FORMAT_EXAMPLE, Dialect::Text0_0_4);
        assert_eq!(
            family_names(&families),
            vec![
                "http_request_duration_seconds",
                "http_requests_total",
                "metric_without_timestamp_and_labels",
                "msdos_file_access_time_seconds",
                "rpc_duration_seconds",
                "something_weird",
            ]
        );

        let histogram = &families[0];
        assert_eq!(histogram.kind, FamilyType::Histogram);
        assert_eq!(histogram.series.len(), 1);
        assert_eq!(
            histogram.series[0].point,
            Point::Histogram {
                buckets: vec![
                    (0.05, 24054),
                    (0.1, 33444),
                    (0.2, 100392),
                    (0.5, 129389),
                    (1.0, 133988),
                    (f64::INFINITY, 144320),
                ],
                sum: Some(53423.0),
                count: 144320,
            }
        );

        let counter = &families[1];
        assert_eq!(counter.kind, FamilyType::Counter);
        assert_eq!(counter.help.as_deref(), Some("The total number of HTTP requests."));
        assert_eq!(counter.series[0].point, Point::Counter(1027.0));
        assert_eq!(counter.series[0].timestamp, Some(1_395_066_363_000_000_000));

        // A family with no `# TYPE` at all is `untyped` in text 0.0.4.
        assert_eq!(families[2].kind, FamilyType::Untyped);
        assert_eq!(families[2].series[0].point, Point::Unknown(12.47));

        // Escapes, unescaped: a literal backslash in the path, a newline and quotes in the error.
        assert_eq!(
            families[3].series[0].labels,
            vec![
                ("error".to_string(), "Cannot find file:\n\"FILE.TXT\"".to_string()),
                ("path".to_string(), "C:\\DIR\\FILE.TXT".to_string()),
            ]
        );

        assert_eq!(families[4].kind, FamilyType::Summary);
        assert_eq!(
            families[4].series[0].point,
            Point::Summary {
                quantiles: vec![
                    (0.01, 3102.0),
                    (0.05, 3272.0),
                    (0.5, 4773.0),
                    (0.9, 9001.0),
                    (0.99, 76656.0)
                ],
                sum: Some(1.7560473e7),
                count: Some(2693),
            }
        );

        // `+Inf` with a negative (pre-epoch) millisecond timestamp.
        assert_eq!(families[5].series[0].point, Point::Unknown(f64::INFINITY));
        assert_eq!(families[5].series[0].timestamp, Some(-3_982_045_000_000));
    }

    #[test]
    fn the_exposition_format_documentations_example_rewrites_to_its_canonical_form() {
        assert_canonical(
            EXPOSITION_FORMAT_EXAMPLE,
            Dialect::Text0_0_4,
            EXPOSITION_FORMAT_CANONICAL,
        );
    }

    /// Writing is idempotent: the canonical form is a fixed point of `write(parse(..))`, not merely
    /// something the first pass happens to produce.
    #[test]
    fn the_canonical_text_form_is_a_fixed_point() {
        assert_canonical(
            EXPOSITION_FORMAT_CANONICAL,
            Dialect::Text0_0_4,
            EXPOSITION_FORMAT_CANONICAL,
        );
    }

    #[test]
    fn the_openmetrics_specs_example_parses_into_the_expected_families() {
        let families = parsed(OPENMETRICS_EXAMPLE, Dialect::OpenMetrics1_0);
        assert_eq!(
            family_names(&families),
            vec![
                "acme_http_router_request_seconds",
                "go_goroutines",
                // The model keeps the counter's *sample* name, `_total` included.
                "process_cpu_seconds_total",
            ]
        );
        assert_eq!(families[0].unit.as_deref(), Some("seconds"));
        assert_eq!(families[0].series.len(), 2);
        assert_eq!(
            families[0].series[0].point,
            Point::Summary { quantiles: vec![], sum: Some(9036.32), count: Some(807283) }
        );
        assert_eq!(families[0].series[0].created, Some(1_605_281_325_000_000_000));
        assert_eq!(families[2].kind, FamilyType::Counter);
        assert_eq!(families[2].series[0].point, Point::Counter(4_200_722.46));
    }

    #[test]
    fn the_openmetrics_specs_example_rewrites_to_its_canonical_form() {
        assert_canonical(OPENMETRICS_EXAMPLE, Dialect::OpenMetrics1_0, OPENMETRICS_CANONICAL);
    }

    #[test]
    fn the_canonical_openmetrics_form_is_a_fixed_point() {
        assert_canonical(OPENMETRICS_CANONICAL, Dialect::OpenMetrics1_0, OPENMETRICS_CANONICAL);
    }

    /// The OpenMetrics spec's histogram-with-exemplars example, including an exemplar with an empty
    /// label set and two whose `trace_id` is not hex at all (so it stays an ordinary exemplar
    /// attribute rather than becoming a trace reference).
    #[test]
    fn the_openmetrics_histogram_with_exemplars_example_round_trips() {
        let body = concat!(
            "# TYPE foo histogram\n",
            "foo_bucket{le=\"0.01\"} 0\n",
            "foo_bucket{le=\"0.1\"} 8 # {} 0.054\n",
            "foo_bucket{le=\"1\"} 11 # {trace_id=\"KOO5S4vxi0o\"} 0.67\n",
            "foo_bucket{le=\"10\"} 17 # {trace_id=\"oHg5SJYRHA0\"} 9.8 1520879607.789\n",
            "foo_bucket{le=\"+Inf\"} 17\n",
            "foo_count 17\n",
            "foo_sum 324789.3\n",
            "foo_created  1520430000.123\n",
            "# EOF\n",
        );
        let canonical = concat!(
            "# TYPE foo histogram\n",
            "foo_bucket{le=\"0.01\"} 0\n",
            "foo_bucket{le=\"0.1\"} 8 # {} 0.054\n",
            "foo_bucket{le=\"1\"} 11 # {trace_id=\"KOO5S4vxi0o\"} 0.67\n",
            "foo_bucket{le=\"10\"} 17 # {trace_id=\"oHg5SJYRHA0\"} 9.8 1520879607.789\n",
            "foo_bucket{le=\"+Inf\"} 17\n",
            "foo_sum 324789.3\n",
            "foo_count 17\n",
            "foo_created 1520430000.123\n",
            "# EOF\n",
        );
        let families = parsed(body, Dialect::OpenMetrics1_0);
        let series = &families[0].series[0];
        assert_eq!(series.exemplars.len(), 3);
        assert_eq!(series.exemplars[0].value, 0.054);
        assert!(series.exemplars[0].trace.is_none(), "an empty exemplar label set has no trace");
        assert!(
            series.exemplars[1].trace.is_none(),
            "`KOO5S4vxi0o` is not hex, so it stays an attribute"
        );
        assert_eq!(
            series.exemplars[1].filtered_attributes.get("trace_id"),
            Some(&Value::from("KOO5S4vxi0o"))
        );
        assert_eq!(series.exemplars[2].timestamp, 1_520_879_607_789_000_000);
        assert_eq!(series.created, Some(1_520_430_000_123_000_000));
        assert_eq!(written(&families, Dialect::OpenMetrics1_0), canonical);
    }

    #[test]
    fn an_exemplars_hex_trace_and_span_ids_become_a_trace_reference_and_are_consumed() {
        let body = concat!(
            "# TYPE foo counter\n",
            "foo_total 17 # {trace_id=\"0123456789abcdef0123456789abcdef\",\
             span_id=\"fedcba9876543210\",detail=\"kept\"} 0.67\n",
            "# EOF\n",
        );
        let families = parsed(body, Dialect::OpenMetrics1_0);
        let exemplar = &families[0].series[0].exemplars[0];
        let trace = exemplar.trace.expect("valid hex ids must become a TraceRef");
        assert_eq!(trace.trace_id[0], 0x01);
        assert_eq!(trace.span_id.unwrap()[0], 0xfe);
        assert_eq!(exemplar.filtered_attributes.get("trace_id"), None, "consumed");
        assert_eq!(exemplar.filtered_attributes.get("span_id"), None, "consumed");
        assert_eq!(exemplar.filtered_attributes.get("detail"), Some(&Value::from("kept")));
        // And back out again, with the exemplar's own labels sorted by name like every other label
        // set -- the two ids are rendered from the `TraceRef`, not carried through as attributes.
        assert_eq!(
            written(&families, Dialect::OpenMetrics1_0),
            concat!(
                "# TYPE foo counter\n",
                "foo_total 17 # {detail=\"kept\",span_id=\"fedcba9876543210\",\
                 trace_id=\"0123456789abcdef0123456789abcdef\"} 0.67\n",
                "# EOF\n",
            )
        );
    }

    #[test]
    fn the_openmetrics_gaugehistogram_example_round_trips_with_gsum_and_gcount() {
        let body = concat!(
            "# TYPE foo gaugehistogram\n",
            "foo_bucket{le=\"0.01\"} 20.0\n",
            "foo_bucket{le=\"0.1\"} 25.0\n",
            "foo_bucket{le=\"1\"} 34.0\n",
            "foo_bucket{le=\"10\"} 34.0\n",
            "foo_bucket{le=\"+Inf\"} 42.0\n",
            "foo_gcount 42.0\n",
            "foo_gsum 3289.3\n",
            "# EOF\n",
        );
        let canonical = concat!(
            "# TYPE foo gaugehistogram\n",
            "foo_bucket{le=\"0.01\"} 20\n",
            "foo_bucket{le=\"0.1\"} 25\n",
            "foo_bucket{le=\"1\"} 34\n",
            "foo_bucket{le=\"10\"} 34\n",
            "foo_bucket{le=\"+Inf\"} 42\n",
            "foo_gsum 3289.3\n",
            "foo_gcount 42\n",
            "# EOF\n",
        );
        let families = parsed(body, Dialect::OpenMetrics1_0);
        assert_eq!(families[0].kind, FamilyType::GaugeHistogram);
        assert_canonical(body, Dialect::OpenMetrics1_0, canonical);
    }

    #[test]
    fn the_openmetrics_info_examples_round_trip() {
        let body = concat!(
            "# TYPE foo info\n",
            "foo_info{name=\"pretty name\",version=\"8.2.7\"} 1\n",
            "# EOF\n",
        );
        let families = parsed(body, Dialect::OpenMetrics1_0);
        assert_eq!(families[0].name, "foo", "the `_info` suffix is the sample's, not the family's");
        assert_eq!(families[0].kind, FamilyType::Info);
        assert_eq!(families[0].series[0].point, Point::Info);
        assert_canonical(body, Dialect::OpenMetrics1_0, body);

        // The spec's `target_info` example, whose labels canonicalize into name order.
        let target = concat!(
            "# TYPE target info\n",
            "# HELP target Target metadata\n",
            "target_info{env=\"prod\",hostname=\"myhost\",datacenter=\"sdc\",region=\"europe\",\
             owner=\"frontend\"} 1\n",
            "# EOF\n",
        );
        let canonical = concat!(
            "# TYPE target info\n",
            "# HELP target Target metadata\n",
            "target_info{datacenter=\"sdc\",env=\"prod\",hostname=\"myhost\",owner=\"frontend\",\
             region=\"europe\"} 1\n",
            "# EOF\n",
        );
        assert_canonical(target, Dialect::OpenMetrics1_0, canonical);
    }

    #[test]
    fn the_openmetrics_stateset_example_round_trips_one_series_per_state() {
        let body = concat!(
            "# TYPE foo stateset\n",
            "foo{foo=\"a\"} 0\n",
            "foo{foo=\"bb\"} 1\n",
            "foo{foo=\"ccc\"} 0\n",
            "# EOF\n",
        );
        let families = parsed(body, Dialect::OpenMetrics1_0);
        assert_eq!(families[0].kind, FamilyType::StateSet);
        assert_eq!(families[0].series.len(), 3);
        assert_eq!(families[0].series[1].point, Point::StateSet(true));
        assert_canonical(body, Dialect::OpenMetrics1_0, body);
    }

    #[test]
    fn the_openmetrics_quantile_less_summary_example_round_trips() {
        let body = concat!(
            "# TYPE foo summary\n",
            "foo_count 17.0\n",
            "foo_sum 324789.3\n",
            "foo_created 1520430000.123\n",
            "# EOF\n",
        );
        let canonical = concat!(
            "# TYPE foo summary\n",
            "foo_sum 324789.3\n",
            "foo_count 17\n",
            "foo_created 1520430000.123\n",
            "# EOF\n",
        );
        assert_canonical(body, Dialect::OpenMetrics1_0, canonical);
    }

    #[test]
    fn the_openmetrics_unknown_example_parses_as_an_unknown_family() {
        let families = parsed("# TYPE foo unknown\nfoo 42.23\n# EOF\n", Dialect::OpenMetrics1_0);
        assert_eq!(families[0].kind, FamilyType::Unknown);
        assert_eq!(families[0].series[0].point, Point::Unknown(42.23));
    }

    // --- dialect differences ---------------------------------------------------------------------

    #[test]
    fn an_openmetrics_body_without_a_trailing_eof_is_malformed() {
        let err = parse(b"# TYPE foo gauge\nfoo 1\n", Dialect::OpenMetrics1_0).unwrap_err();
        assert!(matches!(err, CodecError::Malformed(_)), "got {err:?}");
        // The same body is perfectly good text 0.0.4.
        assert!(parse(b"# TYPE foo gauge\nfoo 1\n", Dialect::Text0_0_4).is_ok());
    }

    #[test]
    fn content_after_eof_is_malformed() {
        let body = b"# TYPE foo gauge\nfoo 1\n# EOF\nfoo 2\n";
        assert!(matches!(
            parse(body, Dialect::OpenMetrics1_0).unwrap_err(),
            CodecError::Malformed(_)
        ));
    }

    #[test]
    fn a_trailing_eof_in_text_0_0_4_is_just_a_comment() {
        let families = parsed("# TYPE foo gauge\nfoo 1\n# EOF\n", Dialect::Text0_0_4);
        assert_eq!(families.len(), 1);
    }

    #[test]
    fn a_timestamp_is_milliseconds_in_text_and_seconds_in_openmetrics() {
        let text = parsed("foo 1 1395066363000\n", Dialect::Text0_0_4);
        assert_eq!(text[0].series[0].timestamp, Some(1_395_066_363_000_000_000));
        let om = parsed("foo 1 1395066363\n# EOF\n", Dialect::OpenMetrics1_0);
        assert_eq!(om[0].series[0].timestamp, Some(1_395_066_363_000_000_000));

        assert_eq!(written(&text, Dialect::Text0_0_4), "# TYPE foo untyped\nfoo 1 1395066363000\n");
        assert_eq!(
            written(&text, Dialect::OpenMetrics1_0),
            "# TYPE foo unknown\nfoo 1 1395066363\n# EOF\n"
        );
    }

    /// A nanosecond-resolution instant survives an OpenMetrics round trip exactly -- the whole
    /// reason timestamps go through `parse_decimal_nanos` rather than an `f64`.
    #[test]
    fn an_openmetrics_timestamp_is_parsed_and_written_digit_exactly() {
        let families = parsed("foo 1 1520430000.123456789\n# EOF\n", Dialect::OpenMetrics1_0);
        assert_eq!(families[0].series[0].timestamp, Some(1_520_430_000_123_456_789));
        assert_eq!(
            written(&families, Dialect::OpenMetrics1_0),
            "# TYPE foo unknown\nfoo 1 1520430000.123456789\n# EOF\n"
        );
    }

    /// OpenMetrics' `realnumber` permits an exponent, which has no digit-exact reading -- that form
    /// alone falls back to `f64`.
    #[test]
    fn an_exponent_timestamp_falls_back_to_float_parsing() {
        let families = parsed("foo 1 1.6052813e9\n# EOF\n", Dialect::OpenMetrics1_0);
        assert_eq!(families[0].series[0].timestamp, Some(1_605_281_300_000_000_000));
    }

    #[test]
    fn special_floats_round_trip_in_both_directions() {
        let families =
            parsed("pos +Inf\nneg -Inf\nnan NaN\nlower inf\nupper NAN\n", Dialect::Text0_0_4);
        let values: Vec<f64> = families
            .iter()
            .map(|f| match f.series[0].point {
                Point::Unknown(v) => v,
                ref other => panic!("expected an unknown point, got {other:?}"),
            })
            .collect();
        // Families are sorted by name: lower, nan, neg, pos, upper.
        assert_eq!(values[0], f64::INFINITY);
        assert!(values[1].is_nan());
        assert_eq!(values[2], f64::NEG_INFINITY);
        assert_eq!(values[3], f64::INFINITY);
        assert!(values[4].is_nan());
        let text = written(&families, Dialect::Text0_0_4);
        assert!(text.contains("pos +Inf\n"), "{text}");
        assert!(text.contains("neg -Inf\n"), "{text}");
        assert!(text.contains("nan NaN\n"), "{text}");
    }

    #[test]
    fn a_counters_value_sample_gains_total_in_both_dialects_and_the_om_family_loses_it() {
        // A text 0.0.4 counter family that never carried the suffix.
        let families = parsed("# TYPE foo counter\nfoo 1\n", Dialect::Text0_0_4);
        assert_eq!(families[0].name, "foo", "decode keeps the wire name verbatim");
        assert_eq!(
            written(&families, Dialect::Text0_0_4),
            "# TYPE foo_total counter\nfoo_total 1\n"
        );
        assert_eq!(
            written(&families, Dialect::OpenMetrics1_0),
            "# TYPE foo counter\nfoo_total 1\n# EOF\n"
        );

        // One that did: the suffix is not doubled, and OpenMetrics strips it off the family.
        let families = parsed("# TYPE foo_total counter\nfoo_total 1\n", Dialect::Text0_0_4);
        assert_eq!(
            written(&families, Dialect::Text0_0_4),
            "# TYPE foo_total counter\nfoo_total 1\n"
        );
        assert_eq!(
            written(&families, Dialect::OpenMetrics1_0),
            "# TYPE foo counter\nfoo_total 1\n# EOF\n"
        );
    }

    /// A counter declared as `# TYPE foo counter` whose sample is `foo_total` -- what an
    /// OpenMetrics-shaped client library emits even in text 0.0.4.
    #[test]
    fn a_text_counter_sample_may_carry_the_total_suffix_the_type_line_lacks() {
        let families = parsed("# TYPE foo counter\nfoo_total 7\n", Dialect::Text0_0_4);
        assert_eq!(families[0].name, "foo_total");
        assert_eq!(families[0].series[0].point, Point::Counter(7.0));
    }

    #[test]
    fn openmetrics_only_features_are_dropped_when_writing_text_0_0_4() {
        // `foo_seconds`, not `foo`: OpenMetrics requires `_<unit>` to suffix the family name, so a
        // fixture naming a unit has to obey that rule to be a legal body in the first place.
        let body = concat!(
            "# TYPE foo_seconds counter\n",
            "# UNIT foo_seconds seconds\n",
            "foo_seconds_total 17 # {detail=\"x\"} 0.67\n",
            "foo_seconds_created 1605281325\n",
            "# EOF\n",
        );
        let families = parsed(body, Dialect::OpenMetrics1_0);
        assert_eq!(
            written(&families, Dialect::Text0_0_4),
            "# TYPE foo_seconds_total counter\nfoo_seconds_total 17\n",
            "no `# UNIT`, no `_created`, no exemplar, no `# EOF`"
        );
    }

    #[test]
    fn openmetrics_only_family_types_degrade_when_writing_text_0_0_4() {
        let om = concat!(
            "# TYPE an_info info\n",
            "an_info_info{k=\"v\"} 1\n",
            "# TYPE a_state stateset\n",
            "a_state{a_state=\"on\"} 1\n",
            "# TYPE a_gh gaugehistogram\n",
            "a_gh_bucket{le=\"+Inf\"} 42\n",
            "a_gh_gsum 3289.3\n",
            "# TYPE an_unknown unknown\n",
            "an_unknown 1\n",
            "# EOF\n",
        );
        let families = parsed(om, Dialect::OpenMetrics1_0);
        assert_eq!(
            written(&families, Dialect::Text0_0_4),
            concat!(
                "# TYPE a_gh histogram\n",
                "a_gh_bucket{le=\"+Inf\"} 42\n",
                "a_gh_sum 3289.3\n",
                "a_gh_count 42\n",
                "# TYPE a_state gauge\n",
                "a_state{a_state=\"on\"} 1\n",
                "# TYPE an_info_info gauge\n",
                "an_info_info{k=\"v\"} 1\n",
                "# TYPE an_unknown untyped\n",
                "an_unknown 1\n",
            )
        );
    }

    #[test]
    fn a_text_untyped_family_writes_as_openmetrics_unknown() {
        let families = parsed("# TYPE foo untyped\nfoo 1\n", Dialect::Text0_0_4);
        assert_eq!(families[0].kind, FamilyType::Untyped);
        assert_eq!(
            written(&families, Dialect::OpenMetrics1_0),
            "# TYPE foo unknown\nfoo 1\n# EOF\n"
        );
    }

    #[test]
    fn help_escaping_follows_the_dialect() {
        // A backslash, a newline and a quote in one HELP string.
        let families =
            parsed("# HELP foo a\\\\b\\nc\"d\n# TYPE foo gauge\nfoo 1\n", Dialect::Text0_0_4);
        assert_eq!(families[0].help.as_deref(), Some("a\\b\nc\"d"));
        assert!(written(&families, Dialect::Text0_0_4).contains("# HELP foo a\\\\b\\nc\"d\n"));
        // OpenMetrics has no bare `"` in an escaped-string, so it gains an escape there.
        assert!(
            written(&families, Dialect::OpenMetrics1_0).contains("# HELP foo a\\\\b\\nc\\\"d\n")
        );
    }

    #[test]
    fn an_undefined_escape_stays_literal_and_is_re_escaped_on_the_way_out() {
        let families = parsed("# HELP foo a\\tb\n# TYPE foo gauge\nfoo 1\n", Dialect::Text0_0_4);
        assert_eq!(families[0].help.as_deref(), Some("a\\tb"), "backslash and `t` both kept");
        assert!(written(&families, Dialect::Text0_0_4).contains("# HELP foo a\\\\tb\n"));
    }

    #[test]
    fn an_empty_help_decodes_as_no_help_at_all() {
        let families = parsed("# HELP foo \n# TYPE foo gauge\nfoo 1\n", Dialect::Text0_0_4);
        assert_eq!(families[0].help, None);
        assert_eq!(written(&families, Dialect::Text0_0_4), "# TYPE foo gauge\nfoo 1\n");
    }

    #[test]
    fn content_type_negotiation_picks_a_dialect() {
        assert_eq!(
            Dialect::from_content_type(
                "application/openmetrics-text; version=1.0.0; charset=utf-8"
            ),
            Dialect::OpenMetrics1_0
        );
        assert_eq!(
            Dialect::from_content_type("APPLICATION/OpenMetrics-Text;version=1.0.0"),
            Dialect::OpenMetrics1_0
        );
        assert_eq!(Dialect::from_content_type("text/plain; version=0.0.4"), Dialect::Text0_0_4);
        assert_eq!(Dialect::from_content_type(""), Dialect::Text0_0_4, "missing type -> text");
        assert!(Dialect::Text0_0_4.content_type().starts_with("text/plain"));
        assert!(Dialect::OpenMetrics1_0.content_type().starts_with("application/openmetrics-text"));
    }

    // --- leniencies -----------------------------------------------------------------------------

    #[test]
    fn a_sample_with_no_type_line_becomes_untyped_or_unknown_per_dialect() {
        assert_eq!(parsed("foo 1\n", Dialect::Text0_0_4)[0].kind, FamilyType::Untyped);
        assert_eq!(parsed("foo 1\n# EOF\n", Dialect::OpenMetrics1_0)[0].kind, FamilyType::Unknown);
    }

    #[test]
    fn a_type_line_after_its_samples_still_types_the_family() {
        let families = parsed("foo 1\n# TYPE foo counter\n# HELP foo late\n", Dialect::Text0_0_4);
        assert_eq!(families[0].kind, FamilyType::Counter);
        assert_eq!(families[0].help.as_deref(), Some("late"));
        assert_eq!(families[0].series[0].point, Point::Counter(1.0));
    }

    #[test]
    fn a_families_samples_need_not_be_contiguous() {
        let families = parsed("# TYPE a gauge\na 1\nb 2\na 3\n", Dialect::Text0_0_4);
        assert_eq!(family_names(&families), vec!["a", "b"]);
        // The second `a 3` is a duplicate of the same (name, labels), so the first wins.
        assert_eq!(families[0].series[0].point, Point::Gauge(1.0));
    }

    #[test]
    fn a_created_sample_is_accepted_in_text_0_0_4_even_though_the_format_has_none() {
        let families =
            parsed("# TYPE foo counter\nfoo 1\nfoo_created 1605281325\n", Dialect::Text0_0_4);
        assert_eq!(families[0].series[0].created, Some(1_605_281_325_000_000_000));
        assert_eq!(
            written(&families, Dialect::Text0_0_4),
            "# TYPE foo_total counter\nfoo_total 1\n",
            "and dropped again on the way out"
        );
    }

    #[test]
    fn a_histogram_missing_its_inf_bucket_gains_one_from_the_count() {
        let families = parsed(
            "# TYPE foo histogram\nfoo_bucket{le=\"1\"} 3\nfoo_count 5\n",
            Dialect::Text0_0_4,
        );
        assert_eq!(
            families[0].series[0].point,
            Point::Histogram { buckets: vec![(1.0, 3), (f64::INFINITY, 5)], sum: None, count: 5 }
        );
    }

    #[test]
    fn buckets_and_quantiles_are_sorted_on_parse() {
        let families = parsed(
            concat!(
                "# TYPE h histogram\n",
                "h_bucket{le=\"+Inf\"} 9\n",
                "h_bucket{le=\"1\"} 2\n",
                "h_bucket{le=\"0.5\"} 1\n",
            ),
            Dialect::Text0_0_4,
        );
        match &families[0].series[0].point {
            Point::Histogram { buckets, .. } => {
                assert_eq!(
                    buckets.iter().map(|(b, _)| *b).collect::<Vec<_>>(),
                    vec![0.5, 1.0, f64::INFINITY]
                );
            }
            other => panic!("expected a histogram, got {other:?}"),
        }
    }

    #[test]
    fn extra_whitespace_and_blank_lines_are_tolerated() {
        let families = parsed("\n  foo{a=\"1\"}    2   \n\n", Dialect::Text0_0_4);
        assert_eq!(families[0].series[0].point, Point::Unknown(2.0));
        assert_eq!(families[0].series[0].labels, vec![("a".to_string(), "1".to_string())]);
    }

    #[test]
    fn a_label_set_may_carry_a_trailing_comma() {
        let families = parsed("foo{a=\"1\",b=\"2\",} 3\n", Dialect::Text0_0_4);
        assert_eq!(families[0].series[0].labels.len(), 2);
    }

    // --- skips and degradations, each observed through telemetry ---------------------------------

    #[test]
    fn a_malformed_sample_line_is_skipped_and_counted() {
        let (families, reasons) = parse_reasons("good 1\n1bad 2\nalso_bad\n", Dialect::Text0_0_4);
        assert_eq!(family_names(&families), vec!["good"]);
        assert!(reasons.contains(&"malformed_line".to_string()));
    }

    /// Prometheus 3's quoted UTF-8 name syntax is a documented gap, not something to half-parse.
    #[test]
    fn a_prometheus_3_quoted_utf8_name_is_skipped_and_counted() {
        let (families, reasons) = parse_reasons(
            "{\"my.dotted.metric\", \"error.message\"=\"Not Found\"} 1\n",
            Dialect::Text0_0_4,
        );
        assert!(families.is_empty());
        assert!(reasons.contains(&"malformed_line".to_string()));
    }

    #[test]
    fn malformed_metadata_is_skipped_and_leaves_the_family_untyped() {
        let (families, reasons) =
            parse_reasons("# TYPE foo nonsense\n# HELP 1bad help\nfoo 1\n", Dialect::Text0_0_4);
        assert_eq!(families[0].kind, FamilyType::Untyped);
        assert!(reasons.contains(&"malformed_metadata".to_string()));
    }

    #[test]
    fn a_second_conflicting_type_line_is_skipped_and_counted() {
        let (families, reasons) =
            parse_reasons("# TYPE foo counter\n# TYPE foo gauge\nfoo 1\n", Dialect::Text0_0_4);
        assert_eq!(families[0].kind, FamilyType::Counter, "the first `# TYPE` wins");
        assert!(reasons.contains(&"duplicate_type".to_string()));
    }

    #[test]
    fn a_second_help_line_is_skipped_and_counted() {
        let (families, reasons) =
            parse_reasons("# HELP foo first\n# HELP foo second\nfoo 1\n", Dialect::Text0_0_4);
        assert_eq!(families[0].help.as_deref(), Some("first"));
        assert!(reasons.contains(&"duplicate_metadata".to_string()));
    }

    #[test]
    fn a_repeated_series_is_skipped_and_counted() {
        let (families, reasons) =
            parse_reasons("foo{a=\"1\"} 1\nfoo{a=\"1\"} 2\n", Dialect::Text0_0_4);
        assert_eq!(families[0].series[0].point, Point::Unknown(1.0));
        assert!(reasons.contains(&"duplicate_series".to_string()));

        let (_, reasons) = parse_reasons(
            "# TYPE h histogram\nh_bucket{le=\"1\"} 1\nh_bucket{le=\"1\"} 2\n",
            Dialect::Text0_0_4,
        );
        assert!(reasons.contains(&"duplicate_series".to_string()), "a repeated bucket too");
    }

    #[test]
    fn a_line_naming_one_label_twice_is_skipped_and_counted() {
        let (families, reasons) = parse_reasons("foo{a=\"1\",a=\"2\"} 1\n", Dialect::Text0_0_4);
        assert!(families.is_empty());
        assert!(reasons.contains(&"duplicate_label".to_string()));
    }

    #[test]
    fn a_sample_with_a_suffix_its_family_type_has_no_meaning_for_is_skipped_and_counted() {
        let (families, reasons) =
            parse_reasons("# TYPE foo counter\nfoo 1\nfoo_sum 2\n", Dialect::Text0_0_4);
        assert_eq!(families[0].series.len(), 1);
        assert!(reasons.contains(&"unknown_suffix".to_string()));
    }

    #[test]
    fn a_series_with_no_value_for_its_type_is_skipped_and_counted() {
        let (families, reasons) =
            parse_reasons("# TYPE foo counter\nfoo_created 1605281325\n", Dialect::Text0_0_4);
        assert!(families.is_empty(), "a `_created` alone is not a counter");
        assert!(reasons.contains(&"incomplete_series".to_string()));

        let (families, reasons) =
            parse_reasons("# TYPE h histogram\nh_sum 1\nh_count 2\n", Dialect::Text0_0_4);
        assert!(families.is_empty(), "a histogram with no buckets is not a histogram");
        assert!(reasons.contains(&"incomplete_series".to_string()));
    }

    #[test]
    fn a_count_line_disagreeing_with_the_inf_bucket_loses_to_it_and_is_counted_degraded() {
        let (families, reasons) = parse_reasons(
            "# TYPE h histogram\nh_bucket{le=\"+Inf\"} 7\nh_count 9\n",
            Dialect::Text0_0_4,
        );
        match &families[0].series[0].point {
            Point::Histogram { count, .. } => assert_eq!(*count, 7, "the `+Inf` bucket wins"),
            other => panic!("expected a histogram, got {other:?}"),
        }
        assert!(reasons.contains(&"histogram_count_mismatch".to_string()));
    }

    #[test]
    fn a_non_utf8_line_is_skipped_and_counted() {
        let (registry, telemetry) = telemetry();
        let mut decoder = PrometheusDecoder::new().with_telemetry(telemetry);
        let mut body = b"good 1\nbad".to_vec();
        body.push(0xff);
        body.extend_from_slice(b" 2\n");
        let families = parse_with(&body, Dialect::Text0_0_4, &mut decoder).expect("must parse");
        assert_eq!(family_names(&families), vec!["good"]);
        assert!(registry.drain(0).iter().any(|event| {
            event.attributes.get("reason").and_then(|v| v.as_str()) == Some("malformed_line")
        }));
    }

    /// A family whose every series was skipped leaves nothing to write, metadata included.
    #[test]
    fn a_family_with_no_usable_series_is_dropped_entirely() {
        let families =
            parsed("# TYPE foo histogram\n# HELP foo nothing here\n", Dialect::Text0_0_4);
        assert!(families.is_empty());
    }

    // --- review follow-ups: counted write-side degradations, dialect edges ------------------------

    /// Renders with counters live, returning the body and every
    /// `logit.output.metrics.degraded{reason}` count it recorded.
    fn write_counted(families: &[MetricFamily], dialect: Dialect) -> (String, Vec<(String, f64)>) {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("prometheus", "prometheus_out", "sink");
        let mut encoder = PrometheusEncoder::new().with_telemetry(telemetry);
        let mut out = Vec::new();
        write_with(families, dialect, &mut out, &mut encoder);
        let counts = registry
            .drain(0)
            .iter()
            .filter_map(|event| {
                let reason = event.attributes.get("reason")?.as_str()?.to_string();
                let value = event.metrics.iter().find_map(|m| match &m.kind {
                    logit_core::MetricKind::Sum(s) => Some(s.value),
                    _ => None,
                })?;
                Some((reason, value))
            })
            .collect();
        (String::from_utf8(out).expect("exposition must be utf-8"), counts)
    }

    fn unit_family(name: &str, unit: &str) -> MetricFamily {
        let mut family = MetricFamily::new(name, FamilyType::Gauge);
        family.unit = Some(unit.to_string());
        family.series = vec![Series::new(vec![], Point::Gauge(1.0))];
        family
    }

    /// OpenMetrics: "an underscore and the unit MUST be the suffix of the MetricFamily name", and
    /// Prometheus's parser fails the *entire* scrape when it isn't -- so a unit that doesn't fit is
    /// dropped rather than poisoning every other family in the response.
    #[test]
    fn an_openmetrics_unit_is_written_only_when_it_suffixes_the_family_name() {
        // Legal: `_<unit>` suffixes the name -- including a name that is *exactly* `_<unit>`, which
        // both the spec and Prometheus's own length check accept.
        for (name, unit) in
            [("latency_seconds", "seconds"), ("_seconds", "seconds"), ("requests_total", "total")]
        {
            let (body, counts) = write_counted(&[unit_family(name, unit)], Dialect::OpenMetrics1_0);
            assert!(body.contains(&format!("# UNIT {name} {unit}\n")), "{name}/{unit}: {body}");
            assert!(counts.is_empty(), "a conforming unit is not a degradation: {counts:?}");
        }

        for (name, unit) in [
            ("request_duration", "s"),  // not a suffix at all
            ("seconds", "seconds"),     // the name *is* the unit, with no `_` before it
            ("latency_seconds", "sec"), // a prefix of the suffix, not the suffix
        ] {
            let (body, counts) = write_counted(&[unit_family(name, unit)], Dialect::OpenMetrics1_0);
            assert!(!body.contains("# UNIT"), "{name}/{unit} must not emit a unit: {body}");
            assert_eq!(counts, vec![("unit_not_suffix".to_string(), 1.0)], "{name}/{unit}");
        }
    }

    /// The parser is deliberately lenient about the suffix rule -- a scraper keeps what it is given
    /// -- so a non-conforming unit rides through the model and only the *writer* drops it. That
    /// pairing is what makes a relay lose the unit rather than emit a body Prometheus would reject
    /// wholesale.
    #[test]
    fn a_non_suffix_unit_is_accepted_on_parse_and_dropped_on_write() {
        let families =
            parsed("# TYPE foo gauge\n# UNIT foo bar\nfoo 1\n# EOF\n", Dialect::OpenMetrics1_0);
        assert_eq!(families[0].unit.as_deref(), Some("bar"), "parse keeps what it was given");
        let (body, counts) = write_counted(&families, Dialect::OpenMetrics1_0);
        assert_eq!(body, "# TYPE foo gauge\nfoo 1\n# EOF\n");
        assert_eq!(counts, vec![("unit_not_suffix".to_string(), 1.0)]);
    }

    /// A unit like OpenMetrics' `{requests}` would break the line grammar as well as the suffix
    /// rule, so the charset is checked too.
    #[test]
    fn an_openmetrics_unit_outside_the_name_charset_is_dropped_and_counted() {
        let (body, counts) = write_counted(
            &[unit_family("queue_{requests}", "{requests}")],
            Dialect::OpenMetrics1_0,
        );
        assert!(!body.contains("# UNIT"), "{body}");
        assert_eq!(counts, vec![("unit_not_suffix".to_string(), 1.0)]);
    }

    /// Text 0.0.4 never writes `# UNIT` at all, so a unit it cannot carry is the operator's dialect
    /// choice rather than a degradation -- and must not be counted as one.
    #[test]
    fn a_unit_dropped_by_the_text_dialect_is_not_counted() {
        let (body, counts) =
            write_counted(&[unit_family("request_duration", "s")], Dialect::Text0_0_4);
        assert!(!body.contains("# UNIT"), "{body}");
        assert!(counts.is_empty(), "{counts:?}");
    }

    /// `# EOF` terminates an OpenMetrics body and nothing else: in text 0.0.4 it is an ordinary
    /// comment, and the exposition after it is ordinary exposition.
    #[test]
    fn a_text_0_0_4_eof_comment_does_not_terminate_the_body() {
        let families = parsed("# EOF\nfoo 1\nbar 2\n", Dialect::Text0_0_4);
        assert_eq!(family_names(&families), vec!["bar", "foo"]);

        // The same bytes are malformed in OpenMetrics, where `# EOF` is the terminator.
        assert!(matches!(
            parse(b"# EOF\nfoo 1\n", Dialect::OpenMetrics1_0).unwrap_err(),
            CodecError::Malformed(_)
        ));
    }

    fn exemplar(value: f64, attrs: &[(&str, &str)]) -> Exemplar {
        let mut filtered_attributes = AttrMap::new();
        for (key, value) in attrs {
            filtered_attributes.insert(key, Value::str(*value));
        }
        Exemplar { timestamp: 0, value, trace: None, filtered_attributes }
    }

    fn counter_with_exemplars(exemplars: Vec<Exemplar>) -> MetricFamily {
        let mut family = MetricFamily::new("requests_total", FamilyType::Counter);
        family.series = vec![Series { exemplars, ..Series::new(vec![], Point::Counter(17.0)) }];
        family
    }

    /// A counter has one line, so OpenMetrics has room for one exemplar -- an OTLP `Sum` carrying
    /// three loses two, counted.
    #[test]
    fn a_counters_extra_exemplars_are_dropped_and_counted() {
        let family = counter_with_exemplars(vec![
            exemplar(0.1, &[("n", "1")]),
            exemplar(0.2, &[("n", "2")]),
            exemplar(0.3, &[("n", "3")]),
        ]);
        let (body, counts) = write_counted(&[family], Dialect::OpenMetrics1_0);
        assert_eq!(body, "# TYPE requests counter\nrequests_total 17 # {n=\"1\"} 0.1\n# EOF\n");
        assert_eq!(counts, vec![("exemplar_dropped".to_string(), 2.0)]);
    }

    /// Two exemplars whose values land in one bucket's range: OpenMetrics allows one exemplar per
    /// bucket, so the second has nowhere to sit.
    #[test]
    fn two_exemplars_in_one_bucket_range_lose_one_counted() {
        let mut family = MetricFamily::new("latency", FamilyType::Histogram);
        family.series = vec![Series {
            exemplars: vec![exemplar(0.05, &[("n", "1")]), exemplar(0.06, &[("n", "2")])],
            ..Series::new(
                vec![],
                Point::Histogram {
                    buckets: vec![(0.1, 2), (f64::INFINITY, 2)],
                    sum: None,
                    count: 2,
                },
            )
        }];
        let (body, counts) = write_counted(&[family], Dialect::OpenMetrics1_0);
        assert!(body.contains("latency_bucket{le=\"0.1\"} 2 # {n=\"1\"} 0.05\n"), "{body}");
        assert_eq!(counts, vec![("exemplar_dropped".to_string(), 1.0)]);
    }

    /// An exemplar whose value falls in no bucket at all (`NaN`, or above the highest bound when
    /// there is no `+Inf` bucket) is dropped rather than attached to an arbitrary line.
    #[test]
    fn an_exemplar_with_no_bucket_to_sit_on_is_dropped_and_counted() {
        let mut family = MetricFamily::new("latency", FamilyType::Histogram);
        family.series = vec![Series {
            exemplars: vec![exemplar(f64::NAN, &[("n", "1")])],
            ..Series::new(
                vec![],
                Point::Histogram { buckets: vec![(f64::INFINITY, 1)], sum: None, count: 1 },
            )
        }];
        let (body, counts) = write_counted(&[family], Dialect::OpenMetrics1_0);
        assert!(!body.contains('#') || !body.contains("{n="), "{body}");
        assert_eq!(counts, vec![("exemplar_dropped".to_string(), 1.0)]);
    }

    /// OpenMetrics caps an exemplar's label set at 128 UTF-8 code points; emitting a longer one
    /// would make the line invalid and truncating a trace id would make it a lie.
    #[test]
    fn an_over_budget_exemplar_label_set_is_dropped_and_counted() {
        let long = "x".repeat(200);
        let family = counter_with_exemplars(vec![exemplar(0.1, &[("detail", &long)])]);
        let (body, counts) = write_counted(&[family], Dialect::OpenMetrics1_0);
        assert_eq!(body, "# TYPE requests counter\nrequests_total 17\n# EOF\n");
        assert_eq!(counts, vec![("exemplar_dropped".to_string(), 1.0)]);
    }

    /// OpenMetrics allows exemplars on `_total` and `_bucket` samples only, so a gauge's are dropped
    /// -- and `otlp_in` really does produce them: OTLP carries exemplars on `Gauge` and on the
    /// non-monotonic `Sum` this codec renders as a gauge.
    #[test]
    fn a_gauges_exemplars_are_dropped_and_counted() {
        let mut family = MetricFamily::new("temperature", FamilyType::Gauge);
        family.series = vec![Series {
            exemplars: vec![exemplar(0.1, &[("n", "1")]), exemplar(0.2, &[("n", "2")])],
            ..Series::new(vec![], Point::Gauge(21.5))
        }];
        let (body, counts) = write_counted(&[family], Dialect::OpenMetrics1_0);
        assert_eq!(body, "# TYPE temperature gauge\ntemperature 21.5\n# EOF\n");
        assert_eq!(counts, vec![("exemplar_dropped".to_string(), 2.0)]);
    }

    /// Same for a summary, which has no exemplar-carrying line either -- the structural reason
    /// OTLP's own `SummaryDataPoint` has no exemplars field. `Distribution`/`Samples` reach the wire
    /// through this arm too.
    #[test]
    fn a_summarys_exemplars_are_dropped_and_counted() {
        let mut family = MetricFamily::new("rpc_seconds", FamilyType::Summary);
        family.series = vec![Series {
            exemplars: vec![exemplar(0.1, &[])],
            ..Series::new(
                vec![],
                Point::Summary { quantiles: vec![(0.5, 0.2)], sum: Some(1.0), count: Some(3) },
            )
        }];
        let (body, counts) = write_counted(&[family], Dialect::OpenMetrics1_0);
        assert!(!body.contains('#') || !body.contains(" # {"), "{body}");
        assert_eq!(counts, vec![("exemplar_dropped".to_string(), 1.0)]);
    }

    /// And for the two OpenMetrics-only gauge-shaped types, so no arm of the writer is left silently
    /// dropping one.
    #[test]
    fn an_info_and_a_stateset_exemplar_are_dropped_and_counted() {
        let mut info = MetricFamily::new("build", FamilyType::Info);
        info.series = vec![Series {
            exemplars: vec![exemplar(1.0, &[])],
            ..Series::new(vec![], Point::Info)
        }];
        let (_, counts) = write_counted(&[info], Dialect::OpenMetrics1_0);
        assert_eq!(counts, vec![("exemplar_dropped".to_string(), 1.0)]);

        let mut state = MetricFamily::new("state", FamilyType::StateSet);
        state.series = vec![Series {
            exemplars: vec![exemplar(1.0, &[])],
            ..Series::new(vec![("state".to_string(), "on".to_string())], Point::StateSet(true))
        }];
        let (_, counts) = write_counted(&[state], Dialect::OpenMetrics1_0);
        assert_eq!(counts, vec![("exemplar_dropped".to_string(), 1.0)]);
    }

    /// Text 0.0.4 has no exemplars at all, so dropping every one of them is the dialect's own
    /// doing -- a permitted normalization, not a counted degradation.
    #[test]
    fn text_0_0_4_drops_every_exemplar_without_counting_any() {
        let family = counter_with_exemplars(vec![exemplar(0.1, &[]), exemplar(0.2, &[])]);
        let (body, counts) = write_counted(&[family], Dialect::Text0_0_4);
        assert_eq!(body, "# TYPE requests_total counter\nrequests_total 17\n");
        assert!(counts.is_empty(), "{counts:?}");

        // Including the kinds that have no exemplar-carrying line even in OpenMetrics.
        let mut gauge = MetricFamily::new("temperature", FamilyType::Gauge);
        gauge.series = vec![Series {
            exemplars: vec![exemplar(0.1, &[])],
            ..Series::new(vec![], Point::Gauge(1.0))
        }];
        let (_, counts) = write_counted(&[gauge], Dialect::Text0_0_4);
        assert!(counts.is_empty(), "{counts:?}");
    }

    /// `_total` is a legal metric name on its own (`_` is a valid leading character), and stripping
    /// the suffix for the OpenMetrics family name would leave a nameless `# TYPE  counter` line.
    #[test]
    fn a_counter_named_total_keeps_its_whole_name_as_the_openmetrics_family_name() {
        let families = parsed("# TYPE _total counter\n_total 1\n", Dialect::Text0_0_4);
        assert_eq!(families[0].name, "_total");
        assert_eq!(
            written(&families, Dialect::OpenMetrics1_0),
            "# TYPE _total counter\n_total_total 1\n# EOF\n"
        );
    }

    /// Both formats separate metadata fields with spaces or tabs interchangeably, exactly as the
    /// sample path already does.
    #[test]
    fn tab_separated_metadata_lines_are_honored() {
        let families =
            parsed("# TYPE\tfoo\tgauge\n# HELP\tfoo\ttabbed help\nfoo 1\n", Dialect::Text0_0_4);
        assert_eq!(families[0].kind, FamilyType::Gauge);
        assert_eq!(families[0].help.as_deref(), Some("tabbed help"));
    }

    #[test]
    fn a_metadata_keyword_with_nothing_after_it_is_counted_malformed() {
        for line in ["# TYPE\n", "# HELP\n", "# UNIT\n"] {
            let (_, reasons) = parse_reasons(&format!("{line}foo 1\n"), Dialect::Text0_0_4);
            assert!(
                reasons.contains(&"malformed_metadata".to_string()),
                "{line:?} must count as malformed metadata, got {reasons:?}"
            );
        }
        // A `# TYPE` with a name but no type keyword is the same kind of malformed.
        let (families, reasons) = parse_reasons("# TYPE foo\nfoo 1\n", Dialect::Text0_0_4);
        assert_eq!(families[0].kind, FamilyType::Untyped);
        assert!(reasons.contains(&"malformed_metadata".to_string()));
    }

    /// An ordinary comment is still an ordinary comment, keyword-shaped or not.
    #[test]
    fn a_non_metadata_comment_is_ignored_rather_than_counted() {
        let (families, reasons) = parse_reasons("# just a comment\n#\nfoo 1\n", Dialect::Text0_0_4);
        assert_eq!(family_names(&families), vec!["foo"]);
        assert!(reasons.is_empty(), "{reasons:?}");
    }
}
