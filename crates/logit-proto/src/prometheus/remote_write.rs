//! Prometheus remote-write 1.0 and 2.0 -- the protobuf layer, both directions.
//!
//! [`decode`] turns one request body into [`MetricFamily`]s; [`encode`] turns them back into
//! protobuf. The sibling of [`super::text`]: same seam, same model mapping, different syntax.
//! Nothing here knows about [`logit_core::Event`], and nothing here re-decides a mapping
//! [`super`]'s module doc already states -- remote-write is a *transport* for exactly the semantics
//! the exposition format describes.
//!
//! Neither Snappy nor HTTP is this module's business. The caller decompresses (block format,
//! `snap::raw`, **not** the framed one), enforces its own body cap, and hands over plain protobuf;
//! [`Version`] holds the header knowledge both ends need so the receiver and the sender don't spell
//! it out independently.
//!
//! References:
//! <https://prometheus.io/docs/specs/remote_write_spec/> (1.0),
//! <https://prometheus.io/docs/specs/remote_write_spec_2_0/> (2.0),
//! [ADR `prometheus-remote-write`](../../../../docs/adr/prometheus-remote-write.md).
//!
//! ## The wire
//!
//! | | 1.0 | 2.0 |
//! |---|---|---|
//! | message | `prometheus.WriteRequest` | `io.prometheus.write.v2.Request` |
//! | `Content-Type` | `application/x-protobuf`, and `application/x-protobuf;proto=prometheus.WriteRequest` is what Prometheus itself sends | `application/x-protobuf;proto=io.prometheus.write.v2.Request` |
//! | `X-Prometheus-Remote-Write-Version` | `0.1.0` | `2.0.0` |
//! | `Content-Encoding` | `snappy` (block) | `snappy` (block) |
//! | metadata | `WriteRequest.metadata[]`, one entry per family, naming the family explicitly | `TimeSeries.metadata`, inline per series, with **no family-name field** -- the family is derived from the sample name and the type ([`assemble::family_base`]) |
//! | strings | inline | a request-wide `symbols` table, `symbols[0] == ""`, everything referenced by index |
//! | created timestamp | no equivalent field | `Sample.start_timestamp` (milliseconds, `0` = unset) |
//! | counts written | -- | `X-Prometheus-Remote-Write-{Samples,Histograms,Exemplars}-Written` on 2xx *and* 4xx |
//!
//! A `TimeSeries` carries samples **or** native histograms, never both, and its labels are sorted
//! by byte order -- which is not the same as "`__name__` first": `_` is `0x5f`, so a label named
//! `Foo` sorts *before* `__name__`.
//!
//! ## Timestamp groups
//!
//! A remote-write `TimeSeries` is one label set and N samples; a [`Series`] is one label set and
//! **one** [`Point`]. The two shapes do not line up, so **[`decode`] partitions a request's samples
//! by timestamp** and runs one [`Assembler`] per distinct timestamp, returning
//! [`Decoded::groups`] in ascending timestamp order. [`encode`] is the same operation inverted: it
//! takes a slice of groups and merges series with identical label sets **across** groups into one
//! `TimeSeries` whose samples are in timestamp order.
//!
//! The partition is the right one rather than a convenient one, for two independent reasons. A
//! classic histogram's `_bucket`/`_sum`/`_count` series all come from one scrape and therefore share
//! one timestamp exactly, so grouping by timestamp puts precisely the samples that belong to one
//! `Histogram { buckets, sum, count }` in front of one assembler and never mixes two scrapes'
//! buckets into one record. And both versions require a sender to write a series' samples in
//! timestamp order, which ascending groups give for free, per series, across the whole request.
//!
//! ## Decode: protobuf → families
//!
//! | Wire | Model |
//! |---|---|
//! | `__name__` | the sample name the assembler routes on; every other label is a series label, verbatim |
//! | `MetricMetadata` (1.0) / `Metadata` (2.0) | a family declaration -- type, `# HELP`, `# UNIT`. 1.0's `UNKNOWN` and 2.0's `UNSPECIFIED` both mean [`FamilyType::Unknown`], and an empty `help`/`unit` is *absent*, not `Some("")` |
//! | `Sample.value`, `Sample.timestamp` (ms) | the point's value, and the group it lands in |
//! | a sample whose value is the stale NaN ([`super::STALE_NAN_BITS`]) | [`Point::Stale`] for that series in that group |
//! | `Sample.start_timestamp` (2.0, ms, `0` = unset) | [`Series::created`] |
//! | `Exemplar` | the shared exemplar mapping ([`assemble::exemplar_from_labels`]): `trace_id`/`span_id` → [`logit_core::TraceRef`], the rest → `filtered_attributes` |
//! | `histograms[]` (native histograms) | **skipped**, counted `logit.input.metrics.skipped{reason="native_histogram"}` and reported in [`Decoded::histograms_skipped`] -- `docs/known-gaps.md`'s native-histogram row |
//!
//! Everything else is the assembler's, unchanged: suffix routing, `le`/`quantile` stripping, the
//! cumulative-bucket rules, the skip reasons.
//!
//! ### Malformed input: what is a `400`, and what is a counted skip
//!
//! [`CodecError::Malformed`] is reserved for a request that is *structurally* broken, where no part
//! of it can be trusted -- the receiver answers `400` and keeps nothing:
//!
//! - the body is not the protobuf message the `Content-Type` promised;
//! - 2.0: `symbols[0]` is not the empty string, a `labels_refs` list has an odd length, or any
//!   symbol reference is out of range. A bad index means the whole table is being read wrongly.
//!
//! Anything wrong with *one series* is a counted skip instead, because a request from a real sender
//! is worth keeping the rest of:
//!
//! | Reason | What it counts |
//! |---|---|
//! | `invalid_labels` | a series with no `__name__`, an empty label name or value, or a label set that is not strictly ascending by byte order -- all of which both specs forbid a sender from producing |
//! | `native_histogram` | one entry of a `histograms[]` list (above) |
//!
//! ## Encode: families → protobuf
//!
//! Each family is flattened into the same suffixed samples the exposition writer emits, since that
//! is the spelling the assembler on the other end reads:
//!
//! | Family | Series on the wire |
//! |---|---|
//! | `counter` | `<name>_total` (the suffix is added when the model name lacks it), with every exemplar |
//! | `gauge`/`unknown`/`untyped` | `<name>` |
//! | `info` | `<name>_info` = `1` |
//! | `stateset` | `<name>`, `0`/`1` -- the state label is an ordinary model label and is already in the series' label set |
//! | `histogram` | `<name>_bucket{le}` per cumulative bucket including `+Inf`, then `<name>_sum` when the model has one, then `<name>_count` |
//! | `gaugehistogram` | the same with `_gsum`/`_gcount`, and no `_gcount` when there is no `_gsum` (OpenMetrics' own rule, which the assembler reads back) |
//! | `summary` | `<name>{quantile}` per quantile, then `<name>_sum`/`<name>_count` when the model has them |
//! | [`Point::Stale`] | one sample of the family's primary series name, valued [`super::STALE_NAN_BITS`] |
//!
//! The label set is the series' labels plus `__name__` plus the generated `le`/`quantile`, **sorted
//! by byte order after** those are added. Exemplars ride the `_total` and `_bucket` series only, as
//! they do in OpenMetrics, each on the bucket its own value falls in -- but *all* of them, not one
//! per bucket: remote-write's `exemplars` is a repeated field with no one-per-line rule to respect.
//!
//! ### What encode drops, and what it counts
//!
//! | Situation | Result |
//! |---|---|
//! | [`Series::timestamp`] is `None` | the series is **skipped**, `logit.output.metrics.skipped{reason="no_timestamp"}`. The wire has no way to omit a timestamp, so a caller on this path runs the encoder with [`super::PrometheusEncoder::with_timestamps_always`]`(true)` and never produces one |
//! | a label with an empty value, or named `__name__`/`le`/`quantile` where the codec generates that name itself | the label is dropped, `logit.output.labels.dropped{reason="empty_value"\|"reserved"}` -- both specs forbid an empty value, and a repeated name is an invalid label set rather than a confusing one |
//! | an exemplar on a family with no `_total`/`_bucket` series (a gauge, an `info`, a `stateset`, a summary), or one whose value is `NaN` and so falls in no bucket | dropped, `logit.output.metrics.degraded{reason="exemplar_dropped"}` -- the same rule and the same counter the OpenMetrics writer uses |
//! | a family with an empty name | **skipped**, `logit.output.metrics.skipped{reason="invalid_labels"}`; `__name__` may not be empty |
//! | [`Series::created`], **version 1.0 only** | dropped, uncounted. 1.0 has no field for it at all; this is the operator's choice of wire version, listed with the permitted normalizations below the way text 0.0.4's `_created` drop is |
//!
//! ## Permitted normalizations
//!
//! `decode`/`encode` are a fixed point modulo this list, which
//! `crates/logit-proto/tests/prometheus_remote_write_fixed_point.rs` pins:
//!
//! - everything on [`super`]'s own list, which this module inherits whole;
//! - series and label reordering: series by label set, labels by byte order, groups by timestamp;
//! - timestamps and created timestamps are **milliseconds** on the wire, so sub-millisecond
//!   precision is truncated (toward zero, as the text 0.0.4 writer truncates);
//! - `Untyped` is spelled `Unknown` on both versions: the metadata enums have one value for "no
//!   type", so a text 0.0.4 relay's `untyped` comes back as `unknown`. Both decode to the same
//!   `Gauge` + `prometheus.type` marker and both write as `untyped` in text 0.0.4;
//! - a counter's value sample gains `_total` when the model name lacks it, exactly as in both
//!   exposition dialects;
//! - 1.0 drops `Series::created` (above);
//! - an exemplar belongs to a `TimeSeries`, not to a sample, in *both* versions -- so a request
//!   carrying several timestamps for one series cannot say which sample an exemplar came from.
//!   Decode assigns each one to the group whose timestamp it matches, and to the latest group when
//!   it matches none;
//! - a histogram's exemplars come back in *bucket-label* order (`le` sorted as a string, so `+Inf`
//!   first), not in the order the model held them.
//!
//! [`Assembler`]: assemble::Assembler
//! [`CodecError::Malformed`]: crate::CodecError::Malformed

use super::assemble::{self, Assembler, Sample};
use super::generated::io::prometheus::write::v2 as pb2;
use super::generated::prometheus as pb1;
use super::{
    is_stale_nan, text, FamilyType, MetricFamily, Point, PrometheusDecoder, PrometheusEncoder,
    Series, STALE_NAN_BITS,
};
use crate::CodecError;
use logit_core::Exemplar;
use prost::Message;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};

/// `X-Prometheus-Remote-Write-Version`, which both versions require on every request.
pub const HEADER_VERSION: &str = "x-prometheus-remote-write-version";
/// 2.0's report of how many samples the receiver actually stored -- set on 2xx *and* 4xx.
pub const HEADER_SAMPLES_WRITTEN: &str = "x-prometheus-remote-write-samples-written";
/// 2.0's report of how many native histograms the receiver stored, which for this codec is always
/// `0` ([`Decoded::histograms_skipped`] counts what it did *not* store).
pub const HEADER_HISTOGRAMS_WRITTEN: &str = "x-prometheus-remote-write-histograms-written";
/// 2.0's report of how many exemplars the receiver stored.
pub const HEADER_EXEMPLARS_WRITTEN: &str = "x-prometheus-remote-write-exemplars-written";
/// The only `Content-Encoding` either version defines -- Snappy **block** format, not framed.
pub const CONTENT_ENCODING_SNAPPY: &str = "snappy";

/// The media type both versions build on; the `proto=` parameter is what tells them apart.
const MEDIA_TYPE: &str = "application/x-protobuf";
const PROTO_V1: &str = "prometheus.WriteRequest";
const PROTO_V2: &str = "io.prometheus.write.v2.Request";

/// Which remote-write message a body is. The operator picks this for a sender the way they pick an
/// exposition dialect; a receiver accepts both on one bind (the 2.0 spec requires a 2.0 receiver to
/// accept 1.0).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Version {
    /// `prometheus.WriteRequest` -- what every Prometheus ships today.
    V1,
    /// `io.prometheus.write.v2.Request` -- symbol table, inline metadata, created timestamps.
    V2,
}

impl Version {
    /// The `Content-Type` a sender sets. 1.0 gets the fully-qualified spelling Prometheus itself
    /// sends rather than the bare media type, because it is strictly more informative and every
    /// conforming 1.0 receiver accepts it.
    pub fn content_type(self) -> &'static str {
        match self {
            Version::V1 => "application/x-protobuf;proto=prometheus.WriteRequest",
            Version::V2 => "application/x-protobuf;proto=io.prometheus.write.v2.Request",
        }
    }

    /// The `X-Prometheus-Remote-Write-Version` value for this version. Both are the *spec* version,
    /// which for 1.0 is the historical `0.1.0` rather than `1.0.0`.
    pub fn header_version(self) -> &'static str {
        match self {
            Version::V1 => "0.1.0",
            Version::V2 => "2.0.0",
        }
    }

    /// The version a request's `Content-Type` selects, or `None` -- the receiver's `415`.
    ///
    /// Tolerant in exactly the ways a real sender is careless: the media type and the parameter
    /// names compare case-insensitively (HTTP says they may), whitespace around `;` and `=` is
    /// ignored, unrecognized parameters (`charset=…`) are ignored, and a bare
    /// `application/x-protobuf` with no `proto=` at all is 1.0 -- which is what the 1.0 spec itself
    /// mandates and what a sender predating the parameter sends.
    pub fn from_content_type(value: &str) -> Option<Version> {
        let mut parts = value.split(';');
        let media = parts.next()?.trim();
        if !media.eq_ignore_ascii_case(MEDIA_TYPE) {
            return None;
        }
        let mut version = Version::V1;
        for part in parts {
            let (key, parameter) = part.split_once('=')?;
            if !key.trim().eq_ignore_ascii_case("proto") {
                continue;
            }
            let parameter = parameter.trim().trim_matches('"');
            version = if parameter.eq_ignore_ascii_case(PROTO_V1) {
                Version::V1
            } else if parameter.eq_ignore_ascii_case(PROTO_V2) {
                Version::V2
            } else {
                // A `proto=` naming a message this codec doesn't speak is not 1.0 by default: the
                // sender said what it meant, and it isn't this.
                return None;
            };
        }
        Some(version)
    }
}

/// One decoded request: its families, partitioned by sample timestamp, plus the counts 2.0's
/// `-Written` headers report.
#[derive(Debug, Default, PartialEq)]
pub struct Decoded {
    /// One family list per distinct sample timestamp, in **ascending timestamp order**; each list
    /// is canonically ordered the way `Assembler::finish` orders one (families by name, series by
    /// label set).
    pub groups: Vec<Vec<MetricFamily>>,
    /// Samples actually stored -- a sample the assembler stepped over (a duplicate, a bad `le`) is
    /// not counted, because the header is a report of what the receiver kept.
    pub samples: u64,
    /// Exemplars actually stored.
    pub exemplars: u64,
    /// Native-histogram entries skipped, each also counted
    /// `logit.input.metrics.skipped{reason="native_histogram"}`.
    pub histograms_skipped: u64,
}

// -------------------------------------------------------------------------------------------------
// Decoding
// -------------------------------------------------------------------------------------------------

/// Decodes one **decompressed** request body -- see this module's doc for the mapping, the
/// timestamp-group rule, and what is `Malformed` versus counted.
pub fn decode(
    body: &[u8],
    version: Version,
    decoder: &mut PrometheusDecoder,
) -> Result<Decoded, CodecError> {
    match version {
        Version::V1 => decode_v1(body, decoder),
        Version::V2 => decode_v2(body, decoder),
    }
}

/// One series' `__name__` and label set once its references have been resolved and validated, or
/// `None` for a series already counted `invalid_labels`. 2.0 decodes in passes -- validate, then
/// declare, then push -- so this is what the first pass hands the other two.
type ResolvedSeries<'a> = Option<(&'a str, Vec<(String, String)>)>;

/// A family declaration lifted out of a request's metadata, before any group exists to put it in.
struct Declaration {
    name: String,
    kind: FamilyType,
    help: Option<String>,
    unit: Option<String>,
}

/// One [`Assembler`] per distinct sample timestamp, each pre-loaded with every declaration the
/// request carried. Declarations are per *request*, not per timestamp, so applying all of them to
/// each group is both correct and cheap -- a real request has one or two groups.
struct Groups<'a> {
    declarations: &'a [Declaration],
    groups: BTreeMap<i64, Assembler>,
}

impl<'a> Groups<'a> {
    fn new(declarations: &'a [Declaration]) -> Self {
        Groups { declarations, groups: BTreeMap::new() }
    }

    /// The group for `timestamp_nanos`, opening it if this is the first sample at that instant.
    fn at(&mut self, timestamp_nanos: i64, decoder: &mut PrometheusDecoder) -> &mut Assembler {
        self.groups.entry(timestamp_nanos).or_insert_with(|| {
            // Remote-write has no "untyped" spelling of its own, so an undeclared family is
            // `Unknown` -- OpenMetrics' spelling, and what both metadata enums' zero value means.
            let mut assembler = Assembler::new(FamilyType::Unknown);
            for declaration in self.declarations {
                assembler.declare(
                    &declaration.name,
                    declaration.kind,
                    declaration.help.clone(),
                    declaration.unit.clone(),
                    decoder,
                );
            }
            assembler
        })
    }

    /// The group an *exemplar* belongs to, without opening one: the exact timestamp match when the
    /// request carried a sample at that instant, else the latest group. An exemplar hangs off a
    /// `TimeSeries` rather than a sample in both versions, so this is a choice the wire forces --
    /// see the permitted-normalization list.
    fn existing(&mut self, timestamp_nanos: i64) -> Option<&mut Assembler> {
        let key = if self.groups.contains_key(&timestamp_nanos) {
            timestamp_nanos
        } else {
            *self.groups.keys().next_back()?
        };
        self.groups.get_mut(&key)
    }

    fn finish(self, decoder: &mut PrometheusDecoder) -> Vec<Vec<MetricFamily>> {
        // A `BTreeMap` keyed by timestamp already drains in ascending order.
        self.groups.into_values().map(|assembler| assembler.finish(decoder)).collect()
    }
}

/// A series' `__name__` and its remaining labels, or `None` -- counted `invalid_labels` by the
/// caller -- when the label set breaks a rule both specs place on senders: a non-empty `__name__`,
/// no empty names or values, and strictly ascending byte order (which also rules out a repeat).
fn series_labels<'a>(pairs: &[(&'a str, &'a str)]) -> Option<(&'a str, Vec<(String, String)>)> {
    let mut name = None;
    let mut labels = Vec::with_capacity(pairs.len().saturating_sub(1));
    let mut previous: Option<&str> = None;
    for (key, value) in pairs {
        if key.is_empty() || value.is_empty() {
            return None;
        }
        if previous.is_some_and(|earlier| earlier.as_bytes() >= key.as_bytes()) {
            return None;
        }
        previous = Some(key);
        if *key == "__name__" {
            name = Some(*value);
        } else {
            labels.push(((*key).to_string(), (*value).to_string()));
        }
    }
    Some((name?, labels))
}

/// Milliseconds (both versions' only time unit) → unix nanoseconds.
fn ms_to_nanos(ms: i64) -> i64 {
    ms.saturating_mul(1_000_000)
}

fn non_empty(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

/// 1.0's `MetricMetadata.type`. `UNKNOWN` (and any value this build doesn't recognize) is
/// [`FamilyType::Unknown`], which is exactly what an undeclared family already gets.
fn family_type_v1(value: i32) -> FamilyType {
    match pb1::metric_metadata::MetricType::try_from(value) {
        Ok(pb1::metric_metadata::MetricType::Counter) => FamilyType::Counter,
        Ok(pb1::metric_metadata::MetricType::Gauge) => FamilyType::Gauge,
        Ok(pb1::metric_metadata::MetricType::Histogram) => FamilyType::Histogram,
        Ok(pb1::metric_metadata::MetricType::Gaugehistogram) => FamilyType::GaugeHistogram,
        Ok(pb1::metric_metadata::MetricType::Summary) => FamilyType::Summary,
        Ok(pb1::metric_metadata::MetricType::Info) => FamilyType::Info,
        Ok(pb1::metric_metadata::MetricType::Stateset) => FamilyType::StateSet,
        Ok(pb1::metric_metadata::MetricType::Unknown) | Err(_) => FamilyType::Unknown,
    }
}

/// 2.0's `Metadata.type` -- the same enum under 2.0's `METRIC_TYPE_` spelling.
fn family_type_v2(value: i32) -> FamilyType {
    match pb2::metadata::MetricType::try_from(value) {
        Ok(pb2::metadata::MetricType::Counter) => FamilyType::Counter,
        Ok(pb2::metadata::MetricType::Gauge) => FamilyType::Gauge,
        Ok(pb2::metadata::MetricType::Histogram) => FamilyType::Histogram,
        Ok(pb2::metadata::MetricType::Gaugehistogram) => FamilyType::GaugeHistogram,
        Ok(pb2::metadata::MetricType::Summary) => FamilyType::Summary,
        Ok(pb2::metadata::MetricType::Info) => FamilyType::Info,
        Ok(pb2::metadata::MetricType::Stateset) => FamilyType::StateSet,
        Ok(pb2::metadata::MetricType::Unspecified) | Err(_) => FamilyType::Unknown,
    }
}

fn decode_v1(body: &[u8], decoder: &mut PrometheusDecoder) -> Result<Decoded, CodecError> {
    let request = pb1::WriteRequest::decode(body)
        .map_err(|e| CodecError::Malformed(format!("prometheus.WriteRequest: {e}")))?;

    let declarations: Vec<Declaration> = request
        .metadata
        .iter()
        .map(|metadata| Declaration {
            name: metadata.metric_family_name.clone(),
            kind: family_type_v1(metadata.r#type),
            help: non_empty(&metadata.help),
            unit: non_empty(&metadata.unit),
        })
        .collect();

    let mut groups = Groups::new(&declarations);
    let mut decoded = Decoded::default();
    let mut pairs: Vec<(&str, &str)> = Vec::new();
    for series in &request.timeseries {
        pairs.clear();
        pairs.extend(series.labels.iter().map(|l| (l.name.as_str(), l.value.as_str())));
        let Some((name, labels)) = series_labels(&pairs) else {
            decoder.skipped("invalid_labels");
            continue;
        };
        for sample in &series.samples {
            let timestamp = ms_to_nanos(sample.timestamp);
            if push_sample(
                groups.at(timestamp, decoder),
                name,
                &labels,
                sample.value,
                timestamp,
                decoder,
            ) {
                decoded.samples += 1;
            }
        }
        for exemplar in &series.exemplars {
            let timestamp = ms_to_nanos(exemplar.timestamp);
            let converted = assemble::exemplar_from_labels(
                exemplar.labels.iter().map(|l| (l.name.clone(), l.value.clone())).collect(),
                exemplar.value,
                timestamp,
            );
            // An exemplar with no group at all to land in has nothing to attach to -- the series
            // carried no samples this codec kept.
            let Some(assembler) = groups.existing(timestamp) else { continue };
            if assembler.push_exemplar(name, labels.clone(), converted, decoder) {
                decoded.exemplars += 1;
            }
        }
        for _ in &series.histograms {
            decoded.histograms_skipped += 1;
            decoder.skipped("native_histogram");
        }
    }
    decoded.groups = groups.finish(decoder);
    Ok(decoded)
}

/// The one sample-pushing decision both versions share: the stale NaN is a property of the series,
/// every other value is a reading.
fn push_sample(
    assembler: &mut Assembler,
    name: &str,
    labels: &[(String, String)],
    value: f64,
    timestamp: i64,
    decoder: &mut PrometheusDecoder,
) -> bool {
    if is_stale_nan(value) {
        return assembler.push_stale(name, labels.to_vec(), Some(timestamp), decoder);
    }
    assembler.push(
        Sample {
            name,
            labels: labels.to_vec(),
            value,
            value_text: None,
            timestamp: Some(timestamp),
            exemplar: None,
        },
        decoder,
    )
}

/// One symbol by reference. `0` is the empty string by construction and means *absent* for an
/// optional field; any other out-of-range index means the table is being read wrongly, which is a
/// request-level error rather than one bad series.
fn symbol(symbols: &[String], reference: u32) -> Result<Option<String>, CodecError> {
    if reference == 0 {
        return Ok(None);
    }
    match symbols.get(reference as usize) {
        Some(value) => Ok(non_empty(value)),
        None => Err(CodecError::Malformed(format!(
            "io.prometheus.write.v2: symbol reference {reference} is out of range \
             ({} symbols)",
            symbols.len()
        ))),
    }
}

/// `labels_refs` → the name/value pairs it points at. An odd length or an out-of-range index is
/// `Malformed`, for the same reason [`symbol`] gives.
fn resolve_refs<'a>(
    symbols: &'a [String],
    refs: &[u32],
) -> Result<Vec<(&'a str, &'a str)>, CodecError> {
    if !refs.len().is_multiple_of(2) {
        return Err(CodecError::Malformed(format!(
            "io.prometheus.write.v2: labels_refs has an odd length ({})",
            refs.len()
        )));
    }
    let mut out = Vec::with_capacity(refs.len() / 2);
    for [name, value] in refs.as_chunks::<2>().0 {
        let resolve = |reference: u32| {
            symbols.get(reference as usize).map(String::as_str).ok_or_else(|| {
                CodecError::Malformed(format!(
                    "io.prometheus.write.v2: label reference {reference} is out of range \
                     ({} symbols)",
                    symbols.len()
                ))
            })
        };
        out.push((resolve(*name)?, resolve(*value)?));
    }
    Ok(out)
}

fn decode_v2(body: &[u8], decoder: &mut PrometheusDecoder) -> Result<Decoded, CodecError> {
    let request = pb2::Request::decode(body)
        .map_err(|e| CodecError::Malformed(format!("io.prometheus.write.v2.Request: {e}")))?;
    if request.symbols.first().is_some_and(|first| !first.is_empty()) {
        return Err(CodecError::Malformed(
            "io.prometheus.write.v2.Request: symbols[0] must be the empty string".into(),
        ));
    }

    // Pass one: resolve and validate every series' labels. Doing this before any declaration is
    // read means a declaration on a *later* series still reaches the group an earlier one opened,
    // without having to retrofit declarations into groups that already exist.
    let mut resolved: Vec<ResolvedSeries<'_>> = Vec::with_capacity(request.timeseries.len());
    for series in &request.timeseries {
        let pairs = resolve_refs(&request.symbols, &series.labels_refs)?;
        match series_labels(&pairs) {
            Some(series) => resolved.push(Some(series)),
            None => {
                decoder.skipped("invalid_labels");
                resolved.push(None);
            }
        }
    }

    // Pass two: the declarations. 2.0's `Metadata` has no family-name field, so the family is the
    // sample name with its type's own suffix taken off. Metadata that says nothing at all -- an
    // unset (or absent) message, which is what a sender with no type to report emits -- declares
    // nothing, leaving the family implicitly `Unknown` and free for a sibling series to type.
    let mut declarations = Vec::new();
    for (series, resolved) in request.timeseries.iter().zip(&resolved) {
        let Some((name, _)) = resolved else { continue };
        let Some(metadata) = series.metadata else { continue };
        let kind = family_type_v2(metadata.r#type);
        let help = symbol(&request.symbols, metadata.help_ref)?;
        let unit = symbol(&request.symbols, metadata.unit_ref)?;
        if kind == FamilyType::Unknown && help.is_none() && unit.is_none() {
            continue;
        }
        declarations.push(Declaration {
            name: assemble::family_base(name, kind).to_string(),
            kind,
            help,
            unit,
        });
    }

    // Pass three: the data.
    let mut groups = Groups::new(&declarations);
    let mut decoded = Decoded::default();
    for (series, resolved) in request.timeseries.iter().zip(&resolved) {
        let Some((name, labels)) = resolved else { continue };
        for sample in &series.samples {
            let timestamp = ms_to_nanos(sample.timestamp);
            let assembler = groups.at(timestamp, decoder);
            if push_sample(assembler, name, labels, sample.value, timestamp, decoder) {
                decoded.samples += 1;
            }
            if sample.start_timestamp != 0 {
                assembler.push_created(
                    name,
                    labels.clone(),
                    ms_to_nanos(sample.start_timestamp),
                    decoder,
                );
            }
        }
        for exemplar in &series.exemplars {
            let pairs = resolve_refs(&request.symbols, &exemplar.labels_refs)?;
            let timestamp = ms_to_nanos(exemplar.timestamp);
            let converted = assemble::exemplar_from_labels(
                pairs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect(),
                exemplar.value,
                timestamp,
            );
            let Some(assembler) = groups.existing(timestamp) else { continue };
            if assembler.push_exemplar(name, labels.clone(), converted, decoder) {
                decoded.exemplars += 1;
            }
        }
        for _ in &series.histograms {
            decoded.histograms_skipped += 1;
            decoder.skipped("native_histogram");
        }
    }
    decoded.groups = groups.finish(decoder);
    Ok(decoded)
}

// -------------------------------------------------------------------------------------------------
// Encoding
// -------------------------------------------------------------------------------------------------

/// Encodes `groups` into one **uncompressed** request body -- see this module's doc for the
/// flattening table, the label rules, and what is dropped. The caller Snappy-block-compresses the
/// result and sets the headers [`Version`] names.
///
/// `groups` is one family list per distinct `Event::timestamp`, the inverse of [`Decoded::groups`]:
/// series with identical label sets across groups merge into one `TimeSeries` whose samples are in
/// timestamp order.
pub fn encode(
    groups: &[Vec<MetricFamily>],
    version: Version,
    encoder: &mut PrometheusEncoder,
) -> Vec<u8> {
    encode_counted(groups, version, encoder).0
}

/// [`encode`], plus how many samples the body it returns actually carries -- what a sender counts
/// as `logit.output.samples`, the mirror of [`Decoded::samples`] on the receiving end.
///
/// A separate entry point rather than a wider return type on [`encode`], because the count is only
/// derivable *here*: one [`Series`] becomes one sample for a gauge and several for a histogram
/// (this module doc's flattening table), and a label set repeated across groups merges into one
/// `TimeSeries` with several. A caller counting its own families would be counting something else
/// and calling it samples.
pub fn encode_counted(
    groups: &[Vec<MetricFamily>],
    version: Version,
    encoder: &mut PrometheusEncoder,
) -> (Vec<u8>, u64) {
    let mut built: BTreeMap<Vec<(String, String)>, SeriesOut> = BTreeMap::new();
    let mut flat = Vec::new();
    for families in groups {
        for family in families {
            for series in &family.series {
                let Some(timestamp) = series.timestamp else {
                    encoder.skipped_reason("no_timestamp");
                    continue;
                };
                flat.clear();
                flatten(family, series, &mut flat, encoder);
                for sample in flat.drain(..) {
                    let Some(labels) =
                        label_set(&sample.name, &series.labels, sample.extra.as_ref(), encoder)
                    else {
                        continue;
                    };
                    let out = built.entry(labels).or_insert_with(|| SeriesOut {
                        family: family.name.clone(),
                        kind: family.kind,
                        help: family.help.clone().unwrap_or_default(),
                        unit: family.unit.clone().unwrap_or_default(),
                        samples: Vec::new(),
                        exemplars: Vec::new(),
                    });
                    out.samples.push(SampleOut {
                        value: sample.value,
                        timestamp_ms: timestamp / 1_000_000,
                        start_ms: series.created.map(|c| c / 1_000_000).unwrap_or(0),
                    });
                    out.exemplars.extend(sample.exemplars);
                }
            }
        }
    }
    for out in built.values_mut() {
        // Both versions require a series' samples to be in timestamp order. Stable, so two samples
        // that truncate to the same millisecond keep the order the groups gave them.
        out.samples.sort_by_key(|sample| sample.timestamp_ms);
    }
    // Counted in its own pass, deliberately *after* everything that can still add to or remove
    // from `built` -- what a caller reports as `logit.output.samples` has to be what the body
    // ends up carrying, not what the merge loop above happened to have accumulated at some point
    // on the way there.
    let samples = built.values().map(|out| out.samples.len() as u64).sum();
    let body = match version {
        Version::V1 => encode_v1(built),
        Version::V2 => encode_v2(built),
    };
    (body, samples)
}

/// One series on its way out: the family facts both versions' metadata needs, plus the samples and
/// exemplars merged across every group this label set appeared in.
struct SeriesOut {
    family: String,
    kind: FamilyType,
    help: String,
    unit: String,
    samples: Vec<SampleOut>,
    exemplars: Vec<ExemplarOut>,
}

struct SampleOut {
    value: f64,
    timestamp_ms: i64,
    start_ms: i64,
}

struct ExemplarOut {
    labels: Vec<(String, String)>,
    value: f64,
    timestamp_ms: i64,
}

/// One flattened wire sample before its label set is built: the sample name, the generated
/// `le`/`quantile` label if its role has one, the value, and the exemplars that sit on it.
struct FlatSample {
    name: String,
    extra: Option<(&'static str, String)>,
    value: f64,
    exemplars: Vec<ExemplarOut>,
}

/// The name of a family's own value sample: a counter's always ends in `_total` and an `info`'s in
/// `_info`, whatever the model name is -- see [`super`]'s "Family naming" table.
fn primary_sample_name(family: &MetricFamily) -> Cow<'_, str> {
    match family.kind {
        FamilyType::Counter if !family.name.ends_with("_total") => {
            Cow::Owned(format!("{}_total", family.name))
        }
        FamilyType::Info => Cow::Owned(format!("{}_info", family.name)),
        _ => Cow::Borrowed(family.name.as_str()),
    }
}

/// A float in Prometheus' own spelling, for the `le`/`quantile` label values (which are *strings*
/// on the wire in every format). Sample values are protobuf doubles and need no formatting at all.
fn bound(value: f64) -> String {
    let mut out = String::new();
    text::push_float_str(&mut out, value);
    out
}

fn exemplar_out(exemplar: &Exemplar) -> ExemplarOut {
    ExemplarOut {
        labels: text::exemplar_labels(exemplar),
        value: exemplar.value,
        timestamp_ms: exemplar.timestamp / 1_000_000,
    }
}

/// One family's series → the flat samples it is spelled as on the wire (this module doc's
/// flattening table).
fn flatten(
    family: &MetricFamily,
    series: &Series,
    out: &mut Vec<FlatSample>,
    encoder: &mut PrometheusEncoder,
) {
    let primary = primary_sample_name(family);
    let plain =
        |name: String, value: f64| FlatSample { name, extra: None, value, exemplars: Vec::new() };
    // Exemplars live on a `_total` or a `_bucket` series and nowhere else, for the same reason the
    // OpenMetrics writer places them there -- the model's own exemplar mapping already decided it,
    // and this transport has no reason to widen it.
    let drop_exemplars = |encoder: &mut PrometheusEncoder| {
        for _ in &series.exemplars {
            encoder.degraded_reason("exemplar_dropped");
        }
    };
    match &series.point {
        Point::Counter(v) => out.push(FlatSample {
            name: primary.into_owned(),
            extra: None,
            value: *v,
            exemplars: series.exemplars.iter().map(exemplar_out).collect(),
        }),
        Point::Gauge(v) | Point::Unknown(v) => {
            drop_exemplars(encoder);
            out.push(plain(primary.into_owned(), *v));
        }
        Point::Info => {
            drop_exemplars(encoder);
            out.push(plain(primary.into_owned(), 1.0));
        }
        Point::StateSet(on) => {
            drop_exemplars(encoder);
            out.push(plain(primary.into_owned(), if *on { 1.0 } else { 0.0 }));
        }
        Point::Stale => {
            // A stale marker replaces the family's value sample: one sample, no reading, the
            // reserved NaN payload. There is nothing for an exemplar to be an example of.
            drop_exemplars(encoder);
            out.push(plain(primary.into_owned(), f64::from_bits(STALE_NAN_BITS)));
        }
        Point::Histogram { buckets, sum, count } => {
            let gauge_histogram = family.kind == FamilyType::GaugeHistogram;
            let mut used = vec![false; series.exemplars.len()];
            let mut previous = f64::NEG_INFINITY;
            for (le, cumulative) in buckets {
                // Every exemplar whose value falls in this bucket's range, not just the first:
                // `exemplars` is a repeated field here, so unlike an OpenMetrics `_bucket` line
                // there is no one-per-bucket cap to respect.
                let mut claimed = Vec::new();
                for (i, exemplar) in series.exemplars.iter().enumerate() {
                    if used[i] || exemplar.value.is_nan() {
                        continue;
                    }
                    if exemplar.value > previous && exemplar.value <= *le {
                        used[i] = true;
                        claimed.push(exemplar_out(exemplar));
                    }
                }
                previous = *le;
                out.push(FlatSample {
                    name: format!("{}_bucket", family.name),
                    extra: Some(("le", bound(*le))),
                    value: *cumulative as f64,
                    exemplars: claimed,
                });
            }
            for _ in used.iter().filter(|claimed| !**claimed) {
                encoder.degraded_reason("exemplar_dropped");
            }
            if let Some(sum) = sum {
                let suffix = if gauge_histogram { "_gsum" } else { "_sum" };
                out.push(plain(format!("{}{suffix}", family.name), *sum));
            }
            // A gaugehistogram's `_gcount` exists if and only if its `_gsum` does (OpenMetrics);
            // an ordinary histogram always reports `_count`.
            if sum.is_some() || !gauge_histogram {
                let suffix = if gauge_histogram { "_gcount" } else { "_count" };
                out.push(plain(format!("{}{suffix}", family.name), *count as f64));
            }
        }
        Point::Summary { quantiles, sum, count } => {
            drop_exemplars(encoder);
            for (quantile, value) in quantiles {
                out.push(FlatSample {
                    name: family.name.clone(),
                    extra: Some(("quantile", bound(*quantile))),
                    value: *value,
                    exemplars: Vec::new(),
                });
            }
            if let Some(sum) = sum {
                out.push(plain(format!("{}_sum", family.name), *sum));
            }
            if let Some(count) = count {
                out.push(plain(format!("{}_count", family.name), *count as f64));
            }
        }
    }
}

/// The wire label set for one flat sample: the series' own labels plus `__name__` plus the
/// generated `le`/`quantile`, sorted by byte order **after** both are added -- `_` is `0x5f`, so
/// `__name__` does not always sort first. `None` for a sample this codec cannot name at all.
fn label_set(
    name: &str,
    labels: &[(String, String)],
    extra: Option<&(&'static str, String)>,
    encoder: &mut PrometheusEncoder,
) -> Option<Vec<(String, String)>> {
    if name.is_empty() {
        encoder.skipped_reason("invalid_labels");
        return None;
    }
    let mut out: Vec<(String, String)> = Vec::with_capacity(labels.len() + 2);
    for (key, value) in labels {
        // Names this codec generates itself cannot also arrive as model labels: a repeated label
        // name is an invalid label set, not merely a confusing one. `events_to_families` already
        // drops an attribute that would collide with `le`/`quantile`, so this is the guard for a
        // hand-built family rather than a path a pipeline reaches.
        if key == "__name__" || extra.is_some_and(|(generated, _)| key == generated) {
            encoder.label_dropped("reserved");
            continue;
        }
        if value.is_empty() {
            // Both specs forbid an empty label value, and Prometheus treats one as the label not
            // being there at all -- so dropping it is what the receiver would have done anyway.
            encoder.label_dropped("empty_value");
            continue;
        }
        out.push((key.clone(), value.clone()));
    }
    out.push(("__name__".to_string(), name.to_string()));
    if let Some((generated, value)) = extra {
        out.push(((*generated).to_string(), value.clone()));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    let mut deduped: Vec<(String, String)> = Vec::with_capacity(out.len());
    for label in out {
        if deduped.last().is_some_and(|(existing, _)| *existing == label.0) {
            encoder.label_dropped("collision");
            continue;
        }
        deduped.push(label);
    }
    Some(deduped)
}

fn metric_type_v1(kind: FamilyType) -> pb1::metric_metadata::MetricType {
    use pb1::metric_metadata::MetricType;
    match kind {
        FamilyType::Counter => MetricType::Counter,
        FamilyType::Gauge => MetricType::Gauge,
        FamilyType::Histogram => MetricType::Histogram,
        FamilyType::GaugeHistogram => MetricType::Gaugehistogram,
        FamilyType::Summary => MetricType::Summary,
        FamilyType::Info => MetricType::Info,
        FamilyType::StateSet => MetricType::Stateset,
        // Neither version has a second spelling of "no type given", so text 0.0.4's `untyped`
        // travels as OpenMetrics' `unknown` -- a named normalization.
        FamilyType::Unknown | FamilyType::Untyped => MetricType::Unknown,
    }
}

fn metric_type_v2(kind: FamilyType) -> pb2::metadata::MetricType {
    use pb2::metadata::MetricType;
    match kind {
        FamilyType::Counter => MetricType::Counter,
        FamilyType::Gauge => MetricType::Gauge,
        FamilyType::Histogram => MetricType::Histogram,
        FamilyType::GaugeHistogram => MetricType::Gaugehistogram,
        FamilyType::Summary => MetricType::Summary,
        FamilyType::Info => MetricType::Info,
        FamilyType::StateSet => MetricType::Stateset,
        FamilyType::Unknown | FamilyType::Untyped => MetricType::Unspecified,
    }
}

fn encode_v1(built: BTreeMap<Vec<(String, String)>, SeriesOut>) -> Vec<u8> {
    // One metadata entry per family name, deduped across groups and emitted in name order; the
    // first series to claim a name supplies its type, help and unit.
    let mut metadata: BTreeMap<String, pb1::MetricMetadata> = BTreeMap::new();
    let mut timeseries = Vec::with_capacity(built.len());
    for (labels, out) in built {
        metadata.entry(out.family.clone()).or_insert_with(|| pb1::MetricMetadata {
            r#type: metric_type_v1(out.kind) as i32,
            metric_family_name: out.family.clone(),
            help: out.help.clone(),
            unit: out.unit.clone(),
        });
        timeseries.push(pb1::TimeSeries {
            labels: labels.into_iter().map(|(name, value)| pb1::Label { name, value }).collect(),
            samples: out
                .samples
                .iter()
                // 1.0 has no created-timestamp field: `start_ms` is dropped here, which is the
                // version's own limitation rather than a mapping choice.
                .map(|sample| pb1::Sample { value: sample.value, timestamp: sample.timestamp_ms })
                .collect(),
            exemplars: out
                .exemplars
                .into_iter()
                .map(|exemplar| pb1::Exemplar {
                    labels: exemplar
                        .labels
                        .into_iter()
                        .map(|(name, value)| pb1::Label { name, value })
                        .collect(),
                    value: exemplar.value,
                    timestamp: exemplar.timestamp_ms,
                })
                .collect(),
            histograms: Vec::new(),
        });
    }
    pb1::WriteRequest { timeseries, metadata: metadata.into_values().collect() }.encode_to_vec()
}

/// 2.0's request-wide string table. `symbols[0]` is the empty string by construction, which is what
/// makes a `0` reference mean "absent" for an optional field.
struct Symbols {
    table: Vec<String>,
    index: HashMap<String, u32>,
}

impl Symbols {
    fn new() -> Self {
        Symbols { table: vec![String::new()], index: HashMap::new() }
    }

    fn intern(&mut self, value: &str) -> u32 {
        if value.is_empty() {
            return 0;
        }
        if let Some(reference) = self.index.get(value) {
            return *reference;
        }
        let reference = self.table.len() as u32;
        self.table.push(value.to_string());
        self.index.insert(value.to_string(), reference);
        reference
    }
}

fn encode_v2(built: BTreeMap<Vec<(String, String)>, SeriesOut>) -> Vec<u8> {
    let mut symbols = Symbols::new();
    let mut timeseries = Vec::with_capacity(built.len());
    for (labels, out) in built {
        let labels_refs = labels
            .iter()
            .flat_map(|(name, value)| [symbols.intern(name), symbols.intern(value)])
            .collect();
        // `metadata` is `optional` only because prost renders a non-nullable singular message that
        // way: the upstream Go type is by-value and Prometheus always writes field 5, and 2.0
        // requires per-series metadata. Always `Some`, therefore, with the unspecified type and
        // `0` refs when there is nothing to say.
        let metadata = Some(pb2::Metadata {
            r#type: metric_type_v2(out.kind) as i32,
            help_ref: symbols.intern(&out.help),
            unit_ref: symbols.intern(&out.unit),
        });
        let exemplars = out
            .exemplars
            .into_iter()
            .map(|exemplar| pb2::Exemplar {
                labels_refs: exemplar
                    .labels
                    .iter()
                    .flat_map(|(name, value)| [symbols.intern(name), symbols.intern(value)])
                    .collect(),
                value: exemplar.value,
                timestamp: exemplar.timestamp_ms,
            })
            .collect();
        timeseries.push(pb2::TimeSeries {
            labels_refs,
            samples: out
                .samples
                .iter()
                .map(|sample| pb2::Sample {
                    value: sample.value,
                    timestamp: sample.timestamp_ms,
                    start_timestamp: sample.start_ms,
                })
                .collect(),
            histograms: Vec::new(),
            exemplars,
            metadata,
        });
    }
    pb2::Request { symbols: symbols.table, timeseries }.encode_to_vec()
}
