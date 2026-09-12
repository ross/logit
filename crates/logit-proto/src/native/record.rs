//! `MetricRecord`/`LogRecord`/`SpanRecord`/`SpanLink`/`SpanEvent`/`Exemplar`/`Event` wire encoding.
//!
//! **Every record type here is TLV-framed**, the same `tag(1) + len(varint) + payload` shape
//! [`super::value`] uses for a `Value`: a field this reader doesn't recognize (an older reader
//! against a newer writer) is skipped whole by byte count, never corrupting the rest of the
//! record. `logit` is pre-release (`docs/adr/lossless-transit.md`) -- growing a record's field set
//! or a `MetricKind`'s variant set is a straight reshape of this module, not a version-negotiated,
//! dual-read compatibility path; skip-unknown-field framing is kept anyway as cheap hygiene against
//! a torn write or a stray extra byte, not to support mixed-version readers and writers.
//!
//! Two shared helper pairs do all the framing:
//! - [`write_field`]/[`for_each_field`] for a record's own fields: `write_field` builds a nested,
//!   variable-length payload (an attribute map, another TLV record, a blob whose length isn't
//!   known until it's built) into a temporary buffer; `for_each_field` drives the read side,
//!   calling back with each `(tag, payload)` pair and silently skipping a tag it's handed that the
//!   caller's `match` doesn't claim.
//! - [`write_scalar_field`] for a field whose payload has a length known *before* it's written --
//!   a fixed-size integer, float, byte array, or dictionary index -- writes tag + len + payload
//!   straight into the output buffer with no temporary allocation at all.
//!
//! **Only non-default field values are encoded**; an absent field decodes to that same default
//! (`0`, `None`, empty), so a record carrying mostly-default values -- the common case -- stays
//! small on the wire. This is safe by construction: encoding is skipped exactly when the value
//! already equals what decoding an absent field produces, so a round trip is always exact whether
//! or not a given field happened to be written.
//!
//! A list of same-typed records (`MetricRecord::exemplars`, `SpanRecord::events`/`links`,
//! `Event::metrics`) is `uvarint(count)` followed by `count` length-prefixed entries -- unlike a
//! single embedded record (which gets its boundary for free from its own enclosing
//! `write_field`/`for_each_field` frame), each entry here needs its own length prefix so its
//! TLV-framed reader knows where to stop instead of consuming its neighbors' bytes too.

use crate::native::dict::{Dict, DictBuilder};
use crate::native::value::{read_attr_map, read_value, write_attr_map, write_value};
use crate::native::varint::{read_ivarint, read_u8, read_uvarint, write_ivarint, write_uvarint};
use crate::CodecError;
use bytes::{Buf, Bytes, BytesMut};
use logit_core::{
    BodyFormat, DdSketch, Event, Exemplar, ExpHistogram, Histogram, HyperLogLog, LogRecord,
    MetricKind, MetricList, MetricRecord, Samples, Severity, SpanEvent, SpanExt, SpanKind,
    SpanLink, SpanRecord, SpanStatus, Sum, Summary, Symbol, Temporality, TraceRef,
};

// -- Shared TLV framing helpers ----------------------------------------------------------------

/// Writes one field as `tag(1) + len(varint) + payload`, where `build` fills the payload into a
/// temporary buffer first -- for a payload whose length isn't known until it's built (a nested
/// attribute map, another TLV record, a variable-length blob assembled from several parts).
fn write_field(out: &mut BytesMut, tag: u8, build: impl FnOnce(&mut BytesMut)) {
    let mut tmp = BytesMut::new();
    build(&mut tmp);
    out.extend_from_slice(&[tag]);
    write_uvarint(out, tmp.len() as u64);
    out.extend_from_slice(&tmp);
}

/// Writes one field as `tag(1) + len(varint) + payload`, where the caller already knows `len`
/// before writing a single payload byte -- a fixed-size integer, float, byte array, or dictionary
/// index. No temporary buffer: `write_payload` appends straight into `out`. `debug_assert`s that
/// `write_payload` wrote exactly `len` bytes, since a mismatch here would desync every field after
/// it -- a bug in this module, not in wire input, so a debug-only check is the right cost/benefit.
fn write_scalar_field(
    out: &mut BytesMut,
    tag: u8,
    len: usize,
    write_payload: impl FnOnce(&mut BytesMut),
) {
    out.extend_from_slice(&[tag]);
    write_uvarint(out, len as u64);
    let before = out.len();
    write_payload(out);
    debug_assert_eq!(
        out.len() - before,
        len,
        "write_scalar_field: declared len did not match what write_payload wrote"
    );
}

/// Drives the read side of [`write_field`]/[`write_scalar_field`]: walks `body`'s `tag(1) +
/// len(varint) + payload` stream, handing each `(tag, payload)` pair to `visit`. A tag `visit`
/// doesn't recognize is simply never matched by its `match` -- there is nothing else to do here,
/// since the `len`-bounded slice was already carved out before `visit` ever saw it, so an unknown
/// tag's bytes are silently and safely dropped along with the loop iteration that read them.
fn for_each_field(
    body: &mut Bytes,
    mut visit: impl FnMut(u8, &mut Bytes) -> Result<(), CodecError>,
) -> Result<(), CodecError> {
    while !body.is_empty() {
        let tag = read_u8(body)?;
        let len = read_uvarint(body)? as usize;
        if body.len() < len {
            return Err(CodecError::Malformed(format!(
                "field {tag} declares {len} bytes but only {} remain",
                body.len()
            )));
        }
        let mut field = body.split_to(len);
        visit(tag, &mut field)?;
    }
    Ok(())
}

/// Writes `count` then, for each item, a `len(varint) + body` entry built by `write_one` -- the
/// shape a *list* of TLV-framed records needs (see this module's own doc comment for why a single
/// embedded record doesn't need this but a list of them does).
fn write_record_list<T>(
    out: &mut BytesMut,
    items: &[T],
    mut write_one: impl FnMut(&mut BytesMut, &T),
) {
    write_uvarint(out, items.len() as u64);
    for item in items {
        let mut tmp = BytesMut::new();
        write_one(&mut tmp, item);
        write_uvarint(out, tmp.len() as u64);
        out.extend_from_slice(&tmp);
    }
}

/// A destination [`read_record_list_into`] can decode straight into -- `reserve` up front (so the
/// loop never reallocates more than once) then `push` per decoded item. Implemented for `Vec<T>`
/// (what [`read_record_list`] hands back) and for [`MetricList`] directly, so `read_event`'s
/// `FIELD_METRICS` arm can decode straight into the event's own `SmallVec` instead of building a
/// throwaway `Vec` first and `.collect()`-ing it across -- see [`read_record_list_into`]'s doc
/// comment for why that second step was a real, avoidable allocation.
trait ListSink<T> {
    fn reserve(&mut self, additional: usize);
    fn push_item(&mut self, item: T);
}

impl<T> ListSink<T> for Vec<T> {
    fn reserve(&mut self, additional: usize) {
        Vec::reserve(self, additional);
    }
    fn push_item(&mut self, item: T) {
        self.push(item);
    }
}

impl ListSink<MetricRecord> for MetricList {
    fn reserve(&mut self, additional: usize) {
        // Inherent `SmallVec::reserve` -- no `smallvec` dependency needed here, since calling an
        // inherent method only requires naming the type (`MetricList`, re-exported by
        // `logit_core`), not depending on the crate that defines it.
        MetricList::reserve(self, additional);
    }
    fn push_item(&mut self, item: MetricRecord) {
        self.push(item);
    }
}

/// The inverse of [`write_record_list`], decoding straight into a caller-supplied `out` rather
/// than building a fresh collection and handing it back -- what lets `read_event`'s `FIELD_METRICS`
/// arm decode directly into the event's `MetricList` (a `SmallVec<[MetricRecord; 1]>`) instead of
/// collecting into an intermediate `Vec<MetricRecord>` first and `.into_iter().collect()`-ing that
/// into the `SmallVec` -- two allocations (the `Vec`, then the `SmallVec`'s own spill) for what a
/// single upfront `reserve` plus a push loop does in one. `count` is capped at 4096 for the
/// `reserve` call, the same defensive pattern every other counted collection in this codec uses.
/// [`read_record_list`] below is the `Vec`-returning convenience wrapper every other caller
/// (exemplars, span events, span links) still uses -- its own single allocation is unchanged.
fn read_record_list_into<T>(
    bytes: &mut Bytes,
    read_one: impl Fn(&mut Bytes) -> Result<T, CodecError>,
    out: &mut impl ListSink<T>,
) -> Result<(), CodecError> {
    let count = read_uvarint(bytes)? as usize;
    out.reserve(count.min(4096));
    for _ in 0..count {
        let len = read_uvarint(bytes)? as usize;
        if bytes.len() < len {
            return Err(CodecError::Malformed(format!(
                "list entry declares {len} bytes but only {} remain",
                bytes.len()
            )));
        }
        let mut item = bytes.split_to(len);
        let value = read_one(&mut item)?;
        if !item.is_empty() {
            return Err(CodecError::Malformed("list entry had trailing bytes".to_string()));
        }
        out.push_item(value);
    }
    Ok(())
}

/// [`read_record_list_into`]'s `Vec`-returning convenience wrapper -- see that function's doc
/// comment for the allocation story this split exists for.
fn read_record_list<T>(
    bytes: &mut Bytes,
    read_one: impl Fn(&mut Bytes) -> Result<T, CodecError>,
) -> Result<Vec<T>, CodecError> {
    let mut items = Vec::new();
    read_record_list_into(bytes, read_one, &mut items)?;
    Ok(items)
}

fn read_exact_u32(bytes: &mut Bytes) -> Result<u32, CodecError> {
    if bytes.len() != 4 {
        return Err(CodecError::Malformed(format!(
            "expected 4 bytes for a u32, got {}",
            bytes.len()
        )));
    }
    let mut buf = [0u8; 4];
    bytes.copy_to_slice(&mut buf);
    Ok(u32::from_le_bytes(buf))
}

fn read_exact_i64(bytes: &mut Bytes) -> Result<i64, CodecError> {
    if bytes.len() != 8 {
        return Err(CodecError::Malformed(format!(
            "expected 8 bytes for an i64, got {}",
            bytes.len()
        )));
    }
    let mut buf = [0u8; 8];
    bytes.copy_to_slice(&mut buf);
    Ok(i64::from_le_bytes(buf))
}

fn read_exact_f64(bytes: &mut Bytes) -> Result<f64, CodecError> {
    if bytes.len() != 8 {
        return Err(CodecError::Malformed(format!(
            "expected 8 bytes for an f64, got {}",
            bytes.len()
        )));
    }
    let mut buf = [0u8; 8];
    bytes.copy_to_slice(&mut buf);
    Ok(f64::from_le_bytes(buf))
}

fn write_symbol_field(out: &mut BytesMut, dict: &mut DictBuilder, tag: u8, sym: Symbol) {
    let idx = dict.intern(sym);
    write_scalar_field(out, tag, 4, |b| b.extend_from_slice(&idx.to_le_bytes()));
}

fn read_symbol_field(field: &mut Bytes, dict: &Dict) -> Result<Symbol, CodecError> {
    dict.get(read_exact_u32(field)?)
}

fn write_bytes_field(out: &mut BytesMut, tag: u8, bytes: &bytes::Bytes) {
    write_scalar_field(out, tag, bytes.len(), |b| b.extend_from_slice(bytes));
}

// -- MetricRecord / MetricKind ---------------------------------------------------------------

const METRIC_SUM: u8 = 0;
const METRIC_GAUGE: u8 = 1;
const METRIC_GAUGE_DELTA: u8 = 2;
const METRIC_SAMPLES: u8 = 3;
const METRIC_DISTRIBUTION: u8 = 4;
const METRIC_SET_MEMBERS: u8 = 5;
const METRIC_SET: u8 = 6;
const METRIC_HISTOGRAM: u8 = 7;
const METRIC_EXPONENTIAL_HISTOGRAM: u8 = 8;
const METRIC_SUMMARY: u8 = 9;

fn temporality_tag(t: Temporality) -> u8 {
    match t {
        Temporality::Delta => 0,
        Temporality::Cumulative => 1,
    }
}

fn temporality_from_tag(tag: u8) -> Result<Temporality, CodecError> {
    match tag {
        0 => Ok(Temporality::Delta),
        1 => Ok(Temporality::Cumulative),
        other => Err(CodecError::Malformed(format!("unknown temporality tag {other}"))),
    }
}

fn write_f64_kind(out: &mut BytesMut, tag: u8, v: f64) {
    out.extend_from_slice(&[tag]);
    write_uvarint(out, 8);
    out.extend_from_slice(&v.to_le_bytes());
}

fn read_f64(bytes: &mut Bytes) -> Result<f64, CodecError> {
    if bytes.len() < 8 {
        return Err(CodecError::Malformed("expected 8 bytes for an f64".to_string()));
    }
    let mut buf = [0u8; 8];
    bytes.copy_to_slice(&mut buf);
    Ok(f64::from_le_bytes(buf))
}

fn write_option_f64(out: &mut BytesMut, v: Option<f64>) {
    match v {
        Some(x) => {
            out.extend_from_slice(&[1]);
            out.extend_from_slice(&x.to_le_bytes());
        }
        None => out.extend_from_slice(&[0]),
    }
}

fn read_option_f64(bytes: &mut Bytes) -> Result<Option<f64>, CodecError> {
    match read_u8(bytes)? {
        0 => Ok(None),
        1 => Ok(Some(read_f64(bytes)?)),
        other => Err(CodecError::Malformed(format!("bad Option<f64> presence byte {other}"))),
    }
}

fn write_exponential_buckets(out: &mut BytesMut, (offset, counts): &(i32, Vec<u64>)) {
    write_ivarint(out, *offset as i64);
    write_uvarint(out, counts.len() as u64);
    for c in counts {
        write_uvarint(out, *c);
    }
}

fn read_exponential_buckets(bytes: &mut Bytes) -> Result<(i32, Vec<u64>), CodecError> {
    let offset = read_ivarint(bytes)? as i32;
    let count = read_uvarint(bytes)? as usize;
    let mut counts = Vec::with_capacity(count.min(4096));
    for _ in 0..count {
        counts.push(read_uvarint(bytes)?);
    }
    Ok((offset, counts))
}

/// Writes one `MetricKind`'s payload -- see this module's own doc comment for the framing this
/// sits inside (`MR_KIND`'s tag + len wrapper). Each variant's own byte layout is fixed and
/// sequential, not itself TLV-framed: unlike a record's *own* fields, a `MetricKind` variant's
/// shape is locked to its tag, so there is nothing to skip-unknown inside one -- an unrecognized
/// *kind* tag is a hard [`CodecError::Malformed`] in [`read_metric_kind`], not a skip, since a
/// metric with no interpretable value can't be meaningfully carried forward.
fn write_metric_kind(out: &mut BytesMut, kind: &MetricKind) {
    match kind {
        MetricKind::Sum(s) => {
            out.extend_from_slice(&[METRIC_SUM]);
            write_uvarint(out, 10);
            out.extend_from_slice(&s.value.to_le_bytes());
            out.extend_from_slice(&[temporality_tag(s.temporality), s.monotonic as u8]);
        }
        MetricKind::Gauge(v) => write_f64_kind(out, METRIC_GAUGE, *v),
        MetricKind::GaugeDelta(v) => write_f64_kind(out, METRIC_GAUGE_DELTA, *v),
        MetricKind::Samples(s) => {
            let mut tmp = BytesMut::new();
            write_uvarint(&mut tmp, s.values.len() as u64);
            for v in &s.values {
                tmp.extend_from_slice(&v.to_le_bytes());
            }
            tmp.extend_from_slice(&s.sample_rate.to_le_bytes());
            out.extend_from_slice(&[METRIC_SAMPLES]);
            write_uvarint(out, tmp.len() as u64);
            out.extend_from_slice(&tmp);
        }
        MetricKind::Distribution(sketch) => {
            let blob = sketch.to_java_bytes();
            out.extend_from_slice(&[METRIC_DISTRIBUTION]);
            write_uvarint(out, blob.len() as u64);
            out.extend_from_slice(&blob);
        }
        MetricKind::SetMembers(members) => {
            let mut tmp = BytesMut::new();
            write_uvarint(&mut tmp, members.len() as u64);
            for m in members {
                write_uvarint(&mut tmp, m.len() as u64);
                tmp.extend_from_slice(m);
            }
            out.extend_from_slice(&[METRIC_SET_MEMBERS]);
            write_uvarint(out, tmp.len() as u64);
            out.extend_from_slice(&tmp);
        }
        MetricKind::Set(hll) => {
            // `HyperLogLog::to_bytes()`'s blob, the same shape `Distribution`'s
            // `DdSketch::to_java_bytes()` blob takes -- see `crates/logit-core/src/metric.rs`.
            let blob = hll.to_bytes();
            out.extend_from_slice(&[METRIC_SET]);
            write_uvarint(out, blob.len() as u64);
            out.extend_from_slice(&blob);
        }
        MetricKind::Histogram(h) => {
            let mut tmp = BytesMut::new();
            write_uvarint(&mut tmp, h.buckets.len() as u64);
            for (bound, count) in &h.buckets {
                tmp.extend_from_slice(&bound.to_le_bytes());
                write_uvarint(&mut tmp, *count);
            }
            tmp.extend_from_slice(&[temporality_tag(h.temporality)]);
            write_option_f64(&mut tmp, h.sum);
            write_option_f64(&mut tmp, h.min);
            write_option_f64(&mut tmp, h.max);
            out.extend_from_slice(&[METRIC_HISTOGRAM]);
            write_uvarint(out, tmp.len() as u64);
            out.extend_from_slice(&tmp);
        }
        MetricKind::ExponentialHistogram(e) => {
            let mut tmp = BytesMut::new();
            write_ivarint(&mut tmp, e.scale as i64);
            write_uvarint(&mut tmp, e.zero_count);
            tmp.extend_from_slice(&e.zero_threshold.to_le_bytes());
            write_exponential_buckets(&mut tmp, &e.positive);
            write_exponential_buckets(&mut tmp, &e.negative);
            tmp.extend_from_slice(&[temporality_tag(e.temporality)]);
            write_uvarint(&mut tmp, e.count);
            write_option_f64(&mut tmp, e.sum);
            write_option_f64(&mut tmp, e.min);
            write_option_f64(&mut tmp, e.max);
            out.extend_from_slice(&[METRIC_EXPONENTIAL_HISTOGRAM]);
            write_uvarint(out, tmp.len() as u64);
            out.extend_from_slice(&tmp);
        }
        MetricKind::Summary(s) => {
            let mut tmp = BytesMut::new();
            write_uvarint(&mut tmp, s.quantiles.len() as u64);
            for (q, v) in &s.quantiles {
                tmp.extend_from_slice(&q.to_le_bytes());
                tmp.extend_from_slice(&v.to_le_bytes());
            }
            write_uvarint(&mut tmp, s.count);
            tmp.extend_from_slice(&s.sum.to_le_bytes());
            out.extend_from_slice(&[METRIC_SUMMARY]);
            write_uvarint(out, tmp.len() as u64);
            out.extend_from_slice(&tmp);
        }
    }
}

fn read_metric_kind(bytes: &mut Bytes) -> Result<MetricKind, CodecError> {
    let kind_tag = read_u8(bytes)?;
    let len = read_uvarint(bytes)? as usize;
    if bytes.len() < len {
        return Err(CodecError::Malformed(format!(
            "metric kind {kind_tag} declares {len} bytes but only {} remain",
            bytes.len()
        )));
    }
    let mut body = bytes.split_to(len);
    let kind = match kind_tag {
        METRIC_SUM => {
            let value = read_f64(&mut body)?;
            let temporality = temporality_from_tag(read_u8(&mut body)?)?;
            let monotonic = read_u8(&mut body)? != 0;
            MetricKind::Sum(Sum { value, temporality, monotonic })
        }
        METRIC_GAUGE => MetricKind::Gauge(read_f64(&mut body)?),
        METRIC_GAUGE_DELTA => MetricKind::GaugeDelta(read_f64(&mut body)?),
        METRIC_SAMPLES => {
            let count = read_uvarint(&mut body)? as usize;
            let mut values = Vec::with_capacity(count.min(4096));
            for _ in 0..count {
                values.push(read_f64(&mut body)?);
            }
            let sample_rate = read_f64(&mut body)?;
            let mut samples = Samples::new(values);
            samples.sample_rate = sample_rate;
            MetricKind::Samples(samples)
        }
        METRIC_DISTRIBUTION => {
            let sketch = DdSketch::from_java_bytes(&body)
                .map_err(|e| CodecError::Malformed(format!("bad distribution blob: {e:?}")))?;
            MetricKind::Distribution(sketch)
        }
        METRIC_SET_MEMBERS => {
            let count = read_uvarint(&mut body)? as usize;
            let mut members = Vec::with_capacity(count.min(4096));
            for _ in 0..count {
                let len = read_uvarint(&mut body)? as usize;
                if body.len() < len {
                    return Err(CodecError::Malformed("set member blob truncated".to_string()));
                }
                members.push(body.split_to(len));
            }
            MetricKind::SetMembers(members)
        }
        METRIC_SET => {
            let hll = HyperLogLog::from_bytes(&body)
                .map_err(|e| CodecError::Malformed(format!("bad set blob: {e}")))?;
            MetricKind::Set(hll)
        }
        METRIC_HISTOGRAM => {
            let count = read_uvarint(&mut body)? as usize;
            let mut buckets = Vec::with_capacity(count.min(4096));
            for _ in 0..count {
                let bound = read_f64(&mut body)?;
                let n = read_uvarint(&mut body)?;
                buckets.push((bound, n));
            }
            let temporality = temporality_from_tag(read_u8(&mut body)?)?;
            let sum = read_option_f64(&mut body)?;
            let min = read_option_f64(&mut body)?;
            let max = read_option_f64(&mut body)?;
            MetricKind::Histogram(Histogram { buckets, temporality, sum, min, max })
        }
        METRIC_EXPONENTIAL_HISTOGRAM => {
            let scale = read_ivarint(&mut body)? as i32;
            let zero_count = read_uvarint(&mut body)?;
            let zero_threshold = read_f64(&mut body)?;
            let positive = read_exponential_buckets(&mut body)?;
            let negative = read_exponential_buckets(&mut body)?;
            let temporality = temporality_from_tag(read_u8(&mut body)?)?;
            let count = read_uvarint(&mut body)?;
            let sum = read_option_f64(&mut body)?;
            let min = read_option_f64(&mut body)?;
            let max = read_option_f64(&mut body)?;
            MetricKind::ExponentialHistogram(ExpHistogram {
                scale,
                zero_count,
                zero_threshold,
                positive,
                negative,
                temporality,
                count,
                sum,
                min,
                max,
            })
        }
        METRIC_SUMMARY => {
            let count = read_uvarint(&mut body)? as usize;
            let mut quantiles = Vec::with_capacity(count.min(4096));
            for _ in 0..count {
                let q = read_f64(&mut body)?;
                let v = read_f64(&mut body)?;
                quantiles.push((q, v));
            }
            let record_count = read_uvarint(&mut body)?;
            let sum = read_f64(&mut body)?;
            MetricKind::Summary(Summary { quantiles, count: record_count, sum })
        }
        other => {
            return Err(CodecError::Malformed(format!(
                "unknown metric kind tag {other} -- logit is pre-release, and MetricKind is \
                 reshaped in place rather than version-negotiated (see this module's own doc \
                 comment); a wire frame carrying an unrecognized kind tag is from an incompatible \
                 build, not a newer optional feature"
            )))
        }
    };
    Ok(kind)
}

// -- Exemplar -----------------------------------------------------------------------------------

const EX_TIMESTAMP: u8 = 1;
const EX_VALUE: u8 = 2;
const EX_TRACE: u8 = 3;
const EX_FILTERED_ATTRIBUTES: u8 = 4;

fn write_exemplar(out: &mut BytesMut, dict: &mut DictBuilder, exemplar: &Exemplar) {
    if exemplar.timestamp != 0 {
        write_scalar_field(out, EX_TIMESTAMP, 8, |b| {
            b.extend_from_slice(&exemplar.timestamp.to_le_bytes())
        });
    }
    if exemplar.value != 0.0 {
        write_scalar_field(out, EX_VALUE, 8, |b| {
            b.extend_from_slice(&exemplar.value.to_le_bytes())
        });
    }
    if let Some(trace) = &exemplar.trace {
        write_field(out, EX_TRACE, |b| write_trace_ref(b, trace));
    }
    if !exemplar.filtered_attributes.is_empty() {
        write_field(out, EX_FILTERED_ATTRIBUTES, |b| {
            write_attr_map(b, dict, &exemplar.filtered_attributes)
        });
    }
}

fn read_exemplar(bytes: &mut Bytes, dict: &Dict) -> Result<Exemplar, CodecError> {
    let mut timestamp = 0i64;
    let mut value = 0.0f64;
    let mut trace = None;
    let mut filtered_attributes = logit_core::AttrMap::new();
    for_each_field(bytes, |tag, field| {
        match tag {
            EX_TIMESTAMP => timestamp = read_exact_i64(field)?,
            EX_VALUE => value = read_exact_f64(field)?,
            EX_TRACE => trace = Some(read_trace_ref(field)?),
            EX_FILTERED_ATTRIBUTES => filtered_attributes = read_attr_map(field, dict)?,
            _unknown => {}
        }
        Ok(())
    })?;
    Ok(Exemplar { timestamp, value, trace, filtered_attributes })
}

// -- MetricRecord ---------------------------------------------------------------------------

const MR_NAME: u8 = 1;
const MR_UNIT: u8 = 2;
const MR_DESCRIPTION: u8 = 3;
const MR_START_TIMESTAMP: u8 = 4;
const MR_EXEMPLARS: u8 = 5;
const MR_KIND: u8 = 6;
const MR_FLAGS: u8 = 7;

pub fn write_metric_record(out: &mut BytesMut, dict: &mut DictBuilder, record: &MetricRecord) {
    write_symbol_field(out, dict, MR_NAME, record.name);
    if let Some(unit) = record.unit {
        write_symbol_field(out, dict, MR_UNIT, unit);
    }
    if let Some(description) = record.description {
        write_symbol_field(out, dict, MR_DESCRIPTION, description);
    }
    if record.start_timestamp != 0 {
        write_scalar_field(out, MR_START_TIMESTAMP, 8, |b| {
            b.extend_from_slice(&record.start_timestamp.to_le_bytes())
        });
    }
    if !record.exemplars.is_empty() {
        write_field(out, MR_EXEMPLARS, |b| {
            write_record_list(b, &record.exemplars, |b, e| write_exemplar(b, dict, e));
        });
    }
    if record.flags != 0 {
        write_scalar_field(out, MR_FLAGS, 4, |b| b.extend_from_slice(&record.flags.to_le_bytes()));
    }
    write_field(out, MR_KIND, |b| write_metric_kind(b, &record.kind));
}

pub fn read_metric_record(bytes: &mut Bytes, dict: &Dict) -> Result<MetricRecord, CodecError> {
    let mut name = None;
    let mut unit = None;
    let mut description = None;
    let mut start_timestamp = 0i64;
    let mut exemplars = Vec::new();
    let mut flags = 0u32;
    let mut kind = None;

    for_each_field(bytes, |tag, field| {
        match tag {
            MR_NAME => name = Some(read_symbol_field(field, dict)?),
            MR_UNIT => unit = Some(read_symbol_field(field, dict)?),
            MR_DESCRIPTION => description = Some(read_symbol_field(field, dict)?),
            MR_START_TIMESTAMP => start_timestamp = read_exact_i64(field)?,
            MR_EXEMPLARS => exemplars = read_record_list(field, |b| read_exemplar(b, dict))?,
            MR_FLAGS => flags = read_exact_u32(field)?,
            MR_KIND => kind = Some(read_metric_kind(field)?),
            _unknown => {}
        }
        Ok(())
    })?;

    let name =
        name.ok_or_else(|| CodecError::Malformed("metric record missing name".to_string()))?;
    let kind =
        kind.ok_or_else(|| CodecError::Malformed("metric record missing kind".to_string()))?;
    Ok(MetricRecord { name, unit, description, start_timestamp, exemplars, flags, kind })
}

// -- LogRecord --------------------------------------------------------------------------------

fn severity_tag(s: Severity) -> u8 {
    match s {
        Severity::Trace => 0,
        Severity::Debug => 1,
        Severity::Info => 2,
        Severity::Warn => 3,
        Severity::Error => 4,
        Severity::Fatal => 5,
    }
}

fn severity_from_tag(tag: u8) -> Result<Severity, CodecError> {
    match tag {
        0 => Ok(Severity::Trace),
        1 => Ok(Severity::Debug),
        2 => Ok(Severity::Info),
        3 => Ok(Severity::Warn),
        4 => Ok(Severity::Error),
        5 => Ok(Severity::Fatal),
        other => Err(CodecError::Malformed(format!("unknown severity tag {other}"))),
    }
}

fn body_format_tag(f: BodyFormat) -> u8 {
    match f {
        BodyFormat::Raw => 0,
        BodyFormat::Json => 1,
        BodyFormat::Structured => 2,
    }
}

fn body_format_from_tag(tag: u8) -> Result<BodyFormat, CodecError> {
    match tag {
        0 => Ok(BodyFormat::Raw),
        1 => Ok(BodyFormat::Json),
        2 => Ok(BodyFormat::Structured),
        other => Err(CodecError::Malformed(format!("unknown body_format tag {other}"))),
    }
}

fn write_trace_ref(out: &mut BytesMut, trace: &TraceRef) {
    out.extend_from_slice(&trace.trace_id);
    match trace.span_id {
        Some(span_id) => {
            out.extend_from_slice(&[1]);
            out.extend_from_slice(&span_id);
        }
        None => out.extend_from_slice(&[0]),
    }
    out.extend_from_slice(&[trace.flags]);
}

fn read_trace_ref(bytes: &mut Bytes) -> Result<TraceRef, CodecError> {
    if bytes.len() < 16 {
        return Err(CodecError::Malformed("truncated trace_id".to_string()));
    }
    let mut trace_id = [0u8; 16];
    bytes.copy_to_slice(&mut trace_id);
    let has_span = read_u8(bytes)?;
    let span_id = match has_span {
        0 => None,
        1 => {
            if bytes.len() < 8 {
                return Err(CodecError::Malformed("truncated span_id".to_string()));
            }
            let mut span_id = [0u8; 8];
            bytes.copy_to_slice(&mut span_id);
            Some(span_id)
        }
        other => return Err(CodecError::Malformed(format!("bad span_id presence byte {other}"))),
    };
    let flags = read_u8(bytes)?;
    Ok(TraceRef { trace_id, span_id, flags })
}

const LR_MESSAGE: u8 = 1;
const LR_SEVERITY: u8 = 2;
const LR_BODY_FORMAT: u8 = 3;
const LR_TRACE: u8 = 4;
const LR_EVENT_NAME: u8 = 5;
const LR_OBSERVED_TIMESTAMP: u8 = 6;
const LR_DROPPED_ATTRIBUTES_COUNT: u8 = 7;

pub fn write_log_record(out: &mut BytesMut, dict: &mut DictBuilder, log: &LogRecord) {
    write_field(out, LR_MESSAGE, |b| write_value(b, dict, &log.message));
    if let Some(s) = log.severity {
        write_scalar_field(out, LR_SEVERITY, 1, |b| b.extend_from_slice(&[severity_tag(s)]));
    }
    if log.body_format != BodyFormat::Raw {
        write_scalar_field(out, LR_BODY_FORMAT, 1, |b| {
            b.extend_from_slice(&[body_format_tag(log.body_format)])
        });
    }
    if let Some(trace) = &log.trace {
        write_field(out, LR_TRACE, |b| write_trace_ref(b, trace));
    }
    if let Some(event_name) = log.event_name {
        write_symbol_field(out, dict, LR_EVENT_NAME, event_name);
    }
    if log.observed_timestamp != 0 {
        write_scalar_field(out, LR_OBSERVED_TIMESTAMP, 8, |b| {
            b.extend_from_slice(&log.observed_timestamp.to_le_bytes())
        });
    }
    if log.dropped_attributes_count != 0 {
        write_scalar_field(out, LR_DROPPED_ATTRIBUTES_COUNT, 4, |b| {
            b.extend_from_slice(&log.dropped_attributes_count.to_le_bytes())
        });
    }
}

pub fn read_log_record(bytes: &mut Bytes, dict: &Dict) -> Result<LogRecord, CodecError> {
    let mut message = None;
    let mut severity = None;
    let mut body_format = BodyFormat::Raw;
    let mut trace = None;
    let mut event_name = None;
    let mut observed_timestamp = 0i64;
    let mut dropped_attributes_count = 0u32;

    for_each_field(bytes, |tag, field| {
        match tag {
            LR_MESSAGE => message = Some(read_value(field, dict)?),
            LR_SEVERITY => severity = Some(severity_from_tag(read_u8(field)?)?),
            LR_BODY_FORMAT => body_format = body_format_from_tag(read_u8(field)?)?,
            LR_TRACE => trace = Some(read_trace_ref(field)?),
            LR_EVENT_NAME => event_name = Some(read_symbol_field(field, dict)?),
            LR_OBSERVED_TIMESTAMP => observed_timestamp = read_exact_i64(field)?,
            LR_DROPPED_ATTRIBUTES_COUNT => dropped_attributes_count = read_exact_u32(field)?,
            _unknown => {}
        }
        Ok(())
    })?;

    let message =
        message.ok_or_else(|| CodecError::Malformed("log record missing message".to_string()))?;
    Ok(LogRecord {
        message,
        severity,
        body_format,
        trace,
        event_name,
        observed_timestamp,
        dropped_attributes_count,
    })
}

// -- SpanEvent / SpanLink / SpanExt / SpanRecord -----------------------------------------------

fn span_kind_tag(k: SpanKind) -> u8 {
    match k {
        SpanKind::Internal => 0,
        SpanKind::Server => 1,
        SpanKind::Client => 2,
        SpanKind::Producer => 3,
        SpanKind::Consumer => 4,
    }
}

fn span_kind_from_tag(tag: u8) -> Result<SpanKind, CodecError> {
    match tag {
        0 => Ok(SpanKind::Internal),
        1 => Ok(SpanKind::Server),
        2 => Ok(SpanKind::Client),
        3 => Ok(SpanKind::Producer),
        4 => Ok(SpanKind::Consumer),
        other => Err(CodecError::Malformed(format!("unknown span kind tag {other}"))),
    }
}

fn span_status_tag(s: SpanStatus) -> u8 {
    match s {
        SpanStatus::Unset => 0,
        SpanStatus::Ok => 1,
        SpanStatus::Error => 2,
    }
}

fn span_status_from_tag(tag: u8) -> Result<SpanStatus, CodecError> {
    match tag {
        0 => Ok(SpanStatus::Unset),
        1 => Ok(SpanStatus::Ok),
        2 => Ok(SpanStatus::Error),
        other => Err(CodecError::Malformed(format!("unknown span status tag {other}"))),
    }
}

const SE_TIMESTAMP: u8 = 1;
const SE_NAME: u8 = 2;
const SE_ATTRIBUTES: u8 = 3;
const SE_DROPPED_ATTRIBUTES_COUNT: u8 = 4;

fn write_span_event(out: &mut BytesMut, dict: &mut DictBuilder, event: &SpanEvent) {
    write_scalar_field(out, SE_TIMESTAMP, 8, |b| {
        b.extend_from_slice(&event.timestamp.to_le_bytes())
    });
    write_field(out, SE_NAME, |b| write_value(b, dict, &event.name));
    if !event.attributes.is_empty() {
        write_field(out, SE_ATTRIBUTES, |b| write_attr_map(b, dict, &event.attributes));
    }
    if event.dropped_attributes_count != 0 {
        write_scalar_field(out, SE_DROPPED_ATTRIBUTES_COUNT, 4, |b| {
            b.extend_from_slice(&event.dropped_attributes_count.to_le_bytes())
        });
    }
}

fn read_span_event(bytes: &mut Bytes, dict: &Dict) -> Result<SpanEvent, CodecError> {
    let mut timestamp = 0i64;
    let mut name = None;
    let mut attributes = logit_core::AttrMap::new();
    let mut dropped_attributes_count = 0u32;
    for_each_field(bytes, |tag, field| {
        match tag {
            SE_TIMESTAMP => timestamp = read_exact_i64(field)?,
            SE_NAME => name = Some(read_value(field, dict)?),
            SE_ATTRIBUTES => attributes = read_attr_map(field, dict)?,
            SE_DROPPED_ATTRIBUTES_COUNT => dropped_attributes_count = read_exact_u32(field)?,
            _unknown => {}
        }
        Ok(())
    })?;
    let name = name.ok_or_else(|| CodecError::Malformed("span event missing name".to_string()))?;
    Ok(SpanEvent { timestamp, name, attributes, dropped_attributes_count })
}

const SL_TRACE_ID: u8 = 1;
const SL_SPAN_ID: u8 = 2;
const SL_ATTRIBUTES: u8 = 3;
const SL_FLAGS: u8 = 4;
const SL_TRACE_STATE: u8 = 5;
const SL_DROPPED_ATTRIBUTES_COUNT: u8 = 6;

fn write_span_link(out: &mut BytesMut, dict: &mut DictBuilder, link: &SpanLink) {
    write_scalar_field(out, SL_TRACE_ID, 16, |b| b.extend_from_slice(&link.trace_id));
    write_scalar_field(out, SL_SPAN_ID, 8, |b| b.extend_from_slice(&link.span_id));
    if !link.attributes.is_empty() {
        write_field(out, SL_ATTRIBUTES, |b| write_attr_map(b, dict, &link.attributes));
    }
    if link.flags != 0 {
        write_scalar_field(out, SL_FLAGS, 4, |b| b.extend_from_slice(&link.flags.to_le_bytes()));
    }
    if let Some(trace_state) = &link.trace_state {
        write_bytes_field(out, SL_TRACE_STATE, trace_state);
    }
    if link.dropped_attributes_count != 0 {
        write_scalar_field(out, SL_DROPPED_ATTRIBUTES_COUNT, 4, |b| {
            b.extend_from_slice(&link.dropped_attributes_count.to_le_bytes())
        });
    }
}

fn read_span_link(bytes: &mut Bytes, dict: &Dict) -> Result<SpanLink, CodecError> {
    let mut trace_id = None;
    let mut span_id = None;
    let mut attributes = logit_core::AttrMap::new();
    let mut flags = 0u32;
    let mut trace_state = None;
    let mut dropped_attributes_count = 0u32;
    for_each_field(bytes, |tag, field| {
        match tag {
            SL_TRACE_ID => {
                if field.len() != 16 {
                    return Err(CodecError::Malformed("bad span link trace_id length".to_string()));
                }
                let mut id = [0u8; 16];
                field.copy_to_slice(&mut id);
                trace_id = Some(id);
            }
            SL_SPAN_ID => {
                if field.len() != 8 {
                    return Err(CodecError::Malformed("bad span link span_id length".to_string()));
                }
                let mut id = [0u8; 8];
                field.copy_to_slice(&mut id);
                span_id = Some(id);
            }
            SL_ATTRIBUTES => attributes = read_attr_map(field, dict)?,
            SL_FLAGS => flags = read_exact_u32(field)?,
            SL_TRACE_STATE => trace_state = Some(field.clone()),
            SL_DROPPED_ATTRIBUTES_COUNT => dropped_attributes_count = read_exact_u32(field)?,
            _unknown => {}
        }
        Ok(())
    })?;
    let trace_id =
        trace_id.ok_or_else(|| CodecError::Malformed("span link missing trace_id".to_string()))?;
    let span_id =
        span_id.ok_or_else(|| CodecError::Malformed("span link missing span_id".to_string()))?;
    Ok(SpanLink { trace_id, span_id, attributes, flags, trace_state, dropped_attributes_count })
}

const SX_STATUS_MESSAGE: u8 = 1;
const SX_TRACE_STATE: u8 = 2;
const SX_DROPPED_ATTRIBUTES_COUNT: u8 = 3;
const SX_DROPPED_EVENTS_COUNT: u8 = 4;
const SX_DROPPED_LINKS_COUNT: u8 = 5;

fn write_span_ext(out: &mut BytesMut, ext: &SpanExt) {
    if let Some(status_message) = &ext.status_message {
        write_bytes_field(out, SX_STATUS_MESSAGE, status_message);
    }
    if let Some(trace_state) = &ext.trace_state {
        write_bytes_field(out, SX_TRACE_STATE, trace_state);
    }
    if ext.dropped_attributes_count != 0 {
        write_scalar_field(out, SX_DROPPED_ATTRIBUTES_COUNT, 4, |b| {
            b.extend_from_slice(&ext.dropped_attributes_count.to_le_bytes())
        });
    }
    if ext.dropped_events_count != 0 {
        write_scalar_field(out, SX_DROPPED_EVENTS_COUNT, 4, |b| {
            b.extend_from_slice(&ext.dropped_events_count.to_le_bytes())
        });
    }
    if ext.dropped_links_count != 0 {
        write_scalar_field(out, SX_DROPPED_LINKS_COUNT, 4, |b| {
            b.extend_from_slice(&ext.dropped_links_count.to_le_bytes())
        });
    }
}

fn read_span_ext(bytes: &mut Bytes) -> Result<SpanExt, CodecError> {
    let mut ext = SpanExt::default();
    for_each_field(bytes, |tag, field| {
        match tag {
            SX_STATUS_MESSAGE => ext.status_message = Some(field.clone()),
            SX_TRACE_STATE => ext.trace_state = Some(field.clone()),
            SX_DROPPED_ATTRIBUTES_COUNT => ext.dropped_attributes_count = read_exact_u32(field)?,
            SX_DROPPED_EVENTS_COUNT => ext.dropped_events_count = read_exact_u32(field)?,
            SX_DROPPED_LINKS_COUNT => ext.dropped_links_count = read_exact_u32(field)?,
            _unknown => {}
        }
        Ok(())
    })?;
    Ok(ext)
}

const SR_TRACE_ID: u8 = 1;
const SR_SPAN_ID: u8 = 2;
const SR_PARENT_SPAN_ID: u8 = 3;
const SR_NAME: u8 = 4;
const SR_KIND: u8 = 5;
const SR_STATUS: u8 = 6;
const SR_EVENTS: u8 = 7;
const SR_LINKS: u8 = 8;
const SR_END_TIMESTAMP: u8 = 9;
const SR_FLAGS: u8 = 10;
const SR_EXT: u8 = 11;

pub fn write_span_record(out: &mut BytesMut, dict: &mut DictBuilder, span: &SpanRecord) {
    write_scalar_field(out, SR_TRACE_ID, 16, |b| b.extend_from_slice(&span.trace_id));
    write_scalar_field(out, SR_SPAN_ID, 8, |b| b.extend_from_slice(&span.span_id));
    if let Some(parent) = span.parent_span_id {
        write_scalar_field(out, SR_PARENT_SPAN_ID, 8, |b| b.extend_from_slice(&parent));
    }
    write_field(out, SR_NAME, |b| write_value(b, dict, &span.name));
    if span.kind != SpanKind::Internal {
        write_scalar_field(out, SR_KIND, 1, |b| b.extend_from_slice(&[span_kind_tag(span.kind)]));
    }
    if span.status != SpanStatus::Unset {
        write_scalar_field(out, SR_STATUS, 1, |b| {
            b.extend_from_slice(&[span_status_tag(span.status)])
        });
    }
    if !span.events.is_empty() {
        write_field(out, SR_EVENTS, |b| {
            write_record_list(b, &span.events, |b, e| write_span_event(b, dict, e));
        });
    }
    if !span.links.is_empty() {
        write_field(out, SR_LINKS, |b| {
            write_record_list(b, &span.links, |b, l| write_span_link(b, dict, l));
        });
    }
    write_scalar_field(out, SR_END_TIMESTAMP, 8, |b| {
        b.extend_from_slice(&span.end_timestamp.to_le_bytes())
    });
    if span.flags != 0 {
        write_scalar_field(out, SR_FLAGS, 4, |b| b.extend_from_slice(&span.flags.to_le_bytes()));
    }
    if let Some(ext) = &span.ext {
        write_field(out, SR_EXT, |b| write_span_ext(b, ext));
    }
}

pub fn read_span_record(bytes: &mut Bytes, dict: &Dict) -> Result<SpanRecord, CodecError> {
    let mut trace_id = None;
    let mut span_id = None;
    let mut parent_span_id = None;
    let mut name = None;
    let mut kind = SpanKind::Internal;
    let mut status = SpanStatus::Unset;
    let mut events = Vec::new();
    let mut links = Vec::new();
    let mut end_timestamp = 0i64;
    let mut flags = 0u32;
    let mut ext = None;

    for_each_field(bytes, |tag, field| {
        match tag {
            SR_TRACE_ID => {
                if field.len() != 16 {
                    return Err(CodecError::Malformed(
                        "bad span record trace_id length".to_string(),
                    ));
                }
                let mut id = [0u8; 16];
                field.copy_to_slice(&mut id);
                trace_id = Some(id);
            }
            SR_SPAN_ID => {
                if field.len() != 8 {
                    return Err(CodecError::Malformed(
                        "bad span record span_id length".to_string(),
                    ));
                }
                let mut id = [0u8; 8];
                field.copy_to_slice(&mut id);
                span_id = Some(id);
            }
            SR_PARENT_SPAN_ID => {
                if field.len() != 8 {
                    return Err(CodecError::Malformed(
                        "bad span record parent_span_id length".to_string(),
                    ));
                }
                let mut id = [0u8; 8];
                field.copy_to_slice(&mut id);
                parent_span_id = Some(id);
            }
            SR_NAME => name = Some(read_value(field, dict)?),
            SR_KIND => kind = span_kind_from_tag(read_u8(field)?)?,
            SR_STATUS => status = span_status_from_tag(read_u8(field)?)?,
            SR_EVENTS => events = read_record_list(field, |b| read_span_event(b, dict))?,
            SR_LINKS => links = read_record_list(field, |b| read_span_link(b, dict))?,
            SR_END_TIMESTAMP => end_timestamp = read_exact_i64(field)?,
            SR_FLAGS => flags = read_exact_u32(field)?,
            SR_EXT => ext = Some(Box::new(read_span_ext(field)?)),
            _unknown => {}
        }
        Ok(())
    })?;

    let trace_id = trace_id
        .ok_or_else(|| CodecError::Malformed("span record missing trace_id".to_string()))?;
    let span_id =
        span_id.ok_or_else(|| CodecError::Malformed("span record missing span_id".to_string()))?;
    let name = name.ok_or_else(|| CodecError::Malformed("span record missing name".to_string()))?;
    Ok(SpanRecord {
        trace_id,
        span_id,
        parent_span_id,
        name,
        kind,
        status,
        events,
        links,
        end_timestamp,
        flags,
        ext,
    })
}

// -- Event: TLV fields, the one extensible record ------------------------------------------------

const FIELD_TIMESTAMP: u8 = 1;
const FIELD_ATTRIBUTES: u8 = 2;
const FIELD_LOG: u8 = 3;
const FIELD_METRICS: u8 = 4;
const FIELD_SPAN: u8 = 5;

pub fn write_event(dict: &mut DictBuilder, event: &Event) -> BytesMut {
    let mut out = BytesMut::new();
    write_field(&mut out, FIELD_TIMESTAMP, |buf| write_ivarint(buf, event.timestamp));
    if !event.attributes.is_empty() {
        write_field(&mut out, FIELD_ATTRIBUTES, |buf| write_attr_map(buf, dict, &event.attributes));
    }
    if let Some(log) = &event.log {
        write_field(&mut out, FIELD_LOG, |buf| write_log_record(buf, dict, log));
    }
    if !event.metrics.is_empty() {
        write_field(&mut out, FIELD_METRICS, |buf| {
            write_record_list(buf, &event.metrics[..], |b, m| write_metric_record(b, dict, m));
        });
    }
    if let Some(span) = &event.span {
        write_field(&mut out, FIELD_SPAN, |buf| write_span_record(buf, dict, span));
    }
    out
}

/// Decodes one event's TLV field stream. A field tag this reader doesn't recognize (a future
/// addition to `Event`) is skipped whole -- `len` bytes consumed via `split_to`, nothing parsed --
/// which is what lets an older reader keep decoding a newer writer's batches, per
/// `docs/design/wire-protocol.md`'s version-skew requirement.
pub fn read_event(body: &mut Bytes, dict: &Dict) -> Result<Event, CodecError> {
    let mut timestamp = 0i64;
    let mut attributes = logit_core::AttrMap::new();
    let mut log = None;
    let mut metrics = MetricList::new();
    let mut span = None;

    for_each_field(body, |tag, field| {
        match tag {
            FIELD_TIMESTAMP => timestamp = read_ivarint(field)?,
            FIELD_ATTRIBUTES => attributes = read_attr_map(field, dict)?,
            FIELD_LOG => log = Some(read_log_record(field, dict)?),
            FIELD_METRICS => {
                read_record_list_into(field, |b| read_metric_record(b, dict), &mut metrics)?;
            }
            FIELD_SPAN => span = Some(read_span_record(field, dict)?),
            _unknown => { /* forward compatibility -- see this function's own doc comment */ }
        }
        Ok(())
    })?;
    Ok(Event { timestamp, attributes, log, metrics, span })
}

// -- Resource / Scope -----------------------------------------------------------------------

const RES_ATTRIBUTES: u8 = 1;
const RES_DROPPED_ATTRIBUTES_COUNT: u8 = 2;
const RES_SCHEMA_URL: u8 = 3;

pub fn write_resource(out: &mut BytesMut, dict: &mut DictBuilder, resource: &logit_core::Resource) {
    if !resource.attributes.is_empty() {
        write_field(out, RES_ATTRIBUTES, |b| write_attr_map(b, dict, &resource.attributes));
    }
    if resource.dropped_attributes_count != 0 {
        write_scalar_field(out, RES_DROPPED_ATTRIBUTES_COUNT, 4, |b| {
            b.extend_from_slice(&resource.dropped_attributes_count.to_le_bytes())
        });
    }
    if let Some(schema_url) = &resource.schema_url {
        write_bytes_field(out, RES_SCHEMA_URL, schema_url);
    }
}

pub fn read_resource(bytes: &mut Bytes, dict: &Dict) -> Result<logit_core::Resource, CodecError> {
    let mut attributes = logit_core::AttrMap::new();
    let mut dropped_attributes_count = 0u32;
    let mut schema_url = None;
    for_each_field(bytes, |tag, field| {
        match tag {
            RES_ATTRIBUTES => attributes = read_attr_map(field, dict)?,
            RES_DROPPED_ATTRIBUTES_COUNT => dropped_attributes_count = read_exact_u32(field)?,
            RES_SCHEMA_URL => schema_url = Some(field.clone()),
            _unknown => {}
        }
        Ok(())
    })?;
    Ok(logit_core::Resource { attributes, dropped_attributes_count, schema_url })
}

const SCOPE_NAME: u8 = 1;
const SCOPE_VERSION: u8 = 2;
const SCOPE_ATTRIBUTES: u8 = 3;
const SCOPE_DROPPED_ATTRIBUTES_COUNT: u8 = 4;
const SCOPE_SCHEMA_URL: u8 = 5;

pub fn write_scope(out: &mut BytesMut, dict: &mut DictBuilder, scope: &logit_core::Scope) {
    if !scope.name.is_empty() {
        write_bytes_field(out, SCOPE_NAME, &scope.name);
    }
    if !scope.version.is_empty() {
        write_bytes_field(out, SCOPE_VERSION, &scope.version);
    }
    if !scope.attributes.is_empty() {
        write_field(out, SCOPE_ATTRIBUTES, |b| write_attr_map(b, dict, &scope.attributes));
    }
    if scope.dropped_attributes_count != 0 {
        write_scalar_field(out, SCOPE_DROPPED_ATTRIBUTES_COUNT, 4, |b| {
            b.extend_from_slice(&scope.dropped_attributes_count.to_le_bytes())
        });
    }
    if let Some(schema_url) = &scope.schema_url {
        write_bytes_field(out, SCOPE_SCHEMA_URL, schema_url);
    }
}

pub fn read_scope(bytes: &mut Bytes, dict: &Dict) -> Result<logit_core::Scope, CodecError> {
    let mut name = bytes::Bytes::new();
    let mut version = bytes::Bytes::new();
    let mut attributes = logit_core::AttrMap::new();
    let mut dropped_attributes_count = 0u32;
    let mut schema_url = None;
    for_each_field(bytes, |tag, field| {
        match tag {
            SCOPE_NAME => name = field.clone(),
            SCOPE_VERSION => version = field.clone(),
            SCOPE_ATTRIBUTES => attributes = read_attr_map(field, dict)?,
            SCOPE_DROPPED_ATTRIBUTES_COUNT => dropped_attributes_count = read_exact_u32(field)?,
            SCOPE_SCHEMA_URL => schema_url = Some(field.clone()),
            _unknown => {}
        }
        Ok(())
    })?;
    Ok(logit_core::Scope { name, version, attributes, dropped_attributes_count, schema_url })
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, DdSketch, Value};

    fn dict_round_trip_metric(record: &MetricRecord) -> MetricRecord {
        let mut builder = DictBuilder::default();
        let mut buf = BytesMut::new();
        write_metric_record(&mut buf, &mut builder, record);
        let mut dict_bytes = BytesMut::new();
        builder.write(&mut dict_bytes);
        let dict = Dict::read(&mut dict_bytes.freeze()).unwrap();
        let mut bytes = buf.freeze();
        let out = read_metric_record(&mut bytes, &dict).unwrap();
        assert!(bytes.is_empty());
        out
    }

    fn sample() -> Samples {
        let mut s = Samples::new([1.0, 2.0, 3.0]);
        s.sample_rate = 0.1;
        s
    }

    #[test]
    fn round_trips_every_metric_kind() {
        let name = logit_core::interner::intern("record_test_metric");
        let mut sketch = DdSketch::new();
        sketch.add(1.0);
        sketch.add(2.5);

        let kinds = vec![
            MetricKind::Sum(Sum {
                value: 3.0,
                temporality: Temporality::Cumulative,
                monotonic: false,
            }),
            MetricKind::Gauge(-1.5),
            MetricKind::GaugeDelta(0.5),
            MetricKind::Samples(sample()),
            MetricKind::Distribution(sketch.clone()),
            MetricKind::SetMembers(vec![
                bytes::Bytes::from_static(b"alice"),
                bytes::Bytes::from_static(b"bob"),
            ]),
            MetricKind::Set({
                let mut hll = HyperLogLog::new();
                hll.insert(b"alice");
                hll.insert(b"bob");
                hll
            }),
            MetricKind::Histogram(Histogram {
                buckets: vec![(1.0, 2), (2.0, 5)],
                temporality: Temporality::Cumulative,
                sum: Some(12.5),
                min: Some(0.1),
                max: Some(9.9),
            }),
            MetricKind::ExponentialHistogram(ExpHistogram {
                scale: 3,
                zero_count: 2,
                zero_threshold: 0.5,
                positive: (1, vec![4, 5, 6]),
                negative: (2, vec![7, 8]),
                temporality: Temporality::Delta,
                count: 26,
                sum: Some(100.0),
                min: Some(-5.0),
                max: Some(50.0),
            }),
            MetricKind::Summary(Summary {
                quantiles: vec![(0.5, 10.0), (0.99, 42.0)],
                count: 100,
                sum: 543.2,
            }),
        ];
        for kind in kinds {
            // Non-zero on every kind here too -- MR_FLAGS is a record-level field, orthogonal to
            // which MetricKind variant it's attached to.
            let record = MetricRecord {
                flags: MetricRecord::FLAG_NO_RECORDED_VALUE,
                ..MetricRecord::new(name, kind.clone())
            };
            let out = dict_round_trip_metric(&record);
            match (&kind, &out.kind) {
                (MetricKind::Distribution(a), MetricKind::Distribution(b)) => {
                    // DDSketch has no PartialEq of its own but MetricKind's PartialEq compares
                    // via to_java_bytes -- exercised directly here too, for clarity.
                    assert_eq!(a, b);
                }
                (a, b) => assert_eq!(a, b, "kind mismatch for {a:?}"),
            }
            assert_eq!(out, record);
        }
    }

    #[test]
    fn round_trips_a_fully_populated_metric_record() {
        let name = logit_core::interner::intern("record_test_full_metric");
        let unit = logit_core::interner::intern("ms");
        let description = logit_core::interner::intern("a full metric record");
        let mut attrs = AttrMap::new();
        attrs.insert("dropped", "attr");
        let record = MetricRecord {
            name,
            unit: Some(unit),
            description: Some(description),
            start_timestamp: 1_700_000_000_000_000_000,
            flags: MetricRecord::FLAG_NO_RECORDED_VALUE | 0x2,
            exemplars: vec![
                Exemplar {
                    timestamp: 100,
                    value: 42.0,
                    trace: Some(TraceRef { trace_id: [1; 16], span_id: Some([2; 8]), flags: 1 }),
                    filtered_attributes: attrs.clone(),
                },
                Exemplar {
                    timestamp: 200,
                    value: 43.0,
                    trace: None,
                    filtered_attributes: AttrMap::new(),
                },
            ],
            kind: MetricKind::counter(9.0),
        };
        let out = dict_round_trip_metric(&record);
        assert_eq!(out, record);
    }

    #[test]
    fn round_trips_a_log_record_with_trace_context() {
        let log = LogRecord {
            message: Value::str("hello"),
            severity: Some(Severity::Warn),
            body_format: BodyFormat::Json,
            trace: Some(TraceRef { trace_id: [1; 16], span_id: Some([2; 8]), flags: 1 }),
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        };
        let mut builder = DictBuilder::default();
        let mut buf = BytesMut::new();
        write_log_record(&mut buf, &mut builder, &log);
        let mut dict_bytes = BytesMut::new();
        builder.write(&mut dict_bytes);
        let dict = Dict::read(&mut dict_bytes.freeze()).unwrap();
        let mut bytes = buf.freeze();
        let out = read_log_record(&mut bytes, &dict).unwrap();
        assert!(bytes.is_empty());
        assert_eq!(out, log);
    }

    #[test]
    fn round_trips_a_fully_populated_log_record() {
        let log = LogRecord {
            message: Value::str("hello"),
            severity: Some(Severity::Error),
            body_format: BodyFormat::Structured,
            trace: Some(TraceRef { trace_id: [9; 16], span_id: None, flags: 0 }),
            event_name: Some(logit_core::interner::intern("record_test_event_name")),
            observed_timestamp: 1_700_000_000_500_000_000,
            dropped_attributes_count: 3,
        };
        let mut builder = DictBuilder::default();
        let mut buf = BytesMut::new();
        write_log_record(&mut buf, &mut builder, &log);
        let mut dict_bytes = BytesMut::new();
        builder.write(&mut dict_bytes);
        let dict = Dict::read(&mut dict_bytes.freeze()).unwrap();
        let mut bytes = buf.freeze();
        let out = read_log_record(&mut bytes, &dict).unwrap();
        assert!(bytes.is_empty());
        assert_eq!(out, log);
    }

    #[test]
    fn round_trips_a_log_record_with_no_trace() {
        let log = LogRecord {
            message: Value::str("no trace here"),
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        };
        let mut builder = DictBuilder::default();
        let mut buf = BytesMut::new();
        write_log_record(&mut buf, &mut builder, &log);
        let mut dict_bytes = BytesMut::new();
        builder.write(&mut dict_bytes);
        let dict = Dict::read(&mut dict_bytes.freeze()).unwrap();
        let mut bytes = buf.freeze();
        let out = read_log_record(&mut bytes, &dict).unwrap();
        assert_eq!(out.severity, None);
        assert_eq!(out.trace, None);
    }

    fn full_span_record() -> SpanRecord {
        let mut event_attrs = AttrMap::new();
        event_attrs.insert("k", "v");
        let mut link_attrs = AttrMap::new();
        link_attrs.insert("reason", "test");

        SpanRecord {
            trace_id: [9; 16],
            span_id: [8; 8],
            parent_span_id: Some([7; 8]),
            name: Value::str("span name"),
            kind: SpanKind::Server,
            status: SpanStatus::Error,
            events: vec![SpanEvent {
                timestamp: 100,
                name: Value::str("checkpoint"),
                attributes: event_attrs,
                dropped_attributes_count: 2,
            }],
            links: vec![SpanLink {
                trace_id: [6; 16],
                span_id: [5; 8],
                attributes: link_attrs,
                flags: 1,
                trace_state: Some(bytes::Bytes::from_static(b"vendor=value")),
                dropped_attributes_count: 1,
            }],
            end_timestamp: 200,
            flags: 1,
            ext: Some(Box::new(SpanExt {
                status_message: Some(bytes::Bytes::from_static(b"boom")),
                trace_state: Some(bytes::Bytes::from_static(b"vendor=value2")),
                dropped_attributes_count: 4,
                dropped_events_count: 5,
                dropped_links_count: 6,
            })),
        }
    }

    #[test]
    fn round_trips_a_span_record_with_events_and_links() {
        let span = full_span_record();

        let mut builder = DictBuilder::default();
        let mut buf = BytesMut::new();
        write_span_record(&mut buf, &mut builder, &span);
        let mut dict_bytes = BytesMut::new();
        builder.write(&mut dict_bytes);
        let dict = Dict::read(&mut dict_bytes.freeze()).unwrap();
        let mut bytes = buf.freeze();
        let out = read_span_record(&mut bytes, &dict).unwrap();
        assert!(bytes.is_empty());
        assert_eq!(out, span);
    }

    #[test]
    fn round_trips_an_event_carrying_log_metrics_and_span_at_once() {
        let mut attrs = AttrMap::new();
        attrs.insert("service", "orders-api");

        let mut event = Event::empty(1_700_000_000_000_000_000, attrs);
        event.log = Some(LogRecord {
            message: Value::str("multi-payload event"),
            severity: Some(Severity::Info),
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        });
        event.metrics.push(MetricRecord::new(
            logit_core::interner::intern("record_test_multi_metric"),
            MetricKind::counter(1.0),
        ));
        event.span = Some(SpanRecord {
            trace_id: [4; 16],
            span_id: [3; 8],
            parent_span_id: None,
            name: Value::str("multi span"),
            kind: SpanKind::Internal,
            status: SpanStatus::Ok,
            events: Vec::new(),
            links: Vec::new(),
            end_timestamp: 1_700_000_000_100_000_000,
            flags: 0,
            ext: None,
        });

        let mut dict = DictBuilder::default();
        let body = write_event(&mut dict, &event);
        let mut dict_bytes = BytesMut::new();
        dict.write(&mut dict_bytes);
        let decoded_dict = Dict::read(&mut dict_bytes.freeze()).unwrap();

        let mut body_bytes = body.freeze();
        let out = read_event(&mut body_bytes, &decoded_dict).unwrap();
        assert!(body_bytes.is_empty());

        assert_eq!(out.timestamp, event.timestamp);
        assert_eq!(out.attributes, event.attributes);
        assert!(out.log.is_some(), "log must survive alongside metrics and span");
        assert_eq!(out.metrics.len(), 1);
        assert!(out.span.is_some(), "span must survive alongside log and metrics");
    }

    #[test]
    fn an_event_with_no_payload_at_all_round_trips_as_empty() {
        let event = Event::empty(42, AttrMap::new());
        let mut dict = DictBuilder::default();
        let body = write_event(&mut dict, &event);
        let mut dict_bytes = BytesMut::new();
        dict.write(&mut dict_bytes);
        let decoded_dict = Dict::read(&mut dict_bytes.freeze()).unwrap();
        let out = read_event(&mut body.freeze(), &decoded_dict).unwrap();
        assert_eq!(out.timestamp, 42);
        assert!(out.attributes.is_empty());
        assert!(out.log.is_none());
        assert!(out.metrics.is_empty());
        assert!(out.span.is_none());
    }

    /// The version-skew gate at the record level: an unrecognized `Event` field tag (a
    /// hypothetical future field this reader predates) must be skipped whole, leaving every
    /// known field around it intact.
    #[test]
    fn an_unrecognized_event_field_tag_is_skipped_without_disturbing_known_fields() {
        let mut attrs = AttrMap::new();
        attrs.insert("k", "v");
        let event = Event::empty(7, attrs);

        let mut dict = DictBuilder::default();
        let mut body = write_event(&mut dict, &event);
        // Splice in a field this reader doesn't know (tag 200) with a plausible payload, as if a
        // newer writer had added a field.
        write_field(&mut body, 200, |buf| buf.extend_from_slice(b"future field payload"));

        let mut dict_bytes = BytesMut::new();
        dict.write(&mut dict_bytes);
        let decoded_dict = Dict::read(&mut dict_bytes.freeze()).unwrap();
        let out = read_event(&mut body.freeze(), &decoded_dict).unwrap();

        assert_eq!(out.timestamp, 7);
        assert_eq!(out.attributes.get("k").and_then(|v| v.as_str()), Some("v"));
    }

    /// Same guarantee, one level down: an unrecognized tag *inside* a metric record must be
    /// skipped, leaving the record's known fields intact.
    #[test]
    fn an_unrecognized_metric_record_field_tag_is_skipped() {
        let name = logit_core::interner::intern("record_test_skip_metric");
        let record = MetricRecord::new(name, MetricKind::counter(5.0));

        let mut builder = DictBuilder::default();
        let mut buf = BytesMut::new();
        write_metric_record(&mut buf, &mut builder, &record);
        write_field(&mut buf, 200, |b| b.extend_from_slice(b"future metric field"));

        let mut dict_bytes = BytesMut::new();
        builder.write(&mut dict_bytes);
        let dict = Dict::read(&mut dict_bytes.freeze()).unwrap();
        let mut bytes = buf.freeze();
        let out = read_metric_record(&mut bytes, &dict).unwrap();
        assert_eq!(out, record);
    }

    /// Same guarantee inside a span record.
    #[test]
    fn an_unrecognized_span_record_field_tag_is_skipped() {
        let span = full_span_record();

        let mut builder = DictBuilder::default();
        let mut buf = BytesMut::new();
        write_span_record(&mut buf, &mut builder, &span);
        write_field(&mut buf, 200, |b| b.extend_from_slice(b"future span field"));

        let mut dict_bytes = BytesMut::new();
        builder.write(&mut dict_bytes);
        let dict = Dict::read(&mut dict_bytes.freeze()).unwrap();
        let mut bytes = buf.freeze();
        let out = read_span_record(&mut bytes, &dict).unwrap();
        assert_eq!(out, span);
    }

    #[test]
    fn round_trips_a_fully_populated_resource() {
        let mut attrs = AttrMap::new();
        attrs.insert("service.name", "orders-api");
        let resource = logit_core::Resource {
            attributes: attrs,
            dropped_attributes_count: 7,
            schema_url: Some(bytes::Bytes::from_static(b"https://example.com/schema")),
        };
        let mut builder = DictBuilder::default();
        let mut buf = BytesMut::new();
        write_resource(&mut buf, &mut builder, &resource);
        let mut dict_bytes = BytesMut::new();
        builder.write(&mut dict_bytes);
        let dict = Dict::read(&mut dict_bytes.freeze()).unwrap();
        let mut bytes = buf.freeze();
        let out = read_resource(&mut bytes, &dict).unwrap();
        assert!(bytes.is_empty());
        assert_eq!(out, resource);
    }

    #[test]
    fn round_trips_a_fully_populated_scope() {
        let mut attrs = AttrMap::new();
        attrs.insert("k", "v");
        let scope = logit_core::Scope {
            name: bytes::Bytes::from_static(b"nginx-otel-module"),
            version: bytes::Bytes::from_static(b"1.0.0"),
            attributes: attrs,
            dropped_attributes_count: 2,
            schema_url: Some(bytes::Bytes::from_static(b"https://example.com/schema")),
        };
        let mut builder = DictBuilder::default();
        let mut buf = BytesMut::new();
        write_scope(&mut buf, &mut builder, &scope);
        let mut dict_bytes = BytesMut::new();
        builder.write(&mut dict_bytes);
        let dict = Dict::read(&mut dict_bytes.freeze()).unwrap();
        let mut bytes = buf.freeze();
        let out = read_scope(&mut bytes, &dict).unwrap();
        assert!(bytes.is_empty());
        assert_eq!(out, scope);
    }
}
