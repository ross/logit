//! Encoding a batch of events as InfluxDB 2.x line protocol: the encode half of [`super`]'s module
//! doc, whose "Encode" section is the mapping table and the carrier rules this implements.
//!
//! [`InfluxLineEncoder`] is an [`Encoder`]: one opaque body per batch, which `influxdb_out` posts
//! to `/api/v2/write?precision=ns`. No socket, so grammar, escaping, and counting tests run here.
//! Throttled diagnostics go through the [`Diagnostics`] handle the sink installs; every counter is
//! a field of [`EncodeStats`], zeroed by each `encode` and read by the sink right after, because
//! [`Encoder::encode`] returns only the body.
//!
//! ## Escaping
//!
//! Settled against `influxd` 2.9.1, not the grammar's documentation. In a measurement, tag key,
//! tag value, and field key, a backslash escapes only a following `,`, space, or `=`; before
//! anything else, another backslash included, it is a literal backslash. So the encoder writes a
//! backslash as itself, never `\\`, and escapes `,` and space with one backslash everywhere and `=`
//! in a tag key, tag value, or field key. A measurement never carries `\=`: `influxd` accepts it
//! and stores a series no query can read. Inside a quoted string field value the rules differ:
//! `\` is written `\\` and `"` is written `\"`.
//!
//! That grammar has no spelling for some keys, and the encoder skips them rather than write a line
//! `influxd` would read differently or reject with the whole batch:
//!
//! - any key ending in `\`, since that backslash would escape the delimiter after it;
//! - a measurement with a `\` directly before `,`, space, or `=`: `influxd` stores the series but
//!   no query reads it back;
//! - a field key holding `\"`, which `influxd` reads as `"`.
//!
//! A tag is skipped alone (`logit.output.tags.dropped{reason="unencodable"}`); a measurement or
//! field key skips its point or field (`logit.output.metrics.skipped{reason="unencodable_key"}`).
//!
//! **Every per-line buffer is a struct field**, cleared and refilled rather than reallocated, so a
//! warm encode allocates only the body, a key per new series, and the series maps' growth
//! (`docs/design/memory.md`'s `influx_encode_100_events` row).

use super::{ATTR_MEASUREMENT, CARRIER_PREFIX, FIELD_ATTR_PREFIX};
use crate::{CodecError, Encoder};
use bytes::Bytes;
use logit_core::interner::{intern, resolve, Symbol};
use logit_core::{
    DdSketch, Diagnostics, Event, EventBatch, MetricKind, MetricRecord, MetricValue, Resource,
    Value,
};
use std::collections::HashMap;
// `write!` into a `String`: formats straight into the output buffer, no `String` per number
// (`docs/design/memory.md`).
use std::fmt::Write;

/// Another protocol's carrier namespace, skipped as a tag **uncounted**, as the `influxdb.`
/// carrier is: these are `statsd_out`'s own wire segments (rule (b) of
/// `docs/adr/lossless-transit.md`), and free text such as `statsd.event.title` would become an
/// indexed, high-cardinality tag.
const STATSD_PREFIX: &str = "statsd.";

/// Tag keys `influxd` rejects with a `400` that fails the whole write.
const RESERVED_TAG_KEYS: [&str; 3] = ["_measurement", "_field", "time"];

/// The field key `influxd` refuses with a `422`, which `influxdb_out` reads as a rejected batch.
const RESERVED_FIELD_KEY: &str = "time";

/// `influxd`'s cap on one field value, in bytes; a longer string is a `422` partial write.
const MAX_FIELD_VALUE_BYTES: usize = 1_048_576;

/// Per-batch counts from [`InfluxLineEncoder`]'s last `encode`, which `influxdb_out` emits as
/// telemetry once per batch. Each field names the counter it feeds.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EncodeStats {
    /// A [`Value::Array`] tag rendered as its last representable element:
    /// `logit.output.tags.normalized{reason="multi_value"}`. **Lossy**, unlike every other
    /// `*.normalized` reason: the other elements are discarded.
    pub tags_normalized_multi_value: usize,
    /// A tag whose value has no plain-text form (`Null`, `Bytes`, `Timestamp`, `Map`, or an
    /// `Array` with no representable element): `logit.output.tags.dropped{reason="unrepresentable"}`.
    pub tags_dropped_unrepresentable: usize,
    /// An empty tag key or value, which `influxd` rejects: `tags.dropped{reason="empty"}`.
    pub tags_dropped_empty: usize,
    /// A tag key or value with a newline, or ending in `\` (this module's "Escaping"):
    /// `tags.dropped{reason="unencodable"}`.
    pub tags_dropped_unencodable: usize,
    /// A tag key `influxd` reserves (`_measurement`, `_field`, `time`):
    /// `tags.dropped{reason="reserved"}`.
    pub tags_dropped_reserved: usize,
    /// A record on a carrier event that doesn't fit the carrier line (its name lacks the
    /// `<measurement>.` prefix, or its kind isn't a `Gauge` or `Sum`), written as its own line:
    /// `logit.output.metrics.normalized{reason="carrier_mismatch"}`.
    pub metrics_normalized_carrier_mismatch: usize,
    /// A non-finite `Gauge`/`Sum` value, or carrier field: `metrics.skipped{reason="unencodable_value"}`.
    pub skipped_unencodable_value: usize,
    /// A field or point whose measurement or field key has no line-protocol spelling (this
    /// module's "Escaping"): `metrics.skipped{reason="unencodable_key"}`, once per field.
    pub skipped_unencodable_key: usize,
    /// A carrier field named `time`, which `influxd` refuses: `metrics.skipped{reason="reserved_key"}`.
    pub skipped_reserved_key: usize,
    /// A string field over `influxd`'s 1 MiB cap: `metrics.skipped{reason="oversize_value"}`.
    pub skipped_oversize_value: usize,
    /// A line whose field set came out empty with no field drop counted above (a `Histogram`
    /// with no finite bound, a `Summary` with no finite quantile): `metrics.skipped{reason="no_fields"}`.
    pub skipped_no_fields: usize,
    /// A record flagged [`MetricRecord::FLAG_NO_RECORDED_VALUE`]: line protocol has no "no
    /// reading" marker. `metrics.skipped{reason="no_recorded_value"}`.
    pub skipped_no_recorded_value: usize,
    /// A `SetMembers` or `ExponentialHistogram` record, which has no line-protocol shape:
    /// `metrics.skipped{metric_kind="set_members"|"exponential_histogram"}`.
    pub skipped_set_members: usize,
    pub skipped_exponential_histogram: usize,
    /// An unresolved `GaugeDelta`: `metrics.skipped{metric_kind="gauge_delta"}`.
    pub skipped_gauge_delta: usize,
    /// A measurement starting with `#`, which `influxd` reads as a comment line:
    /// `metrics.skipped{reason="comment_measurement"}`.
    pub skipped_comment_measurement: usize,
    /// A series with no free timestamp slot left before `i64::MAX`:
    /// `metrics.skipped{reason="timestamp_overflow"}`.
    pub skipped_timestamp_overflow: usize,
}

/// Encodes an [`EventBatch`] as InfluxDB line protocol.
///
/// Only `metrics` and the `influxdb.field.*` carrier have a line-protocol mapping; a log body or
/// span is skipped, since InfluxDB has no established convention for either. Without the
/// `influxdb.measurement` carrier, each metric on an event becomes its own line sharing that
/// event's tags (`docs/adr/multi-payload-events.md`); with it, the event is one multi-field line
/// ([`super`]'s "Encode" section).
///
/// Public so `logit-bench` can drive it with no HTTP server: `docs/design/memory.md` pins its
/// allocation count (`influx_encode_100_events`). Reusable buffers and the batch-scoped series
/// map live on the encoder, not per call, event, or line, because short-lived per-line `String`s
/// were once the pipeline's largest allocation cost. `encode` clears each where its scope begins,
/// so reuse never changes the bytes produced.
pub struct InfluxLineEncoder {
    diag: Diagnostics,
    /// [`ATTR_MEASUREMENT`], interned once so the per-event carrier probe is a binary search.
    measurement_key: Symbol,
    /// The `,key=value` tag suffix, rebuilt once per event and shared across its lines.
    tag_suffix: String,
    /// One complete line, built before it reaches the output (see [`finish_line`]).
    line: String,
    /// The `k=v,k=v` field set for the line being assembled.
    fields: String,
    /// Scratch for rendering one non-string tag value; a `Value::Str` tag is borrowed instead.
    scratch: String,
    /// Scratch for [`allocate_timestamp`]'s walk. A fresh `Vec` would allocate on every timestamp
    /// collision, and a statsd multi-value datagram collides on nearly every line.
    visited: Vec<i64>,
    /// Per-series "next free timestamp slot" successor maps (see [`finish_line`]). Cleared at the
    /// start of every `encode`: batch-scoped contents, allocations kept across batches.
    series: HashMap<String, HashMap<i64, i64>>,
    stats: EncodeStats,
}

impl Default for InfluxLineEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl InfluxLineEncoder {
    pub fn new() -> Self {
        Self {
            diag: Diagnostics::default(),
            measurement_key: intern(ATTR_MEASUREMENT),
            tag_suffix: String::new(),
            line: String::new(),
            fields: String::new(),
            scratch: String::new(),
            visited: Vec::new(),
            series: HashMap::new(),
            stats: EncodeStats::default(),
        }
    }

    /// The handle the encoder's throttled diagnostics (`encode_error`, `gauge_delta_unresolved`,
    /// `no_recorded_value`) go through.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    /// The counts from the last [`Encoder::encode`] call.
    pub fn stats(&self) -> &EncodeStats {
        &self.stats
    }
}

impl Encoder for InfluxLineEncoder {
    fn encode(&mut self, batch: &EventBatch) -> Result<Bytes, CodecError> {
        let mut buf = String::new();
        // InfluxDB identifies a point by (measurement, tag set, timestamp); fields play no part,
        // and a second point with the same identity overwrites the first. `finish_line`
        // disambiguates same-identity points using `self.series`, which must stay batch-scoped
        // (not per-event or per-metric) to catch collisions across the whole batch.
        self.series.clear();
        self.stats = EncodeStats::default();
        let Self {
            diag,
            measurement_key,
            tag_suffix,
            line,
            fields,
            scratch,
            visited,
            series,
            stats,
        } = self;
        let mut ctx = Ctx { diag, stats, line, fields, series, visited, buf: &mut buf };
        for event in &batch.events {
            let carrier = carrier_measurement(event, *measurement_key);
            if event.metrics.is_empty() && carrier.is_none() {
                continue; // a log-only, span-only, or empty event: nothing to encode
            }
            // Tags depend only on resource and event attributes, so render once per event.
            render_tag_suffix(tag_suffix, scratch, &batch.resource, event, ctx.stats);
            match carrier {
                Some(measurement) => ctx.encode_carrier_event(tag_suffix, event, measurement),
                None => {
                    for metric in &event.metrics {
                        ctx.encode_record_line(tag_suffix, metric, event.timestamp);
                    }
                }
            }
        }
        Ok(Bytes::from(buf.into_bytes()))
    }
}

/// The event's `influxdb.measurement`, when it is a non-empty UTF-8 `Str`. Any other value isn't a
/// carrier: the event takes the per-record path, and the attribute is still never a tag.
fn carrier_measurement(event: &Event, key: Symbol) -> Option<&str> {
    match event.attributes.get_sym(key)? {
        Value::Str(s) => std::str::from_utf8(s).ok().filter(|m| !m.is_empty()),
        _ => None,
    }
}

/// The field key and value `metric` contributes to a carrier line under `measurement`: its name
/// is `<measurement>.<key>` with a non-empty key, and its kind is a `Gauge` or `Sum`.
fn carrier_field<'a>(metric: &MetricRecord, measurement: &str) -> Option<(&'a str, MetricValue)> {
    let value = match &metric.kind {
        MetricKind::Gauge(v) => *v,
        MetricKind::Sum(s) => s.value,
        _ => return None,
    };
    let key = resolve(metric.name)
        .strip_prefix(measurement)?
        .strip_prefix('.')
        .filter(|key| !key.is_empty())?;
    Some((key, value))
}

/// The encoder's per-batch state, borrowed apart so a line can be built while the tag suffix is
/// read.
struct Ctx<'a> {
    diag: &'a mut Diagnostics,
    stats: &'a mut EncodeStats,
    line: &'a mut String,
    fields: &'a mut String,
    series: &'a mut HashMap<String, HashMap<i64, i64>>,
    visited: &'a mut Vec<i64>,
    buf: &'a mut String,
}

impl Ctx<'_> {
    /// One line per metric: the record name as the measurement and a `value=` field (or a
    /// multi-value kind's fields). A per-record problem skips that record alone, never a sibling
    /// on the same event or the batch.
    fn encode_record_line(&mut self, tag_suffix: &str, metric: &MetricRecord, timestamp: i64) {
        if self.skip_no_recorded_value(metric) {
            return;
        }
        let measurement = resolve(metric.name);
        let outcome = match render_fields(self.fields, &metric.kind) {
            Ok(outcome) => outcome,
            Err(skip) => {
                self.skip_kind(skip, measurement);
                return;
            }
        };
        match outcome {
            Fields::Written => {}
            Fields::ValueDropped => {
                self.stats.skipped_unencodable_value += 1;
                return;
            }
            Fields::Empty => {
                self.stats.skipped_no_fields += 1;
                return;
            }
        }
        if !self.measurement_ok(measurement, 1) {
            return;
        }
        self.line.clear();
        push_escaped_measurement(self.line, measurement);
        self.line.push_str(tag_suffix);
        self.finish_line(timestamp, 1);
    }

    /// One line for the whole event, under the carrier's measurement ([`super`]'s "Encode"
    /// section), then a line per record that doesn't fit it ([`Ctx::encode_record_line`]),
    /// counted `carrier_mismatch`.
    fn encode_carrier_event(&mut self, tag_suffix: &str, event: &Event, measurement: &str) {
        self.fields.clear();
        let mut written = 0;
        for metric in &event.metrics {
            if self.skip_no_recorded_value(metric) {
                continue;
            }
            let Some((key, value)) = carrier_field(metric, measurement) else {
                continue;
            };
            if !self.field_key_ok(key) {
                continue;
            }
            if let MetricValue::F64(f) = value {
                if !f.is_finite() {
                    self.stats.skipped_unencodable_value += 1;
                    continue;
                }
            }
            separator(self.fields);
            push_escaped_key(self.fields, key);
            self.fields.push('=');
            push_typed_value(self.fields, value);
            written += 1;
        }
        for (key, value) in event.attributes.iter() {
            let Some(field) = resolve(key).strip_prefix(FIELD_ATTR_PREFIX) else {
                continue;
            };
            // A non-`Str` value under the prefix isn't a string field (`super`'s "Encode").
            let Some(text) = value.as_str() else {
                continue;
            };
            if !self.field_key_ok(field) {
                continue;
            }
            if text.len() > MAX_FIELD_VALUE_BYTES {
                self.stats.skipped_oversize_value += 1;
                continue;
            }
            separator(self.fields);
            push_escaped_key(self.fields, field);
            self.fields.push('=');
            push_quoted(self.fields, text);
            written += 1;
        }
        // An empty field set writes no line: every field that dropped was counted where it
        // dropped, and with no field at all there is nothing to write.
        if written > 0 && self.measurement_ok(measurement, written) {
            self.line.clear();
            push_escaped_measurement(self.line, measurement);
            self.line.push_str(tag_suffix);
            self.finish_line(event.timestamp, written);
        }
        for metric in &event.metrics {
            // Flagged records were counted by the pass above.
            if metric.is_no_recorded_value() || carrier_field(metric, measurement).is_some() {
                continue;
            }
            self.stats.metrics_normalized_carrier_mismatch += 1;
            self.encode_record_line(tag_suffix, metric, event.timestamp);
        }
    }

    /// Skips a `NO_RECORDED_VALUE`-flagged point: line protocol can't carry the flag the way
    /// OTLP does, so writing its default `0` would fabricate a reading
    /// (`docs/known-gaps/mappings.md`'s cross-protocol table; `MetricRecord`'s `flags` doc).
    fn skip_no_recorded_value(&mut self, metric: &MetricRecord) -> bool {
        if !metric.is_no_recorded_value() {
            return false;
        }
        self.stats.skipped_no_recorded_value += 1;
        self.diag.warn_throttled(
            "no_recorded_value",
            format_args!(
                "metric '{}' has no recorded value (OTLP NO_RECORDED_VALUE) -- skipped",
                resolve(metric.name)
            ),
        );
        true
    }

    fn skip_kind(&mut self, skip: KindSkip, name: &str) {
        match skip {
            KindSkip::SetMembers => {
                self.stats.skipped_set_members += 1;
                self.diag.warn_throttled(
                    "encode_error",
                    format_args!(
                        "metric {name:?}: SetMembers metrics have no line-protocol encoding yet"
                    ),
                );
            }
            KindSkip::ExponentialHistogram => {
                self.stats.skipped_exponential_histogram += 1;
                self.diag.warn_throttled(
                    "encode_error",
                    format_args!(
                        "metric {name:?}: ExponentialHistogram metrics have no line-protocol \
                         encoding yet"
                    ),
                );
            }
            // Its own key: a `GaugeDelta` here means the pipeline lacks an `aggregate`, not that
            // the metric is malformed (`docs/adr/relative-gauge-adjustments.md`).
            KindSkip::GaugeDelta => {
                self.stats.skipped_gauge_delta += 1;
                self.diag.warn_throttled(
                    "gauge_delta_unresolved",
                    "a relative gauge adjustment reached a sink unresolved -- add an `aggregate` \
                     component between the statsd input and this output",
                );
            }
        }
    }

    /// Whether `measurement` can lead a line; when it can't, the line's `fields` fields are
    /// counted skipped.
    fn measurement_ok(&mut self, measurement: &str, fields: usize) -> bool {
        // A line starting with '#' is a line-protocol comment: InfluxDB would discard it while
        // the write still reports success.
        if measurement.starts_with('#') {
            self.stats.skipped_comment_measurement += fields;
            self.diag.warn_throttled(
                "encode_error",
                format_args!(
                    "measurement name {measurement:?} can't be encoded: a leading '#' is a \
                     line-protocol comment marker"
                ),
            );
            return false;
        }
        if !measurement_encodable(measurement) {
            self.stats.skipped_unencodable_key += fields;
            return false;
        }
        true
    }

    /// Whether `key` can be a carrier field key; when it can't, the field is counted skipped.
    fn field_key_ok(&mut self, key: &str) -> bool {
        if key == RESERVED_FIELD_KEY {
            self.stats.skipped_reserved_key += 1;
            return false;
        }
        if !field_key_encodable(key) {
            self.stats.skipped_unencodable_key += 1;
            return false;
        }
        true
    }

    /// Appends ` <fields> <timestamp>\n` to the series prefix in `line` and the line to `buf`.
    /// `fields` is how many fields the line carries, for the overflow count.
    fn finish_line(&mut self, timestamp: i64, fields: usize) {
        // "measurement + tags" is the series-identity key; fields and timestamp aren't part of it.
        let series_key_len = self.line.len();
        // Disambiguate same-series collisions within this batch (see `encode`'s comment). The
        // simpler schemes are each wrong:
        // - +1ns per prior occurrence collides again on out-of-order input (101 then 100 both
        //   become 101).
        // - max(last + 1, own) per series moves a timestamp that didn't collide (101 then 100
        //   becomes 101 then 102).
        // - A `HashSet` of taken slots, re-probed from `timestamp` each time, is O(k^2) for k
        //   duplicates: `statsd_in` stamps one timestamp on a whole datagram, and its multi-value
        //   form (`x:1:1:1...|c`) expands to one event per value, so a ~65KB datagram makes
        //   k ~30,000, ~450 million lookups.
        //
        // `allocate_timestamp` is a union-find "smallest free slot >= t" allocator with path
        // compression: a free timestamp is returned untouched regardless of arrival order, a
        // collision costs a 1ns nudge, and repeated collisions stay amortized-cheap.
        //
        // Several metrics on one event (`docs/adr/multi-payload-events.md`): distinct names are
        // distinct series and keep the event's timestamp; a repeated name takes the collision
        // path and produces byte-for-byte what `k` separate events would. That holds only while
        // `series` stays batch-scoped.
        //
        // Looked up before inserting, so a series already seen costs one extra hash instead of a
        // throwaway `String` key. (`entry` needs an owned key up front, and `get_mut`-then-
        // `insert` can't share one borrow without polonius.)
        let series_key = &self.line[..series_key_len];
        if !self.series.contains_key(series_key) {
            self.series.insert(series_key.to_string(), HashMap::new());
        }
        let next_free =
            self.series.get_mut(&self.line[..series_key_len]).expect("just inserted if missing");
        let Some(timestamp) = allocate_timestamp(next_free, self.visited, timestamp) else {
            self.stats.skipped_timestamp_overflow += fields;
            let series = &self.line[..series_key_len];
            self.diag.warn_throttled(
                "encode_error",
                format_args!(
                    "no free timestamp slot for series {series:?} near {timestamp} (i64 overflow)"
                ),
            );
            return;
        };
        // A nanosecond nudge is below any input's timing resolution, and simpler than guessing
        // how to aggregate same-series samples, which the source protocol never specified.
        self.line.push(' ');
        self.line.push_str(self.fields);
        self.line.push(' ');
        push_i64(self.line, timestamp);
        self.buf.push_str(self.line);
        self.buf.push('\n');
    }
}

/// Renders one event's `,key=value,key=value` tag suffix into `suffix` (cleared first): resource
/// attributes, with event attributes overriding on a key collision. Infallible: a tag that can't
/// be represented is dropped alone, counted in `stats`, rather than failing the point.
///
/// Every `influxdb.`- and `statsd.`-prefixed key is skipped, uncounted: those are carriers
/// (rule (b), `docs/adr/lossless-transit.md`), not tags.
///
/// The maps are merge-joined ([`logit_core::attrs::merged`]) rather than cloned and re-inserted:
/// no `AttrMap` copy per event and no `resolve` -> `intern` round trip per key.
///
/// ## Multi-value tags: last-value-wins, counted
///
/// A repeated DogStatsD tag key arrives as a [`Value::Array`] in wire order
/// (`logit_proto::statsd::decode::insert_tags`). Line protocol's tag set is a map, one value per
/// key, so this renders the **last** representable element, walking backwards past unrepresentable
/// ones. Last, not first, matches what a decoder that collapsed repeated keys wrote, so existing
/// InfluxDB series don't change. [`EncodeStats::tags_normalized_multi_value`] counts only an array
/// whose chosen element reaches the wire; an empty or wholly unrepresentable `Array` is counted
/// `unrepresentable`, as any other unrepresentable value is.
fn render_tag_suffix(
    suffix: &mut String,
    scratch: &mut String,
    resource: &Resource,
    event: &Event,
    stats: &mut EncodeStats,
) {
    suffix.clear();
    for (key, value) in logit_core::attrs::merged(resource, event) {
        let key = resolve(key);
        if key.starts_with(CARRIER_PREFIX) || key.starts_with(STATSD_PREFIX) {
            continue;
        }
        let is_multi_value = matches!(value, Value::Array(_));
        let rendered = match value {
            // The chosen element is rendered twice (to find it, then to borrow it) because
            // `tag_value`'s result borrows `scratch`, so the search can't hold a candidate.
            // Neither render allocates.
            Value::Array(elements) => {
                let mut chosen = None;
                for (i, element) in elements.iter().enumerate().rev() {
                    if tag_value(scratch, element).is_some() {
                        chosen = Some(i);
                        break;
                    }
                }
                match chosen {
                    Some(i) => tag_value(scratch, &elements[i]),
                    None => None,
                }
            }
            _ => tag_value(scratch, value),
        };
        let Some(value) = rendered else {
            stats.tags_dropped_unrepresentable += 1;
            continue;
        };
        // InfluxDB 2.x rejects an empty tag key or value, and line protocol can't escape a
        // newline; either would corrupt or reject the line. Drop this tag, not the point.
        if key.is_empty() || value.is_empty() {
            stats.tags_dropped_empty += 1;
            continue;
        }
        if !tag_encodable(key) || !tag_encodable(value) {
            stats.tags_dropped_unencodable += 1;
            continue;
        }
        if RESERVED_TAG_KEYS.contains(&key) {
            stats.tags_dropped_reserved += 1;
            continue;
        }
        // Counted past every drop check: the counter means "a collapsed multi-value tag reached
        // the wire", not "an `Array` was seen".
        if is_multi_value {
            stats.tags_normalized_multi_value += 1;
        }
        suffix.push(',');
        push_escaped_key(suffix, key);
        suffix.push('=');
        push_escaped_key(suffix, value);
    }
}

/// Allocates the smallest timestamp `>= requested` not already taken in this series, recording
/// the allocation in `next_free` so a later call sees it as taken. `next_free` maps an occupied
/// timestamp to the next candidate to try after it; a timestamp with no entry is free.
///
/// Path compression: every occupied timestamp the walk passes is repointed at `free + 1` (the
/// free slot itself is about to be taken), so a later call starting anywhere on that chain,
/// including a repeat of `requested`, reaches the next free slot in one hop.
///
/// Returns `None` if the walk lands on `i64::MAX`. That slot is never recorded as occupied, so
/// `successor` never wraps and `next_free` never holds a self-loop. `i64::MAX` ns is the year
/// 2262, so no real batch reaches it.
fn allocate_timestamp(
    next_free: &mut HashMap<i64, i64>,
    visited: &mut Vec<i64>,
    requested: i64,
) -> Option<i64> {
    visited.clear();
    let mut cur = requested;
    while let Some(&next) = next_free.get(&cur) {
        visited.push(cur);
        cur = next;
    }
    // `cur` is free: reserve it and repoint every visited slot at its successor.
    let successor = cur.checked_add(1)?;
    next_free.insert(cur, successor);
    for slot in visited.drain(..) {
        next_free.insert(slot, successor);
    }
    Some(cur)
}

/// What [`render_fields`] left in its buffer.
enum Fields {
    Written,
    /// The one `value` of a `Gauge`/`Sum` was non-finite.
    ValueDropped,
    /// Every component of a multi-field kind was non-finite or absent.
    Empty,
}

/// A kind with no line-protocol shape.
enum KindSkip {
    SetMembers,
    ExponentialHistogram,
    GaugeDelta,
}

/// `Distribution` and re-sketched `Samples` fields: `count` as an unsigned integer
/// ([`push_uint`]), `sum` ([`DdSketch::sum`], `0.0` for an empty sketch), then p50/p90/p99.
/// `count` is unconditional, so there's always one field.
fn render_sketch_fields(out: &mut String, sketch: &DdSketch) {
    out.push_str("count=");
    push_uint(out, sketch.count() as u64);
    let sum = sketch.sum();
    if sum.is_finite() {
        out.push_str(",sum=");
        push_float(out, sum);
    }
    for q in [0.5, 0.9, 0.99] {
        if let Some(v) = sketch.quantile(q).filter(|v| v.is_finite()) {
            let percentile = (q * 100.0).round() as u32;
            let _ = write!(out, ",p{percentile}=");
            push_float(out, v);
        }
    }
}

/// Renders one metric's `k=v,k=v` field set into `out` (cleared first), for the per-record path.
///
/// - `Sum`/`Gauge`: one `value` field in the value's own type ([`push_typed_value`]). Line
///   protocol has no temporality or monotonicity, so a cumulative or non-monotonic `Sum` writes
///   its current `value` too.
/// - `Distribution`, and `Samples` re-sketched with each value weighted by its inverse sample rate:
///   [`render_sketch_fields`].
/// - `Histogram`: a `bucket_<bound>` field per finite bound.
/// - `Summary`: fields keyed by the raw quantile, not a rounded percentage, since rounding isn't
///   collision-free (0.991 and 0.994 both round to "p99" and would overwrite each other).
/// - `Set`: its `HyperLogLog` estimate as an unsigned `value`.
/// - `SetMembers` (raw members, no scalar to render), `ExponentialHistogram` (no line-protocol
///   shape; `docs/plans/lossless-transit.md`), and an unresolved `GaugeDelta` are skipped.
///
/// **Field names are written unescaped.** Each is a literal (`value`, `count`, `sum`) or built from
/// formatted numbers (`p50`, `bucket_1.5`, `q0.99`), and no `f64`/`u32` rendering contains a
/// backslash, comma, equals, or space. A field name derived from user input goes through
/// [`push_escaped_key`].
fn render_fields(out: &mut String, kind: &MetricKind) -> Result<Fields, KindSkip> {
    out.clear();
    match kind {
        MetricKind::Sum(s) => return Ok(push_value_field(out, s.value)),
        MetricKind::Gauge(v) => return Ok(push_value_field(out, *v)),
        MetricKind::Distribution(sketch) => render_sketch_fields(out, sketch),
        MetricKind::Samples(s) => {
            // Weighted by the inverse sample rate, the same extrapolation `aggregate` applies
            // when it builds a `Distribution` from these.
            let mut sketch = DdSketch::new();
            let weight = s.weight();
            for v in &s.values {
                sketch.add_weighted(*v, weight);
            }
            render_sketch_fields(out, &sketch);
        }
        MetricKind::Histogram(h) => {
            for (bound, count) in &h.buckets {
                if bound.is_finite() {
                    separator(out);
                    let _ = write!(out, "bucket_{bound}=");
                    push_uint(out, *count);
                }
            }
        }
        MetricKind::Summary(s) => {
            for (q, v) in &s.quantiles {
                if v.is_finite() {
                    separator(out);
                    let _ = write!(out, "q{q}=");
                    push_float(out, *v);
                }
            }
        }
        MetricKind::Set(hll) => {
            out.push_str("value=");
            push_uint(out, hll.estimate());
        }
        MetricKind::SetMembers(_) => return Err(KindSkip::SetMembers),
        MetricKind::ExponentialHistogram(_) => return Err(KindSkip::ExponentialHistogram),
        MetricKind::GaugeDelta(_) => return Err(KindSkip::GaugeDelta),
    }
    Ok(if out.is_empty() { Fields::Empty } else { Fields::Written })
}

/// Writes the `value` field of a `Sum` or `Gauge`; a non-finite float writes nothing.
fn push_value_field(out: &mut String, v: MetricValue) -> Fields {
    if let MetricValue::F64(f) = v {
        if !f.is_finite() {
            return Fields::ValueDropped;
        }
    }
    out.push_str("value=");
    push_typed_value(out, v);
    Fields::Written
}

/// One field value in its own line-protocol type (`docs/adr/typed-metric-scalar.md`): a bare
/// float (the shortest string that round-trips), `<n>i`, `<n>u`, or `true`/`false`. The caller
/// rejects a non-finite float first.
fn push_typed_value(out: &mut String, v: MetricValue) {
    match v {
        MetricValue::F64(f) => push_float(out, f),
        MetricValue::I64(n) => {
            push_i64(out, n);
            out.push('i');
        }
        MetricValue::U64(n) => push_uint(out, n),
        MetricValue::Bool(b) => out.push_str(if b { "true" } else { "false" }),
    }
}

/// Comma between field entries. Keyed off "anything written yet", not a loop index: non-finite
/// entries are skipped, and an index would leave a leading comma when the first one was.
fn separator(out: &mut String) {
    if !out.is_empty() {
        out.push(',');
    }
}

/// Formats a finite float. Line protocol has no non-finite floats, and `value=NaN` makes InfluxDB
/// reject the whole write, so every caller checks `is_finite` first; statsd's `f64::parse`
/// accepts "NaN" and "inf", so the check isn't theoretical.
///
/// `pub`: `statsd_out` renders the same `Sum`/`Gauge` values and needs identical,
/// locale-independent formatting.
pub fn push_float(out: &mut String, v: f64) {
    debug_assert!(v.is_finite(), "callers must reject non-finite values before formatting");
    let _ = write!(out, "{v}");
}

/// Line-protocol unsigned-integer field (the `u` suffix, InfluxDB 2.x). A bare number parses as
/// `f64`, losing integer semantics and, above 2^53, exactness for a long-running count.
fn push_uint(out: &mut String, v: u64) {
    let _ = write!(out, "{v}u");
}

fn push_i64(out: &mut String, v: i64) {
    let _ = write!(out, "{v}");
}

/// One tag value as a `&str`, or `None` for a `Value` with no plain-text tag form: `Null`,
/// `Bytes`, `Timestamp`, `Array`, and `Map` (a nested attribute is dropped; `flatten` exists for
/// that). A `Value::Str` is borrowed, not copied; a scalar is formatted into `scratch` (cleared
/// first). `pub`: `statsd_out` needs the identical `Value` -> tag-text mapping.
pub fn tag_value<'a>(scratch: &'a mut String, v: &'a Value) -> Option<&'a str> {
    match v {
        Value::Str(s) => std::str::from_utf8(s).ok(),
        Value::Bool(_) | Value::I64(_) | Value::U64(_) | Value::F64(_) => {
            scratch.clear();
            let _ = match v {
                Value::Bool(b) => write!(scratch, "{b}"),
                Value::I64(i) => write!(scratch, "{i}"),
                Value::U64(u) => write!(scratch, "{u}"),
                Value::F64(f) => write!(scratch, "{f}"),
                _ => unreachable!("guarded by the outer match arm"),
            };
            Some(scratch.as_str())
        }
        Value::Null | Value::Bytes(_) | Value::Timestamp(_) | Value::Array(_) | Value::Map(_) => {
            None
        }
    }
}

/// A measurement the line can lead with (this module's "Escaping"): non-empty, no line break, no
/// trailing `\`, and no `\` directly before `,`, space, or `=`.
fn measurement_encodable(s: &str) -> bool {
    if s.is_empty() || s.contains(['\n', '\r']) {
        return false;
    }
    let bytes = s.as_bytes();
    bytes
        .iter()
        .enumerate()
        .all(|(i, &b)| b != b'\\' || !matches!(bytes.get(i + 1), None | Some(b',' | b' ' | b'=')))
}

/// A tag key or value with a spelling: no line break and no trailing `\`.
fn tag_encodable(s: &str) -> bool {
    !s.contains(['\n', '\r']) && !s.ends_with('\\')
}

/// A carrier field key with a spelling: non-empty, as [`tag_encodable`], and no `\"`, which
/// `influxd` reads back as `"`.
fn field_key_encodable(s: &str) -> bool {
    !s.is_empty() && tag_encodable(s) && !s.contains("\\\"")
}

/// Appends a measurement, escaping `,` and space. `=` and `\` go as themselves.
///
/// Written into `out`, not returned: chained `.replace()` allocates per replacement whether or
/// not anything needs escaping, and dominated this encoder's allocation count
/// (`docs/design/memory.md`). The common case copies one run and allocates nothing.
fn push_escaped_measurement(out: &mut String, s: &str) {
    push_escaped(out, s, &[',', ' ']);
}

/// Appends a tag key, tag value, or field key, escaping `,`, `=`, and space. `\` goes as itself.
fn push_escaped_key(out: &mut String, s: &str) {
    push_escaped(out, s, &[',', '=', ' ']);
}

/// Appends `s` to `out`, prefixing each of `needs_escape` with a backslash. Copies in runs between
/// escapes rather than character by character, so an unescaped string is one `push_str`.
fn push_escaped(out: &mut String, s: &str, needs_escape: &[char]) {
    let mut rest = s;
    while let Some(i) = rest.find(needs_escape) {
        out.push_str(&rest[..i]);
        out.push('\\');
        // The matched character: one of `needs_escape`, so one ASCII byte.
        out.push_str(&rest[i..i + 1]);
        rest = &rest[i + 1..];
    }
    out.push_str(rest);
}

/// Appends a string field value in double quotes, `\` written `\\` and `"` written `\"`. A line
/// break goes raw: `influxd` keeps it as part of the value.
fn push_quoted(out: &mut String, s: &str) {
    out.push('"');
    push_escaped(out, s, &['\\', '"']);
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, BodyFormat, Histogram, LogRecord, MetricKind, Summary};
    use std::sync::Arc;

    fn batch_with(events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
    }

    fn metric_event(name: &str, kind: MetricKind, attrs: &[(&str, &str)]) -> Event {
        let mut attributes = AttrMap::new();
        for (k, v) in attrs {
            attributes.insert(k, *v);
        }
        Event::metric(
            1_700_000_000_000_000_000,
            attributes,
            MetricRecord::new(logit_core::interner::intern(name), kind),
        )
    }

    /// A counter event at an explicit timestamp, for the `allocate_timestamp` tests.
    fn counter_event_at(ts: i64, name: &str, v: f64) -> Event {
        let mut event = metric_event(name, MetricKind::counter(v), &[]);
        event.timestamp = ts;
        event
    }

    fn encode_stats(events: Vec<Event>) -> (String, EncodeStats) {
        let mut encoder = InfluxLineEncoder::default();
        let bytes = encoder.encode(&batch_with(events)).expect("encode should succeed");
        let out = String::from_utf8(bytes.to_vec()).expect("output should be valid utf-8");
        (out, *encoder.stats())
    }

    fn encode(events: Vec<Event>) -> String {
        encode_stats(events).0
    }

    fn log_record(message: &str) -> LogRecord {
        LogRecord {
            message: Value::str(message),
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        }
    }

    #[test]
    fn counter_line() {
        let out =
            encode(vec![metric_event("page.views", MetricKind::counter(3.0), &[("env", "prod")])]);
        assert_eq!(out, "page.views,env=prod value=3 1700000000000000000\n");
    }

    /// A typed value keeps its line-protocol type: InfluxDB rejects a write whose field type
    /// conflicts with what the series already stores.
    #[test]
    fn a_typed_value_renders_in_its_own_line_protocol_type() {
        let sum = |value| {
            MetricKind::Sum(logit_core::Sum {
                value,
                temporality: logit_core::Temporality::Delta,
                monotonic: true,
            })
        };
        let cases = [
            (sum(MetricValue::I64(5)), "m value=5i 1700000000000000000\n"),
            (sum(MetricValue::I64(-5)), "m value=-5i 1700000000000000000\n"),
            (
                sum(MetricValue::U64(u64::MAX)),
                "m value=18446744073709551615u 1700000000000000000\n",
            ),
            (MetricKind::Gauge(MetricValue::U64(5)), "m value=5u 1700000000000000000\n"),
            (MetricKind::Gauge(MetricValue::Bool(true)), "m value=true 1700000000000000000\n"),
            (MetricKind::Gauge(MetricValue::Bool(false)), "m value=false 1700000000000000000\n"),
            (MetricKind::Gauge(MetricValue::F64(5.0)), "m value=5 1700000000000000000\n"),
        ];
        for (kind, want) in cases {
            assert_eq!(encode(vec![metric_event("m", kind, &[])]), want);
        }
    }

    /// A cumulative, non-monotonic `Sum` renders the same as a delta-monotonic one.
    #[test]
    fn a_cumulative_sum_renders_the_same_as_a_counter() {
        let out = encode(vec![metric_event(
            "page.views",
            MetricKind::Sum(logit_core::Sum {
                value: logit_core::MetricValue::F64(3.0),
                temporality: logit_core::Temporality::Cumulative,
                monotonic: false,
            }),
            &[("env", "prod")],
        )]);
        assert_eq!(out, "page.views,env=prod value=3 1700000000000000000\n");
    }

    #[test]
    fn gauge_line_with_no_tags() {
        let out = encode(vec![metric_event("cpu.load", MetricKind::gauge(0.5), &[])]);
        assert_eq!(out, "cpu.load value=0.5 1700000000000000000\n");
    }

    #[test]
    fn distribution_line_has_integer_count_and_percentiles() {
        let mut sketch = logit_core::DdSketch::new();
        sketch.add(120.0);
        let out = encode(vec![metric_event("latency", MetricKind::Distribution(sketch), &[])]);
        assert!(
            out.starts_with("latency count=1u,sum=120,"),
            "count should be an unsigned int field, then the sketch's sum: {out}"
        );
        assert!(out.contains("p50="));
        assert!(out.contains("p90="));
        assert!(out.contains("p99="));
    }

    #[test]
    fn histogram_bucket_counts_are_unsigned_integers() {
        let out = encode(vec![metric_event(
            "resp.size",
            MetricKind::Histogram(Histogram {
                buckets: vec![(100.0, 5), (500.0, 2)],
                temporality: logit_core::Temporality::Cumulative,
                sum: None,
                min: None,
                max: None,
            }),
            &[],
        )]);
        assert!(out.contains("bucket_100=5u"), "got: {out}");
        assert!(out.contains("bucket_500=2u"), "got: {out}");
    }

    #[test]
    fn summary_quantile_keys_do_not_collide_when_rounded_percentage_would() {
        // 0.991 and 0.994 both round to "p99"; they must not share a field key.
        let out = encode(vec![metric_event(
            "req.latency",
            MetricKind::Summary(Summary {
                quantiles: vec![(0.991, 10.0), (0.994, 20.0)],
                count: 2,
                sum: 30.0,
            }),
            &[],
        )]);
        assert!(out.contains("q0.991=10"), "got: {out}");
        assert!(out.contains("q0.994=20"), "got: {out}");
    }

    /// Unweighted `Samples` render like a `Distribution` of the same values.
    #[test]
    fn samples_renders_like_a_distribution_built_from_the_same_values() {
        let out = encode(vec![metric_event(
            "latency",
            MetricKind::Samples(logit_core::Samples::new([120.0])),
            &[],
        )]);
        assert!(
            out.starts_with("latency count=1u,sum=120,"),
            "count should be an unsigned int field, then the sketch's sum: {out}"
        );
        assert!(out.contains("p50="));
    }

    /// A NaN `sample_rate` degrades to unweighted samples, not an empty sketch (`count=0u`):
    /// `Samples::weight` is NaN-safe where a bare `f64::clamp` isn't.
    #[test]
    fn samples_with_a_nan_sample_rate_still_render_every_observation() {
        let mut samples = logit_core::Samples::new([120.0, 130.0]);
        samples.sample_rate = f64::NAN;
        let out = encode(vec![metric_event("latency", MetricKind::Samples(samples), &[])]);
        assert!(out.starts_with("latency count=2u,sum=250,"), "got: {out}");
        assert!(out.contains("p50="), "got: {out}");
    }

    /// An unencodable `SetMembers` is skipped, counted, and a sibling metric in the batch still
    /// lands.
    #[test]
    fn set_members_has_no_line_protocol_encoding_yet() {
        let (out, stats) = encode_stats(vec![
            metric_event(
                "unique_visitors",
                MetricKind::SetMembers(vec![bytes::Bytes::from_static(b"a")]),
                &[],
            ),
            metric_event("page.views", MetricKind::counter(1.0), &[]),
        ]);
        assert!(!out.contains("unique_visitors"), "got: {out}");
        assert!(out.contains("page.views value=1"), "got: {out}");
        assert_eq!(stats.skipped_set_members, 1);
    }

    #[test]
    fn exponential_histogram_has_no_line_protocol_encoding_yet() {
        let (out, stats) = encode_stats(vec![
            metric_event(
                "resp.size",
                MetricKind::ExponentialHistogram(logit_core::ExpHistogram {
                    scale: 0,
                    zero_count: 0,
                    zero_threshold: 0.0,
                    positive: (0, vec![]),
                    negative: (0, vec![]),
                    temporality: logit_core::Temporality::Cumulative,
                    count: 0,
                    sum: None,
                    min: None,
                    max: None,
                }),
                &[],
            ),
            metric_event("page.views", MetricKind::counter(1.0), &[]),
        ]);
        assert!(!out.contains("resp.size"), "got: {out}");
        assert!(out.contains("page.views value=1"), "got: {out}");
        assert_eq!(stats.skipped_exponential_histogram, 1);
    }

    /// `statsd.*` and `influxdb.*` carriers never become tags; an ordinary sibling attribute
    /// still does.
    #[test]
    fn statsd_and_influxdb_dot_attributes_never_become_tags() {
        let mut resource = Resource::default();
        resource.attributes.insert("influxdb.bucket", "metrics");
        resource.attributes.insert("influxdb.org", "logit");
        let event = metric_event(
            "page.views",
            MetricKind::counter(1.0),
            &[
                ("statsd.type", "c"),
                ("statsd.container_id", "abcd1234"),
                ("statsd.event.title", "deploy"),
                ("statsd.service_check.message", "a|b"),
                ("influxdb.field.note", "hello"),
                ("env", "prod"),
            ],
        );
        let mut encoder = InfluxLineEncoder::default();
        let batch = EventBatch { resource: Arc::new(resource), scope: None, events: vec![event] };
        let out = String::from_utf8(encoder.encode(&batch).unwrap().to_vec()).unwrap();
        assert_eq!(out, "page.views,env=prod value=1 1700000000000000000\n");
        assert_eq!(*encoder.stats(), EncodeStats::default(), "carriers are skipped uncounted");
    }

    #[test]
    fn tag_values_with_special_characters_are_escaped() {
        let out = encode(vec![metric_event(
            "page.views",
            MetricKind::counter(1.0),
            &[("path", "a,b c=d")],
        )]);
        assert!(out.contains("path=a\\,b\\ c\\=d"), "got: {out}");
    }

    /// A backslash is written as itself in a measurement, tag key, tag value, and field key.
    /// `influxd` 2.9.1 keeps `\\` there as two backslashes, so doubling one on egress doubles it
    /// at every hop. Provenance, the W2 probe of `influxd` 2.9.1: `p01c\\x f=1i` stored measurement
    /// `p01c\\x` (row 1.3); `p02d,t\\k=v\\x f=1i` stored tag `t\\k`=`v\\x` (row 2.4); `f\\k=4i`
    /// stored field key `f\\k` (row 2.7); `p21_d,a=v\\=x` stored `v\=x` (row 2.13).
    #[test]
    fn a_backslash_is_written_as_itself_outside_a_string_value() {
        let out = encode(vec![metric_event(
            "p01c\\x",
            MetricKind::Gauge(MetricValue::I64(1)),
            &[("t\\k", "v\\x"), ("a", "v\\=x")],
        )]);
        assert_eq!(out, "p01c\\x,t\\k=v\\x,a=v\\\\=x value=1i 1700000000000000000\n");

        let mut event = carrier_event("p02g", &[("f\\k", MetricValue::I64(4))]);
        event.attributes.insert("t", "v");
        assert_eq!(encode(vec![event]), "p02g,t=v f\\k=4i 1700000000000000000\n");
    }

    /// `=` needs no escape in a measurement, and `\=` there stores a series no query reads
    /// (probe rows 1.4 `p01d=e f=1i` readable and 1.5 `p01e\=f f=1i` unreadable).
    #[test]
    fn an_equals_sign_in_a_measurement_is_written_unescaped() {
        let out = encode(vec![metric_event("p01d=e", MetricKind::counter(1.0), &[])]);
        assert_eq!(out, "p01d=e value=1 1700000000000000000\n");
        let out = encode(vec![metric_event("p01b c,d", MetricKind::counter(1.0), &[])]);
        assert_eq!(out, "p01b\\ c\\,d value=1 1700000000000000000\n");
    }

    /// A key ending in `\` has no spelling: the backslash would escape the delimiter. Probe rows
    /// 2.6 `p02f,tk=v\ f=1i` (400 `invalid tag format`), 1.6/1.7 (a measurement ending in `\`,
    /// 400 `missing fields`), and 2.8 `f\\=1i` (400 `invalid value`). A measurement with `\`
    /// before `,`, space, or `=` is stored unreadable (rows 1.5, 1.8). The tag drops alone; the
    /// measurement or field key drops its point or field.
    #[test]
    fn a_key_that_ends_in_a_backslash_is_skipped_and_counted() {
        let (out, stats) = encode_stats(vec![metric_event(
            "page.views",
            MetricKind::counter(1.0),
            &[("tk", "v\\"), ("k\\", "v"), ("env", "prod")],
        )]);
        assert_eq!(out, "page.views,env=prod value=1 1700000000000000000\n");
        assert_eq!(stats.tags_dropped_unencodable, 2);

        for name in ["p01g\\", "p01h\\,x", "p01e\\=f", "a\\ b"] {
            let (out, stats) = encode_stats(vec![
                metric_event(name, MetricKind::counter(1.0), &[]),
                metric_event("ok", MetricKind::counter(1.0), &[]),
            ]);
            assert_eq!(out, "ok value=1 1700000000000000000\n", "{name:?}");
            assert_eq!(stats.skipped_unencodable_key, 1, "{name:?}");
        }

        let (out, stats) = encode_stats(vec![carrier_event(
            "m",
            &[
                ("f\\", MetricValue::I64(1)),
                ("g\\\"k", MetricValue::I64(2)),
                ("h", MetricValue::I64(3)),
            ],
        )]);
        assert_eq!(out, "m h=3i 1700000000000000000\n");
        assert_eq!(stats.skipped_unencodable_key, 2);
    }

    /// Inside quotes, `\` is written `\\` and `"` is `\"`; `,`, space, and `=` need nothing
    /// (probe rows 3.1 `f="a\"b\\c"` stored `a"b\c`, 3.3 `f="a, b=c d"`, 3.6 `f="trail\\"`
    /// stored `trail\`).
    #[test]
    fn a_string_field_escapes_backslash_and_quote_only() {
        let mut event = carrier_event("p03a", &[]);
        event.attributes.insert("influxdb.field.f", "a\"b\\c, d=e");
        event.attributes.insert("influxdb.field.g", "trail\\");
        assert_eq!(
            encode(vec![event]),
            "p03a f=\"a\\\"b\\\\c, d=e\",g=\"trail\\\\\" 1700000000000000000\n"
        );
    }

    /// A `Bytes` attribute has no tag form and is dropped, counted.
    #[test]
    fn an_unrepresentable_tag_value_is_dropped_and_counted() {
        let mut event = metric_event("page.views", MetricKind::counter(1.0), &[("env", "prod")]);
        event.attributes.insert("host", Value::Bytes(Bytes::from_static(b"web-01")));
        event.attributes.insert("nested", Value::Map(Box::new(AttrMap::new())));
        let (out, stats) = encode_stats(vec![event]);
        assert_eq!(out, "page.views,env=prod value=1 1700000000000000000\n");
        assert_eq!(stats.tags_dropped_unrepresentable, 2);
    }

    /// `influxd` rejects the whole write for a reserved tag key (probe rows 7.1 to 7.3), and a
    /// `time` field is a `422` (row 7.4), which `influxdb_out` reads as a rejected batch.
    #[test]
    fn reserved_tag_and_field_keys_are_skipped_and_counted() {
        let (out, stats) = encode_stats(vec![metric_event(
            "m",
            MetricKind::counter(1.0),
            &[("time", "x"), ("_field", "y"), ("_measurement", "z"), ("_time", "ok")],
        )]);
        assert_eq!(out, "m,_time=ok value=1 1700000000000000000\n");
        assert_eq!(stats.tags_dropped_reserved, 3);

        let (out, stats) = encode_stats(vec![carrier_event(
            "m",
            &[("time", MetricValue::I64(1)), ("f", MetricValue::I64(2))],
        )]);
        assert_eq!(out, "m f=2i 1700000000000000000\n");
        assert_eq!(stats.skipped_reserved_key, 1);
    }

    #[test]
    fn resource_attributes_become_tags_and_event_attrs_override() {
        let mut resource = Resource::default();
        resource.attributes.insert("host", "web1");
        resource.attributes.insert("env", "staging");
        let mut event = metric_event("page.views", MetricKind::counter(1.0), &[("env", "prod")]);
        event.attributes.insert("env", "prod"); // already set by metric_event; explicit for clarity

        let mut encoder = InfluxLineEncoder::default();
        let batch = EventBatch { resource: Arc::new(resource), scope: None, events: vec![event] };
        let out = String::from_utf8(encoder.encode(&batch).unwrap().to_vec()).unwrap();

        assert!(out.contains("host=web1"), "got: {out}");
        assert!(out.contains("env=prod"), "resource's env=staging should be overridden: {out}");
        assert!(!out.contains("env=staging"), "got: {out}");
    }

    /// A counter event with one `Value::Array` attribute, as a repeated DogStatsD tag arrives.
    fn event_with_array_tag(key: &str, elements: Vec<Value>) -> Event {
        let mut event = metric_event("page.views", MetricKind::counter(1.0), &[]);
        event.attributes.insert(key, Value::Array(elements));
        event
    }

    /// A multi-value tag renders its last element and is counted once per attribute.
    #[test]
    fn a_multi_value_tag_renders_its_last_element_and_is_counted() {
        let (out, stats) = encode_stats(vec![event_with_array_tag(
            "team",
            vec![Value::str("a"), Value::str("b")],
        )]);
        assert_eq!(out, "page.views,team=b value=1 1700000000000000000\n");
        assert_eq!(stats.tags_normalized_multi_value, 1, "once per attribute, not per element");
    }

    #[test]
    fn a_multi_value_tag_whose_last_element_is_unrepresentable_falls_back_to_the_previous_one() {
        let (out, stats) = encode_stats(vec![event_with_array_tag(
            "team",
            vec![Value::str("a"), Value::str("b"), Value::Null],
        )]);
        assert!(out.contains("team=b"), "got: {out}");
        assert_eq!(stats.tags_normalized_multi_value, 1);

        // And it keeps walking backwards past more than one of them.
        let (out, stats) = encode_stats(vec![event_with_array_tag(
            "team",
            vec![Value::str("a"), Value::Timestamp(1), Value::Map(Box::new(AttrMap::new()))],
        )]);
        assert!(out.contains("team=a"), "got: {out}");
        assert_eq!(stats.tags_normalized_multi_value, 1);
    }

    /// An empty or wholly unrepresentable `Array` drops the tag, counted `unrepresentable`, not
    /// `multi_value`.
    #[test]
    fn an_empty_or_all_unrepresentable_array_tag_drops_the_tag() {
        let (out, stats) = encode_stats(vec![event_with_array_tag("team", Vec::new())]);
        assert_eq!(out, "page.views value=1 1700000000000000000\n");
        assert_eq!(stats.tags_normalized_multi_value, 0);
        assert_eq!(stats.tags_dropped_unrepresentable, 1);

        let (out, stats) = encode_stats(vec![event_with_array_tag(
            "team",
            vec![Value::Null, Value::Map(Box::new(AttrMap::new()))],
        )]);
        assert_eq!(out, "page.views value=1 1700000000000000000\n");
        assert_eq!(stats.tags_normalized_multi_value, 0);
        assert_eq!(stats.tags_dropped_unrepresentable, 1);
    }

    /// The stats are zeroed per `encode`, not accumulated across batches.
    #[test]
    fn the_stats_are_zeroed_per_encode_not_accumulated_across_batches() {
        let mut encoder = InfluxLineEncoder::default();
        for _ in 0..3 {
            let batch = batch_with(vec![event_with_array_tag(
                "team",
                vec![Value::str("a"), Value::str("b")],
            )]);
            encoder.encode(&batch).expect("encode should succeed");
            assert_eq!(encoder.stats().tags_normalized_multi_value, 1);
        }
    }

    /// A `NO_RECORDED_VALUE` point is skipped, not written as `value=0`; a sibling still lands.
    #[test]
    fn a_no_recorded_value_point_is_skipped_not_written_as_a_fabricated_zero() {
        let mut flagged = metric_event("conns", MetricKind::gauge(0.0), &[]);
        flagged.metrics[0].flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
        let (out, stats) =
            encode_stats(vec![flagged, metric_event("page.views", MetricKind::counter(1.0), &[])]);
        assert!(!out.contains("conns"), "a flagged point must not be written at all: {out}");
        assert!(out.contains("page.views value=1"), "got: {out}");
        assert_eq!(stats.skipped_no_recorded_value, 1);
    }

    /// A `Set` renders its `HyperLogLog` estimate as an unsigned `value` field.
    #[test]
    fn set_metrics_render_their_hyperloglog_estimate() {
        let mut hll = logit_core::HyperLogLog::default();
        hll.insert(b"a");
        hll.insert(b"b");
        let out = encode(vec![
            metric_event("unique.users", MetricKind::Set(hll), &[]),
            metric_event("page.views", MetricKind::counter(1.0), &[]),
        ]);
        assert!(out.contains("unique.users value=2u"), "got: {out}");
        assert!(out.contains("page.views value=1"), "got: {out}");
    }

    /// An unresolved `GaugeDelta` is dropped, not written as absolute; a sibling still lands.
    #[test]
    fn gauge_delta_is_skipped_not_fatal() {
        let (out, stats) = encode_stats(vec![
            metric_event("conns", MetricKind::GaugeDelta(logit_core::MetricValue::F64(5.0)), &[]),
            metric_event("page.views", MetricKind::counter(1.0), &[]),
        ]);
        assert!(!out.contains("conns"), "got: {out}");
        assert!(out.contains("page.views value=1"), "got: {out}");
        assert_eq!(stats.skipped_gauge_delta, 1);
    }

    /// A `GaugeDelta` reports `gauge_delta_unresolved`, not the generic `encode_error`.
    #[test]
    fn gauge_delta_reports_its_own_diagnostic_key() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("out", "influxdb_out", "sink");
        let mut encoder = InfluxLineEncoder::default()
            .with_diagnostics(Diagnostics::new("out").with_telemetry(telemetry));
        let batch = batch_with(vec![metric_event(
            "conns",
            MetricKind::GaugeDelta(logit_core::MetricValue::F64(5.0)),
            &[],
        )]);
        let _ = encoder.encode(&batch);

        let events = registry.drain(0);
        let has_key = events.iter().any(|e| {
            e.attributes.get("key").and_then(|v| v.as_str()) == Some("gauge_delta_unresolved")
        });
        assert!(
            has_key,
            "expected a logit.component.diagnostics{{key=\"gauge_delta_unresolved\"}} point"
        );
    }

    #[test]
    fn non_finite_values_are_skipped_and_counted_not_written_as_invalid_line_protocol() {
        let (out, stats) =
            encode_stats(vec![metric_event("bad", MetricKind::counter(f64::NAN), &[])]);
        assert_eq!(out, "", "a NaN-only line should produce no output, not `value=NaN`");
        assert_eq!(stats.skipped_unencodable_value, 1);
        assert_eq!(stats.skipped_no_fields, 0, "the value drop already explains the line");
    }

    /// A `Histogram` with no finite bound has no field to write, and says so.
    #[test]
    fn a_line_with_no_fields_is_skipped_and_counted() {
        let (out, stats) = encode_stats(vec![metric_event(
            "resp.size",
            MetricKind::Histogram(Histogram {
                buckets: vec![(f64::INFINITY, 5)],
                temporality: logit_core::Temporality::Cumulative,
                sum: None,
                min: None,
                max: None,
            }),
            &[],
        )]);
        assert_eq!(out, "");
        assert_eq!(stats.skipped_no_fields, 1);
    }

    #[test]
    fn events_with_no_metrics_are_skipped() {
        let log_event = Event::log(0, AttrMap::new(), log_record("hello"));
        assert_eq!(encode(vec![log_event]), "");
    }

    #[test]
    fn measurement_name_starting_with_hash_is_rejected_not_silently_dropped() {
        let (out, stats) = encode_stats(vec![
            metric_event("#requests", MetricKind::counter(1.0), &[]),
            metric_event("page.views", MetricKind::counter(1.0), &[]),
        ]);
        assert!(!out.contains("#requests"), "got: {out}");
        assert!(out.contains("page.views value=1"), "got: {out}");
        assert_eq!(stats.skipped_comment_measurement, 1);
    }

    #[test]
    fn empty_tag_value_is_dropped_not_the_whole_metric() {
        let (out, stats) = encode_stats(vec![metric_event(
            "page.views",
            MetricKind::counter(1.0),
            &[("env", "")],
        )]);
        assert!(!out.contains("env="), "empty tag should be dropped entirely: {out}");
        assert!(out.contains("page.views value=1"), "the rest of the point should survive: {out}");
        assert_eq!(stats.tags_dropped_empty, 1);
    }

    #[test]
    fn tag_value_with_embedded_newline_does_not_corrupt_the_rest_of_the_batch() {
        // A newline-bearing tag must not leave a fragment that corrupts the next line.
        let (out, stats) = encode_stats(vec![
            metric_event("ok.before", MetricKind::counter(1.0), &[]),
            metric_event("bad", MetricKind::counter(1.0), &[("env", "prod\ninjected")]),
            metric_event("ok.after", MetricKind::counter(1.0), &[]),
        ]);
        assert!(out.contains("ok.before value=1"), "got: {out}");
        assert!(out.contains("ok.after value=1"), "got: {out}");
        assert!(!out.contains("injected"), "got: {out}");
        assert_eq!(out.lines().count(), 3, "expected exactly 3 well-formed lines, got: {out}");
        assert_eq!(stats.tags_dropped_unencodable, 1);
    }

    /// A record name with a line break can't lead a line.
    #[test]
    fn a_measurement_with_a_newline_is_skipped_not_split_across_lines() {
        let (out, stats) = encode_stats(vec![
            metric_event("bad\ninjected", MetricKind::counter(1.0), &[]),
            metric_event("ok", MetricKind::counter(1.0), &[]),
        ]);
        assert_eq!(out, "ok value=1 1700000000000000000\n");
        assert_eq!(stats.skipped_unencodable_key, 1);
    }

    #[test]
    fn multi_value_samples_in_one_batch_are_not_collapsed_by_influxdb_point_identity() {
        // statsd's `name:1:2:3|c` decodes to three same-identity events; all three must survive.
        let same_ts = 1_700_000_000_000_000_000;
        let events: Vec<Event> = [1.0, 2.0, 3.0]
            .into_iter()
            .map(|v| counter_event_at(same_ts, "page.views", v))
            .collect();

        let out = encode(events);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 3, "expected 3 distinct points, got: {out}");

        let timestamps: std::collections::HashSet<&str> =
            lines.iter().map(|l| l.rsplit(' ').next().unwrap()).collect();
        assert_eq!(timestamps.len(), 3, "each point must get a distinct timestamp: {out}");

        for value in ["value=1", "value=2", "value=3"] {
            assert!(out.contains(value), "expected {value} to survive, got: {out}");
        }
    }

    #[test]
    fn out_of_order_same_series_timestamps_keep_their_real_value_when_unoccupied() {
        // 101 then 100 must emit 101 and 100, not max(last + 1, own)'s 101 and 102.
        let events = vec![
            counter_event_at(101, "page.views", 1.0),
            counter_event_at(100, "page.views", 2.0),
        ];

        let out = encode(events);
        assert!(out.contains("value=1 101"), "got: {out}");
        assert!(
            out.contains("value=2 100"),
            "timestamp 100 was free and must be kept as-is, not bumped to 102: {out}"
        );
    }

    #[test]
    fn large_same_timestamp_batch_allocates_a_contiguous_range_without_quadratic_blowup() {
        // Also the O(N^2) regression guard (an effective hang at this N); don't shrink N.
        const N: i64 = 50_000;
        let start_ts = 1_700_000_000_000_000_000;
        let events: Vec<Event> =
            (0..N).map(|_| counter_event_at(start_ts, "page.views", 1.0)).collect();

        let out = encode(events);
        let mut timestamps: Vec<i64> =
            out.lines().map(|l| l.rsplit(' ').next().unwrap().parse().unwrap()).collect();
        timestamps.sort_unstable();

        assert_eq!(timestamps.len(), N as usize, "expected {N} distinct points");
        let expected: Vec<i64> = (start_ts..start_ts + N).collect();
        assert_eq!(
            timestamps, expected,
            "allocated timestamps must be exactly the contiguous range [start, start+N)"
        );
    }

    #[test]
    fn interleaved_timestamps_on_one_series_allocate_without_gaps_or_duplicates() {
        // Overlapping probe ranges (100 x3, 101 x2): path compression must not skip a free slot.
        let events: Vec<Event> = [100, 100, 100, 101, 101]
            .into_iter()
            .map(|ts| counter_event_at(ts, "page.views", 1.0))
            .collect();

        let out = encode(events);
        let mut timestamps: Vec<i64> =
            out.lines().map(|l| l.rsplit(' ').next().unwrap().parse().unwrap()).collect();
        timestamps.sort_unstable();
        assert_eq!(timestamps, vec![100, 101, 102, 103, 104], "got: {out}");
    }

    /// Distinct metrics on one event each get a line with the event's tags and timestamp.
    #[test]
    fn several_metrics_on_one_event_share_its_tags_and_each_get_a_line() {
        let mut event = metric_event("requests", MetricKind::counter(1.0), &[("env", "prod")]);
        event.metrics.push(MetricRecord::new(
            logit_core::interner::intern("latency"),
            MetricKind::gauge(5.0),
        ));
        event.metrics.push(MetricRecord::new(
            logit_core::interner::intern("bytes"),
            MetricKind::counter(100.0),
        ));

        let out = encode(vec![event]);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 3, "one line per metric, got: {out}");
        for line in &lines {
            assert!(line.contains("env=prod"), "every line should share the event's tags: {line}");
            assert!(
                line.ends_with(" 1700000000000000000"),
                "distinct measurements should never collide: {line}"
            );
        }
    }

    /// A metric name repeated on one event takes the same collision path as separate events.
    #[test]
    fn the_same_metric_name_twice_on_one_event_gets_distinct_timestamps() {
        let mut event = metric_event("page.views", MetricKind::counter(1.0), &[]);
        event.metrics.push(MetricRecord::new(
            logit_core::interner::intern("page.views"),
            MetricKind::counter(2.0),
        ));

        let out = encode(vec![event]);
        assert_eq!(out.lines().count(), 2, "got: {out}");
        assert!(out.contains("value=1 1700000000000000000"), "got: {out}");
        assert!(out.contains("value=2 1700000000000000001"), "got: {out}");
    }

    /// A bad metric sharing an event with a good one skips only itself.
    #[test]
    fn a_bad_metric_skips_only_itself_not_the_rest_of_its_event() {
        let mut event = metric_event(
            "unique.users",
            MetricKind::SetMembers(vec![bytes::Bytes::from_static(b"a")]),
            &[],
        );
        event.metrics.push(MetricRecord::new(
            logit_core::interner::intern("page.views"),
            MetricKind::counter(1.0),
        ));

        let out = encode(vec![event]);
        assert!(!out.contains("unique.users"), "got: {out}");
        assert!(
            out.contains("page.views value=1"),
            "the good metric on the same event should still land: {out}"
        );
    }

    /// A log event carrying a derived metric writes the metric and ignores the log body.
    #[test]
    fn a_mixed_log_and_metric_event_writes_the_metric_and_ignores_the_log() {
        let mut event =
            Event::log(1_700_000_000_000_000_000, AttrMap::new(), log_record("GET / HTTP/1.1"));
        event.metrics.push(MetricRecord::new(
            logit_core::interner::intern("nginx.requests"),
            MetricKind::counter(1.0),
        ));

        let out = encode(vec![event]);
        assert_eq!(out, "nginx.requests value=1 1700000000000000000\n");
    }

    // -- The `influxdb.*` carrier (`super`'s "Encode") ------------------------------------------

    /// An event as `influxdb_in` decodes one point: the measurement carrier and one `Gauge`
    /// record per field, named `<measurement>.<field>`.
    fn carrier_event(measurement: &str, fields: &[(&str, MetricValue)]) -> Event {
        let mut attributes = AttrMap::new();
        attributes.insert(ATTR_MEASUREMENT, measurement);
        let mut event = Event::empty(1_700_000_000_000_000_000, attributes);
        for (key, value) in fields {
            event.metrics.push(MetricRecord::new(
                intern(&format!("{measurement}.{key}")),
                MetricKind::Gauge(*value),
            ));
        }
        event
    }

    /// One line per carrier event: typed numeric fields in record order, then string fields,
    /// tags from the other attributes.
    #[test]
    fn a_carrier_event_writes_one_multi_field_line() {
        let mut event = carrier_event(
            "cpu",
            &[
                ("usage_user", MetricValue::F64(1.5)),
                ("procs", MetricValue::I64(-3)),
                ("bytes", MetricValue::U64(u64::MAX)),
                ("online", MetricValue::Bool(true)),
            ],
        );
        event.attributes.insert("host", "a");
        event.attributes.insert("influxdb.field.state", "running");
        let (out, stats) = encode_stats(vec![event]);
        assert_eq!(
            out,
            "cpu,host=a usage_user=1.5,procs=-3i,bytes=18446744073709551615u,online=true,\
             state=\"running\" 1700000000000000000\n"
        );
        assert_eq!(stats, EncodeStats::default());
    }

    /// A point with only string fields has no metrics and still writes its line.
    #[test]
    fn a_string_only_carrier_event_still_writes_its_line() {
        let mut event = carrier_event("syslog", &[]);
        event.attributes.insert("influxdb.field.message", "hello world");
        event.attributes.insert("appname", "sshd");
        let out = encode(vec![event]);
        assert_eq!(out, "syslog,appname=sshd message=\"hello world\" 1700000000000000000\n");
    }

    /// A record without the `<measurement>.` prefix, or not a `Gauge`/`Sum`, takes the per-record
    /// path, counted `carrier_mismatch`; the matching records still share the carrier line.
    #[test]
    fn a_record_that_does_not_fit_the_carrier_takes_its_own_line() {
        let mut event = carrier_event("cpu", &[("usage", MetricValue::F64(1.0))]);
        event.attributes.insert("host", "a");
        event.metrics.push(MetricRecord::new(intern("mem.free"), MetricKind::gauge(2.0)));
        event.metrics.push(MetricRecord::new(intern("cpu."), MetricKind::gauge(4.0)));
        let mut hll = logit_core::HyperLogLog::default();
        hll.insert(b"a");
        event.metrics.push(MetricRecord::new(intern("cpu.users"), MetricKind::Set(hll)));
        let (out, stats) = encode_stats(vec![event]);
        assert_eq!(
            out,
            "cpu,host=a usage=1 1700000000000000000\n\
             mem.free,host=a value=2 1700000000000000000\n\
             cpu.,host=a value=4 1700000000000000000\n\
             cpu.users,host=a value=1u 1700000000000000000\n"
        );
        assert_eq!(stats.metrics_normalized_carrier_mismatch, 3);
    }

    /// A `Sum` fits the carrier too; its temporality and monotonicity are dropped, as on the
    /// per-record path.
    #[test]
    fn a_sum_record_fits_the_carrier() {
        let mut event = carrier_event("net", &[]);
        event.metrics.push(MetricRecord::new(intern("net.bytes"), MetricKind::counter(7.0)));
        assert_eq!(encode(vec![event]), "net bytes=7 1700000000000000000\n");
    }

    /// A non-finite carrier field drops alone, counted; a line left with no field writes nothing.
    #[test]
    fn a_non_finite_carrier_field_is_skipped_and_counted() {
        let event =
            carrier_event("m", &[("a", MetricValue::F64(f64::NAN)), ("b", MetricValue::F64(2.0))]);
        let (out, stats) = encode_stats(vec![event]);
        assert_eq!(out, "m b=2 1700000000000000000\n");
        assert_eq!(stats.skipped_unencodable_value, 1);

        let event = carrier_event("m", &[("a", MetricValue::F64(f64::INFINITY))]);
        let (out, stats) = encode_stats(vec![event]);
        assert_eq!(out, "");
        assert_eq!(stats.skipped_unencodable_value, 1);
        assert_eq!(stats.skipped_no_fields, 0);
    }

    /// A batch with no carrier encodes as it did before the carrier existed: one line per
    /// record, the record name as the measurement.
    #[test]
    fn a_batch_with_no_carrier_keeps_the_per_record_lines() {
        let mut event = metric_event("cpu.usage_user", MetricKind::gauge(1.5), &[("host", "a")]);
        event.metrics.push(MetricRecord::new(intern("cpu.procs"), MetricKind::gauge(3.0)));
        assert_eq!(
            encode(vec![event]),
            "cpu.usage_user,host=a value=1.5 1700000000000000000\n\
             cpu.procs,host=a value=3 1700000000000000000\n"
        );
    }

    /// A carrier whose value isn't a non-empty `Str` selects the per-record path, and is still
    /// never a tag.
    #[test]
    fn a_non_string_carrier_is_not_a_carrier() {
        let mut event = metric_event("cpu.usage", MetricKind::gauge(1.0), &[]);
        event.attributes.insert(ATTR_MEASUREMENT, Value::I64(1));
        assert_eq!(encode(vec![event]), "cpu.usage value=1 1700000000000000000\n");
    }

    /// Two carrier points in one series and batch get distinct timestamps, as two records do.
    #[test]
    fn two_carrier_points_in_one_series_take_the_collision_path() {
        let a = carrier_event("m", &[("f", MetricValue::I64(1))]);
        let b = carrier_event("m", &[("f", MetricValue::I64(2))]);
        assert_eq!(encode(vec![a, b]), "m f=1i 1700000000000000000\nm f=2i 1700000000000000001\n");
    }

    /// A string field over `influxd`'s 1 MiB cap is a `422` (probe row 11.4), which would cost
    /// the whole batch at `influxdb_out`; it drops alone.
    #[test]
    fn an_oversize_string_field_is_skipped_and_counted() {
        let mut event = carrier_event("m", &[("f", MetricValue::I64(1))]);
        event
            .attributes
            .insert("influxdb.field.big", "a".repeat(MAX_FIELD_VALUE_BYTES + 1).as_str());
        let (out, stats) = encode_stats(vec![event]);
        assert_eq!(out, "m f=1i 1700000000000000000\n");
        assert_eq!(stats.skipped_oversize_value, 1);
    }

    /// A string value with a raw newline goes raw: `influxd` keeps it in the value (probe row 3.4).
    #[test]
    fn a_string_field_keeps_a_raw_newline() {
        let mut event = carrier_event("p03d", &[]);
        event.attributes.insert("influxdb.field.f", "line1\nline2");
        assert_eq!(encode(vec![event]), "p03d f=\"line1\nline2\" 1700000000000000000\n");
    }
}
