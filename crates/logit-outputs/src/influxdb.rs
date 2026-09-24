//! InfluxDB 2.x line-protocol output: `POST /api/v2/write` with org/bucket query params and a
//! `Token` auth header. Matches the `influxdb` service seeded in `compose.yaml`.
//!
//! The reference `Encoder` sink: [`InfluxLineEncoder`] renders one opaque body per batch, and
//! [`InfluxDbOutput::send`] makes one attempt and classifies the outcome as a [`Fault`]; retry
//! timing belongs to `logit-pipeline`'s writer (`docs/adr/buffered-sink-delivery.md`).
//!
//! It keeps its own client and classifier (`status_class`, `is_retryable_status`,
//! `classify_transport_error`) rather than `crate::http`'s, though it reads a rejection body
//! through `crate::http::read_body_prefix`, the same bounded read `otlp_out` uses. The
//! classifier table is the same today, but its client never disables redirects, so it inherits
//! `reqwest`'s `limited(10)`: a tracked gap in `docs/known-gaps.md`, closed by moving to
//! `crate::http::build_client`.
//!
//! [`render_tag_suffix`] never emits a `statsd.`-prefixed attribute as a tag; see its doc.

use crate::http::{body_snippet, read_body_prefix, ERROR_BODY_SNIPPET_BYTES};
use crate::Output;
use anyhow::Context;
use bytes::Bytes;
use logit_core::interner::resolve;
use logit_core::{
    DdSketch, Diagnostics, Event, EventBatch, MetricKind, MetricRecord, Resource, Telemetry, Value,
};
use logit_pipeline::Fault;
use logit_proto::{CodecError, Encoder};
use std::collections::HashMap;
// `write!` into a `String`: formats straight into the output buffer, no `String` per number
// (`docs/design/memory.md`).
use std::fmt::Write;
use std::time::Duration;

/// `reqwest` has no request timeout by default; without one, a server that accepts the connection
/// but never responds hangs `send`, and the pipeline worker driving it, forever.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// 429 (InfluxDB's rate-limit response) and any 5xx are transient, [`Fault::Ambiguous`]; 429 is
/// the one 4xx exception (ADR `service-lifecycle-and-output-retry`). Every other 4xx is a config
/// error (bad org, bucket, or token), [`Fault::Permanent`].
fn is_retryable_status(status: reqwest::StatusCode) -> bool {
    status.is_server_error() || status.as_u16() == 429
}

pub struct InfluxDbOutput {
    url: String,
    org: String,
    bucket: String,
    token: String,
    client: reqwest::Client,
    encoder: InfluxLineEncoder,
    /// Per-request timeout: what `with_timeout` set, else [`DEFAULT_TIMEOUT`]. `send` makes one
    /// attempt, so there's no retry budget to clamp it against.
    request_timeout: Duration,
    /// Layer-3 detail (`docs/design/internal-telemetry.md`): the response class, which
    /// `run_output`'s `logit.component.send.*` can't see inside one `send`.
    telemetry: Telemetry,
}

impl InfluxDbOutput {
    pub fn new(url: String, org: String, bucket: String, token: String) -> Self {
        Self {
            url,
            org,
            bucket,
            token,
            client: build_client(DEFAULT_TIMEOUT),
            encoder: InfluxLineEncoder::default(),
            request_timeout: DEFAULT_TIMEOUT,
            telemetry: Telemetry::default(),
        }
    }

    /// Overrides the default 10s request timeout. Rebuilds the underlying HTTP client.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.client = build_client(timeout);
        self.request_timeout = timeout;
        self
    }

    /// Attaches the encoder's diagnostics handle (per-metric encode failures). Retry diagnostics
    /// come from `logit-pipeline`'s writer, not this sink.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.encoder = self.encoder.with_diagnostics(diag);
        self
    }

    /// Attaches the layer-3 telemetry handle.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

/// A coarse response-status bucket, `&'static str` so it's a telemetry tag value with no
/// per-response allocation or interning.
fn status_class(status: reqwest::StatusCode) -> &'static str {
    match status.as_u16() / 100 {
        1 => "1xx",
        2 => "2xx",
        3 => "3xx",
        4 => "4xx",
        5 => "5xx",
        _ => "other",
    }
}

fn build_client(timeout: Duration) -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .expect("reqwest client should build with default TLS settings")
}

#[async_trait::async_trait]
impl Output for InfluxDbOutput {
    /// One attempt per call, no loop or sleep: retry timing and budget belong to
    /// `logit-pipeline`'s writer (`docs/adr/buffered-sink-delivery.md`). This classifies the
    /// outcome and attaches it as `.context(fault)`.
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let body = self.encoder.encode(batch)?;
        // Before the empty-body return: a batch whose every line was unencodable still normalized
        // its tags, and an operator chasing a missing tag value needs to see that. Guarded so an
        // ordinary batch doesn't upsert a permanent zero series.
        if self.encoder.multi_value_tags > 0 {
            self.telemetry.count(
                "logit.output.tags.normalized",
                self.encoder.multi_value_tags as f64,
                &[("reason", "multi_value")],
            );
        }
        if body.is_empty() {
            // Nothing had a line-protocol encoding (e.g. log- or span-only events, or every
            // metric was skipped by `render_fields`). Not an error; nothing to write.
            return Ok(());
        }

        self.telemetry.count("logit.output.batch.bytes", body.len() as f64, &[]);

        let write_url = format!("{}/api/v2/write", self.url.trim_end_matches('/'));

        let request_timer = self.telemetry.timer("logit.output.request.duration");
        let result = self
            .client
            .post(&write_url)
            .query(&[
                ("org", self.org.as_str()),
                ("bucket", self.bucket.as_str()),
                ("precision", "ns"),
            ])
            .header("Authorization", format!("Token {}", self.token))
            .header("Content-Type", "text/plain; charset=utf-8")
            .timeout(self.request_timeout)
            .body(body)
            .send()
            .await;
        drop(request_timer);

        match result {
            Ok(resp) if resp.status().is_success() => {
                self.telemetry.count(
                    "logit.output.requests",
                    1.0,
                    &[("class", status_class(resp.status()))],
                );
                Ok(())
            }
            Ok(resp) => {
                let status = resp.status();
                self.telemetry.count(
                    "logit.output.requests",
                    1.0,
                    &[("class", status_class(status))],
                );
                // A bounded read, not `text()`: see `crate::http::read_body_prefix`.
                let text = body_snippet(
                    &read_body_prefix(resp, ERROR_BODY_SNIPPET_BYTES).await,
                    ERROR_BODY_SNIPPET_BYTES,
                );
                let fault =
                    if is_retryable_status(status) { Fault::Ambiguous } else { Fault::Permanent };
                Err(anyhow::anyhow!("InfluxDB write failed ({status}): {text}")).context(fault)
            }
            Err(err) => {
                self.telemetry.count("logit.output.requests", 1.0, &[("class", "network_error")]);
                let fault = classify_transport_error(&err);
                Err(anyhow::Error::new(err)).context(fault)
            }
        }
    }

    /// Every point's timestamp derives from `event.timestamp`, and the per-batch collision map
    /// (`InfluxLineEncoder::series`) is cleared at the top of every `encode`, so a retry re-encodes
    /// byte-for-byte the same body. InfluxDB treats an identical `(measurement, tag set,
    /// timestamp)` write as an idempotent overwrite, not a second point.
    /// See `docs/adr/buffered-sink-delivery.md`.
    fn duplicate_safe(&self) -> bool {
        true
    }
}

/// Classifies a failure that got no HTTP response. `is_connect()` means the connection was never
/// established (refused, DNS failure): the destination never saw the batch, `Fault::Clean`,
/// pinned by `connect_refused_is_reliably_classified_as_a_clean_fault`. Anything else (a timeout,
/// a failed body read) may have reached the destination, so it's `Fault::Ambiguous`:
/// `at_most_once`'s duplicate-safety argument depends on `Clean` never over-claiming.
fn classify_transport_error(err: &reqwest::Error) -> Fault {
    if err.is_connect() {
        Fault::Clean
    } else {
        Fault::Ambiguous
    }
}

/// Encodes an [`EventBatch`] as InfluxDB line protocol.
///
/// Only `metrics` have a line-protocol mapping; a log body or span is skipped, since InfluxDB has
/// no established convention for either. Each metric on an event becomes its own line sharing
/// that event's tags (`docs/adr/multi-payload-events.md`).
///
/// Public so `logit-bench` can drive it with no HTTP server: `docs/design/memory.md` pins its
/// allocation count (`influx_encode_100_events`). Reusable buffers and the batch-scoped series
/// map live on the encoder, not per call, event, or line, because short-lived per-line `String`s
/// were once the pipeline's largest allocation cost. `encode` clears each where its scope begins,
/// so reuse never changes the bytes produced.
#[derive(Default)]
pub struct InfluxLineEncoder {
    diag: Diagnostics,
    /// The `,key=value` tag suffix, rebuilt once per event and shared across its metrics.
    tag_suffix: String,
    /// One complete line, built before it reaches the output (see [`encode_metric_line`]).
    line: String,
    /// The `k=v,k=v` field set for the line being assembled.
    fields: String,
    /// Scratch for rendering one non-string tag value; a `Value::Str` tag is borrowed instead.
    scratch: String,
    /// Scratch for [`allocate_timestamp`]'s walk. A fresh `Vec` would allocate on every timestamp
    /// collision, and a statsd multi-value datagram collides on nearly every line.
    visited: Vec<i64>,
    /// Per-series "next free timestamp slot" successor maps (see [`encode_metric_line`]). Cleared
    /// at the start of every `encode`: batch-scoped contents, allocations kept across batches.
    series: HashMap<String, HashMap<i64, i64>>,
    /// Multi-value tags this batch collapsed to their last element (see [`render_tag_suffix`]).
    /// Zeroed at the top of `encode` and read by [`InfluxDbOutput::send`] right after, because
    /// [`Encoder::encode`] returns only one opaque `Bytes`. `pub` because `logit-bench` drives
    /// this encoder directly.
    pub multi_value_tags: usize,
}

impl InfluxLineEncoder {
    fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }
}

impl Encoder for InfluxLineEncoder {
    fn encode(&mut self, batch: &EventBatch) -> Result<Bytes, CodecError> {
        let mut buf = String::new();
        // InfluxDB identifies a point by (measurement, tag set, timestamp); fields play no part,
        // and a second point with the same identity overwrites the first. `encode_metric_line`
        // disambiguates same-identity points using `self.series`, which must stay batch-scoped
        // (not per-event or per-metric) to catch collisions across the whole batch.
        self.series.clear();
        // Batch-scoped too: `send` reads it once per `encode`.
        self.multi_value_tags = 0;
        for event in &batch.events {
            if event.metrics.is_empty() {
                continue; // a log-only, span-only, or empty event: nothing to encode
            }
            // Tags depend only on resource and event attributes, so render once per event.
            render_tag_suffix(
                &mut self.tag_suffix,
                &mut self.scratch,
                &batch.resource,
                event,
                &mut self.multi_value_tags,
            );
            for metric in &event.metrics {
                // A `NO_RECORDED_VALUE`-flagged point has no reading, and line protocol can't
                // carry the flag the way OTLP does, so skip it under the throttled
                // `no_recorded_value` diagnostic rather than write its default `0` as real
                // (`docs/known-gaps.md`'s cross-protocol table; `MetricRecord`'s `flags` doc).
                if metric.is_no_recorded_value() {
                    self.diag.warn_throttled(
                        "no_recorded_value",
                        format_args!(
                            "metric '{}' has no recorded value (OTLP NO_RECORDED_VALUE) -- \
                             skipped",
                            resolve(metric.name)
                        ),
                    );
                    continue;
                }
                // Logged and skipped per metric, not propagated: a `SetMembers` or `#`-prefixed
                // metric must not take down a good sibling on the same event, or the batch.
                //
                // Buffers go in as separate `&mut` arguments: disjoint field borrows the borrow
                // checker accepts here but not through a `&mut self` method.
                if let Err(err) = encode_metric_line(
                    &mut buf,
                    &mut self.line,
                    &mut self.fields,
                    &self.tag_suffix,
                    metric,
                    event.timestamp,
                    &mut self.series,
                    &mut self.visited,
                ) {
                    // Its own key: a `GaugeDelta` here means the pipeline lacks an `aggregate`,
                    // not that the metric is malformed (`docs/adr/relative-gauge-adjustments.md`).
                    let key = if matches!(metric.kind, MetricKind::GaugeDelta(_)) {
                        "gauge_delta_unresolved"
                    } else {
                        "encode_error"
                    };
                    self.diag.warn_throttled(key, err);
                }
            }
        }
        Ok(Bytes::from(buf.into_bytes()))
    }
}

/// Renders one event's `,key=value,key=value` tag suffix into `suffix` (cleared first): resource
/// attributes, with event attributes overriding on a key collision. Infallible: a tag that can't
/// be represented (an empty key or value, an embedded newline, which line protocol can't escape)
/// is dropped alone rather than failing the point.
///
/// Every `statsd.`-prefixed key is skipped, uncounted, mirroring `statsd_out`'s
/// `build_tag_suffix`: those are carriers `statsd_in` stamps for `statsd_out`'s own wire segments
/// (rule (b), `docs/adr/lossless-transit.md`), not tags. Without the filter,
/// `statsd.service_check.message` or `statsd.event.title`, free text, would become an indexed,
/// high-cardinality InfluxDB tag.
///
/// The maps are merge-joined ([`crate::attrs::merged`]) rather than cloned and re-inserted: no
/// `AttrMap` copy per event and no `resolve` -> `intern` round trip per key.
///
/// ## Multi-value tags: last-value-wins, counted
///
/// A repeated DogStatsD tag key arrives as a [`Value::Array`] in wire order
/// (`logit_inputs::statsd::insert_tags`). Line protocol's tag set is a map, one value per key, so
/// this renders the **last** representable element, walking backwards past unrepresentable ones.
/// Last, not first, matches what a decoder that collapsed repeated keys wrote, so existing
/// InfluxDB series don't change. `normalized` counts only an array whose chosen element reaches
/// the wire; an empty or wholly unrepresentable `Array` drops the tag uncounted, as any other
/// unrepresentable value does.
///
/// It's reported as `logit.output.tags.normalized{reason="multi_value"}` via
/// [`InfluxLineEncoder::multi_value_tags`]. **Unlike every other `*.normalized` reason in
/// `docs/design/internal-telemetry.md`, this one is lossy**: it discards the non-last elements.
/// It's `normalized`, not `dropped`, because the tag and the point still land; read it as data
/// loss.
fn render_tag_suffix(
    suffix: &mut String,
    scratch: &mut String,
    resource: &Resource,
    event: &Event,
    normalized: &mut usize,
) {
    suffix.clear();
    for (key, value) in crate::attrs::merged(resource, event) {
        let key = resolve(key);
        if key.starts_with("statsd.") {
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
            continue;
        };
        // InfluxDB 2.x rejects an empty tag value, and line protocol can't escape a newline;
        // either would corrupt or reject the line. Drop this tag, not the point.
        if key.is_empty()
            || value.is_empty()
            || key.contains(['\n', '\r'])
            || value.contains(['\n', '\r'])
        {
            continue;
        }
        // Counted past every drop check: the counter means "a collapsed multi-value tag reached
        // the wire", not "an `Array` was seen".
        if is_multi_value {
            *normalized += 1;
        }
        suffix.push(',');
        push_escaped_tag(suffix, key);
        suffix.push('=');
        push_escaped_tag(suffix, value);
    }
}

/// One metric's line, appended to `buf`. `line` and `fields` are the encoder's reused scratch
/// buffers, cleared here.
#[allow(clippy::too_many_arguments)]
fn encode_metric_line(
    buf: &mut String,
    line: &mut String,
    fields: &mut String,
    tag_suffix: &str,
    metric: &MetricRecord,
    timestamp: i64,
    series_allocated_timestamps: &mut HashMap<String, HashMap<i64, i64>>,
    visited: &mut Vec<i64>,
) -> Result<(), CodecError> {
    if !render_fields(fields, &metric.kind)? {
        // Every field was non-finite or unrepresentable; an empty field set is invalid.
        return Ok(());
    }

    let measurement = resolve(metric.name);
    // A line starting with '#' is a line-protocol comment: InfluxDB would discard it while the
    // write still reports success.
    if measurement.starts_with('#') {
        return Err(CodecError::Malformed(format!(
            "measurement name {measurement:?} can't be encoded: a leading '#' is a line-protocol \
             comment marker"
        )));
    }

    // Built in `line`, not `buf`, so a later rejection (a timestamp overflow below) leaves no
    // partial line in `buf`. "measurement + tags" is also the series-identity key; the tag half
    // is shared across an event's metrics but the measurement isn't, so it's rebuilt per metric.
    line.clear();
    push_escaped_measurement(line, measurement);
    line.push_str(tag_suffix);
    // Where the series-identity prefix ends; fields and timestamp aren't part of it.
    let series_key_len = line.len();
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
    // distinct series and keep the event's timestamp; a repeated name takes the collision path
    // and produces byte-for-byte what `k` separate events would. That holds only while
    // `series_allocated_timestamps` stays batch-scoped.
    //
    // Looked up before inserting, so a series already seen costs one extra hash instead of a
    // throwaway `String` key. (`entry` needs an owned key up front, and `get_mut`-then-`insert`
    // can't share one borrow without polonius.)
    let series_key = &line[..series_key_len];
    if !series_allocated_timestamps.contains_key(series_key) {
        series_allocated_timestamps.insert(series_key.to_string(), HashMap::new());
    }
    let next_free = series_allocated_timestamps
        .get_mut(&line[..series_key_len])
        .expect("just inserted if it was missing");
    let timestamp = allocate_timestamp(next_free, visited, timestamp).ok_or_else(|| {
        let series = &line[..series_key_len];
        CodecError::Malformed(format!(
            "no free timestamp slot for series {series:?} near {timestamp} (i64 overflow)"
        ))
    })?;

    // A nanosecond nudge is below any input's timing resolution, and simpler than guessing how
    // to aggregate same-series samples, which the source protocol never specified.

    line.push(' ');
    line.push_str(fields);
    line.push(' ');
    push_i64(line, timestamp);

    buf.push_str(line);
    buf.push('\n');
    Ok(())
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

/// `Distribution` and re-sketched `Samples` fields: `count` as an unsigned integer
/// ([`push_uint`]) plus p50/p90/p99. `count` is unconditional, so there's always one field.
fn render_sketch_fields(out: &mut String, sketch: &DdSketch) {
    out.push_str("count=");
    push_uint(out, sketch.count() as u64);
    for q in [0.5, 0.9, 0.99] {
        if let Some(v) = sketch.quantile(q).filter(|v| v.is_finite()) {
            let percentile = (q * 100.0).round() as u32;
            let _ = write!(out, ",p{percentile}=");
            push_float(out, v);
        }
    }
}

/// Renders one metric's `k=v,k=v` field set into `out` (cleared first). Returns `false` when
/// every field was unrepresentable: an empty field set is invalid line protocol.
///
/// - `Sum`/`Gauge`: one `value` field. Line protocol has no temporality or monotonicity, so a
///   cumulative or non-monotonic `Sum` writes its current `value` too.
/// - `Distribution`, and `Samples` re-sketched with each value weighted by its inverse sample rate:
///   [`render_sketch_fields`].
/// - `Histogram`: a `bucket_<bound>` field per finite bound.
/// - `Summary`: fields keyed by the raw quantile, not a rounded percentage, since rounding isn't
///   collision-free (0.991 and 0.994 both round to "p99" and would overwrite each other).
/// - `Set`: its `HyperLogLog` estimate as an unsigned `value`.
/// - `SetMembers` (raw members, no scalar to render) and `ExponentialHistogram` (no line-protocol
///   shape; `docs/plans/lossless-transit.md`) are errors, as is an unresolved `GaugeDelta`.
///
/// **Field names are written unescaped.** Each is a literal (`value`, `count`) or built from
/// formatted numbers (`p50`, `bucket_1.5`, `q0.99`), and no `f64`/`u32` rendering contains a
/// backslash, comma, equals, or space. A field name derived from user input would have to go
/// through [`push_escaped_tag`].
fn render_fields(out: &mut String, kind: &MetricKind) -> Result<bool, CodecError> {
    out.clear();

    match kind {
        MetricKind::Sum(s) => {
            if s.value.is_finite() {
                out.push_str("value=");
                push_float(out, s.value);
            }
        }
        MetricKind::Gauge(v) => {
            if v.is_finite() {
                out.push_str("value=");
                push_float(out, *v);
            }
        }
        MetricKind::Distribution(sketch) => {
            render_sketch_fields(out, sketch);
        }
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
        MetricKind::SetMembers(_) => {
            return Err(CodecError::Malformed(
                "SetMembers metrics have no line-protocol encoding yet".to_string(),
            ))
        }
        MetricKind::ExponentialHistogram(_) => {
            return Err(CodecError::Malformed(
                "ExponentialHistogram metrics have no line-protocol encoding yet".to_string(),
            ))
        }
        MetricKind::GaugeDelta(_) => {
            return Err(CodecError::Malformed(
                "a relative gauge adjustment reached a sink unresolved -- add an `aggregate` \
                 component between the statsd input and this output"
                    .to_string(),
            ))
        }
    }
    Ok(!out.is_empty())
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
/// `pub(crate)`: `statsd_out` renders the same `Sum`/`Gauge` values and needs identical,
/// locale-independent formatting.
pub(crate) fn push_float(out: &mut String, v: f64) {
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
/// first). `pub(crate)`: `statsd_out` needs the identical `Value` -> tag-text mapping.
pub(crate) fn tag_value<'a>(scratch: &'a mut String, v: &'a Value) -> Option<&'a str> {
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

/// Appends `s` with line protocol's measurement escaping (`\`, `,`, and space).
///
/// Written into `out`, not returned: chained `.replace()` allocates per replacement whether or
/// not anything needs escaping, and dominated this encoder's allocation count
/// (`docs/design/memory.md`). The common case copies one run and allocates nothing.
fn push_escaped_measurement(out: &mut String, s: &str) {
    push_escaped(out, s, &['\\', ',', ' ']);
}

/// As [`push_escaped_measurement`], for tag keys, tag values, and field keys, which additionally
/// escape `=`.
fn push_escaped_tag(out: &mut String, s: &str) {
    push_escaped(out, s, &['\\', ',', '=', ' ']);
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

    fn encode(events: Vec<Event>) -> String {
        let mut encoder = InfluxLineEncoder::default();
        let bytes = encoder.encode(&batch_with(events)).expect("encode should succeed");
        String::from_utf8(bytes.to_vec()).expect("output should be valid utf-8")
    }

    #[test]
    fn counter_line() {
        let out =
            encode(vec![metric_event("page.views", MetricKind::counter(3.0), &[("env", "prod")])]);
        assert_eq!(out, "page.views,env=prod value=3 1700000000000000000\n");
    }

    /// A cumulative, non-monotonic `Sum` renders the same as a delta-monotonic one.
    #[test]
    fn a_cumulative_sum_renders_the_same_as_a_counter() {
        let out = encode(vec![metric_event(
            "page.views",
            MetricKind::Sum(logit_core::Sum {
                value: 3.0,
                temporality: logit_core::Temporality::Cumulative,
                monotonic: false,
            }),
            &[("env", "prod")],
        )]);
        assert_eq!(out, "page.views,env=prod value=3 1700000000000000000\n");
    }

    #[test]
    fn gauge_line_with_no_tags() {
        let out = encode(vec![metric_event("cpu.load", MetricKind::Gauge(0.5), &[])]);
        assert_eq!(out, "cpu.load value=0.5 1700000000000000000\n");
    }

    #[test]
    fn distribution_line_has_integer_count_and_percentiles() {
        let mut sketch = logit_core::DdSketch::new();
        sketch.add(120.0);
        let out = encode(vec![metric_event("latency", MetricKind::Distribution(sketch), &[])]);
        assert!(
            out.starts_with("latency count=1u,"),
            "count should be an unsigned int field: {out}"
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
            out.starts_with("latency count=1u,"),
            "count should be an unsigned int field: {out}"
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
        assert!(out.starts_with("latency count=2u,"), "got: {out}");
        assert!(out.contains("p50="), "got: {out}");
    }

    /// An unencodable `SetMembers` is skipped and a sibling metric in the batch still lands.
    #[test]
    fn set_members_has_no_line_protocol_encoding_yet() {
        let out = encode(vec![
            metric_event(
                "unique_visitors",
                MetricKind::SetMembers(vec![bytes::Bytes::from_static(b"a")]),
                &[],
            ),
            metric_event("page.views", MetricKind::counter(1.0), &[]),
        ]);
        assert!(!out.contains("unique_visitors"), "got: {out}");
        assert!(out.contains("page.views value=1"), "got: {out}");
    }

    #[test]
    fn exponential_histogram_has_no_line_protocol_encoding_yet() {
        let out = encode(vec![
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
    }

    /// `statsd.*` carriers never become tags; an ordinary sibling attribute still does.
    #[test]
    fn statsd_dot_attributes_never_become_tags() {
        let out = encode(vec![metric_event(
            "page.views",
            MetricKind::counter(1.0),
            &[
                ("statsd.type", "c"),
                ("statsd.container_id", "abcd1234"),
                ("statsd.event.title", "deploy"),
                ("statsd.service_check.message", "a|b"),
                ("env", "prod"),
            ],
        )]);
        assert!(!out.contains("statsd"), "got: {out}");
        assert!(out.contains("env=prod"), "got: {out}");
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

    /// A `Bytes` attribute stays out of tags after a Lua stage reassigns every attribute
    /// (`docs/design/lua-value-type-preservation.md`).
    #[test]
    fn bytes_attribute_stays_excluded_from_tags_after_a_lua_enrichment_stage() {
        let worker = logit_script::ScriptWorker::new(
            r#"
            function process(event)
                local attrs = event:to_table().attributes
                for k, v in pairs(attrs) do
                    event.attributes[k] = v
                end
                event.attributes.env = "prod"
                return event
            end
            "#,
        )
        .expect("script should load");

        let mut event = metric_event("page.views", MetricKind::counter(1.0), &[]);
        event.attributes.insert("host", Value::Bytes(Bytes::from_static(b"web-01")));
        let event = match worker.process(event).expect("process should succeed") {
            logit_script::ProcessOutcome::Emit(event, _) => *event,
            _ => panic!("expected the script to emit the event unchanged"),
        };

        let out = encode(vec![event]);
        assert!(out.contains("env=prod"), "the new Str tag should be written: {out}");
        assert!(
            !out.contains("host="),
            "a Bytes attribute must stay excluded from tags even after an unrelated Lua stage \
             reads and reassigns it: {out}"
        );
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

    /// `encode`, plus the multi-value-tag count [`Encoder::encode`] can't return.
    fn encode_counting_multi_value(events: Vec<Event>) -> (String, usize) {
        let mut encoder = InfluxLineEncoder::default();
        let bytes = encoder.encode(&batch_with(events)).expect("encode should succeed");
        let out = String::from_utf8(bytes.to_vec()).expect("output should be valid utf-8");
        (out, encoder.multi_value_tags)
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
        let (out, normalized) = encode_counting_multi_value(vec![event_with_array_tag(
            "team",
            vec![Value::str("a"), Value::str("b")],
        )]);
        assert_eq!(out, "page.views,team=b value=1 1700000000000000000\n");
        assert_eq!(normalized, 1, "once per attribute, not once per element");
    }

    #[test]
    fn a_multi_value_tag_whose_last_element_is_unrepresentable_falls_back_to_the_previous_one() {
        let (out, normalized) = encode_counting_multi_value(vec![event_with_array_tag(
            "team",
            vec![Value::str("a"), Value::str("b"), Value::Null],
        )]);
        assert!(out.contains("team=b"), "got: {out}");
        assert_eq!(normalized, 1);

        // And it keeps walking backwards past more than one of them.
        let (out, normalized) = encode_counting_multi_value(vec![event_with_array_tag(
            "team",
            vec![Value::str("a"), Value::Timestamp(1), Value::Map(Box::new(AttrMap::new()))],
        )]);
        assert!(out.contains("team=a"), "got: {out}");
        assert_eq!(normalized, 1);
    }

    /// An empty or wholly unrepresentable `Array` drops the tag uncounted.
    #[test]
    fn an_empty_or_all_unrepresentable_array_tag_drops_the_tag_with_no_count() {
        let (out, normalized) =
            encode_counting_multi_value(vec![event_with_array_tag("team", Vec::new())]);
        assert_eq!(out, "page.views value=1 1700000000000000000\n");
        assert_eq!(normalized, 0);

        let (out, normalized) = encode_counting_multi_value(vec![event_with_array_tag(
            "team",
            vec![Value::Null, Value::Map(Box::new(AttrMap::new()))],
        )]);
        assert_eq!(out, "page.views value=1 1700000000000000000\n");
        assert_eq!(normalized, 0);
    }

    /// `multi_value_tags` is zeroed per `encode`, not accumulated across batches.
    #[test]
    fn the_multi_value_tag_count_is_zeroed_per_encode_not_accumulated_across_batches() {
        let mut encoder = InfluxLineEncoder::default();
        for _ in 0..3 {
            let batch = batch_with(vec![event_with_array_tag(
                "team",
                vec![Value::str("a"), Value::str("b")],
            )]);
            encoder.encode(&batch).expect("encode should succeed");
            assert_eq!(encoder.multi_value_tags, 1);
        }
    }

    /// A `NO_RECORDED_VALUE` point is skipped, not written as `value=0`; a sibling still lands.
    #[test]
    fn a_no_recorded_value_point_is_skipped_not_written_as_a_fabricated_zero() {
        let mut flagged = metric_event("conns", MetricKind::Gauge(0.0), &[]);
        flagged.metrics[0].flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
        let out = encode(vec![flagged, metric_event("page.views", MetricKind::counter(1.0), &[])]);
        assert!(!out.contains("conns"), "a flagged point must not be written at all: {out}");
        assert!(out.contains("page.views value=1"), "got: {out}");
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
        let out = encode(vec![
            metric_event("conns", MetricKind::GaugeDelta(5.0), &[]),
            metric_event("page.views", MetricKind::counter(1.0), &[]),
        ]);
        assert!(!out.contains("conns"), "got: {out}");
        assert!(out.contains("page.views value=1"), "got: {out}");
    }

    /// A `GaugeDelta` reports `gauge_delta_unresolved`, not the generic `encode_error`.
    #[test]
    fn gauge_delta_reports_its_own_diagnostic_key() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("out", "influxdb_out", "sink");
        let mut encoder = InfluxLineEncoder::default()
            .with_diagnostics(Diagnostics::new("out").with_telemetry(telemetry));
        let batch = batch_with(vec![metric_event("conns", MetricKind::GaugeDelta(5.0), &[])]);
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
    fn non_finite_values_are_skipped_not_written_as_invalid_line_protocol() {
        let out = encode(vec![metric_event("bad", MetricKind::counter(f64::NAN), &[])]);
        assert_eq!(out, "", "a NaN-only line should produce no output, not `value=NaN`");
    }

    #[test]
    fn events_with_no_metrics_are_skipped() {
        let log_event = Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::str("hello"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        assert_eq!(encode(vec![log_event]), "");
    }

    #[test]
    fn measurement_name_starting_with_hash_is_rejected_not_silently_dropped() {
        let out = encode(vec![
            metric_event("#requests", MetricKind::counter(1.0), &[]),
            metric_event("page.views", MetricKind::counter(1.0), &[]),
        ]);
        assert!(!out.contains("#requests"), "got: {out}");
        assert!(out.contains("page.views value=1"), "got: {out}");
    }

    #[test]
    fn empty_tag_value_is_dropped_not_the_whole_metric() {
        let out =
            encode(vec![metric_event("page.views", MetricKind::counter(1.0), &[("env", "")])]);
        assert!(!out.contains("env="), "empty tag should be dropped entirely: {out}");
        assert!(out.contains("page.views value=1"), "the rest of the point should survive: {out}");
    }

    #[test]
    fn tag_value_with_embedded_newline_does_not_corrupt_the_rest_of_the_batch() {
        // A newline-bearing tag must not leave a fragment that corrupts the next line.
        let out = encode(vec![
            metric_event("ok.before", MetricKind::counter(1.0), &[]),
            metric_event("bad", MetricKind::counter(1.0), &[("env", "prod\ninjected")]),
            metric_event("ok.after", MetricKind::counter(1.0), &[]),
        ]);
        assert!(out.contains("ok.before value=1"), "got: {out}");
        assert!(out.contains("ok.after value=1"), "got: {out}");
        assert!(!out.contains("injected"), "got: {out}");
        assert_eq!(out.lines().count(), 3, "expected exactly 3 well-formed lines, got: {out}");
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
            MetricKind::Gauge(5.0),
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
        let mut event = Event::log(
            1_700_000_000_000_000_000,
            AttrMap::new(),
            LogRecord {
                message: Value::str("GET / HTTP/1.1"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        event.metrics.push(MetricRecord::new(
            logit_core::interner::intern("nginx.requests"),
            MetricKind::counter(1.0),
        ));

        let out = encode(vec![event]);
        assert_eq!(out, "nginx.requests value=1 1700000000000000000\n");
    }

    /// A bare HTTP/1.1 server: one canned response per connection (the last repeats), then close.
    /// `Connection: close` forces a fresh connection per request, so the counter equals the
    /// number of `send` calls that reached it.
    async fn canned_server(
        responses: Vec<&'static str>,
    ) -> (std::net::SocketAddr, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let count_task = count.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else { return };
                let i = count_task.fetch_add(1, Ordering::SeqCst);
                let response = responses.get(i).or(responses.last()).copied().unwrap_or("");
                let mut buf = [0u8; 8192];
                // Drain some of the request so the client's write can't block; the timeout keeps
                // a silent client from wedging this task.
                let _ =
                    tokio::time::timeout(Duration::from_millis(500), stream.read(&mut buf)).await;
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });
        (addr, count)
    }

    const RESP_204: &str = "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n";
    const RESP_400: &str =
        "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    const RESP_400_WITH_BODY: &str =
        "HTTP/1.1 400 Bad Request\r\nContent-Length: 22\r\nConnection: close\r\n\r\nunable to parse points";
    const RESP_401: &str =
        "HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    const RESP_429: &str =
        "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    const RESP_503: &str =
        "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

    async fn output_against(addr: std::net::SocketAddr) -> InfluxDbOutput {
        InfluxDbOutput::new(format!("http://{addr}"), "org".into(), "bucket".into(), "token".into())
    }

    fn one_metric_batch() -> EventBatch {
        batch_with(vec![metric_event("x", MetricKind::counter(1.0), &[])])
    }

    /// A success is `Ok(())` after one attempt.
    #[tokio::test]
    async fn a_successful_response_returns_ok_on_the_first_attempt() {
        let (addr, count) = canned_server(vec![RESP_204]).await;
        let mut output = output_against(addr).await;

        output.send(&one_metric_batch()).await.expect("a 204 should succeed");
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn influxdb_output_reports_itself_duplicate_safe() {
        let output = InfluxDbOutput::new(
            "http://localhost:8086".to_string(),
            "org".to_string(),
            "bucket".to_string(),
            "token".to_string(),
        );
        assert!(
            output.duplicate_safe(),
            "line protocol's (measurement, tag set, timestamp) identity makes a re-sent batch an \
             idempotent overwrite, not a duplicate"
        );
    }

    #[tokio::test]
    async fn a_503_response_is_classified_ambiguous() {
        let (addr, count) = canned_server(vec![RESP_503]).await;
        let mut output = output_against(addr).await;

        let err = output.send(&one_metric_batch()).await.expect_err("a 503 should fail send");
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1, "exactly one attempt");
    }

    #[tokio::test]
    async fn a_429_rate_limit_response_is_classified_ambiguous() {
        let (addr, count) = canned_server(vec![RESP_429]).await;
        let mut output = output_against(addr).await;

        let err = output.send(&one_metric_batch()).await.expect_err("a 429 should fail send");
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1, "exactly one attempt");
    }

    #[tokio::test]
    async fn a_400_response_is_classified_permanent() {
        let (addr, count) = canned_server(vec![RESP_400]).await;
        let mut output = output_against(addr).await;

        let err = output.send(&one_metric_batch()).await.expect_err("a 400 should fail send");
        assert_eq!(logit_pipeline::classify(&err), Fault::Permanent);
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1, "exactly one attempt");
    }

    #[tokio::test]
    async fn a_401_response_is_classified_permanent() {
        let (addr, count) = canned_server(vec![RESP_401]).await;
        let mut output = output_against(addr).await;

        let err = output.send(&one_metric_batch()).await.expect_err("a 401 should fail send");
        assert_eq!(logit_pipeline::classify(&err), Fault::Permanent);
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1, "exactly one attempt");
    }

    /// A rejection body is quoted verbatim in the error message when it fits the snippet bound.
    #[tokio::test]
    async fn a_short_error_body_is_quoted_verbatim() {
        let (addr, count) = canned_server(vec![RESP_400_WITH_BODY]).await;
        let mut output = output_against(addr).await;

        let err = output.send(&one_metric_batch()).await.expect_err("a 400 should fail send");
        assert_eq!(logit_pipeline::classify(&err), Fault::Permanent);
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1, "exactly one attempt");
        // `.context(fault)` makes `err`'s own `Display` the `Fault` alone; the send message with
        // the quoted body is the wrapped cause.
        let message = err.root_cause().to_string();
        assert!(message.contains("unable to parse points"), "got: {message}");
        assert!(!message.ends_with("..."), "a body under the bound isn't truncated: {message}");
    }

    /// A rejection body past the snippet bound is read only up to the bound (`read_body_prefix`),
    /// not buffered whole, and quoted with an ellipsis.
    #[tokio::test]
    async fn an_oversized_error_body_is_quoted_truncated() {
        let body = "x".repeat(64 * 1024);
        let response = format!(
            "HTTP/1.1 500 Internal Server Error\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let response: &'static str = Box::leak(response.into_boxed_str());
        let (addr, count) = canned_server(vec![response]).await;
        let mut output = output_against(addr).await;

        let err = output.send(&one_metric_batch()).await.expect_err("a 500 should fail send");
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1, "exactly one attempt");
        let message = err.root_cause().to_string();
        assert!(
            message.len() < ERROR_BODY_SNIPPET_BYTES + 96,
            "message should stay bounded regardless of body size: got {} bytes",
            message.len()
        );
        assert!(
            message.ends_with("..."),
            "a truncated body should be quoted with an ellipsis: {message}"
        );
    }

    /// A timeout against a server that accepts and never answers is `Ambiguous`, never `Clean`.
    #[tokio::test]
    async fn a_request_timeout_is_classified_ambiguous() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { return };
                std::mem::forget(stream); // keep it open; a dropped socket would close cleanly
            }
        });

        let mut output = output_against(addr).await.with_timeout(Duration::from_millis(50));
        let start = std::time::Instant::now();
        let err = output.send(&one_metric_batch()).await.expect_err("a stalled write should fail");

        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "should time out promptly, not hang: took {:?}",
            start.elapsed()
        );
    }

    /// A refused connection is `Clean`: pins `is_connect()`, which `at_most_once` relies on.
    #[tokio::test]
    async fn connect_refused_is_reliably_classified_as_a_clean_fault() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // nothing is listening on `addr` any more: a real "connection refused".

        let mut output = output_against(addr).await;
        let err = output
            .send(&one_metric_batch())
            .await
            .expect_err("connecting to a dropped listener should fail");

        assert_eq!(
            logit_pipeline::classify(&err),
            Fault::Clean,
            "connection-refused should classify as Clean -- if this ever fails, `is_connect()` \
             is not reliably distinguishing 'never reached the server' any more, and \
             classify_transport_error must downgrade its mapping to Ambiguous instead (see this \
             workstream's plan/report)"
        );
    }

    /// The multi-value counter fires before the empty-body return (port 1 is never contacted).
    #[tokio::test]
    async fn the_multi_value_tag_counter_is_emitted_even_when_the_batch_encodes_to_nothing() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("out", "influxdb_out", "sink");
        let mut output = InfluxDbOutput::new(
            "http://127.0.0.1:1".to_string(),
            "org".into(),
            "bucket".into(),
            "token".into(),
        )
        .with_telemetry(telemetry);

        // A NaN counter renders no line, but its tag still collapsed.
        let mut event = metric_event("bad", MetricKind::counter(f64::NAN), &[]);
        event.attributes.insert("team", Value::Array(vec![Value::str("a"), Value::str("b")]));
        output
            .send(&batch_with(vec![event]))
            .await
            .expect("an all-unencodable batch is not an error");

        let events = registry.drain(0);
        let counted = events
            .iter()
            .filter(|e| e.attributes.get("reason").and_then(|v| v.as_str()) == Some("multi_value"))
            .find_map(|e| {
                e.metrics.iter().find_map(|m| match &m.kind {
                    MetricKind::Sum(s)
                        if logit_core::interner::resolve(m.name)
                            == "logit.output.tags.normalized" =>
                    {
                        Some(s.value)
                    }
                    _ => None,
                })
            })
            .unwrap_or(0.0);
        assert_eq!(counted, 1.0);
    }

    /// `send` records one `logit.output.requests` per call, tagged by status class.
    #[tokio::test]
    async fn send_records_one_request_per_call_by_status_class() {
        let (addr, _count) = canned_server(vec![RESP_503]).await;
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("out", "influxdb_out", "sink");
        let mut output = output_against(addr).await.with_telemetry(telemetry);

        let _ = output.send(&one_metric_batch()).await;

        let events = registry.drain(0);
        let value = |name: &str, tag: Option<(&str, &str)>| -> f64 {
            events
                .iter()
                .find_map(|e| {
                    if let Some((k, v)) = tag {
                        if e.attributes.get(k).and_then(|v2| v2.as_str()) != Some(v) {
                            return None;
                        }
                    }
                    e.metrics.iter().find_map(|m| match &m.kind {
                        MetricKind::Sum(s) if logit_core::interner::resolve(m.name) == name => {
                            Some(s.value)
                        }
                        _ => None,
                    })
                })
                .unwrap_or(0.0)
        };

        assert_eq!(value("logit.output.requests", Some(("class", "5xx"))), 1.0);
        assert!(value("logit.output.batch.bytes", None) > 0.0);
    }
}
