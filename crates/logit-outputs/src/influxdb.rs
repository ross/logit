//! InfluxDB 2.x line-protocol output -- the output side of the v0.1 vertical slice
//! (`docs/OVERVIEW.md`), writing to `/api/v2/write` with org/bucket query params and a
//! `Token` auth header. Matches the `influxdb` service seeded in `compose.yaml` for local testing.

use crate::Output;
use anyhow::Context;
use bytes::Bytes;
use logit_core::interner::resolve;
use logit_core::{
    Diagnostics, Event, EventBatch, MetricKind, MetricRecord, Resource, Telemetry, Value,
};
use logit_pipeline::Fault;
use logit_proto::{CodecError, Encoder};
use std::cmp::Ordering;
use std::collections::HashMap;
// `std::fmt::Write`, for `write!` into a `String` -- formatting straight into the output buffer
// instead of building an intermediate `String` per number (`docs/design/memory.md`).
use std::fmt::Write;
use std::time::Duration;

/// `reqwest` has no request timeout by default. Without one, a server that accepts the TCP
/// connection but never responds would hang this output's `send` future -- and the pipeline
/// worker driving it -- indefinitely.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// 429 (InfluxDB's own rate-limit response) and any 5xx are treated as transient (`Fault::Ambiguous`
/// -- see [`Fault`]) -- 429 is a deliberate, narrow deviation from "a 4xx stays a hard failure" (see
/// ADR 0013). Every other 4xx (`Fault::Permanent`) is a config error (a bad org/bucket/token), never
/// worth retrying.
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
    /// This attempt's request timeout. `send` makes exactly one attempt per call now
    /// (`docs/adr/0019-buffered-sink-delivery.md` -- retry timing moved to the generic writer in
    /// `logit-pipeline`), so there's no "remaining retry budget" left to clamp this against any
    /// more; it's simply what `with_timeout` set (or [`DEFAULT_TIMEOUT`]), applied to `client` at
    /// build time and passed again per-request for clarity.
    request_timeout: Duration,
    /// Component-specific detail beyond the runtime's uniform layer-2 metrics (`docs/design/
    /// internal-telemetry.md`'s "layer 3") -- which response class came back, which
    /// `run_output`'s own `logit.component.send.duration`/`.errors` can't see inside a single
    /// `send` call.
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

    /// Attaches a component id to this output's encoder diagnostics (per-metric encode failures).
    /// `InfluxDbOutput` itself no longer has any diagnostics of its own to attribute -- it stopped
    /// retrying (`docs/adr/0019-buffered-sink-delivery.md`), and the generic writer that now owns
    /// retry timing gets its own `Diagnostics` handle from `logit-pipeline::runtime::write_loop`.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.encoder = self.encoder.with_diagnostics(diag);
        self
    }

    /// Attaches a telemetry handle -- see the `telemetry` field's doc comment.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

/// A coarse response-status bucket, `&'static str` so it's directly usable as a telemetry tag
/// value (`logit_core::telemetry::Tag`) with no per-response allocation or interning.
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
    /// Exactly one attempt per call -- no loop, no sleep. Retry timing/budget now belongs to the
    /// generic writer in `logit-pipeline` (`docs/adr/0019-buffered-sink-delivery.md`); this only
    /// classifies what happened and reports it via [`Fault`] (`.context(fault)`).
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let body = self.encoder.encode(batch)?;
        if body.is_empty() {
            // Nothing in this batch had a line-protocol encoding (e.g. every event carried only
            // a log or span and no metrics, or every metric was a Set -- see `metric_fields`
            // below). Not an error; nothing to write.
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
                let text = resp.text().await.unwrap_or_default();
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

    /// The line-protocol encoder derives every point's timestamp from `event.timestamp`, and its
    /// per-batch collision-disambiguation map (`InfluxLineEncoder::series`) is cleared at the top
    /// of every `encode` call -- so re-encoding and re-sending a buffered batch on retry produces
    /// byte-for-byte the same body as the first attempt, and InfluxDB treats an identical
    /// `(measurement, tag set, timestamp)` write as an idempotent overwrite, not a second point.
    /// See `docs/adr/0019-buffered-sink-delivery.md`.
    fn duplicate_safe(&self) -> bool {
        true
    }
}

/// Classifies a transport-level (connection never got an HTTP response at all) failure.
/// `reqwest::Error::is_connect()` is `true` specifically for a failure to establish the
/// connection itself (e.g. connection refused, DNS failure) -- provably "the destination never
/// saw this batch," `Fault::Clean`, confirmed against a real connection-refused failure in
/// `connect_refused_is_reliably_classified_as_a_clean_fault` below rather than assumed. Everything
/// else (a request timeout, a body read failure mid-response, ...) may have reached the
/// destination before failing, so it's `Fault::Ambiguous`, never `Clean` -- the duplicate-safety
/// argument for `at_most_once` depends on `Clean` never over-claiming.
fn classify_transport_error(err: &reqwest::Error) -> Fault {
    if err.is_connect() {
        Fault::Clean
    } else {
        Fault::Ambiguous
    }
}

/// Encodes an [`EventBatch`] as InfluxDB line protocol. Split out from [`InfluxDbOutput`] so the
/// encoding logic is directly unit-testable without an HTTP server.
///
/// Only an event's `metrics` have a line-protocol mapping; its log body and span (if any) are
/// skipped (`docs/OVERVIEW.md`'s v0.1 slice is metrics-only -- there's no established convention
/// yet for what a log line or span becomes in InfluxDB, and guessing one isn't this output's job).
/// An event can carry several metrics at once now (`docs/adr/0012-multi-payload-events.md`), so
/// each one becomes its own line, sharing that event's tags.
/// Public only so `logit-bench` can measure encoding in isolation, the same reason this is split
/// out from [`InfluxDbOutput`] at all -- `docs/design/memory.md` quotes a per-point allocation
/// count that has to come from calling this directly, with no HTTP server in the picture.
/// Reusable buffers, and the batch-scoped series map, live on the encoder rather than being
/// allocated per call, per event, or per line. Encoding is the single largest allocation cost in
/// the pipeline (`docs/design/memory.md`), and almost all of it was short-lived `String`s built
/// and dropped inside one line's encoding. `encode` clears each of these at the point its scope
/// begins, so reuse changes nothing about the bytes produced -- only how often the allocator is
/// asked for them. `Default` still gives an encoder with everything empty.
#[derive(Default)]
pub struct InfluxLineEncoder {
    diag: Diagnostics,
    /// The `,key=value` tag suffix for the event being encoded. Batch-scoped buffer, event-scoped
    /// contents: rebuilt once per event and shared across that event's metrics.
    tag_suffix: String,
    /// One complete line, assembled before being committed to the output (see
    /// [`encode_metric_line`] for why a line is built separately rather than appended in place).
    line: String,
    /// The `k=v,k=v` field set for the line being assembled.
    fields: String,
    /// Scratch space for rendering one non-string tag value. A `Value::Str` tag is borrowed
    /// directly and never touches this.
    scratch: String,
    /// Scratch for [`allocate_timestamp`]'s path-compression walk. Reused because a fresh
    /// `Vec::new()` allocates the moment the walk visits anything, i.e. on every timestamp
    /// collision -- and a statsd multi-value datagram collides on essentially every line.
    visited: Vec<i64>,
    /// Per-series "next free timestamp slot" successor maps -- see [`encode_metric_line`]'s
    /// comment for what this is for. Cleared at the start of every `encode`, which keeps it
    /// batch-scoped exactly as before while letting the maps' allocations survive across batches.
    series: HashMap<String, HashMap<i64, i64>>,
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
        // InfluxDB identifies a point by (measurement, tag set, timestamp) -- fields play no
        // part in identity, and a second point with the same identity overwrites the first
        // rather than coexisting. Two events in the same batch that share a measurement+tag-set
        // *and* timestamp would therefore collide silently; `encode_metric_line` disambiguates
        // them. `self.series` holds, per series, a "next free slot" successor map covering every
        // timestamp actually allocated to it so far -- not just the most recent one (see the
        // comment at its use site for why that's not enough either, and for what the successor
        // map buys over a plain occupied-set). Cleared here, so it stays batch-scoped -- not
        // per-event or per-metric: that's what lets it disambiguate collisions across the whole
        // batch. It lives on the encoder only so its allocations outlive one call.
        self.series.clear();
        for event in &batch.events {
            if event.metrics.is_empty() {
                continue; // a log-only, span-only, or empty event: nothing to encode
            }
            // Tags come from `resource.attributes` + `event.attributes` only -- nothing
            // metric-specific -- so they're identical for every metric on this event. Rendered
            // once per event, not once per metric.
            render_tag_suffix(&mut self.tag_suffix, &mut self.scratch, &batch.resource, event);
            for metric in &event.metrics {
                // One bad metric shouldn't drop its event's other metrics, let alone the rest of
                // the batch, so this logs and skips rather than propagating via `?`. Deliberately
                // on the inner, per-metric loop: a `Set` or `#`-prefixed metric sharing an event
                // with a perfectly good one must not take the good one down with it.
                //
                // The buffers are passed as separate `&mut` arguments rather than reached through
                // `self` inside the callee: they're disjoint fields, which the borrow checker
                // accepts here and would not through a `&mut self` method.
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
                    self.diag.warn_throttled("encode_error", err);
                }
            }
        }
        Ok(Bytes::from(buf.into_bytes()))
    }
}

/// Renders the `,key=value,key=value` line-protocol tag suffix for one event into `suffix`
/// (cleared first): resource attributes first, event attributes overriding on key collision (an
/// event-level tag is more specific than the batch-wide resource it came from). Depends on nothing
/// metric-specific, so it's computed once per event and shared across every metric that event
/// carries. Infallible by construction: a tag that can't be represented (an empty key/value, or
/// one with an embedded newline -- line protocol has no escape for that at all) is dropped
/// individually rather than escalated to a whole-point error.
///
/// The two attribute maps are **merge-joined** rather than combined by cloning the resource's map
/// and inserting the event's over the top. Both iterate in sorted-`Symbol` order (`AttrMap::iter`),
/// so walking them in lockstep and preferring the event's value on an equal key produces exactly
/// the same sequence the clone-and-insert did -- without copying an `AttrMap` per event, and
/// without the `resolve` -> `intern` round trip that re-inserting every key required.
fn render_tag_suffix(
    suffix: &mut String,
    scratch: &mut String,
    resource: &Resource,
    event: &Event,
) {
    suffix.clear();
    let mut resource_attrs = resource.attributes.iter().peekable();
    let mut event_attrs = event.attributes.iter().peekable();

    loop {
        let next =
            match (resource_attrs.peek().map(|(k, _)| *k), event_attrs.peek().map(|(k, _)| *k)) {
                (Some(r), Some(e)) => match r.cmp(&e) {
                    Ordering::Less => resource_attrs.next(),
                    Ordering::Greater => event_attrs.next(),
                    // Same key on both: the event's value wins, and the resource's is discarded.
                    Ordering::Equal => {
                        resource_attrs.next();
                        event_attrs.next()
                    }
                },
                (Some(_), None) => resource_attrs.next(),
                (None, Some(_)) => event_attrs.next(),
                (None, None) => break,
            };
        let Some((key, value)) = next else { break };

        let key = resolve(key);
        let Some(value) = tag_value(scratch, value) else {
            continue;
        };
        // InfluxDB 2.x rejects an empty tag value outright, and line protocol has no escape for
        // an embedded newline in a tag value at all -- either would previously have corrupted or
        // rejected this whole line (and, via a since-fixed shared-buffer bug, everything after it
        // in the batch). Drop just this one tag rather than the whole metric: the point is still
        // meaningful without it.
        if key.is_empty()
            || value.is_empty()
            || key.contains(['\n', '\r'])
            || value.contains(['\n', '\r'])
        {
            continue;
        }
        suffix.push(',');
        push_escaped_tag(suffix, key);
        suffix.push('=');
        push_escaped_tag(suffix, value);
    }
}

/// One metric's line, appended to `buf`. `line` and `fields` are caller-owned scratch buffers,
/// cleared here -- see [`InfluxLineEncoder`]'s fields for why they're not local.
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
        // Every field was non-finite (see `push_float`) or otherwise unrepresentable -- an
        // empty line is invalid line protocol, so skip rather than write one.
        return Ok(());
    }

    let measurement = resolve(metric.name);
    // A line whose first character is '#' is a comment in line protocol -- a metric name that
    // happens to start with '#' would otherwise be silently swallowed by InfluxDB (the write
    // still reports success) rather than actually stored.
    if measurement.starts_with('#') {
        return Err(CodecError::Malformed(format!(
            "measurement name {measurement:?} can't be encoded: a leading '#' is a line-protocol \
             comment marker"
        )));
    }

    // Built in a separate buffer first, not `buf` directly: if a later step rejects (there are
    // none right now, but there were -- see `render_tag_suffix`'s doc comment for the history),
    // only a complete, valid line ever reaches `buf`. "measurement + tags" is also the
    // series-identity key fed to `series_allocated_timestamps` below -- the tag half is shared
    // across an event's metrics (see `encode`), but the measurement isn't, so this prefix still
    // has to be rebuilt once per metric even though `tag_suffix` itself is computed only once.
    line.clear();
    push_escaped_measurement(line, measurement);
    line.push_str(tag_suffix);
    // Where the series-identity prefix ends. Everything appended past this point (fields, the
    // timestamp) is not part of the series key.
    let series_key_len = line.len();
    // Disambiguate same-series collisions within this batch (see `encode`'s comment).
    //
    // Three schemes were tried before this one:
    // - "Add 1ns per prior occurrence, in arrival order" only produces distinct timestamps if
    //   same-series events already arrive sorted. Out of order -- e.g. timestamps 101 then 100 --
    //   the first gets stamped 101 (0 prior occurrences) and the second gets 100+1=101 too,
    //   colliding again.
    // - "Track the last timestamp emitted per series, enforce max(last+1, own)" fixes that, but
    //   over-corrects: it forces every subsequent same-series event forward regardless of whether
    //   its *own* timestamp actually collides with anything. For 101 then 100, it emits 101 then
    //   102 -- even though 100 was completely free -- discarding a real, distinct timestamp for
    //   no reason. The gap only grows with how out-of-order the input is.
    // - "Track every timestamp actually allocated in a `HashSet`, linearly re-probing forward
    //   from `event.timestamp` on every call" gets both of the above right, but restarts the
    //   probe from scratch for every duplicate: *k* same-series/same-timestamp events cost
    //   0 + 1 + ... + (k-1) lookups, O(k^2). `logit-inputs::statsd` stamps one timestamp on an
    //   entire datagram and its multi-value form (`x:1:1:1...|c`) expands into one event per
    //   value, so a single ~65KB datagram can make k ~30,000 -- ~450 million lookups.
    //
    // Correct *and* amortized-cheap: a per-series successor map, same idea as a union-find
    // "smallest free slot >= t" allocator with path compression. `allocate_timestamp` walks the
    // chain of already-occupied slots starting at `timestamp`, and repoints every slot it visits
    // directly at the free slot it finds -- so the next probe starting anywhere on that chain
    // jumps straight there instead of re-walking it. A timestamp that isn't already used is still
    // returned untouched, regardless of arrival order; only a genuine collision costs a 1ns
    // nudge, and repeated collisions on the same series no longer cost more than a couple of
    // lookups each once the chain has been compressed once.
    //
    // Needs no algorithmic change for an event carrying several metrics
    // (docs/adr/0012-multi-payload-events.md): distinct metric names on one event produce
    // distinct series keys (they differ by measurement) and never collide, so they keep the
    // event's own timestamp untouched; a *repeated* metric name on one event (e.g. `kv_metrics`
    // configured to add the same counter twice, or two aggregated series that happen to share a
    // name) takes exactly the collision path below, one series key, `k` allocations -- the same
    // amortized-cheap walk, byte-for-byte the same output as if those metrics had arrived as `k`
    // separate events. `series_allocated_timestamps` staying hoisted at batch scope (`encode`,
    // above) is what makes this true -- don't move it to per-event or per-metric scope.
    // Looked up before inserting, so the common path -- a series already seen in this batch --
    // costs one extra hash instead of one `String` allocation for a key that is then thrown away.
    // (`entry` would need the key up front, and `get_mut`-then-`insert` can't share one borrow
    // without polonius.) The key is the prefix of `line` computed above, borrowed for the lookup
    // and only cloned when it turns out to be new.
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

    // This matters in practice today: `logit-inputs::statsd` assigns one timestamp to an entire
    // datagram, and its multi-value form (`name:1:2:3|c`) expands into several otherwise-
    // identical events, all sharing that timestamp -- a nanosecond-scale perturbation here is far
    // below any input's actual timing resolution, and far simpler than guessing at how to
    // aggregate same-series samples together, which the source protocol never specified.

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
/// This is a union-find "smallest free slot" allocator with path compression: the walk from
/// `requested` to the eventual free slot passes through zero or more occupied timestamps, and
/// every one of them gets repointed straight at the free slot (well, `free + 1`, since the free
/// slot itself is about to become occupied) before returning. A later call starting anywhere on
/// that walked chain -- including `requested` itself, on a repeat collision -- then reaches the
/// (new) free slot in one hop instead of re-walking however much of the chain got probed before.
/// That's what keeps *k* collisions on one series amortized-cheap instead of the O(k^2) cost of
/// re-probing an occupied set from `requested` on every call (see the comment at the call site).
///
/// Returns `None` if the free slot the walk lands on is `i64::MAX`: that timestamp is treated as
/// permanently unusable (never recorded as occupied, so a repeat request lands here again rather
/// than looping) purely so `successor` never has to wrap and `next_free` can never contain a
/// self-loop. `i64::MAX` nanoseconds is the year 2262, so in practice this only ever fires for a
/// series with over 2^63 timestamps already allocated at or after `requested` -- not reachable
/// from any real batch.
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
    // `cur` is now free. Reserve it, and repoint every occupied slot visited on the way here
    // directly at its successor so the next walk through any of them stops immediately.
    let successor = cur.checked_add(1)?;
    next_free.insert(cur, successor);
    for slot in visited.drain(..) {
        next_free.insert(slot, successor);
    }
    Some(cur)
}

/// Renders one metric's line-protocol field set as `k=v,k=v` into `out` (cleared first), returning
/// whether anything was written. `false` means every field was unrepresentable, which makes the
/// whole point unwritable -- an empty field set is invalid line protocol.
///
/// `Counter`/`Gauge` are a single `value` field; `Distribution` writes `count` (as an unsigned
/// integer -- see [`push_uint`]) plus a few fixed quantiles; `Histogram` maps its buckets onto
/// fields directly; `Summary` maps its quantiles onto fields keyed by the raw quantile value
/// rather than a rounded percentage, since rounding isn't collision-free (0.991 and 0.994 would
/// both round to "p99" and overwrite each other within one line's field set). `Set` has no
/// encoding yet: it needs a real `HyperLogLog` (`logit_core::metric::HyperLogLog` is still a
/// stub), so this returns an error rather than inventing a meaningless one.
///
/// **Field names are written unescaped, and that is not a shortcut.** Every name this can produce
/// is either a literal (`value`, `count`) or built purely out of formatted numbers
/// (`p50`, `bucket_1.5`, `q0.99`), and no `f64`/`u32` rendering can contain a backslash, comma,
/// equals, or space -- so the escaping the previous version applied was provably a no-op, and the
/// output is byte-for-byte what it was. A future field name derived from anything user-supplied
/// would have to go back through [`push_escaped_tag`].
fn render_fields(out: &mut String, kind: &MetricKind) -> Result<bool, CodecError> {
    out.clear();

    match kind {
        MetricKind::Counter(v) | MetricKind::Gauge(v) => {
            if v.is_finite() {
                out.push_str("value=");
                push_float(out, *v);
            }
        }
        MetricKind::Distribution(sketch) => {
            // Unconditional, so a Distribution always has at least one field.
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
        MetricKind::Histogram { buckets } => {
            for (bound, count) in buckets {
                if bound.is_finite() {
                    separator(out);
                    let _ = write!(out, "bucket_{bound}=");
                    push_uint(out, *count);
                }
            }
        }
        MetricKind::Summary { quantiles } => {
            for (q, v) in quantiles {
                if v.is_finite() {
                    separator(out);
                    let _ = write!(out, "q{q}=");
                    push_float(out, *v);
                }
            }
        }
        MetricKind::Set(_) => {
            return Err(CodecError::Malformed(
                "Set metrics have no line-protocol encoding yet".to_string(),
            ))
        }
    }
    Ok(!out.is_empty())
}

/// Comma between field entries, for the two kinds whose field count isn't known up front. Keyed
/// off "has anything been written yet" rather than a loop index, because entries are skipped for
/// non-finite values and an index would put a leading comma on a line whose first bucket was
/// skipped.
fn separator(out: &mut String) {
    if !out.is_empty() {
        out.push(',');
    }
}

/// Line protocol has no representation for non-finite floats. Every caller guards with
/// `is_finite` before reaching this (writing `value=NaN` would have InfluxDB reject the whole
/// write); a client can produce one today -- statsd's `f64::parse` accepts the literal text
/// "NaN"/"inf" -- so that guard isn't theoretical.
///
/// `write!` rather than `to_string()`: identical output (`to_string` is `format!("{}")`), no
/// intermediate allocation.
fn push_float(out: &mut String, v: f64) {
    debug_assert!(v.is_finite(), "callers must reject non-finite values before formatting");
    let _ = write!(out, "{v}");
}

/// Line-protocol unsigned-integer field (the `u` suffix, InfluxDB 2.x). Without it, a bare number
/// is parsed as `f64` by default, which loses integer semantics and, above 2^53, exactness --
/// real concerns for a count that legitimately grows past that in a long-running series.
fn push_uint(out: &mut String, v: u64) {
    let _ = write!(out, "{v}u");
}

fn push_i64(out: &mut String, v: i64) {
    let _ = write!(out, "{v}");
}

/// One tag value as a `&str`, or `None` for a `Value` with no sensible plain-text tag
/// representation. A `Value::Str` is borrowed straight out of the attribute (no copy, since it's
/// already UTF-8 `Bytes`); everything else is formatted into `scratch`, which is cleared first and
/// reused across tags.
fn tag_value<'a>(scratch: &'a mut String, v: &'a Value) -> Option<&'a str> {
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
        // Null, Bytes, Timestamp, Array, and Map have no sensible plain-text tag representation.
        Value::Null | Value::Bytes(_) | Value::Timestamp(_) | Value::Array(_) | Value::Map(_) => {
            None
        }
    }
}

/// Appends `s` with line protocol's measurement escaping (`\`, `,`, and space).
///
/// Written straight into `out` rather than returned as a new `String`: the previous chained
/// `.replace()` form allocated once per replacement -- three or four `String`s per measurement or
/// tag -- *whether or not anything actually needed escaping*, which was the single biggest
/// contributor to this encoder's allocation count (`docs/design/memory.md`). The common case here
/// copies one contiguous run and allocates nothing.
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
        // The matched character, which `find` guarantees is one of `needs_escape` and therefore
        // one byte of ASCII.
        out.push_str(&rest[i..i + 1]);
        rest = &rest[i + 1..];
    }
    out.push_str(rest);
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, BodyFormat, LogRecord, MetricKind};
    use std::sync::Arc;

    fn batch_with(events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(Resource::default()), events }
    }

    fn metric_event(name: &str, kind: MetricKind, attrs: &[(&str, &str)]) -> Event {
        let mut attributes = AttrMap::new();
        for (k, v) in attrs {
            attributes.insert(k, *v);
        }
        Event::metric(
            1_700_000_000_000_000_000,
            attributes,
            MetricRecord { name: logit_core::interner::intern(name), kind, unit: None },
        )
    }

    /// Like `metric_event`, but with an explicit timestamp -- the leverage point for the
    /// `allocate_timestamp` regression tests below, all of which care about the exact timestamp
    /// a series of events shares.
    fn counter_event_at(ts: i64, name: &str, v: f64) -> Event {
        let mut event = metric_event(name, MetricKind::Counter(v), &[]);
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
            encode(vec![metric_event("page.views", MetricKind::Counter(3.0), &[("env", "prod")])]);
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
            MetricKind::Histogram { buckets: vec![(100.0, 5), (500.0, 2)] },
            &[],
        )]);
        assert!(out.contains("bucket_100=5u"), "got: {out}");
        assert!(out.contains("bucket_500=2u"), "got: {out}");
    }

    #[test]
    fn summary_quantile_keys_do_not_collide_when_rounded_percentage_would() {
        // 0.991 and 0.994 both round to "p99" under a percentage-rounding scheme -- the whole
        // point of this test is that they must not collapse onto the same field key.
        let out = encode(vec![metric_event(
            "req.latency",
            MetricKind::Summary { quantiles: vec![(0.991, 10.0), (0.994, 20.0)] },
            &[],
        )]);
        assert!(out.contains("q0.991=10"), "got: {out}");
        assert!(out.contains("q0.994=20"), "got: {out}");
    }

    #[test]
    fn tag_values_with_special_characters_are_escaped() {
        let out = encode(vec![metric_event(
            "page.views",
            MetricKind::Counter(1.0),
            &[("path", "a,b c=d")],
        )]);
        assert!(out.contains("path=a\\,b\\ c\\=d"), "got: {out}");
    }

    /// The concrete behavioral consequence `docs/design/lua-value-type-preservation.md` and PR #6
    /// review discussion_r3887008990 describe: `value_as_tag_string` treats `Value::Bytes` and
    /// `Value::Str` differently (the former is excluded from tags, the latter included), so a
    /// Lua enrichment stage that carelessly changes an attribute's variant on an unmodified
    /// round-trip -- which it used to, before `logit-script`'s `AttrsProxy::__newindex` grew its
    /// no-op-assignment rule -- silently changes what gets written to InfluxDB with no error and
    /// no script-visible signal. This closes the loop end to end: a `Bytes` attribute must stay
    /// excluded from tags even after passing through a Lua stage that touches every attribute.
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

        let mut event = metric_event("page.views", MetricKind::Counter(1.0), &[]);
        event.attributes.insert("host", Value::Bytes(Bytes::from_static(b"web-01")));
        let event = match worker.process(event).expect("process should succeed") {
            logit_script::ProcessOutcome::Emit(event) => *event,
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
        let mut event = metric_event("page.views", MetricKind::Counter(1.0), &[("env", "prod")]);
        event.attributes.insert("env", "prod"); // already set by metric_event; explicit for clarity

        let mut encoder = InfluxLineEncoder::default();
        let batch = EventBatch { resource: Arc::new(resource), events: vec![event] };
        let out = String::from_utf8(encoder.encode(&batch).unwrap().to_vec()).unwrap();

        assert!(out.contains("host=web1"), "got: {out}");
        assert!(out.contains("env=prod"), "resource's env=staging should be overridden: {out}");
        assert!(!out.contains("env=staging"), "got: {out}");
    }

    #[test]
    fn set_metrics_are_skipped_not_fatal() {
        let out = encode(vec![
            metric_event("unique.users", MetricKind::Set(logit_core::HyperLogLog::default()), &[]),
            metric_event("page.views", MetricKind::Counter(1.0), &[]),
        ]);
        // The Set line is dropped; the Counter line alongside it still comes through.
        assert!(!out.contains("unique.users"), "got: {out}");
        assert!(out.contains("page.views value=1"), "got: {out}");
    }

    #[test]
    fn non_finite_values_are_skipped_not_written_as_invalid_line_protocol() {
        let out = encode(vec![metric_event("bad", MetricKind::Counter(f64::NAN), &[])]);
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
            },
        );
        assert_eq!(encode(vec![log_event]), "");
    }

    #[test]
    fn measurement_name_starting_with_hash_is_rejected_not_silently_dropped() {
        // A line starting with '#' is a comment in line protocol -- writing it would make
        // InfluxDB report success while storing nothing. It must be rejected up front instead.
        let out = encode(vec![
            metric_event("#requests", MetricKind::Counter(1.0), &[]),
            metric_event("page.views", MetricKind::Counter(1.0), &[]),
        ]);
        assert!(!out.contains("#requests"), "got: {out}");
        assert!(out.contains("page.views value=1"), "got: {out}");
    }

    #[test]
    fn empty_tag_value_is_dropped_not_the_whole_metric() {
        let out =
            encode(vec![metric_event("page.views", MetricKind::Counter(1.0), &[("env", "")])]);
        assert!(!out.contains("env="), "empty tag should be dropped entirely: {out}");
        assert!(out.contains("page.views value=1"), "the rest of the point should survive: {out}");
    }

    #[test]
    fn tag_value_with_embedded_newline_does_not_corrupt_the_rest_of_the_batch() {
        // This used to write a truncated, newline-less fragment straight into the shared buffer
        // and bail, corrupting whatever line got appended after it. Two full, valid lines must
        // come out the other side of a batch containing a newline-poisoned tag value in between.
        let out = encode(vec![
            metric_event("ok.before", MetricKind::Counter(1.0), &[]),
            metric_event("bad", MetricKind::Counter(1.0), &[("env", "prod\ninjected")]),
            metric_event("ok.after", MetricKind::Counter(1.0), &[]),
        ]);
        assert!(out.contains("ok.before value=1"), "got: {out}");
        assert!(out.contains("ok.after value=1"), "got: {out}");
        assert!(!out.contains("injected"), "got: {out}");
        assert_eq!(out.lines().count(), 3, "expected exactly 3 well-formed lines, got: {out}");
    }

    #[test]
    fn multi_value_samples_in_one_batch_are_not_collapsed_by_influxdb_point_identity() {
        // InfluxDB identifies a point by (measurement, tag set, timestamp); `logit-inputs::statsd`
        // assigns one timestamp to an entire datagram, so its multi-value form (`name:1:2:3|c`)
        // decodes into three events sharing a measurement, tag set, *and* timestamp. Written
        // verbatim, the second and third would silently overwrite the first in InfluxDB, leaving
        // only "3" stored. All three must survive as distinct points.
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
        // A "track the last emitted timestamp, enforce max(last+1, own)" scheme fixes the
        // original collision but over-corrects: it forces *every* subsequent same-series event
        // forward regardless of whether its own timestamp actually collides with anything. For
        // ts=101 then ts=100, that emits 101 then 102 -- even though 100 was completely free,
        // discarding a real, distinct timestamp for no reason (and the gap only grows with how
        // out-of-order the input is). The fix probes forward from each event's *own* timestamp,
        // only advancing while that exact slot is already taken by this series -- so this must
        // emit exactly 100 and 101, not 101 and 102.
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
        // `logit-inputs::statsd` assigns one timestamp to an entire datagram, and its
        // multi-value form (`x:1:1:1...|c`) expands into one event per value -- so a single
        // ~65KB datagram can decode into tens of thousands of same-series, same-timestamp
        // events. A scheme that re-probes an occupied `HashSet` from `event.timestamp` on every
        // call costs 0 + 1 + ... + (N-1) lookups here -- O(N^2), effectively a hang at this N.
        // This test is a correctness assertion (exact allocated range), but it also stands in as
        // the performance regression guard: it must stay fast. Don't shrink N to "simplify" it.
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
        // Two original timestamps whose forward-probe ranges would overlap (100 x3, 101 x2) must
        // still produce a clean, contiguous, duplicate-free allocation: path compression must
        // never let one walk skip over a slot another walk is about to claim.
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

    /// Proves hoisting tag rendering out of the per-metric loop (`render_tag_suffix`,
    /// `encode`'s inner loop) didn't change output: distinctly-named metrics on one event never
    /// collide in `allocate_timestamp` (different measurements mean different series keys), so
    /// each keeps the event's own timestamp untouched, and each still carries the event's tags.
    #[test]
    fn several_metrics_on_one_event_share_its_tags_and_each_get_a_line() {
        let mut event = metric_event("requests", MetricKind::Counter(1.0), &[("env", "prod")]);
        event.metrics.push(MetricRecord {
            name: logit_core::interner::intern("latency"),
            kind: MetricKind::Gauge(5.0),
            unit: None,
        });
        event.metrics.push(MetricRecord {
            name: logit_core::interner::intern("bytes"),
            kind: MetricKind::Counter(100.0),
            unit: None,
        });

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

    /// Case B from `allocate_timestamp`'s doc comment: the *same* metric name appearing twice on
    /// one event -- a genuinely new input shape this refactor introduces (e.g. `kv_metrics`
    /// configured to add one counter twice, or two upstream series that happen to share a name
    /// landing on one event) -- takes exactly the collision path a repeated statsd value used to,
    /// byte-for-byte the same allocator behavior.
    #[test]
    fn the_same_metric_name_twice_on_one_event_gets_distinct_timestamps() {
        let mut event = metric_event("page.views", MetricKind::Counter(1.0), &[]);
        event.metrics.push(MetricRecord {
            name: logit_core::interner::intern("page.views"),
            kind: MetricKind::Counter(2.0),
            unit: None,
        });

        let out = encode(vec![event]);
        assert_eq!(out.lines().count(), 2, "got: {out}");
        assert!(out.contains("value=1 1700000000000000000"), "got: {out}");
        assert!(out.contains("value=2 1700000000000000001"), "got: {out}");
    }

    /// Pins the error handling's move to the inner, per-metric loop (`encode`): today's
    /// `set_metrics_are_skipped_not_fatal` and `measurement_name_starting_with_hash_is_rejected_
    /// not_silently_dropped` use separate events and so don't actually exercise this -- a bad
    /// metric sharing an event with a good one must not take the good one down too.
    #[test]
    fn a_bad_metric_skips_only_itself_not_the_rest_of_its_event() {
        let mut event =
            metric_event("unique.users", MetricKind::Set(logit_core::HyperLogLog::default()), &[]);
        event.metrics.push(MetricRecord {
            name: logit_core::interner::intern("page.views"),
            kind: MetricKind::Counter(1.0),
            unit: None,
        });

        let out = encode(vec![event]);
        assert!(!out.contains("unique.users"), "got: {out}");
        assert!(
            out.contains("page.views value=1"),
            "the good metric on the same event should still land: {out}"
        );
    }

    /// The nginx shape end to end: an access-log event that a `kv_metrics`-style transform has
    /// added a derived metric to. The metric should be written; the log body is simply ignored,
    /// not an error.
    #[test]
    fn a_mixed_log_and_metric_event_writes_the_metric_and_ignores_the_log() {
        let mut event = Event::log(
            1_700_000_000_000_000_000,
            AttrMap::new(),
            LogRecord {
                message: Value::str("GET / HTTP/1.1"),
                severity: None,
                body_format: BodyFormat::Raw,
            },
        );
        event.metrics.push(MetricRecord {
            name: logit_core::interner::intern("nginx.requests"),
            kind: MetricKind::Counter(1.0),
            unit: None,
        });

        let out = encode(vec![event]);
        assert_eq!(out, "nginx.requests value=1 1700000000000000000\n");
    }

    /// A bare HTTP/1.1 server: writes back one canned response per accepted connection (repeating
    /// the last one past the end of the list), then closes. `Connection: close` on every response
    /// means reqwest opens a fresh connection per request rather than reusing one, so the returned
    /// counter is exactly the number of `send` calls that reached this server.
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
                // Drain (some of) the request so the client's write isn't left blocked on a full
                // socket buffer; a short timeout in case a client sends nothing (shouldn't happen
                // here, but a hung read must not wedge this server thread).
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
        batch_with(vec![metric_event("x", MetricKind::Counter(1.0), &[])])
    }

    /// `send` now makes exactly one attempt per call -- retry timing moved to `logit-pipeline`'s
    /// generic writer (`docs/adr/0019-buffered-sink-delivery.md`). A success is still a plain
    /// `Ok(())`, single attempt.
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
        // 429 is InfluxDB's own rate-limit response and genuinely transient -- the one deliberate
        // deviation from "a 4xx is a hard failure" (ADR 0013).
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

    /// A request timeout is a transport failure that may still have reached the server before the
    /// response was lost -- `Fault::Ambiguous`, never `Fault::Clean`. The stalled server accepts
    /// the connection (so this genuinely isn't "connection refused") and never responds.
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

    /// The throwaway check this workstream's plan explicitly calls for: does
    /// `reqwest::Error::is_connect()` actually distinguish "never reached the server" from other
    /// transport failures? Verified against a genuinely refused connection (a bound-then-dropped
    /// listener -- nothing is listening on `addr` by the time this connects) rather than assumed.
    /// The whole duplicate-safety argument for `at_most_once` rests on `Fault::Clean` never
    /// over-claiming, so this has to actually hold, not just look plausible.
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

    /// The layer-3 telemetry example (`docs/design/internal-telemetry.md`): every response class
    /// actually seen should be visible, not just whether `send` succeeded. No more
    /// `logit.output.retries` here -- that counter moved with the retry loop itself, into
    /// `logit-pipeline::runtime::write_loop` (`logit.component.retries`).
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
                        MetricKind::Counter(v) if logit_core::interner::resolve(m.name) == name => {
                            Some(*v)
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
