//! InfluxDB 2.x line-protocol output -- the output side of the v0.1 vertical slice
//! (`docs/OVERVIEW.md`), writing to `/api/v2/write` with org/bucket query params and a
//! `Token` auth header. Matches the `influxdb` service seeded in `compose.yaml` for local testing.

use crate::Output;
use bytes::Bytes;
use logit_core::interner::resolve;
use logit_core::{Diagnostics, Event, EventBatch, MetricKind, MetricRecord, Resource, Value};
use logit_proto::{CodecError, Encoder};
use std::collections::HashMap;
use std::time::Duration;

/// `reqwest` has no request timeout by default. Without one, a server that accepts the TCP
/// connection but never responds would hang this output's `send` future -- and the pipeline
/// worker driving it -- indefinitely.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Bounded retry for a transient write failure, so one InfluxDB 5xx no longer ends `logit run`
/// outright (`docs/adr/0013-service-lifecycle-and-output-retry.md`). `total_budget` is a hard
/// wall-clock ceiling, not an attempt count: `send` runs inline in the pipeline's drain loop
/// (`logit_pipeline::runtime::run_output`), so an unbounded retry would stall the whole upstream
/// path and silently drop UDP intake at the kernel socket buffer while it waited. The default
/// (~5s) sits comfortably inside the 10s aggregation window -- long enough to ride out a blip or a
/// single 5xx, not long enough to ride out a real outage (that needs delivery decoupled from the
/// drain loop entirely; tracked in `docs/known-gaps.md`, deliberately out of scope here).
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// Hard ceiling on total time spent inside one `send` call, across every attempt and every
    /// backoff sleep combined. Checked before starting each attempt and before each backoff sleep.
    pub total_budget: Duration,
    /// Backoff after attempt `n` is `base_delay * 2^(n-1)`, capped at `max_delay` and further
    /// clamped to whatever's left of `total_budget`. No jitter: there's exactly one writer per
    /// `InfluxDbOutput`, not a fleet thundering-herding a shared endpoint.
    pub base_delay: Duration,
    pub max_delay: Duration,
    /// Per-attempt request timeout while retrying. Independently clamped against
    /// [`InfluxDbOutput::with_timeout`]'s setting and the remaining budget -- whichever is
    /// smallest wins -- so a single stalled attempt can never consume the whole budget on its own.
    pub attempt_timeout: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            total_budget: Duration::from_secs(5),
            base_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(2),
            attempt_timeout: Duration::from_secs(2),
        }
    }
}

/// 429 (InfluxDB's own rate-limit response) and any 5xx are treated as transient and retried --
/// 429 is a deliberate, narrow deviation from "a 4xx stays a hard failure" (see ADR 0013). Every
/// other 4xx bails on the first attempt: it's a config error (a bad org/bucket/token), and
/// retrying it would only delay a diagnosis that's already available.
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
    /// Tracked separately from `client` (which bakes a timeout in at build time with no way to
    /// read it back out) so `send` can clamp each attempt's timeout against both this and
    /// `retry.attempt_timeout` -- whichever the caller configured more tightly.
    request_timeout: Duration,
    retry: RetryPolicy,
    diag: Diagnostics,
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
            retry: RetryPolicy::default(),
            diag: Diagnostics::default(),
        }
    }

    /// Overrides the default 10s request timeout. Rebuilds the underlying HTTP client.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.client = build_client(timeout);
        self.request_timeout = timeout;
        self
    }

    /// Overrides the default retry policy (see [`RetryPolicy`]'s doc comment for why the default
    /// exists and what it does and doesn't ride out).
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Attaches a component id to this output's diagnostics -- both its own (retry/backoff
    /// reports) and its encoder's (per-metric encode failures), so both sides of one component
    /// report under the same id.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.encoder = self.encoder.with_diagnostics(diag.clone());
        self.diag = diag;
        self
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
    async fn send(&mut self, batch: EventBatch) -> anyhow::Result<()> {
        let body = self.encoder.encode(&batch)?;
        if body.is_empty() {
            // Nothing in this batch had a line-protocol encoding (e.g. every event carried only
            // a log or span and no metrics, or every metric was a Set -- see `metric_fields`
            // below). Not an error; nothing to write.
            return Ok(());
        }

        // Encoded once, before the retry loop: `encode` is deterministic, so re-running it on a
        // retry would be pointless. `Bytes::clone` is a cheap refcount bump, not a copy -- one
        // clone per attempt is fine.
        let write_url = format!("{}/api/v2/write", self.url.trim_end_matches('/'));
        let deadline = tokio::time::Instant::now() + self.retry.total_budget;
        let mut attempt: u32 = 0;

        loop {
            attempt += 1;
            let now = tokio::time::Instant::now();
            // Never bail before a first attempt has actually run, even if `total_budget` is
            // pathologically tiny -- only a *retry* is gated on the deadline.
            if attempt > 1 && now >= deadline {
                anyhow::bail!(
                    "InfluxDB write did not succeed within the {:?} retry budget after {} \
                     attempt(s)",
                    self.retry.total_budget,
                    attempt - 1,
                );
            }

            // Floored at 1ms: a zero-duration reqwest timeout fails immediately rather than
            // giving this attempt any real chance, turning "budget nearly exhausted" into
            // "budget already exhausted" a beat early.
            let remaining = deadline.saturating_duration_since(now).max(Duration::from_millis(1));
            let attempt_timeout =
                self.request_timeout.min(self.retry.attempt_timeout).min(remaining);

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
                .timeout(attempt_timeout)
                .body(body.clone())
                .send()
                .await;

            let failure = match result {
                Ok(resp) if resp.status().is_success() => return Ok(()),
                Ok(resp) => {
                    let status = resp.status();
                    if !is_retryable_status(status) {
                        let text = resp.text().await.unwrap_or_default();
                        anyhow::bail!("InfluxDB write failed ({status}): {text}");
                    }
                    let text = resp.text().await.unwrap_or_default();
                    format!("HTTP {status}: {text}")
                }
                Err(err) => err.to_string(),
            };

            let now = tokio::time::Instant::now();
            if now >= deadline {
                anyhow::bail!(
                    "InfluxDB write did not succeed within the {:?} retry budget after \
                     {attempt} attempt(s); last error: {failure}",
                    self.retry.total_budget,
                );
            }
            let shift = (attempt - 1).min(16); // 16 is already far past max_delay's cap
            let backoff = self
                .retry
                .base_delay
                .saturating_mul(2u32.pow(shift))
                .min(self.retry.max_delay)
                .min(deadline.saturating_duration_since(now));

            self.diag.warn(format_args!(
                "InfluxDB write attempt {attempt} failed ({failure}), retrying in {backoff:?}"
            ));
            tokio::time::sleep(backoff).await;
        }
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
#[derive(Default)]
struct InfluxLineEncoder {
    diag: Diagnostics,
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
        // them. This map holds, per series, a "next free slot" successor map covering every
        // timestamp actually allocated to it so far -- not just the most recent one (see the
        // comment at its use site for why that's not enough either, and for what the successor
        // map buys over a plain occupied-set). Kept at batch scope, not per-event or per-metric:
        // that's what lets it disambiguate collisions across the whole batch, not just within
        // one event.
        let mut series_allocated_timestamps: HashMap<String, HashMap<i64, i64>> = HashMap::new();
        for event in &batch.events {
            if event.metrics.is_empty() {
                continue; // a log-only, span-only, or empty event: nothing to encode
            }
            // Tags come from `resource.attributes` + `event.attributes` only -- nothing
            // metric-specific -- so they're identical for every metric on this event. Rendered
            // once per event, not once per metric.
            let tag_suffix = render_tag_suffix(&batch.resource, event);
            for metric in &event.metrics {
                // One bad metric shouldn't drop its event's other metrics, let alone the rest of
                // the batch, so this logs and skips rather than propagating via `?`. Deliberately
                // on the inner, per-metric loop: a `Set` or `#`-prefixed metric sharing an event
                // with a perfectly good one must not take the good one down with it.
                if let Err(err) = encode_metric_line(
                    &mut buf,
                    &tag_suffix,
                    metric,
                    event.timestamp,
                    &mut series_allocated_timestamps,
                ) {
                    self.diag.warn_throttled("encode_error", err);
                }
            }
        }
        Ok(Bytes::from(buf.into_bytes()))
    }
}

/// The `,key=value,key=value` line-protocol tag suffix for one event: resource attributes first,
/// event attributes overriding on key collision (an event-level tag is more specific than the
/// batch-wide resource it came from). Depends on nothing metric-specific, so it's computed once
/// per event and shared across every metric that event carries. Infallible by construction: a tag
/// that can't be represented (an empty key/value, or one with an embedded newline -- line
/// protocol has no escape for that at all) is dropped individually rather than escalated to a
/// whole-point error.
fn render_tag_suffix(resource: &Resource, event: &Event) -> String {
    let mut suffix = String::new();
    let mut tags = resource.attributes.clone();
    for (key, value) in event.attributes.iter() {
        tags.insert(resolve(key), value.clone());
    }
    for (key, value) in tags.iter() {
        let key = resolve(key);
        let Some(value) = value_as_tag_string(value) else {
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
        suffix.push_str(&escape_tag(key));
        suffix.push('=');
        suffix.push_str(&escape_tag(&value));
    }
    suffix
}

fn encode_metric_line(
    buf: &mut String,
    tag_suffix: &str,
    metric: &MetricRecord,
    timestamp: i64,
    series_allocated_timestamps: &mut HashMap<String, HashMap<i64, i64>>,
) -> Result<(), CodecError> {
    let fields = metric_fields(&metric.kind)?;
    if fields.is_empty() {
        // Every field was non-finite (see `format_float`) or otherwise unrepresentable -- an
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

    // Built in a local buffer first, not `buf` directly: if a later step rejects (there are none
    // right now, but there were -- see `render_tag_suffix`'s doc comment for the history), only a
    // complete, valid line ever reaches `buf`. "measurement + tags" is also the series-identity
    // key fed to `series_allocated_timestamps` below -- the tag half is shared across an event's
    // metrics (see `encode`), but the measurement isn't, so this string still has to be rebuilt
    // once per metric even though `tag_suffix` itself is computed only once.
    let mut line = escape_measurement(measurement);
    line.push_str(tag_suffix);

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
    let next_free = series_allocated_timestamps.entry(line.clone()).or_default();
    let timestamp = allocate_timestamp(next_free, timestamp).ok_or_else(|| {
        CodecError::Malformed(format!(
            "no free timestamp slot for series {line:?} near {timestamp} (i64 overflow)"
        ))
    })?;

    // This matters in practice today: `logit-inputs::statsd` assigns one timestamp to an entire
    // datagram, and its multi-value form (`name:1:2:3|c`) expands into several otherwise-
    // identical events, all sharing that timestamp -- a nanosecond-scale perturbation here is far
    // below any input's actual timing resolution, and far simpler than guessing at how to
    // aggregate same-series samples together, which the source protocol never specified.

    line.push(' ');
    for (i, (key, value)) in fields.iter().enumerate() {
        if i > 0 {
            line.push(',');
        }
        line.push_str(&escape_tag(key));
        line.push('=');
        line.push_str(value);
    }
    line.push(' ');
    line.push_str(&timestamp.to_string());

    buf.push_str(&line);
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
fn allocate_timestamp(next_free: &mut HashMap<i64, i64>, requested: i64) -> Option<i64> {
    let mut visited = Vec::new();
    let mut cur = requested;
    while let Some(&next) = next_free.get(&cur) {
        visited.push(cur);
        cur = next;
    }
    // `cur` is now free. Reserve it, and repoint every occupied slot visited on the way here
    // directly at its successor so the next walk through any of them stops immediately.
    let successor = cur.checked_add(1)?;
    next_free.insert(cur, successor);
    for slot in visited {
        next_free.insert(slot, successor);
    }
    Some(cur)
}

/// The line-protocol field set for one metric. `Counter`/`Gauge` are a single `value` field;
/// `Distribution` writes `count` (as an unsigned integer -- see [`format_uint`]) plus a few fixed
/// percentiles (meaningful today even for the single-sample sketches `logit-inputs::statsd`
/// currently produces -- p50/p90/p99 of one sample are all just that sample). `Histogram` maps
/// its buckets onto fields directly; `Summary` maps its quantiles onto fields keyed by the raw
/// quantile value rather than a rounded percentage, since rounding isn't collision-free (0.991
/// and 0.994 would both round to "p99" and overwrite each other within one line's field set).
/// `Set` has no encoding yet: it needs a real `HyperLogLog` (`logit_core::metric::HyperLogLog` is
/// still a stub), so this returns an error rather than inventing a meaningless one.
fn metric_fields(kind: &MetricKind) -> Result<Vec<(String, String)>, CodecError> {
    match kind {
        MetricKind::Counter(v) | MetricKind::Gauge(v) => {
            Ok(format_float(*v).map(|v| vec![("value".to_string(), v)]).unwrap_or_default())
        }
        MetricKind::Distribution(sketch) => {
            let mut fields = vec![("count".to_string(), format_uint(sketch.count() as u64))];
            for q in [0.5, 0.9, 0.99] {
                if let Some(v) = sketch.quantile(q).and_then(format_float) {
                    fields.push((format!("p{}", (q * 100.0).round() as u32), v));
                }
            }
            Ok(fields)
        }
        MetricKind::Histogram { buckets } => Ok(buckets
            .iter()
            .filter_map(|(bound, count)| {
                format_float(*bound).map(|b| (format!("bucket_{b}"), format_uint(*count)))
            })
            .collect()),
        MetricKind::Summary { quantiles } => Ok(quantiles
            .iter()
            .filter_map(|(q, v)| format_float(*v).map(|v| (format!("q{q}"), v)))
            .collect()),
        MetricKind::Set(_) => {
            Err(CodecError::Malformed("Set metrics have no line-protocol encoding yet".to_string()))
        }
    }
}

/// Line protocol has no representation for non-finite floats; guard here rather than writing
/// `value=NaN` and letting InfluxDB reject the whole write. (A client can produce this today --
/// statsd's `f64::parse` accepts the literal text "NaN"/"inf" -- so this isn't a purely
/// theoretical guard.)
fn format_float(v: f64) -> Option<String> {
    v.is_finite().then(|| v.to_string())
}

/// Line-protocol unsigned-integer field (the `u` suffix, InfluxDB 2.x). Without it, a bare number
/// is parsed as `f64` by default, which loses integer semantics and, above 2^53, exactness --
/// real concerns for a count that legitimately grows past that in a long-running series.
fn format_uint(v: u64) -> String {
    format!("{v}u")
}

fn value_as_tag_string(v: &Value) -> Option<String> {
    match v {
        Value::Bool(b) => Some(b.to_string()),
        Value::I64(i) => Some(i.to_string()),
        Value::U64(u) => Some(u.to_string()),
        Value::F64(f) => Some(f.to_string()),
        Value::Str(s) => std::str::from_utf8(s).ok().map(str::to_string),
        // Null, Bytes, Timestamp, Array, and Map have no sensible plain-text tag representation.
        Value::Null | Value::Bytes(_) | Value::Timestamp(_) | Value::Array(_) | Value::Map(_) => {
            None
        }
    }
}

fn escape_measurement(s: &str) -> String {
    s.replace('\\', "\\\\").replace(',', "\\,").replace(' ', "\\ ")
}

fn escape_tag(s: &str) -> String {
    s.replace('\\', "\\\\").replace(',', "\\,").replace('=', "\\=").replace(' ', "\\ ")
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

    /// A stalled connection now gets retried (a request timeout is a transport error, and
    /// transport errors are retryable -- ADR 0013), so this pins the *budget-bounded* behavior
    /// rather than a single 200ms timeout: give up promptly once the retry budget itself is
    /// exhausted, never hang, and never burn anywhere near the default ~5s budget in a test.
    #[tokio::test]
    async fn write_gives_up_within_its_retry_budget_against_a_permanently_stalled_server() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Accepts every connection (so this isn't just "connection refused") and never responds
        // on any of them -- the exact scenario a request timeout exists to catch, now exercised
        // across however many retry attempts fit in the budget below.
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { return };
                std::mem::forget(stream); // keep it open; a dropped socket would close cleanly
            }
        });

        let mut output = InfluxDbOutput::new(
            format!("http://{addr}"),
            "org".to_string(),
            "bucket".to_string(),
            "token".to_string(),
        )
        .with_timeout(Duration::from_millis(50))
        .with_retry(RetryPolicy {
            total_budget: Duration::from_millis(300),
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(50),
            attempt_timeout: Duration::from_millis(50),
        });

        let batch = batch_with(vec![metric_event("x", MetricKind::Counter(1.0), &[])]);
        let start = std::time::Instant::now();
        let result = output.send(batch).await;

        assert!(result.is_err(), "expected the stalled write to eventually give up, not hang");
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "should give up within the ~300ms retry budget, not hang: took {:?}",
            start.elapsed()
        );
    }

    /// A bare HTTP/1.1 server for retry tests: writes back one canned response per accepted
    /// connection (repeating the last one past the end of the list), then closes -- matching this
    /// file's existing no-mock-crate style (`write_gives_up_within_its_retry_budget_...` above).
    /// `Connection: close` on every response means reqwest opens a fresh connection per request
    /// rather than reusing one, so the returned counter is exactly the number of `send` attempts.
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
    const RESP_429: &str =
        "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    const RESP_503: &str =
        "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

    fn fast_retry() -> RetryPolicy {
        RetryPolicy {
            total_budget: Duration::from_secs(2),
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
            attempt_timeout: Duration::from_millis(500),
        }
    }

    #[tokio::test]
    async fn retries_on_5xx_then_succeeds() {
        let (addr, count) = canned_server(vec![RESP_503, RESP_503, RESP_204]).await;
        let mut output = InfluxDbOutput::new(
            format!("http://{addr}"),
            "org".into(),
            "bucket".into(),
            "token".into(),
        )
        .with_retry(fast_retry());

        let batch = batch_with(vec![metric_event("x", MetricKind::Counter(1.0), &[])]);
        output.send(batch).await.expect("should eventually succeed once the 503s clear");
        assert_eq!(
            count.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "expected exactly 3 attempts: two 503s, then the 204"
        );
    }

    #[tokio::test]
    async fn retries_a_429_rate_limit_response() {
        // 429 is InfluxDB's own rate-limit response and genuinely transient -- the one deliberate
        // deviation from "a 4xx is a hard failure" (ADR 0013).
        let (addr, count) = canned_server(vec![RESP_429, RESP_204]).await;
        let mut output = InfluxDbOutput::new(
            format!("http://{addr}"),
            "org".into(),
            "bucket".into(),
            "token".into(),
        )
        .with_retry(fast_retry());

        let batch = batch_with(vec![metric_event("x", MetricKind::Counter(1.0), &[])]);
        output.send(batch).await.expect("should succeed after the rate limit clears");
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn does_not_retry_a_4xx_other_than_429() {
        let (addr, count) = canned_server(vec![RESP_400]).await;
        let mut output = InfluxDbOutput::new(
            format!("http://{addr}"),
            "org".into(),
            "bucket".into(),
            "token".into(),
        )
        .with_retry(fast_retry());

        let batch = batch_with(vec![metric_event("x", MetricKind::Counter(1.0), &[])]);
        let result = output.send(batch).await;
        assert!(result.is_err(), "a 400 should still fail send");
        assert_eq!(
            count.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a 4xx other than 429 must fail on the first attempt, not retry"
        );
    }

    #[tokio::test]
    async fn exhausts_the_retry_budget_and_fails_on_a_persistent_5xx() {
        let (addr, count) = canned_server(vec![RESP_503]).await; // every attempt gets a 503
        let mut output = InfluxDbOutput::new(
            format!("http://{addr}"),
            "org".to_string(),
            "bucket".to_string(),
            "token".to_string(),
        )
        .with_retry(RetryPolicy {
            total_budget: Duration::from_millis(100),
            base_delay: Duration::from_millis(5),
            max_delay: Duration::from_millis(20),
            attempt_timeout: Duration::from_millis(500),
        });

        let batch = batch_with(vec![metric_event("x", MetricKind::Counter(1.0), &[])]);
        let start = std::time::Instant::now();
        let result = output.send(batch).await;

        assert!(result.is_err(), "persistent 5xx should still fail once the budget runs out");
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "should give up within the ~100ms budget, not hang: took {:?}",
            start.elapsed()
        );
        assert!(
            count.load(std::sync::atomic::Ordering::SeqCst) > 1,
            "expected more than one attempt inside the retry budget"
        );
    }
}
