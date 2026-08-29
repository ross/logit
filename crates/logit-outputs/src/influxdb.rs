//! InfluxDB 2.x line-protocol output -- the output side of the v0.1 vertical slice
//! (`docs/OVERVIEW.md`), writing to `/api/v2/write` with org/bucket query params and a
//! `Token` auth header. Matches the `influxdb` service seeded in `compose.yaml` for local testing.

use crate::Output;
use bytes::Bytes;
use logit_core::interner::resolve;
use logit_core::{Event, EventBatch, MetricKind, MetricRecord, Payload, Resource, Value};
use logit_proto::{CodecError, Encoder};
use std::collections::HashMap;
use std::time::Duration;

/// `reqwest` has no request timeout by default. Without one, a server that accepts the TCP
/// connection but never responds would hang this output's `send` future -- and the pipeline
/// worker driving it -- indefinitely.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

pub struct InfluxDbOutput {
    url: String,
    org: String,
    bucket: String,
    token: String,
    client: reqwest::Client,
    encoder: InfluxLineEncoder,
}

impl InfluxDbOutput {
    pub fn new(url: String, org: String, bucket: String, token: String) -> Self {
        Self {
            url,
            org,
            bucket,
            token,
            client: build_client(DEFAULT_TIMEOUT),
            encoder: InfluxLineEncoder,
        }
    }

    /// Overrides the default 10s request timeout. Rebuilds the underlying HTTP client.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.client = build_client(timeout);
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
            // Nothing in this batch had a line-protocol encoding (e.g. all logs/traces, or all
            // Set metrics -- see `metric_fields` below). Not an error; nothing to write.
            return Ok(());
        }

        let write_url = format!("{}/api/v2/write", self.url.trim_end_matches('/'));
        let resp = self
            .client
            .post(&write_url)
            .query(&[
                ("org", self.org.as_str()),
                ("bucket", self.bucket.as_str()),
                ("precision", "ns"),
            ])
            .header("Authorization", format!("Token {}", self.token))
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("InfluxDB write failed ({status}): {text}");
        }
        Ok(())
    }
}

/// Encodes an [`EventBatch`] as InfluxDB line protocol. Split out from [`InfluxDbOutput`] so the
/// encoding logic is directly unit-testable without an HTTP server.
///
/// Only [`Payload::Metric`] events have a line-protocol mapping; logs and traces are skipped
/// (`docs/OVERVIEW.md`'s v0.1 slice is metrics-only -- there's no established convention yet for
/// what a log line or span becomes in InfluxDB, and guessing one isn't this output's job).
struct InfluxLineEncoder;

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
        // map buys over a plain occupied-set).
        let mut series_allocated_timestamps: HashMap<String, HashMap<i64, i64>> = HashMap::new();
        for event in &batch.events {
            let Payload::Metric(metric) = &event.payload else {
                continue;
            };
            // One bad metric shouldn't drop every other metric in the batch, so this logs and
            // skips rather than propagating via `?`.
            // TODO: route through a proper diagnostics facility once one exists, instead of
            // stderr -- same gap noted in logit-inputs::statsd.
            if let Err(err) = encode_metric_line(
                &mut buf,
                &batch.resource,
                event,
                metric,
                &mut series_allocated_timestamps,
            ) {
                eprintln!("influxdb output: {err}");
            }
        }
        Ok(Bytes::from(buf.into_bytes()))
    }
}

fn encode_metric_line(
    buf: &mut String,
    resource: &Resource,
    event: &Event,
    metric: &MetricRecord,
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
    // right now, but there were -- see the tag-validation loop below, which used to write
    // straight into `buf` and could leave a truncated, newline-less fragment behind on an
    // invalid tag, corrupting whatever line got appended after it). Only a complete, valid line
    // ever reaches `buf`.
    let mut line = escape_measurement(measurement);

    // Tags: resource attributes (host, service, ...) first, event attributes override on key
    // collision -- an event-level tag is more specific than the batch-wide resource it came from.
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
        // rejected this whole line (and, via the shared-buffer bug above, everything after it in
        // the batch). Drop just this one tag rather than the whole metric: the point is still
        // meaningful without it.
        if key.is_empty()
            || value.is_empty()
            || key.contains(['\n', '\r'])
            || value.contains(['\n', '\r'])
        {
            continue;
        }
        line.push(',');
        line.push_str(&escape_tag(key));
        line.push('=');
        line.push_str(&escape_tag(&value));
    }

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
    // chain of already-occupied slots starting at `event.timestamp`, and repoints every slot it
    // visits directly at the free slot it finds -- so the next probe starting anywhere on that
    // chain jumps straight there instead of re-walking it. A timestamp that isn't already used is
    // still returned untouched, regardless of arrival order; only a genuine collision costs a 1ns
    // nudge, and repeated collisions on the same series no longer cost more than a couple of
    // lookups each once the chain has been compressed once.
    let next_free = series_allocated_timestamps.entry(line.clone()).or_default();
    let timestamp = allocate_timestamp(next_free, event.timestamp).ok_or_else(|| {
        CodecError::Malformed(format!(
            "no free timestamp slot for series {line:?} near {} (i64 overflow)",
            event.timestamp
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
        Event {
            timestamp: 1_700_000_000_000_000_000,
            attributes,
            payload: Payload::Metric(MetricRecord {
                name: logit_core::interner::intern(name),
                kind,
                unit: None,
            }),
        }
    }

    fn encode(events: Vec<Event>) -> String {
        let mut encoder = InfluxLineEncoder;
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

    /// The concrete behavioral consequence `tmp/lua-value-type-preservation.md` and PR #6 review
    /// discussion_r3887008990 describe: `value_as_tag_string` treats `Value::Bytes` and
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

        let mut encoder = InfluxLineEncoder;
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
    fn non_metric_payloads_are_skipped() {
        let log_event = Event {
            timestamp: 0,
            attributes: AttrMap::new(),
            payload: Payload::Log(LogRecord {
                message: Value::str("hello"),
                severity: None,
                body_format: BodyFormat::Raw,
            }),
        };
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
            .map(|v| Event {
                timestamp: same_ts,
                attributes: AttrMap::new(),
                payload: Payload::Metric(MetricRecord {
                    name: logit_core::interner::intern("page.views"),
                    kind: MetricKind::Counter(v),
                    unit: None,
                }),
            })
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
            Event {
                timestamp: 101,
                attributes: AttrMap::new(),
                payload: Payload::Metric(MetricRecord {
                    name: logit_core::interner::intern("page.views"),
                    kind: MetricKind::Counter(1.0),
                    unit: None,
                }),
            },
            Event {
                timestamp: 100,
                attributes: AttrMap::new(),
                payload: Payload::Metric(MetricRecord {
                    name: logit_core::interner::intern("page.views"),
                    kind: MetricKind::Counter(2.0),
                    unit: None,
                }),
            },
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
        let events: Vec<Event> = (0..N)
            .map(|_| Event {
                timestamp: start_ts,
                attributes: AttrMap::new(),
                payload: Payload::Metric(MetricRecord {
                    name: logit_core::interner::intern("page.views"),
                    kind: MetricKind::Counter(1.0),
                    unit: None,
                }),
            })
            .collect();

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
            .map(|ts| Event {
                timestamp: ts,
                attributes: AttrMap::new(),
                payload: Payload::Metric(MetricRecord {
                    name: logit_core::interner::intern("page.views"),
                    kind: MetricKind::Counter(1.0),
                    unit: None,
                }),
            })
            .collect();

        let out = encode(events);
        let mut timestamps: Vec<i64> =
            out.lines().map(|l| l.rsplit(' ').next().unwrap().parse().unwrap()).collect();
        timestamps.sort_unstable();
        assert_eq!(timestamps, vec![100, 101, 102, 103, 104], "got: {out}");
    }

    #[tokio::test]
    async fn write_times_out_against_a_stalled_server() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Accepts the TCP connection (so this isn't just "connection refused") and then never
        // responds -- the exact scenario a request timeout exists to catch.
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                std::mem::forget(stream); // keep it open; a dropped socket would close cleanly
            }
        });

        let mut output = InfluxDbOutput::new(
            format!("http://{addr}"),
            "org".to_string(),
            "bucket".to_string(),
            "token".to_string(),
        )
        .with_timeout(Duration::from_millis(200));

        let batch = batch_with(vec![metric_event("x", MetricKind::Counter(1.0), &[])]);
        let start = std::time::Instant::now();
        let result = output.send(batch).await;

        assert!(result.is_err(), "expected the stalled write to time out, not hang or succeed");
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "should have timed out at ~200ms, not hung: took {:?}",
            start.elapsed()
        );
    }
}
