//! `shape`: an observer that rewrites each event in place into a measurement of its shape
//! (attribute counts, nesting, key and value lengths, value types, metric and span widths), and on
//! its flush interval emits the per-batch and cumulative facts `process` has nowhere to put.
//! `docs/adr/shape-observer-component.md` is the design; `docs/plans/data-shape-survey.md` is
//! the survey it instruments.
//!
//! Tapped off a flow by ordinary fan-out, never placed in it:
//!
//! ```text
//! statsd_in ─┬─> (real pipeline)
//!            └─> shape ─> aggregate ─> any sink
//! ```
//!
//! Check all three properties on any change here:
//!
//! - **It emits counts and lengths only.** No attribute key, attribute value, log body, or metric
//!   name from an observed event appears in anything it produces: not a metric, a tag, a
//!   diagnostic, or a telemetry point. That's what lets its output leave an environment the
//!   traffic can't. The two exceptions are `resource: keep` ([`Shape::with_resource_kept`]) and
//!   the batch's `Scope`, which passes through because `Transform` has no hook to replace it.
//! - **It emits raw `Samples`, never a sketch.** Every distribution-shaped quantity is a
//!   [`MetricKind::Samples`]; sketching, windowing, and keying are a downstream `aggregate`'s
//!   job (`docs/adr/lossless-transit.md`'s "summarization is opt-in and named").
//! - **Every name it emits is interned once, at construction** ([`Names`]), never per event.
//!
//! A measurement event carries about a dozen metric records, so it always spills `MetricList`'s
//! inline slot; `crates/logit-bench/tests/allocations.rs` pins that (`docs/design/memory.md` §2).

use bytes::Bytes;
use logit_core::interner::{intern, resolve, Symbol};
use logit_core::{
    AttrMap, Event, MetricKind, MetricList, MetricRecord, Provenance, Resource, Samples, Scope,
    SpanLink, Telemetry, Value,
};
use logit_pipeline::{FlushOutput, Transform};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

/// Batches one flush window records before dropping and counting the rest: a memory guard, not a
/// tuning knob, like `aggregate`'s `max_retained_series`. Bounds the window at four `Vec<f64>`s of
/// 4096 across every `source`.
const MAX_BATCHES_PER_WINDOW: usize = 4096;

/// `logit_config::ComponentKind::Shape`'s `max_tracked_keys` default, so a direct
/// [`Shape::new`] caller gets the same bound a config does.
pub const DEFAULT_MAX_TRACKED_KEYS: usize = 4096;

/// `logit_config::ComponentKind::Shape`'s `max_tracked_keysets` default.
pub const DEFAULT_MAX_TRACKED_KEYSETS: usize = 4096;

/// The reported value types, in [`Value`]'s variant order. `I64` and `U64` share one `int` bucket:
/// which one a decoder produced is an artifact of the wire format (`serde_json` picks `U64` for an
/// unsigned literal), not of the producer's data.
const VALUE_TYPES: usize = 9;

/// One name suffix per [`VALUE_TYPES`] bucket -- `logit.shape.values.<suffix>`.
const VALUE_TYPE_NAMES: [&str; VALUE_TYPES] =
    ["null", "bool", "int", "float", "bytes", "string", "timestamp", "array", "map"];

/// Which [`VALUE_TYPES`] bucket a value counts in. Exhaustive, so a new `Value` variant fails to
/// compile here instead of vanishing from the survey.
fn value_type_index(value: &Value) -> usize {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::I64(_) | Value::U64(_) => 2,
        Value::F64(_) => 3,
        Value::Bytes(_) => 4,
        Value::Str(_) => 5,
        Value::Timestamp(_) => 6,
        Value::Array(_) => 7,
        Value::Map(_) => 8,
    }
}

/// The `signal` tag's eight values, indexed by the bitmask `log | metric<<1 | span<<2`. One
/// `+`-joined value rather than three booleans, so signal co-occurrence is an ordinary tag value
/// downstream.
const SIGNAL_NAMES: [&str; 8] =
    ["none", "log", "metric", "log+metric", "span", "log+span", "metric+span", "log+metric+span"];

/// Every metric name and tag key this component emits, interned once at [`Shape::new`] so the
/// per-record path never probes the process-wide table (`docs/design/memory.md` §4).
struct Names {
    events: Symbol,
    attributes: Symbol,
    nested_maps: Symbol,
    nested_map_width: Symbol,
    value_depth: Symbol,
    key_bytes: Symbol,
    value_bytes: Symbol,
    values: [Symbol; VALUE_TYPES],
    metrics: Symbol,
    samples_per_metric: Symbol,
    body_bytes: Symbol,
    span_events: Symbol,
    span_links: Symbol,
    span_event_attributes: Symbol,
    batch_events: Symbol,
    batch_resource_attributes: Symbol,
    batch_scope_attributes: Symbol,
    batch_keysets: Symbol,
    distinct_keys: Symbol,
    distinct_keysets: Symbol,
    keyset_share_top1: Symbol,
    keyset_share_top5: Symbol,
    tracking_overflow: Symbol,
    tag_signal: Symbol,
    tag_source: Symbol,
    tag_tap: Symbol,
    /// The eight `signal` tag values as static `Value`s, free to build and to clone.
    signals: [Value; 8],
}

impl Names {
    fn new() -> Self {
        Names {
            events: intern("logit.shape.events"),
            attributes: intern("logit.shape.attributes"),
            nested_maps: intern("logit.shape.nested_maps"),
            nested_map_width: intern("logit.shape.nested_map_width"),
            value_depth: intern("logit.shape.value_depth"),
            key_bytes: intern("logit.shape.key_bytes"),
            value_bytes: intern("logit.shape.value_bytes"),
            values: std::array::from_fn(|i| {
                intern(&format!("logit.shape.values.{}", VALUE_TYPE_NAMES[i]))
            }),
            metrics: intern("logit.shape.metrics"),
            samples_per_metric: intern("logit.shape.samples_per_metric"),
            body_bytes: intern("logit.shape.body_bytes"),
            span_events: intern("logit.shape.span_events"),
            span_links: intern("logit.shape.span_links"),
            span_event_attributes: intern("logit.shape.span_event_attributes"),
            batch_events: intern("logit.shape.batch.events"),
            batch_resource_attributes: intern("logit.shape.batch.resource_attributes"),
            batch_scope_attributes: intern("logit.shape.batch.scope_attributes"),
            batch_keysets: intern("logit.shape.batch.keysets"),
            distinct_keys: intern("logit.shape.distinct_keys"),
            distinct_keysets: intern("logit.shape.distinct_keysets"),
            keyset_share_top1: intern("logit.shape.keyset_share.top1"),
            keyset_share_top5: intern("logit.shape.keyset_share.top5"),
            tracking_overflow: intern("logit.shape.tracking_overflow"),
            tag_signal: intern("signal"),
            tag_source: intern("source"),
            tag_tap: intern("tap"),
            signals: std::array::from_fn(|i| static_str_value(SIGNAL_NAMES[i])),
        }
    }
}

/// A `&'static str` as a `Value::Str` with no allocation. Every string this component emits goes
/// through here or [`symbol_value`], which is why no tag value allocates.
fn static_str_value(s: &'static str) -> Value {
    Value::Str(Bytes::from_static(s.as_bytes()))
}

/// A component id (this component's, or a batch's `origin`) as a tag value, allocation-free
/// because `resolve` returns `&'static str`. Never apply it to an observed attribute key.
fn symbol_value(sym: Symbol) -> Value {
    static_str_value(resolve(sym))
}

/// Per-event scratch, cleared rather than rebuilt, so `process`'s pinned cost is only the
/// `MetricList` spill and any `Samples` that outgrow `SAMPLES_INLINE`.
#[derive(Default)]
struct Scratch {
    key_bytes: Vec<f64>,
    value_bytes: Vec<f64>,
    map_widths: Vec<f64>,
    samples_per_metric: Vec<f64>,
    span_event_attributes: Vec<f64>,
    type_counts: [u64; VALUE_TYPES],
}

impl Scratch {
    fn reset(&mut self) {
        self.key_bytes.clear();
        self.value_bytes.clear();
        self.map_widths.clear();
        self.samples_per_metric.clear();
        self.span_event_attributes.clear();
        self.type_counts = [0; VALUE_TYPES];
    }
}

/// Accounts one attribute value and everything under it, returning its nesting depth: `0` for a
/// scalar, `1 + max(child depth)` for an `Array`/`Map` (an empty container is `1`).
///
/// Unbounded recursion is safe here: `value_heap_bytes` (behind `Event::estimated_heap_bytes`)
/// recurses the same way on the queue push that delivered the event, so it would overflow first.
fn walk(value: &Value, scratch: &mut Scratch) -> u32 {
    scratch.type_counts[value_type_index(value)] += 1;
    match value {
        Value::Str(b) | Value::Bytes(b) => {
            scratch.value_bytes.push(b.len() as f64);
            0
        }
        Value::Array(items) => {
            let mut deepest = 0;
            for item in items {
                deepest = deepest.max(walk(item, scratch));
            }
            deepest + 1
        }
        Value::Map(map) => {
            scratch.map_widths.push(map.len() as f64);
            let mut deepest = 0;
            for (_, nested) in map.iter() {
                deepest = deepest.max(walk(nested, scratch));
            }
            deepest + 1
        }
        Value::Null
        | Value::Bool(_)
        | Value::I64(_)
        | Value::U64(_)
        | Value::F64(_)
        | Value::Timestamp(_) => 0,
    }
}

/// A log body's length in bytes, or `0` for a non-string body (Lua can set a number).
fn body_len(message: &Value) -> usize {
    match message {
        Value::Str(b) | Value::Bytes(b) => b.len(),
        _ => 0,
    }
}

/// A key-set's identity: a 64-bit hash of the event's `Symbol` sequence. `AttrMap::iter` yields
/// sorted `Symbol`s, so equal key-sets hash equal with no sort. The length goes first so a prefix
/// can't collide. A hash, because the key-sets themselves would be observed data.
fn keyset_hash(attrs: &AttrMap) -> u64 {
    let mut hasher = DefaultHasher::new();
    (attrs.len() as u64).hash(&mut hasher);
    for (key, _) in attrs.iter() {
        key.hash(&mut hasher);
    }
    hasher.finish()
}

/// Which payloads an event carries, as an index into [`SIGNAL_NAMES`].
fn signal_index(event: &Event) -> usize {
    usize::from(event.log.is_some())
        | (usize::from(!event.metrics.is_empty()) << 1)
        | (usize::from(event.span.is_some()) << 2)
}

/// One flush window's per-batch measurements for one `source`, one value per batch in arrival
/// order. Parallel `Vec`s so each goes straight to `Samples::new`.
#[derive(Default)]
struct BatchSamples {
    events: Vec<f64>,
    resource_attributes: Vec<f64>,
    scope_attributes: Vec<f64>,
    keysets: Vec<f64>,
}

/// The current batch, accumulated across `observe_scope`/`map_resource`/`process` and committed
/// by `end_batch`, because `process` can't emit a second event to carry a per-batch fact.
#[derive(Default)]
struct BatchTally {
    events: u64,
    resource_attributes: u64,
    scope_attributes: u64,
}

pub struct Shape {
    interval: Duration,
    keep_resource: bool,
    max_tracked_keys: usize,
    max_tracked_keysets: usize,
    names: Names,
    /// The empty resource `map_resource` substitutes under the default `resource: drop`,
    /// `Arc`-cloned per batch.
    empty_resource: Arc<Resource>,
    /// The `tap` tag: this component's id, so two taps feeding one `aggregate` stay distinct.
    tap: Option<Symbol>,
    /// The `source` tag: the current batch's provenance `origin`, cached as `has_provenance`
    /// caches it.
    source: Option<Symbol>,
    scratch: Scratch,
    batch: BatchTally,
    /// Distinct key-set hashes in the current batch.
    batch_keysets: HashSet<u64>,
    /// This window's per-batch measurements by `source`; a `BTreeMap` so flush order is
    /// deterministic.
    window: BTreeMap<Option<Symbol>, BatchSamples>,
    window_batches: usize,
    /// Distinct top-level attribute keys since start, capped at `max_tracked_keys`.
    keys: HashSet<Symbol>,
    /// Distinct top-level key-sets since start with their event counts (for `keyset_share`),
    /// capped at `max_tracked_keysets`.
    keysets: HashMap<u64, u64>,
    keys_untracked: u64,
    keysets_untracked: u64,
    overflow: bool,
    events_seen: u64,
    telemetry: Telemetry,
}

impl Shape {
    /// `interval` paces the per-batch and cumulative measurements; per-event ones don't wait.
    pub fn new(interval: Duration) -> Self {
        Shape {
            interval,
            keep_resource: false,
            max_tracked_keys: DEFAULT_MAX_TRACKED_KEYS,
            max_tracked_keysets: DEFAULT_MAX_TRACKED_KEYSETS,
            names: Names::new(),
            empty_resource: Arc::new(Resource::default()),
            tap: None,
            source: None,
            scratch: Scratch::default(),
            batch: BatchTally::default(),
            batch_keysets: HashSet::new(),
            window: BTreeMap::new(),
            window_batches: 0,
            keys: HashSet::new(),
            keysets: HashMap::new(),
            keys_untracked: 0,
            keysets_untracked: 0,
            overflow: false,
            events_seen: 0,
            telemetry: Telemetry::default(),
        }
    }

    /// `true` (`resource: keep`) forwards the batch resource unchanged, an operator-chosen
    /// exception to the counts-only property. The default substitutes an empty `Resource`.
    pub fn with_resource_kept(mut self, keep: bool) -> Self {
        self.keep_resource = keep;
        self
    }

    /// Caps the two cumulative tables. Past a cap a new key or key-set is counted as overflow;
    /// tracked entries keep counting, so the shares stay meaningful for what is tracked.
    pub fn with_caps(mut self, max_tracked_keys: usize, max_tracked_keysets: usize) -> Self {
        self.max_tracked_keys = max_tracked_keys;
        self.max_tracked_keysets = max_tracked_keysets;
        self
    }

    /// Sets this component's id, emitted as the `tap` tag (a component name, not observed data).
    pub fn with_name(mut self, id: &str) -> Self {
        self.tap = Some(intern(id));
        self
    }

    /// Attaches a telemetry handle for the drop counters (`logit.transform.batches.dropped`,
    /// `.keys.untracked`, `.keysets.untracked`): counts of what a bounded table didn't record,
    /// naming nothing observed.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Stamps `source`/`tap` onto a flush-emitted event, which has no one `signal` to name.
    fn tag(&self, attrs: &mut AttrMap, source: Option<Symbol>) {
        if let Some(source) = source {
            attrs.insert_sym(self.names.tag_source, symbol_value(source));
        }
        if let Some(tap) = self.tap {
            attrs.insert_sym(self.names.tag_tap, symbol_value(tap));
        }
    }

    /// The counts of the five most common tracked key-sets, descending, by an allocation-free
    /// five-slot insertion scan.
    fn top_keyset_counts(&self) -> [u64; 5] {
        let mut top = [0u64; 5];
        for &count in self.keysets.values() {
            if count <= top[4] {
                continue;
            }
            top[4] = count;
            let mut i = 4;
            while i > 0 && top[i] > top[i - 1] {
                top.swap(i, i - 1);
                i -= 1;
            }
        }
        top
    }

    /// Records one top-level key in the capped distinct-key set.
    fn track_key(&mut self, key: Symbol) {
        if self.keys.contains(&key) {
            return;
        }
        if self.keys.len() >= self.max_tracked_keys {
            self.overflow = true;
            self.keys_untracked += 1;
            return;
        }
        self.keys.insert(key);
    }

    /// Records one key-set in the capped key-set table. Past the cap, only a new key-set is
    /// turned away; a tracked one keeps counting.
    fn track_keyset(&mut self, keyset: u64) {
        if let Some(count) = self.keysets.get_mut(&keyset) {
            *count += 1;
            return;
        }
        if self.keysets.len() >= self.max_tracked_keysets {
            self.overflow = true;
            self.keysets_untracked += 1;
            return;
        }
        self.keysets.insert(keyset, 1);
    }
}

/// Appends a raw-observation record: `Samples`, never a sketch, because summarization is a
/// downstream `aggregate`'s decision (`docs/adr/lossless-transit.md`).
fn push_samples(metrics: &mut MetricList, name: Symbol, values: impl IntoIterator<Item = f64>) {
    metrics.push(MetricRecord::new(name, MetricKind::Samples(Samples::new(values))));
}

fn push_counter(metrics: &mut MetricList, name: Symbol, value: f64) {
    metrics.push(MetricRecord::new(name, MetricKind::counter(value)));
}

fn push_gauge(metrics: &mut MetricList, name: Symbol, value: f64) {
    metrics.push(MetricRecord::new(name, MetricKind::Gauge(value)));
}

impl Transform for Shape {
    /// Measures the event, then replaces its log, span, metrics, and attributes with
    /// `logit.shape.*` records under the same timestamp, tagged `signal`/`source`/`tap`. Always
    /// returns `true`: an absorbed event is a lost measurement.
    ///
    /// Measures before mutating, and clears and refills `attributes`/`metrics` rather than
    /// replacing them, so a spilled allocation is reused.
    fn process(&mut self, _resource: &Arc<Resource>, event: &mut Event) -> bool {
        let signal = self.names.signals[signal_index(event)].clone();

        // -- measure ---------------------------------------------------------------------------
        self.scratch.reset();
        let mut value_depth = 0u32;
        for (key, value) in event.attributes.iter() {
            // A `resolve` per key: a `Symbol` carries no length. Accepted on a tap branch.
            self.scratch.key_bytes.push(resolve(key).len() as f64);
            value_depth = value_depth.max(walk(value, &mut self.scratch));
        }
        let attributes = event.attributes.len() as f64;
        let nested_maps = self.scratch.map_widths.len() as f64;

        for record in &event.metrics {
            if let MetricKind::Samples(samples) = &record.kind {
                self.scratch.samples_per_metric.push(samples.values.len() as f64);
            }
        }
        let metrics = event.metrics.len() as f64;

        let body_bytes = event.log.as_ref().map(|log| body_len(&log.message) as f64);
        let span = event.span.as_ref().map(|span| {
            for span_event in &span.events {
                self.scratch.span_event_attributes.push(span_event.attributes.len() as f64);
            }
            (span.events.len() as f64, span.links.len() as f64)
        });

        let keyset = keyset_hash(&event.attributes);

        // -- fold into the batch and the cumulative tables --------------------------------------
        // A second pass: the loop above holds `&mut self.scratch`, and `track_key` needs
        // `&mut self`.
        for (key, _) in event.attributes.iter() {
            self.track_key(key);
        }
        self.track_keyset(keyset);
        self.batch_keysets.insert(keyset);
        self.batch.events += 1;
        self.events_seen += 1;

        // -- rewrite ----------------------------------------------------------------------------
        event.log = None;
        event.span = None;

        event.attributes.clear();
        event.attributes.insert_sym(self.names.tag_signal, signal);
        if let Some(source) = self.source {
            event.attributes.insert_sym(self.names.tag_source, symbol_value(source));
        }
        if let Some(tap) = self.tap {
            event.attributes.insert_sym(self.names.tag_tap, symbol_value(tap));
        }

        let type_records = self.scratch.type_counts.iter().filter(|&&n| n > 0).count();
        let record_count = 5 // events, attributes, nested_maps, value_depth, metrics
            + usize::from(!self.scratch.map_widths.is_empty())
            + usize::from(!self.scratch.key_bytes.is_empty())
            + usize::from(!self.scratch.value_bytes.is_empty())
            + type_records
            + usize::from(!self.scratch.samples_per_metric.is_empty())
            + usize::from(body_bytes.is_some())
            + if span.is_some() { 2 } else { 0 }
            + usize::from(!self.scratch.span_event_attributes.is_empty());

        let out = &mut event.metrics;
        out.clear();
        // One growth for the whole append; a measurement event always spills the inline slot.
        out.reserve(record_count);

        push_counter(out, self.names.events, 1.0);
        // Emitted even at zero: a distribution that omits its zeroes describes the wrong
        // population.
        push_samples(out, self.names.attributes, [attributes]);
        push_samples(out, self.names.nested_maps, [nested_maps]);
        push_samples(out, self.names.value_depth, [f64::from(value_depth)]);
        push_samples(out, self.names.metrics, [metrics]);

        if !self.scratch.key_bytes.is_empty() {
            push_samples(out, self.names.key_bytes, self.scratch.key_bytes.iter().copied());
        }
        if !self.scratch.value_bytes.is_empty() {
            push_samples(out, self.names.value_bytes, self.scratch.value_bytes.iter().copied());
        }
        if !self.scratch.map_widths.is_empty() {
            push_samples(out, self.names.nested_map_width, self.scratch.map_widths.iter().copied());
        }
        for (i, &count) in self.scratch.type_counts.iter().enumerate() {
            if count > 0 {
                push_counter(out, self.names.values[i], count as f64);
            }
        }
        if !self.scratch.samples_per_metric.is_empty() {
            push_samples(
                out,
                self.names.samples_per_metric,
                self.scratch.samples_per_metric.iter().copied(),
            );
        }
        if let Some(body_bytes) = body_bytes {
            push_samples(out, self.names.body_bytes, [body_bytes]);
        }
        if let Some((events, links)) = span {
            push_samples(out, self.names.span_events, [events]);
            push_samples(out, self.names.span_links, [links]);
        }
        if !self.scratch.span_event_attributes.is_empty() {
            push_samples(
                out,
                self.names.span_event_attributes,
                self.scratch.span_event_attributes.iter().copied(),
            );
        }

        true
    }

    /// Caches the batch's `origin` as the `source` tag, as `has_provenance` does.
    fn observe_provenance(&mut self, provenance: Provenance) {
        self.source = provenance.origin;
    }

    /// Records the incoming scope's attribute count. The scope passes through: `Transform` has
    /// no hook to replace it, and it names an instrumentation library, not payload.
    fn observe_scope(&mut self, scope: Option<Arc<Scope>>) {
        self.batch.scope_attributes = scope.map_or(0, |scope| scope.attributes.len() as u64);
    }

    /// Records the incoming resource's attribute count, then substitutes the empty `Resource`
    /// unless `resource: keep`. This is the only hook that sees the incoming resource.
    fn map_resource(&mut self, resource: &Arc<Resource>) -> Option<Arc<Resource>> {
        self.batch.resource_attributes = resource.attributes.len() as u64;
        if self.keep_resource {
            None
        } else {
            Some(self.empty_resource.clone())
        }
    }

    /// Commits the batch into this flush window and resets. Past [`MAX_BATCHES_PER_WINDOW`] a
    /// batch is dropped and counted.
    fn end_batch(&mut self) {
        let keysets = self.batch_keysets.len() as f64;
        let tally = std::mem::take(&mut self.batch);
        self.batch_keysets.clear();

        if self.window_batches >= MAX_BATCHES_PER_WINDOW {
            self.telemetry.count("logit.transform.batches.dropped", 1.0, &[]);
            return;
        }
        self.window_batches += 1;
        let samples = self.window.entry(self.source).or_default();
        samples.events.push(tally.events as f64);
        samples.resource_attributes.push(tally.resource_attributes as f64);
        samples.scope_attributes.push(tally.scope_attributes as f64);
        samples.keysets.push(keysets);
    }

    fn flush_interval(&self) -> Option<Duration> {
        Some(self.interval)
    }

    /// Emits one event per `source` seen this window (the per-batch measurements) plus one event
    /// of cumulative gauges, always, since a gauge nobody re-reports reads as still holding.
    ///
    /// Everything goes out under the empty resource and no scope, even with `resource: keep`: a
    /// window spans many batches. Links are empty.
    fn flush(&mut self, now: i64) -> FlushOutput {
        if self.keys_untracked > 0 {
            self.telemetry.count("logit.transform.keys.untracked", self.keys_untracked as f64, &[]);
            self.keys_untracked = 0;
        }
        if self.keysets_untracked > 0 {
            self.telemetry.count(
                "logit.transform.keysets.untracked",
                self.keysets_untracked as f64,
                &[],
            );
            self.keysets_untracked = 0;
        }
        let window = std::mem::take(&mut self.window);
        self.window_batches = 0;
        let mut events: Vec<(Event, Vec<SpanLink>)> = Vec::with_capacity(window.len() + 1);

        for (source, samples) in window {
            let mut attrs = AttrMap::new();
            self.tag(&mut attrs, source);
            let mut event = Event::empty(now, attrs);
            event.metrics.reserve(4);
            push_samples(&mut event.metrics, self.names.batch_events, samples.events);
            push_samples(
                &mut event.metrics,
                self.names.batch_resource_attributes,
                samples.resource_attributes,
            );
            push_samples(
                &mut event.metrics,
                self.names.batch_scope_attributes,
                samples.scope_attributes,
            );
            push_samples(&mut event.metrics, self.names.batch_keysets, samples.keysets);
            events.push((event, Vec::new()));
        }

        let top = self.top_keyset_counts();
        let (top1, top5) = if self.events_seen == 0 {
            (0.0, 0.0)
        } else {
            let total = self.events_seen as f64;
            (top[0] as f64 / total, top.iter().sum::<u64>() as f64 / total)
        };
        let mut attrs = AttrMap::new();
        self.tag(&mut attrs, None);
        let mut event = Event::empty(now, attrs);
        event.metrics.reserve(5);
        push_gauge(&mut event.metrics, self.names.distinct_keys, self.keys.len() as f64);
        push_gauge(&mut event.metrics, self.names.distinct_keysets, self.keysets.len() as f64);
        push_gauge(&mut event.metrics, self.names.keyset_share_top1, top1);
        push_gauge(&mut event.metrics, self.names.keyset_share_top5, top5);
        push_gauge(
            &mut event.metrics,
            self.names.tracking_overflow,
            if self.overflow { 1.0 } else { 0.0 },
        );
        events.push((event, Vec::new()));

        vec![(self.empty_resource.clone(), None, events)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{
        BodyFormat, LogRecord, Registry, SpanEvent, SpanKind, SpanRecord, SpanStatus,
    };

    fn resource() -> Arc<Resource> {
        Arc::new(Resource::default())
    }

    fn attrs(pairs: &[(&str, Value)]) -> AttrMap {
        let mut map = AttrMap::new();
        for (key, value) in pairs {
            map.insert(key, value.clone());
        }
        map
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

    fn span_record(events: usize, links: usize, event_attrs: usize) -> SpanRecord {
        SpanRecord {
            trace_id: [1; 16],
            span_id: [2; 8],
            parent_span_id: None,
            name: Value::str("GET /"),
            kind: SpanKind::Server,
            status: SpanStatus::Unset,
            events: (0..events)
                .map(|_| SpanEvent {
                    timestamp: 0,
                    name: Value::str("exception"),
                    attributes: attrs(
                        &(0..event_attrs)
                            .map(|i| (["ea0", "ea1", "ea2"][i], Value::U64(i as u64)))
                            .collect::<Vec<_>>(),
                    ),
                    dropped_attributes_count: 0,
                })
                .collect(),
            links: (0..links)
                .map(|_| SpanLink {
                    trace_id: [3; 16],
                    span_id: [4; 8],
                    attributes: AttrMap::new(),
                    flags: 0,
                    trace_state: None,
                    dropped_attributes_count: 0,
                })
                .collect(),
            end_timestamp: 0,
            flags: 0,
            ext: None,
        }
    }

    /// `logit-bench`'s `nginx_event` shape (this crate can't depend on it): ten top-level
    /// attributes and four derived metrics, the counts `docs/plans/data-shape-survey.md`'s
    /// acceptance check relies on.
    fn nginx_shaped_event() -> Event {
        let mut event = Event::log(
            0,
            attrs(&[
                ("host", Value::str("static.local")),
                ("request_method", Value::str("GET")),
                ("request_uri", Value::str("/index.html")),
                ("status", Value::U64(200)),
                ("body_bytes_sent", Value::U64(1024)),
                ("request_time", Value::F64(0.012)),
                ("upstream_response_time", Value::str("-")),
                ("remote_addr", Value::str("10.0.0.1")),
                ("http_user_agent", Value::str("curl/8.0.1")),
                ("server_protocol", Value::str("HTTP/1.1")),
            ]),
            log_record("{\"host\":\"static.local\"}"),
        );
        for name in ["nginx.requests", "nginx.bytes_sent", "nginx.status", "nginx.request_time"] {
            event.metrics.push(MetricRecord::new(intern(name), MetricKind::counter(1.0)));
        }
        event
    }

    /// The wide-JSON shape: 32 top-level string attributes, all scalars.
    fn wide_json_event() -> Event {
        let mut map = AttrMap::new();
        for i in 0..32 {
            map.insert(&format!("wide_field_{i:02}"), Value::str(format!("value-{i:02}")));
        }
        Event::log(0, map, log_record("{}"))
    }

    fn samples<'a>(event: &'a Event, name: &str) -> Option<&'a [f64]> {
        event.metrics.iter().find_map(|m| match &m.kind {
            MetricKind::Samples(s) if resolve(m.name) == name => Some(s.values.as_slice()),
            _ => None,
        })
    }

    fn counter(event: &Event, name: &str) -> Option<f64> {
        event.metrics.iter().find_map(|m| match &m.kind {
            MetricKind::Sum(sum) if resolve(m.name) == name => Some(sum.value),
            _ => None,
        })
    }

    fn gauge(event: &Event, name: &str) -> Option<f64> {
        event.metrics.iter().find_map(|m| match &m.kind {
            MetricKind::Gauge(v) if resolve(m.name) == name => Some(*v),
            _ => None,
        })
    }

    fn tag(event: &Event, key: &str) -> Option<String> {
        event.attributes.get(key).and_then(|v| v.as_str()).map(String::from)
    }

    fn one(event: &Event, name: &str) -> f64 {
        let values = samples(event, name).unwrap_or_else(|| panic!("{name} should be present"));
        assert_eq!(values.len(), 1, "{name} should carry exactly one value");
        values[0]
    }

    /// Runs one event through a fresh component and hands back the rewritten event.
    fn measure(event: Event) -> Event {
        let mut shape = Shape::new(Duration::from_secs(10));
        let mut event = event;
        assert!(shape.process(&resource(), &mut event), "shape never absorbs an event");
        event
    }

    // -- per-event measurements ----------------------------------------------------------------

    #[test]
    fn the_payload_is_replaced_not_added_to() {
        let event = measure(nginx_shaped_event());
        assert!(event.log.is_none(), "the observed log body must not survive");
        assert!(event.span.is_none());
        assert_eq!(counter(&event, "logit.shape.events"), Some(1.0));
        assert_eq!(event.attributes.len(), 1, "no tap/source configured, so `signal` alone");
        assert_eq!(tag(&event, "signal").as_deref(), Some("log+metric"));
    }

    #[test]
    fn the_nginx_shape_reports_ten_attributes_and_four_metrics() {
        let event = measure(nginx_shaped_event());
        assert_eq!(one(&event, "logit.shape.attributes"), 10.0);
        assert_eq!(one(&event, "logit.shape.metrics"), 4.0);
        assert_eq!(one(&event, "logit.shape.nested_maps"), 0.0);
        assert_eq!(one(&event, "logit.shape.value_depth"), 0.0);
        assert_eq!(samples(&event, "logit.shape.key_bytes").unwrap().len(), 10);
        // Seven of the ten values are strings; `status`/`body_bytes_sent` are ints and
        // `request_time` a float, so only seven contribute a length.
        assert_eq!(samples(&event, "logit.shape.value_bytes").unwrap().len(), 7);
        assert_eq!(counter(&event, "logit.shape.values.string"), Some(7.0));
        assert_eq!(counter(&event, "logit.shape.values.int"), Some(2.0));
        assert_eq!(counter(&event, "logit.shape.values.float"), Some(1.0));
        assert_eq!(counter(&event, "logit.shape.values.map"), None, "no zero-filled noise");
    }

    #[test]
    fn the_wide_json_shape_reports_thirty_two_attributes() {
        let event = measure(wide_json_event());
        assert_eq!(one(&event, "logit.shape.attributes"), 32.0);
        assert_eq!(samples(&event, "logit.shape.key_bytes").unwrap().len(), 32);
        assert_eq!(samples(&event, "logit.shape.value_bytes").unwrap().len(), 32);
        assert_eq!(counter(&event, "logit.shape.values.string"), Some(32.0));
    }

    #[test]
    fn key_and_value_byte_lengths_are_the_resolved_lengths() {
        let event = measure(Event::empty(
            0,
            attrs(&[
                ("ab", Value::str("hello")),
                ("cdef", Value::Bytes(Bytes::from_static(b"\x00\x01\x02"))),
            ]),
        ));
        let mut keys = samples(&event, "logit.shape.key_bytes").unwrap().to_vec();
        keys.sort_by(f64::total_cmp);
        assert_eq!(keys, vec![2.0, 4.0]);
        let mut values = samples(&event, "logit.shape.value_bytes").unwrap().to_vec();
        values.sort_by(f64::total_cmp);
        assert_eq!(values, vec![3.0, 5.0]);
        assert_eq!(counter(&event, "logit.shape.values.bytes"), Some(1.0));
        assert_eq!(counter(&event, "logit.shape.values.string"), Some(1.0));
    }

    #[test]
    fn an_empty_event_still_reports_its_zeroes() {
        let event = measure(Event::empty(0, AttrMap::new()));
        assert_eq!(one(&event, "logit.shape.attributes"), 0.0);
        assert_eq!(one(&event, "logit.shape.nested_maps"), 0.0);
        assert_eq!(one(&event, "logit.shape.value_depth"), 0.0);
        assert_eq!(one(&event, "logit.shape.metrics"), 0.0);
        assert_eq!(tag(&event, "signal").as_deref(), Some("none"));
        // The per-value records are the ones that would be noise at zero.
        assert_eq!(samples(&event, "logit.shape.key_bytes"), None);
        assert_eq!(samples(&event, "logit.shape.value_bytes"), None);
        assert_eq!(samples(&event, "logit.shape.body_bytes"), None);
        assert_eq!(samples(&event, "logit.shape.span_events"), None);
    }

    #[test]
    fn nested_maps_report_their_count_widths_and_depth() {
        let mut inner = AttrMap::new();
        inner.insert("deep", Value::Map(Box::new(attrs(&[("deeper", Value::U64(1))]))));
        inner.insert("flat", Value::str("x"));
        let event = measure(Event::empty(
            0,
            attrs(&[
                ("k8s", Value::Map(Box::new(inner))),
                ("labels", Value::Map(Box::new(attrs(&[("a", Value::U64(1))])))),
                ("plain", Value::U64(7)),
            ]),
        ));

        assert_eq!(one(&event, "logit.shape.attributes"), 3.0);
        assert_eq!(one(&event, "logit.shape.nested_maps"), 3.0, "k8s, k8s.deep, labels");
        let mut widths = samples(&event, "logit.shape.nested_map_width").unwrap().to_vec();
        widths.sort_by(f64::total_cmp);
        assert_eq!(widths, vec![1.0, 1.0, 2.0]);
        // `k8s` (1) holds `deep` (2); depth counts containers, not the scalar at the bottom.
        assert_eq!(one(&event, "logit.shape.value_depth"), 2.0, "k8s -> deep");
        assert_eq!(counter(&event, "logit.shape.values.map"), Some(3.0));
        assert_eq!(counter(&event, "logit.shape.values.int"), Some(3.0), "nested ints count too");
        assert_eq!(
            samples(&event, "logit.shape.key_bytes").unwrap().len(),
            3,
            "key_bytes is top-level keys only"
        );
    }

    #[test]
    fn an_array_counts_as_one_level_of_depth() {
        let event = measure(Event::empty(
            0,
            attrs(&[("tags", Value::Array(vec![Value::str("a"), Value::str("bb")]))]),
        ));
        assert_eq!(one(&event, "logit.shape.value_depth"), 1.0);
        assert_eq!(counter(&event, "logit.shape.values.array"), Some(1.0));
        assert_eq!(counter(&event, "logit.shape.values.string"), Some(2.0));
        let mut values = samples(&event, "logit.shape.value_bytes").unwrap().to_vec();
        values.sort_by(f64::total_cmp);
        assert_eq!(values, vec![1.0, 2.0], "an array's string elements are leaves");
        assert_eq!(one(&event, "logit.shape.nested_maps"), 0.0, "an array is not a map");
    }

    #[test]
    fn every_value_variant_lands_in_a_bucket() {
        let event = measure(Event::empty(
            0,
            attrs(&[
                ("a", Value::Null),
                ("b", Value::Bool(true)),
                ("c", Value::I64(-1)),
                ("d", Value::U64(1)),
                ("e", Value::F64(1.5)),
                ("f", Value::Bytes(Bytes::from_static(b"x"))),
                ("g", Value::str("y")),
                ("h", Value::Timestamp(1)),
                ("i", Value::Array(vec![])),
                ("j", Value::Map(Box::new(AttrMap::new()))),
            ]),
        ));
        assert_eq!(counter(&event, "logit.shape.values.null"), Some(1.0));
        assert_eq!(counter(&event, "logit.shape.values.bool"), Some(1.0));
        assert_eq!(counter(&event, "logit.shape.values.int"), Some(2.0), "I64 and U64 share int");
        assert_eq!(counter(&event, "logit.shape.values.float"), Some(1.0));
        assert_eq!(counter(&event, "logit.shape.values.bytes"), Some(1.0));
        assert_eq!(counter(&event, "logit.shape.values.string"), Some(1.0));
        assert_eq!(counter(&event, "logit.shape.values.timestamp"), Some(1.0));
        assert_eq!(counter(&event, "logit.shape.values.array"), Some(1.0));
        assert_eq!(counter(&event, "logit.shape.values.map"), Some(1.0));
    }

    #[test]
    fn a_log_body_reports_its_length_only() {
        let event = measure(Event::log(0, AttrMap::new(), log_record("hello world")));
        assert_eq!(one(&event, "logit.shape.body_bytes"), 11.0);
        assert_eq!(tag(&event, "signal").as_deref(), Some("log"));
    }

    #[test]
    fn samples_per_metric_counts_raw_observations_per_samples_record() {
        let mut event = Event::empty(0, AttrMap::new());
        event.metrics.push(MetricRecord::new(
            intern("timer.a"),
            MetricKind::Samples(Samples::new([1.0, 2.0, 3.0])),
        ));
        event
            .metrics
            .push(MetricRecord::new(intern("timer.b"), MetricKind::Samples(Samples::new([4.0]))));
        event.metrics.push(MetricRecord::new(intern("counter.c"), MetricKind::counter(1.0)));

        let event = measure(event);
        assert_eq!(one(&event, "logit.shape.metrics"), 3.0);
        let mut per = samples(&event, "logit.shape.samples_per_metric").unwrap().to_vec();
        per.sort_by(f64::total_cmp);
        assert_eq!(per, vec![1.0, 3.0], "only the two Samples records contribute");
        assert_eq!(tag(&event, "signal").as_deref(), Some("metric"));
    }

    #[test]
    fn a_span_reports_its_events_links_and_per_event_attribute_counts() {
        let event = measure(Event::span(0, AttrMap::new(), span_record(2, 1, 3)));
        assert_eq!(one(&event, "logit.shape.span_events"), 2.0);
        assert_eq!(one(&event, "logit.shape.span_links"), 1.0);
        assert_eq!(samples(&event, "logit.shape.span_event_attributes").unwrap(), &[3.0, 3.0]);
        assert_eq!(tag(&event, "signal").as_deref(), Some("span"));
    }

    #[test]
    fn a_span_with_no_events_emits_no_per_event_record() {
        let event = measure(Event::span(0, AttrMap::new(), span_record(0, 0, 0)));
        assert_eq!(one(&event, "logit.shape.span_events"), 0.0);
        assert_eq!(one(&event, "logit.shape.span_links"), 0.0);
        assert_eq!(samples(&event, "logit.shape.span_event_attributes"), None);
    }

    #[test]
    fn signal_names_cover_every_combination_in_a_fixed_order() {
        let combos: [(bool, bool, bool, &str); 8] = [
            (false, false, false, "none"),
            (true, false, false, "log"),
            (false, true, false, "metric"),
            (true, true, false, "log+metric"),
            (false, false, true, "span"),
            (true, false, true, "log+span"),
            (false, true, true, "metric+span"),
            (true, true, true, "log+metric+span"),
        ];
        for (log, metric, span, expected) in combos {
            let mut event = Event::empty(0, AttrMap::new());
            if log {
                event.log = Some(log_record("x"));
            }
            if metric {
                event.metrics.push(MetricRecord::new(intern("m"), MetricKind::counter(1.0)));
            }
            if span {
                event.span = Some(span_record(0, 0, 0));
            }
            let event = measure(event);
            assert_eq!(tag(&event, "signal").as_deref(), Some(expected));
        }
    }

    #[test]
    fn source_and_tap_tag_the_measurement_event() {
        let mut shape = Shape::new(Duration::from_secs(10)).with_name("tap_a");
        shape.observe_provenance(Provenance {
            origin: Some(intern("nginx_in")),
            previous: Some(intern("parse_json")),
        });
        let mut event = nginx_shaped_event();
        assert!(shape.process(&resource(), &mut event));
        assert_eq!(tag(&event, "source").as_deref(), Some("nginx_in"), "origin, not previous");
        assert_eq!(tag(&event, "tap").as_deref(), Some("tap_a"));
    }

    #[test]
    fn an_absent_provenance_origin_omits_the_source_tag() {
        let mut shape = Shape::new(Duration::from_secs(10));
        shape.observe_provenance(Provenance::default());
        let mut event = Event::empty(0, AttrMap::new());
        assert!(shape.process(&resource(), &mut event));
        assert_eq!(tag(&event, "source"), None);
        assert_eq!(tag(&event, "tap"), None);
    }

    // -- resource and scope ---------------------------------------------------------------------

    #[test]
    fn resource_drop_substitutes_one_cached_empty_resource() {
        let mut shape = Shape::new(Duration::from_secs(10));
        let incoming = Arc::new(Resource {
            attributes: attrs(&[("service.name", Value::str("checkout"))]),
            ..Resource::default()
        });
        let first = shape.map_resource(&incoming).expect("drop substitutes");
        assert!(first.attributes.is_empty(), "no resource attribute may flow out");
        let second = shape.map_resource(&incoming).expect("drop substitutes");
        assert!(Arc::ptr_eq(&first, &second), "the empty resource is cached, not rebuilt");
    }

    #[test]
    fn resource_keep_forwards_the_incoming_resource_unchanged() {
        let mut shape = Shape::new(Duration::from_secs(10)).with_resource_kept(true);
        let incoming = Arc::new(Resource {
            attributes: attrs(&[("service.name", Value::str("checkout"))]),
            ..Resource::default()
        });
        assert!(
            shape.map_resource(&incoming).is_none(),
            "None is how a transform says 'forward the batch's own resource'"
        );
    }

    // -- per-batch accumulation and flush --------------------------------------------------------

    /// Drives one batch through the hooks in `logit_pipeline::runtime::process_batch`'s order.
    fn run_batch(
        shape: &mut Shape,
        origin: Option<&str>,
        resource: &Arc<Resource>,
        scope: Option<Arc<Scope>>,
        events: Vec<Event>,
    ) {
        shape.observe_provenance(Provenance {
            origin: origin.map(intern),
            previous: origin.map(intern),
        });
        shape.observe_scope(scope);
        let mapped = shape.map_resource(resource).unwrap_or_else(|| resource.clone());
        for mut event in events {
            assert!(shape.process(&mapped, &mut event));
        }
        shape.end_batch();
    }

    fn flush_events(shape: &mut Shape, now: i64) -> Vec<Event> {
        let out = shape.flush(now);
        assert_eq!(out.len(), 1, "one (resource, scope) group");
        let (resource, scope, events) = out.into_iter().next().expect("one group");
        assert!(resource.attributes.is_empty(), "flush output carries no resource attributes");
        assert!(scope.is_none(), "flush output carries no scope");
        events
            .into_iter()
            .map(|(event, links)| {
                assert!(links.is_empty(), "shape attributes nothing across batches");
                event
            })
            .collect()
    }

    #[test]
    fn two_batches_become_two_values_on_each_per_batch_metric() {
        let mut shape = Shape::new(Duration::from_secs(10)).with_name("tap_a");
        let resource = Arc::new(Resource {
            attributes: attrs(&[("service.name", Value::str("a")), ("host", Value::str("b"))]),
            ..Resource::default()
        });
        let scope =
            Arc::new(Scope { attributes: attrs(&[("sdk", Value::str("x"))]), ..Scope::default() });

        run_batch(
            &mut shape,
            Some("nginx_in"),
            &resource,
            Some(scope),
            vec![nginx_shaped_event(), nginx_shaped_event(), wide_json_event()],
        );
        run_batch(&mut shape, Some("nginx_in"), &resource, None, vec![nginx_shaped_event()]);

        let events = flush_events(&mut shape, 100);
        let batch = events
            .iter()
            .find(|e| samples(e, "logit.shape.batch.events").is_some())
            .expect("one per-batch event for the single source");
        assert_eq!(batch.timestamp, 100);
        assert_eq!(samples(batch, "logit.shape.batch.events").unwrap(), &[3.0, 1.0]);
        assert_eq!(samples(batch, "logit.shape.batch.resource_attributes").unwrap(), &[2.0, 2.0]);
        assert_eq!(
            samples(batch, "logit.shape.batch.scope_attributes").unwrap(),
            &[1.0, 0.0],
            "a batch with no scope reports zero, not nothing"
        );
        assert_eq!(
            samples(batch, "logit.shape.batch.keysets").unwrap(),
            &[2.0, 1.0],
            "the nginx pair share a key-set; the wide-JSON event is a second one"
        );
        assert_eq!(tag(batch, "source").as_deref(), Some("nginx_in"));
        assert_eq!(tag(batch, "tap").as_deref(), Some("tap_a"));
        assert_eq!(tag(batch, "signal"), None, "a flush emission names no single signal");
    }

    #[test]
    fn two_sources_get_one_per_batch_event_each() {
        let mut shape = Shape::new(Duration::from_secs(10));
        let resource = resource();
        run_batch(&mut shape, Some("in_a"), &resource, None, vec![nginx_shaped_event()]);
        run_batch(&mut shape, Some("in_b"), &resource, None, vec![wide_json_event()]);

        let events = flush_events(&mut shape, 1);
        let per_batch: Vec<&Event> =
            events.iter().filter(|e| samples(e, "logit.shape.batch.events").is_some()).collect();
        assert_eq!(per_batch.len(), 2);
        let mut sources: Vec<String> = per_batch.iter().filter_map(|e| tag(e, "source")).collect();
        sources.sort();
        assert_eq!(sources, vec!["in_a".to_string(), "in_b".to_string()]);
    }

    #[test]
    fn a_window_is_emptied_by_its_flush() {
        let mut shape = Shape::new(Duration::from_secs(10));
        let resource = resource();
        run_batch(&mut shape, Some("in"), &resource, None, vec![nginx_shaped_event()]);
        assert_eq!(flush_events(&mut shape, 1).len(), 2, "one per-batch event + the gauges");
        assert_eq!(
            flush_events(&mut shape, 2).len(),
            1,
            "a window with no batches emits only the cumulative gauges"
        );
    }

    #[test]
    fn the_cumulative_gauges_count_distinct_keys_and_keysets() {
        let mut shape = Shape::new(Duration::from_secs(10));
        let resource = resource();
        // Two events sharing one key-set, one event with a different one.
        run_batch(
            &mut shape,
            Some("in"),
            &resource,
            None,
            vec![nginx_shaped_event(), nginx_shaped_event(), wide_json_event()],
        );
        let events = flush_events(&mut shape, 1);
        let gauges = events
            .iter()
            .find(|e| gauge(e, "logit.shape.distinct_keys").is_some())
            .expect("the cumulative event");

        assert_eq!(gauge(gauges, "logit.shape.distinct_keys"), Some(42.0), "10 nginx + 32 wide");
        assert_eq!(gauge(gauges, "logit.shape.distinct_keysets"), Some(2.0));
        assert_eq!(gauge(gauges, "logit.shape.keyset_share.top1"), Some(2.0 / 3.0));
        assert_eq!(gauge(gauges, "logit.shape.keyset_share.top5"), Some(1.0));
        assert_eq!(gauge(gauges, "logit.shape.tracking_overflow"), Some(0.0));
    }

    #[test]
    fn the_cumulative_gauges_survive_a_flush() {
        let mut shape = Shape::new(Duration::from_secs(10));
        let resource = resource();
        run_batch(&mut shape, Some("in"), &resource, None, vec![nginx_shaped_event()]);
        let first = flush_events(&mut shape, 1);
        let first = first.iter().find(|e| gauge(e, "logit.shape.distinct_keys").is_some()).unwrap();
        assert_eq!(gauge(first, "logit.shape.distinct_keys"), Some(10.0));

        let second = flush_events(&mut shape, 2);
        let second =
            second.iter().find(|e| gauge(e, "logit.shape.distinct_keys").is_some()).unwrap();
        assert_eq!(
            gauge(second, "logit.shape.distinct_keys"),
            Some(10.0),
            "cumulative since start, not per window"
        );
    }

    #[test]
    fn a_flush_with_nothing_seen_still_reports_zeroed_gauges() {
        let mut shape = Shape::new(Duration::from_secs(10));
        let events = flush_events(&mut shape, 1);
        assert_eq!(events.len(), 1);
        assert_eq!(gauge(&events[0], "logit.shape.distinct_keys"), Some(0.0));
        assert_eq!(gauge(&events[0], "logit.shape.keyset_share.top1"), Some(0.0));
        assert_eq!(gauge(&events[0], "logit.shape.tracking_overflow"), Some(0.0));
    }

    // -- caps -------------------------------------------------------------------------------------

    fn telemetry_counter(points: &[Event], name: &str) -> Option<f64> {
        points.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(sum) if resolve(m.name) == name => Some(sum.value),
                _ => None,
            })
        })
    }

    #[test]
    fn the_key_cap_stops_tracking_and_raises_overflow() {
        let registry = Registry::new();
        let mut shape = Shape::new(Duration::from_secs(10))
            .with_caps(3, 1024)
            .with_telemetry(registry.telemetry_for("tap", "shape", "transform"));
        let resource = resource();
        let events: Vec<Event> = (0..10)
            .map(|i| Event::empty(0, attrs(&[(&format!("cap_key_{i}"), Value::U64(1))])))
            .collect();
        run_batch(&mut shape, Some("in"), &resource, None, events);

        let flushed = flush_events(&mut shape, 1);
        let gauges =
            flushed.iter().find(|e| gauge(e, "logit.shape.distinct_keys").is_some()).unwrap();
        assert_eq!(gauge(gauges, "logit.shape.distinct_keys"), Some(3.0), "capped at 3");
        assert_eq!(gauge(gauges, "logit.shape.tracking_overflow"), Some(1.0));

        let points = registry.drain(0);
        assert_eq!(
            telemetry_counter(&points, "logit.transform.keys.untracked"),
            Some(7.0),
            "the seven keys past the cap are counted, not silent"
        );
    }

    #[test]
    fn the_keyset_cap_keeps_counting_already_tracked_keysets() {
        let mut shape = Shape::new(Duration::from_secs(10)).with_caps(1024, 1);
        let resource = resource();
        let events = vec![nginx_shaped_event(), nginx_shaped_event(), wide_json_event()];
        run_batch(&mut shape, Some("in"), &resource, None, events);

        let flushed = flush_events(&mut shape, 1);
        let gauges =
            flushed.iter().find(|e| gauge(e, "logit.shape.distinct_keysets").is_some()).unwrap();
        assert_eq!(gauge(gauges, "logit.shape.distinct_keysets"), Some(1.0));
        assert_eq!(gauge(gauges, "logit.shape.tracking_overflow"), Some(1.0));
        assert_eq!(
            gauge(gauges, "logit.shape.keyset_share.top1"),
            Some(2.0 / 3.0),
            "the tracked key-set still counted both of its events"
        );
    }

    #[test]
    fn a_flush_window_drops_and_counts_past_the_batch_cap() {
        let registry = Registry::new();
        let mut shape = Shape::new(Duration::from_secs(10)).with_telemetry(registry.telemetry_for(
            "tap",
            "shape",
            "transform",
        ));
        let resource = resource();
        for _ in 0..(MAX_BATCHES_PER_WINDOW + 3) {
            run_batch(
                &mut shape,
                Some("in"),
                &resource,
                None,
                vec![Event::empty(0, AttrMap::new())],
            );
        }
        let flushed = flush_events(&mut shape, 1);
        let batch =
            flushed.iter().find(|e| samples(e, "logit.shape.batch.events").is_some()).unwrap();
        assert_eq!(
            samples(batch, "logit.shape.batch.events").unwrap().len(),
            MAX_BATCHES_PER_WINDOW
        );

        let points = registry.drain(0);
        assert_eq!(telemetry_counter(&points, "logit.transform.batches.dropped"), Some(3.0));
    }

    // -- the counts-only property --------------------------------------------------------------

    /// No observed key, value, body, metric name, span name, or resource attribute appears in
    /// any metric name, tag key, or tag value `shape` emits, per-event or on flush.
    #[test]
    fn no_observed_key_or_value_appears_anywhere_in_the_output() {
        const SECRETS: [&str; 6] = [
            "zzz_observed_key",
            "zzz_observed_value",
            "zzz_observed_body",
            "zzz_observed_metric",
            "zzz_observed_span",
            "zzz_observed_resource",
        ];

        let mut shape = Shape::new(Duration::from_secs(10)).with_name("tap_a");
        let resource = Arc::new(Resource {
            attributes: attrs(&[("env", Value::str("zzz_observed_resource"))]),
            ..Resource::default()
        });

        let mut event = Event::log(
            0,
            attrs(&[
                ("zzz_observed_key", Value::str("zzz_observed_value")),
                (
                    "nested",
                    Value::Map(Box::new(attrs(&[(
                        "zzz_observed_key",
                        Value::str("zzz_observed_value"),
                    )]))),
                ),
            ]),
            log_record("zzz_observed_body"),
        );
        event.metrics.push(MetricRecord::new(
            intern("zzz_observed_metric"),
            MetricKind::Samples(Samples::new([1.0])),
        ));
        let mut span = span_record(1, 1, 1);
        span.name = Value::str("zzz_observed_span");
        event.span = Some(span);

        shape.observe_provenance(Provenance { origin: Some(intern("statsd_in")), previous: None });
        shape.observe_scope(None);
        let mapped = shape.map_resource(&resource).expect("drop is the default");
        assert!(shape.process(&mapped, &mut event));
        shape.end_batch();

        let mut rendered = vec![render(&event)];
        rendered.extend(flush_events(&mut shape, 1).iter().map(render));
        rendered.push(format!("{mapped:?}"));

        for text in &rendered {
            for secret in SECRETS {
                assert!(
                    !text.contains(secret),
                    "shape leaked {secret:?} into its own output: {text}"
                );
            }
        }
    }

    /// Every metric name, unit, tag key, and tag value a measurement event carries.
    fn render(event: &Event) -> String {
        let mut out = String::new();
        for m in event.metrics.iter() {
            out.push_str(resolve(m.name));
            out.push(' ');
            if let Some(unit) = m.unit {
                out.push_str(resolve(unit));
                out.push(' ');
            }
        }
        for (key, value) in event.attributes.iter() {
            out.push_str(resolve(key));
            out.push('=');
            out.push_str(&format!("{value:?}"));
            out.push(' ');
        }
        out
    }

    #[test]
    fn key_set_identity_ignores_values_and_insertion_order() {
        let a = attrs(&[("x", Value::str("1")), ("y", Value::str("2"))]);
        let mut b = AttrMap::new();
        b.insert("y", Value::U64(9));
        b.insert("x", Value::Null);
        assert_eq!(keyset_hash(&a), keyset_hash(&b));

        let c = attrs(&[("x", Value::str("1"))]);
        assert_ne!(keyset_hash(&a), keyset_hash(&c));
    }
}
