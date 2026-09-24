//! `generate_in`: a synthetic event source, the listener end of the load-test harness
//! (`docs/adr/load-test-harness.md`). No socket and no decoder: it renders a declarative event
//! template as fast as `count`/`rate` allow, so a scenario measures the runtime and the
//! components under test, not a load generator and a kernel socket buffer.
//!
//! ```yaml
//! gen:
//!   type: generate_in
//!   count: 2000000        # total events; omit = unbounded (soak, or a profiler attach)
//!   batch: 100            # events per batch (default 100); the last batch is count % batch
//!   rate: 50000           # events/s; omit = unthrottled (backpressure only)
//!   resource: { service.name: web }
//!   event:
//!     log: '{{"method":"GET","path":"/x/{seq%50}","status":200}}'
//!     attributes: { host: "web-{seq%10}" }
//!     metric: { name: requests, kind: sum, value: 1 }   # sum | gauge | distribution
//! ```
//!
//! `count` is **exact**: the last batch is short (`count % batch`), not rounded up, so a harness
//! deriving events/s and CPU per event divides by the number produced. Once `count` is reached,
//! [`Input::run`] returns `Ok(())` and drops its `Fanout`, and the runtime's listener-exit cascade
//! flushes downstream and ends the process. Without `count`, generation never ends on its own.
//!
//! # Placeholders and the two render paths
//!
//! A template field may use `{seq}` (the run's 0-based event counter, continuing across batches)
//! or `{seq%N}` (that counter modulo `N >= 1`, the cardinality knob); `{{`/`}}` escape a brace
//! (`logit_core::template`). Placeholders cost a render and a copy per event, and whether any
//! field has one picks the path:
//!
//! - **Prototype** (no placeholder anywhere): one [`Event`] is rendered on first use and cloned
//!   per event with only `timestamp` overwritten. Every field's bytes are one shared refcounted
//!   buffer: a refcount bump per event, not a copy.
//! - **Per event** (any placeholder): each templated field is rendered into a reused `String` and
//!   copied out with one `Bytes::copy_from_slice`, one allocation per templated field per event.
//!   Literal fields still clone their construction-time bytes. A templated metric name is instead
//!   interned: `interner::intern`'s cost per event, and one never-freed interner entry per
//!   distinct rendering (`docs/design/memory.md` §4). So a metric name may use `{seq%N}` but not
//!   a bare `{seq}`, rejected here and by graph rule 42.
//!
//! `now_nanos()` is read once per batch, not per event: every event in a batch shares a
//! timestamp.
//!
//! The **resource** is batch-level: an all-literal one is built once and `Arc`-shared by every
//! batch, a templated one is rebuilt per batch. **In resource position `seq` is the batch
//! ordinal**, not the event counter; see [`GenerateInput::with_resource`].
//!
//! # Rate pacing
//!
//! `rate` is events per second, held against the wall clock: before each batch this input sleeps
//! until `start + sent / rate`, recomputed from the run's start, so a late batch doesn't shift
//! later deadlines and the average rate doesn't drift. The first batch goes out immediately.
//! Above roughly 1k batches/s pacing is accurate on average but bursty within a millisecond
//! (`docs/known-gaps.md`). Without `rate`, downstream backpressure is the only limit.
//!
//! # Telemetry
//!
//! **Layer 2 only**: no points of its own, since the runtime's `logit.component.events.sent`
//! already is the generated count (`docs/design/internal-telemetry.md`). One `Diagnostics` key,
//! mirrored as `logit.component.diagnostics{key}`: `rate_behind`, a generator 1s or more behind
//! its `rate` schedule, meaning a rate-limited scenario has become a throughput one.

use crate::Input;
use bytes::Bytes;
use logit_core::interner::{intern, Symbol};
use logit_core::template::{Compiled, Template};
use logit_core::{
    AttrMap, BodyFormat, Diagnostics, Event, EventBatch, LogRecord, MetricKind, MetricList,
    MetricRecord, Resource, Samples, Telemetry, Value,
};
use logit_pipeline::Fanout;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

/// A resolved placeholder: the `V` in [`Template::compile`], so the hot path does no string
/// matching per event.
///
/// Must stay the same set graph rule 42 accepts (`logit_pipeline::graph`'s
/// `generate_var_is_valid`); the two can't share code across the crate layout
/// (`docs/design/pipeline-graph.md`), so they stay small enough to compare by eye.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenVar {
    /// `{seq}` -- the generator's 0-based event counter.
    Seq,
    /// `{seq%N}` -- that counter modulo `N`, a scenario's cardinality knob. `N >= 1`.
    SeqMod(u64),
}

/// Which `logit_core::MetricKind` a generated metric carries.
///
/// A local mirror of `logit_config::GenerateMetricKind`, since `logit-inputs` must not depend on
/// `logit-config` (`docs/design/pipeline-graph.md`'s crate layout); `logit-cli`'s
/// `generate_metric_kind` converts between them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GenerateMetricKind {
    /// A delta, monotonic `Sum` (`MetricKind::counter`).
    #[default]
    Sum,
    Gauge,
    /// Raw `Samples` of the one configured value, never a pre-built `DdSketch`: summarization is
    /// `aggregate`'s job (`docs/adr/lossless-transit.md`), and a scenario measuring sketch merges
    /// needs them to happen there.
    Distribution,
}

/// Resolves one placeholder name, rejecting anything `generate_in` doesn't substitute.
///
/// Mirrors `logit_pipeline::graph`'s `generate_var_is_valid`, including its digits-only modulus:
/// `u64::from_str` accepts a leading `+`, which would make `{seq%+5}` a second spelling of
/// `{seq%5}`. An error, not a panic: a direct caller (`logit-bench`, a test) has no rule 42 in
/// front of it.
fn resolve_var(name: &str) -> anyhow::Result<GenVar> {
    if name == "seq" {
        return Ok(GenVar::Seq);
    }
    if let Some(modulus) = name.strip_prefix("seq%") {
        if !modulus.is_empty() && modulus.bytes().all(|byte| byte.is_ascii_digit()) {
            if let Ok(modulus) = modulus.parse::<u64>() {
                if modulus >= 1 {
                    return Ok(GenVar::SeqMod(modulus));
                }
            }
        }
    }
    anyhow::bail!(
        "generate_in: '{{{name}}}' is not a placeholder generate_in substitutes -- only '{{seq}}' \
         and '{{seq%N}}' (N written as digits, at least 1)"
    )
}

/// [`resolve_var`] without bare `{seq}`, for `event.metric.name`, whose rendering is interned.
///
/// Interning never frees (`docs/design/memory.md` §4), so a bare `{seq}` would leak one metric
/// name per generated event; `{seq%N}` is bounded by `N`. Graph rule 42 rejects the same.
fn resolve_metric_name_var(name: &str) -> anyhow::Result<GenVar> {
    match resolve_var(name)? {
        GenVar::SeqMod(modulus) => Ok(GenVar::SeqMod(modulus)),
        GenVar::Seq => anyhow::bail!(
            "generate_in: a metric name may not use '{{seq}}' -- a metric name is interned for \
             the life of the process, so an unbounded one would intern a fresh name per event. \
             Use '{{seq%N}}' to generate a bounded set of N names"
        ),
    }
}

/// Appends one placeholder's rendering. `write!` into a `String` is infallible.
fn write_var(var: GenVar, seq: u64, out: &mut String) {
    let value = match var {
        GenVar::Seq => seq,
        GenVar::SeqMod(modulus) => seq % modulus,
    };
    let _ = write!(out, "{value}");
}

/// One configured string field, in the form the render path wants it.
#[derive(Debug, Clone)]
enum Field {
    /// No placeholder: rendered once at construction; each event clones these bytes (a refcount
    /// bump).
    Literal(Bytes),
    /// At least one placeholder: rendered per event.
    Templated(Compiled<GenVar>),
}

impl Field {
    fn new(template: &Template) -> anyhow::Result<Self> {
        match template.literal() {
            Some(bytes) => Ok(Field::Literal(bytes.clone())),
            None => Ok(Field::Templated(template.compile(resolve_var)?)),
        }
    }

    fn is_literal(&self) -> bool {
        matches!(self, Field::Literal(_))
    }

    /// This field's bytes for event `seq`: a refcount bump for a literal, one render into
    /// `scratch` plus one `Bytes::copy_from_slice` (one allocation) for a template.
    fn render(&self, seq: u64, scratch: &mut String) -> Bytes {
        match self {
            Field::Literal(bytes) => bytes.clone(),
            Field::Templated(compiled) => {
                scratch.clear();
                compiled.render(scratch, |var, out| write_var(*var, seq, out));
                Bytes::copy_from_slice(scratch.as_bytes())
            }
        }
    }
}

/// A generated metric's name: interned, not copied, so separate from [`Field`]. A literal name is
/// interned once, at construction.
#[derive(Debug, Clone)]
enum MetricName {
    Fixed(Symbol),
    Templated(Compiled<GenVar>),
}

/// The metric stamped on every generated event, when one is configured at all.
#[derive(Debug, Clone)]
struct MetricSpec {
    name: MetricName,
    kind: GenerateMetricKind,
    /// Constant, so a scenario measures the pipeline rather than the generator's arithmetic.
    value: f64,
}

impl MetricSpec {
    fn is_literal(&self) -> bool {
        matches!(self.name, MetricName::Fixed(_))
    }

    fn symbol(&self, seq: u64, scratch: &mut String) -> Symbol {
        match &self.name {
            MetricName::Fixed(symbol) => *symbol,
            MetricName::Templated(compiled) => {
                scratch.clear();
                compiled.render(scratch, |var, out| write_var(*var, seq, out));
                intern(scratch)
            }
        }
    }

    fn metric_kind(&self) -> MetricKind {
        match self.kind {
            GenerateMetricKind::Sum => MetricKind::counter(self.value),
            GenerateMetricKind::Gauge => MetricKind::Gauge(self.value),
            GenerateMetricKind::Distribution => MetricKind::Samples(Samples::new([self.value])),
        }
    }
}

/// How a batch gets its [`Resource`]; independent of [`RenderPath`], so a templated resource over
/// an all-literal event template still takes the prototype path for its events.
#[derive(Debug, Clone)]
enum ResourceSpec {
    /// Every value literal: built once, and every batch shares this `Arc`.
    Fixed(Arc<Resource>),
    /// Any placeholder: rebuilt per batch from the batch ordinal, one `AttrMap` and one
    /// `Arc<Resource>` per batch ([`GenerateInput::with_resource`]).
    Templated(Vec<(Symbol, Field)>),
}

/// Which render path applies. Settled on first use, not in [`GenerateInput::new`], since a
/// `with_*` builder may still add a templated field.
#[derive(Debug, Clone)]
enum RenderPath {
    Undecided,
    /// Every field literal: this event is cloned per generated event, `timestamp` overwritten.
    /// `Box`ed to satisfy clippy's `large_enum_variant`; the box is one allocation per process,
    /// and the per-event clone is of the `Event`, not the `Box`.
    Prototype(Box<Event>),
    /// Something is templated: every event is rendered field by field.
    PerEvent,
}

/// A synthetic listener: renders `count` events from a template, optionally paced to `rate`.
#[derive(Debug)]
pub struct GenerateInput {
    /// `None` means unbounded -- a soak run, or one a profiler attaches to.
    count: Option<u64>,
    /// At least 1 ([`GenerateInput::new`] clamps it): a direct caller has no rule 42, and a
    /// zero-sized batch would loop forever generating nothing.
    batch: usize,
    /// Events per second; `None` means unthrottled, limited only by downstream backpressure.
    rate: Option<u64>,
    log: Option<Field>,
    /// Keys interned once. A `Vec`, not a map: the keys are already distinct and it's only
    /// iterated.
    attributes: Vec<(Symbol, Field)>,
    metric: Option<MetricSpec>,
    resource: ResourceSpec,
    path: RenderPath,
    /// Reused render buffer; allocates nothing once grown to the widest rendering.
    scratch: String,
    diag: Diagnostics,
}

impl GenerateInput {
    /// A generator with no payload: a timestamp and an empty resource, a "runtime floor"
    /// scenario. `count: None` never ends on its own.
    pub fn new(count: Option<u64>, batch: usize) -> Self {
        Self {
            count,
            batch: batch.max(1),
            rate: None,
            log: None,
            attributes: Vec::new(),
            metric: None,
            resource: ResourceSpec::Fixed(Arc::new(Resource::default())),
            path: RenderPath::Undecided,
            scratch: String::new(),
            diag: Diagnostics::default(),
        }
    }

    /// Target events per second. `None` (the default) is unthrottled.
    ///
    /// `Some(0)` becomes `None`: a direct caller has no rule 42, and a zero rate would make
    /// [`GenerateInput::pace`]'s `sent / rate` infinite, which `Duration::from_secs_f64` panics
    /// on.
    pub fn with_rate(mut self, rate: Option<u64>) -> Self {
        self.rate = rate.filter(|rate| *rate > 0);
        self
    }

    /// The log body every generated event carries. Without it, events carry no `LogRecord`.
    pub fn with_log(mut self, template: Template) -> anyhow::Result<Self> {
        self.log = Some(Field::new(&template)?);
        self.path = RenderPath::Undecided;
        Ok(self)
    }

    /// One event attribute: a literal key (interned once) and a templated value.
    pub fn with_attribute(mut self, key: &str, template: Template) -> anyhow::Result<Self> {
        self.attributes.push((intern(key), Field::new(&template)?));
        self.path = RenderPath::Undecided;
        Ok(self)
    }

    /// The metric stamped on every generated event. Without it, events carry no metrics.
    ///
    /// `name` may use `{seq%N}` but not a bare `{seq}` ([`resolve_metric_name_var`]).
    pub fn with_metric(
        mut self,
        name: Template,
        kind: GenerateMetricKind,
        value: f64,
    ) -> anyhow::Result<Self> {
        let name = match name.literal() {
            Some(bytes) => MetricName::Fixed(intern(&String::from_utf8_lossy(bytes))),
            None => MetricName::Templated(name.compile(resolve_metric_name_var)?),
        };
        self.metric = Some(MetricSpec { name, kind, value });
        self.path = RenderPath::Undecided;
        Ok(self)
    }

    /// The resource every batch carries. Values may use placeholders, rendered once per batch,
    /// since the resource is batch-level (`logit_core::EventBatch::resource`).
    ///
    /// **In resource position `seq` is the batch ordinal** (0, 1, 2, ...), not the event counter.
    /// The event counter advances by `batch` per batch, so `{seq%N}` over it yields only
    /// `N / gcd(N, batch)` values: one, for `batch: 100` and `{seq%10}`. From the ordinal,
    /// `resource: { host: "h{seq%10}" }` is ten resources cycling per batch.
    ///
    /// That costs one `AttrMap` and one `Arc<Resource>` per batch, nothing per event. Every event
    /// in a batch shares one resource. An all-literal resource is built once and shared by every
    /// batch ([`ResourceSpec`]).
    ///
    /// Takes raw strings, unlike the other builders: the parse is startup-only either way.
    pub fn with_resource(mut self, resource: BTreeMap<String, String>) -> anyhow::Result<Self> {
        let mut fields = Vec::with_capacity(resource.len());
        for (key, value) in &resource {
            let template = logit_core::template::parse(value).map_err(|err| {
                anyhow::anyhow!("generate_in: resource {key:?} is not a template: {err}")
            })?;
            fields.push((intern(key), Field::new(&template)?));
        }
        self.resource = if fields.iter().all(|(_, field)| field.is_literal()) {
            let mut attributes = AttrMap::new();
            let mut scratch = String::new();
            for (key, field) in &fields {
                attributes.insert_sym(*key, Value::Str(field.render(0, &mut scratch)));
            }
            ResourceSpec::Fixed(Arc::new(Resource { attributes, ..Default::default() }))
        } else {
            ResourceSpec::Templated(fields)
        };
        Ok(self)
    }

    /// Attaches this component's telemetry handle, used only to bridge `rate_behind` into
    /// `logit.component.diagnostics{key}` (module doc's "Telemetry").
    ///
    /// Call this *after* [`GenerateInput::with_diagnostics`]: it attaches to whatever
    /// `Diagnostics` this input holds at the time.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.diag = self.diag.clone().with_telemetry(telemetry);
        self
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    /// Builds one batch of `n` events numbered `first_seq..first_seq + n`, all stamped `now`
    /// (Unix nanoseconds), under batch ordinal `batch_index` (0 for a run's first batch).
    ///
    /// `batch_index` is what a resource template renders from. It's an argument rather than
    /// `first_seq / batch`, which would be wrong for a caller not stepping by exact multiples of
    /// `batch`, as `logit-bench` and this module's tests do.
    ///
    /// Public so `logit-bench` can drive both render paths with no runtime in between
    /// (`docs/design/memory.md`'s "Fixtures" section). The first call settles the render path and
    /// renders any prototype, so a caller counting allocations must warm it first.
    pub fn build_batch(
        &mut self,
        batch_index: u64,
        first_seq: u64,
        n: usize,
        now: i64,
    ) -> EventBatch {
        self.settle_render_path();
        // Taken out and put back so the renders can borrow it mutably while borrowing `self`,
        // keeping its grown capacity across batches.
        let mut scratch = std::mem::take(&mut self.scratch);
        let resource = self.render_resource(batch_index, &mut scratch);
        let mut events = Vec::with_capacity(n);
        if let RenderPath::Prototype(prototype) = &self.path {
            for _ in 0..n {
                // `(**prototype)`, not `prototype.clone()`, which would also allocate a `Box`
                // per event.
                let mut event = (**prototype).clone();
                event.timestamp = now;
                events.push(event);
            }
        } else {
            for seq in first_seq..first_seq + n as u64 {
                events.push(self.render_one(seq, now, &mut scratch));
            }
        }
        self.scratch = scratch;
        EventBatch { resource, scope: None, events }
    }

    /// The shared `Arc` for an all-literal resource, else a fresh one rendered from the batch
    /// ordinal ([`GenerateInput::with_resource`]).
    fn render_resource(&self, batch_index: u64, scratch: &mut String) -> Arc<Resource> {
        match &self.resource {
            ResourceSpec::Fixed(resource) => resource.clone(),
            ResourceSpec::Templated(fields) => {
                let mut attributes = AttrMap::new();
                for (key, field) in fields {
                    attributes.insert_sym(*key, Value::Str(field.render(batch_index, scratch)));
                }
                Arc::new(Resource { attributes, ..Default::default() })
            }
        }
    }

    fn settle_render_path(&mut self) {
        if !matches!(self.path, RenderPath::Undecided) {
            return;
        }
        if !self.is_all_literal() {
            self.path = RenderPath::PerEvent;
            return;
        }
        // An all-literal template reads neither `seq` nor the scratch.
        let prototype = self.render_one(0, 0, &mut String::new());
        self.path = RenderPath::Prototype(Box::new(prototype));
    }

    fn is_all_literal(&self) -> bool {
        self.log.as_ref().is_none_or(Field::is_literal)
            && self.attributes.iter().all(|(_, field)| field.is_literal())
            && self.metric.as_ref().is_none_or(MetricSpec::is_literal)
    }

    fn render_one(&self, seq: u64, now: i64, scratch: &mut String) -> Event {
        let mut attributes = AttrMap::new();
        for (key, field) in &self.attributes {
            attributes.insert_sym(*key, Value::Str(field.render(seq, scratch)));
        }
        let log = self.log.as_ref().map(|field| LogRecord {
            message: Value::Str(field.render(seq, scratch)),
            severity: None,
            // `Raw` even for a JSON body: parsing belongs to a downstream `json` stage.
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        });
        let metrics = match &self.metric {
            Some(spec) => MetricList::from_iter([MetricRecord::new(
                spec.symbol(seq, scratch),
                spec.metric_kind(),
            )]),
            None => MetricList::new(),
        };
        Event { timestamp: now, attributes, log, metrics, span: None }
    }

    /// Sleeps until `started + sent / rate` (module doc's "Rate pacing").
    async fn pace(&mut self, started: Instant, sent: u64, rate: u64) {
        let due_at = started + Duration::from_secs_f64(sent as f64 / rate as f64);
        let now = Instant::now();
        if now < due_at {
            tokio::time::sleep_until(due_at).await;
            return;
        }
        // Behind schedule: the rate-limited measurement has become a throughput one. Reported
        // only from 1s behind, so millisecond jitter stays quiet.
        let behind = now - due_at;
        if behind >= Duration::from_secs(1) {
            self.diag.warn_throttled(
                "rate_behind",
                format!(
                    "generating slower than the configured rate of {rate}/s -- {behind:?} behind \
                     after {sent} events"
                ),
            );
        }
    }
}

#[async_trait::async_trait]
impl Input for GenerateInput {
    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        let started = Instant::now();
        let mut sent: u64 = 0;
        let mut batches: u64 = 0;
        loop {
            // The last batch is short, not rounded up: `count` is exact.
            let n = match self.count {
                Some(count) if sent >= count => break,
                Some(count) => (count - sent).min(self.batch as u64) as usize,
                None => self.batch,
            };
            if let Some(rate) = self.rate {
                self.pace(started, sent, rate).await;
            }
            let batch = self.build_batch(batches, sent, n, now_nanos());
            sink.send(batch).await;
            sent += n as u64;
            batches += 1;
        }
        // `logit-perf` watches for this line: the end of generation (not of the process, which
        // waits on the downstream flush), carrying the count its events/s divides by.
        tracing::info!(
            target: "logit",
            events = sent,
            batches,
            elapsed = ?started.elapsed(),
            "generation complete"
        );
        Ok(())
    }
}

fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::template::parse;
    use logit_core::{interner, Sum, Temporality};
    use logit_pipeline::Delivered;
    use tokio::sync::mpsc;

    fn batch_of(delivered: Delivered) -> EventBatch {
        match delivered {
            Delivered::Owned(batch, _ctx) => batch,
            Delivered::Shared(shared, _ctx) => (*shared).clone(),
        }
    }

    /// Drains every batch a finished run sent, so a test can assert on the run's whole output.
    async fn run_to_completion(mut input: GenerateInput) -> Vec<EventBatch> {
        let (tx, mut rx) = mpsc::channel(1024);
        input.run(Fanout::new(vec![tx])).await.expect("a finite generator returns Ok");
        let mut batches = Vec::new();
        while let Ok(delivered) = rx.try_recv() {
            batches.push(batch_of(delivered));
        }
        batches
    }

    #[tokio::test]
    async fn a_finite_run_generates_exactly_count_events_and_returns_ok() {
        let batches = run_to_completion(GenerateInput::new(Some(300), 100)).await;
        assert_eq!(batches.len(), 3);
        assert_eq!(batches.iter().map(|b| b.events.len()).sum::<usize>(), 300);
    }

    /// `count` is exact, not rounded up to a whole batch.
    #[tokio::test]
    async fn a_count_that_does_not_divide_by_batch_ends_with_a_short_batch() {
        let batches = run_to_completion(GenerateInput::new(Some(250), 100)).await;
        let sizes: Vec<usize> = batches.iter().map(|b| b.events.len()).collect();
        assert_eq!(sizes, vec![100, 100, 50]);
    }

    #[tokio::test]
    async fn a_generator_with_no_event_template_still_stamps_a_timestamp() {
        let batches = run_to_completion(GenerateInput::new(Some(1), 100)).await;
        let event = &batches[0].events[0];
        assert!(event.timestamp > 0, "every generated event carries a real timestamp");
        assert!(event.log.is_none());
        assert!(event.metrics.is_empty());
        assert!(event.attributes.is_empty());
    }

    /// An all-literal template's events share one buffer (asserted by pointer, not equality).
    #[tokio::test]
    async fn literal_templates_share_one_message_buffer_across_events() {
        let input = GenerateInput::new(Some(2), 2)
            .with_log(parse("a fixed line").unwrap())
            .unwrap()
            .with_attribute("host", parse("web-1").unwrap())
            .unwrap();
        let batches = run_to_completion(input).await;

        let events = &batches[0].events;
        assert_eq!(events.len(), 2);
        let message_ptr = |event: &Event| match &event.log.as_ref().unwrap().message {
            Value::Str(bytes) => bytes.as_ptr(),
            other => panic!("expected a Str message, got {other:?}"),
        };
        assert_eq!(
            message_ptr(&events[0]),
            message_ptr(&events[1]),
            "an all-literal log body should be one shared buffer, not a copy per event"
        );
        let attribute_ptr = |event: &Event| match event.attributes.get("host").unwrap() {
            Value::Str(bytes) => bytes.as_ptr(),
            other => panic!("expected a Str attribute, got {other:?}"),
        };
        assert_eq!(attribute_ptr(&events[0]), attribute_ptr(&events[1]));
    }

    /// Prototype-path events get the batch's timestamp, not the prototype's.
    #[tokio::test]
    async fn the_prototype_path_overwrites_the_timestamp_per_batch() {
        let mut input = GenerateInput::new(Some(4), 2).with_log(parse("fixed").unwrap()).unwrap();
        let first = input.build_batch(0, 0, 2, 111);
        let second = input.build_batch(1, 2, 2, 222);
        assert!(first.events.iter().all(|event| event.timestamp == 111));
        assert!(second.events.iter().all(|event| event.timestamp == 222));
    }

    #[tokio::test]
    async fn seq_and_seq_modulo_render_per_event() {
        let input = GenerateInput::new(Some(4), 4)
            .with_log(parse("n={seq}").unwrap())
            .unwrap()
            .with_attribute("host", parse("web-{seq%2}").unwrap())
            .unwrap();
        let batches = run_to_completion(input).await;

        let messages: Vec<String> = batches[0]
            .events
            .iter()
            .map(|event| match &event.log.as_ref().unwrap().message {
                Value::Str(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                other => panic!("expected a Str message, got {other:?}"),
            })
            .collect();
        assert_eq!(messages, vec!["n=0", "n=1", "n=2", "n=3"]);

        let hosts: Vec<String> = batches[0]
            .events
            .iter()
            .map(|event| event.attributes.get("host").and_then(|v| v.as_str()).unwrap().to_string())
            .collect();
        assert_eq!(hosts, vec!["web-0", "web-1", "web-0", "web-1"]);
    }

    /// `seq` is the run's event counter, continuing across batches.
    #[tokio::test]
    async fn seq_continues_across_batch_boundaries() {
        let input = GenerateInput::new(Some(4), 2).with_log(parse("{seq}").unwrap()).unwrap();
        let batches = run_to_completion(input).await;
        let messages: Vec<String> = batches
            .iter()
            .flat_map(|batch| batch.events.iter())
            .map(|event| match &event.log.as_ref().unwrap().message {
                Value::Str(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                other => panic!("expected a Str message, got {other:?}"),
            })
            .collect();
        assert_eq!(messages, vec!["0", "1", "2", "3"]);
    }

    #[tokio::test]
    async fn a_generated_log_body_is_raw_not_pre_parsed_json() {
        let input =
            GenerateInput::new(Some(1), 1).with_log(parse(r#"{{"status":200}}"#).unwrap()).unwrap();
        let batches = run_to_completion(input).await;
        let log = batches[0].events[0].log.as_ref().unwrap();
        assert_eq!(log.body_format, BodyFormat::Raw);
        assert_eq!(log.message.as_str(), Some(r#"{"status":200}"#));
    }

    #[tokio::test]
    async fn a_sum_metric_is_a_delta_monotonic_counter() {
        let input = GenerateInput::new(Some(1), 1)
            .with_metric(parse("requests").unwrap(), GenerateMetricKind::Sum, 2.0)
            .unwrap();
        let batches = run_to_completion(input).await;
        let record = &batches[0].events[0].metrics[0];
        assert_eq!(interner::resolve(record.name), "requests");
        assert_eq!(
            record.kind,
            MetricKind::Sum(Sum { value: 2.0, temporality: Temporality::Delta, monotonic: true })
        );
    }

    #[tokio::test]
    async fn a_gauge_metric_is_a_plain_gauge() {
        let input = GenerateInput::new(Some(1), 1)
            .with_metric(parse("queue.depth").unwrap(), GenerateMetricKind::Gauge, 7.5)
            .unwrap();
        let batches = run_to_completion(input).await;
        assert_eq!(batches[0].events[0].metrics[0].kind, MetricKind::Gauge(7.5));
    }

    /// A distribution generates raw `Samples`, never a `DdSketch`.
    #[tokio::test]
    async fn a_distribution_metric_is_raw_samples_not_a_sketch() {
        let input = GenerateInput::new(Some(1), 1)
            .with_metric(parse("latency").unwrap(), GenerateMetricKind::Distribution, 12.0)
            .unwrap();
        let batches = run_to_completion(input).await;
        match &batches[0].events[0].metrics[0].kind {
            MetricKind::Samples(samples) => {
                assert_eq!(samples.values.as_slice(), &[12.0]);
                assert_eq!(samples.sample_rate, 1.0);
            }
            other => panic!("expected raw Samples, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_templated_metric_name_renders_per_event() {
        let input = GenerateInput::new(Some(2), 2)
            .with_metric(parse("series.{seq%2}").unwrap(), GenerateMetricKind::Sum, 1.0)
            .unwrap();
        let batches = run_to_completion(input).await;
        let names: Vec<&str> = batches[0]
            .events
            .iter()
            .map(|event| interner::resolve(event.metrics[0].name))
            .collect();
        assert_eq!(names, vec!["series.0", "series.1"]);
    }

    /// An all-literal resource is one `Arc` shared by every batch.
    #[tokio::test]
    async fn every_batch_shares_one_resource_arc() {
        let input = GenerateInput::new(Some(4), 2)
            .with_resource(BTreeMap::from([("service.name".to_string(), "web".to_string())]))
            .unwrap();
        let batches = run_to_completion(input).await;
        assert_eq!(batches.len(), 2);
        assert!(
            Arc::ptr_eq(&batches[0].resource, &batches[1].resource),
            "two batches should share one resource Arc"
        );
        assert_eq!(
            batches[0].resource.attributes.get("service.name").and_then(|v| v.as_str()),
            Some("web")
        );
    }

    /// A templated resource cycles per batch on the batch ordinal. `batch: 100` is the case the
    /// event counter gets wrong: `sent % 10` would be `0` forever.
    #[tokio::test]
    async fn a_templated_resource_cycles_per_batch_on_the_batch_ordinal() {
        let input = GenerateInput::new(Some(1100), 100)
            .with_resource(BTreeMap::from([("host".to_string(), "h{seq%10}".to_string())]))
            .unwrap();
        let batches = run_to_completion(input).await;
        assert_eq!(batches.len(), 11);

        let host = |batch: &EventBatch| {
            batch.resource.attributes.get("host").and_then(|v| v.as_str()).unwrap().to_string()
        };
        let hosts: Vec<String> = batches.iter().map(host).collect();
        assert_eq!(
            hosts,
            vec!["h0", "h1", "h2", "h3", "h4", "h5", "h6", "h7", "h8", "h9", "h0"],
            "a resource template renders from the batch ordinal, not the event counter"
        );
        assert!(
            !Arc::ptr_eq(&batches[0].resource, &batches[1].resource),
            "a templated resource is a fresh Arc per batch, not the shared one"
        );
        // Same text as the first batch, but a fresh `Arc`: no cache keyed on the rendering.
        assert!(!Arc::ptr_eq(&batches[0].resource, &batches[10].resource));
    }

    /// A templated resource renders once per batch, from `batch_index`, not `first_seq`.
    #[test]
    fn a_templated_resource_is_rendered_once_for_a_whole_batch() {
        let mut input = GenerateInput::new(None, 100)
            .with_resource(BTreeMap::from([("host".to_string(), "h{seq%10}".to_string())]))
            .unwrap();
        let batch = input.build_batch(3, 300, 4, 1);
        assert_eq!(
            batch.resource.attributes.get("host").and_then(|v| v.as_str()),
            Some("h3"),
            "batch ordinal 3 modulo 10 -- not the first_seq of 300"
        );
        assert_eq!(batch.events.len(), 4);
    }

    /// A batch waits until the events already sent are due at `rate`: the first is immediate,
    /// the second waits out the first's 100 events. Paused clock, so this checks the arithmetic.
    #[tokio::test(start_paused = true)]
    async fn rate_paces_batches_against_the_wall_clock() {
        let mut input = GenerateInput::new(Some(200), 100).with_rate(Some(1000));
        let started = Instant::now();
        let (tx, mut rx) = mpsc::channel(4);
        let generating = tokio::spawn(async move { input.run(Fanout::new(vec![tx])).await });

        rx.recv().await.expect("the first batch");
        let first_at = started.elapsed();
        rx.recv().await.expect("the second batch");
        let second_at = started.elapsed();

        generating.await.expect("the task should not panic").expect("run returns Ok");
        assert_eq!(
            first_at,
            Duration::ZERO,
            "nothing is owed yet, so the first batch is immediate"
        );
        assert_eq!(
            second_at,
            Duration::from_millis(100),
            "the 100 events already sent are due at +100ms at 1000/s, paced from the run's start"
        );
    }

    /// No `rate` means no pacing at all.
    #[tokio::test(start_paused = true)]
    async fn an_unthrottled_generator_sends_every_batch_immediately() {
        let started = Instant::now();
        let batches = run_to_completion(GenerateInput::new(Some(1000), 100)).await;
        assert_eq!(batches.len(), 10);
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[test]
    fn an_unknown_placeholder_is_rejected_at_construction() {
        let err = GenerateInput::new(Some(1), 1)
            .with_log(parse("host-{hostname}").unwrap())
            .expect_err("an unknown placeholder should be rejected");
        assert!(format!("{err}").contains("{hostname}"), "got: {err}");
    }

    /// `{seq%+5}` is rejected, not a second spelling of `{seq%5}` (see [`resolve_var`]).
    #[test]
    fn a_signed_modulus_is_rejected_at_construction() {
        assert!(GenerateInput::new(Some(1), 1)
            .with_attribute("host", parse("h{seq%+5}").unwrap())
            .is_err());
    }

    /// A bare `{seq}` in the interned metric name is rejected.
    #[test]
    fn a_bare_seq_in_a_metric_name_is_rejected_at_construction() {
        let err = GenerateInput::new(Some(1), 1)
            .with_metric(parse("requests.{seq}").unwrap(), GenerateMetricKind::Sum, 1.0)
            .expect_err("a bare {seq} in a metric name should be rejected");
        let err = format!("{err}");
        assert!(err.contains("interned for the life of the process"), "got: {err}");
        assert!(err.contains("{seq%N}"), "got: {err}");
    }

    /// A bounded `{seq%N}` in a metric name is allowed.
    #[test]
    fn a_bounded_seq_modulo_in_a_metric_name_is_accepted() {
        assert!(GenerateInput::new(Some(1), 1)
            .with_metric(parse("requests.{seq%100}").unwrap(), GenerateMetricKind::Sum, 1.0)
            .is_ok());
    }

    /// A bare `{seq}` is fine in a log body or attribute value: copied, not interned.
    #[test]
    fn a_bare_seq_is_still_allowed_outside_a_metric_name() {
        assert!(GenerateInput::new(Some(1), 1).with_log(parse("{seq}").unwrap()).is_ok());
        assert!(GenerateInput::new(Some(1), 1)
            .with_attribute("n", parse("{seq}").unwrap())
            .is_ok());
    }

    /// A direct caller's `rate: 0` means unthrottled, not a `Duration::from_secs_f64` panic.
    #[tokio::test(start_paused = true)]
    async fn a_zero_rate_means_unthrottled_rather_than_a_panic() {
        let started = Instant::now();
        let batches =
            run_to_completion(GenerateInput::new(Some(200), 100).with_rate(Some(0))).await;
        assert_eq!(batches.iter().map(|b| b.events.len()).sum::<usize>(), 200);
        assert_eq!(started.elapsed(), Duration::ZERO, "a zero rate must not pace at all");
    }

    #[test]
    fn a_zero_modulus_is_rejected_at_construction() {
        assert!(GenerateInput::new(Some(1), 1)
            .with_metric(parse("m{seq%0}").unwrap(), GenerateMetricKind::Sum, 1.0)
            .is_err());
    }

    #[test]
    fn an_unknown_placeholder_in_a_resource_value_is_rejected_at_construction() {
        assert!(GenerateInput::new(Some(1), 1)
            .with_resource(BTreeMap::from([("k".to_string(), "{nope}".to_string())]))
            .is_err());
    }

    /// A direct caller's `batch: 0` is clamped to 1 rather than looping forever.
    #[tokio::test]
    async fn a_zero_batch_is_clamped_to_one_rather_than_looping_forever() {
        let batches = run_to_completion(GenerateInput::new(Some(2), 0)).await;
        assert_eq!(batches.iter().map(|b| b.events.len()).collect::<Vec<_>>(), vec![1, 1]);
    }

    /// `build_batch`, `logit-bench`'s seam, works with no runtime and no channel.
    #[test]
    fn build_batch_renders_without_a_runtime() {
        let mut input = GenerateInput::new(None, 100)
            .with_log(parse("path=/x/{seq%2}").unwrap())
            .unwrap()
            .with_attribute("host", parse("web-1").unwrap())
            .unwrap();
        let batch = input.build_batch(0, 10, 3, 42);
        assert_eq!(batch.events.len(), 3);
        assert!(batch.events.iter().all(|event| event.timestamp == 42));
        let messages: Vec<&str> = batch
            .events
            .iter()
            .map(|event| event.log.as_ref().unwrap().message.as_str().unwrap())
            .collect();
        assert_eq!(messages, vec!["path=/x/0", "path=/x/1", "path=/x/0"]);
        // A literal attribute shares one buffer even on the per-event path.
        let ptr = |event: &Event| match event.attributes.get("host").unwrap() {
            Value::Str(bytes) => bytes.as_ptr(),
            other => panic!("expected a Str attribute, got {other:?}"),
        };
        assert_eq!(ptr(&batch.events[0]), ptr(&batch.events[1]));
    }
}
