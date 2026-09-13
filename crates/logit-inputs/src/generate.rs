//! `generate_in`: a synthetic event source, the listener end of the perf harness
//! (`docs/plans/load-test-harness.md`). No socket and no decoder -- it renders a declarative
//! event template as fast as `count`/`rate` allow, so a scenario measures the runtime and the
//! components under test rather than a load-generator process and a kernel socket buffer.
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
//! `count` is **exact**: the last batch is short (`count % batch`) rather than rounded up to a
//! whole batch, so a harness deriving events/s and CPU-per-event divides by the number actually
//! produced. When `count` is reached [`Input::run`] returns `Ok(())`, its `Fanout` drops, and the
//! existing listener-exit cascade flushes and shuts the process down cleanly
//! (`crates/logit-pipeline/src/runtime.rs`).
//!
//! # Two render paths
//!
//! Placeholders are for *cardinality*, never decoration -- each one costs a rendering and a copy
//! per event, and which path this input takes is decided by whether there is a single one anywhere
//! in the template:
//!
//! - **Prototype** (no placeholder in any field): one [`Event`] is rendered once, at first use,
//!   and `clone`d per generated event with only its `timestamp` overwritten. Every field's bytes
//!   are the *same* refcounted buffer on every event -- a refcount bump, not a copy.
//! - **Per event** (at least one placeholder anywhere): each templated field is rendered into a
//!   reused `String` scratch and copied out with one `Bytes::copy_from_slice` -- exactly one
//!   allocation per templated field per event, and none for the literal fields alongside it,
//!   which still clone the bytes rendered once at construction. A templated *metric name* is the
//!   one exception: it is interned rather than copied, so it pays `interner::intern`'s cost per
//!   event and permanently grows the interner by one entry per distinct rendering (`{seq%1000}` in
//!   a metric name means a thousand interned names -- see `docs/design/memory.md` §4). That is why
//!   a metric name may use `{seq%N}` but **not** a bare `{seq}`, which would intern a fresh,
//!   never-freed name per event: rejected at construction here and by graph rule 42 in config.
//!
//! `now_nanos()` is read once per batch, not per event, the same way every decoder amortizes it
//! across a datagram.
//!
//! The **resource** is a third case, because it is batch-level rather than per event: an
//! all-literal resource is built once at construction and `Arc`-shared by every batch forever,
//! and a templated one is rebuilt once per batch. **In resource position `seq` is the batch
//! ordinal** (0, 1, 2, ...), not the event counter -- rendering from the event counter would
//! advance by `batch` each time and collapse `{seq%10}` under `batch: 100` to a single value. So
//! `resource: { host: "h{seq%10}" }` really is ten resources cycling per batch, costing one
//! `AttrMap` and one `Arc<Resource>` per batch and nothing per event. See
//! [`GenerateInput::with_resource`].
//!
//! # Rate pacing
//!
//! `rate` is held against the *wall clock*, never a fixed `interval(batch / rate)` timer: before
//! sending a batch this input sleeps until `start + sent / rate`, recomputed from the run's own
//! start every time, so a batch that ran late doesn't shift every later deadline. The average
//! rate over a run holds instead of drifting, and the first batch goes out immediately rather
//! than waiting out a batch-interval nothing has been generated in yet. Above roughly a thousand
//! batches per second the sleep granularity makes it bursty within any given millisecond --
//! accurate on average, ~1 ms granular in the small (`docs/known-gaps.md`).
//!
//! # Telemetry
//!
//! **Layer 2 only** -- this input records no points of its own. The runtime's uniform
//! per-component instrumentation already counts what this node sent on its fanout edge, and
//! `logit.component.events.sent` *is* the generated count, so a second counter here would only
//! restate it (`docs/design/internal-telemetry.md`). What it does add is one `Diagnostics` key,
//! mirrored as `logit.component.diagnostics{key}` by the bridge: `rate_behind`, for a generator
//! that cannot hold the `rate` it was configured with -- the signal that a rate-limited scenario
//! has quietly become a throughput one.

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

/// A placeholder name resolved into what rendering it actually needs -- the `V` in
/// [`Template::compile`], so the hot path walks pre-resolved segments with no string matching per
/// event.
///
/// The set is deliberately tiny, and deliberately the same set `logit-pipeline`'s graph rule 42
/// accepts at validation time (`generate_var_is_valid`). Those two live in different crates on
/// purpose -- an implementation crate may not depend on `logit-config`
/// (`docs/design/pipeline-graph.md`'s crate layout) -- so they are kept in step by being small
/// enough to compare by eye. See [`resolve_var`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenVar {
    /// `{seq}` -- the generator's 0-based event counter.
    Seq,
    /// `{seq%N}` -- that counter modulo `N`, a scenario's cardinality knob. `N >= 1`, so the
    /// modulo can never divide by zero.
    SeqMod(u64),
}

/// Which `logit_core::MetricKind` a generated metric carries.
///
/// A local mirror of `logit_config::GenerateMetricKind` -- `logit-inputs` must not depend on
/// `logit-config` (`docs/design/pipeline-graph.md`'s crate layout), the same reason
/// `logit_outputs::file::RotatePolicy` mirrors `logit_config::RotateConfig`;
/// `crates/logit-cli/src/pipeline.rs::build_spec` is the sole place a config value crosses into
/// this type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GenerateMetricKind {
    /// A delta, monotonic `Sum` -- `MetricKind::counter`, the cheapest shape to generate.
    #[default]
    Sum,
    Gauge,
    /// Raw `Samples` carrying the one configured value -- **never** a pre-built `DdSketch`. A
    /// listener that pre-summarized would be exactly the shape
    /// `docs/adr/lossless-transit.md`'s "a decoder never pre-summarizes what an explicit
    /// `aggregate` stage should decide about" rule forbids, and a scenario measuring the
    /// sketch-merging path wants that merge to happen in `aggregate`, where it really does.
    Distribution,
}

/// Resolves one placeholder name, rejecting anything `generate_in` doesn't substitute.
///
/// **Mirrors `logit_pipeline::graph`'s `generate_var_is_valid` exactly**, including its
/// digits-only modulus rule: `u64::from_str` would also accept a leading `+`, so `{seq%+5}` is
/// rejected here rather than becoming a second spelling of `{seq%5}`. Graph rule 42 rejects an
/// unknown name at validation time, so in a `logit run` this never fires -- it is still an error
/// rather than a panic, because a direct caller of this module's builders (a unit test,
/// `logit-bench`) has no rule 42 in front of it.
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

/// [`resolve_var`], minus the unbounded one, for the one field whose rendering is **interned**
/// rather than copied: `event.metric.name`.
///
/// A bare `{seq}` there would intern a fresh, never-freed `Symbol` for every event a run
/// generates -- a million-event scenario would leave a million metric names in the process-wide
/// interner (`docs/design/memory.md` §4: interning is monotonic, nothing is ever removed), which
/// is a leak in the shape of a feature rather than a cardinality knob. `{seq%N}` is bounded by
/// `N` and stays allowed; that is what a scenario wanting metric-name cardinality actually means.
/// Graph rule 42 rejects the same thing at validation time, for a `logit run` that never reaches
/// this.
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

/// Appends one placeholder's rendering. `write!` into a `String` is infallible, so the `Result`
/// is discarded rather than propagated.
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
    /// No placeholder: the bytes were rendered once, at construction. Every event gets a
    /// `clone` of *these* bytes -- a refcount bump.
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

    /// This field's bytes for event `seq`. A literal costs a refcount bump; a template costs one
    /// render into `scratch` plus one `Bytes::copy_from_slice` -- the single allocation per
    /// templated field per event this module's doc comment pins.
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

/// A generated metric's name. Split from [`Field`] because a metric name is *interned*, not
/// copied: the literal case resolves to a `Symbol` once, at construction, and never touches the
/// interner again.
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
    /// Constant, deliberately: a varying value would measure the generator's own arithmetic
    /// rather than the pipeline's.
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

/// How a batch gets its [`Resource`] -- the batch-level mirror of [`RenderPath`], and independent
/// of it: a templated resource on an all-literal event template still takes the prototype path
/// for its events.
#[derive(Debug, Clone)]
enum ResourceSpec {
    /// Every value is literal, which is the usual case: the resource was built once, at
    /// construction, and every batch this input ever sends shares *this* `Arc`. Rebuilding it per
    /// batch would re-intern every key and reallocate an `AttrMap` for a value that cannot
    /// change; `InternalInput::resource` carries the same reasoning.
    Fixed(Arc<Resource>),
    /// At least one value has a placeholder: rebuilt once per batch from that batch's *ordinal*,
    /// so one `AttrMap` and one `Arc<Resource>` per batch and nothing per event. See
    /// [`GenerateInput::with_resource`] for why the ordinal and not the event counter.
    Templated(Vec<(Symbol, Field)>),
}

/// Which of this module's two render paths applies -- settled on first use rather than in
/// [`GenerateInput::new`], since a `with_*` builder may still add a templated field afterwards.
#[derive(Debug, Clone)]
enum RenderPath {
    Undecided,
    /// Every field is literal: this event is `clone`d per generated event, `timestamp`
    /// overwritten. `Box`ed because an `Event` is several hundred bytes and the other two
    /// variants carry nothing, so inlining it would make every `GenerateInput` that big
    /// (clippy's `large_enum_variant`). The indirection costs one allocation per process and is
    /// never on the per-event path -- what is cloned per event is the `Event`, not the `Box`.
    Prototype(Box<Event>),
    /// Something is templated: every event is rendered field by field.
    PerEvent,
}

/// A synthetic listener: renders `count` events from a template, optionally paced to `rate`.
#[derive(Debug)]
pub struct GenerateInput {
    /// `None` means unbounded -- a soak run, or one a profiler attaches to.
    count: Option<u64>,
    /// Events per generated batch. Clamped to at least 1 by [`GenerateInput::new`]: graph rule 42
    /// already rejects `batch: 0` in config, but a direct caller has no rule 42 in front of it and
    /// a zero-sized batch would loop forever generating nothing.
    batch: usize,
    /// `None` means unthrottled -- as fast as downstream backpressure allows.
    rate: Option<u64>,
    log: Option<Field>,
    /// Keys interned once, here; values rendered per event (or once, for a literal). A `Vec`
    /// rather than a map: the keys are already distinct (they came from a `BTreeMap`) and this is
    /// only ever iterated, never looked up.
    attributes: Vec<(Symbol, Field)>,
    metric: Option<MetricSpec>,
    resource: ResourceSpec,
    path: RenderPath,
    /// One reused render buffer, `clear`ed per templated field -- it stops allocating entirely
    /// once it has grown to the widest rendering it has seen
    /// (`logit_core::template::Compiled::render`).
    scratch: String,
    diag: Diagnostics,
}

impl GenerateInput {
    /// A generator with no payload at all: a timestamp and an empty resource, which is exactly
    /// what a "runtime floor" scenario wants to measure. Every `with_*` builder adds to that.
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
    /// `Some(0)` is folded to `None` rather than stored: graph rule 42 rejects `rate: 0` in
    /// config, but a direct caller has no rule 42 in front of it, and a zero rate would make
    /// [`GenerateInput::pace`]'s `sent / rate` infinite -- which `Duration::from_secs_f64`
    /// panics on. Same shape as `new`'s `batch.max(1)`.
    pub fn with_rate(mut self, rate: Option<u64>) -> Self {
        self.rate = rate.filter(|rate| *rate > 0);
        self
    }

    /// The log body every generated event carries. Omitted entirely means the event carries no
    /// `LogRecord` at all -- a metrics-only scenario.
    pub fn with_log(mut self, template: Template) -> anyhow::Result<Self> {
        self.log = Some(Field::new(&template)?);
        self.path = RenderPath::Undecided;
        Ok(self)
    }

    /// One event attribute: a literal key (interned here, once) and a templated value.
    pub fn with_attribute(mut self, key: &str, template: Template) -> anyhow::Result<Self> {
        self.attributes.push((intern(key), Field::new(&template)?));
        self.path = RenderPath::Undecided;
        Ok(self)
    }

    /// The metric stamped on every generated event. Omitted means no metrics -- a logs-only
    /// scenario.
    ///
    /// `name` may use `{seq%N}` but **not** a bare `{seq}`: a metric name is interned, and an
    /// interned `Symbol` lives for the life of the process. See [`resolve_metric_name_var`].
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

    /// The resource every batch carries. Values may name placeholders, and the unit a resource
    /// template renders at is **one batch**, not one event: the resource is batch-level and
    /// `Arc`-shared by every event in the batch (`logit_core::EventBatch::resource`), so a
    /// per-event rendering would have nowhere to go.
    ///
    /// **In resource position `seq` is the batch ordinal** -- 0, 1, 2, ... -- not the event
    /// counter. That distinction is the whole feature: the event counter advances by `batch` per
    /// batch, so `{seq%N}` over it would only ever produce `N / gcd(N, batch)` distinct values,
    /// and for the overwhelmingly common case of a `batch` that is a multiple of `N` (`batch:
    /// 100`, `{seq%10}`) that is exactly one -- a "cardinality knob" silently stuck on its first
    /// setting. Rendering from the ordinal instead makes `resource: { host: "h{seq%10}" }` mean
    /// what it reads as: ten distinct resources, cycling per batch.
    ///
    /// So this is a real multi-resource cardinality knob at zero per-event cost -- one `AttrMap`
    /// and one `Arc<Resource>` per batch, nothing per event -- which is what a scenario measuring
    /// resource grouping (`aggregate`'s `logit.transform.resource.groups`, a sink that keys on
    /// the resource) actually needs. Two consequences worth knowing: a batch is the granularity,
    /// so every event in one batch shares one resource no matter how large `batch` is; and an
    /// all-literal resource keeps the strictly cheaper path, built once at construction and
    /// `Arc`-shared by every batch forever ([`ResourceSpec`]).
    ///
    /// Takes raw strings rather than parsed [`Template`]s, unlike every other builder here: a
    /// resource value's parse is startup-only either way, so there is nothing for a caller to
    /// pre-parse on its behalf.
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

    /// Attaches this component's own telemetry handle.
    ///
    /// `generate_in` records **no layer-3 points** -- see this module's "Telemetry" section for
    /// why the runtime's `logit.component.events.sent` already is the generated count -- so the
    /// handle's only job here is the `Diagnostics` bridge, which mirrors every `rate_behind`
    /// occurrence into `logit.component.diagnostics{key}`. Call this *after*
    /// [`GenerateInput::with_diagnostics`]: it attaches to whatever `Diagnostics` this input is
    /// holding at the time.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.diag = self.diag.clone().with_telemetry(telemetry);
        self
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    /// Builds one batch of `n` events numbered `first_seq..first_seq + n`, every one stamped
    /// `now` (Unix nanoseconds), under batch ordinal `batch_index` (0 for a run's first batch).
    ///
    /// `batch_index` is what a resource template's `{seq}`/`{seq%N}` renders from, and it is a
    /// separate argument rather than derived from `first_seq` deliberately: `first_seq / batch`
    /// would be a second, silently-wrong definition the moment a caller drove `build_batch`
    /// with anything but exact multiples of `batch` -- which `logit-bench` and this module's own
    /// tests both do.
    ///
    /// Public so `logit-bench` can drive both render paths directly -- no runtime, no channel,
    /// nothing between the measurement and the code it is measuring, which is what keeps that
    /// crate's allocation counts trustworthy (`crates/logit-bench/benches/pipeline.rs`'s module
    /// doc, `docs/design/memory.md`'s "Fixtures" section). The first call also settles which
    /// render path applies, and (on the prototype path) renders the prototype -- so a caller
    /// measuring allocations must warm it, exactly like every other measurement in that crate.
    pub fn build_batch(
        &mut self,
        batch_index: u64,
        first_seq: u64,
        n: usize,
        now: i64,
    ) -> EventBatch {
        self.settle_render_path();
        // Taken out and put back so the renders below can borrow it mutably while everything they
        // render *from* is borrowed immutably -- and so its grown capacity survives across
        // batches, which is what makes a warm scratch allocate nothing.
        let mut scratch = std::mem::take(&mut self.scratch);
        let resource = self.render_resource(batch_index, &mut scratch);
        let mut events = Vec::with_capacity(n);
        if let RenderPath::Prototype(prototype) = &self.path {
            for _ in 0..n {
                // `(**prototype)`, not `prototype.clone()`: the latter would clone the `Box`
                // itself, paying an allocation per event for the indirection the variant's own
                // doc comment explains is a once-per-process cost.
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

    /// This batch's resource: the one shared `Arc` when every value is literal, or a fresh one
    /// rendered from the **batch ordinal** when any value is templated. See
    /// [`GenerateInput::with_resource`] for why that, and not the event counter, is what `{seq}`
    /// means in resource position.
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
        // Rendered at `seq = 0` with a throwaway scratch: an all-literal template never reads
        // either, and this runs once per process.
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
            // `Raw`, never `Json`, even when the body plainly is JSON: parsing is composed
            // downstream by an ordinary `json` stage, which is the whole point of generating a
            // body as text rather than as a shape enum.
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

    /// Holds `rate` against the wall clock -- see this module's "Rate pacing" section for why the
    /// deadline is recomputed from `started` rather than slept for in fixed increments.
    async fn pace(&mut self, started: Instant, sent: u64, rate: u64) {
        let due_at = started + Duration::from_secs_f64(sent as f64 / rate as f64);
        let now = Instant::now();
        if now < due_at {
            tokio::time::sleep_until(due_at).await;
            return;
        }
        // Behind the pace, and no amount of not-sleeping will fix it: the scenario is measuring
        // something slower than the rate it asked for, which silently turns a rate-limited
        // measurement into a throughput one. Reported only once the deficit passes a whole
        // second's worth of events, so ordinary millisecond-scale jitter stays quiet.
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
            // The last batch is short rather than rounded up, which is what makes `count` exact.
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
        // The line a harness watches for: it marks the end of generation (as distinct from the end
        // of the process, which is whatever the downstream flush takes afterwards), and carries the
        // count a derived events/s divides by (`docs/plans/load-test-harness.md`).
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

    /// `count` is exact, not rounded up to a whole batch -- the property a harness's derived
    /// events/s and CPU-per-event divide by.
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

    /// The prototype path's whole point: an all-literal template renders its bytes once and every
    /// event shares *that* buffer, so the message costs a refcount bump per event rather than a
    /// copy. Asserted on the pointer, not on equality -- equal bytes would pass either way.
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

    /// Every event on the prototype path still gets the batch's own timestamp, not the one the
    /// prototype was rendered with.
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

    /// `seq` keeps counting across batches -- it is the run's event counter, not a per-batch index.
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

    /// A distribution generates *raw* `Samples`, never a pre-built `DdSketch` -- summarization
    /// stays `aggregate`'s job (`docs/adr/lossless-transit.md`).
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

    /// An **all-literal** resource is built once and `Arc`-shared by every batch -- not rebuilt
    /// per batch, which would re-intern every key for a value that cannot change.
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

    /// A **templated** resource renders once per batch, from the batch *ordinal* -- so
    /// `h{seq%10}` really does cycle through ten resources, one per batch, and comes back round
    /// on the eleventh. Driven through a whole real run at the shipped example's own `batch: 100`
    /// specifically because that is the shape the event counter gets wrong: `sent` advances by
    /// 100 per batch, so `sent % 10` would be `0` forever and the knob would be silently stuck.
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
        // ...and the eleventh batch renders the same *text* as the first without being the same
        // `Arc`: this knob is a fresh resource per batch, not a cache keyed on the rendering.
        assert!(!Arc::ptr_eq(&batches[0].resource, &batches[10].resource));
    }

    /// The cost side of the same rule: a templated resource is rendered once per *batch*, never
    /// once per event -- every event in a batch reads the very same `Arc`, which is what keeps
    /// this knob free on the per-event path.
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

    /// `rate` paces against the wall clock: a batch waits until the events already *sent* are due
    /// at the configured rate, so the first goes out immediately and the second waits out the
    /// 100 events the first carried. Paused clock, so this asserts the real arithmetic rather
    /// than a sleep's accuracy.
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

    /// No `rate` means no pacing at all -- the throughput case, which must not sleep.
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

    /// The digits-only modulus rule `logit_pipeline::graph`'s `generate_var_is_valid` enforces,
    /// mirrored here: `u64::from_str` would accept the `+`, so without the digit check this would
    /// become a second spelling of `{seq%5}`.
    #[test]
    fn a_signed_modulus_is_rejected_at_construction() {
        assert!(GenerateInput::new(Some(1), 1)
            .with_attribute("host", parse("h{seq%+5}").unwrap())
            .is_err());
    }

    /// A metric name is interned, and an interned `Symbol` is never freed -- so a bare `{seq}`
    /// there would leave one never-reclaimed name per generated event in the process-wide
    /// interner. Rejected at construction, and by graph rule 42 before that in a `logit run`.
    #[test]
    fn a_bare_seq_in_a_metric_name_is_rejected_at_construction() {
        let err = GenerateInput::new(Some(1), 1)
            .with_metric(parse("requests.{seq}").unwrap(), GenerateMetricKind::Sum, 1.0)
            .expect_err("a bare {seq} in a metric name should be rejected");
        let err = format!("{err}");
        assert!(err.contains("interned for the life of the process"), "got: {err}");
        assert!(err.contains("{seq%N}"), "got: {err}");
    }

    /// ...while a *bounded* one is exactly what metric-name cardinality means, and stays allowed.
    #[test]
    fn a_bounded_seq_modulo_in_a_metric_name_is_accepted() {
        assert!(GenerateInput::new(Some(1), 1)
            .with_metric(parse("requests.{seq%100}").unwrap(), GenerateMetricKind::Sum, 1.0)
            .is_ok());
    }

    /// The same reasoning does *not* apply to a log body or an attribute value: those are copied
    /// per event, not interned, so an unbounded `{seq}` costs one allocation and frees with the
    /// event.
    #[test]
    fn a_bare_seq_is_still_allowed_outside_a_metric_name() {
        assert!(GenerateInput::new(Some(1), 1).with_log(parse("{seq}").unwrap()).is_ok());
        assert!(GenerateInput::new(Some(1), 1)
            .with_attribute("n", parse("{seq}").unwrap())
            .is_ok());
    }

    /// Rule 42 rejects `rate: 0` in config, but a direct caller has no rule 42 in front of it --
    /// and `0` would make `pace`'s `sent / rate` infinite, which `Duration::from_secs_f64`
    /// panics on. Folded to "unthrottled" instead, the same shape as `batch`'s clamp.
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

    /// Graph rule 42 rejects `batch: 0` in config, but a direct caller has no rule 42 in front of
    /// it -- and a zero-sized batch would otherwise loop forever generating nothing.
    #[tokio::test]
    async fn a_zero_batch_is_clamped_to_one_rather_than_looping_forever() {
        let batches = run_to_completion(GenerateInput::new(Some(2), 0)).await;
        assert_eq!(batches.iter().map(|b| b.events.len()).collect::<Vec<_>>(), vec![1, 1]);
    }

    /// `build_batch` is the seam `logit-bench` measures through, so it has to be usable with no
    /// runtime and no channel at all.
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
        // The literal attribute still shares one buffer even on the per-event path.
        let ptr = |event: &Event| match event.attributes.get("host").unwrap() {
            Value::Str(bytes) => bytes.as_ptr(),
            other => panic!("expected a Str attribute, got {other:?}"),
        };
        assert_eq!(ptr(&batch.events[0]), ptr(&batch.events[1]));
    }
}
