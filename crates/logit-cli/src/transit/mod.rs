//! The transit flow driver: runs one case through one flow's real components, in process and on
//! a fixed clock, and reports what the sink's pair decoder reads back and which counters the run
//! moved ([`docs/plans/transit-spec.md`](../../../../docs/plans/transit-spec.md)'s "The flow
//! driver").
//!
//! A **flow** is a `logit` config, `replay_in -> [transforms] -> <sink>`. A **case** is a
//! `format: json` file (`crates/logit-outputs/src/ndjson.rs`'s grammar). [`run`]:
//!
//! 1. loads the flow through [`crate::config::load`] and [`graph::resolve`], and requires a chain
//!    (below);
//! 2. reads the case with [`EventJsonDecoder::decode_batches`], cut at `replay_in`'s `batch:`
//!    as `replay_in` cuts it. `replay_in` itself doesn't run, and its `path:` isn't read;
//! 3. builds each transform with `build_spec` and passes every batch through them in order, with
//!    the hooks the node runtime calls: `observe_batch_context`, `observe_provenance`, then
//!    [`process_batch`] (which calls `observe_scope`);
//! 4. after the last batch, flushes each transform that has a flush interval once, in chain
//!    order, at [`Options::now`], and passes what it emits through the transforms after it, as
//!    the runtime's close cascade does;
//! 5. encodes every batch through the sink's encoder ([`crate::pipeline::sink_encoder`]), counts
//!    the encoder's stats as the sink counts them, and decodes the bytes through the pair's
//!    decoder (`wire`);
//! 6. renders the result as one JSON document (`report`): `events`, the re-decoded events as
//!    `format: json` objects, and `counters`, every loss or normalization series the run moved.
//!
//! **Determinism.** No socket, no task, and no wall clock reaches the output. One instant,
//! [`Options::now`] (by default the case's latest event timestamp plus one second), is both every
//! flush's `now` and every decode's `received_at`, so a wire with no timestamp (a statsd line
//! without `|T`) reads back at it. Events are sorted by timestamp, then first metric or span
//! name, then their rendered line, so batching and reordering, both permitted normalizations,
//! don't change the document. One part of a sink isn't reproduced: `prometheus_out`'s
//! `expire_after:`, which reads a clock, so exposition reads back every series not evicted by
//! `max_series:`. One is rewritten: `otlp_out` stamps a log with no observed time with the wall
//! clock, and `wire` reads that stamp back as the run's clock. Every component counts into a `TelemetryProbe` registry under its
//! real id, kind, and role; the pair decoder counts under the id `transit.decode` and its own
//! kind, as a listener.
//!
//! **What it rejects**, each naming the component: an input other than one `replay_in` with
//! `format: json`; a component with more than one consumer or more than one source; `lua`,
//! `lua_file`, `route`, `route_signals`, and `target`; `has_stale` and `drop_stale`, which read
//! the wall clock per batch; and a sink [`sink_encoder`] has no arm
//! for (today `statsd_out`, `influxdb_out`, `otlp_out`, and `prometheus_out` in both modes).
//!
//! Exposition has no configured dialect: a scraper negotiates one, so the driver takes it as
//! [`Options::dialect`], OpenMetrics by default, which is what a Prometheus scrape negotiates.
//!
//! `spec` runs `spec/transit/`'s cases through its flows with this driver, and `classes` checks
//! each run against `spec/transit/mapping.yaml` (`mapping`); `spec/transit/README.md` is the
//! reader's account.

mod classes;
mod mapping;
mod report;
mod spec;
mod tests;
mod wire;

use crate::pipeline::{build_spec, sink_encoder};
use anyhow::Context;
use logit_config::{ComponentKind, ReplayFormat};
use logit_core::interner::intern;
use logit_core::{Diagnostics, EventBatch, Provenance, Telemetry};
use logit_pipeline::graph::{self, Graph, Role};
use logit_pipeline::test_util::TelemetryProbe;
use logit_pipeline::tls::TlsReloader;
use logit_pipeline::{process_batch, NodeSpec, TraceContext, Transform};
use logit_proto::event_json::EventJsonDecoder;
use logit_proto::prometheus::Dialect;
use std::path::Path;
use std::sync::Arc;

pub(crate) use report::Outcome;

/// One second in Unix nanoseconds: the default gap between a case's last event and the run's
/// clock.
const SECOND: i64 = 1_000_000_000;

/// What a run varies besides the flow and the case.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Options {
    /// Every flush's `now` and every decode's `received_at`, in Unix nanoseconds. `None` is the
    /// case's latest event timestamp plus one second.
    pub now: Option<i64>,
    /// The exposition dialect a `prometheus_out` with `bind:` is read in.
    pub dialect: Dialect,
}

impl Default for Options {
    fn default() -> Self {
        Self { now: None, dialect: Dialect::OpenMetrics1_0 }
    }
}

/// A flow's components in order: its `replay_in`, the transforms after it, and its sink.
#[derive(Debug)]
struct Chain {
    input: String,
    batch: usize,
    transforms: Vec<String>,
    sink: String,
}

/// One built transform and what the runtime would hand it.
struct Stage {
    id: String,
    transform: Box<dyn Transform + Send>,
    telemetry: Telemetry,
    emitted: Vec<logit_core::Event>,
}

/// What a run reached and read back, beside its document: what the spec's class check
/// (`spec`) compares.
pub(crate) struct Traced {
    pub outcome: Outcome,
    /// The batches that reached the sink, after every stage.
    pub sunk: Vec<EventBatch>,
    /// The pair decoder's reading of the sink's bytes.
    pub decoded: Vec<EventBatch>,
    /// The sink's config as `ComponentKind` serializes it, every default filled in.
    pub sink: serde_json::Value,
}

/// Runs `case` through `flow` (module doc).
pub(crate) fn run(flow: &Path, case: &Path, options: Options) -> anyhow::Result<Outcome> {
    run_traced(flow, case, options).map(|traced| traced.outcome)
}

/// [`run`], keeping what reached the sink and what its pair decoder read back.
pub(crate) fn run_traced(flow: &Path, case: &Path, options: Options) -> anyhow::Result<Traced> {
    let config = crate::config::load(flow)?;
    let graph = graph::resolve(config)?;
    let chain = chain(&graph)?;
    let base_dir = flow.parent().unwrap_or(Path::new("."));

    let bytes = std::fs::read(case).with_context(|| format!("reading case {}", case.display()))?;
    let batches = EventJsonDecoder::new()
        .decode_batches(&bytes)
        .with_context(|| format!("decoding case {}", case.display()))?;
    let batches = cut(batches, chain.batch);
    let now = match options.now {
        Some(now) => now,
        None => {
            let latest = batches.iter().flat_map(|b| &b.events).map(|e| e.timestamp).max();
            latest.context("the case holds no events")? + SECOND
        }
    };

    let probe = TelemetryProbe::new();
    let registry = Arc::clone(probe.registry());
    let reloader = TlsReloader::new();
    let hostname: Arc<str> = Arc::from("transit");
    let mut stages = Vec::with_capacity(chain.transforms.len());
    for id in &chain.transforms {
        let component = &graph.components[id];
        let (spec, telemetry) =
            build_spec(id, component, base_dir, Some(&registry), None, &reloader, &hostname)
                .with_context(|| format!("component '{id}'"))?;
        let NodeSpec::Transform(transform) = spec else {
            anyhow::bail!("component '{id}' ({}) isn't a transform", component.kind_name());
        };
        stages.push(Stage { id: id.clone(), transform, telemetry, emitted: Vec::new() });
    }

    let input = intern(&chain.input);
    let mut out = Vec::new();
    for batch in batches {
        let provenance = Provenance { origin: Some(input), previous: Some(input) };
        out.extend(through(&mut stages, 0, batch, provenance));
    }
    for i in 0..stages.len() {
        if stages[i].transform.flush_interval().is_none() {
            continue;
        }
        let me = intern(&stages[i].id);
        for (resource, scope, flushed) in stages[i].transform.flush(now) {
            if flushed.is_empty() {
                continue;
            }
            let events = flushed.into_iter().map(|(event, _links)| event).collect();
            let batch = EventBatch { resource, scope, events };
            let provenance = Provenance { origin: Some(me), previous: Some(me) };
            out.extend(through(&mut stages, i + 1, batch, provenance));
        }
    }

    let sink = &graph.components[&chain.sink];
    let telemetry = probe.telemetry(&chain.sink, sink.kind_name(), sink.role().as_str());
    let diag = Diagnostics::new(&chain.sink).with_telemetry(telemetry.clone());
    let encoder = sink_encoder(&sink.kind, &telemetry, &diag)
        .with_context(|| format!("component '{}'", chain.sink))?;
    let decode = wire::Decode { probe: &probe, received_at: now, dialect: options.dialect };
    let decoded = wire::round_trip(encoder, &out, &telemetry, &diag, &decode)
        .with_context(|| format!("round-tripping through '{}'", chain.sink))?;

    let mut probe = probe;
    let outcome = Outcome::new(&decoded, probe.poll());
    let sink = serde_json::to_value(&sink.kind).context("serializing the sink's config")?;
    Ok(Traced { outcome, sunk: out, decoded, sink })
}

/// Passes `batch` through `stages[from..]`, as the node runtime would, and returns what reaches
/// the sink. Each stage's output carries the provenance `Fanout::stamp` would give it.
fn through(
    stages: &mut [Stage],
    from: usize,
    batch: EventBatch,
    provenance: Provenance,
) -> Vec<EventBatch> {
    let mut batches = vec![(batch, provenance)];
    for stage in &mut stages[from..] {
        let me = intern(&stage.id);
        let mut next = Vec::with_capacity(batches.len());
        for (batch, provenance) in batches {
            stage.transform.observe_batch_context(TraceContext::default());
            stage.transform.observe_provenance(provenance);
            let out =
                process_batch(&mut *stage.transform, batch, &mut stage.emitted, &stage.telemetry);
            if let Some(out) = out {
                next.push((out, Provenance { origin: provenance.origin, previous: Some(me) }));
            }
        }
        batches = next;
    }
    batches.into_iter().map(|(batch, _)| batch).collect()
}

/// Splits each run of equal resource and scope at `batch` events, as `replay_in` does.
fn cut(batches: Vec<EventBatch>, batch: usize) -> Vec<EventBatch> {
    let batch = batch.max(1);
    let mut out = Vec::with_capacity(batches.len());
    for EventBatch { resource, scope, mut events } in batches {
        while !events.is_empty() {
            let rest = events.split_off(events.len().min(batch));
            out.push(EventBatch { resource: Arc::clone(&resource), scope: scope.clone(), events });
            events = rest;
        }
    }
    out
}

/// The flow's chain, or an error naming the first component that breaks it (module doc).
fn chain(graph: &Graph) -> anyhow::Result<Chain> {
    let mut ids: Vec<&String> = graph.components.keys().collect();
    ids.sort();
    let mut inputs = Vec::new();
    for id in ids {
        let component = &graph.components[id];
        match component.role() {
            Role::Listener => inputs.push(id),
            Role::Target => anyhow::bail!("component '{id}': a flow can't hold a target"),
            Role::Transform | Role::Sink => {}
        }
    }
    let [input] = inputs[..] else {
        anyhow::bail!("a flow has one input, a replay_in; this one has {}", inputs.len());
    };
    let batch = match &graph.components[input].kind {
        ComponentKind::ReplayIn { format: ReplayFormat::Json, batch, .. } => *batch,
        ComponentKind::ReplayIn { .. } => {
            anyhow::bail!("component '{input}': a flow's replay_in reads format: json")
        }
        other => {
            let kind = graph::kind_name(other);
            anyhow::bail!("component '{input}': a flow's input is a replay_in, not {kind}")
        }
    };

    let mut transforms = Vec::new();
    let mut at = input;
    loop {
        let consumers = &graph.components[at].consumers;
        let next = match &consumers[..] {
            [] => anyhow::bail!("component '{at}': the chain ends without a sink"),
            [next] => next,
            _ => anyhow::bail!("component '{at}': a flow can't fan out"),
        };
        let component = &graph.components[next];
        if component.sources.len() != 1 {
            anyhow::bail!("component '{next}': a flow can't fan in");
        }
        match &component.kind {
            ComponentKind::Lua { .. }
            | ComponentKind::LuaFile { .. }
            | ComponentKind::Route { .. }
            | ComponentKind::RouteSignals { .. } => {
                anyhow::bail!(
                    "component '{next}': the driver doesn't run a {}",
                    component.kind_name()
                )
            }
            ComponentKind::HasStale { .. } | ComponentKind::DropStale { .. } => {
                anyhow::bail!(
                    "component '{next}': a {} reads the wall clock, which the driver doesn't fix",
                    component.kind_name()
                )
            }
            _ => {}
        }
        match component.role() {
            Role::Sink => {
                return Ok(Chain { input: input.clone(), batch, transforms, sink: next.clone() })
            }
            _ => transforms.push(next.clone()),
        }
        at = next;
    }
}
