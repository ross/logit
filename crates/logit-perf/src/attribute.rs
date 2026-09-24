//! `logit-perf attribute`: where a scenario's time goes, per node.
//!
//! `run` produces one number for a whole graph; this says which node it goes to, without a
//! profiler, from what the runtime records about itself: `logit.component.process.duration`,
//! `.send.blocked.duration`, `.send.duration`, the received/sent counters, and sink queues'
//! `buffer.utilization`, each stamped with `component`/`kind`/`role`
//! (`docs/design/internal-telemetry.md`). They're read through the leg [`crate::telemetry_leg`]
//! appends.
//!
//! **The harness's own two nodes are in the table.** `__perf_internal` and `__perf_dump` are
//! ordinary rows, so attribution's cost is visible, but the verdict and count check exclude them.
//!
//! **Driven scenarios work here too.** `attribute` blasts a real-socket scenario as `run` does
//! (`crate::run`'s `Drive::Driven`) and reads the same dump. Its count check doesn't warn about
//! loss, which a UDP scenario is allowed; `run` checks the sent/received/dropped accounting.

use crate::load::{CpuSet, LoadPlan};
use crate::run::{self, Drive, SpawnConfig};
use crate::scenario::{self, Scenario, Workload};
use crate::telemetry_leg::{self, RemoveOnDrop, HARNESS_PREFIX};
use anyhow::{bail, Context};
use logit_core::{interner, Event, MetricKind, Value};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::time::Duration;

/// The runtime's uniform per-component metrics this reads (`docs/design/internal-telemetry.md`'s
/// "Two layers of instrumentation").
pub(crate) const EVENTS_RECEIVED: &str = "logit.component.events.received";
const EVENTS_SENT: &str = "logit.component.events.sent";
const BATCHES_RECEIVED: &str = "logit.component.batches.received";
const BATCHES_SENT: &str = "logit.component.batches.sent";
const PROCESS_DURATION: &str = "logit.component.process.duration";
const SEND_BLOCKED_DURATION: &str = "logit.component.send.blocked.duration";
const SEND_DURATION: &str = "logit.component.send.duration";
const BUFFER_UTILIZATION: &str = "logit.component.buffer.utilization";
const EVENTS_DROPPED: &str = "logit.component.events.dropped";
const BATCHES_DROPPED: &str = "logit.component.batches.dropped";

pub struct AttributeArgs {
    pub scenario: String,
    /// The appended `internal`'s drain interval: shorter captures more of the run before the final
    /// drain, but each drain is work inside the measured process.
    pub interval: Duration,
    pub settle: Duration,
    pub timeout: Duration,
    pub shutdown_timeout: Duration,
    pub no_build: bool,
    pub profile: String,
    /// Measure this binary instead of building one, as `run::RunArgs::logit_bin`.
    pub logit_bin: Option<std::path::PathBuf>,
    /// Sender CPU pinning; only a driven scenario has a sender. See [`crate::load::CpuSet`].
    pub pin_sender: Option<CpuSet>,
    pub pin_child: Option<CpuSet>,
}

pub fn attribute(root: &Path, args: AttributeArgs) -> anyhow::Result<()> {
    if args.interval < Duration::from_millis(1) {
        bail!("--interval must be at least 1ms");
    }
    let scenarios_dir = root.join("perf/scenarios");
    let scenario = scenario::find(&scenarios_dir, &args.scenario)?;
    // A fresh spool, as `run` clears per repeat (`crate::spool`).
    crate::spool::clear(root, &scenario)?;
    let logit_bin =
        run::build_and_locate(root, &args.profile, args.no_build, args.logit_bin.as_deref())?;

    let workdir = telemetry_leg::make_workdir("attribute")?;
    let dump_path = workdir.join("attribute.native");
    // A stale dump at this pid would decode as this run's frames and inflate every total. Refused
    // rather than deleted: its presence means an assumption here is wrong.
    if dump_path.exists() {
        bail!(
            "{} already exists -- a previous `attribute` run left it behind; remove it (or its \
             whole directory) and try again",
            dump_path.display()
        );
    }

    let outcome = attribute_in(&logit_bin, &scenario, &args, &dump_path);
    match &outcome {
        // Only on success: a failed run's partial dump is kept for debugging. The rewritten
        // scenario is removed either way; it's reproducible from the original.
        Ok(()) => {
            let _ = fs::remove_dir_all(&workdir);
        }
        Err(_) => eprintln!("note: the dump is left at {}", dump_path.display()),
    }
    outcome
}

fn attribute_in(
    logit_bin: &Path,
    scenario: &Scenario,
    args: &AttributeArgs,
    dump_path: &Path,
) -> anyhow::Result<()> {
    let source = fs::read_to_string(&scenario.path)
        .with_context(|| format!("reading {}", scenario.path.display()))?;
    let rewritten = telemetry_leg::rewrite_scenario(&source, args.interval, dump_path)
        .with_context(|| format!("rewriting {}", scenario.path.display()))?;
    let config_path = telemetry_leg::rewritten_config_path(scenario, "attribute")?;
    let _cleanup = RemoveOnDrop(config_path.clone());
    fs::write(&config_path, &rewritten)
        .with_context(|| format!("writing {}", config_path.display()))?;

    telemetry_leg::validate(logit_bin, &config_path)?;

    // Rendered before spawning, so a broken spec fails before a full startup.
    let plan = match &scenario.workload {
        Workload::Generated { .. } => None,
        Workload::Driven(_) => Some(LoadPlan::build(&scenario.load_spec_path()?, &source)?),
    };
    let drive = match (&plan, &scenario.workload) {
        (Some(plan), _) => Drive::Driven { plan, pin_sender: args.pin_sender.as_ref() },
        (None, Workload::Generated { count }) => Drive::Generated { count: *count },
        (None, Workload::Driven(_)) => unreachable!("a driven workload always builds a plan"),
    };

    println!(
        "-- {} ({}, internal interval={})",
        scenario.name,
        scenario.workload.describe(),
        telemetry_leg::format_interval(args.interval)
    );
    let measured = run::spawn_and_measure(SpawnConfig {
        logit_bin,
        wrapper: &[],
        config: &config_path,
        drive,
        pin_child: args.pin_child.as_ref(),
        // The appended `internal`'s drain loop runs until shutdown.
        needs_sigterm: true,
        settle: args.settle,
        timeout: args.timeout,
        shutdown_timeout: args.shutdown_timeout,
    })?;
    if let Some(load) = measured.load {
        println!(
            "   sent {} datagrams ({} lines, {:.1} MiB) in {:.2}s",
            load.sent_datagrams,
            load.sent_lines,
            load.sent_bytes as f64 / (1024.0 * 1024.0),
            load.elapsed.as_secs_f64(),
        );
    }

    let events = telemetry_leg::decode_dump(dump_path, false)?;
    let nodes = aggregate(&events);
    if nodes.is_empty() {
        bail!(
            "the dump decoded to no per-component points at all -- {} events, none carrying a \
             `component` attribute",
            events.len()
        );
    }
    // Denominated over the peak node's `events.received`, not the sink's (`peak_received`). Not a
    // `Sample`: with the leg attached, this graph has two nodes `run` doesn't measure.
    let delivered = peak_received(&nodes);
    println!(
        "   {:.0} events/s, {:.3} us/event, {:.1} MiB peak RSS (with the attribution leg attached)",
        delivered as f64 / measured.wall().as_secs_f64(),
        measured.cpu_us_per_event(delivered),
        measured.usage.max_rss_bytes as f64 / (1024.0 * 1024.0),
    );

    print_table(&nodes);
    println!();
    for line in check_counts(&nodes, &scenario.workload) {
        println!("{line}");
    }
    for line in verdict(&nodes, &consumer_map(&source)?) {
        println!("{line}");
    }
    Ok(())
}

/// Producer id to the ids of every component listing it in `sources:`, read from the original
/// scenario (no dump leg). Lets the verdict name the consumers a blocked producer waited on.
fn consumer_map(yaml: &str) -> anyhow::Result<BTreeMap<String, Vec<String>>> {
    let value: serde_norway::Value = serde_norway::from_str(yaml).context("parsing YAML")?;
    let components = value
        .get("components")
        .and_then(serde_norway::Value::as_mapping)
        .context("no top-level `components` mapping")?;
    let mut map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (id, component) in components {
        let Some(id) = id.as_str() else { continue };
        let Some(sources) = component.get("sources").and_then(serde_norway::Value::as_sequence)
        else {
            continue;
        };
        for source in sources {
            if let Some(source) = source.as_str() {
                map.entry(source.to_string()).or_default().push(id.to_string());
            }
        }
    }
    Ok(map)
}

/// A Σ over a drained `Distribution`: total seconds and observation count.
///
/// Both come off the sketch (`DdSketch::sum`/`count`); the sum is exact, not a quantile estimate.
/// `internal` merges every timing at one `(name, tags)` key into one sketch per drain, so summing
/// across drains gives the whole run's total.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct Total {
    pub secs: f64,
    pub count: u64,
}

impl Total {
    fn add(&mut self, secs: f64, count: usize) {
        self.secs += secs;
        self.count += count as u64;
    }
}

/// One node's whole picture, folded over every drain in the dump.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct NodeStats {
    /// The component kind (`json`, `null_out`, ...) and role (`listener`/`transform`/`sink`) the
    /// drain stamps alongside `component`.
    pub kind: String,
    pub role: String,
    pub events_received: f64,
    pub events_sent: f64,
    pub batches_received: f64,
    pub batches_sent: f64,
    pub process: Total,
    pub send_blocked: Total,
    pub send: Total,
    /// The high-water mark of this sink's queue fill, not an average: averaging hides a queue that
    /// was briefly full.
    pub buffer_utilization: Option<f64>,
    /// `reason` tag to total, per reason because `overflow_oldest` and `send_failed` are different
    /// bugs.
    pub events_dropped: BTreeMap<String, f64>,
    pub batches_dropped: BTreeMap<String, f64>,
}

impl NodeStats {
    fn is_harness(id: &str) -> bool {
        id.starts_with(HARNESS_PREFIX)
    }
}

/// Groups every decoded point by its `component` attribute. Events carrying no `component` (the
/// process-level `logit.process.*` gauges) are skipped -- they belong to no node.
pub fn aggregate(events: &[Event]) -> BTreeMap<String, NodeStats> {
    let mut nodes: BTreeMap<String, NodeStats> = BTreeMap::new();
    for event in events {
        let Some(component) = event.attributes.get("component").and_then(Value::as_str) else {
            continue;
        };
        if event.metrics.is_empty() {
            continue;
        }
        let node = nodes.entry(component.to_string()).or_default();
        if node.kind.is_empty() {
            if let Some(kind) = event.attributes.get("kind").and_then(Value::as_str) {
                node.kind = kind.to_string();
            }
        }
        if node.role.is_empty() {
            if let Some(role) = event.attributes.get("role").and_then(Value::as_str) {
                node.role = role.to_string();
            }
        }
        let reason = event.attributes.get("reason").and_then(Value::as_str).unwrap_or("unknown");
        for metric in &event.metrics {
            fold_metric(node, interner::resolve(metric.name), &metric.kind, reason);
        }
    }
    nodes
}

fn fold_metric(node: &mut NodeStats, name: &str, kind: &MetricKind, reason: &str) {
    match name {
        EVENTS_RECEIVED => node.events_received += sum_value(kind),
        EVENTS_SENT => node.events_sent += sum_value(kind),
        BATCHES_RECEIVED => node.batches_received += sum_value(kind),
        BATCHES_SENT => node.batches_sent += sum_value(kind),
        PROCESS_DURATION => add_distribution(&mut node.process, kind),
        SEND_BLOCKED_DURATION => add_distribution(&mut node.send_blocked, kind),
        SEND_DURATION => add_distribution(&mut node.send, kind),
        BUFFER_UTILIZATION => {
            if let MetricKind::Gauge(v) = kind {
                node.buffer_utilization =
                    Some(node.buffer_utilization.map_or(*v, |seen| seen.max(*v)));
            }
        }
        EVENTS_DROPPED => {
            *node.events_dropped.entry(reason.to_string()).or_default() += sum_value(kind);
        }
        BATCHES_DROPPED => {
            *node.batches_dropped.entry(reason.to_string()).or_default() += sum_value(kind);
        }
        _ => {}
    }
}

/// A counter's value. `internal` drains a count as a `Sum`; anything else is ignored rather than
/// coerced, since reading a gauge as a count would inflate a total.
fn sum_value(kind: &MetricKind) -> f64 {
    match kind {
        MetricKind::Sum(sum) => sum.value,
        _ => 0.0,
    }
}

fn add_distribution(total: &mut Total, kind: &MetricKind) {
    if let MetricKind::Distribution(sketch) = kind {
        total.add(sketch.sum(), sketch.count());
    }
}

fn print_table(nodes: &BTreeMap<String, NodeStats>) {
    let rows = by_process_time(nodes);
    println!(
        "\n{:<18} {:<14} {:<10} {:>11} {:>11} {:>9} {:>9} {:>10} {:>10} {:>10} {:>8}",
        "node",
        "kind",
        "role",
        "events in",
        "events out",
        "batch in",
        "batch out",
        "process s",
        "blocked s",
        "send s",
        "buf max"
    );
    for (id, node) in &rows {
        println!(
            "{:<18} {:<14} {:<10} {:>11.0} {:>11.0} {:>9.0} {:>9.0} {:>10.4} {:>10.4} {:>10.4} {:>8}",
            id,
            node.kind,
            node.role,
            node.events_received,
            node.events_sent,
            node.batches_received,
            node.batches_sent,
            node.process.secs,
            node.send_blocked.secs,
            node.send.secs,
            node.buffer_utilization.map(|v| format!("{v:.2}")).unwrap_or_else(|| "-".to_string()),
        );
    }
    for (id, node) in &rows {
        for (reason, count) in node.events_dropped.iter().filter(|(_, n)| **n > 0.0) {
            println!("   DROPPED {count:.0} events at `{id}` (reason={reason})");
        }
        for (reason, count) in node.batches_dropped.iter().filter(|(_, n)| **n > 0.0) {
            println!("   DROPPED {count:.0} batches at `{id}` (reason={reason})");
        }
    }
}

/// Every node, hottest first, ties broken on id so the table is deterministic.
fn by_process_time(nodes: &BTreeMap<String, NodeStats>) -> Vec<(&String, &NodeStats)> {
    let mut rows: Vec<(&String, &NodeStats)> = nodes.iter().collect();
    rows.sort_by(|(a_id, a), (b_id, b)| {
        b.process
            .secs
            .partial_cmp(&a.process.secs)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a_id.cmp(b_id))
    });
    rows
}

/// The one-line answer, plus a line per back-pressured producer.
///
/// The largest Σ `process.duration` is the node doing the most work itself. Σ
/// `send.blocked.duration` is a node that couldn't hand work on: the constraint is downstream, so
/// each such line names the consumers. The harness's own nodes are excluded from both.
pub fn verdict(
    nodes: &BTreeMap<String, NodeStats>,
    consumers: &BTreeMap<String, Vec<String>>,
) -> Vec<String> {
    let measured: Vec<(&String, &NodeStats)> =
        nodes.iter().filter(|(id, _)| !NodeStats::is_harness(id)).collect();
    let total: f64 = measured.iter().map(|(_, n)| n.process.secs).sum();

    let mut lines = Vec::new();
    match measured.iter().filter(|(_, n)| n.process.secs > 0.0).max_by(|(a_id, a), (b_id, b)| {
        a.process
            .secs
            .partial_cmp(&b.process.secs)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b_id.cmp(a_id))
    }) {
        Some((id, node)) => lines.push(format!(
            "verdict: `{id}` ({}) has the largest Σ process time -- {:.4}s of {total:.4}s \
             ({:.0}%) across every measured node",
            node.kind,
            node.process.secs,
            100.0 * node.process.secs / total,
        )),
        None => lines.push(
            "verdict: no node recorded any process time -- only sinks and listeners are \
             instrumented for it, or the run was shorter than one --interval"
                .to_string(),
        ),
    }

    let mut blocked: Vec<(&String, &NodeStats)> =
        measured.iter().copied().filter(|(_, n)| n.send_blocked.secs > 0.0).collect();
    blocked.sort_by(|(a_id, a), (b_id, b)| {
        b.send_blocked
            .secs
            .partial_cmp(&a.send_blocked.secs)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a_id.cmp(b_id))
    });
    for (id, node) in blocked {
        let downstream = consumers
            .get(id.as_str())
            .filter(|ids| !ids.is_empty())
            .map(|ids| ids.iter().map(|id| format!("`{id}`")).collect::<Vec<_>>().join(", "))
            .unwrap_or_else(|| "its consumers".to_string());
        lines.push(format!(
            "         `{id}` spent {:.4}s blocked in send -- {downstream} could not keep up with \
             it, so the constraint is downstream of `{id}`",
            node.send_blocked.secs
        ));
    }
    lines
}

/// The largest `events.received` any node under test recorded, excluding the harness's nodes.
///
/// **A summary, not `run`'s denominator.** Where a transform drops events, the peak node is the
/// one before the drop: right for describing where work happened, wrong for events/s, which
/// `crate::run` takes from [`delivered_at_sink`].
pub fn peak_received(nodes: &BTreeMap<String, NodeStats>) -> u64 {
    nodes
        .iter()
        .filter(|(id, _)| !NodeStats::is_harness(id))
        .map(|(_, node)| node.events_received)
        .fold(0.0_f64, f64::max) as u64
}

/// `events.received` at the graph's terminal sink: the denominator of a driven scenario's
/// `events_per_s`/`cpu_us_per_event` and of `--verify`'s exact count.
///
/// Selected by the drain's `role` attribute, not a maximum: the maximum is the shallowest
/// receiver, which overstates delivery by whatever a filtering transform drops.
///
/// With several sinks the load spec's `sink:` must name one; with one sink it's omitted.
pub fn delivered_at_sink(
    nodes: &BTreeMap<String, NodeStats>,
    named: Option<&str>,
) -> anyhow::Result<u64> {
    if let Some(named) = named {
        let node = nodes.get(named).with_context(|| {
            format!(
                "the load spec names `{named}` as its sink, but the run's telemetry has no such \
                 component -- known: {}",
                render_ids(nodes.keys().filter(|id| !NodeStats::is_harness(id)))
            )
        })?;
        return Ok(node.events_received as u64);
    }

    let sinks: Vec<(&String, &NodeStats)> = nodes
        .iter()
        .filter(|(id, node)| !NodeStats::is_harness(id) && node.role == "sink")
        .collect();
    match sinks.as_slice() {
        [(_, sink)] => Ok(sink.events_received as u64),
        [] => bail!(
            "the run's telemetry shows no sink at all (nodes: {}) -- a driven scenario needs one \
             to denominate events/s and CPU/event over",
            render_ids(nodes.keys().filter(|id| !NodeStats::is_harness(id)))
        ),
        several => bail!(
            "this scenario has {} sinks ({}), so `events delivered` is ambiguous -- name the one \
             to denominate over with `sink: <id>` in its load spec under perf/load/",
            several.len(),
            render_ids(several.iter().map(|(id, _)| *id))
        ),
    }
}

fn render_ids<'a>(ids: impl Iterator<Item = &'a String>) -> String {
    let ids: Vec<&str> = ids.map(String::as_str).collect();
    if ids.is_empty() {
        "none".to_string()
    } else {
        ids.join(", ")
    }
}

/// Checks the decoded counters against what the scenario said it would produce; a breakdown that
/// doesn't add up describes some other run. Returns a line of what was seen plus a warning per
/// mismatch.
///
/// For a [`Workload::Generated`] scenario, the generator's `events.sent` must equal `count`: it's
/// the `generation complete` number by a separate path, so a mismatch means missing drains (the
/// final drain on shutdown normally closes the gap). The peak `events.received` equals `count`
/// unless the graph drops or collapses events, as `aggregate` does.
///
/// A [`Workload::Driven`] scenario gets neither check: it has no generator, loss is expected, and
/// `run` checks the accounting against `logit.input.kernel.drops`, which this table lacks.
pub fn check_counts(nodes: &BTreeMap<String, NodeStats>, workload: &Workload) -> Vec<String> {
    let measured: Vec<(&String, &NodeStats)> =
        nodes.iter().filter(|(id, _)| !NodeStats::is_harness(id)).collect();
    let peak = peak_received(nodes);

    let count = match workload {
        Workload::Driven(spec) => {
            return vec![format!(
                "events: peak node received {peak}, driven by {} datagrams over a real socket -- \
                 losses are expected here and are checked by `run`, not by this table",
                spec.datagrams
            )]
        }
        Workload::Generated { count } => *count,
    };

    let generated: f64 =
        measured.iter().filter(|(_, n)| n.kind == "generate_in").map(|(_, n)| n.events_sent).sum();

    let mut lines = vec![format!(
        "events: generator sent {generated:.0}, peak node received {peak}, scenario count {count}"
    )];
    if generated != count as f64 {
        lines.push(format!(
            "warning: the generator's `events.sent` ({generated:.0}) does not equal the \
             scenario's count ({count}) -- the dump is missing drains, so every Σ below is an \
             undercount"
        ));
    }
    if peak != count {
        lines.push(format!(
            "warning: no node received all {count} events (peak {peak}) -- expected \
             for a graph that collapses events (`aggregate`), a missing drain otherwise"
        ));
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::load::LoadSpec;
    use logit_core::{AttrMap, DdSketch, MetricRecord, Sum, Temporality};

    #[test]
    fn consumer_map_inverts_the_sources_edges() {
        let yaml = "components:\n  gen:\n    type: generate_in\n    count: 1\n  a:\n    type: json\n    sources: [gen]\n  b:\n    type: null_out\n    sources: [gen, a]\n";
        let map = consumer_map(yaml).unwrap();
        assert_eq!(map.get("gen").unwrap(), &vec!["a".to_string(), "b".to_string()]);
        assert_eq!(map.get("a").unwrap(), &vec!["b".to_string()]);
        assert!(!map.contains_key("b"), "a terminal sink has no consumers");
    }

    /// Builds the event one `internal` drain would emit for a single point: the metric, under the
    /// `component`/`kind`/`role` identity attributes `ComponentBuffer::drain` stamps on every one
    /// of them.
    fn point(
        component: &str,
        kind_name: &str,
        role: &str,
        metric: &str,
        value: MetricKind,
    ) -> Event {
        let mut attrs = AttrMap::new();
        attrs.insert("component", component);
        attrs.insert("kind", kind_name);
        attrs.insert("role", role);
        Event::metric(0, attrs, MetricRecord::new(interner::intern(metric), value))
    }

    fn tagged_point(
        component: &str,
        kind_name: &str,
        role: &str,
        metric: &str,
        reason: &str,
        value: MetricKind,
    ) -> Event {
        let mut attrs = AttrMap::new();
        attrs.insert("component", component);
        attrs.insert("kind", kind_name);
        attrs.insert("role", role);
        attrs.insert("reason", reason);
        Event::metric(0, attrs, MetricRecord::new(interner::intern(metric), value))
    }

    fn timing(values: &[f64]) -> MetricKind {
        let mut sketch = DdSketch::new();
        for value in values {
            sketch.add(*value);
        }
        MetricKind::Distribution(sketch)
    }

    #[test]
    fn aggregate_groups_points_by_component_and_sums_across_drains() {
        // Two drains' worth: the counters and the sketches must accumulate, not overwrite.
        let events = vec![
            point("json", "json", "transform", EVENTS_RECEIVED, MetricKind::counter(600.0)),
            point("json", "json", "transform", EVENTS_RECEIVED, MetricKind::counter(400.0)),
            point("json", "json", "transform", EVENTS_SENT, MetricKind::counter(1_000.0)),
            point("json", "json", "transform", BATCHES_RECEIVED, MetricKind::counter(10.0)),
            point("json", "json", "transform", BATCHES_SENT, MetricKind::counter(10.0)),
            point("json", "json", "transform", PROCESS_DURATION, timing(&[0.2, 0.3])),
            point("json", "json", "transform", PROCESS_DURATION, timing(&[0.5])),
            point("json", "json", "transform", SEND_BLOCKED_DURATION, timing(&[0.1])),
            point("out", "null_out", "sink", EVENTS_RECEIVED, MetricKind::counter(1_000.0)),
            point("out", "null_out", "sink", SEND_DURATION, timing(&[0.01, 0.02])),
            point("out", "null_out", "sink", BUFFER_UTILIZATION, MetricKind::Gauge(0.25)),
            point("out", "null_out", "sink", BUFFER_UTILIZATION, MetricKind::Gauge(0.75)),
            point("out", "null_out", "sink", BUFFER_UTILIZATION, MetricKind::Gauge(0.10)),
        ];
        let nodes = aggregate(&events);

        assert_eq!(nodes.len(), 2);
        let json = &nodes["json"];
        assert_eq!(json.kind, "json");
        assert_eq!(json.role, "transform");
        assert_eq!(json.events_received, 1_000.0);
        assert_eq!(json.events_sent, 1_000.0);
        assert_eq!(json.batches_received, 10.0);
        assert_eq!(json.batches_sent, 10.0);
        // `DdSketch::sum` is exact, so this is an equality assertion, not an epsilon one.
        assert!((json.process.secs - 1.0).abs() < 1e-9, "{}", json.process.secs);
        assert_eq!(json.process.count, 3);
        assert!((json.send_blocked.secs - 0.1).abs() < 1e-9);

        let out = &nodes["out"];
        assert_eq!(out.role, "sink");
        assert_eq!(out.process, Total::default(), "a sink records no process.duration");
        assert_eq!(out.send.count, 2);
        assert_eq!(
            out.buffer_utilization,
            Some(0.75),
            "the high-water mark, not the last or the mean"
        );
    }

    #[test]
    fn aggregate_skips_process_level_points_that_name_no_component() {
        let mut attrs = AttrMap::new();
        attrs.insert("host", "any");
        let events = vec![Event::metric(
            0,
            attrs,
            MetricRecord::new(interner::intern("logit.process.uptime"), MetricKind::Gauge(5.0)),
        )];
        assert!(aggregate(&events).is_empty());
    }

    #[test]
    fn aggregate_keeps_drops_apart_by_reason() {
        let events = vec![
            tagged_point(
                "out",
                "null_out",
                "sink",
                EVENTS_DROPPED,
                "overflow_oldest",
                MetricKind::counter(12.0),
            ),
            tagged_point(
                "out",
                "null_out",
                "sink",
                EVENTS_DROPPED,
                "send_failed",
                MetricKind::counter(3.0),
            ),
            tagged_point(
                "out",
                "null_out",
                "sink",
                BATCHES_DROPPED,
                "send_failed",
                MetricKind::counter(1.0),
            ),
        ];
        let nodes = aggregate(&events);
        let out = &nodes["out"];
        assert_eq!(out.events_dropped["overflow_oldest"], 12.0);
        assert_eq!(out.events_dropped["send_failed"], 3.0);
        assert_eq!(out.batches_dropped["send_failed"], 1.0);
    }

    #[test]
    fn a_cumulative_sum_is_not_mistaken_for_a_counter() {
        // `internal` only ever drains a count as a delta/monotonic `Sum`; a gauge or a cumulative
        // sum arriving under a counter's name is ignored rather than folded in.
        let events = vec![
            point("out", "null_out", "sink", EVENTS_RECEIVED, MetricKind::Gauge(99.0)),
            point(
                "out",
                "null_out",
                "sink",
                EVENTS_RECEIVED,
                MetricKind::Sum(Sum {
                    value: 7.0,
                    temporality: Temporality::Cumulative,
                    monotonic: true,
                }),
            ),
        ];
        // The cumulative `Sum` still reads as a sum -- what's excluded is the gauge.
        assert_eq!(aggregate(&events)["out"].events_received, 7.0);
    }

    fn node(kind: &str, role: &str, process: f64, blocked: f64) -> NodeStats {
        NodeStats {
            kind: kind.to_string(),
            role: role.to_string(),
            process: Total { secs: process, count: 1 },
            send_blocked: Total { secs: blocked, count: 1 },
            ..NodeStats::default()
        }
    }

    #[test]
    fn verdict_names_the_node_with_the_largest_process_time() {
        let nodes = BTreeMap::from([
            ("json".to_string(), node("json", "transform", 3.0, 0.0)),
            ("metrics".to_string(), node("kv_metrics", "transform", 1.0, 0.0)),
            ("out".to_string(), node("null_out", "sink", 0.0, 0.0)),
        ]);
        let lines = verdict(&nodes, &BTreeMap::new());
        assert_eq!(lines.len(), 1);
        assert!(lines[0].starts_with("verdict: `json` (json) has the largest"), "{}", lines[0]);
        assert!(lines[0].contains("3.0000s of 4.0000s (75%)"), "{}", lines[0]);
    }

    #[test]
    fn verdict_excludes_the_harness_own_nodes_from_the_comparison() {
        let nodes = BTreeMap::from([
            ("json".to_string(), node("json", "transform", 1.0, 0.0)),
            ("__perf_dump".to_string(), node("file_out", "sink", 99.0, 0.0)),
        ]);
        let lines = verdict(&nodes, &BTreeMap::new());
        assert!(lines[0].contains("`json`"), "{}", lines[0]);
        assert!(!lines[0].contains("__perf"), "{}", lines[0]);
    }

    #[test]
    fn verdict_points_at_the_consumer_when_a_producer_blocked_on_send() {
        let nodes = BTreeMap::from([
            ("gen".to_string(), node("generate_in", "listener", 0.0, 2.5)),
            ("out".to_string(), node("null_out", "sink", 1.0, 0.0)),
        ]);
        let consumers = BTreeMap::from([("gen".to_string(), vec!["out".to_string()])]);
        let lines = verdict(&nodes, &consumers);
        assert_eq!(lines.len(), 2);
        assert!(lines[1].contains("`gen` spent 2.5000s blocked in send"), "{}", lines[1]);
        assert!(lines[1].contains("`out` could not keep up"), "{}", lines[1]);
    }

    #[test]
    fn verdict_says_so_plainly_when_nothing_recorded_process_time() {
        let nodes = BTreeMap::from([("out".to_string(), node("null_out", "sink", 0.0, 0.0))]);
        let lines = verdict(&nodes, &BTreeMap::new());
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("no node recorded any process time"), "{}", lines[0]);
    }

    fn counted(kind: &str, sent: f64, received: f64) -> NodeStats {
        NodeStats {
            kind: kind.to_string(),
            events_sent: sent,
            events_received: received,
            ..NodeStats::default()
        }
    }

    #[test]
    fn check_counts_is_quiet_when_the_totals_match_the_scenario_count() {
        let nodes = BTreeMap::from([
            ("gen".to_string(), counted("generate_in", 100.0, 0.0)),
            ("out".to_string(), counted("null_out", 0.0, 100.0)),
            // The harness's own nodes carry their own, unrelated counts.
            ("__perf_dump".to_string(), counted("file_out", 0.0, 7.0)),
        ]);
        let lines = check_counts(&nodes, &Workload::Generated { count: 100 });
        assert_eq!(lines.len(), 1, "no warnings: {lines:?}");
        assert!(lines[0].contains("generator sent 100"), "{}", lines[0]);
        assert!(lines[0].contains("peak node received 100"), "{}", lines[0]);
    }

    #[test]
    fn check_counts_warns_when_a_drain_went_missing() {
        let nodes = BTreeMap::from([
            ("gen".to_string(), counted("generate_in", 80.0, 0.0)),
            ("out".to_string(), counted("null_out", 0.0, 80.0)),
        ]);
        let lines = check_counts(&nodes, &Workload::Generated { count: 100 });
        assert_eq!(lines.len(), 3);
        assert!(lines[1].contains("does not equal the scenario's count"), "{}", lines[1]);
        assert!(lines[2].contains("no node received all 100 events"), "{}", lines[2]);
    }

    #[test]
    fn check_counts_reports_a_driven_scenario_without_warning_about_its_losses() {
        let nodes = BTreeMap::from([
            ("statsd".to_string(), counted("statsd_in", 900.0, 0.0)),
            ("out".to_string(), counted("null_out", 0.0, 900.0)),
        ]);
        let spec: LoadSpec = serde_norway::from_str(
            "target: statsd\ndatagrams: 1000\nmodel: m.yaml\ndatagram_mix:\n  - { weight: 1, single: true }\n",
        )
        .unwrap();
        let lines = check_counts(&nodes, &Workload::Driven(spec));
        assert_eq!(lines.len(), 1, "a driven scenario's losses are not a warning here: {lines:?}");
        assert!(lines[0].contains("peak node received 900"), "{}", lines[0]);
        assert!(lines[0].contains("1000 datagrams"), "{}", lines[0]);
    }

    #[test]
    fn peak_received_ignores_the_harness_own_nodes() {
        let nodes = BTreeMap::from([
            ("out".to_string(), counted("null_out", 0.0, 42.0)),
            ("__perf_dump".to_string(), counted("file_out", 0.0, 9_999.0)),
        ]);
        assert_eq!(peak_received(&nodes), 42);
    }

    fn roled(kind: &str, role: &str, received: f64) -> NodeStats {
        NodeStats {
            kind: kind.to_string(),
            role: role.to_string(),
            events_received: received,
            ..NodeStats::default()
        }
    }

    /// A middle node receiving more than the sink: the denominator must be the sink's count.
    #[test]
    fn delivered_at_sink_reports_the_sink_not_the_busiest_node() {
        let nodes = BTreeMap::from([
            ("statsd".to_string(), roled("statsd_in", "listener", 0.0)),
            // A filtering transform: 1,000 in, 400 dropped, 600 out.
            ("keep".to_string(), roled("keep", "transform", 1_000.0)),
            ("out".to_string(), roled("null_out", "sink", 600.0)),
            ("__perf_dump".to_string(), roled("file_out", "sink", 9_999.0)),
        ]);
        assert_eq!(delivered_at_sink(&nodes, None).unwrap(), 600);
        assert_eq!(peak_received(&nodes), 1_000, "the table still wants the busiest node");
    }

    #[test]
    fn delivered_at_sink_refuses_to_guess_between_several_sinks() {
        let nodes = BTreeMap::from([
            ("statsd".to_string(), roled("statsd_in", "listener", 0.0)),
            ("a".to_string(), roled("null_out", "sink", 600.0)),
            ("b".to_string(), roled("null_out", "sink", 400.0)),
        ]);
        let err = delivered_at_sink(&nodes, None).expect_err("two sinks, no single denominator");
        let err = format!("{err:#}");
        assert!(err.contains("2 sinks"), "{err}");
        assert!(err.contains("sink: <id>"), "{err}");

        // ...and takes the answer when the load spec gives one.
        assert_eq!(delivered_at_sink(&nodes, Some("b")).unwrap(), 400);
    }

    #[test]
    fn delivered_at_sink_reports_a_named_sink_that_is_not_in_the_run() {
        let nodes = BTreeMap::from([("out".to_string(), roled("null_out", "sink", 600.0))]);
        let err = delivered_at_sink(&nodes, Some("typo"))
            .expect_err("a spec naming a component the run doesn't have");
        let err = format!("{err:#}");
        assert!(err.contains("no such component"), "{err}");
        assert!(err.contains("known: out"), "{err}");
    }

    #[test]
    fn delivered_at_sink_says_so_when_the_graph_has_no_sink_at_all() {
        let nodes = BTreeMap::from([
            ("statsd".to_string(), roled("statsd_in", "listener", 0.0)),
            // The harness's own sink is excluded, so this graph has none of its own.
            ("__perf_dump".to_string(), roled("file_out", "sink", 9.0)),
        ]);
        let err = delivered_at_sink(&nodes, None).expect_err("no sink under test");
        assert!(format!("{err:#}").contains("no sink at all"), "{err:#}");
    }
}
