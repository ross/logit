//! `logit-perf attribute`: where a scenario's time actually goes, per node.
//!
//! The measurement `run` produces is one number for a whole graph. This answers the next
//! question -- *which node* -- without a profiler, out of numbers the runtime already records
//! about itself: `logit.component.process.duration`, `.send.blocked.duration`, `.send.duration`,
//! the received/sent counters, and the sink queues' `buffer.utilization`, each stamped with the
//! emitting component's `component`/`kind`/`role` (`docs/design/internal-telemetry.md`).
//!
//! Getting at them takes a temporary leg through the graph, appended to a **copy** of the
//! scenario in a temp directory (the shipped file is never touched):
//!
//! ```yaml
//!   __perf_internal: { type: internal, interval: 1s, span_sample_rate: 0.0, logs: off }
//!   __perf_dump: { type: file_out, sources: [__perf_internal], path: <tmp>/attribute.native,
//!                  format: native, rotate: { max_bytes: "1024GiB" } }
//! ```
//!
//! Then the dump is decoded with the very codec that wrote it --
//! [`logit_proto::frame::read_frame`] in a loop over the file, each frame's payload through
//! [`logit_proto::native::decode_batch`], which is exactly what `format: native` writes per batch
//! (`logit_outputs::stdio::StreamEncoder::Native` -> `NativeEncoder::encode` ->
//! `write_frame(CODEC_NATIVE_V1, .., encode_batch(batch))`). `native` is the only format that can
//! be read back byte-exactly: `human` is a render meant for a person, and there is no `json`
//! stream format at all (`StreamFormat` is `Human | Native`) -- see
//! `docs/adr/load-test-harness.md`'s "Per-node attribution" section.
//!
//! **The append is textual, never a parse-and-reserialize.** A scenario is round-tripped through
//! [`logit_config::Config`] nowhere in this crate: that would resolve `!env` (which a scenario
//! never uses, but which would then have to *exist* to run the harness), normalize every default
//! into the file, and couple this tool to the config crate for no gain. The YAML is parsed as a
//! bare [`serde_norway::Value`] only to *check* it -- rule 13 allows at most one `internal` per
//! config, so a scenario that already has one is refused rather than rewritten into a config the
//! binary would reject.
//!
//! **A graph with an `internal` in it never self-exits** -- the drain ticker runs until shutdown
//! -- so this always takes `run`'s settle-then-SIGTERM path, never the wait-for-exit one. That
//! SIGTERM is also what makes the numbers whole: `InternalInput::run_until_shutdown` drains once
//! more on the way out (`docs/design/internal-telemetry.md`, "Shutdown drains once more"), so the
//! last partial interval -- on a 5-second scenario, a fifth of the run -- lands in the dump
//! instead of being thrown away.

use crate::run::{self, SpawnConfig};
use crate::scenario::{self, Scenario};
use anyhow::{bail, Context};
use bytes::Bytes;
use logit_core::{interner, Event, MetricKind, Value};
use logit_proto::frame::read_frame;
use logit_proto::native::{decode_batch, CODEC_NATIVE_V1};
use logit_proto::CodecError;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

/// The appended components' ids. Prefixed so they can't collide with a scenario's own ids and are
/// recognizable in the output as the harness's own machinery rather than part of the graph under
/// test -- [`HARNESS_PREFIX`] is what the verdict and the count check filter on.
const INTERNAL_ID: &str = "__perf_internal";
const DUMP_ID: &str = "__perf_dump";
const HARNESS_PREFIX: &str = "__perf_";

/// `file_out` must have at least one rotation trigger (graph rule 29 -- neither set would
/// silently never rotate). One is required here, but rotating *at all* mid-dump would split the
/// attribution data across `attribute.native` and `attribute.native.1`, so this is set far above
/// any plausible dump: a scenario's whole self-telemetry stream is kilobytes per drain.
const ROTATE_MAX_BYTES: &str = "1024GiB";

/// The runtime's uniform per-component metrics this reads (`docs/design/internal-telemetry.md`'s
/// "Two layers of instrumentation" tables). Named as constants rather than inline literals
/// because each appears twice -- once in [`fold_metric`], once in a test asserting the fold.
const EVENTS_RECEIVED: &str = "logit.component.events.received";
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
    /// The appended `internal`'s drain cadence. Shorter means more drains, so more of the run is
    /// captured before the final one -- but every drain is itself work inside the process being
    /// measured, so this trades resolution against perturbation.
    pub interval: Duration,
    pub settle: Duration,
    pub timeout: Duration,
    pub shutdown_timeout: Duration,
    pub no_build: bool,
    pub profile: String,
}

pub fn attribute(root: &Path, args: AttributeArgs) -> anyhow::Result<()> {
    if args.interval < Duration::from_millis(1) {
        bail!("--interval must be at least 1ms");
    }
    let scenarios_dir = root.join("perf/scenarios");
    let scenario = scenario::find(&scenarios_dir, &args.scenario)?;
    let logit_bin = run::build_and_locate(root, &args.profile, args.no_build)?;

    let workdir = make_workdir()?;
    let outcome = attribute_in(&logit_bin, &scenario, &args, &workdir);
    match &outcome {
        // Only on success: a failed run's rewritten config and partial dump are the two things
        // anyone debugging the failure would want to look at, so they're left in place and named.
        Ok(()) => {
            let _ = fs::remove_dir_all(&workdir);
        }
        Err(_) => {
            eprintln!("note: the rewritten scenario and its dump are left in {}", workdir.display())
        }
    }
    outcome
}

fn attribute_in(
    logit_bin: &Path,
    scenario: &Scenario,
    args: &AttributeArgs,
    workdir: &Path,
) -> anyhow::Result<()> {
    let source = fs::read_to_string(&scenario.path)
        .with_context(|| format!("reading {}", scenario.path.display()))?;
    let dump_path = workdir.join("attribute.native");
    let rewritten = rewrite_scenario(&source, args.interval, &dump_path)
        .with_context(|| format!("rewriting {}", scenario.path.display()))?;
    let config_path = workdir.join(format!("{}.yaml", scenario.name));
    fs::write(&config_path, &rewritten)
        .with_context(|| format!("writing {}", config_path.display()))?;

    validate(logit_bin, &config_path)?;

    println!(
        "-- {} (count={}, internal interval={})",
        scenario.name,
        scenario.count,
        format_interval(args.interval)
    );
    let sample = run::spawn_and_measure(SpawnConfig {
        logit_bin,
        wrapper: &[],
        config: &config_path,
        count: scenario.count,
        // Always: the appended `internal` listener's drain loop runs until shutdown, so this
        // graph can never exit on its own the way a plain `generate_in -> null_out` one does.
        needs_sigterm: true,
        settle: args.settle,
        timeout: args.timeout,
        shutdown_timeout: args.shutdown_timeout,
    })?;
    println!(
        "   {:.0} events/s, {:.3} us/event, {:.1} MiB peak RSS (with the attribution leg attached)",
        sample.events_per_s,
        sample.cpu_us_per_event,
        sample.max_rss_bytes as f64 / (1024.0 * 1024.0),
    );

    let events = decode_dump(&dump_path)?;
    let nodes = aggregate(&events);
    if nodes.is_empty() {
        bail!(
            "the dump decoded to no per-component points at all -- {} events, none carrying a \
             `component` attribute",
            events.len()
        );
    }
    print_table(&nodes);
    println!();
    for line in check_counts(&nodes, scenario.count) {
        println!("{line}");
    }
    for line in verdict(&nodes, &consumer_map(&source)?) {
        println!("{line}");
    }
    Ok(())
}

/// Runs the built binary's own `logit validate` over the rewritten file before spawning it. The
/// rewrite is textual, so the first thing that would notice a malformed append is `logit run`
/// itself, ~10 seconds into a scenario, as a generic startup failure -- this turns that into an
/// immediate error carrying `validate`'s own message about which component and which rule.
fn validate(logit_bin: &Path, config: &Path) -> anyhow::Result<()> {
    let output = Command::new(logit_bin)
        .arg("validate")
        .arg(config)
        .stdin(Stdio::null())
        .output()
        .with_context(|| format!("spawning {} validate", logit_bin.display()))?;
    if !output.status.success() {
        bail!(
            "the rewritten scenario ({}) does not validate:\n{}{}",
            config.display(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
    Ok(())
}

/// A private scratch directory for one `attribute` run: the rewritten scenario and the dump it
/// writes. Named by pid so two concurrent runs can't share one, created fresh (removed first if a
/// previous run at the same pid left one behind).
fn make_workdir() -> anyhow::Result<PathBuf> {
    let dir = std::env::temp_dir().join(format!("logit-perf-attribute-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    Ok(dir)
}

/// Appends the `internal` + `file_out` dump leg to `yaml`, textually.
///
/// Refuses rather than rewrites when the append couldn't produce a valid config: a scenario that
/// already has an `internal` component (graph rule 13 allows at most one per config -- two would
/// each drain, and so split, the same process-wide `Registry`), one that already uses either of
/// the reserved ids, or one with a top-level key other than `components:`. That last check is
/// what makes appending at the end of the file sound: the two new entries are indented as
/// `components:` members, which is only where they land if nothing else follows it.
fn rewrite_scenario(yaml: &str, interval: Duration, dump_path: &Path) -> anyhow::Result<String> {
    let value: serde_norway::Value = serde_norway::from_str(yaml).context("parsing YAML")?;
    let top = value.as_mapping().context("no top-level mapping")?;
    for (key, _) in top {
        let key = key.as_str().unwrap_or_default();
        if key != "components" {
            bail!(
                "scenario has a top-level `{key}:` key alongside `components:` -- this rewrite \
                 appends its dump leg at the end of the file, which would land under `{key}:` \
                 instead"
            );
        }
    }
    let components = value
        .get("components")
        .and_then(serde_norway::Value::as_mapping)
        .context("no top-level `components` mapping")?;
    for (id, component) in components {
        let id = id.as_str().unwrap_or_default();
        if id == INTERNAL_ID || id == DUMP_ID {
            bail!("scenario already has a component named `{id}`, which this rewrite reserves");
        }
        if component.get("type").and_then(serde_norway::Value::as_str) == Some("internal") {
            bail!(
                "scenario already has an `internal` component (`{id}`) -- graph rule 13 allows at \
                 most one per config, so the harness has nowhere to attach its own. Point that \
                 component at a `file_out` with `format: native` and read the dump directly, or \
                 drop it from the scenario."
            );
        }
    }

    let mut out = yaml.to_string();
    if !out.ends_with('\n') {
        out.push('\n');
    }
    out.push_str(
        "\n  # Appended by `logit-perf attribute` (crates/logit-perf/src/attribute.rs) -- a \
         temporary\n  # copy of this scenario, never the shipped file.\n",
    );
    out.push_str(&format!(
        "  {INTERNAL_ID}: {{ type: internal, interval: {}, span_sample_rate: 0.0, logs: off }}\n",
        format_interval(interval)
    ));
    out.push_str(&format!(
        "  {DUMP_ID}: {{ type: file_out, sources: [{INTERNAL_ID}], path: {}, format: native, \
         rotate: {{ max_bytes: \"{ROTATE_MAX_BYTES}\" }} }}\n",
        yaml_double_quoted(&dump_path.to_string_lossy())
    ));
    Ok(out)
}

/// A `humantime` duration literal for the appended `internal`'s `interval:` --
/// `logit_config`'s own `humantime_serde_duration` is what parses it back. Whole seconds render
/// as seconds, everything else as whole milliseconds; sub-millisecond intervals are rejected by
/// the caller, since there is no finer unit this needs and a rounded-to-zero interval would be a
/// config error rather than a fast one.
fn format_interval(interval: Duration) -> String {
    if interval.subsec_nanos() == 0 {
        format!("{}s", interval.as_secs())
    } else {
        format!("{}ms", interval.as_millis())
    }
}

/// A YAML double-quoted scalar, so a temp-directory path containing a `:` or a leading `#`
/// can't be misread as structure. Only `"` and `\` need escaping in a path -- a path holding a
/// raw control character is rejected outright rather than escaped, since it is far more likely to
/// be a bug in whatever produced it than a path anyone meant.
fn yaml_double_quoted(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 2);
    out.push('"');
    for c in raw.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Producer id -> the ids of every component that lists it in `sources:`. Read off the *original*
/// scenario, so the appended dump leg never appears in it. This is the only topology `attribute`
/// knows: it's what lets the verdict say which consumer a blocked producer was waiting on,
/// instead of an unhelpful "something downstream".
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

/// Reads the whole dump and decodes every frame in it. A file written by `format: native` is a
/// plain concatenation of independently-decodable frames (`logit_proto::native`'s module doc), so
/// this is `read_frame` in a loop over one `Bytes` cursor, each frame's payload handed to
/// `decode_batch` -- the exact inverse of `NativeEncoder::encode`, and the same two calls
/// `NativeDecoder::decode_into` makes.
///
/// A torn *final* frame is a warning, not a failure: the process is SIGTERMed on purpose, and a
/// write interrupted mid-frame leaves a valid prefix followed by a partial one. Every earlier
/// frame decoding cleanly is what matters; a corrupt frame anywhere else fails loudly.
fn decode_dump(path: &Path) -> anyhow::Result<Vec<Event>> {
    let raw = fs::read(path)
        .with_context(|| format!("reading the attribution dump {}", path.display()))?;
    if raw.is_empty() {
        bail!(
            "the attribution dump ({}) is empty -- no drain ever reached it. Is --interval longer \
             than the whole run?",
            path.display()
        );
    }
    let mut bytes = Bytes::from(raw);
    let mut events = Vec::new();
    let mut frames = 0usize;
    while !bytes.is_empty() {
        let (codec, mut payload) = match read_frame(&mut bytes) {
            Ok(frame) => frame,
            Err(CodecError::Truncated { .. }) => {
                eprintln!(
                    "warning: ignoring a torn final frame after {frames} whole ones -- the \
                     process was signalled mid-write"
                );
                break;
            }
            Err(err) => {
                return Err(anyhow::Error::new(err))
                    .with_context(|| format!("frame {frames} of {}", path.display()))
            }
        };
        if codec != CODEC_NATIVE_V1 {
            bail!(
                "frame {frames} of {} declares codec byte {codec}, not native v1 \
                 ({CODEC_NATIVE_V1})",
                path.display()
            );
        }
        let batch = decode_batch(&mut payload)
            .with_context(|| format!("decoding frame {frames} of {}", path.display()))?;
        events.extend(batch.events);
        frames += 1;
    }
    println!("   decoded {frames} native frames, {} points", events.len());
    Ok(events)
}

/// A Σ over a drained `Distribution`: total seconds and how many observations produced them.
/// Both come straight off the sketch (`DdSketch::sum`/`count`) -- the sum is exact, not a
/// quantile estimate, and `internal` merges every timing at one `(name, tags)` key into one
/// sketch per drain, so summing across drains reconstructs the whole run's total.
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
    /// drain stamped alongside `component`. Empty only for a node whose every point somehow
    /// lacked them, which the runtime never produces.
    pub kind: String,
    pub role: String,
    pub events_received: f64,
    pub events_sent: f64,
    pub batches_received: f64,
    pub batches_sent: f64,
    pub process: Total,
    pub send_blocked: Total,
    pub send: Total,
    /// The high-water mark of this sink's queue fill, not an average -- a queue that was briefly
    /// full is the interesting fact, and averaging it away is exactly how backpressure hides.
    pub buffer_utilization: Option<f64>,
    /// `reason` tag -> total, for both drop counters. Kept per-reason because
    /// `overflow_oldest` and `send_failed` are different bugs.
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

/// A counter's value. `internal` drains a `count` as `MetricKind::counter(v)` -- a delta,
/// monotonic `Sum` -- so this is the one shape these ever arrive in; anything else is ignored
/// rather than coerced, since misreading a gauge as a count would silently inflate a total.
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
    println!(
        "\n{:<18} {:<14} {:<10} {:>12} {:>12} {:>11} {:>11} {:>11} {:>8}",
        "node",
        "kind",
        "role",
        "events in",
        "events out",
        "process s",
        "blocked s",
        "send s",
        "buf max"
    );
    for (id, node) in by_process_time(nodes) {
        println!(
            "{:<18} {:<14} {:<10} {:>12.0} {:>12.0} {:>11.4} {:>11.4} {:>11.4} {:>8}",
            id,
            node.kind,
            node.role,
            node.events_received,
            node.events_sent,
            node.process.secs,
            node.send_blocked.secs,
            node.send.secs,
            node.buffer_utilization.map(|v| format!("{v:.2}")).unwrap_or_else(|| "-".to_string()),
        );
    }
    for (id, node) in by_process_time(nodes) {
        for (reason, count) in node.events_dropped.iter().filter(|(_, n)| **n > 0.0) {
            println!("   DROPPED {count:.0} events at `{id}` (reason={reason})");
        }
        for (reason, count) in node.batches_dropped.iter().filter(|(_, n)| **n > 0.0) {
            println!("   DROPPED {count:.0} batches at `{id}` (reason={reason})");
        }
    }
}

/// Every node, hottest first. Ties break on the id so the table is deterministic run to run --
/// two nodes with no recorded process time at all (a listener, say) would otherwise swap places
/// between runs on nothing but map iteration luck.
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
/// Two different things make a node "the bottleneck," and they point in opposite directions. The
/// largest Σ `process.duration` is a node doing the most work itself. Σ `send.blocked.duration`
/// is a node that *couldn't hand its work on* -- there the constraint is whoever it was sending
/// to, not the node reporting the time, which is why each such line names the consumers rather
/// than the blocked producer alone.
///
/// The harness's own appended nodes are excluded from both: they are this tool's overhead, not
/// part of the graph under test.
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

/// Checks the decoded counters against the scenario's own `count`, since a per-node breakdown
/// that doesn't add up to the events actually generated is describing something other than this
/// run. Returns the lines to print -- one statement of what was seen, plus a warning per
/// mismatch.
///
/// The generator's `events.sent` is the strict check: it is the same number the `generation
/// complete` line already reported, arriving by an entirely different path, so a mismatch means
/// the dump is missing drains (`internal`'s final drain on shutdown is what normally makes these
/// agree -- without it the last partial interval never lands). The peak `events.received` is the
/// looser one: it equals `count` for any graph that neither drops nor collapses events, which is
/// every scenario except an `aggregate` one, where fewer is correct by construction.
pub fn check_counts(nodes: &BTreeMap<String, NodeStats>, count: u64) -> Vec<String> {
    let measured: Vec<(&String, &NodeStats)> =
        nodes.iter().filter(|(id, _)| !NodeStats::is_harness(id)).collect();
    let generated: f64 =
        measured.iter().filter(|(_, n)| n.kind == "generate_in").map(|(_, n)| n.events_sent).sum();
    let peak_received = measured.iter().map(|(_, n)| n.events_received).fold(0.0_f64, f64::max);

    let mut lines = vec![format!(
        "events: generator sent {generated:.0}, peak node received {peak_received:.0}, scenario \
         count {count}"
    )];
    if generated != count as f64 {
        lines.push(format!(
            "warning: the generator's `events.sent` ({generated:.0}) does not equal the \
             scenario's count ({count}) -- the dump is missing drains, so every Σ below is an \
             undercount"
        ));
    }
    if peak_received != count as f64 {
        lines.push(format!(
            "warning: no node received all {count} events (peak {peak_received:.0}) -- expected \
             for a graph that collapses events (`aggregate`), a missing drain otherwise"
        ));
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, DdSketch, MetricRecord, Sum, Temporality};

    const SCENARIO: &str = "components:\n  gen:\n    type: generate_in\n    count: 100\n  out:\n    type: null_out\n    sources: [gen]\n";

    fn dump_path() -> PathBuf {
        PathBuf::from("/tmp/logit-perf-attribute-1/attribute.native")
    }

    #[test]
    fn rewrite_appends_both_components_under_the_existing_components_mapping() {
        let out = rewrite_scenario(SCENARIO, Duration::from_secs(1), &dump_path()).unwrap();
        assert!(out.starts_with(SCENARIO), "the original text is preserved verbatim:\n{out}");
        assert!(
            out.contains(
                "  __perf_internal: { type: internal, interval: 1s, span_sample_rate: 0.0, logs: off }\n"
            ),
            "{out}"
        );
        assert!(
            out.contains(
                "  __perf_dump: { type: file_out, sources: [__perf_internal], path: \"/tmp/logit-perf-attribute-1/attribute.native\", format: native, rotate: { max_bytes: \"1024GiB\" } }\n"
            ),
            "{out}"
        );
    }

    #[test]
    fn the_rewritten_scenario_still_parses_as_yaml_with_both_new_components() {
        let out = rewrite_scenario(SCENARIO, Duration::from_secs(1), &dump_path()).unwrap();
        let value: serde_norway::Value = serde_norway::from_str(&out).unwrap();
        let components = value.get("components").and_then(serde_norway::Value::as_mapping).unwrap();
        assert_eq!(components.len(), 4, "the two original components plus the two appended");
        for id in [INTERNAL_ID, DUMP_ID] {
            let component = components.get(serde_norway::Value::from(id)).expect(id);
            assert!(component.get("type").is_some(), "{id} has no type: {component:?}");
        }
    }

    #[test]
    fn rewrite_refuses_a_scenario_that_already_has_an_internal_component() {
        let yaml = "components:\n  gen:\n    type: generate_in\n    count: 1\n  self:\n    type: internal\n    interval: 10s\n  out:\n    type: null_out\n    sources: [gen, self]\n";
        let err = rewrite_scenario(yaml, Duration::from_secs(1), &dump_path())
            .expect_err("rule 13 allows at most one internal per config");
        assert!(
            format!("{err:#}").contains("already has an `internal` component (`self`)"),
            "{err:#}"
        );
    }

    #[test]
    fn rewrite_refuses_a_scenario_using_a_reserved_id() {
        let yaml = "components:\n  __perf_dump:\n    type: null_out\n  gen:\n    type: generate_in\n    count: 1\n";
        let err = rewrite_scenario(yaml, Duration::from_secs(1), &dump_path())
            .expect_err("the appended ids are reserved");
        assert!(format!("{err:#}").contains("reserves"), "{err:#}");
    }

    #[test]
    fn rewrite_refuses_a_scenario_with_another_top_level_key() {
        let yaml = format!("{SCENARIO}admin:\n  bind: 127.0.0.1:9000\n");
        let err = rewrite_scenario(&yaml, Duration::from_secs(1), &dump_path())
            .expect_err("appending at the end would land under the wrong key");
        assert!(format!("{err:#}").contains("top-level `admin:` key"), "{err:#}");
    }

    #[test]
    fn interval_renders_as_a_humantime_literal() {
        assert_eq!(format_interval(Duration::from_secs(1)), "1s");
        assert_eq!(format_interval(Duration::from_secs(10)), "10s");
        assert_eq!(format_interval(Duration::from_millis(250)), "250ms");
        assert_eq!(format_interval(Duration::from_millis(1_500)), "1500ms");
    }

    #[test]
    fn a_path_needing_quoting_is_escaped_rather_than_emitted_raw() {
        assert_eq!(yaml_double_quoted("/tmp/a b/c.native"), "\"/tmp/a b/c.native\"");
        assert_eq!(yaml_double_quoted("/tmp/\"x\"/c"), "\"/tmp/\\\"x\\\"/c\"");
        assert_eq!(yaml_double_quoted("/tmp/a\\b"), "\"/tmp/a\\\\b\"");
    }

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
        let lines = check_counts(&nodes, 100);
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
        let lines = check_counts(&nodes, 100);
        assert_eq!(lines.len(), 3);
        assert!(lines[1].contains("does not equal the scenario's count"), "{}", lines[1]);
        assert!(lines[2].contains("no node received all 100 events"), "{}", lines[2]);
    }
}
