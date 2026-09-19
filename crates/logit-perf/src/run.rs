//! `logit-perf run`: builds the release binary once, then spawns it once per scenario per repeat,
//! deriving throughput/CPU/RSS from its stderr and `wait4` (docs/adr/load-test-harness.md,
//! docs/plans/load-test-harness.md's "Harness" section).
//!
//! ## Two shapes of run
//!
//! A [`Workload::Generated`] scenario generates its own events and announces when it's finished
//! (`generation complete`), which is what bounds the measurement. A [`Workload::Driven`] one
//! ([ADR `udp-intake-batching-and-socket-visibility`](../../../docs/adr/udp-intake-batching-and-socket-visibility.md))
//! has no generator at all: the harness waits for the child's `ready` line, blasts it over a real
//! UDP socket from `crate::load`, and the blast returning is what bounds the measurement.
//! [`Drive`] is that fork; everything after it -- settle, SIGTERM, `wait4` -- is the same path
//! both take.
//!
//! **A driven scenario's denominator is events *delivered*, never events sent.** This is the first
//! scenario family in which those two can honestly differ: a real socket may lose datagrams in the
//! kernel before `logit` ever sees them, and denominating over sent would silently understate the
//! per-event cost by exactly the drop rate -- in the very regime the baseline is deliberately tuned
//! into. The delivered count comes out of the child's own `logit.component.events.received` through
//! the `crate::telemetry_leg` dump, attached at run time and never shipped in the scenario YAML.
//!
//! **Every driven run self-checks before its numbers are believed.** `sent == received + kernel
//! drops` has to close (on loopback there is nowhere else for a datagram to go), and the decoder
//! must report no malformed lines -- otherwise the scenario would be quietly benchmarking the
//! error path. `--verify` adds the stricter form: a paced, zero-drop run must deliver *exactly* the
//! event count the load spec's ring says it sent.

use crate::load::{self, CpuSet, LoadOutcome, LoadPlan};
use crate::result::{BinaryInfo, BoxState, GitInfo, RunReport, Sample, ScenarioReport, UdpSample};
use crate::rusage::{self, Usage};
use crate::scenario::{self, Scenario, Workload};
use crate::telemetry_leg::{self, RemoveOnDrop};
use anyhow::{bail, Context};
use logit_core::{interner, MetricKind, Value};
use std::collections::{BTreeMap, VecDeque};
use std::io::{self, BufRead};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The shortest settle a driven scenario ever uses, regardless of `--settle`.
///
/// A generated scenario's settle only has to cover the in-process drain after the last event is
/// produced, and `--settle`'s 1 s default has always been ample for that. A driven one has three
/// slower things to wait for, in order: whatever the kernel still holds in the socket's receive
/// queue after the blast stops, the listener's **guaranteed final `SO_MEMINFO` sample** (taken once
/// the read loop exits, so the last interval of `logit.input.kernel.drops` isn't lost), and
/// `internal`'s own final drain on shutdown, which is what carries both of those into the dump.
///
/// The floor is 3 s because the first two of those are bounded by a 1 s sampling interval and the
/// third by the leg's own `--interval` (1 s by default), so anything at or below 1 s is racing two
/// timers it has no reason to beat. It is not a tuned number — what makes it *checked* rather than
/// hoped for is [`self_check`]: if a settle is ever too short, the datagram accounting does not
/// close and the run fails saying so, rather than quietly reporting a drop rate inflated by
/// datagrams that were still in the receive queue when the process was signalled.
///
/// `--settle` above this is honoured as given.
const DRIVEN_SETTLE_FLOOR: Duration = Duration::from_secs(3);

pub struct RunArgs {
    /// Empty means every discovered scenario.
    pub scenarios: Vec<String>,
    pub repeat: u32,
    pub label: Option<String>,
    pub settle: Duration,
    pub no_build: bool,
    pub profile: String,
    /// Measure this binary instead of building one. Implies `--no-build`; a relative path is
    /// resolved against the repo root, not the process's cwd, so it means the same thing typed on
    /// the host or inside the dev container. The enabling change for a multi-source VM session
    /// (`docs/adr/disposable-azure-perf-vm.md`'s `script/vm build`): each source gets its own
    /// stashed binary under `perf/bins/<slug>/logit`, and this is what points a run at one of
    /// them without the `docker cp`-into-the-target-volume choreography that used to require.
    pub logit_bin: Option<PathBuf>,
    /// How long to wait for a scenario's `generation complete` line (generated) or `ready` line
    /// (driven) before giving up on it as hung.
    pub timeout: Duration,
    /// How long to wait for the process to actually exit after `--settle`/SIGTERM (or, for a
    /// self-exiting scenario, after the completion line) before force-killing it.
    pub shutdown_timeout: Duration,
    /// The run-time telemetry leg's drain cadence, for driven scenarios only.
    pub interval: Duration,
    pub pin_sender: Option<CpuSet>,
    pub pin_child: Option<CpuSet>,
    /// Hold every driven scenario to the strict expectation: zero drops, and an exactly-equal
    /// delivered event count. Meant to be run against a paced spec -- an unpaced blast is tuned to
    /// drop on purpose and will (correctly) fail this.
    pub verify: bool,
    /// Multiplies every driven spec's `rate`. `None` means "whatever the run implies": 1.0
    /// ordinarily, `load::VERIFY_RATE_SCALE` under `--verify`. Set explicitly it wins over both,
    /// which is what lets a `--verify` run be paced by hand if the default derate is wrong for a
    /// particular box.
    pub rate_scale: Option<f64>,
}

pub fn run(root: &Path, args: RunArgs) -> anyhow::Result<()> {
    let scenarios_dir = root.join("perf/scenarios");
    let mut scenarios = scenario::discover(&scenarios_dir)
        .with_context(|| format!("discovering scenarios in {}", scenarios_dir.display()))?;

    if !args.scenarios.is_empty() {
        let known: std::collections::BTreeSet<&str> =
            scenarios.iter().map(|s| s.name.as_str()).collect();
        for wanted in &args.scenarios {
            if !known.contains(wanted.as_str()) {
                bail!("no such scenario `{wanted}` under {}", scenarios_dir.display());
            }
        }
        scenarios.retain(|s| args.scenarios.contains(&s.name));
    }
    if scenarios.is_empty() {
        bail!("no scenarios to run");
    }

    // Read and reported *before* the first scenario, not only into the results file: a run taken
    // under `powersave` or on battery is worth aborting and restarting, and finding that out ten
    // minutes later from the JSON is finding it out too late.
    let state = box_state();
    for warning in state.warnings() {
        eprintln!("warning: {warning}");
    }

    let logit_bin =
        build_and_locate(root, &args.profile, args.no_build, args.logit_bin.as_deref())?;
    let binary = binary_info(&logit_bin);
    println!(
        "binary:        {} (sha256 {}{})",
        binary.path,
        short12(&binary.sha256),
        match (&binary.source_ref, &binary.source_sha) {
            (Some(source), Some(sha)) => format!(", {source} @ {}", short12(sha)),
            (Some(source), None) => format!(", {source}"),
            (None, _) => String::new(),
        }
    );

    let mut reports: BTreeMap<String, ScenarioReport> = BTreeMap::new();
    let mut any_failed = false;

    for scenario in &scenarios {
        println!(
            "-- {} ({}, {})",
            scenario.name,
            scenario.workload.describe(),
            if scenario.needs_sigterm { "needs SIGTERM" } else { "self-exits" }
        );
        // The ring is rendered once per scenario, not once per repeat: it is deterministic from
        // the spec's seed, so every repeat would render the same bytes, and rendering tens of
        // thousands of datagrams inside the measured window would be work the measurement isn't
        // about.
        let plan = match &scenario.workload {
            Workload::Generated { .. } => None,
            Workload::Driven(_) => match driven_plan(scenario, &args) {
                Ok(plan) => Some(plan),
                Err(err) => {
                    eprintln!("   FAILED: {err:#}");
                    any_failed = true;
                    continue;
                }
            },
        };
        if let Some(plan) = &plan {
            let expected = plan.expected();
            println!(
                "   {}: {} distinct datagrams pre-rendered, {}",
                plan.spec_path.display(),
                plan.ring.len(),
                plan.ring.shape(),
            );
            println!(
                "   sending {} lines / {} events / {:.1} MiB over {} sockets on {} threads{}",
                expected.lines,
                expected.events,
                expected.bytes as f64 / (1024.0 * 1024.0),
                plan.spec.sockets,
                plan.spec.threads,
                plan.spec.rate.map(|r| format!(", paced at {r} datagrams/s")).unwrap_or_default(),
            );
        }

        let mut samples = Vec::with_capacity(args.repeat as usize);
        let mut scenario_failed = false;
        for repeat in 1..=args.repeat {
            // Fresh spool every repeat, not just every invocation -- `crate::spool`'s own doc
            // comment has the mechanism (`DiskQueue::open`'s O(segment size) startup scan,
            // `docs/known-gaps.md`'s `buffered` entry): left alone, the second repeat already
            // re-validates the first repeat's spool, and it only grows from there.
            if let Err(err) = crate::spool::clear(root, scenario) {
                eprintln!("   repeat {repeat}/{}: FAILED: {err:#}", args.repeat);
                scenario_failed = true;
                break;
            }
            let outcome = match &plan {
                Some(plan) => run_one_driven(&logit_bin, scenario, plan, &args),
                None => run_one_generated(&logit_bin, scenario, &args),
            };
            match outcome {
                Ok(sample) => {
                    println!(
                        "   repeat {repeat}/{}: {:.0} events/s, {:.3} us/event, {:.1} MiB peak RSS, {} startup{}",
                        args.repeat,
                        sample.events_per_s,
                        sample.cpu_us_per_event,
                        sample.max_rss_bytes as f64 / (1024.0 * 1024.0),
                        format_startup(sample.startup_s),
                        sample.udp.map(format_udp_suffix).unwrap_or_default(),
                    );
                    samples.push(sample);
                }
                Err(err) => {
                    eprintln!("   repeat {repeat}/{}: FAILED: {err:#}", args.repeat);
                    scenario_failed = true;
                    break;
                }
            }
        }
        if scenario_failed || samples.is_empty() {
            any_failed = true;
            continue;
        }
        let median = crate::result::median_sample(&samples);
        let min = crate::result::min_sample(&samples);
        // The scenario's own denominator, recorded so a results file is self-describing: events
        // generated for a generated scenario, **lines sent** for a driven one. Lines, not
        // datagrams: a line is one metric, which is the unit the scenario's event counts are in.
        let count = match &scenario.workload {
            Workload::Generated { count } => *count,
            Workload::Driven(_) => median.udp.map(|udp| udp.sent_lines).unwrap_or_default(),
        };
        reports
            .insert(scenario.name.clone(), ScenarioReport { count, repeats: samples, median, min });
    }

    if reports.is_empty() {
        bail!("every scenario failed; nothing to write");
    }

    let now = now_unix_seconds();
    let report = RunReport {
        git: git_info(root),
        timestamp: format_rfc3339_utc_seconds(now),
        hostname: hostname(),
        cpu_model: cpu_model(),
        nproc: std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1),
        rustc: rustc_version(),
        profile: args.profile.clone(),
        label: args.label.clone(),
        box_state: Some(state),
        binary: Some(binary),
        scenarios: reports,
    };

    let results_dir = root.join("perf/results");
    std::fs::create_dir_all(&results_dir)
        .with_context(|| format!("creating {}", results_dir.display()))?;
    let path = results_dir.join(result_filename(&report, now, args.logit_bin.is_some()));
    std::fs::write(&path, serde_json::to_string_pretty(&report)?)
        .with_context(|| format!("writing {}", path.display()))?;

    print_table(&report);
    print_udp_table(&report);
    println!("\nwrote {}", path.display());

    // Every driven repeat removed its own dump on success and kept it on failure, so this takes
    // the scratch directory with it exactly when there is nothing left in it worth keeping.
    telemetry_leg::remove_workdir_if_empty("run");

    if any_failed {
        bail!("one or more scenarios failed -- see above");
    }
    Ok(())
}

/// Reads a driven scenario's sidecar spec and renders its ring, applying whatever rate scale the
/// run asked for (`LoadPlan::scale_rate` has why one would).
fn driven_plan(scenario: &Scenario, args: &RunArgs) -> anyhow::Result<LoadPlan> {
    let source = std::fs::read_to_string(&scenario.path)
        .with_context(|| format!("reading {}", scenario.path.display()))?;
    let mut plan = LoadPlan::build(&scenario.load_spec_path()?, &source)?;
    // An explicit `--rate-scale` wins over `--verify`'s own default, so a box where a quarter is
    // still not enough headroom (or is more than needed) can be paced by hand.
    let scale = args.rate_scale.unwrap_or(if args.verify { load::VERIFY_RATE_SCALE } else { 1.0 });
    if scale != 1.0 {
        let scaled = plan.scale_rate(scale)?;
        println!(
            "   --rate-scale {scale}: pacing at {scaled} datagrams/s{}",
            if args.verify && args.rate_scale.is_none() { " (--verify's default)" } else { "" }
        );
    }
    Ok(plan)
}

// ---------------------------------------------------------------------------------------------
// Child lifecycle
// ---------------------------------------------------------------------------------------------

/// Something the child announced about itself on stderr, in the order the reader thread saw it.
///
/// Split out of what used to be one `Completion` message because the two kinds of scenario wait
/// for different things: a generated one measures `ready` → `generation complete`, a driven one
/// measures `ready` → *the sender* finishing, and never sees a completion line at all.
enum ChildEvent {
    /// `tracing::info!(target: "logit", "ready")` -- logged once the bind pass has opened every
    /// listener's socket and every node has been spawned (`crates/logit-pipeline/src/runtime.rs`).
    /// For a driven scenario this is a hard precondition, not a nicety: sending before the socket
    /// exists would be measured as loss that never happened.
    Ready(Instant),
    /// `generate_in`'s own completion line, with the `events` count it carried (`None` if that
    /// field was missing or not an unsigned integer -- a format this harness can't trust).
    Complete { at: Instant, events: Option<u64> },
}

/// `None` if `line` isn't the completion line at all; `Some(events)` if it is -- `generate_in`'s
/// own line (`docs/plans/load-test-harness.md`) is `{"message":"generation complete", "events":
/// ..., "batches": ..., "elapsed": ...}` alongside `--log-format json`'s usual fields, so this
/// parses the whole line as JSON and checks `message` for an *exact* match, not a substring: a
/// human-readable field elsewhere in the line quoting the same words must never be mistaken for
/// the real signal.
fn parse_completion_line(line: &str) -> Option<Option<u64>> {
    let json: serde_json::Value = serde_json::from_str(line).ok()?;
    if json.get("message").and_then(serde_json::Value::as_str) != Some("generation complete") {
        return None;
    }
    Some(json.get("events").and_then(serde_json::Value::as_u64))
}

/// Whether `line` is the process's own readiness line, matching [`parse_completion_line`]'s own
/// reasoning: the whole line parsed as JSON, an *exact* `message` match rather than a substring.
fn is_ready_line(line: &str) -> bool {
    let Ok(json) = serde_json::from_str::<serde_json::Value>(line) else { return false };
    json.get("message").and_then(serde_json::Value::as_str) == Some("ready")
}

/// Accumulates a child's stderr, capped to the last 64 KiB -- a hung or unexpectedly chatty
/// scenario must never let one failed repeat's error message grow without bound. Trims whole
/// lines from the front rather than truncating raw bytes, so what's kept is always valid UTF-8
/// and never a fragment of a line.
struct StderrCapture {
    lines: VecDeque<String>,
    total_bytes: usize,
}

impl StderrCapture {
    const CAP_BYTES: usize = 64 * 1024;

    fn new() -> Self {
        StderrCapture { lines: VecDeque::new(), total_bytes: 0 }
    }

    fn push(&mut self, line: String) {
        self.total_bytes += line.len() + 1; // +1: the newline `into_string` rejoins with.
        self.lines.push_back(line);
        while self.total_bytes > Self::CAP_BYTES {
            match self.lines.pop_front() {
                Some(removed) => self.total_bytes -= removed.len() + 1,
                None => break,
            }
        }
    }

    fn into_string(self) -> String {
        self.lines.into_iter().collect::<Vec<_>>().join("\n")
    }
}

/// Reads a child's stderr to its end, announcing the `ready`/`generation complete` lines on
/// `events` as they go past and keeping the tail for an error message. Returns what was captured
/// and, in words, **why the stream ended** -- which is what a driven blast's abort message quotes.
///
/// **Decoded lossily, never fallibly.** `BufRead::lines` yields `Err(InvalidData)` for a line that
/// isn't UTF-8, which is indistinguishable at the call site from the stream ending -- so a single
/// stray byte in the child's stderr (a panic message with a truncated multi-byte character, a
/// library writing raw bytes) used to stop the reader early and, worse, tell a running blast the
/// child had exited. Reading delimited bytes and running them through `from_utf8_lossy` means such
/// a line is captured with replacement characters and the loop carries on; only a real EOF or a
/// real I/O error ends it, and the two are reported apart.
///
/// Split out of `spawn_and_measure` so exactly that can be tested without a child process.
fn read_child_stderr(stderr: impl io::Read, events: &mpsc::Sender<ChildEvent>) -> (String, String) {
    let mut reader = io::BufReader::new(stderr);
    let mut capture = StderrCapture::new();
    let mut raw = Vec::new();
    let mut sent_ready = false;
    let mut sent_complete = false;

    let end = loop {
        raw.clear();
        match reader.read_until(b'\n', &mut raw) {
            Ok(0) => break "the process under test exited mid-blast (its stderr reached EOF)",
            Ok(_) => {}
            Err(_) => break "this harness could not read the process under test's stderr",
        }
        while raw.last().is_some_and(|byte| *byte == b'\n' || *byte == b'\r') {
            raw.pop();
        }
        let line = String::from_utf8_lossy(&raw).into_owned();

        if !sent_ready && is_ready_line(&line) {
            sent_ready = true;
            let _ = events.send(ChildEvent::Ready(Instant::now()));
        }
        if !sent_complete {
            if let Some(events_count) = parse_completion_line(&line) {
                sent_complete = true;
                let _ =
                    events.send(ChildEvent::Complete { at: Instant::now(), events: events_count });
            }
        }
        capture.push(line);
    };
    (capture.into_string(), end.to_string())
}

/// Kills and reaps a still-running child, then joins both reader threads -- every error path that
/// bails before the ordinary settle/SIGTERM/`wait4` sequence has run calls this, so a failed
/// repeat never leaves a live process, a zombie, or a detached reader thread behind.
fn kill_and_reap(
    child: &mut Child,
    stdout_drain: JoinHandle<()>,
    stderr_reader: JoinHandle<String>,
) -> String {
    let _ = child.kill();
    let _ = child.wait();
    drain_and_join(stdout_drain, stderr_reader)
}

/// Joins both reader threads -- used once the child is already known to have exited (reaped
/// either by [`kill_and_reap`] or by `wait4` on the ordinary path), since each thread's own loop
/// ends when its pipe's write end closes.
fn drain_and_join(stdout_drain: JoinHandle<()>, stderr_reader: JoinHandle<String>) -> String {
    let _ = stdout_drain.join();
    stderr_reader.join().unwrap_or_default()
}

/// What bounds one measurement, and how the load gets there.
pub(crate) enum Drive<'a> {
    /// Wait for `generate_in`'s completion line and check it reported exactly `count` events.
    Generated { count: u64 },
    /// Wait for `ready`, then blast `plan` from this process and stop when the sender returns.
    Driven { plan: &'a LoadPlan, pin_sender: Option<&'a CpuSet> },
}

/// One spawn-measure-shutdown cycle's inputs. A struct rather than a pile of positional
/// parameters because three callers share it: [`run_one_generated`]/[`run_one_driven`],
/// `crate::attribute` (the same scenario rewritten with a telemetry leg appended), and
/// `crate::flamegraph` (the scenario as it ships, under `perf record`).
pub(crate) struct SpawnConfig<'a> {
    pub logit_bin: &'a Path,
    /// An argv prefix to run `logit` *under*, with the `logit run <config>` command line appended
    /// to it -- `["perf", "record", .., "--"]` for `flamegraph`. Empty for a plain run.
    ///
    /// When it isn't empty the spawned process is the wrapper, not `logit`: the reported rusage
    /// covers both, and a SIGTERM on the settle path goes to the wrapper (which is what `perf
    /// record` wants -- it finalizes `perf.data` and stops its workload).
    pub wrapper: &'a [String],
    /// The config to hand `logit run` -- a scenario file, or a rewritten copy of one.
    pub config: &'a Path,
    pub drive: Drive<'a>,
    /// CPUs to pin the child to, applied **between `fork` and `exec`** so it never runs a single
    /// instruction unpinned -- see [`spawn_and_measure`].
    pub pin_child: Option<&'a CpuSet>,
    /// Whether this graph will still be running after the load stops -- when true the measurement
    /// is followed by `settle`, then SIGTERM, rather than waiting for the process to exit on its
    /// own. Always true for a driven scenario, which is a socket listener by construction.
    pub needs_sigterm: bool,
    pub settle: Duration,
    pub timeout: Duration,
    pub shutdown_timeout: Duration,
}

/// The raw result of one spawn-measure-shutdown cycle, before a denominator has been chosen.
///
/// Deliberately *not* a [`Sample`]: a driven scenario's denominator isn't known until the child's
/// own telemetry has been decoded, which happens after this returns. Handing back the pieces keeps
/// that choice at the one call site that can make it correctly.
pub(crate) struct Measured {
    pub startup: Option<Duration>,
    pub usage: Usage,
    /// What the sender actually put on the wire -- `None` for a generated scenario, which has no
    /// sender.
    pub load: Option<LoadOutcome>,
}

impl Measured {
    pub fn cpu_us_per_event(&self, events: u64) -> f64 {
        let cpu = self.usage.user.as_secs_f64() + self.usage.sys.as_secs_f64();
        cpu * 1_000_000.0 / events as f64
    }

    /// `ready` -> the load stopping. Carried on the `wait4` result because that is where it was
    /// handed in (`rusage::wait4(pid, wall)`), rather than duplicated on this struct.
    pub fn wall(&self) -> Duration {
        self.usage.wall
    }

    pub fn sample(&self, events: u64) -> Sample {
        Sample::from_usage(
            events,
            self.startup,
            self.usage.wall,
            self.usage.user,
            self.usage.sys,
            self.usage.max_rss_bytes,
        )
    }
}

fn run_one_generated(
    logit_bin: &Path,
    scenario: &Scenario,
    args: &RunArgs,
) -> anyhow::Result<Sample> {
    let Workload::Generated { count } = scenario.workload else {
        unreachable!("run_one_generated is only called for a generated workload")
    };
    let measured = spawn_and_measure(SpawnConfig {
        logit_bin,
        wrapper: &[],
        config: &scenario.path,
        drive: Drive::Generated { count },
        pin_child: args.pin_child.as_ref(),
        needs_sigterm: scenario.needs_sigterm,
        settle: args.settle,
        timeout: args.timeout,
        shutdown_timeout: args.shutdown_timeout,
    })?;
    Ok(measured.sample(count))
}

/// One repeat of a real-socket scenario: attach the telemetry leg, blast, then read the child's
/// own counters back out of the dump and check they add up before believing any of it.
fn run_one_driven(
    logit_bin: &Path,
    scenario: &Scenario,
    plan: &LoadPlan,
    args: &RunArgs,
) -> anyhow::Result<Sample> {
    let Workload::Driven(spec) = &scenario.workload else {
        unreachable!("run_one_driven is only called for a driven workload")
    };
    let source = std::fs::read_to_string(&scenario.path)
        .with_context(|| format!("reading {}", scenario.path.display()))?;

    let workdir = telemetry_leg::make_workdir("run")?;
    let dump_path = workdir.join(format!("{}.native", scenario.name));
    // Unlike `attribute`, which refuses a pre-existing dump, `run` expects one: every repeat of
    // every scenario in one invocation writes here. It's this process's own pid-named directory,
    // so removing last repeat's file is removing our own, not somebody else's.
    let _ = std::fs::remove_file(&dump_path);

    let rewritten = telemetry_leg::rewrite_scenario(&source, args.interval, &dump_path)
        .with_context(|| format!("rewriting {}", scenario.path.display()))?;
    let config_path = telemetry_leg::rewritten_config_path(scenario, "run")?;
    let _cleanup = RemoveOnDrop(config_path.clone());
    std::fs::write(&config_path, &rewritten)
        .with_context(|| format!("writing {}", config_path.display()))?;
    telemetry_leg::validate(logit_bin, &config_path)?;

    let measured = spawn_and_measure(SpawnConfig {
        logit_bin,
        wrapper: &[],
        config: &config_path,
        drive: Drive::Driven { plan, pin_sender: args.pin_sender.as_ref() },
        pin_child: args.pin_child.as_ref(),
        // Always: a driven graph is a socket listener with a telemetry leg attached, so neither
        // half of it can ever decide on its own that it's finished.
        needs_sigterm: true,
        settle: args.settle.max(DRIVEN_SETTLE_FLOOR),
        timeout: args.timeout,
        shutdown_timeout: args.shutdown_timeout,
    })?;
    let load = measured.load.expect("a driven measurement always carries its sender's outcome");

    let outcome = (|| -> anyhow::Result<Sample> {
        let events = telemetry_leg::decode_dump(&dump_path, true)?;
        let nodes = crate::attribute::aggregate(&events);
        // The terminal sink, not the peak receiver -- `delivered_at_sink`'s own doc has why the
        // two agree today and would stop agreeing the moment a driven scenario grew a transform
        // that drops.
        let delivered = crate::attribute::delivered_at_sink(&nodes, spec.sink.as_deref())?;
        let input = input_stats(&events, &spec.target);

        let udp = UdpSample {
            sent_datagrams: load.sent_datagrams,
            sent_lines: load.sent_lines,
            received_datagrams: input.datagrams,
            reads: input.reads,
            kernel_dropped: input.kernel_drops,
            queue_dropped: input.queue_dropped,
            events_delivered: delivered,
            send_errors: load.send_errors,
            kernel_rcvbuf_utilization_max: input.rcvbuf_utilization_max,
            // What the blast actually paced at, after `--rate-scale`/`--verify` -- not the spec's
            // own `rate:`, which is what it would have been.
            effective_rate: plan.spec.rate,
        };
        self_check(scenario, plan, &udp, &input, args.verify)?;

        if delivered == 0 {
            bail!(
                "no events reached the sink at all -- {} datagrams were sent to {} and {} \
                 arrived; check the scenario's `bind:` matches the load spec's `target:`",
                udp.sent_datagrams,
                plan.target,
                udp.received_datagrams
            );
        }
        let mut sample = measured.sample(delivered);
        sample.udp = Some(udp);
        Ok(sample)
    })();

    match &outcome {
        // A per-repeat artifact of a run that worked, and `run` may do dozens of these in one
        // invocation -- left alone they fill the temp directory for no reason. Mirrors
        // `attribute`: removed on success, kept and named on failure, since a failed repeat's dump
        // is the one thing anyone debugging it would want to look at.
        Ok(_) => {
            let _ = std::fs::remove_file(&dump_path);
        }
        Err(_) => eprintln!("note: the telemetry dump is left at {}", dump_path.display()),
    }
    outcome
}

/// The listener-side counters a driven run needs, folded out of the decoded telemetry dump for one
/// component. Kept here rather than in `crate::attribute`'s `NodeStats`: those are the uniform
/// per-node metrics every component emits, these are `logit.input.*`/UDP-specific and only a
/// driven scenario has any use for them (`docs/design/internal-telemetry.md`'s listener tables).
#[derive(Debug, Clone, Default, PartialEq)]
struct InputStats {
    /// `logit.input.datagrams` -- every datagram the listener actually received.
    datagrams: u64,
    /// `logit.input.reads` -- read syscalls made. `datagrams / reads` is the mean fill of one
    /// `recvmmsg(2)` batch (`docs/adr/udp-intake-batching-and-socket-visibility.md`).
    reads: u64,
    /// `logit.input.kernel.drops` -- what the kernel discarded before `recv_from` could return it.
    /// A delta per sample, summed here; not emitted at all when zero, so an absent metric is 0.
    kernel_drops: u64,
    /// `logit.component.datagrams.dropped{reason=overflow_*}` -- `ReceiveQueue` eviction, i.e.
    /// loss `logit` chose and counted itself, downstream of the kernel's.
    queue_dropped: u64,
    /// The high-water mark of `logit.input.receive_buffer.utilization`. 1.0 is not "nearly full":
    /// it is exactly where the kernel starts dropping, and a reading a little above it is normal
    /// under load (the kernel charges an arriving packet and then tests the total).
    rcvbuf_utilization_max: f64,
    /// Whether the kernel socket sampler ever produced a reading at all.
    ///
    /// Tracked as **presence of a sample**, never as a value: every one of the numbers it reports
    /// is legitimately zero at times, so `== 0.0` cannot tell "the sampler said zero" from "the
    /// sampler never ran". `used.bytes` and `utilization` are the two the sampler alone emits
    /// (`receive_buffer.bytes` is also written once at bind, so its presence proves nothing).
    ///
    /// `false` with datagrams received means `getsockopt(SO_MEMINFO)` was unavailable and W1's
    /// sampler disabled itself for the process -- a `diag.warn`, which emits no counter, so this
    /// is the only evidence of it the dump carries. `self_check` has to notice, or it blames a
    /// `--settle` that was never the problem.
    kernel_sampled: bool,
    /// `logit.component.diagnostics{key=...}` -- every throttled warning the component raised.
    /// `bad_line` (a statsd line that didn't parse) and `bad_datagram` (a datagram that wasn't
    /// UTF-8) are the two that would mean the scenario is measuring the error path.
    diagnostics: BTreeMap<String, u64>,
}

const INPUT_DATAGRAMS: &str = "logit.input.datagrams";
const INPUT_READS: &str = "logit.input.reads";
const KERNEL_DROPS: &str = "logit.input.kernel.drops";
const RCVBUF_UTILIZATION: &str = "logit.input.receive_buffer.utilization";
const RCVBUF_USED: &str = "logit.input.receive_buffer.used.bytes";
const DATAGRAMS_DROPPED: &str = "logit.component.datagrams.dropped";
const DIAGNOSTICS: &str = "logit.component.diagnostics";

fn input_stats(events: &[logit_core::Event], component: &str) -> InputStats {
    let mut stats = InputStats::default();
    for event in events {
        if event.attributes.get("component").and_then(Value::as_str) != Some(component) {
            continue;
        }
        for metric in &event.metrics {
            let name = interner::resolve(metric.name);
            match (name, &metric.kind) {
                (INPUT_DATAGRAMS, MetricKind::Sum(sum)) => stats.datagrams += sum.value as u64,
                (INPUT_READS, MetricKind::Sum(sum)) => stats.reads += sum.value as u64,
                (KERNEL_DROPS, MetricKind::Sum(sum)) => stats.kernel_drops += sum.value as u64,
                (DATAGRAMS_DROPPED, MetricKind::Sum(sum)) => {
                    stats.queue_dropped += sum.value as u64
                }
                (RCVBUF_UTILIZATION, MetricKind::Gauge(value)) => {
                    stats.kernel_sampled = true;
                    stats.rcvbuf_utilization_max = stats.rcvbuf_utilization_max.max(*value)
                }
                // Read for its *presence* only: the sampler emits it unconditionally on every
                // successful `SO_MEMINFO` read, including when the utilization above can't be
                // computed because the granted buffer came back as zero.
                (RCVBUF_USED, MetricKind::Gauge(_)) => stats.kernel_sampled = true,
                (DIAGNOSTICS, MetricKind::Sum(sum)) => {
                    let key = event
                        .attributes
                        .get("key")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                        .to_string();
                    *stats.diagnostics.entry(key).or_default() += sum.value as u64;
                }
                _ => {}
            }
        }
    }
    stats
}

/// Everything a driven run has to be true for its numbers to mean anything.
///
/// 1. **The accounting closes.** On loopback a datagram either arrives or the kernel drops it --
///    there is no lossy link, no fragmentation, no middlebox. `sent == received + kernel drops` is
///    therefore an equality, not an inequality, and a mismatch means something this harness
///    believes about the run is wrong (a settle too short for the receive queue to drain, a final
///    socket sample that never landed, a datagram sent after the child was signalled) rather than
///    an interesting measurement.
/// 2. **Nothing was malformed.** A load spec that renders lines the decoder rejects would benchmark
///    the error path and look perfectly healthy doing it -- fast, even, since a rejected line never
///    becomes an event. Any decode diagnostic at all fails the run.
/// 3. **Under `--verify`, the delivered count is exact.** With a paced spec that drops nothing,
///    every line the ring sent has to come out the other end as the exact number of events the
///    ring says it contains -- which is not the same as the number of lines, since a multi-value
///    counter or gauge line decodes to one event per value.
fn self_check(
    scenario: &Scenario,
    plan: &LoadPlan,
    udp: &UdpSample,
    input: &InputStats,
    verify: bool,
) -> anyhow::Result<()> {
    // Checked before the accounting, because without a kernel sampler the accounting *cannot*
    // close -- `kernel_dropped` is unknowable, reads as 0, and every repeat would fail blaming a
    // `--settle` that was never the problem. W1's sampler disables itself for the process after
    // one failed `getsockopt(SO_MEMINFO)` and says so through `Diagnostics::warn`, which emits no
    // counter, so the absence of a reading is the only evidence in the dump.
    if udp.received_datagrams > 0 && !input.kernel_sampled {
        bail!(
            "{}: the kernel's per-socket counters were never reported -- {} datagrams arrived but \
             no `logit.input.receive_buffer.*` sample appeared in the whole run. \
             `getsockopt(SO_MEMINFO)` needs Linux 4.12+ and a sandbox that permits it; without it \
             `logit.input.kernel.drops` is unknowable, so a driven scenario cannot account for \
             where its datagrams went and its drop rate would read as a flat zero. Run the child's \
             own stderr to see the listener's one-off warning",
            scenario.name,
            udp.received_datagrams,
        );
    }

    let accounted = udp.received_datagrams + udp.kernel_dropped;
    if accounted != udp.sent_datagrams {
        bail!(
            "{}: datagram accounting does not close -- sent {}, received {}, kernel-dropped {} \
             (received + kernel = {accounted}, off by {}). On loopback there is nowhere else for a \
             datagram to go, so this is a harness problem, not a measurement: the usual cause is a \
             settle shorter than the time the receive queue needs to drain and the listener's final \
             socket sample to land (--settle, floor {}s for a driven scenario)",
            scenario.name,
            udp.sent_datagrams,
            udp.received_datagrams,
            udp.kernel_dropped,
            accounted.abs_diff(udp.sent_datagrams),
            DRIVEN_SETTLE_FLOOR.as_secs(),
        );
    }
    if !input.diagnostics.is_empty() {
        let rendered: Vec<String> =
            input.diagnostics.iter().map(|(key, count)| format!("{key}={count}")).collect();
        bail!(
            "{}: the listener reported decode diagnostics ({}) -- the load spec is putting lines \
             on the wire that `StatsdDecoder` rejects, so this run would be benchmarking the \
             malformed-line path. Fix perf/load/statsd-app.yaml before trusting any number here",
            scenario.name,
            rendered.join(", "),
        );
    }
    if verify {
        let expected = plan.expected();
        if udp.kernel_dropped != 0 || udp.queue_dropped != 0 {
            bail!(
                "{}: --verify requires a zero-drop run, but the kernel dropped {} datagram(s) and \
                 the receive queue dropped {}. Lower the load spec's `rate` (or raise the \
                 scenario's `receive_buffer_bytes`) for a verification run",
                scenario.name,
                udp.kernel_dropped,
                udp.queue_dropped,
            );
        }
        if udp.events_delivered != expected.events {
            bail!(
                "{}: --verify expected exactly {} events delivered (the ring's own count over {} \
                 datagrams / {} lines) but the sink received {}",
                scenario.name,
                expected.events,
                udp.sent_datagrams,
                expected.lines,
                udp.events_delivered,
            );
        }
        println!(
            "   verified: {} datagrams, {} lines, {} events delivered exactly, zero drops",
            udp.sent_datagrams, expected.lines, udp.events_delivered
        );
    }
    Ok(())
}

/// Spawns one `logit run <config>`, drives it to completion, shuts the process down, and reports
/// its `wait4` rusage. The whole spawn/reader-thread/SIGTERM/`wait4` lifecycle lives here, once,
/// for every subcommand that needs to run a scenario.
pub(crate) fn spawn_and_measure(spawn: SpawnConfig<'_>) -> anyhow::Result<Measured> {
    let SpawnConfig {
        logit_bin,
        wrapper,
        config,
        drive,
        pin_child,
        needs_sigterm,
        settle,
        timeout,
        shutdown_timeout,
    } = spawn;
    let mut command = match wrapper.split_first() {
        Some((program, rest)) => {
            let mut command = Command::new(program);
            command.args(rest).arg(logit_bin);
            command
        }
        None => Command::new(logit_bin),
    };
    command
        .args(["--log-format", "json", "--log-level", "info", "run"])
        .arg(config)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if let Some(pin) = pin_child {
        // `pre_exec`, not a `sched_setaffinity` on the returned pid: pinning after spawn leaves a
        // window in which the child has already started binding sockets and spawning worker
        // threads on whatever CPU the scheduler picked, and a thread created before the affinity
        // change keeps the *old* mask -- so a post-spawn pin can silently miss exactly the threads
        // the measurement is about. Setting it between `fork` and `exec` means every thread the
        // process ever creates inherits the mask.
        let mask = pin.to_raw();
        // SAFETY: `pre_exec` runs in the forked child between `fork` and `exec`, where only
        // async-signal-safe work is allowed. The closure does exactly one thing --
        // `sched_setaffinity(2)`, a bare syscall -- and touches no allocator, no lock, and no
        // memory outside the `cpu_set_t` copied into it by value. `mask` is a plain `cpu_set_t`
        // living in the closure's own captured state, valid for the duration of the call.
        unsafe {
            command.pre_exec(move || {
                let rc = libc::sched_setaffinity(
                    0,
                    std::mem::size_of::<libc::cpu_set_t>(),
                    &mask as *const _,
                );
                if rc != 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }

    let mut child = command
        .spawn()
        // Both halves named: which program actually failed to spawn (the wrapper, when there is
        // one) and which binary it was going to run, since "no such file" is equally plausible
        // for either and the message has to say which.
        .with_context(|| match wrapper.first() {
            Some(program) => format!("spawning {program} around {}", logit_bin.display()),
            None => format!("spawning {}", logit_bin.display()),
        })?;
    let spawned_at = Instant::now();
    let pid = child.id() as libc::pid_t;

    // Stdout is the pipeline's own event stream (null_out discards it downstream, but the
    // process still owns the fd) -- drained on its own thread purely so a full pipe buffer can
    // never make the child block on a write nobody is reading.
    let mut stdout = child.stdout.take().expect("stdout was piped");
    let stdout_drain = std::thread::spawn(move || {
        let _ = std::io::copy(&mut stdout, &mut std::io::sink());
    });

    let stderr = child.stderr.take().expect("stderr was piped");
    let (event_tx, event_rx) = mpsc::channel::<ChildEvent>();
    // Filled in once the child's stderr ends, which for a healthy `logit` means the process exited
    // -- it never closes the stream itself. The cheapest honest liveness signal available here: no
    // extra thread, no `try_wait` poll, and the stream is already being watched for other reasons.
    // A driven blast reads it between batches, so a child that dies mid-run stops the sender
    // instead of letting it finish several seconds of traffic into a socket whose peer is gone.
    // The errno path cannot do this job -- see `crate::load::MAX_CONSECUTIVE_SEND_ERRORS`.
    let child_end = std::sync::Arc::new(std::sync::OnceLock::<String>::new());
    let child_end_writer = std::sync::Arc::clone(&child_end);
    let stderr_reader = std::thread::spawn(move || -> String {
        let (captured, end) = read_child_stderr(stderr, &event_tx);
        let _ = child_end_writer.set(end);
        captured
    });

    // Wall is measured from `ready`, not from spawn: spawn -> ready is process startup (loading
    // the binary, opening every listener's socket in the bind pass, spawning every node), which
    // no load even begins arriving at until it's done -- folding it into `wall` would count it as
    // part of the graph's own per-event cost.
    let deadline = Instant::now() + timeout;
    let mut ready_at: Option<Instant> = None;
    let mut load = None;
    let wall_ends_at;

    match drive {
        Drive::Generated { count } => {
            let completion = loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                match event_rx.recv_timeout(remaining) {
                    Ok(ChildEvent::Ready(at)) => ready_at = Some(at),
                    Ok(ChildEvent::Complete { at, events }) => break Some((at, events)),
                    Err(_) => break None,
                }
            };
            let Some((at, events)) = completion else {
                let stderr_text = kill_and_reap(&mut child, stdout_drain, stderr_reader);
                bail!(
                    "no `generation complete` line within {}s; stderr:\n{stderr_text}",
                    timeout.as_secs()
                );
            };
            if ready_at.is_none() {
                // `ready` missing at all (a binary built without the log line, or a race this
                // harness doesn't expect) falls back to the old spawn -> completion measurement,
                // loudly, rather than silently reporting a startup-inflated number.
                eprintln!(
                    "warning: no `ready` line observed before the completion line -- wall_s falls \
                     back to spawn -> completion, and startup_s is not recorded for this repeat"
                );
            }
            let events = match events {
                Some(events) => events,
                None => {
                    let stderr_text = kill_and_reap(&mut child, stdout_drain, stderr_reader);
                    bail!(
                        "`generation complete` line has no numeric `events` field; stderr:\n{stderr_text}"
                    );
                }
            };
            if events != count {
                let stderr_text = kill_and_reap(&mut child, stdout_drain, stderr_reader);
                bail!(
                    "generate_in reported {events} events but the scenario's `count` is {count} -- \
                     events/s and CPU us/event would be measured against the wrong denominator; \
                     stderr:\n{stderr_text}"
                );
            }
            wall_ends_at = at;
        }
        Drive::Driven { plan, pin_sender } => {
            // Hard-required, not best-effort: a driven scenario has no completion line to fall
            // back on, and sending before the listener's socket exists would be recorded as loss
            // that never happened (or, on a connected socket, as a burst of `ECONNREFUSED`).
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                match event_rx.recv_timeout(remaining) {
                    Ok(ChildEvent::Ready(at)) => {
                        ready_at = Some(at);
                        break;
                    }
                    // A driven scenario has no generator, so this can't happen -- but ignoring it
                    // rather than matching exhaustively-by-panic keeps a surprise in the child's
                    // logging from taking down the harness.
                    Ok(ChildEvent::Complete { .. }) => continue,
                    Err(_) => {
                        let stderr_text = kill_and_reap(&mut child, stdout_drain, stderr_reader);
                        bail!(
                            "no `ready` line within {}s -- a driven scenario cannot start sending \
                             until the listener's socket is bound; stderr:\n{stderr_text}",
                            timeout.as_secs()
                        );
                    }
                }
            }
            let abort = load::Abort { cause: &child_end };
            match load::blast(plan, pin_sender, Some(abort)) {
                Ok(outcome) => {
                    wall_ends_at = Instant::now();
                    load = Some(outcome);
                }
                Err(err) => {
                    let stderr_text = kill_and_reap(&mut child, stdout_drain, stderr_reader);
                    return Err(err).with_context(|| {
                        format!("blasting {} ; child stderr:\n{stderr_text}", plan.target)
                    });
                }
            }
        }
    }

    let (startup, wall) = match ready_at {
        Some(ready_at) => (Some(ready_at.duration_since(spawned_at)), wall_ends_at - ready_at),
        None => (None, wall_ends_at - spawned_at),
    };

    if needs_sigterm {
        std::thread::sleep(settle);
        // SAFETY: `pid` is this scenario's own child, spawned above and not yet reaped (neither
        // `child.wait()` nor `rusage::wait4` below has run yet), so it is a valid, still-live
        // signal target.
        let killed = unsafe { libc::kill(pid, libc::SIGTERM) };
        if killed != 0 {
            let _ = child.kill();
        }
    }

    // `wait4` blocks with no timeout of its own, so it runs on its own thread; the main thread
    // polls that thread's completion against `shutdown_timeout` and force-kills (SIGKILL) if the
    // process is still alive past it -- a hung drain, or a scenario whose graceful-shutdown path
    // is itself broken, would otherwise hang `logit-perf run` forever.
    let wait_thread = std::thread::spawn(move || rusage::wait4(pid, wall));
    let deadline = Instant::now() + shutdown_timeout;
    while !wait_thread.is_finished() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    if !wait_thread.is_finished() {
        let _ = child.kill();
        // The kill above should let the blocked `wait4` return promptly now; joined (not
        // dropped) so the syscall still completes and the child is actually reaped rather than
        // left a zombie.
        let _ = wait_thread.join();
        let stderr_text = drain_and_join(stdout_drain, stderr_reader);
        bail!(
            "process did not exit within {}s after SIGTERM/settle; stderr:\n{stderr_text}",
            shutdown_timeout.as_secs()
        );
    }
    let usage = wait_thread
        .join()
        .unwrap_or_else(|_| Err(io::Error::other("wait4 thread panicked")))
        .with_context(|| format!("wait4({pid})"))?;
    let stderr_text = drain_and_join(stdout_drain, stderr_reader);

    match usage.exit_code() {
        Some(0) => {}
        Some(code) => bail!("exited with status {code}; stderr:\n{stderr_text}"),
        // A wrapper dying by the SIGTERM this harness itself sent is a completed run, not a
        // failure -- verified against `perf record --scenario native-relay`, where perf forwards
        // the signal to `logit` (which drains and exits 0 on its own), waits for it, writes
        // `perf.data`, and then re-raises SIGTERM on itself, which is how a signal-terminated
        // process is *supposed* to report that it stopped on request. Deliberately not extended
        // to the no-wrapper case: `logit` installs its own SIGTERM handler and exits 0, so a
        // signal death there is a real regression in the shutdown path and must stay loud.
        None if !wrapper.is_empty()
            && needs_sigterm
            && usage.termination_signal() == Some(libc::SIGTERM) => {}
        None => bail!("terminated by signal (raw status {}); stderr:\n{stderr_text}", usage.status),
    }

    Ok(Measured { startup, usage, load })
}

/// Builds `logit` under `profile` (unless `no_build`) and returns the path to the binary,
/// failing loudly if it isn't there afterwards. Shared by `run`, `attribute`, and `flamegraph` --
/// all three measure the *same* binary and must agree on which one that is.
///
/// `logit_bin_override` is `--logit-bin`, honoured by `run`/`attribute` only (`flamegraph` always
/// passes `None`: it measures the `profiling` profile for its symbols, and a stashed release
/// binary would produce a useless capture). It **implies skipping the build** regardless of
/// `no_build` -- there is nothing to build toward, the binary is already named -- and a relative
/// path is resolved against `root`, not the process's cwd, so `--logit-bin perf/bins/foo/logit`
/// means the same thing typed on the host or inside the dev container.
pub(crate) fn build_and_locate(
    root: &Path,
    profile: &str,
    no_build: bool,
    logit_bin_override: Option<&Path>,
) -> anyhow::Result<PathBuf> {
    if let Some(path) = logit_bin_override {
        let resolved = if path.is_absolute() { path.to_path_buf() } else { root.join(path) };
        if !resolved.exists() {
            bail!("{} does not exist (--logit-bin)", resolved.display());
        }
        return Ok(resolved);
    }
    if !no_build {
        build(root, profile)?;
    }
    let target_dir_env = std::env::var_os("CARGO_TARGET_DIR");
    let logit_bin = logit_binary_path(root, profile, target_dir_env.as_deref().map(Path::new));
    if !logit_bin.exists() {
        bail!(
            "{} does not exist -- build it first (drop --no-build) or check --profile",
            logit_bin.display()
        );
    }
    Ok(logit_bin)
}

/// What `<bin>.json` records next to a binary `script/vm build` produced
/// (`docs/adr/disposable-azure-perf-vm.md`) -- the source string the operator/agent gave `build`
/// (a git ref, or a directory/tarball path for work that was never pushed), the commit it
/// resolved to when there was one, and when it was built. Parsed with serde's default
/// unknown-field tolerance, not `deny_unknown_fields`: the sidecar is free to carry more than
/// this crate reads (`script/vm build`'s own `sha256`/`profile`, say) without breaking this side.
#[derive(serde::Deserialize)]
struct BinarySidecar {
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    sha: Option<String>,
    #[serde(default)]
    built_at: Option<String>,
}

/// Reads `<logit_bin>.json` if it exists and parses -- absence is silent (an ordinary in-tree
/// build never has one), but a file that exists and doesn't parse warns and is otherwise ignored:
/// bad provenance metadata must never be able to fail a measurement over it.
fn read_binary_sidecar(logit_bin: &Path) -> Option<BinarySidecar> {
    let sidecar_path = logit_bin.with_extension("json");
    let contents = std::fs::read_to_string(&sidecar_path).ok()?;
    match serde_json::from_str(&contents) {
        Ok(sidecar) => Some(sidecar),
        Err(err) => {
            eprintln!(
                "warning: {} exists but did not parse as binary provenance ({err}) -- ignoring it",
                sidecar_path.display()
            );
            None
        }
    }
}

/// `sha256sum`'s own algorithm output for `path` -- shelled out to rather than pulling in a hash
/// crate, the same "no new dependency for W5" reasoning `format_rfc3339_utc_seconds` already
/// documents, and `sha256sum` is already on every image this runs in (coreutils).
fn sha256_file(path: &Path) -> anyhow::Result<String> {
    let output = Command::new("sha256sum")
        .arg(path)
        .output()
        .with_context(|| format!("running sha256sum {}", path.display()))?;
    if !output.status.success() {
        bail!("sha256sum {} failed", path.display());
    }
    String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .next()
        .map(str::to_string)
        .context("sha256sum produced no output")
}

/// The identity recorded for the binary a run actually spawns -- always populated, `--logit-bin`
/// or not, so a results file never leaves "what did this measure" to be inferred from `git`/
/// `profile` alone (`result::BinaryInfo`'s own doc has why the two can differ).
fn binary_info(logit_bin: &Path) -> BinaryInfo {
    let sha256 = sha256_file(logit_bin).unwrap_or_else(|err| {
        eprintln!("warning: could not sha256 {}: {err:#}", logit_bin.display());
        "unknown".to_string()
    });
    let sidecar = read_binary_sidecar(logit_bin);
    BinaryInfo {
        path: logit_bin.to_string_lossy().into_owned(),
        sha256,
        source_ref: sidecar.as_ref().and_then(|s| s.source.clone()),
        source_sha: sidecar.as_ref().and_then(|s| s.sha.clone()),
        built_at: sidecar.and_then(|s| s.built_at),
    }
}

/// The first 12 characters of a hex sha (git or sha256) -- the one truncation convention this
/// crate uses everywhere a full hash would be unreadable clutter.
fn short12(sha: &str) -> String {
    sha.chars().take(12).collect()
}

/// The short-sha component of a results filename.
///
/// **`logit_bin_overridden` is what actually decides this, not merely whether `binary` carries a
/// `source_sha`.** For an ordinary build (`--logit-bin` not given) the checkout's own `git.sha` is
/// exactly what was measured, and naming the file after it is the existing, stable convention --
/// changing it to `binary.sha256` instead would rename every ordinary run's results after a hash
/// that isn't even guaranteed to reproduce across two builds of the *same* commit, breaking
/// filename-based correlation for no benefit. `--logit-bin` is the one case `git.sha` can honestly
/// diverge from what was measured (a stashed binary from a different ref, or no ref at all), which
/// is exactly when the binary's own identity has to win: its sidecar's resolved commit
/// (`source_sha`) when the source was a git ref, else its `sha256` (a tarball or dirty-tree source
/// has no commit, but it is still its own identity).
fn short_provenance_sha(report: &RunReport, logit_bin_overridden: bool) -> String {
    if logit_bin_overridden {
        if let Some(binary) = &report.binary {
            if let Some(sha) = &binary.source_sha {
                return short12(sha);
            }
            return short12(&binary.sha256);
        }
    }
    report.git.sha.as_deref().map(short12).unwrap_or_else(|| "unknown".to_string())
}

/// `perf/results/<compact-utc-timestamp>-<short-sha>[-<label>].json` -- see
/// [`short_provenance_sha`] for which sha wins, and `run`'s own doc for the label suffix.
fn result_filename(
    report: &RunReport,
    now_unix_seconds: i64,
    logit_bin_overridden: bool,
) -> String {
    let short_sha = short_provenance_sha(report, logit_bin_overridden);
    let label_suffix = report
        .label
        .as_deref()
        .map(|label| format!("-{}", sanitize_label(label)))
        .unwrap_or_default();
    format!("{}-{short_sha}{label_suffix}.json", compact_utc_now(now_unix_seconds))
}

fn build(root: &Path, profile: &str) -> anyhow::Result<()> {
    println!("-- building -p logit-cli --profile {profile}");
    let status = Command::new("cargo")
        .current_dir(root)
        .args(["build", "--profile", profile, "-p", "logit-cli"])
        .status()
        .context("spawning cargo build")?;
    if !status.success() {
        bail!("cargo build --profile {profile} -p logit-cli failed");
    }
    Ok(())
}

/// Where `cargo build --profile <profile> -p logit-cli` puts the binary. `dev` is cargo's one
/// irregular case (`target/debug`, not `target/dev`); every other profile name, `release`
/// included, is used as its own directory name verbatim.
///
/// `target_dir_override` is `$CARGO_TARGET_DIR` when set, read once by the caller -- kept as a
/// plain parameter rather than read from the environment in here so this stays a pure function
/// tests can call directly, with no process-global env mutation needed to exercise the override.
fn logit_binary_path(root: &Path, profile: &str, target_dir_override: Option<&Path>) -> PathBuf {
    let target_dir =
        target_dir_override.map(Path::to_path_buf).unwrap_or_else(|| root.join("target"));
    let profile_dir = if profile == "dev" { "debug" } else { profile };
    target_dir.join(profile_dir).join("logit")
}

/// The commit and dirty-state of the binary under test. Two sources, preferred in order:
///
/// 1. `LOGIT_PERF_GIT_SHA`/`LOGIT_PERF_GIT_DIRTY` -- set by `script/perf` itself, computed on the
///    *host* and passed as `env VAR=... cargo run ...` argv ahead of the `cargo run` it execs
///    into the dev container, not through `compose.yaml`'s `environment:` block: `run()`'s
///    `sudo docker compose run` strips the calling shell's own environment before `docker
///    compose` ever gets to interpolate a `${VAR}` there, so an `environment:` entry can never
///    see a value exported in `script/perf`, while argv reaches the container (or, in CI, the
///    directly-exec'd process) unchanged either way. This is the reliable path: a git-worktree
///    checkout's `.git` file points at an absolute host path the dev container's bind mount
///    doesn't include, so `git` run *inside* the container against a worktree checkout routinely
///    can't answer at all.
/// 2. Shelling out to `git` against `root` directly -- works for an ordinary (non-worktree)
///    checkout, or when running `logit-perf` outside the dev container entirely.
///
/// `None` (rendered as JSON `null`, printed as "unknown") if neither source has an answer, rather
/// than a confident-looking default.
fn git_info(root: &Path) -> GitInfo {
    let sha = non_empty_env("LOGIT_PERF_GIT_SHA").or_else(|| git_rev_parse_head(root));
    let dirty = env_git_dirty().or_else(|| git_status_dirty(root));
    GitInfo { sha, dirty }
}

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn env_git_dirty() -> Option<bool> {
    match non_empty_env("LOGIT_PERF_GIT_DIRTY").as_deref() {
        Some("1") => Some(true),
        Some("0") => Some(false),
        _ => None,
    }
}

fn git_rev_parse_head(root: &Path) -> Option<String> {
    Command::new("git")
        .current_dir(root)
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .filter(|sha| !sha.is_empty())
}

fn git_status_dirty(root: &Path) -> Option<bool> {
    Command::new("git")
        .current_dir(root)
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| !out.stdout.is_empty())
}

fn hostname() -> String {
    let mut buf = vec![0u8; 256];
    // SAFETY: `buf` is a valid, uniquely-owned buffer of `buf.len()` bytes for the duration of
    // the call, exactly what `gethostname(2)` requires of its out-parameter.
    let ret = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
    if ret != 0 {
        return "unknown".to_string();
    }
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..end]).into_owned()
}

/// Reads a sysfs one-liner, or `None` if it isn't there / isn't readable. Everything
/// [`box_state`] wants is optional by construction, so a missing file is an answer, not an error.
fn sysfs_line(path: &str) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// The box's power/thermal policy, best-effort -- see [`BoxState`] for why it's recorded.
///
/// Checked against this repo's own dev container: the two `cpufreq` files are visible inside it,
/// `/sys/firmware/acpi/platform_profile` does not exist on this box at all, and the mains supply
/// is `ACAD` rather than `AC` -- hence the scan by `type` below rather than a hard-coded name.
fn box_state() -> BoxState {
    let mains = std::fs::read_dir("/sys/class/power_supply")
        .ok()
        .into_iter()
        .flatten()
        .flatten()
        .find(|entry| {
            sysfs_line(&entry.path().join("type").to_string_lossy()).as_deref() == Some("Mains")
        })
        .and_then(|entry| sysfs_line(&entry.path().join("online").to_string_lossy()))
        .map(|online| online == "1");

    BoxState {
        scaling_governor: sysfs_line("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"),
        energy_performance_preference: sysfs_line(
            "/sys/devices/system/cpu/cpu0/cpufreq/energy_performance_preference",
        ),
        platform_profile: sysfs_line("/sys/firmware/acpi/platform_profile"),
        on_ac_power: mains,
    }
}

fn cpu_model() -> String {
    std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|contents| {
            contents
                .lines()
                .find_map(|line| {
                    line.strip_prefix("model name")
                        .and_then(|rest| rest.trim_start().strip_prefix(':'))
                })
                .map(|value| value.trim().to_string())
        })
        .unwrap_or_else(|| "unknown".to_string())
}

fn rustc_version() -> String {
    Command::new("rustc")
        .arg("-V")
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn sanitize_label(label: &str) -> String {
    label
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect()
}

fn now_unix_seconds() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

/// `YYYY-MM-DDTHH:MM:SSZ`, second precision -- the harness's own results only ever need to be
/// ordered and human-legible, not sub-second precise. Hand-rolled rather than pulling in a
/// date/time crate: no new dependency is authorized for this workstream (docs/plans/load-test-
/// harness.md's W5 row lists `clap`/`serde`/`serde_json`/`serde_norway`/`anyhow`/`libc` only),
/// and `crates/logit-core/src/time.rs` already sets the precedent for hand-rolling this exact
/// civil-from-days algorithm (Howard Hinnant's) rather than reaching for one; not shared with it
/// directly since this crate deliberately doesn't depend on `logit-core` for W5.
fn format_rfc3339_utc_seconds(unix_seconds: i64) -> String {
    let days = unix_seconds.div_euclid(86_400);
    let secs_of_day = unix_seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// Same instant as [`format_rfc3339_utc_seconds`], filesystem-safe: no `:` (illegal on some
/// filesystems, awkward to shell-quote on all of them).
fn compact_utc_now(unix_seconds: i64) -> String {
    let days = unix_seconds.div_euclid(86_400);
    let secs_of_day = unix_seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;
    format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z")
}

/// Howard Hinnant's `civil_from_days`
/// (<http://howardhinnant.github.io/date_algorithms.html#civil_from_days>), exact over the full
/// `i64` day range -- see `crates/logit-core/src/time.rs`'s copy of the same algorithm for the
/// derivation this mirrors.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = if month <= 2 { y + 1 } else { y };
    (year, month, day)
}

fn print_table(report: &RunReport) {
    println!(
        "\n{:<22} {:>12} {:>14} {:>12} {:>10}",
        "scenario", "events/s", "us/event", "peak RSS", "startup"
    );
    for (name, scenario) in &report.scenarios {
        println!(
            "{:<22} {:>12.0} {:>14.3} {:>9.1} MiB {:>10}",
            name,
            scenario.median.events_per_s,
            scenario.median.cpu_us_per_event,
            scenario.median.max_rss_bytes as f64 / (1024.0 * 1024.0),
            format_startup(scenario.median.startup_s),
        );
    }
}

/// The socket-side half of a driven scenario's story, printed only when there is one. A separate
/// table rather than more columns on the first: seven of these nine numbers are `null` for every
/// generated scenario, and a table two-thirds empty reads worse than two tables.
fn print_udp_table(report: &RunReport) {
    let driven: Vec<(&String, &UdpSample)> = report
        .scenarios
        .iter()
        .filter_map(|(name, scenario)| scenario.median.udp.as_ref().map(|udp| (name, udp)))
        .collect();
    if driven.is_empty() {
        return;
    }
    // The rate is in the table, not only in the JSON: two runs at different `--rate-scale`s are
    // different points on the load curve, and reading a drop rate without knowing what pace
    // produced it is reading half the result. An unscaled run has to print it too, or the only
    // time the number is visible is the time it was changed.
    println!(
        "\n{:<22} {:>11} {:>11} {:>7} {:>11} {:>11} {:>12} {:>8} {:>9} {:>11}",
        "scenario",
        "sent dg",
        "recv dg",
        "fill",
        "kern drop",
        "queue drop",
        "delivered",
        "drop %",
        "rcvbuf",
        "rate dg/s"
    );
    for (name, udp) in driven {
        println!(
            "{:<22} {:>11} {:>11} {:>7} {:>11} {:>11} {:>12} {:>7.2}% {:>8.2} {:>11}",
            name,
            udp.sent_datagrams,
            udp.received_datagrams,
            udp.mean_fill().map_or_else(|| "-".to_string(), |fill| format!("{fill:.1}")),
            udp.kernel_dropped,
            udp.queue_dropped,
            udp.events_delivered,
            100.0 * udp.drop_rate(),
            udp.kernel_rcvbuf_utilization_max,
            udp.effective_rate
                .map(|rate| rate.to_string())
                .unwrap_or_else(|| "unpaced".to_string()),
        );
    }
}

/// The per-repeat line's UDP tail: only the two numbers worth reading at a glance while a run is
/// still going, with the full picture left to [`print_udp_table`].
fn format_udp_suffix(udp: UdpSample) -> String {
    format!(
        ", {} sent / {} delivered ({:.2}% dropped)",
        udp.sent_datagrams,
        udp.events_delivered,
        100.0 * udp.drop_rate()
    )
}

/// Renders an optional startup time the way every "unknown" numeric field in this crate's output
/// reads -- never a bare Rust `None`. Shared by the per-repeat line and the summary table.
fn format_startup(startup_s: Option<f64>) -> String {
    startup_s.map(|s| format!("{s:.3}s")).unwrap_or_else(|| "n/a".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, Event, MetricRecord};

    #[test]
    fn logit_binary_path_maps_the_dev_profile_to_the_debug_directory() {
        let root = Path::new("/repo");
        assert_eq!(logit_binary_path(root, "dev", None), Path::new("/repo/target/debug/logit"));
        assert_eq!(
            logit_binary_path(root, "release", None),
            Path::new("/repo/target/release/logit")
        );
        assert_eq!(
            logit_binary_path(root, "profiling", None),
            Path::new("/repo/target/profiling/logit")
        );
    }

    #[test]
    fn logit_binary_path_honors_an_explicit_target_dir_override() {
        let root = Path::new("/repo");
        let target_dir = Path::new("/custom/target");
        assert_eq!(
            logit_binary_path(root, "release", Some(target_dir)),
            Path::new("/custom/target/release/logit")
        );
    }

    #[test]
    fn parse_completion_line_requires_an_exact_message_match() {
        assert_eq!(
            parse_completion_line(r#"{"message":"generation complete","events":5000000}"#),
            Some(Some(5_000_000))
        );
        assert_eq!(parse_completion_line(r#"{"message":"starting up"}"#), None);
        // A message that merely contains the phrase must not match -- exact equality only.
        assert_eq!(
            parse_completion_line(r#"{"message":"about to log generation complete soon"}"#),
            None
        );
        assert_eq!(parse_completion_line("not json at all"), None);
    }

    #[test]
    fn parse_completion_line_reports_a_missing_events_field_as_some_none() {
        assert_eq!(parse_completion_line(r#"{"message":"generation complete"}"#), Some(None));
    }

    #[test]
    fn is_ready_line_requires_an_exact_message_match() {
        assert!(is_ready_line(r#"{"message":"ready"}"#));
        assert!(is_ready_line(r#"{"level":"INFO","target":"logit","message":"ready"}"#));
        assert!(!is_ready_line(r#"{"message":"generation complete","events":100}"#));
        // A message that merely contains the word must not match -- exact equality only, same
        // reasoning as `parse_completion_line`.
        assert!(!is_ready_line(r#"{"message":"getting ready to bind"}"#));
        assert!(!is_ready_line("not json at all"));
    }

    /// A line that isn't UTF-8 must not look like the child exiting.
    ///
    /// `BufRead::lines` returns `Err(InvalidData)` for one, which the old reader could only treat
    /// as the end of the stream -- so a single stray byte (a panic message cut mid-character, a
    /// library writing raw bytes) both truncated the capture and told a running blast the child had
    /// died. The `ready` line *after* the bad one is the part that proves the loop carried on.
    #[test]
    fn a_non_utf8_line_is_captured_lossily_and_does_not_end_the_stream() {
        let (tx, rx) = mpsc::channel::<ChildEvent>();
        let stderr: &[u8] = b"\xff\xfe not valid utf-8\n{\"message\":\"ready\"}\nafter\n";

        let (captured, end) = read_child_stderr(io::Cursor::new(stderr), &tx);

        assert!(
            matches!(rx.try_recv(), Ok(ChildEvent::Ready(_))),
            "the ready line after the bad one must still be announced"
        );
        assert!(captured.contains("not valid utf-8"), "kept lossily: {captured:?}");
        assert!(captured.contains('\u{fffd}'), "with replacement characters: {captured:?}");
        assert!(captured.ends_with("after"), "and the loop ran to the end: {captured:?}");
        assert!(end.contains("reached EOF"), "a real EOF, not a read error: {end}");
    }

    #[test]
    fn the_end_reason_tells_an_eof_apart_from_a_failed_read() {
        /// A reader that hands back one good line and then fails, the way a broken pipe would.
        struct FailAfterFirst(bool);
        impl io::Read for FailAfterFirst {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if self.0 {
                    return Err(io::Error::other("the pipe broke"));
                }
                self.0 = true;
                let line = b"{\"message\":\"ready\"}\n";
                buf[..line.len()].copy_from_slice(line);
                Ok(line.len())
            }
        }

        let (tx, rx) = mpsc::channel::<ChildEvent>();
        let (captured, end) = read_child_stderr(FailAfterFirst(false), &tx);
        assert!(matches!(rx.try_recv(), Ok(ChildEvent::Ready(_))));
        assert!(captured.contains("ready"), "{captured}");
        assert!(end.contains("could not read"), "a read error says so, not `exited`: {end}");
    }

    #[test]
    fn stderr_capture_keeps_only_the_last_64_kib() {
        let mut capture = StderrCapture::new();
        let line = "x".repeat(1024);
        for _ in 0..100 {
            capture.push(line.clone());
        }
        let captured = capture.into_string();
        assert!(captured.len() <= StderrCapture::CAP_BYTES, "{}", captured.len());
        assert!(captured.ends_with(&line), "should keep the most recent lines");
    }

    #[test]
    fn civil_from_days_recovers_the_unix_epoch() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
    }

    #[test]
    fn format_rfc3339_utc_seconds_renders_a_known_instant() {
        // 2026-09-12T00:00:00Z, computed independently: days since epoch for 2026-09-12.
        let days = civil_days_since_epoch_for_test(2026, 9, 12);
        let unix_seconds = days * 86_400 + 12 * 3600 + 34 * 60 + 56;
        assert_eq!(format_rfc3339_utc_seconds(unix_seconds), "2026-09-12T12:34:56Z");
        assert_eq!(compact_utc_now(unix_seconds), "20260912T123456Z");
    }

    /// Round-trips [`civil_from_days`] by linear search from a nearby known point, purely to
    /// build the test fixture above without hand-computing a day count.
    fn civil_days_since_epoch_for_test(year: i64, month: u32, day: u32) -> i64 {
        (0..40_000).find(|&d| civil_from_days(d) == (year, month, day)).expect("date in range")
    }

    #[test]
    fn sanitize_label_replaces_unsafe_characters() {
        assert_eq!(sanitize_label("baseline v2"), "baseline_v2");
        assert_eq!(sanitize_label("a/b:c"), "a_b_c");
        assert_eq!(sanitize_label("fine-name_1"), "fine-name_1");
    }

    /// One point as an `internal` drain would emit it, under the identity attributes
    /// `ComponentBuffer::drain` stamps on every one of them.
    fn point(component: &str, metric: &str, kind: MetricKind) -> Event {
        let mut attrs = AttrMap::new();
        attrs.insert("component", component);
        Event::metric(0, attrs, MetricRecord::new(interner::intern(metric), kind))
    }

    fn keyed_point(component: &str, metric: &str, key: &str, kind: MetricKind) -> Event {
        let mut attrs = AttrMap::new();
        attrs.insert("component", component);
        attrs.insert("key", key);
        Event::metric(0, attrs, MetricRecord::new(interner::intern(metric), kind))
    }

    #[test]
    fn input_stats_sums_counters_and_takes_the_high_water_mark_of_the_gauge() {
        let events = vec![
            point("statsd", INPUT_DATAGRAMS, MetricKind::counter(600.0)),
            point("statsd", INPUT_DATAGRAMS, MetricKind::counter(400.0)),
            point("statsd", KERNEL_DROPS, MetricKind::counter(7.0)),
            point("statsd", KERNEL_DROPS, MetricKind::counter(3.0)),
            point("statsd", DATAGRAMS_DROPPED, MetricKind::counter(5.0)),
            point("statsd", RCVBUF_UTILIZATION, MetricKind::Gauge(0.25)),
            point("statsd", RCVBUF_UTILIZATION, MetricKind::Gauge(0.91)),
            point("statsd", RCVBUF_UTILIZATION, MetricKind::Gauge(0.10)),
            // Another component's numbers must not be folded in.
            point("out", INPUT_DATAGRAMS, MetricKind::counter(999.0)),
        ];
        let stats = input_stats(&events, "statsd");
        assert_eq!(stats.datagrams, 1_000);
        assert_eq!(stats.kernel_drops, 10);
        assert_eq!(stats.queue_dropped, 5);
        assert!((stats.rcvbuf_utilization_max - 0.91).abs() < 1e-9);
        assert!(stats.diagnostics.is_empty());
    }

    #[test]
    fn input_stats_keeps_diagnostics_apart_by_key() {
        let events = vec![
            keyed_point("statsd", DIAGNOSTICS, "bad_line", MetricKind::counter(3.0)),
            keyed_point("statsd", DIAGNOSTICS, "bad_line", MetricKind::counter(4.0)),
            keyed_point("statsd", DIAGNOSTICS, "bad_datagram", MetricKind::counter(1.0)),
        ];
        let stats = input_stats(&events, "statsd");
        assert_eq!(stats.diagnostics.get("bad_line"), Some(&7));
        assert_eq!(stats.diagnostics.get("bad_datagram"), Some(&1));
    }

    #[test]
    fn a_kernel_drops_metric_that_never_appeared_reads_as_zero() {
        // `logit.input.kernel.drops` is not emitted when its delta is zero, so "absent" has to
        // mean "none", not "unknown".
        let stats =
            input_stats(&[point("statsd", INPUT_DATAGRAMS, MetricKind::counter(5.0))], "statsd");
        assert_eq!(stats.kernel_drops, 0);
    }

    fn driven_scenario() -> Scenario {
        let spec: load::LoadSpec = serde_norway::from_str(
            "target: statsd\ndatagrams: 100\nmodel: m.yaml\ndatagram_mix:\n  - { weight: 1, single: true }\n",
        )
        .unwrap();
        Scenario {
            name: "udp-statsd".to_string(),
            path: PathBuf::from("/repo/perf/scenarios/udp-statsd.yaml"),
            workload: Workload::Driven(spec),
            needs_sigterm: true,
            disk_spool_paths: Vec::new(),
        }
    }

    fn plan_for(datagrams: u64) -> LoadPlan {
        let spec: load::LoadSpec = serde_norway::from_str(&format!(
            "target: statsd\ndatagrams: {datagrams}\nring_datagrams: 8\nmodel: m.yaml\ndatagram_mix:\n  - {{ weight: 1, single: true }}\n"
        ))
        .unwrap();
        let model: load::LineModel =
            serde_norway::from_str("lines:\n  - { weight: 1, template: \"a.{seq%3}:1|c\" }\n")
                .unwrap();
        let ring = load::Ring::render(&spec, &model).unwrap();
        LoadPlan {
            spec,
            spec_path: PathBuf::from("/repo/perf/load/udp-statsd.yaml"),
            ring,
            target: "127.0.0.1:18125".parse().unwrap(),
        }
    }

    fn udp_sample(sent: u64, received: u64, kernel: u64, delivered: u64) -> UdpSample {
        UdpSample {
            sent_datagrams: sent,
            sent_lines: sent,
            received_datagrams: received,
            reads: received,
            kernel_dropped: kernel,
            queue_dropped: 0,
            events_delivered: delivered,
            send_errors: 0,
            kernel_rcvbuf_utilization_max: 0.5,
            effective_rate: Some(90_000),
        }
    }

    /// An `InputStats` from a run whose kernel sampler worked -- the ordinary case. The `Default`
    /// has `kernel_sampled: false`, which now means "`SO_MEMINFO` was unavailable" and is its own
    /// failure, so every test about a *later* check has to start from a sampled run.
    fn sampled() -> InputStats {
        InputStats { kernel_sampled: true, ..InputStats::default() }
    }

    #[test]
    fn self_check_rejects_a_run_whose_kernel_sampler_never_reported() {
        // What a kernel older than 4.12, or a sandbox that blocks `getsockopt(SO_MEMINFO)`, looks
        // like from here: datagrams arrived, no `receive_buffer.*` gauge ever did. Detected by the
        // gauge's *presence*, since every value it carries is legitimately zero at times.
        let scenario = driven_scenario();
        let plan = plan_for(100);
        let udp = udp_sample(100, 100, 0, 100);
        let err = self_check(&scenario, &plan, &udp, &InputStats::default(), false)
            .expect_err("without kernel counters a driven run cannot account for its datagrams");
        let err = format!("{err:#}");
        assert!(err.contains("never reported"), "{err}");
        assert!(err.contains("SO_MEMINFO"), "{err}");
        // And specifically *not* the settle advice, which would be a red herring here.
        assert!(!err.contains("--settle"), "{err}");
    }

    #[test]
    fn a_run_that_received_nothing_is_not_blamed_on_the_kernel_sampler() {
        // Nothing arrived, so no sample is expected either: this has to fall through to the
        // accounting check, which is the one with something useful to say.
        let scenario = driven_scenario();
        let plan = plan_for(100);
        let err =
            self_check(&scenario, &plan, &udp_sample(100, 0, 0, 0), &InputStats::default(), false)
                .expect_err("100 sent, nothing accounted for");
        assert!(format!("{err:#}").contains("does not close"), "{err:#}");
    }

    #[test]
    fn self_check_accepts_a_run_whose_accounting_closes() {
        let scenario = driven_scenario();
        let plan = plan_for(100);
        let udp = udp_sample(100, 90, 10, 90);
        self_check(&scenario, &plan, &udp, &sampled(), false).unwrap();
    }

    #[test]
    fn self_check_rejects_a_run_whose_accounting_does_not_close() {
        let scenario = driven_scenario();
        let plan = plan_for(100);
        let udp = udp_sample(100, 90, 5, 90);
        let err = self_check(&scenario, &plan, &udp, &sampled(), false)
            .expect_err("95 accounted for out of 100 sent");
        let err = format!("{err:#}");
        assert!(err.contains("does not close"), "{err}");
        assert!(err.contains("off by 5"), "{err}");
    }

    #[test]
    fn self_check_rejects_any_decode_diagnostic() {
        let scenario = driven_scenario();
        let plan = plan_for(100);
        let udp = udp_sample(100, 100, 0, 100);
        let mut input = sampled();
        input.diagnostics.insert("bad_line".to_string(), 12);
        let err = self_check(&scenario, &plan, &udp, &input, false)
            .expect_err("a malformed line means the wrong path is being measured");
        assert!(format!("{err:#}").contains("bad_line=12"), "{err:#}");
    }

    #[test]
    fn verify_requires_zero_drops_and_an_exact_delivered_count() {
        let scenario = driven_scenario();
        let plan = plan_for(100);
        // The ring is one single-value counter line per datagram, so 100 datagrams is 100 events.
        assert_eq!(plan.expected().events, 100);

        self_check(&scenario, &plan, &udp_sample(100, 100, 0, 100), &sampled(), true)
            .expect("an exact, lossless run verifies");

        let dropped = self_check(&scenario, &plan, &udp_sample(100, 99, 1, 99), &sampled(), true)
            .expect_err("--verify requires a zero-drop run");
        assert!(format!("{dropped:#}").contains("zero-drop"), "{dropped:#}");

        let short = self_check(&scenario, &plan, &udp_sample(100, 100, 0, 97), &sampled(), true)
            .expect_err("--verify requires an exact delivered count");
        assert!(format!("{short:#}").contains("exactly 100 events"), "{short:#}");
    }

    #[test]
    fn build_and_locate_rejects_a_missing_logit_bin_override() {
        let root = Path::new("/repo");
        let err = build_and_locate(root, "release", false, Some(Path::new("no/such/logit")))
            .expect_err("a --logit-bin path that doesn't exist must fail");
        assert!(format!("{err:#}").contains("--logit-bin"), "{err:#}");
    }

    #[test]
    fn build_and_locate_resolves_a_relative_logit_bin_override_against_root() {
        let dir = tempfile_dir();
        let bin_path = dir.join("stashed-logit");
        std::fs::write(&bin_path, b"not a real binary, just bytes to hash").unwrap();

        let resolved =
            build_and_locate(&dir, "release", false, Some(Path::new("stashed-logit"))).unwrap();
        assert_eq!(resolved, bin_path);
    }

    #[test]
    fn build_and_locate_accepts_an_absolute_logit_bin_override() {
        let dir = tempfile_dir();
        let bin_path = dir.join("stashed-logit");
        std::fs::write(&bin_path, b"bytes").unwrap();

        let resolved =
            build_and_locate(Path::new("/unrelated"), "release", false, Some(&bin_path)).unwrap();
        assert_eq!(resolved, bin_path);
    }

    /// A directory unique to this test process and this call, cleaned up on drop -- these tests
    /// touch the real filesystem (a real `sha256sum` invocation, a real sidecar file) rather than
    /// mocking either, since both are cheap and the point is proving the real plumbing works.
    fn tempfile_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "logit-perf-run-tests-{}-{}",
            std::process::id(),
            unique_suffix()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn unique_suffix() -> u64 {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        COUNTER.fetch_add(1, Ordering::Relaxed)
    }

    #[test]
    fn sha256_file_matches_a_known_vector() {
        let dir = tempfile_dir();
        let path = dir.join("hello.txt");
        std::fs::write(&path, b"hello world\n").unwrap();
        // sha256("hello world\n"), a standard test vector.
        assert_eq!(
            sha256_file(&path).unwrap(),
            "a948904f2f0f479b8f8197694b30184b0d2ed1c1cd2a1ec0fb85d299a192a447"
        );
    }

    #[test]
    fn read_binary_sidecar_returns_none_when_no_sidecar_exists() {
        let dir = tempfile_dir();
        let bin_path = dir.join("logit");
        assert!(read_binary_sidecar(&bin_path).is_none());
    }

    #[test]
    fn read_binary_sidecar_parses_a_valid_sidecar() {
        let dir = tempfile_dir();
        let bin_path = dir.join("logit");
        std::fs::write(
            dir.join("logit.json"),
            r#"{"source": "udp/w3", "sha": "abc123", "sha256": "ignored", "built_at": "2026-09-18T00:00:00Z"}"#,
        )
        .unwrap();
        let sidecar = read_binary_sidecar(&bin_path).expect("a valid sidecar must parse");
        assert_eq!(sidecar.source.as_deref(), Some("udp/w3"));
        assert_eq!(sidecar.sha.as_deref(), Some("abc123"));
        assert_eq!(sidecar.built_at.as_deref(), Some("2026-09-18T00:00:00Z"));
    }

    #[test]
    fn read_binary_sidecar_ignores_a_malformed_sidecar_rather_than_failing() {
        let dir = tempfile_dir();
        let bin_path = dir.join("logit");
        std::fs::write(dir.join("logit.json"), "not json at all").unwrap();
        assert!(read_binary_sidecar(&bin_path).is_none());
    }

    fn report_with_binary(git_sha: Option<&str>, binary: Option<BinaryInfo>) -> RunReport {
        RunReport {
            git: GitInfo { sha: git_sha.map(str::to_string), dirty: Some(false) },
            timestamp: "2026-09-18T00:00:00Z".to_string(),
            hostname: "devbox".to_string(),
            cpu_model: "cpu".to_string(),
            nproc: 4,
            rustc: "rustc 1.98.1".to_string(),
            profile: "release".to_string(),
            label: None,
            box_state: None,
            binary,
            scenarios: BTreeMap::new(),
        }
    }

    #[test]
    fn short_provenance_sha_ignores_binary_info_when_logit_bin_was_not_used() {
        // The ordinary, overwhelmingly common case: no --logit-bin, so the checkout's own git.sha
        // is exactly what was measured, and the filename convention every existing doc/tool
        // expects must not change. `binary` is populated here too (it always is), but it must
        // not win -- and specifically must not be read at all, since even its *presence* used to
        // be enough to change the answer before this test existed.
        let report = report_with_binary(
            Some("checkoutsha1234"),
            Some(BinaryInfo {
                path: "/x/logit".to_string(),
                sha256: "f".repeat(64),
                source_ref: None,
                source_sha: None,
                built_at: None,
            }),
        );
        assert_eq!(short_provenance_sha(&report, false), "checkoutsha1");
    }

    #[test]
    fn short_provenance_sha_prefers_the_binarys_own_source_sha_under_logit_bin() {
        let report = report_with_binary(
            Some("checkoutsha1234"),
            Some(BinaryInfo {
                path: "/x/logit".to_string(),
                sha256: "f".repeat(64),
                source_ref: Some("udp/w3".to_string()),
                source_sha: Some("binarycommitsha5678".to_string()),
                built_at: None,
            }),
        );
        assert_eq!(short_provenance_sha(&report, true), "binarycommit");
    }

    #[test]
    fn short_provenance_sha_falls_back_to_the_binarys_sha256_with_no_commit() {
        // The tarball/dirty-tree case under --logit-bin: a binary with no resolvable commit is
        // still its own identity, and a results file named `unknown` for it would be exactly the
        // gap this exists to close.
        let report = report_with_binary(
            Some("checkoutsha1234"),
            Some(BinaryInfo {
                path: "/x/logit".to_string(),
                sha256: "abcdef0123456789".to_string(),
                source_ref: Some("dir".to_string()),
                source_sha: None,
                built_at: None,
            }),
        );
        assert_eq!(short_provenance_sha(&report, true), "abcdef012345");
    }

    #[test]
    fn short_provenance_sha_falls_back_to_git_sha_when_logit_bin_carries_no_binary_info() {
        // Defensive: --logit-bin was used, but for some reason `binary` itself is absent (a
        // results file this function is asked to name outside `run`'s own path). Falling back to
        // git.sha here is better than a bare "unknown" when there's a real sha available.
        let report = report_with_binary(Some("checkoutsha1234"), None);
        assert_eq!(short_provenance_sha(&report, true), "checkoutsha1");
    }

    #[test]
    fn short_provenance_sha_is_unknown_with_nothing_to_go_on() {
        let report = report_with_binary(None, None);
        assert_eq!(short_provenance_sha(&report, false), "unknown");
    }

    #[test]
    fn result_filename_appends_the_label_suffix_when_present() {
        let mut report = report_with_binary(Some("checkoutsha1234"), None);
        report.label = Some("baseline v2".to_string());
        let filename = result_filename(&report, 0, false);
        assert!(filename.ends_with("-checkoutsha1-baseline_v2.json"), "{filename}");
    }

    #[test]
    fn result_filename_names_the_binary_not_the_checkout_under_logit_bin() {
        let report = report_with_binary(
            Some("checkoutsha1234"),
            Some(BinaryInfo {
                path: "/x/logit".to_string(),
                sha256: "f".repeat(64),
                source_ref: Some("udp/w3".to_string()),
                source_sha: Some("binarycommitsha5678".to_string()),
                built_at: None,
            }),
        );
        let filename = result_filename(&report, 0, true);
        assert!(filename.starts_with("19700101T000000Z-binarycommit"), "{filename}");
    }

    #[test]
    fn a_driven_scenario_never_settles_for_less_than_the_floor() {
        // Not a test of `run_one_driven` (which needs a child process) but of the one line in it
        // that matters for the accounting check: whatever `--settle` says, a driven scenario waits
        // at least `DRIVEN_SETTLE_FLOOR`.
        assert_eq!(Duration::from_millis(1).max(DRIVEN_SETTLE_FLOOR), DRIVEN_SETTLE_FLOOR);
        assert_eq!(Duration::from_secs(10).max(DRIVEN_SETTLE_FLOOR), Duration::from_secs(10));
    }
}
