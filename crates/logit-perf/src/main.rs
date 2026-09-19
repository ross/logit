//! `logit-perf`: the out-of-CI load-test harness's CLI (docs/adr/load-test-harness.md,
//! docs/plans/load-test-harness.md). Spawns the real, release-profile `logit` binary against
//! `perf/scenarios/*.yaml` and measures throughput, CPU per event, and peak RSS -- see `run.rs`
//! for the measurement itself, `compare.rs` for before/after diffing, and `scenario.rs` for what
//! it reads out of a scenario file.
//!
//! Deliberately not run by `script/cibuild` (`script/perf`'s own header comment says why, the
//! same reason `script/bench` gives): this loads the machine heavily and its numbers are only
//! meaningful uncontended.

mod attribute;
mod compare;
mod flamegraph;
mod load;
mod result;
mod run;
mod rusage;
mod scenario;
mod spool;
mod telemetry_leg;

use anyhow::Context;
use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Parser)]
#[command(name = "logit-perf", about = "Out-of-CI load-test harness for the real logit binary.")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Build the binary under test, then spawn it once per scenario per repeat, writing a results
    /// file under `perf/results/`.
    Run {
        /// Restrict to these scenario names (repeatable); default is every scenario under
        /// `perf/scenarios/`.
        #[arg(long = "scenario")]
        scenario: Vec<String>,
        /// Repeats per scenario -- `median`/`min` in the results file are computed across these.
        #[arg(long, default_value_t = 3)]
        repeat: u32,
        /// A free-text label carried in the results file and appended to its filename.
        #[arg(long)]
        label: Option<String>,
        /// How long to wait after a scenario's completion line before sending SIGTERM, for a
        /// scenario whose graph doesn't self-exit (`scenario::Scenario::needs_sigterm`).
        #[arg(long, default_value = "1s", value_parser = parse_duration)]
        settle: Duration,
        /// How long to wait for a scenario's `generation complete` line before giving up on it as
        /// hung. Generous by default: scenarios target 5-10s
        /// (docs/plans/load-test-harness.md), so two minutes is a wide margin for a slow/loaded
        /// dev box, not a tight bound tuned to the fast case.
        #[arg(long, default_value = "120s", value_parser = parse_duration)]
        timeout: Duration,
        /// How long to wait for the process to actually exit -- after `--settle`/SIGTERM for a
        /// scenario that needs it, or after the completion line for one that self-exits -- before
        /// force-killing it. A hung drain must not hang `logit-perf run` forever.
        #[arg(long = "shutdown-timeout", default_value = "30s", value_parser = parse_duration)]
        shutdown_timeout: Duration,
        /// Skip the `cargo build` step -- use an already-built binary as is.
        #[arg(long)]
        no_build: bool,
        /// The cargo profile to build (and locate the binary under `target/<profile>/`, `dev`
        /// mapping to `target/debug` as cargo itself does).
        #[arg(long, default_value = "release")]
        profile: String,
        /// Measure this binary instead of building one; implies `--no-build`. A relative path
        /// resolves against the repo root, not the current directory. The results file records
        /// its sha256 and, when a `<path>.json` sidecar sits beside it (`script/vm build`'s own
        /// output, `docs/adr/disposable-azure-perf-vm.md`), the source ref/commit it was built
        /// from -- this is what a multi-source VM session drives instead of the `docker
        /// cp`-into-the-target-volume choreography an earlier session had to invent by hand.
        #[arg(long = "logit-bin")]
        logit_bin: Option<PathBuf>,
        /// The run-time telemetry leg's drain cadence, for real-socket scenarios. Shorter captures
        /// more of the run before the final drain, at the cost of more work inside the process
        /// being measured -- the same trade `attribute --interval` makes.
        #[arg(long, default_value = "1s", value_parser = parse_duration)]
        interval: Duration,
        /// Pin the load sender's threads to these CPUs (`3`, `2,4`, `2-5`). Real-socket scenarios
        /// only. Not optional in practice on a box with heterogeneous cores -- see
        /// docs/design/performance.md's "Driven scenarios" note.
        #[arg(long = "pin-sender", value_parser = parse_cpu_list)]
        pin_sender: Option<load::CpuSet>,
        /// Pin the spawned `logit` process to these CPUs, applied between `fork` and `exec` so
        /// every thread it creates inherits the mask. Pick CPUs disjoint from `--pin-sender`.
        #[arg(long = "pin-child", value_parser = parse_cpu_list)]
        pin_child: Option<load::CpuSet>,
        /// Hold every real-socket scenario to the strict expectation -- zero drops, and an
        /// exactly-equal delivered event count -- instead of only checking that the datagram
        /// accounting closes. A spec with no `rate` at all is rejected rather than asked to be
        /// lossless.
        ///
        /// Implies `--rate-scale 0.25`, since a shipped spec is paced deliberately *above* what
        /// the receiver sustains. An explicit `--rate-scale` overrides **that derate only**, never
        /// the exactness assertion -- so `--verify --rate-scale 1.0` asks "is this spec's own rate
        /// loss-free?" and is expected to fail whenever anything drops. That is the point of it,
        /// not a misuse.
        #[arg(long)]
        verify: bool,
        /// Multiply every real-socket spec's `rate` by this factor. The shipped rates sit just
        /// above the drop knee, which is what a baseline wants and what reading a stable CPU
        /// µs/event does not -- `--rate-scale 0.5` moves the operating point without editing any
        /// spec. Recorded in the results file, and `compare` warns when two runs used different
        /// ones, because they are different points on the load curve rather than a before and
        /// after.
        ///
        /// Given alongside `--verify` it replaces that flag's own 0.25 derate but leaves its
        /// exact-delivery assertion in place, which is how one asks whether a particular rate is
        /// loss-free: `--verify --rate-scale 1.0` holds the spec's shipped rate to zero drops, and
        /// fails if it drops anything.
        #[arg(long = "rate-scale")]
        rate_scale: Option<f64>,
    },
    /// Diff two results files' medians and exit non-zero on a regression past `--threshold`.
    Compare {
        before: PathBuf,
        after: PathBuf,
        /// Percent regression threshold on events/s (a drop) and CPU us/event (a rise).
        #[arg(long, default_value_t = 5.0)]
        threshold: f64,
        /// Percent growth threshold on peak RSS; omitted means RSS is reported but never gates
        /// the exit code.
        #[arg(long = "rss-threshold")]
        rss_threshold: Option<f64>,
    },
    /// List discovered scenarios: how each one is loaded (an in-process `generate_in` or a real
    /// socket), the size of that load, and whether it needs SIGTERM to stop.
    List,
    /// Run one scenario with a temporary `internal` telemetry leg attached, then decode the dump
    /// into a per-node breakdown of where its time went.
    Attribute {
        #[arg(long)]
        scenario: String,
        /// The appended `internal` component's drain cadence -- shorter captures more of the run
        /// but does more work inside the process being measured.
        #[arg(long, default_value = "1s", value_parser = parse_duration)]
        interval: Duration,
        /// How long to wait after the completion line before SIGTERM. An `internal` leg never
        /// self-exits, so this path is always taken.
        #[arg(long, default_value = "1s", value_parser = parse_duration)]
        settle: Duration,
        /// How long to wait for the `generation complete` line before giving up on the run as
        /// hung -- see `run --timeout`.
        #[arg(long, default_value = "120s", value_parser = parse_duration)]
        timeout: Duration,
        #[arg(long = "shutdown-timeout", default_value = "30s", value_parser = parse_duration)]
        shutdown_timeout: Duration,
        #[arg(long)]
        no_build: bool,
        #[arg(long, default_value = "release")]
        profile: String,
        /// Measure this binary instead of building one -- see `run --logit-bin`'s doc, same
        /// semantics.
        #[arg(long = "logit-bin")]
        logit_bin: Option<PathBuf>,
        /// Pin the load sender's threads to these CPUs -- real-socket scenarios only, ignored by a
        /// generator-driven one, which has no sender of its own.
        #[arg(long = "pin-sender", value_parser = parse_cpu_list)]
        pin_sender: Option<load::CpuSet>,
        /// Pin the spawned `logit` process to these CPUs.
        #[arg(long = "pin-child", value_parser = parse_cpu_list)]
        pin_child: Option<load::CpuSet>,
    },
    /// Profile one scenario with `perf record` and render the capture as a flamegraph SVG. Needs
    /// the profiling image (`script/perf flamegraph ...`), which is where `perf`/`inferno` live.
    Flamegraph {
        #[arg(long)]
        scenario: String,
        /// Defaults to `perf/results/<scenario>.svg`.
        #[arg(long)]
        out: Option<PathBuf>,
        /// `perf record -F` sampling frequency, in Hz.
        #[arg(long, default_value_t = 999)]
        freq: u32,
        #[arg(long, default_value = "1s", value_parser = parse_duration)]
        settle: Duration,
        #[arg(long, default_value = "120s", value_parser = parse_duration)]
        timeout: Duration,
        #[arg(long = "shutdown-timeout", default_value = "30s", value_parser = parse_duration)]
        shutdown_timeout: Duration,
        /// Skip the `cargo build --profile profiling` step -- use an already-built binary as is.
        #[arg(long)]
        no_build: bool,
        /// Pin the load sender's threads to these CPUs -- real-socket scenarios only.
        #[arg(long = "pin-sender", value_parser = parse_cpu_list)]
        pin_sender: Option<load::CpuSet>,
        /// Pin the profiled process to these CPUs.
        #[arg(long = "pin-child", value_parser = parse_cpu_list)]
        pin_child: Option<load::CpuSet>,
    },
}

fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Command::Run {
            scenario,
            repeat,
            label,
            settle,
            timeout,
            shutdown_timeout,
            no_build,
            profile,
            logit_bin,
            interval,
            pin_sender,
            pin_child,
            verify,
            rate_scale,
        } => run::run(
            &repo_root(),
            run::RunArgs {
                scenarios: scenario,
                repeat,
                label,
                settle,
                timeout,
                shutdown_timeout,
                no_build,
                profile,
                logit_bin,
                interval,
                pin_sender,
                pin_child,
                verify,
                rate_scale,
            },
        ),
        Command::Compare { before, after, threshold, rss_threshold } => {
            run_compare(&before, &after, threshold, rss_threshold)
        }
        Command::List => run_list(&repo_root()),
        Command::Attribute {
            scenario,
            interval,
            settle,
            timeout,
            shutdown_timeout,
            no_build,
            profile,
            logit_bin,
            pin_sender,
            pin_child,
        } => attribute::attribute(
            &repo_root(),
            attribute::AttributeArgs {
                scenario,
                interval,
                settle,
                timeout,
                shutdown_timeout,
                no_build,
                profile,
                logit_bin,
                pin_sender,
                pin_child,
            },
        ),
        Command::Flamegraph {
            scenario,
            out,
            freq,
            settle,
            timeout,
            shutdown_timeout,
            no_build,
            pin_sender,
            pin_child,
        } => flamegraph::flamegraph(
            &repo_root(),
            flamegraph::FlamegraphArgs {
                scenario,
                out,
                freq,
                settle,
                timeout,
                shutdown_timeout,
                no_build,
                pin_sender,
                pin_child,
            },
        ),
    };

    if let Err(err) = result {
        eprintln!("error: {err:#}");
        std::process::exit(1);
    }
}

fn run_list(root: &Path) -> anyhow::Result<()> {
    let scenarios = scenario::discover(&root.join("perf/scenarios"))?;
    if scenarios.is_empty() {
        println!("no scenarios found under perf/scenarios/");
        return Ok(());
    }
    println!("{:<22} {:<12} {:>22} {:>14}", "scenario", "load", "workload", "shutdown");
    for scenario in &scenarios {
        println!(
            "{:<22} {:<12} {:>22} {:>14}",
            scenario.name,
            if scenario.workload.is_driven() { "real socket" } else { "generate_in" },
            scenario.workload.describe(),
            if scenario.needs_sigterm { "SIGTERM" } else { "self-exits" },
        );
    }
    Ok(())
}

/// `--pin-sender`/`--pin-child`'s parser. `clap`'s `value_parser` wants a `String` error, while
/// [`load::CpuSet::parse`] reports an `anyhow::Error` like everything else in this crate --
/// rendered here with `{:#}` so the whole chain (which part of the list, and why) reaches the user
/// rather than only its outermost sentence.
fn parse_cpu_list(list: &str) -> Result<load::CpuSet, String> {
    load::CpuSet::parse(list).map_err(|err| format!("{err:#}"))
}

fn read_report(path: &Path) -> anyhow::Result<result::RunReport> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

/// Renders an optional field the way every "unknown" value in this CLI's output reads -- never a
/// bare Rust `None`/`null`.
fn display_optional<T: std::fmt::Display>(value: &Option<T>) -> String {
    value.as_ref().map(T::to_string).unwrap_or_else(|| "unknown".to_string())
}

/// `, binary sha256 <short>[ <source>[ @ <short sha>]]` -- appended to `compare`'s header lines,
/// empty when the results file predates `RunReport::binary`. `git.sha` above already names the
/// checkout; this names the binary that was actually spawned, which a `--logit-bin` run can leave
/// pointing at a different source entirely (`result::BinaryInfo`'s own doc).
fn format_binary_provenance(binary: &Option<result::BinaryInfo>) -> String {
    let Some(binary) = binary else { return String::new() };
    let short_sha256: String = binary.sha256.chars().take(12).collect();
    match (&binary.source_ref, &binary.source_sha) {
        (Some(source), Some(sha)) => {
            let short_sha: String = sha.chars().take(12).collect();
            format!(", binary sha256 {short_sha256} ({source} @ {short_sha})")
        }
        (Some(source), None) => format!(", binary sha256 {short_sha256} ({source})"),
        (None, _) => format!(", binary sha256 {short_sha256}"),
    }
}

fn run_compare(
    before: &Path,
    after: &Path,
    threshold: f64,
    rss_threshold: Option<f64>,
) -> anyhow::Result<()> {
    let a = read_report(before)?;
    let b = read_report(after)?;

    println!(
        "before: {} (sha {}, {}, {}{})",
        before.display(),
        display_optional(&a.git.sha),
        a.profile,
        a.rustc,
        format_binary_provenance(&a.binary)
    );
    println!(
        "after:  {} (sha {}, {}, {}{})",
        after.display(),
        display_optional(&b.git.sha),
        b.profile,
        b.rustc,
        format_binary_provenance(&b.binary)
    );

    let report = compare::compare(&a, &b);
    for warning in &report.warnings {
        eprintln!("warning: {warning}");
    }

    println!(
        "\n{:<22} {:>12} {:>12} {:>12} {:>14}",
        "scenario", "events/s", "us/event", "peak RSS", "drop rate"
    );
    for scenario in &report.scenarios {
        match &scenario.deltas {
            Some(deltas) => {
                let regressed = deltas.is_regression(threshold, rss_threshold);
                println!(
                    "{:<22} {:>+11.1}% {:>+11.1}% {:>+11.1}% {:>14}{}",
                    scenario.name,
                    deltas.events_per_s_pct,
                    deltas.cpu_us_per_event_pct,
                    deltas.max_rss_bytes_pct,
                    // Percentage *points*, and never gated -- `Deltas::drop_rate_points` has why.
                    deltas
                        .drop_rate_points
                        .map(|points| format!("{points:+.2} pts"))
                        .unwrap_or_else(|| "-".to_string()),
                    if regressed { "  REGRESSED" } else { "" },
                );
                // Warned, never gated (`Deltas::startup_regressed`'s own doc has why): startup is
                // spawn -> ready process bring-up, not the graph's own per-event cost, so it's
                // worth a human's attention without failing a `compare --threshold` gate meant for
                // throughput/CPU/RSS.
                if deltas.startup_regressed(threshold) {
                    eprintln!(
                        "warning: scenario `{}`: startup_s rose {:+.1}% (spawn -> ready; not \
                         gated)",
                        scenario.name,
                        deltas.startup_s_pct.expect("startup_regressed implies Some"),
                    );
                }
            }
            None => {
                let only_in = match scenario.presence {
                    compare::Presence::BeforeOnly => before,
                    compare::Presence::AfterOnly => after,
                    compare::Presence::Both => {
                        unreachable!("Presence::Both always carries deltas")
                    }
                };
                println!("{:<22} only in {}", scenario.name, only_in.display());
            }
        }
    }

    if report.has_regression(threshold, rss_threshold) {
        anyhow::bail!(
            "regression: events/s dropped or CPU us/event rose by more than {threshold}%{}",
            rss_threshold
                .map(|t| format!(", or peak RSS grew by more than {t}%"))
                .unwrap_or_default()
        );
    }
    Ok(())
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    // `-` is allowed in the numeric portion (not just digits/`.`) purely so a negative value
    // parses through to `Duration::try_from_secs_f64` below and fails there with a clear message,
    // instead of being rejected here as "no unit" and hiding what was actually wrong with it.
    let (number, unit) = s
        .find(|c: char| !c.is_ascii_digit() && c != '.' && c != '-')
        .map(|idx| s.split_at(idx))
        .ok_or_else(|| format!("`{s}` has no unit (expected e.g. `1s`, `500ms`)"))?;
    let value: f64 = number.parse().map_err(|_| format!("`{number}` is not a number"))?;
    let seconds = match unit {
        "s" => value,
        "ms" => value / 1_000.0,
        "m" => value * 60.0,
        other => {
            return Err(format!("unknown duration unit `{other}` (expected `s`, `ms`, or `m`)"))
        }
    };
    Duration::try_from_secs_f64(seconds)
        .map_err(|err| format!("`{s}` is not a valid duration: {err}"))
}

/// The repository root, resolved from this crate's own manifest directory at compile time
/// (`crates/logit-perf` -> repo root) rather than the process's current directory -- `script/perf`
/// already `cd`s to the repo root before running (`script/common.sh`), but resolving it this way
/// means `logit-perf` behaves the same run from anywhere. Only `run`/`list` need it (both walk
/// `perf/scenarios/` relative to it); `compare` takes two explicit file paths and never touches
/// it, so it's resolved lazily at each call site rather than once up front in `main`.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("crates/logit-perf/../.. should resolve to the repo root")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_duration_accepts_seconds_milliseconds_and_minutes() {
        assert_eq!(parse_duration("1s").unwrap(), Duration::from_secs(1));
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
        assert_eq!(parse_duration("1.5s").unwrap(), Duration::from_millis(1_500));
    }

    #[test]
    fn parse_duration_rejects_no_unit_and_unknown_units() {
        assert!(parse_duration("5").is_err());
        assert!(parse_duration("5h").is_err());
    }

    #[test]
    fn parse_duration_rejects_a_negative_value_without_panicking() {
        let err = parse_duration("-1s").expect_err("a negative duration is invalid");
        assert!(err.contains("not a valid duration"), "{err}");
    }

    #[test]
    fn display_optional_reads_unknown_for_none() {
        assert_eq!(display_optional::<String>(&None), "unknown");
        assert_eq!(display_optional(&Some(42)), "42");
    }
}
