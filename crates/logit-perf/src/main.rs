//! `logit-perf`: the out-of-CI load-test harness's CLI (docs/adr/load-test-harness.md).
//!
//! Spawns the real `logit` binary against `perf/scenarios/*.yaml` and measures throughput, CPU
//! per event, and peak RSS. `script/cibuild` never runs it: it loads the machine heavily, and its
//! numbers mean something only uncontended.

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
    /// Build the binary under test, run each scenario `--repeat` times, and write a results file
    /// under `perf/results/`.
    Run {
        /// Run only this scenario (repeatable); the default is every scenario in `perf/scenarios/`.
        #[arg(long = "scenario")]
        scenario: Vec<String>,
        /// Repeats per scenario; the results file's `median` and `min` are taken across them.
        #[arg(long, default_value_t = 3)]
        repeat: u32,
        /// A free-text label carried in the results file and appended to its filename.
        #[arg(long)]
        label: Option<String>,
        /// How long to wait after the load ends before sending SIGTERM to a scenario that doesn't
        /// exit on its own (at least 3s for a real-socket scenario).
        #[arg(long, default_value = "1s", value_parser = parse_duration)]
        settle: Duration,
        /// How long to wait for a scenario's `generation complete` (or, for a real-socket
        /// scenario, `ready`) line before failing it as hung.
        #[arg(long, default_value = "120s", value_parser = parse_duration)]
        timeout: Duration,
        /// How long to wait for the process to exit after the load ends (and any SIGTERM) before
        /// killing it.
        #[arg(long = "shutdown-timeout", default_value = "30s", value_parser = parse_duration)]
        shutdown_timeout: Duration,
        /// Skip the `cargo build` step and use the already-built binary.
        #[arg(long)]
        no_build: bool,
        /// The cargo profile to build and measure, found under `target/<profile>/` (`dev` under
        /// `target/debug`).
        #[arg(long, default_value = "release")]
        profile: String,
        /// Measure this binary instead of building one (relative to the repo root), recording
        /// its sha256 and any `<path>.json` sidecar's source ref and commit.
        #[arg(long = "logit-bin")]
        logit_bin: Option<PathBuf>,
        /// Drain interval of the telemetry leg attached to real-socket scenarios; shorter adds
        /// work inside the measured process.
        #[arg(long, default_value = "1s", value_parser = parse_duration)]
        interval: Duration,
        /// Pin a real-socket scenario's load sender to these CPUs (`3`, `2,4`, `2-5`), disjoint
        /// from `--pin-child`.
        #[arg(long = "pin-sender", value_parser = parse_cpu_list)]
        pin_sender: Option<load::CpuSet>,
        /// Pin the spawned `logit` process and all its threads to these CPUs, disjoint from
        /// `--pin-sender`.
        #[arg(long = "pin-child", value_parser = parse_cpu_list)]
        pin_child: Option<load::CpuSet>,
        /// Fail a real-socket scenario unless it drops nothing and delivers the exact event count;
        /// implies `--rate-scale 0.25` unless one is given.
        #[arg(long)]
        verify: bool,
        /// Multiply every real-socket spec's `rate` by this factor, recorded in the results file
        /// (with `--verify`, replaces its 0.25 derate but keeps the exactness check).
        #[arg(long = "rate-scale")]
        rate_scale: Option<f64>,
    },
    /// Diff two results files' medians and exit non-zero on a regression past `--threshold`.
    Compare {
        before: PathBuf,
        after: PathBuf,
        /// Percent drop in events/s, or rise in CPU us/event, that counts as a regression.
        #[arg(long, default_value_t = 5.0)]
        threshold: f64,
        /// Percent peak-RSS growth that counts as a regression; without it, RSS never fails.
        #[arg(long = "rss-threshold")]
        rss_threshold: Option<f64>,
    },
    /// List scenarios with their load source (`generate_in` or a real socket), size, and
    /// shutdown.
    List,
    /// Run one scenario with a temporary telemetry leg and print where its time went, per node.
    Attribute {
        #[arg(long)]
        scenario: String,
        /// Drain interval of the telemetry leg; shorter adds work inside the measured process.
        #[arg(long, default_value = "1s", value_parser = parse_duration)]
        interval: Duration,
        /// How long to wait after the load ends before sending SIGTERM.
        #[arg(long, default_value = "1s", value_parser = parse_duration)]
        settle: Duration,
        /// How long to wait for the completion (or `ready`) line before failing the run as hung.
        #[arg(long, default_value = "120s", value_parser = parse_duration)]
        timeout: Duration,
        #[arg(long = "shutdown-timeout", default_value = "30s", value_parser = parse_duration)]
        shutdown_timeout: Duration,
        #[arg(long)]
        no_build: bool,
        #[arg(long, default_value = "release")]
        profile: String,
        /// Measure this binary instead of building one (relative to the repo root).
        #[arg(long = "logit-bin")]
        logit_bin: Option<PathBuf>,
        /// Pin a real-socket scenario's load sender to these CPUs, disjoint from `--pin-child`.
        #[arg(long = "pin-sender", value_parser = parse_cpu_list)]
        pin_sender: Option<load::CpuSet>,
        /// Pin the spawned `logit` process and all its threads to these CPUs.
        #[arg(long = "pin-child", value_parser = parse_cpu_list)]
        pin_child: Option<load::CpuSet>,
    },
    /// Profile one scenario with `perf record` into a flamegraph SVG (run via
    /// `script/perf flamegraph`).
    Flamegraph {
        #[arg(long)]
        scenario: String,
        /// Defaults to `perf/results/<scenario>.svg`.
        #[arg(long)]
        out: Option<PathBuf>,
        /// Also write the collapsed stacks (`a;b;c <count>` per line, for `perf/folded_share.py`)
        /// to this path.
        #[arg(long)]
        folded: Option<PathBuf>,
        /// `perf record -F` sampling frequency, in Hz.
        #[arg(long, default_value_t = 999)]
        freq: u32,
        #[arg(long, default_value = "1s", value_parser = parse_duration)]
        settle: Duration,
        #[arg(long, default_value = "120s", value_parser = parse_duration)]
        timeout: Duration,
        #[arg(long = "shutdown-timeout", default_value = "30s", value_parser = parse_duration)]
        shutdown_timeout: Duration,
        /// Skip the `cargo build --profile profiling` step and use the already-built binary.
        #[arg(long)]
        no_build: bool,
        /// Pin a real-socket scenario's load sender to these CPUs, disjoint from `--pin-child`.
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
            folded,
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
                folded,
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

/// `--pin-sender`/`--pin-child`'s parser: renders [`load::CpuSet::parse`]'s error with `{:#}` so
/// the whole chain, not only its outermost sentence, reaches the user.
fn parse_cpu_list(list: &str) -> Result<load::CpuSet, String> {
    load::CpuSet::parse(list).map_err(|err| format!("{err:#}"))
}

fn read_report(path: &Path) -> anyhow::Result<result::RunReport> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

/// Renders an optional field as `unknown` when absent, never a bare `None`/`null`.
fn display_optional<T: std::fmt::Display>(value: &Option<T>) -> String {
    value.as_ref().map(T::to_string).unwrap_or_else(|| "unknown".to_string())
}

/// `, binary sha256 <short>[ (<source>[ @ <short sha>])]` for `compare`'s header lines; empty
/// when the results file has no `binary`. Under `--logit-bin` the binary can come from a
/// different source than `git.sha` names (`result::BinaryInfo`).
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
                    // Percentage points, never gated (`Deltas::drop_rate_points`).
                    deltas
                        .drop_rate_points
                        .map(|points| format!("{points:+.2} pts"))
                        .unwrap_or_else(|| "-".to_string()),
                    if regressed { "  REGRESSED" } else { "" },
                );
                // Warned, never gated (`Deltas::startup_regressed`).
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
    // `-` is accepted here so a negative value reaches `Duration::try_from_secs_f64` and fails
    // with a clear message, not a misleading "no unit".
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

/// The repository root, from this crate's manifest directory at compile time rather than the
/// cwd, so `logit-perf` behaves the same run from anywhere. `compare` doesn't need it, so each
/// subcommand resolves it at its call site.
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
