//! `logit-perf`: the out-of-CI load-test harness's CLI (docs/adr/load-test-harness.md,
//! docs/plans/load-test-harness.md). Spawns the real, release-profile `logit` binary against
//! `perf/scenarios/*.yaml` and measures throughput, CPU per event, and peak RSS -- see `run.rs`
//! for the measurement itself, `compare.rs` for before/after diffing, and `scenario.rs` for what
//! it reads out of a scenario file.
//!
//! Deliberately not run by `script/cibuild` (`script/perf`'s own header comment says why, the
//! same reason `script/bench` gives): this loads the machine heavily and its numbers are only
//! meaningful uncontended.

mod compare;
mod result;
mod run;
mod rusage;
mod scenario;

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
        /// Skip the `cargo build` step -- use an already-built binary as is.
        #[arg(long)]
        no_build: bool,
        /// The cargo profile to build (and locate the binary under `target/<profile>/`, `dev`
        /// mapping to `target/debug` as cargo itself does).
        #[arg(long, default_value = "release")]
        profile: String,
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
    /// List discovered scenarios with their `count` and whether they need SIGTERM to stop.
    List,
    /// Per-node time attribution -- lands in W6 (docs/plans/load-test-harness.md).
    Attribute {
        #[arg(long)]
        scenario: Option<String>,
        #[arg(long)]
        interval: Option<String>,
    },
    /// `perf`/`inferno` flamegraph generation -- lands in W6 (docs/plans/load-test-harness.md).
    Flamegraph {
        #[arg(long)]
        scenario: Option<String>,
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

fn main() {
    let cli = Cli::parse();
    let root = repo_root();

    let result = match cli.command {
        Command::Run { scenario, repeat, label, settle, no_build, profile } => run::run(
            &root,
            run::RunArgs { scenarios: scenario, repeat, label, settle, no_build, profile },
        ),
        Command::Compare { before, after, threshold, rss_threshold } => {
            run_compare(&before, &after, threshold, rss_threshold)
        }
        Command::List => run_list(&root),
        Command::Attribute { .. } => {
            eprintln!("logit-perf attribute: lands in W6 (docs/plans/load-test-harness.md)");
            std::process::exit(2);
        }
        Command::Flamegraph { .. } => {
            eprintln!("logit-perf flamegraph: lands in W6 (docs/plans/load-test-harness.md)");
            std::process::exit(2);
        }
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
    println!("{:<22} {:>12} {:>14}", "scenario", "count", "shutdown");
    for scenario in &scenarios {
        println!(
            "{:<22} {:>12} {:>14}",
            scenario.name,
            scenario.count,
            if scenario.needs_sigterm { "SIGTERM" } else { "self-exits" },
        );
    }
    Ok(())
}

fn run_compare(
    before: &Path,
    after: &Path,
    threshold: f64,
    rss_threshold: Option<f64>,
) -> anyhow::Result<()> {
    let a: result::RunReport = serde_json::from_str(
        &std::fs::read_to_string(before)
            .with_context(|| format!("reading {}", before.display()))?,
    )
    .with_context(|| format!("parsing {}", before.display()))?;
    let b: result::RunReport = serde_json::from_str(
        &std::fs::read_to_string(after).with_context(|| format!("reading {}", after.display()))?,
    )
    .with_context(|| format!("parsing {}", after.display()))?;

    let report = compare::compare(&a, &b);
    if let Some(warning) = &report.environment_warning {
        eprintln!("warning: {warning}");
    }

    println!("{:<22} {:>12} {:>12} {:>12}", "scenario", "events/s", "us/event", "peak RSS");
    for scenario in &report.scenarios {
        match scenario.deltas {
            Some(deltas) => println!(
                "{:<22} {:>+11.1}% {:>+11.1}% {:>+11.1}%",
                scenario.name,
                deltas.events_per_s_pct,
                deltas.cpu_us_per_event_pct,
                deltas.max_rss_bytes_pct,
            ),
            None => println!("{:<22} {:>12}", scenario.name, "only in one file"),
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
    let (number, unit) = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
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
    Ok(Duration::from_secs_f64(seconds))
}

/// The repository root, resolved from this crate's own manifest directory at compile time
/// (`crates/logit-perf` -> repo root) rather than the process's current directory -- `script/perf`
/// already `cd`s to the repo root before running (`script/common.sh`), but resolving it this way
/// means `logit-perf` behaves the same run from anywhere.
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
}
