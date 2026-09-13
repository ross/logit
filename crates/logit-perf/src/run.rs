//! `logit-perf run`: builds the release binary once, then spawns it once per scenario per repeat,
//! deriving throughput/CPU/RSS from its stderr and `wait4` (docs/adr/load-test-harness.md,
//! docs/plans/load-test-harness.md's "Harness" section).

use crate::result::{GitInfo, RunReport, Sample, ScenarioReport};
use crate::rusage;
use crate::scenario::{self, Scenario};
use anyhow::{bail, Context};
use std::collections::BTreeMap;
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub struct RunArgs {
    /// Empty means every discovered scenario.
    pub scenarios: Vec<String>,
    pub repeat: u32,
    pub label: Option<String>,
    pub settle: Duration,
    pub no_build: bool,
    pub profile: String,
}

/// How long to wait for a scenario's `generation complete` line before giving up on it as hung.
/// Generous on purpose: scenarios target 5-10s (docs/plans/load-test-harness.md), so two minutes
/// is a wide margin for a slow/loaded dev box, not a tight bound tuned to the fast case.
const COMPLETION_TIMEOUT: Duration = Duration::from_secs(120);

/// The exact substring `--log-format json` renders `generate_in`'s completion log line as
/// (docs/plans/load-test-harness.md: `tracing::info!(... "generation complete")`, and `--log-format
/// json`'s `message` field is always top-level, per `main.rs`'s own doc on the format).
const COMPLETION_MARKER: &str = "\"message\":\"generation complete\"";

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

    if !args.no_build {
        build(root, &args.profile)?;
    }
    let logit_bin = logit_binary_path(root, &args.profile);
    if !logit_bin.exists() {
        bail!(
            "{} does not exist -- build it first (drop --no-build) or check --profile",
            logit_bin.display()
        );
    }

    let mut reports: BTreeMap<String, ScenarioReport> = BTreeMap::new();
    let mut any_failed = false;

    for scenario in &scenarios {
        println!(
            "-- {} (count={}, {})",
            scenario.name,
            scenario.count,
            if scenario.needs_sigterm { "needs SIGTERM" } else { "self-exits" }
        );
        let mut samples = Vec::with_capacity(args.repeat as usize);
        let mut scenario_failed = false;
        for repeat in 1..=args.repeat {
            match run_one(&logit_bin, scenario, args.settle) {
                Ok(sample) => {
                    println!(
                        "   repeat {repeat}/{}: {:.0} events/s, {:.3} us/event, {:.1} MiB peak RSS",
                        args.repeat,
                        sample.events_per_s,
                        sample.cpu_us_per_event,
                        sample.max_rss_bytes as f64 / (1024.0 * 1024.0),
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
        reports.insert(
            scenario.name.clone(),
            ScenarioReport { count: scenario.count, repeats: samples, median, min },
        );
    }

    if reports.is_empty() {
        bail!("every scenario failed; nothing to write");
    }

    let report = RunReport {
        git: git_info(root),
        timestamp: format_rfc3339_utc_seconds(now_unix_seconds()),
        hostname: hostname(),
        cpu_model: cpu_model(),
        nproc: std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1),
        rustc: rustc_version(),
        profile: args.profile.clone(),
        label: args.label.clone(),
        scenarios: reports,
    };

    let results_dir = root.join("perf/results");
    std::fs::create_dir_all(&results_dir)
        .with_context(|| format!("creating {}", results_dir.display()))?;
    let short_sha: String = report.git.sha.chars().take(12).collect();
    let label_suffix =
        report.label.as_deref().map(|label| format!("-{}", sanitize_label(label))).unwrap_or_default();
    let filename = format!("{}-{short_sha}{label_suffix}.json", compact_utc_now(now_unix_seconds()));
    let path = results_dir.join(filename);
    std::fs::write(&path, serde_json::to_string_pretty(&report)?)
        .with_context(|| format!("writing {}", path.display()))?;

    print_table(&report);
    println!("\nwrote {}", path.display());

    if any_failed {
        bail!("one or more scenarios failed -- see above");
    }
    Ok(())
}

fn run_one(logit_bin: &Path, scenario: &Scenario, settle: Duration) -> anyhow::Result<Sample> {
    let mut child = Command::new(logit_bin)
        .args(["--log-format", "json", "--log-level", "info", "run"])
        .arg(&scenario.path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawning {}", logit_bin.display()))?;
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
    let (completion_tx, completion_rx) = mpsc::channel::<Instant>();
    let stderr_reader = std::thread::spawn(move || -> String {
        let mut all_stderr = String::new();
        let mut sent = false;
        for line in std::io::BufReader::new(stderr).lines() {
            let Ok(line) = line else { break };
            if !sent && line.contains(COMPLETION_MARKER) {
                sent = true;
                let _ = completion_tx.send(Instant::now());
            }
            all_stderr.push_str(&line);
            all_stderr.push('\n');
        }
        all_stderr
    });

    let completion_at = match completion_rx.recv_timeout(COMPLETION_TIMEOUT) {
        Ok(instant) => instant,
        Err(_) => {
            let _ = child.kill();
            let _ = child.wait();
            let stderr_text = stderr_reader.join().unwrap_or_default();
            let _ = stdout_drain.join();
            bail!(
                "no `generation complete` line within {}s; stderr:\n{stderr_text}",
                COMPLETION_TIMEOUT.as_secs()
            );
        }
    };
    let wall = completion_at - spawned_at;

    if scenario.needs_sigterm {
        std::thread::sleep(settle);
        // SAFETY: `pid` is this scenario's own child, spawned above and not yet reaped (neither
        // `child.wait()` nor `rusage::wait4` below has run yet), so it is a valid, still-live
        // signal target.
        let killed = unsafe { libc::kill(pid, libc::SIGTERM) };
        if killed != 0 {
            let _ = child.kill();
        }
    }

    let usage = rusage::wait4(pid, wall).with_context(|| format!("wait4({pid})"))?;
    let stderr_text = stderr_reader.join().unwrap_or_default();
    let _ = stdout_drain.join();

    match usage.exit_code() {
        Some(0) => {}
        Some(code) => bail!("exited with status {code}; stderr:\n{stderr_text}"),
        None => bail!("terminated by signal (raw status {}); stderr:\n{stderr_text}", usage.status),
    }

    Ok(Sample::from_usage(scenario.count, usage.wall, usage.user, usage.sys, usage.max_rss_bytes))
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
fn logit_binary_path(root: &Path, profile: &str) -> PathBuf {
    let target_dir = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join("target"));
    let profile_dir = if profile == "dev" { "debug" } else { profile };
    target_dir.join(profile_dir).join("logit")
}

fn git_info(root: &Path) -> GitInfo {
    let sha = Command::new("git")
        .current_dir(root)
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let dirty = Command::new("git")
        .current_dir(root)
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| !out.stdout.is_empty())
        .unwrap_or(false);
    GitInfo { sha, dirty }
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

fn cpu_model() -> String {
    std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|contents| {
            contents.lines().find_map(|line| {
                line.strip_prefix("model name").and_then(|rest| rest.trim_start().strip_prefix(':'))
            }).map(|value| value.trim().to_string())
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
        "\n{:<20} {:>12} {:>14} {:>12}",
        "scenario", "events/s", "us/event", "peak RSS"
    );
    for (name, scenario) in &report.scenarios {
        println!(
            "{:<20} {:>12.0} {:>14.3} {:>10.1} MiB",
            name,
            scenario.median.events_per_s,
            scenario.median.cpu_us_per_event,
            scenario.median.max_rss_bytes as f64 / (1024.0 * 1024.0),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logit_binary_path_maps_the_dev_profile_to_the_debug_directory() {
        std::env::remove_var("CARGO_TARGET_DIR");
        let root = Path::new("/repo");
        assert_eq!(logit_binary_path(root, "dev"), Path::new("/repo/target/debug/logit"));
        assert_eq!(logit_binary_path(root, "release"), Path::new("/repo/target/release/logit"));
        assert_eq!(logit_binary_path(root, "profiling"), Path::new("/repo/target/profiling/logit"));
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
}
