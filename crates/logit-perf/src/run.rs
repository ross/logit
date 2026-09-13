//! `logit-perf run`: builds the release binary once, then spawns it once per scenario per repeat,
//! deriving throughput/CPU/RSS from its stderr and `wait4` (docs/adr/load-test-harness.md,
//! docs/plans/load-test-harness.md's "Harness" section).

use crate::result::{GitInfo, RunReport, Sample, ScenarioReport};
use crate::rusage;
use crate::scenario::{self, Scenario};
use anyhow::{bail, Context};
use std::collections::{BTreeMap, VecDeque};
use std::io::{self, BufRead};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub struct RunArgs {
    /// Empty means every discovered scenario.
    pub scenarios: Vec<String>,
    pub repeat: u32,
    pub label: Option<String>,
    pub settle: Duration,
    pub no_build: bool,
    pub profile: String,
    /// How long to wait for a scenario's `generation complete` line before giving up on it as
    /// hung.
    pub timeout: Duration,
    /// How long to wait for the process to actually exit after `--settle`/SIGTERM (or, for a
    /// self-exiting scenario, after the completion line) before force-killing it.
    pub shutdown_timeout: Duration,
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

    if !args.no_build {
        build(root, &args.profile)?;
    }
    let target_dir_env = std::env::var_os("CARGO_TARGET_DIR");
    let logit_bin =
        logit_binary_path(root, &args.profile, target_dir_env.as_deref().map(Path::new));
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
            match run_one(&logit_bin, scenario, args.settle, args.timeout, args.shutdown_timeout) {
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
        scenarios: reports,
    };

    let results_dir = root.join("perf/results");
    std::fs::create_dir_all(&results_dir)
        .with_context(|| format!("creating {}", results_dir.display()))?;
    let short_sha: String = report
        .git
        .sha
        .as_deref()
        .map(|sha| sha.chars().take(12).collect())
        .unwrap_or_else(|| "unknown".to_string());
    let label_suffix = report
        .label
        .as_deref()
        .map(|label| format!("-{}", sanitize_label(label)))
        .unwrap_or_default();
    let filename = format!("{}-{short_sha}{label_suffix}.json", compact_utc_now(now));
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

/// One spawned scenario's completion signal: the instant its `generation complete` line arrived,
/// and the `events` field that line carried (`None` if the line parsed as JSON but that field was
/// missing or not an unsigned integer -- a format this harness can't trust).
struct Completion {
    at: Instant,
    events: Option<u64>,
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

/// Kills and reaps a still-running child, then joins both reader threads -- every error path that
/// bails before the ordinary settle/SIGTERM/`wait4` sequence has run (the completion timeout, a
/// malformed or mismatched `events` count) calls this, so a failed repeat never leaves a live
/// process, a zombie, or a detached reader thread behind.
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

fn run_one(
    logit_bin: &Path,
    scenario: &Scenario,
    settle: Duration,
    timeout: Duration,
    shutdown_timeout: Duration,
) -> anyhow::Result<Sample> {
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
    let (completion_tx, completion_rx) = mpsc::channel::<Completion>();
    let stderr_reader = std::thread::spawn(move || -> String {
        let mut capture = StderrCapture::new();
        let mut sent = false;
        for line in io::BufReader::new(stderr).lines() {
            let Ok(line) = line else { break };
            if !sent {
                if let Some(events) = parse_completion_line(&line) {
                    sent = true;
                    let _ = completion_tx.send(Completion { at: Instant::now(), events });
                }
            }
            capture.push(line);
        }
        capture.into_string()
    });

    let completion = match completion_rx.recv_timeout(timeout) {
        Ok(completion) => completion,
        Err(_) => {
            let stderr_text = kill_and_reap(&mut child, stdout_drain, stderr_reader);
            bail!(
                "no `generation complete` line within {}s; stderr:\n{stderr_text}",
                timeout.as_secs()
            );
        }
    };
    let wall = completion.at - spawned_at;

    let events = match completion.events {
        Some(events) => events,
        None => {
            let stderr_text = kill_and_reap(&mut child, stdout_drain, stderr_reader);
            bail!(
                "`generation complete` line has no numeric `events` field; stderr:\n{stderr_text}"
            );
        }
    };
    if events != scenario.count {
        let stderr_text = kill_and_reap(&mut child, stdout_drain, stderr_reader);
        bail!(
            "generate_in reported {events} events but the scenario's `count` is {} -- events/s \
             and CPU us/event would be measured against the wrong denominator; stderr:\n{stderr_text}",
            scenario.count
        );
    }

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
///    *host* before it execs into the dev container (`compose.yaml`'s `dev.environment` forwards
///    them, the same pattern `INFLUXDB_TOKEN` already uses). This is the reliable path: a
///    git-worktree checkout's `.git` file points at an absolute host path the dev container's
///    bind mount doesn't include, so `git` run *inside* the container against a worktree checkout
///    routinely can't answer at all.
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
    println!("\n{:<22} {:>12} {:>14} {:>12}", "scenario", "events/s", "us/event", "peak RSS");
    for (name, scenario) in &report.scenarios {
        println!(
            "{:<22} {:>12.0} {:>14.3} {:>9.1} MiB",
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
}
