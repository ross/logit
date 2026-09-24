//! `logit-perf flamegraph`: one command from a scenario name to an SVG.
//!
//! 1. `cargo build --profile profiling -p logit-cli`. The root `[profile.profiling]` inherits
//!    `release`'s optimization and keeps the line tables and symbols `perf` needs to name frames
//!    (see that profile's comment in the root `Cargo.toml`).
//! 2. `perf record -F <freq> -g --call-graph dwarf -o <tmp>/perf.data -- logit run <scenario>`,
//!    through `run`'s spawn/settle/SIGTERM machinery (`crate::run::spawn_and_measure`'s
//!    `wrapper`), so the capture stops on the scenario's completion line. DWARF unwinding
//!    because the profile doesn't force frame pointers, and without them `-g` has almost nothing
//!    to unwind. For a scenario that doesn't self-exit, SIGTERM goes to `perf`, which forwards
//!    it to `logit` (which drains and exits 0), waits, writes `perf.data`, then re-raises
//!    SIGTERM on itself; that's why `spawn_and_measure` accepts a wrapper's SIGTERM death as a
//!    completed run.
//! 3. `perf script | inferno-collapse-perf | inferno-flamegraph` as a three-process pipeline,
//!    not buffered here: a `perf script` dump of a multi-million-event scenario is hundreds of
//!    megabytes. The SVG goes to a staging file, renamed onto `--out` only once every stage
//!    exits 0 and the result is non-empty, so a failed re-render keeps the previous SVG.
//!
//! **The tooling is not in the dev image.** `perf` and `inferno` live only in
//! `crates/logit-perf/Dockerfile`, run with the capabilities `perf record` needs
//! (`CAP_SYS_ADMIN`, unconfined seccomp; `docs/adr/load-test-harness.md`'s "Profiling" section).
//! Outside that image this fails up front, naming `script/perf flamegraph`.

use crate::load::{CpuSet, LoadPlan};
use crate::run::{self, Drive, SpawnConfig};
use crate::scenario::{self, Scenario, Workload};
use anyhow::{bail, Context};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

/// The profile `perf record` always profiles; a stripped `release` binary yields a flamegraph of
/// hex addresses.
const PROFILE: &str = "profiling";

const PERF: &str = "perf";
const COLLAPSE: &str = "inferno-collapse-perf";
const FLAMEGRAPH: &str = "inferno-flamegraph";
/// `--folded`'s extra stage. Coreutils, not a fourth Rust process.
const TEE: &str = "tee";

pub struct FlamegraphArgs {
    pub scenario: String,
    /// Defaults to `perf/results/<scenario>.svg` under the repo root.
    pub out: Option<PathBuf>,
    /// Where to keep the collapsed stacks, if anywhere; see [`collapse_to_svg`].
    pub folded: Option<PathBuf>,
    /// Sampling frequency in Hz. The default 999, not 1000, keeps the sampler out of lock step
    /// with anything on a whole-millisecond cadence.
    pub freq: u32,
    pub settle: Duration,
    pub timeout: Duration,
    pub shutdown_timeout: Duration,
    pub no_build: bool,
    /// Sender CPU pinning; only a real-socket scenario has a sender.
    pub pin_sender: Option<CpuSet>,
    pub pin_child: Option<CpuSet>,
}

pub fn flamegraph(root: &Path, args: FlamegraphArgs) -> anyhow::Result<()> {
    let scenarios_dir = root.join("perf/scenarios");
    let scenario = scenario::find(&scenarios_dir, &args.scenario)?;
    require_tools()?;
    // A stale spool skews the capture as it skews `run`'s numbers (`crate::spool`).
    crate::spool::clear(root, &scenario)?;

    let out = args
        .out
        .clone()
        .unwrap_or_else(|| root.join("perf/results").join(format!("{}.svg", scenario.name)));
    if let Some(parent) = out.parent() {
        fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    }

    let logit_bin = run::build_and_locate(root, PROFILE, args.no_build, None)?;

    let workdir =
        std::env::temp_dir().join(format!("logit-perf-flamegraph-{}", std::process::id()));
    fs::create_dir_all(&workdir).with_context(|| format!("creating {}", workdir.display()))?;
    let perf_data = workdir.join("perf.data");
    // A leftover capture from an earlier run at this pid would render as this run's.
    if perf_data.exists() {
        bail!(
            "{} already exists -- a previous `flamegraph` run left it behind; remove it (or its \
             whole directory) and try again",
            perf_data.display()
        );
    }

    let outcome = record_and_render(&logit_bin, &scenario, &args, &perf_data, &out);
    match &outcome {
        // Only on success: a failed render's `perf.data` is a multi-minute capture worth keeping
        // to retry the fast render against.
        Ok(()) => {
            let _ = fs::remove_dir_all(&workdir);
        }
        Err(_) => eprintln!("note: the capture is left at {}", perf_data.display()),
    }
    outcome
}

fn record_and_render(
    logit_bin: &Path,
    scenario: &Scenario,
    args: &FlamegraphArgs,
    perf_data: &Path,
    out: &Path,
) -> anyhow::Result<()> {
    println!(
        "-- {} ({}, {} Hz, {})",
        scenario.name,
        scenario.workload.describe(),
        args.freq,
        if scenario.needs_sigterm { "needs SIGTERM" } else { "self-exits" }
    );
    // A real-socket scenario gets the load `run` gives it. The sender runs in this process,
    // outside `perf record`, so the capture is the receive side only. No telemetry leg: profile
    // the shipped graph, not it plus the harness's two nodes.
    let plan = match &scenario.workload {
        Workload::Generated { .. } => None,
        Workload::Driven(_) => {
            let source = fs::read_to_string(&scenario.path)
                .with_context(|| format!("reading {}", scenario.path.display()))?;
            Some(LoadPlan::build(&scenario.load_spec_path()?, &source)?)
        }
    };
    let drive = match (&plan, &scenario.workload) {
        (Some(plan), _) => Drive::Driven { plan, pin_sender: args.pin_sender.as_ref() },
        (None, Workload::Generated { count }) => Drive::Generated { count: *count },
        (None, Workload::Driven(_)) => unreachable!("a driven workload always builds a plan"),
    };

    let wrapper = record_argv(args.freq, perf_data);
    let measured = run::spawn_and_measure(SpawnConfig {
        logit_bin,
        wrapper: &wrapper,
        config: &scenario.path,
        drive,
        pin_child: args.pin_child.as_ref(),
        needs_sigterm: scenario.needs_sigterm,
        settle: args.settle,
        timeout: args.timeout,
        shutdown_timeout: args.shutdown_timeout,
    })
    .context("perf record")?;
    // `perf record` captures from spawn, not `ready`, so this reports `startup + wall`, unlike
    // `run`'s events/s, which excludes startup.
    println!(
        "   captured {:.1}s of wall time ({:.1} MiB of samples)",
        measured.startup.unwrap_or_default().as_secs_f64() + measured.wall().as_secs_f64(),
        fs::metadata(perf_data).map(|m| m.len()).unwrap_or(0) as f64 / (1024.0 * 1024.0),
    );

    // Staged, then renamed onto `out` once every stage exits 0 with a non-empty result: writing
    // straight to `out` would truncate the previous good SVG on a failed re-render. Staged beside
    // `out`, not in the `/tmp` workdir, because `rename` works only within one filesystem and
    // `out` is under the bind-mounted checkout.
    let staged = staging_path(out);
    let _cleanup = RemoveOnDrop(staged.clone());
    collapse_to_svg(perf_data, &staged, args.folded.as_deref())?;

    let bytes = fs::metadata(&staged).map(|m| m.len()).unwrap_or(0);
    if bytes == 0 {
        bail!("the capture produced no resolvable stacks -- {} left unchanged", out.display());
    }
    fs::rename(&staged, out)
        .with_context(|| format!("moving {} onto {}", staged.display(), out.display()))?;

    println!("\nwrote {} ({bytes} bytes)", out.display());
    Ok(())
}

/// `<out>.<pid>.partial`: beside `out` so [`fs::rename`] stays on one filesystem, pid-suffixed so
/// two concurrent runs with one `--out` don't share it.
fn staging_path(out: &Path) -> PathBuf {
    let mut name = out.file_name().unwrap_or_default().to_os_string();
    name.push(format!(".{}.partial", std::process::id()));
    out.with_file_name(name)
}

/// Removes a path when dropped, so a failure before the final `rename` removes the staging file.
struct RemoveOnDrop(PathBuf);

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

/// `perf record`'s argv, up to and including the `--` before the workload.
fn record_argv(freq: u32, perf_data: &Path) -> Vec<String> {
    vec![
        PERF.to_string(),
        "record".to_string(),
        "-F".to_string(),
        freq.to_string(),
        "-g".to_string(),
        "--call-graph".to_string(),
        "dwarf".to_string(),
        "-o".to_string(),
        perf_data.to_string_lossy().into_owned(),
        "--".to_string(),
    ]
}

/// `perf script -i <perf.data> | inferno-collapse-perf | inferno-flamegraph > <out>`, as three
/// processes connected by pipes.
///
/// Every stage's status is checked; a silent mid-pipeline failure would leave a plausible but
/// empty SVG. With `folded`, `tee` also writes the collapsed stacks to that path. They're the
/// form a script can compute over (`perf/folded_share.py`), so a second question about one
/// capture needs no second `perf record`.
fn collapse_to_svg(perf_data: &Path, out: &Path, folded: Option<&Path>) -> anyhow::Result<()> {
    match folded {
        Some(folded) => println!(
            "-- {PERF} script | {COLLAPSE} | tee {} | {FLAMEGRAPH} > {}",
            folded.display(),
            out.display()
        ),
        None => println!("-- {PERF} script | {COLLAPSE} | {FLAMEGRAPH} > {}", out.display()),
    }
    let mut script = Command::new(PERF)
        .arg("script")
        .arg("-i")
        .arg(perf_data)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawning `{PERF} script`"))?;
    let script_out = script.stdout.take().expect("stdout was piped");

    let mut collapse = Command::new(COLLAPSE)
        .stdin(Stdio::from(script_out))
        .stdout(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawning `{COLLAPSE}`"))?;
    let collapse_out = collapse.stdout.take().expect("stdout was piped");

    // `tee` keeps one streaming pipeline; coreutils is in every image this runs in.
    let mut tee = None;
    let mut flame_stdin = collapse_out;
    if let Some(folded) = folded {
        if let Some(parent) = folded.parent() {
            fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
        }
        let mut child = Command::new(TEE)
            .arg(folded)
            .stdin(Stdio::from(flame_stdin))
            .stdout(Stdio::piped())
            .spawn()
            .with_context(|| format!("spawning `{TEE} {}`", folded.display()))?;
        flame_stdin = child.stdout.take().expect("stdout was piped");
        tee = Some(child);
    }

    let svg = fs::File::create(out).with_context(|| format!("creating {}", out.display()))?;
    let mut flame = Command::new(FLAMEGRAPH)
        .stdin(Stdio::from(flame_stdin))
        .stdout(Stdio::from(svg))
        .spawn()
        .with_context(|| format!("spawning `{FLAMEGRAPH}`"))?;

    // Waited in pipeline order, so an upstream failure is reported as itself, not as a
    // downstream early EOF.
    let mut stages: Vec<(&str, &mut std::process::Child)> =
        vec![(PERF, &mut script), (COLLAPSE, &mut collapse)];
    if let Some(child) = tee.as_mut() {
        stages.push((TEE, child));
    }
    stages.push((FLAMEGRAPH, &mut flame));
    for (name, child) in stages {
        let status = child.wait().with_context(|| format!("waiting for `{name}`"))?;
        if !status.success() {
            bail!("`{name}` failed with {status}");
        }
    }
    Ok(())
}

/// Fails naming where the tooling lives, before a multi-minute build, rather than letting the
/// first `spawn` fail with a bare `No such file or directory`.
fn require_tools() -> anyhow::Result<()> {
    let missing: Vec<&str> =
        [PERF, COLLAPSE, FLAMEGRAPH].into_iter().filter(|tool| !on_path(tool)).collect();
    if missing.is_empty() {
        return Ok(());
    }
    bail!(
        "{} not found on PATH. `perf` and `inferno` live only in the profiling image \
         (crates/logit-perf/Dockerfile), never in the dev image -- run this as \
         `script/perf flamegraph --scenario ...`, which builds that image and runs the harness \
         inside it with the capabilities `perf record` needs.",
        missing.join(", ")
    );
}

/// Whether `tool` resolves to a file on `$PATH`.
///
/// A `$PATH` walk, not `tool --version`: Debian's `perf` wrapper warns when the running kernel's
/// `perf_<version>` isn't installed, so its exit status doesn't answer "is it installed".
fn on_path(tool: &str) -> bool {
    let Some(path) = std::env::var_os("PATH") else { return false };
    std::env::split_paths(&path).any(|dir| dir.join(tool).is_file())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_argv_carries_the_frequency_output_path_and_dwarf_unwinding() {
        let argv = record_argv(999, Path::new("/tmp/x/perf.data"));
        assert_eq!(
            argv,
            vec![
                "perf",
                "record",
                "-F",
                "999",
                "-g",
                "--call-graph",
                "dwarf",
                "-o",
                "/tmp/x/perf.data",
                "--",
            ]
        );
    }

    #[test]
    fn record_argv_ends_with_the_separator_so_the_workload_appends_cleanly() {
        let argv = record_argv(97, Path::new("/tmp/perf.data"));
        assert_eq!(argv.last().map(String::as_str), Some("--"));
        assert_eq!(argv[3], "97");
    }

    #[test]
    fn on_path_finds_a_binary_that_exists_and_not_one_that_does_not() {
        assert!(on_path("sh"), "/bin/sh is on PATH in any environment this runs in");
        assert!(!on_path("logit-perf-definitely-not-a-real-binary"));
    }
}
