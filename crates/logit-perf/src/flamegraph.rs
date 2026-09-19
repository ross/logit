//! `logit-perf flamegraph`: one command from a scenario name to an SVG.
//!
//! Three steps, none of them novel -- the value here is that they're one command with the right
//! arguments already filled in, rather than a sequence anyone re-derives from memory each time:
//!
//! 1. `cargo build --profile profiling -p logit-cli`. The root `[profile.profiling]` inherits
//!    `release`'s optimization settings verbatim (profiling anything else would be profiling a
//!    different program) and adds back the `line-tables-only` debug info and the unstripped symbol
//!    table `perf` needs to name what it sampled -- see that profile's own comment in the root
//!    `Cargo.toml`.
//! 2. `perf record -F <freq> -g --call-graph dwarf -o <tmp>/perf.data -- logit run <scenario>`,
//!    through the same spawn/reader-thread/settle-then-SIGTERM machinery `run` uses
//!    (`crate::run::spawn_and_measure`'s `wrapper`), so the capture stops on the generator's own
//!    `generation complete` line rather than at some arbitrary wall-clock cutoff. `--call-graph
//!    dwarf`, not the default frame-pointer walk: this profile doesn't force
//!    `force-frame-pointers`, and an optimized Rust binary without them gives `-g` almost nothing
//!    to unwind. For a scenario that doesn't self-exit (`native-relay`), the settle-then-SIGTERM
//!    goes to `perf`, which forwards it to `logit` (draining and exiting 0 on its own), waits,
//!    writes `perf.data`, and then re-raises SIGTERM on itself -- verified end to end, and why
//!    `spawn_and_measure` accepts a wrapper's SIGTERM death as a completed run.
//! 3. `perf script | inferno-collapse-perf | inferno-flamegraph`, run as a real three-process
//!    pipeline rather than buffered through this process -- a `perf script` dump of a
//!    multi-million-event scenario is hundreds of megabytes. The SVG lands on a staging file and
//!    is `rename`d onto `--out` only once all three stages have exited 0 and the result is
//!    non-empty, so a failed re-render leaves the previous good flamegraph intact rather than
//!    truncating it.
//!
//! **The tooling is not in the dev image.** `perf` and `inferno` live only in
//! `crates/logit-perf/Dockerfile`, a throwaway image built from `logit-dev:local` and run with
//! the elevated capabilities `perf record` needs (`CAP_SYS_ADMIN`, unconfined seccomp) --
//! precedent: `script/protogen`. `Dockerfile.dev` is deliberately untouched, so the ordinary
//! edit/check/test loop carries neither the extra tooling nor those capabilities
//! (`docs/adr/load-test-harness.md`'s "Profiling" section). Running this outside that image is a
//! clear error naming `script/perf flamegraph`, not a confusing failure three steps in.

use crate::load::{CpuSet, LoadPlan};
use crate::run::{self, Drive, SpawnConfig};
use crate::scenario::{self, Scenario, Workload};
use anyhow::{bail, Context};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

/// The profile `perf record` is pointed at -- always, regardless of what `run` was last built
/// with. A stripped `release` binary produces a flamegraph of hex addresses.
const PROFILE: &str = "profiling";

const PERF: &str = "perf";
const COLLAPSE: &str = "inferno-collapse-perf";
const FLAMEGRAPH: &str = "inferno-flamegraph";

pub struct FlamegraphArgs {
    pub scenario: String,
    /// Defaults to `perf/results/<scenario>.svg` under the repo root.
    pub out: Option<PathBuf>,
    /// Sampling frequency in Hz. 999 rather than a round 1000 so the sampler can't lock step with
    /// anything in the program running on a whole-millisecond cadence -- the usual convention.
    pub freq: u32,
    pub settle: Duration,
    pub timeout: Duration,
    pub shutdown_timeout: Duration,
    pub no_build: bool,
    /// Sender/child CPU pinning, for a real-socket scenario -- ignored by a generator-driven one.
    pub pin_sender: Option<CpuSet>,
    pub pin_child: Option<CpuSet>,
}

pub fn flamegraph(root: &Path, args: FlamegraphArgs) -> anyhow::Result<()> {
    let scenarios_dir = root.join("perf/scenarios");
    let scenario = scenario::find(&scenarios_dir, &args.scenario)?;
    require_tools()?;
    // Fresh spool before this capture's own spawn -- same reasoning as `run`'s per-repeat clear
    // (`crate::spool`'s module doc). A stale spool would make the flamegraph's own I/O shape
    // unrepresentative, the same way it corrupts `run`'s throughput numbers.
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
    // A leftover capture from an earlier run at this pid would be rendered as if it were this
    // run's -- caught rather than silently overwritten, the same reasoning `attribute` applies to
    // its own dump.
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
        // to retry the (fast) render against, not something to throw away on the way out.
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
    // A real-socket scenario is profiled under exactly the load `run` gives it -- the sender runs
    // in this process, outside the `perf record` wrapper, so what's captured is the receive side
    // and nothing else. No telemetry leg is attached here: a flamegraph wants the shipped graph,
    // not the graph plus two of the harness's own nodes.
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
    // `perf record` starts capturing at spawn, not at `ready` -- unlike `run`'s events/s (which
    // deliberately excludes startup from the graph's own per-event cost), this line describes how
    // much of `perf.data` was actually written, so it's `startup + wall` (spawn ->
    // completion/shutdown) rather than the measured wall alone.
    println!(
        "   captured {:.1}s of wall time ({:.1} MiB of samples)",
        measured.startup.unwrap_or_default().as_secs_f64() + measured.wall().as_secs_f64(),
        fs::metadata(perf_data).map(|m| m.len()).unwrap_or(0) as f64 / (1024.0 * 1024.0),
    );

    // Rendered into a staging file first and moved onto `out` only once every stage has exited 0
    // and the result is non-empty -- a failed re-render must never leave the previous good SVG
    // truncated, which is exactly what redirecting straight onto `out` does the moment the shell
    // creates the file. Staged in `out`'s *own* directory rather than the temp workdir: `rename`
    // is only atomic (and only succeeds at all) within one filesystem, and the workdir is `/tmp`
    // while `out` is under the bind-mounted checkout.
    let staged = staging_path(out);
    let _cleanup = RemoveOnDrop(staged.clone());
    collapse_to_svg(perf_data, &staged)?;

    let bytes = fs::metadata(&staged).map(|m| m.len()).unwrap_or(0);
    if bytes == 0 {
        bail!("the capture produced no resolvable stacks -- {} left unchanged", out.display());
    }
    fs::rename(&staged, out)
        .with_context(|| format!("moving {} onto {}", staged.display(), out.display()))?;

    println!("\nwrote {} ({bytes} bytes)", out.display());
    Ok(())
}

/// `<out>.<pid>.partial` -- beside `out`, so [`fs::rename`] onto it is a same-filesystem move,
/// and pid-suffixed so two concurrent runs writing the same `--out` can't share a staging file.
fn staging_path(out: &Path) -> PathBuf {
    let mut name = out.file_name().unwrap_or_default().to_os_string();
    name.push(format!(".{}.partial", std::process::id()));
    out.with_file_name(name)
}

/// Removes a path when dropped, so a failure anywhere between rendering and the final `rename`
/// takes the half-written staging file with it instead of leaving it beside the real output.
struct RemoveOnDrop(PathBuf);

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

/// `perf record`'s own argv, up to and including the `--` that separates it from the workload.
/// Split out so the exact flags are readable in one place and asserted by a test, rather than
/// buried in a builder chain.
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
/// real processes connected by pipes. Every stage's status is checked: a silent failure in the
/// middle of the pipeline would otherwise leave a plausible-looking but empty SVG.
fn collapse_to_svg(perf_data: &Path, out: &Path) -> anyhow::Result<()> {
    println!("-- {PERF} script | {COLLAPSE} | {FLAMEGRAPH} > {}", out.display());
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

    let svg = fs::File::create(out).with_context(|| format!("creating {}", out.display()))?;
    let mut flame = Command::new(FLAMEGRAPH)
        .stdin(Stdio::from(collapse_out))
        .stdout(Stdio::from(svg))
        .spawn()
        .with_context(|| format!("spawning `{FLAMEGRAPH}`"))?;

    // Waited in pipeline order, so an upstream stage's failure is reported as itself rather than
    // as the downstream stage seeing an early EOF.
    for (name, child) in [(PERF, &mut script), (COLLAPSE, &mut collapse), (FLAMEGRAPH, &mut flame)]
    {
        let status = child.wait().with_context(|| format!("waiting for `{name}`"))?;
        if !status.success() {
            bail!("`{name}` failed with {status}");
        }
    }
    Ok(())
}

/// Fails with the one message worth printing -- where the tooling actually lives -- rather than
/// letting the first `spawn` fail with a bare `No such file or directory` after a multi-minute
/// build has already run.
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

/// Whether `tool` resolves to an executable on `$PATH`. Deliberately a `$PATH` walk rather than
/// spawning `tool --version`: `perf` on Debian is a shell wrapper that probes for a
/// `perf_<kernel-version>` binary and prints a warning when the running kernel's exact build
/// isn't installed, so "did it exit 0" answers a different question than "is it installed."
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
