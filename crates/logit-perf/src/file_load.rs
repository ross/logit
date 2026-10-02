//! Driving a `tail_in` scenario from a file `logit-perf` writes before the spawn.
//!
//! A `tail_in` scenario has no generator and no socket: its load is a file. The harness renders
//! the scenario's lines from a weighted template model (the format `crate::load` reads for UDP),
//! writes them to the one path the scenario's `tail_in` names, spawns `logit`, and ends the
//! measurement once the sink's `logit.component.events.received` reaches the line count. That
//! count is read from the same `crate::telemetry_leg` dump a UDP scenario's denominator comes
//! from, followed while the child runs (`telemetry_leg::DumpFollower`).
//!
//! ## The spec
//!
//! `perf/load/<scenario>.yaml` with `kind: file` (`perf/load/README.md`'s "File scenarios"):
//!
//! ```yaml
//! kind: file
//! target: app            # the `tail_in` component; its one `paths:` entry is the file written
//! lines: 3000000         # lines written, and the exact count the sink must receive
//! rotate_after: 1500000  # optional: the first file's share, the rest goes to its replacement
//! model: app-log.yaml    # weighted `lines:` templates, as `crate::load` reads them
//! ```
//!
//! **Everything is written before the spawn**, so no write competes with the measured child.
//! With `rotate_after`, the replacement file is staged beside the tailed one under a name the
//! pattern doesn't match, and the rotation runs once the sink has received half of the first
//! file: a hard link of the tailed file at `<path>.1`, then a `rename(2)` of the staged file onto
//! `<path>`. The result is logrotate's `create` mode (old inode at `<path>.1`, a new inode at
//! `<path>`), but `<path>` names a file at every instant. logrotate's own sequence renames the
//! file away first and creates the new one after, and a scan landing between the two finds
//! nothing at `<path>`: `tail_in` then retires the old inode and opens the new one as a new file,
//! with nothing lost but no rotation counted (`docs/known-gaps.md`). The exact self-check counts
//! rotations, so the harness avoids that gap rather than failing a correct run on it.
//!
//! **The file lives under `perf/results/`**, resolved against the scenario's directory as
//! `logit` resolves it, and its parent directory is cleared before every spawn and removed after.
//! `crate::spool::require_within_results_dir` refuses any other location, so a scenario typo
//! can't point the clear at anything else.
//!
//! **The completion time is resolved to the leg's drain cadence**, [`LEG_INTERVAL`]: the sink's
//! count is visible only once a drain carries it. CPU per event comes from `wait4` over the whole
//! process and doesn't depend on it.

use crate::load::{self, LineRenderer, SplitMix64};
use anyhow::{bail, Context};
use logit_core::{interner, Event, MetricKind, Value};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// The telemetry leg's drain cadence for a file scenario under `run`, and so the resolution of
/// its wall time. A tenth of a second keeps the error on a 5-10 s run near 1-2%; the drains
/// themselves cost the child a few points of telemetry each.
pub const LEG_INTERVAL: Duration = Duration::from_millis(100);

/// How often the harness re-reads the dump for new drains while the child runs.
pub const POLL: Duration = Duration::from_millis(10);

/// The suffix the tailed file is renamed to at rotation, as logrotate's first rotation names it.
const ROTATED_SUFFIX: &str = "1";
/// The suffix of the staged replacement, which the scenario's exact (no `*`) path never matches.
const STAGED_SUFFIX: &str = "next";

/// One `perf/load/<scenario>.yaml` with `kind: file`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileSpec {
    /// The `tail_in` component whose single `paths:` entry is the file to write.
    pub target: String,
    /// The component whose `events.received` is the run's denominator. Needed only with several
    /// sinks, as in a UDP spec.
    #[serde(default)]
    pub sink: Option<String>,
    /// Lines written in total, across both files when rotating. The sink must receive
    /// this many events.
    pub lines: u64,
    /// When set, the first file holds this many lines and its replacement the rest.
    #[serde(default)]
    pub rotate_after: Option<u64>,
    /// Seeds the weighted template choice, so every run writes the same bytes.
    #[serde(default = "default_seed")]
    pub seed: u64,
    /// Distinct lines rendered, then cycled. Prime, as `crate::load`'s ring is.
    #[serde(default = "default_ring_lines")]
    pub ring_lines: usize,
    /// The line model, relative to this spec file.
    pub model: PathBuf,
}

fn default_seed() -> u64 {
    20_261_002
}
fn default_ring_lines() -> usize {
    4_093
}

/// The two kinds of sidecar spec, told apart by a top-level `kind:` (`udp` when absent).
pub enum AnySpec {
    Udp(load::LoadSpec),
    File(FileSpec),
}

/// Reads a sidecar spec of either kind. A UDP spec keeps `crate::load::read_spec`'s parsing and
/// checks; `kind:` is removed before either kind's own fields are read.
pub fn read_any_spec(path: &Path) -> anyhow::Result<AnySpec> {
    let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let mut value: serde_norway::Value =
        serde_norway::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    let kind = match value.as_mapping_mut().and_then(|map| map.remove("kind")) {
        None => "udp".to_string(),
        Some(kind) => kind
            .as_str()
            .with_context(|| format!("{}: `kind` is not a plain string", path.display()))?
            .to_string(),
    };
    match kind.as_str() {
        "udp" => Ok(AnySpec::Udp(load::read_spec(path)?)),
        "file" => {
            let spec: FileSpec = serde_norway::from_value(value)
                .with_context(|| format!("parsing {}", path.display()))?;
            validate_spec(&spec).with_context(|| format!("{}", path.display()))?;
            Ok(AnySpec::File(spec))
        }
        other => bail!(
            "{}: `kind: {other}` is not a load kind -- `udp` (the default) sends datagrams, \
             `file` writes a file for `tail_in`",
            path.display()
        ),
    }
}

fn validate_spec(spec: &FileSpec) -> anyhow::Result<()> {
    if spec.target.is_empty() {
        bail!("`target` is empty -- it names the `tail_in` component whose file is written");
    }
    if spec.lines == 0 {
        bail!("`lines` is 0 -- there would be nothing to measure");
    }
    if spec.ring_lines == 0 {
        bail!("`ring_lines` is 0 -- the pre-rendered ring would be empty");
    }
    if let Some(first) = spec.rotate_after {
        if first == 0 || first >= spec.lines {
            bail!(
                "`rotate_after` is {first} but must be between 1 and `lines - 1` ({}) -- both \
                 the rotated file and its replacement need lines for the rotation to be read \
                 through",
                spec.lines - 1
            );
        }
    }
    Ok(())
}

/// The single file a scenario's `target` tails, resolved against the scenario's directory.
///
/// Read as a bare YAML value, as `crate::load::target_addr` reads a `bind:`. One entry,
/// with no `*` and no `!env`: the harness has to know the one path it writes.
fn target_path(scenario_path: &Path, scenario_yaml: &str, target: &str) -> anyhow::Result<PathBuf> {
    let value: serde_norway::Value =
        serde_norway::from_str(scenario_yaml).context("parsing the scenario YAML")?;
    let component = value
        .get("components")
        .and_then(serde_norway::Value::as_mapping)
        .context("no top-level `components` mapping")?
        .get(serde_norway::Value::from(target))
        .with_context(|| format!("no component named `{target}` (the load spec's `target`)"))?;
    let kind = component.get("type").and_then(serde_norway::Value::as_str).unwrap_or_default();
    if kind != "tail_in" {
        bail!("component `{target}` is a `{kind}`, but a `kind: file` spec drives a `tail_in`");
    }
    let paths = component
        .get("paths")
        .and_then(serde_norway::Value::as_sequence)
        .with_context(|| format!("component `{target}` has no `paths:` list"))?;
    let [entry] = paths.as_slice() else {
        bail!(
            "component `{target}` lists {} paths -- a file scenario writes one file, so its \
             `tail_in` names one",
            paths.len()
        );
    };
    if matches!(entry, serde_norway::Value::Tagged(_)) {
        bail!("component `{target}`'s path carries a YAML tag (`!env`), which this crate never resolves");
    }
    let raw = entry
        .as_str()
        .with_context(|| format!("component `{target}`'s path is not a plain string"))?;
    if raw.contains('*') {
        bail!(
            "component `{target}`'s path `{raw}` has a `*` -- the rotation renames the file to \
             `<path>.{ROTATED_SUFFIX}` and stages its replacement as `<path>.{STAGED_SUFFIX}`, \
             which a wildcard could match"
        );
    }
    let base = scenario_path.parent().unwrap_or_else(|| Path::new(""));
    Ok(crate::spool::normalize(&base.join(raw)))
}

/// A file spec with its rendered lines and the resolved path. Built once per scenario.
pub struct FilePlan {
    pub spec: FileSpec,
    pub spec_path: PathBuf,
    /// The file the scenario's `tail_in` names.
    pub path: PathBuf,
    /// Distinct lines, each ending in `\n`, cycled to make up `spec.lines`.
    ring: Vec<Vec<u8>>,
}

impl FilePlan {
    /// Reads the spec and its model, renders the ring, and resolves the target's path, refusing
    /// one whose directory isn't strictly inside `<root>/perf/results/`.
    pub fn build(root: &Path, spec_path: &Path, scenario_path: &Path) -> anyhow::Result<FilePlan> {
        let spec = match read_any_spec(spec_path)? {
            AnySpec::File(spec) => spec,
            AnySpec::Udp(_) => bail!("{} is not a `kind: file` spec", spec_path.display()),
        };
        let yaml = fs::read_to_string(scenario_path)
            .with_context(|| format!("reading {}", scenario_path.display()))?;
        let path = target_path(scenario_path, &yaml, &spec.target)?;
        let dir = path.parent().context("the tailed path has no parent directory")?;
        crate::spool::require_within_results_dir(root, dir)?;

        let model = load::read_model(spec_path, &spec.model)?;
        let mut renderers: Vec<LineRenderer> = model
            .lines
            .iter()
            .map(|line| LineRenderer::compile(&line.template))
            .collect::<anyhow::Result<_>>()?;
        let weights = load::cumulative(model.lines.iter().map(|line| line.weight));
        let mut rng = SplitMix64::new(spec.seed);
        let mut scratch = String::new();
        let ring = (0..spec.ring_lines)
            .map(|_| {
                scratch.clear();
                renderers[rng.weighted(&weights)].render(&mut scratch);
                let mut line = scratch.as_bytes().to_vec();
                line.push(b'\n');
                line
            })
            .collect();
        Ok(FilePlan { spec, spec_path: spec_path.to_path_buf(), path, ring })
    }

    /// Lines in the first file: all of them, or `rotate_after`.
    fn first_lines(&self) -> u64 {
        self.spec.rotate_after.unwrap_or(self.spec.lines)
    }

    /// The sink count at which [`FilePlan::rotate`] runs: half the first file, so the rotation
    /// lands while the first file is still being read. `None` without `rotate_after`.
    pub fn rotate_at(&self) -> Option<u64> {
        self.spec.rotate_after.map(|first| first.div_ceil(2))
    }

    /// How many rotations a complete run makes, for the self-check.
    pub fn rotations(&self) -> u64 {
        u64::from(self.spec.rotate_after.is_some())
    }

    /// Bytes written across both files, newlines included.
    pub fn bytes(&self) -> u64 {
        let ring_bytes: u64 = self.ring.iter().map(|line| line.len() as u64).sum();
        let len = self.ring.len() as u64;
        let whole = self.spec.lines / len;
        let rest: u64 =
            self.ring[..(self.spec.lines % len) as usize].iter().map(|l| l.len() as u64).sum();
        whole * ring_bytes + rest
    }

    /// The mean line length, newline excluded.
    pub fn mean_line_bytes(&self) -> f64 {
        self.bytes() as f64 / self.spec.lines as f64 - 1.0
    }

    fn sibling(&self, suffix: &str) -> PathBuf {
        let mut name = self.path.file_name().unwrap_or_default().to_os_string();
        name.push(format!(".{suffix}"));
        self.path.with_file_name(name)
    }

    /// Clears the file's directory and writes every file a run needs, before the spawn. The
    /// returned guard removes the directory when dropped.
    pub fn stage(&self) -> anyhow::Result<Staged> {
        let dir = self.path.parent().context("the tailed path has no parent directory")?;
        if dir.exists() {
            fs::remove_dir_all(dir).with_context(|| format!("clearing {}", dir.display()))?;
        }
        fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        let staged = Staged(dir.to_path_buf());
        let first = self.first_lines();
        self.write_lines(&self.path, 0, first)?;
        if self.spec.rotate_after.is_some() {
            self.write_lines(&self.sibling(STAGED_SUFFIX), first, self.spec.lines - first)?;
        }
        Ok(staged)
    }

    /// Writes `count` lines to `path`, continuing the ring from line `start`.
    fn write_lines(&self, path: &Path, start: u64, count: u64) -> anyhow::Result<()> {
        let file =
            fs::File::create(path).with_context(|| format!("creating {}", path.display()))?;
        let mut out = BufWriter::with_capacity(1024 * 1024, file);
        let len = self.ring.len() as u64;
        for index in start..start + count {
            out.write_all(&self.ring[(index % len) as usize])
                .with_context(|| format!("writing {}", path.display()))?;
        }
        out.flush().with_context(|| format!("writing {}", path.display()))?;
        Ok(())
    }

    /// Rotates to logrotate `create`'s end state, the old inode at `<path>.1` and the staged
    /// replacement at `<path>`, by a hard link then a rename rather than logrotate's rename then
    /// create, so no scan finds `<path>` missing (see the module doc).
    pub fn rotate(&self) -> anyhow::Result<()> {
        let rotated = self.sibling(ROTATED_SUFFIX);
        fs::hard_link(&self.path, &rotated)
            .with_context(|| format!("linking {} as {}", self.path.display(), rotated.display()))?;
        let staged = self.sibling(STAGED_SUFFIX);
        fs::rename(&staged, &self.path)
            .with_context(|| format!("renaming {} to {}", staged.display(), self.path.display()))?;
        Ok(())
    }
}

/// A file scenario's staged directory, removed on drop: the files run to hundreds of MiB, too
/// much to leave behind per repeat.
pub struct Staged(PathBuf);

impl Drop for Staged {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// The sink's delivered count so far. An error before the sink's first drain is expected; one
/// that persists (a misnamed or ambiguous `sink:`) is the caller's to report.
pub fn delivered_so_far(events: &[Event], sink: Option<&str>) -> anyhow::Result<u64> {
    crate::attribute::delivered_at_sink(&crate::attribute::aggregate(events), sink)
}

/// `tail_in`'s own counters, folded out of the dump for one component.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TailStats {
    /// `logit.input.lines`: lines split and offered to the decoder.
    pub lines: u64,
    /// `logit.input.files.rotated`.
    pub rotated: u64,
    /// `logit.component.diagnostics{key=...}`: any of `bad_line`, `long_line`, `read_error`,
    /// `invalid_utf8`, `truncated` means the run read something other than what was written.
    pub diagnostics: BTreeMap<String, u64>,
}

const LINES: &str = "logit.input.lines";
const ROTATED: &str = "logit.input.files.rotated";
const DIAGNOSTICS: &str = "logit.component.diagnostics";

pub fn tail_stats(events: &[Event], component: &str) -> TailStats {
    let mut stats = TailStats::default();
    for event in events {
        if event.attributes.get("component").and_then(Value::as_str) != Some(component) {
            continue;
        }
        for metric in &event.metrics {
            let MetricKind::Sum(sum) = &metric.kind else { continue };
            match interner::resolve(metric.name) {
                LINES => stats.lines += sum.value as u64,
                ROTATED => stats.rotated += sum.value as u64,
                DIAGNOSTICS => {
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

/// Everything a file run has to be true for its numbers to mean anything. A file can't drop a
/// line the way a socket can, so every check is exact:
///
/// 1. The sink received `lines` events: fewer is loss, more is a replay.
/// 2. `tail_in` split `lines` lines.
/// 3. No diagnostics on `tail_in`: a `bad_line` or `long_line` means the model renders something
///    the decoder doesn't take as one line.
/// 4. The rotations the plan made were the ones `tail_in` counted.
pub fn self_check(
    name: &str,
    plan: &FilePlan,
    delivered: u64,
    stats: &TailStats,
) -> anyhow::Result<()> {
    let lines = plan.spec.lines;
    if delivered != lines {
        bail!(
            "{name}: {lines} lines were written but the sink received {delivered} events -- a \
             file loses nothing, so {} is a tail bug or a harness one, not a measurement",
            if delivered < lines { "the shortfall" } else { "the excess" }
        );
    }
    if stats.lines != lines {
        bail!("{name}: {lines} lines were written but `tail_in` split {}", stats.lines);
    }
    if !stats.diagnostics.is_empty() {
        let rendered: Vec<String> =
            stats.diagnostics.iter().map(|(key, count)| format!("{key}={count}")).collect();
        bail!(
            "{name}: `tail_in` reported diagnostics ({}) -- the run read something other than \
             the lines written",
            rendered.join(", ")
        );
    }
    if stats.rotated != plan.rotations() {
        bail!(
            "{name}: the harness rotated the file {} time(s) but `tail_in` counted {}",
            plan.rotations(),
            stats.rotated
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir()
            .join(format!("logit-perf-file-load-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A repo-shaped tree with one scenario, its spec, and a model, returning (root, scenario,
    /// spec).
    fn tree(name: &str, spec_body: &str, tail_path: &str) -> (PathBuf, PathBuf, PathBuf) {
        let root = temp_dir(name);
        fs::create_dir_all(root.join("perf/scenarios")).unwrap();
        fs::create_dir_all(root.join("perf/load")).unwrap();
        fs::create_dir_all(root.join("perf/results")).unwrap();
        let scenario = root.join("perf/scenarios/t.yaml");
        fs::write(
            &scenario,
            format!(
                "components:\n  app:\n    type: tail_in\n    paths: [\"{tail_path}\"]\n    read_from: beginning\n  out:\n    type: null_out\n    sources: [app]\n"
            ),
        )
        .unwrap();
        fs::write(
            root.join("perf/load/m.yaml"),
            "lines:\n  - { weight: 3, template: 'a={seq%5} b=x' }\n  - { weight: 1, template: 'longer line {seq%7}' }\n",
        )
        .unwrap();
        let spec = root.join("perf/load/t.yaml");
        fs::write(&spec, spec_body).unwrap();
        (root, scenario, spec)
    }

    fn line_count(path: &Path) -> usize {
        fs::read(path).unwrap().iter().filter(|b| **b == b'\n').count()
    }

    #[test]
    fn a_spec_with_no_kind_is_read_as_udp() {
        let dir = temp_dir("udp-kind");
        fs::write(dir.join("m.yaml"), "lines:\n  - { weight: 1, template: \"a.{seq%3}:1|c\" }\n")
            .unwrap();
        let path = dir.join("s.yaml");
        fs::write(
            &path,
            "target: statsd\ndatagrams: 10\nmodel: m.yaml\ndatagram_mix:\n  - { weight: 1, single: true }\n",
        )
        .unwrap();
        assert!(matches!(read_any_spec(&path).unwrap(), AnySpec::Udp(_)));
    }

    #[test]
    fn an_explicit_kind_udp_spec_is_read_as_udp() {
        let dir = temp_dir("udp-explicit");
        fs::write(dir.join("m.yaml"), "lines:\n  - { weight: 1, template: \"a.{seq%3}:1|c\" }\n")
            .unwrap();
        let path = dir.join("s.yaml");
        fs::write(
            &path,
            "kind: udp\ntarget: statsd\ndatagrams: 10\nmodel: m.yaml\ndatagram_mix:\n  - { weight: 1, single: true }\n",
        )
        .unwrap();
        assert!(matches!(read_any_spec(&path).unwrap(), AnySpec::Udp(_)));
        // `LoadPlan::build` re-reads the spec through `load::read_spec`.
        assert_eq!(load::read_spec(&path).unwrap().datagrams, 10);
    }

    #[test]
    fn a_kind_file_spec_is_read_with_its_defaults() {
        let (_, _, spec) =
            tree("file-kind", "kind: file\ntarget: app\nlines: 10\nmodel: m.yaml\n", "x");
        match read_any_spec(&spec).unwrap() {
            AnySpec::File(spec) => {
                assert_eq!(spec.lines, 10);
                assert_eq!(spec.rotate_after, None);
                assert_eq!(spec.ring_lines, default_ring_lines());
            }
            AnySpec::Udp(_) => panic!("kind: file read as udp"),
        }
    }

    #[test]
    fn an_unknown_kind_or_field_is_rejected() {
        let (_, _, spec) =
            tree("bad-kind", "kind: tcp\ntarget: app\nlines: 1\nmodel: m.yaml\n", "x");
        assert!(format!("{:#}", read_any_spec(&spec).err().unwrap()).contains("`kind: tcp`"));
        let (_, _, spec) =
            tree("bad-field", "kind: file\ntarget: app\nlines: 1\nmodel: m.yaml\nrate: 5\n", "x");
        assert!(format!("{:#}", read_any_spec(&spec).err().unwrap()).contains("rate"));
    }

    #[test]
    fn rotate_after_must_leave_lines_on_both_sides() {
        for rotate_after in [0, 10, 11] {
            let (_, _, spec) = tree(
                &format!("rot-{rotate_after}"),
                &format!("kind: file\ntarget: app\nlines: 10\nrotate_after: {rotate_after}\nmodel: m.yaml\n"),
                "x",
            );
            let err = format!("{:#}", read_any_spec(&spec).err().unwrap());
            assert!(err.contains("rotate_after"), "{err}");
        }
    }

    #[test]
    fn a_path_outside_perf_results_is_refused() {
        let (root, scenario, spec) = tree(
            "outside",
            "kind: file\ntarget: app\nlines: 10\nmodel: m.yaml\n",
            "../scenarios/tail/app.log",
        );
        let err = FilePlan::build(&root, &spec, &scenario).err().expect("outside perf/results");
        assert!(format!("{err:#}").contains("perf/results"), "{err:#}");
    }

    #[test]
    fn a_wildcard_path_is_refused() {
        let (root, scenario, spec) = tree(
            "wildcard",
            "kind: file\ntarget: app\nlines: 10\nmodel: m.yaml\n",
            "../results/tail/*.log",
        );
        let err = FilePlan::build(&root, &spec, &scenario).err().expect("a wildcard");
        assert!(format!("{err:#}").contains("has a `*`"), "{err:#}");
    }

    #[test]
    fn stage_writes_every_line_and_the_guard_removes_the_directory() {
        let (root, scenario, spec) = tree(
            "stage",
            "kind: file\ntarget: app\nlines: 25\nring_lines: 7\nmodel: m.yaml\n",
            "../results/tail/app.log",
        );
        let plan = FilePlan::build(&root, &spec, &scenario).unwrap();
        assert_eq!(plan.path, root.join("perf/results/tail/app.log"));
        let staged = plan.stage().unwrap();
        assert_eq!(line_count(&plan.path), 25);
        assert_eq!(fs::metadata(&plan.path).unwrap().len(), plan.bytes());
        drop(staged);
        assert!(!root.join("perf/results/tail").exists());
        assert!(root.join("perf/results").exists(), "only the scenario's own directory goes");
    }

    #[test]
    fn a_rotating_plan_splits_its_lines_and_rotate_ends_as_logrotate_create_does() {
        let (root, scenario, spec) = tree(
            "rotate",
            "kind: file\ntarget: app\nlines: 25\nrotate_after: 10\nring_lines: 7\nmodel: m.yaml\n",
            "../results/tail/app.log",
        );
        let plan = FilePlan::build(&root, &spec, &scenario).unwrap();
        assert_eq!(plan.rotate_at(), Some(5));
        let _staged = plan.stage().unwrap();
        let before = fs::read(&plan.path).unwrap();
        assert_eq!(line_count(&plan.path), 10);
        assert_eq!(line_count(&plan.sibling(STAGED_SUFFIX)), 15);
        use std::os::unix::fs::MetadataExt;
        let old_ino = fs::metadata(&plan.path).unwrap().ino();
        let new_ino = fs::metadata(plan.sibling(STAGED_SUFFIX)).unwrap().ino();
        plan.rotate().unwrap();
        assert_eq!(fs::read(plan.sibling(ROTATED_SUFFIX)).unwrap(), before);
        assert_eq!(fs::metadata(plan.sibling(ROTATED_SUFFIX)).unwrap().ino(), old_ino);
        assert_eq!(fs::metadata(&plan.path).unwrap().ino(), new_ino, "the path is the new inode");
        assert_eq!(line_count(&plan.path), 15);
        assert!(!plan.sibling(STAGED_SUFFIX).exists());
        let total = fs::metadata(&plan.path).unwrap().len()
            + fs::metadata(plan.sibling(ROTATED_SUFFIX)).unwrap().len();
        assert_eq!(
            total,
            plan.bytes(),
            "the second file continues the ring where the first stopped"
        );
    }

    #[test]
    fn the_same_spec_renders_the_same_bytes() {
        let (root, scenario, spec) = tree(
            "deterministic",
            "kind: file\ntarget: app\nlines: 50\nmodel: m.yaml\n",
            "../results/tail/app.log",
        );
        let a = FilePlan::build(&root, &spec, &scenario).unwrap();
        let b = FilePlan::build(&root, &spec, &scenario).unwrap();
        assert_eq!(a.ring, b.ring);
    }

    #[test]
    fn self_check_holds_every_count_exact() {
        let (root, scenario, spec) = tree(
            "check",
            "kind: file\ntarget: app\nlines: 10\nrotate_after: 4\nmodel: m.yaml\n",
            "../results/tail/app.log",
        );
        let plan = FilePlan::build(&root, &spec, &scenario).unwrap();
        let good = TailStats { lines: 10, rotated: 1, diagnostics: BTreeMap::new() };
        self_check("t", &plan, 10, &good).unwrap();
        assert!(self_check("t", &plan, 9, &good).is_err());
        assert!(self_check("t", &plan, 11, &good).is_err());
        assert!(self_check("t", &plan, 10, &TailStats { lines: 9, ..good.clone() }).is_err());
        assert!(self_check("t", &plan, 10, &TailStats { rotated: 0, ..good.clone() }).is_err());
        let mut diagnosed = good.clone();
        diagnosed.diagnostics.insert("long_line".to_string(), 1);
        assert!(self_check("t", &plan, 10, &diagnosed).is_err());
    }

    /// Every shipped `kind: file` spec builds against its scenario, so a broken model or path
    /// fails CI rather than a VM session.
    #[test]
    fn every_shipped_file_spec_builds_against_its_scenario() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let root = crate::spool::normalize(&root);
        let mut built = 0;
        for scenario in crate::scenario::discover(&root.join("perf/scenarios")).unwrap() {
            if let crate::scenario::Workload::File(_) = scenario.workload {
                let plan =
                    FilePlan::build(&root, &scenario.load_spec_path().unwrap(), &scenario.path)
                        .unwrap_or_else(|err| panic!("{}: {err:#}", scenario.name));
                assert!(plan.ring.iter().all(|line| line.ends_with(b"\n")));
                built += 1;
            }
        }
        assert!(built >= 2, "expected the tail scenarios, found {built}");
    }
}
