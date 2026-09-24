//! Reads a running scenario's self-telemetry by appending a temporary `internal → file_out
//! format: native` leg to a copy of it and decoding the dump afterwards.
//!
//! Two callers: `crate::attribute` for its per-node breakdown (docs/adr/load-test-harness.md's
//! "Per-node attribution"), and `crate::run` for a `Driven` scenario's denominator, events
//! delivered to the sink, which exists only as the child's own `logit.component.events.received`
//! (docs/adr/udp-intake-batching-and-socket-visibility.md).
//!
//! The leg appended to a **copy** of the scenario (the shipped file is never touched):
//!
//! ```yaml
//!   __perf_internal: { type: internal, interval: 1s, span_sample_rate: 0.0, logs: off }
//!   __perf_dump: { type: file_out, sources: [__perf_internal], path: <tmp>/<purpose>.native,
//!                  format: native, rotate: { max_bytes: "1024GiB" } }
//! ```
//!
//! The dump is decoded with the codec that wrote it: [`logit_proto::frame::read_frame`] in a loop,
//! each payload through [`logit_proto::native::decode_batch`], the inverse of what `format:
//! native` writes per batch (`NativeEncoder::encode`). `native` is the only stream format that
//! reads back exactly; `human` is a render for people.
//!
//! **The append is textual, never a parse-and-reserialize.** Round-tripping through
//! `logit_config::Config` would resolve `!env`, write every default into the file, and couple
//! this crate to `logit-config`. The YAML is parsed as a bare [`serde_norway::Value`] only to
//! check it: graph rule 13 allows one `internal` per config, so a scenario that has one is
//! refused rather than rewritten into a config the binary would reject.
//!
//! **The rewritten scenario is written next to the original**, as
//! `perf/scenarios/.<name>.<purpose>.<pid>.yaml`, and removed on every exit path; see
//! [`rewritten_config_path`].
//!
//! **A graph with an `internal` never self-exits** (the drain ticker runs until shutdown), so a
//! scenario carrying this leg always takes the settle-then-SIGTERM path. That SIGTERM also makes
//! the numbers whole: `InternalInput::run_until_shutdown` drains once more on the way out
//! (`docs/design/internal-telemetry.md`, "Shutdown drains once more"), so the last partial
//! interval reaches the dump.

use crate::scenario::Scenario;
use anyhow::{bail, Context};
use bytes::Bytes;
use logit_core::Event;
use logit_proto::frame::read_frame;
use logit_proto::native::{decode_batch, CODEC_NATIVE_V1};
use logit_proto::CodecError;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

/// The appended components' ids, prefixed so they can't collide with a scenario's ids;
/// `attribute`'s verdict and count check filter on [`HARNESS_PREFIX`].
pub const INTERNAL_ID: &str = "__perf_internal";
pub const DUMP_ID: &str = "__perf_dump";
pub const HARNESS_PREFIX: &str = "__perf_";

/// `file_out` needs a rotation trigger (graph rule 29), but rotating mid-dump would split the
/// data across two files, so this sits far above any dump (kilobytes per drain).
const ROTATE_MAX_BYTES: &str = "1024GiB";

/// A path removed when this guard drops, so every early return or panic removes the rewritten
/// scenario from `perf/scenarios/`, where a leftover would do the most harm.
pub struct RemoveOnDrop(pub PathBuf);

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

/// Where the rewritten scenario is written: **next to the original**, not in the temp directory
/// with the dump.
///
/// Every relative path in a config resolves against the config file's directory
/// (`logit_cli::pipeline`'s `base_dir`): `lua`'s `script_file`, a sink's `buffer.disk.path`, a
/// file target. Moved to `/tmp`, `perf/scenarios/buffered.yaml`'s `../results/spool` would spool
/// to `/results/spool`, and the scenario measured wouldn't be the one that ships.
///
/// Dot-prefixed so `script/validate`'s glob and `scenario::discover` skip it, pid-suffixed so
/// concurrent runs can't collide. `purpose` (`attribute`, `run`) says which command left a
/// leftover.
pub fn rewritten_config_path(scenario: &Scenario, purpose: &str) -> anyhow::Result<PathBuf> {
    let dir = scenario
        .path
        .parent()
        .with_context(|| format!("{} has no parent directory", scenario.path.display()))?;
    Ok(dir.join(format!(".{}.{purpose}.{}.yaml", scenario.name, std::process::id())))
}

/// Where [`make_workdir`] puts a purpose's scratch directory, without creating it.
pub fn workdir_path(purpose: &str) -> PathBuf {
    std::env::temp_dir().join(format!("logit-perf-{purpose}-{}", std::process::id()))
}

/// Creates a scratch directory for native dumps, named by purpose and pid so concurrent runs
/// can't share one. The rewritten scenario stays beside the original ([`rewritten_config_path`]).
pub fn make_workdir(purpose: &str) -> anyhow::Result<PathBuf> {
    let dir = workdir_path(purpose);
    fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    Ok(dir)
}

/// Removes a workdir left by [`make_workdir`] **only if it is empty**.
///
/// Non-recursive: a successful repeat removes its dump and a failed one keeps it, so only an
/// empty directory means nothing failed. `remove_dir_all` would destroy that evidence. Silent
/// either way.
pub fn remove_workdir_if_empty(purpose: &str) {
    let _ = fs::remove_dir(workdir_path(purpose));
}

/// Runs the binary's own `logit validate` over the rewritten file before spawning it, so a
/// malformed append fails at once with `validate`'s component-and-rule message rather than as a
/// generic startup failure.
pub fn validate(logit_bin: &Path, config: &Path) -> anyhow::Result<()> {
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

/// Appends the `internal` + `file_out` dump leg to `yaml`, textually.
///
/// Refuses a scenario the append can't make valid: one that already has an `internal` (graph
/// rule 13; two would split one process-wide registry's drains), uses a reserved id, or has a
/// top-level key besides `components:`. That last check makes appending at end of file sound:
/// the new entries are indented as `components:` members.
pub fn rewrite_scenario(
    yaml: &str,
    interval: Duration,
    dump_path: &Path,
) -> anyhow::Result<String> {
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
        "\n  # Appended by `logit-perf` (crates/logit-perf/src/telemetry_leg.rs) -- a \
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

    // The append is two-space-indented, which fits every shipped scenario. Another layout either
    // fails to parse or nests the new keys elsewhere; re-checking turns both into an error that
    // names the harness, not a parser position or a `logit validate` complaint.
    check_append_landed(&out).context(
        "this rewrite indents its two appended components by two spaces, so a scenario laid out \
         differently needs the harness taught about it \
         (crates/logit-perf/src/telemetry_leg.rs's `rewrite_scenario`)",
    )?;
    Ok(out)
}

/// Re-parses a rewritten scenario and confirms both appended components landed under
/// `components:`; a parse error and a mis-nested key are the same failure.
fn check_append_landed(rewritten: &str) -> anyhow::Result<()> {
    let value: serde_norway::Value = serde_norway::from_str(rewritten)
        .context("the rewritten scenario is not valid YAML any more")?;
    let components = value
        .get("components")
        .and_then(serde_norway::Value::as_mapping)
        .context("the rewritten scenario has no top-level `components` mapping")?;
    for id in [INTERNAL_ID, DUMP_ID] {
        if !components.contains_key(serde_norway::Value::from(id)) {
            bail!("appending `{id}` did not land under `components:`");
        }
    }
    Ok(())
}

/// A duration literal for the appended `internal`'s `interval:`.
///
/// Whole seconds render as seconds, anything else as whole milliseconds, so a sub-millisecond
/// interval rounds down (`attribute` rejects one up front).
pub fn format_interval(interval: Duration) -> String {
    if interval.subsec_nanos() == 0 {
        format!("{}s", interval.as_secs())
    } else {
        format!("{}ms", interval.as_millis())
    }
}

/// A YAML double-quoted scalar, so a temp path containing `:` or a leading `#` can't be misread
/// as structure. Escapes only `"` and `\`; a control character passes through unescaped.
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

/// Reads the whole dump and decodes every frame in it.
///
/// A `format: native` file is a concatenation of independently decodable frames
/// (`logit_proto::native`'s module doc), decoded as `NativeDecoder::decode_into` does. A torn
/// final frame only warns, since SIGTERM can interrupt a write mid-frame; a bad frame anywhere
/// else fails.
pub fn decode_dump(path: &Path, quiet: bool) -> anyhow::Result<Vec<Event>> {
    let raw =
        fs::read(path).with_context(|| format!("reading the telemetry dump {}", path.display()))?;
    if raw.is_empty() {
        bail!(
            "the telemetry dump ({}) is empty -- no drain ever reached it. Is --interval longer \
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
    if !quiet {
        println!("   decoded {frames} native frames, {} points", events.len());
    }
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn rewrite_works_on_a_driven_scenario_with_no_generator() {
        let yaml = "components:\n  statsd:\n    type: statsd_in\n    bind: 127.0.0.1:18125\n  out:\n    type: null_out\n    sources: [statsd]\n";
        let out = rewrite_scenario(yaml, Duration::from_secs(1), &dump_path()).unwrap();
        let value: serde_norway::Value = serde_norway::from_str(&out).unwrap();
        let components = value.get("components").and_then(serde_norway::Value::as_mapping).unwrap();
        assert_eq!(components.len(), 4);
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
    fn rewrite_refuses_a_layout_its_two_space_indent_does_not_fit() {
        // Four-space indentation is valid YAML the harness's own append doesn't match -- appending
        // two-space-indented keys under it isn't even parseable. Reported as the harness's own
        // limitation, with the file and function to fix, rather than as a bare parser position.
        let yaml = "components:\n    gen:\n        type: generate_in\n        count: 100\n    out:\n        type: null_out\n        sources: [gen]\n";
        let err = rewrite_scenario(yaml, Duration::from_secs(1), &dump_path())
            .expect_err("a four-space-indented scenario doesn't fit this rewrite");
        let err = format!("{err:#}");
        assert!(err.contains("appended components by two spaces"), "{err}");
        assert!(err.contains("not valid YAML any more"), "{err}");
    }

    #[test]
    fn check_append_landed_rejects_a_rewrite_that_nested_the_new_keys() {
        // The other shape of the same problem: parseable, but the two appended components ended
        // up inside another component instead of beside it.
        let nested = "components:\n  gen:\n    type: generate_in\n    count: 1\n    __perf_internal: { type: internal, interval: 1s }\n    __perf_dump: { type: null_out }\n";
        let err = check_append_landed(nested).expect_err("both keys are nested under `gen`");
        assert!(
            format!("{err:#}").contains("`__perf_internal` did not land under `components:`"),
            "{err:#}"
        );
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
}
