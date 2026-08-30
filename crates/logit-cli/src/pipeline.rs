//! `logit run`: resolves a config's component graph (`docs/design/pipeline-graph.md`,
//! `docs/adr/0009-component-graph-configuration.md`) into a runnable [`NodeSpec`] per component
//! and hands it to `logit_pipeline::run`. See `docs/OVERVIEW.md` for the shape (`logit` as
//! sidecar, host agent, or central aggregator is all just this, differing only by config).
//!
//! This module is now just the *registry* -- graph resolution/validation
//! (`logit_pipeline::graph`) and the node runtime (`logit_pipeline::run`) both live in
//! `logit-pipeline`; what's left here is turning one component's `ComponentKind` into the boxed
//! implementation the runtime actually runs, which is exactly the "kind → impl" mapping this
//! project has always kept in one place (previously `build_input`/`build_output`).

use anyhow::Context;
use logit_config::Config;
use logit_inputs::statsd::StatsdInput;
use logit_outputs::influxdb::InfluxDbOutput;
use logit_pipeline::graph::{self, ResolvedComponent};
use logit_pipeline::NodeSpec;
use logit_transforms::{Aggregator, JsonParser};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Loads `path`, resolves its component graph, and runs it until the first component fails.
///
/// Every listener/sink task loops forever in normal operation (a listener keeps listening, a
/// sink keeps draining its inbox), so in the happy path this simply never returns -- matching
/// "this is a service," not "this is a batch job." There's no graceful-shutdown handling yet (no
/// installed Ctrl-C handler, no drain of in-flight events on exit) -- Ctrl-C falls through to the
/// OS default (immediate termination), same as any other long-running process with no handler.
pub async fn run_pipelines(path: PathBuf) -> anyhow::Result<()> {
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("reading config file {}", path.display()))?;
    let config: Config = serde_norway::from_str(&raw)
        .with_context(|| format!("parsing config file {}", path.display()))?;
    let base_dir = path.parent().map(Path::to_path_buf).unwrap_or_default();
    run_config(config, base_dir).await
}

/// The rest of [`run_pipelines`], split out so it's callable with an in-memory `Config` directly
/// (no real file on disk needed) -- what most tests below exercise.
async fn run_config(config: Config, base_dir: PathBuf) -> anyhow::Result<()> {
    let graph = graph::resolve(config)?;

    // Sorted, not raw `HashMap` iteration order: a startup failure (a missing lua_file, an unset
    // token_env) should be reproducible across runs, not depend on hash-seed-driven iteration
    // order -- two independently-broken components should always report the same one first.
    let mut ids: Vec<&String> = graph.components.keys().collect();
    ids.sort();

    let mut specs: HashMap<String, NodeSpec> = HashMap::with_capacity(graph.components.len());
    for id in ids {
        let component = &graph.components[id];
        let spec = build_spec(component, &base_dir).with_context(|| format!("component '{id}'"))?;
        specs.insert(id.clone(), spec);
    }

    logit_pipeline::run(graph, specs).await
}

/// The same checks `logit run` needs before spawning anything, exposed for `logit validate` to
/// share -- so the two commands can never again disagree about whether a config is acceptable.
/// Takes `Config` by value (graph resolution consumes it) rather than by reference: `logit
/// validate` has no further use for the config afterward either.
pub fn validate_semantics(config: Config) -> anyhow::Result<()> {
    graph::resolve(config)?;
    Ok(())
}

/// Turns one resolved component's kind into the boxed implementation the node runtime actually
/// runs. The single source of truth for which `ComponentKind`s this binary can build --
/// `graph::resolve` already rejected every kind `is_implemented` doesn't recognize (rule 7), so
/// the fallback arm below is unreachable in practice, not a silent gap.
fn build_spec(component: &ResolvedComponent, base_dir: &Path) -> anyhow::Result<NodeSpec> {
    use logit_config::ComponentKind::*;
    Ok(match &component.kind {
        StatsdIn { bind } => NodeSpec::Input(Box::new(StatsdInput { bind: bind.clone() })),

        Lua { script, interval } => NodeSpec::Lua { script: script.clone(), interval: *interval },
        LuaFile { lua_file, interval } => {
            let script_path = base_dir.join(lua_file);
            let script = std::fs::read_to_string(&script_path)
                .with_context(|| format!("reading lua_file {}", script_path.display()))?;
            NodeSpec::Lua { script, interval: *interval }
        }
        Aggregate { interval } => NodeSpec::Transform(Box::new(Aggregator::new(*interval))),
        Json { skip_to_brace } => NodeSpec::Transform(Box::new(JsonParser::new(*skip_to_brace))),

        InfluxDbOut { url, org, bucket, token_env } => {
            let token = std::env::var(token_env).with_context(|| {
                format!(
                    "environment variable '{token_env}' (referenced by output config) is not set"
                )
            })?;
            NodeSpec::Output(Box::new(InfluxDbOutput::new(
                url.clone(),
                org.clone(),
                bucket.clone(),
                token,
            )))
        }

        other => unreachable!("graph::resolve already rejected any unimplemented kind: {other:?}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_config::{Component, ComponentKind};
    use std::collections::HashMap as Map;
    use std::time::Duration;

    fn statsd_in() -> Component {
        Component {
            sources: vec![],
            kind: ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() },
        }
    }

    fn influxdb_out(sources: Vec<&str>) -> Component {
        Component {
            sources: sources.into_iter().map(String::from).collect(),
            kind: ComponentKind::InfluxDbOut {
                url: "http://localhost:8086".to_string(),
                org: "org".to_string(),
                bucket: "bucket".to_string(),
                token_env: "LOGIT_TEST_DEFINITELY_UNSET_TOKEN_VAR".to_string(),
            },
        }
    }

    fn config(components: Vec<(&str, Component)>) -> Config {
        let mut map = Map::new();
        for (id, component) in components {
            map.insert(id.to_string(), component);
        }
        Config { components: map }
    }

    #[test]
    fn validate_semantics_rejects_an_empty_config() {
        let err = validate_semantics(config(vec![])).expect_err("expected an error");
        assert!(err.to_string().contains("no components"), "got: {err}");
    }

    #[test]
    fn validate_semantics_rejects_a_listener_with_no_consumers() {
        let err =
            validate_semantics(config(vec![("in", statsd_in())])).expect_err("expected an error");
        assert!(err.to_string().contains("no consumers"), "got: {err}");
    }

    #[test]
    fn validate_semantics_accepts_a_well_formed_config() {
        let cfg = config(vec![("in", statsd_in()), ("out", influxdb_out(vec!["in"]))]);
        assert!(validate_semantics(cfg).is_ok());
    }

    /// The headline regression test at the CLI layer, mirroring `logit-pipeline::graph`'s: a sink
    /// shared by two upstream branches is accepted now, where the pre-graph `validate_semantics`
    /// rejected any output referenced by more than one pipeline outright.
    #[test]
    fn validate_semantics_accepts_a_sink_shared_by_two_branches() {
        let cfg = config(vec![
            ("in", statsd_in()),
            (
                "branch_a",
                Component {
                    sources: vec!["in".to_string()],
                    kind: ComponentKind::Lua { script: "".to_string(), interval: None },
                },
            ),
            (
                "branch_b",
                Component {
                    sources: vec!["in".to_string()],
                    kind: ComponentKind::Lua { script: "".to_string(), interval: None },
                },
            ),
            ("out", influxdb_out(vec!["branch_a", "branch_b"])),
        ]);
        assert!(validate_semantics(cfg).is_ok());
    }

    #[tokio::test]
    async fn run_config_reports_missing_token_env_clearly() {
        let cfg = config(vec![("in", statsd_in()), ("out", influxdb_out(vec!["in"]))]);
        let err = run_config(cfg, PathBuf::new()).await.expect_err("expected an error");
        // `{err}`/`to_string()` shows only the outermost `.with_context` message ("component
        // 'out'"); `{err:?}` is anyhow's chain-printing Debug, which includes every cause.
        assert!(
            format!("{err:?}").contains("LOGIT_TEST_DEFINITELY_UNSET_TOKEN_VAR"),
            "got: {err:?}"
        );
    }

    #[tokio::test]
    async fn run_config_reports_a_missing_lua_file_clearly() {
        let cfg = config(vec![
            ("in", statsd_in()),
            (
                "enrich",
                Component {
                    sources: vec!["in".to_string()],
                    kind: ComponentKind::LuaFile {
                        lua_file: "does-not-exist.lua".to_string(),
                        interval: None,
                    },
                },
            ),
            ("out", influxdb_out(vec!["enrich"])),
        ]);
        let err = run_config(cfg, PathBuf::new()).await.expect_err("expected an error");
        assert!(format!("{err:?}").contains("does-not-exist.lua"), "got: {err:?}");
    }

    #[test]
    fn build_spec_builds_an_aggregate_transform() {
        let component = ResolvedComponent {
            sources: vec!["in".to_string()],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::Aggregate { interval: Duration::from_secs(10) },
        };
        assert!(matches!(build_spec(&component, Path::new("")).unwrap(), NodeSpec::Transform(_)));
    }

    #[test]
    fn build_spec_builds_a_json_transform() {
        let component = ResolvedComponent {
            sources: vec!["in".to_string()],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::Json { skip_to_brace: true },
        };
        assert!(matches!(build_spec(&component, Path::new("")).unwrap(), NodeSpec::Transform(_)));
    }
}
