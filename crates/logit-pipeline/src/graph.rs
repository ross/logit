//! Pure resolution and validation of a [`Config`] into a [`Graph`]. No channels, no threads, no
//! tokio -- mirrors how `apply_transforms` in the pre-graph `logit-cli::pipeline` was kept pure
//! specifically for unit-testability. `logit run`, `logit validate`, and `logit graph` are all
//! just different things layered on top of this one function's output.
//!
//! Validation rules, in order (`docs/design/pipeline-graph.md`):
//! 1. At least one component.
//! 2. Every `sources` id resolves to a defined component.
//! 3. No self-reference.
//! 4. No duplicate source within one component's `sources` list.
//! 5. No cycles.
//! 6. Arity per kind (listener: no sources; transform/sink: at least one).
//! 7. Every non-sink component has at least one consumer.
//! 8. Kind is implemented.
//! 9. No zero-length `interval` on a kind that has one.
//!
//! Sink reachability from a listener needs no separate rule -- it's implied by 2 + 5 + 7: every
//! acyclic chain of sourced components terminates somewhere, and every non-terminal component in
//! it is required (by 7) to have a consumer, so the chain can only terminate at a sink.

use logit_config::{Component, ComponentKind, Config};
use std::collections::{HashMap, VecDeque};
use std::time::Duration;

/// A component's arity class, fixed by its `kind` (`docs/design/pipeline-graph.md`'s arity
/// table) -- never derived from topology, so a typo'd source reference can't silently reclassify
/// a component instead of producing a clear error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Listener,
    Transform,
    Sink,
}

/// The arity class a kind belongs to. Public so `logit graph` (`logit-cli`) can style nodes by
/// role directly off a `Config`, without needing a fully-resolved `Graph` -- useful precisely
/// because it lets `logit graph` render *something* even for a config that fails validation
/// (`docs/design/pipeline-graph.md`'s "`logit graph`" section).
pub fn role(kind: &ComponentKind) -> Role {
    use ComponentKind::*;
    match kind {
        StatsdIn { .. } | SyslogIn { .. } | OtlpIn { .. } | FileTail { .. } | LogitIn { .. } => {
            Role::Listener
        }
        Lua { .. }
        | LuaFile { .. }
        | Aggregate { .. }
        | Json { .. }
        | Logfmt
        | Kv
        | Regex { .. }
        | Csv
        | Rename { .. }
        | Remove { .. }
        | Filter { .. }
        | Sample { .. }
        | Throttle { .. }
        | Dedup { .. } => Role::Transform,
        InfluxDbOut { .. } | OtlpOut { .. } | LogitOut { .. } => Role::Sink,
    }
}

/// The single source of truth for which `ComponentKind`s the runtime can actually build --
/// mirrors the pre-graph `require_implemented_input`/`require_implemented_output`/
/// `require_implemented_transform` trio, now unified over one enum.
fn is_implemented(kind: &ComponentKind) -> bool {
    matches!(
        kind,
        ComponentKind::StatsdIn { .. }
            | ComponentKind::SyslogIn { .. }
            | ComponentKind::Lua { .. }
            | ComponentKind::LuaFile { .. }
            | ComponentKind::Aggregate { .. }
            | ComponentKind::Json { .. }
            | ComponentKind::InfluxDbOut { .. }
    )
}

/// `Some(interval)` for a kind with an `interval` field, `Aggregate`'s always populated,
/// `Lua`/`LuaFile`'s only when set. `None` either means no `interval` field on this kind, or a
/// `Lua`/`LuaFile` component that left it unset -- both are "never flushes", so rule 8 treats
/// them the same: nothing to reject.
fn interval(kind: &ComponentKind) -> Option<Duration> {
    match kind {
        ComponentKind::Lua { interval, .. } | ComponentKind::LuaFile { interval, .. } => *interval,
        ComponentKind::Aggregate { interval } => Some(*interval),
        _ => None,
    }
}

pub struct ResolvedComponent {
    pub sources: Vec<String>,
    pub consumers: Vec<String>,
    pub kind: ComponentKind,
}

impl ResolvedComponent {
    pub fn role(&self) -> Role {
        role(&self.kind)
    }
}

pub struct Graph {
    pub components: HashMap<String, ResolvedComponent>,
    /// Listener-first, sink-last ("produce before consume") order. Used by `logit graph` for
    /// deterministic output; the node runtime doesn't need it -- every component's inbox channel
    /// is created up front, independent of build order, so there's nothing dependency-ordering
    /// actually has to protect there.
    pub topological_order: Vec<String>,
}

pub fn resolve(config: Config) -> anyhow::Result<Graph> {
    let Config { components } = config;

    if components.is_empty() {
        anyhow::bail!("config defines no components");
    }

    // Rules 2 + 3 + 4: every source resolves, no self-reference, no duplicate source within one
    // component's `sources` list. The last of these matters beyond tidiness: a duplicate would
    // otherwise push the same consumer id into `consumers` twice below, so that source's `Fanout`
    // would hold two live `Sender` clones pointing at the same inbox and deliver every batch to
    // it twice -- a repeated source id would silently double telemetry (and, through an
    // `aggregate` component, double every aggregated count) rather than being rejected as the
    // config typo it almost certainly is.
    for (id, component) in &components {
        let mut seen = std::collections::HashSet::with_capacity(component.sources.len());
        for source in &component.sources {
            if source == id {
                anyhow::bail!("component '{id}' lists itself as a source");
            }
            if !components.contains_key(source) {
                anyhow::bail!("component '{id}' references unknown source '{source}'");
            }
            if !seen.insert(source) {
                anyhow::bail!("component '{id}' lists source '{source}' more than once");
            }
        }
    }

    // Invert `sources` into each component's outbound consumer list.
    let mut consumers: HashMap<String, Vec<String>> =
        components.keys().map(|id| (id.clone(), Vec::new())).collect();
    for (id, component) in &components {
        for source in &component.sources {
            consumers.get_mut(source).expect("validated above").push(id.clone());
        }
    }

    // Rule 5: cycle detection, via Kahn's algorithm -- its natural byproduct is also the
    // listener-first topological order `Graph::topological_order` publishes.
    let topological_order = topological_order(&components)?;

    // Rule 6: arity per kind.
    for (id, component) in &components {
        match role(&component.kind) {
            Role::Listener if !component.sources.is_empty() => {
                anyhow::bail!("component '{id}' is a listener and cannot declare sources");
            }
            Role::Transform if component.sources.is_empty() => {
                anyhow::bail!("component '{id}' is a transform and requires at least one source");
            }
            Role::Sink => {
                if component.sources.is_empty() {
                    anyhow::bail!("component '{id}' is a sink and requires at least one source");
                }
                if !consumers.get(id).is_some_and(Vec::is_empty) {
                    anyhow::bail!(
                        "component '{id}' is a sink and cannot be listed as a source of another \
                         component"
                    );
                }
            }
            _ => {}
        }
    }

    // Rule 7: every non-sink component needs at least one consumer.
    for (id, component) in &components {
        if role(&component.kind) != Role::Sink && consumers.get(id).is_none_or(Vec::is_empty) {
            anyhow::bail!("component '{id}' has no consumers -- nothing reads what it produces");
        }
    }

    // Rule 8: kind implemented.
    for (id, component) in &components {
        if !is_implemented(&component.kind) {
            anyhow::bail!("component '{id}': kind {:?} is not implemented yet", component.kind);
        }
    }

    // Rule 9: no zero-length flush interval.
    for (id, component) in &components {
        if interval(&component.kind) == Some(Duration::ZERO) {
            anyhow::bail!(
                "component '{id}': a flush interval of 0s would flush continuously -- use a \
                 positive duration"
            );
        }
    }

    let mut resolved = HashMap::with_capacity(components.len());
    for (id, component) in components {
        let Component { sources, kind } = component;
        let node_consumers = consumers.remove(&id).unwrap_or_default();
        resolved.insert(id, ResolvedComponent { sources, consumers: node_consumers, kind });
    }

    Ok(Graph { components: resolved, topological_order })
}

/// Kahn's algorithm over the `sources` edges (a source's data flows *into* the component that
/// names it, so indegree is `sources.len()`). Returns a listener-first order, or a cycle error
/// naming every component still unresolved once no more zero-indegree nodes remain -- exactly
/// the ones on or feeding into the cycle.
fn topological_order(components: &HashMap<String, Component>) -> anyhow::Result<Vec<String>> {
    let mut indegree: HashMap<&str, usize> =
        components.iter().map(|(id, c)| (id.as_str(), c.sources.len())).collect();
    let mut outgoing: HashMap<&str, Vec<&str>> =
        components.keys().map(|id| (id.as_str(), Vec::new())).collect();
    for (id, c) in components {
        for source in &c.sources {
            if let Some(out) = outgoing.get_mut(source.as_str()) {
                out.push(id.as_str());
            }
        }
    }

    let mut ready: Vec<&str> =
        indegree.iter().filter(|(_, &deg)| deg == 0).map(|(&id, _)| id).collect();
    ready.sort_unstable();
    let mut queue: VecDeque<&str> = ready.into();

    let mut order = Vec::with_capacity(components.len());
    while let Some(id) = queue.pop_front() {
        order.push(id.to_string());
        let mut newly_ready: Vec<&str> = Vec::new();
        for &next in &outgoing[id] {
            let deg = indegree.get_mut(next).expect("every id is in indegree");
            *deg -= 1;
            if *deg == 0 {
                newly_ready.push(next);
            }
        }
        newly_ready.sort_unstable();
        queue.extend(newly_ready);
    }

    if order.len() != components.len() {
        let mut stuck: Vec<&str> =
            indegree.iter().filter(|(_, &deg)| deg > 0).map(|(&id, _)| id).collect();
        stuck.sort_unstable();
        anyhow::bail!("component graph has a cycle involving: {}", stuck.join(", "));
    }
    Ok(order)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as Map;

    fn cfg(components: Vec<(&str, Vec<&str>, ComponentKind)>) -> Config {
        let mut map = Map::new();
        for (id, sources, kind) in components {
            map.insert(
                id.to_string(),
                Component { sources: sources.into_iter().map(String::from).collect(), kind },
            );
        }
        Config { components: map }
    }

    fn listener() -> ComponentKind {
        ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() }
    }

    fn lua() -> ComponentKind {
        ComponentKind::Lua { script: "".to_string(), interval: None }
    }

    fn json() -> ComponentKind {
        ComponentKind::Json { skip_to_brace: false }
    }

    fn sink() -> ComponentKind {
        ComponentKind::InfluxDbOut {
            url: "http://localhost:8086".to_string(),
            org: "org".to_string(),
            bucket: "bucket".to_string(),
            token: "TOKEN".to_string(),
        }
    }

    /// `Graph` isn't `Debug` (it embeds `ComponentKind`, which isn't either), so
    /// `Result::expect_err` -- which needs `Debug` on the `Ok` side to format its panic message --
    /// doesn't work here. Same reason `logit-cli::pipeline` has its own `expect_err` helper.
    fn expect_err(config: Config) -> String {
        match resolve(config) {
            Ok(_) => panic!("expected resolution to fail"),
            Err(err) => err.to_string(),
        }
    }

    #[test]
    fn empty_config_is_rejected() {
        let err = expect_err(cfg(vec![]));
        assert!(err.contains("no components"), "got: {err}");
    }

    #[test]
    fn unknown_source_is_rejected() {
        let err = expect_err(cfg(vec![("out", vec!["missing"], sink())]));
        assert!(err.contains("unknown source 'missing'"), "got: {err}");
    }

    #[test]
    fn self_reference_is_rejected() {
        let err = expect_err(cfg(vec![("a", vec!["a"], lua())]));
        assert!(err.contains("lists itself as a source"), "got: {err}");
    }

    /// A repeated source id would otherwise push the same consumer into `consumers` twice, giving
    /// that source's `Fanout` two live `Sender` clones pointing at the same inbox -- silently
    /// doubling every batch delivered, not a cosmetic issue.
    #[test]
    fn duplicate_source_within_one_component_is_rejected() {
        let err =
            expect_err(cfg(vec![("in", vec![], listener()), ("out", vec!["in", "in"], sink())]));
        assert!(err.contains("lists source 'in' more than once"), "got: {err}");
    }

    #[test]
    fn two_node_cycle_is_rejected() {
        let err = expect_err(cfg(vec![("a", vec!["b"], lua()), ("b", vec!["a"], lua())]));
        assert!(err.contains("cycle"), "got: {err}");
    }

    #[test]
    fn longer_cycle_is_rejected() {
        let err = expect_err(cfg(vec![
            ("a", vec!["c"], lua()),
            ("b", vec!["a"], lua()),
            ("c", vec!["b"], lua()),
        ]));
        assert!(err.contains("cycle"), "got: {err}");
    }

    #[test]
    fn listener_with_sources_is_rejected() {
        let err =
            expect_err(cfg(vec![("in", vec!["other"], listener()), ("other", vec![], listener())]));
        assert!(err.contains("listener") && err.contains("cannot declare sources"), "got: {err}");
    }

    #[test]
    fn sink_named_as_another_components_source_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], sink()),
            ("other", vec!["out"], lua()),
        ]));
        assert!(err.contains("is a sink and cannot be listed as a source"), "got: {err}");
    }

    #[test]
    fn transform_with_no_consumers_is_rejected() {
        let err = expect_err(cfg(vec![("in", vec![], listener()), ("orphan", vec!["in"], lua())]));
        assert!(err.contains("no consumers"), "got: {err}");
    }

    #[test]
    fn listener_with_no_consumers_is_rejected() {
        let err = expect_err(cfg(vec![("in", vec![], listener())]));
        assert!(err.contains("no consumers"), "got: {err}");
    }

    #[test]
    fn unimplemented_kind_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], ComponentKind::OtlpIn { bind: "127.0.0.1:0".to_string() }),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("not implemented yet"), "got: {err}");
    }

    #[test]
    fn a_json_component_resolves_as_a_transform() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            ("parse", vec!["in"], json()),
            ("out", vec!["parse"], sink()),
        ]))
        .expect("should resolve");
        assert_eq!(graph.components["parse"].role(), Role::Transform);
    }

    #[test]
    fn zero_interval_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("agg", vec!["in"], ComponentKind::Aggregate { interval: Duration::ZERO }),
            ("out", vec!["agg"], sink()),
        ]));
        assert!(err.contains("0s"), "got: {err}");
    }

    #[test]
    fn a_well_formed_chain_resolves() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            ("enrich", vec!["in"], lua()),
            ("out", vec!["enrich"], sink()),
        ]))
        .expect("should resolve");
        assert_eq!(graph.topological_order, vec!["in", "enrich", "out"]);
        assert_eq!(graph.components["in"].role(), Role::Listener);
        assert_eq!(graph.components["enrich"].role(), Role::Transform);
        assert_eq!(graph.components["out"].role(), Role::Sink);
        assert_eq!(graph.components["in"].consumers, vec!["enrich"]);
    }

    /// The headline regression test: today's `validate_semantics` rejects an input/output
    /// referenced by more than one pipeline outright. A sink with two independent upstream
    /// branches must now be *accepted*.
    #[test]
    fn a_sink_shared_by_two_branches_is_accepted() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            ("branch_a", vec!["in"], lua()),
            ("branch_b", vec!["in"], lua()),
            ("out", vec!["branch_a", "branch_b"], sink()),
        ]))
        .expect("should resolve");
        assert_eq!(graph.components["in"].consumers.len(), 2);
        assert_eq!(graph.components["out"].sources.len(), 2);
    }

    #[test]
    fn diamond_fan_out_fan_in_resolves() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            ("left", vec!["in"], lua()),
            ("right", vec!["in"], lua()),
            ("out", vec!["left", "right"], sink()),
        ]))
        .expect("should resolve");
        let order = graph.topological_order;
        assert_eq!(order[0], "in");
        assert_eq!(order[3], "out");
        assert!(order[1..3].contains(&"left".to_string()));
        assert!(order[1..3].contains(&"right".to_string()));
    }
}
