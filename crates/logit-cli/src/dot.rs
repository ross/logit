//! `logit graph`: renders a config's component graph as graphviz DOT
//! (`docs/design/pipeline-graph.md`'s "`logit graph`: visualizing the resolved DAG" section).
//!
//! Deliberately renders straight off `Config`, not a resolved `Graph`: it needs only that a
//! `source` id can be written as an edge target, which is true even for a config that ultimately
//! fails validation (an undefined source becomes a bare auto-created node in the rendered
//! graph -- exactly the kind of thing this command exists to make visible). This is what lets
//! `logit graph` print *something* useful for a cyclic or otherwise-broken config, rather than
//! only ever working on configs `logit run` would already accept.

use logit_config::{Component, Config};
use logit_pipeline::graph::{role, Role};
use serde_norway::Value;

pub fn render(config: &Config) -> String {
    let mut out =
        String::from("digraph logit {\n  rankdir=LR;\n  node [fontname=\"monospace\"];\n\n");
    for (id, component) in &config.components {
        let (shape, style) = node_style(role(&component.kind));
        out.push_str(&format!("  {id:?} [shape={shape}, style=\"{style}\", label={id:?}];\n"));
    }
    out.push('\n');
    for (id, component) in &config.components {
        for source in &component.sources {
            out.push_str(&format!("  {source:?} -> {id:?};\n"));
        }
    }
    out.push_str("}\n");
    out
}

/// Renders DOT off the raw resolved config value rather than a validated [`Config`] -- the
/// fallback `logit graph` uses when `!env` substituted a placeholder for a missing variable and
/// that placeholder doesn't type-check against whatever shape its field actually wants (a
/// `Duration`, an `f64`, ...); see `crates/logit-cli/src/config.rs`'s `Loaded::Lenient` and
/// `docs/known-gaps.md`. `sources` is always a plain string list regardless of any of that, so
/// edges render exactly as [`render`] would; a component whose kind can't be individually
/// resolved into a [`Component`] (the specific one a bad placeholder broke, typically) still gets
/// a node, just without a role-derived shape/style -- rendering the graph's topology is the goal
/// here, not reproducing `render`'s exact styling for every last field.
pub fn render_lenient(value: &Value) -> String {
    let mut out =
        String::from("digraph logit {\n  rankdir=LR;\n  node [fontname=\"monospace\"];\n\n");
    let Some(components) = value.get("components").and_then(Value::as_mapping) else {
        out.push_str("}\n");
        return out;
    };

    let mut ids: Vec<&str> = components.keys().filter_map(Value::as_str).collect();
    ids.sort_unstable();

    for &id in &ids {
        let (shape, style) = components
            .get(id)
            .and_then(|component| serde_norway::from_value::<Component>(component.clone()).ok())
            .map_or(("box", "dashed"), |component| node_style(role(&component.kind)));
        out.push_str(&format!("  {id:?} [shape={shape}, style=\"{style}\", label={id:?}];\n"));
    }
    out.push('\n');
    for &id in &ids {
        for source in components.get(id).map(component_sources).unwrap_or_default() {
            out.push_str(&format!("  {source:?} -> {id:?};\n"));
        }
    }
    out.push_str("}\n");
    out
}

fn node_style(role: Role) -> (&'static str, &'static str) {
    match role {
        Role::Listener => ("box", "filled,rounded"),
        Role::Transform => ("ellipse", "filled"),
        Role::Sink => ("box", "filled,bold"),
    }
}

/// Just the `sources` list out of one component's raw value, tolerant of a component whose other
/// fields don't fully type-check -- `sources` is always plain strings, so this never fails for
/// the reason [`render_lenient`] exists to route around. Reads the `Value` tree directly rather
/// than deserializing a struct: `logit-cli` has no direct `serde` dependency of its own to derive
/// against (it goes through `logit-config`'s types, or `serde_norway`'s `Value`/`from_value`).
fn component_sources(component: &Value) -> Vec<String> {
    component
        .get("sources")
        .and_then(Value::as_sequence)
        .map(|sources| sources.iter().filter_map(Value::as_str).map(str::to_string).collect())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_config::{Component, ComponentKind};
    use std::collections::HashMap;

    #[test]
    fn renders_a_node_per_component_and_an_edge_per_source() {
        let mut components = HashMap::new();
        components.insert(
            "in".to_string(),
            Component { sources: vec![], kind: ComponentKind::StatsdIn { bind: "x".to_string() } },
        );
        components.insert(
            "out".to_string(),
            Component {
                sources: vec!["in".to_string()],
                kind: ComponentKind::InfluxDbOut {
                    url: "u".to_string(),
                    org: "o".to_string(),
                    bucket: "b".to_string(),
                    token: "T".to_string(),
                },
            },
        );
        let dot = render(&Config { components });
        assert!(dot.starts_with("digraph logit {"));
        assert!(dot.contains("\"in\""), "got: {dot}");
        assert!(dot.contains("\"out\""), "got: {dot}");
        assert!(dot.contains("\"in\" -> \"out\";"), "got: {dot}");
    }

    /// A dangling source reference (would fail validation) still renders -- graphviz auto-creates
    /// a bare node for an edge target with no explicit definition, which is exactly the point:
    /// `logit graph` should make a typo'd source visible, not refuse to render around it.
    #[test]
    fn a_dangling_source_reference_still_renders_an_edge() {
        let mut components = HashMap::new();
        components.insert(
            "out".to_string(),
            Component {
                sources: vec!["missing".to_string()],
                kind: ComponentKind::InfluxDbOut {
                    url: "u".to_string(),
                    org: "o".to_string(),
                    bucket: "b".to_string(),
                    token: "T".to_string(),
                },
            },
        );
        let dot = render(&Config { components });
        assert!(dot.contains("\"missing\" -> \"out\";"), "got: {dot}");
    }

    /// The regression case: a component whose kind can't individually resolve (here, an `!env`
    /// placeholder standing in for `interval`, which isn't a valid `Duration`) still gets a node
    /// and its edges render -- just without a role-derived style, since `role()` needs a real
    /// `ComponentKind` to match on and this one doesn't have one.
    #[test]
    fn render_lenient_still_renders_a_component_whose_kind_does_not_resolve() {
        let yaml = "components:\n  \
                     in:\n    type: statsd_in\n    bind: \"0.0.0.0:8125\"\n  \
                     windowed:\n    type: aggregate\n    sources: [in]\n    \
                     interval: \"<unset:WINDOW>\"\n";
        let value: Value = serde_norway::from_str(yaml).unwrap();
        let dot = render_lenient(&value);
        assert!(dot.starts_with("digraph logit {"));
        assert!(dot.contains("\"in\""), "got: {dot}");
        assert!(dot.contains("\"windowed\""), "got: {dot}");
        assert!(dot.contains("\"in\" -> \"windowed\";"), "got: {dot}");
        // `in` resolves fully (a real listener) and keeps its normal style; `windowed` doesn't
        // (its `interval` isn't a valid `Duration`) and falls back to the generic one.
        assert!(dot.contains("\"in\" [shape=box, style=\"filled,rounded\""), "got: {dot}");
        assert!(dot.contains("\"windowed\" [shape=box, style=\"dashed\""), "got: {dot}");
    }

    #[test]
    fn render_lenient_renders_normally_when_every_component_resolves() {
        let mut components = HashMap::new();
        components.insert(
            "in".to_string(),
            Component { sources: vec![], kind: ComponentKind::StatsdIn { bind: "x".to_string() } },
        );
        let expected = render(&Config { components });

        let yaml = "components:\n  in:\n    type: statsd_in\n    bind: \"x\"\n";
        let value: Value = serde_norway::from_str(yaml).unwrap();
        assert_eq!(render_lenient(&value), expected);
    }
}
