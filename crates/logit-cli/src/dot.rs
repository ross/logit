//! `logit graph`: renders a config's component graph as graphviz DOT
//! (`docs/design/pipeline-graph.md`'s "`logit graph`: visualizing the resolved DAG" section).
//!
//! Deliberately renders straight off `Config`, not a resolved `Graph`: it needs only that a
//! `source` id can be written as an edge target, which is true even for a config that ultimately
//! fails validation (an undefined source becomes a bare auto-created node in the rendered
//! graph -- exactly the kind of thing this command exists to make visible). This is what lets
//! `logit graph` print *something* useful for a cyclic or otherwise-broken config, rather than
//! only ever working on configs `logit run` would already accept. It still needs a fully-typed
//! `Config`, though -- every `!env` reference (including one on a field this command never reads,
//! like a token) must resolve first, same as `run`/`validate` (`docs/adr/0011-env-yaml-tag.md`).

use logit_config::Config;
use logit_pipeline::graph::{role, Role};

pub fn render(config: &Config) -> String {
    let mut out =
        String::from("digraph logit {\n  rankdir=LR;\n  node [fontname=\"monospace\"];\n\n");
    for (id, component) in &config.components {
        let (shape, style) = match role(&component.kind) {
            Role::Listener => ("box", "filled,rounded"),
            Role::Transform => ("ellipse", "filled"),
            Role::Sink => ("box", "filled,bold"),
        };
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
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec![],
                kind: ComponentKind::StatsdIn { bind: "x".to_string() },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
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
                buffer: logit_config::BufferConfig::default(),
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
}
