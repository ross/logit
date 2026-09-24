//! `logit graph`: renders a config's component graph as graphviz DOT
//! (`docs/design/pipeline-graph.md`'s "`logit graph`: visualizing the resolved DAG" section).
//!
//! Renders off `Config`, not a resolved `Graph`, so a cyclic or otherwise invalid config still
//! renders: an undefined source becomes a bare node graphviz auto-creates, which is what this
//! command exists to show. It still needs a typed `Config`, so every `!env` reference must
//! resolve first, even on a field this never reads (`docs/adr/env-yaml-tag.md`).
//!
//! Conventions: a listener is a rounded box, a transform an ellipse, a sink a bold box, and a
//! target a dashed box. A `sources` edge is solid; a router -> target edge is dashed.

use logit_config::Config;
use logit_pipeline::graph::{self, role, Role};

pub fn render(config: &Config) -> String {
    let mut out =
        String::from("digraph logit {\n  rankdir=LR;\n  node [fontname=\"monospace\"];\n\n");
    for (id, component) in &config.components {
        let (shape, style) = match role(&component.kind) {
            Role::Listener => ("box", "filled,rounded"),
            Role::Transform => ("ellipse", "filled"),
            Role::Sink => ("box", "filled,bold"),
            // A target does nothing to an event (`docs/adr/target-components.md`); dashed, like
            // the router -> target edges into it.
            Role::Target => ("box", "filled,dashed"),
        };
        out.push_str(&format!("  {id:?} [shape={shape}, style=\"{style}\", label={id:?}];\n"));
    }
    out.push('\n');
    for (id, component) in &config.components {
        for source in &component.sources {
            out.push_str(&format!("  {source:?} -> {id:?};\n"));
        }
        // Router -> target edges are declared on the producer, not the consumer
        // (`docs/adr/target-components.md`), so they're dashed and labelled with the route key.
        // A `lua`/`lua_file` `targets:` entry has no key (the script picks), so no label.
        for (key, target) in graph::target_edges(component) {
            match key {
                Some(key) => out
                    .push_str(&format!("  {id:?} -> {target:?} [style=dashed, label={key:?}];\n")),
                None => out.push_str(&format!("  {id:?} -> {target:?} [style=dashed];\n")),
            }
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
                receive: logit_config::ReceiveConfig::default(),
                sources: vec![],
                targets: Vec::new(),
                kind: ComponentKind::StatsdIn {
                    bind: "x".to_string(),
                    transport: logit_config::StatsdTransport::default(),
                    tls: None,
                    handshake_timeout: logit_config::default_handshake_timeout(),
                    idle_timeout: None,
                },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["in".to_string()],
                targets: Vec::new(),
                kind: ComponentKind::InfluxDbOut {
                    url: "u".to_string(),
                    org: "o".to_string(),
                    bucket: "b".to_string(),
                    token: "T".to_string(),
                },
            },
        );
        let dot = render(&Config { components, ..Default::default() });
        assert!(dot.starts_with("digraph logit {"));
        assert!(dot.contains("\"in\""), "got: {dot}");
        assert!(dot.contains("\"out\""), "got: {dot}");
        assert!(dot.contains("\"in\" -> \"out\";"), "got: {dot}");
    }

    /// A dangling source reference still renders as an edge, making a typo'd source visible.
    #[test]
    fn a_dangling_source_reference_still_renders_an_edge() {
        let mut components = HashMap::new();
        components.insert(
            "out".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["missing".to_string()],
                targets: Vec::new(),
                kind: ComponentKind::InfluxDbOut {
                    url: "u".to_string(),
                    org: "o".to_string(),
                    bucket: "b".to_string(),
                    token: "T".to_string(),
                },
            },
        );
        let dot = render(&Config { components, ..Default::default() });
        assert!(dot.contains("\"missing\" -> \"out\";"), "got: {dot}");
    }

    fn component(sources: Vec<&str>, targets: Vec<&str>, kind: ComponentKind) -> Component {
        Component {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: sources.into_iter().map(String::from).collect(),
            targets: targets.into_iter().map(String::from).collect(),
            kind,
        }
    }

    /// A target renders as a dashed node.
    #[test]
    fn a_target_renders_as_a_dashed_node() {
        let mut components = HashMap::new();
        components
            .insert("host_stream".to_string(), component(vec![], vec![], ComponentKind::Target {}));
        let dot = render(&Config { components, ..Default::default() });
        assert!(dot.contains("\"host_stream\" [shape=box, style=\"filled,dashed\""), "got: {dot}");
    }

    /// Router -> target edges are dashed and labelled with the route key; a `lua` one has no label.
    #[test]
    fn router_to_target_edges_render_dashed_and_labelled() {
        let mut components = HashMap::new();
        components.insert(
            "split".to_string(),
            component(
                vec!["in"],
                vec![],
                ComponentKind::Route {
                    by: logit_config::RouteBy::Attribute("stream".to_string()),
                    routes: [
                        ("host", "host_stream"),
                        ("node", "host_stream"),
                        ("app", "app_stream"),
                    ]
                    .into_iter()
                    .map(|(value, target)| (value.to_string(), target.to_string()))
                    .collect(),
                },
            ),
        );
        components.insert(
            "enrich".to_string(),
            component(
                vec!["in"],
                vec!["app_stream"],
                ComponentKind::Lua { script: String::new(), interval: None },
            ),
        );
        components
            .insert("host_stream".to_string(), component(vec![], vec![], ComponentKind::Target {}));
        components
            .insert("app_stream".to_string(), component(vec![], vec![], ComponentKind::Target {}));
        let dot = render(&Config { components, ..Default::default() });
        assert!(
            dot.contains("\"split\" -> \"host_stream\" [style=dashed, label=\"host\"];"),
            "got: {dot}"
        );
        assert!(
            dot.contains("\"split\" -> \"host_stream\" [style=dashed, label=\"node\"];"),
            "many-to-one is two labelled edges, got: {dot}"
        );
        assert!(
            dot.contains("\"split\" -> \"app_stream\" [style=dashed, label=\"app\"];"),
            "got: {dot}"
        );
        assert!(dot.contains("\"enrich\" -> \"app_stream\" [style=dashed];"), "got: {dot}");
        assert!(!dot.contains("\"enrich\" -> \"app_stream\" [style=dashed, label"), "got: {dot}");
    }
}
