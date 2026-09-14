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
//! like a token) must resolve first, same as `run`/`validate` (`docs/adr/env-yaml-tag.md`).

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
            // A target is a named destination, not a component that does anything to an event
            // (`docs/adr/target-components.md`) -- dashed, matching the dashed router -> target
            // edges below.
            Role::Target => ("box", "filled,dashed"),
        };
        out.push_str(&format!("  {id:?} [shape={shape}, style=\"{style}\", label={id:?}];\n"));
    }
    out.push('\n');
    for (id, component) in &config.components {
        for source in &component.sources {
            out.push_str(&format!("  {source:?} -> {id:?};\n"));
        }
        // Router -> target edges run the other way round from a `sources` edge -- they're
        // declared on the *producer* (`docs/adr/target-components.md`) -- so they're drawn
        // dashed, labelled with the route key that directs an event down each one. A
        // `lua`/`lua_file` `targets:` entry has no key (the destination is chosen in the script),
        // so it gets no label rather than an empty one.
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
                kind: ComponentKind::StatsdIn { bind: "x".to_string() },
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

    /// A target is a named destination rather than a component that does anything to an event, so
    /// it renders dashed -- the same styling as the edges directed into it.
    #[test]
    fn a_target_renders_as_a_dashed_node() {
        let mut components = HashMap::new();
        components
            .insert("host_stream".to_string(), component(vec![], vec![], ComponentKind::Target {}));
        let dot = render(&Config { components, ..Default::default() });
        assert!(dot.contains("\"host_stream\" [shape=box, style=\"filled,dashed\""), "got: {dot}");
    }

    /// Router -> target edges are declared on the producer, not the consumer, so they're drawn
    /// dashed and labelled with the route key -- except a `lua` `targets:` entry, whose
    /// destination is chosen in the script and so carries no key to label
    /// (`docs/adr/target-components.md`).
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
