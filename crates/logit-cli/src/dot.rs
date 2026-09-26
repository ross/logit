//! `logit graph`: renders a config's component graph as graphviz DOT
//! (`docs/design/pipeline-graph.md`'s "`logit graph`: visualizing the resolved DAG" section).
//!
//! Renders off `Config`, not a resolved `Graph`, so a cyclic or otherwise invalid config still
//! renders: an undefined source becomes a bare node graphviz auto-creates, which is what this
//! command exists to show. It still needs a typed `Config`, so every `!env` reference must
//! resolve first, even on a field this never reads (`docs/adr/env-yaml-tag.md`).
//!
//! Conventions: a listener is a rounded box, a transform an ellipse, a sink a bold box, and a
//! target a dashed box. A `sources` edge is solid, and labelled `[else]` when its source directs
//! events into targets; a router -> target edge is dashed. Listeners
//! share the leftmost rank and sinks the rightmost, so a graph reads inputs -> processing ->
//! backends whatever the depth of each path.

use logit_config::Config;
use logit_pipeline::graph::{self, role, Role};

pub fn render(config: &Config) -> String {
    let mut out =
        String::from("digraph logit {\n  rankdir=LR;\n  node [fontname=\"monospace\"];\n\n");
    // `components` is a `HashMap`; sorting by id keeps the output, and an SVG committed from it,
    // stable across runs of an unchanged config.
    let mut components: Vec<_> = config.components.iter().collect();
    components.sort_unstable_by_key(|(id, _)| id.as_str());
    for (id, component) in &components {
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
    for (rank, wanted) in [("source", Role::Listener), ("sink", Role::Sink)] {
        let ids: Vec<_> = components
            .iter()
            .filter(|(_, component)| role(&component.kind) == wanted)
            .map(|(id, _)| format!("{id:?};"))
            .collect();
        if !ids.is_empty() {
            out.push_str(&format!("  {{ rank={rank}; {} }}\n", ids.join(" ")));
        }
    }
    out.push('\n');
    for (id, component) in &components {
        for source in &component.sources {
            // A targeting node's ordinary edges carry what no target took, so label them to
            // tell them apart from the dashed target edges (`docs/adr/target-components.md`).
            let targeting = config
                .components
                .get(source)
                .is_some_and(|producer| !graph::target_edges(producer).is_empty());
            if targeting {
                out.push_str(&format!("  {source:?} -> {id:?} [label=\"[else]\"];\n"));
            } else {
                out.push_str(&format!("  {source:?} -> {id:?};\n"));
            }
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
        assert!(dot.contains("{ rank=source; \"in\"; }"), "got: {dot}");
        assert!(dot.contains("{ rank=sink; \"out\"; }"), "got: {dot}");
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

    /// Output doesn't depend on the order components went into the `HashMap`.
    #[test]
    fn output_is_the_same_whatever_the_insertion_order() {
        let ids = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"];
        let render_in = |order: Vec<&str>| {
            let mut components = HashMap::new();
            for id in order {
                components.insert(
                    id.to_string(),
                    component(vec!["alpha", "bravo"], vec![], ComponentKind::Target {}),
                );
            }
            render(&Config { components, ..Default::default() })
        };
        let forward = render_in(ids.to_vec());
        assert_eq!(forward, render_in(ids.iter().rev().copied().collect()));
        for _ in 0..8 {
            assert_eq!(forward, render_in(ids.to_vec()));
        }
        let alpha = forward.find("\"alpha\" [").expect("alpha node");
        let hotel = forward.find("\"hotel\" [").expect("hotel node");
        assert!(alpha < hotel, "nodes sorted by id, got: {forward}");
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
                ComponentKind::Lua { script: String::new(), interval: None, max_memory: None },
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

    /// A targeting node's ordinary consumer edge is labelled `[else]`; other edges are bare.
    #[test]
    fn targeting_nodes_label_their_ordinary_edges_else() {
        let mut components = HashMap::new();
        components.insert(
            "split".to_string(),
            component(
                vec!["in"],
                vec![],
                ComponentKind::Route {
                    by: logit_config::RouteBy::Attribute("stream".to_string()),
                    routes: [("app".to_string(), "app_stream".to_string())].into_iter().collect(),
                },
            ),
        );
        components
            .insert("app_stream".to_string(), component(vec![], vec![], ComponentKind::Target {}));
        components.insert(
            "rest".to_string(),
            component(
                vec!["split"],
                vec![],
                ComponentKind::Lua { script: String::new(), interval: None, max_memory: None },
            ),
        );
        let dot = render(&Config { components, ..Default::default() });
        assert!(dot.contains("\"split\" -> \"rest\" [label=\"[else]\"];"), "got: {dot}");
        assert!(dot.contains("\"in\" -> \"split\";"), "got: {dot}");
    }
}
