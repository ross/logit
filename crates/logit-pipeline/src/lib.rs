//! The pipeline component graph: trait definitions, the graph resolution/validation module, and
//! the node runtime. See `docs/design/pipeline-graph.md` and
//! `docs/adr/0009-component-graph-configuration.md` for the design this crate implements.
//!
//! Crate layout note (`docs/design/pipeline-graph.md`'s "Crate layout" section): this crate
//! defines `Input`/`Output`/`Transform` -- `logit-inputs`/`logit-outputs`/`logit-transforms` hold
//! only implementations and depend on this crate for the trait, not the other way around. That
//! inversion is what avoids a circular dependency: this crate needs to be buildable without
//! knowing about any concrete input/output/transform kind.

pub mod fanout;
pub mod graph;
pub mod input;
pub mod output;
pub mod runtime;
pub mod transform;

pub use fanout::Fanout;
pub use input::Input;
pub use output::Output;
pub use runtime::{run, run_with_shutdown, NodeSpec};
pub use transform::Transform;
