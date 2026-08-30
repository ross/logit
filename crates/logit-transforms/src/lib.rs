//! Built-in native transform components -- no Lua VM involved, per `docs/design/lua-api.md`'s
//! "built-in native processors ... meant to sit in front of user Lua" split. Each implements
//! `logit_pipeline::Transform`, letting the node runtime run it as an ordinary tokio task (no
//! dedicated OS thread, unlike a Lua component -- `docs/design/pipeline-graph.md`'s "Node kinds"
//! section). `aggregate` is the first of these; more (`json`, `logfmt`, `kv`, `regex`, `csv`,
//! `rename`, `remove`, `filter`, `sample`, `throttle`, `dedup`) are expected to land here too.

mod aggregate;

pub use aggregate::Aggregator;
