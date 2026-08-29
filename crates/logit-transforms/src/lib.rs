//! Built-in native transform stages -- no Lua VM involved, per `docs/design/lua-api.md`'s "built-in
//! native processors ... meant to sit in front of user Lua in a chain" split. `aggregate` is the
//! first of these; more (`json`, `logfmt`, `kv`, `regex`, `csv`, `rename`, `remove`, `filter`,
//! `sample`, `throttle`, `dedup`) are expected to land here too.

mod aggregate;

pub use aggregate::Aggregator;
