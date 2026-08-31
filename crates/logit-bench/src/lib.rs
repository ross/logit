//! Allocation measurement and throughput benchmarks for `logit`. Dev-only -- `publish = false`,
//! and nothing in the shipped binary depends on it.
//!
//! Two things live here, against one shared workload ([`fixtures`], the repo's own
//! `examples/nginx-to-influxdb.yaml` reference pipeline):
//!
//! - **`tests/allocations.rs`** -- ordinary `#[test]`s that assert exact allocation counts for each
//!   stage, using [`alloc::CountingAlloc`]. These run in normal CI via `script/test`, so an
//!   allocation regression fails a build instead of appearing in a graph nobody reads. They're
//!   deterministic because `cargo nextest` runs each test in its own process.
//! - **`benches/pipeline.rs`** -- `divan` throughput benches over the same fixtures, run by hand
//!   with `script/bench`. Deliberately *not* in `script/cibuild`: wall-clock benchmarking on
//!   shared CI runners measures the runner, not the code.
//!
//! Every number quoted in `docs/design/memory.md` comes from one of those two, and that document
//! names the command that reproduces it.

pub mod alloc;
pub mod fixtures;
