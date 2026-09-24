//! Allocation measurement and throughput benchmarks for `logit`. Dev-only: `publish = false`, and
//! nothing in the shipped binary depends on it.
//!
//! Both halves share one set of inputs, [`fixtures`], built around the pre-`http_access` shape of
//! the `examples/nginx-to-influxdb.yaml` reference pipeline, kept unchanged so the pinned counts
//! stay comparable ([`fixtures`]'s module doc says how the pipeline has since moved on):
//!
//! - **`tests/allocations.rs`**: ordinary `#[test]`s asserting exact allocation counts per stage
//!   with [`alloc::CountingAlloc`]. They run in CI via `script/test`, so an allocation regression
//!   fails the build, and they're deterministic because `cargo nextest` runs each test in its own
//!   process.
//! - **`benches/pipeline.rs`**: `divan` throughput benches over the same fixtures, run by hand with
//!   `script/bench`. Not in `script/cibuild`: wall-clock timing on shared CI runners measures the
//!   runner, not the code.
//!
//! Every number in `docs/design/memory.md` comes from one of the two, and that document names the
//! command that reproduces it.

pub mod alloc;
pub mod bakeoff;
pub mod fixtures;
