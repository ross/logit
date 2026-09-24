//! The pipeline component graph: trait definitions, the graph resolution/validation module, and
//! the node runtime. See `docs/design/pipeline-graph.md` and
//! `docs/adr/component-graph-configuration.md` for the design this crate implements.
//!
//! This crate defines `Input`/`Output`/`Transform`/`Router`; `logit-inputs`/`logit-outputs`/
//! `logit-transforms` hold only implementations and depend on this crate, never the reverse
//! (`docs/design/pipeline-graph.md`'s "Crate layout" section). Keep this crate buildable without
//! any concrete input/output/transform kind.

// `sockstat`'s `getsockopt` calls are one of the codebase's three raw-`libc` `unsafe` call sites
// (docs/adr/out-of-ci-unsafe-verification.md). Denied per crate, not in `[workspace.lints]`: see
// `logit-inputs/src/lib.rs`'s matching comment for why.
#![deny(unsafe_op_in_unsafe_fn)]
#![deny(clippy::undocumented_unsafe_blocks)]

pub mod accumulator;
pub mod disk_queue;
pub mod fanout;
pub mod graph;
pub mod input;
pub mod output;
pub mod queue;
pub mod readiness;
pub mod router;
pub mod runtime;
/// Per-socket kernel counters (`SO_MEMINFO`, `TCP_INFO`) read off a raw fd. Not re-exported flat:
/// call sites read as `sockstat::meminfo`.
pub mod sockstat;
pub mod transform;

pub use accumulator::{BatchAccumulator, FlushReason};
pub use disk_queue::{DiskQueue, DiskQueueConfig};
pub use fanout::{BatchContext, Delivered, Fanout, SendTimeout, TraceContext};
pub use input::{Input, InputRuntimeConfig};
pub use output::{classify, is_explicitly_permanent, is_retryable, DeliveryPosture, Fault, Output};
pub use queue::{
    BoundedQueue, OverflowPolicy, QueueConfig, QueueMetrics, Queued, SinkQueue, SinkQueueConfig,
    SinkStore, SinkStoreConfig, SINK_QUEUE_METRICS,
};
pub use readiness::{NodeState, Phase, PipelineState, Readiness};
pub use router::{Destination, Router, RouterScratch};
pub use runtime::{
    process_batch, route_batch, run, run_with_shutdown, run_with_telemetry, send_batch,
    unwrap_batch, NodeSpec, RetryConfig, RunError, WriteLoopConfig,
};
pub use transform::{FlushOutput, FlushedEvent, Transform};
