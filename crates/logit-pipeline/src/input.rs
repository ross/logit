//! The `Input` trait -- moved here from `logit-inputs` (`docs/design/pipeline-graph.md`'s "Crate
//! layout" section) so `logit-pipeline` doesn't have to depend on any concrete input
//! implementation to define the shape every listener node fits.

use crate::Fanout;

/// A listener component: reads data from the outside world and produces batches into its
/// [`Fanout`]. What "listens" means varies (a UDP socket, a TCP accept loop, a file-tail watcher)
/// but every input converges on this. A listener component has no `sources` of its own
/// (`docs/design/pipeline-graph.md`'s arity table) -- `sink` here is everything downstream of it.
#[async_trait::async_trait]
pub trait Input {
    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()>;
}
