//! The `Input` trait -- moved here from `logit-inputs` (`docs/design/pipeline-graph.md`'s "Crate
//! layout" section) so `logit-pipeline` doesn't have to depend on any concrete input
//! implementation to define the shape every listener node fits.

use crate::Fanout;
use std::time::Duration;
use tokio::sync::watch;

/// A listener component: reads data from the outside world and produces batches into its
/// [`Fanout`]. What "listens" means varies (a UDP socket, a TCP accept loop, a file-tail watcher)
/// but every input converges on this. A listener component has no `sources` of its own
/// (`docs/design/pipeline-graph.md`'s arity table) -- `sink` here is everything downstream of it.
#[async_trait::async_trait]
pub trait Input {
    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()>;

    /// Runs until `shutdown` flips, with the opportunity to drain buffered work first
    /// (`docs/adr/0027-decoupled-listener-io.md`). The default *is* [`Input::run`] raced against
    /// the signal -- ADR 0013's original cancel-by-drop shutdown, relocated here: `shutdown`
    /// winning drops `run`'s future and the `Fanout` inside it, cascading the close-time flush
    /// through every downstream node exactly as before. An implementation with nothing buffered
    /// needs no override and gets byte-for-byte today's behaviour, including today's latency --
    /// this resolves at the instant `shutdown` fires, not later, for every input that doesn't
    /// override it.
    ///
    /// An override MUST still return within its configured grace
    /// ([`InputRuntimeConfig::shutdown_grace`]): `run_input` (`crate::runtime`) races this against
    /// that deadline as a backstop, and a listener that exceeds it is cancelled by drop anyway,
    /// losing (and not counting) whatever it was still draining.
    async fn run_until_shutdown(
        &mut self,
        sink: Fanout,
        mut shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        tokio::select! {
            result = self.run(sink) => result,
            _ = shutdown.wait_for(|&due| due) => Ok(()),
        }
    }
}

/// A listener's runtime knobs -- today only how long [`crate::runtime::run_input`] waits for a
/// cooperative [`Input::run_until_shutdown`] to drain before cancelling it by drop. Mirrors
/// [`crate::NodeSpec::Output`]'s `SinkQueueConfig`/`WriteLoopConfig`: production call sites
/// (`logit-cli::pipeline::build_spec`) derive `shutdown_grace` from the component's `receive:`
/// block; a test can pass a short grace to keep a shutdown test fast. `Duration::ZERO` -- the
/// default -- means "cancel by drop immediately," i.e. no change from ADR 0013's original
/// behaviour, which is exactly right for a listener with no `receive:` block (nothing overrides
/// `run_until_shutdown`, so nothing is ever waiting to drain).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InputRuntimeConfig {
    pub shutdown_grace: Duration,
}

impl Default for InputRuntimeConfig {
    fn default() -> Self {
        Self { shutdown_grace: Duration::ZERO }
    }
}
