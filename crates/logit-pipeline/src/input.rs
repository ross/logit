//! The `Input` trait every listener node implements. It lives here, not in `logit-inputs`, per
//! `docs/design/pipeline-graph.md`'s "Crate layout" section. What a listener loses when its future
//! is dropped at shutdown is in that doc's "Cancellation points" table.

use crate::Fanout;
use std::time::Duration;
use tokio::sync::watch;

/// A listener component: reads from the outside world (a UDP socket, a TCP accept loop, a
/// file-tail watcher) and produces batches into its [`Fanout`]. It has no `sources`
/// (`docs/design/pipeline-graph.md`'s arity table); `sink` is everything downstream of it.
#[async_trait::async_trait]
pub trait Input {
    /// Opens this listener's sockets/files. `crate::runtime::run_with_telemetry` calls it for
    /// every input, in sorted id order, before any node task is spawned, so a port that can't be
    /// bound fails startup with nothing else running.
    ///
    /// The default is a no-op, for an input with nothing to open (`logit_inputs::internal`).
    ///
    /// Two obligations on an override:
    /// - **Idempotent.** A second call must return `Ok(())` without re-opening.
    /// - **`run`/`run_until_shutdown` must still work if nobody called this first.** They call
    ///   `bind` themselves when whatever it produces is absent, so a caller outside the node
    ///   runtime (a direct unit test) needs only one call.
    ///
    /// `run` may assume the socket/file this opens already exists.
    async fn bind(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Whether the next process reads again a batch this listener was sending when shutdown
    /// stopped it: its source keeps what the listener hasn't confirmed, as a tail checkpoint or an
    /// uncommitted Kafka offset does. When `run_input`'s grace backstop cuts a send, a `true`
    /// listener's batch counts `logit.input.batches.deferred` and a `false` one's counts
    /// `batches.dropped`/`events.dropped{reason="shutdown"}`. The default is `false`.
    fn rereads_after_restart(&self) -> bool {
        false
    }

    /// The socket address this listener holds after [`Input::bind`], `None` before it or for an
    /// input with no network socket. A test binds `127.0.0.1:0` and reads the OS-assigned port
    /// here, through a boxed `dyn Input`, instead of choosing a port first and racing other
    /// processes for it.
    ///
    /// An input with several sockets reports its TCP socket (the stream listener, not a Unix
    /// socket or a second bind). Concrete types keep their inherent `local_addr`; this method has
    /// its own name so the two never shadow each other.
    fn bound_addr(&self) -> Option<std::net::SocketAddr> {
        None
    }

    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()>;

    /// Runs until `shutdown` flips, with the opportunity to drain buffered work first
    /// (`docs/adr/decoupled-listener-io.md`). The default races [`Input::run`] against the
    /// signal: `shutdown` winning drops `run`'s future and the `Fanout` inside it, cascading the
    /// close-time flush through every downstream node (cancel-by-drop, ADR
    /// `service-lifecycle-and-output-retry`). It resolves the instant `shutdown` fires, which is
    /// right for an input with nothing buffered.
    ///
    /// An override should return within its configured grace
    /// ([`InputRuntimeConfig::shutdown_grace`]): `run_input` (`crate::runtime`) races this against
    /// that deadline as a backstop and cancels a listener still running by drop. The backstop
    /// counts a batch the listener was parked sending, but whatever else it held undrained, such
    /// as a UDP listener's accumulated events, is lost uncounted. An override may rely on the
    /// backstop to end a wait it doesn't bound itself, provided the drop loses nothing it can't
    /// afford: an HTTP listener that waits out a request in flight with no ceiling relies on it,
    /// and the drop aborts its connection tasks, costing a client its response and any batch a
    /// handler was still sending, which the backstop counts. ADR
    /// `shutdown-accounting-and-cancellation-safety`'s 2026-11-02 amendment records the counts
    /// and the races in them.
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

/// A listener's runtime knobs: how long [`crate::runtime::run_input`] waits for a cooperative
/// [`Input::run_until_shutdown`] to drain before cancelling it by drop. Production call sites
/// (`logit-cli::pipeline::build_spec`) derive `shutdown_grace` from the component's `receive:`
/// block, and this is the only copy of it a listener sees. The default, `Duration::ZERO`, cancels by drop immediately, which is right for a
/// listener with no `receive:` block: nothing overrides `run_until_shutdown`, so nothing waits to
/// drain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InputRuntimeConfig {
    pub shutdown_grace: Duration,
}

impl Default for InputRuntimeConfig {
    fn default() -> Self {
        Self { shutdown_grace: Duration::ZERO }
    }
}
