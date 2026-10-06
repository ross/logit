//! `logit run`'s process signals (`docs/adr/signal-handling.md`): SIGTERM/SIGINT start a graceful
//! drain and a second one exits 130; SIGHUP bumps a reopen generation and never exits.
//!
//! [`Signals::install`] runs first in `run_pipelines`, before the config loads, and creates every
//! tokio `Signal` stream before it returns. Once a stream exists, the signal's default disposition
//! is gone for the life of the process and a delivery waits for the stream's next poll, so a
//! signal that arrives during startup is handled rather than killing the process.

use std::future::Future;
use std::sync::Arc;
use tokio::sync::{watch, Notify};
use tokio::task::JoinHandle;

/// The signal tasks of one `logit run`, aborted on drop.
pub struct Signals {
    shutdown: Arc<Notify>,
    tasks: [JoinHandle<()>; 2],
    /// The reopen generation's first receiver, created with its sender so a hangup that lands
    /// before any sink clones it still reads as changed to every clone.
    _reopen: watch::Receiver<u64>,
}

impl Signals {
    /// Installs the handlers and spawns the tasks that serve them. Must run inside the tokio
    /// runtime.
    pub fn install() -> Self {
        let shutdown = Arc::new(Notify::new());
        let (reopen_tx, reopen) = watch::channel(0u64);
        #[cfg(unix)]
        let tasks = {
            use tokio::signal::unix::{signal, SignalKind};
            let terminate = signal(SignalKind::terminate()).expect("installing a SIGTERM handler");
            let interrupt = signal(SignalKind::interrupt()).expect("installing a SIGINT handler");
            let hangup = signal(SignalKind::hangup()).expect("installing a SIGHUP handler");
            [
                tokio::spawn(count_shutdown_signals(terminate, interrupt, shutdown.clone())),
                tokio::spawn(bump_on_hangup(hangup, reopen_tx)),
            ]
        };
        #[cfg(not(unix))]
        let tasks = {
            drop(reopen_tx);
            [tokio::spawn(count_ctrl_c(shutdown.clone())), tokio::spawn(async {})]
        };
        Self { shutdown, tasks, _reopen: reopen }
    }

    /// Resolves once the first SIGTERM or SIGINT has arrived, including one that arrived before
    /// this was called: the future `run_with_telemetry` races its run against.
    pub fn shutdown(&self) -> impl Future<Output = ()> + Send + 'static {
        let shutdown = self.shutdown.clone();
        async move { shutdown.notified().await }
    }
}

impl Drop for Signals {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// Counts SIGTERM and SIGINT together: the first fires `shutdown`, the second exits 130 so a
/// wedged drain stays killable by the signal that started it.
#[cfg(unix)]
async fn count_shutdown_signals(
    mut terminate: tokio::signal::unix::Signal,
    mut interrupt: tokio::signal::unix::Signal,
    shutdown: Arc<Notify>,
) {
    tokio::select! {
        _ = terminate.recv() => {}
        _ = interrupt.recv() => {}
    }
    // `notify_one` stores a permit when nothing waits yet, so a signal during startup still fires
    // the future `run_with_telemetry` polls later.
    shutdown.notify_one();
    tokio::select! {
        _ = terminate.recv() => {}
        _ = interrupt.recv() => {}
    }
    std::process::exit(130);
}

/// [`count_shutdown_signals`] off Unix, where Ctrl-C is the only shutdown signal and there's no
/// SIGHUP.
#[cfg(not(unix))]
async fn count_ctrl_c(shutdown: Arc<Notify>) {
    let _ = tokio::signal::ctrl_c().await;
    shutdown.notify_one();
    let _ = tokio::signal::ctrl_c().await;
    std::process::exit(130);
}

/// Bumps `reopen` once per SIGHUP for as long as the process runs, a drain included. A SIGHUP
/// never counts toward the 130 exit.
#[cfg(unix)]
async fn bump_on_hangup(mut hangup: tokio::signal::unix::Signal, reopen: watch::Sender<u64>) {
    while hangup.recv().await.is_some() {
        let mut generation = 0;
        reopen.send_modify(|g| {
            *g += 1;
            generation = *g;
        });
        tracing::info!(target: "logit", generation, config_reloaded = false, "reopen signal received");
    }
}
