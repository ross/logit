//! The wake source a [`crate::tail::driver::Tailer`] races against its own poll tick. Only
//! [`Watcher::Poll`] exists yet -- an `inotify`-backed variant lands in a follow-up change
//! (`docs/adr/file-tailing-and-docker-json-logs.md`'s Workstream B); `WatchMode::Inotify`/
//! `WatchMode::Auto` both fall back to polling for now, since there is nothing else to fall
//! forward to. [`Watcher::next_wake`] under `Poll` simply never resolves
//! (`std::future::pending`), which is correct today: the driver's own `poll_interval` tick is
//! the only wake source, and racing a future that never completes against it costs nothing.

use super::WatchMode;
use std::path::{Path, PathBuf};

/// Something changed under a watched directory. `Overflow` is reserved for the `inotify`
/// variant's kernel event queue overflowing -- unreachable under [`Watcher::Poll`], which has no
/// queue to overflow.
///
/// Neither variant is constructed yet -- [`Watcher::Poll`]'s [`Watcher::next_wake`] never
/// resolves -- so both are dead code by construction until the `inotify` variant lands
/// (Workstream B, `docs/adr/file-tailing-and-docker-json-logs.md`) and starts producing them;
/// `driver::Tailer::run_until_shutdown` already matches on both today so that landing needs no
/// call-site change.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) enum Wake {
    Changed(PathBuf),
    Overflow,
}

pub(crate) enum Watcher {
    Poll,
}

impl Watcher {
    /// `mode` is accepted (not ignored outright) so the call site doesn't need to change once a
    /// second variant exists -- every mode resolves to [`Watcher::Poll`] today.
    pub fn new(_mode: WatchMode) -> anyhow::Result<Self> {
        Ok(Watcher::Poll)
    }

    pub fn watch_dir(&mut self, _dir: &Path) -> std::io::Result<()> {
        Ok(())
    }

    /// Unused until a caller ever needs to stop watching a directory mid-run (`docker_in`'s
    /// container churn, landing with `docker_in` itself) -- present now so `Tailer` has a stable
    /// method to call once it does.
    #[allow(dead_code)]
    pub fn unwatch_dir(&mut self, _dir: &Path) {}

    pub async fn next_wake(&mut self) -> Wake {
        match self {
            Watcher::Poll => std::future::pending().await,
        }
    }

    /// Test-only for now (asserting `Auto`'s fallback behavior once `inotify` exists to fall
    /// back from) -- `driver::Tailer` tags its own `logit.input.watch.wakes` counter by which
    /// `select!` outcome actually fired, which doesn't need this.
    #[allow(dead_code)]
    pub fn is_inotify(&self) -> bool {
        false
    }
}
