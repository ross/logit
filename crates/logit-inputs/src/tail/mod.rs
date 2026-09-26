//! Rotation-, truncation-, and checkpoint-aware file tailing into log events. [`TailInput`]
//! (`tail_in`) emits one raw log event per line; `crate::docker`'s `docker_in` runs the same
//! [`driver::Tailer`] with a json-file envelope decoder in place of [`line::LineDecoder`]. See
//! `docs/adr/file-tailing-and-docker-json-logs.md`.
//!
//! **No receive queue.** A datagram listener needs a `ReceiveQueue`
//! (`docs/adr/decoupled-listener-io.md`) because a kernel socket buffer can't be asked to wait. A
//! tailed file is already a durable buffer, so the read loop stops advancing while `Fanout::send`
//! is slow and resumes at the same offset. Batch assembly still reuses
//! `logit_pipeline::BatchAccumulator`, one per tracked file, configured from `receive:` through
//! [`TailBatching`].

mod checkpoint;
mod driver;
mod line;
mod pattern;
mod watch;

pub use line::{LineDecoder, TailDecoder};
pub use pattern::PathPattern;

// `pub(crate)` so `crate::docker` can build `docker_in` on the same driver.
pub(crate) use driver::{DecoderFactory, Refresh, Tailer};
use logit_core::{Diagnostics, Resource, Telemetry};
use logit_pipeline::Fanout;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch as shutdown_watch;

/// Where a newly seen file with no checkpoint entry starts reading.
///
/// A copy of `logit_config::ReadFrom`, because `logit-inputs` doesn't depend on `logit-config`
/// (`docs/design/pipeline-graph.md`'s "Crate layout"); `logit-cli::pipeline` converts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadFrom {
    #[default]
    End,
    Beginning,
}

/// A copy of `logit_config::WatchMode`, for the reason [`ReadFrom`] gives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WatchMode {
    #[default]
    Auto,
    Inotify,
    Poll,
}

/// The batch-assembly half of a tailing listener's `receive:` block.
///
/// `logit_config::ReceiveConfig`'s queue fields (`max_datagrams`, `max_bytes`, `overflow`,
/// `receive_buffer_bytes`, `read_batch`) have no meaning without a receive queue; graph rule 17
/// rejects them on a tail listener.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TailBatching {
    pub max_events: usize,
    pub max_bytes: u64,
    pub flush_interval: Duration,
}

impl Default for TailBatching {
    fn default() -> Self {
        Self {
            max_events: 1_000,
            max_bytes: 1024 * 1024,
            flush_interval: Duration::from_millis(100),
        }
    }
}

/// A tailing listener's runtime configuration, built by `logit-cli::pipeline` from
/// `logit_config::TailOptions` plus `receive:`.
#[derive(Debug, Clone, PartialEq)]
pub struct TailConfig {
    /// `None` (the default) means no checkpoint: every restart applies `read_from` to every file.
    pub checkpoint_path: Option<PathBuf>,
    pub read_from: ReadFrom,
    pub watch: WatchMode,
    /// The read/rescan cadence under `WatchMode::Poll`, and the reconciliation pass under
    /// `Inotify`/`Auto`. Never disabled: graph validation rejects `0s`.
    pub poll_interval: Duration,
    /// How long a dirty checkpoint may wait before it's written. It's also written on every file
    /// close and on shutdown. Graph validation rejects `0s`.
    pub checkpoint_interval: Duration,
    /// A longer line is dropped whole (not truncated) and diagnosed. Graph validation rejects
    /// `0`.
    pub max_line_bytes: usize,
    pub batching: TailBatching,
}

impl Default for TailConfig {
    fn default() -> Self {
        Self {
            checkpoint_path: None,
            read_from: ReadFrom::default(),
            watch: WatchMode::default(),
            poll_interval: Duration::from_secs(1),
            checkpoint_interval: Duration::from_secs(5),
            max_line_bytes: 1024 * 1024,
            batching: TailBatching::default(),
        }
    }
}

/// `tail_in`: tails a fixed set of path patterns, one raw log event per line ([`LineDecoder`]).
pub struct TailInput {
    inner: Tailer<LineDecoder, LineDecoderFactory>,
}

struct LineDecoderFactory {
    diag: Diagnostics,
}

impl DecoderFactory<LineDecoder> for LineDecoderFactory {
    fn accept(&mut self, _path: &std::path::Path) -> bool {
        true // tail_in has no filter -- every path a pattern matches is tailed
    }

    fn open(&mut self, path: &std::path::Path) -> anyhow::Result<LineDecoder> {
        Ok(LineDecoder::new(path, Arc::new(Resource::default()))
            .with_diagnostics(self.diag.clone()))
    }
}

impl TailInput {
    pub fn new(paths: Vec<PathBuf>, config: TailConfig) -> Self {
        let patterns = paths.into_iter().map(PathPattern::new).collect();
        let factory = LineDecoderFactory { diag: Diagnostics::default() };
        Self { inner: Tailer::new(patterns, factory, config) }
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.inner.factory_mut().diag = diag.clone();
        self.inner = self.inner.with_diagnostics(diag);
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.inner = self.inner.with_telemetry(telemetry);
        self
    }

    /// The configured knobs, for test introspection.
    pub fn config(&self) -> &TailConfig {
        self.inner.config()
    }
}

#[async_trait::async_trait]
impl logit_pipeline::Input for TailInput {
    async fn bind(&mut self) -> anyhow::Result<()> {
        self.inner.bind().await
    }

    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        let (_tx, rx) = shutdown_watch::channel(false);
        self.inner.run_until_shutdown(sink, rx).await
    }

    async fn run_until_shutdown(
        &mut self,
        sink: Fanout,
        shutdown: shutdown_watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        self.inner.run_until_shutdown(sink, shutdown).await
    }
}

/// Unique scratch directories for the `tail/` tests; this crate has no `tempfile` dependency.
#[cfg(test)]
pub(crate) mod test_support {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    pub(crate) fn scratch_dir(label: &str) -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir()
            .join(format!("logit-tail-test-{label}-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create scratch dir");
        dir
    }
}
