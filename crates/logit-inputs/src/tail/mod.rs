//! Generic file tailing: read one or more files line by line into log events -- rotation-,
//! truncation-, and checkpoint-aware. [`TailInput`] (`tail_in`) is the plain "one line, one raw
//! log event" listener; `crate::docker`'s `docker_in` builds on the same [`driver::Tailer`],
//! swapping in a decoder for Docker's json-file envelope instead of [`line::LineDecoder`]'s bare
//! line. See `docs/adr/file-tailing-and-docker-json-logs.md`.
//!
//! **No receive queue.** Every other listener in this crate decouples its socket read from its
//! decode loop through a `ReceiveQueue` (`docs/adr/decoupled-listener-io.md`), because a kernel
//! socket buffer can't be asked to wait. A tailed file is different: the file itself is already
//! a durable, arbitrarily-large buffer, so there's nothing to protect against overflowing by
//! dropping -- the read loop simply stops advancing when `Fanout::send` is slow, and resumes
//! exactly where it left off once downstream has room again. `logit_pipeline::BatchAccumulator`
//! is reused as-is for the decoded-events-\>batch half, one instance per tracked file; only the
//! read/queue half genuinely differs from `crate::udp`.

mod checkpoint;
mod driver;
mod line;
mod pattern;
mod watch;

pub use line::{LineDecoder, TailDecoder};
pub use pattern::PathPattern;

// `pub(crate)`, not a private `use`: `crate::docker`'s `docker_in` builds on this same driver
// (`docs/adr/file-tailing-and-docker-json-logs.md`), so both need to be reachable as
// `crate::tail::{Tailer, DecoderFactory}` from outside this module, not just from within it.
pub(crate) use driver::{DecoderFactory, Tailer};
use logit_core::{Diagnostics, Resource, Telemetry};
use logit_pipeline::Fanout;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch as shutdown_watch;

/// Where a tailed file starts reading, the first time it's seen and no checkpoint entry names
/// it. Mirrors `logit_config::ReadFrom` -- kept as its own copy rather than a dependency on
/// `logit-config` (`docs/design/pipeline-graph.md`'s crate layout: `logit-inputs` holds impls,
/// not config types); `logit-cli::pipeline` converts between the two, the same pattern
/// `logit_pipeline::OverflowPolicy` already follows for `receive:`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadFrom {
    #[default]
    End,
    Beginning,
}

/// Mirrors `logit_config::WatchMode` -- see [`ReadFrom`]'s doc comment for why this is its own
/// copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WatchMode {
    #[default]
    Auto,
    Inotify,
    Poll,
}

/// The receive-side batch-assembly knobs a tailing listener's `receive:` block may set --
/// everything `logit_config::ReceiveConfig` carries *except* the queue-bounding fields
/// (`max_datagrams`, `max_bytes`, `overflow`, `receive_buffer_bytes`), which have no meaning
/// here (`docs/adr/file-tailing-and-docker-json-logs.md`; graph rule 17 rejects them on a tail
/// listener). Mirrors `crate::udp::UdpListenerConfig`'s own subset of the same source config.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TailBatching {
    pub max_events: usize,
    pub max_bytes: u64,
    pub flush_interval: Duration,
    pub shutdown_grace: Duration,
}

impl Default for TailBatching {
    fn default() -> Self {
        Self {
            max_events: 1_000,
            max_bytes: 1024 * 1024,
            flush_interval: Duration::from_millis(100),
            shutdown_grace: Duration::from_secs(5),
        }
    }
}

/// A tailing listener's full runtime configuration -- built from `logit_config::TailOptions`
/// (plus `receive:`) by `logit-cli::pipeline`, mirroring `UdpListenerConfig`'s own role for the
/// datagram listeners.
#[derive(Debug, Clone, PartialEq)]
pub struct TailConfig {
    /// `None` -- the default -- means no checkpoint at all: every restart re-applies
    /// `read_from` to every file as if newly discovered.
    pub checkpoint_path: Option<PathBuf>,
    pub read_from: ReadFrom,
    pub watch: WatchMode,
    /// The read/rescan cadence used as-is under `WatchMode::Poll`, and as a reconciliation pass
    /// under `Inotify`/`Auto`. Never disabled -- rejected at `0s` by graph validation.
    pub poll_interval: Duration,
    /// How long a dirty checkpoint may sit before being flushed, in addition to being flushed on
    /// every file close and on shutdown. Rejected at `0s` by graph validation.
    pub checkpoint_interval: Duration,
    /// A line longer than this is dropped whole (not truncated) and diagnosed. Rejected at `0`
    /// by graph validation.
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

/// `tail_in`: tails a fixed set of file path patterns, one line -\> one raw log event
/// ([`LineDecoder`]).
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

    /// The currently-configured knobs -- test introspection, mirroring `UdpListener::config`.
    pub fn config(&self) -> &TailConfig {
        self.inner.config()
    }
}

#[async_trait::async_trait]
impl logit_pipeline::Input for TailInput {
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

/// Scratch-directory helpers shared by every test module under `tail/` -- this crate has no
/// `tempfile` dependency (`docs/adr/file-tailing-and-docker-json-logs.md`'s Alternatives), so
/// tests build and tear down their own unique temp directories by hand, following
/// `crates/logit-cli/src/pipeline.rs`'s own `std::env::temp_dir()`-based test precedent.
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
