//! `cri_in`: tails every Kubernetes container log under the kubelet's pod log directory
//! (`<root>/<namespace>_<pod>_<uid>/<container>/<n>.log`, default `/var/log/pods`) in the CRI
//! format containerd and CRI-O write, and stamps each file's pod and container identity from its
//! path. It runs on `tail_in`'s driver (`crate::tail`), with [`CriDecoder`] in place of
//! [`crate::tail::LineDecoder`] and [`PathPattern::pod_logs`] in place of config-driven patterns.
//! `receive:` takes the tail listener's batch-assembly fields. The decisions are
//! `docs/adr/cri-container-logs.md`'s; this doc cites its sections.
//!
//! - **Discovery** (section 2): the live `<n>.log` and every rotated `<n>.log.<YYYYMMDD-HHMMSS>`
//!   the kubelet hasn't compressed, so a restart resumes a renamed file. A rotation by rename
//!   rebinds the tracked inode to its new name and keeps its decoder and resource.
//! - **Records** go through `logit_proto::cri::parse`. The splitter keeps a trailing `\r` and
//!   the driver hands over raw bytes: a reassembled line is converted to UTF-8 once, diagnosed
//!   `invalid_utf8` when that replaced anything (section 3).
//! - **Reassembly** (section 4): a `P` record opens or extends its stream's held run and the next
//!   `F` of that stream closes it, so the two streams' runs interleave freely. The event's time is
//!   the run's first record's (section 6).
//! - **Resource** (section 7): `k8s.namespace.name`, `k8s.pod.name`, `k8s.pod.uid`,
//!   `k8s.container.name`, and `k8s.container.restart_count` (an `i64`), from the path. Each event
//!   carries `log.iostream`.
//! - **Counters** of its own: `logit.input.frames.dropped{reason="oversize"|"truncated"|"malformed"}`
//!   and `logit.input.frames.truncated{reason="partial_timeout"}` ([`CriDecoder`]). **Diagnostics**
//!   of its own: `bad_time` and `long_line`, beside the driver's `bad_line` and `invalid_utf8`.

use crate::docker::envelope_cap;
use crate::tail::{
    is_container_dir, log_restart_count, name_of, pod_dir_parts, DecoderFactory, PathPattern,
    TailConfig, TailDecoder, Tailer,
};
use bytes::Bytes;
use logit_core::{AttrMap, BodyFormat, Diagnostics, Event, LogRecord, Resource, Telemetry, Value};
use logit_pipeline::Fanout;
use logit_proto::cri::{self, Stream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch as shutdown_watch;
use tokio::time::Instant;

const FRAMES_DROPPED: &str = "logit.input.frames.dropped";
const FRAMES_TRUNCATED: &str = "logit.input.frames.truncated";

/// A line the runtime wrote as `P` records, held until its `F`.
struct Run {
    content: Vec<u8>,
    /// The first record's timestamp, or the read time when it didn't parse.
    timestamp: i64,
}

/// One stream's reassembly state.
#[derive(Default)]
struct StreamState {
    run: Option<Run>,
    /// The stream's line was dropped for its length; its later records are discarded until its
    /// `F`, which clears this. Counted when the drop began.
    dropping: bool,
    /// When a record last left this stream holding a run or dropping one, for
    /// [`TailDecoder::flush_idle`]. `None` while it holds nothing.
    last: Option<Instant>,
}

impl StreamState {
    fn holds(&self) -> bool {
        self.run.is_some() || self.dropping
    }
}

/// `cri_in`'s [`TailDecoder`]: reassembles each stream's `P` run into one line, and stamps each
/// event with the file's resource and its stream as `log.iostream`.
///
/// `max_line_bytes` bounds the reassembled content and is checked before each append, so a held
/// run never exceeds it. What it counts, one per line:
///
/// - `logit.input.frames.dropped{reason="oversize"}` when a line's drop begins, here or through
///   [`TailDecoder::line_dropped`] for a record the splitter dropped; the stream's later records
///   are discarded through its `F`, uncounted.
/// - `logit.input.frames.dropped{reason="truncated"}` for each stream's run that
///   [`TailDecoder::reset`] discards.
/// - `logit.input.frames.dropped{reason="malformed"}` for a record that isn't CRI grammar, beside
///   the driver's `bad_line`. Both streams' runs are emitted as they stand first, stdout first.
/// - `logit.input.frames.truncated{reason="partial_timeout"}` for a run
///   [`TailDecoder::flush_idle`] emits as it stands: delivered, not lost.
pub struct CriDecoder {
    resource: Arc<Resource>,
    /// Indexed by [`index`].
    streams: [StreamState; 2],
    max_line_bytes: usize,
    /// `Duration::ZERO` turns the idle flush off.
    partial_timeout: Duration,
    diag: Diagnostics,
    telemetry: Telemetry,
}

const STREAMS: [Stream; 2] = [Stream::Stdout, Stream::Stderr];

fn index(stream: Stream) -> usize {
    match stream {
        Stream::Stdout => 0,
        Stream::Stderr => 1,
    }
}

impl CriDecoder {
    pub(crate) fn new(
        resource: Arc<Resource>,
        max_line_bytes: usize,
        partial_timeout: Duration,
    ) -> Self {
        Self {
            resource,
            streams: Default::default(),
            max_line_bytes,
            partial_timeout,
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
        }
    }

    pub(crate) fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    pub(crate) fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Emits every stream's held run as it stands, stdout first. A drop in progress stays.
    fn flush_held(&mut self, out: &mut Vec<Event>) {
        for stream in STREAMS {
            self.flush_stream(stream, out);
        }
    }

    /// Emits `stream`'s held run as it stands, if it has one. Returns whether it had.
    fn flush_stream(&mut self, stream: Stream, out: &mut Vec<Event>) -> bool {
        let state = &mut self.streams[index(stream)];
        let Some(run) = state.run.take() else { return false };
        if !state.dropping {
            state.last = None;
        }
        self.emit(run.timestamp, stream, Bytes::from(run.content), out);
        true
    }

    /// Starts dropping `stream`'s line: its held run goes with it, counted once with a
    /// `long_line`. A dropped `F` ends the drop at once.
    fn start_drop(&mut self, stream: Stream, partial: bool) {
        let state = &mut self.streams[index(stream)];
        state.run = None;
        state.dropping = partial;
        state.last = partial.then(Instant::now);
        self.telemetry.count(FRAMES_DROPPED, 1.0, &[("reason", "oversize")]);
        self.diag.warn_throttled(
            "long_line",
            "a container log line exceeded max_line_bytes and was dropped whole",
        );
    }

    fn emit(&mut self, timestamp: i64, stream: Stream, content: Bytes, out: &mut Vec<Event>) {
        let message = match std::str::from_utf8(&content) {
            Ok(_) => content,
            Err(_) => {
                self.diag.warn_throttled(
                    "invalid_utf8",
                    "a line contained invalid UTF-8; lossily converted",
                );
                Bytes::from(String::from_utf8_lossy(&content).into_owned())
            }
        };
        let mut attrs = AttrMap::new();
        attrs.insert("log.iostream", stream.as_str());
        out.push(Event::log(
            timestamp,
            attrs,
            LogRecord {
                message: Value::Str(message),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        ));
    }

    /// One CRI record (section 4's rules).
    fn decode_record(
        &mut self,
        line: &Bytes,
        record: cri::CriLine<'_>,
        read_at: i64,
        out: &mut Vec<Event>,
    ) {
        let stream = record.stream;
        let state = &mut self.streams[index(stream)];
        if state.dropping {
            if record.partial {
                state.last = Some(Instant::now());
            } else {
                state.dropping = false;
                state.last = None;
            }
            return;
        }
        let held = state.run.as_ref().map_or(0, |run| run.content.len());
        if held + record.content.len() > self.max_line_bytes {
            self.start_drop(stream, record.partial);
            return;
        }
        let timestamp = match (&state.run, record.timestamp_nanos) {
            (Some(run), _) => run.timestamp,
            (None, Ok(ts)) => ts,
            (None, Err(err)) => {
                self.diag.warn_throttled(
                    "bad_time",
                    format!("unreadable CRI log timestamp ({err:?}); using read time"),
                );
                read_at
            }
        };
        let state = &mut self.streams[index(stream)];
        if record.partial {
            let run = state.run.get_or_insert_with(|| Run { content: Vec::new(), timestamp });
            run.content.extend_from_slice(record.content);
            state.last = Some(Instant::now());
            return;
        }
        state.last = None;
        let content = match state.run.take() {
            Some(mut run) => {
                run.content.extend_from_slice(record.content);
                Bytes::from(run.content)
            }
            // A whole line in one record: a slice of the read, no copy.
            None => line.slice_ref(record.content),
        };
        self.emit(timestamp, stream, content, out);
    }
}

/// What [`CriDecoder::line_dropped`] reads from a dropped record's first bytes: its stream and
/// whether its first tag is `P`. Unlike `logit_proto::cri::parse`, it doesn't need the space
/// after the tag, which a long tag field can push past the head (ADR "Implementation notes for
/// W2"). A head that ends right after a `P` reads as `P`.
fn parse_head(head: &[u8]) -> Option<(Stream, bool)> {
    let after_ts = head.iter().position(|&b| b == b' ')? + 1;
    let rest = &head[after_ts..];
    let after_stream = rest.iter().position(|&b| b == b' ')?;
    let stream = match &rest[..after_stream] {
        b"stdout" => Stream::Stdout,
        b"stderr" => Stream::Stderr,
        _ => return None,
    };
    let tag = &rest[after_stream + 1..];
    let partial = match tag {
        [] => return None,
        [b'P'] | [b'P', b':' | b' ', ..] => true,
        _ => false,
    };
    Some((stream, partial))
}

impl TailDecoder for CriDecoder {
    const STRIPS_CR: bool = false;
    const LOSSY_UTF8: bool = false;
    const KEEPS_DROP_HEADS: bool = true;
    const FLUSHES_IDLE: bool = true;

    fn decode_line(
        &mut self,
        line: Bytes,
        read_at: i64,
        out: &mut Vec<Event>,
    ) -> Result<Arc<Resource>, logit_proto::CodecError> {
        match cri::parse(&line) {
            Ok(record) => {
                self.decode_record(&line, record, read_at, out);
                Ok(self.resource.clone())
            }
            Err(err) => {
                self.flush_held(out);
                self.telemetry.count(FRAMES_DROPPED, 1.0, &[("reason", "malformed")]);
                Err(logit_proto::CodecError::Malformed(format!("CRI log record: {err}")))
            }
        }
    }

    /// Section 4's splitter drop. A head that doesn't parse flushes both streams' runs, as a
    /// malformed record does, and leaves the count to the driver.
    fn line_dropped(&mut self, head: &[u8], _start: u64, out: &mut Vec<Event>) -> bool {
        let Some((stream, partial)) = parse_head(head) else {
            self.flush_held(out);
            return false;
        };
        let state = &mut self.streams[index(stream)];
        if state.dropping {
            if partial {
                state.last = Some(Instant::now());
            } else {
                state.dropping = false;
                state.last = None;
            }
            return true;
        }
        self.start_drop(stream, partial);
        true
    }

    /// Section 5: each stream whose last record is at least `partial_timeout` old is flushed, or
    /// its drop ended.
    fn flush_idle(&mut self, now: Instant, out: &mut Vec<Event>) {
        if self.partial_timeout.is_zero() {
            return;
        }
        for stream in STREAMS {
            let state = &mut self.streams[index(stream)];
            let expired = state
                .last
                .is_some_and(|last| now.saturating_duration_since(last) >= self.partial_timeout);
            if !expired {
                continue;
            }
            state.dropping = false;
            state.last = None;
            if self.flush_stream(stream, out) {
                self.telemetry.count(FRAMES_TRUNCATED, 1.0, &[("reason", "partial_timeout")]);
            }
        }
    }

    fn close(&mut self, out: &mut Vec<Event>) {
        self.flush_held(out);
    }

    fn holds_entry(&self) -> bool {
        self.streams.iter().any(StreamState::holds)
    }

    fn hold_at_shutdown(&self) -> bool {
        true
    }

    fn reset(&mut self) {
        let held = self.streams.iter().filter(|s| s.run.is_some()).count();
        if held > 0 {
            self.telemetry.count(FRAMES_DROPPED, held as f64, &[("reason", "truncated")]);
        }
        self.streams = Default::default();
    }

    fn resource(&self) -> Arc<Resource> {
        self.resource.clone()
    }
}

/// The resource a pods-layout path names (section 7), or `None` when it isn't one. A rotated
/// file's is its live file's: the rotation suffix carries nothing.
pub(crate) fn path_resource(path: &Path) -> Option<Arc<Resource>> {
    let container_dir = path.parent();
    let restart_count = name_of(Some(path)).and_then(log_restart_count)?;
    let container = name_of(container_dir).filter(|n| is_container_dir(n))?;
    let (namespace, pod, uid) =
        name_of(container_dir.and_then(Path::parent)).and_then(pod_dir_parts)?;
    let mut attrs = AttrMap::new();
    attrs.insert("k8s.namespace.name", namespace);
    attrs.insert("k8s.pod.name", pod);
    attrs.insert("k8s.pod.uid", uid);
    attrs.insert("k8s.container.name", container);
    attrs.insert("k8s.container.restart_count", Value::I64(restart_count));
    Some(Arc::new(Resource { attributes: attrs, ..Default::default() }))
}

/// Builds a [`CriDecoder`] for each path [`PathPattern::pod_logs`] matches. Every matched path is
/// accepted: `cri_in` has no selection (section 10).
struct CriDecoderFactory {
    max_line_bytes: usize,
    partial_timeout: Duration,
    diag: Diagnostics,
    telemetry: Telemetry,
}

impl DecoderFactory<CriDecoder> for CriDecoderFactory {
    fn accept(&mut self, _path: &Path) -> bool {
        true
    }

    // Each file is decoded as CRI. cri-dockerd's Docker-format files (section 9) need a per-file
    // format latch on the first byte, which belongs here and in `CriDecoder::decode_line`.
    fn open(&mut self, path: &Path) -> anyhow::Result<CriDecoder> {
        let resource = path_resource(path)
            .ok_or_else(|| anyhow::anyhow!("{}: not a kubelet pod log path", path.display()))?;
        Ok(CriDecoder::new(resource, self.max_line_bytes, self.partial_timeout)
            .with_diagnostics(self.diag.clone())
            .with_telemetry(self.telemetry.clone()))
    }
}

/// `cri_in`: tails every container log under `root` on `tail_in`'s [`Tailer`] driver. See this
/// module's doc.
pub struct CriInput {
    inner: Tailer<CriDecoder, CriDecoderFactory>,
}

impl CriInput {
    /// `config.max_line_bytes` is the operator's, bounding a reassembled line; the splitter gets
    /// `envelope_cap` of it (section 4), so every runtime's piece reaches the decoder.
    pub fn new(root: PathBuf, partial_timeout: Duration, config: TailConfig) -> Self {
        let factory = CriDecoderFactory {
            max_line_bytes: config.max_line_bytes,
            partial_timeout,
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
        };
        let config = TailConfig { max_line_bytes: envelope_cap(config.max_line_bytes), ..config };
        Self { inner: Tailer::new(vec![PathPattern::pod_logs(root)], factory, config) }
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.inner.factory_mut().diag = diag.clone();
        self.inner = self.inner.with_diagnostics(diag);
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.inner.factory_mut().telemetry = telemetry.clone();
        self.inner = self.inner.with_telemetry(telemetry);
        self
    }

    /// The driver's knobs, for tests. `max_line_bytes` is the splitter's bound.
    pub fn config(&self) -> &TailConfig {
        self.inner.config()
    }
}

#[async_trait::async_trait]
impl logit_pipeline::Input for CriInput {
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

#[cfg(test)]
mod tests;
