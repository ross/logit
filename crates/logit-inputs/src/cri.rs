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
//!   `k8s.container.name`, and `k8s.container.restart_count` (an `i64`), from the path, and
//!   `container.id` when [`ContainerIds`] knows it. Each event carries `log.iostream`.
//! - **`container.id`** (section 8): read from the kubelet's `containers_dir` symlinks by
//!   [`ContainerIds`]. A file that gains its id swaps in a new resource through
//!   [`DecoderFactory::refresh`], so no batch mixes the two.
//! - **Counters** of its own: `logit.input.frames.dropped{reason="oversize"|"truncated"|"malformed"}`
//!   and `logit.input.frames.truncated{reason="partial_timeout"}` ([`CriDecoder`]). **Diagnostics**
//!   of its own: `bad_time`, `long_line`, and `container_id_error`, beside the driver's `bad_line`
//!   and `invalid_utf8`.

use crate::docker::{envelope_cap, DockerDecoder};
use crate::tail::{
    ensure_utf8, is_container_dir, log_restart_count, name_of, pod_dir_parts, DecoderFactory,
    PathPattern, Refresh, TailConfig, TailDecoder, Tailer,
};
use bytes::Bytes;
use logit_core::{AttrMap, BodyFormat, Diagnostics, Event, LogRecord, Resource, Telemetry, Value};
use logit_pipeline::Fanout;
use logit_proto::cri::{self, Stream};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::OsString;
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

/// A file's format, latched from its first byte (section 9).
enum Format {
    /// No byte at offset 0 yet.
    Undecided,
    Cri,
    /// cri-dockerd's file: Docker json-file, decoded by `docker_in`'s decoder.
    Docker(DockerDecoder),
}

/// The byte a Docker json-file line opens with.
const DOCKER_FIRST_BYTE: u8 = b'{';

/// `cri_in`'s [`TailDecoder`]: reassembles each stream's `P` run into one line, and stamps each
/// event with the file's resource and its stream as `log.iostream`. A file whose first byte is
/// `{` is cri-dockerd's Docker json-file instead, and goes to a [`DockerDecoder`] under this
/// file's resource, with this decoder's diagnostics, telemetry, and `partial_timeout`; the rest of
/// this doc is the CRI branch's.
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
    format: Format,
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
            format: Format::Undecided,
        }
    }

    /// Replaces the resource, the Docker branch's included.
    fn set_resource(&mut self, resource: Arc<Resource>) {
        if let Format::Docker(docker) = &mut self.format {
            docker.set_resource(resource.clone());
        }
        self.resource = resource;
    }

    /// Latches the format from the file's first byte. The same format again keeps the state.
    fn latch(&mut self, first: u8) {
        match (&self.format, first == DOCKER_FIRST_BYTE) {
            (Format::Cri, false) | (Format::Docker(_), true) => {}
            (_, false) => self.format = Format::Cri,
            (_, true) => {
                let docker = DockerDecoder::new(self.resource.clone(), self.max_line_bytes)
                    .with_diagnostics(self.diag.clone())
                    .with_telemetry(self.telemetry.clone())
                    .with_partial_timeout(self.partial_timeout);
                self.format = Format::Docker(docker);
            }
        }
    }

    /// Latches from `first` while undecided. [`TailDecoder::file_head`] latches before any line
    /// reaches the decoder, so this decides only for a caller that skips it.
    fn decide(&mut self, first: Option<u8>) {
        if let (Format::Undecided, Some(first)) = (&self.format, first) {
            self.latch(first);
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
        self.decide(line.first().copied());
        if let Format::Docker(docker) = &mut self.format {
            // The CRI branch's raw bytes: the Docker branch converts each envelope whole, as
            // `docker_in`'s driver does.
            return docker.decode_line(ensure_utf8(line, &mut self.diag), read_at, out);
        }
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
    fn line_dropped(&mut self, head: &[u8], start: u64, out: &mut Vec<Event>) -> bool {
        self.decide(head.first().copied());
        if let Format::Docker(docker) = &mut self.format {
            return docker.line_dropped(head, start, out);
        }
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
        if let Format::Docker(docker) = &mut self.format {
            return docker.flush_idle(now, out);
        }
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

    fn file_head(&mut self, head: &[u8]) {
        if let Some(&first) = head.first() {
            self.latch(first);
        }
    }

    fn close(&mut self, out: &mut Vec<Event>) {
        match &mut self.format {
            Format::Docker(docker) => docker.close(out),
            _ => self.flush_held(out),
        }
    }

    fn holds_entry(&self) -> bool {
        match &self.format {
            Format::Docker(docker) => docker.holds_entry(),
            _ => self.streams.iter().any(StreamState::holds),
        }
    }

    fn hold_at_shutdown(&self) -> bool {
        true
    }

    /// The format stays latched until the truncated file's new first byte reaches
    /// [`TailDecoder::file_head`].
    fn reset(&mut self) {
        if let Format::Docker(docker) = &mut self.format {
            return docker.reset();
        }
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

/// `resource` with `container.id` set to `id`.
fn with_container_id(resource: &Resource, id: &str) -> Arc<Resource> {
    let mut resource = resource.clone();
    resource.attributes.insert("container.id", id);
    Arc::new(resource)
}

/// The key a pod log path and a kubelet symlink's target share: `<pod dir>/<container>/<n>`, from
/// their last three path components, so a `root:` mounted at another path still matches. A rotated
/// file's key is its live file's.
fn id_key(path: &Path) -> Option<String> {
    let container_dir = path.parent();
    let restart_count = name_of(Some(path)).and_then(log_restart_count)?;
    let container = name_of(container_dir)?;
    let pod = name_of(container_dir.and_then(Path::parent))?;
    Some(format!("{pod}/{container}/{restart_count}"))
}

/// The container id in a kubelet symlink name (`<pod>_<namespace>_<container>-<id>.log`): the
/// text after the last `-` and before `.log`, when it is 64 lowercase hex characters. A name the
/// kubelet cut at 251 bytes has lost its id and yields `None`.
fn link_id(name: &str) -> Option<&str> {
    let (_, id) = name.strip_suffix(".log")?.rsplit_once('-')?;
    let hex = id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'));
    (id.len() == 64 && hex).then_some(id)
}

/// Container ids read from the kubelet's `containers_dir` symlinks (section 8), keyed by
/// [`id_key`].
///
/// An id is cached until no discovered file maps to its key, so the kubelet removing a link
/// after its container exits doesn't remove the id from that file's later lines, and a rotated
/// file shares its live file's. The directory is listed at most once per scan, on the first
/// lookup that misses, so a scan where every file has its id costs no `read_dir`. A name already
/// read isn't read again while its id is cached.
struct ContainerIds {
    dir: PathBuf,
    ids: BTreeMap<String, String>,
    /// Link name to key, for the links in `ids`.
    links: HashMap<OsString, String>,
    /// The keys looked up or retained this scan: what `end_scan` keeps.
    seen: BTreeSet<String>,
    listed: bool,
    diag: Diagnostics,
}

impl ContainerIds {
    fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            ids: BTreeMap::new(),
            links: HashMap::new(),
            seen: BTreeSet::new(),
            listed: false,
            diag: Diagnostics::default(),
        }
    }

    /// Marks `path`'s key as in use this scan.
    fn touch(&mut self, path: &Path) {
        if let Some(key) = id_key(path) {
            self.seen.insert(key);
        }
    }

    /// The id for `path`, listing the directory first if the cache misses and this scan hasn't
    /// listed it yet.
    fn id_for(&mut self, path: &Path) -> Option<String> {
        let key = id_key(path)?;
        self.seen.insert(key.clone());
        if !self.ids.contains_key(&key) && !self.listed {
            self.listed = true;
            self.list();
        }
        self.ids.get(&key).cloned()
    }

    /// Reads the links not yet read. A missing directory is an empty listing; any other failure
    /// is diagnosed `container_id_error`, keeps the cache, and counts nothing: no data is lost,
    /// only an attribute.
    fn list(&mut self) {
        let entries = match std::fs::read_dir(&self.dir) {
            Ok(entries) => entries,
            Err(err) => {
                self.listing_failed(&err);
                return;
            }
        };
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => {
                    self.listing_failed(&err);
                    continue;
                }
            };
            let name = entry.file_name();
            if self.links.contains_key(&name) {
                continue;
            }
            let Some(id) = name.to_str().and_then(link_id) else { continue };
            let target = match std::fs::read_link(entry.path()) {
                Ok(target) => target,
                Err(err) => {
                    self.listing_failed(&err);
                    continue;
                }
            };
            let Some(key) = id_key(&target) else { continue };
            self.ids.insert(key.clone(), id.to_string());
            self.links.insert(name, key);
        }
    }

    fn listing_failed(&mut self, err: &std::io::Error) {
        if err.kind() != std::io::ErrorKind::NotFound {
            self.diag
                .warn_throttled("container_id_error", format!("{}: {err}", self.dir.display()));
        }
    }

    /// Drops the ids no file used this scan, and opens the next scan's one listing.
    fn end_scan(&mut self) {
        let seen = std::mem::take(&mut self.seen);
        self.ids.retain(|key, _| seen.contains(key));
        let ids = &self.ids;
        self.links.retain(|_, key| ids.contains_key(key));
        self.listed = false;
    }
}

/// Builds a [`CriDecoder`] for each path [`PathPattern::pod_logs`] matches. Every matched path is
/// accepted: `cri_in` has no selection (section 10).
struct CriDecoderFactory {
    container_ids: ContainerIds,
    max_line_bytes: usize,
    partial_timeout: Duration,
    diag: Diagnostics,
    telemetry: Telemetry,
}

impl DecoderFactory<CriDecoder> for CriDecoderFactory {
    fn accept(&mut self, _path: &Path) -> bool {
        true
    }

    fn open(&mut self, path: &Path) -> anyhow::Result<CriDecoder> {
        let mut resource = path_resource(path)
            .ok_or_else(|| anyhow::anyhow!("{}: not a kubelet pod log path", path.display()))?;
        if let Some(id) = self.container_ids.id_for(path) {
            resource = with_container_id(&resource, &id);
        }
        Ok(CriDecoder::new(resource, self.max_line_bytes, self.partial_timeout)
            .with_diagnostics(self.diag.clone())
            .with_telemetry(self.telemetry.clone()))
    }

    /// Installs `container.id` on a file that lacked it when its link has appeared. The kubelet
    /// makes the link after the container starts, so the first lines can be read before it exists.
    fn refresh(&mut self, path: &Path, decoder: &mut CriDecoder) -> Refresh {
        if decoder.resource.attributes.get("container.id").is_some() {
            self.container_ids.touch(path);
            return Refresh::Unchanged;
        }
        let Some(id) = self.container_ids.id_for(path) else { return Refresh::Unchanged };
        let resource = with_container_id(&decoder.resource, &id);
        decoder.set_resource(resource);
        Refresh::Identity
    }

    fn retain(&mut self, path: &Path) {
        self.container_ids.touch(path);
    }

    fn end_scan(&mut self) {
        self.container_ids.end_scan();
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
    pub fn new(
        root: PathBuf,
        containers_dir: PathBuf,
        partial_timeout: Duration,
        config: TailConfig,
    ) -> Self {
        let factory = CriDecoderFactory {
            container_ids: ContainerIds::new(containers_dir),
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
        self.inner.factory_mut().container_ids.diag = diag.clone();
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
