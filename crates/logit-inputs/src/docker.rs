//! `docker_in`: tails Docker's json-file container logs (`<root>/<id>/<id>-json.log`) and stamps
//! per-container resource attributes read from the sibling `config.v2.json`, with no docker
//! socket and no HTTP client. It runs on `tail_in`'s driver (`crate::tail`), with
//! [`DockerDecoder`] in place of [`crate::tail::LineDecoder`] and
//! [`PathPattern::docker_containers`] in place of config-driven patterns. `receive:` takes the
//! tail listener's batch-assembly fields.
//!
//! - **Selection** ([`ContainerFilter`]): `containers:` entries match a container's name or an id
//!   prefix of at least 12 hex characters; `discover: true` follows every container, including
//!   later ones. Graph rule 27 rejects neither being set.
//! - **Identity**: the resource carries `container.id`, `container.name`, `container.image.name`,
//!   `container.image.tag`, and `container.label.<key>` for each key in `labels:`
//!   ([`ContainerMeta::resource`]). `config.v2.json` is re-read on a poll tick only when its stat
//!   changes ([`DockerDecoderFactory::refresh_cache`]). While it has never been read successfully
//!   (missing or malformed), lines still flow with a `container.id`-only resource and a
//!   `metadata_error` diagnostic.
//! - **Diagnostics** of its own: `bad_time` (read time used instead), `long_line`,
//!   `metadata_error`, and the info keys `container_renamed` and `container_deselected`.
//!
//! See `docs/adr/file-tailing-and-docker-json-logs.md` and
//! `docs/adr/docker-container-identity-and-minimal-watches.md`.

use crate::tail::{DecoderFactory, PathPattern, Refresh, TailConfig, TailDecoder, Tailer};
use anyhow::Context;
use bytes::Bytes;
use logit_core::{AttrMap, BodyFormat, Diagnostics, Event, LogRecord, Resource, Telemetry, Value};
use logit_pipeline::Fanout;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::watch as shutdown_watch;

/// Which containers under `root` this listener follows: only those named, unless `discover: true`
/// follows every one, including containers that appear after startup. The file-tailing ADR's
/// "Selection" section says why explicit is the default.
pub struct ContainerFilter {
    entries: Vec<String>,
    discover: bool,
}

impl ContainerFilter {
    pub fn new(entries: Vec<String>, discover: bool) -> Self {
        Self { entries, discover }
    }

    /// Whether an entry equals `name` or is an id prefix of `dir_name` (the full id). `name` is
    /// `None` while `config.v2.json` is unread, so only an id prefix can match then. Graph rule 27
    /// doesn't check an entry's shape; a shorter or non-hex entry never matches an id.
    fn matches(&self, dir_name: &str, name: Option<&str>) -> bool {
        if self.discover {
            return true;
        }
        self.entries
            .iter()
            .any(|entry| Some(entry.as_str()) == name || is_id_prefix(entry, dir_name))
    }
}

fn is_id_prefix(entry: &str, dir_name: &str) -> bool {
    entry.len() >= 12 && entry.bytes().all(|b| b.is_ascii_hexdigit()) && dir_name.starts_with(entry)
}

/// One container's identity and image reference, read from the sibling `config.v2.json` through
/// [`DockerDecoderFactory::refresh_cache`].
pub(crate) struct ContainerMeta {
    id: String,
    name: String,
    image: String,
    labels: BTreeMap<String, String>,
}

#[derive(serde::Deserialize)]
struct ConfigV2 {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Config")]
    config: ConfigV2Config,
}

#[derive(serde::Deserialize)]
struct ConfigV2Config {
    #[serde(rename = "Image")]
    image: String,
    #[serde(rename = "Labels", default)]
    labels: BTreeMap<String, String>,
}

impl ContainerMeta {
    /// Reads `dir/config.v2.json`, where `dir`'s name is the full container id. Fails if the file
    /// is missing, unreadable, or malformed; the caller degrades to an id-only resource.
    pub(crate) fn read(dir: &Path) -> anyhow::Result<Self> {
        let path = dir.join("config.v2.json");
        let bytes = std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
        let doc: ConfigV2 = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing {}", path.display()))?;
        let id = dir.file_name().and_then(|n| n.to_str()).unwrap_or_default().to_string();
        let name = doc.name.strip_prefix('/').unwrap_or(&doc.name).to_string();
        Ok(Self { id, name, image: doc.config.image, labels: doc.config.labels })
    }

    fn name(&self) -> &str {
        &self.name
    }

    /// Builds this container's resource: `container.id`, `container.name`,
    /// `container.image.name`, `container.image.tag` (when the reference has one), and
    /// `container.label.<key>` for each key in `label_keys` the container carries. Labels are
    /// opt-in (the file-tailing ADR's "Event and resource shape").
    pub(crate) fn resource(&self, label_keys: &[String]) -> Arc<Resource> {
        let mut attrs = AttrMap::new();
        attrs.insert("container.id", self.id.as_str());
        attrs.insert("container.name", self.name.as_str());
        let (image_name, image_tag) = split_image_ref(&self.image);
        attrs.insert("container.image.name", image_name.as_str());
        if let Some(tag) = &image_tag {
            attrs.insert("container.image.tag", tag.as_str());
        }
        for key in label_keys {
            if let Some(value) = self.labels.get(key) {
                attrs.insert(&format!("container.label.{key}"), value.as_str());
            }
        }
        Arc::new(Resource { attributes: attrs, ..Default::default() })
    }
}

/// Splits a Docker image reference into `(name, tag)`. A digest reference (`app@sha256:...`) has
/// no tag. Otherwise splits on the last `:`, unless a `/` follows it: that `:` introduces a
/// registry port (`registry:5000/app`), not a tag.
fn split_image_ref(image: &str) -> (String, Option<String>) {
    if image.contains('@') {
        return (image.to_string(), None);
    }
    match image.rfind(':') {
        Some(idx) if !image[idx + 1..].contains('/') => {
            (image[..idx].to_string(), Some(image[idx + 1..].to_string()))
        }
        _ => (image.to_string(), None),
    }
}

/// Where dockerd cuts a long line into json-file entries: `daemon/logger/copier.go`'s
/// `defaultBufSize`. The json-file driver isn't a `SizedLogger`, so the copier's default applies.
const DOCKERD_FRAGMENT_BYTES: usize = 16 * 1024;

/// An envelope's bytes beyond its escaped `log`: 75 fixed (`{"log":"`, the escaped `\n`,
/// `","stream":"stdout"`, `,"time":"`, an RFC 3339 time of at most 35 bytes, `"}`), and the
/// container's `attrs` object (`--log-opt labels`, `env`, and `tag`) in the rest.
const ENVELOPE_SLACK_BYTES: usize = 64 * 1024;

/// The `LineSplitter` bound on one json-file line, for a message bounded by `max_line_bytes`.
///
/// The splitter measures the envelope, and an envelope it drops never reaches the decoder, so
/// the bound has to pass every entry the decoder could keep: a dockerd fragment (at most
/// [`DOCKERD_FRAGMENT_BYTES`] raw) and any entry whose `log` fits `max_line_bytes`. JSON escaping
/// writes a raw byte as at most six (`\u00XX`), which gives `6 × max(max_line_bytes, 16 KiB)`
/// plus [`ENVELOPE_SLACK_BYTES`].
pub(crate) const fn envelope_cap(max_line_bytes: usize) -> usize {
    let m = if max_line_bytes > DOCKERD_FRAGMENT_BYTES {
        max_line_bytes
    } else {
        DOCKERD_FRAGMENT_BYTES
    };
    m.saturating_mul(6).saturating_add(ENVELOPE_SLACK_BYTES)
}

/// `stream` values by [`DockerDecoder::streams`] index.
const STREAMS: [&str; 2] = ["stdout", "stderr"];

/// A logical line dockerd cut into several json-file entries (an entry whose `log` doesn't end in
/// `\n` is a fragment), held across [`DockerDecoder::decode_line`] calls until the entry that
/// ends it.
///
/// Held per stream: dockerd copies stdout and stderr on separate goroutines
/// (`daemon/logger/copier.go`), and json-file records no partial id or ordinal, so a stdout
/// line's fragments and a stderr line's interleave at entry granularity. The stream is the only
/// thing in the file that tells them apart. The emitted `timestamp` and `attrs` are the latest
/// entry's.
#[derive(Default)]
struct PartialEntry {
    message: String,
    timestamp: i64,
    attrs: Vec<(String, String)>,
}

/// One stream's reassembly state.
#[derive(Default)]
struct StreamState {
    partial: Option<PartialEntry>,
    /// Set once a reassembly is dropped for exceeding `max_line_bytes`: the stream's later
    /// fragments are discarded uncounted until the entry that ends the line clears it, as in
    /// `LineSplitter`'s `dropping`.
    dropping: bool,
}

/// `docker_in`'s [`TailDecoder`]: decodes Docker's json-file envelope, reassembles split lines
/// per stream (see [`PartialEntry`]), and stamps every event with the container's resource, which
/// `DockerDecoderFactory::refresh` swaps in place on an identity change. Never parses the inner
/// application line in `log`; that is a downstream `json` transform's job.
///
/// `max_line_bytes` bounds the reassembled message and is checked before each append, so a held
/// reassembly never exceeds it. A `Malformed` entry flushes every stream's held fragment as its
/// own event before the error, so a fragment is never spliced across a rejected line; a drop in
/// progress stays in progress.
pub struct DockerDecoder {
    resource: Arc<Resource>,
    /// Indexed as [`STREAMS`].
    streams: [StreamState; 2],
    max_line_bytes: usize,
    diag: Diagnostics,
}

impl DockerDecoder {
    pub(crate) fn new(resource: Arc<Resource>, max_line_bytes: usize) -> Self {
        Self { resource, streams: Default::default(), max_line_bytes, diag: Diagnostics::default() }
    }

    pub(crate) fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    /// Emits every stream's held fragment as its own event, stdout first. A drop in progress is
    /// left as it is.
    fn flush_held(&mut self, out: &mut Vec<Event>) {
        for (index, stream) in STREAMS.iter().enumerate() {
            if let Some(partial) = self.streams[index].partial.take() {
                self.emit(partial.timestamp, stream, &partial.attrs, partial.message, out);
            }
        }
    }

    fn emit(
        &self,
        timestamp: i64,
        stream: &'static str,
        attrs: &[(String, String)],
        message: String,
        out: &mut Vec<Event>,
    ) {
        let mut event_attrs = AttrMap::new();
        for (k, v) in attrs {
            event_attrs.insert(k, v.as_str());
        }
        // After `attrs`: an `attrs` key named `log.iostream` must not replace the entry's stream.
        event_attrs.insert("log.iostream", stream);
        out.push(Event::log(
            timestamp,
            event_attrs,
            LogRecord {
                message: Value::str(message),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        ));
    }
}

/// One json-file entry. Every string is a `Cow`: `serde_json` borrows a string only when it has
/// no escapes, and dockerd writes `<`, `>`, `&`, and control bytes as `\u00XX` in `attrs` keys and
/// values as well as in `log`.
#[derive(serde::Deserialize)]
struct JsonFileLine<'a> {
    #[serde(borrow)]
    log: Cow<'a, str>,
    #[serde(borrow)]
    stream: Cow<'a, str>,
    #[serde(borrow)]
    time: Cow<'a, str>,
    #[serde(default, borrow)]
    attrs: Option<BTreeMap<Cow<'a, str>, Cow<'a, str>>>,
}

impl TailDecoder for DockerDecoder {
    fn decode_line(
        &mut self,
        line: Bytes,
        read_at: i64,
        out: &mut Vec<Event>,
    ) -> Result<Arc<Resource>, logit_proto::CodecError> {
        let entry: JsonFileLine = match serde_json::from_slice(&line) {
            Ok(entry) => entry,
            Err(err) => {
                self.flush_held(out);
                return Err(logit_proto::CodecError::Malformed(format!(
                    "docker json-file entry: {err}"
                )));
            }
        };
        let index = match &*entry.stream {
            "stdout" => 0,
            "stderr" => 1,
            other => {
                let err = logit_proto::CodecError::Malformed(format!(
                    "unknown docker log stream {other:?}"
                ));
                self.flush_held(out);
                return Err(err);
            }
        };
        let (text, is_complete) = match entry.log.strip_suffix('\n') {
            Some(text) => (text, true),
            None => (&*entry.log, false),
        };

        // Length before `time` and `attrs`: an entry discarded here never becomes an event, so it
        // pays for neither and can't report `bad_time`.
        let state = &mut self.streams[index];
        if state.dropping {
            if is_complete {
                state.dropping = false;
            }
            return Ok(self.resource.clone());
        }
        let held_len = state.partial.as_ref().map_or(0, |p| p.message.len());
        if held_len + text.len() > self.max_line_bytes {
            state.partial = None;
            state.dropping = !is_complete;
            self.diag.warn_throttled(
                "long_line",
                "a docker log line exceeded max_line_bytes and was dropped whole",
            );
            return Ok(self.resource.clone());
        }

        let timestamp = match logit_core::parse_rfc3339_to_nanos(&entry.time) {
            Ok(ts) => ts,
            Err(_) => {
                self.diag.warn_throttled(
                    "bad_time",
                    format!("unparseable docker log timestamp {:?}; using read time", entry.time),
                );
                read_at
            }
        };
        let attrs: Vec<(String, String)> = entry
            .attrs
            .map(|m| m.into_iter().map(|(k, v)| (k.into_owned(), v.into_owned())).collect())
            .unwrap_or_default();

        let state = &mut self.streams[index];
        if !is_complete {
            let held = state.partial.get_or_insert_with(PartialEntry::default);
            held.message.push_str(text);
            held.timestamp = timestamp;
            held.attrs = attrs;
            return Ok(self.resource.clone());
        }
        let mut message = state.partial.take().map(|p| p.message).unwrap_or_default();
        message.push_str(text);
        self.emit(timestamp, STREAMS[index], &attrs, message, out);
        Ok(self.resource.clone())
    }

    fn close(&mut self, out: &mut Vec<Event>) {
        self.flush_held(out);
    }

    fn holds_entry(&self) -> bool {
        self.streams.iter().any(|s| s.partial.is_some() || s.dropping)
    }

    fn reset(&mut self) {
        // Both halves: a stale `dropping` would swallow the new generation's first complete
        // entry, as a stale `partial` would splice into it.
        self.streams = Default::default();
    }

    fn resource(&self) -> Arc<Resource> {
        self.resource.clone()
    }
}

/// Enough of `config.v2.json`'s stat to notice a rewrite, whether in place or by the daemon's
/// usual tmp-file-plus-rename (which changes `ino`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ConfigStat {
    dev: u64,
    ino: u64,
    len: u64,
    mtime: (i64, i64),
}

impl ConfigStat {
    fn from_metadata(meta: &std::fs::Metadata) -> Self {
        use std::os::unix::fs::MetadataExt;
        Self {
            dev: meta.dev(),
            ino: meta.ino(),
            len: meta.len(),
            mtime: (meta.mtime(), meta.mtime_nsec()),
        }
    }
}

/// One container's currently-known name and resource, cached by [`DockerDecoderFactory`] between
/// `config.v2.json` re-reads.
struct Identity {
    name: String,
    resource: Arc<Resource>,
}

/// One container directory's cached `config.v2.json` read, refreshed by
/// [`DockerDecoderFactory::refresh_cache`].
#[derive(Default)]
struct CachedMeta {
    /// The stat of the last read attempt. `None` (the file couldn't be stat'd) never counts as
    /// unchanged, so a not-yet-existing config is retried every scan.
    stat: Option<ConfigStat>,
    /// `None` until a read succeeds; `open` uses [`id_only_resource`] until then.
    identity: Option<Identity>,
    /// Set on a failed read; gates `metadata_error` to once per failure rather than once per
    /// poll tick (`warn_throttled` counts every call even while it throttles the log line).
    failed: bool,
    /// The scan generation that last touched this entry; `end_scan` evicts the rest, bounding
    /// `meta` by containers on the host now rather than ever.
    seen: u64,
}

fn id_only_resource(container_dir: &Path) -> Arc<Resource> {
    let id = container_dir.file_name().and_then(|n| n.to_str()).unwrap_or_default();
    let mut attrs = AttrMap::new();
    attrs.insert("container.id", id);
    Arc::new(Resource { attributes: attrs, ..Default::default() })
}

/// Turns a discovered `<root>/<id>/<id>-json.log` path into a [`DockerDecoder`]:
/// [`accept`](DecoderFactory::accept) applies the [`ContainerFilter`] (never reading
/// `config.v2.json` under `discover: true`), [`open`](DecoderFactory::open) builds the resource,
/// and [`refresh`](DecoderFactory::refresh) keeps it current each scan. All three go through
/// [`refresh_cache`](DockerDecoderFactory::refresh_cache), so a poll tick costs one `stat` per
/// container. A container never read successfully is tailed with a `container.id`-only resource.
struct DockerDecoderFactory {
    filter: ContainerFilter,
    labels: Vec<String>,
    max_line_bytes: usize,
    diag: Diagnostics,
    meta: BTreeMap<PathBuf, CachedMeta>,
    generation: u64,
}

impl DockerDecoderFactory {
    /// Brings `dir`'s cached identity up to date, reading `config.v2.json` only when its stat
    /// changed. A failed read keeps the previous identity (or none) and is diagnosed only on the
    /// transition into failure.
    fn refresh_cache(&mut self, dir: &Path) {
        let stat = std::fs::metadata(dir.join("config.v2.json"))
            .ok()
            .map(|m| ConfigStat::from_metadata(&m));
        let entry = self.meta.entry(dir.to_path_buf()).or_default();
        entry.seen = self.generation;
        if stat.is_some() && stat == entry.stat {
            return; // unchanged since the last read
        }
        entry.stat = stat;
        match ContainerMeta::read(dir) {
            Ok(meta) => {
                entry.failed = false;
                let resource = meta.resource(&self.labels);
                // Keep the existing `Arc` when the rebuilt `Resource` compares equal: the daemon
                // rewrites `config.v2.json` for restart counts and healthchecks, and a fresh `Arc`
                // each time would force a spurious `ResourceChange` flush.
                let resource = match &entry.identity {
                    Some(existing) if existing.resource == resource => existing.resource.clone(),
                    _ => resource,
                };
                entry.identity = Some(Identity { name: meta.name().to_string(), resource });
            }
            Err(err) => {
                if !entry.failed {
                    self.diag
                        .warn_throttled("metadata_error", format!("{}: {err:#}", dir.display()));
                }
                entry.failed = true;
            }
        }
    }

    fn cached(&self, dir: &Path) -> Option<&Identity> {
        self.meta.get(dir).and_then(|entry| entry.identity.as_ref())
    }
}

impl DecoderFactory<DockerDecoder> for DockerDecoderFactory {
    fn accept(&mut self, path: &Path) -> bool {
        if self.filter.discover {
            return true; // selecting needs no config.v2.json read
        }
        let Some(container_dir) = path.parent() else { return false };
        let Some(dir_name) = container_dir.file_name().and_then(|n| n.to_str()) else {
            return false;
        };
        self.refresh_cache(container_dir);
        let name = self.cached(container_dir).map(|identity| identity.name.as_str());
        self.filter.matches(dir_name, name)
    }

    fn open(&mut self, path: &Path) -> anyhow::Result<DockerDecoder> {
        let container_dir = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("{}: no parent directory", path.display()))?;
        // Usually just a stat: `accept` already refreshed this path earlier in the same scan.
        self.refresh_cache(container_dir);
        let resource = match self.cached(container_dir) {
            Some(identity) => identity.resource.clone(),
            None => id_only_resource(container_dir),
        };
        Ok(DockerDecoder::new(resource, self.max_line_bytes).with_diagnostics(self.diag.clone()))
    }

    fn refresh(&mut self, path: &Path, decoder: &mut DockerDecoder) -> Refresh {
        let Some(container_dir) = path.parent() else { return Refresh::Unchanged };
        let Some(dir_name) = container_dir.file_name().and_then(|n| n.to_str()) else {
            return Refresh::Unchanged;
        };
        self.refresh_cache(container_dir);
        let Some(identity) = self.cached(container_dir) else { return Refresh::Unchanged };
        // Selection before identity: a container renamed out of `containers:` is closing, and
        // what `close_decoder` flushes must carry the identity those lines were read under, so its
        // resource is not swapped. Never taken under `discover: true`.
        if !self.filter.matches(dir_name, Some(identity.name.as_str())) {
            self.diag.info(
                "container_deselected",
                format!("{}: renamed out of the configured selection", path.display()),
            );
            return Refresh::Deselected;
        }
        if Arc::ptr_eq(&decoder.resource, &identity.resource) {
            return Refresh::Unchanged;
        }
        decoder.resource = identity.resource.clone();
        self.diag.info(
            "container_renamed",
            format!(
                "{}: container identity changed (name, image, or a watched label)",
                path.display()
            ),
        );
        Refresh::Identity
    }

    fn end_scan(&mut self) {
        let generation = self.generation;
        self.meta.retain(|_, entry| entry.seen == generation);
        self.generation += 1;
    }
}

/// `docker_in`: tails every selected container's json-file log under `root` on `tail_in`'s
/// [`Tailer`] driver. See this module's doc.
pub struct DockerInput {
    inner: Tailer<DockerDecoder, DockerDecoderFactory>,
}

impl DockerInput {
    pub fn new(
        root: PathBuf,
        filter: ContainerFilter,
        labels: Vec<String>,
        config: TailConfig,
    ) -> Self {
        let pattern = PathPattern::docker_containers(root);
        // The decoder bounds the reassembled message; the splitter only has to pass every
        // envelope that could carry part of one.
        let factory = DockerDecoderFactory {
            filter,
            labels,
            max_line_bytes: config.max_line_bytes,
            diag: Diagnostics::default(),
            meta: BTreeMap::new(),
            generation: 0,
        };
        let config = TailConfig { max_line_bytes: envelope_cap(config.max_line_bytes), ..config };
        Self { inner: Tailer::new(vec![pattern], factory, config) }
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

    /// The driver's knobs, for tests. `max_line_bytes` is the splitter's envelope bound, which
    /// `envelope_cap` derives from the operator's `max_line_bytes`.
    pub fn config(&self) -> &TailConfig {
        self.inner.config()
    }
}

#[async_trait::async_trait]
impl logit_pipeline::Input for DockerInput {
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
mod tests {
    use super::*;
    use crate::tail::test_support::scratch_dir;
    use crate::tail::{ReadFrom, TailBatching, WatchMode};
    use logit_pipeline::test_util::{
        assert_no_batch, fanout_channel, recv_batch, recv_events, spawn_input, wait_until,
        TelemetryProbe,
    };
    use std::io::Write;
    use std::time::Duration;

    fn decoder() -> DockerDecoder {
        DockerDecoder::new(Arc::new(Resource::default()), 1024 * 1024)
    }

    fn line(json: &str) -> Bytes {
        Bytes::copy_from_slice(json.as_bytes())
    }

    // -- DockerDecoder ------------------------------------------------------------------------

    #[test]
    fn a_json_file_line_becomes_a_log_event_with_the_docker_time_and_stream() {
        let mut d = decoder();
        let mut out = Vec::new();
        d.decode_line(
            line(
                r#"{"log":"INFO:     Started server process [1]\n","stream":"stderr","time":"2026-08-17T19:35:46.529536683Z"}"#,
            ),
            999, // read_at -- must not be used, since the embedded time is valid
            &mut out,
        )
        .unwrap();
        assert_eq!(out.len(), 1);
        let event = &out[0];
        assert_eq!(
            event.log.as_ref().unwrap().message.as_str(),
            Some("INFO:     Started server process [1]")
        );
        assert_eq!(event.attributes.get("log.iostream").and_then(|v| v.as_str()), Some("stderr"));
        assert_eq!(
            event.timestamp,
            logit_core::parse_rfc3339_to_nanos("2026-08-17T19:35:46.529536683Z").unwrap()
        );
    }

    #[test]
    fn a_bad_time_falls_back_to_read_time_and_reports_bad_time() {
        let mut d = decoder();
        let mut out = Vec::new();
        d.decode_line(
            line(r#"{"log":"hi\n","stream":"stdout","time":"not-a-time"}"#),
            42,
            &mut out,
        )
        .unwrap();
        assert_eq!(out[0].timestamp, 42);
    }

    #[test]
    fn an_unknown_stream_is_rejected_as_a_bad_line() {
        let mut d = decoder();
        let mut out = Vec::new();
        let err = d
            .decode_line(
                line(r#"{"log":"hi\n","stream":"weird","time":"2026-08-17T19:35:46.000000000Z"}"#),
                0,
                &mut out,
            )
            .unwrap_err();
        assert!(matches!(err, logit_proto::CodecError::Malformed(_)));
        assert!(out.is_empty());
    }

    #[test]
    fn malformed_json_is_rejected_as_a_bad_line() {
        let mut d = decoder();
        let mut out = Vec::new();
        assert!(d.decode_line(line("not json"), 0, &mut out).is_err());
    }

    #[test]
    fn entries_without_a_trailing_newline_are_reassembled_until_one_ends_in_a_newline() {
        let mut d = decoder();
        let mut out = Vec::new();
        d.decode_line(
            line(
                r#"{"log":"part one ","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z"}"#,
            ),
            0,
            &mut out,
        )
        .unwrap();
        assert!(out.is_empty(), "a fragment with no trailing newline must not emit yet");
        d.decode_line(
            line(
                r#"{"log":"part two\n","stream":"stdout","time":"2026-08-17T19:35:46.500000000Z"}"#,
            ),
            0,
            &mut out,
        )
        .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].log.as_ref().unwrap().message.as_str(), Some("part one part two"));
    }

    #[test]
    fn an_oversized_reassembly_is_dropped_whole_and_resumes_after_the_closing_entry() {
        let mut d = DockerDecoder::new(Arc::new(Resource::default()), 10); // tiny bound
        let mut out = Vec::new();
        d.decode_line(
            // 16 bytes, already over the 10-byte bound, no trailing newline.
            line(r#"{"log":"0123456789ABCDEF","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z"}"#),
            0,
            &mut out,
        )
        .unwrap();
        assert!(out.is_empty());
        d.decode_line(
            line(r#"{"log":"still dropping","stream":"stdout","time":"2026-08-17T19:35:46.100000000Z"}"#),
            0,
            &mut out,
        )
        .unwrap();
        assert!(out.is_empty());
        d.decode_line(
            line(r#"{"log":"closes now\n","stream":"stdout","time":"2026-08-17T19:35:46.200000000Z"}"#),
            0,
            &mut out,
        )
        .unwrap();
        assert!(out.is_empty(), "the whole dropped reassembly should never emit, even once closed");

        // A fresh line within the 10-byte bound decodes normally again.
        d.decode_line(
            line(r#"{"log":"ok\n","stream":"stdout","time":"2026-08-17T19:35:46.300000000Z"}"#),
            0,
            &mut out,
        )
        .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].log.as_ref().unwrap().message.as_str(), Some("ok"));
    }

    /// `holds_entry` is `true` from the first fragment until the closing one emits the message.
    #[test]
    fn holds_entry_from_the_first_fragment_until_the_closing_fragment() {
        let mut d = decoder();
        let mut out = Vec::new();
        let first = r#"{"log":"one-","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z"}"#;
        let second = r#"{"log":"two-","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z"}"#;
        assert!(!d.holds_entry());
        d.decode_line(line(first), 0, &mut out).unwrap();
        assert!(d.holds_entry());
        d.decode_line(line(second), 0, &mut out).unwrap();
        assert!(d.holds_entry());
        d.decode_line(
            line(r#"{"log":"end\n","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z"}"#),
            0,
            &mut out,
        )
        .unwrap();
        assert_eq!(out.len(), 1);
        assert!(!d.holds_entry());
    }

    #[test]
    fn a_partial_entry_is_emitted_on_close_rather_than_lost() {
        let mut d = decoder();
        let mut out = Vec::new();
        d.decode_line(
            line(
                r#"{"log":"dangling, no newline yet","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z"}"#,
            ),
            0,
            &mut out,
        )
        .unwrap();
        assert!(out.is_empty());
        d.close(&mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].log.as_ref().unwrap().message.as_str(), Some("dangling, no newline yet"));
    }

    #[test]
    fn attrs_object_entries_are_copied_verbatim_as_event_attributes() {
        let mut d = decoder();
        let mut out = Vec::new();
        d.decode_line(
            line(
                r#"{"log":"hi\n","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z","attrs":{"custom.key":"custom-value"}}"#,
            ),
            0,
            &mut out,
        )
        .unwrap();
        assert_eq!(
            out[0].attributes.get("custom.key").and_then(|v| v.as_str()),
            Some("custom-value")
        );
    }

    #[test]
    fn docker_in_never_parses_the_inner_application_line() {
        let mut d = decoder();
        let mut out = Vec::new();
        // The inner line looks like JSON but must stay an opaque string.
        d.decode_line(
            line(
                r#"{"log":"{\"level\":\"info\",\"msg\":\"hello\"}\n","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z"}"#,
            ),
            0,
            &mut out,
        )
        .unwrap();
        assert_eq!(
            out[0].log.as_ref().unwrap().message.as_str(),
            Some(r#"{"level":"info","msg":"hello"}"#)
        );
    }

    #[test]
    fn resource_arc_is_stable_across_lines_of_one_container() {
        let mut d = decoder();
        let mut out = Vec::new();
        let r1 = d
            .decode_line(
                line(r#"{"log":"a\n","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z"}"#),
                0,
                &mut out,
            )
            .unwrap();
        let r2 = d
            .decode_line(
                line(r#"{"log":"b\n","stream":"stdout","time":"2026-08-17T19:35:46.100000000Z"}"#),
                0,
                &mut out,
            )
            .unwrap();
        assert!(Arc::ptr_eq(&r1, &r2), "no resource_change should ever fire within one container");
    }

    // -- per-stream reassembly, Malformed flushes, and the bound -------------------------------

    /// One json-file entry on `stream` whose `log` is `log`.
    fn entry(stream: &str, log: &str) -> Bytes {
        Bytes::from(serde_json::json!({"log": log, "stream": stream, "time": TIME}).to_string())
    }

    const TIME: &str = "2026-08-17T19:35:46.000000000Z";

    /// Each event's message and `log.iostream`.
    fn message_streams(events: &[Event]) -> Vec<(String, String)> {
        events
            .iter()
            .map(|e| {
                (
                    e.log.as_ref().unwrap().message.as_str().unwrap().to_string(),
                    e.attributes.get("log.iostream").unwrap().as_str().unwrap().to_string(),
                )
            })
            .collect()
    }

    fn pairs(list: &[(&str, &str)]) -> Vec<(String, String)> {
        list.iter().map(|(m, s)| (m.to_string(), s.to_string())).collect()
    }

    #[test]
    fn a_malformed_entry_mid_reassembly_flushes_the_held_fragment_as_its_own_event() {
        let mut d = decoder();
        let mut out = Vec::new();
        d.decode_line(entry("stdout", "head-"), 0, &mut out).unwrap();
        assert!(d.decode_line(line("not json"), 0, &mut out).is_err());
        assert_eq!(message_streams(&out), pairs(&[("head-", "stdout")]));
        assert!(!d.holds_entry());

        out.clear();
        d.decode_line(entry("stdout", "tail\n"), 0, &mut out).unwrap();
        assert_eq!(message_streams(&out), pairs(&[("tail", "stdout")]), "nothing is spliced");
    }

    #[test]
    fn an_unknown_stream_mid_reassembly_flushes_the_held_fragment() {
        let mut d = decoder();
        let mut out = Vec::new();
        d.decode_line(entry("stderr", "head-"), 0, &mut out).unwrap();
        let err = d.decode_line(entry("stdin", "x\n"), 0, &mut out).unwrap_err();
        assert!(matches!(err, logit_proto::CodecError::Malformed(_)));
        assert_eq!(message_streams(&out), pairs(&[("head-", "stderr")]));
        assert!(!d.holds_entry());
    }

    #[test]
    fn a_malformed_entry_flushes_both_streams_held_fragments_stdout_first() {
        let mut d = decoder();
        let mut out = Vec::new();
        d.decode_line(entry("stderr", "e-"), 0, &mut out).unwrap();
        d.decode_line(entry("stdout", "o-"), 0, &mut out).unwrap();
        assert!(d.decode_line(line("{\"log\":"), 0, &mut out).is_err());
        assert_eq!(message_streams(&out), pairs(&[("o-", "stdout"), ("e-", "stderr")]));
    }

    #[test]
    fn a_malformed_entry_while_dropping_leaves_the_drop_in_place() {
        let mut d = DockerDecoder::new(Arc::new(Resource::default()), 10);
        let mut out = Vec::new();
        d.decode_line(entry("stdout", "0123456789ABCDEF"), 0, &mut out).unwrap();
        assert!(d.decode_line(line("not json"), 0, &mut out).is_err());
        assert!(out.is_empty());
        assert!(d.holds_entry(), "the drop is still in progress");
        d.decode_line(entry("stdout", "rest\n"), 0, &mut out).unwrap();
        assert!(out.is_empty(), "the dropped line's closing entry is discarded, not emitted");
        d.decode_line(entry("stdout", "ok\n"), 0, &mut out).unwrap();
        assert_eq!(message_streams(&out), pairs(&[("ok", "stdout")]));
    }

    /// dockerd copies stdout and stderr on separate goroutines, so their fragments interleave.
    #[test]
    fn interleaved_stdout_and_stderr_fragments_reassemble_per_stream() {
        let mut d = decoder();
        let mut out = Vec::new();
        d.decode_line(entry("stdout", "a"), 0, &mut out).unwrap();
        d.decode_line(entry("stderr", "b\n"), 0, &mut out).unwrap();
        d.decode_line(entry("stdout", "c\n"), 0, &mut out).unwrap();
        assert_eq!(message_streams(&out), pairs(&[("b", "stderr"), ("ac", "stdout")]));
    }

    #[test]
    fn an_oversized_stdout_reassembly_does_not_swallow_a_stderr_line() {
        let mut d = DockerDecoder::new(Arc::new(Resource::default()), 10);
        let mut out = Vec::new();
        d.decode_line(entry("stdout", "0123456789ABCDEF"), 0, &mut out).unwrap();
        d.decode_line(entry("stderr", "err\n"), 0, &mut out).unwrap();
        d.decode_line(entry("stdout", "still dropping\n"), 0, &mut out).unwrap();
        d.decode_line(entry("stdout", "ok\n"), 0, &mut out).unwrap();
        assert_eq!(message_streams(&out), pairs(&[("err", "stderr"), ("ok", "stdout")]));
    }

    #[test]
    fn a_completed_message_over_max_line_bytes_is_dropped_on_the_closing_entry() {
        let mut d = DockerDecoder::new(Arc::new(Resource::default()), 10);
        let mut out = Vec::new();
        d.decode_line(entry("stdout", "12345"), 0, &mut out).unwrap();
        d.decode_line(entry("stdout", "678901\n"), 0, &mut out).unwrap();
        assert!(out.is_empty(), "11 bytes reassembled is over the 10-byte bound");
        assert!(!d.holds_entry(), "the closing entry ends the line: nothing left to drop");
        d.decode_line(entry("stdout", "1234567890\n"), 0, &mut out).unwrap();
        assert_eq!(message_streams(&out), pairs(&[("1234567890", "stdout")]), "10 bytes fits");
    }

    #[test]
    fn an_attrs_key_named_log_iostream_does_not_override_the_stream() {
        let mut d = decoder();
        let mut out = Vec::new();
        d.decode_line(
            line(&format!(
                r#"{{"log":"hi\n","stream":"stderr","time":"{TIME}","attrs":{{"log.iostream":"stdout","k":"v"}}}}"#
            )),
            0,
            &mut out,
        )
        .unwrap();
        assert_eq!(message_streams(&out), pairs(&[("hi", "stderr")]));
        assert_eq!(out[0].attributes.get("k").and_then(|v| v.as_str()), Some("v"));
    }

    /// dockerd writes `&`, `<`, and `>` as `\u00XX`, and `serde_json` can't borrow an escaped
    /// string, so a borrowed `&str` key would reject every line of such a container.
    #[test]
    fn an_escaped_attrs_key_decodes_instead_of_rejecting_the_line() {
        let mut d = decoder();
        let mut out = Vec::new();
        d.decode_line(
            line(&format!(
                r#"{{"log":"hi\n","stream":"stdout","time":"{TIME}","attrs":{{"a&b":"<v>","a\"b":"q","a\nb":"n"}}}}"#
            )),
            0,
            &mut out,
        )
        .unwrap();
        assert_eq!(message_streams(&out), pairs(&[("hi", "stdout")]));
        let attrs = &out[0].attributes;
        assert_eq!(attrs.get("a&b").and_then(|v| v.as_str()), Some("<v>"));
        assert_eq!(attrs.get("a\"b").and_then(|v| v.as_str()), Some("q"));
        assert_eq!(attrs.get("a\nb").and_then(|v| v.as_str()), Some("n"));
    }

    /// While a line is being dropped, a checkpoint must not land inside it, so the decoder still
    /// reports it as held.
    #[test]
    fn holds_entry_stays_true_while_dropping_until_the_closing_fragment() {
        let mut d = DockerDecoder::new(Arc::new(Resource::default()), 10);
        let mut out = Vec::new();
        d.decode_line(entry("stdout", "0123456789ABCDEF"), 0, &mut out).unwrap();
        assert!(d.holds_entry());
        d.decode_line(entry("stdout", "more"), 0, &mut out).unwrap();
        assert!(d.holds_entry());
        d.decode_line(entry("stdout", "end\n"), 0, &mut out).unwrap();
        assert!(!d.holds_entry());
        assert!(out.is_empty());
    }

    #[test]
    fn close_emits_held_fragments_but_keeps_a_drop_in_progress() {
        let mut d = DockerDecoder::new(Arc::new(Resource::default()), 10);
        let mut out = Vec::new();
        d.decode_line(entry("stdout", "0123456789ABCDEF"), 0, &mut out).unwrap();
        d.decode_line(entry("stderr", "e-"), 0, &mut out).unwrap();
        d.close(&mut out);
        assert_eq!(message_streams(&out), pairs(&[("e-", "stderr")]));
        assert!(d.holds_entry(), "the stdout drop outlives close");
    }

    #[test]
    fn bad_time_is_not_reported_for_entries_discarded_while_dropping() {
        let diag = Diagnostics::new("test");
        let mut d =
            DockerDecoder::new(Arc::new(Resource::default()), 10).with_diagnostics(diag.clone());
        let mut out = Vec::new();
        let bad = |log: &str| {
            Bytes::from(
                serde_json::json!({"log": log, "stream": "stdout", "time": "nope"}).to_string(),
            )
        };
        d.decode_line(bad("0123456789ABCDEF"), 0, &mut out).unwrap();
        d.decode_line(bad("more"), 0, &mut out).unwrap();
        d.decode_line(bad("end\n"), 0, &mut out).unwrap();
        assert_eq!(diag.occurrences("bad_time"), 0);
        assert_eq!(diag.occurrences("long_line"), 1);
        d.decode_line(bad("ok\n"), 7, &mut out).unwrap();
        assert_eq!(diag.occurrences("bad_time"), 1);
        assert_eq!(out[0].timestamp, 7);
    }

    /// The worst envelope dockerd can write for one fragment fits the splitter's bound: 16 KiB of
    /// bytes it escapes six to one, the longest RFC 3339 time, `stderr`, and `attrs` filling the
    /// slack.
    #[test]
    fn envelope_cap_covers_a_fully_escaped_dockerd_fragment() {
        let time = "2026-08-17T19:35:46.529536683+05:00";
        assert_eq!(time.len(), 35);
        let log = format!("{}\n", "<".repeat(DOCKERD_FRAGMENT_BYTES));
        let bare = crate::docker_verification::envelope("stderr", &log, time, &[]);
        assert_eq!(bare.len(), 6 * DOCKERD_FRAGMENT_BYTES + 75);
        // `,"attrs":{"k":"<value>"}` is 17 bytes around the value.
        let value = "v".repeat(ENVELOPE_SLACK_BYTES - 77 - 17);
        let full = crate::docker_verification::envelope("stderr", &log, time, &[("k", &value)]);
        assert_eq!(full.len(), bare.len() + ENVELOPE_SLACK_BYTES - 77);
        assert!(full.len() <= envelope_cap(1), "{} > {}", full.len(), envelope_cap(1));

        let mut splitter = crate::tail::LineSplitter::new(envelope_cap(1));
        let mut lines = Vec::new();
        let stats = splitter.push(Bytes::from(format!("{full}\n")), |l, _| lines.push(l));
        assert_eq!(stats.dropped_lines, 0);
        let mut d = DockerDecoder::new(Arc::new(Resource::default()), DOCKERD_FRAGMENT_BYTES);
        let mut out = Vec::new();
        d.decode_line(lines.pop().unwrap(), 0, &mut out).unwrap();
        assert_eq!(out.len(), 1);
    }

    // -- split_image_ref / ContainerMeta / ContainerFilter -------------------------------------

    #[test]
    fn container_meta_strips_the_leading_slash_and_splits_image_tag_on_the_last_colon() {
        assert_eq!(split_image_ref("nginx:1.25"), ("nginx".to_string(), Some("1.25".to_string())));
        assert_eq!(split_image_ref("nginx"), ("nginx".to_string(), None));
        assert_eq!(
            split_image_ref("registry:5000/app"),
            ("registry:5000/app".to_string(), None),
            "a registry port must not be misread as a tag"
        );
        assert_eq!(
            split_image_ref("registry:5000/app:1.0"),
            ("registry:5000/app".to_string(), Some("1.0".to_string()))
        );
        assert_eq!(
            split_image_ref("app@sha256:abcdef0123456789"),
            ("app@sha256:abcdef0123456789".to_string(), None),
            "a digest reference has no separate tag"
        );
    }

    #[test]
    fn container_meta_read_strips_the_leading_slash_from_name() {
        let dir = scratch_dir("docker-meta");
        std::fs::write(
            dir.join("config.v2.json"),
            r#"{"Name":"/logit-demo-nginx","Config":{"Image":"nginx:1.25","Labels":{"com.example":"1"}}}"#,
        )
        .unwrap();
        let meta = ContainerMeta::read(&dir).unwrap();
        assert_eq!(meta.name, "logit-demo-nginx");
        assert_eq!(meta.image, "nginx:1.25");
        assert_eq!(meta.labels.get("com.example"), Some(&"1".to_string()));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn resource_carries_container_attrs_and_only_the_configured_labels() {
        let dir = scratch_dir("docker-resource");
        let id = "abc123def456abc123def456abc123def456abc123def456abc123def456ab";
        let container_dir = dir.join(id);
        std::fs::create_dir_all(&container_dir).unwrap();
        std::fs::write(
            container_dir.join("config.v2.json"),
            r#"{"Name":"/web","Config":{"Image":"nginx:1.25","Labels":{"team":"infra","secret":"shh"}}}"#,
        )
        .unwrap();
        let meta = ContainerMeta::read(&container_dir).unwrap();
        let resource = meta.resource(&["team".to_string()]);
        assert_eq!(resource.attributes.get("container.id").and_then(|v| v.as_str()), Some(id));
        assert_eq!(resource.attributes.get("container.name").and_then(|v| v.as_str()), Some("web"));
        assert_eq!(
            resource.attributes.get("container.image.name").and_then(|v| v.as_str()),
            Some("nginx")
        );
        assert_eq!(
            resource.attributes.get("container.image.tag").and_then(|v| v.as_str()),
            Some("1.25")
        );
        assert_eq!(
            resource.attributes.get("container.label.team").and_then(|v| v.as_str()),
            Some("infra")
        );
        assert!(
            resource.attributes.get("container.label.secret").is_none(),
            "an unconfigured label must not be exposed"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn filter_matches_by_name_or_by_hex_id_prefix_of_at_least_12_chars() {
        let filter =
            ContainerFilter::new(vec!["nginx".to_string(), "abc123def456".to_string()], false);
        let long_id = "deadbeef".repeat(8);
        assert!(filter.matches(&long_id, Some("nginx")), "name match");
        let prefixed_id = format!("abc123def456{}", "f".repeat(52));
        assert!(filter.matches(&prefixed_id, Some("other")), "id-prefix match");
        assert!(!filter.matches(&long_id, Some("other")), "neither name nor id-prefix matches");
    }

    #[test]
    fn discover_mode_matches_every_container_regardless_of_entries() {
        let filter = ContainerFilter::new(vec![], true);
        assert!(filter.matches("anything", None));
    }

    #[test]
    fn a_short_or_non_hex_entry_never_matches_by_id_prefix() {
        let filter = ContainerFilter::new(vec!["not-hex-12-chars".to_string()], false);
        assert!(!filter.matches(&format!("not-hex-12-chars{}", "0".repeat(48)), None));
    }

    #[test]
    fn open_with_missing_metadata_degrades_to_container_id_only_and_reports_metadata_error() {
        let root = scratch_dir("docker-factory-nometa");
        let id = "5".repeat(64);
        let dir = root.join(&id);
        std::fs::create_dir_all(&dir).unwrap();
        let log_path = dir.join(format!("{id}-json.log"));
        std::fs::write(&log_path, b"").unwrap();

        let mut factory = DockerDecoderFactory {
            filter: ContainerFilter::new(vec![], true),
            labels: vec![],
            max_line_bytes: 1024,
            diag: Diagnostics::new("test"),
            meta: BTreeMap::new(),
            generation: 0,
        };
        let decoder = factory.open(&log_path).expect("open should still succeed");
        let resource = decoder.resource();
        assert_eq!(
            resource.attributes.get("container.id").and_then(|v| v.as_str()),
            Some(id.as_str())
        );
        assert!(resource.attributes.get("container.name").is_none());

        std::fs::remove_dir_all(&root).ok();
    }

    // -- DockerInput, end to end ----------------------------------------------------------------

    fn messages(events: &[Event]) -> Vec<String> {
        events
            .iter()
            .map(|e| e.log.as_ref().unwrap().message.as_str().unwrap().to_string())
            .collect()
    }

    fn fast_tail_config() -> TailConfig {
        TailConfig {
            checkpoint_path: None,
            read_from: ReadFrom::Beginning,
            watch: WatchMode::Poll,
            poll_interval: Duration::from_millis(15),
            checkpoint_interval: Duration::from_secs(5),
            max_line_bytes: 1024 * 1024,
            batching: TailBatching {
                max_events: 1_000,
                max_bytes: 1024 * 1024,
                flush_interval: Duration::from_millis(15),
            },
        }
    }

    /// Builds `<root>/<id>/{config.v2.json, <id>-json.log}` and returns the log file's path,
    /// ready to `std::fs::write` lines into.
    fn container(root: &Path, id: &str, name: &str, image: &str) -> PathBuf {
        let dir = root.join(id);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("config.v2.json"),
            format!(r#"{{"Name":"/{name}","Config":{{"Image":"{image}","Labels":{{}}}}}}"#),
        )
        .unwrap();
        dir.join(format!("{id}-json.log"))
    }

    #[tokio::test]
    async fn explicit_mode_ignores_an_unlisted_container_and_discover_mode_follows_it() {
        let root = scratch_dir("docker-explicit");
        let log_a = container(&root, &"a".repeat(64), "wanted", "nginx:1.25");
        let log_b = container(&root, &"b".repeat(64), "unwanted", "nginx:1.25");
        std::fs::write(
            &log_a,
            format!(
                "{}\n",
                r#"{"log":"from a\n","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z"}"#
            ),
        )
        .unwrap();
        std::fs::write(
            &log_b,
            format!(
                "{}\n",
                r#"{"log":"from b\n","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z"}"#
            ),
        )
        .unwrap();

        let (fanout, mut rx) = fanout_channel(8);
        let filter = ContainerFilter::new(vec!["wanted".to_string()], false);
        let input = DockerInput::new(root.clone(), filter, vec![], fast_tail_config());
        let running = spawn_input(input, fanout).await;

        let events = recv_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["from a"]);
        // 4x the 15ms poll and flush ticks that would deliver an unlisted container's line.
        assert_no_batch(
            &mut rx,
            Duration::from_millis(60),
            "an unlisted container must never be tailed",
        )
        .await;
        running.stop().await;

        let (fanout2, mut rx2) = fanout_channel(8);
        let filter2 = ContainerFilter::new(vec![], true);
        let input2 = DockerInput::new(root.clone(), filter2, vec![], fast_tail_config());
        let running2 = spawn_input(input2, fanout2).await;
        let events2 = recv_events(&mut rx2, 2).await;
        let mut msgs = messages(&events2);
        msgs.sort();
        assert_eq!(msgs, vec!["from a", "from b"], "discover: true should follow every container");
        running2.stop().await;

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn a_recreated_container_with_a_new_id_is_picked_up_by_name() {
        let root = scratch_dir("docker-recreate");
        let old_id = "1".repeat(64);
        let log_old = container(&root, &old_id, "web", "nginx:1.25");
        std::fs::write(
            &log_old,
            format!(
                "{}\n",
                r#"{"log":"old\n","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z"}"#
            ),
        )
        .unwrap();

        let (fanout, mut rx) = fanout_channel(8);
        let filter = ContainerFilter::new(vec!["web".to_string()], false);
        let input = DockerInput::new(root.clone(), filter, vec![], fast_tail_config());
        let running = spawn_input(input, fanout).await;

        let events = recv_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["old"]);

        // Recreated: the old directory disappears and a new id appears under the same name.
        std::fs::remove_dir_all(root.join(&old_id)).unwrap();
        let new_id = "2".repeat(64);
        let log_new = container(&root, &new_id, "web", "nginx:1.25");
        std::fs::write(
            &log_new,
            format!(
                "{}\n",
                r#"{"log":"new\n","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z"}"#
            ),
        )
        .unwrap();

        let events2 = recv_events(&mut rx, 1).await;
        assert_eq!(messages(&events2), vec!["new"]);

        running.stop().await;
        std::fs::remove_dir_all(&root).ok();
    }

    // -- live container identity (docs/adr/docker-container-identity-and-minimal-watches.md) ---

    /// A `docker rename` takes effect on a fresh batch, never relabelling earlier lines.
    #[tokio::test]
    async fn a_rewritten_config_v2_json_changes_the_name_on_a_fresh_batch_boundary() {
        let root = scratch_dir("docker-identity-refresh");
        let id = "9".repeat(64);
        let log_path = container(&root, &id, "before", "nginx:1.25");
        std::fs::write(
            &log_path,
            format!(
                "{}\n",
                r#"{"log":"one\n","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z"}"#
            ),
        )
        .unwrap();

        let (fanout, mut rx) = fanout_channel(8);
        let filter = ContainerFilter::new(vec![], true);
        let mut probe = TelemetryProbe::new();
        let input = DockerInput::new(root.clone(), filter, vec![], fast_tail_config())
            .with_telemetry(probe.telemetry("docker", "docker_in", "listener"));
        let running = spawn_input(input, fanout).await;

        let batch1 = recv_batch(&mut rx).await;
        assert_eq!(messages(&batch1.events), vec!["one"]);
        assert_eq!(
            batch1.resource.attributes.get("container.name").and_then(|v| v.as_str()),
            Some("before")
        );

        // A `docker rename`, as seen on disk.
        std::fs::write(
            root.join(&id).join("config.v2.json"),
            r#"{"Name":"/after","Config":{"Image":"nginx:1.25","Labels":{}}}"#,
        )
        .unwrap();
        // Wait for a poll tick's `scan` to refresh the identity before the next line exists. A
        // flush-timer `drain` can decode a line before any `scan` runs, under the stale identity.
        // Refresh is bounded by `poll_interval`, so that isn't a bug, but this assertion needs the
        // refresh to have landed. The counter is bumped in the same call that swaps the resource.
        probe
            .wait_for("a scan to swap in the renamed identity", |t| {
                t.sum("logit.input.files.identity_changed", &[]) >= 1.0
            })
            .await;
        std::fs::OpenOptions::new()
            .append(true)
            .open(&log_path)
            .unwrap()
            .write_all(
                format!(
                    "{}\n",
                    r#"{"log":"two\n","stream":"stdout","time":"2026-08-17T19:35:47.000000000Z"}"#
                )
                .as_bytes(),
            )
            .unwrap();

        let batch2 = recv_batch(&mut rx).await;
        assert_eq!(messages(&batch2.events), vec!["two"]);
        assert_eq!(
            batch2.resource.attributes.get("container.name").and_then(|v| v.as_str()),
            Some("after")
        );
        assert!(
            !Arc::ptr_eq(&batch1.resource, &batch2.resource),
            "a renamed container must get a fresh resource Arc, never share the old one"
        );

        running.stop().await;
        std::fs::remove_dir_all(&root).ok();
    }

    /// A `config.v2.json` that appears late recovers the full resource on a later poll tick.
    #[tokio::test]
    async fn a_metadata_read_that_fails_then_succeeds_recovers_the_full_resource() {
        let root = scratch_dir("docker-identity-recover");
        let id = "a".repeat(64);
        let dir = root.join(&id);
        std::fs::create_dir_all(&dir).unwrap();
        // No config.v2.json yet.
        let log_path = dir.join(format!("{id}-json.log"));
        std::fs::write(
            &log_path,
            format!(
                "{}\n",
                r#"{"log":"one\n","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z"}"#
            ),
        )
        .unwrap();

        let (fanout, mut rx) = fanout_channel(8);
        let filter = ContainerFilter::new(vec![], true);
        let mut probe = TelemetryProbe::new();
        let input = DockerInput::new(root.clone(), filter, vec![], fast_tail_config())
            .with_telemetry(probe.telemetry("docker", "docker_in", "listener"));
        let running = spawn_input(input, fanout).await;

        let batch1 = recv_batch(&mut rx).await;
        assert_eq!(messages(&batch1.events), vec!["one"]);
        assert_eq!(batch1.resource.attributes.get("container.name"), None);
        assert_eq!(
            batch1.resource.attributes.get("container.id").and_then(|v| v.as_str()),
            Some(id.as_str())
        );

        // The config appears, as it does when Docker creates the directory first.
        std::fs::write(
            dir.join("config.v2.json"),
            r#"{"Name":"/recovered","Config":{"Image":"nginx:1.25","Labels":{}}}"#,
        )
        .unwrap();
        // Wait for a scan to swap in the recovered identity first; see the same wait in
        // `a_rewritten_config_v2_json_changes_the_name_on_a_fresh_batch_boundary`.
        probe
            .wait_for("a scan to swap in the recovered identity", |t| {
                t.sum("logit.input.files.identity_changed", &[]) >= 1.0
            })
            .await;
        std::fs::OpenOptions::new()
            .append(true)
            .open(&log_path)
            .unwrap()
            .write_all(
                format!(
                    "{}\n",
                    r#"{"log":"two\n","stream":"stdout","time":"2026-08-17T19:35:47.000000000Z"}"#
                )
                .as_bytes(),
            )
            .unwrap();

        let batch2 = recv_batch(&mut rx).await;
        assert_eq!(messages(&batch2.events), vec!["two"]);
        assert_eq!(
            batch2.resource.attributes.get("container.name").and_then(|v| v.as_str()),
            Some("recovered")
        );

        running.stop().await;
        std::fs::remove_dir_all(&root).ok();
    }

    /// A rewrite that reproduces the same `Resource` keeps the same `Arc`, so batching holds.
    #[tokio::test]
    async fn a_stat_changing_rewrite_that_reproduces_the_same_resource_keeps_the_same_arc() {
        let root = scratch_dir("docker-identity-nop-rewrite");
        let id = "b".repeat(64);
        let log_path = container(&root, &id, "steady", "nginx:1.25");
        std::fs::write(
            &log_path,
            format!(
                "{}\n",
                r#"{"log":"one\n","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z"}"#
            ),
        )
        .unwrap();

        let (fanout, mut rx) = fanout_channel(8);
        let filter = ContainerFilter::new(vec![], true);
        let mut probe = TelemetryProbe::new();
        let input = DockerInput::new(root.clone(), filter, vec![], fast_tail_config())
            .with_telemetry(probe.telemetry("docker", "docker_in", "listener"));
        let running = spawn_input(input, fanout).await;

        let batch1 = recv_batch(&mut rx).await;
        assert_eq!(messages(&batch1.events), vec!["one"]);

        // Same name/image/labels: a daemon rewrite for an untracked field.
        std::fs::write(
            root.join(&id).join("config.v2.json"),
            r#"{"Name":"/steady","Config":{"Image":"nginx:1.25","Labels":{}}}"#,
        )
        .unwrap();
        // Two poll wakes past the rewrite: `watch.wakes` is counted before its `scan` runs, so
        // the second one means the first has finished its `scan`, which re-reads a changed
        // `config.v2.json`.
        let polls = probe.sum("logit.input.watch.wakes", &[("source", "poll")]);
        probe
            .wait_for("a full poll scan after the rewrite", |t| {
                t.sum("logit.input.watch.wakes", &[("source", "poll")]) >= polls + 2.0
            })
            .await;
        assert_eq!(
            probe.sum("logit.input.files.identity_changed", &[]),
            0.0,
            "a rewrite reproducing the same resource is no identity change"
        );
        std::fs::OpenOptions::new()
            .append(true)
            .open(&log_path)
            .unwrap()
            .write_all(
                format!(
                    "{}\n",
                    r#"{"log":"two\n","stream":"stdout","time":"2026-08-17T19:35:47.000000000Z"}"#
                )
                .as_bytes(),
            )
            .unwrap();

        let batch2 = recv_batch(&mut rx).await;
        assert_eq!(messages(&batch2.events), vec!["two"]);
        assert!(
            Arc::ptr_eq(&batch1.resource, &batch2.resource),
            "a rewrite reproducing the same resource value must keep the existing Arc, not mint \
             a new one -- otherwise every untracked daemon rewrite would flush a batch that never \
             needed to split"
        );

        running.stop().await;
        std::fs::remove_dir_all(&root).ok();
    }

    // -- selection follows the rename (docs/adr/docker-container-identity-and-minimal-watches.md)

    /// A container renamed out of `containers:` stops flowing without draining to EOF.
    #[tokio::test]
    async fn a_container_renamed_out_of_the_explicit_selection_stops_flowing() {
        let root = scratch_dir("docker-deselect");
        let id = "c".repeat(64);
        let log_path = container(&root, &id, "wanted", "nginx:1.25");
        std::fs::write(
            &log_path,
            format!(
                "{}\n",
                r#"{"log":"one\n","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z"}"#
            ),
        )
        .unwrap();

        let (fanout, mut rx) = fanout_channel(8);
        let filter = ContainerFilter::new(vec!["wanted".to_string()], false);
        let mut probe = TelemetryProbe::new();
        let input = DockerInput::new(root.clone(), filter, vec![], fast_tail_config())
            .with_telemetry(probe.telemetry("docker", "docker_in", "listener"));
        let running = spawn_input(input, fanout).await;

        let events = recv_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["one"]);

        // Renamed out of `containers: ["wanted"]`.
        std::fs::write(
            root.join(&id).join("config.v2.json"),
            r#"{"Name":"/elsewhere","Config":{"Image":"nginx:1.25","Labels":{}}}"#,
        )
        .unwrap();
        // `files.open` is set only at the end of a `scan`, and the reap happens in a `drain`
        // after the `scan` that de-selects, so `0` is a later scan seeing the file gone. A line
        // written before the reap would be read, since a de-selected file drains to EOF.
        probe
            .wait_for("a scan after the de-selected file is reaped", |t| {
                t.gauge("logit.input.files.open", &[]) == Some(0.0)
            })
            .await;

        std::fs::OpenOptions::new()
            .append(true)
            .open(&log_path)
            .unwrap()
            .write_all(
                format!(
                    "{}\n",
                    r#"{"log":"two\n","stream":"stdout","time":"2026-08-17T19:35:47.000000000Z"}"#
                )
                .as_bytes(),
            )
            .unwrap();

        // 10x the 15ms poll and flush ticks that would pick the line up.
        assert_no_batch(
            &mut rx,
            Duration::from_millis(150),
            "a container renamed out of the selection must not keep flowing",
        )
        .await;

        running.stop().await;
        std::fs::remove_dir_all(&root).ok();
    }

    /// Renamed back in, a container resumes without replaying, delivering lines written meanwhile.
    #[tokio::test]
    async fn a_container_renamed_back_into_the_selection_resumes_without_replaying() {
        let root = scratch_dir("docker-reselect");
        let id = "d".repeat(64);
        let log_path = container(&root, &id, "wanted", "nginx:1.25");
        std::fs::write(
            &log_path,
            format!(
                "{}\n",
                r#"{"log":"one\n","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z"}"#
            ),
        )
        .unwrap();

        let (fanout, mut rx) = fanout_channel(8);
        let filter = ContainerFilter::new(vec!["wanted".to_string()], false);
        let mut probe = TelemetryProbe::new();
        let input = DockerInput::new(root.clone(), filter, vec![], fast_tail_config())
            .with_telemetry(probe.telemetry("docker", "docker_in", "listener"));
        let running = spawn_input(input, fanout).await;

        let events = recv_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["one"]);

        // Renamed away, then back, with a line written while away.
        std::fs::write(
            root.join(&id).join("config.v2.json"),
            r#"{"Name":"/elsewhere","Config":{"Image":"nginx:1.25","Labels":{}}}"#,
        )
        .unwrap();
        // See the same wait in `a_container_renamed_out_of_the_explicit_selection_stops_flowing`.
        probe
            .wait_for("a scan after the de-selected file is reaped", |t| {
                t.gauge("logit.input.files.open", &[]) == Some(0.0)
            })
            .await;
        std::fs::OpenOptions::new()
            .append(true)
            .open(&log_path)
            .unwrap()
            .write_all(
                format!(
                    "{}\n",
                    r#"{"log":"two\n","stream":"stdout","time":"2026-08-17T19:35:47.000000000Z"}"#
                )
                .as_bytes(),
            )
            .unwrap();
        // 6x the 15ms poll and flush ticks that would pick the line up.
        assert_no_batch(
            &mut rx,
            Duration::from_millis(100),
            "still renamed away -- nothing should arrive yet",
        )
        .await;

        std::fs::write(
            root.join(&id).join("config.v2.json"),
            r#"{"Name":"/wanted","Config":{"Image":"nginx:1.25","Labels":{}}}"#,
        )
        .unwrap();

        let events2 = recv_events(&mut rx, 1).await;
        assert_eq!(
            messages(&events2),
            vec!["two"],
            "must resume from the retained offset -- \"one\" must never be replayed"
        );

        running.stop().await;
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn rotated_json_log_1_is_never_opened_and_the_fresh_json_log_is_followed() {
        let root = scratch_dir("docker-rotate");
        let id = "3".repeat(64);
        let log = container(&root, &id, "web", "nginx:1.25");
        // A rotated file beside the real one must never match.
        std::fs::write(
            log.with_extension("log.1"),
            format!(
                "{}\n",
                r#"{"log":"stale\n","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z"}"#
            ),
        )
        .unwrap();
        std::fs::write(
            &log,
            format!(
                "{}\n",
                r#"{"log":"fresh\n","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z"}"#
            ),
        )
        .unwrap();

        let (fanout, mut rx) = fanout_channel(8);
        let filter = ContainerFilter::new(vec!["web".to_string()], false);
        let input = DockerInput::new(root.clone(), filter, vec![], fast_tail_config());
        let running = spawn_input(input, fanout).await;

        let events = recv_events(&mut rx, 1).await;
        assert_eq!(
            messages(&events),
            vec!["fresh"],
            "only the real <id>-json.log should ever be tailed"
        );

        running.stop().await;
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn missing_config_v2_json_reports_metadata_error_and_still_tails_with_container_id_only()
    {
        let root = scratch_dir("docker-nometa");
        let id = "4".repeat(64);
        let dir = root.join(&id);
        std::fs::create_dir_all(&dir).unwrap();
        // No config.v2.json written at all.
        std::fs::write(
            dir.join(format!("{id}-json.log")),
            format!(
                "{}\n",
                r#"{"log":"hi\n","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z"}"#
            ),
        )
        .unwrap();

        let (fanout, mut rx) = fanout_channel(8);
        // discover mode: `accept` needs no metadata, so a missing config.v2.json can't stop it.
        let filter = ContainerFilter::new(vec![], true);
        let input = DockerInput::new(root.clone(), filter, vec![], fast_tail_config());
        let running = spawn_input(input, fanout).await;

        let events = recv_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["hi"]);

        running.stop().await;
        std::fs::remove_dir_all(&root).ok();
    }

    /// Only `root` is watched, so a log file created inside an existing container directory is
    /// found on the next poll tick even under inotify (the identity ADR's "Watch set").
    #[tokio::test]
    async fn under_inotify_a_container_log_created_after_its_directory_is_discovered_only_on_the_poll_tick(
    ) {
        let root = scratch_dir("docker-inotify-new-log");
        let mut config = fast_tail_config();
        config.watch = WatchMode::Inotify;
        // Past the negative window below by over 6x, so scheduler lag can't carry the window into
        // the tick that is the only thing allowed to discover the log.
        config.poll_interval = Duration::from_secs(1);

        let (fanout, mut rx) = fanout_channel(8);
        let filter = ContainerFilter::new(vec![], true);
        let mut probe = TelemetryProbe::new();
        let input = DockerInput::new(root.clone(), filter, vec![], config)
            .with_telemetry(probe.telemetry("docker", "docker_in", "listener"));
        // Bound before it returns: the initial scan has armed `root`'s watch.
        let running = spawn_input(input, fanout).await;
        // Drains the initial scan's `files.open` now, so the wait below sees only a later scan's.
        let before = probe.poll().events.len();

        let id = "6".repeat(64);
        // Directory and config.v2.json only, as Docker creates them before the log file.
        let log_path = container(&root, &id, "web", "nginx:1.25");

        // Wait for `root`'s IN_CREATE to be handled; its `scan` finds no log file yet. The wake is
        // counted before its `scan` runs and `files.open` is set at the end of one, so the
        // two together mean that `scan` has finished. Nothing else scans before the 1s tick.
        probe
            .wait_for("the scan for root's IN_CREATE", |t| {
                t.sum("logit.input.watch.wakes", &[("source", "inotify")]) >= 1.0
                    && t.events[before..].iter().any(|e| {
                        e.metrics.iter().any(|m| {
                            logit_core::interner::resolve(m.name) == "logit.input.files.open"
                        })
                    })
            })
            .await;
        std::fs::write(
            &log_path,
            format!(
                "{}\n",
                r#"{"log":"hello\n","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z"}"#
            ),
        )
        .unwrap();

        // Within the 1s poll_interval and with nothing changed under `root`: not found yet. The
        // window is 6x the 15ms flush tick that would deliver the line of a discovered log.
        assert_no_batch(
            &mut rx,
            Duration::from_millis(100),
            "must not be discovered before the poll tick -- the container's own subdirectory \
             isn't watched, only root, and root saw no event",
        )
        .await;

        // The next poll tick picks it up.
        let events = recv_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["hello"]);

        running.stop().await;
        std::fs::remove_dir_all(&root).ok();
    }

    /// A tracked log's truncation is noticed before the poll tick: the file has its own watch,
    /// and `O_TRUNC` fires `IN_MODIFY` on it.
    #[tokio::test]
    async fn under_inotify_a_truncated_container_log_is_noticed_before_the_poll_interval() {
        let root = scratch_dir("docker-inotify-truncate");
        let id = "7".repeat(64);
        let log_path = container(&root, &id, "web", "nginx:1.25");
        std::fs::write(
            &log_path,
            format!(
                "{}\n",
                r#"{"log":"first\n","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z"}"#
            ),
        )
        .unwrap();

        let mut config = fast_tail_config();
        config.watch = WatchMode::Inotify;
        config.poll_interval = Duration::from_secs(30);

        let (fanout, mut rx) = fanout_channel(8);
        let filter = ContainerFilter::new(vec![], true);
        let input = DockerInput::new(root.clone(), filter, vec![], config);
        let running = spawn_input(input, fanout).await;

        let events = recv_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["first"]);

        // `std::fs::write` opens with `O_TRUNC`: same inode, shorter length, a truncation.
        std::fs::write(
            &log_path,
            format!(
                "{}\n",
                r#"{"log":"new\n","stream":"stdout","time":"2026-08-17T19:35:46.500000000Z"}"#
            ),
        )
        .unwrap();

        let events2 =
            tokio::time::timeout(Duration::from_secs(3), recv_events(&mut rx, 1)).await.expect(
                "inotify should notice the truncation well within 3s, nowhere near the 30s \
                 poll_interval",
            );
        assert_eq!(messages(&events2), vec!["new"]);

        running.stop().await;
        std::fs::remove_dir_all(&root).ok();
    }

    /// A truncation discards `DockerDecoder`'s held `partial`, not just `LineSplitter`'s.
    #[tokio::test]
    async fn a_truncation_discards_a_docker_partial_entry_held_from_the_previous_generation() {
        let root = scratch_dir("docker-truncate-partial");
        let id = "8".repeat(64);
        let log_path = container(&root, &id, "web", "nginx:1.25");
        // A fragment (no trailing `\n` in `log`) padded long enough that the post-truncation
        // generation is unambiguously shorter.
        let padding = "x".repeat(48);
        let gen1 = format!(
            r#"{{"log":"partial-{padding}","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z"}}"#
        ) + "\n";
        std::fs::write(&log_path, &gen1).unwrap();

        let (fanout, mut rx) = fanout_channel(8);
        let filter = ContainerFilter::new(vec![], true);
        let mut probe = TelemetryProbe::new();
        let input = DockerInput::new(root.clone(), filter, vec![], fast_tail_config())
            .with_telemetry(probe.telemetry("docker", "docker_in", "listener"));
        let running = spawn_input(input, fanout).await;

        // The tailer has read the fragment's line, then nothing emits for 4x the 15ms flush tick.
        probe
            .wait_for("the fragment's line to be read", |t| t.sum("logit.input.lines", &[]) >= 1.0)
            .await;
        assert_no_batch(
            &mut rx,
            Duration::from_millis(60),
            "an unterminated fragment must not emit before its close",
        )
        .await;

        let gen2 = format!(
            "{}\n",
            r#"{"log":"restarted\n","stream":"stdout","time":"2026-08-17T19:35:47.000000000Z"}"#
        );
        assert!(
            gen2.len() < gen1.len(),
            "fixture must be a genuine truncation: gen2 ({}) must be shorter than gen1 ({})",
            gen2.len(),
            gen1.len()
        );
        // `std::fs::write` opens with `O_TRUNC`: same inode, shorter length.
        std::fs::write(&log_path, &gen2).unwrap();

        let events = recv_events(&mut rx, 1).await;
        assert_eq!(
            messages(&events),
            vec!["restarted"],
            "the pre-truncation partial must not be spliced onto the first post-truncation entry"
        );

        running.stop().await;
        assert!(
            rx.try_recv().is_err(),
            "the discarded partial must not resurface on close -- proves reset() ran, not close()"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    /// A truncation clears a stale `dropping`, so the next generation's first entry survives.
    #[tokio::test]
    async fn a_truncation_clears_a_docker_dropping_state_so_the_next_generation_is_not_swallowed() {
        let root = scratch_dir("docker-truncate-dropping");
        let id = "9".repeat(64);
        let log_path = container(&root, &id, "web", "nginx:1.25");

        // Three unterminated fragments whose reassembly passes `max_line_bytes` (200) and leaves
        // the decoder `dropping`.
        let fragment = "y".repeat(80);
        let mut gen1 = String::new();
        for i in 0..3 {
            let entry = format!(
                r#"{{"log":"{fragment}-{i}","stream":"stdout","time":"2026-08-17T19:35:46.00000000{i}Z"}}"#
            );
            gen1.push_str(&entry);
            gen1.push('\n');
        }
        assert!(
            fragment.len() * 3 > 200,
            "the accumulated fragments must exceed max_line_bytes for `dropping` to engage"
        );
        std::fs::write(&log_path, &gen1).unwrap();

        let mut config = fast_tail_config();
        config.max_line_bytes = 200;
        let (fanout, mut rx) = fanout_channel(8);
        let filter = ContainerFilter::new(vec![], true);
        let mut probe = TelemetryProbe::new();
        let input = DockerInput::new(root.clone(), filter, vec![], config)
            .with_telemetry(probe.telemetry("docker", "docker_in", "listener"));
        let running = spawn_input(input, fanout).await;

        // All three fragment lines read, then nothing emits for 4x the 15ms flush tick.
        probe
            .wait_for("all three fragment lines to be read", |t| {
                t.sum("logit.input.lines", &[]) >= 3.0
            })
            .await;
        assert_no_batch(
            &mut rx,
            Duration::from_millis(60),
            "a dropped oversized reassembly must never emit",
        )
        .await;

        let gen2 = format!(
            "{}\n",
            r#"{"log":"ok\n","stream":"stdout","time":"2026-08-17T19:35:47.000000000Z"}"#
        );
        assert!(gen2.len() < gen1.len(), "fixture must be a genuine truncation");
        std::fs::write(&log_path, &gen2).unwrap();

        let events = recv_events(&mut rx, 1).await;
        assert_eq!(
            messages(&events),
            vec!["ok"],
            "a stale dropping flag must not swallow the next generation's first complete entry"
        );

        running.stop().await;
        std::fs::remove_dir_all(&root).ok();
    }

    // -- a held fragment and the checkpoint --

    /// One json-file line (with its `\n`) whose `log` is `log`.
    fn json_file_line(log: &str) -> String {
        format!(
            "{}\n",
            serde_json::json!({"log": log, "stream": "stdout", "time": "2026-08-17T19:35:46.000000000Z"})
        )
    }

    /// The first file's offset in the checkpoint at `path`, if one is written.
    fn checkpointed_offset(path: &Path) -> Option<u64> {
        let text = std::fs::read_to_string(path).ok()?;
        serde_json::from_str::<serde_json::Value>(&text).ok()?["files"][0]["offset"].as_u64()
    }

    /// The first file's offset in the checkpoint at `path`, once one with a nonzero offset is
    /// written (a tick before the first read records `0`).
    async fn first_nonzero_checkpoint(path: &Path) -> u64 {
        let mut found = None;
        wait_until("a checkpoint with a nonzero offset", || {
            found = checkpointed_offset(path).filter(|&offset| offset > 0);
            found.is_some()
        })
        .await;
        found.expect("the wait returns only once an offset is found")
    }

    /// A container log holding a complete entry, then a fragment line (one Docker writes for a
    /// line over 16 KiB) with no closing fragment yet, then `after`; and a checkpoint config with
    /// a short interval. Returns the fragment line's file offset among the rest.
    fn fragment_fixture(label: &str, after: &str) -> (PathBuf, PathBuf, PathBuf, u64, TailConfig) {
        let root = scratch_dir(label);
        let log = container(&root, &"f".repeat(64), "frag", "nginx:1.25");
        let whole = json_file_line("whole\n");
        let head = json_file_line("head-");
        std::fs::write(&log, format!("{whole}{head}{after}")).unwrap();
        let checkpoint = root.join("checkpoint.json");
        let mut config = fast_tail_config();
        config.checkpoint_path = Some(checkpoint.clone());
        config.checkpoint_interval = Duration::from_millis(30);
        (root, log, checkpoint, whole.len() as u64, config)
    }

    /// `DockerDecoder` holds a fragment line in `partial` until its closing fragment arrives, and
    /// the line itself is complete, so the splitter holds nothing. The interval checkpoint must
    /// still stop before it: nothing has been emitted for it.
    #[tokio::test]
    async fn an_interval_checkpoint_never_covers_a_held_fragment_line() {
        let (root, _log, checkpoint, whole_len, config) =
            fragment_fixture("docker-held-checkpoint", "");

        let (fanout, mut rx) = fanout_channel(8);
        let input =
            DockerInput::new(root.clone(), ContainerFilter::new(vec![], true), vec![], config);
        let running = spawn_input(input, fanout).await;

        assert_eq!(messages(&recv_events(&mut rx, 1).await), vec!["whole"]);
        assert_eq!(
            first_nonzero_checkpoint(&checkpoint).await,
            whole_len,
            "the checkpoint must end before the held fragment line"
        );

        running.stop().await;
        std::fs::remove_dir_all(&root).ok();
    }

    /// A crash (no shutdown, so no `close` and no final checkpoint) while a fragment is held, then
    /// the closing fragment, then a restart: the restart re-reads the held fragment, so the whole
    /// message arrives, not only its tail.
    #[tokio::test]
    async fn a_crash_before_the_closing_fragment_replays_the_whole_message_after_restart() {
        let (root, log, checkpoint, whole_len, config) = fragment_fixture("docker-held-crash", "");

        let (fanout, mut rx) = fanout_channel(8);
        let input = DockerInput::new(
            root.clone(),
            ContainerFilter::new(vec![], true),
            vec![],
            config.clone(),
        );
        let running = spawn_input(input, fanout).await;
        assert_eq!(messages(&recv_events(&mut rx, 1).await), vec!["whole"]);
        assert_eq!(first_nonzero_checkpoint(&checkpoint).await, whole_len);
        running.handle.abort();
        let _ = running.handle.await;

        std::fs::OpenOptions::new()
            .append(true)
            .open(&log)
            .unwrap()
            .write_all(json_file_line("tail\n").as_bytes())
            .unwrap();

        let (fanout2, mut rx2) = fanout_channel(8);
        let input2 =
            DockerInput::new(root.clone(), ContainerFilter::new(vec![], true), vec![], config);
        let running2 = spawn_input(input2, fanout2).await;
        assert_eq!(
            messages(&recv_events(&mut rx2, 1).await),
            vec!["head-tail"],
            "the restart must reassemble the message from its first fragment"
        );
        running2.stop().await;

        std::fs::remove_dir_all(&root).ok();
    }

    /// A line the decoder rejects, read after a held fragment, flushes the fragment as its own
    /// event, and the checkpoint moves past both once nothing is held. The closing fragment then
    /// arrives alone and is emitted as its own message.
    #[tokio::test]
    async fn a_rejected_line_after_a_held_fragment_emits_it_and_releases_the_checkpoint() {
        let rejected = "this is not a json-file entry\n";
        let (root, log, checkpoint, whole_len, config) =
            fragment_fixture("docker-held-rejected", rejected);
        let head_len = json_file_line("head-").len() as u64;

        let (fanout, mut rx) = fanout_channel(8);
        let input =
            DockerInput::new(root.clone(), ContainerFilter::new(vec![], true), vec![], config);
        let running = spawn_input(input, fanout).await;

        assert_eq!(messages(&recv_events(&mut rx, 2).await), vec!["whole", "head-"]);
        assert_eq!(
            first_nonzero_checkpoint(&checkpoint).await,
            whole_len + head_len + rejected.len() as u64,
            "nothing is held once the rejected line flushed the fragment"
        );

        std::fs::OpenOptions::new()
            .append(true)
            .open(&log)
            .unwrap()
            .write_all(json_file_line("tail\n").as_bytes())
            .unwrap();
        assert_eq!(messages(&recv_events(&mut rx, 1).await), vec!["tail"]);

        running.stop().await;
        std::fs::remove_dir_all(&root).ok();
    }

    /// A json-file line the splitter drops as over `envelope_cap`, read after a held fragment,
    /// never reaches the decoder, and the checkpoint still stops at the fragment's start. It pins
    /// the gap in `docs/known-gaps.md`'s "An envelope over the cap is dropped by the splitter"
    /// entry: a splitter-dropped envelope is invisible to the decoder, so the held fragment and
    /// the next closing entry are joined across it.
    #[tokio::test]
    async fn an_oversized_line_dropped_after_a_held_fragment_keeps_the_checkpoint_at_the_fragment_start(
    ) {
        let oversized = format!("{}\n", "x".repeat(envelope_cap(200) + 1));
        let (root, log, checkpoint, head_start, mut config) =
            fragment_fixture("docker-held-oversized", &oversized);
        config.max_line_bytes = 200;

        let (fanout, mut rx) = fanout_channel(8);
        let input =
            DockerInput::new(root.clone(), ContainerFilter::new(vec![], true), vec![], config);
        let running = spawn_input(input, fanout).await;

        assert_eq!(messages(&recv_events(&mut rx, 1).await), vec!["whole"]);
        assert_eq!(
            first_nonzero_checkpoint(&checkpoint).await,
            head_start,
            "the checkpoint must stop at the held fragment's first byte, not inside it"
        );

        std::fs::OpenOptions::new()
            .append(true)
            .open(&log)
            .unwrap()
            .write_all(json_file_line("tail\n").as_bytes())
            .unwrap();
        assert_eq!(messages(&recv_events(&mut rx, 1).await), vec!["head-tail"]);
        // 4x the 15ms flush tick that would deliver a second copy.
        assert_no_batch(&mut rx, Duration::from_millis(60), "the message must be emitted once")
            .await;

        running.stop().await;
        std::fs::remove_dir_all(&root).ok();
    }

    // -- the envelope bound and a drop in progress, end to end --

    /// One json-file line (with its `\n`) as dockerd escapes it.
    fn docker_line(stream: &str, log: &str) -> String {
        format!("{}\n", crate::docker_verification::envelope(stream, log, TIME, &[]))
    }

    /// Appends `text` to `path`.
    fn append(path: &Path, text: &str) {
        std::fs::OpenOptions::new()
            .append(true)
            .open(path)
            .unwrap()
            .write_all(text.as_bytes())
            .unwrap();
    }

    /// `max_line_bytes` bounds the message, not the envelope: three fragments of 300 `<`, each
    /// escaped to an 1.8 KiB envelope, reassemble into one 900-byte message under a 1 KiB bound.
    #[tokio::test]
    async fn a_fragment_envelope_larger_than_max_line_bytes_still_reaches_the_decoder() {
        let root = scratch_dir("docker-envelope-cap");
        let log = container(&root, &"c".repeat(64), "esc", "nginx:1.25");
        let piece = "<".repeat(300);
        let fragment = docker_line("stdout", &piece);
        assert!(fragment.len() > 1024, "each envelope must exceed max_line_bytes");
        std::fs::write(
            &log,
            format!("{fragment}{fragment}{}", docker_line("stdout", &format!("{piece}\n"))),
        )
        .unwrap();

        let mut config = fast_tail_config();
        config.max_line_bytes = 1024;
        let (fanout, mut rx) = fanout_channel(8);
        let input =
            DockerInput::new(root.clone(), ContainerFilter::new(vec![], true), vec![], config);
        let running = spawn_input(input, fanout).await;

        assert_eq!(messages(&recv_events(&mut rx, 1).await), vec!["<".repeat(900)]);

        running.stop().await;
        std::fs::remove_dir_all(&root).ok();
    }

    /// A 16 KiB fragment over a 1 KiB bound drops the whole line in the decoder, closing entry
    /// included. Were the splitter to drop the fragment's envelope, the decoder would never see it
    /// and would emit the closing `xyz` as a line of its own.
    #[tokio::test]
    async fn a_16_kib_dockerd_fragment_over_a_small_max_line_bytes_drops_the_whole_line_not_just_its_tail(
    ) {
        let root = scratch_dir("docker-envelope-drop");
        let log = container(&root, &"d".repeat(64), "big", "nginx:1.25");
        std::fs::write(
            &log,
            format!(
                "{}{}",
                docker_line("stdout", &"a".repeat(DOCKERD_FRAGMENT_BYTES)),
                docker_line("stdout", "xyz\n")
            ),
        )
        .unwrap();

        let mut config = fast_tail_config();
        config.max_line_bytes = 1024;
        let diag = Diagnostics::new("docker-envelope-drop");
        let (fanout, mut rx) = fanout_channel(8);
        let input =
            DockerInput::new(root.clone(), ContainerFilter::new(vec![], true), vec![], config)
                .with_diagnostics(diag.clone());
        let running = spawn_input(input, fanout).await;

        wait_until("the oversized line to be dropped", || diag.occurrences("long_line") >= 1).await;
        append(&log, &docker_line("stdout", "ok\n"));
        assert_eq!(
            messages(&recv_events(&mut rx, 1).await),
            vec!["ok"],
            "the dropped line's closing entry must not be emitted ahead of the next line"
        );
        assert_eq!(diag.occurrences("long_line"), 1, "one line dropped, once");

        running.stop().await;
        std::fs::remove_dir_all(&root).ok();
    }

    /// A crash while a line is being dropped, then its closing entry, then a restart: the
    /// checkpoint stayed at the dropped line's start, so the restart drops it whole again instead
    /// of emitting its closing entry as a message.
    #[tokio::test]
    async fn a_crash_mid_drop_does_not_emit_the_dropped_lines_tail_after_restart() {
        let root = scratch_dir("docker-crash-mid-drop");
        let log = container(&root, &"e".repeat(64), "drop", "nginx:1.25");
        let whole = json_file_line("whole\n");
        let fragment = json_file_line(&"y".repeat(80));
        std::fs::write(&log, format!("{whole}{fragment}{fragment}{fragment}")).unwrap();
        let checkpoint = root.join("checkpoint.json");
        let mut config = fast_tail_config();
        config.checkpoint_path = Some(checkpoint.clone());
        config.checkpoint_interval = Duration::from_millis(30);
        config.max_line_bytes = 200;

        let diag = Diagnostics::new("docker-crash-mid-drop");
        let (fanout, mut rx) = fanout_channel(8);
        let input = DockerInput::new(
            root.clone(),
            ContainerFilter::new(vec![], true),
            vec![],
            config.clone(),
        )
        .with_diagnostics(diag.clone());
        let running = spawn_input(input, fanout).await;
        assert_eq!(messages(&recv_events(&mut rx, 1).await), vec!["whole"]);
        wait_until("the third fragment to start the drop", || diag.occurrences("long_line") >= 1)
            .await;
        assert_eq!(
            first_nonzero_checkpoint(&checkpoint).await,
            whole.len() as u64,
            "the checkpoint must stay at the dropped line's first fragment"
        );
        running.handle.abort();
        let _ = running.handle.await;

        append(&log, &json_file_line("end\n"));

        let diag2 = Diagnostics::new("docker-crash-mid-drop-2");
        let (fanout2, mut rx2) = fanout_channel(8);
        let input2 =
            DockerInput::new(root.clone(), ContainerFilter::new(vec![], true), vec![], config)
                .with_diagnostics(diag2.clone());
        let running2 = spawn_input(input2, fanout2).await;
        wait_until("the restart to drop the line again", || diag2.occurrences("long_line") >= 1)
            .await;
        append(&log, &json_file_line("ok\n"));
        assert_eq!(
            messages(&recv_events(&mut rx2, 1).await),
            vec!["ok"],
            "the dropped line's closing entry must not surface as a message"
        );
        running2.stop().await;

        std::fs::remove_dir_all(&root).ok();
    }

    /// At shutdown the splitter hands the decoder a torn last envelope, which it rejects; the
    /// fragment held before it is still emitted.
    #[tokio::test]
    async fn a_torn_envelope_at_shutdown_still_emits_the_held_fragment() {
        let root = scratch_dir("docker-torn-shutdown");
        let log = container(&root, &"7".repeat(64), "torn", "nginx:1.25");
        std::fs::write(&log, format!("{}{{\"log\":\"ta", json_file_line("head-"))).unwrap();

        let (fanout, mut rx) = fanout_channel(8);
        let mut probe = TelemetryProbe::new();
        let input = DockerInput::new(
            root.clone(),
            ContainerFilter::new(vec![], true),
            vec![],
            fast_tail_config(),
        )
        .with_telemetry(probe.telemetry("docker", "docker_in", "listener"));
        let running = spawn_input(input, fanout).await;
        probe
            .wait_for("the fragment's line to be read", |t| t.sum("logit.input.lines", &[]) >= 1.0)
            .await;

        running.stop().await;
        assert_eq!(messages(&recv_events(&mut rx, 1).await), vec!["head-"]);
        std::fs::remove_dir_all(&root).ok();
    }
}
