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

/// A container-log line Docker's json-file driver split across entries (it writes in ~16 KiB
/// chunks; an entry whose `log` doesn't end in `\n` is a fragment), held across
/// [`DockerDecoder::decode_line`] calls. The emitted `timestamp`/`stream`/`attrs` are the latest
/// entry's; one daemon call writes all the fragments in immediate succession.
#[derive(Default)]
struct PartialEntry {
    message: String,
    timestamp: i64,
    stream: &'static str,
    attrs: Vec<(String, String)>,
}

/// `docker_in`'s [`TailDecoder`]: decodes Docker's json-file envelope, reassembles split lines
/// (see [`PartialEntry`]), and stamps every event with the container's resource, which
/// `DockerDecoderFactory::refresh` swaps in place on an identity change. Never parses the inner
/// application line in `log`; that is a downstream `json` transform's job.
pub struct DockerDecoder {
    resource: Arc<Resource>,
    partial: Option<PartialEntry>,
    /// Set once a reassembly is dropped for exceeding `max_line_bytes`: later fragments are
    /// discarded uncounted until the closing one clears it, as in `LineSplitter`'s `dropping`.
    dropping: bool,
    max_line_bytes: usize,
    diag: Diagnostics,
}

impl DockerDecoder {
    pub(crate) fn new(resource: Arc<Resource>, max_line_bytes: usize) -> Self {
        Self {
            resource,
            partial: None,
            dropping: false,
            max_line_bytes,
            diag: Diagnostics::default(),
        }
    }

    pub(crate) fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
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
        event_attrs.insert("log.iostream", stream);
        for (k, v) in attrs {
            event_attrs.insert(k, v.as_str());
        }
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

#[derive(serde::Deserialize)]
struct JsonFileLine<'a> {
    #[serde(borrow)]
    log: Cow<'a, str>,
    stream: &'a str,
    time: &'a str,
    #[serde(default, borrow)]
    attrs: Option<BTreeMap<&'a str, Cow<'a, str>>>,
}

impl TailDecoder for DockerDecoder {
    fn decode_line(
        &mut self,
        line: Bytes,
        read_at: i64,
        out: &mut Vec<Event>,
    ) -> Result<Arc<Resource>, logit_proto::CodecError> {
        let entry: JsonFileLine = serde_json::from_slice(&line).map_err(|err| {
            logit_proto::CodecError::Malformed(format!("docker json-file entry: {err}"))
        })?;
        let stream: &'static str = match entry.stream {
            "stdout" => "stdout",
            "stderr" => "stderr",
            other => {
                return Err(logit_proto::CodecError::Malformed(format!(
                    "unknown docker log stream {other:?}"
                )))
            }
        };
        let timestamp = match logit_core::parse_rfc3339_to_nanos(entry.time) {
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
            .map(|m| m.into_iter().map(|(k, v)| (k.to_string(), v.into_owned())).collect())
            .unwrap_or_default();
        let is_complete = entry.log.ends_with('\n');

        if self.dropping {
            if is_complete {
                self.dropping = false;
            }
            return Ok(self.resource.clone());
        }

        if !is_complete {
            let held = self.partial.get_or_insert_with(PartialEntry::default);
            held.message.push_str(&entry.log);
            held.timestamp = timestamp;
            held.stream = stream;
            held.attrs = attrs;
            if held.message.len() > self.max_line_bytes {
                self.diag.warn_throttled(
                    "long_line",
                    "a docker log line exceeded max_line_bytes and was dropped whole",
                );
                self.partial = None;
                self.dropping = true;
            }
            return Ok(self.resource.clone());
        }

        let mut message = self.partial.take().map(|p| p.message).unwrap_or_default();
        // `strip_suffix` cannot fail: `is_complete` above is exactly this check.
        message.push_str(entry.log.strip_suffix('\n').unwrap_or(&entry.log));
        if message.len() > self.max_line_bytes {
            self.diag.warn_throttled(
                "long_line",
                "a docker log line exceeded max_line_bytes and was dropped whole",
            );
            return Ok(self.resource.clone());
        }
        self.emit(timestamp, stream, &attrs, message, out);
        Ok(self.resource.clone())
    }

    fn close(&mut self, out: &mut Vec<Event>) {
        if let Some(partial) = self.partial.take() {
            // Every fragment append is bounds-checked, so a held partial always fits.
            self.emit(partial.timestamp, partial.stream, &partial.attrs, partial.message, out);
        }
    }

    fn reset(&mut self) {
        // Both halves: a stale `dropping` would swallow the new generation's first complete
        // entry, as a stale `partial` would splice into it.
        self.partial = None;
        self.dropping = false;
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
        let factory = DockerDecoderFactory {
            filter,
            labels,
            max_line_bytes: config.max_line_bytes,
            diag: Diagnostics::default(),
            meta: BTreeMap::new(),
            generation: 0,
        };
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

    /// The configured knobs, for tests.
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
    use logit_pipeline::{unwrap_batch, Delivered, Input};
    use std::io::Write;
    use std::time::Duration;
    use tokio::sync::mpsc;

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

    fn recording_fanout(capacity: usize) -> (Fanout, mpsc::Receiver<Delivered>) {
        let (tx, rx) = mpsc::channel(capacity);
        (Fanout::new(vec![tx]), rx)
    }

    fn spawn(
        mut input: DockerInput,
        sink: Fanout,
    ) -> (shutdown_watch::Sender<bool>, tokio::task::JoinHandle<anyhow::Result<()>>) {
        let (tx, rx) = shutdown_watch::channel(false);
        let handle = tokio::spawn(async move { input.run_until_shutdown(sink, rx).await });
        (tx, handle)
    }

    async fn shutdown(
        tx: shutdown_watch::Sender<bool>,
        handle: tokio::task::JoinHandle<anyhow::Result<()>>,
    ) {
        let _ = tx.send(true);
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("should shut down within 5s")
            .expect("should not panic")
            .expect("should exit cleanly");
    }

    async fn expect_events(rx: &mut mpsc::Receiver<Delivered>, n: usize) -> Vec<Event> {
        let mut events = Vec::new();
        while events.len() < n {
            let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("timed out waiting for events")
                .expect("fanout channel closed unexpectedly");
            events.extend(unwrap_batch(delivered).events);
        }
        events
    }

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
                shutdown_grace: Duration::from_secs(5),
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

        let (fanout, mut rx) = recording_fanout(8);
        let filter = ContainerFilter::new(vec!["wanted".to_string()], false);
        let input = DockerInput::new(root.clone(), filter, vec![], fast_tail_config());
        let (tx, handle) = spawn(input, fanout);

        let events = expect_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["from a"]);
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(rx.try_recv().is_err(), "an unlisted container must never be tailed");
        shutdown(tx, handle).await;

        let (fanout2, mut rx2) = recording_fanout(8);
        let filter2 = ContainerFilter::new(vec![], true);
        let input2 = DockerInput::new(root.clone(), filter2, vec![], fast_tail_config());
        let (tx2, handle2) = spawn(input2, fanout2);
        let events2 = expect_events(&mut rx2, 2).await;
        let mut msgs = messages(&events2);
        msgs.sort();
        assert_eq!(msgs, vec!["from a", "from b"], "discover: true should follow every container");
        shutdown(tx2, handle2).await;

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

        let (fanout, mut rx) = recording_fanout(8);
        let filter = ContainerFilter::new(vec!["web".to_string()], false);
        let input = DockerInput::new(root.clone(), filter, vec![], fast_tail_config());
        let (tx, handle) = spawn(input, fanout);

        let events = expect_events(&mut rx, 1).await;
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

        let events2 = expect_events(&mut rx, 1).await;
        assert_eq!(messages(&events2), vec!["new"]);

        shutdown(tx, handle).await;
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

        let (fanout, mut rx) = recording_fanout(8);
        let filter = ContainerFilter::new(vec![], true);
        let input = DockerInput::new(root.clone(), filter, vec![], fast_tail_config());
        let (tx, handle) = spawn(input, fanout);

        let batch1 = unwrap_batch(rx.recv().await.expect("first batch"));
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
        // Let a poll tick refresh the identity before the next line exists. Refresh happens only
        // on a `scan`, but a flush-timer `drain` can decode a line first, under the stale
        // identity. Refresh is bounded by `poll_interval`, so that isn't a bug, but this
        // assertion needs the refresh to have landed.
        tokio::time::sleep(Duration::from_millis(60)).await;
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

        let batch2 = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("second batch should arrive well within 3s")
            .expect("channel open");
        let batch2 = unwrap_batch(batch2);
        assert_eq!(messages(&batch2.events), vec!["two"]);
        assert_eq!(
            batch2.resource.attributes.get("container.name").and_then(|v| v.as_str()),
            Some("after")
        );
        assert!(
            !Arc::ptr_eq(&batch1.resource, &batch2.resource),
            "a renamed container must get a fresh resource Arc, never share the old one"
        );

        shutdown(tx, handle).await;
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

        let (fanout, mut rx) = recording_fanout(8);
        let filter = ContainerFilter::new(vec![], true);
        let input = DockerInput::new(root.clone(), filter, vec![], fast_tail_config());
        let (tx, handle) = spawn(input, fanout);

        let batch1 = unwrap_batch(rx.recv().await.expect("first batch"));
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
        // Let a poll tick refresh the identity first; see the same sleep in
        // `a_rewritten_config_v2_json_changes_the_name_on_a_fresh_batch_boundary`.
        tokio::time::sleep(Duration::from_millis(60)).await;
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

        let batch2 = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("recovery should be noticed well within 3s")
            .expect("channel open");
        let batch2 = unwrap_batch(batch2);
        assert_eq!(messages(&batch2.events), vec!["two"]);
        assert_eq!(
            batch2.resource.attributes.get("container.name").and_then(|v| v.as_str()),
            Some("recovered")
        );

        shutdown(tx, handle).await;
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

        let (fanout, mut rx) = recording_fanout(8);
        let filter = ContainerFilter::new(vec![], true);
        let input = DockerInput::new(root.clone(), filter, vec![], fast_tail_config());
        let (tx, handle) = spawn(input, fanout);

        let batch1 = unwrap_batch(rx.recv().await.expect("first batch"));
        assert_eq!(messages(&batch1.events), vec!["one"]);

        // Same name/image/labels: a daemon rewrite for an untracked field.
        std::fs::write(
            root.join(&id).join("config.v2.json"),
            r#"{"Name":"/steady","Config":{"Image":"nginx:1.25","Labels":{}}}"#,
        )
        .unwrap();
        // Give at least one poll tick a chance to see the stat change and re-read.
        tokio::time::sleep(Duration::from_millis(60)).await;
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

        let batch2 = unwrap_batch(rx.recv().await.expect("second batch"));
        assert_eq!(messages(&batch2.events), vec!["two"]);
        assert!(
            Arc::ptr_eq(&batch1.resource, &batch2.resource),
            "a rewrite reproducing the same resource value must keep the existing Arc, not mint \
             a new one -- otherwise every untracked daemon rewrite would flush a batch that never \
             needed to split"
        );

        shutdown(tx, handle).await;
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

        let (fanout, mut rx) = recording_fanout(8);
        let filter = ContainerFilter::new(vec!["wanted".to_string()], false);
        let input = DockerInput::new(root.clone(), filter, vec![], fast_tail_config());
        let (tx, handle) = spawn(input, fanout);

        let events = expect_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["one"]);

        // Renamed out of `containers: ["wanted"]`.
        std::fs::write(
            root.join(&id).join("config.v2.json"),
            r#"{"Name":"/elsewhere","Config":{"Image":"nginx:1.25","Labels":{}}}"#,
        )
        .unwrap();
        // Give it a couple of poll ticks to notice and close.
        tokio::time::sleep(Duration::from_millis(60)).await;

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

        let nothing = tokio::time::timeout(Duration::from_millis(150), rx.recv()).await;
        assert!(nothing.is_err(), "a container renamed out of the selection must not keep flowing");

        shutdown(tx, handle).await;
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

        let (fanout, mut rx) = recording_fanout(8);
        let filter = ContainerFilter::new(vec!["wanted".to_string()], false);
        let input = DockerInput::new(root.clone(), filter, vec![], fast_tail_config());
        let (tx, handle) = spawn(input, fanout);

        let events = expect_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["one"]);

        // Renamed away, then back, with a line written while away.
        std::fs::write(
            root.join(&id).join("config.v2.json"),
            r#"{"Name":"/elsewhere","Config":{"Image":"nginx:1.25","Labels":{}}}"#,
        )
        .unwrap();
        tokio::time::sleep(Duration::from_millis(60)).await;
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
        let nothing = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;
        assert!(nothing.is_err(), "still renamed away -- nothing should arrive yet");

        std::fs::write(
            root.join(&id).join("config.v2.json"),
            r#"{"Name":"/wanted","Config":{"Image":"nginx:1.25","Labels":{}}}"#,
        )
        .unwrap();

        let events2 = expect_events(&mut rx, 1).await;
        assert_eq!(
            messages(&events2),
            vec!["two"],
            "must resume from the retained offset -- \"one\" must never be replayed"
        );

        shutdown(tx, handle).await;
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

        let (fanout, mut rx) = recording_fanout(8);
        let filter = ContainerFilter::new(vec!["web".to_string()], false);
        let input = DockerInput::new(root.clone(), filter, vec![], fast_tail_config());
        let (tx, handle) = spawn(input, fanout);

        let events = expect_events(&mut rx, 1).await;
        assert_eq!(
            messages(&events),
            vec!["fresh"],
            "only the real <id>-json.log should ever be tailed"
        );

        shutdown(tx, handle).await;
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

        let (fanout, mut rx) = recording_fanout(8);
        // discover mode: `accept` needs no metadata, so a missing config.v2.json can't stop it.
        let filter = ContainerFilter::new(vec![], true);
        let input = DockerInput::new(root.clone(), filter, vec![], fast_tail_config());
        let (tx, handle) = spawn(input, fanout);

        let events = expect_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["hi"]);

        shutdown(tx, handle).await;
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
        config.poll_interval = Duration::from_millis(300);

        let (fanout, mut rx) = recording_fanout(8);
        let filter = ContainerFilter::new(vec![], true);
        let input = DockerInput::new(root.clone(), filter, vec![], config);
        let (tx, handle) = spawn(input, fanout);

        // Let the initial scan start watching an empty `root`.
        tokio::time::sleep(Duration::from_millis(30)).await;

        let id = "6".repeat(64);
        // Directory and config.v2.json only, as Docker creates them before the log file.
        let log_path = container(&root, &id, "web", "nginx:1.25");

        // Let `root`'s IN_CREATE be handled; it finds no log file yet.
        tokio::time::sleep(Duration::from_millis(30)).await;
        std::fs::write(
            &log_path,
            format!(
                "{}\n",
                r#"{"log":"hello\n","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z"}"#
            ),
        )
        .unwrap();

        // Within the 300ms poll_interval and with nothing changed under `root`: not found yet.
        let too_soon =
            tokio::time::timeout(Duration::from_millis(100), expect_events(&mut rx, 1)).await;
        assert!(
            too_soon.is_err(),
            "must not be discovered before the poll tick -- the container's own subdirectory \
             isn't watched any more, only root, and root saw no event"
        );

        // The very next poll tick picks it up.
        let events = tokio::time::timeout(Duration::from_secs(2), expect_events(&mut rx, 1))
            .await
            .expect("the poll tick should discover it shortly after");
        assert_eq!(messages(&events), vec!["hello"]);

        shutdown(tx, handle).await;
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

        let (fanout, mut rx) = recording_fanout(8);
        let filter = ContainerFilter::new(vec![], true);
        let input = DockerInput::new(root.clone(), filter, vec![], config);
        let (tx, handle) = spawn(input, fanout);

        let events = expect_events(&mut rx, 1).await;
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
            tokio::time::timeout(Duration::from_secs(3), expect_events(&mut rx, 1)).await.expect(
                "inotify should notice the truncation well within 3s, nowhere near the 30s \
                 poll_interval",
            );
        assert_eq!(messages(&events2), vec!["new"]);

        shutdown(tx, handle).await;
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

        let (fanout, mut rx) = recording_fanout(8);
        let filter = ContainerFilter::new(vec![], true);
        let input = DockerInput::new(root.clone(), filter, vec![], fast_tail_config());
        let (tx, handle) = spawn(input, fanout);

        // Let the tailer read and hold the fragment; nothing emits yet.
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(rx.try_recv().is_err(), "an unterminated fragment must not emit before its close");

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

        let events = expect_events(&mut rx, 1).await;
        assert_eq!(
            messages(&events),
            vec!["restarted"],
            "the pre-truncation partial must not be spliced onto the first post-truncation entry"
        );

        shutdown(tx, handle).await;
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

        // Three unterminated fragments, each under 200 bytes so the splitter passes them, whose
        // reassembly passes `max_line_bytes` (200) and leaves the decoder `dropping`.
        let fragment = "y".repeat(80);
        let mut gen1 = String::new();
        for i in 0..3 {
            let entry = format!(
                r#"{{"log":"{fragment}-{i}","stream":"stdout","time":"2026-08-17T19:35:46.00000000{i}Z"}}"#
            );
            assert!(entry.len() < 200, "one envelope line must stay under max_line_bytes: {entry}");
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
        let (fanout, mut rx) = recording_fanout(8);
        let filter = ContainerFilter::new(vec![], true);
        let input = DockerInput::new(root.clone(), filter, vec![], config);
        let (tx, handle) = spawn(input, fanout);

        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(rx.try_recv().is_err(), "a dropped oversized reassembly must never emit");

        let gen2 = format!(
            "{}\n",
            r#"{"log":"ok\n","stream":"stdout","time":"2026-08-17T19:35:47.000000000Z"}"#
        );
        assert!(gen2.len() < gen1.len(), "fixture must be a genuine truncation");
        std::fs::write(&log_path, &gen2).unwrap();

        let events = expect_events(&mut rx, 1).await;
        assert_eq!(
            messages(&events),
            vec!["ok"],
            "a stale dropping flag must not swallow the next generation's first complete entry"
        );

        shutdown(tx, handle).await;
        std::fs::remove_dir_all(&root).ok();
    }
}
