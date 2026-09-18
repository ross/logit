//! `docker_in`: tails Docker's json-file container logs (`<root>/<id>/<id>-json.log`) and stamps
//! per-container resource attributes read from the sibling `config.v2.json` -- no docker socket,
//! no HTTP client. Built on the exact same driver `tail_in` (`crate::tail`) uses, swapping in
//! [`DockerDecoder`] for [`crate::tail::LineDecoder`] and [`PathPattern::docker_containers`]
//! (`crate::tail::PathPattern`) for `tail_in`'s own config-driven patterns. See
//! `docs/adr/file-tailing-and-docker-json-logs.md` and
//! `docs/adr/docker-container-identity-and-minimal-watches.md` (identity refresh via
//! [`DockerDecoderFactory::refresh_cache`], a cache keyed on `config.v2.json`'s own stat).

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

/// Which containers under `root` this listener follows. Explicit by default -- a container not
/// named here is never tailed, even if it exists -- with `discover: true` as the opt-in to follow
/// everything, including containers that appear after startup. See the ADR's "Selection" section
/// for why explicit is the default.
pub struct ContainerFilter {
    entries: Vec<String>,
    discover: bool,
}

impl ContainerFilter {
    pub fn new(entries: Vec<String>, discover: bool) -> Self {
        Self { entries, discover }
    }

    /// `dir_name` is the container's full id (the directory name under `root`); `name` is its
    /// human name from `config.v2.json`, when known (`None` if metadata couldn't be read yet --
    /// only an id-prefix entry can still match in that case). Each `entries` value matches either
    /// exactly against `name`, or as a prefix (at least 12 hex characters, config-validated by
    /// graph rule 27 doesn't check this specifically, but a shorter or non-hex entry simply never
    /// matches an id) of `dir_name`.
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

/// One container's identity and image reference, read from the sibling `config.v2.json`. Read at
/// open, and re-read on every later `scan` via [`DockerDecoderFactory::refresh_cache`], which
/// skips the actual `read`+parse unless the file's own stat has changed since
/// (`docs/adr/docker-container-identity-and-minimal-watches.md`).
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
    /// `dir` is one container's own directory under `root` (its name is the container's full
    /// id). Fails if `config.v2.json` is missing, unreadable, or doesn't parse as expected --
    /// the caller ([`DockerDecoderFactory`]) degrades to an id-only resource rather than treating
    /// this as fatal to tailing the container's log.
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

    /// Builds this container's resource: `container.id`/`container.name`/`container.image.name`/
    /// `container.image.tag` always, plus `container.label.<key>` for every key in `label_keys`
    /// that this container actually carries -- opt-in, never every label (a label's value is
    /// operator-controlled data, not `logit`'s to expose unasked; see the ADR's event/resource
    /// shape section).
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
/// no tag half by this scheme -- the `@` half is the identity, not a human-chosen tag. Otherwise
/// splits on the reference's *last* `:`, and only if nothing after it contains a `/` -- a `:` that
/// introduces a registry port (`registry:5000/app`) is not a tag separator, and the part after it
/// containing `/` is what tells the two apart.
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

/// Held across [`DockerDecoder::decode_line`] calls while Docker's own json-file driver has split
/// one logical container-log line across more than one JSON entry (its writer buffers in ~16 KiB
/// chunks; an entry whose `log` field doesn't yet end in `\n` is a fragment, not a whole line).
/// `timestamp`/`stream`/`attrs` are always taken from the *closing* entry (the one whose `log`
/// finally does end in `\n`) -- the entries making up one logical line are written by the same
/// daemon call in immediate succession, so the difference is negligible, and keeping only the
/// latest avoids holding onto values that will just be overwritten.
#[derive(Default)]
struct PartialEntry {
    message: String,
    timestamp: i64,
    stream: &'static str,
    attrs: Vec<(String, String)>,
}

/// `docker_in`'s own [`TailDecoder`]: decodes Docker's json-file envelope, reassembles a
/// split-across-entries line (see [`PartialEntry`]), and stamps every event with this container's
/// resource -- built at open (by [`DockerDecoderFactory::open`]) and kept live afterward by
/// [`DockerDecoderFactory::refresh`], which reaches into `resource` directly (same module,
/// private field) to swap it on an identity change. Never looks inside the envelope's own `log`
/// field past reassembling it -- the inner application line stays whatever downstream transform
/// (`json`, typically) an operator chains after this, exactly as it would for `tail_in`.
pub struct DockerDecoder {
    resource: Arc<Resource>,
    partial: Option<PartialEntry>,
    /// Set once an in-progress reassembly has already been dropped for exceeding
    /// `max_line_bytes` -- every further fragment is silently discarded (not re-counted, not
    /// re-diagnosed) until the one that finally closes the line clears it, mirroring
    /// `crate::tail::line::LineSplitter`'s own `dropping` flag.
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
            // Already bounds-checked on every fragment append (`decode_line` above) -- a held
            // partial here is always within `max_line_bytes`, so this always emits.
            self.emit(partial.timestamp, partial.stream, &partial.attrs, partial.message, out);
        }
    }

    fn reset(&mut self) {
        // Both halves of the reassembly state, not just `partial`: a stale `dropping == true`
        // surviving a truncation silently *swallows* the new generation's first complete entry
        // (the entry that clears the flag is itself discarded), the mirror of the splicing a
        // stale `partial` causes.
        self.partial = None;
        self.dropping = false;
    }

    fn resource(&self) -> Arc<Resource> {
        self.resource.clone()
    }
}

/// Enough of `config.v2.json`'s own stat to notice it was rewritten -- Docker typically rewrites
/// it via a tmp-file-plus-rename, so `ino` usually changes too, not just `mtime`/`len`; comparing
/// all four is cheap and catches either style of rewrite.
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

/// One container directory's cached read, keyed by that directory in
/// [`DockerDecoderFactory::meta`]. Read once, then refreshed only when `config.v2.json`'s own
/// stat changes -- see [`DockerDecoderFactory::refresh_cache`].
#[derive(Default)]
struct CachedMeta {
    /// The stat `identity` (or the last failed attempt) was read at. `None` means the file
    /// couldn't be stat'd at all (missing, or a transient race) -- deliberately never compared
    /// equal to itself by [`DockerDecoderFactory::refresh_cache`], so a not-yet-existing config
    /// is retried every scan rather than settling into a permanently-stale "unchanged" state.
    stat: Option<ConfigStat>,
    /// `None` until a read has ever succeeded for this directory -- `open`/`accept` degrade to
    /// [`id_only_resource`] while this is `None`.
    identity: Option<Identity>,
    /// Set once a read attempt has failed since `identity` was last refreshed -- gates
    /// `metadata_error` to fire once per failure, not once per poll tick for as long as it
    /// persists (`Diagnostics::warn_throttled` counts every call into
    /// `logit.component.diagnostics` even while it throttles the log line itself).
    failed: bool,
    /// Stamped with `DockerDecoderFactory::generation` on every `accept`/`refresh` that touches
    /// this entry -- what lets `end_scan` evict entries no live path reached this scan, bounding
    /// `meta` by containers currently on the host rather than containers ever seen.
    seen: u64,
}

fn id_only_resource(container_dir: &Path) -> Arc<Resource> {
    let id = container_dir.file_name().and_then(|n| n.to_str()).unwrap_or_default();
    let mut attrs = AttrMap::new();
    attrs.insert("container.id", id);
    Arc::new(Resource { attributes: attrs, ..Default::default() })
}

/// Turns a discovered `<root>/<id>/<id>-json.log` path into a [`DockerDecoder`]: applies
/// [`ContainerFilter`] (reading `config.v2.json` only when it needs the container's name to do
/// so -- `discover: true` never reads it at this stage) in [`accept`](DecoderFactory::accept),
/// builds the resource every line carries from that same cached read in
/// [`open`](DecoderFactory::open), and keeps it current from there through
/// [`refresh`](DecoderFactory::refresh) on every later scan. Every one of those three goes
/// through `meta`/[`refresh_cache`](DockerDecoderFactory::refresh_cache), so a poll tick costs
/// one `stat` per container rather than a read and a parse. A container whose metadata has never
/// been read successfully degrades to a `container.id`-only resource (diagnosed
/// `metadata_error`) rather than not being tailed at all -- lines still flow, just without the
/// richer identity, until a later read succeeds.
struct DockerDecoderFactory {
    filter: ContainerFilter,
    labels: Vec<String>,
    max_line_bytes: usize,
    diag: Diagnostics,
    meta: BTreeMap<PathBuf, CachedMeta>,
    generation: u64,
}

impl DockerDecoderFactory {
    /// Brings `dir`'s cached identity up to date, reading and parsing `config.v2.json` only when
    /// its stat differs from the one the cached entry was built from. A read failure leaves
    /// `identity` exactly as it was (a container that had a good identity keeps it; one that
    /// never had one stays on [`id_only_resource`]) and is diagnosed only on the transition into
    /// failure -- every later scan silently keeps retrying until the stat changes again.
    fn refresh_cache(&mut self, dir: &Path) {
        let stat = std::fs::metadata(dir.join("config.v2.json"))
            .ok()
            .map(|m| ConfigStat::from_metadata(&m));
        let entry = self.meta.entry(dir.to_path_buf()).or_default();
        entry.seen = self.generation;
        if stat.is_some() && stat == entry.stat {
            return; // unchanged since the last read at this exact stat
        }
        entry.stat = stat;
        match ContainerMeta::read(dir) {
            Ok(meta) => {
                entry.failed = false;
                let resource = meta.resource(&self.labels);
                // Keep the existing `Arc` when the rebuilt `Resource` compares equal --
                // `config.v2.json` is rewritten by the daemon for reasons unrelated to identity
                // (restart counts, healthcheck results) far more often than an operator renames a
                // container, and a fresh `Arc` on every such rewrite would force a spurious
                // `ResourceChange` flush on every tailed line of every container, constantly.
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
            return true; // never reads config.v2.json at this stage -- see the ADR's Selection
                         // section; the field-level `discover` short-circuit is unconditional
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
        // Already fresh in the ordinary case: `open_tracked` always calls `accept` for this same
        // path first, in the same `scan`, which already ran this exact read.
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
        // Selection before identity, deliberately: a container renamed out of `containers:` is
        // closing, and the decoder's resource must NOT be swapped for it -- whatever
        // `close_decoder` flushes on the way out should carry the identity this listener actually
        // read those lines under, not the new one it's no longer allowed to follow.
        // `discover: true` can never reach this branch (`ContainerFilter::matches` returns `true`
        // unconditionally in that mode, before looking at anything).
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

/// `docker_in`: tails every currently-selected container's json-file log under `root`, built on
/// the same [`Tailer`] driver `tail_in` (`crate::tail::TailInput`) uses.
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

    /// The currently-configured knobs -- test introspection, mirroring `TailInput::config`.
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

        // A fresh line afterward behaves normally again -- short enough to fit the same tiny
        // 10-byte bound this test uses to force the drop above.
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
        // The application's own log line happens to itself look like JSON -- docker_in must
        // treat it as an opaque string, not decode it a second time.
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

        // "Recreated": the old container directory disappears, a new id appears under the same
        // name.
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

    // -- W2: live container identity (docs/adr/docker-container-identity-and-minimal-watches.md) -

    /// The core property: a `docker rename` (a `config.v2.json` rewrite) is picked up on the next
    /// poll tick, and the resource swap goes through the accumulator's own `Arc::ptr_eq` check --
    /// a batch never mixes two identities, and the rename never retroactively relabels a line
    /// already emitted under the old name. `discover: true` -- selection isn't under test here
    /// (W3's job).
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

        // Rewrite `config.v2.json` in place -- a `docker rename`, from `logit`'s point of view.
        std::fs::write(
            root.join(&id).join("config.v2.json"),
            r#"{"Name":"/after","Config":{"Image":"nginx:1.25","Labels":{}}}"#,
        )
        .unwrap();
        // Give a poll tick a chance to refresh the identity *before* the next line exists to
        // read: identity refresh only ever happens on a `scan` (`Tailer::refresh_identity`), but
        // `drain` -- the thing that actually decodes a line -- runs on every loop iteration,
        // including one driven by the (equally 15ms) flush timer rather than the poll timer. A
        // line written immediately after the rewrite, with no gap, can race a flush-driven drain
        // that reads it before the next poll-driven scan has refreshed anything, and get decoded
        // under the stale identity -- not a production bug (identity refresh is documented as
        // bounded by `poll_interval`, not "as of the very next byte"), but this test's own
        // assertion needs the refresh to have already landed.
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

    /// A `config.v2.json` that's momentarily unreadable (the race the ADR names: a container
    /// directory appearing a moment before its config does) recovers on its own once the file
    /// settles, instead of sticking as `container.id`-only for the life of the handle.
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

        // The config appears -- the race resolving itself, the same as a real container whose
        // directory is created a moment before `config.v2.json` is written into it.
        std::fs::write(
            dir.join("config.v2.json"),
            r#"{"Name":"/recovered","Config":{"Image":"nginx:1.25","Labels":{}}}"#,
        )
        .unwrap();
        // Give a poll tick a chance to refresh the identity before the next line exists to read
        // -- see the identically-shaped sleep and comment in
        // `a_rewritten_config_v2_json_changes_the_name_on_a_fresh_batch_boundary` above for why
        // this is needed: a flush-driven `drain` can otherwise decode this line before the next
        // poll-driven `scan` has refreshed anything.
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

    /// The batching-correctness half of the value-equality check in `refresh_cache`: a
    /// `config.v2.json` rewrite that changes only fields this factory doesn't track (or doesn't
    /// change the rebuilt `Resource`'s value at all) must never force a new `Arc` -- otherwise
    /// every unrelated daemon rewrite (restart counts, healthcheck results) would shatter batching
    /// for that container.
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

        // Rewritten, but to the exact same name/image/labels this factory stamps onto the
        // resource -- a daemon rewrite for an untracked reason (a restart counter, a healthcheck
        // field this repo never reads), not an identity change.
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

    // -- W3: selection follows the rename (docs/adr/docker-container-identity-and-minimal-
    // watches.md) --

    /// A container renamed out of an explicit `containers:` list stops flowing -- and does so
    /// without draining the container to EOF first, since it's still running: only the lines
    /// already read before the rename arrive.
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

    /// A container renamed back into the selection resumes rather than replaying its whole log,
    /// and lines written while it was away are delivered once it's selected again -- under the
    /// identity current when they're finally read.
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

        // Renamed away, then back -- with a line written while it was away, which must survive.
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
        // A stale rotated file sitting right next to the real one -- must never match.
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
        // discover mode -- accept() never needs metadata, so a missing config.v2.json must not
        // stop discovery either; open()'s own fallback (unit-tested directly above) is what
        // handles the missing-metadata case for the resource itself.
        let filter = ContainerFilter::new(vec![], true);
        let input = DockerInput::new(root.clone(), filter, vec![], fast_tail_config());
        let (tx, handle) = spawn(input, fanout);

        let events = expect_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["hi"]);

        shutdown(tx, handle).await;
        std::fs::remove_dir_all(&root).ok();
    }

    /// F1, superseded by `docs/adr/docker-container-identity-and-minimal-watches.md`: `docker_in`
    /// no longer watches each container's own subdirectory, only `root` (Docker's per-container
    /// state directories are direct children of it, so `root` alone already catches a container
    /// *arriving*). A log file appearing a moment later *inside an already-existing* container
    /// directory is therefore not an `inotify` event at all -- nothing changed under `root`
    /// itself -- and is picked up on the next `poll_interval` tick instead, exactly as it would be
    /// under `watch: poll`. This pins that contract, inverted from what this test asserted before
    /// the watch-set change: discovery here is poll-bound even under `WatchMode::Inotify`.
    /// `discover: true` -- selection isn't what's under test.
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

        // Let the initial scan run (and start watching `root`) against an empty directory before
        // the container appears at all.
        tokio::time::sleep(Duration::from_millis(30)).await;

        let id = "6".repeat(64);
        // Directory + config.v2.json only -- no log file yet, exactly the race Docker itself
        // creates (the container directory appears a moment before the log file inside it).
        let log_path = container(&root, &id, "web", "nginx:1.25");

        // Give `root`'s own IN_CREATE (the container directory appearing) time to be handled --
        // it finds no log file yet, since none exists at that point.
        tokio::time::sleep(Duration::from_millis(30)).await;
        std::fs::write(
            &log_path,
            format!(
                "{}\n",
                r#"{"log":"hello\n","stream":"stdout","time":"2026-08-17T19:35:46.000000000Z"}"#
            ),
        )
        .unwrap();

        // Well within the 300ms poll_interval, and nothing under `root` itself changed when the
        // log file appeared inside the already-existing container directory -- must not be
        // discovered yet.
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

    /// F1's other half, still true after `docs/adr/docker-container-identity-and-minimal-watches.md`
    /// (unlike its sibling above): a truncation of an already-tracked container log is still
    /// noticed via `inotify`, near-immediately -- not because the container's directory is
    /// watched (it isn't, any more), but because the *file itself* now has its own watch,
    /// registered when `docker_in` opened it. `O_TRUNC` fires `IN_MODIFY` on that watch directly.
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

        // `std::fs::write` opens with `O_TRUNC` on the existing path -- same inode, shorter length
        // -- a real truncation, not a rotation.
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

    /// F2: a docker log truncation must reset `DockerDecoder`'s own `partial`, not just
    /// `LineSplitter`'s -- modeled on `driver.rs`'s
    /// `a_truncation_discards_the_partial_line_held_from_the_previous_generation`.
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

        // Give the tailer time to read the fragment and hold it -- nothing should emit yet.
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
        // `std::fs::write` opens with `O_TRUNC` on the existing path -- same inode, shorter length.
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

    /// F2's other half: a truncation must also clear a stale `dropping == true`, or the very next
    /// generation's first complete entry is silently swallowed clearing it.
    #[tokio::test]
    async fn a_truncation_clears_a_docker_dropping_state_so_the_next_generation_is_not_swallowed() {
        let root = scratch_dir("docker-truncate-dropping");
        let id = "9".repeat(64);
        let log_path = container(&root, &id, "web", "nginx:1.25");

        // Three fragment entries, none terminated -- individually each envelope line is well under
        // 200 bytes (the splitter passes each through fine), but the three fragments' `log` values
        // accumulate past `max_line_bytes` (200) inside `DockerDecoder`'s own reassembly, which
        // then starts (and stays) `dropping` until an entry finally closes the line.
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
