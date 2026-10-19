//! `replay_in`: reads one file from start to end, sends its events downstream, and returns
//! `Ok(())` ([ADR `replay-input`](../../../docs/adr/replay-input.md)). The file is what a
//! `stdio_out`/`file_out` wrote, or events written by hand in the same form.
//!
//! - **`json`**: one event per line, as `crates/logit-outputs/src/ndjson.rs`'s module doc writes
//!   it, read by [`EventJsonDecoder`]. Consecutive lines with equal resource and scope form one
//!   batch, cut at `batch` events. Blank lines are skipped.
//! - **`native`**: the bare `CODEC_BATCH` frames a `format: native` stream sink writes. One frame
//!   is one batch, cut at `batch` events.
//!
//! Timestamps are the file's. Nothing is stamped on the way in: no peer, no receive time.
//!
//! **Ending.** At the end of the file [`Input::run`] returns `Ok(())` and drops its `Fanout`, so
//! the runtime's close cascade flushes every node downstream, as for `generate_in` under `count`.
//! The process exits once every node has finished, so another input that doesn't end (an
//! `internal`, a socket listener) keeps it running.
//!
//! **Loss.** Every unreadable unit is skipped and counted under
//! `logit.input.frames.dropped{reason}`, the name the line and frame drivers already count with,
//! and the rest of the file is read:
//!
//! | `reason` | What was skipped |
//! |---|---|
//! | `undecodable` | a JSON line [`EventJsonDecoder`] refused (diagnostic `bad_line`, with its line number) |
//! | `magic` | bytes that don't start a frame, up to the next frame magic: one count per run |
//! | `version` | a frame header of a version this build doesn't read |
//! | `too_large` | a frame header declaring a body, compressed or not, over the frame cap |
//! | `crc` | a frame whose body fails its CRC, a frame torn mid-file among them |
//! | `codec` | a frame that isn't a bare batch: a spool's hop batch, or a control frame |
//! | `decode_budget` | a batch that would decode past its budget |
//! | `malformed` | any other frame or batch that doesn't parse, a CRC-valid body that doesn't decompress or uses a compression this build doesn't decode among them |
//! | `truncated` | a frame whose declared length runs past the end of the file |
//!
//! Each native fault but `truncated` has the `bad_frame` diagnostic, with the frame's byte
//! offset. A frame that passes its CRC and then fails is skipped by its length. After a header
//! fault, a CRC fault, or a `truncated` frame, the read resumes at the next frame magic past the faulty frame's own magic, not past its declared
//! length: a frame torn by a killed writer, with a later run's frames appended behind it
//! (`file_out` opens its file to append), declares a length that reaches into those frames. So a
//! whole frame anywhere after a fault is read, and only a `truncated` frame with no magic behind
//! it ends the read. A false magic inside a payload costs one more CRC check.
//!
//! One fault is counted per unreadable run: from a counted fault until a frame passes its CRC,
//! nothing more is counted, except that a frame or header the end of the file cuts short, magic
//! included, is counted `truncated` once.
//!
//! A batch no consumer took is counted `logit.input.batches.dropped{reason="closed_consumer"}`
//! and ends the read.
//!
//! The read side records `logit.input.lines`/`.line.bytes` per non-blank JSON line, and
//! `logit.input.frames`/`.frame.bytes` per native frame that passes its CRC, each before it's
//! decoded.

use bytes::{Buf, Bytes, BytesMut};
use logit_core::{Diagnostics, EventBatch, Scope, Telemetry};
use logit_pipeline::{Fanout, Input};
use logit_proto::event_json::{EventJsonDecoder, Line};
use logit_proto::frame::{
    self, compressed_bound, FrameHeader, FLAG_CONTROL, HEADER_LEN, MAGIC, MAX_SANE_UNCOMPRESSED_LEN,
};
use logit_proto::native::{self, DecodeBudget, CODEC_BATCH};
use logit_proto::CodecError;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};

const FRAMES_DROPPED: &str = "logit.input.frames.dropped";

/// How the file is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayFormat {
    Json,
    Native,
}

/// Reads one file once and sends its events downstream.
#[derive(Debug)]
pub struct ReplayInput {
    path: PathBuf,
    format: ReplayFormat,
    /// At least 1 ([`ReplayInput::new`] clamps it).
    batch: usize,
    file: Option<tokio::fs::File>,
    telemetry: Telemetry,
    diag: Diagnostics,
}

impl ReplayInput {
    pub fn new(path: PathBuf, format: ReplayFormat, batch: usize) -> Self {
        Self {
            path,
            format,
            batch: batch.max(1),
            file: None,
            telemetry: Telemetry::default(),
            diag: Diagnostics::default(),
        }
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    /// Sends `events` in batches of at most `self.batch`. Returns `false`, counted, when no
    /// consumer took one.
    async fn send(&self, sink: &Fanout, batch: EventBatch) -> bool {
        let EventBatch { resource, scope, mut events } = batch;
        while !events.is_empty() {
            let rest = events.split_off(events.len().min(self.batch));
            let batch = EventBatch { resource: resource.clone(), scope: scope.clone(), events };
            if !sink.send(batch).await {
                self.telemetry.count(
                    "logit.input.batches.dropped",
                    1.0,
                    &[("reason", "closed_consumer")],
                );
                self.diag.clone().warn_throttled(
                    "closed_consumer",
                    format_args!(
                        "no consumer took a batch from {}; the rest of the file is not read",
                        self.path.display()
                    ),
                );
                return false;
            }
            events = rest;
        }
        true
    }

    async fn run_json<R: AsyncRead + Unpin>(
        &mut self,
        reader: R,
        sink: &Fanout,
    ) -> anyhow::Result<()> {
        let mut reader = BufReader::new(reader);
        let mut decoder = EventJsonDecoder::new();
        let mut line = Vec::new();
        let mut number: u64 = 0;
        let mut pending: Option<EventBatch> = None;
        loop {
            line.clear();
            if reader.read_until(b'\n', &mut line).await? == 0 {
                break;
            }
            number += 1;
            let text = line.strip_suffix(b"\n").unwrap_or(&line);
            if text.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            self.telemetry.count("logit.input.lines", 1.0, &[]);
            self.telemetry.count("logit.input.line.bytes", line.len() as f64, &[]);
            let Line { resource, scope, event } = match decoder.decode_line(text) {
                Ok(decoded) => decoded,
                Err(err) => {
                    self.telemetry.count(FRAMES_DROPPED, 1.0, &[("reason", "undecodable")]);
                    self.diag.warn_throttled(
                        "bad_line",
                        format_args!("skipping line {number} of {}: {err}", self.path.display()),
                    );
                    continue;
                }
            };
            match &mut pending {
                // The decoder hands back the previous line's `Arc`s when they're equal.
                Some(b)
                    if Arc::ptr_eq(&b.resource, &resource)
                        && same_scope(&b.scope, &scope)
                        && b.events.len() < self.batch =>
                {
                    b.events.push(event)
                }
                _ => {
                    let next = EventBatch { resource, scope, events: vec![event] };
                    if let Some(full) = pending.replace(next) {
                        if !self.send(sink, full).await {
                            return Ok(());
                        }
                    }
                }
            }
        }
        if let Some(last) = pending {
            self.send(sink, last).await;
        }
        Ok(())
    }

    async fn run_native<R: AsyncRead + Unpin>(
        &mut self,
        mut reader: R,
        sink: &Fanout,
    ) -> anyhow::Result<()> {
        let budget_cap = MAX_SANE_UNCOMPRESSED_LEN;
        let mut buf = BytesMut::with_capacity(64 * 1024);
        // The file offset of `buf[0]`, for diagnostics.
        let mut offset: u64 = 0;
        // Set from a counted fault until a frame passes its CRC: the bytes in between are one
        // unreadable run, so a false magic inside them isn't counted again.
        let mut skipping = false;
        // Set from a counted `truncated` until a frame passes its CRC: a false magic inside the
        // torn frame's payload is cut short by the same end of file, and isn't counted again.
        let mut truncated = false;
        loop {
            if !fill(&mut reader, &mut buf, HEADER_LEN).await? {
                // Leftover bytes after a counted fault are part of its run, unless a resync
                // landed on a magic: then they're a torn header of their own.
                if !buf.is_empty() && !truncated && (!skipping || buf.starts_with(&MAGIC)) {
                    self.truncated(offset, buf.len());
                }
                break;
            }
            if buf[..MAGIC.len()] != MAGIC {
                if !skipping {
                    self.frame_fault("magic", offset, "no frame magic here");
                    skipping = true;
                }
                match frame::resync(&buf[1..]) {
                    Some(at) => {
                        buf.advance(1 + at);
                        offset += 1 + at as u64;
                    }
                    None => {
                        // Keep a tail that could be the start of a magic split across reads.
                        let drop = buf.len() - (MAGIC.len() - 1);
                        buf.advance(drop);
                        offset += drop as u64;
                        let want = buf.len() + 1;
                        if !fill(&mut reader, &mut buf, want).await? {
                            break;
                        }
                    }
                }
                continue;
            }
            let header = match FrameHeader::read(&mut Bytes::copy_from_slice(&buf[..HEADER_LEN])) {
                Ok(header) => header,
                Err(err) => {
                    let reason = match err {
                        CodecError::Unsupported(_) => "version",
                        _ => "malformed",
                    };
                    self.skip_header(reason, &mut buf, &mut offset, &mut skipping, err);
                    continue;
                }
            };
            if header.compressed_len > compressed_bound(MAX_SANE_UNCOMPRESSED_LEN)
                || header.uncompressed_len > MAX_SANE_UNCOMPRESSED_LEN
            {
                let err = format!(
                    "a body of {} bytes ({} uncompressed) is over the frame cap",
                    header.compressed_len, header.uncompressed_len
                );
                self.skip_header("too_large", &mut buf, &mut offset, &mut skipping, err);
                continue;
            }
            let len = HEADER_LEN + header.compressed_len as usize;
            if !fill(&mut reader, &mut buf, len).await? {
                // A frame torn mid-file, with a later write appended after it, declares a body
                // that runs into the frames behind it and past the end. Those frames are whole,
                // so read on from the next magic; only a frame with none behind it ends the read.
                let next = frame::resync(&buf[MAGIC.len()..]);
                if !truncated && (next.is_none() || !skipping) {
                    self.truncated(offset, buf.len());
                    truncated = true;
                }
                let Some(at) = next else { break };
                buf.advance(MAGIC.len() + at);
                offset += (MAGIC.len() + at) as u64;
                skipping = true;
                continue;
            }
            let consumed = buf.split_to(len).freeze();
            let at = offset;
            offset += len as u64;
            let mut whole = consumed.clone();
            let read = frame::read_frame_with_header(&mut whole);
            // The header's caps are checked above, so a failure other than the CRC comes after
            // it: a body that doesn't decompress, or a compression this build doesn't decode.
            let crc_failed =
                matches!(&read, Err(CodecError::Malformed(msg)) if msg.contains("crc32c"));
            if !crc_failed {
                skipping = false;
                truncated = false;
                self.telemetry.count("logit.input.frames", 1.0, &[]);
                self.telemetry.count("logit.input.frame.bytes", len as f64, &[]);
            }
            let (header, mut payload) = match read {
                Ok(read) => read,
                Err(err) if !crc_failed => {
                    self.frame_fault("malformed", at, err);
                    continue;
                }
                Err(err) => {
                    // A torn frame followed by a later write fails here, its declared length
                    // reaching into the frames behind it. Rescan from the byte after its magic, so
                    // a whole frame inside those bytes is still read; a false magic in a payload
                    // costs one more CRC check.
                    if !skipping {
                        self.frame_fault("crc", at, err);
                        skipping = true;
                    }
                    let mut rest = BytesMut::with_capacity(len - MAGIC.len() + buf.len());
                    rest.extend_from_slice(&consumed[MAGIC.len()..]);
                    rest.extend_from_slice(&buf);
                    buf = rest;
                    offset = at + MAGIC.len() as u64;
                    continue;
                }
            };
            if header.codec != CODEC_BATCH || header.flags & FLAG_CONTROL != 0 {
                self.frame_fault(
                    "codec",
                    at,
                    format_args!(
                        "codec byte {} (flags {:#x}), not a bare batch ({CODEC_BATCH})",
                        header.codec, header.flags
                    ),
                );
                continue;
            }
            let budget = DecodeBudget::for_frame_cap(budget_cap);
            let batch = match native::decode_batch(&mut payload, &budget) {
                Ok(batch) => batch,
                Err(err) => {
                    let reason = match err {
                        CodecError::BudgetExceeded { .. } => "decode_budget",
                        _ => "malformed",
                    };
                    self.frame_fault(reason, at, err);
                    continue;
                }
            };
            if !self.send(sink, batch).await {
                return Ok(());
            }
        }
        Ok(())
    }

    fn truncated(&mut self, offset: u64, have: usize) {
        self.telemetry.count(FRAMES_DROPPED, 1.0, &[("reason", "truncated")]);
        self.diag.warn_throttled(
            "truncated",
            format_args!(
                "{} holds only {have} bytes of the frame at offset {offset}",
                self.path.display()
            ),
        );
    }

    fn frame_fault(&mut self, reason: &'static str, offset: u64, err: impl std::fmt::Display) {
        self.telemetry.count(FRAMES_DROPPED, 1.0, &[("reason", reason)]);
        self.diag.warn_throttled(
            "bad_frame",
            format_args!("skipping the frame at offset {offset} of {}: {err}", self.path.display()),
        );
    }

    /// Counts a header fault, unless it's inside a run already counted, and steps past its
    /// magic: with no trustworthy length, the read resumes at the next magic.
    fn skip_header(
        &mut self,
        reason: &'static str,
        buf: &mut BytesMut,
        offset: &mut u64,
        skipping: &mut bool,
        err: impl std::fmt::Display,
    ) {
        if !*skipping {
            self.frame_fault(reason, *offset, err);
        }
        buf.advance(MAGIC.len());
        *offset += MAGIC.len() as u64;
        *skipping = true;
    }
}

/// Reads until `buf` holds at least `want` bytes. `false` when the file ends first.
async fn fill<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut BytesMut,
    want: usize,
) -> std::io::Result<bool> {
    while buf.len() < want {
        buf.reserve((want - buf.len()).max(64 * 1024));
        if reader.read_buf(buf).await? == 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

fn same_scope(a: &Option<Arc<Scope>>, b: &Option<Arc<Scope>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => Arc::ptr_eq(a, b),
        _ => false,
    }
}

#[async_trait::async_trait]
impl Input for ReplayInput {
    /// Opens the file, so a missing or unreadable one fails startup.
    async fn bind(&mut self) -> anyhow::Result<()> {
        if self.file.is_some() {
            return Ok(());
        }
        let path = self.path.display();
        let file = tokio::fs::File::open(&self.path)
            .await
            .map_err(|e| anyhow::anyhow!("opening {path} to replay: {e}"))?;
        let meta = file.metadata().await.map_err(|e| anyhow::anyhow!("reading {path}: {e}"))?;
        if meta.is_dir() {
            anyhow::bail!("{path} is a directory; replay_in reads one file");
        }
        self.file = Some(file);
        Ok(())
    }

    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        self.bind().await?;
        let file = self.file.take().expect("bind opened the file");
        let path = self.path.display().to_string();
        let result = match self.format {
            ReplayFormat::Json => self.run_json(file, &sink).await,
            ReplayFormat::Native => self.run_native(file, &sink).await,
        };
        result.map_err(|e| e.context(format!("reading {path}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner::intern;
    use logit_core::{
        AttrMap, BodyFormat, LogRecord, MetricKind, MetricRecord, MetricValue, Severity, Value,
    };
    use logit_core::{Event, Resource};
    use logit_outputs::stdio::StreamEncoder;
    use logit_pipeline::test_util::{fanout_channel, recv_batch, scratch_dir, TelemetryProbe};
    use logit_proto::frame::{write_frame, Compression};
    use logit_proto::Encoder;

    fn resource(service: &str) -> Arc<Resource> {
        let mut attributes = AttrMap::new();
        attributes.insert("service.name", Value::from(service));
        Arc::new(Resource { attributes, ..Resource::default() })
    }

    fn metric_event(ts: i64, name: &str, value: u64) -> Event {
        let kind = MetricKind::Gauge(MetricValue::U64(value));
        Event::metric(ts, AttrMap::new(), MetricRecord::new(intern(name), kind))
    }

    fn log_event(ts: i64, message: &str) -> Event {
        let record = LogRecord {
            message: Value::str(message),
            severity: Some(Severity::Info),
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        };
        Event::log(ts, AttrMap::new(), record)
    }

    /// The bytes a `stdio_out`/`file_out` writes for `batches`, through its own encoder.
    fn written(batches: &[EventBatch], mut encoder: StreamEncoder) -> Vec<u8> {
        let mut out = Vec::new();
        for batch in batches {
            out.extend_from_slice(&encoder.encode(batch).expect("encode a batch"));
        }
        out
    }

    fn json_lines(batches: &[EventBatch]) -> Vec<u8> {
        written(batches, StreamEncoder::json())
    }

    fn native_frames(batches: &[EventBatch], compression: Compression) -> Vec<u8> {
        written(batches, StreamEncoder::native(compression))
    }

    struct Replayed {
        batches: Vec<EventBatch>,
        probe: TelemetryProbe,
    }

    /// Runs a replay of `bytes` to its end and collects every batch it sent.
    async fn replay(bytes: &[u8], format: ReplayFormat, batch: usize) -> Replayed {
        let dir = scratch_dir("replay");
        let path = dir.join("events");
        std::fs::write(&path, bytes).expect("write the file");
        let probe = TelemetryProbe::new();
        let mut input = ReplayInput::new(path, format, batch).with_telemetry(probe.telemetry(
            "replay",
            "replay_in",
            "listener",
        ));
        input.bind().await.expect("bind opens the file");
        let (fanout, mut rx) = fanout_channel(1024);
        input.run(fanout).await.expect("the end of the file returns Ok");
        let mut batches = Vec::new();
        // `run` returned and dropped the fanout, so the channel ends after the last batch.
        while !rx.is_empty() {
            batches.push(recv_batch(&mut rx).await);
        }
        assert!(rx.recv().await.is_none(), "the fanout closes when run returns");
        Replayed { batches, probe }
    }

    fn shape(batches: &[EventBatch]) -> Vec<(String, usize)> {
        batches
            .iter()
            .map(|b| {
                let service = b.resource.attributes.get("service.name").and_then(Value::as_str);
                (service.unwrap_or_default().to_string(), b.events.len())
            })
            .collect()
    }

    fn three_runs() -> Vec<EventBatch> {
        vec![
            EventBatch {
                resource: resource("web"),
                scope: None,
                events: (0..5).map(|i| metric_event(1_000 + i, "requests", i as u64)).collect(),
            },
            EventBatch {
                resource: resource("db"),
                scope: None,
                events: vec![log_event(2_000, "slow query")],
            },
            EventBatch {
                resource: resource("web"),
                scope: None,
                events: vec![metric_event(3_000, "requests", u64::MAX)],
            },
        ]
    }

    /// A batch ends where the resource changes and at `batch` events, and every event reads back
    /// as written, timestamps included.
    #[tokio::test]
    async fn json_batches_split_at_a_resource_change_and_at_the_batch_bound() {
        let written = three_runs();
        let out = replay(&json_lines(&written), ReplayFormat::Json, 2).await;
        assert_eq!(
            shape(&out.batches),
            [
                ("web".into(), 2),
                ("web".into(), 2),
                ("web".into(), 1),
                ("db".into(), 1),
                ("web".into(), 1)
            ]
        );
        let read: Vec<&Event> = out.batches.iter().flat_map(|b| &b.events).collect();
        let want: Vec<&Event> = written.iter().flat_map(|b| &b.events).collect();
        assert_eq!(read, want);
        assert_eq!(out.batches[0].resource, written[0].resource);
    }

    /// Lines with one resource share one `Arc` across the batches the bound cuts.
    #[tokio::test]
    async fn json_batches_cut_by_the_bound_share_the_resource() {
        let out = replay(&json_lines(&three_runs()), ReplayFormat::Json, 2).await;
        assert!(Arc::ptr_eq(&out.batches[0].resource, &out.batches[1].resource));
    }

    #[tokio::test]
    async fn a_malformed_json_line_is_skipped_and_counted_and_the_rest_delivered() {
        let written = three_runs();
        let mut bytes = json_lines(&written[..1]);
        bytes.extend_from_slice(b"{\"timestamp\": \n\n");
        bytes.extend_from_slice(&json_lines(&written[1..]));
        let mut out = replay(&bytes, ReplayFormat::Json, 1000).await;
        assert_eq!(shape(&out.batches), [("web".into(), 5), ("db".into(), 1), ("web".into(), 1)]);
        assert_eq!(out.probe.sum(FRAMES_DROPPED, &[("reason", "undecodable")]), 1.0);
        assert_eq!(out.probe.sum("logit.input.lines", &[]), 8.0, "the blank line isn't counted");
    }

    /// A frame is one batch, cut only past `batch`, through either compression.
    #[tokio::test]
    async fn native_frames_read_back_one_batch_per_frame() {
        let written = three_runs();
        for compression in [Compression::None, Compression::Lz4] {
            let mut out =
                replay(&native_frames(&written, compression), ReplayFormat::Native, 3).await;
            assert_eq!(
                shape(&out.batches),
                [("web".into(), 3), ("web".into(), 2), ("db".into(), 1), ("web".into(), 1)]
            );
            let read: Vec<&Event> = out.batches.iter().flat_map(|b| &b.events).collect();
            let want: Vec<&Event> = written.iter().flat_map(|b| &b.events).collect();
            assert_eq!(read, want);
            assert_eq!(out.probe.sum("logit.input.frames", &[]), 3.0);
        }
    }

    #[tokio::test]
    async fn a_torn_final_native_frame_ends_the_file_and_is_counted() {
        let written = three_runs();
        let mut bytes = native_frames(&written, Compression::None);
        bytes.truncate(bytes.len() - 5);
        let mut out = replay(&bytes, ReplayFormat::Native, 1000).await;
        assert_eq!(shape(&out.batches), [("web".into(), 5), ("db".into(), 1)]);
        assert_eq!(out.probe.sum(FRAMES_DROPPED, &[("reason", "truncated")]), 1.0);
    }

    /// A CRC failure, a hop batch, and garbage between frames are each counted once under their
    /// reason, and every whole frame after each is still read.
    #[tokio::test]
    async fn bad_native_frames_are_skipped_and_counted_by_reason() {
        let written = three_runs();
        let mut bytes = native_frames(&written[..1], Compression::None);
        let mut corrupt = native_frames(&written[1..2], Compression::None);
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0xff;
        bytes.extend_from_slice(&corrupt);
        let hop = write_frame(native::CODEC_HOP_BATCH, Compression::None, b"whatever").unwrap();
        bytes.extend_from_slice(&hop);
        bytes.extend_from_slice(b"not a frame at all, and longer than a header");
        bytes.extend_from_slice(&native_frames(&written[2..], Compression::None));
        let mut out = replay(&bytes, ReplayFormat::Native, 1000).await;
        assert_eq!(shape(&out.batches), [("web".into(), 5), ("web".into(), 1)]);
        let totals = out.probe.poll();
        assert_eq!(totals.sum(FRAMES_DROPPED, &[("reason", "crc")]), 1.0);
        assert_eq!(totals.sum(FRAMES_DROPPED, &[("reason", "codec")]), 1.0);
        assert_eq!(totals.sum(FRAMES_DROPPED, &[("reason", "magic")]), 1.0);
        assert_eq!(totals.sum(FRAMES_DROPPED, &[("reason", "truncated")]), 0.0);
    }

    fn dropped(probe: &mut TelemetryProbe) -> Vec<(&'static str, f64)> {
        let totals = probe.poll();
        ["magic", "version", "too_large", "crc", "codec", "decode_budget", "malformed", "truncated"]
            .into_iter()
            .map(|reason| (reason, totals.sum(FRAMES_DROPPED, &[("reason", reason)])))
            .filter(|(_, n)| *n > 0.0)
            .collect()
    }

    /// A frame torn mid-file, with a later run's frames appended behind it (`file_out` opens its
    /// file to append), declares a length reaching into those frames. It fails its CRC, and the
    /// whole frames inside the bytes it claimed are still read.
    #[tokio::test]
    async fn a_torn_frame_with_frames_appended_behind_it_loses_only_itself() {
        let written = three_runs();
        let mut bytes = native_frames(&written[..1], Compression::None);
        let db = native_frames(&written[1..2], Compression::None);
        let body = db.len() - HEADER_LEN;
        bytes.extend_from_slice(&db[..HEADER_LEN + body / 2]);
        bytes.extend_from_slice(&native_frames(&written[..1], Compression::None));
        bytes.extend_from_slice(&native_frames(&written[2..], Compression::None));
        let mut out = replay(&bytes, ReplayFormat::Native, 1000).await;
        assert_eq!(shape(&out.batches), [("web".into(), 5), ("web".into(), 5), ("web".into(), 1)]);
        assert_eq!(dropped(&mut out.probe), [("crc", 1.0)]);
    }

    /// A torn frame whose declared length runs past the end of the file, with whole frames
    /// behind it, doesn't end the read.
    #[tokio::test]
    async fn a_torn_frame_declaring_more_than_the_file_holds_loses_only_itself() {
        let written = three_runs();
        let big = EventBatch {
            resource: resource("db"),
            scope: None,
            events: (0..500)
                .map(|i| log_event(i, "a long enough message to pad the frame"))
                .collect(),
        };
        let mut bytes = native_frames(&written[..1], Compression::None);
        let torn = native_frames(std::slice::from_ref(&big), Compression::None);
        bytes.extend_from_slice(&torn[..HEADER_LEN + 64]);
        bytes.extend_from_slice(&native_frames(&written[1..], Compression::None));
        let mut out = replay(&bytes, ReplayFormat::Native, 1000).await;
        assert_eq!(shape(&out.batches), [("web".into(), 5), ("db".into(), 1), ("web".into(), 1)]);
        assert_eq!(dropped(&mut out.probe), [("truncated", 1.0)]);
    }

    /// Trailing bytes of a garbage run already counted aren't counted again as `truncated`.
    #[tokio::test]
    async fn the_end_of_a_garbage_run_is_not_counted_again() {
        // Longer than one 64 KiB read, so the run ends with a short remainder.
        let mut out = replay(&vec![b'x'; 65_546], ReplayFormat::Native, 1000).await;
        assert!(out.batches.is_empty());
        assert_eq!(dropped(&mut out.probe), [("magic", 1.0)]);
    }

    /// A last frame of a version this build doesn't read, with an empty body, is one `version`.
    #[tokio::test]
    async fn an_unknown_version_last_frame_is_counted_once() {
        let mut bytes = native_frames(&three_runs()[..1], Compression::None);
        let mut header =
            native_frames(&three_runs()[..1], Compression::None)[..HEADER_LEN].to_vec();
        header[4..6].copy_from_slice(&99u16.to_le_bytes());
        header[16..20].copy_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&header);
        let mut out = replay(&bytes, ReplayFormat::Native, 1000).await;
        assert_eq!(shape(&out.batches), [("web".into(), 5)]);
        assert_eq!(dropped(&mut out.probe), [("version", 1.0)]);
    }

    /// A torn header a resync lands on is counted, beside the garbage run before it.
    #[tokio::test]
    async fn a_torn_header_after_garbage_is_counted_truncated() {
        let mut bytes = native_frames(&three_runs()[..1], Compression::None);
        bytes.extend_from_slice(&[b'x'; 30]);
        bytes.extend_from_slice(&native_frames(&three_runs()[2..], Compression::None)[..12]);
        let mut out = replay(&bytes, ReplayFormat::Native, 1000).await;
        assert_eq!(shape(&out.batches), [("web".into(), 5)]);
        assert_eq!(dropped(&mut out.probe), [("magic", 1.0), ("truncated", 1.0)]);
    }

    /// A torn final frame whose payload holds a false magic near its end is counted once: the
    /// false magic is cut short by the same end of file.
    #[tokio::test]
    async fn a_false_magic_in_a_torn_final_frame_is_not_counted_again() {
        let mut bytes = native_frames(&three_runs()[..1], Compression::None);
        let mut header =
            native_frames(&three_runs()[2..], Compression::None)[..HEADER_LEN].to_vec();
        header[16..20].copy_from_slice(&1000u32.to_le_bytes());
        bytes.extend_from_slice(&header);
        bytes.extend_from_slice(&[b'a'; 100]);
        bytes.extend_from_slice(&MAGIC);
        bytes.extend_from_slice(&[b'b'; 10]);
        let mut out = replay(&bytes, ReplayFormat::Native, 1000).await;
        assert_eq!(shape(&out.batches), [("web".into(), 5)]);
        assert_eq!(dropped(&mut out.probe), [("truncated", 1.0)]);
    }

    /// A frame that passes its CRC and then fails to decompress, read in the run after a torn
    /// frame, is counted under its own reason and as a frame read.
    #[tokio::test]
    async fn a_crc_valid_frame_that_fails_to_decompress_is_counted_on_its_own() {
        let written = three_runs();
        let db = native_frames(&written[1..2], Compression::None);
        let mut bytes = db[..HEADER_LEN + (db.len() - HEADER_LEN) / 2].to_vec();
        // The CRC covers the stored bytes, so relabeling an uncompressed body as lz4 keeps it
        // valid and makes the body fail to decompress.
        let mut bad = write_frame(CODEC_BATCH, Compression::None, &[0xf0; 256]).unwrap().to_vec();
        bad[9] = Compression::Lz4 as u8;
        bytes.extend_from_slice(&bad);
        bytes.extend_from_slice(&native_frames(&written[2..], Compression::None));
        let mut out = replay(&bytes, ReplayFormat::Native, 1000).await;
        assert_eq!(shape(&out.batches), [("web".into(), 1)]);
        assert_eq!(dropped(&mut out.probe), [("crc", 1.0), ("malformed", 1.0)]);
        assert_eq!(out.probe.sum("logit.input.frames", &[]), 2.0);
    }

    #[tokio::test]
    async fn a_missing_file_fails_bind() {
        let path = scratch_dir("replay-missing").join("absent.json");
        let mut input = ReplayInput::new(path, ReplayFormat::Json, 1000);
        let err = input.bind().await.expect_err("a missing file fails bind");
        assert!(err.to_string().contains("absent.json"), "names the path: {err}");
    }

    #[tokio::test]
    async fn an_empty_file_ends_the_run_with_ok_and_no_batch() {
        let out = replay(b"", ReplayFormat::Json, 1000).await;
        assert!(out.batches.is_empty());
        let out = replay(b"", ReplayFormat::Native, 1000).await;
        assert!(out.batches.is_empty());
    }
}
