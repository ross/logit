//! `stdio_out`: a debug sink that writes a pipeline's events as readable text (default) or NDJSON
//! to stdout (default), stderr, or a file. Also home of [`StreamOutput`], which `file_out`
//! (`crate::file`) builds on: `stdio_out`'s file target is `file_out` with an empty rotation
//! policy, not a second implementation (`docs/adr/rotating-file-output.md`).
//!
//! The default render is [`crate::human`]'s block format, whose module doc is its grammar
//! (`docs/adr/human-render-block-format.md`).
//! A program reads [`Format::Json`] instead, one JSON object per event per line, whose grammar
//! is [`crate::ndjson`]'s module doc (`docs/adr/stream-json-format.md`).
//!
//! Split like `InfluxDbOutput`/`InfluxLineEncoder`: a pure [`EventDump`] encoder with no file
//! descriptor (every format test runs against it alone), and the thin [`StreamOutput`] that owns
//! the target. [`StreamEncoder`] picks `EventDump` or `logit_proto::native::NativeEncoder`
//! (`format: native`, `docs/adr/file-output-native-format.md`); `Target`/`FileTarget` never see
//! which.
//!
//! **Response classes**, for `stdio_out` and `file_out` alike. A file or stream has no
//! application response, so the I/O error decides. The Evidence column names the test that pins
//! each row.
//!
//! | Response | Class | Why | Evidence |
//! |---|---|---|---|
//! | an encode error | `Rejected` (no `Fault` attached) | about this batch: it would encode the same way again | -- |
//! | a failed re-open of the active file after a rotation's rename | `Clean` | the batch provably reached no file, so a retry is safe under either posture; the failure is likely transient (`ENOSPC`, `EMFILE`) | `a_failed_reopen_is_classified_clean_so_the_batch_is_retried_rather_than_dropped` |
//! | a failed write or flush of the active file (a full disk, a permission lost, an I/O error), or of stdout or stderr | `Rejected` (no `Fault` attached) | part of the batch may already be in the file, and a resend would repeat it; the write is local, so nothing tells this batch's failure from the next one's | `a_failed_write_or_flush_of_the_active_file_is_rejected` |
//! | a failed rotation rename or `max_files: 1` truncate | no fault: the batch is written to the current file, `rotate_failure` warns, and the next batch retries the rotation | nothing on disk changed | `a_failed_truncate_under_max_files_one_is_not_rotated_and_keeps_writing_to_the_existing_file` |
//!
//! A full disk drops batch after batch while it lasts, counted
//! `batches.dropped{reason="rejected"}`, where holding them as `Refused` would keep them until
//! space came back. That trade is the current one, unchanged by
//! [ADR `sink-fault-classes`](../../../docs/adr/sink-fault-classes.md): reading a write error as
//! `Refused` needs the write to be all-or-nothing, which a partial `write_all` isn't, or a
//! truncate back to the batch's start on failure, which this sink doesn't do.
//!
//! **Delivery posture.** The one case the posture decides is a write the shutdown grace cuts off,
//! which is `Ambiguous`: under the default, `at_least_once`
//! (`docs/adr/delivery-semantics.md`, item 5), the batch stays queued, so a `buffer.disk:` spool
//! replays it after a restart and the file can repeat its block; under `at_most_once` it's
//! dropped and counted `reason="shutdown"`.

use crate::file::{FileTarget, RotateOutcome, RotatePolicy};
pub use crate::human::{EventDump, Format, MessageMode};
use crate::Output;
use anyhow::Context;
use bytes::Bytes;
use logit_core::{Diagnostics, EventBatch, Telemetry};
use logit_proto::frame::Compression as NativeCompression;
use logit_proto::native::NativeEncoder;
use logit_proto::{CodecError, Encoder};
use std::path::Path;
use tokio::io::{self, AsyncWriteExt};

/// Which encoder [`StreamOutput`] writes through: [`EventDump`]'s block or NDJSON render, or
/// `logit_proto::native::NativeEncoder` (`docs/adr/file-output-native-format.md`).
///
/// An enum, not `Box<dyn Encoder>`, so `StreamOutput<StreamEncoder>` is the one concrete type
/// `build_spec` constructs. Both encoders are `Copy` and carry no state across calls:
/// `NativeEncoder` rebuilds its dictionary inside every `encode()`, so each frame decodes on its
/// own, which is what lets `file_out` rotate mid-stream without stranding a reader.
#[derive(Debug, Clone, Copy)]
pub enum StreamEncoder {
    Dump(EventDump),
    Native(NativeEncoder),
}

impl StreamEncoder {
    /// The block render in its default [`MessageMode::Escaped`].
    pub fn human() -> Self {
        StreamEncoder::Dump(EventDump::default())
    }

    pub fn json() -> Self {
        StreamEncoder::Dump(EventDump::new(Format::Json))
    }

    pub fn human_with(message: MessageMode) -> Self {
        StreamEncoder::Dump(EventDump::default().with_message_mode(message))
    }

    pub fn native(compression: NativeCompression) -> Self {
        StreamEncoder::Native(NativeEncoder::new(compression))
    }
}

impl Encoder for StreamEncoder {
    fn encode(&mut self, batch: &EventBatch) -> Result<Bytes, CodecError> {
        match self {
            StreamEncoder::Dump(e) => e.encode(batch),
            StreamEncoder::Native(e) => e.encode(batch),
        }
    }
}

/// The open destination [`StreamOutput`] writes to. `File` always carries a [`FileTarget`] with a
/// [`RotatePolicy`] ([`RotatePolicy::never`] for `stdio_out`), which is what makes `stdio_out`'s
/// file target `file_out` without rotation in the type system.
#[derive(Debug)]
enum Target {
    Stdout(io::Stdout),
    Stderr(io::Stderr),
    File(FileTarget),
}

/// The `stdio_out` and `file_out` sink, generic over its [`Encoder`]
/// (`docs/adr/rotating-file-output.md`). The two differ only in `target`: `stdio_out` never
/// rotates, `file_out` carries a real policy. `build_spec` picks the constructor from the config.
#[derive(Debug)]
pub struct StreamOutput<E> {
    target: Target,
    encoder: E,
    telemetry: Telemetry,
    /// Only for a rotating file target's two non-fatal failures, `FileTarget::rotate`'s
    /// `rotate_failure`/`retention_failure`; every other failure returns `Err` from `send`.
    diagnostics: Diagnostics,
}

impl StreamOutput<StreamEncoder> {
    pub fn stdout() -> Self {
        Self {
            target: Target::Stdout(io::stdout()),
            encoder: StreamEncoder::human(),
            telemetry: Telemetry::default(),
            diagnostics: Diagnostics::default(),
        }
    }

    pub fn stderr() -> Self {
        Self {
            target: Target::Stderr(io::stderr()),
            encoder: StreamEncoder::human(),
            telemetry: Telemetry::default(),
            diagnostics: Diagnostics::default(),
        }
    }

    /// Opens (creating if needed) `path` for append, never rotating ([`RotatePolicy::never`]).
    ///
    /// Eager, at config-build time, so a bad path or permissions error fails startup before
    /// anything listens. `path` is used as given; `build_spec` resolves a relative one against the
    /// config file's directory.
    pub fn open_path(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Self::rotating(path, RotatePolicy::never())
    }

    /// The `file_out` constructor: [`Self::open_path`] under a real [`RotatePolicy`].
    pub fn rotating(path: impl AsRef<Path>, policy: RotatePolicy) -> anyhow::Result<Self> {
        let file = FileTarget::open(path, policy)?;
        Ok(Self {
            target: Target::File(file),
            encoder: StreamEncoder::human(),
            telemetry: Telemetry::default(),
            diagnostics: Diagnostics::default(),
        })
    }

    /// Replaces the `human()` default every constructor starts with; `build_spec` calls this for
    /// `format: native` (`docs/adr/file-output-native-format.md`).
    pub fn with_format(mut self, encoder: StreamEncoder) -> Self {
        self.encoder = encoder;
        self
    }
}

impl<E> StreamOutput<E> {
    /// Attaches a telemetry handle -- see `send`'s `logit.output.batch.bytes`.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Attaches a diagnostics handle for the two rotation keys the `diagnostics` field names.
    pub fn with_diagnostics(mut self, diagnostics: Diagnostics) -> Self {
        self.diagnostics = diagnostics;
        self
    }
}

#[async_trait::async_trait]
impl<E: Encoder + Send> Output for StreamOutput<E> {
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        // Check the input, not the encoded bytes: `NativeEncoder` emits a non-empty frame
        // (header, dictionary, resource) even for zero events.
        if batch.events.is_empty() {
            return Ok(());
        }
        let bytes = self.encoder.encode(batch).context("encoding batch")?;

        // One `write_all` and one `flush` per batch, so nothing sits in tokio's buffer between
        // batches. `flush` is not `fsync`: the OS page cache still holds the bytes. A write error
        // carries no `Fault`, so the runtime classifies it `Rejected` and doesn't retry the batch.
        // A failed re-open after rotation is the exception: `Fault::Clean` (`FileTarget::rotate`).
        match &mut self.target {
            Target::Stdout(w) => {
                w.write_all(&bytes).await?;
                w.flush().await?;
            }
            Target::Stderr(w) => {
                w.write_all(&bytes).await?;
                w.flush().await?;
            }
            Target::File(file) => {
                // Rotation is decided before the write, so a batch is never split across files
                // (`FileTarget::should_rotate` on a batch bigger than `max_bytes`).
                let now = crate::file::now_unix();
                if file.should_rotate(now, bytes.len()) {
                    let rotated = file.rotate(&mut self.diagnostics).await;
                    // Counted here because `FileTarget` holds no `Telemetry`. `NotRotated` means
                    // the active file's rename or truncate failed and nothing on disk changed, so
                    // it isn't a rotation. An `Err` after the commit-point rename, a failed
                    // re-open, is one, and leaves the file closed.
                    let committed = match &rotated {
                        Ok(outcome) => *outcome == RotateOutcome::Rotated,
                        Err(_) => file.awaiting_reopen(),
                    };
                    if committed {
                        self.telemetry.count("logit.output.file.rotations", 1.0, &[]);
                    }
                    // `committed` already read the outcome.
                    let _ = rotated?;
                }
                file.write_all(&bytes).await?;
                file.flush().await?;
                // After the write, so a retry of a batch whose write failed isn't noted twice
                // and doesn't rotate early.
                file.note_written(now, bytes.len());
            }
        }
        // Once, for the attempt that wrote the batch: an attempt that failed before the write
        // wrote nothing.
        self.telemetry.count("logit.output.batch.bytes", bytes.len() as f64, &[]);
        Ok(())
    }

    /// Not the default no-op: `send` flushes every batch anyway, but this keeps the runtime's
    /// shutdown flush (`finish_and_flush`) meaningful if that ever changes.
    async fn flush(&mut self) -> anyhow::Result<()> {
        match &mut self.target {
            Target::Stdout(w) => w.flush().await.context("flushing stdout")?,
            Target::Stderr(w) => w.flush().await.context("flushing stderr")?,
            Target::File(f) => f.flush().await.context("flushing file target")?,
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, Event, MetricKind, MetricRecord, Resource};
    use std::sync::Arc;

    fn batch_with(events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
    }

    fn metric_event(ts: i64, name: &str, kind: MetricKind) -> Event {
        Event::metric(
            ts,
            AttrMap::new(),
            MetricRecord::new(logit_core::interner::intern(name), kind),
        )
    }

    #[tokio::test]
    async fn send_writes_the_encoded_batch_to_a_file_target_and_flushes() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("logit-stdio-out-test-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let mut output = StreamOutput::open_path(&path).expect("path should open");
        output
            .send(&batch_with(vec![metric_event(0, "x", MetricKind::counter(1.0))]))
            .await
            .expect("send should succeed");

        let contents = std::fs::read_to_string(&path).expect("file should exist and be readable");
        assert!(contents.contains("  - name: x\n    kind: sum\n    value: 1\n"), "got: {contents}");
        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn send_appends_across_multiple_batches_rather_than_truncating() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("logit-stdio-out-test-append-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let mut output = StreamOutput::open_path(&path).expect("path should open");
        output
            .send(&batch_with(vec![metric_event(0, "first", MetricKind::counter(1.0))]))
            .await
            .expect("send should succeed");
        output
            .send(&batch_with(vec![metric_event(0, "second", MetricKind::counter(2.0))]))
            .await
            .expect("send should succeed");

        let contents = std::fs::read_to_string(&path).expect("file should exist and be readable");
        assert!(
            contents.contains("  - name: first\n    kind: sum\n    value: 1\n"),
            "got: {contents}"
        );
        assert!(
            contents.contains("  - name: second\n    kind: sum\n    value: 2\n"),
            "got: {contents}"
        );
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn open_path_reports_a_clear_path_naming_error_for_an_unopenable_path() {
        // A missing parent directory fails regardless of permissions.
        let path = std::env::temp_dir().join("logit-stdio-out-test-no-such-dir").join("x.log");
        let err = StreamOutput::open_path(&path).expect_err("expected an error");
        assert!(format!("{err:?}").contains(&path.display().to_string()), "got: {err:?}");
    }

    #[tokio::test]
    async fn send_on_an_empty_batch_writes_nothing_and_does_not_error() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("logit-stdio-out-test-empty-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let mut output = StreamOutput::open_path(&path).expect("path should open");
        output.send(&batch_with(vec![])).await.expect("send should succeed");

        let contents = std::fs::read_to_string(&path).unwrap_or_default();
        assert_eq!(contents, "", "an empty batch should write nothing");
        std::fs::remove_file(&path).ok();
    }

    /// `logit.output.batch.bytes` equals the encoded length written to the file.
    #[tokio::test]
    async fn send_records_batch_bytes_matching_the_actual_encoded_length() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("logit-stdio-out-test-telemetry-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("tap", "stdio_out", "sink");
        let mut output =
            StreamOutput::open_path(&path).expect("path should open").with_telemetry(telemetry);
        output
            .send(&batch_with(vec![metric_event(0, "x", MetricKind::counter(1.0))]))
            .await
            .expect("send should succeed");

        let contents = std::fs::read(&path).expect("file should exist and be readable");
        std::fs::remove_file(&path).ok();

        let events = registry.drain(0);
        let recorded = events
            .iter()
            .find_map(|e| {
                e.metrics.iter().find_map(|m| match &m.kind {
                    MetricKind::Sum(s)
                        if logit_core::interner::resolve(m.name) == "logit.output.batch.bytes" =>
                    {
                        Some(s.value)
                    }
                    _ => None,
                })
            })
            .expect("logit.output.batch.bytes should have been recorded");
        assert_eq!(recorded, contents.len() as f64);
    }

    /// `stdio_out`'s file target never rotates through `send`, however much is written.
    #[tokio::test]
    async fn open_path_never_rotates_no_matter_how_much_is_written() {
        let dir = std::env::temp_dir();
        let path =
            dir.join(format!("logit-stdio-out-test-never-rotate-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let mut output = StreamOutput::open_path(&path).expect("path should open");
        for i in 0..20 {
            output
                .send(&batch_with(vec![metric_event(0, "x", MetricKind::counter(i as f64))]))
                .await
                .expect("send should succeed");
        }

        assert!(
            !dir.join(format!("logit-stdio-out-test-never-rotate-{}.log.1", std::process::id()))
                .exists(),
            "an unrotated target must never create a .1"
        );
        std::fs::remove_file(&path).ok();
    }

    /// `send`'s should_rotate/rotate/note_written sequencing and `logit.output.file.rotations`.
    #[tokio::test]
    async fn rotating_via_stream_output_rotates_and_counts_the_rotation() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("logit-stdio-out-test-rotating-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let rotated =
            dir.join(format!("logit-stdio-out-test-rotating-{}.log.1", std::process::id()));
        let _ = std::fs::remove_file(&rotated);

        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("tap", "file_out", "sink");
        let policy = RotatePolicy { max_bytes: Some(1), interval: None, max_files: 5 };
        let mut output = StreamOutput::rotating(&path, policy)
            .expect("path should open")
            .with_telemetry(telemetry);

        output
            .send(&batch_with(vec![metric_event(0, "first", MetricKind::counter(1.0))]))
            .await
            .expect("send should succeed");
        output
            .send(&batch_with(vec![metric_event(0, "second", MetricKind::counter(2.0))]))
            .await
            .expect("send should succeed");

        assert!(rotated.exists(), "the first batch should have been rotated out to .1");
        let rotated_contents = std::fs::read_to_string(&rotated).unwrap();
        assert!(rotated_contents.contains("first"), "got: {rotated_contents}");
        let active_contents = std::fs::read_to_string(&path).unwrap();
        assert!(active_contents.contains("second"), "got: {active_contents}");

        let events = registry.drain(0);
        let rotations = events
            .iter()
            .find_map(|e| {
                e.metrics.iter().find_map(|m| match &m.kind {
                    MetricKind::Sum(s)
                        if logit_core::interner::resolve(m.name)
                            == "logit.output.file.rotations" =>
                    {
                        Some(s.value)
                    }
                    _ => None,
                })
            })
            .expect("logit.output.file.rotations should have been recorded");
        assert_eq!(rotations, 1.0, "exactly one rotation should have happened");

        std::fs::remove_file(&path).ok();
        std::fs::remove_file(&rotated).ok();
    }

    /// A failed active-file rename (`RotateOutcome::NotRotated`) isn't counted as a rotation.
    #[tokio::test]
    async fn a_rotation_that_could_not_rename_the_active_file_is_never_counted_as_a_rotation() {
        let dir = std::env::temp_dir();
        let path =
            dir.join(format!("logit-stdio-out-test-failed-rotate-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("tap", "file_out", "sink");
        let policy = RotatePolicy { max_bytes: Some(1), interval: None, max_files: 5 };
        let mut output = StreamOutput::rotating(&path, policy)
            .expect("path should open")
            .with_telemetry(telemetry);

        output
            .send(&batch_with(vec![metric_event(0, "first", MetricKind::counter(1.0))]))
            .await
            .expect("send should succeed");

        // The fd stays valid, but `rotate`'s rename now has nothing at `path`.
        std::fs::remove_file(&path).ok();

        output
            .send(&batch_with(vec![metric_event(0, "second", MetricKind::counter(2.0))]))
            .await
            .expect("send should still succeed even though the rename underneath it failed");

        let events = registry.drain(0);
        let rotations = events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(s)
                    if logit_core::interner::resolve(m.name) == "logit.output.file.rotations" =>
                {
                    Some(s.value)
                }
                _ => None,
            })
        });
        assert!(rotations.is_none(), "a failed rotation must never be counted, got: {rotations:?}");

        std::fs::remove_file(&path).ok();
    }

    /// A rotation commits, then the re-open fails twice: once inside the rotation and once when
    /// the retry's write re-opens the file. The third attempt writes. The committed rotation
    /// counts once, and the batch's bytes count once, in `batch.bytes` and in the size the
    /// rotation policy tracks, so the next rotation isn't early.
    #[tokio::test]
    async fn a_rotation_whose_reopen_fails_counts_once_and_the_retries_count_no_bytes_twice() {
        use logit_pipeline::fault::{self, errno, sites, Op, Point};
        use logit_pipeline::test_util::{drive_write_loop, scratch_dir, TelemetryProbe};

        let small = batch_with(vec![metric_event(0, "b", MetricKind::counter(2.0))]);
        let len = StreamEncoder::human().encode(&small).unwrap().len();
        let large: Vec<Event> =
            (0..3).map(|i| metric_event(0, "a", MetricKind::counter(f64::from(i)))).collect();
        let large = batch_with(large);
        let large_len = StreamEncoder::human().encode(&large).unwrap().len();
        assert!(large_len > len);

        let dir = scratch_dir("stream-output-reopen-retry");
        let path = dir.join("events.log");
        let mut probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("out", "file_out", "sink");
        // Two of `small` fit; `large` then `small` crosses the bound and rotates.
        let policy = RotatePolicy { max_bytes: Some(2 * len as u64), interval: None, max_files: 3 };
        let mut output = StreamOutput::rotating(&path, policy)
            .expect("path should open")
            .with_telemetry(telemetry.clone());
        output.send(&large).await.expect("the first batch never rotates");

        let scope = fault::scope(&dir);
        let open = Point::new(sites::FILE_OUT_ACTIVE, Op::Open);
        // Two rules failing their first hit: a rule counts only the hits no earlier rule failed,
        // so the second rule's first hit is the second re-open (`docs/known-gaps/runtime.md`,
        // "Pipeline runtime and graph", the `fault` seam entry).
        scope.fail_nth(open, 1, errno::EMFILE).fail_nth(open, 1, errno::EMFILE);
        let config = crate::test_support::fast_retry();
        drive_write_loop(&mut output, vec![small], config, telemetry).await;
        drop(scope);

        let totals = probe.poll();
        assert_eq!(totals.sum("logit.component.errors", &[]), 2.0, "two failed re-opens");
        assert_eq!(totals.sum("logit.component.batches.delivered", &[]), 1.0);
        assert_eq!(totals.sum("logit.output.file.rotations", &[]), 1.0, "the rotation committed");
        assert_eq!(totals.sum("logit.output.batch.bytes", &[]), (large_len + len) as f64);
        assert!(std::fs::read_to_string(dir.join("events.log.1")).unwrap().contains("a"));
        assert!(std::fs::read_to_string(&path).unwrap().contains("b"));
        let Target::File(file) = &output.target else { panic!("a file target") };
        assert!(
            !file.should_rotate(crate::file::now_unix(), len),
            "one batch of `len` bytes is in the new file, so a second one still fits"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    // --- StreamEncoder ---

    #[test]
    fn stream_encoder_human_delegates_to_event_dump() {
        let mut encoder = StreamEncoder::human();
        let bytes = encoder
            .encode(&batch_with(vec![metric_event(0, "x", MetricKind::counter(1.0))]))
            .expect("should encode");
        let text = String::from_utf8(bytes.to_vec()).expect("human output is always valid utf-8");
        assert!(text.contains("  - name: x\n    kind: sum\n    value: 1\n"), "got: {text}");
    }

    /// `StreamEncoder::Native`'s output decodes through `read_frame` + `decode_batch`.
    #[test]
    fn stream_encoder_native_round_trips_through_the_real_native_decoder() {
        let mut encoder = StreamEncoder::native(NativeCompression::None);
        let mut bytes = encoder
            .encode(&batch_with(vec![metric_event(0, "x", MetricKind::counter(1.0))]))
            .expect("native encode should succeed");

        let (codec_id, mut payload) =
            logit_proto::frame::read_frame(&mut bytes).expect("frame should read");
        assert_eq!(codec_id, logit_proto::native::CODEC_BATCH);
        let decoded = logit_proto::native::decode_batch(&mut payload, &Default::default())
            .expect("payload should decode");
        assert_eq!(decoded.events.len(), 1);
        match &decoded.events[0].metrics[0].kind {
            MetricKind::Sum(s) => assert_eq!(s.value, 1.0),
            other => panic!("expected Sum, got {other:?}"),
        }
    }

    /// An empty batch writes nothing under `format: native`, whose encoder emits a frame anyway.
    #[tokio::test]
    async fn send_on_an_empty_batch_writes_nothing_under_native_format_either() {
        let dir = std::env::temp_dir();
        let path =
            dir.join(format!("logit-stdio-out-test-native-empty-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let mut output = StreamOutput::open_path(&path)
            .expect("path should open")
            .with_format(StreamEncoder::native(NativeCompression::None));
        output.send(&batch_with(vec![])).await.expect("send should succeed");

        let contents = std::fs::read(&path).unwrap_or_default();
        assert!(contents.is_empty(), "an empty batch under format: native should write nothing");
        std::fs::remove_file(&path).ok();
    }

    /// Under `format: native`, the rotated `.1` and the fresh active file each decode on their own
    /// (`docs/adr/file-output-native-format.md`).
    #[tokio::test]
    async fn rotating_under_native_format_leaves_both_files_independently_decodable() {
        let dir = std::env::temp_dir();
        let path =
            dir.join(format!("logit-stdio-out-test-native-rotate-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let rotated =
            dir.join(format!("logit-stdio-out-test-native-rotate-{}.log.1", std::process::id()));
        let _ = std::fs::remove_file(&rotated);

        let policy = RotatePolicy { max_bytes: Some(1), interval: None, max_files: 5 };
        let mut output = StreamOutput::rotating(&path, policy)
            .expect("path should open")
            .with_format(StreamEncoder::native(NativeCompression::None));

        output
            .send(&batch_with(vec![metric_event(0, "first", MetricKind::counter(1.0))]))
            .await
            .expect("send should succeed");
        output
            .send(&batch_with(vec![metric_event(0, "second", MetricKind::counter(2.0))]))
            .await
            .expect("send should succeed");
        assert!(rotated.exists(), "the first batch should have been rotated out to .1");

        for (file_path, expected_name) in [(&rotated, "first"), (&path, "second")] {
            let mut bytes = Bytes::from(std::fs::read(file_path).unwrap());
            let (codec_id, mut payload) =
                logit_proto::frame::read_frame(&mut bytes).expect("frame should read");
            assert_eq!(codec_id, logit_proto::native::CODEC_BATCH);
            let decoded = logit_proto::native::decode_batch(&mut payload, &Default::default())
                .expect("payload should decode");
            assert_eq!(
                logit_core::interner::resolve(decoded.events[0].metrics[0].name),
                expected_name,
                "got the wrong events out of {}",
                file_path.display()
            );
        }

        std::fs::remove_file(&path).ok();
        std::fs::remove_file(&rotated).ok();
    }
}
