//! Streaming gzip and zstd decompression for `replay_in` ([`crate::replay`]), chosen by a file's
//! leading bytes ([ADR `replay-input`](../../../docs/adr/replay-input.md), 2026-10-30 amendment).
//!
//! The decoders are blocking `Read`s, so [`spawn`] runs one on a blocking thread and writes its
//! output into a bounded in-memory pipe the async line and frame readers consume. The pipe's
//! capacity is the only buffering, so a file never sits whole in memory, and a reader that stops
//! early (a closed consumer) breaks the pipe and ends the thread.
//!
//! - **gzip**: every member, because a writer may append members to one file.
//! - **zstd**: every data frame, and skippable frames are stepped over, because a writer may emit
//!   one frame per batch. A frame declaring a window over [`MAX_WINDOW`] is refused, because
//!   `ruzstd` reserves a frame's declared window before decoding a byte, and a content checksum
//!   that doesn't match is refused, because `ruzstd` reads one but never compares it.

use flate2::read::MultiGzDecoder;
use ruzstd::decoding::{FrameDecoder, StreamingDecoder};
use std::io::{BufRead, BufReader, Read};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncWriteExt, DuplexStream};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];
/// A zstd frame header declaring a window past this is refused. 64 MiB is half the zstd
/// reference decoder's default ceiling, and covers the writers `s3_out` and `zstd -19` use.
const MAX_WINDOW: u64 = 64 * 1024 * 1024;
/// Bytes the pipe holds between the decoder thread and the reader.
const PIPE_CAPACITY: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Compression {
    None,
    Gzip,
    Zstd,
}

impl Compression {
    /// The compression a file starting with `head` uses. A file shorter than a magic is plain.
    pub(crate) fn sniff(head: &[u8]) -> Self {
        if head.starts_with(&ZSTD_MAGIC) {
            Self::Zstd
        } else if head.starts_with(&GZIP_MAGIC) {
            Self::Gzip
        } else {
            Self::None
        }
    }
}

/// A running decompression: the reader of its output, and what `finish` reports.
pub(crate) struct Decompressing {
    pub(crate) output: DuplexStream,
    task: JoinHandle<()>,
    error: Arc<Mutex<Option<String>>>,
}

impl Decompressing {
    /// Waits for the thread to end, which it does once `output` is dropped or read to its end.
    /// Returns why the decoder stopped early, when it did.
    pub(crate) async fn finish(self) -> Option<String> {
        let Self { output, task, error } = self;
        drop(output);
        let _ = task.await;
        let why = error.lock().expect("the decoder thread doesn't panic holding the lock").take();
        why
    }
}

/// Decodes `file` as `compression` on a blocking thread. Must run inside a Tokio runtime.
pub(crate) fn spawn(file: std::fs::File, compression: Compression) -> Decompressing {
    debug_assert_ne!(compression, Compression::None);
    let (writer, output) = tokio::io::duplex(PIPE_CAPACITY);
    let error = Arc::new(Mutex::new(None));
    let slot = Arc::clone(&error);
    let handle = Handle::current();
    let task = tokio::task::spawn_blocking(move || {
        let reader = BufReader::new(file);
        let result = match compression {
            Compression::Gzip => pump(MultiGzDecoder::new(reader), writer, &handle),
            _ => pump(ZstdFrames::new(reader), writer, &handle),
        };
        if let Err(message) = result {
            *slot.lock().expect("the reader side doesn't panic holding the lock") = Some(message);
        }
    });
    Decompressing { output, task, error }
}

/// Copies `decoder` into `writer` until its end. `Err` is a decode failure; a reader that went
/// away is a normal stop.
fn pump(mut decoder: impl Read, mut writer: DuplexStream, handle: &Handle) -> Result<(), String> {
    let mut chunk = vec![0u8; 64 * 1024];
    loop {
        let n = match decoder.read(&mut chunk) {
            Ok(0) => return Ok(()),
            Ok(n) => n,
            Err(err) => return Err(err.to_string()),
        };
        if handle.block_on(writer.write_all(&chunk[..n])).is_err() {
            return Ok(());
        }
    }
}

/// A `Read` over every zstd frame in a stream, one `StreamingDecoder` at a time.
struct ZstdFrames<R: BufRead> {
    /// The input between frames. `None` while `current` owns it.
    source: Option<R>,
    current: Option<StreamingDecoder<R, FrameDecoder>>,
    /// The previous frame's decoder, kept so the next frame reuses its buffers.
    spare: Option<FrameDecoder>,
}

impl<R: BufRead> ZstdFrames<R> {
    fn new(source: R) -> Self {
        Self { source: Some(source), current: None, spare: None }
    }

    /// Opens the next data frame, stepping over skippable frames. `false` at the end of input.
    fn next_frame(&mut self) -> std::io::Result<bool> {
        let mut source = self.source.take().expect("no frame is open");
        loop {
            let head = peek4(&mut source)?;
            let Some(head) = head else {
                self.source = Some(source);
                return Ok(false);
            };
            // Skippable frames: magic 0x184D2A50..=0x184D2A5F, a 4-byte length, then that many
            // opaque bytes.
            if u32::from_le_bytes(head) & 0xffff_fff0 == 0x184d_2a50 {
                source.consume(4);
                let mut len = [0u8; 4];
                source.read_exact(&mut len).map_err(|_| truncated("skippable frame"))?;
                let len = u64::from(u32::from_le_bytes(len));
                let skipped = std::io::copy(&mut (&mut source).take(len), &mut std::io::sink())?;
                if skipped != len {
                    return Err(truncated("skippable frame"));
                }
                continue;
            }
            let mut decoder = self.spare.take().unwrap_or_default();
            decoder.set_max_window_size(MAX_WINDOW);
            let streaming = StreamingDecoder::new_with_decoder(source, decoder)
                .map_err(|err| std::io::Error::other(format!("invalid zstd frame: {err}")))?;
            self.current = Some(streaming);
            return Ok(true);
        }
    }

    fn finish_frame(&mut self) -> std::io::Result<()> {
        let streaming = self.current.take().expect("a frame is open");
        let (source, decoder) = streaming.into_parts();
        if let Some(sent) = decoder.get_checksum_from_data() {
            if decoder.get_calculated_checksum() != Some(sent) {
                return Err(std::io::Error::other("zstd content checksum mismatch"));
            }
        }
        self.source = Some(source);
        self.spare = Some(decoder);
        Ok(())
    }
}

impl<R: BufRead> Read for ZstdFrames<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        loop {
            if self.current.is_none() && !self.next_frame()? {
                return Ok(0);
            }
            let streaming = self.current.as_mut().expect("next_frame opened a frame");
            match streaming.read(buf)? {
                0 => self.finish_frame()?,
                n => return Ok(n),
            }
        }
    }
}

fn truncated(what: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::UnexpectedEof, format!("truncated zstd {what}"))
}

/// The next 4 bytes without consuming them. `None` at a clean end of input. A shorter tail is an
/// error, because no frame is under 4 bytes; a file read returns less than the buffer holds only
/// at its end.
fn peek4<R: BufRead>(source: &mut R) -> std::io::Result<Option<[u8; 4]>> {
    let buf = source.fill_buf()?;
    match buf {
        [] => Ok(None),
        [a, b, c, d, ..] => Ok(Some([*a, *b, *c, *d])),
        _ => Err(truncated("frame header")),
    }
}
