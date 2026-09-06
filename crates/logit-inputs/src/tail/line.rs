//! Line splitting, and the [`TailDecoder`] trait every tailing decoder implements against.
//!
//! [`TailDecoder`] is the file-tailing analogue of `logit_proto::Decoder`: "one already-split
//! line in, zero or more events out" rather than "one whole payload in, zero or more events
//! out". Splitting itself lives here, in [`LineSplitter`], not inside each decoder -- unlike a
//! UDP datagram (which a `Decoder` always receives whole), a line can straddle two separate
//! reads of the underlying file, so *something* has to carry a partial line across `push` calls,
//! and every decoder would otherwise have to reimplement that.

use bytes::{Bytes, BytesMut};
use logit_core::interner::intern;
use logit_core::{AttrMap, BodyFormat, Diagnostics, Event, LogRecord, Resource, Symbol, Value};
use logit_proto::CodecError;
use std::path::Path;
use std::sync::Arc;

/// What one tailing decoder implements against: an already-newline-split, `\r`-stripped,
/// UTF-8-valid line turns into zero or more [`Event`]s appended to `out`. Mirrors
/// `logit_proto::Decoder::decode_into`'s "append, don't return a fresh `Vec`" shape, at line
/// rather than whole-payload granularity. `read_at` is when the line was read off the file, not
/// necessarily this event's own timestamp -- `docker_in`'s decoder uses the line's own embedded
/// timestamp instead, falling back to `read_at` only when that's unparseable.
pub trait TailDecoder: Send {
    fn decode_line(
        &mut self,
        line: Bytes,
        read_at: i64,
        out: &mut Vec<Event>,
    ) -> Result<Arc<Resource>, CodecError>;

    /// Called once, when the file this decoder is reading is closing (rotated away, removed, or
    /// the whole component is shutting down) -- an opportunity to emit anything a decoder held
    /// back across lines (`docker_in`'s reassembled-but-never-newline-terminated final entry;
    /// `tail_in`'s own unterminated last line is handled one level up, by
    /// [`LineSplitter::take_partial`], since every `TailDecoder` shares that same behavior).
    /// Default: nothing held.
    fn close(&mut self, _out: &mut Vec<Event>) {}

    /// This decoder's resource, without decoding a line -- needed to seed accounting before
    /// anything has been read. Every decoder here builds one `Resource` per file and never
    /// changes it afterward (`docs/adr/decoupled-listener-io.md`'s "never merges across a
    /// resource change" rule, upheld the same way `syslog_in`/`docker_in` uphold it).
    fn resource(&self) -> Arc<Resource>;
}

/// Bytes dropped from one oversized line, and how many complete lines a [`LineSplitter::push`]
/// call yielded via its callback -- both purely observational; [`LineSplitter`] itself takes no
/// `Diagnostics`/`Telemetry` (kept dependency-free and directly unit-testable), so the caller
/// (`crate::tail::driver::Tailer`) reports whatever this carries.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LineStats {
    pub dropped_lines: u32,
}

/// Splits a stream of read chunks into complete lines (`\n`-terminated, with a trailing `\r`
/// stripped), carrying an incomplete line across chunks. A line entirely contained in one
/// `push` call -- the overwhelming common case -- is handed to the caller as a zero-copy
/// `Bytes::slice` of that chunk; only a line that actually spans two or more chunks pays a copy
/// (into `partial`, unavoidable: `Bytes` can't cheaply concatenate two independent
/// allocations). `docs/design/data-model.md`'s "`bytes::Bytes` everywhere strings and blobs
/// appear" is what this exists to uphold.
pub struct LineSplitter {
    /// A line seen so far that hasn't reached its `\n` yet. Empty in the common case.
    partial: BytesMut,
    max_line_bytes: usize,
    /// `true` while skipping the remainder of a line that already exceeded `max_line_bytes` --
    /// spans however many `push` calls it takes to reach that line's own `\n`.
    dropping: bool,
}

impl LineSplitter {
    pub fn new(max_line_bytes: usize) -> Self {
        Self { partial: BytesMut::new(), max_line_bytes, dropping: false }
    }

    /// Feeds one read chunk, calling `emit` once per complete line found (never including the
    /// terminating `\n`, and with any trailing `\r` also stripped). Returns how many lines this
    /// call dropped whole for exceeding `max_line_bytes` -- the caller decides how to report
    /// that (`Diagnostics::warn_throttled`, a counter), since this type has no telemetry handle
    /// of its own.
    pub fn push(&mut self, chunk: Bytes, mut emit: impl FnMut(Bytes)) -> LineStats {
        let mut stats = LineStats::default();
        let mut start = 0usize;
        while start < chunk.len() {
            let nl = chunk[start..].iter().position(|&b| b == b'\n');
            let seg_end = nl.map(|i| start + i).unwrap_or(chunk.len());
            let seg = chunk.slice(start..seg_end);

            if self.dropping {
                if nl.is_some() {
                    self.dropping = false; // this segment's newline closes the oversized line
                }
                // else: still mid-drop, this whole segment is discarded.
            } else if self.partial.is_empty() {
                if let Some(_i) = nl {
                    // The whole line lives in this one chunk -- the zero-copy path.
                    if seg.len() > self.max_line_bytes {
                        stats.dropped_lines += 1;
                    } else {
                        emit(strip_cr(seg));
                    }
                } else if seg.len() > self.max_line_bytes {
                    stats.dropped_lines += 1;
                    self.dropping = true;
                } else {
                    self.partial.extend_from_slice(&seg);
                }
            } else if self.partial.len() + seg.len() > self.max_line_bytes {
                self.partial.clear();
                stats.dropped_lines += 1;
                self.dropping = nl.is_none();
            } else {
                self.partial.extend_from_slice(&seg);
                if nl.is_some() {
                    let line = self.partial.split().freeze();
                    emit(strip_cr(line));
                }
            }

            start = match nl {
                Some(i) => start + i + 1,
                None => chunk.len(),
            };
        }
        stats
    }

    /// The file this splitter was reading is closing: returns whatever incomplete line was held
    /// (never terminated by a `\n`), if any and if it wasn't itself already dropped for being
    /// oversized. `tail_in`'s own precedent for "a final line with no trailing newline is still
    /// real data, not framing to discard."
    pub fn take_partial(&mut self) -> Option<Bytes> {
        self.dropping = false;
        if self.partial.is_empty() {
            return None;
        }
        Some(strip_cr(self.partial.split().freeze()))
    }
}

fn strip_cr(b: Bytes) -> Bytes {
    if b.last() == Some(&b'\r') {
        b.slice(..b.len() - 1)
    } else {
        b
    }
}

/// `tail_in`'s own [`TailDecoder`]: one line becomes one log event, unmodified, with a
/// `log.file.path` attribute naming the file it came from. `message` stays a zero-copy
/// [`Value::Str`] slice of whatever [`LineSplitter`] handed in -- this decoder never copies a
/// line's bytes itself, matching `syslog_in`'s own zero-copy precedent.
pub struct LineDecoder {
    resource: Arc<Resource>,
    path_key: Symbol,
    path_value: Value,
    #[allow(dead_code)] // carried for parity with decoders that do use it (diagnostics context)
    diag: Diagnostics,
}

impl LineDecoder {
    pub fn new(path: &Path, resource: Arc<Resource>) -> Self {
        Self {
            resource,
            path_key: intern("log.file.path"),
            path_value: Value::str(path.to_string_lossy().into_owned()),
            diag: Diagnostics::default(),
        }
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }
}

impl TailDecoder for LineDecoder {
    fn decode_line(
        &mut self,
        line: Bytes,
        read_at: i64,
        out: &mut Vec<Event>,
    ) -> Result<Arc<Resource>, CodecError> {
        let mut attrs = AttrMap::new();
        attrs.insert_sym(self.path_key, self.path_value.clone());
        out.push(Event::log(
            read_at,
            attrs,
            LogRecord {
                message: Value::Str(line),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
            },
        ));
        Ok(self.resource.clone())
    }

    fn resource(&self) -> Arc<Resource> {
        self.resource.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines_of(splitter: &mut LineSplitter, chunk: &[u8]) -> (Vec<Vec<u8>>, LineStats) {
        let mut out = Vec::new();
        let stats = splitter.push(Bytes::copy_from_slice(chunk), |line| out.push(line.to_vec()));
        (out, stats)
    }

    #[test]
    fn a_single_chunk_with_two_terminated_lines_yields_both() {
        let mut s = LineSplitter::new(1024);
        let (lines, stats) = lines_of(&mut s, b"one\ntwo\n");
        assert_eq!(lines, vec![b"one".to_vec(), b"two".to_vec()]);
        assert_eq!(stats.dropped_lines, 0);
        assert_eq!(s.take_partial(), None);
    }

    #[test]
    fn a_line_split_across_two_chunks_is_carried_and_joined() {
        let mut s = LineSplitter::new(1024);
        let (first, _) = lines_of(&mut s, b"hel");
        assert!(first.is_empty(), "no newline yet -- nothing should emit");
        let (second, _) = lines_of(&mut s, b"lo\nworld\n");
        assert_eq!(second, vec![b"hello".to_vec(), b"world".to_vec()]);
    }

    #[test]
    fn crlf_line_endings_have_the_cr_stripped() {
        let mut s = LineSplitter::new(1024);
        let (lines, _) = lines_of(&mut s, b"one\r\ntwo\r\n");
        assert_eq!(lines, vec![b"one".to_vec(), b"two".to_vec()]);
    }

    #[test]
    fn empty_lines_are_still_emitted() {
        let mut s = LineSplitter::new(1024);
        let (lines, _) = lines_of(&mut s, b"\n\nx\n");
        assert_eq!(lines, vec![Vec::<u8>::new(), Vec::new(), b"x".to_vec()]);
    }

    #[test]
    fn an_unterminated_final_chunk_is_returned_by_take_partial() {
        let mut s = LineSplitter::new(1024);
        let (lines, _) = lines_of(&mut s, b"no newline here");
        assert!(lines.is_empty());
        assert_eq!(s.take_partial(), Some(Bytes::from_static(b"no newline here")));
        // Taken once -- a second call has nothing left.
        assert_eq!(s.take_partial(), None);
    }

    #[test]
    fn a_line_over_the_limit_within_one_chunk_is_dropped_whole_and_resumes_after_it() {
        let mut s = LineSplitter::new(4);
        let (lines, stats) = lines_of(&mut s, b"toolong\nok\n");
        assert_eq!(lines, vec![b"ok".to_vec()]);
        assert_eq!(stats.dropped_lines, 1);
    }

    #[test]
    fn a_line_over_the_limit_spanning_chunks_is_dropped_whole_and_resumes_after_it() {
        let mut s = LineSplitter::new(4);
        let (first, stats1) = lines_of(&mut s, b"toolo");
        assert!(first.is_empty());
        assert_eq!(stats1.dropped_lines, 1, "the drop is counted as soon as it's detected");
        let (second, stats2) = lines_of(&mut s, b"ng\nok\n");
        assert_eq!(second, vec![b"ok".to_vec()]);
        assert_eq!(stats2.dropped_lines, 0, "already counted -- not counted again on resolution");
    }

    #[test]
    fn zero_max_line_bytes_drops_every_line() {
        let mut s = LineSplitter::new(0);
        let (lines, stats) = lines_of(&mut s, b"a\nb\n");
        assert!(lines.is_empty());
        assert_eq!(stats.dropped_lines, 2);
    }

    #[test]
    fn line_decoder_emits_a_raw_log_event_with_the_file_path_attribute() {
        let mut decoder =
            LineDecoder::new(Path::new("/var/log/app.log"), Arc::new(Resource::default()));
        let mut out = Vec::new();
        decoder.decode_line(Bytes::from_static(b"hello world"), 42, &mut out).unwrap();
        assert_eq!(out.len(), 1);
        let event = &out[0];
        assert_eq!(event.timestamp, 42);
        assert_eq!(event.log.as_ref().unwrap().message.as_str(), Some("hello world"));
        assert_eq!(
            event.attributes.get("log.file.path").and_then(|v| v.as_str()),
            Some("/var/log/app.log")
        );
    }

    /// The zero-copy promise: a fully-contained line's message must be a slice of the exact same
    /// allocation the read chunk itself owns, not a copy -- `syslog_in`'s own precedent
    /// (`crates/logit-inputs/src/syslog.rs`'s zero-copy tests).
    #[test]
    fn a_fully_contained_lines_message_is_a_zero_copy_slice_of_the_chunk() {
        let mut s = LineSplitter::new(1024);
        let chunk = Bytes::copy_from_slice(b"hello\n");
        let chunk_ptr = chunk.as_ptr();
        let mut emitted = None;
        s.push(chunk, |line| emitted = Some(line));
        let line = emitted.expect("should have emitted one line");
        let offset = line.as_ptr() as usize - chunk_ptr as usize;
        assert_eq!(offset, 0);
        assert_eq!(&line[..], b"hello");
    }
}
