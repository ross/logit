//! Line splitting, and the [`TailDecoder`] trait every tailing decoder implements.
//!
//! [`TailDecoder`] is `logit_proto::Decoder` at line granularity. Splitting lives in
//! [`LineSplitter`], shared by every decoder, because a line can straddle two reads of the file
//! and something has to carry the partial line between them.

use bytes::{Bytes, BytesMut};
use logit_core::interner::intern;
use logit_core::{AttrMap, BodyFormat, Diagnostics, Event, LogRecord, Resource, Symbol, Value};
use logit_proto::CodecError;
use std::path::Path;
use std::sync::Arc;

/// Turns one split line into zero or more [`Event`]s appended to `out`.
///
/// The line has no `\n`, has had one trailing `\r` stripped (a second one stays), and is valid
/// UTF-8 (the driver's `ensure_utf8`). `read_at` is when the line was read, not necessarily the
/// event's timestamp: `docker_in` uses the envelope's own `time` and falls back to `read_at` only
/// when that's unparseable.
///
/// A decoder may push events to `out` and then return `Err`: held state it flushed because the
/// rejected line ended a run. The driver still emits those events, under
/// [`TailDecoder::resource`].
pub trait TailDecoder: Send {
    fn decode_line(
        &mut self,
        line: Bytes,
        read_at: i64,
        out: &mut Vec<Event>,
    ) -> Result<Arc<Resource>, CodecError>;

    /// The file is closing (rotated away, removed, or shutdown): emit anything held across lines,
    /// such as `docker_in`'s unfinished partial-entry reassembly. An unterminated last line is
    /// [`LineSplitter::take_partial`]'s job, not this. A line being dropped stays dropped: `close`
    /// emits held lines only. A clean stop skips this call for a decoder whose
    /// [`TailDecoder::hold_at_shutdown`] is set and whose checkpoint will replay its held lines.
    /// Default: nothing held.
    fn close(&mut self, _out: &mut Vec<Event>) {}

    /// The writer always finishes a line, so a clean stop leaves this file's unterminated line
    /// and, when a checkpoint will replay them, its held lines in place rather than emitting
    /// them. A file that is no longer `Active` (rotated away, removed, or de-selected) still emits
    /// both, because a restart won't read it. Default: `false`.
    fn hold_at_shutdown(&self) -> bool {
        false
    }

    /// Whether this decoder holds lines it hasn't produced events for yet, or is discarding the
    /// rest of a line it began dropping. The driver records where the oldest such line starts and
    /// never checkpoints past it. Default: nothing held.
    fn holds_entry(&self) -> bool {
        false
    }

    /// The file was truncated in place: drop, **not** emit, everything held across lines.
    ///
    /// Held state belongs to content that no longer exists; emitting it, or splicing it onto the
    /// new content's first line, is worse than losing it. `Tailer::scan`'s truncation branch
    /// treats [`LineSplitter`]'s partial the same way (rebuilds it rather than calling
    /// `take_partial`). That's the difference from [`TailDecoder::close`], which emits. Default:
    /// nothing held ([`LineDecoder`] is stateless across lines).
    fn reset(&mut self) {}

    /// This decoder's resource, readable before any line is decoded.
    ///
    /// [`LineDecoder`]'s never changes. `docker_in`'s can: `DecoderFactory::refresh` may swap in
    /// a freshly read identity mid-stream
    /// (`docs/adr/docker-container-identity-and-minimal-watches.md`), and
    /// `BatchAccumulator::absorb`'s `Arc::ptr_eq` check keeps two identities out of one batch.
    fn resource(&self) -> Arc<Resource>;
}

/// What one [`LineSplitter::push`] observed, for the caller to report: the splitter holds no
/// `Diagnostics`/`Telemetry` handle.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LineStats {
    pub dropped_lines: u32,
}

/// Splits read chunks into `\n`-terminated lines with one trailing `\r` stripped, carrying an
/// incomplete line across chunks.
///
/// A line inside one chunk is a zero-copy `Bytes::slice` of it; only a line spanning chunks is
/// copied into `partial`, since `Bytes` can't concatenate two allocations. That upholds
/// `docs/design/data-model.md`'s "Strings and blobs are `bytes::Bytes` everywhere."
pub struct LineSplitter {
    /// A line seen so far that hasn't reached its `\n` yet. Empty in the common case.
    partial: BytesMut,
    max_line_bytes: usize,
    /// Skipping the rest of an oversized line, across as many `push` calls as it takes to reach
    /// its `\n`. The line is counted dropped once, when detected.
    dropping: bool,
    /// While `dropping`, how many of the dropped line's bytes have been consumed so far: a count,
    /// not held bytes, so memory stays bounded by `max_line_bytes`.
    dropping_len: u64,
}

impl LineSplitter {
    pub fn new(max_line_bytes: usize) -> Self {
        Self { partial: BytesMut::new(), max_line_bytes, dropping: false, dropping_len: 0 }
    }

    /// Feeds one read chunk, calling `emit` once per complete line (no `\n`, one `\r` stripped).
    ///
    /// `emit`'s second argument is where the line starts: `Some(i)` at index `i` of `chunk`, or
    /// `None` when it began in an earlier chunk, at the start of the partial
    /// [`LineSplitter::pending_bytes`] counted before this call.
    ///
    /// A line over `max_line_bytes` (measured before the `\r` strip) is dropped whole, never
    /// truncated, and counted in the returned [`LineStats`]. That holds across a restart too:
    /// while dropping, [`LineSplitter::pending_bytes`] covers the dropped line, so a checkpoint
    /// stays at its start and a restart drops it whole again (counted a second time) rather than
    /// emitting its tail as a line.
    pub fn push(&mut self, chunk: Bytes, mut emit: impl FnMut(Bytes, Option<usize>)) -> LineStats {
        let mut stats = LineStats::default();
        let mut start = 0usize;
        while start < chunk.len() {
            let nl = memchr::memchr(b'\n', &chunk[start..]);
            let seg_end = nl.map(|i| start + i).unwrap_or(chunk.len());
            let seg = chunk.slice(start..seg_end);

            if self.dropping {
                if nl.is_some() {
                    // This segment's newline closes the oversized line.
                    self.dropping = false;
                    self.dropping_len = 0;
                } else {
                    self.dropping_len += seg.len() as u64;
                }
            } else if self.partial.is_empty() {
                if let Some(_i) = nl {
                    // Zero-copy path: the whole line is in this chunk.
                    if seg.len() > self.max_line_bytes {
                        stats.dropped_lines += 1;
                    } else {
                        emit(strip_cr(seg), Some(start));
                    }
                } else if seg.len() > self.max_line_bytes {
                    stats.dropped_lines += 1;
                    self.dropping = true;
                    self.dropping_len = seg.len() as u64;
                } else {
                    self.partial.extend_from_slice(&seg);
                }
            } else if self.partial.len() + seg.len() > self.max_line_bytes {
                stats.dropped_lines += 1;
                if nl.is_none() {
                    self.dropping = true;
                    self.dropping_len = (self.partial.len() + seg.len()) as u64;
                }
                self.partial.clear();
            } else {
                self.partial.extend_from_slice(&seg);
                if nl.is_some() {
                    let line = self.partial.split().freeze();
                    emit(strip_cr(line), None);
                }
            }

            start = match nl {
                Some(i) => start + i + 1,
                None => chunk.len(),
            };
        }
        stats
    }

    /// The file is closing: returns the held unterminated line, if any. A final line with no
    /// `\n` is still data. An oversized line already being dropped isn't returned, and the drop
    /// stays in place: [`LineSplitter::pending_bytes`] still covers it, so the checkpoint written
    /// at shutdown stays at the dropped line's start, and a later `push` continues the drop to
    /// its `\n`.
    pub fn take_partial(&mut self) -> Option<Bytes> {
        if self.dropping || self.partial.is_empty() {
            return None;
        }
        Some(strip_cr(self.partial.split().freeze()))
    }

    /// Bytes read (so already in the tailer's offset) since the current line's start: the held
    /// partial, or, while dropping an oversized line, the bytes of it consumed so far.
    /// `Tailer::write_checkpoint` subtracts this, so a checkpoint always lands on a line start:
    /// never covering an unemitted line, and never inside a dropped one.
    pub fn pending_bytes(&self) -> u64 {
        if self.dropping {
            self.dropping_len
        } else {
            self.partial.len() as u64
        }
    }
}

fn strip_cr(b: Bytes) -> Bytes {
    if b.last() == Some(&b'\r') {
        b.slice(..b.len() - 1)
    } else {
        b
    }
}

/// `tail_in`'s [`TailDecoder`]: one line becomes one raw log event with a `log.file.path`
/// attribute. `message` is the [`LineSplitter`] slice itself, never a copy.
pub struct LineDecoder {
    resource: Arc<Resource>,
    path_key: Symbol,
    path_value: Value,
    #[allow(dead_code)] // parity with decoders that diagnose; this one never reads it
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
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
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
        let stats = splitter.push(Bytes::copy_from_slice(chunk), |line, _| out.push(line.to_vec()));
        (out, stats)
    }

    /// `push` reports an in-chunk line's start index, and `None` for a line continued from a
    /// previous chunk's partial.
    #[test]
    fn push_reports_where_each_line_starts() {
        let mut s = LineSplitter::new(1024);
        let mut starts = Vec::new();
        s.push(Bytes::from_static(b"ab\ncd\nef"), |_, start| starts.push(start));
        assert_eq!(starts, vec![Some(0), Some(3)]);
        starts.clear();
        s.push(Bytes::from_static(b"g\nhi\n"), |_, start| starts.push(start));
        assert_eq!(starts, vec![None, Some(2)]);
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

    /// Config rejects `0`, but the splitter accepts it: every non-empty line is dropped, and an
    /// empty one is kept because the limit is exclusive. A bare `\r\n` is one byte, so dropped.
    #[test]
    fn zero_max_line_bytes_still_emits_empty_lines() {
        let mut s = LineSplitter::new(0);
        let (lines, stats) = lines_of(&mut s, b"a\nb\n");
        assert!(lines.is_empty());
        assert_eq!(stats.dropped_lines, 2);
        let (lines, stats) = lines_of(&mut s, b"\n");
        assert_eq!(lines, vec![Vec::<u8>::new()]);
        assert_eq!(stats.dropped_lines, 0);
        let (lines, stats) = lines_of(&mut s, b"\r\n");
        assert!(lines.is_empty());
        assert_eq!(stats.dropped_lines, 1);
    }

    type Emitted = Vec<(Vec<u8>, Option<usize>)>;

    fn push_starts(splitter: &mut LineSplitter, chunk: &[u8]) -> (Emitted, u32) {
        let mut out = Vec::new();
        let stats =
            splitter.push(Bytes::copy_from_slice(chunk), |line, at| out.push((line.to_vec(), at)));
        (out, stats.dropped_lines)
    }

    /// The limit is inclusive in the in-chunk branch and in both spanning branches (the partial
    /// grows, then the `\n` arrives in a later chunk or alone).
    #[test]
    fn a_line_of_exactly_max_line_bytes_is_kept_in_both_branches() {
        let mut s = LineSplitter::new(4);
        assert_eq!(push_starts(&mut s, b"abcd\n"), (vec![(b"abcd".to_vec(), Some(0))], 0));

        let mut s = LineSplitter::new(4);
        assert_eq!(push_starts(&mut s, b"ab"), (vec![], 0));
        assert_eq!(push_starts(&mut s, b"cd\n"), (vec![(b"abcd".to_vec(), None)], 0));

        let mut s = LineSplitter::new(4);
        assert_eq!(push_starts(&mut s, b"abcd"), (vec![], 0));
        assert_eq!(s.pending_bytes(), 4);
        assert_eq!(push_starts(&mut s, b"\n"), (vec![(b"abcd".to_vec(), None)], 0));
    }

    /// One byte over is dropped in every branch, and the `\r` of a CRLF ending counts toward the
    /// limit.
    #[test]
    fn a_line_one_byte_over_max_is_dropped_in_both_branches() {
        let mut s = LineSplitter::new(4);
        assert_eq!(push_starts(&mut s, b"abcde\nok\n"), (vec![(b"ok".to_vec(), Some(6))], 1));

        let mut s = LineSplitter::new(4);
        assert_eq!(push_starts(&mut s, b"ab"), (vec![], 0));
        assert_eq!(push_starts(&mut s, b"cde\nok\n"), (vec![(b"ok".to_vec(), Some(4))], 1));

        let mut s = LineSplitter::new(4);
        assert_eq!(push_starts(&mut s, b"abcde"), (vec![], 1));
        assert_eq!(push_starts(&mut s, b"\nok\n"), (vec![(b"ok".to_vec(), Some(1))], 0));

        let mut s = LineSplitter::new(4);
        assert_eq!(push_starts(&mut s, b"abcd\r\nok\n"), (vec![(b"ok".to_vec(), Some(6))], 1));
    }

    #[test]
    fn a_cr_at_a_chunk_edge_followed_by_lf_is_stripped() {
        let mut s = LineSplitter::new(1024);
        assert_eq!(push_starts(&mut s, b"ab\r"), (vec![], 0));
        assert_eq!(s.pending_bytes(), 3, "the held `\r` is part of the pending line");
        assert_eq!(push_starts(&mut s, b"\n"), (vec![(b"ab".to_vec(), None)], 0));
        assert_eq!(s.pending_bytes(), 0);
    }

    #[test]
    fn an_empty_chunk_changes_nothing() {
        // At a line boundary.
        let mut s = LineSplitter::new(4);
        assert_eq!(push_starts(&mut s, b""), (vec![], 0));
        assert_eq!(s.pending_bytes(), 0);
        assert_eq!(push_starts(&mut s, b"ab\n"), (vec![(b"ab".to_vec(), Some(0))], 0));

        // Mid-line: the line still continues from the partial.
        let mut s = LineSplitter::new(4);
        push_starts(&mut s, b"ab");
        assert_eq!(push_starts(&mut s, b""), (vec![], 0));
        assert_eq!(s.pending_bytes(), 2);
        assert_eq!(push_starts(&mut s, b"c\n"), (vec![(b"abc".to_vec(), None)], 0));

        // Mid-drop: still dropping, and not counted again.
        let mut s = LineSplitter::new(4);
        assert_eq!(push_starts(&mut s, b"abcdef"), (vec![], 1));
        assert_eq!(push_starts(&mut s, b""), (vec![], 0));
        assert_eq!(s.pending_bytes(), 6);
        assert_eq!(push_starts(&mut s, b"g\nok\n"), (vec![(b"ok".to_vec(), Some(2))], 0));
    }

    #[test]
    fn an_oversized_line_spanning_many_chunks_is_counted_once() {
        let mut s = LineSplitter::new(4);
        assert_eq!(push_starts(&mut s, b"aaaaa"), (vec![], 1));
        assert_eq!(push_starts(&mut s, b"aaaa"), (vec![], 0));
        assert_eq!(push_starts(&mut s, b"aa"), (vec![], 0));
        assert_eq!(push_starts(&mut s, b"a\nok\n"), (vec![(b"ok".to_vec(), Some(2))], 0));
    }

    /// The partial fits, and the chunk carrying the `\n` pushes it over: counted in that push,
    /// with nothing left dropping.
    #[test]
    fn an_overflow_detected_on_the_newline_chunk_is_counted_there() {
        let mut s = LineSplitter::new(4);
        assert_eq!(push_starts(&mut s, b"abc"), (vec![], 0));
        assert_eq!(push_starts(&mut s, b"de\nok\n"), (vec![(b"ok".to_vec(), Some(3))], 1));
        assert_eq!(s.pending_bytes(), 0);
    }

    #[test]
    fn only_one_trailing_cr_is_stripped() {
        let mut s = LineSplitter::new(1024);
        assert_eq!(push_starts(&mut s, b"a\r\r\n"), (vec![(b"a\r".to_vec(), Some(0))], 0));
    }

    #[test]
    fn a_bare_crlf_line_is_emitted_empty() {
        let mut s = LineSplitter::new(1024);
        assert_eq!(
            push_starts(&mut s, b"\r\nx\n"),
            (vec![(vec![], Some(0)), (b"x".to_vec(), Some(2))], 0)
        );
    }

    #[test]
    fn take_partial_of_a_lone_cr_yields_an_empty_line() {
        let mut s = LineSplitter::new(1024);
        push_starts(&mut s, b"\r");
        assert_eq!(s.take_partial(), Some(Bytes::new()));
        assert_eq!(s.pending_bytes(), 0);
    }

    #[test]
    fn take_partial_mid_drop_returns_none_and_keeps_the_drop_in_place() {
        let mut s = LineSplitter::new(4);
        assert_eq!(push_starts(&mut s, b"toolo"), (vec![], 1));
        assert_eq!(s.take_partial(), None);
        assert_eq!(s.pending_bytes(), 5, "the drop still pins the checkpoint to its start");
        assert_eq!(
            push_starts(&mut s, b"ng\nok\n"),
            (vec![(b"ok".to_vec(), Some(3))], 0),
            "the drop continues to its newline and isn't counted again"
        );
        assert_eq!(s.pending_bytes(), 0);
    }

    /// `Tailer::write_checkpoint` subtracts `pending_bytes`, so while dropping it must cover the
    /// dropped line back to its start, whichever branch started the drop.
    #[test]
    fn pending_bytes_covers_the_dropped_line_while_dropping() {
        let mut s = LineSplitter::new(4);
        push_starts(&mut s, b"toolo");
        assert_eq!(s.pending_bytes(), 5);
        push_starts(&mut s, b"ng");
        assert_eq!(s.pending_bytes(), 7);
        push_starts(&mut s, b"\n");
        assert_eq!(s.pending_bytes(), 0);

        let mut s = LineSplitter::new(4);
        push_starts(&mut s, b"abc");
        assert_eq!(s.pending_bytes(), 3);
        assert_eq!(push_starts(&mut s, b"de"), (vec![], 1));
        assert_eq!(s.pending_bytes(), 5);
    }

    #[test]
    fn a_usize_max_limit_never_drops() {
        let mut s = LineSplitter::new(usize::MAX);
        let long = vec![b'x'; 70_000];
        assert_eq!(push_starts(&mut s, &long), (vec![], 0));
        assert_eq!(push_starts(&mut s, &long), (vec![], 0));
        let (lines, dropped) = push_starts(&mut s, b"\n");
        assert_eq!(dropped, 0);
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].0.len(), 140_000);
        assert_eq!(lines[0].1, None);
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

    /// A line inside one chunk is a slice of the chunk's allocation, not a copy.
    #[test]
    fn a_fully_contained_lines_message_is_a_zero_copy_slice_of_the_chunk() {
        let mut s = LineSplitter::new(1024);
        let chunk = Bytes::copy_from_slice(b"hello\n");
        let chunk_ptr = chunk.as_ptr();
        let mut emitted = None;
        s.push(chunk, |line, _| emitted = Some(line));
        let line = emitted.expect("should have emitted one line");
        let offset = line.as_ptr() as usize - chunk_ptr as usize;
        assert_eq!(offset, 0);
        assert_eq!(&line[..], b"hello");
    }
}
