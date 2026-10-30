//! The CRI container log line, as containerd and CRI-O write it under `/var/log/pods`, parsed
//! without copying. `cri_in` reads each line through [`parse`] and reassembles partial lines
//! itself.
//!
//! # Grammar
//!
//! ```text
//! <timestamp> SP <stream> SP <tag>[:<tag>...] SP <content>
//! ```
//!
//! The caller passes one line without its terminating LF. [`parse`] follows the kubelet's
//! `parseCRILog` (`staging/src/k8s.io/cri-client/pkg/logs/logs.go`), which reads each of the three
//! leading fields up to the next space:
//!
//! - **Timestamp.** The kubelet parses RFC 3339 with 0 to 9 fraction digits and `Z` or `±hh:mm`
//!   (`RFC3339NanoLenient`). [`parse`] hands the field to
//!   [`logit_core::time::parse_rfc3339_to_nanos`], which accepts the same set. A timestamp that
//!   doesn't parse doesn't reject the line: [`CriLine::timestamp_nanos`] carries the error and the
//!   rest of the line is still returned, so the caller can fall back to read time. The kubelet
//!   rejects such a line; logit keeps the data.
//! - **Stream.** `stdout` or `stderr`. Anything else is [`CriError::UnknownStream`], as in the
//!   kubelet (`unexpected stream type`).
//! - **Tag.** Colon-separated, and only the first tag is read: `P` is a partial line, and every
//!   other value, `F` and the empty tag included, is a full line. That is the kubelet's rule
//!   (`runtimeapi.LogTag(tags[0]) == runtimeapi.LogTagPartial`, under the comment "Keep this
//!   forward compatible"), so a tag logit doesn't know reads the way `kubectl logs` reads it.
//!   Neither runtime writes a tag other than a bare `P` or `F` (containerd's
//!   `internal/cri/io/logger.go`, conmon's `src/ctr_logging.c`).
//! - **Content.** Everything after the space that ends the tag field, and it may be empty. The
//!   space is required even then: both runtimes write it, and the kubelet fails a line without it
//!   (`log tag is not found`), so a line that ends at the tag is [`CriError::MissingTag`]. Content
//!   is bytes, because neither runtime validates UTF-8.
//!
//! A trailing `\r` stays in the content. conmon writes it, and containerd strips it before it
//! writes (`readLine` in `internal/cri/io/logger.go`), so a CRLF producer reads differently per
//! runtime and the parser doesn't hide that.
//!
//! # What each runtime writes
//!
//! - **containerd** formats the timestamp with `time.RFC3339Nano` on `time.Now()`
//!   (`internal/cri/io/logger.go`, `timestampFormat`): the node's local zone, so `Z` on a UTC node
//!   and `±hh:mm` otherwise, and a fraction of variable width with trailing zeros trimmed (none
//!   at an exact second). It cuts a line longer than `max_container_log_line_size` (16 KiB by
//!   default; a non-positive value means no limit) into `P` pieces of that size and an `F`
//!   remainder, and writes an unterminated tail as `P` at end of stream or when the file is
//!   reopened.
//! - **CRI-O** (conmon, `set_k8s_timestamp` in `src/ctr_logging.c`) writes a fixed nine-digit
//!   fraction and always a numeric offset: `+00:00` for UTC, never `Z`. It writes `P` for each
//!   `read()` that ends without a newline, so a `P` is at most `STDIO_BUF_SIZE` (8 KiB) and can be
//!   shorter, and a short line can arrive as `P` then `F`.
//!
//! In both, the two streams' pieces can interleave, so reassembly keys on stream. That is the
//! caller's job.

use std::fmt;

use logit_core::time::{parse_rfc3339_to_nanos, TimestampError};

/// Which stream a CRI line came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stream {
    Stdout,
    Stderr,
}

impl Stream {
    /// The CRI name, which is also the `log.iostream` value.
    pub fn as_str(self) -> &'static str {
        match self {
            Stream::Stdout => "stdout",
            Stream::Stderr => "stderr",
        }
    }
}

/// One parsed CRI line. `content` borrows from the input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CriLine<'a> {
    /// Unix nanoseconds, or why the timestamp field couldn't be read. The rest of the line is
    /// valid either way.
    pub timestamp_nanos: Result<i64, TimestampError>,
    pub stream: Stream,
    /// The first tag was `P`: the line continues in a later record of the same stream.
    pub partial: bool,
    pub content: &'a [u8],
}

/// Why a line isn't CRI grammar. Each variant names the first field that failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CriError {
    /// No space ends the timestamp field.
    MissingTimestamp,
    /// No space ends the stream field.
    MissingStream,
    /// The stream is neither `stdout` nor `stderr`.
    UnknownStream,
    /// No space ends the tag field.
    MissingTag,
}

impl fmt::Display for CriError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            CriError::MissingTimestamp => "timestamp field not terminated by a space",
            CriError::MissingStream => "stream field not terminated by a space",
            CriError::UnknownStream => "stream is neither stdout nor stderr",
            CriError::MissingTag => "tag field not terminated by a space",
        })
    }
}

impl std::error::Error for CriError {}

/// Parses one CRI log line, given without its LF.
pub fn parse(line: &[u8]) -> Result<CriLine<'_>, CriError> {
    let (ts, rest) = split_space(line).ok_or(CriError::MissingTimestamp)?;
    let (stream, rest) = split_space(rest).ok_or(CriError::MissingStream)?;
    let stream = match stream {
        b"stdout" => Stream::Stdout,
        b"stderr" => Stream::Stderr,
        _ => return Err(CriError::UnknownStream),
    };
    let (tag, content) = split_space(rest).ok_or(CriError::MissingTag)?;
    let first_tag = tag.split(|&b| b == b':').next().unwrap_or_default();
    let timestamp_nanos = match std::str::from_utf8(ts) {
        Ok(text) => parse_rfc3339_to_nanos(text),
        Err(_) => Err(TimestampError::Malformed),
    };
    Ok(CriLine { timestamp_nanos, stream, partial: first_tag == b"P", content })
}

fn split_space(b: &[u8]) -> Option<(&[u8], &[u8])> {
    let at = b.iter().position(|&c| c == b' ')?;
    Some((&b[..at], &b[at + 1..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2026-09-21T13:33:20.123456789Z
    const T: i64 = 1_789_997_600_123_456_789;

    fn ok(line: &str) -> CriLine<'_> {
        parse(line.as_bytes()).unwrap()
    }

    #[test]
    fn containerd_utc_full_line() {
        let l = ok("2026-09-21T13:33:20.123456789Z stdout F hello world");
        assert_eq!(l.timestamp_nanos, Ok(T));
        assert_eq!(l.stream, Stream::Stdout);
        assert!(!l.partial);
        assert_eq!(l.content, b"hello world");
    }

    #[test]
    fn containerd_trimmed_and_absent_fractions() {
        assert_eq!(ok("2026-09-21T13:33:20.123Z stderr F x").timestamp_nanos, Ok(T - 456_789));
        assert_eq!(
            ok("2026-09-21T13:33:20Z stderr F x").timestamp_nanos,
            Ok(1_789_997_600_000_000_000)
        );
    }

    #[test]
    fn containerd_local_zone_offset() {
        let l = ok("2026-09-21T09:33:20.123456789-04:00 stdout F x");
        assert_eq!(l.timestamp_nanos, Ok(T));
        let l = ok("2026-09-21T15:03:20.123456789+01:30 stdout F x");
        assert_eq!(l.timestamp_nanos, Ok(T));
    }

    #[test]
    fn crio_nine_digits_and_zero_offset() {
        let l = ok("2026-09-21T13:33:20.123456789+00:00 stderr P partial chunk");
        assert_eq!(l.timestamp_nanos, Ok(T));
        assert_eq!(l.stream, Stream::Stderr);
        assert!(l.partial);
        assert_eq!(l.content, b"partial chunk");
    }

    #[test]
    fn empty_content_needs_the_separating_space() {
        let l = ok("2026-09-21T13:33:20Z stdout F ");
        assert_eq!(l.content, b"");
        assert_eq!(parse(b"2026-09-21T13:33:20Z stdout F"), Err(CriError::MissingTag));
    }

    #[test]
    fn only_the_first_tag_decides_partial() {
        assert!(!ok("2026-09-21T13:33:20Z stdout F:x y").partial);
        assert!(ok("2026-09-21T13:33:20Z stdout P:F y").partial);
        assert!(!ok("2026-09-21T13:33:20Z stdout F:P y").partial);
        assert_eq!(ok("2026-09-21T13:33:20Z stdout F:x y").content, b"y");
    }

    #[test]
    fn an_unknown_or_empty_first_tag_is_a_full_line_as_in_the_kubelet() {
        assert!(!ok("2026-09-21T13:33:20Z stdout X y").partial);
        assert!(!ok("2026-09-21T13:33:20Z stdout  y").partial);
        assert!(!ok("2026-09-21T13:33:20Z stdout p y").partial);
    }

    #[test]
    fn trailing_cr_and_inner_spaces_stay_in_content() {
        assert_eq!(ok("2026-09-21T13:33:20Z stdout F a  b \r").content, b"a  b \r");
    }

    #[test]
    fn content_is_bytes() {
        let mut line = b"2026-09-21T13:33:20Z stdout F ".to_vec();
        line.extend_from_slice(&[0xff, 0xfe, 0x00, b'a']);
        assert_eq!(parse(&line).unwrap().content, &[0xff, 0xfe, 0x00, b'a']);
    }

    #[test]
    fn a_bad_timestamp_keeps_the_rest_of_the_line() {
        for ts in ["yesterday", "2026-09-21T13:33:20", "2026-02-30T00:00:00Z", ""] {
            let line = format!("{ts} stdout P body");
            let l = parse(line.as_bytes()).unwrap();
            assert_eq!(l.timestamp_nanos, Err(TimestampError::Malformed), "{ts:?}");
            assert_eq!(l.stream, Stream::Stdout);
            assert!(l.partial);
            assert_eq!(l.content, b"body");
        }
        let mut line = vec![0xff];
        line.extend_from_slice(b" stdout F x");
        assert_eq!(parse(&line).unwrap().timestamp_nanos, Err(TimestampError::Malformed));
    }

    #[test]
    fn a_well_formed_timestamp_out_of_range_is_distinct() {
        let l = ok("9999-12-31T23:59:59Z stdout F x");
        assert_eq!(l.timestamp_nanos, Err(TimestampError::OutOfRange));
    }

    #[test]
    fn each_grammar_failure_has_its_own_error() {
        assert_eq!(parse(b""), Err(CriError::MissingTimestamp));
        assert_eq!(parse(b"2026-09-21T13:33:20Z"), Err(CriError::MissingTimestamp));
        assert_eq!(parse(b"2026-09-21T13:33:20Z stdout"), Err(CriError::MissingStream));
        assert_eq!(parse(b"2026-09-21T13:33:20Z stdin F x"), Err(CriError::UnknownStream));
        assert_eq!(parse(b"2026-09-21T13:33:20Z STDOUT F x"), Err(CriError::UnknownStream));
        assert_eq!(parse(b"2026-09-21T13:33:20Z  F x"), Err(CriError::UnknownStream));
        assert_eq!(parse(b"2026-09-21T13:33:20Z stdout F:x"), Err(CriError::MissingTag));
        assert_eq!(parse(b"{\"log\":\"x\"}"), Err(CriError::MissingTimestamp));
    }

    #[test]
    fn stream_names() {
        assert_eq!(Stream::Stdout.as_str(), "stdout");
        assert_eq!(Stream::Stderr.as_str(), "stderr");
    }
}
