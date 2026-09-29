//! Property test for `docker_in`'s json-file decode, TAIL-09 in
//! `docs/plans/critical-sections-inventory.md`. The contract is
//! `docs/adr/file-tailing-and-docker-json-logs.md`'s 2026-09-28 amendment: reassembly per stream,
//! a `Malformed` entry flushes the held fragments, and `max_line_bytes` bounds the reassembled
//! message while the splitter takes `envelope_cap`.
//!
//! Random stdout and stderr entries, freely interleaved, with `Malformed` entries, resets, and
//! closes among them, are written as dockerd would escape them ([`envelope`]) and fed through the
//! real `LineSplitter` and [`DockerDecoder`] in arbitrary chunks. A close can follow an
//! unterminated last line, which reaches `decode_line` through `take_partial` as at shutdown. After every line the decoder's
//! events, result, `holds_entry`, and `bad_time` count must equal [`Model`]'s. Two checks don't
//! trust the model: every message starts with its writer line's tag and carries no other, and a
//! writer line seen whole with nothing between its first and last entries and within the bound is
//! emitted once, verbatim.
//!
//! The case count is a floor: a `PROPTEST_CASES` above it raises it for a deeper run.

use crate::docker::{envelope_cap, DockerDecoder};
use crate::tail::{LineSplitter, TailDecoder};
use bytes::Bytes;
use logit_core::interner::resolve;
use logit_core::{Diagnostics, Event, Resource};
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;

/// `cases`, or `PROPTEST_CASES` when that is larger.
fn config(cases: u32) -> ProptestConfig {
    let default = ProptestConfig::default();
    ProptestConfig { cases: cases.max(default.cases), ..default }
}

/// Appends `s` as a JSON string body the way dockerd's json-file writer escapes it
/// (`daemon/logger/jsonfilelog/jsonlog/jsonlogbytes.go`): `"`, `\`, `\n`, `\r`, and `\t` as
/// two-byte escapes, other control bytes and `<`, `>`, `&` as `\u00XX`, and U+2028/U+2029 as
/// ` `/` `. `serde_json` escapes none of `<>&`, so it would understate the ratio.
pub(crate) fn docker_escape(s: &str, out: &mut String) {
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '<' | '>' | '&' | '\u{2028}' | '\u{2029}' => {
                write!(out, "\\u{:04x}", c as u32).unwrap();
            }
            c if (c as u32) < 0x20 => write!(out, "\\u{:04x}", c as u32).unwrap(),
            c => out.push(c),
        }
    }
}

/// One json-file line, without its `\n`, as dockerd writes it.
pub(crate) fn envelope(stream: &str, log: &str, time: &str, attrs: &[(&str, &str)]) -> String {
    let mut out = String::from("{\"log\":\"");
    docker_escape(log, &mut out);
    out.push_str("\",\"stream\":\"");
    docker_escape(stream, &mut out);
    out.push_str("\",\"time\":\"");
    docker_escape(time, &mut out);
    out.push('"');
    if !attrs.is_empty() {
        out.push_str(",\"attrs\":{");
        for (i, (k, v)) in attrs.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push('"');
            docker_escape(k, &mut out);
            out.push_str("\":\"");
            docker_escape(v, &mut out);
            out.push('"');
        }
        out.push('}');
    }
    out.push('}');
    out
}

const STREAMS: [&str; 2] = ["stdout", "stderr"];

/// Used as the event timestamp when `time` is unparseable.
const READ_AT: i64 = -7;

/// The first code point of the writer-line tags: line `n`'s entries all start with
/// `char::from_u32(TAG_BASE + n)`, which the alphabet never produces.
const TAG_BASE: u32 = 0xE000;

// -- Strategies -------------------------------------------------------------------------------

fn text() -> impl Strategy<Value = String> {
    let alphabet = prop::sample::select(vec![
        'a', 'b', ' ', '"', '\\', '<', '>', '&', '\t', '\r', '\u{1}', '\u{1f}', '\u{2028}', 'é',
        '日',
    ]);
    prop_oneof![
        prop::collection::vec(alphabet.clone(), 0..8),
        prop::collection::vec(alphabet, 0..100),
    ]
    .prop_map(|chars| chars.into_iter().collect())
}

fn attrs() -> impl Strategy<Value = BTreeMap<String, String>> {
    let key = prop::sample::select(vec!["k", "a&b", "a\"b", "a\nb", "<x>", "log.iostream"]);
    prop::collection::btree_map(key.prop_map(str::to_string), text(), 0..3)
}

#[derive(Debug, Clone)]
enum Op {
    /// An entry on `stream`: a fragment, or the entry that ends its writer line when `complete`.
    Entry {
        stream: usize,
        complete: bool,
        text: String,
        attrs: BTreeMap<String, String>,
        bad_time: bool,
    },
    /// A line that isn't JSON.
    BadJson,
    /// A well-formed entry on a stream Docker doesn't write.
    UnknownStream,
    /// The file was truncated: `TailDecoder::reset`, and a fresh splitter.
    Reset,
    /// The file is closing: `TailDecoder::close`, after the splitter's unterminated last line (an
    /// `Entry`, `BadJson`, or `UnknownStream` written without its `\n`), if any, goes through
    /// `decode_line` as the driver's `close_decoder` does. The decoder is used again after it, as
    /// a stand-in for the next file's decoder starting clean except for a drop in progress.
    Close(Option<Box<Op>>),
}

fn entry() -> impl Strategy<Value = Op> {
    (0..2usize, any::<bool>(), text(), attrs(), prop::bool::weighted(0.1)).prop_map(
        |(stream, complete, text, attrs, bad_time)| Op::Entry {
            stream,
            complete,
            text,
            attrs,
            bad_time,
        },
    )
}

fn op() -> impl Strategy<Value = Op> {
    let torn = prop_oneof![
        8 => entry(),
        1 => Just(Op::BadJson),
        1 => Just(Op::UnknownStream),
    ];
    prop_oneof![
        20 => entry(),
        1 => Just(Op::BadJson),
        1 => Just(Op::UnknownStream),
        1 => Just(Op::Reset),
        1 => prop::option::of(torn).prop_map(|tail| Op::Close(tail.map(Box::new))),
    ]
}

// -- Model ------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
struct Emitted {
    message: String,
    stream: &'static str,
    timestamp: i64,
    attrs: BTreeMap<String, String>,
}

struct Held {
    message: String,
    timestamp: i64,
    attrs: BTreeMap<String, String>,
}

#[derive(Default)]
struct ModelStream {
    held: Option<Held>,
    dropping: bool,
}

/// Per-stream reassembly, restated from the ADR amendment rather than from `DockerDecoder`.
struct Model {
    max: usize,
    streams: [ModelStream; 2],
    bad_time: u64,
}

impl Model {
    fn new(max: usize) -> Self {
        Self { max, streams: Default::default(), bad_time: 0 }
    }

    fn holds(&self) -> bool {
        self.streams.iter().any(|s| s.held.is_some() || s.dropping)
    }

    fn flush(&mut self, out: &mut Vec<Emitted>) {
        for (index, stream) in STREAMS.iter().enumerate() {
            if let Some(held) = self.streams[index].held.take() {
                out.push(emitted(held.message, stream, held.timestamp, &held.attrs));
            }
        }
    }

    fn entry(
        &mut self,
        stream: usize,
        complete: bool,
        text: &str,
        timestamp: Option<i64>,
        attrs: &BTreeMap<String, String>,
        out: &mut Vec<Emitted>,
    ) {
        let max = self.max;
        let state = &mut self.streams[stream];
        if state.dropping {
            state.dropping = !complete;
            return;
        }
        let held_len = state.held.as_ref().map_or(0, |h| h.message.len());
        if held_len + text.len() > max {
            state.held = None;
            state.dropping = !complete;
            return;
        }
        if timestamp.is_none() {
            self.bad_time += 1;
        }
        let timestamp = timestamp.unwrap_or(READ_AT);
        let state = &mut self.streams[stream];
        let held = state.held.get_or_insert(Held {
            message: String::new(),
            timestamp,
            attrs: BTreeMap::new(),
        });
        held.message.push_str(text);
        held.timestamp = timestamp;
        held.attrs = attrs.clone();
        if complete {
            let held = state.held.take().expect("inserted above");
            out.push(emitted(held.message, STREAMS[stream], held.timestamp, &held.attrs));
        }
    }
}

fn emitted(
    message: String,
    stream: &'static str,
    timestamp: i64,
    attrs: &BTreeMap<String, String>,
) -> Emitted {
    let mut attrs = attrs.clone();
    attrs.insert("log.iostream".to_string(), stream.to_string());
    Emitted { message, stream, timestamp, attrs }
}

fn observed(events: &[Event]) -> Vec<Emitted> {
    events
        .iter()
        .map(|event| {
            let attrs: BTreeMap<String, String> = event
                .attributes
                .iter()
                .map(|(k, v)| (resolve(k).to_string(), v.as_str().unwrap().to_string()))
                .collect();
            let stream = match attrs["log.iostream"].as_str() {
                "stdout" => "stdout",
                "stderr" => "stderr",
                other => panic!("unexpected log.iostream {other:?}"),
            };
            Emitted {
                message: event.log.as_ref().unwrap().message.as_str().unwrap().to_string(),
                stream,
                timestamp: event.timestamp,
                attrs,
            }
        })
        .collect()
}

// -- Harness ----------------------------------------------------------------------------------

/// One json-file line the harness wrote, and what the model expects of it.
struct Written {
    bytes: String,
    expect: Expect,
}

enum Expect {
    Entry {
        stream: usize,
        complete: bool,
        text: String,
        timestamp: Option<i64>,
        attrs: BTreeMap<String, String>,
    },
    Malformed,
}

/// A writer line's bookkeeping for the emitted-once check.
#[derive(Default)]
struct WriterLine {
    text: String,
    /// No `Malformed` entry, reset, or close came between its first and last entries.
    clean: bool,
    complete: bool,
}

struct Harness {
    model: Model,
    decoder: DockerDecoder,
    diag: Diagnostics,
    splitter: LineSplitter,
    chunks: Vec<usize>,
    next_chunk: usize,
    pending: Vec<Written>,
    emitted: Vec<Emitted>,
    lines: Vec<WriterLine>,
    /// Each stream's writer line in progress, as an index into `lines`.
    open: [Option<usize>; 2],
}

impl Harness {
    fn new(max: usize, chunks: Vec<usize>) -> Self {
        let diag = Diagnostics::new("docker-verification");
        Self {
            model: Model::new(max),
            decoder: DockerDecoder::new(Arc::new(Resource::default()), max)
                .with_diagnostics(diag.clone()),
            diag,
            splitter: LineSplitter::new(envelope_cap(max)),
            chunks,
            next_chunk: 0,
            pending: Vec::new(),
            emitted: Vec::new(),
            lines: Vec::new(),
            open: [None, None],
        }
    }

    /// Marks every open writer line as interrupted.
    fn interrupt(&mut self) {
        for index in self.open.iter().flatten() {
            self.lines[*index].clean = false;
        }
    }

    fn op(&mut self, index: usize, op: Op) -> Result<(), TestCaseError> {
        match op {
            Op::Entry { .. } | Op::BadJson | Op::UnknownStream => {
                let written = self.write(index, op)?;
                self.pending.push(written);
            }
            Op::Reset => {
                self.feed(None)?;
                self.interrupt();
                self.decoder.reset();
                self.splitter = LineSplitter::new(envelope_cap(self.model.max));
                self.model.streams = Default::default();
                prop_assert!(!self.decoder.holds_entry());
            }
            Op::Close(tail) => {
                let tail = match tail {
                    Some(tail) => Some(self.write(index, *tail)?),
                    None => None,
                };
                self.feed(tail.as_ref().map(|t| t.bytes.as_str()))?;
                let partial = self.splitter.take_partial();
                match tail {
                    Some(tail) => {
                        prop_assert_eq!(partial.as_deref(), Some(tail.bytes.as_bytes()));
                        self.line(partial.unwrap(), tail.expect)?;
                    }
                    None => prop_assert_eq!(partial, None),
                }
                self.interrupt();
                let mut out = Vec::new();
                self.decoder.close(&mut out);
                let mut want = Vec::new();
                self.model.flush(&mut want);
                self.check(&out, want)?;
                prop_assert_eq!(
                    self.decoder.holds_entry(),
                    self.model.streams.iter().any(|s| s.dropping),
                    "after close, only a drop in progress is held"
                );
            }
        }
        Ok(())
    }

    /// Writes one line op as dockerd would, and records its writer line.
    fn write(&mut self, index: usize, op: Op) -> Result<Written, TestCaseError> {
        Ok(match op {
            Op::Entry { stream, complete, text, attrs, bad_time } => {
                let line = match self.open[stream] {
                    Some(line) => line,
                    None => {
                        self.lines.push(WriterLine { clean: true, ..Default::default() });
                        self.lines.len() - 1
                    }
                };
                let tag = char::from_u32(TAG_BASE + line as u32).unwrap();
                let text = format!("{tag}{text}");
                self.lines[line].text.push_str(&text);
                if complete {
                    self.lines[line].complete = true;
                    self.open[stream] = None;
                } else {
                    self.open[stream] = Some(line);
                }
                let time = if bad_time {
                    "not-a-time".to_string()
                } else {
                    format!("2026-08-17T19:35:{:02}.{:09}Z", index % 60, index)
                };
                let timestamp =
                    (!bad_time).then(|| logit_core::parse_rfc3339_to_nanos(&time).unwrap());
                let log = if complete { format!("{text}\n") } else { text.clone() };
                let pairs: Vec<(&str, &str)> =
                    attrs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                let bytes = envelope(STREAMS[stream], &log, &time, &pairs);
                prop_assert!(bytes.len() <= envelope_cap(self.model.max));
                Written {
                    bytes,
                    expect: Expect::Entry { stream, complete, text, timestamp, attrs },
                }
            }
            Op::BadJson => {
                self.interrupt();
                Written { bytes: "{\"log\":\"torn".to_string(), expect: Expect::Malformed }
            }
            Op::UnknownStream => {
                self.interrupt();
                Written {
                    bytes: envelope("stdin", "x\n", "2026-08-17T19:35:46.000000000Z", &[]),
                    expect: Expect::Malformed,
                }
            }
            Op::Reset | Op::Close(_) => unreachable!("not a line"),
        })
    }

    /// Writes every pending line, then `torn` with no `\n`, through the splitter in the next chunk
    /// sizes, decoding each line the splitter yields and checking it against the model. `torn`
    /// stays in the splitter for `take_partial`.
    fn feed(&mut self, torn: Option<&str>) -> Result<(), TestCaseError> {
        let mut stream = String::new();
        for written in &self.pending {
            stream.push_str(&written.bytes);
            stream.push('\n');
        }
        stream.push_str(torn.unwrap_or_default());
        let expects: Vec<Expect> = self.pending.drain(..).map(|w| w.expect).collect();
        let mut expects = expects.into_iter();
        let bytes = Bytes::from(stream);
        let mut at = 0;
        while at < bytes.len() {
            let size = self.chunks[self.next_chunk % self.chunks.len()];
            self.next_chunk += 1;
            let end = (at + size).min(bytes.len());
            let mut lines = Vec::new();
            let stats = self.splitter.push(bytes.slice(at..end), |line, _| lines.push(line));
            prop_assert_eq!(stats.dropped_lines, 0, "no envelope may exceed envelope_cap");
            for line in lines {
                let expect = expects.next().expect("one expectation per written line");
                self.line(line, expect)?;
            }
            at = end;
        }
        prop_assert!(expects.next().is_none(), "every written line reaches the decoder");
        Ok(())
    }

    fn line(&mut self, line: Bytes, expect: Expect) -> Result<(), TestCaseError> {
        let mut out = Vec::new();
        let result = self.decoder.decode_line(line, READ_AT, &mut out);
        let mut want = Vec::new();
        match expect {
            Expect::Entry { stream, complete, text, timestamp, attrs } => {
                prop_assert!(result.is_ok(), "a well-formed entry decodes: {:?}", result.err());
                self.model.entry(stream, complete, &text, timestamp, &attrs, &mut want);
            }
            Expect::Malformed => {
                prop_assert!(result.is_err(), "a malformed entry is rejected");
                self.model.flush(&mut want);
            }
        }
        self.check(&out, want)
    }

    fn check(&mut self, out: &[Event], want: Vec<Emitted>) -> Result<(), TestCaseError> {
        let got = observed(out);
        prop_assert_eq!(&got, &want);
        for event in &got {
            prop_assert!(event.message.len() <= self.model.max, "message over max_line_bytes");
            let tags: Vec<u32> = event
                .message
                .chars()
                .map(|c| c as u32)
                .filter(|c| (TAG_BASE..TAG_BASE + 0x1000).contains(c))
                .collect();
            prop_assert!(!tags.is_empty(), "every message starts inside a writer line");
            prop_assert!(
                tags.iter().all(|&t| t == tags[0]),
                "a message mixes writer lines {:?}",
                tags
            );
        }
        self.emitted.extend(got);
        prop_assert_eq!(self.decoder.holds_entry(), self.model.holds());
        prop_assert_eq!(self.diag.occurrences("bad_time"), self.model.bad_time);
        Ok(())
    }

    /// Every writer line seen whole, uninterrupted, and within the bound is emitted once,
    /// verbatim.
    fn check_emitted_once(&self) -> Result<(), TestCaseError> {
        for (index, line) in self.lines.iter().enumerate() {
            if !(line.complete && line.clean && line.text.len() <= self.model.max) {
                continue;
            }
            let tag = char::from_u32(TAG_BASE + index as u32).unwrap();
            let copies: Vec<&Emitted> =
                self.emitted.iter().filter(|e| e.message.starts_with(tag)).collect();
            prop_assert_eq!(
                copies.len(),
                1,
                "writer line {} emitted {} times",
                index,
                copies.len()
            );
            prop_assert_eq!(&copies[0].message, &line.text);
        }
        Ok(())
    }
}

fn run(max: usize, chunks: Vec<usize>, ops: Vec<Op>) -> Result<(), TestCaseError> {
    let mut harness = Harness::new(max, chunks);
    for (index, op) in ops.into_iter().enumerate() {
        harness.op(index, op)?;
    }
    harness.feed(None)?;
    harness.check_emitted_once()
}

proptest! {
    #![proptest_config(config(256))]

    #[test]
    fn docker_decoder_matches_the_per_stream_model(
        max in 1usize..=200,
        chunks in prop::collection::vec(prop_oneof![1usize..16, 16usize..4096], 1..6),
        ops in prop::collection::vec(op(), 0..60),
    ) {
        run(max, chunks, ops)?;
    }
}

/// `envelope` against `serde_json`: whatever dockerd escapes, the decoder reads back the same
/// strings, keys included.
#[test]
fn a_docker_escaped_envelope_decodes_to_the_same_strings() {
    let log = "a\"b\\c<d>&e\tf\rg\u{1}h\u{2028}日\n";
    let line = envelope("stderr", log, "2026-08-17T19:35:46Z", &[("a&b", "<v>"), ("a\nb", "\"")]);
    assert!(line.contains("\\u003c") && line.contains("\\u0026") && line.contains("\\u2028"));
    let parsed: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(parsed["log"], log);
    assert_eq!(parsed["stream"], "stderr");
    assert_eq!(parsed["attrs"]["a&b"], "<v>");
    assert_eq!(parsed["attrs"]["a\nb"], "\"");
}
