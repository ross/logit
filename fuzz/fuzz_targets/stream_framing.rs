//! A connection's bytes through the stream `Framer`, pushed and drained as `logit_inputs::tcp`'s
//! driver does it. Byte 0 picks the mode, modulo 5: `Rfc6587Auto`, `Lines` draining, `Lines`
//! fatal, the big-endian length prefix, the little-endian one. Bytes 1-2 pick the frame bound:
//! `0xFFFF` is `MAX_FRAME_BYTES`, anything else `1..=4096`. Byte 3 seeds the push sizes, empty
//! pushes included, and the rest is the stream.
//!
//! Oracles, each over every run:
//! - chunking: the outcome sequence is the same for one push of the stream and for the chunked
//!   pushes, with `drained` read as `oversize`, since only a chunk boundary decides whether a
//!   line crosses the bound before its `LF` arrives;
//! - residency: after every `Ok(None)` the buffer holds at most the bound plus the framing's
//!   header (`crates/logit-proto/src/framing.rs`'s "What a connection holds");
//! - progress: every frame and every non-fatal error shrinks the buffer;
//! - a reference model: under `Lines` and both length prefixes, a splitter written here produces
//!   the same sequence;
//! - the latch: `framing()` is `None` only under `Rfc6587Auto` before a non-empty push, and never
//!   changes once set;
//! - FIN and RST agree: `finish` and `abandon` over the same pushes count the same bytes, except
//!   that `finish` delivers `Rfc6587Auto`'s LF remainder.
#![no_main]

use libfuzzer_sys::fuzz_target;
use logit_proto::framing::{Framer, Framing, FramingMode, Oversize, MAX_FRAME_BYTES};

/// One step of a connection: a frame, a framing error (`reason`, `is_fatal`), or the end of a
/// clean close, after `finish`'s own outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    Frame(Vec<u8>),
    Err(&'static str, bool),
    Closed,
}

fn mode_for(selector: u8) -> FramingMode {
    match selector % 5 {
        0 => FramingMode::Rfc6587Auto,
        1 => FramingMode::Lines { oversize: Oversize::DrainToNextLine },
        2 => FramingMode::Lines { oversize: Oversize::Fatal },
        3 => FramingMode::LengthPrefixed,
        _ => FramingMode::LengthPrefixedLe,
    }
}

fn bound_for(raw: u16) -> usize {
    if raw == u16::MAX {
        MAX_FRAME_BYTES
    } else {
        1 + usize::from(raw) % 4096
    }
}

/// Push sizes from a seed: mostly short, sometimes a whole read buffer, and an empty push now and
/// then, never two in a row.
fn chunks(stream: &[u8], seed: u8) -> Vec<&[u8]> {
    let mut state = u64::from(seed).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut out = Vec::new();
    let mut rest = stream;
    let mut last_empty = false;
    while !rest.is_empty() {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let roll = (state >> 33) as usize;
        let size = match roll % 16 {
            0 if !last_empty => 0,
            1 => 1 + roll / 16 % 8192,
            _ => 1 + roll / 16 % 64,
        };
        let (chunk, tail) = rest.split_at(size.min(rest.len()));
        out.push(chunk);
        rest = tail;
        last_empty = size == 0;
    }
    out
}

/// The header a framing can hold beside a frame of the bound, between pushes.
fn header_slack(framing: Option<Framing>) -> usize {
    match framing {
        Some(Framing::OctetCounting) => 10,
        Some(Framing::LengthPrefixed | Framing::LengthPrefixedLe) => 4,
        Some(Framing::NonTransparent) | None => 0,
    }
}

/// A connection fed `pushes`, draining after each, up to its first fatal error: the outcomes and
/// the framer, for the caller to close. Checks the residency, progress, and latch oracles.
fn run(mode: FramingMode, bound: usize, pushes: &[&[u8]]) -> (Vec<Outcome>, Framer, bool) {
    let mut framer = Framer::new(mode, bound);
    let mut outcomes = Vec::new();
    let mut latched = framer.framing();
    let mut seen = false;
    assert_eq!(
        latched.is_none(),
        mode == FramingMode::Rfc6587Auto,
        "latch: only the auto mode starts unlatched"
    );
    for push in pushes {
        framer.push(push);
        seen |= !push.is_empty();
        let framing = framer.framing();
        assert_eq!(
            framing.is_none(),
            mode == FramingMode::Rfc6587Auto && !seen,
            "latch: unlatched only under the auto mode before a non-empty push"
        );
        if latched.is_some() {
            assert_eq!(framing, latched, "latch: a latched framing never changes");
        }
        latched = framing;
        loop {
            let before = framer.buffered();
            match framer.next_frame() {
                Ok(Some(frame)) => {
                    assert!(framer.buffered() < before, "progress: a frame consumes bytes");
                    outcomes.push(Outcome::Frame(frame.to_vec()));
                }
                Ok(None) => {
                    let slack = header_slack(framer.framing());
                    assert!(
                        framer.buffered() <= bound + slack,
                        "residency: {} buffered over a {bound}-byte bound",
                        framer.buffered()
                    );
                    break;
                }
                Err(err) => {
                    outcomes.push(Outcome::Err(err.reason(), err.is_fatal()));
                    if err.is_fatal() {
                        return (outcomes, framer, true);
                    }
                    assert!(framer.buffered() < before, "progress: a skipped line consumes bytes");
                }
            }
        }
    }
    (outcomes, framer, false)
}

/// Appends `finish`'s outcome, if any, then `Closed`, and returns that outcome.
fn close(framer: &mut Framer, outcomes: &mut Vec<Outcome>) -> Option<Outcome> {
    let fin = match framer.finish() {
        Ok(Some(frame)) => Some(Outcome::Frame(frame.to_vec())),
        Ok(None) => None,
        Err(err) => Some(Outcome::Err(err.reason(), err.is_fatal())),
    };
    assert_eq!(framer.buffered(), 0, "finish consumes the remainder");
    assert_eq!(framer.finish(), Ok(None), "a second finish has nothing left");
    outcomes.extend(fin.clone());
    outcomes.push(Outcome::Closed);
    fin
}

/// `drained` read as `oversize`, for the comparisons a chunk boundary can change.
fn normalized(outcomes: &[Outcome]) -> Vec<Outcome> {
    outcomes
        .iter()
        .map(|outcome| match outcome {
            Outcome::Err("drained", fatal) => Outcome::Err("oversize", *fatal),
            other => other.clone(),
        })
        .collect()
}

/// `Lines`: split on `LF`, strip one `CR`, skip an empty line, drop an over-bound piece (fatal
/// under `Fatal`); at the close, a tail over the bound is oversize, a blank one nothing, and any
/// other truncated.
fn lines_model(stream: &[u8], bound: usize, fatal: bool) -> Vec<Outcome> {
    let mut out = Vec::new();
    let mut rest = stream;
    while let Some(at) = rest.iter().position(|&b| b == b'\n') {
        let piece = &rest[..at];
        rest = &rest[at + 1..];
        if piece.len() > bound {
            out.push(Outcome::Err("oversize", fatal));
            if fatal {
                return out;
            }
            continue;
        }
        let line = piece.strip_suffix(b"\r").unwrap_or(piece);
        if !line.is_empty() {
            out.push(Outcome::Frame(line.to_vec()));
        }
    }
    if rest.len() > bound {
        out.push(Outcome::Err("oversize", fatal));
        if fatal {
            return out;
        }
    } else if !rest.iter().all(u8::is_ascii_whitespace) {
        out.push(Outcome::Err("truncated", true));
    }
    out.push(Outcome::Closed);
    out
}

/// A 4-byte length then that many bytes; a length over the bound is fatal, and a close with any
/// byte left is truncated.
fn length_prefixed_model(stream: &[u8], bound: usize, big_endian: bool) -> Vec<Outcome> {
    let mut out = Vec::new();
    let mut rest = stream;
    while let Some((prefix, body)) = rest.split_first_chunk::<4>() {
        let len =
            if big_endian { u32::from_be_bytes(*prefix) } else { u32::from_le_bytes(*prefix) };
        let len = len as usize;
        if len > bound {
            out.push(Outcome::Err("oversize", true));
            return out;
        }
        if body.len() < len {
            break;
        }
        out.push(Outcome::Frame(body[..len].to_vec()));
        rest = &body[len..];
    }
    if !rest.is_empty() {
        out.push(Outcome::Err("truncated", true));
    }
    out.push(Outcome::Closed);
    out
}

fuzz_target!(|data: &[u8]| {
    let Some((&[selector, hi, lo, seed], stream)) = data.split_first_chunk::<4>() else {
        return;
    };
    let mode = mode_for(selector);
    let bound = bound_for(u16::from_be_bytes([hi, lo]));
    let pushes = chunks(stream, seed);

    let (mut whole, mut whole_framer, whole_stopped) = run(mode, bound, &[stream]);
    if !whole_stopped {
        close(&mut whole_framer, &mut whole);
    }
    let (mut chunked, mut fin, stopped) = run(mode, bound, &pushes);
    let fin_outcome = if stopped { None } else { close(&mut fin, &mut chunked) };
    assert_eq!(
        normalized(&chunked),
        normalized(&whole),
        "chunking: {} pushes and one push disagree",
        pushes.len()
    );
    if !matches!(mode, FramingMode::Lines { oversize: Oversize::DrainToNextLine }) {
        assert!(
            !chunked.contains(&Outcome::Err("drained", false)),
            "drained: only a draining line framer drains"
        );
    }

    let model = match mode {
        FramingMode::Lines { oversize } => {
            Some(lines_model(stream, bound, oversize == Oversize::Fatal))
        }
        FramingMode::LengthPrefixed => Some(length_prefixed_model(stream, bound, true)),
        FramingMode::LengthPrefixedLe => Some(length_prefixed_model(stream, bound, false)),
        FramingMode::Rfc6587Auto => None,
    };
    if let Some(model) = model {
        assert_eq!(normalized(&whole), model, "model: the framer and the splitter disagree");
    }

    if !stopped {
        // The same pushes again, ended by an RST instead of a FIN.
        let (_, mut rst, _) = run(mode, bound, &pushes);
        let held = rst.buffered();
        let framing = rst.framing();
        let abandoned = rst.abandon().map(|err| err.reason());
        assert_eq!(rst.buffered(), 0, "fin/rst: abandon consumes the remainder");
        if abandoned.is_some() {
            assert!(held > 0, "fin/rst: an RST over nothing counts nothing");
        }
        match fin_outcome {
            Some(Outcome::Err(reason, _)) => {
                assert_eq!(
                    reason, "truncated",
                    "fin/rst: a close after the drain loop can only truncate"
                );
                assert_eq!(
                    abandoned,
                    Some("truncated"),
                    "fin/rst: a FIN counted and an RST didn't"
                );
            }
            Some(Outcome::Frame(_)) => {
                assert!(
                    mode == FramingMode::Rfc6587Auto && framing == Some(Framing::NonTransparent),
                    "fin/rst: only the auto mode's LF framing delivers at a FIN"
                );
                assert_eq!(abandoned, Some("truncated"), "fin/rst: an RST counts what a FIN sent");
            }
            Some(Outcome::Closed) => unreachable!("close never returns Closed"),
            None => assert_eq!(abandoned, None, "fin/rst: an RST counted and a FIN didn't"),
        }
    }
});
