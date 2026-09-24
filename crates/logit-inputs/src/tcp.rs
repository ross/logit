//! The shared TCP (optionally TLS) listener driver behind `syslog_in`, `graphite_in`, and
//! `statsd_in` under `transport: tcp`: an accept loop, one connection task per peer, framing, and
//! frame-to-batch assembly. The stream twin of [`crate::udp::UdpListener`]
//! (`docs/adr/decoupled-listener-io.md`), generic over the decoder for the same reason: the decoder
//! is the only thing two such listeners differ in. Nothing here is protocol-specific
//! (`docs/adr/syslog-tcp-ingress-and-tls.md`).
//!
//! **Framing is chosen per listener, not guessed per driver.** [`FramingMode`] is set once, at
//! construction, through [`TcpListener::with_framing`]: RFC 6587's auto-detecting pair for
//! `syslog_in`, LF-delimited lines for a line protocol whose messages may *start* with a digit
//! (`graphite_in` plaintext, `statsd_in`), or carbon's 4-byte big-endian length prefix
//! (`docs/adr/graphite-carbon-relay.md`). A builder rather than a [`TcpListenerConfig`] field:
//! that struct is the image of the `receive:` config block, and framing is not something an
//! operator sets.
//!
//! **`D: Clone` is load-bearing.** Every connection gets its own decoder clone, because a decoder
//! may hold per-connection state (scratch buffers, or sticky identity the way `collectd`'s
//! decoder holds it per datagram). `SyslogDecoder`'s clonable state is only its `Diagnostics`,
//! whose counts every clone shares. One decoder behind a lock would serialize every connection's
//! decode against every other's.
//!
//! **No receive queue.** Unlike the UDP driver, there is no [`crate::udp::ReceiveQueue`] here and
//! no `receive.max_datagrams`/`max_bytes`/`overflow` to configure. TCP's own flow control *is* the
//! queue: a connection whose downstream has stalled stops being read, the kernel window closes,
//! and the sender blocks. That is correct for a reliable transport, where dropping bytes to keep
//! reading (the UDP driver's `drop_oldest` default) would corrupt the frame stream rather than
//! lose one self-contained datagram.
//!
//! **Batching is per connection.** Each connection task owns its own
//! [`logit_pipeline::BatchAccumulator`], so `batch_max_events` bounds one connection's in-flight
//! events, not the listener's: N concurrent connections can hold N times that.
//!
//! **Connection limit.** A [`tokio::sync::Semaphore`] with `try_acquire_owned`, capped at
//! [`MAX_CONCURRENT_CONNECTIONS`], as in `logit_in` (`crates/logit-inputs/src/logit.rs`'s
//! "Connection limit" section): reject, don't queue. **The one difference from `logit_in`:**
//! there, a past-the-cap connection is wrapped in TLS first so it can be told why it is being
//! closed (a `Reject` control frame). None of this driver's protocols has an in-band reject
//! message, so there is nothing to spend a handshake saying: a past-the-cap connection is dropped
//! immediately, before any TLS accept, and counted as
//! `logit.input.connections.rejected{reason="limit"}`. The `logit.input.connections` gauge counts
//! permit holders only.
//!
//! **Pre-handshake timeout.** [`HANDSHAKE_TIMEOUT`] (overridden by the operator's
//! `handshake_timeout:` through [`TcpListener::with_handshake_timeout`]) bounds each of a
//! connection's two pre-message phases *independently*, as `logit_in` bounds its own two: the TLS
//! accept (in the accept loop's `Some` arm, when TLS is configured), then the wait for the
//! connection's first byte inside [`serve_connection`], which starts a fresh budget of the same
//! length rather than inheriting a shared deadline. So on the TLS path the worst case is two of
//! these back to back (10s at the default) before a silent connection gives up its permit.
//!
//! **The first-byte bound applies on both arms, plaintext included.** No `tls:` block is the
//! default shape, and without the bound 1024 connections that complete the TCP handshake and then
//! send nothing would hold every permit forever, at a cost to the peer of 1024 SYNs and no bytes.
//! The bound covers only the *first* byte (until [`Framer::first_byte_seen`] is true), the phase
//! with no legitimate reason to be slow. The opt-in idle timeout bounds the gaps after it.
//!
//! The predicate is `first_byte_seen`, not "has the framer latched a [`Framing`]": only
//! [`FramingMode::Rfc6587Auto`] has anything to latch, so under either explicit mode a
//! latch-shaped predicate would read "already framed" on a connection that has not sent a byte,
//! and the deadline would never fire. `the_first_byte_deadline_applies_under_every_framing_mode`
//! pins it.
//!
//! **Idle timeout.** [`TcpListener::with_idle_timeout`] (the operator's `idle_timeout:`) is off
//! unless set, and when set bounds how long a connection may stay quiet before this listener
//! closes it and hands its permit back (`docs/adr/idle-connection-timeout.md`). It shares one
//! next-byte deadline with the first-byte bound: whichever phase the connection is in supplies the
//! deadline, so there is only ever one clock on the read.
//!
//! *What resets it.* The deadline is `last_progress + idle_timeout`, and `last_progress` advances
//! on two things only: bytes read from the peer (stamped after the inner frame loop drains, which
//! also covers an [`absorb_frame`] emit returning), and this connection's own interval flush
//! emitting a batch. A flush tick with nothing to emit does neither, so this process's own timer
//! never re-arms the clock.
//!
//! *Why time blocked downstream never counts.* [`emit`] awaits `Fanout::send`, which awaits a
//! bounded channel's capacity; a connection parked there is waiting on us, not idle. Because
//! `last_progress` is stamped when that await *returns* and the deadline is consulted only while
//! this task is in the read, a full downstream can never make a busy connection look quiet.
//!
//! *Why `Ok(())`.* An idle close is policy, not a fault: it returns `Ok(())`, so it never reaches
//! the accept loop's `connection_error` diagnostic, and is counted
//! `logit.input.connections.closed{reason="idle"}` instead. On the way out, complete accumulated
//! events are flushed [`FlushReason::Closed`] and a buffered *partial* frame is reported through
//! [`report_buffered_tail`], as the shutdown and RST paths do.

use crate::Input;
use bytes::{Bytes, BytesMut};
use logit_core::{Diagnostics, Event, EventBatch, Telemetry};
use logit_pipeline::sockstat;
use logit_pipeline::{BatchAccumulator, Fanout, FlushReason};
use logit_proto::Decoder;
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;

/// `crate::tls::TlsServerSettings`, re-exported for symmetry with `crate::logit`/`crate::otlp`.
pub use crate::tls::TlsServerSettings;

/// See this module's "Connection limit" doc section. The same number `logit_in` and `otlp_in` use:
/// one shared figure is one thing for an operator to learn.
const MAX_CONCURRENT_CONNECTIONS: usize = 1024;

/// How long a connection has, per pre-message phase, before this listener releases its
/// connection-limit permit: the TLS accept when TLS is configured, and on both arms the wait for
/// the first byte. Each phase gets its own budget, so a silent TLS connection costs two. See this
/// module's "Pre-handshake timeout" doc section.
///
/// The default only: the `handshake_timeout:` field on `syslog_in`/`graphite_in`/`statsd_in`
/// overrides it through [`TcpListener::with_handshake_timeout`]. `logit_config`'s
/// `default_handshake_timeout` mirrors this number by hand (it cannot depend on this crate).
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// The largest single frame this driver assembles, in bytes, for any framing: the default for
/// [`TcpListener::with_framing`]'s second argument when a listener never calls it.
///
/// Not configurable on `syslog_in` or `statsd_in`. `graphite_in` overrides it with its own
/// `max_line_bytes`/`max_frame_bytes`, which carbon's receivers expose. It is *not* tied to
/// `syslog_out`'s `max_message_bytes` (8192): that is a sender-side knob an operator may raise,
/// and a receiver ceiling tracking it would need re-tuning in lockstep with every sender. 64 KiB
/// matches the practical per-message ceiling the UDP driver's 65507-byte read buffer already
/// imposes, and is generous against RFC 5424's "receiver MUST be able to accept 2048 octets" and
/// every real sender's default.
pub const MAX_FRAME_BYTES: usize = 65_536;

/// Bytes pulled off the socket per read. Well under [`MAX_FRAME_BYTES`]: a listener pays this per
/// connection, and [`Framer`] assembles a larger frame across reads anyway.
const READ_BUFFER_BYTES: usize = 8 * 1024;

/// Bytes in [`FramingMode::LengthPrefixed`]'s frame prefix: one big-endian `u32` payload length
/// (Twisted's `Int32StringReceiver`, which carbon's pickle listener speaks). A local copy of
/// `logit_proto::graphite::pickle::LENGTH_PREFIX_BYTES` so this driver names nothing
/// graphite-specific; the assert below keeps the two equal.
const LENGTH_PREFIX_BYTES: usize = 4;

const _: () = assert!(LENGTH_PREFIX_BYTES == logit_proto::graphite::pickle::LENGTH_PREFIX_BYTES);

// ---- framing ---------------------------------------------------------------------------------

/// How a [`Framer`] delimits one connection's messages. Chosen once per listener, through
/// [`TcpListener::with_framing`], and never re-evaluated.
///
/// Explicit rather than "always sniff the first byte" because the sniff is sound only for syslog:
/// it reads a leading ASCII digit as an RFC 6587 octet count, right for a protocol whose every
/// non-transparent message starts `<` and wrong for one whose lines routinely start with a digit
/// (`1.hits:1|c` in statsd, a carbon path beginning with a host number).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FramingMode {
    /// RFC 6587's two framings, auto-detected from the connection's first byte and latched for its
    /// life (`docs/adr/syslog-tcp-ingress-and-tls.md`). `syslog_in`'s mode, and nothing else's.
    Rfc6587Auto,
    /// LF-delimited lines only, never octet counting, whatever the first byte is. `graphite_in`
    /// plaintext and `statsd_in`.
    Lines {
        /// What a line past the frame bound does.
        oversize: Oversize,
    },
    /// A 4-byte big-endian payload length, then that many bytes: Twisted's `Int32StringReceiver`,
    /// which is how carbon frames a pickle batch (`crates/logit-proto/src/graphite/pickle.rs`).
    LengthPrefixed,
}

/// What [`FramingMode::Lines`] does with a line past the frame bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Oversize {
    /// Close the connection, as RFC 6587 framing does: a line past the ceiling can only get
    /// longer, and under octet counting there is no resync point.
    Fatal,
    /// Skip that one line and resynchronize at the next `LF`, counting the skip **once**. Carbon's
    /// behaviour (`docs/adr/graphite-carbon-relay.md`): one pathological datapoint must not cost a
    /// busy relay's whole connection, and an LF-delimited stream has an unambiguous resync point.
    DrainToNextLine,
}

/// Which framing a connection is speaking, once known.
///
/// Under [`FramingMode::Rfc6587Auto`] this is latched from the first byte a connection sends and
/// never re-evaluated (`docs/adr/syslog-tcp-ingress-and-tls.md`): an ASCII digit can only begin an
/// octet count, since a non-transparent syslog frame always begins with the PRI's `<`. Anything
/// else is non-transparent. Under either explicit mode it is fixed at construction.
///
/// The latch keys on any ASCII digit, not `1`-`9`, although RFC 6587 §3.4.1's `MSG-LEN =
/// NONZERO-DIGIT *DIGIT` forbids a leading zero. A leading `0` is a malformed octet count, so it
/// latches octet counting and fails loudly in [`Framer::next_frame`]; reading it as
/// non-transparent would mis-frame a broken sender's whole stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Framing {
    /// RFC 6587 §3.4.1: `MSG-LEN SP MSG`, where `MSG-LEN` is the octet count of `MSG`. The only
    /// framing that can carry a message containing a newline.
    OctetCounting,
    /// RFC 6587 §3.4.2: messages separated by a trailing `LF` (a `CR` before it is stripped). Also
    /// what [`FramingMode::Lines`] speaks from the first byte.
    NonTransparent,
    /// A 4-byte big-endian payload length, then that many payload bytes
    /// ([`FramingMode::LengthPrefixed`]).
    LengthPrefixed,
}

impl Framing {
    pub fn as_str(self) -> &'static str {
        match self {
            Framing::OctetCounting => "octet_counting",
            Framing::NonTransparent => "non_transparent",
            Framing::LengthPrefixed => "length_prefixed",
        }
    }
}

/// Why [`Framer`] could not produce the next frame.
///
/// All but [`FrameError::OversizeSkipped`] are fatal *to the connection*
/// ([`FrameError::is_fatal`]), so the driver counts, diagnoses, and closes: an untrusted octet
/// count leaves no way to find the next frame, a declared length past the ceiling has nothing
/// buffered after it, and a line past the ceiling would only get longer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
    /// A frame larger than this listener's frame bound -- a declared octet count or length prefix
    /// above it, or a line that passed it without a terminator under [`Oversize::Fatal`].
    Oversize(String),
    /// An octet count that is not one RFC 6587 §3.4.1's `MSG-LEN = NONZERO-DIGIT *DIGIT`
    /// production permits: a non-digit before the SP, a leading zero (a zero count included), or
    /// more than nine digits.
    Malformed(String),
    /// The peer closed mid-frame: under octet counting or a length prefix, bytes the declared
    /// length promised are missing; under [`FramingMode::Lines`], a non-whitespace remainder has
    /// no `LF`. Nothing was wrong with what the peer sent; it stopped. See [`Framer::finish`].
    Truncated(String),
    /// One line past the frame bound under [`Oversize::DrainToNextLine`]: dropped, counted, and
    /// resynchronized at the next `LF`. The one **non-fatal** variant: the connection stays open
    /// and the next line still decodes.
    OversizeSkipped(String),
}

impl FrameError {
    /// The `reason` tag on `logit.input.frames.dropped`
    /// (`docs/design/internal-telemetry.md`'s "Naming" section). `OversizeSkipped` shares
    /// `oversize` with its fatal sibling: the operator cares that a frame was too big, and
    /// `is_fatal` says whether the connection survived.
    pub fn reason(&self) -> &'static str {
        match self {
            FrameError::Oversize(_) | FrameError::OversizeSkipped(_) => "oversize",
            FrameError::Malformed(_) => "malformed",
            FrameError::Truncated(_) => "truncated",
        }
    }

    /// Whether this error ends the connection. Only [`FrameError::OversizeSkipped`] does not:
    /// every other variant leaves the framer with no trustworthy resync point.
    pub fn is_fatal(&self) -> bool {
        !matches!(self, FrameError::OversizeSkipped(_))
    }
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::Oversize(detail)
            | FrameError::Malformed(detail)
            | FrameError::Truncated(detail)
            | FrameError::OversizeSkipped(detail) => f.write_str(detail),
        }
    }
}

impl std::error::Error for FrameError {}

/// Frame extraction over a byte stream: pure, socket-free, and synchronous, so a recorded interop
/// fixture can be replayed through it byte for byte without standing anything up.
///
/// `push` whatever came off the socket, then call `next_frame` until it returns `Ok(None)`; at EOF,
/// call [`Framer::finish`] once for whatever partial frame is left.
pub struct Framer {
    /// How this connection's messages are delimited; fixed at construction.
    mode: FramingMode,
    /// The largest single frame this connection will assemble. Per listener, not a constant:
    /// `syslog_in`/`statsd_in` take [`MAX_FRAME_BYTES`], a `graphite_in` takes its operator-facing
    /// `max_line_bytes`/`max_frame_bytes`.
    max_frame_bytes: usize,
    /// `None` only under [`FramingMode::Rfc6587Auto`] before the first byte arrives (the latch
    /// rule is on [`Framing`]). Both explicit modes set it at construction.
    framing: Option<Framing>,
    buf: BytesMut,
    /// How far into `buf` the line path has already looked for an `LF` without finding one. Reset
    /// whenever a frame is taken. Without it, a long line arriving over many reads is rescanned
    /// from the start on every read: O(n^2) in the line's length.
    scanned: usize,
    /// Set when a line passed the bound with no `LF` under [`Oversize::DrainToNextLine`]:
    /// everything up to and including the next `LF` belongs to that abandoned line and is
    /// discarded uncounted (the skip was counted once, when the bound was crossed).
    draining: bool,
    /// Whether this connection has ever produced a byte: the first-byte deadline's predicate
    /// ([`Self::first_byte_seen`]).
    seen_bytes: bool,
}

impl Framer {
    /// A framer speaking `mode`, refusing any single frame larger than `max_frame_bytes`.
    ///
    /// No `Default`: both arguments are per-listener decisions (a `graphite_in` plaintext
    /// connection bounds lines at `max_line_bytes` and drains past them; a `syslog_in` connection
    /// bounds RFC 6587 frames at [`MAX_FRAME_BYTES`] and closes), and a default would pick
    /// syslog's.
    pub fn new(mode: FramingMode, max_frame_bytes: usize) -> Self {
        let framing = match mode {
            FramingMode::Rfc6587Auto => None,
            FramingMode::Lines { .. } => Some(Framing::NonTransparent),
            FramingMode::LengthPrefixed => Some(Framing::LengthPrefixed),
        };
        Self {
            mode,
            max_frame_bytes,
            framing,
            buf: BytesMut::with_capacity(READ_BUFFER_BYTES),
            scanned: 0,
            draining: false,
            seen_bytes: false,
        }
    }

    /// The framing this connection is speaking, or `None` if it is an [`FramingMode::Rfc6587Auto`]
    /// connection that has not sent a byte yet.
    pub fn framing(&self) -> Option<Framing> {
        self.framing
    }

    /// Whether this connection has ever produced a byte: the first-byte deadline's predicate
    /// (this module's "Pre-handshake timeout" doc section). Not `framing().is_some()`, which is
    /// true from construction under both explicit modes and would make that deadline inert on
    /// every listener but `syslog_in`.
    pub fn first_byte_seen(&self) -> bool {
        self.seen_bytes
    }

    /// Bytes held but not yet formed into a frame. Read by `report_buffered_tail` on the paths
    /// that end a connection without reaching [`Framer::finish`] (a peer RST mid-message, or
    /// shutdown), so a discarded partial frame is still counted.
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    /// Appends whatever came off the socket. Under [`FramingMode::Rfc6587Auto`] only, latches
    /// [`Framing`] on the first byte ever pushed.
    pub fn push(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
        self.seen_bytes |= !bytes.is_empty();
        if matches!(self.mode, FramingMode::Rfc6587Auto) && self.framing.is_none() {
            if let Some(&first) = self.buf.first() {
                self.framing = Some(if first.is_ascii_digit() {
                    Framing::OctetCounting
                } else {
                    Framing::NonTransparent
                });
            }
        }
    }

    /// The next complete frame, if one is fully buffered.
    ///
    /// `Ok(None)` means "need more bytes", not "end of stream": only the caller knows the socket
    /// closed, and says so via [`Framer::finish`]. The returned [`Bytes`] is the message verbatim:
    /// under octet counting the declared MSG-LEN bytes (an embedded `LF` is payload), under
    /// non-transparent the line with its `LF` and at most one preceding `CR` removed, under a
    /// length prefix the payload without its prefix.
    pub fn next_frame(&mut self) -> Result<Option<Bytes>, FrameError> {
        loop {
            match self.framing {
                None => return Ok(None),
                Some(Framing::OctetCounting) => return self.next_octet_counted(),
                Some(Framing::LengthPrefixed) => return self.next_length_prefixed(),
                Some(Framing::NonTransparent) => match self.next_line()? {
                    // An empty line carries no message (a stray `LF` after a `CRLF`, a
                    // keepalive newline), and RFC 6587 §3.4.2 gives a receiver nothing to do with
                    // one: skip it rather than hand the decoder an empty frame to reject.
                    Some(line) if line.is_empty() => continue,
                    other => return Ok(other),
                },
            }
        }
    }

    /// Whatever is left when the peer closes. What a terminator-less remainder means depends on
    /// the framing:
    ///
    /// - Octet counting or a length prefix: [`FrameError::Truncated`]; the declared length says
    ///   bytes are missing.
    /// - [`FramingMode::Rfc6587Auto`] under LF framing: an ordinary final message, returned. RFC
    ///   6587 §3.4.2 permits one, and `docs/design/internal-telemetry.md`'s `syslog_in` text pins
    ///   it.
    /// - [`FramingMode::Lines`]: [`FrameError::Truncated`] for a non-whitespace remainder. A line
    ///   protocol's `LF` is its only completeness signal, so half a carbon line is a truncation,
    ///   not a short datapoint. A whitespace-only remainder is dropped uncounted.
    ///
    /// So under every framing a clean FIN and an abrupt RST agree about the same bytes: the
    /// `ReadStep::Eof` arm routes this `Err` through `report_frame_error`, and
    /// [`report_buffered_tail`] reports the RST case identically. The one case with no counter
    /// either way is a drain in progress, whose bytes were counted when the bound was crossed.
    pub fn finish(&mut self) -> Result<Option<Bytes>, FrameError> {
        if self.buf.is_empty() {
            return Ok(None);
        }
        match self.framing {
            None => Ok(None),
            Some(Framing::OctetCounting) => {
                let held = self.buf.len();
                self.buf.clear();
                self.scanned = 0;
                Err(FrameError::Truncated(format!(
                    "the peer closed with {held} octet-counted byte(s) of an incomplete frame \
                     buffered"
                )))
            }
            Some(Framing::LengthPrefixed) => {
                let held = self.buf.len();
                self.buf.clear();
                self.scanned = 0;
                Err(FrameError::Truncated(format!(
                    "the peer closed with {held} byte(s) of an incomplete length-prefixed frame \
                     buffered"
                )))
            }
            Some(Framing::NonTransparent) => {
                // A drain in progress means these bytes are the tail of a line already counted
                // as skipped; delivering them would emit half a datapoint.
                if self.draining {
                    self.buf.clear();
                    self.scanned = 0;
                    return Ok(None);
                }
                let bound = self.max_frame_bytes;
                if self.buf.len() > bound {
                    let held = self.buf.len();
                    self.buf.clear();
                    self.scanned = 0;
                    return Err(match self.oversize_policy() {
                        Oversize::Fatal => FrameError::Oversize(format!(
                            "the peer closed with a {held}-byte unterminated line buffered, over \
                             the {bound}-byte frame ceiling"
                        )),
                        Oversize::DrainToNextLine => FrameError::OversizeSkipped(format!(
                            "the peer closed with a {held}-byte unterminated line buffered, over \
                             the {bound}-byte frame ceiling; skipping it"
                        )),
                    });
                }
                let line = self.buf.split_to(self.buf.len()).freeze();
                self.scanned = 0;
                let line = strip_cr(line);
                if line.is_empty() {
                    return Ok(None);
                }
                match self.mode {
                    // RFC 6587 §3.4.2 can't distinguish "the sender finished and closed" from
                    // "the sender died mid-message", and permits a final message with no
                    // terminator, so this is an ordinary message. (`LengthPrefixed` never gets
                    // here: its framing is never `NonTransparent`.)
                    FramingMode::Rfc6587Auto | FramingMode::LengthPrefixed => Ok(Some(line)),
                    // A line protocol's terminator is its completeness signal, so a remainder
                    // without one is a truncated frame, not a short message; carbon's own
                    // receiver discards it. Emitting it would turn a sender dying mid-line into a
                    // datapoint with a truncated path or timestamp, and would make a clean FIN
                    // disagree with an RST, which `report_buffered_tail` counts `truncated`.
                    FramingMode::Lines { .. } => {
                        // Whitespace only (trailing padding, a bare `CR`, a keepalive): nothing
                        // was lost, so nothing is counted, as `next_frame` does for an empty line.
                        if line.iter().all(|b| b.is_ascii_whitespace()) {
                            return Ok(None);
                        }
                        Err(FrameError::Truncated(format!(
                            "the peer closed with a {}-byte unterminated line buffered; a \
                             line-framed stream's LF is its only completeness signal, so the \
                             remainder is dropped",
                            line.len()
                        )))
                    }
                }
            }
        }
    }

    /// What a line past [`Self::max_frame_bytes`] costs: [`Oversize::Fatal`] everywhere but
    /// [`FramingMode::Lines`], which carries its own.
    fn oversize_policy(&self) -> Oversize {
        match self.mode {
            FramingMode::Lines { oversize } => oversize,
            FramingMode::Rfc6587Auto | FramingMode::LengthPrefixed => Oversize::Fatal,
        }
    }

    /// RFC 6587 §3.4.2 (and [`FramingMode::Lines`]): everything up to the next `LF`, with at most
    /// one preceding `CR` removed.
    fn next_line(&mut self) -> Result<Option<Bytes>, FrameError> {
        // Finish an abandoned line from a previous call first: every byte up to and including the
        // next `LF` still belongs to it.
        if self.draining {
            match self.buf.iter().position(|&b| b == b'\n') {
                Some(at) => {
                    let _skipped = self.buf.split_to(at + 1);
                    self.draining = false;
                    self.scanned = 0;
                }
                None => {
                    self.buf.clear();
                    self.scanned = 0;
                    return Ok(None);
                }
            }
        }

        let bound = self.max_frame_bytes;
        let found = self.buf[self.scanned..].iter().position(|&b| b == b'\n');
        let Some(offset) = found else {
            self.scanned = self.buf.len();
            if self.buf.len() > bound {
                let held = self.buf.len();
                return Err(match self.oversize_policy() {
                    Oversize::Fatal => FrameError::Oversize(format!(
                        "a non-transparent line reached {held} bytes with no LF, over the \
                         {bound}-byte frame ceiling"
                    )),
                    // No resync point has arrived yet: abandon what is buffered and discard
                    // bytes until the `LF` that ends this line.
                    Oversize::DrainToNextLine => {
                        self.buf.clear();
                        self.scanned = 0;
                        self.draining = true;
                        FrameError::OversizeSkipped(format!(
                            "a line reached {held} bytes with no LF, over the {bound}-byte bound; \
                             skipping it and draining to the next newline"
                        ))
                    }
                });
            }
            return Ok(None);
        };
        let idx = self.scanned + offset;
        if idx > bound {
            return Err(match self.oversize_policy() {
                Oversize::Fatal => FrameError::Oversize(format!(
                    "a non-transparent line of {idx} bytes is over the {bound}-byte frame ceiling"
                )),
                // The terminator is already buffered, so drop this line alone; the next line
                // frames normally with no drain state.
                Oversize::DrainToNextLine => {
                    let _skipped = self.buf.split_to(idx + 1);
                    self.scanned = 0;
                    FrameError::OversizeSkipped(format!(
                        "a line of {idx} bytes is over the {bound}-byte bound; skipping it"
                    ))
                }
            });
        }
        let line = self.buf.split_to(idx).freeze();
        let _lf = self.buf.split_to(1);
        self.scanned = 0;
        Ok(Some(strip_cr(line)))
    }

    /// Twisted's `Int32StringReceiver`: a 4-byte **big-endian** payload length, then that many
    /// payload bytes. The prefix is validated and stripped here, so the decoder gets one unframed
    /// payload, which `GraphiteDecoder`'s pickle path expects (framing is the listener's job).
    ///
    /// A declared length past the bound is [`FrameError::Oversize`] and fatal: unlike an
    /// LF-delimited stream there is no resync point to skip to. A short buffer is `Ok(None)`.
    fn next_length_prefixed(&mut self) -> Result<Option<Bytes>, FrameError> {
        if self.buf.len() < LENGTH_PREFIX_BYTES {
            return Ok(None);
        }
        let mut prefix = [0u8; LENGTH_PREFIX_BYTES];
        prefix.copy_from_slice(&self.buf[..LENGTH_PREFIX_BYTES]);
        let payload_len = u32::from_be_bytes(prefix) as usize;
        if payload_len > self.max_frame_bytes {
            return Err(FrameError::Oversize(format!(
                "a length-prefixed frame declared {payload_len} bytes, over the {}-byte frame \
                 ceiling; a length-framed stream has no resync point to skip forward to",
                self.max_frame_bytes
            )));
        }
        if self.buf.len() < LENGTH_PREFIX_BYTES + payload_len {
            return Ok(None); // the rest of this frame hasn't arrived yet
        }
        let _prefix = self.buf.split_to(LENGTH_PREFIX_BYTES);
        let payload = self.buf.split_to(payload_len).freeze();
        self.scanned = 0;
        Ok(Some(payload))
    }

    /// RFC 6587 §3.4.1: `MSG-LEN SP MSG`, where `MSG-LEN = NONZERO-DIGIT *DIGIT`, so a leading
    /// zero (and so a count of zero) is malformed, not a zero-length message.
    ///
    /// At most nine digits: [`MAX_FRAME_BYTES`] needs five, and an explicit ceiling turns a peer
    /// that sends digits forever into a bounded [`FrameError::Malformed`] rather than an unbounded
    /// buffer. The size ceiling is this framer's `max_frame_bytes`, which is [`MAX_FRAME_BYTES`]
    /// on `syslog_in`, the one listener that speaks this framing.
    fn next_octet_counted(&mut self) -> Result<Option<Bytes>, FrameError> {
        const MAX_COUNT_DIGITS: usize = 9;

        let mut digits = 0usize;
        loop {
            match self.buf.get(digits) {
                // Not enough bytes yet. `digits <= MAX_COUNT_DIGITS` here, so this waits for at
                // most one more byte before the checks fire.
                None => return Ok(None),
                Some(&b) if b.is_ascii_digit() => {
                    if digits == MAX_COUNT_DIGITS {
                        return Err(FrameError::Malformed(format!(
                            "an octet count of more than {MAX_COUNT_DIGITS} digits"
                        )));
                    }
                    digits += 1;
                }
                Some(&b' ') => break,
                Some(&b) => {
                    return Err(FrameError::Malformed(format!(
                        "expected a digit or SP in an octet count, got byte 0x{b:02x} at offset \
                         {digits}"
                    )))
                }
            }
        }

        // Unreachable through `next_frame` (the framing latches `OctetCounting` only on a leading
        // digit), but reachable directly from a test.
        if digits == 0 {
            return Err(FrameError::Malformed("an octet count with no digits".to_string()));
        }
        // RFC 6587 §3.4.1's `NONZERO-DIGIT` first character. `0` and `012` are both rejected, the
        // second before it can be read as 12: a sender that pads its counts is not speaking this
        // framing, and guessing would mis-frame the rest of the stream.
        if self.buf[0] == b'0' {
            return Err(FrameError::Malformed(if digits == 1 {
                "an octet count of zero".to_string()
            } else {
                "an octet count with a leading zero (RFC 6587 §3.4.1: MSG-LEN = NONZERO-DIGIT \
                 *DIGIT)"
                    .to_string()
            }));
        }

        let len: usize = std::str::from_utf8(&self.buf[..digits])
            .expect("every byte was checked to be an ASCII digit")
            .parse()
            .expect("at most nine ASCII digits always fit a usize");
        if len > self.max_frame_bytes {
            return Err(FrameError::Oversize(format!(
                "an octet count of {len} is over the {}-byte frame ceiling",
                self.max_frame_bytes
            )));
        }

        let header = digits + 1; // the digits plus the single SP
        if self.buf.len() < header + len {
            return Ok(None);
        }
        let _header = self.buf.split_to(header);
        let msg = self.buf.split_to(len).freeze();
        self.scanned = 0;
        Ok(Some(msg))
    }
}

/// Removes one trailing `CR`, so `CRLF`- and `LF`-terminated senders hand the decoder the same
/// message. Only one: a message ending in `CR CR` keeps the first.
fn strip_cr(line: Bytes) -> Bytes {
    match line.last() {
        Some(b'\r') => line.slice(..line.len() - 1),
        _ => line,
    }
}

// ---- the kernel's accept queue -----------------------------------------------------------------

/// How often [`AcceptQueueSampler::accept`] re-reads the accept queue while waiting for a
/// connection. The same one-second cadence [`crate::udp`]'s receive-buffer sampler uses: frequent
/// enough to be a usable gauge, cheap enough not to need a config knob.
///
/// The cost is one `getsockopt` and three gauge writes per tick **plus one per accepted
/// connection**, since the sample runs at the top of every loop turn and the loop turns on every
/// accept as well as every tick. That is small next to the connection setup it accompanies.
const ACCEPT_QUEUE_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// Gauges the kernel's accept queue for one listening socket: how many completed connections are
/// waiting for an `accept()`, against the backlog ceiling at which the kernel starts refusing them.
///
/// **Why an accept loop cannot see this for itself.** Nothing observable from inside the loop
/// distinguishes "no traffic" from "so far behind that the kernel is dropping SYNs." Only the
/// queue depth does, and it lives in the kernel ([`logit_pipeline::sockstat::listen_queue`]).
///
/// **Sampled before each accept *and* on a fixed interval.** Before each accept, because the depth
/// just before this loop takes one off the queue is what a backlog is made of. On an interval as
/// well, because the accept-time sample fires only when a connection is *taken*, which is what
/// stops happening when the loop is starved of runtime or stuck. Every caller gets both by calling
/// [`Self::accept`] instead of `listener.accept()`.
///
/// Like `crate::udp`'s receive-buffer sampler, this disables itself for good after one failed read
/// and says so once: `TCP_INFO`'s listener aliasing either works on a socket or never will.
pub(crate) struct AcceptQueueSampler {
    /// How the accept queue is read, as a function of the listener [`Self::accept`] was handed.
    ///
    /// **Not a descriptor captured at construction.** A stored `fd` would make the socket
    /// *gauged* and the socket *accepted on* independent: `sampler.accept(&other_listener)` would
    /// compile and gauge one socket while draining another, indistinguishably from correct
    /// output. Taking the descriptor from the `listener` argument at each sample makes them the
    /// same socket by construction. A `BorrowedFd<'_>` field would fix the lifetime but not the
    /// identity, since two listeners can both outlive a sampler.
    ///
    /// A function pointer so a test can substitute a reader that reports nothing or counts its
    /// calls: the disabled path is what every non-Linux build runs and no Linux CI run would
    /// otherwise exercise, and the call count is the only cheap observable for the cadence
    /// [`Self::accept_every`] keeps.
    read_queue: QueueReader,
    telemetry: Telemetry,
    diag: Diagnostics,
    enabled: bool,
    /// The one timer this sampler arms, kept across loop turns *and* across calls.
    ///
    /// `None` until the first enabled [`Self::accept`], because a disabled sampler must arm no
    /// timer, and dropped again when the sampler disables itself. Boxed and pinned so it can live
    /// in a struct and still be polled as a `Pin<&mut Sleep>`.
    ///
    /// **Why one `Sleep` rather than a fresh `sleep(interval)` per turn.** (1) Cost: in tokio
    /// 1.53.1 a `Sleep` registers its `TimerEntry` lazily on first poll (`Sleep::poll_elapsed` ->
    /// `TimerEntry::init` -> `reregister`, which takes the timer driver lock) and cancels it on
    /// drop (`PinnedDrop for TimerEntry` -> `cancel` -> `clear_entry`, which takes that lock
    /// again unconditionally; the `might_be_registered()` check inside only gates the wheel
    /// removal). With `biased;` polling the timer arm first, a fresh `Sleep` per turn pays both
    /// per accepted connection on every stream listener in the process; re-polling a registered
    /// `Sleep` is one `Acquire` load (`StateCell::read_state`). (2) Correctness: a fresh
    /// `sleep(interval)` re-anchors to *now* every turn, so under a steady accept rate faster than
    /// one per interval the tick never fires, starving the sample that exists so a busy listener
    /// still reports. One `Sleep` reset only when it fires keeps the cadence.
    tick: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
}

/// [`AcceptQueueSampler::read_queue`]'s type.
type QueueReader = fn(&TokioTcpListener) -> Result<(u32, u32), sockstat::Unavailable>;

/// The production [`QueueReader`]: this listener's descriptor, into
/// [`logit_pipeline::sockstat::listen_queue`].
fn read_listen_queue(listener: &TokioTcpListener) -> Result<(u32, u32), sockstat::Unavailable> {
    sockstat::fd_of(listener)
        .ok_or(sockstat::Unavailable::NoDescriptor)
        .and_then(sockstat::listen_queue)
}

impl AcceptQueueSampler {
    /// Takes no listener: the socket this gauges is whichever one is handed to [`Self::accept`]
    /// (see [`Self::read_queue`]).
    pub(crate) fn new(telemetry: Telemetry, diag: Diagnostics) -> Self {
        Self { read_queue: read_listen_queue, telemetry, diag, enabled: true, tick: None }
    }

    /// Accepts the next connection on `listener`, gauging the accept queue before the attempt and
    /// once per [`ACCEPT_QUEUE_SAMPLE_INTERVAL`] for as long as the wait lasts.
    pub(crate) async fn accept(
        &mut self,
        listener: &TokioTcpListener,
    ) -> std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)> {
        self.accept_every(listener, ACCEPT_QUEUE_SAMPLE_INTERVAL).await
    }

    /// [`Self::accept`] over an arbitrary interval, split out (as [`crate::udp::sample_while`] is)
    /// so a test can drive the interval tick without sleeping a second.
    ///
    /// **Once the sampler has disabled itself, no timer is armed** and this is `listener.accept()`.
    /// Otherwise every idle listener on a non-Linux build (or a kernel without the counters)
    /// would wake once a second, forever, to call a function that returns immediately.
    ///
    /// **Cancellation-safe**, which this driver's and `logit_in`'s accept loops depend on:
    /// `TcpListener::accept` takes nothing off the queue unless it returns a connection, so
    /// dropping this future when a caller's `select!` loses it to `shutdown` loses at most one
    /// sample, never a connection. The timer is a field of the sampler rather than of this future,
    /// so a cancelled `accept` does not restart the interval either.
    async fn accept_every(
        &mut self,
        listener: &TokioTcpListener,
        interval: Duration,
    ) -> std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)> {
        loop {
            self.sample_once(listener);
            if !self.enabled {
                // Latched off mid-run: give the timer entry back rather than leave it in the
                // wheel for the life of the listener.
                self.tick = None;
                return listener.accept().await;
            }
            // Timer arm first, matching `crate::udp::sample_while`, but for uniformity rather than
            // need. There the work arm is one long-lived `read_loop` future, so an arm behind it
            // is silenced by tokio's coop budget for a whole overload. Here the loop returns to
            // the synchronous `sample_once` on every accepted connection, so a backed-up queue is
            // sampled per connection anyway, and an idle `accept()` parks with budget to spare,
            // so the timer fires normally. Either ordering is correct for this loop; one rule for
            // both samplers is one thing for an edit to preserve. The cost is a due tick taken
            // ahead of a ready connection: one extra loop turn per tick, never a lost connection.
            let tick = self.tick.get_or_insert_with(|| Box::pin(tokio::time::sleep(interval)));
            tokio::select! {
                biased;
                () = tick.as_mut() => {
                    // Re-armed from *now* rather than from the old deadline: the cadence promised
                    // is a sample at least every `interval` while waiting, not a fixed schedule to
                    // catch up to after a stall.
                    let next = tokio::time::Instant::now() + interval;
                    tick.as_mut().reset(next);
                }
                accepted = listener.accept() => return accepted,
            }
        }
    }

    /// One `getsockopt`, and the three gauges it feeds.
    ///
    /// `logit.input.accept_queue.limit` is re-emitted every sample although the backlog does not
    /// change after `listen(2)`, as `crate::udp`'s sampler re-emits `receive_buffer.bytes`:
    /// `ComponentBuffer::drain` (`logit_core::telemetry`) `mem::take`s its point map, so a value
    /// written once would appear in one `internal` drain window and vanish. It is its own gauge
    /// because an operator deciding whether to raise `net.core.somaxconn` (or the backlog) needs
    /// the ceiling, and backing it out of `depth / utilization` is undefined at an idle listener's
    /// depth of 0.
    ///
    /// `logit.input.accept_queue.utilization` is skipped when the kernel reports a backlog of 0, so
    /// the gauge's presence is evidence that a ceiling was read. It is **not** clamped to 1.0 and
    /// must not be: `sk_acceptq_is_full` is strictly greater-than, so a `listen(N)` socket settles
    /// at a depth of `N + 1` and a utilization of `(N + 1) / N` when the kernel starts refusing.
    /// `sockstat::listen_queue`'s doc has the kernel citation.
    fn sample_once(&mut self, listener: &TokioTcpListener) {
        if !self.enabled {
            return;
        }
        let (depth, backlog) = match (self.read_queue)(listener) {
            Ok(queue) => queue,
            Err(err) => {
                self.enabled = false;
                // The platform hint only where it holds: `EBADF` or a listener that has left
                // `LISTEN` are about this socket, not about Linux.
                let hint = if err.is_unsupported_option() {
                    " (TCP_INFO's listener fields are Linux-only)"
                } else {
                    ""
                };
                let message = format_args!(
                    "the kernel's accept-queue depth is not available for this listener: \
                     {err}{hint}; logit.input.accept_queue.depth, .limit and .utilization will \
                     not be reported"
                );
                // See `crate::udp::ReceiveBufferSampler::sample_once` for why a non-Linux build
                // gets `debug` and everything else gets `warn`.
                if matches!(
                    err,
                    sockstat::Unavailable::NotLinux | sockstat::Unavailable::NoDescriptor
                ) {
                    self.diag.debug(message);
                } else {
                    self.diag.warn(message);
                }
                return;
            }
        };
        self.telemetry.gauge("logit.input.accept_queue.depth", f64::from(depth), &[]);
        self.telemetry.gauge("logit.input.accept_queue.limit", f64::from(backlog), &[]);
        if backlog > 0 {
            self.telemetry.gauge(
                "logit.input.accept_queue.utilization",
                f64::from(depth) / f64::from(backlog),
                &[],
            );
        }
    }
}

// ---- the listener ----------------------------------------------------------------------------

/// [`TcpListener`]'s runtime knobs: [`crate::udp::UdpListenerConfig`] minus every queue field
/// (this module's "No receive queue" doc section), with the same defaults for the other four.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TcpListenerConfig {
    /// Events to accumulate **per connection** before one `Fanout::send`; `1` means one send per
    /// frame. The listener's worst-case in-flight event count is this times the number of live
    /// connections (this module's "Batching is per connection" doc section).
    pub batch_max_events: usize,
    /// The same bound by estimated heap bytes, also **per connection**.
    pub batch_max_bytes: u64,
    /// `Duration::ZERO` disables the flush timer; the bounds are then the only trigger.
    pub batch_flush_interval: Duration,
    /// How long [`TcpListener::run_until_shutdown`] keeps draining after shutdown fires before
    /// [`logit_pipeline::runtime::run_input`]'s grace backstop cancels it by drop.
    pub shutdown_grace: Duration,
}

/// The same numbers as [`crate::udp::UdpListenerConfig::default`]'s corresponding fields
/// (`docs/adr/decoupled-listener-io.md`): a TCP listener has no reason to batch differently.
impl Default for TcpListenerConfig {
    fn default() -> Self {
        Self {
            batch_max_events: 1_000,
            batch_max_bytes: 1024 * 1024,
            batch_flush_interval: Duration::from_millis(100),
            shutdown_grace: Duration::from_secs(5),
        }
    }
}

/// A TCP (optionally TLS) listener that turns each connection's frame stream into batches of
/// decoded events. This module's doc has the accept, cap, handshake, framing, and batching
/// contracts.
pub struct TcpListener<D: Decoder + Clone + Send + 'static> {
    bind: String,
    decoder: D,
    config: TcpListenerConfig,
    diag: Diagnostics,
    telemetry: Telemetry,
    /// How every connection's messages are delimited, and (below) the bound on one of them:
    /// [`FramingMode::Rfc6587Auto`] and [`MAX_FRAME_BYTES`] unless [`Self::with_framing`] is
    /// called.
    framing: FramingMode,
    max_frame_bytes: usize,
    tls: Option<Arc<rustls::ServerConfig>>,
    /// Set by [`Input::bind`], taken by [`Input::run_until_shutdown`]: the bind pre-pass `otlp_in`
    /// and `logit_in` also use, so `logit run` fails startup on a bind error before anything is
    /// spawned and a test can learn the real address.
    listener: Option<TokioTcpListener>,
    max_connections: usize,
    handshake_timeout: Duration,
    /// `None` (the default) means no idle timeout. See this module's "Idle timeout" doc section.
    idle_timeout: Option<Duration>,
}

impl<D: Decoder + Clone + Send + 'static> TcpListener<D> {
    pub fn new(bind: impl Into<String>, decoder: D, config: TcpListenerConfig) -> Self {
        Self {
            bind: bind.into(),
            decoder,
            config,
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
            framing: FramingMode::Rfc6587Auto,
            max_frame_bytes: MAX_FRAME_BYTES,
            tls: None,
            listener: None,
            max_connections: MAX_CONCURRENT_CONNECTIONS,
            handshake_timeout: HANDSHAKE_TIMEOUT,
            idle_timeout: None,
        }
    }

    /// The address bound, once [`Input::bind`] has run; lets a test learn the OS-assigned port
    /// without a bind-drop-rebind race.
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        self.listener.as_ref().and_then(|l| l.local_addr().ok())
    }

    /// Sets *this listener's own* diagnostics: the `connection_error`, `bad_frame` and
    /// `framing_error` keys. Does **not** reach the decoder's diagnostics
    /// ([`crate::udp::UdpListener::with_diagnostics`] explains); use [`Self::map_decoder`] for
    /// that.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Applies `f` to the wrapped decoder, so a caller that knows the concrete type can chain its
    /// consuming builder methods, as [`crate::udp::UdpListener::map_decoder`] does.
    pub fn map_decoder(mut self, f: impl FnOnce(D) -> D) -> Self {
        self.decoder = f(self.decoder);
        self
    }

    /// Overrides the batching/shutdown-grace knobs, which a `receive:` config block sets.
    pub fn with_config(mut self, config: TcpListenerConfig) -> Self {
        self.config = config;
        self
    }

    /// The configured batching/shutdown-grace knobs, for test introspection.
    pub fn config(&self) -> TcpListenerConfig {
        self.config
    }

    /// Turns on TLS termination (`tls:` in config), with no ALPN: as with `logit_in`, the
    /// protocol isn't HTTP-shaped, so there's nothing to negotiate. Every path in `settings`
    /// resolves against `base_dir` (the config file's directory).
    pub fn with_tls(
        mut self,
        settings: &TlsServerSettings,
        base_dir: &Path,
    ) -> anyhow::Result<Self> {
        self.tls = Some(Arc::new(crate::tls::build_server_config(settings, base_dir, &[])?));
        Ok(self)
    }

    /// This listener's own diagnostics, test-only: a wrapper's `with_diagnostics` has to set both
    /// this and `Self::decoder`'s, and only an accessor on each can prove it did
    /// (`crate::syslog`'s regression test).
    #[cfg(test)]
    pub(crate) fn diag(&self) -> &Diagnostics {
        &self.diag
    }

    /// The wrapped decoder, test-only; the counterpart of `Self::diag`.
    #[cfg(test)]
    pub(crate) fn decoder(&self) -> &D {
        &self.decoder
    }

    /// How this listener's connections are framed, and the largest single frame any of them will
    /// assemble. [`FramingMode::Rfc6587Auto`] with [`MAX_FRAME_BYTES`] (what `syslog_in` wants)
    /// when never called.
    ///
    /// A builder rather than a [`TcpListenerConfig`] field: that struct is the image of the
    /// operator's `receive:` block, and framing is a property of the protocol. See
    /// [`FramingMode`] for why it is explicit rather than sniffed.
    pub fn with_framing(mut self, mode: FramingMode, max_frame_bytes: usize) -> Self {
        self.set_framing(mode, max_frame_bytes);
        self
    }

    /// [`Self::with_framing`] on an already-built listener, for a wrapper that defers the decision
    /// until `bind()`: `graphite_in` applies `max_line_bytes`/`max_frame_bytes` there so its own
    /// builder methods can be called in any order.
    pub(crate) fn set_framing(&mut self, mode: FramingMode, max_frame_bytes: usize) {
        self.framing = mode;
        self.max_frame_bytes = max_frame_bytes;
    }

    /// Test-only override of [`MAX_CONCURRENT_CONNECTIONS`], so the cap is reachable with two
    /// connections rather than 1025. `pub(crate)` so a wrapper's test module can use it too.
    #[cfg(test)]
    pub(crate) fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Overrides [`HANDSHAKE_TIMEOUT`] for both pre-message budgets (the TLS accept and the
    /// first-byte wait): the `handshake_timeout:` field of `syslog_in`/`graphite_in`/`statsd_in`,
    /// through each wrapper's `with_handshake_timeout`. Graph rule 45 rejects `0s` before it can
    /// reach here.
    pub fn with_handshake_timeout(mut self, handshake_timeout: Duration) -> Self {
        self.handshake_timeout = handshake_timeout;
        self
    }

    /// Bounds how long a connection may stay quiet once past the first-byte phase: the
    /// `idle_timeout:` field of `syslog_in`/`graphite_in`/`statsd_in`. `None` (the default) means
    /// no idle timeout. See this module's "Idle timeout" doc section. Graph rule 53 rejects
    /// `Some(0s)` (and any value on a UDP listener) before it can reach here.
    ///
    /// Takes the `Option` so every wrapper and `logit-cli`'s `build_spec` can pass the config
    /// value straight through.
    pub fn with_idle_timeout(mut self, idle_timeout: Option<Duration>) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }
}

#[async_trait::async_trait]
impl<D: Decoder + Clone + Send + 'static> Input for TcpListener<D> {
    async fn bind(&mut self) -> anyhow::Result<()> {
        if self.listener.is_some() {
            return Ok(()); // idempotent, per `Input::bind`'s contract
        }
        let listener = TokioTcpListener::bind(&self.bind).await?;
        self.diag.info("bound", format_args!("listening on {}", self.bind));
        self.listener = Some(listener);
        Ok(())
    }

    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        // Never exercised in production: `run_input` always calls `run_until_shutdown`. The trait
        // requires it.
        let (_tx, rx) = watch::channel(false);
        self.run_until_shutdown(sink, rx).await
    }

    async fn run_until_shutdown(
        &mut self,
        sink: Fanout,
        mut shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        self.bind().await?;
        let listener = self.listener.take().expect("bind() leaves a listener behind");
        let connection_limit = Arc::new(tokio::sync::Semaphore::new(self.max_connections));
        // Built once: `TlsAcceptor::from` wraps the `Arc<ServerConfig>`, so cloning it per
        // connection is an `Arc` clone, not a config rebuild.
        let tls_acceptor = self.tls.clone().map(TlsAcceptor::from);
        let live_connections = Arc::new(AtomicI64::new(0));
        let handshake_timeout = self.handshake_timeout;
        let idle_timeout = self.idle_timeout;
        let config = self.config;
        let framing = self.framing;
        let max_frame_bytes = self.max_frame_bytes;
        // All three diagnostic keys (`connection_error`, `framing_error`, `bad_frame`) throttle
        // listener-wide through the per-connection `Diagnostics` clone below: a clone shares its
        // original's counts (`logit_core::Diagnostics`' type doc).
        //
        // `accept_queue.accept(&listener)` has `listener.accept()`'s cancellation safety against
        // the `shutdown` arm, plus the kernel accept-queue gauges (`AcceptQueueSampler`).
        let mut accept_queue = AcceptQueueSampler::new(self.telemetry.clone(), self.diag.clone());
        loop {
            let (stream, _peer) = tokio::select! {
                accepted = accept_queue.accept(&listener) => accepted?,
                _ = shutdown.wait_for(|&due| due) => return Ok(()),
            };

            // `try_acquire_owned`, not `acquire_owned`: at capacity the connection is closed
            // immediately rather than queued behind a permit that may never come, and before any
            // TLS accept (this module's "Connection limit" doc section says why that differs from
            // `logit_in`).
            let Ok(permit) = connection_limit.clone().try_acquire_owned() else {
                self.telemetry.count(
                    "logit.input.connections.rejected",
                    1.0,
                    &[("reason", "limit")],
                );
                drop(stream);
                continue;
            };

            // Every connection task holds a `Fanout` clone, so the shutdown cascade
            // (`docs/adr/service-lifecycle-and-output-retry.md`) completes only once every one has
            // dropped, which `serve_connection`'s shutdown race guarantees.
            let sink = sink.clone();
            let mut diag = self.diag.clone();
            let telemetry = self.telemetry.clone();
            let tls_acceptor = tls_acceptor.clone();
            let conn_shutdown = shutdown.clone();
            let live_connections = Arc::clone(&live_connections);
            let decoder = self.decoder.clone();

            tokio::spawn(async move {
                // Held for as long as this task runs: a TLS accept that fails or times out gives
                // the permit back here.
                let _permit = permit;
                let framer = Framer::new(framing, max_frame_bytes);
                // Published from the read-modify-write's return value, not a separate `load`:
                // `Telemetry::gauge` is last-write-wins per key, so two tasks interleaving an add
                // and a load would publish the stale value until the next transition.
                let live = live_connections.fetch_add(1, Ordering::Relaxed) + 1;
                telemetry.gauge("logit.input.connections", live as f64, &[]);

                let result = match tls_acceptor {
                    Some(acceptor) => {
                        match tokio::time::timeout(handshake_timeout, acceptor.accept(stream)).await
                        {
                            Ok(Ok(tls_stream)) => {
                                serve_connection(
                                    tls_stream,
                                    decoder,
                                    framer,
                                    config,
                                    handshake_timeout,
                                    idle_timeout,
                                    sink,
                                    telemetry.clone(),
                                    &mut diag,
                                    conn_shutdown,
                                )
                                .await
                            }
                            Ok(Err(err)) => Err(anyhow::anyhow!("TLS handshake failed: {err}")),
                            Err(_elapsed) => Err(anyhow::anyhow!(
                                "TLS handshake did not complete within {handshake_timeout:?}"
                            )),
                        }
                    }
                    // No TLS to bound, but `serve_connection`'s first-byte deadline still applies
                    // (this module's "Pre-handshake timeout" doc section).
                    None => {
                        serve_connection(
                            stream,
                            decoder,
                            framer,
                            config,
                            handshake_timeout,
                            idle_timeout,
                            sink,
                            telemetry.clone(),
                            &mut diag,
                            conn_shutdown,
                        )
                        .await
                    }
                };

                let live = live_connections.fetch_sub(1, Ordering::Relaxed) - 1;
                telemetry.gauge("logit.input.connections", live as f64, &[]);

                // One connection's error (a peer vanishing mid-frame, a TLS accept that failed or
                // timed out) is never fatal to the listener or its siblings; only
                // `TcpListener::accept` failing in the loop above is.
                if let Err(err) = result {
                    diag.warn_throttled("connection_error", err);
                }
            });
        }
    }
}

/// What one `read`-versus-`shutdown` race produced.
enum ReadStep {
    /// Bytes landed in the read buffer.
    Bytes,
    /// The peer closed cleanly.
    Eof,
    /// Shutdown fired before anything else did.
    Shutdown,
    Failed(std::io::Error),
}

/// One read step, raced against `shutdown`.
///
/// `AsyncReadExt::read_buf` is cancellation-safe (no bytes are consumed if another `select!` arm
/// wins), which lets both this race and [`serve_connection`]'s deadline timeout drop it mid-await
/// without losing stream bytes.
///
/// `shutdown.changed()`, not `wait_for`: `wait_for`'s `Ref` guard makes the combined future
/// `!Send`, and `tokio::spawn`ing this connection's task requires `Send`. The caller's explicit
/// `*shutdown.borrow()` check covers what `changed()` alone cannot: shutdown having fired before
/// this loop iteration began. `crate::logit`'s `serve_connection` follows the same discipline.
async fn read_step<S: AsyncRead + Unpin + Send>(
    stream: &mut S,
    buf: &mut BytesMut,
    shutdown: &mut watch::Receiver<bool>,
) -> ReadStep {
    tokio::select! {
        result = stream.read_buf(buf) => match result {
            Ok(0) => ReadStep::Eof,
            Ok(_) => ReadStep::Bytes,
            Err(err) => ReadStep::Failed(err),
        },
        _ = shutdown.changed() => ReadStep::Shutdown,
    }
}

/// Serves one accepted (and, under TLS, handshaken) connection to completion. Generic over the IO
/// type so plaintext (`TcpStream`) and TLS (`tokio_rustls::server::TlsStream<TcpStream>`) share
/// every line.
///
/// Owns its own [`Framer`], [`BatchAccumulator`] and decoder clone; nothing is shared with a
/// sibling connection. Flushes on the accumulator's bounds, on `batch_flush_interval`, on
/// shutdown, and on close (clean or otherwise).
///
/// `diag` is the accept loop's per-connection [`Diagnostics`] clone, borrowed so it is still
/// there for the `connection_error` report on whatever this returns. `framing_error` and
/// `bad_frame` are reported on it and still throttle listener-wide.
///
/// `handshake_timeout` bounds the wait for the *first* byte, TLS or not (this module's
/// "Pre-handshake timeout" doc section). `idle_timeout`, when `Some`, bounds every gap after it
/// (the "Idle timeout" section). The two share one next-byte deadline, since a connection is in
/// one phase at a time.
#[allow(clippy::too_many_arguments)] // one connection's whole context; a params struct would only move it
async fn serve_connection<S, D>(
    mut stream: S,
    mut decoder: D,
    mut framer: Framer,
    config: TcpListenerConfig,
    handshake_timeout: Duration,
    idle_timeout: Option<Duration>,
    sink: Fanout,
    telemetry: Telemetry,
    diag: &mut Diagnostics,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    D: Decoder + Send,
{
    // The idle clock's origin, advanced only when bytes are read from the peer and when this
    // connection's interval flush emits (this module's "Idle timeout" doc section).
    //
    // `first_byte_deadline` is absolute, computed once, rather than a budget re-armed per read:
    // the read is re-entered on every `batch_flush_interval` tick (the `Err(_elapsed) => continue`
    // arm), so a per-read budget would be reset by each tick and never fire.
    let mut last_progress = tokio::time::Instant::now();
    let first_byte_deadline = last_progress + handshake_timeout;
    // Cleared, not replaced, between reads so its capacity survives.
    let mut read_buf = BytesMut::with_capacity(READ_BUFFER_BYTES);
    let mut accumulator = BatchAccumulator::new(config.batch_max_events, config.batch_max_bytes);
    // Cleared, not taken, between `decode_into` calls: `BatchAccumulator::absorb`'s doc says why
    // `std::mem::take` would undo the allocation win.
    let mut scratch: Vec<Event> = Vec::new();
    let has_interval = !config.batch_flush_interval.is_zero();
    let mut next_flush =
        has_interval.then(|| tokio::time::Instant::now() + config.batch_flush_interval);

    loop {
        // The interval trigger, using `BatchAccumulator::next_deadline`'s cadence math as
        // `crate::udp::decode_loop` does.
        if let Some(deadline) = next_flush {
            let now_instant = tokio::time::Instant::now();
            if deadline <= now_instant {
                if let Some(batch) = accumulator.take() {
                    emit(&sink, &telemetry, batch, FlushReason::Interval).await;
                    // Stamped after the send returns, so time blocked on a full downstream is
                    // not counted against the peer. A tick with nothing to emit never gets here:
                    // this process's own timer must not keep a silent connection alive.
                    last_progress = tokio::time::Instant::now();
                }
                next_flush = Some(BatchAccumulator::next_deadline(
                    deadline,
                    now_instant,
                    config.batch_flush_interval,
                ));
            }
        }

        // Checked explicitly: `read_step`'s `changed()` fires only on a transition this receiver
        // has not observed, so it would miss shutdown already being true. The `Ref` temporary
        // drops at the end of this statement, before any `.await`.
        if *shutdown.borrow() {
            report_buffered_tail(&framer, &telemetry, diag);
            if let Some(batch) = accumulator.take() {
                emit(&sink, &telemetry, batch, FlushReason::Shutdown).await;
            }
            return Ok(());
        }

        read_buf.clear();
        // Two deadlines can bound this read: the flush tick (recurring, benign) and the next-byte
        // deadline (ends the connection). Race whichever comes first, then decide which it was;
        // `timeout_at`, not `timeout`, so the next-byte deadline stays absolute across flush
        // ticks.
        //
        // The next-byte deadline is the first-byte deadline before the first byte and
        // `last_progress + idle_timeout` after it. With no `idle_timeout` it is `far_future`, so
        // there is no "is there an idle timeout" branch, only a deadline that may never arrive.
        // `checked_add` because an absurd (but legal) `idle_timeout` can overflow, and rule 53
        // caps nothing above `0s`. `first_byte_seen`, never `framing().is_none()` (this module's
        // "Pre-handshake timeout" doc section).
        let awaiting_first_byte = !framer.first_byte_seen();
        let next_byte_deadline = if awaiting_first_byte {
            first_byte_deadline
        } else {
            idle_timeout.and_then(|idle| last_progress.checked_add(idle)).unwrap_or_else(far_future)
        };
        let read_deadline =
            next_flush.map_or(next_byte_deadline, |flush| flush.min(next_byte_deadline));
        let step = match tokio::time::timeout_at(
            read_deadline,
            read_step(&mut stream, &mut read_buf, &mut shutdown),
        )
        .await
        {
            Ok(step) => step,
            Err(_elapsed) => {
                // Checked against the clock rather than inferred from which deadline was smaller,
                // so a flush tick landing on the same instant can't mask it.
                if tokio::time::Instant::now() >= next_byte_deadline {
                    // No first byte: a fault, returned as `Err` so it reaches the accept loop's
                    // `connection_error` diagnostic.
                    if awaiting_first_byte {
                        return Err(anyhow::anyhow!(
                            "the peer sent no bytes within {handshake_timeout:?}"
                        ));
                    }
                    // Idle: policy, not a fault, so `Ok(())` and no `connection_error` (this
                    // module's "Idle timeout" doc section). A buffered partial frame is counted
                    // `truncated`, as on the shutdown and RST paths.
                    report_buffered_tail(&framer, &telemetry, diag);
                    if let Some(batch) = accumulator.take() {
                        emit(&sink, &telemetry, batch, FlushReason::Closed).await;
                    }
                    telemetry.count("logit.input.connections.closed", 1.0, &[("reason", "idle")]);
                    return Ok(());
                }
                // The flush deadline won: back to the interval trigger.
                continue;
            }
        };

        match step {
            ReadStep::Bytes => {}
            ReadStep::Shutdown => {
                report_buffered_tail(&framer, &telemetry, diag);
                if let Some(batch) = accumulator.take() {
                    emit(&sink, &telemetry, batch, FlushReason::Shutdown).await;
                }
                return Ok(());
            }
            ReadStep::Eof => {
                // `Framer::finish` decides what a terminator-less remainder is.
                match framer.finish() {
                    Ok(Some(frame)) => {
                        absorb_frame(
                            frame,
                            now_nanos(),
                            &mut decoder,
                            &mut scratch,
                            &mut accumulator,
                            &sink,
                            &telemetry,
                            diag,
                        )
                        .await;
                    }
                    Ok(None) => {}
                    Err(err) => {
                        report_frame_error(&err, &telemetry, diag);
                    }
                }
                if let Some(batch) = accumulator.take() {
                    emit(&sink, &telemetry, batch, FlushReason::Closed).await;
                }
                return Ok(());
            }
            ReadStep::Failed(err) => {
                // The connection broke, but what was already decoded is good: deliver it before
                // surfacing the error as `connection_error`.
                report_buffered_tail(&framer, &telemetry, diag);
                if let Some(batch) = accumulator.take() {
                    emit(&sink, &telemetry, batch, FlushReason::Closed).await;
                }
                return Err(err.into());
            }
        }

        // `received_at` is when the bytes came off the socket, not when the frame they complete is
        // decoded (`logit_proto::Decoder::decode_into`'s contract).
        let received_at = now_nanos();
        framer.push(&read_buf);
        loop {
            match framer.next_frame() {
                Ok(Some(frame)) => {
                    absorb_frame(
                        frame,
                        received_at,
                        &mut decoder,
                        &mut scratch,
                        &mut accumulator,
                        &sink,
                        &telemetry,
                        diag,
                    )
                    .await;
                }
                Ok(None) => break,
                Err(err) => {
                    // Its own diagnostic key, not `connection_error`: the cause is the peer's
                    // framing, not I/O.
                    report_frame_error(&err, &telemetry, diag);
                    // A non-fatal error (`FrameError::OversizeSkipped`) has already resynchronized
                    // the framer: it dropped one line and either consumed its terminator or
                    // latched the drain state that will.
                    if !err.is_fatal() {
                        continue;
                    }
                    // Nothing can resynchronize past a fatal error (see `FrameError`).
                    if let Some(batch) = accumulator.take() {
                        emit(&sink, &telemetry, batch, FlushReason::Closed).await;
                    }
                    return Ok(());
                }
            }
        }

        // One stamp covers both halves of progress: the read, and `absorb_frame`'s `emit` of a
        // full batch having returned. After the loop, not before, so time blocked in that `emit`
        // is not charged to the peer (this module's "Idle timeout" doc section).
        last_progress = tokio::time::Instant::now();
    }
}

/// One complete frame: counted, decoded, and accumulated. A decode error is diagnosed and the
/// frame dropped; the connection stays open, as `crate::udp::decode_loop` does for one bad
/// datagram.
#[allow(clippy::too_many_arguments)] // eight threaded-through borrows; a params struct would only move them
async fn absorb_frame<D: Decoder + Send>(
    frame: Bytes,
    received_at: i64,
    decoder: &mut D,
    scratch: &mut Vec<Event>,
    accumulator: &mut BatchAccumulator,
    sink: &Fanout,
    telemetry: &Telemetry,
    diag: &mut Diagnostics,
) {
    telemetry.count("logit.input.frames", 1.0, &[]);
    telemetry.count("logit.input.frame.bytes", frame.len() as f64, &[]);
    scratch.clear();
    match decoder.decode_into(frame, received_at, scratch) {
        Ok((resource, scope)) => {
            // `scope` is threaded through rather than hardcoded `None`, as `crate::udp` does.
            if let Some((batch, reason)) = accumulator.absorb(resource, scope, scratch) {
                emit(sink, telemetry, batch, reason).await;
            }
        }
        Err(err) => {
            diag.warn_throttled("bad_frame", err);
        }
    }
}

/// Counts and diagnoses a framing failure, on its own diagnostic key: "my sender's frames are
/// rejected" is a different triage from "a peer's socket broke". Returns whether the diagnostic
/// reported (was not throttled), so a test can assert the listener-wide cadence.
fn report_frame_error(err: &FrameError, telemetry: &Telemetry, diag: &mut Diagnostics) -> bool {
    telemetry.count("logit.input.frames.dropped", 1.0, &[("reason", err.reason())]);
    diag.warn_throttled("framing_error", err)
}

/// Reports a partial frame still held by the [`Framer`] when a connection ends *without* a clean
/// EOF: a peer RST mid-message, a shutdown or idle close before the sender finished one.
///
/// Dropping those bytes is correct, since no complete message was sent, but it is counted as
/// `logit.input.frames.dropped{reason="truncated"}` so the loss is visible. This agrees with what
/// the same bytes followed by a FIN would do ([`Framer::finish`]) under octet counting, a length
/// prefix, and [`FramingMode::Lines`]; only [`FramingMode::Rfc6587Auto`]'s LF arm instead emits
/// the remainder on a FIN, as RFC 6587 permits. A no-op when nothing is buffered, the ordinary
/// case.
fn report_buffered_tail(framer: &Framer, telemetry: &Telemetry, diag: &mut Diagnostics) {
    let held = framer.buffered();
    if held == 0 {
        return;
    }
    report_frame_error(
        &FrameError::Truncated(format!(
            "the connection ended abruptly with {held} byte(s) of an incomplete frame buffered"
        )),
        telemetry,
        diag,
    );
}

/// Sends one batch. `sink.send` mints a fresh [`logit_pipeline::TraceContext::new_root`] once
/// per *accumulated* batch, not per frame that fed it: the many-to-one attribution gap every
/// accumulating listener shares (`docs/known-gaps.md`'s internal-spans entry).
async fn emit(sink: &Fanout, telemetry: &Telemetry, batch: EventBatch, reason: FlushReason) {
    telemetry.count("logit.component.receive.flushed", 1.0, &[("reason", reason.as_str())]);
    sink.send(batch).await;
}

/// Shared with [`crate::udp`] so both drivers stamp `received_at` identically.
fn now_nanos() -> i64 {
    crate::udp::now_nanos()
}

/// A deadline far enough out that it never arrives: the next-byte deadline on a connection with
/// no `idle_timeout` (or on overflow of an absurd one), so [`serve_connection`]'s read races one
/// deadline rather than an `Option` of one.
///
/// Local because tokio's `Instant::far_future` is `pub(crate)` to tokio; the horizon is tokio's
/// (30 years, since 100 overflows on some platforms). The read waits on it only with no flush
/// interval either; otherwise the flush tick is the earlier deadline. `pub(crate)` so the other
/// listeners' idle deadlines (`crate::logit`, `crate::http`) share one definition.
pub(crate) fn far_future() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(86_400 * 365 * 30)
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, MetricKind, Registry, Resource, Value};
    use logit_pipeline::unwrap_batch;
    use logit_proto::CodecError;
    use rustls_pki_types::pem::PemObject;
    use rustls_pki_types::{CertificateDer, PrivateKeyDer};
    use std::sync::atomic::AtomicUsize;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;
    use tokio::sync::mpsc;

    // ---- framer ------------------------------------------------------------------------------

    /// Pushes `bytes` and drains every frame that completes, as UTF-8 strings. Panics on a
    /// framing error.
    fn push_and_drain(framer: &mut Framer, bytes: &[u8]) -> Vec<String> {
        framer.push(bytes);
        let mut out = Vec::new();
        loop {
            match framer.next_frame().expect("framing should succeed") {
                Some(frame) => out.push(String::from_utf8_lossy(&frame).into_owned()),
                None => return out,
            }
        }
    }

    /// Pushes `bytes` and drains until the framer reports an error, which it must.
    fn push_and_expect_error(framer: &mut Framer, bytes: &[u8]) -> FrameError {
        framer.push(bytes);
        loop {
            match framer.next_frame() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("expected a framing error, got 'need more bytes'"),
                Err(err) => return err,
            }
        }
    }

    #[test]
    fn the_first_byte_latches_the_framing_for_the_connection() {
        assert_eq!(
            Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES).framing(),
            None,
            "nothing is latched before the first byte"
        );

        let mut counted = Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES);
        counted.push(b"1");
        assert_eq!(counted.framing(), Some(Framing::OctetCounting));
        assert_eq!(counted.framing().unwrap().as_str(), "octet_counting");

        // Anything that isn't an ASCII digit latches non-transparent, not only `<`.
        for first in [&b"<"[..], b" ", b"x", b"\n"] {
            let mut lines = Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES);
            lines.push(first);
            assert_eq!(
                lines.framing(),
                Some(Framing::NonTransparent),
                "leading byte {first:?} should latch non-transparent"
            );
        }
        assert_eq!(Framing::NonTransparent.as_str(), "non_transparent");
    }

    /// A later message that looks like the other framing does not change the latch.
    #[test]
    fn the_latched_framing_is_never_re_evaluated() {
        let mut lines = Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES);
        assert_eq!(
            push_and_drain(&mut lines, b"<13>one\n12 not a count\n"),
            vec!["<13>one", "12 not a count"]
        );
        assert_eq!(lines.framing(), Some(Framing::NonTransparent));
    }

    #[test]
    fn a_frame_delivered_one_byte_per_push_is_assembled() {
        for wire in [&b"<13>hello\n"[..], &b"9 <13>hello"[..]] {
            let mut framer = Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES);
            let mut got = Vec::new();
            for byte in wire {
                got.extend(push_and_drain(&mut framer, &[*byte]));
            }
            // The octet-counted case has no terminator: its last byte completes the frame only
            // because the declared length says so.
            assert_eq!(
                got,
                vec!["<13>hello".to_string()],
                "wire {:?}",
                String::from_utf8_lossy(wire)
            );
        }
    }

    #[test]
    fn a_frame_spanning_two_pushes_is_assembled() {
        let mut lines = Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES);
        assert!(push_and_drain(&mut lines, b"<13>hel").is_empty(), "no LF yet");
        assert_eq!(push_and_drain(&mut lines, b"lo\n"), vec!["<13>hello"]);

        let mut counted = Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES);
        assert!(push_and_drain(&mut counted, b"9 <13>he").is_empty(), "short of the declared 9");
        assert_eq!(push_and_drain(&mut counted, b"llo"), vec!["<13>hello"]);
    }

    #[test]
    fn several_frames_in_one_push_come_out_in_order() {
        let mut lines = Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES);
        assert_eq!(
            push_and_drain(&mut lines, b"<13>one\n<13>two\n<13>three\n"),
            vec!["<13>one", "<13>two", "<13>three"]
        );

        let mut counted = Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES);
        assert_eq!(
            push_and_drain(&mut counted, b"7 <13>one7 <13>two9 <13>three"),
            vec!["<13>one", "<13>two", "<13>three"]
        );
    }

    /// Why octet counting exists: a MSG containing a newline is one frame, not two.
    #[test]
    fn an_octet_counted_message_containing_a_newline_stays_one_frame() {
        let msg = "<13>first line\nsecond line\nthird";
        let wire = format!("{} {msg}", msg.len());
        let mut framer = Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES);
        assert_eq!(push_and_drain(&mut framer, wire.as_bytes()), vec![msg]);
    }

    #[test]
    fn one_trailing_cr_is_stripped_from_a_non_transparent_line() {
        let mut framer = Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES);
        assert_eq!(push_and_drain(&mut framer, b"<13>hello\r\n"), vec!["<13>hello"]);
        // Only one: a message ending in CR keeps it.
        assert_eq!(push_and_drain(&mut framer, b"<13>hello\r\r\n"), vec!["<13>hello\r"]);
    }

    #[test]
    fn an_empty_non_transparent_line_is_skipped() {
        let mut framer = Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES);
        assert_eq!(push_and_drain(&mut framer, b"<13>a\n\n\r\n<13>b\n"), vec!["<13>a", "<13>b"]);
    }

    #[test]
    fn an_oversize_non_transparent_line_is_an_oversize_error() {
        // Past the ceiling with no terminator: the framer must not keep buffering.
        let unterminated = vec![b'<'; MAX_FRAME_BYTES + 1];
        let err = push_and_expect_error(
            &mut Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES),
            &unterminated,
        );
        assert_eq!(err.reason(), "oversize", "{err}");

        // And the same line *with* its terminator, which takes the other branch.
        let mut terminated = unterminated.clone();
        terminated.push(b'\n');
        let err = push_and_expect_error(
            &mut Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES),
            &terminated,
        );
        assert_eq!(err.reason(), "oversize", "{err}");

        // At the ceiling is fine: the bound is inclusive.
        let at_ceiling = {
            let mut line = vec![b'<'; MAX_FRAME_BYTES];
            line.push(b'\n');
            line
        };
        assert_eq!(
            push_and_drain(
                &mut Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES),
                &at_ceiling
            )
            .len(),
            1
        );
    }

    #[test]
    fn an_octet_count_over_the_ceiling_is_an_oversize_error() {
        let wire = format!("{} x", MAX_FRAME_BYTES + 1);
        let err = push_and_expect_error(
            &mut Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES),
            wire.as_bytes(),
        );
        assert_eq!(err.reason(), "oversize", "{err}");
    }

    #[test]
    fn a_trailing_partial_line_at_eof_is_emitted_as_a_final_message() {
        let mut framer = Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES);
        assert_eq!(push_and_drain(&mut framer, b"<13>a\n<13>no terminator"), vec!["<13>a"]);
        let last = framer.finish().expect("a terminator-less remainder is a message, not an error");
        assert_eq!(last.as_deref().map(String::from_utf8_lossy), Some("<13>no terminator".into()));
        assert_eq!(framer.finish(), Ok(None), "nothing is left after finish()");
    }

    #[test]
    fn a_trailing_partial_octet_counted_frame_at_eof_is_truncated() {
        let mut framer = Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES);
        assert!(push_and_drain(&mut framer, b"9 <13>hel").is_empty());
        assert_eq!(framer.buffered(), 9);
        let err = framer.finish().expect_err("a short MSG under a declared count is truncated");
        assert_eq!(err.reason(), "truncated", "{err}");
        assert_eq!(framer.buffered(), 0, "the unusable remainder is discarded");
    }

    #[test]
    fn a_non_digit_before_the_sp_is_malformed() {
        let err = push_and_expect_error(
            &mut Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES),
            b"12x <13>hello",
        );
        assert_eq!(err.reason(), "malformed", "{err}");
    }

    #[test]
    fn a_ten_digit_octet_count_is_malformed() {
        let err = push_and_expect_error(
            &mut Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES),
            b"1234567890 <13>hello",
        );
        assert_eq!(err.reason(), "malformed", "{err}");
    }

    #[test]
    fn a_zero_octet_count_is_malformed() {
        let err = push_and_expect_error(
            &mut Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES),
            b"0 <13>hello",
        );
        assert_eq!(err.reason(), "malformed", "{err}");
        assert!(err.to_string().contains("zero"), "{err}");
    }

    /// A padded count latches octet counting and fails loudly rather than reading as
    /// non-transparent (see [`Framing`]).
    #[test]
    fn an_octet_count_with_a_leading_zero_is_malformed() {
        let mut framer = Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES);
        framer.push(b"012 <13>hello");
        assert_eq!(framer.framing(), Some(Framing::OctetCounting));
        let err = framer.next_frame().expect_err("a padded count is malformed, not 12");
        assert_eq!(err.reason(), "malformed", "{err}");
        assert!(err.to_string().contains("leading zero"), "{err}");
    }

    // ---- framer: explicit framing modes --------------------------------------------------------

    /// Why [`FramingMode::Lines`] exists: a leading digit (`1.hits:1|c`) would latch octet
    /// counting under [`FramingMode::Rfc6587Auto`].
    #[test]
    fn a_line_only_framer_never_latches_octet_counting_on_a_leading_digit() {
        let mut framer =
            Framer::new(FramingMode::Lines { oversize: Oversize::Fatal }, MAX_FRAME_BYTES);
        assert_eq!(
            framer.framing(),
            Some(Framing::NonTransparent),
            "fixed at construction, with nothing to sniff"
        );
        assert!(!framer.first_byte_seen(), "and no byte has arrived yet");

        assert_eq!(
            push_and_drain(&mut framer, b"1.hits:1|c\n12 not a count\n"),
            vec!["1.hits:1|c", "12 not a count"]
        );
        assert!(framer.first_byte_seen());
        assert_eq!(framer.framing(), Some(Framing::NonTransparent), "and never re-evaluated");

        // The same first byte under the auto mode, for contrast.
        let mut auto = Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES);
        auto.push(b"1.hits:1|c\n");
        assert_eq!(auto.framing(), Some(Framing::OctetCounting), "the contrast this test is for");
    }

    /// [`Oversize::DrainToNextLine`]: one line past the bound is dropped and counted once, and the
    /// next line still frames.
    #[test]
    fn a_line_only_framer_drains_to_the_next_newline_past_the_bound() {
        let mut framer =
            Framer::new(FramingMode::Lines { oversize: Oversize::DrainToNextLine }, 16);

        // Past the bound with no terminator: abandon the line and discard until its `LF`.
        framer.push(&[b'x'; 40]);
        let err = framer.next_frame().expect_err("40 bytes with no LF is past the 16-byte bound");
        assert_eq!(err.reason(), "oversize", "{err}");
        assert!(!err.is_fatal(), "a line protocol resynchronizes at the next LF: {err}");
        assert_eq!(framer.next_frame(), Ok(None), "still draining, nothing to hand over");

        // The tail of the abandoned line, then a good one: only the good one comes out.
        assert_eq!(push_and_drain(&mut framer, b"more of it\nsurvivor\n"), vec!["survivor"]);
        assert_eq!(
            push_and_drain(&mut framer, b"another\n"),
            vec!["another"],
            "the bound is per line, not a running total over the connection"
        );

        // The other branch: the terminator is already buffered, so only that line is dropped.
        let mut terminated =
            Framer::new(FramingMode::Lines { oversize: Oversize::DrainToNextLine }, 16);
        let err = push_and_expect_error(&mut terminated, b"0123456789012345678\nsurvivor\n");
        assert_eq!(err.reason(), "oversize", "{err}");
        assert!(!err.is_fatal(), "{err}");
        assert_eq!(push_and_drain(&mut terminated, b""), vec!["survivor"]);
    }

    /// Under `Lines` an unterminated remainder at a clean EOF is truncated, unlike under
    /// `Rfc6587Auto` (asserted alongside); see [`Framer::finish`].
    #[test]
    fn a_line_only_framer_drops_an_unterminated_tail_at_eof_as_truncated() {
        let mut framer = Framer::new(
            FramingMode::Lines { oversize: Oversize::DrainToNextLine },
            MAX_FRAME_BYTES,
        );
        assert_eq!(
            push_and_drain(&mut framer, b"svc.web01.cpu 1 17000\nsvc.web01.cpu 42.5 17000"),
            vec!["svc.web01.cpu 1 17000"],
            "only the terminated line frames"
        );
        let err = framer.finish().expect_err("an unterminated tail is truncated, not a datapoint");
        assert_eq!(err.reason(), "truncated", "{err}");
        assert!(err.is_fatal(), "the connection is already over; nothing to resynchronize");
        assert_eq!(framer.buffered(), 0, "and the remainder is consumed either way");

        // A whitespace-only remainder at EOF is not counted. (Mid-stream, a whitespace-only line
        // is still framed and handed to the decoder; only an empty one is skipped.)
        let mut padded =
            Framer::new(FramingMode::Lines { oversize: Oversize::Fatal }, MAX_FRAME_BYTES);
        assert_eq!(push_and_drain(&mut padded, b"a.b 1 17000\n\n \t"), vec!["a.b 1 17000"]);
        assert_eq!(padded.finish(), Ok(None), "whitespace padding is not a truncated frame");

        let mut syslog = Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES);
        syslog.push(b"<13>no terminator");
        assert_eq!(
            syslog.finish().expect("RFC 6587 permits a terminator-less final message"),
            Some(Bytes::from_static(b"<13>no terminator"))
        );
    }

    #[test]
    fn a_length_prefixed_frame_split_across_pushes_is_assembled() {
        let payload = b"a pickled batch";
        let mut wire = (payload.len() as u32).to_be_bytes().to_vec();
        wire.extend_from_slice(payload);

        let mut framer = Framer::new(FramingMode::LengthPrefixed, MAX_FRAME_BYTES);
        assert_eq!(framer.framing(), Some(Framing::LengthPrefixed));
        assert_eq!(Framing::LengthPrefixed.as_str(), "length_prefixed");

        // One byte per push, so the prefix itself straddles pushes: a reader that assumed a
        // whole prefix per read, or read it little-endian, fails this.
        let mut got = Vec::new();
        for byte in &wire {
            got.extend(push_and_drain(&mut framer, &[*byte]));
        }
        assert_eq!(got, vec!["a pickled batch".to_string()]);

        let mut both = wire.clone();
        both.extend_from_slice(&wire);
        assert_eq!(
            push_and_drain(&mut framer, &both).len(),
            2,
            "two frames back to back in one push come out in order"
        );
    }

    #[test]
    fn a_length_prefix_over_the_bound_is_a_fatal_oversize() {
        let mut framer = Framer::new(FramingMode::LengthPrefixed, 1024);
        let err = push_and_expect_error(&mut framer, &1_000_000u32.to_be_bytes());
        assert_eq!(err.reason(), "oversize", "{err}");
        assert!(
            err.is_fatal(),
            "nothing after a bad length has been read, so there is no resync point: {err}"
        );

        // The peer closing mid-payload is truncated: the declared length says bytes are missing.
        let mut short = Framer::new(FramingMode::LengthPrefixed, 1024);
        short.push(&[0, 0, 0, 9, b'h', b'i']);
        assert_eq!(short.next_frame(), Ok(None), "the declared 9 bytes have not all arrived");
        assert_eq!(short.buffered(), 6);
        let err = short.finish().expect_err("a short payload under a declared length is truncated");
        assert_eq!(err.reason(), "truncated", "{err}");
    }

    // ---- framer: recorded interop fixtures ----------------------------------------------------
    //
    // A real rsyslog `omfwd` forwarder's TCP byte stream at its default `TCP_Framing` (RFC 6587
    // §3.4.2 non-transparent), captured by `script/record-fixtures rsyslog-tcp`
    // (`testdata/interop/syslog/README.md`'s `rsyslog-tcp-000.raw` row), pushed through `Framer`
    // as it arrived and decoded with the real `SyslogDecoder`.

    /// `testdata/interop/syslog/<name>` as raw bytes: a TCP fixture is a whole connection's byte
    /// stream, not one UTF-8 datagram like `crate::syslog`'s `interop_fixture` reads.
    fn interop_fixture_bytes(name: &str) -> Vec<u8> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../testdata/interop/syslog")
            .join(name);
        std::fs::read(&path)
            .unwrap_or_else(|e| panic!("reading interop fixture {}: {e}", path.display()))
    }

    #[test]
    fn interop_fixture_rsyslog_tcp_non_transparent_frame() {
        let wire = interop_fixture_bytes("rsyslog-tcp-000.raw");

        // One push, then `finish`: the recorded connection carries one message and is torn down
        // by the recording harness.
        let mut framer = Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES);
        framer.push(&wire);
        assert_eq!(
            framer.framing(),
            Some(Framing::NonTransparent),
            "a stock omfwd forwarder with no TCP_Framing parameter must latch non-transparent, not \
             octet-counting"
        );

        let mut frames = Vec::new();
        while let Some(frame) = framer.next_frame().expect("framing should succeed") {
            frames.push(frame);
        }
        if let Some(trailing) = framer.finish().expect("finish should succeed") {
            frames.push(trailing);
        }
        assert_eq!(frames.len(), 1, "exactly one message on this connection: {frames:?}");

        // A non-transparent frame never has an embedded newline, so `SyslogDecoder`'s
        // `\n`-splitting is a no-op here.
        let mut decoder = crate::syslog::SyslogDecoder::new(Arc::new(Resource::default()));
        let events = decoder
            .decode(frames.into_iter().next().unwrap())
            .expect("decode should succeed")
            .events;
        assert_eq!(events.len(), 1, "exactly one decoded event: {events:?}");
        let event = &events[0];

        assert_eq!(
            event.attributes.get("syslog.tag").and_then(Value::as_str),
            Some("logit-fixture"),
            "syslog.tag should match the `logger -t logit-fixture` invocation the fixture recorded"
        );
        // `logger` with no `-p` sends the default `user.notice` (PRI 13 = facility 1 * 8 +
        // severity 5).
        assert_eq!(
            event.log.as_ref().and_then(|log| log.severity),
            Some(logit_core::Severity::Info),
            "user.notice (PRI 13) maps to Severity::Info (13 % 8 = 5)"
        );
        let message = event.log.as_ref().expect("event should carry a log").message.as_str();
        assert_eq!(message, Some("hello from rsyslog, captured for logit interop fixtures"));
    }

    // ---- driver: fixtures and harness ---------------------------------------------------------

    /// One frame to one event carrying the raw frame under `"payload"`, except the literal bytes
    /// `b"BAD"`, which are rejected.
    #[derive(Clone)]
    struct TestDecoder {
        resource: Arc<Resource>,
    }

    impl TestDecoder {
        fn new() -> Self {
            Self { resource: Arc::new(Resource::default()) }
        }
    }

    impl Decoder for TestDecoder {
        fn decode_into(
            &mut self,
            bytes: Bytes,
            received_at: i64,
            out: &mut Vec<Event>,
        ) -> Result<(Arc<Resource>, Option<Arc<logit_core::Scope>>), CodecError> {
            if &bytes[..] == b"BAD" {
                return Err(CodecError::Malformed("bad frame".to_string()));
            }
            let mut attrs = AttrMap::new();
            attrs.insert("payload", Value::str(String::from_utf8_lossy(&bytes)));
            out.push(Event::empty(received_at, attrs));
            Ok((Arc::clone(&self.resource), None))
        }
    }

    fn payload(event: &Event) -> String {
        match event.attributes.get("payload") {
            Some(Value::Str(bytes)) => String::from_utf8_lossy(bytes).into_owned(),
            other => panic!("expected a payload attribute, got {other:?}"),
        }
    }

    /// One event per frame and no interval timer, so every delivery is attributable to one frame.
    fn one_per_frame() -> TcpListenerConfig {
        TcpListenerConfig {
            batch_max_events: 1,
            batch_flush_interval: Duration::ZERO,
            ..TcpListenerConfig::default()
        }
    }

    /// Binds an ephemeral port through `Input::bind`, not a bind-and-drop probe socket.
    async fn bound_listener(config: TcpListenerConfig) -> (String, TcpListener<TestDecoder>) {
        let mut listener = TcpListener::new("127.0.0.1:0", TestDecoder::new(), config);
        listener.bind().await.expect("binding an ephemeral port should succeed");
        let addr = listener.local_addr().expect("bind() leaves a real address behind").to_string();
        (addr, listener)
    }

    fn fanout_into_channel(capacity: usize) -> (Fanout, mpsc::Receiver<logit_pipeline::Delivered>) {
        let (tx, rx) = mpsc::channel(capacity);
        (Fanout::new(vec![tx]), rx)
    }

    async fn recv_batch(rx: &mut mpsc::Receiver<logit_pipeline::Delivered>) -> EventBatch {
        let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("a batch should be delivered within 5s")
            .expect("the fanout should not have closed");
        unwrap_batch(delivered)
    }

    fn payloads(batch: &EventBatch) -> Vec<String> {
        batch.events.iter().map(payload).collect()
    }

    async fn connect(addr: &str) -> TcpStream {
        TcpStream::connect(addr).await.expect("connecting to the bound listener should succeed")
    }

    /// Reads one byte, expecting the peer to have closed instead.
    async fn expect_closed<S: AsyncRead + Unpin>(stream: &mut S, what: &str) {
        let mut buf = [0u8; 1];
        let result = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
            .await
            .unwrap_or_else(|_| panic!("{what}: expected a close within 2s"));
        match result {
            Ok(n) => assert_eq!(n, 0, "{what}: expected a close, got a byte"),
            // A close with bytes still unread in the peer's receive queue is an RST, not a FIN
            // (Linux `tcp_close`), and a read after RST is `ECONNRESET`: still a close. The
            // oversize-frame test gets either, depending on socket-buffer sizes.
            Err(err) if err.kind() == std::io::ErrorKind::ConnectionReset => {}
            Err(err) => panic!("{what}: read failed outright: {err}"),
        }
    }

    /// The value of `metric`'s `Sum` in a drained `Registry` snapshot, optionally restricted to
    /// the point carrying `tag`. Drain-then-query, so one test can check several metrics.
    fn sum_of(events: &[Event], metric: &str, tag: Option<(&str, &str)>) -> Option<f64> {
        events.iter().find_map(|e| {
            if let Some((key, value)) = tag {
                if e.attributes.get(key).and_then(|v| v.as_str()) != Some(value) {
                    return None;
                }
            }
            e.metrics.iter().find_map(|m| {
                if logit_core::interner::resolve(m.name) != metric {
                    return None;
                }
                match m.kind {
                    MetricKind::Sum(sum) => Some(sum.value),
                    _ => None,
                }
            })
        })
    }

    /// The single value of gauge `name` (last-write-wins per drain), or `None` if never recorded.
    fn gauge_of(events: &[Event], name: &str) -> Option<f64> {
        events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match m.kind {
                MetricKind::Gauge(v) if logit_core::interner::resolve(m.name) == name => Some(v),
                _ => None,
            })
        })
    }

    fn testdata_dir() -> std::path::PathBuf {
        // The repo root's `testdata/tls` (`testdata/tls/README.md`), two levels up.
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    fn test_tls_settings(client_ca_file: Option<&str>) -> TlsServerSettings {
        TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: client_ca_file.map(str::to_string),
        }
    }

    /// A `tokio-rustls` client trusting only `ca_file` under `testdata/tls` (`other-ca.pem` makes
    /// a real wrong-CA case). `client_cert` is `(cert, key)` file names for mTLS, `None` for none.
    async fn tls_connector(
        ca_file: &str,
        client_cert: Option<(&str, &str)>,
    ) -> tokio_rustls::TlsConnector {
        let dir = testdata_dir();
        let mut roots = rustls::RootCertStore::empty();
        let ca: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(dir.join(ca_file))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        roots.add_parsable_certificates(ca);
        let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots);
        let cfg = match client_cert {
            Some((cert_file, key_file)) => {
                let chain: Vec<CertificateDer<'static>> =
                    CertificateDer::pem_file_iter(dir.join(cert_file))
                        .unwrap()
                        .collect::<Result<_, _>>()
                        .unwrap();
                let key = PrivateKeyDer::from_pem_file(dir.join(key_file)).unwrap();
                builder.with_client_auth_cert(chain, key).unwrap()
            }
            None => builder.with_no_client_auth(),
        };
        tokio_rustls::TlsConnector::from(Arc::new(cfg))
    }

    /// `testdata/tls/server.pem`'s SAN.
    fn server_name() -> rustls_pki_types::ServerName<'static> {
        rustls_pki_types::ServerName::try_from("localhost").unwrap()
    }

    type ClientTls = tokio_rustls::client::TlsStream<TcpStream>;

    async fn tls_connect(connector: &tokio_rustls::TlsConnector, addr: &str) -> ClientTls {
        let stream = connect(addr).await;
        tokio::time::timeout(Duration::from_secs(5), connector.connect(server_name(), stream))
            .await
            .expect("the TLS handshake should complete within 5s")
            .expect("the TLS handshake should succeed")
    }

    // ---- driver: plaintext --------------------------------------------------------------------

    /// After `bind()` the port is live and its address readable before `run`; a second `bind()`
    /// is a no-op.
    #[tokio::test]
    async fn bind_makes_the_port_live_and_local_addr_reports_it_before_run() {
        let mut listener =
            TcpListener::new("127.0.0.1:0", TestDecoder::new(), TcpListenerConfig::default());
        assert_eq!(listener.local_addr(), None, "no address before bind()");

        listener.bind().await.expect("binding an ephemeral port should succeed");
        let addr = listener.local_addr().expect("bind() should leave a real address behind");

        // Nothing is running yet: this connection sits in the accept backlog.
        let _early = TcpStream::connect(addr).await.expect("the bound port should accept");

        listener.bind().await.expect("a second bind should be a harmless no-op");
        assert_eq!(listener.local_addr(), Some(addr), "the address must not change");
    }

    #[tokio::test]
    async fn a_plaintext_connection_round_trips_a_decoded_frame() {
        let (addr, mut listener) = bound_listener(one_per_frame()).await;
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        client.write_all(b"<13>hello\n").await.unwrap();

        let batch = recv_batch(&mut rx).await;
        assert_eq!(payloads(&batch), vec!["<13>hello"]);

        handle.abort();
    }

    #[tokio::test]
    async fn the_accumulator_flushes_on_batch_max_events() {
        let config = TcpListenerConfig {
            batch_max_events: 2,
            batch_flush_interval: Duration::ZERO,
            ..TcpListenerConfig::default()
        };
        let (addr, mut listener) = bound_listener(config).await;
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        // The bound fires on the second frame; with the interval timer off the third stays held.
        client.write_all(b"<13>one\n<13>two\n<13>three\n").await.unwrap();

        let batch = recv_batch(&mut rx).await;
        assert_eq!(payloads(&batch), vec!["<13>one", "<13>two"]);
        assert!(
            tokio::time::timeout(Duration::from_millis(200), rx.recv()).await.is_err(),
            "the third frame must still be accumulating, not delivered"
        );

        handle.abort();
    }

    #[tokio::test]
    async fn the_accumulator_flushes_on_the_batch_flush_interval() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let config = TcpListenerConfig {
            batch_max_events: 1_000,
            batch_flush_interval: Duration::from_millis(50),
            ..TcpListenerConfig::default()
        };
        let (addr, listener) = bound_listener(config).await;
        let mut listener = listener.with_telemetry(telemetry);
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        // Short of `batch_max_events` on an open connection: only the interval timer delivers it.
        client.write_all(b"<13>alone\n").await.unwrap();

        let batch = recv_batch(&mut rx).await;
        assert_eq!(payloads(&batch), vec!["<13>alone"]);

        let events = registry.drain(0);
        assert_eq!(
            sum_of(&events, "logit.component.receive.flushed", Some(("reason", "interval"))),
            Some(1.0)
        );
        assert_eq!(sum_of(&events, "logit.input.frames", None), Some(1.0));
        assert_eq!(sum_of(&events, "logit.input.frame.bytes", None), Some(9.0));

        handle.abort();
    }

    #[tokio::test]
    async fn a_clean_close_flushes_whatever_is_accumulated() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let config = TcpListenerConfig {
            batch_max_events: 1_000,
            batch_flush_interval: Duration::ZERO,
            ..TcpListenerConfig::default()
        };
        let (addr, listener) = bound_listener(config).await;
        let mut listener = listener.with_telemetry(telemetry);
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        // The unterminated last message is emitted as a final frame (`Framer::finish`), then the
        // accumulator flushes.
        client.write_all(b"<13>one\n<13>two").await.unwrap();
        client.flush().await.unwrap();
        drop(client);

        let batch = recv_batch(&mut rx).await;
        assert_eq!(payloads(&batch), vec!["<13>one", "<13>two"]);
        assert_eq!(
            sum_of(
                &registry.drain(0),
                "logit.component.receive.flushed",
                Some(("reason", "closed"))
            ),
            Some(1.0)
        );

        handle.abort();
    }

    /// A rejected frame is dropped; the frames either side of it are still served.
    #[tokio::test]
    async fn a_frame_the_decoder_rejects_does_not_close_the_connection() {
        let (addr, mut listener) = bound_listener(one_per_frame()).await;
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        client.write_all(b"<13>good-1\nBAD\n<13>good-2\n").await.unwrap();

        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>good-1"]);
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>good-2"]);

        handle.abort();
    }

    /// Past the cap, a connection is closed rather than queued, and counted.
    #[tokio::test]
    async fn the_connection_cap_drops_a_connection_past_the_limit_and_counts_it() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener = listener.with_telemetry(telemetry).with_max_connections(1);
        let (sink, _rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        // The first connection holds the one permit.
        let _first = connect(&addr).await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut second = connect(&addr).await;
        expect_closed(&mut second, "a past-the-cap connection").await;

        assert_eq!(
            sum_of(
                &registry.drain(0),
                "logit.input.connections.rejected",
                Some(("reason", "limit"))
            ),
            Some(1.0)
        );

        handle.abort();
    }

    /// A connection's `Diagnostics` clone throttles on the listener-wide count.
    #[test]
    fn the_per_frame_diagnostic_throttle_is_shared_not_per_connection() {
        let diag = Diagnostics::new("syslog_in");
        // What the accept loop hands one connection task.
        let mut connection_diag = diag.clone();
        let telemetry = Telemetry::default();
        let err = FrameError::Malformed("an octet count of zero".to_string());

        assert!(
            report_frame_error(&err, &telemetry, &mut connection_diag),
            "the 1st occurrence across the listener reports"
        );
        assert!(
            report_frame_error(&err, &telemetry, &mut connection_diag),
            "the 2nd reports too -- 2 is a power of two"
        );
        assert!(
            !report_frame_error(&err, &telemetry, &mut connection_diag),
            "the 3rd is suppressed, which an unshared per-connection count could never manage"
        );
        assert_eq!(
            diag.occurrences("framing_error"),
            3,
            "and the listener's own value reads all three back"
        );
    }

    /// Three connections' framing errors all count on the one [`Diagnostics`] the listener got.
    #[tokio::test]
    async fn three_connections_report_their_framing_errors_through_one_throttle() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let diag = Diagnostics::new("syslog_in");
        // The occurrence count is the one observable that differs if counts aren't shared: the
        // metrics below reach 3 either way, since `Telemetry` mirrors into one component buffer.
        let listener_diag = diag.clone();
        let listener = listener.with_telemetry(telemetry).with_diagnostics(diag);
        let (sink, _rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut listener = listener;
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        for attempt in 0..3 {
            // A zero octet count: malformed, and fatal to its connection.
            let mut client = connect(&addr).await;
            client.write_all(b"0 nope").await.unwrap();
            expect_closed(&mut client, &format!("connection {attempt} after a malformed count"))
                .await;
        }

        assert_eq!(
            listener_diag.occurrences("framing_error"),
            3,
            "all three connections must count on the one listener-wide Diagnostics -- a clone \
             with counts of its own would leave this at 0, having counted 1 in each throwaway copy"
        );
        // Holds either way; confirms the classification.
        assert_eq!(
            sum_of(&registry.drain(0), "logit.input.frames.dropped", Some(("reason", "malformed"))),
            Some(3.0)
        );

        handle.abort();
    }

    /// An RST with a partial frame buffered (`ReadStep::Failed`) counts it `truncated`.
    #[tokio::test]
    async fn an_abrupt_close_with_a_buffered_partial_frame_counts_it_truncated() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener = listener.with_telemetry(telemetry);
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        client.write_all(b"<13>complete\n").await.unwrap();
        // Awaiting the delivery proves the server is back blocked in the next read, so it
        // consumes the tail below into its framer before the RST (an RST landing on still-queued
        // bytes discards them unread, an uncountable case).
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>complete"]);

        client.write_all(b"<13>unterminated").await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // `SO_LINGER 0` makes the close an RST, not a FIN, so the server's read fails
        // `ECONNRESET` (`ReadStep::Failed`) rather than reaching a clean EOF.
        socket2::SockRef::from(&client)
            .set_linger(Some(Duration::ZERO))
            .expect("SO_LINGER should be settable on a loopback socket");
        drop(client);

        // The count lands on the connection's own task, after its read fails.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            sum_of(&registry.drain(0), "logit.input.frames.dropped", Some(("reason", "truncated"))),
            Some(1.0),
            "the partial frame the RST discarded must still be counted"
        );

        handle.abort();
    }

    /// Shutdown mid-message counts the partial frame `truncated`.
    #[tokio::test]
    async fn shutdown_mid_message_counts_the_buffered_partial_frame() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener = listener.with_telemetry(telemetry);
        let (sink, mut rx) = fanout_into_channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        client.write_all(b"<13>complete\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>complete"]);

        client.write_all(b"<13>half a mes").await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown_tx.send(true).expect("the receiver should still be alive");

        let closed = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
        assert!(
            closed.expect("the fanout should close within 2s").is_none(),
            "nothing complete was pending, so no batch should follow"
        );
        assert_eq!(
            sum_of(&registry.drain(0), "logit.input.frames.dropped", Some(("reason", "truncated"))),
            Some(1.0)
        );

        handle.await.expect("the task should not panic").expect("shutdown should be clean");
    }

    /// A fatal framing error closes its own connection and no sibling.
    #[tokio::test]
    async fn an_oversize_frame_closes_only_that_connection() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener = listener.with_telemetry(telemetry);
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut good = connect(&addr).await;
        let mut bad = connect(&addr).await;
        good.write_all(b"<13>fine\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>fine"]);

        // The write may fail once the server has closed on us, which is the behaviour under test.
        let oversize = vec![b'<'; MAX_FRAME_BYTES + 4_096];
        let _ = tokio::time::timeout(Duration::from_secs(5), bad.write_all(&oversize)).await;
        expect_closed(&mut bad, "the connection that sent an oversize frame").await;

        good.write_all(b"<13>still here\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>still here"]);

        assert_eq!(
            sum_of(&registry.drain(0), "logit.input.frames.dropped", Some(("reason", "oversize"))),
            Some(1.0)
        );

        handle.abort();
    }

    /// Shutdown drops every connection's `Fanout` clone, even an idle connection's.
    #[tokio::test]
    async fn shutdown_returns_promptly_with_an_idle_connection_still_open() {
        let (addr, mut listener) = bound_listener(one_per_frame()).await;
        let (sink, mut rx) = fanout_into_channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        // An idle connection's task drops its clone only because its read races `shutdown`.
        let _idle = connect(&addr).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown_tx.send(true).expect("the receiver should still be alive");

        let closed = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
        assert!(
            closed.expect("the fanout should close within 2s").is_none(),
            "expected every Fanout clone to have dropped"
        );
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("run_until_shutdown should return within 2s")
            .expect("the task should not panic")
            .expect("shutdown should be clean");
    }

    // ---- driver: TLS --------------------------------------------------------------------------

    #[tokio::test]
    async fn a_tls_connection_round_trips_a_decoded_frame() {
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener = listener.with_tls(&test_tls_settings(None), &testdata_dir()).unwrap();
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let connector = tls_connector("ca.pem", None).await;
        let mut client = tls_connect(&connector, &addr).await;
        // Octet counting over TLS: the latch is independent of the transport.
        client.write_all(b"12 <13>over tls").await.unwrap();
        client.flush().await.unwrap();

        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>over tls"]);

        handle.abort();
    }

    #[tokio::test]
    async fn a_client_trusting_the_wrong_ca_is_refused_and_the_listener_keeps_serving() {
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener = listener.with_tls(&test_tls_settings(None), &testdata_dir()).unwrap();
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let wrong = tls_connector("other-ca.pem", None).await;
        let stream = connect(&addr).await;
        let refused =
            tokio::time::timeout(Duration::from_secs(5), wrong.connect(server_name(), stream))
                .await
                .expect("the handshake should resolve within 5s");
        assert!(refused.is_err(), "a client trusting only other-ca.pem must not complete");

        // A failed handshake must not be fatal to the listener or its siblings.
        let connector = tls_connector("ca.pem", None).await;
        let mut client = tls_connect(&connector, &addr).await;
        client.write_all(b"<13>still serving\n").await.unwrap();
        client.flush().await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>still serving"]);

        handle.abort();
    }

    #[tokio::test]
    async fn mutual_tls_accepts_a_client_certificate_and_refuses_a_client_without_one() {
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener =
            listener.with_tls(&test_tls_settings(Some("ca.pem")), &testdata_dir()).unwrap();
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let with_cert = tls_connector("ca.pem", Some(("client.pem", "client.key"))).await;
        let mut client = tls_connect(&with_cert, &addr).await;
        client.write_all(b"<13>authenticated\n").await.unwrap();
        client.flush().await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>authenticated"]);

        // Under TLS 1.3 the server's "certificate required" alert lands after the client believes
        // the handshake finished, so the failure may surface at connect or on the first write;
        // either way nothing is delivered.
        let without_cert = tls_connector("ca.pem", None).await;
        let stream = connect(&addr).await;
        if let Ok(Ok(mut anonymous)) = tokio::time::timeout(
            Duration::from_secs(5),
            without_cert.connect(server_name(), stream),
        )
        .await
        {
            let _ = anonymous.write_all(b"<13>no certificate\n").await;
            let _ = anonymous.flush().await;
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(500), rx.recv()).await.is_err(),
            "a client presenting no certificate must not deliver events"
        );

        handle.abort();
    }

    /// A silent plaintext connection releases its permit at the first-byte deadline.
    #[tokio::test]
    async fn a_silent_plaintext_connection_releases_its_permit_after_the_handshake_timeout() {
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener =
            listener.with_max_connections(1).with_handshake_timeout(Duration::from_millis(50));
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        // Held (not dropped) past the deadline, so only the deadline can free the permit.
        let mut silent = connect(&addr).await;
        expect_closed(&mut silent, "a plaintext connection that sent no bytes").await;

        let mut client = connect(&addr).await;
        client.write_all(b"<13>permit came back\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>permit came back"]);

        drop(silent);
        handle.abort();
    }

    /// With no `idle_timeout`, a gap past the first-byte budget after the first frame is fine.
    #[tokio::test]
    async fn the_first_byte_deadline_does_not_apply_once_the_framing_has_latched() {
        // The flush timer left on: a tick re-entering the read is what a per-read budget would
        // keep resetting.
        let config = TcpListenerConfig {
            batch_max_events: 1,
            batch_flush_interval: Duration::from_millis(100),
            ..TcpListenerConfig::default()
        };
        let (addr, listener) = bound_listener(config).await;
        let mut listener = listener.with_handshake_timeout(Duration::from_millis(50));
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        client.write_all(b"<13>first\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>first"]);

        // Past the first-byte budget and several flush ticks.
        tokio::time::sleep(Duration::from_millis(300)).await;
        client.write_all(b"<13>much later\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>much later"]);

        handle.abort();
    }

    /// The first-byte deadline fires under every framing mode, not only `Rfc6587Auto`.
    #[tokio::test]
    async fn the_first_byte_deadline_applies_under_every_framing_mode() {
        // `(mode, the wire bytes of one frame)`; each decodes to `<13>hello`.
        let length_prefixed = {
            let mut wire = 9u32.to_be_bytes().to_vec();
            wire.extend_from_slice(b"<13>hello");
            wire
        };
        let cases: Vec<(FramingMode, Vec<u8>)> = vec![
            (FramingMode::Rfc6587Auto, b"<13>hello\n".to_vec()),
            (FramingMode::Lines { oversize: Oversize::Fatal }, b"<13>hello\n".to_vec()),
            (FramingMode::Lines { oversize: Oversize::DrainToNextLine }, b"<13>hello\n".to_vec()),
            (FramingMode::LengthPrefixed, length_prefixed),
        ];

        for (mode, wire) in cases {
            let (addr, listener) = bound_listener(one_per_frame()).await;
            let mut listener = listener
                .with_framing(mode, MAX_FRAME_BYTES)
                .with_max_connections(1)
                .with_handshake_timeout(Duration::from_millis(50));
            let (sink, mut rx) = fanout_into_channel(16);
            let (_shutdown_tx, shutdown_rx) = watch::channel(false);
            let handle =
                tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

            // Held (not dropped) past the deadline, so only the deadline can free the permit.
            let mut silent = connect(&addr).await;
            expect_closed(&mut silent, &format!("a silent connection under {mode:?}")).await;

            let mut client = connect(&addr).await;
            client.write_all(&wire).await.unwrap();
            assert_eq!(
                payloads(&recv_batch(&mut rx).await),
                vec!["<13>hello"],
                "the permit must have come back under {mode:?}"
            );

            drop(silent);
            handle.abort();
        }
    }

    /// A TLS connection that never sends a ClientHello releases its permit at the timeout.
    #[tokio::test]
    async fn a_silent_connection_releases_its_permit_after_the_handshake_timeout() {
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener = listener
            .with_tls(&test_tls_settings(None), &testdata_dir())
            .unwrap()
            .with_max_connections(1)
            .with_handshake_timeout(Duration::from_millis(50));
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        // Held (not dropped) past the timeout, so only the timeout can free the permit.
        let _silent = connect(&addr).await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        let connector = tls_connector("ca.pem", None).await;
        let mut client = tls_connect(&connector, &addr).await;
        client.write_all(b"<13>permit came back\n").await.unwrap();
        client.flush().await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>permit came back"]);

        handle.abort();
    }

    // ---- driver: idle timeout -----------------------------------------------------------------
    //
    // Real durations (50-200ms), never `tokio::time::pause()`: these tests race a timer against
    // a socket read, and paused time would advance past the read. "Closed" assertions have
    // `expect_closed`'s 2s ceiling against deadlines of at most 200ms; "still open" ones assert
    // `timeout(50ms, read) == Err(Elapsed)`, which scheduler lag only makes more true.

    /// Asserts a client connection is still open: this driver never writes to a peer, so a
    /// blocked read means live, while a closed one returns `Ok(0)` or `ECONNRESET` immediately.
    async fn expect_still_open<S: AsyncRead + Unpin>(stream: &mut S, what: &str) {
        let mut buf = [0u8; 1];
        match tokio::time::timeout(Duration::from_millis(50), stream.read(&mut buf)).await {
            Err(_elapsed) => {}
            Ok(Ok(0)) => panic!("{what}: expected the connection to still be open, got a close"),
            Ok(Ok(n)) => panic!("{what}: expected no bytes, got {n}"),
            Ok(Err(err)) => panic!("{what}: expected the connection to still be open, got {err}"),
        }
    }

    /// An idle connection is closed, counted but not diagnosed, and its permit comes back.
    #[tokio::test]
    async fn an_idle_connection_is_closed_after_the_idle_timeout_and_releases_its_permit() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let diag = Diagnostics::new("syslog_in");
        let listener_diag = diag.clone();
        let mut listener = listener
            .with_telemetry(telemetry)
            .with_diagnostics(diag)
            .with_max_connections(1)
            .with_idle_timeout(Some(Duration::from_millis(100)));
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        // One frame, so only the idle clock can close this; then silence, socket held open.
        let mut quiet = connect(&addr).await;
        quiet.write_all(b"<13>hello\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>hello"]);

        expect_closed(&mut quiet, "a connection quiet past its idle_timeout").await;

        let drained = registry.drain(0);
        assert_eq!(
            sum_of(&drained, "logit.input.connections.closed", Some(("reason", "idle"))),
            Some(1.0),
            "an idle close is counted"
        );
        assert_eq!(
            listener_diag.occurrences("connection_error"),
            0,
            "and never diagnosed -- an idle close returns Ok(()), so the accept loop's \
             connection_error path must not see it"
        );

        let mut client = connect(&addr).await;
        client.write_all(b"<13>permit came back\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>permit came back"]);

        drop(quiet);
        handle.abort();
    }

    /// Time parked in `Fanout::send` on a full downstream never counts toward the idle clock.
    #[tokio::test]
    async fn a_connection_blocked_on_a_full_downstream_is_not_closed_as_idle() {
        let idle = Duration::from_millis(100);
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener = listener.with_idle_timeout(Some(idle));
        // Capacity 1: the first send is buffered, the second blocks until something receives.
        let (sink, mut rx) = fanout_into_channel(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        client.write_all(b"<13>one\n").await.unwrap();
        client.write_all(b"<13>two\n").await.unwrap();

        // Long enough that a clock running across the blocked send would have fired three times.
        tokio::time::sleep(idle * 3).await;
        expect_still_open(&mut client, "a connection blocked on a full downstream").await;

        // Written while the task is parked in `Fanout::send`; these bytes sit in the socket
        // buffer.
        client.write_all(b"<13>three\n").await.unwrap();

        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>one"]);
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>two"]);
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>three"]);

        handle.abort();
    }

    /// An idle close flushes the accumulated batch and counts a buffered partial frame.
    #[tokio::test]
    async fn an_idle_close_flushes_the_accumulated_batch_and_counts_a_buffered_partial_frame_truncated(
    ) {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        // No interval timer and a high bound: only the idle close's flush delivers the frame.
        let config = TcpListenerConfig {
            batch_flush_interval: Duration::ZERO,
            ..TcpListenerConfig::default()
        };
        let (addr, listener) = bound_listener(config).await;
        let mut listener =
            listener.with_telemetry(telemetry).with_idle_timeout(Some(Duration::from_millis(100)));
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        client.write_all(b"<13>complete\n<13>half a mes").await.unwrap();

        assert_eq!(
            payloads(&recv_batch(&mut rx).await),
            vec!["<13>complete"],
            "the accumulated batch is flushed on the way out, not dropped"
        );
        expect_closed(&mut client, "a connection quiet past its idle_timeout").await;

        let drained = registry.drain(0);
        assert_eq!(
            sum_of(&drained, "logit.input.frames.dropped", Some(("reason", "truncated"))),
            Some(1.0),
            "the partial frame the idle close discarded must still be counted"
        );
        assert_eq!(
            sum_of(&drained, "logit.component.receive.flushed", Some(("reason", "closed"))),
            Some(1.0)
        );
        assert_eq!(
            sum_of(&drained, "logit.input.connections.closed", Some(("reason", "idle"))),
            Some(1.0)
        );

        handle.abort();
    }

    /// A flush tick with nothing to emit does not re-arm the idle clock.
    #[tokio::test]
    async fn a_flush_tick_does_not_reset_the_idle_clock() {
        let config = TcpListenerConfig {
            batch_max_events: 1,
            batch_flush_interval: Duration::from_millis(20),
            ..TcpListenerConfig::default()
        };
        let (addr, listener) = bound_listener(config).await;
        let mut listener = listener.with_idle_timeout(Some(Duration::from_millis(100)));
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        client.write_all(b"<13>hello\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>hello"]);

        // Several 20ms ticks land in every 100ms idle window, so a tick-shaped reset would keep
        // this open and `expect_closed` would hit its 2s ceiling.
        expect_closed(&mut client, "a quiet connection under a fast flush interval").await;

        handle.abort();
    }

    /// Progress is bytes, not frames: a sender dribbling a partial frame is not idle.
    #[tokio::test]
    async fn bytes_that_complete_no_frame_still_reset_the_idle_clock() {
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener = listener.with_idle_timeout(Some(Duration::from_millis(200)));
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        client.write_all(b"<13>").await.unwrap();
        for byte in b"drib" {
            tokio::time::sleep(Duration::from_millis(100)).await;
            client.write_all(&[*byte]).await.unwrap();
        }
        // 500ms of wall clock has passed on a 200ms idle timeout, with no frame ever completed.
        client.write_all(b"ble\n").await.unwrap();

        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>dribble"]);

        handle.abort();
    }

    /// The idle timeout fires under every framing mode.
    #[tokio::test]
    async fn the_idle_timeout_applies_under_every_framing_mode() {
        let length_prefixed = {
            let mut wire = 9u32.to_be_bytes().to_vec();
            wire.extend_from_slice(b"<13>hello");
            wire
        };
        let cases: Vec<(FramingMode, Vec<u8>)> = vec![
            (FramingMode::Rfc6587Auto, b"<13>hello\n".to_vec()),
            (FramingMode::Lines { oversize: Oversize::Fatal }, b"<13>hello\n".to_vec()),
            (FramingMode::Lines { oversize: Oversize::DrainToNextLine }, b"<13>hello\n".to_vec()),
            (FramingMode::LengthPrefixed, length_prefixed),
        ];

        for (mode, wire) in cases {
            let (addr, listener) = bound_listener(one_per_frame()).await;
            let mut listener = listener
                .with_framing(mode, MAX_FRAME_BYTES)
                .with_max_connections(1)
                .with_idle_timeout(Some(Duration::from_millis(50)));
            let (sink, mut rx) = fanout_into_channel(16);
            let (_shutdown_tx, shutdown_rx) = watch::channel(false);
            let handle =
                tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

            let mut quiet = connect(&addr).await;
            quiet.write_all(&wire).await.unwrap();
            assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>hello"]);
            expect_closed(&mut quiet, &format!("a quiet connection under {mode:?}")).await;

            let mut client = connect(&addr).await;
            client.write_all(&wire).await.unwrap();
            assert_eq!(
                payloads(&recv_batch(&mut rx).await),
                vec!["<13>hello"],
                "the permit must have come back under {mode:?}"
            );

            drop(quiet);
            handle.abort();
        }
    }

    /// With no `idle_timeout`, a quiet connection is never closed or counted idle.
    #[tokio::test]
    async fn no_idle_timeout_means_a_quiet_connection_is_never_closed() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let config = TcpListenerConfig {
            batch_max_events: 1,
            batch_flush_interval: Duration::from_millis(20),
            ..TcpListenerConfig::default()
        };
        let (addr, listener) = bound_listener(config).await;
        // No `with_idle_timeout` call.
        let mut listener = listener.with_telemetry(telemetry);
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        client.write_all(b"<13>first\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>first"]);

        // Fifteen flush ticks of silence.
        tokio::time::sleep(Duration::from_millis(300)).await;
        expect_still_open(&mut client, "a quiet connection with no idle_timeout").await;
        client.write_all(b"<13>much later\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>much later"]);

        assert_eq!(
            sum_of(&registry.drain(0), "logit.input.connections.closed", Some(("reason", "idle"))),
            None,
            "nothing was closed as idle, so the counter was never touched"
        );

        handle.abort();
    }

    // ---- the kernel's accept queue (`AcceptQueueSampler`) --------------------------------------

    /// A running listener reports the accept-queue gauges with no configuration.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn the_kernel_accept_queue_gauges_are_reported_for_a_running_listener() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener = listener.with_telemetry(telemetry);
        let (sink, mut rx) = fanout_into_channel(8);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        // A delivered frame proves its accept happened, and so the sample before it.
        let mut client = connect(&addr).await;
        client.write_all(b"<13>hello\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>hello"]);

        let events = registry.drain(0);
        let depth = gauge_of(&events, "logit.input.accept_queue.depth")
            .expect("the accept-queue depth should be gauged before each accept");
        let limit = gauge_of(&events, "logit.input.accept_queue.limit")
            .expect("the backlog ceiling should be gauged in its own right, not left implicit");
        let utilization = gauge_of(&events, "logit.input.accept_queue.utilization")
            .expect("the utilization gauge's presence means the kernel reported a real backlog");
        assert!(depth >= 0.0, "a queue depth is never negative, got {depth}");
        assert!(limit > 0.0, "a listening socket always has a backlog ceiling, got {limit}");
        // No upper bound of 1.0: the ratio can exceed it
        // (`an_over_full_accept_queue_reports_a_utilization_above_one`).
        assert!(utilization >= 0.0, "utilization is depth/backlog, never negative: {utilization}");
        assert!(
            (utilization - depth / limit).abs() < 1e-9,
            "the three must be consistent: utilization is exactly depth/limit"
        );

        handle.abort();
    }

    /// A sampler with nothing to read disables itself on its first call.
    #[tokio::test]
    async fn an_accept_queue_sampler_that_cannot_read_the_queue_disables_itself() {
        let listener = TokioTcpListener::bind("127.0.0.1:0").await.expect("should bind loopback");
        let mut sampler = AcceptQueueSampler::new(Telemetry::default(), Diagnostics::default());
        // The non-Linux shape.
        sampler.read_queue = |_| Err(sockstat::Unavailable::NotLinux);

        assert!(sampler.enabled, "a fresh sampler always tries once");
        sampler.sample_once(&listener);
        assert!(!sampler.enabled, "one failed read is enough -- these fields never appear later");
        sampler.sample_once(&listener); // still a harmless no-op
        assert!(!sampler.enabled);
    }

    /// A disabled sampler still accepts, arms no timer, and records nothing.
    #[tokio::test]
    async fn a_disabled_accept_queue_sampler_still_accepts_and_records_nothing() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let listener = TokioTcpListener::bind("127.0.0.1:0").await.expect("should bind loopback");
        let addr = listener.local_addr().expect("a bound listener has an address");
        let mut sampler = AcceptQueueSampler::new(telemetry, Diagnostics::default());
        sampler.read_queue = |_| Err(sockstat::Unavailable::NotLinux);

        let client = tokio::spawn(async move { TcpStream::connect(addr).await });
        let (_accepted, _peer) = sampler
            .accept(&listener)
            .await
            .expect("a disabled sampler must still accept exactly as a bare accept() would");
        client.await.expect("the connect task should not panic").expect("connect should succeed");
        assert!(sampler.tick.is_none(), "a disabled sampler arms no timer at all");

        let events = registry.drain(0);
        assert_eq!(gauge_of(&events, "logit.input.accept_queue.depth"), None);
        assert_eq!(gauge_of(&events, "logit.input.accept_queue.limit"), None);
        assert_eq!(gauge_of(&events, "logit.input.accept_queue.utilization"), None);
    }

    /// A `listen(1)` socket's queue overshoots its ceiling and the gauge reports it unclamped.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn an_over_full_accept_queue_reports_a_utilization_above_one() {
        use socket2::{Domain, Socket, Type};

        let addr: std::net::SocketAddr = "127.0.0.1:0".parse().expect("a literal address");
        let server = Socket::new(Domain::IPV4, Type::STREAM, None).expect("socket(2)");
        server.bind(&addr.into()).expect("bind to an ephemeral loopback port");
        // `sk_acceptq_is_full` (`include/net/sock.h`, v6.12) is `sk_ack_backlog >
        // sk_max_ack_backlog`, strictly greater (kernel commit 64a146513f8f), and
        // `inet_csk_reqsk_queue_add` increments after that check with no second test. So a
        // `listen(1)` socket admits two connections and settles at depth 2: `limit + 1`. The
        // third connect's handshake is dropped, and nothing ever calls `accept`.
        server.listen(1).expect("listen(2) with a backlog of exactly one");
        server.set_nonblocking(true).expect("tokio requires a nonblocking listener");
        let bound = server
            .local_addr()
            .expect("a bound socket has an address")
            .as_socket()
            .expect("an AF_INET address");

        let mut clients = Vec::new();
        for _ in 0..3 {
            let client = Socket::new(Domain::IPV4, Type::STREAM, None).expect("socket(2)");
            client.set_nonblocking(true).expect("a blocking connect could hang on a full queue");
            // `EINPROGRESS` is the expected answer for all three; the handshake finishes (or does
            // not) in the kernel while this test waits below.
            let _ = client.connect(&bound.into());
            clients.push(client);
        }

        let listener = TokioTcpListener::from_std(std::net::TcpListener::from(server))
            .expect("a nonblocking listening socket is a valid tokio listener");
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let mut sampler = AcceptQueueSampler::new(telemetry, Diagnostics::default());

        // Poll rather than sleep a fixed time. If both handshakes haven't finished in the window,
        // only the `> 1.0` half below is skipped.
        let mut depth = 0;
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            let (d, _limit) = read_listen_queue(&listener).expect("TCP_INFO on a real listener");
            depth = d;
            if depth >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let (queued, limit) = read_listen_queue(&listener).expect("TCP_INFO on a real listener");
        assert_eq!(
            limit, 1,
            "listen(1) is what this socket asked for, and somaxconn cannot \
                              raise it -- only lower it, and never below 1"
        );
        assert!(
            queued <= limit + 1,
            "the kernel admits one connection past its own ceiling and no more, got {queued}"
        );

        sampler.sample_once(&listener);
        let events = registry.drain(0);
        let reported_depth = gauge_of(&events, "logit.input.accept_queue.depth")
            .expect("the depth should have been gauged");
        let reported_limit = gauge_of(&events, "logit.input.accept_queue.limit")
            .expect("the ceiling should have been gauged");
        let utilization = gauge_of(&events, "logit.input.accept_queue.utilization")
            .expect("the utilization should have been gauged");
        assert_eq!(reported_limit, 1.0);
        assert!(
            (utilization - reported_depth / reported_limit).abs() < 1e-9,
            "utilization is exactly depth/limit, unclamped: {utilization} vs \
             {reported_depth}/{reported_limit}"
        );
        if depth >= 2 {
            assert!(
                utilization > 1.0,
                "a listen(1) socket holding two connections is over its ceiling, and the gauge \
                 must say so rather than clamp: {utilization}"
            );
        } else {
            eprintln!(
                "SKIPPED the >1.0 half: this kernel left the accept queue at depth {depth} \
                 within the poll window; the depth bound and the depth/limit identity were still \
                 checked"
            );
        }

        drop(clients);
    }

    /// The queue is sampled **before** the accept, not after it.
    #[tokio::test]
    async fn the_accept_queue_is_sampled_before_the_accept_not_after_it() {
        static SAMPLES: AtomicUsize = AtomicUsize::new(0);
        SAMPLES.store(0, Ordering::SeqCst);

        let listener = TokioTcpListener::bind("127.0.0.1:0").await.expect("should bind loopback");
        let addr = listener.local_addr().expect("a bound listener has an address");
        let mut sampler = AcceptQueueSampler::new(Telemetry::default(), Diagnostics::default());
        sampler.read_queue = |_| {
            SAMPLES.fetch_add(1, Ordering::SeqCst);
            Ok((0, 1))
        };

        let accepting = tokio::spawn(async move {
            sampler.accept_every(&listener, Duration::from_secs(3600)).await
        });

        // Nothing has connected, and an hour's interval cannot tick: a sample here can only be
        // the pre-accept one.
        tokio::time::timeout(Duration::from_secs(5), async {
            while SAMPLES.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the queue must be sampled while accept() is still parked, not after it returns");

        TcpStream::connect(addr).await.expect("loopback connect should succeed");
        accepting
            .await
            .expect("the accept task should not panic")
            .expect("the connection should still be accepted");
    }

    /// An idle listener is still sampled on the interval.
    #[tokio::test]
    async fn an_idle_listener_is_sampled_once_per_interval() {
        static SAMPLES: AtomicUsize = AtomicUsize::new(0);
        SAMPLES.store(0, Ordering::SeqCst);

        let listener = TokioTcpListener::bind("127.0.0.1:0").await.expect("should bind loopback");
        let mut sampler = AcceptQueueSampler::new(Telemetry::default(), Diagnostics::default());
        sampler.read_queue = |_| {
            SAMPLES.fetch_add(1, Ordering::SeqCst);
            Ok((0, 1))
        };

        // Nothing ever connects, so this only returns by timing out.
        let _ = tokio::time::timeout(
            Duration::from_millis(250),
            sampler.accept_every(&listener, Duration::from_millis(20)),
        )
        .await;

        let samples = SAMPLES.load(Ordering::SeqCst);
        assert!(
            samples >= 4,
            "250 ms at a 20 ms interval must sample many times over with no connection at all, \
             got {samples}"
        );
    }

    /// A steady accept rate faster than the interval does not starve the interval tick
    /// ([`AcceptQueueSampler::tick`]).
    #[tokio::test]
    async fn a_steady_stream_of_accepts_does_not_starve_the_interval_tick() {
        static SAMPLES: AtomicUsize = AtomicUsize::new(0);
        SAMPLES.store(0, Ordering::SeqCst);

        const ACCEPTS: usize = 20;
        const TICK: Duration = Duration::from_millis(20);

        let listener = TokioTcpListener::bind("127.0.0.1:0").await.expect("should bind loopback");
        let addr = listener.local_addr().expect("a bound listener has an address");
        let mut sampler = AcceptQueueSampler::new(Telemetry::default(), Diagnostics::default());
        sampler.read_queue = |_| {
            SAMPLES.fetch_add(1, Ordering::SeqCst);
            Ok((0, 1))
        };

        // Every connection waits in the backlog, so each `accept_every` returns immediately and
        // no single call ever waits a whole interval.
        let mut clients = Vec::new();
        for _ in 0..ACCEPTS {
            clients.push(TcpStream::connect(addr).await.expect("loopback connect"));
        }

        for _ in 0..ACCEPTS {
            let _accepted = sampler
                .accept_every(&listener, TICK)
                .await
                .expect("every queued connection is accepted");
            // Far faster than the interval: a per-turn `sleep` would never come due.
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let samples = SAMPLES.load(Ordering::SeqCst);
        assert!(
            samples > ACCEPTS + 1,
            "{ACCEPTS} accepts spread over ~{}ms must also carry interval ticks -- one sample per \
             accept and no more means the timer was restarted on every loop turn and never came \
             due, got {samples}",
            ACCEPTS * 5
        );
    }
}
