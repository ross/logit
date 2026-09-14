//! The shared TCP (optionally TLS) listener driver: an accept loop, one connection task per
//! peer, framing, and frame->batch assembly -- the stream-transport twin of
//! [`crate::udp::UdpListener`] (`docs/adr/decoupled-listener-io.md`).
//!
//! **Why a generic driver rather than a `syslog_in`-shaped accept loop.** `syslog_in` over TCP was
//! the first caller (`docs/plans/syslog-tls.md`), but nothing below mentions syslog: the same
//! accept loop, connection cap, TLS termination and batching apply unchanged to any
//! newline-or-length-framed stream protocol. `graphite_in` is the second caller
//! (`docs/adr/graphite-carbon-relay.md`'s amendment), `statsd_in` the next. Generic over the
//! decoder for exactly the reason [`crate::udp::UdpListener`] is -- that is the only thing two
//! such listeners ever differ in.
//!
//! **Framing is chosen per listener, not guessed per driver.** [`FramingMode`] is set once, at
//! construction, through [`TcpListener::with_framing`]: RFC 6587's auto-detecting pair for
//! `syslog_in`, LF-delimited lines for a line protocol whose messages may legitimately *start*
//! with a digit (`graphite_in` plaintext, `statsd_in`), or carbon's 4-byte big-endian length
//! prefix. A builder rather than a [`TcpListenerConfig`] field: that struct is the image of the
//! `receive:` config block, and framing is not something an operator sets.
//!
//! **`D: Clone` is load-bearing.** Every connection gets its own decoder clone, because a decoder
//! may hold real per-connection state (a future decoder's scratch buffers or sticky identity, the
//! way `collectd`'s already works per datagram; `SyslogDecoder`'s clonable state today is only its
//! `Diagnostics`, whose counts every clone shares). Sharing one decoder across connections behind
//! a lock would serialize every connection's decode against every other's; cloning keeps each
//! connection independent.
//!
//! **No receive queue.** Unlike the UDP driver, there is no [`crate::udp::ReceiveQueue`] here and
//! no `receive.max_datagrams`/`max_bytes`/`overflow` to configure. TCP's own flow control *is* the
//! queue: a connection whose downstream has stalled simply stops being read, the kernel window
//! closes, and the sender blocks -- which is the correct behaviour for a reliable transport, where
//! dropping bytes to keep reading (the UDP driver's `drop_oldest` default) would corrupt the frame
//! stream rather than lose one self-contained datagram.
//!
//! **Batching is per connection.** Each connection task owns its own
//! [`logit_pipeline::BatchAccumulator`], so `batch_max_events` bounds one connection's in-flight
//! events, not the listener's -- N concurrent connections can hold N times that. See
//! [`TcpListenerConfig::batch_max_events`].
//!
//! **Connection limit.** A [`tokio::sync::Semaphore`] with `try_acquire_owned`, capped at
//! [`MAX_CONCURRENT_CONNECTIONS`], exactly as `logit_in`
//! (`crates/logit-inputs/src/logit.rs`'s "Connection limit" section) -- reject, don't queue.
//! **The one deliberate difference from `logit_in`:** there, a past-the-cap connection is wrapped
//! in TLS first so it can be told *why* it is being closed (a `Reject` control frame). Syslog over
//! TCP has no in-band reject message of any kind, so there is nothing to say and no reason to
//! spend a handshake saying it -- a past-the-cap connection here is dropped immediately, before
//! any TLS accept, and counted as `logit.input.connections.rejected{reason="limit"}`. The
//! `logit.input.connections` gauge correspondingly counts permit holders only.
//!
//! **Pre-handshake timeout.** [`HANDSHAKE_TIMEOUT`] -- the default behind `syslog_in`'s
//! operator-facing `handshake_timeout:` field, which overrides it via
//! [`TcpListener::with_handshake_timeout`] -- bounds each of a connection's two pre-message
//! phases *independently*, exactly as `logit_in` bounds its own two: the TLS accept (in the accept
//! loop's `Some` arm, when TLS is configured) and then the wait for the connection's very first
//! byte, inside [`serve_connection`], which starts a fresh budget of the same length rather than
//! inheriting a shared deadline. So on the TLS path the worst case is two of these back to back --
//! 10s at the default -- before a connection that has said nothing gives up its permit.
//!
//! **The first-byte bound applies on both arms, plaintext included.** It has to: `syslog_in` with
//! no `tls:` block is the default shape, and without it 1024 connections that complete the TCP
//! handshake and then send nothing would hold every permit forever, at a cost to the peer of 1024
//! SYNs and no bytes. The bound is on the *first* byte specifically -- i.e. until
//! [`Framer::first_byte_seen`] is true -- because that is the phase with no legitimate reason to
//! be slow; after it there is deliberately *no* idle timeout, so a connection that sent one frame
//! and then went quiet holds its permit indefinitely, the same known gap `otlp_in` has
//! (`docs/known-gaps.md`).
//!
//! `first_byte_seen`, and not "has the framer latched a [`Framing`] yet": only
//! [`FramingMode::Rfc6587Auto`] has anything to latch, so under either explicit mode a
//! latch-shaped predicate would read "already framed" on a connection that has not sent a byte,
//! and the deadline would silently never fire. The test
//! `the_first_byte_deadline_applies_under_every_framing_mode` is the pin.

use crate::Input;
use bytes::{Bytes, BytesMut};
use logit_core::{Diagnostics, Event, EventBatch, Telemetry};
use logit_pipeline::{BatchAccumulator, Fanout, FlushReason};
use logit_proto::graphite::pickle::LENGTH_PREFIX_BYTES;
use logit_proto::Decoder;
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;

/// `crate::tls::TlsServerSettings`, re-exported here for symmetry with `crate::logit`/`crate::otlp`
/// (all three listeners share the one definition in `crate::tls`).
pub use crate::tls::TlsServerSettings;

/// See this module's "Connection limit" doc section. The same number `logit_in` and `otlp_in` use
/// -- there is no protocol reason for a syslog listener to differ, and one shared figure is one
/// thing for an operator to learn.
const MAX_CONCURRENT_CONNECTIONS: usize = 1024;

/// How long a connection has, per pre-message phase, before this listener gives up on it and
/// releases its connection-limit permit: the TLS accept when TLS is configured, and -- on both
/// arms, plaintext included -- the wait for the connection's first byte. Each phase gets its own
/// budget of this length, so a TLS connection that says nothing at all costs two of them. See this
/// module's "Pre-handshake timeout" doc section.
///
/// The *default* only: `syslog_in`'s `handshake_timeout:` config field overrides it through
/// [`TcpListener::with_handshake_timeout`]. `logit_config`'s own `default_handshake_timeout`
/// mirrors this number by hand (it cannot depend on this crate).
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// The largest single frame this driver will assemble, in bytes, for any framing -- the
/// **default** behind [`TcpListener::with_framing`]'s second argument, and what a listener that
/// never calls it gets.
///
/// Not configurable on `syslog_in`, deliberately (`graphite_in` overrides it with its own
/// operator-facing `max_line_bytes`/`max_frame_bytes`, which carbon's own receivers expose and
/// whose pickle default is a megabyte). It is *not* `syslog_out`'s `max_message_bytes` (8192): that is a
/// sender-side knob an operator may legitimately raise, and a receiver whose ceiling tracked it
/// would have to be re-tuned in lockstep with every sender on the network. 64 KiB instead, which
/// is where the UDP driver's own 65507-byte read buffer already puts the practical per-message
/// ceiling for the same protocols -- generous against RFC 5424's own "no upper limit, but a
/// receiver MUST be able to accept 2048 octets" and against every real sender's default.
pub const MAX_FRAME_BYTES: usize = 65_536;

/// Bytes pulled off the socket per read. Deliberately well under [`MAX_FRAME_BYTES`]: a listener
/// with many idle connections pays this per connection, and a frame larger than one read is
/// assembled across reads by [`Framer`] regardless.
const READ_BUFFER_BYTES: usize = 8 * 1024;

// ---- framing ---------------------------------------------------------------------------------

/// How a [`Framer`] delimits one connection's messages. Chosen once per listener, through
/// [`TcpListener::with_framing`], and never re-evaluated.
///
/// Explicit rather than "always sniff the first byte" because the sniff is only sound for syslog:
/// it reads a leading ASCII digit as an RFC 6587 octet count, which is right for a protocol whose
/// every non-transparent message starts `<`, and catastrophically wrong for one whose lines
/// routinely start with a digit -- `1.hits:1|c` (statsd), or a carbon path beginning with a host
/// number. A line protocol says so instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FramingMode {
    /// RFC 6587's two framings, auto-detected from the connection's first byte and latched for its
    /// life (`docs/adr/syslog-tcp-ingress-and-tls.md`). `syslog_in`'s mode, and nothing else's.
    Rfc6587Auto,
    /// LF-delimited lines only, never octet-counting, whatever the first byte is. `graphite_in`'s
    /// plaintext mode; `statsd_in`'s.
    Lines {
        /// What a line past the frame bound does -- see [`Oversize`].
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
    /// longer, and under octet counting there is no resync point at all.
    Fatal,
    /// Skip that one line and resynchronize at the next `LF`, counting the skip **once**. Carbon's
    /// own behaviour (`docs/adr/graphite-carbon-relay.md`), and the right call for a metrics line
    /// protocol: one pathological datapoint must not cost a busy relay's whole connection, and an
    /// LF-delimited stream has an unambiguous resync point that a length-framed one does not.
    DrainToNextLine,
}

/// Which framing a connection is actually speaking, once known.
///
/// Under [`FramingMode::Rfc6587Auto`] this is latched from the very first byte a connection sends
/// and never re-evaluated (`docs/adr/syslog-tcp-ingress-and-tls.md`): an ASCII digit can only
/// begin an octet count, since a non-transparent syslog frame always begins `<` (the PRI's opening
/// angle bracket). Anything else is non-transparent. Under either explicit mode it is fixed at
/// construction and nothing is sniffed.
///
/// Note the latch keys on "ASCII digit", not on `1`-`9`, even though RFC 6587 §3.4.1's `MSG-LEN =
/// NONZERO-DIGIT *DIGIT` forbids a leading zero. A leading `0` is a malformed octet count, not a
/// non-transparent frame, so latching it here and failing loudly in
/// [`Framer::next_frame`] is the honest reading -- treating it as non-transparent would silently
/// mis-frame a broken sender's whole stream instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Framing {
    /// RFC 6587 §3.4.1: `MSG-LEN SP MSG`, where `MSG-LEN` is the octet count of `MSG`. The only
    /// framing that can carry a message containing a newline.
    OctetCounting,
    /// RFC 6587 §3.4.2: messages separated by a trailing `LF` (a `CR` before it is stripped). Also
    /// what [`FramingMode::Lines`] speaks, from the first byte, with no octet-counting sibling to
    /// be mistaken for.
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

/// Why [`Framer`] could not produce the next frame. All but [`FrameError::OversizeSkipped`] are
/// fatal *to the connection* ([`FrameError::is_fatal`]): neither RFC 6587 framing nor a
/// length-prefixed one can resynchronize after one (an octet count that cannot be trusted leaves
/// no way to know where the next frame starts, a declared length past the ceiling has nothing
/// buffered after it, and a line past the size ceiling would only get longer), so the driver
/// counts it, diagnoses it, and closes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
    /// A frame larger than this listener's frame bound -- a declared octet count or length prefix
    /// above it, or a line that passed it without a terminator under [`Oversize::Fatal`].
    Oversize(String),
    /// An octet count that is not one RFC 6587 §3.4.1's `MSG-LEN = NONZERO-DIGIT *DIGIT`
    /// production permits: a non-digit before the SP, a leading zero (a zero count included), or
    /// more than nine digits.
    Malformed(String),
    /// The peer closed mid-frame under a framing whose declared length says bytes are missing
    /// (octet counting, or a length prefix). Distinct from the two above in that nothing was wrong
    /// with what the peer *sent* -- it just stopped -- and distinct from the LF-delimited EOF
    /// case, where a terminator-less remainder is a perfectly ordinary final message and is
    /// emitted rather than dropped.
    Truncated(String),
    /// One line past the frame bound under [`Oversize::DrainToNextLine`]: dropped, counted, and
    /// resynchronized at the next `LF`. The one **non-fatal** variant -- the connection stays open
    /// and the line after it still decodes.
    OversizeSkipped(String),
}

impl FrameError {
    /// The `reason` tag on `logit.input.frames.dropped`
    /// (`docs/design/internal-telemetry.md`'s "Naming" section). `OversizeSkipped` shares
    /// `oversize` with its fatal sibling on purpose: an operator watching the counter cares that a
    /// frame was too big, and `is_fatal` is what says whether the connection survived it.
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

/// Frame extraction over a byte stream -- pure, socket-free and synchronous, so it is directly
/// unit-testable and a recorded interop fixture can be replayed through it byte for byte
/// (`docs/plans/recorded-interop-fixtures.md`) without standing anything up.
///
/// Usage is `push` whatever came off the socket, then `next_frame` in a loop until it returns
/// `Ok(None)`; at EOF, [`Framer::finish`] once for whatever partial frame is left.
pub struct Framer {
    /// How this connection's messages are delimited -- fixed at construction.
    mode: FramingMode,
    /// The largest single frame this connection will assemble. Per listener, not a constant:
    /// `syslog_in`/`statsd_in` take [`MAX_FRAME_BYTES`], a `graphite_in` takes its operator-facing
    /// `max_line_bytes`/`max_frame_bytes`.
    max_frame_bytes: usize,
    /// `None` only under [`FramingMode::Rfc6587Auto`] before the first byte arrives -- see
    /// [`Framing`]'s doc comment for the latch rule. Both explicit modes set it at construction.
    framing: Option<Framing>,
    buf: BytesMut,
    /// How far into `buf` the line path has already looked for a `LF` without finding one. Reset
    /// whenever a frame is taken. Without it, a long line arriving over many reads would be
    /// rescanned from the start on every read -- O(n^2) in the line's own length.
    scanned: usize,
    /// Set when a line passed the bound with no `LF` under [`Oversize::DrainToNextLine`]:
    /// everything up to and including the next `LF` belongs to that abandoned line and is
    /// discarded uncounted (the skip was counted once, when the bound was crossed).
    draining: bool,
    /// Whether this connection has ever produced a byte. The first-byte deadline's predicate
    /// ([`Self::first_byte_seen`]) -- *not* `framing.is_none()`, which only ever means anything
    /// under [`FramingMode::Rfc6587Auto`].
    seen_bytes: bool,
}

impl Framer {
    /// A framer speaking `mode`, refusing any single frame larger than `max_frame_bytes`.
    ///
    /// No `Default`: both arguments are real per-listener decisions (a `graphite_in` plaintext
    /// connection bounds lines at `max_line_bytes` and drains past them; a `syslog_in` connection
    /// bounds RFC 6587 frames at [`MAX_FRAME_BYTES`] and closes), and a default would silently
    /// pick syslog's.
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

    /// Whether this connection has ever produced a byte -- the first-byte deadline's predicate
    /// (this module's "Pre-handshake timeout" doc section). Distinct from
    /// `framing().is_some()`, which is true from construction under both explicit modes and so
    /// would make that deadline inert on every listener but `syslog_in`.
    pub fn first_byte_seen(&self) -> bool {
        self.seen_bytes
    }

    /// Bytes held but not yet formed into a frame. Read by `report_buffered_tail` on the paths
    /// that end a connection without ever reaching [`Framer::finish`] -- a peer RST mid-message,
    /// or shutdown -- so a discarded partial frame is still counted rather than vanishing.
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

    /// The next complete frame, if one is fully buffered. `Ok(None)` means "need more bytes", not
    /// "end of stream" -- only the caller knows the socket closed, and says so via
    /// [`Framer::finish`].
    ///
    /// The returned [`Bytes`] is the message *verbatim*: under octet counting exactly the declared
    /// MSG-LEN bytes (an embedded `LF` is payload, not a terminator), under non-transparent the
    /// line with its `LF` and at most one preceding `CR` removed.
    pub fn next_frame(&mut self) -> Result<Option<Bytes>, FrameError> {
        loop {
            match self.framing {
                None => return Ok(None),
                Some(Framing::OctetCounting) => return self.next_octet_counted(),
                Some(Framing::LengthPrefixed) => return self.next_length_prefixed(),
                Some(Framing::NonTransparent) => match self.next_line()? {
                    // An empty line carries no message. Senders emit them (a stray `LF` after a
                    // `CRLF`-terminated message, a keepalive newline), and RFC 6587 §3.4.2 has
                    // nothing for a receiver to do with one -- skip it and look for the next,
                    // rather than handing the decoder an empty frame to reject.
                    Some(line) if line.is_empty() => continue,
                    other => return Ok(other),
                },
            }
        }
    }

    /// Whatever is left when the peer closes. Under LF-delimited framing a terminator-less
    /// remainder is an ordinary final message and is returned; under octet counting or a length
    /// prefix a partial payload is [`FrameError::Truncated`], since its declared length says bytes
    /// are missing.
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
                // as skipped -- delivering them would emit half a datapoint.
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
                Ok(if line.is_empty() { None } else { Some(line) })
            }
        }
    }

    /// What a line past [`Self::max_frame_bytes`] costs -- [`Oversize::Fatal`] everywhere but
    /// [`FramingMode::Lines`], which says so for itself.
    fn oversize_policy(&self) -> Oversize {
        match self.mode {
            FramingMode::Lines { oversize } => oversize,
            FramingMode::Rfc6587Auto | FramingMode::LengthPrefixed => Oversize::Fatal,
        }
    }

    /// RFC 6587 §3.4.2 (and [`FramingMode::Lines`]): everything up to the next `LF`, with at most
    /// one preceding `CR` removed.
    fn next_line(&mut self) -> Result<Option<Bytes>, FrameError> {
        // Finishing an abandoned line from a previous call, before anything else is looked at:
        // every byte up to and including the next `LF` still belongs to it.
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
                    // Nothing after it has arrived, so there is no resync point *yet*: abandon
                    // what is buffered and discard bytes until the `LF` that ends this line.
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
                // The terminator is already buffered, so this line's end is known: drop exactly
                // it, and the next line is framed normally with no drain state at all.
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
    /// payload bytes. The prefix is validated and stripped here, so the decoder is handed exactly
    /// one already-unframed payload -- which is what `GraphiteDecoder`'s pickle path expects
    /// (`logit_proto::graphite::decode`'s module doc: framing is the listener's job).
    ///
    /// A declared length past the bound is [`FrameError::Oversize`] and therefore fatal: nothing
    /// after it has been read, so unlike an LF-delimited stream there is no resync point to skip
    /// forward to. A short buffer is `Ok(None)` -- the rest of the frame has not arrived yet.
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

    /// RFC 6587 §3.4.1: `MSG-LEN SP MSG`, where `MSG-LEN = NONZERO-DIGIT *DIGIT` -- so a leading
    /// zero (and therefore a count of zero) is malformed, not a zero-length message.
    ///
    /// At most nine digits, rather than "as many as fit": [`MAX_FRAME_BYTES`] needs five, so nine
    /// is already far past any legitimate count, and an explicit ceiling is what turns "a peer
    /// that sent digits forever" from an unbounded buffer into a bounded, diagnosable
    /// [`FrameError::Malformed`]. The size ceiling itself is this framer's own `max_frame_bytes`,
    /// which is [`MAX_FRAME_BYTES`] on the one listener that speaks this framing.
    fn next_octet_counted(&mut self) -> Result<Option<Bytes>, FrameError> {
        const MAX_COUNT_DIGITS: usize = 9;

        let mut digits = 0usize;
        loop {
            match self.buf.get(digits) {
                // Not enough bytes to know yet -- `digits <= MAX_COUNT_DIGITS` here, so this waits
                // for at most one more byte before the checks below fire.
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

        // Unreachable through `next_frame` (the framing only latches to `OctetCounting` on a
        // leading digit), but this function is reachable directly from a test and a zero-digit
        // count is malformed either way.
        if digits == 0 {
            return Err(FrameError::Malformed("an octet count with no digits".to_string()));
        }
        // RFC 6587 §3.4.1's `NONZERO-DIGIT` first character. `0` alone and `012` are both rejected
        // here, the second before it can be read as 12 -- a sender that pads its counts is not
        // speaking this framing, and guessing at its intent would silently mis-frame the rest of
        // the stream.
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

/// Removes one trailing `CR`, so a `CRLF`-terminated sender and an `LF`-terminated one hand the
/// decoder the identical message. Only one: a message genuinely ending in `CR CR` keeps the first.
fn strip_cr(line: Bytes) -> Bytes {
    match line.last() {
        Some(b'\r') => line.slice(..line.len() - 1),
        _ => line,
    }
}

// ---- the listener ----------------------------------------------------------------------------

/// [`TcpListener`]'s runtime knobs -- [`crate::udp::UdpListenerConfig`] minus every queue field
/// (see this module's "No receive queue" doc section), with the four batching/shutdown fields at
/// byte-for-byte the same defaults.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TcpListenerConfig {
    /// Events to accumulate **per connection** before one `Fanout::send`. `1` means one send per
    /// frame. Because each connection accumulates independently (this module's "Batching is per
    /// connection" doc section), the listener's worst-case in-flight event count is this times the
    /// number of live connections, not this.
    pub batch_max_events: usize,
    /// The same bound by estimated heap bytes, also **per connection**.
    pub batch_max_bytes: u64,
    /// `Duration::ZERO` disables the flush timer entirely; bounds are then the only trigger.
    pub batch_flush_interval: Duration,
    /// How long [`TcpListener::run_until_shutdown`] keeps draining after shutdown fires before
    /// [`logit_pipeline::runtime::run_input`]'s grace backstop cancels it by drop.
    pub shutdown_grace: Duration,
}

/// The same numbers as [`crate::udp::UdpListenerConfig::default`]'s corresponding fields --
/// `docs/adr/decoupled-listener-io.md` justifies them, and a TCP listener has no reason to batch
/// differently from a UDP one.
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
/// decoded events -- the stream twin of [`crate::udp::UdpListener`]. See this module's own doc
/// comment for the accept/cap/handshake/framing/batching contracts.
pub struct TcpListener<D: Decoder + Clone + Send + 'static> {
    bind: String,
    decoder: D,
    config: TcpListenerConfig,
    diag: Diagnostics,
    telemetry: Telemetry,
    /// How every connection's messages are delimited, and the bound on one of them. See
    /// [`Self::with_framing`]; [`FramingMode::Rfc6587Auto`] + [`MAX_FRAME_BYTES`] by default.
    framing: FramingMode,
    max_frame_bytes: usize,
    tls: Option<Arc<rustls::ServerConfig>>,
    /// Set by [`Input::bind`], taken back out by [`Input::run_until_shutdown`] -- the same
    /// bind-pre-pass shape `otlp_in` uses (`docs/plans/operator-surface.md`, workstream B), rather
    /// than `logit_in`'s bind-inside-`run_until_shutdown`, so a test (and `logit run`'s startup
    /// ordering) can learn the real address before anything is spawned.
    listener: Option<TokioTcpListener>,
    max_connections: usize,
    handshake_timeout: Duration,
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
        }
    }

    /// The address actually bound, once [`Input::bind`] has run -- lets a test learn the
    /// OS-assigned port without a bind-drop-rebind race.
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        self.listener.as_ref().and_then(|l| l.local_addr().ok())
    }

    /// Sets *this listener's own* diagnostics -- the `connection_error`, `bad_frame` and
    /// `framing_error` keys reported below. Does **not** reach `self.decoder`'s own diagnostics
    /// field, if it has one; see [`crate::udp::UdpListener::with_diagnostics`]'s doc comment for
    /// the full reasoning, and use [`Self::map_decoder`] to propagate the same value into a
    /// concrete decoder that needs it.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Applies `f` to the wrapped decoder -- lets a caller that knows the concrete decoder type
    /// chain that decoder's own consuming builder methods through this builder-style API, exactly
    /// as [`crate::udp::UdpListener::map_decoder`] does.
    pub fn map_decoder(mut self, f: impl FnOnce(D) -> D) -> Self {
        self.decoder = f(self.decoder);
        self
    }

    /// Overrides the batching/shutdown-grace knobs -- what a `receive:` config block sets.
    /// Defaults to [`TcpListenerConfig::default`] when never called.
    pub fn with_config(mut self, config: TcpListenerConfig) -> Self {
        self.config = config;
        self
    }

    /// The currently-configured batching/shutdown-grace knobs -- for test introspection, mirroring
    /// [`crate::udp::UdpListener::config`].
    pub fn config(&self) -> TcpListenerConfig {
        self.config
    }

    /// Turns on TLS termination for this listener (`tls:` in config) -- no ALPN, for the same
    /// reason `logit_in` passes none (`crates/logit-inputs/src/logit.rs::with_tls`): this isn't an
    /// HTTP-shaped protocol, so there's nothing for a client to negotiate down to. Every path in
    /// `settings` is resolved against `base_dir` (the config file's own directory).
    pub fn with_tls(
        mut self,
        settings: &TlsServerSettings,
        base_dir: &Path,
    ) -> anyhow::Result<Self> {
        self.tls = Some(Arc::new(crate::tls::build_server_config(settings, base_dir, &[])?));
        Ok(self)
    }

    /// This listener's own diagnostics -- test-only, the driver-half counterpart of
    /// [`Self::decoder`] below: a wrapper's `with_diagnostics` has to set both, and only an
    /// accessor on each can prove it did (`crate::syslog`'s own regression test).
    #[cfg(test)]
    pub(crate) fn diag(&self) -> &Diagnostics {
        &self.diag
    }

    /// The wrapped decoder -- test-only, and for exactly the reason
    /// [`crate::udp::UdpListener::decoder`] exists: a wrapper's `with_diagnostics` has to reach
    /// the decoder's own `Diagnostics` as well as this listener's, and only an accessor can prove
    /// it did (`crate::syslog`'s own regression test).
    #[cfg(test)]
    pub(crate) fn decoder(&self) -> &D {
        &self.decoder
    }

    /// How this listener's connections are framed, and the largest single frame any of them will
    /// assemble. [`FramingMode::Rfc6587Auto`] with [`MAX_FRAME_BYTES`] when never called -- what
    /// `syslog_in` wants, and the only shape that existed before `graphite_in` joined this driver.
    ///
    /// A builder rather than a [`TcpListenerConfig`] field: that struct is the image of the
    /// `receive:` config block an operator writes, and framing is a property of the protocol, not
    /// of the receive pipeline. See [`FramingMode`] for why it is explicit rather than always
    /// sniffed.
    pub fn with_framing(mut self, mode: FramingMode, max_frame_bytes: usize) -> Self {
        self.set_framing(mode, max_frame_bytes);
        self
    }

    /// [`Self::with_framing`] against an already-built listener, for a wrapper that has to defer
    /// the decision until `bind()` -- `graphite_in` holds `max_line_bytes`/`max_frame_bytes` as
    /// fields and applies them there, so its own builder methods can be called in any order
    /// (`crates/logit-inputs/src/graphite/mod.rs`).
    pub(crate) fn set_framing(&mut self, mode: FramingMode, max_frame_bytes: usize) {
        self.framing = mode;
        self.max_frame_bytes = max_frame_bytes;
    }

    /// Test-only override of [`MAX_CONCURRENT_CONNECTIONS`] -- opening 1025 real connections to
    /// exercise the cap would be slow and flaky; this makes the cap reachable with two.
    /// `pub(crate)` so a wrapper's own test module (`crate::graphite`'s) can expose it too.
    #[cfg(test)]
    pub(crate) fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Overrides [`HANDSHAKE_TIMEOUT`] for both pre-message budgets (the TLS accept and the
    /// first-byte wait, on either arm) -- what `syslog_in`'s `handshake_timeout:` config field
    /// sets, through `SyslogInput::with_handshake_timeout`. The constant stays the default when
    /// this is never called; a test uses it to observe a permit actually coming back without a
    /// multi-second sleep. Graph rule 45 rejects `0s` before it can reach here.
    pub fn with_handshake_timeout(mut self, handshake_timeout: Duration) -> Self {
        self.handshake_timeout = handshake_timeout;
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
        // Never exercised in production -- `run_input` always calls `run_until_shutdown`. Present
        // because the trait requires it, mirroring `crate::udp::UdpListener::run`.
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
        // Built once outside the loop -- `TlsAcceptor::from` just wraps the `Arc<ServerConfig>`,
        // so cloning it per connection below is an `Arc` clone, not a config rebuild.
        let tls_acceptor = self.tls.clone().map(TlsAcceptor::from);
        let live_connections = Arc::new(AtomicI64::new(0));
        let handshake_timeout = self.handshake_timeout;
        let config = self.config;
        let framing = self.framing;
        let max_frame_bytes = self.max_frame_bytes;
        // All three of this listener's diagnostic keys (`connection_error`, `framing_error`,
        // `bad_frame`) throttle listener-wide through the per-connection `Diagnostics` clone
        // below: a clone shares its original's counts -- see `logit_core::Diagnostics`' type doc.
        loop {
            let (stream, _peer) = tokio::select! {
                accepted = listener.accept() => accepted?,
                _ = shutdown.wait_for(|&due| due) => return Ok(()),
            };

            // Non-blocking (`try_acquire_owned`, not `acquire_owned`): at capacity the connection
            // is closed immediately rather than queued behind a permit that may never come. And
            // it is closed *here*, before any TLS accept -- see this module's "Connection limit"
            // doc section for why this deliberately diverges from `logit_in`.
            let Ok(permit) = connection_limit.clone().try_acquire_owned() else {
                self.telemetry.count(
                    "logit.input.connections.rejected",
                    1.0,
                    &[("reason", "limit")],
                );
                drop(stream);
                continue;
            };

            // Every connection task holds its own `Fanout` clone, so the shutdown cascade
            // (`docs/adr/service-lifecycle-and-output-retry.md`) only completes once every one of
            // them has dropped -- which is what `serve_connection`'s own shutdown race guarantees.
            let sink = sink.clone();
            let mut diag = self.diag.clone();
            let telemetry = self.telemetry.clone();
            let tls_acceptor = tls_acceptor.clone();
            let conn_shutdown = shutdown.clone();
            let live_connections = Arc::clone(&live_connections);
            let decoder = self.decoder.clone();

            tokio::spawn(async move {
                // Held for exactly as long as this task runs -- a TLS accept that fails or times
                // out gives the permit back here, which is the whole point of bounding it.
                let _permit = permit;
                // One framer per connection, built from this listener's one framing decision.
                let framer = Framer::new(framing, max_frame_bytes);
                live_connections.fetch_add(1, Ordering::Relaxed);
                telemetry.gauge(
                    "logit.input.connections",
                    live_connections.load(Ordering::Relaxed) as f64,
                    &[],
                );

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
                    // No TLS to bound, but `serve_connection`'s own first-byte deadline still
                    // applies -- see this module's "Pre-handshake timeout" doc section for why
                    // this arm needs it just as much as the TLS one.
                    None => {
                        serve_connection(
                            stream,
                            decoder,
                            framer,
                            config,
                            handshake_timeout,
                            sink,
                            telemetry.clone(),
                            &mut diag,
                            conn_shutdown,
                        )
                        .await
                    }
                };

                live_connections.fetch_sub(1, Ordering::Relaxed);
                telemetry.gauge(
                    "logit.input.connections",
                    live_connections.load(Ordering::Relaxed) as f64,
                    &[],
                );

                // One connection's I/O error (a peer vanishing mid-frame, a TLS accept that failed
                // or timed out) must not be fatal to the listener or its sibling connections --
                // only `TcpListener::accept` failing in the loop above is.
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
/// wins), which is what lets both this race and the flush-deadline timeout in
/// [`serve_connection`] drop it mid-await without losing stream bytes.
///
/// `shutdown.changed()`, not `wait_for` -- `wait_for`'s `Ref` guard makes the combined future
/// `!Send`, which `tokio::spawn`ing this connection's task requires. The caller's explicit
/// `*shutdown.borrow()` check before calling this is what covers the case `changed()` alone
/// cannot: shutdown having *already* fired before this loop iteration began. Exactly the
/// discipline `crates/logit-inputs/src/logit.rs`'s own `serve_connection` documents.
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

/// Serves one already-accepted (and, under TLS, already-handshaken) connection to completion --
/// generic over the IO type so the plaintext (`TcpStream`) and TLS
/// (`tokio_rustls::server::TlsStream<TcpStream>`) cases share every line, exactly as
/// `crate::logit::serve_connection` and `crate::otlp::serve_connection` do.
///
/// Owns its own [`Framer`], [`BatchAccumulator`] and decoder clone, so nothing here is shared with
/// any sibling connection. Flushes on the accumulator's own bounds, on `batch_flush_interval`, on
/// shutdown, and on close (clean or otherwise).
///
/// `diag` is the accept loop's per-connection [`Diagnostics`] clone -- borrowed, not moved, so it
/// is still there for the `connection_error` report on whatever this returns. It is where
/// `framing_error` and `bad_frame` are reported, and those still throttle listener-wide: a clone
/// shares its original's counts (`logit_core::Diagnostics`' type doc).
///
/// `handshake_timeout` bounds the wait for this connection's *first* byte -- see this module's
/// "Pre-handshake timeout" doc section. Passed on both arms of the accept loop, TLS or not.
#[allow(clippy::too_many_arguments)] // one connection's whole context; a params struct would only move it
async fn serve_connection<S, D>(
    mut stream: S,
    mut decoder: D,
    mut framer: Framer,
    config: TcpListenerConfig,
    handshake_timeout: Duration,
    sink: Fanout,
    telemetry: Telemetry,
    diag: &mut Diagnostics,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    D: Decoder + Send,
{
    // Absolute, computed once, rather than a budget re-armed per read: the read below is re-entered
    // on every `batch_flush_interval` tick (the `Err(_elapsed) => continue` arm), so a per-read
    // budget would be reset by each 100ms tick and never actually fire. Only ever consulted while
    // `framer` has not seen a byte, i.e. before this connection's first one.
    let first_byte_deadline = tokio::time::Instant::now() + handshake_timeout;
    // Reused across every read, cleared (not replaced) between them, so its allocated capacity
    // survives from one read to the next.
    let mut read_buf = BytesMut::with_capacity(READ_BUFFER_BYTES);
    let mut accumulator = BatchAccumulator::new(config.batch_max_events, config.batch_max_bytes);
    // Reused across every `decode_into` call, cleared (not taken) between them -- see
    // `BatchAccumulator::absorb`'s own doc comment on why `std::mem::take` here would silently
    // undo the allocation win.
    let mut scratch: Vec<Event> = Vec::new();
    let has_interval = !config.batch_flush_interval.is_zero();
    let mut next_flush =
        has_interval.then(|| tokio::time::Instant::now() + config.batch_flush_interval);

    loop {
        // The interval trigger, reusing `BatchAccumulator::next_deadline`'s cadence math rather
        // than a second copy of it -- the identical shape `crate::udp::decode_loop` uses.
        if let Some(deadline) = next_flush {
            let now_instant = tokio::time::Instant::now();
            if deadline <= now_instant {
                if let Some(batch) = accumulator.take() {
                    emit(&sink, &telemetry, batch, FlushReason::Interval).await;
                }
                next_flush = Some(BatchAccumulator::next_deadline(
                    deadline,
                    now_instant,
                    config.batch_flush_interval,
                ));
            }
        }

        // Checked explicitly rather than left to `read_step`'s `changed()` arm: `changed()` only
        // fires on a transition this receiver has not yet observed, which would miss "shutdown was
        // already true when this iteration started". The `Ref` temporary is dropped at the end of
        // this statement, well before any `.await`.
        if *shutdown.borrow() {
            report_buffered_tail(&framer, &telemetry, diag);
            if let Some(batch) = accumulator.take() {
                emit(&sink, &telemetry, batch, FlushReason::Shutdown).await;
            }
            return Ok(());
        }

        read_buf.clear();
        // Two independent deadlines can bound this read: the flush tick (recurring, benign) and
        // the first-byte deadline (once, fatal). Race whichever comes first, then decide which it
        // was -- `timeout_at`, not `timeout`, so the first-byte deadline stays absolute across
        // however many flush ticks elapse before it.
        // `first_byte_seen`, never `framing().is_none()`: under an explicit `FramingMode` the
        // framing is known from construction, so the latch-shaped predicate would read "already
        // framed" here and this deadline would never fire at all (this module's "Pre-handshake
        // timeout" doc section; `the_first_byte_deadline_applies_under_every_framing_mode`).
        let awaiting_first_byte = !framer.first_byte_seen();
        let read_deadline = match (next_flush, awaiting_first_byte) {
            (Some(flush), true) => Some(flush.min(first_byte_deadline)),
            (Some(flush), false) => Some(flush),
            (None, true) => Some(first_byte_deadline),
            (None, false) => None,
        };
        let step = match read_deadline {
            None => read_step(&mut stream, &mut read_buf, &mut shutdown).await,
            Some(deadline) => {
                match tokio::time::timeout_at(
                    deadline,
                    read_step(&mut stream, &mut read_buf, &mut shutdown),
                )
                .await
                {
                    Ok(step) => step,
                    Err(_elapsed) => {
                        // Checked against the clock rather than inferred from which deadline was
                        // smaller, so a flush tick landing on the same instant can't mask it.
                        // Returning `Err` is what routes this through the accept loop's
                        // `connection_error` diagnostic and drops the permit.
                        if awaiting_first_byte && tokio::time::Instant::now() >= first_byte_deadline
                        {
                            return Err(anyhow::anyhow!(
                                "the peer sent no bytes within {handshake_timeout:?}"
                            ));
                        }
                        // The flush deadline won -- loop back round to the interval trigger above.
                        continue;
                    }
                }
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
                // A terminator-less remainder is an ordinary final message under non-transparent
                // framing and a truncated frame under octet counting -- `Framer::finish` decides.
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
                // The connection broke, but whatever was already decoded is still good -- deliver
                // it before surfacing the error as this connection's `connection_error`.
                report_buffered_tail(&framer, &telemetry, diag);
                if let Some(batch) = accumulator.take() {
                    emit(&sink, &telemetry, batch, FlushReason::Closed).await;
                }
                return Err(err.into());
            }
        }

        // `received_at` is when the bytes came off the socket, not when the frame they complete is
        // decoded -- `logit_proto::Decoder::decode_into`'s own contract.
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
                    // Diagnosed on its own key rather than bubbling up as a `connection_error`,
                    // since the cause is the peer's framing, not I/O.
                    report_frame_error(&err, &telemetry, diag);
                    // A non-fatal error (`FrameError::OversizeSkipped`) has already resynchronized
                    // the framer -- it dropped one line and either consumed its terminator or
                    // latched the drain state that will. Carrying on is the whole point of it: a
                    // carbon relay must not lose a connection over one pathological datapoint.
                    if !err.is_fatal() {
                        continue;
                    }
                    // Nothing can resynchronize past the rest (see [`FrameError`]), so this
                    // connection ends here.
                    if let Some(batch) = accumulator.take() {
                        emit(&sink, &telemetry, batch, FlushReason::Closed).await;
                    }
                    return Ok(());
                }
            }
        }
    }
}

/// One complete frame: counted, decoded, and accumulated. A decode error is diagnosed and the
/// frame dropped -- the connection stays open, mirroring `crate::udp::decode_loop`'s
/// `bad_datagram` handling of one malformed datagram among good ones.
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
            // `scope` is whatever the decoder returned -- `None` for every decoder driven here
            // today, but threaded through rather than hardcoded, exactly as `crate::udp` does.
            if let Some((batch, reason)) = accumulator.absorb(resource, scope, scratch) {
                emit(sink, telemetry, batch, reason).await;
            }
        }
        Err(err) => {
            diag.warn_throttled("bad_frame", err);
        }
    }
}

/// Counts and diagnoses a framing failure. Its own diagnostic key, not `connection_error`: an
/// operator triaging "my sender's frames are being rejected" is looking at something quite
/// different from "a peer's socket broke". Returns whether the diagnostic actually reported (i.e.
/// was not throttled), so a test can assert the listener-wide cadence directly.
fn report_frame_error(err: &FrameError, telemetry: &Telemetry, diag: &mut Diagnostics) -> bool {
    telemetry.count("logit.input.frames.dropped", 1.0, &[("reason", err.reason())]);
    diag.warn_throttled("framing_error", err)
}

/// A partial frame still held by the [`Framer`] when a connection ends *without* a clean EOF --
/// a peer RST mid-message, or this listener shutting down before the sender finished one.
///
/// Dropping those bytes is correct (nobody ever sent a complete message, and on shutdown the
/// sender has not finished), but dropping them *silently* is the gap: the identical bytes followed
/// by a FIN would be emitted by [`Framer::finish`] under non-transparent framing or counted
/// `truncated` under octet counting, and `logit.input.frames.dropped{reason="truncated"}` exists
/// precisely to make this class visible. A no-op when nothing is buffered, which is the ordinary
/// case on both paths.
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

/// `sink.send` mints a fresh [`logit_pipeline::TraceContext::new_root`] here -- once per
/// *accumulated* batch, not once per frame that fed it. See `crate::udp::emit`'s own doc comment
/// for the many-to-one attribution gap this shares with every other accumulating listener;
/// `docs/known-gaps.md`'s internal-spans entry is the one place it is tracked.
async fn emit(sink: &Fanout, telemetry: &Telemetry, batch: EventBatch, reason: FlushReason) {
    telemetry.count("logit.component.receive.flushed", 1.0, &[("reason", reason.as_str())]);
    sink.send(batch).await;
}

/// The one clock this driver reads, shared with [`crate::udp`] rather than duplicated so both
/// listeners stamp `received_at` identically.
fn now_nanos() -> i64 {
    crate::udp::now_nanos()
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, MetricKind, Registry, Resource, Value};
    use logit_pipeline::unwrap_batch;
    use logit_proto::CodecError;
    use rustls_pki_types::pem::PemObject;
    use rustls_pki_types::{CertificateDer, PrivateKeyDer};
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;
    use tokio::sync::mpsc;

    // ---- framer ------------------------------------------------------------------------------

    /// Pushes `bytes` and drains every frame that completes, as UTF-8 strings. Panics on a
    /// framing error -- [`push_and_expect_error`] is the test-side counterpart for those.
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

        // A non-transparent syslog frame always starts `<` -- but anything that isn't an ASCII
        // digit latches this way, not just `<`.
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

    /// The latch is for the connection's life: a later message that looks like the *other* framing
    /// changes nothing.
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
            // The octet-counted case has no terminator, so its last byte completes the frame only
            // because the declared length says so -- exactly the property under test.
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

    /// The reason octet counting exists at all: a MSG containing a newline is one frame, not two.
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
        // Only one: a message genuinely ending in CR keeps it.
        assert_eq!(push_and_drain(&mut framer, b"<13>hello\r\r\n"), vec!["<13>hello\r"]);
    }

    #[test]
    fn an_empty_non_transparent_line_is_skipped() {
        let mut framer = Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES);
        assert_eq!(push_and_drain(&mut framer, b"<13>a\n\n\r\n<13>b\n"), vec!["<13>a", "<13>b"]);
    }

    #[test]
    fn an_oversize_non_transparent_line_is_an_oversize_error() {
        // Past the ceiling with no terminator in sight: the framer must not keep buffering in the
        // hope that one arrives.
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

        // Exactly at the ceiling is fine -- the bound is inclusive.
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

    /// RFC 6587 §3.4.1's `MSG-LEN = NONZERO-DIGIT *DIGIT`: a padded count is not this framing.
    /// The leading `0` still latches octet counting (see [`Framing`]'s doc comment), so the
    /// connection fails loudly here rather than being silently read as non-transparent.
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

    /// The reason [`FramingMode::Lines`] exists at all. `graphite_in`'s paths and `statsd_in`'s
    /// metric names routinely begin with a digit (`1.hits:1|c`), which under
    /// [`FramingMode::Rfc6587Auto`] is an RFC 6587 octet count -- so the sniff would reframe the
    /// whole connection off one leading character. The contrast is asserted here rather than
    /// assumed, because "the mode was wired through" is exactly the sort of thing a refactor
    /// silently loses.
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

        // The same first byte under the auto mode, which is what this mode exists to avoid.
        let mut auto = Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES);
        auto.push(b"1.hits:1|c\n");
        assert_eq!(auto.framing(), Some(Framing::OctetCounting), "the contrast this test is for");
    }

    /// [`Oversize::DrainToNextLine`]: one line past the bound is dropped and counted, the
    /// connection survives, and the *next* line still frames -- carbon's own behaviour, and what
    /// `graphite_in`'s `max_line_bytes` has always meant.
    #[test]
    fn a_line_only_framer_drains_to_the_next_newline_past_the_bound() {
        let mut framer =
            Framer::new(FramingMode::Lines { oversize: Oversize::DrainToNextLine }, 16);

        // Past the bound with no terminator in sight: the end of this line has not arrived, so
        // the framer abandons it now and discards bytes until the `LF` that ends it.
        framer.push(&[b'x'; 40]);
        let err = framer.next_frame().expect_err("40 bytes with no LF is past the 16-byte bound");
        assert_eq!(err.reason(), "oversize", "{err}");
        assert!(!err.is_fatal(), "a line protocol resynchronizes at the next LF: {err}");
        assert_eq!(framer.next_frame(), Ok(None), "still draining, nothing to hand over");

        // The tail of the abandoned line, then a good one: only the good one comes out, and it is
        // counted once (when the bound was crossed), not once per byte drained.
        assert_eq!(push_and_drain(&mut framer, b"more of it\nsurvivor\n"), vec!["survivor"]);
        assert_eq!(
            push_and_drain(&mut framer, b"another\n"),
            vec!["another"],
            "the bound is per line, not a running total over the connection"
        );

        // The other branch: a line already *has* its terminator buffered when the bound is
        // checked, so there is nothing to drain -- exactly that line is dropped.
        let mut terminated =
            Framer::new(FramingMode::Lines { oversize: Oversize::DrainToNextLine }, 16);
        let err = push_and_expect_error(&mut terminated, b"0123456789012345678\nsurvivor\n");
        assert_eq!(err.reason(), "oversize", "{err}");
        assert!(!err.is_fatal(), "{err}");
        assert_eq!(push_and_drain(&mut terminated, b""), vec!["survivor"]);
    }

    #[test]
    fn a_length_prefixed_frame_split_across_pushes_is_assembled() {
        let payload = b"a pickled batch";
        let mut wire = (payload.len() as u32).to_be_bytes().to_vec();
        wire.extend_from_slice(payload);

        let mut framer = Framer::new(FramingMode::LengthPrefixed, MAX_FRAME_BYTES);
        assert_eq!(framer.framing(), Some(Framing::LengthPrefixed));
        assert_eq!(Framing::LengthPrefixed.as_str(), "length_prefixed");

        // One byte per push, so the 4-byte big-endian prefix itself straddles pushes: a reader
        // that assumed a whole prefix per read -- or read it little-endian -- fails this and
        // passes a single-push test. (`graphite/mod.rs`'s socket-level test is its twin.)
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

        // And the peer closing mid-payload is truncated, not an ordinary final message -- the
        // declared length says bytes are missing.
        let mut short = Framer::new(FramingMode::LengthPrefixed, 1024);
        short.push(&[0, 0, 0, 9, b'h', b'i']);
        assert_eq!(short.next_frame(), Ok(None), "the declared 9 bytes have not all arrived");
        assert_eq!(short.buffered(), 6);
        let err = short.finish().expect_err("a short payload under a declared length is truncated");
        assert_eq!(err.reason(), "truncated", "{err}");
    }

    // ---- framer: recorded interop fixtures ----------------------------------------------------
    //
    // A real rsyslog forwarder's TCP byte stream, captured by `script/record-fixtures rsyslog-tcp`
    // -- see `testdata/interop/syslog/README.md`'s `rsyslog-tcp-000.raw` row and
    // `docs/plans/recorded-interop-fixtures.md`'s TCP-framed-syslog amendment. This is the framer
    // half of that fixture's promise: real bytes from a real, un-tuned `omfwd` forwarder (default
    // `TCP_Framing`, i.e. RFC 6587 §3.4.2 non-transparent) pushed through `Framer` exactly as they
    // arrived over the wire, then decoded with the real `SyslogDecoder` -- not a hand-typed literal
    // shaped like what non-transparent framing is assumed to look like.

    /// `testdata/interop/syslog/<name>` as raw bytes -- the TCP fixtures are a whole connection's
    /// byte stream, not a single UTF-8 datagram, so this is a byte-oriented sibling of
    /// `crate::syslog`'s own `interop_fixture` test helper rather than a shared one.
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

        // One push, then `finish` -- rsyslog's own TCP connection here sends its one message and
        // is torn down by the recording harness rather than the peer sending an explicit
        // terminator-then-more-traffic, so the whole fixture arrives as a single read.
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

        // Line splitting stays on: this frame has no embedded newline (non-transparent framing
        // never can), so `SyslogDecoder`'s own `\n`-splitting is a no-op here rather than
        // something this test needs to disable.
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
        // `logger -t logit-fixture "hello from rsyslog, ..."` with no `-p` carries the default
        // facility/priority `user.notice` (PRI 13 = facility 1 * 8 + severity 5), the same PRI
        // `rsyslog-000.raw`'s UDP sibling fixture carries -- see
        // `testdata/interop/syslog/README.md`'s row for both.
        assert_eq!(
            event.log.as_ref().and_then(|log| log.severity),
            Some(logit_core::Severity::Info),
            "user.notice (PRI 13) maps to Severity::Info (13 % 8 = 5)"
        );
        let message = event.log.as_ref().expect("event should carry a log").message.as_str();
        assert_eq!(message, Some("hello from rsyslog, captured for logit interop fixtures"));
    }

    // ---- driver: fixtures and harness ---------------------------------------------------------

    /// A trivial `Decoder`: one frame -> one event, except the literal bytes `b"BAD"`, which are
    /// rejected -- enough to exercise decode-error handling without pulling in syslog grammar
    /// specifics. Every decoded event carries the raw frame under `"payload"`.
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

    /// One event per frame, no interval timer -- the configuration most driver tests want, since
    /// it makes every delivery attributable to exactly one frame.
    fn one_per_frame() -> TcpListenerConfig {
        TcpListenerConfig {
            batch_max_events: 1,
            batch_flush_interval: Duration::ZERO,
            ..TcpListenerConfig::default()
        }
    }

    /// Binds an ephemeral port through `Input::bind` (not by binding and dropping a probe socket
    /// the way `logit_in`'s tests must) -- the whole point of this driver's bind pre-pass.
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
            // (Linux `tcp_close`), and a read after RST is `ECONNRESET` -- still a close. The
            // oversize-frame test writes more than the server ever reads, so which of the two it
            // gets depends on socket-buffer sizes rather than on anything under test.
            Err(err) if err.kind() == std::io::ErrorKind::ConnectionReset => {}
            Err(err) => panic!("{what}: read failed outright: {err}"),
        }
    }

    /// The value of `metric`'s `Sum` in a drained `Registry` snapshot, optionally restricted to
    /// the point carrying `tag` -- mirrors `crate::logit`'s own test-module `drained_counter`,
    /// split into drain-then-query so one test can check several metrics from one snapshot.
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

    fn testdata_dir() -> std::path::PathBuf {
        // `logit-inputs` lives at `crates/logit-inputs`; the fixtures live at the repo root's
        // `testdata/tls` (`testdata/tls/README.md`) -- two levels up from `CARGO_MANIFEST_DIR`,
        // the same path `crate::logit`/`crate::otlp`'s own TLS tests use.
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    fn test_tls_settings(client_ca_file: Option<&str>) -> TlsServerSettings {
        TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: client_ca_file.map(str::to_string),
        }
    }

    /// A `tokio-rustls` client trusting exactly `ca_file` under `testdata/tls` -- `other-ca.pem`
    /// is what makes a "wrong CA" test real rather than a certificate-name mismatch.
    /// `client_cert` is `(cert, key)` file names for the mTLS cases, `None` for a client
    /// presenting nothing.
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

    /// `testdata/tls/server.pem` carries a `localhost` SAN (`testdata/tls/README.md`), so that is
    /// the name every TLS client here presents.
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

    /// The bind pre-pass (`docs/plans/operator-surface.md`, workstream B): the port is live and
    /// its OS-assigned address is readable before anything is spawned, and a second `bind()` is a
    /// no-op per `Input::bind`'s contract.
    #[tokio::test]
    async fn bind_makes_the_port_live_and_local_addr_reports_it_before_run() {
        let mut listener =
            TcpListener::new("127.0.0.1:0", TestDecoder::new(), TcpListenerConfig::default());
        assert_eq!(listener.local_addr(), None, "no address before bind()");

        listener.bind().await.expect("binding an ephemeral port should succeed");
        let addr = listener.local_addr().expect("bind() should leave a real address behind");

        // Nothing is running yet -- this connection sits in the accept backlog, which is exactly
        // what makes a bind pre-pass worth having: no startup window where the port refuses.
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
        // Three frames in one write: the bound fires on the second, and the third stays held --
        // with the interval timer off, nothing else can flush it while the connection is open.
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
        // Far short of `batch_max_events`, and the connection stays open -- only the interval
        // timer can deliver this.
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
        // The last message has no terminator: a non-transparent close emits it as a final frame
        // (`Framer::finish`), and only then does the accumulator flush.
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

    /// A frame the decoder rejects is diagnosed and dropped; the connection keeps serving the
    /// frames either side of it -- `crate::udp::decode_loop`'s `bad_datagram` contract, per frame.
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

    /// The cap rejects rather than queues, and -- unlike `logit_in` -- closes before any TLS
    /// handshake, since syslog has no in-band reject to deliver (this module's "Connection limit"
    /// doc section).
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

        // The first connection takes the one permit and holds it, idle.
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

    /// The throttle `report_frame_error` reports through has to be shared across connections to
    /// work at all: a framing error is fatal to its connection, so if each connection counted in
    /// a copy of its own, the count would sit at 1 forever and every single occurrence would
    /// warn. The sharing is now `Diagnostics`' own (a clone shares its original's counts), so
    /// this half of the property is asserted straight against a connection-shaped clone, on
    /// `warn_throttled`'s return value -- the `tracing` output itself is only capturable on the
    /// emitting thread, and the real reports come from spawned tasks.
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

    /// The wiring half of the test above, and the half that discriminates against the bug: three
    /// separate connections' framing errors must all land on the *same* [`Diagnostics`], so its
    /// `framing_error` occurrence count reaches 3.
    ///
    /// Note what deliberately isn't asserted. `logit.component.diagnostics{key="framing_error"}`
    /// and `logit.input.frames.dropped{reason="malformed"}` both reach 3 either way --
    /// `Telemetry` mirrors into one shared component buffer no matter which `Diagnostics` value
    /// did the counting -- so a metric assertion would pass against the per-connection clone this
    /// test exists to rule out. `warn_throttled`'s return value is no help from out here either,
    /// since the reports happen on spawned tasks. The occurrence count read back through a clone
    /// of the value handed to `with_diagnostics` is the one observable that differs: 3 when the
    /// counts are shared, and 0 when each connection counts 1 in its own throwaway clone. This is
    /// the regression net for that sharing now living inside `Diagnostics` itself.
    #[tokio::test]
    async fn three_connections_report_their_framing_errors_through_one_throttle() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let diag = Diagnostics::new("syslog_in");
        // Held before the listener moves into its task: a clone of the very value it was given,
        // sharing the counts every connection task's own clone reports through.
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
        // Weaker (it would hold either way, per this test's doc comment), but it does confirm the
        // three errors were classified as malformed rather than as something else on the way.
        assert_eq!(
            sum_of(&registry.drain(0), "logit.input.frames.dropped", Some(("reason", "malformed"))),
            Some(3.0)
        );

        handle.abort();
    }

    /// The `ReadStep::Failed` path -- the one way a connection ends without `Framer::finish` ever
    /// running, so a buffered partial frame would otherwise be discarded with no
    /// `logit.input.frames.dropped` count and no diagnostic, while the identical bytes followed by
    /// a FIN would be emitted as a final message.
    ///
    /// `SO_LINGER 0` is what makes it deterministic: it turns the client's `close` into an RST
    /// rather than a FIN, so the server's blocked read fails with `ECONNRESET` instead of
    /// reporting a clean EOF. The two writes are separate, with the first one's delivery awaited
    /// in between, so the server has demonstrably consumed the tail into its framer before the RST
    /// arrives -- an RST landing while bytes are still queued would discard them unread, which is
    /// a different (and uncountable) case.
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
        // Awaiting the delivery proves the server finished that read and is back blocked in the
        // next one, so the write below is what it picks up.
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>complete"]);

        client.write_all(b"<13>unterminated").await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

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

    /// The shutdown twin of the test above: a connection mid-message when the listener stops has
    /// its partial frame counted too, rather than dropped in silence.
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

        // Half a message, then shut the listener down: the sender never finished it, so dropping
        // it is right -- being quiet about it is not.
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

    /// A framing error is fatal to *its* connection and to nothing else -- neither framing can
    /// resynchronize past one, but a sibling connection never saw it.
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

        // Past the ceiling with no terminator. The write may itself fail once the server has
        // closed on us, which is the behaviour under test, not a failure.
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

    /// The shutdown cascade (`docs/adr/service-lifecycle-and-output-retry.md`): every connection
    /// task's `Fanout` clone must drop, or the downstream inbox never observes a close. An idle
    /// connection is the case that only works because each one races its read against `shutdown`.
    #[tokio::test]
    async fn shutdown_returns_promptly_with_an_idle_connection_still_open() {
        let (addr, mut listener) = bound_listener(one_per_frame()).await;
        let (sink, mut rx) = fanout_into_channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

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
        // Octet counting over TLS, so this also proves the framing latch is per connection and
        // entirely independent of the transport wrapping it.
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

        // No client certificate. Under TLS 1.3 the server's "certificate required" alert lands
        // after the client believes the handshake finished, so the failure may surface at connect
        // time or on the first write -- what must hold either way is that nothing is delivered.
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

    /// The plaintext twin of the TLS test below, and the case that actually matters in production:
    /// `syslog_in` with no `tls:` block is the default shape, so without a first-byte deadline on
    /// this arm 1024 connections that complete the TCP handshake and send nothing would hold every
    /// permit forever -- 1024 SYNs, no crypto, no bytes. Under `with_max_connections(1)` the second
    /// client can only be served if the first one's permit genuinely came back.
    #[tokio::test]
    async fn a_silent_plaintext_connection_releases_its_permit_after_the_handshake_timeout() {
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener =
            listener.with_max_connections(1).with_handshake_timeout(Duration::from_millis(50));
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        // Connected, not a byte sent, and held (not dropped) past the deadline -- so nothing but
        // the deadline itself could free the permit.
        let mut silent = connect(&addr).await;
        expect_closed(&mut silent, "a plaintext connection that sent no bytes").await;

        let mut client = connect(&addr).await;
        client.write_all(b"<13>permit came back\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>permit came back"]);

        drop(silent);
        handle.abort();
    }

    /// A first-byte deadline must not become an idle deadline: once a connection has latched its
    /// framing, a long gap before the next frame is ordinary (this module's "Pre-handshake
    /// timeout" doc section says there is deliberately no idle timeout). With a 50ms budget and a
    /// 100ms flush interval, this also exercises the interaction the naive wrapper would get
    /// wrong -- several flush ticks elapse between the two frames.
    #[tokio::test]
    async fn the_first_byte_deadline_does_not_apply_once_the_framing_has_latched() {
        // The flush timer left on (unlike `one_per_frame`), since a flush tick re-entering the
        // read is exactly what a per-read budget would keep resetting.
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

        // Comfortably past the first-byte budget, and past several `batch_flush_interval` ticks.
        tokio::time::sleep(Duration::from_millis(300)).await;
        client.write_all(b"<13>much later\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>much later"]);

        handle.abort();
    }

    /// **The regression net for `first_byte_seen()`.** The first-byte deadline's predicate used to
    /// be `framer.framing().is_none()`, which is only ever `true` before the first byte under
    /// [`FramingMode::Rfc6587Auto`]: under either explicit mode the framing is known from
    /// construction, so that predicate reads "already framed" on a connection that has said
    /// nothing and the deadline silently never fires. Nothing else would catch it -- `syslog_in`
    /// keeps working, and `graphite_in`/`statsd_in` just quietly stop bounding a silent
    /// connection.
    ///
    /// So: every mode, `with_max_connections(1)`, a silent client that is *held* past the
    /// deadline, and then a real frame that can only be served if the first one's permit genuinely
    /// came back.
    #[tokio::test]
    async fn the_first_byte_deadline_applies_under_every_framing_mode() {
        // `(mode, the wire bytes of one frame, the payload the decoder should see)`.
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

            // Connected, not a byte sent, and held (not dropped) past the deadline -- so nothing
            // but the deadline itself could free the permit.
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

    /// The pre-handshake timeout's whole purpose (this module's "Pre-handshake timeout" doc
    /// section): a client that opens a connection and never sends a ClientHello must not pin a
    /// connection-limit permit. Proven under `with_max_connections(1)`, so the second connection
    /// can only succeed if the first one's permit genuinely came back.
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

        // Raw TCP, not a byte sent -- held (not dropped) past the timeout, so nothing but the
        // timeout itself could free the permit.
        let _silent = connect(&addr).await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        let connector = tls_connector("ca.pem", None).await;
        let mut client = tls_connect(&connector, &addr).await;
        client.write_all(b"<13>permit came back\n").await.unwrap();
        client.flush().await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>permit came back"]);

        handle.abort();
    }
}
