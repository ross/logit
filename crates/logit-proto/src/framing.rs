//! Frame extraction over a byte stream for the TCP and Unix-stream listeners: [`Framer`], pure,
//! socket-free, and synchronous, which `logit_inputs::tcp`'s driver feeds each connection's bytes
//! through. Not the native transport's frame envelope, which is [`crate::frame`].
//!
//! **Framing is chosen per listener, not guessed.** A [`FramingMode`] is fixed when the listener
//! is built: RFC 6587's auto-detecting pair for `syslog_in`, LF-delimited lines for a line
//! protocol whose messages may *start* with a digit (`graphite_in` plaintext, `statsd_in`,
//! `lines_in`), carbon's 4-byte big-endian length prefix (`docs/adr/graphite-carbon-relay.md`), or DogStatsD's
//! 4-byte little-endian one (`statsd_in`'s `transport: unix_stream`; ADR
//! `datadog-agent-and-intake-relay`, decision 12).
//!
//! The sockets, the first-byte and idle deadlines, and what a [`FrameError`] costs a connection
//! are the driver's: `crates/logit-inputs/src/tcp.rs`'s module doc.

use bytes::{Bytes, BytesMut};

/// The largest single frame the stream driver assembles, in bytes, for any framing: the default
/// for `logit_inputs::tcp::TcpListener::with_framing`'s second argument when a listener never
/// calls it.
///
/// Not configurable on `syslog_in` or `statsd_in`. `graphite_in` overrides it with its own
/// `max_line_bytes`/`max_frame_bytes`, which carbon's receivers expose. It is *not* tied to
/// `syslog_out`'s `max_message_bytes` (8192): that is a sender-side knob an operator may raise,
/// and a receiver ceiling tracking it would need re-tuning in lockstep with every sender. 64 KiB
/// matches the practical per-message ceiling the UDP driver's 65507-byte read buffer already
/// imposes, and is generous against RFC 5424's "receiver MUST be able to accept 2048 octets" and
/// every real sender's default.
pub const MAX_FRAME_BYTES: usize = 65_536;

/// Bytes the stream driver pulls off the socket per read, and a [`Framer`]'s initial buffer
/// capacity. Well under [`MAX_FRAME_BYTES`]: a listener pays this per connection, and a [`Framer`]
/// assembles a larger frame across reads anyway.
pub const READ_BUFFER_BYTES: usize = 8 * 1024;

/// Bytes in [`FramingMode::LengthPrefixed`]'s frame prefix: one big-endian `u32` payload length
/// (Twisted's `Int32StringReceiver`, which carbon's pickle listener speaks). A local copy of
/// [`crate::graphite::pickle::LENGTH_PREFIX_BYTES`] so the framer names nothing
/// graphite-specific; the assert below keeps the two equal.
const LENGTH_PREFIX_BYTES: usize = 4;

const _: () = assert!(LENGTH_PREFIX_BYTES == crate::graphite::pickle::LENGTH_PREFIX_BYTES);

/// How a [`Framer`] delimits one connection's messages. Chosen once per listener, through
/// `logit_inputs::tcp::TcpListener::with_framing`, and never re-evaluated.
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
    /// A 4-byte **little-endian** payload length, then that many bytes: DogStatsD's stream Unix
    /// socket, where the payload is one datagram's worth of newline-separated lines, each ending
    /// in `LF`. That is what the `datadog` Python client writes to an Agent's
    /// `dogstatsd_stream_socket` (`testdata/interop/datadog/dogstatsd-unix-stream-*.raw`).
    LengthPrefixedLe,
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
    /// A 4-byte little-endian payload length, then that many payload bytes
    /// ([`FramingMode::LengthPrefixedLe`]).
    LengthPrefixedLe,
}

impl Framing {
    pub fn as_str(self) -> &'static str {
        match self {
            Framing::OctetCounting => "octet_counting",
            Framing::NonTransparent => "non_transparent",
            Framing::LengthPrefixed => "length_prefixed",
            Framing::LengthPrefixedLe => "length_prefixed_le",
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
            FramingMode::LengthPrefixedLe => Some(Framing::LengthPrefixedLe),
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
    /// (`crates/logit-inputs/src/tcp.rs`'s "Pre-handshake timeout" doc section). Not
    /// `framing().is_some()`, which is true from construction under both explicit modes and would
    /// make that deadline inert on every listener but `syslog_in`.
    pub fn first_byte_seen(&self) -> bool {
        self.seen_bytes
    }

    /// Bytes held but not yet formed into a frame. Read by the stream driver's
    /// `report_buffered_tail` (`crates/logit-inputs/src/tcp.rs`) on the paths
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
                Some(Framing::LengthPrefixed) => {
                    return self.next_length_prefixed(u32::from_be_bytes)
                }
                Some(Framing::LengthPrefixedLe) => {
                    return self.next_length_prefixed(u32::from_le_bytes)
                }
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
    /// stream driver's `ReadStep::Eof` arm routes this `Err` through `report_frame_error`, and
    /// its `report_buffered_tail` reports the RST case identically
    /// (`crates/logit-inputs/src/tcp.rs`). The one case with no counter
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
            Some(Framing::LengthPrefixed | Framing::LengthPrefixedLe) => {
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
                    FramingMode::Rfc6587Auto
                    | FramingMode::LengthPrefixed
                    | FramingMode::LengthPrefixedLe => Ok(Some(line)),
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
            FramingMode::Rfc6587Auto
            | FramingMode::LengthPrefixed
            | FramingMode::LengthPrefixedLe => Oversize::Fatal,
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

    /// A 4-byte payload length, then that many payload bytes: big-endian for Twisted's
    /// `Int32StringReceiver` ([`FramingMode::LengthPrefixed`]), little-endian for DogStatsD's
    /// stream socket ([`FramingMode::LengthPrefixedLe`]); `read_len` is the byte order. The prefix
    /// is validated and stripped here, so the decoder gets one unframed payload, which
    /// `GraphiteDecoder`'s pickle path expects (framing is the listener's job).
    ///
    /// A declared length past the bound is [`FrameError::Oversize`] and fatal: unlike an
    /// LF-delimited stream there is no resync point to skip to. A short buffer is `Ok(None)`.
    fn next_length_prefixed(
        &mut self,
        read_len: fn([u8; LENGTH_PREFIX_BYTES]) -> u32,
    ) -> Result<Option<Bytes>, FrameError> {
        if self.buf.len() < LENGTH_PREFIX_BYTES {
            return Ok(None);
        }
        let mut prefix = [0u8; LENGTH_PREFIX_BYTES];
        prefix.copy_from_slice(&self.buf[..LENGTH_PREFIX_BYTES]);
        let payload_len = read_len(prefix) as usize;
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
