//! RFC 3164 / RFC 5424 syslog over UDP or TCP, the input half of the `syslog_in -> syslog_out`
//! lossless-relay pair (`docs/adr/lossless-transit.md`). nginx's `access_log syslog:` writer
//! speaks it over UDP.
//!
//! ## Transports and framing
//!
//! **Both transports, one decoder.** UDP is the default. `transport: tcp`
//! (`docs/adr/syslog-tcp-ingress-and-tls.md`) runs the same [`SyslogDecoder`] behind
//! [`crate::tcp::TcpListener`] instead of [`crate::udp::UdpListener`], adding an accept loop, RFC
//! 6587 framing and, with a `tls:` block, TLS termination. RFC 5425 syslog over TLS is RFC 6587
//! framing carried over TLS.
//!
//! The TCP framing is auto-detected from each connection's first byte and latched for its life:
//! an ASCII digit starts an **octet count** (`MSG-LEN SP MSG`), and anything else is
//! **non-transparent** (LF-delimited), since such a frame always starts with `<`. A final
//! LF-framed message with no terminator is emitted on a clean close, as RFC 6587 §3.4.2 permits;
//! after an abrupt close or a shutdown it is counted `truncated`. A frame past the driver's 64 KiB
//! [`MAX_FRAME_BYTES`](crate::tcp::MAX_FRAME_BYTES) closes the connection, counted
//! `logit.input.frames.dropped{reason="oversize"}`, under either framing, since octet counting has
//! no resync point; an octet-counted frame cut short by a close is
//! `logit.input.frames.dropped{reason="truncated"}`. As in `statsd_in`, only `receive:`'s
//! batch-assembly fields and `shutdown_grace` apply under TCP (graph rule 17).
//!
//! The decoder differs between the transports only in **line splitting**. A UDP datagram may carry
//! several LF-separated messages, so the UDP arm splits on `\n`. A TCP frame is already one
//! message, and an octet-counted one may contain `\n` as MSG content, so [`SyslogInput::tcp`] turns
//! splitting off ([`SyslogDecoder::with_line_splitting`]) and the framer is the sole delimiter.
//!
//! ## Telemetry and diagnostics
//!
//! The drivers own every `logit.input.*` counter, as for `statsd_in` (`crate::statsd`'s
//! "Telemetry and diagnostics"). **A malformed message is skipped and reported as a throttled
//! `bad_line`**, and the rest of its datagram still decodes. `decode_into` never fails, so the
//! drivers' `bad_datagram`/`bad_frame` never fire here. The decoder's other diagnostics keep the
//! event: `sniff_fallback`, `timestamp_out_of_range`, and `hostname_not_utf8`, each described
//! below.
//!
//! ## Dialect disambiguation
//!
//! Per message, right after `<PRI>`: a digit then a space (`1 `) means RFC 5424, anything else
//! RFC 3164. This sniff is a guess, since RFC 5424's VERSION allows `1`-`999` and a tag-less RFC
//! 3164 line whose MSG starts `4 requests failed` matches it too. A failed RFC 5424 parse with
//! version `1` (the only version real senders emit) is rejected as malformed RFC 5424. A failed
//! parse with any other digit is taken for a false-positive sniff: the line is reparsed as RFC
//! 3164 (whose parse never fails) with a throttled `sniff_fallback` diagnostic, which would
//! surface a future RFC 5424 version. So each RFC 5424 field rejection below rejects a version-`1`
//! line and sends any other version to the RFC 3164 fallback.
//!
//! ## Mapping
//!
//! PRI must be 1-3 digits with no leading zero (except `<0>`) and at most 191, or the line is
//! malformed. It yields `syslog.facility` and `syslog.severity` ([`Value::U64`]) and a
//! [`Severity`] (see `map_severity`). The other attributes:
//!
//! - `syslog.timestamp`: the sender's TIMESTAMP (below).
//! - `syslog.hostname`: HOSTNAME.
//! - `syslog.tag`: RFC 3164's TAG name or RFC 5424's APP-NAME.
//! - `syslog.pid`: RFC 3164's `tag[pid]` bracket or RFC 5424's PROCID (below).
//! - `syslog.msgid`: RFC 5424's MSGID.
//! - `syslog.sd`: RFC 5424's STRUCTURED-DATA (below).
//!
//! An RFC 5424 nil (`-`) or empty HOSTNAME/APP-NAME/PROCID/MSGID stamps nothing.
//!
//! **Timestamp semantics.** Every [`Event`]'s `timestamp` is *receipt* time: the `received_at`
//! passed to [`SyslogDecoder::decode_into`], captured when the datagram came off the socket
//! (`docs/adr/decoupled-listener-io.md`), not when decode runs and never the sender's own. RFC
//! 3164's timestamp has no year and no timezone, so resolving it means guessing both, and resolving
//! only RFC 5424's would give two senders on one listener different semantics. The sender's
//! timestamp lands in `syslog.timestamp` instead:
//!
//! - RFC 5424's RFC 3339 form is a [`Value::Timestamp`].
//! - RFC 3164's is the raw [`Value::Str`], since resolving it needs a guess.
//! - A nil RFC 5424 TIMESTAMP (`-`) is an explicit [`Value::Null`], so `syslog_out` or a Lua script
//!   can tell "the sender said no timestamp" from RFC 3164's absent timestamp, which omits the
//!   attribute.
//! - A well-formed RFC 5424 TIMESTAMP outside the `i64`-nanosecond range
//!   ([`TimestampError::OutOfRange`]) keeps the event, omits `syslog.timestamp`, and reports a
//!   throttled `timestamp_out_of_range`. One that doesn't parse
//!   ([`Malformed`](TimestampError::Malformed)) rejects the line.
//!
//! `docs/known-gaps.md`'s syslog entry "`event.timestamp` is still receipt time" has the full
//! writeup and a sketched opt-in `syslog_timestamp` transform.
//!
//! **RFC 5424 STRUCTURED-DATA becomes `syslog.sd`.** [`parse_structured_data`] is a quote-aware
//! RFC 5424 §6.3 parser (`docs/adr/syslog-structured-data-convention.md` has the rationale):
//!
//! - The nil marker `-` produces no attribute.
//! - One or more `[SD-ID SP PARAM-NAME="PARAM-VALUE" ...]` SD-ELEMENTs produce `syslog.sd` = a
//!   [`Value::Map`] of `"<SD-ID>"` to a nested [`Value::Map`] of `"<PARAM-NAME>"` to
//!   [`Value::Str`]. It nests rather than flattening because `SD-NAME` may contain `.`, which would
//!   make a `syslog.sd.<id>.<param>` key ambiguous.
//! - `SD-NAME` (`SD-ID` and `PARAM-NAME`) is 1..=32 bytes of PRINTUSASCII (`%d33-126`) excluding
//!   `=`, SP, `]`, and `"`.
//! - A repeated `SD-ID` in one message is a grammar violation, since two elements sharing an id
//!   have no defined merge. A `PARAM-NAME` repeated within one element is legal and becomes a
//!   [`Value::Array`] of [`Value::Str`] in wire order.
//! - `PARAM-VALUE` is a quoted UTF-8 string in which only `\"`, `\\`, and `\]` are escapes,
//!   unescaped on decode; a backslash before any other byte is kept, with that byte.
//! - Any violation rejects the line, with a `bad_line` naming the rule and its byte offset.
//!
//! **A leading RFC 5424 §6.4 UTF-8 BOM (`EF BB BF`) on MSG is stripped**, so it doesn't leak into
//! `log.message` as U+FEFF: it is a `MSG-UTF8` signal, not payload. It is stripped only when the
//! whole MSG is valid UTF-8; a non-UTF-8 MSG has no `Value::Str` to strip it from.
//!
//! **Header fields are parsed off raw bytes and validated one by one; only MSG may hold non-UTF-8
//! bytes.** [`SyslogDecoder::decode_into`] splits on the `\n` byte with no whole-line UTF-8 check.
//! PRI, the RFC 3164 timestamp, HOSTNAME, TAG/APP-NAME, PROCID, MSGID, and STRUCTURED-DATA are
//! PRINTUSASCII by grammar and validated where extracted. A violation in an RFC 5424 field rejects
//! the line. RFC 3164's header parse never fails, because the sniff fallback depends on that: a
//! non-UTF-8 RFC 3164 HOSTNAME candidate is left unstamped with a throttled `hostname_not_utf8`.
//! MSG is validated on its own: valid UTF-8 becomes [`Value::Str`], and invalid UTF-8 becomes
//! [`Value::Bytes`] rather than rejecting the line, since RFC 5424 allows arbitrary binary MSG-ANY.
//!
//! **`syslog.pid`** is [`Value::U64`] when PROCID or a `tag[pid]` bracket parses as one, and
//! [`Value::Str`] of the raw token otherwise: RFC 5424's PROCID is free-form PRINTUSASCII. For RFC
//! 3164, [`is_tag_shaped`] accepts non-numeric bracket content that is PRINTUSASCII without `]`
//! (so the bracket still balances), so the `[...]` isn't absorbed into the message body.
//!
//! ## The RFC 3164 header
//!
//! nginx's `nohostname` option omits a field RFC 3164 calls mandatory, and nginx's MSG is JSON full
//! of `": "`, so the header can't be parsed by scanning for the first `: ` or assuming HOSTNAME is
//! present. [`parse_3164`]'s rule:
//!
//! 1. `<PRI>`: `<`, 1-3 digits, `>`. A missing or non-numeric PRI is a malformed line.
//! 2. The `Mmm dd hh:mm:ss` timestamp (15 bytes), if present; absent is tolerated.
//! 3. **At most the next two whitespace-delimited tokens** are HOSTNAME/TAG candidates. If the
//!    first is TAG-shaped, there is no hostname. Otherwise, if the second is TAG-shaped, the first
//!    is the hostname. Everything after the TAG token (minus one leading space) is MSG.
//! 4. If neither is TAG-shaped, there is no tag: the whole remainder is MSG, with no `syslog.tag`.
//!    **The two-token bound is what makes this safe**: an unbounded "find the first `: `" scan
//!    would find one inside a JSON body on a tag-less message and truncate the log line.
//! 5. `tag[pid]:` splits into `syslog.tag` + `syslog.pid`.
//!
//! [`is_tag_shaped`] is stricter than "ends in `:` or `]:`": the token's body must also look like a
//! process name (letters, digits, `_`, `-`, `.`, `/`, optionally a bracketed PID). Otherwise a
//! tag-less message starting `{"status": 200, ...}` would have `{"status":` taken for a tag and
//! part of its body eaten; no real tag contains `{` or `"`.

use crate::tcp::{TcpListener, TcpListenerConfig, TlsServerSettings};
use crate::udp::{UdpListener, UdpListenerConfig};
use crate::Input;
use bytes::Bytes;
use logit_core::interner::{intern, KeyCache};
use logit_core::time::{parse_rfc3339_to_nanos, TimestampError};
use logit_core::{
    AttrMap, BodyFormat, Diagnostics, Event, LogRecord, Resource, Scope, Severity, Symbol,
    Telemetry, Value,
};
use logit_pipeline::Fanout;
use logit_proto::{CodecError, Decoder};
use std::path::Path;
use std::sync::{Arc, LazyLock};
use tokio::sync::watch;

/// Which driver a [`SyslogInput`] wraps, chosen once by `transport:`. An enum rather than a
/// `Box<dyn Input>` so each arm's concrete builders ([`TcpListener::with_tls`],
/// [`UdpListener::with_config`]) stay reachable.
enum Inner {
    Udp(UdpListener<SyslogDecoder>),
    Tcp(TcpListener<SyslogDecoder>),
}

/// The `syslog_in` listener: a [`SyslogDecoder`] over [`UdpListener`] or [`TcpListener`].
///
/// All transport behavior lives in the drivers; see this module's "Transports and framing".
pub struct SyslogInput {
    inner: Inner,
}

impl SyslogInput {
    /// A UDP listener, the default transport, with line splitting on: one datagram may carry
    /// several LF-separated messages.
    pub fn new(bind: impl Into<String>) -> Self {
        Self {
            inner: Inner::Udp(UdpListener::new(
                bind,
                SyslogDecoder::new(Arc::new(Resource::default())),
                UdpListenerConfig::default(),
            )),
        }
    }

    /// A TCP listener (`transport: tcp`), plaintext until [`Self::with_tls`] is called.
    ///
    /// Line splitting is **off** ([`SyslogDecoder::with_line_splitting`]): the framer delimits one
    /// message per frame, and an octet-counted MSG may contain a `\n` that re-splitting would shred
    /// into spurious events. Under LF framing the `\n` is already gone, so splitting could only be
    /// a no-op or a bug there too.
    pub fn tcp(bind: impl Into<String>) -> Self {
        Self {
            inner: Inner::Tcp(TcpListener::new(
                bind,
                SyslogDecoder::new(Arc::new(Resource::default())).with_line_splitting(false),
                TcpListenerConfig::default(),
            )),
        }
    }

    /// Attaches a component id to the driver's diagnostics and to the wrapped [`SyslogDecoder`]'s.
    ///
    /// Both must carry it: the driver reports transport failures (`framing_error`/
    /// `connection_error` on TCP) and the decoder reports every rejected message as `bad_line`,
    /// on either transport, since [`Decoder::decode_into`] never fails here. Miss one and that
    /// class of failure reports under no component id with telemetry disabled.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.inner = match self.inner {
            Inner::Udp(listener) => Inner::Udp(
                listener.with_diagnostics(diag.clone()).map_decoder(|d| d.with_diagnostics(diag)),
            ),
            Inner::Tcp(listener) => Inner::Tcp(
                listener.with_diagnostics(diag.clone()).map_decoder(|d| d.with_diagnostics(diag)),
            ),
        };
        self
    }

    /// Attaches a telemetry handle for the drivers' layer-3 counters
    /// (`docs/design/internal-telemetry.md`): datagrams and bytes on UDP, connections and frames on
    /// TCP.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.inner = match self.inner {
            Inner::Udp(listener) => Inner::Udp(listener.with_telemetry(telemetry)),
            Inner::Tcp(listener) => Inner::Tcp(listener.with_telemetry(telemetry)),
        };
        self
    }

    /// Sets a **UDP** listener's `receive:` block (`docs/adr/decoupled-listener-io.md`); leaves a
    /// TCP listener untouched.
    ///
    /// Two transport-specific setters because the configs aren't interchangeable: a TCP listener
    /// has no receive queue (graph rule 17), so one setter would have to decide at runtime what to
    /// do with a queue bound it can't honour. [`Self::with_tcp_receive`] is the counterpart.
    pub fn with_receive(mut self, config: UdpListenerConfig) -> Self {
        if let Inner::Udp(listener) = self.inner {
            self.inner = Inner::Udp(listener.with_config(config));
        }
        self
    }

    /// [`Self::with_receive`]'s TCP counterpart; leaves a UDP listener untouched.
    pub fn with_tcp_receive(mut self, config: TcpListenerConfig) -> Self {
        if let Inner::Tcp(listener) = self.inner {
            self.inner = Inner::Tcp(listener.with_config(config));
        }
        self
    }

    /// Sets a **TCP** listener's per-phase pre-message budget (`handshake_timeout:`): the TLS
    /// accept and the wait for the first byte (`crate::tcp`'s "Pre-handshake timeout").
    ///
    /// A UDP listener is left untouched rather than failing, since it has no connection to bound;
    /// graph rule 45 rejects a non-default value there. `tls:` differs ([`Self::with_tls`] fails):
    /// it has no default, so its presence is an instruction.
    pub fn with_handshake_timeout(mut self, handshake_timeout: std::time::Duration) -> Self {
        if let Inner::Tcp(listener) = self.inner {
            self.inner = Inner::Tcp(listener.with_handshake_timeout(handshake_timeout));
        }
        self
    }

    /// Bounds how long a **TCP** connection may stay quiet past its first byte (`idle_timeout:`)
    /// before it is closed and its permit returned; `None` (the default) disables it. See
    /// `crate::tcp`'s "Idle timeout" for what resets the clock.
    ///
    /// A UDP listener is left untouched, as in [`Self::with_handshake_timeout`]; graph rule 53
    /// rejects the field there.
    pub fn with_idle_timeout(mut self, idle_timeout: Option<std::time::Duration>) -> Self {
        if let Inner::Tcp(listener) = self.inner {
            self.inner = Inner::Tcp(listener.with_idle_timeout(idle_timeout));
        }
        self
    }

    /// Test-only override of the driver's connection cap, so a test reaches it with two
    /// connections rather than 1025. A UDP listener is left untouched.
    #[cfg(test)]
    fn with_max_connections(mut self, max_connections: usize) -> Self {
        if let Inner::Tcp(listener) = self.inner {
            self.inner = Inner::Tcp(listener.with_max_connections(max_connections));
        }
        self
    }

    /// Terminates TLS (RFC 5425) on a TCP listener (`tls:`); paths in `settings` resolve against
    /// `base_dir`.
    ///
    /// Fails on a UDP listener: DTLS (RFC 6012) is out of scope
    /// (`docs/adr/syslog-tcp-ingress-and-tls.md`'s Alternatives). Graph rule 43 is what an operator
    /// sees; this arm backstops a caller that skipped validation.
    pub fn with_tls(
        mut self,
        settings: &TlsServerSettings,
        base_dir: &Path,
    ) -> anyhow::Result<Self> {
        self.inner = match self.inner {
            Inner::Tcp(listener) => Inner::Tcp(listener.with_tls(settings, base_dir)?),
            Inner::Udp(_) => anyhow::bail!(
                "syslog_in: 'tls:' needs 'transport: tcp' -- there is no syslog-over-DTLS support \
                 (docs/adr/syslog-tcp-ingress-and-tls.md)"
            ),
        };
        Ok(self)
    }

    /// The bound address after `bind()`, so a caller learns an ephemeral port with no bind-drop
    /// race.
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        match &self.inner {
            Inner::Udp(listener) => listener.local_addr(),
            Inner::Tcp(listener) => listener.local_addr(),
        }
    }
}

#[async_trait::async_trait]
impl Input for SyslogInput {
    async fn bind(&mut self) -> anyhow::Result<()> {
        match &mut self.inner {
            Inner::Udp(listener) => listener.bind().await,
            Inner::Tcp(listener) => listener.bind().await,
        }
    }

    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        match &mut self.inner {
            Inner::Udp(listener) => listener.run(sink).await,
            Inner::Tcp(listener) => listener.run(sink).await,
        }
    }

    async fn run_until_shutdown(
        &mut self,
        sink: Fanout,
        shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        match &mut self.inner {
            Inner::Udp(listener) => listener.run_until_shutdown(sink, shutdown).await,
            Inner::Tcp(listener) => listener.run_until_shutdown(sink, shutdown).await,
        }
    }
}

/// Decodes syslog bytes into events; testable without a socket.
///
/// `Clone` because [`TcpListener`] gives every connection its own decoder
/// (`crates/logit-inputs/src/tcp.rs`'s "`D: Clone` is load-bearing"). A clone shares its
/// `Diagnostics` throttle counts (`logit_core::Diagnostics`' type doc), so `bad_line` throttles
/// listener-wide. It must: a peer looping connect, one bad message, close would otherwise report
/// its "1st" occurrence once per connection forever.
#[derive(Clone)]
pub struct SyslogDecoder {
    resource: Arc<Resource>,
    diag: Diagnostics,
    /// See [`Self::with_line_splitting`].
    line_splitting: bool,
    /// SD-IDs and PARAM-NAMEs memoised `&str -> Symbol`: they repeat on every line, so after the
    /// first each is a `memcmp`, not an interner probe. The `syslog.*` carrier keys are in `KEYS`.
    keys: KeyCache,
}

impl SyslogDecoder {
    pub fn new(resource: Arc<Resource>) -> Self {
        Self { resource, diag: Diagnostics::default(), line_splitting: true, keys: KeyCache::new() }
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    /// Whether [`Decoder::decode_into`] splits its input on `\n` (the default, for UDP) or treats
    /// it as one message (`docs/adr/syslog-tcp-ingress-and-tls.md`).
    ///
    /// Off is for a transport that already delimits messages, [`SyslogInput::tcp`]'s RFC 6587
    /// [`crate::tcp::Framer`]. Splitting there would be wrong: an octet-counted MSG may contain
    /// `\n`, and re-splitting would turn one multiline message into half-messages, most of them
    /// missing a PRI and dropped as `bad_line`.
    pub fn with_line_splitting(mut self, line_splitting: bool) -> Self {
        self.line_splitting = line_splitting;
        self
    }

    /// Parses one delimited message into `out`, or reports a throttled `bad_line`; both arms of
    /// [`Decoder::decode_into`] use it.
    fn absorb_line(&mut self, line: Bytes, received_at: i64, out: &mut Vec<Event>) {
        // Only an empty record is skipped; whitespace-only content is MSG data, not framing.
        if line.is_empty() {
            return;
        }
        match parse_line(&line, received_at, &mut self.diag, &mut self.keys) {
            Ok(event) => out.push(event),
            Err(err) => {
                self.diag.warn_throttled("bad_line", err);
            }
        }
    }

    /// Test-only: confirms `SyslogInput::with_diagnostics` reached this decoder, not only the
    /// driver.
    #[cfg(test)]
    pub(crate) fn diag(&self) -> &Diagnostics {
        &self.diag
    }
}

impl Decoder for SyslogDecoder {
    fn decode_into(
        &mut self,
        bytes: Bytes,
        received_at: i64,
        out: &mut Vec<Event>,
    ) -> Result<(Arc<Resource>, Option<Arc<Scope>>), CodecError> {
        // Split on raw bytes, with no UTF-8 check: `parse_line` validates each field (module doc).
        if !self.line_splitting {
            // One framed message. An octet-counted MSG-LEN may cover a trailing `\r\n`; strip one
            // `\n`, then one `\r` behind it, so it decodes as the same line over UDP would (the
            // splitting arm and `crate::tcp::Framer`'s LF framing strip the same).
            //
            // The `\r` comes off **only** when an `\n` did. An LF-framed frame arrives
            // terminator-free, so a `\r` still at its end is payload (`...msg\r\r\n`) that UDP
            // would keep. A counted lone `\r` is kept too: RFC 6587 has no bare-CR terminator.
            let mut line = bytes;
            if line.ends_with(b"\n") {
                line = line.slice(..line.len() - 1);
                if line.ends_with(b"\r") {
                    line = line.slice(..line.len() - 1);
                }
            }
            self.absorb_line(line, received_at, out);
            return Ok((self.resource.clone(), None));
        }
        // Per line, not per datagram. nginx's `escape=json` guarantees no raw newline in an
        // access-log body, so this split is safe for it.
        let mut start = 0usize;
        while start <= bytes.len() {
            let nl = bytes[start..].iter().position(|&b| b == b'\n');
            let end = start + nl.unwrap_or(bytes.len() - start);
            let mut line = bytes.slice(start..end);
            if line.ends_with(b"\r") {
                line = line.slice(..line.len() - 1);
            }
            self.absorb_line(line, received_at, out);
            match nl {
                Some(i) => start += i + 1,
                None => break,
            }
        }
        // syslog has no instrumentation scope.
        Ok((self.resource.clone(), None))
    }
}

/// Rebuilds `sub` as a `Bytes` sharing `line`'s allocation, by pointer arithmetic.
///
/// `sub` must be a slice of `line`, never a copy, so the offset is in bounds. This keeps every
/// extracted field a zero-copy slice of the datagram (`docs/design/data-model.md`'s
/// "`bytes::Bytes` everywhere strings and blobs appear"). An unescaped PARAM-VALUE is not a slice
/// and goes through `Bytes::from` instead.
fn slice_of(line: &Bytes, sub: &[u8]) -> Bytes {
    let line_start = line.as_ptr() as usize;
    let sub_start = sub.as_ptr() as usize;
    let start = sub_start - line_start;
    line.slice(start..start + sub.len())
}

/// Splits `s` at the first ASCII space into `(token, rest)`, consuming the space. With no space,
/// `rest` is `&s[s.len()..]`, not `b""`, so it stays a subslice of `s` as [`slice_of`] requires.
fn split_first_token(s: &[u8]) -> (&[u8], &[u8]) {
    match s.iter().position(|&b| b == b' ') {
        Some(i) => (&s[..i], &s[i + 1..]),
        None => (s, &s[s.len()..]),
    }
}

/// Maps a PRI's severity (`pri % 8`) onto [`Severity`].
/// `0 emerg`/`1 alert`/`2 crit` -> `Fatal`; `3 err` -> `Error`; `4 warning` -> `Warn`;
/// `5 notice`/`6 info` -> `Info`; `7 debug` -> `Debug`. `Trace` has no syslog equivalent.
fn map_severity(n: u32) -> Severity {
    match n {
        0..=2 => Severity::Fatal,
        3 => Severity::Error,
        4 => Severity::Warn,
        5 | 6 => Severity::Info,
        7 => Severity::Debug,
        _ => unreachable!("severity is `pri % 8`, always in 0..=7"),
    }
}

/// `true` for every byte in RFC 5424's PRINTUSASCII class (`%d33-126`).
fn is_printusascii_byte(b: u8) -> bool {
    (33..=126).contains(&b)
}

/// `true` when every byte of `b` is PRINTUSASCII, RFC 5424's class for HOSTNAME, APP-NAME,
/// PROCID, MSGID, and (narrowed) `SD-NAME`.
fn is_printusascii(b: &[u8]) -> bool {
    b.iter().all(|&c| is_printusascii_byte(c))
}

/// The `syslog.*` carrier keys, interned once per process so each line pays a sorted
/// `insert_sym`, not an interner hash and shard lock. A `LazyLock` rather than a decoder field
/// (as collectd's `AttrKeys` is) because the parsers are free functions; `KEYS.x` is one acquire
/// load after first use.
static KEYS: LazyLock<SyslogKeys> = LazyLock::new(|| SyslogKeys {
    facility: intern("syslog.facility"),
    severity: intern("syslog.severity"),
    timestamp: intern("syslog.timestamp"),
    hostname: intern("syslog.hostname"),
    tag: intern("syslog.tag"),
    pid: intern("syslog.pid"),
    msgid: intern("syslog.msgid"),
    sd: intern("syslog.sd"),
});

struct SyslogKeys {
    facility: Symbol,
    severity: Symbol,
    timestamp: Symbol,
    hostname: Symbol,
    tag: Symbol,
    pid: Symbol,
    msgid: Symbol,
    sd: Symbol,
}

/// Parses one non-empty message, a slice of the datagram not yet validated as UTF-8. `diag`
/// reports the diagnostics that keep the event (`sniff_fallback`, `timestamp_out_of_range`,
/// `hostname_not_utf8`).
fn parse_line(
    line: &Bytes,
    recv_ts: i64,
    diag: &mut Diagnostics,
    keys: &mut KeyCache,
) -> Result<Event, CodecError> {
    let malformed = || {
        CodecError::Malformed(format!("malformed syslog line: {:?}", String::from_utf8_lossy(line)))
    };

    if line.first() != Some(&b'<') {
        return Err(malformed());
    }
    let after_lt = &line[1..];
    let gt = after_lt.iter().position(|&b| b == b'>').ok_or_else(malformed)?;
    // 1-3 digits between '<' and '>'.
    if gt == 0 || gt > 3 {
        return Err(malformed());
    }
    let digits = &after_lt[..gt];
    if !digits.iter().all(|b| b.is_ascii_digit()) {
        return Err(malformed());
    }
    // PRI is facility*8+severity in 0..=191 with no leading zero except `0` itself, so `<013>`
    // and `<192>..<999>` are malformed: they'd attach an impossible facility (124 for `<999>`).
    if digits.len() > 1 && digits[0] == b'0' {
        return Err(malformed());
    }
    let pri: u32 =
        std::str::from_utf8(digits).expect("digits are ASCII").parse().map_err(|_| malformed())?;
    if pri > 191 {
        return Err(malformed());
    }
    let after_pri = &after_lt[gt + 1..];

    let facility = pri / 8;
    let severity_num = pri % 8;
    let severity = map_severity(severity_num);

    // Dialect sniff (module doc, "Dialect disambiguation").
    let is_5424_after = match (after_pri.first(), after_pri.get(1)) {
        (Some(&c0), Some(&b' ')) if c0.is_ascii_digit() => Some((c0 as char, &after_pri[2..])),
        _ => None,
    };

    match is_5424_after {
        Some((version, after_version)) => {
            match parse_5424(
                line,
                after_version,
                facility,
                severity_num,
                severity,
                recv_ts,
                diag,
                keys,
            ) {
                Ok(event) => Ok(event),
                // Version `1` is the only one real senders emit, so its failure is malformed RFC
                // 5424. Any other digit failing is likely a tag-less RFC 3164 MSG starting with a
                // digit and a space, so reparse as RFC 3164, which never fails.
                Err(err) if version == '1' => Err(err),
                Err(err) => {
                    // Reported so a future RFC 5424 version past `1` doesn't get reparsed as RFC
                    // 3164 unnoticed.
                    diag.warn_throttled(
                        "sniff_fallback",
                        format_args!(
                            "RFC 5424 dialect sniff (version {version:?}) failed to parse \
                             ({err}); reparsing the line as RFC 3164"
                        ),
                    );
                    Ok(parse_3164(line, after_pri, facility, severity_num, severity, recv_ts, diag))
                }
            }
        }
        None => Ok(parse_3164(line, after_pri, facility, severity_num, severity, recv_ts, diag)),
    }
}

/// Matches the 15-byte `Mmm dd hh:mm:ss` shape (day space- or zero-padded) at the start of `s`,
/// returning `(timestamp, rest)` with one following space consumed. `None` when absent, which is
/// tolerated: nginx omits fields RFC 3164 calls mandatory.
fn parse_3164_timestamp(s: &[u8]) -> Option<(&[u8], &[u8])> {
    if s.len() < 15 {
        return None;
    }
    let ts = &s[..15];
    let digit = |i: usize| ts[i].is_ascii_digit();
    let ok = ts[0].is_ascii_alphabetic()
        && ts[1].is_ascii_alphabetic()
        && ts[2].is_ascii_alphabetic()
        && ts[3] == b' '
        && (ts[4] == b' ' || digit(4))
        && digit(5)
        && ts[6] == b' '
        && digit(7)
        && digit(8)
        && ts[9] == b':'
        && digit(10)
        && digit(11)
        && ts[12] == b':'
        && digit(13)
        && digit(14);
    if !ok {
        return None;
    }
    let after = &s[15..];
    Some((ts, after.strip_prefix(b" ").unwrap_or(after)))
}

/// A token is a TAG if it ends in `:` (`name[pid]:` included) and what precedes the colon looks
/// like a process name; the module doc's "The RFC 3164 header" says why this is stricter than
/// "ends in `:`".
fn is_tag_shaped(token: &[u8]) -> bool {
    let Some(body) = token.strip_suffix(b":") else { return false };
    if body.is_empty() {
        return false;
    }
    let name = if let Some(open) = body.iter().rposition(|&b| b == b'[') {
        if body.last() != Some(&b']') {
            return false;
        }
        let pid = &body[open + 1..body.len() - 1];
        if pid.is_empty() {
            return false;
        }
        // Numeric fitting `u64`, or PRINTUSASCII without `]` so the bracket still balances
        // (module doc, `syslog.pid`).
        let numeric_fits_u64 = pid.iter().all(|b| b.is_ascii_digit())
            && std::str::from_utf8(pid).is_ok_and(|s| s.parse::<u64>().is_ok());
        if !numeric_fits_u64 && !pid.iter().all(|&b| is_printusascii_byte(b) && b != b']') {
            return false;
        }
        &body[..open]
    } else {
        body
    };
    !name.is_empty()
        && name.iter().all(|&b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b'/'))
}

fn parse_3164(
    line: &Bytes,
    after_pri: &[u8],
    facility: u32,
    severity_num: u32,
    severity: Severity,
    recv_ts: i64,
    diag: &mut Diagnostics,
) -> Event {
    let (ts_token, after_ts) = match parse_3164_timestamp(after_pri) {
        Some((ts, rest)) => (Some(ts), rest),
        None => (None, after_pri),
    };

    let (token1, after1) = split_first_token(after_ts);
    let (hostname, tag, msg) = if is_tag_shaped(token1) {
        (None, Some(token1), after1)
    } else {
        let (token2, after2) = split_first_token(after1);
        if is_tag_shaped(token2) {
            (Some(token1), Some(token2), after2)
        } else {
            // No tag, no hostname: MSG starts at `after_ts`, not `after1`/`after2`.
            (None, None, after_ts)
        }
    };

    let mut attrs = AttrMap::new();
    attrs.insert_sym(KEYS.facility, Value::U64(facility as u64));
    attrs.insert_sym(KEYS.severity, Value::U64(severity_num as u64));
    if let Some(ts) = ts_token {
        // ASCII by construction in `parse_3164_timestamp`.
        attrs.insert_sym(KEYS.timestamp, Value::Str(slice_of(line, ts)));
    }
    if let Some(host) = hostname {
        if !host.is_empty() {
            // RFC 3164 parsing never fails (the sniff fallback depends on it), and `Value::Str`
            // must be valid UTF-8, so a non-UTF-8 HOSTNAME is skipped and reported instead.
            if std::str::from_utf8(host).is_ok() {
                attrs.insert_sym(KEYS.hostname, Value::Str(slice_of(line, host)));
            } else {
                diag.warn_throttled(
                    "hostname_not_utf8",
                    format_args!(
                        "syslog_in: RFC 3164 HOSTNAME token {:?} is not valid UTF-8; skipping \
                         the syslog.hostname attribute",
                        String::from_utf8_lossy(host)
                    ),
                );
            }
        }
    }
    if let Some(tag_token) = tag {
        let tag_body = &tag_token[..tag_token.len() - 1]; // strip the trailing ':'
        if let Some(open) = tag_body.iter().rposition(|&b| b == b'[') {
            // `is_tag_shaped` already validated the bracketed-PID shape.
            let name = &tag_body[..open];
            let pid_bytes = &tag_body[open + 1..tag_body.len() - 1];
            attrs.insert_sym(KEYS.tag, Value::Str(slice_of(line, name)));
            match std::str::from_utf8(pid_bytes).ok().and_then(|s| s.parse::<u64>().ok()) {
                Some(n) => attrs.insert_sym(KEYS.pid, Value::U64(n)),
                // `is_tag_shaped` guarantees a non-`u64` PID is PRINTUSASCII, so valid UTF-8.
                None => attrs.insert_sym(KEYS.pid, Value::Str(slice_of(line, pid_bytes))),
            }
        } else {
            attrs.insert_sym(KEYS.tag, Value::Str(slice_of(line, tag_body)));
        }
    }

    let message = message_value(line, msg, false);
    Event::log(
        recv_ts,
        attrs,
        LogRecord {
            message,
            severity: Some(severity),
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    )
}

/// `-` (RFC 5424's nil) or an empty field means absent, for every nillable field.
fn nil_or(field: &[u8]) -> Option<&[u8]> {
    if field.is_empty() || field == b"-" {
        None
    } else {
        Some(field)
    }
}

/// Validates and slices a HOSTNAME, APP-NAME, or MSGID (PROCID has its own numeric split in
/// [`parse_5424`]). Non-PRINTUSASCII is a grammar violation; `label` names the field in the error.
fn field_value(line: &Bytes, field: &[u8], label: &str) -> Result<Option<Value>, CodecError> {
    match nil_or(field) {
        None => Ok(None),
        Some(f) => {
            if !is_printusascii(f) {
                return Err(CodecError::Malformed(format!(
                    "malformed RFC 5424 syslog line: {label} {:?} is not PRINTUSASCII",
                    String::from_utf8_lossy(f)
                )));
            }
            Ok(Some(Value::Str(slice_of(line, f))))
        }
    }
}

/// Builds the MSG [`Value`]: valid UTF-8 is a [`Value::Str`], minus a leading BOM when
/// `strip_bom` is set; invalid UTF-8 is a raw [`Value::Bytes`], BOM and all (module doc).
fn message_value(line: &Bytes, msg: &[u8], strip_bom: bool) -> Value {
    match std::str::from_utf8(msg) {
        Ok(s) => {
            let s = if strip_bom { s.strip_prefix('\u{FEFF}').unwrap_or(s) } else { s };
            Value::Str(slice_of(line, s.as_bytes()))
        }
        Err(_) => Value::Bytes(slice_of(line, msg)),
    }
}

/// One STRUCTURED-DATA grammar violation. `offset` is relative to [`parse_structured_data`]'s
/// input; the caller adds its base for the `bad_line` message.
#[derive(Debug)]
struct SdError {
    offset: usize,
    message: String,
}

impl SdError {
    fn new(offset: usize, message: impl Into<String>) -> Self {
        Self { offset, message: message.into() }
    }
}

/// `true` for an `SD-NAME` byte: PRINTUSASCII (which already excludes space) minus `=`, `]`, `"`.
fn is_sd_name_byte(b: u8) -> bool {
    is_printusascii_byte(b) && !matches!(b, b'=' | b']' | b'"')
}

/// Parses one `SD-NAME` (1..=32 bytes of [`is_sd_name_byte`]), advancing `*pos` past it.
fn parse_sd_name<'a>(s: &'a [u8], pos: &mut usize) -> Result<&'a [u8], SdError> {
    let start = *pos;
    while s.get(*pos).is_some_and(|&b| is_sd_name_byte(b)) {
        *pos += 1;
    }
    let name = &s[start..*pos];
    if name.is_empty() {
        return Err(SdError::new(start, "expected an SD-NAME"));
    }
    if name.len() > 32 {
        return Err(SdError::new(
            start,
            format!(
                "SD-NAME {:?} is {} bytes, longer than RFC 5424's 32-byte limit",
                String::from_utf8_lossy(name),
                name.len()
            ),
        ));
    }
    Ok(name)
}

/// Parses a `PARAM-VALUE` from after its opening `"` through its closing `"`, unescaping RFC 5424
/// §6.3.3's `\"`, `\\`, `\]`; any other backslash is kept with its byte. The caller validates the
/// result as UTF-8.
fn parse_param_value(s: &[u8], pos: &mut usize) -> Result<Vec<u8>, SdError> {
    let mut out = Vec::new();
    loop {
        match s.get(*pos) {
            None => {
                return Err(SdError::new(*pos, "unterminated PARAM-VALUE (missing closing '\"')"))
            }
            Some(b'"') => {
                *pos += 1;
                return Ok(out);
            }
            Some(b'\\') => {
                *pos += 1;
                match s.get(*pos) {
                    Some(&b @ (b'"' | b'\\' | b']')) => {
                        out.push(b);
                        *pos += 1;
                    }
                    Some(&other) => {
                        out.push(b'\\');
                        out.push(other);
                        *pos += 1;
                    }
                    None => {
                        return Err(SdError::new(
                            *pos,
                            "unterminated PARAM-VALUE (trailing backslash)",
                        ))
                    }
                }
            }
            Some(&b) => {
                out.push(b);
                *pos += 1;
            }
        }
    }
}

/// Inserts one PARAM into `inner`; a repeated `PARAM-NAME` folds into a `Value::Array` in wire
/// order rather than the last write winning.
fn insert_param(inner: &mut AttrMap, name: Symbol, value: Bytes) {
    let value = Value::Str(value);
    let merged = match inner.remove_sym(name) {
        None => value,
        Some(Value::Array(mut arr)) => {
            arr.push(value);
            Value::Array(arr)
        }
        Some(existing) => Value::Array(vec![existing, value]),
    };
    inner.insert_sym(name, merged);
}

/// Parses RFC 5424 §6.3 STRUCTURED-DATA into `syslog.sd` (`None` for nil) and the offset into `s`
/// where MSG begins, one following space consumed. The module doc has the grammar and shape.
fn parse_structured_data(s: &[u8], keys: &mut KeyCache) -> Result<(Option<Value>, usize), SdError> {
    if let Some(rest) = s.strip_prefix(b"-") {
        return Ok((None, 1 + usize::from(rest.first() == Some(&b' '))));
    }
    if s.first() != Some(&b'[') {
        return Err(SdError::new(0, "expected '-' (nil) or '[' (the start of an SD-ELEMENT)"));
    }

    let mut pos = 0usize;
    let mut sd = AttrMap::new();
    while s.get(pos) == Some(&b'[') {
        let elem_start = pos;
        pos += 1;
        let id_bytes = parse_sd_name(s, &mut pos)?;
        let id = std::str::from_utf8(id_bytes)
            .expect("parse_sd_name only accepts PRINTUSASCII, always valid UTF-8");
        // `AttrMap::get`, not an intern: the element may still be rejected, and the sniff
        // routes RFC 3164 lines containing `[token` through here before falling back. Interning
        // now would keep producer-controlled text for the process's life, outside
        // `docs/design/memory.md` §4's accepted exposure; it happens at the insert below.
        if sd.get(id).is_some() {
            return Err(SdError::new(elem_start, format!("duplicate SD-ID {id:?}")));
        }

        let mut inner = AttrMap::new();
        loop {
            match s.get(pos) {
                Some(b']') => {
                    pos += 1;
                    break;
                }
                Some(b' ') => {
                    pos += 1;
                    let name_bytes = parse_sd_name(s, &mut pos)?;
                    let name = std::str::from_utf8(name_bytes)
                        .expect("parse_sd_name only accepts PRINTUSASCII, always valid UTF-8");
                    if s.get(pos) != Some(&b'=') {
                        return Err(SdError::new(
                            pos,
                            format!("expected '=' after PARAM-NAME {name:?}"),
                        ));
                    }
                    pos += 1;
                    if s.get(pos) != Some(&b'"') {
                        return Err(SdError::new(pos, "expected opening '\"' for PARAM-VALUE"));
                    }
                    pos += 1;
                    let value_start = pos;
                    let value_bytes = parse_param_value(s, &mut pos)?;
                    let value = String::from_utf8(value_bytes).map_err(|_| {
                        SdError::new(value_start, "PARAM-VALUE is not valid UTF-8 once unescaped")
                    })?;
                    insert_param(&mut inner, keys.get_or_intern(name), Bytes::from(value));
                }
                Some(_) => {
                    return Err(SdError::new(pos, "expected SP or ']' inside SD-ELEMENT"));
                }
                None => {
                    return Err(SdError::new(pos, "unterminated SD-ELEMENT (missing ']')"));
                }
            }
        }
        sd.insert_sym(keys.get_or_intern(id), Value::Map(Box::new(inner)));
    }

    if s.get(pos) == Some(&b' ') {
        pos += 1;
    }
    Ok((Some(Value::Map(Box::new(sd))), pos))
}

#[allow(clippy::too_many_arguments)]
fn parse_5424(
    line: &Bytes,
    after_version: &[u8],
    facility: u32,
    severity_num: u32,
    severity: Severity,
    recv_ts: i64,
    diag: &mut Diagnostics,
    keys: &mut KeyCache,
) -> Result<Event, CodecError> {
    let malformed =
        |detail: String| CodecError::Malformed(format!("malformed RFC 5424 syslog line: {detail}"));

    let (ts_field, rest) = split_first_token(after_version);
    let (host_field, rest) = split_first_token(rest);
    let (app_field, rest) = split_first_token(rest);
    let (procid_field, rest) = split_first_token(rest);
    let (msgid_field, rest) = split_first_token(rest);
    // For an absolute `SdError` offset. `rest` is a subslice of `line` via `split_first_token`,
    // so the subtraction is sound as in `slice_of`.
    let sd_base = rest.as_ptr() as usize - line.as_ptr() as usize;
    let (sd_value, sd_offset) = parse_structured_data(rest, keys).map_err(|e| {
        malformed(format!("STRUCTURED-DATA at byte {}: {}", sd_base + e.offset, e.message))
    })?;
    let msg = &rest[sd_offset..];

    let mut attrs = AttrMap::new();
    attrs.insert_sym(KEYS.facility, Value::U64(facility as u64));
    attrs.insert_sym(KEYS.severity, Value::U64(severity_num as u64));
    // Nil is an explicit `Value::Null`; unparseable rejects the line like a bad PRI; parseable
    // but out of `i64`-nanosecond range keeps the event without the attribute (module doc).
    match nil_or(ts_field) {
        None => {
            attrs.insert_sym(KEYS.timestamp, Value::Null);
        }
        Some(ts) => {
            let ts_str = std::str::from_utf8(ts)
                .map_err(|_| malformed("TIMESTAMP is not valid UTF-8".to_string()))?;
            match parse_rfc3339_to_nanos(ts_str) {
                Ok(nanos) => {
                    attrs.insert_sym(KEYS.timestamp, Value::Timestamp(nanos));
                }
                Err(TimestampError::OutOfRange) => {
                    diag.warn_throttled(
                        "timestamp_out_of_range",
                        format_args!(
                            "RFC 5424 TIMESTAMP {ts_str:?} is well-formed but names an instant \
                             outside the representable range; keeping the event without \
                             syslog.timestamp"
                        ),
                    );
                }
                Err(TimestampError::Malformed) => {
                    return Err(malformed(format!("TIMESTAMP {ts_str:?} does not parse")));
                }
            }
        }
    }
    if let Some(v) = field_value(line, host_field, "HOSTNAME")? {
        attrs.insert_sym(KEYS.hostname, v);
    }
    if let Some(v) = field_value(line, app_field, "APP-NAME")? {
        attrs.insert_sym(KEYS.tag, v);
    }
    if let Some(pid) = nil_or(procid_field) {
        // Free-form PRINTUSASCII: `U64` when numeric, else `Str` (module doc, `syslog.pid`).
        if !is_printusascii(pid) {
            return Err(malformed(format!(
                "PROCID {:?} is not PRINTUSASCII",
                String::from_utf8_lossy(pid)
            )));
        }
        match std::str::from_utf8(pid).expect("validated PRINTUSASCII above").parse::<u64>() {
            Ok(n) => {
                attrs.insert_sym(KEYS.pid, Value::U64(n));
            }
            Err(_) => {
                attrs.insert_sym(KEYS.pid, Value::Str(slice_of(line, pid)));
            }
        }
    }
    if let Some(v) = field_value(line, msgid_field, "MSGID")? {
        attrs.insert_sym(KEYS.msgid, v);
    }
    if let Some(sd) = sd_value {
        attrs.insert_sym(KEYS.sd, sd);
    }

    let message = message_value(line, msg, true);
    Ok(Event::log(
        recv_ts,
        attrs,
        LogRecord {
            message,
            severity: Some(severity),
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn decode(datagram: &str) -> Vec<Event> {
        let mut decoder = SyslogDecoder::new(Arc::new(Resource::default()));
        decoder.decode(Bytes::from(datagram.to_string())).expect("decode should succeed").events
    }

    fn decode_bytes(datagram: Vec<u8>) -> Vec<Event> {
        let mut decoder = SyslogDecoder::new(Arc::new(Resource::default()));
        decoder.decode(Bytes::from(datagram)).expect("decode should succeed").events
    }

    /// `with_diagnostics` reaches the UDP decoder as well as the driver, so `bad_line` reports
    /// under the component id.
    #[test]
    fn with_diagnostics_reaches_the_wrapped_decoder_too() {
        let input = SyslogInput::new("127.0.0.1:0").with_diagnostics(Diagnostics::new("my-id"));
        match &input.inner {
            Inner::Udp(listener) => {
                assert_eq!(listener.decoder().diag().component_id(), "my-id");
                assert_eq!(listener.diag().component_id(), "my-id");
            }
            Inner::Tcp(_) => panic!("SyslogInput::new must build a UDP listener"),
        }
    }

    /// The same on the TCP arm, which has its own `map_decoder` call.
    #[test]
    fn with_diagnostics_reaches_the_wrapped_decoder_on_the_tcp_arm_too() {
        let input = SyslogInput::tcp("127.0.0.1:0").with_diagnostics(Diagnostics::new("tcp-id"));
        match &input.inner {
            Inner::Tcp(listener) => {
                assert_eq!(listener.decoder().diag().component_id(), "tcp-id");
                assert_eq!(listener.diag().component_id(), "tcp-id");
                assert!(
                    !listener.decoder().line_splitting,
                    "with_diagnostics must not undo SyslogInput::tcp's line-splitting choice"
                );
            }
            Inner::Udp(_) => panic!("SyslogInput::tcp must build a TCP listener"),
        }
    }

    /// Events carry the caller's `received_at`, not decode time, which can lag arrival under
    /// backlog (`docs/adr/decoupled-listener-io.md`).
    #[test]
    fn decode_into_stamps_events_with_the_callers_received_at_not_the_current_time() {
        let mut decoder = SyslogDecoder::new(Arc::new(Resource::default()));
        let deliberately_not_now: i64 = 123;
        let mut out = Vec::new();
        decoder
            .decode_into(
                Bytes::from_static(b"<134>1 - - - - - - test"),
                deliberately_not_now,
                &mut out,
            )
            .expect("decode should succeed");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].timestamp, deliberately_not_now);
    }

    /// `decode_into` appends to `out`, so `logit_pipeline::BatchAccumulator` can reuse one buffer.
    #[test]
    fn decode_into_appends_to_an_already_populated_out_buffer_rather_than_replacing_it() {
        let mut decoder = SyslogDecoder::new(Arc::new(Resource::default()));
        let mut out = vec![Event::empty(0, AttrMap::new())];
        decoder
            .decode_into(Bytes::from_static(b"<134>1 - - - - - - test"), 1, &mut out)
            .expect("decode should succeed");
        assert_eq!(out.len(), 2, "the pre-existing event must survive, plus the newly decoded one");
    }

    fn only_event(events: Vec<Event>) -> Event {
        assert_eq!(events.len(), 1, "expected exactly one event");
        events.into_iter().next().unwrap()
    }

    fn message_val(event: &Event) -> &Value {
        &event.log.as_ref().expect("event should carry a log").message
    }

    fn message_str(event: &Event) -> &str {
        message_val(event).as_str().unwrap()
    }

    fn parse_err(line: &str) -> CodecError {
        let bytes = Bytes::from(line.to_string());
        let mut diag = Diagnostics::default();
        parse_line(&bytes, 0, &mut diag, &mut KeyCache::new())
            .expect_err("expected this line to be rejected")
    }

    #[test]
    fn rfc3164_with_hostname_decodes_message_severity_and_attributes() {
        let event =
            only_event(decode(r#"<134>Aug 30 10:00:00 myhost nginx_access: {"status":200}"#));
        assert_eq!(message_str(&event), r#"{"status":200}"#);
        assert_eq!(event.log.as_ref().unwrap().severity, Some(Severity::Info)); // 134 % 8 = 6
        assert_eq!(event.attributes.get("syslog.facility"), Some(&Value::U64(134 / 8)));
        assert_eq!(event.attributes.get("syslog.severity"), Some(&Value::U64(6)));
        assert_eq!(event.attributes.get("syslog.hostname").and_then(Value::as_str), Some("myhost"));
        assert_eq!(
            event.attributes.get("syslog.tag").and_then(Value::as_str),
            Some("nginx_access")
        );
        assert_eq!(
            event.attributes.get("syslog.timestamp").and_then(Value::as_str),
            Some("Aug 30 10:00:00")
        );
    }

    #[test]
    fn rfc3164_without_hostname_decodes_with_tag_identified_and_no_hostname_attribute() {
        let event = only_event(decode(r#"<134>Aug 30 10:00:00 nginx_access: {"status":200}"#));
        assert_eq!(message_str(&event), r#"{"status":200}"#);
        assert!(event.attributes.get("syslog.hostname").is_none());
        assert_eq!(
            event.attributes.get("syslog.tag").and_then(Value::as_str),
            Some("nginx_access")
        );
    }

    /// A skipped non-UTF-8 RFC 3164 HOSTNAME stays observable, as
    /// `logit.component.diagnostics{key="hostname_not_utf8"}`.
    #[test]
    fn a_non_utf8_rfc3164_hostname_is_skipped_with_a_throttled_diagnostic() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog", "input");
        let diag = Diagnostics::new("syslog_in").with_telemetry(telemetry);
        let mut decoder = SyslogDecoder::new(Arc::new(Resource::default())).with_diagnostics(diag);

        let mut line = b"<134>Aug 30 10:00:00 ".to_vec();
        line.push(0xff); // not valid UTF-8 on its own
        line.extend_from_slice(b" nginx_access: hi");
        let batch = decoder
            .decode(Bytes::from(line))
            .expect("RFC 3164 parsing never fails outright over a bad HOSTNAME");
        assert_eq!(batch.events.len(), 1);
        assert!(
            batch.events[0].attributes.get("syslog.hostname").is_none(),
            "a non-UTF-8 HOSTNAME token must not be stamped as an attribute"
        );
        assert_eq!(
            batch.events[0].attributes.get("syslog.tag").and_then(Value::as_str),
            Some("nginx_access"),
            "the rest of the line must still parse"
        );

        let events = registry.drain(0);
        let fired = events
            .iter()
            .any(|e| e.attributes.get("key").and_then(|v| v.as_str()) == Some("hostname_not_utf8"));
        assert!(fired, "expected logit.component.diagnostics{{key=\"hostname_not_utf8\"}}");
    }

    #[test]
    fn rfc3164_json_body_containing_colon_space_is_kept_whole() {
        // A tag-less JSON body's leading `{"key":` token must not be taken for a tag.
        let line = r#"<134>Aug 30 10:00:00 {"status": 200, "path": "/foo: bar"}"#;
        let event = only_event(decode(line));
        assert_eq!(message_str(&event), r#"{"status": 200, "path": "/foo: bar"}"#);
        assert!(event.attributes.get("syslog.tag").is_none());
        assert!(event.attributes.get("syslog.hostname").is_none());
    }

    #[test]
    fn rfc3164_tag_with_pid_splits_into_tag_and_pid() {
        let event = only_event(decode("<13>tag[1234]: hello"));
        assert_eq!(event.attributes.get("syslog.tag").and_then(Value::as_str), Some("tag"));
        assert_eq!(event.attributes.get("syslog.pid"), Some(&Value::U64(1234)));
        assert_eq!(message_str(&event), "hello");
    }

    #[test]
    fn rfc3164_tag_with_an_overflowing_numeric_pid_becomes_a_str_pid_not_a_panic() {
        // A PID that overflows `u64` must not panic; it becomes `Value::Str` (module doc,
        // `syslog.pid`) and the `tag[pid]:` token stays out of the message.
        let line = "<13>tag[99999999999999999999]: hello";
        let event = only_event(decode(line));
        assert_eq!(event.attributes.get("syslog.tag").and_then(Value::as_str), Some("tag"));
        assert_eq!(
            event.attributes.get("syslog.pid").and_then(Value::as_str),
            Some("99999999999999999999")
        );
        assert_eq!(message_str(&event), "hello");
    }

    #[test]
    fn rfc3164_non_numeric_bracketed_pid_becomes_a_str_pid() {
        let event = only_event(decode("<13>tag[abc]: hello"));
        assert_eq!(event.attributes.get("syslog.tag").and_then(Value::as_str), Some("tag"));
        assert_eq!(event.attributes.get("syslog.pid").and_then(Value::as_str), Some("abc"));
        assert_eq!(message_str(&event), "hello");
    }

    #[test]
    fn rfc3164_tag_with_no_trailing_space_and_empty_message_does_not_panic() {
        // A TAG-shaped token with nothing after it: the empty MSG must be a real subslice of the
        // line, or `slice_of`'s pointer arithmetic underflows and panics.
        let event = only_event(decode("<13>nginx:"));
        assert_eq!(event.attributes.get("syslog.tag").and_then(Value::as_str), Some("nginx"));
        assert_eq!(message_str(&event), "");

        // Same hazard one token later: the empty MSG this time comes from `after2`.
        let event = only_event(decode("<13>myhost nginx:"));
        assert_eq!(event.attributes.get("syslog.hostname").and_then(Value::as_str), Some("myhost"));
        assert_eq!(event.attributes.get("syslog.tag").and_then(Value::as_str), Some("nginx"));
        assert_eq!(message_str(&event), "");
    }

    #[test]
    fn rfc5424_decodes_msgid_and_timestamp_and_stamps_null_for_a_nil_one() {
        let event = only_event(decode(
            "<134>1 2003-10-11T22:14:15.003Z myhost app 1234 ID47 - some message",
        ));
        assert_eq!(message_str(&event), "some message");
        assert_eq!(event.attributes.get("syslog.hostname").and_then(Value::as_str), Some("myhost"));
        assert_eq!(event.attributes.get("syslog.tag").and_then(Value::as_str), Some("app"));
        assert_eq!(event.attributes.get("syslog.pid"), Some(&Value::U64(1234)));
        assert_eq!(event.attributes.get("syslog.msgid").and_then(Value::as_str), Some("ID47"));
        assert_eq!(
            event.attributes.get("syslog.timestamp"),
            Some(&Value::Timestamp(1_065_910_455_003_000_000))
        );

        let event = only_event(decode("<134>1 - - - - - - nil fields"));
        assert_eq!(
            event.attributes.get("syslog.timestamp"),
            Some(&Value::Null),
            "a nil (`-`) TIMESTAMP now stamps an explicit Value::Null rather than omitting the \
             attribute entirely"
        );
        assert!(event.attributes.get("syslog.hostname").is_none());
        assert!(event.attributes.get("syslog.tag").is_none());
        assert!(event.attributes.get("syslog.pid").is_none());
        assert!(event.attributes.get("syslog.msgid").is_none());
        assert!(event.attributes.get("syslog.sd").is_none());
        assert_eq!(message_str(&event), "nil fields");
    }

    #[test]
    fn rfc5424_non_numeric_procid_becomes_a_str_pid() {
        let event = only_event(decode("<134>1 - - - notanumber - - msg"));
        assert_eq!(event.attributes.get("syslog.pid").and_then(Value::as_str), Some("notanumber"));
    }

    #[test]
    fn rfc5424_invalid_timestamp_is_rejected_rather_than_treated_as_absent() {
        for ts in [
            "2024-02-31T00:00:00Z",      // February has no 31st day
            "2023-02-29T00:00:00Z",      // 2023 is not a leap year
            "2024-13-01T00:00:00Z",      // month 13
            "2024-01-01T00:00:00+99:99", // offset hour/minute both out of range
            "2024-01-01T23:59:60Z",      // RFC 5424 forbids leap seconds
            "2024-01-01t00:00:00z",      // lowercase t/z
            // 10 fractional digits, past nanoseconds. RFC 5424 allows 6, but the shared
            // `logit_core::time` RFC 3339 parser accepts up to 9, a harmless leniency.
            "2024-01-01T00:00:00.1234567890Z",
        ] {
            let line = format!("<134>1 {ts} - - - - - msg");
            assert!(
                matches!(parse_err(&line), CodecError::Malformed(_)),
                "expected timestamp {ts:?} to be rejected"
            );
        }
    }

    #[test]
    fn rfc5424_valid_timestamps_at_the_edges_are_accepted() {
        // 2024 is a leap year: Feb 29 is valid; six fractional digits is the RFC 5424 maximum;
        // an explicit numeric offset is legal alongside `Z`.
        for ts in
            ["2024-02-29T00:00:00Z", "2024-01-01T00:00:00.123456Z", "2024-01-01T00:00:00+23:59"]
        {
            let line = format!("<134>1 {ts} - - - - - msg");
            let event = only_event(decode(&line));
            assert!(
                event.attributes.get("syslog.timestamp").is_some(),
                "expected timestamp {ts:?} to be accepted"
            );
        }
    }

    #[test]
    fn rfc5424_structured_data_with_an_escaped_bracket_is_parsed_not_skipped() {
        let event = only_event(decode(r#"<134>1 - - - - - [id@32473 k="v\]"] the message"#));
        assert_eq!(message_str(&event), "the message");
        let mut params = AttrMap::new();
        params.insert("k", Value::str("v]"));
        let mut sd = AttrMap::new();
        sd.insert("id@32473", Value::Map(Box::new(params)));
        assert_eq!(event.attributes.get("syslog.sd"), Some(&Value::Map(Box::new(sd))));
    }

    #[test]
    fn rfc5424_timestamp_outside_the_representable_range_is_kept_without_the_attribute() {
        // Well-formed but outside the `i64` nanosecond range (roughly 1677-09-21 to 2262-04-11):
        // the record is kept without `syslog.timestamp`, not discarded as malformed.
        for ts in ["2400-01-01T00:00:00Z", "1000-01-01T00:00:00Z"] {
            let line = format!("<134>1 {ts} h a 1 - - msg");
            let event = only_event(decode(&line));
            assert!(
                event.attributes.get("syslog.timestamp").is_none(),
                "expected timestamp {ts:?} to be omitted, not attached"
            );
            // The rest of the line parsed fine and must still be kept.
            assert_eq!(event.attributes.get("syslog.hostname").and_then(Value::as_str), Some("h"));
            assert_eq!(event.attributes.get("syslog.tag").and_then(Value::as_str), Some("a"));
            assert_eq!(event.attributes.get("syslog.pid"), Some(&Value::U64(1)));
            assert_eq!(message_str(&event), "msg");
        }
    }

    #[test]
    fn rfc5424_message_starting_with_a_bom_has_it_stripped() {
        let line = "<134>1 - - - - - - \u{FEFF}hello";
        let event = only_event(decode(line));
        assert_eq!(message_str(&event), "hello");
    }

    #[test]
    fn a_digit_led_rfc3164_message_that_fails_as_rfc5424_falls_back_instead_of_being_dropped() {
        // "4 requests failed" sniffs as RFC 5424 version 4 and fails `parse_5424`, so it must
        // fall back to RFC 3164 as an untagged, hostname-less message.
        let event = only_event(decode("<13>4 requests failed"));
        assert_eq!(message_str(&event), "4 requests failed");
        assert!(event.attributes.get("syslog.tag").is_none());
        assert!(event.attributes.get("syslog.hostname").is_none());
    }

    #[test]
    fn malformed_or_absent_priority_is_a_clear_skip_and_continue() {
        for line in ["no priority here", "<>msg", "<abc>msg", "<1234>msg"] {
            assert!(
                matches!(parse_err(line), CodecError::Malformed(_)),
                "expected {line:?} to be rejected"
            );
        }
    }

    #[test]
    fn pri_out_of_range_or_with_a_leading_zero_is_rejected() {
        for line in ["<192>msg", "<999>msg", "<013>msg", "<00>msg"] {
            assert!(
                matches!(parse_err(line), CodecError::Malformed(_)),
                "expected {line:?} to be rejected"
            );
        }
    }

    #[test]
    fn pri_at_the_boundary_of_the_valid_range_is_accepted() {
        let event = only_event(decode("<191>msg"));
        assert_eq!(event.attributes.get("syslog.facility"), Some(&Value::U64(191 / 8)));
        // "<0>" is the one legal single-digit-zero PRI -- not a rejected leading zero.
        let event = only_event(decode("<0>msg"));
        assert_eq!(event.attributes.get("syslog.facility"), Some(&Value::U64(0)));
    }

    #[test]
    fn multi_line_datagram_with_one_bad_line_still_emits_the_good_ones() {
        let events = decode("<13>a\nnot a syslog line\n<13>b");
        assert_eq!(events.len(), 2);
        assert_eq!(message_str(&events[0]), "a");
        assert_eq!(message_str(&events[1]), "b");
    }

    #[test]
    fn multi_line_datagram_with_an_invalid_utf8_line_still_emits_the_good_ones() {
        // One invalid-UTF-8, PRI-less line must not take its good sibling lines with it.
        let mut datagram = Vec::new();
        datagram.extend_from_slice(b"<13>a\n");
        datagram.extend_from_slice(&[0xff, 0xfe]); // not valid UTF-8, no `<PRI>` either
        datagram.push(b'\n');
        datagram.extend_from_slice(b"<13>b");
        let events = decode_bytes(datagram);
        assert_eq!(events.len(), 2);
        assert_eq!(message_str(&events[0]), "a");
        assert_eq!(message_str(&events[1]), "b");
    }

    #[test]
    fn rfc5424_non_utf8_message_becomes_bytes_with_header_fields_intact() {
        let mut datagram = Vec::new();
        datagram.extend_from_slice(b"<134>1 2003-10-11T22:14:15.003Z myhost app 123 - - ");
        datagram.extend_from_slice(&[0xff, 0xfe, b'x']);
        let event = only_event(decode_bytes(datagram));
        assert_eq!(event.attributes.get("syslog.hostname").and_then(Value::as_str), Some("myhost"));
        assert_eq!(event.attributes.get("syslog.tag").and_then(Value::as_str), Some("app"));
        assert_eq!(event.attributes.get("syslog.pid"), Some(&Value::U64(123)));
        match message_val(&event) {
            Value::Bytes(b) => assert_eq!(b.as_ref(), &[0xff, 0xfe, b'x']),
            other => panic!("expected Value::Bytes, got {other:?}"),
        }
    }

    #[test]
    fn rfc3164_non_utf8_message_becomes_bytes_with_header_fields_intact() {
        let mut datagram = Vec::new();
        datagram.extend_from_slice(b"<13>tag[1]: ");
        datagram.extend_from_slice(&[0xff, 0xfe, b'x']);
        let event = only_event(decode_bytes(datagram));
        assert_eq!(event.attributes.get("syslog.tag").and_then(Value::as_str), Some("tag"));
        assert_eq!(event.attributes.get("syslog.pid"), Some(&Value::U64(1)));
        match message_val(&event) {
            Value::Bytes(b) => assert_eq!(b.as_ref(), &[0xff, 0xfe, b'x']),
            other => panic!("expected Value::Bytes, got {other:?}"),
        }
    }

    #[test]
    fn valid_utf8_message_is_still_a_str_not_bytes() {
        let event = only_event(decode("<13>hello"));
        assert!(matches!(message_val(&event), Value::Str(_)));
    }

    #[test]
    fn message_whitespace_is_preserved_not_trimmed() {
        // Trailing MSG spaces are payload, and an all-whitespace MSG is not a blank line.
        let event = only_event(decode("<13>tag: value  "));
        assert_eq!(message_str(&event), "value  ");

        let event = only_event(decode("<13>tag:    "));
        assert_eq!(message_str(&event), "   ");
    }

    #[test]
    fn each_severity_number_maps_to_the_expected_severity() {
        let expected = [
            (0, Severity::Fatal),
            (1, Severity::Fatal),
            (2, Severity::Fatal),
            (3, Severity::Error),
            (4, Severity::Warn),
            (5, Severity::Info),
            (6, Severity::Info),
            (7, Severity::Debug),
        ];
        for (n, sev) in expected {
            let event = only_event(decode(&format!("<{n}>msg")));
            assert_eq!(event.log.as_ref().unwrap().severity, Some(sev), "severity {n}");
        }
    }

    #[test]
    fn emitted_message_is_a_zero_copy_slice_of_the_datagram() {
        let datagram = r#"<134>Aug 30 10:00:00 nginx_access: {"status":200}"#;
        let bytes = Bytes::from(datagram.to_string());
        let mut decoder = SyslogDecoder::new(Arc::new(Resource::default()));
        let event = only_event(decoder.decode(bytes.clone()).unwrap().events);
        let msg = match &event.log.as_ref().unwrap().message {
            Value::Str(b) => b.clone(),
            other => panic!("expected Value::Str, got {other:?}"),
        };
        let base_start = bytes.as_ptr() as usize;
        let base_end = base_start + bytes.len();
        let msg_start = msg.as_ptr() as usize;
        let msg_end = msg_start + msg.len();
        assert!(
            msg_start >= base_start && msg_end <= base_end,
            "message should be a slice of the original datagram, not a copy"
        );
    }

    #[test]
    fn blank_lines_are_skipped() {
        let events = decode("\n\n<13>a\n\n");
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn every_emitted_event_is_log_only() {
        let event = only_event(decode("<13>hello"));
        assert!(event.metrics.is_empty(), "syslog_in emits log-only events");
        assert!(event.span.is_none(), "syslog_in emits log-only events");
    }

    // ---- RFC 5424 section 6.5 examples ---------------------------------------------------------
    //
    // Transcribed from memory of the RFC 5424 text, not copied from a fetched copy: verify them
    // word for word against the RFC before relying on them as a fidelity gate.

    #[test]
    fn rfc5424_section_6_5_example_1_nil_sd_with_bom_message() {
        let line = "<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - ID47 - \
                     \u{FEFF}'su root' failed for lonvick on /dev/pts/8";
        let event = only_event(decode(line));
        assert_eq!(message_str(&event), "'su root' failed for lonvick on /dev/pts/8");
        assert_eq!(event.attributes.get("syslog.facility"), Some(&Value::U64(4)));
        assert_eq!(event.attributes.get("syslog.severity"), Some(&Value::U64(2)));
        assert_eq!(
            event.attributes.get("syslog.hostname").and_then(Value::as_str),
            Some("mymachine.example.com")
        );
        assert_eq!(event.attributes.get("syslog.tag").and_then(Value::as_str), Some("su"));
        assert!(event.attributes.get("syslog.pid").is_none());
        assert_eq!(event.attributes.get("syslog.msgid").and_then(Value::as_str), Some("ID47"));
        assert!(event.attributes.get("syslog.sd").is_none());
        assert_eq!(
            event.attributes.get("syslog.timestamp"),
            Some(&Value::Timestamp(1_065_910_455_003_000_000))
        );
    }

    #[test]
    fn rfc5424_section_6_5_example_2_negative_offset_and_numeric_procid() {
        let line = "<165>1 2003-08-24T05:14:15.000003-07:00 192.0.2.1 myproc 8710 - - \
                     %% It's time to make the do-nuts.";
        let event = only_event(decode(line));
        assert_eq!(message_str(&event), "%% It's time to make the do-nuts.");
        assert_eq!(event.attributes.get("syslog.facility"), Some(&Value::U64(20)));
        assert_eq!(event.attributes.get("syslog.severity"), Some(&Value::U64(5)));
        assert_eq!(
            event.attributes.get("syslog.hostname").and_then(Value::as_str),
            Some("192.0.2.1")
        );
        assert_eq!(event.attributes.get("syslog.tag").and_then(Value::as_str), Some("myproc"));
        assert_eq!(event.attributes.get("syslog.pid"), Some(&Value::U64(8710)));
        assert!(event.attributes.get("syslog.msgid").is_none());
        assert!(event.attributes.get("syslog.sd").is_none());
        assert!(event.attributes.get("syslog.timestamp").is_some());
    }

    #[test]
    fn rfc5424_section_6_5_example_3_one_sd_element_with_three_params() {
        let line = "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 \
                     [exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"] \
                     \u{FEFF}An application event log entry...";
        let event = only_event(decode(line));
        assert_eq!(message_str(&event), "An application event log entry...");
        assert_eq!(
            event.attributes.get("syslog.hostname").and_then(Value::as_str),
            Some("mymachine.example.com")
        );
        assert_eq!(event.attributes.get("syslog.tag").and_then(Value::as_str), Some("evntslog"));
        assert!(event.attributes.get("syslog.pid").is_none());
        assert_eq!(event.attributes.get("syslog.msgid").and_then(Value::as_str), Some("ID47"));

        let mut params = AttrMap::new();
        params.insert("iut", Value::str("3"));
        params.insert("eventSource", Value::str("Application"));
        params.insert("eventID", Value::str("1011"));
        let mut sd = AttrMap::new();
        sd.insert("exampleSDID@32473", Value::Map(Box::new(params)));
        assert_eq!(event.attributes.get("syslog.sd"), Some(&Value::Map(Box::new(sd))));
    }

    /// A repeat line with the same structured data (params reordered) interns nothing new, and
    /// the `KeyCache` holds exactly the id and three param names. `nextest` runs each test in its
    /// own process, so `interner::len()` reflects only this test.
    #[test]
    fn repeat_sd_ids_and_param_names_are_cache_hits() {
        let mut decoder = SyslogDecoder::new(Arc::new(Resource::default()));
        let first = "<165>1 2003-10-11T22:14:15.003Z host app - ID47 \
                     [sdcache@1 sdc_iut=\"3\" sdc_src=\"App\" sdc_id=\"1011\"] one\n";
        let second = "<165>1 2003-10-11T22:14:16.003Z host app - ID48 \
                      [sdcache@1 sdc_id=\"1012\" sdc_iut=\"4\" sdc_src=\"App\"] two\n";
        drop(decoder.decode(Bytes::from(first)).expect("decode should succeed"));
        assert_eq!(decoder.keys.len(), 4, "one SD-ID plus three PARAM-NAMEs");

        let before = logit_core::interner::len();
        let events = decoder.decode(Bytes::from(second)).expect("decode should succeed").events;
        assert_eq!(logit_core::interner::len(), before, "nothing new to intern on a repeat");
        assert_eq!(decoder.keys.len(), 4);

        let event = only_event(events);
        let mut params = AttrMap::new();
        params.insert("sdc_iut", Value::str("4"));
        params.insert("sdc_src", Value::str("App"));
        params.insert("sdc_id", Value::str("1012"));
        let mut sd = AttrMap::new();
        sd.insert("sdcache@1", Value::Map(Box::new(params)));
        assert_eq!(event.attributes.get("syslog.sd"), Some(&Value::Map(Box::new(sd))));
        assert_eq!(event.attributes.get("syslog.msgid").and_then(Value::as_str), Some("ID48"));
    }

    #[test]
    fn rfc5424_section_6_5_example_4_two_sd_elements() {
        let line = "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 \
                     [exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"]\
                     [examplePriority@32473 class=\"high\"] \
                     \u{FEFF}An application event log entry...";
        let event = only_event(decode(line));
        assert_eq!(message_str(&event), "An application event log entry...");

        let mut params1 = AttrMap::new();
        params1.insert("iut", Value::str("3"));
        params1.insert("eventSource", Value::str("Application"));
        params1.insert("eventID", Value::str("1011"));
        let mut params2 = AttrMap::new();
        params2.insert("class", Value::str("high"));
        let mut sd = AttrMap::new();
        sd.insert("exampleSDID@32473", Value::Map(Box::new(params1)));
        sd.insert("examplePriority@32473", Value::Map(Box::new(params2)));
        assert_eq!(event.attributes.get("syslog.sd"), Some(&Value::Map(Box::new(sd))));
    }

    // ---- STRUCTURED-DATA grammar coverage beyond the RFC examples above -------------------------

    #[test]
    fn structured_data_unescapes_all_three_escape_sequences_in_one_param_value() {
        let line = "<134>1 - - - - - [ex@32473 p=\"a\\\"b\\\\c\\]d\"] msg";
        let event = only_event(decode(line));
        let mut params = AttrMap::new();
        params.insert("p", Value::str("a\"b\\c]d"));
        let mut sd = AttrMap::new();
        sd.insert("ex@32473", Value::Map(Box::new(params)));
        assert_eq!(event.attributes.get("syslog.sd"), Some(&Value::Map(Box::new(sd))));
        assert_eq!(message_str(&event), "msg");
    }

    #[test]
    fn structured_data_keeps_a_literal_backslash_before_a_non_escape_character() {
        let line = "<134>1 - - - - - [ex@32473 p=\"a\\qb\"] msg";
        let event = only_event(decode(line));
        let mut params = AttrMap::new();
        params.insert("p", Value::str("a\\qb"));
        let mut sd = AttrMap::new();
        sd.insert("ex@32473", Value::Map(Box::new(params)));
        assert_eq!(event.attributes.get("syslog.sd"), Some(&Value::Map(Box::new(sd))));
    }

    #[test]
    fn structured_data_repeated_param_name_becomes_an_array_in_order() {
        let line = r#"<134>1 - - - - - [ex@32473 p="1" p="2"] msg"#;
        let event = only_event(decode(line));
        let mut params = AttrMap::new();
        params.insert("p", Value::Array(vec![Value::str("1"), Value::str("2")]));
        let mut sd = AttrMap::new();
        sd.insert("ex@32473", Value::Map(Box::new(params)));
        assert_eq!(event.attributes.get("syslog.sd"), Some(&Value::Map(Box::new(sd))));
    }

    #[test]
    fn structured_data_dotted_sd_id_is_accepted() {
        let line = r#"<134>1 - - - - - [ex.mp@32473 k="v"] msg"#;
        let event = only_event(decode(line));
        let mut params = AttrMap::new();
        params.insert("k", Value::str("v"));
        let mut sd = AttrMap::new();
        sd.insert("ex.mp@32473", Value::Map(Box::new(params)));
        assert_eq!(event.attributes.get("syslog.sd"), Some(&Value::Map(Box::new(sd))));
    }

    #[test]
    fn structured_data_33_char_sd_name_is_rejected() {
        let id = "a".repeat(33);
        let line = format!(r#"<134>1 - - - - - [{id} k="v"] msg"#);
        assert!(
            matches!(parse_err(&line), CodecError::Malformed(_)),
            "a 33-byte SD-NAME exceeds RFC 5424's 32-byte limit and must be rejected"
        );
    }

    #[test]
    fn structured_data_duplicate_sd_id_is_rejected() {
        let line = r#"<134>1 - - - - - [ex@32473 k="v"][ex@32473 j="w"] msg"#;
        assert!(matches!(parse_err(line), CodecError::Malformed(_)));
    }

    /// A line rejected inside `parse_structured_data` interns nothing, whether it is an RFC 3164
    /// line routed there by the sniff or a version-`1` line with a malformed SD-ELEMENT
    /// (`docs/design/memory.md` §4). `nextest` runs each test in its own process, so
    /// `interner::len()` reflects only this test.
    #[test]
    fn a_line_rejected_inside_structured_data_interns_nothing() {
        let mut decoder = SyslogDecoder::new(Arc::new(Resource::default()));
        // Warm-up: initializes `KEYS` before the window opens.
        drop(
            decoder
                .decode(Bytes::from_static(b"<134>1 - - - - - - warm"))
                .expect("decode should succeed"),
        );

        let before = logit_core::interner::len();

        // The sniff fallback: version `4` is really RFC 3164 MSG text, and so is `[session-8f3a1c`.
        let fallback = Bytes::from_static(b"<13>4 requests failed in pool A [session-8f3a1c retry");
        let events = decoder.decode(fallback).expect("decode should succeed").events;
        assert_eq!(
            message_str(&only_event(events)),
            "4 requests failed in pool A [session-8f3a1c retry",
            "the line falls back to RFC 3164 and keeps its whole MSG"
        );

        // A genuine version-`1` line whose SD-ELEMENT is malformed: rejected, no event at all.
        let rejected = Bytes::from_static(b"<134>1 - - - - - [badelem@1 msg");
        let events = decoder.decode(rejected).expect("decode should succeed").events;
        assert!(events.is_empty(), "a malformed SD-ELEMENT rejects the whole line");

        assert_eq!(
            logit_core::interner::len(),
            before,
            "an SD-ID from a line that never validated must not reach the interner"
        );
    }

    #[test]
    fn structured_data_unterminated_element_is_rejected() {
        let line = r#"<134>1 - - - - - [ex@32473 k="v""#;
        assert!(matches!(parse_err(line), CodecError::Malformed(_)));
    }

    #[test]
    fn structured_data_unescaped_closing_bracket_inside_a_quoted_value_is_kept_literal() {
        let line = r#"<134>1 - - - - - [ex@32473 k="a]b"] msg"#;
        let event = only_event(decode(line));
        let mut params = AttrMap::new();
        params.insert("k", Value::str("a]b"));
        let mut sd = AttrMap::new();
        sd.insert("ex@32473", Value::Map(Box::new(params)));
        assert_eq!(event.attributes.get("syslog.sd"), Some(&Value::Map(Box::new(sd))));
    }

    // ---- line splitting off (the TCP framing path) --------------------------------------------

    /// With splitting off, an octet-counted MSG's `\n` stays content.
    #[test]
    fn with_line_splitting_off_treats_the_whole_buffer_as_one_line() {
        let mut decoder =
            SyslogDecoder::new(Arc::new(Resource::default())).with_line_splitting(false);
        let mut out = Vec::new();
        decoder
            .decode_into(
                Bytes::from_static(b"<134>1 - - - - - - first\nsecond\nthird"),
                7,
                &mut out,
            )
            .expect("decode should succeed");
        assert_eq!(out.len(), 1, "the embedded newlines are message content, not delimiters");
        assert_eq!(message_str(&out[0]), "first\nsecond\nthird");
    }

    /// `decode_into` with splitting off, for reuse across the terminator cases below.
    fn decode_framed(frame: &[u8]) -> Vec<Event> {
        let mut decoder =
            SyslogDecoder::new(Arc::new(Resource::default())).with_line_splitting(false);
        let mut out = Vec::new();
        decoder
            .decode_into(Bytes::copy_from_slice(frame), 7, &mut out)
            .expect("decode should succeed");
        out
    }

    /// A `\r\n` counted inside MSG-LEN is stripped, matching the same line over UDP.
    #[test]
    fn with_line_splitting_off_strips_a_counted_crlf_terminator() {
        assert_eq!(
            message_str(&only_event(decode_framed(b"<134>1 - - - - - - hello\r\n"))),
            "hello"
        );
        assert_eq!(
            message_str(&only_event(decode("<134>1 - - - - - - hello\r\n"))),
            "hello",
            "the identical bytes over UDP must agree"
        );
    }

    /// A counted bare `\n` is stripped too.
    #[test]
    fn with_line_splitting_off_strips_a_counted_lf_terminator() {
        assert_eq!(message_str(&only_event(decode_framed(b"<134>1 - - - - - - hello\n"))), "hello");
    }

    /// A payload `\r` left after the framer unwrapped `...hello\r\r\n` is kept, as over UDP.
    #[test]
    fn with_line_splitting_off_keeps_a_payload_cr_the_framer_already_unwrapped() {
        // What `Framer::next_line` hands the decoder for the wire bytes `...hello\r\r\n`.
        assert_eq!(
            message_str(&only_event(decode_framed(b"<134>1 - - - - - - hello\r"))),
            "hello\r"
        );
        // The same wire bytes through the UDP arm.
        assert_eq!(message_str(&only_event(decode("<134>1 - - - - - - hello\r\r\n"))), "hello\r");
    }

    #[test]
    fn with_line_splitting_off_skips_an_empty_buffer() {
        let mut decoder =
            SyslogDecoder::new(Arc::new(Resource::default())).with_line_splitting(false);
        let mut out = Vec::new();
        decoder
            .decode_into(Bytes::from_static(b""), 7, &mut out)
            .expect("an empty frame is not an error");
        assert!(out.is_empty(), "an empty frame carries no message");
    }

    /// Splitting is on by default, as UDP needs.
    #[test]
    fn line_splitting_is_on_by_default() {
        assert_eq!(decode("<13>a\n<13>b\n").len(), 2);
    }

    // ---- TCP end to end (`transport: tcp`) ----------------------------------------------------

    /// Binds an ephemeral TCP port through `Input::bind`, then starts the listener, with no
    /// bind-drop race.
    async fn running_tcp_input(
        tls: Option<&TlsServerSettings>,
    ) -> (String, tokio::task::JoinHandle<()>, tokio::sync::mpsc::Receiver<logit_pipeline::Delivered>)
    {
        running_tcp_input_with(tls, None).await
    }

    /// [`running_tcp_input`] with a `Diagnostics` attached, so a test can read the component's
    /// own occurrence counts back.
    async fn running_tcp_input_with(
        tls: Option<&TlsServerSettings>,
        diag: Option<Diagnostics>,
    ) -> (String, tokio::task::JoinHandle<()>, tokio::sync::mpsc::Receiver<logit_pipeline::Delivered>)
    {
        let mut input = SyslogInput::tcp("127.0.0.1:0").with_tcp_receive(TcpListenerConfig {
            // One event per batch, no timer: a multiline message split into two events arrives
            // as two batches, not one.
            batch_max_events: 1,
            batch_flush_interval: Duration::ZERO,
            ..TcpListenerConfig::default()
        });
        if let Some(settings) = tls {
            input = input
                .with_tls(settings, &testdata_tls_dir())
                .expect("the committed testdata/tls fixtures should load");
        }
        if let Some(diag) = diag {
            input = input.with_diagnostics(diag);
        }
        input.bind().await.expect("binding an ephemeral port should succeed");
        let addr = input.local_addr().expect("bind() leaves a real address behind").to_string();

        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let sink = Fanout::new(vec![tx]);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            // Held for the task's life: dropping it would fail every `changed()` await in the
            // driver. Tests abort the handle instead.
            let _shutdown_tx = shutdown_tx;
            let _ = input.run_until_shutdown(sink, shutdown_rx).await;
        });
        (addr, handle, rx)
    }

    async fn recv_events(
        rx: &mut tokio::sync::mpsc::Receiver<logit_pipeline::Delivered>,
    ) -> Vec<Event> {
        let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("a batch should be delivered within 5s")
            .expect("the fanout should not have closed");
        logit_pipeline::unwrap_batch(delivered).events
    }

    fn assert_nginx_line(event: &Event) {
        assert_eq!(event.attributes.get("syslog.tag").and_then(Value::as_str), Some("nginx"));
        assert_eq!(event.log.as_ref().unwrap().severity, Some(Severity::Info)); // 134 % 8 = 6
        assert_eq!(message_str(event), "hello over tcp");
    }

    /// `bad_line` throttles listener-wide: two connections rejecting two messages each leave the
    /// component's count at 4, where per-connection counting would leave it at 0.
    ///
    /// Each connection's final good line orders the assertion: a connection decodes in order, so
    /// its event proves the bad lines ahead of it were absorbed.
    #[tokio::test]
    async fn bad_line_throttles_across_connections() {
        let diag = Diagnostics::new("syslog_in");
        let (addr, handle, mut rx) = running_tcp_input_with(None, Some(diag.clone())).await;

        for connection in 0..2 {
            let mut client = tokio::net::TcpStream::connect(&addr).await.unwrap();
            tokio::io::AsyncWriteExt::write_all(
                &mut client,
                b"not a syslog line\nnor is this one\n\
                  <134>Aug 30 10:00:00 myhost nginx: hello over tcp\n",
            )
            .await
            .unwrap();

            let events = recv_events(&mut rx).await;
            assert_eq!(
                events.len(),
                1,
                "connection {connection}: only the well-formed line becomes an event"
            );
            assert_nginx_line(&events[0]);
        }

        assert_eq!(
            diag.occurrences("bad_line"),
            4,
            "two connections rejecting two messages each must count on one listener-wide \
             throttle -- a decoder clone with counts of its own would leave this at 0, having \
             counted 2 in each throwaway copy"
        );
        handle.abort();
    }

    /// rsyslog's `omfwd` default framing (RFC 6587 section 3.4.2) through a real listener.
    #[tokio::test]
    async fn tcp_decodes_an_lf_framed_message_end_to_end() {
        let (addr, handle, mut rx) = running_tcp_input(None).await;
        let mut client = tokio::net::TcpStream::connect(&addr).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(
            &mut client,
            b"<134>Aug 30 10:00:00 myhost nginx: hello over tcp\n",
        )
        .await
        .unwrap();

        let events = recv_events(&mut rx).await;
        assert_eq!(events.len(), 1);
        assert_nginx_line(&events[0]);
        handle.abort();
    }

    /// `syslog_out`'s TCP framing (RFC 6587 section 3.4.1), detected from the leading digit.
    #[tokio::test]
    async fn tcp_decodes_an_octet_counted_message_end_to_end() {
        let (addr, handle, mut rx) = running_tcp_input(None).await;
        let msg = "<134>Aug 30 10:00:00 myhost nginx: hello over tcp";
        let mut client = tokio::net::TcpStream::connect(&addr).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut client, format!("{} {msg}", msg.len()).as_bytes())
            .await
            .unwrap();

        let events = recv_events(&mut rx).await;
        assert_eq!(events.len(), 1);
        assert_nginx_line(&events[0]);
        handle.abort();
    }

    /// A message ending in CR, LF-framed as `...\r\r\n`, keeps that byte, as over UDP. Decoder
    /// twin: `with_line_splitting_off_keeps_a_payload_cr_the_framer_already_unwrapped`.
    #[tokio::test]
    async fn tcp_keeps_a_payload_cr_on_an_lf_framed_message_end_to_end() {
        let (addr, handle, mut rx) = running_tcp_input(None).await;
        let mut client = tokio::net::TcpStream::connect(&addr).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(
            &mut client,
            b"<134>Aug 30 10:00:00 myhost nginx: hello over tcp\r\r\n",
        )
        .await
        .unwrap();

        let events = recv_events(&mut rx).await;
        assert_eq!(events.len(), 1);
        assert_eq!(message_str(&events[0]), "hello over tcp\r");
        handle.abort();
    }

    /// An octet-counted MSG-LEN covering its own `\r\n` has it stripped, matching UDP.
    #[tokio::test]
    async fn tcp_strips_a_counted_crlf_terminator_end_to_end() {
        let (addr, handle, mut rx) = running_tcp_input(None).await;
        let msg = "<134>Aug 30 10:00:00 myhost nginx: hello over tcp\r\n";
        let mut client = tokio::net::TcpStream::connect(&addr).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut client, format!("{} {msg}", msg.len()).as_bytes())
            .await
            .unwrap();

        let events = recv_events(&mut rx).await;
        assert_eq!(events.len(), 1);
        assert_nginx_line(&events[0]);
        handle.abort();
    }

    /// An octet-counted MSG containing a newline is one event with the newline intact, not a
    /// first half plus a PRI-less remainder dropped as `bad_line`.
    #[tokio::test]
    async fn tcp_keeps_a_multiline_octet_counted_message_as_exactly_one_event() {
        let (addr, handle, mut rx) = running_tcp_input(None).await;
        let msg = "<134>Aug 30 10:00:00 myhost nginx: line one\nline two\nline three";
        let mut client = tokio::net::TcpStream::connect(&addr).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut client, format!("{} {msg}", msg.len()).as_bytes())
            .await
            .unwrap();

        let events = recv_events(&mut rx).await;
        assert_eq!(events.len(), 1, "a multiline MSG must not be shredded into several events");
        assert_eq!(message_str(&events[0]), "line one\nline two\nline three");

        // A second event would arrive as its own batch.
        assert!(
            tokio::time::timeout(Duration::from_millis(200), rx.recv()).await.is_err(),
            "the multiline message must produce exactly one event"
        );
        handle.abort();
    }

    /// `with_idle_timeout` reaches the driver: under `with_max_connections(1)`, a second client
    /// is served only if the quiet first one is closed. The driver's tests cover the clock.
    #[tokio::test]
    async fn an_idle_tcp_connection_releases_its_permit_after_the_idle_timeout() {
        const LINE: &[u8] = b"<134>Aug 30 10:00:00 myhost nginx: hello over tcp\n";

        let mut input = SyslogInput::tcp("127.0.0.1:0")
            .with_tcp_receive(TcpListenerConfig {
                batch_max_events: 1,
                batch_flush_interval: Duration::ZERO,
                ..TcpListenerConfig::default()
            })
            .with_max_connections(1)
            .with_idle_timeout(Some(Duration::from_millis(50)));
        input.bind().await.expect("binding an ephemeral port should succeed");
        let addr = input.local_addr().expect("bind() leaves a real address behind").to_string();
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let sink = Fanout::new(vec![tx]);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            let _shutdown_tx = shutdown_tx;
            let _ = input.run_until_shutdown(sink, shutdown_rx).await;
        });

        // One frame passes the first-byte deadline, so only the idle clock can close this.
        let mut quiet = tokio::net::TcpStream::connect(&addr).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut quiet, LINE).await.unwrap();
        assert_nginx_line(&recv_events(&mut rx).await[0]);

        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(
            Duration::from_secs(2),
            tokio::io::AsyncReadExt::read(&mut quiet, &mut byte),
        )
        .await
        .expect("a connection quiet past its idle_timeout is closed, not left hanging")
        .expect("reading a closed socket is Ok(0), not an error");
        assert_eq!(read, 0, "the listener hung up on a connection that went quiet");

        let mut client = tokio::net::TcpStream::connect(&addr).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut client, LINE).await.unwrap();
        assert_nginx_line(&recv_events(&mut rx).await[0]);

        drop(quiet);
        handle.abort();
    }

    /// The repo root's `testdata/tls` (`testdata/tls/README.md`), two levels up from
    /// `CARGO_MANIFEST_DIR`.
    fn testdata_tls_dir() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    /// RFC 5425 end to end: a real `TlsConnector` trusting exactly `testdata/tls/ca.pem` hands an
    /// octet-counted frame to a TLS-terminating `syslog_in`.
    #[tokio::test]
    async fn tcp_over_tls_decodes_a_message_end_to_end() {
        let settings = TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: None,
        };
        let (addr, handle, mut rx) = running_tcp_input(Some(&settings)).await;

        let mut roots = rustls::RootCertStore::empty();
        let ca: Vec<rustls_pki_types::CertificateDer<'static>> =
            <rustls_pki_types::CertificateDer as rustls_pki_types::pem::PemObject>::pem_file_iter(
                testdata_tls_dir().join("ca.pem"),
            )
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        roots.add_parsable_certificates(ca);
        let client_config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));

        let tcp = tokio::net::TcpStream::connect(&addr).await.unwrap();
        // `testdata/tls/server.pem` carries a `localhost` SAN.
        let name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
        let mut client = tokio::time::timeout(Duration::from_secs(5), connector.connect(name, tcp))
            .await
            .expect("the TLS handshake should complete within 5s")
            .expect("the TLS handshake should succeed");

        let msg = "<134>Aug 30 10:00:00 myhost nginx: hello over tcp";
        tokio::io::AsyncWriteExt::write_all(&mut client, format!("{} {msg}", msg.len()).as_bytes())
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::flush(&mut client).await.unwrap();

        let events = recv_events(&mut rx).await;
        assert_eq!(events.len(), 1);
        assert_nginx_line(&events[0]);
        handle.abort();
    }

    /// The builder refuses `tls:` on UDP rather than ignoring it, backing graph rule 43.
    #[test]
    fn with_tls_on_a_udp_listener_is_a_clear_error() {
        let settings = TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: None,
        };
        // `SyslogInput` isn't `Debug`, so `expect_err` is out -- match the `Result` by hand.
        let err = match SyslogInput::new("127.0.0.1:0").with_tls(&settings, &testdata_tls_dir()) {
            Ok(_) => panic!("tls on a UDP syslog listener must not be accepted"),
            Err(err) => err,
        };
        assert!(format!("{err:?}").contains("transport: tcp"), "got: {err:?}");
    }

    // ---- recorded interop fixtures (testdata/interop/syslog/) ---------------------------------
    //
    // Real captured traffic from util-linux `logger(1)`, Python's
    // `logging.handlers.SysLogHandler`, and rsyslog, recorded by `script/record-fixtures`
    // (testdata/interop/README.md, docs/plans/recorded-interop-fixtures.md). Asserted on decoded
    // values, not bytes, which a re-record changes (that README's "Consuming these fixtures").

    /// `testdata/interop/syslog/<name>` as a `String`; every fixture here is UTF-8 text.
    fn interop_fixture(name: &str) -> String {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../testdata/interop/syslog")
            .join(name);
        std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("reading interop fixture {}: {e}", path.display()))
    }

    #[test]
    fn interop_fixture_logger_rfc3164_basic_decodes_message_tag_and_severity() {
        let event = only_event(decode(&interop_fixture("logger-rfc3164-basic-000.raw")));
        assert_eq!(
            message_str(&event),
            "hello from logger(1), captured for logit interop fixtures"
        );
        assert_eq!(
            event.attributes.get("syslog.tag").and_then(Value::as_str),
            Some("logit-fixture")
        );
        assert_eq!(event.log.as_ref().unwrap().severity, Some(Severity::Info)); // user.notice = 13 % 8 = 5
    }

    #[test]
    fn interop_fixture_logger_rfc3164_unicode_preserves_multibyte_message_content() {
        // A real sender emitting non-ASCII MSG, not a hand-typed test literal -- see
        // testdata/interop/syslog/README.md's row for this fixture.
        let event = only_event(decode(&interop_fixture("logger-rfc3164-unicode-000.raw")));
        assert_eq!(message_str(&event), "héllo wörld — ünïcödé ✓ (UTF-8 multibyte smoke test)");
    }

    #[test]
    fn interop_fixture_logger_rfc5424_basic_decodes_message_and_structured_data() {
        // Real STRUCTURED-DATA (`[timeQuality tzKnown="1" ...]`, added by util-linux logger), with
        // PROCID/MSGID both nil ("-").
        let event = only_event(decode(&interop_fixture("logger-rfc5424-basic-000.raw")));
        assert_eq!(
            message_str(&event),
            "hello from logger(1) in RFC 5424 mode, captured for logit interop fixtures"
        );
        assert_eq!(
            event.attributes.get("syslog.tag").and_then(Value::as_str),
            Some("logit-fixture")
        );
        assert!(event.attributes.get("syslog.pid").is_none());
        assert!(event.attributes.get("syslog.msgid").is_none());

        let mut time_quality = AttrMap::new();
        time_quality.insert("tzKnown", Value::str("1"));
        time_quality.insert("isSynced", Value::str("1"));
        time_quality.insert("syncAccuracy", Value::str("69277"));
        let mut sd = AttrMap::new();
        sd.insert("timeQuality", Value::Map(Box::new(time_quality)));
        assert_eq!(event.attributes.get("syslog.sd"), Some(&Value::Map(Box::new(sd))));
    }

    #[test]
    fn interop_fixture_python_syslog_handler_plain_message_has_no_trailing_garbage() {
        // Real output of NoNulSysLogHandler (demo/app/pages/syslog_handler.py), which exists
        // because the base handler's trailing NUL breaks the json transform. syslog_in doesn't
        // strip NULs, so the exact match also checks the handler's output stays NUL-free.
        let event = only_event(decode(&interop_fixture("python-syslog-handler-000.raw")));
        assert_eq!(
            message_str(&event),
            "hello from python logging.handlers.SysLogHandler, captured for logit interop fixtures"
        );
    }

    #[test]
    fn interop_fixture_python_syslog_handler_json_body_is_clean_for_the_json_transform() {
        // The shape demo/logit.yaml's app tier logs: a JSON MSG with no trailing NUL.
        let event = only_event(decode(&interop_fixture("python-syslog-handler-001.raw")));
        assert_eq!(
            message_str(&event),
            r#"{"level": "info", "msg": "request handled", "path": "/", "status": 200}"#
        );
        assert!(
            serde_json::from_str::<serde_json::Value>(message_str(&event)).is_ok(),
            "a trailing NUL (or any other trailing byte) would make this fail, the way it used to \
             before NoNulSysLogHandler -- see demo/app/pages/syslog_handler.py's docstring"
        );
    }

    #[test]
    fn interop_fixture_rsyslog_forwarded_message_decodes_tag_and_message() {
        // rsyslog's own omfwd RFC 3164 formatting, from a real second-hop forwarder rather than a
        // direct sender -- see testdata/interop/syslog/README.md's row for this fixture.
        let event = only_event(decode(&interop_fixture("rsyslog-000.raw")));
        assert_eq!(
            event.attributes.get("syslog.tag").and_then(Value::as_str),
            Some("logit-fixture")
        );
        assert_eq!(message_str(&event), "hello from rsyslog, captured for logit interop fixtures");
    }
}
