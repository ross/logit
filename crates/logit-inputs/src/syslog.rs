//! RFC 3164 / RFC 5424 syslog over UDP -- the log-producing input the nginx integration rests on
//! (nginx's `access_log syslog:` writer speaks this).
//!
//! **UDP only.** nginx's `syslog:` writer is UDP-only, so a TCP accept loop would buy this
//! integration nothing; see `docs/known-gaps.md`.
//!
//! **Dialect disambiguation** happens per message, right after `<PRI>`: a leading version digit
//! followed by a space (`1 `) means RFC 5424; anything else is parsed as RFC 3164. This sniff is
//! necessarily a guess -- RFC 5424's VERSION grammar allows any of `1`-`999`, so a tag-less RFC
//! 3164 line whose MSG happens to start with a digit and a space (`4 requests failed`) also
//! matches it. A failed RFC 5424 parse with version `1` (the only version any real sender emits)
//! is treated as genuinely malformed RFC 5424 and rejected as such; a failed parse with any other
//! digit is treated as a false-positive sniff and falls back to reparsing the whole line as RFC
//! 3164 (whose grammar is permissive enough to never itself fail) rather than dropping it, with a
//! throttled `sniff_fallback` diagnostic -- quiet against today's traffic (the fallback only fires
//! on a false positive), but observable the day RFC 5424 defines a version past `1` and a real
//! sender's lines start hitting it.
//!
//! **Timestamp semantics.** Every emitted [`Event`]'s `timestamp` is *receipt* time -- the
//! `received_at` passed into [`SyslogDecoder::decode_into`], captured by the read half at the
//! moment the datagram came off the socket (`docs/adr/decoupled-listener-io.md`), not
//! whenever decode happens to run -- never the sender's own timestamp. RFC 3164's timestamp
//! carries no year and no timezone, so resolving it
//! to an instant means guessing both; doing that only for RFC 5424 (whose timestamp *is*
//! unambiguous) would silently give two senders on one listener different timestamp semantics.
//! The sender's own timestamp is not discarded -- it lands in the `syslog.timestamp` attribute
//! (a [`Value::Timestamp`] for RFC 5424's RFC 3339 form, the raw [`Value::Str`] for RFC 3164's,
//! which can't be resolved without guessing). A nil RFC 5424 TIMESTAMP (`-`) now stamps
//! `syslog.timestamp` as an explicit [`Value::Null`], rather than leaving the attribute simply
//! absent -- `syslog_out` (and any Lua script) can then tell "the sender said no timestamp" apart
//! from "this dialect never carries one at all" (RFC 3164's own timestamp-absent case, which still
//! omits the attribute entirely, since there's no nil marker to distinguish "absent" from "not
//! present in this grammar"). See `docs/known-gaps.md` for the full writeup and the sketch of an
//! opt-in `syslog_timestamp` transform that would make the RFC 3164 guesswork explicit. A
//! well-formed RFC 5424 TIMESTAMP that names an instant outside the representable `i64`-nanosecond
//! range is kept as [`TimestampError::OutOfRange`] -- the event is emitted with `syslog.timestamp`
//! omitted and a throttled diagnostic, not discarded ([`Malformed`](TimestampError::Malformed) is
//! reserved for a TIMESTAMP that doesn't parse at all).
//!
//! **RFC 5424 STRUCTURED-DATA is parsed into `syslog.sd`, not merely balanced-and-skipped.**
//! [`parse_structured_data`] is a real, quote-aware RFC 5424 section 6.3 parser: the nil marker
//! `-` produces no attribute at all; one or more `[SD-ID SP PARAM-NAME="PARAM-VALUE" ...]`
//! SD-ELEMENTs produce `syslog.sd` = a [`Value::Map`] of `"<SD-ID>"` to a nested [`Value::Map`] of
//! `"<PARAM-NAME>"` to [`Value::Str`] (or [`Value::Array`] of [`Value::Str`] for a PARAM-NAME
//! repeated within one element) -- nested rather than flattened, because `SD-NAME` may itself
//! contain `.`, which would make a flattened `syslog.sd.<id>.<param>` key ambiguous to reassemble;
//! see the ADR at `../../../docs/adr/syslog-structured-data-convention.md` for the full rationale.
//! `SD-NAME` (both `SD-ID` and `PARAM-NAME`) is 1..=32 bytes of PRINTUSASCII (`%d33-126`) excluding
//! `=`, SP, `]`, and `"`; an `SD-ID` repeated within one message is a grammar violation (there's no
//! defined merge for two elements sharing an id, so this project rejects rather than silently
//! picking one), while a `PARAM-NAME` repeated *within one element* is legal and becomes the
//! `Value::Array` above, in the order encountered. `PARAM-VALUE` is a quoted UTF-8 string in which
//! exactly three sequences are escapes -- `\"`, `\\`, `\]` -- unescaped on decode; a backslash
//! before any other byte is kept literally, along with that byte, rather than being treated as an
//! unrecognized escape. Any STRUCTURED-DATA grammar violation rejects the whole line (a `bad_line`
//! diagnostic naming what was violated and its byte offset), the same strictness every other
//! malformed RFC 5424 field on this line already gets.
//!
//! **A leading RFC 5424 §6.4 UTF-8 BOM (`EF BB BF`) on MSG is stripped**, not left to leak into
//! `log.message` as U+FEFF -- it's a `MSG-UTF8` signal, not payload. It's stripped only when the
//! whole MSG (BOM included) is valid UTF-8, since the BOM's own bytes are themselves valid UTF-8
//! and there is no `Value::Str` to strip a signal byte from otherwise (see the non-UTF-8 MSG case
//! below). nginx never emits one.
//!
//! **Header fields are parsed off raw bytes and validated individually; only MSG may hold
//! non-UTF-8 bytes.** [`SyslogDecoder::decode_into`] splits the raw datagram into lines on the
//! `\n` byte; no whole-line UTF-8 validation happens anywhere any more. PRI, the RFC 3164
//! timestamp token, HOSTNAME, TAG/APP-NAME, PROCID, MSGID, and STRUCTURED-DATA are all
//! PRINTUSASCII by grammar (a strict subset of UTF-8), and each is validated as such where it's
//! extracted; a violation in an RFC 5424 field rejects the whole line exactly as before (RFC
//! 3164's own header parse stays deliberately permissive and never itself fails, since the
//! dialect-sniff fallback above depends on that -- an RFC 3164 HOSTNAME candidate that somehow
//! isn't valid UTF-8 is simply not stamped as an attribute, rather than failing the line). MSG
//! alone gets UTF-8-validated on its own, independent of every other field: valid UTF-8 decodes to
//! [`Value::Str`] as always; invalid UTF-8 decodes to [`Value::Bytes`] instead of rejecting the
//! line -- a non-UTF-8 payload (arbitrary binary MSG-ANY content RFC 5424 itself explicitly
//! allows) is exactly the case this exists for.
//!
//! **`syslog.pid`** is [`Value::U64`] when PROCID (RFC 5424) or a `tag[pid]` bracket (RFC 3164)
//! parses as one, and [`Value::Str`] of the raw token otherwise -- RFC 5424's PROCID is a
//! free-form PRINTUSASCII string, not necessarily numeric, and this project now keeps it either
//! way rather than dropping a non-numeric one. [`is_tag_shaped`] mirrors this for RFC 3164:
//! bracket content that isn't all-digit still counts as TAG-shaped as long as it's PRINTUSASCII
//! without `]` (so the bracket still unambiguously balances), rather than causing the whole token
//! to be reclassified as "not TAG-shaped" and the `[...]` silently absorbed into the message body.
//!
//! ## The RFC 3164 header
//!
//! nginx's `nohostname` option omits a field RFC 3164 says is mandatory, and the MSG body here is
//! JSON full of `": "` sequences -- so the header can't be parsed by scanning for the first
//! `: ` or assuming HOSTNAME is always present. The rule implemented in [`parse_3164`]:
//!
//! 1. `<PRI>` -- `<`, 1-3 digits, `>`. A missing or non-numeric PRI is a malformed line (skip and
//!    continue, per [`crate::statsd::StatsdDecoder`]'s precedent).
//! 2. The `Mmm dd hh:mm:ss` timestamp (exactly 15 bytes), if present; absent is tolerated.
//! 3. **At most the next two whitespace-delimited tokens** are candidates for HOSTNAME and TAG.
//!    If the *first* candidate is TAG-shaped, there is no hostname. Otherwise, if the *second*
//!    candidate is TAG-shaped, the first is the hostname. Everything after the TAG token (minus
//!    one leading space) is MSG.
//! 4. If neither candidate is TAG-shaped, there is no tag: the whole remainder is MSG, with no
//!    `syslog.tag` attribute. **Bounding the search to two tokens is what makes this safe** -- an
//!    unbounded "find the first `: `" scan would find one *inside* a JSON body on a tag-less
//!    message and silently truncate the log line.
//! 5. `tag[pid]:` splits into `syslog.tag` + `syslog.pid` (`Value::U64` when the bracket content
//!    parses as one, `Value::Str` otherwise -- see above).
//!
//! [`is_tag_shaped`] is deliberately stricter than "ends in `:` or `]:`" read literally: it also
//! requires the token's body to look like a process name (letters, digits, `_`, `-`, `.`, `/`,
//! optionally followed by a bracketed PID). Without that restriction, a tag-less message whose
//! first JSON key happens to have a space after its colon (`{"status": 200, ...}`) would see its
//! very first whitespace-delimited token (`{"status":`) misclassified as TAG-shaped, since it does
//! technically end in `:` -- silently eating part of the body as a fake tag. Restricting the
//! character class rules that out: `{"status"` contains `{`/`"`, which no real tag ever does.

use crate::udp::{UdpListener, UdpListenerConfig};
use crate::Input;
use bytes::Bytes;
use logit_core::time::{parse_rfc3339_to_nanos, TimestampError};
use logit_core::{
    AttrMap, BodyFormat, Diagnostics, Event, LogRecord, Resource, Scope, Severity, Telemetry, Value,
};
use logit_pipeline::Fanout;
use logit_proto::{CodecError, Decoder};
use std::sync::Arc;
use tokio::sync::watch;

/// Thin wrapper over [`UdpListener<SyslogDecoder>`] -- the read/decode split and datagram-\>batch
/// assembly all live there (`docs/adr/decoupled-listener-io.md`); this type is just the
/// decoder choice plus the public constructor/builder surface `logit-cli::pipeline` and this
/// module's own tests already depend on.
pub struct SyslogInput {
    inner: UdpListener<SyslogDecoder>,
}

impl SyslogInput {
    pub fn new(bind: impl Into<String>) -> Self {
        Self {
            inner: UdpListener::new(
                bind,
                SyslogDecoder::new(Arc::new(Resource::default())),
                UdpListenerConfig::default(),
            ),
        }
    }

    /// Attaches a component id to this listener's diagnostics -- and to the [`SyslogDecoder`] it
    /// wraps, so both report under the same id. Both halves matter: `UdpListener`'s own
    /// `diag` is what a whole-datagram decode failure reports through
    /// (`decode_loop`'s `bad_datagram`); the decoder's own `diag` field is what a malformed
    /// *line* inside an otherwise-valid datagram reports through (`bad_line`) -- two distinct
    /// `Diagnostics` values that must both carry the same id and telemetry handle, or one class
    /// of decode failure silently reports under no component id and with telemetry disabled.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.inner =
            self.inner.with_diagnostics(diag.clone()).map_decoder(|d| d.with_diagnostics(diag));
        self
    }

    /// Attaches a telemetry handle -- component-specific detail beyond the runtime's uniform
    /// layer-2 metrics (`docs/design/internal-telemetry.md`'s "layer 3"): how many datagrams and
    /// bytes actually arrived on the wire, mirroring `StatsdInput`'s own worked example.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.inner = self.inner.with_telemetry(telemetry);
        self
    }

    /// Overrides the receive-queue/batching/shutdown-grace knobs a `receive:` config block sets
    /// (`docs/adr/decoupled-listener-io.md`). Defaults to [`UdpListenerConfig::default`] --
    /// today's behaviour -- when never called.
    pub fn with_receive(mut self, config: UdpListenerConfig) -> Self {
        self.inner = self.inner.with_config(config);
        self
    }

    /// Passthrough to the wrapped [`UdpListener::local_addr`] -- lets a caller (`crates/
    /// logit-cli/tests/syslog_round_trip.rs`) learn the real ephemeral port after `bind()`,
    /// mirroring `otlp_round_trip.rs`'s own `Input::bind`-then-`local_addr` readiness pattern,
    /// with no bind-drop race.
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        self.inner.local_addr()
    }
}

#[async_trait::async_trait]
impl Input for SyslogInput {
    async fn bind(&mut self) -> anyhow::Result<()> {
        self.inner.bind().await
    }

    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        self.inner.run(sink).await
    }

    async fn run_until_shutdown(
        &mut self,
        sink: Fanout,
        shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        self.inner.run_until_shutdown(sink, shutdown).await
    }
}

/// Decodes raw syslog datagram bytes into an [`EventBatch`]. Split out from [`SyslogInput`] so
/// the parsing logic is directly unit-testable without a socket.
pub struct SyslogDecoder {
    resource: Arc<Resource>,
    diag: Diagnostics,
}

impl SyslogDecoder {
    pub fn new(resource: Arc<Resource>) -> Self {
        Self { resource, diag: Diagnostics::default() }
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    /// Test-only: confirms `SyslogInput::with_diagnostics` actually reached this decoder's own
    /// `diag`, not just `UdpListener`'s.
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
        // Per line, not per datagram -- exactly `StatsdDecoder::decode_into`'s precedent. nginx's
        // `escape=json` guarantees no raw newline inside an access-log body, so this split is
        // safe for the target workload. Splitting happens on the raw bytes; there is no
        // whole-line UTF-8 validation here any more -- `parse_line` validates each header field
        // individually and only MSG is allowed to carry non-UTF-8 bytes (see the module doc).
        let mut start = 0usize;
        while start <= bytes.len() {
            let nl = bytes[start..].iter().position(|&b| b == b'\n');
            let end = start + nl.unwrap_or(bytes.len() - start);
            let mut line = bytes.slice(start..end);
            if line.ends_with(b"\r") {
                line = line.slice(..line.len() - 1);
            }
            // Only a truly empty record (a bare newline used as a separator) is skipped here --
            // *not* whitespace-only content, which is real MSG data, not framing.
            if !line.is_empty() {
                match parse_line(&line, received_at, &mut self.diag) {
                    Ok(event) => out.push(event),
                    Err(err) => {
                        self.diag.warn_throttled("bad_line", err);
                    }
                }
            }
            match nl {
                Some(i) => start += i + 1,
                None => break,
            }
        }
        // syslog datagrams carry no OTLP instrumentation-scope concept -- `None`, always.
        Ok((self.resource.clone(), None))
    }
}

/// Reconstructs a `Bytes` sharing `line`'s underlying allocation for `sub`, a byte slice derived
/// from `line` through ordinary slicing (never copied or reconstructed) -- so `sub`'s pointer
/// always lands inside `line`'s allocation, and the offset computed here is always non-negative
/// and in-bounds. See `docs/design/data-model.md`'s "`bytes::Bytes` everywhere strings and blobs
/// appear" -- this is what keeps every extracted field a zero-copy slice of the original
/// datagram. Not used for anything derived by unescaping (RFC 5424 STRUCTURED-DATA's PARAM-VALUE)
/// -- that content isn't a subslice of anything and is wrapped directly via `Bytes::from` instead.
fn slice_of(line: &Bytes, sub: &[u8]) -> Bytes {
    let line_start = line.as_ptr() as usize;
    let sub_start = sub.as_ptr() as usize;
    let start = sub_start - line_start;
    line.slice(start..start + sub.len())
}

/// Splits `s` at the first ASCII space, returning `(token, rest)` with the space itself consumed.
/// `rest` is an empty slice positioned at the end of `s` when there is no more space in `s` (the
/// whole of `s` becomes the token) -- deliberately `&s[s.len()..]` rather than the literal `b""`,
/// so `rest` is always a genuine subslice of `s` with a pointer inside `s`'s allocation, which
/// [`slice_of`]'s precondition depends on.
fn split_first_token(s: &[u8]) -> (&[u8], &[u8]) {
    match s.iter().position(|&b| b == b' ') {
        Some(i) => (&s[..i], &s[i + 1..]),
        None => (s, &s[s.len()..]),
    }
}

/// Maps a syslog PRI's severity nibble (0-7, i.e. `pri % 8`) onto [`Severity`].
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

/// `true` when every byte of `b` is PRINTUSASCII -- the character class RFC 5424 uses for
/// HOSTNAME, APP-NAME, PROCID, MSGID, and (further restricted below) `SD-NAME`.
fn is_printusascii(b: &[u8]) -> bool {
    b.iter().all(|&c| is_printusascii_byte(c))
}

/// Parses one non-empty line, already isolated as a `Bytes` slice of the original datagram by
/// [`SyslogDecoder::decode_into`] -- not yet validated as UTF-8 anywhere; that validation now
/// happens field-by-field below (PRINTUSASCII for every header field, UTF-8-or-`Bytes` for MSG
/// alone). `diag` is threaded down to [`parse_5424`], which uses it to report a
/// well-formed-but-unrepresentable TIMESTAMP without failing the whole line over it.
fn parse_line(line: &Bytes, recv_ts: i64, diag: &mut Diagnostics) -> Result<Event, CodecError> {
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
    // RFC 3164 and RFC 5424 both define PRI as facility*8+severity in 0..=191, encoded with no
    // leading zero except the literal value `0`. `<013>` and `<192>..<999>` are therefore
    // malformed, not merely unusual: accepting them would attach an impossible facility/severity
    // (e.g. facility 124 for `<999>`) to the event.
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

    // Disambiguate: a leading version digit followed by a space means RFC 5424; anything else is
    // RFC 3164.
    let is_5424_after = match (after_pri.first(), after_pri.get(1)) {
        (Some(&c0), Some(&b' ')) if c0.is_ascii_digit() => Some((c0 as char, &after_pri[2..])),
        _ => None,
    };

    match is_5424_after {
        Some((version, after_version)) => {
            match parse_5424(line, after_version, facility, severity_num, severity, recv_ts, diag) {
                Ok(event) => Ok(event),
                // The sniff above only checks "digit, then space" -- RFC 5424's VERSION is
                // `NONZERO-DIGIT 0*2DIGIT`, so a tag-less RFC 3164 line whose MSG happens to start
                // with a digit and a space (`4 requests failed`) also matches it. `1` is the only
                // version any real sender emits, so a failure with that exact version is treated
                // as a genuine, malformed RFC 5424 line -- the same skip-and-continue a bad
                // TIMESTAMP or PRI gets, per the previous review round's fix. Any *other* digit
                // failing is far more likely a false-positive sniff than a real, currently
                // undefined version, so it falls back to reparsing the whole `after_pri` as RFC
                // 3164 (whose grammar is permissive enough to never itself fail) instead of
                // dropping the line outright.
                Err(err) if version == '1' => Err(err),
                Err(err) => {
                    // Every other skip/recover path in this decoder (a bad PRI, a bad line, a bad
                    // TIMESTAMP, an out-of-range one) reports through `diag`; this one shouldn't
                    // be the exception. Quiet today -- the fallback only fires on a false-positive
                    // sniff against current traffic -- but if RFC 5424 ever defines a version past
                    // `1`, a real sender's lines would otherwise be silently reparsed as RFC 3164
                    // with nothing anywhere saying so.
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

/// Checks the `Mmm dd hh:mm:ss` shape at the start of `s` (exactly 15 bytes: 3-letter month, ' ',
/// a space- or zero-padded day, ' ', `hh:mm:ss`). Returns `(timestamp, rest)` with exactly one
/// following space consumed from `rest` when present; `None` when absent -- tolerated, per
/// nginx's occasional omission of fields RFC 3164 calls mandatory.
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

/// A token qualifies as a syslog TAG if it ends in `:` (which includes `name[pid]:`, since that
/// ends in `]:`... followed by `:`) *and* everything before that trailing colon looks like a
/// process name -- see the module doc comment for why this is stricter than "ends in `:`" read
/// literally.
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
        // A numeric PID that fits `u64` (what `syslog.pid` stores it as when it parses) is the
        // common case. RFC 5424's own PROCID grammar allows any PRINTUSASCII string though, and
        // this project keeps a non-numeric PROCID as `Value::Str` rather than dropping it -- so a
        // 3164 sender's non-numeric bracket content is accepted here too (as long as it's
        // PRINTUSASCII without `]`, so the bracket still unambiguously balances), rather than
        // causing the whole token to be reclassified as "not TAG-shaped" and the `[...]` silently
        // absorbed into the message body. See the module doc's `syslog.pid` section.
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
            // Neither candidate is TAG-shaped: no tag, no hostname -- the whole remainder
            // (starting from `after_ts`, not `after1`/`after2`) is MSG.
            (None, None, after_ts)
        }
    };

    let mut attrs = AttrMap::new();
    attrs.insert("syslog.facility", Value::U64(facility as u64));
    attrs.insert("syslog.severity", Value::U64(severity_num as u64));
    if let Some(ts) = ts_token {
        // ASCII by construction -- `parse_3164_timestamp` only accepts alphabetic/digit/space/
        // colon bytes.
        attrs.insert("syslog.timestamp", Value::Str(slice_of(line, ts)));
    }
    if let Some(host) = hostname {
        if !host.is_empty() {
            // RFC 3164 parsing never fails outright (the version-sniff fallback in `parse_line`
            // depends on that): a HOSTNAME candidate that somehow isn't valid UTF-8 is simply not
            // stamped as an attribute, rather than rejecting the whole line or violating
            // `Value::Str`'s "always valid UTF-8" invariant -- reported through a throttled
            // `hostname_not_utf8` diagnostic instead, so the skip stays observable (before
            // `syslog.sd` parsing existed, a non-UTF-8 line failed whole-line UTF-8 validation
            // and was rejected with its own diagnostic; this keeps that observability for what is
            // now a partial, per-field loss instead of a whole-line rejection).
            if std::str::from_utf8(host).is_ok() {
                attrs.insert("syslog.hostname", Value::Str(slice_of(line, host)));
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
            attrs.insert("syslog.tag", Value::Str(slice_of(line, name)));
            match std::str::from_utf8(pid_bytes).ok().and_then(|s| s.parse::<u64>().ok()) {
                Some(n) => attrs.insert("syslog.pid", Value::U64(n)),
                // `is_tag_shaped` guarantees `pid_bytes` is PRINTUSASCII (hence valid UTF-8) when
                // it isn't a `u64`, so this `Value::Str` construction can't violate its invariant.
                None => attrs.insert("syslog.pid", Value::Str(slice_of(line, pid_bytes))),
            }
        } else {
            attrs.insert("syslog.tag", Value::Str(slice_of(line, tag_body)));
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

/// `-` (the RFC 5424 nil value) or an empty field both mean "absent" -- every nillable field
/// (HOSTNAME, APP-NAME, PROCID, MSGID, TIMESTAMP) is treated identically.
fn nil_or(field: &[u8]) -> Option<&[u8]> {
    if field.is_empty() || field == b"-" {
        None
    } else {
        Some(field)
    }
}

/// Validates and slices one non-nil RFC 5424 header field (HOSTNAME, APP-NAME, or MSGID -- PROCID
/// is handled separately in [`parse_5424`] since it has a numeric/string split `syslog.pid`
/// cares about). `label` names the field in the error message. A field that isn't PRINTUSASCII is
/// a grammar violation, consistent with this dialect's strictness elsewhere.
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

/// Builds the MSG [`Value`]: valid UTF-8 becomes [`Value::Str`] (stripping a leading BOM when
/// `strip_bom` is set and the BOM is followed by more valid UTF-8 -- see the module doc); invalid
/// UTF-8 becomes [`Value::Bytes`], raw, with no BOM handling (there is no `MSG-UTF8` signal to
/// strip from bytes that were never `MSG-UTF8` to begin with).
fn message_value(line: &Bytes, msg: &[u8], strip_bom: bool) -> Value {
    match std::str::from_utf8(msg) {
        Ok(s) => {
            let s = if strip_bom { s.strip_prefix('\u{FEFF}').unwrap_or(s) } else { s };
            Value::Str(slice_of(line, s.as_bytes()))
        }
        Err(_) => Value::Bytes(slice_of(line, msg)),
    }
}

/// One RFC 5424 STRUCTURED-DATA grammar violation, with a byte offset relative to the start of
/// the slice [`parse_structured_data`] was called with -- the caller adds its own base offset
/// within the line to produce an absolute position for its `bad_line` diagnostic.
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

/// `true` for every byte RFC 5424's `SD-NAME` grammar allows: PRINTUSASCII excluding `=`, `]`,
/// and `"` (space is already excluded by the PRINTUSASCII range itself).
fn is_sd_name_byte(b: u8) -> bool {
    is_printusascii_byte(b) && !matches!(b, b'=' | b']' | b'"')
}

/// Parses one `SD-NAME` (an `SD-ID` or `PARAM-NAME`): 1..=32 bytes of [`is_sd_name_byte`].
/// Advances `*pos` past the name and returns its byte slice.
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

/// Parses one `PARAM-VALUE`'s content after the opening `"`, up to and consuming the closing `"`.
/// Unescapes RFC 5424 section 6.3.3's three escapes (`\"`, `\\`, `\]`); a backslash before any
/// other byte is kept literally, along with that byte, rather than treated as an error or a
/// no-op. Returns the unescaped bytes; the caller validates them as UTF-8 (`PARAM-VALUE` is
/// defined as `UTF-8-STRING`).
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

/// Inserts one `PARAM-NAME`/value pair into `inner`. A repeated `PARAM-NAME` within the same
/// SD-ELEMENT becomes a `Value::Array` of `Value::Str`, in the order encountered -- RFC 5424
/// doesn't forbid repetition, and this project's `syslog.sd` convention keeps every occurrence
/// rather than the last-write-wins an ordinary `AttrMap::insert` would give.
fn insert_param(inner: &mut AttrMap, name: &str, value: Bytes) {
    let value = Value::Str(value);
    let merged = match inner.remove(name) {
        None => value,
        Some(Value::Array(mut arr)) => {
            arr.push(value);
            Value::Array(arr)
        }
        Some(existing) => Value::Array(vec![existing, value]),
    };
    inner.insert(name, merged);
}

/// Parses RFC 5424 STRUCTURED-DATA (section 6.3): the nil marker `-`, or one or more concatenated
/// `[SD-ID SP PARAM-NAME="PARAM-VALUE" ...]` SD-ELEMENTs. Returns the parsed `syslog.sd` value
/// (`None` for nil) and the byte offset into `s` where MSG begins, having consumed exactly one
/// following space when present -- the same "consumed one following space" contract
/// [`skip_structured_data`](self) (this function's predecessor) used. An `Err` names what grammar
/// rule was violated and where, relative to the start of `s`.
fn parse_structured_data(s: &[u8]) -> Result<(Option<Value>, usize), SdError> {
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
                    insert_param(&mut inner, name, Bytes::from(value));
                }
                Some(_) => {
                    return Err(SdError::new(pos, "expected SP or ']' inside SD-ELEMENT"));
                }
                None => {
                    return Err(SdError::new(pos, "unterminated SD-ELEMENT (missing ']')"));
                }
            }
        }
        sd.insert(id, Value::Map(Box::new(inner)));
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
) -> Result<Event, CodecError> {
    let malformed =
        |detail: String| CodecError::Malformed(format!("malformed RFC 5424 syslog line: {detail}"));

    let (ts_field, rest) = split_first_token(after_version);
    let (host_field, rest) = split_first_token(rest);
    let (app_field, rest) = split_first_token(rest);
    let (procid_field, rest) = split_first_token(rest);
    let (msgid_field, rest) = split_first_token(rest);
    // Base offset of the STRUCTURED-DATA field within `line`, used only to translate an `SdError`
    // (relative to `rest`) into an absolute byte offset for the `bad_line` diagnostic. `rest` is
    // always a genuine subslice of `line` (built entirely through `split_first_token`), so this
    // pointer subtraction is sound the same way `slice_of`'s is.
    let sd_base = rest.as_ptr() as usize - line.as_ptr() as usize;
    let (sd_value, sd_offset) = parse_structured_data(rest).map_err(|e| {
        malformed(format!("STRUCTURED-DATA at byte {}: {}", sd_base + e.offset, e.message))
    })?;
    let msg = &rest[sd_offset..];

    let mut attrs = AttrMap::new();
    attrs.insert("syslog.facility", Value::U64(facility as u64));
    attrs.insert("syslog.severity", Value::U64(severity_num as u64));
    // A nil TIMESTAMP (`-`) now stamps an explicit `Value::Null` -- distinct from "this decoder
    // never looked" -- but a non-nil TIMESTAMP that fails to parse is not "absent", it's
    // malformed input, and must take the same skip-and-continue path a bad PRI does rather than
    // silently landing on the floor with no `syslog.timestamp` attribute and no diagnostic. A
    // TIMESTAMP that *does* parse but names an instant outside the `i64` nanosecond range
    // `Value::Timestamp` uses is a different condition from malformed, though: the line and every
    // other field on it are still good, so it's kept, with `syslog.timestamp` omitted and a
    // throttled diagnostic instead of the whole record being discarded over one unrepresentable
    // field.
    match nil_or(ts_field) {
        None => {
            attrs.insert("syslog.timestamp", Value::Null);
        }
        Some(ts) => {
            let ts_str = std::str::from_utf8(ts)
                .map_err(|_| malformed("TIMESTAMP is not valid UTF-8".to_string()))?;
            match parse_rfc3339_to_nanos(ts_str) {
                Ok(nanos) => {
                    attrs.insert("syslog.timestamp", Value::Timestamp(nanos));
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
        attrs.insert("syslog.hostname", v);
    }
    if let Some(v) = field_value(line, app_field, "APP-NAME")? {
        attrs.insert("syslog.tag", v);
    }
    if let Some(pid) = nil_or(procid_field) {
        // PROCID is a free-form PRINTUSASCII string per RFC 5424 (it need not be numeric).
        // `syslog.pid` keeps it as `Value::U64` when it parses as one, `Value::Str` otherwise --
        // see the module doc's `syslog.pid` section.
        if !is_printusascii(pid) {
            return Err(malformed(format!(
                "PROCID {:?} is not PRINTUSASCII",
                String::from_utf8_lossy(pid)
            )));
        }
        match std::str::from_utf8(pid).expect("validated PRINTUSASCII above").parse::<u64>() {
            Ok(n) => {
                attrs.insert("syslog.pid", Value::U64(n));
            }
            Err(_) => {
                attrs.insert("syslog.pid", Value::Str(slice_of(line, pid)));
            }
        }
    }
    if let Some(v) = field_value(line, msgid_field, "MSGID")? {
        attrs.insert("syslog.msgid", v);
    }
    if let Some(sd) = sd_value {
        attrs.insert("syslog.sd", sd);
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

    fn decode(datagram: &str) -> Vec<Event> {
        let mut decoder = SyslogDecoder::new(Arc::new(Resource::default()));
        decoder.decode(Bytes::from(datagram.to_string())).expect("decode should succeed").events
    }

    fn decode_bytes(datagram: Vec<u8>) -> Vec<Event> {
        let mut decoder = SyslogDecoder::new(Arc::new(Resource::default()));
        decoder.decode(Bytes::from(datagram)).expect("decode should succeed").events
    }

    /// Regression: `SyslogInput::with_diagnostics` used to only set `UdpListener`'s own `diag`,
    /// never reaching the wrapped `SyslogDecoder`'s -- so a malformed *line* (as opposed to a
    /// whole malformed datagram) reported through a permanently unnamed, telemetry-disabled
    /// `Diagnostics::default()`, regardless of what the component was actually configured with.
    #[test]
    fn with_diagnostics_reaches_the_wrapped_decoder_too() {
        let input = SyslogInput::new("127.0.0.1:0").with_diagnostics(Diagnostics::new("my-id"));
        assert_eq!(input.inner.decoder().diag().component_id(), "my-id");
    }

    /// `decode_into` must stamp every event with the caller's `received_at`, not a fresh
    /// call-time clock read -- the property `docs/adr/decoupled-listener-io.md` exists for:
    /// once decode runs on its own loop, "now" at decode time can be arbitrarily later than
    /// arrival under backlog, and this module's own doc comment promises `timestamp` is receipt
    /// time, not decode time.
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

    /// `decode_into` appends to `out` rather than replacing it -- the property that lets a caller
    /// accumulate several datagrams' events into one reused buffer
    /// (`logit_pipeline::BatchAccumulator`) instead of allocating fresh per datagram.
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
        parse_line(&bytes, 0, &mut diag).expect_err("expected this line to be rejected")
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

    /// Regression test for a review finding: before `syslog.sd` parsing existed, a whole
    /// non-UTF-8 line failed whole-line UTF-8 validation and was rejected with its own
    /// diagnostic; now that only MSG is allowed to carry non-UTF-8 bytes, a non-UTF-8 HOSTNAME
    /// token is simply skipped -- this pins that the skip is still observable, through a
    /// throttled `hostname_not_utf8` diagnostic mirrored into
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
        // Regression guard for the two-token bound: a tag-less message whose JSON body has a
        // space after a colon must not have its leading `{"key":` token mistaken for a tag.
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
        // Regression test for the original blocker: a PID this long used to reach a bare
        // `.parse().expect(...)` and panic the listener task on one crafted UDP packet. Now
        // `syslog.pid` becomes `Value::Str` for PROCID/bracketed-PID content that doesn't fit
        // `u64` -- see the module doc's `syslog.pid` section -- rather than being dropped and
        // the whole `tag[pid]:` token absorbed into the message.
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
        // Regression test for the second blocker: a TAG-shaped token followed by nothing (no
        // trailing space, so an empty MSG) used to hand `slice_of` the `&'static str` literal
        // `split_first_token` returned for "no more space in s", rather than a real slice of the
        // line -- pointer-arithmetic underflow, panicking the listener task on one UDP packet.
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
            // 10 fractional digits -- past what an i64 of nanoseconds can hold. RFC 5424 itself
            // allows at most 6, but the parser is `logit_core::time`'s shared RFC 3339 one now
            // (it also serves `trace_context`'s `span.*_rfc3339`), which accepts up to 9; a
            // 7-9 digit fraction from a syslog sender is harmless leniency, not a rejection.
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
        // A well-formed RFC 3339 timestamp naming an instant outside the `i64` nanosecond range
        // (roughly 1677-09-21 to 2262-04-11) used to be indistinguishable from a malformed one,
        // discarding the whole log record rather than just the unrepresentable attribute.
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
        // "4 requests failed" sniffs as a plausible RFC 5424 VERSION ("4", a digit, then a
        // space), but has none of RFC 5424's mandatory fields after it, so `parse_5424` fails.
        // That failure must fall back to RFC 3164 (whose grammar tolerates all of this as an
        // untagged, hostname-less message) rather than discarding the line.
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
        // Regression test: a whole-datagram `str::from_utf8` used to reject every line in the
        // packet as soon as any single byte anywhere was invalid UTF-8. There is no whole-line
        // UTF-8 gate any more, but this line still fails to parse (it has no `<PRI>` at all), so
        // the assertion -- the good sibling lines still decode -- still exercises the property.
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
        // Regression test: `.trim()` on the whole line used to eat trailing MSG spaces and
        // collapse an all-whitespace MSG into an (incorrectly) skipped "blank line".
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
    // Transcribed from the author's own knowledge of the RFC 5424 text, not copy-pasted from a
    // fetched copy -- flagged here explicitly so the docs/test worker verifies these word-for-word
    // against the actual RFC before relying on them as a fidelity gate.

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

    // ---- recorded interop fixtures (testdata/interop/syslog/) ---------------------------------
    //
    // Real captured wire traffic from real senders (util-linux `logger(1)`, Python's
    // `logging.handlers.SysLogHandler`, and rsyslog itself), recorded by `script/record-fixtures`
    // -- see testdata/interop/README.md and docs/plans/recorded-interop-fixtures.md for how and
    // why. These assert on *decoded, identifiable values* (message content, tag, severity), not on
    // the fixture bytes staying byte-for-byte stable across a re-record -- see
    // testdata/interop/README.md's "Consuming these fixtures" section for why.

    /// `testdata/interop/syslog/<name>` as a `String` -- every fixture here is UTF-8 text, so this
    /// reuses `decode`'s existing `&str` signature rather than adding a byte-oriented variant.
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
        // This capture's STRUCTURED-DATA (`[timeQuality tzKnown="1" ...]`, util-linux logger's own
        // addition) is real, not hand-typed -- exercising the real `parse_structured_data` path
        // the module doc describes, with PROCID/MSGID both nil ("-").
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
        // NoNulSysLogHandler (demo/app/pages/syslog_handler.py) exists because the base
        // SysLogHandler's trailing NUL byte breaks the json transform -- this fixture is real
        // captured output from that handler; a trailing byte here would make `decode` see it as
        // part of the message (syslog_in has no NUL-stripping of its own), so an exact match
        // doubles as an empirical check that the real handler output stays NUL-free.
        let event = only_event(decode(&interop_fixture("python-syslog-handler-000.raw")));
        assert_eq!(
            message_str(&event),
            "hello from python logging.handlers.SysLogHandler, captured for logit interop fixtures"
        );
    }

    #[test]
    fn interop_fixture_python_syslog_handler_json_body_is_clean_for_the_json_transform() {
        // The exact shape demo/logit.yaml's app tier logs in production: a JSON MSG body with no
        // trailing NUL to trip up the downstream `json` transform.
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
