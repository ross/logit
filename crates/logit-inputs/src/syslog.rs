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
//! moment the datagram came off the socket (`docs/adr/0026-decoupled-listener-io.md`), not
//! whenever decode happens to run -- never the sender's own timestamp. RFC 3164's timestamp
//! carries no year and no timezone, so resolving it
//! to an instant means guessing both; doing that only for RFC 5424 (whose timestamp *is*
//! unambiguous) would silently give two senders on one listener different timestamp semantics.
//! The sender's own timestamp is not discarded -- it lands in the `syslog.timestamp` attribute
//! (a [`Value::Timestamp`] for RFC 5424's RFC 3339 form, the raw [`Value::Str`] for RFC 3164's,
//! which can't be resolved without guessing). See `docs/known-gaps.md` for the full writeup and
//! the sketch of an opt-in `syslog_timestamp` transform that would make the guesswork explicit. A
//! well-formed RFC 5424 TIMESTAMP that names an instant outside the representable `i64`-nanosecond
//! range is kept as [`TimestampError::OutOfRange`] -- the event is emitted with `syslog.timestamp`
//! omitted and a throttled diagnostic, not discarded ([`Malformed`](TimestampError::Malformed) is
//! reserved for a TIMESTAMP that doesn't parse at all).
//!
//! **RFC 5424 STRUCTURED-DATA is parsed only enough to be skipped correctly** (balanced `[...]`
//! honoring a backslash-escaped `]`) -- its contents are not merged into attributes. nginx emits
//! none, and inventing a naming scheme for `[id@32473 k="v"]` without a consumer would be
//! guesswork. Deliberate, marked gap; see `docs/known-gaps.md`.
//!
//! **A leading RFC 5424 §6.4 UTF-8 BOM (`EF BB BF`) on MSG is stripped**, not left to leak into
//! `log.message` as U+FEFF -- it's a `MSG-UTF8` signal, not payload. nginx never emits one.
//!
//! **Byte validity is checked per line, not per datagram.** [`SyslogDecoder::decode`] splits the
//! raw datagram into lines on the `\n` byte *before* any UTF-8 validation, so one line containing
//! an invalid UTF-8 byte -- most plausibly inside MSG-ANY, which RFC 5424 allows to hold arbitrary
//! octets -- is skipped and diagnosed without dropping its sibling lines. A non-UTF-8 MSG on an
//! otherwise well-formed line is still a malformed-line rejection rather than a `Value::Bytes`
//! emission; see `docs/known-gaps.md`.
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
//! 5. `tag[pid]:` splits into `syslog.tag` + `syslog.pid`.
//!
//! [`is_tag_shaped`] is deliberately stricter than "ends in `:` or `]:`" read literally: it also
//! requires the token's body to look like a process name (letters, digits, `_`, `-`, `.`, `/`,
//! optionally followed by `[<digits>]`). Without that restriction, a tag-less message whose first
//! JSON key happens to have a space after its colon (`{"status": 200, ...}`) would see its very
//! first whitespace-delimited token (`{"status":`) misclassified as TAG-shaped, since it does
//! technically end in `:` -- silently eating part of the body as a fake tag. Restricting the
//! character class rules that out: `{"status"` contains `{`/`"`, which no real tag ever does.

use crate::udp::{UdpListener, UdpListenerConfig};
use crate::Input;
use bytes::Bytes;
use logit_core::{
    AttrMap, BodyFormat, Diagnostics, Event, LogRecord, Resource, Severity, Telemetry, Value,
};
use logit_pipeline::Fanout;
use logit_proto::{CodecError, Decoder};
use std::sync::Arc;
use tokio::sync::watch;

/// Thin wrapper over [`UdpListener<SyslogDecoder>`] -- the read/decode split and datagram-\>batch
/// assembly all live there (`docs/adr/0026-decoupled-listener-io.md`); this type is just the
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
    /// (`docs/adr/0026-decoupled-listener-io.md`). Defaults to [`UdpListenerConfig::default`] --
    /// today's behaviour -- when never called.
    pub fn with_receive(mut self, config: UdpListenerConfig) -> Self {
        self.inner = self.inner.with_config(config);
        self
    }
}

#[async_trait::async_trait]
impl Input for SyslogInput {
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
    ) -> Result<Arc<Resource>, CodecError> {
        // Per line, not per datagram -- exactly `StatsdDecoder::decode_into`'s precedent. nginx's
        // `escape=json` guarantees no raw newline inside an access-log body, so this split is
        // safe for the target workload. Splitting happens on the raw bytes, before any UTF-8
        // validation, so one line with an invalid UTF-8 byte can't reject its siblings -- see the
        // module doc's "Byte validity" note.
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
                match std::str::from_utf8(&line) {
                    Ok(text) => match parse_line(&line, text, received_at, &mut self.diag) {
                        Ok(event) => out.push(event),
                        Err(err) => {
                            self.diag.warn_throttled("bad_line", err);
                        }
                    },
                    Err(e) => {
                        self.diag.warn_throttled(
                            "bad_line",
                            CodecError::Malformed(format!("invalid utf-8: {e}")),
                        );
                    }
                }
            }
            match nl {
                Some(i) => start += i + 1,
                None => break,
            }
        }
        Ok(self.resource.clone())
    }
}

/// Reconstructs a `Bytes` sharing the line's underlying allocation for `sub`, a substring derived
/// (through ordinary `&str` slicing -- `split`, indexing) from `text`, which in turn was parsed
/// directly out of `bytes` via `str::from_utf8`. Because `sub` is always obtained by slicing
/// `text` rather than by copying or reconstructing it, this pointer-arithmetic round-trip always
/// lands inside `bytes`'s allocation -- unlike `logit-transforms::json::borrowed_str_bytes`, which
/// guards against a non-subset because a serde_json-unescaped string can legitimately live outside
/// the input buffer, there is no such case here, so no fallback copy is needed. See
/// `docs/design/data-model.md`'s "`bytes::Bytes` everywhere strings and blobs appear" -- this is
/// what keeps `message` (and every other extracted field) a zero-copy slice of the original
/// datagram, since `bytes` (the line) is itself a zero-copy `Bytes::slice` of the datagram passed
/// into [`SyslogDecoder::decode`].
fn slice_of(bytes: &Bytes, text: &str, sub: &str) -> Bytes {
    let text_start = text.as_ptr() as usize;
    let sub_start = sub.as_ptr() as usize;
    let start = sub_start - text_start;
    bytes.slice(start..start + sub.len())
}

/// Splits `s` at the first ASCII space, returning `(token, rest)` with the space itself consumed.
/// `rest` is an empty slice positioned at the end of `s` when there is no more space in `s` (the
/// whole of `s` becomes the token) -- deliberately `&s[s.len()..]` rather than the literal `""`,
/// so `rest` is always a genuine substring of `s` with a pointer inside `s`'s allocation. Callers
/// (`parse_3164`, `parse_5424`) feed tokens straight into [`slice_of`], whose pointer-arithmetic
/// round-trip is only sound when its `sub` argument really is a slice of the buffer it came from;
/// the static `""` literal used to violate that silently and take the listener down (see the
/// regression tests below).
fn split_first_token(s: &str) -> (&str, &str) {
    match s.find(' ') {
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

/// Parses one non-empty line, already isolated as a valid-UTF-8 `Bytes` slice of the original
/// datagram by [`SyslogDecoder::decode`]. `bytes`/`text` are that one line -- the same slice, as
/// `Bytes` and as the `&str` view `str::from_utf8` produced from it -- passed through so every
/// extracted field (`message`, `syslog.tag`, ...) can be sliced zero-copy out of `bytes` via
/// [`slice_of`]. `diag` is threaded down to [`parse_5424`], which uses it to report a
/// well-formed-but-unrepresentable TIMESTAMP without failing the whole line over it.
fn parse_line(
    bytes: &Bytes,
    text: &str,
    recv_ts: i64,
    diag: &mut Diagnostics,
) -> Result<Event, CodecError> {
    let malformed = || CodecError::Malformed(format!("malformed syslog line: {text:?}"));

    if !text.starts_with('<') {
        return Err(malformed());
    }
    let after_lt = &text[1..];
    let gt = after_lt.find('>').ok_or_else(malformed)?;
    // 1-3 digits between '<' and '>'.
    if gt == 0 || gt > 3 {
        return Err(malformed());
    }
    let digits = &after_lt[..gt];
    if !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(malformed());
    }
    // RFC 3164 and RFC 5424 both define PRI as facility*8+severity in 0..=191, encoded with no
    // leading zero except the literal value `0`. `<013>` and `<192>..<999>` are therefore
    // malformed, not merely unusual: accepting them would attach an impossible facility/severity
    // (e.g. facility 124 for `<999>`) to the event.
    if digits.len() > 1 && digits.starts_with('0') {
        return Err(malformed());
    }
    let pri: u32 = digits.parse().map_err(|_| malformed())?;
    if pri > 191 {
        return Err(malformed());
    }
    let after_pri = &after_lt[gt + 1..];

    let facility = pri / 8;
    let severity_num = pri % 8;
    let severity = map_severity(severity_num);

    // Disambiguate: a leading version digit followed by a space means RFC 5424; anything else is
    // RFC 3164.
    let mut chars = after_pri.char_indices();
    let is_5424_after = match (chars.next(), chars.next()) {
        (Some((_, c0)), Some((i1, ' '))) if c0.is_ascii_digit() => Some((c0, &after_pri[i1 + 1..])),
        _ => None,
    };

    match is_5424_after {
        Some((version, after_version)) => {
            match parse_5424(
                bytes,
                text,
                after_version,
                facility,
                severity_num,
                severity,
                recv_ts,
                diag,
            ) {
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
                    Ok(parse_3164(
                        bytes,
                        text,
                        after_pri,
                        facility,
                        severity_num,
                        severity,
                        recv_ts,
                    ))
                }
            }
        }
        None => Ok(parse_3164(bytes, text, after_pri, facility, severity_num, severity, recv_ts)),
    }
}

/// Checks the `Mmm dd hh:mm:ss` shape at the start of `s` (exactly 15 bytes: 3-letter month, ' ',
/// a space- or zero-padded day, ' ', `hh:mm:ss`). Returns `(timestamp, rest)` with exactly one
/// following space consumed from `rest` when present; `None` when absent -- tolerated, per
/// nginx's occasional omission of fields RFC 3164 calls mandatory.
fn parse_3164_timestamp(s: &str) -> Option<(&str, &str)> {
    if !s.is_char_boundary(15) {
        return None;
    }
    let ts = &s[..15];
    let b = ts.as_bytes();
    let digit = |i: usize| b[i].is_ascii_digit();
    let ok = b[0].is_ascii_alphabetic()
        && b[1].is_ascii_alphabetic()
        && b[2].is_ascii_alphabetic()
        && b[3] == b' '
        && (b[4] == b' ' || digit(4))
        && digit(5)
        && b[6] == b' '
        && digit(7)
        && digit(8)
        && b[9] == b':'
        && digit(10)
        && digit(11)
        && b[12] == b':'
        && digit(13)
        && digit(14);
    if !ok {
        return None;
    }
    let after = &s[15..];
    Some((ts, after.strip_prefix(' ').unwrap_or(after)))
}

/// A token qualifies as a syslog TAG if it ends in `:` (which includes `name[pid]:`, since that
/// ends in `]:`... followed by `:`) *and* everything before that trailing colon looks like a
/// process name -- see the module doc comment for why this is stricter than "ends in `:`" read
/// literally.
fn is_tag_shaped(token: &str) -> bool {
    let Some(body) = token.strip_suffix(':') else { return false };
    if body.is_empty() {
        return false;
    }
    let name = if let Some(open) = body.rfind('[') {
        if !body.ends_with(']') {
            return false;
        }
        let pid = &body[open + 1..body.len() - 1];
        // Must fit `u64` (what `syslog.pid` is stored as), not merely be all-digit -- an
        // unauthenticated sender can otherwise put an arbitrarily long digit run in `[...]` and
        // panic the listener at the `.parse().expect(...)` call site in `parse_3164`. A PID this
        // large is never real, so falling through to "not TAG-shaped" (the whole token, and thus
        // the rest of the line, is treated as an untagged message) is correct, not just safe.
        if pid.is_empty() || pid.parse::<u64>().is_err() {
            return false;
        }
        &body[..open]
    } else {
        body
    };
    !name.is_empty()
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b'/'))
}

#[allow(clippy::too_many_arguments)]
fn parse_3164(
    bytes: &Bytes,
    text: &str,
    after_pri: &str,
    facility: u32,
    severity_num: u32,
    severity: Severity,
    recv_ts: i64,
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
        attrs.insert("syslog.timestamp", Value::Str(slice_of(bytes, text, ts)));
    }
    if let Some(host) = hostname {
        if !host.is_empty() {
            attrs.insert("syslog.hostname", Value::Str(slice_of(bytes, text, host)));
        }
    }
    if let Some(tag_token) = tag {
        let tag_body = &tag_token[..tag_token.len() - 1]; // strip the trailing ':'
        if let Some(open) = tag_body.rfind('[') {
            // `is_tag_shaped` already validated the `[<digits>]` shape.
            let name = &tag_body[..open];
            let pid_str = &tag_body[open + 1..tag_body.len() - 1];
            attrs.insert("syslog.tag", Value::Str(slice_of(bytes, text, name)));
            attrs.insert(
                "syslog.pid",
                Value::U64(pid_str.parse().expect("is_tag_shaped validated the PID fits u64")),
            );
        } else {
            attrs.insert("syslog.tag", Value::Str(slice_of(bytes, text, tag_body)));
        }
    }

    let message = Value::Str(slice_of(bytes, text, msg));
    Event::log(
        recv_ts,
        attrs,
        LogRecord { message, severity: Some(severity), body_format: BodyFormat::Raw },
    )
}

/// `-` (the RFC 5424 nil value) or an empty field both mean "absent" -- every nillable field
/// (HOSTNAME, APP-NAME, PROCID, MSGID, TIMESTAMP) is treated identically.
fn nil_or(field: &str) -> Option<&str> {
    if field.is_empty() || field == "-" {
        None
    } else {
        Some(field)
    }
}

fn field_value(bytes: &Bytes, text: &str, field: &str) -> Option<Value> {
    nil_or(field).map(|f| Value::Str(slice_of(bytes, text, f)))
}

/// Parses (and discards the contents of) RFC 5424 STRUCTURED-DATA: either the nil marker `-`, or
/// one or more concatenated `[...]` SD-ELEMENTs. Honors a backslash-escaped `]` inside a
/// parameter value (RFC 5424 section 6.3.3) so it can't terminate an element early. Returns the
/// byte offset into `s` where MSG begins, having consumed exactly one following space when
/// present. `None` means neither a nil marker nor a well-formed bracketed element was found (an
/// unbalanced `[` runs off the end of `s`) -- a malformed line.
fn skip_structured_data(s: &str) -> Option<usize> {
    if let Some(rest) = s.strip_prefix('-') {
        return Some(1 + if rest.starts_with(' ') { 1 } else { 0 });
    }
    let bytes = s.as_bytes();
    let mut i = 0;
    let mut any = false;
    while bytes.get(i) == Some(&b'[') {
        any = true;
        i += 1;
        let mut escaped = false;
        loop {
            match bytes.get(i) {
                None => return None,
                Some(b']') if !escaped => {
                    i += 1;
                    break;
                }
                Some(b'\\') if !escaped => {
                    escaped = true;
                    i += 1;
                }
                Some(_) => {
                    escaped = false;
                    i += 1;
                }
            }
        }
    }
    if !any {
        return None;
    }
    if bytes.get(i) == Some(&b' ') {
        i += 1;
    }
    Some(i)
}

#[allow(clippy::too_many_arguments)]
fn parse_5424(
    bytes: &Bytes,
    text: &str,
    after_version: &str,
    facility: u32,
    severity_num: u32,
    severity: Severity,
    recv_ts: i64,
    diag: &mut Diagnostics,
) -> Result<Event, CodecError> {
    let malformed = || CodecError::Malformed(format!("malformed RFC 5424 syslog line: {text:?}"));

    let (ts_field, rest) = split_first_token(after_version);
    let (host_field, rest) = split_first_token(rest);
    let (app_field, rest) = split_first_token(rest);
    let (procid_field, rest) = split_first_token(rest);
    let (msgid_field, rest) = split_first_token(rest);
    let sd_offset = skip_structured_data(rest).ok_or_else(malformed)?;
    let msg = &rest[sd_offset..];

    let mut attrs = AttrMap::new();
    attrs.insert("syslog.facility", Value::U64(facility as u64));
    attrs.insert("syslog.severity", Value::U64(severity_num as u64));
    // A nil TIMESTAMP (`-`) means "absent", same as every other nillable field -- but a non-nil
    // TIMESTAMP that fails to parse is not "absent", it's malformed input, and must take the same
    // skip-and-continue path a bad PRI does rather than silently landing on the floor with no
    // `syslog.timestamp` attribute and no diagnostic. A TIMESTAMP that *does* parse but names an
    // instant outside the `i64` nanosecond range `Value::Timestamp` uses is a different condition
    // from malformed, though: the line and every other field on it are still good, so it's kept,
    // with `syslog.timestamp` omitted and a throttled diagnostic instead of the whole record
    // being discarded over one unrepresentable field.
    match nil_or(ts_field) {
        None => {}
        Some(ts) => match parse_rfc3339_to_nanos(ts) {
            Ok(nanos) => {
                attrs.insert("syslog.timestamp", Value::Timestamp(nanos));
            }
            Err(TimestampError::OutOfRange) => {
                diag.warn_throttled(
                    "timestamp_out_of_range",
                    format_args!(
                        "RFC 5424 TIMESTAMP {ts:?} is well-formed but names an instant outside \
                         the representable range; keeping the event without syslog.timestamp"
                    ),
                );
            }
            Err(TimestampError::Malformed) => return Err(malformed()),
        },
    }
    if let Some(v) = field_value(bytes, text, host_field) {
        attrs.insert("syslog.hostname", v);
    }
    if let Some(v) = field_value(bytes, text, app_field) {
        attrs.insert("syslog.tag", v);
    }
    if let Some(pid) = nil_or(procid_field) {
        // PROCID is a free-form string per RFC 5424 (it need not be numeric), but `syslog.pid`
        // is documented as `Value::U64` -- nginx always emits its numeric PID here, and a
        // non-numeric PROCID (legal per the RFC, just not what this integration's sender does) is
        // dropped rather than forced into the wrong type.
        if let Ok(n) = pid.parse::<u64>() {
            attrs.insert("syslog.pid", Value::U64(n));
        }
    }
    if let Some(v) = field_value(bytes, text, msgid_field) {
        attrs.insert("syslog.msgid", v);
    }

    // RFC 5424 section 6.4: MSG may open with a UTF-8 BOM (`EF BB BF`, i.e. U+FEFF) to signal
    // `MSG-UTF8`. It's a signal, not payload, so it's stripped rather than left to leak into
    // `log.message` as a leading U+FEFF. Still a genuine slice of `text` (`strip_prefix` on a
    // `&str` returns a subslice, never a copy), so `slice_of`'s "always a subset" precondition
    // holds.
    let msg = msg.strip_prefix('\u{FEFF}').unwrap_or(msg);
    let message = Value::Str(slice_of(bytes, text, msg));
    Ok(Event::log(
        recv_ts,
        attrs,
        LogRecord { message, severity: Some(severity), body_format: BodyFormat::Raw },
    ))
}

/// Why an RFC 3339 timestamp couldn't become a `syslog.timestamp` attribute -- kept distinct from
/// a plain `Option`/bare error because the two cases warrant different treatment at the call
/// site: [`Malformed`](TimestampError::Malformed) means the *line* is bad (same skip-and-continue
/// path as a bad PRI), while [`OutOfRange`](TimestampError::OutOfRange) means the timestamp is
/// syntactically fine but names an instant the `i64`-nanosecond [`Value::Timestamp`] can't
/// represent -- the rest of the line is still good and should be kept, just without that one
/// attribute.
enum TimestampError {
    Malformed,
    OutOfRange,
}

/// Parses an RFC 3339 timestamp (the form RFC 5424 mandates for TIMESTAMP) into Unix nanoseconds.
/// Self-contained here rather than shared through `logit-core` -- this has exactly one caller, and
/// pulling in a date/time crate for it is an ADR-scale decision (`AGENTS.md`) that doesn't belong
/// inside one input's PR. Accepts an uppercase `Z` or `+HH:MM`/`-HH:MM` offset and an optional
/// 1-6 digit fractional seconds component (RFC 5424's `TIME-SECFRAC`), padded to nanosecond
/// precision. Rejects a calendar date that doesn't exist (`2024-02-31`, `2023-02-29`), an
/// out-of-range offset, and a leap second (`:60`) -- RFC 5424 forbids all three, and silently
/// normalizing or truncating them would attach a confidently-typed but wrong `syslog.timestamp`.
/// Separately, a timestamp that parses cleanly but falls outside the representable `i64`
/// nanosecond range (roughly 1677-09-21 to 2262-04-11) is [`TimestampError::OutOfRange`], not
/// [`TimestampError::Malformed`] -- see that type's doc for why the distinction matters.
/// TODO: replace with a real crate once the crate list is finalized (see `logit-config`'s
/// hand-rolled humantime duration codec for the precedent this follows).
fn parse_rfc3339_to_nanos(s: &str) -> Result<i64, TimestampError> {
    let (days, seconds_of_day, offset_seconds, nanos_frac) =
        parse_rfc3339_components(s).ok_or(TimestampError::Malformed)?;
    let total_seconds = days * 86_400 + seconds_of_day - offset_seconds;
    total_seconds
        .checked_mul(1_000_000_000)
        .and_then(|n| n.checked_add(nanos_frac))
        .ok_or(TimestampError::OutOfRange)
}

/// The well-formedness half of [`parse_rfc3339_to_nanos`]: validates and decomposes `s` into
/// `(days since the Unix epoch, seconds of day, offset in seconds, fractional nanoseconds)`,
/// deferring the final arithmetic (and its overflow check) to the caller. Split out purely so
/// "malformed" and "out of range" can be told apart -- see [`TimestampError`].
fn parse_rfc3339_components(s: &str) -> Option<(i64, i64, i64, i64)> {
    let b = s.as_bytes();
    if b.len() < 20 {
        return None; // "YYYY-MM-DDTHH:MM:SSZ" is the shortest legal form.
    }
    let digits = |start: usize, n: usize| -> Option<i64> {
        let slice = s.get(start..start + n)?;
        slice.bytes().all(|c| c.is_ascii_digit()).then(|| slice.parse().ok())?
    };

    let year = digits(0, 4)?;
    if b[4] != b'-' {
        return None;
    }
    let month = digits(5, 2)?;
    if b[7] != b'-' {
        return None;
    }
    let day = digits(8, 2)?;
    // RFC 5424 (unlike RFC 3339 itself) requires uppercase `T` and `Z`.
    if b[10] != b'T' {
        return None;
    }
    let hour = digits(11, 2)?;
    if b[13] != b':' {
        return None;
    }
    let minute = digits(14, 2)?;
    if b[16] != b':' {
        return None;
    }
    let second = digits(17, 2)?;

    let mut idx = 19;
    let mut nanos_frac: i64 = 0;
    if b.get(idx) == Some(&b'.') {
        idx += 1;
        let frac_start = idx;
        while b.get(idx).is_some_and(u8::is_ascii_digit) {
            idx += 1;
        }
        let frac_len = idx - frac_start;
        // RFC 5424's `TIME-SECFRAC` is `"." 1*6DIGIT` -- at least one digit, at most six.
        if !(1..=6).contains(&frac_len) {
            return None;
        }
        let frac = &s[frac_start..idx];
        let mut padded = [b'0'; 9];
        for (dst, src) in padded.iter_mut().zip(frac.bytes()) {
            *dst = src;
        }
        nanos_frac = std::str::from_utf8(&padded).ok()?.parse().ok()?;
    }

    let offset_seconds: i64 = match b.get(idx) {
        Some(b'Z') => {
            idx += 1;
            0
        }
        Some(sign @ (b'+' | b'-')) => {
            let sign: i64 = if *sign == b'-' { -1 } else { 1 };
            let oh = digits(idx + 1, 2)?;
            if b.get(idx + 3) != Some(&b':') {
                return None;
            }
            let om = digits(idx + 4, 2)?;
            if !(0..=23).contains(&oh) || !(0..=59).contains(&om) {
                return None;
            }
            idx += 6;
            sign * (oh * 3600 + om * 60)
        }
        _ => return None,
    };
    if idx != b.len() {
        return None; // trailing garbage
    }
    // RFC 5424 forbids leap seconds outright (unlike RFC 3339, which allows `:60`) -- `second`
    // is checked against 0..=59, not 0..=60.
    if !(1..=12).contains(&month) || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    if day < 1 || day > days_in_month(year, month) {
        return None;
    }

    let days = days_from_civil(year, month, day);
    let seconds_of_day = hour * 3600 + minute * 60 + second;
    Some((days, seconds_of_day, offset_seconds, nanos_frac))
}

/// Whether `y` is a Gregorian leap year -- divisible by 4, except century years, which must also
/// be divisible by 400 (so 2000 is a leap year, 1900 and 2100 are not).
fn is_leap_year(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

/// Days in `month` (1-12) of `y`, honoring [`is_leap_year`] for February. `month` is assumed
/// already range-checked by the caller to 1..=12.
fn days_in_month(y: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(y) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// Howard Hinnant's `days_from_civil` -- proleptic Gregorian, correct for any year (including
/// negative/pre-1970), and exactly the ~15 lines a hand-rolled date conversion needs; see
/// <http://howardhinnant.github.io/date_algorithms.html>. `logit-core::time` (workstream D) adds
/// the inverse (civil-from-days, for formatting); this is the one direction that input needs.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = (m + 9) % 12; // [0, 11]
    let doy = (153 * mp + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(datagram: &str) -> Vec<Event> {
        let mut decoder = SyslogDecoder::new(Arc::new(Resource::default()));
        decoder.decode(Bytes::from(datagram.to_string())).expect("decode should succeed").events
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
    /// call-time clock read -- the property `docs/adr/0026-decoupled-listener-io.md` exists for:
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

    fn message_str(event: &Event) -> &str {
        event.log.as_ref().expect("event should carry a log").message.as_str().unwrap()
    }

    fn parse_err(line: &str) -> CodecError {
        let bytes = Bytes::from(line.to_string());
        let text = std::str::from_utf8(&bytes).unwrap();
        let mut diag = Diagnostics::default();
        parse_line(&bytes, text, 0, &mut diag).expect_err("expected this line to be rejected")
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
    fn rfc3164_tag_with_a_pid_that_overflows_u64_does_not_panic() {
        // Regression test for the blocker: a PID this long used to reach a bare
        // `.parse().expect(...)` and panic the listener task on one crafted UDP packet.
        let line = "<13>tag[99999999999999999999]: hello";
        let event = only_event(decode(line));
        // The oversized PID makes the whole token fail `is_tag_shaped`, so there's no tag/pid --
        // the untouched remainder becomes the message, rather than the line being dropped.
        assert!(event.attributes.get("syslog.tag").is_none());
        assert!(event.attributes.get("syslog.pid").is_none());
        assert_eq!(message_str(&event), "tag[99999999999999999999]: hello");
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
    fn rfc5424_decodes_msgid_and_timestamp_and_omits_nil_fields() {
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
        assert!(event.attributes.get("syslog.timestamp").is_none());
        assert!(event.attributes.get("syslog.hostname").is_none());
        assert!(event.attributes.get("syslog.tag").is_none());
        assert!(event.attributes.get("syslog.pid").is_none());
        assert!(event.attributes.get("syslog.msgid").is_none());
        assert_eq!(message_str(&event), "nil fields");
    }

    #[test]
    fn rfc5424_invalid_timestamp_is_rejected_rather_than_treated_as_absent() {
        for ts in [
            "2024-02-31T00:00:00Z",         // February has no 31st day
            "2023-02-29T00:00:00Z",         // 2023 is not a leap year
            "2024-13-01T00:00:00Z",         // month 13
            "2024-01-01T00:00:00+99:99",    // offset hour/minute both out of range
            "2024-01-01T23:59:60Z",         // RFC 5424 forbids leap seconds
            "2024-01-01t00:00:00z",         // lowercase t/z
            "2024-01-01T00:00:00.1234567Z", // 7 fractional digits, RFC 5424 allows at most 6
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
    fn rfc5424_structured_data_is_skipped_including_an_escaped_bracket() {
        let event = only_event(decode(r#"<134>1 - - - - - [id@32473 k="v\]"] the message"#));
        assert_eq!(message_str(&event), "the message");
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
        // packet as soon as any single byte anywhere was invalid UTF-8.
        let mut datagram = Vec::new();
        datagram.extend_from_slice(b"<13>a\n");
        datagram.extend_from_slice(&[0xff, 0xfe]); // not valid UTF-8, no `<PRI>` either
        datagram.push(b'\n');
        datagram.extend_from_slice(b"<13>b");
        let mut decoder = SyslogDecoder::new(Arc::new(Resource::default()));
        let events = decoder.decode(Bytes::from(datagram)).expect("decode should succeed").events;
        assert_eq!(events.len(), 2);
        assert_eq!(message_str(&events[0]), "a");
        assert_eq!(message_str(&events[1]), "b");
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
}
