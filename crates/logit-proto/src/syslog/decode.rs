//! Decoding RFC 3164 and RFC 5424 syslog into events: the code behind [`super`]'s module doc,
//! which is the spec for everything here.
//!
//! [`SyslogDecoder::decode_into`] splits its input into messages (on `\n`, unless line splitting
//! is off) and hands each to `parse_line`, which reads PRI, sniffs the dialect, and calls
//! `parse_3164` or `parse_5424`. Framing is the listener's job: `syslog_in` hands this decoder a
//! whole datagram or one RFC 6587 frame.

use crate::{CodecError, Decoder};
use bytes::Bytes;
use logit_core::interner::{intern, KeyCache};
use logit_core::subslice;
use logit_core::time::{parse_rfc3339_to_nanos, TimestampError};
use logit_core::{
    AttrMap, BodyFormat, Diagnostics, Event, LogRecord, Resource, Scope, Severity, Symbol, Value,
};
use std::sync::{Arc, LazyLock};

/// Decodes syslog bytes into events; testable without a socket.
///
/// `Clone` because `syslog_in`'s TCP driver gives every connection its own decoder
/// (`crates/logit-inputs/src/tcp.rs`'s module doc). A clone shares its `Diagnostics` throttle
/// counts (`logit_core::Diagnostics`' type doc), so `bad_line` throttles listener-wide. It must: a
/// peer looping connect, one bad message, close would otherwise report its "1st" occurrence once
/// per connection forever.
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
    /// Off is for a transport that already delimits messages, such as `syslog_in`'s TCP arm and its
    /// RFC 6587 [`crate::framing::Framer`]. Splitting there would be wrong: an octet-counted MSG
    /// may contain `\n`, and re-splitting would turn one multiline message into half-messages, most
    /// of them missing a PRI and dropped as `bad_line`.
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

    /// This decoder's diagnostics handle, public for
    /// [`crate::collectd::CollectdDecoder::diag`]'s reason.
    pub fn diag(&self) -> &Diagnostics {
        &self.diag
    }

    /// Whether [`Self::with_line_splitting`] left splitting on. Public for the same reason as
    /// [`Self::diag`]: `syslog_in`'s test that its TCP arm turns splitting off.
    pub fn line_splitting(&self) -> bool {
        self.line_splitting
    }
}

impl Decoder for SyslogDecoder {
    fn decode_into(
        &mut self,
        bytes: Bytes,
        received_at: i64,
        out: &mut Vec<Event>,
    ) -> Result<(Arc<Resource>, Option<Arc<Scope>>), CodecError> {
        // Split on raw bytes, with no UTF-8 check: `parse_line` validates each field (`super`'s
        // module doc).
        if !self.line_splitting {
            // One framed message. An octet-counted MSG-LEN may cover a trailing `\r\n`; strip one
            // `\n`, then one `\r` behind it, so it decodes as the same line over UDP would (the
            // splitting arm and `logit_proto::framing::Framer`'s LF framing strip the same).
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

/// Splits `s` at the first ASCII space into `(token, rest)`, consuming the space. With no space,
/// `rest` is `&s[s.len()..]`, not `b""`, so it stays a suffix of `s` and an empty field shares the
/// line rather than taking [`subslice::share`]'s copy path.
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

/// A PROCID or `tag[pid]` token as a `u64` when it is canonical decimal: ASCII digits with no
/// leading zero, or `0` (`super`'s module doc, `syslog.pid`). `u64::from_str` alone would also take
/// `+5` and `007`, which `syslog_out` would then write as `5` and `7`.
fn canonical_pid(token: &[u8]) -> Option<u64> {
    let canonical = !token.is_empty()
        && token.iter().all(u8::is_ascii_digit)
        && (token.len() == 1 || token[0] != b'0');
    if !canonical {
        return None;
    }
    std::str::from_utf8(token).ok()?.parse().ok()
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

    // Dialect sniff (`super`'s module doc, "Dialect disambiguation").
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
                // Whatever the version, a line that doesn't parse as RFC 5424 is most likely a
                // tag-less RFC 3164 MSG starting with a digit and a space (`<14>1 worker died`),
                // and rejecting it would drop it, so reparse it as RFC 3164, which never fails and
                // keeps the whole remainder as MSG. Reported, so a malformed RFC 5424 sender or a
                // future version still shows.
                Err(err) => {
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
/// like a process name; `super`'s module doc's "The RFC 3164 header" says why this is stricter than
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
        // PRINTUSASCII without `]`, so the bracket still balances (`super`'s module doc,
        // `syslog.pid`). Every decimal PID is in that class.
        if !pid.iter().all(|&b| is_printusascii_byte(b) && b != b']') {
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
        attrs.insert_sym(KEYS.timestamp, Value::Str(subslice::share(line, ts)));
    }
    if let Some(host) = hostname {
        if !host.is_empty() {
            // RFC 3164 parsing never fails (the sniff fallback depends on it), and `Value::Str`
            // must be valid UTF-8, so a non-UTF-8 HOSTNAME is skipped and reported instead.
            if std::str::from_utf8(host).is_ok() {
                attrs.insert_sym(KEYS.hostname, Value::Str(subslice::share(line, host)));
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
            attrs.insert_sym(KEYS.tag, Value::Str(subslice::share(line, name)));
            match canonical_pid(pid_bytes) {
                Some(n) => attrs.insert_sym(KEYS.pid, Value::U64(n)),
                // `is_tag_shaped` guarantees the PID is PRINTUSASCII, so valid UTF-8.
                None => attrs.insert_sym(KEYS.pid, Value::Str(subslice::share(line, pid_bytes))),
            }
        } else {
            attrs.insert_sym(KEYS.tag, Value::Str(subslice::share(line, tag_body)));
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
            Ok(Some(Value::Str(subslice::share(line, f))))
        }
    }
}

/// Builds the MSG [`Value`]: valid UTF-8 is a [`Value::Str`], minus a leading BOM when
/// `strip_bom` is set; invalid UTF-8 is a raw [`Value::Bytes`], BOM and all (`super`'s module doc).
fn message_value(line: &Bytes, msg: &[u8], strip_bom: bool) -> Value {
    match std::str::from_utf8(msg) {
        Ok(s) => {
            let s = if strip_bom { s.strip_prefix('\u{FEFF}').unwrap_or(s) } else { s };
            Value::Str(subslice::share(line, s.as_bytes()))
        }
        Err(_) => Value::Bytes(subslice::share(line, msg)),
    }
}

/// One STRUCTURED-DATA grammar violation. `offset` is relative to [`parse_structured_data`]'s
/// input; the caller adds its base for the `sniff_fallback` message.
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
/// where MSG begins, one following space consumed. `super`'s module doc has the grammar and shape.
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
        // `AttrMap::get`, not an intern, so a line rejected before its first PARAM or
        // SD-ELEMENT completes interns nothing; the sniff routes RFC 3164 lines containing
        // `[token` through here before falling back. A completed PARAM's name and a closed
        // element's SD-ID are interned as they complete, before the rest of the line is
        // validated, so a line rejected later has interned those. That is the interner exposure
        // `docs/design/memory.md`'s "Interning: the bargain, and its bounds" accepts, under ADR
        // `deployment-threat-model`: a sender writing a malformed line writes it with the same
        // names each time.
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
    // For an absolute `SdError` offset. `after_version` and every `rest` `split_first_token`
    // returns are suffixes of `line`, so the length difference is where `rest` starts.
    let sd_base = line.len() - rest.len();
    let (sd_value, sd_offset) = parse_structured_data(rest, keys).map_err(|e| {
        malformed(format!("STRUCTURED-DATA at byte {}: {}", sd_base + e.offset, e.message))
    })?;
    let msg = &rest[sd_offset..];

    let mut attrs = AttrMap::new();
    attrs.insert_sym(KEYS.facility, Value::U64(facility as u64));
    attrs.insert_sym(KEYS.severity, Value::U64(severity_num as u64));
    // Nil or empty is an explicit `Value::Null`; unparseable fails the RFC 5424 parse, so the line
    // falls back to RFC 3164; parseable but out of `i64`-nanosecond range keeps the event without
    // the attribute (`super`'s module doc).
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
        // Free-form PRINTUSASCII: `U64` when numeric, else `Str` (`super`'s module doc,
        // `syslog.pid`).
        if !is_printusascii(pid) {
            return Err(malformed(format!(
                "PROCID {:?} is not PRINTUSASCII",
                String::from_utf8_lossy(pid)
            )));
        }
        match canonical_pid(pid) {
            Some(n) => {
                attrs.insert_sym(KEYS.pid, Value::U64(n));
            }
            None => {
                attrs.insert_sym(KEYS.pid, Value::Str(subslice::share(line, pid)));
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
    use crate::framing::{Framer, Framing, FramingMode, MAX_FRAME_BYTES};

    fn decode(datagram: &str) -> Vec<Event> {
        let mut decoder = SyslogDecoder::new(Arc::new(Resource::default()));
        decoder.decode(Bytes::from(datagram.to_string())).expect("decode should succeed").events
    }

    fn decode_bytes(datagram: Vec<u8>) -> Vec<Event> {
        let mut decoder = SyslogDecoder::new(Arc::new(Resource::default()));
        decoder.decode(Bytes::from(datagram)).expect("decode should succeed").events
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

    /// Decodes `line`, which sniffs as RFC 5424 and fails that parse, asserting it falls back to
    /// RFC 3164 with a `sniff_fallback`: one event whose MSG is everything after PRI, with no
    /// RFC 5424 field stamped (`super`'s module doc, "Dialect disambiguation").
    fn falls_back(line: &str) -> Event {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog", "input");
        let diag = Diagnostics::new("syslog_in").with_telemetry(telemetry);
        let mut decoder = SyslogDecoder::new(Arc::new(Resource::default())).with_diagnostics(diag);
        let batch = decoder.decode(Bytes::from(line.to_string())).expect("decode should succeed");
        let event = only_event(batch.events);
        let after_pri = &line[line.find('>').unwrap() + 1..];
        assert_eq!(message_str(&event), after_pri, "{line}: the whole remainder is MSG");
        for key in ["syslog.timestamp", "syslog.msgid", "syslog.sd"] {
            assert!(event.attributes.get(key).is_none(), "{line}: {key} is not stamped");
        }
        let keys: Vec<_> = registry
            .drain(0)
            .iter()
            .filter_map(|e| e.attributes.get("key").and_then(Value::as_str).map(String::from))
            .collect();
        assert_eq!(keys, ["sniff_fallback"], "{line}");
        event
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
        // A PID that overflows `u64` must not panic; it becomes `Value::Str` (`super`'s module doc,
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
        // A TAG-shaped token with nothing after it: the empty MSG is an empty field at the end of
        // the line, which must decode without a panic.
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
    fn rfc5424_invalid_timestamp_falls_back_rather_than_being_treated_as_absent() {
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
            falls_back(&format!("<134>1 {ts} - - - - - msg"));
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
        assert!(
            logit_core::subslice::within(&bytes, &msg),
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
    /// the `KeyCache` holds only the id and three param names. `nextest` runs each test in its
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
    fn structured_data_33_char_sd_name_falls_back() {
        // Past RFC 5424's 32-byte limit, so the line isn't RFC 5424.
        let id = "a".repeat(33);
        falls_back(&format!(r#"<134>1 - - - - - [{id} k="v"] msg"#));
    }

    #[test]
    fn structured_data_duplicate_sd_id_falls_back() {
        falls_back(r#"<134>1 - - - - - [ex@32473 k="v"][ex@32473 j="w"] msg"#);
    }

    /// The tag-less RFC 3164 line Python's `SysLogHandler` sends for a message starting `1 `:
    /// version `1` gets the same fallback as every other version.
    #[test]
    fn a_tag_less_rfc3164_msg_starting_1_space_falls_back_to_rfc3164() {
        let event = falls_back("<14>1 worker died");
        assert_eq!(event.attributes.get("syslog.severity"), Some(&Value::U64(6)));
        assert!(event.attributes.get("syslog.tag").is_none());
        assert!(event.attributes.get("syslog.hostname").is_none());
        falls_back("<13>1 2 3 msg");
    }

    /// A line whose STRUCTURED-DATA fails before its first PARAM or SD-ELEMENT completes interns
    /// nothing, whatever its version, and falls back to RFC 3164 with its whole MSG. A line that
    /// fails later has interned the names it completed: the exposure `parse_structured_data`'s
    /// comment accepts. `nextest` runs each test in its own process, so `interner::len()`
    /// reflects only this test.
    #[test]
    fn a_line_whose_structured_data_fails_interns_only_the_names_it_completed() {
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

        // A version-`1` line whose SD-ELEMENT is malformed: it falls back the same way.
        let malformed = Bytes::from_static(b"<134>1 - - - - - [badelem@1 msg");
        let events = decoder.decode(malformed).expect("decode should succeed").events;
        assert_eq!(message_str(&only_event(events)), "1 - - - - - [badelem@1 msg");

        assert_eq!(
            logit_core::interner::len(),
            before,
            "nothing on a line that fails before a PARAM completes reaches the interner"
        );

        // One complete PARAM and one closed element, then a malformed second element: the line
        // falls back, and the completed PARAM-NAME and SD-ID are interned.
        let late = Bytes::from_static(b"<134>1 - - - - - [first@1 done=\"v\"][second@1 broken");
        let events = decoder.decode(late).expect("decode should succeed").events;
        assert!(only_event(events).attributes.get("syslog.sd").is_none());
        assert_eq!(
            logit_core::interner::len(),
            before + 2,
            "`done` and `first@1` completed before the failure, and nothing else did"
        );
    }

    /// `syslog.pid` is `U64` only for canonical decimal, so a PID `u64::from_str` would rewrite
    /// (`+5`, `007`) stays the `Str` the sender wrote and relays unchanged.
    #[test]
    fn a_non_canonical_numeric_pid_stays_a_str() {
        for (line, pid) in [
            ("<134>1 - - - +5 - - msg", "+5"),
            ("<134>1 - - - 007 - - msg", "007"),
            ("<13>app[+5]: msg", "+5"),
            ("<13>app[007]: msg", "007"),
        ] {
            let event = only_event(decode(line));
            assert_eq!(
                event.attributes.get("syslog.pid").and_then(Value::as_str),
                Some(pid),
                "{line}"
            );
        }
        for (line, pid) in [
            ("<134>1 - - - 0 - - msg", 0),
            ("<134>1 - - - 18446744073709551615 - - msg", u64::MAX),
            ("<13>app[0]: msg", 0),
            ("<13>app[42]: msg", 42),
        ] {
            let event = only_event(decode(line));
            assert_eq!(event.attributes.get("syslog.pid"), Some(&Value::U64(pid)), "{line}");
        }
    }

    #[test]
    fn structured_data_unterminated_element_falls_back() {
        falls_back(r#"<134>1 - - - - - [ex@32473 k="v""#);
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

    // ---- recorded RFC 6587 TCP stream (testdata/interop/syslog/) -----------------------------
    //
    // A real rsyslog `omfwd` forwarder's TCP byte stream at its default `TCP_Framing` (RFC 6587
    // §3.4.2 non-transparent), captured by `script/record-fixtures rsyslog-tcp`
    // (`testdata/interop/syslog/README.md`'s `rsyslog-tcp-000.raw` row), pushed through `Framer`
    // as it arrived and decoded with the real `SyslogDecoder`.

    /// `testdata/interop/syslog/<name>` as raw bytes: a TCP fixture is a whole connection's byte
    /// stream, not one UTF-8 datagram like `interop_fixture` reads.
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
        let mut decoder = SyslogDecoder::new(Arc::new(Resource::default()));
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
}
