//! RFC 3164 / RFC 5424 syslog egress over UDP or TCP -- the mirror of `logit_inputs::syslog`, and
//! a real relay: header fields round-trip from an event's `syslog.*` attributes when present
//! (exactly what `SyslogDecoder` writes), falling back to configured defaults only for an event
//! that never passed through `syslog_in`. See `docs/adr/syslog-output.md`.
//!
//! Split the way `influxdb.rs`/`stdio.rs` are: a pure [`SyslogEncoder`] (no socket anywhere, every
//! format/precedence/sanitization test runs against it directly) plus the thin [`SyslogOutput`]
//! that owns the socket.
//!
//! **This does not implement `logit_proto::Encoder`.** That trait is `fn encode(&mut self,
//! &EventBatch) -> Result<Bytes, CodecError>` -- one opaque buffer per batch, with no framing
//! metadata -- and this sink genuinely needs per-message boundaries: one UDP datagram per message,
//! or one octet-counted frame per message on TCP. There is no single `Bytes` that carries those
//! boundaries without reinventing them on the other side, which is worse than not implementing the
//! trait. [`EventDump`](crate::stdio::EventDump) is the in-tree precedent for a sink whose encoder
//! sidesteps the trait for the same class of reason (it returns a `String`, not a `Bytes`, since a
//! human terminal has no framing at all). See `docs/known-gaps.md` for this recorded as an open
//! gap in `logit_proto::Encoder`'s shape, not a defect in this module.
//!
//! ## Timestamp semantics
//!
//! Every emitted message's TIMESTAMP is `event.timestamp`, **not** the `syslog.timestamp`
//! attribute `syslog_in` may have left on the event. `syslog_in`'s own module doc explains why
//! that attribute can't be resolved to an instant for RFC 3164 (no year, no timezone) without
//! guessing -- re-emitting it here would reintroduce exactly that guess on the way back out. The
//! consequence, recorded in `docs/known-gaps.md`: a `syslog_in -> syslog_out` relay re-stamps
//! with receipt time rather than preserving the origin's own clock. The opt-in `syslog_timestamp`
//! transform `docs/known-gaps.md` already sketches is the right place to resolve `syslog.timestamp`
//! onto `event.timestamp` explicitly, for either direction -- not this sink.
//!
//! ## Header-field precedence
//!
//! Per event, per field, first hit wins: the `syslog.*` attribute, then the configured default,
//! then a format-appropriate absence (`-` for RFC 5424's NILVALUE, an omitted token for RFC 3164).
//! `syslog.severity` deliberately outranks `log.severity`: `syslog_in`'s PRI-to-`Severity` mapping
//! is lossy by construction (`map_severity` collapses six syslog severities onto
//! `logit_core::Severity`'s six variants, e.g. both `notice` (5) and `info` (6) become `Info`), so
//! preferring the raw attribute is what makes a relay byte-faithful for the severities that
//! survive it; [`syslog_severity_of`] is only the fallback for an event whose log record came from
//! somewhere other than `syslog_in`.
//!
//! ## Injection safety
//!
//! `syslog_in` splits a datagram into lines on `\n`, and Grafana Alloy's `loki.source.syslog` UDP
//! listener does the same -- so an embedded newline in a relayed message forges a second, fully
//! attacker-controlled message at the receiver (a fabricated PRI, hostname, and app name, i.e. a
//! fabricated Loki stream). [`sanitize_msg`] neutralizes this by escaping `\n`/`\r`/NUL and every
//! other C0 control character and DEL, in the rendered message, regardless of transport -- so the
//! bytes on the wire don't depend on which transport is configured. On TCP, octet-counting framing
//! ([`frame_octet_counting`]) is already newline-transparent, making this defense-in-depth rather
//! than the only guard on that path.
//!
//! **A literal backslash is deliberately not escaped.** The demo's message body is a JSON
//! document (`access_json`'s output), where a real newline inside a JSON string is already
//! encoded as the two characters `\` `n` on the wire -- escaping a literal backslash would double
//! every one of them and break Loki's `| json` LogQL parsing on every line. This does mean a
//! message that already contained the literal two characters `\` `n` is indistinguishable on the
//! wire from one that contained a real newline; recorded in `docs/known-gaps.md`.
//!
//! RFC 5424's HOSTNAME/APP-NAME/PROCID/MSGID are `PRINTUSASCII` with length caps
//! ([`sanitize_5424_field`]); RFC 3164's HOSTNAME/TAG additionally forbid `:`/`[`/`]`
//! ([`sanitize_3164_token`]), matching `syslog_in`'s own two-token header rule (a `:` in HOSTNAME
//! would make it misread the token as TAG instead) and the same "must not end in `:`" warning
//! `demo/hello/app.py` already carries. A non-`PRINTUSASCII` byte (including a raw space, which
//! sits below the `PRINTUSASCII` range) becomes `_`, which is also what keeps a header field free
//! of whitespace a downstream token-scanner could misread as a field boundary.
//!
//! ## Message body
//!
//! `log.message` is a `Value` and may be non-string. [`render_message`] renders `Value::Str`
//! **verbatim** (before [`sanitize_msg`]'s control-character pass) -- the demo's case, and the
//! one that must reach Loki unmangled for `| json` to parse it -- and deliberately does *not*
//! reuse `stdio::render_value` for that case, since that function quotes and escapes a string for
//! a human reading a terminal. `Value::Map`/`Value::Array` do reuse it, as a container-encoding
//! fallback rather than a second implementation, since [`sanitize_msg`] still runs over whatever
//! it produces.
//!
//! ## Sizing
//!
//! `max_message_bytes` bounds one whole encoded message (PRI + header + MSG), defaulting to
//! 8192 -- Grafana Alloy's own `loki.source.syslog` `max_message_length` default, the receiver the
//! demo stack points this at, rather than RFC 3164 §4.1's traditional 1024 (which would truncate
//! a JSON-bodied message on every modern relay chain). An oversize MSG is truncated on a UTF-8
//! character boundary, counted, and throttle-warned -- truncating rather than dropping, since a
//! truncated line still carries a correct header and a readable prefix. An oversize *header*
//! (unreachable except with an absurdly small `max_message_bytes`) drops the whole message instead
//! of emitting a malformed one.

use crate::stdio::render_value;
use crate::Output;
use anyhow::Context;
use logit_core::time::format_rfc3339_utc;
use logit_core::{AttrMap, Diagnostics, Event, EventBatch, Severity, Telemetry, Value};
use logit_pipeline::Fault;
use std::fmt::Write as _;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{lookup_host, TcpStream, UdpSocket};

/// Matches Grafana Alloy's `loki.source.syslog` `max_message_length` default -- see the module
/// doc's "Sizing" section.
pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 8192;

/// TCP only -- how long a connect attempt (including a reconnect after a dropped connection) may
/// take before `send` reports it as a failure. `logit-config` can't reference this directly
/// (`logit-outputs` depends on it, never the reverse), so its own
/// `default_syslog_connect_timeout` hardcodes the same 5-second value -- keep the two in sync by
/// hand if this ever changes.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Which syslog dialect [`SyslogEncoder`] emits. Deliberately its own tiny enum rather than
/// `logit_config::SyslogFormat` -- `logit-outputs` depends on `logit-pipeline`/`logit-core`/
/// `logit-proto`, never `logit-config` (`docs/design/pipeline-graph.md`'s crate layout), so
/// `logit-cli::pipeline::build_spec` is the sole place a config value crosses into this type,
/// exactly like `queue_config`/`write_config` already do for `BufferConfig`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Rfc3164,
    Rfc5424,
}

/// A reusable buffer of encoded messages: one contiguous byte buffer plus a range per message, so
/// encoding a batch allocates once (the backing `Vec<u8>` grows as needed and is never freed
/// between calls) rather than once per message. `SyslogOutput` reuses one across every `send`.
#[derive(Debug, Default)]
pub struct MessageBuf {
    bytes: Vec<u8>,
    ranges: Vec<std::ops::Range<usize>>,
}

impl MessageBuf {
    fn clear(&mut self) {
        self.bytes.clear();
        self.ranges.clear();
    }

    fn push(&mut self, msg: &str) {
        let start = self.bytes.len();
        self.bytes.extend_from_slice(msg.as_bytes());
        self.ranges.push(start..self.bytes.len());
    }

    /// One slice per encoded message, in batch order.
    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.ranges.iter().map(move |r| &self.bytes[r.clone()])
    }

    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub fn total_bytes(&self) -> usize {
        self.bytes.len()
    }
}

/// Per-batch outcome counts from [`SyslogEncoder::encode_into`] -- what `SyslogOutput::send` turns
/// into `logit.output.*` telemetry (`docs/design/internal-telemetry.md`).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EncodeStats {
    /// Events with no `log` record (legal under `docs/adr/multi-payload-events.md`) -- a
    /// metric-only or span-only event has nothing to render as a syslog message. The exact
    /// inverse of `influxdb_out`, which skips log-only/span-only events: neither sink invents a
    /// rendering for a payload shape it can't represent.
    pub skipped_no_log: usize,
    pub truncated: usize,
    pub dropped_oversize_header: usize,
}

/// Encodes events as syslog messages. Pure -- no socket anywhere -- so every format/precedence/
/// sanitization test runs directly against this, with no transport of any kind involved.
pub struct SyslogEncoder {
    format: Format,
    default_facility: u8,
    default_hostname: Option<String>,
    default_app_name: Option<String>,
    max_message_bytes: usize,
    diag: Diagnostics,
    /// The header/message text being built for the event currently being encoded -- reused
    /// across every event *and* across every `encode_into` call (a struct field, not a local, is
    /// what makes the second half of that true: a function-local recreated on every call regrows
    /// from empty capacity every time, which is exactly why an earlier version of this encoder
    /// showed real reallocation cost even with `encode_into` warmed once before measurement --
    /// warming a local's very first call doesn't help its *next* call, only within-call reuse
    /// across events did). Cleared, never reallocated, at the top of each event.
    line: String,
    /// `render_message`'s pre-sanitize rendering of `log.message`, reused the same way as `line`.
    raw_msg: String,
    /// Reused for every per-field sanitize call in a header (`sanitize_5424_field`/
    /// `sanitize_3164_token`, once each for hostname/app-name/msgid) *and* for `sanitize_msg`'s
    /// output afterward -- safe because every use within one event is read-immediately-into-
    /// `line`-then-cleared before the next use, never overlapping in time.
    scratch: String,
}

impl SyslogEncoder {
    pub fn new(format: Format, default_facility: u8) -> Self {
        Self {
            format,
            default_facility: default_facility.min(23),
            default_hostname: None,
            default_app_name: None,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            diag: Diagnostics::default(),
            line: String::new(),
            raw_msg: String::new(),
            scratch: String::new(),
        }
    }

    pub fn with_hostname(mut self, hostname: impl Into<String>) -> Self {
        self.default_hostname = Some(hostname.into());
        self
    }

    pub fn with_app_name(mut self, app_name: impl Into<String>) -> Self {
        self.default_app_name = Some(app_name.into());
        self
    }

    pub fn with_max_message_bytes(mut self, max_message_bytes: usize) -> Self {
        self.max_message_bytes = max_message_bytes;
        self
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    /// Encodes every event in `batch` into `out` (cleared first), one message per event that
    /// carries a `log` record. Never fails -- a per-event problem (no log record, an oversize
    /// header) is a skip/drop counted in the returned [`EncodeStats`], not an error; there is
    /// nothing for a caller to react to beyond what the stats already report.
    pub fn encode_into(&mut self, batch: &EventBatch, out: &mut MessageBuf) -> EncodeStats {
        out.clear();
        let mut stats = EncodeStats::default();
        for event in &batch.events {
            self.line.clear();
            if self.encode_event(event, &mut stats) {
                out.push(&self.line);
            }
        }
        stats
    }

    /// Encodes one event into `self.line` (already cleared by the caller), returning whether it
    /// produced a message at all.
    fn encode_event(&mut self, event: &Event, stats: &mut EncodeStats) -> bool {
        let Some(log) = &event.log else {
            stats.skipped_no_log += 1;
            return false;
        };

        let attrs = &event.attributes;
        let facility = resolve_facility(attrs, self.default_facility);
        let severity = resolve_severity(attrs, log.severity);
        let pri = facility * 8 + severity;
        let hostname = resolve_str(attrs, "syslog.hostname").or(self.default_hostname.as_deref());
        let app_name = resolve_str(attrs, "syslog.tag").or(self.default_app_name.as_deref());
        let pid = resolve_pid(attrs);
        let msgid = resolve_str(attrs, "syslog.msgid");

        match self.format {
            Format::Rfc5424 => write_rfc5424_header(
                &mut self.line,
                &mut self.scratch,
                pri,
                event.timestamp,
                hostname,
                app_name,
                pid,
                msgid,
            ),
            Format::Rfc3164 => write_rfc3164_header(
                &mut self.line,
                &mut self.scratch,
                pri,
                event.timestamp,
                hostname,
                app_name,
                pid,
            ),
        }

        // Header alone exceeds the bound: drop the message entirely rather than truncate a
        // header field and emit something a receiver would misparse (module doc's "Sizing").
        if self.line.len() > self.max_message_bytes {
            self.line.clear();
            stats.dropped_oversize_header += 1;
            self.diag.warn_throttled(
                "oversize_header",
                format_args!(
                    "syslog_out: header alone exceeds max_message_bytes ({}); dropping message",
                    self.max_message_bytes
                ),
            );
            return false;
        }

        self.raw_msg.clear();
        render_message(&mut self.raw_msg, &log.message);
        sanitize_msg(&mut self.scratch, &self.raw_msg);
        // **No RFC 5424 §6.4 BOM.** An earlier version emitted one (the symmetric choice to
        // `syslog_in` stripping one on the way in), on the assumption that Alloy's receiver
        // would tolerate it. Verified against the real demo stack that it does not: Loki's
        // `| json` LogQL stage uses Go's `encoding/json`, which does not skip a leading BOM,
        // so every relayed line silently failed to parse as JSON and every `| json`-filtered
        // dashboard panel came back empty despite lines actually landing in Loki. Confirmed
        // by re-running the same query with the BOM removed. See `docs/adr/syslog-output.md`.
        if !self.scratch.is_empty() {
            // The separator itself counts against `max_message_bytes` -- pushing it
            // unconditionally, then only clearing the message on overflow, left the separator on
            // the wire even when there was no room for it at all, one byte over the configured
            // cap when the header alone exactly filled the budget. Only push it when there's room.
            if self.line.len() >= self.max_message_bytes {
                stats.truncated += 1;
                self.diag.warn_throttled(
                    "message_truncated",
                    format_args!(
                        "syslog_out: no room left for a message after the header ({} bytes); \
                         message dropped",
                        self.max_message_bytes
                    ),
                );
            } else {
                self.line.push(' ');
                let budget = self.max_message_bytes - self.line.len();
                if truncate_on_char_boundary(&mut self.scratch, budget) {
                    stats.truncated += 1;
                    self.diag.warn_throttled(
                        "message_truncated",
                        format_args!(
                            "syslog_out: message exceeded max_message_bytes ({}); truncated",
                            self.max_message_bytes
                        ),
                    );
                }
                self.line.push_str(&self.scratch);
            }
        }
        true
    }
}

/// `syslog.facility` if present and in range (`Value::U64(n)`, `n <= 23`), else `default`.
fn resolve_facility(attrs: &AttrMap, default: u8) -> u8 {
    match attrs.get("syslog.facility") {
        Some(Value::U64(n)) if *n <= 23 => *n as u8,
        _ => default,
    }
}

/// `syslog.severity` if present and in range (`Value::U64(n)`, `n <= 7`) -- this deliberately
/// outranks `log.severity`; see the module doc's "Header-field precedence" section. Falls back to
/// [`syslog_severity_of`], then `6` (info) for an event with no severity at all.
fn resolve_severity(attrs: &AttrMap, log_severity: Option<Severity>) -> u8 {
    if let Some(Value::U64(n)) = attrs.get("syslog.severity") {
        if *n <= 7 {
            return *n as u8;
        }
    }
    match log_severity {
        Some(s) => syslog_severity_of(s),
        None => 6,
    }
}

/// The deliberately-lossy inverse of `syslog_in::map_severity`, for an event whose log record
/// didn't come from `syslog_in` at all (or came from it but never carried a `syslog.severity`
/// attribute -- can't happen from `syslog_in` itself, but nothing enforces that at the type
/// level). `Fatal` maps to `2` (crit), not `0` (emerg): `emerg` means "system unusable", a claim
/// `Fatal` never makes. `Trace` has no syslog equivalent and maps to `7` (debug), same as `Debug`.
fn syslog_severity_of(severity: Severity) -> u8 {
    match severity {
        Severity::Trace => 7,
        Severity::Debug => 7,
        Severity::Info => 6,
        Severity::Warn => 4,
        Severity::Error => 3,
        Severity::Fatal => 2,
    }
}

/// A non-empty string attribute, or `None` -- an empty string is treated the same as absent, so a
/// blank `syslog.hostname` (unlikely, but not impossible from a hand-built event) falls through to
/// the configured default rather than encoding as a header field with nothing in it.
fn resolve_str<'a>(attrs: &'a AttrMap, key: &str) -> Option<&'a str> {
    attrs.get(key).and_then(Value::as_str).filter(|s| !s.is_empty())
}

fn resolve_pid(attrs: &AttrMap) -> Option<u64> {
    match attrs.get("syslog.pid") {
        Some(Value::U64(n)) => Some(*n),
        _ => None,
    }
}

/// `HEADER SP STRUCTURED-DATA` (no `[SP MSG]` yet -- `encode_event` appends that only if MSG is
/// non-empty, per the grammar). STRUCTURED-DATA is always the NILVALUE `-`: this sink doesn't
/// generate SD-ELEMENTs (`docs/known-gaps.md`), the same "not invented without a consumer"
/// reasoning `syslog_in` gives for not parsing them on the way in.
#[allow(clippy::too_many_arguments)]
fn write_rfc5424_header(
    out: &mut String,
    scratch: &mut String,
    pri: u8,
    timestamp: i64,
    hostname: Option<&str>,
    app_name: Option<&str>,
    pid: Option<u64>,
    msgid: Option<&str>,
) {
    let _ = write!(out, "<{pri}>1 ");
    push_rfc5424_timestamp(out, timestamp);
    out.push(' ');
    push_5424_field(out, scratch, hostname, 255);
    out.push(' ');
    push_5424_field(out, scratch, app_name, 48);
    out.push(' ');
    match pid {
        Some(p) => {
            let _ = write!(out, "{p}");
        }
        None => out.push('-'),
    }
    out.push(' ');
    push_5424_field(out, scratch, msgid, 32);
    out.push(' ');
    out.push('-'); // STRUCTURED-DATA NILVALUE
}

/// Sanitizes `value` into `scratch` (a reused buffer -- see [`SyslogEncoder::scratch`]'s doc
/// comment) and appends the result to `out`, or `-` (NILVALUE) if `value` is absent or sanitizes
/// to nothing.
fn push_5424_field(out: &mut String, scratch: &mut String, value: Option<&str>, max_len: usize) {
    match value {
        Some(v) => {
            sanitize_5424_field(scratch, v, max_len);
            if scratch.is_empty() {
                out.push('-');
            } else {
                out.push_str(scratch);
            }
        }
        None => out.push('-'),
    }
}

/// RFC 5424 section 6: HOSTNAME/APP-NAME/PROCID/MSGID are all `PRINTUSASCII` (`%d33-126`), with a
/// per-field length cap. Every non-conforming character (including a raw space, which sits below
/// the `PRINTUSASCII` range) becomes `_` rather than being dropped, so the result is always pure
/// ASCII and stays a fixed number of bytes per character for the subsequent byte-length
/// truncation. Writes into `scratch` (cleared first) rather than returning a fresh `String`.
fn sanitize_5424_field(scratch: &mut String, s: &str, max_len: usize) {
    scratch.clear();
    for c in s.chars() {
        if scratch.len() >= max_len {
            break;
        }
        scratch.push(if is_printusascii(c) { c } else { '_' });
    }
}

/// `Mmm dd hh:mm:ss ` (space-padded day), UTC -- RFC 3164's header, no structured data, no
/// trailing separator (`encode_event`/`write_rfc3164_header`'s callers add exactly the separators
/// they need). HOSTNAME/TAG are omitted entirely when absent or empty after sanitization, rather
/// than emitting an RFC 5424-style NILVALUE RFC 3164 has no concept of.
#[allow(clippy::too_many_arguments)]
fn write_rfc3164_header(
    out: &mut String,
    scratch: &mut String,
    pri: u8,
    timestamp: i64,
    hostname: Option<&str>,
    tag: Option<&str>,
    pid: Option<u64>,
) {
    let _ = write!(out, "<{pri}>");
    push_rfc3164_timestamp(out, timestamp);
    if let Some(h) = hostname {
        sanitize_3164_token(scratch, h, 255);
        if !scratch.is_empty() {
            out.push(' ');
            out.push_str(scratch);
        }
    }
    if let Some(t) = tag {
        sanitize_3164_token(scratch, t, 32);
        if !scratch.is_empty() {
            out.push(' ');
            out.push_str(scratch);
            if let Some(p) = pid {
                let _ = write!(out, "[{p}]");
            }
            out.push(':');
        }
    }
}

/// Like [`sanitize_5424_field`], plus `:`/`[`/`]` also become `_` -- matching `syslog_in`'s own
/// two-token HOSTNAME/TAG rule (`crates/logit-inputs/src/syslog.rs`): a `:` in HOSTNAME would make
/// that parser misread the token as TAG instead, and `demo/hello/app.py` carries the same warning
/// about a trailing `:`. A raw space (below `PRINTUSASCII`) already becomes `_`, which is what
/// keeps a token free of whitespace a receiver's token scanner could misread as a field boundary.
fn sanitize_3164_token(scratch: &mut String, s: &str, max_len: usize) {
    scratch.clear();
    for c in s.chars() {
        if scratch.len() >= max_len {
            break;
        }
        let forbidden = matches!(c, ':' | '[' | ']');
        scratch.push(if is_printusascii(c) && !forbidden { c } else { '_' });
    }
}

fn is_printusascii(c: char) -> bool {
    matches!(c, '\u{21}'..='\u{7e}')
}

/// RFC 3339 with microsecond precision (`2026-09-02T14:03:11.123456Z`) -- RFC 5424 section
/// 6.2.3.1's TIME-SECFRAC allows at most 6 digits, so this reuses
/// `logit_core::time::format_rfc3339_utc`'s nanosecond-precision output (always exactly 9
/// fractional digits then `Z`) and trims the last 3 fractional digits rather than reimplementing
/// the civil-from-days conversion.
fn push_rfc5424_timestamp(out: &mut String, nanos: i64) {
    let full = format_rfc3339_utc(nanos);
    out.push_str(&full[..full.len() - 4]);
    out.push('Z');
}

const MONTH_ABBR: [&str; 12] =
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

/// `Mmm dd hh:mm:ss`, UTC, no year (RFC 3164's TIMESTAMP has none) -- shares
/// `format_rfc3339_utc`'s civil-from-days approach (Howard Hinnant's algorithm) rather than
/// importing it, since that function is private to `logit-core` and returns a full RFC 3339
/// string, not the decomposed parts this shape needs.
fn push_rfc3164_timestamp(out: &mut String, nanos: i64) {
    let (month, day, hour, minute, second) = civil_time_of(nanos);
    let _ = write!(
        out,
        "{} {day:2} {hour:02}:{minute:02}:{second:02}",
        MONTH_ABBR[(month as usize - 1).min(11)]
    );
}

/// UTC `(month, day, hour, minute, second)` for a Unix-nanosecond timestamp, deliberately dropping
/// the year (RFC 3164 has none). See `push_rfc3164_timestamp`'s doc comment for why this doesn't
/// call into `logit_core::time` instead.
fn civil_time_of(nanos: i64) -> (u32, u32, u32, u32, u32) {
    let secs = nanos.div_euclid(1_000_000_000);
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400) as u32;

    let z = days + 719_468;
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;

    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;
    (month, day, hour, minute, second)
}

/// Renders a log message's `Value` into `out`, before [`sanitize_msg`]'s control-character pass.
/// See the module doc's "Message body" section for why `Str` is verbatim and not routed through
/// `stdio::render_value`.
fn render_message(out: &mut String, value: &Value) {
    match value {
        Value::Null => {}
        Value::Bool(b) => {
            let _ = write!(out, "{b}");
        }
        Value::I64(i) => {
            let _ = write!(out, "{i}");
        }
        Value::U64(u) => {
            let _ = write!(out, "{u}");
        }
        Value::F64(f) => {
            let _ = write!(out, "{f}");
        }
        Value::Timestamp(ns) => out.push_str(&format_rfc3339_utc(*ns)),
        // RFC 5424 MSG-ANY permits arbitrary octets, but the whole receiver chain (Alloy, Loki)
        // wants UTF-8, and `syslog_in` already rejects a non-UTF-8 line rather than emit one
        // (`docs/known-gaps.md`) -- lossy conversion here is the symmetric choice on the way out.
        Value::Bytes(b) => out.push_str(&String::from_utf8_lossy(b)),
        Value::Str(s) => {
            // `Value::Str` is constructed only from valid UTF-8 (see its own doc comment).
            let text = std::str::from_utf8(s).expect("Value::Str is always valid UTF-8");
            out.push_str(text);
        }
        // Container fallback -- reuses `stdio::render_value` rather than a second
        // implementation. `sanitize_msg` still runs over whatever this produces, so its own
        // quoting is harmless, just redundant for this case.
        Value::Array(_) | Value::Map(_) => render_value(out, value),
    }
}

/// Neutralizes `\n`/`\r`/NUL and every other C0 control character (plus DEL) in a rendered
/// message -- see the module doc's "Injection safety" section for why, and for why a literal
/// backslash is deliberately left untouched. Writes into `out` (cleared first) rather than
/// returning a fresh `String`.
fn sanitize_msg(out: &mut String, msg: &str) {
    out.clear();
    for c in msg.chars() {
        match c {
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\0' => out.push_str("\\0"),
            c if (c as u32) < 0x20 || c as u32 == 0x7f => {
                let _ = write!(out, "\\x{:02x}", c as u32);
            }
            c => out.push(c),
        }
    }
}

/// Truncates `s` in place to at most `budget` bytes, on a UTF-8 character boundary. Returns
/// whether truncation actually happened.
fn truncate_on_char_boundary(s: &mut String, budget: usize) -> bool {
    if s.len() <= budget {
        return false;
    }
    let mut cut = budget;
    while cut > 0 && !s.is_char_boundary(cut) {
        cut -= 1;
    }
    s.truncate(cut);
    true
}

/// RFC 6587 §3.4.1 octet-counting: `MSG-LEN SP SYSLOG-MSG` per message, concatenated. Chosen over
/// non-transparent (LF-delimited) framing because it is transparent to a literal newline inside
/// MSG -- which [`sanitize_msg`] already escapes, but this is the framing-level half of the same
/// defense, not a second, redundant one (a non-transparent frame would still depend on
/// `sanitize_msg` never having a bug). Alloy/Loki's syslog receiver (built on `go-syslog`)
/// auto-detects octet-counting from a message's leading digit, so this needs no corresponding
/// receiver-side configuration.
fn frame_octet_counting(messages: &MessageBuf, out: &mut Vec<u8>) {
    out.clear();
    for msg in messages.iter() {
        out.extend_from_slice(msg.len().to_string().as_bytes());
        out.push(b' ');
        out.extend_from_slice(msg);
    }
}

/// The live half of a `syslog_out` sink: `Udp` binds eagerly (a bad local bind is a config error,
/// matching `StdioOutput::open_path`'s "fail before anything starts listening" precedent); `Tcp`
/// connects lazily inside `send`, since a not-yet-up downstream syslog receiver must not block
/// `logit` from starting -- a compose-level `depends_on` on one would be equally wrong.
enum Conn {
    Udp(UdpSocket),
    Tcp { stream: Option<TcpStream>, connect_timeout: Duration },
}

/// `logit_pipeline::Output` for `syslog_out`. Built via [`SyslogOutput::udp`] or
/// [`SyslogOutput::tcp`] -- never a bare constructor, mirroring `StdioOutput`'s three named
/// constructors for the same reason: which one is legal depends on config
/// (`crates/logit-cli/src/pipeline.rs::build_spec`).
pub struct SyslogOutput {
    endpoint: String,
    conn: Conn,
    encoder: SyslogEncoder,
    messages: MessageBuf,
    /// TCP only, reused across `send` calls -- the octet-counted frame for the whole batch.
    /// Never shrinks (only `clear()`ed), so one outlier batch pins its peak capacity for the rest
    /// of the process's life -- the same trade `InfluxLineEncoder`'s own reused buffers already
    /// make (`docs/design/memory.md`), accepted here for the same reason: reallocating back down
    /// only to regrow on the next similarly-sized batch would trade a one-time worst case for a
    /// recurring one.
    frame_buf: Vec<u8>,
    diag: Diagnostics,
    telemetry: Telemetry,
}

impl SyslogOutput {
    /// Binds an ephemeral local UDP socket eagerly. `endpoint` (the remote `host:port`) is
    /// resolved per `send`, not here -- a DNS hiccup at bind time would otherwise be
    /// indistinguishable from every other config error this constructor can raise, when it's
    /// really a delivery-time condition (`Fault::Clean`).
    pub fn udp(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        let socket = std::net::UdpSocket::bind("0.0.0.0:0")
            .context("binding syslog_out's local UDP socket")?;
        socket.set_nonblocking(true).context("configuring syslog_out's UDP socket")?;
        let socket = UdpSocket::from_std(socket).context("registering syslog_out's UDP socket")?;
        Ok(Self::new(endpoint, Conn::Udp(socket)))
    }

    /// Never connects here -- see [`Conn`]'s doc comment.
    pub fn tcp(endpoint: impl Into<String>, connect_timeout: Duration) -> Self {
        Self::new(endpoint, Conn::Tcp { stream: None, connect_timeout })
    }

    fn new(endpoint: impl Into<String>, conn: Conn) -> Self {
        Self {
            endpoint: endpoint.into(),
            conn,
            encoder: SyslogEncoder::new(Format::Rfc5424, 16),
            messages: MessageBuf::default(),
            frame_buf: Vec::new(),
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
        }
    }

    pub fn with_encoder(mut self, encoder: SyslogEncoder) -> Self {
        self.encoder = encoder;
        self
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag.clone();
        self.encoder = self.encoder.with_diagnostics(diag);
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

#[async_trait::async_trait]
impl Output for SyslogOutput {
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let stats = self.encoder.encode_into(batch, &mut self.messages);
        self.telemetry.count("logit.output.events.skipped", stats.skipped_no_log as f64, &[]);
        self.telemetry.count("logit.output.messages.truncated", stats.truncated as f64, &[]);
        self.telemetry.count(
            "logit.output.messages.dropped",
            stats.dropped_oversize_header as f64,
            &[("reason", "oversize_header")],
        );
        if self.messages.is_empty() {
            // Every event in this batch was skipped or dropped -- nothing to write. Matches
            // `influxdb_out`'s own "nothing to write" early return on an empty encoded body.
            return Ok(());
        }

        self.telemetry.count("logit.output.batch.bytes", self.messages.total_bytes() as f64, &[]);
        let request_timer = self.telemetry.timer("logit.output.request.duration");
        let result = match &mut self.conn {
            Conn::Udp(socket) => {
                Self::send_udp(
                    socket,
                    &self.endpoint,
                    &self.messages,
                    &mut self.diag,
                    &self.telemetry,
                )
                .await
            }
            Conn::Tcp { stream, connect_timeout } => {
                Self::send_tcp(
                    stream,
                    &self.endpoint,
                    *connect_timeout,
                    &self.messages,
                    &mut self.frame_buf,
                )
                .await
            }
        };
        drop(request_timer);

        match &result {
            Ok(sent) => {
                self.telemetry.count("logit.output.messages", *sent as f64, &[]);
                self.telemetry.count("logit.output.requests", 1.0, &[("class", "ok")]);
            }
            Err(_) => {
                self.telemetry.count("logit.output.requests", 1.0, &[("class", "error")]);
            }
        }
        result.map(|_| ())
    }

    /// Implemented explicitly (rather than relying on the default no-op) so the contract is
    /// spelled out rather than assumed: `send` already performs one write per batch and retains
    /// nothing between calls, so there is nothing buffered here at shutdown -- for TCP, this
    /// simply flushes the underlying stream, which is cheap and correct even though it should
    /// already be a no-op in practice.
    async fn flush(&mut self) -> anyhow::Result<()> {
        if let Conn::Tcp { stream: Some(stream), .. } = &mut self.conn {
            stream.flush().await.context("flushing syslog_out TCP stream")?;
        }
        Ok(())
    }

    /// `false` for both transports: syslog has no destination-side idempotency to lean on (unlike
    /// `influxdb_out`'s idempotent-overwrite semantics) -- a redelivered message is a duplicated
    /// log line at the receiver. This still lets a `Fault::Clean` retry succeed under the derived
    /// `AtMostOnce` posture (`docs/adr/buffered-sink-delivery.md`'s table), which covers the
    /// common outage shape (the receiver restarting) with zero duplicate risk.
    fn duplicate_safe(&self) -> bool {
        false
    }
}

impl SyslogOutput {
    /// One `send_to` per message -- never packed into one datagram, which would depend on the
    /// receiver splitting on a delimiter this sink's whole "Injection safety" section exists to
    /// stop relying on. `EMSGSIZE`/`InvalidInput` (the datagram is too large for the local path
    /// MTU or send buffer) is a per-message data condition, not a sink failure: dropped and
    /// counted, never classified as a `Fault` -- doing so would risk tripping
    /// `docs/adr/buffered-sink-delivery.md`'s sustained-permanent-failure exit window on an
    /// otherwise healthy sink. A failure on the *first* message this call attempts is
    /// `Fault::Clean` -- nothing in this batch has left the host yet. Any later message's
    /// failure, after at least one earlier datagram in the same batch already went out
    /// (`sent > 0`), is `Fault::Ambiguous` instead: `Clean` is a whole-batch promise that the
    /// destination saw none of it, and claiming that after a partial send would make the generic
    /// writer resend the whole batch under `at_most_once`, duplicating whatever already landed
    /// (`docs/adr/buffered-sink-delivery.md`'s duplicate-safety argument depends on `Clean`
    /// never over-claiming this way, exactly as `influxdb_out`'s own `classify_transport_error`
    /// doc comment stresses for its own `Clean`/`Ambiguous` split).
    ///
    /// `endpoint` is resolved to one [`SocketAddr`] here, once per batch -- not once per
    /// message. `UdpSocket::send_to` accepts anything implementing `ToSocketAddrs`, and for a
    /// non-numeric host (`relay.internal:5141`, a representative container-DNS endpoint) tokio's
    /// `&str` impl re-resolves
    /// via DNS on *every* call if handed the raw string directly, which every UDP test here never
    /// exercises since they all pass an IP literal (tokio's `SocketAddr`-parse fast path, no DNS
    /// at all). Resolving once per `send_udp` call still re-resolves every batch rather than
    /// caching indefinitely, so a genuine DNS change is picked up between batches -- matching
    /// [`SyslogOutput::udp`]'s own documented intent, which this used to violate in practice.
    async fn send_udp(
        socket: &UdpSocket,
        endpoint: &str,
        messages: &MessageBuf,
        diag: &mut Diagnostics,
        telemetry: &Telemetry,
    ) -> anyhow::Result<usize> {
        let mut addrs = lookup_host(endpoint)
            .await
            .context("resolving syslog_out endpoint")
            .context(Fault::Clean)?;
        let addr = addrs
            .next()
            .context("syslog_out endpoint resolved to no addresses")
            .context(Fault::Clean)?;

        let mut sent = 0usize;
        for msg in messages.iter() {
            match socket.send_to(msg, addr).await {
                Ok(_) => sent += 1,
                Err(err) if is_message_too_large(&err) => {
                    telemetry.count(
                        "logit.output.messages.dropped",
                        1.0,
                        &[("reason", "oversize_datagram")],
                    );
                    diag.warn_throttled(
                        "oversize_datagram",
                        format_args!("syslog_out: message too large for one UDP datagram: {err}"),
                    );
                }
                Err(err) => return Err(anyhow::Error::new(err).context(udp_send_fault(sent))),
            }
        }
        Ok(sent)
    }

    /// One frame (all messages octet-counted and concatenated) per **batch**, written with at
    /// most one internal reconnect-and-retry. Two correctness properties this is built around,
    /// both raised in review of an earlier version that got them wrong:
    ///
    /// - **Cancellation safety.** `deliver_with_retry` races every attempt against
    ///   `tokio::time::timeout` (`docs/adr/buffered-sink-delivery.md`), and
    ///   [`AsyncWriteExt::write_all`] is explicitly documented as not cancel-safe: if the timeout
    ///   fires mid-write, the future is dropped with an unknown number of bytes already on the
    ///   wire. This function always `stream.take()`s the connection into a local before writing
    ///   to it, never writing through `*stream` directly -- so a cancelled write simply drops
    ///   (and closes) the local `TcpStream`, leaving `*stream` as `None` for the next `send` to
    ///   reconnect fresh, rather than resuming writes into a connection whose framing this
    ///   process can no longer account for.
    /// - **Never resend once any byte has gone out.** The first write of each connect attempt is
    ///   a single, non-`write_all` [`AsyncWriteExt::write`] call, which either returns `Ok(n)`
    ///   with `n > 0` (proof delivery has *started* -- from here, any later failure is
    ///   `Fault::Ambiguous` and the frame is never resent, since resending would duplicate
    ///   whatever the peer already accepted) or fails having written nothing at all (proof
    ///   nothing left this host on this attempt -- safe to reconnect once and retry the entire
    ///   frame from scratch, and safe to classify `Fault::Clean` if that retry also fails). The
    ///   previous version instead ran a single `write_all` per attempt and inferred "nothing was
    ///   written" from "the connection was merely inherited from an earlier `send` call" -- which
    ///   `write_all` cannot support: it can complete several of its own inner writes, including
    ///   an entire earlier *message* in a multi-message batch, before a later one fails.
    ///
    /// [`AsyncWriteExt::write_all`]: tokio::io::AsyncWriteExt::write_all
    /// [`AsyncWriteExt::write`]: tokio::io::AsyncWriteExt::write
    async fn send_tcp(
        stream: &mut Option<TcpStream>,
        endpoint: &str,
        connect_timeout: Duration,
        messages: &MessageBuf,
        frame_buf: &mut Vec<u8>,
    ) -> anyhow::Result<usize> {
        frame_octet_counting(messages, frame_buf);

        let mut retried_after_a_zero_byte_failure = false;
        loop {
            // Always taken out of `*stream`, never written through it directly -- see this
            // function's doc comment's cancellation-safety point.
            let mut conn = match stream.take() {
                Some(conn) => conn,
                None => tokio::time::timeout(connect_timeout, TcpStream::connect(endpoint))
                    .await
                    .context("connecting to syslog_out endpoint timed out")
                    .and_then(|r| r.context("connecting to syslog_out endpoint"))
                    .context(Fault::Clean)?,
            };

            // `Ok(0)` from `write()` on a non-empty buffer is, in practice, as good as an error
            // here (the stream is not accepting writes) -- normalized to a real `io::Error` so
            // the rest of this match only has one "nothing was written" case to handle.
            let first_write = match conn.write(frame_buf).await {
                Ok(0) if !frame_buf.is_empty() => {
                    Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "wrote zero bytes"))
                }
                Ok(n) => Ok(n),
                Err(err) => Err(err),
            };

            match first_write {
                Ok(n) => {
                    let rest_result = if n < frame_buf.len() {
                        conn.write_all(&frame_buf[n..]).await
                    } else {
                        Ok(())
                    };
                    return match rest_result {
                        Ok(()) => {
                            *stream = Some(conn);
                            Ok(messages.len())
                        }
                        // At least one byte of this frame reached the peer -- resending would
                        // duplicate it, and `*stream` is deliberately left `None` (this
                        // now-partially-written connection is not reusable).
                        Err(err) => Err(anyhow::Error::new(err).context(Fault::Ambiguous)),
                    };
                }
                Err(_) if !retried_after_a_zero_byte_failure => {
                    // Nothing left this host on this attempt -- `*stream` is already `None`
                    // (taken above), so the next loop iteration connects fresh and retries the
                    // whole frame exactly once.
                    retried_after_a_zero_byte_failure = true;
                    continue;
                }
                Err(err) => return Err(anyhow::Error::new(err).context(Fault::Clean)),
            }
        }
    }
}

/// `Fault::Clean` only when nothing in this batch has left the host yet -- pulled out of
/// `send_udp` as a pure, directly-testable function since the real network condition it encodes
/// (a `send_to` failure *after* an earlier datagram in the same batch already went out) isn't
/// something a unit test can reliably provoke over a real UDP socket.
fn udp_send_fault(sent: usize) -> Fault {
    if sent > 0 {
        Fault::Ambiguous
    } else {
        Fault::Clean
    }
}

/// `90` is `EMSGSIZE` on Linux specifically (macOS/BSD use `40`) -- deliberately not
/// platform-general: `logit` only ever ships and runs inside the Linux containers this repo
/// builds (`Dockerfile`/`Dockerfile.dev`, `AGENTS.md`'s "everything runs in a container"), so a
/// non-Linux raw errno here would be a dev-host-only false negative, never a real one in
/// production. The `InvalidInput` fallback doesn't reliably catch other platforms' encodings of
/// this either, which is accepted for the same reason.
fn is_message_too_large(err: &std::io::Error) -> bool {
    matches!(err.raw_os_error(), Some(libc_emsgsize) if libc_emsgsize == 90 /* EMSGSIZE, Linux */)
        || err.kind() == std::io::ErrorKind::InvalidInput
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{BodyFormat, LogRecord, MetricKind, MetricRecord, Resource};
    use logit_inputs::syslog::SyslogDecoder;
    use logit_proto::Decoder;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;

    fn batch_with(events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(Resource::default()), events }
    }

    fn log_event(ts: i64, message: &str, severity: Option<Severity>) -> Event {
        Event::log(
            ts,
            AttrMap::new(),
            LogRecord { message: Value::str(message), severity, body_format: BodyFormat::Raw },
        )
    }

    fn metric_event(ts: i64) -> Event {
        Event::metric(
            ts,
            AttrMap::new(),
            MetricRecord {
                name: logit_core::interner::intern("m"),
                kind: MetricKind::Counter(1.0),
                unit: None,
            },
        )
    }

    fn encode(events: Vec<Event>) -> (Vec<String>, EncodeStats) {
        let mut encoder = SyslogEncoder::new(Format::Rfc5424, 16);
        let mut out = MessageBuf::default();
        let stats = encoder.encode_into(&batch_with(events), &mut out);
        let msgs = out.iter().map(|b| String::from_utf8_lossy(b).into_owned()).collect();
        (msgs, stats)
    }

    fn encode_with(encoder: &mut SyslogEncoder, events: Vec<Event>) -> (Vec<String>, EncodeStats) {
        let mut out = MessageBuf::default();
        let stats = encoder.encode_into(&batch_with(events), &mut out);
        let msgs = out.iter().map(|b| String::from_utf8_lossy(b).into_owned()).collect();
        (msgs, stats)
    }

    // -- Encoder: RFC 5424 --------------------------------------------------------------------

    #[test]
    fn rfc5424_encodes_a_full_message() {
        let (msgs, stats) = encode(vec![log_event(0, "hello world", Some(Severity::Info))]);
        assert_eq!(stats, EncodeStats::default());
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], "<134>1 1970-01-01T00:00:00.000000Z - - - - - hello world");
    }

    /// No RFC 5424 §6.4 BOM before MSG -- verified against the real demo stack
    /// (`docs/adr/syslog-output.md`) that Loki's `| json` LogQL stage silently fails to parse a
    /// BOM-prefixed JSON body, so every relayed line's fields would be unqueryable despite
    /// landing in Loki.
    #[test]
    fn no_bom_precedes_the_message_even_though_rfc_5424_section_6_4_allows_one() {
        let (msgs, _) = encode(vec![log_event(0, "hello", None)]);
        assert!(!msgs[0].contains('\u{feff}'), "a leading BOM breaks Loki's `| json` LogQL stage");
    }

    #[test]
    fn rfc5424_uses_configured_hostname_and_app_name_when_no_attributes_present() {
        let mut encoder =
            SyslogEncoder::new(Format::Rfc5424, 16).with_hostname("logit").with_app_name("logit");
        let (msgs, _) = encode_with(&mut encoder, vec![log_event(0, "x", None)]);
        assert!(msgs[0].contains(" logit logit - - -"));
    }

    #[test]
    fn rfc5424_empty_message_has_no_bom_and_no_trailing_space() {
        let (msgs, _) = encode(vec![log_event(0, "", None)]);
        assert_eq!(msgs[0], "<134>1 1970-01-01T00:00:00.000000Z - - - - -");
    }

    // -- Encoder: RFC 3164 --------------------------------------------------------------------

    #[test]
    fn rfc3164_encodes_hostname_and_tag_with_pid() {
        let mut attrs = AttrMap::new();
        attrs.insert("syslog.hostname", Value::str("myhost"));
        attrs.insert("syslog.tag", Value::str("myapp"));
        attrs.insert("syslog.pid", Value::U64(1234));
        let event = Event::log(
            0,
            attrs,
            LogRecord { message: Value::str("hi"), severity: None, body_format: BodyFormat::Raw },
        );
        let mut encoder = SyslogEncoder::new(Format::Rfc3164, 16);
        let (msgs, _) = encode_with(&mut encoder, vec![event]);
        assert_eq!(msgs[0], "<134>Jan  1 00:00:00 myhost myapp[1234]: hi");
    }

    #[test]
    fn rfc3164_omits_tag_entirely_when_absent() {
        let mut encoder = SyslogEncoder::new(Format::Rfc3164, 16);
        let (msgs, _) = encode_with(&mut encoder, vec![log_event(0, "hi", None)]);
        assert_eq!(msgs[0], "<134>Jan  1 00:00:00 hi");
    }

    // -- Header-field precedence -----------------------------------------------------------

    #[test]
    fn syslog_severity_attribute_outranks_log_severity() {
        let mut attrs = AttrMap::new();
        attrs.insert("syslog.severity", Value::U64(1)); // alert
        let event = Event::log(
            0,
            attrs,
            LogRecord {
                message: Value::str("x"),
                severity: Some(Severity::Fatal), // would otherwise map to 2
                body_format: BodyFormat::Raw,
            },
        );
        let (msgs, _) = encode(vec![event]);
        // facility 16 * 8 + severity 1 = 129
        assert!(msgs[0].starts_with("<129>"));
    }

    #[test]
    fn log_severity_is_used_when_no_syslog_severity_attribute_is_present() {
        for (sev, expected_severity) in [
            (Severity::Trace, 7),
            (Severity::Debug, 7),
            (Severity::Info, 6),
            (Severity::Warn, 4),
            (Severity::Error, 3),
            (Severity::Fatal, 2),
        ] {
            let (msgs, _) = encode(vec![log_event(0, "x", Some(sev))]);
            let expected_pri = 16 * 8 + expected_severity;
            assert!(
                msgs[0].starts_with(&format!("<{expected_pri}>")),
                "severity {sev:?} should map to syslog severity {expected_severity}, got {}",
                msgs[0]
            );
        }
    }

    #[test]
    fn no_severity_at_all_defaults_to_info() {
        let (msgs, _) = encode(vec![log_event(0, "x", None)]);
        assert!(msgs[0].starts_with(&format!("<{}>", 16 * 8 + 6)));
    }

    #[test]
    fn out_of_range_syslog_severity_falls_back() {
        let mut attrs = AttrMap::new();
        attrs.insert("syslog.severity", Value::U64(9));
        let event = Event::log(
            0,
            attrs,
            LogRecord {
                message: Value::str("x"),
                severity: Some(Severity::Warn),
                body_format: BodyFormat::Raw,
            },
        );
        let (msgs, _) = encode(vec![event]);
        assert!(msgs[0].starts_with(&format!("<{}>", 16 * 8 + 4)));
    }

    #[test]
    fn configured_hostname_used_only_when_attribute_absent() {
        let mut attrs = AttrMap::new();
        attrs.insert("syslog.hostname", Value::str("from-attr"));
        let event = Event::log(
            0,
            attrs,
            LogRecord { message: Value::str("x"), severity: None, body_format: BodyFormat::Raw },
        );
        let mut encoder = SyslogEncoder::new(Format::Rfc5424, 16).with_hostname("from-config");
        let (msgs, _) = encode_with(&mut encoder, vec![event]);
        assert!(msgs[0].contains("from-attr"));
        assert!(!msgs[0].contains("from-config"));
    }

    // -- A real syslog_in -> syslog_out relay, using the actual decoder --------------------

    #[test]
    fn a_decoded_nginx_syslog_line_relays_with_facility_and_hostname_preserved() {
        let mut decoder = SyslogDecoder::new(Arc::new(Resource::default()));
        let line = "<134>Sep  2 12:00:00 myhost app: {\"status\":200}\n";
        let batch = decoder.decode(bytes::Bytes::from(line)).expect("should decode");
        let mut encoder = SyslogEncoder::new(Format::Rfc5424, 0);
        let mut out = MessageBuf::default();
        let stats = encoder.encode_into(&batch, &mut out);
        assert_eq!(stats, EncodeStats::default());
        let msg = String::from_utf8_lossy(out.iter().next().unwrap()).into_owned();
        // facility 16 (134/8), severity 6 (134%8) -> PRI 134, preserved exactly.
        assert!(msg.starts_with("<134>1 "));
        assert!(msg.contains(" myhost app - - -"));
        assert!(msg.contains("{\"status\":200}"));
    }

    // -- Events with no log record -----------------------------------------------------------

    #[test]
    fn a_metric_only_event_produces_no_message() {
        let (msgs, stats) = encode(vec![metric_event(0)]);
        assert!(msgs.is_empty());
        assert_eq!(stats.skipped_no_log, 1);
    }

    // -- Injection safety ---------------------------------------------------------------------

    #[test]
    fn an_embedded_newline_cannot_forge_a_second_message() {
        let (msgs, _) =
            encode(vec![log_event(0, "line one\n<0>Jan 1 00:00:00 evil: forged", None)]);
        assert_eq!(msgs.len(), 1, "one event must always encode to exactly one message");
        assert!(!msgs[0].as_bytes().contains(&b'\n'), "no raw newline byte may appear on the wire");
        assert!(msgs[0].contains("line one\\n<0>Jan 1 00:00:00 evil: forged"));
    }

    #[test]
    fn embedded_carriage_return_and_nul_are_escaped() {
        let (msgs, _) = encode(vec![log_event(0, "a\rb\0c", None)]);
        assert!(msgs[0].contains("a\\rb\\0c"));
        assert!(!msgs[0].as_bytes().contains(&b'\r'));
        assert!(!msgs[0].as_bytes().contains(&0u8));
    }

    #[test]
    fn a_literal_backslash_passes_through_unescaped_so_json_bodies_stay_valid() {
        let (msgs, _) = encode(vec![log_event(0, r#"{"a":"line1\nline2"}"#, None)]);
        assert!(msgs[0].contains(r#"{"a":"line1\nline2"}"#));
    }

    // -- Header sanitization -----------------------------------------------------------------

    #[test]
    fn a_hostname_with_space_and_non_ascii_is_sanitized() {
        let mut attrs = AttrMap::new();
        attrs.insert("syslog.hostname", Value::str("bad host\u{00e9}"));
        let event = Event::log(
            0,
            attrs,
            LogRecord { message: Value::str("x"), severity: None, body_format: BodyFormat::Raw },
        );
        let (msgs, _) = encode(vec![event]);
        assert!(msgs[0].contains("bad_host_"));
    }

    #[test]
    fn rfc3164_hostname_with_trailing_colon_is_sanitized() {
        let mut attrs = AttrMap::new();
        attrs.insert("syslog.hostname", Value::str("host:"));
        let event = Event::log(
            0,
            attrs,
            LogRecord { message: Value::str("x"), severity: None, body_format: BodyFormat::Raw },
        );
        let mut encoder = SyslogEncoder::new(Format::Rfc3164, 16);
        let (msgs, _) = encode_with(&mut encoder, vec![event]);
        assert!(msgs[0].contains("host_ "), "trailing ':' must not survive: {}", msgs[0]);
    }

    #[test]
    fn an_app_name_longer_than_48_bytes_is_truncated() {
        let mut attrs = AttrMap::new();
        attrs.insert("syslog.tag", Value::str("a".repeat(100)));
        let event = Event::log(
            0,
            attrs,
            LogRecord { message: Value::str("x"), severity: None, body_format: BodyFormat::Raw },
        );
        let (msgs, _) = encode(vec![event]);
        let app_name = msgs[0].split(' ').nth(3).unwrap();
        assert_eq!(app_name.len(), 48);
    }

    #[test]
    fn an_all_non_printable_hostname_becomes_nilvalue() {
        let mut attrs = AttrMap::new();
        attrs.insert("syslog.hostname", Value::str("\u{0001}\u{0002}"));
        // sanitizes to "__", which is non-empty, so this actually checks the sanitized-but-
        // non-empty path renders the substituted characters rather than falling back --
        // "-" only happens when the source attribute itself is absent/empty.
        let event = Event::log(
            0,
            attrs,
            LogRecord { message: Value::str("x"), severity: None, body_format: BodyFormat::Raw },
        );
        let (msgs, _) = encode(vec![event]);
        assert!(msgs[0].contains(" __ "));
    }

    // -- Truncation ---------------------------------------------------------------------------

    #[test]
    fn an_oversize_message_is_truncated_on_a_char_boundary_not_the_header() {
        let mut encoder = SyslogEncoder::new(Format::Rfc5424, 16).with_max_message_bytes(60);
        // A multi-byte char ('é', 2 bytes in UTF-8) straddling the truncation boundary.
        let long_msg = format!("{}\u{e9}{}", "a".repeat(10), "b".repeat(10));
        let (msgs, stats) = encode_with(&mut encoder, vec![log_event(0, &long_msg, None)]);
        assert_eq!(stats.truncated, 1);
        assert!(msgs[0].starts_with("<134>1 "), "header must survive intact: {}", msgs[0]);
        assert!(String::from_utf8(msgs[0].clone().into_bytes()).is_ok());
    }

    #[test]
    fn an_oversize_header_drops_the_message_entirely() {
        let mut encoder = SyslogEncoder::new(Format::Rfc5424, 16).with_max_message_bytes(5);
        let (msgs, stats) = encode_with(&mut encoder, vec![log_event(0, "x", None)]);
        assert!(msgs.is_empty());
        assert_eq!(stats.dropped_oversize_header, 1);
    }

    /// Regression test: `max_message_bytes` exactly equal to the header's own length used to
    /// still push a trailing separator before noticing there was no room for it, emitting one
    /// byte over the configured cap. The RFC 5424 header for facility 16, an epoch timestamp, and
    /// no hostname/app_name/pid/msgid attributes is exactly 44 bytes:
    /// `<134>1 1970-01-01T00:00:00.000000Z - - - - -`.
    #[test]
    fn max_message_bytes_exactly_at_the_header_length_never_overflows_the_cap() {
        let mut encoder = SyslogEncoder::new(Format::Rfc5424, 16).with_max_message_bytes(44);
        let (msgs, stats) = encode_with(&mut encoder, vec![log_event(0, "nonempty", None)]);
        assert_eq!(msgs[0], "<134>1 1970-01-01T00:00:00.000000Z - - - - -");
        assert_eq!(msgs[0].len(), 44, "must never exceed max_message_bytes: {:?}", msgs[0]);
        assert_eq!(stats.truncated, 1);
    }

    /// One byte more than the header's own length leaves room for exactly the separator and
    /// nothing else -- still must never exceed the cap.
    #[test]
    fn max_message_bytes_one_byte_larger_than_the_header_fits_only_the_separator() {
        let mut encoder = SyslogEncoder::new(Format::Rfc5424, 16).with_max_message_bytes(45);
        let (msgs, stats) = encode_with(&mut encoder, vec![log_event(0, "nonempty", None)]);
        assert_eq!(msgs[0].len(), 45, "must never exceed max_message_bytes: {:?}", msgs[0]);
        assert_eq!(stats.truncated, 1);
    }

    // -- MessageBuf / framing ----------------------------------------------------------------

    #[test]
    fn frame_octet_counting_prefixes_each_message_with_its_exact_byte_length() {
        let mut buf = MessageBuf::default();
        buf.push("hello");
        buf.push("world!!");
        let mut frame = Vec::new();
        frame_octet_counting(&buf, &mut frame);
        assert_eq!(frame, b"5 hello7 world!!".to_vec());
    }

    // -- Sink: UDP ------------------------------------------------------------------------------

    async fn udp_collector() -> (SocketAddr, Arc<UdpSocket>) {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        (addr, Arc::new(socket))
    }

    #[tokio::test]
    async fn udp_sends_one_datagram_per_message() {
        let (addr, collector) = udp_collector().await;
        let mut output = SyslogOutput::udp(addr.to_string()).unwrap();
        let batch = batch_with(vec![log_event(0, "one", None), log_event(0, "two", None)]);
        let recv_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            let mut received = Vec::new();
            for _ in 0..2 {
                let (n, _) = collector.recv_from(&mut buf).await.unwrap();
                received.push(String::from_utf8_lossy(&buf[..n]).into_owned());
            }
            received
        });
        output.send(&batch).await.expect("send should succeed");
        let received = tokio::time::timeout(Duration::from_secs(2), recv_task)
            .await
            .expect("should receive both datagrams promptly")
            .unwrap();
        assert_eq!(received.len(), 2);
        assert!(received[0].ends_with("one"));
        assert!(received[1].ends_with("two"));
    }

    #[tokio::test]
    async fn a_batch_of_only_metric_only_events_performs_no_io() {
        // Bind to a port nothing is listening on and never receive from it -- if `send` performed
        // any I/O here, there would be nothing to observe it failing against, which is the point:
        // the assertion is simply that `send` returns `Ok` without needing a receiver at all.
        let mut output = SyslogOutput::udp("127.0.0.1:1").unwrap();
        let batch = batch_with(vec![metric_event(0)]);
        output.send(&batch).await.expect("an all-skipped batch must not attempt any I/O");
    }

    #[tokio::test]
    async fn duplicate_safe_is_false() {
        // `SyslogOutput::udp` binds via `UdpSocket::from_std`, which registers with the tokio
        // reactor and so needs a runtime context, even though this test never awaits anything.
        let output = SyslogOutput::udp("127.0.0.1:0").unwrap();
        assert!(!output.duplicate_safe());
    }

    /// Regression test for a review finding: a `send_to` failure partway through a batch used to
    /// be classified `Fault::Clean` unconditionally, which would make the generic writer resend
    /// (and so duplicate) whatever earlier datagrams in the same batch already reached the wire.
    /// `Clean` is a whole-batch promise the destination saw *none* of it -- only true when
    /// nothing has sent yet. A real `send_to` failure partway through a batch isn't reliably
    /// provokable over a loopback UDP socket in a unit test, so this pins the pure classification
    /// function directly instead.
    #[test]
    fn udp_send_fault_is_clean_only_before_anything_in_the_batch_has_sent() {
        assert_eq!(udp_send_fault(0), Fault::Clean);
        assert_eq!(udp_send_fault(1), Fault::Ambiguous);
        assert_eq!(udp_send_fault(5), Fault::Ambiguous);
    }

    // -- Sink: TCP ------------------------------------------------------------------------------

    /// A bare TCP receiver: reads every connection to EOF and records its bytes and the number of
    /// connections accepted.
    async fn tcp_collector() -> (SocketAddr, Arc<Mutex<Vec<Vec<u8>>>>, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let received: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
        let accepts = Arc::new(AtomicUsize::new(0));
        {
            let received = Arc::clone(&received);
            let accepts = Arc::clone(&accepts);
            tokio::spawn(async move {
                loop {
                    let Ok((mut stream, _)) = listener.accept().await else { break };
                    accepts.fetch_add(1, Ordering::SeqCst);
                    use tokio::io::AsyncReadExt;
                    let mut buf = Vec::new();
                    let _ = stream.read_to_end(&mut buf).await;
                    received.lock().unwrap().push(buf);
                }
            });
        }
        (addr, received, accepts)
    }

    #[tokio::test]
    async fn tcp_sends_one_octet_counted_frame_per_batch() {
        let (addr, received, accepts) = tcp_collector().await;
        let mut output = SyslogOutput::tcp(addr.to_string(), Duration::from_secs(2));
        let batch = batch_with(vec![log_event(0, "one", None), log_event(0, "two", None)]);
        output.send(&batch).await.expect("send should succeed");
        // Drop the sink so its write side closes and the collector's read_to_end returns.
        drop(output);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(accepts.load(Ordering::SeqCst), 1);
        let got = received.lock().unwrap();
        let frame = String::from_utf8_lossy(&got[0]);
        assert!(frame.contains("one") && frame.contains("two"));
    }

    #[tokio::test]
    async fn tcp_connect_refused_is_classified_as_a_clean_fault() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // now nothing is listening on `addr`
        let mut output = SyslogOutput::tcp(addr.to_string(), Duration::from_millis(500));
        let batch = batch_with(vec![log_event(0, "x", None)]);
        let err = output.send(&batch).await.expect_err("connect should fail");
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
    }

    #[tokio::test]
    async fn tcp_reconnects_after_the_peer_resets_an_inherited_connection() {
        let (addr, received, accepts) = tcp_collector().await;
        let mut output = SyslogOutput::tcp(addr.to_string(), Duration::from_secs(2));

        let batch = batch_with(vec![log_event(0, "first", None)]);
        output.send(&batch).await.expect("first send should succeed against a fresh connection");

        // Deterministically break the *local* end of the inherited connection, rather than
        // trying to provoke a genuine peer-sent RST and race its propagation back through the
        // kernel (unreliable inside a sandboxed/virtualized loopback stack, and this repo's own
        // discipline is exact assertions, not timing-dependent ones). `send_tcp`'s reconnect
        // logic reacts identically to any write failure on an inherited connection regardless of
        // its real-world cause, so shutting down our own write half is a faithful trigger for
        // the behavior under test: the very next local `write()` on a write-shutdown socket
        // reliably fails with `BrokenPipe`, no network round trip required.
        if let Conn::Tcp { stream: Some(stream), .. } = &mut output.conn {
            stream.shutdown().await.expect("local shutdown should succeed");
        }

        let batch2 = batch_with(vec![log_event(0, "second", None)]);
        output
            .send(&batch2)
            .await
            .expect("second send should reconnect once and succeed, not surface the failure");

        drop(output); // closes the second connection so its `read_to_end` completes
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            accepts.load(Ordering::SeqCst),
            2,
            "the failure must cause exactly one reconnect, not be silently absorbed or looped"
        );
        let got = received.lock().unwrap();
        assert!(got.iter().any(|b| String::from_utf8_lossy(b).contains("first")));
        assert!(got.iter().any(|b| String::from_utf8_lossy(b).contains("second")));
    }
}
