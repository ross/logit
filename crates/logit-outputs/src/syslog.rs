//! RFC 3164 / RFC 5424 syslog egress over UDP or TCP, the mirror of `logit_inputs::syslog`. A
//! relay: header fields round-trip from the `syslog.*` attributes `SyslogDecoder` writes, and
//! configured defaults apply only to an event that never passed through `syslog_in`. See
//! `docs/adr/syslog-output.md`.
//!
//! A pure [`SyslogEncoder`] (no socket; every format, precedence, and sanitization test runs
//! against it) plus the thin [`SyslogOutput`] that owns the socket, split as `influxdb.rs` and
//! `stdio.rs` are.
//!
//! **This implements [`logit_proto::FramedEncoder`], not `logit_proto::Encoder`.** The sink needs
//! per-message boundaries (one UDP datagram per message, one octet-counted frame per message on
//! TCP), which one opaque `Bytes` per batch can't carry. [`SyslogEncoder::encode_into`] fills one
//! [`MessageBuf`] entry per message and reports every skip or drop through [`EncodeStats`]
//! instead of failing (ADR `framed-encoder`). `statsd_out` is the other implementor.
//!
//! ## Timestamp semantics
//!
//! Per event, a `syslog.timestamp` attribute that renders without guessing wins; `event.timestamp`
//! (receipt time) is the fallback. By the decoder's shapes:
//!
//! - `Value::Timestamp` (5424's parsed RFC 3339 TIMESTAMP) renders directly on either output
//!   format, so an origin instant survives a `5424 -> 3164` or `5424 -> 5424` relay.
//! - `Value::Str` (3164's raw 15-byte token, no year or timezone) is written verbatim only on a
//!   3164 output, and only if it has that shape ([`is_rfc3164_timestamp_shape`]). A 5424 output
//!   has nowhere to put it, so it falls through rather than make the guess `syslog_in`'s module
//!   doc declines to make on the way in.
//! - `Value::Null` (5424's nil `-`) renders as `-` on a 5424 output. RFC 3164 has no NILVALUE
//!   TIMESTAMP, so a 3164 output falls through.
//! - An absent attribute, or any other variant, falls through.
//!
//! `event.timestamp` stays receipt time (`docs/adr/decoupled-listener-io.md`); this sink never
//! resolves `syslog.timestamp` onto it. The opt-in `syslog_timestamp` transform sketched in
//! `docs/known-gaps.md` is where that would happen, before an event reaches this sink.
//!
//! ## Header-field precedence
//!
//! Per event, per field, first hit wins: the `syslog.*` attribute, then the configured default,
//! then a format-appropriate absence (`-` for RFC 5424's NILVALUE, an omitted token for RFC 3164).
//! `syslog.severity` outranks `log.severity` because `syslog_in`'s `map_severity` is lossy: it
//! collapses syslog's eight severities onto five `Severity` variants (0-2 all become `Fatal`, 5 and
//! 6 both become `Info`). Preferring the raw attribute keeps a relay byte-faithful;
//! [`syslog_severity_of`] is the fallback for a log record that didn't come from `syslog_in`.
//!
//! **PROCID** (`syslog.pid`) is a `Value::U64`, or a `Value::Str` when the origin's PROCID wasn't
//! numeric; [`resolve_pid`] returns the [`Pid`] enum covering both. On a 5424 output a `Pid::Str`
//! is sanitized with [`sanitize_5424_field`] and capped at RFC 5424's 128-byte PROCID maximum. On
//! a 3164 output it renders as `tag[pid]` after [`sanitize_3164_token`], under the same 128-byte
//! cap for consistency (3164 defines none).
//!
//! ## STRUCTURED-DATA
//!
//! [`write_structured_data`] is the inverse of the decoder's `syslog.sd` shape: `Value::Map {
//! "<SD-ID>" -> Value::Map { "<PARAM-NAME>" -> Value::Str | Value::Array<Value::Str> } }` renders
//! as `[<SD-ID> <PARAM-NAME>="<value>" ...]` per element, concatenated with no separator.
//!
//! - **SD-ID and PARAM-NAME order is canonicalized by name** (sorted by name bytes, independent of
//!   interning history): a relay that saw `[b@2 ..][a@1 ..]` re-emits `[a@1 ..][b@2 ..]`. A
//!   repeated PARAM-NAME's occurrences, already grouped under one `Value::Array` by the decoder,
//!   emit one `PARAM-NAME="..."` per item in order, so a wire `a b a` interleaving isn't preserved
//!   (`docs/known-gaps.md`).
//! - PARAM-VALUEs are escaped by [`push_sd_escaped`]: `"` -> `\"`, `\` -> `\\`, `]` -> `\]`, plus
//!   every C0 control character and DEL (see "Injection safety").
//! - SD-ID and PARAM-NAME must be RFC 5424 section 6.3.2 `SD-NAME`s ([`is_valid_sd_name`]: 1-32
//!   `PRINTUSASCII` characters excluding `=`, SP, `]`, `"`). An invalid SD-ID skips the whole
//!   element, an invalid PARAM-NAME skips that param; both count in
//!   [`EncodeStats::dropped_invalid_sd`] and a throttled `invalid_structured_data` diagnostic.
//! - `syslog.sd` absent, not a `Value::Map`, or producing zero elements renders as `-`.
//! - A non-`Str`/`Array` PARAM-VALUE (number, bool, nested container) still renders, through
//!   [`render_sd_value`]; the `syslog.sd` contract constrains only the outer two `Map` layers.
//!
//! **Opt-in `structured_data`**: when [`SyslogEncoder::with_structured_data`] sets an SD-ID and the
//! output is 5424, every event attribute whose key doesn't start with `syslog.` is emitted as one
//! extra SD-ELEMENT under that SD-ID (PARAM-NAME = attribute key; same validation, skip, and count
//! rule; the element is omitted when no attribute qualifies). This closes the
//! `syslog_in -> json -> syslog_out` gap: an attribute a transform added still reaches the wire. A
//! relayed multi-valued statsd tag (`team: Value::Array[Str("a"), Str("b")]`) takes
//! [`write_sd_param`]'s `Array` arm and emits `team="a" team="b"`.
//!
//! - **No default private enterprise number ships.** `sd_id` must contain exactly one `@` (e.g.
//!   `myapp@12345`), checked in `with_structured_data`. RFC 5424's `32473` example PEN is
//!   documentation only; choosing a real one is the operator's decision.
//! - `syslog_in` decodes the element back into `syslog.sd` like any other; lifting it back into
//!   top-level attributes is a transform's job.
//! - **A duplicate SD-ID is refused, not emitted twice.** When `sd_id` already names a key of the
//!   event's own `syslog.sd`, the opt-in element is skipped ([`EncodeStats::dropped_invalid_sd`],
//!   a throttled `invalid_structured_data` diagnostic naming the collision). `syslog_in`'s
//!   `parse_structured_data` rejects a repeated SD-ID, so emitting both would make a
//!   `syslog_in -> syslog_out -> syslog_in` relay fail at the far end.
//!
//! **RFC 3164 output never emits STRUCTURED-DATA**, since 3164 has no such field: `syslog.sd` and
//! the opt-in element are dropped on a `5424 -> 3164` relay. That's a permitted normalization (a
//! sink-configured dialect change) under `docs/adr/lossless-transit.md`.
//!
//! ## Injection safety
//!
//! `syslog_in` and Grafana Alloy's `loki.source.syslog` UDP listener both split a datagram into
//! lines on `\n`, so an embedded newline in a relayed message forges a second, attacker-controlled
//! message at the receiver (its own PRI, hostname, and app name, i.e. a fabricated Loki stream).
//! [`sanitize_msg`] escapes `\n`/`\r`/NUL and every other C0 control character and DEL in the
//! rendered message on every transport, so the wire bytes don't depend on the transport. On TCP,
//! octet-counting ([`frame_octet_counting`]) is already newline-transparent, which makes this
//! defense in depth there.
//!
//! **STRUCTURED-DATA PARAM-VALUEs get the same control-character treatment** through
//! [`push_sd_escaped`], composed with RFC 5424 section 6.3.3's `"`/`\`/`]` escaping because a
//! PARAM-VALUE sits inside a quoted string. Like `sanitize_msg`, it's a one-way sanitizer
//! normalization (`docs/adr/lossless-transit.md`'s permitted list): the decoder reads the escaped
//! bytes back as the literal text `\n` (backslash, `n`), never a real newline.
//!
//! **A literal backslash is not escaped.** The demo's message body is a JSON document, where a
//! newline inside a string is already the two characters `\` `n`; escaping a backslash would
//! double every one of them and break Loki's `| json` parsing on every line. The cost: a message
//! that contained the literal two characters `\` `n` is indistinguishable on the wire from one
//! with a real newline (`docs/known-gaps.md`).
//!
//! RFC 5424's HOSTNAME/APP-NAME/PROCID/MSGID are `PRINTUSASCII` with length caps
//! ([`sanitize_5424_field`]). RFC 3164's HOSTNAME/TAG also forbid `:`/`[`/`]`
//! ([`sanitize_3164_token`]), matching `syslog_in`'s two-token header rule (a `:` in HOSTNAME
//! would make it read the token as TAG) and the "must not end in `:`" warning in
//! `demo/hello/app.py`. A non-`PRINTUSASCII` byte, raw space included, becomes `_`, so no header
//! field carries whitespace a receiver could read as a field boundary.
//!
//! ## Message body
//!
//! `log.message` is a `Value`. [`render_message`] renders `Value::Str` verbatim before
//! [`sanitize_msg`]'s pass: the demo's JSON body must reach Loki unmangled for `| json` to parse
//! it, so this doesn't reuse `stdio::render_value`, which quotes and escapes a string for a
//! terminal. `Value::Map`/`Value::Array` do reuse it as a container fallback, since
//! [`sanitize_msg`] still runs over the result. **`Value::Bytes` is never lossy-UTF-8-decoded**:
//! [`sanitize_msg_bytes`] applies the same escapes to the raw bytes, which are appended with
//! [`MessageBuf::push_bytes`], so a non-UTF-8 payload reaches the wire without
//! `char::REPLACEMENT_CHARACTER` substitutions.
//!
//! ## Sizing
//!
//! `max_message_bytes` bounds one whole encoded message (PRI + header + MSG). It defaults to 8192,
//! Grafana Alloy's `loki.source.syslog` `max_message_length` default (the demo stack's receiver),
//! rather than RFC 3164 §4.1's traditional 1024, which would truncate a JSON-bodied message on
//! every modern relay chain.
//!
//! - STRUCTURED-DATA counts as header: it's written into the line buffer before the header-length
//!   check, so an SD element that pushes the header over the limit drops the whole message
//!   ([`EncodeStats::dropped_oversize_header`]), as an oversize hostname would, rather than being
//!   truncated or omitted.
//! - An oversize header (otherwise reachable only with a tiny `max_message_bytes`) drops the
//!   message rather than emit a malformed one.
//! - An oversize MSG is truncated, not dropped, since a truncated line still has a correct header
//!   and a readable prefix: on a UTF-8 character boundary for `Value::Str`
//!   ([`truncate_on_char_boundary`]), on a byte boundary for `Value::Bytes` ([`truncate_bytes`]).
//!   Counted and throttle-warned either way.
//!
//! ## TLS
//!
//! `transport: tcp` optionally runs over TLS, RFC 5425 ([`SyslogOutput::with_tls`],
//! `docs/adr/syslog-tcp-ingress-and-tls.md`). A `tls:` block's presence turns it on and makes it
//! required, the `logit_out` precedent: `endpoint` is a bare `host:port` with no scheme to carry
//! the signal, so there's no plaintext fallback. DTLS is out of scope, so `tls:` under
//! `transport: udp` is an error here and at config time (`logit-pipeline::graph::resolve`'s rule
//! 44). Every connect after the first counts `logit.output.reconnects`, TLS or plaintext.
//!
//! TLS changes this sink's fault classification: a `tokio_rustls` write's `Ok` means the session
//! accepted the bytes, not that the kernel has them, and its `Err` never proves nothing left the
//! host. [`SyslogOutput::send_tcp`]'s doc comment has the per-transport invariants and their
//! consequences (on TLS, no internal retry and no `Fault::Clean` after an application write; on
//! both, a flush before any batch is called delivered).

use crate::stdio::render_value;
// Shared with `logit_out`, which dials the same bare `host:port`, optionally TLS-wrapped.
// `AsyncStream` lets `Conn::Tcp` hold either without `SyslogOutput` becoming generic; `host_only`
// derives the SNI name from an endpoint with no scheme.
use crate::tls::{host_only, poll_pending_close, AsyncStream, PendingClose};
use crate::Output;
use anyhow::Context;
use logit_core::time::format_rfc3339_utc;
use logit_core::{interner, AttrMap, Diagnostics, Event, EventBatch, Severity, Telemetry, Value};
use logit_pipeline::Fault;
use logit_proto::{FramedEncoder, MessageBuf};
use rustls_pki_types::ServerName;
use std::fmt::Write as _;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{lookup_host, TcpStream, UdpSocket};
use tokio_rustls::TlsConnector;

/// The shared `crate::tls` type, re-exported at this path as `crate::otlp` and `crate::logit` do.
pub use crate::tls::TlsClientSettings;

/// Grafana Alloy's `loki.source.syslog` `max_message_length` default; see the module doc's
/// "Sizing".
pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 8192;

/// TCP only: how long one connect attempt, reconnects included, may take.
///
/// `logit-config` can't reference this (the dependency runs the other way), so its
/// `default_syslog_connect_timeout` hardcodes the same 5 seconds. Change both together.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Which syslog dialect [`SyslogEncoder`] emits.
///
/// Its own enum rather than `logit_config::SyslogFormat` because `logit-outputs` never depends on
/// `logit-config` (`docs/design/pipeline-graph.md`'s "Crate layout");
/// `logit-cli::pipeline::build_spec` does the conversion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Rfc3164,
    Rfc5424,
}

/// Per-batch outcome counts from [`SyslogEncoder::encode_into`], which `SyslogOutput::send` turns
/// into `logit.output.*` telemetry.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EncodeStats {
    /// Events with no `log` record (metric-only or span-only): nothing to render as a message.
    pub skipped_no_log: usize,
    pub truncated: usize,
    pub dropped_oversize_header: usize,
    /// SD-ELEMENTs or SD-PARAMs skipped: an invalid `SD-NAME`, a non-map `syslog.sd` element, or
    /// an opt-in SD-ID collision (module doc's "STRUCTURED-DATA").
    pub dropped_invalid_sd: usize,
}

/// The validated `sd_id` behind [`SyslogEncoder::with_structured_data`].
#[derive(Debug, Clone, PartialEq, Eq)]
struct StructuredDataConfig {
    sd_id: String,
}

/// Encodes events as syslog messages. Pure: no socket, so every format test runs against it.
pub struct SyslogEncoder {
    format: Format,
    default_facility: u8,
    default_hostname: Option<String>,
    default_app_name: Option<String>,
    max_message_bytes: usize,
    diag: Diagnostics,
    /// The message being built for the current event. A field, not a local, so its capacity
    /// survives across `encode_into` calls as well as across events: a local regrows from empty
    /// on every call. Cleared, never reallocated, per event.
    line: String,
    /// `render_message`'s pre-sanitize rendering of `log.message`, reused like `line`.
    raw_msg: String,
    /// Shared by every header-field sanitize, `sanitize_msg`'s output, and one PARAM-VALUE's
    /// rendering. Safe because each use is copied into `line` and cleared before the next.
    scratch: String,
    /// [`sanitize_msg_bytes`]'s output for a `Value::Bytes` message: `scratch`'s byte twin.
    byte_scratch: Vec<u8>,
    /// Header, separator, and `byte_scratch` composed for a `Value::Bytes` message, reused like
    /// `line`.
    line_bytes: Vec<u8>,
    /// Set by [`SyslogEncoder::with_structured_data`].
    structured_data: Option<StructuredDataConfig>,
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
            byte_scratch: Vec::new(),
            line_bytes: Vec::new(),
            structured_data: None,
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

    /// Opts into the operator-configured STRUCTURED-DATA element (module doc's
    /// "STRUCTURED-DATA").
    ///
    /// Fails unless `sd_id` is an `SD-NAME` ([`is_valid_sd_name`]) with exactly one `@`, a
    /// PEN-qualified id such as `"myapp@12345"`; no default PEN ships. `logit-cli::pipeline`'s
    /// `SyslogOut` arm surfaces the error at config time.
    pub fn with_structured_data(mut self, sd_id: impl Into<String>) -> anyhow::Result<Self> {
        let sd_id = sd_id.into();
        if !is_valid_sd_name(&sd_id) {
            anyhow::bail!(
                "syslog_out structured_data.sd_id {sd_id:?} is not a valid SD-NAME (1-32 \
                 PRINTUSASCII characters excluding '=', SP, ']', '\"')"
            );
        }
        if sd_id.matches('@').count() != 1 {
            anyhow::bail!(
                "syslog_out structured_data.sd_id {sd_id:?} must contain exactly one '@' (a \
                 private-enterprise-number-qualified id, e.g. \"myapp@12345\"); no default PEN \
                 is shipped -- see the module doc's \"STRUCTURED-DATA\" section"
            );
        }
        self.structured_data = Some(StructuredDataConfig { sd_id });
        Ok(self)
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }
}

impl FramedEncoder for SyslogEncoder {
    /// None: the transport frames each message as-is (a datagram on UDP, an octet-counted frame
    /// on TCP).
    type Meta = ();
    type Stats = EncodeStats;

    /// One message per event carrying a `log` record, into `out` (cleared first). Never fails: a
    /// per-event problem is a skip or drop counted in the returned [`EncodeStats`].
    fn encode_into(&mut self, batch: &EventBatch, out: &mut MessageBuf) -> EncodeStats {
        out.clear();
        let mut stats = EncodeStats::default();
        for event in &batch.events {
            self.line.clear();
            self.encode_event(event, &mut stats, out);
        }
        stats
    }
}

impl SyslogEncoder {
    /// Encodes one event's header into `self.line` (cleared by the caller), then its message,
    /// pushed as raw bytes ([`MessageBuf::push_bytes`]) for `Value::Bytes` and as `self.line`
    /// ([`MessageBuf::push`]) otherwise. A skipped or dropped event pushes nothing.
    fn encode_event(&mut self, event: &Event, stats: &mut EncodeStats, out: &mut MessageBuf) {
        let Some(log) = &event.log else {
            stats.skipped_no_log += 1;
            return;
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
                attrs,
                event.timestamp,
                hostname,
                app_name,
                pid,
                msgid,
                self.structured_data.as_ref(),
                stats,
                &mut self.diag,
            ),
            Format::Rfc3164 => write_rfc3164_header(
                &mut self.line,
                &mut self.scratch,
                pri,
                attrs,
                event.timestamp,
                hostname,
                app_name,
                pid,
            ),
        }

        // An oversize header drops the message rather than emit a truncated field a receiver
        // would misparse. STRUCTURED-DATA is already in `self.line`, so it counts as header
        // (module doc's "Sizing").
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
            return;
        }

        // No RFC 5424 §6.4 BOM before MSG: Loki's `| json` stage (Go's `encoding/json`) doesn't
        // skip one, so every BOM-prefixed JSON line fails to parse. `docs/adr/syslog-output.md`.
        match &log.message {
            // Sanitized as bytes, never lossy-decoded (module doc's "Message body").
            Value::Bytes(raw) => {
                self.byte_scratch.clear();
                sanitize_msg_bytes(&mut self.byte_scratch, raw);
                self.push_message_bytes(stats, out);
            }
            other => {
                self.raw_msg.clear();
                render_message(&mut self.raw_msg, other);
                sanitize_msg(&mut self.scratch, &self.raw_msg);
                self.push_message_str(stats, out);
            }
        }
    }

    /// Finishes a non-`Bytes` message from the sanitized text in `self.scratch`, truncating on a
    /// UTF-8 character boundary. Always pushes: the header alone if the message is empty or
    /// there's no room left for it.
    fn push_message_str(&mut self, stats: &mut EncodeStats, out: &mut MessageBuf) {
        if !self.scratch.is_empty() {
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
        out.push(&self.line);
    }

    /// The `Value::Bytes` twin of [`SyslogEncoder::push_message_str`], from `self.byte_scratch`,
    /// truncating on a byte boundary. Composes header, separator, and bytes into
    /// `self.line_bytes`; pushes `self.line` alone when there's no message to append.
    fn push_message_bytes(&mut self, stats: &mut EncodeStats, out: &mut MessageBuf) {
        if !self.byte_scratch.is_empty() {
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
                if truncate_bytes(&mut self.byte_scratch, budget) {
                    stats.truncated += 1;
                    self.diag.warn_throttled(
                        "message_truncated",
                        format_args!(
                            "syslog_out: message exceeded max_message_bytes ({}); truncated",
                            self.max_message_bytes
                        ),
                    );
                }
                self.line_bytes.clear();
                self.line_bytes.extend_from_slice(self.line.as_bytes());
                self.line_bytes.extend_from_slice(&self.byte_scratch);
                out.push_bytes(&self.line_bytes);
                return;
            }
        }
        out.push(&self.line);
    }
}

/// `syslog.facility` if present and in range (`Value::U64(n)`, `n <= 23`), else `default`.
fn resolve_facility(attrs: &AttrMap, default: u8) -> u8 {
    match attrs.get("syslog.facility") {
        Some(Value::U64(n)) if *n <= 23 => *n as u8,
        _ => default,
    }
}

/// `syslog.severity` if present and in range (`Value::U64(n)`, `n <= 7`), outranking
/// `log.severity` (module doc's "Header-field precedence"). Else [`syslog_severity_of`], else `6`
/// (info).
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

/// The lossy inverse of `syslog_in::map_severity`, for an event with no `syslog.severity`.
/// `Fatal` maps to `2` (crit), not `0` (emerg): `emerg` means "system unusable", a claim `Fatal`
/// never makes. `Trace` has no syslog equivalent and maps to `7` (debug), as `Debug` does.
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

/// A non-empty string attribute, or `None`. Empty counts as absent, so a blank
/// `syslog.hostname` falls through to the configured default instead of an empty header field.
fn resolve_str<'a>(attrs: &'a AttrMap, key: &str) -> Option<&'a str> {
    attrs.get(key).and_then(Value::as_str).filter(|s| !s.is_empty())
}

/// `syslog.pid` in either shape the decoder leaves it in (module doc's "PROCID" note).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pid<'a> {
    U64(u64),
    Str(&'a str),
}

fn resolve_pid(attrs: &AttrMap) -> Option<Pid<'_>> {
    match attrs.get("syslog.pid") {
        Some(Value::U64(n)) => Some(Pid::U64(*n)),
        Some(Value::Str(_)) => resolve_str(attrs, "syslog.pid").map(Pid::Str),
        _ => None,
    }
}

/// `HEADER SP STRUCTURED-DATA`. The caller appends `[SP MSG]` only for a non-empty MSG, per the
/// grammar.
#[allow(clippy::too_many_arguments)]
fn write_rfc5424_header(
    out: &mut String,
    scratch: &mut String,
    pri: u8,
    attrs: &AttrMap,
    event_timestamp: i64,
    hostname: Option<&str>,
    app_name: Option<&str>,
    pid: Option<Pid<'_>>,
    msgid: Option<&str>,
    structured_data: Option<&StructuredDataConfig>,
    stats: &mut EncodeStats,
    diag: &mut Diagnostics,
) {
    let _ = write!(out, "<{pri}>1 ");
    write_5424_timestamp(out, attrs, event_timestamp);
    out.push(' ');
    push_5424_field(out, scratch, hostname, 255);
    out.push(' ');
    push_5424_field(out, scratch, app_name, 48);
    out.push(' ');
    match pid {
        Some(Pid::U64(p)) => {
            let _ = write!(out, "{p}");
        }
        Some(Pid::Str(s)) => push_5424_field(out, scratch, Some(s), 128),
        None => out.push('-'),
    }
    out.push(' ');
    push_5424_field(out, scratch, msgid, 32);
    out.push(' ');
    write_structured_data(out, scratch, attrs, structured_data, stats, diag);
}

/// Per-event RFC 5424 TIMESTAMP, per the module doc's "Timestamp semantics".
fn write_5424_timestamp(out: &mut String, attrs: &AttrMap, event_timestamp: i64) {
    match attrs.get("syslog.timestamp") {
        Some(Value::Timestamp(t)) => push_rfc5424_timestamp(out, *t),
        Some(Value::Null) => out.push('-'),
        _ => push_rfc5424_timestamp(out, event_timestamp),
    }
}

/// Appends `value` sanitized (through `scratch`), or `-` (NILVALUE) if it's absent or sanitizes
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

/// RFC 5424 section 6: HOSTNAME/APP-NAME/PROCID/MSGID are `PRINTUSASCII` (`%d33-126`) with a
/// per-field length cap. Every other character, space included, becomes `_` rather than being
/// dropped, so the output is pure ASCII and the byte-length cap is also a character cap. Writes
/// into `scratch`, cleared first.
fn sanitize_5424_field(scratch: &mut String, s: &str, max_len: usize) {
    scratch.clear();
    for c in s.chars() {
        if scratch.len() >= max_len {
            break;
        }
        scratch.push(if is_printusascii(c) { c } else { '_' });
    }
}

/// RFC 3164's `<PRI>TIMESTAMP HOSTNAME TAG[PID]:` header, UTC, with no trailing separator.
/// HOSTNAME and TAG are omitted when absent or empty after sanitization: 3164 has no NILVALUE.
#[allow(clippy::too_many_arguments)]
fn write_rfc3164_header(
    out: &mut String,
    scratch: &mut String,
    pri: u8,
    attrs: &AttrMap,
    event_timestamp: i64,
    hostname: Option<&str>,
    tag: Option<&str>,
    pid: Option<Pid<'_>>,
) {
    let _ = write!(out, "<{pri}>");
    write_3164_timestamp(out, attrs, event_timestamp);
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
                out.push('[');
                match p {
                    Pid::U64(n) => {
                        let _ = write!(out, "{n}");
                    }
                    Pid::Str(s) => {
                        sanitize_3164_token(scratch, s, 128);
                        out.push_str(scratch);
                    }
                }
                out.push(']');
            }
            out.push(':');
        }
    }
}

/// Per-event RFC 3164 TIMESTAMP, per the module doc's "Timestamp semantics": a `Value::Timestamp`
/// renders as `Mmm dd hh:mm:ss`, and `Value::Null` falls through (3164 has no NILVALUE).
fn write_3164_timestamp(out: &mut String, attrs: &AttrMap, event_timestamp: i64) {
    match attrs.get("syslog.timestamp") {
        Some(Value::Timestamp(t)) => push_rfc3164_timestamp(out, *t),
        Some(Value::Str(raw)) => {
            // `Value::Str` is constructed only from valid UTF-8 (see its own doc comment).
            let raw = std::str::from_utf8(raw).expect("Value::Str is always valid UTF-8");
            if is_rfc3164_timestamp_shape(raw) {
                out.push_str(raw);
            } else {
                push_rfc3164_timestamp(out, event_timestamp);
            }
        }
        _ => push_rfc3164_timestamp(out, event_timestamp),
    }
}

/// True if `s` is the 15-byte RFC 3164 `Mmm dd hh:mm:ss` shape, the only raw `syslog.timestamp`
/// [`write_3164_timestamp`] emits verbatim. Works on bytes so a non-ASCII string can't panic on a
/// `str` char-boundary slice.
fn is_rfc3164_timestamp_shape(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() != 15 {
        return false;
    }
    let month_ok = MONTH_ABBR.iter().any(|m| m.as_bytes() == &b[0..3]);
    let digit = |i: usize| b[i].is_ascii_digit();
    month_ok
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
        && digit(14)
}

/// [`sanitize_5424_field`], plus `:`/`[`/`]` become `_`: a `:` in HOSTNAME would make
/// `syslog_in`'s two-token header rule read it as TAG (module doc's "Injection safety").
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

/// RFC 5424 section 6.3.2's `SD-NAME`: 1 to 32 `PRINTUSASCII` characters excluding `=`, `]`, `"`
/// (SP is outside `PRINTUSASCII`). Checks SD-IDs, PARAM-NAMEs, and the opt-in `sd_id`. Byte-wise
/// is exact here: no non-ASCII byte is `PRINTUSASCII`.
fn is_valid_sd_name(s: &str) -> bool {
    (1..=32).contains(&s.len())
        && s.bytes().all(|b| {
            let c = b as char;
            is_printusascii(c) && !matches!(c, '=' | ']' | '"')
        })
}

/// Renders a PARAM-VALUE before [`push_sd_escaped`] escapes it: [`render_message`]'s SD analogue.
/// `Bytes`/`Map`/`Array` fall back to [`render_value`].
fn render_sd_value(out: &mut String, value: &Value) {
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
        Value::Str(s) => {
            // `Value::Str` is constructed only from valid UTF-8 (see its own doc comment).
            let text = std::str::from_utf8(s).expect("Value::Str is always valid UTF-8");
            out.push_str(text);
        }
        Value::Bytes(_) | Value::Map(_) | Value::Array(_) => render_value(out, value),
    }
}

/// Escapes a rendered PARAM-VALUE per RFC 5424 section 6.3.3 (`"` -> `\"`, `\` -> `\\`, `]` ->
/// `\]`, the inverse of the decoder's unescaping), plus every C0 control character and DEL as
/// [`sanitize_msg`]'s mnemonics (`\n`, `\r`, `\0`, `\xNN`) with the mnemonic's backslash itself
/// escaped. A newline becomes the three wire bytes `\`, `\`, `n`, which `parse_param_value`
/// decodes to the text `\n`, never a real newline (module doc's "Injection safety").
fn push_sd_escaped(out: &mut String, value: &str) {
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            ']' => out.push_str("\\]"),
            '\n' => out.push_str("\\\\n"),
            '\r' => out.push_str("\\\\r"),
            '\0' => out.push_str("\\\\0"),
            c if (c as u32) < 0x20 || c as u32 == 0x7f => {
                let _ = write!(out, "\\\\x{:02x}", c as u32);
            }
            c => out.push(c),
        }
    }
}

/// The RFC 5424 STRUCTURED-DATA field: every `syslog.sd` element, then the opt-in element if
/// configured, or `-` when neither produces anything (module doc's "STRUCTURED-DATA").
///
/// SD-IDs are sorted by name bytes ([`write_sd_element`] sorts PARAM-NAMEs): `AttrMap` iterates
/// in process-global intern order, so writing it through would make element order depend on
/// interning history.
#[allow(clippy::too_many_arguments)]
fn write_structured_data(
    out: &mut String,
    scratch: &mut String,
    attrs: &AttrMap,
    structured_data: Option<&StructuredDataConfig>,
    stats: &mut EncodeStats,
    diag: &mut Diagnostics,
) {
    let start = out.len();
    if let Some(Value::Map(sd)) = attrs.get("syslog.sd") {
        let mut elements: Vec<(&str, &Value)> =
            sd.iter().map(|(id_sym, id_value)| (interner::resolve(id_sym), id_value)).collect();
        elements.sort_by(|(a, _), (b, _)| a.as_bytes().cmp(b.as_bytes()));
        for (id, id_value) in elements {
            match id_value {
                Value::Map(params) => {
                    write_sd_element(
                        out,
                        scratch,
                        id,
                        params.iter().map(|(sym, v)| (interner::resolve(sym), v)),
                        stats,
                        diag,
                    );
                }
                _ => {
                    stats.dropped_invalid_sd += 1;
                    diag.warn_throttled(
                        "invalid_structured_data",
                        format_args!(
                            "syslog_out: dropping syslog.sd element {id:?}: expected a nested \
                             map of PARAM-NAME -> value"
                        ),
                    );
                }
            }
        }
    }
    if let Some(cfg) = structured_data {
        // Checked before the collision, so an event with only `syslog.*` attributes, which would
        // emit no opt-in element anyway, isn't counted as a drop.
        let has_extra_attrs =
            attrs.iter().any(|(sym, _)| !interner::resolve(sym).starts_with("syslog."));
        if has_extra_attrs {
            // `syslog_in` rejects a repeated SD-ID, so on a collision the origin's element wins
            // and the opt-in one is dropped rather than make the far end reject the line.
            let collides = matches!(
                attrs.get("syslog.sd"),
                Some(Value::Map(sd)) if sd.get(&cfg.sd_id).is_some()
            );
            if collides {
                stats.dropped_invalid_sd += 1;
                diag.warn_throttled(
                    "invalid_structured_data",
                    format_args!(
                        "syslog_out: dropping opt-in structured_data SD-ELEMENT {:?}: collides with \
                         an existing syslog.sd element of the same SD-ID",
                        cfg.sd_id
                    ),
                );
            } else {
                write_sd_element(
                    out,
                    scratch,
                    &cfg.sd_id,
                    attrs
                        .iter()
                        .filter(|(sym, _)| !interner::resolve(*sym).starts_with("syslog."))
                        .map(|(sym, v)| (interner::resolve(sym), v)),
                    stats,
                    diag,
                );
            }
        }
    }
    if out.len() == start {
        out.push('-');
    }
}

/// One SD-ELEMENT: `[SD-ID PARAM-NAME="value" ...]`. An invalid `sd_id` skips the whole element
/// and an invalid PARAM-NAME skips that param ([`write_sd_param`]), each counted in
/// [`EncodeStats::dropped_invalid_sd`] with a throttled `invalid_structured_data` diagnostic.
///
/// PARAM-NAMEs are sorted by name bytes. They're already unique (the decoder groups a repeated
/// one under a `Value::Array`), so sorting makes the element a function of its data; a wire
/// `a b a` interleaving re-emits as `a a b` (`docs/known-gaps.md`).
fn write_sd_element<'a>(
    out: &mut String,
    scratch: &mut String,
    sd_id: &str,
    params: impl Iterator<Item = (&'a str, &'a Value)>,
    stats: &mut EncodeStats,
    diag: &mut Diagnostics,
) {
    if !is_valid_sd_name(sd_id) {
        stats.dropped_invalid_sd += 1;
        diag.warn_throttled(
            "invalid_structured_data",
            format_args!("syslog_out: dropping SD-ELEMENT with invalid SD-ID {sd_id:?}"),
        );
        return;
    }
    let mut params: Vec<(&str, &Value)> = params.collect();
    params.sort_by(|(a, _), (b, _)| a.as_bytes().cmp(b.as_bytes()));
    out.push('[');
    out.push_str(sd_id);
    for (name, value) in params {
        write_sd_param(out, scratch, name, value, stats, diag);
    }
    out.push(']');
}

/// One SD-PARAM, or one per item of an `Array` value (the decoder's repeated-PARAM-NAME shape).
/// Skipped and counted, as in [`write_sd_element`], when `name` isn't a valid `SD-NAME`.
fn write_sd_param(
    out: &mut String,
    scratch: &mut String,
    name: &str,
    value: &Value,
    stats: &mut EncodeStats,
    diag: &mut Diagnostics,
) {
    if !is_valid_sd_name(name) {
        stats.dropped_invalid_sd += 1;
        diag.warn_throttled(
            "invalid_structured_data",
            format_args!("syslog_out: dropping SD-PARAM with invalid name {name:?}"),
        );
        return;
    }
    match value {
        Value::Array(items) => {
            for item in items {
                push_one_sd_param(out, scratch, name, item);
            }
        }
        other => push_one_sd_param(out, scratch, name, other),
    }
}

/// ` PARAM-NAME="escaped-value"` for one PARAM-VALUE.
fn push_one_sd_param(out: &mut String, scratch: &mut String, name: &str, value: &Value) {
    scratch.clear();
    render_sd_value(scratch, value);
    out.push(' ');
    out.push_str(name);
    out.push_str("=\"");
    push_sd_escaped(out, scratch);
    out.push('"');
}

/// RFC 3339 with microseconds (`2026-09-02T14:03:11.123456Z`): RFC 5424 section 6.2.3.1's
/// TIME-SECFRAC allows at most 6 digits, so this trims the last 3 of `format_rfc3339_utc`'s
/// always-9 fractional digits.
fn push_rfc5424_timestamp(out: &mut String, nanos: i64) {
    let full = format_rfc3339_utc(nanos);
    out.push_str(&full[..full.len() - 4]);
    out.push('Z');
}

const MONTH_ABBR: [&str; 12] =
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

/// `Mmm dd hh:mm:ss` (space-padded day), UTC, no year (RFC 3164's TIMESTAMP has none).
fn push_rfc3164_timestamp(out: &mut String, nanos: i64) {
    let (month, day, hour, minute, second) = civil_time_of(nanos);
    let _ = write!(
        out,
        "{} {day:2} {hour:02}:{minute:02}:{second:02}",
        MONTH_ABBR[(month as usize - 1).min(11)]
    );
}

/// UTC `(month, day, hour, minute, second)` for a Unix-nanosecond timestamp, by Howard Hinnant's
/// civil-from-days algorithm. Not shared with `logit_core::time`, which returns only a whole RFC
/// 3339 string.
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

/// Renders a log message's `Value` before [`sanitize_msg`]'s pass; `Str` is verbatim (module
/// doc's "Message body").
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
        // Unreached from `encode_event`, which sends a `Bytes` message through
        // `sanitize_msg_bytes` instead; lossy only for a direct caller.
        Value::Bytes(b) => out.push_str(&String::from_utf8_lossy(b)),
        Value::Str(s) => {
            // `Value::Str` is constructed only from valid UTF-8 (see its own doc comment).
            let text = std::str::from_utf8(s).expect("Value::Str is always valid UTF-8");
            out.push_str(text);
        }
        // `sanitize_msg` still runs over the result, so `render_value`'s quoting is redundant
        // here, not harmful.
        Value::Array(_) | Value::Map(_) => render_value(out, value),
    }
}

/// Escapes `\n`/`\r`/NUL, every other C0 control character, and DEL in a rendered message, and
/// leaves a literal backslash alone (module doc's "Injection safety"). Writes into `out`, cleared
/// first.
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

/// [`sanitize_msg`]'s escapes over raw bytes, for a `Value::Bytes` message. Writes into `out`,
/// cleared first.
fn sanitize_msg_bytes(out: &mut Vec<u8>, msg: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.clear();
    for &b in msg {
        match b {
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            0 => out.extend_from_slice(b"\\0"),
            b if b < 0x20 || b == 0x7f => {
                out.extend_from_slice(b"\\x");
                out.push(HEX[(b >> 4) as usize]);
                out.push(HEX[(b & 0xf) as usize]);
            }
            b => out.push(b),
        }
    }
}

/// Truncates `s` to at most `budget` bytes on a UTF-8 character boundary; `true` if it cut.
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

/// [`truncate_on_char_boundary`]'s byte twin: truncates to `budget` bytes; `true` if it cut.
fn truncate_bytes(buf: &mut Vec<u8>, budget: usize) -> bool {
    if buf.len() <= budget {
        return false;
    }
    buf.truncate(budget);
    true
}

/// RFC 6587 §3.4.1 octet-counting: `MSG-LEN SP SYSLOG-MSG` per message, concatenated.
/// Newline-transparent, so framing doesn't depend on [`sanitize_msg`] being bug-free. Alloy's
/// `go-syslog` receiver detects it from the leading digit with no configuration
/// (`docs/adr/syslog-output.md`).
fn frame_octet_counting(messages: &MessageBuf, out: &mut Vec<u8>) {
    out.clear();
    for msg in messages.iter() {
        out.extend_from_slice(msg.len().to_string().as_bytes());
        out.push(b' ');
        out.extend_from_slice(msg);
    }
}

/// The socket side of a `syslog_out` sink. `Udp` binds eagerly, so a bad local bind fails
/// startup; `Tcp` connects lazily in `send`, so a receiver that isn't up yet doesn't block
/// `logit` from starting.
enum Conn {
    Udp(UdpSocket),
    /// A `Box<dyn AsyncStream>` covers plaintext and TLS without making [`SyslogOutput`]
    /// generic, as `logit_out`'s `Conn` does. No DTLS arm: rule 44 rejects `tls:` under
    /// `transport: udp`.
    Tcp {
        stream: Option<Box<dyn AsyncStream>>,
        connect_timeout: Duration,
    },
}

/// `logit_pipeline::Output` for `syslog_out`, built by [`SyslogOutput::udp`] or
/// [`SyslogOutput::tcp`].
pub struct SyslogOutput {
    endpoint: String,
    conn: Conn,
    encoder: SyslogEncoder,
    messages: MessageBuf,
    /// TCP only: the batch's octet-counted frame, reused across `send` calls. Never shrinks, so
    /// an outlier batch pins its peak capacity, the trade `InfluxLineEncoder`'s buffers make
    /// (`docs/design/memory.md`).
    frame_buf: Vec<u8>,
    /// TCP only: `Some` exactly when a `tls:` block was configured (module doc's "TLS"). Built
    /// once by [`SyslogOutput::with_tls`], shared by every connect.
    tls: Option<Arc<rustls::ClientConfig>>,
    /// Whether this sink has connected before; only later connects count
    /// `logit.output.reconnects`.
    has_connected_once: bool,
    diag: Diagnostics,
    telemetry: Telemetry,
}

impl SyslogOutput {
    /// Binds an ephemeral local UDP socket now. `endpoint` is resolved per batch instead, so a DNS
    /// failure is a delivery-time `Fault::Clean`, not a startup error.
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
            tls: None,
            has_connected_once: false,
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
        }
    }

    pub fn with_encoder(mut self, encoder: SyslogEncoder) -> Self {
        self.encoder = encoder;
        self
    }

    /// Turns on RFC 5425 TLS for the TCP connection (`tls:` in config).
    ///
    /// Presence turns it on, so an empty `tls: {}` means TLS with the bundled Mozilla roots and no
    /// client certificate. Unlike `otlp_out`, there's no [`TlsClientSettings::is_empty`] early
    /// return: `otlp_out` has `https://` to select TLS, this sink's bare `host:port` has nothing.
    ///
    /// Errors on the UDP arm (no DTLS) so a caller that bypasses rule 44 can't end up with an
    /// unencrypted socket. Paths in `settings` resolve against `base_dir`, the config file's
    /// directory, and load here, since `graph::resolve` never touches the filesystem.
    pub fn with_tls(
        mut self,
        settings: &TlsClientSettings,
        base_dir: &Path,
    ) -> anyhow::Result<Self> {
        if matches!(self.conn, Conn::Udp(_)) {
            anyhow::bail!(
                "syslog_out: tls: requires transport: tcp -- DTLS (syslog over TLS over UDP) is \
                 out of scope"
            );
        }
        if settings.insecure_skip_verify {
            self.diag.warn(
                "tls.insecure_skip_verify is set -- the connection is encrypted, but this \
                 output will accept any certificate the peer presents, self-signed or otherwise",
            );
        }
        self.tls = Some(Arc::new(crate::tls::build_client_config(settings, base_dir)?));
        Ok(self)
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
        self.telemetry.count(
            "logit.output.structured_data.dropped",
            stats.dropped_invalid_sd as f64,
            &[("reason", "invalid_sd_name")],
        );
        if self.messages.is_empty() {
            // Every event was skipped or dropped: nothing to write.
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
                let mut dial = TcpDial {
                    endpoint: &self.endpoint,
                    connect_timeout: *connect_timeout,
                    tls: self.tls.as_ref(),
                    telemetry: &self.telemetry,
                    has_connected_once: &mut self.has_connected_once,
                };
                Self::send_tcp(stream, &mut dial, &self.messages, &mut self.frame_buf).await
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

    /// Belt and braces: `*stream` is only repopulated after a successful flush, and a cancelled
    /// attempt drops its connection ([`SyslogOutput::send_tcp`]), so there's normally nothing
    /// left here. Kept explicit because on TLS "flushed" is a property of the stream, not the
    /// socket: finished records sit in the rustls session until something drains them.
    async fn flush(&mut self) -> anyhow::Result<()> {
        if let Conn::Tcp { stream: Some(stream), .. } = &mut self.conn {
            stream.flush().await.context("flushing syslog_out TCP stream")?;
        }
        Ok(())
    }

    /// `false` on both transports: a redelivered message is a duplicated log line. `AtMostOnce`
    /// still retries a `Fault::Clean` (`docs/adr/buffered-sink-delivery.md`), which covers a
    /// restarting receiver with no duplicate risk.
    fn duplicate_safe(&self) -> bool {
        false
    }
}

impl SyslogOutput {
    /// One `send_to` per message, never packed into one datagram, which would depend on the
    /// receiver splitting on a delimiter (module doc's "Injection safety").
    ///
    /// - `EMSGSIZE`/`InvalidInput` (too large for the path MTU or send buffer) is a per-message
    ///   data condition: dropped and counted, never a `Fault`, which could trip
    ///   `docs/adr/buffered-sink-delivery.md`'s sustained-failure exit on a healthy sink.
    /// - Any other failure is `Fault::Clean` only before the first datagram of the batch has gone
    ///   out ([`udp_send_fault`]). After that it's `Fault::Ambiguous`: `Clean` promises the
    ///   destination saw none of the batch, and over-claiming it would resend, and so duplicate,
    ///   what already landed.
    ///
    /// `endpoint` resolves to one [`SocketAddr`] per batch. Passing the `&str` to `send_to` would
    /// re-resolve a hostname by DNS on every message; the tests all use IP literals, so they
    /// wouldn't notice. Per batch rather than cached still picks up a DNS change.
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

    /// One frame (every message octet-counted and concatenated) per **batch**, with at most one
    /// internal reconnect-and-retry. Built around two properties:
    ///
    /// - **Cancellation safety.** `deliver_with_retry` races every attempt against
    ///   `tokio::time::timeout` (`docs/adr/buffered-sink-delivery.md`), and
    ///   [`AsyncWriteExt::write_all`] isn't cancel-safe: a timeout mid-write leaves an unknown
    ///   number of bytes on the wire. So the connection is always `stream.take()`n into a local
    ///   before writing; a cancelled write drops (and closes) it, leaving `*stream` `None` for the
    ///   next `send` to reconnect, rather than resuming a connection whose framing is unknown.
    /// - **Never resend once any byte has gone out.** Each attempt's first write is one
    ///   [`AsyncWriteExt::write`], not `write_all`. `Ok(n)` with `n > 0` means delivery has
    ///   started: any later failure is `Fault::Ambiguous` and the frame is never resent. A failure
    ///   with nothing written allows one reconnect and a whole-frame retry, and `Fault::Clean` if
    ///   that fails too. A single `write_all` can't support this: it may complete several inner
    ///   writes, whole earlier messages included, before a later one fails.
    ///
    /// **What a write proves depends on the transport, and TLS proves less**
    /// (`docs/adr/syslog-tcp-ingress-and-tls.md`'s `syslog_out` section). Behind the boxed stream
    /// the two look the same, so the code asks [`TcpDial::is_tls`]:
    ///
    /// - **Plaintext.** One `write()` is one `write(2)`: `Ok(n)` means the kernel owns `n` bytes,
    ///   and `Err` means this call wrote nothing (tokio loops only on `WouldBlock`). Both halves
    ///   of the rule above hold as written.
    /// - **TLS.** `tokio_rustls`' `poll_write` copies plaintext into the session, then writes to
    ///   the socket until it returns `Pending`, and returns `Ok(n)` with finished records still
    ///   queued in userspace: `Ok` proves only that the session took the bytes, and `flush` makes
    ///   them the kernel's. A failing `poll_write` may already have completed socket writes
    ///   (rustls fragments at 16 KiB, and each record is a complete octet-counted message a
    ///   receiver keeps), so `Err` never proves zero bytes. So on TLS: no internal retry, no
    ///   resend once an application write has been attempted, every such failure is
    ///   `Fault::Ambiguous`, and `Fault::Clean` is left only for [`TcpDial::connect`] failures,
    ///   which precede every byte of the frame.
    ///
    /// **A reused connection is probed before the first write.** The receiver may have closed a
    /// pooled connection since the last `send` (a graceful shutdown, a far-end `idle_timeout:`,
    /// an stunnel hop cycling), and plaintext syslog has no ack to say so: the write lands in the
    /// local socket buffer, the batch is reported delivered, and the message is lost. So a
    /// connection taken from `*stream` (never a fresh one) gets one non-consuming `poll_read`
    /// first ([`crate::tls::poll_pending_close`] has why one poll and not a timed read); anything
    /// but "still open" drops it and dials fresh with nothing written. That's an ordinary
    /// reconnect, counted by [`TcpDial::connect`], and it doesn't use up the post-write-failure
    /// retry. `docs/adr/idle-connection-timeout.md`.
    ///
    /// **Success always `flush`es**, on both transports, before the connection goes back into
    /// `*stream` and this returns `Ok`. Otherwise a TLS batch could be committed off the sink queue
    /// with its records still in the rustls buffer, to be discarded with the stream by the next
    /// reconnect or cancelled attempt. A failed flush is `Fault::Ambiguous` (an earlier record may
    /// have landed) and the connection is dropped. On plaintext it's a no-op.
    ///
    /// [`AsyncWriteExt::write_all`]: tokio::io::AsyncWriteExt::write_all
    /// [`AsyncWriteExt::write`]: tokio::io::AsyncWriteExt::write
    async fn send_tcp(
        stream: &mut Option<Box<dyn AsyncStream>>,
        dial: &mut TcpDial<'_>,
        messages: &MessageBuf,
        frame_buf: &mut Vec<u8>,
    ) -> anyhow::Result<usize> {
        frame_octet_counting(messages, frame_buf);

        let mut retried_after_a_zero_byte_failure = false;
        loop {
            // Taken out of `*stream`, never written through it (cancellation safety, above).
            let mut conn: Box<dyn AsyncStream> = match stream.take() {
                // The reuse probe (doc comment above). A closed connection was never written
                // to, so replacing it doesn't consume `retried_after_a_zero_byte_failure`.
                Some(mut conn) => {
                    let mut probe = [0u8; 1];
                    let pending = poll_pending_close(&mut *conn, &mut probe).await;
                    match pending {
                        PendingClose::Open => conn,
                        _closed => {
                            drop(conn);
                            dial.connect().await?
                        }
                    }
                }
                None => dial.connect().await?,
            };

            // `Ok(0)` on a non-empty buffer means the stream isn't accepting writes; normalized
            // to an error so there's one "nothing written" case. Plaintext only: `tokio_rustls`
            // maps zero progress to `Pending`.
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
                    // Flush before calling the batch delivered (doc comment above).
                    let rest_result = match rest_result {
                        Ok(()) => conn.flush().await,
                        Err(err) => Err(err),
                    };
                    return match rest_result {
                        Ok(()) => {
                            *stream = Some(conn);
                            Ok(messages.len())
                        }
                        // Part of the frame may be at the peer, so resending could duplicate.
                        // `*stream` stays `None`: a partly written connection isn't reusable.
                        Err(err) => Err(anyhow::Error::new(err).context(Fault::Ambiguous)),
                    };
                }
                // Plaintext only: a failed `write(2)` wrote nothing, so reconnect and retry the
                // whole frame once. TLS gives no such proof and falls through to `Ambiguous`.
                Err(_) if !dial.is_tls() && !retried_after_a_zero_byte_failure => {
                    retried_after_a_zero_byte_failure = true;
                    continue;
                }
                Err(err) => {
                    let fault = if dial.is_tls() { Fault::Ambiguous } else { Fault::Clean };
                    return Err(anyhow::Error::new(err).context(fault));
                }
            }
        }
    }
}

/// What [`SyslogOutput::send_tcp`] needs to open a fresh connection, borrowed per `send` from the
/// sink's fields (one value rather than five more parameters).
struct TcpDial<'a> {
    endpoint: &'a str,
    connect_timeout: Duration,
    /// See [`SyslogOutput::tls`].
    tls: Option<&'a Arc<rustls::ClientConfig>>,
    telemetry: &'a Telemetry,
    has_connected_once: &'a mut bool,
}

impl TcpDial<'_> {
    /// Whether connections are TLS-wrapped, which decides what a write proves
    /// ([`SyslogOutput::send_tcp`]).
    fn is_tls(&self) -> bool {
        self.tls.is_some()
    }

    /// One fresh connection: TCP connect, then the RFC 5425 handshake when `tls` is set, as
    /// `logit_out`'s `connect_and_handshake` does. Each phase gets its own `connect_timeout`, so a
    /// TLS connect can take twice the configured value. Both are `Fault::Clean`: nothing of the
    /// batch has left the host yet.
    async fn connect(&mut self) -> anyhow::Result<Box<dyn AsyncStream>> {
        let tcp = tokio::time::timeout(self.connect_timeout, TcpStream::connect(self.endpoint))
            .await
            .context("connecting to syslog_out endpoint timed out")
            .and_then(|r| r.context("connecting to syslog_out endpoint"))
            .context(Fault::Clean)?;

        let conn: Box<dyn AsyncStream> = match self.tls {
            Some(cfg) => {
                let host = host_only(self.endpoint);
                let server_name = ServerName::try_from(host.to_string())
                    .map_err(|e| {
                        anyhow::anyhow!("syslog_out: invalid TLS server name {host:?}: {e}")
                    })
                    .context(Fault::Clean)?;
                let connector = TlsConnector::from(cfg.clone());
                let tls_stream =
                    tokio::time::timeout(self.connect_timeout, connector.connect(server_name, tcp))
                        .await
                        .context("TLS handshake with syslog_out endpoint timed out")
                        .and_then(|r| r.context("TLS handshake with syslog_out endpoint"))
                        .context(Fault::Clean)?;
                Box::new(tls_stream)
            }
            None => Box::new(tcp),
        };

        // Counted at connect, not after the write, so a reconnect whose write then fails still
        // counts.
        if *self.has_connected_once {
            self.telemetry.count("logit.output.reconnects", 1.0, &[]);
        } else {
            *self.has_connected_once = true;
        }
        Ok(conn)
    }
}

/// `Fault::Clean` only when nothing in the batch has left the host. Its own function because a
/// mid-batch `send_to` failure can't be provoked reliably over a real socket in a test.
fn udp_send_fault(sent: usize) -> Fault {
    if sent > 0 {
        Fault::Ambiguous
    } else {
        Fault::Clean
    }
}

/// `90` is `EMSGSIZE` on Linux only (macOS/BSD use `40`). `logit` ships only in Linux containers,
/// so a miss elsewhere is a dev-host false negative, and the `InvalidInput` fallback doesn't
/// cover other platforms either.
fn is_message_too_large(err: &std::io::Error) -> bool {
    matches!(err.raw_os_error(), Some(libc_emsgsize) if libc_emsgsize == 90 /* EMSGSIZE, Linux */)
        || err.kind() == std::io::ErrorKind::InvalidInput
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{BodyFormat, LogRecord, MetricKind, MetricRecord, Registry, Resource};
    use logit_inputs::syslog::SyslogDecoder;
    use logit_proto::Decoder;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;

    fn batch_with(events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
    }

    fn log_event(ts: i64, message: &str, severity: Option<Severity>) -> Event {
        Event::log(
            ts,
            AttrMap::new(),
            LogRecord {
                message: Value::str(message),
                severity,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        )
    }

    fn metric_event(ts: i64) -> Event {
        Event::metric(
            ts,
            AttrMap::new(),
            MetricRecord::new(logit_core::interner::intern("m"), MetricKind::counter(1.0)),
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

    /// [`encode_with`] returning raw bytes, for a non-UTF-8 `Value::Bytes` message.
    fn encode_with_bytes(
        encoder: &mut SyslogEncoder,
        events: Vec<Event>,
    ) -> (Vec<Vec<u8>>, EncodeStats) {
        let mut out = MessageBuf::default();
        let stats = encoder.encode_into(&batch_with(events), &mut out);
        let msgs = out.iter().map(|b| b.to_vec()).collect();
        (msgs, stats)
    }

    fn log_event_with_attrs(
        ts: i64,
        message: Value,
        severity: Option<Severity>,
        attrs: AttrMap,
    ) -> Event {
        Event::log(
            ts,
            attrs,
            LogRecord {
                message,
                severity,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        )
    }

    /// Builds a `syslog.sd`-shaped `Value::Map { "<SD-ID>" -> Value::Map { "<PARAM-NAME>" ->
    /// value } }` from a plain list, for tests that don't want to hand-build `AttrMap`s.
    fn sd_value(elements: Vec<(&str, Vec<(&str, Value)>)>) -> Value {
        let mut outer = AttrMap::new();
        for (id, params) in elements {
            let mut inner = AttrMap::new();
            for (name, value) in params {
                inner.insert(name, value);
            }
            outer.insert(id, Value::Map(Box::new(inner)));
        }
        Value::Map(Box::new(outer))
    }

    // -- Encoder: RFC 5424 --------------------------------------------------------------------

    #[test]
    fn rfc5424_encodes_a_full_message() {
        let (msgs, stats) = encode(vec![log_event(0, "hello world", Some(Severity::Info))]);
        assert_eq!(stats, EncodeStats::default());
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], "<134>1 1970-01-01T00:00:00.000000Z - - - - - hello world");
    }

    /// No RFC 5424 §6.4 BOM before MSG: Loki's `| json` can't parse a BOM-prefixed body
    /// (`docs/adr/syslog-output.md`).
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
            LogRecord {
                message: Value::str("hi"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
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
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
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
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
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
            LogRecord {
                message: Value::str("x"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
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
            LogRecord {
                message: Value::str("x"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
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
            LogRecord {
                message: Value::str("x"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
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
            LogRecord {
                message: Value::str("x"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        let (msgs, _) = encode(vec![event]);
        let app_name = msgs[0].split(' ').nth(3).unwrap();
        assert_eq!(app_name.len(), 48);
    }

    #[test]
    fn an_all_non_printable_hostname_becomes_nilvalue() {
        let mut attrs = AttrMap::new();
        attrs.insert("syslog.hostname", Value::str("\u{0001}\u{0002}"));
        // Despite the name: this sanitizes to the non-empty "__", which renders; "-" needs an
        // absent or empty attribute.
        let event = Event::log(
            0,
            attrs,
            LogRecord {
                message: Value::str("x"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
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

    /// A cap equal to the header's length (44 bytes here) emits no trailing separator past it.
    #[test]
    fn max_message_bytes_exactly_at_the_header_length_never_overflows_the_cap() {
        let mut encoder = SyslogEncoder::new(Format::Rfc5424, 16).with_max_message_bytes(44);
        let (msgs, stats) = encode_with(&mut encoder, vec![log_event(0, "nonempty", None)]);
        assert_eq!(msgs[0], "<134>1 1970-01-01T00:00:00.000000Z - - - - -");
        assert_eq!(msgs[0].len(), 44, "must never exceed max_message_bytes: {:?}", msgs[0]);
        assert_eq!(stats.truncated, 1);
    }

    /// A cap one byte past the header fits the separator and nothing else.
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
        // Nothing listens on this port: `Ok` means `send` needed no receiver.
        let mut output = SyslogOutput::udp("127.0.0.1:1").unwrap();
        let batch = batch_with(vec![metric_event(0)]);
        output.send(&batch).await.expect("an all-skipped batch must not attempt any I/O");
    }

    #[tokio::test]
    async fn duplicate_safe_is_false() {
        // A tokio test only because `UdpSocket::from_std` needs a runtime context.
        let output = SyslogOutput::udp("127.0.0.1:0").unwrap();
        assert!(!output.duplicate_safe());
    }

    /// A mid-batch `send_to` failure is `Ambiguous`, since earlier datagrams may have landed.
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
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_out", "syslog", "output");
        let mut output =
            SyslogOutput::tcp(addr.to_string(), Duration::from_secs(2)).with_telemetry(telemetry);

        let batch = batch_with(vec![log_event(0, "first", None)]);
        output.send(&batch).await.expect("first send should succeed against a fresh connection");

        assert_eq!(
            reconnects_in(registry.drain(0)),
            None,
            "the first connect must not be counted as a reconnect"
        );

        // Shut down the local write half rather than race a real peer RST: the next `write()`
        // fails with `BrokenPipe` deterministically, and `send_tcp` treats any write failure
        // on an inherited connection alike.
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
        assert_eq!(
            reconnects_in(registry.drain(0)),
            Some(1.0),
            "exactly one reconnect, counted (`logit.output.reconnects`, \
             docs/design/internal-telemetry.md)"
        );
        let got = received.lock().unwrap();
        assert!(got.iter().any(|b| String::from_utf8_lossy(b).contains("first")));
        assert!(got.iter().any(|b| String::from_utf8_lossy(b).contains("second")));
    }

    /// [`tcp_collector`], except each connection closes after one read, as an idle-timing-out or
    /// restarting receiver does. A clean FIN, which a plaintext sender can't detect from a write.
    async fn tcp_collector_that_closes_after_one_read(
    ) -> (SocketAddr, Arc<Mutex<Vec<Vec<u8>>>>, Arc<AtomicUsize>) {
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
                    let mut buf = vec![0u8; 8192];
                    if let Ok(n) = stream.read(&mut buf).await {
                        buf.truncate(n);
                        received.lock().unwrap().push(buf);
                    }
                    // `stream` drops here: one message read, then a clean close.
                }
            });
        }
        (addr, received, accepts)
    }

    /// The reuse probe (`SyslogOutput::send_tcp`): a message sent after the receiver closed the
    /// pooled connection still arrives. Asserted at the collector, since a write into a FIN'd
    /// socket succeeds locally and `send` would return `Ok` either way.
    #[tokio::test]
    async fn a_pooled_connection_the_peer_closed_is_reconnected_before_writing_and_the_message_is_not_lost(
    ) {
        let (addr, received, accepts) = tcp_collector_that_closes_after_one_read().await;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_out", "syslog", "output");
        let mut output =
            SyslogOutput::tcp(addr.to_string(), Duration::from_secs(2)).with_telemetry(telemetry);

        output
            .send(&batch_with(vec![log_event(0, "first", None)]))
            .await
            .expect("first send should succeed against a fresh connection");

        // Let the collector's FIN arrive before the probe looks for it.
        tokio::time::sleep(Duration::from_millis(100)).await;

        output
            .send(&batch_with(vec![log_event(0, "second", None)]))
            .await
            .expect("the probe should reconnect rather than write into a closed socket");

        drop(output);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            accepts.load(Ordering::SeqCst),
            2,
            "the probe must have dialled a second connection for the second message"
        );
        assert_eq!(
            reconnects_in(registry.drain(0)),
            Some(1.0),
            "the replacement is an ordinary reconnect, counted like any other"
        );
        let got = received.lock().unwrap();
        assert!(got.iter().any(|b| String::from_utf8_lossy(b).contains("first")));
        assert!(
            got.iter().any(|b| String::from_utf8_lossy(b).contains("second")),
            "the second message must actually have reached the receiver: {got:?}"
        );
    }

    /// `logit.output.reconnects` from a drained [`Registry`], or `None` if it was never counted.
    fn reconnects_in(events: Vec<Event>) -> Option<f64> {
        events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(sum) if interner::resolve(m.name) == "logit.output.reconnects" => {
                    Some(sum.value)
                }
                _ => None,
            })
        })
    }

    // -- Sink: TCP over TLS (RFC 5425, module doc's "TLS" section) -----------------------------

    fn testdata_dir() -> std::path::PathBuf {
        // The repo root's `testdata/tls` (`testdata/tls/README.md`), as in `otlp.rs`'s tests.
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    fn tls_settings(overrides: impl FnOnce(&mut TlsClientSettings)) -> TlsClientSettings {
        let mut settings = TlsClientSettings::default();
        overrides(&mut settings);
        settings
    }

    /// A `rustls::ServerConfig` presenting `testdata/tls/server.{pem,key}` (SANs `localhost` and
    /// `127.0.0.1`), optionally requiring a client certificate chaining to `testdata/tls/ca.pem`.
    /// `otlp.rs`'s `test_server_tls_config` minus ALPN, which RFC 5425 predates.
    fn server_tls_config(require_client_auth: bool) -> Arc<rustls::ServerConfig> {
        use rustls_pki_types::pem::PemObject;
        use rustls_pki_types::{CertificateDer, PrivateKeyDer};

        let dir = testdata_dir();
        let chain: Vec<CertificateDer<'static>> =
            CertificateDer::pem_file_iter(dir.join("server.pem"))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
        let key = PrivateKeyDer::from_pem_file(dir.join("server.key")).unwrap();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap();
        let cfg = if require_client_auth {
            let mut roots = rustls::RootCertStore::empty();
            let ca: Vec<CertificateDer<'static>> =
                CertificateDer::pem_file_iter(dir.join("ca.pem"))
                    .unwrap()
                    .collect::<Result<_, _>>()
                    .unwrap();
            roots.add_parsable_certificates(ca);
            let verifier =
                rustls::server::WebPkiClientVerifier::builder(Arc::new(roots)).build().unwrap();
            builder.with_client_cert_verifier(verifier).with_single_cert(chain, key).unwrap()
        } else {
            builder.with_no_client_auth().with_single_cert(chain, key).unwrap()
        };
        Arc::new(cfg)
    }

    /// [`tcp_collector`]'s TLS twin: records each handshaken connection's plaintext to EOF. The
    /// third return counts completed handshakes, not accepts, so a rejected client never counts.
    /// One task per connection, so a failed handshake can't stall the accept loop.
    async fn tls_tcp_collector(
        require_client_auth: bool,
    ) -> (SocketAddr, Arc<Mutex<Vec<Vec<u8>>>>, Arc<AtomicUsize>) {
        let acceptor = tokio_rustls::TlsAcceptor::from(server_tls_config(require_client_auth));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let received: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
        let handshakes = Arc::new(AtomicUsize::new(0));
        {
            let received = Arc::clone(&received);
            let handshakes = Arc::clone(&handshakes);
            tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else { break };
                    let acceptor = acceptor.clone();
                    let received = Arc::clone(&received);
                    let handshakes = Arc::clone(&handshakes);
                    tokio::spawn(async move {
                        let Ok(mut tls_stream) = acceptor.accept(stream).await else { return };
                        handshakes.fetch_add(1, Ordering::SeqCst);
                        use tokio::io::AsyncReadExt;
                        let mut buf = Vec::new();
                        let _ = tls_stream.read_to_end(&mut buf).await;
                        received.lock().unwrap().push(buf);
                    });
                }
            });
        }
        (addr, received, handshakes)
    }

    /// The bytes a plaintext `syslog_out` delivers for `batch`: the exact reference the TLS
    /// tests compare against.
    async fn plaintext_frame_for(batch: &EventBatch) -> Vec<u8> {
        let (addr, received, _accepts) = tcp_collector().await;
        let mut output = SyslogOutput::tcp(addr.to_string(), Duration::from_secs(2));
        output.send(batch).await.expect("plaintext send should succeed");
        drop(output);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let got = received.lock().unwrap();
        got[0].clone()
    }

    #[tokio::test]
    async fn tls_tcp_sends_the_same_octet_counted_frame_as_plaintext() {
        let batch = batch_with(vec![log_event(0, "one", None), log_event(0, "two", None)]);
        let expected = plaintext_frame_for(&batch).await;

        let (addr, received, handshakes) = tls_tcp_collector(false).await;
        // `localhost`, not `127.0.0.1` (both SANs), so `host_only` yields a DNS SNI name.
        let endpoint = format!("localhost:{}", addr.port());
        let mut output = SyslogOutput::tcp(endpoint, Duration::from_secs(2))
            .with_tls(&tls_settings(|t| t.ca_file = Some("ca.pem".to_string())), &testdata_dir())
            .expect("a tls: block on the TCP transport is legal");
        output.send(&batch).await.expect("send over TLS should succeed");
        drop(output);
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(handshakes.load(Ordering::SeqCst), 1);
        let got = received.lock().unwrap();
        assert_eq!(
            got[0], expected,
            "TLS must deliver byte-for-byte the same octet-counted frame plaintext does"
        );
    }

    #[tokio::test]
    async fn tls_tcp_with_a_client_certificate_satisfies_a_client_ca_requiring_collector() {
        let (addr, received, handshakes) = tls_tcp_collector(true).await;
        let mut output =
            SyslogOutput::tcp(format!("localhost:{}", addr.port()), Duration::from_secs(2))
                .with_tls(
                    &tls_settings(|t| {
                        t.ca_file = Some("ca.pem".to_string());
                        t.cert_file = Some("client.pem".to_string());
                        t.key_file = Some("client.key".to_string());
                    }),
                    &testdata_dir(),
                )
                .expect("a client certificate is legal on the TCP transport");
        let batch = batch_with(vec![log_event(0, "mutual", None)]);
        output.send(&batch).await.expect("mutual TLS should succeed");
        drop(output);
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(handshakes.load(Ordering::SeqCst), 1);
        let got = received.lock().unwrap();
        assert!(String::from_utf8_lossy(&got[0]).contains("mutual"));
    }

    /// A sink with no client certificate delivers nothing to a mutual-TLS collector.
    ///
    /// Asserted at the collector, not on `send`: under TLS 1.3 the client's `connect` completes
    /// before the server rejects its certificate, the rejection is an alert this write-only sink
    /// never reads, and whether the next `write()` fails depends on RST timing.
    #[tokio::test]
    async fn tls_tcp_without_a_client_certificate_delivers_nothing_to_a_client_ca_requiring_collector(
    ) {
        let (addr, received, handshakes) = tls_tcp_collector(true).await;
        let mut output =
            SyslogOutput::tcp(format!("localhost:{}", addr.port()), Duration::from_secs(2))
                .with_tls(
                    &tls_settings(|t| t.ca_file = Some("ca.pem".to_string())),
                    &testdata_dir(),
                )
                .expect("a tls: block on the TCP transport is legal");
        let batch = batch_with(vec![log_event(0, "rejected", None)]);
        // Usually `Ok`, but a fast RST can fail the write; either way never `Permanent`.
        if let Err(err) = output.send(&batch).await {
            assert!(
                matches!(logit_pipeline::classify(&err), Fault::Clean | Fault::Ambiguous),
                "a rejected-handshake write is never Permanent: {err:?}"
            );
        }
        drop(output);
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert_eq!(
            handshakes.load(Ordering::SeqCst),
            0,
            "a client with no certificate must not complete the handshake"
        );
        assert!(
            received.lock().unwrap().is_empty(),
            "nothing may reach a mutual-TLS collector from an unauthenticated client"
        );
    }

    /// An untrusted server certificate fails the handshake before any batch byte leaves the host,
    /// so it's `Fault::Clean`.
    #[tokio::test]
    async fn tls_tcp_against_a_server_certificate_from_an_untrusted_ca_is_a_clean_fault() {
        let (addr, received, handshakes) = tls_tcp_collector(false).await;
        let mut output =
            SyslogOutput::tcp(format!("localhost:{}", addr.port()), Duration::from_secs(2))
                .with_tls(
                    &tls_settings(|t| t.ca_file = Some("other-ca.pem".to_string())),
                    &testdata_dir(),
                )
                .expect("a tls: block on the TCP transport is legal");
        let batch = batch_with(vec![log_event(0, "untrusted", None)]);
        let err = output.send(&batch).await.expect_err("an untrusted CA must fail the handshake");
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(handshakes.load(Ordering::SeqCst), 0);
        assert!(received.lock().unwrap().is_empty());
    }

    /// A `tracing` writer capturing rendered events, for asserting on a `Diagnostics::warn`,
    /// which has no telemetry counterpart.
    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
        type Writer = CapturedLogs;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[tokio::test]
    async fn tls_tcp_insecure_skip_verify_connects_to_an_untrusted_server_and_warns() {
        use tracing_subscriber::util::SubscriberInitExt as _;

        let logs = CapturedLogs::default();
        let guard = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_ansi(false)
            .finish()
            .set_default();

        let (addr, received, handshakes) = tls_tcp_collector(false).await;
        // The bundled Mozilla roots never signed `server.pem`, so only the skip lets this connect.
        let mut output =
            SyslogOutput::tcp(format!("localhost:{}", addr.port()), Duration::from_secs(2))
                .with_diagnostics(Diagnostics::new("syslog_out"))
                .with_tls(&tls_settings(|t| t.insecure_skip_verify = true), &testdata_dir())
                .expect("insecure_skip_verify is legal, if loud");
        let batch = batch_with(vec![log_event(0, "insecure", None)]);
        output.send(&batch).await.expect("insecure_skip_verify should bypass CA trust");
        drop(output);
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(guard);

        assert_eq!(handshakes.load(Ordering::SeqCst), 1);
        assert!(String::from_utf8_lossy(&received.lock().unwrap()[0]).contains("insecure"));
        let logged = String::from_utf8_lossy(&logs.0.lock().unwrap()).into_owned();
        assert!(
            logged.contains("tls.insecure_skip_verify is set"),
            "the warning must actually be emitted: {logged}"
        );
    }

    /// A plaintext sink delivers nothing to a TLS collector. `send` usually returns `Ok` (the
    /// write lands in the socket buffer before the server gives up), so only a retryable class is
    /// asserted if it fails.
    #[tokio::test]
    async fn a_plaintext_sink_against_a_tls_collector_delivers_nothing() {
        let (addr, received, handshakes) = tls_tcp_collector(false).await;
        let mut output = SyslogOutput::tcp(addr.to_string(), Duration::from_secs(2));
        let batch = batch_with(vec![log_event(0, "cleartext", None)]);
        let result = output.send(&batch).await;
        if let Err(err) = &result {
            assert!(
                matches!(logit_pipeline::classify(err), Fault::Clean | Fault::Ambiguous),
                "a failed cleartext write is never Permanent: {err:?}"
            );
        }
        drop(output);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(handshakes.load(Ordering::SeqCst), 0);
        assert!(
            received.lock().unwrap().is_empty(),
            "a TLS listener must never surface cleartext bytes as a message"
        );
    }

    // -- Sink: TCP over TLS, write/flush semantics --------------------------------------------
    //
    // "The session took the frame but the socket took only part of it" needs a backpressured
    // socket of known buffer size, so these drive `send_tcp` against a scripted
    // [`FakeTlsStream`]. The real-TLS tests around them cover the socket level.

    /// An [`AsyncStream`] with `tokio_rustls`' backpressured write semantics: `write` buffers and
    /// reports success, and only `flush` puts bytes on the notional wire. Failures are scripted
    /// per call, so each of `send_tcp`'s arms can be reached.
    #[derive(Clone, Default)]
    struct FakeTlsStream(Arc<Mutex<FakeState>>);

    #[derive(Default)]
    struct FakeState {
        /// Accepted by `write`, not yet flushed: `tokio_rustls`' `sendable_tls`.
        buffered: Vec<u8>,
        /// What `flush` has put on the wire.
        sent: Vec<u8>,
        writes: usize,
        flushes: usize,
        /// `write` fails on this 1-based call number.
        fail_write_on: Option<usize>,
        /// The first `write` accepts one byte, so `send_tcp` takes its `write_all` path.
        short_first_write: bool,
        fail_flush: bool,
    }

    impl FakeTlsStream {
        fn state(&self) -> std::sync::MutexGuard<'_, FakeState> {
            self.0.lock().unwrap()
        }

        fn failing_write(call: usize) -> Self {
            let fake = Self::default();
            fake.state().fail_write_on = Some(call);
            fake
        }

        fn failing_flush() -> Self {
            let fake = Self::default();
            fake.state().fail_flush = true;
            fake
        }

        /// One byte accepted, then the `write_all` of the remainder fails.
        fn short_then_failing_write() -> Self {
            let fake = Self::default();
            let mut state = fake.state();
            state.short_first_write = true;
            state.fail_write_on = Some(2);
            drop(state);
            fake
        }
    }

    impl tokio::io::AsyncWrite for FakeTlsStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            let mut state = self.state();
            state.writes += 1;
            if state.fail_write_on == Some(state.writes) {
                return std::task::Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "scripted write failure",
                )));
            }
            let accepted = if state.short_first_write && state.writes == 1 {
                buf.len().min(1)
            } else {
                buf.len()
            };
            state.buffered.extend_from_slice(&buf[..accepted]);
            std::task::Poll::Ready(Ok(accepted))
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let mut state = self.state();
            state.flushes += 1;
            if state.fail_flush {
                return std::task::Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "scripted flush failure",
                )));
            }
            let buffered = std::mem::take(&mut state.buffered);
            state.sent.extend_from_slice(&buffered);
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    impl tokio::io::AsyncRead for FakeTlsStream {
        /// `Pending`, as a live, quiet stream is, so the reuse probe
        /// (`crate::tls::poll_pending_close`) keeps it; an empty `Ok(())` would read as EOF. No
        /// waker is registered, so anything that awaited a read here would hang loudly.
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Pending
        }
    }

    /// A default client config, so a [`TcpDial`] reports `is_tls()`; no handshake ever happens.
    fn any_client_config() -> Arc<rustls::ClientConfig> {
        Arc::new(
            crate::tls::build_client_config(&TlsClientSettings::default(), &testdata_dir())
                .expect("the default settings always build"),
        )
    }

    fn one_message_frame() -> (MessageBuf, Vec<u8>) {
        let mut messages = MessageBuf::default();
        messages.push("hello");
        (messages, Vec::new())
    }

    #[tokio::test]
    async fn a_tls_batch_is_reported_delivered_only_once_the_stream_has_been_flushed() {
        let fake = FakeTlsStream::default();
        let mut stream: Option<Box<dyn AsyncStream>> = Some(Box::new(fake.clone()));
        let cfg = any_client_config();
        let telemetry = Telemetry::default();
        let mut connected = true;
        let mut dial = TcpDial {
            endpoint: "127.0.0.1:1",
            connect_timeout: Duration::from_secs(1),
            tls: Some(&cfg),
            telemetry: &telemetry,
            has_connected_once: &mut connected,
        };
        let (messages, mut frame_buf) = one_message_frame();

        let sent = SyslogOutput::send_tcp(&mut stream, &mut dial, &messages, &mut frame_buf)
            .await
            .expect("the write and the flush both succeed");

        assert_eq!(sent, 1);
        let state = fake.state();
        assert_eq!(state.flushes, 1, "the success path must flush exactly once");
        assert!(state.buffered.is_empty(), "nothing may be left in the session buffer");
        assert_eq!(state.sent, b"5 hello".to_vec(), "the whole frame must be on the wire");
        drop(state);
        assert!(stream.is_some(), "a flushed connection is reusable");
    }

    #[tokio::test]
    async fn a_tls_flush_failure_is_ambiguous_and_discards_the_connection() {
        let fake = FakeTlsStream::failing_flush();
        let mut stream: Option<Box<dyn AsyncStream>> = Some(Box::new(fake.clone()));
        let cfg = any_client_config();
        let telemetry = Telemetry::default();
        let mut connected = true;
        let mut dial = TcpDial {
            endpoint: "127.0.0.1:1",
            connect_timeout: Duration::from_secs(1),
            tls: Some(&cfg),
            telemetry: &telemetry,
            has_connected_once: &mut connected,
        };
        let (messages, mut frame_buf) = one_message_frame();

        let err = SyslogOutput::send_tcp(&mut stream, &mut dial, &messages, &mut frame_buf)
            .await
            .expect_err("a failed flush must fail the send");

        // The session may have pushed some records to the socket before the flush failed.
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
        assert!(stream.is_none(), "a stream whose flush failed must not be reused");
        assert_eq!(fake.state().writes, 1, "and must not be rewritten either");
    }

    /// A TLS write error may follow landed records, so it's `Ambiguous` and never resent (the
    /// plaintext counterpart is below).
    #[tokio::test]
    async fn a_tls_write_failure_is_ambiguous_and_never_resent() {
        let fake = FakeTlsStream::failing_write(1);
        let mut stream: Option<Box<dyn AsyncStream>> = Some(Box::new(fake.clone()));
        let cfg = any_client_config();
        let telemetry = Telemetry::default();
        let mut connected = true;
        let mut dial = TcpDial {
            // Nothing listens here: a wrongful retry would surface as a `Clean` connect failure.
            endpoint: "127.0.0.1:1",
            connect_timeout: Duration::from_millis(200),
            tls: Some(&cfg),
            telemetry: &telemetry,
            has_connected_once: &mut connected,
        };
        let (messages, mut frame_buf) = one_message_frame();

        let err = SyslogOutput::send_tcp(&mut stream, &mut dial, &messages, &mut frame_buf)
            .await
            .expect_err("a failed write must fail the send");

        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
        assert!(stream.is_none());
        let state = fake.state();
        assert_eq!(state.writes, 1, "exactly one write attempt -- no resend");
        assert!(state.sent.is_empty());
    }

    /// A failure after a partial first write is `Ambiguous` and never resent.
    #[tokio::test]
    async fn a_failure_after_a_partial_write_is_ambiguous_and_never_resent() {
        let fake = FakeTlsStream::short_then_failing_write();
        let mut stream: Option<Box<dyn AsyncStream>> = Some(Box::new(fake.clone()));
        let cfg = any_client_config();
        let telemetry = Telemetry::default();
        let mut connected = true;
        let mut dial = TcpDial {
            endpoint: "127.0.0.1:1",
            connect_timeout: Duration::from_millis(200),
            tls: Some(&cfg),
            telemetry: &telemetry,
            has_connected_once: &mut connected,
        };
        let (messages, mut frame_buf) = one_message_frame();

        let err = SyslogOutput::send_tcp(&mut stream, &mut dial, &messages, &mut frame_buf)
            .await
            .expect_err("a failed write_all must fail the send");

        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
        assert!(stream.is_none());
        let state = fake.state();
        assert_eq!(state.writes, 2, "the short write, then the failing remainder -- no resend");
        assert_eq!(state.flushes, 0, "a failed write never reaches the flush");
        assert!(state.sent.is_empty());
    }

    /// On plaintext a failed first write wrote nothing, so `send_tcp` reconnects once and reports
    /// `Fault::Clean` when that fails too.
    #[tokio::test]
    async fn a_plaintext_write_failure_still_reconnects_once_and_stays_clean() {
        let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_addr = dead.local_addr().unwrap().to_string();
        drop(dead); // now nothing is listening there

        let fake = FakeTlsStream::failing_write(1);
        let mut stream: Option<Box<dyn AsyncStream>> = Some(Box::new(fake.clone()));
        let telemetry = Telemetry::default();
        let mut connected = true;
        let mut dial = TcpDial {
            endpoint: &dead_addr,
            connect_timeout: Duration::from_millis(500),
            tls: None,
            telemetry: &telemetry,
            has_connected_once: &mut connected,
        };
        let (messages, mut frame_buf) = one_message_frame();

        let err = SyslogOutput::send_tcp(&mut stream, &mut dial, &messages, &mut frame_buf)
            .await
            .expect_err("the retry's connect is refused");

        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
        assert_eq!(fake.state().writes, 1);
        assert!(
            format!("{err:#}").contains("connecting to syslog_out endpoint"),
            "the failure must come from the retry's fresh connect, proving one happened: {err:#}"
        );
    }

    /// Once a TLS `send` returns, the receiver can read the whole frame while the sink is still
    /// alive (dropping it would flush on the way out).
    #[tokio::test]
    async fn after_a_tls_send_returns_the_whole_frame_is_readable_without_dropping_the_sink() {
        let batch = batch_with(vec![log_event(0, "one", None), log_event(0, "two", None)]);
        let expected = plaintext_frame_for(&batch).await;

        let acceptor = tokio_rustls::TlsAcceptor::from(server_tls_config(false));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let want = expected.len();
        let collector = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut tls_stream = acceptor.accept(stream).await.unwrap();
            use tokio::io::AsyncReadExt;
            let mut got = vec![0u8; want];
            // `want` bytes with no EOF: returns only if the frame is on the wire already.
            tls_stream.read_exact(&mut got).await.unwrap();
            got
        });

        let mut output =
            SyslogOutput::tcp(format!("localhost:{}", addr.port()), Duration::from_secs(2))
                .with_tls(
                    &tls_settings(|t| t.ca_file = Some("ca.pem".to_string())),
                    &testdata_dir(),
                )
                .expect("a tls: block on the TCP transport is legal");
        output.send(&batch).await.expect("send over TLS should succeed");

        let got = tokio::time::timeout(Duration::from_secs(2), collector)
            .await
            .expect("the frame must already be readable once send has returned")
            .unwrap();
        assert_eq!(got, expected);
        drop(output);
    }

    /// After a TLS write failure the frame never reaches the wire a second time. The collector
    /// keeps accepting, so a resend on a fresh connection would be recorded.
    #[tokio::test]
    async fn a_tls_frame_is_never_resent_after_the_peer_goes_away() {
        let (addr, received, _handshakes) = tls_tcp_collector(false).await;
        let mut output =
            SyslogOutput::tcp(format!("localhost:{}", addr.port()), Duration::from_secs(2))
                .with_tls(
                    &tls_settings(|t| t.ca_file = Some("ca.pem".to_string())),
                    &testdata_dir(),
                )
                .expect("a tls: block on the TCP transport is legal");

        let batch = batch_with(vec![log_event(0, "once", None)]);
        output.send(&batch).await.expect("the first send should succeed");
        // Fails the next write deterministically, as in
        // `tcp_reconnects_after_the_peer_resets_an_inherited_connection`.
        if let Conn::Tcp { stream: Some(stream), .. } = &mut output.conn {
            let _ = stream.shutdown().await;
        }
        if let Err(err) = output.send(&batch).await {
            assert_eq!(
                logit_pipeline::classify(&err),
                Fault::Ambiguous,
                "a TLS write failure is never Clean -- it can't prove nothing landed"
            );
        }

        drop(output);
        tokio::time::sleep(Duration::from_millis(200)).await;
        let got = received.lock().unwrap();
        // Occurrences across all connections: a resend could land on either one.
        let deliveries: usize =
            got.iter().map(|b| String::from_utf8_lossy(b).matches("once").count()).sum();
        assert_eq!(deliveries, 1, "the frame must reach the receiver exactly once: {got:?}");
    }

    /// Rule 44's check, repeated at construction: `tls:` on the UDP arm is an error.
    #[tokio::test]
    async fn with_tls_on_the_udp_transport_is_an_error() {
        let output = SyslogOutput::udp("127.0.0.1:514").unwrap();
        // `.err()` rather than `expect_err`, which would need `SyslogOutput: Debug`.
        let err = output
            .with_tls(&TlsClientSettings::default(), &testdata_dir())
            .err()
            .expect("DTLS is out of scope");
        assert!(err.to_string().contains("transport: tcp"), "got: {err}");
    }

    // -- Timestamp precedence (module doc's "Timestamp semantics") --------------------------

    #[test]
    fn a_resolved_timestamp_attribute_renders_directly_on_5424() {
        let mut attrs = AttrMap::new();
        // 2026-09-02T14:03:11Z, distinct from the event's own timestamp (0).
        attrs.insert("syslog.timestamp", Value::Timestamp(1_788_357_791_000_000_000));
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let (msgs, _) = encode(vec![event]);
        assert!(
            msgs[0].starts_with("<134>1 2026-09-02T14:03:11"),
            "expected the syslog.timestamp attribute, not event.timestamp (epoch): {}",
            msgs[0]
        );
    }

    #[test]
    fn a_resolved_timestamp_attribute_renders_in_3164_shape_on_3164() {
        let mut attrs = AttrMap::new();
        attrs.insert("syslog.timestamp", Value::Timestamp(1_788_357_791_000_000_000));
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let mut encoder = SyslogEncoder::new(Format::Rfc3164, 16);
        let (msgs, _) = encode_with(&mut encoder, vec![event]);
        assert!(
            msgs[0].contains("Sep  2 14:03:11"),
            "expected syslog.timestamp rendered in 3164 shape, not receipt time: {}",
            msgs[0]
        );
    }

    #[test]
    fn a_raw_3164_timestamp_token_is_written_verbatim_when_output_is_also_3164() {
        let mut attrs = AttrMap::new();
        attrs.insert("syslog.timestamp", Value::str("Mar 15 02:03:04"));
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let mut encoder = SyslogEncoder::new(Format::Rfc3164, 16);
        let (msgs, _) = encode_with(&mut encoder, vec![event]);
        assert!(
            msgs[0].starts_with("<134>Mar 15 02:03:04"),
            "expected the raw token verbatim: {}",
            msgs[0]
        );
    }

    #[test]
    fn a_raw_3164_timestamp_token_falls_through_to_event_timestamp_when_output_is_5424() {
        let mut attrs = AttrMap::new();
        attrs.insert("syslog.timestamp", Value::str("Mar 15 02:03:04"));
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let (msgs, _) = encode(vec![event]);
        assert!(
            msgs[0].starts_with("<134>1 1970-01-01T00:00:00"),
            "5424 has no year/timezone to build from a raw 3164 token -- must fall through to \
             event.timestamp: {}",
            msgs[0]
        );
    }

    #[test]
    fn a_malformed_timestamp_token_falls_through_to_event_timestamp_on_3164() {
        let mut attrs = AttrMap::new();
        // Not the 15-byte `Mmm dd hh:mm:ss` shape.
        attrs.insert("syslog.timestamp", Value::str("not-a-timestamp"));
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let mut encoder = SyslogEncoder::new(Format::Rfc3164, 16);
        let (msgs, _) = encode_with(&mut encoder, vec![event]);
        assert!(
            msgs[0].starts_with("<134>Jan  1 00:00:00"),
            "a malformed token must fall through to receipt time, not be written verbatim: {}",
            msgs[0]
        );
    }

    #[test]
    fn a_nil_timestamp_renders_the_nilvalue_on_5424() {
        let mut attrs = AttrMap::new();
        attrs.insert("syslog.timestamp", Value::Null);
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let (msgs, _) = encode(vec![event]);
        assert!(msgs[0].starts_with("<134>1 - "), "expected NILVALUE `-`: {}", msgs[0]);
    }

    #[test]
    fn a_nil_timestamp_falls_through_to_event_timestamp_on_3164() {
        let mut attrs = AttrMap::new();
        attrs.insert("syslog.timestamp", Value::Null);
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let mut encoder = SyslogEncoder::new(Format::Rfc3164, 16);
        let (msgs, _) = encode_with(&mut encoder, vec![event]);
        assert!(
            msgs[0].starts_with("<134>Jan  1 00:00:00"),
            "RFC 3164 has no NILVALUE concept for TIMESTAMP: {}",
            msgs[0]
        );
    }

    #[test]
    fn an_absent_timestamp_attribute_uses_event_timestamp_on_5424() {
        let (msgs, _) = encode(vec![log_event(0, "x", None)]);
        assert!(msgs[0].starts_with("<134>1 1970-01-01T00:00:00"));
    }

    #[test]
    fn an_absent_timestamp_attribute_uses_event_timestamp_on_3164() {
        let mut encoder = SyslogEncoder::new(Format::Rfc3164, 16);
        let (msgs, _) = encode_with(&mut encoder, vec![log_event(0, "x", None)]);
        assert!(msgs[0].starts_with("<134>Jan  1 00:00:00"));
    }

    // -- PROCID (module doc's "PROCID" note) --------------------------------------------------

    #[test]
    fn a_non_numeric_pid_is_sanitized_and_rendered_on_5424() {
        let mut attrs = AttrMap::new();
        attrs.insert("syslog.pid", Value::str("worker-1"));
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let (msgs, _) = encode(vec![event]);
        let pid_field = msgs[0].split(' ').nth(4).unwrap();
        assert_eq!(pid_field, "worker-1");
    }

    #[test]
    fn a_non_numeric_pid_renders_as_tag_pid_on_3164() {
        let mut attrs = AttrMap::new();
        attrs.insert("syslog.hostname", Value::str("h"));
        attrs.insert("syslog.tag", Value::str("app"));
        attrs.insert("syslog.pid", Value::str("worker-1"));
        let event = log_event_with_attrs(0, Value::str("hi"), None, attrs);
        let mut encoder = SyslogEncoder::new(Format::Rfc3164, 16);
        let (msgs, _) = encode_with(&mut encoder, vec![event]);
        assert_eq!(msgs[0], "<134>Jan  1 00:00:00 h app[worker-1]: hi");
    }

    // -- STRUCTURED-DATA (module doc's "STRUCTURED-DATA" section) ----------------------------

    #[test]
    fn a_single_sd_element_with_one_param_renders() {
        let mut attrs = AttrMap::new();
        attrs.insert(
            "syslog.sd",
            sd_value(vec![("exampleSDID@32473", vec![("iut", Value::str("3"))])]),
        );
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let (msgs, _) = encode(vec![event]);
        assert!(msgs[0].contains(r#"[exampleSDID@32473 iut="3"]"#), "got: {}", msgs[0]);
    }

    #[test]
    fn two_sd_elements_concatenate_with_no_separator() {
        let mut attrs = AttrMap::new();
        attrs.insert(
            "syslog.sd",
            sd_value(vec![
                ("a@1", vec![("k", Value::str("v"))]),
                ("b@2", vec![("k2", Value::str("v2"))]),
            ]),
        );
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let (msgs, _) = encode(vec![event]);
        assert!(
            msgs[0].contains(r#"[a@1 k="v"][b@2 k2="v2"]"#),
            "elements must be concatenated with no space between them: {}",
            msgs[0]
        );
    }

    #[test]
    fn sd_param_value_escapes_quote_backslash_and_close_bracket() {
        let mut attrs = AttrMap::new();
        attrs.insert(
            "syslog.sd",
            sd_value(vec![(
                "a@1",
                vec![("k", Value::str(r#"has "quote", \back, and ] bracket"#))],
            )]),
        );
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let (msgs, _) = encode(vec![event]);
        assert!(
            msgs[0].contains(r#"k="has \"quote\", \\back, and \] bracket""#),
            "got: {}",
            msgs[0]
        );
    }

    #[test]
    fn an_array_param_emits_one_param_per_element_in_order() {
        let mut attrs = AttrMap::new();
        attrs.insert(
            "syslog.sd",
            sd_value(vec![(
                "a@1",
                vec![("tag", Value::Array(vec![Value::str("one"), Value::str("two")]))],
            )]),
        );
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let (msgs, _) = encode(vec![event]);
        assert!(msgs[0].contains(r#"[a@1 tag="one" tag="two"]"#), "got: {}", msgs[0]);
    }

    #[test]
    fn an_invalid_sd_id_skips_the_whole_element_and_is_counted() {
        let mut attrs = AttrMap::new();
        // Contains a space, which PRINTUSASCII (and so SD-NAME) forbids.
        attrs.insert("syslog.sd", sd_value(vec![("bad id", vec![("k", Value::str("v"))])]));
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let (msgs, stats) = encode(vec![event]);
        assert!(!msgs[0].contains("bad id"), "invalid SD-ID must not reach the wire: {}", msgs[0]);
        assert_eq!(stats.dropped_invalid_sd, 1);
        // No valid element survived -> NILVALUE, as in the absent case.
        assert!(msgs[0].ends_with("- - x"), "got: {}", msgs[0]);
    }

    #[test]
    fn an_invalid_param_name_skips_only_that_param_and_is_counted() {
        let mut attrs = AttrMap::new();
        attrs.insert(
            "syslog.sd",
            sd_value(vec![("a@1", vec![("bad name", Value::str("x")), ("good", Value::str("y"))])]),
        );
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let (msgs, stats) = encode(vec![event]);
        assert!(msgs[0].contains(r#"[a@1 good="y"]"#), "got: {}", msgs[0]);
        assert!(!msgs[0].contains("bad name"));
        assert_eq!(stats.dropped_invalid_sd, 1);
    }

    #[test]
    fn absent_syslog_sd_renders_the_nilvalue() {
        let (msgs, _) = encode(vec![log_event(0, "x", None)]);
        // MSGID field then STRUCTURED-DATA: "... - - x" (msgid nil, sd nil, then message).
        assert!(msgs[0].ends_with("- - x"), "got: {}", msgs[0]);
    }

    #[test]
    fn rfc3164_output_never_emits_structured_data() {
        let mut attrs = AttrMap::new();
        attrs.insert("syslog.sd", sd_value(vec![("a@1", vec![("k", Value::str("v"))])]));
        let event = log_event_with_attrs(0, Value::str("hi"), None, attrs);
        let mut encoder = SyslogEncoder::new(Format::Rfc3164, 16);
        let (msgs, _) = encode_with(&mut encoder, vec![event]);
        assert!(!msgs[0].contains("a@1"), "3164 has no STRUCTURED-DATA field: {}", msgs[0]);
        assert!(!msgs[0].contains('['));
    }

    // -- STRUCTURED-DATA: injection safety (module doc's "Injection safety" section) --------

    #[test]
    fn an_embedded_newline_in_an_sd_value_cannot_forge_a_second_message() {
        let mut attrs = AttrMap::new();
        attrs.insert(
            "syslog.sd",
            sd_value(vec![(
                "a@1",
                vec![("k", Value::str("line one\n<0>Jan 1 00:00:00 evil: forged"))],
            )]),
        );
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let (msgs, _) = encode(vec![event]);
        assert_eq!(msgs.len(), 1, "one event must always encode to exactly one message");
        assert!(!msgs[0].as_bytes().contains(&b'\n'), "no raw newline byte may appear on the wire");
        assert!(
            msgs[0].contains(r#"k="line one\\n<0>Jan 1 00:00:00 evil: forged""#),
            "got: {}",
            msgs[0]
        );
    }

    #[test]
    fn an_embedded_newline_in_an_opt_in_attribute_cannot_forge_a_second_message() {
        let mut attrs = AttrMap::new();
        attrs.insert("evil", Value::str("line one\n<0>Jan 1 00:00:00 evil: forged"));
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let mut encoder =
            SyslogEncoder::new(Format::Rfc5424, 16).with_structured_data("myapp@12345").unwrap();
        let (msgs, _) = encode_with(&mut encoder, vec![event]);
        assert_eq!(msgs.len(), 1, "one event must always encode to exactly one message");
        assert!(!msgs[0].as_bytes().contains(&b'\n'), "no raw newline byte may appear on the wire");
        assert!(
            msgs[0].contains(r#"evil="line one\\n<0>Jan 1 00:00:00 evil: forged""#),
            "got: {}",
            msgs[0]
        );
    }

    #[test]
    fn sd_value_carriage_return_nul_and_esc_are_escaped() {
        let mut attrs = AttrMap::new();
        attrs.insert("syslog.sd", sd_value(vec![("a@1", vec![("k", Value::str("a\rb\0c\x1bd"))])]));
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let (msgs, _) = encode(vec![event]);
        assert!(msgs[0].contains(r#"k="a\\rb\\0c\\x1bd""#), "got: {}", msgs[0]);
        assert!(!msgs[0].as_bytes().contains(&b'\r'));
        assert!(!msgs[0].as_bytes().contains(&0u8));
        assert!(!msgs[0].as_bytes().contains(&0x1bu8));
    }

    /// An escaped SD newline decodes to the text `\n` (backslash, `n`), never a real newline.
    #[test]
    fn an_escaped_sd_control_char_round_trips_to_the_literal_text_form() {
        let mut attrs = AttrMap::new();
        attrs.insert("syslog.sd", sd_value(vec![("a@1", vec![("k", Value::str("line\ntwo"))])]));
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let (msgs, _) = encode(vec![event]);

        let mut decoder = SyslogDecoder::new(Arc::new(Resource::default()));
        let batch = decoder
            .decode(bytes::Bytes::from(format!("{}\n", msgs[0])))
            .expect("the escaped output must decode");
        assert_eq!(batch.events.len(), 1);
        let sd = match batch.events[0].attributes.get("syslog.sd") {
            Some(Value::Map(sd)) => sd,
            other => panic!("expected syslog.sd to be a map, got {other:?}"),
        };
        let elem = match sd.get("a@1") {
            Some(Value::Map(p)) => p,
            other => panic!("expected element a@1 to be a map, got {other:?}"),
        };
        assert_eq!(
            elem.get("k").and_then(Value::as_str),
            Some("line\\ntwo"),
            "decoder must yield the literal text `\\n`, not a real newline"
        );
    }

    // -- SD-ELEMENT/PARAM order canonicalization (module doc's "STRUCTURED-DATA" section) ----

    /// PARAM-NAMEs sort by name, not intern order: `zz` is interned first so an uncanonicalized
    /// encoder would emit it first.
    #[test]
    fn sd_param_order_is_canonicalized_independent_of_intern_order() {
        interner::intern("zz");
        let mut attrs = AttrMap::new();
        attrs.insert(
            "syslog.sd",
            sd_value(vec![("x@1", vec![("zz", Value::str("1")), ("aa", Value::str("2"))])]),
        );
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let (msgs, _) = encode(vec![event]);
        let aa_pos = msgs[0].find("aa=").expect("aa param should be present");
        let zz_pos = msgs[0].find("zz=").expect("zz param should be present");
        assert!(aa_pos < zz_pos, "aa must precede zz regardless of intern order: {}", msgs[0]);
    }

    // -- Permitted normalization: bare-backslash canonicalization (RFC 5424 section 6.3.3) ---

    /// A backslash before a non-escape byte is literal (RFC 5424 section 6.3.3), so `p="a\xb"`
    /// relays as the equivalent canonical `p="a\\xb"`.
    #[test]
    fn a_bare_backslash_param_value_is_re_emitted_in_canonical_escaped_form() {
        let mut decoder = SyslogDecoder::new(Arc::new(Resource::default()));
        let line = "<134>1 - - - - - [a@1 p=\"a\\xb\"] msg\n";
        let batch = decoder.decode(bytes::Bytes::from(line)).expect("should decode");
        let mut encoder = SyslogEncoder::new(Format::Rfc5424, 0);
        let mut out = MessageBuf::default();
        encoder.encode_into(&batch, &mut out);
        let msg = String::from_utf8_lossy(out.iter().next().unwrap()).into_owned();
        assert!(
            msg.contains(r#"p="a\\xb""#),
            "a non-escape backslash must round-trip to the canonical escaped form: {msg}"
        );
    }

    // -- Opt-in `structured_data` (module doc's "Opt-in `structured_data`" note) -------------

    #[test]
    fn structured_data_emits_non_syslog_attributes_as_one_sd_element() {
        let mut attrs = AttrMap::new();
        attrs.insert("env", Value::str("prod"));
        attrs.insert("retries", Value::U64(3));
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let mut encoder =
            SyslogEncoder::new(Format::Rfc5424, 16).with_structured_data("myapp@12345").unwrap();
        let (msgs, _) = encode_with(&mut encoder, vec![event]);
        assert!(msgs[0].contains(r#"env="prod""#), "got: {}", msgs[0]);
        assert!(msgs[0].contains(r#"retries="3""#), "got: {}", msgs[0]);
        assert!(msgs[0].contains("myapp@12345"));
    }

    /// A multi-valued attribute emits a repeated PARAM-NAME per item, in array order.
    #[test]
    fn structured_data_emits_repeated_param_name_for_a_multi_valued_array_attribute() {
        let mut attrs = AttrMap::new();
        attrs.insert("team", Value::Array(vec![Value::str("a"), Value::str("b")]));
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let mut encoder =
            SyslogEncoder::new(Format::Rfc5424, 16).with_structured_data("myapp@12345").unwrap();
        let (msgs, _) = encode_with(&mut encoder, vec![event]);
        assert!(
            msgs[0].contains(r#"team="a" team="b""#),
            "expected two repeated PARAM-NAMEs in array order, got: {}",
            msgs[0]
        );
    }

    #[test]
    fn structured_data_excludes_syslog_prefixed_keys() {
        let mut attrs = AttrMap::new();
        attrs.insert("syslog.hostname", Value::str("h"));
        attrs.insert("env", Value::str("prod"));
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let mut encoder =
            SyslogEncoder::new(Format::Rfc5424, 16).with_structured_data("myapp@12345").unwrap();
        let (msgs, _) = encode_with(&mut encoder, vec![event]);
        assert!(msgs[0].contains(r#"env="prod""#));
        assert!(!msgs[0].contains("syslog.hostname"));
    }

    #[test]
    fn structured_data_skips_invalid_attribute_keys_and_counts_them() {
        // A key over 32 PRINTUSASCII characters is not a valid SD-NAME.
        let long_key = "a".repeat(33);
        let mut attrs = AttrMap::new();
        attrs.insert(&long_key, Value::str("x"));
        attrs.insert("ok", Value::str("y"));
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let mut encoder =
            SyslogEncoder::new(Format::Rfc5424, 16).with_structured_data("myapp@12345").unwrap();
        let (msgs, stats) = encode_with(&mut encoder, vec![event]);
        assert!(msgs[0].contains(r#"ok="y""#));
        assert!(!msgs[0].contains(&long_key));
        assert_eq!(stats.dropped_invalid_sd, 1);
    }

    #[test]
    fn structured_data_emits_no_element_when_no_attribute_qualifies() {
        let event = log_event(0, "x", None);
        let mut encoder =
            SyslogEncoder::new(Format::Rfc5424, 16).with_structured_data("myapp@12345").unwrap();
        let (msgs, _) = encode_with(&mut encoder, vec![event]);
        assert!(msgs[0].ends_with("- - x"), "no attributes qualify -> NILVALUE: {}", msgs[0]);
    }

    /// On an SD-ID collision the origin's element wins and the opt-in one is dropped and counted,
    /// since `syslog_in` rejects a repeated SD-ID.
    #[test]
    fn structured_data_skips_the_opt_in_element_when_its_sd_id_collides_with_an_existing_one() {
        let mut attrs = AttrMap::new();
        attrs.insert("syslog.sd", sd_value(vec![("myapp@12345", vec![("orig", Value::str("1"))])]));
        attrs.insert("env", Value::str("prod"));
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let mut encoder =
            SyslogEncoder::new(Format::Rfc5424, 16).with_structured_data("myapp@12345").unwrap();
        let (msgs, stats) = encode_with(&mut encoder, vec![event]);
        assert!(msgs[0].contains(r#"[myapp@12345 orig="1"]"#), "got: {}", msgs[0]);
        assert!(!msgs[0].contains(r#"env="prod""#), "opt-in element must be dropped: {}", msgs[0]);
        assert_eq!(
            msgs[0].matches("myapp@12345").count(),
            1,
            "the SD-ID must appear exactly once, not twice: {}",
            msgs[0]
        );
        assert_eq!(stats.dropped_invalid_sd, 1);
    }

    /// A collision isn't counted or warned for an event with only `syslog.*` attributes, which
    /// would emit no opt-in element anyway.
    #[test]
    fn collision_is_not_counted_when_no_extra_attributes_would_be_emitted() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_out", "syslog", "output");
        let diag = Diagnostics::new("syslog_out").with_telemetry(telemetry);
        let mut attrs = AttrMap::new();
        attrs.insert("syslog.sd", sd_value(vec![("myapp@12345", vec![("orig", Value::str("1"))])]));
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let mut encoder = SyslogEncoder::new(Format::Rfc5424, 16)
            .with_structured_data("myapp@12345")
            .unwrap()
            .with_diagnostics(diag);
        let (msgs, stats) = encode_with(&mut encoder, vec![event]);
        assert!(msgs[0].contains(r#"[myapp@12345 orig="1"]"#), "got: {}", msgs[0]);
        assert_eq!(
            stats.dropped_invalid_sd, 0,
            "nothing would have been emitted, so no drop should be counted"
        );
        let events = registry.drain(0);
        assert!(
            !events.iter().any(|e| e.attributes.get("key").and_then(|v| v.as_str())
                == Some("invalid_structured_data")),
            "no diagnostic should fire when nothing would have been emitted"
        );
    }

    #[test]
    fn with_structured_data_rejects_an_sd_id_without_an_at_sign() {
        let result = SyslogEncoder::new(Format::Rfc5424, 16).with_structured_data("myapp");
        let err = match result {
            Ok(_) => panic!("expected an error for an sd_id with no '@'"),
            Err(e) => e,
        };
        assert!(err.to_string().contains('@'), "error should mention the missing '@': {err}");
    }

    #[test]
    fn with_structured_data_rejects_an_invalid_sd_name() {
        // Contains a space, forbidden by SD-NAME/PRINTUSASCII.
        let result = SyslogEncoder::new(Format::Rfc5424, 16).with_structured_data("my app@123");
        let err = match result {
            Ok(_) => panic!("expected an error for an invalid SD-NAME"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("SD-NAME"), "got: {err}");
    }

    // -- `Value::Bytes` message (module doc's "Message body" section) ------------------------

    #[test]
    fn a_bytes_message_is_written_raw_with_control_bytes_escaped() {
        let event = log_event_with_attrs(
            0,
            Value::Bytes(bytes::Bytes::from_static(b"line one\nctrl\x01byte")),
            None,
            AttrMap::new(),
        );
        let mut encoder = SyslogEncoder::new(Format::Rfc5424, 16);
        let (msgs, stats) = encode_with_bytes(&mut encoder, vec![event]);
        assert_eq!(stats, EncodeStats::default());
        let msg = String::from_utf8(msgs[0].clone()).expect("escaped output must be valid UTF-8");
        assert!(msg.ends_with("line one\\nctrl\\x01byte"), "got: {msg}");
    }

    #[test]
    fn a_non_utf8_bytes_message_survives_unmangled_apart_from_escaping() {
        // 0xff alone isn't valid UTF-8; `from_utf8_lossy` would turn it into U+FFFD.
        let mut raw = b"before-".to_vec();
        raw.push(0xff);
        raw.extend_from_slice(b"-after");
        let event = log_event_with_attrs(
            0,
            Value::Bytes(bytes::Bytes::from(raw.clone())),
            None,
            AttrMap::new(),
        );
        let mut encoder = SyslogEncoder::new(Format::Rfc5424, 16);
        let (msgs, _) = encode_with_bytes(&mut encoder, vec![event]);
        // 0xff isn't a control byte, so it passes through unescaped.
        assert!(
            msgs[0].windows(3).any(|w| w == [b'-', 0xffu8, b'-']),
            "raw byte 0xff must survive: {:?}",
            msgs[0]
        );
    }

    // -- Pure-codec fixed point (`docs/plans/lossless-transit.md`'s "Tests") -------------------
    //
    // A small RFC 5424 grammar generator run through the real decoder and this encoder. For
    // every generated line: `decode(encode(decode(line))) == decode(line)` (receipt-time
    // `timestamp` fields normalized), and `encode(decode(line))` is a byte-exact fixed point of
    // `encode . decode`. Neither needs the generated line's own formatting to survive;
    // `crates/logit-cli/tests/syslog_round_trip.rs`'s corpus covers byte-for-byte against real
    // input.
    mod fixed_point {
        use super::*;
        use logit_inputs::syslog::SyslogDecoder;
        use logit_proto::Decoder;
        use proptest::prelude::*;
        use std::collections::HashSet;

        /// A HOSTNAME/APP-NAME/PROCID/MSGID, or nil. Already `PRINTUSASCII`, so
        /// `sanitize_5424_field` leaves it alone.
        fn opt_token() -> impl Strategy<Value = Option<String>> {
            prop_oneof![Just(None), "[A-Za-z0-9.-]{1,16}".prop_map(Some)]
        }

        /// An RFC 3339 TIMESTAMP (6 fractional digits, `Z` or `+02:00`) or nil. The offset form
        /// needn't match `push_rfc5424_timestamp`'s `Z` output: the property compares the
        /// second encode against the first, not against the generated line.
        fn opt_timestamp() -> impl Strategy<Value = Option<String>> {
            prop_oneof![
                Just(None),
                (
                    1970i32..2100,
                    1u32..=12,
                    1u32..=28,
                    0u32..24,
                    0u32..60,
                    0u32..60,
                    0u32..1_000_000u32,
                    any::<bool>(),
                )
                    .prop_map(|(y, mo, d, h, mi, s, frac, use_offset)| {
                        let base = format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{frac:06}");
                        Some(if use_offset { format!("{base}+02:00") } else { format!("{base}Z") })
                    }),
            ]
        }

        /// Unescaped PARAM-VALUE content: printable ASCII (including `"`, `\`, `]`) plus a few
        /// multi-byte characters.
        fn printable_utf8(max_len: usize) -> impl Strategy<Value = String> {
            prop::collection::vec(
                prop_oneof![
                    3 => proptest::char::range('\u{20}', '\u{7e}'),
                    1 => prop_oneof![Just('é'), Just('—'), Just('✓'), Just('日')],
                ],
                0..max_len,
            )
            .prop_map(|chars| chars.into_iter().collect())
        }

        fn sd_param() -> impl Strategy<Value = (String, String)> {
            ("[a-zA-Z]{1,8}", printable_utf8(8))
        }

        fn sd_element() -> impl Strategy<Value = (String, Vec<(String, String)>)> {
            ("[a-z]{1,8}", prop::collection::vec(sd_param(), 0..=3))
                .prop_map(|(id, params)| (format!("{id}@32473"), params))
        }

        /// 0-3 SD-ELEMENTs with distinct SD-IDs, since the decoder rejects a repeat (tested in
        /// `syslog_in`). A duplicate is dropped rather than the whole case discarded.
        fn sd_elements() -> impl Strategy<Value = Vec<(String, Vec<(String, String)>)>> {
            prop::collection::vec(sd_element(), 0..=3).prop_map(|elements| {
                let mut seen = HashSet::new();
                elements.into_iter().filter(|(id, _)| seen.insert(id.clone())).collect()
            })
        }

        fn escape_param_value(v: &str) -> String {
            let mut out = String::new();
            for c in v.chars() {
                match c {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    ']' => out.push_str("\\]"),
                    c => out.push(c),
                }
            }
            out
        }

        /// One valid RFC 5424 line, rendered independently of `SyslogEncoder` so the test isn't
        /// the encoder agreeing with itself.
        #[allow(clippy::too_many_arguments)]
        fn render_line(
            pri: u8,
            ts: &Option<String>,
            host: &Option<String>,
            app: &Option<String>,
            procid: &Option<String>,
            msgid: &Option<String>,
            sd: &[(String, Vec<(String, String)>)],
            msg: &str,
        ) -> String {
            let field = |v: &Option<String>| v.clone().unwrap_or_else(|| "-".to_string());
            let sd_text = if sd.is_empty() {
                "-".to_string()
            } else {
                sd.iter()
                    .map(|(id, params)| {
                        let mut s = format!("[{id}");
                        for (name, value) in params {
                            s.push_str(&format!(" {name}=\"{}\"", escape_param_value(value)));
                        }
                        s.push(']');
                        s
                    })
                    .collect::<String>()
            };
            let mut line = format!(
                "<{pri}>1 {} {} {} {} {} {}",
                field(ts),
                field(host),
                field(app),
                field(procid),
                field(msgid),
                sd_text,
            );
            if !msg.is_empty() {
                line.push(' ');
                line.push_str(msg);
            }
            line
        }

        fn normalize_receipt_time(batch: &mut EventBatch) {
            for event in &mut batch.events {
                event.timestamp = 0;
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(200))]

            #[test]
            fn decode_encode_decode_is_a_fixed_point(
                pri in 0u8..=191,
                ts in opt_timestamp(),
                host in opt_token(),
                app in opt_token(),
                procid in opt_token(),
                msgid in opt_token(),
                sd in sd_elements(),
                msg in printable_utf8(24),
            ) {
                let line = render_line(pri, &ts, &host, &app, &procid, &msgid, &sd, &msg);

                let mut decoder = SyslogDecoder::new(Arc::new(Resource::default()));
                let mut d1 = decoder
                    .decode(bytes::Bytes::from(line.clone()))
                    .unwrap_or_else(|e| panic!("generated line {line:?} should decode: {e}"));
                prop_assert_eq!(d1.events.len(), 1, "one line should decode to one event: {:?}", line);

                let mut encoder = SyslogEncoder::new(Format::Rfc5424, 16);
                let mut out1 = MessageBuf::default();
                encoder.encode_into(&d1, &mut out1);
                prop_assert_eq!(out1.iter().count(), 1);
                let e1 = out1.iter().next().unwrap().to_vec();

                let mut d2 = decoder
                    .decode(bytes::Bytes::from(e1.clone()))
                    .unwrap_or_else(|e| panic!("re-encoded line {e1:?} should decode: {e}"));

                normalize_receipt_time(&mut d1);
                normalize_receipt_time(&mut d2);
                prop_assert_eq!(
                    &d1, &d2,
                    "decode(encode(decode(line))) must equal decode(line) for {:?}",
                    line
                );

                let mut out2 = MessageBuf::default();
                encoder.encode_into(&d2, &mut out2);
                let e2 = out2.iter().next().unwrap().to_vec();
                prop_assert_eq!(
                    e1, e2,
                    "encode(decode(line)) must be a fixed point of encode . decode for {:?}",
                    line
                );
            }
        }
    }
}
