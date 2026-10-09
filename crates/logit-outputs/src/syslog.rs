//! RFC 3164 / RFC 5424 syslog egress over UDP or TCP, the mirror of `logit_proto::syslog`. A
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
//!   has nowhere to put it, so it falls through rather than make the guess `logit_proto::syslog`'s
//!   module doc declines to make on the way in.
//! - `Value::Null` (5424's nil `-`) renders as `-` on a 5424 output. RFC 3164 has no NILVALUE
//!   TIMESTAMP, so a 3164 output falls through.
//! - An absent attribute, or any other variant, falls through.
//!
//! `event.timestamp` stays receipt time (`docs/adr/decoupled-listener-io.md`); this sink never
//! resolves `syslog.timestamp` onto it. The `timestamp` transform (`format: rfc3164`, `from:
//! syslog.timestamp`, `docs/adr/timestamp-transform.md`) does that before an event reaches this
//! sink. It removes the source attribute unless `keep_source: true`, and a removed attribute
//! re-renders from `event.timestamp` in UTC. Set `keep_source: true` when the transform's
//! `timezone:` isn't UTC, so a 3164 output still writes the sender's original token.
//!
//! ## Header-field precedence
//!
//! Per event, per field, first hit wins: the `syslog.*` attribute, then the configured default,
//! then a format-appropriate absence (`-` for RFC 5424's NILVALUE, an omitted token for RFC 3164).
//! `syslog.severity` outranks `log.severity` because `logit_proto::syslog`'s `map_severity` is lossy: it
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
//!   (`docs/known-gaps/syslog.md`).
//! - PARAM-VALUEs are escaped by [`push_sd_escaped`]: `"` -> `\"`, `\` -> `\\`, `]` -> `\]`, plus
//!   every C0 control character and DEL (see "Injection safety").
//! - SD-ID and PARAM-NAME must be RFC 5424 section 6.3.2 `SD-NAME`s ([`is_valid_sd_name`]: 1-32
//!   `PRINTUSASCII` characters excluding `=`, SP, `]`, `"`). An invalid SD-ID skips the whole
//!   element, an invalid PARAM-NAME skips that param; both count in
//!   [`EncodeStats::dropped_invalid_sd_name`] and a throttled `invalid_structured_data`
//!   diagnostic.
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
//!   event's own `syslog.sd`, the opt-in element is skipped
//!   ([`EncodeStats::dropped_sd_id_collision`], a throttled `invalid_structured_data` diagnostic
//!   naming the collision). `logit_proto::syslog`'s `parse_structured_data` fails the RFC 5424
//!   parse on a repeated SD-ID, so emitting both would make the far end of a
//!   `syslog_in -> syslog_out -> syslog_in` relay read the line as RFC 3164 and lose its structure.
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
//! with a real newline (`docs/known-gaps/syslog.md`).
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
//! it, so this doesn't reuse `human::render_value_inline`, which quotes and escapes a string for
//! a terminal. `Value::Map`/`Value::Array` do reuse it as a container fallback, since
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
//! every modern relay chain. Over UDP the encoder's bound is at most 65507, the largest UDP
//! payload ([`SyslogOutput::with_encoder`]), so a longer message is truncated below rather than
//! refused by the kernel. UDP sends one datagram per message through the path the UDP sinks share
//! (`crate::datagram`, which lists the fault rules).
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
//! 44). An endpoint whose host is no valid TLS server name fails at construction. Every connect
//! after the first counts `logit.output.reconnects`, TLS or plaintext.
//!
//! TCP sends one octet-counted frame per batch through the pooled-stream driver in
//! `crate::stream`, shared with `statsd_out` and `graphite_out`; its module doc lists the
//! fault rules. On TLS a write `Err` is `Fault::Ambiguous`, and on both a batch is called
//! delivered only after a flush. Both transports count `logit.output.requests` tagged
//! `class=ok|clean|ambiguous|rejected|refused`.
//!
//! ## Delivery posture
//!
//! The default, `at_least_once` (`docs/adr/delivery-semantics.md`, item 5), retries an
//! `Ambiguous` attempt on both transports, and a resent message is a second line at the receiver.
//! `buffer.delivery: at_most_once` drops the batch instead; a `Fault::Clean` attempt is retried
//! under either.

use crate::accounting::BatchAccounting;
use crate::count_request;
use crate::datagram::{Datagrams, Framing, Report, UdpDest};
use crate::human::render_value_inline;
use crate::stream::{Dial, PooledStream, Target, TlsTarget};
use crate::Output;
use anyhow::Context;
use logit_core::time::{format_rfc3339_utc, write_rfc3164_utc};
use logit_core::{interner, AttrMap, Diagnostics, Event, EventBatch, Severity, Telemetry, Value};
use logit_pipeline::{BatchContext, SeqId};
use logit_proto::{FramedEncoder, MessageBuf, MAX_UDP_PAYLOAD_BYTES};
use std::fmt::Write as _;
use std::path::Path;
use std::time::Duration;

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
    /// SD-ELEMENTs skipped for an invalid SD-ID, or SD-PARAMs skipped for an invalid PARAM-NAME
    /// (module doc's "STRUCTURED-DATA").
    pub dropped_invalid_sd_name: usize,
    /// `syslog.sd` elements skipped because their value isn't a `Value::Map`.
    pub dropped_sd_not_map: usize,
    /// Opt-in `structured_data` SD-ELEMENTs skipped because the event's `syslog.sd` already
    /// carries their SD-ID.
    pub dropped_sd_id_collision: usize,
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

    pub fn max_message_bytes(&self) -> usize {
        self.max_message_bytes
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

/// The lossy inverse of `logit_proto::syslog`'s `map_severity`, for an event with no
/// `syslog.severity`. `Fatal` maps to `2` (crit), not `0` (emerg): `emerg` means "system unusable",
/// a claim `Fatal` never makes. `Trace` has no syslog equivalent and maps to `7` (debug), as
/// `Debug` does.
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
/// `Bytes`/`Map`/`Array` fall back to [`render_value_inline`].
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
        Value::Bytes(_) | Value::Map(_) | Value::Array(_) => render_value_inline(out, value),
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
                    stats.dropped_sd_not_map += 1;
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
            // `syslog_in` reads a line with a repeated SD-ID as RFC 3164, so on a collision the
            // origin's element wins and the opt-in one is dropped rather than make the far end
            // lose the line's structure.
            let collides = matches!(
                attrs.get("syslog.sd"),
                Some(Value::Map(sd)) if sd.get(&cfg.sd_id).is_some()
            );
            if collides {
                stats.dropped_sd_id_collision += 1;
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
/// [`EncodeStats::dropped_invalid_sd_name`] with a throttled `invalid_structured_data` diagnostic.
///
/// PARAM-NAMEs are sorted by name bytes. They're already unique (the decoder groups a repeated
/// one under a `Value::Array`), so sorting makes the element a function of its data; a wire
/// `a b a` interleaving re-emits as `a a b` (`docs/known-gaps/syslog.md`).
fn write_sd_element<'a>(
    out: &mut String,
    scratch: &mut String,
    sd_id: &str,
    params: impl Iterator<Item = (&'a str, &'a Value)>,
    stats: &mut EncodeStats,
    diag: &mut Diagnostics,
) {
    if !is_valid_sd_name(sd_id) {
        stats.dropped_invalid_sd_name += 1;
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
/// Skipped and counted in [`EncodeStats::dropped_invalid_sd_name`], as in [`write_sd_element`],
/// when `name` isn't a valid `SD-NAME`.
fn write_sd_param(
    out: &mut String,
    scratch: &mut String,
    name: &str,
    value: &Value,
    stats: &mut EncodeStats,
    diag: &mut Diagnostics,
) {
    if !is_valid_sd_name(name) {
        stats.dropped_invalid_sd_name += 1;
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
    write_rfc3164_utc(out, nanos);
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
        // `sanitize_msg` still runs over the result, so `render_value_inline`'s quoting is
        // redundant here, not harmful.
        Value::Array(_) | Value::Map(_) => render_value_inline(out, value),
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
    Udp(UdpDest),
    /// Plaintext or TLS through the shared [`PooledStream`] driver. No DTLS arm: rule 44 rejects
    /// `tls:` under `transport: udp`.
    Tcp {
        pool: PooledStream,
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
    /// The batch's octet-counted TCP frame, reused across `send` calls (UDP sends each message
    /// straight from `messages` and only clears it). Never shrinks, so an outlier batch pins its
    /// peak capacity, the trade `InfluxLineEncoder`'s buffers make (`docs/design/memory.md`).
    frame_buf: Vec<u8>,
    /// TCP only: `Some` exactly when a `tls:` block was configured (module doc's "TLS"). Built
    /// once by [`SyslogOutput::with_tls`], shared by every connect.
    tls: Option<TlsTarget>,
    /// Ungated: the transport's counts, the `oversize_datagram` drops, and the sink's own
    /// warnings. The encoder holds a view gated by `accounting` (`crate::accounting`).
    diag: Diagnostics,
    telemetry: Telemetry,
    accounting: BatchAccounting,
    /// Replaces the stream transports' dial target with scripted connections.
    #[cfg(test)]
    dial_script: Option<std::sync::Arc<crate::test_support::ScriptedDial>>,
}

impl SyslogOutput {
    /// Binds an ephemeral local IPv4 UDP socket now. `endpoint` is resolved per batch instead, so
    /// a DNS failure is a delivery-time `Fault::Clean`, not a startup error
    /// (`crate::datagram`'s module doc).
    pub fn udp(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self::new(endpoint, Conn::Udp(UdpDest::bind("syslog_out")?)))
    }

    /// Never connects here -- see [`Conn`]'s doc comment.
    pub fn tcp(endpoint: impl Into<String>, connect_timeout: Duration) -> Self {
        Self::new(endpoint, Conn::Tcp { pool: PooledStream::default(), connect_timeout })
    }

    fn new(endpoint: impl Into<String>, conn: Conn) -> Self {
        Self {
            endpoint: endpoint.into(),
            conn,
            encoder: SyslogEncoder::new(Format::Rfc5424, 16),
            messages: MessageBuf::default(),
            frame_buf: Vec::new(),
            tls: None,
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
            accounting: BatchAccounting::default(),
            #[cfg(test)]
            dial_script: None,
        }
    }

    /// Installs `encoder`. Over UDP its `max_message_bytes` is capped at
    /// [`MAX_UDP_PAYLOAD_BYTES`], so a longer message is truncated to fit one datagram (module
    /// doc's "Sizing") rather than refused by the kernel and dropped. The encoder gets this
    /// sink's diagnostics (gated), whatever the builder order.
    pub fn with_encoder(mut self, encoder: SyslogEncoder) -> Self {
        let encoder = encoder.with_diagnostics(self.diag.gated(self.accounting.gate()));
        self.encoder = match self.conn {
            Conn::Udp(_) => {
                let cap = encoder.max_message_bytes().min(MAX_UDP_PAYLOAD_BYTES);
                encoder.with_max_message_bytes(cap)
            }
            Conn::Tcp { .. } => encoder,
        };
        self
    }

    /// Turns on RFC 5425 TLS for the TCP connection (`tls:` in config).
    ///
    /// Presence turns it on, so an empty `tls: {}` means TLS with the bundled Mozilla roots and no
    /// client certificate. Unlike `otlp_out`, there's no [`TlsClientSettings::is_empty`] early
    /// return: `otlp_out` has `https://` to select TLS, this sink's bare `host:port` has nothing.
    ///
    /// Errors on the UDP arm (no DTLS) so a caller that bypasses rule 44 can't end up with an
    /// unencrypted socket, and on an endpoint whose host is no valid TLS server name
    /// (`TlsTarget::new`), so it fails startup and not every batch. Paths in `settings` resolve
    /// against `base_dir`, the config file's directory, and load here, since `graph::resolve`
    /// never touches the filesystem.
    ///
    /// Registers the files with `reloader` under this sink's diagnostics and telemetry as they are
    /// when this runs, so call it after `with_diagnostics` and `with_telemetry`.
    pub fn with_tls(
        mut self,
        settings: &TlsClientSettings,
        base_dir: &Path,
        reloader: &logit_pipeline::tls::TlsReloader,
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
        let config = logit_pipeline::tls::build_client_config(
            settings,
            base_dir,
            reloader,
            &self.diag,
            &self.telemetry,
        )?;
        self.tls = Some(TlsTarget::new("syslog_out", &self.endpoint, config)?);
        Ok(self)
    }

    /// The encoder gets a view gated by this sink's batch accounting.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.encoder = self.encoder.with_diagnostics(diag.gated(self.accounting.gate()));
        self.diag = diag;
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

/// Turns one `encode_into` call's [`EncodeStats`] into `logit.output.*` telemetry points.
/// A free function so it's testable without a real socket.
fn report_encode_stats(telemetry: &Telemetry, stats: &EncodeStats) {
    telemetry.count("logit.output.events.skipped", stats.skipped_no_log as f64, &[]);
    telemetry.count("logit.output.messages.truncated", stats.truncated as f64, &[]);
    telemetry.count(
        "logit.output.messages.dropped",
        stats.dropped_oversize_header as f64,
        &[("reason", "oversize_header")],
    );
    telemetry.count(
        "logit.output.structured_data.dropped",
        stats.dropped_invalid_sd_name as f64,
        &[("reason", "invalid_sd_name")],
    );
    telemetry.count(
        "logit.output.structured_data.dropped",
        stats.dropped_sd_not_map as f64,
        &[("reason", "not_a_map")],
    );
    telemetry.count(
        "logit.output.structured_data.dropped",
        stats.dropped_sd_id_collision as f64,
        &[("reason", "sd_id_collision")],
    );
}

impl SyslogOutput {
    /// One `Output::send` attempt.
    async fn attempt(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let (first, stats) =
            self.accounting.encode(0, || self.encoder.encode_into(batch, &mut self.messages));
        if first {
            report_encode_stats(&self.telemetry, &stats);
        }
        if self.messages.is_empty() {
            // Every event was skipped or dropped: nothing to write.
            return Ok(());
        }

        if first {
            let bytes = self.messages.total_bytes() as f64;
            self.telemetry.count("logit.output.batch.bytes", bytes, &[]);
        }
        let request_timer = self.telemetry.timer("logit.output.request.duration");
        let result = match &mut self.conn {
            // One datagram per message, never packed, which would depend on the receiver
            // splitting on a delimiter (module doc's "Injection safety").
            Conn::Udp(udp) => {
                let batch = Datagrams {
                    entries: &self.messages,
                    weight: |_| 1,
                    cap: MAX_UDP_PAYLOAD_BYTES,
                    framing: Framing::OnePerEntry,
                };
                let mut report =
                    Report { sink: "syslog_out", diag: &mut self.diag, telemetry: &self.telemetry };
                let (sent, result) =
                    udp.send(&self.endpoint, batch, &mut self.frame_buf, &mut report).await;
                // What reached the kernel, even when the batch then failed.
                self.telemetry.count("logit.output.messages", sent.entries as f64, &[]);
                count_request(&self.telemetry, &result);
                result
            }
            // The driver counts `requests` for this arm.
            Conn::Tcp { pool, connect_timeout } => {
                frame_octet_counting(&self.messages, &mut self.frame_buf);
                let target = Target::Tcp { endpoint: &self.endpoint, tls: self.tls.as_ref() };
                #[cfg(test)]
                let target = crate::stream::scripted_or(target, &self.dial_script);
                let dial = Dial {
                    target,
                    connect_timeout: *connect_timeout,
                    sink: "syslog_out",
                    nodelay: false,
                };
                let result = pool.send(&dial, &self.frame_buf, &self.telemetry).await;
                if result.is_ok() {
                    self.telemetry.count("logit.output.messages", self.messages.len() as f64, &[]);
                }
                result
            }
        };
        drop(request_timer);
        result
    }
}

#[async_trait::async_trait]
impl Output for SyslogOutput {
    /// Arms this sink's batch accounting (`crate::accounting`).
    fn observe_batch(&mut self, _ctx: BatchContext, _seq: SeqId) {
        self.accounting.observe();
    }

    /// One attempt ([`SyslogOutput::attempt`]). An `Ok` disarms the batch accounting on every
    /// path, a batch that encoded to nothing included.
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let result = self.attempt(batch).await;
        self.accounting.finish(result)
    }

    /// A backstop: `send` pools a connection only after flushing it, and a cancelled attempt
    /// drops its connection, so there's normally nothing left to flush. On TLS "flushed" is a
    /// property of the session, not the socket, which is why it stays.
    async fn flush(&mut self) -> anyhow::Result<()> {
        if let Conn::Tcp { pool, .. } = &mut self.conn {
            pool.flush().await.context("flushing syslog_out TCP stream")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        assert_counted_once_per_batch, assert_direct_sends_count_after_an_empty_batch, fast_retry,
        fixture_server_config, rewrite_tls_file, scratch_tls_files, server_tls_config, sum_of,
        sums_through_write_loop, testdata_dir, tls_settings, Collector, DialStep, FakeStream,
        ReadMode, ScriptedDest, ScriptedDial, SendStep, WriteStep,
    };
    use logit_core::{BodyFormat, LogRecord, MetricKind, MetricRecord, Registry, Resource};
    use logit_pipeline::test_util::TelemetryProbe;
    use logit_pipeline::Fault;
    use logit_proto::syslog::SyslogDecoder;
    use logit_proto::Decoder;
    use std::sync::{Arc, Mutex};
    use tokio::io::AsyncWriteExt;
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

    /// Asserts each of `EncodeStats`'s three structured-data drop counters landed on the expected
    /// value, so a test naming one cause also proves the other two stayed at zero.
    fn assert_sd_drops(stats: &EncodeStats, invalid_name: usize, not_map: usize, collision: usize) {
        assert_eq!(stats.dropped_invalid_sd_name, invalid_name, "dropped_invalid_sd_name");
        assert_eq!(stats.dropped_sd_not_map, not_map, "dropped_sd_not_map");
        assert_eq!(stats.dropped_sd_id_collision, collision, "dropped_sd_id_collision");
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

    #[tokio::test]
    async fn udp_sends_one_datagram_per_message() {
        let mut collector = Collector::udp().await;
        let mut output = SyslogOutput::udp(collector.addr().to_string()).unwrap();
        let batch = batch_with(vec![log_event(0, "one", None), log_event(0, "two", None)]);
        output.send(&batch).await.expect("send should succeed");
        let received = collector.take(2).await;
        assert!(received[0].ends_with(b"one"));
        assert!(received[1].ends_with(b"two"));
    }

    #[tokio::test]
    async fn a_batch_of_only_metric_only_events_performs_no_io() {
        // Nothing listens on this port: `Ok` means `send` needed no receiver.
        let mut output = SyslogOutput::udp("127.0.0.1:1").unwrap();
        let batch = batch_with(vec![metric_event(0)]);
        output.send(&batch).await.expect("an all-skipped batch must not attempt any I/O");
    }

    /// A message longer than one UDP datagram can carry is truncated to fit one by the encoder,
    /// whatever `max_message_bytes` allows, rather than refused by the kernel and dropped.
    #[tokio::test]
    async fn a_message_longer_than_a_udp_datagram_is_truncated_to_fit_one() {
        let mut collector = Collector::udp().await;
        let mut probe = TelemetryProbe::new();
        let mut output = SyslogOutput::udp(collector.addr().to_string())
            .unwrap()
            .with_encoder(SyslogEncoder::new(Format::Rfc5424, 16).with_max_message_bytes(100_000))
            .with_telemetry(probe.telemetry("out", "syslog_out", "sink"));
        let message = "x".repeat(70_000);
        output.send(&batch_with(vec![log_event(0, &message, None)])).await.expect("send");

        let got = collector.next().await;
        assert_eq!(got.len(), 65_507, "the largest IPv4 UDP payload");
        assert!(got.ends_with(b"xxxx"), "the MSG is truncated, not the header");
        assert_eq!(probe.sum("logit.output.messages.truncated", &[]), 1.0);
        assert_eq!(probe.sum("logit.output.messages", &[]), 1.0);
        let oversize = [("reason", "oversize_datagram")];
        assert_eq!(probe.sum("logit.output.messages.dropped", &oversize), 0.0);
    }

    /// The kernel refuses a UDP send to port 0 with `EINVAL`: a clean error, not every message
    /// counted `oversize_datagram` under an `ok` request.
    #[tokio::test]
    async fn a_udp_endpoint_with_port_zero_fails_clean_and_counts_no_oversize() {
        let mut probe = TelemetryProbe::new();
        let mut output = SyslogOutput::udp("127.0.0.1:0").unwrap().with_telemetry(probe.telemetry(
            "out",
            "syslog_out",
            "sink",
        ));
        let batch = batch_with(vec![log_event(0, "one", None)]);
        let err = output.send(&batch).await.expect_err("the kernel refuses port 0");
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
        assert_eq!(crate::test_support::errno_in(&err), Some(22), "EINVAL on Linux: {err:#}");
        let oversize = [("reason", "oversize_datagram")];
        assert_eq!(probe.sum("logit.output.messages.dropped", &oversize), 0.0);
        assert_eq!(probe.sum("logit.output.requests", &[("class", "clean")]), 1.0);
    }

    /// A UDP `syslog_out` over `script`, reporting into `probe`.
    fn scripted_udp(script: &Arc<ScriptedDest>, probe: &TelemetryProbe) -> SyslogOutput {
        let mut output = SyslogOutput::udp("127.0.0.1:514")
            .unwrap()
            .with_telemetry(probe.telemetry("out", "syslog_out", "sink"));
        output.conn = Conn::Udp(UdpDest::Scripted(Arc::clone(script)));
        output
    }

    fn three_messages() -> EventBatch {
        batch_with(["one", "two", "three"].map(|m| log_event(0, m, None)).to_vec())
    }

    /// Over IPv4 the encoder's cap keeps a message inside one datagram, so a kernel `EMSGSIZE`
    /// needs a script: the refused message is dropped and counted, and the rest are sent.
    #[tokio::test]
    async fn an_emsgsize_message_is_dropped_and_counted_and_the_rest_are_sent() {
        let script = ScriptedDest::new([SendStep::Accept, SendStep::TooLarge, SendStep::Accept]);
        let mut probe = TelemetryProbe::new();
        let mut output = scripted_udp(&script, &probe);
        output.send(&three_messages()).await.expect("an EMSGSIZE message is a drop, not a fault");
        let sent = script.datagrams();
        assert_eq!(sent.len(), 2);
        assert!(sent[0].ends_with(b"one") && sent[1].ends_with(b"three"));
        let dropped =
            probe.sum("logit.output.messages.dropped", &[("reason", "oversize_datagram")]);
        assert_eq!(dropped, 1.0);
        assert_eq!(probe.sum("logit.output.messages", &[]), 2.0);
        assert_eq!(probe.sum("logit.output.requests", &[("class", "ok")]), 1.0);
    }

    /// A batch that fails after two messages counts the two before it returns the error.
    #[tokio::test]
    async fn a_udp_failure_after_two_messages_counts_what_reached_the_wire() {
        let script = ScriptedDest::new([
            SendStep::Accept,
            SendStep::Accept,
            SendStep::Fail(std::io::ErrorKind::ConnectionRefused),
        ]);
        let mut probe = TelemetryProbe::new();
        let mut output = scripted_udp(&script, &probe);
        let err = output.send(&three_messages()).await.expect_err("the third message fails");
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
        assert_eq!(probe.sum("logit.output.messages", &[]), 2.0);
        assert_eq!(probe.sum("logit.output.requests", &[("class", "ambiguous")]), 1.0);
    }

    /// An IPv6 endpoint goes out over an IPv6 socket.
    #[tokio::test]
    async fn an_ipv6_udp_endpoint_is_delivered() {
        let Ok(mut collector) = Collector::udp_at("[::1]:0").await else {
            println!("skipping: this environment has no usable IPv6 loopback");
            return;
        };
        let mut output = SyslogOutput::udp(collector.addr().to_string()).unwrap();
        let batch = batch_with(vec![log_event(0, "one", None)]);
        output.send(&batch).await.expect("an IPv6 endpoint must be reachable");
        assert!(collector.next().await.ends_with(b"one"));
    }

    // -- Sink: TCP ------------------------------------------------------------------------------

    #[tokio::test]
    async fn tcp_sends_one_octet_counted_frame_per_batch() {
        let mut collector = Collector::tcp(ReadMode::ToEof).await;
        let mut output = SyslogOutput::tcp(collector.addr().to_string(), Duration::from_secs(2));
        let batch = batch_with(vec![log_event(0, "one", None), log_event(0, "two", None)]);
        output.send(&batch).await.expect("send should succeed");
        // Drop the sink so its write side closes and the collector's read_to_end returns.
        drop(output);
        let got = collector.next().await;
        assert_eq!(collector.accepts(), 1);
        let frame = String::from_utf8_lossy(&got);
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
        let mut collector = Collector::tcp(ReadMode::ToEof).await;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_out", "syslog", "output");
        let mut output = SyslogOutput::tcp(collector.addr().to_string(), Duration::from_secs(2))
            .with_telemetry(telemetry);

        let batch = batch_with(vec![log_event(0, "first", None)]);
        output.send(&batch).await.expect("first send should succeed against a fresh connection");

        assert_eq!(
            reconnects_in(registry.drain(0)),
            None,
            "the first connect must not be counted as a reconnect"
        );

        // Shut down the local write half rather than race a real peer RST: the next `write()`
        // fails with `BrokenPipe` deterministically, and the driver treats any write failure
        // on an inherited connection alike.
        pooled(&mut output).shutdown().await.expect("local shutdown should succeed");

        let batch2 = batch_with(vec![log_event(0, "second", None)]);
        output
            .send(&batch2)
            .await
            .expect("second send should reconnect once and succeed, not surface the failure");

        drop(output); // closes the second connection so its `read_to_end` completes
        let got = collector.take(2).await;
        assert_eq!(
            collector.accepts(),
            2,
            "the failure must cause exactly one reconnect, not be silently absorbed or looped"
        );
        assert_eq!(
            reconnects_in(registry.drain(0)),
            Some(1.0),
            "exactly one reconnect, counted (`logit.output.reconnects`, \
             docs/design/internal-telemetry.md)"
        );
        assert!(got.iter().any(|b| String::from_utf8_lossy(b).contains("first")));
        assert!(got.iter().any(|b| String::from_utf8_lossy(b).contains("second")));
    }

    /// The reuse probe (`crate::stream::PooledStream::send`): a message sent after the receiver
    /// closed the pooled connection still arrives. Asserted at the collector, since a write into a
    /// FIN'd socket succeeds locally and `send` would return `Ok` either way.
    #[tokio::test]
    async fn a_pooled_connection_the_peer_closed_is_reconnected_before_writing_and_the_message_is_not_lost(
    ) {
        let mut collector = Collector::tcp(ReadMode::FirstReadThenClose).await;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_out", "syslog", "output");
        let mut output = SyslogOutput::tcp(collector.addr().to_string(), Duration::from_secs(2))
            .with_telemetry(telemetry);

        output
            .send(&batch_with(vec![log_event(0, "first", None)]))
            .await
            .expect("first send should succeed against a fresh connection");

        // The collector closes before it reports, so its FIN is sent before the probe looks.
        let first = collector.next().await;
        assert!(String::from_utf8_lossy(&first).contains("first"));

        output
            .send(&batch_with(vec![log_event(0, "second", None)]))
            .await
            .expect("the probe should reconnect rather than write into a closed socket");

        drop(output);
        let second = collector.next().await;
        assert_eq!(
            collector.accepts(),
            2,
            "the probe must have dialled a second connection for the second message"
        );
        assert_eq!(
            reconnects_in(registry.drain(0)),
            Some(1.0),
            "the replacement is an ordinary reconnect, counted like any other"
        );
        let second = String::from_utf8_lossy(&second);
        assert!(
            second.contains("second"),
            "the second message must have reached the receiver: {second:?}"
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

    /// The bytes a plaintext `syslog_out` delivers for `batch`: the exact reference the TLS
    /// tests compare against.
    async fn plaintext_frame_for(batch: &EventBatch) -> Vec<u8> {
        let mut collector = Collector::tcp(ReadMode::ToEof).await;
        let mut output = SyslogOutput::tcp(collector.addr().to_string(), Duration::from_secs(2));
        output.send(batch).await.expect("plaintext send should succeed");
        drop(output);
        collector.next().await
    }

    #[tokio::test]
    async fn tls_tcp_sends_the_same_octet_counted_frame_as_plaintext() {
        let batch = batch_with(vec![log_event(0, "one", None), log_event(0, "two", None)]);
        let expected = plaintext_frame_for(&batch).await;

        let mut collector = Collector::tls(server_tls_config(false).into(), ReadMode::ToEof).await;
        // `localhost`, not `127.0.0.1` (both SANs), so `host_only` yields a DNS SNI name.
        let endpoint = format!("localhost:{}", collector.addr().port());
        let mut output = SyslogOutput::tcp(endpoint, Duration::from_secs(2))
            .with_tls(
                &tls_settings(|t| t.ca_file = Some("ca.pem".to_string())),
                &testdata_dir(),
                &logit_pipeline::tls::TlsReloader::new(),
            )
            .expect("a tls: block on the TCP transport is legal");
        output.send(&batch).await.expect("send over TLS should succeed");
        drop(output);
        let got = collector.next().await;
        assert_eq!(collector.accepts(), 1);
        assert_eq!(
            got, expected,
            "TLS must deliver byte-for-byte the same octet-counted frame plaintext does"
        );
    }

    /// The stream driver's `TlsTarget` holds the config built at startup, and a reloaded `ca_file`
    /// still reaches its next dial.
    #[tokio::test]
    async fn a_reloaded_ca_file_reaches_the_next_dial() {
        let mut collector = Collector::tls(
            fixture_server_config("server-other", None, &[]).into(),
            ReadMode::ToEof,
        )
        .await;
        let dir = scratch_tls_files("syslog-out-ca-reload", &[("ca.pem", "ca.pem")]);
        let reloader = logit_pipeline::tls::TlsReloader::new();
        let mut output = SyslogOutput::tcp(
            format!("localhost:{}", collector.addr().port()),
            Duration::from_secs(2),
        )
        .with_tls(&tls_settings(|t| t.ca_file = Some("ca.pem".to_string())), &dir, &reloader)
        .expect("a tls: block on the TCP transport is legal");
        let batch = batch_with(vec![log_event(0, "rotated", None)]);
        output.send(&batch).await.expect_err("ca.pem trusted a collector under other-ca.pem");

        rewrite_tls_file(&dir, "ca.pem", "other-ca.pem");
        reloader.check_now();

        output.send(&batch).await.expect("the reloaded CA should trust the collector");
        drop(output);
        assert!(String::from_utf8_lossy(&collector.next().await).contains("rotated"));
    }

    #[tokio::test]
    async fn tls_tcp_with_a_client_certificate_satisfies_a_client_ca_requiring_collector() {
        let mut collector = Collector::tls(server_tls_config(true).into(), ReadMode::ToEof).await;
        let mut output = SyslogOutput::tcp(
            format!("localhost:{}", collector.addr().port()),
            Duration::from_secs(2),
        )
        .with_tls(
            &tls_settings(|t| {
                t.ca_file = Some("ca.pem".to_string());
                t.cert_file = Some("client.pem".to_string());
                t.key_file = Some("client.key".to_string());
            }),
            &testdata_dir(),
            &logit_pipeline::tls::TlsReloader::new(),
        )
        .expect("a client certificate is legal on the TCP transport");
        let batch = batch_with(vec![log_event(0, "mutual", None)]);
        output.send(&batch).await.expect("mutual TLS should succeed");
        drop(output);
        let got = collector.next().await;
        assert_eq!(collector.accepts(), 1);
        assert!(String::from_utf8_lossy(&got).contains("mutual"));
    }

    /// A sink with no client certificate delivers nothing to a mutual-TLS collector.
    ///
    /// Asserted at the collector, not on `send`: under TLS 1.3 the client's `connect` completes
    /// before the server rejects its certificate, the rejection is an alert this write-only sink
    /// never reads, and whether the next `write()` fails depends on RST timing.
    #[tokio::test]
    async fn tls_tcp_without_a_client_certificate_delivers_nothing_to_a_client_ca_requiring_collector(
    ) {
        let mut collector = Collector::tls(server_tls_config(true).into(), ReadMode::ToEof).await;
        let mut output = SyslogOutput::tcp(
            format!("localhost:{}", collector.addr().port()),
            Duration::from_secs(2),
        )
        .with_tls(
            &tls_settings(|t| t.ca_file = Some("ca.pem".to_string())),
            &testdata_dir(),
            &logit_pipeline::tls::TlsReloader::new(),
        )
        .expect("a tls: block on the TCP transport is legal");
        let batch = batch_with(vec![log_event(0, "rejected", None)]);
        // Usually `Ok`, but a fast RST can fail the write; either way never `Rejected` or `Refused`.
        if let Err(err) = output.send(&batch).await {
            assert!(
                matches!(logit_pipeline::classify(&err), Fault::Clean | Fault::Ambiguous),
                "a rejected-handshake write is never rejected or refused: {err:?}"
            );
        }
        drop(output);
        // Covers the collector reading the client's certificate-less flight and rejecting it,
        // well under 1 ms on loopback.
        collector
            .assert_quiet(
                Duration::from_millis(200),
                "nothing may reach a mutual-TLS collector from an unauthenticated client",
            )
            .await;
        assert_eq!(
            collector.accepts(),
            0,
            "a client with no certificate must not complete the handshake"
        );
    }

    /// An untrusted server certificate fails the handshake before any batch byte leaves the host,
    /// so it's `Fault::Clean`.
    #[tokio::test]
    async fn tls_tcp_against_a_server_certificate_from_an_untrusted_ca_is_a_clean_fault() {
        let mut collector = Collector::tls(server_tls_config(false).into(), ReadMode::ToEof).await;
        let mut output = SyslogOutput::tcp(
            format!("localhost:{}", collector.addr().port()),
            Duration::from_secs(2),
        )
        .with_tls(
            &tls_settings(|t| t.ca_file = Some("other-ca.pem".to_string())),
            &testdata_dir(),
            &logit_pipeline::tls::TlsReloader::new(),
        )
        .expect("a tls: block on the TCP transport is legal");
        let batch = batch_with(vec![log_event(0, "untrusted", None)]);
        let err = output.send(&batch).await.expect_err("an untrusted CA must fail the handshake");
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
        // Covers the collector's side of the aborted handshake, well under 1 ms on loopback.
        collector.assert_quiet(Duration::from_millis(100), "an untrusted-CA client").await;
        assert_eq!(collector.accepts(), 0);
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

        let mut collector = Collector::tls(server_tls_config(false).into(), ReadMode::ToEof).await;
        // The bundled Mozilla roots never signed `server.pem`, so only the skip lets this connect.
        let mut output = SyslogOutput::tcp(
            format!("localhost:{}", collector.addr().port()),
            Duration::from_secs(2),
        )
        .with_diagnostics(Diagnostics::new("syslog_out"))
        .with_tls(
            &tls_settings(|t| t.insecure_skip_verify = true),
            &testdata_dir(),
            &logit_pipeline::tls::TlsReloader::new(),
        )
        .expect("insecure_skip_verify is legal, if loud");
        let batch = batch_with(vec![log_event(0, "insecure", None)]);
        output.send(&batch).await.expect("insecure_skip_verify should bypass CA trust");
        drop(output);
        let got = collector.next().await;
        drop(guard);

        assert_eq!(collector.accepts(), 1);
        assert!(String::from_utf8_lossy(&got).contains("insecure"));
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
        let mut collector = Collector::tls(server_tls_config(false).into(), ReadMode::ToEof).await;
        let mut output = SyslogOutput::tcp(collector.addr().to_string(), Duration::from_secs(2));
        let batch = batch_with(vec![log_event(0, "cleartext", None)]);
        let result = output.send(&batch).await;
        if let Err(err) = &result {
            assert!(
                matches!(logit_pipeline::classify(err), Fault::Clean | Fault::Ambiguous),
                "a failed cleartext write is never rejected or refused: {err:?}"
            );
        }
        drop(output);
        // Covers the collector reading the cleartext as a ClientHello and failing it, well under
        // 1 ms on loopback.
        collector
            .assert_quiet(
                Duration::from_millis(200),
                "a TLS listener must never surface cleartext bytes as a message",
            )
            .await;
        assert_eq!(collector.accepts(), 0);
    }

    // -- Sink: TCP over TLS, write/flush semantics --------------------------------------------
    //
    // These drive `SyslogOutput::send` with a scripted [`FakeStream`] pooled behind the
    // boxed-stream seam and a real `with_tls` target, so the shared driver takes its TLS arms
    // with this sink's framing. The real-TLS tests around them cover the socket level,
    // `crate::stream`'s tests cover every fault arm once, and `crate::stream_pins` pins the
    // tokio-rustls behavior those arms assume.

    /// The TCP arm's pooled connection, for a test that breaks it in place.
    fn pooled(output: &mut SyslogOutput) -> &mut Box<dyn crate::tls::AsyncStream> {
        match &mut output.conn {
            Conn::Tcp { pool, .. } => {
                pool.stream_mut().expect("a successful send pools its connection")
            }
            Conn::Udp(_) => panic!("not a TCP syslog_out"),
        }
    }

    fn pool_is_empty(output: &SyslogOutput) -> bool {
        match &output.conn {
            Conn::Tcp { pool, .. } => pool.is_empty(),
            Conn::Udp(_) => panic!("not a TCP syslog_out"),
        }
    }

    /// A TCP `syslog_out` at `endpoint` with `fake` already pooled, as after a first send, TLS on
    /// when `tls` is set (no handshake happens: the pooled stream is reused), and its telemetry
    /// on `probe`.
    fn with_pooled_fake(
        endpoint: &str,
        fake: &FakeStream,
        tls: bool,
        probe: &TelemetryProbe,
    ) -> SyslogOutput {
        let mut output = SyslogOutput::tcp(endpoint, Duration::from_millis(500))
            .with_telemetry(probe.telemetry("out", "syslog_out", "sink"));
        if tls {
            output = output
                .with_tls(
                    &TlsClientSettings::default(),
                    &testdata_dir(),
                    &logit_pipeline::tls::TlsReloader::new(),
                )
                .expect("the default settings and an IP endpoint always build");
        }
        output.conn = Conn::Tcp {
            pool: PooledStream::pooled(Box::new(fake.clone())),
            connect_timeout: Duration::from_millis(500),
        };
        output
    }

    fn one_message() -> EventBatch {
        batch_with(vec![log_event(0, "hello", None)])
    }

    #[tokio::test]
    async fn a_tls_batch_is_reported_delivered_only_once_the_stream_has_been_flushed() {
        let fake = FakeStream::new();
        let mut probe = TelemetryProbe::new();
        let mut output = with_pooled_fake("127.0.0.1:1", &fake, true, &probe);
        let expected = plaintext_frame_for(&one_message()).await;

        output.send(&one_message()).await.expect("the write and the flush both succeed");

        let state = fake.state();
        assert_eq!(state.flushes, 1, "the success path must flush once");
        assert!(state.unflushed.is_empty(), "nothing may be left in the session buffer");
        assert_eq!(state.flushed, expected, "the whole frame must be on the wire");
        drop(state);
        assert!(!pool_is_empty(&output), "a flushed connection is reusable");
        assert_eq!(probe.sum("logit.output.messages", &[]), 1.0);
        assert_eq!(probe.sum("logit.output.requests", &[("class", "ok")]), 1.0);
    }

    #[tokio::test]
    async fn a_tls_flush_failure_is_ambiguous_and_discards_the_connection() {
        let fake = FakeStream::new().failing_flush(std::io::ErrorKind::BrokenPipe);
        let mut probe = TelemetryProbe::new();
        let mut output = with_pooled_fake("127.0.0.1:1", &fake, true, &probe);

        let err = output.send(&one_message()).await.expect_err("a failed flush must fail the send");

        // The session may have pushed some records to the socket before the flush failed.
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
        assert!(pool_is_empty(&output), "a stream whose flush failed must not be reused");
        assert_eq!(fake.state().writes, 1, "and must not be rewritten either");
        assert_eq!(probe.sum("logit.output.requests", &[("class", "ambiguous")]), 1.0);
    }

    /// A TLS write error may follow landed records, so it's `Ambiguous` and never resent (the
    /// plaintext counterpart is below).
    #[tokio::test]
    async fn a_tls_write_failure_is_ambiguous_and_never_resent() {
        let fake = FakeStream::new().on_write(1, WriteStep::Fail(std::io::ErrorKind::BrokenPipe));
        let mut probe = TelemetryProbe::new();
        // Nothing listens here: a wrongful retry would surface as a `Clean` connect failure.
        let mut output = with_pooled_fake("127.0.0.1:1", &fake, true, &probe);

        let err = output.send(&one_message()).await.expect_err("a failed write must fail the send");

        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
        assert!(pool_is_empty(&output));
        let state = fake.state();
        assert_eq!(state.writes, 1, "one write attempt, no resend");
        assert!(state.flushed.is_empty());
        drop(state);
        assert_eq!(probe.sum("logit.output.requests", &[("class", "ambiguous")]), 1.0);
    }

    /// A failure after a partial first write is `Ambiguous` and never resent.
    #[tokio::test]
    async fn a_failure_after_a_partial_write_is_ambiguous_and_never_resent() {
        let fake = FakeStream::new()
            .on_write(1, WriteStep::Short(1))
            .on_write(2, WriteStep::Fail(std::io::ErrorKind::BrokenPipe));
        let mut probe = TelemetryProbe::new();
        let mut output = with_pooled_fake("127.0.0.1:1", &fake, true, &probe);

        let err =
            output.send(&one_message()).await.expect_err("a failed write_all must fail the send");

        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
        assert!(pool_is_empty(&output));
        let state = fake.state();
        assert_eq!(state.writes, 2, "the short write, then the failing remainder, no resend");
        assert_eq!(state.flushes, 0, "a failed write never reaches the flush");
        assert!(state.flushed.is_empty());
        drop(state);
        assert_eq!(probe.sum("logit.output.requests", &[("class", "ambiguous")]), 1.0);
    }

    /// On plaintext a failed first write wrote nothing, so the driver reconnects once and reports
    /// `Fault::Clean` when that fails too.
    #[tokio::test]
    async fn a_plaintext_write_failure_still_reconnects_once_and_stays_clean() {
        let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_addr = dead.local_addr().unwrap().to_string();
        drop(dead); // now nothing is listening there

        let fake = FakeStream::new().on_write(1, WriteStep::Fail(std::io::ErrorKind::BrokenPipe));
        let mut probe = TelemetryProbe::new();
        let mut output = with_pooled_fake(&dead_addr, &fake, false, &probe);

        let err = output.send(&one_message()).await.expect_err("the retry's connect is refused");

        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
        assert_eq!(fake.state().writes, 1);
        assert!(
            format!("{err:#}").contains("connecting to syslog_out endpoint"),
            "the failure must come from the retry's fresh connect, proving one happened: {err:#}"
        );
        assert_eq!(probe.sum("logit.output.requests", &[("class", "clean")]), 1.0);
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
                    &logit_pipeline::tls::TlsReloader::new(),
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
        let mut collector = Collector::tls(server_tls_config(false).into(), ReadMode::ToEof).await;
        let mut output = SyslogOutput::tcp(
            format!("localhost:{}", collector.addr().port()),
            Duration::from_secs(2),
        )
        .with_tls(
            &tls_settings(|t| t.ca_file = Some("ca.pem".to_string())),
            &testdata_dir(),
            &logit_pipeline::tls::TlsReloader::new(),
        )
        .expect("a tls: block on the TCP transport is legal");

        let batch = batch_with(vec![log_event(0, "once", None)]);
        output.send(&batch).await.expect("the first send should succeed");
        // Fails the next write deterministically, as in
        // `tcp_reconnects_after_the_peer_resets_an_inherited_connection`.
        let _ = pooled(&mut output).shutdown().await;
        if let Err(err) = output.send(&batch).await {
            assert_eq!(
                logit_pipeline::classify(&err),
                Fault::Ambiguous,
                "a TLS write failure is never Clean -- it can't prove nothing landed"
            );
        }

        drop(output);
        let first = collector.next().await;
        assert_eq!(
            String::from_utf8_lossy(&first).matches("once").count(),
            1,
            "the frame must reach the receiver once: {first:?}"
        );
        // A resend would dial during `send`, so its connection closed with the drop above; the
        // window covers the collector reading it to EOF, well under 1 ms on loopback.
        collector.assert_quiet(Duration::from_millis(200), "a resent TLS frame").await;
    }

    /// Rule 44's check, repeated at construction: `tls:` on the UDP arm is an error.
    #[tokio::test]
    async fn with_tls_on_the_udp_transport_is_an_error() {
        let output = SyslogOutput::udp("127.0.0.1:514").unwrap();
        // `.err()` rather than `expect_err`, which would need `SyslogOutput: Debug`.
        let err = output
            .with_tls(
                &TlsClientSettings::default(),
                &testdata_dir(),
                &logit_pipeline::tls::TlsReloader::new(),
            )
            .err()
            .expect("DTLS is out of scope");
        assert!(err.to_string().contains("transport: tcp"), "got: {err}");
    }

    /// Plaintext and TLS hand the driver the same octet-counted frame and report the same
    /// counts: one message per encoded message and one `ok` request.
    #[tokio::test]
    async fn tcp_and_tls_report_their_counts_through_the_driver() {
        let batch = batch_with(vec![log_event(0, "one", None), log_event(0, "two", None)]);
        let expected = plaintext_frame_for(&batch).await;
        for tls in [false, true] {
            let mut collector = if tls {
                Collector::tls(server_tls_config(false).into(), ReadMode::ToEof).await
            } else {
                Collector::tcp(ReadMode::ToEof).await
            };
            let mut probe = TelemetryProbe::new();
            let endpoint = format!("localhost:{}", collector.addr().port());
            let mut output = SyslogOutput::tcp(endpoint, Duration::from_secs(2))
                .with_telemetry(probe.telemetry("out", "syslog_out", "sink"));
            if tls {
                output = output
                    .with_tls(
                        &tls_settings(|t| t.ca_file = Some("ca.pem".to_string())),
                        &testdata_dir(),
                        &logit_pipeline::tls::TlsReloader::new(),
                    )
                    .unwrap();
            }
            output.send(&batch).await.expect("send");
            drop(output);
            assert_eq!(collector.next().await, expected, "tls={tls}");
            let totals = probe.poll();
            assert_eq!(totals.sum("logit.output.messages", &[]), 2.0, "tls={tls}");
            assert_eq!(totals.sum("logit.output.requests", &[("class", "ok")]), 1.0);
            assert_eq!(totals.sum("logit.output.requests", &[]), 1.0, "one attempt");
        }
    }

    /// A TLS endpoint whose host is no valid server name fails at construction, naming the
    /// endpoint, rather than failing every batch; an IP literal is a valid name.
    #[test]
    fn with_tls_rejects_an_endpoint_with_no_valid_server_name() {
        for endpoint in ["[fe80::1%eth0]:514", ":514"] {
            let err = SyslogOutput::tcp(endpoint, Duration::from_secs(1))
                .with_tls(
                    &TlsClientSettings::default(),
                    &testdata_dir(),
                    &logit_pipeline::tls::TlsReloader::new(),
                )
                .err()
                .expect(endpoint);
            assert!(err.to_string().contains(endpoint), "{err}");
        }
        for endpoint in ["127.0.0.1:514", "[::1]:6514", "logs.example.com:6514"] {
            SyslogOutput::tcp(endpoint, Duration::from_secs(1))
                .with_tls(
                    &TlsClientSettings::default(),
                    &testdata_dir(),
                    &logit_pipeline::tls::TlsReloader::new(),
                )
                .unwrap_or_else(|err| panic!("{endpoint}: {err}"));
        }
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
        assert_sd_drops(&stats, 1, 0, 0);
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
        assert_sd_drops(&stats, 1, 0, 0);
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
        assert_sd_drops(&stats, 1, 0, 0);
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
    /// since `syslog_in` reads a line with a repeated SD-ID as RFC 3164.
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
        assert_sd_drops(&stats, 0, 0, 1);
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
        assert_sd_drops(&stats, 0, 0, 0);
        let events = registry.drain(0);
        assert!(
            !events.iter().any(|e| e.attributes.get("key").and_then(|v| v.as_str())
                == Some("invalid_structured_data")),
            "no diagnostic should fire when nothing would have been emitted"
        );
    }

    /// A `syslog.sd` element whose value isn't a nested map is skipped and counted separately
    /// from an invalid SD-NAME; a sibling valid element still renders.
    #[test]
    fn a_non_map_syslog_sd_element_is_skipped_and_counted() {
        let mut valid_params = AttrMap::new();
        valid_params.insert("k", Value::str("v"));
        let mut sd = AttrMap::new();
        sd.insert("a@1", Value::Map(Box::new(valid_params)));
        sd.insert("bad", Value::str("nope"));
        let mut attrs = AttrMap::new();
        attrs.insert("syslog.sd", Value::Map(Box::new(sd)));
        let event = log_event_with_attrs(0, Value::str("x"), None, attrs);
        let (msgs, stats) = encode(vec![event]);
        assert!(msgs[0].contains(r#"[a@1 k="v"]"#), "got: {}", msgs[0]);
        assert!(
            !msgs[0].contains("nope"),
            "the non-map element's value must not reach the wire: {}",
            msgs[0]
        );
        assert_sd_drops(&stats, 0, 1, 0);
    }

    /// `report_encode_stats` maps each of `EncodeStats`'s three structured-data drop counters to
    /// its own `logit.output.structured_data.dropped{reason}` point, tested directly rather than
    /// through `SyslogOutput::send`, which needs a live socket.
    #[test]
    fn report_encode_stats_tags_each_structured_data_drop_reason_separately() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_out", "syslog", "output");
        let stats = EncodeStats {
            dropped_invalid_sd_name: 1,
            dropped_sd_not_map: 1,
            dropped_sd_id_collision: 1,
            ..EncodeStats::default()
        };
        report_encode_stats(&telemetry, &stats);

        let events = registry.drain(0);
        let dropped_for_reason = |reason: &str| -> f64 {
            events
                .iter()
                .filter(|e| e.attributes.get("reason").and_then(|v| v.as_str()) == Some(reason))
                .flat_map(|e| &e.metrics)
                .filter(|m| {
                    logit_core::interner::resolve(m.name) == "logit.output.structured_data.dropped"
                })
                .map(|m| match m.kind {
                    MetricKind::Sum(ref s) => s.value,
                    ref other => panic!("expected a counter, got {other:?}"),
                })
                .sum()
        };
        assert_eq!(dropped_for_reason("invalid_sd_name"), 1.0);
        assert_eq!(dropped_for_reason("not_a_map"), 1.0);
        assert_eq!(dropped_for_reason("sd_id_collision"), 1.0);
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
        use logit_proto::syslog::SyslogDecoder;
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

    // -- Attempt accounting (ADR `sink-send-path-and-attempt-accounting`, decision 2) ----------

    /// A truncated message with its diagnostic, and a skipped metric-only event.
    fn encode_side_batch() -> EventBatch {
        batch_with(vec![log_event(0, &"x".repeat(300), None), metric_event(0)])
    }

    const ENCODE_SIDE: [(&str, &[(&str, &str)]); 4] = [
        ("logit.output.messages.truncated", &[]),
        ("logit.output.events.skipped", &[]),
        ("logit.component.diagnostics", &[("key", "message_truncated")]),
        ("logit.output.batch.bytes", &[]),
    ];

    /// Runs [`encode_side_batch`] through the write loop over the sink `build` makes, once with a
    /// first attempt that fails `Fault::Clean` and once without, with the builders `build_spec`
    /// calls, and compares.
    async fn assert_a_retry_counts_encode_side_once(build: impl Fn(bool) -> SyslogOutput) {
        let mut runs = Vec::new();
        for fail_first in [false, true] {
            let mut probe = TelemetryProbe::new();
            let telemetry = probe.telemetry("out", "syslog_out", "sink");
            let mut output = build(fail_first)
                .with_encoder(SyslogEncoder::new(Format::Rfc5424, 16).with_max_message_bytes(128))
                .with_diagnostics(Diagnostics::new("out").with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry);
            let batches = vec![encode_side_batch()];
            runs.push(
                sums_through_write_loop(
                    &mut output,
                    &mut probe,
                    "syslog_out",
                    batches,
                    fast_retry(),
                )
                .await,
            );
        }
        let (single, retried) = (&runs[0], &runs[1]);
        assert_eq!(sum_of(single, "logit.output.requests", &[("class", "ok")]), 1.0);
        assert_eq!(sum_of(retried, "logit.output.requests", &[("class", "clean")]), 1.0);
        assert_eq!(sum_of(retried, "logit.output.requests", &[("class", "ok")]), 1.0);
        assert_counted_once_per_batch(single, retried, &ENCODE_SIDE, &[]);
    }

    #[tokio::test]
    async fn a_udp_retry_counts_encode_side_counters_once() {
        assert_a_retry_counts_encode_side_once(|fail_first| {
            let steps = fail_first.then_some(SendStep::Fail(std::io::ErrorKind::ConnectionRefused));
            let mut output = SyslogOutput::udp("127.0.0.1:514").unwrap();
            output.conn = Conn::Udp(UdpDest::Scripted(ScriptedDest::new(steps)));
            output
        })
        .await;
    }

    #[tokio::test]
    async fn a_tcp_retry_counts_encode_side_counters_once() {
        assert_a_retry_counts_encode_side_once(|fail_first| {
            let connect = DialStep::Connect(Box::new(FakeStream::new()));
            let steps = if fail_first { vec![DialStep::Refuse, connect] } else { vec![connect] };
            let mut output = SyslogOutput::tcp("127.0.0.1:514", Duration::from_secs(1));
            output.dial_script = Some(Arc::new(ScriptedDial::new(false, steps)));
            output
        })
        .await;
    }

    /// A batch the encoder skips whole returns `Ok` early and still leaves the accounting
    /// disarmed, so later direct sends count.
    #[tokio::test]
    async fn direct_sends_after_a_batch_that_encoded_nothing_count_every_time() {
        let mut probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("out", "syslog_out", "sink");
        let mut output = SyslogOutput::udp("127.0.0.1:514")
            .unwrap()
            .with_encoder(SyslogEncoder::new(Format::Rfc5424, 16).with_max_message_bytes(128))
            .with_diagnostics(Diagnostics::new("out").with_telemetry(telemetry.clone()))
            .with_telemetry(telemetry);
        output.conn = Conn::Udp(UdpDest::Scripted(ScriptedDest::new([])));
        assert_direct_sends_count_after_an_empty_batch(
            &mut output,
            &mut probe,
            "syslog_out",
            batch_with(vec![metric_event(0)]),
            encode_side_batch,
            &ENCODE_SIDE,
        )
        .await;
    }
}
