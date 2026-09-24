//! statsd / DogStatsD egress over UDP, TCP, or a Unix socket, the mirror of `logit_inputs::statsd`. Names, values,
//! and tags round-trip through the real `StatsdDecoder`, which this module's tests pin.
//!
//! A pure [`StatsdEncoder`] (no socket; every grammar, sanitization, and packing test runs against
//! it) plus the thin [`StatsdOutput`] that owns the socket, the `syslog.rs`/`influxdb.rs` split.
//!
//! The encoder implements [`logit_proto::FramedEncoder`], not `logit_proto::Encoder`'s one opaque
//! buffer per batch: UDP packs lines into datagrams, so it needs per-line boundaries.
//! [`StatsdEncoder::encode_into`] fills one [`MessageBuf`] entry per line and reports every drop
//! through [`EncodeStats`] instead of failing (`docs/adr/framed-encoder.md`;
//! [`crate::syslog::SyslogEncoder`] is another implementor). The per-line size cap is encoder
//! state ([`StatsdEncoder::with_max_packet_bytes`], set once by [`StatsdOutput`] for its
//! transport), not a per-call argument, so the trait's one `encode_into` signature fits every
//! implementor.
//!
//! ## Grammar and round-trip contract
//!
//! `<name>:<value>|<type>[|@<sample-rate>][|#<tag>[:<value>],...][|c:<container-id>][|e:<external-data>][|card:<cardinality>][|T<unix-seconds>]`
//! is the grammar `logit_inputs::statsd` parses. When `@` and `|T` appear is under "Sample rate:
//! never for a counter, real for `Samples`" and "`\|c:<container-id>` and `\|T<timestamp>`" below.
//! Every sanitization rule exists because `StatsdDecoder::parse_line` would otherwise misparse the
//! result; that module's grammar doc has the decoder side of each.
//!
//! ## Dialects
//!
//! `Format::DogStatsd` (default) emits the `|#k:v,k:v` tag segment. `Format::Statsd` omits it
//! entirely (not an empty `|#`, which some plain-statsd receivers reject) and counts every tag it
//! drops (`EncodeStats::tags_dropped_dialect`). The unit is a **wire tag**, not an attribute: a
//! multi-value tag (see "Multi-value tags") counts once per element, one `k:v` token each.
//!
//! `Format::Statsd` also normalizes two shapes only DogStatsD can express: a `Samples`
//! multi-value line (`name:v1:v2|ms`) becomes one `name:v|ms` line per value, and a timer's `h`/`d`
//! wire type collapses to `ms` (counted `EncodeStats::type_normalized_dialect`). These are the
//! "sink-configured dialect change" and "splitting a multi-value line" normalizations
//! `docs/adr/lossless-transit.md` permits by name. `|c:<container-id>`, `|e:<external-data>`,
//! `|card:<cardinality>` and `|T<timestamp>` have no plain-statsd equivalent and are dropped, not
//! normalized.
//!
//! ## Sanitization
//!
//! A metric name has every `: | @ # , \n \r \0`, ASCII control character, and whitespace character
//! replaced with `_`. Substitution, not deletion, keeps distinct names distinct (as
//! `syslog.rs::sanitize_5424_field` does). Each forbidden character is a delimiter in the
//! decoder's grammar: `:` splits name from values, `|` splits segments, `@`/`#` open the
//! sample-rate/tag segments, `,` separates tags, `\n` separates lines. Whitespace is substituted
//! defensively: `decode_into` trims only a line's ends, so `my metric:1|c` decodes to the name
//! `"my metric"`, and an embedded space is reachable input this sink still sanitizes
//! (`crates/logit-cli/tests/statsd_round_trip.rs`'s normalization (5)).
//!
//! Tag *keys* forbid the same set as a name, **including `:`**, for a different reason:
//! `parse_line` splits a tag on its *first* colon (`tag.split_once(':')`), so a `:` inside a key
//! would reparse as a shorter key with the remainder folded into the value. Tag *values* forbid
//! the same set **except `:`**: only the first colon is significant, so `env:a:b` round-trips as
//! key `env`, value `a:b`. This key/value asymmetry is easy to get backwards and has its own test.
//! A multi-value tag's key and value are sanitized per element under these same two rules, with no
//! cross-element interaction.
//!
//! A `SetMembers` member has its own, narrower rule ([`is_forbidden_in_set_member`], applied after
//! lossy UTF-8, since a member is arbitrary wire bytes), neither the name/tag-key rule nor the
//! tag-value rule. A member sits in `name:<member>|s`'s *value* position, which `parse_line` splits
//! on `:` to find several values sharing a line (`name:v1:v2|c`), so `:` would re-decode one member
//! as two and `|` would open a new segment. Nothing else there is a delimiter: `values_part` is
//! split only on `:`, and the line only on `|`/newline, so `@`, `#`, `,`, and interior whitespace
//! survive a real decode, and forbidding them would only make distinct members collide. Control
//! bytes (`\n`/`\r`/`\0` included) stay forbidden, since they corrupt line/datagram framing in any
//! position. A member that differs from its raw bytes (invalid UTF-8, or a substituted character)
//! is counted (`EncodeStats::members_sanitized`).
//!
//! ## Multi-value tags (a repeated tag key)
//!
//! A DogStatsD `|#` segment is a **list** of `key[:value]` tokens, not a map: `#team:a,team:b` is
//! two live tags, and a query grouping by `team` places the point in both groups.
//! `logit_inputs::statsd::insert_tags` folds a repeated key into a [`Value::Array`] in wire order
//! (as `syslog_in` does for a repeated PARAM-NAME), and this sink inverts it: an `Array` attribute
//! expands to one wire tag per element, in array order, each through the same [`push_one_tag`] a
//! scalar tag uses. A `Bool(true)` element emits the bare form (`#urgent`), so a bare/valued mix
//! (`#urgent,urgent:1`) relays intact.
//!
//! What it does *not* do:
//!
//! - **No encode-side dedupe.** The Datadog agent's "only exact duplicates are one tag" rule is
//!   applied once, at decode; encode is a pure function of the array. A Lua-authored
//!   `Array[Str("a"), Str("a")]` emits `#k:a,k:a`, which the agent itself dedupes.
//! - **No sorting.** Element order is array order. `syslog_out` canonicalizes SD-ID/PARAM-NAME
//!   order only because `AttrMap` order is process-global intern order; an `Array` carries real
//!   order, and that order is the wire's.
//! - **No element-level union across the merge.** [`build_tag_suffix`]'s merged resource⊕event
//!   walk ([`crate::attrs::merged`]) has the event's value win *whole* on an equal key: an
//!   event-level `Array` replaces a resource-level scalar, and an event-level scalar replaces a
//!   resource-level `Array`. The resource's value was never in *this* event's wire tag list, so an
//!   element-wise union would emit tags nothing sent.
//!
//! An element [`tag_value`] can't render (`Null`, `Bytes`, `Timestamp`, `Map`, a nested `Array`)
//! is skipped and counted `EncodeStats::tags_dropped_unrepresentable` **per element**; an array
//! whose every element drops emits no tag and leaves no stray `,`. A tag named `statsd.type` in the
//! `#` segment can decode to an `Array` too; it matches no [`Carriers`] arm (each expects a
//! `Value::Str`/`Value::U64`) and is filtered out of the tag segment uncounted, like any
//! wrong-typed carrier.
//!
//! ## Metric-kind coverage: raw kinds in, sketches still deferred
//!
//! Encoded: a delta, monotonic `Sum` (`MetricKind::counter`, `|c`), `Gauge`/`GaugeDelta` (`|g`),
//! `Samples` (`|ms`/`|h`/`|d`), and `SetMembers` (`|s`). `Samples`/`SetMembers` are the raw shapes
//! `statsd_in` decodes losslessly (`docs/adr/lossless-transit.md`'s "summarization is opt-in and
//! named"), so a `statsd_in -> statsd_out` relay with no `aggregate` round-trips a timer or set
//! line intact. Dropped and counted (`EncodeStats::dropped_unsupported_kind`, recorded in
//! `docs/known-gaps.md`): `Distribution`, `Set`, `Histogram`, `ExponentialHistogram`, `Summary`,
//! and a cumulative or non-monotonic `Sum`, the kinds that exist only after some stage summarized.
//!
//! **So a `statsd_in -> aggregate -> statsd_out` relay drops every timer/set metric under
//! `aggregate`'s default summarizing config.** That default turns `Samples` into a `Distribution`
//! sketch and `SetMembers` into a `Set` estimate, neither of which has a lossless statsd rendering
//! (`docs/adr/statsd-output.md`'s "What's still deferred" section). Configuring that `aggregate`
//! with `distributions: samples` / `sets: members` keeps the raw shapes flowing to this sink.
//!
//! ## Relative gauges
//!
//! statsd is the one protocol with a native *relative* gauge adjustment (`name:+5|g`/`name:-5|g`),
//! [`logit_core::MetricKind::GaugeDelta`]'s wire origin. By default a `GaugeDelta` reaching this
//! sink is dropped with `influxdb_out`'s message (`gauge_delta_unresolved`): the pipeline is
//! missing an `aggregate`, not carrying a malformed metric. `relative_gauges: true` encodes it
//! natively instead; no other sink's wire format has the concept
//! (`docs/adr/relative-gauge-adjustments.md`).
//!
//! A positive delta needs an explicit `+`: `write!("{}", 5.0)` yields `"5"`, which the decoder
//! reads back as an *absolute* `Gauge`.
//!
//! ## Negative absolute gauges
//!
//! The grammar has no syntax for a negative absolute gauge (`logit_inputs::statsd::build_event`'s
//! `"g"` arm reads *any* leading `-` as a delta), so a naive `Gauge(-5.0)` would render as
//! `name:-5|g` and decode as `GaugeDelta(-5.0)`. This sink emits the idiom Etsy statsd and
//! DogStatsD both document for the case: `name:0|g` immediately followed by `name:-5|g`. The two
//! lines go into the [`MessageBuf`] as **one indivisible entry** (joined by an embedded `\n`) so
//! the packer can never split them across two datagrams: if the first datagram were lost, `-5`
//! would apply to whatever stale value the receiver's gauge held. This is the only entry that
//! contains a newline; every sanitizer above exists to guarantee no other does.
//!
//! `Gauge(-0.0)` is *not* a pair: it is numerically zero, so its sign is normalized away and it
//! renders as `name:0|g`. The naive `name:-0|g` would decode as a no-op `GaugeDelta`, since the
//! decoder dispatches on the leading `-` without parsing the value.
//!
//! ## Packing and framing
//!
//! UDP **packs** several lines into one datagram, up to `max_packet_bytes` (`\n`-joined, no
//! trailing `\n`; the datagram boundary ends the last line). `syslog_out` refuses to pack, since
//! packing there would rely on the receiver splitting on a delimiter its "injection safety"
//! section avoids relying on. Here splitting on `\n` **is** the grammar (every statsd client packs
//! a buffered send this way, and `StatsdDecoder::decode_into` splits on it), and the sanitizers
//! make an embedded `\n` unrepresentable in a name, key, or value, so a packed datagram can't
//! forge an extra metric. A line that would overflow the cap starts a new datagram. A single line
//! longer than the cap is **dropped whole**, never truncated (unlike `syslog_out`): a truncated
//! statsd line decodes as a different metric or a parse error, never a shorter version of itself.
//!
//! TCP terminates **every** line with `\n`, including the last: a stream has no per-batch EOF, so
//! the last line of one batch would otherwise glue onto the first line of the next. No
//! octet-counting: statsd has no such convention and no receiver auto-detects one, unlike syslog's
//! `go-syslog`.
//!
//! The two Unix transports carry **packets**, packed as UDP datagrams are (newline-joined, no
//! trailing newline, at most `max_packet_bytes`):
//!
//! - `transport: unix` sends each packet as one datagram on a socket connected to the path in
//!   `endpoint`, connected lazily on the first send. A receiver whose queue stays full makes the
//!   send wait, not drop (unlike UDP, `AF_UNIX` pushes back on the sender), so each datagram's
//!   wait is bounded by `connect_timeout` and a timeout fails the send like any other socket
//!   error. The socket must be connected: Linux parks a sender on a full receiver, and wakes it
//!   when the receiver drains, only when it's connected. An unconnected sender is reported
//!   writable again right after each `EAGAIN`, so behind other clients' datagrams it retries in a
//!   busy loop. A connected socket follows the receiver's socket, not the path, so a
//!   timeout, `ECONNREFUSED`, or `ENOTCONN` drops it and the next send connects to whatever is
//!   at the path then; the first datagram of a batch gets one immediate reconnect-and-retry, so
//!   a receiver restart costs no batch (ADR `datadog-agent-and-intake-relay`, decision 12).
//! - `transport: unix_stream` writes each packet after its length as a 4-byte little-endian
//!   integer (the Agent's `dogstatsd_stream_socket` framing; UNVERIFIED, `docs/known-gaps.md`) on
//!   one connection, with everything [`StatsdOutput::send_tcp`] says about TCP's plaintext arm:
//!   the lazy connect, the probe of a reused connection, the one reconnect after a zero-byte
//!   failure, and the flush before a batch is called delivered.
//!
//! ## TLS
//!
//! `transport: tcp` optionally runs over TLS ([`StatsdOutput::with_tls`]) in `syslog_out`'s
//! arrangement; a Unix socket never does (graph rule 64) (`docs/adr/statsd-output.md`'s "Amendment: TLS" section). A `tls:` block's presence
//! turns TLS on and makes it *required*: `endpoint` is a bare `host:port` with no scheme to carry
//! the signal, so there is no plaintext fallback. No statsd client in the wild speaks TLS, so this
//! is for a `logit`-to-`logit` (or stunnel-shaped) relay hop. DTLS is out of scope, so `tls:` under
//! `transport: udp` is a config error (`logit-pipeline::graph::resolve`'s rule 52) as well as an
//! error here. Every connect after the first counts `logit.output.reconnects`, on TLS and plaintext
//! alike.
//!
//! TLS changes this sink's fault classification: a `tokio_rustls` write's `Ok` means "the session
//! accepted these bytes", not "the kernel has them", and its `Err` is never proof that nothing
//! left the host. [`StatsdOutput::send_tcp`]'s doc has the per-transport rules that follow: no
//! internal retry and no `Fault::Clean` after an application write on TLS, and a mandatory flush
//! before any batch is called delivered.
//!
//! ## Sample rate: never for a counter, real for `Samples`
//!
//! A delta, monotonic `Sum` (`MetricKind::counter`) already has its sample rate divided out at
//! decode, so this sink never emits `@<rate>` for `|c`: that would double-extrapolate downstream.
//! `statsd_in` keeps timer/histogram samples raw instead, so `Samples.sample_rate` is un-applied
//! information a lossless relay must carry: `@<rate>` is emitted whenever it isn't `1.0` (see
//! [`render_metric`]'s `Samples` arm). `MetricRecord::unit` has no statsd wire representation and
//! is dropped.
//!
//! ## `|c:<container-id>` and `|T<timestamp>`
//!
//! Both are DogStatsD-only line extensions this sink round-trips. `statsd.container_id` (a
//! `Value::Str` `statsd_in` stamps from an incoming `|c:<id>`) renders as `|c:<id>`, sanitized like
//! a tag value so an embedded `:` survives. `statsd.timestamp == Value::U64(secs)` (the raw wire
//! seconds `statsd_in` stamps alongside setting `Event::timestamp`) renders as `|T<secs>` **from
//! the carrier, never from `event.timestamp`**: a stage that rebuilds `Event::timestamp` after
//! decode (`aggregate`'s flush) can't fabricate or collapse a `|T`, since the carrier rides on the
//! series key like any other attribute. A `statsd.timestamp` that isn't a `Value::U64` (reachable
//! from a cross-protocol relay or Lua, never from `statsd_in`) isn't emitted.
//!
//! `statsd.external_data` and `statsd.cardinality` (DogStatsD v1.5's `|e:` and v1.6's `|card:`)
//! round-trip the same way as `|c:`: a `Value::Str` carrier. Cardinality is sanitized like a tag
//! value; external data keeps its own `,` separators ([`is_forbidden_in_external_data`]).
//!
//! On a metric line the segments follow the tag segment, in the order `|c:`, `|e:`, `|card:`, `|T`
//! (`append_dialect_extras`); on an event or service-check line `c:`, `e:`, `card:` follow the tags
//! and precede `m:` (`append_origin_fields`). That order is UNVERIFIED against a real DogStatsD
//! client (`docs/known-gaps.md`); the decoder accepts any order. Under `Format::Statsd` none has
//! anywhere to go, so each is dropped and counted (`EncodeStats::dropped_dialect_fields`).
//!
//! `statsd.*` attributes (`statsd.type`, `statsd.container_id`, `statsd.timestamp`, and the
//! rest) are protocol-namespaced carriers, not tags: `build_tag_suffix` filters the prefix out of
//! the `|#k:v,...` segment, as `syslog_out` never re-emits its own `syslog.*` attributes as
//! SD-ELEMENT fields. The same merged resource⊕event walk captures every carrier into
//! [`EncodeCtx`], so a
//! carrier set only on the resource (a `set` transform's `resource:` block, say) is honored like
//! an event-level one. `append_dialect_extras`/`statsd_wire_type` read `EncodeCtx`, never
//! `event.attributes`, which keeps the filter and the read symmetric.
//!
//! ## DogStatsD events and service checks
//!
//! An **event** is `event.log.is_some()` *and* `statsd.event.title` present as a `Value::Str` (the
//! shape `statsd_in`'s `parse_event` produces), the event's value winning over the resource's as
//! in `crate::attrs::merged`. A **service check** is `statsd.service_check.name` present as a
//! `Value::Str`. [`StatsdEncoder::encode_into`] detects them in two tiers, split on
//! `event.metrics.is_empty()`, because most metrics-empty events are plain log/span lines with no
//! `statsd.*` carrier at all:
//!
//! - A metrics-empty event gets one *unmerged* [`is_dogstatsd_event`] lookup (event attributes,
//!   then resource). The common "not an event" case skips [`build_tag_suffix`]'s full merged walk
//!   and counts only `skipped_no_metrics`, without inflating `tags_dropped_dialect` for tags
//!   nothing would render. Only a "yes" runs the full walk, for the other carriers the line needs.
//! - An event with metrics (a service check, or an ordinary metric event) always runs the full
//!   walk, and service-check detection reads `Carriers::service_check_name` off it.
//!
//! Either way the full walk into [`Carriers`] is where `statsd.type`/`statsd.container_id`/
//! `statsd.timestamp` are captured too, so `event.attributes` is never read a second time.
//!
//! **Event wire form**, one line: `_e{tlen,xlen}:title|text`, then, in this order and only when
//! present and valid: `|d:<secs>` (from the `statsd.timestamp` carrier; `d:`, never `|T`, since an
//! event's timestamp is a named field of its own grammar), `|h:<host>`, `|p:<priority>`
//! (`normal`/`low` only), `|t:<alert_type>` (`info`/`success`/`warning`/`error` only),
//! `|k:<aggregation_key>`, `|s:<source_type>`, the `|#...` tag segment (via
//! [`build_tag_suffix`]/[`append_tags`], as for a metric line), then `|c:<container id>`,
//! `|e:<external data>`, `|card:<cardinality>`. `title`
//! is `statsd.event.title`; `text` is the log `message`, which must be a `Value::Str` (anything
//! else drops the event, counted `EncodeStats::dropped_unencodable_value`), with every real `\n`
//! escaped to the two bytes `\n` (`statsd_in`'s `unescape_event_text` is the mirror). `tlen`/`xlen`
//! are the *byte* lengths of the sanitized title and escaped text: `parse_event` slices by those
//! byte counts, so a char count would corrupt a multi-byte title or text. Severity is never
//! re-derived from `LogRecord.severity` to synthesize a missing `t:` field: under rule (b)
//! (`docs/adr/lossless-transit.md`) the raw carrier outranks the normalized field on this
//! protocol's own egress, and an absent carrier means an absent wire field.
//!
//! **Service-check wire form**, one line: `_sc|<name>|<status>`, then `|d:<secs>`, `|h:<host>`,
//! the `|#...` tag segment, `|c:<container id>`, `|e:<external data>`, `|card:<cardinality>`, and
//! always last `|m:<message>`, since `m:`
//! consumes the rest of the line on decode. The event's **first** metric is the check and must be a
//! `Gauge`; otherwise the whole event is dropped and counted
//! (`EncodeStats::dropped_invalid_service_check`) rather than falling through to a `name:v|g`
//! line, since the point is the check, not a gauge that shares its value. `name` is the
//! `statsd.service_check.name` carrier, **not** the metric's normalized name (rule (b) again),
//! sanitized with [`is_forbidden_in_extended_field`] rather than [`is_forbidden_in_name`], so it
//! keeps `.` and spaces as sent. `status` is `statsd.service_check.status` when it's a `U64` in
//! `0..=3`, else the gauge's value if finite and rounding into `0..=3`, else the check is dropped
//! and counted (`dropped_invalid_service_check`) rather than written with a status DogStatsD's own
//! decoder would reject. Metrics after the first render as ordinary lines (`render_metric`) right
//! after the `_sc` line.
//!
//! **Sanitization**, one rule per field, all substitution:
//!
//! - Event title: control bytes -> `_` ([`is_forbidden_in_event_title`]). A bare `|` is fine; the
//!   length prefix delimits the field.
//! - Event text: a real newline becomes the two-byte escape `\n`; any other control byte -> `_`
//!   ([`append_event_text`]).
//! - Event host/aggregation key/source, service-check name/host: `|` and control bytes -> `_`
//!   ([`is_forbidden_in_extended_field`]). Each sits in a `|letter:value` field a decode ends at
//!   the next `|`, as for a tag value, but none has a tag value's colon-splitting ambiguity.
//! - Event priority/alert type: never sanitized. A value in the fixed set is written verbatim; any
//!   other is omitted and counted `EncodeStats::dropped_invalid_event_fields`, since an
//!   out-of-set value has no sanitized form that means the same thing. The line is still emitted,
//!   so this is its own counter rather than `dropped_unencodable_value`, which reports a message
//!   drop.
//! - Service-check message: control bytes (a real newline included) -> `_`, with no escape; `|`
//!   is left alone since `m:` is last ([`is_forbidden_in_service_check_message`]).
//!
//! **`Format::Statsd` has no wire form for either shape** (no `_e`/`_sc` sigil), so the whole
//! event is dropped and counted (`EncodeStats::dropped_dialect_events`) before anything else about
//! it is inspected; a service check's gauge isn't emitted as `name:v|g` either. Either shape
//! dropped this way has already run the full merged walk, so its ordinary attributes are also
//! tallied into `tags_dropped_dialect`: both counters incrementing for one dropped line is
//! expected, not a double count.
//!
//! `statsd.event.*`/`statsd.service_check.*` are carriers like `statsd.type`: the same
//! `key_str.starts_with("statsd.")` check in [`build_tag_suffix`] keeps them out of every line's
//! `|#k:v,...` segment.

use crate::influxdb::{push_float, tag_value};
// Shared with `syslog_out`/`logit_out`, which dial the same bare `host:port`, optionally
// TLS-wrapped. `AsyncStream` lets `Conn::Tcp` hold either without `StatsdOutput` becoming generic;
// `host_only` derives the SNI name from an endpoint with no scheme.
use crate::tls::{host_only, poll_pending_close, AsyncStream, PendingClose};
use crate::Output;
use anyhow::Context;
use logit_core::{
    Diagnostics, Event, EventBatch, MetricKind, MetricRecord, Resource, Telemetry, Temporality,
    Value,
};
use logit_pipeline::Fault;
use logit_proto::{FramedEncoder, MessageBuf};
use rustls_pki_types::ServerName;
use std::fmt::Write as _;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{lookup_host, TcpStream, UdpSocket, UnixDatagram, UnixStream};
use tokio_rustls::TlsConnector;

/// Re-exported for symmetry with `crate::syslog`'s and `crate::logit`'s paths; every TLS-dialing
/// sink shares the one definition in `crate::tls`.
pub use crate::tls::TlsClientSettings;

/// Bounds one **datagram** (several packed lines), not one line. Etsy statsd's "commodity
/// Ethernet LAN" recommendation and DataDog's DogStatsD client default: 1500 MTU minus IPv4/UDP
/// headers minus ~40 bytes of headroom for VXLAN/IPsec encapsulation, where a 1472-byte datagram
/// would fragment or fail with `EMSGSIZE`. DataDog's loopback/UDS figure (8192, `syslog_out`'s
/// `DEFAULT_MAX_MESSAGE_BYTES`) assumes a local destination; this doesn't.
pub const DEFAULT_MAX_PACKET_BYTES: usize = 1432;

/// TCP only; `syslog::DEFAULT_CONNECT_TIMEOUT`'s value. `logit-config`'s
/// `default_statsd_connect_timeout` hardcodes the same 5 seconds, kept in sync by hand.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Which statsd dialect [`StatsdEncoder`] emits. Its own enum rather than
/// `logit_config::StatsdFormat` because `logit-outputs` never depends on `logit-config`
/// (`docs/design/pipeline-graph.md`'s "Crate layout" section); `logit-cli::pipeline::build_spec`
/// is the one place a config value crosses into this type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    DogStatsd,
    Statsd,
}

/// Per-batch outcome counts from [`StatsdEncoder::encode_into`], this sink's
/// [`FramedEncoder::Stats`]; `StatsdOutput::send` turns them into `logit.output.*` telemetry
/// (`docs/design/internal-telemetry.md`).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EncodeStats {
    /// Events with no metrics (log-only or span-only, legal under
    /// `docs/adr/multi-payload-events.md`), the same skip `influxdb_out` makes.
    pub skipped_no_metrics: usize,
    pub dropped_gauge_delta: usize,
    /// A `NO_RECORDED_VALUE`-flagged point. statsd has no "no value here" concept, so unlike
    /// `otlp_out` this sink can't keep the point flagged; it drops it rather than write its default
    /// value as a fabricated sample (`docs/known-gaps.md`'s cross-protocol table).
    pub dropped_no_recorded_value: usize,
    pub dropped_unsupported_kind: usize,
    /// One bucket for every kind of unencodable input: a non-finite value in a
    /// `Samples`/`Sum`/`Gauge`/`GaugeDelta`; an out-of-range `Samples.sample_rate` (the line is
    /// still written, without `@rate`); an empty `Samples`/`SetMembers` record; or an event whose
    /// log `message` isn't a UTF-8 `Value::Str`, or whose `statsd.event.title` isn't valid UTF-8
    /// ([`is_dogstatsd_event`] accepts any `Value::Str`), which drops the whole event
    /// ([`render_event`]). An out-of-set event priority/alert type still emits its line, so it
    /// counts in [`Self::dropped_invalid_event_fields`] instead.
    pub dropped_unencodable_value: usize,
    pub dropped_empty_name: usize,
    pub dropped_oversize_line: usize,
    /// Tags dropped because [`Format::Statsd`] has no tag segment. Counted **per wire tag**: an
    /// `Array` attribute counts once per element, and an empty `Array` counts nothing. See the
    /// module doc's "Dialects" section.
    pub tags_dropped_dialect: usize,
    /// A tag [`tag_value`] can't render (`Null`, `Bytes`, `Timestamp`, `Map`, a nested `Array`),
    /// or whose key is empty or sanitizes to nothing. Counted **per wire tag**, as
    /// [`Self::tags_dropped_dialect`] is. See the module doc's "Multi-value tags" section.
    pub tags_dropped_unrepresentable: usize,
    /// A timer's `h`/`d` wire type (`statsd.type`) collapsed to `ms` under `Format::Statsd`.
    /// Counted once per `Samples` record, not per split line.
    pub type_normalized_dialect: usize,
    /// `statsd.container_id`/`statsd.external_data`/`statsd.cardinality`/`statsd.timestamp`
    /// dropped under `Format::Statsd`
    /// (`append_dialect_extras`). Counted once per field per emitted line, so a negative-gauge pair
    /// or a multi-member `SetMembers` record counts once per physical line.
    pub dropped_dialect_fields: usize,
    /// A `SetMembers` member that differs from its raw bytes after lossy UTF-8 and
    /// [`is_forbidden_in_set_member`]'s substitution. See the module doc's "Sanitization" section.
    pub members_sanitized: usize,
    /// An event or service check dropped whole under [`Format::Statsd`], which has no `_e`/`_sc`
    /// wire form. See the module doc's "DogStatsD events and service checks" section.
    pub dropped_dialect_events: usize,
    /// A service check whose first metric isn't a `Gauge`, or with no status in `0..=3` from either
    /// the `statsd.service_check.status` carrier or the gauge's rounded value. See
    /// [`render_service_check`].
    pub dropped_invalid_service_check: usize,
    /// An event priority/alert type outside its fixed set: that one field is omitted and the line
    /// is still emitted. Not `dropped_unencodable_value`, whose
    /// `logit.output.messages.dropped{reason="unencodable_value"}` would claim the whole message
    /// was dropped. See [`render_event`].
    pub dropped_invalid_event_fields: usize,
}

/// Encodes events as statsd lines. Pure: no socket, so every grammar, sanitization, and packing
/// test runs against it directly.
pub struct StatsdEncoder {
    format: Format,
    relative_gauges: bool,
    /// The longest line this encoder emits. A longer line is dropped whole
    /// (`EncodeStats::dropped_oversize_line`), never truncated, since the UDP packer
    /// (`StatsdOutput::send_udp`) could never fit it in a datagram. `usize::MAX` (the default,
    /// and TCP's) is uncapped. `StatsdOutput` sets it once for its transport.
    max_packet_bytes: usize,
    diag: Diagnostics,
    /// The current event's `|#k:v,k:v` tag segment, built once and shared across its metrics
    /// (`influxdb.rs::render_tag_suffix`'s split).
    tag_suffix: String,
    /// One rendered line, or a negative-gauge pair joined by `\n`. Cleared per metric, never
    /// reallocated (`syslog.rs::SyslogEncoder::line`'s discipline).
    line: String,
    /// The current metric's sanitized name; a field, not a local, so it isn't reallocated per
    /// metric.
    name: String,
    /// The current `SetMembers` member's sanitized text; its own field because `name` is live at
    /// the same time.
    member: String,
    /// Scratch for [`tag_value`]'s non-`Str` formatting. Each use is copied into `tag_suffix` and
    /// cleared before the next.
    scratch: String,
    /// The current event's sanitized title. Never live alongside `name`/`member`: an event line
    /// and a metric line never render from the same call.
    title_buf: String,
    /// The current event's escaped text. Its own buffer because the `_e{tlen,xlen}` header needs
    /// both lengths before either field is written into `line`.
    text_buf: String,
}

impl StatsdEncoder {
    pub fn new(format: Format) -> Self {
        Self {
            format,
            relative_gauges: false,
            max_packet_bytes: usize::MAX,
            diag: Diagnostics::default(),
            tag_suffix: String::new(),
            line: String::new(),
            name: String::new(),
            member: String::new(),
            scratch: String::new(),
            title_buf: String::new(),
            text_buf: String::new(),
        }
    }

    pub fn with_relative_gauges(mut self, relative_gauges: bool) -> Self {
        self.relative_gauges = relative_gauges;
        self
    }

    /// Caps the longest line this encoder emits; `usize::MAX` means uncapped.
    pub fn with_max_packet_bytes(mut self, max_packet_bytes: usize) -> Self {
        self.max_packet_bytes = max_packet_bytes;
        self
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }
}

impl FramedEncoder for StatsdEncoder {
    /// A statsd line is self-describing; the UDP packer needs only each entry's bytes.
    type Meta = ();
    type Stats = EncodeStats;

    /// Encodes every event in `batch` into `out` (cleared first). Never fails: a per-metric
    /// problem (an unsupported kind, an unresolved delta, a non-finite value, an oversize line) is
    /// a drop counted in the returned [`EncodeStats`].
    fn encode_into(&mut self, batch: &EventBatch, out: &mut MessageBuf) -> EncodeStats {
        out.clear();
        let mut stats = EncodeStats::default();
        for event in &batch.events {
            if event.metrics.is_empty() {
                // Usually a plain log/span line, rarely a DogStatsD event; telling them apart
                // needs only `statsd.event.title`, not the full merged walk (module doc's
                // "DogStatsD events and service checks" section).
                if !is_dogstatsd_event(&batch.resource, event) {
                    stats.skipped_no_metrics += 1;
                    continue;
                }

                let mut carriers = Carriers::default();
                build_tag_suffix(
                    &mut self.tag_suffix,
                    &mut self.scratch,
                    self.format,
                    &batch.resource,
                    event,
                    &mut stats,
                    &mut carriers,
                );

                if self.format == Format::Statsd {
                    // No `Format::Statsd` wire form. The walk above already counted this event's
                    // attributes into `tags_dropped_dialect`; both counters rising is expected.
                    stats.dropped_dialect_events += 1;
                    continue;
                }

                let mut ctx = EncodeCtx {
                    format: self.format,
                    tag_suffix: &self.tag_suffix,
                    statsd_type: carriers.statsd_type,
                    container_id: carriers.container_id,
                    external_data: carriers.external_data,
                    cardinality: carriers.cardinality,
                    timestamp_secs: carriers.timestamp_secs,
                    max_packet_bytes: self.max_packet_bytes,
                    stats: &mut stats,
                    diag: &mut self.diag,
                    out: &mut *out,
                };
                render_event(
                    &mut self.line,
                    &mut self.title_buf,
                    &mut self.text_buf,
                    &carriers,
                    event,
                    &mut ctx,
                );
                continue;
            }

            // An event with metrics always runs the full walk; service-check detection reads
            // `carriers` with no extra lookup.
            let mut carriers = Carriers::default();
            build_tag_suffix(
                &mut self.tag_suffix,
                &mut self.scratch,
                self.format,
                &batch.resource,
                event,
                &mut stats,
                &mut carriers,
            );

            let is_service_check = carriers.service_check_name.is_some();

            if is_service_check && self.format == Format::Statsd {
                // No `Format::Statsd` wire form; drop before inspecting anything else.
                stats.dropped_dialect_events += 1;
                continue;
            }

            let mut ctx = EncodeCtx {
                format: self.format,
                tag_suffix: &self.tag_suffix,
                statsd_type: carriers.statsd_type,
                container_id: carriers.container_id,
                external_data: carriers.external_data,
                cardinality: carriers.cardinality,
                timestamp_secs: carriers.timestamp_secs,
                max_packet_bytes: self.max_packet_bytes,
                stats: &mut stats,
                diag: &mut self.diag,
                out: &mut *out,
            };

            if is_service_check {
                match event.metrics.first() {
                    Some(first) if matches!(first.kind, MetricKind::Gauge(_)) => {
                        render_service_check(&mut self.line, &carriers, first, &mut ctx);
                        for metric in &event.metrics[1..] {
                            render_metric(
                                &mut self.line,
                                &mut self.name,
                                &mut self.member,
                                self.relative_gauges,
                                metric,
                                &mut ctx,
                            );
                        }
                    }
                    _ => {
                        ctx.stats.dropped_invalid_service_check += 1;
                        ctx.diag.warn_throttled(
                            "invalid_service_check",
                            format_args!(
                                "statsd_out: service check {:?} has no Gauge as its first \
                                 metric; dropping",
                                carriers.service_check_name.unwrap_or_default()
                            ),
                        );
                    }
                }
                continue;
            }

            for metric in &event.metrics {
                render_metric(
                    &mut self.line,
                    &mut self.name,
                    &mut self.member,
                    self.relative_gauges,
                    metric,
                    &mut ctx,
                );
            }
        }
        stats
    }
}

/// Whether a metrics-empty event is a DogStatsD event: a log carrying `statsd.event.title` as a
/// `Value::Str`, the event's value winning over the resource's as in `crate::attrs::merged`. At
/// most two `AttrMap::get` lookups (each a binary search, not a scan), so a plain log line skips
/// [`build_tag_suffix`]'s walk over every attribute. Gates only the metrics-empty path of
/// [`StatsdEncoder::encode_into`].
fn is_dogstatsd_event(resource: &Resource, event: &Event) -> bool {
    if event.log.is_none() {
        return false;
    }
    let title = event
        .attributes
        .get("statsd.event.title")
        .or_else(|| resource.attributes.get("statsd.event.title"));
    matches!(title, Some(Value::Str(_)))
}

/// The per-event context [`render_metric`] and its helpers need but don't own, as one borrow
/// instead of a long parameter list. `statsd_type`/`container_id`/`external_data`/`cardinality`/
/// `timestamp_secs` come from [`build_tag_suffix`]'s merged resource⊕event walk; [`append_dialect_extras`]/
/// [`statsd_wire_type`] read them here, never from `event.attributes`, so a resource-only carrier
/// is honored.
struct EncodeCtx<'a> {
    format: Format,
    tag_suffix: &'a str,
    statsd_type: Option<&'a str>,
    container_id: Option<&'a str>,
    external_data: Option<&'a str>,
    cardinality: Option<&'a str>,
    timestamp_secs: Option<u64>,
    max_packet_bytes: usize,
    stats: &'a mut EncodeStats,
    diag: &'a mut Diagnostics,
    out: &'a mut MessageBuf,
}

/// Out-parameter for [`build_tag_suffix`]'s merged walk: the `statsd.*` carriers, captured as
/// they're filtered out of the tag segment, so the read and the filter stay symmetric. Separate
/// from [`EncodeCtx`] because `EncodeCtx::tag_suffix` borrows the `String` `build_tag_suffix` still
/// holds `&mut` while capturing.
#[derive(Debug, Default, Clone, Copy)]
struct Carriers<'a> {
    statsd_type: Option<&'a str>,
    container_id: Option<&'a str>,
    external_data: Option<&'a str>,
    cardinality: Option<&'a str>,
    timestamp_secs: Option<u64>,
    /// `statsd.event.title`, present only if valid UTF-8. [`is_dogstatsd_event`] detects an event
    /// by the raw attribute, so this can be `None` on a detected event.
    event_title: Option<&'a str>,
    event_priority: Option<&'a str>,
    event_alert_type: Option<&'a str>,
    event_aggregation_key: Option<&'a str>,
    event_source_type: Option<&'a str>,
    event_host: Option<&'a str>,
    /// `statsd.service_check.name`; its presence is the service-check detection rule.
    service_check_name: Option<&'a str>,
    service_check_status: Option<u64>,
    service_check_message: Option<&'a str>,
    service_check_host: Option<&'a str>,
}

/// Builds this event's DogStatsD tag segment into `suffix` (cleared first, **no** leading `|#`;
/// [`append_tags`] adds that only if `suffix` is non-empty), walking [`crate::attrs::merged`]
/// (event attributes override the resource's on key collision). Under [`Format::Statsd`] `suffix`
/// stays empty and every would-be wire tag counts into `stats.tags_dropped_dialect`.
///
/// Each tag goes through [`push_one_tag`], once per scalar attribute and once per **element** of a
/// `Value::Array` (module doc's "Multi-value tags"). The same walk captures the `statsd.*` carriers
/// into `carriers`.
fn build_tag_suffix<'a>(
    suffix: &mut String,
    scratch: &mut String,
    format: Format,
    resource: &'a Resource,
    event: &'a Event,
    stats: &mut EncodeStats,
    carriers: &mut Carriers<'a>,
) {
    suffix.clear();
    for (key, value) in crate::attrs::merged(resource, event) {
        let key_str = logit_core::interner::resolve(key);
        // `statsd.*` attributes are protocol carriers `statsd_in` stamps from a line's own
        // segments, emitted through their own segments, so never also as tags. Not counted as a
        // drop: the carrier is being used, not lost (`syslog_out` filters `syslog.*` the same way).
        if key_str.starts_with("statsd.") {
            match (key_str, value) {
                ("statsd.type", Value::Str(s)) => {
                    carriers.statsd_type = std::str::from_utf8(s).ok();
                }
                ("statsd.container_id", Value::Str(s)) => {
                    carriers.container_id = std::str::from_utf8(s).ok();
                }
                ("statsd.external_data", Value::Str(s)) => {
                    carriers.external_data = std::str::from_utf8(s).ok();
                }
                ("statsd.cardinality", Value::Str(s)) => {
                    carriers.cardinality = std::str::from_utf8(s).ok();
                }
                ("statsd.timestamp", Value::U64(secs)) => {
                    carriers.timestamp_secs = Some(*secs);
                }
                ("statsd.event.title", Value::Str(s)) => {
                    carriers.event_title = std::str::from_utf8(s).ok();
                }
                ("statsd.event.priority", Value::Str(s)) => {
                    carriers.event_priority = std::str::from_utf8(s).ok();
                }
                ("statsd.event.alert_type", Value::Str(s)) => {
                    carriers.event_alert_type = std::str::from_utf8(s).ok();
                }
                ("statsd.event.aggregation_key", Value::Str(s)) => {
                    carriers.event_aggregation_key = std::str::from_utf8(s).ok();
                }
                ("statsd.event.source_type", Value::Str(s)) => {
                    carriers.event_source_type = std::str::from_utf8(s).ok();
                }
                ("statsd.event.host", Value::Str(s)) => {
                    carriers.event_host = std::str::from_utf8(s).ok();
                }
                ("statsd.service_check.name", Value::Str(s)) => {
                    carriers.service_check_name = std::str::from_utf8(s).ok();
                }
                ("statsd.service_check.status", Value::U64(v)) => {
                    carriers.service_check_status = Some(*v);
                }
                ("statsd.service_check.message", Value::Str(s)) => {
                    carriers.service_check_message = std::str::from_utf8(s).ok();
                }
                ("statsd.service_check.host", Value::Str(s)) => {
                    carriers.service_check_host = std::str::from_utf8(s).ok();
                }
                // A wrong-typed carrier or an unknown `statsd.*` key is ignored, not an error.
                _ => {}
            }
            continue;
        }

        if format == Format::Statsd {
            // Per wire tag: an `Array` counts once per element, an empty one not at all.
            stats.tags_dropped_dialect += match value {
                Value::Array(elements) => elements.len(),
                _ => 1,
            };
            continue;
        }

        match value {
            // A repeated tag key, folded at decode; one token per element, in array order, no
            // dedupe, no sorting (module doc's "Multi-value tags").
            Value::Array(elements) => {
                for element in elements {
                    push_one_tag(suffix, scratch, key_str, element, stats);
                }
            }
            _ => {
                push_one_tag(suffix, scratch, key_str, value, stats);
            }
        }
    }
}

/// Appends one `key[:value]` wire tag to `suffix`, comma-separated, returning whether it did. On
/// an empty key, a key that sanitizes to nothing, or a `value` [`tag_value`] can't render, it
/// counts `stats.tags_dropped_unrepresentable` and leaves `suffix` **byte-identical to entry**,
/// separator included, so an array whose every element drops leaves no stray `,`.
///
/// Scalar tags and each `Array` element share this, so both get the same grammar, sanitization,
/// and counting. `scratch` is copied into `suffix` before this returns, which keeps
/// [`StatsdEncoder::scratch`] single-use across a multi-element array.
fn push_one_tag(
    suffix: &mut String,
    scratch: &mut String,
    key_str: &str,
    value: &Value,
    stats: &mut EncodeStats,
) -> bool {
    if key_str.is_empty() {
        stats.tags_dropped_unrepresentable += 1;
        return false;
    }
    // `Bool(true)` is what `logit_inputs::statsd::parse_line` produces for a bare tag
    // (`#urgent`). Emitting `key:true` would round-trip as `Value::Str("true")`.
    let bare = matches!(value, Value::Bool(true));
    let rendered_value = if bare { None } else { tag_value(scratch, value) };
    if !bare && rendered_value.is_none() {
        stats.tags_dropped_unrepresentable += 1;
        return false;
    }

    let restore_to = suffix.len();
    if !suffix.is_empty() {
        suffix.push(',');
    }
    let key_start = suffix.len();
    sanitize_into(suffix, key_str, is_forbidden_in_tag_key);
    if suffix.len() == key_start {
        // Unreachable while substitution never shortens a non-empty key; a defensive guard that
        // rewinds to entry, separator included.
        suffix.truncate(restore_to);
        stats.tags_dropped_unrepresentable += 1;
        return false;
    }
    if let Some(v) = rendered_value {
        suffix.push(':');
        sanitize_into(suffix, v, is_forbidden_in_tag_value_only);
    }
    true
}

/// Appends the `|#`-prefixed tag segment if `tag_suffix` is non-empty.
fn append_tags(line: &mut String, tag_suffix: &str) {
    if !tag_suffix.is_empty() {
        line.push_str("|#");
        line.push_str(tag_suffix);
    }
}

/// Appends `|c:<container-id>`, `|e:<external-data>`, `|card:<cardinality>`, then
/// `|T<timestamp>` under [`Format::DogStatsd`], or counts each present field into
/// `EncodeStats::dropped_dialect_fields` under [`Format::Statsd`]. Called after [`append_tags`] on
/// every physical metric line, both lines of a negative-gauge pair included, since each is its own
/// statsd line on the wire. `|T` is the carrier's wire seconds, never `event.timestamp` (module
/// doc's "`|c:<container-id>` and `|T<timestamp>`").
fn append_dialect_extras(line: &mut String, ctx: &mut EncodeCtx) {
    match ctx.format {
        Format::DogStatsd => {
            append_origin_fields(line, ctx);
            if let Some(secs) = ctx.timestamp_secs {
                let _ = write!(line, "|T{secs}");
            }
        }
        Format::Statsd => {
            let present = [
                ctx.container_id.is_some(),
                ctx.external_data.is_some(),
                ctx.cardinality.is_some(),
                ctx.timestamp_secs.is_some(),
            ];
            ctx.stats.dropped_dialect_fields += present.iter().filter(|&&p| p).count();
        }
    }
}

/// Appends `|c:<container-id>|e:<external-data>|card:<cardinality>`, each only when its carrier is
/// present. The container id and cardinality are sanitized like a tag value, the external data by
/// [`is_forbidden_in_external_data`]. The metric-line tail before `|T`, and the whole tail of an
/// event or service-check line before `|m:`. **Never `|T`** on those two shapes, since their
/// timestamp already went out as `d:<secs>`. No dialect accounting: [`append_dialect_extras`]
/// does that for a metric line, and [`StatsdEncoder::encode_into`] drops both other shapes under
/// `Format::Statsd` before rendering.
fn append_origin_fields(line: &mut String, ctx: &EncodeCtx) {
    if let Some(id) = ctx.container_id {
        line.push_str("|c:");
        sanitize_into(line, id, is_forbidden_in_tag_value_only);
    }
    if let Some(data) = ctx.external_data {
        line.push_str("|e:");
        sanitize_into(line, data, is_forbidden_in_external_data);
    }
    if let Some(cardinality) = ctx.cardinality {
        line.push_str("|card:");
        sanitize_into(line, cardinality, is_forbidden_in_tag_value_only);
    }
}

/// Renders one `_e{tlen,xlen}:title|text[...]` event line and pushes it. Drops and counts
/// `EncodeStats::dropped_unencodable_value` if the log `message` or the title isn't UTF-8 text
/// ([`is_dogstatsd_event`] accepts a non-UTF-8 title, so `carriers.event_title` can be `None`).
/// Field order and sanitization: the module doc's "DogStatsD events and service checks" section.
fn render_event(
    line: &mut String,
    title_buf: &mut String,
    text_buf: &mut String,
    carriers: &Carriers,
    event: &Event,
    ctx: &mut EncodeCtx,
) {
    let Some(title) = carriers.event_title else {
        ctx.stats.dropped_unencodable_value += 1;
        ctx.diag.warn_throttled(
            "unencodable_value",
            format_args!("statsd_out: event has a non-UTF-8 title; dropping"),
        );
        return;
    };
    let log = event.log.as_ref().expect("caller only calls this when event.log is Some");

    let Value::Str(raw) = &log.message else {
        ctx.stats.dropped_unencodable_value += 1;
        ctx.diag.warn_throttled(
            "unencodable_value",
            format_args!("statsd_out: event {title:?} has a non-string log message; dropping"),
        );
        return;
    };
    let Ok(raw_text) = std::str::from_utf8(raw) else {
        ctx.stats.dropped_unencodable_value += 1;
        ctx.diag.warn_throttled(
            "unencodable_value",
            format_args!("statsd_out: event {title:?} has a non-UTF-8 log message; dropping"),
        );
        return;
    };

    title_buf.clear();
    sanitize_into(title_buf, title, is_forbidden_in_event_title);
    text_buf.clear();
    append_event_text(text_buf, raw_text);

    line.clear();
    let _ = write!(line, "_e{{{},{}}}:", title_buf.len(), text_buf.len());
    line.push_str(title_buf);
    line.push('|');
    line.push_str(text_buf);

    if let Some(secs) = ctx.timestamp_secs {
        let _ = write!(line, "|d:{secs}");
    }
    if let Some(host) = carriers.event_host {
        line.push_str("|h:");
        sanitize_into(line, host, is_forbidden_in_extended_field);
    }
    match carriers.event_priority {
        Some(p @ ("normal" | "low")) => {
            line.push_str("|p:");
            line.push_str(p);
        }
        Some(p) => {
            ctx.stats.dropped_invalid_event_fields += 1;
            ctx.diag.warn_throttled(
                "invalid_event_field",
                format_args!(
                    "statsd_out: event {title:?} has an out-of-set priority {p:?}; omitting the \
                     p: field"
                ),
            );
        }
        None => {}
    }
    match carriers.event_alert_type {
        Some(t @ ("info" | "success" | "warning" | "error")) => {
            line.push_str("|t:");
            line.push_str(t);
        }
        Some(t) => {
            ctx.stats.dropped_invalid_event_fields += 1;
            ctx.diag.warn_throttled(
                "invalid_event_field",
                format_args!(
                    "statsd_out: event {title:?} has an out-of-set alert_type {t:?}; omitting the \
                     t: field"
                ),
            );
        }
        None => {}
    }
    if let Some(key) = carriers.event_aggregation_key {
        line.push_str("|k:");
        sanitize_into(line, key, is_forbidden_in_extended_field);
    }
    if let Some(source) = carriers.event_source_type {
        line.push_str("|s:");
        sanitize_into(line, source, is_forbidden_in_extended_field);
    }
    append_tags(line, ctx.tag_suffix);
    append_origin_fields(line, ctx);

    push_line(line, ctx.max_packet_bytes, ctx.stats, ctx.diag, ctx.out);
}

/// Renders one `_sc|name|status[...]` service-check line and pushes it. `metric` is the event's
/// first metric, which the caller has confirmed is a `Gauge`. Drops and counts
/// `EncodeStats::dropped_invalid_service_check` when neither the status carrier nor the gauge
/// gives a status in `0..=3`, and `EncodeStats::dropped_empty_name` when the sanitized name is
/// empty (`_sc||0` is no more legal than an empty metric name). See the module doc's "DogStatsD
/// events and service checks" section.
fn render_service_check(
    line: &mut String,
    carriers: &Carriers,
    metric: &MetricRecord,
    ctx: &mut EncodeCtx,
) {
    let name = carriers
        .service_check_name
        .expect("caller only calls this when service_check_name is Some");
    let gauge_value = match &metric.kind {
        MetricKind::Gauge(v) => *v,
        other => {
            unreachable!("caller only calls this when the first metric is a Gauge, got {other:?}")
        }
    };

    let status = match carriers.service_check_status {
        Some(s) if s <= 3 => s,
        _ => {
            let rounded = gauge_value.round();
            if gauge_value.is_finite() && (0.0..=3.0).contains(&rounded) {
                rounded as u64
            } else {
                ctx.stats.dropped_invalid_service_check += 1;
                ctx.diag.warn_throttled(
                    "invalid_service_check",
                    format_args!(
                        "statsd_out: service check {name:?} has no valid status in 0..=3; \
                         dropping"
                    ),
                );
                return;
            }
        }
    };

    line.clear();
    line.push_str("_sc|");
    let name_start = line.len();
    sanitize_into(line, name, is_forbidden_in_extended_field);
    if line.len() == name_start {
        ctx.stats.dropped_empty_name += 1;
        ctx.diag.warn_throttled(
            "empty_metric_name",
            format_args!("statsd_out: service check name {name:?} sanitizes to nothing; dropping"),
        );
        return;
    }
    let _ = write!(line, "|{status}");

    if let Some(secs) = ctx.timestamp_secs {
        let _ = write!(line, "|d:{secs}");
    }
    if let Some(host) = carriers.service_check_host {
        line.push_str("|h:");
        sanitize_into(line, host, is_forbidden_in_extended_field);
    }
    append_tags(line, ctx.tag_suffix);
    append_origin_fields(line, ctx);
    if let Some(message) = carriers.service_check_message {
        line.push_str("|m:");
        sanitize_into(line, message, is_forbidden_in_service_check_message);
    }

    push_line(line, ctx.max_packet_bytes, ctx.stats, ctx.diag, ctx.out);
}

/// Pushes `line` to `out`, or drops and counts it if longer than `max_packet_bytes`. Every
/// rendered line goes through here, so the oversize rule is the same for every kind.
fn push_line(
    line: &str,
    max_packet_bytes: usize,
    stats: &mut EncodeStats,
    diag: &mut Diagnostics,
    out: &mut MessageBuf,
) {
    if line.len() > max_packet_bytes {
        stats.dropped_oversize_line += 1;
        diag.warn_throttled(
            "oversize_line",
            format_args!(
                "statsd_out: a single metric line exceeds max_packet_bytes ({max_packet_bytes}); \
                 dropping it whole rather than truncating"
            ),
        );
        return;
    }
    out.push(line);
}

/// Encodes one metric into `ctx.out`: no entry for a dropped metric, one for most kinds (a
/// negative-gauge pair is one two-line entry), one per member for `SetMembers`, and one per value
/// for `Samples` under `Format::Statsd`. `name`/`member` are reused scratch buffers.
fn render_metric(
    line: &mut String,
    name: &mut String,
    member: &mut String,
    relative_gauges: bool,
    metric: &MetricRecord,
    ctx: &mut EncodeCtx,
) {
    name.clear();
    sanitize_into(name, logit_core::interner::resolve(metric.name), is_forbidden_in_name);
    if name.is_empty() {
        ctx.stats.dropped_empty_name += 1;
        ctx.diag.warn_throttled(
            "empty_metric_name",
            format_args!(
                "statsd_out: metric name {:?} sanitizes to nothing; dropping",
                logit_core::interner::resolve(metric.name)
            ),
        );
        return;
    }

    if metric.is_no_recorded_value() {
        ctx.stats.dropped_no_recorded_value += 1;
        ctx.diag.warn_throttled(
            "no_recorded_value",
            format_args!(
                "statsd_out: metric {name:?} has no recorded value (OTLP NO_RECORDED_VALUE); \
                 dropping"
            ),
        );
        return;
    }

    match &metric.kind {
        // Only a delta, monotonic `Sum` means `|c`; any other `Sum` falls to the next arm.
        MetricKind::Sum(s) if s.temporality == Temporality::Delta && s.monotonic => {
            if !s.value.is_finite() {
                ctx.stats.dropped_unencodable_value += 1;
                ctx.diag.warn_throttled(
                    "unencodable_value",
                    format_args!("statsd_out: non-finite counter value on {name:?}; dropping"),
                );
                return;
            }
            line.clear();
            line.push_str(name);
            line.push(':');
            push_float(line, s.value);
            line.push_str("|c");
            append_tags(line, ctx.tag_suffix);
            append_dialect_extras(line, ctx);
            push_line(line, ctx.max_packet_bytes, ctx.stats, ctx.diag, ctx.out);
        }
        MetricKind::Sum(s) => {
            let kind_name = if s.temporality == Temporality::Cumulative {
                "cumulative Sum"
            } else {
                "non-monotonic Sum"
            };
            dropped_unsupported_kind(ctx.stats, ctx.diag, name, kind_name, None);
        }
        MetricKind::Gauge(v) => {
            if !v.is_finite() {
                ctx.stats.dropped_unencodable_value += 1;
                ctx.diag.warn_throttled(
                    "unencodable_value",
                    format_args!("statsd_out: non-finite gauge value on {name:?}; dropping"),
                );
                return;
            }
            // `f64`'s `Display` renders `-0.0` as `"-0"`, which the decoder's `"g"` arm reads as a
            // no-op `GaugeDelta` (it checks the leading `-` before parsing). Normalize the sign so
            // `-0.0` is a plain `name:0|g` reset and the pair below is only for real negatives.
            // `stdio_out`'s `gauge_delta_negative_zero_does_not_double_the_sign` is the same trap.
            let v = if *v == 0.0 { 0.0 } else { *v };
            line.clear();
            if v.is_sign_negative() {
                // One indivisible entry (module doc's "Negative absolute gauges").
                write_gauge_line(line, name, 0.0, ctx);
                line.push('\n');
                write_gauge_line(line, name, v, ctx);
            } else {
                write_gauge_line(line, name, v, ctx);
            }
            push_line(line, ctx.max_packet_bytes, ctx.stats, ctx.diag, ctx.out);
        }
        MetricKind::GaugeDelta(v) => {
            if !relative_gauges {
                ctx.stats.dropped_gauge_delta += 1;
                ctx.diag.warn_throttled(
                    "gauge_delta_unresolved",
                    "a relative gauge adjustment reached a sink unresolved -- add an `aggregate` \
                     component between the statsd input and this output",
                );
                return;
            }
            if !v.is_finite() {
                ctx.stats.dropped_unencodable_value += 1;
                ctx.diag.warn_throttled(
                    "unencodable_value",
                    format_args!("statsd_out: non-finite gauge delta on {name:?}; dropping"),
                );
                return;
            }
            line.clear();
            line.push_str(name);
            line.push(':');
            if v.is_sign_positive() {
                line.push('+');
            }
            push_float(line, *v);
            line.push_str("|g");
            append_tags(line, ctx.tag_suffix);
            append_dialect_extras(line, ctx);
            push_line(line, ctx.max_packet_bytes, ctx.stats, ctx.diag, ctx.out);
        }
        MetricKind::Distribution(_) => dropped_unsupported_kind(
            ctx.stats,
            ctx.diag,
            name,
            "Distribution",
            Some(
                "summarize with `aggregate: distributions: samples` to relay the raw ms/h/d data \
                 through this sink instead",
            ),
        ),
        MetricKind::Set(_) => dropped_unsupported_kind(
            ctx.stats,
            ctx.diag,
            name,
            "Set",
            Some(
                "summarize with `aggregate: sets: members` to relay the raw set members through \
                 this sink instead",
            ),
        ),
        MetricKind::Histogram(_) => {
            dropped_unsupported_kind(ctx.stats, ctx.diag, name, "Histogram", None)
        }
        MetricKind::Summary(_) => {
            dropped_unsupported_kind(ctx.stats, ctx.diag, name, "Summary", None)
        }
        MetricKind::ExponentialHistogram(_) => {
            dropped_unsupported_kind(ctx.stats, ctx.diag, name, "ExponentialHistogram", None)
        }
        MetricKind::Samples(samples) => render_samples(line, name.as_str(), samples, ctx),
        MetricKind::SetMembers(members) => {
            render_set_members(line, member, name.as_str(), members, ctx)
        }
    }
}

/// Counts and warns for a `MetricKind` [`render_metric`] can't encode. Its match stays exhaustive
/// with no wildcard, so a new `MetricKind` variant is a compile error there. `hint` names the
/// `aggregate` config that relays the kind's raw form instead (`Distribution`/`Set` only; the rest
/// have no raw statsd counterpart).
fn dropped_unsupported_kind(
    stats: &mut EncodeStats,
    diag: &mut Diagnostics,
    name: &str,
    kind_name: &str,
    hint: Option<&str>,
) {
    stats.dropped_unsupported_kind += 1;
    match hint {
        Some(hint) => diag.warn_throttled(
            "unsupported_metric_kind",
            format_args!(
                "statsd_out: {kind_name} metrics are not implemented yet (metric {name:?}); \
                 dropping -- {hint}"
            ),
        ),
        None => diag.warn_throttled(
            "unsupported_metric_kind",
            format_args!(
                "statsd_out: {kind_name} metrics are not implemented yet (metric {name:?}); dropping"
            ),
        ),
    };
}

/// Appends one `name:v|g` line, tags and dialect extras included; `Gauge`'s plain and pair cases
/// share it.
fn write_gauge_line(line: &mut String, name: &str, v: f64, ctx: &mut EncodeCtx) {
    line.push_str(name);
    line.push(':');
    push_float(line, v);
    line.push_str("|g");
    append_tags(line, ctx.tag_suffix);
    append_dialect_extras(line, ctx);
}

/// The `statsd.type` carrier when it names a timer wire type (`ms`/`h`/`d`), else `"ms"`.
fn statsd_wire_type(ctx: &EncodeCtx) -> &'static str {
    match ctx.statsd_type {
        Some("ms") => "ms",
        Some("h") => "h",
        Some("d") => "d",
        _ => "ms",
    }
}

/// Encodes a `Samples` record (raw `ms`/`h`/`d` observations). `Format::DogStatsd` writes one
/// multi-value line (`name:v1:v2:...|<type>[|@rate]|#tags...`); `Format::Statsd` writes one
/// `name:v|ms[|@rate]` line per value, `h`/`d` normalized to `ms`. Each non-finite value is
/// dropped and counted, never written as `NaN`/`inf`. A rate outside `(0, 1]` omits `@rate`,
/// counted once per record. An empty `values` list emits nothing, counted once; one emptied by
/// non-finite values emits nothing with no extra count.
fn render_samples(
    line: &mut String,
    name: &str,
    samples: &logit_core::Samples,
    ctx: &mut EncodeCtx,
) {
    if samples.values.is_empty() {
        ctx.stats.dropped_unencodable_value += 1;
        ctx.diag.warn_throttled(
            "unencodable_value",
            format_args!("statsd_out: Samples metric {name:?} has no values; dropping"),
        );
        return;
    }

    let wire_type = statsd_wire_type(ctx);
    let (encode_type, normalized) = match ctx.format {
        Format::Statsd if wire_type != "ms" => ("ms", true),
        _ => (wire_type, false),
    };
    if normalized {
        ctx.stats.type_normalized_dialect += 1;
    }

    let rate = samples.sample_rate;
    let rate_valid = rate.is_finite() && rate > 0.0 && rate <= 1.0;
    let write_rate = rate != 1.0 && rate_valid;
    if rate != 1.0 && !rate_valid {
        ctx.stats.dropped_unencodable_value += 1;
        ctx.diag.warn_throttled(
            "unencodable_value",
            format_args!(
                "statsd_out: Samples metric {name:?} has an out-of-range sample rate {rate}; \
                 omitting @rate"
            ),
        );
    }

    match ctx.format {
        Format::DogStatsd => {
            line.clear();
            line.push_str(name);
            let mut wrote_any = false;
            for v in &samples.values {
                if !v.is_finite() {
                    ctx.stats.dropped_unencodable_value += 1;
                    ctx.diag.warn_throttled(
                        "unencodable_value",
                        format_args!("statsd_out: non-finite sample value on {name:?}; dropping"),
                    );
                    continue;
                }
                line.push(':');
                push_float(line, *v);
                wrote_any = true;
            }
            if !wrote_any {
                // Every value was non-finite and already counted.
                return;
            }
            line.push('|');
            line.push_str(encode_type);
            if write_rate {
                line.push_str("|@");
                push_float(line, rate);
            }
            append_tags(line, ctx.tag_suffix);
            append_dialect_extras(line, ctx);
            push_line(line, ctx.max_packet_bytes, ctx.stats, ctx.diag, ctx.out);
        }
        Format::Statsd => {
            for v in &samples.values {
                if !v.is_finite() {
                    ctx.stats.dropped_unencodable_value += 1;
                    ctx.diag.warn_throttled(
                        "unencodable_value",
                        format_args!("statsd_out: non-finite sample value on {name:?}; dropping"),
                    );
                    continue;
                }
                line.clear();
                line.push_str(name);
                line.push(':');
                push_float(line, *v);
                line.push('|');
                line.push_str(encode_type);
                if write_rate {
                    line.push_str("|@");
                    push_float(line, rate);
                }
                // A no-op (`tag_suffix` is empty under `Format::Statsd`), kept for symmetry.
                append_tags(line, ctx.tag_suffix);
                append_dialect_extras(line, ctx);
                push_line(line, ctx.max_packet_bytes, ctx.stats, ctx.diag, ctx.out);
            }
        }
    }
}

/// Encodes a `SetMembers` record (raw `s` observations) as one `name:<member>|s` line per member,
/// in both dialects. Member sanitization: the module doc's "Sanitization" section. An empty member
/// list emits nothing, counted once.
fn render_set_members(
    line: &mut String,
    member: &mut String,
    name: &str,
    members: &[bytes::Bytes],
    ctx: &mut EncodeCtx,
) {
    if members.is_empty() {
        ctx.stats.dropped_unencodable_value += 1;
        ctx.diag.warn_throttled(
            "unencodable_value",
            format_args!("statsd_out: SetMembers metric {name:?} has no members; dropping"),
        );
        return;
    }

    for raw in members {
        let lossy = String::from_utf8_lossy(raw);
        member.clear();
        sanitize_into(member, &lossy, is_forbidden_in_set_member);
        if std::str::from_utf8(raw) != Ok(member.as_str()) {
            ctx.stats.members_sanitized += 1;
        }

        line.clear();
        line.push_str(name);
        line.push(':');
        line.push_str(member.as_str());
        line.push_str("|s");
        append_tags(line, ctx.tag_suffix);
        append_dialect_extras(line, ctx);
        push_line(line, ctx.max_packet_bytes, ctx.stats, ctx.diag, ctx.out);
    }
}

/// Appends `s` to `out` (not cleared first), replacing each character `forbidden` rejects with
/// `_`. Substitution, not deletion, so distinct inputs stay distinct.
fn sanitize_into(out: &mut String, s: &str, forbidden: impl Fn(char) -> bool) {
    for c in s.chars() {
        out.push(if forbidden(c) { '_' } else { c });
    }
}

/// Forbidden in a metric name and a tag key (module doc's "Sanitization" section). A `:` in a key
/// would reparse as a shorter key, since `parse_line` splits a tag on its first colon.
fn is_forbidden_in_name(c: char) -> bool {
    matches!(c, ':' | '|' | '@' | '#' | ',' | '\n' | '\r' | '\0')
        || c.is_control()
        || c.is_whitespace()
}

/// [`is_forbidden_in_name`], named for the tag-key call site.
fn is_forbidden_in_tag_key(c: char) -> bool {
    is_forbidden_in_name(c)
}

/// The name set **except `:`**: `parse_line` splits a tag on its first colon only, so `env:a:b`
/// round-trips as key `env`, value `a:b`.
fn is_forbidden_in_tag_value_only(c: char) -> bool {
    c != ':' && is_forbidden_in_name(c)
}

/// Forbidden in a `SetMembers` member: `:` (it would split one member into two values), `|` (it
/// would open a segment), and control bytes (they corrupt framing). `@`, `#`, `,`, and whitespace
/// survive a real decode in this position, so forbidding them would only make distinct members
/// collide (module doc's "Sanitization" section).
fn is_forbidden_in_set_member(c: char) -> bool {
    matches!(c, ':' | '|') || c.is_control()
}

/// Forbidden in an event title: control bytes only. A `|` is fine, since `parse_event` slices the
/// title by the `_e{tlen,xlen}` header's byte lengths. A raw `\n` would still look like a line
/// boundary inside a packed UDP datagram (module doc's "Packing and framing" section).
fn is_forbidden_in_event_title(c: char) -> bool {
    c.is_control()
}

/// Appends `raw` as DogStatsD event `TEXT`: a real newline becomes the two-byte escape `\n`
/// (`statsd_in`'s `unescape_event_text` reverses it), and any other control byte, which has no
/// escape, becomes `_`.
fn append_event_text(out: &mut String, raw: &str) {
    for c in raw.chars() {
        if c == '\n' {
            out.push('\\');
            out.push('n');
        } else if c.is_control() {
            out.push('_');
        } else {
            out.push(c);
        }
    }
}

/// Forbidden in an event's `host`/`aggregation_key`/`source_type` and a service check's
/// `name`/`host`: `|` and control bytes. A decode reads each field up to the next `|` (a service
/// check's `name` via `parse_service_check`'s `splitn(3, '|')`), so an embedded `|` would leak the
/// remainder into the next field. None has a colon-splitting ambiguity. Not
/// [`is_forbidden_in_name`]: a service check name keeps `.` and spaces as sent.
fn is_forbidden_in_extended_field(c: char) -> bool {
    c == '|' || c.is_control()
}

/// A DogStatsD `|e:<external-data>` value: `|`, control characters, and whitespace -> `_`. Not the
/// tag-value rule, which substitutes `,`: external data is itself a comma-separated list
/// (`it-<bool>,cn-<container name>,pu-<pod uid>`), and the decoder ends the field only at the next
/// `|` or line end. Whitespace goes because a metric line's trailing whitespace is trimmed at decode.
fn is_forbidden_in_external_data(c: char) -> bool {
    c == '|' || c.is_control() || c.is_whitespace()
}

/// Forbidden in a service check's `m:` message: control bytes only. A newline is substituted, not
/// escaped, since the decoder has no unescape for this field. `|` is allowed: `m:` is always
/// rendered and parsed last, so a `|` can't start another field.
fn is_forbidden_in_service_check_message(c: char) -> bool {
    c.is_control()
}

/// The live half of a `statsd_out` sink, as `syslog::Conn`: the UDP arm binds eagerly (a bad
/// local socket is a config error); the Unix datagram and stream arms connect lazily inside
/// `send`, so a receiver that isn't up yet can't block startup.
enum Conn {
    Udp(UdpSocket),
    /// `Box<dyn AsyncStream>` covers plaintext and TLS without making [`StatsdOutput`] generic,
    /// since `logit-cli::pipeline::build_spec` builds one concrete sink type per kind. No DTLS arm:
    /// `logit-pipeline::graph::resolve`'s rule 52 rejects `tls:` under `transport: udp`.
    Tcp {
        stream: Option<Box<dyn AsyncStream>>,
        connect_timeout: Duration,
    },
    /// Connected to the path in `endpoint` on first use, and `None` again after a send that shows
    /// the receiver gone or stuck, so the next send reconnects; each send is bounded by
    /// `send_timeout` (module doc's "Packing and framing").
    UnixDatagram {
        socket: Option<UnixDatagram>,
        send_timeout: Duration,
    },
    /// Always plaintext (graph rule 64); boxed like `Tcp` so both share
    /// [`StatsdOutput::send_tcp`].
    UnixStream {
        stream: Option<Box<dyn AsyncStream>>,
        connect_timeout: Duration,
    },
}

/// `logit_pipeline::Output` for `statsd_out`, built via [`StatsdOutput::udp`],
/// [`StatsdOutput::tcp`], [`StatsdOutput::unix_datagram`] or [`StatsdOutput::unix_stream`].
pub struct StatsdOutput {
    endpoint: String,
    conn: Conn,
    encoder: StatsdEncoder,
    max_packet_bytes: usize,
    lines: MessageBuf,
    /// Reused across `send` calls: the packed UDP datagram, or the whole TCP frame.
    packet_buf: Vec<u8>,
    /// TCP only. `Some` exactly when a `tls:` block was configured (presence turns TLS on). Built
    /// once by [`StatsdOutput::with_tls`] and shared by every connect.
    tls: Option<Arc<rustls::ClientConfig>>,
    /// Set by the first connect; every later connect counts `logit.output.reconnects`.
    has_connected_once: bool,
    diag: Diagnostics,
    telemetry: Telemetry,
}

impl StatsdOutput {
    /// Binds an ephemeral local UDP socket now; `endpoint` is resolved per `send`
    /// (`SyslogOutput::udp` has why).
    pub fn udp(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        let socket = std::net::UdpSocket::bind("0.0.0.0:0")
            .context("binding statsd_out's local UDP socket")?;
        socket.set_nonblocking(true).context("configuring statsd_out's UDP socket")?;
        let socket = UdpSocket::from_std(socket).context("registering statsd_out's UDP socket")?;
        Ok(Self::new(endpoint, Conn::Udp(socket)))
    }

    /// Never connects here; see [`Conn`].
    pub fn tcp(endpoint: impl Into<String>, connect_timeout: Duration) -> Self {
        Self::new(endpoint, Conn::Tcp { stream: None, connect_timeout })
    }

    /// Never connects here; see [`Conn`]. `send_timeout` bounds each send's wait on a full
    /// receiver.
    pub fn unix_datagram(path: impl Into<String>, send_timeout: Duration) -> Self {
        Self::new(path, Conn::UnixDatagram { socket: None, send_timeout })
    }

    /// Never connects here; see [`Conn`].
    pub fn unix_stream(path: impl Into<String>, connect_timeout: Duration) -> Self {
        Self::new(path, Conn::UnixStream { stream: None, connect_timeout })
    }

    fn new(endpoint: impl Into<String>, conn: Conn) -> Self {
        Self {
            endpoint: endpoint.into(),
            conn,
            encoder: StatsdEncoder::new(Format::DogStatsd),
            max_packet_bytes: DEFAULT_MAX_PACKET_BYTES,
            lines: MessageBuf::default(),
            packet_buf: Vec::new(),
            tls: None,
            has_connected_once: false,
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
        }
        .with_max_packet_bytes(DEFAULT_MAX_PACKET_BYTES)
    }

    /// The encoder's per-line cap for this transport: `max_packet_bytes` wherever lines are packed
    /// into packets (UDP and both Unix transports), uncapped on TCP. Applied by the builders, so
    /// `send` never mutates the encoder.
    fn encoder_cap(&self) -> usize {
        if matches!(self.conn, Conn::Tcp { .. }) {
            usize::MAX
        } else {
            self.max_packet_bytes
        }
    }

    /// Installs `encoder`, overriding its line cap with this sink's, whatever the builder order.
    pub fn with_encoder(mut self, encoder: StatsdEncoder) -> Self {
        self.encoder = encoder.with_max_packet_bytes(self.encoder_cap());
        self
    }

    /// Bounds one **packet** (several packed lines: a UDP or Unix datagram, or one length-prefixed
    /// `unix_stream` packet), and so one line; no effect on TCP.
    pub fn with_max_packet_bytes(mut self, max_packet_bytes: usize) -> Self {
        self.max_packet_bytes = max_packet_bytes;
        let cap = self.encoder_cap();
        self.encoder = self.encoder.with_max_packet_bytes(cap);
        self
    }

    /// Turns on TLS for this sink's TCP connection (`tls:` in config;
    /// `docs/adr/statsd-output.md`'s "Amendment: TLS" section).
    ///
    /// **Presence turns it on**, the `syslog_out`/`logit_out` shape rather than `otlp_out`'s:
    /// `endpoint` has no scheme to carry the signal, so an empty `tls: {}` still means TLS with the
    /// bundled Mozilla roots and no client certificate. That is why this ignores
    /// [`TlsClientSettings::is_empty`], which `otlp_out` can use only because `https://` already
    /// selected TLS. TLS is then *required*; there is no plaintext fallback.
    ///
    /// Errors on UDP (DTLS is out of scope) and on a Unix socket. `logit-pipeline::graph::resolve`'s
    /// rules 52 and 64 already reject those configs; this check stops a caller that skips graph
    /// validation from getting an unencrypted socket.
    ///
    /// Paths in `settings` resolve against `base_dir` (the config file's directory) and load here,
    /// since `graph::resolve` never touches the filesystem.
    pub fn with_tls(
        mut self,
        settings: &TlsClientSettings,
        base_dir: &Path,
    ) -> anyhow::Result<Self> {
        match self.conn {
            Conn::Tcp { .. } => {}
            Conn::Udp(_) => anyhow::bail!(
                "statsd_out: tls: requires transport: tcp -- DTLS (statsd over TLS over UDP) is \
                 out of scope"
            ),
            Conn::UnixDatagram { .. } | Conn::UnixStream { .. } => anyhow::bail!(
                "statsd_out: tls: requires transport: tcp -- a Unix socket is always plaintext"
            ),
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
impl Output for StatsdOutput {
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let stats = self.encoder.encode_into(batch, &mut self.lines);
        self.telemetry.count("logit.output.events.skipped", stats.skipped_no_metrics as f64, &[]);
        self.telemetry.count(
            "logit.output.messages.dropped",
            stats.dropped_gauge_delta as f64,
            &[("reason", "unresolved_gauge_delta")],
        );
        self.telemetry.count(
            "logit.output.messages.dropped",
            stats.dropped_unsupported_kind as f64,
            &[("reason", "unsupported_kind")],
        );
        self.telemetry.count(
            "logit.output.messages.dropped",
            stats.dropped_no_recorded_value as f64,
            &[("reason", "no_recorded_value")],
        );
        self.telemetry.count(
            "logit.output.messages.dropped",
            stats.dropped_unencodable_value as f64,
            &[("reason", "unencodable_value")],
        );
        self.telemetry.count(
            "logit.output.messages.dropped",
            stats.dropped_empty_name as f64,
            &[("reason", "empty_name")],
        );
        self.telemetry.count(
            "logit.output.messages.dropped",
            stats.dropped_oversize_line as f64,
            &[("reason", "oversize_line")],
        );
        self.telemetry.count(
            "logit.output.tags.dropped",
            stats.tags_dropped_dialect as f64,
            &[("reason", "dialect")],
        );
        self.telemetry.count(
            "logit.output.tags.dropped",
            stats.tags_dropped_unrepresentable as f64,
            &[("reason", "unrepresentable")],
        );
        self.telemetry.count(
            "logit.output.messages.dropped",
            stats.dropped_dialect_fields as f64,
            &[("reason", "dialect_field")],
        );
        self.telemetry.count(
            "logit.output.messages.dropped",
            stats.dropped_dialect_events as f64,
            &[("reason", "dialect_event")],
        );
        self.telemetry.count(
            "logit.output.messages.dropped",
            stats.dropped_invalid_service_check as f64,
            &[("reason", "invalid_service_check")],
        );
        self.telemetry.count(
            "logit.output.messages.dropped",
            stats.dropped_invalid_event_fields as f64,
            &[("reason", "invalid_event_field")],
        );
        self.telemetry.count(
            "logit.output.messages.normalized",
            stats.type_normalized_dialect as f64,
            &[("reason", "dialect")],
        );
        self.telemetry.count(
            "logit.output.messages.normalized",
            stats.members_sanitized as f64,
            &[("reason", "member_sanitized")],
        );

        if self.lines.is_empty() {
            return Ok(());
        }

        self.telemetry.count("logit.output.batch.bytes", self.lines.total_bytes() as f64, &[]);
        let request_timer = self.telemetry.timer("logit.output.request.duration");
        let result = match &mut self.conn {
            Conn::Udp(socket) => {
                Self::send_udp(
                    socket,
                    &self.endpoint,
                    &self.lines,
                    self.max_packet_bytes,
                    &mut self.packet_buf,
                    &mut self.diag,
                    &self.telemetry,
                )
                .await
            }
            Conn::UnixDatagram { socket, send_timeout } => {
                let mut dest = DatagramDest::Unix(UnixDest {
                    socket,
                    path: Path::new(&self.endpoint),
                    send_timeout: *send_timeout,
                    telemetry: &self.telemetry,
                    has_connected_once: &mut self.has_connected_once,
                });
                Self::send_datagrams(
                    &mut dest,
                    &self.lines,
                    self.max_packet_bytes,
                    &mut self.packet_buf,
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
                    kind: StreamKind::Tcp,
                    telemetry: &self.telemetry,
                    has_connected_once: &mut self.has_connected_once,
                };
                Self::send_tcp(stream, &mut dial, &self.lines, &mut self.packet_buf).await
            }
            Conn::UnixStream { stream, connect_timeout } => {
                let mut dial = TcpDial {
                    endpoint: &self.endpoint,
                    connect_timeout: *connect_timeout,
                    tls: None,
                    kind: StreamKind::Unix { max_packet_bytes: self.max_packet_bytes },
                    telemetry: &self.telemetry,
                    has_connected_once: &mut self.has_connected_once,
                };
                Self::send_tcp(stream, &mut dial, &self.lines, &mut self.packet_buf).await
            }
        };
        drop(request_timer);

        match &result {
            Ok((messages, datagrams)) => {
                self.telemetry.count("logit.output.messages", *messages as f64, &[]);
                if matches!(self.conn, Conn::Udp(_) | Conn::UnixDatagram { .. }) {
                    self.telemetry.count("logit.output.datagrams", *datagrams as f64, &[]);
                }
                self.telemetry.count("logit.output.requests", 1.0, &[("class", "ok")]);
            }
            Err(_) => {
                self.telemetry.count("logit.output.requests", 1.0, &[("class", "error")]);
            }
        }
        result.map(|_| ())
    }

    /// `send` flushes and retains nothing between calls, so this only flushes a stream as a
    /// backstop at shutdown.
    async fn flush(&mut self) -> anyhow::Result<()> {
        if let Conn::Tcp { stream: Some(stream), .. }
        | Conn::UnixStream { stream: Some(stream), .. } = &mut self.conn
        {
            stream.flush().await.context("flushing statsd_out stream")?;
        }
        Ok(())
    }

    /// `false`: a redelivered `hits:5|c` **increments the destination counter a second time**,
    /// corrupting the value with no trace at the receiver (worse than `syslog_out`'s duplicated
    /// log line). The derived `AtMostOnce` posture still retries a `Fault::Clean` failure, which
    /// covers the common receiver-restart outage with no duplicate risk.
    fn duplicate_safe(&self) -> bool {
        false
    }
}

/// Running totals for one [`StatsdOutput::send_udp`] call. `entries_in_packet` can't be recovered
/// from `packet_buf`'s bytes: a negative-gauge pair is **one** [`MessageBuf`] entry with an
/// embedded `\n`, so counting `\n` bytes would report two messages where `send_tcp` (and
/// `syslog_out`) report one.
#[derive(Default)]
struct UdpSendCounts {
    /// [`MessageBuf`] entries written to the socket (`logit.output.messages`).
    messages: usize,
    /// Datagrams written to the socket (`logit.output.datagrams`).
    datagrams: usize,
    /// Entries appended to `packet_buf` since the last flush.
    entries_in_packet: usize,
}

impl StatsdOutput {
    /// Packs `lines` greedily into datagrams of at most `max_packet_bytes` (newline-joined, no
    /// trailing newline), one `send_to` each (module doc's "Packing and framing" section). The
    /// encoder already dropped any line over the cap, so every line fits a datagram alone. Returns
    /// `(messages sent, datagrams sent)`.
    async fn send_udp(
        socket: &UdpSocket,
        endpoint: &str,
        lines: &MessageBuf,
        max_packet_bytes: usize,
        packet_buf: &mut Vec<u8>,
        diag: &mut Diagnostics,
        telemetry: &Telemetry,
    ) -> anyhow::Result<(usize, usize)> {
        // Once per batch, not per datagram (`syslog::send_udp` has why).
        let mut addrs = lookup_host(endpoint)
            .await
            .context("resolving statsd_out endpoint")
            .context(Fault::Clean)?;
        let addr = addrs
            .next()
            .context("statsd_out endpoint resolved to no addresses")
            .context(Fault::Clean)?;
        let mut dest = DatagramDest::Udp { socket, addr };
        Self::send_datagrams(&mut dest, lines, max_packet_bytes, packet_buf, diag, telemetry).await
    }

    /// [`Self::send_udp`]'s packing loop over either datagram family.
    async fn send_datagrams(
        dest: &mut DatagramDest<'_>,
        lines: &MessageBuf,
        max_packet_bytes: usize,
        packet_buf: &mut Vec<u8>,
        diag: &mut Diagnostics,
        telemetry: &Telemetry,
    ) -> anyhow::Result<(usize, usize)> {
        let mut counts = UdpSendCounts::default();
        packet_buf.clear();
        for msg in lines.iter() {
            let needs_sep = !packet_buf.is_empty();
            let extra = msg.len() + usize::from(needs_sep);
            if !packet_buf.is_empty() && packet_buf.len() + extra > max_packet_bytes {
                Self::flush_datagram(dest, packet_buf, &mut counts, diag, telemetry).await?;
            }
            if needs_sep && !packet_buf.is_empty() {
                packet_buf.push(b'\n');
            }
            packet_buf.extend_from_slice(msg);
            counts.entries_in_packet += 1;
        }
        if !packet_buf.is_empty() {
            Self::flush_datagram(dest, packet_buf, &mut counts, diag, telemetry).await?;
        }
        Ok((counts.messages, counts.datagrams))
    }

    /// Sends one packed datagram, then clears `packet_buf` and `counts.entries_in_packet`. Counts
    /// [`MessageBuf`] entries, not `\n` bytes (see [`UdpSendCounts`]), toward `counts.messages`
    /// or, when the kernel rejects the datagram as too large,
    /// `logit.output.messages.dropped{reason="oversize_datagram"}`.
    async fn flush_datagram(
        dest: &mut DatagramDest<'_>,
        packet_buf: &mut Vec<u8>,
        counts: &mut UdpSendCounts,
        diag: &mut Diagnostics,
        telemetry: &Telemetry,
    ) -> anyhow::Result<()> {
        match dest.send(packet_buf, counts.datagrams == 0).await {
            Ok(_) => {
                counts.messages += counts.entries_in_packet;
                counts.datagrams += 1;
            }
            Err(err) if is_message_too_large(&err) => {
                telemetry.count(
                    "logit.output.messages.dropped",
                    counts.entries_in_packet as f64,
                    &[("reason", "oversize_datagram")],
                );
                diag.warn_throttled(
                    "oversize_datagram",
                    format_args!("statsd_out: packed datagram too large for one send: {err}"),
                );
            }
            Err(err) => {
                let fault = if counts.datagrams > 0 { Fault::Ambiguous } else { Fault::Clean };
                packet_buf.clear();
                counts.entries_in_packet = 0;
                return Err(anyhow::Error::new(err).context(fault));
            }
        }
        packet_buf.clear();
        counts.entries_in_packet = 0;
        Ok(())
    }

    /// Writes the whole batch as one frame, every line `\n`-terminated including the last (or, on
    /// a Unix stream, as length-prefixed packets; module doc's "Packing and framing"), with at
    /// most one internal reconnect-and-retry. Shares `syslog::send_tcp`'s two properties:
    /// cancellation safety via `stream.take()`, and never resending once a byte has left this
    /// host. Returns `(messages sent, 0)`; TCP has no datagram count.
    ///
    /// **What a write proves depends on the transport, and TLS proves less.** The stream is a
    /// `Box<dyn AsyncStream>`, so the code asks [`TcpDial::is_tls`] which case applies:
    ///
    /// - **Plaintext.** One `write()` is one `write(2)`: `Ok(n)` means the kernel owns `n` bytes,
    ///   and `Err` means zero bytes of *this* call were accepted (tokio loops only on
    ///   `WouldBlock`). So one reconnect-and-retry after a zero-byte failure is safe, and a failed
    ///   retry is `Fault::Clean`.
    /// - **TLS.** `tokio_rustls`' `poll_write` copies plaintext into the rustls session and loops
    ///   socket writes until one returns `Pending`, then returns `Ok(n)` with finished records
    ///   still queued in userspace. `Ok` proves only that the *session* accepted the bytes; `flush`
    ///   is what hands them to the kernel. A failing `poll_write` may already have completed
    ///   several socket writes (rustls fragments at 16 KiB, and each record is a run of complete,
    ///   LF-terminated lines a receiver keeps and counts), so `Err` never proves a zero-byte
    ///   attempt. **A TLS write failure is never retried**: once an application write is attempted,
    ///   every failure is `Fault::Ambiguous`, and `Fault::Clean` survives only for failures inside
    ///   [`TcpDial::connect`], which precede every byte of the frame. A resend here would be worse
    ///   than `syslog_out`'s duplicate log line: [`StatsdOutput::duplicate_safe`] is `false`
    ///   because a redelivered `hits:5|c` increments the destination counter a second time.
    ///
    /// **A reused connection is probed before the first write.** The receiver may have closed it
    /// since the last `send` (a graceful shutdown, a far-end `idle_timeout:`, a relay hop cycling),
    /// and plaintext statsd has no ack to reveal that: the write lands in the local socket buffer,
    /// the batch is reported delivered, and the increments are lost. So a connection taken from
    /// `*stream` (never a fresh one) gets one non-consuming `poll_read` first
    /// ([`crate::tls::poll_pending_close`] has why one poll, not a `timeout(read)`); anything but
    /// "still open" drops it and dials fresh with nothing written. That is an ordinary reconnect
    /// (counted by [`TcpDial::connect`]) and doesn't consume the post-write-failure retry
    /// (`docs/adr/idle-connection-timeout.md`).
    ///
    /// **The success path always `flush`es**, on both transports, before the connection returns
    /// to `*stream` and this returns `Ok`. TLS requires it: otherwise a batch could be reported
    /// delivered (and committed off the sink queue, `docs/adr/buffered-sink-delivery.md`) with its
    /// records still in the rustls buffer, discarded with the boxed stream on the next reconnect
    /// or cancellation. On plaintext `TcpStream::poll_flush` is a no-op. One delivery rule for
    /// both matters because nothing reported delivered is ever retried. A failed flush is
    /// `Fault::Ambiguous` (an earlier record may have landed) and the connection is dropped
    /// (`docs/adr/statsd-output.md`'s "Amendment: TLS" section).
    async fn send_tcp(
        stream: &mut Option<Box<dyn AsyncStream>>,
        dial: &mut TcpDial<'_>,
        lines: &MessageBuf,
        frame_buf: &mut Vec<u8>,
    ) -> anyhow::Result<(usize, usize)> {
        match dial.kind {
            StreamKind::Tcp => {
                frame_buf.clear();
                for msg in lines.iter() {
                    frame_buf.extend_from_slice(msg);
                    frame_buf.push(b'\n');
                }
            }
            StreamKind::Unix { max_packet_bytes } => {
                build_length_prefixed_frame(lines, max_packet_bytes, frame_buf);
            }
        }

        let mut retried_after_a_zero_byte_failure = false;
        loop {
            let mut conn: Box<dyn AsyncStream> = match stream.take() {
                // A reused connection is probed first (doc comment). A closed one is replaced
                // with nothing written, so the post-write retry isn't consumed.
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
                    // Always flush before calling the batch delivered (doc comment).
                    let rest_result = match rest_result {
                        Ok(()) => conn.flush().await,
                        Err(err) => Err(err),
                    };
                    return match rest_result {
                        Ok(()) => {
                            *stream = Some(conn);
                            Ok((lines.len(), 0))
                        }
                        // Part of the frame may be at the peer, so a resend could duplicate.
                        // `*stream` stays `None`: a partly written connection isn't reusable.
                        Err(err) => Err(anyhow::Error::new(err).context(Fault::Ambiguous)),
                    };
                }
                // Plaintext only: the failed `write(2)` accepted zero bytes, so reconnect and
                // retry the whole frame once. TLS has no such proof and falls to `Ambiguous`.
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

/// What [`StatsdOutput::send_tcp`] needs to open a fresh connection, borrowed per `send` from the
/// sink's fields (one value instead of five parameters, for `clippy::too_many_arguments`).
/// `syslog::TcpDial`'s twin: both dial a bare `host:port` plus SNI.
struct TcpDial<'a> {
    endpoint: &'a str,
    connect_timeout: Duration,
    /// `Some` if and only if a `tls:` block was configured -- see [`StatsdOutput::tls`]. Always
    /// `None` for [`StreamKind::Unix`].
    tls: Option<&'a Arc<rustls::ClientConfig>>,
    /// What `endpoint` names and how a batch is framed on it.
    kind: StreamKind,
    telemetry: &'a Telemetry,
    has_connected_once: &'a mut bool,
}

impl TcpDial<'_> {
    /// Whether connections are TLS-wrapped, which decides how [`StatsdOutput::send_tcp`]
    /// classifies a write failure.
    fn is_tls(&self) -> bool {
        self.tls.is_some()
    }

    /// One fresh connection: TCP connect, then the TLS handshake when `tls` is set. Each phase gets
    /// its own `connect_timeout`, as in `syslog_out` and `logit_out`, so a TLS connect can take up
    /// to twice the configured value. Both phases fail `Fault::Clean`: nothing of the batch has
    /// left the host yet.
    async fn connect(&mut self) -> anyhow::Result<Box<dyn AsyncStream>> {
        if let StreamKind::Unix { .. } = self.kind {
            let unix =
                tokio::time::timeout(self.connect_timeout, UnixStream::connect(self.endpoint))
                    .await
                    .context("connecting to statsd_out socket timed out")
                    .and_then(|r| r.context("connecting to statsd_out socket"))
                    .context(Fault::Clean)?;
            self.count_connect();
            return Ok(Box::new(unix));
        }
        let tcp = tokio::time::timeout(self.connect_timeout, TcpStream::connect(self.endpoint))
            .await
            .context("connecting to statsd_out endpoint timed out")
            .and_then(|r| r.context("connecting to statsd_out endpoint"))
            .context(Fault::Clean)?;

        let conn: Box<dyn AsyncStream> = match self.tls {
            Some(cfg) => {
                let host = host_only(self.endpoint);
                let server_name = ServerName::try_from(host.to_string())
                    .map_err(|e| {
                        anyhow::anyhow!("statsd_out: invalid TLS server name {host:?}: {e}")
                    })
                    .context(Fault::Clean)?;
                let connector = TlsConnector::from(cfg.clone());
                let tls_stream =
                    tokio::time::timeout(self.connect_timeout, connector.connect(server_name, tcp))
                        .await
                        .context("TLS handshake with statsd_out endpoint timed out")
                        .and_then(|r| r.context("TLS handshake with statsd_out endpoint"))
                        .context(Fault::Clean)?;
                Box::new(tls_stream)
            }
            None => Box::new(tcp),
        };

        self.count_connect();
        Ok(conn)
    }

    /// Counts every connect after the first as `logit.output.reconnects`. Counted at connect, not
    /// after the write, so a reconnect whose first write fails still shows up.
    fn count_connect(&mut self) {
        count_connect(self.telemetry, self.has_connected_once);
    }
}

/// [`TcpDial::count_connect`]'s rule, shared with [`UnixDest`].
fn count_connect(telemetry: &Telemetry, has_connected_once: &mut bool) {
    if *has_connected_once {
        telemetry.count("logit.output.reconnects", 1.0, &[]);
    } else {
        *has_connected_once = true;
    }
}

/// Which stream [`TcpDial`] opens and how [`StatsdOutput::send_tcp`] frames a batch on it.
#[derive(Debug, Clone, Copy)]
enum StreamKind {
    /// A TCP connection to a `host:port`, optionally TLS; every line `\n`-terminated.
    Tcp,
    /// A Unix stream socket at a path; packets of up to `max_packet_bytes`, each after a 4-byte
    /// little-endian length.
    Unix { max_packet_bytes: usize },
}

/// Packs `lines` into packets of at most `max_packet_bytes` (newline-joined, no trailing newline,
/// as [`StatsdOutput::send_udp`] packs a datagram) and writes each into `frame` after its length as
/// a 4-byte little-endian integer: the `unix_stream` framing. The encoder already dropped any line
/// over the cap, so every line fits a packet alone.
fn build_length_prefixed_frame(lines: &MessageBuf, max_packet_bytes: usize, frame: &mut Vec<u8>) {
    const PREFIX: usize = 4;
    fn close(frame: &mut [u8], start: usize) {
        let body = (frame.len() - start - PREFIX) as u32;
        frame[start..start + PREFIX].copy_from_slice(&body.to_le_bytes());
    }
    frame.clear();
    let mut open: Option<usize> = None; // where the current packet's prefix starts
    for msg in lines.iter() {
        if let Some(start) = open {
            let body = frame.len() - start - PREFIX;
            if body + 1 + msg.len() <= max_packet_bytes {
                frame.push(b'\n');
                frame.extend_from_slice(msg);
                continue;
            }
            close(frame, start);
        }
        open = Some(frame.len());
        frame.extend_from_slice(&[0; PREFIX]);
        frame.extend_from_slice(msg);
    }
    if let Some(start) = open {
        close(frame, start);
    }
}

/// Where [`StatsdOutput::send_datagrams`] sends each packed packet.
enum DatagramDest<'a> {
    Udp { socket: &'a UdpSocket, addr: std::net::SocketAddr },
    Unix(UnixDest<'a>),
}

impl DatagramDest<'_> {
    /// One datagram; `first_of_batch` is whether nothing of this batch has been sent yet.
    async fn send(&mut self, buf: &[u8], first_of_batch: bool) -> std::io::Result<usize> {
        match self {
            DatagramDest::Udp { socket, addr } => socket.send_to(buf, *addr).await,
            DatagramDest::Unix(dest) => dest.send(buf, first_of_batch).await,
        }
    }
}

/// A `transport: unix` sender: a datagram socket connected to `path` (module doc's "Packing and
/// framing" has why it's connected and when it reconnects).
struct UnixDest<'a> {
    socket: &'a mut Option<UnixDatagram>,
    path: &'a Path,
    send_timeout: Duration,
    telemetry: &'a Telemetry,
    has_connected_once: &'a mut bool,
}

impl UnixDest<'_> {
    /// Sends one datagram. When it's the batch's first and an inherited socket finds its receiver
    /// gone, reconnects and retries once: nothing of the batch has left, so the retry can't
    /// duplicate.
    async fn send(&mut self, buf: &[u8], first_of_batch: bool) -> std::io::Result<usize> {
        let inherited = self.socket.is_some();
        match self.send_once(buf).await {
            Err(err) if first_of_batch && inherited && is_receiver_gone(&err) => {
                self.send_once(buf).await
            }
            result => result,
        }
    }

    /// Connects when there's no socket, then sends under `send_timeout`. Drops the socket on a
    /// timeout or a gone receiver, so the next send reconnects to whatever is at `path`. A connect
    /// doesn't block on a datagram socket; its failure reaches `flush_datagram` as a send error,
    /// `Fault::Clean` on a batch's first datagram.
    async fn send_once(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let socket: &UnixDatagram = match &mut *self.socket {
            Some(socket) => socket,
            slot @ None => {
                let socket = UnixDatagram::unbound()
                    .and_then(|socket| socket.connect(self.path).map(|()| socket))
                    .map_err(|err| {
                        std::io::Error::new(
                            err.kind(),
                            format!(
                                "connecting to statsd_out socket {}: {err}",
                                self.path.display()
                            ),
                        )
                    })?;
                count_connect(self.telemetry, self.has_connected_once);
                slot.insert(socket)
            }
        };
        let result = match tokio::time::timeout(self.send_timeout, socket.send(buf)).await {
            Ok(result) => result,
            Err(_elapsed) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "the receiver at {} did not take a datagram within {:?}",
                    self.path.display(),
                    self.send_timeout
                ),
            )),
        };
        if let Err(err) = &result {
            if err.kind() == std::io::ErrorKind::TimedOut || is_receiver_gone(err) {
                *self.socket = None;
            }
        }
        result
    }
}

/// `ECONNREFUSED` (the connected receiver's socket closed) or `ENOTCONN` (a later send on a socket
/// the kernel already disconnected): the path may now name a new receiver.
fn is_receiver_gone(err: &std::io::Error) -> bool {
    matches!(err.kind(), std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotConnected)
}

/// `90` is `EMSGSIZE` on Linux, the only target (`syslog::is_message_too_large` has more).
fn is_message_too_large(err: &std::io::Error) -> bool {
    matches!(err.raw_os_error(), Some(errno) if errno == 90 /* EMSGSIZE, Linux */)
        || err.kind() == std::io::ErrorKind::InvalidInput
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{interner::intern, AttrMap, BodyFormat, LogRecord, MetricRecord, Resource};
    use logit_inputs::statsd::StatsdDecoder;
    use logit_proto::Decoder;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;

    fn batch_with(events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
    }

    fn metric_event(name: &str, kind: MetricKind, attrs: &[(&str, Value)]) -> Event {
        metric_event_at(0, name, kind, attrs)
    }

    fn metric_event_at(ts: i64, name: &str, kind: MetricKind, attrs: &[(&str, Value)]) -> Event {
        let mut attributes = AttrMap::new();
        for (k, v) in attrs {
            attributes.insert(k, v.clone());
        }
        Event::metric(ts, attributes, MetricRecord::new(intern(name), kind))
    }

    fn log_event(ts: i64) -> Event {
        Event::log(
            ts,
            AttrMap::new(),
            logit_core::LogRecord {
                message: Value::str("x"),
                severity: None,
                body_format: logit_core::BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        )
    }

    fn encode(events: Vec<Event>) -> (Vec<String>, EncodeStats) {
        encode_with_format(events, Format::DogStatsd)
    }

    fn encode_with_format(events: Vec<Event>, format: Format) -> (Vec<String>, EncodeStats) {
        let mut encoder = StatsdEncoder::new(format);
        let mut out = MessageBuf::default();
        let stats = encoder.encode_into(&batch_with(events), &mut out);
        let msgs = out.iter().map(|b| String::from_utf8_lossy(b).into_owned()).collect();
        (msgs, stats)
    }

    fn encode_with(encoder: &mut StatsdEncoder, events: Vec<Event>) -> (Vec<String>, EncodeStats) {
        let mut out = MessageBuf::default();
        let stats = encoder.encode_into(&batch_with(events), &mut out);
        let msgs = out.iter().map(|b| String::from_utf8_lossy(b).into_owned()).collect();
        (msgs, stats)
    }

    // -- Grammar ----------------------------------------------------------------------------

    #[test]
    fn a_counter_encodes_as_name_colon_value_pipe_c() {
        let (msgs, stats) = encode(vec![metric_event("hits", MetricKind::counter(3.0), &[])]);
        assert_eq!(stats, EncodeStats::default());
        assert_eq!(msgs, vec!["hits:3|c"]);
    }

    #[test]
    fn a_gauge_encodes_as_name_colon_value_pipe_g() {
        let (msgs, _) = encode(vec![metric_event("load", MetricKind::Gauge(0.75), &[])]);
        assert_eq!(msgs, vec!["load:0.75|g"]);
    }

    #[test]
    fn an_event_with_no_metrics_is_skipped_and_counted_rather_than_encoded_as_an_empty_line() {
        let (msgs, stats) = encode(vec![log_event(0)]);
        assert!(msgs.is_empty());
        assert_eq!(stats.skipped_no_metrics, 1);
    }

    /// A plain log line counts only `skipped_no_metrics`, not `tags_dropped_dialect` too, which a
    /// `syslog_in -> statsd_out` relay would otherwise inflate on every line.
    #[test]
    fn a_plain_log_event_under_statsd_is_skipped_without_touching_tags_dropped_dialect() {
        let mut attributes = AttrMap::new();
        attributes.insert("host", "web1");
        attributes.insert("env", "prod");
        attributes.insert("service", "api");
        let event = Event::log(
            0,
            attributes,
            LogRecord {
                message: Value::str("hello"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        let (msgs, stats) = encode_with_format(vec![event], Format::Statsd);
        assert!(msgs.is_empty());
        assert_eq!(stats.skipped_no_metrics, 1);
        assert_eq!(stats.tags_dropped_dialect, 0);
    }

    #[test]
    fn no_sample_rate_segment_is_ever_emitted() {
        let (msgs, _) = encode(vec![metric_event("hits", MetricKind::counter(3.0), &[])]);
        assert!(!msgs[0].contains('@'));
    }

    #[test]
    fn no_timestamp_segment_is_ever_emitted() {
        let (msgs, _) = encode(vec![metric_event("hits", MetricKind::counter(3.0), &[])]);
        assert!(!msgs[0].contains('T'));
    }

    #[test]
    fn encoding_the_same_batch_twice_produces_byte_identical_output() {
        let events =
            vec![metric_event("hits", MetricKind::counter(3.0), &[("env", "prod".into())])];
        let (first, _) = encode_with_format(events.clone(), Format::DogStatsd);
        let (second, _) = encode_with_format(events, Format::DogStatsd);
        assert_eq!(first, second);
    }

    // -- Tags ---------------------------------------------------------------------------------

    #[test]
    fn dogstatsd_tags_render_as_one_hash_prefixed_comma_separated_segment() {
        let (msgs, _) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[("env", "prod".into()), ("host", "web1".into())],
        )]);
        assert_eq!(msgs[0], "hits:1|c|#env:prod,host:web1");
    }

    #[test]
    fn a_bool_true_attribute_encodes_as_a_bare_tag_not_key_colon_true() {
        let (msgs, _) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[("urgent", true.into())],
        )]);
        assert_eq!(msgs[0], "hits:1|c|#urgent");
    }

    #[test]
    fn a_bool_false_attribute_encodes_as_key_colon_false() {
        let (msgs, _) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[("verified", false.into())],
        )]);
        assert_eq!(msgs[0], "hits:1|c|#verified:false");
    }

    #[test]
    fn plain_statsd_format_omits_the_tag_segment_entirely() {
        let (msgs, stats) = encode_with_format(
            vec![metric_event("hits", MetricKind::counter(1.0), &[("env", "prod".into())])],
            Format::Statsd,
        );
        assert_eq!(msgs[0], "hits:1|c");
        assert_eq!(stats.tags_dropped_dialect, 1);
    }

    // -- Multi-value tags (a repeated tag key) ---------------------------------------------------

    /// `encode` with a non-default `Resource`, for the resource⊕event precedence tests.
    fn encode_with_resource(resource: Resource, events: Vec<Event>) -> (Vec<String>, EncodeStats) {
        let mut encoder = StatsdEncoder::new(Format::DogStatsd);
        let mut out = MessageBuf::default();
        let batch = EventBatch { resource: Arc::new(resource), scope: None, events };
        let stats = encoder.encode_into(&batch, &mut out);
        let msgs = out.iter().map(|b| String::from_utf8_lossy(b).into_owned()).collect();
        (msgs, stats)
    }

    #[test]
    fn an_array_tag_expands_to_one_tag_per_element_in_array_order() {
        let (msgs, stats) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[("team", Value::Array(vec![Value::str("a"), Value::str("b")]))],
        )]);
        assert_eq!(msgs[0], "hits:1|c|#team:a,team:b");
        assert_eq!(stats, EncodeStats::default());
    }

    /// `#urgent,urgent:1`: a bare and a valued token sharing a key both survive, in array order.
    #[test]
    fn an_array_tag_of_a_bare_and_a_valued_element_keeps_both_forms_in_order() {
        let (msgs, _) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[("urgent", Value::Array(vec![Value::Bool(true), Value::str("1")]))],
        )]);
        assert_eq!(msgs[0], "hits:1|c|#urgent,urgent:1");

        let (msgs, _) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[("urgent", Value::Array(vec![Value::str("1"), Value::Bool(true)]))],
        )]);
        assert_eq!(msgs[0], "hits:1|c|#urgent:1,urgent");
    }

    #[test]
    fn an_array_tag_of_numbers_gets_the_same_formatting_a_scalar_number_gets() {
        let (msgs, _) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[("shard", Value::Array(vec![Value::I64(1), Value::I64(2)]))],
        )]);
        assert_eq!(msgs[0], "hits:1|c|#shard:1,shard:2");
    }

    #[test]
    fn each_element_of_an_array_tag_is_sanitized_on_its_own() {
        let (msgs, _) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[("team", Value::Array(vec![Value::str("a@b"), Value::str("c,d")]))],
        )]);
        assert_eq!(msgs[0], "hits:1|c|#team:a_b,team:c_d");
    }

    #[test]
    fn an_array_tags_key_is_sanitized_identically_for_every_element() {
        let mut attrs = AttrMap::new();
        attrs.insert("a:b", Value::Array(vec![Value::str("x"), Value::str("y")]));
        let event =
            Event::metric(0, attrs, MetricRecord::new(intern("hits"), MetricKind::counter(1.0)));
        let (msgs, _) = encode(vec![event]);
        assert_eq!(msgs[0], "hits:1|c|#a_b:x,a_b:y");
    }

    #[test]
    fn an_unrepresentable_array_element_is_skipped_and_counted_per_element() {
        let (msgs, stats) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[(
                "team",
                Value::Array(vec![
                    Value::str("a"),
                    Value::Null,
                    Value::Map(Box::new(AttrMap::new())),
                    Value::Array(vec![Value::str("nested")]),
                ]),
            )],
        )]);
        assert_eq!(msgs[0], "hits:1|c|#team:a");
        assert_eq!(
            stats.tags_dropped_unrepresentable, 3,
            "the counter's unit is a wire tag: `Null`, `Map` and a nested `Array` each cost one"
        );
    }

    #[test]
    fn an_all_unrepresentable_array_tag_emits_no_tag_segment_at_all() {
        let (msgs, stats) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[("team", Value::Array(vec![Value::Null, Value::Timestamp(1)]))],
        )]);
        assert_eq!(msgs[0], "hits:1|c", "no `|#`, and no stray `,` inside one either");
        assert_eq!(stats.tags_dropped_unrepresentable, 2);
    }

    #[test]
    fn an_empty_array_tag_emits_no_tag_and_counts_nothing() {
        let (msgs, stats) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[("team", Value::Array(Vec::new()))],
        )]);
        assert_eq!(msgs[0], "hits:1|c");
        assert_eq!(stats, EncodeStats::default());
    }

    /// Calls [`push_one_tag`] directly: an earlier tag's suffix survives byte-identical when every
    /// element drops, which an end-to-end assertion can't isolate.
    #[test]
    fn an_all_unrepresentable_array_leaves_a_non_empty_tag_suffix_byte_identical() {
        let mut suffix = String::from("env:prod");
        let mut scratch = String::new();
        let mut stats = EncodeStats::default();
        for element in [Value::Null, Value::Map(Box::new(AttrMap::new()))] {
            assert!(!push_one_tag(&mut suffix, &mut scratch, "team", &element, &mut stats));
        }
        assert_eq!(suffix, "env:prod");
        assert_eq!(stats.tags_dropped_unrepresentable, 2);
    }

    /// No encode-side dedupe (module doc's "Multi-value tags").
    #[test]
    fn duplicate_array_elements_are_not_deduped_on_encode() {
        let (msgs, stats) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[("team", Value::Array(vec![Value::str("a"), Value::str("a")]))],
        )]);
        assert_eq!(msgs[0], "hits:1|c|#team:a,team:a");
        assert_eq!(stats, EncodeStats::default());
    }

    #[test]
    fn an_event_level_array_tag_overrides_a_resource_level_scalar_whole() {
        let mut resource = Resource::default();
        resource.attributes.insert("team", "resource");
        let (msgs, _) = encode_with_resource(
            resource,
            vec![metric_event(
                "hits",
                MetricKind::counter(1.0),
                &[("team", Value::Array(vec![Value::str("a"), Value::str("b")]))],
            )],
        );
        assert_eq!(msgs[0], "hits:1|c|#team:a,team:b");
    }

    #[test]
    fn an_event_level_scalar_tag_overrides_a_resource_level_array_whole() {
        let mut resource = Resource::default();
        resource.attributes.insert("team", Value::Array(vec![Value::str("a"), Value::str("b")]));
        let (msgs, _) = encode_with_resource(
            resource,
            vec![metric_event("hits", MetricKind::counter(1.0), &[("team", "c".into())])],
        );
        assert_eq!(msgs[0], "hits:1|c|#team:c");
    }

    #[test]
    fn plain_statsd_format_counts_a_multi_value_tag_once_per_element() {
        let (msgs, stats) = encode_with_format(
            vec![metric_event(
                "hits",
                MetricKind::counter(1.0),
                &[("team", Value::Array(vec![Value::str("a"), Value::str("b")]))],
            )],
            Format::Statsd,
        );
        assert_eq!(msgs[0], "hits:1|c");
        assert_eq!(
            stats.tags_dropped_dialect, 2,
            "the counter's unit is a wire tag, so a two-element array costs two, not one"
        );

        let (_, stats) = encode_with_format(
            vec![metric_event(
                "hits",
                MetricKind::counter(1.0),
                &[("team", Value::Array(Vec::new()))],
            )],
            Format::Statsd,
        );
        assert_eq!(stats.tags_dropped_dialect, 0);
    }

    // -- Sanitization ---------------------------------------------------------------------------

    #[test]
    fn a_colon_in_a_tag_value_survives() {
        let (msgs, _) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[("range", "a:b".into())],
        )]);
        assert_eq!(msgs[0], "hits:1|c|#range:a:b");
    }

    #[test]
    fn a_colon_in_a_tag_key_is_replaced() {
        let mut attrs = AttrMap::new();
        attrs.insert("a:b", "x");
        let event =
            Event::metric(0, attrs, MetricRecord::new(intern("hits"), MetricKind::counter(1.0)));
        let (msgs, _) = encode(vec![event]);
        assert_eq!(msgs[0], "hits:1|c|#a_b:x");
    }

    #[test]
    fn an_embedded_newline_in_a_metric_name_cannot_forge_a_second_metric_line() {
        let (msgs, _) = encode(vec![metric_event("a\nb", MetricKind::counter(1.0), &[])]);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], "a_b:1|c");
    }

    #[test]
    fn a_metric_name_that_sanitizes_to_nothing_drops_the_metric_and_counts_it() {
        // Only an empty name sanitizes to nothing; `:` alone becomes `_`, a valid name.
        let (msgs, stats) = encode(vec![metric_event("", MetricKind::counter(1.0), &[])]);
        assert!(msgs.is_empty());
        assert_eq!(stats.dropped_empty_name, 1);
    }

    // -- Values and kinds -----------------------------------------------------------------------

    #[test]
    fn a_non_finite_counter_value_is_dropped_rather_than_written_as_the_text_nan() {
        let (msgs, stats) = encode(vec![metric_event("hits", MetricKind::counter(f64::NAN), &[])]);
        assert!(msgs.is_empty());
        assert_eq!(stats.dropped_unencodable_value, 1);
    }

    /// A `NO_RECORDED_VALUE`-flagged point is dropped and counted, never written as a fabricated
    /// `name:0|g`.
    #[test]
    fn a_no_recorded_value_point_is_dropped_and_counted() {
        let mut flagged = metric_event("conns", MetricKind::Gauge(0.0), &[]);
        flagged.metrics[0].flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
        let (msgs, stats) = encode(vec![flagged]);
        assert!(msgs.is_empty(), "a flagged point must not be written at all: {msgs:?}");
        assert_eq!(stats.dropped_no_recorded_value, 1);
    }

    #[test]
    fn a_gauge_delta_is_dropped_by_default() {
        let (msgs, stats) = encode(vec![metric_event("conns", MetricKind::GaugeDelta(5.0), &[])]);
        assert!(msgs.is_empty());
        assert_eq!(stats.dropped_gauge_delta, 1);
    }

    #[test]
    fn a_gauge_delta_encodes_as_a_signed_value_only_under_relative_gauges_true() {
        let mut encoder = StatsdEncoder::new(Format::DogStatsd).with_relative_gauges(true);
        let (msgs, stats) = encode_with(
            &mut encoder,
            vec![metric_event("conns", MetricKind::GaugeDelta(5.0), &[])],
        );
        assert_eq!(stats.dropped_gauge_delta, 0);
        assert_eq!(msgs[0], "conns:+5|g");
    }

    #[test]
    fn a_positive_gauge_delta_carries_an_explicit_plus_sign() {
        let mut encoder = StatsdEncoder::new(Format::DogStatsd).with_relative_gauges(true);
        let (msgs, _) = encode_with(
            &mut encoder,
            vec![metric_event("conns", MetricKind::GaugeDelta(5.0), &[])],
        );
        assert!(msgs[0].starts_with("conns:+5"));
    }

    #[test]
    fn a_negative_gauge_delta_needs_no_extra_sign_from_float_formatting() {
        let mut encoder = StatsdEncoder::new(Format::DogStatsd).with_relative_gauges(true);
        let (msgs, _) = encode_with(
            &mut encoder,
            vec![metric_event("conns", MetricKind::GaugeDelta(-5.0), &[])],
        );
        assert_eq!(msgs[0], "conns:-5|g");
    }

    #[test]
    fn a_negative_absolute_gauge_never_renders_as_a_bare_minus() {
        let (msgs, _) = encode(vec![metric_event("free", MetricKind::Gauge(-5.0), &[])]);
        assert_eq!(msgs.len(), 1, "the pair is one MessageBuf entry");
        assert_eq!(msgs[0], "free:0|g\nfree:-5|g");
    }

    #[test]
    fn a_negative_zero_gauge_renders_as_a_plain_zero_not_a_pair() {
        let (msgs, _) = encode(vec![metric_event("free", MetricKind::Gauge(-0.0), &[])]);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], "free:0|g");
    }

    #[test]
    fn distribution_set_histogram_and_summary_each_drop_with_a_clear_message() {
        let events = vec![
            metric_event("d", MetricKind::Distribution(logit_core::DdSketch::new()), &[]),
            metric_event("s", MetricKind::Set(logit_core::HyperLogLog::default()), &[]),
            metric_event(
                "h",
                MetricKind::Histogram(logit_core::Histogram {
                    buckets: vec![],
                    temporality: Temporality::Cumulative,
                    sum: None,
                    min: None,
                    max: None,
                }),
                &[],
            ),
            metric_event(
                "q",
                MetricKind::Summary(logit_core::Summary { quantiles: vec![], count: 0, sum: 0.0 }),
                &[],
            ),
        ];
        let (msgs, stats) = encode(events);
        assert!(msgs.is_empty());
        assert_eq!(stats.dropped_unsupported_kind, 4);
    }

    #[test]
    fn exponential_histogram_drops_with_a_clear_message() {
        let events = vec![metric_event(
            "e",
            MetricKind::ExponentialHistogram(logit_core::ExpHistogram {
                scale: 0,
                zero_count: 0,
                zero_threshold: 0.0,
                positive: (0, vec![]),
                negative: (0, vec![]),
                temporality: Temporality::Cumulative,
                count: 0,
                sum: None,
                min: None,
                max: None,
            }),
            &[],
        )];
        let (msgs, stats) = encode(events);
        assert!(msgs.is_empty());
        assert_eq!(stats.dropped_unsupported_kind, 1);
    }

    #[test]
    fn a_cumulative_sum_is_dropped_and_a_delta_monotonic_sum_still_encodes_as_c() {
        let (msgs, stats) = encode(vec![
            metric_event(
                "cumulative",
                MetricKind::Sum(logit_core::Sum {
                    value: 5.0,
                    temporality: Temporality::Cumulative,
                    monotonic: true,
                }),
                &[],
            ),
            metric_event("delta", MetricKind::counter(3.0), &[]),
        ]);
        assert_eq!(msgs, vec!["delta:3|c"]);
        assert_eq!(stats.dropped_unsupported_kind, 1);
    }

    #[test]
    fn a_dropped_distribution_does_not_take_a_healthy_counter_on_the_same_event_with_it() {
        let mut event = metric_event("ok", MetricKind::counter(1.0), &[]);
        event.metrics.push(MetricRecord::new(
            intern("bad"),
            MetricKind::Distribution(logit_core::DdSketch::new()),
        ));
        let (msgs, stats) = encode(vec![event]);
        assert_eq!(msgs, vec!["ok:1|c"]);
        assert_eq!(stats.dropped_unsupported_kind, 1);
    }

    // -- Samples --------------------------------------------------------------------------------

    #[test]
    fn a_single_value_samples_metric_encodes_as_name_colon_value_pipe_ms_by_default() {
        let (msgs, stats) = encode(vec![metric_event(
            "timer",
            MetricKind::Samples(logit_core::Samples::new([12.5])),
            &[],
        )]);
        assert_eq!(msgs, vec!["timer:12.5|ms"]);
        assert_eq!(stats, EncodeStats::default());
    }

    #[test]
    fn a_multi_value_samples_metric_encodes_as_one_multi_value_line_under_dogstatsd() {
        let (msgs, _) = encode(vec![metric_event(
            "timer",
            MetricKind::Samples(logit_core::Samples::new([1.0, 2.0, 3.0])),
            &[],
        )]);
        assert_eq!(msgs, vec!["timer:1:2:3|ms"]);
    }

    #[test]
    fn statsd_type_attribute_selects_the_wire_type_letter() {
        for (wire_type, expected) in [("ms", "ms"), ("h", "h"), ("d", "d")] {
            let (msgs, _) = encode(vec![metric_event(
                "timer",
                MetricKind::Samples(logit_core::Samples::new([1.0])),
                &[("statsd.type", Value::str(wire_type))],
            )]);
            assert_eq!(msgs, vec![format!("timer:1|{expected}")]);
        }
    }

    #[test]
    fn an_unrecognized_statsd_type_attribute_falls_back_to_ms() {
        let (msgs, _) = encode(vec![metric_event(
            "timer",
            MetricKind::Samples(logit_core::Samples::new([1.0])),
            &[("statsd.type", Value::str("bogus"))],
        )]);
        assert_eq!(msgs, vec!["timer:1|ms"]);
    }

    #[test]
    fn a_sample_rate_other_than_one_is_written_as_at_rate() {
        let mut samples = logit_core::Samples::new([1.0]);
        samples.sample_rate = 0.5;
        let (msgs, _) = encode(vec![metric_event("timer", MetricKind::Samples(samples), &[])]);
        assert_eq!(msgs, vec!["timer:1|ms|@0.5"]);
    }

    #[test]
    fn a_sample_rate_of_one_omits_at_rate() {
        let (msgs, _) = encode(vec![metric_event(
            "timer",
            MetricKind::Samples(logit_core::Samples::new([1.0])),
            &[],
        )]);
        assert!(!msgs[0].contains('@'));
    }

    #[test]
    fn statsd_dialect_splits_multi_value_samples_and_normalizes_h_and_d_to_ms() {
        let (msgs, stats) = encode_with_format(
            vec![metric_event(
                "timer",
                MetricKind::Samples(logit_core::Samples::new([1.0, 2.0])),
                &[("statsd.type", Value::str("h"))],
            )],
            Format::Statsd,
        );
        assert_eq!(msgs, vec!["timer:1|ms", "timer:2|ms"]);
        assert_eq!(stats.type_normalized_dialect, 1);
    }

    #[test]
    fn statsd_dialect_does_not_count_normalization_when_the_wire_type_was_already_ms() {
        let (_, stats) = encode_with_format(
            vec![metric_event("timer", MetricKind::Samples(logit_core::Samples::new([1.0])), &[])],
            Format::Statsd,
        );
        assert_eq!(stats.type_normalized_dialect, 0);
    }

    #[test]
    fn a_non_finite_sample_value_is_dropped_and_counted_per_value_others_still_encode() {
        let (msgs, stats) = encode(vec![metric_event(
            "timer",
            MetricKind::Samples(logit_core::Samples::new([1.0, f64::NAN, 3.0])),
            &[],
        )]);
        assert_eq!(msgs, vec!["timer:1:3|ms"]);
        assert_eq!(stats.dropped_unencodable_value, 1);
    }

    #[test]
    fn an_out_of_range_sample_rate_omits_at_rate_and_is_counted() {
        let mut samples = logit_core::Samples::new([1.0]);
        samples.sample_rate = -1.0;
        let (msgs, stats) = encode(vec![metric_event("timer", MetricKind::Samples(samples), &[])]);
        assert_eq!(msgs, vec!["timer:1|ms"]);
        assert_eq!(stats.dropped_unencodable_value, 1);
    }

    #[test]
    fn an_empty_samples_list_emits_nothing_and_is_counted() {
        let (msgs, stats) = encode(vec![metric_event(
            "timer",
            MetricKind::Samples(logit_core::Samples::new([])),
            &[],
        )]);
        assert!(msgs.is_empty());
        assert_eq!(stats.dropped_unencodable_value, 1);
    }

    // -- SetMembers -----------------------------------------------------------------------------

    #[test]
    fn a_single_member_set_members_metric_encodes_as_name_colon_member_pipe_s() {
        let (msgs, stats) = encode(vec![metric_event(
            "tags",
            MetricKind::SetMembers(vec![bytes::Bytes::from_static(b"alice")]),
            &[],
        )]);
        assert_eq!(msgs, vec!["tags:alice|s"]);
        assert_eq!(stats, EncodeStats::default());
    }

    #[test]
    fn a_multi_member_set_members_metric_encodes_as_one_line_per_member_in_both_formats() {
        let members = || {
            MetricKind::SetMembers(vec![
                bytes::Bytes::from_static(b"alice"),
                bytes::Bytes::from_static(b"bob"),
            ])
        };
        let (msgs, _) = encode(vec![metric_event("tags", members(), &[])]);
        assert_eq!(msgs, vec!["tags:alice|s", "tags:bob|s"]);

        let (msgs, _) =
            encode_with_format(vec![metric_event("tags", members(), &[])], Format::Statsd);
        assert_eq!(msgs, vec!["tags:alice|s", "tags:bob|s"]);
    }

    #[test]
    fn a_non_utf8_set_member_is_lossily_sanitized_and_counted() {
        let (msgs, stats) = encode(vec![metric_event(
            "tags",
            MetricKind::SetMembers(vec![bytes::Bytes::from_static(&[0xff, 0xfe])]),
            &[],
        )]);
        assert_eq!(msgs.len(), 1);
        assert!(msgs[0].starts_with("tags:"));
        assert!(msgs[0].ends_with("|s"));
        assert_eq!(stats.members_sanitized, 1);
    }

    #[test]
    fn a_member_containing_a_forbidden_character_is_sanitized_and_counted() {
        let (msgs, stats) = encode(vec![metric_event(
            "tags",
            MetricKind::SetMembers(vec![bytes::Bytes::from_static(b"a|b")]),
            &[],
        )]);
        assert_eq!(msgs, vec!["tags:a_b|s"]);
        assert_eq!(stats.members_sanitized, 1);
    }

    /// An unsanitized `a:b` would render `tags:a:b|s`, which decodes as two members.
    #[test]
    fn a_member_containing_a_colon_is_sanitized_and_counted() {
        let (msgs, stats) = encode(vec![metric_event(
            "tags",
            MetricKind::SetMembers(vec![bytes::Bytes::from_static(b"a:b")]),
            &[],
        )]);
        assert_eq!(msgs, vec!["tags:a_b|s"]);
        assert_eq!(stats.members_sanitized, 1);
    }

    #[test]
    fn a_member_containing_a_colon_re_decodes_as_one_member_not_two() {
        let (msgs, _) = encode(vec![metric_event(
            "tags",
            MetricKind::SetMembers(vec![bytes::Bytes::from_static(b"a:b")]),
            &[],
        )]);
        assert_eq!(msgs, vec!["tags:a_b|s"]);
        let events = decode_one(&msgs[0]);
        assert_eq!(events.len(), 1);
        assert!(
            matches!(&events[0].metrics[0].kind, MetricKind::SetMembers(m) if m.len() == 1 && m[0] == "a_b")
        );
    }

    /// `@`, `,`, and a space, which the name rule would substitute, survive in a member.
    #[test]
    fn members_with_at_hash_comma_or_a_space_round_trip_byte_for_byte() {
        for line in ["users:a@b|s", "users:a,b|s", "users:a b|s"] {
            let original = decode_one(line);
            let (msgs, stats) = encode(original.clone());
            assert_eq!(msgs, vec![line], "expected {line:?} to round-trip byte for byte");
            assert_eq!(
                stats.members_sanitized, 0,
                "no substitution should have happened for {line:?}"
            );
            let relayed = decode_one(&msgs[0]);
            // Metrics only: each decode stamps its own receipt time (no `|T` on this line).
            assert_eq!(
                relayed[0].metrics, original[0].metrics,
                "decode(encode(decode(line))) should equal decode(line) for {line:?}"
            );
        }
    }

    /// A real `users:a:b|s` decodes as two members, each re-encoding as its own untouched line.
    #[test]
    fn a_member_split_by_a_real_colon_decodes_as_two_members_each_re_encoding_untouched() {
        let events = decode_one("users:a:b|s");
        assert_eq!(events.len(), 1, "one SetMembers event for the whole line");
        match &events[0].metrics[0].kind {
            MetricKind::SetMembers(m) => {
                assert_eq!(
                    m,
                    &vec![bytes::Bytes::from_static(b"a"), bytes::Bytes::from_static(b"b")]
                )
            }
            other => panic!("expected SetMembers, got {other:?}"),
        }
        let (msgs, stats) = encode(events);
        assert_eq!(msgs, vec!["users:a|s", "users:b|s"]);
        assert_eq!(stats.members_sanitized, 0);
    }

    #[test]
    fn an_empty_set_members_list_emits_nothing_and_is_counted() {
        let (msgs, stats) = encode(vec![metric_event("tags", MetricKind::SetMembers(vec![]), &[])]);
        assert!(msgs.is_empty());
        assert_eq!(stats.dropped_unencodable_value, 1);
    }

    // -- container id and timestamp markers ------------------------------------------------------

    #[test]
    fn a_container_id_attribute_appends_pipe_c_under_dogstatsd() {
        let (msgs, _) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[("statsd.container_id", Value::str("abcd1234"))],
        )]);
        assert_eq!(msgs, vec!["hits:1|c|c:abcd1234"]);
    }

    /// A resource-only carrier (a `set` transform's `resource:` block) reaches the wire too.
    #[test]
    fn a_container_id_on_the_resource_is_emitted_as_pipe_c_under_dogstatsd() {
        let mut resource_attrs = AttrMap::new();
        resource_attrs.insert("statsd.container_id", Value::str("res-id"));
        let resource = Arc::new(Resource { attributes: resource_attrs, ..Default::default() });
        let batch = EventBatch {
            resource,
            scope: None,
            events: vec![metric_event("hits", MetricKind::counter(1.0), &[])],
        };
        let mut encoder = StatsdEncoder::new(Format::DogStatsd);
        let mut out = MessageBuf::default();
        encoder.encode_into(&batch, &mut out);
        let msgs: Vec<String> =
            out.iter().map(|b| std::str::from_utf8(b).unwrap().to_string()).collect();
        assert_eq!(msgs, vec!["hits:1|c|c:res-id"]);
    }

    #[test]
    fn a_timestamp_marker_appends_pipe_t_seconds_under_dogstatsd() {
        let (msgs, _) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[("statsd.timestamp", Value::U64(5))],
        )]);
        assert_eq!(msgs, vec!["hits:1|c|T5"]);
    }

    #[test]
    fn container_id_and_timestamp_come_after_the_tag_segment() {
        let (msgs, _) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[
                ("env", "prod".into()),
                ("statsd.container_id", Value::str("abcd1234")),
                ("statsd.timestamp", Value::U64(5)),
            ],
        )]);
        assert_eq!(msgs, vec!["hits:1|c|#env:prod|c:abcd1234|T5"]);
    }

    /// A non-`U64` `statsd.timestamp` (a Lua or cross-protocol attribute) emits no `|T`.
    #[test]
    fn a_non_u64_statsd_timestamp_value_is_not_emitted() {
        let (msgs, _) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[("statsd.timestamp", true.into())],
        )]);
        assert!(!msgs[0].contains('T'));
    }

    #[test]
    fn container_id_and_timestamp_are_dropped_and_counted_under_plain_statsd() {
        let (msgs, stats) = encode_with_format(
            vec![metric_event(
                "hits",
                MetricKind::counter(1.0),
                &[
                    ("statsd.container_id", Value::str("abcd1234")),
                    ("statsd.timestamp", Value::U64(5)),
                ],
            )],
            Format::Statsd,
        );
        assert_eq!(msgs, vec!["hits:1|c"]);
        assert_eq!(stats.dropped_dialect_fields, 2);
    }

    #[test]
    fn statsd_dot_attributes_never_become_generic_tags() {
        let (msgs, _) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[
                ("statsd.type", Value::str("ms")),
                ("statsd.container_id", Value::str("abcd1234")),
                ("statsd.timestamp", Value::U64(0)),
                ("env", "prod".into()),
            ],
        )]);
        // Carriers appear only in their own segments, never beside `env`.
        assert_eq!(msgs, vec!["hits:1|c|#env:prod|c:abcd1234|T0"]);
    }

    // -- external data (`|e:`) and cardinality (`|card:`) ---------------------------------------

    /// `|c:`, `|e:`, `|card:`, `|T`, in that order, after the tag segment.
    #[test]
    fn external_data_and_cardinality_come_between_the_container_id_and_the_timestamp() {
        let (msgs, stats) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[
                ("env", "prod".into()),
                ("statsd.container_id", Value::str("abcd1234")),
                ("statsd.external_data", Value::str("it-false,cn-web,pu-abc")),
                ("statsd.cardinality", Value::str("high")),
                ("statsd.timestamp", Value::U64(5)),
            ],
        )]);
        assert_eq!(stats, EncodeStats::default());
        assert_eq!(
            msgs,
            vec!["hits:1|c|#env:prod|c:abcd1234|e:it-false,cn-web,pu-abc|card:high|T5"]
        );
    }

    #[test]
    fn external_data_alone_and_cardinality_alone_each_render_their_own_segment() {
        let (msgs, _) = encode(vec![
            metric_event(
                "a",
                MetricKind::counter(1.0),
                &[("statsd.external_data", Value::str("x"))],
            ),
            metric_event(
                "b",
                MetricKind::counter(1.0),
                &[("statsd.cardinality", Value::str("low"))],
            ),
        ]);
        assert_eq!(msgs, vec!["a:1|c|e:x", "b:1|c|card:low"]);
    }

    /// Each field is one dropped dialect field per emitted line under `format: statsd`, as `|c:`
    /// and `|T` are.
    #[test]
    fn external_data_and_cardinality_are_dropped_and_counted_under_plain_statsd() {
        let (msgs, stats) = encode_with_format(
            vec![metric_event(
                "hits",
                MetricKind::counter(1.0),
                &[
                    ("statsd.container_id", Value::str("abcd1234")),
                    ("statsd.external_data", Value::str("ext")),
                    ("statsd.cardinality", Value::str("high")),
                    ("statsd.timestamp", Value::U64(5)),
                ],
            )],
            Format::Statsd,
        );
        assert_eq!(msgs, vec!["hits:1|c"]);
        assert_eq!(stats.dropped_dialect_fields, 4);
    }

    /// Cardinality is sanitized as a tag value. External data keeps its own `,` separators (and
    /// `#`, `:`), losing only `|`, control characters, and whitespace
    /// ([`is_forbidden_in_external_data`]).
    #[test]
    fn external_data_and_cardinality_are_sanitized() {
        let (msgs, _) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[
                ("statsd.external_data", Value::str("a|b#c:d\ne,f g")),
                ("statsd.cardinality", Value::str("hi gh,x")),
            ],
        )]);
        assert_eq!(msgs, vec!["hits:1|c|e:a_b#c:d_e,f_g|card:hi_gh_x"]);
    }

    #[test]
    fn external_data_and_cardinality_follow_the_container_id_on_an_event_line() {
        let event = event_line_event(
            "t",
            "x",
            &[
                ("statsd.container_id", Value::str("cid1")),
                ("statsd.external_data", Value::str("ext1")),
                ("statsd.cardinality", Value::str("low")),
                ("env", "prod".into()),
            ],
        );
        let (msgs, _) = encode(vec![event]);
        assert_eq!(msgs, vec!["_e{1,1}:t|x|#env:prod|c:cid1|e:ext1|card:low"]);
    }

    #[test]
    fn external_data_and_cardinality_precede_the_message_on_a_service_check_line() {
        let event = service_check_event(
            "check",
            MetricKind::Gauge(0.0),
            &[
                ("statsd.service_check.status", Value::U64(0)),
                ("statsd.container_id", Value::str("cid1")),
                ("statsd.external_data", Value::str("ext1")),
                ("statsd.cardinality", Value::str("orchestrator")),
                ("statsd.service_check.message", Value::str("all good")),
            ],
        );
        let (msgs, _) = encode(vec![event]);
        assert_eq!(msgs, vec!["_sc|check|0|c:cid1|e:ext1|card:orchestrator|m:all good"]);
    }

    #[test]
    fn external_data_and_cardinality_round_trip_through_the_real_statsd_decoder() {
        for line in [
            "hits:1|c|#env:prod|c:abcd1234|e:it-false,cn-web|card:high|T1700000000",
            "_e{5,4}:title|text|c:cid1|e:ext1|card:low",
            "_sc|check|0|c:cid1|e:ext1|card:none|m:ok",
        ] {
            // Receipt time differs between the two decodes of a line with no wire timestamp.
            let zeroed = |mut events: Vec<Event>| {
                events.iter_mut().for_each(|e| e.timestamp = 0);
                events
            };
            let original = decode_one(line);
            let (msgs, _) = encode(original.clone());
            assert_eq!(msgs, vec![line.to_string()], "byte for byte");
            assert_eq!(
                zeroed(decode_one(&msgs[0])),
                zeroed(original),
                "decode(encode(x)) == x for {line:?}"
            );
        }
    }

    // -- Events -----------------------------------------------------------------------------

    fn event_line_event(title: &str, text: &str, extra_attrs: &[(&str, Value)]) -> Event {
        let mut attributes = AttrMap::new();
        attributes.insert("statsd.event.title", Value::str(title));
        for (k, v) in extra_attrs {
            attributes.insert(k, v.clone());
        }
        Event::log(
            0,
            attributes,
            LogRecord {
                message: Value::str(text),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        )
    }

    #[test]
    fn an_event_with_every_field_renders_the_canonical_line_with_byte_lengths() {
        // "héllo" is 6 UTF-8 bytes but 5 chars, so `tlen` must count bytes.
        let event = event_line_event(
            "héllo",
            "world",
            &[
                ("statsd.timestamp", Value::U64(1_700_000_000)),
                ("statsd.event.host", Value::str("web1")),
                ("statsd.event.priority", Value::str("low")),
                ("statsd.event.alert_type", Value::str("error")),
                ("statsd.event.aggregation_key", Value::str("key1")),
                ("statsd.event.source_type", Value::str("my_app")),
                ("statsd.container_id", Value::str("cid1")),
                ("env", "prod".into()),
            ],
        );
        let (msgs, stats) = encode(vec![event]);
        assert_eq!(stats, EncodeStats::default());
        assert_eq!(
            msgs,
            vec![
                "_e{6,5}:héllo|world|d:1700000000|h:web1|p:low|t:error|k:key1|s:my_app|#env:prod|c:cid1"
            ]
        );
    }

    #[test]
    fn event_text_with_a_real_newline_is_escaped_and_the_length_counts_the_escape() {
        let event = event_line_event("t", "a\nb", &[]);
        let (msgs, _) = encode(vec![event]);
        // "a\nb" (3 bytes) escapes to "a\\nb" (4 bytes): tlen=1, xlen=4.
        assert_eq!(msgs, vec!["_e{1,4}:t|a\\nb"]);
    }

    #[test]
    fn absent_event_carriers_produce_no_optional_fields() {
        let event = event_line_event("t", "x", &[]);
        let (msgs, _) = encode(vec![event]);
        assert_eq!(msgs, vec!["_e{1,1}:t|x"]);
    }

    #[test]
    fn an_invalid_event_priority_is_omitted_and_counted() {
        let event = event_line_event("t", "x", &[("statsd.event.priority", Value::str("urgent"))]);
        let (msgs, stats) = encode(vec![event]);
        assert_eq!(msgs, vec!["_e{1,1}:t|x"]);
        // A dropped field on an emitted line, not a dropped message.
        assert_eq!(stats.dropped_invalid_event_fields, 1);
        assert_eq!(stats.dropped_unencodable_value, 0);
    }

    #[test]
    fn an_invalid_event_alert_type_is_omitted_and_counted() {
        let event =
            event_line_event("t", "x", &[("statsd.event.alert_type", Value::str("critical"))]);
        let (msgs, stats) = encode(vec![event]);
        assert_eq!(msgs, vec!["_e{1,1}:t|x"]);
        assert_eq!(stats.dropped_invalid_event_fields, 1);
        assert_eq!(stats.dropped_unencodable_value, 0);
    }

    #[test]
    fn an_event_with_a_non_string_log_message_is_dropped_and_counted() {
        let mut attributes = AttrMap::new();
        attributes.insert("statsd.event.title", Value::str("t"));
        let event = Event::log(
            0,
            attributes,
            LogRecord {
                message: Value::U64(5),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        let (msgs, stats) = encode(vec![event]);
        assert!(msgs.is_empty());
        assert_eq!(stats.dropped_unencodable_value, 1);
    }

    /// `Value::Str` doesn't enforce UTF-8 at construction, so [`is_dogstatsd_event`] can accept a
    /// title `render_event` can't render; that must be a counted drop, not a panic.
    #[test]
    fn an_event_with_a_non_utf8_title_is_dropped_and_counted_rather_than_panicking() {
        let mut attributes = AttrMap::new();
        attributes.insert("statsd.event.title", Value::Str(bytes::Bytes::from_static(b"\xff")));
        let event = Event::log(
            0,
            attributes,
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
        let (msgs, stats) = encode(vec![event]);
        assert!(msgs.is_empty());
        assert_eq!(stats.dropped_unencodable_value, 1);
    }

    #[test]
    fn event_carriers_never_appear_as_tags() {
        let event = event_line_event(
            "t",
            "x",
            &[("statsd.event.host", Value::str("web1")), ("env", "prod".into())],
        );
        let (msgs, _) = encode(vec![event]);
        assert_eq!(msgs, vec!["_e{1,1}:t|x|h:web1|#env:prod"]);
    }

    #[test]
    fn event_carriers_set_on_the_resource_are_honored() {
        let mut resource_attrs = AttrMap::new();
        resource_attrs.insert("statsd.event.title", Value::str("from-resource"));
        let resource = Arc::new(Resource { attributes: resource_attrs, ..Default::default() });
        let event = Event::log(
            0,
            AttrMap::new(),
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
        let batch = EventBatch { resource, scope: None, events: vec![event] };
        let mut encoder = StatsdEncoder::new(Format::DogStatsd);
        let mut out = MessageBuf::default();
        encoder.encode_into(&batch, &mut out);
        let msgs: Vec<String> =
            out.iter().map(|b| std::str::from_utf8(b).unwrap().to_string()).collect();
        assert_eq!(msgs, vec!["_e{13,1}:from-resource|x"]);
    }

    #[test]
    fn an_oversize_event_line_is_dropped_via_the_existing_oversize_path() {
        let event = event_line_event("t", "x", &[]);
        // "_e{1,1}:t|x" is 11 bytes, longer than this cap.
        let mut encoder = StatsdEncoder::new(Format::DogStatsd).with_max_packet_bytes(5);
        let mut out = MessageBuf::default();
        let stats = encoder.encode_into(&batch_with(vec![event]), &mut out);
        assert!(out.is_empty());
        assert_eq!(stats.dropped_oversize_line, 1);
    }

    // -- Service checks -----------------------------------------------------------------------

    fn service_check_event(name: &str, status: MetricKind, extra_attrs: &[(&str, Value)]) -> Event {
        let mut attributes = AttrMap::new();
        attributes.insert("statsd.service_check.name", Value::str(name));
        for (k, v) in extra_attrs {
            attributes.insert(k, v.clone());
        }
        Event::metric(0, attributes, MetricRecord::new(intern("ignored"), status))
    }

    #[test]
    fn a_service_check_renders_the_canonical_line() {
        let event = service_check_event(
            "my.check",
            MetricKind::Gauge(1.0),
            &[
                ("statsd.timestamp", Value::U64(1_700_000_000)),
                ("statsd.service_check.host", Value::str("web1")),
                ("env", "prod".into()),
                ("statsd.container_id", Value::str("cid1")),
            ],
        );
        let (msgs, stats) = encode(vec![event]);
        assert_eq!(stats, EncodeStats::default());
        assert_eq!(msgs, vec!["_sc|my.check|1|d:1700000000|h:web1|#env:prod|c:cid1"]);
    }

    #[test]
    fn a_service_check_message_containing_a_pipe_is_kept_since_m_is_last() {
        let event = service_check_event(
            "chk",
            MetricKind::Gauge(0.0),
            &[("statsd.service_check.message", Value::str("a|b"))],
        );
        let (msgs, _) = encode(vec![event]);
        assert_eq!(msgs, vec!["_sc|chk|0|m:a|b"]);
    }

    #[test]
    fn service_check_status_attribute_wins_over_the_gauge_value() {
        let event = service_check_event(
            "chk",
            MetricKind::Gauge(3.0),
            &[("statsd.service_check.status", Value::U64(1))],
        );
        let (msgs, _) = encode(vec![event]);
        assert_eq!(msgs, vec!["_sc|chk|1"]);
    }

    #[test]
    fn service_check_status_falls_back_to_the_rounded_gauge_value() {
        let event = service_check_event("chk", MetricKind::Gauge(1.6), &[]);
        let (msgs, _) = encode(vec![event]);
        assert_eq!(msgs, vec!["_sc|chk|2"]);
    }

    #[test]
    fn an_out_of_range_service_check_status_is_dropped_and_counted() {
        let event = service_check_event(
            "chk",
            MetricKind::Gauge(9.0),
            &[("statsd.service_check.status", Value::U64(5))],
        );
        let (msgs, stats) = encode(vec![event]);
        assert!(msgs.is_empty());
        assert_eq!(stats.dropped_invalid_service_check, 1);
    }

    /// An empty name drops the check rather than rendering `_sc||0`.
    #[test]
    fn a_service_check_with_an_empty_name_is_dropped_and_counted() {
        let event = service_check_event("", MetricKind::Gauge(0.0), &[]);
        let (msgs, stats) = encode(vec![event]);
        assert!(msgs.is_empty());
        assert_eq!(stats.dropped_empty_name, 1);
    }

    #[test]
    fn a_non_gauge_first_metric_on_a_service_check_event_is_dropped_and_counted() {
        let mut attributes = AttrMap::new();
        attributes.insert("statsd.service_check.name", Value::str("chk"));
        let event = Event::metric(
            0,
            attributes,
            MetricRecord::new(intern("chk"), MetricKind::counter(1.0)),
        );
        let (msgs, stats) = encode(vec![event]);
        assert!(msgs.is_empty());
        assert_eq!(stats.dropped_invalid_service_check, 1);
    }

    #[test]
    fn a_second_metric_on_a_service_check_event_renders_as_a_normal_line_after_it() {
        let mut attributes = AttrMap::new();
        attributes.insert("statsd.service_check.name", Value::str("chk"));
        let mut event =
            Event::metric(0, attributes, MetricRecord::new(intern("chk"), MetricKind::Gauge(0.0)));
        event.metrics.push(MetricRecord::new(intern("extra"), MetricKind::counter(1.0)));
        let (msgs, _) = encode(vec![event]);
        assert_eq!(msgs, vec!["_sc|chk|0", "extra:1|c"]);
    }

    #[test]
    fn service_check_carriers_never_appear_as_tags() {
        let event = service_check_event(
            "chk",
            MetricKind::Gauge(0.0),
            &[("statsd.service_check.message", Value::str("m")), ("env", "prod".into())],
        );
        let (msgs, _) = encode(vec![event]);
        assert_eq!(msgs, vec!["_sc|chk|0|#env:prod|m:m"]);
    }

    #[test]
    fn events_and_service_checks_are_dropped_under_plain_statsd() {
        let ev = event_line_event("t", "x", &[]);
        let sc = service_check_event("chk", MetricKind::Gauge(0.0), &[]);
        let (msgs, stats) = encode_with_format(vec![ev, sc], Format::Statsd);
        assert!(msgs.is_empty());
        assert_eq!(stats.dropped_dialect_events, 2);
    }

    /// Unlike a plain log line, a real event ran the full walk, so both counters rise (module
    /// doc's "DogStatsD events and service checks" section).
    #[test]
    fn a_real_event_dropped_under_statsd_also_tallies_its_tags_into_tags_dropped_dialect() {
        let ev = event_line_event("t", "x", &[("env", "prod".into())]);
        let (msgs, stats) = encode_with_format(vec![ev], Format::Statsd);
        assert!(msgs.is_empty());
        assert_eq!(stats.dropped_dialect_events, 1);
        assert_eq!(stats.tags_dropped_dialect, 1);
    }

    // -- Packing and framing ------------------------------------------------------------------

    #[tokio::test]
    async fn udp_packs_several_lines_into_one_newline_separated_datagram() {
        let (addr, collector) = udp_collector().await;
        let mut output = StatsdOutput::udp(addr.to_string()).unwrap();
        let batch = batch_with(vec![
            metric_event("a", MetricKind::counter(1.0), &[]),
            metric_event("b", MetricKind::counter(2.0), &[]),
        ]);
        let recv_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            let (n, _) = collector.recv_from(&mut buf).await.unwrap();
            String::from_utf8_lossy(&buf[..n]).into_owned()
        });
        output.send(&batch).await.expect("send should succeed");
        let received =
            tokio::time::timeout(Duration::from_secs(2), recv_task).await.unwrap().unwrap();
        assert_eq!(received, "a:1|c\nb:2|c");
    }

    // -- transport: unix / unix_stream ---------------------------------------------------------

    /// A per-test directory for a socket file, removed on drop.
    struct SocketDir(std::path::PathBuf);

    impl SocketDir {
        fn new(tag: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("los-{tag}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn socket(&self) -> String {
            self.0.join("dsd.socket").display().to_string()
        }
    }

    impl Drop for SocketDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn two_counters() -> EventBatch {
        batch_with(vec![
            metric_event("a", MetricKind::counter(1.0), &[]),
            metric_event("b", MetricKind::counter(2.0), &[]),
        ])
    }

    /// Packs lines into one Unix datagram, as UDP does, and counts it as a datagram.
    #[tokio::test]
    async fn unix_datagram_packs_lines_into_one_datagram_sent_to_the_path() {
        let dir = SocketDir::new("dgram");
        let path = dir.socket();
        let receiver = UnixDatagram::bind(&path).unwrap();
        let mut output = StatsdOutput::unix_datagram(&path, Duration::from_secs(1));
        output.send(&two_counters()).await.expect("send should succeed");
        let mut buf = vec![0u8; 4096];
        let n = tokio::time::timeout(Duration::from_secs(2), receiver.recv(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf[..n], b"a:1|c\nb:2|c");
    }

    /// `max_packet_bytes` caps a Unix datagram as it caps a UDP one.
    #[tokio::test]
    async fn unix_datagram_starts_a_new_datagram_at_max_packet_bytes() {
        let dir = SocketDir::new("dgram-cap");
        let path = dir.socket();
        let receiver = UnixDatagram::bind(&path).unwrap();
        let mut output =
            StatsdOutput::unix_datagram(&path, Duration::from_secs(1)).with_max_packet_bytes(10);
        output.send(&two_counters()).await.expect("send should succeed");
        let mut buf = vec![0u8; 4096];
        for expected in [&b"a:1|c"[..], b"b:2|c"] {
            let n = receiver.recv(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], expected);
        }
    }

    /// No socket at the path: nothing was sent, so the failure is `Fault::Clean`.
    #[tokio::test]
    async fn unix_datagram_to_a_missing_socket_fails_clean() {
        let dir = SocketDir::new("dgram-missing");
        let mut output = StatsdOutput::unix_datagram(dir.socket(), Duration::from_secs(1));
        let err = output.send(&two_counters()).await.expect_err("no receiver");
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
    }

    /// A receiver that never reads fills its queue; the next send waits at most `send_timeout`
    /// and then fails rather than hanging the sink.
    #[tokio::test]
    async fn unix_datagram_send_to_a_full_receiver_times_out() {
        let dir = SocketDir::new("dgram-full");
        let path = dir.socket();
        let _receiver = UnixDatagram::bind(&path).unwrap();
        let mut output = StatsdOutput::unix_datagram(&path, Duration::from_millis(200))
            .with_max_packet_bytes(16); // one line per datagram
        let events =
            (0..5000).map(|i| metric_event("m", MetricKind::counter(f64::from(i)), &[])).collect();
        let started = std::time::Instant::now();
        let err = tokio::time::timeout(Duration::from_secs(10), output.send(&batch_with(events)))
            .await
            .expect("the send must not hang")
            .expect_err("a queue that never drains must fail the send");
        assert!(format!("{err:#}").contains("did not take a datagram"), "{err:#}");
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous, "earlier datagrams landed");
    }

    /// Other clients' datagrams fill the receiver's queue; the pending send parks until the
    /// receiver drains, then completes. Linux parks a sender on a full receiver only when it's
    /// connected to it; an unconnected one is reported writable again right after each `EAGAIN`
    /// and retries in a busy loop, which the poll count catches.
    #[tokio::test]
    async fn unix_datagram_send_behind_other_clients_completes_when_the_receiver_drains() {
        let dir = SocketDir::new("dgram-fanin");
        let path = dir.socket();
        let receiver = UnixDatagram::bind(&path).unwrap();
        // One filler socket is capped by its own send buffer before the receiver's queue length,
        // so add fillers until a fresh one can't queue a single datagram.
        let mut fillers = Vec::new();
        loop {
            assert!(fillers.len() < 256, "the receiver's queue never filled");
            let filler = std::os::unix::net::UnixDatagram::unbound().unwrap();
            filler.connect(&path).unwrap();
            filler.set_nonblocking(true).unwrap();
            let mut sent = 0;
            loop {
                match filler.send(b"filler:1|c") {
                    Ok(_) => sent += 1,
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(err) => panic!("filler send: {err}"),
                }
            }
            fillers.push(filler);
            if sent == 0 {
                break;
            }
        }

        let send_timeout = Duration::from_secs(5);
        let mut output = StatsdOutput::unix_datagram(&path, send_timeout);
        let batch = batch_with(vec![metric_event("mine", MetricKind::counter(1.0), &[])]);
        let drain = async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let mut buf = vec![0u8; 4096];
            loop {
                let n = tokio::time::timeout(Duration::from_secs(3), receiver.recv(&mut buf))
                    .await
                    .expect("the sink's datagram should arrive once the queue drains")
                    .unwrap();
                if &buf[..n] == b"mine:1|c" {
                    break;
                }
            }
        };
        let polls = std::cell::Cell::new(0u32);
        let mut send = std::pin::pin!(output.send(&batch));
        let counted = std::future::poll_fn(|cx| {
            polls.set(polls.get() + 1);
            std::future::Future::poll(send.as_mut(), cx)
        });
        let started = std::time::Instant::now();
        let (result, ()) = tokio::join!(counted, drain);
        result.expect("the send should complete once the receiver drains");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "woken by the drain, not by send_timeout: {:?}",
            started.elapsed()
        );
        assert!(polls.get() < 50, "the send spun instead of parking: {} polls", polls.get());
    }

    /// A receiver restarted at the same path between batches receives the second batch: the
    /// refused send on the old connection reconnects and retries once, counted as a reconnect.
    #[tokio::test]
    async fn unix_datagram_follows_a_receiver_rebound_at_the_same_path() {
        let dir = SocketDir::new("dgram-rebind");
        let path = dir.socket();
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("statsd_out", "statsd_out", "sink");
        let mut output =
            StatsdOutput::unix_datagram(&path, Duration::from_secs(1)).with_telemetry(telemetry);
        let mut buf = vec![0u8; 4096];

        let first = UnixDatagram::bind(&path).unwrap();
        let batch = batch_with(vec![metric_event("first", MetricKind::counter(1.0), &[])]);
        output.send(&batch).await.expect("first send");
        let n = first.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"first:1|c");

        drop(first);
        std::fs::remove_file(&path).unwrap();
        let second = UnixDatagram::bind(&path).unwrap();
        let batch = batch_with(vec![metric_event("second", MetricKind::counter(2.0), &[])]);
        output.send(&batch).await.expect("the send should reconnect to the new receiver");
        let n = tokio::time::timeout(Duration::from_secs(2), second.recv(&mut buf))
            .await
            .expect("the new receiver should get the second batch")
            .unwrap();
        assert_eq!(&buf[..n], b"second:2|c");
        assert_eq!(reconnects_in(registry.drain(0)), Some(1.0));
    }

    #[test]
    fn a_length_prefixed_frame_packs_lines_into_le_prefixed_packets() {
        let mut lines = MessageBuf::default();
        for line in ["a:1|c", "b:2|c", "ccc:3|c"] {
            lines.push(line);
        }
        let mut frame = Vec::new();
        // "a:1|c\nb:2|c" is 11 bytes; the third line doesn't fit an 11-byte packet.
        build_length_prefixed_frame(&lines, 11, &mut frame);
        let mut expected = 11u32.to_le_bytes().to_vec();
        expected.extend_from_slice(b"a:1|c\nb:2|c");
        expected.extend_from_slice(&7u32.to_le_bytes());
        expected.extend_from_slice(b"ccc:3|c");
        assert_eq!(frame, expected);

        build_length_prefixed_frame(&MessageBuf::default(), 11, &mut frame);
        assert!(frame.is_empty(), "no lines, no packets");
    }

    /// One connection, LE-length-prefixed packets, the lines packed as in a datagram.
    #[tokio::test]
    async fn unix_stream_writes_length_prefixed_packets_on_one_connection() {
        use tokio::io::AsyncReadExt;
        let dir = SocketDir::new("stream");
        let path = dir.socket();
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let reader = tokio::spawn(async move {
            let (mut conn, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            conn.read_to_end(&mut buf).await.unwrap();
            buf
        });
        let mut output = StatsdOutput::unix_stream(&path, Duration::from_secs(1));
        output.send(&two_counters()).await.expect("first batch");
        output
            .send(&batch_with(vec![metric_event("c", MetricKind::counter(3.0), &[])]))
            .await
            .expect("second batch, same connection");
        drop(output);
        let got = tokio::time::timeout(Duration::from_secs(2), reader).await.unwrap().unwrap();
        let mut expected = 11u32.to_le_bytes().to_vec();
        expected.extend_from_slice(b"a:1|c\nb:2|c");
        expected.extend_from_slice(&5u32.to_le_bytes());
        expected.extend_from_slice(b"c:3|c");
        assert_eq!(got, expected);
    }

    /// The encoder caps a line at `max_packet_bytes` on a Unix stream, since a packet can't be
    /// longer; TCP stays uncapped.
    #[test]
    fn the_encoder_line_cap_applies_on_both_unix_transports() {
        let stream = StatsdOutput::unix_stream("/tmp/x.socket", Duration::from_secs(1))
            .with_max_packet_bytes(64);
        assert_eq!(stream.encoder_cap(), 64);
        let tcp =
            StatsdOutput::tcp("127.0.0.1:1", Duration::from_secs(1)).with_max_packet_bytes(64);
        assert_eq!(tcp.encoder_cap(), usize::MAX);
    }

    #[tokio::test]
    async fn with_tls_on_a_unix_statsd_out_is_an_error() {
        let settings = TlsClientSettings::default();
        let err = StatsdOutput::unix_stream("/tmp/x.socket", Duration::from_secs(1))
            .with_tls(&settings, Path::new("."))
            .err()
            .expect("a Unix socket is always plaintext");
        assert!(err.to_string().contains("plaintext"), "{err}");
        let err = StatsdOutput::unix_datagram("/tmp/x.socket", Duration::from_secs(1))
            .with_tls(&settings, Path::new("."))
            .err()
            .expect("a Unix socket is always plaintext");
        assert!(err.to_string().contains("plaintext"), "{err}");
    }

    #[tokio::test]
    async fn a_line_that_would_overflow_the_cap_starts_a_new_datagram() {
        let (addr, collector) = udp_collector().await;
        let mut output = StatsdOutput::udp(addr.to_string()).unwrap();
        output = output.with_max_packet_bytes(10); // "a:1|c" is 5 bytes; two won't fit with a sep
        let batch = batch_with(vec![
            metric_event("a", MetricKind::counter(1.0), &[]),
            metric_event("b", MetricKind::counter(2.0), &[]),
        ]);
        let recv_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            let mut got = Vec::new();
            for _ in 0..2 {
                let (n, _) = collector.recv_from(&mut buf).await.unwrap();
                got.push(String::from_utf8_lossy(&buf[..n]).into_owned());
            }
            got
        });
        output.send(&batch).await.expect("send should succeed");
        let received =
            tokio::time::timeout(Duration::from_secs(2), recv_task).await.unwrap().unwrap();
        assert_eq!(received, vec!["a:1|c", "b:2|c"]);
    }

    #[test]
    fn a_single_line_longer_than_max_packet_bytes_is_dropped_whole() {
        // "hits:1|c" is longer than this cap.
        let mut encoder = StatsdEncoder::new(Format::DogStatsd).with_max_packet_bytes(3);
        let mut out = MessageBuf::default();
        let stats = encoder.encode_into(
            &batch_with(vec![metric_event("hits", MetricKind::counter(1.0), &[])]),
            &mut out,
        );
        assert!(out.is_empty());
        assert_eq!(stats.dropped_oversize_line, 1);
    }

    /// UDP caps the encoder at `max_packet_bytes` and TCP leaves it uncapped, in either builder
    /// order (`logit-cli` calls `with_encoder` first).
    #[tokio::test]
    async fn the_encoder_line_cap_follows_the_transport_at_build_time() {
        let oversize = || batch_with(vec![metric_event("hits", MetricKind::counter(1.0), &[])]);

        let udp_encoder_first = StatsdOutput::udp("127.0.0.1:1")
            .unwrap()
            .with_encoder(StatsdEncoder::new(Format::DogStatsd))
            .with_max_packet_bytes(3);
        let udp_cap_first = StatsdOutput::udp("127.0.0.1:1")
            .unwrap()
            .with_max_packet_bytes(3)
            .with_encoder(StatsdEncoder::new(Format::DogStatsd));
        for mut output in [udp_encoder_first, udp_cap_first] {
            assert_eq!(output.encoder.max_packet_bytes, 3);
            // "hits:1|c" (8 bytes) is dropped whole, so nothing is sent.
            output.send(&oversize()).await.expect("an all-dropped batch performs no I/O");
            assert!(output.lines.is_empty());
        }
        let udp_default = StatsdOutput::udp("127.0.0.1:1").unwrap();
        assert_eq!(udp_default.encoder.max_packet_bytes, DEFAULT_MAX_PACKET_BYTES);

        // TCP: the same value never caps the encoder, so the line reaches the wire.
        let (addr, received, _) = tcp_collector().await;
        let mut tcp = StatsdOutput::tcp(addr.to_string(), Duration::from_secs(2))
            .with_encoder(StatsdEncoder::new(Format::DogStatsd))
            .with_max_packet_bytes(3);
        assert_eq!(tcp.encoder.max_packet_bytes, usize::MAX);
        assert_eq!(tcp.max_packet_bytes, 3);
        tcp.send(&oversize()).await.expect("send should succeed");
        assert_eq!(tcp.lines.len(), 1);
        drop(tcp);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let got = received.lock().unwrap();
        assert_eq!(String::from_utf8_lossy(&got[0]), "hits:1|c\n");
    }

    #[test]
    fn a_negative_absolute_gauges_two_lines_are_never_split_across_datagrams() {
        // One `MessageBuf` entry is one packer unit, so the pair can't be split.
        let mut encoder = StatsdEncoder::new(Format::DogStatsd);
        let mut out = MessageBuf::default();
        let stats = encoder.encode_into(
            &batch_with(vec![metric_event("free", MetricKind::Gauge(-5.0), &[])]),
            &mut out,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(stats.dropped_oversize_line, 0);
    }

    #[tokio::test]
    async fn a_udp_datagram_carries_no_trailing_newline() {
        let (addr, collector) = udp_collector().await;
        let mut output = StatsdOutput::udp(addr.to_string()).unwrap();
        let batch = batch_with(vec![metric_event("a", MetricKind::counter(1.0), &[])]);
        let recv_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            let (n, _) = collector.recv_from(&mut buf).await.unwrap();
            buf[..n].to_vec()
        });
        output.send(&batch).await.expect("send should succeed");
        let received =
            tokio::time::timeout(Duration::from_secs(2), recv_task).await.unwrap().unwrap();
        assert!(!received.ends_with(b"\n"));
    }

    #[tokio::test]
    async fn tcp_terminates_every_line_with_a_newline_including_the_last_one() {
        let (addr, received, _accepts) = tcp_collector().await;
        let mut output = StatsdOutput::tcp(addr.to_string(), Duration::from_secs(2));
        let batch = batch_with(vec![
            metric_event("a", MetricKind::counter(1.0), &[]),
            metric_event("b", MetricKind::counter(2.0), &[]),
        ]);
        output.send(&batch).await.expect("send should succeed");
        drop(output);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let got = received.lock().unwrap();
        assert_eq!(String::from_utf8_lossy(&got[0]), "a:1|c\nb:2|c\n");
    }

    // -- Socket ---------------------------------------------------------------------------------

    async fn udp_collector() -> (SocketAddr, Arc<UdpSocket>) {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        (addr, Arc::new(socket))
    }

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
    async fn a_batch_with_nothing_encodable_performs_no_io_at_all() {
        let mut output = StatsdOutput::udp("127.0.0.1:1").unwrap();
        let batch = batch_with(vec![log_event(0)]);
        output.send(&batch).await.expect("an all-skipped batch must not attempt any I/O");
    }

    #[tokio::test]
    async fn tcp_sends_one_newline_delimited_frame_per_batch() {
        let (addr, received, accepts) = tcp_collector().await;
        let mut output = StatsdOutput::tcp(addr.to_string(), Duration::from_secs(2));
        let batch = batch_with(vec![metric_event("a", MetricKind::counter(1.0), &[])]);
        output.send(&batch).await.expect("send should succeed");
        drop(output);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(accepts.load(Ordering::SeqCst), 1);
        let got = received.lock().unwrap();
        assert_eq!(String::from_utf8_lossy(&got[0]), "a:1|c\n");
    }

    #[tokio::test]
    async fn tcp_reconnects_exactly_once_after_the_peer_resets_an_inherited_connection() {
        let (addr, received, accepts) = tcp_collector().await;
        let mut output = StatsdOutput::tcp(addr.to_string(), Duration::from_secs(2));

        let batch = batch_with(vec![metric_event("first", MetricKind::counter(1.0), &[])]);
        output.send(&batch).await.expect("first send should succeed against a fresh connection");

        if let Conn::Tcp { stream: Some(stream), .. } = &mut output.conn {
            stream.shutdown().await.expect("local shutdown should succeed");
        }

        let batch2 = batch_with(vec![metric_event("second", MetricKind::counter(1.0), &[])]);
        output
            .send(&batch2)
            .await
            .expect("second send should reconnect once and succeed, not surface the failure");

        drop(output);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(accepts.load(Ordering::SeqCst), 2);
        let got = received.lock().unwrap();
        assert!(got.iter().any(|b| String::from_utf8_lossy(b).contains("first")));
        assert!(got.iter().any(|b| String::from_utf8_lossy(b).contains("second")));
    }

    /// [`tcp_collector`], except each connection closes after its first read: a receiver's idle
    /// timeout or restart, seen from a sink holding a pooled connection. A clean FIN, not an RST,
    /// which a plaintext sender can't detect from a write.
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
                }
            });
        }
        (addr, received, accepts)
    }

    /// The pooled-connection probe: a write into a FIN'd socket succeeds locally, so without it the
    /// second batch would be reported delivered and lost.
    #[tokio::test]
    async fn a_pooled_connection_the_peer_closed_is_reconnected_before_writing_and_the_message_is_not_lost(
    ) {
        let (addr, received, accepts) = tcp_collector_that_closes_after_one_read().await;
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("statsd_out", "statsd_out", "sink");
        let mut output =
            StatsdOutput::tcp(addr.to_string(), Duration::from_secs(2)).with_telemetry(telemetry);

        output
            .send(&batch_with(vec![metric_event("first", MetricKind::counter(1.0), &[])]))
            .await
            .expect("first send should succeed against a fresh connection");

        // Let the collector's FIN arrive before the probe looks for it.
        tokio::time::sleep(Duration::from_millis(100)).await;

        output
            .send(&batch_with(vec![metric_event("second", MetricKind::counter(1.0), &[])]))
            .await
            .expect("the probe should reconnect rather than write into a closed socket");

        drop(output);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            accepts.load(Ordering::SeqCst),
            2,
            "the probe must have dialled a second connection for the second batch"
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
            "the second batch must actually have reached the receiver: {got:?}"
        );
    }

    // -- Faults ---------------------------------------------------------------------------------

    #[tokio::test]
    async fn tcp_connect_refused_is_classified_as_a_clean_fault() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let mut output = StatsdOutput::tcp(addr.to_string(), Duration::from_millis(500));
        let batch = batch_with(vec![metric_event("a", MetricKind::counter(1.0), &[])]);
        let err = output.send(&batch).await.expect_err("connect should fail");
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
    }

    #[tokio::test]
    async fn duplicate_safe_is_false() {
        let output = StatsdOutput::udp("127.0.0.1:0").unwrap();
        assert!(!output.duplicate_safe());
    }

    // -- Sink: TCP over TLS (module doc's "TLS" section) ----------------------------------------
    //
    // `syslog.rs`'s TLS suite's twin: the same fixtures, collector shape, and scripted-stream
    // tests.

    fn testdata_dir() -> std::path::PathBuf {
        // The repo root's `testdata/tls` (`testdata/tls/README.md`).
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    fn tls_settings(overrides: impl FnOnce(&mut TlsClientSettings)) -> TlsClientSettings {
        let mut settings = TlsClientSettings::default();
        overrides(&mut settings);
        settings
    }

    /// A `rustls::ServerConfig` presenting `testdata/tls/server.{pem,key}` (SANs `localhost` and
    /// `127.0.0.1`), optionally requiring a client certificate chaining to `testdata/tls/ca.pem`.
    /// No ALPN: statsd over TLS has no identifier.
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

    /// [`tcp_collector`]'s TLS twin: reads each handshaken connection to EOF. The third return
    /// counts completed handshakes, not accepts, so a rejected client never counts. One task per
    /// connection, so a failed handshake can't stall the accept loop.
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

    /// TLS delivers the same LF-terminated frame plaintext does.
    #[tokio::test]
    async fn tls_tcp_round_trips_a_batch_to_a_trusting_collector() {
        let (addr, received, handshakes) = tls_tcp_collector(false).await;
        // `localhost`, not `127.0.0.1`, so `host_only` yields a non-IP SNI name.
        let mut output =
            StatsdOutput::tcp(format!("localhost:{}", addr.port()), Duration::from_secs(2))
                .with_tls(
                    &tls_settings(|t| t.ca_file = Some("ca.pem".to_string())),
                    &testdata_dir(),
                )
                .expect("a tls: block on the TCP transport is legal");
        let batch = batch_with(vec![
            metric_event("a", MetricKind::counter(1.0), &[]),
            metric_event("b", MetricKind::counter(2.0), &[]),
        ]);
        output.send(&batch).await.expect("send over TLS should succeed");
        drop(output);
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(handshakes.load(Ordering::SeqCst), 1);
        let got = received.lock().unwrap();
        assert_eq!(
            String::from_utf8_lossy(&got[0]),
            "a:1|c\nb:2|c\n",
            "TLS must deliver byte-for-byte the same LF-terminated frame plaintext does"
        );
    }

    /// `other-ca.pem` never signed `server.pem`, so the handshake fails before any batch byte
    /// leaves the host: `Fault::Clean`.
    #[tokio::test]
    async fn tls_tcp_against_an_untrusted_ca_fails_clean() {
        let (addr, received, handshakes) = tls_tcp_collector(false).await;
        let mut output =
            StatsdOutput::tcp(format!("localhost:{}", addr.port()), Duration::from_secs(2))
                .with_tls(
                    &tls_settings(|t| t.ca_file = Some("other-ca.pem".to_string())),
                    &testdata_dir(),
                )
                .expect("a tls: block on the TCP transport is legal");
        let batch = batch_with(vec![metric_event("untrusted", MetricKind::counter(1.0), &[])]);
        let err = output.send(&batch).await.expect_err("an untrusted CA must fail the handshake");
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(handshakes.load(Ordering::SeqCst), 0);
        assert!(received.lock().unwrap().is_empty());
    }

    /// A `tracing` writer that captures rendered events, so the `insecure_skip_verify` warning
    /// (`Diagnostics::warn`, `tracing` only, no telemetry) can be asserted.
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
        // The bundled Mozilla roots never signed `server.pem`; only skipping verification works.
        let mut output =
            StatsdOutput::tcp(format!("localhost:{}", addr.port()), Duration::from_secs(2))
                .with_diagnostics(Diagnostics::new("statsd_out"))
                .with_tls(&tls_settings(|t| t.insecure_skip_verify = true), &testdata_dir())
                .expect("insecure_skip_verify is legal, if loud");
        let batch = batch_with(vec![metric_event("insecure", MetricKind::counter(1.0), &[])]);
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

    /// Mutual TLS: a completed handshake proves `client.pem` was presented and accepted.
    #[tokio::test]
    async fn tls_tcp_mutual_tls_presents_the_client_certificate() {
        let (addr, received, handshakes) = tls_tcp_collector(true).await;
        let mut output =
            StatsdOutput::tcp(format!("localhost:{}", addr.port()), Duration::from_secs(2))
                .with_tls(
                    &tls_settings(|t| {
                        t.ca_file = Some("ca.pem".to_string());
                        t.cert_file = Some("client.pem".to_string());
                        t.key_file = Some("client.key".to_string());
                    }),
                    &testdata_dir(),
                )
                .expect("a client certificate is legal on the TCP transport");
        let batch = batch_with(vec![metric_event("mutual", MetricKind::counter(1.0), &[])]);
        output.send(&batch).await.expect("mutual TLS should succeed");
        drop(output);
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(handshakes.load(Ordering::SeqCst), 1);
        let got = received.lock().unwrap();
        assert!(String::from_utf8_lossy(&got[0]).contains("mutual"));
    }

    /// `tls:` on UDP is an error here too, not only under `graph::resolve`'s rule 52.
    #[tokio::test]
    async fn with_tls_on_a_udp_statsd_out_is_an_error() {
        let output = StatsdOutput::udp("127.0.0.1:8125").unwrap();
        // `.err()` rather than `expect_err`, which would need `StatsdOutput: Debug`.
        let err = output
            .with_tls(&TlsClientSettings::default(), &testdata_dir())
            .err()
            .expect("DTLS is out of scope");
        assert!(err.to_string().contains("transport: tcp"), "got: {err}");
    }

    /// `logit.output.reconnects` counts every connect after the first.
    #[tokio::test]
    async fn reconnects_are_counted_from_the_second_connect() {
        let (addr, _received, accepts) = tcp_collector().await;
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("statsd_out", "statsd_out", "sink");
        let mut output =
            StatsdOutput::tcp(addr.to_string(), Duration::from_secs(2)).with_telemetry(telemetry);

        let batch = batch_with(vec![metric_event("first", MetricKind::counter(1.0), &[])]);
        output.send(&batch).await.expect("first send should succeed against a fresh connection");
        assert_eq!(
            reconnects_in(registry.drain(0)),
            None,
            "the first connect must not be counted as a reconnect"
        );

        // Break the local end deterministically rather than racing a peer RST.
        if let Conn::Tcp { stream: Some(stream), .. } = &mut output.conn {
            stream.shutdown().await.expect("local shutdown should succeed");
        }
        let batch2 = batch_with(vec![metric_event("second", MetricKind::counter(1.0), &[])]);
        output.send(&batch2).await.expect("second send should reconnect once and succeed");

        drop(output);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(accepts.load(Ordering::SeqCst), 2);
        assert_eq!(
            reconnects_in(registry.drain(0)),
            Some(1.0),
            "exactly one reconnect, counted (`logit.output.reconnects`)"
        );
    }

    /// `logit.output.reconnects` from a drained `Registry`, or `None` if never counted.
    fn reconnects_in(events: Vec<Event>) -> Option<f64> {
        events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(sum)
                    if logit_core::interner::resolve(m.name) == "logit.output.reconnects" =>
                {
                    Some(sum.value)
                }
                _ => None,
            })
        })
    }

    // -- Sink: TCP over TLS, write/flush semantics ----------------------------------------------
    //
    // The TLS state that matters ("the session accepted the frame, the socket took part of it")
    // needs a backpressured socket of known send-buffer size to provoke for real, so these tests
    // drive `send_tcp` against a scripted [`FakeTlsStream`] instead.

    /// An [`AsyncStream`] with `tokio_rustls`' write semantics: `write` buffers in userspace and
    /// reports success, and only `flush` puts bytes on the notional wire. Failures are scripted
    /// per call to reach each of `send_tcp`'s arms.
    #[derive(Clone, Default)]
    struct FakeTlsStream(Arc<Mutex<FakeState>>);

    #[derive(Default)]
    struct FakeState {
        /// Accepted by `write`, not yet flushed (`tokio_rustls`' `sendable_tls`).
        buffered: Vec<u8>,
        /// What `flush` has put on the wire.
        sent: Vec<u8>,
        writes: usize,
        flushes: usize,
        /// `write` fails on this 1-based call number.
        fail_write_on: Option<usize>,
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
            state.buffered.extend_from_slice(buf);
            std::task::Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let mut state = self.state();
            state.flushes += 1;
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
        /// `Pending`, as a live, quiet stream is: `Ok(())` with nothing filled is EOF, and
        /// `crate::tls::poll_pending_close` would replace the stream before any scripted write.
        /// No waker is registered, so anything that awaited a read here would hang loudly.
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Pending
        }
    }

    /// A client config so a [`TcpDial`] reports `is_tls()`; no handshake happens in these tests.
    fn any_client_config() -> Arc<rustls::ClientConfig> {
        Arc::new(
            crate::tls::build_client_config(&TlsClientSettings::default(), &testdata_dir())
                .expect("the default settings always build"),
        )
    }

    fn one_line_frame() -> (MessageBuf, Vec<u8>) {
        let mut lines = MessageBuf::default();
        lines.push("hits:1|c");
        (lines, Vec::new())
    }

    /// `send_tcp`'s `!dial.is_tls()` guard: a TLS write failure is `Ambiguous` and never resent,
    /// since a resend would increment the destination counter twice.
    #[tokio::test]
    async fn a_tls_write_failure_is_ambiguous_and_never_retried() {
        let fake = FakeTlsStream::failing_write(1);
        let mut stream: Option<Box<dyn AsyncStream>> = Some(Box::new(fake.clone()));
        let cfg = any_client_config();
        let telemetry = Telemetry::default();
        let mut connected = true;
        let mut dial = TcpDial {
            // Nothing listens here: a wrongly taken retry would fail its connect as `Clean`.
            endpoint: "127.0.0.1:1",
            connect_timeout: Duration::from_millis(200),
            tls: Some(&cfg),
            kind: StreamKind::Tcp,
            telemetry: &telemetry,
            has_connected_once: &mut connected,
        };
        let (lines, mut frame_buf) = one_line_frame();

        let err = StatsdOutput::send_tcp(&mut stream, &mut dial, &lines, &mut frame_buf)
            .await
            .expect_err("a failed write must fail the send");

        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
        assert!(stream.is_none(), "a stream whose write failed must not be reused");
        let state = fake.state();
        assert_eq!(state.writes, 1, "exactly one write attempt -- no resend");
        assert_eq!(state.flushes, 0, "a failed write never reaches the flush");
        assert!(state.sent.is_empty());
    }

    /// The plaintext counterpart on the same scripted stream: a zero-byte write failure reconnects
    /// once, and a failed retry is `Fault::Clean`.
    #[tokio::test]
    async fn a_plaintext_zero_byte_write_still_reconnects_once() {
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
            kind: StreamKind::Tcp,
            telemetry: &telemetry,
            has_connected_once: &mut connected,
        };
        let (lines, mut frame_buf) = one_line_frame();

        let err = StatsdOutput::send_tcp(&mut stream, &mut dial, &lines, &mut frame_buf)
            .await
            .expect_err("the retry's connect is refused");

        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
        assert_eq!(fake.state().writes, 1);
        assert!(
            format!("{err:#}").contains("connecting to statsd_out endpoint"),
            "the failure must come from the retry's fresh connect, proving one happened: {err:#}"
        );
    }

    /// Nothing is left in the session buffer once a TLS batch is reported delivered, and the
    /// flushed connection is kept.
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
            kind: StreamKind::Tcp,
            telemetry: &telemetry,
            has_connected_once: &mut connected,
        };
        let (lines, mut frame_buf) = one_line_frame();

        let (sent, datagrams) =
            StatsdOutput::send_tcp(&mut stream, &mut dial, &lines, &mut frame_buf)
                .await
                .expect("the write and the flush both succeed");

        assert_eq!((sent, datagrams), (1, 0));
        let state = fake.state();
        assert_eq!(state.flushes, 1, "the success path must flush exactly once");
        assert!(state.buffered.is_empty(), "nothing may be left in the session buffer");
        assert_eq!(state.sent, b"hits:1|c\n".to_vec(), "the whole frame must be on the wire");
        drop(state);
        assert!(stream.is_some(), "a flushed connection is reusable");
    }

    // -- Telemetry --------------------------------------------------------------------------

    #[tokio::test]
    async fn an_unresolved_gauge_delta_reports_under_its_own_diagnostic_key() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("out", "statsd_out", "sink");
        let diag = Diagnostics::new("out").with_telemetry(telemetry);
        let mut output = StatsdOutput::udp("127.0.0.1:1").unwrap().with_diagnostics(diag);
        let batch = batch_with(vec![metric_event("conns", MetricKind::GaugeDelta(5.0), &[])]);
        output.send(&batch).await.expect("nothing encodable means no I/O");

        let found = registry.drain(0).into_iter().any(|e| {
            e.attributes.get("key").and_then(|v| v.as_str()) == Some("gauge_delta_unresolved")
        });
        assert!(found, "expected a gauge_delta_unresolved diagnostic");
    }

    /// Both transports report the same `logit.output.messages`: a negative-gauge pair counts once.
    #[tokio::test]
    async fn udp_and_tcp_report_the_same_message_count_for_the_same_batch() {
        fn messages(registry: &logit_core::Registry) -> f64 {
            registry
                .drain(0)
                .into_iter()
                .flat_map(|e| e.metrics)
                .filter(|m| logit_core::interner::resolve(m.name) == "logit.output.messages")
                .map(|m| match m.kind {
                    MetricKind::Sum(s) => s.value,
                    _ => panic!("logit.output.messages must be a counter"),
                })
                .sum()
        }
        let batch = || {
            batch_with(vec![
                metric_event("free", MetricKind::Gauge(-5.0), &[]),
                metric_event("hits", MetricKind::counter(1.0), &[]),
            ])
        };

        let (udp_addr, _collector) = udp_collector().await;
        let udp_registry = logit_core::Registry::new();
        let mut udp_out = StatsdOutput::udp(udp_addr.to_string())
            .unwrap()
            .with_telemetry(udp_registry.telemetry_for("out", "statsd_out", "sink"));
        udp_out.send(&batch()).await.expect("udp send should succeed");

        let (tcp_addr, _received, _accepts) = tcp_collector().await;
        let tcp_registry = logit_core::Registry::new();
        let mut tcp_out = StatsdOutput::tcp(tcp_addr.to_string(), Duration::from_secs(2))
            .with_telemetry(tcp_registry.telemetry_for("out", "statsd_out", "sink"));
        tcp_out.send(&batch()).await.expect("tcp send should succeed");

        assert_eq!(
            messages(&udp_registry),
            2.0,
            "one message per MessageBuf entry: the pair counts once"
        );
        assert_eq!(messages(&tcp_registry), 2.0);
    }

    // -- Round-trip through the real StatsdDecoder -----------------------------------------

    fn decode_one(line: &str) -> Vec<Event> {
        let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()));
        decoder.decode(bytes::Bytes::from(line.to_string())).expect("decode should succeed").events
    }

    #[test]
    fn a_counter_with_tags_round_trips_through_the_real_statsd_decoder() {
        let (msgs, _) =
            encode(vec![metric_event("hits", MetricKind::counter(3.0), &[("env", "prod".into())])]);
        let events = decode_one(&msgs[0]);
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0].metrics[0].kind, MetricKind::Sum(s) if s.value == 3.0));
        assert_eq!(events[0].attributes.get("env").and_then(|v| v.as_str()), Some("prod"));
    }

    #[test]
    fn a_bare_tag_round_trips_as_value_bool_true_through_the_real_statsd_decoder() {
        let (msgs, _) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[("urgent", true.into())],
        )]);
        let events = decode_one(&msgs[0]);
        assert!(matches!(events[0].attributes.get("urgent"), Some(Value::Bool(true))));
    }

    /// `#team:a,team:b` relays byte for byte, through the decoder's repeated-key fold.
    #[test]
    fn a_repeated_tag_key_relays_byte_for_byte_through_the_real_statsd_decoder() {
        let line = "x:1|c|#team:a,team:b";
        let events = decode_one(line);
        assert!(
            matches!(events[0].attributes.get("team"), Some(Value::Array(a)) if a.len() == 2),
            "the decoder must fold a repeated tag key into a two-element Array; got {:?}",
            events[0].attributes.get("team")
        );
        let (msgs, stats) = encode(events);
        assert_eq!(msgs, vec![line.to_string()]);
        assert_eq!(stats, EncodeStats::default());
    }

    #[test]
    fn a_bare_and_valued_tag_mix_relays_byte_for_byte_through_the_real_statsd_decoder() {
        let line = "x:1|c|#urgent,urgent:1";
        let events = decode_one(line);
        let (msgs, stats) = encode(events);
        assert_eq!(msgs, vec![line.to_string()]);
        assert_eq!(stats, EncodeStats::default());
    }

    #[test]
    fn a_packed_multi_line_datagram_round_trips_as_several_events() {
        let mut encoder = StatsdEncoder::new(Format::DogStatsd);
        let mut out = MessageBuf::default();
        encoder.encode_into(
            &batch_with(vec![
                metric_event("a", MetricKind::counter(1.0), &[]),
                metric_event("b", MetricKind::counter(2.0), &[]),
            ]),
            &mut out,
        );
        let packed: Vec<&str> = out.iter().map(|b| std::str::from_utf8(b).unwrap()).collect();
        let datagram = packed.join("\n");
        let events = decode_one(&datagram);
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn a_gauge_delta_round_trips_as_a_gauge_delta_under_relative_gauges() {
        let mut encoder = StatsdEncoder::new(Format::DogStatsd).with_relative_gauges(true);
        let (msgs, _) = encode_with(
            &mut encoder,
            vec![metric_event("conns", MetricKind::GaugeDelta(-5.0), &[])],
        );
        let events = decode_one(&msgs[0]);
        assert!(matches!(events[0].metrics[0].kind, MetricKind::GaugeDelta(v) if v == -5.0));
    }

    #[test]
    fn a_negative_absolute_gauge_round_trips_to_the_same_effective_value_not_a_delta() {
        let (msgs, _) = encode(vec![metric_event("free", MetricKind::Gauge(-5.0), &[])]);
        // Reset to 0, then -5: the encoded value.
        let events = decode_one(&msgs[0]);
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0].metrics[0].kind, MetricKind::Gauge(v) if v == 0.0));
        assert!(matches!(events[1].metrics[0].kind, MetricKind::GaugeDelta(v) if v == -5.0));
    }

    #[test]
    fn a_negative_zero_gauge_does_not_decode_as_a_gauge_delta() {
        let (msgs, _) = encode(vec![metric_event("free", MetricKind::Gauge(-0.0), &[])]);
        assert!(!msgs[0].contains('-'), "no minus may reach the wire: {}", msgs[0]);
        let events = decode_one(&msgs[0]);
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0].metrics[0].kind, MetricKind::Gauge(v) if v == 0.0));
    }

    #[test]
    fn a_tag_value_containing_a_colon_round_trips_with_its_colon_intact() {
        let (msgs, _) = encode(vec![metric_event(
            "hits",
            MetricKind::counter(1.0),
            &[("range", "a:b".into())],
        )]);
        let events = decode_one(&msgs[0]);
        assert_eq!(events[0].attributes.get("range").and_then(|v| v.as_str()), Some("a:b"));
    }

    #[test]
    fn a_full_statsd_in_to_statsd_out_relay_preserves_every_name_value_and_tag() {
        let original = decode_one("api.hits:3|c|#env:prod,host:web1");
        let name = logit_core::interner::resolve(original[0].metrics[0].name).to_string();
        let (msgs, _) = encode(original.clone());
        let relayed = decode_one(&msgs[0]);
        assert_eq!(logit_core::interner::resolve(relayed[0].metrics[0].name), name);
        assert!(matches!(relayed[0].metrics[0].kind, MetricKind::Sum(s) if s.value == 3.0));
        assert_eq!(relayed[0].attributes.get("env").and_then(|v| v.as_str()), Some("prod"));
        assert_eq!(relayed[0].attributes.get("host").and_then(|v| v.as_str()), Some("web1"));
    }

    /// Multi-value, `@rate`, and tags all survive a timer line's relay.
    #[test]
    fn a_timer_line_round_trips_byte_for_byte_through_the_real_statsd_decoder() {
        let original_line = "req.duration:12.5:34:56|ms|@0.5|#env:prod,host:web1";
        let original = decode_one(original_line);
        assert_eq!(original.len(), 1, "one Samples event per line");
        let (msgs, _) = encode(original);
        assert_eq!(msgs.len(), 1);
        let relayed = decode_one(&msgs[0]);
        assert_eq!(relayed.len(), 1);
        assert!(matches!(
            &relayed[0].metrics[0].kind,
            MetricKind::Samples(s)
                if s.values.as_slice() == [12.5, 34.0, 56.0] && s.sample_rate == 0.5
        ));
        assert_eq!(relayed[0].attributes.get("env").and_then(|v| v.as_str()), Some("prod"));
        assert_eq!(relayed[0].attributes.get("host").and_then(|v| v.as_str()), Some("web1"));
        assert_eq!(relayed[0].attributes.get("statsd.type").and_then(|v| v.as_str()), Some("ms"));
    }

    /// A multi-member set line relays as one line per member, members in order.
    #[test]
    fn a_set_line_round_trips_byte_for_byte_through_the_real_statsd_decoder() {
        let original_line = "unique.visitors:alice:bob|s";
        let original = decode_one(original_line);
        assert_eq!(original.len(), 1, "one SetMembers event per line");
        let (msgs, _) = encode(original);
        assert_eq!(msgs.len(), 2);
        let relayed: Vec<Event> = msgs.iter().flat_map(|m| decode_one(m)).collect();
        assert_eq!(relayed.len(), 2);
        let members: Vec<&[u8]> = relayed
            .iter()
            .map(|e| match &e.metrics[0].kind {
                MetricKind::SetMembers(m) => m[0].as_ref(),
                other => panic!("expected SetMembers, got {other:?}"),
            })
            .collect();
        assert_eq!(members, vec![b"alice".as_slice(), b"bob".as_slice()]);
    }

    /// `|c:` and `|T` survive a relay, and `|T` lands on `Event::timestamp`.
    #[test]
    fn a_container_id_and_timestamp_line_round_trips_through_the_real_statsd_decoder() {
        let original = decode_one("hits:1|c|c:abcd1234|T1700000000");
        assert_eq!(original.len(), 1);
        let (msgs, _) = encode(original);
        assert_eq!(msgs, vec!["hits:1|c|c:abcd1234|T1700000000"]);
        let relayed = decode_one(&msgs[0]);
        assert_eq!(relayed[0].timestamp, 1_700_000_000_000_000_000);
        assert_eq!(
            relayed[0].attributes.get("statsd.container_id").and_then(|v| v.as_str()),
            Some("abcd1234")
        );
        assert_eq!(relayed[0].attributes.get("statsd.timestamp"), Some(&Value::U64(1_700_000_000)));
    }

    #[test]
    fn an_event_line_round_trips_through_the_real_statsd_decoder() {
        let original_line =
            "_e{5,18}:title|line one\\nline two|d:1700000000|h:web1|p:low|t:warning|k:key1|\
             s:my_app|#env:prod|c:cid1";
        let original = decode_one(original_line);
        assert_eq!(original.len(), 1, "one Event event per line");
        let (msgs, _) = encode(original);
        assert_eq!(msgs, vec![original_line]);
        let relayed = decode_one(&msgs[0]);
        assert_eq!(
            relayed[0].log.as_ref().map(|l| l.message.clone()),
            Some(Value::str("line one\nline two"))
        );
        assert_eq!(
            relayed[0].attributes.get("statsd.event.title").and_then(|v| v.as_str()),
            Some("title")
        );
        assert_eq!(relayed[0].attributes.get("statsd.timestamp"), Some(&Value::U64(1_700_000_000)));
        assert_eq!(relayed[0].attributes.get("env").and_then(|v| v.as_str()), Some("prod"));
    }

    /// Every optional field, and a `|` inside the last field (`m:a|b`), survives the relay.
    #[test]
    fn a_service_check_line_round_trips_through_the_real_statsd_decoder() {
        let original_line = "_sc|my.check|2|d:1700000000|h:web1|#env:prod|c:cid1|m:a|b";
        let original = decode_one(original_line);
        assert_eq!(original.len(), 1, "one service-check event per line");
        let (msgs, _) = encode(original);
        assert_eq!(msgs, vec![original_line]);
        let relayed = decode_one(&msgs[0]);
        assert!(matches!(relayed[0].metrics[0].kind, MetricKind::Gauge(v) if v == 2.0));
        assert_eq!(
            relayed[0].attributes.get("statsd.service_check.name").and_then(|v| v.as_str()),
            Some("my.check")
        );
        assert_eq!(
            relayed[0].attributes.get("statsd.service_check.message").and_then(|v| v.as_str()),
            Some("a|b")
        );
        assert_eq!(relayed[0].attributes.get("env").and_then(|v| v.as_str()), Some("prod"));
    }

    // -- Fixed-point property: decode/encode agree with each other -----------------------------
    //
    // `docs/adr/lossless-transit.md`'s fixed point, over a small DogStatsD grammar generator:
    // `decode(encode(decode(line))) == decode(line)` (whole `EventBatch`, receipt timestamps
    // normalized when the line carries no `|T`), and `encode(decode(line))` is a fixed point of
    // `encode . decode`. Same shape as `syslog.rs`'s `mod fixed_point`.
    mod fixed_point {
        use super::*;
        use logit_core::EventBatch;
        use proptest::prelude::*;

        fn metric_name() -> impl Strategy<Value = String> {
            "[a-zA-Z][a-zA-Z0-9_.]{0,12}"
        }

        /// 1-3 values shaped for `kind`: decimals for `c`/`ms`/`h`/`d`; for `g`, an optional
        /// leading `-` for the delta path; for `s`, short members with no `:`/`|` (sanitized
        /// members have their own unit tests) but with `@`/`#`/`,`/space, which a member keeps.
        fn values_for(kind: &'static str) -> impl Strategy<Value = Vec<String>> {
            let count = 1usize..=3;
            match kind {
                "s" => count
                    .prop_flat_map(|n| prop::collection::vec("[a-z][a-z0-9 @#,]{0,5}", n))
                    .boxed(),
                "g" => count
                    .prop_flat_map(|n| {
                        prop::collection::vec(
                            (any::<bool>(), 0u32..1000).prop_map(|(neg, v)| {
                                if neg {
                                    format!("-{v}")
                                } else {
                                    v.to_string()
                                }
                            }),
                            n,
                        )
                    })
                    .boxed(),
                _ => count
                    .prop_flat_map(|n| {
                        prop::collection::vec((0u32..1000).prop_map(|v| v.to_string()), n)
                    })
                    .boxed(),
            }
        }

        /// A wire-type letter paired with values shaped for it.
        fn kind_and_values() -> impl Strategy<Value = (&'static str, Vec<String>)> {
            prop_oneof![Just("c"), Just("g"), Just("ms"), Just("h"), Just("d"), Just("s")]
                .prop_flat_map(|kind| values_for(kind).prop_map(move |values| (kind, values)))
        }

        fn opt_rate() -> impl Strategy<Value = Option<u32>> {
            // 1..=100 -> @0.01..=@1.00, always finite and in (0, 1].
            prop_oneof![Just(None), (1u32..=100).prop_map(Some)]
        }

        /// One `key[:value]` tag token. A two-key pool makes repeated keys (multi-value tags) and
        /// exact duplicates (the decoder's dedupe) common; an optional value generates the bare
        /// form and bare/valued mixes.
        fn tag() -> impl Strategy<Value = (String, Option<String>)> {
            (
                prop_oneof![Just("team".to_string()), Just("env".to_string())],
                prop_oneof![Just(None), "[a-z][a-z0-9]{0,6}".prop_map(Some)],
            )
        }

        fn tags() -> impl Strategy<Value = Vec<(String, Option<String>)>> {
            prop::collection::vec(tag(), 0..=3)
        }

        /// Appends a `|#`-prefixed tag segment, a valueless token rendered bare.
        fn push_tag_segment(line: &mut String, tags: &[(String, Option<String>)]) {
            if tags.is_empty() {
                return;
            }
            line.push_str("|#");
            line.push_str(
                &tags
                    .iter()
                    .map(|(k, v)| match v {
                        Some(v) => format!("{k}:{v}"),
                        None => k.clone(),
                    })
                    .collect::<Vec<_>>()
                    .join(","),
            );
        }

        /// The rendered `|c:<id>|e:<data>|card:<card>` tail, each part optional and in
        /// `append_origin_fields`' order, or `None` for no tail at all. External data carries
        /// its own `,` separators.
        fn opt_origin() -> impl Strategy<Value = Option<String>> {
            (
                prop_oneof![Just(None), "[a-z0-9]{4,12}".prop_map(Some)],
                prop_oneof![
                    Just(None),
                    "it-(true|false)(,cn-[a-z]{1,6})?(,pu-[a-z0-9]{1,8})?".prop_map(Some)
                ],
                prop_oneof![
                    Just(None),
                    Just(Some("none")),
                    Just(Some("low")),
                    Just(Some("orchestrator")),
                    Just(Some("high")),
                ],
            )
                .prop_map(|(id, data, card)| {
                    let mut tail = String::new();
                    if let Some(id) = id {
                        let _ = write!(tail, "|c:{id}");
                    }
                    if let Some(data) = data {
                        let _ = write!(tail, "|e:{data}");
                    }
                    if let Some(card) = card {
                        let _ = write!(tail, "|card:{card}");
                    }
                    (!tail.is_empty()).then_some(tail)
                })
        }

        fn opt_secs() -> impl Strategy<Value = Option<u32>> {
            prop_oneof![Just(None), (1u32..2_000_000_000).prop_map(Some)]
        }

        /// Renders one valid line, independently of `StatsdEncoder` (the thing under test).
        #[allow(clippy::too_many_arguments)]
        fn render_line(
            name: &str,
            kind: &str,
            values: &[String],
            rate: Option<u32>,
            tags: &[(String, Option<String>)],
            origin: &Option<String>,
            secs: Option<u32>,
        ) -> String {
            let mut line = format!("{name}:{}|{kind}", values.join(":"));
            if let Some(r) = rate {
                let _ = write!(line, "|@{:.2}", f64::from(r) / 100.0);
            }
            push_tag_segment(&mut line, tags);
            if let Some(origin) = origin {
                line.push_str(origin);
            }
            if let Some(s) = secs {
                let _ = write!(line, "|T{s}");
            }
            line
        }

        fn normalize_receipt_time(batch: &mut EventBatch, had_explicit_timestamp: bool) {
            if !had_explicit_timestamp {
                for event in &mut batch.events {
                    event.timestamp = 0;
                }
            }
        }

        // -- Events and service checks: extra generators -----------------------------------

        /// A piece of an event `TITLE`/`TEXT`: ASCII, `|`/`:` (legal, since the header delimits
        /// by byte length), the two-byte escape `\n` (decoded to a newline and re-escaped), and a
        /// multi-byte character for `tlen`/`xlen` byte counting.
        fn event_piece() -> impl Strategy<Value = String> {
            prop::collection::vec(
                prop_oneof![
                    Just("a".to_string()),
                    Just("|".to_string()),
                    Just(":".to_string()),
                    Just("\\n".to_string()),
                    Just("é".to_string()),
                    Just(" ".to_string()),
                ],
                0..=6,
            )
            .prop_map(|parts| parts.concat())
        }

        fn opt_priority() -> impl Strategy<Value = Option<&'static str>> {
            prop_oneof![Just(None), Just(Some("normal")), Just(Some("low"))]
        }

        fn opt_alert_type() -> impl Strategy<Value = Option<&'static str>> {
            prop_oneof![
                Just(None),
                Just(Some("info")),
                Just(Some("success")),
                Just(Some("warning")),
                Just(Some("error")),
            ]
        }

        fn opt_word() -> impl Strategy<Value = Option<String>> {
            prop_oneof![Just(None), "[a-z][a-z0-9]{0,6}".prop_map(Some)]
        }

        /// One generated line of any of the three shapes, as one enum so all three share the
        /// property body below.
        #[derive(Debug)]
        #[allow(clippy::large_enum_variant)]
        enum GeneratedLine {
            Metric {
                name: String,
                kind: &'static str,
                values: Vec<String>,
                rate: Option<u32>,
                tags: Vec<(String, Option<String>)>,
                origin: Option<String>,
                secs: Option<u32>,
            },
            Event {
                title: String,
                text: String,
                secs: Option<u32>,
                host: Option<String>,
                priority: Option<&'static str>,
                alert_type: Option<&'static str>,
                key: Option<String>,
                source: Option<String>,
                tags: Vec<(String, Option<String>)>,
                origin: Option<String>,
            },
            ServiceCheck {
                name: String,
                status: u32,
                secs: Option<u32>,
                host: Option<String>,
                tags: Vec<(String, Option<String>)>,
                origin: Option<String>,
                message: Option<String>,
            },
        }

        /// Renders a `GeneratedLine::Event` in [`render_event`]'s field order (`d:`, `h:`, `p:`,
        /// `t:`, `k:`, `s:`, tags, `c:`/`e:`/`card:`).
        #[allow(clippy::too_many_arguments)]
        fn render_event_line(
            title: &str,
            text: &str,
            secs: Option<u32>,
            host: &Option<String>,
            priority: Option<&str>,
            alert_type: Option<&str>,
            key: &Option<String>,
            source: &Option<String>,
            tags: &[(String, Option<String>)],
            origin: &Option<String>,
        ) -> String {
            let mut line = format!("_e{{{},{}}}:{title}|{text}", title.len(), text.len());
            if let Some(s) = secs {
                let _ = write!(line, "|d:{s}");
            }
            if let Some(h) = host {
                let _ = write!(line, "|h:{h}");
            }
            if let Some(p) = priority {
                let _ = write!(line, "|p:{p}");
            }
            if let Some(t) = alert_type {
                let _ = write!(line, "|t:{t}");
            }
            if let Some(k) = key {
                let _ = write!(line, "|k:{k}");
            }
            if let Some(s) = source {
                let _ = write!(line, "|s:{s}");
            }
            push_tag_segment(&mut line, tags);
            if let Some(origin) = origin {
                line.push_str(origin);
            }
            line
        }

        /// Renders a `GeneratedLine::ServiceCheck` in [`render_service_check`]'s field order.
        fn render_service_check_line(
            name: &str,
            status: u32,
            secs: Option<u32>,
            host: &Option<String>,
            tags: &[(String, Option<String>)],
            origin: &Option<String>,
            message: &Option<String>,
        ) -> String {
            let mut line = format!("_sc|{name}|{status}");
            if let Some(s) = secs {
                let _ = write!(line, "|d:{s}");
            }
            if let Some(h) = host {
                let _ = write!(line, "|h:{h}");
            }
            push_tag_segment(&mut line, tags);
            if let Some(origin) = origin {
                line.push_str(origin);
            }
            if let Some(m) = message {
                let _ = write!(line, "|m:{m}");
            }
            line
        }

        fn event_strategy() -> impl Strategy<Value = GeneratedLine> {
            (
                event_piece(),
                event_piece(),
                opt_secs(),
                opt_word(),
                opt_priority(),
                opt_alert_type(),
                opt_word(),
                opt_word(),
                tags(),
                opt_origin(),
            )
                .prop_map(
                    |(title, text, secs, host, priority, alert_type, key, source, tags, origin)| {
                        GeneratedLine::Event {
                            title,
                            text,
                            secs,
                            host,
                            priority,
                            alert_type,
                            key,
                            source,
                            tags,
                            origin,
                        }
                    },
                )
        }

        fn service_check_strategy() -> impl Strategy<Value = GeneratedLine> {
            (
                metric_name(),
                0u32..=3,
                opt_secs(),
                opt_word(),
                tags(),
                opt_origin(),
                // `event_piece` includes `|`, which must survive in the last field, `m:`.
                prop_oneof![Just(None), event_piece().prop_map(Some)],
            )
                .prop_map(|(name, status, secs, host, tags, origin, message)| {
                    GeneratedLine::ServiceCheck { name, status, secs, host, tags, origin, message }
                })
        }

        fn arb_line() -> impl Strategy<Value = GeneratedLine> {
            prop_oneof![
                (metric_name(), kind_and_values(), opt_rate(), tags(), opt_origin(), opt_secs())
                    .prop_map(|(name, (kind, values), rate, tags, origin, secs)| {
                        GeneratedLine::Metric { name, kind, values, rate, tags, origin, secs }
                    }),
                event_strategy(),
                service_check_strategy(),
            ]
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(200))]

            #[test]
            fn decode_encode_decode_is_a_fixed_point(generated in arb_line()) {
                let (line, secs, is_set_members) = match &generated {
                    GeneratedLine::Metric { name, kind, values, rate, tags, origin, secs } => (
                        render_line(name, kind, values, *rate, tags, origin, *secs),
                        *secs,
                        *kind == "s",
                    ),
                    GeneratedLine::Event {
                        title, text, secs, host, priority, alert_type, key, source, tags, origin,
                    } => (
                        render_event_line(
                            title, text, *secs, host, *priority, *alert_type, key, source, tags,
                            origin,
                        ),
                        *secs,
                        false,
                    ),
                    GeneratedLine::ServiceCheck { name, status, secs, host, tags, origin, message } => (
                        render_service_check_line(name, *status, *secs, host, tags, origin, message),
                        *secs,
                        false,
                    ),
                };

                let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()));
                let mut d1 = match decoder.decode(bytes::Bytes::from(line.clone())) {
                    Ok(batch) => batch,
                    Err(_) => return Ok(()), // a line the decoder rejects; not this property
                };
                match &generated {
                    // A metric line can decode to zero events for reasons outside this property.
                    GeneratedLine::Metric { .. } => {
                        if d1.events.is_empty() {
                            return Ok(());
                        }
                    }
                    // An `_e{`/`_sc|` line decodes to one event or errors. Asserting it catches a
                    // decode bug that yields zero events instead of an error (trimming trailing
                    // whitespace off these shapes was one).
                    GeneratedLine::Event { .. } | GeneratedLine::ServiceCheck { .. } => {
                        prop_assert_eq!(
                            d1.events.len(),
                            1,
                            "expected exactly one decoded event for {:?}",
                            line
                        );
                    }
                }

                // Otherwise the default encoder drops every signed `g` line's `GaugeDelta`.
                let mut encoder = StatsdEncoder::new(Format::DogStatsd).with_relative_gauges(true);
                let mut out1 = MessageBuf::default();
                encoder.encode_into(&d1, &mut out1);

                let mut d2_events = Vec::new();
                for msg in out1.iter() {
                    let mut d = decoder
                        .decode(bytes::Bytes::copy_from_slice(msg))
                        .unwrap_or_else(|e| panic!("re-encoded line should decode: {e}"));
                    d2_events.append(&mut d.events);
                }
                let mut d2 = EventBatch { resource: d1.resource.clone(), scope: None, events: d2_events };

                normalize_receipt_time(&mut d1, secs.is_some());
                normalize_receipt_time(&mut d2, secs.is_some());
                if is_set_members {
                    // One N-member line re-encodes as N one-member lines, a permitted
                    // normalization (`docs/adr/lossless-transit.md`'s "splitting a multi-value
                    // statsd line"), so compare the members in order, not the event count.
                    let members_of = |batch: &EventBatch| -> Vec<Vec<u8>> {
                        batch
                            .events
                            .iter()
                            .flat_map(|e| match &e.metrics[0].kind {
                                MetricKind::SetMembers(m) => {
                                    m.iter().map(|b| b.to_vec()).collect::<Vec<_>>()
                                }
                                other => panic!("expected SetMembers, got {other:?}"),
                            })
                            .collect()
                    };
                    prop_assert_eq!(
                        members_of(&d1),
                        members_of(&d2),
                        "every member must survive decode(encode(decode(line))) for {:?}",
                        line
                    );
                } else {
                    prop_assert_eq!(
                        &d1, &d2,
                        "decode(encode(decode(line))) must equal decode(line) for {:?}",
                        line
                    );
                }

                let mut out2 = MessageBuf::default();
                encoder.encode_into(&d2, &mut out2);
                let e1: Vec<Vec<u8>> = out1.iter().map(|b| b.to_vec()).collect();
                let e2: Vec<Vec<u8>> = out2.iter().map(|b| b.to_vec()).collect();
                prop_assert_eq!(
                    e1, e2,
                    "encode(decode(line)) must be a fixed point of encode . decode for {:?}",
                    line
                );
            }
        }
    }
}
