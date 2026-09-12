//! statsd / DogStatsD egress over UDP or TCP -- the mirror of `logit_inputs::statsd`, and a real
//! relay: names, values, and tags round-trip through the real `StatsdDecoder` (pinned by this
//! module's own tests), not just through hand-checked example lines.
//!
//! Split the way `syslog.rs`/`influxdb.rs` are: a pure [`StatsdEncoder`] (no socket anywhere,
//! every grammar/sanitization/packing test runs against it directly) plus the thin
//! [`StatsdOutput`] that owns the socket.
//!
//! **This does not implement `logit_proto::Encoder`.** That trait is `fn encode(&mut self,
//! &EventBatch) -> Result<Bytes, CodecError>` -- one opaque buffer per batch, with no framing
//! metadata -- and this sink genuinely needs per-message boundaries (one line per metric, packed
//! into datagrams up to a size cap on UDP). [`crate::syslog::SyslogEncoder`] is the in-tree
//! precedent for a sink whose encoder sidesteps the trait for the same class of reason. See
//! `docs/known-gaps.md` for this recorded as an open gap in `logit_proto::Encoder`'s shape, not a
//! defect in this module.
//!
//! ## Grammar and round-trip contract
//!
//! `<name>:<value>|<type>[|@<sample-rate>][|#<tag>[:<value>],...][|c:<container-id>][|T<unix-seconds>]`
//! -- the same grammar `logit_inputs::statsd` parses. `@<sample-rate>` and `|T<unix-seconds>` are
//! both real segments this sink emits, not omitted unconditionally -- see "Sample rate: never for
//! a counter, real for `Samples`" and "`\|c:<container-id>` and `\|T<timestamp>`" below for
//! exactly when each appears. Every sanitization rule exists because of a specific way
//! `StatsdDecoder::parse_line` would otherwise misparse the result; see that module's grammar doc
//! comment for the decoder side of each rule cited here.
//!
//! ## Dialects
//!
//! `Format::DogStatsd` (default) emits the `|#k:v,k:v` tag segment; `Format::Statsd` omits it
//! entirely -- not an empty `|#`, which some plain-statsd receivers reject outright -- and counts
//! every tag it drops (`EncodeStats::tags_dropped_dialect`).
//!
//! `Format::Statsd` also normalizes two shapes that only DogStatsD's grammar can express:
//! `Samples` loses its multi-value line (`name:v1:v2|ms`) and becomes one `name:v|ms` line per
//! value, and a timer's own wire-type letter collapses from `h`/`d` to the classic grammar's `ms`
//! (counted `EncodeStats::type_normalized_dialect`) -- both are the "sink-configured dialect
//! change" and "splitting a multi-value line" normalizations `docs/adr/lossless-transit.md`
//! permits by name. `|c:<container-id>`/`|T<timestamp>` (below) have no plain-statsd equivalent at
//! all and are dropped rather than normalized.
//!
//! ## Sanitization
//!
//! A metric name has every one of `: | @ # , \n \r \0`, ASCII control characters, and whitespace
//! replaced with `_` (substitution, not deletion, so distinct names stay distinct -- following
//! `syslog.rs::sanitize_5424_field`'s approach). Each forbidden character earns its place against
//! the decoder's own grammar: `:` splits name from values, `|` splits segments, `@`/`#` open the
//! sample-rate/tag segments, `,` separates tags, `\n` separates lines. Whitespace is substituted
//! defensively, not because `decode_into` needs it to be gone: that function only trims a line's
//! leading/trailing whitespace (`line.trim_end_matches('\r').trim()`), so an embedded space or tab
//! inside a name survives a real decode unchanged (`my metric:1|c` decodes to the name
//! `"my metric"`, not an error) and is a real, reachable byte this sink still has to sanitize --
//! see `crates/logit-cli/tests/statsd_round_trip.rs`'s normalization (5) for the fixture.
//!
//! Tag *keys* forbid the same set as a name, plus nothing extra -- **and forbid `:`** for a
//! different reason than the name does: `parse_line` splits a tag on its *first* colon
//! (`tag.split_once(':')`), so a `:` inside a key would silently reparse as a shorter key with the
//! remainder folded into the value. Tag *values* forbid the same set **except `:`, which is
//! deliberately allowed**: since only the first colon is significant, `env:a:b` round-trips as key
//! `env`, value `a:b` -- an asymmetry between key and value sanitization that is easy to get
//! backwards, so it has its own test.
//!
//! A `SetMembers` member is rendered through its own, narrower rule
//! ([`is_forbidden_in_set_member`]: lossy UTF-8 first, since a member is arbitrary bytes off the
//! wire, then that rule's substitution) -- **neither** the name/tag-key rule **nor** the tag-value
//! rule, even though a member is a bare value: a member sits in `name:<member>|s`'s colon-
//! separated *value* position, the same position `parse_line` splits on to find several values
//! sharing one line (`name:v1:v2|c`), so `:` has to be forbidden here or it would silently
//! re-decode a single member as two, and `|` would open a new segment. Nothing else about that
//! position is delimiter-sensitive, though: `values_part` is only ever split on `:`, and the line
//! only on `|`/newline, so `@`, `#`, `,`, and interior whitespace all survive a real decode intact
//! -- forbidding them here (the name/tag-key rule's reason for forbidding them is that *they* open
//! or separate segments, a role a member never plays) would sanitize bytes that never needed it
//! and let distinct members collide. Control bytes (`\n`/`\r`/`\0` included) stay forbidden, since
//! any of them would corrupt line/datagram framing regardless of position. A member that comes out
//! different from its raw bytes, either because the bytes weren't valid UTF-8 or because a
//! forbidden character was substituted, is counted (`EncodeStats::members_sanitized`).
//!
//! ## Metric-kind coverage: raw kinds in, sketches still deferred
//!
//! `Counter` (`|c`), `Gauge`/`GaugeDelta` (`|g`), `Samples` (`|ms`/`|h`/`|d`), and `SetMembers`
//! (`|s`) are all encoded -- `Samples`/`SetMembers` are the raw, unsummarized shapes
//! `statsd_in` decodes losslessly (`docs/adr/lossless-transit.md`'s "summarization is opt-in and
//! named"), so a `statsd_in -> statsd_out` relay with no `aggregate` in between round-trips a
//! timer or set line intact. `Distribution`, `Set`, `Histogram`, `ExponentialHistogram`,
//! `Summary`, and a cumulative or non-monotonic `Sum` -- everything that only exists *after* some
//! stage has already summarized -- are dropped with a clear "not implemented yet" message
//! (`EncodeStats::dropped_unsupported_kind`) -- recorded in `docs/known-gaps.md`.
//!
//! **This means a `statsd_in -> aggregate -> statsd_out` relay still drops every timer/set metric
//! whose window used `aggregate`'s default summarizing config.** `aggregate`'s default turns
//! `Samples` into a `Distribution` sketch and `SetMembers` into a `Set` cardinality estimate,
//! neither of which has a lossless statsd rendering (see the module doc's opening paragraph and
//! `docs/adr/statsd-output.md`'s original deferral) -- configuring that `aggregate` component with
//! `distributions: samples` / `sets: members` keeps the raw shapes flowing through instead, so
//! this sink's real `Samples`/`SetMembers` encoding can relay them.
//!
//! ## Relative gauges
//!
//! statsd is the one protocol that natively expresses a *relative* gauge adjustment
//! (`name:+5|g`/`name:-5|g`) -- exactly [`logit_core::MetricKind::GaugeDelta`]'s wire origin. By
//! default a `GaugeDelta` reaching this sink is dropped with the same message
//! `influxdb_out` uses (`gauge_delta_unresolved`): it means the pipeline is missing an `aggregate`
//! component, not that this metric is malformed. Setting `relative_gauges: true` opts into
//! encoding it natively instead -- the one sink that *can* round-trip a delta losslessly, since
//! every other sink's wire format has no such concept at all.
//!
//! A positive delta needs an explicit `+`: `write!("{}", 5.0)` yields `"5"`, which the decoder
//! reads back as an *absolute* `Gauge`, not a delta -- silently corrupting the round-trip this
//! sink exists to preserve.
//!
//! ## Negative absolute gauges
//!
//! The statsd/DogStatsD grammar has no wire syntax for setting a gauge to a negative absolute
//! value at all (`logit_inputs::statsd::build_event`'s `"g"` arm reads *any* leading `-` as a
//! delta) -- so a naive `Gauge(-5.0)` would render as `name:-5|g` and decode back as
//! `GaugeDelta(-5.0)`, a silent semantic corruption. This sink instead emits the idiom both Etsy
//! statsd and DogStatsD document for exactly this case: `name:0|g` immediately followed by
//! `name:-5|g`. The two lines are pushed into the [`MessageBuf`] as **one indivisible entry**
//! (joined by an embedded `\n`) so the packer can never split them across two datagrams -- a lost
//! first datagram would otherwise apply `-5` to whatever stale value the gauge already held at the
//! receiver. This is the only place an entry contains a newline; every sanitizer above exists
//! precisely to guarantee nothing else ever does.
//!
//! `Gauge(-0.0)` is deliberately *not* a pair: it is numerically zero, so its sign is normalized
//! away and it renders as the plain `name:0|g` -- the naive `name:-0|g` would decode as a no-op
//! `GaugeDelta`, since the decoder dispatches on the leading `-` without parsing the value.
//!
//! ## Packing and framing
//!
//! UDP **packs** several lines into one datagram, up to `max_packet_bytes`
//! (`\n`-joined, no trailing `\n`, since the datagram boundary itself ends the last line). This is
//! a deliberate divergence from `syslog_out`, which refuses to pack because packing there would
//! depend on the receiver splitting on a delimiter its whole "injection safety" section exists to
//! avoid relying on. The reasoning inverts here: splitting on `\n` **is** the statsd grammar
//! (every statsd client packs a buffered send this way, and `StatsdDecoder::decode_into` splits on
//! it directly), and the sanitizers above make an embedded `\n` unrepresentable in a name, key, or
//! value -- so a packed datagram cannot forge an extra metric the way a packed syslog datagram
//! could forge an extra log line. A line that would overflow the cap starts a new datagram rather
//! than being split; a single line longer than the cap is **dropped whole**, never truncated
//! (unlike `syslog_out`) -- a truncated statsd line decodes as a different metric or a parse
//! error, never a shorter version of the same one.
//!
//! TCP terminates **every** line with `\n`, including the last -- a stream has no per-batch EOF,
//! so without a trailing separator the last line of one batch would glue onto the first line of
//! the next. No octet-counting: statsd has no such framing convention and no receiver auto-detects
//! one, unlike syslog's `go-syslog`.
//!
//! ## Sample rate: never for a counter, real for `Samples`
//!
//! A `Counter`'s value already has its sample rate divided out at decode time, so this sink never
//! emits `@<rate>` for `|c` -- doing so would double-extrapolate downstream. `Samples` is
//! different: `statsd_in` no longer extrapolates timer/histogram samples at all (raw values are
//! kept, unlike a counter's single scalar), so `Samples.sample_rate` is real, un-applied
//! information that must reach the wire for a lossless relay -- `@<rate>` is emitted whenever it
//! isn't `1.0` (see [`render_metric`]'s `Samples` arm). `MetricRecord::unit` still has no statsd
//! wire representation and is dropped the same way it always was.
//!
//! ## `|c:<container-id>` and `|T<timestamp>`
//!
//! Both are DogStatsD-only line extensions this sink now round-trips: `statsd.container_id`
//! (a `Value::Str` attribute `statsd_in` stamps from an incoming `|c:<id>` segment) renders as
//! `|c:<id>` (sanitized like a tag value, so an embedded `:` survives); `statsd.timestamp ==
//! Value::U64(secs)` (the raw wire seconds `statsd_in` stamps alongside moving the value onto
//! `Event::timestamp`) renders as `|T<secs>` **using that carrier's own value, never
//! `event.timestamp`** -- a stage that rebuilds `Event::timestamp` after decode (`aggregate`'s
//! flush, notably) can't fabricate or collapse a `|T` this way, since the carrier rides on the
//! series key exactly like any other attribute; a `statsd.timestamp` present but not a
//! `Value::U64` (never produced by `statsd_in` itself, but reachable from a cross-protocol relay
//! or a Lua-authored attribute) is simply not emitted. Both segments are appended after the tag
//! segment, to every line this sink emits for that event -- see `append_dialect_extras`. Under
//! `Format::Statsd` neither has anywhere to go (the classic grammar has no equivalent segment), so
//! both are dropped and counted (`EncodeStats::dropped_dialect_fields`) rather than silently
//! disappearing.
//!
//! `statsd.*` attributes (`statsd.type`, `statsd.container_id`, `statsd.timestamp`) are
//! protocol-namespaced carriers for exactly this information, not ordinary tags -- they are never
//! emitted through the `|#k:v,...` tag segment (`build_tag_suffix` filters the prefix), the same
//! way `syslog_out` never re-emits its own `syslog.*` attributes as generic SD-ELEMENT fields.
//! `build_tag_suffix`'s merged resource⊕event walk (the same one that filters them out of the tag
//! segment) is also where all three carriers are captured, into [`EncodeCtx`]'s own fields, so a
//! carrier set only on the resource (a `set` transform's `resource:` block, say) is honored the
//! same way an event-level one already is -- `append_dialect_extras`/`statsd_wire_type` read
//! `EncodeCtx`, never `event.attributes`, directly, keeping the filter and the read symmetric.
//!
//! ## DogStatsD events and service checks
//!
//! An **event** is `event.log.is_some()` *and* carries `statsd.event.title` as a `Value::Str`
//! (the shape `statsd_in`'s `parse_event` always produces), event value winning over the
//! resource's on collision, mirroring `crate::attrs::merged`'s precedence for this one key; a
//! **service check** is `statsd.service_check.name` present as a `Value::Str`. Detection is
//! two-tier in [`StatsdEncoder::encode_into`], split on `event.metrics.is_empty()` -- a real
//! event/service check never has both an empty `metrics` list and no event-ness, but the
//! overwhelming majority of metrics-empty events are a plain log/span-only line with no
//! `statsd.*` carriers at all (any log-only input, `statsd_in`'s own event lines aside), so:
//! a metrics-empty event first gets a single, *unmerged* [`is_dogstatsd_event`] lookup
//! (`event.attributes.get` then, only if absent, `resource.attributes.get`) -- cheap enough that
//! the ordinary "not an event" case never touches [`build_tag_suffix`]'s full merged walk at all,
//! and is counted straight into `skipped_no_metrics` without inflating `tags_dropped_dialect` for
//! tags nothing was ever going to render; only once that lookup says "yes" does the full walk run
//! (for every other carrier the line needs). An event with metrics (a service check, or an
//! ordinary metric event) always runs the full walk first, exactly as before -- service-check
//! detection reads `Carriers::service_check_name` off it, zero extra lookups. Either way, the
//! full walk (into [`Carriers`]) is what `statsd.type`/`statsd.container_id`/`statsd.timestamp`
//! are also captured from, so a carrier set only on the resource is honored the same way an
//! event-level one is -- `event.attributes` is never read a second time for those.
//!
//! **Event wire form**, one line: `_e{tlen,xlen}:title|text`, followed, in this order and only
//! when the field is present and valid, by `|d:<secs>` (from the `statsd.timestamp` carrier --
//! `d:`, never `|T`: an event/service-check line's timestamp is a named field of its own grammar,
//! not the generic metric-line extension `append_dialect_extras` emits elsewhere in this module),
//! `|h:<host>`, `|p:<priority>` (`normal`/`low` only), `|t:<alert_type>` (`info`/`success`/
//! `warning`/`error` only), `|k:<aggregation_key>`, `|s:<source_type>`, the `|#...` tag segment
//! (via [`build_tag_suffix`]/[`append_tags`], identical to a metric line's), then `|c:<container
//! id>`. `title` is `statsd.event.title` verbatim; `text` is the log `message` -- must be a
//! `Value::Str`, anything else drops the event and counts `EncodeStats::dropped_unencodable_value`
//! -- with every real `\n` it contains escaped to the two bytes `\n` (`statsd_in`'s
//! `unescape_event_text` is the decode-side mirror of exactly this). `tlen`/`xlen` are the *byte*
//! lengths of the sanitized title and the escaped text as written, not char counts -- DogStatsD's
//! `_e{TITLE_LEN,TEXT_LEN}` header is a byte-length-prefixed encoding, and `parse_event` slices by
//! those exact byte counts, so a char count would silently corrupt a multi-byte title/text.
//! Severity is never re-derived from `LogRecord.severity` to synthesize a `t:` field when
//! `statsd.event.alert_type` is absent -- rule (b) (`docs/adr/lossless-transit.md`): the raw
//! carrier outranks the normalized field on this protocol's own egress, and an absent carrier
//! means an absent wire field, not an invented one.
//!
//! **Service-check wire form**, one line: `_sc|<name>|<status>`, followed by `|d:<secs>`, `|h:
//! <host>`, the `|#...` tag segment, `|c:<container id>`, and -- always last, since `m:` consumes
//! the rest of the line verbatim on decode -- `|m:<message>`. The event's **first** metric is the
//! service check; it must be a `Gauge`, else the whole event is dropped and counted
//! (`EncodeStats::dropped_invalid_service_check`) rather than falling through to an ordinary
//! `name:v|g` line -- the point *is* the check, not a gauge that happens to share its value.
//! `name` is the `statsd.service_check.name` carrier, **not** the metric's own (normalized) name
//! -- rule (b) again, and unlike a metric name this is sanitized with [`is_forbidden_in_extended_field`]
//! (`|`/control bytes only), not [`is_forbidden_in_name`], so a service check name keeps `.` and
//! spaces exactly as sent. `status` is `statsd.service_check.status` when it's a `U64` in `0..=3`;
//! otherwise the gauge's own value, if finite and rounding into `0..=3`; otherwise the service
//! check is dropped and counted (`dropped_invalid_service_check`) rather than writing an
//! out-of-range status DogStatsD's own decoder would reject. `message` is
//! `statsd.service_check.message` verbatim except a newline or other control byte substituted
//! with `_` -- unlike event text, there is no escape for this position, only substitution; `|`
//! survives untouched since `m:` is always the last field. Any metrics after the first on a
//! service-check event render as ordinary lines (`render_metric`), right after the `_sc` line.
//!
//! **Sanitization**, one rule per field, all substitution (never deletion, following this
//! module's existing convention): event title -- control bytes -> `_` ([`is_forbidden_in_event_title`];
//! a bare `|` is fine, since the length prefix delimits the field, not a scan for `|`). Event text
//! -- a real newline becomes the two-byte escape `\n`; any other control byte -> `_`
//! ([`append_event_text`]). Event host/aggregation-key/source, and service-check name/host --
//! `|`/control bytes -> `_` ([`is_forbidden_in_extended_field`] -- shared by all five, since each
//! sits in a `|letter:value` field a real decode splits on the *next* `|`, exactly the way a tag
//! value does, except a tag value's own `:`-preserving rule doesn't apply here since none of these
//! fields has a tag value's colon-splitting ambiguity). Event priority/alert-type are never
//! sanitized at all -- either they exactly match the fixed allowed set (`normal`/`low`;
//! `info`/`success`/`warning`/`error`) and are written verbatim, or they don't and are omitted,
//! counting `EncodeStats::dropped_invalid_event_fields` (an out-of-set value has no sanitized form
//! that would still mean the same thing, so there is nothing to substitute into -- and it's a
//! dropped *field* on a line that still gets emitted, not a dropped message, hence its own counter
//! rather than `dropped_unencodable_value`, which reports as a message drop). Service-check
//! message -- control bytes (including a real newline) -> `_`, `|` left alone
//! ([`is_forbidden_in_service_check_message`]).
//!
//! **`Format::Statsd` has no wire form for either shape at all** -- the classic grammar has no
//! `_e`/`_sc` sigil -- so the whole event is dropped and counted
//! (`EncodeStats::dropped_dialect_events`) before anything else about it is even inspected; a
//! service check's gauge is not emitted as `name:v|g` either, since the point being made is the
//! check, not a value that happens to coincide with one. A real event/service check dropped this
//! way still had its full merged walk run first ([`is_dogstatsd_event`] said "yes"), so its
//! ordinary attributes are also tallied into `tags_dropped_dialect` by that walk -- both counters
//! incrementing for the one dropped line is expected, not a double-count bug.
//!
//! `statsd.event.*`/`statsd.service_check.*` are protocol carriers exactly like `statsd.type`/
//! `statsd.container_id`/`statsd.timestamp` -- filtered out of the generic `|#k:v,...` tag segment
//! by the same `key_str.starts_with("statsd.")` check in [`build_tag_suffix`], never re-emitted as
//! a tag on any line, event/service-check or otherwise.

use crate::influxdb::{push_float, tag_value};
use crate::msgbuf::MessageBuf;
use crate::Output;
use anyhow::Context;
use logit_core::{
    Diagnostics, Event, EventBatch, MetricKind, MetricRecord, Resource, Telemetry, Temporality,
    Value,
};
use logit_pipeline::Fault;
use std::fmt::Write as _;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{lookup_host, TcpStream, UdpSocket};

/// Etsy statsd's own "commodity Ethernet LAN" recommendation, and also DataDog's documented
/// DogStatsD client default -- 1500 MTU minus IPv4/UDP headers minus ~40 bytes of headroom for
/// VXLAN/IPsec encapsulation, exactly the case where a 1472-byte datagram would silently fragment
/// or `EMSGSIZE`. Unlike DataDog's loopback/UDS figure (8192, matching `syslog_out`'s
/// `DEFAULT_MAX_MESSAGE_BYTES`) this doesn't assume the destination is local. Bounds one
/// **datagram** (several packed lines), not a single line.
pub const DEFAULT_MAX_PACKET_BYTES: usize = 1432;

/// TCP only -- mirrors `syslog::DEFAULT_CONNECT_TIMEOUT`'s reasoning and value exactly: `logit-
/// config`'s own `default_statsd_connect_timeout` hardcodes the same 5 seconds, kept in sync by
/// hand.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Which statsd dialect [`StatsdEncoder`] emits. Deliberately its own tiny enum rather than
/// `logit_config::StatsdFormat` -- `logit-outputs` never depends on `logit-config`
/// (`docs/design/pipeline-graph.md`'s crate layout); `logit-cli::pipeline::build_spec` is the sole
/// place a config value crosses into this type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    DogStatsd,
    Statsd,
}

/// Per-batch outcome counts from [`StatsdEncoder::encode_into`] -- what `StatsdOutput::send` turns
/// into `logit.output.*` telemetry (`docs/design/internal-telemetry.md`).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EncodeStats {
    /// Events with no metrics (a log-only or span-only event, legal under
    /// `docs/adr/multi-payload-events.md`) -- the same "nothing to render" skip `influxdb_out`
    /// makes for the same shape of event.
    pub skipped_no_metrics: usize,
    pub dropped_gauge_delta: usize,
    /// A `NO_RECORDED_VALUE`-flagged point -- statsd has no wire concept of "no value here," so
    /// unlike `otlp_out` this sink can't keep the point flagged; it drops it rather than write its
    /// default value as a fabricated real sample (`docs/adr/lossless-transit.md`,
    /// `docs/known-gaps.md`'s cross-protocol table).
    pub dropped_no_recorded_value: usize,
    pub dropped_unsupported_kind: usize,
    /// Also counts a non-finite value inside a `Samples`/`Sum`/`Gauge`/`GaugeDelta` record, an
    /// out-of-range `Samples.sample_rate` (finite, `@rate` omitted rather than written), an
    /// entirely empty `Samples`/`SetMembers` record -- see [`render_metric`]'s `Samples`/
    /// `SetMembers` arms -- and, new in this workstream: an event whose log `message` isn't a
    /// `Value::Str` (or isn't valid UTF-8), which drops the whole event ([`render_event`]). The
    /// shared name follows this file's existing "one bucket per kind of unencodable input"
    /// convention rather than adding a near-duplicate counter. An out-of-the-fixed-set
    /// `statsd.event.priority`/`statsd.event.alert_type` is a *different* shape of problem --
    /// the line still gets emitted, just without that one field -- so it's counted separately, in
    /// [`Self::dropped_invalid_event_fields`], not here.
    pub dropped_unencodable_value: usize,
    pub dropped_empty_name: usize,
    pub dropped_oversize_line: usize,
    pub tags_dropped_dialect: usize,
    pub tags_dropped_unrepresentable: usize,
    /// A timer's wire-type letter (`statsd.type`) was `h`/`d` and had to collapse to `ms` under
    /// `Format::Statsd`, which has no such distinction -- counted once per `Samples` record
    /// normalized, not once per split line.
    pub type_normalized_dialect: usize,
    /// `statsd.container_id`/`statsd.timestamp` dropped under `Format::Statsd`, which has no
    /// `|c:`/`|T` equivalent -- see `append_dialect_extras`. Counted once per field per emitted
    /// line (so a negative-absolute-gauge's two-line pair, or a multi-member `SetMembers` record,
    /// counts once per physical line, matching "every emitted line" in the module doc).
    pub dropped_dialect_fields: usize,
    /// A `SetMembers` member came out different from its raw bytes after lossy UTF-8 plus
    /// [`is_forbidden_in_set_member`]'s own, member-specific substitution -- see the module doc's
    /// "Sanitization" section.
    pub members_sanitized: usize,
    /// An event or service check dropped whole under [`Format::Statsd`], which has no `_e`/`_sc`
    /// wire form at all -- counted before anything else about the event is inspected. See the
    /// module doc's "DogStatsD events and service checks" section.
    pub dropped_dialect_events: usize,
    /// A service check whose first metric wasn't a `Gauge` (the shape `statsd_in` always
    /// produces), or whose status was neither a `statsd.service_check.status` carrier in `0..=3`
    /// nor a gauge value finite and rounding into `0..=3` -- the whole event is dropped rather
    /// than falling through to an ordinary `name:v|g` line. See [`render_service_check`].
    pub dropped_invalid_service_check: usize,
    /// An event's `statsd.event.priority`/`statsd.event.alert_type` carrier held a value outside
    /// its fixed allowed set (`normal`/`low`; `info`/`success`/`warning`/`error`) -- that one
    /// field is omitted, counted once per occurrence, but the rest of the line still renders and
    /// is still emitted. Deliberately **not** `dropped_unencodable_value`: that field reports as
    /// `logit.output.messages.dropped{reason="unencodable_value"}`, which would claim the whole
    /// message was dropped when only one of its fields was. See [`render_event`].
    pub dropped_invalid_event_fields: usize,
}

/// Encodes events as statsd lines. Pure -- no socket anywhere -- so every grammar/sanitization/
/// packing test runs directly against this, with no transport of any kind involved.
pub struct StatsdEncoder {
    format: Format,
    relative_gauges: bool,
    diag: Diagnostics,
    /// The `|#k:v,k:v` tag segment for the event currently being encoded -- built once per event,
    /// shared across that event's metrics (`influxdb.rs::render_tag_suffix`'s same split).
    tag_suffix: String,
    /// One rendered line (or, for a negative absolute gauge, the two-line pair joined by `\n`).
    /// Cleared per metric, never reallocated -- `syslog.rs::SyslogEncoder::line`'s discipline.
    line: String,
    /// The sanitized metric name for the metric currently being encoded. Its own field, not a
    /// local in `render_metric`, for the same reason `line` is -- a function-local `String::new()`
    /// would reallocate on every single metric.
    name: String,
    /// The sanitized member text for the `SetMembers` member currently being rendered -- same
    /// reallocation-avoidance reasoning as `name`, its own field since `SetMembers` renders one
    /// line per member and needs `name` and the member's own text live at once.
    member: String,
    /// Scratch for [`tag_value`]'s non-`Str` formatting only -- every use within one event is
    /// read-immediately-into-`tag_suffix`-then-cleared before the next, never overlapping in time.
    scratch: String,
    /// The sanitized event title currently being rendered -- its own field for the same
    /// reallocation-avoidance reason `name`/`member` are. Never live at the same time as `name`/
    /// `member`: an event line and an ordinary metric line are never rendered from the same call.
    title_buf: String,
    /// The escaped event text (the log message, real `\n` turned into the two-byte `\n` escape)
    /// currently being rendered -- computed into its own buffer, alongside `title_buf`, before
    /// either is known to fit in the final line (the `_e{tlen,xlen}` header needs both lengths
    /// first).
    text_buf: String,
}

impl StatsdEncoder {
    pub fn new(format: Format) -> Self {
        Self {
            format,
            relative_gauges: false,
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

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    /// Encodes every event in `batch` into `out` (cleared first). Never fails -- a per-metric
    /// problem (an unsupported kind, an unresolved delta, a non-finite value, an oversize line) is
    /// a drop counted in the returned [`EncodeStats`], not an error; there is nothing for a caller
    /// to react to beyond what the stats already report. `max_packet_bytes` bounds a UDP
    /// datagram's worth of packed lines; pass `usize::MAX` for TCP, which has no such cap (the
    /// per-line oversize drop still applies).
    pub fn encode_into(
        &mut self,
        batch: &EventBatch,
        max_packet_bytes: usize,
        out: &mut MessageBuf,
    ) -> EncodeStats {
        out.clear();
        let mut stats = EncodeStats::default();
        for event in &batch.events {
            if event.metrics.is_empty() {
                // A metrics-empty event is either a DogStatsD event or (overwhelmingly more
                // often -- any log-only input, `statsd_in`'s own event lines aside) a plain log/
                // span-only line with nothing to render. Deciding which needs only
                // `statsd.event.title`'s own precedence, not the full merged walk
                // `build_tag_suffix` runs -- see [`is_dogstatsd_event`] and the module doc's
                // "DogStatsD events and service checks" section.
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
                    // No `Format::Statsd` wire form at all -- drop the whole event. The walk just
                    // above already tallied this event's ordinary attributes into
                    // `tags_dropped_dialect`, same as any other dialect-dropped tag; both counters
                    // incrementing here is expected (module doc).
                    stats.dropped_dialect_events += 1;
                    continue;
                }

                let mut ctx = EncodeCtx {
                    format: self.format,
                    tag_suffix: &self.tag_suffix,
                    statsd_type: carriers.statsd_type,
                    container_id: carriers.container_id,
                    timestamp_secs: carriers.timestamp_secs,
                    max_packet_bytes,
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

            // An event with metrics: the full merged walk always runs (as it always has), and
            // service-check detection reads it straight off `carriers` -- zero extra lookups.
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
                // No `Format::Statsd` wire form at all -- drop the whole event before inspecting
                // anything else about it (module doc).
                stats.dropped_dialect_events += 1;
                continue;
            }

            let mut ctx = EncodeCtx {
                format: self.format,
                tag_suffix: &self.tag_suffix,
                statsd_type: carriers.statsd_type,
                container_id: carriers.container_id,
                timestamp_secs: carriers.timestamp_secs,
                max_packet_bytes,
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

/// Cheap detection for a metrics-empty event: does it carry `statsd.event.title` as a
/// `Value::Str`, the event's own value winning over the resource's on collision (mirroring
/// `crate::attrs::merged`'s precedence for this one key, without paying the full merged walk over
/// every attribute)? `event.attributes.get`/`resource.attributes.get` are `O(log n)` binary
/// searches on an already-sorted `AttrMap` (`crates/logit-core/src/attrs.rs`), not a scan -- so
/// this costs at most two lookups, against the full walk [`build_tag_suffix`] would otherwise run
/// (over every resource and event attribute, plus a `tags_dropped_dialect` tally under
/// `Format::Statsd`) for what is, for the overwhelming majority of metrics-empty events, a plain
/// log line carrying no `statsd.*` carrier at all. Used only to gate whether
/// [`StatsdEncoder::encode_into`] pays that full walk for a metrics-empty event; an event with
/// metrics always pays it regardless (service-check detection needs the rest of `Carriers` too).
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

/// Bundles the per-batch context [`render_metric`] and its `Samples`/`SetMembers` helpers need but
/// don't own -- one mutable borrow of this instead of six-plus loose parameters on every function
/// in the call chain. `out`/`stats`/`diag` are threaded through as `&mut` since every line
/// rendered writes into all three (a pushed line, an updated counter, a throttled diagnostic).
/// `statsd_type`/`container_id`/`timestamp_secs` are the three `statsd.*` carriers
/// [`build_tag_suffix`]'s merged resource⊕event walk captures (the event's value winning over the
/// resource's, same as every other attribute) -- [`append_dialect_extras`]/[`statsd_wire_type`]
/// read them from here, never from `event.attributes` directly, so a carrier set only on the
/// resource is honored the same way an event-level one already is.
struct EncodeCtx<'a> {
    format: Format,
    tag_suffix: &'a str,
    statsd_type: Option<&'a str>,
    container_id: Option<&'a str>,
    timestamp_secs: Option<u64>,
    max_packet_bytes: usize,
    stats: &'a mut EncodeStats,
    diag: &'a mut Diagnostics,
    out: &'a mut MessageBuf,
}

/// Out-parameter for [`build_tag_suffix`]'s merged walk: the three `statsd.*` carriers, captured
/// as they're skipped out of the generic tag segment rather than re-read from `event.attributes`
/// afterward -- one pass does both jobs, and stays symmetric with what it filters out. Built
/// separately from [`EncodeCtx`] because `EncodeCtx::tag_suffix` borrows the very `String`
/// `build_tag_suffix` still needs `&mut` access to at the point these carriers are captured.
#[derive(Debug, Default, Clone, Copy)]
struct Carriers<'a> {
    statsd_type: Option<&'a str>,
    container_id: Option<&'a str>,
    timestamp_secs: Option<u64>,
    /// `statsd.event.title` -- always present on a real `statsd_in`-decoded event; its presence
    /// (alongside `event.log.is_some()`) is exactly [`StatsdEncoder::encode_into`]'s event
    /// detection rule.
    event_title: Option<&'a str>,
    event_priority: Option<&'a str>,
    event_alert_type: Option<&'a str>,
    event_aggregation_key: Option<&'a str>,
    event_source_type: Option<&'a str>,
    event_host: Option<&'a str>,
    /// `statsd.service_check.name` -- always present on a real `statsd_in`-decoded service check;
    /// its presence is exactly the encoder's service-check detection rule.
    service_check_name: Option<&'a str>,
    service_check_status: Option<u64>,
    service_check_message: Option<&'a str>,
    service_check_host: Option<&'a str>,
}

/// Builds this event's DogStatsD tag segment into `suffix` (cleared first, **no** leading `|#` --
/// [`render_metric`]/[`append_tags`] add that only if `suffix` ends up non-empty). Resource
/// attributes first, event attributes overriding on key collision, merge-joined via
/// [`crate::attrs::merged`]. Under [`Format::Statsd`] this always leaves `suffix` empty and counts
/// every attribute that would otherwise have become a tag into `stats.tags_dropped_dialect`.
///
/// The same merged walk also captures the three `statsd.*` carriers into `carriers` -- see
/// [`Carriers`]'s doc comment for why that happens here rather than as a second, separate read of
/// `event.attributes` later.
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
        // `statsd.*` attributes are protocol carriers this decoder stamped from a line's own
        // `|c:`/`|T`/wire-type segments (`statsd.container_id`/`statsd.timestamp`/`statsd.type`),
        // not ordinary tags -- `append_dialect_extras`/`statsd_wire_type` read them (via `carriers`
        // then `EncodeCtx`) and emit their own dedicated segments, so they must never also
        // round-trip through the generic tag segment. Not counted: this is a carrier being read
        // for its real purpose, not data being dropped (`syslog_out`'s identical `syslog.*` filter
        // is the precedent).
        if key_str.starts_with("statsd.") {
            match (key_str, value) {
                ("statsd.type", Value::Str(s)) => {
                    carriers.statsd_type = std::str::from_utf8(s).ok();
                }
                ("statsd.container_id", Value::Str(s)) => {
                    carriers.container_id = std::str::from_utf8(s).ok();
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
                // Anything else (an absent/wrong-typed carrier, or a `statsd.*` key this sink
                // doesn't know about) is ignored here the same way it's ignored as a tag --
                // forward-compatible, not a hard error.
                _ => {}
            }
            continue;
        }

        if format == Format::Statsd {
            stats.tags_dropped_dialect += 1;
            continue;
        }

        if key_str.is_empty() {
            stats.tags_dropped_unrepresentable += 1;
            continue;
        }
        // `Bool(true)` is DogStatsD's own bare-tag idiom (`#urgent`, no `:value`) -- exactly what
        // `logit_inputs::statsd::parse_line` produces for a valueless tag. Emitting `key:true`
        // instead would round-trip as `Value::Str("true")`, silently changing the value's type.
        let bare = matches!(value, Value::Bool(true));
        let rendered_value = if bare { None } else { tag_value(scratch, value) };
        if !bare && rendered_value.is_none() {
            stats.tags_dropped_unrepresentable += 1;
            continue;
        }

        if !suffix.is_empty() {
            suffix.push(',');
        }
        let key_start = suffix.len();
        sanitize_into(suffix, key_str, is_forbidden_in_tag_key);
        if suffix.len() == key_start {
            // Sanitized to nothing (only possible if `key_str` itself was empty, already handled
            // above, but kept as a defensive no-op-key guard) -- undo any separator just pushed.
            if suffix.ends_with(',') {
                suffix.pop();
            }
            stats.tags_dropped_unrepresentable += 1;
            continue;
        }
        if let Some(v) = rendered_value {
            suffix.push(':');
            sanitize_into(suffix, v, is_forbidden_in_tag_value_only);
        }
    }
}

/// Appends `line`'s `|#`-prefixed tag segment, if `tag_suffix` is non-empty. Shared by every
/// metric-kind arm in [`render_metric`], including both lines of a negative-gauge pair.
fn append_tags(line: &mut String, tag_suffix: &str) {
    if !tag_suffix.is_empty() {
        line.push_str("|#");
        line.push_str(tag_suffix);
    }
}

/// Appends `ctx`'s `|c:<container-id>`/`|T<timestamp>` segments under [`Format::DogStatsd`] --
/// see the module doc's "`|c:<container-id>` and `|T<timestamp>`" section. Called once per
/// physical line [`render_metric`] (or its `Samples`/`SetMembers` helpers) emits, *after*
/// [`append_tags`], so a negative-absolute-gauge's two-line pair or a multi-line `Samples`/
/// `SetMembers` record gets it on every line -- each is, on the wire, its own statsd line that
/// genuinely carried (or would carry) its own `|c:`/`|T` segment. Reads `ctx.container_id`/
/// `ctx.timestamp_secs` -- captured by `build_tag_suffix`'s merged walk, not re-read from
/// `event.attributes` here -- so `|T` always emits exactly the wire seconds the carrier holds,
/// never a value derived from `event.timestamp` (which a summarizing stage can rebuild at flush
/// time). Under [`Format::Statsd`] neither field has anywhere to go, so both are dropped and
/// counted (`EncodeStats::dropped_dialect_fields`) rather than silently omitted.
fn append_dialect_extras(line: &mut String, ctx: &mut EncodeCtx) {
    match ctx.format {
        Format::DogStatsd => {
            if let Some(id) = ctx.container_id {
                line.push_str("|c:");
                sanitize_into(line, id, is_forbidden_in_tag_value_only);
            }
            if let Some(secs) = ctx.timestamp_secs {
                let _ = write!(line, "|T{secs}");
            }
        }
        Format::Statsd => {
            if ctx.container_id.is_some() {
                ctx.stats.dropped_dialect_fields += 1;
            }
            if ctx.timestamp_secs.is_some() {
                ctx.stats.dropped_dialect_fields += 1;
            }
        }
    }
}

/// Appends `|c:<container-id>` only, **never** `|T` -- the counterpart to
/// [`append_dialect_extras`] for an event/service-check line, whose own timestamp carriage
/// already happened via its `d:<secs>` field (module doc's "DogStatsD events and service checks"
/// section). Called only under `Format::DogStatsd`: [`StatsdEncoder::encode_into`] drops an
/// event/service-check whole under `Format::Statsd` before either render function -- and
/// therefore this -- is ever reached, so there is no dialect-drop accounting to do here the way
/// `append_dialect_extras` has to for an ordinary metric line.
fn append_container_id(line: &mut String, ctx: &EncodeCtx) {
    if let Some(id) = ctx.container_id {
        line.push_str("|c:");
        sanitize_into(line, id, is_forbidden_in_tag_value_only);
    }
}

/// Renders one `_e{tlen,xlen}:title|text[...]` line for an event straight into `line`, or renders
/// nothing (dropping and counting `EncodeStats::dropped_unencodable_value`) if the log `message`
/// isn't valid UTF-8 text. `title_buf`/`text_buf` are reused scratch (never live at the same time
/// as `render_metric`'s `name`/`member` -- an event line and an ordinary metric line are never
/// rendered from the same call). See the module doc's "DogStatsD events and service checks"
/// section for the full field order and sanitization rules.
fn render_event(
    line: &mut String,
    title_buf: &mut String,
    text_buf: &mut String,
    carriers: &Carriers,
    event: &Event,
    ctx: &mut EncodeCtx,
) {
    let title = carriers.event_title.expect("caller only calls this when event_title is Some");
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
    append_container_id(line, ctx);

    push_line(line, ctx.max_packet_bytes, ctx.stats, ctx.diag, ctx.out);
}

/// Renders one `_sc|name|status[...]` line for a service check straight into `line`. `metric` is
/// the event's first metric, already confirmed by the caller ([`StatsdEncoder::encode_into`]) to
/// be a `MetricKind::Gauge`. Renders nothing (dropping and counting
/// `EncodeStats::dropped_invalid_service_check`) if no valid `0..=3` status can be found on
/// either the `statsd.service_check.status` carrier or the gauge's own value. See the module
/// doc's "DogStatsD events and service checks" section.
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
    sanitize_into(line, name, is_forbidden_in_extended_field);
    let _ = write!(line, "|{status}");

    if let Some(secs) = ctx.timestamp_secs {
        let _ = write!(line, "|d:{secs}");
    }
    if let Some(host) = carriers.service_check_host {
        line.push_str("|h:");
        sanitize_into(line, host, is_forbidden_in_extended_field);
    }
    append_tags(line, ctx.tag_suffix);
    append_container_id(line, ctx);
    if let Some(message) = carriers.service_check_message {
        line.push_str("|m:");
        sanitize_into(line, message, is_forbidden_in_service_check_message);
    }

    push_line(line, ctx.max_packet_bytes, ctx.stats, ctx.diag, ctx.out);
}

/// Checks `line`'s length against `max_packet_bytes` and either pushes it to `out` or counts and
/// logs the drop -- the single place every rendered line (one per `render_metric` call for most
/// kinds, one per value/member for `Samples`/`SetMembers`) funnels through, so the oversize-drop
/// behavior stays identical regardless of which arm produced the line.
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

/// Encodes one metric, pushing every line it produces straight into `ctx.out` (zero lines for a
/// dropped metric, one for most kinds, two for a negative-absolute-gauge pair, or one per value/
/// member for `Samples`/`SetMembers`). `name`/`member` are reused scratch buffers (cleared here or
/// by the helper that owns them), not locals -- a fresh `String::new()` per metric/member would
/// reallocate on every single call, the same reasoning `line`/`tag_suffix`/`scratch` are struct
/// fields for.
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
        // A delta, monotonic `Sum` is what `MetricKind::Counter` used to mean -- encodes exactly
        // as it did, `name:v|c`. Any other `Sum` (cumulative, or non-monotonic) has no `|c`
        // meaning statsd can represent and falls through to the unsupported-kind arm below.
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
            // `-0.0` is numerically zero, and `0` *is* representable as an absolute gauge -- but
            // `f64`'s `Display` renders it `"-0"`, and `StatsdDecoder::build_event`'s `"g"` arm
            // decides `Gauge` vs `GaugeDelta` on the leading `-` alone, without parsing the float
            // first, so the naive rendering would decode back as a no-op `GaugeDelta(-0.0)`
            // instead of an absolute reset to zero. Normalizing the sign away here emits the
            // plain `name:0|g` that says exactly that, and keeps the two-line idiom below for
            // values that really are negative. `stdio_out`'s
            // `gauge_delta_negative_zero_does_not_double_the_sign` is the same
            // `Display`-of-negative-zero trap in that sink.
            let v = if *v == 0.0 { 0.0 } else { *v };
            line.clear();
            if v.is_sign_negative() {
                // No wire syntax for a negative absolute gauge -- emit the documented two-line
                // idiom as one indivisible entry (module doc's "Negative absolute gauges").
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
        // Raw, unsummarized data (statsd's own `ms`/`h`/`d`/`s` shapes, decoded losslessly by
        // `statsd_in` -- `docs/adr/lossless-transit.md`) -- real encoding, not a drop.
        MetricKind::Samples(samples) => render_samples(line, name.as_str(), samples, ctx),
        MetricKind::SetMembers(members) => {
            render_set_members(line, member, name.as_str(), members, ctx)
        }
    }
}

/// Shared by every `MetricKind` arm `render_metric` can't encode -- counts the drop and logs a
/// throttled warning naming exactly which kind was unencodable. Extracted so the match above can
/// stay one arm per variant (fully exhaustive, no wildcard) without repeating these lines per arm
/// -- the exhaustiveness itself is the point: a future `MetricKind` variant is a compile error
/// here, not a silent `unreachable!` panic at runtime. `hint`, when given, points at the
/// `aggregate` config that would let this kind's *raw* form relay through this sink instead
/// (`Distribution`/`Set` only -- the other unsupported kinds have no raw statsd counterpart at
/// all, so no such hint applies to them).
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

/// Renders one `name:v|g` (or `name:+v|g`/`name:-v|g`) line into `line`, including the tag segment
/// and, under `Format::DogStatsd`, `|c:`/`|T` -- shared by `Gauge`'s plain and negative-pair cases.
fn write_gauge_line(line: &mut String, name: &str, v: f64, ctx: &mut EncodeCtx) {
    line.push_str(name);
    line.push(':');
    push_float(line, v);
    line.push_str("|g");
    append_tags(line, ctx.tag_suffix);
    append_dialect_extras(line, ctx);
}

/// `ctx.statsd_type`'s value when it names one of the timer wire types, else the default `"ms"` --
/// see the module doc's "`|c:<container-id>` and `|T<timestamp>`" section for the sibling carriers
/// this one is captured alongside. Reads `EncodeCtx`, not `event.attributes`, for the same reason
/// [`append_dialect_extras`] does.
fn statsd_wire_type(ctx: &EncodeCtx) -> &'static str {
    match ctx.statsd_type {
        Some("ms") => "ms",
        Some("h") => "h",
        Some("d") => "d",
        _ => "ms",
    }
}

/// Encodes a `Samples` record (statsd's raw `ms`/`h`/`d` observations). Under `Format::DogStatsd`,
/// every value shares one multi-value line (`name:v1:v2:...|<type>[|@rate]|#tags...`) -- the
/// DogStatsD grammar's own multi-value extension, the same one `logit_inputs::statsd` parses on
/// the way in. Under `Format::Statsd`, which has no such extension, each value becomes its own
/// `name:v|ms[|@rate]` line (`h`/`d` normalize to `ms`, counted once per record via
/// `EncodeStats::type_normalized_dialect`). A non-finite value is dropped and counted per value,
/// not written as the text "NaN"/"inf"; an out-of-range `sample_rate` (not `1.0`, and not finite
/// and in `(0, 1]`) omits `@rate` rather than writing bad wire text, counted once per record; an
/// empty (or now-empty, every value non-finite) `values` list emits nothing, counted once.
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
                // Every value was non-finite -- nothing left to encode; each was already counted
                // above, so this isn't the "empty to begin with" case and needs no extra count.
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
                // `ctx.tag_suffix` is already empty under `Format::Statsd` (`build_tag_suffix`),
                // so this is a no-op -- kept for symmetry with the `DogStatsd` arm above.
                append_tags(line, ctx.tag_suffix);
                append_dialect_extras(line, ctx);
                push_line(line, ctx.max_packet_bytes, ctx.stats, ctx.diag, ctx.out);
            }
        }
    }
}

/// Encodes a `SetMembers` record (statsd's raw `s` set-membership observations) as one
/// `name:<member>|s` line per member, in both dialects -- the classic grammar has no multi-value
/// extension for sets the way DogStatsD's timers get. See the module doc's "Sanitization" section
/// for the member-rendering rule. An empty member list emits nothing, counted once.
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
        // `is_forbidden_in_set_member`, not the name/tag-key or tag-value rule -- a member sits in
        // `name:<member>|s`'s colon-separated *value* position, the same position `parse_line`
        // splits on to find several values on one line (`name:v1:v2|c`), so `:` must still be
        // forbidden here (unlike the tag-value rule, which allows it) or a member containing `:`
        // would silently re-decode as two members instead of one. Unlike the name/tag-key rule,
        // `@`/`#`/`,`/whitespace are all preserved: none of them is delimiter-sensitive in this
        // position, so sanitizing them would only make distinct members collide.
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

/// Appends `s` to `out` (does **not** clear it first -- callers that want a fresh buffer clear
/// explicitly), replacing every character `forbidden` rejects with `_`. Substitution, not
/// deletion, so distinct inputs stay distinct.
fn sanitize_into(out: &mut String, s: &str, forbidden: impl Fn(char) -> bool) {
    for c in s.chars() {
        out.push(if forbidden(c) { '_' } else { c });
    }
}

/// Forbidden in a metric name -- see the module doc's "Sanitization" section for why each
/// character earns its place. Also the tag-*key* rule: `:` is forbidden there too, because
/// `parse_line` splits a tag on its first colon, so a `:` in a key would silently reparse as a
/// shorter key with the remainder folded into the value.
fn is_forbidden_in_name(c: char) -> bool {
    matches!(c, ':' | '|' | '@' | '#' | ',' | '\n' | '\r' | '\0')
        || c.is_control()
        || c.is_whitespace()
}

/// Alias for [`is_forbidden_in_name`], named for its other use site (tag keys) -- see that
/// function's doc comment.
fn is_forbidden_in_tag_key(c: char) -> bool {
    is_forbidden_in_name(c)
}

/// Tag *values* forbid the same set as a name **except `:`**, which is deliberately preserved:
/// since `parse_line` only looks at the first colon, `env:a:b` round-trips as key `env`, value
/// `a:b`.
fn is_forbidden_in_tag_value_only(c: char) -> bool {
    c != ':' && is_forbidden_in_name(c)
}

/// Forbidden in a `SetMembers` member -- its own, narrower rule, sharing nothing with
/// [`is_forbidden_in_name`]/[`is_forbidden_in_tag_value_only`] beyond the two characters that
/// genuinely matter here. A member sits in `name:<member>|s`'s colon-separated *value* position,
/// the same position `parse_line` splits on to find several values sharing one line
/// (`name:v1:v2|c`), so `:` has to be forbidden or it would silently re-decode a single member as
/// two; `|` would open a new segment instead of staying part of the value. Control bytes
/// (`\n`/`\r`/`\0` included, via `char::is_control`) are forbidden too, since any of them would
/// corrupt line/datagram framing regardless of position. Nothing else is: `@`, `#`, `,`, and
/// whitespace are only delimiter-sensitive at the *start* of a segment, a role a member never
/// plays, so forbidding them here would sanitize bytes a real decode can hand back unchanged and
/// let distinct members collide (module doc's "Sanitization" section).
fn is_forbidden_in_set_member(c: char) -> bool {
    matches!(c, ':' | '|') || c.is_control()
}

/// Forbidden in an event title -- control bytes only ([`char::is_control`], which covers
/// `\n`/`\r`/`\0`). A bare `|` is fine: `parse_event` slices `TITLE`/`TEXT` by the `_e{tlen,xlen}`
/// header's own byte lengths, not by scanning for `|`, so nothing here is delimiter-sensitive the
/// way a name/tag-key/member is. Control bytes still have to go: an embedded raw `\n` would look
/// like a line boundary to a receiver once several statsd lines get packed into one `\n`-joined
/// UDP datagram (module doc's "Packing and framing" section), which the byte-length header alone
/// doesn't protect against.
fn is_forbidden_in_event_title(c: char) -> bool {
    c.is_control()
}

/// Appends `raw` to `out` as DogStatsD event `TEXT`: a real newline becomes the two-byte escape
/// `\n` (the wire encoding `statsd_in`'s `unescape_event_text` turns back into a real newline on
/// decode), and any other control byte is substituted with `_` -- there is no escape for those,
/// and, same reasoning as [`is_forbidden_in_event_title`], an unescaped one could corrupt a
/// packed datagram's line framing.
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

/// Forbidden in an event's `host`/`aggregation_key`/`source_type` field and a service check's
/// `name`/`host` field -- `|` and control bytes, substituted with `_`. Each of these sits in a
/// `|letter:value` field a real decode reads up to the *next* `|` (or, for a service check's
/// `name`, up to the second `|` on the line -- `parse_service_check`'s own `splitn(3, '|')`), so
/// an embedded `|` would truncate the field and leak its remainder into whatever comes next --
/// unlike a tag value, none of these fields has a colon-splitting ambiguity that would need `:`
/// forbidden too. Deliberately **not** [`is_forbidden_in_name`]: a service check name keeps `.`
/// and spaces exactly as sent (module doc), which the name/tag-key rule would substitute.
fn is_forbidden_in_extended_field(c: char) -> bool {
    c == '|' || c.is_control()
}

/// Forbidden in a service check's `message` (`m:`) field -- control bytes only, substituted with
/// `_`. Unlike event text, a real newline is *not* escaped here (there is no equivalent unescape
/// on the decode side for this field): both are simply substituted, the same defensive reasoning
/// as [`is_forbidden_in_event_title`]. `|` is deliberately left alone: `m:` is always the last
/// field on the line (this module always renders it last, and `parse_service_check` always reads
/// it last), so an embedded `|` can't be misread as the start of another field.
fn is_forbidden_in_service_check_message(c: char) -> bool {
    c.is_control()
}

/// The live half of a `statsd_out` sink: `Udp` binds eagerly (a bad local bind is a config error);
/// `Tcp` connects lazily inside `send`, since a not-yet-up downstream receiver must not block
/// `logit` from starting. Mirrors `syslog::Conn` exactly.
enum Conn {
    Udp(UdpSocket),
    Tcp { stream: Option<TcpStream>, connect_timeout: Duration },
}

/// `logit_pipeline::Output` for `statsd_out`. Built via [`StatsdOutput::udp`] or
/// [`StatsdOutput::tcp`] -- never a bare constructor, mirroring `SyslogOutput`.
pub struct StatsdOutput {
    endpoint: String,
    conn: Conn,
    encoder: StatsdEncoder,
    max_packet_bytes: usize,
    lines: MessageBuf,
    /// Reused across `send` calls: the packed UDP datagram, or the whole TCP frame.
    packet_buf: Vec<u8>,
    diag: Diagnostics,
    telemetry: Telemetry,
}

impl StatsdOutput {
    /// Binds an ephemeral local UDP socket eagerly -- see `SyslogOutput::udp`'s doc comment for
    /// why `endpoint` itself is resolved per `send`, not here.
    pub fn udp(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        let socket = std::net::UdpSocket::bind("0.0.0.0:0")
            .context("binding statsd_out's local UDP socket")?;
        socket.set_nonblocking(true).context("configuring statsd_out's UDP socket")?;
        let socket = UdpSocket::from_std(socket).context("registering statsd_out's UDP socket")?;
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
            encoder: StatsdEncoder::new(Format::DogStatsd),
            max_packet_bytes: DEFAULT_MAX_PACKET_BYTES,
            lines: MessageBuf::default(),
            packet_buf: Vec::new(),
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
        }
    }

    pub fn with_encoder(mut self, encoder: StatsdEncoder) -> Self {
        self.encoder = encoder;
        self
    }

    pub fn with_max_packet_bytes(mut self, max_packet_bytes: usize) -> Self {
        self.max_packet_bytes = max_packet_bytes;
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
impl Output for StatsdOutput {
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        // TCP has no datagram to overflow -- only the per-line oversize drop applies there.
        let cap =
            if matches!(self.conn, Conn::Udp(_)) { self.max_packet_bytes } else { usize::MAX };
        let stats = self.encoder.encode_into(batch, cap, &mut self.lines);
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
            Conn::Tcp { stream, connect_timeout } => {
                Self::send_tcp(
                    stream,
                    &self.endpoint,
                    *connect_timeout,
                    &self.lines,
                    &mut self.packet_buf,
                )
                .await
            }
        };
        drop(request_timer);

        match &result {
            Ok((messages, datagrams)) => {
                self.telemetry.count("logit.output.messages", *messages as f64, &[]);
                if matches!(self.conn, Conn::Udp(_)) {
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

    /// Implemented explicitly for the same reason `syslog_out` does: `send` performs one write per
    /// batch and retains nothing between calls, so there's nothing buffered here at shutdown --
    /// for TCP, this simply flushes the underlying stream.
    async fn flush(&mut self) -> anyhow::Result<()> {
        if let Conn::Tcp { stream: Some(stream), .. } = &mut self.conn {
            stream.flush().await.context("flushing statsd_out TCP stream")?;
        }
        Ok(())
    }

    /// `false`: a redelivered `hits:5|c` **increments the destination counter a second time**,
    /// silently corrupting the value with no trace at the receiver -- a stronger reason than
    /// `syslog_out`'s (which only duplicates a log line). Under the derived `AtMostOnce` posture
    /// this still lets a `Fault::Clean` retry succeed, covering the common receiver-restart
    /// outage with zero duplicate risk.
    fn duplicate_safe(&self) -> bool {
        false
    }
}

/// Running totals for one [`StatsdOutput::send_udp`] call. `entries_in_packet` is the count that
/// cannot be recovered from `packet_buf`'s bytes: a negative-absolute-gauge pair is **one**
/// [`MessageBuf`] entry containing an embedded `\n` (module doc's "Negative absolute gauges"), so
/// counting `\n` bytes in a packed datagram would report it as two messages over UDP where
/// `send_tcp` (and `syslog_out`, on both transports) reports one.
#[derive(Default)]
struct UdpSendCounts {
    /// [`MessageBuf`] entries actually written to the socket -- `logit.output.messages`.
    messages: usize,
    /// Datagrams actually written to the socket -- `logit.output.datagrams`.
    datagrams: usize,
    /// Entries appended to `packet_buf` since the last flush; reset by every flush.
    entries_in_packet: usize,
}

impl StatsdOutput {
    /// Packs `lines` into as few UDP datagrams as fit under `max_packet_bytes` (newline-joined, no
    /// trailing newline), then sends one `send_to` per datagram. See the module doc's "Packing and
    /// framing" section for why packing is correct here where `syslog_out` refuses it. A single
    /// line already longer than `max_packet_bytes` was dropped by the encoder, so every line seen
    /// here fits in its own datagram at minimum. Returns `(messages sent, datagrams sent)`.
    async fn send_udp(
        socket: &UdpSocket,
        endpoint: &str,
        lines: &MessageBuf,
        max_packet_bytes: usize,
        packet_buf: &mut Vec<u8>,
        diag: &mut Diagnostics,
        telemetry: &Telemetry,
    ) -> anyhow::Result<(usize, usize)> {
        // Resolved once per batch, not once per datagram -- see `syslog::send_udp`'s doc comment
        // for why a non-numeric host must not be re-resolved on every call.
        let mut addrs = lookup_host(endpoint)
            .await
            .context("resolving statsd_out endpoint")
            .context(Fault::Clean)?;
        let addr = addrs
            .next()
            .context("statsd_out endpoint resolved to no addresses")
            .context(Fault::Clean)?;

        let mut counts = UdpSendCounts::default();
        packet_buf.clear();
        for msg in lines.iter() {
            let needs_sep = !packet_buf.is_empty();
            let extra = msg.len() + usize::from(needs_sep);
            if !packet_buf.is_empty() && packet_buf.len() + extra > max_packet_bytes {
                Self::flush_datagram(socket, addr, packet_buf, &mut counts, diag, telemetry)
                    .await?;
            }
            if needs_sep && !packet_buf.is_empty() {
                packet_buf.push(b'\n');
            }
            packet_buf.extend_from_slice(msg);
            counts.entries_in_packet += 1;
        }
        if !packet_buf.is_empty() {
            Self::flush_datagram(socket, addr, packet_buf, &mut counts, diag, telemetry).await?;
        }
        Ok((counts.messages, counts.datagrams))
    }

    /// Sends one packed datagram, clearing `packet_buf` and `counts.entries_in_packet` after.
    /// Counts the datagram's [`MessageBuf`] **entries** -- not its `\n` bytes -- toward
    /// `counts.messages`: one entry may itself be a negative-gauge pair (two statsd lines joined
    /// by an embedded `\n`, module doc's "Negative absolute gauges"), and that is still one unit
    /// of "messages", the same convention [`Self::send_tcp`] (`lines.len()`) and `syslog_out`
    /// count by on both transports. The same count is what an oversize drop reports under
    /// `logit.output.messages.dropped{reason="oversize_datagram"}`.
    async fn flush_datagram(
        socket: &UdpSocket,
        addr: std::net::SocketAddr,
        packet_buf: &mut Vec<u8>,
        counts: &mut UdpSendCounts,
        diag: &mut Diagnostics,
        telemetry: &Telemetry,
    ) -> anyhow::Result<()> {
        match socket.send_to(packet_buf, addr).await {
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

    /// One newline-terminated frame (every line, including the last) per **batch**, written with
    /// at most one internal reconnect-and-retry -- a near-verbatim port of `syslog::send_tcp`,
    /// including both correctness properties documented on that function (cancellation safety via
    /// `stream.take()`, and never resending once a byte has left this host). Returns `(messages
    /// sent, 0)` -- there's no datagram count on TCP.
    async fn send_tcp(
        stream: &mut Option<TcpStream>,
        endpoint: &str,
        connect_timeout: Duration,
        lines: &MessageBuf,
        frame_buf: &mut Vec<u8>,
    ) -> anyhow::Result<(usize, usize)> {
        frame_buf.clear();
        for msg in lines.iter() {
            frame_buf.extend_from_slice(msg);
            frame_buf.push(b'\n');
        }

        let mut retried_after_a_zero_byte_failure = false;
        loop {
            let mut conn = match stream.take() {
                Some(conn) => conn,
                None => tokio::time::timeout(connect_timeout, TcpStream::connect(endpoint))
                    .await
                    .context("connecting to statsd_out endpoint timed out")
                    .and_then(|r| r.context("connecting to statsd_out endpoint"))
                    .context(Fault::Clean)?,
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
                    return match rest_result {
                        Ok(()) => {
                            *stream = Some(conn);
                            Ok((lines.len(), 0))
                        }
                        Err(err) => Err(anyhow::Error::new(err).context(Fault::Ambiguous)),
                    };
                }
                Err(_) if !retried_after_a_zero_byte_failure => {
                    retried_after_a_zero_byte_failure = true;
                    continue;
                }
                Err(err) => return Err(anyhow::Error::new(err).context(Fault::Clean)),
            }
        }
    }
}

/// `90` is `EMSGSIZE` on Linux specifically -- see `syslog::is_message_too_large`'s doc comment;
/// this repo only ever ships/runs inside the Linux containers it builds.
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
        let stats = encoder.encode_into(&batch_with(events), usize::MAX, &mut out);
        let msgs = out.iter().map(|b| String::from_utf8_lossy(b).into_owned()).collect();
        (msgs, stats)
    }

    fn encode_with(encoder: &mut StatsdEncoder, events: Vec<Event>) -> (Vec<String>, EncodeStats) {
        let mut out = MessageBuf::default();
        let stats = encoder.encode_into(&batch_with(events), usize::MAX, &mut out);
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

    /// A plain log event (no `statsd.event.title`, so not a DogStatsD event) with ordinary
    /// attributes must not pay the merged tag walk at all under `Format::Statsd` -- it's
    /// `skipped_no_metrics`, full stop, not also `tags_dropped_dialect` for tags nothing was
    /// ever going to render. Regression for a `syslog_in -> statsd_out` relay inflating that
    /// counter for every ordinary log line.
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
        // Substitution never produces an empty result except from an already-empty input --
        // every forbidden character becomes `_`, not nothing (`:` alone sanitizes to `"_"`, a
        // perfectly good one-character name, not a drop).
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

    /// A `NO_RECORDED_VALUE`-flagged point must be dropped and counted, not written as a
    /// fabricated `name:0|g` -- fix 3 in PR #123's review (`docs/adr/lossless-transit.md`,
    /// `docs/known-gaps.md`'s cross-protocol table).
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

    /// `ExponentialHistogram` (raw or lossless data with no statsd wire shape at all) falls through
    /// to the unsupported-kind drop path -- not a panic, and not silently dropped uncounted.
    /// `Samples`/`SetMembers` are real encoded kinds now (W3): see the dedicated `-- Samples --`/
    /// `-- SetMembers --` sections below for their coverage.
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

    /// A cumulative (or non-monotonic) `Sum` has no `|c` statsd can represent and is dropped, same
    /// as any other unsupported kind -- only a delta, monotonic `Sum` still encodes as `|c`.
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

    /// A member containing `:` must be sanitized, not preserved: `:` is the multi-value separator
    /// in a member's own wire position (`name:<member>|s`), so an unsanitized `a:b` would render
    /// `tags:a:b|s` and `StatsdDecoder::parse_line` would split that into two members (`a`, `b`)
    /// instead of decoding the original one member back.
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

    /// The real-decoder regression for the same case: a colon-sanitized member re-decodes as
    /// exactly one member, never two.
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

    /// Bytes the name/tag-key rule would substitute but a member's own, narrower rule preserves --
    /// `@`, `#`, `,`, and a space are none of them delimiter-sensitive in a member's colon-
    /// separated *value* position (`values_part` only ever splits on `:`; the line only on
    /// `|`/newline), so a real decode hands each straight through and this sink must round-trip
    /// it byte for byte, not substitute it the way it would in a name or tag key.
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
            // Compares metrics only, not the whole `Event`: both decodes carry a real receipt-time
            // `Event::timestamp` (no `|T` on this line), which two independent `decode_one` calls
            // never agree on down to the nanosecond -- `statsd_round_trip.rs`'s own
            // `normalize_receipt_time` exists for exactly this reason.
            assert_eq!(
                relayed[0].metrics, original[0].metrics,
                "decode(encode(decode(line))) should equal decode(line) for {line:?}"
            );
        }
    }

    /// `:` is still forbidden in a member (unlike in a tag value): a real `users:a:b|s` decode
    /// produces *two* members (`a`, `b`), and each one still re-encodes as its own untouched line
    /// -- the colon is what changes structure here, not any per-member sanitization.
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

    /// The resource-level counterpart: `build_tag_suffix`'s merged walk captures a `statsd.*`
    /// carrier from *either* side of the resource⊕event merge, event winning on collision (same
    /// as every other attribute) -- so a carrier a `set` transform's `resource:` block stamped
    /// reaches the wire exactly like one `statsd_in` stamped directly on the event.
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
        encoder.encode_into(&batch, usize::MAX, &mut out);
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

    /// `statsd.timestamp` present but not a `Value::U64` -- never produced by `statsd_in` itself,
    /// but reachable from a cross-protocol relay or a Lua-authored attribute -- is simply not
    /// emitted, since `EncodeCtx::timestamp_secs` only ever gets set from the `U64` arm of
    /// `build_tag_suffix`'s merged walk. This is also the regression for the old
    /// `Value::Bool(true)` marker representation this carrier used before it held the wire value
    /// itself: it must not be misread as a timestamp of any kind.
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
        // `statsd.container_id`/`statsd.timestamp` still show up, but only via their own dedicated
        // segments -- never through the generic `|#k:v,...` tag segment alongside `env`.
        assert_eq!(msgs, vec!["hits:1|c|#env:prod|c:abcd1234|T0"]);
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
        // "héllo" is 6 UTF-8 bytes but 5 chars -- proves `tlen` counts bytes, not chars.
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
        // A dropped *field* on an otherwise-emitted line, not a dropped message -- its own
        // counter, not `dropped_unencodable_value` (which would claim the whole event was
        // dropped).
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
        encoder.encode_into(&batch, usize::MAX, &mut out);
        let msgs: Vec<String> =
            out.iter().map(|b| std::str::from_utf8(b).unwrap().to_string()).collect();
        assert_eq!(msgs, vec!["_e{13,1}:from-resource|x"]);
    }

    #[test]
    fn an_oversize_event_line_is_dropped_via_the_existing_oversize_path() {
        let event = event_line_event("t", "x", &[]);
        let mut encoder = StatsdEncoder::new(Format::DogStatsd);
        let mut out = MessageBuf::default();
        // "_e{1,1}:t|x" is 11 bytes, longer than this cap.
        let stats = encoder.encode_into(&batch_with(vec![event]), 5, &mut out);
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

    /// Unlike a plain log line (which is `skipped_no_metrics` without ever touching the merged
    /// walk -- see `a_plain_log_event_under_statsd_is_skipped_without_touching_tags_dropped_
    /// dialect` above), a *real* event dropped under `Format::Statsd` did have its full merged
    /// walk run (that's how it was confirmed to be an event at all), so its ordinary attributes
    /// are also tallied into `tags_dropped_dialect` by that walk -- both counters incrementing
    /// for the one dropped line is expected, not a double-count bug (module doc).
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
        let mut encoder = StatsdEncoder::new(Format::DogStatsd);
        let mut out = MessageBuf::default();
        let stats = encoder.encode_into(
            &batch_with(vec![metric_event("hits", MetricKind::counter(1.0), &[])]),
            3, // "hits:1|c" is longer than this
            &mut out,
        );
        assert!(out.is_empty());
        assert_eq!(stats.dropped_oversize_line, 1);
    }

    #[test]
    fn a_negative_absolute_gauges_two_lines_are_never_split_across_datagrams() {
        // The pair is pushed as one MessageBuf entry (an embedded '\n'), so the packer can never
        // split it -- verified indirectly: it either fits whole in a datagram, or the *whole* pair
        // is dropped as one oversize line, never half of it.
        let mut encoder = StatsdEncoder::new(Format::DogStatsd);
        let mut out = MessageBuf::default();
        let stats = encoder.encode_into(
            &batch_with(vec![metric_event("free", MetricKind::Gauge(-5.0), &[])]),
            usize::MAX,
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

    /// The identical batch must report the identical `logit.output.messages` on both transports.
    /// A negative-absolute-gauge pair is one `MessageBuf` entry holding two statsd lines; UDP
    /// used to count its embedded `\n` as a second message where TCP counted the entry.
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

    #[test]
    fn a_packed_multi_line_datagram_round_trips_as_several_events() {
        let mut encoder = StatsdEncoder::new(Format::DogStatsd);
        let mut out = MessageBuf::default();
        encoder.encode_into(
            &batch_with(vec![
                metric_event("a", MetricKind::counter(1.0), &[]),
                metric_event("b", MetricKind::counter(2.0), &[]),
            ]),
            usize::MAX,
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
        // The single MessageBuf entry contains two lines; decode both, in order.
        let events = decode_one(&msgs[0]);
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0].metrics[0].kind, MetricKind::Gauge(v) if v == 0.0));
        assert!(matches!(events[1].metrics[0].kind, MetricKind::GaugeDelta(v) if v == -5.0));
        // Applied in order against a starting gauge of 0, this reaches -5 -- the value we encoded.
    }

    /// `f64`'s `Display` renders `-0.0` as `"-0"`, and the decoder's `"g"` arm dispatches on the
    /// leading `-` without parsing the value -- so a naive rendering would come back as a no-op
    /// `GaugeDelta`, not the absolute reset to zero it was. Regression for that.
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

    /// A timer line round-trips byte-for-byte under dogstatsd: multi-value, `@rate`, and tags all
    /// survive `statsd_in -> statsd_out` with no `aggregate` in between (W3's real `Samples`
    /// encoding, not the v1 unsupported-kind drop).
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

    /// A set line round-trips byte-for-byte under dogstatsd -- multiple members, one line in, one
    /// line out, same members in the same order.
    #[test]
    fn a_set_line_round_trips_byte_for_byte_through_the_real_statsd_decoder() {
        let original_line = "unique.visitors:alice:bob|s";
        let original = decode_one(original_line);
        assert_eq!(original.len(), 1, "one SetMembers event per line");
        let (msgs, _) = encode(original);
        // The classic multi-value `:`-joined form is DogStatsD's own decoder-side grammar for
        // `s`, but this sink emits one line per member (module doc's "Sanitization" section) --
        // so the round trip is two lines decoding back to two events, each a one-member set,
        // rather than one two-member line. Both members still survive, in order.
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

    /// A `|c:`/`|T` line round-trips through the real decoder under dogstatsd: the container id
    /// and timestamp marker both survive, and the timestamp lands on `Event::timestamp`.
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

    /// An event line round-trips through the real decoder: title/text, every optional field, and
    /// a tag all survive `statsd_in -> statsd_out` with no `aggregate` in between.
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

    /// A service check line round-trips through the real decoder: name, status, every optional
    /// field, and a `|`-containing message all survive.
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
    // `docs/plans/lossless-transit.md`'s W3 fixed-point requirement: for a small DogStatsD
    // grammar generator, `decode(encode(decode(line))) == decode(line)` (whole `EventBatch`,
    // receipt timestamps normalized when the line carries no `|T`) and `encode(decode(line))` is
    // a fixed point of `encode . decode`. Mirrors `syslog.rs`'s `mod fixed_point` exactly.
    mod fixed_point {
        use super::*;
        use logit_core::EventBatch;
        use proptest::prelude::*;

        fn metric_name() -> impl Strategy<Value = String> {
            "[a-zA-Z][a-zA-Z0-9_.]{0,12}"
        }

        /// 1-3 values -- for `c`/`ms`/`h`/`d`, plain decimal numbers; for `g`, a leading `-`/`+`
        /// exercises the delta path too; for `s`, short member tokens with no `:`/`|` (or any
        /// other genuinely forbidden character) in them, so every generated line already has its
        /// values unambiguously delimited at the *grammar* level -- a member sanitized because it
        /// embedded one of those is the dedicated `a_member_containing_a_colon_is_sanitized_and_
        /// counted`/`..._re_decodes_as_one_member_not_two` unit tests' job, not this generic
        /// property's. `@`/`#`/`,`/a space are deliberately included, though: none of them is
        /// delimiter-sensitive in a member's own wire position (`is_forbidden_in_set_member`
        /// preserves all four), so a real decode can hand any of them back unchanged and this
        /// property should cover that, not just the alphanumeric case. Paired with its own
        /// wire-type letter via `prop_flat_map` below so the shape always matches the type (a
        /// top-level proptest parameter can't depend on another one).
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

        fn tag() -> impl Strategy<Value = (String, String)> {
            ("[a-z][a-z0-9]{0,6}", "[a-z][a-z0-9]{0,6}")
        }

        fn tags() -> impl Strategy<Value = Vec<(String, String)>> {
            prop::collection::vec(tag(), 0..=2)
        }

        fn opt_container_id() -> impl Strategy<Value = Option<String>> {
            prop_oneof![Just(None), "[a-z0-9]{4,12}".prop_map(Some)]
        }

        fn opt_secs() -> impl Strategy<Value = Option<u32>> {
            prop_oneof![Just(None), (1u32..2_000_000_000).prop_map(Some)]
        }

        /// Renders one syntactically valid line from the generated pieces -- deliberately
        /// independent of `StatsdEncoder` (the thing under test).
        #[allow(clippy::too_many_arguments)]
        fn render_line(
            name: &str,
            kind: &str,
            values: &[String],
            rate: Option<u32>,
            tags: &[(String, String)],
            container_id: &Option<String>,
            secs: Option<u32>,
        ) -> String {
            let mut line = format!("{name}:{}|{kind}", values.join(":"));
            if let Some(r) = rate {
                let _ = write!(line, "|@{:.2}", f64::from(r) / 100.0);
            }
            if !tags.is_empty() {
                line.push_str("|#");
                line.push_str(
                    &tags.iter().map(|(k, v)| format!("{k}:{v}")).collect::<Vec<_>>().join(","),
                );
            }
            if let Some(id) = container_id {
                let _ = write!(line, "|c:{id}");
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

        /// A small piece of an event `TITLE`/`TEXT`: plain ASCII, `|`/`:` (both legal in this
        /// position -- the `_e{tlen,xlen}` header delimits by byte length, not by scanning for
        /// either), the literal two-byte escape sequence `\n` (the wire's own encoding of a real
        /// newline -- `parse_event`'s `unescape_event_text` turns it back into one on decode, and
        /// `render_event`'s `append_event_text` turns a real one back into this on re-encode), and
        /// a multi-byte UTF-8 character, so `tlen`/`xlen` byte-counting is exercised too.
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

        /// One generated line, of any of the three shapes this fixed point covers. Carries
        /// exactly the pieces its own `render_*` function needs -- kept as one enum, rather than
        /// three separate `proptest!` blocks, so all three shapes exercise the same
        /// decode-encode-decode/encode-is-a-fixed-point body below.
        #[derive(Debug)]
        #[allow(clippy::large_enum_variant)]
        enum GeneratedLine {
            Metric {
                name: String,
                kind: &'static str,
                values: Vec<String>,
                rate: Option<u32>,
                tags: Vec<(String, String)>,
                container_id: Option<String>,
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
                tags: Vec<(String, String)>,
                container_id: Option<String>,
            },
            ServiceCheck {
                name: String,
                status: u32,
                secs: Option<u32>,
                host: Option<String>,
                tags: Vec<(String, String)>,
                container_id: Option<String>,
                message: Option<String>,
            },
        }

        /// Renders a `GeneratedLine::Event` -- field order matches [`render_event`]'s own
        /// canonical order exactly (`d:`,`h:`,`p:`,`t:`,`k:`,`s:`,tags,`c:`): `AttrMap` sorts by
        /// key regardless of insertion order, so this doesn't matter for `d1 == d2` below, but
        /// matching it anyway keeps `e1 == e2` (the literal re-encoded bytes) trivially true
        /// rather than merely equal-after-reordering.
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
            tags: &[(String, String)],
            container_id: &Option<String>,
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
            if !tags.is_empty() {
                line.push_str("|#");
                line.push_str(
                    &tags.iter().map(|(k, v)| format!("{k}:{v}")).collect::<Vec<_>>().join(","),
                );
            }
            if let Some(id) = container_id {
                let _ = write!(line, "|c:{id}");
            }
            line
        }

        /// Renders a `GeneratedLine::ServiceCheck` -- field order matches [`render_service_check`]
        /// exactly (name, status, `d:`, `h:`, tags, `c:`, `m:` last).
        fn render_service_check_line(
            name: &str,
            status: u32,
            secs: Option<u32>,
            host: &Option<String>,
            tags: &[(String, String)],
            container_id: &Option<String>,
            message: &Option<String>,
        ) -> String {
            let mut line = format!("_sc|{name}|{status}");
            if let Some(s) = secs {
                let _ = write!(line, "|d:{s}");
            }
            if let Some(h) = host {
                let _ = write!(line, "|h:{h}");
            }
            if !tags.is_empty() {
                line.push_str("|#");
                line.push_str(
                    &tags.iter().map(|(k, v)| format!("{k}:{v}")).collect::<Vec<_>>().join(","),
                );
            }
            if let Some(id) = container_id {
                let _ = write!(line, "|c:{id}");
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
                opt_container_id(),
            )
                .prop_map(
                    |(
                        title,
                        text,
                        secs,
                        host,
                        priority,
                        alert_type,
                        key,
                        source,
                        tags,
                        container_id,
                    )| {
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
                            container_id,
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
                opt_container_id(),
                // Reuses `event_piece`'s richer alphabet (including `|`) for the message: `m:` is
                // always the last field, so an embedded `|` must survive untouched.
                prop_oneof![Just(None), event_piece().prop_map(Some)],
            )
                .prop_map(|(name, status, secs, host, tags, container_id, message)| {
                    GeneratedLine::ServiceCheck {
                        name,
                        status,
                        secs,
                        host,
                        tags,
                        container_id,
                        message,
                    }
                })
        }

        fn arb_line() -> impl Strategy<Value = GeneratedLine> {
            prop_oneof![
                (
                    metric_name(),
                    kind_and_values(),
                    opt_rate(),
                    tags(),
                    opt_container_id(),
                    opt_secs()
                )
                    .prop_map(
                        |(name, (kind, values), rate, tags, container_id, secs)| {
                            GeneratedLine::Metric {
                                name,
                                kind,
                                values,
                                rate,
                                tags,
                                container_id,
                                secs,
                            }
                        }
                    ),
                event_strategy(),
                service_check_strategy(),
            ]
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(200))]

            #[test]
            fn decode_encode_decode_is_a_fixed_point(generated in arb_line()) {
                let (line, secs, is_set_members) = match &generated {
                    GeneratedLine::Metric { name, kind, values, rate, tags, container_id, secs } => (
                        render_line(name, kind, values, *rate, tags, container_id, *secs),
                        *secs,
                        *kind == "s",
                    ),
                    GeneratedLine::Event {
                        title, text, secs, host, priority, alert_type, key, source, tags, container_id,
                    } => (
                        render_event_line(
                            title, text, *secs, host, *priority, *alert_type, key, source, tags,
                            container_id,
                        ),
                        *secs,
                        false,
                    ),
                    GeneratedLine::ServiceCheck { name, status, secs, host, tags, container_id, message } => (
                        render_service_check_line(name, *status, *secs, host, tags, container_id, message),
                        *secs,
                        false,
                    ),
                };

                let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()));
                let mut d1 = match decoder.decode(bytes::Bytes::from(line.clone())) {
                    Ok(batch) => batch,
                    Err(_) => return Ok(()), // a generated edge case the grammar allows but the
                                              // decoder rejects (e.g. an out-of-range value) --
                                              // not what this property tests.
                };
                if d1.events.is_empty() {
                    return Ok(());
                }

                // `relative_gauges: true` -- otherwise every generated `g` line with a leading
                // sign decodes to an unresolved `GaugeDelta` that the default encoder drops
                // outright (`a_gauge_delta_is_dropped_by_default`), which isn't what this
                // property tests.
                let mut encoder = StatsdEncoder::new(Format::DogStatsd).with_relative_gauges(true);
                let mut out1 = MessageBuf::default();
                encoder.encode_into(&d1, usize::MAX, &mut out1);

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
                    // `SetMembers` splits from one line (one event, N members) into N one-member
                    // lines regardless of dialect (the module doc's "Sanitization" section) -- a
                    // permitted normalization (`docs/adr/lossless-transit.md`'s "splitting a
                    // multi-value statsd line ... into several lines, or the reverse"), so `d1`/
                    // `d2` legitimately differ in event *count* even though every member survives.
                    // Compare the flat multiset of members instead of whole-batch equality.
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
                encoder.encode_into(&d2, usize::MAX, &mut out2);
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
