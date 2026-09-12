//! statsd / DogStatsD-tagged metrics over UDP -- the input side of the v0.1 vertical slice
//! (`docs/OVERVIEW.md`: statsd -> transform -> InfluxDB) and, since W3, the input half of
//! [`docs/adr/lossless-transit.md`]'s `statsd_in -> statsd_out` lossless-relay pair
//! (`docs/plans/lossless-transit.md`'s W3).
//!
//! Grammar (superset covering plain statsd and the DogStatsD tag/container-id/timestamp
//! extensions):
//!
//! ```text
//! <name>:<value>[:<value>...]|<type>[|@<sample-rate>][|#<tag>[:<value>],...][|c:<container-id>][|T<unix-seconds>][|<ignored>]
//! ```
//!
//! The `|#` segment is a comma-separated **list**, not a map -- a key may legally repeat, and a
//! repeat folds into a [`logit_core::Value::Array`] rather than overwriting; see the "DogStatsD
//! tags" section below.
//!
//! `<type>` is one of:
//!
//! - `c` (counter) -- one [`Event`] per value, sample-rate-extrapolated (`value / sample_rate`)
//!   into [`logit_core::MetricKind::Sum`], as always.
//! - `g` (gauge) -- one `Event` per value: unsigned into [`logit_core::MetricKind::Gauge`], a
//!   leading `+`/`-` into an unresolved [`logit_core::MetricKind::GaugeDelta`]
//!   (`docs/adr/relative-gauge-adjustments.md`). Sample rate is ignored -- a gauge value is not a
//!   count to extrapolate.
//! - `ms`/`h`/`d` (timing/histogram/distribution) -- **one `Event` per line, not per value**:
//!   every `:`-separated value on the line lands in one [`logit_core::MetricKind::Samples`],
//!   `sample_rate` carried verbatim, with no extrapolation and no sketching at decode time.
//!   [`docs/adr/lossless-transit.md`]'s "summarization is opt-in and named" rule: only
//!   `aggregate` decides whether/how to turn raw samples into a sketch
//!   (`docs/adr/aggregation-window-semantics.md`'s amendment), never this decoder. This replaces
//!   this module's pre-W3 behaviour of sketching straight into a [`logit_core::DdSketch`] at
//!   decode time (extrapolating to `(1.0 / sample_rate).round()` weighted samples, clamped to a
//!   `MAX_SAMPLE_WEIGHT` of 1000) -- both the sketch and the clamp diagnostic moved to
//!   `aggregate`, which owns `Samples::MAX_WEIGHT`/the `samples_cap_exceeded`-style diagnostics
//!   now. The wire type letter survives as the `statsd.type` attribute (rule (b),
//!   `docs/adr/lossless-transit.md`: a protocol-namespaced carrier for something the model
//!   normalizes) since `ms`/`h`/`d` all land on the same `Samples` shape.
//! - `s` (set) -- **one `Event` per line**: every `:`-separated value on the line lands in one
//!   [`logit_core::MetricKind::SetMembers`], each member a zero-copy `Bytes` slice of the
//!   datagram, in wire order. `aggregate` is the only component that turns these into a real
//!   [`logit_core::HyperLogLog`] estimate ([`logit_core::MetricKind::Set`]); sample rate is
//!   ignored, same reasoning as `g`.
//!
//! Multiple `:`-separated values on a `c`/`g` line share one type/sample-rate/tags and become
//! independent events (gauge sign semantics are per value, so folding them into one event would
//! lose which value was which sign); a `ms`/`h`/`d`/`s` line's values stay together on one event
//! instead, matching the shape statsd itself hands them over in. A datagram may contain multiple
//! newline-separated lines.
//!
//! **`|c:<container-id>` and `|T<unix-seconds>` apply to every metric type here**, not only the
//! `c`/`g` the DogStatsD spec itself restricts them to (`docs/design/telemetry-landscape.md`) --
//! a forward-compatible superset, the same stance this decoder already takes toward unrecognized
//! `|` segments generally. `|c:<id>` (v1.2+; v1.4+'s `ci-`/`in-`-prefixed variants land in the
//! same slot verbatim) stamps `statsd.container_id: Value::Str`, a zero-copy datagram slice.
//! `|T<secs>` sets [`Event::timestamp`] to `secs * 1_000_000_000` (checked -- a non-digit or
//! overflowing value rejects *only that line*, as a `CodecError::Malformed`, leaving the rest of
//! the datagram unaffected) instead of the receipt-time timestamp `decode_into`'s `received_at`
//! would otherwise stamp, and stamps `statsd.timestamp: Value::U64(secs)` -- the raw parsed
//! seconds, not just a marker bit -- so a consumer can both tell a wire-supplied timestamp from a
//! receipt-time one *and* read back the exact wire value, independent of whatever
//! `Event::timestamp` becomes downstream (a summarizing stage like `aggregate` rebuilds
//! `Event::timestamp` at flush time; the carrier attribute is what survives that rebuild
//! unchanged). Both attributes are rule-(b) protocol-namespaced carriers
//! (`docs/adr/lossless-transit.md`) for a concept this model has no normalized field for at all.
//! Every other unrecognized `|` segment is accepted and silently ignored -- forward-compatible
//! with segment kinds this decoder doesn't know about yet, rather than a hard error on something
//! benign.
//!
//! ## DogStatsD tags
//!
//! A `|#` segment is a **list** of `key[:value]` tokens, not a map. The Datadog agent keeps every
//! token and dedupes only *exact* duplicates, so `#team:a,team:b` is two live tags -- a query
//! grouping by `team` places that point in both the `a` and the `b` group -- while `#team:a,team:a`
//! is one. `insert_tags` reproduces exactly that: **a repeated tag key folds into a
//! [`logit_core::Value::Array`] in wire order** (`#team:a,team:b` -> `team: Array[Str("a"),
//! Str("b")]`, three occurrences -> three elements), and **an exact duplicate token is deduped at
//! decode** (`#team:a,team:a` -> `Str("a")`, `#urgent,urgent` -> `Bool(true)`). **A one-element
//! `Array` is never produced**, so a non-repeated tag's decoded shape is byte-identical to what it
//! was before this fold existed. It is the same fold [`crate::syslog`]'s `insert_param` applies to
//! a repeated RFC 5424 PARAM-NAME (`docs/adr/syslog-structured-data-convention.md`), for the same
//! reason: a plain `AttrMap::insert` per token lets the last token win, destroying a value the
//! wire carried inside the decoder, before any sink sees the event -- loss, not a re-spelling,
//! under `docs/adr/lossless-transit.md`.
//!
//! A bare token and a valued one that share a key are not duplicates; **both forms survive, in
//! order**: `#urgent,urgent:1` -> `urgent: Array[Bool(true), Str("1")]` (re-emitted by
//! `statsd_out` as `urgent,urgent:1`) and `#urgent:1,urgent` -> `Array[Str("1"), Bool(true)]` ->
//! `urgent:1,urgent`. Array order is wire order and array-internal; the attribute map itself stays
//! sorted by `Symbol` as always, so nothing about tag *key* order changes. Element values stay
//! zero-copy `slice_of` slices of the datagram, exactly like a scalar tag value.
//!
//! **The fold applies to the `#` segment's payload only.** A repeated `|` *segment* keeps
//! `parse_line`'s pre-existing behaviour, unchanged and out of scope: every `#` segment on a line
//! unions its tokens into the same attribute map (so `|#a:1|#a:2` folds just as a single
//! `|#a:1,a:2` would), while `@`, `|c:` and `|T` are last-segment-wins -- a repeat of one of those
//! simply overwrites what the earlier one stamped. A repeated `|T`/`|c:`/wire-type therefore can't
//! reach `insert_tags` at all. What *can* is a tag **literally named** `statsd.type` (or any other
//! `statsd.*` carrier key) inside the `#` segment: that now decodes to an `Array` where it
//! previously always decoded to a scalar. On egress such a value matches no `statsd_out` carrier
//! arm (each expects a `Value::Str`/`Value::U64`) and is filtered out of the tag segment
//! uncounted, exactly as a wrong-typed carrier already is today. On a `ms`/`h`/`d` line the
//! decoder's own `statsd.type` stamp runs after the tags and overwrites whatever the `#` segment
//! folded there.
//!
//! ## DogStatsD events and service checks
//!
//! Two more line shapes, picked out by their leading sigil rather than the `<name>:<value>|<type>`
//! grammar above at all: `_e{...}:...` (an **event**) and `_sc|...` (a **service check**). The
//! dispatch checks for exactly those two prefixes, `_e{` and `_sc|` -- nothing else about a line
//! starting with `_` is special. A line that merely starts with `_` without matching either
//! (including one whose name is legitimately `_`-prefixed, like `_total.count:1|c`) falls through
//! unchanged into the generic `<name>:<value>|<type>` grammar below, exactly as it did before this
//! section existed: `_` is an ordinary, legal name byte in statsd, and Datadog's own DogStatsD
//! parser special-cases only these same two sigils, nothing broader.
//!
//! **Trailing whitespace is real payload on both shapes, so `decode_into` never trims it off
//! them.** Every line has `\r` and *leading* whitespace trimmed unconditionally (packet padding, a
//! proxy's added indentation, and the like); trailing whitespace is trimmed too, for every line
//! *except* one starting with `_e{` or `_sc|`. `_e{TITLE_LEN,TEXT_LEN}`'s lengths are authoritative
//! for splitting `TITLE`/`TEXT` -- trimming trailing whitespace first would either shrink the line
//! out from under a correct length (rejecting an otherwise-legal event as malformed) or, if the
//! trimmed byte was itself part of `TEXT`, silently change what `TEXT` is. `_sc|`'s `m:` field
//! consumes the rest of the line verbatim, trailing spaces included -- trimming would silently drop
//! them from the decoded message with no error to signal it. See
//! `event_text_ending_in_whitespace_is_kept`/`service_check_message_trailing_whitespace_is_kept`
//! below.
//!
//! **Event** -- `_e{<TITLE_LEN>,<TEXT_LEN>}:<TITLE>|<TEXT>|d:<secs>|h:<hostname>|p:<normal|low>|
//! t:<info|success|warning|error>|k:<aggregation_key>|s:<source_type_name>|#<tags>|
//! c:<container_id>`. `TITLE_LEN`/`TEXT_LEN` are the exact *byte* lengths of `TITLE`/`TEXT` as they
//! sit on the wire and are authoritative for splitting -- not naive `|`-splitting, since `TEXT` may
//! itself contain `|` and `:` -- so a length that runs past the line, a missing `|` right after the
//! title, a length that lands mid-UTF-8-char (checked via `str::get`, never an indexing panic that
//! could), or a malformed `{a,b}` header rejects the line. Decodes to one [`Event::log`]: `message`
//! is `TEXT` with its `\n` (backslash, `n`) two-byte escape unescaped to a real newline (zero-copy
//! when there's nothing to unescape, same stance as everywhere else in this module); `severity`
//! maps `t:error`/`t:warning`/`t:success`/`t:info` to `Error`/`Warn`/`Info`/`Info`, `None` when
//! `t:` is absent, and an unrecognized `t:`/`p:` value rejects the line. `event_name` stays `None`
//! on purpose: an event title is free text an operator or their application chose at send time, not
//! a fixed, bounded vocabulary the way a metric or tag name is -- interning it would grow the
//! global interner without bound. Attributes (all `Value::Str`, zero-copy slices of the datagram
//! where possible): `statsd.event.title` (always), `statsd.event.priority` (`p:`, only if
//! present, raw `normal`/`low`), `statsd.event.alert_type` (`t:`, only if present, raw value),
//! `statsd.event.aggregation_key` (`k:`), `statsd.event.source_type` (`s:`),
//! `statsd.event.host` (`h:`), plus the same `statsd.timestamp`/`statsd.container_id`/`#tags`
//! handling as metric lines below -- `d:<secs>` plays `|T<secs>`'s role here: the same
//! checked-seconds-to-nanoseconds parse, setting both the event's own timestamp and the
//! `statsd.timestamp` carrier. `|T` itself is not part of this grammar; like any other unrecognized
//! field here, it's accepted and ignored, the same forward-compatible stance metric lines take
//! toward an unrecognized `|` segment.
//!
//! **Service check** -- `_sc|<NAME>|<STATUS>|d:<secs>|h:<hostname>|#<tags>|c:<container_id>|
//! m:<message>`. `NAME` must be non-empty; `STATUS` an integer `0..=3` (OK/WARNING/CRITICAL/
//! UNKNOWN) -- anything else rejects the line. `m:`, when present, is always the *last* field and
//! consumes the rest of the line verbatim, so a message may itself contain `|`; every other field
//! may come in any order before it. Decodes to one [`Event::metric`], `MetricKind::Gauge(status as
//! f64)` under the check's own name (`intern`ed, like a metric name). Attributes:
//! `statsd.service_check.name` (always, `Value::Str` -- the raw carrier, rule (b): a service
//! check's name has nowhere else on `MetricRecord` to land), `statsd.service_check.status`
//! (always, `Value::U64`), `statsd.service_check.message` (`m:`, only if present, verbatim
//! including any `|`), `statsd.service_check.host` (`h:`, only if present), plus the same
//! `statsd.timestamp`/`statsd.container_id`/`#tags` handling as events and metric lines.
//!
//! **DogStatsD tag values, `|c:<id>`, and `s`'s set members are all zero-copy slices of the
//! datagram**, exactly like every field [`crate::syslog`] extracts: `slice_of` reconstructs each
//! one's `Bytes` by pointer arithmetic back into the datagram passed to
//! [`StatsdDecoder::decode`], rather than going through `impl From<&str> for Value`
//! (`Bytes::from(String)`, a fresh copy). Tag *keys* and the metric name don't need this
//! treatment -- both only ever reach [`logit_core::interner::intern`], which hashes/copies into
//! its own table regardless of where the `&str` it's given points.

use crate::udp::{UdpListener, UdpListenerConfig};
use crate::Input;
use bytes::Bytes;
use logit_core::{
    interner::intern, AttrMap, BodyFormat, Diagnostics, Event, LogRecord, MetricKind, MetricRecord,
    Resource, Samples, Scope, Severity, Telemetry, Value,
};
use logit_pipeline::Fanout;
use logit_proto::{CodecError, Decoder};
use std::sync::Arc;
use tokio::sync::watch;

/// Thin wrapper over [`UdpListener<StatsdDecoder>`] -- the read/decode split and datagram-\>batch
/// assembly all live there (`docs/adr/decoupled-listener-io.md`); this type is just the
/// decoder choice plus the public constructor/builder surface `logit-cli::pipeline` and this
/// module's own tests already depend on.
pub struct StatsdInput {
    inner: UdpListener<StatsdDecoder>,
}

impl StatsdInput {
    pub fn new(bind: impl Into<String>) -> Self {
        Self {
            inner: UdpListener::new(
                bind,
                StatsdDecoder::new(Arc::new(Resource::default())),
                UdpListenerConfig::default(),
            ),
        }
    }

    /// Attaches a component id to this listener's diagnostics -- and to the [`StatsdDecoder`] it
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
    /// bytes actually arrived on the wire, which `Fanout`-level `events.sent` can't tell apart
    /// from a single busy client.
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

    /// The currently-configured receive-queue/batching/shutdown-grace knobs -- for test
    /// introspection (`logit-cli::pipeline`'s `build_spec` wiring tests).
    pub fn receive_config(&self) -> UdpListenerConfig {
        self.inner.config()
    }

    /// Passthrough to the wrapped [`UdpListener::local_addr`] -- mirrors
    /// [`crate::syslog::SyslogInput::local_addr`]: lets a caller (a round-trip test) learn the
    /// real ephemeral port after `bind()`, with no bind-drop race.
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        self.inner.local_addr()
    }
}

#[async_trait::async_trait]
impl Input for StatsdInput {
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

/// Decodes raw statsd/DogStatsD datagram bytes into an [`EventBatch`]. Split out from
/// [`StatsdInput`] so the parsing logic is directly unit-testable without a socket.
pub struct StatsdDecoder {
    resource: Arc<Resource>,
    diag: Diagnostics,
}

impl StatsdDecoder {
    pub fn new(resource: Arc<Resource>) -> Self {
        Self { resource, diag: Diagnostics::default() }
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    /// Test-only: confirms `StatsdInput::with_diagnostics` actually reached this decoder's own
    /// `diag`, not just `UdpListener`'s.
    #[cfg(test)]
    pub(crate) fn diag(&self) -> &Diagnostics {
        &self.diag
    }
}

impl Decoder for StatsdDecoder {
    fn decode_into(
        &mut self,
        bytes: Bytes,
        received_at: i64,
        out: &mut Vec<Event>,
    ) -> Result<(Arc<Resource>, Option<Arc<Scope>>), CodecError> {
        let text = std::str::from_utf8(&bytes)
            .map_err(|e| CodecError::Malformed(format!("invalid utf-8: {e}")))?;
        for line in text.split('\n') {
            // `\r` and leading whitespace are trimmed off every line unconditionally; trailing
            // whitespace is trimmed too, *except* on an `_e{`/`_sc|` line, where it can be real
            // payload -- see the module doc's "DogStatsD events and service checks" section for
            // why trimming it there would corrupt a length-delimited or `m:`-terminated field
            // instead of just removing packet padding.
            let line = line.trim_end_matches('\r').trim_start();
            let line = if line.starts_with("_e{") || line.starts_with("_sc|") {
                line
            } else {
                line.trim_end()
            };
            if line.is_empty() {
                continue;
            }
            // One malformed line must not discard unrelated valid metrics elsewhere in the same
            // datagram -- StatsD clients routinely pack several independent metrics into one
            // packet, so treating the datagram as atomic would let a single bad line take down
            // everything alongside it. Isolate per line: keep what parsed, report what didn't.
            match parse_line(&bytes, text, line, received_at) {
                Ok(mut line_events) => out.append(&mut line_events),
                Err(err) => {
                    self.diag.warn_throttled("bad_line", err);
                }
            }
        }
        // statsd datagrams carry no OTLP instrumentation-scope concept -- `None`, always.
        Ok((self.resource.clone(), None))
    }
}

/// Reconstructs a `Bytes` sharing the datagram's underlying allocation for `sub`, a substring
/// derived (through ordinary `&str` slicing -- `split`, `split_once`, `trim_end_matches`/`trim`,
/// indexing) from `text`, which in turn was parsed directly out of `bytes` via `str::from_utf8`.
/// Mirrors `syslog.rs`'s `slice_of` exactly; see that function's doc comment for the full
/// reasoning. The short version: because `sub` is always obtained by slicing `text` rather than by
/// copying or reconstructing it, the pointer-arithmetic round-trip always lands inside `bytes`'s
/// allocation. Unlike `logit-transforms::json::borrowed_str_bytes`, there is no fallback copy here
/// -- a DogStatsD tag value is never unescaped, so there's no case where `sub` could legitimately
/// live outside `bytes`.
fn slice_of(bytes: &Bytes, text: &str, sub: &str) -> Bytes {
    let text_start = text.as_ptr() as usize;
    let sub_start = sub.as_ptr() as usize;
    let start = sub_start - text_start;
    bytes.slice(start..start + sub.len())
}

/// Parses a comma-separated `#<tag>[:<value>],...` segment (the text after the `#`, for a metric
/// line, an event, or a service check alike) and folds each tag into `attributes`. A `key:value`
/// tag's value is a zero-copy [`slice_of`] `text`/`bytes`; a valueless tag (`#urgent`) marks
/// presence as `Value::Bool(true)` instead, since there's nothing to slice. Shared verbatim across
/// every line shape that carries `#tags` -- factored out of `parse_line`'s original inline loop
/// once events and service checks needed the identical behaviour.
///
/// **A repeated tag key folds into a `Value::Array` in wire order**, rather than the
/// last-token-wins behaviour a plain [`AttrMap::insert`] per token would give: a `|#` segment is a
/// list, not a map, so `#team:a,team:b` decodes to `team: Array[Str("a"), Str("b")]`. **An exact duplicate
/// token is deduped here**, matching the Datadog agent's own rule (it keeps every token and
/// dedupes only exact duplicates): `#team:a,team:a` stays `Str("a")` and `#urgent,urgent` stays
/// `Bool(true)`, so a one-element `Array` is never produced and a non-repeated tag's decoded shape
/// is byte-identical to what it was before this fold existed. A bare token and a valued one that
/// share a key are *not* duplicates and both survive, in wire order: `#urgent,urgent:1` is
/// `Array[Bool(true), Str("1")]`, `#urgent:1,urgent` is `Array[Str("1"), Bool(true)]`.
///
/// [`AttrMap::remove`] + [`AttrMap::insert`] is the same two-step [`crate::syslog`]'s
/// `insert_param` uses for a repeated RFC 5424 PARAM-NAME -- two binary searches over the sorted
/// inline map, paid only on a repeat. See the module doc's "DogStatsD tags" section for the
/// semantics this implements and for what it deliberately leaves alone (a repeated `|` *segment*,
/// and the `statsd.*` carrier keys).
fn insert_tags(attributes: &mut AttrMap, bytes: &Bytes, text: &str, tags: &str) {
    for tag in tags.split(',').filter(|t| !t.is_empty()) {
        let (key, value) = match tag.split_once(':') {
            // `v` is a genuine `&str` slice of `text`, so `slice_of` shares the datagram's
            // allocation instead of `Value::from(&str)`'s `Bytes::from(String)` copy.
            Some((k, v)) => (k, Value::Str(slice_of(bytes, text, v))),
            None => (tag, Value::Bool(true)),
        };
        let merged = match attributes.remove(key) {
            None => value,
            Some(Value::Array(mut arr)) => {
                if !arr.iter().any(|e| tag_element_eq(e, &value)) {
                    arr.push(value);
                }
                Value::Array(arr)
            }
            Some(existing) if tag_element_eq(&existing, &value) => existing,
            Some(existing) => Value::Array(vec![existing, value]),
        };
        attributes.insert(key, merged);
    }
}

/// Exact-token equality for [`insert_tags`]'s dedupe rule: `Str`/`Str` by bytes, `Bool`/`Bool` by
/// value, anything else unequal. Allocation-free -- `Bytes: PartialEq` is a plain byte compare, so
/// two tokens pointing at different offsets of the same datagram still compare equal on content.
/// Only these two variants can appear as a decoded tag value (a valued token is always `Str`, a
/// bare one always `Bool(true)`), and the catch-all arm is what makes a `Bool`/`Str` pairing
/// unequal -- the rule that keeps both forms of `#urgent,urgent:1`. Deliberately not `Value`'s own
/// `PartialEq`: that compares `F64`s and nested maps too, neither of which a tag token can be, and
/// this function's contract is the agent's exact-token rule rather than general value equality.
fn tag_element_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Str(a), Value::Str(b)) => a == b,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        _ => false,
    }
}

/// Stamps `statsd.container_id` (rule (b), `docs/adr/lossless-transit.md`: a protocol-namespaced
/// carrier for a concept this model has no normalized field for) as a zero-copy datagram slice.
/// Shared by metric lines' `|c:`, events' `c:`, and service checks' `c:` alike.
fn insert_container_id(attributes: &mut AttrMap, bytes: &Bytes, text: &str, container_id: &str) {
    attributes.insert("statsd.container_id", Value::Str(slice_of(bytes, text, container_id)));
}

/// Parses a `|T<unix-seconds>`/`d:<unix-seconds>` value shared by metric lines, events, and
/// service checks alike: a non-digit or a value whose seconds-to-nanoseconds conversion overflows
/// `i64` rejects only the line it's on, per `docs/adr/lossless-transit.md`'s per-line isolation
/// stance, rather than silently falling back to receipt time. Returns `(nanos, secs)` -- `nanos`
/// for `Event::timestamp`, `secs` (the raw parsed wire value, not just a marker bit) for the
/// `statsd.timestamp` carrier every caller also stamps, so a stage downstream that rebuilds
/// `Event::timestamp` (`aggregate`'s flush, notably) can't fabricate a wire timestamp that was
/// never sent.
fn parse_wire_seconds(secs: &str, line: &str) -> Result<(i64, u64), CodecError> {
    let malformed = || CodecError::Malformed(format!("malformed statsd line: {line:?}"));
    let secs: u64 = secs.parse().map_err(|_| malformed())?;
    let nanos = secs
        .checked_mul(1_000_000_000)
        .and_then(|n| i64::try_from(n).ok())
        .ok_or_else(malformed)?;
    Ok((nanos, secs))
}

/// `bytes`/`text` are the *whole datagram* -- the same `Bytes` (and its `&str` view) passed into
/// [`StatsdDecoder::decode`] -- threaded down so [`slice_of`] can reconstruct each tag value as a
/// zero-copy slice of it. `line` is one line of that datagram (already isolated by `decode`, and
/// itself a genuine `&str` slice of `text`), used for parsing and error messages.
fn parse_line(
    bytes: &Bytes,
    text: &str,
    line: &str,
    timestamp: i64,
) -> Result<Vec<Event>, CodecError> {
    // DogStatsD events and service checks are picked out by their leading sigil, before any of
    // the `<name>:<value>|<type>` grammar below applies at all -- see the module doc's "DogStatsD
    // events and service checks" section. Exactly these two prefixes are special; any other line
    // -- including one that merely starts with `_` without matching either, like a legal
    // `_`-prefixed metric name -- falls through unchanged into the generic grammar below.
    if line.starts_with("_e{") {
        return parse_event(bytes, text, line, timestamp).map(|event| vec![event]);
    }
    if line.starts_with("_sc|") {
        return parse_service_check(bytes, text, line, timestamp).map(|event| vec![event]);
    }

    let malformed = || CodecError::Malformed(format!("malformed statsd line: {line:?}"));

    let (name, rest) = line.split_once(':').ok_or_else(malformed)?;
    if name.is_empty() {
        return Err(malformed());
    }

    let mut segments = rest.split('|');
    let values_part = segments.next().ok_or_else(malformed)?;
    let type_part = segments.next().ok_or_else(malformed)?;

    let mut sample_rate = 1.0f64;
    let mut attributes = AttrMap::new();
    // Overridden by `|T<secs>` below; otherwise every event on this line keeps the receipt-time
    // timestamp `decode_into` was called with.
    let mut line_timestamp = timestamp;
    for extra in segments {
        if let Some(rate) = extra.strip_prefix('@') {
            let parsed: f64 = rate.parse().map_err(|_| malformed())?;
            // A sample rate is a probability: it must be finite and in (0, 1]. `f64::parse`
            // happily accepts "NaN"/"inf"/negative/zero/>1 text, any of which would turn into a
            // non-finite or negative counter value (or a divide-by-zero) below -- reject them here
            // rather than let bad input poison a value that later gets merged and shipped.
            if !parsed.is_finite() || parsed <= 0.0 || parsed > 1.0 {
                return Err(malformed());
            }
            sample_rate = parsed;
        } else if let Some(tags) = extra.strip_prefix('#') {
            insert_tags(&mut attributes, bytes, text, tags);
        } else if let Some(container_id) = extra.strip_prefix("c:") {
            // DogStatsD container id (`|c:<id>`, v1.2+; v1.4+'s `ci-`/`in-`-prefixed variants
            // land in the same slot verbatim -- this decoder carries whatever follows `c:`
            // unchanged, it doesn't parse the prefixed forms specially). Applied to every metric
            // type here, not only `c`/`g` as the spec restricts it to -- see the module doc's
            // forward-compatibility note. Rule (b) (`docs/adr/lossless-transit.md`): a
            // protocol-namespaced carrier for a concept this model has no normalized field for.
            insert_container_id(&mut attributes, bytes, text, container_id);
        } else if let Some(secs) = extra.strip_prefix('T') {
            // DogStatsD point timestamp (`|T<unix-seconds>`, v1.3+, spec-restricted to `c`/`g`
            // but accepted here on every type -- see the module doc). A non-digit or
            // seconds-to-nanoseconds-overflowing value rejects only this line, rather than
            // silently falling back to receipt time.
            let (nanos, secs) = parse_wire_seconds(secs, line)?;
            line_timestamp = nanos;
            // The carrier holds the parsed wire value itself, not just a marker bit -- so a
            // stage downstream that rebuilds `Event::timestamp` (`aggregate`'s flush, notably)
            // can't fabricate a `|T` value the wire never sent: `statsd_out` reads this attribute
            // directly rather than trusting `event.timestamp`.
            attributes.insert("statsd.timestamp", Value::U64(secs));
        }
        // Anything else is accepted and ignored -- forward-compatible with segment kinds this
        // decoder doesn't know about yet, rather than a hard error on something benign.
        // (DogStatsD events and service checks are dispatched to their own parsers above, via
        // the `_e{`/`_sc|` leading sigils, before this per-segment loop is ever reached for
        // those lines.)
    }

    match type_part {
        "c" | "g" => values_part
            .split(':')
            .map(|raw_value| {
                build_event(
                    name,
                    raw_value,
                    type_part,
                    sample_rate,
                    &attributes,
                    line_timestamp,
                    line,
                )
            })
            .collect(),
        "ms" | "h" | "d" => {
            // One `Event` per *line*, not per value -- every value on the line shares one
            // `Samples` record (`docs/adr/lossless-transit.md`'s "summarization is opt-in and
            // named": no sketching, no sample-rate extrapolation here; `sample_rate` rides
            // verbatim for `aggregate` to decide about). Pushed straight into `Samples::default`'s
            // own inline `SmallVec` (`SAMPLES_INLINE = 19`, `crates/logit-core/src/metric.rs`)
            // rather than collected into an intermediate `Vec<f64>` first -- an owned `Vec` would
            // be a real allocation even for a single value, which `SmallVec`'s inline storage
            // avoids up to 19 of them.
            let mut samples = Samples::default();
            for raw_value in values_part.split(':') {
                samples.values.push(parse_finite_value(raw_value, "timing/histogram", line)?);
            }
            samples.sample_rate = sample_rate;
            let mut attrs = attributes.clone();
            // The wire type letter survives as `statsd.type` (rule (b)) since `ms`/`h`/`d` all
            // land on the same `Samples` shape -- a zero-copy slice of the datagram, like every
            // other string-valued attribute this decoder stamps.
            attrs.insert("statsd.type", Value::Str(slice_of(bytes, text, type_part)));
            let kind = MetricKind::Samples(samples);
            Ok(vec![Event::metric(line_timestamp, attrs, MetricRecord::new(intern(name), kind))])
        }
        "s" => {
            // One `Event` per line, mirroring `ms`/`h`/`d` above: every member on the line is a
            // zero-copy `Bytes` slice of the datagram, in wire order. `sample_rate` is ignored,
            // same reasoning as `g` -- a set member is not a count to extrapolate.
            let members: Vec<Bytes> =
                values_part.split(':').map(|raw_value| slice_of(bytes, text, raw_value)).collect();
            Ok(vec![Event::metric(
                line_timestamp,
                attributes.clone(),
                MetricRecord::new(intern(name), MetricKind::SetMembers(members)),
            )])
        }
        other => Err(CodecError::Malformed(format!("unknown metric type '{other}': {line:?}"))),
    }
}

/// Parses a DogStatsD event line (`_e{<TITLE_LEN>,<TEXT_LEN>}:<TITLE>|<TEXT>|...`) -- see the
/// module doc's "DogStatsD events and service checks" section for the full grammar and decoded
/// shape. `line` is already known to start with `"_e{"` (checked by `parse_line`'s dispatch).
fn parse_event(bytes: &Bytes, text: &str, line: &str, timestamp: i64) -> Result<Event, CodecError> {
    let malformed = || CodecError::Malformed(format!("malformed dogstatsd event: {line:?}"));

    let header_rest = line.strip_prefix("_e{").ok_or_else(malformed)?;
    let (header, after_header) = header_rest.split_once('}').ok_or_else(malformed)?;
    let (title_len, text_len) = header.split_once(',').ok_or_else(malformed)?;
    let title_len: usize = title_len.parse().map_err(|_| malformed())?;
    let text_len: usize = text_len.parse().map_err(|_| malformed())?;
    let after_header = after_header.strip_prefix(':').ok_or_else(malformed)?;

    // `TITLE_LEN`/`TEXT_LEN` are authoritative, *byte* lengths -- not naive `|`-splitting, since
    // `TEXT` may itself contain `|`/`:`. `str::get` on a byte range returns `None` for both an
    // out-of-bounds length and one that doesn't land on a UTF-8 char boundary, so this rejects
    // both cases without ever indexing in a way that could panic.
    let title = after_header.get(..title_len).ok_or_else(malformed)?;
    let after_title = after_header[title_len..].strip_prefix('|').ok_or_else(malformed)?;
    let raw_text = after_title.get(..text_len).ok_or_else(malformed)?;
    let after_text = &after_title[text_len..];

    let mut attributes = AttrMap::new();
    attributes.insert("statsd.event.title", Value::Str(slice_of(bytes, text, title)));

    let mut line_timestamp = timestamp;
    let mut severity = None;

    if !after_text.is_empty() {
        let fields = after_text.strip_prefix('|').ok_or_else(malformed)?;
        for field in fields.split('|') {
            if let Some(tags) = field.strip_prefix('#') {
                insert_tags(&mut attributes, bytes, text, tags);
            } else if let Some(container_id) = field.strip_prefix("c:") {
                insert_container_id(&mut attributes, bytes, text, container_id);
            } else if let Some(secs) = field.strip_prefix("d:") {
                let (nanos, secs) = parse_wire_seconds(secs, line)?;
                line_timestamp = nanos;
                attributes.insert("statsd.timestamp", Value::U64(secs));
            } else if let Some(host) = field.strip_prefix("h:") {
                attributes.insert("statsd.event.host", Value::Str(slice_of(bytes, text, host)));
            } else if let Some(priority) = field.strip_prefix("p:") {
                if priority != "normal" && priority != "low" {
                    return Err(malformed());
                }
                attributes
                    .insert("statsd.event.priority", Value::Str(slice_of(bytes, text, priority)));
            } else if let Some(alert_type) = field.strip_prefix("t:") {
                severity = Some(match alert_type {
                    "error" => Severity::Error,
                    "warning" => Severity::Warn,
                    "success" | "info" => Severity::Info,
                    _ => return Err(malformed()),
                });
                attributes.insert(
                    "statsd.event.alert_type",
                    Value::Str(slice_of(bytes, text, alert_type)),
                );
            } else if let Some(key) = field.strip_prefix("k:") {
                attributes
                    .insert("statsd.event.aggregation_key", Value::Str(slice_of(bytes, text, key)));
            } else if let Some(source) = field.strip_prefix("s:") {
                attributes
                    .insert("statsd.event.source_type", Value::Str(slice_of(bytes, text, source)));
            }
            // Anything else (including `|T`, which is not part of this grammar) is accepted and
            // ignored -- same forward-compatible stance as an unrecognized segment on a metric
            // line.
        }
    }

    Ok(Event::log(
        line_timestamp,
        attributes,
        LogRecord {
            message: unescape_event_text(bytes, text, raw_text),
            severity,
            body_format: BodyFormat::Raw,
            trace: None,
            // Deliberately `None`, not `intern`ed: an event title is free text an operator or
            // their application chose at send time, not a fixed, bounded vocabulary the way a
            // metric/tag name is -- interning every one would grow the global interner without
            // bound.
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    ))
}

/// The wire `TEXT` of a DogStatsD event, unescaped: DogStatsD's own `\n` (backslash, `n`)
/// two-byte escape means a real newline in the decoded message. Zero-copy (a [`slice_of`] the
/// datagram) in the common case where there's nothing to unescape; only allocates when `raw`
/// actually contains the escape sequence -- and then exactly once: the output length is known up
/// front (every two-byte escape shrinks to one byte), so the buffer is sized exactly and
/// `Bytes::from(Vec)` takes its no-copy `len == capacity` path instead of paying a second
/// allocation for a slack-capacity `String::replace` result. The title is never unescaped --
/// only `TEXT`.
fn unescape_event_text(bytes: &Bytes, text: &str, raw: &str) -> Value {
    let escapes = raw.matches("\\n").count();
    if escapes == 0 {
        return Value::Str(slice_of(bytes, text, raw));
    }
    let mut out = Vec::with_capacity(raw.len() - escapes);
    let mut rest = raw;
    while let Some(at) = rest.find("\\n") {
        out.extend_from_slice(&rest.as_bytes()[..at]);
        out.push(b'\n');
        rest = &rest[at + 2..];
    }
    out.extend_from_slice(rest.as_bytes());
    debug_assert_eq!(out.len(), out.capacity());
    Value::Str(Bytes::from(out))
}

/// Parses a DogStatsD service check line (`_sc|<NAME>|<STATUS>|...`) -- see the module doc's
/// "DogStatsD events and service checks" section for the full grammar and decoded shape. `line`
/// is already known to start with `"_sc|"` (checked by `parse_line`'s dispatch).
fn parse_service_check(
    bytes: &Bytes,
    text: &str,
    line: &str,
    timestamp: i64,
) -> Result<Event, CodecError> {
    let malformed =
        || CodecError::Malformed(format!("malformed dogstatsd service check: {line:?}"));

    let rest = line.strip_prefix("_sc|").ok_or_else(malformed)?;
    // `m:`, when present, is always the *last* field and consumes the rest of the line verbatim
    // (a message may itself contain `|`) -- so this only ever needs to split the line into at
    // most three pieces: NAME, STATUS, and "everything else" (handled field-by-field below).
    let mut parts = rest.splitn(3, '|');
    let name = parts.next().ok_or_else(malformed)?;
    if name.is_empty() {
        return Err(malformed());
    }
    let status: u8 = parts.next().ok_or_else(malformed)?.parse().map_err(|_| malformed())?;
    if status > 3 {
        return Err(malformed());
    }

    let mut attributes = AttrMap::new();
    // Rule (b) (`docs/adr/lossless-transit.md`): the raw carrier -- `MetricRecord` has nowhere
    // else for a service check's name to land, so it's stamped unconditionally, not just on
    // mismatch.
    attributes.insert("statsd.service_check.name", Value::Str(slice_of(bytes, text, name)));
    attributes.insert("statsd.service_check.status", Value::U64(status as u64));

    let mut line_timestamp = timestamp;

    if let Some(mut cursor) = parts.next() {
        loop {
            if let Some(message) = cursor.strip_prefix("m:") {
                attributes.insert(
                    "statsd.service_check.message",
                    Value::Str(slice_of(bytes, text, message)),
                );
                break;
            }
            let (field, rest) = match cursor.split_once('|') {
                Some((field, rest)) => (field, Some(rest)),
                None => (cursor, None),
            };
            if let Some(tags) = field.strip_prefix('#') {
                insert_tags(&mut attributes, bytes, text, tags);
            } else if let Some(container_id) = field.strip_prefix("c:") {
                insert_container_id(&mut attributes, bytes, text, container_id);
            } else if let Some(secs) = field.strip_prefix("d:") {
                let (nanos, secs) = parse_wire_seconds(secs, line)?;
                line_timestamp = nanos;
                attributes.insert("statsd.timestamp", Value::U64(secs));
            } else if let Some(host) = field.strip_prefix("h:") {
                attributes
                    .insert("statsd.service_check.host", Value::Str(slice_of(bytes, text, host)));
            }
            // Anything else (including `|T`) is accepted and ignored, same forward-compatible
            // stance as everywhere else in this decoder.

            match rest {
                Some(next) => cursor = next,
                None => break,
            }
        }
    }

    Ok(Event::metric(
        line_timestamp,
        attributes,
        MetricRecord::new(intern(name), MetricKind::Gauge(status as f64)),
    ))
}

#[allow(clippy::too_many_arguments)]
fn build_event(
    name: &str,
    raw_value: &str,
    type_part: &str,
    sample_rate: f64,
    attributes: &AttrMap,
    timestamp: i64,
    line: &str,
) -> Result<Event, CodecError> {
    let kind = match type_part {
        "c" => {
            let value = parse_finite_value(raw_value, "counter", line)?;
            MetricKind::counter(value / sample_rate)
        }
        "g" => {
            // Any leading '+'/'-' means a *relative* adjustment to the gauge's previous value,
            // per the statsd/DogStatsD spec -- and per that same spec there is no wire syntax for
            // setting a gauge to a negative absolute value at all, so a leading '-' is just as
            // unambiguous as '+', not a case needing its own guess. No config escape hatch: this
            // decoder used to reject any signed value outright, so there is no prior working
            // "absolute negative gauge" behavior a `negative_gauge: delta|absolute` toggle could
            // ever have been preserving (see docs/adr/relative-gauge-adjustments.md's
            // Alternatives). `f64::from_str` accepts a leading '+' the same as '-' (pinned by
            // `plus_prefixed_gauge_values_parse_via_from_str`), so `parse_finite_value` handles
            // both signs identically; only the *choice* between `Gauge`/`GaugeDelta` is decided
            // here. Resolution belongs to `aggregate` (docs/design/data-model.md is explicit that
            // aggregation state lives there, not in the wire decoder) -- this decoder only marks
            // the value unresolved and hands it off; a `GaugeDelta` that reaches a sink with no
            // `aggregate` on its path is that component's problem to report, not this one's to
            // guess around.
            //
            // `sample_rate` is deliberately ignored here: a gauge value is absolute (or, for a
            // delta, an adjustment), not a count of occurrences, so there is nothing to
            // extrapolate -- unlike `c`/`ms`/`h`/`d`, "1 in N samples reported this value"
            // doesn't imply anything about the other N-1, and pretending otherwise would be
            // meaningless, not just a missed opportunity.
            let value = parse_finite_value(raw_value, "gauge", line)?;
            if raw_value.starts_with('+') || raw_value.starts_with('-') {
                MetricKind::GaugeDelta(value)
            } else {
                MetricKind::Gauge(value)
            }
        }
        // `ms`/`h`/`d`/`s` never reach here -- `parse_line` handles them itself, one `Event` per
        // line rather than per value.
        other => unreachable!("build_event only handles c/g, got {other:?}"),
    };

    // Cheap for the multi-value form (`name:1:2:3|c`), where this runs once per shared value:
    // every `Value::Str` in `attributes` is already a slice of the datagram's one shared
    // allocation (see `slice_of`), so cloning a scalar-valued map is a `SmallVec` memcpy plus a
    // refcount bump per tag, not a fresh copy of the tag bytes. A tag whose key repeated on the
    // wire is the one exception: it holds a `Value::Array` (see `insert_tags`), and cloning that
    // deep-copies the `Vec` spine -- one fresh allocation per such tag per value event -- though
    // the elements inside it are still refcounted `Bytes` slices of the same datagram, never
    // copied bytes.
    Ok(Event::metric(timestamp, attributes.clone(), MetricRecord::new(intern(name), kind)))
}

/// Parses a metric value and rejects it unless finite. `f64::parse` accepts the literal text
/// "NaN"/"inf"/"-inf", which would otherwise become a non-finite `Sum` (`MetricKind::counter`),
/// `Gauge(inf)`, or -- worse --
/// get inserted into a `DdSketch`, where a NaN sample corrupts the sketch's summary state rather
/// than just producing one bad data point. Shared by the counter/gauge/timing-histogram-
/// distribution branches in `build_event`, which differ only in the value's name for the error.
fn parse_finite_value(raw_value: &str, what: &str, line: &str) -> Result<f64, CodecError> {
    let value: f64 = raw_value
        .parse()
        .map_err(|_| CodecError::Malformed(format!("invalid {what} value: {line:?}")))?;
    if !value.is_finite() {
        return Err(CodecError::Malformed(format!("{what} value must be finite: {line:?}")));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(line: &str) -> Vec<Event> {
        let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()));
        decoder.decode(Bytes::from(line.to_string())).expect("decode should succeed").events
    }

    /// Regression: `StatsdInput::with_diagnostics` used to only set `UdpListener`'s own `diag`,
    /// never reaching the wrapped `StatsdDecoder`'s -- so a malformed *line* (as opposed to a
    /// whole malformed datagram) reported through a permanently unnamed, telemetry-disabled
    /// `Diagnostics::default()`, regardless of what the component was actually configured with.
    #[test]
    fn with_diagnostics_reaches_the_wrapped_decoder_too() {
        let input = StatsdInput::new("127.0.0.1:0").with_diagnostics(Diagnostics::new("my-id"));
        assert_eq!(input.inner.decoder().diag().component_id(), "my-id");
    }

    /// `decode_into` must stamp every event with the caller's `received_at`, not a fresh
    /// call-time clock read -- the property `docs/adr/decoupled-listener-io.md` exists for:
    /// once decode runs on its own loop, "now" at decode time can be arbitrarily later than
    /// arrival under backlog.
    #[test]
    fn decode_into_stamps_events_with_the_callers_received_at_not_the_current_time() {
        let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()));
        let deliberately_not_now: i64 = 123;
        let mut out = Vec::new();
        decoder
            .decode_into(Bytes::from_static(b"hits:1|c"), deliberately_not_now, &mut out)
            .expect("decode should succeed");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].timestamp, deliberately_not_now);
    }

    /// `decode_into` appends to `out` rather than replacing it -- the property that lets a caller
    /// accumulate several datagrams' events into one reused buffer
    /// (`logit_pipeline::BatchAccumulator`) instead of allocating fresh per datagram.
    #[test]
    fn decode_into_appends_to_an_already_populated_out_buffer_rather_than_replacing_it() {
        let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()));
        let mut out = vec![Event::empty(0, AttrMap::new())];
        decoder
            .decode_into(Bytes::from_static(b"hits:1|c"), 1, &mut out)
            .expect("decode should succeed");
        assert_eq!(out.len(), 2, "the pre-existing event must survive, plus the newly decoded one");
    }

    fn only_metric(events: Vec<Event>) -> MetricRecord {
        assert_eq!(events.len(), 1, "expected exactly one event");
        let mut event = events.into_iter().next().unwrap();
        assert_eq!(event.metrics.len(), 1, "expected exactly one metric on that event");
        // statsd is a metrics-only input: pinning this down here means a future attempt to fold
        // a multi-value line into one multi-metric event (rather than one event per value, as
        // today) fails loudly in this helper rather than silently changing 17 tests' meaning.
        assert!(event.log.is_none() && event.span.is_none(), "statsd emits metric-only events");
        event.metrics.pop().unwrap()
    }

    /// For asserting a specific line is rejected: `decode()` itself now isolates per-line errors
    /// (a malformed line must not discard unrelated valid metrics in the same datagram -- see
    /// `malformed_line_does_not_drop_other_valid_metrics_in_same_datagram` below), so it no
    /// longer surfaces one. `parse_line` is where that rejection actually happens.
    fn parse_err(line: &str) -> CodecError {
        let bytes = Bytes::from(line.to_string());
        let text = std::str::from_utf8(&bytes).unwrap();
        parse_line(&bytes, text, text, 0).expect_err("expected this line to be rejected")
    }

    #[test]
    fn counter() {
        let metric = only_metric(decode("page.views:1|c"));
        assert_eq!(intern("page.views"), metric.name);
        assert!(
            matches!(metric.kind, MetricKind::Sum(logit_core::Sum { value, .. }) if value == 1.0)
        );
    }

    #[test]
    fn counter_with_sample_rate_extrapolates() {
        let metric = only_metric(decode("page.views:2|c|@0.5"));
        assert!(matches!(
            metric.kind,
            MetricKind::Sum(logit_core::Sum { value, .. }) if (value - 4.0).abs() < 1e-9
        ));
    }

    #[test]
    fn invalid_sample_rates_are_rejected() {
        // Zero would divide-by-zero into an infinite counter; negative and >1 aren't valid
        // probabilities; NaN/inf parse successfully as f64 but aren't finite. Any of these would
        // otherwise poison a counter's `Sum` value that later gets merged and shipped downstream.
        for rate in ["0", "-0.5", "1.5", "NaN", "inf", "-inf"] {
            let line = format!("hits:1|c|@{rate}");
            assert!(
                matches!(parse_err(&line), CodecError::Malformed(_)),
                "expected @{rate} to be rejected"
            );
        }
    }

    #[test]
    fn non_finite_counter_values_are_rejected() {
        // `f64::parse` accepts the literal text "NaN"/"inf"/"-inf" -- unguarded, these would
        // become a non-finite `Sum` (`MetricKind::counter`) rather than being caught at decode
        // time.
        for value in ["NaN", "inf", "-inf"] {
            let line = format!("hits:{value}|c");
            assert!(
                matches!(parse_err(&line), CodecError::Malformed(_)),
                "expected {value} to be rejected"
            );
        }
    }

    #[test]
    fn non_finite_gauge_values_are_rejected() {
        for value in ["NaN", "inf", "-inf"] {
            let line = format!("load:{value}|g");
            assert!(
                matches!(parse_err(&line), CodecError::Malformed(_)),
                "expected {value} to be rejected"
            );
        }
    }

    #[test]
    fn non_finite_distribution_values_are_rejected() {
        // Worse than a bad counter/`Gauge` value: a NaN sample inserted into a DdSketch corrupts
        // the sketch's summary state rather than just producing one bad data point.
        for value in ["NaN", "inf", "-inf"] {
            let line = format!("latency:{value}|ms");
            assert!(
                matches!(parse_err(&line), CodecError::Malformed(_)),
                "expected {value} to be rejected"
            );
        }
    }

    #[test]
    fn gauge() {
        let metric = only_metric(decode("cpu.load:0.75|g"));
        assert!(matches!(metric.kind, MetricKind::Gauge(v) if v == 0.75));
    }

    /// `f64::from_str`'s grammar accepts a leading `+` the same as `-` -- pinned directly, since
    /// `build_event`'s `"g"` arm relies on this to make `parse_finite_value` handle both signs
    /// identically and let only the `starts_with` check decide `Gauge` vs. `GaugeDelta`.
    #[test]
    fn plus_prefixed_gauge_values_parse_via_from_str() {
        assert_eq!("+5".parse::<f64>(), Ok(5.0));
        assert_eq!("+0".parse::<f64>(), Ok(0.0));
    }

    #[test]
    fn a_leading_plus_decodes_as_a_gauge_delta() {
        let metric = only_metric(decode("conns:+5|g"));
        assert!(matches!(metric.kind, MetricKind::GaugeDelta(v) if v == 5.0));
    }

    #[test]
    fn a_leading_minus_decodes_as_a_gauge_delta() {
        let metric = only_metric(decode("conns:-5|g"));
        assert!(matches!(metric.kind, MetricKind::GaugeDelta(v) if v == -5.0));
    }

    /// The unsigned case is unchanged by this workstream -- pinned directly, not just implied by
    /// the pre-existing `gauge` test, since it's the regression that matters most here.
    #[test]
    fn an_unsigned_gauge_value_still_decodes_as_an_absolute_gauge() {
        let metric = only_metric(decode("cpu.load:5|g"));
        assert!(matches!(metric.kind, MetricKind::Gauge(v) if v == 5.0));
    }

    /// `+0` is a legal no-op delta, not an error -- distinct from an *unsigned* `0`, which is
    /// (and stays) an ordinary absolute `Gauge(0.0)`.
    #[test]
    fn a_leading_plus_zero_is_a_legal_no_op_delta_not_an_error() {
        let metric = only_metric(decode("conns:+0|g"));
        assert!(matches!(metric.kind, MetricKind::GaugeDelta(v) if v == 0.0));
    }

    /// A signed non-finite value is still rejected by `parse_finite_value`, same as an unsigned
    /// one -- the sign only decides `Gauge` vs. `GaugeDelta`, never bypasses the finiteness check.
    #[test]
    fn signed_non_finite_gauge_values_are_still_rejected() {
        for value in ["+NaN", "+inf", "-inf"] {
            let line = format!("load:{value}|g");
            assert!(
                matches!(parse_err(&line), CodecError::Malformed(_)),
                "expected {value} to be rejected"
            );
        }
    }

    #[test]
    fn a_signed_gauge_with_tags_and_a_sample_rate_decodes() {
        let events = decode("conns:-5|g|@0.5|#host:web1");
        let event = &events[0];
        assert!(matches!(event.metrics[0].kind, MetricKind::GaugeDelta(v) if v == -5.0));
        assert_eq!(event.attributes.get("host").and_then(|v| v.as_str()), Some("web1"));
    }

    /// Multi-value grammar (`name:v1:v2|type`) applied to signed gauge values: each value is
    /// decoded independently, so a mix of signs on one line yields two independent deltas, not
    /// one merged value or a decode error.
    #[test]
    fn multi_value_signed_gauges_yield_two_independent_deltas() {
        let events = decode("conns:+1:-2|g");
        assert_eq!(events.len(), 2);
        assert!(
            matches!(only_metric(vec![events[0].clone()]).kind, MetricKind::GaugeDelta(v) if v == 1.0)
        );
        assert!(
            matches!(only_metric(vec![events[1].clone()]).kind, MetricKind::GaugeDelta(v) if v == -2.0)
        );
    }

    /// `ms`/`h`/`d` decode straight to raw [`MetricKind::Samples`] now -- no sketching, no
    /// extrapolation (`docs/adr/lossless-transit.md`'s "summarization is opt-in and named": only
    /// `aggregate` sketches). This replaces this test's pre-W3 assertion that a single `ms` value
    /// became a one-count `DdSketch`.
    #[test]
    fn timer_becomes_a_single_sample_distribution() {
        let metric = only_metric(decode("request.latency:120|ms"));
        match metric.kind {
            MetricKind::Samples(samples) => {
                assert_eq!(samples.values.as_slice(), &[120.0]);
                assert_eq!(samples.sample_rate, 1.0);
            }
            other => panic!("expected Samples, got {other:?}"),
        }
    }

    /// Pre-W3 this asserted the `@0.5` rate got extrapolated into two weighted `DdSketch`
    /// samples at decode time. `docs/adr/lossless-transit.md`'s "summarization is opt-in and
    /// named" moves that extrapolation to `aggregate` -- the raw rate now rides verbatim on the
    /// decoded `Samples` instead.
    #[test]
    fn sampled_distribution_at_half_rate_preserves_the_rate_without_extrapolating() {
        let metric = only_metric(decode("x:100|ms|@0.5"));
        match metric.kind {
            MetricKind::Samples(samples) => {
                assert_eq!(samples.values.as_slice(), &[100.0]);
                assert_eq!(samples.sample_rate, 0.5);
            }
            other => panic!("expected Samples, got {other:?}"),
        }
    }

    /// Same shift as the half-rate test above, at `@0.1`.
    #[test]
    fn sampled_distribution_at_tenth_rate_preserves_the_rate_without_extrapolating() {
        let metric = only_metric(decode("x:100|ms|@0.1"));
        match metric.kind {
            MetricKind::Samples(samples) => {
                assert_eq!(samples.values.as_slice(), &[100.0]);
                assert_eq!(samples.sample_rate, 0.1);
            }
            other => panic!("expected Samples, got {other:?}"),
        }
    }

    /// An explicit `@1` (the default, unsampled rate) still decodes to one raw value at rate
    /// `1.0` -- unchanged in spirit from this test's pre-W3 "one sample, not extrapolated"
    /// claim, just against `Samples` instead of a `DdSketch`. `statsd_decode_one_line` in
    /// `crates/logit-bench/tests/allocations.rs` pins the same claim at the allocation level.
    #[test]
    fn unsampled_distribution_still_inserts_exactly_one_sample() {
        let metric = only_metric(decode("x:100|ms|@1"));
        match metric.kind {
            MetricKind::Samples(samples) => {
                assert_eq!(samples.values.as_slice(), &[100.0]);
                assert_eq!(samples.sample_rate, 1.0);
            }
            other => panic!("expected Samples, got {other:?}"),
        }
    }

    /// `ms`/`h`/`d` share one `Samples` record per *line*, not per value -- every `:`-separated
    /// value on the line lands in `values`, in wire order, on a single `Event`.
    #[test]
    fn multi_value_timer_produces_one_event_with_all_values() {
        let events = decode("request.latency:100:200:300|ms");
        assert_eq!(events.len(), 1, "ms/h/d lines are one event per line, not per value");
        let metric = only_metric(events);
        match metric.kind {
            MetricKind::Samples(samples) => {
                assert_eq!(samples.values.as_slice(), &[100.0, 200.0, 300.0]);
            }
            other => panic!("expected Samples, got {other:?}"),
        }
    }

    /// The wire type letter survives as `statsd.type` (rule (b), `docs/adr/lossless-transit.md`)
    /// on every one of the three types that normalize onto the same `Samples` shape.
    #[test]
    fn statsd_type_is_stamped_for_each_timer_type() {
        for (line, expected) in [("x:1|ms", "ms"), ("x:1|h", "h"), ("x:1|d", "d")] {
            let events = decode(line);
            assert_eq!(
                events[0].attributes.get("statsd.type").and_then(|v| v.as_str()),
                Some(expected),
                "statsd.type should be stamped for {line:?}"
            );
        }
    }

    // The weight-clamping and decode-time-sketch-quantile-accuracy coverage that used to live
    // here (an extreme `@rate` clamping to `MAX_SAMPLE_WEIGHT`, and a sampled distribution's
    // quantile staying within the configured relative error bound) moved to `aggregate`, the
    // only component that sketches a `Samples` record now
    // (`docs/adr/aggregation-window-semantics.md`'s amendment) -- see
    // `crates/logit-transforms/src/aggregate.rs`'s
    // `samples_sketch_mode_merges_weighted_values_and_counts_weight_clamp` for the clamp, and
    // `logit_core::metric`'s `Samples::sketch` tests for the quantile-accuracy claim. This
    // decoder no longer builds a `DdSketch` at all.

    #[test]
    fn dogstatsd_tags_become_attributes() {
        let events = decode("page.views:1|c|#env:prod,host:web1,urgent");
        let event = &events[0];
        assert_eq!(event.attributes.get("env").and_then(|v| v.as_str()), Some("prod"));
        assert_eq!(event.attributes.get("host").and_then(|v| v.as_str()), Some("web1"));
        assert!(matches!(event.attributes.get("urgent"), Some(logit_core::Value::Bool(true))));
    }

    #[test]
    fn multi_value_shares_type_and_tags() {
        let events = decode("page.views:1:2:3|c|#env:prod");
        assert_eq!(events.len(), 3);
        for event in &events {
            assert_eq!(event.attributes.get("env").and_then(|v| v.as_str()), Some("prod"));
        }
    }

    #[test]
    fn dogstatsd_tag_value_is_a_zero_copy_slice_of_the_datagram() {
        // Structural companion to `syslog.rs`'s `emitted_message_is_a_zero_copy_slice_of_the_datagram`
        // -- pins the property this module's `slice_of` exists for, not just its resulting value.
        // The repeated `team` key carries the same assertion through the `Value::Array` fold: a
        // repeat must not start reaching for `Value::from(&str)`'s copy -- each element is the
        // same datagram slice a scalar tag value gets.
        let datagram = Bytes::from("page.views:1|c|#env:prod,team:a,team:b".to_string());
        let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()));
        let event = only_metric_event(decoder.decode(datagram.clone()).unwrap().events);

        let tag = event.attributes.get("env").expect("env tag");
        let Value::Str(tag) = tag else { panic!("expected Value::Str, got {tag:?}") };
        assert_shares_datagram_allocation(&datagram, tag, "a scalar tag value");

        let team = event.attributes.get("team").expect("team tag");
        let Value::Array(elements) = team else { panic!("expected Value::Array, got {team:?}") };
        assert_eq!(elements.len(), 2, "both wire values should survive the fold");
        for element in elements {
            let Value::Str(element) = element else {
                panic!("expected Value::Str element, got {element:?}")
            };
            assert_shares_datagram_allocation(&datagram, element, "an array tag element");
        }
    }

    /// Asserts `slice` points inside `datagram`'s allocation -- i.e. it is a [`slice_of`] view
    /// into the received bytes rather than a fresh copy of them.
    fn assert_shares_datagram_allocation(datagram: &Bytes, slice: &Bytes, what: &str) {
        let base_start = datagram.as_ptr() as usize;
        let base_end = base_start + datagram.len();
        let start = slice.as_ptr() as usize;
        let end = start + slice.len();
        assert!(
            start >= base_start && end <= base_end,
            "{what} should be a slice of the original datagram, not a copy"
        );
    }

    /// A `|#` segment is a list, not a map: `#team:a,team:b` is two live tags, so the repeated key
    /// folds into a `Value::Array` in wire order instead of the last token winning.
    #[test]
    fn a_repeated_tag_key_folds_into_an_array_in_wire_order() {
        let events = decode("page.views:1|c|#team:a,team:b");
        assert_eq!(
            events[0].attributes.get("team"),
            Some(&Value::Array(vec![Value::str("a"), Value::str("b")]))
        );
    }

    #[test]
    fn three_occurrences_of_a_tag_key_fold_into_three_array_elements() {
        let events = decode("page.views:1|c|#team:a,team:b,team:c");
        assert_eq!(
            events[0].attributes.get("team"),
            Some(&Value::Array(vec![Value::str("a"), Value::str("b"), Value::str("c")]))
        );
    }

    /// The Datadog agent dedupes *exact* duplicate tokens, and so does this decoder -- which is
    /// also what guarantees a one-element `Array` is never produced, leaving a non-repeated tag's
    /// decoded shape exactly as it was before the fold existed.
    #[test]
    fn an_exact_duplicate_tag_is_deduped_instead_of_becoming_an_array() {
        let events = decode("page.views:1|c|#team:a,team:a");
        assert_eq!(events[0].attributes.get("team"), Some(&Value::str("a")));
    }

    #[test]
    fn an_exact_duplicate_bare_tag_is_deduped_instead_of_becoming_an_array() {
        let events = decode("page.views:1|c|#urgent,urgent");
        assert_eq!(events[0].attributes.get("urgent"), Some(&Value::Bool(true)));
    }

    /// A duplicate among distinct values drops only the duplicate -- `a,b,a` is two live tags.
    #[test]
    fn a_duplicate_among_distinct_tag_values_drops_only_the_duplicate() {
        let events = decode("page.views:1|c|#team:a,team:b,team:a");
        assert_eq!(
            events[0].attributes.get("team"),
            Some(&Value::Array(vec![Value::str("a"), Value::str("b")]))
        );
    }

    /// A bare token and a valued one that share a key differ in *form*, not just value, so they
    /// are not duplicates: both survive, in wire order, and `statsd_out` re-emits both forms.
    #[test]
    fn a_bare_and_a_valued_tag_sharing_a_key_keep_both_forms_in_wire_order() {
        let events = decode("page.views:1|c|#urgent,urgent:1");
        assert_eq!(
            events[0].attributes.get("urgent"),
            Some(&Value::Array(vec![Value::Bool(true), Value::str("1")]))
        );

        let events = decode("page.views:1|c|#urgent:1,urgent");
        assert_eq!(
            events[0].attributes.get("urgent"),
            Some(&Value::Array(vec![Value::str("1"), Value::Bool(true)]))
        );
    }

    /// An event line's `#` field goes through the same `insert_tags` a metric line's `|#` segment
    /// does, so it folds a repeat identically.
    #[test]
    fn a_repeated_tag_key_on_an_event_line_folds_into_an_array() {
        let event = only_log_event(decode("_e{5,4}:title|text|#k:a,k:b"));
        assert_eq!(
            event.attributes.get("k"),
            Some(&Value::Array(vec![Value::str("a"), Value::str("b")]))
        );
    }

    /// ...and so does a service check's, the third caller of that same function.
    #[test]
    fn a_repeated_tag_key_on_a_service_check_line_folds_into_an_array() {
        let events = decode("_sc|check|0|#k:a,k:b");
        assert_eq!(
            events[0].attributes.get("k"),
            Some(&Value::Array(vec![Value::str("a"), Value::str("b")]))
        );
    }

    /// A tag *literally named* `statsd.type` inside the `#` segment now folds like any other
    /// repeated key -- the carrier namespace buys no protection here, since `insert_tags` only
    /// ever sees the `#` payload. On egress an `Array` matches no `statsd_out` carrier arm and is
    /// filtered out of the tag segment uncounted, exactly as a wrong-typed carrier already is.
    /// (Deliberately a `c` line: on `ms`/`h`/`d` the decoder stamps its own `statsd.type` after
    /// the tags, overwriting whatever the `#` segment folded there.)
    #[test]
    fn a_tag_literally_named_statsd_type_folds_into_an_array_like_any_other() {
        let events = decode("page.views:1|c|#statsd.type:ms,statsd.type:h");
        assert_eq!(
            events[0].attributes.get("statsd.type"),
            Some(&Value::Array(vec![Value::str("ms"), Value::str("h")]))
        );
    }

    fn only_metric_event(events: Vec<Event>) -> Event {
        assert_eq!(events.len(), 1, "expected exactly one event");
        events.into_iter().next().unwrap()
    }

    #[test]
    fn multiple_lines_in_one_datagram() {
        let events = decode("a:1|c\nb:2|c\n");
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn malformed_line_does_not_drop_other_valid_metrics_in_same_datagram() {
        // A datagram is not atomic: StatsD clients routinely pack several independent metrics
        // into one packet, so one bad line must not discard unrelated valid ones alongside it.
        let events = decode("a:1|c\nbad\nb:2|c");
        assert_eq!(
            events.len(),
            2,
            "expected both 'a' and 'b' to survive the malformed middle line"
        );
        assert_eq!(intern("a"), only_metric(vec![events[0].clone()]).name);
        assert_eq!(intern("b"), only_metric(vec![events[1].clone()]).name);
    }

    #[test]
    fn unknown_type_is_rejected() {
        assert!(matches!(parse_err("x:1|zz"), CodecError::Malformed(_)));
    }

    #[test]
    fn missing_colon_is_rejected() {
        assert!(matches!(parse_err("nocolon|c"), CodecError::Malformed(_)));
    }

    /// `s` decodes to raw `SetMembers` now -- one member, a zero-copy slice of the datagram.
    /// Replaces this test's pre-W3 "not implemented" assertion.
    #[test]
    fn set_type_becomes_set_members() {
        let metric = only_metric(decode("unique.users:abc123|s"));
        match metric.kind {
            MetricKind::SetMembers(members) => {
                assert_eq!(members, vec![Bytes::from_static(b"abc123")])
            }
            other => panic!("expected SetMembers, got {other:?}"),
        }
    }

    /// `s` shares one `SetMembers` record per line, same as `ms`/`h`/`d`: every `:`-separated
    /// value on the line lands in the same event, in wire order.
    #[test]
    fn multi_value_set_produces_one_event_with_all_members() {
        let events = decode("unique.users:abc123:def456|s");
        assert_eq!(events.len(), 1, "s lines are one event per line, not per value");
        let metric = only_metric(events);
        match metric.kind {
            MetricKind::SetMembers(members) => {
                assert_eq!(
                    members,
                    vec![Bytes::from_static(b"abc123"), Bytes::from_static(b"def456")]
                );
            }
            other => panic!("expected SetMembers, got {other:?}"),
        }
    }

    #[test]
    fn blank_lines_are_skipped() {
        let events = decode("\n\na:1|c\n\n");
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn container_id_segment_becomes_an_attribute() {
        let events = decode("hits:1|c|c:abcdef0123456789");
        assert_eq!(
            events[0].attributes.get("statsd.container_id").and_then(|v| v.as_str()),
            Some("abcdef0123456789")
        );
    }

    /// `|c:<id>` applies to every metric type here, not only `c`/`g` as the DogStatsD spec
    /// itself restricts it to -- see the module doc's forward-compatibility note.
    #[test]
    fn container_id_segment_applies_to_every_metric_type() {
        for line in ["x:1|ms|c:cid", "x:1|s|c:cid", "x:1|g|c:cid"] {
            let events = decode(line);
            assert_eq!(
                events[0].attributes.get("statsd.container_id").and_then(|v| v.as_str()),
                Some("cid"),
                "expected statsd.container_id on {line:?}"
            );
        }
    }

    #[test]
    fn timestamp_segment_sets_the_event_timestamp_and_marker() {
        let events = decode("hits:1|c|T1700000000");
        let event = &events[0];
        assert_eq!(event.timestamp, 1_700_000_000 * 1_000_000_000);
        assert_eq!(event.attributes.get("statsd.timestamp"), Some(&Value::U64(1_700_000_000)));
    }

    #[test]
    fn malformed_timestamp_segment_rejects_only_that_line() {
        assert!(matches!(parse_err("hits:1|c|Tabc"), CodecError::Malformed(_)), "non-digit");
        assert!(matches!(parse_err("hits:1|c|T-5"), CodecError::Malformed(_)), "negative");
        assert!(
            matches!(parse_err("hits:1|c|T18446744073709551615"), CodecError::Malformed(_)),
            "seconds-to-nanoseconds overflow"
        );

        // A malformed |T must not take down the rest of the datagram -- same isolation contract
        // as any other malformed line.
        let events = decode("a:1|c|Tbad\nb:2|c");
        assert_eq!(events.len(), 1, "only the malformed-T line should be dropped");
        assert_eq!(intern("b"), only_metric(events).name);
    }

    /// `|c:`/`|T`/`@rate`/`#tags` combine freely, in any order, on the same line.
    #[test]
    fn container_id_timestamp_rate_and_tags_combine_in_any_order() {
        let orderings = [
            "x:100|ms|@0.5|#env:prod|c:abc123|T1700000000",
            "x:100|ms|c:abc123|T1700000000|@0.5|#env:prod",
            "x:100|ms|T1700000000|#env:prod|c:abc123|@0.5",
            "x:100|ms|#env:prod|@0.5|T1700000000|c:abc123",
        ];
        for line in orderings {
            let events = decode(line);
            assert_eq!(events.len(), 1, "expected one event for {line:?}");
            let event = &events[0];
            assert_eq!(event.timestamp, 1_700_000_000 * 1_000_000_000, "line: {line:?}");
            assert_eq!(
                event.attributes.get("statsd.container_id").and_then(|v| v.as_str()),
                Some("abc123"),
                "line: {line:?}"
            );
            assert_eq!(
                event.attributes.get("env").and_then(|v| v.as_str()),
                Some("prod"),
                "line: {line:?}"
            );
            assert_eq!(
                event.attributes.get("statsd.timestamp"),
                Some(&Value::U64(1_700_000_000)),
                "line: {line:?}"
            );
            match &event.metrics[0].kind {
                MetricKind::Samples(samples) => {
                    assert_eq!(samples.sample_rate, 0.5, "line: {line:?}")
                }
                other => panic!("expected Samples, got {other:?}"),
            }
        }
    }

    /// Companion to `only_metric`/`only_metric_event`: asserts an event batch decoded to exactly
    /// one log-only `Event` (no metrics, no span) and hands it back whole, so a test can inspect
    /// its attributes and timestamp alongside the log record.
    fn only_log_event(events: Vec<Event>) -> Event {
        assert_eq!(events.len(), 1, "expected exactly one event");
        let event = events.into_iter().next().unwrap();
        assert!(
            event.metrics.is_empty() && event.span.is_none(),
            "expected a log-only event, got {event:?}"
        );
        assert!(event.log.is_some(), "expected a log body");
        event
    }

    /// The DogStatsD docs' own canonical event example.
    #[test]
    fn dogstatsd_docs_example_event_decodes() {
        let events = decode(
            "_e{21,36}:An exception occurred|Cannot parse CSV file from 10.0.0.17|t:warning|#err_type:bad_file",
        );
        let event = only_log_event(events);
        let log = event.log.as_ref().unwrap();
        assert_eq!(log.message.as_str(), Some("Cannot parse CSV file from 10.0.0.17"));
        assert_eq!(log.severity, Some(Severity::Warn));
        assert_eq!(log.body_format, BodyFormat::Raw);
        assert_eq!(log.event_name, None);
        assert_eq!(
            event.attributes.get("statsd.event.title").and_then(|v| v.as_str()),
            Some("An exception occurred")
        );
        assert_eq!(
            event.attributes.get("statsd.event.alert_type").and_then(|v| v.as_str()),
            Some("warning")
        );
        assert_eq!(event.attributes.get("err_type").and_then(|v| v.as_str()), Some("bad_file"));
    }

    /// The DogStatsD docs' own canonical service check example.
    #[test]
    fn dogstatsd_docs_example_service_check_decodes() {
        let events =
            decode("_sc|Redis connection|2|#env:dev|m:Redis connection timed out after 10s");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert!(event.log.is_none() && event.span.is_none());
        assert_eq!(event.metrics.len(), 1);
        assert_eq!(intern("Redis connection"), event.metrics[0].name);
        assert!(matches!(event.metrics[0].kind, MetricKind::Gauge(v) if v == 2.0));
        assert_eq!(
            event.attributes.get("statsd.service_check.name").and_then(|v| v.as_str()),
            Some("Redis connection")
        );
        assert_eq!(event.attributes.get("statsd.service_check.status"), Some(&Value::U64(2)));
        assert_eq!(
            event.attributes.get("statsd.service_check.message").and_then(|v| v.as_str()),
            Some("Redis connection timed out after 10s")
        );
        assert_eq!(event.attributes.get("env").and_then(|v| v.as_str()), Some("dev"));
    }

    #[test]
    fn event_with_every_optional_field_decodes() {
        let events =
            decode("_e{5,4}:title|text|d:1700000000|h:host1|p:low|t:success|k:agg1|s:src1|#env:prod|c:cid1");
        let event = only_log_event(events);
        assert_eq!(event.timestamp, 1_700_000_000 * 1_000_000_000);
        let log = event.log.as_ref().unwrap();
        assert_eq!(log.message.as_str(), Some("text"));
        // `t:success` maps to `Severity::Info`, same as `t:info`.
        assert_eq!(log.severity, Some(Severity::Info));
        assert_eq!(
            event.attributes.get("statsd.event.title").and_then(|v| v.as_str()),
            Some("title")
        );
        assert_eq!(
            event.attributes.get("statsd.event.priority").and_then(|v| v.as_str()),
            Some("low")
        );
        assert_eq!(
            event.attributes.get("statsd.event.alert_type").and_then(|v| v.as_str()),
            Some("success")
        );
        assert_eq!(
            event.attributes.get("statsd.event.aggregation_key").and_then(|v| v.as_str()),
            Some("agg1")
        );
        assert_eq!(
            event.attributes.get("statsd.event.source_type").and_then(|v| v.as_str()),
            Some("src1")
        );
        assert_eq!(
            event.attributes.get("statsd.event.host").and_then(|v| v.as_str()),
            Some("host1")
        );
        assert_eq!(event.attributes.get("env").and_then(|v| v.as_str()), Some("prod"));
        assert_eq!(
            event.attributes.get("statsd.container_id").and_then(|v| v.as_str()),
            Some("cid1")
        );
        assert_eq!(event.attributes.get("statsd.timestamp"), Some(&Value::U64(1_700_000_000)));
    }

    /// TEXT may itself contain `|` and `:` (only the byte length decides where it ends), and its
    /// `\n` (backslash, `n`) escape unescapes to a real newline; the title is never unescaped.
    #[test]
    fn event_text_containing_pipe_colon_and_an_escaped_newline_decodes() {
        // Wire bytes: `a|b:c\nd` where `\n` is the two-byte escape sequence -- 8 bytes total.
        let events = decode("_e{1,8}:T|a|b:c\\nd");
        let event = only_log_event(events);
        let message = event.log.as_ref().unwrap().message.as_str().expect("message should be str");
        assert_eq!(message, "a|b:c\nd", "the escape sequence should become a real newline");
        assert!(message.contains('\n'), "expected a real newline byte in the decoded message");
    }

    /// `decode_into` must not trim trailing whitespace off an `_e{` line -- `TEXT_LEN` is
    /// authoritative for where `TEXT` ends, not a trim (module doc's "Trailing whitespace is real
    /// payload" note). Covers both a trailing space and a trailing tab.
    #[test]
    fn event_text_ending_in_whitespace_is_kept() {
        let events = decode("_e{1,2}:a|b ");
        let event = only_log_event(events);
        let message = event.log.as_ref().unwrap().message.as_str().expect("message should be str");
        assert_eq!(message, "b ", "the trailing space is real TEXT, not packet padding");

        let events = decode("_e{1,2}:a|b\t");
        let event = only_log_event(events);
        let message = event.log.as_ref().unwrap().message.as_str().expect("message should be str");
        assert_eq!(message, "b\t", "a trailing tab is kept the same way");
    }

    #[test]
    fn event_title_length_running_past_the_line_is_rejected() {
        assert!(matches!(parse_err("_e{100,4}:title|text"), CodecError::Malformed(_)));
    }

    #[test]
    fn event_missing_pipe_after_title_is_rejected() {
        // TITLE_LEN=5 correctly covers "title", but nothing separates it from "text".
        assert!(matches!(parse_err("_e{5,4}:titletext"), CodecError::Malformed(_)));
    }

    #[test]
    fn event_title_length_landing_mid_char_boundary_is_rejected() {
        // 'é' is 2 UTF-8 bytes; TITLE_LEN=1 lands inside it, not on a char boundary.
        assert!(matches!(parse_err("_e{1,4}:\u{e9}|text"), CodecError::Malformed(_)));
    }

    #[test]
    fn event_unknown_priority_or_alert_type_is_rejected() {
        assert!(matches!(parse_err("_e{5,4}:title|text|p:bogus"), CodecError::Malformed(_)));
        assert!(matches!(parse_err("_e{5,4}:title|text|t:bogus"), CodecError::Malformed(_)));
    }

    #[test]
    fn event_d_field_sets_the_timestamp_and_the_carrier() {
        let events = decode("_e{5,4}:title|text|d:1700000000");
        let event = only_log_event(events);
        assert_eq!(event.timestamp, 1_700_000_000 * 1_000_000_000);
        assert_eq!(event.attributes.get("statsd.timestamp"), Some(&Value::U64(1_700_000_000)));
    }

    #[test]
    fn event_container_id_becomes_an_attribute() {
        let events = decode("_e{5,4}:title|text|c:cid1");
        let event = only_log_event(events);
        assert_eq!(
            event.attributes.get("statsd.container_id").and_then(|v| v.as_str()),
            Some("cid1")
        );
    }

    #[test]
    fn service_check_d_field_sets_the_timestamp_and_the_carrier() {
        let events = decode("_sc|check|0|d:1700000000");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.timestamp, 1_700_000_000 * 1_000_000_000);
        assert_eq!(event.attributes.get("statsd.timestamp"), Some(&Value::U64(1_700_000_000)));
    }

    #[test]
    fn service_check_container_id_becomes_an_attribute() {
        let events = decode("_sc|check|0|c:cid2");
        assert_eq!(
            events[0].attributes.get("statsd.container_id").and_then(|v| v.as_str()),
            Some("cid2")
        );
    }

    /// `m:` is always the last field and consumes the rest of the line verbatim, so a message may
    /// itself contain `|`.
    #[test]
    fn service_check_message_containing_pipe_decodes_verbatim() {
        let events = decode("_sc|check|0|m:a|b|c");
        assert_eq!(
            events[0].attributes.get("statsd.service_check.message").and_then(|v| v.as_str()),
            Some("a|b|c")
        );
    }

    /// `decode_into` must not trim a trailing space off an `_sc|` line either -- `m:` consumes the
    /// rest of the line verbatim, so a trailing space is real message content (module doc's
    /// "Trailing whitespace is real payload" note).
    #[test]
    fn service_check_message_trailing_whitespace_is_kept() {
        let events = decode("_sc|check|0|m:disk almost full ");
        assert_eq!(
            events[0].attributes.get("statsd.service_check.message").and_then(|v| v.as_str()),
            Some("disk almost full ")
        );
    }

    #[test]
    fn service_check_out_of_range_or_non_numeric_status_is_rejected() {
        for status in ["4", "-1", "abc"] {
            let line = format!("_sc|check|{status}");
            assert!(
                matches!(parse_err(&line), CodecError::Malformed(_)),
                "expected status {status:?} to be rejected"
            );
        }
    }

    #[test]
    fn service_check_empty_name_is_rejected() {
        assert!(matches!(parse_err("_sc||0"), CodecError::Malformed(_)));
    }

    /// Only `_e{`/`_sc|` are special-cased sigils -- any other `_`-prefixed line falls through to
    /// the generic `<name>:<value>|<type>` grammar unchanged, matching pre-W6 behavior and
    /// Datadog's own DogStatsD parser (which reserves exactly these two prefixes, nothing
    /// broader). `_total.count` is a legal statsd name that merely happens to start with `_`.
    #[test]
    fn an_underscore_prefixed_metric_name_still_decodes_as_a_metric() {
        let metric = only_metric(decode("_total.count:1|c"));
        assert_eq!(intern("_total.count"), metric.name);
        assert!(
            matches!(metric.kind, MetricKind::Sum(logit_core::Sum { value, .. }) if value == 1.0)
        );
    }

    /// `_x|1` isn't `_e{`/`_sc|`, so it falls through to the generic grammar the same as any other
    /// `_`-prefixed line -- and is rejected there, same as any other line with no `:`, not because
    /// of its leading sigil.
    #[test]
    fn an_underscore_prefixed_line_without_a_colon_is_rejected_for_missing_colon() {
        assert!(matches!(parse_err("_x|1"), CodecError::Malformed(_)));
    }

    /// A datagram mixing a counter, an event, and a service check decodes all three, in wire
    /// order -- `parse_line`'s dispatch on the leading sigil doesn't disturb line ordering.
    #[test]
    fn a_packed_datagram_mixing_a_counter_an_event_and_a_service_check_decodes_all_three_in_order()
    {
        let events = decode("hits:1|c\n_e{5,4}:title|text\n_sc|check|0");
        assert_eq!(events.len(), 3);
        assert!(events[0].metrics.len() == 1 && events[0].log.is_none(), "expected the counter");
        assert!(events[1].log.is_some(), "expected the event");
        assert!(
            events[2].metrics.len() == 1 && events[2].log.is_none(),
            "expected the service check"
        );
    }

    /// Mirrors `syslog.rs`'s own `local_addr`-after-`bind` property (`crates/logit-inputs/src/
    /// udp.rs`'s `bind_then_run_delivers_a_real_datagram`): no address before `bind()`, a real
    /// one after.
    #[tokio::test]
    async fn local_addr_is_available_after_bind() {
        let mut input = StatsdInput::new("127.0.0.1:0");
        assert_eq!(input.local_addr(), None, "no address before bind()");

        input.bind().await.expect("binding an ephemeral port should succeed");
        let addr = input.local_addr().expect("bind() should leave a real address behind");
        assert_eq!(addr.ip().to_string(), "127.0.0.1");
    }
}
