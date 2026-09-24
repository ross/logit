//! statsd / DogStatsD-tagged metrics over UDP or TCP: the input half of the `statsd_in ->
//! statsd_out` lossless-relay pair (`docs/adr/lossless-transit.md`; the mirror is
//! `docs/adr/statsd-output.md`).
//!
//! ## Transports
//!
//! One component, two shared drivers, chosen by `transport:`. This type is the decoder choice plus
//! the builder surface `logit-cli::pipeline` and these tests use, as
//! [`crate::syslog::SyslogInput`] and [`crate::graphite::GraphiteInput`] are.
//!
//! | `transport:` | Driver | What it brings |
//! |---|---|---|
//! | `udp` (the default) | [`UdpListener<StatsdDecoder>`](crate::udp::UdpListener) | the read/decode split, the receive queue, datagram->batch assembly, `SO_RCVBUF` (`docs/adr/decoupled-listener-io.md`); the whole `receive:` block applies |
//! | `tcp` | [`TcpListener<StatsdDecoder>`](crate::tcp::TcpListener) | an accept loop, the 1024-connection cap, a per-connection decoder clone and batch accumulator, the first-byte deadline, and, with a `tls:` block, TLS termination (`docs/adr/syslog-tcp-ingress-and-tls.md`) |
//!
//! A TCP listener has **no [`ReceiveQueue`](crate::udp::ReceiveQueue)**: TCP's flow control is
//! the backpressure, and ADR `decoupled-listener-io` exists for UDP's silent drops, which a stream
//! cannot have. So only `receive:`'s batch-assembly fields (`batch_max_events`,
//! `batch_max_bytes`, `batch_flush_interval`) and `shutdown_grace` apply to one; graph rule 17
//! rejects the queue fields and `read_batch` by name.
//!
//! There is no statsd-over-TCP specification. The Etsy reference server, the Datadog agent and
//! every TCP-capable client speak the line grammar below, LF-delimited on a stream; that is what
//! this listener accepts and what `statsd_out`'s `transport: tcp` emits.
//!
//! ## Framing
//!
//! **LF-delimited lines, always**: [`FramingMode::Lines`] with [`Oversize::DrainToNextLine`],
//! never [`FramingMode::Rfc6587Auto`]. That mode reads a leading ASCII digit as an RFC 6587 octet
//! count, which is right for syslog (every non-transparent message starts `<`) and wrong here:
//! `1.hits:1|c` is an ordinary statsd line, and latching octet counting on it would reframe the
//! whole connection. `a_tcp_line_starting_with_a_digit_is_not_read_as_an_octet_count` pins this.
//!
//! **The LF is the completeness signal, at the end of the stream too.** An unterminated final line
//! on a clean close is not emitted: it is dropped and counted
//! `logit.input.frames.dropped{reason="truncated"}` (diagnostic `framing_error`), the same as an
//! abrupt close or a shutdown mid-line. A whitespace-only remainder (trailing padding, a bare `CR`)
//! is not counted, since nothing was lost. Emitting a half-line would turn a sender dying
//! mid-write into a plausible datapoint with a truncated name or value. This differs from
//! `syslog_in`, because RFC 6587 §3.4.2 permits a terminator-less final message and statsd has no
//! such licence.
//!
//! Oversize is **recoverable**: a line past the driver's 64 KiB
//! [`MAX_FRAME_BYTES`](crate::tcp::MAX_FRAME_BYTES) is dropped, counted once as
//! `logit.input.frames.dropped{reason="oversize"}`, and the connection resynchronizes at the next
//! `LF`. `graphite_in` makes the same call for carbon plaintext
//! (`docs/adr/graphite-carbon-relay.md`): one pathological line must not cost every other metric
//! on the connection, and an LF-delimited stream has an unambiguous resync point. There is **no
//! `max_line_bytes` field**: unlike carbon, no statsd server has such a knob for an operator to
//! match.
//!
//! ## Telemetry and diagnostics
//!
//! All of it comes from the shared drivers; this component adds none. Under `transport: udp`:
//! `logit.input.datagrams`/`.datagram.bytes`, the `logit.component.receive.*` queue gauges,
//! `logit.input.receive_buffer.bytes`, and the driver's `bad_datagram` for a whole-datagram decode
//! failure. Under `transport: tcp`: `logit.input.connections` (gauge),
//! `logit.input.connections.rejected{reason="limit"}`, `logit.input.frames`/`.frame.bytes` (one
//! frame is one statsd line), `logit.input.frames.dropped{reason="oversize"|"truncated"}`,
//! `logit.component.receive.flushed{reason}` from the per-connection batch assembly, and
//! `framing_error`/`connection_error` diagnostics.
//!
//! The decoder's own `bad_line` diagnostic is the same under both: **a malformed line is skipped
//! and reported as `bad_line`, and the rest of its datagram still decodes.** It throttles per
//! listener, not per connection, because every connection's decoder clone shares one set of
//! [`Diagnostics`] counts (`logit_core::Diagnostics`' type doc). The driver's `bad_frame` fires
//! only for the one whole-frame failure [`StatsdDecoder::decode_into`] returns, a frame that is
//! not valid UTF-8.
//!
//! ## Grammar
//!
//! A superset covering plain statsd and the DogStatsD tag/container-id/timestamp extensions:
//!
//! ```text
//! <name>:<value>[:<value>...]|<type>[|@<sample-rate>][|#<tag>[:<value>],...][|c:<container-id>][|T<unix-seconds>][|<ignored>]
//! ```
//!
//! `<type>` is one of:
//!
//! - `c` (counter): one [`Event`] per value, extrapolated (`value / sample_rate`) into
//!   [`logit_core::MetricKind::Sum`].
//! - `g` (gauge): one `Event` per value. Unsigned is a [`logit_core::MetricKind::Gauge`]; a leading
//!   `+`/`-` is an unresolved [`logit_core::MetricKind::GaugeDelta`]
//!   (`docs/adr/relative-gauge-adjustments.md`). Sample rate is ignored: a gauge value is not a
//!   count to extrapolate.
//! - `ms`/`h`/`d` (timing/histogram/distribution): **one `Event` per line**, every value in one raw
//!   [`logit_core::MetricKind::Samples`] with `sample_rate` carried verbatim. No extrapolation and
//!   no sketching here: under `docs/adr/lossless-transit.md`'s "summarization is opt-in and named"
//!   rule only `aggregate` sketches. The weighting bound lives there too: `Samples::weight` clamps
//!   `round(1 / sample_rate)` to `Samples::MAX_WEIGHT` (1000), and `aggregate` counts a clamp as
//!   `logit.transform.samples.weight_clamped` with a `sample_rate_clamped` diagnostic. The wire
//!   type letter survives as the `statsd.type` attribute (rule (b) of
//!   `docs/adr/lossless-transit.md`), since all three land on the same `Samples` shape.
//! - `s` (set): **one `Event` per line**, every value in one
//!   [`logit_core::MetricKind::SetMembers`], each member a zero-copy `Bytes` slice of the datagram,
//!   in wire order. Only `aggregate` turns these into a [`logit_core::HyperLogLog`]
//!   ([`logit_core::MetricKind::Set`]). Sample rate is ignored, as for `g`.
//!
//! Multiple values on a `c`/`g` line become independent events sharing type, sample rate and tags
//! (gauge sign is per value, so one event would lose which value had which sign). A
//! `ms`/`h`/`d`/`s` line's values stay together on one event. A datagram may hold many
//! newline-separated lines.
//!
//! **`|c:<container-id>` and `|T<unix-seconds>` apply to every metric type**, not only the `c`/`g`
//! the DogStatsD spec restricts them to (`docs/design/telemetry-landscape.md`): a
//! forward-compatible superset. `|c:<id>` (v1.2+; v1.4+'s `ci-`/`in-`-prefixed forms land
//! verbatim) stamps `statsd.container_id: Value::Str`, a zero-copy datagram slice. `|T<secs>` sets
//! [`Event::timestamp`] to `secs * 1_000_000_000` in place of the receipt time `decode_into`'s
//! `received_at` supplies, and stamps `statsd.timestamp: Value::U64(secs)`, the raw wire value.
//! The carrier is what survives a stage that rebuilds `Event::timestamp` (`aggregate`'s flush), and
//! it tells a wire timestamp from a receipt-time one. A non-digit or overflowing `|T` rejects only
//! that line as `bad_line`. Both attributes are protocol-namespaced carriers
//! (`docs/adr/lossless-transit.md`). Every other unrecognized `|` segment is accepted and ignored,
//! for forward compatibility.
//!
//! A line is rejected as `bad_line` when it has no `:` or an empty name, no `|<type>`, an unknown
//! type, a `c`/`g`/`ms`/`h`/`d` value that doesn't parse or isn't finite (`NaN`/`inf` parse as
//! `f64`; `s` members are opaque and never parsed), or an `@rate` that doesn't parse, isn't
//! finite, or is outside `(0, 1]` (checked even on a `g`/`s` line, which ignores the rate).
//!
//! ## DogStatsD tags
//!
//! A `|#` segment is a **list** of `key[:value]` tokens, not a map. The Datadog agent keeps every
//! token and dedupes only exact duplicates, so `#team:a,team:b` is two live tags (a query grouping
//! by `team` places the point in both groups) while `#team:a,team:a` is one. `insert_tags` does
//! the same: **a repeated tag key folds into a [`logit_core::Value::Array`] in wire order**
//! (`#team:a,team:b` -> `team: Array[Str("a"), Str("b")]`, three occurrences -> three elements),
//! and **an exact duplicate token is deduped** (`#team:a,team:a` -> `Str("a")`, `#urgent,urgent`
//! -> `Bool(true)`). **A one-element `Array` is never produced**, so a non-repeated tag decodes to
//! a scalar. A valueless tag is `Bool(true)`. [`crate::syslog`]'s `insert_param` applies the same
//! fold to a repeated RFC 5424 PARAM-NAME (`docs/adr/syslog-structured-data-convention.md`): a
//! plain `AttrMap::insert` per token would let the last token win, which is loss under
//! `docs/adr/lossless-transit.md`.
//!
//! A bare token and a valued one that share a key are not duplicates; **both survive, in order**:
//! `#urgent,urgent:1` -> `urgent: Array[Bool(true), Str("1")]` (re-emitted by `statsd_out` as
//! `urgent,urgent:1`) and `#urgent:1,urgent` -> `Array[Str("1"), Bool(true)]` ->
//! `urgent:1,urgent`. Array order is wire order; the attribute map stays sorted by `Symbol`, so tag
//! key order doesn't change. Element values are zero-copy `slice_of` slices, like a scalar tag
//! value.
//!
//! **The fold applies to the `#` payload only.** Every `#` segment on a line unions into the same
//! attribute map (`|#a:1|#a:2` folds as `|#a:1,a:2` would), while `@`, `|c:` and `|T` are
//! last-segment-wins. A repeated `|T`/`|c:`/type therefore never reaches `insert_tags`. A tag
//! **literally named** `statsd.type` (or any `statsd.*` carrier key) inside `#` does, and can
//! decode to an `Array`. On egress it matches no `statsd_out` carrier arm (each expects a
//! `Value::Str`/`Value::U64`) and is filtered out of the tag segment uncounted, as any wrong-typed
//! carrier is. On a `ms`/`h`/`d` line the decoder's own `statsd.type` stamp runs after the tags
//! and overwrites it.
//!
//! ## DogStatsD events and service checks
//!
//! Two more line shapes, picked out by their leading sigil before the grammar above applies: `_e{`
//! (an **event**) and `_sc|` (a **service check**). Nothing else about a leading `_` is special:
//! `_total.count:1|c` falls through to the metric grammar, since `_` is a legal name byte and
//! Datadog's own parser reserves only these two sigils.
//!
//! **Trailing whitespace is payload on both shapes, so `decode_into` never trims it off them.**
//! Every line has `\r` and leading whitespace trimmed; trailing whitespace is trimmed too, except
//! on a line starting `_e{` or `_sc|`. `_e{TITLE_LEN,TEXT_LEN}`'s lengths are authoritative, so a
//! trim would either shrink the line under a correct length (rejecting a legal event) or change
//! `TEXT`. `_sc|`'s `m:` consumes the rest of the line verbatim, so a trim would drop message
//! bytes with no error. `event_text_ending_in_whitespace_is_kept` and
//! `service_check_message_trailing_whitespace_is_kept` pin this.
//!
//! **Event**: `_e{<TITLE_LEN>,<TEXT_LEN>}:<TITLE>|<TEXT>|d:<secs>|h:<hostname>|p:<normal|low>|
//! t:<info|success|warning|error>|k:<aggregation_key>|s:<source_type_name>|#<tags>|
//! c:<container_id>`. `TITLE_LEN`/`TEXT_LEN` are byte lengths as on the wire and decide the split,
//! since `TEXT` may contain `|` and `:`. The line is rejected when a length runs past the line,
//! lands mid-UTF-8-char (checked via `str::get`, so never a panic), or no `|` follows the title,
//! when the `{a,b}` header is malformed, or when a `t:`/`p:` value is unrecognized. It decodes to
//! one [`Event::log`]:
//!
//! - `message` is `TEXT` with its `\n` (backslash, `n`) escape unescaped to a newline; zero-copy
//!   when there is nothing to unescape. The title is never unescaped.
//! - `severity` maps `t:error`/`t:warning`/`t:success`/`t:info` to `Error`/`Warn`/`Info`/`Info`,
//!   and is `None` when `t:` is absent.
//! - `event_name` stays `None`: a title is free text, and interning it would grow the global
//!   interner without bound.
//! - Attributes (all `Value::Str`, zero-copy where possible): `statsd.event.title` (always),
//!   `statsd.event.priority` (`p:`, raw), `statsd.event.alert_type` (`t:`, raw),
//!   `statsd.event.aggregation_key` (`k:`), `statsd.event.source_type` (`s:`),
//!   `statsd.event.host` (`h:`), each only if present, plus `statsd.timestamp`,
//!   `statsd.container_id` and `#tags` as on a metric line. `d:<secs>` plays `|T`'s role (same
//!   checked parse, same event timestamp and carrier); `|T` itself is an unrecognized field here
//!   and is ignored.
//!
//! **Service check**: `_sc|<NAME>|<STATUS>|d:<secs>|h:<hostname>|#<tags>|c:<container_id>|
//! m:<message>`. `NAME` must be non-empty and `STATUS` an integer `0..=3`
//! (OK/WARNING/CRITICAL/UNKNOWN), or the line is rejected. `m:`, when present, is always last and
//! consumes the rest of the line verbatim, so a message may contain `|`; other fields come in any
//! order before it. It decodes to one [`Event::metric`], `MetricKind::Gauge(status as f64)` under
//! the check's name (interned, like a metric name), with attributes `statsd.service_check.name`
//! (always, `Value::Str`: `MetricRecord` has nowhere else to carry it),
//! `statsd.service_check.status` (always, `Value::U64`), `statsd.service_check.message` (`m:`,
//! verbatim) and `statsd.service_check.host` (`h:`), plus `statsd.timestamp`,
//! `statsd.container_id` and `#tags` as above.
//!
//! **Tag values, `|c:<id>`, and set members are zero-copy slices of the datagram**, like every
//! field [`crate::syslog`] extracts: `slice_of` rebuilds each `Bytes` by pointer arithmetic into
//! the datagram passed to [`StatsdDecoder::decode_into`], rather than copying through
//! `impl From<&str> for Value`. Tag keys and the metric name don't need this: both only reach
//! [`logit_core::interner::intern`], which copies into its own table regardless.

use crate::tcp::{FramingMode, Oversize, TcpListener, TcpListenerConfig, TlsServerSettings};
use crate::udp::{UdpListener, UdpListenerConfig};
use crate::Input;
use bytes::Bytes;
use logit_core::{
    interner::{intern, KeyCache},
    AttrMap, BodyFormat, Diagnostics, Event, LogRecord, MetricKind, MetricRecord, Resource,
    Samples, Scope, Severity, Symbol, Telemetry, Value,
};
use logit_pipeline::Fanout;
use logit_proto::{CodecError, Decoder};
use std::path::Path;
use std::sync::{Arc, LazyLock};
use tokio::sync::watch;

/// Which driver a [`StatsdInput`] wraps, chosen once by `transport:`. An enum rather than a
/// `Box<dyn Input>` so each arm's concrete builders ([`TcpListener::with_tls`],
/// [`UdpListener::with_config`]) stay reachable; [`crate::syslog::SyslogInput`] does the same.
enum Inner {
    Udp(UdpListener<StatsdDecoder>),
    Tcp(TcpListener<StatsdDecoder>),
}

/// The `statsd_in` listener: a [`StatsdDecoder`] over [`UdpListener`] or [`TcpListener`].
///
/// All transport behavior lives in the drivers; see this module's "Transports" section.
pub struct StatsdInput {
    inner: Inner,
}

impl StatsdInput {
    /// A UDP listener, the default transport.
    pub fn new(bind: impl Into<String>) -> Self {
        Self {
            inner: Inner::Udp(UdpListener::new(
                bind,
                StatsdDecoder::new(Arc::new(Resource::default())),
                UdpListenerConfig::default(),
            )),
        }
    }

    /// A TCP listener (`transport: tcp`), plaintext until [`Self::with_tls`] is called.
    ///
    /// Framing is fixed here, at construction: [`FramingMode::Lines`], never
    /// [`FramingMode::Rfc6587Auto`], with oversize draining to the next `LF` (this module's
    /// "Framing" section). Unlike `graphite_in`, framing needn't wait for `bind()`: there is no
    /// `max_line_bytes` field for a later builder to set.
    pub fn tcp(bind: impl Into<String>) -> Self {
        Self {
            inner: Inner::Tcp(
                TcpListener::new(
                    bind,
                    StatsdDecoder::new(Arc::new(Resource::default())),
                    TcpListenerConfig::default(),
                )
                .with_framing(
                    FramingMode::Lines { oversize: Oversize::DrainToNextLine },
                    crate::tcp::MAX_FRAME_BYTES,
                ),
            ),
        }
    }

    /// Attaches a component id to the driver's diagnostics and to the wrapped [`StatsdDecoder`]'s.
    ///
    /// Both must carry it: the driver reports transport failures (`bad_datagram` on UDP;
    /// `framing_error`/`bad_frame`/`connection_error` on TCP) and the decoder reports `bad_line`.
    /// Miss one and that class of failure reports under no component id with telemetry disabled.
    /// On TCP every connection clones this decoder, sharing its throttle counts, so `bad_line`
    /// throttles per listener.
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
    /// TCP, which `Fanout`-level `events.sent` can't tell apart from one busy client.
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
    /// Two transport-specific setters, as [`crate::syslog::SyslogInput::with_receive`] has,
    /// because the configs aren't interchangeable: a TCP listener has no receive queue (graph rule
    /// 17), so one setter would have to decide at runtime what to do with a queue bound it can't
    /// honour. [`Self::with_tcp_receive`] is the counterpart.
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

    /// Terminates TLS on a TCP listener (`tls:`); paths in `settings` resolve against `base_dir`.
    ///
    /// Fails on a UDP listener: DTLS is out of scope (`docs/adr/syslog-tcp-ingress-and-tls.md`'s
    /// Alternatives) and no statsd client speaks it. Graph rule 43 is what an operator sees; this
    /// arm backstops a caller that skipped validation.
    pub fn with_tls(
        mut self,
        settings: &TlsServerSettings,
        base_dir: &Path,
    ) -> anyhow::Result<Self> {
        self.inner = match self.inner {
            Inner::Tcp(listener) => Inner::Tcp(listener.with_tls(settings, base_dir)?),
            Inner::Udp(_) => anyhow::bail!(
                "statsd_in: 'tls:' needs 'transport: tcp' -- TLS is defined over a byte stream, \
                 and DTLS is out of scope (docs/adr/syslog-tcp-ingress-and-tls.md)"
            ),
        };
        Ok(self)
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
impl Input for StatsdInput {
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

/// Decodes statsd/DogStatsD bytes into events; testable without a socket.
///
/// `Clone` because [`TcpListener`] gives every connection its own decoder
/// (`crates/logit-inputs/src/tcp.rs`'s "`D: Clone` is load-bearing"). A clone shares the one
/// `Arc<Resource>`, which must stay shared: `logit_pipeline::BatchAccumulator::absorb` keys on
/// `Arc::ptr_eq`, so a resource per connection would stop two connections' events sharing a batch.
/// It also shares its `Diagnostics` throttle counts, so `bad_line` throttles listener-wide.
///
/// On TCP the driver hands this one already-delimited line, so [`Self::decode_into`]'s `\n` split
/// is a single iteration, not a second framing pass. That is why no `with_line_splitting` switch is
/// needed, unlike [`crate::syslog::SyslogDecoder`]: an octet-counted syslog frame may contain a
/// `\n`, and a statsd line never can.
#[derive(Clone)]
pub struct StatsdDecoder {
    resource: Arc<Resource>,
    diag: Diagnostics,
    /// Tag keys memoised `&str -> Symbol`: a client's tag names repeat on every line, so after the
    /// first each is a `memcmp`, not an interner probe. The `statsd.*` carrier keys are in `KEYS`.
    keys: KeyCache,
}

impl StatsdDecoder {
    pub fn new(resource: Arc<Resource>) -> Self {
        Self { resource, diag: Diagnostics::default(), keys: KeyCache::new() }
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    /// Test-only: confirms `StatsdInput::with_diagnostics` reached this decoder, not only the
    /// driver.
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
            // Trailing whitespace is payload on an `_e{`/`_sc|` line (module doc, "DogStatsD
            // events and service checks"), so only other lines are trimmed at the end.
            let line = line.trim_end_matches('\r').trim_start();
            let line = if line.starts_with("_e{") || line.starts_with("_sc|") {
                line
            } else {
                line.trim_end()
            };
            if line.is_empty() {
                continue;
            }
            // Per-line isolation: clients pack independent metrics into one datagram, so a bad
            // line is reported and skipped without discarding the others.
            match parse_line(&bytes, text, line, received_at, &mut self.keys) {
                Ok(mut line_events) => out.append(&mut line_events),
                Err(err) => {
                    self.diag.warn_throttled("bad_line", err);
                }
            }
        }
        // statsd has no instrumentation scope.
        Ok((self.resource.clone(), None))
    }
}

/// Rebuilds `sub` as a `Bytes` sharing `bytes`'s allocation, by pointer arithmetic.
///
/// `sub` must be a `&str` slice of `text`, and `text` the `str::from_utf8` view of `bytes`; every
/// caller gets `sub` by slicing (`split`, `split_once`, `trim*`, indexing), never by copying.
/// Mirrors [`crate::syslog`]'s `slice_of`. There is no fallback copy, unlike
/// `logit-transforms::json::borrowed_str_bytes`: nothing sliced here is ever unescaped first.
fn slice_of(bytes: &Bytes, text: &str, sub: &str) -> Bytes {
    let text_start = text.as_ptr() as usize;
    let sub_start = sub.as_ptr() as usize;
    let start = sub_start - text_start;
    bytes.slice(start..start + sub.len())
}

/// The `statsd.*` carrier keys, interned once per process so each line pays a sorted
/// `insert_sym`, not an interner hash and shard lock. A `LazyLock` rather than a decoder field
/// because the parsers are free functions; `KEYS.x` is one acquire load after first use.
static KEYS: LazyLock<StatsdKeys> = LazyLock::new(|| StatsdKeys {
    container_id: intern("statsd.container_id"),
    timestamp: intern("statsd.timestamp"),
    type_: intern("statsd.type"),
    event_title: intern("statsd.event.title"),
    event_host: intern("statsd.event.host"),
    event_priority: intern("statsd.event.priority"),
    event_aggregation_key: intern("statsd.event.aggregation_key"),
    event_source_type: intern("statsd.event.source_type"),
    service_check_name: intern("statsd.service_check.name"),
    service_check_status: intern("statsd.service_check.status"),
    service_check_host: intern("statsd.service_check.host"),
});

struct StatsdKeys {
    container_id: Symbol,
    timestamp: Symbol,
    type_: Symbol,
    event_title: Symbol,
    event_host: Symbol,
    event_priority: Symbol,
    event_aggregation_key: Symbol,
    event_source_type: Symbol,
    service_check_name: Symbol,
    service_check_status: Symbol,
    service_check_host: Symbol,
}

/// Folds a `#<tag>[:<value>],...` payload (the text after `#`) into `attributes`, for a metric
/// line, an event, or a service check alike.
///
/// A valued tag is a zero-copy [`slice_of`] the datagram; a bare one is `Value::Bool(true)`. A
/// repeated key folds into a `Value::Array` in wire order and an exact duplicate token is deduped;
/// the module doc's "DogStatsD tags" section has the full rule and what it leaves alone. The
/// remove-then-insert per token is two binary searches over the sorted map, the same two-step
/// [`crate::syslog`]'s `insert_param` uses.
fn insert_tags(
    attributes: &mut AttrMap,
    bytes: &Bytes,
    text: &str,
    tags: &str,
    keys: &mut KeyCache,
) {
    for tag in tags.split(',').filter(|t| !t.is_empty()) {
        let (key, value) = match tag.split_once(':') {
            Some((k, v)) => (k, Value::Str(slice_of(bytes, text, v))),
            None => (tag, Value::Bool(true)),
        };
        let key = keys.get_or_intern(key);
        let merged = match attributes.remove_sym(key) {
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
        attributes.insert_sym(key, merged);
    }
}

/// Exact-token equality for [`insert_tags`]'s dedupe: `Str`/`Str` by bytes (so two offsets into
/// one datagram compare on content), `Bool`/`Bool` by value, anything else unequal. The catch-all
/// is what keeps both forms of `#urgent,urgent:1`. Not `Value`'s `PartialEq`, because the contract
/// is the agent's exact-token rule, not general value equality.
fn tag_element_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Str(a), Value::Str(b)) => a == b,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        _ => false,
    }
}

/// Stamps `statsd.container_id`, a protocol-namespaced carrier (`docs/adr/lossless-transit.md`),
/// as a zero-copy datagram slice, for a metric line's `|c:` and an event's or service check's `c:`.
fn insert_container_id(attributes: &mut AttrMap, bytes: &Bytes, text: &str, container_id: &str) {
    attributes.insert_sym(KEYS.container_id, Value::Str(slice_of(bytes, text, container_id)));
}

/// Parses a `|T<unix-seconds>`/`d:<unix-seconds>` value into `(nanos, secs)`: `nanos` for
/// `Event::timestamp`, `secs` for the `statsd.timestamp` carrier.
///
/// A non-digit, or seconds whose nanosecond conversion overflows `i64`, rejects the line rather
/// than falling back to receipt time.
fn parse_wire_seconds(secs: &str, line: &str) -> Result<(i64, u64), CodecError> {
    let malformed = || CodecError::Malformed(format!("malformed statsd line: {line:?}"));
    let secs: u64 = secs.parse().map_err(|_| malformed())?;
    let nanos = secs
        .checked_mul(1_000_000_000)
        .and_then(|n| i64::try_from(n).ok())
        .ok_or_else(malformed)?;
    Ok((nanos, secs))
}

/// Parses one line. `bytes`/`text` are the whole datagram and its `&str` view, threaded down for
/// [`slice_of`]; `line` must be a slice of `text`.
fn parse_line(
    bytes: &Bytes,
    text: &str,
    line: &str,
    timestamp: i64,
    keys: &mut KeyCache,
) -> Result<Vec<Event>, CodecError> {
    // Only these two sigils are special; a legal `_`-prefixed metric name falls through.
    if line.starts_with("_e{") {
        return parse_event(bytes, text, line, timestamp, keys).map(|event| vec![event]);
    }
    if line.starts_with("_sc|") {
        return parse_service_check(bytes, text, line, timestamp, keys).map(|event| vec![event]);
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
    // Receipt time unless `|T<secs>` overrides it.
    let mut line_timestamp = timestamp;
    for extra in segments {
        if let Some(rate) = extra.strip_prefix('@') {
            let parsed: f64 = rate.parse().map_err(|_| malformed())?;
            // A probability: finite and in (0, 1]. `f64::parse` accepts NaN/inf/zero/negative
            // text, which would become a non-finite or negative counter (or divide by zero).
            if !parsed.is_finite() || parsed <= 0.0 || parsed > 1.0 {
                return Err(malformed());
            }
            sample_rate = parsed;
        } else if let Some(tags) = extra.strip_prefix('#') {
            insert_tags(&mut attributes, bytes, text, tags, keys);
        } else if let Some(container_id) = extra.strip_prefix("c:") {
            // Carried verbatim, `ci-`/`in-` prefixes included, on every metric type.
            insert_container_id(&mut attributes, bytes, text, container_id);
        } else if let Some(secs) = extra.strip_prefix('T') {
            // DogStatsD v1.3+ point timestamp, accepted on every type.
            let (nanos, secs) = parse_wire_seconds(secs, line)?;
            line_timestamp = nanos;
            // `statsd_out` emits `|T` from this carrier, not `event.timestamp`, which a stage
            // like `aggregate` rebuilds; so a `|T` the wire never sent can't be fabricated.
            attributes.insert_sym(KEYS.timestamp, Value::U64(secs));
        }
        // Any other segment is ignored, for forward compatibility.
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
            // One raw `Samples` per line, unsketched, `sample_rate` verbatim (module doc). Pushed
            // straight into its inline `SmallVec` (`SAMPLES_INLINE`, 19 values) so a line up to
            // that size doesn't allocate an intermediate `Vec`.
            let mut samples = Samples::default();
            for raw_value in values_part.split(':') {
                samples.values.push(parse_finite_value(raw_value, "timing/histogram", line)?);
            }
            samples.sample_rate = sample_rate;
            let mut attrs = attributes.clone();
            // Stamped after the tags, so it overwrites a `#statsd.type` tag.
            attrs.insert_sym(KEYS.type_, Value::Str(slice_of(bytes, text, type_part)));
            let kind = MetricKind::Samples(samples);
            Ok(vec![Event::metric(line_timestamp, attrs, MetricRecord::new(intern(name), kind))])
        }
        "s" => {
            // `sample_rate` is ignored: a set member is not a count to extrapolate.
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

/// Parses a DogStatsD event line starting `_e{` (module doc, "DogStatsD events and service
/// checks").
fn parse_event(
    bytes: &Bytes,
    text: &str,
    line: &str,
    timestamp: i64,
    keys: &mut KeyCache,
) -> Result<Event, CodecError> {
    let malformed = || CodecError::Malformed(format!("malformed dogstatsd event: {line:?}"));

    let header_rest = line.strip_prefix("_e{").ok_or_else(malformed)?;
    let (header, after_header) = header_rest.split_once('}').ok_or_else(malformed)?;
    let (title_len, text_len) = header.split_once(',').ok_or_else(malformed)?;
    let title_len: usize = title_len.parse().map_err(|_| malformed())?;
    let text_len: usize = text_len.parse().map_err(|_| malformed())?;
    let after_header = after_header.strip_prefix(':').ok_or_else(malformed)?;

    // `str::get` is `None` both past the end and off a char boundary, so a bad length rejects the
    // line; the later indexing reuses the lengths `get` already validated and cannot panic.
    let title = after_header.get(..title_len).ok_or_else(malformed)?;
    let after_title = after_header[title_len..].strip_prefix('|').ok_or_else(malformed)?;
    let raw_text = after_title.get(..text_len).ok_or_else(malformed)?;
    let after_text = &after_title[text_len..];

    let mut attributes = AttrMap::new();
    attributes.insert_sym(KEYS.event_title, Value::Str(slice_of(bytes, text, title)));

    let mut line_timestamp = timestamp;
    let mut severity = None;

    if !after_text.is_empty() {
        let fields = after_text.strip_prefix('|').ok_or_else(malformed)?;
        for field in fields.split('|') {
            if let Some(tags) = field.strip_prefix('#') {
                insert_tags(&mut attributes, bytes, text, tags, keys);
            } else if let Some(container_id) = field.strip_prefix("c:") {
                insert_container_id(&mut attributes, bytes, text, container_id);
            } else if let Some(secs) = field.strip_prefix("d:") {
                let (nanos, secs) = parse_wire_seconds(secs, line)?;
                line_timestamp = nanos;
                attributes.insert_sym(KEYS.timestamp, Value::U64(secs));
            } else if let Some(host) = field.strip_prefix("h:") {
                attributes.insert_sym(KEYS.event_host, Value::Str(slice_of(bytes, text, host)));
            } else if let Some(priority) = field.strip_prefix("p:") {
                if priority != "normal" && priority != "low" {
                    return Err(malformed());
                }
                attributes
                    .insert_sym(KEYS.event_priority, Value::Str(slice_of(bytes, text, priority)));
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
                    .insert_sym(KEYS.event_aggregation_key, Value::Str(slice_of(bytes, text, key)));
            } else if let Some(source) = field.strip_prefix("s:") {
                attributes
                    .insert_sym(KEYS.event_source_type, Value::Str(slice_of(bytes, text, source)));
            }
            // Any other field, `|T` included, is ignored.
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
            // Not the title: interning free text would grow the global interner without bound.
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    ))
}

/// A DogStatsD event's `TEXT` with its `\n` (backslash, `n`) escape turned into a newline.
///
/// Zero-copy when there is nothing to unescape. Otherwise one allocation: each escape shrinks by
/// one byte, so the buffer is sized exactly and `Bytes::from(Vec)` takes its no-copy
/// `len == capacity` path, where `String::replace`'s slack would cost a second allocation.
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

/// Parses a DogStatsD service check line starting `_sc|` (module doc, "DogStatsD events and
/// service checks").
fn parse_service_check(
    bytes: &Bytes,
    text: &str,
    line: &str,
    timestamp: i64,
    keys: &mut KeyCache,
) -> Result<Event, CodecError> {
    let malformed =
        || CodecError::Malformed(format!("malformed dogstatsd service check: {line:?}"));

    let rest = line.strip_prefix("_sc|").ok_or_else(malformed)?;
    // NAME, STATUS, and the rest: `m:` may contain `|`, so the rest is walked field by field.
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
    // Always stamped: `MetricRecord` has nowhere else to carry the raw name.
    attributes.insert_sym(KEYS.service_check_name, Value::Str(slice_of(bytes, text, name)));
    attributes.insert_sym(KEYS.service_check_status, Value::U64(status as u64));

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
                insert_tags(&mut attributes, bytes, text, tags, keys);
            } else if let Some(container_id) = field.strip_prefix("c:") {
                insert_container_id(&mut attributes, bytes, text, container_id);
            } else if let Some(secs) = field.strip_prefix("d:") {
                let (nanos, secs) = parse_wire_seconds(secs, line)?;
                line_timestamp = nanos;
                attributes.insert_sym(KEYS.timestamp, Value::U64(secs));
            } else if let Some(host) = field.strip_prefix("h:") {
                attributes
                    .insert_sym(KEYS.service_check_host, Value::Str(slice_of(bytes, text, host)));
            }
            // Any other field, `|T` included, is ignored.

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
            // A leading '+' or '-' is a relative adjustment: the spec has no syntax for a negative
            // absolute gauge, so '-' is as unambiguous as '+' and there is no config toggle
            // (`docs/adr/relative-gauge-adjustments.md`'s Alternatives). `f64::from_str` accepts
            // both signs (`plus_prefixed_gauge_values_parse_via_from_str`), so only the
            // `Gauge`/`GaugeDelta` choice is made here; `aggregate` resolves a delta, and a sink
            // reached without one reports it.
            //
            // `sample_rate` is ignored: a gauge value is not a count of occurrences.
            let value = parse_finite_value(raw_value, "gauge", line)?;
            if raw_value.starts_with('+') || raw_value.starts_with('-') {
                MetricKind::GaugeDelta(value)
            } else {
                MetricKind::Gauge(value)
            }
        }
        // `parse_line` handles `ms`/`h`/`d`/`s` itself, one event per line.
        other => unreachable!("build_event only handles c/g, got {other:?}"),
    };

    // Runs once per value on a multi-value line. Cloning scalar tags is a `SmallVec` memcpy plus a
    // refcount bump each; a repeated-key tag's `Value::Array` costs one `Vec` spine allocation per
    // event, its elements still refcounted datagram slices.
    Ok(Event::metric(timestamp, attributes.clone(), MetricRecord::new(intern(name), kind)))
}

/// Parses a metric value, rejecting it unless finite. `f64::parse` accepts "NaN"/"inf"/"-inf",
/// which would become a non-finite `Sum` or `Gauge`, or a `Samples` value that corrupts the
/// `DdSketch` `aggregate` later builds from it. `what` names the value in the error.
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
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    fn decode(line: &str) -> Vec<Event> {
        let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()));
        decoder.decode(Bytes::from(line.to_string())).expect("decode should succeed").events
    }

    /// `with_diagnostics` reaches the UDP decoder as well as the driver, so `bad_line` reports
    /// under the component id.
    #[test]
    fn with_diagnostics_reaches_the_wrapped_decoder_too() {
        let input = StatsdInput::new("127.0.0.1:0").with_diagnostics(Diagnostics::new("my-id"));
        match &input.inner {
            Inner::Udp(listener) => {
                assert_eq!(listener.decoder().diag().component_id(), "my-id");
                assert_eq!(listener.diag().component_id(), "my-id");
            }
            Inner::Tcp(_) => panic!("StatsdInput::new must build a UDP listener"),
        }
    }

    /// The same on the TCP arm, which has its own `map_decoder` call.
    #[test]
    fn with_diagnostics_reaches_a_tcp_connections_decoder() {
        let input = StatsdInput::tcp("127.0.0.1:0").with_diagnostics(Diagnostics::new("tcp-id"));
        match &input.inner {
            Inner::Tcp(listener) => {
                assert_eq!(listener.decoder().diag().component_id(), "tcp-id");
                assert_eq!(listener.diag().component_id(), "tcp-id");
            }
            Inner::Udp(_) => panic!("StatsdInput::tcp must build a TCP listener"),
        }
    }

    /// Events carry the caller's `received_at`, not decode time, which can lag arrival under
    /// backlog (`docs/adr/decoupled-listener-io.md`).
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

    /// `decode_into` appends to `out`, so `logit_pipeline::BatchAccumulator` can reuse one buffer.
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
        // Folding a multi-value line into one multi-metric event must fail here, not quietly
        // change what every caller of this helper asserts.
        assert!(event.log.is_none() && event.span.is_none(), "statsd emits metric-only events");
        event.metrics.pop().unwrap()
    }

    /// A line's rejection, straight from `parse_line`: `decode()` isolates per-line errors and
    /// never surfaces one.
    fn parse_err(line: &str) -> CodecError {
        let bytes = Bytes::from(line.to_string());
        let text = std::str::from_utf8(&bytes).unwrap();
        parse_line(&bytes, text, text, 0, &mut KeyCache::new())
            .expect_err("expected this line to be rejected")
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
        // Zero divides by zero; negative and >1 aren't probabilities; NaN/inf parse but aren't
        // finite.
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
        // `f64::parse` accepts "NaN"/"inf"/"-inf".
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
        // A NaN sample would corrupt the `DdSketch` `aggregate` builds, not just one point.
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

    /// `f64::from_str` accepts a leading `+` as it does `-`, which `build_event`'s `"g"` arm relies
    /// on so that only its `starts_with` check decides `Gauge` vs. `GaugeDelta`.
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

    /// Sign detection must not turn an unsigned gauge into a delta.
    #[test]
    fn an_unsigned_gauge_value_still_decodes_as_an_absolute_gauge() {
        let metric = only_metric(decode("cpu.load:5|g"));
        assert!(matches!(metric.kind, MetricKind::Gauge(v) if v == 5.0));
    }

    /// `+0` is a legal no-op delta, distinct from an unsigned `0` (`Gauge(0.0)`).
    #[test]
    fn a_leading_plus_zero_is_a_legal_no_op_delta_not_an_error() {
        let metric = only_metric(decode("conns:+0|g"));
        assert!(matches!(metric.kind, MetricKind::GaugeDelta(v) if v == 0.0));
    }

    /// A sign never bypasses the finiteness check.
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

    /// Mixed signs on one multi-value gauge line decode as independent deltas.
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

    /// `ms` decodes to a raw [`MetricKind::Samples`], never a sketch.
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

    /// A sample rate rides verbatim on `Samples`; extrapolating is `aggregate`'s job.
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

    /// The same at `@0.1`.
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

    /// An explicit `@1` decodes to one raw value at rate `1.0`; `statsd_decode_one_line` in
    /// `crates/logit-bench/tests/allocations.rs` pins the same at the allocation level.
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

    /// A `ms`/`h`/`d` line's values share one `Samples`, in wire order, on one event.
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

    /// Each of `ms`/`h`/`d` keeps its type letter as `statsd.type`.
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

    // Weight clamping to `Samples::MAX_WEIGHT` and sketch accuracy are tested where sketching
    // happens: `crates/logit-transforms/src/aggregate.rs`'s
    // `samples_sketch_mode_merges_weighted_values_and_counts_weight_clamp`, and
    // `logit_core::metric`'s `Samples::sketch` tests.

    #[test]
    fn dogstatsd_tags_become_attributes() {
        let events = decode("page.views:1|c|#env:prod,host:web1,urgent");
        let event = &events[0];
        assert_eq!(event.attributes.get("env").and_then(|v| v.as_str()), Some("prod"));
        assert_eq!(event.attributes.get("host").and_then(|v| v.as_str()), Some("web1"));
        assert!(matches!(event.attributes.get("urgent"), Some(logit_core::Value::Bool(true))));
    }

    /// A repeat line with the same tag names in another order (one repeated, so the merge runs)
    /// interns nothing new, and the `KeyCache` holds exactly the three tag names. `nextest` runs
    /// each test in its own process, so `interner::len()` reflects only this test.
    #[test]
    fn repeat_tag_keys_are_cache_hits() {
        let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()));
        let line = |s: &str| Bytes::from(s.to_string());
        // `|c:` here too: the first touch of `KEYS` interns every carrier key at once.
        drop(
            decoder.decode(line("tc.views:1|c|#tc_env:prod,tc_host:web1,tc_urgent|c:abc")).unwrap(),
        );
        assert_eq!(decoder.keys.len(), 3);

        let before = logit_core::interner::len();
        let events = decoder
            .decode(line("tc.views:2|c|#tc_host:web2,tc_urgent,tc_env:dev,tc_env:qa|c:abc"))
            .expect("decode should succeed")
            .events;
        assert_eq!(logit_core::interner::len(), before, "same tag keys, same carrier keys");
        assert_eq!(decoder.keys.len(), 3);

        let event = &events[0];
        assert_eq!(event.attributes.get("tc_host").and_then(|v| v.as_str()), Some("web2"));
        assert_eq!(
            event.attributes.get("tc_env"),
            Some(&Value::Array(vec![Value::str("dev"), Value::str("qa")]))
        );
        assert!(matches!(event.attributes.get("tc_urgent"), Some(Value::Bool(true))));
        assert_eq!(
            event.attributes.get("statsd.container_id").and_then(|v| v.as_str()),
            Some("abc")
        );
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
        // The structural pin for `slice_of`, like `crate::syslog`'s
        // `emitted_message_is_a_zero_copy_slice_of_the_datagram`. The repeated `team` key checks
        // that each `Value::Array` element is a datagram slice too, not a copy.
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

    /// Asserts `slice` points inside `datagram`'s allocation rather than at a copy.
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

    /// A repeated tag key folds into a `Value::Array` in wire order; the last token doesn't win.
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

    /// An exact duplicate token is deduped, so no one-element `Array` is produced.
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

    /// A bare token and a valued one sharing a key are not duplicates; both survive, in order.
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

    /// An event line's `#` field folds a repeated key as a metric line's does.
    #[test]
    fn a_repeated_tag_key_on_an_event_line_folds_into_an_array() {
        let event = only_log_event(decode("_e{5,4}:title|text|#k:a,k:b"));
        assert_eq!(
            event.attributes.get("k"),
            Some(&Value::Array(vec![Value::str("a"), Value::str("b")]))
        );
    }

    /// So does a service check's.
    #[test]
    fn a_repeated_tag_key_on_a_service_check_line_folds_into_an_array() {
        let events = decode("_sc|check|0|#k:a,k:b");
        assert_eq!(
            events[0].attributes.get("k"),
            Some(&Value::Array(vec![Value::str("a"), Value::str("b")]))
        );
    }

    /// A tag literally named `statsd.type` folds like any other repeated key. A `c` line, because
    /// on `ms`/`h`/`d` the decoder's own `statsd.type` stamp overwrites it.
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

    /// `s` decodes to raw `SetMembers`: one member, a zero-copy slice of the datagram.
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

    /// A `s` line's values share one `SetMembers`, in wire order, on one event.
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

    /// `|c:<id>` applies to every metric type, not only the spec's `c`/`g`.
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

        // A malformed |T rejects only its own line.
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

    /// Asserts exactly one log-only `Event` and returns it whole, attributes and timestamp
    /// included.
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

    /// TEXT may contain `|` and `:`, and its `\n` escape becomes a newline; the title's doesn't.
    #[test]
    fn event_text_containing_pipe_colon_and_an_escaped_newline_decodes() {
        // Wire bytes: `a|b:c\nd` where `\n` is the two-byte escape sequence -- 8 bytes total.
        let events = decode("_e{1,8}:T|a|b:c\\nd");
        let event = only_log_event(events);
        let message = event.log.as_ref().unwrap().message.as_str().expect("message should be str");
        assert_eq!(message, "a|b:c\nd", "the escape sequence should become a real newline");
        assert!(message.contains('\n'), "expected a real newline byte in the decoded message");
    }

    /// Trailing whitespace (a space, a tab) on an `_e{` line is kept as `TEXT`.
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

    /// `m:` consumes the rest of the line, `|` included.
    #[test]
    fn service_check_message_containing_pipe_decodes_verbatim() {
        let events = decode("_sc|check|0|m:a|b|c");
        assert_eq!(
            events[0].attributes.get("statsd.service_check.message").and_then(|v| v.as_str()),
            Some("a|b|c")
        );
    }

    /// A trailing space on an `_sc|` line is kept as message content.
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

    /// Any `_`-prefixed line other than `_e{`/`_sc|` is an ordinary metric line.
    #[test]
    fn an_underscore_prefixed_metric_name_still_decodes_as_a_metric() {
        let metric = only_metric(decode("_total.count:1|c"));
        assert_eq!(intern("_total.count"), metric.name);
        assert!(
            matches!(metric.kind, MetricKind::Sum(logit_core::Sum { value, .. }) if value == 1.0)
        );
    }

    /// `_x|1` falls through to the metric grammar and is rejected there for having no `:`.
    #[test]
    fn an_underscore_prefixed_line_without_a_colon_is_rejected_for_missing_colon() {
        assert!(matches!(parse_err("_x|1"), CodecError::Malformed(_)));
    }

    /// A counter, an event, and a service check in one datagram decode in wire order.
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

    /// No address before `bind()`, a real one after.
    #[tokio::test]
    async fn local_addr_is_available_after_bind() {
        let mut input = StatsdInput::new("127.0.0.1:0");
        assert_eq!(input.local_addr(), None, "no address before bind()");

        input.bind().await.expect("binding an ephemeral port should succeed");
        let addr = input.local_addr().expect("bind() should leave a real address behind");
        assert_eq!(addr.ip().to_string(), "127.0.0.1");
    }

    // ---- transport: tcp (`StatsdInput::tcp`) ---------------------------------------------------
    //
    // `crate::tcp`'s own tests cover the driver. These cover what is statsd-specific: the framing
    // mode, and that the wrapper's builders reach the driver.

    /// A running TCP listener, ready once `bind()` returns (no sleep-based guess); modelled on
    /// `crate::graphite`'s `Running`/`start`.
    struct RunningTcp {
        addr: std::net::SocketAddr,
        rx: tokio::sync::mpsc::Receiver<logit_pipeline::Delivered>,
        shutdown: watch::Sender<bool>,
        handle: tokio::task::JoinHandle<anyhow::Result<()>>,
        registry: Arc<logit_core::telemetry::Registry>,
    }

    impl RunningTcp {
        /// The next delivered batch's events, or a panic naming `what`. Five seconds, the budget
        /// every socket test in this crate uses.
        async fn next_events(&mut self, what: &str) -> Vec<Event> {
            let delivered = tokio::time::timeout(Duration::from_secs(5), self.rx.recv())
                .await
                .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
                .expect("the channel should not have closed");
            logit_pipeline::unwrap_batch(delivered).events
        }

        async fn connect(&self) -> TcpStream {
            TcpStream::connect(self.addr).await.expect("the listener should accept")
        }
    }

    /// Binds `build`'s listener on an ephemeral port and runs it, one event per batch with no
    /// flush timer, so each delivery is one line.
    async fn start_tcp(build: impl FnOnce(StatsdInput) -> StatsdInput) -> RunningTcp {
        let registry = logit_core::telemetry::Registry::new();
        let telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        let input = StatsdInput::tcp("127.0.0.1:0")
            .with_diagnostics(Diagnostics::new("statsd_in").with_telemetry(telemetry.clone()))
            .with_telemetry(telemetry)
            .with_tcp_receive(TcpListenerConfig {
                batch_max_events: 1,
                batch_flush_interval: Duration::ZERO,
                ..TcpListenerConfig::default()
            });
        let mut input = build(input);
        input.bind().await.expect("binding an ephemeral port should succeed");
        let addr = input.local_addr().expect("bind() should leave a real address behind");

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let fanout = Fanout::new(vec![tx]);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { input.run_until_shutdown(fanout, shutdown_rx).await });
        RunningTcp { addr, rx, shutdown, handle, registry }
    }

    /// The sum of every counter point named `metric`, optionally narrowed to one tag.
    fn metric_sum(events: &[Event], metric: &str, tag: Option<(&str, &str)>) -> f64 {
        events
            .iter()
            .filter(|event| match tag {
                Some((key, value)) => {
                    event.attributes.get(key).and_then(Value::as_str) == Some(value)
                }
                None => true,
            })
            .flat_map(|event| &event.metrics)
            .filter(|m| m.name == intern(metric))
            .map(|m| match &m.kind {
                MetricKind::Sum(sum) => sum.value,
                MetricKind::Gauge(v) => *v,
                other => panic!("{metric} should be a counter or a gauge, got {other:?}"),
            })
            .sum()
    }

    fn metric_name(event: &Event) -> &'static str {
        logit_core::interner::resolve(event.metrics[0].name)
    }

    fn counter_value(event: &Event) -> f64 {
        match &event.metrics[0].kind {
            MetricKind::Sum(sum) => sum.value,
            other => panic!("expected a counter, got {other:?}"),
        }
    }

    /// The pin for this module's "Framing" section: under the driver's `Rfc6587Auto` default,
    /// `1.hits:1|c`'s leading digit would latch octet counting and mis-frame the connection.
    #[tokio::test]
    async fn a_tcp_line_starting_with_a_digit_is_not_read_as_an_octet_count() {
        let mut running = start_tcp(|input| input).await;
        let mut client = running.connect().await;
        client.write_all(b"1.hits:7|c\n").await.unwrap();
        client.flush().await.unwrap();

        let events = running.next_events("the digit-leading line").await;
        assert_eq!(events.len(), 1);
        assert_eq!(metric_name(&events[0]), "1.hits");
        assert_eq!(counter_value(&events[0]), 7.0);

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// Two concurrent clients both deliver (`crate::tcp`'s "Batching is per connection"): the
    /// per-connection `StatsdDecoder` clone works, not merely compiles.
    #[tokio::test]
    async fn two_concurrent_tcp_connections_both_deliver() {
        let mut running = start_tcp(|input| input).await;

        let mut first = running.connect().await;
        let mut second = running.connect().await;
        first.write_all(b"from.first:1|c\n").await.unwrap();
        first.flush().await.unwrap();
        second.write_all(b"from.second:2|c\n").await.unwrap();
        second.flush().await.unwrap();

        let mut seen = vec![
            metric_name(&running.next_events("the first connection's line").await[0]).to_string(),
            metric_name(&running.next_events("the second connection's line").await[0]).to_string(),
        ];
        seen.sort();
        assert_eq!(
            seen,
            ["from.first", "from.second"],
            "both connections deliver -- the order between them is the scheduler's, not \
             something to pin"
        );

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// A line split across two writes is one event: the driver frames across reads, where a
    /// per-read decoder would pass a single-write test and fail this.
    #[tokio::test]
    async fn a_tcp_line_split_across_writes_is_reassembled() {
        let mut running = start_tcp(|input| input).await;
        let mut client = running.connect().await;
        client.write_all(b"split.across:12").await.unwrap();
        client.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        client.write_all(b"3|c\n").await.unwrap();
        client.flush().await.unwrap();

        let events = running.next_events("the reassembled line").await;
        assert_eq!(events.len(), 1);
        assert_eq!(metric_name(&events[0]), "split.across");
        assert_eq!(
            counter_value(&events[0]),
            123.0,
            "the two halves must be one line, not two malformed ones"
        );

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// The repo root's `testdata/tls` (`testdata/tls/README.md`), two levels up from
    /// `CARGO_MANIFEST_DIR`.
    fn testdata_tls_dir() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    /// Wiring only: `with_tls` reaches the driver and a statsd line survives TLS; `crate::tcp`'s
    /// tests cover mTLS, client certificates and the handshake timeout.
    #[tokio::test]
    async fn a_tls_tcp_connection_round_trips_a_line() {
        let settings = TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: None,
        };
        let mut running = start_tcp(|input| {
            input.with_tls(&settings, &testdata_tls_dir()).expect("a tcp listener takes tls")
        })
        .await;

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

        let stream = TcpStream::connect(running.addr).await.expect("the listener should accept");
        // `testdata/tls/server.pem` carries a `localhost` SAN.
        let name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
        let mut client =
            tokio::time::timeout(Duration::from_secs(5), connector.connect(name, stream))
                .await
                .expect("the TLS handshake should complete within 5s")
                .expect("the TLS handshake should succeed");
        client.write_all(b"over.tls:4|c|#env:prod\n").await.unwrap();
        client.flush().await.unwrap();

        let events = running.next_events("the line sent over TLS").await;
        assert_eq!(events.len(), 1);
        assert_eq!(metric_name(&events[0]), "over.tls");
        assert_eq!(events[0].attributes.get("env").and_then(Value::as_str), Some("prod"));

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// An unterminated final line on a clean close is dropped, not emitted (this module's
    /// "Framing" section). The remainder here still looks decodable, so emitting it would produce
    /// a plausible counter rather than a visible error.
    #[tokio::test]
    async fn an_unterminated_tail_at_a_clean_close_is_dropped_and_counted_truncated() {
        let mut running = start_tcp(|input| input).await;
        let mut client = running.connect().await;
        // No trailing newline: the sender got this far and stopped.
        client.write_all(b"page.views:1|c").await.unwrap();
        client.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(client); // a clean FIN, not an RST

        assert!(
            tokio::time::timeout(Duration::from_millis(500), running.rx.recv()).await.is_err(),
            "half a line is not a metric -- nothing should be delivered"
        );

        let drained = running.registry.drain(0);
        assert_eq!(
            metric_sum(&drained, "logit.input.frames.dropped", Some(("reason", "truncated"))),
            1.0,
            "and the loss is counted, exactly as an abrupt close's is"
        );
        assert_eq!(
            metric_sum(&drained, "logit.input.frames", None),
            0.0,
            "the remainder never became a frame"
        );

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// `with_handshake_timeout` reaches the driver: under `with_max_connections(1)`, a second
    /// client is served only if a silent first one's permit came back.
    #[tokio::test]
    async fn a_silent_tcp_connection_releases_its_permit_after_the_handshake_timeout() {
        let mut running = start_tcp(|input| {
            input.with_max_connections(1).with_handshake_timeout(Duration::from_millis(50))
        })
        .await;

        // Held open past the deadline, so only the deadline can free the permit.
        let mut silent = running.connect().await;
        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(2), silent.read(&mut byte))
            .await
            .expect("a silent connection is closed within the handshake timeout, not left hanging")
            .expect("reading a closed socket is Ok(0), not an error");
        assert_eq!(read, 0, "the listener hung up on a connection that said nothing");

        let mut client = running.connect().await;
        client.write_all(b"permit.came.back:1|c\n").await.unwrap();
        client.flush().await.unwrap();
        let events = running.next_events("a line on the connection after the silent one").await;
        assert_eq!(metric_name(&events[0]), "permit.came.back");

        drop(silent);
        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// The same for `with_idle_timeout`; the driver's tests cover the clock itself.
    #[tokio::test]
    async fn an_idle_tcp_connection_releases_its_permit_after_the_idle_timeout() {
        let mut running = start_tcp(|input| {
            input.with_max_connections(1).with_idle_timeout(Some(Duration::from_millis(50)))
        })
        .await;

        // One line passes the first-byte deadline, so only the idle clock can close this.
        let mut quiet = running.connect().await;
        quiet.write_all(b"quiet.then.idle:1|c\n").await.unwrap();
        quiet.flush().await.unwrap();
        let events = running.next_events("the line before going quiet").await;
        assert_eq!(metric_name(&events[0]), "quiet.then.idle");

        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(2), quiet.read(&mut byte))
            .await
            .expect("a connection quiet past its idle_timeout is closed, not left hanging")
            .expect("reading a closed socket is Ok(0), not an error");
        assert_eq!(read, 0, "the listener hung up on a connection that went quiet");

        let mut client = running.connect().await;
        client.write_all(b"permit.came.back:1|c\n").await.unwrap();
        client.flush().await.unwrap();
        let events = running.next_events("a line on the connection after the quiet one").await;
        assert_eq!(metric_name(&events[0]), "permit.came.back");

        drop(quiet);
        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    // -------------------------------------------------------------------------------------------
    // Recorded interop fixtures (testdata/interop/statsd/, docs/plans/recorded-interop-fixtures.md)
    //
    // Real UDP datagrams from two real clients, Datadog's `datadog` package and the plain-statsd
    // `statsd` package, recorded by `script/record-fixtures statsd`. The tests above check this
    // repo's reading of the grammar; these check what real clients send, which is how a shared
    // misunderstanding surfaces.
    //
    // Asserted on decoded values, never bytes: a re-record changes the container id and flush
    // boundaries (that directory's README).
    // -------------------------------------------------------------------------------------------

    fn interop_dir() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/interop/statsd")
    }

    fn interop_fixture(name: &str) -> Bytes {
        let path = interop_dir().join(name);
        let raw = std::fs::read(&path)
            .unwrap_or_else(|e| panic!("reading interop fixture {}: {e}", path.display()));
        Bytes::from(raw)
    }

    /// Every captured datagram whose filename starts with `prefix`, in name order.
    fn interop_fixtures(prefix: &str) -> Vec<(String, Bytes)> {
        let dir = interop_dir();
        let mut names: Vec<String> = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("reading {}: {e}", dir.display()))
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with(prefix) && name.ends_with(".raw"))
            .collect();
        names.sort();
        assert!(!names.is_empty(), "no fixture matching `{prefix}*` under {}", dir.display());
        names
            .into_iter()
            .map(|name| {
                let bytes = interop_fixture(&name);
                (name, bytes)
            })
            .collect()
    }

    /// Decodes one captured datagram with drainable diagnostics, so a test can assert zero of
    /// them; a length check alone would pass a datagram whose every line was rejected.
    fn decode_interop(datagram: &Bytes) -> (Vec<Event>, Vec<String>) {
        let registry = logit_core::telemetry::Registry::new();
        let telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()))
            .with_diagnostics(Diagnostics::new("statsd_in").with_telemetry(telemetry));
        let mut events = Vec::new();
        decoder
            .decode_into(datagram.clone(), 0, &mut events)
            .expect("a captured datagram must decode as a whole");
        let keys = registry
            .drain(0)
            .into_iter()
            .filter_map(|event| match event.attributes.get("key") {
                Some(Value::Str(key)) => Some(String::from_utf8_lossy(key).into_owned()),
                _ => None,
            })
            .collect();
        (events, keys)
    }

    #[test]
    fn interop_fixture_every_captured_datagram_decodes_with_no_diagnostics() {
        let mut datagrams = 0usize;
        let mut events = 0usize;
        for (name, bytes) in interop_fixtures("statsd-") {
            let (decoded, diagnostics) = decode_interop(&bytes);
            assert!(diagnostics.is_empty(), "{name} raised {diagnostics:?}");
            assert!(!decoded.is_empty(), "{name} decoded to no events at all");
            datagrams += 1;
            events += decoded.len();
        }
        assert!(datagrams >= 50, "expected the whole capture, got {datagrams} datagrams");
        assert!(events >= 100, "expected a real workload, got {events} events");
    }

    #[test]
    fn interop_fixture_a_buffered_dogstatsd_datagram_carries_a_whole_packed_batch() {
        // The client packed many metrics into one datagram, cut on a line boundary.
        let bytes = interop_fixture("statsd-dogstatsd-buffered-000.raw");
        assert!(bytes.len() > 1_000, "the client packs close to its 1432-byte UDP ceiling");
        assert!(bytes.len() <= 1_432, "and never past it");
        let (events, diagnostics) = decode_interop(&bytes);
        assert!(diagnostics.is_empty(), "{diagnostics:?}");
        assert!(
            events.len() >= 8,
            "a packed DogStatsD datagram holds many metrics, got {}",
            events.len()
        );

        // Only these two shapes are asserted: which types land in one datagram depends on the
        // client's flush boundary.
        let mut sums = 0;
        let mut samples = 0;
        for event in &events {
            for metric in &event.metrics {
                match &metric.kind {
                    MetricKind::Sum(_) => sums += 1,
                    MetricKind::Samples(_) => samples += 1,
                    MetricKind::SetMembers(_) | MetricKind::Gauge(_) => {}
                    other => panic!("unexpected metric kind from a real client: {other:?}"),
                }
            }
        }
        assert!(sums > 0 && samples > 0, "got {sums} counters, {samples} timings");
    }

    #[test]
    fn interop_fixture_a_real_dogstatsd_line_carries_its_tags_and_container_id() {
        // The client detected its own container and sent `|c:<id>` unprompted, which a
        // hand-written fixture wouldn't have included.
        let (events, diagnostics) =
            decode_interop(&interop_fixture("statsd-dogstatsd-unbuffered-000.raw"));
        assert!(diagnostics.is_empty(), "{diagnostics:?}");
        let event = &events[0];
        assert_eq!(
            event.attributes.get("env").and_then(Value::as_str),
            Some("prod"),
            "the workload's own `env:prod` tag"
        );
        assert_eq!(event.attributes.get("service").and_then(Value::as_str), Some("checkout-api"));
        assert!(
            event.attributes.get("endpoint").is_some(),
            "a per-request tag the client hung off the line"
        );
        assert!(
            event.attributes.get("statsd.container_id").is_some(),
            "DogStatsd volunteers `|c:<id>` when it can see its own container"
        );
    }

    #[test]
    fn interop_fixture_plain_statsd_carries_its_cardinality_in_the_name_and_no_tags() {
        // Plain statsd: no tags, so what the tagged client put in tags is in the metric name.
        let (events, diagnostics) =
            decode_interop(&interop_fixture("statsd-plain-unbuffered-000.raw"));
        assert!(diagnostics.is_empty(), "{diagnostics:?}");
        assert_eq!(events.len(), 1, "one metric per datagram, unbuffered");
        let event = &events[0];
        let name = metric_name(event);
        assert!(name.starts_with("app.checkout_api."), "got {name:?}");
        assert!(
            name.len() > 40,
            "a tagless name carries the cardinality: {name:?} is {} bytes",
            name.len()
        );
        assert!(
            event.attributes.get("env").is_none()
                && event.attributes.get("statsd.container_id").is_none(),
            "the plain-statsd dialect has no tag or container-id syntax at all"
        );
    }
}
