//! Encoding a batch of events back into collectd datagrams -- the `| Model | Wire |` half of
//! [`super`]'s module doc, which is the spec for everything here.
//!
//! Pure: no socket anywhere, so every packing, elision and sanitization test runs directly against
//! [`CollectdEncoder`] (`crates/logit-outputs/src/statsd.rs`'s same split). The output is a
//! [`Packets`], not one opaque `Bytes` -- see [`super`]'s "No `crate::Encoder`" paragraph.

use super::part::{self, DsValue};
use super::{
    nanos_to_cdtime, ATTR_PREFIX, CDTIME_ONE_SECOND, DATA_MAX_NAME_LEN, MAX_VALUES_PER_LIST,
};
use bytes::Bytes;
use logit_core::{
    Diagnostics, Event, EventBatch, MetricKind, MetricRecord, Resource, Telemetry, Temporality,
    Value,
};
use std::ops::Range;

/// Usable bytes in an identity field: [`DATA_MAX_NAME_LEN`] minus the NUL terminator collectd's own
/// `parse_part_string` insists on. A longer field would make collectd reject the **whole packet**,
/// taking every unrelated list in it down too, so the encoder truncates rather than hoping.
const MAX_IDENTITY_BYTES: usize = DATA_MAX_NAME_LEN - 1;

/// Encoded datagrams: one contiguous buffer, one range per datagram, and the number of value lists
/// each datagram carries. [`crate::MessageBuf`]`<usize>` is now exactly this shape (ADR
/// `framed-encoder`); replacing this type with it, and implementing [`crate::FramedEncoder`], is
/// the named follow-up before `collectd_out` is built on it.
///
/// The per-datagram list count is what `collectd_out` needs to report an `EMSGSIZE` honestly: the
/// number dropped is the lists in that one datagram, not one "message."
#[derive(Debug, Default)]
pub struct Packets {
    bytes: Vec<u8>,
    ranges: Vec<Range<usize>>,
    lists: Vec<usize>,
}

impl Packets {
    /// One `(datagram bytes, value lists in it)` pair per datagram, in the order they were packed.
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], usize)> {
        self.ranges
            .iter()
            .zip(&self.lists)
            .map(move |(range, lists)| (&self.bytes[range.clone()], *lists))
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

    pub fn clear(&mut self) {
        self.bytes.clear();
        self.ranges.clear();
        self.lists.clear();
    }

    fn push(&mut self, packet: &[u8], lists: usize) {
        let start = self.bytes.len();
        self.bytes.extend_from_slice(packet);
        self.ranges.push(start..self.bytes.len());
        self.lists.push(lists);
    }
}

/// Per-batch outcome counts from [`CollectdEncoder::encode_into`] -- what `collectd_out` (W3) turns
/// into its own `logit.output.*` telemetry, and what this module's tests assert on. The codec also
/// emits the per-drop counters itself, at each drop site (see [`Ctx`]); these are the aggregate a
/// caller can compare exactly.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EncodeStats {
    /// Events carrying no metrics at all -- a log- or span-only event, legal under
    /// `docs/adr/multi-payload-events.md`. Not a loss: there was nothing collectd could carry.
    pub skipped_no_metrics: usize,
    /// A metric kind collectd has no data-source type for (every post-summarization kind, plus a
    /// delta non-monotonic `Sum`). Counted once per drop, with the kind as the counter's tag.
    pub dropped_unsupported_kind: usize,
    /// A `Sum` that is non-finite, has a fractional part, or falls outside its target integer
    /// range. collectd's COUNTER/DERIVE/ABSOLUTE are integers; rounding would fabricate.
    pub dropped_unencodable_value: usize,
    /// A `NO_RECORDED_VALUE`-flagged point of any kind **other than** `Gauge` (a flagged gauge has a
    /// real wire form -- NaN -- and is encoded, not dropped).
    pub dropped_no_recorded_value: usize,
    /// A `MetricKind::GaugeDelta`, which means a missing `aggregate` stage rather than a bad metric.
    pub dropped_gauge_delta: usize,
    /// An event whose `timestamp` is zero or negative: there is no cdtime before the epoch, and
    /// stamping "now" instead would invent an instant nothing upstream reported. Counted once per
    /// value **list** the event would have produced -- 1 for a like-relay event, one per record for
    /// a fallback one -- so this number means the same thing as every other sink's
    /// `metrics.skipped`.
    pub dropped_unencodable_timestamp: usize,
    /// An event with no host to write: no `collectd.host`, no `host.name`, and no configured
    /// [`CollectdEncoder::with_hostname`]. collectd's receiver rejects an empty host outright, and
    /// this encoder has no business inventing one -- see that builder's own doc. Counted per value
    /// list, exactly as [`Self::dropped_unencodable_timestamp`] is.
    pub dropped_no_host: usize,
    /// A like-relay event carrying more than [`MAX_VALUES_PER_LIST`] records. One list, one `u16`
    /// `count` on the wire, and the decode side of this very codec rejects a longer one as a
    /// malformed part -- which would take every unrelated list packed behind it in the same
    /// datagram with it. Counted once: it is one list that was dropped, however many records it
    /// held.
    pub dropped_too_many_values: usize,
    /// A list whose plugin or type sanitized to nothing -- collectd's receiver rejects both.
    pub dropped_empty_name: usize,
    /// A single value list larger than `max_packet_bytes` all by itself: dropped whole, never split
    /// across datagrams (a split list would be dispatched against the wrong identity).
    pub dropped_oversize_list: usize,
    /// An attribute outside the `collectd.` namespace. collectd has no tag concept at all, so every
    /// one of them is dropped -- counted once per attribute per event, including `host.name`, which
    /// the host resolution reads but cannot carry as itself.
    pub tags_dropped_no_wire_form: usize,
    /// A `collectd.*` attribute whose `Value` type has no wire form here: an identity field that
    /// isn't `Str`/`Bytes`, an interval that isn't a finite positive `F64`, or a `collectd.*` name
    /// this codec doesn't know (`collectd.severity`, until W5 gives it a wire form).
    pub tags_dropped_unrepresentable: usize,
    /// An identity field that had a NUL or `/` replaced with `_`. Counted once per field, not once
    /// per byte.
    pub identity_sanitized_substituted: usize,
    /// An identity field truncated to [`MAX_IDENTITY_BYTES`]. Counted once per field.
    pub identity_sanitized_truncated: usize,
}

/// The identity a value list is dispatched against, owned rather than borrowed: the *previous*
/// list's identity has to outlive the event that produced it, since elision compares across events
/// within one datagram. `clone_from` reuses these `Vec`s, so steady-state encoding allocates
/// nothing here.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Identity {
    host: Vec<u8>,
    plugin: Vec<u8>,
    plugin_instance: Vec<u8>,
    type_: Vec<u8>,
    type_instance: Vec<u8>,
}

impl Identity {
    /// The "nothing written yet" state, which is also exactly what a receiver's sticky state is at
    /// the start of a datagram -- so comparing against this is what makes a fresh packet's first
    /// list carry its full identity.
    fn clear(&mut self) {
        self.host.clear();
        self.plugin.clear();
        self.plugin_instance.clear();
        self.type_.clear();
        self.type_instance.clear();
    }

    /// Clears everything except `host`, which is resolved once per event and shared by every list
    /// that event produces.
    fn clear_below_host(&mut self) {
        self.plugin.clear();
        self.plugin_instance.clear();
        self.type_.clear();
        self.type_instance.clear();
    }
}

/// Encodes events as collectd value lists packed into datagrams. Pure -- no socket -- so
/// `collectd_out` (W3) is only a transport wrapper over this.
pub struct CollectdEncoder {
    telemetry: Telemetry,
    diag: Diagnostics,
    /// The operator-configured hostname, already sanitized at construction so the per-event host
    /// path never re-sanitizes (and never counts) a value that came from config rather than data.
    /// `None` means "not configured", which is not the same as empty -- see
    /// [`CollectdEncoder::with_hostname`].
    hostname: Option<Bytes>,
    /// The identity of the last list written into the packet currently being packed.
    last: Identity,
    /// The identity of the list currently being encoded.
    cur: Identity,
    /// The datagram being packed.
    packet: Vec<u8>,
    /// One encoded value list -- cleared per list, never reallocated.
    list: Vec<u8>,
    /// The resolved wire values of the list currently being encoded.
    values: Vec<DsValue>,
}

impl Default for CollectdEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl CollectdEncoder {
    pub fn new() -> Self {
        Self {
            telemetry: Telemetry::default(),
            diag: Diagnostics::default(),
            hostname: None,
            last: Identity::default(),
            cur: Identity::default(),
            packet: Vec::new(),
            list: Vec::new(),
            values: Vec::new(),
        }
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    /// The host written when neither `collectd.host` nor `host.name` is present -- `collectd_out`'s
    /// own `hostname:` config field (W3), passed straight through.
    ///
    /// **Deliberately operator-supplied, with no default of any kind.** This encoder neither reads
    /// the OS hostname (an OS-hostname source is explicitly deferred work, `docs/known-gaps.md`)
    /// nor invents a literal placeholder (`syslog_out` rejected exactly that, for exactly the
    /// reason it would be wrong here: a receiver keys every series on the host, so one made-up name
    /// silently merges every unlabelled sender into a single host's metrics). With nothing
    /// configured and nothing on the event, the value list is dropped and counted -- a visible,
    /// greppable misconfiguration instead of a quiet mislabelling.
    ///
    /// Sanitized here, once, rather than per event; an empty or all-substituted-away value is the
    /// same as not configuring one at all.
    pub fn with_hostname(mut self, hostname: impl Into<Bytes>) -> Self {
        let raw: Bytes = hostname.into();
        let mut sanitized = Vec::new();
        // Byte truncation, not character truncation: a configured hostname arrives as bytes here and
        // is not guaranteed UTF-8 any more than a wire one is.
        sanitize_raw(&mut sanitized, &raw, false);
        self.hostname = (!sanitized.is_empty()).then(|| Bytes::from(sanitized));
        self
    }

    /// Encodes every event in `batch` into `out` (cleared first), packing value lists into datagrams
    /// of at most `max_packet_bytes`. Never fails: a per-list problem is a counted drop, not an
    /// error, and there is nothing for a caller to react to beyond the returned [`EncodeStats`].
    ///
    /// `max_packet_bytes` bounds one **datagram**, not one list; a list that exceeds it alone is
    /// dropped whole. [`super::DEFAULT_MAX_PACKET_BYTES`] is collectd's own default.
    pub fn encode_into(
        &mut self,
        batch: &EventBatch,
        max_packet_bytes: usize,
        out: &mut Packets,
    ) -> EncodeStats {
        out.clear();
        let mut stats = EncodeStats::default();
        // Destructured rather than reached through `self`: `last`, `cur`, `packet`, `list` and
        // `values` are all borrowed at once by the packing loop below, which `&mut self` methods
        // could not express.
        let Self { telemetry, diag, hostname, last, cur, packet, list, values } = self;
        let mut ctx = Ctx { telemetry, diag, stats: &mut stats };

        packet.clear();
        last.clear();
        let mut lists_in_packet = 0usize;

        for event in &batch.events {
            if event.metrics.is_empty() {
                ctx.stats.skipped_no_metrics += 1;
                continue;
            }

            let carriers = collect_carriers(&batch.resource, event, &mut ctx);

            // How many value lists this event would have produced: one for a like-relay event, one
            // per record for a fallback one. The two whole-event drops below happen before either
            // path runs, so this is the number they have to count -- `logit.output.metrics.skipped`
            // is a *per record* figure at every other sink (`prometheus/mod.rs` counts inside its
            // own `for record in &event.metrics`), and an operator summing it across sinks needs
            // this one to mean the same thing.
            let lists = if carriers.type_.is_some() { 1 } else { event.metrics.len() };

            // `nanos_to_cdtime` returns 0 for any non-positive instant, which is also collectd's own
            // "no time given" -- and a list with no time is one its receiver rejects, so this is a
            // drop rather than a zero on the wire.
            let time_cdtime = nanos_to_cdtime(event.timestamp);
            if time_cdtime == 0 {
                ctx.drop_unencodable_timestamp(event.timestamp, lists);
                continue;
            }

            // The host is the same for every list this event produces, so it is resolved once --
            // and if it cannot be resolved at all, every one of those lists goes.
            cur.host.clear();
            if !resolve_host(&mut cur.host, &carriers, hostname.as_deref(), lists, &mut ctx) {
                continue;
            }

            if carriers.type_.is_some() {
                // Like-relay: the event arrived from `collectd_in` (or was given `collectd.*`
                // attributes on purpose), so its identity is the wire's own and its whole
                // `MetricList` is one value list, in order.
                //
                // Which is why the encode side needs the same cap the decode side enforces: the
                // wire's `count` is a `u16`, this codec accepts at most `MAX_VALUES_PER_LIST` of
                // them, and a longer list would sail under the byte cap only to be rejected as a
                // malformed part by any receiver built on this codec -- taking every unrelated list
                // packed behind it in the same datagram down with it. `aggregate`/`kv_metrics` can
                // both put far more than 64 records on one event, and a `set` stamping
                // `collectd.type` is all it takes to route that here.
                if event.metrics.len() > MAX_VALUES_PER_LIST {
                    ctx.drop_too_many_values(event.metrics.len());
                    continue;
                }

                cur.clear_below_host();
                sanitize_carrier(&mut cur.plugin, carriers.plugin, &mut ctx);
                sanitize_carrier(&mut cur.plugin_instance, carriers.plugin_instance, &mut ctx);
                sanitize_carrier(&mut cur.type_, carriers.type_, &mut ctx);
                sanitize_carrier(&mut cur.type_instance, carriers.type_instance, &mut ctx);
                if cur.plugin.is_empty() || cur.type_.is_empty() {
                    ctx.drop_empty_name();
                    continue;
                }

                values.clear();
                let mut resolved_all = true;
                for record in &event.metrics {
                    match resolve_value(record, &mut ctx) {
                        Some(value) => values.push(value),
                        None => {
                            // The whole list goes, counted once by `resolve_value` for the first
                            // failing record: collectd's receiver rejects a list whose value count
                            // disagrees with its type's `ds_num`, so a partial list would be
                            // discarded at the far end anyway -- and silently, which is worse.
                            resolved_all = false;
                            break;
                        }
                    }
                }
                if !resolved_all {
                    continue;
                }

                pack_list(
                    packet,
                    list,
                    last,
                    cur,
                    time_cdtime,
                    carriers.interval_cdtime,
                    values,
                    max_packet_bytes,
                    &mut lists_in_packet,
                    out,
                    &mut ctx,
                );
                continue;
            }

            // Fallback: an event from anywhere else in the pipeline. Each record becomes its own
            // single-data-source list, named the way collectd's own `write_graphite` reads a
            // dotted name back: plugin, then type_instance.
            for record in &event.metrics {
                let Some(value) = resolve_value(record, &mut ctx) else { continue };
                let full = logit_core::interner::resolve(record.name);
                let (plugin_text, instance_text) = full.split_once('.').unwrap_or((full, ""));

                cur.clear_below_host();
                sanitize_into(&mut cur.plugin, plugin_text.as_bytes(), true, &mut ctx);
                sanitize_into(&mut cur.type_instance, instance_text.as_bytes(), true, &mut ctx);
                cur.type_.extend_from_slice(fallback_type(value).as_bytes());
                if cur.plugin.is_empty() {
                    ctx.drop_empty_name();
                    continue;
                }

                values.clear();
                values.push(value);
                pack_list(
                    packet,
                    list,
                    last,
                    cur,
                    time_cdtime,
                    carriers.interval_cdtime,
                    values,
                    max_packet_bytes,
                    &mut lists_in_packet,
                    out,
                    &mut ctx,
                );
            }
        }

        if !packet.is_empty() {
            out.push(packet, lists_in_packet);
        }
        stats
    }
}

/// The telemetry/diagnostics/stats triple every drop site needs, carried together so a drop reports
/// itself in all three places at once and can never be counted in one but not the others
/// (`crates/logit-outputs/src/statsd.rs`'s `EncodeCtx` is the same idea).
struct Ctx<'a> {
    telemetry: &'a Telemetry,
    diag: &'a mut Diagnostics,
    stats: &'a mut EncodeStats,
}

impl Ctx<'_> {
    fn tag_dropped_no_wire_form(&mut self) {
        self.stats.tags_dropped_no_wire_form += 1;
        self.telemetry.count("logit.output.tags.dropped", 1.0, &[("reason", "no_wire_form")]);
    }

    fn tag_dropped_unrepresentable(&mut self) {
        self.stats.tags_dropped_unrepresentable += 1;
        self.telemetry.count("logit.output.tags.dropped", 1.0, &[("reason", "unrepresentable")]);
    }

    fn identity_substituted(&mut self) {
        self.stats.identity_sanitized_substituted += 1;
        self.telemetry.count("logit.output.identity.sanitized", 1.0, &[("reason", "substituted")]);
    }

    fn identity_truncated(&mut self) {
        self.stats.identity_sanitized_truncated += 1;
        self.telemetry.count("logit.output.identity.sanitized", 1.0, &[("reason", "truncated")]);
    }

    /// One of the metric kinds collectd has no data-source type for. `metric_kind` is the counter
    /// tag (`&'static str`, as every tag must be); `described` is the prose the diagnostic uses.
    fn drop_kind(&mut self, metric_kind: &'static str, described: &str, name: &str) {
        self.stats.dropped_unsupported_kind += 1;
        self.telemetry.count("logit.output.metrics.skipped", 1.0, &[("metric_kind", metric_kind)]);
        self.diag.warn_throttled(
            "unsupported_metric_kind",
            format_args!(
                "collectd_out: {described} has no collectd data-source type (metric {name:?}); \
                 dropping"
            ),
        );
    }

    fn drop_gauge_delta(&mut self) {
        self.stats.dropped_gauge_delta += 1;
        self.telemetry.count(
            "logit.output.metrics.skipped",
            1.0,
            &[("metric_kind", "gauge_delta")],
        );
        self.diag.warn_throttled(
            "gauge_delta_unresolved",
            "a relative gauge adjustment reached a sink unresolved -- add an `aggregate` component \
             between the statsd input and this output",
        );
    }

    fn drop_unencodable_value(&mut self, name: &str, value: f64) {
        self.stats.dropped_unencodable_value += 1;
        self.telemetry.count(
            "logit.output.metrics.skipped",
            1.0,
            &[("reason", "unencodable_value")],
        );
        self.diag.warn_throttled(
            "unencodable_value",
            format_args!(
                "collectd_out: {value} on metric {name:?} is not an integer collectd can carry \
                 (COUNTER/DERIVE/ABSOLUTE are integral); dropping rather than rounding"
            ),
        );
    }

    fn drop_no_recorded_value(&mut self, name: &str) {
        self.stats.dropped_no_recorded_value += 1;
        self.telemetry.count(
            "logit.output.metrics.skipped",
            1.0,
            &[("reason", "no_recorded_value")],
        );
        self.diag.warn_throttled(
            "no_recorded_value",
            format_args!(
                "collectd_out: metric {name:?} has no recorded value (OTLP NO_RECORDED_VALUE) and \
                 is not a gauge, whose NaN is collectd's own way to say so; dropping"
            ),
        );
    }

    /// `lists` is how many value lists the dropped event would have produced -- see
    /// [`EncodeStats::dropped_unencodable_timestamp`] for why both whole-event drops are counted
    /// per list rather than per event.
    fn drop_unencodable_timestamp(&mut self, timestamp: i64, lists: usize) {
        self.stats.dropped_unencodable_timestamp += lists;
        self.telemetry.count(
            "logit.output.metrics.skipped",
            lists as f64,
            &[("reason", "unencodable_timestamp")],
        );
        self.diag.warn_throttled(
            "unencodable_timestamp",
            format_args!(
                "collectd_out: timestamp {timestamp} is not a positive instant; dropping the \
                 event rather than stamping one collectd would read as unset"
            ),
        );
    }

    /// `lists` as in [`Ctx::drop_unencodable_timestamp`].
    fn drop_no_host(&mut self, lists: usize) {
        self.stats.dropped_no_host += lists;
        self.telemetry.count(
            "logit.output.metrics.skipped",
            lists as f64,
            &[("reason", "no_host")],
        );
        self.diag.warn_throttled(
            "no_host",
            "collectd_out: no host to write -- the event carries neither `collectd.host` nor \
             `host.name`, and this sink has no `hostname:` configured; dropping. Set `hostname:` \
             on the sink, or stamp `host.name` with a `set` transform: collectd's receiver rejects \
             an empty host, and inventing one would merge every unlabelled sender into one host's \
             metrics",
        );
    }

    fn drop_too_many_values(&mut self, records: usize) {
        self.stats.dropped_too_many_values += 1;
        self.telemetry.count("logit.output.metrics.skipped", 1.0, &[("reason", "too_many_values")]);
        self.diag.warn_throttled(
            "too_many_values",
            format_args!(
                "collectd_out: a value list of {records} data sources exceeds the \
                 {MAX_VALUES_PER_LIST}-source cap this codec reads and writes; dropping it whole \
                 rather than emitting a list the receiver would reject as a malformed part, \
                 taking every list packed behind it with it"
            ),
        );
    }

    fn drop_empty_name(&mut self) {
        self.stats.dropped_empty_name += 1;
        self.telemetry.count("logit.output.metrics.skipped", 1.0, &[("reason", "empty_name")]);
        self.diag.warn_throttled(
            "empty_metric_name",
            "collectd_out: a value list's plugin or type is empty after sanitizing; dropping it \
             (collectd's own receiver rejects the same list)",
        );
    }

    fn drop_oversize_list(&mut self, max_packet_bytes: usize) {
        self.stats.dropped_oversize_list += 1;
        self.telemetry.count(
            "logit.output.metrics.skipped",
            1.0,
            &[("reason", "oversize_value_list")],
        );
        self.diag.warn_throttled(
            "oversize_value_list",
            format_args!(
                "collectd_out: a single value list exceeds max_packet_bytes \
                 ({max_packet_bytes}); dropping it whole rather than splitting it across \
                 datagrams"
            ),
        );
    }
}

/// The `collectd.*` carriers (plus `host.name`) read off one event's merged attributes.
#[derive(Default, Clone, Copy)]
struct Carriers<'a> {
    host: Option<&'a Value>,
    plugin: Option<&'a Value>,
    plugin_instance: Option<&'a Value>,
    type_: Option<&'a Value>,
    type_instance: Option<&'a Value>,
    /// Already converted to ticks; `0` means absent, which is collectd's own "unspecified".
    interval_cdtime: u64,
    /// The normalized host attribute, used only when `collectd.host` is absent.
    host_name: Option<&'a Value>,
}

/// Walks one event's attributes merged over its resource (event wins -- [`logit_core::attrs::merged`]),
/// capturing the `collectd.*` carriers and counting everything that has no wire form. One pass does
/// both jobs and stays symmetric with what it filters out, exactly the way `statsd_out`'s
/// `build_tag_suffix` does.
fn collect_carriers<'a>(resource: &'a Resource, event: &'a Event, ctx: &mut Ctx) -> Carriers<'a> {
    let mut carriers = Carriers::default();
    for (key, value) in logit_core::attrs::merged(resource, event) {
        let key = logit_core::interner::resolve(key);
        if let Some(field) = key.strip_prefix(ATTR_PREFIX) {
            match (field, value) {
                ("host", Value::Str(_) | Value::Bytes(_)) => carriers.host = Some(value),
                ("plugin", Value::Str(_) | Value::Bytes(_)) => carriers.plugin = Some(value),
                ("plugin_instance", Value::Str(_) | Value::Bytes(_)) => {
                    carriers.plugin_instance = Some(value)
                }
                ("type", Value::Str(_) | Value::Bytes(_)) => carriers.type_ = Some(value),
                ("type_instance", Value::Str(_) | Value::Bytes(_)) => {
                    carriers.type_instance = Some(value)
                }
                ("interval", Value::F64(seconds)) if seconds.is_finite() && *seconds > 0.0 => {
                    // Exact for every interval a real sender uses: the tick count is what the
                    // decode side divided by, so `10.0` seconds comes back as `10 << 30` ticks.
                    carriers.interval_cdtime = (seconds * CDTIME_ONE_SECOND as f64).round() as u64;
                }
                // A carrier of the wrong `Value` type, a non-positive interval, or a `collectd.*`
                // name this codec has no wire form for yet (`collectd.severity`, until W5). Not
                // silently ignored: an operator who set one of these deliberately deserves to see
                // it counted.
                _ => ctx.tag_dropped_unrepresentable(),
            }
            continue;
        }
        if key == "host.name" && matches!(value, Value::Str(_) | Value::Bytes(_)) {
            carriers.host_name = Some(value);
        }
        // Counted even for `host.name`: the host resolution reads it, but the *attribute* still has
        // no wire form of its own -- collectd has no tags at all.
        ctx.tag_dropped_no_wire_form();
    }
    carriers
}

/// This event's host: `collectd.host`, else `host.name`, else the encoder's configured
/// [`CollectdEncoder::with_hostname`] -- the first that survives sanitizing non-empty. Returns
/// whether one was found; `false` means the event is dropped (counted `no_host`), because collectd's
/// receiver rejects an empty host and no honest value exists to substitute.
fn resolve_host(
    out: &mut Vec<u8>,
    carriers: &Carriers,
    hostname: Option<&[u8]>,
    lists: usize,
    ctx: &mut Ctx,
) -> bool {
    for candidate in [carriers.host, carriers.host_name] {
        if let Some((raw, is_utf8)) = candidate.and_then(text_of) {
            sanitize_into(out, raw, is_utf8, ctx);
            if !out.is_empty() {
                return true;
            }
        }
    }
    // Already sanitized and known non-empty (`with_hostname`), so this is a copy.
    if let Some(hostname) = hostname {
        out.clear();
        out.extend_from_slice(hostname);
        return true;
    }
    ctx.drop_no_host(lists);
    false
}

/// [`sanitize_into`] for an optional carrier: an absent one leaves `out` empty, which is exactly
/// what an absent instance means on the wire.
fn sanitize_carrier(out: &mut Vec<u8>, carrier: Option<&Value>, ctx: &mut Ctx) {
    if let Some((raw, is_utf8)) = carrier.and_then(text_of) {
        sanitize_into(out, raw, is_utf8, ctx);
    }
}

/// A `Value`'s bytes and whether they are known-valid UTF-8 (which is what decides between
/// character- and byte-boundary truncation). `None` for every other `Value` kind -- those never
/// reach here, since [`collect_carriers`] filters and counts them first.
fn text_of(value: &Value) -> Option<(&[u8], bool)> {
    match value {
        Value::Str(bytes) => Some((bytes, true)),
        Value::Bytes(bytes) => Some((bytes, false)),
        _ => None,
    }
}

/// [`sanitize_raw`], reporting what it did. See [`super`]'s "Sanitization" section for the rules and
/// why the list is as short as it is.
fn sanitize_into(out: &mut Vec<u8>, raw: &[u8], is_utf8: bool, ctx: &mut Ctx) {
    let (substituted, truncated) = sanitize_raw(out, raw, is_utf8);
    if substituted {
        ctx.identity_substituted();
    }
    if truncated {
        ctx.identity_truncated();
    }
}

/// Writes `raw` into `out` (cleared first) with NUL and `/` replaced by `_`, truncated to
/// [`MAX_IDENTITY_BYTES`]. Returns `(substituted, truncated)`.
///
/// Pure, so [`CollectdEncoder::with_hostname`] can sanitize a configured hostname at
/// construction with nothing to count. Substitution rather than deletion, following
/// `crates/logit-outputs/src/statsd.rs`'s `sanitize_into`: distinct inputs stay distinct.
/// `is_utf8` truncates on a character boundary instead of a byte one -- neither substitution
/// changes a byte's length, so the boundaries of the input still hold in `out`.
fn sanitize_raw(out: &mut Vec<u8>, raw: &[u8], is_utf8: bool) -> (bool, bool) {
    out.clear();
    let mut substituted = false;
    for &byte in raw {
        if byte == 0 || byte == b'/' {
            out.push(b'_');
            substituted = true;
        } else {
            out.push(byte);
        }
    }
    let mut truncated = false;
    if out.len() > MAX_IDENTITY_BYTES {
        let mut end = MAX_IDENTITY_BYTES;
        if is_utf8 {
            // Walk back off any UTF-8 continuation byte (`0b10xxxxxx`) so the truncated string is
            // still valid UTF-8 -- a half-written character would make the attribute it round-trips
            // into a `Value::Bytes` instead of a `Value::Str`.
            while end > 0 && out[end] & 0xC0 == 0x80 {
                end -= 1;
            }
        }
        out.truncate(end);
        truncated = true;
    }
    (substituted, truncated)
}

/// The stock `types.db` type name for a one-data-source list of this value's kind. All four
/// (`counter`, `gauge`, `derive`, `absolute`) are single-data-source types in collectd's own shipped
/// `types.db`, so a receiver resolves them with nothing extra installed.
fn fallback_type(value: DsValue) -> &'static str {
    match value {
        DsValue::Counter(_) => "counter",
        DsValue::Gauge(_) => "gauge",
        DsValue::Derive(_) => "derive",
        DsValue::Absolute(_) => "absolute",
    }
}

/// One record's wire value, or `None` (counted, with a throttled diagnostic) when collectd has no
/// way to carry it. The exhaustive `match` below has one arm per [`MetricKind`] variant and **no
/// wildcard**, on purpose: a new variant must be a compile error here, not a silent drop.
fn resolve_value(record: &MetricRecord, ctx: &mut Ctx) -> Option<DsValue> {
    let name = logit_core::interner::resolve(record.name);

    // Checked before the kind match rather than inside every arm: only `Gauge` has a wire form for
    // "no reading this interval" (NaN), so a flagged point of any other kind is a drop regardless of
    // what its default numeric payload happens to be (`MetricRecord::flags`' own doc).
    if record.is_no_recorded_value() && !matches!(record.kind, MetricKind::Gauge(_)) {
        ctx.drop_no_recorded_value(name);
        return None;
    }

    match &record.kind {
        MetricKind::Sum(sum) => match (sum.temporality, sum.monotonic) {
            (Temporality::Cumulative, true) => as_u64(sum.value, name, ctx).map(DsValue::Counter),
            (Temporality::Cumulative, false) => as_i64(sum.value, name, ctx).map(DsValue::Derive),
            (Temporality::Delta, true) => as_u64(sum.value, name, ctx).map(DsValue::Absolute),
            // ABSOLUTE is collectd's delta-*monotonic* type; there is no delta type that can go
            // down, and DERIVE would relabel the value as cumulative.
            (Temporality::Delta, false) => {
                ctx.drop_kind("non_monotonic_delta_sum", "a delta, non-monotonic Sum", name);
                None
            }
        },
        // A flagged gauge leaves as NaN -- the exact inverse of the decode side's flagged zero.
        MetricKind::Gauge(value) => {
            Some(DsValue::Gauge(if record.is_no_recorded_value() { f64::NAN } else { *value }))
        }
        MetricKind::GaugeDelta(_) => {
            ctx.drop_gauge_delta();
            None
        }
        MetricKind::Samples(_) => {
            ctx.drop_kind("samples", "a raw sample list", name);
            None
        }
        MetricKind::Distribution(_) => {
            ctx.drop_kind("distribution", "a sketched distribution", name);
            None
        }
        MetricKind::SetMembers(_) => {
            ctx.drop_kind("set_members", "a raw set-member list", name);
            None
        }
        MetricKind::Set(_) => {
            ctx.drop_kind("set", "a cardinality estimate", name);
            None
        }
        MetricKind::Histogram(_) => {
            ctx.drop_kind("histogram", "an explicit-bucket histogram", name);
            None
        }
        MetricKind::ExponentialHistogram(_) => {
            ctx.drop_kind("exponential_histogram", "an exponential histogram", name);
            None
        }
        MetricKind::Summary(_) => {
            ctx.drop_kind("summary", "a precomputed summary", name);
            None
        }
    }
}

/// A `Sum`'s `f64` as a `u64`, or `None` (counted) when it is non-finite, fractional, or out of
/// range. Never rounds: statsd's `page.views:2|c|@0.3` reaches a sink as `6.666…`, and both `6` and
/// `7` are numbers nobody sent.
///
/// The bounds are inclusive, and the `as` cast saturates (guaranteed since Rust 1.45) -- which
/// matters at exactly one value: a wire COUNTER of `u64::MAX` decodes to the `f64` `2^64` (the
/// nearest representable double, since `u64::MAX` itself is not), so an exclusive bound would drop
/// the very value it round-tripped from. Everything above `2^53` is already imprecise on the way in
/// (`docs/known-gaps.md`'s shared int/double row); saturating there is the honest end of an
/// already-documented approximation, and `decode(encode(b)) == b` holds across the whole range.
fn as_u64(value: f64, name: &str, ctx: &mut Ctx) -> Option<u64> {
    if value.is_finite() && value.fract() == 0.0 && value >= 0.0 && value <= u64::MAX as f64 {
        return Some(value as u64);
    }
    ctx.drop_unencodable_value(name, value);
    None
}

/// [`as_u64`]'s signed sibling, for DERIVE -- same inclusive bounds, for the same reason
/// (`i64::MAX as f64` is `2^63`; `i64::MIN as f64` is exactly `-2^63`).
fn as_i64(value: f64, name: &str, ctx: &mut Ctx) -> Option<i64> {
    if value.is_finite()
        && value.fract() == 0.0
        && value >= i64::MIN as f64
        && value <= i64::MAX as f64
    {
        return Some(value as i64);
    }
    ctx.drop_unencodable_value(name, value);
    None
}

/// Encodes one value list into `list` (cleared first), eliding every identity part that already
/// matches `last`.
///
/// TimeHR and IntervalHR are written for **every** list, never elided -- collectd's own sender does
/// the same, and the two bytes saved would not be worth a list whose time silently came from an
/// earlier one.
fn write_list(
    list: &mut Vec<u8>,
    last: &Identity,
    cur: &Identity,
    time_cdtime: u64,
    interval_cdtime: u64,
    values: &[DsValue],
) {
    list.clear();
    if cur.host != last.host {
        part::write_string_part(list, part::TYPE_HOST, &cur.host);
    }
    part::write_number_part(list, part::TYPE_TIME_HR, time_cdtime);
    part::write_number_part(list, part::TYPE_INTERVAL_HR, interval_cdtime);
    if cur.plugin != last.plugin {
        part::write_string_part(list, part::TYPE_PLUGIN, &cur.plugin);
    }
    if cur.plugin_instance != last.plugin_instance {
        part::write_string_part(list, part::TYPE_PLUGIN_INSTANCE, &cur.plugin_instance);
    }
    if cur.type_ != last.type_ {
        part::write_string_part(list, part::TYPE_TYPE, &cur.type_);
    }
    if cur.type_instance != last.type_instance {
        part::write_string_part(list, part::TYPE_TYPE_INSTANCE, &cur.type_instance);
    }
    part::write_values_part(list, values);
}

/// Encodes one list and appends it to the packet being packed, flushing the packet first if the list
/// will not fit.
///
/// Every buffer is passed in rather than reached through `&mut self`: `packet`, `list`, `last`,
/// `cur` and `values` are all live simultaneously here, which no `&mut self` method could express.
#[allow(clippy::too_many_arguments)]
fn pack_list(
    packet: &mut Vec<u8>,
    list: &mut Vec<u8>,
    last: &mut Identity,
    cur: &Identity,
    time_cdtime: u64,
    interval_cdtime: u64,
    values: &[DsValue],
    max_packet_bytes: usize,
    lists_in_packet: &mut usize,
    out: &mut Packets,
    ctx: &mut Ctx,
) {
    write_list(list, last, cur, time_cdtime, interval_cdtime, values);

    if !packet.is_empty() && packet.len() + list.len() > max_packet_bytes {
        // Flush, then **encode the same list a second time**. The version just built elided every
        // identity part that matched the previous list *in the packet being flushed*, and a
        // receiver resets its sticky state at each datagram boundary -- so if that elided list led
        // the next datagram, its plugin/type would be whatever that datagram's later parts happen
        // to set, or nothing at all. Encoding at most twice per boundary is the entire cost of
        // getting this right, and it only ever happens on a boundary, not per list.
        out.push(packet, *lists_in_packet);
        packet.clear();
        *lists_in_packet = 0;
        last.clear();
        write_list(list, last, cur, time_cdtime, interval_cdtime, values);
    }

    if list.len() > max_packet_bytes {
        ctx.drop_oversize_list(max_packet_bytes);
        return;
    }

    packet.extend_from_slice(list);
    *lists_in_packet += 1;
    last.clone_from(cur);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collectd::decode::tests::{single_gauge_packet, PacketBuilder, GAUGE_1_5};
    use crate::collectd::{
        CollectdDecoder, ATTR_HOST, ATTR_INTERVAL, ATTR_PLUGIN, ATTR_PLUGIN_INSTANCE, ATTR_TYPE,
        ATTR_TYPE_INSTANCE, DEFAULT_MAX_PACKET_BYTES,
    };
    use crate::Decoder;
    use logit_core::interner::intern;
    use logit_core::telemetry::Registry;
    use logit_core::{AttrMap, Sum};
    use std::sync::Arc;

    const TS: i64 = 1_700_000_000_000_000_000;

    fn attrs(pairs: &[(&str, Value)]) -> AttrMap {
        let mut map = AttrMap::new();
        for (key, value) in pairs {
            map.insert(key, value.clone());
        }
        map
    }

    fn record(name: &str, kind: MetricKind) -> MetricRecord {
        MetricRecord::new(intern(name), kind)
    }

    fn counter_record(name: &str, value: f64) -> MetricRecord {
        record(
            name,
            MetricKind::Sum(Sum { value, temporality: Temporality::Cumulative, monotonic: true }),
        )
    }

    fn batch(events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
    }

    /// A like-relay event: `collectd.*` identity present, so the encoder re-emits it verbatim.
    fn relay_event(records: Vec<MetricRecord>) -> Event {
        let mut event = Event::empty(
            TS,
            attrs(&[
                (ATTR_HOST, Value::from("web-1")),
                (ATTR_PLUGIN, Value::from("load")),
                (ATTR_TYPE, Value::from("load")),
                (ATTR_INTERVAL, Value::F64(10.0)),
            ]),
        );
        for record in records {
            event.metrics.push(record);
        }
        event
    }

    /// Every encoding test runs with a configured hostname: without one, an event carrying neither
    /// `collectd.host` nor `host.name` is dropped outright ([`CollectdEncoder::with_hostname`]'s own
    /// doc), which is its own test below rather than a trap for every other one.
    fn encode(batch: &EventBatch, max_packet_bytes: usize) -> (Packets, EncodeStats) {
        let mut encoder = CollectdEncoder::new().with_hostname("fixture-host");
        let mut packets = Packets::default();
        let stats = encoder.encode_into(batch, max_packet_bytes, &mut packets);
        (packets, stats)
    }

    /// Encodes with live telemetry and diagnostics attached, so a test can assert on both the
    /// aggregate [`EncodeStats`] and the emitted counters.
    fn encode_counted(
        batch: &EventBatch,
        max_packet_bytes: usize,
    ) -> (Packets, EncodeStats, Arc<Registry>, Arc<Registry>) {
        let registry = Registry::new();
        let diag_registry = Registry::new();
        let mut encoder = CollectdEncoder::new()
            .with_hostname("fixture-host")
            .with_telemetry(registry.telemetry_for("collectd_out", "collectd_out", "sink"))
            .with_diagnostics(Diagnostics::new("collectd_out").with_telemetry(
                diag_registry.telemetry_for("collectd_out/diag", "collectd_out", "sink"),
            ));
        let mut packets = Packets::default();
        let stats = encoder.encode_into(batch, max_packet_bytes, &mut packets);
        (packets, stats, registry, diag_registry)
    }

    /// Whether `registry` recorded a point named `metric` carrying `tag`. Drains, so call once.
    fn counted(registry: &Registry, metric: &str, tag: (&str, &str)) -> bool {
        registry.drain(0).iter().any(|event| {
            event.metrics.iter().any(|m| logit_core::interner::resolve(m.name) == metric)
                && event.attributes.get(tag.0).and_then(|v| v.as_str()) == Some(tag.1)
        })
    }

    /// The **total** recorded on `logit.output.metrics.skipped{reason}` -- [`counted`] only answers
    /// "was anything recorded at all", and a per-list count needs the number itself. Drains, so call
    /// it once.
    fn skipped_total(registry: &Registry, reason: &str) -> f64 {
        let events = registry.drain(0);
        let mut total = 0.0;
        for event in &events {
            if event.attributes.get("reason").and_then(|v| v.as_str()) != Some(reason) {
                continue;
            }
            for metric in &event.metrics {
                if logit_core::interner::resolve(metric.name) != "logit.output.metrics.skipped" {
                    continue;
                }
                match &metric.kind {
                    MetricKind::Sum(sum) => total += sum.value,
                    other => panic!("expected a Sum counter, got {other:?}"),
                }
            }
        }
        total
    }

    /// Decodes everything `packets` holds back into events, the way a real `collectd_in` would.
    fn decode_all(packets: &Packets) -> Vec<Event> {
        let mut decoder = CollectdDecoder::new(Arc::new(Resource::default()));
        let mut events = Vec::new();
        for (bytes, _) in packets.iter() {
            decoder
                .decode_into(Bytes::copy_from_slice(bytes), TS, &mut events)
                .expect("every packet this encoder writes must decode");
        }
        events
    }

    fn attr(event: &Event, key: &str) -> Option<Value> {
        event.attributes.get(key).cloned()
    }

    /// How many parts of `part_type` a datagram carries -- a real part walk, never a byte scan:
    /// `TYPE_HOST` is `0x0000`, a pair of bytes that turns up inside half the payloads here.
    fn count_parts(bytes: &[u8], part_type: u16) -> usize {
        let mut at = 0;
        let mut count = 0;
        while let Ok((header, _)) = part::read_part(bytes, at) {
            if header.part_type == part_type {
                count += 1;
            }
            at += header.len;
        }
        count
    }

    // --- round trips ---------------------------------------------------------------------------

    #[test]
    fn a_like_relay_event_re_emits_its_identity_verbatim() {
        let event = relay_event(vec![record("load.load.0", MetricKind::Gauge(0.5))]);
        let (packets, stats) = encode(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(packets.len(), 1);
        assert_eq!(stats, EncodeStats::default());

        let events = decode_all(&packets);
        assert_eq!(events.len(), 1);
        assert_eq!(attr(&events[0], ATTR_HOST), Some(Value::from("web-1")));
        assert_eq!(attr(&events[0], ATTR_PLUGIN), Some(Value::from("load")));
        assert_eq!(attr(&events[0], ATTR_TYPE), Some(Value::from("load")));
        assert_eq!(attr(&events[0], ATTR_INTERVAL), Some(Value::F64(10.0)));
        assert_eq!(events[0].timestamp, TS);
        assert_eq!(events[0].metrics[0].kind, MetricKind::Gauge(0.5));
    }

    /// A decoded packet, re-encoded, must decode to the same events -- the fixed point in miniature
    /// (`tests/collectd_fixed_point.rs` is the exhaustive version).
    #[test]
    fn a_decoded_packet_survives_a_re_encode() {
        let mut decoder = CollectdDecoder::new(Arc::new(Resource::default()));
        let mut events = Vec::new();
        decoder.decode_into(single_gauge_packet(), TS, &mut events).unwrap();
        let first = batch(events);

        let (packets, _) = encode(&first, DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(decode_all(&packets), first.events);
    }

    /// The byte-order assertion, encode side: a gauge is written little-endian.
    #[test]
    fn a_gauge_is_written_little_endian() {
        let event = relay_event(vec![record("load.load", MetricKind::Gauge(1.5))]);
        let (packets, _) = encode(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        let (bytes, _) = packets.iter().next().unwrap();
        assert!(
            bytes.windows(8).any(|window| window == GAUGE_1_5),
            "1.5 must appear as {GAUGE_1_5:02X?} (little-endian), not big-endian"
        );
    }

    #[test]
    fn counter_derive_and_absolute_are_written_big_endian() {
        let event = relay_event(vec![
            counter_record("a.b", 7.0),
            record(
                "a.c",
                MetricKind::Sum(Sum {
                    value: -1.0,
                    temporality: Temporality::Cumulative,
                    monotonic: false,
                }),
            ),
            record(
                "a.d",
                MetricKind::Sum(Sum {
                    value: 9.0,
                    temporality: Temporality::Delta,
                    monotonic: true,
                }),
            ),
        ]);
        let (packets, _) = encode(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        let (bytes, _) = packets.iter().next().unwrap();
        for expected in [
            [0u8, 0, 0, 0, 0, 0, 0, 7],
            [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            [0, 0, 0, 0, 0, 0, 0, 9],
        ] {
            assert!(
                bytes.windows(8).any(|window| window == expected),
                "expected {expected:02X?} big-endian in the packet"
            );
        }
    }

    // --- packing, elision, boundaries ----------------------------------------------------------

    #[test]
    fn identity_parts_are_elided_after_the_first_list_in_a_packet() {
        let first = relay_event(vec![record("load.load", MetricKind::Gauge(0.5))]);
        let mut second = first.clone();
        second.attributes.insert(ATTR_TYPE_INSTANCE, Value::from("short"));
        let (packets, _) = encode(&batch(vec![first, second]), DEFAULT_MAX_PACKET_BYTES);

        assert_eq!(packets.len(), 1, "both lists fit one datagram");
        let (bytes, lists) = packets.iter().next().unwrap();
        assert_eq!(lists, 2);
        // One Host/Plugin/Type part for two lists is the whole point of elision; the differing
        // TypeInstance is the one identity part the second list still has to write.
        assert_eq!(count_parts(bytes, part::TYPE_HOST), 1, "the second list must elide Host");
        assert_eq!(count_parts(bytes, part::TYPE_PLUGIN), 1);
        assert_eq!(count_parts(bytes, part::TYPE_TYPE), 1);
        assert_eq!(count_parts(bytes, part::TYPE_TYPE_INSTANCE), 1, "only the second list has one");
        assert_eq!(count_parts(bytes, part::TYPE_VALUES), 2);
        assert_eq!(decode_all(&packets).len(), 2);
    }

    /// The re-encode `pack_list` exists for: the first list of a *new* datagram carries its full
    /// identity, because the receiver's sticky state resets at the boundary.
    #[test]
    fn the_first_list_of_every_datagram_carries_its_full_identity() {
        // Two lists that cannot share a datagram: a cap just above one list's size.
        let one = relay_event(vec![record("load.load", MetricKind::Gauge(0.5))]);
        let (single, _) = encode(&batch(vec![one.clone()]), DEFAULT_MAX_PACKET_BYTES);
        let list_bytes = single.total_bytes();

        let mut second = one.clone();
        second.attributes.insert(ATTR_HOST, Value::from("web-1"));
        second.metrics[0] = record("load.load", MetricKind::Gauge(0.75));
        let (packets, stats) = encode(&batch(vec![one, second]), list_bytes);

        assert_eq!(packets.len(), 2, "the cap fits exactly one list per datagram");
        assert_eq!(stats.dropped_oversize_list, 0);
        for (bytes, lists) in packets.iter() {
            assert_eq!(lists, 1);
            assert_eq!(
                count_parts(bytes, part::TYPE_HOST),
                1,
                "a datagram's first list must not elide its identity"
            );
            assert_eq!(count_parts(bytes, part::TYPE_PLUGIN), 1);
            assert_eq!(count_parts(bytes, part::TYPE_TYPE), 1);
        }
        // And the values survive: without the re-encode the second datagram would have no plugin.
        let events = decode_all(&packets);
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].metrics[0].kind, MetricKind::Gauge(0.75));
        assert_eq!(attr(&events[1], ATTR_PLUGIN), Some(Value::from("load")));
    }

    #[test]
    fn a_list_larger_than_the_cap_on_its_own_is_dropped_whole() {
        let event = relay_event(vec![record("load.load", MetricKind::Gauge(0.5))]);
        let (packets, stats, registry, diag_registry) = encode_counted(&batch(vec![event]), 8);
        assert!(packets.is_empty());
        assert_eq!(stats.dropped_oversize_list, 1);
        assert!(counted(
            &registry,
            "logit.output.metrics.skipped",
            ("reason", "oversize_value_list")
        ));
        assert!(counted(
            &diag_registry,
            "logit.component.diagnostics",
            ("key", "oversize_value_list")
        ));
    }

    #[test]
    fn many_lists_pack_into_several_datagrams_under_the_cap() {
        let events: Vec<Event> = (0..40)
            .map(|i| {
                let mut event = relay_event(vec![record("load.load", MetricKind::Gauge(i as f64))]);
                event.attributes.insert(ATTR_TYPE_INSTANCE, Value::str(format!("i{i}")));
                event
            })
            .collect();
        let (packets, stats) = encode(&batch(events), 256);
        assert_eq!(stats, EncodeStats::default(), "nothing is dropped, only repacked");
        assert!(packets.len() > 1, "40 lists cannot fit one 256-byte datagram");
        for (bytes, _) in packets.iter() {
            assert!(bytes.len() <= 256, "a datagram exceeded the cap");
        }
        let decoded = decode_all(&packets);
        assert_eq!(decoded.len(), 40);
        let total: usize = packets.iter().map(|(_, lists)| lists).sum();
        assert_eq!(total, 40, "the per-datagram list counts must add up to the lists written");
    }

    #[test]
    fn an_empty_batch_produces_no_packets() {
        let (packets, stats) = encode(&batch(Vec::new()), DEFAULT_MAX_PACKET_BYTES);
        assert!(packets.is_empty());
        assert_eq!(stats, EncodeStats::default());
    }

    #[test]
    fn encode_into_clears_its_output_rather_than_appending_to_it() {
        let event = relay_event(vec![record("load.load", MetricKind::Gauge(0.5))]);
        let batch = batch(vec![event]);
        let mut encoder = CollectdEncoder::new();
        let mut packets = Packets::default();
        encoder.encode_into(&batch, DEFAULT_MAX_PACKET_BYTES, &mut packets);
        let first = packets.total_bytes();
        encoder.encode_into(&batch, DEFAULT_MAX_PACKET_BYTES, &mut packets);
        assert_eq!(packets.total_bytes(), first, "a second call must not append to the first");
        assert_eq!(packets.len(), 1);
    }

    // --- fallback naming -----------------------------------------------------------------------

    #[test]
    fn a_fallback_event_derives_plugin_type_and_type_instance_from_the_record_name() {
        let mut event = Event::empty(TS, AttrMap::new());
        event.metrics.push(counter_record("nginx.requests.total", 42.0));
        event.metrics.push(record("uptime", MetricKind::Gauge(3.5)));
        let (packets, stats) = encode(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(stats, EncodeStats::default());

        let events = decode_all(&packets);
        assert_eq!(events.len(), 2, "each record becomes its own single-data-source list");
        assert_eq!(attr(&events[0], ATTR_PLUGIN), Some(Value::from("nginx")));
        assert_eq!(attr(&events[0], ATTR_TYPE), Some(Value::from("counter")));
        assert_eq!(attr(&events[0], ATTR_TYPE_INSTANCE), Some(Value::from("requests.total")));
        assert_eq!(attr(&events[0], ATTR_PLUGIN_INSTANCE), None);
        // A name with no `.` at all: the whole name is the plugin, no type_instance.
        assert_eq!(attr(&events[1], ATTR_PLUGIN), Some(Value::from("uptime")));
        assert_eq!(attr(&events[1], ATTR_TYPE), Some(Value::from("gauge")));
        assert_eq!(attr(&events[1], ATTR_TYPE_INSTANCE), None);
    }

    #[test]
    fn every_fallback_type_name_matches_its_data_source_kind() {
        let mut event = Event::empty(TS, AttrMap::new());
        event.metrics.push(counter_record("a", 1.0));
        event.metrics.push(record("b", MetricKind::Gauge(1.0)));
        event.metrics.push(record(
            "c",
            MetricKind::Sum(Sum {
                value: 1.0,
                temporality: Temporality::Cumulative,
                monotonic: false,
            }),
        ));
        event.metrics.push(record(
            "d",
            MetricKind::Sum(Sum { value: 1.0, temporality: Temporality::Delta, monotonic: true }),
        ));
        let (packets, _) = encode(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        let types: Vec<Option<Value>> =
            decode_all(&packets).iter().map(|event| attr(event, ATTR_TYPE)).collect();
        assert_eq!(
            types,
            vec![
                Some(Value::from("counter")),
                Some(Value::from("gauge")),
                Some(Value::from("derive")),
                Some(Value::from("absolute")),
            ]
        );
    }

    // --- the kind table ------------------------------------------------------------------------

    #[test]
    fn a_delta_non_monotonic_sum_is_dropped_and_counted_by_kind() {
        let mut event = Event::empty(TS, AttrMap::new());
        event.metrics.push(record(
            "a.b",
            MetricKind::Sum(Sum { value: 1.0, temporality: Temporality::Delta, monotonic: false }),
        ));
        let (packets, stats, registry, _) = encode_counted(&batch(vec![event]), 1452);
        assert!(packets.is_empty());
        assert_eq!(stats.dropped_unsupported_kind, 1);
        assert!(counted(
            &registry,
            "logit.output.metrics.skipped",
            ("metric_kind", "non_monotonic_delta_sum")
        ));
    }

    #[test]
    fn a_non_integral_or_out_of_range_sum_is_dropped_as_unencodable() {
        for value in [6.666, f64::INFINITY, f64::NAN, -1.0, 1e30] {
            let mut event = Event::empty(TS, AttrMap::new());
            event.metrics.push(counter_record("a.b", value));
            let (packets, stats, registry, diag_registry) =
                encode_counted(&batch(vec![event]), 1452);
            assert!(packets.is_empty(), "{value} must not be encoded");
            assert_eq!(stats.dropped_unencodable_value, 1, "{value}");
            assert!(counted(
                &registry,
                "logit.output.metrics.skipped",
                ("reason", "unencodable_value")
            ));
            assert!(counted(
                &diag_registry,
                "logit.component.diagnostics",
                ("key", "unencodable_value")
            ));
        }
    }

    /// A negative DERIVE is legal (it is a signed type), and `u64::MAX`-adjacent counters are not.
    #[test]
    fn a_negative_derive_encodes_where_a_negative_counter_does_not() {
        let mut event = Event::empty(TS, AttrMap::new());
        event.metrics.push(record(
            "if.octets",
            MetricKind::Sum(Sum {
                value: -4096.0,
                temporality: Temporality::Cumulative,
                monotonic: false,
            }),
        ));
        let (packets, stats) = encode(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(stats, EncodeStats::default());
        assert_eq!(
            decode_all(&packets)[0].metrics[0].kind,
            MetricKind::Sum(Sum {
                value: -4096.0,
                temporality: Temporality::Cumulative,
                monotonic: false
            })
        );
    }

    /// The one value the integer bounds have to be *inclusive* for: a wire `u64::MAX` COUNTER
    /// decodes to the `f64` 2^64 (the nearest representable double), which an exclusive bound would
    /// then refuse to re-encode -- dropping the very value it just round-tripped from.
    #[test]
    fn a_counter_at_the_top_of_the_u64_range_survives_a_round_trip() {
        let event = relay_event(vec![
            counter_record("load.load.0", u64::MAX as f64),
            record(
                "load.load.1",
                MetricKind::Sum(Sum {
                    value: i64::MAX as f64,
                    temporality: Temporality::Cumulative,
                    monotonic: false,
                }),
            ),
        ]);
        let (packets, stats) = encode(&batch(vec![event.clone()]), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(stats, EncodeStats::default());
        assert_eq!(decode_all(&packets), vec![event]);
    }

    #[test]
    fn a_gauge_delta_is_dropped_under_the_shared_diagnostic_key() {
        let mut event = Event::empty(TS, AttrMap::new());
        event.metrics.push(record("a.b", MetricKind::GaugeDelta(5.0)));
        let (packets, stats, registry, diag_registry) = encode_counted(&batch(vec![event]), 1452);
        assert!(packets.is_empty());
        assert_eq!(stats.dropped_gauge_delta, 1);
        assert!(counted(&registry, "logit.output.metrics.skipped", ("metric_kind", "gauge_delta")));
        assert!(counted(
            &diag_registry,
            "logit.component.diagnostics",
            ("key", "gauge_delta_unresolved")
        ));
    }

    #[test]
    fn every_post_summarization_kind_is_dropped_with_its_own_tag() {
        let cases: Vec<(MetricKind, &str)> = vec![
            (MetricKind::Samples(logit_core::Samples::new([1.0])), "samples"),
            (MetricKind::Distribution(logit_core::DdSketch::new()), "distribution"),
            (MetricKind::SetMembers(vec![Bytes::from_static(b"a")]), "set_members"),
            (MetricKind::Set(logit_core::HyperLogLog::new()), "set"),
            (
                MetricKind::Histogram(logit_core::Histogram {
                    buckets: vec![(1.0, 1)],
                    temporality: Temporality::Cumulative,
                    sum: None,
                    min: None,
                    max: None,
                }),
                "histogram",
            ),
            (
                MetricKind::ExponentialHistogram(logit_core::ExpHistogram {
                    scale: 0,
                    zero_count: 0,
                    zero_threshold: 0.0,
                    positive: (0, vec![1]),
                    negative: (0, vec![]),
                    temporality: Temporality::Cumulative,
                    count: 1,
                    sum: None,
                    min: None,
                    max: None,
                }),
                "exponential_histogram",
            ),
            (
                MetricKind::Summary(logit_core::Summary {
                    quantiles: vec![(0.5, 1.0)],
                    count: 1,
                    sum: 1.0,
                }),
                "summary",
            ),
        ];
        for (kind, tag) in cases {
            let mut event = Event::empty(TS, AttrMap::new());
            event.metrics.push(record("a.b", kind));
            let (packets, stats, registry, _) = encode_counted(&batch(vec![event]), 1452);
            assert!(packets.is_empty(), "{tag} must not be encoded");
            assert_eq!(stats.dropped_unsupported_kind, 1, "{tag}");
            assert!(
                counted(&registry, "logit.output.metrics.skipped", ("metric_kind", tag)),
                "{tag} must be counted under its own metric_kind tag"
            );
        }
    }

    /// A flagged gauge is the one flagged point with a wire form: NaN, which decodes back to a
    /// flagged zero gauge.
    #[test]
    fn a_flagged_gauge_round_trips_through_a_nan_and_a_flagged_non_gauge_is_dropped() {
        let mut event = relay_event(vec![MetricRecord {
            flags: MetricRecord::FLAG_NO_RECORDED_VALUE,
            ..record("load.load", MetricKind::Gauge(0.0))
        }]);
        let (packets, stats) = encode(&batch(vec![event.clone()]), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(stats, EncodeStats::default());
        let decoded = decode_all(&packets);
        assert_eq!(decoded[0].metrics[0].kind, MetricKind::Gauge(0.0));
        assert!(decoded[0].metrics[0].is_no_recorded_value());

        event.metrics[0] = MetricRecord {
            flags: MetricRecord::FLAG_NO_RECORDED_VALUE,
            ..counter_record("load.load", 1.0)
        };
        let (packets, stats, registry, diag_registry) =
            encode_counted(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        assert!(packets.is_empty());
        assert_eq!(stats.dropped_no_recorded_value, 1);
        assert!(counted(
            &registry,
            "logit.output.metrics.skipped",
            ("reason", "no_recorded_value")
        ));
        assert!(counted(
            &diag_registry,
            "logit.component.diagnostics",
            ("key", "no_recorded_value")
        ));
    }

    /// A like-relay list is all-or-nothing: one bad record takes the list, counted once.
    #[test]
    fn a_like_relay_list_with_one_failing_record_is_dropped_whole_and_counted_once() {
        let event = relay_event(vec![
            record("load.load.0", MetricKind::Gauge(0.5)),
            counter_record("load.load.1", 6.666),
            record("load.load.2", MetricKind::Gauge(0.7)),
        ]);
        let (packets, stats) = encode(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        assert!(
            packets.is_empty(),
            "a partial list would be rejected by collectd's own ds_num check"
        );
        assert_eq!(stats.dropped_unencodable_value, 1, "counted once, not once per record");
    }

    /// The fallback path is per record, so a bad record there costs only its own list.
    #[test]
    fn a_failing_fallback_record_costs_only_its_own_list() {
        let mut event = Event::empty(TS, AttrMap::new());
        event.metrics.push(counter_record("a.good", 1.0));
        event.metrics.push(counter_record("a.bad", 6.666));
        event.metrics.push(counter_record("a.alsogood", 2.0));
        let (packets, stats) = encode(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(stats.dropped_unencodable_value, 1);
        assert_eq!(decode_all(&packets).len(), 2);
    }

    // --- timestamps, intervals, hosts, sanitization ---------------------------------------------

    #[test]
    fn a_non_positive_timestamp_drops_the_whole_event() {
        for timestamp in [0i64, -1] {
            let mut event = relay_event(vec![record("load.load", MetricKind::Gauge(0.5))]);
            event.timestamp = timestamp;
            let (packets, stats, registry, _) =
                encode_counted(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
            assert!(packets.is_empty(), "timestamp {timestamp}");
            assert_eq!(stats.dropped_unencodable_timestamp, 1);
            assert!(counted(
                &registry,
                "logit.output.metrics.skipped",
                ("reason", "unencodable_timestamp")
            ));
        }
    }

    #[test]
    fn an_absent_interval_is_written_as_zero_and_decodes_back_absent() {
        let mut event = relay_event(vec![record("load.load", MetricKind::Gauge(0.5))]);
        event.attributes.remove(ATTR_INTERVAL);
        let (packets, stats) = encode(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(stats, EncodeStats::default());
        assert_eq!(attr(&decode_all(&packets)[0], ATTR_INTERVAL), None);
    }

    #[test]
    fn a_non_f64_or_non_positive_interval_is_counted_unrepresentable_and_written_as_zero() {
        for value in [Value::from("10"), Value::F64(0.0), Value::F64(f64::NAN), Value::I64(10)] {
            let mut event = relay_event(vec![record("load.load", MetricKind::Gauge(0.5))]);
            event.attributes.insert(ATTR_INTERVAL, value.clone());
            let (packets, stats, registry, _) =
                encode_counted(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
            assert_eq!(stats.tags_dropped_unrepresentable, 1, "{value:?}");
            assert_eq!(attr(&decode_all(&packets)[0], ATTR_INTERVAL), None);
            assert!(counted(&registry, "logit.output.tags.dropped", ("reason", "unrepresentable")));
        }
    }

    #[test]
    fn the_host_falls_back_from_collectd_host_to_host_name_to_the_configured_hostname() {
        // `collectd.host` wins.
        let mut event = relay_event(vec![record("load.load", MetricKind::Gauge(0.5))]);
        event.attributes.insert("host.name", Value::from("ignored"));
        let (packets, _) = encode(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(attr(&decode_all(&packets)[0], ATTR_HOST), Some(Value::from("web-1")));

        // `host.name` next.
        let mut event = relay_event(vec![record("load.load", MetricKind::Gauge(0.5))]);
        event.attributes.remove(ATTR_HOST);
        event.attributes.insert("host.name", Value::from("from-host-name"));
        let (packets, stats) = encode(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(attr(&decode_all(&packets)[0], ATTR_HOST), Some(Value::from("from-host-name")));
        assert_eq!(stats.tags_dropped_no_wire_form, 1, "`host.name` is read but still dropped");

        // Then the configured hostname.
        let mut event = relay_event(vec![record("load.load", MetricKind::Gauge(0.5))]);
        event.attributes.remove(ATTR_HOST);
        let (packets, _) = encode(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(attr(&decode_all(&packets)[0], ATTR_HOST), Some(Value::from("fixture-host")));
    }

    /// A whole-event drop is counted per value **list** the event would have produced, so
    /// `metrics.skipped` means the same thing here as at every other sink: a three-record fallback
    /// event would have been three one-source lists, and losing it loses three.
    #[test]
    fn a_whole_event_drop_is_counted_once_per_list_the_event_would_have_produced() {
        let mut fallback = Event::empty(TS, AttrMap::new());
        for name in ["a.one", "a.two", "a.three"] {
            fallback.metrics.push(counter_record(name, 1.0));
        }

        for (label, mut event, expected) in [
            ("fallback, 3 records", fallback.clone(), 3),
            // A like-relay event is a single list however many records it carries, so it counts 1.
            (
                "like-relay, 3 records",
                relay_event(vec![
                    record("load.load.0", MetricKind::Gauge(0.1)),
                    record("load.load.1", MetricKind::Gauge(0.2)),
                    record("load.load.2", MetricKind::Gauge(0.3)),
                ]),
                1,
            ),
        ] {
            // No host anywhere and no configured hostname.
            event.attributes.remove(ATTR_HOST);
            let registry = Registry::new();
            let mut encoder = CollectdEncoder::new().with_telemetry(registry.telemetry_for(
                "collectd_out",
                "collectd_out",
                "sink",
            ));
            let mut packets = Packets::default();
            let stats = encoder.encode_into(
                &batch(vec![event.clone()]),
                DEFAULT_MAX_PACKET_BYTES,
                &mut packets,
            );
            assert!(packets.is_empty(), "{label}");
            assert_eq!(stats.dropped_no_host, expected, "{label}: no_host stat");
            assert_eq!(
                skipped_total(&registry, "no_host"),
                expected as f64,
                "{label}: no_host counter"
            );

            // The same rule for the other whole-event drop.
            event.timestamp = 0;
            event.attributes.insert(ATTR_HOST, Value::from("web-1"));
            let (packets, stats, registry, _) =
                encode_counted(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
            assert!(packets.is_empty(), "{label}");
            assert_eq!(stats.dropped_unencodable_timestamp, expected, "{label}: timestamp stat");
            assert_eq!(
                skipped_total(&registry, "unencodable_timestamp"),
                expected as f64,
                "{label}: timestamp counter"
            );
        }
    }

    /// The encode-side half of `MAX_VALUES_PER_LIST`. Without it a 65-record like-relay event fits
    /// under the byte cap, encodes with `EncodeStats::default()`, and is then rejected as a
    /// malformed part by any receiver running this codec -- taking every list packed behind it in
    /// that datagram with it.
    #[test]
    fn a_like_relay_list_over_the_value_cap_is_dropped_whole_and_counted() {
        let records: Vec<MetricRecord> = (0..MAX_VALUES_PER_LIST + 1)
            .map(|i| record("load.load", MetricKind::Gauge(i as f64)))
            .collect();
        let (packets, stats, registry, diag_registry) =
            encode_counted(&batch(vec![relay_event(records)]), DEFAULT_MAX_PACKET_BYTES);
        assert!(packets.is_empty(), "65 data sources must not reach the wire");
        assert_eq!(stats.dropped_too_many_values, 1, "one list dropped, not one per record");
        assert!(counted(&registry, "logit.output.metrics.skipped", ("reason", "too_many_values")));
        assert!(counted(&diag_registry, "logit.component.diagnostics", ("key", "too_many_values")));
    }

    /// And the value exactly at the cap still round-trips -- an off-by-one here would silently drop
    /// every 64-source list instead.
    #[test]
    fn a_like_relay_list_exactly_at_the_value_cap_still_round_trips() {
        let records: Vec<MetricRecord> = (0..MAX_VALUES_PER_LIST)
            .map(|i| record(&format!("load.load.{i}"), MetricKind::Gauge(i as f64)))
            .collect();
        let event = relay_event(records);
        let (packets, stats) = encode(&batch(vec![event.clone()]), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(stats, EncodeStats::default());
        let decoded = decode_all(&packets);
        assert_eq!(decoded, vec![event]);
        assert_eq!(decoded[0].metrics.len(), MAX_VALUES_PER_LIST);
    }

    /// No host anywhere -- and no invented one either. The event is dropped, counted and named in a
    /// diagnostic that tells the operator exactly which two knobs fix it.
    #[test]
    fn an_event_with_no_host_and_no_configured_hostname_is_dropped_and_counted() {
        let mut event = relay_event(vec![record("load.load", MetricKind::Gauge(0.5))]);
        event.attributes.remove(ATTR_HOST);

        let registry = Registry::new();
        let diag_registry = Registry::new();
        let mut encoder = CollectdEncoder::new()
            .with_telemetry(registry.telemetry_for("collectd_out", "collectd_out", "sink"))
            .with_diagnostics(Diagnostics::new("collectd_out").with_telemetry(
                diag_registry.telemetry_for("collectd_out/diag", "collectd_out", "sink"),
            ));
        let mut packets = Packets::default();
        let stats =
            encoder.encode_into(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES, &mut packets);

        assert!(packets.is_empty());
        assert_eq!(stats.dropped_no_host, 1);
        assert!(counted(&registry, "logit.output.metrics.skipped", ("reason", "no_host")));
        assert!(counted(&diag_registry, "logit.component.diagnostics", ("key", "no_host")));
    }

    /// An empty, or entirely substituted-away, configured hostname is the same as not configuring
    /// one -- it must not become a literal `_` host on the wire.
    #[test]
    fn an_empty_or_substituted_away_hostname_counts_as_unconfigured() {
        for hostname in ["", "\0"] {
            let mut event = relay_event(vec![record("load.load", MetricKind::Gauge(0.5))]);
            event.attributes.insert(ATTR_HOST, Value::from(""));
            let mut encoder = CollectdEncoder::new().with_hostname(hostname);
            let mut packets = Packets::default();
            let stats =
                encoder.encode_into(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES, &mut packets);
            if hostname.is_empty() {
                assert!(packets.is_empty(), "an empty hostname is not a host");
                assert_eq!(stats.dropped_no_host, 1);
            } else {
                // `\0` sanitizes to `_`, which is a real (if odd) host the operator asked for --
                // substitution never deletes, so this stays configured rather than becoming empty.
                assert_eq!(attr(&decode_all(&packets)[0], ATTR_HOST), Some(Value::from("_")));
            }
        }
    }

    /// Normalization (8): `/` and NUL become `_`, counted. This is the one identity transformation
    /// that is deliberately **not** a fixed point -- a wire string carrying `/` comes back carrying
    /// `_` -- which is exactly why `tests/collectd_fixed_point.rs`'s generated grammar never
    /// produces one, and why this test exists in its place.
    #[test]
    fn a_slash_or_nul_in_an_identity_field_becomes_an_underscore_and_is_counted() {
        for (raw, expected) in [("a/b", "a_b"), ("sda/1\0x", "sda_1_x")] {
            let mut event = relay_event(vec![record("load.load", MetricKind::Gauge(0.5))]);
            event.attributes.insert(ATTR_TYPE_INSTANCE, Value::from(raw));
            let (packets, stats, registry, _) =
                encode_counted(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
            assert_eq!(
                attr(&decode_all(&packets)[0], ATTR_TYPE_INSTANCE),
                Some(Value::from(expected)),
                "{raw:?}"
            );
            assert_eq!(
                stats.identity_sanitized_substituted, 1,
                "counted once per field, not once per byte"
            );
            assert!(counted(
                &registry,
                "logit.output.identity.sanitized",
                ("reason", "substituted")
            ));
        }
    }

    #[test]
    fn an_over_long_identity_field_is_truncated_on_a_character_boundary() {
        // 64 two-byte characters = 128 bytes, so the 127-byte limit falls mid-character.
        let long: String = "é".repeat(64);
        let mut event = relay_event(vec![record("load.load", MetricKind::Gauge(0.5))]);
        event.attributes.insert(ATTR_TYPE_INSTANCE, Value::str(long));
        let (packets, stats, registry, _) =
            encode_counted(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(stats.identity_sanitized_truncated, 1);
        assert!(counted(&registry, "logit.output.identity.sanitized", ("reason", "truncated")));
        let instance = attr(&decode_all(&packets)[0], ATTR_TYPE_INSTANCE).unwrap();
        // 63 characters (126 bytes) -- a `Value::Str`, not a `Value::Bytes`, which is what proves
        // the truncation landed on a character boundary.
        assert_eq!(instance, Value::str("é".repeat(63)));
    }

    #[test]
    fn a_non_utf8_identity_field_truncates_on_a_byte_boundary_and_stays_bytes() {
        let raw = Bytes::from(vec![0xFFu8; 200]);
        let mut event = relay_event(vec![record("load.load", MetricKind::Gauge(0.5))]);
        event.attributes.insert(ATTR_TYPE_INSTANCE, Value::Bytes(raw));
        let (packets, stats) = encode(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(stats.identity_sanitized_truncated, 1);
        assert_eq!(
            attr(&decode_all(&packets)[0], ATTR_TYPE_INSTANCE),
            Some(Value::Bytes(Bytes::from(vec![0xFFu8; MAX_IDENTITY_BYTES])))
        );
    }

    #[test]
    fn an_empty_plugin_or_type_drops_the_list() {
        for key in [ATTR_PLUGIN, ATTR_TYPE] {
            // An outright empty value, not a `/`: substitution never deletes, so `/` would sanitize
            // to the perfectly usable `_` rather than to nothing.
            let mut event = relay_event(vec![record("load.load", MetricKind::Gauge(0.5))]);
            event.attributes.insert(key, Value::from(""));
            let (packets, stats, registry, _) =
                encode_counted(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
            assert!(packets.is_empty(), "{key}");
            assert_eq!(stats.dropped_empty_name, 1);
            assert!(counted(&registry, "logit.output.metrics.skipped", ("reason", "empty_name")));
        }
    }

    #[test]
    fn a_non_collectd_attribute_is_dropped_and_counted_once_per_attribute() {
        let mut event = relay_event(vec![record("load.load", MetricKind::Gauge(0.5))]);
        event.attributes.insert("env", Value::from("prod"));
        event.attributes.insert("region", Value::from("us-east"));
        let (packets, stats, registry, _) =
            encode_counted(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(stats.tags_dropped_no_wire_form, 2);
        assert!(counted(&registry, "logit.output.tags.dropped", ("reason", "no_wire_form")));
        let decoded = decode_all(&packets);
        assert_eq!(attr(&decoded[0], "env"), None, "collectd has no tag concept at all");
    }

    #[test]
    fn a_collectd_identity_carrier_of_the_wrong_value_type_is_counted_unrepresentable() {
        let mut event = relay_event(vec![record("load.load", MetricKind::Gauge(0.5))]);
        event.attributes.insert(ATTR_PLUGIN_INSTANCE, Value::I64(7));
        let (packets, stats) = encode(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(stats.tags_dropped_unrepresentable, 1);
        assert_eq!(attr(&decode_all(&packets)[0], ATTR_PLUGIN_INSTANCE), None);
    }

    #[test]
    fn an_event_with_no_metrics_is_skipped_without_being_counted_as_a_loss() {
        let event = Event::empty(TS, attrs(&[(ATTR_HOST, Value::from("web-1"))]));
        let (packets, stats) = encode(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        assert!(packets.is_empty());
        assert_eq!(stats, EncodeStats { skipped_no_metrics: 1, ..EncodeStats::default() });
    }

    /// A resource-level `collectd.*` carrier works, and an event-level one wins over it -- the
    /// ordinary merge, nothing collectd-specific.
    #[test]
    fn resource_carriers_apply_and_event_carriers_win() {
        let mut resource_attrs = AttrMap::new();
        resource_attrs.insert(ATTR_HOST, Value::from("from-resource"));
        resource_attrs.insert(ATTR_PLUGIN, Value::from("from-resource"));
        let resource = Arc::new(Resource { attributes: resource_attrs, ..Resource::default() });

        let mut event = Event::empty(
            TS,
            attrs(&[(ATTR_PLUGIN, Value::from("from-event")), (ATTR_TYPE, Value::from("gauge"))]),
        );
        event.metrics.push(record("x", MetricKind::Gauge(1.0)));
        let batch = EventBatch { resource, scope: None, events: vec![event] };
        let (packets, _) = encode(&batch, DEFAULT_MAX_PACKET_BYTES);
        let decoded = decode_all(&packets);
        assert_eq!(attr(&decoded[0], ATTR_HOST), Some(Value::from("from-resource")));
        assert_eq!(attr(&decoded[0], ATTR_PLUGIN), Some(Value::from("from-event")));
    }

    /// Every packet this encoder writes must be readable by a real decoder, byte for byte -- a
    /// weaker but much broader guard than any single assertion above, over a hand-built input.
    #[test]
    fn a_hand_built_multi_list_packet_survives_decode_encode_decode() {
        let bytes = PacketBuilder::new()
            .string(part::TYPE_HOST, b"web-1")
            .number(part::TYPE_TIME_HR, 1_700_000_000u64 << 30)
            .number(part::TYPE_INTERVAL_HR, 10u64 << 30)
            .string(part::TYPE_PLUGIN, b"cpu")
            .string(part::TYPE_PLUGIN_INSTANCE, b"0")
            .string(part::TYPE_TYPE, b"cpu")
            .string(part::TYPE_TYPE_INSTANCE, b"user")
            .values(&[(part::DS_DERIVE, 12345u64.to_be_bytes())])
            .string(part::TYPE_TYPE_INSTANCE, b"system")
            .values(&[(part::DS_DERIVE, 678u64.to_be_bytes())])
            .build();

        let mut decoder = CollectdDecoder::new(Arc::new(Resource::default()));
        let mut first = Vec::new();
        decoder.decode_into(bytes, TS, &mut first).unwrap();
        assert_eq!(first.len(), 2);

        let (packets, stats) = encode(&batch(first.clone()), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(stats, EncodeStats::default());
        assert_eq!(decode_all(&packets), first);
    }
}
