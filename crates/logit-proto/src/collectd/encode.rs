//! Encoding a batch of events back into collectd datagrams: the encode half of [`super`]'s module
//! doc, which is the spec for everything here.
//!
//! No socket, so packing, elision, and sanitization tests run directly against
//! [`CollectdEncoder`]. [`super`]'s module doc says why it is a [`crate::FramedEncoder`] of packed
//! datagrams, each entry's meta the number of messages it carries.

use super::part::{self, DsValue};
use super::{
    nanos_to_cdtime, ATTR_PREFIX, ATTR_SEVERITY, CDTIME_ONE_SECOND, DATA_MAX_NAME_LEN,
    MAX_VALUES_PER_LIST, NOTIF_MAX_MSG_LEN,
};
use crate::{FramedEncoder, MessageBuf};
use bytes::Bytes;
use logit_core::{
    Diagnostics, Event, EventBatch, LogRecord, MetricKind, MetricRecord, Resource, Telemetry,
    Temporality, Value,
};

/// Usable bytes in an identity field: [`DATA_MAX_NAME_LEN`] minus the NUL. A longer field makes
/// collectd reject the **whole packet**, so the encoder truncates.
const MAX_IDENTITY_BYTES: usize = DATA_MAX_NAME_LEN - 1;

/// Per-batch outcome counts from [`CollectdEncoder::encode_into`], for tests and benches.
///
/// The codec emits its `logit.output.*` counters and diagnostics itself at each drop site (see
/// [`Ctx`]), so `collectd_out` discards this value, unlike `statsd_out`, whose sink turns its
/// encoder's stats into telemetry.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EncodeStats {
    /// Metrics-empty events that aren't notification attempts (a span, or a log without
    /// `collectd.severity`). Not a loss: nothing to carry.
    pub skipped_no_metrics: usize,
    /// A metric kind collectd has no data-source type for (every post-summarization kind, plus a
    /// delta non-monotonic `Sum`), tagged with the kind.
    pub dropped_unsupported_kind: usize,
    /// A `Sum` that is non-finite, has a fractional part, or falls outside its target integer
    /// range. collectd's COUNTER/DERIVE/ABSOLUTE are integers; rounding would fabricate.
    pub dropped_unencodable_value: usize,
    /// A `NO_RECORDED_VALUE`-flagged point of any kind **other than** `Gauge` (which encodes as
    /// NaN).
    pub dropped_no_recorded_value: usize,
    /// A `MetricKind::GaugeDelta`, which means a missing `aggregate` stage, not a bad metric.
    pub dropped_gauge_delta: usize,
    /// An event whose `timestamp` is zero or negative: there is no cdtime before the epoch, and
    /// stamping "now" would invent an instant.
    ///
    /// Counted once per value **list** the event would have produced (1 for like-relay, one per
    /// record for fallback), so it means what every other sink's `metrics.skipped` means.
    pub dropped_unencodable_timestamp: usize,
    /// An event with no `collectd.host`, no `host.name`, and no
    /// [`CollectdEncoder::with_hostname`]. Counted per value list, like
    /// [`Self::dropped_unencodable_timestamp`].
    pub dropped_no_host: usize,
    /// A like-relay event carrying more than [`MAX_VALUES_PER_LIST`] records. Counted once per
    /// list, however many records it held.
    pub dropped_too_many_values: usize,
    /// A list whose plugin or type sanitized to nothing, which collectd's receiver rejects.
    pub dropped_empty_name: usize,
    /// A single value list larger than `max_packet_bytes`: dropped whole, never split across
    /// datagrams (a split list would be dispatched against the wrong identity).
    pub dropped_oversize_list: usize,
    /// An attribute outside the `collectd.` namespace (collectd has no tags), once per attribute
    /// per event, including `host.name`, which host resolution reads but can't carry as itself.
    pub tags_dropped_no_wire_form: usize,
    /// A `collectd.*` attribute with no wire form here: an identity field that isn't
    /// `Str`/`Bytes`, an interval that isn't a finite positive `F64`, a `collectd.severity` that
    /// isn't `U64` or rides on a value list, or a `collectd.*` name this codec doesn't know.
    pub tags_dropped_unrepresentable: usize,
    /// An identity field that had a NUL or `/` replaced with `_`. Counted once per field.
    pub identity_sanitized_substituted: usize,
    /// An identity field truncated to [`MAX_IDENTITY_BYTES`]. Counted once per field.
    pub identity_sanitized_truncated: usize,
    /// A `log`-only event whose [`super::ATTR_SEVERITY`] is not `Value::U64` in `{1, 2, 4}`: an
    /// attempted notification. An event with **no** such attribute is not an attempt, and counts
    /// under [`Self::skipped_no_metrics`] instead.
    pub dropped_notification: usize,
    /// An attempted notification whose `LogRecord::message` is empty (after sanitizing) or not a
    /// `Str`/`Bytes`; collectd's receiver rejects an empty message.
    pub dropped_empty_message: usize,
    /// A single notification larger than `max_packet_bytes`, dropped whole: the notification
    /// counterpart of [`Self::dropped_oversize_list`].
    pub dropped_oversize_notification: usize,
    /// A notification message truncated to [`NOTIF_MAX_MSG_LEN`] `- 1` (255) bytes, once per
    /// message.
    pub notification_messages_truncated: usize,
}

/// The identity a value list is dispatched against, owned because the *previous* list's identity
/// outlives its event: elision compares across events within one datagram.
///
/// `Clone` is **hand-implemented, not derived**: the derive leaves the default `clone_from`
/// (`*self = source.clone()`), which allocates per non-empty field on every list in `pack_list`'s
/// `last.clone_from(cur)`. The override reuses the `Vec`s, so steady-state encoding allocates
/// nothing here.
#[derive(Debug, Default, PartialEq, Eq)]
struct Identity {
    host: Vec<u8>,
    plugin: Vec<u8>,
    plugin_instance: Vec<u8>,
    type_: Vec<u8>,
    type_instance: Vec<u8>,
}

impl Clone for Identity {
    fn clone(&self) -> Self {
        Self {
            host: self.host.clone(),
            plugin: self.plugin.clone(),
            plugin_instance: self.plugin_instance.clone(),
            type_: self.type_.clone(),
            type_instance: self.type_instance.clone(),
        }
    }

    /// Refills each field in place, so it doesn't allocate once `self`'s buffers have grown.
    fn clone_from(&mut self, source: &Self) {
        self.host.clear();
        self.host.extend_from_slice(&source.host);
        self.plugin.clear();
        self.plugin.extend_from_slice(&source.plugin);
        self.plugin_instance.clear();
        self.plugin_instance.extend_from_slice(&source.plugin_instance);
        self.type_.clear();
        self.type_.extend_from_slice(&source.type_);
        self.type_instance.clear();
        self.type_instance.extend_from_slice(&source.type_instance);
    }
}

impl Identity {
    /// The "nothing written yet" state, which is a receiver's sticky state at the start of a
    /// datagram, so a fresh packet's first list carries its full identity.
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

/// Encodes events as collectd value lists and notifications packed into datagrams. `collectd_out`
/// is a transport wrapper over it.
pub struct CollectdEncoder {
    telemetry: Telemetry,
    diag: Diagnostics,
    /// The operator-configured hostname, sanitized once at construction so the per-event path
    /// never counts a config value. `None` means not configured.
    hostname: Option<Bytes>,
    /// The longest single **datagram** this encoder packs; a list that alone exceeds it is
    /// dropped whole, never split. `usize::MAX` (the default) is uncapped. Encoder state, not an
    /// argument, so `encode_into` keeps [`FramedEncoder`]'s signature.
    max_packet_bytes: usize,
    /// The identity of the last list written into the packet currently being packed.
    last: Identity,
    /// The identity of the list currently being encoded.
    cur: Identity,
    /// The datagram being packed.
    packet: Vec<u8>,
    /// One encoded value list *or* notification, cleared before each; they never overlap in time,
    /// so one buffer serves both.
    list: Vec<u8>,
    /// The resolved wire values of the list currently being encoded.
    values: Vec<DsValue>,
    /// The sanitized message of the notification being encoded, reused across notifications.
    message: Vec<u8>,
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
            max_packet_bytes: usize::MAX,
            last: Identity::default(),
            cur: Identity::default(),
            packet: Vec::new(),
            list: Vec::new(),
            values: Vec::new(),
            message: Vec::new(),
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

    /// Caps the longest single datagram this encoder packs; `usize::MAX` means uncapped.
    /// [`super::DEFAULT_MAX_PACKET_BYTES`] is collectd's own default.
    pub fn with_max_packet_bytes(mut self, max_packet_bytes: usize) -> Self {
        self.max_packet_bytes = max_packet_bytes;
        self
    }

    /// The host written when neither `collectd.host` nor `host.name` is present (`collectd_out`'s
    /// `hostname:`).
    ///
    /// **Operator-supplied, with no default.** The encoder neither reads the OS hostname (deferred,
    /// `docs/known-gaps.md`) nor invents a placeholder: a receiver keys every series on the host,
    /// so one made-up name would merge every unlabelled sender into one host's metrics. With
    /// nothing configured and nothing on the event, the list is dropped and counted.
    ///
    /// Sanitized once, here; an empty value is the same as none.
    pub fn with_hostname(mut self, hostname: impl Into<Bytes>) -> Self {
        let raw: Bytes = hostname.into();
        let mut sanitized = Vec::new();
        // Byte truncation: a configured hostname is no more guaranteed UTF-8 than a wire one.
        sanitize_raw(&mut sanitized, &raw, false);
        self.hostname = (!sanitized.is_empty()).then(|| Bytes::from(sanitized));
        self
    }
}

impl FramedEncoder for CollectdEncoder {
    /// The number of messages (value lists or notifications) each datagram carries; `1` for a
    /// notification.
    type Meta = usize;
    type Stats = EncodeStats;

    /// Encodes every event in `batch` into `out` (cleared first), packing value lists into
    /// datagrams of at most [`CollectdEncoder::with_max_packet_bytes`]. Never fails: a per-list
    /// problem is a counted drop.
    fn encode_into(&mut self, batch: &EventBatch, out: &mut MessageBuf<usize>) -> EncodeStats {
        out.clear();
        let mut stats = EncodeStats::default();
        let max_packet_bytes = self.max_packet_bytes;
        // Destructured: the packing loop borrows these fields at once, which `&mut self` methods
        // can't express.
        let Self { telemetry, diag, hostname, last, cur, packet, list, values, message, .. } = self;
        let mut ctx = Ctx { telemetry, diag, stats: &mut stats };

        packet.clear();
        last.clear();
        let mut lists_in_packet = 0usize;

        for event in &batch.events {
            if event.metrics.is_empty() {
                // A `log`-only event is a notification *attempt* only if it carries
                // `collectd.severity`; otherwise it is the uncounted no-metrics skip. A plain scan,
                // not `collect_carriers`, so a non-attempt doesn't count every attribute as
                // `tags_dropped_no_wire_form`.
                let is_notification_attempt = event.log.is_some()
                    && logit_core::attrs::merged(&batch.resource, event)
                        .any(|(key, _)| logit_core::interner::resolve(key) == ATTR_SEVERITY);
                if is_notification_attempt {
                    let carriers = collect_carriers(&batch.resource, event, &mut ctx);
                    let log = event.log.as_ref().expect("checked by is_notification_attempt");
                    encode_notification(
                        event,
                        &carriers,
                        log,
                        message,
                        hostname.as_deref(),
                        packet,
                        list,
                        last,
                        cur,
                        max_packet_bytes,
                        &mut lists_in_packet,
                        out,
                        &mut ctx,
                    );
                } else {
                    ctx.stats.skipped_no_metrics += 1;
                }
                continue;
            }

            let carriers = collect_carriers(&batch.resource, event, &mut ctx);

            // Metrics win: `collectd.severity` has no wire form on a value list. `collect_carriers`
            // counted a wrong-typed one; it leaves the `U64` case for the caller, which knows the
            // path.
            if carriers.severity.is_some() {
                ctx.tag_dropped_unrepresentable();
            }

            // The value lists this event would produce, which the two whole-event drops below
            // count, so `logit.output.metrics.skipped` stays per-record as at every other sink.
            let lists = if carriers.type_.is_some() { 1 } else { event.metrics.len() };

            // 0 is collectd's "no time given", which its receiver rejects: drop, don't send it.
            let time_cdtime = nanos_to_cdtime(event.timestamp);
            if time_cdtime == 0 {
                ctx.drop_unencodable_timestamp(event.timestamp, lists);
                continue;
            }

            // Resolved once per event; without one, every list the event produces goes.
            cur.host.clear();
            if !resolve_host(&mut cur.host, &carriers, hostname.as_deref(), lists, &mut ctx) {
                continue;
            }

            if carriers.type_.is_some() {
                // Like-relay: the identity is the wire's own and the whole `MetricList` is one
                // value list, in order, so the decode side's `MAX_VALUES_PER_LIST` cap applies.
                // `aggregate`/`kv_metrics` can put far more records on one event, and a `set`
                // stamping `collectd.type` routes it here.
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
                            // The whole list goes, counted once for the first failing record:
                            // collectd's receiver would reject a partial list (its value count
                            // disagrees with the type's `ds_num`) without counting it.
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

            // Fallback: each record becomes its own single-data-source list, its dotted name split
            // into plugin and type_instance.
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
            out.push_with(packet, lists_in_packet);
        }
        stats
    }
}

/// The telemetry/diagnostics/stats triple every drop site needs, carried together so a drop is
/// never counted in one but not the others.
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

    /// A metric kind collectd has no data-source type for. `metric_kind` is the counter tag;
    /// `described` is the diagnostic's prose.
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

    /// `lists` is how many value lists the dropped event would have produced (see
    /// [`EncodeStats::dropped_unencodable_timestamp`]).
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

    /// A `log`-only event's [`super::ATTR_SEVERITY`] is the wrong `Value` type (`None`) or a
    /// `U64` outside `{1, 2, 4}`.
    fn drop_notification(&mut self, severity: Option<u64>) {
        self.stats.dropped_notification += 1;
        self.telemetry.count(
            "logit.output.metrics.skipped",
            1.0,
            &[("reason", "notification_dropped")],
        );
        self.diag.warn_throttled(
            "notification_dropped",
            format_args!(
                "collectd_out: a log event's collectd.severity ({severity:?}) is not one of 1 \
                 (FAILURE), 2 (WARNING), or 4 (OKAY); dropping the notification"
            ),
        );
    }

    /// See [`EncodeStats::dropped_empty_message`].
    fn drop_empty_message(&mut self) {
        self.stats.dropped_empty_message += 1;
        self.telemetry.count("logit.output.metrics.skipped", 1.0, &[("reason", "empty_message")]);
        self.diag.warn_throttled(
            "empty_message",
            "collectd_out: a notification's message is empty; dropping (collectd's own receiver \
             rejects the same notification)",
        );
    }

    /// A notification message truncated to [`NOTIF_MAX_MSG_LEN`] `- 1` bytes, counted as
    /// `logit.output.messages.truncated` (as `syslog_out` does), not `identity.sanitized`.
    fn notification_message_truncated(&mut self) {
        self.stats.notification_messages_truncated += 1;
        self.telemetry.count("logit.output.messages.truncated", 1.0, &[]);
        self.diag.warn_throttled(
            "message_truncated",
            format_args!(
                "collectd_out: a notification message exceeded {} bytes; truncated",
                NOTIF_MAX_MSG_LEN - 1
            ),
        );
    }

    /// [`Ctx::drop_oversize_list`] for a notification, under its own reason.
    fn drop_oversize_notification(&mut self, max_packet_bytes: usize) {
        self.stats.dropped_oversize_notification += 1;
        self.telemetry.count(
            "logit.output.metrics.skipped",
            1.0,
            &[("reason", "oversize_notification")],
        );
        self.diag.warn_throttled(
            "oversize_notification",
            format_args!(
                "collectd_out: a single notification exceeds max_packet_bytes \
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
    /// In ticks; `0` means absent, collectd's "unspecified".
    interval_cdtime: u64,
    /// The normalized host attribute, used only when `collectd.host` is absent.
    host_name: Option<&'a Value>,
    /// Whether a [`super::ATTR_SEVERITY`] attribute was present in **any** `Value` type, which
    /// makes a `log`-only event an *attempted* notification.
    severity_present: bool,
    /// The raw wire severity when the attribute was `Value::U64`; `None` for absent or wrong-typed,
    /// which fail identically once a notification is attempted.
    severity: Option<u64>,
}

/// Walks one event's attributes merged over its resource ([`logit_core::attrs::merged`]; the
/// event wins), capturing the `collectd.*` carriers and counting everything with no wire form.
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
                    // Exact for every interval a decoded list carries: `10.0` is `10 << 30` ticks.
                    carriers.interval_cdtime = (seconds * CDTIME_ONE_SECOND as f64).round() as u64;
                }
                // Uncounted here: only the caller knows whether it's on the notification path.
                ("severity", Value::U64(v)) => {
                    carriers.severity_present = true;
                    carriers.severity = Some(*v);
                }
                // Wrong-typed: still an attempt, so it fails as `dropped_notification` rather than
                // skipping as a plain log event.
                ("severity", _) => {
                    carriers.severity_present = true;
                    ctx.tag_dropped_unrepresentable();
                }
                // A wrong-typed carrier, a non-positive interval, or an unknown `collectd.*` name.
                _ => ctx.tag_dropped_unrepresentable(),
            }
            continue;
        }
        if key == "host.name" && matches!(value, Value::Str(_) | Value::Bytes(_)) {
            carriers.host_name = Some(value);
        }
        // Counted even for `host.name`: host resolution reads it, but it has no wire form itself.
        ctx.tag_dropped_no_wire_form();
    }
    carriers
}

/// This event's host: `collectd.host`, else `host.name`, else
/// [`CollectdEncoder::with_hostname`], the first that sanitizes non-empty. `false` means none, and
/// the event is dropped (counted `no_host`).
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
    // Sanitized and non-empty since `with_hostname`.
    if let Some(hostname) = hostname {
        out.clear();
        out.extend_from_slice(hostname);
        return true;
    }
    ctx.drop_no_host(lists);
    false
}

/// [`sanitize_into`] for an optional carrier: an absent one leaves `out` empty, which is what an
/// absent instance means on the wire.
fn sanitize_carrier(out: &mut Vec<u8>, carrier: Option<&Value>, ctx: &mut Ctx) {
    if let Some((raw, is_utf8)) = carrier.and_then(text_of) {
        sanitize_into(out, raw, is_utf8, ctx);
    }
}

/// A `Value`'s bytes and whether they are known-valid UTF-8 (character- or byte-boundary
/// truncation). `None` for any other kind; [`collect_carriers`] filters those out first.
fn text_of(value: &Value) -> Option<(&[u8], bool)> {
    match value {
        Value::Str(bytes) => Some((bytes, true)),
        Value::Bytes(bytes) => Some((bytes, false)),
        _ => None,
    }
}

/// [`sanitize_raw`], counting what it did. [`super`]'s "Sanitization" section has the rules.
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
/// Uncounted, so [`CollectdEncoder::with_hostname`] can use it. Substitution rather than deletion
/// keeps distinct inputs distinct. `is_utf8` truncates on a character boundary; a substitution
/// never changes length, so the input's boundaries still hold in `out`.
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
            // Walk back off UTF-8 continuation bytes (`0b10xxxxxx`): a half-written character
            // would round-trip as a `Value::Bytes` instead of a `Value::Str`.
            while end > 0 && out[end] & 0xC0 == 0x80 {
                end -= 1;
            }
        }
        out.truncate(end);
        truncated = true;
    }
    (substituted, truncated)
}

/// The stock `types.db` type name for a one-data-source list of this value's kind. All four are
/// single-data-source types in collectd's shipped `types.db`, so any receiver resolves them.
fn fallback_type(value: DsValue) -> &'static str {
    match value {
        DsValue::Counter(_) => "counter",
        DsValue::Gauge(_) => "gauge",
        DsValue::Derive(_) => "derive",
        DsValue::Absolute(_) => "absolute",
    }
}

/// One record's wire value, or `None` (counted, with a throttled diagnostic) when collectd can't
/// carry it. The `match` has **no wildcard**, so a new [`MetricKind`] variant is a compile error
/// here, not an uncounted drop.
fn resolve_value(record: &MetricRecord, ctx: &mut Ctx) -> Option<DsValue> {
    let name = logit_core::interner::resolve(record.name);

    // Only `Gauge` has a wire form for "no reading" (NaN); any other flagged kind is a drop,
    // whatever its default payload (`MetricRecord::flags`).
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
        // A flagged gauge leaves as NaN, the inverse of the decode side's flagged zero.
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
/// range. Never rounds: statsd's `page.views:2|c|@0.3` reaches a sink as `6.666…`, and neither `6`
/// nor `7` was sent.
///
/// The bounds are inclusive and the `as` cast saturates: a wire COUNTER of `u64::MAX` decodes to
/// the `f64` `2^64`, so an exclusive bound would drop the value it round-tripped from. Above `2^53`
/// the value is already imprecise (`docs/known-gaps.md`), and `decode(encode(b)) == b` holds across
/// the whole range.
fn as_u64(value: f64, name: &str, ctx: &mut Ctx) -> Option<u64> {
    if value.is_finite() && value.fract() == 0.0 && value >= 0.0 && value <= u64::MAX as f64 {
        return Some(value as u64);
    }
    ctx.drop_unencodable_value(name, value);
    None
}

/// [`as_u64`]'s signed sibling, for DERIVE, with the same inclusive bounds (`i64::MAX as f64` is
/// `2^63`; `i64::MIN as f64` is exactly `-2^63`).
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
/// TimeHR and IntervalHR are written for **every** list, never elided, though collectd's own
/// sender elides unchanged ones: normalization 10 in [`super`]'s list.
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

/// Encodes one list and appends it to the packet being packed, flushing the packet first if the
/// list won't fit. Buffers are passed in because they are all live at once.
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
    out: &mut MessageBuf<usize>,
    ctx: &mut Ctx,
) {
    write_list(list, last, cur, time_cdtime, interval_cdtime, values);

    if !packet.is_empty() && packet.len() + list.len() > max_packet_bytes {
        // Flush, then **encode the same list again**: the version just built elided parts that
        // matched the packet being flushed, and the receiver resets sticky state at each datagram
        // boundary, so leading the next datagram it would have no plugin/type. The cost is one
        // re-encode per boundary.
        out.push_with(packet, *lists_in_packet);
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

/// Sanitizes a notification message into `out` (cleared first) per [`super`]'s "Sanitization"
/// section: NUL becomes `_`, `/` rides through, and it is truncated to [`NOTIF_MAX_MSG_LEN`] `- 1`
/// bytes on a character (`is_utf8`) or byte boundary. Returns whether it truncated.
fn sanitize_message(out: &mut Vec<u8>, raw: &[u8], is_utf8: bool) -> bool {
    out.clear();
    for &byte in raw {
        out.push(if byte == 0 { b'_' } else { byte });
    }
    let max = NOTIF_MAX_MSG_LEN - 1;
    if out.len() <= max {
        return false;
    }
    let mut end = max;
    if is_utf8 {
        // Same character-boundary walk-back as `sanitize_raw`.
        while end > 0 && out[end] & 0xC0 == 0x80 {
            end -= 1;
        }
    }
    out.truncate(end);
    true
}

/// Encodes one notification into `notif` (cleared first), in collectd's sender order: TimeHR,
/// Severity, Host, Plugin, PluginInstance, Type, TypeInstance, Message. The four optional identity
/// parts are written only when non-empty but, unlike [`write_list`], **never elided against
/// `last`** (see [`super`]'s module doc).
fn write_notification(
    notif: &mut Vec<u8>,
    cur: &Identity,
    time_cdtime: u64,
    severity: u64,
    message: &[u8],
) {
    notif.clear();
    part::write_number_part(notif, part::TYPE_TIME_HR, time_cdtime);
    part::write_number_part(notif, part::TYPE_SEVERITY, severity);
    part::write_string_part(notif, part::TYPE_HOST, &cur.host);
    if !cur.plugin.is_empty() {
        part::write_string_part(notif, part::TYPE_PLUGIN, &cur.plugin);
    }
    if !cur.plugin_instance.is_empty() {
        part::write_string_part(notif, part::TYPE_PLUGIN_INSTANCE, &cur.plugin_instance);
    }
    if !cur.type_.is_empty() {
        part::write_string_part(notif, part::TYPE_TYPE, &cur.type_);
    }
    if !cur.type_instance.is_empty() {
        part::write_string_part(notif, part::TYPE_TYPE_INSTANCE, &cur.type_instance);
    }
    part::write_string_part(notif, part::TYPE_MESSAGE, message);
}

/// Encodes one notification and pushes it as its **own** datagram: [`pack_list`]'s counterpart,
/// with no re-encode (nothing is elided) and no packing decision.
#[allow(clippy::too_many_arguments)]
fn pack_notification(
    packet: &mut Vec<u8>,
    notif: &mut Vec<u8>,
    last: &mut Identity,
    cur: &Identity,
    time_cdtime: u64,
    severity: u64,
    message: &[u8],
    max_packet_bytes: usize,
    lists_in_packet: &mut usize,
    out: &mut MessageBuf<usize>,
    ctx: &mut Ctx,
) {
    write_notification(notif, cur, time_cdtime, severity, message);

    if notif.len() > max_packet_bytes {
        ctx.drop_oversize_notification(max_packet_bytes);
        return;
    }

    // Flush any value-list packet in progress, then push the notification alone. Without the
    // trailing `clear`, the next list would elide against this notification's `cur`, including
    // fields `write_notification` omitted as empty, which the receiver's sticky state never held.
    if !packet.is_empty() {
        out.push_with(packet, *lists_in_packet);
        packet.clear();
        *lists_in_packet = 0;
        last.clear();
    }
    out.push_with(notif, 1);
    last.clear();
}

/// Encodes one `log`-only event already established as an *attempted* notification
/// (`carriers.severity_present`). Every early return is a drop, counted where it's detected.
#[allow(clippy::too_many_arguments)]
fn encode_notification(
    event: &Event,
    carriers: &Carriers,
    log: &LogRecord,
    message: &mut Vec<u8>,
    hostname: Option<&[u8]>,
    packet: &mut Vec<u8>,
    notif: &mut Vec<u8>,
    last: &mut Identity,
    cur: &mut Identity,
    max_packet_bytes: usize,
    lists_in_packet: &mut usize,
    out: &mut MessageBuf<usize>,
    ctx: &mut Ctx,
) {
    let Some(severity) = carriers.severity.filter(|v| matches!(v, 1 | 2 | 4)) else {
        ctx.drop_notification(carriers.severity);
        return;
    };

    let time_cdtime = nanos_to_cdtime(event.timestamp);
    if time_cdtime == 0 {
        ctx.drop_unencodable_timestamp(event.timestamp, 1);
        return;
    }

    cur.host.clear();
    if !resolve_host(&mut cur.host, carriers, hostname, 1, ctx) {
        return;
    }

    let Some((raw, is_utf8)) = text_of(&log.message) else {
        ctx.drop_empty_message();
        return;
    };
    if sanitize_message(message, raw, is_utf8) {
        ctx.notification_message_truncated();
    }
    if message.is_empty() {
        ctx.drop_empty_message();
        return;
    }

    cur.clear_below_host();
    sanitize_carrier(&mut cur.plugin, carriers.plugin, ctx);
    sanitize_carrier(&mut cur.plugin_instance, carriers.plugin_instance, ctx);
    sanitize_carrier(&mut cur.type_, carriers.type_, ctx);
    sanitize_carrier(&mut cur.type_instance, carriers.type_instance, ctx);

    pack_notification(
        packet,
        notif,
        last,
        cur,
        time_cdtime,
        severity,
        message,
        max_packet_bytes,
        lists_in_packet,
        out,
        ctx,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collectd::decode::tests::{single_gauge_packet, PacketBuilder, GAUGE_1_5};
    use crate::collectd::{
        CollectdDecoder, ATTR_HOST, ATTR_INTERVAL, ATTR_PLUGIN, ATTR_PLUGIN_INSTANCE,
        ATTR_SEVERITY, ATTR_TYPE, ATTR_TYPE_INSTANCE, DEFAULT_MAX_PACKET_BYTES,
    };
    use crate::Decoder;
    use logit_core::interner::intern;
    use logit_core::telemetry::Registry;
    use logit_core::{AttrMap, BodyFormat, Severity, Sum};
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

    /// An encoder with a configured hostname, so events without a host aren't dropped; the no-host
    /// drop has its own test.
    fn encode(batch: &EventBatch, max_packet_bytes: usize) -> (MessageBuf<usize>, EncodeStats) {
        let mut encoder = CollectdEncoder::new()
            .with_hostname("fixture-host")
            .with_max_packet_bytes(max_packet_bytes);
        let mut out = MessageBuf::default();
        let stats = encoder.encode_into(batch, &mut out);
        (out, stats)
    }

    /// Encodes with live telemetry and diagnostics, to assert on both [`EncodeStats`] and counters.
    fn encode_counted(
        batch: &EventBatch,
        max_packet_bytes: usize,
    ) -> (MessageBuf<usize>, EncodeStats, Arc<Registry>, Arc<Registry>) {
        let registry = Registry::new();
        let diag_registry = Registry::new();
        let mut encoder = CollectdEncoder::new()
            .with_hostname("fixture-host")
            .with_max_packet_bytes(max_packet_bytes)
            .with_telemetry(registry.telemetry_for("collectd_out", "collectd_out", "sink"))
            .with_diagnostics(Diagnostics::new("collectd_out").with_telemetry(
                diag_registry.telemetry_for("collectd_out/diag", "collectd_out", "sink"),
            ));
        let mut out = MessageBuf::default();
        let stats = encoder.encode_into(batch, &mut out);
        (out, stats, registry, diag_registry)
    }

    /// Whether `registry` recorded a point named `metric` carrying `tag`. Drains, so call once.
    fn counted(registry: &Registry, metric: &str, tag: (&str, &str)) -> bool {
        registry.drain(0).iter().any(|event| {
            event.metrics.iter().any(|m| logit_core::interner::resolve(m.name) == metric)
                && event.attributes.get(tag.0).and_then(|v| v.as_str()) == Some(tag.1)
        })
    }

    /// Whether `registry` recorded a point named `metric`, regardless of tags (for an untagged
    /// counter). Drains, so call once.
    fn metric_recorded(registry: &Registry, metric: &str) -> bool {
        registry.drain(0).iter().any(|event| {
            event.metrics.iter().any(|m| logit_core::interner::resolve(m.name) == metric)
        })
    }

    /// The **total** recorded on `logit.output.metrics.skipped{reason}`. Drains, so call once.
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

    /// Decodes every datagram in `packets` back into events.
    fn decode_all(packets: &MessageBuf<usize>) -> Vec<Event> {
        let mut decoder = CollectdDecoder::new(Arc::new(Resource::default()));
        let mut events = Vec::new();
        for bytes in packets.iter() {
            decoder
                .decode_into(Bytes::copy_from_slice(bytes), TS, &mut events)
                .expect("every packet this encoder writes must decode");
        }
        events
    }

    fn attr(event: &Event, key: &str) -> Option<Value> {
        event.attributes.get(key).cloned()
    }

    /// How many parts of `part_type` a datagram carries: a part walk, not a byte scan, because
    /// `TYPE_HOST` is `0x0000`, which turns up inside many payloads.
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

    /// A decoded packet, re-encoded, decodes to the same events (`tests/collectd_fixed_point.rs` is
    /// the exhaustive version).
    #[test]
    fn a_decoded_packet_survives_a_re_encode() {
        let mut decoder = CollectdDecoder::new(Arc::new(Resource::default()));
        let mut events = Vec::new();
        decoder.decode_into(single_gauge_packet(), TS, &mut events).unwrap();
        let first = batch(events);

        let (packets, _) = encode(&first, DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(decode_all(&packets), first.events);
    }

    /// A gauge is written little-endian.
    #[test]
    fn a_gauge_is_written_little_endian() {
        let event = relay_event(vec![record("load.load", MetricKind::Gauge(1.5))]);
        let (packets, _) = encode(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        let bytes = packets.iter().next().unwrap();
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
        let bytes = packets.iter().next().unwrap();
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
        let (bytes, lists) = packets.iter_with().next().unwrap();
        assert_eq!(*lists, 2);
        // One Host/Plugin/Type part for two lists; only the differing TypeInstance is rewritten.
        assert_eq!(count_parts(bytes, part::TYPE_HOST), 1, "the second list must elide Host");
        assert_eq!(count_parts(bytes, part::TYPE_PLUGIN), 1);
        assert_eq!(count_parts(bytes, part::TYPE_TYPE), 1);
        assert_eq!(count_parts(bytes, part::TYPE_TYPE_INSTANCE), 1, "only the second list has one");
        assert_eq!(count_parts(bytes, part::TYPE_VALUES), 2);
        assert_eq!(decode_all(&packets).len(), 2);
    }

    /// The first list of a *new* datagram carries its full identity, because the receiver's
    /// sticky state resets at the boundary.
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
        for (bytes, lists) in packets.iter_with() {
            assert_eq!(*lists, 1);
            assert_eq!(
                count_parts(bytes, part::TYPE_HOST),
                1,
                "a datagram's first list must not elide its identity"
            );
            assert_eq!(count_parts(bytes, part::TYPE_PLUGIN), 1);
            assert_eq!(count_parts(bytes, part::TYPE_TYPE), 1);
        }
        // Without the re-encode, the second datagram would have no plugin.
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
        for bytes in packets.iter() {
            assert!(bytes.len() <= 256, "a datagram exceeded the cap");
        }
        let decoded = decode_all(&packets);
        assert_eq!(decoded.len(), 40);
        let total: usize = packets.iter_with().map(|(_, lists)| *lists).sum();
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
        let mut encoder = CollectdEncoder::new().with_max_packet_bytes(DEFAULT_MAX_PACKET_BYTES);
        let mut packets = MessageBuf::default();
        encoder.encode_into(&batch, &mut packets);
        let first = packets.total_bytes();
        encoder.encode_into(&batch, &mut packets);
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
        // With no `.`, the whole name is the plugin, with no type_instance.
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

    /// A negative DERIVE is legal (a signed type); an out-of-range counter is not.
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

    /// A wire `u64::MAX` COUNTER decodes to the `f64` 2^64, which the *inclusive* bound re-encodes.
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

    /// A flagged gauge encodes as NaN, which decodes back to a flagged zero gauge.
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

    /// A whole-event drop is counted per value **list** the event would have produced.
    #[test]
    fn a_whole_event_drop_is_counted_once_per_list_the_event_would_have_produced() {
        let mut fallback = Event::empty(TS, AttrMap::new());
        for name in ["a.one", "a.two", "a.three"] {
            fallback.metrics.push(counter_record(name, 1.0));
        }

        for (label, mut event, expected) in [
            ("fallback, 3 records", fallback.clone(), 3),
            // A like-relay event is one list however many records it carries.
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
            let mut encoder = CollectdEncoder::new()
                .with_max_packet_bytes(DEFAULT_MAX_PACKET_BYTES)
                .with_telemetry(registry.telemetry_for("collectd_out", "collectd_out", "sink"));
            let mut packets = MessageBuf::default();
            let stats = encoder.encode_into(&batch(vec![event.clone()]), &mut packets);
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

    /// A like-relay event over `MAX_VALUES_PER_LIST` records is dropped whole and counted.
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

    /// A list at exactly the cap still round-trips.
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

    /// With no host anywhere, the event is dropped, counted, and diagnosed; no host is invented.
    #[test]
    fn an_event_with_no_host_and_no_configured_hostname_is_dropped_and_counted() {
        let mut event = relay_event(vec![record("load.load", MetricKind::Gauge(0.5))]);
        event.attributes.remove(ATTR_HOST);

        let registry = Registry::new();
        let diag_registry = Registry::new();
        let mut encoder = CollectdEncoder::new()
            .with_max_packet_bytes(DEFAULT_MAX_PACKET_BYTES)
            .with_telemetry(registry.telemetry_for("collectd_out", "collectd_out", "sink"))
            .with_diagnostics(Diagnostics::new("collectd_out").with_telemetry(
                diag_registry.telemetry_for("collectd_out/diag", "collectd_out", "sink"),
            ));
        let mut packets = MessageBuf::default();
        let stats = encoder.encode_into(&batch(vec![event]), &mut packets);

        assert!(packets.is_empty());
        assert_eq!(stats.dropped_no_host, 1);
        assert!(counted(&registry, "logit.output.metrics.skipped", ("reason", "no_host")));
        assert!(counted(&diag_registry, "logit.component.diagnostics", ("key", "no_host")));
    }

    /// An empty configured hostname is the same as none; a `\0` one becomes `_`.
    #[test]
    fn an_empty_or_substituted_away_hostname_counts_as_unconfigured() {
        for hostname in ["", "\0"] {
            let mut event = relay_event(vec![record("load.load", MetricKind::Gauge(0.5))]);
            event.attributes.insert(ATTR_HOST, Value::from(""));
            let mut encoder = CollectdEncoder::new()
                .with_hostname(hostname)
                .with_max_packet_bytes(DEFAULT_MAX_PACKET_BYTES);
            let mut packets = MessageBuf::default();
            let stats = encoder.encode_into(&batch(vec![event]), &mut packets);
            if hostname.is_empty() {
                assert!(packets.is_empty(), "an empty hostname is not a host");
                assert_eq!(stats.dropped_no_host, 1);
            } else {
                // Substitution never deletes, so `\0` stays configured, as `_`.
                assert_eq!(attr(&decode_all(&packets)[0], ATTR_HOST), Some(Value::from("_")));
            }
        }
    }

    /// Normalization (8): `/` and NUL become `_`, counted. Not a fixed point, so
    /// `tests/collectd_fixed_point.rs`'s grammar never generates one and this test covers it.
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
        // 63 characters (126 bytes), still a `Value::Str`: the cut landed on a character boundary.
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
            // Empty, not `/`: substitution never deletes, so `/` would sanitize to a usable `_`.
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

    /// A resource-level `collectd.*` carrier works, and an event-level one wins over it.
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

    /// Metrics win: `collectd.severity` on an event with metrics is counted
    /// `tags_dropped_unrepresentable`.
    #[test]
    fn a_severity_attribute_on_a_metrics_bearing_event_is_dropped_and_counted() {
        let mut event = relay_event(vec![record("load.load", MetricKind::Gauge(0.5))]);
        event.attributes.insert(ATTR_SEVERITY, Value::U64(2));
        let (packets, stats, registry, _) =
            encode_counted(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(stats.tags_dropped_unrepresentable, 1);
        assert!(counted(&registry, "logit.output.tags.dropped", ("reason", "unrepresentable")));

        let decoded = decode_all(&packets);
        assert_eq!(decoded.len(), 1, "the event still encodes as an ordinary value list");
        assert_eq!(decoded[0].metrics[0].kind, MetricKind::Gauge(0.5));
        assert_eq!(
            decoded[0].attributes.get(ATTR_SEVERITY),
            None,
            "collectd.severity never reaches the wire on a value list"
        );
    }

    /// Every packet this encoder writes decodes cleanly, over a hand-built mixed input.
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

    // --- notifications --------------------------------------------------------------------------

    fn log_record(message: Value, severity: Severity) -> LogRecord {
        LogRecord {
            message,
            severity: Some(severity),
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        }
    }

    /// A notification-shaped event: `log` set, no metrics, `collectd.severity` as the raw wire
    /// value. `log_severity` is independent, so a test can build mismatches the encoder ignores.
    fn notification_event(wire_severity: Value, log_severity: Severity, message: &str) -> Event {
        let mut attrs = attrs(&[
            (ATTR_HOST, Value::from("web-1")),
            (ATTR_PLUGIN, Value::from("load")),
            (ATTR_TYPE, Value::from("load")),
        ]);
        attrs.insert(ATTR_SEVERITY, wire_severity);
        Event::log(TS, attrs, log_record(Value::from(message), log_severity))
    }

    #[test]
    fn every_severity_encodes_as_a_notification_that_decodes_back_the_same() {
        for (wire, severity) in
            [(1u64, Severity::Error), (2u64, Severity::Warn), (4u64, Severity::Info)]
        {
            let event = notification_event(Value::U64(wire), severity, "threshold exceeded");
            let (packets, stats) = encode(&batch(vec![event.clone()]), DEFAULT_MAX_PACKET_BYTES);
            assert_eq!(stats, EncodeStats::default(), "severity {wire}");
            let decoded = decode_all(&packets);
            assert_eq!(decoded.len(), 1, "severity {wire}");
            assert!(decoded[0].metrics.is_empty(), "a notification carries no metrics");
            let log = decoded[0].log.as_ref().expect("must decode back to a log record");
            assert_eq!(log.severity, Some(severity), "severity {wire}");
            assert_eq!(log.message, Value::from("threshold exceeded"));
            assert_eq!(attr(&decoded[0], ATTR_HOST), Some(Value::from("web-1")));
            assert_eq!(attr(&decoded[0], ATTR_PLUGIN), Some(Value::from("load")));
            assert_eq!(attr(&decoded[0], ATTR_TYPE), Some(Value::from("load")));
            assert_eq!(decoded[0].attributes.get(ATTR_SEVERITY), Some(&Value::U64(wire)));
        }
    }

    /// A non-UTF-8 message rides byte-verbatim, like a non-UTF-8 identity field.
    #[test]
    fn a_non_utf8_message_encodes_and_decodes_byte_verbatim() {
        let raw = Bytes::from(vec![0xFFu8, 0xFE, b'm']);
        let mut event = notification_event(Value::U64(2), Severity::Warn, "placeholder");
        event.log.as_mut().unwrap().message = Value::Bytes(raw.clone());
        let (packets, stats) = encode(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(stats, EncodeStats::default());
        let decoded = decode_all(&packets);
        assert_eq!(decoded[0].log.as_ref().unwrap().message, Value::Bytes(raw));
    }

    #[test]
    fn an_empty_message_is_dropped_and_counted() {
        let event = notification_event(Value::U64(2), Severity::Warn, "");
        let (packets, stats, registry, diag_registry) =
            encode_counted(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        assert!(packets.is_empty());
        assert_eq!(stats.dropped_empty_message, 1);
        assert!(counted(&registry, "logit.output.metrics.skipped", ("reason", "empty_message")));
        assert!(counted(&diag_registry, "logit.component.diagnostics", ("key", "empty_message")));
    }

    #[test]
    fn a_message_value_that_is_not_str_or_bytes_is_treated_as_empty_and_dropped() {
        let mut event = notification_event(Value::U64(1), Severity::Error, "placeholder");
        event.log.as_mut().unwrap().message = Value::I64(42);
        let (packets, stats) = encode(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        assert!(packets.is_empty());
        assert_eq!(stats.dropped_empty_message, 1);
    }

    #[test]
    fn an_out_of_set_severity_is_dropped_and_counted() {
        for wire in [Value::U64(0), Value::U64(3), Value::U64(5), Value::U64(u64::MAX)] {
            let event = notification_event(wire.clone(), Severity::Warn, "x");
            let (packets, stats, registry, diag_registry) =
                encode_counted(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
            assert!(packets.is_empty(), "{wire:?}");
            assert_eq!(stats.dropped_notification, 1, "{wire:?}");
            assert!(counted(
                &registry,
                "logit.output.metrics.skipped",
                ("reason", "notification_dropped")
            ));
            assert!(counted(
                &diag_registry,
                "logit.component.diagnostics",
                ("key", "notification_dropped")
            ));
        }
    }

    /// A wrong-typed `collectd.severity` is still an *attempt*: counted as an unrepresentable tag
    /// and a dropped notification, not `skipped_no_metrics`.
    #[test]
    fn a_severity_attribute_of_the_wrong_type_is_an_attempt_that_fails() {
        let event = notification_event(Value::from("2"), Severity::Warn, "x");
        let (packets, stats) = encode(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        assert!(packets.is_empty());
        assert_eq!(stats.dropped_notification, 1);
        assert_eq!(stats.tags_dropped_unrepresentable, 1);
        assert_eq!(stats.skipped_no_metrics, 0, "a present collectd.severity is an attempt");
    }

    /// A `log`-only event with **no** `collectd.severity` is not an attempt: `skipped_no_metrics`.
    #[test]
    fn a_log_event_without_collectd_severity_is_not_a_notification() {
        let event = Event::log(
            TS,
            attrs(&[(ATTR_HOST, Value::from("web-1"))]),
            log_record(Value::from("just a log line"), Severity::Info),
        );
        let (packets, stats) = encode(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        assert!(packets.is_empty());
        assert_eq!(stats, EncodeStats { skipped_no_metrics: 1, ..EncodeStats::default() });
    }

    #[test]
    fn a_non_positive_timestamp_drops_the_notification() {
        for timestamp in [0i64, -1] {
            let mut event = notification_event(Value::U64(4), Severity::Info, "x");
            event.timestamp = timestamp;
            let (packets, stats) = encode(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
            assert!(packets.is_empty(), "timestamp {timestamp}");
            assert_eq!(stats.dropped_unencodable_timestamp, 1);
        }
    }

    #[test]
    fn a_notification_with_no_host_and_no_configured_hostname_is_dropped() {
        let mut event = notification_event(Value::U64(1), Severity::Error, "x");
        event.attributes.remove(ATTR_HOST);

        let mut encoder = CollectdEncoder::new().with_max_packet_bytes(DEFAULT_MAX_PACKET_BYTES);
        let mut packets = MessageBuf::default();
        let stats = encoder.encode_into(&batch(vec![event]), &mut packets);
        assert!(packets.is_empty());
        assert_eq!(stats.dropped_no_host, 1);
    }

    /// A notification's host falls back as a value list's does.
    #[test]
    fn a_notification_falls_back_from_host_name_to_the_configured_hostname() {
        let mut event = notification_event(Value::U64(1), Severity::Error, "x");
        event.attributes.remove(ATTR_HOST);
        event.attributes.insert("host.name", Value::from("from-host-name"));
        let (packets, _) = encode(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(attr(&decode_all(&packets)[0], ATTR_HOST), Some(Value::from("from-host-name")));
    }

    /// A message longer than 255 bytes truncates on a character boundary and is counted; a
    /// message at exactly the limit is untouched.
    #[test]
    fn a_notification_message_over_255_bytes_is_truncated_and_counted() {
        let long: String = "é".repeat(200); // 400 bytes, well past the limit
        let event = notification_event(Value::U64(1), Severity::Error, &long);
        let (packets, stats, registry, diag_registry) =
            encode_counted(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(stats.notification_messages_truncated, 1);
        assert!(metric_recorded(&registry, "logit.output.messages.truncated"));
        assert!(counted(
            &diag_registry,
            "logit.component.diagnostics",
            ("key", "message_truncated")
        ));
        let decoded = decode_all(&packets);
        let Value::Str(message) = &decoded[0].log.as_ref().unwrap().message else {
            panic!("a truncated UTF-8 message must still be a Value::Str")
        };
        assert!(message.len() <= 255);
        assert!(std::str::from_utf8(message).is_ok());
    }

    /// A message at exactly the 255-byte limit round-trips untouched.
    #[test]
    fn a_notification_message_at_exactly_255_bytes_is_not_truncated() {
        let message: String = "a".repeat(255);
        let event = notification_event(Value::U64(2), Severity::Warn, &message);
        let (packets, stats) = encode(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(stats.notification_messages_truncated, 0);
        let decoded = decode_all(&packets);
        assert_eq!(decoded[0].log.as_ref().unwrap().message, Value::str(message));
    }

    /// NUL inside a message becomes `_`, uncounted, unlike an identity field's substitution.
    #[test]
    fn a_nul_in_a_notification_message_becomes_an_underscore_uncounted() {
        let event = notification_event(Value::U64(1), Severity::Error, "disk\0full");
        let (packets, stats) = encode(&batch(vec![event]), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(
            stats.identity_sanitized_substituted, 0,
            "not counted the way identity fields are"
        );
        let decoded = decode_all(&packets);
        assert_eq!(decoded[0].log.as_ref().unwrap().message, Value::from("disk_full"));
    }

    /// A notification is always its own datagram, as in
    /// `testdata/interop/collectd/collectd-notification-000.raw`, even when a preceding list shares
    /// its host/plugin/type.
    #[test]
    fn a_notification_is_always_its_own_datagram_never_packed_with_a_preceding_list() {
        let list_event = relay_event(vec![record("load.load", MetricKind::Gauge(0.5))]);
        let notif_event = notification_event(Value::U64(2), Severity::Warn, "load high");
        let (packets, stats) =
            encode(&batch(vec![list_event, notif_event]), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(stats, EncodeStats::default());
        assert_eq!(
            packets.len(),
            2,
            "the notification flushes the list's packet and starts its own"
        );
        let (list_bytes, notif_bytes) = {
            let mut iter = packets.iter();
            (iter.next().unwrap().to_vec(), iter.next().unwrap().to_vec())
        };
        assert_eq!(count_parts(&list_bytes, part::TYPE_HOST), 1);
        assert_eq!(count_parts(&list_bytes, part::TYPE_MESSAGE), 0);
        assert_eq!(count_parts(&notif_bytes, part::TYPE_HOST), 1);
        assert_eq!(count_parts(&notif_bytes, part::TYPE_PLUGIN), 1);
        assert_eq!(count_parts(&notif_bytes, part::TYPE_TYPE), 1);
        assert_eq!(count_parts(&notif_bytes, part::TYPE_MESSAGE), 1);
        assert_eq!(count_parts(&notif_bytes, part::TYPE_SEVERITY), 1);

        let decoded = decode_all(&packets);
        assert_eq!(decoded.len(), 2);
        assert!(decoded[0].log.is_none() && !decoded[0].metrics.is_empty());
        assert!(decoded[1].log.is_some() && decoded[1].metrics.is_empty());
    }

    /// A value list right after a notification restates its identity in full, even when shared.
    #[test]
    fn a_value_list_after_a_notification_does_not_elide_against_it() {
        let notif_event = notification_event(Value::U64(2), Severity::Warn, "load high");
        let list_event = relay_event(vec![record("load.load", MetricKind::Gauge(0.5))]);
        let (packets, stats) =
            encode(&batch(vec![notif_event, list_event]), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(stats, EncodeStats::default());
        assert_eq!(
            packets.len(),
            2,
            "the list starts its own fresh datagram after the notification"
        );
        let list_bytes = packets.iter().nth(1).unwrap();
        assert_eq!(
            count_parts(list_bytes, part::TYPE_HOST),
            1,
            "the value list must restate its identity, not elide against the notification's"
        );
        assert_eq!(count_parts(list_bytes, part::TYPE_PLUGIN), 1);
        assert_eq!(count_parts(list_bytes, part::TYPE_TYPE), 1);
    }

    #[test]
    fn a_batch_mixing_a_value_list_and_a_notification_round_trips() {
        let list_event = relay_event(vec![
            record("load.load.0", MetricKind::Gauge(0.1)),
            record("load.load.1", MetricKind::Gauge(0.2)),
        ]);
        let notif_event = notification_event(Value::U64(1), Severity::Error, "load spiked");
        let (packets, stats) =
            encode(&batch(vec![list_event.clone(), notif_event.clone()]), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(stats, EncodeStats::default());
        assert_eq!(packets.len(), 2, "a notification is always its own datagram");
        let decoded = decode_all(&packets);
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].metrics.len(), 2);
        assert_eq!(decoded[1].log.as_ref().unwrap().message, Value::from("load spiked"));
    }

    /// A notification between two value lists that differ only in `type_instance` (the second
    /// empty) must not let the second inherit the first's: elision against the notification's
    /// empty `type_instance` would leave a receiver reading `"free"`, relabeling the series.
    #[test]
    fn a_notification_between_two_value_lists_does_not_poison_elision() {
        let mut first = relay_event(vec![record("load.load", MetricKind::Gauge(512.0))]);
        first.attributes.insert(ATTR_TYPE_INSTANCE, Value::from("free"));
        let notif = notification_event(Value::U64(2), Severity::Warn, "low memory");
        let mut third = relay_event(vec![record("load.load", MetricKind::Gauge(256.0))]);
        third.attributes.remove(ATTR_TYPE_INSTANCE);

        let (packets, stats) = encode(&batch(vec![first, notif, third]), DEFAULT_MAX_PACKET_BYTES);
        assert_eq!(stats, EncodeStats::default());
        assert_eq!(packets.len(), 3, "list, notification, list -- three separate datagrams");

        let decoded = decode_all(&packets);
        assert_eq!(decoded.len(), 3);
        assert_eq!(
            attr(&decoded[0], ATTR_TYPE_INSTANCE),
            Some(Value::from("free")),
            "the first list keeps its own type_instance"
        );
        assert_eq!(
            attr(&decoded[1], ATTR_TYPE_INSTANCE),
            None,
            "the notification never had a type_instance"
        );
        assert_eq!(
            attr(&decoded[2], ATTR_TYPE_INSTANCE),
            None,
            "the second list must NOT have inherited \"free\" from the first"
        );

        // The third datagram (the second list) restates Host/Plugin/Type and has no
        // TypeInstance part: absent, which a receiver's reset sticky state reads as empty.
        let third_bytes = packets.iter().nth(2).unwrap();
        assert_eq!(count_parts(third_bytes, part::TYPE_HOST), 1);
        assert_eq!(count_parts(third_bytes, part::TYPE_PLUGIN), 1);
        assert_eq!(count_parts(third_bytes, part::TYPE_TYPE), 1);
        assert_eq!(count_parts(third_bytes, part::TYPE_TYPE_INSTANCE), 0);
    }
}
