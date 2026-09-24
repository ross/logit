//! Decoding one collectd datagram into events: the decode half of [`super`]'s module doc, which is
//! the spec for everything here.
//!
//! A flat walk over [`super::part::read_part`] with a block of **sticky** state reset per datagram,
//! as collectd's own `parse_packet` does: an identity part sets a field, a Values part dispatches
//! one value list against whatever is set. Every string part becomes a zero-copy
//! [`bytes::Bytes::slice`] of the datagram, so attributes share the receive buffer's allocation
//! (`docs/design/memory.md` §2).

use super::part::{self, DsValue, PartError, PartHeader};
use super::types_db::{DataSource, TypesDb};
use super::{cdtime_to_nanos, CDTIME_ONE_SECOND, MAX_VALUES_PER_LIST};
use crate::{CodecError, Decoder};
use bytes::Bytes;
use logit_core::interner::{intern, Symbol};
use logit_core::{
    AttrMap, BodyFormat, Diagnostics, Event, LogRecord, MetricKind, MetricRecord, Resource, Scope,
    Severity, Sum, Temporality, Value,
};
use std::fmt::Write as _;
use std::ops::Range;
use std::sync::Arc;

/// Decodes collectd binary-protocol datagrams, with no socket, so framing, stickiness, and
/// malformed-input tests run against it directly.
pub struct CollectdDecoder {
    /// One shared resource for every batch, **not** one per host (see [`super`]'s module doc).
    resource: Arc<Resource>,
    diag: Diagnostics,
    /// Scratch for the record name being built (`<plugin>.<type>[.<i>]`), a field so a
    /// 64-data-source list allocates no `String`.
    name: String,
    /// The `collectd.*` attribute keys, interned once so the per-list path uses
    /// [`AttrMap::insert_sym`] instead of re-hashing them.
    keys: AttrKeys,
    /// The operator-supplied `types.db`, loaded once per component; `None` means index naming.
    /// [`super::types_db`] says what it changes and what it doesn't.
    types_db: Option<Arc<TypesDb>>,
}

/// The interned `collectd.*` attribute keys; see [`CollectdDecoder::keys`].
struct AttrKeys {
    host: Symbol,
    plugin: Symbol,
    plugin_instance: Symbol,
    type_: Symbol,
    type_instance: Symbol,
    interval: Symbol,
    severity: Symbol,
}

impl CollectdDecoder {
    pub fn new(resource: Arc<Resource>) -> Self {
        Self {
            resource,
            diag: Diagnostics::default(),
            name: String::new(),
            keys: AttrKeys {
                host: intern(super::ATTR_HOST),
                plugin: intern(super::ATTR_PLUGIN),
                plugin_instance: intern(super::ATTR_PLUGIN_INSTANCE),
                type_: intern(super::ATTR_TYPE),
                type_instance: intern(super::ATTR_TYPE_INSTANCE),
                interval: intern(super::ATTR_INTERVAL),
                severity: intern(super::ATTR_SEVERITY),
            },
            types_db: None,
        }
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    /// Resolves data-source *names* from an operator-supplied `types.db`
    /// ([`super::types_db::TypesDb`]), turning `load.load.0` into `load.load.shortterm`.
    ///
    /// Naming rule, once per Values part:
    ///
    /// - the list's `type` resolves **and** the entry's data-source count and kinds match the
    ///   wire's: `<plugin>.<type>` for a single-data-source type (the lone data source,
    ///   conventionally `value`, is omitted, as `write_graphite` does), else
    ///   `<plugin>.<type>.<ds_name>`;
    /// - it resolves but the count or a kind disagrees: index naming, plus a throttled
    ///   `types_db_mismatch` diagnostic. The configured file is not the sender's, and naming from
    ///   it would mislabel a real measurement;
    /// - it does not resolve: index naming, **no** diagnostic. A type missing from `types.db` is
    ///   routine (a custom plugin, a newer collectd).
    ///
    /// Never changes what `collectd_out` puts back on the wire; see [`super::types_db`]'s module
    /// doc, and `collectd_fixed_point.rs`'s `names_do_not_affect_the_fixed_point`.
    pub fn with_types_db(mut self, types_db: Arc<TypesDb>) -> Self {
        self.types_db = Some(types_db);
        self
    }

    /// This decoder's diagnostics handle.
    ///
    /// Public, unlike `StatsdDecoder`/`SyslogDecoder`'s test-only equivalents, because
    /// `collectd_in` lives in `logit-inputs`, and its test that `with_diagnostics` reached the
    /// decoder can't call a crate-private accessor.
    pub fn diag(&self) -> &Diagnostics {
        &self.diag
    }
}

/// The identity, time, and interval a Values part is dispatched against. Reset per datagram, as
/// collectd's receiver does; a sender relying on otherwise would break on any dropped packet.
///
/// The five identity fields hold *byte ranges into the datagram*. `None` means "no such part" or
/// "an empty string part", which are the same thing on this wire (an empty string part is how a
/// sender clears an instance).
#[derive(Debug, Default)]
struct Sticky {
    host: Option<Range<usize>>,
    plugin: Option<Range<usize>>,
    plugin_instance: Option<Range<usize>>,
    type_: Option<Range<usize>>,
    type_instance: Option<Range<usize>>,
    /// Unix nanoseconds; `0` means no Time/TimeHR part yet, so the list takes `received_at`.
    time_ns: i64,
    /// Raw `cdtime_t`; `0` means unspecified, which leaves `collectd.interval` absent.
    interval_cdtime: u64,
    /// The raw severity from the latest `0x0101` part; `0` (never a valid severity) means none yet.
    /// Read only by [`CollectdDecoder::decode_notification`].
    severity: u64,
}

/// Whether to keep walking this datagram after a part. Only an Encryption part stops early.
enum Step {
    Advance,
    Stop,
}

/// Why a part could not be decoded: [`PartError`] covers framing only, so a payload-shape problem
/// reports what was wrong with the payload. Every variant abandons the rest of the datagram; see
/// [`CollectdDecoder::decode_into`] for the events already decoded from it.
#[derive(Debug, thiserror::Error)]
enum PartFault {
    #[error(transparent)]
    Framing(#[from] PartError),
    #[error("string part {part_type:#06x} is not NUL-terminated")]
    UnterminatedString { part_type: u16 },
    #[error("numeric part {part_type:#06x} is {len} bytes, not {expected}")]
    BadNumberLength { part_type: u16, len: usize, expected: usize },
    #[error("values part is {len} bytes, too short to carry a data-source count")]
    ShortValues { len: usize },
    #[error("values part declares {count} data source(s) in {len} bytes, expected {expected}")]
    BadValuesLength { count: usize, len: usize, expected: usize },
    #[error("values part declares {count} data source(s), outside 1..={max}")]
    BadValuesCount { count: usize, max: usize },
    #[error("values part declares unknown data-source type {ds_type}")]
    UnknownDsType { ds_type: u8 },
}

impl Decoder for CollectdDecoder {
    fn decode_into(
        &mut self,
        bytes: Bytes,
        received_at: i64,
        out: &mut Vec<Event>,
    ) -> Result<(Arc<Resource>, Option<Arc<Scope>>), CodecError> {
        let pushed_before = out.len();
        let mut sticky = Sticky::default();
        let mut at = 0usize;
        while at < bytes.len() {
            match self.decode_part(&bytes, at, &mut sticky, received_at, out) {
                Ok((Step::Advance, len)) => at += len,
                Ok((Step::Stop, _)) => break,
                // A length-prefixed part stream has no resync point, so the rest of the datagram is
                // lost. Lists decoded before the bad part are independent metrics that shared a
                // packet, so they are kept. With nothing decoded yet, the datagram fails as a
                // whole and the listener's `bad_datagram` counter fires.
                Err(fault) => {
                    let kept = out.len() - pushed_before;
                    if kept > 0 {
                        self.diag.warn_throttled(
                            "bad_part",
                            format_args!(
                                "collectd: {fault}; abandoning the rest of the datagram and \
                                 keeping the {kept} value list(s) already decoded from it"
                            ),
                        );
                        break;
                    }
                    return Err(CodecError::Malformed(format!("collectd: {fault}")));
                }
            }
        }
        // collectd has no instrumentation-scope concept.
        Ok((self.resource.clone(), None))
    }
}

impl CollectdDecoder {
    /// Decodes the part at `at`, returning whether to keep walking and how many bytes to advance.
    fn decode_part(
        &mut self,
        bytes: &Bytes,
        at: usize,
        sticky: &mut Sticky,
        received_at: i64,
        out: &mut Vec<Event>,
    ) -> Result<(Step, usize), PartFault> {
        let (header, payload) = part::read_part(bytes, at)?;
        let payload_at = at + part::HEADER_LEN;
        match header.part_type {
            part::TYPE_HOST => sticky.host = string_range(&header, payload, payload_at)?,
            part::TYPE_PLUGIN => sticky.plugin = string_range(&header, payload, payload_at)?,
            part::TYPE_PLUGIN_INSTANCE => {
                sticky.plugin_instance = string_range(&header, payload, payload_at)?
            }
            part::TYPE_TYPE => sticky.type_ = string_range(&header, payload, payload_at)?,
            part::TYPE_TYPE_INSTANCE => {
                sticky.type_instance = string_range(&header, payload, payload_at)?
            }
            // Legacy second-resolution Time; re-emitted as TimeHR (normalization 1).
            part::TYPE_TIME => {
                let seconds = read_number(&header, payload)?;
                sticky.time_ns =
                    i64::try_from(seconds.saturating_mul(1_000_000_000)).unwrap_or(i64::MAX);
            }
            part::TYPE_TIME_HR => sticky.time_ns = cdtime_to_nanos(read_number(&header, payload)?),
            // Legacy Interval, likewise; seconds scale into cdtime ticks exactly.
            part::TYPE_INTERVAL => {
                sticky.interval_cdtime =
                    read_number(&header, payload)?.saturating_mul(CDTIME_ONE_SECOND)
            }
            part::TYPE_INTERVAL_HR => sticky.interval_cdtime = read_number(&header, payload)?,
            part::TYPE_VALUES => {
                self.decode_values(bytes, payload, sticky, received_at, out)?;
            }
            // Sticky; the notification is dispatched at the Message part, as a Values part is.
            part::TYPE_SEVERITY => sticky.severity = read_number(&header, payload)?,
            part::TYPE_MESSAGE => {
                let message_range = string_range(&header, payload, payload_at)?;
                self.decode_notification(bytes, message_range, sticky, received_at, out);
            }
            // Everything after an Encryption part is ciphertext and this codec holds no keys
            // (`docs/known-gaps.md`): stop, rather than report each garbage part as malformed.
            part::TYPE_ENCRYPTION => {
                self.diag.warn_throttled(
                    "encrypted_packet_dropped",
                    "collectd: encrypted packet (part 0x0210); dropping the rest of the datagram \
                     -- this codec does not implement collectd's `SecurityLevel Encrypt`",
                );
                return Ok((Step::Stop, header.len));
            }
            // A Signature part signs the plaintext that follows, which stays readable, unverified.
            // No diagnostic: it would fire on every datagram from a `SecurityLevel Sign` sender.
            part::TYPE_SIGNATURE => {}
            // Skipped by length, so a newer collectd can add a part type without breaking this.
            _ => {}
        }
        Ok((Step::Advance, header.len))
    }

    /// Decodes one Values part into one [`Event`] carrying one [`MetricRecord`] per data source, in
    /// wire order.
    ///
    /// **Every length and type check happens before any allocation.** The declared data-source
    /// count is attacker-controlled (up to 65535), and sizing anything from it before checking it
    /// against the part's length is the bug `crates/logit-proto/tests/robustness.rs` catches.
    fn decode_values(
        &mut self,
        bytes: &Bytes,
        payload: &[u8],
        sticky: &Sticky,
        received_at: i64,
        out: &mut Vec<Event>,
    ) -> Result<(), PartFault> {
        let len = payload.len() + part::HEADER_LEN;
        if payload.len() < 2 {
            return Err(PartFault::ShortValues { len });
        }
        let count = u16::from_be_bytes([payload[0], payload[1]]) as usize;
        let expected = part::VALUES_OVERHEAD + part::BYTES_PER_VALUE * count;
        if len != expected {
            return Err(PartFault::BadValuesLength { count, len, expected });
        }
        if count == 0 || count > MAX_VALUES_PER_LIST {
            return Err(PartFault::BadValuesCount { count, max: MAX_VALUES_PER_LIST });
        }
        let types = &payload[2..2 + count];
        for &ds_type in types {
            if !DsValue::is_known_type(ds_type) {
                return Err(PartFault::UnknownDsType { ds_type });
            }
        }

        // collectd's `network_dispatch_values` rejects a list with an empty host, plugin, or type
        // (`-EINVAL`). Skipped, not malformed: the part is well formed, and so are its neighbors.
        let (Some(host), Some(plugin), Some(type_)) = (&sticky.host, &sticky.plugin, &sticky.type_)
        else {
            self.diag.warn_throttled(
                "incomplete_identity",
                "collectd: a value list arrived with an empty host, plugin or type; skipping it \
                 (collectd's own receiver rejects the same list)",
            );
            return Ok(());
        };

        let mut attrs = AttrMap::new();
        attrs.insert_sym(self.keys.host, string_value(bytes, host.clone()));
        attrs.insert_sym(self.keys.plugin, string_value(bytes, plugin.clone()));
        if let Some(range) = &sticky.plugin_instance {
            attrs.insert_sym(self.keys.plugin_instance, string_value(bytes, range.clone()));
        }
        attrs.insert_sym(self.keys.type_, string_value(bytes, type_.clone()));
        if let Some(range) = &sticky.type_instance {
            attrs.insert_sym(self.keys.type_instance, string_value(bytes, range.clone()));
        }
        if sticky.interval_cdtime != 0 {
            // Exact up to `f64`'s 2^53-tick mantissa (~97 days): the divisor is a power of two.
            let seconds = sticky.interval_cdtime as f64 / CDTIME_ONE_SECOND as f64;
            attrs.insert_sym(self.keys.interval, Value::F64(seconds));
        }

        // collectd's receiver rejects a `time == 0` list; this stamps receipt time instead, like
        // every other input (`crate::Decoder::decode_into`'s `received_at` contract).
        let timestamp = if sticky.time_ns != 0 { sticky.time_ns } else { received_at };
        let mut event = Event::empty(timestamp, attrs);

        let plugin_bytes = &bytes[plugin.clone()];
        let type_bytes = &bytes[type_.clone()];
        // One `types.db` lookup per Values part, not per data source. It borrows `self.types_db`
        // alongside `self.name`/`self.diag` (disjoint fields), so nothing is cloned per list.
        let data_sources =
            resolve_data_sources(self.types_db.as_deref(), type_bytes, types, &mut self.diag);
        let values_at = 2 + count;
        for index in 0..count {
            let ds_type = types[index];
            let raw: [u8; 8] = payload[values_at + index * 8..values_at + (index + 1) * 8]
                .try_into()
                .expect("exactly 8 bytes: the part's length was validated against `count` above");
            let value = DsValue::from_wire(ds_type, raw)
                .expect("every data-source type byte was validated above");

            self.name.clear();
            push_lossy(&mut self.name, plugin_bytes);
            self.name.push('.');
            push_lossy(&mut self.name, type_bytes);
            // A single-data-source list is `<plugin>.<type>` either way, as collectd's
            // `write_graphite` omits the lone data source's name (conventionally `value`).
            match data_sources {
                Some(sources) if count > 1 => {
                    self.name.push('.');
                    self.name.push_str(&sources[index].name);
                }
                _ if count > 1 => {
                    let _ = write!(self.name, ".{index}");
                }
                _ => {}
            }
            let name = intern(&self.name);

            let (kind, flags) = match value {
                DsValue::Counter(v) => (
                    MetricKind::Sum(Sum {
                        value: v as f64,
                        temporality: Temporality::Cumulative,
                        monotonic: true,
                    }),
                    0,
                ),
                DsValue::Derive(v) => (
                    MetricKind::Sum(Sum {
                        value: v as f64,
                        temporality: Temporality::Cumulative,
                        monotonic: false,
                    }),
                    0,
                ),
                DsValue::Absolute(v) => (
                    MetricKind::Sum(Sum {
                        value: v as f64,
                        temporality: Temporality::Delta,
                        monotonic: true,
                    }),
                    0,
                ),
                // NaN is collectd's "no value for this interval": a flagged point, not a
                // `Gauge(NaN)` that never compares equal ([`super`]'s decode table).
                DsValue::Gauge(v) if v.is_nan() => {
                    (MetricKind::Gauge(0.0), MetricRecord::FLAG_NO_RECORDED_VALUE)
                }
                DsValue::Gauge(v) => (MetricKind::Gauge(v), 0),
            };
            event.metrics.push(MetricRecord { flags, ..MetricRecord::new(name, kind) });
        }
        out.push(event);
        Ok(())
    }

    /// Dispatches a notification at a Message part, per [`super`]'s "Notifications" section.
    ///
    /// [`string_range`] has already validated framing, so every failure here is semantic (an
    /// invalid severity, an empty message, no host): a skip-and-count, never a `PartFault`.
    fn decode_notification(
        &mut self,
        bytes: &Bytes,
        message_range: Option<Range<usize>>,
        sticky: &Sticky,
        received_at: i64,
        out: &mut Vec<Event>,
    ) {
        // `notification_t` has no interval field, so a notification never carries
        // `collectd.interval`, even when an earlier list in the datagram set one.
        let severity = match sticky.severity {
            1 => Severity::Error,
            2 => Severity::Warn,
            4 => Severity::Info,
            other => {
                self.diag.warn_throttled(
                    "notification_dropped",
                    format_args!(
                        "collectd: notification severity {other} is not one of 1 (FAILURE), 2 \
                         (WARNING), 4 (OKAY); dropping"
                    ),
                );
                return;
            }
        };
        let Some(message_range) = message_range else {
            self.diag.warn_throttled(
                "notification_dropped",
                "collectd: notification message is empty; dropping (collectd's own receiver \
                 rejects the same notification)",
            );
            return;
        };
        let Some(host) = &sticky.host else {
            self.diag.warn_throttled(
                "notification_dropped",
                "collectd: notification arrived with no host set; dropping",
            );
            return;
        };

        let mut attrs = AttrMap::new();
        attrs.insert_sym(self.keys.host, string_value(bytes, host.clone()));
        if let Some(range) = &sticky.plugin {
            attrs.insert_sym(self.keys.plugin, string_value(bytes, range.clone()));
        }
        if let Some(range) = &sticky.plugin_instance {
            attrs.insert_sym(self.keys.plugin_instance, string_value(bytes, range.clone()));
        }
        if let Some(range) = &sticky.type_ {
            attrs.insert_sym(self.keys.type_, string_value(bytes, range.clone()));
        }
        if let Some(range) = &sticky.type_instance {
            attrs.insert_sym(self.keys.type_instance, string_value(bytes, range.clone()));
        }
        attrs.insert_sym(self.keys.severity, Value::U64(sticky.severity));

        // Receipt time when the wire carried none, as on the Values path.
        let timestamp = if sticky.time_ns != 0 { sticky.time_ns } else { received_at };
        let message = string_value(bytes, message_range);
        out.push(Event::log(
            timestamp,
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
        ));
    }
}

/// The `types.db` entry to name this value list's data sources from, or `None` for index naming.
///
/// `None` covers no `types.db`, a type it doesn't define, and a type it defines *differently* from
/// what arrived. Only the last emits a diagnostic, throttled, since a mismatched sender repeats it
/// every interval.
///
/// The kinds are compared, not just the count: a file that agrees on the count but not the kinds
/// describes a different type of the same width.
fn resolve_data_sources<'a>(
    types_db: Option<&'a TypesDb>,
    type_bytes: &[u8],
    ds_types: &[u8],
    diag: &mut Diagnostics,
) -> Option<&'a [DataSource]> {
    let types_db = types_db?;
    // A non-UTF-8 type name can't match a `types.db` key; it gets index naming, no diagnostic.
    let type_name = std::str::from_utf8(type_bytes).ok()?;
    let sources = types_db.get(type_name)?;
    let matches = sources.len() == ds_types.len()
        && sources
            .iter()
            .zip(ds_types)
            .all(|(source, &ds_type)| source.kind.ds_type_byte() == ds_type);
    if matches {
        return Some(sources);
    }
    diag.warn_throttled(
        "types_db_mismatch",
        format_args!(
            "collectd: type '{type_name}' is defined in the configured types.db with {} data \
             source(s) that do not match the {} on the wire; naming this list's records by index \
             instead -- the configured types.db is probably not the one the sender is running \
             against",
            sources.len(),
            ds_types.len()
        ),
    );
    None
}

/// The byte range of a string part's content (the payload minus its NUL), or `None` for the empty
/// string, which clears the sticky field.
///
/// A string part with no NUL rejects the datagram, as collectd's `parse_part_string` does; taking
/// the whole payload would accept a packet collectd refuses.
fn string_range(
    header: &PartHeader,
    payload: &[u8],
    payload_at: usize,
) -> Result<Option<Range<usize>>, PartFault> {
    if payload.last() != Some(&0) {
        return Err(PartFault::UnterminatedString { part_type: header.part_type });
    }
    let content = payload.len() - 1;
    Ok((content > 0).then_some(payload_at..payload_at + content))
}

/// Reads a numeric part's single `u64` BE payload.
fn read_number(header: &PartHeader, payload: &[u8]) -> Result<u64, PartFault> {
    let raw: [u8; 8] = payload.try_into().map_err(|_| PartFault::BadNumberLength {
        part_type: header.part_type,
        len: payload.len() + part::HEADER_LEN,
        expected: part::NUMBER_PART_LEN,
    })?;
    Ok(u64::from_be_bytes(raw))
}

/// One string part's content as an attribute value, sharing the datagram's allocation:
/// [`Value::Str`] when valid UTF-8, else [`Value::Bytes`]. collectd's strings are bytes (a host
/// name from a non-UTF-8 locale is real), and lossy replacement would break the fixed point.
fn string_value(bytes: &Bytes, range: Range<usize>) -> Value {
    let slice = bytes.slice(range);
    if std::str::from_utf8(&slice).is_ok() {
        Value::Str(slice)
    } else {
        Value::Bytes(slice)
    }
}

/// Appends `bytes` to `out` as UTF-8, one U+FFFD per invalid sequence, without allocating. A record
/// name must be `&str` to be interned; the *attribute* keeps the raw bytes.
fn push_lossy(out: &mut String, bytes: &[u8]) {
    for chunk in bytes.utf8_chunks() {
        out.push_str(chunk.valid());
        if !chunk.invalid().is_empty() {
            out.push(char::REPLACEMENT_CHARACTER);
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::super::{
        ATTR_HOST, ATTR_INTERVAL, ATTR_PLUGIN, ATTR_PLUGIN_INSTANCE, ATTR_SEVERITY, ATTR_TYPE,
        ATTR_TYPE_INSTANCE,
    };
    use super::*;
    use logit_core::interner::resolve;
    use logit_core::telemetry::Registry;

    pub(crate) const RECEIVED_AT: i64 = 1_700_000_000_000_000_000;
    /// `1_700_000_000` seconds as a `cdtime_t`.
    pub(crate) const TIME_HR: u64 = 1_700_000_000 << 30;

    /// Builds collectd datagrams byte by byte, **independent of [`super::super::part`]'s
    /// writers**: sharing the encoder's code would make decode tests tautological, and couldn't
    /// express malformed shapes (a missing NUL, an inflated count, a `len` of 0).
    #[derive(Default)]
    pub(crate) struct PacketBuilder {
        bytes: Vec<u8>,
    }

    impl PacketBuilder {
        pub(crate) fn new() -> Self {
            Self::default()
        }

        /// A well-formed string part: header, content, NUL.
        pub(crate) fn string(self, part_type: u16, value: &[u8]) -> Self {
            let mut payload = value.to_vec();
            payload.push(0);
            self.part(part_type, &payload)
        }

        /// A string part with **no** NUL terminator, which collectd rejects.
        pub(crate) fn unterminated_string(self, part_type: u16, value: &[u8]) -> Self {
            self.part(part_type, value)
        }

        /// A well-formed `u64`-BE numeric part.
        pub(crate) fn number(self, part_type: u16, value: u64) -> Self {
            self.part(part_type, &value.to_be_bytes())
        }

        /// A Values part from `(ds_type, raw 8 bytes)` pairs; raw so a test controls byte order.
        pub(crate) fn values(self, values: &[(u8, [u8; 8])]) -> Self {
            let mut payload = Vec::new();
            payload.extend_from_slice(&(values.len() as u16).to_be_bytes());
            for (ds_type, _) in values {
                payload.push(*ds_type);
            }
            for (_, raw) in values {
                payload.extend_from_slice(raw);
            }
            self.part(part::TYPE_VALUES, &payload)
        }

        /// A Values part whose declared data-source count is a lie: the hostile-length case.
        pub(crate) fn values_with_declared_count(
            self,
            declared: u16,
            payload_bytes: &[u8],
        ) -> Self {
            let mut payload = Vec::new();
            payload.extend_from_slice(&declared.to_be_bytes());
            payload.extend_from_slice(payload_bytes);
            self.part(part::TYPE_VALUES, &payload)
        }

        /// A well-framed part (`len` computed); every helper above goes through it.
        pub(crate) fn part(mut self, part_type: u16, payload: &[u8]) -> Self {
            let len = (part::HEADER_LEN + payload.len()) as u16;
            self.bytes.extend_from_slice(&part_type.to_be_bytes());
            self.bytes.extend_from_slice(&len.to_be_bytes());
            self.bytes.extend_from_slice(payload);
            self
        }

        /// A part header with an arbitrary declared `len`, for the framing-error cases.
        pub(crate) fn header(mut self, part_type: u16, len: u16) -> Self {
            self.bytes.extend_from_slice(&part_type.to_be_bytes());
            self.bytes.extend_from_slice(&len.to_be_bytes());
            self
        }

        pub(crate) fn raw(mut self, bytes: &[u8]) -> Self {
            self.bytes.extend_from_slice(bytes);
            self
        }

        pub(crate) fn build(self) -> Bytes {
            Bytes::from(self.bytes)
        }
    }

    /// The eight bytes of an IEEE-754 `1.5` in collectd's little-endian gauge order.
    pub(crate) const GAUGE_1_5: [u8; 8] = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF8, 0x3F];

    pub(crate) fn gauge(v: f64) -> (u8, [u8; 8]) {
        (part::DS_GAUGE, v.to_le_bytes())
    }

    pub(crate) fn counter(v: u64) -> (u8, [u8; 8]) {
        (part::DS_COUNTER, v.to_be_bytes())
    }

    pub(crate) fn derive(v: i64) -> (u8, [u8; 8]) {
        (part::DS_DERIVE, v.to_be_bytes())
    }

    pub(crate) fn absolute(v: u64) -> (u8, [u8; 8]) {
        (part::DS_ABSOLUTE, v.to_be_bytes())
    }

    /// A minimal, complete single-gauge datagram.
    pub(crate) fn single_gauge_packet() -> Bytes {
        PacketBuilder::new()
            .string(part::TYPE_HOST, b"web-1")
            .number(part::TYPE_TIME_HR, TIME_HR)
            .number(part::TYPE_INTERVAL_HR, 10u64 << 30)
            .string(part::TYPE_PLUGIN, b"memory")
            .string(part::TYPE_TYPE, b"memory")
            .string(part::TYPE_TYPE_INSTANCE, b"used")
            .values(&[gauge(1.5)])
            .build()
    }

    fn decoder() -> CollectdDecoder {
        CollectdDecoder::new(Arc::new(Resource::default()))
    }

    /// A decoder whose diagnostics mirror into a drainable registry, for asserting on
    /// `logit.component.diagnostics{key}`.
    fn decoder_with_diag() -> (CollectdDecoder, Arc<Registry>) {
        let registry = Registry::new();
        let diag = Diagnostics::new("collectd_in").with_telemetry(registry.telemetry_for(
            "collectd_in",
            "collectd_in",
            "listener",
        ));
        (decoder().with_diagnostics(diag), registry)
    }

    /// Whether `registry` recorded a `logit.component.diagnostics` point for `key`. Drains, so call
    /// it once per test.
    fn diagnosed(registry: &Registry, key: &str) -> bool {
        registry.drain(0).iter().any(|event| {
            event.metrics.iter().any(|m| resolve(m.name) == "logit.component.diagnostics")
                && event.attributes.get("key").and_then(|v| v.as_str()) == Some(key)
        })
    }

    fn decode(bytes: Bytes) -> Vec<Event> {
        let mut out = Vec::new();
        decoder().decode_into(bytes, RECEIVED_AT, &mut out).expect("decode must succeed");
        out
    }

    fn attr(event: &Event, key: &str) -> Option<Value> {
        event.attributes.get(key).cloned()
    }

    // --- happy paths ---------------------------------------------------------------------------

    #[test]
    fn a_single_gauge_list_decodes_to_one_event_with_one_record() {
        let events = decode(single_gauge_packet());
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.timestamp, RECEIVED_AT);
        assert_eq!(event.metrics.len(), 1);
        assert_eq!(resolve(event.metrics[0].name), "memory.memory");
        assert_eq!(event.metrics[0].kind, MetricKind::Gauge(1.5));
        assert_eq!(attr(event, ATTR_HOST), Some(Value::from("web-1")));
        assert_eq!(attr(event, ATTR_PLUGIN), Some(Value::from("memory")));
        assert_eq!(attr(event, ATTR_TYPE), Some(Value::from("memory")));
        assert_eq!(attr(event, ATTR_TYPE_INSTANCE), Some(Value::from("used")));
        assert_eq!(attr(event, ATTR_PLUGIN_INSTANCE), None);
        assert_eq!(attr(event, ATTR_INTERVAL), Some(Value::F64(10.0)));
    }

    /// A gauge is little-endian, every other value type big-endian.
    #[test]
    fn a_gauge_is_read_little_endian_and_every_other_type_big_endian() {
        let events = decode(
            PacketBuilder::new()
                .string(part::TYPE_HOST, b"h")
                .string(part::TYPE_PLUGIN, b"p")
                .string(part::TYPE_TYPE, b"t")
                .values(&[
                    (part::DS_GAUGE, GAUGE_1_5),
                    (part::DS_COUNTER, [0, 0, 0, 0, 0, 0, 0, 7]),
                    (part::DS_DERIVE, [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]),
                    (part::DS_ABSOLUTE, [0, 0, 0, 0, 0, 0, 0, 9]),
                ])
                .build(),
        );
        let kinds: Vec<MetricKind> =
            events[0].metrics.iter().map(|record| record.kind.clone()).collect();
        assert_eq!(
            kinds,
            vec![
                MetricKind::Gauge(1.5),
                MetricKind::Sum(Sum {
                    value: 7.0,
                    temporality: Temporality::Cumulative,
                    monotonic: true
                }),
                MetricKind::Sum(Sum {
                    value: -1.0,
                    temporality: Temporality::Cumulative,
                    monotonic: false
                }),
                MetricKind::Sum(Sum {
                    value: 9.0,
                    temporality: Temporality::Delta,
                    monotonic: true
                }),
            ]
        );
    }

    #[test]
    fn a_multi_value_list_names_its_records_by_zero_based_index() {
        let events = decode(
            PacketBuilder::new()
                .string(part::TYPE_HOST, b"web-1")
                .string(part::TYPE_PLUGIN, b"load")
                .string(part::TYPE_TYPE, b"load")
                .values(&[gauge(0.1), gauge(0.2), gauge(0.3)])
                .build(),
        );
        assert_eq!(events.len(), 1, "one Values part is one event, not one per data source");
        let names: Vec<&str> =
            events[0].metrics.iter().map(|record| resolve(record.name)).collect();
        assert_eq!(names, vec!["load.load.0", "load.load.1", "load.load.2"]);
    }

    #[test]
    fn identity_parts_stay_sticky_across_lists_and_past_an_unknown_part() {
        let events = decode(
            PacketBuilder::new()
                .string(part::TYPE_HOST, b"web-1")
                .string(part::TYPE_PLUGIN, b"cpu")
                .string(part::TYPE_TYPE, b"cpu")
                .string(part::TYPE_TYPE_INSTANCE, b"user")
                .values(&[derive(10)])
                // An unknown part type, skipped by length; sticky identity survives it.
                .part(0x0999, b"whatever this is")
                .string(part::TYPE_TYPE_INSTANCE, b"system")
                .values(&[derive(20)])
                .build(),
        );
        assert_eq!(events.len(), 2);
        for event in &events {
            assert_eq!(attr(event, ATTR_HOST), Some(Value::from("web-1")));
            assert_eq!(attr(event, ATTR_PLUGIN), Some(Value::from("cpu")));
        }
        assert_eq!(attr(&events[0], ATTR_TYPE_INSTANCE), Some(Value::from("user")));
        assert_eq!(attr(&events[1], ATTR_TYPE_INSTANCE), Some(Value::from("system")));
    }

    /// An empty string part *clears* an instance mid-datagram, leaving the attribute absent.
    #[test]
    fn an_empty_instance_part_clears_the_attribute_rather_than_setting_it_empty() {
        let events = decode(
            PacketBuilder::new()
                .string(part::TYPE_HOST, b"web-1")
                .string(part::TYPE_PLUGIN, b"df")
                .string(part::TYPE_PLUGIN_INSTANCE, b"root")
                .string(part::TYPE_TYPE, b"df_complex")
                .values(&[gauge(1.0)])
                .string(part::TYPE_PLUGIN_INSTANCE, b"")
                .values(&[gauge(2.0)])
                .build(),
        );
        assert_eq!(attr(&events[0], ATTR_PLUGIN_INSTANCE), Some(Value::from("root")));
        assert_eq!(attr(&events[1], ATTR_PLUGIN_INSTANCE), None);
    }

    #[test]
    fn a_legacy_time_and_interval_decode_to_the_same_values_their_hr_forms_would() {
        let legacy = decode(
            PacketBuilder::new()
                .string(part::TYPE_HOST, b"h")
                .number(part::TYPE_TIME, 1_700_000_000)
                .number(part::TYPE_INTERVAL, 10)
                .string(part::TYPE_PLUGIN, b"p")
                .string(part::TYPE_TYPE, b"t")
                .values(&[gauge(1.0)])
                .build(),
        );
        assert_eq!(legacy[0].timestamp, 1_700_000_000_000_000_000);
        assert_eq!(attr(&legacy[0], ATTR_INTERVAL), Some(Value::F64(10.0)));
    }

    #[test]
    fn a_zero_interval_leaves_the_attribute_absent() {
        let events = decode(
            PacketBuilder::new()
                .string(part::TYPE_HOST, b"h")
                .number(part::TYPE_INTERVAL_HR, 0)
                .string(part::TYPE_PLUGIN, b"p")
                .string(part::TYPE_TYPE, b"t")
                .values(&[gauge(1.0)])
                .build(),
        );
        assert_eq!(attr(&events[0], ATTR_INTERVAL), None);
    }

    #[test]
    fn a_list_with_a_time_part_uses_it_instead_of_received_at() {
        let events = decode(
            PacketBuilder::new()
                .string(part::TYPE_HOST, b"h")
                .number(part::TYPE_TIME_HR, TIME_HR)
                .string(part::TYPE_PLUGIN, b"p")
                .string(part::TYPE_TYPE, b"t")
                .values(&[gauge(1.0)])
                .build(),
        );
        assert_eq!(events[0].timestamp, 1_700_000_000_000_000_000);

        let mut out = Vec::new();
        let other_receipt = 42;
        decoder().decode_into(single_gauge_packet(), other_receipt, &mut out).unwrap();
        assert_eq!(out[0].timestamp, 1_700_000_000_000_000_000, "a TimeHR part wins over receipt");
    }

    #[test]
    fn a_list_with_no_time_part_borrows_the_datagrams_received_at() {
        let bytes = PacketBuilder::new()
            .string(part::TYPE_HOST, b"h")
            .string(part::TYPE_PLUGIN, b"p")
            .string(part::TYPE_TYPE, b"t")
            .values(&[gauge(1.0)])
            .build();
        let mut out = Vec::new();
        decoder().decode_into(bytes, 12345, &mut out).unwrap();
        assert_eq!(out[0].timestamp, 12345);
    }

    #[test]
    fn a_non_utf8_host_becomes_value_bytes_rather_than_lossy_text() {
        let events = decode(
            PacketBuilder::new()
                .string(part::TYPE_HOST, &[0xFF, 0xFE, b'h'])
                .string(part::TYPE_PLUGIN, b"p")
                .string(part::TYPE_TYPE, b"t")
                .values(&[gauge(1.0)])
                .build(),
        );
        assert_eq!(
            attr(&events[0], ATTR_HOST),
            Some(Value::Bytes(Bytes::from_static(&[0xFF, 0xFE, b'h'])))
        );
        // The record *name* must be text, so it is lossy; the attribute keeps the raw bytes.
        assert_eq!(resolve(events[0].metrics[0].name), "p.t");
    }

    #[test]
    fn a_nan_gauge_becomes_a_flagged_zero_gauge() {
        let events = decode(
            PacketBuilder::new()
                .string(part::TYPE_HOST, b"h")
                .string(part::TYPE_PLUGIN, b"p")
                .string(part::TYPE_TYPE, b"t")
                .values(&[gauge(f64::NAN)])
                .build(),
        );
        assert_eq!(events[0].metrics[0].kind, MetricKind::Gauge(0.0));
        assert!(events[0].metrics[0].is_no_recorded_value());
    }

    #[test]
    fn an_infinite_gauge_is_kept_as_is_and_never_flagged() {
        let events = decode(
            PacketBuilder::new()
                .string(part::TYPE_HOST, b"h")
                .string(part::TYPE_PLUGIN, b"p")
                .string(part::TYPE_TYPE, b"t")
                .values(&[gauge(f64::INFINITY), gauge(f64::NEG_INFINITY)])
                .build(),
        );
        assert_eq!(events[0].metrics[0].kind, MetricKind::Gauge(f64::INFINITY));
        assert_eq!(events[0].metrics[1].kind, MetricKind::Gauge(f64::NEG_INFINITY));
        assert!(!events[0].metrics[0].is_no_recorded_value());
    }

    #[test]
    fn decode_into_appends_to_an_already_populated_out_buffer() {
        let mut out = decode(single_gauge_packet());
        decoder().decode_into(single_gauge_packet(), RECEIVED_AT, &mut out).unwrap();
        assert_eq!(
            out.len(),
            2,
            "decode_into appends, never replaces (the accumulator depends on it)"
        );
    }

    #[test]
    fn a_signature_part_is_skipped_and_the_plaintext_behind_it_still_decodes() {
        let events = decode(
            PacketBuilder::new()
                .part(part::TYPE_SIGNATURE, &[0xAB; 36])
                .string(part::TYPE_HOST, b"h")
                .string(part::TYPE_PLUGIN, b"p")
                .string(part::TYPE_TYPE, b"t")
                .values(&[gauge(1.0)])
                .build(),
        );
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn an_encryption_part_stops_the_walk_and_reports_it() {
        let (mut decoder, registry) = decoder_with_diag();
        let bytes = PacketBuilder::new()
            .string(part::TYPE_HOST, b"h")
            .string(part::TYPE_PLUGIN, b"p")
            .string(part::TYPE_TYPE, b"t")
            .values(&[gauge(1.0)])
            .part(part::TYPE_ENCRYPTION, &[0xCD; 20])
            // Plausible-looking plaintext behind the Encryption part: it must not be decoded.
            .string(part::TYPE_TYPE_INSTANCE, b"nope")
            .values(&[gauge(2.0)])
            .build();
        let mut out = Vec::new();
        decoder.decode_into(bytes, RECEIVED_AT, &mut out).unwrap();
        assert_eq!(out.len(), 1, "only the list before the Encryption part is decoded");
        assert!(diagnosed(&registry, "encrypted_packet_dropped"));
    }

    #[test]
    fn a_message_before_any_identity_part_is_dropped_for_missing_host() {
        let (mut decoder, registry) = decoder_with_diag();
        let mut out = Vec::new();
        decoder
            .decode_into(
                PacketBuilder::new()
                    .number(part::TYPE_SEVERITY, 2)
                    .string(part::TYPE_MESSAGE, b"disk almost full")
                    .string(part::TYPE_HOST, b"h")
                    .string(part::TYPE_PLUGIN, b"p")
                    .string(part::TYPE_TYPE, b"t")
                    .values(&[gauge(1.0)])
                    .build(),
                RECEIVED_AT,
                &mut out,
            )
            .unwrap();
        assert_eq!(out.len(), 1, "the notification is dropped, the value list is not");
        assert!(out[0].log.is_none());
        assert!(diagnosed(&registry, "notification_dropped"));
    }

    // --- notifications (0x0100 Message / 0x0101 Severity) --------------------------------------

    /// Each stock severity maps per [`super::super`]'s "Notifications" table.
    #[test]
    fn every_severity_maps_to_its_own_log_severity() {
        for (wire, expected) in
            [(1u64, Severity::Error), (2u64, Severity::Warn), (4u64, Severity::Info)]
        {
            let events = decode(
                PacketBuilder::new()
                    .string(part::TYPE_HOST, b"web-1")
                    .string(part::TYPE_PLUGIN, b"load")
                    .string(part::TYPE_TYPE, b"load")
                    .number(part::TYPE_TIME_HR, TIME_HR)
                    .number(part::TYPE_SEVERITY, wire)
                    .string(part::TYPE_MESSAGE, b"threshold exceeded")
                    .build(),
            );
            assert_eq!(events.len(), 1, "severity {wire}");
            let log = events[0].log.as_ref().expect("a Message part must produce a log record");
            assert_eq!(log.severity, Some(expected), "severity {wire}");
            assert_eq!(log.message, Value::from("threshold exceeded"));
            assert_eq!(events[0].timestamp, 1_700_000_000_000_000_000);
            assert_eq!(attr(&events[0], ATTR_HOST), Some(Value::from("web-1")));
            assert_eq!(attr(&events[0], ATTR_PLUGIN), Some(Value::from("load")));
            assert_eq!(attr(&events[0], ATTR_TYPE), Some(Value::from("load")));
            assert_eq!(events[0].attributes.get(ATTR_SEVERITY), Some(&Value::U64(wire)));
            assert!(events[0].metrics.is_empty(), "a notification carries no metrics");
        }
    }

    /// A notification never carries `collectd.interval`, even when a sticky `IntervalHR` is set.
    #[test]
    fn a_notification_never_carries_an_interval_even_when_one_is_sticky() {
        let events = decode(
            PacketBuilder::new()
                .string(part::TYPE_HOST, b"web-1")
                .string(part::TYPE_PLUGIN, b"load")
                .string(part::TYPE_TYPE, b"load")
                .number(part::TYPE_INTERVAL_HR, 10u64 << 30)
                .number(part::TYPE_SEVERITY, 4)
                .string(part::TYPE_MESSAGE, b"ok now")
                .build(),
        );
        assert_eq!(attr(&events[0], ATTR_INTERVAL), None);
    }

    /// Plugin and type may be absent on a notification, unlike a value list.
    #[test]
    fn a_notification_with_no_plugin_or_type_still_decodes() {
        let events = decode(
            PacketBuilder::new()
                .string(part::TYPE_HOST, b"web-1")
                .number(part::TYPE_SEVERITY, 1)
                .string(part::TYPE_MESSAGE, b"host is down")
                .build(),
        );
        assert_eq!(events.len(), 1);
        assert_eq!(attr(&events[0], ATTR_PLUGIN), None);
        assert_eq!(attr(&events[0], ATTR_TYPE), None);
        assert_eq!(events[0].log.as_ref().unwrap().severity, Some(Severity::Error));
    }

    #[test]
    fn a_non_utf8_message_becomes_value_bytes_rather_than_lossy_text() {
        let events = decode(
            PacketBuilder::new()
                .string(part::TYPE_HOST, b"h")
                .number(part::TYPE_SEVERITY, 2)
                .string(part::TYPE_MESSAGE, &[0xFF, 0xFE, b'm'])
                .build(),
        );
        assert_eq!(
            events[0].log.as_ref().unwrap().message,
            Value::Bytes(Bytes::from_static(&[0xFF, 0xFE, b'm']))
        );
    }

    #[test]
    fn an_empty_message_is_dropped_and_counted() {
        let (mut decoder, registry) = decoder_with_diag();
        let mut out = Vec::new();
        decoder
            .decode_into(
                PacketBuilder::new()
                    .string(part::TYPE_HOST, b"h")
                    .number(part::TYPE_SEVERITY, 2)
                    .string(part::TYPE_MESSAGE, b"")
                    .build(),
                RECEIVED_AT,
                &mut out,
            )
            .unwrap();
        assert!(out.is_empty());
        assert!(diagnosed(&registry, "notification_dropped"));
    }

    #[test]
    fn an_out_of_set_severity_is_dropped_and_counted() {
        for wire in [0u64, 3, 5, 8, u64::MAX] {
            let (mut decoder, registry) = decoder_with_diag();
            let mut out = Vec::new();
            decoder
                .decode_into(
                    PacketBuilder::new()
                        .string(part::TYPE_HOST, b"h")
                        .number(part::TYPE_SEVERITY, wire)
                        .string(part::TYPE_MESSAGE, b"whatever")
                        .build(),
                    RECEIVED_AT,
                    &mut out,
                )
                .unwrap();
            assert!(out.is_empty(), "severity {wire}");
            assert!(diagnosed(&registry, "notification_dropped"), "severity {wire}");
        }
    }

    /// No Severity part leaves `0`, dropped like any out-of-set value.
    #[test]
    fn a_message_with_no_severity_part_at_all_is_dropped() {
        let events = decode(
            PacketBuilder::new()
                .string(part::TYPE_HOST, b"h")
                .string(part::TYPE_MESSAGE, b"x")
                .build(),
        );
        assert!(events.is_empty());
    }

    /// Sticky severity resets at the datagram boundary, like every other sticky field.
    #[test]
    fn sticky_severity_resets_per_datagram() {
        let mut decoder = decoder();
        let mut out = Vec::new();
        decoder
            .decode_into(
                PacketBuilder::new()
                    .string(part::TYPE_HOST, b"h")
                    .number(part::TYPE_SEVERITY, 4)
                    .string(part::TYPE_MESSAGE, b"first")
                    .build(),
                RECEIVED_AT,
                &mut out,
            )
            .unwrap();
        assert_eq!(out.len(), 1);

        // No Severity part of its own: it decodes only if severity leaked across datagrams.
        decoder
            .decode_into(
                PacketBuilder::new()
                    .string(part::TYPE_HOST, b"h")
                    .string(part::TYPE_MESSAGE, b"second")
                    .build(),
                RECEIVED_AT,
                &mut out,
            )
            .unwrap();
        assert_eq!(out.len(), 1, "the second datagram's severity-less message must be dropped");
    }

    /// A value list and a notification under one shared identity, as a `threshold` plugin
    /// produces alongside `load` reads on the same host.
    #[test]
    fn a_datagram_mixing_a_value_list_and_a_notification_decodes_both() {
        let events = decode(
            PacketBuilder::new()
                .string(part::TYPE_HOST, b"web-1")
                .number(part::TYPE_TIME_HR, TIME_HR)
                .string(part::TYPE_PLUGIN, b"load")
                .string(part::TYPE_TYPE, b"load")
                .values(&[gauge(0.1), gauge(0.2), gauge(0.3)])
                .number(part::TYPE_SEVERITY, 2)
                .string(part::TYPE_MESSAGE, b"load average high")
                .build(),
        );
        assert_eq!(events.len(), 2);
        assert!(events[0].log.is_none() && !events[0].metrics.is_empty(), "the value list first");
        assert!(events[1].log.is_some() && events[1].metrics.is_empty(), "the notification second");
        assert_eq!(attr(&events[1], ATTR_HOST), Some(Value::from("web-1")));
        assert_eq!(attr(&events[1], ATTR_PLUGIN), Some(Value::from("load")));
        assert_eq!(attr(&events[1], ATTR_TYPE), Some(Value::from("load")));
    }

    // --- skips and faults ----------------------------------------------------------------------

    #[test]
    fn a_list_with_no_host_plugin_or_type_is_skipped_and_counted() {
        for bytes in [
            // No host at all.
            PacketBuilder::new()
                .string(part::TYPE_PLUGIN, b"p")
                .string(part::TYPE_TYPE, b"t")
                .values(&[gauge(1.0)])
                .build(),
            // An explicitly *empty* host, which is how a sender clears one.
            PacketBuilder::new()
                .string(part::TYPE_HOST, b"")
                .string(part::TYPE_PLUGIN, b"p")
                .string(part::TYPE_TYPE, b"t")
                .values(&[gauge(1.0)])
                .build(),
            // No type.
            PacketBuilder::new()
                .string(part::TYPE_HOST, b"h")
                .string(part::TYPE_PLUGIN, b"p")
                .values(&[gauge(1.0)])
                .build(),
        ] {
            let (mut decoder, registry) = decoder_with_diag();
            let mut out = Vec::new();
            decoder.decode_into(bytes, RECEIVED_AT, &mut out).expect("a skip is not an error");
            assert!(out.is_empty());
            assert!(diagnosed(&registry, "incomplete_identity"));
        }
    }

    /// With nothing decoded yet, every malformed shape fails the whole datagram (`bad_datagram`).
    #[test]
    fn every_malformed_part_shape_fails_the_datagram_when_nothing_decoded_yet() {
        let cases: Vec<(&str, Bytes)> = vec![
            ("len below the header", PacketBuilder::new().header(part::TYPE_HOST, 0).build()),
            (
                "len past the end of the datagram",
                PacketBuilder::new().header(part::TYPE_HOST, 400).raw(b"short").build(),
            ),
            ("a trailing fragment", PacketBuilder::new().raw(&[0x00, 0x00]).build()),
            (
                "an unterminated string",
                PacketBuilder::new().unterminated_string(part::TYPE_HOST, b"web-1").build(),
            ),
            (
                "a string part with no payload at all",
                PacketBuilder::new().header(part::TYPE_HOST, 4).build(),
            ),
            (
                "a 4-byte numeric part",
                PacketBuilder::new().part(part::TYPE_TIME_HR, &[0, 0, 0, 1]).build(),
            ),
            (
                "a values part with no count",
                PacketBuilder::new().part(part::TYPE_VALUES, &[0]).build(),
            ),
            (
                "a values count of zero",
                PacketBuilder::new()
                    .string(part::TYPE_HOST, b"h")
                    .string(part::TYPE_PLUGIN, b"p")
                    .string(part::TYPE_TYPE, b"t")
                    .values(&[])
                    .build(),
            ),
            (
                "a values count past the cap",
                PacketBuilder::new()
                    .string(part::TYPE_HOST, b"h")
                    .string(part::TYPE_PLUGIN, b"p")
                    .string(part::TYPE_TYPE, b"t")
                    .values(&vec![gauge(1.0); MAX_VALUES_PER_LIST + 1])
                    .build(),
            ),
            (
                "a values length disagreeing with its count",
                PacketBuilder::new().values_with_declared_count(4, &[part::DS_GAUGE; 4]).build(),
            ),
            (
                "an unknown data-source type",
                PacketBuilder::new()
                    .string(part::TYPE_HOST, b"h")
                    .string(part::TYPE_PLUGIN, b"p")
                    .string(part::TYPE_TYPE, b"t")
                    .values(&[(99, [0; 8])])
                    .build(),
            ),
        ];
        for (label, bytes) in cases {
            let mut out = Vec::new();
            let result = decoder().decode_into(bytes, RECEIVED_AT, &mut out);
            assert!(matches!(result, Err(CodecError::Malformed(_))), "{label} must be Malformed");
            assert!(out.is_empty(), "{label} pushed an event");
        }
    }

    /// A malformed part *behind* a valid list keeps the list and reports `bad_part`.
    #[test]
    fn a_malformed_part_behind_a_valid_list_keeps_the_list_and_reports_bad_part() {
        let prefix = PacketBuilder::new()
            .string(part::TYPE_HOST, b"h")
            .string(part::TYPE_PLUGIN, b"p")
            .string(part::TYPE_TYPE, b"t")
            .values(&[gauge(1.0)]);
        let bytes = prefix.unterminated_string(part::TYPE_TYPE_INSTANCE, b"broken").build();

        let (mut decoder, registry) = decoder_with_diag();
        let mut out = Vec::new();
        decoder.decode_into(bytes, RECEIVED_AT, &mut out).expect("earlier lists survive");
        assert_eq!(out.len(), 1);
        assert!(diagnosed(&registry, "bad_part"));
    }

    /// A malformed part **abandons the rest of the datagram**; decoding never resumes past it.
    #[test]
    fn a_malformed_part_abandons_the_rest_of_the_datagram_rather_than_resuming_past_it() {
        let (mut decoder, registry) = decoder_with_diag();
        let bytes = PacketBuilder::new()
            .string(part::TYPE_HOST, b"h")
            .string(part::TYPE_PLUGIN, b"p")
            .string(part::TYPE_TYPE, b"t")
            .values(&[gauge(1.0)])
            .unterminated_string(part::TYPE_TYPE_INSTANCE, b"broken")
            // Structurally valid, but unreachable: a length-prefixed stream has no resync point.
            .string(part::TYPE_TYPE_INSTANCE, b"fine")
            .values(&[gauge(2.0)])
            .build();
        let mut out = Vec::new();
        decoder.decode_into(bytes, RECEIVED_AT, &mut out).expect("earlier lists survive");
        assert_eq!(out.len(), 1, "only the list before the malformed part is decoded");
        assert_eq!(out[0].metrics[0].kind, MetricKind::Gauge(1.0));
        assert!(diagnosed(&registry, "bad_part"));
    }

    /// An inflated `count` is rejected on the length check, before anything is sized from it
    /// (`crates/logit-proto/tests/robustness.rs` measures the allocation side).
    #[test]
    fn an_inflated_values_count_is_rejected_without_reading_a_value() {
        let bytes = PacketBuilder::new().values_with_declared_count(65535, b"tiny").build();
        let mut out = Vec::new();
        assert!(decoder().decode_into(bytes, RECEIVED_AT, &mut out).is_err());
    }

    #[test]
    fn an_empty_datagram_decodes_to_nothing_without_failing() {
        assert!(decode(Bytes::new()).is_empty());
    }

    #[test]
    fn the_resource_is_the_decoders_own_shared_one_and_the_scope_is_always_none() {
        let resource = Arc::new(Resource::default());
        let mut decoder = CollectdDecoder::new(resource.clone());
        let mut out = Vec::new();
        let (decoded, scope) =
            decoder.decode_into(single_gauge_packet(), RECEIVED_AT, &mut out).unwrap();
        assert!(Arc::ptr_eq(&decoded, &resource), "a per-datagram resource would split batches");
        assert!(scope.is_none());
    }

    #[test]
    fn with_diagnostics_reaches_the_decoders_own_handle() {
        let decoder = decoder().with_diagnostics(Diagnostics::new("collectd_in/a"));
        assert_eq!(decoder.diag().component_id(), "collectd_in/a");
    }

    // --- types.db naming -----------------------------------------------------------------------

    fn types_db() -> Arc<TypesDb> {
        Arc::new(TypesDb::parse(super::super::types_db::TEST_TYPES_DB).expect("fixture parses"))
    }

    /// A packet carrying one list of `values` under `plugin`/`type`.
    fn list_packet(plugin: &[u8], type_: &[u8], values: &[(u8, [u8; 8])]) -> Bytes {
        PacketBuilder::new()
            .string(part::TYPE_HOST, b"web-1")
            .string(part::TYPE_PLUGIN, plugin)
            .string(part::TYPE_TYPE, type_)
            .values(values)
            .build()
    }

    fn names_with_types_db(bytes: Bytes) -> (Vec<String>, Arc<Registry>) {
        let (decoder, registry) = decoder_with_diag();
        let mut decoder = decoder.with_types_db(types_db());
        let mut out = Vec::new();
        decoder.decode_into(bytes, RECEIVED_AT, &mut out).expect("decode must succeed");
        let names = out[0]
            .metrics
            .iter()
            .map(|record| resolve(record.name).to_string())
            .collect::<Vec<_>>();
        (names, registry)
    }

    /// A type resolving with matching count and kinds names each record after its data source.
    #[test]
    fn a_resolved_multi_data_source_type_names_its_records_after_its_data_sources() {
        let (names, registry) = names_with_types_db(list_packet(
            b"load",
            b"load",
            &[gauge(0.1), gauge(0.2), gauge(0.3)],
        ));
        assert_eq!(names, vec!["load.load.shortterm", "load.load.midterm", "load.load.longterm"]);
        assert!(!diagnosed(&registry, "types_db_mismatch"), "a clean match must be silent");
    }

    /// A resolved **single**-data-source type omits the lone name (`value`), as index naming does.
    #[test]
    fn a_resolved_single_data_source_type_omits_the_lone_data_source_name() {
        let (names, _) = names_with_types_db(list_packet(b"cpu", b"cpu", &[derive(7)]));
        assert_eq!(names, vec!["cpu.cpu"], "not `cpu.cpu.value`");
    }

    /// A count or kind mismatch falls back to index naming and reports `types_db_mismatch`.
    #[test]
    fn a_types_db_mismatch_falls_back_to_index_naming_and_reports_it() {
        // Right type, wrong data-source count: the fixture's `load` has three.
        let (names, registry) =
            names_with_types_db(list_packet(b"load", b"load", &[gauge(0.1), gauge(0.2)]));
        assert_eq!(names, vec!["load.load.0", "load.load.1"]);
        assert!(diagnosed(&registry, "types_db_mismatch"));

        // Right count, wrong kinds: the fixture's `if_octets` is two DERIVEs, not two GAUGEs.
        let (names, registry) =
            names_with_types_db(list_packet(b"interface", b"if_octets", &[gauge(1.0), gauge(2.0)]));
        assert_eq!(names, vec!["interface.if_octets.0", "interface.if_octets.1"]);
        assert!(diagnosed(&registry, "types_db_mismatch"));
    }

    /// An undefined type is routine: index naming, **no** diagnostic.
    #[test]
    fn an_unresolved_type_falls_back_to_index_naming_without_a_diagnostic() {
        let (names, registry) =
            names_with_types_db(list_packet(b"custom", b"custom_type", &[gauge(1.0), gauge(2.0)]));
        assert_eq!(names, vec!["custom.custom_type.0", "custom.custom_type.1"]);
        assert!(!diagnosed(&registry, "types_db_mismatch"), "an unknown type is not a mismatch");
    }

    /// A non-UTF-8 type name takes the quiet unresolved path, not the mismatch one.
    #[test]
    fn a_non_utf8_type_name_falls_back_quietly() {
        let (names, registry) =
            names_with_types_db(list_packet(b"p", &[0xFF, 0xFE], &[gauge(1.0), gauge(2.0)]));
        assert_eq!(names.len(), 2);
        assert!(names[0].ends_with(".0"));
        assert!(!diagnosed(&registry, "types_db_mismatch"));
    }

    /// With no `types.db`, the same packet is index-named with no diagnostic.
    #[test]
    fn no_types_db_configured_means_index_naming_and_no_diagnostic() {
        let (mut decoder, registry) = decoder_with_diag();
        let mut out = Vec::new();
        decoder
            .decode_into(
                list_packet(b"load", b"load", &[gauge(0.1), gauge(0.2), gauge(0.3)]),
                RECEIVED_AT,
                &mut out,
            )
            .unwrap();
        let names: Vec<&str> = out[0].metrics.iter().map(|r| resolve(r.name)).collect();
        assert_eq!(names, vec!["load.load.0", "load.load.1", "load.load.2"]);
        assert!(!diagnosed(&registry, "types_db_mismatch"));
    }

    #[test]
    fn a_counter_at_the_top_of_the_u64_range_decodes_as_a_lossy_but_finite_f64() {
        let events = decode(
            PacketBuilder::new()
                .string(part::TYPE_HOST, b"h")
                .string(part::TYPE_PLUGIN, b"p")
                .string(part::TYPE_TYPE, b"t")
                .values(&[counter(u64::MAX), absolute(u64::MAX)])
                .build(),
        );
        // Precision loss above 2^53 is a known gap (`docs/known-gaps.md`), not a failure.
        assert_eq!(
            events[0].metrics[0].kind,
            MetricKind::Sum(Sum {
                value: u64::MAX as f64,
                temporality: Temporality::Cumulative,
                monotonic: true
            })
        );
        assert_eq!(
            events[0].metrics[1].kind,
            MetricKind::Sum(Sum {
                value: u64::MAX as f64,
                temporality: Temporality::Delta,
                monotonic: true
            })
        );
    }
}
