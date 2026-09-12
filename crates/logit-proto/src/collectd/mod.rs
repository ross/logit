//! The collectd binary protocol ("`network` plugin") codec, both directions: [`CollectdDecoder`]
//! turns one UDP datagram into events, [`CollectdEncoder`] packs a batch of events back into
//! datagrams. The part framing itself lives next door in [`part`]; nothing in [`decode`] or
//! [`encode`] knows a byte offset or a byte order.
//!
//! **This module doc is the mapping table** (house convention, see [`crate::prometheus`]'s and
//! [`crate::otlp`]'s module docs). [ADR `collectd-binary-relay`](../../../../docs/adr/collectd-binary-relay.md)
//! is the decision record; [`docs/plans/collectd-binary-relay.md`](../../../../docs/plans/collectd-binary-relay.md)
//! is the workstream plan.
//!
//! [`CollectdEncoder`] **implements [`crate::FramedEncoder`], not [`crate::Encoder`]** (ADR
//! `framed-encoder`), for the reason `statsd_out`/`syslog_out`/`prometheus_out`'s encoders do too
//! (`crates/logit-outputs/src/statsd.rs`'s module doc): `Encoder` is `fn encode(&mut self,
//! &EventBatch) -> Result<Bytes, _>` -- one opaque buffer per batch, with no framing metadata --
//! and collectd egress genuinely needs per-*datagram* boundaries, since the receiver resets its
//! sticky identity state at every datagram edge and a `max_packet_bytes` cap decides where those
//! edges fall. [`CollectdEncoder::encode_into`] fills a [`crate::MessageBuf`]`<usize>` instead: one
//! entry per datagram, whose `usize` meta is the value-list count that datagram carries (what
//! `collectd_out` needs to attribute an `EMSGSIZE` drop to the right number of metrics). The
//! decode direction has no such problem -- one datagram in, N events out -- so [`CollectdDecoder`]
//! is an ordinary [`crate::Decoder`].
//!
//! ## Wire shape, in one paragraph
//!
//! A datagram is a flat sequence of parts (`type u16 BE, len u16 BE`, `len` including the header).
//! The five identity parts -- Host, Plugin, PluginInstance, Type, TypeInstance -- plus Time and
//! Interval are **sticky**: each one sets a field that stays set until overwritten, and every Values
//! part dispatches one *value list* against whatever identity is currently in force. That is the
//! protocol's compression scheme: a sender writes an identity part only when it differs from the
//! last one it wrote *in that datagram*, so 25 lists from one host share a single Host part. The
//! sticky state resets at every datagram boundary, which is why the encoder's packing loop has to
//! re-encode a list that lands at the start of a fresh packet (see [`encode`]).
//!
//! ## Decode: bytes → events
//!
//! One [`logit_core::Event`] per Values part, its `metrics` carrying one
//! [`logit_core::MetricRecord`] per data source **in wire order** -- a 3-data-source `load` list is
//! one event with three records, not three events, which is what makes it re-encodable as the same
//! single list (`logit_core::MetricList` is a `SmallVec` inlined at 1, so the common
//! single-data-source list costs no allocation).
//!
//! | Wire | Model | Counter / diag |
//! |---|---|---|
//! | Values part, N values | one `Event`, `metrics` = N records in wire order | -- |
//! | COUNTER `u64` | `Sum { value: v as f64, Cumulative, monotonic: true }` | none (above 2⁵³ the `f64` loses precision -- `docs/known-gaps.md`, shared with OTLP's int/double collapse) |
//! | DERIVE `i64` | `Sum { Cumulative, monotonic: false }` | none |
//! | ABSOLUTE `u64` | `Sum { Delta, monotonic: true }` | none |
//! | GAUGE finite/±inf | `Gauge(v)` | none |
//! | GAUGE NaN | `Gauge(0.0)` + [`logit_core::MetricRecord::FLAG_NO_RECORDED_VALUE`] | none -- NaN *is* collectd's "no value for this interval," and a flagged point is exactly the model's word for that. Carrying the NaN through instead would make every `PartialEq` fixed-point test a lie (`NaN != NaN`). |
//! | Host/Plugin/PluginInstance/Type/TypeInstance | sticky → the `collectd.*` attributes below; an **empty** string part clears the field, so the attribute is absent | -- |
//! | Values with an empty host, plugin or type | list skipped, nothing pushed | `incomplete_identity` (collectd's own receiver rejects the same list with `-EINVAL`) |
//! | TimeHR (2⁻³⁰ s) / Time (s) / neither | `Event::timestamp` = [`cdtime_to_nanos`] / `s * 1e9` / `received_at` | -- (collectd rejects a `time == 0` list; this codec observes rather than rejects, so a timeless list is stamped with receipt time like every other `logit` input) |
//! | IntervalHR / Interval | `collectd.interval` = `F64` seconds (`cdtime / 2³⁰`, exact); `0` → absent | -- |
//! | record name, no `types_db` configured or the list's type not in it | `<plugin>.<type>` for a one-data-source list, `<plugin>.<type>.<i>` (0-based) otherwise | -- (a type missing from `types.db` is routine, not a misconfiguration) |
//! | record name, the list's type resolved in [`types_db`] with a matching data-source count **and** kinds | `<plugin>.<type>` for a one-data-source type (the lone data source, conventionally `value`, is omitted -- collectd's own `write_graphite` default), `<plugin>.<type>.<ds_name>` otherwise | -- |
//! | record name, the type resolved but its count or kinds disagree with the wire | index naming, as above | `types_db_mismatch` -- the configured file is not the one the sender is running against, and naming from it would label a real measurement wrongly |
//! | a part whose `len` is `< 4`, runs past the datagram, a string part with no NUL terminator, a numeric part not 12 bytes, a Values part where `len != 6 + 9 * count`, `count == 0`, `count > `[`MAX_VALUES_PER_LIST`], or an unknown data-source type byte | the rest of the datagram is abandoned; events already decoded from it are **kept** | `bad_part` when something was already decoded, else `CodecError::Malformed` (the listener's own `bad_datagram`) |
//! | `0x0200` Signature | skipped by length, **unverified** | -- (`docs/known-gaps.md`) |
//! | `0x0210` Encryption | the rest of the datagram is dropped | `encrypted_packet_dropped` |
//! | `0x0100` Message / `0x0101` Severity | skipped by length until W5 | -- |
//! | any other part type | skipped by length | -- |
//!
//! The decoded [`logit_core::Resource`] is always the decoder's own, shared, usually-default one and
//! the [`logit_core::Scope`] is always `None`: a per-host resource would look tidier but
//! `logit_pipeline::BatchAccumulator::absorb` keys accumulation on `Arc::ptr_eq`, so minting one per
//! datagram would split every batch by sender. The host rides on `collectd.host` instead.
//!
//! **Record names are display/cross-protocol only.** `collectd_out` re-encodes a list from the
//! `collectd.*` attributes, the `MetricList`'s order and each record's kind, and never reads the
//! name -- so a pipeline with no `types_db:`, one with a stale file, and one with the sender's own
//! file all relay the same bytes. What the names change is what an InfluxDB/Prometheus/statsd sink
//! calls the series, which is the whole reason to configure [`types_db`] at all.
//!
//! ## Encode: events → datagrams
//!
//! | Model | Wire | Counter |
//! |---|---|---|
//! | event with `collectd.type` present (merged: event attributes, then resource) | **like-relay**: one Values part carrying every record of `event.metrics` in order, identity straight off the `collectd.*` attributes | -- |
//! | event without `collectd.type` | **fallback**: one single-data-source list per record -- plugin = the record name up to its first `.` (the whole name if it has none), type_instance = the remainder, type = `counter`/`gauge`/`derive`/`absolute` by kind (all single-data-source types in a stock `types.db`), no plugin_instance | -- |
//! | `Sum{Cumulative, monotonic}`, integral, in `u64` range | COUNTER | -- |
//! | `Sum{Cumulative, !monotonic}`, integral, in `i64` range | DERIVE | -- |
//! | `Sum{Delta, monotonic}`, integral, in `u64` range | ABSOLUTE | -- |
//! | `Sum{Delta, !monotonic}` | dropped | `logit.output.metrics.skipped{metric_kind="non_monotonic_delta_sum"}` |
//! | any `Sum` non-integral, out of range, or non-finite | dropped | `logit.output.metrics.skipped{reason="unencodable_value"}` + diag `unencodable_value`. Rounding would fabricate: statsd's `page.views:2\|c\|@0.3` reaches a sink as `6.666…`, and `6` or `7` is a number nobody sent. |
//! | `Gauge(v)`, finite or ±inf | GAUGE | -- |
//! | `Gauge` flagged `NO_RECORDED_VALUE` | GAUGE NaN -- the inverse of the decode row above | -- |
//! | any non-`Gauge` flagged `NO_RECORDED_VALUE` | dropped | `logit.output.metrics.skipped{reason="no_recorded_value"}` + diag `no_recorded_value` |
//! | `GaugeDelta` | dropped | `logit.output.metrics.skipped{metric_kind="gauge_delta"}` + diag `gauge_delta_unresolved` (the same greppable key every other sink uses -- it means a missing `aggregate`, not a malformed metric) |
//! | `Samples`, `Distribution`, `SetMembers`, `Set`, `Histogram`, `ExponentialHistogram`, `Summary` | dropped, one exhaustive `match` arm each, no wildcard | `logit.output.metrics.skipped{metric_kind="samples"\|"distribution"\|"set_members"\|"set"\|"histogram"\|"exponential_histogram"\|"summary"}` |
//! | a **like-relay** list where any one record fails the rows above | the whole list is dropped, counted once for the first failing reason | as above -- collectd's receiver rejects a list whose value count disagrees with its type's `ds_num` anyway, so emitting the survivors would be worse than dropping |
//! | `Event::timestamp > 0` | TimeHR = [`nanos_to_cdtime`] | -- |
//! | `Event::timestamp <= 0` | the list is dropped | `logit.output.metrics.skipped{reason="unencodable_timestamp"}` |
//! | `collectd.interval` `F64`, finite, `> 0` | IntervalHR = `round(v * 2³⁰)` | -- |
//! | `collectd.interval` absent | IntervalHR `0` (collectd's own "unspecified") | -- |
//! | `collectd.interval` present but not a finite positive `F64` | IntervalHR `0` | `logit.output.tags.dropped{reason="unrepresentable"}` |
//! | host | `collectd.host`, else `host.name`, else the encoder's [`CollectdEncoder::with_hostname`] value -- the first that survives sanitizing non-empty | -- |
//! | none of those three present | the whole event is dropped | `logit.output.metrics.skipped{reason="no_host"}` + diag `no_host`. collectd's receiver rejects an empty host, and there is no honest substitute: this codec neither reads the OS hostname (deferred work, `docs/known-gaps.md`) nor invents a placeholder, since one made-up name would silently merge every unlabelled sender into a single host's metrics. |
//! | `collectd.*` identity, `Str` or `Bytes` | verbatim after sanitizing; a `Bytes` is byte-verbatim (collectd's own strings are bytes, not UTF-8) | `logit.output.identity.sanitized{reason="substituted"\|"truncated"}` |
//! | `collectd.*` identity of any other `Value` type | treated as absent | `logit.output.tags.dropped{reason="unrepresentable"}` |
//! | every attribute outside the `collectd.` namespace | dropped -- collectd has no tag concept at all, and `host.name` is counted here too even though the host resolution above reads it | `logit.output.tags.dropped{reason="no_wire_form"}`, once per attribute per event |
//! | plugin or type empty after sanitizing | the list is dropped | `logit.output.metrics.skipped{reason="empty_name"}` |
//! | a like-relay event carrying more than [`MAX_VALUES_PER_LIST`] records | the list is dropped whole | `logit.output.metrics.skipped{reason="too_many_values"}` + diag `too_many_values`. The cap is pair-wide: a longer list fits comfortably under the byte cap, but the decode side of this very codec rejects it as a malformed part -- and that abandons every unrelated list packed behind it in the same datagram. `aggregate`/`kv_metrics` can both put far more than 64 records on one event. |
//! | a list that alone exceeds `max_packet_bytes` | dropped whole, never split | `logit.output.metrics.skipped{reason="oversize_value_list"}` + diag `oversize_value_list` |
//! | an event with no metrics at all (a log- or span-only event) | skipped | [`EncodeStats::skipped_no_metrics`] (no counter of its own -- nothing was lost, there was nothing to send) |
//! | `MetricRecord`'s `unit`, `description`, `start_timestamp`, `exemplars`; `EventBatch::scope`; `Resource::schema_url` | dropped | none; `docs/known-gaps.md` rows -- the protocol has no field for any of them |
//!
//! ## Well-known attributes (`collectd.*`)
//!
//! Rule (b) of [ADR `lossless-transit`](../../../../docs/adr/lossless-transit.md): the raw,
//! protocol-native fact rides alongside the normalized model field and wins on the way back out.
//! These are **event** attributes, never resource ones (the `Arc::ptr_eq` accumulator keying above,
//! and `syslog.hostname`'s precedent). Every one of them is *consumed* by `collectd_out` -- read for
//! its own purpose, never re-emitted as something else -- and appears as an ordinary tag at every
//! other sink. `docs/design/data-model.md`'s well-known-attribute table is the canonical list.
//!
//! | Attribute | Value | Meaning |
//! |---|---|---|
//! | [`ATTR_HOST`] | `Str` (`Bytes` when not UTF-8) | the wire Host; absent when the wire carried an empty one; outranks `host.name` on collectd egress |
//! | [`ATTR_PLUGIN`] | `Str`/`Bytes` | the wire Plugin; required for like-relay encoding |
//! | [`ATTR_PLUGIN_INSTANCE`] | `Str`/`Bytes` | the wire PluginInstance, only when non-empty |
//! | [`ATTR_TYPE`] | `Str`/`Bytes` | the wire Type; **its presence is what selects like-relay encoding** |
//! | [`ATTR_TYPE_INSTANCE`] | `Str`/`Bytes` | the wire TypeInstance, only when non-empty |
//! | [`ATTR_INTERVAL`] | `F64` seconds | `cdtime / 2³⁰`, exact; absent when the wire carried no interval or a zero one |
//! | [`ATTR_SEVERITY`] | `U64` ∈ {1, 2, 4} | **reserved for notifications (W5)**; raw wire severity, marking the event as a notification rather than a value list |
//!
//! ## Sanitization
//!
//! NUL and `/` become `_` (collectd's own `escape_slashes` does the latter; the former would
//! truncate the string part it sits in), and every identity field is truncated to 127 bytes --
//! [`DATA_MAX_NAME_LEN`] minus the NUL, the point at which collectd's `parse_part_string` rejects
//! the whole packet. A `Str` truncates on a UTF-8 character boundary, a `Bytes` on a byte boundary.
//! Both are counted (`logit.output.identity.sanitized{reason}`). Nothing else is substituted:
//! whitespace and control bytes ride through collectd untouched, and sanitizing them would break the
//! fixed point for no wire-level reason.
//!
//! ## Permitted normalizations
//!
//! `collectd_in -> collectd_out` is a fixed point modulo exactly this list (the round-trip tests in
//! `crates/logit-proto/tests/collectd_fixed_point.rs` pin it):
//!
//! 1. legacy `Time`/`Interval` (second resolution) re-emit as their HR (2⁻³⁰ s) counterparts;
//! 2. a TimeHR may move by at most one tick (2⁻³⁰ s ≈ 0.93 ns) on the first hop, and is stable
//!    afterward -- `cdtime → ns → cdtime` is not quite the identity, `ns → cdtime → ns` is;
//! 3. string-part elision is recomputed per *output* datagram, and datagram boundaries are re-chosen
//!    by the sink's own `max_packet_bytes` -- an input packet carrying 25 lists may leave as two
//!    packets, and vice versa;
//! 4. value lists may be reordered within a batch (everything downstream of a batch boundary is
//!    order-insensitive by design);
//! 5. unknown part types and Signature parts are dropped;
//! 6. a NaN gauge's payload bits canonicalize (NaN is carried as the `NO_RECORDED_VALUE` flag, and
//!    re-emitted as whatever `f64::NAN` is on this platform);
//! 7. a list that arrived with no Time part at all leaves carrying `TimeHR = received_at`;
//! 8. `/` and NUL become `_`, and an identity field longer than 127 bytes is truncated (both
//!    counted);
//! 9. an absent interval leaves as `IntervalHR 0`.
//!
//! Everything else is an error or a counted drop, never a silent reinterpretation.

pub mod decode;
pub mod encode;
pub mod part;
pub mod types_db;

pub use decode::CollectdDecoder;
pub use encode::{CollectdEncoder, EncodeStats};
pub use types_db::{DataSource, DsKind, TypesDb, TypesDbError};

/// collectd's own default `network` plugin port, for both the listener and the sink.
pub const DEFAULT_PORT: u16 = 25826;

/// collectd's own `MaxPacketSize` default: 1452 bytes, i.e. a 1500-byte Ethernet MTU minus the
/// IPv4 and UDP headers minus a little headroom. Bounds one **datagram** (several packed value
/// lists), not one list. collectd itself accepts `1024..=65535` for this setting.
pub const DEFAULT_MAX_PACKET_BYTES: usize = 1452;

/// collectd's `DATA_MAX_NAME_LEN` (`plugin.h`): the buffer an identity string is parsed into,
/// including its NUL terminator -- so 127 usable bytes. `parse_part_string` rejects the entire
/// packet when a string does not fit, which is why the encoder truncates rather than trusting a
/// peer to cope.
pub const DATA_MAX_NAME_LEN: usize = 128;

/// collectd's `NOTIF_MAX_MSG_LEN` (`plugin.h`), including the NUL -- so 255 usable bytes. Unused
/// until notifications land in W5; defined here so both halves of that workstream share one
/// constant.
pub const NOTIF_MAX_MSG_LEN: usize = 256;

/// The most data sources this codec will read or write in one Values part. collectd's own wire
/// format allows up to `(65535 - 6) / 9 = 7281`, but nothing real comes close (`load` has 3,
/// `if_octets` 2, `disk_io_time` 2).
///
/// A **pair-wide** cap, not a decode-side one. Over it, a Values part is malformed on decode (the
/// rest of the datagram is abandoned, not truncated -- see this module's decode table), and on
/// encode the list is dropped whole and counted
/// `logit.output.metrics.skipped{reason="too_many_values"}`: a longer list would sail under the
/// byte cap only for a receiver running this same codec to reject it, taking every unrelated list
/// packed behind it in that datagram with it.
///
/// What it bounds is the decoder's per-part work and the per-list record-name suffix fan-out
/// (`<plugin>.<type>.<i>`). It is deliberately **not** a bound on interner growth: the unbounded
/// axis there is distinct `<plugin>`/`<type>` strings, which the decoder caps in neither count nor
/// length, and a fresh Plugin part plus a one-value list mints a new interned name for ~21 wire
/// bytes whatever this constant is. That exposure is exactly the one `statsd_in` already has --
/// wire-chosen metric names, accepted on `docs/design/memory.md` §4's "listeners are private"
/// premise.
pub const MAX_VALUES_PER_LIST: usize = 64;

/// The wire Host. See this module doc's well-known-attribute table.
pub const ATTR_HOST: &str = "collectd.host";
/// The wire Plugin.
pub const ATTR_PLUGIN: &str = "collectd.plugin";
/// The wire PluginInstance, present only when non-empty.
pub const ATTR_PLUGIN_INSTANCE: &str = "collectd.plugin_instance";
/// The wire Type. Its **presence** is what makes `collectd_out` re-emit an event as a like-relay
/// value list rather than through the fallback naming path.
pub const ATTR_TYPE: &str = "collectd.type";
/// The wire TypeInstance, present only when non-empty.
pub const ATTR_TYPE_INSTANCE: &str = "collectd.type_instance";
/// The wire Interval, as `Value::F64` seconds (`cdtime / 2³⁰`, exact).
pub const ATTR_INTERVAL: &str = "collectd.interval";
/// The raw wire severity of a *notification* (`1` FAILURE, `2` WARNING, `4` OKAY), as
/// `Value::U64`. **Reserved for W5** -- nothing reads or writes it yet; it is named here so the
/// attribute namespace is decided in one place rather than invented twice.
pub const ATTR_SEVERITY: &str = "collectd.severity";

/// The prefix every attribute above shares. The encoder skips the whole namespace when it decides
/// what has no wire form, rather than matching the seven names individually, so a later addition is
/// automatically not leaked onto the wire as something it isn't.
pub const ATTR_PREFIX: &str = "collectd.";

/// One second in collectd's `cdtime_t` units: 2⁻³⁰ s ticks, i.e. `1 << 30` ticks per second.
pub(crate) const CDTIME_ONE_SECOND: u64 = 1 << 30;

/// A `cdtime_t` (2⁻³⁰-second ticks since the epoch, collectd's `utils_time.h`) as Unix nanoseconds.
///
/// Deliberately collectd's **own split arithmetic** -- whole seconds and sub-second ticks converted
/// separately, the sub-second half rounded half-up -- rather than the obvious
/// `t * 1e9 / 2^30`: the latter overflows `u64` above ~18 seconds since the epoch, and doing it in
/// `u128` instead would give a *different* answer at the rounding boundary than collectd itself
/// produces, which is exactly the kind of one-tick disagreement that turns a fixed-point test
/// flaky. Saturating at [`i64::MAX`] rather than wrapping: a `cdtime` past year 2554 is nonsense,
/// and a nonsense timestamp must not become a negative one.
pub fn cdtime_to_nanos(cdtime: u64) -> i64 {
    let seconds = cdtime >> 30;
    let ticks = cdtime & (CDTIME_ONE_SECOND - 1);
    // `ticks < 2^30`, so `ticks * 1e9 < 1.08e18` -- comfortably inside `u64`, no saturation needed
    // on this half.
    let sub_nanos = (ticks * 1_000_000_000 + (CDTIME_ONE_SECOND / 2)) >> 30;
    let nanos = seconds.saturating_mul(1_000_000_000).saturating_add(sub_nanos);
    i64::try_from(nanos).unwrap_or(i64::MAX)
}

/// Unix nanoseconds as a `cdtime_t` -- the inverse of [`cdtime_to_nanos`], and exact in this
/// direction (`ns → cdtime → ns` round-trips; the other way may move one tick once).
///
/// A non-positive `ns` yields `0`, collectd's own "no time given": there is no cdtime before the
/// epoch, and `0` is precisely what its receiver reads as unset.
pub fn nanos_to_cdtime(ns: i64) -> u64 {
    if ns <= 0 {
        return 0;
    }
    let ns = ns as u64;
    let seconds = ns / 1_000_000_000;
    let remainder = ns % 1_000_000_000;
    // Same split-arithmetic reasoning as `cdtime_to_nanos`: `remainder < 1e9`, so
    // `remainder << 30 < 1.08e18` and the shift cannot overflow.
    let ticks = ((remainder << 30) + 500_000_000) / 1_000_000_000;
    seconds.saturating_mul(CDTIME_ONE_SECOND).saturating_add(ticks)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The table from `docs/plans/collectd-binary-relay.md`'s wire-facts section, plus the two
    /// saturation edges. `2^30 - 1` ticks is 999_999_999 ns (not 1e9) and `1` tick is 1 ns after
    /// rounding -- the two values a naive `t * 1e9 >> 30` gets subtly wrong.
    #[test]
    fn cdtime_to_nanos_matches_collectds_own_split_arithmetic() {
        let cases: &[(u64, i64)] = &[
            (0, 0),
            (1, 1),
            (CDTIME_ONE_SECOND - 1, 999_999_999),
            (CDTIME_ONE_SECOND, 1_000_000_000),
            (CDTIME_ONE_SECOND + 1, 1_000_000_001),
            (1_700_000_000 << 30, 1_700_000_000_000_000_000),
        ];
        for &(cdtime, nanos) in cases {
            assert_eq!(cdtime_to_nanos(cdtime), nanos, "cdtime {cdtime}");
        }
    }

    /// `u64::MAX` cdtime is year ~2554 in seconds; `seconds * 1e9` overflows `i64`, so it must
    /// saturate rather than wrap into a negative timestamp.
    #[test]
    fn cdtime_to_nanos_saturates_instead_of_wrapping_negative() {
        assert_eq!(cdtime_to_nanos(u64::MAX), i64::MAX);
    }

    #[test]
    fn nanos_to_cdtime_is_the_exact_inverse() {
        let cases: &[i64] = &[
            1,
            999_999_999,
            1_000_000_000,
            1_000_000_001,
            1_700_000_000_000_000_000,
            1_700_000_000_123_456_789,
        ];
        for &nanos in cases {
            assert_eq!(cdtime_to_nanos(nanos_to_cdtime(nanos)), nanos, "nanos {nanos}");
        }
    }

    #[test]
    fn nanos_to_cdtime_maps_a_whole_second_onto_a_whole_tick_count() {
        assert_eq!(nanos_to_cdtime(1_000_000_000), CDTIME_ONE_SECOND);
        assert_eq!(nanos_to_cdtime(1_700_000_000_000_000_000), 1_700_000_000 << 30);
    }

    /// Zero and every negative instant collapse to collectd's own "unset" -- there is no cdtime
    /// before the epoch to be faithful to.
    #[test]
    fn nanos_to_cdtime_clamps_non_positive_instants_to_zero() {
        assert_eq!(nanos_to_cdtime(0), 0);
        assert_eq!(nanos_to_cdtime(-1), 0);
        assert_eq!(nanos_to_cdtime(i64::MIN), 0);
    }

    /// The one-hop tolerance the ADR's normalization list names: `cdtime → ns → cdtime` may move by
    /// a single tick, and must then be stable.
    #[test]
    fn cdtime_round_trips_within_one_tick_and_is_then_stable() {
        for cdtime in [1u64, 12345, CDTIME_ONE_SECOND - 1, (1_700_000_000 << 30) | 0x1234_5678] {
            let once = nanos_to_cdtime(cdtime_to_nanos(cdtime));
            assert!(
                once.abs_diff(cdtime) <= 1,
                "cdtime {cdtime} moved to {once}, more than one tick"
            );
            let twice = nanos_to_cdtime(cdtime_to_nanos(once));
            assert_eq!(twice, once, "cdtime {once} is not stable on a second hop");
        }
    }
}
