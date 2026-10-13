//! The InfluxDB 2.x line-protocol codec, both directions: [`decode`] turns a `/api/v2/write`
//! body into one batch, [`encode`] packs a batch of events back into lines, [`line`] is the
//! decoder's lexer, and [`response`] writes the error JSON an InfluxDB client expects.
//!
//! **This module doc is the codec's canonical mapping table and normalization list**, which docs,
//! tests, and examples point at.
//! [ADR `influxdb-line-protocol-relay`](../../../../docs/adr/influxdb-line-protocol-relay.md) is
//! the decision record; it carries the normalization list with identical numbering. The grammar
//! below is what a real influxd 2.9.1 accepts, settled by probe; each [`line`] test cites its
//! probe row.
//!
//! ## Grammar
//!
//! `measurement[,tag=value...] field=value[,field=value...] [timestamp]`, one point per logical
//! line.
//!
//! | Element | Rule |
//! |---|---|
//! | escaping in a measurement, tag key, tag value, field key | a backslash before `,`, `=`, or a space is dropped and the character is literal; a field key also unescapes `\"`; any other backslash, `\\` included, is kept as is, so `\\` is two backslashes |
//! | a delimiter after a backslash | escaped, whatever precedes the backslash, so no name can end in a backslash: `v\\ f=1i` fails |
//! | `=` in a measurement | literal, unescaped |
//! | string field value | `"`-quoted; `\"` is a quote, `\\` one backslash, any other `\x` two characters; `,`, `=`, a space, and a raw newline are literal inside |
//! | float | `-?(digits[.digits] \| .digits)([eE][+-]?digits)?`, finite: `1`, `5.`, `.5`, `1.5e+3`, `-0`; `+1`, `inf`, `NaN`, `0x10`, `1e309` fail |
//! | integer / unsigned | `-?digitsi` in `i64` / `digitsu` in `u64`; `1.0i`, `-1u`, `1_000i`, an overflow fail |
//! | boolean | `t T true True TRUE f F false False FALSE`, nothing else |
//! | timestamp | `-?digits`, scaled by `precision` with overflow checked, then within ±9223372036854775806 ns |
//! | line splitting | a newline ends a line unless escaped or inside a quoted string value ([`line::lines`]) |
//! | whitespace | leading and trailing spaces and tabs, and runs of them after a separating space, are skipped; a tab never separates sections |
//! | comments, blank lines | a whole line starting `#`, after optional whitespace, and a blank line are skipped |
//! | a repeated tag key; tag key `_measurement`, `_field`, or `time`; field key `time` | a parse error |
//! | a repeated field key in one line | the last value at the first position; a different type is a parse error |
//! | CRLF, a trailing `# comment`, any other malformed shape | a parse error, influxd's reason as the probe recorded it |
//! | names and string values | UTF-8, else a parse error |
//!
//! ## Decode: wire → model
//!
//! One body is one [`logit_core::EventBatch`] under the caller's resource, or one
//! [`line::LineError`] and nothing. One point is one [`logit_core::Event`].
//!
//! | Wire | Model | Counter |
//! |---|---|---|
//! | measurement | event attribute [`ATTR_MEASUREMENT`], `Value::Str` | -- |
//! | tag `k=v` | event attribute `k`, `Value::Str`, never a resource attribute | -- |
//! | tag key under [`CARRIER_PREFIX`] | dropped (normalization 12) | `logit.input.tags.dropped{reason="reserved_prefix"}` |
//! | float / integer / unsigned / boolean field `f` | a [`logit_core::MetricRecord`] named `<measurement>.f`, `Gauge(F64 \| I64 \| U64 \| Bool)` | -- |
//! | N numeric or boolean fields | N records on the one event, in wire order | -- |
//! | string field `f="x"` | event attribute `influxdb.field.f` ([`FIELD_ATTR_PREFIX`]), `Value::Str` | -- |
//! | timestamp | `event.timestamp`, scaled to nanoseconds by [`Precision`] | -- |
//! | no timestamp | the request's arrival time truncated to [`Precision`], one instant for the body (normalization 14) | -- |
//! | a later point with the same measurement, tag set, and timestamp | merged into the earlier event: each field overwrites its namesake, across types too, or is appended (normalizations 2 and 4) | -- |
//! | a line that fails the grammar, or a timestamp outside the range | the whole body fails: [`line::LineError`] with the 1-based line index, the line's text, and the reason | -- |
//! | `org`, `orgID`, `bucket` | resource attributes [`RESOURCE_ATTR_ORG`], [`RESOURCE_ATTR_ORG_ID`], [`RESOURCE_ATTR_BUCKET`], set by the listener on the resource it passes in | -- |
//! | `Scope` | `None` | -- |
//!
//! A tag value, string value, or measurement that unescaping left unchanged is a zero-copy slice
//! of the body; one that changed is a copy. `_time` and `_value` arrive as ordinary tags: influxd
//! accepts them at write and its Flux reads then fail on them.
//!
//! <!-- encode-table:start -->
//! ## Encode: model → wire
//!
//! [`encode::InfluxLineEncoder`] writes one body per batch for `precision=ns`. Its module doc has
//! the escaping rules and the keys they can't spell; [`encode::EncodeStats`] names each counter,
//! which `influxdb_out` emits once per batch. The tables write each counter without its
//! `logit.output.` prefix.
//!
//! **Which path.** An event whose attribute [`ATTR_MEASUREMENT`] is a non-empty UTF-8 `Str` takes
//! the carrier path; any other event, or a carrier of any other value, takes the per-record path.
//!
//! - **Per-record path**: one line per [`logit_core::MetricRecord`], the record name as the
//!   measurement, and the kind's fields from the table below.
//! - **Carrier path**: one line per event, the carrier as the measurement. A `Gauge` or `Sum`
//!   record named `<measurement>.<key>`, with a non-empty key, becomes field `<key>` in its own
//!   type, in record order. Each event attribute `influxdb.field.<key>` ([`FIELD_ATTR_PREFIX`])
//!   whose value is a `Str` then becomes a quoted string field `<key>`, in attribute order; a
//!   non-`Str` value there is ignored. Any other record leaves on its own per-record line after
//!   the carrier line, counted `logit.output.metrics.normalized{reason="carrier_mismatch"}`. A
//!   carrier event with only string fields still writes its line; one with no field writes none.
//! - **Both paths**: the tags are the same, from the attribute table below, and every key under
//!   [`CARRIER_PREFIX`] or `statsd.` is skipped as a tag, uncounted.
//!
//! | Model | Wire | Counter |
//! |---|---|---|
//! | `Gauge`/`Sum` `F64` | `value=` (or `<key>=`) and the shortest string that round-trips | -- |
//! | `Gauge`/`Sum` `I64` / `U64` / `Bool` | `<n>i` / `<n>u` / `true` or `false` | -- |
//! | `Gauge`/`Sum` non-finite `F64` | skipped | `metrics.skipped{reason="unencodable_value"}` |
//! | `Sum` temporality and monotonicity | dropped (normalization 10) | -- |
//! | `GaugeDelta` | skipped: the pipeline lacks an `aggregate` | `metrics.skipped{metric_kind="gauge_delta"}`, diagnostic `gauge_delta_unresolved` |
//! | `Distribution` | `count=<n>u,sum=,p50=,p90=,p99=`, a non-finite component omitted | -- |
//! | `Samples` | sketched with each value weighted by its inverse sample rate, then as `Distribution` | -- |
//! | `Histogram` | `bucket_<bound>=<n>u` per finite bound; `sum`, `min`, `max`, and the `+Inf` bucket dropped | `metrics.skipped{reason="no_fields"}` when no bound is finite |
//! | `Summary` | `q<quantile>=<v>` per finite quantile; `count` and `sum` dropped | `metrics.skipped{reason="no_fields"}` when none is finite |
//! | `Set` | `value=<estimate>u` | -- |
//! | `SetMembers`, `ExponentialHistogram` | skipped | `metrics.skipped{metric_kind="set_members" \| "exponential_histogram"}`, diagnostic `encode_error` |
//! | a record flagged `NO_RECORDED_VALUE` | skipped | `metrics.skipped{reason="no_recorded_value"}`, diagnostic `no_recorded_value` |
//! | `unit`, `description`, `start_timestamp`, exemplars; `EventBatch::scope`; `Resource::schema_url` | dropped | -- |
//! | a log or span payload | skipped | -- |
//! | a measurement starting `#` | skipped: influxd reads the line as a comment | `metrics.skipped{reason="comment_measurement"}`, diagnostic `encode_error` |
//! | a measurement or carrier field key with no spelling | skipped, once per field | `metrics.skipped{reason="unencodable_key"}` |
//! | carrier field key `time` | skipped: influxd answers `422` | `metrics.skipped{reason="reserved_key"}` |
//! | carrier string field over 1 MiB | skipped: influxd answers `422` | `metrics.skipped{reason="oversize_value"}` |
//! | `event.timestamp` | the line's timestamp in nanoseconds; a same-series point already in the batch takes the next free nanosecond (normalization 9) | `metrics.skipped{reason="timestamp_overflow"}` when none is free below `i64::MAX` |
//!
//! Tags are the resource's attributes merged with the event's, the event's value winning:
//!
//! | Attribute value | Tag | Counter |
//! |---|---|---|
//! | `Str` (UTF-8) | as is, escaped | -- |
//! | `Bool`, `I64`, `U64`, `F64` | its decimal text | -- |
//! | `Array` | its last representable element | `tags.normalized{reason="multi_value"}` (lossy) |
//! | `Null`, `Bytes`, `Timestamp`, `Map`, a non-UTF-8 `Str`, an `Array` with no representable element | dropped | `tags.dropped{reason="unrepresentable"}` |
//! | an empty key or value | dropped | `tags.dropped{reason="empty"}` |
//! | a key or value with a line break, or ending in `\` | dropped | `tags.dropped{reason="unencodable"}` |
//! | key `_measurement`, `_field`, or `time` | dropped: influxd rejects the whole write | `tags.dropped{reason="reserved"}` |
//! | key under `influxdb.` or `statsd.` | not a tag | -- |
//!
//! <!-- encode-table:end -->
//!
//! ## Permitted normalizations
//!
//! `influxdb_in -> influxdb_out` is a fixed point modulo this list
//! (`crates/logit-proto/tests/influxdb_fixed_point.rs` pins it). The ADR uses the same numbering:
//!
//! 1. Batching: the writes `influxdb_out` sends needn't match the requests `influxdb_in` received.
//! 2. Duplicate points in one body (same measurement, tag set, and timestamp) merge, last write
//!    wins per field.
//! 3. Tag order is canonicalized.
//! 4. Field order: numeric and boolean fields leave in wire order (a field a later duplicate adds
//!    goes after the existing ones), and string fields leave after them, in an order the encoder
//!    doesn't preserve. A field's identity is its key, so order carries no meaning at the
//!    receiver.
//! 5. Number spelling: a float leaves as the shortest string that round-trips its `f64`, an
//!    integer keeps its `i` or `u` suffix, and a boolean leaves as `true` or `false` whatever
//!    spelling arrived.
//! 6. Escaping: the encoder writes only the escapes the grammar requires, so a backslash a sender
//!    wrote doubled leaves as it was read, and a key the grammar can't spell is dropped, counted
//!    `skipped{reason="unencodable_key"}`.
//! 7. Timestamps: every point leaves with a timestamp in nanoseconds, so `precision` isn't
//!    carried, and a point that arrived with none leaves with its receipt time (see item 14).
//! 8. Comments, blank lines, and runs of spaces are dropped. A CRLF line ending, a tab separator,
//!    and a trailing comment after the fields are parse errors, not normalized.
//! 9. A same-series point from another source in one sink batch is nudged forward by 1 ns.
//! 10. A `Sum`'s temporality and monotonicity are dropped on egress. A point from `influxdb_in` is
//!     always a `Gauge`, so this applies only to an event from another protocol.
//! 11. A listener's sender attributes (`network.peer.*`, `client.*`) are added under `peer:`,
//!     `proxy_protocol:`, or `forwarded:`.
//! 12. A tag key under the reserved `influxdb.` prefix is dropped, counted.
//! 13. A duplicate field key within one line: the last value wins.
//! 14. An absent timestamp: the request's arrival time truncated to `precision`, one instant for
//!     every such point in the request.
//!
//! ## Deviations from influxd
//!
//! - A `time` field key and a same-line field type conflict (`f=1i,f=2.5`) are parse errors
//!   (`400`), where influxd answers `422`. Across lines the decoder doesn't police types: a later
//!   value wins even across types, and a conflict is the destination's `422`.
//! - Neither influxd's 1 MiB field-value limit nor its 65,535-byte series-key limit is enforced;
//!   the listener's `max_request_bytes` bounds the body.
//! - A name or string value that isn't UTF-8 is a parse error, since an attribute string must be
//!   UTF-8.
//! - The error names the first bad line only; influxd lists every bad line, joined by `\n`.

pub mod decode;
pub mod encode;
pub mod line;
pub mod response;

pub use decode::{DecodeStats, InfluxDecoder};
pub use line::LineError;

/// The event attribute carrying a point's measurement, so the encoder can rebuild the line and
/// cross-protocol egress keeps the measurement in the record name (`<measurement>.<field>`).
pub const ATTR_MEASUREMENT: &str = "influxdb.measurement";
/// A string field rides as `influxdb.field.<key>` (`Value::Str`); strings are not metrics.
pub const FIELD_ATTR_PREFIX: &str = "influxdb.field.";
/// Every carrier attribute shares this prefix; the encoder never writes one as a tag.
pub const CARRIER_PREFIX: &str = "influxdb.";
pub const RESOURCE_ATTR_ORG: &str = "influxdb.org";
pub const RESOURCE_ATTR_ORG_ID: &str = "influxdb.org_id";
pub const RESOURCE_ATTR_BUCKET: &str = "influxdb.bucket";

/// The `precision` query parameter of a write: the unit of every timestamp in the body, and the
/// unit a timestamp-less point's arrival time is truncated to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Precision {
    Ns,
    Us,
    Ms,
    S,
}

impl Precision {
    /// The four spellings influxd accepts; anything else is a `400`.
    pub fn from_query(s: &str) -> Option<Self> {
        match s {
            "ns" => Some(Self::Ns),
            "us" => Some(Self::Us),
            "ms" => Some(Self::Ms),
            "s" => Some(Self::S),
            _ => None,
        }
    }

    fn factor(self) -> i64 {
        match self {
            Self::Ns => 1,
            Self::Us => 1_000,
            Self::Ms => 1_000_000,
            Self::S => 1_000_000_000,
        }
    }

    /// Scales a wire timestamp to nanoseconds; `None` when it overflows.
    pub fn scale_to_ns(self, t: i64) -> Option<i64> {
        t.checked_mul(self.factor())
    }

    /// Truncates a nanosecond instant to this unit, as influxd stamps a timestamp-less point.
    pub fn truncate(self, ns: i64) -> i64 {
        ns - ns.rem_euclid(self.factor())
    }
}
