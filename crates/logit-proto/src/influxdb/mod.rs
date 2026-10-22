//! The InfluxDB 2.x line-protocol codec, both directions: [`decode`] turns a `/api/v2/write`
//! body into one batch, [`encode`] packs a batch of events back into lines, [`line`] is the
//! decoder's lexer, and [`response`] writes the error JSON an InfluxDB client expects.
//!
//! **This module doc is the codec's canonical decode table, grammar, and normalization list**,
//! which docs, tests, and examples point at. Its encode mapping is generated from
//! `influxdb_out`'s column of `spec/transit/mapping.yaml`, the canonical copy ("Encode" below).
//! [ADR `influxdb-line-protocol-relay`](../../../../docs/adr/influxdb-line-protocol-relay.md) is
//! the decision record; it carries the normalization list with identical numbering. The grammar
//! below is what a real influxd 2.9.1 accepts, settled by probe; each [`line`] test cites the
//! ADR's "Verified `influxd` behavior" item that records it.
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
//! | `=` as a tag value's first byte | literal, unescaped: only a later unescaped `=` fails, and a first byte of `,` or a space is a missing value |
//! | string field value | `"`-quoted; `\"` is a quote, `\\` one backslash, any other `\x` two characters; `,`, `=`, a space, and a raw newline are literal inside |
//! | float | `-?(digits[.digits] \| .digits)([eE][+-]?digits)?`, finite: `1`, `5.`, `.5`, `1.5e+3`, `-0`; `+1`, `inf`, `NaN`, `0x10`, `1e309` fail |
//! | integer / unsigned | `-?digitsi` in `i64` / `digitsu` in `u64`; `1.0i`, `-1u`, `1_000i`, an overflow fail |
//! | boolean | `t T true True TRUE f F false False FALSE`, nothing else |
//! | timestamp | `-?digits`, scaled by `precision` with overflow checked, then within ±9223372036854775806 ns |
//! | line splitting | a newline ends a line unless escaped or inside a quoted string value ([`line::lines`]) |
//! | whitespace | spaces, tabs, and NULs leading a line, and runs of them after a separating space, are skipped; a tab or NUL never separates sections; only spaces may follow a timestamp |
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
//! | tag key under the private `_logit.` prefix | omitted (normalization 12) | `logit.input.tags.omitted{reason="reserved_prefix"}` |
//! | float / integer / unsigned / boolean field `f` | a [`logit_core::MetricRecord`] named `<measurement>.f`, `Gauge(F64 \| I64 \| U64 \| Bool)` | -- |
//! | N numeric or boolean fields | N records on the one event, in wire order | -- |
//! | string field `f="x"` | event attribute `_logit.influxdb.field.f` ([`FIELD_ATTR_PREFIX`]), `Value::Str` | -- |
//! | timestamp | `event.timestamp`, scaled to nanoseconds by [`Precision`] | -- |
//! | no timestamp | the request's arrival time truncated to [`Precision`], one instant for the body (normalization 14) | -- |
//! | a later point with the same measurement, tag set, and timestamp | merged into the earlier event: each field overwrites its namesake, across types too, or is appended (normalizations 2 and 4) | -- |
//! | a line that fails the grammar, or a timestamp outside the range | the whole body fails: [`line::LineError`] with the 1-based line index, the line's text, and the reason | -- |
//! | `org`, `orgID`, `bucket` | resource attributes [`RESOURCE_ATTR_ORG`], [`RESOURCE_ATTR_ORG_ID`], [`RESOURCE_ATTR_BUCKET`], set by the listener on the resource it passes in | -- |
//! | `Scope` | `None` | -- |
//!
//! The measurement, the string fields, and the three resource attributes are private carriers
//! under `_logit.influxdb.`
//! ([ADR `private-attribute-namespace`](../../../../docs/adr/private-attribute-namespace.md)).
//! Among the protocol sinks only `influxdb_out` writes them, and every other one leaves them off
//! its wire, uncounted; a whole-event render carries them, as it carries every private key (that
//! ADR's "The ignore rule" lists the renders). A string field's value is free text, and as a tag
//! or label at another sink each distinct value would start a series. Lua and transforms still
//! read a string field by its full key, and `aggregate` leaves it out of a series.
//!
//! **A point whose fields are all strings carries no signal.** Its event has no log, no metrics,
//! and no span, only the carriers, and `influxdb_out` still writes its line. A component that
//! selects by signal discards such an event unless its config names `none`: a `route_signals`
//! ahead of `influxdb_out` needs `signals.none` set to the target that feeds `influxdb_out`
//! (`none: remote_metrics` beside `metrics: remote_metrics`, say), a `has_signal` needs `none` in
//! its list, and a `keep_signals` drops it unless its list names `none`.
//!
//! A sender's tag never becomes a carrier or a string field, because every key under `_logit.` is
//! private to `logit`. Any other tag, one under `influxdb.` or `influxdb.field.` included, is an
//! ordinary tag, and `influxdb_out` writes it back as one.
//!
//! **The `line_protocol` transform** builds its events through the same path
//! ([`InfluxDecoder::decode_skipping`]), so this table is its mapping too, with four differences:
//! each event starts from a copy of the parent event's attributes, a point with no timestamp
//! takes the parent's timestamp untruncated, a bad line is skipped rather than failing the
//! message, and the counters are under `logit.transform.`. There are no `org`, `orgID`, or
//! `bucket` attributes. The transform's module doc
//! (`crates/logit-transforms/src/line_protocol.rs`) has its per-line rules and counters.
//!
//! A tag value, string value, or measurement that unescaping left unchanged is a zero-copy slice
//! of the body; one that changed is a copy. `_time` and `_value` arrive as ordinary tags: influxd
//! accepts them at write and its Flux reads then fail on them.
//!
//! ## Encode: model → wire
//!
//! [`encode::InfluxLineEncoder`] writes one body per batch for `precision=ns`. Its module doc has
//! the escaping rules and the keys they can't spell; [`encode::EncodeStats`] names each counter,
//! which `influxdb_out` emits once per batch. The hand-written tables write each counter without
//! its `logit.output.` prefix.
//!
//! **Which path.** An event whose attribute [`ATTR_MEASUREMENT`] is a non-empty UTF-8 `Str` takes
//! the carrier path; any other event, or a carrier of any other value, takes the per-record path.
//!
//! - **Per-record path**: one line per [`logit_core::MetricRecord`], the record name as the
//!   measurement, and the kind's fields from the table below.
//! - **Carrier path**: one line per event, the carrier as the measurement. A `Gauge` or `Sum`
//!   record named `<measurement>.<key>`, with a non-empty key, becomes field `<key>` in its own
//!   type, in record order. Each event attribute `_logit.influxdb.field.<key>`
//!   ([`FIELD_ATTR_PREFIX`]) whose value is a `Str` then becomes a quoted string field `<key>`, in
//!   attribute order; a non-`Str` value there is ignored. Any other record leaves on its own per-record line after
//!   the carrier line, counted `logit.output.metrics.normalized{reason="carrier_mismatch"}`. A
//!   carrier event with only string fields still writes its line; one with no field writes none.
//! - **Both paths**: the tags are the same, from the attribute table below, and every key under
//!   the private `_logit.` namespace is skipped as a tag, uncounted, [`FIELD_ATTR_PREFIX`] too
//!   ([ADR `private-attribute-namespace`](../../../../docs/adr/private-attribute-namespace.md)).
//!
//! The tables below are generated from `influxdb_out`'s column of `spec/transit/mapping.yaml`,
//! the canonical copy, by `script/transit docs`, and a test fails when they're stale: change the
//! table there, not here. "Consumer" means `influxdb_in` reading the write back.
//!
//! <!-- transit:doc:influxdb_out -->
//!
//! Per metric kind, and per record feature that decides a record's fate:
//!
//! | Row | Class | Reason or counter | What a consumer sees |
//! |---|---|---|---|
//! | `sum/delta_monotonic` | degraded | `logit.output.metrics.degraded{metric_kind=monotonic_delta_sum}` | A `value` field of a measurement named for the record, its temporality and monotonicity dropped, which influxdb_in reads back as a gauge named `<name>.value`, counted once per record. |
//! | `sum/delta_nonmonotonic` | degraded | `logit.output.metrics.degraded{metric_kind=non_monotonic_delta_sum}` | A `value` field, its temporality and monotonicity dropped, read back as a gauge named `<name>.value`, counted once per record. |
//! | `sum/cumulative_monotonic` | degraded | `logit.output.metrics.degraded{metric_kind=cumulative_sum}` | A `value` field holding the running total, its temporality and monotonicity dropped, read back as a gauge named `<name>.value`, counted once per record. |
//! | `sum/cumulative_nonmonotonic` | degraded | `logit.output.metrics.degraded{metric_kind=cumulative_sum}` | A `value` field, its temporality and monotonicity dropped, read back as a gauge named `<name>.value`, counted once per record. |
//! | `sum/f64` | exact | -- | A float field, written as the shortest string that reads back to the same bits. |
//! | `sum/i64` | exact | -- | An `<n>i` integer field, read back as an `i64`. |
//! | `sum/u64` | exact | -- | An `<n>u` unsigned field, read back as a `u64`. |
//! | `sum/bool` | exact | -- | A `true` or `false` field, read back as a boolean. |
//! | `gauge/f64` | normalized | `value_field` | A `value` field of a measurement named for the record, read back as a gauge named `<name>.value`; an event carrying the `_logit.influxdb.measurement` carrier writes its gauges as that measurement's fields instead, under their own names. |
//! | `gauge/i64` | normalized | `value_field` | An `<n>i` `value` field, read back as an `i64` gauge named `<name>.value`. |
//! | `gauge/u64` | normalized | `value_field` | An `<n>u` `value` field, read back as a `u64` gauge named `<name>.value`. |
//! | `gauge/bool` | normalized | `value_field` | A `true` or `false` `value` field, read back as a gauge named `<name>.value`. |
//! | `gauge/negative` | normalized | `value_field` | A negative `value` field, read back as a gauge named `<name>.value`. |
//! | `gauge_delta/f64` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | An unresolved relative adjustment is dropped, with a throttled `gauge_delta_unresolved` warning; an `aggregate` upstream resolves it into a gauge. |
//! | `gauge_delta/i64` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | Dropped, as for a float adjustment. |
//! | `gauge_delta/u64` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | Dropped, as for a float adjustment. |
//! | `gauge_delta/bool` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | Dropped, as for a float adjustment. |
//! | `samples` | degraded | `logit.output.metrics.degraded{metric_kind=samples}` | Sketched, each value weighted by its inverse sample rate, then written as the `distribution` row describes, counted once per record. A non-finite sum or quantile counts as that row says. |
//! | `set_members` | skipped | `logit.output.metrics.skipped{metric_kind=set_members}` | Line protocol has no set, so set members aren't written, with a throttled `encode_error` warning. |
//! | `distribution` | degraded | `logit.output.metrics.degraded{metric_kind=distribution}` | In the `summary` row's shape: float `count` and `sum` fields and the sketch's `0.5`, `0.9`, and `0.99` quantiles, each read back as a gauge under `<name>.`; the sketch is gone, counted once per record. A non-finite sum or quantile is left off the line, and the record also counts `logit.output.metrics.degraded{reason=non_finite_component}`. |
//! | `set` | degraded | `logit.output.metrics.degraded{metric_kind=set}` | A `value=<estimate>u` field, read back as a gauge named `<name>.value`; the registers are gone, counted once per record. |
//! | `histogram/delta` | degraded | `logit.output.metrics.degraded{metric_kind=histogram}` | Telegraf's shape: float `count`, `sum`, `min`, and `max` fields, a field per finite bound named by the bound and holding the cumulative count up to it, and `+Inf` holding the total, each read back as a gauge under `<name>.`, its temporality dropped, counted once per record. A non-finite `sum`, `min`, or `max` is left off the line, and the record also counts `logit.output.metrics.degraded{reason=non_finite_component}`. |
//! | `histogram/cumulative` | degraded | `logit.output.metrics.degraded{metric_kind=histogram}` | Telegraf's shape: float `count`, `sum`, `min`, and `max` fields, a field per finite bound named by the bound and holding the cumulative count up to it, and `+Inf` holding the total, each read back as a gauge under `<name>.`, its temporality dropped, counted once per record. A non-finite `sum`, `min`, or `max` is left off the line, and the record also counts `logit.output.metrics.degraded{reason=non_finite_component}`. |
//! | `histogram/nan_bound` | degraded | `logit.output.metrics.degraded{reason=non_finite_component}` | No field key spells a `NaN` bound, so none is written, and its count reaches only `count` and `+Inf`. |
//! | `histogram/neg_inf_bound` | degraded | `logit.output.metrics.degraded{reason=non_finite_component}` | No field key spells a `-Inf` bound, so none is written, and its count reaches every finite bound's field. |
//! | `exponential_histogram/delta` | skipped | `logit.output.metrics.skipped{metric_kind=exponential_histogram}` | Neither Telegraf nor the OTel Collector's InfluxDB exporter has a line-protocol form for an exponential histogram, so none is written, with a throttled `encode_error` warning. |
//! | `exponential_histogram/cumulative` | skipped | `logit.output.metrics.skipped{metric_kind=exponential_histogram}` | Neither Telegraf nor the OTel Collector's InfluxDB exporter has a line-protocol form for an exponential histogram, so none is written, with a throttled `encode_error` warning. |
//! | `summary/delta` | degraded | `logit.output.metrics.degraded{metric_kind=summary}` | Telegraf's shape: float `count` and `sum` fields and a field per quantile named by the quantile (`0.5`, `0.99`), each read back as a gauge under `<name>.`, counted once per record. Line protocol has no temporality, so a delta summary writes as a cumulative one does. A non-finite `sum`, quantile, or quantile value is left off the line, and the record also counts `logit.output.metrics.degraded{reason=non_finite_component}`. |
//! | `summary/cumulative` | degraded | `logit.output.metrics.degraded{metric_kind=summary}` | Telegraf's shape: float `count` and `sum` fields and a field per quantile named by the quantile (`0.5`, `0.99`), each read back as a gauge under `<name>.`, counted once per record. A non-finite `sum`, quantile, or quantile value is left off the line, and the record also counts `logit.output.metrics.degraded{reason=non_finite_component}`. |
//! | `record/no_recorded_value` | skipped | `logit.output.metrics.skipped{reason=no_recorded_value}` | A point flagged as holding no recorded value isn't written, with a throttled `no_recorded_value` warning. |
//! | `record/no_recorded_value_delta_sum` | skipped | `logit.output.metrics.skipped{reason=no_recorded_value}` | A point flagged as holding no recorded value isn't written, with a throttled `no_recorded_value` warning. |
//! | `record/no_recorded_value_other_kind` | skipped | `logit.output.metrics.skipped{reason=no_recorded_value}` | A point flagged as holding no recorded value isn't written, with a throttled `no_recorded_value` warning. |
//! | `record/service_check` | normalized | `value_field` | The status gauge as a `value` field; the check's carriers aren't tags. |
//! | `record/sample_rate` | exact | -- | Each value is weighted by its inverse rate in the sketch the `samples` row describes. |
//! | `record/superseded` | exact | -- | Each point is its own line at its own timestamp. |
//! | `record/same_instant` | normalized | `next_nanosecond` | A point sharing a series and a timestamp with one already in the batch takes the next free nanosecond, so both are kept. |
//! | `record/same_millisecond` | exact | -- | Each point is its own line at its own nanosecond. |
//! | `record/non_finite` | dropped | `logit.output.metrics.dropped{reason=unencodable_value}` | Line protocol has no non-finite float, so the record is dropped. |
//! | `log/dogstatsd_event` | skipped | `logit.output.events.skipped{reason=no_metrics}` | A log has no line-protocol form and isn't written. |
//! | `log/plain` | skipped | `logit.output.events.skipped{reason=no_metrics}` | A log has no line-protocol form and isn't written. |
//! | `span/alone` | skipped | `logit.output.events.skipped{reason=no_metrics}` | A span has no line-protocol form and isn't written. |
//! | `span/no_wire_form` | exact | -- | Nothing beyond `span/alone`'s outcome. |
//! | `event/attributes_only` | skipped | `logit.output.events.skipped{reason=no_metrics}` | An event with no metric, log, or span and no `_logit.influxdb.measurement` carrier has no measurement to write under and isn't written. A stage that drops the carrier leaves an InfluxDB point whose fields are all strings this way: a `remove` of it, or a `keep` under `private: drop`, which drops the string fields too. |
//! | `event/string_only` | exact | -- | A line of the event's `_logit.influxdb.field.*` string fields under the measurement its `_logit.influxdb.measurement` carrier names, read back as the same event. |
//!
//! Per field of a record that reaches the wire, attribute shape, and payload beside metrics:
//!
//! | Row | Class | Reason or counter | What a consumer sees |
//! |---|---|---|---|
//! | `record/unit` | lost | structural | Line protocol has no unit, so none is written. |
//! | `record/description` | lost | structural | Line protocol has no description, so none is written. |
//! | `record/start_timestamp` | lost | structural | Line protocol has no start time, so none is written, though the OTel Collector's InfluxDB exporter writes a `start_time_unix_nano` field; `docs/known-gaps/influxdb.md` has the gap. |
//! | `record/exemplars` | lost | structural | Line protocol has no exemplars, so none are written, though the OTel Collector's InfluxDB exporter writes each as a point of a `<name>_exemplar` measurement; `docs/known-gaps/influxdb.md` has the gap. |
//! | `attributes/string_value` | lost | `logit.output.tags.omitted{reason=unencodable}` | A tag. A value holding a line break, ending in `\`, or with `\` before a space has no spelling and is left off, counted. |
//! | `attributes/bool_value` | normalized | `stringified` | A tag of `true` or `false`, read back as the string. |
//! | `attributes/number_value` | normalized | `stringified` | A tag of the number's decimal text, read back as the string. |
//! | `attributes/array_value` | normalized | `multi_value`; `logit.output.tags.normalized{reason=multi_value}` | A tag holds one value, so only the last representable element is written, counted. |
//! | `attributes/map_value` | lost | `logit.output.tags.omitted{reason=unrepresentable}` | A map has no tag form and is left off, counted. |
//! | `attributes/bytes_value` | lost | `logit.output.tags.omitted{reason=unrepresentable}` | A bytes value has no tag form and is left off, counted. |
//! | `attributes/timestamp_value` | lost | `logit.output.tags.omitted{reason=unrepresentable}` | A timestamp value has no tag form and is left off, counted. |
//! | `attributes/private_carrier` | exact | -- | A `_logit.influxdb.*` carrier is consumed into the wire form (the measurement, the string fields, the write's org and bucket) and influxdb_in stamps it back. |
//! | `attributes/foreign_carrier` | lost | structural | Another protocol's `_logit.` carrier is private and isn't a tag, so it's left off. |
//! | `attributes/resource_attribute` | normalized | `resource_merged` | A tag, the event's value winning on a shared key, read back as an event attribute. |
//! | `attributes/scope` | lost | structural | Line protocol has no instrumentation scope, so none is written. |
//! | `attributes/datadog_carrier_log` | lost | structural | Another protocol's `_logit.` carrier is private and isn't a tag, so it's left off. |
//! | `attributes/no_host` | exact | -- | A record with no host leaves with none. |
//! | `log/beside_metrics` | lost | uncounted | A log on an event that carries metrics is left off while the metrics are written, uncounted. |
//! | `span/beside_metrics` | lost | uncounted | A span on an event that carries metrics is left off while the metrics are written, uncounted. |
//!
//! <!-- transit:end -->
//!
//! The rest of the encode mapping is hand-written: rules the spec has no row for, and details its
//! cells don't state. The multi-field kinds take the shape the OTel Collector's InfluxDB exporter
//! and Telegraf write by default
//! ([ADR `influxdb-histogram-summary-shape`](../../../../docs/adr/influxdb-histogram-summary-shape.md)).
//!
//! | Model | Wire | Counter |
//! |---|---|---|
//! | `Histogram` | `count=,sum=,min=,max=`, then `<bound>=` per finite bound, in ascending order, holding the cumulative count up to it, then `+Inf=` holding the total; `count` and `+Inf` always, the rest when present | -- |
//! | `Summary` | `count=,sum=`, then `<quantile>=` per quantile | -- |
//! | `Distribution`, `Samples` | the `Summary` shape with the sketch's count, sum, and `0.5`, `0.9`, and `0.99` quantiles; an empty sketch's quantiles left off | -- |
//! | each kind in the three rows above | every value an unsuffixed float; a bound or quantile key is the `f64`'s shortest round-trip decimal, never an exponent (`1000000`, `0.00001`) | -- |
//! | a non-finite `sum`, `min`, `max`, quantile, quantile value, or bound other than `+Inf`, in one of those kinds | left off the line, which is still written | `metrics.degraded{reason="non_finite_component"}`, once per record |
//! | `unit`, `description`, `start_timestamp`, exemplars; `EventBatch::scope`; `Resource::schema_url` | dropped | -- |
//! | a measurement starting `#` | dropped: influxd reads the line as a comment | `metrics.dropped{reason="comment_measurement"}`, diagnostic `encode_error` |
//! | a measurement or carrier field key with no spelling (encode.rs's "Escaping"), such as a measurement starting with a tab or NUL, or a line whose every field key does | dropped, once per field | `metrics.dropped{reason="unencodable_key"}` |
//! | carrier field key `time` | dropped: influxd answers `422` | `metrics.dropped{reason="reserved_key"}` |
//! | carrier string field over 1 MiB | dropped: influxd answers `422` | `metrics.dropped{reason="oversize_value"}` |
//! | `event.timestamp` | the line's timestamp in nanoseconds; a same-series point already in the batch takes the next free nanosecond (normalization 9) | `metrics.dropped{reason="timestamp_overflow"}` when none is free below `i64::MAX` |
//!
//! Tags are the resource's attributes merged with the event's, the event's value winning:
//!
//! | Attribute value | Tag | Counter |
//! |---|---|---|
//! | `Null`, `Bytes`, `Timestamp`, `Map`, a non-UTF-8 `Str`, an `Array` with no representable element | omitted | `tags.omitted{reason="unrepresentable"}` |
//! | an empty key or value | omitted | `tags.omitted{reason="empty"}` |
//! | a key or value with a line break, ending in `\`, or with `\` directly before a space | omitted | `tags.omitted{reason="unencodable"}` |
//! | key `_measurement`, `_field`, or `time` | omitted: influxd rejects the whole write | `tags.omitted{reason="reserved"}` |
//! | key under `_logit.` | not a tag | -- |
//!
//! ## Permitted normalizations
//!
//! `influxdb_in -> influxdb_out` is a fixed point modulo this list
//! (`crates/logit-proto/tests/influxdb_fixed_point.rs` pins it). The ADR uses the same numbering:
//!
//! 1. Batching: the writes `influxdb_out` sends needn't match the requests `influxdb_in` received.
//! 2. Duplicate points in one body (same measurement, tag set, and timestamp) merge, last write
//!    wins per field.
//! 3. Tag order isn't preserved: tags leave in the order the process interned their keys.
//!    influxd sorts the series key itself, so what it stores is unaffected; only the bytes on the
//!    wire differ between processes.
//! 4. Field order: numeric and boolean fields leave in wire order (a field a later duplicate adds
//!    goes after the existing ones), and string fields leave after them, in an order the encoder
//!    doesn't preserve. A field whose key starts with a tab or NUL is never written first. A
//!    field's identity is its key, so order carries no meaning at the receiver.
//! 5. Number spelling: a float leaves as the shortest string that round-trips its `f64`, an
//!    integer keeps its `i` or `u` suffix, and a boolean leaves as `true` or `false` whatever
//!    spelling arrived.
//! 6. Escaping: the encoder writes only the escapes the grammar requires, so a backslash a sender
//!    wrote doubled leaves as it was read, and a key the grammar can't spell is left out, counted:
//!    a measurement starting with a tab or NUL, a field key starting with one on a line whose
//!    every field key does, and a measurement or field key ending in `\` or with `\` before a
//!    delimiter are `metrics.dropped{reason="unencodable_key"}`, and a tag key or value ending in
//!    `\` or with `\` before a space is `tags.omitted{reason="unencodable"}`.
//! 7. Timestamps: every point leaves with a timestamp in nanoseconds, so `precision` isn't
//!    carried, and a point that arrived with none leaves with its receipt time (see item 14).
//! 8. Comments, blank lines, and runs of spaces are dropped. A CRLF line ending, a tab separator,
//!    and a trailing comment after the fields are parse errors, not normalized.
//! 9. A same-series point from another source in one sink batch is nudged forward by 1 ns.
//! 10. A `Sum`'s temporality and monotonicity, and a `Histogram`'s temporality, are dropped on
//!     egress. A point from `influxdb_in` is always a `Gauge`, so this applies only to an event
//!     from another protocol.
//! 11. A listener's sender attributes (`network.peer.*`, `client.*`) are added under `peer:`,
//!     `proxy_protocol:`, or `forwarded:`.
//! 12. A tag key under the private `_logit.` prefix is omitted, counted.
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
pub const ATTR_MEASUREMENT: &str = "_logit.influxdb.measurement";
/// A string field rides as `_logit.influxdb.field.<key>` (`Value::Str`); strings are not metrics.
/// The key is private, so the decoder refuses a tag under it and no protocol sink writes one as a
/// tag, and a tag and a string field can't trade places.
pub const FIELD_ATTR_PREFIX: &str = "_logit.influxdb.field.";
pub const RESOURCE_ATTR_ORG: &str = "_logit.influxdb.org";
pub const RESOURCE_ATTR_ORG_ID: &str = "_logit.influxdb.org_id";
pub const RESOURCE_ATTR_BUCKET: &str = "_logit.influxdb.bucket";

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

/// The resource `influxdb_in` decodes a write under: `org`, `orgID`, and `bucket` as
/// [`RESOURCE_ATTR_ORG`], [`RESOURCE_ATTR_ORG_ID`], and [`RESOURCE_ATTR_BUCKET`], each only
/// when the request named it.
pub fn write_resource(
    org: Option<&str>,
    org_id: Option<&str>,
    bucket: &str,
) -> logit_core::Resource {
    let mut resource = logit_core::Resource::default();
    if let Some(org) = org {
        resource.attributes.insert(RESOURCE_ATTR_ORG, logit_core::Value::str(org));
    }
    if let Some(org_id) = org_id {
        resource.attributes.insert(RESOURCE_ATTR_ORG_ID, logit_core::Value::str(org_id));
    }
    resource.attributes.insert(RESOURCE_ATTR_BUCKET, logit_core::Value::str(bucket));
    resource
}
