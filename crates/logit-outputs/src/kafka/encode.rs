//! `kafka_out`'s record encoder: one [`MessageBuf`] entry per record, its bytes the record's value
//! and its [`RecordMeta`] the key and timestamp (`docs/adr/kafka-output.md`, item 11 and
//! "Amendment: what W3 settled").
//!
//! | `format:` | `record: event` (default but for `native`) | `record: batch` (`native`'s default) |
//! |---|---|---|
//! | `json` | `crate::ndjson`'s object, without the newline | those objects as NDJSON, each line newline-terminated |
//! | `text` | the log body: a `Str` or `Bytes` as it is, a `Map` or `Array` in `crate::ndjson`'s value form, a timestamp as RFC 3339, another scalar as its `Display` text, `Null` as nothing | those bodies, each with `\` escaped as `\\` and LF as `\n` (`logit_proto::kafka::text`), joined by LF with none after the last |
//! | `msgpack` | `crate::msgpack`'s map | those maps back to back |
//! | `native` | a `CODEC_BATCH` frame of a one-event batch under the batch's resource and scope | one `CODEC_BATCH` frame of the events |
//! | `otlp_proto` | takes no `record:`: one signal's OTLP export request in protobuf, below | |
//! | `otlp_json` | takes no `record:`: the same request as OTLP/JSON, below | |
//!
//! `crate::ndjson`'s doc is the grammar of the JSON object and `crate::msgpack`'s the
//! MessagePack value forms. A native frame is uncompressed: `compression:` compresses the record
//! batch it travels in. `json`, `msgpack`, and `native` leave out an event's top-level
//! `_logit.kafka.*` attributes, the record metadata `kafka_in` carries
//! (`docs/adr/kafka-input.md`, "12. Record metadata rides as private carriers").
//!
//! `format: text` counts an event it skips for having no log record in [`EncodeStats::no_log`],
//! and the metrics and span an event whose body it writes leaves behind in
//! [`EncodeStats::metrics_omitted`] and [`EncodeStats::spans_omitted`], at encode, so a record
//! the producer later drops as oversize, or the broker rejects or names, has still counted them.
//! An event dropped as `oversize` under `record: batch` counts only there. An OTLP format counts
//! an event with no log, span, or metric, which no request holds, in [`EncodeStats::no_payload`],
//! and each attribute value it writes as a double, a `u64` above `i64::MAX`, in
//! [`EncodeStats::u64_as_double`]. What each metric kind, log field, span, and attribute
//! becomes, in every format, is the generated table under "What each metric, log, span, and
//! attribute becomes".
//!
//! Under `record: batch`, events go into a record in batch order until the next would push its
//! value over the encoder's `budget`, which the sink sets so a value that fits never trips the
//! producer's `max_request_bytes` check ([`super::batch::value_budget`]); that event starts the
//! next record. An event whose own value is over the budget is dropped and counted in
//! [`EncodeStats::oversize`]. A native frame's size isn't the sum of its events' (the dictionary
//! is shared), so `native` finds each record's longest prefix of the remaining events that fits by
//! encoding candidates: the whole batch once, then for each record by doubling and bisecting. A
//! frame can't skip an event, so an event over the budget ends the open `native` record where the
//! other formats keep filling it. A batch record has no key, and its timestamp is its first
//! event's.
//!
//! Under `record: event`, a key is text, as UTF-8 bytes (`docs/adr/kafka-output.md`, item 3), the
//! same canonical bytes `sample` hashes for a key (`logit_core::sampling::hash_value`):
//!
//! | Value at the key | Key bytes |
//! |---|---|
//! | `trace_id`: the span's, else the log's `TraceRef` | 32 lowercase hex characters; an all-zero id is no value |
//! | `Str`, `Bytes` | as they are |
//! | `I64`, `U64` | decimal text |
//! | `F64` | Rust `Display`: the shortest decimal text that round-trips, never an exponent |
//! | `Bool` | `true` or `false` |
//! | `Timestamp` | decimal nanoseconds |
//! | `Null`, `Array`, `Map`, or no value | no key: the record goes unkeyed, counted in [`EncodeStats::missing_key`] |
//!
//! A record's timestamp is the event's, in milliseconds since the Unix epoch, rounded down.
//!
//! # OTLP formats
//!
//! `otlp_proto` and `otlp_json` write one record per OTLP export request
//! (`docs/adr/kafka-otlp-formats.md`); they differ only in the serialization of the request, and
//! the size budget measures that serialization's length.
//! A batch becomes one request per signal that has records, in the order logs, traces, metrics,
//! each built by `OtlpEncoder::request` from the batch's own events under its resource and
//! scope, and tagged with its signal in [`RecordMeta::signal`], which picks its topic. Two
//! things split a signal's request further, and nothing else does:
//!
//! - **The key.** `trace_id` splits logs and spans into one request per trace id, in order of
//!   first appearance, keyed as the per-event table's first row; each signal reads its own
//!   field, a log its `TraceRef` and a span its own id, so an event carrying both can key its
//!   two records differently. The events with none form one unkeyed request, which counts once
//!   per record in [`EncodeStats::missing_key`]. Metrics stay one unkeyed request and count
//!   nothing. `{resource: NAME}` keys every request of the batch with that value's text, or
//!   sends each unkeyed, counted. `{attribute: NAME}` is refused by the sink.
//! - **The size.** A request's budget is the encoder's less its key's length, the record overhead
//!   the producer's cap check adds for a key. A request over it is cut by events, each record the
//!   longest run of the remaining events that fits, found as `native` finds one. A single event
//!   whose request is over the budget alone is dropped, counted once in
//!   [`EncodeStats::oversize`] for each signal record it would have formed.
//!
//! The first encode of each key group is the one that counts the OTLP mapping's side effects
//! (`logit.output.metrics.skipped`, `dropped`, and `degraded`); every probe of the cut runs with
//! the gate [`KafkaEncoder::with_counters`] takes muted. A record's timestamp is the time the
//! encoder wrote it: a request holds many events, and one future-stamped event would otherwise
//! lose the whole request to Kafka's rule against records stamped more than an hour ahead.
//!
//! # What each metric, log, span, and attribute becomes
//!
//! The tables below are generated from `kafka_out`'s column of `spec/transit/mapping.yaml`, the
//! canonical copy, by `script/transit docs`, and a test fails when they're stale: change the
//! table there, not here. They cover every `format:`, each condition a cell's `when:`, and
//! "consumer" means `kafka_in` reading the record in the same format.
//!
//! <!-- transit:doc:kafka_out -->
//!
//! Per metric kind, and per record feature that decides a record's fate:
//!
//! | Row | Class | Reason or counter | What a consumer sees |
//! |---|---|---|---|
//! | `sum/delta_monotonic`, under `format: otlp_proto` | exact | -- | As at otlp_out: An OTLP `Sum` with delta temporality and the monotonic flag. |
//! | `sum/delta_monotonic`, under `format: otlp_json` | exact | -- | As at otlp_out: An OTLP `Sum` with delta temporality and the monotonic flag. |
//! | `sum/delta_monotonic`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `sum/delta_nonmonotonic`, under `format: otlp_proto` | exact | -- | As at otlp_out: An OTLP `Sum` with delta temporality, not monotonic. |
//! | `sum/delta_nonmonotonic`, under `format: otlp_json` | exact | -- | As at otlp_out: An OTLP `Sum` with delta temporality, not monotonic. |
//! | `sum/delta_nonmonotonic`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `sum/cumulative_monotonic`, under `format: otlp_proto` | exact | -- | As at otlp_out: An OTLP `Sum` with cumulative temporality and the monotonic flag. |
//! | `sum/cumulative_monotonic`, under `format: otlp_json` | exact | -- | As at otlp_out: An OTLP `Sum` with cumulative temporality and the monotonic flag. |
//! | `sum/cumulative_monotonic`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `sum/cumulative_nonmonotonic`, under `format: otlp_proto` | exact | -- | As at otlp_out: An OTLP `Sum` with cumulative temporality, not monotonic. |
//! | `sum/cumulative_nonmonotonic`, under `format: otlp_json` | exact | -- | As at otlp_out: An OTLP `Sum` with cumulative temporality, not monotonic. |
//! | `sum/cumulative_nonmonotonic`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `sum/f64`, under `format: otlp_proto` | exact | -- | As at otlp_out: The data point's `as_double`. |
//! | `sum/f64`, under `format: otlp_json` | exact | -- | As at otlp_out: The data point's `as_double`. |
//! | `sum/f64`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `sum/i64`, under `format: otlp_proto` | exact | -- | As at otlp_out: The data point's `as_int`. |
//! | `sum/i64`, under `format: otlp_json` | exact | -- | As at otlp_out: The data point's `as_int`. |
//! | `sum/i64`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `sum/u64`, under `format: otlp_proto` | normalized | `u64_as_int`; `logit.output.metrics.normalized{reason=u64_as_double}` | As at otlp_out: OTLP has one signed integer, so a `u64` that fits an `int64` is `as_int` and reads back as an `i64`; one above `i64::MAX` is `as_double`, counted, and reads back as an `f64`. |
//! | `sum/u64`, under `format: otlp_json` | normalized | `u64_as_int`; `logit.output.metrics.normalized{reason=u64_as_double}` | As at otlp_out: OTLP has one signed integer, so a `u64` that fits an `int64` is `as_int` and reads back as an `i64`; one above `i64::MAX` is `as_double`, counted, and reads back as an `f64`. |
//! | `sum/u64`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `sum/bool`, under `format: otlp_proto` | normalized | `bool_value`; `logit.output.metrics.normalized{reason=bool_value}` | As at otlp_out: A boolean is `as_int` `1` or `0` and reads back as that integer. |
//! | `sum/bool`, under `format: otlp_json` | normalized | `bool_value`; `logit.output.metrics.normalized{reason=bool_value}` | As at otlp_out: A boolean is `as_int` `1` or `0` and reads back as that integer. |
//! | `sum/bool`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `gauge/f64`, under `format: otlp_proto` | exact | -- | As at otlp_out: An OTLP `Gauge`, `as_double`. |
//! | `gauge/f64`, under `format: otlp_json` | exact | -- | As at otlp_out: An OTLP `Gauge`, `as_double`. |
//! | `gauge/f64`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `gauge/i64`, under `format: otlp_proto` | exact | -- | As at otlp_out: An OTLP `Gauge`, `as_int`. |
//! | `gauge/i64`, under `format: otlp_json` | exact | -- | As at otlp_out: An OTLP `Gauge`, `as_int`. |
//! | `gauge/i64`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `gauge/u64`, under `format: otlp_proto` | normalized | `u64_as_int`; `logit.output.metrics.normalized{reason=u64_as_double}` | As at otlp_out: A `u64` that fits an `int64` is `as_int` and reads back as an `i64`; one above `i64::MAX` is `as_double`, counted, and reads back as an `f64`. |
//! | `gauge/u64`, under `format: otlp_json` | normalized | `u64_as_int`; `logit.output.metrics.normalized{reason=u64_as_double}` | As at otlp_out: A `u64` that fits an `int64` is `as_int` and reads back as an `i64`; one above `i64::MAX` is `as_double`, counted, and reads back as an `f64`. |
//! | `gauge/u64`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `gauge/bool`, under `format: otlp_proto` | normalized | `bool_value`; `logit.output.metrics.normalized{reason=bool_value}` | As at otlp_out: A boolean is `as_int` `1` or `0` and reads back as that integer. |
//! | `gauge/bool`, under `format: otlp_json` | normalized | `bool_value`; `logit.output.metrics.normalized{reason=bool_value}` | As at otlp_out: A boolean is `as_int` `1` or `0` and reads back as that integer. |
//! | `gauge/bool`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `gauge/negative`, under `format: otlp_proto` | exact | -- | As at otlp_out: A negative data point, written as is. |
//! | `gauge/negative`, under `format: otlp_json` | exact | -- | As at otlp_out: A negative data point, written as is. |
//! | `gauge/negative`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `gauge/prometheus_info`, under `format: otlp_proto` | normalized | `prometheus_info_sum` | As at otlp_out: A cumulative non-monotonic OTLP `Sum` named `<name>_info`, as the OTel Prometheus compatibility spec's Info section (marked Development) has it, read back as that sum. |
//! | `gauge/prometheus_info`, under `format: otlp_json` | normalized | `prometheus_info_sum` | As at otlp_out: A cumulative non-monotonic OTLP `Sum` named `<name>_info`, as the OTel Prometheus compatibility spec's Info section (marked Development) has it, read back as that sum. |
//! | `gauge/prometheus_info`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `gauge/prometheus_stateset`, under `format: otlp_proto` | normalized | `prometheus_stateset_sum` | As at otlp_out: A cumulative non-monotonic OTLP `Sum`, as the OTel Prometheus compatibility spec's StateSet section (marked Development) has it, read back as that sum. |
//! | `gauge/prometheus_stateset`, under `format: otlp_json` | normalized | `prometheus_stateset_sum` | As at otlp_out: A cumulative non-monotonic OTLP `Sum`, as the OTel Prometheus compatibility spec's StateSet section (marked Development) has it, read back as that sum. |
//! | `gauge/prometheus_stateset`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `gauge_delta/f64`, under `format: otlp_proto` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | As at otlp_out: An unresolved relative adjustment is dropped, with a throttled `gauge_delta_unresolved` warning; an `aggregate` upstream resolves it into a gauge. |
//! | `gauge_delta/f64`, under `format: otlp_json` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | As at otlp_out: An unresolved relative adjustment is dropped, with a throttled `gauge_delta_unresolved` warning; an `aggregate` upstream resolves it into a gauge. |
//! | `gauge_delta/f64`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `gauge_delta/i64`, under `format: otlp_proto` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | As at otlp_out: Dropped, as for a float adjustment. |
//! | `gauge_delta/i64`, under `format: otlp_json` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | As at otlp_out: Dropped, as for a float adjustment. |
//! | `gauge_delta/i64`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `gauge_delta/u64`, under `format: otlp_proto` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | As at otlp_out: Dropped, as for a float adjustment. |
//! | `gauge_delta/u64`, under `format: otlp_json` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | As at otlp_out: Dropped, as for a float adjustment. |
//! | `gauge_delta/u64`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `gauge_delta/bool`, under `format: otlp_proto` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | As at otlp_out: Dropped, as for a float adjustment. |
//! | `gauge_delta/bool`, under `format: otlp_json` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | As at otlp_out: Dropped, as for a float adjustment. |
//! | `gauge_delta/bool`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `samples`, under `format: otlp_proto` | degraded | `logit.output.metrics.degraded{metric_kind=samples}` | As at otlp_out: Sketched, each value weighted by its inverse sample rate, then written as the `distribution` row describes. |
//! | `samples`, under `format: otlp_json` | degraded | `logit.output.metrics.degraded{metric_kind=samples}` | As at otlp_out: Sketched, each value weighted by its inverse sample rate, then written as the `distribution` row describes. |
//! | `samples`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `set_members`, under `format: otlp_proto` | skipped | `logit.output.metrics.skipped{metric_kind=set_members}` | As at otlp_out: OTLP has no cardinality type, so set members aren't written, with a throttled `otlp_set_members_metric_skipped` warning. |
//! | `set_members`, under `format: otlp_json` | skipped | `logit.output.metrics.skipped{metric_kind=set_members}` | As at otlp_out: OTLP has no cardinality type, so set members aren't written, with a throttled `otlp_set_members_metric_skipped` warning. |
//! | `set_members`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `distribution`, under `format: otlp_proto` | degraded | `logit.output.metrics.degraded{metric_kind=distribution}` | As at otlp_out: An OTLP `Summary` of the sketch's `0` (its minimum), `0.5`, `0.75`, `0.9`, `0.95`, `0.99`, and `1` (its maximum) quantiles with its own count and sum; OTLP has no mergeable sketch. |
//! | `distribution`, under `format: otlp_json` | degraded | `logit.output.metrics.degraded{metric_kind=distribution}` | As at otlp_out: An OTLP `Summary` of the sketch's `0` (its minimum), `0.5`, `0.75`, `0.9`, `0.95`, `0.99`, and `1` (its maximum) quantiles with its own count and sum; OTLP has no mergeable sketch. |
//! | `distribution`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `set`, under `format: otlp_proto` | skipped | `logit.output.metrics.skipped{metric_kind=set}` | As at otlp_out: OTLP has no cardinality type, so a set estimate isn't written, with a throttled `otlp_set_metric_skipped` warning. |
//! | `set`, under `format: otlp_json` | skipped | `logit.output.metrics.skipped{metric_kind=set}` | As at otlp_out: OTLP has no cardinality type, so a set estimate isn't written, with a throttled `otlp_set_metric_skipped` warning. |
//! | `set`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `histogram/delta`, under `format: otlp_proto` | normalized | `inf_bucket` | As at otlp_out: An OTLP `Histogram` with delta temporality. OTLP requires an overflow count above the highest bound, so a histogram whose last bound is finite reads back with an empty `+Inf` bucket. |
//! | `histogram/delta`, under `format: otlp_json` | normalized | `inf_bucket` | As at otlp_out: An OTLP `Histogram` with delta temporality. OTLP requires an overflow count above the highest bound, so a histogram whose last bound is finite reads back with an empty `+Inf` bucket. |
//! | `histogram/delta`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `histogram/cumulative`, under `format: otlp_proto` | normalized | `inf_bucket` | As at otlp_out: An OTLP `Histogram` with cumulative temporality. OTLP requires an overflow count above the highest bound, so a histogram whose last bound is finite reads back with an empty `+Inf` bucket. |
//! | `histogram/cumulative`, under `format: otlp_json` | normalized | `inf_bucket` | As at otlp_out: An OTLP `Histogram` with cumulative temporality. OTLP requires an overflow count above the highest bound, so a histogram whose last bound is finite reads back with an empty `+Inf` bucket. |
//! | `histogram/cumulative`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `histogram/nan_bound`, under `format: otlp_proto` | degraded | `logit.output.metrics.degraded{reason=non_finite_component}` | As at otlp_out: OTLP bounds are finite, so a `NaN` bound's count joins the overflow bucket. `count` and every cumulative count are kept. |
//! | `histogram/nan_bound`, under `format: otlp_json` | degraded | `logit.output.metrics.degraded{reason=non_finite_component}` | As at otlp_out: OTLP bounds are finite, so a `NaN` bound's count joins the overflow bucket. `count` and every cumulative count are kept. |
//! | `histogram/nan_bound`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `histogram/neg_inf_bound`, under `format: otlp_proto` | degraded | `logit.output.metrics.degraded{reason=non_finite_component}` | As at otlp_out: OTLP bounds are finite, so a `-Inf` bound's count joins the first bucket. `count` and every cumulative count are kept. |
//! | `histogram/neg_inf_bound`, under `format: otlp_json` | degraded | `logit.output.metrics.degraded{reason=non_finite_component}` | As at otlp_out: OTLP bounds are finite, so a `-Inf` bound's count joins the first bucket. `count` and every cumulative count are kept. |
//! | `histogram/neg_inf_bound`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `histogram/gauge_histogram`, under `format: otlp_proto` | skipped | `logit.output.metrics.skipped{metric_kind=gauge_histogram}` | As at otlp_out: The OTel Prometheus compatibility spec drops a GaugeHistogram, since OTLP has no histogram that isn't a running count, so it isn't written, with a throttled `otlp_gauge_histogram_skipped` warning. |
//! | `histogram/gauge_histogram`, under `format: otlp_json` | skipped | `logit.output.metrics.skipped{metric_kind=gauge_histogram}` | As at otlp_out: The OTel Prometheus compatibility spec drops a GaugeHistogram, since OTLP has no histogram that isn't a running count, so it isn't written, with a throttled `otlp_gauge_histogram_skipped` warning. |
//! | `histogram/gauge_histogram`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `exponential_histogram/delta`, under `format: otlp_proto` | exact | -- | As at otlp_out: An OTLP `ExponentialHistogram`, field for field. |
//! | `exponential_histogram/delta`, under `format: otlp_json` | exact | -- | As at otlp_out: An OTLP `ExponentialHistogram`, field for field. |
//! | `exponential_histogram/delta`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `exponential_histogram/cumulative`, under `format: otlp_proto` | exact | -- | As at otlp_out: An OTLP `ExponentialHistogram`, field for field. |
//! | `exponential_histogram/cumulative`, under `format: otlp_json` | exact | -- | As at otlp_out: An OTLP `ExponentialHistogram`, field for field. |
//! | `exponential_histogram/cumulative`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `summary/delta`, under `format: otlp_proto` | dropped | `logit.output.metrics.dropped{reason=unresolved_delta}` | As at otlp_out: OTLP defines every summary as cumulative, so a delta one is dropped, with a throttled `delta_summary_unresolved` warning. |
//! | `summary/delta`, under `format: otlp_json` | dropped | `logit.output.metrics.dropped{reason=unresolved_delta}` | As at otlp_out: OTLP defines every summary as cumulative, so a delta one is dropped, with a throttled `delta_summary_unresolved` warning. |
//! | `summary/delta`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `summary/cumulative`, under `format: otlp_proto` | exact | -- | As at otlp_out: An OTLP `Summary`. |
//! | `summary/cumulative`, under `format: otlp_json` | exact | -- | As at otlp_out: An OTLP `Summary`. |
//! | `summary/cumulative`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `record/no_recorded_value`, under `format: otlp_proto` | exact | -- | As at otlp_out: The data point's `flags` carry `NO_RECORDED_VALUE`, and the point is kept. |
//! | `record/no_recorded_value`, under `format: otlp_json` | exact | -- | As at otlp_out: The data point's `flags` carry `NO_RECORDED_VALUE`, and the point is kept. |
//! | `record/no_recorded_value`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `record/no_recorded_value_delta_sum`, under `format: otlp_proto` | exact | -- | As at otlp_out: The data point's `flags` carry `NO_RECORDED_VALUE`, and the point is kept. |
//! | `record/no_recorded_value_delta_sum`, under `format: otlp_json` | exact | -- | As at otlp_out: The data point's `flags` carry `NO_RECORDED_VALUE`, and the point is kept. |
//! | `record/no_recorded_value_delta_sum`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `record/no_recorded_value_other_kind`, under `format: otlp_proto` | exact | -- | As at otlp_out: The data point's `flags` carry `NO_RECORDED_VALUE`, and the point is kept. |
//! | `record/no_recorded_value_other_kind`, under `format: otlp_json` | exact | -- | As at otlp_out: The data point's `flags` carry `NO_RECORDED_VALUE`, and the point is kept. |
//! | `record/no_recorded_value_other_kind`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `record/service_check`, under `format: otlp_proto` | exact | -- | As at otlp_out: The status gauge; the check's carriers aren't attributes. |
//! | `record/service_check`, under `format: otlp_json` | exact | -- | As at otlp_out: The status gauge; the check's carriers aren't attributes. |
//! | `record/service_check`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `record/sample_rate`, under `format: otlp_proto` | exact | -- | As at otlp_out: Each value is weighted by its inverse rate in the sketch the `samples` row describes. |
//! | `record/sample_rate`, under `format: otlp_json` | exact | -- | As at otlp_out: Each value is weighted by its inverse rate in the sketch the `samples` row describes. |
//! | `record/sample_rate`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `record/superseded`, under `format: otlp_proto` | exact | -- | As at otlp_out: Each record is its own data point. |
//! | `record/superseded`, under `format: otlp_json` | exact | -- | As at otlp_out: Each record is its own data point. |
//! | `record/superseded`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `record/same_instant`, under `format: otlp_proto` | exact | -- | As at otlp_out: Each record is its own data point. |
//! | `record/same_instant`, under `format: otlp_json` | exact | -- | As at otlp_out: Each record is its own data point. |
//! | `record/same_instant`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `record/same_instant_equal`, under `format: otlp_proto` | exact | -- | As at otlp_out: Each record is its own data point. |
//! | `record/same_instant_equal`, under `format: otlp_json` | exact | -- | As at otlp_out: Each record is its own data point. |
//! | `record/same_instant_equal`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `record/same_millisecond`, under `format: otlp_proto` | exact | -- | As at otlp_out: Each record is its own data point, at its own nanosecond. |
//! | `record/same_millisecond`, under `format: otlp_json` | exact | -- | As at otlp_out: Each record is its own data point, at its own nanosecond. |
//! | `record/same_millisecond`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `record/same_millisecond_equal`, under `format: otlp_proto` | exact | -- | As at otlp_out: Each record is its own data point, at its own nanosecond. |
//! | `record/same_millisecond_equal`, under `format: otlp_json` | exact | -- | As at otlp_out: Each record is its own data point, at its own nanosecond. |
//! | `record/same_millisecond_equal`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `record/non_finite`, under `format: otlp_proto` | exact | -- | As at otlp_out: The non-finite `as_double`. |
//! | `record/non_finite`, under `format: otlp_json` | exact | -- | As at otlp_out: The non-finite `as_double`. |
//! | `record/non_finite`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `log/dogstatsd_event`, under `format: text` | exact | -- | The event's text as the record's value; its title and the other `_logit.statsd.event.*` carriers are attributes, left off. |
//! | `log/dogstatsd_event`, under `format: otlp_proto` | exact | -- | As at otlp_out: An OTLP log record. Its `_logit.statsd.event.*` carriers aren't attributes; the `log` field rows have its fields. |
//! | `log/dogstatsd_event`, under `format: otlp_json` | exact | -- | As at otlp_out: An OTLP log record. Its `_logit.statsd.event.*` carriers aren't attributes; the `log` field rows have its fields. |
//! | `log/dogstatsd_event`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `log/plain`, under `format: text` | exact | -- | The log's message as the record's value, read back as one log per record. |
//! | `log/plain`, under `format: otlp_proto` | exact | -- | As at otlp_out: An OTLP log record; the `log` field rows have its fields. |
//! | `log/plain`, under `format: otlp_json` | exact | -- | As at otlp_out: An OTLP log record; the `log` field rows have its fields. |
//! | `log/plain`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `log/message_unsafe` | normalized | `message_as_text` | Under `format: text`, a message that isn't a string leaves as its text: a number or a boolean as its decimal or `true`/`false`, a timestamp as RFC 3339, no message as an empty record, and a map or array as its JSON text; each reads back as a string. `format: json` writes the message as it is. |
//! | `log/structured_message`, under `format: text` | normalized | `message_as_text` | A `Map` or `Array` message leaves as its JSON text, which reads back as a string. |
//! | `log/structured_message`, under `format: otlp_proto` | exact | -- | As at otlp_out: A `kvlistValue` or `arrayValue` body. |
//! | `log/structured_message`, under `format: otlp_json` | exact | -- | As at otlp_out: A `kvlistValue` or `arrayValue` body. |
//! | `log/structured_message`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `log/bytes_message`, under `format: text` | normalized | `message_as_text` | A `Bytes` message leaves as its raw bytes, the record's value, which carries no type, so a consumer that reads values as text sees a UTF-8 one as a string. |
//! | `log/bytes_message`, under `format: otlp_proto` | exact | -- | As at otlp_out: A `bytesValue` body. |
//! | `log/bytes_message`, under `format: otlp_json` | exact | -- | As at otlp_out: A `bytesValue` body. |
//! | `log/bytes_message`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `log/blank_message`, under `format: text` | normalized | `message_as_text` | An empty message leaves an empty record, and a log with no message one too, which reads back as the empty string. |
//! | `log/blank_message`, under `format: otlp_proto` | exact | -- | As at otlp_out: An empty string body, or a log record with no body. |
//! | `log/blank_message`, under `format: otlp_json` | exact | -- | As at otlp_out: An empty string body, or a log record with no body. |
//! | `log/blank_message`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `span/alone`, under `format: text` | skipped | `logit.output.events.skipped{reason=no_log}` | An event with no log has no `format: text` record. |
//! | `span/alone`, under `format: otlp_proto` | exact | -- | As at otlp_out: An OTLP span. |
//! | `span/alone`, under `format: otlp_json` | exact | -- | As at otlp_out: An OTLP span. |
//! | `span/alone`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `span/not_datadog_origin` | exact | -- | Nothing beyond the span's `span/alone`, `span/beside_log`, or `span/beside_metrics` outcome: only `datadog_out` gates a trace chunk. |
//! | `span/needs_agent_processing` | exact | -- | Nothing beyond the span's `span/alone`, `span/beside_log`, or `span/beside_metrics` outcome: only `datadog_out` gates a trace chunk. |
//! | `event/attributes_only`, under `format: text` | skipped | `logit.output.events.skipped{reason=no_log}` | An event with no log has no `format: text` record. |
//! | `event/attributes_only`, under `format: otlp_proto` | skipped | `logit.output.events.skipped{reason=no_payload}` | As at otlp_out: An event with no metric, log, or span has no OTLP signal and isn't sent. |
//! | `event/attributes_only`, under `format: otlp_json` | skipped | `logit.output.events.skipped{reason=no_payload}` | As at otlp_out: An event with no metric, log, or span has no OTLP signal and isn't sent. |
//! | `event/attributes_only`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `event/string_only`, under `format: text` | skipped | `logit.output.events.skipped{reason=no_log}` | An InfluxDB point whose fields are all strings has no log, so it has no `format: text` record. |
//! | `event/string_only`, under `format: otlp_proto` | skipped | `logit.output.events.skipped{reason=no_payload}` | As at otlp_out: An InfluxDB point whose fields are all strings has no OTLP signal and isn't sent, so its fields are lost. |
//! | `event/string_only`, under `format: otlp_json` | skipped | `logit.output.events.skipped{reason=no_payload}` | As at otlp_out: An InfluxDB point whose fields are all strings has no OTLP signal and isn't sent, so its fields are lost. |
//! | `event/string_only`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//!
//! Per field of a record that reaches the wire, attribute shape, and payload beside metrics:
//!
//! | Row | Class | Reason or counter | What a consumer sees |
//! |---|---|---|---|
//! | `record/unit`, under `format: otlp_proto` | exact | -- | As at otlp_out: The `Metric`'s `unit`. |
//! | `record/unit`, under `format: otlp_json` | exact | -- | As at otlp_out: The `Metric`'s `unit`. |
//! | `record/unit`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `record/description`, under `format: otlp_proto` | exact | -- | As at otlp_out: The `Metric`'s `description`. |
//! | `record/description`, under `format: otlp_json` | exact | -- | As at otlp_out: The `Metric`'s `description`. |
//! | `record/description`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `record/start_timestamp`, under `format: otlp_proto` | exact | -- | As at otlp_out: The data point's `start_time_unix_nano`. |
//! | `record/start_timestamp`, under `format: otlp_json` | exact | -- | As at otlp_out: The data point's `start_time_unix_nano`. |
//! | `record/start_timestamp`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `record/exemplars`, under `format: otlp_proto` | exact | -- | As at otlp_out: The data point's exemplars. |
//! | `record/exemplars`, under `format: otlp_json` | exact | -- | As at otlp_out: The data point's exemplars. |
//! | `record/exemplars`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `record/exemplars_on_summary`, under `format: otlp_proto` | lost | structural | As at otlp_out: A `SummaryDataPoint` has no exemplars field, so a summary's exemplars are left off. |
//! | `record/exemplars_on_summary`, under `format: otlp_json` | lost | structural | As at otlp_out: A `SummaryDataPoint` has no exemplars field, so a summary's exemplars are left off. |
//! | `record/exemplars_on_summary`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `record/min_max`, under `format: otlp_proto` | exact | -- | As at otlp_out: The data point's `min` and `max`. |
//! | `record/min_max`, under `format: otlp_json` | exact | -- | As at otlp_out: The data point's `min` and `max`. |
//! | `record/min_max`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/string_value`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/string_value`, under `format: otlp_proto` | exact | -- | As at otlp_out: A string attribute. |
//! | `attributes/string_value`, under `format: otlp_json` | exact | -- | As at otlp_out: A string attribute. |
//! | `attributes/string_value`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/host_beside_host_name`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/host_beside_host_name`, under `format: otlp_proto` | exact | -- | As at otlp_out: A string attribute. |
//! | `attributes/host_beside_host_name`, under `format: otlp_json` | exact | -- | As at otlp_out: A string attribute. |
//! | `attributes/host_beside_host_name`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/hostname`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/hostname`, under `format: otlp_proto` | exact | -- | As at otlp_out: A log attribute. |
//! | `attributes/hostname`, under `format: otlp_json` | exact | -- | As at otlp_out: A log attribute. |
//! | `attributes/hostname`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/log_service`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/log_service`, under `format: otlp_proto` | exact | -- | As at otlp_out: A log attribute. |
//! | `attributes/log_service`, under `format: otlp_json` | exact | -- | As at otlp_out: A log attribute. |
//! | `attributes/log_service`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/log_service_field`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/log_service_field`, under `format: otlp_proto` | exact | -- | As at otlp_out: A log attribute. |
//! | `attributes/log_service_field`, under `format: otlp_json` | exact | -- | As at otlp_out: A log attribute. |
//! | `attributes/log_service_field`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/container_id`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/container_id`, under `format: otlp_proto` | exact | -- | As at otlp_out: A string attribute. |
//! | `attributes/container_id`, under `format: otlp_json` | exact | -- | As at otlp_out: A string attribute. |
//! | `attributes/container_id`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/service_identity`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/service_identity`, under `format: otlp_proto` | exact | -- | As at otlp_out: A string attribute. |
//! | `attributes/service_identity`, under `format: otlp_json` | exact | -- | As at otlp_out: A string attribute. |
//! | `attributes/service_identity`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/service_tag`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/service_tag`, under `format: otlp_proto` | exact | -- | As at otlp_out: A string attribute. |
//! | `attributes/service_tag`, under `format: otlp_json` | exact | -- | As at otlp_out: A string attribute. |
//! | `attributes/service_tag`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/service_tag_beside_key`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/service_tag_beside_key`, under `format: otlp_proto` | exact | -- | As at otlp_out: A string attribute. |
//! | `attributes/service_tag_beside_key`, under `format: otlp_json` | exact | -- | As at otlp_out: A string attribute. |
//! | `attributes/service_tag_beside_key`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/bool_value`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/bool_value`, under `format: otlp_proto` | exact | -- | As at otlp_out: A boolean attribute. |
//! | `attributes/bool_value`, under `format: otlp_json` | exact | -- | As at otlp_out: A boolean attribute. |
//! | `attributes/bool_value`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/number_value`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/number_value`, under `format: otlp_proto` | normalized | `u64_as_int`; `logit.output.attributes.normalized{reason=u64_as_double}` | As at otlp_out: An integer or double attribute. A `u64` reads back as an `i64`, or, above `i64::MAX`, as a double, approximate beyond 2^53, counted once per such value. |
//! | `attributes/number_value`, under `format: otlp_json` | normalized | `u64_as_int`; `logit.output.attributes.normalized{reason=u64_as_double}` | As at otlp_out: An integer or double attribute. A `u64` reads back as an `i64`, or, above `i64::MAX`, as a double, approximate beyond 2^53, counted once per such value. |
//! | `attributes/number_value`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/array_value`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/array_value`, under `format: otlp_proto` | exact | -- | As at otlp_out: An array attribute. |
//! | `attributes/array_value`, under `format: otlp_json` | exact | -- | As at otlp_out: An array attribute. |
//! | `attributes/array_value`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/map_value`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/map_value`, under `format: otlp_proto` | exact | -- | As at otlp_out: A key-value list attribute. |
//! | `attributes/map_value`, under `format: otlp_json` | exact | -- | As at otlp_out: A key-value list attribute. |
//! | `attributes/map_value`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/bytes_value`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/bytes_value`, under `format: otlp_proto` | exact | -- | As at otlp_out: A bytes attribute. |
//! | `attributes/bytes_value`, under `format: otlp_json` | exact | -- | As at otlp_out: A bytes attribute. |
//! | `attributes/bytes_value`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/timestamp_value`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/timestamp_value`, under `format: otlp_proto` | normalized | `timestamp_as_int` | As at otlp_out: OTLP has no timestamp value, so it's an integer of Unix nanoseconds. |
//! | `attributes/timestamp_value`, under `format: otlp_json` | normalized | `timestamp_as_int` | As at otlp_out: OTLP has no timestamp value, so it's an integer of Unix nanoseconds. |
//! | `attributes/timestamp_value`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/null_value`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/null_value`, under `format: otlp_proto` | exact | -- | As at otlp_out: An `AnyValue` with no value set, read back as null. |
//! | `attributes/null_value`, under `format: otlp_json` | exact | -- | As at otlp_out: An `AnyValue` with no value set, read back as null. |
//! | `attributes/null_value`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/private_carrier` | normalized | `restamped` | kafka_out leaves `_logit.kafka.*` out of every record, and kafka_in stamps the relay's own record: its topic, its partition and offset where the relay's producer puts it, and the timestamp kafka_out gives it, the event's or under an OTLP format the time it encodes the request. A carrier reads back changed wherever the relay re-packs records. |
//! | `attributes/kafka_key_headers` | lost | uncounted | `_logit.kafka.key` and `_logit.kafka.headers`, a record's key and headers, aren't written back: kafka_out takes a record's key and headers from its `key:` and `headers:` config. |
//! | `attributes/prometheus_target`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/prometheus_target`, under `format: otlp_proto` | lost | structural | As at otlp_out: The scrape target is private and isn't an OTLP attribute, so it's left off. |
//! | `attributes/prometheus_target`, under `format: otlp_json` | lost | structural | As at otlp_out: The scrape target is private and isn't an OTLP attribute, so it's left off. |
//! | `attributes/prometheus_target`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/foreign_carrier`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/foreign_carrier`, under `format: otlp_proto` | lost | structural | As at otlp_out: A `_logit.` carrier is private and isn't an OTLP attribute, so it's left off. The `_logit.prometheus.type` carrier changes nothing; the `family_type` row has the `_logit.metrics.type` values the sink reads. |
//! | `attributes/foreign_carrier`, under `format: otlp_json` | lost | structural | As at otlp_out: A `_logit.` carrier is private and isn't an OTLP attribute, so it's left off. The `_logit.prometheus.type` carrier changes nothing; the `family_type` row has the `_logit.metrics.type` values the sink reads. |
//! | `attributes/foreign_carrier`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/timestamped`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/timestamped`, under `format: otlp_proto` | lost | structural | As at otlp_out: The flag is private and isn't an OTLP attribute, so it's left off. The point's time carries the timestamp. |
//! | `attributes/timestamped`, under `format: otlp_json` | lost | structural | As at otlp_out: The flag is private and isn't an OTLP attribute, so it's left off. The point's time carries the timestamp. |
//! | `attributes/timestamped`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/rate`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/rate`, under `format: otlp_proto` | lost | structural | As at otlp_out: The flag is private and isn't an OTLP attribute, so it's left off. |
//! | `attributes/rate`, under `format: otlp_json` | lost | structural | As at otlp_out: The flag is private and isn't an OTLP attribute, so it's left off. |
//! | `attributes/rate`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/interval`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/interval`, under `format: otlp_proto` | lost | structural | As at otlp_out: The flag is private and isn't an OTLP attribute, so it's left off. |
//! | `attributes/interval`, under `format: otlp_json` | lost | structural | As at otlp_out: The flag is private and isn't an OTLP attribute, so it's left off. |
//! | `attributes/interval`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/rate_on_non_gauge`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/rate_on_non_gauge`, under `format: otlp_proto` | lost | structural | As at otlp_out: The flag is private and isn't an OTLP attribute, so it's left off. |
//! | `attributes/rate_on_non_gauge`, under `format: otlp_json` | lost | structural | As at otlp_out: The flag is private and isn't an OTLP attribute, so it's left off. |
//! | `attributes/rate_on_non_gauge`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/family_type`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/family_type`, under `format: otlp_proto` | lost | structural | As at otlp_out: The flag is private and isn't an OTLP attribute, so it's left off. The sink reads it, writing an info or stateset gauge as a non-monotonic `Sum` and skipping a gauge histogram, by the `prometheus_info`, `prometheus_stateset`, and `gauge_histogram` rows. |
//! | `attributes/family_type`, under `format: otlp_json` | lost | structural | As at otlp_out: The flag is private and isn't an OTLP attribute, so it's left off. The sink reads it, writing an info or stateset gauge as a non-monotonic `Sum` and skipping a gauge histogram, by the `prometheus_info`, `prometheus_stateset`, and `gauge_histogram` rows. |
//! | `attributes/family_type`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/family_type_on_other_kind`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/family_type_on_other_kind`, under `format: otlp_proto` | lost | structural | As at otlp_out: The value names another kind, so it's ignored: the record keeps its own kind's OTLP type, and the flag is private and isn't an OTLP attribute. |
//! | `attributes/family_type_on_other_kind`, under `format: otlp_json` | lost | structural | As at otlp_out: The value names another kind, so it's ignored: the record keeps its own kind's OTLP type, and the flag is private and isn't an OTLP attribute. |
//! | `attributes/family_type_on_other_kind`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/interval_on_distribution`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/interval_on_distribution`, under `format: otlp_proto` | lost | structural | As at otlp_out: The flag is private and isn't an OTLP attribute, so it's left off. |
//! | `attributes/interval_on_distribution`, under `format: otlp_json` | lost | structural | As at otlp_out: The flag is private and isn't an OTLP attribute, so it's left off. |
//! | `attributes/interval_on_distribution`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/interval_on_histogram`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/interval_on_histogram`, under `format: otlp_proto` | lost | structural | As at otlp_out: The flag is private and isn't an OTLP attribute, so it's left off. |
//! | `attributes/interval_on_histogram`, under `format: otlp_json` | lost | structural | As at otlp_out: The flag is private and isn't an OTLP attribute, so it's left off. |
//! | `attributes/interval_on_histogram`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/resource_attribute`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the resource is left off. |
//! | `attributes/resource_attribute`, under `format: otlp_proto` | exact | -- | As at otlp_out: A resource attribute. |
//! | `attributes/resource_attribute`, under `format: otlp_json` | exact | -- | As at otlp_out: A resource attribute. |
//! | `attributes/resource_attribute`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/scope`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the scope is left off. |
//! | `attributes/scope`, under `format: otlp_proto` | exact | -- | As at otlp_out: The `InstrumentationScope` of the batch. |
//! | `attributes/scope`, under `format: otlp_json` | exact | -- | As at otlp_out: The `InstrumentationScope` of the batch. |
//! | `attributes/scope`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/datadog_carrier_log`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/datadog_carrier_log`, under `format: otlp_proto` | lost | structural | As at otlp_out: A `_logit.` carrier is private and isn't an OTLP attribute, so it's left off. |
//! | `attributes/datadog_carrier_log`, under `format: otlp_json` | lost | structural | As at otlp_out: A `_logit.` carrier is private and isn't an OTLP attribute, so it's left off. |
//! | `attributes/datadog_carrier_log`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/severity_carrier_otel`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/severity_carrier_otel`, under `format: otlp_proto` | exact | -- | As at otlp_out: The raw `severity_number` and `severity_text` the carriers hold, which otlp_in reads back into the same carriers beside the band's severity. |
//! | `attributes/severity_carrier_otel`, under `format: otlp_json` | exact | -- | As at otlp_out: The raw `severity_number` and `severity_text` the carriers hold, which otlp_in reads back into the same carriers beside the band's severity. |
//! | `attributes/severity_carrier_otel`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/severity_carrier_syslog`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/severity_carrier_syslog`, under `format: otlp_proto` | lost | structural | As at otlp_out: A `_logit.` carrier is private and isn't an OTLP attribute, so it's left off and the severity reads back at band resolution. |
//! | `attributes/severity_carrier_syslog`, under `format: otlp_json` | lost | structural | As at otlp_out: A `_logit.` carrier is private and isn't an OTLP attribute, so it's left off and the severity reads back at band resolution. |
//! | `attributes/severity_carrier_syslog`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/span_numeric`, under `format: otlp_proto` | normalized | `u64_as_int`; `logit.output.attributes.normalized{reason=u64_as_double}` | As at otlp_out: An integer or double span attribute. A `u64` reads back as an `i64`, or, above `i64::MAX`, as a double, counted once per such value. |
//! | `attributes/span_numeric`, under `format: otlp_json` | normalized | `u64_as_int`; `logit.output.attributes.normalized{reason=u64_as_double}` | As at otlp_out: An integer or double span attribute. A `u64` reads back as an `i64`, or, above `i64::MAX`, as a double, counted once per such value. |
//! | `attributes/span_numeric`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `attributes/datadog_span_identity`, under `format: otlp_proto` | exact | -- | As at otlp_out: A string span attribute. |
//! | `attributes/datadog_span_identity`, under `format: otlp_json` | exact | -- | As at otlp_out: A string span attribute. |
//! | `attributes/datadog_span_identity`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `attributes/datadog_span_marks`, under `format: otlp_proto` | exact | -- | As at otlp_out: An ordinary span attribute: Datadog's internal mark leaves as an OTel attribute. |
//! | `attributes/datadog_span_marks`, under `format: otlp_json` | exact | -- | As at otlp_out: An ordinary span attribute: Datadog's internal mark leaves as an OTel attribute. |
//! | `attributes/datadog_span_marks`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `attributes/datadog_carrier_span`, under `format: otlp_proto` | lost | structural | As at otlp_out: OTLP has no field for a Datadog trace chunk's fields or a span's raw `error`, and a `_logit.` carrier is private, so it's left off. |
//! | `attributes/datadog_carrier_span`, under `format: otlp_json` | lost | structural | As at otlp_out: OTLP has no field for a Datadog trace chunk's fields or a span's raw `error`, and a `_logit.` carrier is private, so it's left off. |
//! | `attributes/datadog_carrier_span`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `attributes/datadog_tracer_resource`, under `format: otlp_proto` | lost | structural | As at otlp_out: OTLP has no field for a `TracerPayload`'s fields, and a `_logit.` carrier is private, so it's left off. |
//! | `attributes/datadog_tracer_resource`, under `format: otlp_json` | lost | structural | As at otlp_out: OTLP has no field for a `TracerPayload`'s fields, and a `_logit.` carrier is private, so it's left off. |
//! | `attributes/datadog_tracer_resource`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `attributes/datadog_tracer_headers`, under `format: otlp_proto` | lost | structural | As at otlp_out: OTLP has no field for a tracer request header, and a `_logit.` carrier is private, so it's left off. |
//! | `attributes/datadog_tracer_headers`, under `format: otlp_json` | lost | structural | As at otlp_out: OTLP has no field for a tracer request header, and a `_logit.` carrier is private, so it's left off. |
//! | `attributes/datadog_tracer_headers`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `attributes/datadog_agent_resource`, under `format: otlp_proto` | lost | structural | As at otlp_out: OTLP has no field for an `AgentPayload`'s fields, and a `_logit.` carrier is private, so it's left off. |
//! | `attributes/datadog_agent_resource`, under `format: otlp_json` | lost | structural | As at otlp_out: OTLP has no field for an `AgentPayload`'s fields, and a `_logit.` carrier is private, so it's left off. |
//! | `attributes/datadog_agent_resource`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `attributes/syslog_header`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/syslog_header`, under `format: otlp_proto` | normalized | `u64_as_int` | As at otlp_out: A string or integer attribute; `intValue` is signed, so a `u64` `syslog.facility` or `process.pid` reads back as an `i64`. |
//! | `attributes/syslog_header`, under `format: otlp_json` | normalized | `u64_as_int` | As at otlp_out: A string or integer attribute; `intValue` is signed, so a `u64` `syslog.facility` or `process.pid` reads back as an `i64`. |
//! | `attributes/syslog_header`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/syslog_msgid`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/syslog_msgid`, under `format: otlp_proto` | exact | -- | As at otlp_out: A string attribute. |
//! | `attributes/syslog_msgid`, under `format: otlp_json` | exact | -- | As at otlp_out: A string attribute. |
//! | `attributes/syslog_msgid`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/syslog_sd`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/syslog_sd`, under `format: otlp_proto` | exact | -- | As at otlp_out: A key-value list attribute. |
//! | `attributes/syslog_sd`, under `format: otlp_json` | exact | -- | As at otlp_out: A key-value list attribute. |
//! | `attributes/syslog_sd`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/syslog_timestamp`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/syslog_timestamp`, under `format: otlp_proto` | normalized | `timestamp_as_int` | As at otlp_out: An RFC 3164 timestamp is a string attribute; OTLP has no timestamp value, so an RFC 5424 one is an integer of Unix nanoseconds. |
//! | `attributes/syslog_timestamp`, under `format: otlp_json` | normalized | `timestamp_as_int` | As at otlp_out: An RFC 3164 timestamp is a string attribute; OTLP has no timestamp value, so an RFC 5424 one is an integer of Unix nanoseconds. |
//! | `attributes/syslog_timestamp`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/host_name`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/host_name`, under `format: otlp_proto` | exact | -- | As at otlp_out: A string attribute. |
//! | `attributes/host_name`, under `format: otlp_json` | exact | -- | As at otlp_out: A string attribute. |
//! | `attributes/host_name`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/splunk_envelope`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the attribute is left off, as every attribute is. |
//! | `attributes/splunk_envelope`, under `format: otlp_proto` | exact | -- | As at otlp_out: A resource attribute. |
//! | `attributes/splunk_envelope`, under `format: otlp_json` | exact | -- | As at otlp_out: A resource attribute. |
//! | `attributes/splunk_envelope`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `attributes/syslog_facility` | exact | -- | `kafka_out` writes no attribute from a record's own fields or its config. |
//! | `attributes/no_host` | exact | -- | `kafka_out` writes no attribute from a record's own fields or its config. |
//! | `attributes/default_tag` | exact | -- | `kafka_out` writes no attribute from a record's own fields or its config. |
//! | `attributes/log_status` | exact | -- | `kafka_out` writes no attribute from a record's own fields or its config. |
//! | `attributes/log_severity_fields` | exact | -- | `kafka_out` writes no attribute from a record's own fields or its config. |
//! | `attributes/span_kind` | exact | -- | `kafka_out` writes no attribute from a record's own fields or its config. |
//! | `attributes/trace_id_high` | exact | -- | `kafka_out` writes no attribute from a record's own fields or its config. |
//! | `log/severity`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the log's severity is left off. |
//! | `log/severity`, under `format: otlp_proto` | exact | -- | As at otlp_out: The band's `severity_number` and text, or the raw ones the `_logit.otel.severity_*` carriers hold, read back at band resolution. |
//! | `log/severity`, under `format: otlp_json` | exact | -- | As at otlp_out: The band's `severity_number` and text, or the raw ones the `_logit.otel.severity_*` carriers hold, read back at band resolution. |
//! | `log/severity`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `log/severity_unset`, under `format: text` | exact | -- | Nothing is written, and nothing reads back. |
//! | `log/severity_unset`, under `format: otlp_proto` | exact | -- | As at otlp_out: `SEVERITY_NUMBER_UNSPECIFIED` and no text, read back as no severity. |
//! | `log/severity_unset`, under `format: otlp_json` | exact | -- | As at otlp_out: `SEVERITY_NUMBER_UNSPECIFIED` and no text, read back as no severity. |
//! | `log/severity_unset`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `log/body_format`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the log's body format is left off. |
//! | `log/body_format`, under `format: otlp_proto` | exact | -- | As at otlp_out: OTLP has no body format, so it leaves as the `logit.body_format` attribute, which otlp_in reads back into the format. |
//! | `log/body_format`, under `format: otlp_json` | exact | -- | As at otlp_out: OTLP has no body format, so it leaves as the `logit.body_format` attribute, which otlp_in reads back into the format. |
//! | `log/body_format`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `log/event_name`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the log's event name is left off. |
//! | `log/event_name`, under `format: otlp_proto` | exact | -- | As at otlp_out: The `LogRecord`'s `event_name`. |
//! | `log/event_name`, under `format: otlp_json` | exact | -- | As at otlp_out: The `LogRecord`'s `event_name`. |
//! | `log/event_name`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `log/observed_timestamp`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the log's observed time is left off. |
//! | `log/observed_timestamp`, under `format: otlp_proto` | exact | -- | As at otlp_out: The `LogRecord`'s `observed_time_unix_nano`. |
//! | `log/observed_timestamp`, under `format: otlp_json` | exact | -- | As at otlp_out: The `LogRecord`'s `observed_time_unix_nano`. |
//! | `log/observed_timestamp`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `log/observed_timestamp_unset`, under `format: text` | exact | -- | Nothing is written, and nothing reads back. |
//! | `log/observed_timestamp_unset`, under `format: otlp_proto` | normalized | `observed_timestamp` | As at otlp_out: otlp_out stamps a log that has no observed time with the time it encodes the request, as `logs.proto` defines the field. |
//! | `log/observed_timestamp_unset`, under `format: otlp_json` | normalized | `observed_timestamp` | As at otlp_out: otlp_out stamps a log that has no observed time with the time it encodes the request, as `logs.proto` defines the field. |
//! | `log/observed_timestamp_unset`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `log/trace`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the log's trace and span ids are left off. |
//! | `log/trace`, under `format: otlp_proto` | exact | -- | As at otlp_out: The `LogRecord`'s `trace_id` and `span_id`. |
//! | `log/trace`, under `format: otlp_json` | exact | -- | As at otlp_out: The `LogRecord`'s `trace_id` and `span_id`. |
//! | `log/trace`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `log/trace_unset`, under `format: text` | exact | -- | Nothing is written, and nothing reads back. |
//! | `log/trace_unset`, under `format: otlp_proto` | normalized | `trace_from_span` | As at otlp_out: A log with no trace on an event that carries a span leaves with the span's ids and flags `0`; without a span, the log leaves with no trace. |
//! | `log/trace_unset`, under `format: otlp_json` | normalized | `trace_from_span` | As at otlp_out: A log with no trace on an event that carries a span leaves with the span's ids and flags `0`; without a span, the log leaves with no trace. |
//! | `log/trace_unset`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `log/trace_flags`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the log's trace flags are left off. |
//! | `log/trace_flags`, under `format: otlp_proto` | exact | -- | As at otlp_out: The low 8 bits of the `LogRecord`'s `flags`. |
//! | `log/trace_flags`, under `format: otlp_json` | exact | -- | As at otlp_out: The low 8 bits of the `LogRecord`'s `flags`. |
//! | `log/trace_flags`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `log/trace_beside_id_attribute`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the log's trace is left off, as `log/trace` has. |
//! | `log/trace_beside_id_attribute`, under `format: otlp_proto` | exact | -- | As at otlp_out: The `LogRecord`'s trace, span ids, and flags, beside the attributes, as `log/trace` has. |
//! | `log/trace_beside_id_attribute`, under `format: otlp_json` | exact | -- | As at otlp_out: The `LogRecord`'s trace, span ids, and flags, beside the attributes, as `log/trace` has. |
//! | `log/trace_beside_id_attribute`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `log/dropped_attributes_count`, under `format: text` | lost | structural | A `format: text` record holds the log body alone, so the log's dropped-attribute count is left off. |
//! | `log/dropped_attributes_count`, under `format: otlp_proto` | exact | -- | As at otlp_out: The `LogRecord`'s `dropped_attributes_count`. |
//! | `log/dropped_attributes_count`, under `format: otlp_json` | exact | -- | As at otlp_out: The `LogRecord`'s `dropped_attributes_count`. |
//! | `log/dropped_attributes_count`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `log/timestamp`, under `format: text` | normalized | `timestamp_precision` | The record's timestamp, in whole milliseconds. |
//! | `log/timestamp`, under `format: otlp_proto` | exact | -- | As at otlp_out: The `LogRecord`'s `time_unix_nano`. |
//! | `log/timestamp`, under `format: otlp_json` | exact | -- | As at otlp_out: The `LogRecord`'s `time_unix_nano`. |
//! | `log/timestamp`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `log/receipt_timestamp`, under `format: text` | normalized | `timestamp_precision` | The record's timestamp, the receipt time in whole milliseconds; the `syslog.timestamp` beside it is an attribute, left off. |
//! | `log/receipt_timestamp`, under `format: otlp_proto` | exact | -- | As at otlp_out: The `LogRecord`'s `time_unix_nano`, the time the log was received. |
//! | `log/receipt_timestamp`, under `format: otlp_json` | exact | -- | As at otlp_out: The `LogRecord`'s `time_unix_nano`, the time the log was received. |
//! | `log/receipt_timestamp`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `log/beside_metrics`, under `format: text` | exact | `logit.output.metrics.omitted{reason=format}` | The log's body as the record; the event's metrics are left off, counted. |
//! | `log/beside_metrics`, under `format: otlp_proto` | exact | -- | As at otlp_out: An OTLP log record in the logs request, beside the metrics in theirs. |
//! | `log/beside_metrics`, under `format: otlp_json` | exact | -- | As at otlp_out: An OTLP log record in the logs request, beside the metrics in theirs. |
//! | `log/beside_metrics`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `span/name`, under `format: otlp_proto` | exact | -- | As at otlp_out: The span's `name`. |
//! | `span/name`, under `format: otlp_json` | exact | -- | As at otlp_out: The span's `name`. |
//! | `span/name`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/name_structured`, under `format: otlp_proto` | lost | uncounted | As at otlp_out: OTLP's span `name` is a string, and a name that isn't a `Str` leaves as `""`, uncounted. |
//! | `span/name_structured`, under `format: otlp_json` | lost | uncounted | As at otlp_out: OTLP's span `name` is a string, and a name that isn't a `Str` leaves as `""`, uncounted. |
//! | `span/name_structured`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/trace_id`, under `format: otlp_proto` | exact | -- | As at otlp_out: The 16-byte `trace_id`. |
//! | `span/trace_id`, under `format: otlp_json` | exact | -- | As at otlp_out: The 16-byte `trace_id`. |
//! | `span/trace_id`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/parent_span_id`, under `format: otlp_proto` | exact | -- | As at otlp_out: The 8-byte `parent_span_id`. |
//! | `span/parent_span_id`, under `format: otlp_json` | exact | -- | As at otlp_out: The 8-byte `parent_span_id`. |
//! | `span/parent_span_id`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/parent_unset`, under `format: otlp_proto` | exact | -- | As at otlp_out: An empty `parent_span_id`, read back as no parent. |
//! | `span/parent_unset`, under `format: otlp_json` | exact | -- | As at otlp_out: An empty `parent_span_id`, read back as no parent. |
//! | `span/parent_unset`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/kind`, under `format: otlp_proto` | exact | -- | As at otlp_out: The `SpanKind`. |
//! | `span/kind`, under `format: otlp_json` | exact | -- | As at otlp_out: The `SpanKind`. |
//! | `span/kind`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/status_error`, under `format: otlp_proto` | exact | -- | As at otlp_out: `Status.code` `STATUS_CODE_ERROR`. |
//! | `span/status_error`, under `format: otlp_json` | exact | -- | As at otlp_out: `Status.code` `STATUS_CODE_ERROR`. |
//! | `span/status_error`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/status_ok`, under `format: otlp_proto` | exact | -- | As at otlp_out: `Status.code` `STATUS_CODE_OK`. |
//! | `span/status_ok`, under `format: otlp_json` | exact | -- | As at otlp_out: `Status.code` `STATUS_CODE_OK`. |
//! | `span/status_ok`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/status_unset`, under `format: otlp_proto` | exact | -- | As at otlp_out: `Status.code` `STATUS_CODE_UNSET`, read back as unset. |
//! | `span/status_unset`, under `format: otlp_json` | exact | -- | As at otlp_out: `Status.code` `STATUS_CODE_UNSET`, read back as unset. |
//! | `span/status_unset`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/status_message`, under `format: otlp_proto` | exact | -- | As at otlp_out: `Status.message`. |
//! | `span/status_message`, under `format: otlp_json` | exact | -- | As at otlp_out: `Status.message`. |
//! | `span/status_message`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/timestamp`, under `format: otlp_proto` | exact | -- | As at otlp_out: `start_time_unix_nano`. |
//! | `span/timestamp`, under `format: otlp_json` | exact | -- | As at otlp_out: `start_time_unix_nano`. |
//! | `span/timestamp`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/end_timestamp`, under `format: otlp_proto` | exact | -- | As at otlp_out: `end_time_unix_nano`. |
//! | `span/end_timestamp`, under `format: otlp_json` | exact | -- | As at otlp_out: `end_time_unix_nano`. |
//! | `span/end_timestamp`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/end_before_start`, under `format: otlp_proto` | exact | -- | As at otlp_out: `end_time_unix_nano` as it is, earlier than the start. |
//! | `span/end_before_start`, under `format: otlp_json` | exact | -- | As at otlp_out: `end_time_unix_nano` as it is, earlier than the start. |
//! | `span/end_before_start`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/flags`, under `format: otlp_proto` | exact | -- | As at otlp_out: The span's `flags`. |
//! | `span/flags`, under `format: otlp_json` | exact | -- | As at otlp_out: The span's `flags`. |
//! | `span/flags`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/trace_state`, under `format: otlp_proto` | exact | -- | As at otlp_out: The span's `trace_state`. |
//! | `span/trace_state`, under `format: otlp_json` | exact | -- | As at otlp_out: The span's `trace_state`. |
//! | `span/trace_state`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/dropped_attributes_count`, under `format: otlp_proto` | exact | -- | As at otlp_out: The span's `dropped_attributes_count`. |
//! | `span/dropped_attributes_count`, under `format: otlp_json` | exact | -- | As at otlp_out: The span's `dropped_attributes_count`. |
//! | `span/dropped_attributes_count`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/dropped_events_count`, under `format: otlp_proto` | exact | -- | As at otlp_out: The span's `dropped_events_count`. |
//! | `span/dropped_events_count`, under `format: otlp_json` | exact | -- | As at otlp_out: The span's `dropped_events_count`. |
//! | `span/dropped_events_count`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/dropped_links_count`, under `format: otlp_proto` | exact | -- | As at otlp_out: The span's `dropped_links_count`. |
//! | `span/dropped_links_count`, under `format: otlp_json` | exact | -- | As at otlp_out: The span's `dropped_links_count`. |
//! | `span/dropped_links_count`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/events`, under `format: otlp_proto` | exact | -- | As at otlp_out: Each span event's time, name, and attributes. |
//! | `span/events`, under `format: otlp_json` | exact | -- | As at otlp_out: Each span event's time, name, and attributes. |
//! | `span/events`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/event_nested_attribute`, under `format: otlp_proto` | exact | -- | As at otlp_out: A key-value list, array, or empty attribute of a span event, as OTLP writes any attribute. |
//! | `span/event_nested_attribute`, under `format: otlp_json` | exact | -- | As at otlp_out: A key-value list, array, or empty attribute of a span event, as OTLP writes any attribute. |
//! | `span/event_nested_attribute`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/event_name_structured`, under `format: otlp_proto` | lost | uncounted | As at otlp_out: OTLP's span event `name` is a string, and a name that isn't a `Str` leaves as `""`, uncounted. |
//! | `span/event_name_structured`, under `format: otlp_json` | lost | uncounted | As at otlp_out: OTLP's span event `name` is a string, and a name that isn't a `Str` leaves as `""`, uncounted. |
//! | `span/event_name_structured`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/nested_bytes_attribute`, under `format: otlp_proto` | exact | -- | As at otlp_out: A `bytesValue` attribute of a span event or link. |
//! | `span/nested_bytes_attribute`, under `format: otlp_json` | exact | -- | As at otlp_out: A `bytesValue` attribute of a span event or link. |
//! | `span/nested_bytes_attribute`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/nested_u64_attribute`, under `format: otlp_proto` | normalized | `u64_as_int` | As at otlp_out: A `u64` attribute of a span event or link reads back as an `i64`, or, above `i64::MAX`, as a double, approximate beyond 2^53 and uncounted. |
//! | `span/nested_u64_attribute`, under `format: otlp_json` | normalized | `u64_as_int` | As at otlp_out: A `u64` attribute of a span event or link reads back as an `i64`, or, above `i64::MAX`, as a double, approximate beyond 2^53 and uncounted. |
//! | `span/nested_u64_attribute`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/nested_timestamp_attribute`, under `format: otlp_proto` | normalized | `timestamp_as_int` | As at otlp_out: A timestamp attribute of a span event or link leaves as an `intValue` of Unix nanoseconds, read back as an `i64`. |
//! | `span/nested_timestamp_attribute`, under `format: otlp_json` | normalized | `timestamp_as_int` | As at otlp_out: A timestamp attribute of a span event or link leaves as an `intValue` of Unix nanoseconds, read back as an `i64`. |
//! | `span/nested_timestamp_attribute`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/event_dropped_count`, under `format: otlp_proto` | exact | -- | As at otlp_out: A span event's `dropped_attributes_count`. |
//! | `span/event_dropped_count`, under `format: otlp_json` | exact | -- | As at otlp_out: A span event's `dropped_attributes_count`. |
//! | `span/event_dropped_count`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/links`, under `format: otlp_proto` | exact | -- | As at otlp_out: Each link's ids and attributes. |
//! | `span/links`, under `format: otlp_json` | exact | -- | As at otlp_out: Each link's ids and attributes. |
//! | `span/links`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/link_flags`, under `format: otlp_proto` | exact | -- | As at otlp_out: A link's `flags`. |
//! | `span/link_flags`, under `format: otlp_json` | exact | -- | As at otlp_out: A link's `flags`. |
//! | `span/link_flags`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/link_trace_state`, under `format: otlp_proto` | exact | -- | As at otlp_out: A link's `trace_state`. |
//! | `span/link_trace_state`, under `format: otlp_json` | exact | -- | As at otlp_out: A link's `trace_state`. |
//! | `span/link_trace_state`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/link_dropped_count`, under `format: otlp_proto` | exact | -- | As at otlp_out: A link's `dropped_attributes_count`. |
//! | `span/link_dropped_count`, under `format: otlp_json` | exact | -- | As at otlp_out: A link's `dropped_attributes_count`. |
//! | `span/link_dropped_count`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. A `format: text` record writes no span (`span/alone`), so no field of one is compared. |
//! | `span/beside_log`, under `format: text` | lost | `logit.output.spans.omitted{reason=format}` | The record holds the log body alone, so the span beside it is left off, counted. |
//! | `span/beside_log`, under `format: otlp_proto` | exact | -- | As at otlp_out: The span in the traces request and the log in the logs request, each read back as an event of its own. |
//! | `span/beside_log`, under `format: otlp_json` | exact | -- | As at otlp_out: The span in the traces request and the log in the logs request, each read back as an event of its own. |
//! | `span/beside_log`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//! | `span/beside_metrics`, under `format: text` | lost | `logit.output.events.skipped{reason=no_log}` | An event with no log has no `format: text` record, so the span and its metrics are left off, counted once for the event. |
//! | `span/beside_metrics`, under `format: otlp_proto` | exact | -- | As at otlp_out: An OTLP span in the traces request, beside the metrics in theirs. |
//! | `span/beside_metrics`, under `format: otlp_json` | exact | -- | As at otlp_out: An OTLP span in the traces request, beside the metrics in theirs. |
//! | `span/beside_metrics`, otherwise | exact | -- | Under `json`, `msgpack`, and `native`, the event whole, as `stdio_out`'s JSON object, its MessagePack map, or a native frame, read back whole by kafka_in. |
//!
//! <!-- transit:end -->

use std::collections::HashMap;
use std::fmt::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use logit_core::interner::intern;
use logit_core::time::write_rfc3339_utc;
use logit_core::trace::push_hex;
use logit_core::{
    CountGate, Diagnostics, Event, EventBatch, Resource, Scope, Symbol, Telemetry, Value,
};
use logit_proto::frame::{write_frame, Compression};
use logit_proto::kafka::text::escape_line_into;
use logit_proto::msgpack::Writer;
use logit_proto::native::{encode_events_omitting, CODEC_BATCH};
use logit_proto::otlp::common::u64_as_double_count;
use logit_proto::otlp::{OtlpEncoder, OtlpRequest};
use logit_proto::{FramedEncoder, MessageBuf, Signal};

use super::CARRIER_PREFIX;

/// What each record's key is. Mirrors `logit_config::KafkaKey`, which `logit-cli` converts.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum RecordKey {
    #[default]
    None,
    TraceId,
    /// A top-level event attribute, named literally.
    Attribute(String),
    /// A resource attribute.
    Resource(String),
}

/// What each record's value holds. Mirrors `logit_config::KafkaFormat`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RecordFormat {
    #[default]
    Json,
    Text,
    Msgpack,
    Native,
    OtlpProto,
    OtlpJson,
}

impl RecordFormat {
    /// The record granularity a format has when `record:` isn't set.
    pub fn default_record(self) -> RecordMode {
        match self {
            RecordFormat::Json | RecordFormat::Text | RecordFormat::Msgpack => RecordMode::Event,
            RecordFormat::Native => RecordMode::Batch,
            RecordFormat::OtlpProto | RecordFormat::OtlpJson => RecordMode::Request,
        }
    }

    /// Whether each record holds one OTLP export request, a unit `record:` can't change.
    pub fn is_otlp(self) -> bool {
        self.default_record() == RecordMode::Request
    }
}

/// How many events one record holds. Mirrors `logit_config::KafkaRecord`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordMode {
    /// One event per record.
    Event,
    /// As many of a batch's events as fit under `max_request_bytes`.
    Batch,
    /// One OTLP export request of one signal: the OTLP formats' own unit, which `record:` can't
    /// select.
    Request,
}

/// [`RecordKey`] with its attribute name interned once.
#[derive(Debug, Clone, Copy)]
enum Compiled {
    None,
    TraceId,
    Attribute(Symbol),
    Resource(Symbol),
}

/// A record's key, timestamp, and signal; its value is the message's bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordMeta {
    pub key: Option<Bytes>,
    /// `CreateTime`, in milliseconds since the Unix epoch.
    pub timestamp: i64,
    /// The signal of an OTLP format's request, which picks its topic; `None` for the other
    /// formats, whose records all go to `topic:`.
    pub signal: Option<Signal>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct EncodeStats {
    /// Events sent unkeyed because a configured key had no value on them.
    pub missing_key: usize,
    /// Events `format: text` skipped for having no log record.
    pub no_log: usize,
    /// Events an OTLP format skipped for carrying no log, span, or metric, which no request
    /// holds, as `otlp_out` skips one.
    pub no_payload: usize,
    /// Attribute values an OTLP format wrote as a double for being a `u64` above `i64::MAX`, on
    /// the events it wrote and the batch's resource and scope, once per batch as `otlp_out`
    /// counts them.
    pub u64_as_double: usize,
    /// Metric records `format: text` left out of a record it wrote from the event's log.
    pub metrics_omitted: usize,
    /// Spans `format: text` left out of a record it wrote from the event's log.
    pub spans_omitted: usize,
    /// Events dropped under `record: batch` because their value alone is over the budget; under
    /// an OTLP format, one per signal of an event whose request alone is over it.
    pub oversize: usize,
}

/// `kafka_out`'s [`FramedEncoder`]. Public for the allocation pins in `logit-bench`.
pub struct KafkaEncoder {
    key: Compiled,
    format: RecordFormat,
    record: RecordMode,
    /// The most bytes one `record: batch` value may hold.
    budget: usize,
    /// A key's text; reused across events and batches.
    scratch: String,
    render: Render,
    /// The `record: batch` value being filled; reused.
    value: Vec<u8>,
    /// The OTLP mapping, on handles gated by `gate`.
    otlp: OtlpEncoder,
    /// Muted around every OTLP encode that repeats an event's mapping, so the mapping's
    /// side-effect counters count each event once.
    gate: CountGate,
}

/// One event's value for the per-event formats, in buffers reused across events and batches.
#[derive(Default)]
struct Render {
    text: String,
    body: Vec<u8>,
    /// `body` in the batch line form.
    escaped: Vec<u8>,
    msgpack: Writer,
    msgpack_scratch: String,
}

impl Render {
    /// `event`'s value under `format`, or `None` for an event `format: text` skips. `newline`
    /// asks for the `record: batch` line form: a JSON object ends with `\n`, as an NDJSON line,
    /// and a text body is escaped.
    fn value(
        &mut self,
        format: RecordFormat,
        newline: bool,
        resource: &Resource,
        scope: Option<&Scope>,
        event: &Event,
    ) -> Option<&[u8]> {
        match format {
            RecordFormat::Json => {
                self.text.clear();
                let omit = Some(CARRIER_PREFIX);
                crate::ndjson::render_event(&mut self.text, resource, scope, event, omit);
                if newline {
                    self.text.push('\n');
                }
                Some(self.text.as_bytes())
            }
            RecordFormat::Msgpack => {
                self.msgpack.clear();
                crate::msgpack::render_event(
                    &mut self.msgpack,
                    &mut self.msgpack_scratch,
                    resource,
                    scope,
                    event,
                    Some(CARRIER_PREFIX),
                );
                Some(self.msgpack.as_slice())
            }
            RecordFormat::Text => {
                let log = event.log.as_ref()?;
                self.body.clear();
                push_body(&mut self.body, &mut self.text, &log.message);
                if !newline {
                    return Some(&self.body);
                }
                self.escaped.clear();
                escape_line_into(&mut self.escaped, &self.body);
                Some(&self.escaped)
            }
            RecordFormat::Native => unreachable!("native frames go through `native_frame`"),
            RecordFormat::OtlpProto | RecordFormat::OtlpJson => {
                unreachable!("OTLP requests go through `otlp`")
            }
        }
    }
}

/// The value `format: text` writes for a log whose message is `message`, by the module doc's
/// table.
pub fn text_body(message: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    push_body(&mut out, &mut String::new(), message);
    out
}

/// A log body's text, by the module doc's table.
fn push_body(out: &mut Vec<u8>, text: &mut String, message: &Value) {
    text.clear();
    match message {
        Value::Null => {}
        Value::Str(b) | Value::Bytes(b) => return out.extend_from_slice(b),
        Value::Bool(b) => {
            let _ = write!(text, "{b}");
        }
        Value::I64(n) => {
            let _ = write!(text, "{n}");
        }
        Value::U64(n) => {
            let _ = write!(text, "{n}");
        }
        Value::F64(n) => {
            let _ = write!(text, "{n}");
        }
        Value::Timestamp(ns) => write_rfc3339_utc(text, *ns),
        Value::Array(_) | Value::Map(_) => crate::ndjson::render_value(text, message),
    }
    out.extend_from_slice(text.as_bytes());
}

/// One `CODEC_BATCH` frame of `events`, or `None` past the frame's own payload cap.
fn native_frame(resource: &Resource, scope: Option<&Scope>, events: &[Event]) -> Option<Bytes> {
    let payload = encode_events_omitting(resource, scope, events, CARRIER_PREFIX);
    write_frame(CODEC_BATCH, Compression::None, &payload).ok()
}

fn millis(nanos: i64) -> i64 {
    nanos.div_euclid(1_000_000)
}

impl KafkaEncoder {
    /// `budget` caps a `record: batch` value; `record: event` leaves the cap to the producer.
    pub fn new(key: &RecordKey, format: RecordFormat, record: RecordMode, budget: usize) -> Self {
        let key = match key {
            RecordKey::None => Compiled::None,
            RecordKey::TraceId => Compiled::TraceId,
            RecordKey::Attribute(name) => Compiled::Attribute(intern(name)),
            RecordKey::Resource(name) => Compiled::Resource(intern(name)),
        };
        Self {
            key,
            format,
            record,
            budget,
            scratch: String::new(),
            render: Render::default(),
            value: Vec::new(),
            otlp: OtlpEncoder::new(),
            gate: CountGate::new(),
        }
    }

    /// Counts the OTLP mapping's side effects (`logit.output.metrics.degraded`, `skipped`, and
    /// `dropped`, and their warnings) through views of `telemetry` and `diag` gated by `gate`, the sink's
    /// batch accounting gate.
    pub fn with_counters(
        mut self,
        telemetry: &Telemetry,
        diag: &Diagnostics,
        gate: &CountGate,
    ) -> Self {
        self.otlp = OtlpEncoder::new()
            .with_telemetry(telemetry.gated(gate))
            .with_diagnostics(diag.gated(gate));
        self.gate = gate.clone();
        self
    }

    /// The key bytes for `event`; `Err(())` when a configured key has no value on it.
    fn key(&mut self, resource: &Resource, event: &Event) -> Result<Option<Bytes>, ()> {
        let value = match self.key {
            Compiled::None => return Ok(None),
            Compiled::TraceId => {
                let id = event
                    .span
                    .as_ref()
                    .map(|span| &span.trace_id)
                    .or_else(|| event.log.as_ref()?.trace.as_ref().map(|t| &t.trace_id))
                    .filter(|id| id.iter().any(|&b| b != 0))
                    .ok_or(())?;
                self.scratch.clear();
                push_hex(&mut self.scratch, id);
                return Ok(Some(Bytes::copy_from_slice(self.scratch.as_bytes())));
            }
            Compiled::Attribute(sym) => event.attributes.get_sym(sym),
            Compiled::Resource(sym) => resource.attributes.get_sym(sym),
        };
        key_bytes(value.ok_or(())?, &mut self.scratch).map(Some)
    }

    fn keyed_meta(
        &mut self,
        resource: &Resource,
        event: &Event,
        stats: &mut EncodeStats,
    ) -> RecordMeta {
        let key = self.key(resource, event).unwrap_or_else(|()| {
            stats.missing_key += 1;
            None
        });
        RecordMeta { key, timestamp: millis(event.timestamp), signal: None }
    }

    /// `record: event`: one record per event.
    fn per_event(
        &mut self,
        batch: &EventBatch,
        out: &mut MessageBuf<RecordMeta>,
        stats: &mut EncodeStats,
    ) {
        let scope = batch.scope.as_deref();
        for event in &batch.events {
            if self.format == RecordFormat::Text && event.log.is_none() {
                stats.no_log += 1;
                continue;
            }
            let meta = self.keyed_meta(&batch.resource, event, stats);
            if self.format == RecordFormat::Native {
                // A frame past its payload cap is far over any `max_request_bytes`: dropped, as
                // the producer would drop it.
                match native_frame(&batch.resource, scope, std::slice::from_ref(event)) {
                    Some(frame) => out.push_with(&frame, meta),
                    None => stats.oversize += 1,
                }
                continue;
            }
            let value = self
                .render
                .value(self.format, false, &batch.resource, scope, event)
                .expect("a text event here has a log record");
            out.push_with(value, meta);
            self.count_omitted(event, stats);
        }
    }

    /// Under `format: text`, counts the metrics and span `event` carries beside the log body
    /// its record holds.
    fn count_omitted(&self, event: &Event, stats: &mut EncodeStats) {
        if self.format == RecordFormat::Text {
            stats.metrics_omitted += event.metrics.len();
            stats.spans_omitted += usize::from(event.span.is_some());
        }
    }

    /// `record: batch` for `json`, `text`, and `msgpack`: values appended in batch order until
    /// the next would push the record over the budget.
    fn packed(
        &mut self,
        batch: &EventBatch,
        out: &mut MessageBuf<RecordMeta>,
        stats: &mut EncodeStats,
    ) {
        let scope = batch.scope.as_deref();
        let joined = self.format == RecordFormat::Text;
        let mut count = 0;
        let mut timestamp = 0;
        self.value.clear();
        for event in &batch.events {
            let Some(piece) = self.render.value(self.format, true, &batch.resource, scope, event)
            else {
                stats.no_log += 1;
                continue;
            };
            if piece.len() > self.budget {
                stats.oversize += 1;
                continue;
            }
            let separator = usize::from(joined && count > 0);
            if count > 0 && self.value.len() + separator + piece.len() > self.budget {
                out.push_with(&self.value, RecordMeta { key: None, timestamp, signal: None });
                self.value.clear();
                count = 0;
            }
            if count == 0 {
                timestamp = millis(event.timestamp);
            } else if joined {
                self.value.push(b'\n');
            }
            self.value.extend_from_slice(piece);
            count += 1;
            self.count_omitted(event, stats);
        }
        if count > 0 {
            out.push_with(&self.value, RecordMeta { key: None, timestamp, signal: None });
        }
    }

    /// `record: batch` for `native`: each record the longest run of the remaining events whose
    /// frame fits the budget. A frame only grows as events are added, so the search is sound.
    fn native_packed(
        &self,
        batch: &EventBatch,
        out: &mut MessageBuf<RecordMeta>,
        stats: &mut EncodeStats,
    ) {
        let scope = batch.scope.as_deref();
        let fits = |events: &[Event]| {
            native_frame(&batch.resource, scope, events).filter(|f| f.len() <= self.budget)
        };
        let mut rest = &batch.events[..];
        let Some(first) = rest.first() else { return };
        if let Some(frame) = fits(rest) {
            let meta = RecordMeta { key: None, timestamp: millis(first.timestamp), signal: None };
            out.push_with(&frame, meta);
            return;
        }
        // The first pass starts knowing the whole batch doesn't fit. Later passes find their
        // bound by doubling, because probing the whole rest again would cost every record an
        // encode of the remaining batch.
        let mut known_over = Some(rest.len());
        while let Some(first) = rest.first() {
            let meta = RecordMeta { key: None, timestamp: millis(first.timestamp), signal: None };
            // A run length that doesn't fit, or one past the end when none is known.
            let over_at_start = known_over.take().unwrap_or(rest.len() + 1);
            let Some(mut best) = fits(&rest[..1]) else {
                stats.oversize += 1;
                rest = &rest[1..];
                continue;
            };
            // `fit` events fit and `over` don't: double, then bisect between them.
            let (mut fit, mut over) = (1, over_at_start);
            let mut probe = 2;
            while probe < over && fit < rest.len() {
                let n = probe.min(rest.len());
                match fits(&rest[..n]) {
                    Some(frame) => {
                        (fit, best) = (n, frame);
                        probe *= 2;
                    }
                    None => over = n,
                }
            }
            while over - fit > 1 {
                let mid = fit + (over - fit) / 2;
                match fits(&rest[..mid]) {
                    Some(frame) => (fit, best) = (mid, frame),
                    None => over = mid,
                }
            }
            out.push_with(&best, meta);
            rest = &rest[fit..];
        }
    }
}

/// One OTLP request's worth of a signal's events, before the size cap cuts it.
struct Group<'e> {
    key: Option<Bytes>,
    /// Unkeyed because the configured key has no value here: each record counts in
    /// [`EncodeStats::missing_key`].
    missing: bool,
    events: Vec<&'e Event>,
}

/// What one candidate encode of a run of events gave.
enum Fit {
    /// The request fits the budget.
    /// The request, and its OTLP/JSON when the format is `otlp_json`, written once to measure it.
    Fits(OtlpRequest, Option<Vec<u8>>),
    /// No event of the run yields a record of the signal: a metric the mapping skips.
    Empty,
    Over,
}

/// The order an OTLP format writes a batch's signals in, `SignalEncoder::encode_signals`'s.
const SIGNALS: [Signal; 3] = [Signal::Logs, Signal::Traces, Signal::Metrics];

fn carries(signal: Signal, event: &Event) -> bool {
    match signal {
        Signal::Logs => event.log.is_some(),
        Signal::Traces => event.span.is_some(),
        Signal::Metrics => !event.metrics.is_empty(),
    }
}

/// The trace id `signal`'s record of `event` carries: a span's own, or the log record's
/// `TraceRef`. An all-zero id is none.
fn signal_trace_id(signal: Signal, event: &Event) -> Option<[u8; 16]> {
    let id = match signal {
        Signal::Logs => event.log.as_ref()?.trace.as_ref()?.trace_id,
        Signal::Traces => event.span.as_ref()?.trace_id,
        Signal::Metrics => return None,
    };
    id.iter().any(|&b| b != 0).then_some(id)
}

impl KafkaEncoder {
    /// The OTLP formats: one request per signal, split by key and then by the budget
    /// (`docs/adr/kafka-otlp-formats.md`, items 1, 5, 6, and 7).
    fn otlp(
        &mut self,
        batch: &EventBatch,
        out: &mut MessageBuf<RecordMeta>,
        stats: &mut EncodeStats,
    ) {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
        let timestamp = millis(i64::try_from(nanos).unwrap_or(i64::MAX));
        let mut written = false;
        for event in &batch.events {
            if event.log.is_none() && event.span.is_none() && event.metrics.is_empty() {
                stats.no_payload += 1;
            } else {
                written = true;
                stats.u64_as_double += u64_as_double_count(&event.attributes);
            }
        }
        if written {
            stats.u64_as_double += u64_as_double_count(&batch.resource.attributes)
                + batch.scope.as_ref().map_or(0, |s| u64_as_double_count(&s.attributes));
        }
        for signal in SIGNALS {
            let groups = self.groups(signal, batch);
            for group in groups {
                self.otlp_group(signal, batch, group, timestamp, out, stats);
            }
        }
    }

    /// `signal`'s events of `batch`, grouped by their key: one group per trace id, in order of
    /// first appearance, then the events with none; or one group for the batch.
    fn groups<'e>(&mut self, signal: Signal, batch: &'e EventBatch) -> Vec<Group<'e>> {
        let events = batch.events.iter().filter(|e| carries(signal, e));
        let one = |key: Option<Bytes>, missing: bool, events: Vec<&'e Event>| {
            if events.is_empty() {
                Vec::new()
            } else {
                vec![Group { key, missing, events }]
            }
        };
        match self.key {
            Compiled::TraceId if signal != Signal::Metrics => {
                let mut groups: Vec<Group<'e>> = Vec::new();
                let mut index: HashMap<[u8; 16], usize> = HashMap::new();
                let mut unkeyed = Vec::new();
                for event in events {
                    let Some(id) = signal_trace_id(signal, event) else {
                        unkeyed.push(event);
                        continue;
                    };
                    let at = *index.entry(id).or_insert_with(|| {
                        self.scratch.clear();
                        push_hex(&mut self.scratch, &id);
                        let key = Some(Bytes::copy_from_slice(self.scratch.as_bytes()));
                        groups.push(Group { key, missing: false, events: Vec::new() });
                        groups.len() - 1
                    });
                    groups[at].events.push(event);
                }
                groups.extend(one(None, true, unkeyed));
                groups
            }
            Compiled::Resource(sym) => {
                let key = batch
                    .resource
                    .attributes
                    .get_sym(sym)
                    .and_then(|value| key_bytes(value, &mut self.scratch).ok());
                one(key.clone(), key.is_none(), events.collect())
            }
            // `key: {attribute: ..}` is refused under the OTLP formats.
            _ => one(None, false, events.collect()),
        }
    }

    /// Writes `group` as records of `signal`: the whole request when it fits the budget less
    /// its key, else each the longest run of the remaining events that fits, found as
    /// `native_packed` finds one. The whole request's encode is the one that counts the
    /// mapping's side effects; every probe after it runs with the gate muted.
    fn otlp_group(
        &mut self,
        signal: Signal,
        batch: &EventBatch,
        group: Group<'_>,
        timestamp: i64,
        out: &mut MessageBuf<RecordMeta>,
        stats: &mut EncodeStats,
    ) {
        let Group { key, missing, events } = group;
        let budget = self.budget.saturating_sub(key.as_ref().map_or(0, Bytes::len));
        let (resource, scope) = (&batch.resource, batch.scope.as_deref());
        let emit = |(request, json): (OtlpRequest, Option<Vec<u8>>),
                    out: &mut MessageBuf<RecordMeta>,
                    stats: &mut EncodeStats| {
            let meta = RecordMeta { key: key.clone(), timestamp, signal: Some(signal) };
            match json {
                Some(json) => out.push_with(&json, meta),
                None => out.push_with(&request.encode_to_vec(), meta),
            };
            stats.missing_key += usize::from(missing);
        };
        let json = self.format == RecordFormat::OtlpJson;
        let otlp = &mut self.otlp;
        let mut fits =
            |events: &[&Event]| match otlp.request(signal, resource, scope, events.iter().copied())
            {
                None => Fit::Empty,
                Some(request) if json => {
                    let mut body = Vec::new();
                    request.write_json(&mut body);
                    if body.len() <= budget {
                        Fit::Fits(request, Some(body))
                    } else {
                        Fit::Over
                    }
                }
                Some(request) if request.encoded_len() <= budget => Fit::Fits(request, None),
                Some(_) => Fit::Over,
            };
        match fits(&events) {
            Fit::Fits(request, json) => return emit((request, json), out, stats),
            Fit::Empty => return,
            Fit::Over => {}
        }
        let gate = self.gate.clone();
        gate.muted(|| {
            let mut rest = &events[..];
            // The first pass starts knowing the whole group doesn't fit.
            let mut known_over = Some(rest.len());
            while !rest.is_empty() {
                let over_at_start = known_over.take().unwrap_or(rest.len() + 1);
                let mut best = match fits(&rest[..1]) {
                    Fit::Fits(request, json) => (request, json),
                    Fit::Empty => {
                        rest = &rest[1..];
                        continue;
                    }
                    Fit::Over => {
                        stats.oversize += 1;
                        rest = &rest[1..];
                        continue;
                    }
                };
                // `fit` events fit and `over` don't: double, then bisect between them. A run
                // starting with an event that yields a record never encodes empty.
                let (mut fit, mut over) = (1, over_at_start);
                let mut probe = 2;
                while probe < over && fit < rest.len() {
                    let n = probe.min(rest.len());
                    match fits(&rest[..n]) {
                        Fit::Fits(request, json) => {
                            (fit, best) = (n, (request, json));
                            probe *= 2;
                        }
                        Fit::Empty | Fit::Over => over = n,
                    }
                }
                while over - fit > 1 {
                    let mid = fit + (over - fit) / 2;
                    match fits(&rest[..mid]) {
                        Fit::Fits(request, json) => (fit, best) = (mid, (request, json)),
                        Fit::Empty | Fit::Over => over = mid,
                    }
                }
                emit(best, out, stats);
                rest = &rest[fit..];
            }
        });
    }
}

/// A value's key bytes, by the module doc's table; `Err(())` for one that can't be a key.
fn key_bytes(value: &Value, scratch: &mut String) -> Result<Bytes, ()> {
    scratch.clear();
    match value {
        Value::Str(b) | Value::Bytes(b) => return Ok(b.clone()),
        Value::I64(n) => write!(scratch, "{n}"),
        Value::U64(n) => write!(scratch, "{n}"),
        Value::F64(n) => write!(scratch, "{n}"),
        Value::Bool(b) => write!(scratch, "{b}"),
        Value::Timestamp(ns) => write!(scratch, "{ns}"),
        Value::Null | Value::Array(_) | Value::Map(_) => return Err(()),
    }
    .expect("writing to a String never fails");
    Ok(Bytes::copy_from_slice(scratch.as_bytes()))
}

impl FramedEncoder for KafkaEncoder {
    type Meta = RecordMeta;
    type Stats = EncodeStats;

    fn encode_into(&mut self, batch: &EventBatch, out: &mut MessageBuf<RecordMeta>) -> EncodeStats {
        out.clear();
        let mut stats = EncodeStats::default();
        match (self.record, self.format) {
            (RecordMode::Event, _) => self.per_event(batch, out, &mut stats),
            (RecordMode::Batch, RecordFormat::Native) => self.native_packed(batch, out, &mut stats),
            (RecordMode::Batch, _) => self.packed(batch, out, &mut stats),
            (RecordMode::Request, _) => self.otlp(batch, out, &mut stats),
        }
        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, LogRecord, TraceRef};

    fn batch(resource: AttrMap, events: Vec<Event>) -> EventBatch {
        let resource = Resource { attributes: resource, ..Resource::default() };
        EventBatch { resource: std::sync::Arc::new(resource), scope: None, events }
    }

    fn event(attributes: AttrMap) -> Event {
        Event::empty(1_700_000_000_123_456_789, attributes)
    }

    fn encode(key: RecordKey, batch: &EventBatch) -> (Vec<(String, RecordMeta)>, EncodeStats) {
        let mut encoder =
            KafkaEncoder::new(&key, RecordFormat::Json, RecordMode::Event, usize::MAX);
        let mut out = MessageBuf::default();
        let stats = encoder.encode_into(batch, &mut out);
        let records = out
            .iter_with()
            .map(|(v, m)| (String::from_utf8(v.to_vec()).unwrap(), m.clone()))
            .collect();
        (records, stats)
    }

    fn key_of(key: RecordKey, value: Value) -> Option<Bytes> {
        let mut attrs = AttrMap::new();
        attrs.insert("k", value);
        let (records, _) = encode(key, &batch(AttrMap::new(), vec![event(attrs)]));
        records[0].1.key.clone()
    }

    #[test]
    fn a_value_is_the_ndjson_object_without_its_newline() {
        let mut attrs = AttrMap::new();
        attrs.insert("a", Value::I64(1));
        let b = batch(AttrMap::new(), vec![event(attrs)]);
        let (records, stats) = encode(RecordKey::None, &b);
        let mut line = String::new();
        crate::ndjson::render_ndjson(&mut line, &b);
        assert_eq!(format!("{}\n", records[0].0), line);
        assert_eq!(
            records[0].1,
            RecordMeta { key: None, timestamp: 1_700_000_000_123, signal: None }
        );
        assert_eq!(stats.missing_key, 0, "no key configured is not a missing key");
    }

    #[test]
    fn a_timestamp_before_the_epoch_rounds_down() {
        let b = batch(AttrMap::new(), vec![Event::empty(-1, AttrMap::new())]);
        assert_eq!(encode(RecordKey::None, &b).0[0].1.timestamp, -1);
    }

    #[test]
    fn each_value_type_becomes_its_key_text() {
        let attr = || RecordKey::Attribute("k".into());
        let cases = [
            (Value::str("abc"), Some("abc")),
            (Value::Bytes(Bytes::from_static(b"\xff\x00")), Some("\u{0}")),
            (Value::I64(-7), Some("-7")),
            (Value::U64(200), Some("200")),
            (Value::F64(200.0), Some("200")),
            (Value::F64(0.1), Some("0.1")),
            (Value::Bool(true), Some("true")),
            (Value::Timestamp(42), Some("42")),
            (Value::Null, None),
            (Value::Array(vec![Value::I64(1)]), None),
            (Value::Map(Box::default()), None),
        ];
        for (value, want) in cases {
            let got = key_of(attr(), value.clone());
            match (value, want) {
                (Value::Bytes(b), _) => assert_eq!(got, Some(b)),
                (_, want) => assert_eq!(got.as_deref(), want.map(str::as_bytes), "{want:?}"),
            }
        }
    }

    #[test]
    fn a_missing_key_is_counted_and_the_record_goes_unkeyed() {
        let b = batch(AttrMap::new(), vec![event(AttrMap::new()), event(AttrMap::new())]);
        let (records, stats) = encode(RecordKey::Attribute("absent".into()), &b);
        assert_eq!(stats.missing_key, 2);
        assert!(records.iter().all(|(_, m)| m.key.is_none()));
    }

    #[test]
    fn a_resource_key_reads_the_batch_resource() {
        let mut resource = AttrMap::new();
        resource.insert("service.name", Value::str("api"));
        let b = batch(resource, vec![event(AttrMap::new())]);
        let (records, _) = encode(RecordKey::Resource("service.name".into()), &b);
        assert_eq!(records[0].1.key.as_deref(), Some(&b"api"[..]));
    }

    #[test]
    fn a_trace_id_key_is_lowercase_hex_and_all_zero_is_missing() {
        let id = [0xab; 16];
        let mut e = event(AttrMap::new());
        e.log = Some(LogRecord {
            message: Value::str("m"),
            severity: None,
            body_format: logit_core::BodyFormat::Raw,
            trace: Some(TraceRef { trace_id: id, span_id: None, flags: 0 }),
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        });
        let mut zero = e.clone();
        zero.log.as_mut().unwrap().trace.as_mut().unwrap().trace_id = [0; 16];
        let (records, stats) = encode(RecordKey::TraceId, &batch(AttrMap::new(), vec![e, zero]));
        assert_eq!(records[0].1.key.as_deref(), Some("ab".repeat(16).as_bytes()));
        assert_eq!(records[1].1.key, None);
        assert_eq!(stats.missing_key, 1);
    }

    // ---- formats and record modes -------------------------------------------------------

    use crate::msgpack::tests::{every_field_batch, json_tree, read_tree, same};
    use logit_proto::msgpack::Reader;

    fn encoder(format: RecordFormat, record: RecordMode, budget: usize) -> KafkaEncoder {
        KafkaEncoder::new(&RecordKey::None, format, record, budget)
    }

    /// Every record `encoder` writes for `batch`, and its stats.
    fn records(
        encoder: &mut KafkaEncoder,
        batch: &EventBatch,
    ) -> (Vec<(Vec<u8>, RecordMeta)>, EncodeStats) {
        let mut out = MessageBuf::default();
        let stats = encoder.encode_into(batch, &mut out);
        (out.iter_with().map(|(v, m)| (v.to_vec(), m.clone())).collect(), stats)
    }

    /// `event`'s NDJSON object, the reference every format is decoded back against.
    fn ndjson(batch: &EventBatch, event: &Event) -> String {
        let mut line = String::new();
        let scope = batch.scope.as_deref();
        crate::ndjson::render_event(&mut line, &batch.resource, scope, event, Some(CARRIER_PREFIX));
        line
    }

    /// The events of every native frame in `records`, in order, each frame checked to be one
    /// `CODEC_BATCH` frame under the batch's resource and scope.
    fn native_events(batch: &EventBatch, records: &[(Vec<u8>, RecordMeta)]) -> Vec<Event> {
        let mut events = Vec::new();
        for (value, _) in records {
            let mut bytes = Bytes::copy_from_slice(value);
            let (codec, mut payload) = logit_proto::frame::read_frame(&mut bytes).unwrap();
            assert_eq!(codec, CODEC_BATCH);
            assert!(bytes.is_empty(), "one frame per record");
            let decoded =
                logit_proto::native::decode_batch(&mut payload, &Default::default()).unwrap();
            assert_eq!(decoded.resource, batch.resource);
            assert_eq!(decoded.scope, batch.scope);
            events.extend(decoded.events);
        }
        events
    }

    /// The events `format`'s records decode back to, as their NDJSON objects, so a NaN compares
    /// equal to itself. `text` decodes to bodies instead, which [`text_round_trips`] checks.
    fn decoded(
        format: RecordFormat,
        batch: &EventBatch,
        records: &[(Vec<u8>, RecordMeta)],
        record: RecordMode,
    ) -> Vec<String> {
        match format {
            RecordFormat::Json => records
                .iter()
                .flat_map(|(value, _)| {
                    let text = std::str::from_utf8(value).unwrap().to_string();
                    match record {
                        RecordMode::Event => vec![text],
                        RecordMode::Batch => {
                            assert!(text.ends_with('\n'), "NDJSON lines end with a newline");
                            text.lines().map(str::to_string).collect()
                        }
                        RecordMode::Request => unreachable!("json writes no request"),
                    }
                })
                .inspect(|line| {
                    serde_json::from_str::<serde_json::Value>(line).expect("each object parses");
                })
                .collect(),
            RecordFormat::Msgpack => {
                let mut trees = Vec::new();
                for (value, _) in records {
                    let mut r = Reader::new(value);
                    let before = trees.len();
                    while !r.is_empty() {
                        trees.push(read_tree(&mut r));
                    }
                    if record == RecordMode::Event {
                        assert_eq!(trees.len() - before, 1, "one map per event record");
                    }
                }
                // Each map carries what the event's NDJSON object does; the reference string
                // stands in for it once that holds.
                assert_eq!(trees.len(), batch.events.len());
                trees
                    .iter()
                    .zip(&batch.events)
                    .map(|(tree, event)| {
                        let line = ndjson(batch, event);
                        assert!(same(&json_tree(&line), tree), "{line}\n{tree:#?}");
                        line
                    })
                    .collect()
            }
            RecordFormat::Native => {
                native_events(batch, records).iter().map(|e| ndjson(batch, e)).collect()
            }
            RecordFormat::Text | RecordFormat::OtlpProto | RecordFormat::OtlpJson => unreachable!(),
        }
    }

    /// The `kafka_in` round-trip property, in-process: each format's records, in both record
    /// modes, decode back to the events that went in.
    #[test]
    fn each_format_round_trips_in_both_record_modes() {
        let batch = every_field_batch();
        let want: Vec<String> = batch.events.iter().map(|e| ndjson(&batch, e)).collect();
        for format in [RecordFormat::Json, RecordFormat::Msgpack, RecordFormat::Native] {
            for record in [RecordMode::Event, RecordMode::Batch] {
                let mut encoder = encoder(format, record, usize::MAX);
                let (records, stats) = records(&mut encoder, &batch);
                assert_eq!(stats, EncodeStats::default(), "{format:?} {record:?}");
                let expected_records = match record {
                    RecordMode::Event => batch.events.len(),
                    RecordMode::Batch | RecordMode::Request => 1,
                };
                assert_eq!(records.len(), expected_records, "{format:?} {record:?}");
                assert_eq!(
                    decoded(format, &batch, &records, record),
                    want,
                    "{format:?} {record:?}"
                );
            }
        }
    }

    fn log_event(timestamp: i64, message: Value) -> Event {
        Event::log(
            timestamp,
            AttrMap::new(),
            LogRecord {
                message,
                severity: None,
                body_format: logit_core::BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        )
    }

    /// `format: text` writes each log body by the module doc's table, joins them with newlines
    /// under `record: batch`, and skips and counts an event with no log record in both modes.
    #[test]
    fn text_round_trips() {
        let mut map = AttrMap::new();
        map.insert("text_test.k", Value::str("v"));
        let bodies = [
            (Value::str("GET / 200"), &b"GET / 200"[..]),
            (Value::Bytes(Bytes::from_static(b"raw\xff")), b"raw\xff"),
            (Value::I64(-3), b"-3"),
            (Value::F64(0.5), b"0.5"),
            (Value::Bool(false), b"false"),
            (Value::Null, b""),
            (Value::Timestamp(1_000_000_001), b"1970-01-01T00:00:01.000000001Z"),
            (Value::Map(Box::new(map)), br#"{"text_test.k":"v"}"#),
            (Value::Array(vec![Value::I64(1), Value::str("a")]), br#"[1,"a"]"#),
        ];
        let mut events: Vec<Event> = bodies
            .iter()
            .enumerate()
            .map(|(i, (v, _))| log_event(i as i64 * 1_000_000, v.clone()))
            .collect();
        events.insert(2, Event::empty(0, AttrMap::new()));
        let b = batch(AttrMap::new(), events);

        let (records_out, stats) =
            records(&mut encoder(RecordFormat::Text, RecordMode::Event, usize::MAX), &b);
        assert_eq!(stats.no_log, 1, "the event with no log record");
        let got: Vec<&[u8]> = records_out.iter().map(|(v, _)| &v[..]).collect();
        let want: Vec<&[u8]> = bodies.iter().map(|(_, text)| *text).collect();
        assert_eq!(got, want);
        assert_eq!(records_out[1].1.timestamp, 1, "each record keeps its event's timestamp");

        let (batched, stats) =
            records(&mut encoder(RecordFormat::Text, RecordMode::Batch, usize::MAX), &b);
        assert_eq!(stats.no_log, 1);
        assert_eq!(batched.len(), 1);
        assert_eq!(batched[0].0, want.join(&b'\n'));
        assert_eq!(batched[0].1, RecordMeta { key: None, timestamp: 0, signal: None });
    }

    /// A log event that also carries two metric records and a span.
    fn mixed_event(message: &str) -> Event {
        let mut event = log_event(0, Value::str(message));
        for name in ["a", "b"] {
            event.metrics.push(logit_core::MetricRecord::new(
                intern(name),
                logit_core::MetricKind::Gauge(1.0.into()),
            ));
        }
        event.span = Some(logit_core::SpanRecord {
            trace_id: [1; 16],
            span_id: [1; 8],
            parent_span_id: None,
            name: Value::str("s"),
            kind: logit_core::SpanKind::Internal,
            status: logit_core::SpanStatus::Unset,
            events: vec![],
            links: vec![],
            end_timestamp: 1,
            flags: 0,
            ext: None,
        });
        event
    }

    /// `format: text` counts the metrics and span of each event whose body it writes, in both
    /// record modes; an event skipped for no log or dropped as `oversize` counts none, and no
    /// other format counts any.
    #[test]
    fn text_counts_the_metrics_and_span_it_leaves_out_of_a_written_record() {
        let mut metric_only = log_event(0, Value::Null);
        metric_only.log = None;
        metric_only.metrics.push(logit_core::MetricRecord::new(
            intern("m"),
            logit_core::MetricKind::Gauge(1.0.into()),
        ));
        let b = batch(
            AttrMap::new(),
            vec![mixed_event("one"), metric_only, mixed_event(&"x".repeat(64)), mixed_event("two")],
        );

        let (out, stats) =
            records(&mut encoder(RecordFormat::Text, RecordMode::Event, usize::MAX), &b);
        assert_eq!(out.len(), 3);
        assert_eq!((stats.no_log, stats.metrics_omitted, stats.spans_omitted), (1, 6, 3));

        let (out, stats) = records(&mut encoder(RecordFormat::Text, RecordMode::Batch, 32), &b);
        assert_eq!(out.len(), 1);
        assert_eq!(stats.oversize, 1, "the 64-byte body");
        assert_eq!((stats.no_log, stats.metrics_omitted, stats.spans_omitted), (1, 4, 2));

        for format in [RecordFormat::Json, RecordFormat::Msgpack] {
            let (_, stats) = records(&mut encoder(format, RecordMode::Event, usize::MAX), &b);
            assert_eq!((stats.metrics_omitted, stats.spans_omitted), (0, 0), "{format:?}");
        }
    }

    /// Under `record: batch`, each record takes events until the next would push it over the
    /// budget, an event over the budget alone is dropped and counted, and a record's timestamp
    /// is its first event's.
    #[test]
    fn a_batch_record_fills_to_the_budget_and_drops_an_event_over_it_alone() {
        let line = |e: &Event, b: &EventBatch| ndjson(b, e).len() + 1;
        let events: Vec<Event> =
            (0..5).map(|i| log_event(i * 1_000_000, Value::str("abcd"))).collect();
        let mut events_with_big = events.clone();
        events_with_big.insert(2, log_event(99_000_000, Value::str("x".repeat(500))));
        let b = batch(AttrMap::new(), events_with_big);
        let one = line(&b.events[0], &b);
        // Room for two lines, one byte short of three.
        let budget = 3 * one - 1;
        let (out, stats) = records(&mut encoder(RecordFormat::Json, RecordMode::Batch, budget), &b);
        assert_eq!(stats.oversize, 1, "the 500-byte event");
        let lines: Vec<usize> =
            out.iter().map(|(v, _)| v.iter().filter(|&&c| c == b'\n').count()).collect();
        assert_eq!(lines, vec![2, 2, 1]);
        assert!(out.iter().all(|(v, _)| v.len() <= budget));
        let stamps: Vec<i64> = out.iter().map(|(_, m)| m.timestamp).collect();
        assert_eq!(stamps, vec![0, 2, 4], "each record's first event");
        assert!(out.iter().all(|(_, m)| m.key.is_none()));

        // A value of the budget's length fits: two whole lines.
        let (out, _) = records(&mut encoder(RecordFormat::Json, RecordMode::Batch, 2 * one), &b);
        assert_eq!(out[0].0.len(), 2 * one);

        // Text counts the newline between bodies; msgpack values abut.
        let (out, _) = records(
            &mut encoder(RecordFormat::Text, RecordMode::Batch, 9),
            &batch(AttrMap::new(), events.clone()),
        );
        let values: Vec<&[u8]> = out.iter().map(|(v, _)| &v[..]).collect();
        assert_eq!(values, vec![&b"abcd\nabcd"[..], b"abcd\nabcd", b"abcd"]);
        let (out, _) = records(
            &mut encoder(RecordFormat::Text, RecordMode::Batch, 8),
            &batch(AttrMap::new(), events.clone()),
        );
        assert_eq!(out.len(), 5, "8 bytes hold one body, not two and a newline");
    }

    /// An event carrying `kafka_in`'s record metadata and an ordinary attribute.
    fn carrier_event(timestamp: i64) -> Event {
        let mut event = log_event(timestamp, Value::str("m"));
        event.attributes.insert("_logit.kafka.topic", "orders");
        event.attributes.insert("_logit.kafka.offset", Value::I64(7));
        event.attributes.insert("host", "web-1");
        event
    }

    /// The top-level attribute keys of one decoded JSON or MessagePack object, or `None` when it
    /// has no `attributes` key.
    fn json_attribute_keys(line: &str) -> Option<Vec<String>> {
        let object: serde_json::Value = serde_json::from_str(line).unwrap();
        let attrs = object.get("attributes")?.as_object().unwrap();
        Some(attrs.keys().cloned().collect())
    }

    fn msgpack_attribute_keys(tree: &crate::msgpack::tests::Tree) -> Option<Vec<String>> {
        use crate::msgpack::tests::Tree;
        let Tree::Map(top) = tree else { panic!("an event is a map") };
        let (_, Tree::Map(attrs)) = top.iter().find(|(k, _)| k == "attributes")? else {
            panic!("attributes is a map")
        };
        Some(attrs.iter().map(|(k, _)| k.clone()).collect())
    }

    /// `kafka_out` leaves `_logit.kafka.*` out of `json`, `msgpack`, and `native` records under
    /// both record modes, and keeps every other attribute; an event whose only attributes are
    /// carriers writes no `attributes` key.
    #[test]
    fn kafka_carriers_are_left_out_of_every_attribute_writing_format() {
        let mut only_carriers = log_event(2_000_000, Value::str("c"));
        only_carriers.attributes.insert("_logit.kafka.partition", Value::I64(0));
        let b =
            batch(AttrMap::new(), vec![carrier_event(0), carrier_event(1_000_000), only_carriers]);
        let host = Some(vec!["host".to_string()]);
        for record in [RecordMode::Event, RecordMode::Batch] {
            let (out, _) = records(&mut encoder(RecordFormat::Json, record, usize::MAX), &b);
            let lines: Vec<String> = out
                .iter()
                .flat_map(|(v, _)| {
                    std::str::from_utf8(v).unwrap().lines().map(str::to_string).collect::<Vec<_>>()
                })
                .collect();
            let keys: Vec<_> = lines.iter().map(|l| json_attribute_keys(l)).collect();
            assert_eq!(keys, vec![host.clone(), host.clone(), None], "json, {record:?}");

            let (out, _) = records(&mut encoder(RecordFormat::Msgpack, record, usize::MAX), &b);
            let mut keys = Vec::new();
            for (value, _) in &out {
                let mut r = Reader::new(value);
                while !r.is_empty() {
                    keys.push(msgpack_attribute_keys(&read_tree(&mut r)));
                }
            }
            assert_eq!(keys, vec![host.clone(), host.clone(), None], "msgpack, {record:?}");

            let (out, _) = records(&mut encoder(RecordFormat::Native, record, usize::MAX), &b);
            let attrs: Vec<AttrMap> =
                native_events(&b, &out).into_iter().map(|e| e.attributes).collect();
            let mut want = AttrMap::new();
            want.insert("host", "web-1");
            assert_eq!(attrs, vec![want.clone(), want, AttrMap::new()], "native, {record:?}");
        }
    }

    /// Under `record: batch`, a text body's `\` and LF are escaped, so a body holding either
    /// reads back as one body, a `Bytes` body stays bytes, and the budget measures the escaped
    /// form. `record: event` writes each body as it is.
    #[test]
    fn a_text_batch_record_escapes_each_body() {
        let bodies: [&[u8]; 4] = [b"two\nlines", b"back\\slash", b"raw\xff\n", b""];
        let events: Vec<Event> = vec![
            log_event(0, Value::str("two\nlines")),
            log_event(1_000_000, Value::str("back\\slash")),
            log_event(2_000_000, Value::Bytes(Bytes::from_static(b"raw\xff\n"))),
            log_event(3_000_000, Value::Null),
        ];
        let b = batch(AttrMap::new(), events);

        let (per_event, _) =
            records(&mut encoder(RecordFormat::Text, RecordMode::Event, usize::MAX), &b);
        let got: Vec<&[u8]> = per_event.iter().map(|(v, _)| &v[..]).collect();
        assert_eq!(got, bodies);

        let (out, _) = records(&mut encoder(RecordFormat::Text, RecordMode::Batch, usize::MAX), &b);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, b"two\\nlines\nback\\\\slash\nraw\xff\\n\n");
        assert_eq!(logit_proto::kafka::text::unescape_lines(&out[0].0), bodies);

        // Escaped, the bodies are 10, 11, 6, and 0 bytes: under a 10-byte budget the second is
        // over alone, and the third and fourth share a record, 6 + 1 + 0 bytes.
        let (out, stats) = records(&mut encoder(RecordFormat::Text, RecordMode::Batch, 10), &b);
        assert_eq!(stats.oversize, 1);
        let values: Vec<&[u8]> = out.iter().map(|(v, _)| &v[..]).collect();
        assert_eq!(values, vec![&b"two\\nlines"[..], b"raw\xff\\n\n"]);
        // The first body is 9 bytes as it is, so a 9-byte budget measuring that would keep it.
        let (_, stats) = records(&mut encoder(RecordFormat::Text, RecordMode::Batch, 9), &b);
        assert_eq!(stats.oversize, 2);
    }

    /// `native` under `record: batch` cuts the events into frames that each fit the budget and
    /// hold as many events as do: one more would not fit.
    #[test]
    fn a_native_batch_record_holds_the_longest_run_that_fits() {
        let events: Vec<Event> = (0..37)
            .map(|i| log_event(i * 1_000_000, Value::str("y".repeat(10 + (i as usize * 7) % 40))))
            .collect();
        let b = batch(AttrMap::new(), events);
        let frame = |events: &[Event]| native_frame(&b.resource, None, events).unwrap().len();
        let budget = frame(&b.events[..5]);
        let (out, stats) =
            records(&mut encoder(RecordFormat::Native, RecordMode::Batch, budget), &b);
        assert_eq!(stats, EncodeStats::default());
        assert!(out.len() > 1);
        let mut start = 0;
        for (value, meta) in &out {
            assert!(value.len() <= budget);
            let n = native_events(&b, std::slice::from_ref(&(value.clone(), meta.clone()))).len();
            assert_eq!(meta.timestamp, millis(b.events[start].timestamp));
            if start + n < b.events.len() {
                assert!(
                    frame(&b.events[start..start + n + 1]) > budget,
                    "record at {start} stopped short"
                );
            }
            start += n;
        }
        assert_eq!(start, b.events.len());
        let got: Vec<String> = native_events(&b, &out).iter().map(|e| ndjson(&b, e)).collect();
        let want: Vec<String> = b.events.iter().map(|e| ndjson(&b, e)).collect();
        assert_eq!(got, want, "every event, in order");

        // One event over the budget alone is dropped and counted; the rest still go.
        let mut big = b.events[..3].to_vec();
        big.insert(1, log_event(0, Value::str("z".repeat(2 * budget))));
        let b2 = batch(AttrMap::new(), big);
        let (out, stats) =
            records(&mut encoder(RecordFormat::Native, RecordMode::Batch, budget), &b2);
        assert_eq!(stats.oversize, 1);
        assert_eq!(native_events(&b2, &out).len(), 3);
    }

    /// An event over the budget mid-batch ends a `native` record, because a frame can't skip an
    /// event, while the in-order formats drop it and keep filling the open record.
    #[test]
    fn an_oversize_event_mid_batch_ends_a_native_record_but_not_a_json_one() {
        let small = |i: i64| log_event(i * 1_000_000, Value::str("abcd"));
        let big = log_event(9_000_000, Value::str("x".repeat(4096)));
        let b = batch(AttrMap::new(), vec![small(0), small(1), big, small(2)]);
        let abc = [b.events[0].clone(), b.events[1].clone(), b.events[3].clone()];
        let groups = |format, budget| {
            let (out, stats) = records(&mut encoder(format, RecordMode::Batch, budget), &b);
            assert_eq!(stats.oversize, 1);
            out.iter()
                .map(|r| decoded(format, &b, std::slice::from_ref(r), RecordMode::Batch))
                .collect::<Vec<_>>()
        };
        let objects = |events: &[Event]| events.iter().map(|e| ndjson(&b, e)).collect::<Vec<_>>();

        let native_budget = native_frame(&b.resource, None, &abc).unwrap().len();
        assert_eq!(
            groups(RecordFormat::Native, native_budget),
            vec![objects(&abc[..2]), objects(&abc[2..])],
        );
        let json_budget: usize = abc.iter().map(|e| ndjson(&b, e).len() + 1).sum();
        assert_eq!(groups(RecordFormat::Json, json_budget), vec![objects(&abc)]);
    }

    /// `record: event` keys each record in every format; a batch record never has one.
    #[test]
    fn every_format_keys_its_event_records() {
        let mut attrs = AttrMap::new();
        attrs.insert("k", Value::str("abc"));
        let mut e = log_event(0, Value::str("m"));
        e.attributes = attrs;
        let b = batch(AttrMap::new(), vec![e]);
        for format in
            [RecordFormat::Json, RecordFormat::Text, RecordFormat::Msgpack, RecordFormat::Native]
        {
            let mut encoder = KafkaEncoder::new(
                &RecordKey::Attribute("k".into()),
                format,
                RecordMode::Event,
                usize::MAX,
            );
            let (out, _) = records(&mut encoder, &b);
            assert_eq!(out[0].1.key.as_deref(), Some(&b"abc"[..]), "{format:?}");
        }
    }

    /// The OTLP formats share `otlp_out`'s encoder, so they read the neutral type flag: an `info`
    /// gauge is a non-monotonic `Sum` named `_info`, a flagged gauge histogram is skipped, and
    /// `prometheus_in`'s own carrier changes nothing.
    #[test]
    fn the_otlp_formats_read_the_neutral_type_flag_and_not_the_prometheus_carrier() {
        use logit_core::attrs::METRICS_TYPE;
        use logit_core::{Histogram, MetricKind, MetricRecord, Temporality};
        let metric = |name: &str, key: &str, kind: MetricKind| {
            let mut attributes = AttrMap::new();
            attributes.insert(key, Value::str(if name == "h" { "gaugehistogram" } else { "info" }));
            Event::metric(0, attributes, MetricRecord::new(intern(name), kind))
        };
        let histogram = MetricKind::Histogram(Histogram {
            buckets: vec![(1.0, 2), (f64::INFINITY, 1)],
            temporality: Temporality::Cumulative,
            sum: Some(3.0),
            min: None,
            max: None,
        });
        let b = batch(
            AttrMap::new(),
            vec![
                metric("flagged", METRICS_TYPE, MetricKind::gauge(1.0)),
                metric("carried", logit_proto::prometheus::ATTR_TYPE, MetricKind::gauge(1.0)),
                metric("h", METRICS_TYPE, histogram),
            ],
        );
        let (out, _) =
            records(&mut encoder(RecordFormat::OtlpJson, RecordMode::Request, usize::MAX), &b);
        assert_eq!(out.len(), 1, "one metrics request");
        let request: serde_json::Value = serde_json::from_slice(&out[0].0).unwrap();
        let metrics = request["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| {
                let data = ["gauge", "sum", "histogram"].into_iter().find(|k| m.get(*k).is_some());
                (m["name"].as_str().unwrap().to_string(), data.unwrap())
            })
            .collect::<Vec<_>>();
        assert_eq!(
            metrics,
            [("flagged_info".to_string(), "sum"), ("carried".to_string(), "gauge")],
            "the gauge histogram is skipped"
        );
    }

    /// The soak harness's protobuf walker (`tools/soak/soaklib/otlp.py`) is checked against
    /// this batch's `otlp_proto` record, committed as hex. The hex must be what the encoder
    /// writes; on a mismatch the message holds the encoder's hex to commit in its place.
    #[test]
    fn the_soak_otlp_metrics_vector_is_what_the_encoder_writes() {
        use logit_core::{Histogram, MetricKind, MetricRecord, MetricValue, Sum, Temporality};
        const TS: i64 = 1_791_720_000_250_000_000;
        let metric = |name: &str, attributes: AttrMap, kind: MetricKind| {
            Event::metric(TS, attributes, MetricRecord::new(intern(name), kind))
        };
        let sum = |value, monotonic| {
            MetricKind::Sum(Sum { value, temporality: Temporality::Delta, monotonic })
        };
        let mut shard = AttrMap::new();
        shard.insert("shard", Value::str("b"));
        let mut resource = AttrMap::new();
        resource.insert("host.name", Value::str("soak"));
        let b = batch(
            resource,
            vec![
                metric("soak_requests_1", AttrMap::new(), sum(MetricValue::I64(30), true)),
                metric("soak_requests_2", shard, sum(MetricValue::F64(2.5), true)),
                metric("soak_gauge", AttrMap::new(), MetricKind::gauge(9.0)),
                metric(
                    "soak_latency",
                    AttrMap::new(),
                    MetricKind::Histogram(Histogram {
                        buckets: vec![(1.0, 2), (f64::INFINITY, 3)],
                        temporality: Temporality::Delta,
                        sum: Some(4.0),
                        min: None,
                        max: None,
                    }),
                ),
                metric("soak_requests_3", AttrMap::new(), sum(MetricValue::I64(-4), false)),
            ],
        );
        let mut encoder = encoder(RecordFormat::OtlpProto, RecordMode::Request, usize::MAX);
        let (out, _) = records(&mut encoder, &b);
        assert_eq!(out.len(), 1, "one metrics request");
        let hex: String = out[0].0.iter().map(|byte| format!("{byte:02x}")).collect();
        let committed: String =
            include_str!("../../../../tools/soak/vectors/kafka-otlp-metrics.hex")
                .lines()
                .filter(|line| !line.starts_with('#'))
                .flat_map(str::split_whitespace)
                .collect();
        assert_eq!(hex, committed, "commit the encoder's hex: {hex}");
    }

    /// An event with no log, span, or metric has no OTLP signal, so no request holds it: each
    /// OTLP format counts it, as `otlp_out` does, and writes the other events.
    #[test]
    fn an_otlp_format_counts_an_event_with_no_payload() {
        for format in [RecordFormat::OtlpProto, RecordFormat::OtlpJson] {
            let b = batch(
                AttrMap::new(),
                vec![Event::empty(0, AttrMap::new()), log_event(1, Value::str("m"))],
            );
            let (out, stats) = records(&mut encoder(format, RecordMode::Request, usize::MAX), &b);
            assert_eq!(stats.no_payload, 1, "{format:?}: the event with no payload");
            assert_eq!(out.len(), 1, "{format:?}: one logs request");
        }
    }

    /// A `u64` attribute above `i64::MAX` leaves an OTLP format as a double, counted once per
    /// value on a written event and on the batch's resource, as `otlp_out` counts it; one on an
    /// event no request holds isn't written, so it isn't counted.
    #[test]
    fn an_otlp_format_counts_a_u64_attribute_written_as_a_double() {
        let big = |k: &str| {
            let mut a = AttrMap::new();
            a.insert(k, Value::U64(u64::MAX));
            a
        };
        for format in [RecordFormat::OtlpProto, RecordFormat::OtlpJson] {
            let mut log = log_event(1, Value::str("m"));
            log.attributes = big("big");
            let b = batch(big("res"), vec![log, Event::empty(0, big("unsent"))]);
            let (_, stats) = records(&mut encoder(format, RecordMode::Request, usize::MAX), &b);
            assert_eq!(stats.u64_as_double, 2, "{format:?}: the event's and the resource's");
        }
    }
}
