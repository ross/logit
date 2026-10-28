//! Splunk's HTTP Event Collector (HEC): the codec behind `splunk_hec_in` and `splunk_hec_out`
//! ([ADR `splunk-hec-relay`](../../../../docs/adr/splunk-hec-relay.md), whose "Verified
//! destination behavior" is the protocol survey). This doc is the
//! canonical mapping table and permitted-normalization list; the submodules hold one signal each
//! ([`logs`], [`metrics`], [`spans`]), the timestamp rule ([`time`]), and the response bodies
//! ([`response`]).
//!
//! The vocabulary is the OpenTelemetry Collector's `splunk_hec` exporter's, not a `splunk.*`
//! namespace, so `otlp_out` to a Splunk Collector and `splunk_hec_out` put the same names in
//! Splunk. HTTP concerns (routes, auth, `Content-Encoding`, channels, acknowledgment) belong to the
//! listener and sink, never here.
//!
//! **The listener adds sender attributes after decode.** Under `splunk_hec_in`'s `peer:`,
//! `proxy_protocol:`, or `forwarded:`, `network.peer.*` or `client.*` is added to every event, an
//! opt-in addition the sender never sent, and one of ADR `lossless-transit`'s "Permitted
//! normalizations". It applies to `/event` and `/raw` alike.
//!
//! # API
//!
//! [`SplunkDecoder::decode_events`] takes one decompressed `/services/collector/event` body
//! (concatenated objects or a JSON array) and returns one [`EventBatch`] per distinct resource, in
//! first-appearance order, wire order kept within each. It isn't a [`crate::Decoder`]: one request
//! carries several resources. A body that isn't HEC JSON is a [`HecError`] carrying the status
//! and, for a syntax error, the index of the first bad object. The listener calls
//! [`SplunkDecoder::decode_events_prefix`] instead, which also returns the batches of the objects
//! before that index, so it delivers what Splunk would index and answers what Splunk would.
//! [`SplunkDecoder::decode_raw`] takes a `/raw` body and the [`Envelope`] the listener read from
//! the query string.
//!
//! [`SplunkEncoder::encode_objects`] writes one HEC JSON object per entry of a
//! [`MessageBuf<ObjectMeta>`](crate::MessageBuf), in batch order, and never fails: every loss is
//! counted. The sink packs objects greedily into bodies under its size cap and can drop object `N`
//! by slicing the same buffer, with no re-encode. Its [`crate::Encoder`] impl is the
//! concatenation, one body per batch.
//!
//! # Envelope
//!
//! Every object carries its own `time`, `host`, `source`, `sourcetype`, and `index`. The last
//! four are the batch [`Resource`]'s carriers on decode. On encode, `host` is chosen per object:
//!
//! | Wire | Model |
//! |---|---|
//! | `host` | decode: resource [`RESOURCE_ATTR_HOST_NAME`] (`Str`). Encode: the event's own [`RESOURCE_ATTR_HOST_NAME`], a non-empty `Str`, which then isn't also written as a field or span attribute; else the resource's, when a `Str`; else, when the resource has no `host.name`, [`SplunkEncoder::with_default_host`]'s value (`splunk_hec_out`'s process hostname). An event `host.name` that is empty or not a `Str` (a `Bytes` included) isn't a host and stays an attribute |
//! | `source` | resource [`RESOURCE_ATTR_SOURCE`] |
//! | `sourcetype` | resource [`RESOURCE_ATTR_SOURCETYPE`] |
//! | `index` | resource [`RESOURCE_ATTR_INDEX`] |
//!
//! ## Decode: HEC → events
//!
//! | Wire | Model | Counter |
//! |---|---|---|
//! | body empty, whitespace, or `[]` | `HecError` code 5 (`No data`) | -- |
//! | object *i* not valid JSON, or not a JSON object | `HecError` code 6 with `invalid-event-number` *i*; objects before *i* decoded by `decode_events_prefix`, none by `decode_events` | -- |
//! | object *i*'s `event`, `fields`, or a carrier holding a number outside `f64`'s range (`1e400`) or nesting past 127 levels, which the model can't hold | the same `HecError` code 6 | -- |
//! | a carrier `null` | absent | -- |
//! | a carrier that isn't a string | its JSON text | `logit.input.events.degraded{reason="non_string_envelope"}` |
//! | any other envelope key | dropped | `logit.input.events.degraded{reason="unknown_envelope_key"}` |
//! | `time`: a number, or a string holding one | `Event::timestamp`, every digit kept ([`time::parse_hec_time`]); `0` is the epoch | -- |
//! | `time` absent or `null` | `received_at` | -- |
//! | `time` another type, or a number or string that doesn't parse (`1e400`) | `received_at` | `degraded{reason="bad_time"}` |
//! | `fields` not an object | ignored | `degraded{reason="bad_fields"}` |
//! | `fields` object | event attributes (a span's: resource attributes), each value typed as [`crate::json`] converts it, a nested map flattened to dotted keys by [`crate::json::flatten_into`] (an empty map dropped, an array holding a container as its JSON text) | -- |
//! | objects with an equal resource | one batch, in first-appearance order | -- |
//! | `event` absent or `null`, with a `metric_name:<n>` field or both `metric_name` and `_value` | a metric event ([`metrics`]), as Splunk indexes it | see [`metrics`] |
//! | `event` absent or `null` otherwise | dropped; the rest delivered | `logit.input.events.dropped{reason="no_event"}` |
//! | `event` `""` | dropped; the rest delivered | `dropped{reason="blank_event"}` |
//! | `event` `"metric"` with a `metric_name:<n>` field, or both `metric_name` and `_value` | a metric event ([`metrics`]) | see [`metrics`] |
//! | `event` an object carrying valid `trace_id`, `span_id`, `start_time`, `end_time` | a span event ([`spans`]) | see [`spans`] |
//! | any other `event` | a log event ([`logs`]) | see [`logs`] |
//!
//! `/raw` ([`SplunkDecoder::decode_raw`]): one batch whose resource is the query-string
//! [`Envelope`]; one [`logit_core::Event::log`] per LF-delimited line, a trailing CR stripped,
//! empty lines skipped, each a `Str` message with `BodyFormat::Raw` stamped `received_at`.
//! Invalid UTF-8 is replaced, counted `degraded{reason="invalid_utf8"}` once per line.
//!
//! ## Encode: events → HEC
//!
//! One object per payload: an event carrying a log, metrics, and a span writes a log object, one
//! or more metric objects, and a span object, in that order. Every object writes `time`, then its
//! `host` and each other carrier present as a `Str` on the resource, then `event`, then `fields`
//! when non-empty.
//!
//! | Model | Wire | Counter |
//! |---|---|---|
//! | `Event::timestamp` (a span's: its start) | `time`, seconds with up to nine decimals ([`time::write_hec_time`]) | -- |
//! | a carrier that isn't a `Str` | omitted | `logit.output.tags.omitted{reason="unrepresentable"}`, once per batch |
//! | every other resource attribute, then every event attribute but a `host.name` the object's `host` took (the event's winning on a key) | `fields`, a `Map` flattened to dotted keys by [`crate::json::flatten_into`] (a span's event attributes go in its `attributes` instead) | -- |
//! | a private `_logit.` attribute, on the resource or the event, a span event, or a link | omitted ([ADR `private-attribute-namespace`](../../../../docs/adr/private-attribute-namespace.md)); a `_logit.` key nested inside a map is data and flattens | -- |
//! | an event with no log, metric, or span | nothing | `logit.output.events.skipped{reason="no_payload"}` |
//!
//! [`logs`], [`metrics`], and [`spans`] each carry their own rows.
//!
//! ## What each log, span, and attribute becomes
//!
//! The tables below are generated from `splunk_hec_out`'s column of `spec/transit/mapping.yaml`,
//! the canonical copy, by `script/transit docs`, and a test fails when they're stale: change the
//! table there, not here. They cover logs, spans, and attributes; the spec has no metric cells
//! for `splunk_hec_out`, so [`metrics`]' tables are the only copy of the metric mapping.
//! "Consumer" means `splunk_hec_in` reading the body back.
//!
//! <!-- transit:doc:splunk_hec_out -->
//!
//! Per metric kind, and per record feature that decides a record's fate:
//!
//! | Row | Class | Reason or counter | What a consumer sees |
//! |---|---|---|---|
//! | `log/dogstatsd_event` | exact | -- | One HEC object whose `event` is the message. Its `_logit.statsd.event.*` carriers are private and aren't written; the `log` field rows have its fields. |
//! | `log/plain` | exact | -- | One HEC object whose `event` is the message; the `log` field rows have its fields. |
//! | `log/message_unsafe` | normalized | `message_as_text` | A `Bytes` message leaves as its text, an invalid UTF-8 sequence replaced, and a timestamp message as its RFC 3339 text, each read back as a string. |
//! | `log/structured_message` | exact | -- | A JSON object or array `event`, read back as the same structure. |
//! | `log/bytes_message` | normalized | `message_as_text` | A string `event`, read back as a string, as the `log/message_unsafe` row says. |
//! | `log/blank_message` | dropped | `logit.output.events.dropped{reason=blank_event}` | A log whose message is empty, null, or a non-finite number isn't written, since HEC rejects a whole request holding a blank `event`; the rest of the batch is sent. |
//! | `span/alone` | exact | -- | A span object in the OpenTelemetry exporter's shape, its resource's attributes as `fields`; the `span` field rows have what it can't carry. |
//! | `span/not_datadog_origin` | exact | -- | Nothing beyond the span's `span/alone`, `span/beside_log`, or `span/beside_metrics` outcome: only `datadog_out` gates a trace chunk. |
//! | `span/needs_agent_processing` | exact | -- | Nothing beyond the span's `span/alone`, `span/beside_log`, or `span/beside_metrics` outcome: only `datadog_out` gates a trace chunk. |
//! | `event/attributes_only` | skipped | `logit.output.events.skipped{reason=no_payload}` | An event with no log, metric, or span has no HEC object and isn't sent. |
//! | `event/string_only` | skipped | `logit.output.events.skipped{reason=no_payload}` | An InfluxDB point whose fields are all strings has no log, metric, or span, so it isn't sent and its fields are lost. |
//!
//! Per field of a record that reaches the wire, attribute shape, and payload beside metrics:
//!
//! | Row | Class | Reason or counter | What a consumer sees |
//! |---|---|---|---|
//! | `attributes/string_value` | normalized | `log_fields` | A `fields` string, or on a span a string in its `attributes` object, read back as itself. On a log, splunk_hec_in reads a non-empty `otel.log.name` into the event name, and a `trace_id` that parses as a trace id, with a `span_id` that parses or none, into the trace, so none of them reads back as an attribute. |
//! | `attributes/host_beside_host_name` | exact | -- | A `host` field, an ordinary key beside the object's own `host`. |
//! | `attributes/hostname` | exact | -- | A `fields` key. |
//! | `attributes/log_service` | exact | -- | A `fields` key. |
//! | `attributes/log_service_field` | exact | -- | A `fields` key. |
//! | `attributes/container_id` | exact | -- | A `fields` key. |
//! | `attributes/service_identity` | exact | -- | A `fields` key. |
//! | `attributes/service_tag` | exact | -- | A `fields` key. |
//! | `attributes/service_tag_beside_key` | exact | -- | A `fields` key, beside the standard key's. |
//! | `attributes/bool_value` | exact | -- | A JSON boolean, read back as itself. |
//! | `attributes/number_value` | exact | -- | A JSON number, read back as the same integer or double; a `u64` above `i64::MAX` stays a `u64`. |
//! | `attributes/array_value` | normalized | `flattened` | A JSON array; one holding a map or an array leaves as its JSON text, read back as a string, since `fields` is flat. |
//! | `attributes/map_value` | normalized | `flattened` | `fields` is flat, so a map leaves as one dotted key per leaf (`http.method`), read back as that many attributes, and an empty map is left off. A span's `attributes` object keeps its nesting. |
//! | `attributes/bytes_value` | normalized | `stringified` | Its base64 text as a JSON string, read back as a string. |
//! | `attributes/timestamp_value` | normalized | `stringified` | Its RFC 3339 text as a JSON string, read back as a string. |
//! | `attributes/null_value` | exact | -- | A JSON null, read back as itself. |
//! | `attributes/private_carrier` | lost | structural | A `_logit.` carrier or flag is private and isn't a HEC field, so it's left off. |
//! | `attributes/prometheus_target` | lost | structural | The scrape target is private and isn't a HEC field, so it's left off. |
//! | `attributes/foreign_carrier` | lost | structural | Another protocol's `_logit.` carrier is private and isn't a HEC field, so it's left off. |
//! | `attributes/timestamped` | lost | structural | The flag is private and isn't a HEC field, so it's left off. `time` carries the timestamp. |
//! | `attributes/rate` | lost | structural | The flag is private and isn't a HEC field, so it's left off. |
//! | `attributes/interval` | lost | structural | The flag is private and isn't a HEC field, so it's left off. |
//! | `attributes/rate_on_non_gauge` | lost | structural | The flag is private and isn't a HEC field, so it's left off. |
//! | `attributes/family_type` | lost | structural | The flag is private and isn't a HEC field, so it's left off. |
//! | `attributes/family_type_on_other_kind` | lost | structural | The flag is private and isn't a HEC field, so it's left off. |
//! | `attributes/interval_on_distribution` | lost | structural | The flag is private and isn't a HEC field, so it's left off. |
//! | `attributes/interval_on_histogram` | lost | structural | The flag is private and isn't a HEC field, so it's left off. |
//! | `attributes/resource_attribute` | normalized | `resource_as_field` | A `fields` key beside the event's own, so on a log or a metric it reads back as an event attribute, an event attribute of the same key winning; a span's reads back on the resource. |
//! | `attributes/scope` | lost | structural | HEC has no instrumentation scope, so none is written. |
//! | `attributes/datadog_carrier_log` | lost | structural | A `_logit.` carrier is private and isn't a HEC field, so it's left off. |
//! | `attributes/severity_carrier_otel` | lost | structural | A `_logit.` carrier is private and isn't a HEC field, so it's left off and the severity reads back at band resolution. |
//! | `attributes/severity_carrier_syslog` | lost | structural | A `_logit.` carrier is private and isn't a HEC field, so it's left off and the severity reads back at band resolution. |
//! | `attributes/span_numeric` | exact | -- | A JSON number in the span's `attributes`, read back as the same integer or double; a `u64` above `i64::MAX` stays a `u64`. |
//! | `attributes/datadog_span_identity` | exact | -- | A string in the span's `attributes`. |
//! | `attributes/datadog_span_marks` | exact | -- | An ordinary entry of the span's `attributes`: Datadog's internal mark leaves as an OTel attribute. |
//! | `attributes/datadog_carrier_span` | lost | structural | The exporter's span object has no field for a Datadog trace chunk's fields or a span's raw `error`, and a `_logit.` carrier is private, so it's left off. |
//! | `attributes/datadog_tracer_resource` | lost | structural | HEC has no field for a `TracerPayload`'s or an `AgentPayload`'s fields, and a `_logit.` carrier is private, so it's left off. |
//! | `attributes/datadog_tracer_headers` | lost | structural | HEC has no field for a tracer request header, and a `_logit.` carrier is private, so it's left off. |
//! | `attributes/syslog_header` | normalized | `u64_as_int` | A `fields` string or number; a `u64` `syslog.facility` or `process.pid` reads back as an `i64`. |
//! | `attributes/syslog_msgid` | exact | -- | A `fields` string. |
//! | `attributes/syslog_sd` | normalized | `flattened` | `fields` is flat, so `syslog.sd` leaves as one `syslog.sd.<SD-ID>.<PARAM-NAME>` key per PARAM, read back as that many attributes and no `syslog.sd` map. |
//! | `attributes/syslog_timestamp` | normalized | `stringified` | A `fields` string: an RFC 3164 timestamp as itself, an RFC 5424 one as its RFC 3339 text, read back as a string. |
//! | `attributes/host_name` | normalized | `host_on_resource` | A non-empty string `host.name` is the object's `host`, the event's over the resource's, which splunk_hec_in reads back as the resource's `host.name`. |
//! | `attributes/splunk_envelope` | exact | -- | The object's `source`, `sourcetype`, or `index`, read back onto the resource; a value that isn't a string is omitted, counted once per batch. |
//! | `attributes/syslog_facility` | exact | -- | A log with no `syslog.facility` leaves with none. |
//! | `attributes/no_host` | normalized | `default_host` | A record with no `host.name` leaves with the process hostname as its `host`, which splunk_hec_in reads back as the resource's `host.name`. |
//! | `attributes/default_tag` | exact | -- | A log with no `syslog.tag` leaves with none. |
//! | `attributes/log_status` | exact | -- | No `status` attribute is written. |
//! | `attributes/log_severity_fields` | normalized | `severity_fields` | A log with a severity and neither `otel.log.severity.*` attribute leaves with both, the severity's name and its band's base number, which splunk_hec_in reads back as attributes beside the severity. |
//! | `attributes/span_kind` | exact | -- | The kind is the span object's `kind`; no `span.kind` attribute is written. |
//! | `attributes/trace_id_high` | exact | -- | The 128-bit trace id is the span object's `trace_id`; no `_dd.p.tid` attribute is written. |
//! | `log/severity` | exact | -- | `otel.log.severity.text` and `.number`, the band's name and base number unless the log carries either as an attribute, which splunk_hec_in reads back at band resolution. |
//! | `log/severity_unset` | exact | -- | No severity fields, read back as no severity. |
//! | `log/body_format` | lost | structural | HEC has no body format, so splunk_hec_in reads `json` for an object or array `event` and `raw` for any other: a `structured` message reads back as `json`, and a string message's `json` format as `raw`. |
//! | `log/event_name` | exact | -- | The `otel.log.name` field, read back into the event name. |
//! | `log/observed_timestamp` | lost | structural | HEC has no observed time, so it's left off. |
//! | `log/observed_timestamp_unset` | exact | -- | No observed time is written, and splunk_hec_in reads back none. |
//! | `log/trace` | exact | -- | The `trace_id` and `span_id` fields, lowercase hex, read back into the trace, unless the log carries either as an attribute. |
//! | `log/trace_unset` | normalized | `log_fields` | No trace fields are written, and splunk_hec_in reads back no trace, unless the log carries a `trace_id` attribute that parses as a trace id, with a `span_id` that parses or none, which it reads into the trace. |
//! | `log/trace_flags` | normalized | `flags_zero` | HEC has no trace flags, so a log's trace reads back with flags `0`. |
//! | `log/trace_beside_id_attribute` | lost | `logit.output.tags.omitted{reason=reserved_key}` | A `trace_id` or `span_id` attribute wins and is written as that field, and the log's own ids are left off, counted once per log; an attribute that isn't a hex id reads back as an attribute, with no trace. |
//! | `log/dropped_attributes_count` | lost | structural | HEC has no dropped count, so it's left off. |
//! | `log/timestamp` | exact | -- | The object's `time`, in seconds with up to nine decimals. |
//! | `log/receipt_timestamp` | exact | -- | The object's `time`, the time the log was received. |
//! | `log/beside_metrics` | lost | `logit.output.events.dropped{reason=blank_event}` | The log is its own HEC object before the event's metric objects, read back as an event of its own; a blank one is dropped and counted, as the `log/blank_message` row says. |
//! | `span/name` | exact | -- | The span object's `name`. |
//! | `span/name_structured` | normalized | `stringified` | A name that isn't a `Str` leaves as its text, JSON for a map or an array, read back as a string. |
//! | `span/trace_id` | exact | -- | 32 lowercase hex digits. |
//! | `span/parent_span_id` | exact | -- | 16 lowercase hex digits. |
//! | `span/parent_unset` | exact | -- | An empty `parent_span_id`, read back as no parent. |
//! | `span/kind` | exact | -- | The exporter's `SPAN_KIND_*` name. |
//! | `span/status_error` | exact | -- | `status.code` `STATUS_CODE_ERROR`. |
//! | `span/status_ok` | exact | -- | `status.code` `STATUS_CODE_OK`. |
//! | `span/status_unset` | exact | -- | `status.code` `STATUS_CODE_UNSET`, read back as unset. |
//! | `span/status_message` | exact | -- | `status.message`. |
//! | `span/timestamp` | exact | -- | `start_time`, in Unix nanoseconds. |
//! | `span/end_timestamp` | exact | -- | `end_time`, in Unix nanoseconds. |
//! | `span/end_before_start` | exact | -- | `end_time` as it is, earlier than `start_time`. |
//! | `span/flags` | lost | `logit.output.spans.degraded{reason=no_wire_form}` | The exporter's span object has no member for span flags, so it's left off, counted once per span. |
//! | `span/trace_state` | lost | `logit.output.spans.degraded{reason=no_wire_form}` | The exporter's span object has no member for a span's `trace_state`, so it's left off, counted once per span. |
//! | `span/dropped_attributes_count` | lost | `logit.output.spans.degraded{reason=no_wire_form}` | The exporter's span object has no member for a dropped-attribute count, so it's left off, counted once per span. |
//! | `span/dropped_events_count` | lost | `logit.output.spans.degraded{reason=no_wire_form}` | The exporter's span object has no member for a dropped-event count, so it's left off, counted once per span. |
//! | `span/dropped_links_count` | lost | `logit.output.spans.degraded{reason=no_wire_form}` | The exporter's span object has no member for a dropped-link count, so it's left off, counted once per span. |
//! | `span/events` | exact | -- | Each span event's `name`, `timestamp`, and `attributes`. |
//! | `span/event_nested_attribute` | exact | -- | A map, array, or null attribute of a span event, nesting kept. |
//! | `span/event_name_structured` | normalized | `stringified` | A name that isn't a `Str` leaves as its text, JSON for a map or an array, read back as a string. |
//! | `span/nested_bytes_attribute` | normalized | `stringified` | A `Bytes` attribute of a span event or link leaves as a base64 string, read back as the string. |
//! | `span/nested_u64_attribute` | normalized | `u64_as_int` | A JSON number in the span event's or link's `attributes`, read back as an `i64`, or, above `i64::MAX`, as the `u64` itself. |
//! | `span/nested_timestamp_attribute` | normalized | `stringified` | A timestamp attribute of a span event or link leaves as an RFC 3339 string, read back as the string. |
//! | `span/event_dropped_count` | lost | `logit.output.spans.degraded{reason=no_wire_form}` | The exporter's span object has no member for a span event's dropped-attribute count, so it's left off, counted once per span. |
//! | `span/links` | exact | -- | Each link's `trace_id`, `span_id`, and `attributes`. |
//! | `span/link_flags` | lost | `logit.output.spans.degraded{reason=no_wire_form}` | The exporter's span object has no member for a link's flags, so it's left off, counted once per span. |
//! | `span/link_trace_state` | exact | -- | A link's `trace_state`, an absent one written as `""` and read back as none. |
//! | `span/link_dropped_count` | lost | `logit.output.spans.degraded{reason=no_wire_form}` | The exporter's span object has no member for a link's dropped-attribute count, so it's left off, counted once per span. |
//! | `span/beside_log` | exact | -- | The span and the log are each their own HEC object, read back as events of their own. |
//! | `span/beside_metrics` | exact | -- | A span object after the event's metric objects, read back as an event of its own. |
//!
//! <!-- transit:end -->
//!
//! # Permitted normalizations
//!
//! `splunk_hec_in -> splunk_hec_out` is a fixed point modulo this list
//! (`tests/splunk_fixed_point.rs` pins it):
//!
//! 1. batching and framing: an array body leaves as concatenated objects, objects regroup into one
//!    batch per resource (first-appearance order, wire order within), and a sink splits them
//!    across requests by size;
//! 2. JSON formatting: whitespace and key order within an object, a number's spelling (`1` read as
//!    a metric value leaves as `1.0`, `1.50` as `1.5`), a string `time` leaves as a number, and
//!    `time` is written with trailing zeros trimmed;
//! 3. an absent `time` leaves as the receipt time, a non-string carrier as its JSON text, and a
//!    span's `fields` key spelled like a carrier the envelope lacks as that carrier;
//! 4. `fields` leaves flat ([`crate::json::flatten_into`]): a nested map as dotted keys, an empty
//!    map dropped, and an array holding a map or array as its JSON text;
//! 5. a metric event's single-metric form (`metric_name` + `_value`) leaves in multi-metric form
//!    (`metric_name:<n>`), its records in name order, a metric object with no `metric_type`
//!    leaves with `metric_type` `Gauge`, the OpenTelemetry exporter's form, one with no `event`
//!    leaves with `"event":"metric"`, and a measurement written as a numeric string leaves as a
//!    number;
//! 6. a span's `kind` spelled without the `SPAN_KIND_` prefix or as a number leaves as the
//!    exporter's `SPAN_KIND_*` name (unspecified as `SPAN_KIND_INTERNAL`), a status code the same
//!    way (`STATUS_CODE_*`), an absent `name` as `""`, and an absent `status` as
//!    `{"message":"","code":"STATUS_CODE_UNSET"}`;
//! 7. `/raw` splits a body into one event per line, as Splunk's line breaker does, and a relayed
//!    line leaves through `/event`;
//! 8. an unknown envelope key, a `fields` that isn't an object, an event with no `event` or a
//!    blank one, and a span's `fields` key spelled like a carrier the envelope also sets are
//!    dropped and counted;
//! 9. a hex id (a span's, parent's, or link's `trace_id`/`span_id`, and a log's `trace_id`/`span_id`
//!    fields when they parse) is read in either case and leaves lowercase;
//! 10. an object that arrived with no `host` leaves with the relay's process hostname as its
//!     `host` ([`SplunkEncoder::with_default_host`]); a `host` it carries is never replaced, except
//!     by its own `fields` `host.name` (a span object's `attributes` `host.name`) (11);
//! 11. an object's `fields` `host.name` (a span object's `attributes` `host.name`), a non-empty
//!     string, is an event attribute that leaves as the object's `host` and not as an attribute,
//!     so it returns on the resource, in a batch of its own per host; when the object also carried
//!     a `host`, that `host` is replaced.
//!
//! The metric model's losses (`Sum` temporality, the multi-number kinds under `MultiValue`) are
//! in [`metrics`]; a log's and a span's are in the generated tables above.

pub mod logs;
pub mod metrics;
pub mod response;
pub mod spans;
pub mod time;

pub use response::{HecReply, HecStatus};

use crate::json::{flatten_into, json_text, json_to_value, write_str, JsonObject};
use crate::{CodecError, Encoder, MessageBuf, MultiValue, Signal};
use logit_core::interner::{intern, resolve};
use logit_core::{AttrMap, Diagnostics, Event, EventBatch, Resource, Symbol, Telemetry, Value};
use serde_json::value::RawValue;
use serde_json::{Map, Value as Json};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, LazyLock};

/// `host.name`: the envelope's `host`.
pub const RESOURCE_ATTR_HOST_NAME: &str = logit_core::attrs::HOST_NAME;
/// `com.splunk.source`: the envelope's `source`.
pub const RESOURCE_ATTR_SOURCE: &str = "com.splunk.source";
/// `com.splunk.sourcetype`: the envelope's `sourcetype`.
pub const RESOURCE_ATTR_SOURCETYPE: &str = "com.splunk.sourcetype";
/// `com.splunk.index`: the envelope's `index`.
pub const RESOURCE_ATTR_INDEX: &str = "com.splunk.index";
/// `otel.log.severity.text` / `.number`: the OpenTelemetry exporter's severity fields, decoded
/// into `LogRecord::severity` and kept as attributes.
pub const ATTR_SEVERITY_TEXT: &str = "otel.log.severity.text";
pub const ATTR_SEVERITY_NUMBER: &str = "otel.log.severity.number";
/// `otel.log.name`: the exporter's log event name, `LogRecord::event_name`.
pub const ATTR_LOG_NAME: &str = "otel.log.name";
/// `trace_id` / `span_id`: a log's hex trace reference, `LogRecord::trace`.
pub const ATTR_TRACE_ID: &str = "trace_id";
pub const ATTR_SPAN_ID: &str = "span_id";
/// `metric_type`: a metric object's kind dimension (`Gauge`, `Sum`, or a verbatim other value).
pub const ATTR_METRIC_TYPE: &str = "metric_type";

/// The four carrier symbols, interned once.
struct Carriers {
    host: Symbol,
    source: Symbol,
    sourcetype: Symbol,
    index: Symbol,
}

static CARRIERS: LazyLock<Carriers> = LazyLock::new(|| Carriers {
    host: intern(RESOURCE_ATTR_HOST_NAME),
    source: intern(RESOURCE_ATTR_SOURCE),
    sourcetype: intern(RESOURCE_ATTR_SOURCETYPE),
    index: intern(RESOURCE_ATTR_INDEX),
});

fn is_carrier(key: Symbol) -> bool {
    let c = &*CARRIERS;
    key == c.host || key == c.source || key == c.sourcetype || key == c.index
}

/// An object's `host`, `source`, `sourcetype`, and `index`; for `/raw`, the query string's.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Envelope {
    pub host: Option<String>,
    pub source: Option<String>,
    pub sourcetype: Option<String>,
    pub index: Option<String>,
}

impl Envelope {
    /// The batch [`Resource`] this envelope names: each present field as its carrier, a `Str`.
    pub fn to_resource(&self) -> Resource {
        let mut resource = Resource::default();
        self.insert_into(&mut resource.attributes);
        resource
    }

    fn insert_into(&self, attrs: &mut AttrMap) {
        let c = &*CARRIERS;
        for (key, value) in [
            (c.host, &self.host),
            (c.source, &self.source),
            (c.sourcetype, &self.sourcetype),
            (c.index, &self.index),
        ] {
            if let Some(value) = value {
                attrs.insert_sym(key, Value::str(value.as_str()));
            }
        }
    }

    /// The carriers `resource` holds as `Str`, and how many it holds as something else.
    fn from_resource(resource: &Resource) -> (Envelope, usize) {
        let c = &*CARRIERS;
        let mut unrepresentable = 0;
        let mut read = |key: Symbol| match resource.attributes.get_sym(key) {
            None => None,
            Some(value) => match value.as_str() {
                Some(s) => Some(s.to_string()),
                None => {
                    unrepresentable += 1;
                    None
                }
            },
        };
        let envelope = Envelope {
            host: read(c.host),
            source: read(c.source),
            sourcetype: read(c.sourcetype),
            index: read(c.index),
        };
        (envelope, unrepresentable)
    }
}

/// Why a whole `/event` body was rejected: the status to answer and, for a syntax error, the
/// zero-based index of the first object that failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("HEC code {}: {}", .status.code, .status.text)]
pub struct HecError {
    pub status: HecStatus,
    pub invalid_event_number: Option<u64>,
}

impl HecError {
    fn no_data() -> Self {
        HecError { status: HecStatus::NO_DATA, invalid_event_number: None }
    }

    fn invalid_at(index: usize) -> Self {
        HecError {
            status: HecStatus::INVALID_DATA_FORMAT,
            invalid_event_number: Some(index as u64),
        }
    }

    /// The response body Splunk sends for this error.
    pub fn body(&self) -> Vec<u8> {
        self.body_with_ack(None)
    }

    /// [`HecError::body`] with `ack_id` after `invalid-event-number`, when the error names an
    /// object; a body without one never carries an `ackId`.
    pub fn body_with_ack(&self, ack_id: Option<u64>) -> Vec<u8> {
        match self.invalid_event_number {
            Some(n) => response::encode_invalid_event_acked(self.status, n, ack_id),
            None => response::encode_status(self.status),
        }
    }
}

/// What [`SplunkDecoder::decode_events_prefix`] made of a body.
#[derive(Debug)]
pub struct DecodedPrefix {
    /// The batches the objects before `error`'s (all of them, without one) decode to, one per
    /// distinct resource; empty when the decoder skipped every one of them.
    pub batches: Vec<EventBatch>,
    /// Why decoding stopped: code 6 naming the first object that failed, or code 5 for a body
    /// with none.
    pub error: Option<HecError>,
}

/// What one [`SplunkEncoder::encode_objects`] entry carries beyond its bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectMeta {
    /// The index in the batch of the event this object came from.
    pub event_index: usize,
    pub signal: Signal,
    /// The records the object carries: `1` for a log or span, the `metric_name:` count for a
    /// metric object.
    pub records: usize,
}

/// Decodes HEC request bodies. Every drop and degrade is counted through its [`Telemetry`]; a
/// default-constructed handle makes that a no-op.
#[derive(Default)]
pub struct SplunkDecoder {
    telemetry: Telemetry,
    diagnostics: Diagnostics,
}

/// One decoded object, before grouping.
type Object<'a> = BTreeMap<String, &'a RawValue>;

impl SplunkDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn with_diagnostics(mut self, diagnostics: Diagnostics) -> Self {
        self.diagnostics = diagnostics;
        self
    }

    /// Decodes one `/services/collector/event` body into one batch per distinct resource, or
    /// rejects it whole: every object is parsed before any is decoded, so a syntax error anywhere
    /// yields the error and no batch. [`SplunkDecoder::decode_events_prefix`] is the listener's
    /// form.
    pub fn decode_events(
        &mut self,
        body: &[u8],
        received_at: i64,
    ) -> Result<Vec<EventBatch>, HecError> {
        let (parsed, error) = parse_objects(body);
        if let Some(error) = error {
            return Err(error);
        }
        Ok(self.decode_parsed(parsed, received_at))
    }

    /// Decodes the objects of one `/services/collector/event` body up to the first that fails,
    /// which is what Splunk indexes: the batches objects `0..N` decode to, and the [`HecError`]
    /// naming object `N` when there is one. Nothing from object `N` on is decoded or counted.
    pub fn decode_events_prefix(&mut self, body: &[u8], received_at: i64) -> DecodedPrefix {
        let (parsed, error) = parse_objects(body);
        DecodedPrefix { batches: self.decode_parsed(parsed, received_at), error }
    }

    fn decode_parsed(
        &mut self,
        parsed: Vec<ParsedObject<'_>>,
        received_at: i64,
    ) -> Vec<EventBatch> {
        let mut groups = Groups::default();
        for object in parsed {
            self.decode_object(object, received_at, &mut groups);
        }
        groups.into_batches()
    }

    /// Decodes one `/raw` body: one log event per line, all under `envelope`'s resource.
    pub fn decode_raw(&mut self, body: &[u8], envelope: &Envelope, received_at: i64) -> EventBatch {
        let mut events = Vec::new();
        for line in body.split(|&b| b == b'\n') {
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            if line.is_empty() {
                continue;
            }
            let text = match std::str::from_utf8(line) {
                Ok(text) => text.to_string(),
                Err(_) => {
                    self.degrade_event("invalid_utf8");
                    String::from_utf8_lossy(line).into_owned()
                }
            };
            events.push(logs::raw_line(Value::str(text), received_at));
        }
        EventBatch { resource: Arc::new(envelope.to_resource()), scope: None, events }
    }

    fn decode_object(&mut self, object: ParsedObject<'_>, received_at: i64, groups: &mut Groups) {
        for _ in 0..object.unknown_keys {
            self.degrade_event("unknown_envelope_key");
        }
        let envelope = Envelope {
            host: self.read_carrier(object.host),
            source: self.read_carrier(object.source),
            sourcetype: self.read_carrier(object.sourcetype),
            index: self.read_carrier(object.index),
        };
        let timestamp = object.time.and_then(|raw| self.read_time(raw)).unwrap_or(received_at);
        let fields = match object.fields {
            None | Some(Json::Null) => Map::new(),
            Some(Json::Object(map)) => map,
            Some(_) => {
                self.degrade_event("bad_fields");
                Map::new()
            }
        };
        let event = match object.event {
            // Splunk indexes an object with no `event` whose `fields` carry a measurement as a
            // metric; SC4S sends its own metrics that way.
            None | Some(Json::Null) if metrics::is_metric_fields(&fields) => {
                if let Some(decoded) = self.decode_metric(fields, timestamp) {
                    groups.push(envelope.to_resource(), decoded);
                }
                return;
            }
            None | Some(Json::Null) => return self.undecoded(Undecoded::NoEvent),
            Some(Json::String(s)) if s.is_empty() => return self.undecoded(Undecoded::BlankEvent),
            Some(event) => event,
        };

        if event.as_str() == Some(metrics::METRIC_EVENT) && metrics::is_metric_fields(&fields) {
            if let Some(decoded) = self.decode_metric(fields, timestamp) {
                groups.push(envelope.to_resource(), decoded);
            }
            return;
        }
        if let Json::Object(span) = &event {
            if spans::is_span(span) {
                match self.decode_span(span) {
                    Some(decoded) => {
                        let resource = self.span_resource(&envelope, &fields);
                        groups.push(resource, decoded);
                        return;
                    }
                    None => {
                        self.telemetry.count(
                            "logit.input.spans.degraded",
                            1.0,
                            &[("reason", "malformed_span")],
                        );
                        self.diagnostics.warn_throttled(
                            "malformed_span",
                            "HEC span object has a field that doesn't parse; kept as a log",
                        );
                    }
                }
            }
        }
        let decoded = self.decode_log(event, fields, timestamp);
        groups.push(envelope.to_resource(), decoded);
    }

    /// A span's resource: its carriers plus its `fields`, which the OpenTelemetry exporter fills
    /// from the span's resource attributes. A field spelled like a carrier the envelope also sets
    /// loses to the envelope.
    fn span_resource(&self, envelope: &Envelope, fields: &Map<String, Json>) -> Resource {
        let mut resource = Resource::default();
        for (key, value) in fields {
            if is_carrier(intern(key)) && envelope_has(envelope, key) {
                self.telemetry.count(
                    "logit.input.spans.degraded",
                    1.0,
                    &[("reason", "carrier_collision")],
                );
                continue;
            }
            flatten_into(key, &json_to_value(value), &mut resource.attributes);
        }
        envelope.insert_into(&mut resource.attributes);
        resource
    }

    /// `None` for `null`; an unparseable number or string, or any other type, is counted.
    fn read_time(&self, raw: &RawValue) -> Option<i64> {
        let text = raw.get();
        let nanos = match text.as_bytes().first() {
            _ if text == "null" => return None,
            Some(b'-' | b'0'..=b'9') => time::parse_hec_time(text),
            Some(b'"') => serde_json::from_str::<String>(text)
                .ok()
                .and_then(|s| time::parse_hec_time(s.trim())),
            _ => None,
        };
        if nanos.is_none() {
            self.degrade_event("bad_time");
        }
        nanos
    }

    fn read_carrier(&self, value: Option<Json>) -> Option<String> {
        match value? {
            Json::Null => None,
            Json::String(s) => Some(s),
            other => {
                self.degrade_event("non_string_envelope");
                Some(other.to_string())
            }
        }
    }

    fn degrade_event(&self, reason: &'static str) {
        self.telemetry.count("logit.input.events.degraded", 1.0, &[("reason", reason)]);
    }

    pub(super) fn undecoded(&self, why: Undecoded) {
        let (metric, reason) = why.counter();
        self.telemetry.count(metric, 1.0, &[("reason", reason)]);
    }
}

/// Every object or measurement the decoder leaves out of its batches, and the counter and reason
/// each one is counted under. Malformed input is loss (`dropped`); a metric event with nothing
/// left to carry is `skipped`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Undecoded {
    NoEvent,
    BlankEvent,
    NoMetric,
    BadName,
    BadValue,
    DuplicateName,
    IncompleteSingleMetric,
}

impl Undecoded {
    #[cfg(test)]
    const ALL: [Self; 7] = [
        Self::NoEvent,
        Self::BlankEvent,
        Self::NoMetric,
        Self::BadName,
        Self::BadValue,
        Self::DuplicateName,
        Self::IncompleteSingleMetric,
    ];

    fn counter(self) -> (&'static str, &'static str) {
        const EVENTS_DROPPED: &str = "logit.input.events.dropped";
        const METRICS_DROPPED: &str = "logit.input.metrics.dropped";
        match self {
            Self::NoEvent => (EVENTS_DROPPED, "no_event"),
            Self::BlankEvent => (EVENTS_DROPPED, "blank_event"),
            Self::NoMetric => ("logit.input.events.skipped", "no_metric"),
            Self::BadName => (METRICS_DROPPED, "bad_name"),
            Self::BadValue => (METRICS_DROPPED, "bad_value"),
            Self::DuplicateName => (METRICS_DROPPED, "duplicate_name"),
            Self::IncompleteSingleMetric => (METRICS_DROPPED, "incomplete_single_metric"),
        }
    }
}

/// Every object or record the encoder leaves off the wire, and the counter and tag each one is
/// counted under. A gap the destination can't take or an upstream stage didn't resolve is loss
/// (`dropped`); a kind the format has no form for, the operator's `multi_value: skip`, or a record
/// with nothing to carry is `skipped`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Unencoded {
    /// An event with no log, metric, or span.
    NoPayload,
    /// A log whose message HEC would reject as a blank `event`.
    BlankEvent,
    /// A record flagged `NO_RECORDED_VALUE`.
    NoRecordedValue,
    /// A `GaugeDelta` no `aggregate` upstream resolved.
    UnresolvedGaugeDelta,
    /// A metric name another record of the same object already wrote.
    DuplicateName,
    /// A kind HEC has no form for, or one `MultiValue::Skip` leaves out, by its
    /// [`MetricKind::name`](logit_core::MetricKind::name).
    Kind(&'static str),
}

impl Unencoded {
    #[cfg(test)]
    const ALL: [Self; 6] = [
        Self::NoPayload,
        Self::BlankEvent,
        Self::NoRecordedValue,
        Self::UnresolvedGaugeDelta,
        Self::DuplicateName,
        Self::Kind("histogram"),
    ];

    fn counter(self) -> (&'static str, (&'static str, &'static str)) {
        const METRICS_DROPPED: &str = "logit.output.metrics.dropped";
        const METRICS_SKIPPED: &str = "logit.output.metrics.skipped";
        match self {
            Self::NoPayload => ("logit.output.events.skipped", ("reason", "no_payload")),
            Self::BlankEvent => ("logit.output.events.dropped", ("reason", "blank_event")),
            Self::NoRecordedValue => (METRICS_SKIPPED, ("reason", "no_recorded_value")),
            Self::UnresolvedGaugeDelta => (METRICS_DROPPED, ("reason", "unresolved_gauge_delta")),
            Self::DuplicateName => (METRICS_DROPPED, ("reason", "duplicate_name")),
            Self::Kind(kind) => (METRICS_SKIPPED, ("metric_kind", kind)),
        }
    }
}

fn envelope_has(envelope: &Envelope, carrier: &str) -> bool {
    match carrier {
        RESOURCE_ATTR_HOST_NAME => envelope.host.is_some(),
        RESOURCE_ATTR_SOURCE => envelope.source.is_some(),
        RESOURCE_ATTR_SOURCETYPE => envelope.sourcetype.is_some(),
        RESOURCE_ATTR_INDEX => envelope.index.is_some(),
        _ => false,
    }
}

/// One object's envelope members, parsed out of their `RawValue`s. `time` stays raw so its
/// number text keeps every digit.
struct ParsedObject<'a> {
    time: Option<&'a RawValue>,
    host: Option<Json>,
    source: Option<Json>,
    sourcetype: Option<Json>,
    index: Option<Json>,
    event: Option<Json>,
    fields: Option<Json>,
    unknown_keys: usize,
}

impl<'a> ParsedObject<'a> {
    /// `None` when a member is syntactically valid JSON that `serde_json::Value` can't hold: a
    /// number outside `f64`'s range (`1e400`), or nesting past `serde_json`'s recursion limit.
    /// The object is unrepresentable, so the body is rejected as Splunk rejects invalid data.
    fn parse(object: &Object<'a>) -> Option<Self> {
        let parse = |raw: &RawValue| serde_json::from_str::<Json>(raw.get()).ok();
        let mut parsed = ParsedObject {
            time: None,
            host: None,
            source: None,
            sourcetype: None,
            index: None,
            event: None,
            fields: None,
            unknown_keys: 0,
        };
        for (key, raw) in object {
            let raw: &'a RawValue = raw;
            match key.as_str() {
                "time" => parsed.time = Some(raw),
                "host" => parsed.host = Some(parse(raw)?),
                "source" => parsed.source = Some(parse(raw)?),
                "sourcetype" => parsed.sourcetype = Some(parse(raw)?),
                "index" => parsed.index = Some(parse(raw)?),
                "event" => parsed.event = Some(parse(raw)?),
                "fields" => parsed.fields = Some(parse(raw)?),
                _ => parsed.unknown_keys += 1,
            }
        }
        Some(parsed)
    }
}

/// Parses a body's objects in order up to the first that fails: the parsed ones, and the error
/// naming the failed object's index. A body with no object is code 5.
fn parse_objects(body: &[u8]) -> (Vec<ParsedObject<'_>>, Option<HecError>) {
    let (objects, mut error) = split_objects(body);
    let mut parsed = Vec::with_capacity(objects.len());
    for (i, object) in objects.iter().enumerate() {
        match ParsedObject::parse(object) {
            Some(object) => parsed.push(object),
            None => {
                error = Some(HecError::invalid_at(i));
                break;
            }
        }
    }
    (parsed, error)
}

/// Splits a body into its objects, a JSON array's elements or concatenated top-level objects,
/// up to the first that isn't a valid JSON object: the objects before it, and the error naming
/// its index.
fn split_objects(body: &[u8]) -> (Vec<Object<'_>>, Option<HecError>) {
    let Some(start) = body.iter().position(|b| !b.is_ascii_whitespace()) else {
        return (Vec::new(), Some(HecError::no_data()));
    };
    let body = &body[start..];
    let (objects, error) = if body[0] == b'[' {
        match serde_json::from_slice::<Vec<Object<'_>>>(body) {
            Ok(objects) => (objects, None),
            Err(_) => array_prefix(body),
        }
    } else {
        let mut objects = Vec::new();
        let mut error = None;
        for (i, item) in
            serde_json::Deserializer::from_slice(body).into_iter::<Object<'_>>().enumerate()
        {
            match item {
                Ok(object) => objects.push(object),
                Err(_) => {
                    error = Some(HecError::invalid_at(i));
                    break;
                }
            }
        }
        (objects, error)
    };
    if objects.is_empty() && error.is_none() {
        return (objects, Some(HecError::no_data()));
    }
    (objects, error)
}

/// The elements of a JSON array body (which failed to parse as a whole) before the first that
/// isn't a valid object, and the error naming that element: a scan for the array's top-level
/// commas, tracking strings and nesting, then a parse of each element. When every element
/// parses, the fault is in the array's own syntax after the last one (a missing `]`, trailing
/// bytes), and the index is the element count.
fn array_prefix(body: &[u8]) -> (Vec<Object<'_>>, Option<HecError>) {
    let mut elements = Vec::new();
    let (mut depth, mut in_string, mut escaped, mut from) = (0usize, false, false, 1usize);
    let mut closed = false;
    for (i, &b) in body.iter().enumerate() {
        if in_string {
            match b {
                _ if escaped => escaped = false,
                b'\\' => escaped = true,
                b'"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'[' | b'{' => depth += 1,
            b']' | b'}' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    elements.push(&body[from..i]);
                    closed = true;
                    break;
                }
            }
            b',' if depth == 1 => {
                elements.push(&body[from..i]);
                from = i + 1;
            }
            _ => {}
        }
    }
    if !closed {
        elements.push(&body[from.min(body.len())..]);
    }
    let mut objects = Vec::with_capacity(elements.len());
    for element in elements {
        match serde_json::from_slice::<Object<'_>>(element) {
            Ok(object) => objects.push(object),
            // `[]` scans as one empty element; an empty element anywhere else is a stray comma.
            Err(_) => break,
        }
    }
    let index = objects.len();
    (objects, Some(HecError::invalid_at(index)))
}

/// Decoded events grouped by resource, in first-appearance order.
#[derive(Default)]
struct Groups {
    batches: Vec<(Resource, Vec<Event>)>,
    /// The resource's attributes as JSON text → its index in `batches`.
    index: HashMap<String, usize>,
}

impl Groups {
    fn push(&mut self, resource: Resource, event: Event) {
        let key = json_text(&Value::Map(Box::new(resource.attributes.clone())));
        let slot = *self.index.entry(key).or_insert_with(|| {
            self.batches.push((resource, Vec::new()));
            self.batches.len() - 1
        });
        self.batches[slot].1.push(event);
    }

    fn into_batches(self) -> Vec<EventBatch> {
        self.batches
            .into_iter()
            .map(|(resource, events)| EventBatch {
                resource: Arc::new(resource),
                scope: None,
                events,
            })
            .collect()
    }
}

/// Encodes event batches into HEC `/event` objects. Every skip and degrade is counted through its
/// [`Telemetry`]; a disabled handle costs nothing.
#[derive(Default)]
pub struct SplunkEncoder {
    telemetry: Telemetry,
    diagnostics: Diagnostics,
    multi_value: MultiValue,
    /// The `host` of an object whose event and resource have no `host.name`.
    default_host: Option<Arc<str>>,
    /// One object's bytes, reused across objects.
    scratch: Vec<u8>,
}

/// What every object of one batch shares.
struct BatchContext<'a> {
    envelope: Envelope,
    resource: &'a Resource,
    /// [`SplunkEncoder::with_default_host`]'s value, set only when the resource has no
    /// `host.name`. Shared rather than copied into `envelope`, so filling costs no allocation.
    default_host: Option<Arc<str>>,
}

impl BatchContext<'_> {
    /// `event`'s object `host`: its own non-empty `Str` `host.name`, else the resource's, else the
    /// default. A `host.name` of another type, a `Bytes` included, isn't a host and stays a field.
    fn host<'e>(&'e self, event: &'e Event) -> ObjectHost<'e> {
        match event.attributes.get_sym(CARRIERS.host).and_then(Value::as_str) {
            Some(host) if !host.is_empty() => ObjectHost { host: Some(host), from_event: true },
            _ => ObjectHost {
                host: self.envelope.host.as_deref().or(self.default_host.as_deref()),
                from_event: false,
            },
        }
    }
}

/// One object's `host`, and whether the event's `host.name` supplied it. That attribute is then
/// the envelope's, so the object doesn't also write it among its attributes.
#[derive(Clone, Copy)]
struct ObjectHost<'a> {
    host: Option<&'a str>,
    from_event: bool,
}

impl ObjectHost<'_> {
    /// The event attribute the envelope consumed, if any.
    fn consumed(self) -> Option<Symbol> {
        self.from_event.then(|| CARRIERS.host)
    }
}

impl SplunkEncoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn with_diagnostics(mut self, diagnostics: Diagnostics) -> Self {
        self.diagnostics = diagnostics;
        self
    }

    /// What a metric kind with no single number does: skipped (the default) or expanded into the
    /// series [`metrics`]'s module doc lists.
    pub fn with_multi_value(mut self, multi_value: MultiValue) -> Self {
        self.multi_value = multi_value;
        self
    }

    /// The `host` written for an object whose event and resource carry no `host.name`. A resource
    /// `host.name` that isn't a `Str` is still the batch's own: an object without its own host
    /// omits `host`, counted, and never takes this one.
    pub fn with_default_host(mut self, host: Option<Arc<str>>) -> Self {
        self.default_host = host;
        self
    }

    /// Writes one HEC object per entry of `out` (cleared first), in batch order.
    pub fn encode_objects(&mut self, batch: &EventBatch, out: &mut MessageBuf<ObjectMeta>) {
        out.clear();
        let (envelope, unrepresentable) = Envelope::from_resource(&batch.resource);
        if unrepresentable > 0 {
            self.telemetry.count(
                "logit.output.tags.omitted",
                unrepresentable as f64,
                &[("reason", "unrepresentable")],
            );
        }
        let default_host = match batch.resource.attributes.get_sym(CARRIERS.host) {
            None => self.default_host.clone(),
            Some(_) => None,
        };
        let ctx = BatchContext { envelope, resource: &batch.resource, default_host };
        for (event_index, event) in batch.events.iter().enumerate() {
            if event.log.is_none() && event.metrics.is_empty() && event.span.is_none() {
                self.unencoded(Unencoded::NoPayload, 1);
                continue;
            }
            if let Some(log) = &event.log {
                self.encode_log(&ctx, event, log, event_index, out);
            }
            if !event.metrics.is_empty() {
                self.encode_metrics(&ctx, event, event_index, out);
            }
            if let Some(span) = &event.span {
                self.encode_span(&ctx, event, span, event_index, out);
            }
        }
    }

    /// `fields` for a log or metric object: every non-carrier resource attribute, then every
    /// event attribute but the one `host` consumed, flattened, the event's value winning on a key.
    fn object_fields(resource: &Resource, event: &Event, host: ObjectHost<'_>) -> AttrMap {
        let mut fields = AttrMap::new();
        for (key, value) in resource.attributes.iter() {
            let name = resolve(key);
            if !is_carrier(key) && !logit_core::attrs::is_private(name) {
                flatten_into(name, value, &mut fields);
            }
        }
        let consumed = host.consumed();
        for (key, value) in event.attributes.iter() {
            let name = resolve(key);
            if Some(key) != consumed && !logit_core::attrs::is_private(name) {
                flatten_into(name, value, &mut fields);
            }
        }
        fields
    }

    fn reserved_key_omitted(&self, n: usize) {
        if n > 0 {
            self.telemetry.count(
                "logit.output.tags.omitted",
                n as f64,
                &[("reason", "reserved_key")],
            );
        }
    }

    pub(super) fn unencoded(&self, why: Unencoded, n: usize) {
        let (metric, tag) = why.counter();
        self.telemetry.count(metric, n as f64, &[tag]);
    }
}

/// Writes an object's envelope (`time`, `host` from [`BatchContext::host`], and every other
/// carrier present) and returns it open, for the caller to add `event` and `fields`.
fn begin_object<'a>(
    out: &'a mut Vec<u8>,
    timestamp: i64,
    ctx: &BatchContext<'_>,
    host: ObjectHost<'_>,
) -> JsonObject<'a> {
    let mut obj = JsonObject::begin(out);
    time::write_hec_time(obj.key("time"), timestamp);
    let envelope = &ctx.envelope;
    for (key, value) in [
        ("host", host.host),
        ("source", envelope.source.as_deref()),
        ("sourcetype", envelope.sourcetype.as_deref()),
        ("index", envelope.index.as_deref()),
    ] {
        if let Some(value) = value {
            write_str(obj.key(key), value);
        }
    }
    obj
}

/// Writes `fields` as a JSON object member, when non-empty.
fn write_fields(
    obj: &mut JsonObject<'_>,
    fields: &AttrMap,
    trailer: impl FnOnce(&mut JsonObject<'_>),
) {
    let mut inner = JsonObject::begin(obj.key("fields"));
    for (key, value) in fields.iter() {
        crate::json::write_value(inner.key(resolve(key)), value);
    }
    trailer(&mut inner);
    inner.finish();
}

impl Encoder for SplunkEncoder {
    /// The batch's objects concatenated, one `/event` body.
    fn encode(&mut self, batch: &EventBatch) -> Result<bytes::Bytes, CodecError> {
        let mut objects = MessageBuf::default();
        self.encode_objects(batch, &mut objects);
        let mut body = Vec::with_capacity(objects.total_bytes());
        for object in objects.iter() {
            body.extend_from_slice(object);
        }
        Ok(bytes::Bytes::from(body))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use logit_core::telemetry::Registry;
    use logit_core::MetricKind;

    pub(crate) const RECEIVED_AT: i64 = 1_690_000_000_000_000_000;

    thread_local! {
        /// Every telemetry point drained so far, per registry, so [`counted`] can be called more
        /// than once on one registry.
        static DRAINED: std::cell::RefCell<HashMap<usize, Vec<Event>>> = Default::default();
    }

    /// The summed value of every `metric` point tagged `tag` that `registry` has recorded.
    pub(crate) fn counted(registry: &Registry, metric: &str, tag: (&str, &str)) -> f64 {
        DRAINED.with(|drained| {
            let mut drained = drained.borrow_mut();
            let events = drained.entry(registry as *const Registry as usize).or_default();
            events.extend(registry.drain(0));
            let mut total = 0.0;
            for event in events.iter() {
                if event.attributes.get(tag.0).and_then(|v| v.as_str()) != Some(tag.1) {
                    continue;
                }
                for m in &event.metrics {
                    if resolve(m.name) == metric {
                        if let MetricKind::Sum(sum) = &m.kind {
                            total += sum.value.as_f64();
                        }
                    }
                }
            }
            total
        })
    }

    pub(crate) fn decoder(registry: &Registry) -> SplunkDecoder {
        SplunkDecoder::new().with_telemetry(registry.telemetry_for(
            "splunk_hec_in",
            "splunk_hec_in",
            "listener",
        ))
    }

    pub(crate) fn encoder(registry: &Registry) -> SplunkEncoder {
        SplunkEncoder::new().with_telemetry(registry.telemetry_for(
            "splunk_hec_out",
            "splunk_hec_out",
            "sink",
        ))
    }

    pub(crate) fn decode(body: &str) -> Vec<EventBatch> {
        SplunkDecoder::new().decode_events(body.as_bytes(), RECEIVED_AT).expect("decodes")
    }

    pub(crate) fn encode(batches: &[EventBatch]) -> String {
        let mut encoder = SplunkEncoder::new();
        let mut out = String::new();
        for batch in batches {
            out.push_str(std::str::from_utf8(&encoder.encode(batch).unwrap()).unwrap());
        }
        out
    }

    #[test]
    fn empty_bodies_are_no_data() {
        let mut decoder = SplunkDecoder::new();
        for body in ["", "  \n", "[]", " [ ] "] {
            let err = decoder.decode_events(body.as_bytes(), 0).unwrap_err();
            assert_eq!(err.status, HecStatus::NO_DATA, "{body:?}");
            assert_eq!(err.body(), br#"{"text":"No data","code":5}"#.to_vec());
        }
    }

    #[test]
    fn a_syntax_error_names_the_first_bad_object() {
        let mut decoder = SplunkDecoder::new();
        for (body, index) in [
            (r#"{"event":"a"}{"event":"b""#, 1),
            (r#"{"event":"a"}{"event":"b"}x"#, 2),
            (r#"{"event":"a"} 7 {"event":"b"}"#, 1),
            (r#"[{"event":"a"},{"event":"b",},{"event":"c"}]"#, 1),
            (r#"[{"event":"a,]"},{"event":"b"}"#, 2),
            (r#"[{"event":"a"},"x"]"#, 1),
            (r#"[{"event":"a"}] {"#, 1),
            (r#"not json"#, 0),
        ] {
            let err = decoder.decode_events(body.as_bytes(), 0).unwrap_err();
            assert_eq!(err.status, HecStatus::INVALID_DATA_FORMAT, "{body}");
            assert_eq!(err.invalid_event_number, Some(index), "{body}");
        }
        let err = decoder.decode_events(br#"{"event":"a"}{"#, 0).unwrap_err();
        assert_eq!(
            err.body(),
            br#"{"text":"Invalid data format","code":6,"invalid-event-number":1}"#.to_vec()
        );
    }

    #[test]
    fn the_prefix_form_decodes_the_objects_before_the_error_and_names_the_same_index() {
        let deep = format!(r#"{{"event":{}1{}}}"#, "[".repeat(130), "]".repeat(130));
        let cases = [
            (r#"{"event":"a"}{"event":"b""#.to_string(), 1),
            (r#"{"event":"a"}{"event":"b"}x"#.to_string(), 2),
            (r#"{"event":"a"} 7 {"event":"b"}"#.to_string(), 1),
            (r#"[{"event":"a"},{"event":"b",},{"event":"c"}]"#.to_string(), 1),
            (r#"[{"event":"a,]"},{"event":"b"}"#.to_string(), 2),
            (r#"[{"event":"a"}] {"#.to_string(), 1),
            (r#"not json"#.to_string(), 0),
            (r#"{"event":"a"}{"event":"x","fields":{"a":1e400}}"#.to_string(), 1),
            (format!(r#"{{"event":"a"}}{{"event":"b"}}{deep}"#), 2),
        ];
        for (body, index) in cases {
            let strict = SplunkDecoder::new().decode_events(body.as_bytes(), 0).unwrap_err();
            let prefix = SplunkDecoder::new().decode_events_prefix(body.as_bytes(), 0);
            assert_eq!(prefix.error, Some(strict), "{body}");
            assert_eq!(strict.invalid_event_number, Some(index as u64), "{body}");
            let events: usize = prefix.batches.iter().map(|b| b.events.len()).sum();
            assert_eq!(events, index, "{body}");
        }

        let whole = r#"{"event":"a","host":"h1"}{"event":"b","host":"h2"}"#;
        let prefix = SplunkDecoder::new().decode_events_prefix(whole.as_bytes(), RECEIVED_AT);
        assert_eq!(prefix.error, None);
        assert_eq!(prefix.batches, decode(whole));
        for body in ["", " \n", "[]"] {
            let prefix = SplunkDecoder::new().decode_events_prefix(body.as_bytes(), 0);
            assert!(prefix.batches.is_empty(), "{body:?}");
            assert_eq!(prefix.error.map(|e| e.status), Some(HecStatus::NO_DATA), "{body:?}");
        }
    }

    #[test]
    fn the_prefix_form_groups_by_resource_and_counts_only_what_it_decoded() {
        let registry = Registry::new();
        let body = concat!(
            r#"{"event":"a","host":"h1"}{"event":"b","host":"h2"}{"event":"","host":"h1"}"#,
            r#"{"event":"c","host":"h1"{"event":"","host":"h1"}"#,
        );
        let prefix = decoder(&registry).decode_events_prefix(body.as_bytes(), RECEIVED_AT);
        assert_eq!(prefix.error.and_then(|e| e.invalid_event_number), Some(3));
        let hosts: Vec<usize> = prefix.batches.iter().map(|b| b.events.len()).collect();
        assert_eq!(hosts, [1, 1], "h1's a, then h2's b; the blank object dropped");
        assert_eq!(
            counted(&registry, "logit.input.events.dropped", ("reason", "blank_event")),
            1.0,
            "the blank object after the error isn't decoded"
        );
    }

    #[test]
    fn a_member_the_model_cant_hold_rejects_the_body_with_its_index() {
        let deep = format!(r#"{{"event":{}1{}}}"#, "[".repeat(130), "]".repeat(130));
        let cases = [
            (r#"{"event":"a"}{"event":"x","fields":{"a":1e400,"b":"keep"}}"#.to_string(), 1),
            (r#"{"event":{"n":1e400}}"#.to_string(), 0),
            (r#"{"event":"metric","fields":{"metric_name:x":1e400}}"#.to_string(), 0),
            (r#"{"event":"x","host":1e400}"#.to_string(), 0),
            (format!(r#"{{"event":"a"}}{{"event":"b"}}{deep}"#), 2),
        ];
        for (body, index) in cases {
            let err = SplunkDecoder::new().decode_events(body.as_bytes(), 0).unwrap_err();
            assert_eq!(err.status, HecStatus::INVALID_DATA_FORMAT, "{body}");
            assert_eq!(err.invalid_event_number, Some(index), "{body}");
        }
    }

    #[test]
    fn an_unparseable_time_number_is_counted() {
        let registry = Registry::new();
        let batches = decoder(&registry)
            .decode_events(br#"{"event":"a","time":1e400}{"event":"b","time":"x"}"#, RECEIVED_AT)
            .unwrap();
        assert!(batches[0].events.iter().all(|e| e.timestamp == RECEIVED_AT));
        assert_eq!(counted(&registry, "logit.input.events.degraded", ("reason", "bad_time")), 2.0);
    }

    #[test]
    fn objects_group_by_resource_in_first_appearance_order() {
        let batches = decode(
            r#"{"event":"a","host":"h1"}{"event":"b","host":"h2","index":"i"}{"event":"c","host":"h1"}"#,
        );
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].resource.attributes.get("host.name"), Some(&Value::str("h1")));
        assert_eq!(batches[0].events.len(), 2);
        let messages: Vec<_> =
            batches[0].events.iter().map(|e| e.log.as_ref().unwrap().message.clone()).collect();
        assert_eq!(messages, vec![Value::str("a"), Value::str("c")]);
        assert_eq!(batches[1].resource.attributes.get("com.splunk.index"), Some(&Value::str("i")));
        assert_eq!(batches[1].resource.attributes.len(), 2);
    }

    #[test]
    fn an_array_body_and_concatenated_objects_decode_alike() {
        let concatenated = decode(r#"{"event":"a","time":1} {"event":"b","time":2}"#);
        let array = decode(r#"[{"event":"a","time":1},{"event":"b","time":2}]"#);
        assert_eq!(concatenated, array);
    }

    #[test]
    fn envelope_oddities_are_counted() {
        let registry = Registry::new();
        let batches = decoder(&registry)
            .decode_events(
                br#"{"event":"a","host":7,"extra":1,"fields":[1],"time":true}{"event":null}{"event":""}{"time":1}"#,
                RECEIVED_AT,
            )
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].resource.attributes.get("host.name"), Some(&Value::str("7")));
        assert_eq!(batches[0].events[0].timestamp, RECEIVED_AT);
        for reason in ["non_string_envelope", "unknown_envelope_key", "bad_fields", "bad_time"] {
            assert_eq!(counted(&registry, "logit.input.events.degraded", ("reason", reason)), 1.0);
        }
        assert_eq!(counted(&registry, "logit.input.events.dropped", ("reason", "no_event")), 2.0);
        assert_eq!(
            counted(&registry, "logit.input.events.dropped", ("reason", "blank_event")),
            1.0
        );
        assert_eq!(counted(&registry, "logit.input.events.skipped", ("reason", "no_event")), 0.0);
    }

    #[test]
    fn time_forms_decode_to_nanoseconds() {
        let batches = decode(
            r#"{"event":"a","time":1700000000.123456789}{"event":"b","time":"1700000000.5"}{"event":"c","time":0}{"event":"d","time":null}"#,
        );
        let times: Vec<i64> = batches[0].events.iter().map(|e| e.timestamp).collect();
        assert_eq!(
            times,
            vec![1_700_000_000_123_456_789, 1_700_000_000_500_000_000, 0, RECEIVED_AT]
        );
    }

    #[test]
    fn raw_bodies_split_on_lines_under_the_query_envelope() {
        let registry = Registry::new();
        let envelope = Envelope {
            host: Some("h".into()),
            sourcetype: Some("syslog".into()),
            ..Envelope::default()
        };
        let batch =
            decoder(&registry).decode_raw(b"one\r\ntwo\n\n\xffthree\n", &envelope, RECEIVED_AT);
        assert_eq!(batch.resource.attributes.get("host.name"), Some(&Value::str("h")));
        assert_eq!(
            batch.resource.attributes.get("com.splunk.sourcetype"),
            Some(&Value::str("syslog"))
        );
        let messages: Vec<_> =
            batch.events.iter().map(|e| e.log.as_ref().unwrap().message.clone()).collect();
        assert_eq!(
            messages,
            vec![Value::str("one"), Value::str("two"), Value::str("\u{fffd}three")]
        );
        assert!(batch.events.iter().all(|e| e.timestamp == RECEIVED_AT));
        assert_eq!(
            counted(&registry, "logit.input.events.degraded", ("reason", "invalid_utf8")),
            1.0
        );

        // A relayed `/raw` line leaves through `/event` and comes back the same.
        let again = decode(&encode(std::slice::from_ref(&batch)));
        assert_eq!(again, vec![batch]);
    }

    #[test]
    fn a_non_string_carrier_is_omitted_and_counted() {
        let registry = Registry::new();
        let mut batch = decode(r#"{"event":"a","time":1,"host":"h"}"#).remove(0);
        let mut resource = (*batch.resource).clone();
        resource.attributes.insert("com.splunk.index", Value::I64(3));
        batch.resource = Arc::new(resource);
        let mut out = MessageBuf::default();
        encoder(&registry).encode_objects(&batch, &mut out);
        assert_eq!(out.iter().next().unwrap(), br#"{"time":1,"host":"h","event":"a"}"#);
        assert_eq!(
            counted(&registry, "logit.output.tags.omitted", ("reason", "unrepresentable")),
            1.0
        );
    }

    /// One object per payload of `body`'s first batch, encoded with `relay` as the default host.
    fn encode_with_default_host(body: &str) -> Vec<Vec<u8>> {
        let batch = decode(body).remove(0);
        let mut out = MessageBuf::default();
        SplunkEncoder::new()
            .with_default_host(Some(Arc::from("relay")))
            .encode_objects(&batch, &mut out);
        out.iter().map(<[u8]>::to_vec).collect()
    }

    #[test]
    fn a_hostless_object_of_every_signal_leaves_with_the_default_host() {
        let objects = encode_with_default_host(
            r#"{"event":"a","time":1}{"time":1,"event":"metric","fields":{"metric_name:m":1}}{"time":1,"event":{"trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","span_id":"00f067aa0ba902b7","start_time":1,"end_time":2}}"#,
        );
        assert_eq!(objects.len(), 3);
        for object in &objects {
            assert!(
                String::from_utf8_lossy(object).contains(r#","host":"relay","event":"#),
                "{}",
                String::from_utf8_lossy(object)
            );
        }
    }

    #[test]
    fn a_host_the_object_carries_beats_the_default_host() {
        let objects = encode_with_default_host(r#"{"event":"a","time":1,"host":"web-1"}"#);
        assert_eq!(objects, vec![br#"{"time":1,"host":"web-1","event":"a"}"#.to_vec()]);
    }

    #[test]
    fn a_non_string_host_name_is_omitted_not_replaced_by_the_default_host() {
        let mut batch = decode(r#"{"event":"a","time":1}"#).remove(0);
        let mut resource = (*batch.resource).clone();
        resource.attributes.insert("host.name", Value::I64(3));
        batch.resource = Arc::new(resource);
        let mut out = MessageBuf::default();
        SplunkEncoder::new()
            .with_default_host(Some(Arc::from("relay")))
            .encode_objects(&batch, &mut out);
        assert_eq!(out.iter().next().unwrap(), br#"{"time":1,"event":"a"}"#);
    }

    /// `body`'s first batch, its resource carrying `host.name` `res` when `resource_host` is set,
    /// encoded with `relay` as the default host, one string per object.
    fn encode_hosts(body: &str, resource_host: bool) -> Vec<String> {
        let mut batch = decode(body).remove(0);
        if resource_host {
            let mut resource = (*batch.resource).clone();
            resource.attributes.insert("host.name", Value::str("res"));
            batch.resource = Arc::new(resource);
        }
        let mut out = MessageBuf::default();
        SplunkEncoder::new()
            .with_default_host(Some(Arc::from("relay")))
            .encode_objects(&batch, &mut out);
        out.iter().map(|o| String::from_utf8(o.to_vec()).unwrap()).collect()
    }

    #[test]
    fn an_event_host_name_is_the_object_host_of_every_signal_and_not_a_field() {
        let objects = encode_hosts(
            concat!(
                r#"{"time":1,"event":"a","fields":{"host.name":"web-1","k":"v"}}"#,
                r#"{"time":1,"event":"metric","fields":{"metric_name:m":1,"host.name":"web-1"}}"#,
                r#"{"time":1,"event":{"trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","span_id":"00f067aa0ba902b7","start_time":1,"end_time":2,"attributes":{"host.name":"web-1","k":"v"}}}"#,
            ),
            true,
        );
        assert_eq!(
            objects,
            [
                r#"{"time":1,"host":"web-1","event":"a","fields":{"k":"v"}}"#,
                r#"{"time":1,"host":"web-1","event":"metric","fields":{"metric_type":"Gauge","metric_name:m":1.0}}"#,
                r#"{"time":0.000000001,"host":"web-1","event":{"trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","span_id":"00f067aa0ba902b7","parent_span_id":"","name":"","attributes":{"k":"v"},"end_time":2,"kind":"SPAN_KIND_INTERNAL","status":{"message":"","code":"STATUS_CODE_UNSET"},"start_time":1}}"#,
            ]
        );
    }

    #[test]
    fn each_object_takes_its_own_event_host_else_the_resource_else_the_default() {
        let body = concat!(
            r#"{"time":1,"event":"a","fields":{"host.name":"web-1"}}"#,
            r#"{"time":1,"event":"b"}"#,
            r#"{"time":1,"event":"c","fields":{"host.name":"web-2"}}"#,
        );
        assert_eq!(
            encode_hosts(body, true),
            [
                r#"{"time":1,"host":"web-1","event":"a"}"#,
                r#"{"time":1,"host":"res","event":"b"}"#,
                r#"{"time":1,"host":"web-2","event":"c"}"#,
            ]
        );
        assert_eq!(encode_hosts(body, false)[1], r#"{"time":1,"host":"relay","event":"b"}"#);
    }

    /// An empty `Str` and a `Bytes` aren't a host: the object takes the next one, and the
    /// attribute stays a field.
    #[test]
    fn an_empty_or_bytes_event_host_name_falls_through_and_stays_a_field() {
        let empty = r#"{"time":1,"event":"a","fields":{"host.name":""}}"#;
        assert_eq!(
            encode_hosts(empty, true),
            [r#"{"time":1,"host":"res","event":"a","fields":{"host.name":""}}"#]
        );
        assert_eq!(
            encode_hosts(empty, false),
            [r#"{"time":1,"host":"relay","event":"a","fields":{"host.name":""}}"#]
        );

        let mut batch = decode(r#"{"time":1,"event":"a"}"#).remove(0);
        batch.events[0]
            .attributes
            .insert("host.name", Value::Bytes(bytes::Bytes::from_static(b"web-1")));
        let mut out = MessageBuf::default();
        SplunkEncoder::new()
            .with_default_host(Some(Arc::from("relay")))
            .encode_objects(&batch, &mut out);
        assert_eq!(
            out.iter().next().unwrap(),
            br#"{"time":1,"host":"relay","event":"a","fields":{"host.name":"d2ViLTE="}}"#
        );
    }

    #[test]
    fn an_event_with_no_payload_is_skipped_and_counted() {
        let registry = Registry::new();
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![Event::empty(0, AttrMap::new())],
        };
        let mut out = MessageBuf::default();
        encoder(&registry).encode_objects(&batch, &mut out);
        assert!(out.is_empty());
        assert_eq!(
            counted(&registry, "logit.output.events.skipped", ("reason", "no_payload")),
            1.0
        );
    }

    /// Loss is `*.dropped` tagged `reason`, and nothing else is: the classification each variant
    /// carries, pinned so a new variant has to take a side.
    #[test]
    fn every_loss_is_counted_dropped_and_nothing_else_is() {
        for why in Undecoded::ALL {
            let (metric, reason) = why.counter();
            let loss = !matches!(why, Undecoded::NoMetric);
            assert_eq!(metric.ends_with(".dropped"), loss, "{why:?}: {metric}");
            assert!(!loss || metric.starts_with("logit.input."), "{why:?}: {metric}");
            assert!(!reason.contains("dropped") && !reason.contains("skipped"), "{why:?}");
        }
        for why in Unencoded::ALL {
            let (metric, (key, value)) = why.counter();
            let loss = matches!(
                why,
                Unencoded::BlankEvent | Unencoded::UnresolvedGaugeDelta | Unencoded::DuplicateName
            );
            assert_eq!(metric.ends_with(".dropped"), loss, "{why:?}: {metric}");
            assert!(!loss || key == "reason", "{why:?}: a loss is tagged reason, not {key}");
            assert!(!value.contains("dropped") && !value.contains("skipped"), "{why:?}");
        }
    }
}
