//! Datadog's intake API and the Datadog Agent's own payloads: the codecs behind `datadog_in` and
//! `datadog_out` ([ADR `datadog-agent-and-intake-relay`](../../../../docs/adr/datadog-agent-and-intake-relay.md),
//! [`docs/plans/datadog-relay.md`](../../../../docs/plans/datadog-relay.md)). One submodule per
//! payload family, each documenting its own wire ↔ model mapping table in the format
//! [`crate::collectd`]'s module doc set; this doc holds what they share.
//!
//! Like [`crate::prometheus`], this family implements none of [`crate::Encoder`],
//! [`crate::FramedEncoder`], or [`crate::SignalEncoder`]: Datadog has several endpoints per
//! signal (series, sketches, and distribution points are all "metrics"), each with its own body
//! shape, so the decoders are methods on [`DatadogDecoder`] taking one already-decompressed request
//! body, and the encoders are methods on [`DatadogEncoder`] producing one body per route. HTTP
//! concerns (routing, `Content-Type`, `Content-Encoding`, zstd/gzip/deflate) belong to the
//! listener and sink in `logit-inputs`/`logit-outputs`, never here.
//!
//! # Shared vocabulary
//!
//! Raw Datadog fields the model has no typed home for live in event attributes under `datadog.*`
//! ([ADR `lossless-transit`](../../../../docs/adr/lossless-transit.md)'s carrier rule); the
//! constants below are the only spelling. `host.name` carries a series' `host` resource, a
//! sketch's `host`, or a log's `hostname` fallback. Events and service checks reuse
//! `crates/logit-inputs/src/statsd.rs`'s `statsd.event.*` / `statsd.service_check.*` names: they
//! are the same Datadog concepts DogStatsD carries. Tags fold into attributes by [`tags`]'s rule;
//! timestamps convert by [`time`]'s.
//!
//! # Metrics (`series`, `sketches`)
//!
//! Five routes, each a [`DatadogDecoder`] method taking one decompressed body and `received_at`
//! (`decode_series_v2_protobuf`, `decode_series_v2_json`, `decode_series_v1`,
//! `decode_distribution_points`, `decode_sketches`) and a matching `DatadogEncoder::encode_*`
//! returning `None` when nothing in the batch belongs on that route. The decoded batch scope is
//! `None`, and its `Resource` is empty except a sketch payload's `CommonMetadata`: every other
//! Datadog field is an event attribute.
//!
//! ## Decode: series and distribution points → events
//!
//! One `Event` per point, carrying one `MetricRecord` named after the series' `metric`. Every
//! point of a series shares its attributes.
//!
//! | Wire | Model | Counter / diag |
//! |---|---|---|
//! | `COUNT` / `1` / `"count"` | `MetricKind::counter(v)` (delta, monotonic) | -- |
//! | `GAUGE` / `3` / `"gauge"`; a v1 series with no `type` | `Gauge(v)` | -- |
//! | `RATE` / `2` / `"rate"` | `Gauge(v)` + [`ATTR_TYPE`] `rate` | -- |
//! | `UNSPECIFIED` / `0` / `""`; a v2 JSON series with no `type` | `Gauge(v)` + [`ATTR_TYPE`] `unspecified` | -- |
//! | an unknown type, no `metric`, no `points` array, or a field of the wrong JSON type | the series is skipped; the rest of the request decodes | `logit.input.metrics.skipped{reason="bad_series"}` + diag `bad_series` |
//! | a point that isn't `[ts, value]` / `{timestamp, value}` / `[ts, [values]]` | the point is skipped | `skipped{reason="bad_point"}` |
//! | v1 `[ts, null]` | the point is skipped | `skipped{reason="null_value"}` |
//! | a non-finite protobuf value | the point is skipped | `skipped{reason="non_finite_value"}` |
//! | point timestamp (s) | `Event::timestamp`; a fractional v1 timestamp truncates to its second | -- |
//! | timestamp absent, zero, or negative | `received_at`, truncated to its second | `logit.input.metrics.degraded{reason="no_timestamp"}` |
//! | `unit` | `MetricRecord::unit` when non-empty | -- |
//! | `tags` | attributes by [`tags`]'s rule | -- |
//! | the first named `host` resource; v1 and distribution `host` | [`ATTR_HOST_NAME`] | -- |
//! | the first named `device` resource; v1 `device` | [`ATTR_DEVICE`] (the Agent's v2 serializer sends a v1 `device` as that resource, UNVERIFIED) | -- |
//! | every other resource, including an empty-named or second `host` | [`ATTR_RESOURCES`]: `Array` of `Map{type, name}`, in wire order | -- |
//! | `source_type_name` | [`ATTR_SOURCE_TYPE_NAME`] when non-empty | -- |
//! | `interval` | [`ATTR_INTERVAL`] (`I64`) when nonzero | -- |
//! | protobuf `metadata.origin` | [`ATTR_ORIGIN_PRODUCT`] / [`ATTR_ORIGIN_CATEGORY`] / [`ATTR_ORIGIN_SERVICE`] (`U64`), each when nonzero | -- |
//! | JSON `metadata.origin` `product` / `service` / `metric_type` | [`ATTR_ORIGIN_PRODUCT`] / [`ATTR_ORIGIN_SERVICE`] / [`ATTR_ORIGIN_METRIC_TYPE`], each when nonzero | -- |
//! | distribution point `[ts, [v, ...]]` | `MetricKind::Samples(Samples::new(values))`, rate 1.0; the series' `type` is ignored | -- |
//! | a distribution point with no values | skipped | `skipped{reason="empty_distribution"}` |
//! | a body that isn't JSON (or protobuf), or has no top-level `series` array | `CodecError::Malformed` | -- |
//!
//! A tag spelled like a carrier (`host.name:x`) loses to the wire field it names. On a route with
//! a `type` (every series route, not distribution points), that includes `datadog.type`: a GAUGE
//! or COUNT series drops a `datadog.type:rate` tag rather than turning into a RATE on re-encode.
//!
//! ## Decode: sketches → events
//!
//! | Wire | Model | Counter / diag |
//! |---|---|---|
//! | one `Dogsketch` | one `Event`, `MetricKind::Distribution` under `Mapping::agent`; `timestamp` = `ts` s (absent: `received_at` on its second, counted as above) | -- |
//! | `k > 0` / `k < 0` / `k == 0` with count `n` | positive bin `key: k` / negative bin `key: -k` / the zero count; repeated keys sum | -- |
//! | `cnt`, `min`, `max`, `sum` | the sketch's exact summary (`SketchStats`); `avg` is ignored | -- |
//! | `metric`, `host`, `tags`, `metadata.origin` | as for a series | -- |
//! | no bins and `cnt == 0` | skipped | `skipped{reason="empty_sketch"}` |
//! | `k`/`n` lengths differ, a key outside ±32767, `cnt <= 0` with populated bins, a non-finite `min`/`max`/`sum`, or no `metric` | skipped | `skipped{reason="bad_sketch"}` + diag `bad_sketch` |
//! | legacy `distributions` entries | ignored | `skipped{reason="legacy_distribution"}`, one per entry |
//! | payload `metadata` (`CommonMetadata`) `agent_version`, `timezone`, `internal_ip`, `public_ip` | batch resource [`RESOURCE_ATTR_AGENT_VERSION`], [`RESOURCE_ATTR_AGENT_TIMEZONE`], [`RESOURCE_ATTR_AGENT_INTERNAL_IP`], [`RESOURCE_ATTR_AGENT_PUBLIC_IP`] (`Str`), each when non-empty | -- |
//! | payload `metadata.current_epoch` | batch resource [`RESOURCE_ATTR_AGENT_EPOCH`] (`F64`) when nonzero | -- |
//! | payload `metadata.api_key` | dropped; a credential, never stored | -- |
//!
//! ## Encode: events → series, distribution points, sketches
//!
//! One series (or sketch) per `(event, MetricRecord)`, carrying one point. Carriers are read from
//! the resource merged with the event's attributes (event wins); every other attribute renders as
//! a tag by [`tags`]'s rule. A `Map`/`Bytes`/`Null` tag value, or a carrier of the wrong `Value`
//! type, counts `logit.output.tags.dropped{reason="unrepresentable"}`.
//!
//! | Model | Wire | Counter |
//! |---|---|---|
//! | `Sum{Delta, monotonic}` | series `count`, `interval` from [`ATTR_INTERVAL`], else `0` | -- |
//! | `Gauge` | series `gauge`; `rate` or unspecified when [`ATTR_TYPE`] says so | -- |
//! | `Set` | series `gauge` of `estimate()`, the Agent's `s` | `logit.output.metrics.degraded{reason="set_estimate"}` |
//! | `Samples` | a distribution point only; with `sample_rate < 1`, each value repeated `weight()` times, because the route has no rate | `degraded{reason="sample_rate_expanded"}` |
//! | `Distribution` under `Mapping::agent` | a sketch only: `Dogsketch{ts, cnt, min, max, avg: sum/cnt, sum, k, n}`, `k` ascending (negative bins from the largest magnitude as `-key`, then `0`, then positive), a count above 65535 split into repeated entries | -- |
//! | `Distribution` under a logarithmic mapping | re-binned into `Mapping::agent` at each bin's representative value, summary kept | `degraded{reason="rebinned"}` |
//! | a sketch bin with a fractional count | rounded; a bin rounding to 0 is omitted | `degraded{reason="fractional_count"}`, once per sketch |
//! | a `Distribution` with no bins and zero count | skipped | `logit.output.metrics.skipped{reason="empty_sketch"}` |
//! | `Sum{Cumulative}` / `Sum{Delta, !monotonic}` | skipped by the series encoders | `skipped{metric_kind="cumulative_sum"\|"non_monotonic_delta_sum"}` |
//! | `GaugeDelta`, `SetMembers`, `Histogram`, `ExponentialHistogram`, `Summary` | skipped by the series encoders | `skipped{metric_kind="gauge_delta"\|"set_members"\|"histogram"\|"exponential_histogram"\|"summary"}` |
//! | a kind another metrics route carries | left for that route, uncounted: only the series encoders count skips, and only of kinds no route carries | -- |
//! | record 0 of a service check (`statsd.service_check.name` present and a `Gauge` first metric) | left for the service-checks route, uncounted; any later record encodes as usual, without the check's `statsd.service_check.*` carriers as tags | -- |
//! | every record of an APM stats event ([`stats::ATTR_STATS_NAME`] present; [`is_datadog_stats`]) | left for the stats routes, uncounted: its `Sum`s and summary `Distribution`s are a stats group, not series or sketches | -- |
//! | a record flagged `NO_RECORDED_VALUE` | skipped; Datadog has no no-value marker | `skipped{reason="no_recorded_value"}` |
//! | a non-finite value or sample | skipped (JSON has no `NaN`) | `skipped{reason="non_finite_value"}` |
//! | `Event::timestamp` | whole seconds, rounded toward negative infinity | -- |
//! | [`ATTR_HOST_NAME`] | v2: a `{type: "host"}` resource, first; v1 and distribution: `host`, always (`""` when absent); sketch: `host` | -- |
//! | [`ATTR_DEVICE`] | v1: `device`; v2: a `{type: "device"}` resource after the host | -- |
//! | [`ATTR_RESOURCES`] | v2 resources after host and device, in order | -- |
//! | the `datadog.origin.*` codes | protobuf and sketch `metadata.origin` (no `metric_type`); JSON v2 `metadata.origin` (no `category`) | -- |
//! | a carrier the route has no field for (v1: resources and origin; distribution points: all but the host; sketches: device, resources, source type, interval, type, `metric_type`; every route but sketches: the `datadog.agent.*` payload metadata) | dropped | `logit.output.tags.dropped{reason="no_wire_form"}`, one per carrier |
//! | batch resource `datadog.agent.version` / `.timezone` / `.epoch` / `.internal_ip` / `.public_ip` | sketches: payload `metadata` (`CommonMetadata`, `api_key` `""`); omitted when the resource carries none | -- |
//! | `MetricRecord::unit` | v1 and v2 `unit` when `Some` | -- |
//!
//! v1 JSON emits the Agent's field set: `metric`, `points`, `tags`, `host`, `type`, and `interval`
//! always; `device`, `source_type_name`, and `unit` when present. Names and tags go out as-is,
//! because Datadog sanitizes them itself.
//!
//! ## Permitted normalizations (metrics)
//!
//! `datadog_in -> datadog_out` on one metrics route is a fixed point modulo this list
//! (`tests/datadog_metrics_fixed_point.rs` pins it):
//!
//! 1. point timestamps are whole seconds: a fractional v1 timestamp truncates, and a missing one
//!    becomes `received_at`, truncated;
//! 2. a series with N points leaves as N one-point series;
//! 3. v2 resources reorder to host, device, then the rest; tags reorder by attribute order, and an
//!    exact duplicate tag is dropped;
//! 4. a zero `interval` or origin code, an empty `unit`/`source_type_name`/`device`, and a v1 `type`
//!    of `gauge` or none all encode to the same wire;
//! 5. a v1 point with a `null` value, a distribution point with no values, and an empty sketch are
//!    dropped and counted;
//! 6. a sketch's repeated `k` entries sum on decode and re-split at 65535 on encode, and its `avg`
//!    is recomputed as `sum / cnt`;
//! 7. across routes (v1 and v2), a carrier the target has no field for is dropped and counted
//!    `no_wire_form`, per the encode table.
//! 8. a JSON route (v1, v2 JSON, distribution points) carries a metric value exactly: this
//!    crate's `serde_json` dependency enables `float_roundtrip` (see `Cargo.toml` and
//!    `docs/known-gaps.md`'s Datadog entry). The protobuf routes (v2 protobuf, sketches) never go
//!    through that parser and carry values bit-exact either way.
//!
//! # Logs, events, service checks (`logs`, `events`, `service_checks`)
//!
//! All three are JSON. Events and service checks decode to exactly what `statsd_in`'s DogStatsD
//! `_e{...}`/`_sc|...` parsers build, under the same `statsd.event.*`/`statsd.service_check.*`
//! names ([`events::ATTR_EVENT_TITLE`] and its siblings, [`service_checks::ATTR_SERVICE_CHECK_NAME`]
//! and its siblings), so a DogStatsD event and an intake event are indistinguishable in the model
//! and either re-encodes on either protocol. Every decoded batch's `Resource` is empty except an
//! events envelope's.
//!
//! ## Decode: logs (`/api/v2/logs`, `/v1/input`) → events
//!
//! | Wire | Model | Counter / diag |
//! |---|---|---|
//! | body: a JSON array of objects, or one bare object | one [`logit_core::Event::log`] per object | anything else (or not JSON): `CodecError::Malformed` |
//! | an array element that isn't an object | skipped | `logit.input.logs.skipped{reason="not_an_object"}` + diag `malformed_log` |
//! | `message` string | `LogRecord::message` = `Str`, `body_format: Raw` | -- |
//! | `message` of another JSON type | `Str` of its JSON text | -- |
//! | `message` absent or `null` | item skipped | `logit.input.logs.skipped{reason="no_message"}` |
//! | `status` | attribute `status`, verbatim; `severity` by case-insensitive match: `emergency`/`emerg`/`alert`/`critical`/`crit`/`fatal` → `Fatal`, `error`/`err` → `Error`, `warning`/`warn` → `Warn`, `notice`/`info` → `Info`, `debug` → `Debug`, `trace` → `Trace`, else `None` | -- |
//! | `timestamp`: integer (or fractional) milliseconds, or an RFC 3339 string | `Event::timestamp` | -- |
//! | `timestamp` absent, `null`, or `0` | `received_at` | -- |
//! | `timestamp` another type, or an unparseable string | `received_at` | diag `bad_timestamp` |
//! | `hostname`, `service`, `ddsource`, `ddtags` | attributes of the same names, verbatim (`ddtags` stays one comma-joined string; expanding it is a transform's job) | -- |
//! | any other key | an attribute of the same name: object → `Map`, array → `Array`, integer → `I64` (`U64` above `i64::MAX`), other number → `F64`, string → `Str`, bool, `null` → `Null` | -- |
//!
//! ## Decode: events (`/api/v2/events` envelope, `/api/v1/events`) → events
//!
//! A top-level object whose `events` member is an object is the Agent's envelope
//! (`{"apiKey","events":{"<source>":[...]},"internalHostname"}`); any other object is one public
//! event. Both shapes accept both field spellings (`msg_title`/`title`, `msg_text`/`text`,
//! `timestamp`/`date_happened`). An empty optional string means the same as an absent one, because
//! the Agent omits empty fields.
//!
//! | Wire | Model | Counter / diag |
//! |---|---|---|
//! | body not a JSON object | -- | `CodecError::Malformed` |
//! | envelope `internalHostname`, non-empty | resource [`RESOURCE_ATTR_AGENT_HOSTNAME`] | -- |
//! | envelope `apiKey` | dropped; a credential, never stored | -- |
//! | envelope group that isn't an array, or an item that isn't an object | skipped | `logit.input.events.skipped{reason="malformed"}` + diag `malformed_event` |
//! | neither a title nor a text | item skipped | `logit.input.events.skipped{reason="no_title"}` |
//! | `msg_title`/`title` | `statsd.event.title` (`""` when only a text arrived) | -- |
//! | `msg_text`/`text` | `LogRecord::message` = `Str`, `body_format: Raw`, `event_name: None` | -- |
//! | `timestamp`/`date_happened` seconds | `Event::timestamp`; absent or `0` → `received_at` | -- |
//! | `priority`, `host`, `aggregation_key` | `statsd.event.priority`, `.host`, `.aggregation_key` | -- |
//! | `tags` | attributes, by [`tags`]'s rule | -- |
//! | `alert_type` | `statsd.event.alert_type`, verbatim; `severity`: `error` → `Error`, `warning` → `Warn`, `success`/`info` → `Info`, else `None` | -- |
//! | `source_type_name`, else the envelope group key unless it is `api` | `statsd.event.source_type` | -- |
//! | `event_type` (Agent) | [`ATTR_EVENT_TYPE`] | -- |
//! | `device_name`, `related_event_id` (public) | [`ATTR_EVENT_DEVICE_NAME`], [`ATTR_EVENT_RELATED_EVENT_ID`] (the id as `I64`) | -- |
//!
//! ## Decode: service checks (`/api/v1/check_run`) → events
//!
//! | Wire | Model | Counter / diag |
//! |---|---|---|
//! | body: a JSON array of objects (one bare object is accepted too) | one [`logit_core::Event::metric`] per check | anything else: `CodecError::Malformed` |
//! | an element that isn't an object | skipped | `logit.input.metrics.skipped{reason="malformed"}` + diag `malformed_service_check` |
//! | `check`, absent or empty | skipped | `logit.input.metrics.skipped{reason="no_name"}` |
//! | `check` | the record's name, and `statsd.service_check.name` | -- |
//! | `status` 0 to 3 | `MetricKind::Gauge(status)`, and `statsd.service_check.status` = `U64` | -- |
//! | `status` absent, negative, above 3, or not an integer | skipped | `logit.input.metrics.skipped{reason="invalid_status"}` |
//! | `message`, `host_name`, non-empty | `statsd.service_check.message`, `.host` | -- |
//! | `tags`, an array or `null` | attributes, by [`tags`]'s rule | -- |
//! | `timestamp` seconds | `Event::timestamp`; absent or `0` → `received_at` | -- |
//!
//! ## Encode: events → logs, events, service checks
//!
//! Each encoder reads the merged resource and event attributes (the event's value winning), and
//! skips an event that belongs to another route without counting it: that is ordinary fan-out.
//! The carriers decide which route owns an event, through one shared predicate per route, so the
//! routes never disagree: a `log` carrying `statsd.event.title` is a Datadog event, sent only by
//! `encode_events`; `statsd.service_check.name` with a `Gauge` first metric is a service check,
//! whose record 0 is sent only by `encode_service_checks` (the metrics routes send its later
//! records, as `statsd_out` does); `datadog.stats.name` marks APM stats, sent whole and only by
//! the stats routes (see "APM stats" below).
//!
//! | Model | Wire | Counter |
//! |---|---|---|
//! | **logs** ([`DatadogEncoder::encode_logs`]): an event with a `log` and no `statsd.event.title` | one object in a JSON array; `None` when no event qualifies | -- |
//! | `LogRecord::message` | `message`: a `Str` as-is, `Bytes` as lossy UTF-8, anything else as its JSON text | -- |
//! | attribute `status`, else `severity` | `status` (`severity.as_str()`), omitted when neither | -- |
//! | `Event::timestamp` | `timestamp`, integer milliseconds | -- |
//! | `hostname`, else `host.name` | `hostname` | -- |
//! | `service`, else `service.name` | `service` | -- |
//! | `ddsource`, else [`ATTR_SOURCE`], else [`DatadogEncoder::with_default_source`] | `ddsource`, omitted when none | -- |
//! | `ddtags` | `ddtags`, verbatim | -- |
//! | every other attribute | a top-level key: `Map`/`Array` nested, `Bytes` → base64, `Timestamp` → RFC 3339, non-finite `F64` → `null` | -- |
//! | an attribute named `message` or `timestamp` | dropped: it would collide with the wire's own | `logit.output.tags.dropped{reason="reserved_key"}` |
//! | **events** ([`DatadogEncoder::encode_events`]): an event with a `log` carrying `statsd.event.title` | [`events::EventFormat::AgentEnvelope`] (the default): one envelope for the batch, `apiKey` `""`, groups keyed by `statsd.event.source_type` (else `api`) in sorted order; [`events::EventFormat::PublicV1`]: one object per event | -- |
//! | resource [`RESOURCE_ATTR_AGENT_HOSTNAME`] | envelope `internalHostname`, `""` when absent | -- |
//! | the event fields | Agent item keys in the Agent's order: `msg_title`, `msg_text`, `timestamp` (seconds), `priority`, `host` (always, `""` when absent), `tags`, `alert_type`, `aggregation_key`, `source_type_name`, `event_type`, then `device_name`, `related_event_id`; the public form uses `title`, `text`, `date_happened` and omits an absent `host`. Empty optional fields are omitted | -- |
//! | no `statsd.event.alert_type` | `alert_type` from `severity`: `Error`/`Fatal` → `error`, `Warn` → `warning`, `Info` → `info`, else omitted | -- |
//! | **service checks** ([`DatadogEncoder::encode_service_checks`]): an event carrying `statsd.service_check.name` whose first metric is a `Gauge` | one array item with every key: `check`, `host_name` (`""`), `timestamp` (seconds), `status`, `message` (`""`), `tags` (`[]`) | -- |
//! | `statsd.service_check.status` `U64` 0 to 3, else the gauge when it is an integer 0 to 3 | `status` | otherwise skipped: `logit.output.metrics.skipped{reason="invalid_status"}` |
//! | every attribute not consumed above (events and checks) | `tags`, by [`tags::render_tags`]; `statsd.timestamp` (DogStatsD's timestamp carrier) is never rendered | `logit.output.tags.dropped{reason="unrepresentable"}` for a `Map`/`Bytes`/`Null` |
//!
//! ## Permitted normalizations (logs, events, service checks)
//!
//! `datadog_in -> datadog_out` on one of these routes is a fixed point modulo this list
//! (`tests/datadog_logs_fixed_point.rs` pins it):
//!
//! 1. object keys reorder: reserved fields first, in the orders above, then attributes by symbol;
//!    envelope groups sort by source, so the first hop may reorder events across groups;
//! 2. a log timestamp is whole milliseconds and an event or check timestamp whole seconds; an RFC
//!    3339 log timestamp leaves as integer milliseconds, and a missing one as `received_at`;
//! 3. a non-string log `message` leaves as its JSON text;
//! 4. an envelope item's `source_type_name` is restated from its group key; the Agent's
//!    `tags: null` on a check leaves as `[]`; an empty optional event field is omitted;
//! 5. a log with `host.name`, `service.name`, or `datadog.source` but no `hostname`, `service`, or
//!    `ddsource` leaves under the Datadog name;
//! 6. tags reorder by attribute order, and an exact duplicate tag is dropped.
//!
//! # Traces (`traces`, `traces_msgpack`, `traces_proto`)
//!
//! Four forms, each a [`DatadogDecoder`] method taking one decompressed body and a matching
//! `DatadogEncoder::encode_*` returning `None` when the batch has no span: the tracer API's
//! `/v0.4/traces` msgpack (an array of traces, each an array of span maps;
//! [`DatadogDecoder::decode_traces_v04`]), `/v0.5/traces` (`[dictionary, traces]`, 12-element
//! span arrays of dictionary indices; [`DatadogDecoder::decode_traces_v05`]), `/v0.7/traces` (one
//! msgpack `TracerPayload`; [`DatadogDecoder::decode_tracer_payload_v07`]), and the intake's
//! `/api/v0.2/traces` protobuf `AgentPayload` ([`DatadogDecoder::decode_agent_payload`], one batch
//! per `TracerPayload`). The v0.3/v0.4 JSON form and the v1.0 string-table form (`idx`) are not
//! implemented. `received_at` is unused: every span has its own `start`. One [`traces`] mapping
//! serves all four, so a span means the same thing on every route.
//!
//! msgpack decoding is the Agent's own (`span_gen.go`, `decoder_v05.go`): unknown keys are
//! skipped, an absent or `nil` field is its zero value, any int format satisfies an integer field,
//! and a string field also takes `bin`.
//!
//! ## Decode: spans → events
//!
//! | Wire | Model | Counter / diag |
//! |---|---|---|
//! | one span | one [`logit_core::Event::span`], in wire order | -- |
//! | `trace_id` + `meta["_dd.p.tid"]` (1 to 16 hex digits, Go's `ParseUint(v, 16, 64)`) | `SpanRecord::trace_id`: high 8 bytes from `_dd.p.tid`, low 8 from `trace_id`, big-endian. Every span of the chunk (v0.4/v0.5: of the trace array) with the same `trace_id` gets the high half of the first span carrying a parseable one; `_dd.p.tid` stays a `Str` attribute on its own span only | -- |
//! | an unparseable `_dd.p.tid` | kept as an attribute; the span takes the chunk's high half, else zero | `logit.input.spans.degraded{reason="bad_tid"}` |
//! | `span_id`; `parent_id` (0 = none) | `span_id`; `parent_span_id` (big-endian) | -- |
//! | `start`, `duration` (ns) | `Event::timestamp` = `start`; `end_timestamp` = `start + duration`, saturating | -- |
//! | a negative `duration` | clamped to 0 | `degraded{reason="negative_duration"}` |
//! | `name` | `SpanRecord::name` (`Str`) | -- |
//! | `service`, `resource`, `type` | [`ATTR_SERVICE_NAME`], [`ATTR_RESOURCE_NAME`], [`ATTR_SPAN_TYPE`], each when non-empty | -- |
//! | `error` | `status: Error` when nonzero, else `Unset`; a value other than 0 or 1 also as [`traces::ATTR_SPAN_ERROR`] (`I64`) | -- |
//! | `meta` | attributes verbatim (`Str`), `span.kind`, `_dd.*`, `env`, `version` included | -- |
//! | `metrics` | attributes verbatim (`F64`): `_sampling_priority_v1`, `_top_level`, `_dd.measured`, `_sample_rate`, ... | -- |
//! | `meta_struct` | attributes verbatim (`Bytes`) | -- |
//! | the final `span.kind` attribute | `kind` when `server`/`client`/`producer`/`consumer`/`internal`, else `Internal` | -- |
//! | one key in two of `meta`/`metrics`/`meta_struct`, or a field spelled like a carrier | the later write wins, in that order, then `service`/`resource`/`type`/`error`, then the chunk's | `degraded{reason="key_collision"}` |
//! | `span_links[]` | `SpanLink{trace_id: trace_id_high ‖ trace_id, span_id, attributes (Str), trace_state (when non-empty), flags}` | -- |
//! | `span_events[]` | `SpanEvent{timestamp: time_unix_nano, name, attributes}`; `AttributeAnyValue` → `Str`/`Bool`/`I64`/`F64`/`Array` of those | -- |
//! | a `time_unix_nano` above `i64::MAX` | `i64::MAX` | `degraded{reason="timestamp_range"}` |
//! | an `AttributeAnyValue` of unknown `type` (or an array element of type `ARRAY_VALUE`) | the attribute is dropped | `degraded{reason="bad_attribute_type"}` |
//! | a string that isn't UTF-8 | lossy UTF-8 | `degraded{reason="invalid_utf8"}` |
//! | v0.7/`AgentPayload` chunk `priority` | [`traces::ATTR_CHUNK_PRIORITY`] (`I64`) on every span of the chunk, omitted when `-128` (`PriorityNone`); a v0.7 chunk with no `priority` key is 0, as in Go | -- |
//! | chunk `origin`, `dropped_trace`, `tags` | [`traces::ATTR_CHUNK_ORIGIN`] (non-empty), [`traces::ATTR_CHUNK_DROPPED_TRACE`] (`true`), [`traces::ATTR_CHUNK_TAGS`] (`Map` of `Str`, non-empty), on every span | -- |
//! | `TracerPayload` fields | batch resource `datadog.tracer.container_id`, `.language_name`, `.language_version`, `.tracer_version`, `.runtime_id`, `.env`, `.hostname`, `.app_version` (`Str`), `.tags` (`Map`), each when non-empty; `.container_debug` (`Map` of its non-zero fields) whenever present | -- |
//! | tracer API request headers (read by `datadog_trace_in`, not by these decoders): `Datadog-Meta-Lang`, `-Lang-Version`, `-Tracer-Version`, `Datadog-Container-ID` | batch resource `datadog.tracer.language_name`, `.language_version`, `.tracer_version`, `.container_id` (`Str`), each when non-empty; on v0.7 only where the `TracerPayload` left the field empty | -- |
//! | `Datadog-Meta-Lang-Interpreter`, `-Lang-Interpreter-Vendor`, `Datadog-Entity-ID` | [`RESOURCE_ATTR_TRACER_LANGUAGE_INTERPRETER`], [`RESOURCE_ATTR_TRACER_LANGUAGE_INTERPRETER_VENDOR`], [`RESOURCE_ATTR_TRACER_ENTITY_ID`] (`Str`), each when non-empty | -- |
//! | `Datadog-Client-Computed-Top-Level` (any non-empty value); `Datadog-Client-Computed-Stats` (any non-empty value but a Go `false` spelling) | [`RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_TOP_LEVEL`], [`RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_STATS`] = `Bool(true)`; absent otherwise | -- |
//! | `Datadog-Client-Dropped-P0-Traces`, `-Spans` | [`RESOURCE_ATTR_TRACER_DROPPED_P0_TRACES`], [`RESOURCE_ATTR_TRACER_DROPPED_P0_SPANS`] (`U64`); a value that isn't an unsigned integer is left out | diag `bad_header` |
//! | `AgentPayload` fields | every batch's resource: [`RESOURCE_ATTR_AGENT_HOSTNAME`], `datadog.agent.env`, [`RESOURCE_ATTR_AGENT_VERSION`] (`Str`), `.target_tps`, `.error_tps` (`F64`, nonzero), `.rare_sampler_enabled` (`true`), `.tags` (`Map`) | -- |
//! | `AgentPayload.idxTracerPayloads` (v1.0) | skipped | `logit.input.spans.skipped{reason="idx_payload"}`, one per payload |
//! | a span (v0.5: wrong arity, a dictionary index out of range), trace array, or chunk that doesn't parse | dropped; the rest decodes | `skipped{reason="malformed"}` + diag `malformed_span` |
//! | a body whose structure doesn't parse (not the top-level shape, truncated, bad protobuf) | -- | `CodecError::Malformed` |
//!
//! ## Encode: events → spans
//!
//! Every event with a `span`, grouped into one chunk per 128-bit trace id in first-appearance
//! order, spans in batch order within it. Attributes are the resource merged with the event's
//! (event wins); the carriers below are consumed, only when of the type decode gives them (a
//! non-empty `Str` for the string ones): one of another type goes out by the typing rules like
//! any attribute. Every map is sorted by key.
//!
//! | Model | Wire | Counter |
//! |---|---|---|
//! | `trace_id` | `trace_id` = low 8 bytes; `meta["_dd.p.tid"]` from the attribute where present, else synthesized as 16 hex digits on the chunk's first span when the high half is nonzero and no span carries one | -- |
//! | `end_timestamp - Event::timestamp` | `duration`; negative → 0 | `logit.output.spans.degraded{reason="negative_duration"}` |
//! | `service.name`, `resource.name`, `span.type` | `service`, `resource`, `type` (`""` when absent) | -- |
//! | [`traces::ATTR_SPAN_ERROR`], else `status` | `error`: the attribute, else 1 for `Error`, 0 otherwise | -- |
//! | `kind` | `meta["span.kind"]`, only when no `span.kind` attribute exists and `kind` isn't `Internal` | -- |
//! | `Str` | `meta` | -- |
//! | `F64`, `I64`, `U64` | `metrics` (`f64`) | `degraded{reason="int_as_f64"}` when an integer isn't exact |
//! | `Bool` | `meta` `"true"`/`"false"` | -- |
//! | `Bytes` | `meta_struct` | -- |
//! | `Timestamp` | `meta` as RFC 3339 | -- |
//! | `Array`, `Map`, `Null` | `meta` as JSON text | `degraded{reason="json_text"}` |
//! | links; events (a scalar or an array of scalars natively, anything else as a string) | `span_links`, `span_events` | `int_as_f64` for a `U64` event value above `i64::MAX`; `json_text`; `timestamp_range` for a negative event time |
//! | `datadog.chunk.*` of a chunk's first span | v0.7/`AgentPayload` chunk `priority` (`-128` when absent), `origin`, `dropped_trace`, `tags` | -- |
//! | batch resource `datadog.tracer.*` / `datadog.agent.*` | v0.7: one `TracerPayload`; `AgentPayload`: one payload around one `TracerPayload` | -- |
//! | a carrier the form has no field for: `datadog.chunk.*` in v0.4/v0.5 (per span), `datadog.tracer.*` in v0.4/v0.5 and `datadog.agent.*` below `AgentPayload` (per batch), `meta_struct`/links/events in v0.5 | dropped | `degraded{reason="no_wire_form"}`, one per item |
//! | `status: Ok`, span `flags`, `SpanExt` status message / `trace_state` / dropped counts, a link's or event's dropped-attribute count | dropped: Datadog has no field | `degraded{reason="no_wire_form"}`, one per field |
//!
//! The encoders write the Agent's key sets and orders (`EncodeMsg`), omitting what its
//! `omitempty` tags omit; an `AttributeAnyValue` carries `type` and its one value field. The
//! protobuf encoder is hand-written rather than prost's, because prost's `HashMap` maps encode in
//! a random order.
//!
//! ## Permitted normalizations (traces)
//!
//! `datadog_trace_in -> datadog_trace_out` on one form, and `datadog_in -> datadog_out` on
//! `AgentPayload`, is a fixed point modulo this list (`tests/datadog_traces_fixed_point.rs` pins
//! it):
//!
//! 1. batching: an `AgentPayload` decodes to one batch per `TracerPayload` and encodes one
//!    `TracerPayload` per batch;
//! 2. chunk regrouping: spans regroup into one chunk (trace array) per 128-bit trace id, in
//!    first-appearance order; a chunk's fields come from its first span;
//! 3. every map is sorted by key; a key in two of `meta`/`metrics`/`meta_struct` keeps one value;
//! 4. v0.5's dictionary is rebuilt in first-use order, `""` at index 0;
//! 5. a negative `duration` leaves as 0; a zero-valued or absent field leaves as the Agent writes
//!    it (`priority` `-128` in no chunk, `nil` as the zero value);
//! 6. a 128-bit trace id with no `_dd.p.tid` gains one on its chunk's first span;
//! 7. across forms, a field the target has no home for is dropped and counted `no_wire_form`.
//!
//! # APM stats (`stats`)
//!
//! Two routes, both msgpack under the Agent's Go field names (`ClientStatsPayload` and friends
//! have no `msg` tags, so the keys are `Hostname`, `Stats`, `HTTPStatusCode`, ...; the one
//! exception is `srv_src`): a tracer's `/v0.6/stats` body, one `ClientStatsPayload`
//! ([`DatadogDecoder::decode_client_stats_v06`], [`DatadogEncoder::encode_client_stats_v06`]),
//! and the intake's `/api/v0.2/stats` body, one `StatsPayload` wrapping several
//! ([`DatadogDecoder::decode_stats_payload`], one batch per `ClientStatsPayload`;
//! [`DatadogEncoder::encode_stats_payload`]). `OkSummary`/`ErrorSummary` are `sketches-go`
//! DDSketch protobufs, which carry their own mapping, so a decoded summary keeps it
//! ([`logit_core::Mapping::logarithmic`]) and relays bin-for-bin. The decoded batch scope is
//! `None`. The attribute and resource names are [`stats`]'s constants.
//!
//! ## Decode: stats → events
//!
//! | Wire | Model | Counter / diag |
//! |---|---|---|
//! | a body that isn't msgpack, a truncated value, a top level that isn't a map, or (v0.6) a `ClientStatsPayload` field of the wrong type | -- | `CodecError::Malformed` |
//! | an intake `Stats` element that isn't a well-formed `ClientStatsPayload` | dropped; the other payloads decode | `logit.input.stats.skipped{reason="malformed_payload"}` + diag `malformed_stats` |
//! | a bucket, or a group, with a field of the wrong type (or that isn't a map) | dropped; the rest decodes | `skipped{reason="malformed_bucket"\|"malformed_group"}` + diag `malformed_stats` |
//! | an unknown key; a `nil` value | skipped; the field's zero value (msgp's own rules) | -- |
//! | one `ClientGroupedStats` of a bucket | one `Event::metric`; `timestamp` = the bucket's `Start` (ns) | a `Start` above `i64::MAX`: clamped, `logit.input.stats.degraded{reason="bucket_start_overflow"}` |
//! | a bucket with no groups | nothing | `skipped{reason="empty_bucket"}` |
//! | `Hits`, `Errors`, `TopLevelHits` | `datadog.stats.hits`, `.errors`, `.top_level_hits`: delta monotonic `Sum` of the `uint64` as `f64`, always present | above 2^53 and not exactly representable: `degraded{reason="inexact_count"}` |
//! | `Duration` | `datadog.stats.duration`: the same, unit `ns` | the same |
//! | `OkSummary`, `ErrorSummary` | `datadog.stats.ok_summary`, `.error_summary`: `Distribution` under `Mapping::logarithmic(gamma, indexOffset, 2048)`, sparse `binCounts` and `contiguousBinCounts` both read (a key in both sums), `zeroCount`; the summary (`count`/`min`/`max`/`sum`) derived from the bins; absent when the wire bytes are empty | -- |
//! | a summary whose `interpolation` isn't `NONE` | that record dropped; its keys aren't the logarithmic mapping's | `skipped{reason="interpolation"}` |
//! | a summary that isn't a DDSketch, has no mapping, or has a `gamma`/`indexOffset` no mapping can use | that record dropped | `skipped{reason="bad_sketch"}` + diag `bad_stats_sketch` |
//! | `Service`, `Resource`, `Type`, `SpanKind` | `service.name`, `resource.name`, `span.type`, `span.kind` (ADR decision 8), when non-empty | -- |
//! | `Name` | [`stats::ATTR_STATS_NAME`], always, even empty: it is what marks the event as APM stats | -- |
//! | `DBType`, `GRPCStatusCode`, `HTTPMethod`, `HTTPEndpoint`, `srv_src` | `datadog.stats.db_type`, `.grpc_status_code`, `.http_method`, `.http_endpoint`, `.service_source`, when non-empty | -- |
//! | `HTTPStatusCode` | `datadog.stats.http_status_code` (`U64`), when nonzero | -- |
//! | `Synthetics` | `datadog.stats.synthetics` = `Bool(true)`, when true | -- |
//! | `IsTraceRoot` `1` / `2` / `0` | `datadog.stats.is_trace_root` `"true"` / `"false"` / absent | any other value: absent, `degraded{reason="unknown_trilean"}` |
//! | `PeerTags`, `AdditionalMetricTags`, `SpanDerivedPrimaryTags` | `datadog.stats.peer_tags`, `.additional_metric_tags`, `.span_derived_primary_tags`: `Array` of `Str`, when non-empty | -- |
//! | bucket `Duration`, `AgentTimeShift` | each of its groups' `datadog.stats.bucket.duration` (`U64`, always) and `.bucket.agent_time_shift` (`I64`, when nonzero) | -- |
//! | `ClientStatsPayload` `Hostname`, `Env`, `Version`, `Lang`, `TracerVersion`, `RuntimeID`, `ContainerID` | batch resource `datadog.tracer.hostname`, `.env`, `.app_version`, `.language_name`, `.tracer_version`, `.runtime_id`, `.container_id`, when non-empty | -- |
//! | `AgentAggregation`, `Service`, `GitCommitSha`, `ImageTag`, `ProcessTags` | resource `datadog.stats.agent_aggregation`, `.service`, `.git_commit_sha`, `.image_tag`, `.process_tags`, when non-empty | -- |
//! | `Sequence`, `ProcessTagsHash`; `Tags` | resource `datadog.stats.sequence`, `.process_tags_hash` (`U64`), when nonzero; `datadog.stats.tags` (`Array` of `Str`), when non-empty | -- |
//! | `StatsPayload` `AgentHostname`, `AgentEnv`, `AgentVersion` | every batch's resource [`RESOURCE_ATTR_AGENT_HOSTNAME`], `datadog.agent.env`, [`RESOURCE_ATTR_AGENT_VERSION`], when non-empty | -- |
//! | `ClientComputed`, `SplitPayload` | every batch's resource `datadog.stats.client_computed`, `.split_payload` = `Bool(true)`, when true | -- |
//!
//! ## Encode: events → stats
//!
//! The stats routes send every event [`is_datadog_stats`] accepts and nothing else, uncounted,
//! as ordinary fan-out; every other route skips such an event whole. Buckets are rebuilt by
//! grouping on `(Event::timestamp, datadog.stats.bucket.duration,
//! datadog.stats.bucket.agent_time_shift)`, in first-seen order; every map writes every key, in
//! the Go struct's field order, as msgp's `EncodeMsg` does. `encode_stats_payload` wraps the
//! batch's one `ClientStatsPayload`.
//!
//! | Model | Wire | Counter |
//! |---|---|---|
//! | a group attribute (the event's, else the resource's) | its field above; `""`, `0`, `false`, or `[]` when absent | a value of the wrong type: `logit.output.tags.dropped{reason="unrepresentable"}` |
//! | any other event attribute; a resource attribute that is none of the fields above (`datadog.agent.*` and the two envelope flags count on the v0.6 route, which has no envelope) | dropped | `tags.dropped{reason="no_wire_form"}`, one per attribute (a resource's once per batch) |
//! | a delta `Sum` hits/errors/top-level-hits/duration | `uint64`, rounded to the nearest integer | a fraction: `logit.output.stats.degraded{reason="fractional_count"}`; negative or non-finite: `0`, `degraded{reason="bad_count"}`; above 2^64: `u64::MAX`, `degraded{reason="count_overflow"}` |
//! | an ok/error summary `Distribution` under a logarithmic mapping | a DDSketch protobuf: its `gamma` and `indexOffset`, `interpolation: NONE`, both stores as sparse `binCounts` in ascending key order, `zeroCount`. Hand-encoded, since prost's `HashMap` would order the bins at random | a bin limit other than 2048 (a receiver collapses at 2048): `degraded{reason="bin_limit"}`; a summary tracked from observations (`stats_exact`), which the protobuf has no field for: `degraded{reason="exact_summary"}` |
//! | the same under `Mapping::agent` | `gamma = 1.015625`, `indexOffset = bias + 0.5`, keys unchanged: Datadog's own conversion, reading the Agent's round-half-to-even key as the logarithmic floor. Only a value on an exact tie keys differently, and the exact summary is lost | `degraded{reason="agent_mapping"}` |
//! | a record of another name, or a known name of another kind (a cumulative `Sum`, say) | skipped | `logit.output.stats.skipped{reason="unrecognized_record"}` |
//! | a negative `Event::timestamp` | bucket `Start` `0` | `degraded{reason="negative_timestamp"}` |
//!
//! ## Permitted normalizations (APM stats)
//!
//! `datadog_in -> datadog_out` on one stats route is a fixed point modulo this list
//! (`tests/datadog_stats_fixed_point.rs` pins it):
//!
//! 1. every map key is written, in Go field order, zero values included: a tracer's omitted key or
//!    `nil` leaves as the explicit zero value, and an unknown key is dropped;
//! 2. a DDSketch's contiguous bins leave as sparse ones (a key in both summed), and a store over
//!    2048 bins collapses from the lowest key on decode;
//! 3. a bucket with no groups is dropped, and buckets sharing a start, duration, and time shift
//!    merge, their groups in first-seen order;
//! 4. an intake `StatsPayload` leaves as one `StatsPayload` per `ClientStatsPayload` (batching),
//!    its envelope restated on each;
//! 5. a count above 2^53 that `f64` can't hold exactly is rounded, and counted.

pub mod generated;
pub mod tags;
pub mod time;

pub mod series;
pub mod sketches;

pub mod events;
pub mod logs;
pub mod service_checks;

pub mod traces;
pub mod traces_msgpack;
pub mod traces_proto;

pub mod stats;

use logit_core::{Diagnostics, Event, MetricKind, Resource, Telemetry, Value};

/// `host.name`: the Datadog host of a series/sketch/log, as an event attribute.
pub const ATTR_HOST_NAME: &str = "host.name";
/// `datadog.type`: `rate` or `unspecified`; absent for `count` and `gauge`, whose model kinds say
/// so themselves.
pub const ATTR_TYPE: &str = "datadog.type";
/// `datadog.interval`: a count's or rate's interval in seconds (I64), present only when nonzero.
pub const ATTR_INTERVAL: &str = "datadog.interval";
/// `datadog.source_type_name`: the check or integration that produced a series.
pub const ATTR_SOURCE_TYPE_NAME: &str = "datadog.source_type_name";
/// `datadog.device`: the v1 series `device` field.
pub const ATTR_DEVICE: &str = "datadog.device";
/// `datadog.resources`: every series resource whose `type` isn't `host`, as an `Array` of
/// `Map{type, name}`.
pub const ATTR_RESOURCES: &str = "datadog.resources";
/// `datadog.origin.product` / `.category` / `.service`: the protobuf `Origin` codes (U64).
pub const ATTR_ORIGIN_PRODUCT: &str = "datadog.origin.product";
pub const ATTR_ORIGIN_CATEGORY: &str = "datadog.origin.category";
pub const ATTR_ORIGIN_SERVICE: &str = "datadog.origin.service";
/// `datadog.origin.metric_type`: the JSON v2 API's extra origin code, which the protobuf lacks.
pub const ATTR_ORIGIN_METRIC_TYPE: &str = "datadog.origin.metric_type";
/// `datadog.event_type`: the Agent-only event field.
pub const ATTR_EVENT_TYPE: &str = "datadog.event_type";
/// `datadog.event.device_name` / `.related_event_id`: the public events API's extra fields.
pub const ATTR_EVENT_DEVICE_NAME: &str = "datadog.event.device_name";
pub const ATTR_EVENT_RELATED_EVENT_ID: &str = "datadog.event.related_event_id";
/// `datadog.source`: a log's `ddsource` when it arrives from elsewhere than a Datadog log payload
/// (an upstream `set`); a decoded Datadog log keeps `ddsource` verbatim instead.
pub const ATTR_SOURCE: &str = "datadog.source";
/// `datadog.agent.hostname`: an events envelope's `internalHostname`, as a resource attribute.
pub const RESOURCE_ATTR_AGENT_HOSTNAME: &str = "datadog.agent.hostname";
/// `datadog.agent.version` / `.timezone` / `.epoch` / `.internal_ip` / `.public_ip`: a sketch
/// payload's `CommonMetadata`, as resource attributes (`epoch` an `F64`, the rest `Str`).
pub const RESOURCE_ATTR_AGENT_VERSION: &str = "datadog.agent.version";
pub const RESOURCE_ATTR_AGENT_TIMEZONE: &str = "datadog.agent.timezone";
pub const RESOURCE_ATTR_AGENT_EPOCH: &str = "datadog.agent.epoch";
pub const RESOURCE_ATTR_AGENT_INTERNAL_IP: &str = "datadog.agent.internal_ip";
pub const RESOURCE_ATTR_AGENT_PUBLIC_IP: &str = "datadog.agent.public_ip";
/// `datadog.agent.env`: an `AgentPayload`'s or a stats `StatsPayload` envelope's `env`, as a
/// resource attribute. Shared by [`traces`] and [`stats`], so it lives here rather than in either.
pub const RESOURCE_ATTR_AGENT_ENV: &str = "datadog.agent.env";
/// `datadog.tracer.*`: a `TracerPayload`'s or a stats `ClientStatsPayload`'s fields, as batch
/// resource attributes. Shared by [`traces`] and [`stats`], so they live here rather than in
/// either; each module also has its own carriers with no counterpart in the other.
pub const RESOURCE_ATTR_TRACER_CONTAINER_ID: &str = "datadog.tracer.container_id";
pub const RESOURCE_ATTR_TRACER_LANGUAGE_NAME: &str = "datadog.tracer.language_name";
pub const RESOURCE_ATTR_TRACER_VERSION: &str = "datadog.tracer.tracer_version";
pub const RESOURCE_ATTR_TRACER_RUNTIME_ID: &str = "datadog.tracer.runtime_id";
pub const RESOURCE_ATTR_TRACER_ENV: &str = "datadog.tracer.env";
pub const RESOURCE_ATTR_TRACER_HOSTNAME: &str = "datadog.tracer.hostname";
pub const RESOURCE_ATTR_TRACER_APP_VERSION: &str = "datadog.tracer.app_version";
/// `datadog.tracer.language_interpreter` / `.language_interpreter_vendor` / `.entity_id`: a
/// tracer's `Datadog-Meta-Lang-Interpreter`, `Datadog-Meta-Lang-Interpreter-Vendor`, and
/// `Datadog-Entity-ID` request headers (`Str`), which no payload carries.
pub const RESOURCE_ATTR_TRACER_LANGUAGE_INTERPRETER: &str = "datadog.tracer.language_interpreter";
pub const RESOURCE_ATTR_TRACER_LANGUAGE_INTERPRETER_VENDOR: &str =
    "datadog.tracer.language_interpreter_vendor";
pub const RESOURCE_ATTR_TRACER_ENTITY_ID: &str = "datadog.tracer.entity_id";
/// `datadog.tracer.client_computed_top_level` / `.client_computed_stats`: a tracer's
/// `Datadog-Client-Computed-Top-Level` and `Datadog-Client-Computed-Stats` request headers, as
/// `Bool(true)` when set and absent otherwise.
pub const RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_TOP_LEVEL: &str =
    "datadog.tracer.client_computed_top_level";
pub const RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_STATS: &str = "datadog.tracer.client_computed_stats";
/// `datadog.tracer.dropped_p0_traces` / `.dropped_p0_spans`: a tracer's
/// `Datadog-Client-Dropped-P0-Traces` and `-Spans` request headers (`U64`), the priority-0
/// traces and spans it dropped before sending.
pub const RESOURCE_ATTR_TRACER_DROPPED_P0_TRACES: &str = "datadog.tracer.dropped_p0_traces";
pub const RESOURCE_ATTR_TRACER_DROPPED_P0_SPANS: &str = "datadog.tracer.dropped_p0_spans";

/// The tracer API's request headers that carry `datadog.tracer.*` resource attributes, spelled
/// in lowercase as `http::HeaderName` stores them. `datadog_trace_in` reads them and
/// `datadog_trace_out` writes them back; the mapping is in this module's doc, under "Traces".
pub const HEADER_META_LANG: &str = "datadog-meta-lang";
pub const HEADER_META_LANG_VERSION: &str = "datadog-meta-lang-version";
pub const HEADER_META_LANG_INTERPRETER: &str = "datadog-meta-lang-interpreter";
pub const HEADER_META_LANG_INTERPRETER_VENDOR: &str = "datadog-meta-lang-interpreter-vendor";
pub const HEADER_META_TRACER_VERSION: &str = "datadog-meta-tracer-version";
pub const HEADER_CONTAINER_ID: &str = "datadog-container-id";
pub const HEADER_ENTITY_ID: &str = "datadog-entity-id";
pub const HEADER_CLIENT_COMPUTED_TOP_LEVEL: &str = "datadog-client-computed-top-level";
pub const HEADER_CLIENT_COMPUTED_STATS: &str = "datadog-client-computed-stats";
pub const HEADER_CLIENT_DROPPED_P0_TRACES: &str = "datadog-client-dropped-p0-traces";
pub const HEADER_CLIENT_DROPPED_P0_SPANS: &str = "datadog-client-dropped-p0-spans";
/// `X-Datadog-Trace-Count`: how many traces the tracer says the body holds.
pub const HEADER_TRACE_COUNT: &str = "x-datadog-trace-count";
/// `service.name` / `resource.name` / `span.type` / `span.kind`: a span's `service`, `resource`,
/// `type`, and (a `meta` key, kept verbatim, that also sets `SpanRecord::kind`) `span.kind` — the
/// names Datadog's own OTLP receiver honors (ADR `datadog-agent-and-intake-relay` decision 8). An
/// APM stats group's `Service`/`Resource`/`Type`/`SpanKind` carry the same names, so these live
/// here rather than in [`traces`] or [`stats`] alone.
pub const ATTR_SERVICE_NAME: &str = "service.name";
pub const ATTR_RESOURCE_NAME: &str = "resource.name";
pub const ATTR_SPAN_TYPE: &str = "span.type";
pub const ATTR_SPAN_KIND: &str = "span.kind";

/// The attribute `key` with the event's value winning over the resource's, as
/// [`logit_core::attrs::merged`] resolves it, without the full merged walk.
fn merged_get<'a>(resource: &'a Resource, event: &'a Event, key: &str) -> Option<&'a Value> {
    event.attributes.get(key).or_else(|| resource.attributes.get(key))
}

/// A service check: `statsd.service_check.name` present and a `Gauge` first metric, exactly what
/// [`DatadogEncoder::encode_service_checks`] sends. Its record 0 belongs to that route alone; the
/// metrics routes skip it and send any later records as ordinary metrics, as `statsd_out` does.
pub(super) fn is_service_check(resource: &Resource, event: &Event) -> bool {
    matches!(event.metrics.first().map(|m| &m.kind), Some(MetricKind::Gauge(_)))
        && merged_get(resource, event, service_checks::ATTR_SERVICE_CHECK_NAME).is_some()
}

/// A Datadog (or DogStatsD) event: a `log` carrying `statsd.event.title`, exactly what
/// [`DatadogEncoder::encode_events`] sends. The logs route skips it.
pub(super) fn is_datadog_event(resource: &Resource, event: &Event) -> bool {
    event.log.is_some() && merged_get(resource, event, events::ATTR_EVENT_TITLE).is_some()
}

/// APM stats: `datadog.stats.name` present (the event's, else the resource's), exactly what
/// [`DatadogEncoder::encode_client_stats_v06`] and [`DatadogEncoder::encode_stats_payload`] send.
/// Every metrics route skips such an event whole: its records are a stats group's, not series.
pub(super) fn is_datadog_stats(resource: &Resource, event: &Event) -> bool {
    merged_get(resource, event, stats::ATTR_STATS_NAME).is_some()
}

/// Decodes Datadog intake bodies, one per route. Carries its [`Diagnostics`] and [`Telemetry`]
/// so a malformed item can be dropped and counted while the rest of a request decodes;
/// default-constructed handles make those no-ops for a codec used standalone.
#[derive(Default)]
pub struct DatadogDecoder {
    telemetry: Telemetry,
    diagnostics: Diagnostics,
}

impl DatadogDecoder {
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
}

/// Encodes an `EventBatch` into Datadog intake bodies, one per route. Every skip and degrade is
/// counted through [`Telemetry`]; a disabled handle costs nothing.
#[derive(Default)]
pub struct DatadogEncoder {
    telemetry: Telemetry,
    diagnostics: Diagnostics,
    /// `ddsource` for a log that carries neither `ddsource` nor `datadog.source`; `None` omits it.
    default_source: Option<String>,
}

impl DatadogEncoder {
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

    pub fn with_default_source(mut self, source: impl Into<String>) -> Self {
        self.default_source = Some(source.into());
        self
    }
}
