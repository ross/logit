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
//! records, as `statsd_out` does).
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

pub mod generated;
pub mod tags;
pub mod time;

pub mod series;
pub mod sketches;

pub mod events;
pub mod logs;
pub mod service_checks;

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
