//! Datadog's intake API and the Datadog Agent's own payloads: the codecs behind `datadog_in` and
//! `datadog_out` ([ADR `datadog-agent-and-intake-relay`](../../../../docs/adr/datadog-agent-and-intake-relay.md),
//! whose "Verified destination behavior" is the protocol survey). One submodule per
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
//! Raw Datadog fields the model has no typed home for live in event attributes under
//! `_logit.datadog.*` ([ADR `lossless-transit`](../../../../docs/adr/lossless-transit.md)'s carrier
//! rule); the constants below are the only spelling.
//!
//! **The host is `host.name`, both ways.** Every decoder writes a record's own wire host, a
//! series' or sketch's `host`, a log's `hostname`, an event's `host`, or a service check's
//! `host_name`, as `host.name` on the event, and the wire key doesn't survive as an attribute or
//! a carrier ([`insert_wire_host`]). The host displaces a `host.name` the record's tags or keys
//! spelled, counted `logit.input.tags.omitted{reason="reserved_key"}`; with no host of its own, a
//! `host.name` tag is the record's `host.name`. Every encoder writes `host.name` (a non-empty one)
//! back in its route's own field, else the hostless mark ([`ATTR_HOSTLESS`], `datadog_in`'s and
//! `statsd_in`'s reading of a sample the sender left hostless) as the route's empty host, else
//! [`DatadogEncoder::with_default_host`] (`datadog_out`'s process hostname). A `host` attribute is
//! an ordinary tag (`statsd_in` decodes a DogStatsD `host:` tag to `host.name`), and a log's
//! `hostname` attribute would collide with the wire's own key, so it's omitted, counted. A
//! `TracerPayload`'s or `ClientStatsPayload`'s `hostname` is the host of its spans or stats, so
//! it is the batch resource's `host.name` both ways ("Traces" and "APM stats" below); the Agent's
//! own `hostName`, the relaying Agent's identity, stays the carrier
//! [`RESOURCE_ATTR_AGENT_HOSTNAME`]. ADR `process-hostname`'s "Amendment: a source fills the
//! common keys (2026-10-23)" has the rule.
//!
//! **The container is `container.id`, both ways.** A tracer's container id (a `TracerPayload`'s
//! or a `ClientStatsPayload`'s `container_id`, or the `Datadog-Container-ID` header) is the batch
//! resource's [`ATTR_CONTAINER_ID`], and an APM stats `ImageTag` the one element of its
//! [`ATTR_CONTAINER_IMAGE_TAGS`]; no carrier keeps either. On a metric, a Datadog event, or a
//! service check, `container.id` leaves as Datadog's `container_id` tag and
//! `container.image.tags`' first element as `image_tag` ([`tags::render_tags`]), as Datadog's
//! OTLP exporter spells them; on a log, `container.id` is a `container_id` entry in `ddtags`.
//! `datadog_in` reads a `container_id` tag back as an attribute of that name: a tag isn't a
//! field the protocol defines as the container's identity.
//!
//! **Service identity on a tag list is the standard keys, both ways.** On a metric, a Datadog
//! event, or a service check, a valued `env:`, `version:`, or `service:` tag decodes to
//! `deployment.environment.name`, `service.version`, or `service.name` on the event, and every
//! encoder writing a tag list writes each key back as that tag ([`tags`]), the Datadog OTLP
//! exporter's spelling. A tag spelling the key itself beside the Datadog-spelled one is omitted,
//! counted `logit.input.tags.omitted{reason="reserved_key"}`; on encode, a valued `env`,
//! `version`, or `service` attribute beside its key is omitted, counted
//! `logit.output.tags.omitted{reason="unrepresentable"}`. A valueless token stays a tag of its
//! own name.
//!
//! **A log's service identity is on the standard keys, both ways.** A log's `service` is
//! `service.name`, and its `ddtags` `env:` and `version:` entries are `deployment.environment.name`
//! and `service.version`; `datadog_out` writes them back from those keys whatever source wrote
//! them, an OTLP log's resource included ("Logs, events, service checks" below). Only the three
//! Unified Service Tagging entries leave `ddtags`: a tag and a top-level field are different
//! things in Datadog, and every other attribute leaves a log as a top-level field.
//!
//! **A trace's environment and version are `deployment.environment.name` and `service.version`,
//! both ways.** A span's `env` and `version` meta are those keys on the event; a
//! `TracerPayload`'s `env` and `app_version`, and a `ClientStatsPayload`'s `Env` and `Version`,
//! are those keys on the batch resource, and a `ClientStatsPayload`'s `Service` is the resource's
//! [`ATTR_SERVICE_NAME`]. Each encoder writes the event's value as span meta and the resource's
//! in the payload field, by the rule `host.name` follows ("Traces" and "APM stats" below).
//!
//! Events and service checks reuse
//! `crates/logit-proto/src/statsd/decode.rs`'s `_logit.statsd.event.*` /
//! `_logit.statsd.service_check.*` names: they are the same Datadog concepts DogStatsD carries.
//! Tags fold into attributes by [`tags`]'s rule; timestamps convert by [`time`]'s.
//!
//! Every encoder here reads its own carriers by exact name, and skips every other private `_logit.`
//! attribute, its own unread carriers included, wherever it renders attributes generically (tags,
//! log fields, span `meta`, `metrics`, and `meta_struct`, link and span-event attributes, and the
//! stats routes' `no_wire_form` count). It never counts one
//! ([ADR `private-attribute-namespace`](../../../../docs/adr/private-attribute-namespace.md)).
//!
//! **The listener adds sender attributes after decode.** Under `datadog_in`'s or
//! `datadog_trace_in`'s `peer:`, `proxy_protocol:`, or `forwarded:`, `network.peer.*` or
//! `client.*` is added to every event, an opt-in addition the sender never sent, and one of ADR
//! `lossless-transit`'s "Permitted normalizations". It applies to every payload family below.
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
//! | `RATE` / `2` / `"rate"` | `Gauge(v)` + [`METRICS_TYPE`](logit_core::attrs::METRICS_TYPE) `rate` | -- |
//! | `UNSPECIFIED` / `0` / `""`; a v2 JSON series with no `type` | `Gauge(v)` + [`ATTR_TYPE`] `unspecified` | -- |
//! | an unknown type, no `metric`, no `points` array, or a field of the wrong JSON type | the series is dropped; the rest of the request decodes | `logit.input.metrics.dropped{reason="bad_series"}` + diag `bad_series` |
//! | a point that isn't `[ts, value]` / `{timestamp, value}` / `[ts, [values]]` | the point is dropped | `dropped{reason="bad_point"}` |
//! | v1 `[ts, null]` | the point is skipped | `logit.input.metrics.skipped{reason="null_value"}` |
//! | a non-finite protobuf value | the point is dropped | `dropped{reason="non_finite_value"}` |
//! | point timestamp (s) | `Event::timestamp`; a fractional v1 timestamp truncates to its second | -- |
//! | timestamp absent, zero, or negative | `received_at`, truncated to its second | `logit.input.metrics.degraded{reason="no_timestamp"}` |
//! | `unit` | `MetricRecord::unit` when non-empty; every series batch carries [`ATTR_UNITS`] on its resource | -- |
//! | `tags` | attributes by [`tags`]'s rule; a valued `env:`, `version:`, or `service:` tag is `deployment.environment.name`, `service.version`, or `service.name` | -- |
//! | a `deployment.environment.name:`, `service.version:`, or `service.name:` tag beside the `env:`, `version:`, or `service:` tag in the same list | omitted; counted once per series that delivers a point | `logit.input.tags.omitted{reason="reserved_key"}` |
//! | a tag whose key is under the private `_logit.` prefix | omitted; counted once per series that delivers a point | `logit.input.tags.omitted{reason="reserved_prefix"}` |
//! | the first named `host` resource; v1 and distribution `host` | [`ATTR_HOST_NAME`] | -- |
//! | a v1 or distribution `"host":""` (present and empty: the Agent's hostless sample) | [`ATTR_HOSTLESS`] `Bool(true)`; an absent `host` decodes to neither | -- |
//! | the first named `device` resource; v1 `device` | [`ATTR_DEVICE`] (the Agent's v2 serializer sends a v1 `device` as that resource, UNVERIFIED: the recorded Agent ran no check that sets one) | -- |
//! | every other resource, including an empty-named or second `host` | [`ATTR_RESOURCES`]: `Array` of `Map{type, name}`, in wire order | -- |
//! | `source_type_name` | [`ATTR_SOURCE_TYPE_NAME`] when non-empty | -- |
//! | `interval` | [`METRICS_INTERVAL`](logit_core::attrs::METRICS_INTERVAL) (`I64`) when nonzero, on any series type | -- |
//! | protobuf `metadata.origin` | [`ATTR_ORIGIN_PRODUCT`] / [`ATTR_ORIGIN_CATEGORY`] / [`ATTR_ORIGIN_SERVICE`] (`U64`), each when nonzero | -- |
//! | JSON `metadata.origin` `product` / `service` / `metric_type` | [`ATTR_ORIGIN_PRODUCT`] / [`ATTR_ORIGIN_SERVICE`] / [`ATTR_ORIGIN_METRIC_TYPE`], each when nonzero | -- |
//! | distribution point `[ts, [v, ...]]` | `MetricKind::Samples(Samples::new(values))`, rate 1.0; the series' `type` is ignored | -- |
//! | a distribution point with no values | skipped | `skipped{reason="empty_distribution"}` |
//! | a body that isn't JSON (or protobuf), isn't a JSON object, or has a `series` that isn't an array | `CodecError::Malformed` | -- |
//! | an object with no `series` member (the Agent's `{}` connectivity probe, which the intake answers `202`) | an empty batch | -- |
//!
//! A `host.name:x` tag loses to the series' own `host`, an empty one included, and is counted
//! `logit.input.tags.omitted{reason="reserved_key"}`; with no `host`, it's the series'
//! `host.name`. A tag can't spell a carrier or a flag: every one is under the private `_logit.` prefix, which the row above
//! omits, so a `_logit.metrics.type:rate` tag never turns a GAUGE or COUNT series into a RATE.
//!
//! A rate and an interval are neutral metric flags, not Datadog carriers (ADR
//! `protocol-knowledge-stays-in-family`'s "Amendment: move, don't mirror (2026-10-23)"), so a
//! stage outside the Datadog family reads them, and a `Gauge` a script or a `set` flags as a rate
//! leaves `datadog_out` as a `rate`.
//!
//! ## Decode: sketches → events
//!
//! | Wire | Model | Counter / diag |
//! |---|---|---|
//! | one `Dogsketch` | one `Event`, `MetricKind::Distribution` under `Mapping::agent`; `timestamp` = `ts` s (absent: `received_at` on its second, counted as above) | -- |
//! | `k > 0` / `k < 0` / `k == 0` with count `n` | positive bin `key: k` / negative bin `key: -k` / the zero count; repeated keys sum | -- |
//! | `cnt`, `min`, `max`, `sum` | the sketch's exact summary (`SketchStats`); `avg` is ignored | -- |
//! | `metric`, `host`, `tags`, `metadata.origin` | as for a series; an empty `host` is [`ATTR_HOSTLESS`], because protobuf can't tell it from an absent one and the Agent sends one only for a sample it made hostless | -- |
//! | no bins and `cnt == 0` | skipped | `logit.input.metrics.skipped{reason="empty_sketch"}` |
//! | `k`/`n` lengths differ, a key outside ±32767, `cnt <= 0` with populated bins, a non-finite `min`/`max`/`sum`, or no `metric` | dropped | `logit.input.metrics.dropped{reason="bad_sketch"}` + diag `bad_sketch` |
//! | legacy `distributions` entries | dropped: not implemented | `dropped{reason="legacy_distribution"}`, one per entry |
//! | payload `metadata` (`CommonMetadata`) `agent_version`, `timezone`, `internal_ip`, `public_ip` | batch resource [`RESOURCE_ATTR_AGENT_VERSION`], [`RESOURCE_ATTR_AGENT_TIMEZONE`], [`RESOURCE_ATTR_AGENT_INTERNAL_IP`], [`RESOURCE_ATTR_AGENT_PUBLIC_IP`] (`Str`), each when non-empty | -- |
//! | payload `metadata.current_epoch` | batch resource [`RESOURCE_ATTR_AGENT_EPOCH`] (`F64`) when nonzero | -- |
//! | payload `metadata.api_key` | dropped; a credential, never stored | -- |
//!
//! ## Encode: events → series, distribution points, sketches
//!
//! One series (or sketch) per `(event, MetricRecord)`, carrying one point, but for the series a
//! histogram or summary expands to (below). Carriers are read from
//! the resource merged with the event's attributes (event wins); every other attribute renders as
//! a tag by [`tags`]'s rule. A `Map`/`Bytes`/`Null` tag value, or a carrier of the
//! wrong `Value` type, counts `logit.output.tags.omitted{reason="unrepresentable"}`. An event with
//! no metric, log, or span takes no route, counted `logit.output.events.skipped{reason="no_payload"}`
//! by `datadog_out`'s routing ([`DatadogEncoder::events_without_payload`]).
//!
//! The tables below are generated from `datadog_out`'s column of `spec/transit/mapping.yaml`, the
//! canonical copy, by `script/transit docs`, and a test fails when they're stale: change the table
//! there, not here. They cover every route `datadog_out` sends but the APM stats routes, which
//! have no case: logs, events, service checks, and spans included. "Consumer" means `datadog_in`
//! reading the request back. Four drops `datadog_out` makes above these encoders, `stale`,
//! `too_many_tags`, `needs_agent_processing`, and `not_datadog_origin`, and its size cut, which
//! drops an event too large to send alone as `oversize`, read the wall clock, a whole trace chunk,
//! or a request's size, and are outside the spec; `crates/logit-outputs/src/datadog.rs`'s module
//! doc has them.
//!
//! <!-- transit:doc:datadog_out -->
//!
//! Per metric kind, and per record feature that decides a record's fate:
//!
//! | Row | Class | Reason or counter | What a consumer sees |
//! |---|---|---|---|
//! | `sum/delta_monotonic` | exact | -- | A series `count` point, its `interval` from the `_logit.metrics.interval` flag or 0, which datadog_in reads back as a delta monotonic sum. |
//! | `sum/delta_nonmonotonic` | degraded | `logit.output.metrics.degraded{metric_kind=non_monotonic_sum}` | A series `count` point of the raw value, as Datadog's exporter sends any delta sum, which datadog_in reads back as a monotonic delta sum. |
//! | `sum/cumulative_monotonic` | dropped | `logit.output.metrics.dropped{reason=unresolved_cumulative}` | Datadog's exporter differences a running total into a delta first, so it's dropped, with a throttled `unresolved_cumulative` warning; a `cumulative_to_delta` upstream makes it a delta `count`. |
//! | `sum/cumulative_nonmonotonic` | degraded | `logit.output.metrics.degraded{metric_kind=non_monotonic_sum}` | A series `gauge` point of the running value, as Datadog's exporter sends it, which datadog_in reads back as a gauge; a `cumulative_to_delta` upstream with `non_monotonic: difference` or `reset_on_decrease` makes it a delta `count`. |
//! | `sum/f64` | exact | -- | The point's double. |
//! | `sum/i64` | normalized | `integer_as_double` | The point's double, the integer's nearest `f64`, read back as a float. |
//! | `sum/u64` | normalized | `integer_as_double` | The point's double, the integer's nearest `f64`, read back as a float. |
//! | `sum/bool` | normalized | `bool_as_double`; `logit.output.metrics.normalized{reason=bool_value}` | The point's double, `1` or `0`, read back as a float. |
//! | `gauge/f64` | exact | -- | A series `gauge` point, a `rate` under the `_logit.metrics.type` flag, or unspecified under the `_logit.datadog.type` carrier. |
//! | `gauge/i64` | normalized | `integer_as_double` | The point's double, the integer's nearest `f64`, read back as a float. |
//! | `gauge/u64` | normalized | `integer_as_double` | The point's double, the integer's nearest `f64`, read back as a float. |
//! | `gauge/bool` | normalized | `bool_as_double`; `logit.output.metrics.normalized{reason=bool_value}` | The point's double, `1` or `0`, read back as a float. |
//! | `gauge/negative` | exact | -- | A negative double, as any other. |
//! | `gauge/prometheus_info` | exact | -- | As any other gauge; the type flag isn't written. |
//! | `gauge/prometheus_stateset` | exact | -- | As any other gauge; the type flag isn't written. |
//! | `gauge_delta/f64` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | Nothing upstream resolved the relative gauge to an absolute one, so the series encoders drop it. |
//! | `gauge_delta/i64` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | Nothing upstream resolved the relative gauge to an absolute one, so the series encoders drop it. |
//! | `gauge_delta/u64` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | Nothing upstream resolved the relative gauge to an absolute one, so the series encoders drop it. |
//! | `gauge_delta/bool` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | Nothing upstream resolved the relative gauge to an absolute one, so the series encoders drop it. |
//! | `samples` | exact | -- | A distribution point holding every value, which datadog_in reads back as samples at rate 1. |
//! | `set_members` | skipped | `logit.output.metrics.skipped{metric_kind=set_members}` | The series encoders have no form for set members, so none is written. |
//! | `distribution` | exact | -- | A sketch under `Mapping::agent`, bin for bin, with its exact count, sum, min, and max; `avg` is recomputed from them. |
//! | `set` | degraded | `logit.output.metrics.degraded{metric_kind=set}` | A series `gauge` of the estimate, the Agent's `s`; the registers are gone. |
//! | `histogram/delta`, under `histograms: {"mode":"counters","send_aggregation_metrics":true}` | degraded | `logit.output.metrics.degraded{metric_kind=histogram}` | Under `histograms.mode: counters`, a `<name>.bucket` count per bucket tagged `lower_bound` and `upper_bound` in the exporter's `formatFloat` form, beside the `.count`, `.sum`, `.min`, and `.max` series. |
//! | `histogram/delta`, under `histograms: {"mode":"nobuckets","send_aggregation_metrics":true}` | degraded | `logit.output.metrics.degraded{metric_kind=histogram}` | Under `histograms.mode: nobuckets`, the `.count`, `.sum`, `.min`, and `.max` series alone. |
//! | `histogram/delta`, otherwise | degraded | `logit.output.metrics.degraded{metric_kind=histogram}` | A sketch under `Mapping::agent` with its counts spread over the buckets as Datadog's exporter spreads them, and the point's count, sum, and average, which datadog_in reads back as a distribution. A histogram whose one bucket is `+Inf`, such as otlp_in's reading of a bucketless point, keys every observation at the Agent's key -1, about `-9.94e-10`, as the exporter does. |
//! | `histogram/cumulative` | dropped | `logit.output.metrics.dropped{reason=unresolved_cumulative}` | Datadog's exporter differences a running total into a delta first, so it's dropped, with a throttled `unresolved_cumulative` warning; a `cumulative_to_delta` upstream makes it a delta histogram. |
//! | `histogram/nan_bound`, under `histograms: {"mode":"counters","send_aggregation_metrics":true}` | degraded | `logit.output.metrics.degraded{metric_kind=histogram}` | Under `histograms.mode: counters`, a `NaN` bound is one more bucket's bound: the `<name>.bucket` counts beside it carry a `nan` `lower_bound` or `upper_bound` tag, the exporter's `formatFloat` form. |
//! | `histogram/nan_bound`, under `histograms: {"mode":"nobuckets","send_aggregation_metrics":true}` | degraded | `logit.output.metrics.degraded{metric_kind=histogram}` | Under `histograms.mode: nobuckets`, no bucket is written, so the bound goes with the rest. |
//! | `histogram/nan_bound`, otherwise | dropped | `logit.output.metrics.dropped{reason=non_monotonic_bounds}` | Datadog's exporter keys a `NaN` bound at the Agent's key 1 on amd64, so a `NaN` above a bound that keys higher makes a bucket whose upper key is below its lower one, and the point is dropped, as the exporter drops it. A `NaN` below every bound that keys above 1 would instead be sketched with its count at key 1. |
//! | `histogram/neg_inf_bound`, under `histograms: {"mode":"counters","send_aggregation_metrics":true}` | degraded | `logit.output.metrics.degraded{metric_kind=histogram}` | Under `histograms.mode: counters`, a `-Inf` bound is one more bucket's bound: the `<name>.bucket` counts beside it carry a `-inf` `lower_bound` or `upper_bound` tag, the exporter's `formatFloat` form. |
//! | `histogram/neg_inf_bound`, under `histograms: {"mode":"nobuckets","send_aggregation_metrics":true}` | degraded | `logit.output.metrics.degraded{metric_kind=histogram}` | Under `histograms.mode: nobuckets`, no bucket is written, so the bound goes with the rest. |
//! | `histogram/neg_inf_bound`, otherwise | degraded | `logit.output.metrics.degraded{metric_kind=histogram}` | In the sketch, as Datadog's exporter builds it on amd64: Go's `int(-Inf)` keys the bound at the Agent's key -1, so the `-Inf` bucket's count lands near `-1e-9` rather than below every other bucket, and the sketch's `min` is that bin's value. |
//! | `histogram/gauge_histogram` | exact | -- | As `histogram/cumulative`; the type flag isn't written. |
//! | `exponential_histogram/delta` | degraded | `logit.output.metrics.degraded{metric_kind=exponential_histogram}` | A sketch under every `histograms.mode`, its buckets re-mapped onto the Agent's bins as Datadog's exporter re-maps them, with the point's count, sum, and average, which datadog_in reads back as a distribution; with `send_aggregation_metrics`, the `.count`, `.sum`, `.min`, and `.max` series too. |
//! | `exponential_histogram/cumulative` | dropped | `logit.output.metrics.dropped{reason=unresolved_cumulative}` | Datadog's exporter can't send a cumulative exponential histogram, so it's dropped, with a throttled `unresolved_cumulative` warning; a `cumulative_to_delta` upstream makes it a delta one. |
//! | `summary/delta` | degraded | `logit.output.metrics.degraded{metric_kind=summary}` | A `<name>.quantile` gauge per quantile tagged `quantile:<q>`, and `.count` (no unit) and `.sum` as `count` points, as Datadog's exporter writes a summary it differenced, counted once per record; datadog_in reads them back as gauges and counts. |
//! | `summary/cumulative` | degraded | `logit.output.fields.dropped{reason=unresolved_cumulative}` | A `<name>.quantile` gauge per quantile tagged `quantile:<q>`, which datadog_in reads back as gauges. The running count and sum need differencing first, so they're dropped, counted, with a throttled `unresolved_cumulative` warning; a `cumulative_to_delta` upstream makes them sendable. |
//! | `record/no_recorded_value` | skipped | `logit.output.metrics.skipped{reason=no_recorded_value}` | Datadog has no marker for a point with no recorded value, so it isn't written. |
//! | `record/no_recorded_value_delta_sum` | skipped | `logit.output.metrics.skipped{reason=no_recorded_value}` | Datadog has no marker for a point with no recorded value, so it isn't written. |
//! | `record/no_recorded_value_other_kind` | skipped | `logit.output.metrics.skipped{reason=no_recorded_value}` | Datadog has no marker for a point with no recorded value, so it isn't written. |
//! | `record/service_check` | exact | -- | Record 0 of a service check goes to the check route alone, and datadog_in reads it back as the same status gauge and carriers. |
//! | `record/sample_rate` | degraded | `logit.output.metrics.degraded{reason=sample_rate_expanded}` | The distribution route has no sample rate, so each value is repeated by its weight, the inverse of the rate rounded, and datadog_in reads the values back at rate 1. |
//! | `record/superseded` | exact | -- | Each record is its own point at its own timestamp. |
//! | `record/same_instant` | dropped | `logit.output.metrics.dropped{reason=duplicate_timestamp}` | Datadog keeps one point per series per second, the higher of two in one request, so the sink sends the later point and drops this one, counted, when it differs from the one sent or is an increment: two equal delta counts or `rate`s are two events. Only the series route collapses: a `samples` record leaves on the distribution-points route, where Datadog keeps both, and a `distribution`, an `exponential_histogram`, and a `histogram` under `histograms.mode: distributions` on the sketches route, so they don't reach this row, unless `histograms.send_aggregation_metrics` also writes a histogram's `.count`, `.sum`, `.min`, and `.max` series, which collapse. A `dedup_timestamps` with `precision: 1s` upstream merges or collapses the pair instead. |
//! | `record/same_instant_equal` | normalized | `duplicate_timestamp`; `logit.output.metrics.normalized{reason=duplicate_timestamp}` | Datadog keeps one point per series per second, so the sink sends the later point and leaves this state, equal to it, off, counted: Datadog stores the same point either way. Only the series route collapses, as for `record/same_instant`. |
//! | `record/same_millisecond` | dropped | `logit.output.metrics.dropped{reason=duplicate_timestamp}` | Series timestamps are whole seconds, and Datadog keeps one point per series per second, so the sink sends the later point and drops this one, counted, as for `record/same_instant`. |
//! | `record/same_millisecond_equal` | normalized | `duplicate_timestamp`; `logit.output.metrics.normalized{reason=duplicate_timestamp}` | Series timestamps are whole seconds, so the sink sends the later point and leaves this one, equal to it, off, counted, as for `record/same_instant_equal`. |
//! | `record/non_finite` | dropped | `logit.output.metrics.dropped{reason=non_finite_value}` | JSON has no `NaN` or infinity, so the point isn't written. |
//! | `log/dogstatsd_event` | exact | -- | One `/api/v1/events` request per event, read back with the same `_logit.statsd.event.*` carriers. |
//! | `log/plain` | exact | -- | One object of a `/api/v2/logs` body, its attributes as top-level keys. |
//! | `log/message_unsafe` | normalized | `message_text` | The `message` field is a string: a `Bytes` message leaves as lossy UTF-8 and any other non-string message as its JSON text, which datadog_in reads back as a string. |
//! | `log/structured_message` | normalized | `message_text` | A map or array message leaves as its JSON text, read back as a string. |
//! | `log/bytes_message` | normalized | `message_text` | A bytes message leaves as lossy UTF-8 text, read back as a string; an invalid sequence reads back as U+FFFD. |
//! | `log/blank_message` | exact | -- | An empty message is an empty `message` field; a log with no message leaves `null` as its text, which the `message_unsafe` row covers. |
//! | `span/alone` | exact | -- | A span of an `AgentPayload` chunk on the trace route, read back with its kind as a `span.kind` attribute and its trace id's high half as `_dd.p.tid` (the `attributes` rows). |
//! | `span/beside_log` | exact | -- | A span of an `AgentPayload` chunk on the trace route, read back with its kind as a `span.kind` attribute and its trace id's high half as `_dd.p.tid` (the `attributes` rows). |
//! | `span/no_wire_form` | degraded | `logit.output.spans.degraded{reason=no_wire_form}` | Datadog has no field for `status: ok`, a status message, `trace_state`, span flags, or a dropped count, so each is dropped, counted, and the span reads back without it. |
//! | `event/attributes_only` | skipped | `logit.output.events.skipped{reason=no_payload}` | An event with no metric, log, or span takes none of the intake's routes, so no request carries it. |
//! | `event/string_only` | skipped | `logit.output.events.skipped{reason=no_payload}` | An InfluxDB point whose fields are all strings has no metric, so it takes none of the intake's routes and no request carries it. |
//!
//! Per field of a record that reaches the wire, attribute shape, and payload beside metrics:
//!
//! | Row | Class | Reason or counter | What a consumer sees |
//! |---|---|---|---|
//! | `record/unit` | lost | `logit.output.fields.omitted{reason=unit_unmapped}` | The series' `unit` as Datadog names it. A unit datadog_in decoded (its batch marked `_logit.datadog.units`) leaves as it is. Any other source's unit leaves as it is when it's a Datadog unit name, else mapped as Datadog's OTel exporter maps a UCUM unit (`ms` as `millisecond`, `By/s` as `byte/second`, `{request}` as `request`); one with no Datadog name, `1` included, is left off, counted once per record. |
//! | `record/description` | lost | structural | Datadog's intake has no description, so none is written. |
//! | `record/start_timestamp` | lost | structural | A series point has no start time, so none is written. |
//! | `record/exemplars` | lost | structural | Datadog's intake has no exemplars, so none are written. |
//! | `record/unit_on_distribution` | lost | structural | Distribution points and sketches have no unit field, so a `samples` or `distribution` record's unit is left off; only the series routes write one. |
//! | `record/sample_rate_unweighted` | lost | `logit.output.metrics.degraded{reason=sample_rate_rounded}` | A rate below 1 that rounds to weight 1, such as 0.8, writes each value once on the distribution route, which has no rate, so datadog_in reads it back at rate 1, counted once per record. |
//! | `attributes/string_value` | exact | -- | A `key:value` tag, `host` included. `host.name` is the record's host, which datadog_in reads back as `host.name` on every route. On a log, a top-level key of the log object. |
//! | `attributes/host_beside_host_name` | exact | -- | A `host:` tag, which datadog_in reads back as `host`. |
//! | `attributes/hostname` | lost | `logit.output.tags.omitted{reason=reserved_key}` | A log's `hostname` is `host.name`, so a `hostname` attribute has no key of its own on the wire and is left off, counted. |
//! | `attributes/log_service` | lost | `logit.output.tags.omitted{reason=reserved_key}` | A log's `service` is `service.name`, so a `service` attribute beside it has no key of its own on the wire and is left off, counted. |
//! | `attributes/log_service_field` | normalized | `respelled` | A log's string `service` attribute with no `service.name` is the log's `service` field, which datadog_in reads back as `service.name`. |
//! | `attributes/container_id` | normalized | `respelled` | On a metric, an event, or a service check, Datadog's `container_id` tag, the Datadog OTLP exporter's spelling, which datadog_in reads back as a `container_id` attribute. On a log, a `container_id:<id>` entry appended to `ddtags`, the Agent's tag for a container's logs, which datadog_in keeps in the `ddtags` string, so no `container.id` reads back. |
//! | `attributes/service_identity` | exact | -- | On a metric, an event, or a service check, Datadog's `env`, `version`, or `service` tag, the Datadog OTLP exporter's spelling, which datadog_in reads back as the key. |
//! | `attributes/service_tag` | normalized | `respelled` | On a metric, an event, or a service check, the tag of that name, which datadog_in reads back as `deployment.environment.name`, `service.version`, or `service.name`. |
//! | `attributes/service_tag_beside_key` | lost | `logit.output.tags.omitted{reason=unrepresentable}` | On a metric, an event, or a service check, beside its standard key, which leaves as the same tag, a valued `env`, `version`, or `service` would be a second value for the key, so it's left off, counted once per wire tag. |
//! | `attributes/bool_value` | normalized | `bool_tag` | `true` is a bare `key` tag and reads back `true`; `false` is `key:false` and reads back as the string. On a log, a JSON boolean, read back as itself. |
//! | `attributes/number_value` | normalized | `stringified` | A `key:<number>` tag, read back as a string. On a log, a JSON number, read back as itself. |
//! | `attributes/array_value` | normalized | `stringified` | One tag per element in order, which datadog_in folds back into an array of strings, so a number element reads back as its text. On a log, a JSON array, read back as itself. |
//! | `attributes/map_value` | lost | `logit.output.tags.omitted{reason=unrepresentable}` | A tag has no nested form, so a map is left off, counted. On a log, a JSON object, read back as itself. |
//! | `attributes/bytes_value` | lost | `logit.output.tags.omitted{reason=unrepresentable}` | A tag has no bytes form, so the value is left off, counted. On a log, its base64 text, read back as a string. |
//! | `attributes/timestamp_value` | normalized | `stringified` | A `key:<RFC 3339>` tag, read back as a string. On a log, the RFC 3339 text as a JSON string. |
//! | `attributes/null_value` | exact | -- | On a log, a JSON null, read back as itself. |
//! | `attributes/private_carrier` | lost | `logit.output.tags.omitted{reason=no_wire_form}` | A `_logit.datadog.*` carrier rides in its own wire field; one the route has no field for is left off, counted. `_logit.datadog.units`, which marks a decoded batch's units as Datadog names, is read and not written; datadog_in stamps it again on every series batch. The v2 series route writes `_logit.datadog.hostless` as a `host` resource with no name, which datadog_in reads into `_logit.datadog.resources`. |
//! | `attributes/prometheus_target` | lost | structural | The scrape target is private and never a tag, so it's left off. |
//! | `attributes/foreign_carrier` | lost | structural | Another protocol's `_logit.` carrier is private and never a tag, so it's left off. The `_logit.statsd.event.*` and `_logit.statsd.service_check.*` carriers are read as the Datadog fields they name. |
//! | `attributes/timestamped` | lost | structural | The flag is private and never a tag, so it's left off. The point carries its timestamp, in whole seconds. |
//! | `attributes/rate` | exact | -- | A flagged gauge is written as a series `rate` point, and datadog_in sets the flag again. |
//! | `attributes/interval` | exact | -- | The series `interval` of a series point of any type, and datadog_in sets the flag again. |
//! | `attributes/rate_on_non_gauge` | lost | structural | The flag applies to a gauge alone, so on any other kind it's ignored: the point keeps the type its kind gives it, and datadog_in sets no flag. |
//! | `attributes/family_type` | lost | structural | The flag is private and never a tag, so it's left off. |
//! | `attributes/family_type_on_other_kind` | lost | structural | The flag is private and never a tag, so it's left off. |
//! | `attributes/interval_on_distribution` | lost | `logit.output.tags.omitted{reason=no_wire_form}` | The distribution-points and sketches routes have no interval field, so a nonzero interval is left off, counted. |
//! | `attributes/interval_on_histogram`, under `histograms: {"mode":"counters","send_aggregation_metrics":true}` | exact | -- | Under `histograms.mode: counters`, the `interval` of each series the histogram writes, and datadog_in sets the flag again. |
//! | `attributes/interval_on_histogram`, under `histograms: {"mode":"counters","send_aggregation_metrics":false}` | exact | -- | Under `histograms.mode: counters`, the `interval` of each `<name>.bucket` series, and datadog_in sets the flag again. |
//! | `attributes/interval_on_histogram`, under `histograms: {"mode":"nobuckets","send_aggregation_metrics":true}` | exact | -- | Under `histograms.mode: nobuckets`, the `interval` of each series the histogram writes, and datadog_in sets the flag again. |
//! | `attributes/interval_on_histogram`, otherwise | lost | `logit.output.tags.omitted{reason=no_wire_form}` | Under `histograms.mode: distributions`, the default, the sketches route has no interval field, so a nonzero interval is left off, counted. |
//! | `attributes/resource_attribute` | normalized | `resource_as_tag` | Merged into the record's tags, so it reads back as an event attribute. |
//! | `attributes/scope` | lost | structural | Datadog has no instrumentation scope, so none is written. |
//! | `attributes/datadog_carrier_log` | lost | uncounted | A `_logit.datadog.*` carrier on a log, a Datadog event, or a service check that the route has no field for is left off, uncounted, where the metrics routes count one as `no_wire_form`; the events route's public form has no field for the Agent envelope's `_logit.datadog.agent.hostname`. |
//! | `attributes/severity_carrier_otel` | lost | structural | A Datadog log has a status and no raw OTLP severity, so the carriers are left off and the severity reads back at band resolution. |
//! | `attributes/severity_carrier_syslog` | lost | structural | A Datadog log has a status and no raw syslog severity, so the carrier is left off and the severity reads back at band resolution. |
//! | `attributes/syslog_header` | normalized | `u64_as_int` | A top-level key of the log object; a `u64` `syslog.facility` or `process.pid` is a JSON number, which datadog_in reads back as an `i64`. |
//! | `attributes/syslog_msgid` | exact | -- | A top-level key of the log object. |
//! | `attributes/syslog_sd` | exact | -- | A JSON object in the log object, read back as itself. |
//! | `attributes/syslog_timestamp` | normalized | `stringified` | A top-level key of the log object: an RFC 5424 timestamp as its RFC 3339 text, read back as a string. |
//! | `attributes/host_name` | exact | -- | The log's `hostname`, or the event's `host`, read back as `host.name`. |
//! | `attributes/splunk_envelope` | normalized | `resource_as_tag` | Merged into the log's attributes, so it reads back as an event attribute. |
//! | `attributes/syslog_facility` | exact | -- | A log with no `syslog.facility` leaves with none. |
//! | `attributes/no_host` | normalized | `default_host` | A record with no host and no hostless mark leaves with the process hostname as its host, which datadog_in reads back as `host.name`; a span's host is its batch resource's `host.name`, filled only when no relaying Agent's hostname rides beside it. |
//! | `attributes/log_status` | normalized | `status_from_severity` | A log with a severity and no `status` attribute leaves with `status` set to the severity's name, which datadog_in reads back as a `status` attribute beside the severity. |
//! | `attributes/log_severity_fields` | exact | -- | The severity is `status`'s; no `otel.log.severity.*` attribute is written. |
//! | `attributes/span_kind` | normalized | `span_kind_meta` | A span whose kind isn't internal, with no `span.kind` attribute, leaves with `meta["span.kind"]`, which datadog_in reads back as a `span.kind` attribute beside the kind. |
//! | `attributes/trace_id_high` | normalized | `trace_id_high_tag` | On the first span of a trace in a batch, when no span of the trace carries `_dd.p.tid`, a 128-bit trace id's high 64 bits leave as `meta["_dd.p.tid"]`, which datadog_in reads back as a `_dd.p.tid` attribute beside the trace id. |
//! | `log/severity` | exact | -- | The `status` field, the severity's name unless a `status` attribute stands in its place, which datadog_in reads back as the severity. |
//! | `log/severity_unset` | exact | -- | No `status` field, read back as no severity. |
//! | `log/body_format` | lost | structural | A Datadog log has no body format, so a `json` or `structured` format reads back as `raw`. |
//! | `log/event_name` | lost | structural | A Datadog log has no event name, so none is written. |
//! | `log/observed_timestamp` | lost | structural | A Datadog log has no observed time, so none is written. |
//! | `log/observed_timestamp_unset` | exact | -- | No observed time is written, and none reads back. |
//! | `log/trace` | normalized | `trace_as_attributes` | The ids leave as hex `trace_id` and `span_id` keys, which Datadog's log intake reads for trace correlation and datadog_in reads back as attributes, not as the log's trace. |
//! | `log/trace_unset` | exact | -- | No trace ids are written, and none read back. |
//! | `log/trace_flags` | lost | structural | A Datadog log has no trace flags, so none are written. |
//! | `log/trace_beside_id_attribute` | lost | `logit.output.tags.omitted{reason=reserved_key}` | A `trace_id` or `span_id` attribute wins and is written under its own name, so a relayed Datadog log leaves as it arrived, and the log's own ids are left off, counted once per log. |
//! | `log/dropped_attributes_count` | lost | structural | A Datadog log has no dropped-attribute count, so none is written. |
//! | `log/timestamp` | normalized | `milliseconds` | The `timestamp` field, in Unix milliseconds, so a finer timestamp is truncated. |
//! | `log/receipt_timestamp` | normalized | `milliseconds` | The `timestamp` field, in Unix milliseconds, the time the log was received. |
//! | `log/beside_metrics` | exact | -- | The log goes to the logs route, the metrics to theirs, and each reads back as its own event. |
//! | `span/beside_metrics` | normalized | `no_wire_form`; `logit.output.spans.degraded{reason=no_wire_form}` | The span goes to the trace route, the metrics to theirs, and each reads back as its own event; the span loses the fields Datadog has no form for, as `span/alone` says. |
//!
//! <!-- transit:end -->
//!
//! The rest of the encode mapping is hand-written: rules the spec has no row for, and details its
//! cells don't state.
//!
//! | Model | Wire | Counter |
//! |---|---|---|
//! | `Distribution` under `Mapping::agent` | a sketch only: `Dogsketch{ts, cnt, min, max, avg: sum/cnt, sum, k, n}`, `k` ascending (negative bins from the largest magnitude as `-key`, then `0`, then positive), a count above 65535 split into repeated entries, the remainder first | -- |
//! | `Distribution` under a logarithmic mapping | re-binned into `Mapping::agent` at each bin's representative value, summary kept | `degraded{reason="rebinned"}` |
//! | a sketch bin with a fractional count | rounded; a bin rounding to 0 is omitted | `degraded{reason="fractional_count"}`, once per sketch |
//! | a `Distribution` with no bins and zero count | skipped | `logit.output.metrics.skipped{reason="empty_sketch"}` |
//! | a `Distribution` whose counts split into more than [`sketches::MAX_DOGSKETCH_ENTRIES`] `k`/`n` entries | dropped | `logit.output.metrics.dropped{reason="oversized_sketch"}` + diag `oversized_sketch` |
//! | a delta `Histogram` under `histograms.mode: distributions`, and every delta `ExponentialHistogram` | a sketch built as Datadog's exporter builds it (`logit_core::sketch::from_otlp`): `Dogsketch{cnt, min, max, avg, sum}` the converter's own summary, `avg` included | `degraded{metric_kind}` |
//! | such a histogram that can't be sketched | none: no observations; more than 4,096 × 65,535 of them; a bucket whose upper bound keys below its lower; an exponential histogram past the Agent's key range | `skipped{reason="empty_sketch"}`; `dropped{reason="oversized_sketch"}` + diag `oversized_sketch`; `dropped{reason="non_monotonic_bounds"}`; `dropped{reason="sketch_range"}` |
//! | a delta `Histogram` under `histograms.mode: counters` | a `<name>.bucket` series `count` per bucket, empty buckets included, tagged `lower_bound:` and `upper_bound:` in the exporter's `formatFloat` form ([`series::format_float`]), with no unit | `degraded{metric_kind="histogram"}` |
//! | `histograms.send_aggregation_metrics` on a delta `Histogram` or `ExponentialHistogram` | `<name>.count` (a `count`, no unit, the buckets' total or the record's `count`) and `<name>.sum` (a `count`), then `<name>.min` and `<name>.max` gauges where the record has them, ahead of any `.bucket` series; none when the sum is absent or non-finite, and none for an exponential histogram past the observation cap, as the exporter drops it whole | `fields.omitted{reason="no_sum"}` for a missing sum |
//! | a cumulative `Summary` | a `<name>.quantile` gauge per quantile tagged `quantile:` in `formatFloat`'s form, with the record's unit; no `.count` or `.sum`, since they're running totals | `fields.dropped{reason="unresolved_cumulative"}` + diag `unresolved_cumulative`, once per summary |
//! | a delta `Summary` (from `cumulative_to_delta`) | the `.quantile` gauges, then `<name>.count` (a `count`, no unit) and `<name>.sum` (a `count`, the record's unit), as the exporter's `MapSummaryMetrics` writes a summary it differenced | -- |
//! | a cumulative monotonic `Sum`, cumulative `Histogram`, or cumulative `ExponentialHistogram` | dropped: Datadog's exporter differences it into a delta first; a `cumulative_to_delta` upstream does that | `dropped{reason="unresolved_cumulative"}` + diag `unresolved_cumulative` |
//! | a series whose tags, the event's and its own (a `.bucket` series' bounds, a `.quantile` series' quantile), pass [`MAX_METRIC_TAGS`] | left off; [`series::series_over_tag_limit`] counts it for `datadog_out`, which counts it `records.dropped{reason="too_many_tags"}` before encoding | -- |
//! | a record flagged `NO_RECORDED_VALUE` that a sketch route would carry | skipped once, by the sketches route; the series route leaves its aggregation series off, uncounted | `skipped{reason="no_recorded_value"}` |
//! | a kind another metrics route carries | left for that route, uncounted: only the series encoders count skips, and only of kinds no route carries | -- |
//! | record 0 of a service check (`_logit.statsd.service_check.name` present and a `Gauge` first metric) | left for the service-checks route, uncounted; any later record encodes as usual, without the check's `_logit.statsd.service_check.*` carriers as tags | -- |
//! | every record of an APM stats event ([`stats::ATTR_STATS_NAME`] present; [`is_datadog_stats`]) | left for the stats routes, uncounted: its `Sum`s and summary `Distribution`s are a stats group, not series or sketches | -- |
//! | `Event::timestamp` | whole seconds, rounded toward negative infinity | -- |
//! | two series points in one batch (under `datadog_out`, planned before its request cuts; [`series::SeriesDuplicates`]) with one metric name, one tag set (order and repeats aside), one host, device, and resources, in one second | the latest point is written, the later in batch order on a tie; Datadog would keep the higher of the two, not the later, and never sums two counts. The type, unit, interval, and origin aren't part of the key. Distribution points and sketches aren't collapsed | per point left off: `logit.output.metrics.normalized{reason="duplicate_timestamp"}` when both records are states (a gauge not written as a `rate`, or a cumulative kind) and it equals the point written (the value by its bits, and the type, unit, interval, source type, and origin), else `logit.output.metrics.dropped{reason="duplicate_timestamp"}` + diag `duplicate_timestamp` |
//! | [`ATTR_HOST_NAME`] | v2: a `{type: "host"}` resource, first; v1, distribution, and sketch: `host` | -- |
//! | a `host`-typed entry in [`ATTR_RESOURCES`] and no host from the rows above (the Agent's hostless v2 series: a `host` resource with no name) | v2: that entry, in order; v1, distribution, and sketch: a `host` of `""` (the entry itself is omitted, per the `no_wire_form` row); never filled | -- |
//! | [`ATTR_HOSTLESS`] `true` and no host from the rows above | v2: a `{type: "host", name: ""}` resource, the Agent's spelling; v1, distribution, and sketch: a `host` of `""`; never filled. Beside a host from the rows above, the mark is private and left unwritten, uncounted | -- |
//! | no host from any row above ([`ATTR_HOST_NAME`] absent or empty) | the default host ([`DatadogEncoder::with_default_host`]) where [`ATTR_HOST_NAME`] would go; with none, no v2 resource, no v1 or distribution `host`, and a sketch `host` of `""`. A non-`Str` [`ATTR_HOST_NAME`] is counted `unrepresentable` and never replaced: no v2 resource, no v1 or distribution `host`, and a sketch `host` of `""` | -- |
//! | [`ATTR_CONTAINER_ID`]; [`ATTR_CONTAINER_IMAGE_TAGS`] | the `container_id` tag; one `image_tag` tag from the array's first element, the Datadog OTLP exporter's spellings ([`tags::render_tags`]) | a further `container.image.tags` element, and every tag a `container_id` or `image_tag` attribute beside its key would write: `logit.output.tags.omitted{reason="unrepresentable"}` |
//! | [`ATTR_DEVICE`] | v1: `device`; v2: a `{type: "device"}` resource after the host | -- |
//! | [`ATTR_RESOURCES`] | v2 resources after host and device, in order | -- |
//! | `MetricRecord::unit`, on a series that carries one | under [`ATTR_UNITS`] (a `datadog_in` series batch), the unit as it is; otherwise the series `unit` as Datadog names it ([`units::to_datadog`]): a Datadog unit name as it is, else the UCUM unit mapped as Datadog's OTel exporter maps it (`ms` → `millisecond`, `By/s` → `byte/second`, `{request}` → `request`), else none | a unit with no Datadog name, `1` included: `logit.output.fields.omitted{reason="unit_unmapped"}`, once per record |
//! | the `_logit.datadog.origin.*` codes | protobuf and sketch `metadata.origin` (no `metric_type`); JSON v2 `metadata.origin` (no `category`) | -- |
//! | a carrier the route has no field for (v1: resources and origin; distribution points: all but the host; sketches: device, resources, source type, interval, type, `metric_type`; every route but sketches: the `_logit.datadog.agent.*` payload metadata) | omitted | `logit.output.tags.omitted{reason="no_wire_form"}`, one per carrier |
//! | batch resource `_logit.datadog.agent.version` / `.timezone` / `.epoch` / `.internal_ip` / `.public_ip` | sketches: payload `metadata` (`CommonMetadata`, `api_key` `""`); omitted when the resource carries none | -- |
//!
//! v1 JSON emits the Agent's field set: `metric`, `points`, `tags`, `type`, and `interval` always;
//! `host`, `device`, `source_type_name`, and `unit` when present. `host` is absent only with no
//! host, no hostless mark, and no default host (`datadog_out` always sets one), or with a
//! non-`Str` [`ATTR_HOST_NAME`], which is never replaced: an empty one would decode as hostless.
//! Names and tags go out as-is, because Datadog sanitizes them itself.
//!
//! ## Permitted normalizations (metrics)
//!
//! `datadog_in -> datadog_out` on one metrics route is a fixed point modulo this list
//! (`tests/datadog_metrics_fixed_point.rs` pins it):
//!
//! 1. point timestamps are whole seconds: a fractional v1 timestamp truncates, and a missing one
//!    becomes `received_at`, truncated;
//! 2. a series with N points leaves as N one-point series. Two of those points in one second
//!    aren't a normalization: the earlier is dropped and counted (the encode table above), so a
//!    relay of such a request isn't a fixed point;
//! 3. v2 resources reorder to host, device, then the rest; tags reorder by attribute order, and an
//!    exact duplicate tag is dropped. A `deployment.environment.name:`, `service.version:`, or
//!    `service.name:` tag beside its `env:`, `version:`, or `service:` tag is omitted at decode,
//!    counted, and the Datadog-spelled tag relays;
//! 4. a zero `interval` or origin code, an empty `unit`/`source_type_name`/`device`, and a v1 `type`
//!    of `gauge` or none all encode to the same wire;
//! 5. a v1 point with a `null` value, a distribution point with no values, and an empty sketch are
//!    skipped and counted;
//! 6. a sketch's repeated `k` entries sum on decode and re-split at 65535 on encode, and its `avg`
//!    is recomputed as `sum / cnt`;
//! 7. across routes (v1 and v2), a carrier the target has no field for is omitted and counted
//!    `no_wire_form`, per the encode table;
//! 8. a JSON route (v1, v2 JSON, distribution points) carries a metric value exactly: this
//!    crate's `serde_json` dependency enables `float_roundtrip` (see `Cargo.toml`). The protobuf
//!    routes (v2 protobuf, sketches) never go through that parser and carry values bit-exact
//!    either way;
//! 9. a series, distribution point, or sketch with no host of its own leaves with the relay's
//!    process hostname (`datadog_out`'s default host). Its own host is a non-empty `host.name`. A
//!    hostless sample is never filled: a v2 `host` resource with no name, or a v1, distribution,
//!    or sketch `"host":""` ([`ATTR_HOSTLESS`]). A non-empty `host.name` outranks a hostless
//!    mark, which it fills, and a `host.name:` tag beside the series' own host is omitted and
//!    counted at decode. Across routes it
//!    leaves in the target's hostless spelling: `""` on v1, distribution points, and sketches, and
//!    a `host` resource with no name on v2;
//! 10. a tag whose key is under the private `_logit.` prefix is omitted and counted.
//! 11. every series batch carries [`ATTR_UNITS`] on its resource, so its units leave as they
//!     arrived, whatever they spell. The UCUM mapping in the encode table applies only to other
//!     sources' units.
//!
//! # Logs, events, service checks (`logs`, `events`, `service_checks`)
//!
//! All three are JSON. Events and service checks decode to exactly what `statsd_in`'s DogStatsD
//! `_e{...}`/`_sc|...` parsers build, under the same
//! `_logit.statsd.event.*`/`_logit.statsd.service_check.*` names ([`events::ATTR_EVENT_TITLE`] and
//! its siblings, [`service_checks::ATTR_SERVICE_CHECK_NAME`] and its siblings), so a DogStatsD
//! event and an intake event are indistinguishable in the model and either re-encodes on either
//! protocol. Every decoded batch's `Resource` is empty except an events envelope's.
//!
//! ## Decode: logs (`/api/v2/logs`, `/v1/input`) → events
//!
//! | Wire | Model | Counter / diag |
//! |---|---|---|
//! | body: a JSON array of objects, or one bare object | one [`logit_core::Event::log`] per object | anything else (or not JSON): `CodecError::Malformed` |
//! | body: a bare empty object `{}`, the connectivity check an Agent's logs sender posts before its first batch (`testdata/interop/datadog/agent-api-v2-logs-000.bin`) | no events | -- |
//! | an array element that isn't an object | dropped | `logit.input.logs.dropped{reason="not_an_object"}` + diag `malformed_log` |
//! | `message` string | `LogRecord::message` = `Str`, `body_format: Raw` | -- |
//! | `message` of another JSON type | `Str` of its JSON text | -- |
//! | `message` absent or `null` | item dropped | `logit.input.logs.dropped{reason="no_message"}` |
//! | `status` | attribute `status`, verbatim; `severity` by case-insensitive match: `emergency`/`emerg`/`alert`/`critical`/`crit`/`fatal` → `Fatal`, `error`/`err` → `Error`, `warning`/`warn` → `Warn`, `notice`/`info` → `Info`, `debug` → `Debug`, `trace` → `Trace`, else `None` | -- |
//! | `timestamp`: integer (or fractional) milliseconds, or an RFC 3339 string | `Event::timestamp`; a fractional number is read from its own text, every digit to the nanosecond kept ([`time::millis_text_to_nanos`]) | -- |
//! | `timestamp` absent, `null`, or `0` | `received_at` | -- |
//! | `timestamp` another type, or an unparseable string | `received_at` | diag `bad_timestamp` |
//! | `hostname`, a non-empty string | [`ATTR_HOST_NAME`], displacing a `host.name` key | `logit.input.tags.omitted{reason="reserved_key"}` for a displaced key |
//! | `hostname`, an empty string | nothing: no host | -- |
//! | `service`, a non-empty string | `service.name`, displacing a `service.name` key | `logit.input.tags.omitted{reason="reserved_key"}` for a displaced key |
//! | `service`, not a string | the attribute `service`, verbatim (`service.name` is a string); it still wins over a `service:` entry in `ddtags` and a `service.name` key, both left off | as above, per left-off entry or key |
//! | `service`, an empty string | nothing: no service | -- |
//! | `ddtags` `env:<v>`, `version:<v>`, `service:<v>` entries with a non-empty value (Unified Service Tagging's three tags) | `deployment.environment.name`, `service.version`, `service.name`, from the first such entry per key, but the log's own `service` wins for `service.name`; each displaces a key of its name. With no such entry and no `service`, a field of the key's name the log carries wins and stays as it is: a non-empty string, or for `service.name` anything but an empty string. Every other entry naming a key with a winner (a later one, an empty or bare one, the `service:` beside a `service` field) is left out of `ddtags` | one `logit.input.tags.omitted{reason="reserved_key"}` per left-out entry and displaced key |
//! | `ddtags`, a string | `ddtags`, verbatim when no entry is lifted; else its other entries joined with `,` in their order, empty tokens left out, and no attribute when none remains. Splitting it further is a transform's job | -- |
//! | `ddsource`, a `ddtags` that isn't a string, and a `hostname` that isn't a string | attributes of the same names, verbatim | -- |
//! | any other key | an attribute of the same name: object → `Map`, array → `Array`, integer → `I64` (`U64` above `i64::MAX`), other number → `F64`, string → `Str`, bool, `null` → `Null` | -- |
//!
//! ## Decode: events (`/api/v2/events` envelope, `/api/v1/events`) → events
//!
//! A top-level object whose `events` member is an object is the Agent's envelope
//! (`{"apiKey","events":{"<source>":[...]},"internalHostname"}`); any other object is one public
//! event. Both shapes accept both field spellings (`msg_title`/`title`, `msg_text`/`text`,
//! `timestamp`/`date_happened`). An empty optional string means the same as an absent one, because
//! the Agent omits empty fields, but for `host`, which the Agent always writes.
//!
//! | Wire | Model | Counter / diag |
//! |---|---|---|
//! | body not a JSON object | -- | `CodecError::Malformed` |
//! | envelope `internalHostname`, non-empty | resource [`RESOURCE_ATTR_AGENT_HOSTNAME`] | -- |
//! | envelope `apiKey` | dropped; a credential, never stored | -- |
//! | envelope group that isn't an array, or an item that isn't an object | dropped | `logit.input.events.dropped{reason="malformed"}` + diag `malformed_event` |
//! | neither a title nor a text | item dropped | `logit.input.events.dropped{reason="no_title"}` |
//! | `msg_title`/`title` | `_logit.statsd.event.title` (`""` when only a text arrived) | -- |
//! | `msg_text`/`text` | `LogRecord::message` = `Str`, `body_format: Raw`, `event_name: None` | -- |
//! | `timestamp`/`date_happened` seconds | `Event::timestamp`; absent or `0` → `received_at` | -- |
//! | `host`, non-empty | [`ATTR_HOST_NAME`], displacing a `host.name` tag | `logit.input.tags.omitted{reason="reserved_key"}` for a displaced tag |
//! | `host`, `""` (the Agent always writes it, so this is its hostless event) | [`ATTR_HOSTLESS`] `Bool(true)`, displacing a `host.name` tag, as a series' `"host":""` | as above |
//! | `priority`, `aggregation_key` | `_logit.statsd.event.priority`, `.aggregation_key` | -- |
//! | `tags` | attributes, by [`tags`]'s rule; a key under the private `_logit.` prefix omitted; a valued `env:`, `version:`, or `service:` tag the standard key, and a valued tag spelling that key beside it omitted | `logit.input.tags.omitted{reason="reserved_prefix"}` for a private key, `{reason="reserved_key"}` for a standard key beside its Datadog tag |
//! | `alert_type` | `_logit.statsd.event.alert_type`, verbatim; `severity`: `error` → `Error`, `warning` → `Warn`, `success`/`info` → `Info`, else `None` | -- |
//! | `source_type_name`, else the envelope group key unless it is `api` | `_logit.statsd.event.source_type` | -- |
//! | `event_type` (Agent) | [`ATTR_EVENT_TYPE`] | -- |
//! | `device_name`, `related_event_id` (public) | [`ATTR_EVENT_DEVICE_NAME`], [`ATTR_EVENT_RELATED_EVENT_ID`] (the id as `I64`) | -- |
//!
//! ## Decode: service checks (`/api/v1/check_run`) → events
//!
//! | Wire | Model | Counter / diag |
//! |---|---|---|
//! | body: a JSON array of objects (one bare object is accepted too) | one [`logit_core::Event::metric`] per check | anything else: `CodecError::Malformed` |
//! | an element that isn't an object | dropped | `logit.input.metrics.dropped{reason="malformed"}` + diag `malformed_service_check` |
//! | `check`, absent or empty | dropped | `logit.input.metrics.dropped{reason="no_name"}` |
//! | `check` | the record's name, and `_logit.statsd.service_check.name` | -- |
//! | `status` 0 to 3 | `MetricKind::Gauge(status)`, and `_logit.statsd.service_check.status` = `U64` | -- |
//! | `status` absent, negative, above 3, or not an integer | dropped | `logit.input.metrics.dropped{reason="invalid_status"}` |
//! | `host_name`, non-empty | [`ATTR_HOST_NAME`], displacing a `host.name` tag | `logit.input.tags.omitted{reason="reserved_key"}` for a displaced tag |
//! | `host_name`, `""` (the Agent always writes it, so this is its hostless check) | [`ATTR_HOSTLESS`] `Bool(true)`, displacing a `host.name` tag, as a series' `"host":""` | as above |
//! | `message`, non-empty | `_logit.statsd.service_check.message` | -- |
//! | `tags`, an array or `null` | attributes, by [`tags`]'s rule; a key under the private `_logit.` prefix omitted; a valued `env:`, `version:`, or `service:` tag the standard key, and a valued tag spelling that key beside it omitted | `logit.input.tags.omitted{reason="reserved_prefix"}` for a private key, `{reason="reserved_key"}` for a standard key beside its Datadog tag |
//! | `timestamp` seconds | `Event::timestamp`; absent or `0` → `received_at` | -- |
//!
//! ## Encode: events → logs, events, service checks
//!
//! Each encoder reads the merged resource and event attributes (the event's value winning), and
//! skips an event that belongs to another route without counting it: that is ordinary fan-out. The
//! carriers decide which route owns an event, through one shared predicate per route, so the routes
//! never disagree: a `log` carrying `_logit.statsd.event.title` is a Datadog event, sent only by
//! `encode_events`; `_logit.statsd.service_check.name` with a `Gauge` first metric is a service
//! check, whose record 0 is sent only by `encode_service_checks` (the metrics routes send its later
//! records, as `statsd_out` does); `_logit.datadog.stats.name` marks APM stats, sent whole and only
//! by the stats routes (see "APM stats" below).
//!
//! | Model | Wire | Counter |
//! |---|---|---|
//! | **logs** ([`DatadogEncoder::encode_logs`]): an event with a `log` and no `_logit.statsd.event.title` | one object in a JSON array; `None` when no event qualifies | -- |
//! | `LogRecord::message` | `message`: a `Str` as-is, `Bytes` as lossy UTF-8, anything else as its JSON text | -- |
//! | attribute `status`, else `severity` | `status` (`severity.as_str()`), omitted when neither | -- |
//! | `Event::timestamp` | `timestamp`, integer milliseconds | -- |
//! | `host.name` other than an empty `Str`, else, unless the log carries `syslog.hostname` or `host` (Datadog's own reserved host attributes, which its log preprocessing reads and which leave as attributes; `syslog_in` writes neither), the default host | `hostname`, omitted when none; `host.name` itself is never an attribute | -- |
//! | a `hostname` attribute | omitted: it would collide with the wire's own | `logit.output.tags.omitted{reason="reserved_key"}` |
//! | `service.name` other than an empty `Str`, else a `service` attribute (a decoded `service` that isn't a string) | `service`, omitted when neither; `service.name` itself is never an attribute | -- |
//! | a `service` attribute beside a `service.name` written | omitted: it would be a second `service` | `logit.output.tags.omitted{reason="reserved_key"}` |
//! | `ddsource`, else [`ATTR_SOURCE`] | `ddsource`, omitted when neither; an operator who wants one on every log sets `ddsource` upstream with `set` | -- |
//! | `ddtags` | `ddtags`, verbatim, but for the next row | -- |
//! | `deployment.environment.name`, `service.version`, [`ATTR_CONTAINER_ID`], each a non-empty `Str`, beside a text `ddtags` or none | `env:<v>`, `version:<v>`, and `container_id:<id>` (the tag the Agent gives a container's logs) appended to `ddtags` in that order, after its other entries; an entry already in the string naming one of the three tags written is left off. Beside a `ddtags` that isn't text, each key leaves as an attribute | the left-off entry: `logit.output.tags.omitted{reason="reserved_key"}` |
//! | every other attribute | a top-level key: `Map`/`Array` nested, `Bytes` → base64, `Timestamp` → RFC 3339, non-finite `F64` → `null` | -- |
//! | an attribute named `message` or `timestamp` | omitted: it would collide with the wire's own | `logit.output.tags.omitted{reason="reserved_key"}` |
//! | `LogRecord::trace`, when no `trace_id` or `span_id` attribute is present | `trace_id` (32 lowercase hex) and, when the ref has one, `span_id` (16 lowercase hex), after the attributes: the OTel form Datadog's log intake detects. An attribute of either name suppresses both, so a relayed intake log re-encodes unchanged, and the log's own ids are left off | the suppressed ids, once per log: `logit.output.tags.omitted{reason="reserved_key"}` |
//! | **events** ([`DatadogEncoder::encode_events`]): an event with a `log` carrying `_logit.statsd.event.title` | [`events::EventFormat::AgentEnvelope`] (the default): one envelope for the batch, `apiKey` `""`, groups keyed by `_logit.statsd.event.source_type` (else `api`) in sorted order; [`events::EventFormat::PublicV1`]: one object per event | -- |
//! | resource [`RESOURCE_ATTR_AGENT_HOSTNAME`] | envelope `internalHostname`, `""` when absent | -- |
//! | the event fields | Agent item keys in the Agent's order: `msg_title`, `msg_text`, `timestamp` (seconds), `priority`, `host`, `tags`, `alert_type`, `aggregation_key`, `source_type_name`, `event_type`, then `device_name`, `related_event_id`; the public form uses `title`, `text`, `date_happened` and omits an absent `host`. Empty optional fields are omitted | -- |
//! | `host.name` other than an empty `Str`, else [`ATTR_HOSTLESS`] (an empty DogStatsD `host:` tag), never filled, else the default host | `host`. Hostless: `""` in the Agent form, which `datadog_in` reads back as the mark, and omitted in the public form, which has no hostless spelling. With no host, no mark, and no default host: omitted in both. `host.name`, empty or not, is never a tag; a `host` is an ordinary tag | -- |
//! | no `_logit.statsd.event.alert_type` | `alert_type` from `severity`: `Error`/`Fatal` → `error`, `Warn` → `warning`, `Info` → `info`, else omitted | -- |
//! | **service checks** ([`DatadogEncoder::encode_service_checks`]): an event carrying `_logit.statsd.service_check.name` whose first metric is a `Gauge` | one array item with every key: `check`, `host_name` (below), `timestamp` (seconds), `status`, `message` (`""`), `tags` (`[]`) | -- |
//! | `host.name` other than an empty `Str`, else [`ATTR_HOSTLESS`] (an empty DogStatsD `host:` tag), never filled, else the default host | `host_name`: `""` when hostless, which `datadog_in` reads back as the mark; omitted with no host, no mark, and no default host. `host.name`, empty or not, is never a tag; a `host` is an ordinary tag | -- |
//! | `_logit.statsd.service_check.status` `U64` 0 to 3, else the gauge when it is an integer 0 to 3 | `status` | otherwise dropped: `logit.output.metrics.dropped{reason="invalid_status"}` |
//! | every attribute not consumed above (events and checks) | `tags`, by [`tags::render_tags`]; a private `_logit.` key is never rendered; [`ATTR_CONTAINER_ID`] is the `container_id` tag and [`ATTR_CONTAINER_IMAGE_TAGS`]' first element the `image_tag` tag, and `deployment.environment.name`, `service.version`, and `service.name` the `env`, `version`, and `service` tags, as on a metric | `logit.output.tags.omitted{reason="unrepresentable"}` for a `Map`/`Bytes`/`Null`, a further `container.image.tags` element, a `container_id` or `image_tag` attribute beside its key, and a valued `env`, `version`, or `service` attribute beside its key |
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
//! 5. a log with `_logit.datadog.source` but no `ddsource` leaves under the Datadog name, and
//!    one with a top-level `service.name` field leaves with it as its `service`, a top-level
//!    `deployment.environment.name` or `service.version` as an `env:` or `version:` entry in
//!    `ddtags` (the next hop reads back each key as it was, but a `service.name` that isn't a
//!    string, which it reads back as the attribute `service`); a log's, an event's, or a check's
//!    own host displaces a `host.name` key or tag beside it, and a log's own `service` a
//!    `service.name` key, omitted and counted at decode; a log's `hostname` that isn't a string
//!    is decoded as the attribute `hostname` and left off at encode, counted, so the relay loses
//!    it on the first hop (a `service` that isn't a string relays as the field);
//! 6. tags reorder by attribute order, and an exact duplicate tag is dropped. On an event or a
//!    service check, a `deployment.environment.name:`, `service.version:`, or `service.name:` tag
//!    beside its `env:`, `version:`, or `service:` tag is omitted at decode, counted, and the
//!    Datadog-spelled tag relays;
//! 7. a log with no `hostname` (or an empty one) and none of `host.name`, `syslog.hostname`, or
//!    `host`, and an event or service check with no host of its own, leaves with the relay's
//!    process hostname (`datadog_out`'s default host); an event's empty `host` or a check's empty
//!    `host_name` is hostless and stays so, though a public v1 event leaves with no `host`, which
//!    a later hop fills;
//! 8. an event or service check with no host of its own takes a `host.name:<h>` tag as its host,
//!    which leaves as the host and not a tag.
//! 9. an event's or check's tag whose key is under the private `_logit.` prefix is omitted and
//!    counted.
//! 10. a log's `ddtags` `env:`, `version:`, and `service:` entries are lifted at decode (the
//!     decode table above): `env:` and `version:` rejoin at the end of `ddtags`, in that order
//!     and before a `container_id:` the relay writes, and `service:` leaves as the `service`
//!     field, or not at all beside a `service` field of the log's own. Every other entry naming
//!     a tag with a winner (a repeat, an empty or bare one) is left off, counted, and a
//!     rewritten string has no empty tokens; an empty or bare entry stays only when neither a
//!     valued entry, the log's `service`, nor a field of the key's name gives its key a value. A
//!     `ddtags` emptied by the lift leaves with no `ddtags`, and an empty `service` with no
//!     `service`.
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
//! | `meta` | attributes verbatim (`Str`), `span.kind` and `_dd.*` included, except `env` and `version` | -- |
//! | `meta["env"]`, `meta["version"]` | [`ATTR_DEPLOYMENT_ENVIRONMENT_NAME`], [`ATTR_SERVICE_VERSION`] (`Str`): the span's own environment and version, which the Agent reads into an empty `TracerPayload` `env` and `app_version` | -- |
//! | a `meta`, `metrics`, or `meta_struct` key spelled `deployment.environment.name` or `service.version` beside that key's `meta` name (`env`, `version`) | dropped: the `meta` entry is the key. With no `meta` name beside it, it lands verbatim, which is the key | `logit.input.tags.omitted{reason="reserved_key"}` |
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
//! | `TracerPayload` `hostname` | batch resource [`ATTR_HOST_NAME`], when non-empty; empty is no host (an Agent leaves it empty when neither the tracer's payload nor its root span's `_dd.hostname` named one, and sends it beside its own `hostName`). v0.3 to v0.5 have no such field, and a root span's `_dd.hostname` stays a span attribute | -- |
//! | `TracerPayload` `container_id` | batch resource [`ATTR_CONTAINER_ID`], when non-empty | -- |
//! | `TracerPayload` `env`, `app_version` | batch resource [`ATTR_DEPLOYMENT_ENVIRONMENT_NAME`], [`ATTR_SERVICE_VERSION`], each when non-empty | -- |
//! | other `TracerPayload` fields | batch resource `_logit.datadog.tracer.language_name`, `.language_version`, `.tracer_version`, `.runtime_id` (`Str`), `.tags` (`Map`), each when non-empty; `.container_debug` (`Map` of its non-zero fields) whenever present | -- |
//! | tracer API request headers (read by `datadog_trace_in`, not by these decoders): `Datadog-Meta-Lang`, `-Lang-Version`, `-Tracer-Version`, `Datadog-Container-ID` | batch resource `_logit.datadog.tracer.language_name`, `.language_version`, `.tracer_version`, [`ATTR_CONTAINER_ID`] (`Str`), each when non-empty; on v0.7 only where the `TracerPayload` left the field empty (the Agent's v0.7 receiver reads no header and keeps the payload's own, `pkg/trace/api/api.go` `decodeTracerPayload`) | -- |
//! | `Datadog-Meta-Lang-Interpreter`, `-Lang-Interpreter-Vendor`, `Datadog-Entity-ID`, `Datadog-External-Env` | [`RESOURCE_ATTR_TRACER_LANGUAGE_INTERPRETER`], [`RESOURCE_ATTR_TRACER_LANGUAGE_INTERPRETER_VENDOR`], [`RESOURCE_ATTR_TRACER_ENTITY_ID`], [`RESOURCE_ATTR_TRACER_EXTERNAL_ENV`] (`Str`), each when non-empty | -- |
//! | `Datadog-Client-Computed-Top-Level` (any non-empty value); `Datadog-Client-Computed-Stats` (any non-empty value but a Go `false` spelling) | [`RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_TOP_LEVEL`], [`RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_STATS`] = `Bool(true)`; absent otherwise | -- |
//! | `Datadog-Client-Dropped-P0-Traces`, `-Spans` | [`RESOURCE_ATTR_TRACER_DROPPED_P0_TRACES`], [`RESOURCE_ATTR_TRACER_DROPPED_P0_SPANS`] (`U64`); a value that isn't an unsigned integer is left out | diag `bad_header` |
//! | `AgentPayload` fields | every batch's resource: [`RESOURCE_ATTR_AGENT_HOSTNAME`], `_logit.datadog.agent.env`, [`RESOURCE_ATTR_AGENT_VERSION`] (`Str`), `.target_tps`, `.error_tps` (`F64`, nonzero), `.rare_sampler_enabled` (`true`), `.tags` (`Map`) | -- |
//! | `AgentPayload.idxTracerPayloads` (v1.0) | dropped: not implemented | `logit.input.spans.dropped{reason="idx_payload"}`, one per payload |
//! | a span (v0.5: wrong arity, a dictionary index out of range), trace array, or chunk that doesn't parse | dropped; the rest decodes | `dropped{reason="malformed"}` + diag `malformed_span` |
//! | a body whose structure doesn't parse (not the top-level shape, truncated, bad protobuf) | -- | `CodecError::Malformed` |
//!
//! ## Encode: events → spans
//!
//! Every event with a `span`, grouped into one chunk per 128-bit trace id in first-appearance
//! order, spans in batch order within it. Attributes are the resource merged with the event's
//! (event wins); the carriers below are consumed, only when of the type decode gives them (a
//! non-empty `Str` for the string ones). A public carrier of another type (`service.name`,
//! `resource.name`, `span.type`) goes out by the typing rules like any attribute; a
//! `_logit.datadog.*` carrier of another type is skipped uncounted, like any private key. Every
//! map is sorted by key.
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
//! | `_logit.datadog.chunk.*` of a chunk's first span | v0.7/`AgentPayload` chunk `priority` (`-128` when absent), `origin`, `dropped_trace`, `tags` | -- |
//! | batch resource `_logit.datadog.tracer.*` / `_logit.datadog.agent.*` | v0.7: one `TracerPayload`; `AgentPayload`: one payload around one `TracerPayload` | -- |
//! | batch resource [`ATTR_HOST_NAME`], a non-empty `Str` | v0.7 and `AgentPayload`: the `TracerPayload`'s `hostname`, not a span tag; v0.4 and v0.5, which have no such field: a `meta` entry on every span, as any resource attribute, unless the span carries its own `host.name`, which wins as any event attribute does. An event's own `host.name` is always a `meta` entry | v0.4/v0.5, a span whose own `host.name` differs: `degraded{reason="no_wire_form"}`, once per span |
//! | [`ATTR_DEPLOYMENT_ENVIRONMENT_NAME`], [`ATTR_SERVICE_VERSION`] whose value lands in `meta` (an event's; v0.4 and v0.5, also the resource's) | `meta["env"]`, `meta["version"]`, displacing a bare `env` or `version` attribute from `meta`. In `metrics` or `meta_struct`, a value keeps the key's own name | the displaced attribute: `logit.output.tags.omitted{reason="unrepresentable"}` |
//! | batch resource [`ATTR_DEPLOYMENT_ENVIRONMENT_NAME`], [`ATTR_SERVICE_VERSION`], a non-empty `Str` | v0.7 and `AgentPayload`: the `TracerPayload`'s `env` and `app_version`, not span tags; v0.4 and v0.5: every span's `meta["env"]` and `meta["version"]`, as [`ATTR_HOST_NAME`] rides, unless the span carries its own | as for [`ATTR_HOST_NAME`] |
//! | batch resource [`ATTR_CONTAINER_ID`], a non-empty `Str` | v0.7 and `AgentPayload`: the `TracerPayload`'s `container_id`; v0.4 and v0.5: the `Datadog-Container-ID` header under [`DatadogEncoder::encode_tracer_api_traces`] (`datadog_trace_out`), else dropped. Never a span tag. An event's own `container.id`, or one of another type, is an ordinary span attribute | v0.4/v0.5 with no header: `degraded{reason="no_wire_form"}`, per body |
//! | no resource [`ATTR_HOST_NAME`] | `AgentPayload`: the default host ([`DatadogEncoder::with_default_host`]) as `hostname` when the resource has no [`RESOURCE_ATTR_AGENT_HOSTNAME`] either; with one, `""`, as that Agent sent it. v0.7: `""`, which the Agent behind `datadog_trace_out` fills from the root span's `_dd.hostname` as for any tracer | -- |
//! | a carrier the form has no field for: `_logit.datadog.chunk.*` in v0.4/v0.5 (per span), `_logit.datadog.tracer.*` in v0.4/v0.5 and `_logit.datadog.agent.*` below `AgentPayload` (per body), `meta_struct`/links/events in v0.5 | dropped | `degraded{reason="no_wire_form"}`, one per item |
//! | a tracer header's carrier ([`TRACER_STR_HEADERS`], [`TRACER_FLAG_HEADERS`], [`TRACER_U64_HEADERS`]) on the batch resource | never a span tag: a payload field where the form has one (v0.7's `language_name`, say), else dropped | `no_wire_form`, per body, except under [`DatadogEncoder::encode_tracer_api_traces`], whose caller sends each as its request header |
//! | `status: Ok`, span `flags`, `SpanExt` status message / `trace_state` / dropped counts, a link's or event's dropped-attribute count | dropped: Datadog has no field | `degraded{reason="no_wire_form"}`, one per field |
//!
//! "Per body" is once per encode call: a sender that cuts one batch into several requests, as
//! `datadog_trace_out` does past 1,000 traces, counts a batch-resource carrier once per request.
//! The same holds for the batch resource's `json_text` in a `TracerPayload` or `AgentPayload`,
//! and for the stats encoders' resource counts below.
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
//! 3. every map is sorted by key; a key in two of `meta`/`metrics`/`meta_struct` keeps one value,
//!    except `env` and `version`, whose `meta` entry is a common key and keeps both;
//! 4. v0.5's dictionary is rebuilt in first-use order, `""` at index 0;
//! 5. a negative `duration` leaves as 0; a zero-valued or absent field leaves as the Agent writes
//!    it (`priority` `-128` in no chunk, `nil` as the zero value);
//! 6. a 128-bit trace id with no `_dd.p.tid` gains one on its chunk's first span;
//! 7. across forms, a field the target has no home for is dropped and counted `no_wire_form`;
//!    a resource `host.name`, `deployment.environment.name`, or `service.version` going to v0.4
//!    or v0.5 lands in every span's `meta` instead (as `host.name`, `env`, or `version`), unless
//!    the span carries a different value of its own, which wins as any event attribute does,
//!    counted;
//! 8. `datadog_out` only: a `TracerPayload` with no `hostname` and no `AgentPayload` `hostName`
//!    leaves with the process hostname (`datadog_out`'s default host) as its `hostname`;
//! 9. a span `meta` key spelled `deployment.environment.name` or `service.version` leaves as
//!    `env` or `version`, and one beside that `meta` name is dropped, counted.
//!
//! ## Readiness for the intake
//!
//! The intake's `/api/v0.2/traces` expects what an Agent sends: spans normalized and marked, with
//! the stats the Agent computed sent beside them. [`trace_readiness`] decides per trace chunk, from
//! the data rather than the source component (ADR decision 2): a chunk whose root carries
//! [`METRIC_TOP_LEVEL`] is [`TraceReadiness::Ready`]; one whose root has other Datadog span
//! attributes ([`is_datadog_span`]) but no `_top_level` is raw tracer output
//! ([`TraceReadiness::NeedsAgentProcessing`]); anything else is
//! [`TraceReadiness::NotDatadogOrigin`]. Senders count the latter two under
//! [`TraceReadiness::drop_reason`].
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
//! | an intake `Stats` element that isn't a well-formed `ClientStatsPayload` | dropped; the other payloads decode | `logit.input.stats.dropped{reason="malformed_payload"}` + diag `malformed_stats` |
//! | a bucket, or a group, with a field of the wrong type (or that isn't a map) | dropped; the rest decodes | `dropped{reason="malformed_bucket"\|"malformed_group"}` + diag `malformed_stats` |
//! | an unknown key; a `nil` value | skipped; the field's zero value (msgp's own rules) | -- |
//! | one `ClientGroupedStats` of a bucket | one `Event::metric`; `timestamp` = the bucket's `Start` (ns) | a `Start` above `i64::MAX`: clamped, `logit.input.stats.degraded{reason="bucket_start_overflow"}` |
//! | a bucket with no groups | nothing | `logit.input.stats.skipped{reason="empty_bucket"}` |
//! | `Hits`, `Errors`, `TopLevelHits` | `datadog.stats.hits`, `.errors`, `.top_level_hits`: delta monotonic `Sum` of the `uint64` as `U64`, exact, always present | -- |
//! | `Duration` | `datadog.stats.duration`: the same, unit `ns` | the same |
//! | `OkSummary`, `ErrorSummary` | `datadog.stats.ok_summary`, `.error_summary`: `Distribution` under `Mapping::logarithmic(gamma, indexOffset, 2048)`, sparse `binCounts` and `contiguousBinCounts` both read (a key in both sums), `zeroCount`; the summary (`count`/`min`/`max`/`sum`) derived from the bins; absent when the wire bytes are empty | -- |
//! | a summary whose `interpolation` isn't `NONE` | that record dropped; its keys aren't the logarithmic mapping's, and decoding them isn't implemented | `logit.input.stats.dropped{reason="interpolation"}` |
//! | a summary that isn't a DDSketch, has no mapping, or has a `gamma`/`indexOffset` no mapping can use | that record dropped | `dropped{reason="bad_sketch"}` + diag `bad_stats_sketch` |
//! | `Service`, `Resource`, `Type`, `SpanKind` | `service.name`, `resource.name`, `span.type`, `span.kind` (ADR decision 8), when non-empty | -- |
//! | `Name` | [`stats::ATTR_STATS_NAME`], always, even empty: it is what marks the event as APM stats | -- |
//! | (the group as a whole) | the flag `_logit.metrics.closed: "bucketed"`, always, so `aggregate` forwards the group unmerged (`docs/adr/metric-flags.md`, "The flags") | -- |
//! | `DBType`, `GRPCStatusCode`, `HTTPMethod`, `HTTPEndpoint`, `srv_src` | `_logit.datadog.stats.db_type`, `.grpc_status_code`, `.http_method`, `.http_endpoint`, `.service_source`, when non-empty | -- |
//! | `HTTPStatusCode` | `_logit.datadog.stats.http_status_code` (`U64`), when nonzero | -- |
//! | `Synthetics` | `_logit.datadog.stats.synthetics` = `Bool(true)`, when true | -- |
//! | `IsTraceRoot` `1` / `2` / `0` | `_logit.datadog.stats.is_trace_root` `"true"` / `"false"` / absent | any other value: absent, `degraded{reason="unknown_trilean"}` |
//! | `PeerTags`, `AdditionalMetricTags`, `SpanDerivedPrimaryTags` | `_logit.datadog.stats.peer_tags`, `.additional_metric_tags`, `.span_derived_primary_tags`: `Array` of `Str`, when non-empty | -- |
//! | bucket `Duration`, `AgentTimeShift` | each of its groups' `_logit.datadog.stats.bucket.duration` (`U64`, always) and `.bucket.agent_time_shift` (`I64`, when nonzero) | -- |
//! | `ClientStatsPayload` `Hostname` | batch resource [`ATTR_HOST_NAME`], when non-empty, as a `TracerPayload`'s `hostname` | -- |
//! | `ClientStatsPayload` `ContainerID` | batch resource [`ATTR_CONTAINER_ID`], when non-empty, as a `TracerPayload`'s `container_id` | -- |
//! | `ClientStatsPayload` `Env`, `Version`, `Service` | batch resource [`ATTR_DEPLOYMENT_ENVIRONMENT_NAME`], [`ATTR_SERVICE_VERSION`], [`ATTR_SERVICE_NAME`], when non-empty, as a `TracerPayload`'s `env` and `app_version` | -- |
//! | `ClientStatsPayload` `Lang`, `TracerVersion`, `RuntimeID` | batch resource `_logit.datadog.tracer.language_name`, `.tracer_version`, `.runtime_id`, when non-empty | -- |
//! | `/v0.6/stats` request headers (read by `datadog_trace_in`, not by these decoders): `Datadog-Meta-Lang`, `-Tracer-Version`, `Datadog-Container-ID` | batch resource `_logit.datadog.tracer.language_name`, `.tracer_version`, [`ATTR_CONTAINER_ID`] (`Str`), each when non-empty and only where the payload's `Lang`, `TracerVersion`, or `ContainerID` left it unset, as the Agent's stats receiver fills them | -- |
//! | `ImageTag` | resource [`ATTR_CONTAINER_IMAGE_TAGS`], an `Array` of the one `Str`, when non-empty | -- |
//! | `AgentAggregation`, `GitCommitSha`, `ProcessTags` | resource `_logit.datadog.stats.agent_aggregation`, `.git_commit_sha`, `.process_tags`, when non-empty | -- |
//! | `Sequence`, `ProcessTagsHash`; `Tags` | resource `_logit.datadog.stats.sequence`, `.process_tags_hash` (`U64`), when nonzero; `_logit.datadog.stats.tags` (`Array` of `Str`), when non-empty | -- |
//! | `StatsPayload` `AgentHostname`, `AgentEnv`, `AgentVersion` | every batch's resource [`RESOURCE_ATTR_AGENT_HOSTNAME`], `_logit.datadog.agent.env`, [`RESOURCE_ATTR_AGENT_VERSION`], when non-empty | -- |
//! | `ClientComputed`, `SplitPayload` | every batch's resource `_logit.datadog.stats.client_computed`, `.split_payload` = `Bool(true)`, when true | -- |
//!
//! ## Encode: events → stats
//!
//! The stats routes send every event [`is_datadog_stats`] accepts and nothing else, uncounted, as
//! ordinary fan-out; every other route skips such an event whole. Buckets are rebuilt by grouping
//! on `(Event::timestamp, _logit.datadog.stats.bucket.duration,
//! _logit.datadog.stats.bucket.agent_time_shift)`, in first-seen order; every map writes every key,
//! in the Go struct's field order, as msgp's `EncodeMsg` does. `encode_stats_payload` wraps the
//! batch's one `ClientStatsPayload`.
//!
//! | Model | Wire | Counter |
//! |---|---|---|
//! | a group attribute (the event's, else the resource's; `service.name` the event's alone, since the resource's is the payload's `Service`) | its field above; `""`, `0`, `false`, or `[]` when absent | a value of the wrong type: `logit.output.tags.omitted{reason="unrepresentable"}` |
//! | [`ATTR_CONTAINER_IMAGE_TAGS`] (the resource's only) | `ImageTag`: the array's first element, or a lone `Str`; `""` when absent | each further element: `tags.omitted{reason="unrepresentable"}` |
//! | a payload attribute (the resource's only), [`ATTR_HOST_NAME`], [`ATTR_CONTAINER_ID`], [`ATTR_DEPLOYMENT_ENVIRONMENT_NAME`], [`ATTR_SERVICE_VERSION`], and [`ATTR_SERVICE_NAME`] included | its `ClientStatsPayload` field above; `""` or `0` when absent, except that `/api/v0.2/stats` writes the default host ([`DatadogEncoder::with_default_host`]) as `Hostname` when the resource has neither [`ATTR_HOST_NAME`] nor [`RESOURCE_ATTR_AGENT_HOSTNAME`] | the same; an event's `host.name` has no wire form, counted as below |
//! | any other event attribute; a resource attribute that is none of the fields above | omitted | `tags.omitted{reason="no_wire_form"}`, one per attribute (a resource's once per body); none for a private `_logit.` key, so the v0.6 route, which has no envelope, skips `_logit.datadog.agent.*` and the two envelope flags uncounted |
//! | a delta `Sum` hits/errors/top-level-hits/duration | `uint64`: a `U64` or non-negative `I64` as is, a `Bool` as `1` or `0`, an `F64` rounded to the nearest integer | a `Bool`: `logit.output.metrics.normalized{reason="bool_value"}`; a negative `I64`: `0`, `degraded{reason="bad_count"}`; a fraction: `logit.output.stats.degraded{reason="fractional_count"}`; negative or non-finite: `0`, `degraded{reason="bad_count"}`; above 2^64: `u64::MAX`, `degraded{reason="count_overflow"}` |
//! | an ok/error summary `Distribution` under a logarithmic mapping | a DDSketch protobuf: its `gamma` and `indexOffset`, `interpolation: NONE`, both stores as sparse `binCounts` in ascending key order, `zeroCount`. Hand-encoded, since prost's `HashMap` would order the bins at random | a bin limit other than 2048 (a receiver collapses at 2048): `degraded{reason="bin_limit"}`; a summary tracked from observations (`stats_exact`), which the protobuf has no field for: `degraded{reason="exact_summary"}` |
//! | the same under `Mapping::agent` | `gamma = 1.015625`, `indexOffset = bias + 0.5`, keys unchanged: Datadog's own conversion, reading the Agent's round-half-to-even key as the logarithmic floor. Only a value on an exact tie keys differently, and the exact summary is lost | `degraded{reason="agent_mapping"}` |
//! | a record of another name, or a known name of another kind (a cumulative `Sum`, say) | dropped | `logit.output.stats.dropped{reason="unrecognized_record"}` |
//! | a negative `Event::timestamp` | bucket `Start` `0` | `degraded{reason="negative_timestamp"}`, once per bucket of a body |
//!
//! ## Permitted normalizations (APM stats)
//!
//! `datadog_in -> datadog_out` on one stats route, and `datadog_trace_in -> datadog_trace_out` on
//! `/v0.6/stats`, is a fixed point modulo this list (`tests/datadog_stats_fixed_point.rs` pins
//! items 1-4):
//!
//! 1. every map key is written, in Go field order, zero values included: a tracer's omitted key or
//!    `nil` leaves as the explicit zero value, and an unknown key is dropped;
//! 2. a DDSketch's contiguous bins leave as sparse ones (a key in both summed), and a store over
//!    2048 bins collapses from the lowest key on decode;
//! 3. a bucket with no groups is dropped, and buckets sharing a start, duration, and time shift
//!    merge, their groups in first-seen order;
//! 4. an intake `StatsPayload` leaves as one `StatsPayload` per `ClientStatsPayload` (batching),
//!    its envelope restated on each;
//! 5. on `/v0.6/stats`, a `Lang`, `TracerVersion`, or `ContainerID` the payload left empty leaves
//!    in the payload when the request's matching header carried it, as it does behind an Agent;
//! 6. `datadog_out` only: a `ClientStatsPayload` with no `Hostname` and no `StatsPayload`
//!    `AgentHostname` leaves with the process hostname as its `Hostname`.

pub mod generated;
pub mod tags;
pub mod time;
pub mod units;

mod outcomes;

pub mod series;
pub mod sketches;

pub mod events;
pub mod logs;
pub mod service_checks;

pub mod traces;
pub mod traces_msgpack;
pub mod traces_proto;

pub mod stats;

use logit_core::interner::{intern, resolve};
use logit_core::{
    AttrMap, Diagnostics, Event, EventBatch, MetricKind, Resource, Symbol, Telemetry, Value,
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

/// `host.name`: the host of every Datadog record, as an event attribute.
pub const ATTR_HOST_NAME: &str = logit_core::attrs::HOST_NAME;
/// `container.id`: a tracer's container (a `TracerPayload`'s or a `ClientStatsPayload`'s
/// `container_id`, or the `Datadog-Container-ID` header), as a batch resource attribute; on a
/// metric, an event, or a service check, Datadog's `container_id` tag.
pub const ATTR_CONTAINER_ID: &str = logit_core::attrs::CONTAINER_ID;
/// `container.image.tags`: an `Array` of `Str` whose first element is a `ClientStatsPayload`'s
/// `ImageTag`, and on a metric, an event, or a service check Datadog's `image_tag` tag.
pub const ATTR_CONTAINER_IMAGE_TAGS: &str = logit_core::attrs::CONTAINER_IMAGE_TAGS;
/// `deployment.environment.name`: Datadog's `env`. A span's `env` meta and a log's `ddtags`
/// `env:` entry on the event, a `TracerPayload`'s `env` or a `ClientStatsPayload`'s `Env` on the
/// batch resource.
pub const ATTR_DEPLOYMENT_ENVIRONMENT_NAME: &str = logit_core::attrs::DEPLOYMENT_ENVIRONMENT_NAME;
/// `service.version`: Datadog's `version`. A span's `version` meta and a log's `ddtags`
/// `version:` entry on the event, a `TracerPayload`'s `app_version` or a `ClientStatsPayload`'s
/// `Version` on the batch resource.
pub const ATTR_SERVICE_VERSION: &str = logit_core::attrs::SERVICE_VERSION;

/// Writes a record's own wire host onto `attributes`: a non-empty one as [`ATTR_HOST_NAME`], an
/// empty one as [`ATTR_HOSTLESS`], `None` as neither. A host either way displaces a `host.name`
/// the record's tags or keys spelled, which it removes; returns how many wire tags that was, for
/// the caller to count as `logit.input.tags.omitted{reason="reserved_key"}`.
pub(crate) fn insert_wire_host(attributes: &mut AttrMap, host: Option<&str>) -> usize {
    let Some(host) = host else { return 0 };
    let displaced = match attributes.remove(ATTR_HOST_NAME) {
        None => 0,
        Some(Value::Array(elements)) => elements.len(),
        Some(_) => 1,
    };
    if host.is_empty() {
        attributes.insert(ATTR_HOSTLESS, Value::Bool(true));
    } else {
        attributes.insert(ATTR_HOST_NAME, Value::str(host));
    }
    displaced
}

/// `_logit.datadog.units`: `"datadog"` on the resource of every series batch `datadog_in` decodes
/// (v1, v2 JSON, v2 protobuf), so `datadog_out` writes its units as they are rather than mapping
/// them as UCUM ([`units`]): they're Datadog unit names already. It's on every one, unit or not,
/// because `aggregate` and `cumulative_to_delta` key a series on its resource. It marks the whole
/// batch, so a unit a later stage sets on one of its records leaves unmapped too. Distribution
/// points and sketches carry no unit and no mark.
pub const ATTR_UNITS: &str = "_logit.datadog.units";
/// [`ATTR_UNITS`]'s one value.
pub const UNITS_DATADOG: &str = "datadog";

static UNITS_KEY: LazyLock<Symbol> = LazyLock::new(|| intern(ATTR_UNITS));

/// Whether an event's units are Datadog unit names ([`ATTR_UNITS`]), the event's attribute over
/// the resource's.
pub(crate) fn units_are_datadog(resource: &Resource, event: &Event) -> bool {
    let key = *UNITS_KEY;
    let value = event.attributes.get_sym(key).or_else(|| resource.attributes.get_sym(key));
    value.and_then(Value::as_str) == Some(UNITS_DATADOG)
}

/// `_logit.datadog.type`: `unspecified`, the one series type with no neutral meaning; absent for
/// `count` and `gauge`, whose model kinds say so themselves, and for `rate`, which is the neutral
/// flag [`METRICS_TYPE`](logit_core::attrs::METRICS_TYPE).
pub const ATTR_TYPE: &str = "_logit.datadog.type";
/// `_logit.datadog.source_type_name`: the check or integration that produced a series.
pub const ATTR_SOURCE_TYPE_NAME: &str = "_logit.datadog.source_type_name";
/// `_logit.datadog.device`: the v1 series `device` field.
pub const ATTR_DEVICE: &str = "_logit.datadog.device";
/// `_logit.datadog.resources`: every series resource whose `type` isn't `host`, as an `Array` of
/// `Map{type, name}`.
pub const ATTR_RESOURCES: &str = "_logit.datadog.resources";
/// `_logit.datadog.hostless`: `Bool(true)` on a v1 series, distribution point, or sketch whose
/// `host` arrived present and empty, the Agent's spelling of a sample it marked hostless. The
/// encoders then send `""` and never fill it with the default host.
pub const ATTR_HOSTLESS: &str = "_logit.datadog.hostless";
/// `_logit.datadog.origin.product` / `.category` / `.service`: the protobuf `Origin` codes (U64).
pub const ATTR_ORIGIN_PRODUCT: &str = "_logit.datadog.origin.product";
pub const ATTR_ORIGIN_CATEGORY: &str = "_logit.datadog.origin.category";
pub const ATTR_ORIGIN_SERVICE: &str = "_logit.datadog.origin.service";
/// `_logit.datadog.origin.metric_type`: the JSON v2 API's extra origin code, which the protobuf
/// lacks.
pub const ATTR_ORIGIN_METRIC_TYPE: &str = "_logit.datadog.origin.metric_type";
/// `_logit.datadog.event_type`: the Agent-only event field.
pub const ATTR_EVENT_TYPE: &str = "_logit.datadog.event_type";
/// `_logit.datadog.event.device_name` / `.related_event_id`: the public events API's extra fields.
pub const ATTR_EVENT_DEVICE_NAME: &str = "_logit.datadog.event.device_name";
pub const ATTR_EVENT_RELATED_EVENT_ID: &str = "_logit.datadog.event.related_event_id";
/// `_logit.datadog.source`: a log's `ddsource` when it arrives from elsewhere than a Datadog log
/// payload (an upstream `set`); a decoded Datadog log keeps `ddsource` verbatim instead.
pub const ATTR_SOURCE: &str = "_logit.datadog.source";
/// `_logit.datadog.agent.hostname`: an events envelope's `internalHostname`, as a resource
/// attribute.
pub const RESOURCE_ATTR_AGENT_HOSTNAME: &str = "_logit.datadog.agent.hostname";
/// `_logit.datadog.agent.version` / `.timezone` / `.epoch` / `.internal_ip` / `.public_ip`: a
/// sketch payload's `CommonMetadata`, as resource attributes (`epoch` an `F64`, the rest `Str`).
pub const RESOURCE_ATTR_AGENT_VERSION: &str = "_logit.datadog.agent.version";
pub const RESOURCE_ATTR_AGENT_TIMEZONE: &str = "_logit.datadog.agent.timezone";
pub const RESOURCE_ATTR_AGENT_EPOCH: &str = "_logit.datadog.agent.epoch";
pub const RESOURCE_ATTR_AGENT_INTERNAL_IP: &str = "_logit.datadog.agent.internal_ip";
pub const RESOURCE_ATTR_AGENT_PUBLIC_IP: &str = "_logit.datadog.agent.public_ip";
/// `_logit.datadog.agent.env`: an `AgentPayload`'s or a stats `StatsPayload` envelope's `env`, as a
/// resource attribute. Shared by [`traces`] and [`stats`], so it lives here rather than in either.
pub const RESOURCE_ATTR_AGENT_ENV: &str = "_logit.datadog.agent.env";
/// `_logit.datadog.tracer.*`: a `TracerPayload`'s or a stats `ClientStatsPayload`'s fields, as
/// batch resource attributes. Shared by [`traces`] and [`stats`], so they live here rather than in
/// either; each module also has its own carriers with no counterpart in the other.
pub const RESOURCE_ATTR_TRACER_LANGUAGE_NAME: &str = "_logit.datadog.tracer.language_name";
pub const RESOURCE_ATTR_TRACER_VERSION: &str = "_logit.datadog.tracer.tracer_version";
pub const RESOURCE_ATTR_TRACER_RUNTIME_ID: &str = "_logit.datadog.tracer.runtime_id";
/// `_logit.datadog.tracer.language_interpreter` / `.language_interpreter_vendor` / `.entity_id` /
/// `.external_env`: a tracer's `Datadog-Meta-Lang-Interpreter`,
/// `Datadog-Meta-Lang-Interpreter-Vendor`, `Datadog-Entity-ID`, and `Datadog-External-Env` request
/// headers (`Str`), which no payload carries. `Datadog-External-Env` is the tracer's copy of the
/// `DD_EXTERNAL_ENV` a DogStatsD client sends as `|e:`, which the Agent's origin detection reads.
pub const RESOURCE_ATTR_TRACER_LANGUAGE_INTERPRETER: &str =
    "_logit.datadog.tracer.language_interpreter";
pub const RESOURCE_ATTR_TRACER_LANGUAGE_INTERPRETER_VENDOR: &str =
    "_logit.datadog.tracer.language_interpreter_vendor";
pub const RESOURCE_ATTR_TRACER_ENTITY_ID: &str = "_logit.datadog.tracer.entity_id";
pub const RESOURCE_ATTR_TRACER_EXTERNAL_ENV: &str = "_logit.datadog.tracer.external_env";
/// `_logit.datadog.tracer.client_computed_top_level` / `.client_computed_stats`: a tracer's
/// `Datadog-Client-Computed-Top-Level` and `Datadog-Client-Computed-Stats` request headers, as
/// `Bool(true)` when set and absent otherwise.
pub const RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_TOP_LEVEL: &str =
    "_logit.datadog.tracer.client_computed_top_level";
pub const RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_STATS: &str =
    "_logit.datadog.tracer.client_computed_stats";
/// `_logit.datadog.tracer.dropped_p0_traces` / `.dropped_p0_spans`: a tracer's
/// `Datadog-Client-Dropped-P0-Traces` and `-Spans` request headers (`U64`), the priority-0
/// traces and spans it dropped before sending.
pub const RESOURCE_ATTR_TRACER_DROPPED_P0_TRACES: &str = "_logit.datadog.tracer.dropped_p0_traces";
pub const RESOURCE_ATTR_TRACER_DROPPED_P0_SPANS: &str = "_logit.datadog.tracer.dropped_p0_spans";

/// The tracer API's request headers that carry `_logit.datadog.tracer.*` resource attributes,
/// spelled in lowercase as `http::HeaderName` stores them. `datadog_trace_in` reads them and
/// `datadog_trace_out` writes them back; the mapping is in this module's doc, under "Traces".
pub const HEADER_META_LANG: &str = "datadog-meta-lang";
pub const HEADER_META_LANG_VERSION: &str = "datadog-meta-lang-version";
pub const HEADER_META_LANG_INTERPRETER: &str = "datadog-meta-lang-interpreter";
pub const HEADER_META_LANG_INTERPRETER_VENDOR: &str = "datadog-meta-lang-interpreter-vendor";
pub const HEADER_META_TRACER_VERSION: &str = "datadog-meta-tracer-version";
pub const HEADER_CONTAINER_ID: &str = "datadog-container-id";
pub const HEADER_ENTITY_ID: &str = "datadog-entity-id";
pub const HEADER_EXTERNAL_ENV: &str = "datadog-external-env";
pub const HEADER_CLIENT_COMPUTED_TOP_LEVEL: &str = "datadog-client-computed-top-level";
pub const HEADER_CLIENT_COMPUTED_STATS: &str = "datadog-client-computed-stats";
pub const HEADER_CLIENT_DROPPED_P0_TRACES: &str = "datadog-client-dropped-p0-traces";
pub const HEADER_CLIENT_DROPPED_P0_SPANS: &str = "datadog-client-dropped-p0-spans";
/// `X-Datadog-Trace-Count`: how many traces the tracer says the body holds.
pub const HEADER_TRACE_COUNT: &str = "x-datadog-trace-count";

/// The tracer headers whose value is a `Str` resource attribute: header, then attribute.
/// `datadog_trace_in` reads them and `datadog_trace_out` writes them back.
pub const TRACER_STR_HEADERS: [(&str, &str); 8] = [
    (HEADER_META_LANG, RESOURCE_ATTR_TRACER_LANGUAGE_NAME),
    (HEADER_META_LANG_VERSION, traces::RESOURCE_ATTR_TRACER_LANGUAGE_VERSION),
    (HEADER_META_LANG_INTERPRETER, RESOURCE_ATTR_TRACER_LANGUAGE_INTERPRETER),
    (HEADER_META_LANG_INTERPRETER_VENDOR, RESOURCE_ATTR_TRACER_LANGUAGE_INTERPRETER_VENDOR),
    (HEADER_META_TRACER_VERSION, RESOURCE_ATTR_TRACER_VERSION),
    (HEADER_CONTAINER_ID, ATTR_CONTAINER_ID),
    (HEADER_ENTITY_ID, RESOURCE_ATTR_TRACER_ENTITY_ID),
    (HEADER_EXTERNAL_ENV, RESOURCE_ATTR_TRACER_EXTERNAL_ENV),
];

/// The tracer headers whose resource attribute is `Bool(true)` when set and absent otherwise.
pub const TRACER_FLAG_HEADERS: [(&str, &str); 2] = [
    (HEADER_CLIENT_COMPUTED_TOP_LEVEL, RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_TOP_LEVEL),
    (HEADER_CLIENT_COMPUTED_STATS, RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_STATS),
];

/// The tracer headers whose value is a `U64` resource attribute.
pub const TRACER_U64_HEADERS: [(&str, &str); 2] = [
    (HEADER_CLIENT_DROPPED_P0_TRACES, RESOURCE_ATTR_TRACER_DROPPED_P0_TRACES),
    (HEADER_CLIENT_DROPPED_P0_SPANS, RESOURCE_ATTR_TRACER_DROPPED_P0_SPANS),
];

/// Whether `key` is the resource attribute of one of the tracer headers
/// ([`TRACER_STR_HEADERS`], [`TRACER_FLAG_HEADERS`], [`TRACER_U64_HEADERS`]).
pub fn is_tracer_header_attr(key: &str) -> bool {
    TRACER_STR_HEADERS
        .iter()
        .chain(&TRACER_FLAG_HEADERS)
        .chain(&TRACER_U64_HEADERS)
        .any(|(_, attr)| *attr == key)
}
/// `service.name` / `resource.name` / `span.type` / `span.kind`: a span's `service`, `resource`,
/// `type`, and (a `meta` key, kept verbatim, that also sets `SpanRecord::kind`) `span.kind` — the
/// names Datadog's own OTLP receiver honors (ADR `datadog-agent-and-intake-relay` decision 8). An
/// APM stats group's `Service`/`Resource`/`Type`/`SpanKind` carry the same names, so these live
/// here rather than in [`traces`] or [`stats`] alone.
pub const ATTR_SERVICE_NAME: &str = logit_core::attrs::SERVICE_NAME;
pub const ATTR_RESOURCE_NAME: &str = "resource.name";
pub const ATTR_SPAN_TYPE: &str = "span.type";
pub const ATTR_SPAN_KIND: &str = "span.kind";

/// The attribute `key` with the event's value winning over the resource's, as
/// [`logit_core::attrs::merged`] resolves it, without the full merged walk.
fn merged_get<'a>(resource: &'a Resource, event: &'a Event, key: &str) -> Option<&'a Value> {
    event.attributes.get(key).or_else(|| resource.attributes.get(key))
}

/// A service check: `_logit.statsd.service_check.name` present and a `Gauge` first metric, the
/// shape [`DatadogEncoder::encode_service_checks`] sends. Its record 0 belongs to that route alone;
/// the metrics routes skip it and send any later records as ordinary metrics, as `statsd_out` does.
pub fn is_service_check(resource: &Resource, event: &Event) -> bool {
    matches!(event.metrics.first().map(|m| &m.kind), Some(MetricKind::Gauge(_)))
        && merged_get(resource, event, service_checks::ATTR_SERVICE_CHECK_NAME).is_some()
}

/// A Datadog (or DogStatsD) event: a `log` carrying `_logit.statsd.event.title`, the shape
/// [`DatadogEncoder::encode_events`] sends. The logs route skips it.
pub fn is_datadog_event(resource: &Resource, event: &Event) -> bool {
    event.log.is_some() && merged_get(resource, event, events::ATTR_EVENT_TITLE).is_some()
}

/// APM stats: `_logit.datadog.stats.name` present (the event's, else the resource's), the events
/// [`DatadogEncoder::encode_client_stats_v06`] and [`DatadogEncoder::encode_stats_payload`] send.
/// Every metrics route skips such an event whole: its records are a stats group's, not series.
/// A caller that tests every event of a batch resolves `stats_name` once with
/// [`datadog_stats_name`]; each test is then two binary searches with no interner probe.
pub fn is_datadog_stats(resource: &Resource, event: &Event, stats_name: Option<Symbol>) -> bool {
    stats_name.is_some_and(|name| {
        event.attributes.get_sym(name).is_some() || resource.attributes.get_sym(name).is_some()
    })
}

/// The interned [`stats::ATTR_STATS_NAME`] for [`is_datadog_stats`], `None` while no attribute of
/// that name has been interned, in which case no event carries it.
pub fn datadog_stats_name() -> Option<Symbol> {
    logit_core::interner::lookup(stats::ATTR_STATS_NAME)
}

/// `_top_level`: the span metric the Agent writes (`1`) on every root, orphan, and
/// service-boundary span (`traceutil.ComputeTopLevel`), decoded verbatim into an `F64` attribute.
/// [`is_agent_processed`] reads it on a chunk's root.
pub const METRIC_TOP_LEVEL: &str = "_top_level";

/// `_sampling_priority_v1`: the tracer's sampling decision, a span `metrics` entry decoded
/// verbatim. One of the marks [`is_datadog_span`] reads.
pub const METRIC_SAMPLING_PRIORITY: &str = "_sampling_priority_v1";

/// What a sender of the intake's `/api/v0.2/traces` does with one span, decided per trace chunk
/// by [`trace_readiness`] (ADR `datadog-agent-and-intake-relay` decision 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceReadiness {
    /// The chunk's root carries [`METRIC_TOP_LEVEL`]: an Agent, or a processor doing its job, has
    /// been through it, so it goes out natively.
    Ready,
    /// The root carries Datadog span attributes ([`is_datadog_span`]) but no `_top_level`: raw
    /// tracer output, with no Agent stats, normalization, or obfuscation behind it.
    NeedsAgentProcessing,
    /// The root carries no Datadog span attribute: an OTel or `logit`-made span, whose path to
    /// Datadog is `otlp_out`.
    NotDatadogOrigin,
}

impl TraceReadiness {
    /// The `reason` tag a sender counts an unsent span under; `None` for `Ready`.
    pub fn drop_reason(self) -> Option<&'static str> {
        match self {
            Self::Ready => None,
            Self::NeedsAgentProcessing => Some("needs_agent_processing"),
            Self::NotDatadogOrigin => Some("not_datadog_origin"),
        }
    }
}

/// Whether `root` (a chunk's root span) carries a nonzero numeric [`METRIC_TOP_LEVEL`]. Read from
/// the event's own attributes: `_top_level` is a per-span `metrics` entry, never a resource
/// attribute.
pub fn is_agent_processed(root: &Event) -> bool {
    match root.attributes.get(METRIC_TOP_LEVEL) {
        Some(Value::F64(f)) => *f != 0.0,
        Some(Value::I64(i)) => *i != 0,
        Some(Value::U64(u)) => *u != 0,
        _ => false,
    }
}

/// Whether `event`'s span carries an attribute only a Datadog span has: on the event,
/// `resource.name`, `span.type`, `_logit.datadog.span.error`, a `_logit.datadog.chunk.*` carrier,
/// [`METRIC_SAMPLING_PRIORITY`], [`METRIC_TOP_LEVEL`], or a key starting `_dd.` (the tracer's and
/// Agent's own `meta`/`metrics` namespace, `_dd.p.tid` and `_dd.p.dm` included); on the batch
/// resource, a `_logit.datadog.tracer.*` or `_logit.datadog.agent.*` carrier. `service.name` alone
/// doesn't count: every OTel resource carries it.
pub fn is_datadog_span(resource: &Resource, event: &Event) -> bool {
    const SPAN_KEYS: [&str; 9] = [
        ATTR_RESOURCE_NAME,
        ATTR_SPAN_TYPE,
        traces::ATTR_SPAN_ERROR,
        traces::ATTR_CHUNK_PRIORITY,
        traces::ATTR_CHUNK_ORIGIN,
        traces::ATTR_CHUNK_DROPPED_TRACE,
        traces::ATTR_CHUNK_TAGS,
        METRIC_SAMPLING_PRIORITY,
        METRIC_TOP_LEVEL,
    ];
    let on_event = event.attributes.iter().any(|(sym, _)| {
        let key = resolve(sym);
        SPAN_KEYS.contains(&key) || key.starts_with("_dd.")
    });
    on_event
        || resource.attributes.iter().any(|(sym, _)| {
            let key = resolve(sym);
            key.starts_with("_logit.datadog.tracer.") || key.starts_with("_logit.datadog.agent.")
        })
}

/// Every event's [`TraceReadiness`], by index into `batch.events`; `None` for an event with no
/// span. Spans group into chunks by 128-bit trace id, as [`DatadogEncoder::encode_agent_payload`]
/// groups them, and every span of a chunk takes its root's verdict, so a chunk is sent whole or
/// not at all.
///
/// The root is the Agent's own choice (`traceutil.GetRoot`): scanning from the last span, the
/// first with no parent; failing that, the first span in chunk order whose parent isn't in the
/// chunk; failing that (a parent cycle), the last span.
pub fn trace_readiness(batch: &EventBatch) -> Vec<Option<TraceReadiness>> {
    let mut out = vec![None; batch.events.len()];
    for members in &trace_chunks(batch) {
        let root = &batch.events[chunk_root(batch, members)];
        let verdict = if is_agent_processed(root) {
            TraceReadiness::Ready
        } else if is_datadog_span(&batch.resource, root) {
            TraceReadiness::NeedsAgentProcessing
        } else {
            TraceReadiness::NotDatadogOrigin
        };
        for &i in members {
            out[i] = Some(verdict);
        }
    }
    out
}

/// `batch`'s span events grouped into trace chunks, as every trace encoder groups them: one chunk
/// per 128-bit trace id in first-appearance order, each the indices into `batch.events` of its
/// spans in batch order. Events without a span are in no chunk.
pub fn trace_chunks(batch: &EventBatch) -> Vec<Vec<usize>> {
    let mut chunks: Vec<Vec<usize>> = Vec::new();
    let mut index: HashMap<[u8; 16], usize> = HashMap::new();
    for (i, event) in batch.events.iter().enumerate() {
        let Some(span) = &event.span else { continue };
        let chunk = *index.entry(span.trace_id).or_insert_with(|| {
            chunks.push(Vec::new());
            chunks.len() - 1
        });
        chunks[chunk].push(i);
    }
    chunks
}

/// The index into `batch.events` of the root of the chunk `members`, by [`trace_readiness`]'s
/// rule. `members` is non-empty and every member carries a span.
fn chunk_root(batch: &EventBatch, members: &[usize]) -> usize {
    let span_of = |i: usize| batch.events[i].span.as_ref().expect("chunk members carry a span");
    if let Some(&i) = members.iter().rev().find(|&&i| span_of(i).parent_span_id.is_none()) {
        return i;
    }
    let ids: HashSet<[u8; 8]> = members.iter().map(|&i| span_of(i).span_id).collect();
    members
        .iter()
        .copied()
        .find(|&i| span_of(i).parent_span_id.is_some_and(|p| !ids.contains(&p)))
        .unwrap_or(*members.last().expect("a chunk has at least one span"))
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

    /// `logit.input.tags.omitted`: the tags [`tags::insert_tags`] left off a series, sketch,
    /// event, or check that was delivered, `reason="reserved_prefix"` for a private key and
    /// `reason="reserved_key"` for a standard service identity key beside its Datadog-spelled tag.
    pub(super) fn reserved_tags(&self, omitted: tags::Omitted) {
        self.omitted_tags(omitted.reserved_prefix, "reserved_prefix");
        self.omitted_tags(omitted.reserved_key, "reserved_key");
    }

    /// `logit.input.tags.omitted{reason="reserved_key"}`: `n` `host.name` tags or keys
    /// [`insert_wire_host`] displaced with a record's own host, on a record that was delivered.
    pub(super) fn displaced_host_names(&self, n: usize) {
        self.omitted_tags(n, "reserved_key");
    }

    fn omitted_tags(&self, n: usize, reason: &'static str) {
        if n > 0 {
            self.telemetry.count("logit.input.tags.omitted", n as f64, &[("reason", reason)]);
        }
    }
}

/// Encodes an `EventBatch` into Datadog intake bodies, one per route. Every skip and degrade is
/// counted through [`Telemetry`]; a disabled handle costs nothing.
#[derive(Default)]
pub struct DatadogEncoder {
    telemetry: Telemetry,
    diagnostics: Diagnostics,
    /// The host of a series, sketch, log, event, or service check that carries none of its own;
    /// `None` leaves it empty or absent.
    default_host: Option<Arc<str>>,
    histograms: HistogramOptions,
    /// Series points the last series encode left off as duplicates of a later one.
    series_duplicates: usize,
    /// The sink's per-batch duplicate plan for the next series encodes, if any.
    series_plan: Option<series::SeriesPlanned>,
}

/// The most `tags` strings one series, distribution point, or sketch may carry: Datadog's intake
/// drops a metric with more, so `datadog_out` drops it first and counts it. Measured on a trial org
/// on 2026-10-08: 100 tags were stored and 101 dropped, duplicates counted and `resources` not; the
/// series route names the drop in its `202` body, and the other two drop it silently.
pub const MAX_METRIC_TAGS: usize = 100;

/// How a delta `Histogram` is written, the Datadog exporter's `histograms.mode`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HistogramMode {
    /// One sketch per record, bin for bin as the exporter's translator builds it.
    #[default]
    Distributions,
    /// One `<name>.bucket` count series per bucket, tagged with its bounds.
    Counters,
    /// No bucket output; the aggregation series alone.
    NoBuckets,
}

/// The Datadog exporter's `histograms:` settings, which apply to `Histogram` and, for
/// `send_aggregation_metrics`, to `ExponentialHistogram` records.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HistogramOptions {
    pub mode: HistogramMode,
    /// Adds `<name>.count`, `<name>.sum`, `<name>.min`, and `<name>.max` series.
    pub send_aggregation_metrics: bool,
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

    /// The host written for a series, sketch, log, event, or service check that carries no host
    /// of its own, and for an `AgentPayload`'s or intake `StatsPayload`'s tracer payload with no
    /// resource `host.name` and no relaying Agent's hostname (the module doc's encode tables say
    /// which attributes count); `datadog_out` passes the process hostname. Never replaces a host
    /// the event carries.
    pub fn with_default_host(mut self, host: Option<Arc<str>>) -> Self {
        self.default_host = host;
        self
    }

    pub fn with_histograms(mut self, histograms: HistogramOptions) -> Self {
        self.histograms = histograms;
        self
    }

    pub fn histograms(&self) -> HistogramOptions {
        self.histograms
    }

    /// Counts `n` events no route carries, an event with no metric, log, or span, as
    /// `logit.output.events.skipped{reason="no_payload"}`. The sink's routing finds them, so it
    /// counts them through the encoder's handle.
    pub fn events_without_payload(&self, n: usize) {
        if n > 0 {
            self.telemetry.count(
                "logit.output.events.skipped",
                n as f64,
                &[("reason", "no_payload")],
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, SpanKind, SpanRecord, SpanStatus};
    use std::sync::Arc;

    fn span(trace: u8, id: u8, parent: Option<u8>, attrs: &[(&str, Value)]) -> Event {
        let mut attributes = AttrMap::new();
        for (key, value) in attrs {
            attributes.insert(key, value.clone());
        }
        Event::span(
            1,
            attributes,
            SpanRecord {
                trace_id: [trace; 16],
                span_id: [id; 8],
                parent_span_id: parent.map(|p| [p; 8]),
                name: Value::str("op"),
                kind: SpanKind::Internal,
                status: SpanStatus::Unset,
                events: Vec::new(),
                links: Vec::new(),
                end_timestamp: 2,
                flags: 0,
                ext: None,
            },
        )
    }

    fn batch(events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
    }

    /// A root with `_top_level` makes its whole chunk ready, children included; a Datadog root
    /// without it holds its chunk back; a span with no Datadog attribute is not Datadog-origin;
    /// a non-span event has no verdict.
    #[test]
    fn a_chunk_takes_its_roots_verdict() {
        let dd = (ATTR_RESOURCE_NAME, Value::str("GET /"));
        let top = (METRIC_TOP_LEVEL, Value::F64(1.0));
        let b = batch(vec![
            // Chunk 1: the child arrives first; only the root carries `_top_level`.
            span(1, 2, Some(1), std::slice::from_ref(&dd)),
            span(1, 1, None, &[dd.clone(), top]),
            // Chunk 2: a raw tracer root.
            span(2, 3, None, &[dd.clone(), ("_dd.p.dm", Value::str("-0"))]),
            // Chunk 3: an OTel span.
            span(3, 4, None, &[("http.route", Value::str("/"))]),
            Event::metric(
                1,
                AttrMap::new(),
                logit_core::MetricRecord::new(
                    logit_core::interner::intern("m"),
                    MetricKind::gauge(1.0),
                ),
            ),
        ]);
        use TraceReadiness::*;
        assert_eq!(
            trace_readiness(&b),
            vec![
                Some(Ready),
                Some(Ready),
                Some(NeedsAgentProcessing),
                Some(NotDatadogOrigin),
                None
            ]
        );
        assert_eq!(NeedsAgentProcessing.drop_reason(), Some("needs_agent_processing"));
        assert_eq!(NotDatadogOrigin.drop_reason(), Some("not_datadog_origin"));
        assert_eq!(Ready.drop_reason(), None);
    }

    /// `_top_level: 0` isn't the mark, and a `_logit.datadog.tracer.*` resource makes an otherwise
    /// bare span Datadog-origin.
    #[test]
    fn a_zero_top_level_is_unprocessed_and_a_tracer_resource_marks_the_origin() {
        assert!(!is_agent_processed(&span(1, 1, None, &[(METRIC_TOP_LEVEL, Value::F64(0.0))])));
        assert!(is_agent_processed(&span(1, 1, None, &[(METRIC_TOP_LEVEL, Value::I64(1))])));

        let mut resource = Resource::default();
        resource.attributes.insert(RESOURCE_ATTR_TRACER_LANGUAGE_NAME, Value::str("python"));
        let b = EventBatch {
            resource: Arc::new(resource),
            scope: None,
            events: vec![span(1, 1, None, &[])],
        };
        assert_eq!(trace_readiness(&b), vec![Some(TraceReadiness::NeedsAgentProcessing)]);
    }

    /// With no parentless span, the root is the first span whose parent is outside the chunk,
    /// not the first span.
    #[test]
    fn an_orphan_chunks_root_is_the_span_whose_parent_is_missing() {
        let top = (METRIC_TOP_LEVEL, Value::F64(1.0));
        let dd = (ATTR_SPAN_TYPE, Value::str("web"));
        let b = batch(vec![
            span(1, 3, Some(2), std::slice::from_ref(&dd)),
            span(1, 2, Some(9), &[dd, top]),
        ]);
        assert_eq!(trace_readiness(&b), vec![Some(TraceReadiness::Ready); 2]);
    }
}
