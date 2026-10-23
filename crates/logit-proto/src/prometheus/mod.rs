//! The Prometheus codec: the exposition/OpenMetrics *semantic* model (`MetricFamily`/`Series`/
//! `Point`) and its two conversions to and from `logit`'s own event model. The text syntax for both
//! dialects lives next door in [`text`]; nothing in this file knows what a line looks like.
//!
//! **This module doc is the canonical decode table and "Permitted normalizations" list**; docs,
//! tests, and examples point here. Its encode mapping is generated from `prometheus_out`'s column
//! of `spec/transit/mapping.yaml`, the canonical copy ("Encode" below).
//!
//! **Why the split.** [`MetricFamily`] is the seam: [`text`] maps exposition bytes ↔ families,
//! [`remote_write`] maps prompb 1.0/2.0 ↔ families, [`assemble`] holds the flat-sample reassembly
//! both of those need, and *this* module maps families ↔ [`Event`]s. Remote-write carries the same
//! semantics the exposition format describes (`docs/design/telemetry-landscape.md`'s remote-write
//! section), so the tables below must not depend on wire syntax. What remote-write needs beyond
//! them is three switches ([`PrometheusDecoder::with_timestamp_marker`],
//! [`PrometheusEncoder::with_timestamps_always`], [`PrometheusEncoder::with_stale_markers`]) and
//! one [`Point`] variant ([`Point::Stale`]); each switch defaults to the exposition behaviour
//! ([ADR `prometheus-remote-write`](../../../../docs/adr/prometheus-remote-write.md)).
//!
//! **No [`crate::Encoder`]/[`crate::Decoder`]/[`crate::SignalEncoder`]/[`crate::SignalDecoder`]
//! implementation here** (ADR `statsd-output` §"No `logit_proto::Encoder`" has the same reasoning):
//! `prometheus_out` is a *stateful* registry an HTTP handler renders on demand, with no
//! `EventBatch -> bytes` call, and `prometheus_in` is an HTTP *client* that already holds the
//! response body, not a framed stream a `Decoder` gets fed. Plain functions plus the two
//! builder-configured handles ([`PrometheusDecoder`], [`PrometheusEncoder`],
//! `with_telemetry`/`with_diagnostics` like [`crate::otlp::OtlpEncoder`]) give the counters a home.
//!
//! ## Family naming
//!
//! [`MetricFamily::name`] is **the name the model uses** -- what a [`logit_core::MetricRecord`]'s
//! `name` is, verbatim, in both directions. That is not always the name on the family's `# TYPE`
//! line, and the difference is entirely `text.rs`'s business:
//!
//! | Family type | `# TYPE` name | Sample names |
//! |---|---|---|
//! | `counter` | text 0.0.4: `name`, with `_total` appended if it lacks it; OpenMetrics: `name` minus a trailing `_total` | the value sample always ends in `_total`; OM adds `<base>_created` |
//! | `histogram`/`gaugehistogram` | `name` | `name_bucket`, `name_sum`/`name_gsum`, `name_count`/`name_gcount`, `name_created` |
//! | `summary` | `name` | `name` (quantiles), `name_sum`, `name_count`, `name_created` |
//! | `info` | `name` | `name_info` |
//! | `stateset` | `name` | `name` (one per state) |
//! | `gauge`/`unknown`/`untyped` | `name` | `name` |
//!
//! So a text 0.0.4 counter family `http_requests_total` and an OpenMetrics counter family
//! `http_requests` (samples `http_requests_total`) both give `name = "http_requests_total"`: the
//! *sample* name is what a scraped series is called, so that is what the model keeps ("name kept
//! verbatim, `_total` included"). The `_total` handling is a dialect concern, not a model one.
//!
//! ## Decode: families → events ([`families_to_events`])
//!
//! One [`Event`] per [`Series`], each carrying exactly one [`logit_core::MetricRecord`]; the
//! series' labels become `Value::Str` event attributes verbatim (no sanitization -- they arrived
//! valid). A label whose name is under the private `_logit.` prefix is omitted, counted
//! `logit.input.tags.omitted{reason="reserved_prefix"}` once per event, so a sender's label never
//! reads back as a carrier
//! ([ADR `private-attribute-namespace`](../../../../docs/adr/private-attribute-namespace.md)).
//! Only remote-write can carry one: the exposition grammar [`text`] reads has no `.` in a label
//! name.
//!
//! | Wire | Model |
//! |---|---|
//! | `counter` sample (name verbatim, `_total` included) | `Sum { value, Cumulative, monotonic: true }` |
//! | `gauge` | `Gauge(v)` |
//! | `histogram` (`_bucket{le}` cumulative, `_sum`, `_count`) | `Histogram { buckets: per-bucket counts by successive difference, trailing `(+Inf, n)`, Cumulative, sum, min/max: None }` |
//! | `gaugehistogram` (OM, `_gsum`/`_gcount`) | the same `Histogram`, plus the flag `_logit.metrics.type: "gaugehistogram"` |
//! | `summary` (`{quantile}`, `_sum`, `_count`) | `Summary { quantiles, count, sum }`, `Cumulative` |
//! | `untyped` (text) / `unknown` (OM) / no `# TYPE` | `Gauge(v)` + `_logit.prometheus.type: "untyped"` \| `"unknown"` |
//! | `info` (OM, `X_info{...} 1`) | `Gauge(1)` + the flag `_logit.metrics.type: "info"`, `name = X` (`_info` stripped) |
//! | `stateset` (OM) | one `Gauge(0\|1)` per state line, each + the flag `_logit.metrics.type: "stateset"` |
//! | `# HELP` / `# UNIT` | `description` / `unit` (interned) |
//! | `_created` (OM) | `start_timestamp` (seconds → nanos) |
//! | [`Point::Stale`] (remote-write's stale-marker NaN) | the family type's own kind with an empty payload -- `counter` → `Sum{NaN, Cumulative, monotonic}`, `gauge`/`unknown`/`untyped`/`info`/`stateset` → `Gauge(NaN)`, `histogram`/`gaugehistogram` → a bucket-less `Histogram`, `summary` → an empty `Summary` -- and **`flags = FLAG_NO_RECORDED_VALUE`**, which carries the meaning ([`logit_core::MetricRecord::flags`]). The `_logit.prometheus.type` carrier or `_logit.metrics.type` flag is unchanged |
//! | sample timestamp | `Event::timestamp` + the flag `_logit.metrics.closed: "timestamped"`, which tells `aggregate` the sample has its own time and the encode side to write it back (`docs/adr/metric-flags.md`, "The flags"); absent → `received_at_nanos`, no flag. The flag says *the producer chose to expose a timestamp on this line*, so a transport that mandates one turns it off with [`PrometheusDecoder::with_timestamp_marker`]`(false)` and sets only the timestamp |
//! | OM exemplar | `Exemplar { value, timestamp, trace, filtered_attributes }` -- `trace_id`/`span_id` labels become a [`logit_core::TraceRef`] when both are valid hex (consumed); every other exemplar label, and an invalid id, stays in `filtered_attributes`. Bucket exemplars all collect onto the one record. |
//!
//! ## Encode: events → families ([`events_to_families`])
//!
//! The inverse, plus the rules for model kinds Prometheus has no wire type for. Labels are
//! `resource` attributes merged with event attributes (event wins), private `_logit.*` keys
//! skipped.
//!
//! The tables below are generated from `prometheus_out`'s column of `spec/transit/mapping.yaml`,
//! the canonical copy, by `script/transit docs`, and a test fails when they're stale: change the
//! table there, not here. "Consumer" means `prometheus_in` reading the exposition, or the
//! remote-write receiver reading a request, back. A cell under "with `bind:` unset" is
//! remote-write's; the others are exposition's, in OpenMetrics.
//!
//! <!-- transit:doc:prometheus_out -->
//!
//! Per metric kind, and per record feature that decides a record's fate:
//!
//! | Row | Class | Reason or counter | What a consumer sees |
//! |---|---|---|---|
//! | `sum/delta_monotonic` | dropped | `logit.output.metrics.dropped{reason=unresolved_delta}` | Prometheus has no delta counter, so a bare delta sum is dropped, with a throttled `delta_temporality_unresolved` warning; an `aggregate` with `temporality: cumulative` upstream turns it into a counter. |
//! | `sum/delta_nonmonotonic` | dropped | `logit.output.metrics.dropped{reason=unresolved_delta}` | A bare delta sum is dropped, as for a monotonic one. |
//! | `sum/cumulative_monotonic` | normalized | `total_suffix` | A `counter` family. Its sample is named with `_total`, appended when the record's name lacks it, so such a record reads back under the longer name. |
//! | `sum/cumulative_nonmonotonic` | degraded | `logit.output.metrics.degraded{metric_kind=non_monotonic_sum}` | Prometheus has no non-monotonic counter, so the running total is a `gauge`. |
//! | `sum/f64` | exact | -- | The sample's double. |
//! | `sum/i64` | normalized | `f64_value` | A sample is a double, so an integer is written as its nearest `f64` and reads back as a float, rounded beyond 2^53. |
//! | `sum/u64` | normalized | `f64_value` | A sample is a double, so an integer is written as its nearest `f64` and reads back as a float, rounded beyond 2^53. |
//! | `sum/bool` | normalized | `bool_value`; `logit.output.metrics.normalized{reason=bool_value}` | A boolean is the sample `1` or `0` and reads back as that number. |
//! | `gauge/f64` | exact | -- | A `gauge` family, or the family type the `_logit.metrics.type` flag (`info`, `stateset`) or the `_logit.prometheus.type` carrier (`untyped`, `unknown`) names. |
//! | `gauge/i64` | normalized | `f64_value` | A sample is a double, so an integer is written as its nearest `f64` and reads back as a float, rounded beyond 2^53. |
//! | `gauge/u64` | normalized | `f64_value` | A sample is a double, so an integer is written as its nearest `f64` and reads back as a float, rounded beyond 2^53. |
//! | `gauge/bool` | normalized | `bool_value`; `logit.output.metrics.normalized{reason=bool_value}` | A boolean is the sample `1` or `0` and reads back as that number. |
//! | `gauge/negative` | exact | -- | A negative sample, written as is. |
//! | `gauge/prometheus_info` | exact | -- | An `info` family, its sample named `<name>_info`. |
//! | `gauge/prometheus_stateset` | exact | -- | A `stateset` family, one sample per state. |
//! | `gauge_delta/f64` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | An unresolved relative adjustment has no absolute value to expose and is dropped, with a throttled `gauge_delta_unresolved` warning; an `aggregate` upstream resolves it into a gauge. |
//! | `gauge_delta/i64` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | Dropped, as for a float adjustment. |
//! | `gauge_delta/u64` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | Dropped, as for a float adjustment. |
//! | `gauge_delta/bool` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | Dropped, as for a float adjustment. |
//! | `samples` | degraded | `logit.output.metrics.degraded{metric_kind=samples}` | Sketched, each value weighted by its inverse sample rate, then written as the `distribution` row describes. |
//! | `set_members` | degraded | `logit.output.metrics.degraded{metric_kind=set_members}` | A `gauge` of the distinct member count; the members are gone. |
//! | `distribution` | degraded | `logit.output.metrics.degraded{metric_kind=distribution}` | A `summary` of the sketch's `0` (its minimum), `0.5`, `0.75`, `0.9`, `0.95`, `0.99`, and `1` (its maximum) quantiles with its exact `_sum` and `_count`; the sketch is gone, so quantiles from two senders don't merge. |
//! | `set` | degraded | `logit.output.metrics.degraded{metric_kind=set}` | A `gauge` of the estimate; the registers are gone. |
//! | `histogram/delta` | dropped | `logit.output.metrics.dropped{reason=unresolved_delta}` | A delta histogram is dropped, with a throttled `delta_temporality_unresolved` warning; an `aggregate` with `temporality: cumulative` upstream makes it a running one. |
//! | `histogram/cumulative` | normalized | `inf_bucket` | A `histogram` family of running-sum buckets and `_sum` when it has one, or a `gaugehistogram` under the type flag. `+Inf` states the total, so a histogram whose last bound is finite reads back with an empty `+Inf` bucket. `min` and `max` are `record/min_max`'s. |
//! | `histogram/nan_bound` | degraded | `logit.output.metrics.degraded{reason=non_finite_component}` | A `NaN` bound has no place among the sorted bounds, so it has no bucket, and its count reaches only `+Inf` and `_count`. |
//! | `histogram/neg_inf_bound` | exact | -- | `-Inf` is an ordinary bound: an `le="-Inf"` bucket, first. |
//! | `histogram/gauge_histogram` | exact | -- | A `gaugehistogram` family, with `_gsum` and `_gcount`. |
//! | `exponential_histogram/delta`, with `bind:` unset | dropped | `logit.output.metrics.dropped{reason=native_histogram}` | Remote-write carries native histograms, but the sender doesn't map them yet, so one is dropped. |
//! | `exponential_histogram/delta`, otherwise | skipped | `logit.output.metrics.skipped{metric_kind=exponential_histogram}` | Neither exposition dialect has native-histogram syntax, so none is written. |
//! | `exponential_histogram/cumulative`, with `bind:` unset | dropped | `logit.output.metrics.dropped{reason=native_histogram}` | Dropped, as for a delta one. |
//! | `exponential_histogram/cumulative`, otherwise | skipped | `logit.output.metrics.skipped{metric_kind=exponential_histogram}` | Neither exposition dialect has native-histogram syntax, so none is written. |
//! | `summary/delta` | dropped | `logit.output.metrics.dropped{reason=unresolved_delta}` | A Prometheus summary is a running total, so a delta one, which only `cumulative_to_delta` makes, is dropped, with a throttled `delta_summary_unresolved` warning; send this sink the summary from ahead of that component. |
//! | `summary/cumulative` | exact | -- | A `summary` family of its quantiles, `_sum`, and `_count`. |
//! | `record/no_recorded_value`, with `bind:` unset | exact | -- | Remote-write writes a flagged `Gauge` or cumulative `Sum` as Prometheus's stale marker, read back flagged; a flagged non-monotonic `Sum`'s goes on a `gauge`, as its value would. |
//! | `record/no_recorded_value`, otherwise | skipped | `logit.output.metrics.skipped{reason=no_recorded_value}` | A point flagged as holding no recorded value isn't exposed. |
//! | `record/no_recorded_value_delta_sum`, with `bind:` unset | degraded | `logit.output.metrics.degraded` | Remote-write writes a flagged delta `Sum` as a stale marker, its temporality ignored, since a marker holds no reading to be delta about: a monotonic one on a `counter`, read back as a flagged cumulative counter, counted `logit.output.metrics.degraded{metric_kind=delta_sum}`, and a non-monotonic one on a `gauge`, counted `logit.output.metrics.degraded{metric_kind=non_monotonic_sum}`. An unflagged delta `Sum` is dropped. |
//! | `record/no_recorded_value_delta_sum`, otherwise | skipped | `logit.output.metrics.skipped{reason=no_recorded_value}` | A point flagged as holding no recorded value isn't exposed. |
//! | `record/no_recorded_value_other_kind` | skipped | `logit.output.metrics.skipped{reason=no_recorded_value}` | A flagged record of a kind other than `Gauge` or `Sum` isn't written in either mode. It would be several series, and the flag doesn't say which of them existed, so a stale marker would name a series that may never have been sent. |
//! | `record/service_check` | exact | -- | The status gauge; the check's carriers aren't labels. |
//! | `record/name_unsafe` | normalized | `name_sanitized`; `logit.output.metrics.normalized{reason=name_sanitized}` | A name is forced into `[a-zA-Z_:][a-zA-Z0-9_:]*`, each other character `_`, so the record reads back under the sanitized name, counted once per record it renames. Two names that sanitize onto one in a batch keep one family, the other counted `logit.output.metrics.dropped{reason=name_collision}`; across batches they merge into one series, uncounted. |
//! | `record/exemplar_key_unsafe` | normalized | `name_sanitized`; `logit.output.labels.normalized{reason=name_sanitized}` | An exemplar's attribute key is written as a label name forced into `[a-zA-Z_][a-zA-Z0-9_]*`, each other character `_`, as a series label's is, so the exemplar reads back under the sanitized key, counted once per key it renames. |
//! | `record/exemplar_key_unsafe_histogram` | normalized | `name_sanitized`; `logit.output.labels.normalized{reason=name_sanitized}` | An exemplar's attribute key is written as a label name forced into `[a-zA-Z_][a-zA-Z0-9_]*`, each other character `_`, as a series label's is, so the exemplar reads back on its bucket under the sanitized key, counted once per key it renames. |
//! | `record/sample_rate` | exact | -- | Each value is weighted by its inverse rate in the sketch the `samples` row describes. |
//! | `record/superseded`, with `bind:` unset | exact | -- | Remote-write sends each sample with its own timestamp. |
//! | `record/superseded`, otherwise | normalized | `superseded` | The exposition registry holds one sample per series, so a later record of the series replaces an earlier one before a scrape reads it: the pull model's overwrite, uncounted. |
//! | `record/same_instant`, with `bind:` unset | dropped | `logit.output.metrics.dropped{reason=duplicate_timestamp}` | One series can't hold two samples at one timestamp, so the later point wins and the earlier is dropped, counted, when it differs from it or is an increment, such as a Datadog `rate`, or an observation set, such as a `samples` record's summary; a `dedup_timestamps` upstream merges or collapses the pair instead. |
//! | `record/same_instant`, otherwise | normalized | `superseded` | The exposition registry holds one sample per series, so the later point replaces this one: the pull model's overwrite, uncounted. |
//! | `record/same_instant_equal`, with `bind:` unset | normalized | `duplicate_timestamp`; `logit.output.metrics.normalized{reason=duplicate_timestamp}` | One series can't hold two samples at one timestamp, so the later point is sent, and this state, equal to it, is left off, counted: the receiver stores the same sample either way. |
//! | `record/same_instant_equal`, otherwise | normalized | `superseded` | The exposition registry holds one sample per series, so the later point replaces this one: the pull model's overwrite, uncounted. |
//! | `record/same_millisecond`, with `bind:` unset | dropped | `logit.output.metrics.dropped{reason=duplicate_timestamp}` | A remote-write sample's timestamp is in milliseconds, and one series can't hold two samples at one timestamp, so the later reading wins and the earlier is dropped, counted, when it differs from it or is an increment, such as a Datadog `rate`, or an observation set; a `dedup_timestamps` with `precision: 1ms` upstream merges or collapses the pair instead. |
//! | `record/same_millisecond`, otherwise | normalized | `superseded` | The exposition registry holds one sample per series, so the later point replaces this one: the pull model's overwrite, uncounted. |
//! | `record/same_millisecond_equal`, with `bind:` unset | normalized | `duplicate_timestamp`; `logit.output.metrics.normalized{reason=duplicate_timestamp}` | A remote-write sample's timestamp is in milliseconds, so the later reading is sent, and this state, equal to it, is left off, counted: the receiver stores the same sample either way. |
//! | `record/same_millisecond_equal`, otherwise | normalized | `superseded` | The exposition registry holds one sample per series, so the later point replaces this one: the pull model's overwrite, uncounted. |
//! | `record/non_finite` | exact | -- | `NaN`, `+Inf`, or `-Inf`, as the sample. |
//! | `log/dogstatsd_event` | skipped | `logit.output.events.skipped{reason=no_metrics}` | A log has no Prometheus form and isn't written. |
//! | `log/plain` | skipped | `logit.output.events.skipped{reason=no_metrics}` | A log has no Prometheus form and isn't written. |
//! | `span/alone` | skipped | `logit.output.events.skipped{reason=no_metrics}` | A span has no Prometheus form and isn't written. |
//! | `span/no_wire_form` | exact | -- | Nothing beyond `span/alone`'s outcome. |
//! | `event/attributes_only` | skipped | `logit.output.events.skipped{reason=no_metrics}` | An event with no metric, log, or span has no sample and isn't written. |
//! | `event/string_only` | skipped | `logit.output.events.skipped{reason=no_metrics}` | An InfluxDB point whose fields are all strings has no metric, so it has no sample and isn't written. |
//!
//! Per field of a record that reaches the wire, attribute shape, and payload beside metrics:
//!
//! | Row | Class | Reason or counter | What a consumer sees |
//! |---|---|---|---|
//! | `record/unit`, with `bind:` unset | exact | -- | Remote-write carries the unit in its metadata. |
//! | `record/unit`, otherwise | lost | `logit.output.metrics.degraded{reason=unit_not_suffix}` | OpenMetrics writes `# UNIT` only when `_<unit>` ends the family name; otherwise it's left off, counted. |
//! | `record/description` | exact | -- | The family's `# HELP` line. |
//! | `record/start_timestamp` | lost | structural | OpenMetrics' `_created` sample, written for a counter, histogram, or summary; a gauge has no `_created`, so its start time is left off. |
//! | `record/exemplars` | lost | `logit.output.metrics.degraded{reason=exemplar_omitted}` | In OpenMetrics, on a counter's `_total` line or the bucket line its value falls in, at most one per line. One with no line to sit on, such as a gauge's, or one over the 128-code-point label budget, on either wire, is left off, counted. |
//! | `record/unit_on_distribution`, with `bind:` unset | exact | -- | Remote-write carries the unit in its metadata. |
//! | `record/unit_on_distribution`, otherwise | lost | `logit.output.metrics.degraded{reason=unit_not_suffix}` | OpenMetrics writes `# UNIT` only when `_<unit>` ends the family name; otherwise it's left off, counted. |
//! | `record/exemplars_on_summary` | lost | `logit.output.metrics.degraded{reason=exemplar_omitted}` | A summary has no `_total` or bucket line for an exemplar to sit on, so its exemplars are left off, counted. |
//! | `record/min_max` | lost | structural | A histogram's `min` and `max` have no Prometheus series, so they aren't written. |
//! | `attributes/string_value` | normalized | `label_sanitized`; `logit.output.labels.normalized{reason=name_sanitized}` | A label; its name is forced into `[a-zA-Z_][a-zA-Z0-9_]*`, so a key holding `.` or another character reads back under the sanitized name, counted once per label it renames. |
//! | `attributes/bool_value` | normalized | `stringified`; `logit.output.labels.normalized{reason=name_sanitized}` | A label of `true` or `false`, read back as the string. A name outside the label-name grammar is sanitized, counted once per label it renames. |
//! | `attributes/number_value` | normalized | `stringified`; `logit.output.labels.normalized{reason=name_sanitized}` | A label of the number's decimal text, read back as the string. A name outside the label-name grammar is sanitized, counted once per label it renames. |
//! | `attributes/array_value` | normalized | `multi_value`; `logit.output.labels.normalized{reason=multi_value}` | A label set is a map, so only the last representable element is written, counted. |
//! | `attributes/map_value` | lost | `logit.output.labels.omitted{reason=unrepresentable}` | A map has no label form and is left off, counted. |
//! | `attributes/bytes_value` | lost | `logit.output.labels.omitted{reason=unrepresentable}` | A bytes value has no label form and is left off, counted. |
//! | `attributes/timestamp_value` | lost | `logit.output.labels.omitted{reason=unrepresentable}` | A timestamp value has no label form and is left off, counted. |
//! | `attributes/private_carrier` | normalized | `dialect` | The family type carrier, `untyped` or `unknown`, is consumed into the wire form and a scrape stamps it back; OpenMetrics has no `untyped`, so that type reads back as `unknown`. |
//! | `attributes/prometheus_target` | normalized | `retargeted` | The resource's `_logit.prometheus.target` isn't written. A scrape of prometheus_out stamps prometheus_out's own URL, and a remote-write receiver stamps none. Only the original target's host:port survives, as the `instance` label; its scheme and path are lost. |
//! | `attributes/foreign_carrier` | lost | structural | Another protocol's `_logit.` carrier is private and isn't a label, so it's left off. A `_logit.metrics.*` flag is left off too; the `timestamped`, `rate`, `family_type`, and `interval` rows have `_logit.metrics.closed: timestamped`, `_logit.metrics.type`, and `_logit.metrics.interval`. |
//! | `attributes/timestamped`, with `bind:` unset | lost | structural | Remote-write gives every sample its timestamp, so the timestamp survives, but a remote-write receiver can't tell it from one the producer didn't choose and sets no flag. |
//! | `attributes/timestamped`, otherwise | exact | -- | The event's timestamp is written as the sample's timestamp, and a scrape sets the flag again. A statsd `\|T` line leaves with a sample timestamp too. |
//! | `attributes/rate` | lost | structural | The flag is private and isn't a label, so it's left off. The sink reads it to count a same-millisecond pair of rates `dropped` rather than `normalized`, and writes no wire form for it. |
//! | `attributes/interval` | lost | structural | The flag is private and isn't a label, so it's left off. |
//! | `attributes/rate_on_non_gauge` | lost | structural | The flag is private and isn't a label, so it's left off. |
//! | `attributes/family_type` | exact | -- | The flag is consumed into the family's `# TYPE` line, and prometheus_in sets it again. An info or stateset gauge a script or a `set` flagged exposes with its type line too. |
//! | `attributes/family_type_on_other_kind` | lost | structural | The value names another kind, so it's ignored: the record takes its own kind's family type, and the flag is private and isn't a label. |
//! | `attributes/interval_on_distribution` | lost | structural | The flag is private and isn't a label, so it's left off. |
//! | `attributes/interval_on_histogram` | lost | structural | The flag is private and isn't a label, so it's left off. |
//! | `attributes/resource_attribute` | normalized | `resource_merged`; `logit.output.labels.normalized{reason=name_sanitized}` | A label, the event's value winning on a shared key, read back as an event attribute. A name outside the label-name grammar is sanitized, counted once per label it renames. |
//! | `attributes/scope` | lost | structural | Prometheus has no instrumentation scope, so none is written. |
//! | `attributes/datadog_carrier_log` | lost | structural | Another protocol's `_logit.` carrier is private and isn't a label, so it's left off. |
//! | `attributes/no_host` | exact | -- | A record with no host leaves with none. |
//! | `log/beside_metrics` | lost | uncounted | A log on an event that carries metrics is left off while the metrics are written, uncounted. |
//! | `span/beside_metrics` | lost | uncounted | A span on an event that carries metrics is left off while the metrics are written, uncounted. |
//!
//! <!-- transit:end -->
//!
//! The rest of the encode mapping is hand-written: rules the spec has no row for, and details its
//! cells don't state.
//!
//! | Model | Wire |
//! |---|---|
//! | a `Sum` or `Gauge` value, or an exemplar's value, of `I64`/`U64`/`Bool` | the sample's double, on both wires: an integer's nearest `f64`, a `Bool` `1` or `0`, counted `logit.output.metrics.normalized{reason="bool_value"}` |
//! | `Distribution(sketch)` | `summary` of [`SUMMARY_QUANTILES`], `0` and `1` the sketch's minimum and maximum, + `_sum` + `_count`; `_sum` is [`DdSketch::sum`], exact unlike a quantile (approximate only when [`DdSketch::stats_exact`] is `false`, i.e. the sketch's stats were derived from its bins) -- `logit.output.metrics.degraded{metric_kind="distribution"}` |
//! | a `Histogram` bound | sorted ascending, equal bounds merged into one `le`; a `NaN` bound has no `le` bucket, its count reaching only `+Inf` and `_count` -- `logit.output.metrics.degraded{reason="non_finite_component"}`, once per record. `-Inf` is an ordinary bound, `le="-Inf"` |
//! | an event with no metric record (a log, a span, or neither) | nothing -- `logit.output.events.skipped{reason="no_metrics"}` |
//! | a flagged delta monotonic `Sum` under [`PrometheusEncoder::with_stale_markers`]`(true)` | a stale marker on a `counter`, which reads back as a cumulative counter -- `logit.output.metrics.degraded{metric_kind="delta_sum"}`; a flagged non-monotonic one is a `gauge`'s marker, `{metric_kind="non_monotonic_sum"}` |
//! | `Event::timestamp` | emitted only when the event is flagged `_logit.metrics.closed: "timestamped"`, on the event or else the resource ([`logit_core::attrs::is_closed_timestamped`]; `bucketed`, any other value, and `Bool(true)` emit none), whichever decoder set it, so a `statsd_in` `\|T` line gets a sample timestamp too -- or on every series, flag or not, under [`PrometheusEncoder::with_timestamps_always`]`(true)`, which is what a transport with no way to omit a timestamp needs |
//! | a `Gauge`'s or cumulative `Histogram`'s family type | `info` or `stateset` for a `Gauge`, and `gaugehistogram` for a `Histogram`, when the event is flagged `_logit.metrics.type` with that value, on the event or else the resource ([`logit_core::attrs::metric_type`]); else, for a `Gauge`, `untyped` or `unknown` when the event carries that [`ATTR_TYPE`]; else `gauge` or `histogram`. Any other value of either key is ignored, so a carrier spelling `info` exposes a plain gauge |
//! | labels | `Value::Str/I64/U64/F64/Bool` stringified; `Null/Bytes/Timestamp/Map` dropped -- `logit.output.labels.omitted{reason="unrepresentable"}`. An `Array` (a repeated DogStatsD tag key, `crate::statsd::decode::insert_tags`) renders its **last** representable element -- a Prometheus label set is a map, so there is no multi-value label -- counted `logit.output.labels.normalized{reason="multi_value"}`; an empty or entirely unrepresentable one stays on the `omitted{reason="unrepresentable"}` path |
//! | names | sanitized ([`sanitize_metric_name`], [`sanitize_label_name`]), each record written under a rewritten name counted `logit.output.metrics.normalized{reason="name_sanitized"}` and each label, a written exemplar's included, `logit.output.labels.normalized{reason="name_sanitized"}`; labels are ordered and collision-checked on their **rendered** names, and on a collision the one whose *original* attribute name sorts first wins -- `logit.output.labels.omitted{reason="collision"}`; a label sanitizing onto a generated one (`le` on a histogram, `quantile` on a summary) is omitted -- `logit.output.labels.omitted{reason="reserved"}`. An exemplar's attribute keys follow the same rule, and one sanitizing onto the `trace_id`/`span_id` its trace is written as is `reserved` |
//! | two names sanitizing onto one wire name | within one batch, the family whose *model* name sorts first is exposed, the rest are **dropped** -- `logit.output.metrics.dropped{reason="name_collision"}`. Both cannot be exposed: a second `# TYPE` line for one name makes Prometheus reject the whole scrape, so one clash would poison every other metric in the body. The tie-break is on model names, which means a name that needed no sanitizing can lose to one that did (`a.b` sorts before `a_b`) -- deterministic and counted, but worth knowing before reading it as a bug. **Across batches nothing resolves it**: the registry keys a family by its wire name, and a receiver a series by its labels, so `a.b` in one batch and `a_b` in the next merge into one series, uncounted beyond the rewrite's `name_sanitized` (`docs/known-gaps/prometheus.md`) |
//! | `unit` / `description` | `# UNIT` (OM only, and only when `_<unit>` suffixes the family name and the unit is `[a-zA-Z0-9_]+` -- the OpenMetrics spec requires it and Prometheus's parser fails the entire body otherwise; dropped counted `logit.output.metrics.degraded{reason="unit_not_suffix"}`) / `# HELP` |
//! | two families whose `# TYPE` lines carry one name | one is written, the other's series **dropped**, `logit.output.metrics.dropped{reason="name_collision"}` -- see the normalization list below |
//! | two records, one name, different family types | the first record's type wins, the rest are **dropped**, `logit.output.metrics.dropped{reason="type_conflict"}` -- one name cannot carry two `# TYPE` lines |
//! | two records of one series at one `Event::timestamp`, in one [`events_to_families`] call | the later replaces the earlier, and what the earlier's conversion counted (a degrade, a normalization, an omitted label) is discarded with it. Under [`Destination::RemoteWrite`], where the sender calls this once per timestamp, the earlier is a lost reading, counted `logit.output.metrics.dropped{reason="duplicate_timestamp"}` with a throttled `duplicate_timestamp` diagnostic naming `dedup_timestamps` as the remedy, unless both are states and it's the same reading as the later one ([`same_reading`]), which counts `logit.output.metrics.normalized{reason="duplicate_timestamp"}`; [`remote_write`]'s drop table has the sub-millisecond case. Each collapse compares with its own survivor, so an earlier reading equal to a later one of its instant counts `normalized` even when the millisecond collapse then drops that later one, counted `dropped`: the value is counted lost once. Under exposition it's the registry's one value per series, uncounted |
//! | `EventBatch::scope`, `Resource::schema_url`, `dropped_attributes_count` | dropped (known-gaps rows) |
//!
//! ## Well-known attributes (`_logit.prometheus.*`) and target identity
//!
//! Rule (b) of [ADR `lossless-transit`](../../../../docs/adr/lossless-transit.md) -- the raw,
//! protocol-native fact rides alongside the normalized model field and wins on the way back out.
//! Every `_logit.prometheus.*` attribute is private
//! ([ADR `private-attribute-namespace`](../../../../docs/adr/private-attribute-namespace.md)):
//! the encode side never renders it as a label, and every other protocol sink leaves it off its
//! tags, labels, and fields. Like any private `_logit.` attribute, it's never counted. A
//! whole-event render keeps it; that ADR's "The ignore rule" lists the renders. An attribute
//! spelled `prometheus.<name>` without the prefix is an ordinary label.
//! `docs/design/data-model.md`'s well-known-attribute table is the canonical list; in short:
//! [`ATTR_TYPE`] (`Value::Str`) is `untyped` or `unknown`, the family type of a family that
//! declared none;
//! [`ATTR_TARGET`] (`Value::Str`, a **resource** attribute stamped by `prometheus_in`) is the full
//! scrape URL.
//!
//! Target identity is the exception: `prometheus_in` also stamps an **unprefixed**
//! [`LABEL_INSTANCE`] (`instance`, `host:port`) on the resource, which this encoder therefore
//! renders as a label like any other resource attribute -- exactly the `instance` label Prometheus's
//! own scrape adds, so two targets exposing the same exporter don't collapse onto one series through
//! a relay. A relay's exposition is thus its input plus `instance`, which the fixed-point tests and
//! the ADR name as a permitted normalization. `job` stays an operator concern (a downstream `set`).
//!
//! ## Permitted normalizations
//!
//! `prometheus_in -> prometheus_out` is a fixed point modulo exactly this list (the round-trip
//! tests in `crates/logit-proto/tests/prometheus_fixed_point.rs` pin it):
//!
//! - family and series reordering: families sorted by the name on their `# TYPE` line, series by
//!   label set, labels by rendered name;
//! - the scraper's own `instance` label, added the way Prometheus's own scrape adds it (above);
//! - the scrape target re-stamped: `_logit.prometheus.target` isn't written, so a scrape of the
//!   relay stamps the relay's URL and a remote-write receiver stamps none, and only the original
//!   target's host:port survives, as `instance`;
//! - the remote-write receiver's missing `_logit.metrics.closed: "timestamped"` flag: every sample
//!   keeps its timestamp, but the receiver stamps no flag
//!   ([`PrometheusDecoder::with_timestamp_marker`]);
//! - float formatting: shortest round-trip (`1.0` → `1`, `1.458255915e9` → `1458255915`);
//! - a counter's value sample gains `_total` when the model name lacks it (both dialects), and the
//!   OpenMetrics family name loses it -- see [`text`]'s own table;
//! - `# TYPE x untyped`/`unknown` emitted for a family that arrived with no metadata at all;
//! - `_created`, `# UNIT` and exemplars dropped when the *output* dialect is text 0.0.4, and the
//!   OpenMetrics-only family types rendered as their nearest text 0.0.4 shape (see [`text`]);
//! - `# EOF` present per dialect; blank lines, non-`HELP`/`TYPE`/`UNIT` comments dropped;
//! - two families whose `# TYPE` names coincide (a text 0.0.4 counter `foo` beside a gauge
//!   `foo_total`, or counters `foo` and `foo_total`) write once: the one whose model name sorts
//!   first, with each series of the other counted
//!   `logit.output.metrics.dropped{reason="name_collision"}`, since a second `# TYPE` line for a
//!   name makes Prometheus reject the whole scrape;
//! - a family with no line to write (every series stale, or a text 0.0.4 summary holding only
//!   `_created`) loses its `# HELP` and `# TYPE` lines too;
//! - a histogram's `+Inf` bucket is authoritative for the total: a `_count` line that disagrees is
//!   ignored rather than kept as a second, conflicting total the model has nowhere to put;
//! - a histogram exemplar sits on the bucket its own *value* falls in, which for a conforming
//!   producer is the bucket it arrived on and for a non-conforming one is a relocation;
//! - a wire `summary` with no `_sum`/`_count` re-emits `_sum 0`/`_count 0`: [`logit_core::Summary`]
//!   holds `sum: f64`/`count: u64`, not `Option`s, so "absent" and "zero" are the same model value.
//!   (This applies to a wire `summary` only: a `Distribution` or `Samples` builds its [`Point`]
//!   directly, with the sketch's own sum and count.)
//! - under the remote-write receiver's `peer:`, `proxy_protocol:`, or `forwarded:`,
//!   `network.peer.*` or `client.*` added to every event after decode, an opt-in addition the
//!   sender never sent, and one of ADR `lossless-transit`'s "Permitted normalizations";
//! - a label whose name is under the private `_logit.` prefix omitted, counted.
//!
//! Everything else is an error or a counted drop or skip, never a silent reinterpretation.

/// The committed, pre-generated prompb protobuf types (`docs/adr/committed-pregenerated-otlp-protobuf.md`'s
/// scheme, extended to this second proto family -- see `crates/logit-proto/proto/README.md`'s
/// "Vendored Prometheus prompb" section for the pinned tag/commit). Remote-write's own codec plugs
/// into this the way `text.rs` plugs into `otlp::generated`.
pub mod generated;

mod assemble;

pub mod compression;

pub mod remote_write;

pub mod text;

pub use text::Dialect;

use crate::otlp::metrics::SUMMARY_QUANTILES;
use logit_core::attrs::{
    is_closed_timestamped, is_private, metric_type, CLOSED_TIMESTAMPED, METRICS_CLOSED,
    METRICS_TYPE, TYPE_GAUGE_HISTOGRAM, TYPE_INFO, TYPE_STATESET,
};
use logit_core::interner::{intern, resolve};
use logit_core::{
    AttrMap, DdSketch, Diagnostics, Event, Exemplar, Histogram, MetricKind, MetricRecord,
    MetricValue, Resource, Sum, Summary, Telemetry, Temporality, Value,
};
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

/// The wire family type when the source declared none: `untyped` (text 0.0.4) or `unknown`
/// (OpenMetrics). `info`, `stateset`, and `gaugehistogram` ride as the neutral
/// [`METRICS_TYPE`] flag instead -- see the module doc's well-known-attribute section.
pub const ATTR_TYPE: &str = "_logit.prometheus.type";
/// The full scrape URL -- a **resource** attribute stamped by `prometheus_in`, private like every
/// other `_logit.prometheus.*` attribute.
pub const ATTR_TARGET: &str = "_logit.prometheus.target";
/// `host:port` of the scraped target: an **unprefixed** resource attribute stamped by
/// `prometheus_in`, *outside* the private `_logit.` namespace so it renders as
/// the `instance` label Prometheus's own scrape adds. Two targets exposing the same exporter stay
/// distinct series through a relay because of it; an event-level `instance` wins over the
/// resource's (`honor_labels` semantics), which the resource/event merge already gives.
pub const LABEL_INSTANCE: &str = "instance";

/// The bit pattern of the NaN Prometheus reserves for a **stale marker**: a sample whose value is
/// exactly this says "this series has gone away", and one whose value is any other NaN (including
/// `f64::NAN`) is an ordinary reading that happens to be NaN. The two are indistinguishable through
/// `==`, `is_nan()` or a float round trip -- only the bits tell them apart, which is why this is a
/// constant and [`is_stale_nan`] compares `to_bits`.
///
/// Prometheus' own `value.StaleNaN`. See the module doc's [`Point::Stale`] rows for the mapping in
/// both directions.
pub const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

/// Whether `value` is Prometheus' stale marker rather than an ordinary NaN -- see
/// [`STALE_NAN_BITS`].
pub fn is_stale_nan(value: f64) -> bool {
    value.to_bits() == STALE_NAN_BITS
}

/// A family's wire type. `Untyped` and `Unknown` are the same *semantics* (an unannotated sample)
/// under two dialect spellings, kept apart so `prometheus_in -> prometheus_out` re-emits the
/// spelling it received (`_logit.prometheus.type` carries it through the model). Each dialect
/// writes the other's spelling as its own -- see [`text`]'s module doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FamilyType {
    Counter,
    Gauge,
    Histogram,
    /// OpenMetrics only: a histogram of a quantity that can decrease (`_gsum`/`_gcount`).
    GaugeHistogram,
    Summary,
    /// OpenMetrics only: an always-`1` sample whose labels are the payload.
    Info,
    /// OpenMetrics only: one boolean sample per state.
    StateSet,
    /// OpenMetrics' spelling of "no type given".
    Unknown,
    /// Text 0.0.4's spelling of the same thing.
    Untyped,
}

impl FamilyType {
    /// The `# TYPE` keyword for this type in its own dialect -- also the [`ATTR_TYPE`] value for
    /// `untyped` and `unknown`, and the [`METRICS_TYPE`] value for `info`, `stateset`, and
    /// `gaugehistogram`.
    pub fn as_str(self) -> &'static str {
        match self {
            FamilyType::Counter => "counter",
            FamilyType::Gauge => "gauge",
            FamilyType::Histogram => "histogram",
            FamilyType::GaugeHistogram => "gaugehistogram",
            FamilyType::Summary => "summary",
            FamilyType::Info => "info",
            FamilyType::StateSet => "stateset",
            FamilyType::Unknown => "unknown",
            FamilyType::Untyped => "untyped",
        }
    }

    /// The inverse of [`FamilyType::as_str`] over a `# TYPE` keyword (`None` for an unrecognized
    /// one, which [`text`] treats as malformed metadata).
    pub fn from_keyword(s: &str) -> Option<FamilyType> {
        Some(match s {
            "counter" => FamilyType::Counter,
            "gauge" => FamilyType::Gauge,
            "histogram" => FamilyType::Histogram,
            "gaugehistogram" => FamilyType::GaugeHistogram,
            "summary" => FamilyType::Summary,
            "info" => FamilyType::Info,
            "stateset" => FamilyType::StateSet,
            "unknown" => FamilyType::Unknown,
            "untyped" => FamilyType::Untyped,
            _ => return None,
        })
    }

    /// Whether this type needs the [`ATTR_TYPE`] carrier to survive the model: the two spellings
    /// of "no type declared", which decode onto a plain `Gauge` and have no neutral meaning.
    fn needs_marker(self) -> bool {
        matches!(self, FamilyType::Untyped | FamilyType::Unknown)
    }

    /// The [`METRICS_TYPE`] value this type rides as: the three types that decode onto a plain
    /// `Gauge` or `Histogram` and name a kind of reading any producer can make.
    fn neutral_type(self) -> Option<&'static str> {
        match self {
            FamilyType::Info => Some(TYPE_INFO),
            FamilyType::StateSet => Some(TYPE_STATESET),
            FamilyType::GaugeHistogram => Some(TYPE_GAUGE_HISTOGRAM),
            _ => None,
        }
    }

    /// Whether this type carries a `_created` sample (OpenMetrics): counter, histogram and summary
    /// only, per the OpenMetrics spec -- a `gaugehistogram` explicitly has none.
    pub fn has_created(self) -> bool {
        matches!(self, FamilyType::Counter | FamilyType::Histogram | FamilyType::Summary)
    }
}

/// One sample point of a series, in wire terms: bucket counts are *cumulative* (`le`-style), the
/// way both dialects carry them, not the per-bucket counts [`logit_core::Histogram`] holds.
#[derive(Debug, Clone, PartialEq)]
pub enum Point {
    Counter(f64),
    Gauge(f64),
    /// `buckets` is cumulative, in ascending bound order with no `NaN` bound and no bound twice,
    /// and ends with a `(f64::INFINITY, total)` entry; `count` is that total restated (the
    /// `_count`/`_gcount` sample), never an independent number -- see the module doc's
    /// normalization list.
    Histogram {
        buckets: Vec<(f64, u64)>,
        sum: Option<f64>,
        count: u64,
    },
    /// `sum`/`count` are `Option` because OpenMetrics permits omitting them.
    Summary {
        quantiles: Vec<(f64, f64)>,
        sum: Option<f64>,
        count: Option<u64>,
    },
    /// `X_info{...} 1` -- the value is always `1`, so there is nothing to carry.
    Info,
    /// One state of a stateset: `X{X="state"} 0|1`.
    StateSet(bool),
    /// An `untyped`/`unknown` sample.
    Unknown(f64),
    /// A **stale marker**: this series has gone away, and the sample carries no reading at all.
    ///
    /// Prometheus spells this as a sample whose value is the specific NaN payload
    /// [`STALE_NAN_BITS`]; the model spells it as `MetricRecord::flags`'
    /// `FLAG_NO_RECORDED_VALUE` over an empty payload of the family's own kind (see the module
    /// doc's decode table). It is a [`Point`] variant rather than a `bool` on [`Series`] so that
    /// every exhaustive match in this codec has to consider it.
    ///
    /// Neither exposition dialect has a spelling for it: [`text::write`] skips a `Stale` series
    /// and counts `logit.output.metrics.skipped{reason="stale"}`. Only
    /// [`PrometheusEncoder::with_stale_markers`]`(true)` produces one.
    Stale,
}

/// One series of a family: a label set and its single point, plus the three per-series wire extras.
#[derive(Debug, Clone, PartialEq)]
pub struct Series {
    /// Sorted by label name, `le`/`quantile` excluded (those are part of [`Point`]).
    pub labels: Vec<(String, String)>,
    pub point: Point,
    /// Unix nanoseconds, when the sample carried its own timestamp.
    pub timestamp: Option<i64>,
    /// Unix nanoseconds, from an OpenMetrics `_created` sample.
    pub created: Option<i64>,
    /// OpenMetrics exemplars, in bucket order for a histogram. The writer places each one on the
    /// bucket its own value falls in.
    pub exemplars: Vec<Exemplar>,
    /// The reading is an increment or an observation set, such as a `Samples` record written as
    /// a summary, so a later equal reading doesn't replace it: remote-write's collapse counts it
    /// lost even when equal ([`same_reading`]). Never on the wire, and `false` from every decoder.
    pub increment: bool,
}

impl Series {
    /// A series with no timestamp, no `_created`, and no exemplars -- what most producers and
    /// nearly every test want.
    pub fn new(labels: Vec<(String, String)>, point: Point) -> Self {
        Series {
            labels,
            point,
            timestamp: None,
            created: None,
            exemplars: Vec::new(),
            increment: false,
        }
    }
}

/// A metric family: one name, one type, one metadata pair, N series. The syntax-independent seam
/// between [`text`]/[`remote_write`] and the model mapping in this module -- see the module doc's
/// "Family naming" table for what `name` means.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricFamily {
    pub name: String,
    pub kind: FamilyType,
    /// `# HELP`, unescaped. An empty `# HELP` decodes as `None`, not `Some("")`.
    pub help: Option<String>,
    /// `# UNIT` (OpenMetrics only).
    pub unit: Option<String>,
    /// Sorted by label set.
    pub series: Vec<Series>,
}

impl MetricFamily {
    /// An empty family -- `help`/`unit` `None`, no series.
    pub fn new(name: impl Into<String>, kind: FamilyType) -> Self {
        MetricFamily { name: name.into(), kind, help: None, unit: None, series: Vec::new() }
    }
}

/// Counts the decode side's drops (and carries the throttled-diagnostics handle a parse needs).
/// `Telemetry::default()`/`Diagnostics::default()` make every call a no-op, so a decoder used as a
/// pure codec -- [`text::parse`] with no component attached -- costs nothing to carry.
pub struct PrometheusDecoder {
    telemetry: Telemetry,
    diagnostics: Diagnostics,
    timestamp_marker: bool,
}

impl Default for PrometheusDecoder {
    fn default() -> Self {
        PrometheusDecoder {
            telemetry: Telemetry::default(),
            diagnostics: Diagnostics::default(),
            timestamp_marker: true,
        }
    }
}

impl PrometheusDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether [`families_to_events`] stamps the flag `_logit.metrics.closed: "timestamped"` on a
    /// series that carried its own timestamp. Default `true`, for a scrape: an exposition sample's
    /// timestamp is optional, so its presence is a producer choice worth preserving across a round
    /// trip.
    ///
    /// A remote-write receiver sets this `false`. Every remote-write sample carries a timestamp, so
    /// the flag would record a choice nobody made, and a remote-write → exposition relay would
    /// then stamp a timestamp on every line it writes, which no scrape of the same data would.
    /// `Event::timestamp` is set from the sample either way.
    pub fn with_timestamp_marker(mut self, marker: bool) -> Self {
        self.timestamp_marker = marker;
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn with_diagnostics(mut self, diagnostics: Diagnostics) -> Self {
        self.diagnostics = diagnostics;
        self
    }

    /// `logit.input.metrics.dropped{reason}` -- one greppable counter for every sample or series
    /// the decoder stepped over.
    pub(crate) fn dropped(&self, reason: DecodeDrop) {
        self.telemetry.count("logit.input.metrics.dropped", 1.0, &[("reason", reason.reason())]);
    }

    /// `logit.input.metrics.degraded{reason}` -- the input was kept, but something in it was
    /// normalized away (a histogram whose `_count` disagreed with its `+Inf` bucket, a dropped
    /// exemplar, a seeded type the samples contradicted).
    pub(crate) fn degraded(&self, reason: &'static str) {
        self.telemetry.count("logit.input.metrics.degraded", 1.0, &[("reason", reason)]);
    }

    pub(crate) fn diagnostics(&mut self) -> &mut Diagnostics {
        &mut self.diagnostics
    }
}

/// Why the decode side dropped a sample or series, each counted
/// `logit.input.metrics.dropped{reason}`. Every reason is malformed input or a kind this codec
/// can't decode, so every one is loss; the module docs of [`assemble`], [`text`], and
/// [`remote_write`] say when each applies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DecodeDrop {
    MalformedLine,
    MalformedMetadata,
    DuplicateType,
    DuplicateMetadata,
    DuplicateSeries,
    DuplicateLabel,
    UnknownSuffix,
    IncompleteSeries,
    EmptyHistogram,
    NonMonotonicBuckets,
    InvalidLabels,
    NativeHistogram,
}

impl DecodeDrop {
    #[cfg(test)]
    const ALL: [DecodeDrop; 12] = [
        DecodeDrop::MalformedLine,
        DecodeDrop::MalformedMetadata,
        DecodeDrop::DuplicateType,
        DecodeDrop::DuplicateMetadata,
        DecodeDrop::DuplicateSeries,
        DecodeDrop::DuplicateLabel,
        DecodeDrop::UnknownSuffix,
        DecodeDrop::IncompleteSeries,
        DecodeDrop::EmptyHistogram,
        DecodeDrop::NonMonotonicBuckets,
        DecodeDrop::InvalidLabels,
        DecodeDrop::NativeHistogram,
    ];

    fn reason(self) -> &'static str {
        match self {
            DecodeDrop::MalformedLine => "malformed_line",
            DecodeDrop::MalformedMetadata => "malformed_metadata",
            DecodeDrop::DuplicateType => "duplicate_type",
            DecodeDrop::DuplicateMetadata => "duplicate_metadata",
            DecodeDrop::DuplicateSeries => "duplicate_series",
            DecodeDrop::DuplicateLabel => "duplicate_label",
            DecodeDrop::UnknownSuffix => "unknown_suffix",
            DecodeDrop::IncompleteSeries => "incomplete_series",
            DecodeDrop::EmptyHistogram => "empty_histogram",
            DecodeDrop::NonMonotonicBuckets => "non_monotonic_buckets",
            DecodeDrop::InvalidLabels => "invalid_labels",
            DecodeDrop::NativeHistogram => "native_histogram",
        }
    }
}

/// Why the encode side didn't write a record or series, and the counter that says so. A
/// `*.skipped` outcome had nothing to carry or no form in the destination's format; a `*.dropped`
/// one is loss a different config or a later `logit` would have delivered
/// ([ADR `telemetry-loss-vocabulary`](../../../../docs/adr/telemetry-loss-vocabulary.md)).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NotEncoded {
    /// A `NO_RECORDED_VALUE` record with no stale marker to become.
    NoRecordedValue,
    /// A [`Point::Stale`] series in an exposition body, which has no stale-marker syntax.
    Stale,
    /// An `ExponentialHistogram` bound for exposition, whose text dialects have no syntax for it.
    ExponentialHistogram,
    /// An `ExponentialHistogram` bound for remote-write, which carries native histograms this
    /// codec doesn't map yet.
    NativeHistogram,
    /// A delta `Sum` or `Histogram` with no cumulative `aggregate` upstream, or a delta
    /// `Summary`, which only `cumulative_to_delta` makes.
    UnresolvedDelta,
    /// A `GaugeDelta` with no `aggregate` upstream.
    UnresolvedGaugeDelta,
    TypeConflict,
    NameCollision,
    InvalidLabels,
    NoTimestamp,
    /// A remote-write reading another reading of its series replaced at the same millisecond.
    DuplicateTimestamp,
    /// A remote-write state replaced at the same millisecond by a state equal to it
    /// ([`same_reading`]): nothing is lost, since the receiver stores the same sample.
    EqualDuplicateTimestamp,
}

impl NotEncoded {
    #[cfg(test)]
    const ALL: [NotEncoded; 12] = [
        NotEncoded::NoRecordedValue,
        NotEncoded::Stale,
        NotEncoded::ExponentialHistogram,
        NotEncoded::NativeHistogram,
        NotEncoded::UnresolvedDelta,
        NotEncoded::UnresolvedGaugeDelta,
        NotEncoded::TypeConflict,
        NotEncoded::NameCollision,
        NotEncoded::InvalidLabels,
        NotEncoded::NoTimestamp,
        NotEncoded::DuplicateTimestamp,
        NotEncoded::EqualDuplicateTimestamp,
    ];

    /// The counter name and its one tag.
    fn counter(self) -> (&'static str, (&'static str, &'static str)) {
        const SKIPPED: &str = "logit.output.metrics.skipped";
        const DROPPED: &str = "logit.output.metrics.dropped";
        const NORMALIZED: &str = "logit.output.metrics.normalized";
        match self {
            NotEncoded::NoRecordedValue => (SKIPPED, ("reason", "no_recorded_value")),
            NotEncoded::Stale => (SKIPPED, ("reason", "stale")),
            NotEncoded::ExponentialHistogram => (SKIPPED, ("metric_kind", "exponential_histogram")),
            NotEncoded::NativeHistogram => (DROPPED, ("reason", "native_histogram")),
            NotEncoded::UnresolvedDelta => (DROPPED, ("reason", "unresolved_delta")),
            NotEncoded::UnresolvedGaugeDelta => (DROPPED, ("reason", "unresolved_gauge_delta")),
            NotEncoded::TypeConflict => (DROPPED, ("reason", "type_conflict")),
            NotEncoded::NameCollision => (DROPPED, ("reason", "name_collision")),
            NotEncoded::InvalidLabels => (DROPPED, ("reason", "invalid_labels")),
            NotEncoded::NoTimestamp => (DROPPED, ("reason", "no_timestamp")),
            NotEncoded::DuplicateTimestamp => (DROPPED, ("reason", "duplicate_timestamp")),
            NotEncoded::EqualDuplicateTimestamp => (NORMALIZED, ("reason", "duplicate_timestamp")),
        }
    }
}

/// Which wire an encode feeds. It decides how an `ExponentialHistogram` is counted: exposition
/// has no syntax for one, and remote-write has a native-histogram field this codec doesn't fill.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Destination {
    #[default]
    Exposition,
    RemoteWrite,
}

/// Counts the encode side's lossy paths -- the mirror of [`PrometheusDecoder`], same builders, same
/// disabled-by-default no-op behavior as [`crate::otlp::OtlpEncoder`].
#[derive(Default)]
pub struct PrometheusEncoder {
    telemetry: Telemetry,
    diagnostics: Diagnostics,
    timestamps_always: bool,
    stale_markers: bool,
    destination: Destination,
    /// While set, counts go here instead of to `telemetry`: one series' conversion in
    /// [`events_to_families_pending`].
    pending: RefCell<Option<PendingCounts>>,
}

impl PrometheusEncoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether every series gets `Series::timestamp = Some(event.timestamp)` regardless of the
    /// `_logit.metrics.closed: "timestamped"` flag. Default `false`, the mirror of
    /// [`PrometheusDecoder::with_timestamp_marker`]: exposition timestamps are optional, so only a
    /// sample that arrived with one gets one back.
    ///
    /// A remote-write sender sets this `true`, because that wire has no way to omit a timestamp.
    pub fn with_timestamps_always(mut self, always: bool) -> Self {
        self.timestamps_always = always;
        self
    }

    /// Whether a record carrying `FLAG_NO_RECORDED_VALUE` becomes a [`Point::Stale`] series instead
    /// of being skipped. Default `false`, for exposition: neither dialect can express a stale
    /// marker, and a bare NaN sample would read as a real reading of NaN.
    ///
    /// A remote-write sender sets this `true`. Only single-series kinds convert (`Gauge`, `Sum`,
    /// and a gauge of any family type); a flagged `Histogram`, `Summary` or sketch
    /// kind stays skipped and counted, for the reason the module doc's encode table gives.
    pub fn with_stale_markers(mut self, stale: bool) -> Self {
        self.stale_markers = stale;
        self
    }

    /// The wire this encode feeds. Default [`Destination::Exposition`]; a remote-write sender sets
    /// [`Destination::RemoteWrite`].
    pub fn with_destination(mut self, destination: Destination) -> Self {
        self.destination = destination;
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn with_diagnostics(mut self, diagnostics: Diagnostics) -> Self {
        self.diagnostics = diagnostics;
        self
    }

    /// Counts one record or series the encoder didn't write, under the counter
    /// [`NotEncoded::counter`] names.
    fn not_encoded(&self, outcome: NotEncoded) {
        let (metric, tag) = outcome.counter();
        self.count(metric, tag);
    }

    /// One count, held back while a conversion is captured.
    fn count(&self, metric: &'static str, tag: (&'static str, &'static str)) {
        if let Some(pending) = self.pending.borrow_mut().as_mut() {
            pending.0.push((metric, tag));
            return;
        }
        self.telemetry.count(metric, 1.0, &[tag]);
    }

    /// Starts holding back counts.
    fn capture(&self) {
        *self.pending.borrow_mut() = Some(PendingCounts::default());
    }

    /// Stops holding back counts, returning them.
    fn release(&self) -> PendingCounts {
        self.pending.borrow_mut().take().unwrap_or_default()
    }

    /// Counts what a conversion held back.
    fn flush(&self, counts: PendingCounts) {
        for (metric, tag) in counts.0 {
            self.telemetry.count(metric, 1.0, &[tag]);
        }
    }

    fn degraded(&self, metric_kind: &'static str) {
        self.count("logit.output.metrics.degraded", ("metric_kind", metric_kind));
    }

    /// A scalar's sample value. A sample is a double on both wires, so an integer leaves as its
    /// nearest `f64` and a `Bool` as `1` or `0`, counted.
    fn scalar(&self, value: MetricValue) -> f64 {
        if let MetricValue::Bool(_) = value {
            self.count("logit.output.metrics.normalized", ("reason", "bool_value"));
        }
        value.as_f64()
    }

    /// `logit.output.metrics.degraded{reason}` -- the same counter keyed by *why* rather than by
    /// which model kind, for a degradation the wire format forces on a metric of any kind (a
    /// `# UNIT` the format won't accept, an exemplar with nowhere to sit).
    fn degraded_reason(&self, reason: &'static str) {
        self.count("logit.output.metrics.degraded", ("reason", reason));
    }

    /// `logit.output.events.skipped{reason}`: an event the encoder wrote nothing for, once per
    /// event. The one reason is `no_metrics`: a log, a span, or neither, with no metric record.
    fn event_skipped(&self, reason: &'static str) {
        self.telemetry.count("logit.output.events.skipped", 1.0, &[("reason", reason)]);
    }

    /// `logit.output.metrics.normalized{reason="name_sanitized"}`: a record written under a name
    /// [`sanitize_metric_name`] rewrote.
    fn name_sanitized(&self) {
        self.count("logit.output.metrics.normalized", ("reason", "name_sanitized"));
    }

    fn label_omitted(&self, reason: &'static str) {
        self.count("logit.output.labels.omitted", ("reason", reason));
    }

    /// `logit.output.labels.normalized{reason}` -- a label that reached the wire, but not in the
    /// form the model held it: `name_sanitized`, a series or exemplar label whose name
    /// [`sanitize_label_name`] rewrote, or `multi_value`, a `Value::Array` attribute collapsed to
    /// its last representable element by [`build_labels`], because a Prometheus label set is a map
    /// and has no multi-value label at all.
    ///
    /// **`multi_value` is lossy**, unlike every other `*.normalized` reason in
    /// `docs/design/internal-telemetry.md`: those are a lossless-but-different rendering of the
    /// same information, whereas this discards the non-last elements outright. It is `normalized`
    /// rather than `dropped` because the label itself survives and the series still exposes -- but
    /// an operator reading this counter should read it as data loss.
    fn label_normalized(&self, reason: &'static str) {
        self.count("logit.output.labels.normalized", ("reason", reason));
    }
}

// -------------------------------------------------------------------------------------------------
// Decode: families -> events
// -------------------------------------------------------------------------------------------------

/// Families → events, one [`Event`] per [`Series`] (see the module doc's decode table).
/// `received_at_nanos` is the scrape's start instant: every series that carried no timestamp of its
/// own is stamped with it, the same contract [`crate::Decoder::decode_into`]'s `received_at`
/// documents for every other input.
pub fn families_to_events(
    families: &[MetricFamily],
    received_at_nanos: i64,
    decoder: &mut PrometheusDecoder,
) -> Vec<Event> {
    families_to_events_with(families, received_at_nanos, decoder, &mut |_, _| {})
}

/// [`families_to_events`] plus a report of what it stepped over: `on_dropped` is called once per
/// [`Series`] that has no model kind at all (an empty histogram, a histogram whose cumulative
/// bucket counts decrease -- see [`point_to_kind`]), with the family it belonged to.
///
/// The counted-drop telemetry is already emitted either way; this exists for a caller that has to
/// report, per call, *how much* of its input survived -- a remote-write receiver owes its sender
/// an `X-Prometheus-Remote-Write-Samples-Written` count of what it kept, and the
/// assembler's own accepted total is a statement about an earlier stage than this one. Pair it with
/// [`remote_write::wire_samples`] to turn the dropped series back into the wire samples they were.
pub fn families_to_events_with(
    families: &[MetricFamily],
    received_at_nanos: i64,
    decoder: &mut PrometheusDecoder,
    on_dropped: &mut dyn FnMut(&MetricFamily, &Series),
) -> Vec<Event> {
    let mut out = Vec::new();
    let mut reserved = 0usize;
    for family in families {
        let name = intern(&family.name);
        let unit = family.unit.as_deref().map(intern);
        let description = family.help.as_deref().map(intern);
        for series in &family.series {
            let Some((kind, flags)) = point_to_kind(&series.point, family.kind, decoder) else {
                on_dropped(family, series);
                continue;
            };
            let mut attributes = AttrMap::new();
            for (label, value) in &series.labels {
                if is_private(label) {
                    reserved += 1;
                    continue;
                }
                attributes.insert(label, Value::str(value.as_str()));
            }
            if family.kind.needs_marker() {
                attributes.insert(ATTR_TYPE, Value::str(family.kind.as_str()));
            } else if let Some(neutral) = family.kind.neutral_type() {
                attributes.insert(METRICS_TYPE, Value::static_str(neutral));
            }
            let timestamp = match series.timestamp {
                Some(ts) => {
                    if decoder.timestamp_marker {
                        attributes.insert(METRICS_CLOSED, Value::static_str(CLOSED_TIMESTAMPED));
                    }
                    ts
                }
                None => received_at_nanos,
            };
            let record = MetricRecord {
                name,
                unit,
                description,
                start_timestamp: series.created.unwrap_or(0),
                exemplars: series.exemplars.clone(),
                flags,
                kind,
            };
            out.push(Event::metric(timestamp, attributes, record));
        }
    }
    if reserved > 0 {
        decoder.telemetry.count(
            "logit.input.tags.omitted",
            reserved as f64,
            &[("reason", "reserved_prefix")],
        );
    }
    out
}

/// One [`Point`] → one [`MetricKind`] and the record `flags` that go with it, or `None` for a
/// series this codec steps over (counted). The cumulative → per-bucket conversion lives here rather
/// than in [`text`] because it is a model rule, not a syntax one: both syntaxes share it.
///
/// `family` is the point's own family type, which only [`Point::Stale`] needs: a stale marker has
/// no payload of its own, so the kind it takes is the one the family would have produced anyway.
fn point_to_kind(
    point: &Point,
    family: FamilyType,
    decoder: &mut PrometheusDecoder,
) -> Option<(MetricKind, u32)> {
    if matches!(point, Point::Stale) {
        return Some((stale_kind(family), MetricRecord::FLAG_NO_RECORDED_VALUE));
    }
    Some((
        match point {
            Point::Counter(v) => MetricKind::Sum(Sum {
                value: MetricValue::F64(*v),
                temporality: Temporality::Cumulative,
                monotonic: true,
            }),
            Point::Gauge(v) | Point::Unknown(v) => MetricKind::gauge(*v),
            Point::Info => MetricKind::gauge(1.0),
            Point::StateSet(on) => MetricKind::gauge(if *on { 1.0 } else { 0.0 }),
            Point::Histogram { buckets, sum, .. } => {
                let buckets = per_bucket_counts(buckets, decoder)?;
                MetricKind::Histogram(Histogram {
                    buckets,
                    temporality: Temporality::Cumulative,
                    sum: *sum,
                    min: None,
                    max: None,
                })
            }
            Point::Summary { quantiles, sum, count } => MetricKind::Summary(Summary {
                quantiles: quantiles.clone(),
                count: count.unwrap_or(0),
                sum: sum.unwrap_or(0.0),
                temporality: Temporality::Cumulative,
            }),
            // Handled above, before any of the value-bearing arms can be reached.
            Point::Stale => unreachable!("stale markers return early"),
        },
        0,
    ))
}

/// The empty payload a [`Point::Stale`] takes for its family's type. The model has no "there is no
/// reading" variant -- `FLAG_NO_RECORDED_VALUE` on the record is what says that -- so the kind has
/// to be *something*, and the something that loses the least is the kind the family would have
/// produced had the series carried a value: a downstream `aggregate` or sink sees the same shape
/// before and after a series goes stale, and the encode side can put the marker back on the right
/// family type without guessing.
///
/// `NaN` rather than `0` for the scalar kinds, because `0` is a perfectly good counter reading and
/// a consumer that ignores the flag would read a reset.
fn stale_kind(family: FamilyType) -> MetricKind {
    match family {
        FamilyType::Counter => MetricKind::Sum(Sum {
            value: MetricValue::F64(f64::NAN),
            temporality: Temporality::Cumulative,
            monotonic: true,
        }),
        FamilyType::Histogram | FamilyType::GaugeHistogram => MetricKind::Histogram(Histogram {
            buckets: Vec::new(),
            temporality: Temporality::Cumulative,
            sum: None,
            min: None,
            max: None,
        }),
        FamilyType::Summary => MetricKind::Summary(Summary {
            quantiles: Vec::new(),
            count: 0,
            sum: 0.0,
            temporality: Temporality::Cumulative,
        }),
        FamilyType::Gauge
        | FamilyType::Info
        | FamilyType::StateSet
        | FamilyType::Unknown
        | FamilyType::Untyped => MetricKind::gauge(f64::NAN),
    }
}

/// Cumulative `le` counts → [`logit_core::Histogram`]'s per-bucket counts, by successive
/// difference. A family with no buckets at all, or whose cumulative counts decrease (which no
/// conforming exposition produces and which would yield a negative bucket count), is dropped:
/// `logit.input.metrics.dropped{reason="empty_histogram"|"non_monotonic_buckets"}`. Bucket bounds
/// are assumed sorted ascending -- [`text::parse`] sorts them, and so does the encode side.
fn per_bucket_counts(
    cumulative: &[(f64, u64)],
    decoder: &mut PrometheusDecoder,
) -> Option<Vec<(f64, u64)>> {
    if cumulative.is_empty() {
        decoder.dropped(DecodeDrop::EmptyHistogram);
        return None;
    }
    let mut buckets = Vec::with_capacity(cumulative.len());
    let mut running = 0u64;
    for (bound, cumulative_count) in cumulative {
        let Some(delta) = cumulative_count.checked_sub(running) else {
            decoder.dropped(DecodeDrop::NonMonotonicBuckets);
            decoder.diagnostics().warn_throttled(
                "prometheus_non_monotonic_buckets",
                format_args!(
                    "histogram bucket counts decrease at le={bound} -- the series is not \
                     cumulative; dropping it"
                ),
            );
            return None;
        };
        running = *cumulative_count;
        buckets.push((*bound, delta));
    }
    Some(buckets)
}

// -------------------------------------------------------------------------------------------------
// Encode: events -> families
// -------------------------------------------------------------------------------------------------

/// Events → families (see the module doc's encode table). Takes `(resource, event)` pairs rather
/// than an `EventBatch` so a stateful sink can feed several batches' worth of events -- or a
/// registry's worth of retained ones -- through one call without rebuilding a batch it doesn't
/// have.
///
/// The result is canonically ordered: families sorted by name, series by label set, labels by name.
pub fn events_to_families<'a>(
    events: impl Iterator<Item = (&'a Resource, &'a Event)>,
    encoder: &mut PrometheusEncoder,
) -> Vec<MetricFamily> {
    events_to_families_pending(events, encoder)
        .into_iter()
        .map(|pending| {
            for counts in pending.pending {
                encoder.flush(counts);
            }
            pending.family
        })
        .collect()
}

/// [`events_to_families`], holding back what each surviving [`Series`]' conversion counted (a
/// degrade, a normalization, an omitted label) on [`PendingFamily`], for a caller that may still
/// drop the series: [`remote_write::encode_pending`] collapses readings of one series at one
/// millisecond, and a reading it drops counts only its `duplicate_timestamp`. What a record
/// that leaves no series counts, and a series this call replaces, is settled here.
pub fn events_to_families_pending<'a>(
    events: impl Iterator<Item = (&'a Resource, &'a Event)>,
    encoder: &mut PrometheusEncoder,
) -> Vec<PendingFamily> {
    // Keyed by the *emitted* (sanitized) name, not the model name: two model names that sanitize
    // onto one cannot both be exposed -- a second `# TYPE` line for one name makes Prometheus reject
    // the whole scrape, so one naming clash would poison every other metric in the body.
    let mut families: BTreeMap<String, FamilyEntry> = BTreeMap::new();
    for (resource, event) in events {
        if event.metrics.is_empty() {
            encoder.event_skipped("no_metrics");
        }
        for record in &event.metrics {
            let stale = record.is_no_recorded_value();
            if stale && !encoder.stale_markers {
                encoder.not_encoded(NotEncoded::NoRecordedValue);
                continue;
            }
            encoder.capture();
            let converted = if stale {
                stale_point(record, resource, event, encoder)
            } else {
                record_to_point(record, resource, event, encoder)
            };
            let Some((kind, point)) = converted else {
                let counts = encoder.release();
                encoder.flush(counts);
                continue;
            };
            let name = resolve(record.name);
            // Borrows `name` whenever it already conforms, which is nearly always -- one allocation
            // per *family*, not per point, and only for a name that has to change.
            let key = sanitize_metric_name(name);
            let labels = build_labels(resource, event, kind, encoder);
            if matches!(key, Cow::Owned(_)) {
                encoder.name_sanitized();
            }
            let counts = encoder.release();
            let timestamp = if encoder.timestamps_always {
                Some(event.timestamp)
            } else {
                is_closed_timestamped(resource, event).then_some(event.timestamp)
            };
            let created = if kind.has_created() && record.start_timestamp != 0 {
                Some(record.start_timestamp)
            } else {
                None
            };
            let mut series = Some(Series {
                labels,
                point,
                timestamp,
                created,
                exemplars: record.exemplars.clone(),
                increment: !crate::replaced_by_a_later_reading(&record.kind)
                    || logit_core::attrs::is_rate(resource, event, &record.kind),
            });

            // One map probe on the steady-state path (a family this record already belongs to); the
            // second write happens only when a family is created or displaced.
            let mut start_family = false;
            let mut displaced = 0;
            match families.get_mut(key.as_ref()) {
                Some(existing) if existing.origin == name => {
                    if existing.family.kind != kind {
                        // Two records sharing one name but disagreeing on type: the wire has exactly
                        // one `# TYPE` line per name, so the second one has nowhere to go. Its drop
                        // is its whole fate, so its held-back counts go with it.
                        drop(counts);
                        encoder.not_encoded(NotEncoded::TypeConflict);
                    } else {
                        if !counts.0.is_empty() {
                            existing.pending.push((existing.family.series.len(), counts));
                        }
                        existing.family.series.push(series.take().expect("built above"));
                    }
                    continue;
                }
                // A different model name sanitizing onto the same wire name: the one whose model
                // name sorts first wins, so the outcome is the data's, not the arrival order's.
                Some(existing) if name < existing.origin => {
                    // The displaced family's series are dropped whole, held-back counts too.
                    displaced = existing.family.series.len().max(1);
                    existing.pending.clear();
                    start_family = true;
                }
                Some(_) => encoder.not_encoded(NotEncoded::NameCollision),
                None => start_family = true,
            }
            if !start_family {
                continue;
            }
            let series = series.take().expect("only the matching-family arm consumes it");
            for _ in 0..displaced {
                encoder.not_encoded(NotEncoded::NameCollision);
            }
            let key = key.into_owned();
            families.insert(
                key.clone(),
                FamilyEntry {
                    origin: name,
                    family: MetricFamily {
                        name: key,
                        kind,
                        help: record.description.map(|s| resolve(s).to_string()),
                        unit: record.unit.map(|s| resolve(s).to_string()),
                        series: vec![series],
                    },
                    pending: if counts.0.is_empty() { Vec::new() } else { vec![(0, counts)] },
                },
            );
        }
    }

    // The map is keyed by the emitted name, so draining it in key order is already the canonical
    // family ordering -- no re-sort needed.
    let mut replaced = Replaced::default();
    let mut out: Vec<PendingFamily> = Vec::with_capacity(families.len());
    for entry in families.into_values() {
        if entry.pending.is_empty() {
            // Nothing held back: the latest-wins pass alone, with no counts to carry.
            let mut family = entry.family;
            family.series.sort_by(|a, b| a.labels.cmp(&b.labels));
            // Walked from the end, so the first of a run of equal label sets met is the latest,
            // the one kept, and each earlier one is compared with it.
            let mut deduped: Vec<Series> = Vec::with_capacity(family.series.len());
            for series in family.series.drain(..).rev() {
                match deduped.last() {
                    Some(kept) if kept.labels == series.labels => replaced.add(&series, kept),
                    _ => deduped.push(series),
                }
            }
            deduped.reverse();
            family.series = deduped;
            out.push(PendingFamily { family, pending: Vec::new() });
            continue;
        }
        let MetricFamily { name, kind, help, unit, series } = entry.family;
        let mut dense: Vec<PendingCounts> = vec![PendingCounts::default(); series.len()];
        for (index, counts) in entry.pending {
            dense[index] = counts;
        }
        let mut paired: Vec<(Series, PendingCounts)> = series.into_iter().zip(dense).collect();
        // Stable, so the last of a run of equal label sets is the latest, and walked from the end,
        // so it's the one kept -- latest wins, the cumulative-series semantics `prometheus_out`'s
        // registry upsert also uses. A replaced series' own counts go with it.
        paired.sort_by(|a, b| a.0.labels.cmp(&b.0.labels));
        let mut deduped: Vec<(Series, PendingCounts)> = Vec::with_capacity(paired.len());
        for item in paired.into_iter().rev() {
            match deduped.last() {
                Some(kept) if kept.0.labels == item.0.labels => replaced.add(&item.0, &kept.0),
                _ => deduped.push(item),
            }
        }
        deduped.reverse();
        let (series, pending) = deduped.into_iter().unzip();
        out.push(PendingFamily {
            family: MetricFamily { name, kind, help, unit, series },
            pending,
        });
    }
    // A remote-write sender calls this once per timestamp, so a replaced series is a reading lost
    // at its instant, unless it's a state equal to the one kept. The exposition registry keeps one value per
    // series between scrapes by design, so there it's the pull model's overwrite, uncounted.
    if encoder.destination == Destination::RemoteWrite {
        count_duplicates(encoder, replaced.differing, replaced.equal);
    }
    out
}

/// One [`MetricFamily`] from [`events_to_families_pending`], with the counts each of its series'
/// conversion is holding back, in series order, or none at all when no series holds any.
#[derive(Debug, Clone)]
pub struct PendingFamily {
    pub family: MetricFamily,
    pending: Vec<PendingCounts>,
}

/// The counts one series' conversion holds back until the series is known to survive.
#[derive(Debug, Clone, Default)]
pub struct PendingCounts(Vec<(&'static str, (&'static str, &'static str))>);

/// How many readings a remote-write collapse removed, by whether each equals the reading kept for
/// its series and millisecond ([`same_reading`]).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Replaced {
    pub(crate) differing: usize,
    pub(crate) equal: usize,
}

impl Replaced {
    /// Counts `removed`, which `kept` replaces.
    pub(crate) fn add(&mut self, removed: &Series, kept: &Series) {
        if same_reading(removed, kept) {
            self.equal += 1;
        } else {
            self.differing += 1;
        }
    }
}

/// Whether two readings of one family and label set are one state on the remote-write wire:
/// neither an increment ([`Series::increment`]), the same point, each `f64` by its bits (as
/// Prometheus's own duplicate check compares them), the same created timestamp at the wire's
/// millisecond, and the same exemplars, which go with a removed reading. A value's model type
/// isn't compared: every sample is a double.
pub(crate) fn same_reading(a: &Series, b: &Series) -> bool {
    let millis = |created: Option<i64>| created.map(|c| c / 1_000_000);
    !a.increment
        && !b.increment
        && same_point(&a.point, &b.point)
        && millis(a.created) == millis(b.created)
        && a.exemplars == b.exemplars
}

fn same_point(a: &Point, b: &Point) -> bool {
    let f = |x: f64, y: f64| x.to_bits() == y.to_bits();
    let sum = |x: Option<f64>, y: Option<f64>| match (x, y) {
        (Some(x), Some(y)) => f(x, y),
        (x, y) => x.is_none() && y.is_none(),
    };
    match (a, b) {
        (Point::Counter(x), Point::Counter(y))
        | (Point::Gauge(x), Point::Gauge(y))
        | (Point::Unknown(x), Point::Unknown(y)) => f(*x, *y),
        (
            Point::Histogram { buckets: ab, sum: asum, count: ac },
            Point::Histogram { buckets: bb, sum: bsum, count: bc },
        ) => {
            ac == bc
                && sum(*asum, *bsum)
                && ab.len() == bb.len()
                && ab.iter().zip(bb).all(|(x, y)| f(x.0, y.0) && x.1 == y.1)
        }
        (
            Point::Summary { quantiles: aq, sum: asum, count: ac },
            Point::Summary { quantiles: bq, sum: bsum, count: bc },
        ) => {
            ac == bc
                && sum(*asum, *bsum)
                && aq.len() == bq.len()
                && aq.iter().zip(bq).all(|(x, y)| f(x.0, y.0) && f(x.1, y.1))
        }
        (Point::StateSet(x), Point::StateSet(y)) => x == y,
        (Point::Info, Point::Info) | (Point::Stale, Point::Stale) => true,
        _ => false,
    }
}

/// Counts the readings a remote-write collapse removed: `differing` ones as lost, with the
/// diagnostic, and `equal` ones as normalized, since the receiver stores the same sample.
fn count_duplicates(encoder: &mut PrometheusEncoder, differing: usize, equal: usize) {
    for _ in 0..equal {
        encoder.not_encoded(NotEncoded::EqualDuplicateTimestamp);
    }
    if differing == 0 {
        return;
    }
    for _ in 0..differing {
        encoder.not_encoded(NotEncoded::DuplicateTimestamp);
    }
    warn_duplicate_timestamp(encoder, differing);
}

/// The diagnostic both remote-write collapses report under: `n` readings of a series dropped
/// because a later, different one shared its millisecond.
fn warn_duplicate_timestamp(encoder: &mut PrometheusEncoder, n: usize) {
    encoder.diagnostics.warn_throttled(
        "duplicate_timestamp",
        format_args!(
            "dropped {n} reading(s) that shared a series and a millisecond with a later, \
             different one, which remote-write can't carry -- add a `dedup_timestamps` \
             component with `precision: 1ms` before this output, or an `aggregate` for a live \
             delta flow"
        ),
    );
}

/// A family under construction plus the model name that claimed it -- the two together are what
/// resolves a post-sanitization name collision deterministically (see [`events_to_families`]).
struct FamilyEntry {
    origin: &'static str,
    family: MetricFamily,
    /// The held-back counts of each series that has any, by its index in `family.series`.
    pending: Vec<(usize, PendingCounts)>,
}

/// One [`MetricRecord`] → its family type and wire point, or `None` for a kind Prometheus can't
/// carry (counted, see the module doc's encode table).
fn record_to_point(
    record: &MetricRecord,
    resource: &Resource,
    event: &Event,
    encoder: &mut PrometheusEncoder,
) -> Option<(FamilyType, Point)> {
    Some(match &record.kind {
        MetricKind::Sum(s) => match (s.temporality, s.monotonic) {
            (Temporality::Cumulative, true) => {
                (FamilyType::Counter, Point::Counter(encoder.scalar(s.value)))
            }
            (Temporality::Cumulative, false) => {
                encoder.degraded("non_monotonic_sum");
                (FamilyType::Gauge, Point::Gauge(encoder.scalar(s.value)))
            }
            (Temporality::Delta, _) => {
                encoder.not_encoded(NotEncoded::UnresolvedDelta);
                warn_delta(encoder, resolve(record.name));
                return None;
            }
        },
        MetricKind::Gauge(v) => match gauge_family_type(resource, event) {
            FamilyType::Untyped => (FamilyType::Untyped, Point::Unknown(encoder.scalar(*v))),
            FamilyType::Unknown => (FamilyType::Unknown, Point::Unknown(encoder.scalar(*v))),
            FamilyType::Info => (FamilyType::Info, Point::Info),
            FamilyType::StateSet => (FamilyType::StateSet, Point::StateSet(v.as_f64() != 0.0)),
            _ => (FamilyType::Gauge, Point::Gauge(encoder.scalar(*v))),
        },
        MetricKind::GaugeDelta(_) => {
            encoder.not_encoded(NotEncoded::UnresolvedGaugeDelta);
            encoder.diagnostics.warn_throttled(
                "gauge_delta_unresolved",
                format_args!(
                    "a relative gauge adjustment reached a sink unresolved -- add an `aggregate` \
                     component between the statsd input and this output"
                ),
            );
            return None;
        }
        MetricKind::Histogram(h) => {
            if h.temporality == Temporality::Delta {
                encoder.not_encoded(NotEncoded::UnresolvedDelta);
                warn_delta(encoder, resolve(record.name));
                return None;
            }
            let kind = histogram_family_type(resource, event);
            let (buckets, count) = cumulative_counts(&h.buckets);
            if h.buckets.iter().any(|(bound, _)| bound.is_nan()) {
                encoder.degraded_reason("non_finite_component");
            }
            (kind, Point::Histogram { buckets, sum: h.sum, count })
        }
        MetricKind::ExponentialHistogram(_) => {
            let name = resolve(record.name);
            match encoder.destination {
                Destination::Exposition => {
                    encoder.not_encoded(NotEncoded::ExponentialHistogram);
                    encoder.diagnostics.warn_throttled(
                        "prometheus_exponential_histogram_skipped",
                        format_args!(
                            "metric '{name}' is an ExponentialHistogram, which neither \
                             Prometheus text 0.0.4 nor OpenMetrics 1.0 can express -- skipped"
                        ),
                    );
                }
                Destination::RemoteWrite => {
                    encoder.not_encoded(NotEncoded::NativeHistogram);
                    encoder.diagnostics.warn_throttled(
                        "prometheus_exponential_histogram_skipped",
                        format_args!(
                            "metric '{name}' is an ExponentialHistogram, and this sender doesn't \
                             write remote-write native histograms -- dropped"
                        ),
                    );
                }
            }
            return None;
        }
        MetricKind::Summary(s) => {
            if s.temporality == Temporality::Delta {
                encoder.not_encoded(NotEncoded::UnresolvedDelta);
                warn_delta_summary(encoder, resolve(record.name));
                return None;
            }
            (
                FamilyType::Summary,
                Point::Summary {
                    quantiles: s.quantiles.clone(),
                    sum: Some(s.sum),
                    count: Some(s.count),
                },
            )
        }
        MetricKind::Distribution(sketch) => {
            encoder.degraded("distribution");
            (FamilyType::Summary, sketch_summary(sketch))
        }
        MetricKind::Samples(samples) => {
            encoder.degraded("samples");
            (FamilyType::Summary, sketch_summary(&samples.sketch()))
        }
        MetricKind::Set(hll) => {
            encoder.degraded("set");
            (FamilyType::Gauge, Point::Gauge(hll.estimate() as f64))
        }
        MetricKind::SetMembers(members) => {
            encoder.degraded("set_members");
            let distinct = members.iter().collect::<BTreeSet<_>>().len();
            (FamilyType::Gauge, Point::Gauge(distinct as f64))
        }
    })
}

/// A `Gauge` record's family type: `info` or `stateset` when the event is flagged
/// [`METRICS_TYPE`] with that value, on the event or else the resource; else `untyped` or
/// `unknown` when the event carries that [`ATTR_TYPE`]; else `gauge`. Any other value of either
/// key is ignored, so a carrier spelling `info` exposes a plain gauge.
fn gauge_family_type(resource: &Resource, event: &Event) -> FamilyType {
    match metric_type(resource, event) {
        Some(TYPE_INFO) => return FamilyType::Info,
        Some(TYPE_STATESET) => return FamilyType::StateSet,
        _ => {}
    }
    match event.attributes.get(ATTR_TYPE).and_then(Value::as_str) {
        Some("untyped") => FamilyType::Untyped,
        Some("unknown") => FamilyType::Unknown,
        _ => FamilyType::Gauge,
    }
}

/// A cumulative `Histogram` record's family type: `gaugehistogram` when the event is flagged
/// [`METRICS_TYPE`] `gaugehistogram`, on the event or else the resource, else `histogram`.
fn histogram_family_type(resource: &Resource, event: &Event) -> FamilyType {
    if metric_type(resource, event) == Some(TYPE_GAUGE_HISTOGRAM) {
        FamilyType::GaugeHistogram
    } else {
        FamilyType::Histogram
    }
}

/// A record carrying `FLAG_NO_RECORDED_VALUE` → the family its stale marker belongs on, under
/// [`PrometheusEncoder::with_stale_markers`]. `None` -- skipped and counted exactly as an unflagged
/// skip would be -- for every kind that expands to more than one wire series.
///
/// Temporality is not consulted: `Sum{Delta}` is normally dropped because Prometheus
/// cannot express a delta *reading*, and a stale marker has no reading to be delta about. The
/// family type otherwise follows [`record_to_point`]'s own choices, so a series that went stale
/// lands on the same family it was on while it had values.
fn stale_point(
    record: &MetricRecord,
    resource: &Resource,
    event: &Event,
    encoder: &mut PrometheusEncoder,
) -> Option<(FamilyType, Point)> {
    let kind = match &record.kind {
        MetricKind::Sum(s) => {
            if s.monotonic {
                // A marker has no reading to be delta about, so a delta one reads back as a
                // cumulative counter: a record of another kind.
                if s.temporality == Temporality::Delta {
                    encoder.degraded("delta_sum");
                }
                FamilyType::Counter
            } else {
                encoder.degraded("non_monotonic_sum");
                FamilyType::Gauge
            }
        }
        MetricKind::Gauge(_) => gauge_family_type(resource, event),
        _ => {
            encoder.not_encoded(NotEncoded::NoRecordedValue);
            return None;
        }
    };
    Some((kind, Point::Stale))
}

/// The one diagnostic both delta arms report under -- a single greppable key naming the fix, the
/// way `gauge_delta_unresolved` does for its own missing-`aggregate` case.
fn warn_delta(encoder: &mut PrometheusEncoder, name: &str) {
    encoder.diagnostics.warn_throttled(
        "delta_temporality_unresolved",
        format_args!(
            "metric '{name}' has delta temporality, which Prometheus exposition cannot express -- \
             add an `aggregate` component with `temporality: cumulative` before this output"
        ),
    );
}

/// A delta `Summary`'s diagnostic, under its own key because its fix differs from
/// [`warn_delta`]'s: `aggregate` doesn't make a summary cumulative, and only
/// `cumulative_to_delta` makes one delta.
fn warn_delta_summary(encoder: &mut PrometheusEncoder, name: &str) {
    encoder.diagnostics.warn_throttled(
        "delta_summary_unresolved",
        format_args!(
            "metric '{name}' is a delta summary, which Prometheus cannot express -- route the \
             summary to this output from ahead of the `cumulative_to_delta` component"
        ),
    );
}

/// A sketch → the summary point the module doc's `Distribution`/`Samples` rows describe: the
/// shared [`SUMMARY_QUANTILES`], `0` and `1` being the sketch's minimum and maximum, the sketch's
/// sum, and its count.
fn sketch_summary(sketch: &DdSketch) -> Point {
    let quantiles =
        SUMMARY_QUANTILES.iter().filter_map(|q| sketch.quantile(*q).map(|v| (*q, v))).collect();
    Point::Summary { quantiles, sum: Some(sketch.sum()), count: Some(sketch.count() as u64) }
}

/// [`logit_core::Histogram`]'s per-bucket counts → the cumulative `le` counts both dialects carry,
/// plus the total, with one entry per distinct bound and `+Inf` last. Bounds are sorted ascending
/// first, because the model doesn't promise an order and `otlp_in` keeps the wire's. A `NaN`
/// bound has no place in that order, so it gets no entry, and its count reaches only `+Inf` and
/// the total; each caller counts that record degraded. `-Inf` is an ordinary bound and sorts
/// first. Equal bounds merge into one entry. A `+Inf` entry is appended when no bucket has that
/// bound: every conforming exposition has one, and the total has to live somewhere.
///
/// The sort is [`f64::total_cmp`], a total order once `NaN` is out. A comparator that isn't one
/// can leave bounds out of order, and `sort_by` may panic on it.
pub(crate) fn cumulative_counts(buckets: &[(f64, u64)]) -> (Vec<(f64, u64)>, u64) {
    let mut sorted: Vec<(f64, u64)> =
        buckets.iter().copied().filter(|(b, _)| !b.is_nan()).collect();
    sorted.sort_by(|a, b| a.0.total_cmp(&b.0));
    let total = buckets.iter().fold(0u64, |acc, (_, count)| acc.saturating_add(*count));
    let mut out: Vec<(f64, u64)> = Vec::with_capacity(sorted.len() + 1);
    let mut running = 0u64;
    for (bound, count) in sorted {
        running = running.saturating_add(count);
        match out.last_mut() {
            // `==` rather than the sort's order, so `-0.0` and `0.0` merge as well.
            Some(last) if last.0 == bound => *last = (bound, running),
            _ => out.push((bound, running)),
        }
    }
    match out.last_mut() {
        Some(last) if last.0 == f64::INFINITY => last.1 = total,
        _ => out.push((f64::INFINITY, total)),
    }
    (out, total)
}

/// Resource attributes merged with event attributes (event wins on a collision), private `_logit.*`
/// keys skipped, each name sanitized, each value stringified -- the label set for one series.
///
/// Ordering and collision resolution are decided on the **rendered** label names, not on `Symbol`
/// order: the emitted label set is sorted by the name that appears on the wire, and when two
/// attributes sanitize onto one label name the one whose *original* attribute name sorts first wins,
/// so the outcome depends on the data rather than on process-wide interning history.
fn build_labels(
    resource: &Resource,
    event: &Event,
    kind: FamilyType,
    encoder: &mut PrometheusEncoder,
) -> Vec<(String, String)> {
    // The label name this family type generates itself, which an attribute must not collide with:
    // a duplicate label name is invalid exposition, not merely confusing.
    let generated = match kind {
        FamilyType::Histogram | FamilyType::GaugeHistogram => Some("le"),
        FamilyType::Summary => Some("quantile"),
        _ => None,
    };
    let mut candidates: Vec<(&str, Cow<'_, str>, String)> = Vec::new();
    for (key, value) in logit_core::attrs::merged(resource, event) {
        let key = resolve(key);
        if logit_core::attrs::is_private(key) {
            continue;
        }
        // A repeated DogStatsD tag key arrives as a `Value::Array` in wire order
        // (`crate::statsd::decode::insert_tags`; `statsd_out` re-expands it to one tag per
        // element). A Prometheus label set is a map -- one name, one value -- so the **last**
        // representable element wins, walked backwards so a trailing unrepresentable element falls
        // through to the one before it. Last, not first, to match `influxdb_out`, and to keep the
        // label a last-token-wins collapse of the repeated key would have kept. An empty or
        // entirely unrepresentable array has no element to fall back to and stays on the
        // `unrepresentable` drop path below.
        let rendered = match value {
            Value::Array(elements) => match elements.iter().rev().find_map(label_value) {
                Some(last) => {
                    encoder.label_normalized("multi_value");
                    Some(last)
                }
                None => None,
            },
            _ => label_value(value),
        };
        let Some(rendered) = rendered else {
            encoder.label_omitted("unrepresentable");
            continue;
        };
        let name = sanitize_label_name(key);
        if Some(name.as_ref()) == generated {
            encoder.label_omitted("reserved");
            continue;
        }
        candidates.push((key, name, rendered));
    }
    candidates.sort_by(|a, b| a.0.cmp(b.0));
    let mut labels: Vec<(String, String)> = Vec::with_capacity(candidates.len());
    for (_, name, value) in candidates {
        if labels.iter().any(|(existing, _)| existing.as_str() == name.as_ref()) {
            encoder.label_omitted("collision");
            continue;
        }
        if matches!(name, Cow::Owned(_)) {
            encoder.label_normalized("name_sanitized");
        }
        labels.push((name.into_owned(), value));
    }
    labels.sort_by(|a, b| a.0.cmp(&b.0));
    labels
}

/// A [`Value`] as a Prometheus label value (always a string on the wire), or `None` for the kinds
/// with no faithful string form -- see the module doc's encode table.
fn label_value(value: &Value) -> Option<String> {
    Some(match value {
        Value::Str(_) => value.as_str()?.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::I64(i) => i.to_string(),
        Value::U64(u) => u.to_string(),
        Value::F64(f) => f.to_string(),
        Value::Null | Value::Bytes(_) | Value::Timestamp(_) | Value::Array(_) | Value::Map(_) => {
            return None
        }
    })
}

// -------------------------------------------------------------------------------------------------
// Sanitization
// -------------------------------------------------------------------------------------------------

/// A metric name forced into `[a-zA-Z_:][a-zA-Z0-9_:]*`: every other character becomes `_`
/// (substitution, not deletion, so distinct inputs stay distinct -- the `statsd_out` precedent,
/// `crates/logit-outputs/src/statsd.rs`'s `sanitize_into`), and a leading digit gains a `_` prefix
/// rather than being replaced, which would fold `5xx_total` and `_xx_total` together. An empty name
/// becomes `_`.
///
/// Returns a [`Cow`] **borrowing** `name` whenever it already conforms, which is the overwhelmingly
/// common case: every name off a Prometheus scrape, and every name a metrics library produces.
/// [`events_to_families`] calls this once per metric *record*, so allocating there would cost one
/// allocation per point rather than one per family (`docs/design/memory.md`,
/// [ADR `minimize-allocations-over-event-size`](../../../../docs/adr/minimize-allocations-over-event-size.md)).
///
/// Two names that sanitize onto one cannot both be exposed -- see [`events_to_families`] and the
/// module doc's encode table for how that collision is resolved and counted.
pub fn sanitize_metric_name(name: &str) -> Cow<'_, str> {
    sanitize(name, true)
}

/// A label name forced into `[a-zA-Z_][a-zA-Z0-9_]*` -- the same rules as
/// [`sanitize_metric_name`] minus `:`, which is legal in a metric name and not in a label name.
pub fn sanitize_label_name(name: &str) -> Cow<'_, str> {
    sanitize(name, false)
}

fn sanitize(name: &str, colon_ok: bool) -> Cow<'_, str> {
    let conforms = |c: char| c.is_ascii_alphanumeric() || c == '_' || (colon_ok && c == ':');
    let leads = |c: char| conforms(c) && !c.is_ascii_digit();
    if name.starts_with(leads) && name.chars().all(conforms) {
        return Cow::Borrowed(name);
    }
    let mut out = String::with_capacity(name.len() + 1);
    for c in name.chars() {
        out.push(if conforms(c) { c } else { '_' });
    }
    match out.chars().next() {
        None => out.push('_'),
        Some(c) if c.is_ascii_digit() => out.insert(0, '_'),
        Some(_) => {}
    }
    Cow::Owned(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::attrs::CLOSED_BUCKETED;
    use logit_core::telemetry::Registry;
    use logit_core::{ExpHistogram, HyperLogLog, Samples, Sum, TraceRef};
    use std::sync::Arc;

    const RECEIVED_AT: i64 = 1_700_000_000_000_000_000;

    fn family(name: &str, kind: FamilyType, series: Vec<Series>) -> MetricFamily {
        MetricFamily { series, ..MetricFamily::new(name, kind) }
    }

    fn labels(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    fn decode(families: &[MetricFamily]) -> Vec<Event> {
        families_to_events(families, RECEIVED_AT, &mut PrometheusDecoder::new())
    }

    /// The one metric kind a single-series family decodes to.
    fn decode_kind(families: &[MetricFamily]) -> MetricKind {
        let events = decode(families);
        assert_eq!(events.len(), 1, "expected exactly one event");
        events[0].metrics[0].kind.clone()
    }

    fn telemetry() -> (Arc<Registry>, Telemetry) {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("prometheus", "prometheus_out", "sink");
        (registry, telemetry)
    }

    /// Whether `registry` recorded a point named `metric` carrying `tag`. Drains, so call once.
    fn counted(registry: &Registry, metric: &str, tag: (&str, &str)) -> bool {
        registry.drain(0).iter().any(|event| {
            event.metrics.iter().any(|m| resolve(m.name) == metric)
                && event.attributes.get(tag.0).and_then(|v| v.as_str()) == Some(tag.1)
        })
    }

    fn resource_with(attrs: &[(&str, Value)]) -> Resource {
        let mut attributes = AttrMap::new();
        for (key, value) in attrs {
            attributes.insert(key, value.clone());
        }
        Resource { attributes, ..Default::default() }
    }

    fn event_with(attrs: &[(&str, Value)], record: MetricRecord) -> Event {
        let mut attributes = AttrMap::new();
        for (key, value) in attrs {
            attributes.insert(key, value.clone());
        }
        Event::metric(RECEIVED_AT, attributes, record)
    }

    fn record(name: &str, kind: MetricKind) -> MetricRecord {
        MetricRecord::new(intern(name), kind)
    }

    fn encode(resource: &Resource, event: &Event) -> Vec<MetricFamily> {
        events_to_families(std::iter::once((resource, event)), &mut PrometheusEncoder::new())
    }

    fn encode_counted(
        resource: &Resource,
        event: &Event,
    ) -> (Vec<MetricFamily>, Arc<Registry>, Arc<Registry>) {
        let (registry, telemetry) = telemetry();
        let diag_registry = Registry::new();
        let diagnostics = Diagnostics::new("prometheus_out")
            .with_telemetry(diag_registry.telemetry_for("prometheus", "prometheus_out", "diag"));
        let mut encoder =
            PrometheusEncoder::new().with_telemetry(telemetry).with_diagnostics(diagnostics);
        let families = events_to_families(std::iter::once((resource, event)), &mut encoder);
        (families, registry, diag_registry)
    }

    fn encode_kind(kind: MetricKind) -> Vec<MetricFamily> {
        encode(&Resource::default(), &event_with(&[], record("m", kind)))
    }

    // --- decode: one test per row of the module doc's decode table ------------------------------

    #[test]
    fn a_counter_decodes_as_a_cumulative_monotonic_sum_keeping_its_name_verbatim() {
        let families = vec![family(
            "http_requests_total",
            FamilyType::Counter,
            vec![Series::new(labels(&[("code", "200")]), Point::Counter(1027.0))],
        )];
        let events = decode(&families);
        assert_eq!(resolve(events[0].metrics[0].name), "http_requests_total");
        assert_eq!(
            events[0].metrics[0].kind,
            MetricKind::Sum(Sum {
                value: MetricValue::F64(1027.0),
                temporality: Temporality::Cumulative,
                monotonic: true
            })
        );
        assert_eq!(events[0].attributes.get("code"), Some(&Value::from("200")));
    }

    /// A private label is left off the event and counted once per event; `logit.x` and `_logitx`
    /// aren't private.
    #[test]
    fn a_private_label_is_omitted_and_counted_and_lookalikes_are_not() {
        let registry = Registry::new();
        let mut decoder = PrometheusDecoder::new().with_telemetry(registry.telemetry_for(
            "prometheus_in",
            "prometheus_in",
            "listener",
        ));
        let pairs = [
            ("code", "200"),
            ("_logit.prometheus.type", "gauge"),
            ("logit.x", "1"),
            ("_logitx", "2"),
        ];
        let families = vec![family(
            "up",
            FamilyType::Gauge,
            vec![
                Series::new(labels(&pairs), Point::Gauge(1.0)),
                Series::new(labels(&[("_logit.y", "1"), ("code", "200")]), Point::Gauge(2.0)),
            ],
        )];
        let events = families_to_events(&families, RECEIVED_AT, &mut decoder);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].attributes.get("code"), Some(&Value::from("200")));
        assert_eq!(events[0].attributes.get("logit.x"), Some(&Value::from("1")));
        assert_eq!(events[0].attributes.get("_logitx"), Some(&Value::from("2")));
        assert_eq!(events[0].attributes.len(), 3, "{:?}", events[0].attributes);
        assert_eq!(events[1].attributes.len(), 1, "{:?}", events[1].attributes);
        let total: f64 = registry
            .drain(0)
            .iter()
            .filter(|e| {
                e.attributes.get("reason").and_then(|v| v.as_str()) == Some("reserved_prefix")
            })
            .flat_map(|e| &e.metrics)
            .filter(|m| resolve(m.name) == "logit.input.tags.omitted")
            .map(|m| match &m.kind {
                MetricKind::Sum(sum) => sum.value.as_f64(),
                other => panic!("expected a counter, got {other:?}"),
            })
            .sum();
        assert_eq!(total, 2.0);
    }

    #[test]
    fn a_gauge_decodes_as_a_gauge_with_no_type_marker() {
        let families = vec![family(
            "go_goroutines",
            FamilyType::Gauge,
            vec![Series::new(vec![], Point::Gauge(69.0))],
        )];
        let events = decode(&families);
        assert_eq!(events[0].metrics[0].kind, MetricKind::gauge(69.0));
        assert_eq!(events[0].attributes.get(ATTR_TYPE), None, "a plain gauge needs no marker");
    }

    /// The cumulative-to-per-bucket successive difference, `+Inf` bucket included.
    #[test]
    fn a_histogram_decodes_with_per_bucket_counts_by_successive_difference() {
        let point = Point::Histogram {
            buckets: vec![(0.05, 24054), (0.1, 33444), (f64::INFINITY, 144320)],
            sum: Some(53423.0),
            count: 144320,
        };
        let families = vec![family("h", FamilyType::Histogram, vec![Series::new(vec![], point)])];
        assert_eq!(
            decode_kind(&families),
            MetricKind::Histogram(Histogram {
                buckets: vec![(0.05, 24054), (0.1, 9390), (f64::INFINITY, 110876)],
                temporality: Temporality::Cumulative,
                sum: Some(53423.0),
                min: None,
                max: None,
            })
        );
    }

    #[test]
    fn a_gaugehistogram_decodes_as_a_histogram_flagged_with_the_neutral_type() {
        let point = Point::Histogram {
            buckets: vec![(0.01, 20), (f64::INFINITY, 42)],
            sum: Some(3289.3),
            count: 42,
        };
        let families =
            vec![family("h", FamilyType::GaugeHistogram, vec![Series::new(vec![], point)])];
        let events = decode(&families);
        assert!(matches!(events[0].metrics[0].kind, MetricKind::Histogram(_)));
        assert_eq!(events[0].attributes.get(METRICS_TYPE), Some(&Value::from("gaugehistogram")));
        assert_eq!(events[0].attributes.get(ATTR_TYPE), None, "the flag alone");
    }

    #[test]
    fn a_summary_decodes_with_its_quantiles_count_and_sum() {
        let point = Point::Summary {
            quantiles: vec![(0.5, 4773.0), (0.99, 76656.0)],
            sum: Some(1.7560473e7),
            count: Some(2693),
        };
        let families = vec![family("s", FamilyType::Summary, vec![Series::new(vec![], point)])];
        assert_eq!(
            decode_kind(&families),
            MetricKind::Summary(Summary {
                quantiles: vec![(0.5, 4773.0), (0.99, 76656.0)],
                count: 2693,
                sum: 1.7560473e7,
                temporality: Temporality::Cumulative,
            })
        );
    }

    /// A wire `summary` with neither `_sum` nor `_count` still decodes -- both become the model's
    /// own zero, which is the one place this mapping cannot tell "absent" from "zero" (see the
    /// module doc's normalization list).
    #[test]
    fn a_summary_with_no_sum_or_count_decodes_as_zero_for_both() {
        let point = Point::Summary { quantiles: vec![(0.5, 1.0)], sum: None, count: None };
        let families = vec![family("s", FamilyType::Summary, vec![Series::new(vec![], point)])];
        assert_eq!(
            decode_kind(&families),
            MetricKind::Summary(Summary {
                quantiles: vec![(0.5, 1.0)],
                count: 0,
                sum: 0.0,
                temporality: Temporality::Cumulative
            })
        );
    }

    #[test]
    fn untyped_and_unknown_decode_as_a_gauge_carrying_their_own_spelling() {
        for (kind, expected) in [(FamilyType::Untyped, "untyped"), (FamilyType::Unknown, "unknown")]
        {
            let families =
                vec![family("m", kind, vec![Series::new(vec![], Point::Unknown(12.47))])];
            let events = decode(&families);
            assert_eq!(events[0].metrics[0].kind, MetricKind::gauge(12.47));
            assert_eq!(events[0].attributes.get(ATTR_TYPE), Some(&Value::from(expected)));
            assert_eq!(events[0].attributes.get(METRICS_TYPE), None, "the carrier alone");
        }
    }

    #[test]
    fn an_info_family_decodes_as_a_gauge_of_one_named_without_the_info_suffix() {
        let families = vec![family(
            "target",
            FamilyType::Info,
            vec![Series::new(labels(&[("env", "prod")]), Point::Info)],
        )];
        let events = decode(&families);
        assert_eq!(resolve(events[0].metrics[0].name), "target");
        assert_eq!(events[0].metrics[0].kind, MetricKind::gauge(1.0));
        assert_eq!(events[0].attributes.get(METRICS_TYPE), Some(&Value::from("info")));
        assert_eq!(events[0].attributes.get(ATTR_TYPE), None, "the flag alone");
        assert_eq!(events[0].attributes.get("env"), Some(&Value::from("prod")));
    }

    #[test]
    fn a_stateset_decodes_as_one_gauge_per_state() {
        let families = vec![family(
            "foo",
            FamilyType::StateSet,
            vec![
                Series::new(labels(&[("foo", "a")]), Point::StateSet(false)),
                Series::new(labels(&[("foo", "bb")]), Point::StateSet(true)),
            ],
        )];
        let events = decode(&families);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].metrics[0].kind, MetricKind::gauge(0.0));
        assert_eq!(events[1].metrics[0].kind, MetricKind::gauge(1.0));
        for event in &events {
            assert_eq!(event.attributes.get(METRICS_TYPE), Some(&Value::from("stateset")));
            assert_eq!(event.attributes.get(ATTR_TYPE), None, "the flag alone");
        }
    }

    #[test]
    fn help_and_unit_decode_onto_description_and_unit() {
        let mut f =
            family("m", FamilyType::Counter, vec![Series::new(vec![], Point::Counter(1.0))]);
        f.help = Some("a help string".to_string());
        f.unit = Some("seconds".to_string());
        let events = decode(&[f]);
        assert_eq!(events[0].metrics[0].description.map(resolve), Some("a help string"));
        assert_eq!(events[0].metrics[0].unit.map(resolve), Some("seconds"));
    }

    #[test]
    fn created_decodes_onto_start_timestamp_and_is_zero_when_absent() {
        let mut series = Series::new(vec![], Point::Counter(1.0));
        series.created = Some(1_605_281_325_000_000_000);
        let with = vec![family("m", FamilyType::Counter, vec![series])];
        assert_eq!(decode(&with)[0].metrics[0].start_timestamp, 1_605_281_325_000_000_000);

        let without =
            vec![family("m", FamilyType::Counter, vec![Series::new(vec![], Point::Counter(1.0))])];
        assert_eq!(decode(&without)[0].metrics[0].start_timestamp, 0);
    }

    #[test]
    fn a_sample_timestamp_becomes_the_events_timestamp_and_is_flagged() {
        let mut series = Series::new(vec![], Point::Gauge(1.0));
        series.timestamp = Some(1_395_066_363_000_000_000);
        let events = decode(&[family("m", FamilyType::Gauge, vec![series])]);
        assert_eq!(events[0].timestamp, 1_395_066_363_000_000_000);
        assert_eq!(
            events[0].attributes.get(METRICS_CLOSED).and_then(Value::as_str),
            Some(CLOSED_TIMESTAMPED)
        );
        assert_eq!(events[0].attributes.len(), 1, "the flag alone, no protocol carrier beside it");
    }

    #[test]
    fn a_sample_with_no_timestamp_takes_the_scrape_time_and_no_flag() {
        let events =
            decode(&[family("m", FamilyType::Gauge, vec![Series::new(vec![], Point::Gauge(1.0))])]);
        assert_eq!(events[0].timestamp, RECEIVED_AT);
        assert_eq!(events[0].attributes.get(METRICS_CLOSED), None);
    }

    #[test]
    fn exemplars_ride_onto_the_metric_record() {
        let exemplar = Exemplar {
            timestamp: 1_520_879_607_789_000_000,
            value: MetricValue::F64(0.67),
            trace: Some(TraceRef { trace_id: [7; 16], span_id: Some([6; 8]), flags: 0 }),
            filtered_attributes: AttrMap::new(),
        };
        let mut series = Series::new(vec![], Point::Counter(17.0));
        series.exemplars = vec![exemplar.clone()];
        let events = decode(&[family("m", FamilyType::Counter, vec![series])]);
        assert_eq!(events[0].metrics[0].exemplars, vec![exemplar]);
    }

    #[test]
    fn a_histogram_whose_cumulative_counts_decrease_is_dropped_and_counted() {
        let (registry, telemetry) = telemetry();
        let mut decoder = PrometheusDecoder::new().with_telemetry(telemetry);
        let point = Point::Histogram {
            buckets: vec![(1.0, 5), (2.0, 3), (f64::INFINITY, 3)],
            sum: None,
            count: 3,
        };
        let families = vec![family("h", FamilyType::Histogram, vec![Series::new(vec![], point)])];
        let events = families_to_events(&families, RECEIVED_AT, &mut decoder);
        assert!(events.is_empty(), "a non-cumulative bucket list must not decode");
        assert!(counted(
            &registry,
            "logit.input.metrics.dropped",
            ("reason", "non_monotonic_buckets")
        ));
    }

    #[test]
    fn a_histogram_with_no_buckets_at_all_is_dropped_and_counted() {
        let (registry, telemetry) = telemetry();
        let mut decoder = PrometheusDecoder::new().with_telemetry(telemetry);
        let point = Point::Histogram { buckets: vec![], sum: None, count: 0 };
        let families = vec![family("h", FamilyType::Histogram, vec![Series::new(vec![], point)])];
        assert!(families_to_events(&families, RECEIVED_AT, &mut decoder).is_empty());
        assert!(counted(&registry, "logit.input.metrics.dropped", ("reason", "empty_histogram")));
    }

    // --- encode: one test per row of the module doc's encode table ------------------------------

    #[test]
    fn a_cumulative_monotonic_sum_encodes_as_a_counter() {
        let kind = MetricKind::Sum(Sum {
            value: MetricValue::F64(5.0),
            temporality: Temporality::Cumulative,
            monotonic: true,
        });
        let families = encode_kind(kind);
        assert_eq!(families[0].kind, FamilyType::Counter);
        assert_eq!(families[0].series[0].point, Point::Counter(5.0));
    }

    #[test]
    fn a_non_monotonic_cumulative_sum_encodes_as_a_gauge_and_is_counted_degraded() {
        let kind = MetricKind::Sum(Sum {
            value: MetricValue::F64(5.0),
            temporality: Temporality::Cumulative,
            monotonic: false,
        });
        let (families, registry, _) =
            encode_counted(&Resource::default(), &event_with(&[], record("m", kind)));
        assert_eq!(families[0].kind, FamilyType::Gauge);
        assert!(counted(
            &registry,
            "logit.output.metrics.degraded",
            ("metric_kind", "non_monotonic_sum")
        ));
    }

    #[test]
    fn a_delta_sum_is_dropped_and_names_aggregates_cumulative_mode() {
        let (families, registry, diag) = encode_counted(
            &Resource::default(),
            &event_with(&[], record("m", MetricKind::counter(1.0))),
        );
        assert!(families.is_empty());
        assert!(counted(&registry, "logit.output.metrics.dropped", ("reason", "unresolved_delta")));
        assert!(counted(
            &diag,
            "logit.component.diagnostics",
            ("key", "delta_temporality_unresolved")
        ));
    }

    #[test]
    fn a_delta_histogram_is_dropped_and_counted() {
        let kind = MetricKind::Histogram(Histogram {
            buckets: vec![(1.0, 1), (f64::INFINITY, 0)],
            temporality: Temporality::Delta,
            sum: None,
            min: None,
            max: None,
        });
        let (families, registry, _) =
            encode_counted(&Resource::default(), &event_with(&[], record("m", kind)));
        assert!(families.is_empty());
        assert!(counted(&registry, "logit.output.metrics.dropped", ("reason", "unresolved_delta")));
    }

    /// A Prometheus summary is a running total, so a delta one, which only `cumulative_to_delta`
    /// makes, is dropped and counted.
    #[test]
    fn a_delta_summary_is_dropped_and_counted() {
        let kind = MetricKind::Summary(Summary {
            quantiles: vec![(0.5, 1.0)],
            count: 2,
            sum: 3.0,
            temporality: Temporality::Delta,
        });
        let (families, registry, diag) =
            encode_counted(&Resource::default(), &event_with(&[], record("m", kind)));
        assert!(families.is_empty());
        assert!(counted(&registry, "logit.output.metrics.dropped", ("reason", "unresolved_delta")));
        assert!(counted(&diag, "logit.component.diagnostics", ("key", "delta_summary_unresolved")));
    }

    /// `untyped` and `unknown` come from the carrier, `info` and `stateset` from the neutral flag,
    /// on the event or else the resource.
    #[test]
    fn a_gauges_family_type_follows_the_carrier_or_the_neutral_type() {
        let gauge = || record("m", MetricKind::gauge(2.0));
        for (key, value, expected_kind, expected_point) in [
            (ATTR_TYPE, "untyped", FamilyType::Untyped, Point::Unknown(2.0)),
            (ATTR_TYPE, "unknown", FamilyType::Unknown, Point::Unknown(2.0)),
            (METRICS_TYPE, "info", FamilyType::Info, Point::Info),
            (METRICS_TYPE, "stateset", FamilyType::StateSet, Point::StateSet(true)),
        ] {
            let event = event_with(&[(key, Value::from(value))], gauge());
            let families = encode(&Resource::default(), &event);
            assert_eq!(families[0].kind, expected_kind, "{key}: {value}");
            assert_eq!(families[0].series[0].point, expected_point, "{key}: {value}");
            assert!(
                families[0].series[0].labels.is_empty(),
                "private attributes are consumed, never rendered as labels"
            );
        }
        for value in ["info", "stateset"] {
            let mut resource = Resource::default();
            resource.attributes.insert(METRICS_TYPE, value);
            let families = encode(&resource, &event_with(&[], gauge()));
            assert_eq!(families[0].kind.as_str(), value, "the resource's {value}");
        }
    }

    /// The carrier spells only `untyped` and `unknown`, and the neutral flag only `info`,
    /// `stateset`, and `gaugehistogram`, so each key's other values expose a plain gauge.
    #[test]
    fn a_gauge_ignores_a_type_its_key_does_not_carry() {
        for (key, value) in [
            (ATTR_TYPE, "info"),
            (ATTR_TYPE, "stateset"),
            (METRICS_TYPE, "untyped"),
            (METRICS_TYPE, "gaugehistogram"),
            (METRICS_TYPE, "rate"),
        ] {
            let event =
                event_with(&[(key, Value::from(value))], record("m", MetricKind::gauge(2.0)));
            let families = encode(&Resource::default(), &event);
            assert_eq!(families[0].kind, FamilyType::Gauge, "{key}: {value}");
            assert_eq!(families[0].series[0].point, Point::Gauge(2.0), "{key}: {value}");
        }
    }

    #[test]
    fn a_gauge_delta_is_dropped_under_the_shared_diagnostic_key() {
        let (families, registry, diag) = encode_counted(
            &Resource::default(),
            &event_with(&[], record("m", MetricKind::GaugeDelta(MetricValue::F64(1.0)))),
        );
        assert!(families.is_empty());
        assert!(counted(
            &registry,
            "logit.output.metrics.dropped",
            ("reason", "unresolved_gauge_delta")
        ));
        assert!(counted(&diag, "logit.component.diagnostics", ("key", "gauge_delta_unresolved")));
    }

    #[test]
    fn a_cumulative_histogram_encodes_with_running_sum_buckets_and_drops_min_max() {
        let kind = MetricKind::Histogram(Histogram {
            buckets: vec![(1.0, 2), (5.0, 3), (f64::INFINITY, 1)],
            temporality: Temporality::Cumulative,
            sum: Some(12.5),
            min: Some(0.5),
            max: Some(9.9),
        });
        let families = encode_kind(kind);
        assert_eq!(families[0].kind, FamilyType::Histogram);
        assert_eq!(
            families[0].series[0].point,
            Point::Histogram {
                buckets: vec![(1.0, 2), (5.0, 5), (f64::INFINITY, 6)],
                sum: Some(12.5),
                count: 6,
            }
        );
    }

    /// `min`/`max` have no exposition representation (the known-gaps row): output is identical.
    #[test]
    fn a_histograms_min_and_max_are_dropped_without_changing_anything_else() {
        let with = MetricKind::Histogram(Histogram {
            buckets: vec![(1.0, 2), (f64::INFINITY, 1)],
            temporality: Temporality::Cumulative,
            sum: Some(3.0),
            min: Some(0.5),
            max: Some(9.9),
        });
        let without = MetricKind::Histogram(Histogram {
            buckets: vec![(1.0, 2), (f64::INFINITY, 1)],
            temporality: Temporality::Cumulative,
            sum: Some(3.0),
            min: None,
            max: None,
        });
        assert_eq!(encode_kind(with), encode_kind(without));
    }

    /// A histogram flagged `gaugehistogram` on the event or the resource exposes as one; the
    /// carrier spelling it exposes a plain histogram.
    #[test]
    fn a_histogram_flagged_gaugehistogram_encodes_as_a_gaugehistogram() {
        let kind = MetricKind::Histogram(Histogram {
            buckets: vec![(1.0, 2), (f64::INFINITY, 1)],
            temporality: Temporality::Cumulative,
            sum: Some(3.0),
            min: None,
            max: None,
        });
        let flagged = |key: &'static str| {
            event_with(&[(key, Value::from("gaugehistogram"))], record("m", kind.clone()))
        };
        let bare = Resource::default();
        assert_eq!(encode(&bare, &flagged(METRICS_TYPE))[0].kind, FamilyType::GaugeHistogram);
        assert_eq!(encode(&bare, &flagged(ATTR_TYPE))[0].kind, FamilyType::Histogram);
        let mut resource = Resource::default();
        resource.attributes.insert(METRICS_TYPE, "gaugehistogram");
        let event = event_with(&[], record("m", kind.clone()));
        assert_eq!(encode(&resource, &event)[0].kind, FamilyType::GaugeHistogram);
    }

    /// A model histogram whose highest bound is finite (nothing forbids it) still gets a `+Inf`
    /// bucket on the way out: both formats require one, and the total has to live somewhere.
    #[test]
    fn a_histogram_with_no_infinite_bound_gains_an_inf_bucket() {
        let kind = MetricKind::Histogram(Histogram {
            buckets: vec![(1.0, 2), (5.0, 3)],
            temporality: Temporality::Cumulative,
            sum: None,
            min: None,
            max: None,
        });
        assert_eq!(
            encode_kind(kind)[0].series[0].point,
            Point::Histogram {
                buckets: vec![(1.0, 2), (5.0, 5), (f64::INFINITY, 5)],
                sum: None,
                count: 5,
            }
        );
    }

    /// More than 20 buckets in a shuffled order, one with a `NaN` bound: past 20 elements `sort_by`
    /// may panic on a comparator that isn't a total order, and one that reads `NaN` as equal to
    /// everything leaves bounds out of order. The `NaN` bucket's count reaches only `+Inf`.
    #[test]
    fn cumulative_counts_sorts_many_shuffled_bounds_around_a_nan() {
        // 7 is coprime to 24, so `i * 7 % 24` visits every bound once, out of order.
        let mut buckets: Vec<(f64, u64)> =
            (0..24u64).map(|i| ((i * 7 % 24 + 1) as f64, i * 7 % 24 + 1)).collect();
        buckets.insert(11, (f64::NAN, 1000));
        let (out, total) = cumulative_counts(&buckets);
        let mut expected: Vec<(f64, u64)> =
            (1..=24u64).map(|bound| (bound as f64, bound * (bound + 1) / 2)).collect();
        expected.push((f64::INFINITY, 300 + 1000));
        assert_eq!(out, expected);
        assert_eq!(total, 1300);
    }

    /// `-Inf` sorts first, a `NaN` has no entry, and a repeated `+Inf` bound is one entry, last,
    /// holding the total.
    #[test]
    fn cumulative_counts_keeps_neg_inf_first_and_writes_one_inf_entry() {
        let buckets = [
            (f64::INFINITY, 1),
            (2.0, 1),
            (f64::NAN, 5),
            (f64::NEG_INFINITY, 1),
            (f64::INFINITY, 3),
        ];
        let (out, total) = cumulative_counts(&buckets);
        assert_eq!(out, vec![(f64::NEG_INFINITY, 1), (2.0, 2), (f64::INFINITY, 11)]);
        assert_eq!(total, 11);
    }

    /// An out-of-order histogram with a `NaN` bound encodes sorted, with no entry for the `NaN`
    /// and its count in `+Inf` and `_count`, and the record counts degraded once.
    #[test]
    fn a_histogram_with_an_out_of_order_nan_bound_encodes_sorted_without_it() {
        let kind = MetricKind::Histogram(Histogram {
            buckets: vec![(5.0, 2), (f64::NAN, 4), (1.0, 1), (f64::INFINITY, 1)],
            temporality: Temporality::Cumulative,
            sum: None,
            min: None,
            max: None,
        });
        let (families, registry, _) =
            encode_counted(&Resource::default(), &event_with(&[], record("m", kind)));
        assert_eq!(
            families[0].series[0].point,
            Point::Histogram {
                buckets: vec![(1.0, 1), (5.0, 3), (f64::INFINITY, 8)],
                sum: None,
                count: 8,
            }
        );
        let degraded = (
            "logit.output.metrics.degraded".to_string(),
            "reason".to_string(),
            "non_finite_component".to_string(),
        );
        let points = drained_points(&registry);
        assert_eq!(points.iter().filter(|p| **p == degraded).count(), 1, "{points:?}");
    }

    #[test]
    fn a_summary_encodes_with_its_sum_and_count() {
        let kind = MetricKind::Summary(Summary {
            quantiles: vec![(0.5, 1.0)],
            count: 7,
            sum: 42.0,
            temporality: Temporality::Cumulative,
        });
        assert_eq!(
            encode_kind(kind)[0].series[0].point,
            Point::Summary { quantiles: vec![(0.5, 1.0)], sum: Some(42.0), count: Some(7) }
        );
    }

    #[test]
    fn a_distribution_encodes_as_a_summary_with_its_extremes_and_is_counted_degraded() {
        let mut sketch = DdSketch::new();
        for v in [1.0, 2.0, 3.0, 4.0, 5.0] {
            sketch.add(v);
        }
        let (families, registry, _) = encode_counted(
            &Resource::default(),
            &event_with(&[], record("m", MetricKind::Distribution(sketch))),
        );
        assert_eq!(families[0].kind, FamilyType::Summary);
        match &families[0].series[0].point {
            Point::Summary { quantiles, sum, count } => {
                assert_eq!(quantiles.len(), SUMMARY_QUANTILES.len());
                assert_eq!(quantiles.first(), Some(&(0.0, 1.0)), "quantile 0 is the minimum");
                assert_eq!(quantiles.last(), Some(&(1.0, 5.0)), "quantile 1 is the maximum");
                assert_eq!(*sum, Some(15.0), "a sketch's sum is exact");
                assert_eq!(*count, Some(5));
            }
            other => panic!("expected a summary, got {other:?}"),
        }
        assert!(counted(
            &registry,
            "logit.output.metrics.degraded",
            ("metric_kind", "distribution")
        ));
    }

    #[test]
    fn samples_are_sketched_into_the_same_summary_shape_and_counted_degraded() {
        let (families, registry, _) = encode_counted(
            &Resource::default(),
            &event_with(&[], record("m", MetricKind::Samples(Samples::new([1.0, 2.0, 3.0])))),
        );
        assert_eq!(families[0].kind, FamilyType::Summary);
        match &families[0].series[0].point {
            Point::Summary { sum, count, .. } => {
                assert_eq!(*sum, Some(6.0));
                assert_eq!(*count, Some(3));
            }
            other => panic!("expected a summary, got {other:?}"),
        }
        assert!(counted(&registry, "logit.output.metrics.degraded", ("metric_kind", "samples")));
    }

    #[test]
    fn a_set_encodes_as_a_gauge_of_its_estimate_and_is_counted_degraded() {
        let mut hll = HyperLogLog::new();
        hll.insert(b"alice");
        hll.insert(b"bob");
        let (families, registry, _) = encode_counted(
            &Resource::default(),
            &event_with(&[], record("m", MetricKind::Set(hll))),
        );
        assert_eq!(families[0].kind, FamilyType::Gauge);
        assert_eq!(families[0].series[0].point, Point::Gauge(2.0));
        assert!(counted(&registry, "logit.output.metrics.degraded", ("metric_kind", "set")));
    }

    #[test]
    fn set_members_encode_as_a_gauge_of_the_distinct_count_and_are_counted_degraded() {
        let members = vec![
            bytes::Bytes::from_static(b"alice"),
            bytes::Bytes::from_static(b"bob"),
            bytes::Bytes::from_static(b"alice"),
        ];
        let (families, registry, _) = encode_counted(
            &Resource::default(),
            &event_with(&[], record("m", MetricKind::SetMembers(members))),
        );
        assert_eq!(families[0].series[0].point, Point::Gauge(2.0));
        assert!(counted(
            &registry,
            "logit.output.metrics.degraded",
            ("metric_kind", "set_members")
        ));
    }

    #[test]
    fn an_exponential_histogram_is_counted_by_destination() {
        let kind = MetricKind::ExponentialHistogram(ExpHistogram {
            scale: 1,
            zero_count: 0,
            zero_threshold: 0.0,
            positive: (0, vec![1]),
            negative: (0, vec![]),
            temporality: Temporality::Cumulative,
            count: 1,
            sum: None,
            min: None,
            max: None,
        });
        let event = event_with(&[], record("m", kind));

        // Exposition has no syntax for one: a format skip.
        let (families, registry, _) = encode_counted(&Resource::default(), &event);
        assert!(families.is_empty());
        let points = drained_points(&registry);
        assert!(points.contains(&(
            "logit.output.metrics.skipped".into(),
            "metric_kind".into(),
            "exponential_histogram".into()
        )));
        assert!(!points.iter().any(|(name, ..)| name == "logit.output.metrics.dropped"));

        // Remote-write carries native histograms this codec doesn't map: loss.
        let (registry, telemetry) = telemetry();
        let mut encoder = PrometheusEncoder::new()
            .with_destination(Destination::RemoteWrite)
            .with_telemetry(telemetry);
        let families =
            events_to_families(std::iter::once((&Resource::default(), &event)), &mut encoder);
        assert!(families.is_empty());
        let points = drained_points(&registry);
        assert!(points.contains(&(
            "logit.output.metrics.dropped".into(),
            "reason".into(),
            "native_histogram".into()
        )));
        assert!(!points.iter().any(|(name, ..)| name == "logit.output.metrics.skipped"));
    }

    /// Every `(name, tag key, tag value)` point `registry` recorded. Drains, so call once.
    fn drained_points(registry: &Registry) -> Vec<(String, String, String)> {
        let mut points = Vec::new();
        for event in registry.drain(0) {
            for metric in &event.metrics {
                for (key, value) in event.attributes.iter() {
                    let key = resolve(key);
                    if matches!(key, "reason" | "metric_kind") {
                        let value = value.as_str().unwrap_or_default().to_string();
                        points.push((resolve(metric.name).to_string(), key.to_string(), value));
                    }
                }
            }
        }
        points
    }

    #[test]
    fn every_decode_drop_counts_input_metrics_dropped_under_its_own_reason() {
        let mut seen = BTreeSet::new();
        for reason in DecodeDrop::ALL {
            let (registry, telemetry) = telemetry();
            PrometheusDecoder::new().with_telemetry(telemetry).dropped(reason);
            let points = drained_points(&registry);
            assert_eq!(
                points,
                [(
                    "logit.input.metrics.dropped".to_string(),
                    "reason".to_string(),
                    reason.reason().to_string()
                )],
                "{reason:?}"
            );
            assert!(seen.insert(reason.reason()), "{reason:?} shares a reason");
            assert!(
                !reason.reason().contains("dropped"),
                "{reason:?}: a reason never contains `dropped`, which names loss in a metric name"
            );
        }
    }

    #[test]
    fn every_not_encoded_outcome_counts_under_the_loss_vocabulary() {
        let mut seen = BTreeSet::new();
        for outcome in NotEncoded::ALL {
            let (registry, telemetry) = telemetry();
            PrometheusEncoder::new().with_telemetry(telemetry).not_encoded(outcome);
            let (metric, (key, value)) = outcome.counter();
            assert_eq!(
                drained_points(&registry),
                [(metric.to_string(), key.to_string(), value.to_string())],
                "{outcome:?}"
            );
            assert!(seen.insert((metric, value)), "{outcome:?} shares a counter");
            let dropped = matches!(
                outcome,
                NotEncoded::NativeHistogram
                    | NotEncoded::UnresolvedDelta
                    | NotEncoded::UnresolvedGaugeDelta
                    | NotEncoded::TypeConflict
                    | NotEncoded::NameCollision
                    | NotEncoded::InvalidLabels
                    | NotEncoded::NoTimestamp
                    | NotEncoded::DuplicateTimestamp
            );
            let expected = if dropped {
                "logit.output.metrics.dropped"
            } else if outcome == NotEncoded::EqualDuplicateTimestamp {
                "logit.output.metrics.normalized"
            } else {
                "logit.output.metrics.skipped"
            };
            assert_eq!(metric, expected, "{outcome:?}");
            if dropped {
                assert_eq!(key, "reason", "{outcome:?}: a drop is tagged by reason only");
            }
        }
    }

    #[test]
    fn a_no_recorded_value_point_is_skipped_and_counted() {
        let mut rec = record("m", MetricKind::gauge(0.0));
        rec.flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
        let (families, registry, _) = encode_counted(&Resource::default(), &event_with(&[], rec));
        assert!(families.is_empty());
        assert!(counted(
            &registry,
            "logit.output.metrics.skipped",
            ("reason", "no_recorded_value")
        ));
    }

    /// The `timestamped` flag alone asks for a sample timestamp, on the event or else the resource,
    /// whichever decoder wrote it; the flag's other values close a span for `aggregate` but don't
    /// say the timestamp is the source's.
    #[test]
    fn a_timestamp_is_emitted_only_for_an_event_flagged_timestamped() {
        let gauge = || record("m", MetricKind::gauge(1.0));
        let flagged = |value: Value| {
            event_with(&[(METRICS_CLOSED, value)], record("m", MetricKind::gauge(1.0)))
        };
        let timestamp =
            |resource: &Resource, event: &Event| encode(resource, event)[0].series[0].timestamp;
        let bare = Resource::default();

        assert_eq!(timestamp(&bare, &flagged(Value::from(CLOSED_TIMESTAMPED))), Some(RECEIVED_AT));
        for other in [Value::from(CLOSED_BUCKETED), Value::from("other"), Value::Bool(true)] {
            assert_eq!(timestamp(&bare, &flagged(other.clone())), None, "{other:?}");
        }
        assert_eq!(timestamp(&bare, &event_with(&[], gauge())), None);

        let mut stamped = Resource::default();
        stamped.attributes.insert(METRICS_CLOSED, CLOSED_TIMESTAMPED);
        assert_eq!(timestamp(&stamped, &event_with(&[], gauge())), Some(RECEIVED_AT), "resource");

        // A rewritten `Event::timestamp` is the one written.
        let mut moved = flagged(Value::from(CLOSED_TIMESTAMPED));
        moved.timestamp = 7_000_000_000;
        assert_eq!(timestamp(&bare, &moved), Some(7_000_000_000));
    }

    #[test]
    fn created_is_emitted_only_for_the_types_that_have_one_and_only_when_known() {
        let mut counter = record(
            "m",
            MetricKind::Sum(Sum {
                value: MetricValue::F64(1.0),
                temporality: Temporality::Cumulative,
                monotonic: true,
            }),
        );
        counter.start_timestamp = 1_605_281_325_000_000_000;
        let event = event_with(&[], counter);
        assert_eq!(
            encode(&Resource::default(), &event)[0].series[0].created,
            Some(1_605_281_325_000_000_000)
        );

        // A gauge has no `_created` sample in either format, so a `start_timestamp` on one has
        // nowhere to go.
        let mut gauge = record("m", MetricKind::gauge(1.0));
        gauge.start_timestamp = 1_605_281_325_000_000_000;
        let event = event_with(&[], gauge);
        assert_eq!(encode(&Resource::default(), &event)[0].series[0].created, None);
    }

    #[test]
    fn resource_and_event_attributes_both_become_labels_with_the_event_winning() {
        let resource =
            resource_with(&[("instance", Value::from("host:9100")), ("env", Value::from("dev"))]);
        let event =
            event_with(&[("env", Value::from("prod"))], record("m", MetricKind::gauge(1.0)));
        assert_eq!(
            encode(&resource, &event)[0].series[0].labels,
            labels(&[("env", "prod"), ("instance", "host:9100")])
        );
    }

    /// Target identity is *not* in the private namespace: `instance` renders,
    /// `_logit.prometheus.target` does not.
    #[test]
    fn the_instance_resource_attribute_renders_while_prometheus_target_is_consumed() {
        let resource = resource_with(&[
            (LABEL_INSTANCE, Value::from("node-exporter:9100")),
            (ATTR_TARGET, Value::from("http://node-exporter:9100/metrics")),
        ]);
        let event = event_with(&[], record("m", MetricKind::gauge(1.0)));
        assert_eq!(
            encode(&resource, &event)[0].series[0].labels,
            labels(&[("instance", "node-exporter:9100")])
        );
    }

    /// Only the private prefix is skipped: an attribute spelled `prometheus.<name>` with no
    /// `_logit.` in front is an ordinary label, as it is at every other sink.
    #[test]
    fn an_unprefixed_prometheus_dotted_attribute_renders_as_a_label() {
        let event = event_with(
            &[("prometheus.type", Value::from("x"))],
            record("m", MetricKind::gauge(1.0)),
        );
        let families = encode(&Resource::default(), &event);
        assert_eq!(families[0].kind, FamilyType::Gauge);
        assert_eq!(families[0].series[0].labels, labels(&[("prometheus_type", "x")]));
    }

    #[test]
    fn every_representable_value_kind_stringifies_and_the_rest_are_counted_omitted() {
        let event = event_with(
            &[
                ("s", Value::from("text")),
                ("b", Value::Bool(true)),
                ("i", Value::I64(-3)),
                ("u", Value::U64(7)),
                ("f", Value::F64(1.5)),
                ("null", Value::Null),
                ("bytes", Value::Bytes(bytes::Bytes::from_static(b"\xff"))),
                ("ts", Value::Timestamp(1)),
                // An *empty* array: a non-empty one renders its last element (see
                // `a_multi_value_label_renders_its_last_element_and_is_counted`).
                ("arr", Value::Array(Vec::new())),
                ("map", Value::Map(Box::new(AttrMap::new()))),
            ],
            record("m", MetricKind::gauge(1.0)),
        );
        let (families, registry, _) = encode_counted(&Resource::default(), &event);
        assert_eq!(
            families[0].series[0].labels,
            labels(&[("b", "true"), ("f", "1.5"), ("i", "-3"), ("s", "text"), ("u", "7")])
        );
        assert!(counted(&registry, "logit.output.labels.omitted", ("reason", "unrepresentable")));
    }

    /// Every `(point name, reason)` pair the registry holds, from **one** drain -- [`counted`]
    /// drains, so a test that asserts one counter fired *and* another didn't cannot call it twice
    /// (the second call would see an already-empty registry and pass vacuously).
    fn label_points(registry: &Registry) -> Vec<(String, String)> {
        registry
            .drain(0)
            .iter()
            .flat_map(|event| {
                let reason = event
                    .attributes
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                event
                    .metrics
                    .iter()
                    .map(|m| (resolve(m.name).to_string(), reason.clone()))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn has_point(points: &[(String, String)], name: &str, reason: &str) -> bool {
        points.iter().any(|(n, r)| n == name && r == reason)
    }

    /// A private attribute, on the event or the resource, is never a label and never counted,
    /// even one whose value no label could hold; a public sibling still renders.
    #[test]
    fn a_private_attribute_is_never_a_label_and_never_counted() {
        let event = event_with(
            &[
                ("_logit.x", Value::from("e")),
                ("_logit.m", Value::Map(Box::new(AttrMap::new()))),
                ("env", Value::from("prod")),
            ],
            record("m", MetricKind::gauge(1.0)),
        );
        let resource = resource_with(&[("_logit.y", Value::from("r"))]);
        let (families, registry, _) = encode_counted(&resource, &event);
        assert_eq!(families[0].series[0].labels, labels(&[("env", "prod")]));
        let points = label_points(&registry);
        assert!(
            points.iter().all(|(name, _)| !name.starts_with("logit.output.labels.")),
            "{points:?}"
        );
    }

    /// A multi-value attribute renders its **last** representable element, counted as normalized.
    #[test]
    fn a_multi_value_label_renders_its_last_element_and_is_counted() {
        let event = event_with(
            &[("team", Value::Array(vec![Value::from("a"), Value::from("b")]))],
            record("m", MetricKind::gauge(1.0)),
        );
        let (families, registry, _) = encode_counted(&Resource::default(), &event);
        assert_eq!(families[0].series[0].labels, labels(&[("team", "b")]));
        let points = label_points(&registry);
        assert!(has_point(&points, "logit.output.labels.normalized", "multi_value"), "{points:?}");
        assert!(
            !has_point(&points, "logit.output.labels.omitted", "unrepresentable"),
            "a normalized label is not also a dropped one: {points:?}"
        );
    }

    #[test]
    fn a_multi_value_label_walks_back_past_an_unrepresentable_last_element() {
        let event = event_with(
            &[(
                "team",
                Value::Array(vec![
                    Value::from("a"),
                    Value::from("b"),
                    Value::Null,
                    Value::Map(Box::new(AttrMap::new())),
                ]),
            )],
            record("m", MetricKind::gauge(1.0)),
        );
        let (families, registry, _) = encode_counted(&Resource::default(), &event);
        assert_eq!(families[0].series[0].labels, labels(&[("team", "b")]));
        assert!(counted(&registry, "logit.output.labels.normalized", ("reason", "multi_value")));
    }

    /// An array with no element to fall back to is `dropped{reason="unrepresentable"}`, not
    /// normalized.
    #[test]
    fn an_all_unrepresentable_array_label_is_omitted_unrepresentable_not_normalized() {
        for elements in [Vec::new(), vec![Value::Null, Value::Timestamp(1)]] {
            let event = event_with(
                &[("team", Value::Array(elements))],
                record("m", MetricKind::gauge(1.0)),
            );
            let (families, registry, _) = encode_counted(&Resource::default(), &event);
            assert!(families[0].series[0].labels.is_empty());
            let points = label_points(&registry);
            assert!(
                has_point(&points, "logit.output.labels.omitted", "unrepresentable"),
                "{points:?}"
            );
            assert!(
                !has_point(&points, "logit.output.labels.normalized", "multi_value"),
                "nothing was normalized: {points:?}"
            );
        }
    }

    /// Two attribute names that sanitize onto one label: the one whose *original* name sorts first
    /// wins, so the outcome depends on the data rather than on interning order.
    #[test]
    fn a_post_sanitization_label_collision_keeps_the_first_original_name_and_is_counted() {
        let event = event_with(
            &[("a.b", Value::from("first")), ("a-b", Value::from("second"))],
            record("m", MetricKind::gauge(1.0)),
        );
        let (families, registry, _) = encode_counted(&Resource::default(), &event);
        assert_eq!(families[0].series[0].labels, labels(&[("a_b", "second")]), "`a-b` < `a.b`");
        assert!(counted(&registry, "logit.output.labels.omitted", ("reason", "collision")));
    }

    #[test]
    fn an_attribute_colliding_with_a_generated_label_is_omitted_and_counted() {
        let histogram = MetricKind::Histogram(Histogram {
            buckets: vec![(1.0, 1), (f64::INFINITY, 0)],
            temporality: Temporality::Cumulative,
            sum: None,
            min: None,
            max: None,
        });
        let event = event_with(&[("le", Value::from("0.5"))], record("m", histogram));
        let (families, registry, _) = encode_counted(&Resource::default(), &event);
        assert!(families[0].series[0].labels.is_empty(), "`le` belongs to the bucket lines");
        assert!(counted(&registry, "logit.output.labels.omitted", ("reason", "reserved")));

        let summary = MetricKind::Summary(Summary {
            quantiles: vec![(0.5, 1.0)],
            count: 1,
            sum: 1.0,
            temporality: Temporality::Cumulative,
        });
        let event = event_with(&[("quantile", Value::from("0.5"))], record("m", summary));
        let (families, registry, _) = encode_counted(&Resource::default(), &event);
        assert!(families[0].series[0].labels.is_empty());
        assert!(counted(&registry, "logit.output.labels.omitted", ("reason", "reserved")));
    }

    #[test]
    fn description_and_unit_become_help_and_unit() {
        let mut rec = record("m", MetricKind::gauge(1.0));
        rec.description = Some(intern("what it measures"));
        rec.unit = Some(intern("seconds"));
        let families = encode(&Resource::default(), &event_with(&[], rec));
        assert_eq!(families[0].help.as_deref(), Some("what it measures"));
        assert_eq!(families[0].unit.as_deref(), Some("seconds"));
    }

    #[test]
    fn two_records_of_one_name_disagreeing_on_type_keep_the_first_and_count_the_rest() {
        let (registry, telemetry) = telemetry();
        let mut encoder = PrometheusEncoder::new().with_telemetry(telemetry);
        let resource = Resource::default();
        let gauge = event_with(&[], record("m", MetricKind::gauge(1.0)));
        let counter = event_with(
            &[],
            record(
                "m",
                MetricKind::Sum(Sum {
                    value: MetricValue::F64(2.0),
                    temporality: Temporality::Cumulative,
                    monotonic: true,
                }),
            ),
        );
        let families = events_to_families(
            [(&resource, &gauge), (&resource, &counter)].into_iter(),
            &mut encoder,
        );
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].kind, FamilyType::Gauge);
        assert!(counted(&registry, "logit.output.metrics.dropped", ("reason", "type_conflict")));
    }

    #[test]
    fn one_name_and_one_label_set_twice_keeps_the_later_series() {
        let resource = Resource::default();
        let first = event_with(&[("k", Value::from("v"))], record("m", MetricKind::gauge(1.0)));
        let second = event_with(&[("k", Value::from("v"))], record("m", MetricKind::gauge(2.0)));
        let families = events_to_families(
            [(&resource, &first), (&resource, &second)].into_iter(),
            &mut PrometheusEncoder::new(),
        );
        assert_eq!(families[0].series.len(), 1, "one label set is one series");
        assert_eq!(families[0].series[0].point, Point::Gauge(2.0), "latest wins");
    }

    #[test]
    fn families_come_back_sorted_by_name_and_series_by_label_set() {
        let resource = Resource::default();
        let z = event_with(&[("k", Value::from("z"))], record("z_metric", MetricKind::gauge(1.0)));
        let a2 = event_with(&[("k", Value::from("b"))], record("a_metric", MetricKind::gauge(1.0)));
        let a1 = event_with(&[("k", Value::from("a"))], record("a_metric", MetricKind::gauge(1.0)));
        let families = events_to_families(
            [(&resource, &z), (&resource, &a2), (&resource, &a1)].into_iter(),
            &mut PrometheusEncoder::new(),
        );
        assert_eq!(
            families.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
            vec!["a_metric", "z_metric"]
        );
        assert_eq!(
            families[0].series.iter().map(|s| s.labels[0].1.as_str()).collect::<Vec<_>>(),
            vec!["a", "b"]
        );
    }

    #[test]
    fn an_unsanitary_metric_name_is_substituted_not_deleted() {
        let families = encode(
            &Resource::default(),
            &event_with(&[], record("some.metric-name", MetricKind::gauge(1.0))),
        );
        assert_eq!(families[0].name, "some_metric_name");
    }

    // --- sanitization ---------------------------------------------------------------------------

    #[test]
    fn metric_name_sanitization_substitutes_and_keeps_colons() {
        assert_eq!(sanitize_metric_name("job:rate5m"), "job:rate5m");
        assert_eq!(sanitize_metric_name("some.metric name"), "some_metric_name");
        // One `_` per *character*, not per byte -- `statsd_out`'s `sanitize_into` precedent.
        assert_eq!(sanitize_metric_name("ünïcode"), "_n_code");
    }

    #[test]
    fn a_leading_digit_gains_an_underscore_rather_than_replacing_the_digit() {
        assert_eq!(sanitize_metric_name("5xx_total"), "_5xx_total");
        assert_eq!(sanitize_label_name("2fa"), "_2fa");
    }

    #[test]
    fn an_empty_name_sanitizes_to_a_single_underscore() {
        assert_eq!(sanitize_metric_name(""), "_");
        assert_eq!(sanitize_label_name(""), "_");
    }

    #[test]
    fn a_colon_is_forbidden_in_a_label_name_though_legal_in_a_metric_name() {
        assert_eq!(sanitize_label_name("job:rate"), "job_rate");
    }

    // --- post-sanitization metric-name collisions -----------------------------------------------

    /// Two model names sanitizing onto one wire name: the one whose *model* name sorts first wins.
    #[test]
    fn two_metric_names_sanitizing_onto_one_expose_the_first_and_count_the_rest() {
        let (registry, telemetry) = telemetry();
        let mut encoder = PrometheusEncoder::new().with_telemetry(telemetry);
        let resource = Resource::default();
        let dotted = event_with(&[], record("a.b", MetricKind::gauge(1.0)));
        let dashed = event_with(&[], record("a-b", MetricKind::gauge(2.0)));
        let families = events_to_families(
            [(&resource, &dotted), (&resource, &dashed)].into_iter(),
            &mut encoder,
        );
        assert_eq!(families.len(), 1, "one wire name is one family");
        assert_eq!(families[0].name, "a_b");
        // `a-b` < `a.b`, so the dashed one wins however the events arrive.
        assert_eq!(families[0].series[0].point, Point::Gauge(2.0));
        assert!(counted(&registry, "logit.output.metrics.dropped", ("reason", "name_collision")));
    }

    /// The same, with the winner arriving *second*: the already-started family is displaced.
    #[test]
    fn a_colliding_name_that_sorts_first_displaces_the_family_already_started() {
        let (registry, telemetry) = telemetry();
        let mut encoder = PrometheusEncoder::new().with_telemetry(telemetry);
        let resource = Resource::default();
        let dotted = event_with(&[], record("a.b", MetricKind::gauge(1.0)));
        let dashed = event_with(&[], record("a-b", MetricKind::gauge(2.0)));
        // Reverse arrival order from the test above; the result must be identical.
        let families = events_to_families(
            [(&resource, &dashed), (&resource, &dotted)].into_iter(),
            &mut encoder,
        );
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].series[0].point, Point::Gauge(2.0));
        assert!(counted(&registry, "logit.output.metrics.dropped", ("reason", "name_collision")));
    }

    /// A series dropped as a type-conflict or name-collision loser counts only that drop: the
    /// `bool_value` normalization its conversion held back describes a series that isn't written.
    /// That covers the arriving loser of either kind and a displaced family.
    #[test]
    fn a_conflict_or_collision_loser_counts_none_of_its_held_back_counts() {
        let bool_gauge =
            |name: &str| event_with(&[], record(name, MetricKind::Gauge(MetricValue::Bool(true))));
        let counter = event_with(
            &[],
            record(
                "m",
                MetricKind::Sum(Sum {
                    value: MetricValue::Bool(true),
                    temporality: Temporality::Cumulative,
                    monotonic: true,
                }),
            ),
        );
        let float = |name: &str| event_with(&[], record(name, MetricKind::gauge(1.0)));
        let (m, winner, dashed) = (float("m"), float("a-b"), float("a-b"));
        let (loser, displaced) = (bool_gauge("a.b"), bool_gauge("a.b"));
        let cases: [(&str, [&Event; 2], &str); 3] = [
            ("type conflict", [&m, &counter], "type_conflict"),
            ("arriving name collision", [&winner, &loser], "name_collision"),
            ("displaced family", [&displaced, &dashed], "name_collision"),
        ];
        for (case, events, reason) in cases {
            let (registry, telemetry) = telemetry();
            let mut encoder = PrometheusEncoder::new().with_telemetry(telemetry);
            let resource = Resource::default();
            events_to_families(events.into_iter().map(|e| (&resource, e)), &mut encoder);
            let points = registry.drain(0);
            let has = |metric: &str, value: &str| {
                points.iter().any(|event| {
                    event.metrics.iter().any(|m| resolve(m.name) == metric)
                        && event.attributes.get("reason").and_then(|v| v.as_str()) == Some(value)
                })
            };
            assert!(has("logit.output.metrics.dropped", reason), "{case}");
            assert!(!has("logit.output.metrics.normalized", "bool_value"), "{case}");
        }
    }

    /// A name that needs sanitizing but collides with nothing is renamed, not counted.
    #[test]
    fn a_sanitized_name_with_no_collision_is_not_counted() {
        let (registry, telemetry) = telemetry();
        let mut encoder = PrometheusEncoder::new().with_telemetry(telemetry);
        let resource = Resource::default();
        let event = event_with(&[], record("a.b", MetricKind::gauge(1.0)));
        let families = events_to_families(std::iter::once((&resource, &event)), &mut encoder);
        assert_eq!(families[0].name, "a_b");
        assert!(!counted(&registry, "logit.output.metrics.dropped", ("reason", "name_collision")));
    }

    // --- stale markers and the three transport switches ----------------------------------------

    /// A `Stale` series decodes to its family's own kind, empty, flagged `FLAG_NO_RECORDED_VALUE`.
    #[test]
    fn a_stale_point_decodes_to_its_family_type_with_the_no_recorded_value_flag() {
        for kind in [
            FamilyType::Counter,
            FamilyType::Gauge,
            FamilyType::Unknown,
            FamilyType::Untyped,
            FamilyType::Info,
            FamilyType::StateSet,
            FamilyType::Histogram,
            FamilyType::GaugeHistogram,
            FamilyType::Summary,
        ] {
            let families = [family("m", kind, vec![Series::new(vec![], Point::Stale)])];
            let events = decode(&families);
            assert_eq!(events.len(), 1, "{kind:?}");
            let record = &events[0].metrics[0];
            assert!(record.is_no_recorded_value(), "{kind:?} must set the flag");
            let shaped = match (kind, &record.kind) {
                (FamilyType::Counter, MetricKind::Sum(sum)) => {
                    sum.value.as_f64().is_nan()
                        && sum.monotonic
                        && sum.temporality == Temporality::Cumulative
                }
                (
                    FamilyType::Gauge
                    | FamilyType::Unknown
                    | FamilyType::Untyped
                    | FamilyType::Info
                    | FamilyType::StateSet,
                    MetricKind::Gauge(v),
                ) => v.as_f64().is_nan(),
                (
                    FamilyType::Histogram | FamilyType::GaugeHistogram,
                    MetricKind::Histogram(histogram),
                ) => histogram.buckets.is_empty() && histogram.sum.is_none(),
                (FamilyType::Summary, MetricKind::Summary(summary)) => {
                    summary.quantiles.is_empty() && summary.count == 0 && summary.sum == 0.0
                }
                _ => false,
            };
            assert!(shaped, "{kind:?} got {:?}", record.kind);
            // The carrier and the neutral types still say what they were on the wire.
            let (key, expected) = if kind.needs_marker() {
                (ATTR_TYPE, Some(kind.as_str()))
            } else {
                (METRICS_TYPE, kind.neutral_type())
            };
            assert_eq!(
                events[0].attributes.get(key).and_then(|v| v.as_str()),
                expected,
                "{kind:?}"
            );
        }
    }

    /// Only the stale NaN's bit pattern is stale; every other NaN, `f64::NAN` included, is not.
    #[test]
    fn only_the_reserved_bit_pattern_is_a_stale_nan() {
        assert!(is_stale_nan(f64::from_bits(STALE_NAN_BITS)));
        assert!(f64::from_bits(STALE_NAN_BITS).is_nan());
        assert!(!is_stale_nan(f64::NAN));
        assert!(!is_stale_nan(0.0));
        assert!(!is_stale_nan(f64::INFINITY));
    }

    /// `with_timestamp_marker(false)`: the timestamp still lands on the event, the flag doesn't.
    #[test]
    fn the_timestamp_marker_can_be_turned_off_without_losing_the_timestamp() {
        let stamped = [family(
            "m",
            FamilyType::Gauge,
            vec![Series { timestamp: Some(42), ..Series::new(vec![], Point::Gauge(1.0)) }],
        )];

        let marked = families_to_events(&stamped, RECEIVED_AT, &mut PrometheusDecoder::new());
        assert_eq!(marked[0].timestamp, 42);
        assert_eq!(
            marked[0].attributes.get(METRICS_CLOSED).and_then(Value::as_str),
            Some(CLOSED_TIMESTAMPED)
        );

        let mut decoder = PrometheusDecoder::new().with_timestamp_marker(false);
        let bare = families_to_events(&stamped, RECEIVED_AT, &mut decoder);
        assert_eq!(bare[0].timestamp, 42);
        assert_eq!(bare[0].attributes.get(METRICS_CLOSED), None);

        // A series with no timestamp of its own is unaffected either way.
        let unstamped =
            [family("m", FamilyType::Gauge, vec![Series::new(vec![], Point::Gauge(1.0))])];
        let events = families_to_events(&unstamped, RECEIVED_AT, &mut decoder);
        assert_eq!(events[0].timestamp, RECEIVED_AT);
        assert_eq!(events[0].attributes.get(METRICS_CLOSED), None);
    }

    /// `with_timestamps_always(true)`: every series gets one, flagged or not.
    #[test]
    fn timestamps_always_fills_a_timestamp_the_flag_did_not_ask_for() {
        let event = event_with(&[], record("m", MetricKind::gauge(1.0)));
        assert_eq!(encode(&Resource::default(), &event)[0].series[0].timestamp, None);

        let mut encoder = PrometheusEncoder::new().with_timestamps_always(true);
        let families =
            events_to_families(std::iter::once((&Resource::default(), &event)), &mut encoder);
        assert_eq!(families[0].series[0].timestamp, Some(RECEIVED_AT));
    }

    fn encode_stale(event: &Event) -> (Vec<MetricFamily>, Arc<Registry>) {
        let (registry, telemetry) = telemetry();
        let mut encoder =
            PrometheusEncoder::new().with_stale_markers(true).with_telemetry(telemetry);
        let families =
            events_to_families(std::iter::once((&Resource::default(), event)), &mut encoder);
        (families, registry)
    }

    /// `with_stale_markers(true)`: single-series kinds convert, on the family type they'd have had.
    #[test]
    fn stale_markers_convert_the_single_series_kinds() {
        let sum = |temporality| {
            MetricKind::Sum(Sum { value: MetricValue::F64(0.0), temporality, monotonic: true })
        };
        let cases = [
            (None, sum(Temporality::Cumulative), FamilyType::Counter),
            // Temporality is not consulted: a stale marker has no reading to be delta about, so
            // the `unresolved_delta` drop that would apply to a *valued* record does not apply here.
            (None, sum(Temporality::Delta), FamilyType::Counter),
            (None, MetricKind::gauge(0.0), FamilyType::Gauge),
            (Some((ATTR_TYPE, "unknown")), MetricKind::gauge(0.0), FamilyType::Unknown),
            (Some((ATTR_TYPE, "untyped")), MetricKind::gauge(0.0), FamilyType::Untyped),
            (Some((METRICS_TYPE, "info")), MetricKind::gauge(0.0), FamilyType::Info),
            (Some((METRICS_TYPE, "stateset")), MetricKind::gauge(0.0), FamilyType::StateSet),
        ];
        for (declared, kind, expected) in cases {
            let mut rec = record("m", kind.clone());
            rec.flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
            let attrs: Vec<(&str, Value)> =
                declared.into_iter().map(|(key, d)| (key, Value::str(d))).collect();
            let (families, _) = encode_stale(&event_with(&attrs, rec));
            assert_eq!(families.len(), 1, "{kind:?} {declared:?}");
            assert_eq!(families[0].kind, expected, "{kind:?} {declared:?}");
            assert_eq!(families[0].series[0].point, Point::Stale, "{kind:?} {declared:?}");
        }
    }

    /// A flagged non-monotonic sum is still a `gauge`, and still counted as the degradation it is.
    #[test]
    fn a_stale_non_monotonic_sum_degrades_to_a_gauge() {
        let mut rec = record(
            "m",
            MetricKind::Sum(Sum {
                value: MetricValue::F64(0.0),
                temporality: Temporality::Cumulative,
                monotonic: false,
            }),
        );
        rec.flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
        let (families, registry) = encode_stale(&event_with(&[], rec));
        assert_eq!(families[0].kind, FamilyType::Gauge);
        assert!(counted(
            &registry,
            "logit.output.metrics.degraded",
            ("metric_kind", "non_monotonic_sum")
        ));
    }

    /// Multi-series kinds stay skipped even with `with_stale_markers(true)`.
    #[test]
    fn stale_markers_do_not_convert_the_multi_series_kinds() {
        let kinds = [
            MetricKind::Histogram(Histogram {
                buckets: vec![(1.0, 1)],
                temporality: Temporality::Cumulative,
                sum: Some(1.0),
                min: None,
                max: None,
            }),
            MetricKind::Summary(Summary {
                quantiles: vec![],
                count: 1,
                sum: 1.0,
                temporality: Temporality::Cumulative,
            }),
            MetricKind::Set(HyperLogLog::new()),
        ];
        for kind in kinds {
            let mut rec = record("m", kind.clone());
            rec.flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
            let (families, registry) = encode_stale(&event_with(&[], rec));
            assert!(families.is_empty(), "{kind:?}");
            assert!(
                counted(&registry, "logit.output.metrics.skipped", ("reason", "no_recorded_value")),
                "{kind:?}"
            );
        }
    }

    /// Without `with_stale_markers(true)`, a flagged record is skipped.
    #[test]
    fn stale_markers_are_off_by_default() {
        let mut rec = record("m", MetricKind::gauge(0.0));
        rec.flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
        assert!(encode(&Resource::default(), &event_with(&[], rec)).is_empty());
    }

    /// A `Stale` family round-trips unchanged with both remote-write switches on.
    #[test]
    fn a_stale_series_round_trips_through_the_model_with_the_switches_on() {
        for kind in [
            FamilyType::Counter,
            FamilyType::Gauge,
            FamilyType::Unknown,
            FamilyType::Untyped,
            FamilyType::Info,
            FamilyType::StateSet,
        ] {
            let families = vec![family(
                "m",
                kind,
                vec![Series {
                    timestamp: Some(RECEIVED_AT),
                    ..Series::new(labels(&[("shard", "3")]), Point::Stale)
                }],
            )];
            let mut decoder = PrometheusDecoder::new().with_timestamp_marker(false);
            let events = families_to_events(&families, 0, &mut decoder);
            let resource = Resource::default();
            let mut encoder =
                PrometheusEncoder::new().with_stale_markers(true).with_timestamps_always(true);
            let round_tripped =
                events_to_families(events.iter().map(|event| (&resource, event)), &mut encoder);
            assert_eq!(round_tripped, families, "{kind:?}");
        }
    }
}
