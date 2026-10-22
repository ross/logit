//! The Graphite/Carbon codec, both directions and both wire protocols: [`GraphiteDecoder`] turns
//! carbon plaintext lines or one pickle batch payload into events, [`GraphiteEncoder`] packs a
//! batch of events back into lines or pickle frames. The pickle writer and the restricted pickle
//! reader live in [`pickle`]; nothing in [`decode`] or [`encode`] knows a pickle opcode.
//!
//! **This module doc is the codec's canonical mapping table and normalization list**, which docs,
//! tests, and examples point at.
//! [ADR `graphite-carbon-relay`](../../../../docs/adr/graphite-carbon-relay.md) is the decision
//! record. It carries the normalization list with identical numbering; keep the two in step.
//!
//! ## Wire shape, in one paragraph
//!
//! Carbon speaks two protocols for the same four facts: a dotted path, an optional tag set, one
//! number, one whole second. **Plaintext** (TCP or UDP, port [`DEFAULT_PLAINTEXT_PORT`]) is
//! `path[;k=v...] value timestamp\n`, three whitespace-separated fields per line. **Pickle** (TCP
//! only, port [`DEFAULT_PICKLE_PORT`]) is a 4-byte **big-endian** length prefix (Twisted's
//! `Int32StringReceiver`, whose `MAX_LENGTH` is [`DEFAULT_MAX_FRAME_BYTES`]) followed by a pickled
//! `[(path, (timestamp, value)), ...]`. Neither wire has a type, unit, temporality, or
//! description, so the decoded model is [`logit_core::MetricKind::Gauge`] only.
//!
//! ## No `graphite.*` namespace, and no well-known attributes
//!
//! Unlike [`crate::collectd`], [`crate::syslog`], or [`crate::prometheus`], **this pair defines no
//! `ATTR_*` constants and no `graphite.*` attribute namespace**. Each of the four wire facts is
//! stored once, with no lossy normalization: the path *is* [`logit_core::MetricRecord::name`], the
//! tags *are* event attributes, the number *is* the `Gauge` payload, the second *is*
//! [`logit_core::Event::timestamp`]. Rule (b) of
//! [ADR `lossless-transit`](../../../../docs/adr/lossless-transit.md) (a raw carrier outranks a
//! normalized field) has no conflict to resolve here.
//!
//! So a `lua`/`set` stage that renames [`logit_core::MetricRecord::name`] changes the wire path.
//! That is the intended way to rename a series (neither component has a `prefix:`/`template:`
//! field), and `docs/deploying.md` says so.
//!
//! ## Decode: wire → model
//!
//! One [`logit_core::Event`] carrying exactly one [`logit_core::MetricRecord`] per line (plaintext)
//! or per datapoint (pickle). Tags are **event** attributes, never resource ones: the accumulator
//! keys batches on `Arc::ptr_eq` of the resource ([`crate::collectd`]'s module doc).
//!
//! | Wire | Model | Counter / diag |
//! |---|---|---|
//! | one line / one pickle datapoint | one `Event`, one `MetricRecord` | -- |
//! | `path` (everything before the first `;`) | `name = intern(path)`, `Gauge(F64(v))` | -- |
//! | `;name=value` | event attribute, `Value::Str` over a zero-copy [`bytes::Bytes`] slice of the input; a copy when the path is a protocol-0 pickle string the reader decoded into its scratch ([`pickle`]'s "Bounds") | -- |
//! | repeated tag key | last occurrence wins | `logit.input.tags.normalized{reason="duplicate_key"}` + diag `duplicate_tag_key` |
//! | tag name under the private `_logit.` prefix | omitted, so a sender's tag never reads back as a carrier ([ADR `private-attribute-namespace`](../../../../docs/adr/private-attribute-namespace.md)); the line still decodes | `logit.input.tags.omitted{reason="reserved_prefix"}` |
//! | finite value (`3`, `-1.5`, `1e5`, or a pickle `"3.14"` string) | `Gauge(F64(v))`: carbon's value is a double, so an integer reads as `F64` too | -- |
//! | NaN / ±inf | line dropped | `logit.input.metrics.dropped{reason="non_finite_value"}` + diag `non_finite_value` |
//! | `timestamp == -1` | `received_at` (carbon's own rule) | -- |
//! | `timestamp > 0`, integral or fractional | whole seconds and the sub-second part scaled to nanoseconds separately, the part rounded half away from zero, saturating at `i64::MAX`; under half a nanosecond reads as `0` | -- |
//! | any other `timestamp` (`<= 0`, non-finite, unparseable) | dropped | `logit.input.metrics.dropped{reason="bad_timestamp"}` + diag `bad_timestamp` |
//! | not exactly 3 whitespace-separated fields; an unparseable value; non-UTF-8 bytes; an empty path | dropped | `logit.input.metrics.dropped{reason="bad_line"}` + diag `bad_line` |
//! | malformed tag (a `;` segment with no `=`, an empty name, or an empty value) | the **whole line** is dropped -- carbon's own `TaggedSeries.parse` raises rather than dropping the one tag | `logit.input.metrics.dropped{reason="bad_tag"}` + diag `bad_tag` |
//! | empty / ASCII-whitespace-only line | skipped, **uncounted** (packet padding, a trailing `\n`); a line of Unicode whitespace is a `bad_line` | -- |
//! | line longer than `max_line_bytes` (TCP) | the framer drops it and resynchronizes at the next `\n`; the line after it still decodes, and the connection stays up | `logit.input.frames.dropped{reason="oversize"}`, or `reason="drained"` when the line crossed the bound before its `\n` arrived, after which the connection discards input until its next `\n` + diag `framing_error` |
//! | pickle frame longer than `max_frame_bytes` | the connection is closed -- there is no resync point in a length-framed stream | `logit.input.frames.dropped{reason="oversize"}` + diag `framing_error` |
//! | a disallowed pickle opcode, a declared length past the payload, a protocol-0 argument the reader refuses ([`pickle`]'s "Protocol 0"), a string that isn't UTF-8 once decoded, the depth/item caps, or a non-empty list item in a list grown by `APPEND`/`APPENDS` ([`pickle::PickleReader::read_datapoints`]) | `CodecError::Malformed`, the whole frame is dropped | `logit.input.frames.dropped{reason="undecodable"}`, counted by the listener's driver, + diag `bad_pickle` |
//! | any other pickle item that is not `(str, (num, num))` | **that datapoint** is dropped; the rest of the frame still decodes | `logit.input.metrics.dropped{reason="bad_shape"}` + diag `bad_shape` |
//! | `Resource` / `Scope` | the decoder's own shared default / `None` | -- |
//!
//! Framing is the listener's job: the two oversize rows are counted and diagnosed by the shared TCP
//! driver's [`Framer`](crate::framing::Framer), not by [`GraphiteDecoder`], and the driver counts
//! the whole-frame pickle failure's `frames.dropped` when [`GraphiteDecoder`] returns its error.
//! Every other row is emitted here.
//!
//! ## Encode: model → wire
//!
//! Counter prefix `logit.output.metrics.dropped{reason=…}` unless noted. A record with nothing to
//! carry, or a kind the operator chose to skip, is `logit.output.metrics.skipped` instead, tagged
//! `{metric_kind=…}` for a whole kind, the shape every other sink uses.
//!
//! The tables below are generated from `graphite_out`'s column of `spec/transit/mapping.yaml`, the
//! canonical copy, by `script/transit docs`, and a test fails when they're stale: change the table
//! there, not here. "Consumer" means `graphite_in` reading the lines or frames back.
//!
//! <!-- transit:doc:graphite_out -->
//!
//! Per metric kind, and per record feature that decides a record's fate:
//!
//! | Row | Class | Reason or counter | What a consumer sees |
//! |---|---|---|---|
//! | `sum/delta_monotonic` | degraded | `logit.output.metrics.degraded{metric_kind=monotonic_delta_sum}` | The bare value as a datapoint, its temporality and monotonicity dropped, which graphite_in reads back as a gauge. |
//! | `sum/delta_nonmonotonic` | degraded | `logit.output.metrics.degraded{metric_kind=non_monotonic_delta_sum}` | The bare value, read back as a gauge, as for a monotonic delta sum. |
//! | `sum/cumulative_monotonic` | degraded | `logit.output.metrics.degraded{metric_kind=cumulative_sum}` | The running total as a bare value, read back as a gauge. |
//! | `sum/cumulative_nonmonotonic` | degraded | `logit.output.metrics.degraded{metric_kind=cumulative_sum}` | The running total as a bare value, read back as a gauge. |
//! | `sum/f64` | exact | -- | The value is written as the shortest float that reads back to the same bits. |
//! | `sum/i64` | normalized | `f64_value` | Plaintext writes every digit and pickle the nearest `f64`, and graphite_in reads either back as a float, rounded beyond 2^53. |
//! | `sum/u64` | normalized | `f64_value` | Plaintext writes every digit and pickle the nearest `f64`, and graphite_in reads either back as a float, rounded beyond 2^53. |
//! | `sum/bool` | normalized | `bool_value`; `logit.output.metrics.normalized{reason=bool_value}` | A boolean is written as `1` or `0` and reads back as that number. |
//! | `gauge/f64` | exact | -- | A `path value timestamp` line, or a pickled datapoint. |
//! | `gauge/i64` | normalized | `f64_value` | Plaintext writes every digit and pickle the nearest `f64`, and graphite_in reads either back as a float, rounded beyond 2^53. |
//! | `gauge/u64` | normalized | `f64_value` | Plaintext writes every digit and pickle the nearest `f64`, and graphite_in reads either back as a float, rounded beyond 2^53. |
//! | `gauge/bool` | normalized | `bool_value`; `logit.output.metrics.normalized{reason=bool_value}` | A boolean is written as `1` or `0` and reads back as that number. |
//! | `gauge/negative` | exact | -- | A negative value, written as is. |
//! | `gauge_delta/f64` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | Carbon has no relative gauge, so an unresolved adjustment is dropped, with a throttled `gauge_delta_unresolved` warning; an `aggregate` upstream resolves it into a gauge. |
//! | `gauge_delta/i64` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | Dropped, as for a float adjustment. |
//! | `gauge_delta/u64` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | Dropped, as for a float adjustment. |
//! | `gauge_delta/bool` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | Dropped, as for a float adjustment. |
//! | `samples`, under `multi_value: expand` | degraded | `logit.output.metrics.degraded{metric_kind=samples}` | Under `multi_value: expand`, sketched, each value weighted by its inverse sample rate, then dotted `.count`, `.sum`, `.min`, `.max`, and `.q0_5` to `.q0_99` datapoints. |
//! | `samples`, otherwise | skipped | `logit.output.metrics.skipped{metric_kind=samples}` | Under `multi_value: skip`, the default, no datapoint is written. |
//! | `set_members`, under `multi_value: expand` | degraded | `logit.output.metrics.degraded{metric_kind=set_members}` | Under `multi_value: expand`, the distinct member count as a `.count` datapoint; the members are gone. |
//! | `set_members`, otherwise | skipped | `logit.output.metrics.skipped{metric_kind=set_members}` | Under `multi_value: skip`, the default, no datapoint is written. |
//! | `distribution`, under `multi_value: expand` | degraded | `logit.output.metrics.degraded{metric_kind=distribution}` | Under `multi_value: expand`, dotted `.count`, `.sum`, `.min`, `.max`, and `.q0_5` to `.q0_99` datapoints; the sketch is gone, so quantiles from two senders don't merge. |
//! | `distribution`, otherwise | skipped | `logit.output.metrics.skipped{metric_kind=distribution}` | Under `multi_value: skip`, the default, no datapoint is written. |
//! | `set`, under `multi_value: expand` | degraded | `logit.output.metrics.degraded{metric_kind=set}` | Under `multi_value: expand`, the estimate as a `.count` datapoint; the registers are gone. |
//! | `set`, otherwise | skipped | `logit.output.metrics.skipped{metric_kind=set}` | Under `multi_value: skip`, the default, no datapoint is written. |
//! | `histogram/delta`, under `multi_value: expand` | degraded | `logit.output.metrics.degraded{metric_kind=histogram}` | Under `multi_value: expand`, dotted `.count`, `.sum`, `.min`, and `.max` datapoints and a `.bucket_<bound>` datapoint per bucket holding that bucket's own count, its temporality dropped. A non-finite `.sum`, `.min`, or `.max` is left off, and the record also counts once as `logit.output.metrics.degraded{reason=non_finite_component}`. |
//! | `histogram/delta`, otherwise | skipped | `logit.output.metrics.skipped{metric_kind=histogram}` | Under `multi_value: skip`, the default, no datapoint is written. |
//! | `histogram/cumulative`, under `multi_value: expand` | degraded | `logit.output.metrics.degraded{metric_kind=histogram}` | Under `multi_value: expand`, the same dotted datapoints as a delta one's. |
//! | `histogram/cumulative`, otherwise | skipped | `logit.output.metrics.skipped{metric_kind=histogram}` | Under `multi_value: skip`, the default, no datapoint is written. |
//! | `histogram/nan_bound`, under `multi_value: expand` | degraded | `logit.output.metrics.degraded{metric_kind=histogram}` | Under `multi_value: expand`, each bucket is its own dotted datapoint, as for any other bound: `.bucket_NaN` holds that bucket's own count. |
//! | `histogram/nan_bound`, otherwise | skipped | `logit.output.metrics.skipped{metric_kind=histogram}` | Under `multi_value: skip`, the default, no datapoint is written. |
//! | `histogram/neg_inf_bound`, under `multi_value: expand` | degraded | `logit.output.metrics.degraded{metric_kind=histogram}` | Under `multi_value: expand`, each bucket is its own dotted datapoint, as for any other bound: `.bucket_-inf` holds that bucket's own count. |
//! | `histogram/neg_inf_bound`, otherwise | skipped | `logit.output.metrics.skipped{metric_kind=histogram}` | Under `multi_value: skip`, the default, no datapoint is written. |
//! | `exponential_histogram/delta`, under `multi_value: expand` | degraded | `logit.output.metrics.degraded{metric_kind=exponential_histogram}` | Under `multi_value: expand`, dotted `.count`, `.sum`, `.min`, `.max`, and `.zero_count` datapoints. Its buckets aren't written, since each scale would mint its own paths. |
//! | `exponential_histogram/delta`, otherwise | skipped | `logit.output.metrics.skipped{metric_kind=exponential_histogram}` | Under `multi_value: skip`, the default, no datapoint is written. |
//! | `exponential_histogram/cumulative`, under `multi_value: expand` | degraded | `logit.output.metrics.degraded{metric_kind=exponential_histogram}` | Under `multi_value: expand`, the same dotted datapoints as a delta one's, and no buckets. |
//! | `exponential_histogram/cumulative`, otherwise | skipped | `logit.output.metrics.skipped{metric_kind=exponential_histogram}` | Under `multi_value: skip`, the default, no datapoint is written. |
//! | `summary/delta`, under `multi_value: expand` | degraded | `logit.output.metrics.degraded{metric_kind=summary}` | Under `multi_value: expand`, dotted `.count` and `.sum` datapoints and a `.q<quantile>` datapoint per quantile, its temporality dropped. |
//! | `summary/delta`, otherwise | skipped | `logit.output.metrics.skipped{metric_kind=summary}` | Under `multi_value: skip`, the default, no datapoint is written. |
//! | `summary/cumulative`, under `multi_value: expand` | degraded | `logit.output.metrics.degraded{metric_kind=summary}` | Under `multi_value: expand`, dotted `.count` and `.sum` datapoints and a `.q<quantile>` datapoint per quantile. |
//! | `summary/cumulative`, otherwise | skipped | `logit.output.metrics.skipped{metric_kind=summary}` | Under `multi_value: skip`, the default, no datapoint is written. |
//! | `record/no_recorded_value` | skipped | `logit.output.metrics.skipped{reason=no_recorded_value}` | Carbon has no marker for a point with no recorded value, so it isn't written, with a throttled `no_recorded_value` warning. |
//! | `record/no_recorded_value_delta_sum` | skipped | `logit.output.metrics.skipped{reason=no_recorded_value}` | Carbon has no marker for a point with no recorded value, so it isn't written, with a throttled `no_recorded_value` warning. |
//! | `record/no_recorded_value_other_kind` | skipped | `logit.output.metrics.skipped{reason=no_recorded_value}` | Carbon has no marker for a point with no recorded value, so it isn't written, with a throttled `no_recorded_value` warning. |
//! | `record/service_check` | exact | -- | The status gauge; the check's carriers aren't tags. |
//! | `record/name_unsafe` | normalized | `path_sanitized`; `logit.output.metrics.normalized{reason=path_sanitized}` | Every whitespace, control, `;`, `/`, and `\` character in a path is written as `_`, so the record reads back under the sanitized path, counted once per record. |
//! | `record/sample_rate` | exact | -- | Under `multi_value: expand`, each value is weighted by its inverse rate in the sketch the `samples` row describes. |
//! | `record/superseded` | exact | -- | Each record is its own datapoint at its own second. A whisper store keeps one point per path and second, so there a later point in the same second replaces an earlier one. |
//! | `record/same_instant` | exact | -- | Each record is its own datapoint; a whisper store keeps the later of two points at one second. |
//! | `record/same_instant_equal` | exact | -- | Each record is its own datapoint; a whisper store keeps the later of two points at one second. |
//! | `record/same_millisecond` | exact | -- | Each record is its own datapoint, its timestamp floored to the second; a whisper store keeps the later of two points in one second. |
//! | `record/same_millisecond_equal` | exact | -- | Each record is its own datapoint, its timestamp floored to the second; a whisper store keeps the later of two points in one second. |
//! | `record/non_finite` | dropped | `logit.output.metrics.dropped{reason=unencodable_value}` | Carbon has no non-finite value, so the datapoint isn't written, with a throttled `unencodable_value` warning. |
//! | `log/dogstatsd_event` | skipped | `logit.output.events.skipped{reason=no_metrics}` | A log has no carbon form and isn't written. |
//! | `log/plain` | skipped | `logit.output.events.skipped{reason=no_metrics}` | A log has no carbon form and isn't written. |
//! | `span/alone` | skipped | `logit.output.events.skipped{reason=no_metrics}` | A span has no carbon form and isn't written. |
//! | `span/no_wire_form` | exact | -- | Nothing beyond `span/alone`'s outcome. |
//! | `event/attributes_only` | skipped | `logit.output.events.skipped{reason=no_metrics}` | An event with no metric, log, or span has no datapoint and isn't written. |
//! | `event/string_only` | skipped | `logit.output.events.skipped{reason=no_metrics}` | An InfluxDB point whose fields are all strings has no metric, so it has no datapoint and isn't written. |
//!
//! Per field of a record that reaches the wire, attribute shape, and payload beside metrics:
//!
//! | Row | Class | Reason or counter | What a consumer sees |
//! |---|---|---|---|
//! | `record/unit` | lost | structural | Carbon has no unit, so none is written. |
//! | `record/description` | lost | structural | Carbon has no description, so none is written. |
//! | `record/start_timestamp` | lost | structural | Carbon has no start time, so none is written. |
//! | `record/exemplars` | lost | structural | Carbon has no exemplars, so none are written. |
//! | `attributes/string_value`, under `tags: drop` | lost | `logit.output.tags.omitted{reason=dialect}` | Under `tags: drop`, no tag is written, counted once per attribute, so series that differ only in their tags write one path. |
//! | `attributes/string_value`, otherwise | normalized | `tag_sanitized`; `logit.output.metrics.normalized{reason=tag_sanitized}` | A `;name=value` tag. Each `;`, `!`, `^`, `=`, whitespace, and control character in the name, and each `;`, whitespace, and control character and a leading `~` in the value, is written as `_`, counted once per record. |
//! | `attributes/bool_value`, under `tags: drop` | lost | `logit.output.tags.omitted{reason=dialect}` | Under `tags: drop`, no tag is written, counted once per attribute. |
//! | `attributes/bool_value`, otherwise | normalized | `stringified`; `logit.output.metrics.normalized{reason=tag_sanitized}` | A tag of `true` or `false`, read back as the string. Its name is sanitized as the `string_value` row says, counted once per record as `tag_sanitized`. |
//! | `attributes/number_value`, under `tags: drop` | lost | `logit.output.tags.omitted{reason=dialect}` | Under `tags: drop`, no tag is written, counted once per attribute. |
//! | `attributes/number_value`, otherwise | normalized | `stringified`; `logit.output.metrics.normalized{reason=tag_sanitized}` | A tag of the number's text, an `f64` in its shortest form (`3` for `3.0`), read back as the string. Its name is sanitized as the `string_value` row says, counted once per record as `tag_sanitized`. |
//! | `attributes/array_value`, under `tags: drop` | lost | `logit.output.tags.omitted{reason=dialect}` | Under `tags: drop`, no tag is written, counted once per attribute. |
//! | `attributes/array_value`, otherwise | normalized | `multi_value`; `logit.output.tags.normalized{reason=multi_value}` | A tag holds one value, so only the last representable element is written, counted. |
//! | `attributes/map_value`, under `tags: drop` | lost | `logit.output.tags.omitted{reason=dialect}` | Under `tags: drop`, no tag is written, counted once per attribute. |
//! | `attributes/map_value`, otherwise | lost | `logit.output.tags.omitted{reason=unrepresentable}` | A map has no tag form and is left off, counted. |
//! | `attributes/bytes_value`, under `tags: drop` | lost | `logit.output.tags.omitted{reason=dialect}` | Under `tags: drop`, no tag is written, counted once per attribute. |
//! | `attributes/bytes_value`, otherwise | lost | `logit.output.tags.omitted{reason=unrepresentable}` | A bytes value has no tag form and is left off, counted. |
//! | `attributes/timestamp_value`, under `tags: drop` | lost | `logit.output.tags.omitted{reason=dialect}` | Under `tags: drop`, no tag is written, counted once per attribute. |
//! | `attributes/timestamp_value`, otherwise | lost | `logit.output.tags.omitted{reason=unrepresentable}` | A timestamp value has no tag form and is left off, counted. |
//! | `attributes/prometheus_target` | lost | structural | The scrape target is private and isn't a tag, so it's left off, under either `tags:` mode. |
//! | `attributes/foreign_carrier` | lost | structural | Another protocol's `_logit.` carrier is private and isn't a tag, so it's left off, under either `tags:` mode. |
//! | `attributes/resource_attribute`, under `tags: drop` | lost | `logit.output.tags.omitted{reason=dialect}` | Under `tags: drop`, no tag is written, counted once per attribute. |
//! | `attributes/resource_attribute`, otherwise | normalized | `resource_merged`; `logit.output.metrics.normalized{reason=tag_sanitized}` | A tag, read back as an event attribute; on a key the event shares, the event's value wins, uncounted. Its name and value are sanitized as the `string_value` row says, counted once per record as `tag_sanitized`. |
//! | `attributes/scope` | lost | structural | Carbon has no instrumentation scope, so none is written. |
//! | `attributes/datadog_carrier_log` | lost | structural | Another protocol's `_logit.` carrier is private and isn't a tag, so it's left off. |
//! | `attributes/no_host` | exact | -- | A record with no host leaves with none. |
//! | `log/beside_metrics` | lost | uncounted | A log on an event that carries metrics is left off while the metrics are written, uncounted. |
//! | `span/beside_metrics` | lost | uncounted | A span on an event that carries metrics is left off while the metrics are written, uncounted. |
//!
//! <!-- transit:end -->
//!
//! The rest of the encode mapping is hand-written: rules the spec has no row for, and details its
//! cells don't state.
//!
//! | Model | Wire | Counter |
//! |---|---|---|
//! | event with N metrics | N datapoints, in `MetricList` order | -- |
//! | `MetricRecord::name`, sanitized | the path, verbatim | -- |
//! | any kind flagged [`logit_core::MetricRecord::FLAG_NO_RECORDED_VALUE`], tested before the kind | skipped | `logit.output.metrics.skipped{reason="no_recorded_value"}` + diag `no_recorded_value` |
//! | `Samples`, `Distribution`, `Histogram`, `ExponentialHistogram`, `Summary`, `Set`, and `SetMembers` under [`MultiValue::Expand`] | the sub-path table in [`crate::multi_value`] | `logit.output.metrics.degraded{metric_kind=…}`, **once per record** however many sub-paths it produced |
//! | `event.timestamp.div_euclid(1_000_000_000) <= 0` | dropped | `{reason="unencodable_timestamp"}` + diag `unencodable_timestamp` |
//! | attributes under [`Tags::Carbon`] (the default) | `;name=value`, ascending **rendered** name | -- |
//! | a `Value::Null`, or an `Array` with no representable element | the tag is omitted | `logit.output.tags.omitted{reason="unrepresentable"}` |
//! | a forbidden byte in the path / a tag name / a tag value | `_` | `logit.output.metrics.normalized{reason="path_sanitized"\|"tag_sanitized"}`, once per record per reason |
//! | a tag whose name or value is empty after sanitizing | the tag is omitted | `logit.output.tags.omitted{reason="empty"}` |
//! | two tags colliding after rendering | the one whose **original** name sorts first is kept | `logit.output.tags.omitted{reason="collision"}` |
//! | a path empty after sanitizing | dropped | `{reason="empty_name"}` |
//! | a plaintext line longer than `max_packet_bytes` | dropped whole, never split | `{reason="oversize_line"}` + diag `oversize_line` |
//! | a single pickle datapoint larger than `max_frame_bytes` | dropped whole, never split | `{reason="oversize_datapoint"}` + diag `oversize_datapoint` |
//! | `Resource::schema_url` and every `dropped_attributes_count` | dropped | none; carbon has no field for either |
//! | an event with no metrics at all (a log, a span, or attributes alone) | skipped, and its log, span, or attributes are lost | `logit.output.events.skipped{reason="no_metrics"}`, once per event, no diagnostic |
//! | a non-finite component of a record under [`MultiValue::Expand`] | that sub-path is left off, and the rest are written | `logit.output.metrics.degraded{reason="non_finite_component"}`, once per record however many it left off, beside the record's `{metric_kind=…}` |
//!
//! Why the wire forces the generated rows:
//!
//! - A carbon datapoint is one number at one second: no bucket, quantile, sketch, or member-set
//!   form. `multi_value: skip` is the default because the alternative is a naming convention the
//!   far end doesn't know: `x.q0_99` is a series called `x.q0_99`, not a quantile of `x`.
//!   `expand` is opt-in and named, and loses mergeability: two relays' `.count` series can't
//!   recombine like their sketches. An `ExponentialHistogram`'s buckets aren't expanded, since
//!   materializing `base^i` bounds would mint unbounded paths from one record.
//! - A `Sum`'s bare value can't be misread, since carbon has no opinion on temporality or
//!   monotonicity, so dropping both is normalization 12 rather than a skip. `otlp_in ->
//!   graphite_out -> graphite_in` turns a cumulative counter into a gauge. The record counts
//!   `metrics.degraded` under `statsd_out`'s `metric_kind` tags, plus `monotonic_delta_sum`,
//!   which `statsd_out` writes exact. Unlike `statsd_out`, `multi_value: skip` still writes it.
//! - Carbon drops a `NaN` on receipt and whisper can't store an infinity, and substituting zero
//!   fabricates a reading, so a non-finite value is dropped.
//! - Carbon has no "no reading this interval" marker: Graphite's "no data" is an absent
//!   datapoint, and writing a flagged record's default payload would report a sample nobody sent.
//!
//! Every private `_logit.` attribute is **skipped uncounted**, as `influxdb_out` skips it: it is
//! a carrier, not a tag
//! ([ADR `private-attribute-namespace`](../../../../docs/adr/private-attribute-namespace.md)).
//! Resource attributes *do* become tags (as at `influxdb_out`/`statsd_out`); a `graphite_in`
//! resource is empty, so the pair stays a fixed point, and cross-protocol it is a
//! `docs/known-gaps/mappings.md` row.
//!
//! A tagged carbon 1.1.10 replaces a tag named `name` with the metric's path
//! (`TaggedSeries.parse_carbon`), uncounted. `graphite_out` writes the tag as any other, so to keep
//! its value, rename or remove the attribute at the source, or ahead of the sink with a `remove` or
//! `lua` component.
//!
//! ## Sanitization
//!
//! Substitute `_`, **never delete**: deletion would merge `a b` and `ab` into one series.
//! Remaining collisions are resolved on the **rendered** names, never on interner order (ADR
//! `prometheus-scrape-and-exposition`'s "Names and sanitization" section).
//!
//! | Field | Forbidden → `_` |
//! |---|---|
//! | path | whitespace ([`char::is_whitespace`]), [`char::is_control`], `;`, `/`, `\` |
//! | tag name | `;`, `!`, `^`, `=`, whitespace, control |
//! | tag value | `;`, whitespace, control; and a **leading** `~` only |
//!
//! `/` and `\` are whisper's directory separators: one in a path would create a nested directory,
//! not a series segment. There is **no path truncation**: carbon has no length bound, and whisper's
//! 255-byte filesystem component limit is a `docs/known-gaps/mappings.md` row. A leading `~` is
//! reserved by carbon's tag grammar; a `~` elsewhere in a tag value rides through.
//!
//! Whitespace is [`char::is_whitespace`] (Unicode): carbon splits a *decoded* `str` with Python's
//! `str.split()`, and [`decode`] mirrors that with [`str::split_whitespace`]. Sanitizing only ASCII
//! whitespace would let a U+00A0 in a path decode as a four-field line.
//!
//! ## `MultiValue::Expand` sub-paths
//!
//! [`crate::multi_value`]'s module doc is the canonical table of the dotted sub-paths each kind
//! expands into, with the rules behind it: every expansion adds at least one suffix, a sketch's
//! `.sum`, `.min`, and `.max` come from its tracked statistics rather than its bins, non-finite
//! values are skipped and reported to the encoder, and number tokens are injective. The encoder
//! appends each sub-path's suffix to the sanitized path, then the tag segment.
//!
//! ## `Protocol` and `Meta`
//!
//! [`GraphiteEncoder`] implements [`crate::FramedEncoder`], not [`crate::Encoder`], because both
//! carbon protocols need per-message boundaries, which `Encoder`'s one blob per batch lacks: a UDP
//! datagram packs whole lines up to `max_packet_bytes`, and a pickle stream writes one
//! length-prefixed frame at a time.
//!
//! `type Meta = usize` is each message's **datapoint count**, so a sink that fails a send
//! attributes the loss to the right number of metrics. Under [`Protocol::Plaintext`] every entry is
//! one line, meta `1`. Under [`Protocol::Pickle`] every entry is one **complete, length-prefixed
//! frame** (the encoder writes the 4-byte big-endian prefix, so the sink does one `write_all` per
//! entry), meta its datapoint count. A new frame opens when the next datapoint would push the
//! payload past `max_frame_bytes`.
//!
//! `Protocol`/`Tags`/`MultiValue` and both size caps are **encoder state**, set by the `with_*`
//! builders, since [`crate::FramedEncoder`] has one signature. The codec never sees a transport,
//! so pickle over UDP is rejected by a graph rule (`crates/logit-pipeline/src/graph.rs`).
//!
//! ## Permitted normalizations
//!
//! `graphite_in -> graphite_out` is a fixed point modulo exactly this list (the round-trip tests in
//! `crates/logit-proto/tests/graphite_fixed_point.rs` pin it). The ADR uses the same numbering:
//!
//! 1. Re-framing: lines are repacked into datagrams or stream writes, and datapoints into pickle
//!    frames of at most `max_frame_bytes`, so one input frame may leave as several and vice versa.
//! 2. An operator-chosen dialect change: plaintext ↔ pickle, in either direction.
//! 3. Datapoint reordering within a batch (everything downstream of a batch boundary is
//!    order-insensitive by design).
//! 4. Tag order is canonicalized to ascending rendered name, as carbon's `TaggedSeries.format`
//!    sorts.
//! 5. A repeated tag key collapses to its **last** occurrence at decode, counted, as carbon's
//!    `TaggedSeries.parse` `dict` does. Not `statsd_in`'s fold into a `Value::Array`: keeping
//!    arrays out of the pair keeps the encoder's array→last-element tag rule from firing in it.
//! 6. Timestamps floor to whole seconds on egress (`div_euclid`, so a negative instant floors
//!    downward rather than toward zero).
//! 7. A `-1` timestamp becomes receipt time on ingress, and leaves as that absolute second.
//! 8. Number formatting becomes the shortest round-trip `f64` rendering: `1.50`, `1.5e0` and
//!    `+1.5` all leave as `1.5`, and `3.0` leaves as `3`.
//! 9. Field separators collapse to a single space, `\r\n` to `\n`; a TCP write terminates every
//!    line including the last, a UDP datagram terminates none.
//! 10. Sanitizer substitutions, empty-tag drops and rendered-name collision drops (all counted).
//! 11. [`Tags::Drop`] drops the tag set entirely (counted).
//! 12. A `Sum`'s temporality and monotonicity are dropped; the value goes on the wire bare, counted
//!     `logit.output.metrics.degraded{metric_kind=…}` once per record. A **named normalization,
//!     not a skip**: unlike `prometheus_out`, which drops a delta `Sum` because exposition has a
//!     competing cumulative meaning, carbon's wire has no opinion on either, so the number is
//!     carried faithfully and only the model's extra facts are lost. `graphite_in` decodes only
//!     gauges, so the like pair never reaches it.
//! 13. A pickle sender's spelling (protocol version, opcode choice, memo use) leaves as
//!     `graphite_out`'s protocol-2 writer spelling. A protocol-0, protocol-1, or protocol-5 sender
//!     is re-spelled, and no decoded field changes.
//! 14. A tag whose name is under the private `_logit.` prefix is omitted at decode, counted.
//!
//! One more sits outside the numbering the ADR and plan share, because the listener adds it after
//! decode rather than the codec: under `graphite_in`'s `peer:` or `proxy_protocol:`,
//! `network.peer.*` or `client.*` is added to every event, an opt-in addition the sender never
//! sent, and one of ADR `lossless-transit`'s "Permitted normalizations".
//!
//! Everything else is an error or a counted drop, never a silent reinterpretation.

pub mod decode;
pub mod encode;
pub mod pickle;

pub use decode::GraphiteDecoder;
pub use encode::{EncodeStats, GraphiteEncoder};

/// Carbon's own plaintext listener port (`LINE_RECEIVER_PORT`), for both `graphite_in` and
/// `graphite_out`.
pub const DEFAULT_PLAINTEXT_PORT: u16 = 2003;

/// Carbon's own pickle listener port (`PICKLE_RECEIVER_PORT`). TCP only -- the length-prefixed
/// framing has no meaning in a datagram, and a graph rule rejects the combination.
pub const DEFAULT_PICKLE_PORT: u16 = 2004;

/// The longest single UDP datagram `graphite_out` packs plaintext lines into: a 1500-byte Ethernet
/// MTU minus the IPv4 and UDP headers and headroom, as `statsd_out` uses. Bounds a whole datagram
/// (several `\n`-joined lines); a line that alone exceeds it is dropped whole, since half a line is
/// a corrupt series.
pub const DEFAULT_MAX_PACKET_BYTES: usize = 1432;

/// The longest plaintext line `graphite_in` assembles before draining to the next `\n`. (Twisted's
/// `LineReceiver` defaults to 16384, which carbon keeps.) 8 KiB is past any real tagged path and
/// keeps one misbehaving connection from growing an unbounded read buffer.
pub const DEFAULT_MAX_LINE_BYTES: usize = 8192;

/// Twisted's `Int32StringReceiver.MAX_LENGTH`, which carbon's pickle receiver inherits: a frame
/// declaring more is refused and the connection dropped. Both the decode and the pack bound, so a
/// relay never writes a frame the far end would refuse.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 1 << 20;

/// The deepest nesting the restricted pickle reader follows, one open `MARK` per level. A real
/// carbon payload reaches 2 (the list, then a datapoint tuple).
pub const MAX_PICKLE_DEPTH: usize = 16;

/// The most values the restricted pickle reader holds at once, bounding the stack, each of the
/// two arenas, and the memo independently. A real batch's datapoint is about 20 bytes (a
/// five-byte `BININT` second, a nine-byte `BINFLOAT`, its path or a memo reference to it) and
/// adds four values to the tuple arena, so a [`DEFAULT_MAX_FRAME_BYTES`] frame stays near 200k;
/// a frame of one-byte opcodes or tiny integer datapoints can reach the cap. The cap keeps a
/// corrupt frame from making the reader's own `Vec`s the denial of service.
pub const MAX_PICKLE_ITEMS: usize = 500_000;

/// Which carbon wire protocol a decoder reads or an encoder writes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    /// `path[;k=v...] value timestamp\n`, TCP or UDP.
    #[default]
    Plaintext,
    /// A 4-byte big-endian length prefix then a pickled `[(path, (timestamp, value)), ...]`. TCP
    /// only.
    Pickle,
}

/// Whether `graphite_out` renders attributes as carbon tags at all.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Tags {
    /// Render `;name=value` per attribute, ascending rendered name -- carbon 1.1+'s tag syntax.
    #[default]
    Carbon,
    /// Emit no tag segment, for a pre-1.1 Graphite, whose whisper backend would take the `;` into
    /// a directory name. Each dropped tag is counted `logit.output.tags.omitted{reason="dialect"}`.
    Drop,
}

pub use crate::MultiValue;
