---
created: 2026-09-10
updated: 2026-09-12
---

# statsd/DogStatsD egress: dialect, transport, packing, and the v1 metric-kind deferral

## Status
Accepted

## Context

`statsd_in` (`crates/logit-inputs/src/statsd.rs`) has ingested statsd/DogStatsD metrics since
v0.1, but `logit` has had no way to emit them: there was no `statsd_out` `ComponentKind`. That's
the one asymmetry in the `_in`/`_out` naming convention every other protocol pair satisfies
(`docs/design/pipeline-graph.md`), and it blocks the obvious relay topology — a `logit` edge node
receiving statsd, aggregating, and forwarding to a central statsd/DogStatsD agent (a Datadog
agent, an Etsy-statsd instance, another `logit`).

Four questions had to be settled before implementation: which statsd dialect to emit, which
transport(s) to support, how a merged `DdSketch` (no longer holding the original samples) should
become wire lines, and whether `GaugeDelta` — statsd's own native relative-gauge syntax — should
be encoded natively or treated like every other sink treats it.

## Decision

### Dialect: configurable, default DogStatsD

`format: dogstatsd | statsd`, defaulting to **DogStatsD**. DogStatsD's tag extension
(`|#k:v,k:v`) is what carries an event's attributes across the wire at all; a plain-statsd
receiver that doesn't expect the segment can reject the whole line, so `format: statsd` omits it
entirely — not an empty `|#`, which is equally liable to be rejected — and counts every dropped
attribute (`logit.output.tags.dropped{reason="dialect"}`) rather than silently discarding data
with nothing to show for it.

### Transport: both UDP and TCP

`transport: udp | tcp`, defaulting to **UDP** — classic statsd and every DogStatsD client is UDP,
and it needs no ordering guarantee against the receiver's startup, the same reasoning
`docs/adr/syslog-output.md` gives for its own default. TCP is what makes `Fault` classification
meaningful for this sink, same as `syslog_out`: a connect failure is unambiguously `Fault::Clean`.
`send_udp`/`send_tcp` are near-verbatim ports of `syslog_out`'s, including its two TCP
correctness properties (cancellation-safe via `stream.take()`, and never resending once a byte has
left the host).

### Packing: UDP packs several lines into one datagram — the opposite of `syslog_out`

`max_packet_bytes` (default **1432**) bounds one **datagram**, into which `statsd_out` newline-
joins as many lines as fit before starting the next one. This is a deliberate divergence from
`syslog_out`, which refuses to pack, precisely because packing there would depend on the receiver
splitting on a delimiter its own injection-safety section exists to avoid relying on. The
reasoning inverts here: splitting on `\n` **is** the statsd grammar — every buffered statsd client
packs a send this way, and `StatsdDecoder::decode_into` splits an incoming datagram on `\n`
directly (`crates/logit-inputs/src/statsd.rs`) — and this sink's own sanitizers (below) make an
embedded `\n` unrepresentable in a name, key, or value, so a packed datagram cannot forge an extra
metric the way a packed syslog datagram could forge an extra log line. A line that would overflow
the cap starts a new datagram; a single line longer than the cap is **dropped whole**, never
truncated (`syslog_out` truncates) — a truncated statsd line decodes as a different metric or a
parse error, never a shorter version of the same one.

**1432, not 512 or 8192.** 512 is Etsy statsd's conservative internet-safe figure; 8192 is
DataDog's loopback/UDS figure (and `syslog_out`'s own `DEFAULT_MAX_MESSAGE_BYTES`, since Alloy's
receiver is local to the demo stack). 1432 is both Etsy statsd's own commodity-Ethernet-LAN
recommendation and DataDog's documented DogStatsD client default: 1500 MTU minus IPv4/UDP headers
minus headroom for VXLAN/IPsec encapsulation — exactly where a 1472-byte datagram would silently
fragment or `EMSGSIZE`. It doesn't assume the destination is loopback the way 8192 does.

TCP terminates **every** line with `\n`, including the last (no per-batch EOF on a stream, so
omitting it would glue the last line of one batch onto the next batch's first). No octet-counting
on TCP, unlike `syslog_out`: statsd has no such framing convention, and no receiver auto-detects
one the way Alloy's `go-syslog` does.

### Metric-kind coverage: `Counter`/`Gauge`/`GaugeDelta` only in v1

Only `Counter` (`|c`) and `Gauge`/`GaugeDelta` (`|g`) are encoded. `Distribution`, `Set`,
`Histogram`, and `Summary` are dropped with a clear "not implemented yet" diagnostic
(`unsupported_metric_kind`) — the same shape `influxdb_out`'s `render_fields` already uses for
`Set`.

**This means a `statsd_in -> aggregate -> statsd_out` relay drops every timer metric today.**
`ms`/`h`/`d` on the wire all decode to `MetricKind::Distribution`
(`logit_inputs::statsd::build_event`), so the single most common statsd workload — timers — makes
it through the input and the aggregator, then dies at this sink, loudly counted but dropped.
Deferred rather than guessed at: the aggregator's `DdSketch` no longer holds the original samples
it merged, so "how does a merged sketch become one or more statsd lines" is a real design
question — emit one line per fixed quantile (losing the ability to compute an arbitrary
downstream percentile)? Synthesize N samples at the sketch's own quantile boundaries (fabricating
a population that never existed)? — and deserves its own ADR once there's a concrete consumer to
design against, not a default picked in passing here.

### Relative gauges: opt-in native encoding, off by default

statsd is the one protocol that natively expresses a *relative* gauge adjustment (`+n|g`/`-n|g`)
— exactly `MetricKind::GaugeDelta`'s wire origin (`docs/adr/relative-gauge-adjustments.md`). By
default a `GaugeDelta` reaching this sink is dropped with the identical message `influxdb_out`
uses (`gauge_delta_unresolved`): it means the pipeline is missing an `aggregate` component, not
that the metric is malformed — and keeping that one invariant true across every sink by default
matters more than the convenience of a lossless relay. `relative_gauges: true` opts into encoding
it natively instead: `statsd_out` is the only sink that *can* round-trip a delta losslessly, since
every other sink's wire format has no equivalent concept at all, so it's a deliberate, named
exception rather than a silent default.

A positive delta needs an explicit `+`: plain float formatting of `5.0` yields `"5"`, which the
decoder reads back as an *absolute* `Gauge`, not a delta — silently corrupting the exact
round-trip this feature exists to preserve.

### Negative absolute gauges: the two-line `0|g` / `-n|g` idiom

The statsd/DogStatsD grammar has no wire syntax for setting a gauge to a negative absolute value
at all — `logit_inputs::statsd::build_event`'s `"g"` arm reads *any* leading `-` as a relative
delta, unconditionally. A naive `Gauge(-5.0)` would therefore render as `name:-5|g` and decode
back as `GaugeDelta(-5.0)`: a silent semantic corruption of a value type this sink otherwise
preserves exactly. `statsd_out` instead emits the idiom both Etsy statsd and DogStatsD document
for exactly this case: `name:0|g` immediately followed by `name:-5|g`. The two lines are pushed as
**one indivisible `MessageBuf` entry** (joined by an embedded `\n`) so the UDP packer can never
split them across two datagrams — a lost first datagram would otherwise apply `-5` to whatever
stale value the gauge already held at the receiver, rather than to the `0` this sink meant to
reset it to first. This is the only place an entry contains a newline; every sanitizer this sink
applies exists precisely to guarantee nothing else ever does.

`Gauge(-0.0)` is the one negative-signed value that is *not* sent as a pair: it is numerically
zero, and `0` is directly representable as an absolute gauge, so its sign is normalized away and it
renders as the plain `name:0|g`. Without that normalization `f64`'s `Display` would write
`name:-0|g`, which the decoder's leading-`-` dispatch reads as a delta — the exact corruption the
pair exists to prevent, for the one value the pair would be pointless for.

### Sanitization

A metric name has every one of `: | @ # , \n \r \0`, ASCII control characters, and whitespace
replaced with `_` (substitution, not deletion, so distinct names stay distinct — following
`syslog_out`'s `sanitize_5424_field` precedent). Each character is forbidden because of a specific
way `StatsdDecoder::parse_line` would otherwise misparse the result: `:` splits name from values,
`|` splits segments, `@`/`#` open the sample-rate/tag segments, `,` separates tags, `\n` separates
lines; whitespace is stripped because the decoder trims every line before parsing it.

Tag *keys* forbid the same set, **and additionally forbid `:`** for a different reason than the
name does: `parse_line` splits a tag on its *first* colon only, so a `:` inside a key would
silently reparse as a shorter key with the remainder folded into the value. Tag *values* forbid
the same set **except `:`, which is deliberately allowed**: since only the first colon is
significant, `env:a:b` round-trips as key `env`, value `a:b` — an asymmetry between key and value
sanitization that is easy to get backwards, so it carries its own dedicated test.

A `Value::Bool(true)` attribute renders as a bare tag (`#urgent`, no `:value`) — exactly what the
decoder produces for a valueless tag — rather than `key:true`, which would round-trip as
`Value::Str("true")`, silently changing the value's type.

### `duplicate_safe()` is `false`

Stronger than `syslog_out`'s reasoning: a redelivered `hits:5|c` doesn't just duplicate a record
the way a repeated log line would — it **increments the destination counter a second time**,
silently corrupting the value with no trace at the receiver. This yields the conservative
`AtMostOnce` default posture, under which `Fault::Clean` still retries, covering the common
receiver-restart outage with zero duplicate risk.

### No `logit_proto::Encoder`

Same reasoning as `syslog_out`: that trait returns one opaque `Bytes` per batch with no framing
metadata, which can't express the per-line/per-datagram boundaries this sink genuinely needs.
`StatsdEncoder::encode_into` is a bespoke, pure, directly-unit-testable method instead.

### No sample rate, no timestamp, no unit

Never `@<rate>`: `statsd_in` already extrapolated at decode time (a `Counter`'s value already has
the sample rate divided out; a `Distribution`'s samples are already replicated to the extrapolated
weight), so emitting `@1` would be a no-op at best and anything else would double-extrapolate
downstream. Never `|T<ts>`: the classic grammar has no timestamp segment at all, and
`logit_inputs::statsd::parse_line` would silently ignore one if emitted, so it wouldn't even
round-trip through this repo's own input — a receiver stamps with its own receipt time instead.
`MetricRecord::unit` has no statsd wire representation either and is dropped the same way. All
three are recorded in `docs/known-gaps.md`.

## Alternatives considered

- **One UDP datagram per line, mirroring `syslog_out`.** Rejected: it throws away the packing
  every real statsd client already does, for a safety property (no delimiter-splitting
  assumption) that doesn't apply here — this sink's sanitizers already make an embedded `\n`
  unrepresentable, so packing costs nothing that unpacking would have bought.
- **Octet-counted TCP framing, mirroring `syslog_out`.** Rejected: no statsd receiver auto-detects
  it, and there's no equivalent injection-safety property it would be defense-in-depth for —
  plain newline framing is what every statsd/DogStatsD TCP client and server already speaks.
- **A shared `DatagramTransport` enum with `syslog_out`.** Rejected even though the two variants
  are identical: `schemars` publishes a type's own name into the schema's `$defs`, so sharing one
  would make `statsd_out` document its transport by pointing at a syslog-named type. Precedent for
  coexisting near-duplicate config enums: `Compression` vs. `OtlpCompression`.
- **A `prefix`/`namespace` config field.** Rejected for v1: there is genuinely no way to rename or
  namespace a metric on egress anywhere in the pipeline today (`docs/design/lua-api.md` notes a
  metric's value/fields are unexposed to Lua), which is a real gap — but a sink-side prefix would
  pre-empt the general metric-rename transform that gap actually calls for, and coexisting with
  one awkwardly once it exists. Schema-additive later at zero compatibility cost.
- **`Distribution` support in v1, guessing at a `DdSketch` -> lines mapping.** Rejected: see the
  Decision section above — the design question deserves its own ADR against a concrete consumer,
  not a guess made in passing here.
- **The one-line multi-value idiom `name:0:-5|g` for negative gauges.** Atomic for free (statsd's
  own `:`-separated multi-value grammar decodes it to exactly the same `Gauge(0.0)` +
  `GaugeDelta(-5.0)` pair), but it's a DogStatsD-only extension that Etsy statsd rejects — it
  couldn't be used under `format: statsd` without a second code path, where the two-line idiom
  works identically under both dialects.
- **`|T<ts>` timestamps on egress.** Rejected: the classic grammar has none, and `statsd_in` would
  silently ignore one, so it wouldn't even round-trip through this repo's own input.

## Consequences

- `crates/logit-outputs/src/msgbuf.rs` (new): `MessageBuf` lifted out of `syslog.rs` (now needed
  by both sinks); `syslog.rs` re-exports it under its original path for source compatibility.
- `crates/logit-outputs/src/attrs.rs` (new): the resource⊕event attribute merge-join lifted out of
  `influxdb_out::render_tag_suffix` (also now needed by both sinks); `influxdb.rs` switches to it.
- `crates/logit-outputs/src/statsd.rs` (new): `StatsdEncoder` + `StatsdOutput`, per this ADR.
- `crates/logit-config/src/lib.rs`: new `ComponentKind::StatsdOut`, `StatsdTransport`,
  `StatsdFormat`; `schema/logit.schema.json` regenerated.
- `crates/logit-pipeline/src/graph.rs`: `role`/`kind_name`/`is_implemented` all gain the variant;
  a new rule rejects `max_packet_bytes: 0` (an impossible bound, not a small one — the same shape
  as the existing `buffer.max_batches`/`max_bytes: 0` rule).
- `crates/logit-cli/src/pipeline.rs`: `build_spec`'s `StatsdOut` arm, and the sole place a
  `logit_config::StatsdFormat`/`StatsdTransport` value crosses into `logit_outputs::statsd`'s own
  mirror types.
- `examples/statsd-relay.yaml` (new): a runnable relay config exercising the sink.
- `docs/known-gaps.md`: new entries for the v1 metric-kind deferral (and its timer-drop
  consequence for the `statsd_in -> aggregate -> statsd_out` path), no egress timestamp, no
  `unit`, no metric prefix/rename anywhere in the pipeline, and no TLS/DTLS.

## Amendment: raw timers and sets relay; sample rate and timestamps are carried

[ADR `lossless-transit`](lossless-transit.md) commits `logit` to a lossless `statsd_in ->
statsd_out` relay; [ADR `metrics-model-v2`](metrics-model-v2.md) is what gives the model the
raw-vs-summarized pair (`Samples`/`Distribution`, `SetMembers`/`Set`) this ADR's original deferral
had nowhere to land against. [`docs/plans/lossless-transit.md`](../plans/lossless-transit.md)'s W3
is the workstream landing the statsd side of both, once W1 (the model) and W2 (`aggregate`'s absorb
rules) were in place.

This ADR's "Metric-kind coverage" decision said: "Only `Counter` (`|c`) and `Gauge`/`GaugeDelta`
(`|g`) are encoded. `Distribution`, `Set`, `Histogram`, and `Summary` are dropped with a clear 'not
implemented yet' diagnostic (`unsupported_metric_kind`)" — and named the blocker directly: "the
aggregator's `DdSketch` no longer holds the original samples it merged, so 'how does a merged
sketch become one or more statsd lines' is a real design question... deserves its own ADR once
there's a concrete consumer to design against." Its "No sample rate, no timestamp, no unit"
decision said: "Never `@<rate>`: `statsd_in` already extrapolated at decode time... Never
`|T<ts>`: the classic grammar has no timestamp segment at all, and
`logit_inputs::statsd::parse_line` would silently ignore one if emitted, so it wouldn't even
round-trip through this repo's own input."

**Both premises are gone: `statsd_in` no longer sketches or extrapolates `ms`/`h`/`d`/`s` at decode
time at all, and this sink now encodes the raw `Samples`/`SetMembers` shapes those lines decode to
back onto the wire, carrying their real sample rate and, on `|T`-marked lines, their real
timestamp.**

### `Samples`/`SetMembers` wire forms, per dialect

`MetricKind::Samples` (`crates/logit-outputs/src/statsd.rs`'s `render_samples`) renders under
`format: dogstatsd` as one multi-value line, `name:v1:v2:...|<type>[|@rate]` — DogStatsD's own
multi-value extension, the same one `logit_inputs::statsd::parse_line` parses on the way in.
`<type>` is read off the record's `statsd.type` attribute when it names `ms`/`h`/`d`
(`statsd_wire_type`), defaulting to `ms` when the attribute is absent or names anything else
(`an_unrecognized_statsd_type_attribute_falls_back_to_ms`); `@rate` is omitted whenever
`sample_rate == 1.0`. Under `format: statsd`, which has no multi-value grammar, each value becomes
its own `name:v|ms[|@rate]` line, and `h`/`d` both collapse to `ms` (counted once per record via
`EncodeStats::type_normalized_dialect`, not once per split line) — both are the "splitting a
multi-value statsd line" and "sink-configured dialect change" normalizations
[ADR `lossless-transit`](lossless-transit.md) already permits by name.

`MetricKind::SetMembers` (`render_set_members`) renders one `name:<member>|s` line per member, in
**both** dialects — the classic grammar has no multi-value extension for sets the way DogStatsD's
timers get, so there is no dialect-conditional form here at all. Each member is rendered through
a member-specific rule (lossy UTF-8 first, since a member is arbitrary bytes off the wire): `:`,
`|`, and control bytes are substituted; everything else, including `@`, `#`, `,` and spaces, is
preserved. A member that comes out different from its raw bytes is counted
(`EncodeStats::members_sanitized`).

### `|c:`/`|T` carriage: dogstatsd only, dropped and counted under `format: statsd`

`statsd.container_id` renders as `|c:<id>` and the per-line `statsd.timestamp ==
Value::U64(secs)` carrier renders as `|T<secs>`, using that carrier's own value verbatim — both
only under `Format::DogStatsd` (`append_dialect_extras`), appended after the tag segment to
**every** physical line an event produces, so a negative-absolute-gauge's two-line pair or a
multi-line `Samples`/`SetMembers` record carries them on each line. `|T` renders **only** when the
carrier is present and holds a `Value::U64` (a wire `|T<secs>` segment always produces exactly
that), never derived from `Event::timestamp` — a receipt-time stamp is not a wire-supplied
timestamp, and a stage that rebuilds `Event::timestamp` after decode (`aggregate`'s flush,
notably) can't fabricate or collapse a `|T` this way, since the carrier rides on the series key
like any other attribute. A `statsd.timestamp` present but not a `Value::U64` (never produced by
`statsd_in` itself, but reachable from a cross-protocol relay or a Lua-authored attribute) is
simply not emitted (`a_non_u64_statsd_timestamp_value_is_not_emitted`). Under `format: statsd`,
neither field has anywhere to go — both are dropped and counted once per field per emitted
physical line (`EncodeStats::dropped_dialect_fields`,
`container_id_and_timestamp_are_dropped_and_counted_under_plain_statsd`).

### `statsd.*` is never emitted as a tag, and is read off the merged resource⊕event view

`statsd.type`/`statsd.container_id`/`statsd.timestamp` are protocol-namespaced carriers this
decoder stamped from a line's own wire-type/`|c:`/`|T` segments, not ordinary attributes —
`build_tag_suffix` filters every key starting with `statsd.` out of the generic `|#k:v,...`
segment before it's ever considered as a tag, uncounted (a carrier being read for its real
purpose, not data being dropped), the same way `syslog_out` never re-emits its own `syslog.*`
attributes as a generic SD-ELEMENT field. That same merged walk (resource attributes first, event
attributes overriding on collision, `crate::attrs::merged`) is also where all three carriers are
captured, into `EncodeCtx`'s own fields, rather than a second, separate read of `event.attributes`
afterward — `append_dialect_extras`/`statsd_wire_type` read `EncodeCtx`, never `event.attributes`,
directly. This means a carrier set only on the **resource** (a `set` transform's `resource:`
block, say) is honored exactly like one an event carries directly
(`a_container_id_on_the_resource_is_emitted_as_pipe_c_under_dogstatsd`) — filtering a key out of
the tag segment and reading it for its dedicated segment are now symmetric, where before only the
event's own attributes were ever read back.

### What's still deferred, and why that's now the "opt-in summarization" carve-out

`Distribution`, `Set`, `Histogram`, `ExponentialHistogram`, `Summary`, and a cumulative or
non-monotonic `Sum` are still dropped and counted (`EncodeStats::dropped_unsupported_kind`,
`unsupported_metric_kind`) — every kind that only exists *after* some stage has already
summarized, none of which has a lossless statsd rendering. The difference from the original v1
deferral is *when* that arm is ever reached: `aggregate`'s defaults (`distributions: sketch`,
`sets: estimate`) still summarize a raw series the moment it's absorbed, so a `statsd_in ->
aggregate -> statsd_out` relay using those defaults still drops every timer/set metric exactly as
it did before this amendment — but that is now an operator's explicit choice, not the only path
available. Configuring that `aggregate` with `distributions: samples`/`sets: members`
(`docs/adr/aggregation-window-semantics.md`'s amendment) keeps the raw shapes flowing through
instead, and a relay with no `aggregate` at all already only ever saw the raw shapes. **This stays
true only while every sample landing in a window shares one sample rate and the window stays under
`max_samples_per_series`/`max_set_members_per_series`**; once either limit is crossed, `aggregate`
falls back to a sketch/estimate for that window regardless of the `samples`/`members` config, and
this sink has no lossless rendering for that fallback either — it drops and counts it exactly like
the default-summarized case (`docs/known-gaps.md`). This is [ADR `lossless-transit`](lossless-transit.md)'s
"summarization is opt-in and named" rule made concrete on the egress side: the sink itself never
guessed at a sketch-to-lines mapping (still deserving its own design, per the original Decision
section above, should a concrete consumer ever need one), and the kinds it can't encode are now
exactly the kinds *only* an explicit `aggregate` choice, or a config limit, can produce.

**A repeated tag key is a separate, model-level gap, not a `statsd_out` one.** `#team:a,team:b` is
legal DogStatsD (a repeated tag key), but `AttrMap` is a map, not a multiset, so the second
`team:b` silently overwrites the first inside `statsd_in` itself, before this sink ever sees the
event — `x:1|c|#team:a,team:b` relays as `x:1|c|#team:b`. Tracked as debt against
[ADR `lossless-transit`](lossless-transit.md) in `docs/known-gaps.md`, not something this
amendment's carrier/sanitizer fixes touch.

### Permitted normalizations, restated for the raw shapes

Every normalization already permitted for `Counter`/`Gauge` applies identically to `Samples`/
`SetMembers`: tag reordering (`AttrMap` order) and number formatting. New to this amendment,
following directly from the wire forms above: splitting a multi-value statsd line into several
single-value lines (`SetMembers`, always; `Samples`, only under `format: statsd`) or the reverse,
and a timer's wire-type letter collapsing under a sink-configured dialect change (`h`/`d` → `ms`).
None of these change a value, a tag, a sample rate, a container id, or a timestamp — only how many
physical lines carry them and which literal type letter appears on the wire.

### Closing test enumeration

`crates/logit-outputs/src/statsd.rs` gained real-decoder relay coverage for both new kinds and
both new segments: `a_single_value_samples_metric_encodes_as_name_colon_value_pipe_ms_by_default`,
`a_multi_value_samples_metric_encodes_as_one_multi_value_line_under_dogstatsd`,
`statsd_type_attribute_selects_the_wire_type_letter`/
`an_unrecognized_statsd_type_attribute_falls_back_to_ms`,
`a_sample_rate_other_than_one_is_written_as_at_rate`/`a_sample_rate_of_one_omits_at_rate`,
`statsd_dialect_splits_multi_value_samples_and_normalizes_h_and_d_to_ms`/
`statsd_dialect_does_not_count_normalization_when_the_wire_type_was_already_ms`,
`a_non_finite_sample_value_is_dropped_and_counted_per_value_others_still_encode`,
`an_out_of_range_sample_rate_omits_at_rate_and_is_counted`, and
`an_empty_samples_list_emits_nothing_and_is_counted` for `Samples`;
`a_single_member_set_members_metric_encodes_as_name_colon_member_pipe_s`,
`a_multi_member_set_members_metric_encodes_as_one_line_per_member_in_both_formats`,
`a_non_utf8_set_member_is_lossily_sanitized_and_counted`,
`members_with_at_hash_comma_or_a_space_round_trip_byte_for_byte`,
`a_member_split_by_a_real_colon_decodes_as_two_members_each_re_encoding_untouched`, and
`an_empty_set_members_list_emits_nothing_and_is_counted` for `SetMembers`;
`a_container_id_attribute_appends_pipe_c_under_dogstatsd`,
`a_container_id_on_the_resource_is_emitted_as_pipe_c_under_dogstatsd`,
`a_timestamp_marker_appends_pipe_t_seconds_under_dogstatsd`,
`container_id_and_timestamp_come_after_the_tag_segment`,
`a_non_u64_statsd_timestamp_value_is_not_emitted`, and
`container_id_and_timestamp_are_dropped_and_counted_under_plain_statsd` for `|c:`/`|T`; plus
`a_container_id_and_timestamp_line_round_trips_through_the_real_statsd_decoder` and a `proptest`
fixed point (`mod fixed_point::decode_encode_decode_is_a_fixed_point`, 200 generated cases over
`c`/`g`/`ms`/`h`/`d`/`s` lines with an optional rate/tags/container id/timestamp) pinning
`decode(encode(decode(line))) == decode(line)` and `encode(decode(line))` as a fixed point of
`encode . decode`. `crates/logit-inputs/src/statsd.rs` gained the matching decode-side coverage:
`timer_becomes_a_single_sample_distribution`,
`sampled_distribution_at_half_rate_preserves_the_rate_without_extrapolating`/
`sampled_distribution_at_tenth_rate_preserves_the_rate_without_extrapolating`,
`unsampled_distribution_still_inserts_exactly_one_sample`,
`multi_value_timer_produces_one_event_with_all_values`,
`statsd_type_is_stamped_for_each_timer_type`, `set_type_becomes_set_members`,
`multi_value_set_produces_one_event_with_all_members`,
`container_id_segment_becomes_an_attribute`/`container_id_segment_applies_to_every_metric_type`,
`timestamp_segment_sets_the_event_timestamp_and_marker`/
`malformed_timestamp_segment_rejects_only_that_line`, and
`container_id_timestamp_rate_and_tags_combine_in_any_order`.
`crates/logit-cli/tests/statsd_round_trip.rs` (mirroring `syslog_round_trip.rs`'s real-UDP-socket
harness, landing alongside this amendment per `docs/plans/lossless-transit.md`'s W3 phase B)
extends the same coverage end to end through real sockets.
