---
created: 2026-10-07
updated: 2026-10-07
---

# `statsd_out` multi-value expansion: summarized kinds leave as dotted counter and gauge lines by default

## Status
Accepted

## Context
`statsd_out` has no rendering for a metric kind that exists only after a stage summarized:
`Distribution`, `Set`, `Histogram`, `ExponentialHistogram`, `Summary`, and a cumulative or
non-monotonic `Sum`. `render_metric` drops each one, counted
`logit.output.messages.dropped{reason="unsupported_kind"}` with a throttled
`unsupported_metric_kind` warning (`docs/known-gaps/statsd.md`, first entry).

That drop lands on the most common statsd workload. `aggregate`'s defaults
(`distributions: sketch`, `sets: estimate`) turn every timer into a `Distribution` and every set
into a `Set`, so a `statsd_in -> aggregate -> statsd_out` pipeline with a default `aggregate`
forwards counters and gauges and loses every timer and set, counted but gone. Raw retention
(`distributions: samples`, `sets: members`) avoids the drop only while a window stays under
`max_samples_per_series`/`max_set_members_per_series` and every sample shares one rate. Past
either limit, `aggregate` falls back to a sketch or estimate for that window, and the sink drops it.

[ADR `statsd-output`](statsd-output.md) deferred the mapping. Its "Metric-kind coverage" section
names the candidates (one line per fixed quantile, or samples synthesized at the sketch's
quantile boundaries) and asks for an ADR "once there's a concrete consumer to design against".
Its amendment's "What's still deferred" section restates that the kinds left undrawn are the ones
only an explicit `aggregate` choice or a config limit can produce.

### The consumers

No statsd server or agent accepts a sketch on the wire, and none emits statsd lines from one. Each
flushes a timer as a set of separate scalar metrics, counts as counters or rates and everything
else as gauges:

- **Etsy statsd** flushes a timer as `stats.timers.<name>.{count,sum,mean,upper,lower,median,upper_90,...}`
  and a set as `stats.sets.<name>.count`. Its `repeater` backend forwards raw packets only, so a
  statsd tier that relays after flushing doesn't exist in the reference implementation.
- **The Datadog Agent's DogStatsD** server flushes a histogram or timer as `.count` (a rate) and
  `.avg`, `.median`, `.max`, and `.95percentile` (gauges), and a set as one gauge of distinct
  values per flush.
- **Vector's statsd sink** drops an aggregated histogram or summary, counted. That's what
  `statsd_out` does today.

A downstream statsd server therefore reads dotted scalar names (`x.count`, `x.upper`,
`x.95percentile`) as the ordinary output of a statsd tier.

### The in-tree precedent
`graphite_out` and `splunk_hec_out` already face the same problem: one number per point on the
wire, several numbers per record in the model. Both answer it with a `multi_value: skip | expand`
switch on the shared `logit_proto::MultiValue`, `skip` by default:

- [ADR `graphite-carbon-relay`](graphite-carbon-relay.md)'s "`multi_value: expand` sub-paths"
  section defines a dotted suffix table (`.count`, `.sum`, `.q0_5` through `.q0_99`,
  `.bucket_<b>`, `.zero_count`, `.min`, `.max`) and an injective number-token form. The table's
  canonical copy is the "`MultiValue::Expand` sub-paths" section of
  `crates/logit-proto/src/graphite/mod.rs`'s module doc, and the expansion code is private to
  `graphite::encode`.
- [ADR `splunk-hec-relay`](splunk-hec-relay.md)'s decision 4 uses the OTel Collector `splunk_hec`
  exporter's shape instead (`_sum`, `_count`, cumulative `_bucket` with `le`, `<n>_<q>` with
  `qt`), because Splunk dashboards built for that exporter read those names.

## Decision
`statsd_out` gains `multi_value: skip | expand` on the shared `logit_proto::MultiValue`, and
defaults to `expand`. Under `expand`, each summarized kind leaves as one statsd line per component,
named with graphite's dotted suffix table and typed `|c` or `|g` by whether the component sums
correctly at a statsd server.

### The switch and its default
- **`expand`** (the `statsd_out` default) writes the lines below and counts the record as degraded.
- **`skip`** drops the record, counted as skipped, which is today's behavior under a new counter.

The default differs from `graphite_out` and `splunk_hec_out`, which keep `skip`. The config field
defaults to `expand`, and so does a `StatsdEncoder` constructed directly, so a test or a caller
outside the config path gets the same behavior an operator does. `logit_proto::MultiValue`'s own
`#[default]` stays `Skip`.

No graph rule constrains the field. Both values are valid on every transport and both dialects.

### One shared suffix table
The dotted suffixes are graphite's, unchanged:

| Kind | Components |
|---|---|
| `Distribution` | `.count`, `.sum`, `.q0_5`, `.q0_75`, `.q0_9`, `.q0_95`, `.q0_99` |
| `Histogram` | `.count`, `.sum`/`.min`/`.max` when present, `.bucket_<b>` per bucket |
| `ExponentialHistogram` | `.count`, `.sum`/`.min`/`.max` when present, `.zero_count` |
| `Summary` | `.count`, `.sum`, `.q<q>` per quantile it carries |
| `Set` | `.count` (the estimate) |

Amended (2026-10-07): the `Distribution` row gains `.min` and `.max` after `.sum`; see the
amendment at the end.

The table moves into a new shared module, `logit_proto::multi_value`, whose module doc becomes
its one canonical copy. Both `graphite_out` and `statsd_out` call it, and it reports each
component's suffix, value, and part (count, sum, bucket, zero count, quantile, min, max, or
distinct count). This ADR summarizes the table; the module doc is authoritative.

- **Graphite's wire output doesn't change.** (Superseded for the sketch row, 2026-10-07: see
  "Amendment: a sketch's min and max expand".) The extraction moves code, the table, and the
  number-token injectivity argument; a golden test pins every kind's plaintext and pickle output
  across the move.
- **Splunk's table stays its own.** It follows an external exporter's naming, which isn't the
  dotted convention statsd and carbon consumers read.
- **The `Distribution` row gains no `.min` or `.max`.** See "Why no `.min` or `.max`" below.
  Superseded (2026-10-07): see the amendment at the end.

A non-finite component is skipped, and the rest of the record is still written. An empty sketch
yields `.count 0` and `.sum 0` and no quantiles.

### Type letter per component
Each component is a counter (`|c`) only when it's additive and its kind counts per window.
Everything else is a gauge (`|g`).

| Component | Per-window kind: `Distribution`, delta `Histogram`, delta `ExponentialHistogram` | Running-total kind: cumulative `Histogram` or `ExponentialHistogram`, every `Summary` |
|---|---|---|
| `.count`, `.sum`, `.bucket_<b>`, `.zero_count` | `\|c` | `\|g` |
| `.q<q>`, `.min`, `.max` | `\|g` | `\|g` |

Three cases fall outside the table:

- **A `Set`'s `.count` is `|g`.** It's a cardinality estimate, and two estimates don't add.
- **A cumulative `Sum`**, monotonic or not, is one bare `name:v|g` line: the running total as a
  gauge, with no suffix.
- **A non-monotonic delta `Sum`** is one bare `name:v|c` line. A statsd counter takes a signed
  increment, and statsd has no carrier for the non-monotonic flag: `statsd_in` reads `-5|c` back
  as a delta `Sum` flagged monotonic, the kind `statsd_out`'s delta-monotonic arm already writes
  as `|c`. The flag is what's lost, which is why the record counts as degraded.

`aggregate` re-emits a series' attributes on its flush, so a `Distribution` still carries
`statsd.type` (`ms`, `h`, or `d`). Expansion ignores it: the carrier names the raw wire type of
samples that are no longer there.

**Why the split falls there.** A statsd server sums every `|c` line for a name across its flush
interval and across every sender. For a per-window count or sum, that's the correct merge, and
it's the merge a split-collection topology needs: two `logit` processes each flushing
`req.latency.count:3|c` for their own share of the traffic add up to the true total downstream.
Quantiles, minimums, maximums, and cardinality estimates aren't additive, so summing them
produces a number with no meaning. A running total is additive in the wrong way: sent as `|c`,
a cumulative count is added to itself on every flush and grows without bound at the receiver. A
gauge keeps the last value written, which is the right reading for all of these.

### Negative gauges
A negative `|g` component, such as a quantile of a distribution of negative values or a negative
cumulative `Sum`, uses the existing two-line idiom from [ADR `statsd-output`](statsd-output.md)'s
"Negative absolute gauges" section: `name.q0_5:0|g` then `name.q0_5:-3|g`, as one indivisible
entry. Without it, a receiver reads a leading `-` on a gauge as a relative adjustment. A `-0`
component renders as a plain `0|g`, as for any gauge. A counter component needs no pair, because
`|c` has no absolute form to confuse.

### Tags, dialect extras, and packing
An expanded line is an ordinary metric line:

- **Tags and extras.** Each line carries the event's `|#tags` and the DogStatsD extras
  (`|c:<container-id>`, `|e:<external-data>`, `|card:<cardinality>`, `|T<unix-seconds>`) any
  metric line from that event carries. No expanded line carries `@<rate>`: the components describe
  what an upstream summary already absorbed (a `DdSketch` built by `aggregate` has each sample's
  rate folded into its count and sum), and a sample rate on a `|c` line would extrapolate twice, as
  `crates/logit-outputs/src/statsd.rs`'s module doc already rules for counters
  under "Sample rate: never for a counter, real for `Samples`".
- **`format: statsd`.** The tag segment is dropped and counted as for any event, and each dropped
  extra is counted per line it would have appeared on.
- **Packing.** Each component line is its own `MessageBuf` entry, and a negative pair is one
  entry. A UDP packer can split an expansion across datagrams, and a line longer than
  `max_packet_bytes` is dropped and counted alone, so an oversize expansion loses one line, not
  the record.

### Telemetry
Each record is counted once, however many lines it writes or fails to write:

- **`skip`** counts `logit.output.metrics.skipped{metric_kind}`.
- **`expand`** counts `logit.output.metrics.degraded{metric_kind}`.

The `metric_kind` tag is one of `distribution`, `set`, `histogram`, `exponential_histogram`,
`summary`, `cumulative_sum`, or `non_monotonic_delta_sum`. The counter names are the ones
`graphite_out` and `splunk_hec_out` already report for the same switch, and the first five tag
values are theirs too. The two `Sum` values are this sink's own: `graphite_out` writes every `Sum`
bare and never tags one, and `splunk_hec_out` splits the cases differently (`delta_sum`,
`non_monotonic_sum`) because its wire carries a cumulative monotonic `Sum` natively. A dashboard reading
`degraded` sees how much of the sink's traffic left as per-component lines instead of a sketch.

`logit.output.messages.dropped{reason="unsupported_kind"}` and the `EncodeStats` field behind it
are removed, with no alias. `logit` is pre-release and keeps no compatibility for telemetry names,
and a record isn't a message: one expanded record is several messages.

`skip` keeps a throttled `unsupported_metric_kind` warning, with a hint naming `multi_value: expand`
and, for a `Distribution` or `Set`, `aggregate`'s `distributions: samples` or `sets: members`.
`expand` emits no diagnostic. It's the default path, and a warning on every pipeline that uses
`aggregate` with defaults would be noise. `graphite_out` warns `multi_value_expanded` because there
`expand` is an opt-in the operator may want confirmed.

### What doesn't expand
`Samples` and `SetMembers` keep their native `|ms`/`|h`/`|d` and `|s` lines under both settings.
They're the raw shapes `statsd_in` decodes, and a `statsd_in -> statsd_out` relay with no
`aggregate`, or with raw retention, still round-trips a timer or set line intact. A `Gauge`,
`GaugeDelta`, and delta monotonic `Sum` render as they do today.

So the expansion renders only components an upstream summary already held, whether an explicit
`aggregate` produced it or a producer sent it that way (`otlp_in`, `prometheus_in`, and
`datadog_in` all decode histograms, summaries, cumulative sums, or sketches straight from the
wire, and those pipelines change from drop to expand under the default too). The sink summarizes
nothing, so [ADR `lossless-transit`](lossless-transit.md)'s "Summarization is opt-in and named"
rule is untouched. The expansion is a named, counted degradation. It isn't one of the
like-protocol pair's permitted normalizations, and `statsd_in -> statsd_out` stays lossless only
on the raw shapes.

### Why `expand` is the default here and `skip` elsewhere
A statsd receiver consumes flattened scalar names natively. `.count`, `.upper`, and
`.95percentile` are the ecosystem's own output shape, so `x.count` and `x.q0_99` read at a statsd
server like any tier's flush. Under `skip`, the default pipeline loses the dominant statsd
workload, timers, with nothing but a counter to show for it.

The costs that justify `skip` in the other two sinks are absent here. In graphite, every sub-path
is a persistent whisper file on the carbon server, created on first write and kept until someone
deletes it. In Splunk, every series counts against licensed ingest volume. Both are reasons for
an operator to opt in. A statsd server holds a name only for its flush interval, and it bills
nothing per name.

### Why no `.min` or `.max`
Superseded (2026-10-07): see the amendment.

A `DdSketch` tracks its own minimum and maximum, and Etsy statsd's `.lower`/`.upper` and the
Agent's `.max` show consumers want them. They aren't added:

- The table is shared, and adding rows to it changes graphite's wire output, which this decision
  holds fixed.
- The five quantiles, from `.q0_5` to `.q0_99`, bound the distribution closely enough for the
  dashboards these consumers drive.
- A later amendment can add both rows to the shared table and change both sinks at once, with a
  graphite wire change recorded in its ADR.

## Alternatives considered
- **Synthesize samples at the sketch's quantile boundaries and send them as `|ms` lines.** This
  fabricates a population that never existed. A downstream statsd server then computes
  percentiles of percentiles and a `.count` equal to however many points were synthesized, with
  nothing on the wire to mark either as wrong.
- **`|h` or `|d` lines carrying fake `@rate` weights**, so one line per bucket or quantile stands
  for its count. The receiver extrapolates each line by `1/rate` into a count it treats as real
  observations, and every receiver rounds or caps sample rates differently. It's the same
  fabricated population with a weight attached.
- **`|s` lines for a `Set`.** The members are gone; a `HyperLogLog` holds registers, not values.
  Sending placeholder members would make the downstream count depend on what placeholders were
  chosen.
- **`skip` as the default, matching the other two sinks.** It keeps today's behavior: the default
  pipeline drops every timer and set. See "Why `expand` is the default here and `skip` elsewhere".
- **A statsd-native suffix table** (`.upper`, `.lower`, `.mean`, `.median`, `.upper_90`, or the
  Agent's `.avg`/`.95percentile`). Etsy statsd and the Agent disagree on the names, so there's no
  single native table to copy, and a second table is a second copy of a list the codebase keeps
  one copy of. The graphite table is already in tree, already documented, and its number tokens
  are injective.
- **Keep `messages.dropped{reason="unsupported_kind"}`** for `skip`. It counts in the wrong unit:
  a skipped record is one metric, not one wire message, and an expanded record is several
  messages that no `messages` counter describes as one degradation. `metrics.skipped` and `metrics.degraded` are the units the
  other two sinks already use for the same switch.

## Consequences
- **Downstream can't merge quantiles or estimates across senders.** Two `logit` processes each
  sending `x.q0_99` and `uniq.count` as gauges leave the receiver with whichever arrived last. The
  counts and sums merge correctly; the rest are per-sender values. A split-collection topology that
  needs a correct global quantile or cardinality keeps the sketch in the model and sends it over
  the native hop, not over statsd.
- **A suffix can collide with a producer's own name.** A record named `x` expands to `x.count`, and
  a producer that already sends its own `x.count` writes to the same name at the receiver.
  `graphite_out` has the same note in its module doc. Nothing guards it.
- **One timer series becomes about seven lines.** A `Distribution` writes seven lines per flush, so
  a downstream statsd server stores seven names, and a downstream `logit` interns seven names and
  carries seven records, where the raw relay carried one. Amended (2026-10-07): nine lines, with
  `.min` and `.max`.
- **Behind a Datadog Agent, raw retention stays the better choice.** With `aggregate`'s defaults,
  the Agent receives `.count`, `.sum`, `.min`, `.max`, and `.q*` lines as plain counts and gauges
  and never sees a distribution, so its own timer aggregates and percentiles are never computed.
  To keep the Agent's own timer aggregates and its `d` sketches, use
  `distributions: samples` and `sets: members`, or no `aggregate` at all.
- **Two older ADRs gain amendments with the code.** [ADR `statsd-output`](statsd-output.md)'s
  "Metric-kind coverage" and "What's still deferred" sections and
  [ADR `graphite-carbon-relay`](graphite-carbon-relay.md)'s sub-path table are amended in the
  changes that implement this decision, not here.
- **`docs/known-gaps/statsd.md` keeps a narrowed entry.** Summarized kinds no longer drop by
  default; they leave as per-component lines that don't merge downstream as a sketch would, and
  `skip` restores the drop. The entry stays tracked as debt against
  [ADR `lossless-transit`](lossless-transit.md).

## Amendment: a sketch's min and max expand (2026-10-07)

The shared table's `Samples`/`Distribution` row gains `.min` and `.max`, after `.sum` and before
the quantiles, the order the `Histogram` row already uses. This supersedes "Why no `.min` or
`.max`" above. `logit_proto::multi_value`'s module doc remains the canonical table.

| Kind | Components |
|---|---|
| `Distribution` | `.count`, `.sum`, `.min`, `.max`, `.q0_5`, `.q0_75`, `.q0_9`, `.q0_95`, `.q0_99` |

- **Why now.** The reasons for holding them back were graphite's wire, which this amendment
  changes, with its own record in [ADR `graphite-carbon-relay`](graphite-carbon-relay.md),
  and a judgment that five quantiles bound the distribution well enough. Etsy statsd's
  `.lower`/`.upper` and the Agent's `.max` show the extremes are what operators alert on, and a
  `q0_99` isn't a maximum: an outlier above it is invisible without `.max`.
- **Where the values come from.** `DdSketch::min` and `DdSketch::max` are tracked from the
  observations alongside its sum, not read from bins. A sketch decoded from bins alone, such as
  one from the DDSketch protobuf, derives all three from bin representatives, so they're
  approximate there, as its quantiles are.
- **Empty sketch.** An empty sketch has no minimum or maximum, so it still writes `.count 0` and
  `.sum 0` and nothing else.
- **Type letter.** Both are gauges (`|g`), per "Type letter per component" above: an extreme
  doesn't add across windows or senders. A negative minimum or maximum goes through the
  `0|g` + `-v|g` pair, as a negative quantile does.
- **Line count.** A `Distribution` writes nine lines per flush instead of seven, which updates the
  "about seven lines" consequence above. The encoder still allocates nothing for them.
- **Merging downstream.** Like a quantile, a downstream statsd server keeps the last sender's
  `.min` and `.max`, so two senders' extremes don't combine into the true global extreme.
