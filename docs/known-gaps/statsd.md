# Known gaps: statsd

Entry format and the other areas: [the known-gaps index](README.md).

- **`statsd_out` writes summarized metric kinds as per-component lines that don't merge
  downstream as a sketch would.** `Distribution`, `Set`, `Histogram`, `ExponentialHistogram`,
  `Summary`, and a cumulative or non-monotonic `Sum` exist only after a stage has summarized, and
  no statsd line carries a `DdSketch` or a `HyperLogLog`. Under `multi_value: expand`, the default,
  each leaves as dotted `.count`/`.sum`/`.min`/`.max`/`.q*`/`.bucket_*` counter and gauge lines,
  counted `logit.output.metrics.degraded{metric_kind}`
  ([ADR `statsd-out-multi-value-expansion`](../adr/statsd-out-multi-value-expansion.md)).
  - **What round-trips:** a `statsd_in -> statsd_out` relay with no `aggregate`, or with one
    configured `distributions: samples`/`sets: members`, relays a timer line intact, and a set
    line as one `|s` line per member, which reads back as one record per member: `Samples` and
    `SetMembers` never expand.
  - **What degrades:** `aggregate`'s defaults (`distributions: sketch`/`sets: estimate`) turn every
    timer and set into a sketch or an estimate, which leaves expanded. Counts and sums merge
    correctly across senders at a statsd server; quantiles, extremes, and set estimates are
    gauges, so the receiver keeps the last sender's value. The `samples`/`members` config keeps a
    window's raw shape only while every sample in it shares one sample rate and the window stays
    under `max_samples_per_series`/`max_set_members_per_series`. Past either limit, `aggregate`
    falls back to a sketch or estimate for that window, which this sink expands and counts the
    same way.
  - **What drops:** `multi_value: skip` restores the drop, counted
    `logit.output.metrics.skipped{metric_kind}` with a throttled `unsupported_metric_kind`
    warning.

  Tracked as debt against [ADR `lossless-transit`](../adr/lossless-transit.md)'s "Residual debt".

- **An expanded component with a negative value leaves as a two-record gauge pair, uncounted.**
  Under `multi_value: expand`, every `|g` component of a summarized kind goes through the
  negative-gauge path: a negative `.min`, `.max`, or `.q*` leaves as `name.min:0|g` then
  `name.min:-v|g`, and so does the negative `.sum` of a `Summary` or of a cumulative `Histogram`
  or `ExponentialHistogram`. A statsd server ends at the right value, but `statsd_in` reads the
  pair back as a gauge of 0 plus a relative adjustment, two records. Only the kind's `degraded`
  count covers it (`logit.output.metrics.degraded{metric_kind}`); the pair itself isn't counted,
  unlike a negative `Gauge` record's pair, which counts
  `logit.output.metrics.degraded{reason="negative_gauge"}`. A
  `|c` component such as a sketch's or delta histogram's negative `.sum` is a signed increment and
  isn't affected. An OTLP exponential histogram with a negative `min` reaches it in the
  `otlp-to-statsd` transit flow. The general negative-gauge case is the `gauge/negative` row in
  `spec/transit/mapping.yaml`; the expansion case has no row of its own.

  **Workaround:** none short of `multi_value: skip`, which drops the kind. Revisit with a
  `mapping.yaml` row for the expanded case, which the exponential-histograms case can check.

- **`statsd_out` has no `unit` and no metric renaming/prefixing, and carries an egress timestamp
  only on a `|T`-marked line.**
  - **Timestamp:** `statsd_out` writes no timestamp for an event that isn't flagged
    `_logit.metrics.closed: "timestamped"`, so the receiver stamps its receipt time, like
    `syslog_out`.
    DogStatsD's `|T<unix-seconds>` segment (`format: dogstatsd` only) round-trips: `statsd_in`
    sets `Event::timestamp` from it and flags the event `timestamped`, and `statsd_out` writes
    `|T<secs>` from `Event::timestamp`, truncated to whole seconds, for any event so flagged. A
    stage that rewrites `Event::timestamp` moves the `|T` with it, and a scraped Prometheus
    sample with its own timestamp leaves with one too, since the scrape sets the same flag.
    `aggregate` passes a flagged record through unmerged with its own timestamp. The classic
    grammar has no timestamp segment, so `format: statsd` omits `|T` and counts it
    (`fields.omitted{reason="dialect"}`).
  - **Unit:** `MetricRecord::unit` has no statsd wire representation and is dropped.
  - **Renaming:** there's no native, sink-level way to rename or namespace a metric on egress.
    `docs/adr/statsd-output.md`'s Alternatives rejects a sink-side `prefix` field in favor of a
    general metric-rename *transform* (native, not Lua), which doesn't exist yet.
    **Workaround:** a `lua` component ahead of `statsd_out` can rename or retag;
    `event.metrics[i].name` is writable on every kind (`docs/design/lua-api.md`'s "Reading and
    writing `event.metrics`").

  Tracked as debt against [ADR `lossless-transit`](../adr/lossless-transit.md)'s "Residual debt".

- **A span beside a DogStatsD event is left off uncounted.** `statsd_out` writes an event that
  carries no metrics only when its log is a DogStatsD event (`_logit.statsd.event.title`), as one
  `_e{}` line, and a span on that event has no statsd form. The sink writes the line and leaves
  the span off with no count, where a span beside any other log, or on its own, counts the event
  `logit.output.events.skipped{reason="no_metrics"}`. No decoder puts a span beside a DogStatsD
  event; a `lua` stage or a hand-written `replay_in` case can, and the `statsd-logs-relay`
  transit flow does (`span/beside_log` in `spec/transit/mapping.yaml`).

  **Revisit:** count the span as `logit.output.spans.omitted`, as `kafka_out`'s `format: text`
  counts a span beside the log it writes, and make the cell name that counter.

- **Relative gauge adjustments: three by-design residuals.** `statsd_in` decodes a leading `+`/`-`
  on a `g` value into an unresolved `MetricKind::GaugeDelta`. `aggregate` resolves it against its
  running gauge value and retains gauge series across flushes under `series_retention` (a
  windows-count TTL, default `5`) and `max_retained_series` (a cardinality cap, default `10,000`)
  ([ADR `relative-gauge-adjustments`](../adr/relative-gauge-adjustments.md),
  [ADR `aggregation-window-semantics`](../adr/aggregation-window-semantics.md)'s amendment).
  - **Retention is on by default for every `aggregate`.** With high-cardinality, slowly churning
    gauge tags, each `aggregate` holds up to `max_retained_series` idle series for up to
    `series_retention` extra windows. `logit.transform.series.retained` shows the number;
    `series_retention: 0` restores strictly tumbling behavior. It isn't opt-in because the
    feature's point is resolving deltas correctly.
  - **A delta after eviction (the cardinality cap) or after a process restart resolves against
    0.0.** Eviction is counted (`logit.transform.gauge.delta.unseeded`,
    `logit.transform.series.evicted{reason="cardinality"}`), never silent. The restart case needs
    durable aggregator state, which isn't built: ADR `aggregation-window-semantics`'s objection to
    cumulative counters ("state grows unbounded with series cardinality and a process restart
    resets every series to zero with no way to detect that from the emitted stream") applies to a
    retained gauge too. Retention narrows the window; it doesn't close it. A *cumulative* series
    has the same exposure but not the same blindness: every point carries a `start_timestamp`, so
    a consumer can see the restart. A gauge has no such field, and inventing one is off the table.
  - **A `GaugeDelta` reaching a sink with no `aggregate` on its path degrades to a throttled,
    per-metric drop, not a config-time error.** Every protocol sink skips it, counted (the
    cross-protocol `GaugeDelta` row); `statsd_out` under `relative_gauges: true` relays it
    instead. A `logit validate` graph check ("a statsd input reaches an output with no
    `aggregate` on the path") is implementable in `logit-pipeline::graph`, but it has a real false
    positive (resolving downstream in another collector is legitimate), and `logit validate` has
    no warning channel, only pass/fail. Deferred.
