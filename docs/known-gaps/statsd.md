# Known gaps: statsd

Entry format and the other areas: [the known-gaps index](README.md).

- **`statsd_out` writes summarized metric kinds as per-component lines that don't merge
  downstream as a sketch would.** `Distribution`, `Set`, `Histogram`, `ExponentialHistogram`,
  `Summary`, and a cumulative or non-monotonic `Sum` exist only after a stage has summarized, and
  no statsd line carries a `DdSketch` or a `HyperLogLog`. Under `multi_value: expand`, the default,
  each leaves as dotted `.count`/`.sum`/`.q*`/`.bucket_*` counter and gauge lines, counted
  `logit.output.metrics.degraded{metric_kind}`
  ([ADR `statsd-out-multi-value-expansion`](../adr/statsd-out-multi-value-expansion.md)).
  - **What round-trips:** a `statsd_in -> statsd_out` relay with no `aggregate`, or with one
    configured `distributions: samples`/`sets: members`, relays a timer or set line intact:
    `Samples` and `SetMembers` never expand.
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

  Tracked as debt against [ADR `lossless-transit`](../adr/lossless-transit.md); the closing
  assessment's residual-debt list is in
  [`docs/plans/lossless-transit.md`](../plans/lossless-transit.md).

- **`statsd_out` has no `unit` and no metric renaming/prefixing, and carries an egress timestamp
  only on a `|T`-marked line.**
  - **Timestamp:** `statsd_out` stamps any event without a `statsd.timestamp` `U64` carrier
    (everything but a relayed `|T`-carrying line) with the receiver's receipt time, like
    `syslog_out`.
    DogStatsD's `|T<unix-seconds>` segment (`format: dogstatsd` only) round-trips: `statsd_in`
    sets `Event::timestamp` from it and stamps a `statsd.timestamp: Value::U64(secs)` carrier with
    the raw wire value. `statsd_out` re-emits `|T<secs>` from that carrier, never from
    `Event::timestamp`, so a stage that rebuilds `Event::timestamp` (notably `aggregate`'s flush)
    can't fabricate or collapse a `|T`. The classic grammar has no timestamp segment, so
    `format: statsd` drops `|T` and counts it (`dropped_dialect_fields`).
  - **Unit:** `MetricRecord::unit` has no statsd wire representation and is dropped.
  - **Renaming:** there's no native, sink-level way to rename or namespace a metric on egress.
    `docs/adr/statsd-output.md`'s Alternatives rejects a sink-side `prefix` field in favor of a
    general metric-rename *transform* (native, not Lua), which doesn't exist yet.
    **Workaround:** a `lua` component ahead of `statsd_out` can rename or retag;
    `event.metrics[i].name` is writable on every kind (`docs/design/lua-api.md`'s "Reading and
    writing `event.metrics`").

  Tracked as debt against [ADR `lossless-transit`](../adr/lossless-transit.md); the closing
  assessment's residual-debt list is in
  [`docs/plans/lossless-transit.md`](../plans/lossless-transit.md).

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
