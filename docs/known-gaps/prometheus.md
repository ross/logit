# Known gaps: Prometheus

Entry format and the other areas: [the known-gaps index](README.md).

- **`prometheus_out` has no TLS and no auth either** (ADR `prometheus-scrape-and-exposition`'s
  "Security posture"). Anyone who can reach its `bind:` reads the whole registry: every label on
  every series the sink holds. The admin endpoint's matching gap ("The admin endpoint has no TLS
  and no auth" under
  [Admin endpoint, readiness, and release image](runtime.md#admin-endpoint-readiness-and-release-image))
  is a non-goal; this one is deferred, because every exposition-mode `prometheus_out` listens, and
  its payload is the full metric surface, which reveals far more than a lifecycle phase.
  - **Workaround:** `fixtures/prometheus-expose.yaml` binds `127.0.0.1`, and the field's doc
    comment says to keep it loopback or pod-local behind something that provides both.
  - **To close:** server-side TLS can reuse `logit-inputs`' existing builder (`otlp_in`'s
    `tls:`). Auth has no in-tree precedent on any listener, so the kind (bearer, mTLS) needs a
    decision before code.
- **The remote-write receiver has TLS but no authentication** ([ADR
  `prometheus-remote-write`](../adr/prometheus-remote-write.md)'s "Security posture";
  `crates/logit-inputs/src/prometheus.rs`'s module doc). `prometheus_in`'s `bind_tls:` is transport
  security, not identity: no bearer token, no basic auth, and no mutual-TLS identity check beyond
  `rustls` accepting whatever chain a client presents when `client_ca_file` is set. Anything that
  can reach the socket can write series into the pipeline. It's the same gap as `admin:` and
  `prometheus_out`'s exposition `bind:` ("The admin endpoint has no TLS and no auth" and
  "`prometheus_out` has no TLS and no auth either"), for the same reason: auth has no in-tree
  precedent, so the kind (bearer, mTLS subject matching) needs deciding first.
  - **Workaround:** bind loopback or pod-local and front it with something that authenticates
    ([`fixtures/prometheus-remote-write-receive.yaml`](../../fixtures/prometheus-remote-write-receive.yaml);
    `docs/deploying.md`'s "Prometheus remote-write" section).
- **1.0 remote-write typing depends on the metadata cache, which is bounded and therefore lapses**
  ([ADR `prometheus-remote-write`](../adr/prometheus-remote-write.md)'s "The receiver is stateless";
  `prometheus_in`'s "Metadata cache" module-doc section). An expired family decodes untyped again
  (its samples are `unknown` and its derived series come apart) until the sender's next metadata
  request re-declares it.

  **Why a cache:** remote-write 1.0 carries a family's type, `# HELP`, and `# UNIT` in
  `WriteRequest.metadata[]`, and Prometheus's 1.0 sender ships those in separate requests on its
  own schedule (`metadata_config`, once a minute by default). Without a cache, nearly every family
  decodes as `unknown`, and `http_request_duration_seconds_bucket`/`_sum`/`_count` arrive as three
  unrelated series instead of one histogram. No sample is lost either way: only a declared family
  name claims a suffix, so an undeclared `foo_sum` is its own family
  (`crates/logit-proto/src/prometheus/assemble.rs`). The cache buys typing, not data. 2.0 needs
  none of this: every request is fully typed.

  **Configuration:** `metadata_cache:` defaults to `max_families: 10000` and `ttl: 10m`, evicting
  the least recently seen family first over the cap. `max_families: 0` turns it off, the setting
  for a pure-2.0 fleet. The default TTL is ten times Prometheus's metadata cadence, so a live
  sender must miss ten refreshes in a row to lapse. A sender with a longer `metadata_config`
  interval, or one that went quiet and came back, will lapse.

  **What to watch:** `logit.input.metadata_cache.size` (gauge),
  `.evicted{reason="expired"|"cardinality"}`, and `.replaced`. A steady `expired` stream against a
  live sender means `ttl` is under that sender's cadence.

  The same cache has two smaller bounds:
  - A remembered `# HELP`/`# UNIT` is cut to a fixed byte cap (`MAX_METADATA_TEXT_BYTES`, counted
    `logit.input.metadata_cache.truncated`), because the table outlives the request whose size cap
    bounded it. The type is remembered in full; only description text is affected.
  - A remembered type is advisory, never authoritative. Where a declaration carried in the request
    itself would make the assembler discard a sample, a remembered one yields, and the sample opens
    an implicit family of its own, counted `logit.input.metrics.degraded{reason="seed_mismatch"}`
    (`crates/logit-proto/src/prometheus/assemble.rs`'s "A seeded type is advisory" table). A stale
    memory costs typing, never data. A steady `seed_mismatch` stream means the table and the
    senders disagree about a family's shape; chase it rather than tune it.
- **The remote-write receiver's metadata table is shared by every sender that can reach it, and
  peers can evict each other's entries.** Each `prometheus_in(bind)` component has one table, not
  one per peer, evicted strictly by `last_seen` (`crates/logit-inputs/src/prometheus.rs`'s
  "Metadata cache" module-doc section). Sharing lets a 2.0 sender's inline declarations type a 1.0
  sender's series, but a peer declaring many families pushes others out of `max_families`, counted
  `.evicted{reason="cardinality"}` with no attribution.
  - **Consequence:** if this repeats faster than the victims' `metadata_config` cadence,
    well-behaved senders stay untyped permanently. Their samples still arrive, as flat families
    (the remembered type is advisory; see "1.0 remote-write typing depends on the metadata
    cache").
  - **Workaround:** no sender is authenticated, so, as in "The remote-write receiver has TLS but
    no authentication", don't point a `bind:` at senders you don't control. `max_families: 0`
    turns off the sharing along with the typing.
- **The remote-write sender does no cross-batch reordering, so a fan-in topology can draw
  out-of-order `400`s.** `prometheus_out(endpoint)` sends samples in batch order ([ADR
  `prometheus-remote-write`](../adr/prometheus-remote-write.md)'s "Sender behaviour"; the sink's
  module doc). Both specs require one series' samples in timestamp order, so two upstream branches
  writing the same series into one `prometheus_out` can send an older sample after a newer one. A
  single chain into one `prometheus_out` can't hit this.
  - **Consequence:** a receiver with no out-of-order window (stock Prometheus, Mimir without
    `out_of_order_time_window`) answers `400`, which the sink classifies `Fault::Rejected` and
    drops.
  - **Workaround:** use a topology that doesn't split one series across branches, or a receiver
    with an out-of-order window. The sink won't buffer its way out: a reorder window is
    `aggregate`-shaped state a sink doesn't hold.
- **`prometheus_in`'s scrape client still follows HTTP redirects.** It builds its own
  `reqwest::Client` with no redirect policy (`crates/logit-inputs/src/prometheus.rs`'s scrape
  client), so it inherits `reqwest`'s `limited(10)`.
  - **Consequence:** a `307`/`308` replays the request and the operator's `headers:` at the
    `Location` host, past a config-time `https://` check that has no say at runtime. `reqwest`
    strips only `Authorization`/`Cookie`, and only on a host or port change, so a tenant header
    always travels, as would a scrape URL's basic-auth credential.
  - **To close:** the HTTP sinks share `crates/logit-outputs/src/http.rs`'s `build_client`, which
    turns redirects off; that helper's doc comment has the reasoning. `logit-inputs` doesn't
    depend on `logit-outputs`, so the scrape client needs its own copy of the policy, or a shared
    home for it.
- **`logit.input.samples` means two different things depending on `prometheus_in`'s mode.**
  - Scrape mode counts the series a scrape decoded (`events.len()`, one event per series,
    `crates/logit-inputs/src/prometheus.rs`'s `tick`).
  - Bind mode counts wire samples reaching the `Fanout`: for a classic histogram, one per
    `_bucket` plus `_sum` plus `_count`. This matches the 2.0
    `X-Prometheus-Remote-Write-Samples-Written` header; a counter that disagreed with that header
    would have no right answer.

  Summing `logit.input.samples` across a deployment running both modes adds series to samples.
  There's no mode tag: which of `logit.input.scrapes`/`logit.input.writes` a component reports
  already tells the modes apart.
- **A remote-write lost at shutdown behind an open transform is still answered `204`.**
  `prometheus_in(bind)` answers `503` for a write no direct consumer took (counted
  `logit.input.writes{class="closed_consumer"}`), but still acknowledges a shutdown-time closure
  that reaches only a sink behind an open transform. That's the part of
  [`design/pipeline-graph.md`](../design/pipeline-graph.md)'s "Open question: a closed downstream"
  that stays open ([ADR `delivery-semantics`](../adr/delivery-semantics.md), item 3, and its W3
  amendment).
  - **Consequence:** the sender counts the write delivered and doesn't resend it.
- **VictoriaMetrics discards remote-write 2.0 silently, and `prometheus_out` can't tell.**
  VictoriaMetrics v1.152.0 answers a 2.0 request `204` with an empty body and stores nothing, with
  nothing in its log and `vm_http_request_errors_total` unchanged
  ([`docs/plans/victoriametrics-interop.md`](../plans/victoriametrics-interop.md)'s "Findings", leg
  2). A `204` is success under both specs, and the sink doesn't read the 2.0
  `X-Prometheus-Remote-Write-*-Written` headers, so `prometheus_out` `version: 2` counts every
  batch delivered while all of it is lost.
  - **Workaround:** use `version: 1` for VictoriaMetrics (`docs/deploying.md`'s "Choosing
    `version: 1` or `2`"). Reading the `-Written` headers would help only against a receiver that
    sends them, which VictoriaMetrics doesn't.
- **An `ExponentialHistogram` can't reach VictoriaMetrics's native-histogram ingest over
  remote-write.** VictoriaMetrics accepts a remote-write native histogram and converts it to its
  `vmrange` buckets, but `prometheus_out` skips and counts every `ExponentialHistogram` on both
  wires: see the native-histogram row under [Cross-protocol mappings](mappings.md).
  - **Workaround:** send it over OTLP. `otlp_out` to VictoriaMetrics's `/opentelemetry` carries
    it, and VictoriaMetrics stores it as `_bucket` series with a `vmrange` label plus `_count` and
    `_sum` (verified, leg 6).
  - **Revisit trigger:** closes with that native-histogram row.
- **A `Distribution` isn't re-binned onto VictoriaMetrics's `vmrange` buckets.** Both are
  log-bucketed, but `prometheus_out` sends a `Distribution` as the five-quantile summary it sends
  any Prometheus receiver. VictoriaMetrics's histogram functions (`prometheus_buckets()`,
  `histogram_quantile()` over `vmrange`) don't apply to it, and the quantiles can't be merged
  across series. VictoriaMetrics doesn't impose this loss: re-binning is a mapping nobody has
  built, and a non-goal of [ADR `victoriametrics-interop`](../adr/victoriametrics-interop.md).
- **A series scraped back from VictoriaMetrics's `/federate` is untyped.** `/federate` emits no
  `# TYPE` or `# HELP`, so `prometheus_in` decodes every series as a `Gauge` tagged
  `prometheus.type="untyped"`: a counter can't be told from a gauge, and a histogram's `_bucket`,
  `_sum`, and `_count` arrive as unrelated series (verified, leg 9). VictoriaMetrics stores no
  metric type, so there's nothing for `logit` to recover.
