# Known gaps: Prometheus

Entry format and the other areas: [the known-gaps index](README.md).

- **The remote-write receiver's metadata table is shared by every sender that can reach it, and
  peers can evict each other's entries.** Each `prometheus_in(bind)` component has one table, not
  one per peer, evicted strictly by `last_seen` (`crates/logit-inputs/src/prometheus.rs`'s
  "Metadata cache" module-doc section). Sharing lets a 2.0 sender's inline declarations type a 1.0
  sender's series, but a peer declaring many families pushes others out of `max_families`, counted
  `.evicted{reason="cardinality"}` with no attribution.
  - **Consequence:** if this repeats faster than the victims' `metadata_config` cadence,
    well-behaved senders stay untyped permanently. Their samples still arrive, as flat families
    (the remembered type is advisory; see [Deploying `logit`](../deploying.md#prometheus-remote-write-receiving-sending-and-picking-a-version)'s
    `metadata_cache:` notes).
  - **Workaround:** give each population of senders its own `bind:`, each with its own table.
    `bind_auth:` limits who can write, not what an accepted sender declares, so it doesn't stop
    one authenticated sender evicting another's families. `max_families: 0` turns off the sharing
    along with the typing.
- **A later batch repeating a series and millisecond with a new value draws a `400` that drops
  the whole batch.** `prometheus_out(endpoint)` collapses readings of one series that share a
  millisecond within one batch, keeping the later and counting the rest
  `logit.output.metrics.dropped{reason="duplicate_timestamp"}`, or `normalized` for a state
  equal to the one kept, and `dedup_timestamps` works within
  one batch too ([ADR `dedup-timestamps-transform`](../adr/dedup-timestamps-transform.md)'s
  "Scope"). Neither sees across batches. Prometheus 3.15.1 and Mimir 3.2.2, run 2026-10-21, accept a
  repeat with the same value silently and answer one with a new value
  `400 duplicate sample for timestamp`.
  - **Consequence:** the sink reads the `400` as `Rejected` and drops the batch, counted
    `batches.dropped{reason="rejected"}`. Against Prometheus under remote-write 1.0, the sink's
    default `version:`, that loses every clean sample in the batch, because Prometheus stored
    none of them. Against Prometheus under 2.0, or Mimir under either version, the clean samples
    were stored, so the drop counter overstates the loss.
  - **Workaround:** keep one series' readings of one millisecond in one batch: a source that
    stamps sub-millisecond readings rarely splits them across batches, and an `aggregate` upstream
    leaves one point per series per window.
  - **To close:** undecided. Reading the `400` body, which names the series, could tell a batch
    whose clean samples were stored from one whose weren't; making `version: 2` the default would
    make the clean samples stored on Prometheus too.
- **Two metric names that sanitize onto one merge across batches, uncounted.** `prometheus_out`
  writes a name's `.`, `-`, and other characters outside the metric-name grammar as `_`, counted
  `logit.output.metrics.normalized{reason="name_sanitized"}`. Within one batch, two names that
  land on one wire name keep one family and count the other's records
  `metrics.dropped{reason="name_collision"}` (`crates/logit-proto/src/prometheus/mod.rs`'s encode
  table). Across batches nothing compares them: the exposition registry keys a family by its wire
  name, and a remote-write receiver keys a series by its labels.
  - **Consequence:** `http.requests` in one batch and `http_requests` in the next are one series,
    each batch's sample replacing the other's, with no `name_collision` for either. Label names
    merge the same way across batches.
  - **Workaround:** a nonzero `metrics.normalized{reason="name_sanitized"}` says some name was
    rewritten; rename upstream (a `lua` stage) so no two producers' names sanitize onto one.
- **The scrape client still follows HTTP redirects**, for `prometheus_in` in scrape mode and
  `nginx_stub_status_in` alike. It builds its own `reqwest::Client` with no redirect policy
  (`crates/logit-inputs/src/scrape.rs`, the scrape driver), so it inherits `reqwest`'s
  `limited(10)`.
  - **Consequence:** a `307`/`308` replays the request and the operator's `headers:` at the
    `Location` host, past a config-time `https://` check that has no say at runtime. `reqwest`
    strips only `Authorization`/`Cookie`, and only on a host or port change, so a tenant header
    always travels, as would a scrape URL's basic-auth credential.
  - **To close:** the HTTP sinks share `crates/logit-outputs/src/http.rs`'s `build_client`, which
    turns redirects off; that helper's doc comment has the reasoning. `logit-inputs` doesn't
    depend on `logit-outputs`, so the scrape client needs its own copy of the policy, or a shared
    home for it.
- **A remote-write lost at shutdown behind an open transform is still answered `204`.**
  `prometheus_in(bind)` answers `503` for a write no direct consumer took (counted
  `logit.input.writes{class="closed_consumer"}`), but still acknowledges a shutdown-time closure
  that reaches only a sink behind an open transform. That's the part of
  [`design/pipeline-graph.md`](../design/pipeline-graph.md)'s "Open question: a closed downstream"
  that stays open ([ADR `delivery-semantics`](../adr/delivery-semantics.md), item 3, and its W3
  amendment).
  - **Consequence:** the sender counts the write delivered and doesn't resend it.
- **An `ExponentialHistogram` can't reach VictoriaMetrics's native-histogram ingest over
  remote-write.** VictoriaMetrics accepts a remote-write native histogram and converts it to its
  `vmrange` buckets, but `prometheus_out` writes no `ExponentialHistogram` on either wire, and a
  remote-write one is dropped and counted `logit.output.metrics.dropped{reason="native_histogram"}`:
  see the native-histogram row under [Cross-protocol mappings](mappings.md).
  - **Workaround:** send it over OTLP. `otlp_out` to VictoriaMetrics's `/opentelemetry` carries
    it, and VictoriaMetrics stores it as `_bucket` series with a `vmrange` label plus `_count` and
    `_sum` (verified, leg 6).
  - **Revisit trigger:** closes with that native-histogram row.
- **A `Distribution` isn't re-binned onto VictoriaMetrics's `vmrange` buckets.** Both are
  log-bucketed, but `prometheus_out` sends a `Distribution` as the fixed-quantile summary it sends
  any Prometheus receiver. VictoriaMetrics's histogram functions (`prometheus_buckets()`,
  `histogram_quantile()` over `vmrange`) don't apply to it, and the quantiles can't be merged
  across series. VictoriaMetrics doesn't impose this loss: re-binning is a mapping nobody has
  built, and a non-goal of [ADR `victoriametrics-interop`](../adr/victoriametrics-interop.md).
- **A series scraped back from VictoriaMetrics's `/federate` is untyped.** `/federate` emits no
  `# TYPE` or `# HELP`, so `prometheus_in` decodes every series as a `Gauge` tagged
  `_logit.prometheus.type: "untyped"`: a counter can't be told from a gauge, and a histogram's
  `_bucket`, `_sum`, and `_count` arrive as unrelated series (verified, leg 9). VictoriaMetrics
  stores no metric type, so there's nothing for `logit` to recover.
- **Some rows of `prometheus_out`'s and `influxdb_out`'s response-class tables rest on
  documentation and secondhand bodies, not a recorded response.** `prometheus_out`'s Grafana Cloud
  `401` body (`authentication error: invalid authentication credentials`) comes from a forum
  post, and only a test uses it. `influxdb_out`'s `413` and parse-error (`unable to parse ...`)
  texts follow InfluxDB's documentation and are unverified; that sink classifies by status alone,
  so the text matters only to the diagnostic. The tables are
  `crates/logit-outputs/src/prometheus.rs`, "Faults, retries and duplicate safety (sender mode)",
  and `crates/logit-outputs/src/influxdb.rs`, "Response classes".
  - **Consequence:** none known for `influxdb_out`. For `prometheus_out`, only the decompression
    `400` reads the body; a receiver whose decompression text differs from the matched prefixes
    reads as `Rejected` and drops the batch where it would hold.
  - **Revisit trigger:** a recorded response from a real Grafana Cloud or InfluxDB stack.
