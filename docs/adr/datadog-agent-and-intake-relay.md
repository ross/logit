---
created: 2026-09-23
updated: 2026-09-24
---

# Datadog: two lossless pairs, the Agent's own protocols, and a Datadog-mapped `DdSketch`

## Status
Accepted

## Context

`logit` sends nothing to Datadog except DogStatsD through `statsd_out`, and receives nothing
Datadog-shaped except DogStatsD through `statsd_in`. Migrating to or from Datadog needs `logit`
to stand in two places: in front of applications, where the Datadog Agent listens (DogStatsD on
`:8125`, the APM API on `:8126`), and behind a fleet of Agents, where Datadog's intake listens
(`dd_url`, `additional_endpoints` dual-shipping). Both are Datadog's own protocols, and the Agent
is open source, so the protocols are readable even where Datadog documents nothing.
[`docs/plans/datadog-relay.md`](../plans/datadog-relay.md) has the survey, the trade-offs, the
best-practice recommendation, and the workstreams; this record holds the decisions.

Three facts from the survey drive the shape of the decision:

- Datadog's OTLP ingestion is lossy for a dd-trace span. The Agent's OTLP receiver keeps only the
  low 64 bits of a trace id (the rest survives as an `otel.trace_id` tag, not `_dd.p.tid`),
  serializes span events and links into JSON strings in `meta`, has no path to `meta_struct`, and
  adds `otel.*` tags. A Datadog consumer can tell an OTLP-relayed span from the original, which
  fails [ADR `lossless-transit`](lossless-transit.md)'s test.
- Datadog's backend doesn't derive trace metrics from spans. The Agent computes them from every
  span before sampling and sends them as a separate `/api/v0.2/stats` payload; a sender that
  ships only `/api/v0.2/traces` gets empty service pages. Vector's `datadog_traces` sink, the one
  third-party implementation of this protocol, drops the Agent's stats and recomputes them,
  partially.
- The Agent's metrics sketch (`pkg/util/quantile`) carries no mapping parameters on the wire, so
  a receiver has to assume the Agent's exact bin mapping: `key = round_to_even(log_γ(v)) + bias`
  with γ = 1.015625. `sketches-ddsketch`, which backed `DdSketch`, keys by `floor`, exposes no
  bins, and serializes to a third format that is neither the Agent's `dogsketch` nor the DDSketch
  protobuf APM stats use.

## Decision

1. **Two new like-protocol pairs**, added to `lossless-transit` by amendment:
   `datadog_in -> datadog_out` (Datadog's intake API: series v1/v2, sketches, service checks,
   `/intake/` events, logs, `AgentPayload` traces, and `StatsPayload` APM stats) and
   `datadog_trace_in -> datadog_trace_out` (the Agent's APM API on `:8126`: every
   `/v0.3`–`/v1.0/traces` form, `/v0.6/stats`, `/info`). `statsd_in -> statsd_out` already covers
   DogStatsD and gains Unix-socket transports and the `|e:`/`|card:` suffixes. Both codecs live in
   `crates/logit-proto/src/datadog/`, one module per payload family whose doc is its mapping
   table.

2. **Traces go to Datadog over the Agent's own protocol, never OTLP, when they came from
   Datadog.** `datadog_out` sends `/api/v0.2/traces` and relays `/api/v0.2/stats`; it doesn't
   recompute stats. It decides readiness from the data, not the source component: a chunk whose
   root span carries the Agent-written `_top_level` metric has been through an Agent or an
   equivalent processor and goes out natively; one without it is counted
   `skipped{reason="needs_agent_processing"}`, so `datadog_trace_in` must not feed `datadog_out`
   directly. `otlp_out` remains the path for OTel-origin spans.

3. **`DdSketch` is hand-rolled** (`crates/logit-core/src/sketch.rs`), replacing
   `sketches-ddsketch`, and carries its bin mapping. `Mapping::agent` is the default for every
   sketch built in this process: the Agent's γ, bias, minimum, int16 key range, and
   collapse-lowest-at-4,096 policy, so `aggregate`'s output can be sent to Datadog as native
   sketches and an Agent's sketch relays bin-for-bin. `Mapping::logarithmic(gamma, index_offset,
   bin_limit)` is `sketches-go`'s mapping, what a decoded APM stats sketch keeps so it relays
   exactly. Counts are `f64` (the protobuf's type; the Agent's `uint16` counts are exact in it),
   bins are public, and a merge across mappings re-bins by representative value, a bounded-error
   normalization, never a failure. Quantiles are the bin center at the rank rather than the
   Agent's own `Sketch.Quantile` interpolation: Datadog's backend, not the Agent, evaluates a
   shipped sketch, that code is closed, and the bin center is the estimator whose `1 - 1/√γ`
   bound the Agent documents. The native wire's `Distribution` payload is the sketch's own
   `to_bytes` form, not the crate's "java bytes".

4. **`datadog_in` decodes zstd** with `ruzstd`, a pure-Rust decoder, because Agents compress with
   zstd by default and `additional_endpoints` can't vary the compressor per endpoint. The
   workspace's rationale against the `zstd` crate stays for compressing; senders use gzip.

5. **`datadog_in`'s backpressure is a bounded wait on delivery, then `503` with
   `Retry-After: 1`**, not `otlp_in`'s blocked connection. A Datadog Agent's forwarder times a
   request out at 20 seconds and retries it with backoff, so a connection held open until the
   pipeline drains costs the Agent a slot for 20 seconds and still ends in a retry: only an
   expensive `503`. The wait is `BUSY_AFTER` (5 s) per request. Delivery under it is
   all-edges-or-nothing (`Fanout::send_with_deadline`): every downstream consumer's channel slot
   is reserved before any consumer is sent the batch, so a `503` means no consumer holds the batch
   that timed out, and the Agent's retry is the only copy. Batches of a multi-batch request
   (traces, stats) fully delivered before the deadline are delivered again by that retry, which
   is what Datadog's own intake does with a resent request.

6. **msgpack is hand-rolled** in `logit_proto::msgpack` (`nil`, bool, int, float, str, bin, array,
   map), the pickle precedent; `agent-payload`'s protos are vendored at a pinned tag and generated
   by `script/protogen` ([ADR `committed-pregenerated-otlp-protobuf`](committed-pregenerated-otlp-protobuf.md)).

7. **Protobuf is decoded through prost and encoded by hand for the two map-bearing families.**
   Every Datadog protobuf is decoded with prost's generated types. `AgentPayload` and the DDSketch
   protobuf are encoded by hand instead (`crates/logit-proto/src/datadog/traces_proto.rs`,
   `stats.rs`). prost holds a map field as a `HashMap`, whose iteration order makes its bytes
   non-deterministic, and the fixed-point tests need `encode(decode(encode(d))) == encode(d)` on
   bytes. The hand-written encoders write fields in tag order, skip proto3 defaults as prost does,
   and sort every map by key. Each has a unit test proving prost decodes its bytes to the same
   message. This narrows
   [ADR `committed-pregenerated-otlp-protobuf`](committed-pregenerated-otlp-protobuf.md)'s
   rejection of hand-rolled protobuf to decoding, and to families without maps.

8. **Attribute vocabulary.** `datadog.*` for raw encodings (`datadog.type`, `datadog.interval`,
   `datadog.resources`, `datadog.source_type_name`, `datadog.origin.*`, `datadog.chunk.*`,
   `datadog.tracer.*`, `datadog.agent.*`, `datadog.stats.*`); Datadog's own OTLP-honored names for
   span fields (`service.name`, `resource.name`, `span.type`); `meta`/`metrics` keys verbatim;
   `statsd.event.*`/`statsd.service_check.*` for events and service checks, which are the same
   Datadog concepts DogStatsD carries. A Datadog `rate` is a `Gauge` with `datadog.type: rate`,
   not a `Sum`, because folding a per-second value into a delta multiplies and rounds.

9. **Trace ids** are built from a uint64 and `_dd.p.tid` on decode, and emitted as the low 64
   bits plus `_dd.p.tid` when the high bits are nonzero.

10. **`datadog_out` derives host, service, source, and tags from attributes and the resource**,
    never from per-sink fields; an upstream `set` supplies them. It drops and counts points older
    than Datadog's windows (1 h for metrics, 18 h for logs, 10 min for checks) before sending.

11. **`datadog_trace_in` shares decision 5's bounded-wait-then-`503` mechanism, but a `503` there
    is loss, not deferral, and the bound is shorter.** Delivery is the same
    `Fanout::send_with_deadline` all-edges-or-nothing wait, under `BUSY_AFTER` at 2 s rather than
    5 s: a dd-trace tracer writes with a short timeout, commonly 2 s, and drops the payload on any
    non-`2xx` answer or its own timeout, with no retry (UNVERIFIED against a real tracer until
    W7). So a `503` here doesn't defer delivery to a retry, the way it does on `datadog_in` — it
    is the loss, counted `logit.input.batches.dropped{reason="busy"}` rather than assumed
    recovered. The 2 s bound is sized to answer before the tracer gives up on its own, which would
    lose the payload the same way with nothing counted. The operator's lever against the loss is
    downstream capacity, not a retry: a `buffer:` (memory or disk) on the sinks behind this
    listener, sized to absorb a stall so the channel keeps draining. The
    `datadog_trace_in -> datadog_trace_out` pair's lossless-relay contract (decision 1) holds only
    while that channel drains; the counter makes a stall long enough to break it visible.

## Alternatives considered

- **OTLP as the only trace egress.** Nothing to build, and the documented direct path. Rejected
  for Datadog-origin spans because the mapping is lossy in ways a Datadog consumer sees, trace
  metrics would come only from sampled spans, and Datadog-only features (ingestion controls,
  ingestion reasons) are documented as unavailable for OTel-origin data. Kept for OTel-origin
  spans, where sending natively would mean synthesizing Datadog semantics and stats.
- **Recompute APM stats in `datadog_out`, as Vector does.** Rejected: the stats are already on
  the wire from every Agent and every tracer that computes them, the Agent's concentrator keys on
  fields (`span.kind`, peer tags, gRPC and HTTP keys) Vector's reimplementation lacks, and Vector
  documents the result as beta and incompatible with Agent sampling. Relaying is lossless;
  recomputing is a follow-up for the topology with no Agent in the path.
- **Keep `sketches-ddsketch` and convert at the codec edge.** Rejected: its `floor` keys differ
  from the Agent's rounded keys for every value whose `frac(log_γ)` is at least 0.5 (Datadog's own
  code documents the `+0.5` offset that reconciles them), the crate has no hook to apply that
  offset and no bin access, and re-binning at the edge would make the sketch relay a bounded-error
  normalization rather than lossless. Using the crate's `Config::new` was tried first and fails
  on the rounding alone.
- **Port the Agent's `Sketch.Quantile` interpolation.** Rejected: it can miss a single-bin
  population by `γ^1.5 - 1` (2.35%), it isn't what Datadog shows for a shipped sketch, and the
  bin-center estimator keeps the documented bound.
- **Encode through prost and compare decoded values rather than bytes.** Rejected: it would keep
  one protobuf path, but a value-level fixed point can't catch an encoder that writes two byte
  strings for one batch, and the other Datadog routes and every other lossless pair assert on
  bytes. Two hand-written encoders, each checked against prost's decoder, cost less than a
  weaker contract for these routes alone.
- **Decide trace readiness by batch provenance (`origin == datadog_in`).** Rejected in favor of
  the `_top_level` mark so a future transform that does the Agent's processing makes the same
  data ready by writing the same mark, with no change to `datadog_out`.
- **Block the connection on a full pipeline, as `otlp_in` and `prometheus_in` do.** No new
  `Fanout` method, and the precedent every other HTTP listener follows. Rejected: those listeners'
  clients wait for as long as the pipeline takes; an Agent gives up at 20 seconds and retries,
  so blocking buys nothing a `503` doesn't and holds a connection and an Agent worker for the
  whole wait. A plain timeout around `Fanout::send` was rejected with it: it sends consumer by
  consumer, so a deadline between two leaves the first holding a batch the Agent then resends.
- **Require `serializer_compressor_kind: gzip` on redirected Agents instead of a zstd decoder.**
  Rejected: it changes what Datadog receives too under dual-shipping, and it's one more edit per
  Agent in a migration whose point is that only the URL changes.

## Consequences

- `size_of::<DdSketch>()` is 128 (was 176); `Samples` at 168 now bounds `MetricKind` at the same
  176, so no event shrinks. Shrinking `Samples` is a perf-VM decision, not a free change
  ([`docs/design/memory.md`](../design/memory.md)).
- The relative error of a locally built sketch is 0.78% (was 1%); the native wire's `Distribution`
  bytes changed shape, which is fine pre-release; `sketches-ddsketch` is no longer a dependency.
- `Mapping::agent`'s constants are part of the Datadog wire contract: changing them breaks
  bin-for-bin relay silently, since the wire carries no parameters.
- A merge across mappings is bounded-error, not exact; it only happens when an operator
  aggregates a relayed stats sketch into a locally built one.
- The Agent-equivalent trace processor (normalization, `_top_level`, sampler tags, a stats
  concentrator) for the no-Agent topology is a follow-up plan, not this one; until it exists the
  tracer-direct topology runs through `datadog_trace_out` and a real Agent.
- Nine survey facts remain unverified against Datadog's closed backend and are settled by the
  plan's W7 against a trial org and a real Agent; whichever changes a decision here amends it.
- The hand-rolled `DdSketch` store measures +4.2% CPU on `aggregate` and +4.4% on `json-parse` on
  the perf VM, both sketch-heavy paths, and no measurable cost anywhere else
  ([`docs/design/performance.md`](../design/performance.md) §9). Accepted: bin-for-bin Datadog
  parity needs the Agent's own bin mapping, not a cheaper store that doesn't match it.
- `datadog_trace_in`'s `503` counts as loss (`logit.input.batches.dropped{reason="busy"}`), not
  the deferral `datadog_in`'s is (decision 11); the tracer short-timeout, no-retry behavior behind
  that is UNVERIFIED until W7.
