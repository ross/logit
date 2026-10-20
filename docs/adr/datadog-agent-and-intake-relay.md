---
created: 2026-09-23
updated: 2026-10-19
---

# Datadog: two lossless pairs, the Agent's own protocols, and a Datadog-mapped `DdSketch`

## Status
Accepted

Realized as of 2026-09-24: both pairs and the `statsd` additions this record decides are built,
and relay losslessly modulo the permitted normalizations `crates/logit-proto/src/datadog/mod.rs`
lists. [Verified destination behavior](#verified-destination-behavior) has the evidence, and
[`docs/known-gaps/datadog.md`](../known-gaps/datadog.md) what stays open.

## Context

`logit` sends nothing to Datadog except DogStatsD through `statsd_out`, and receives nothing
Datadog-shaped except DogStatsD through `statsd_in`. Migrating to or from Datadog needs `logit`
to stand in two places: in front of applications, where the Datadog Agent listens (DogStatsD on
`:8125`, the APM API on `:8126`), and behind a fleet of Agents, where Datadog's intake listens
(`dd_url`, `additional_endpoints` dual-shipping). Both are Datadog's own protocols, and the Agent
is open source, so the protocols are readable even where Datadog documents nothing.
[Verified destination behavior](#verified-destination-behavior) has the protocol survey,
[`docs/datadog.md`](../datadog.md) the trade-offs and the best-practice recommendation; this
record holds the decisions.

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
   `records.dropped{reason="needs_agent_processing"}`, so `datadog_trace_in` must not feed `datadog_out`
   directly. `otlp_out` remains the path for OTel-origin spans. A trial org accepts both routes
   from a sender that isn't an Agent: spans an Agent sent to `datadog_in` and `datadog_out`
   relayed arrived with their 128-bit ids, and the relayed stats populated `trace.*.hits`,
   `.errors`, and the duration distribution.

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

8. **Attribute vocabulary.** `_logit.datadog.*` for raw encodings (`_logit.datadog.type`,
   `_logit.datadog.interval`, `_logit.datadog.resources`, `_logit.datadog.source_type_name`,
   `_logit.datadog.origin.*`, `_logit.datadog.chunk.*`, `_logit.datadog.tracer.*`,
   `_logit.datadog.agent.*`, `_logit.datadog.stats.*`); Datadog's own OTLP-honored names for span
   fields (`service.name`, `resource.name`, `span.type`); `meta`/`metrics` keys verbatim;
   `_logit.statsd.event.*`/`_logit.statsd.service_check.*` for events and service checks, which
   are the same Datadog concepts DogStatsD carries. A Datadog `rate` is a `Gauge` with
   `_logit.datadog.type: rate`, not a `Sum`, because folding a per-second value into a delta
   multiplies and rounds.

9. **Trace ids** are built from a uint64 and `_dd.p.tid` on decode, and emitted as the low 64
   bits plus `_dd.p.tid` when the high bits are nonzero.

10. **`datadog_out` derives host, service, source, and tags from attributes and the resource**,
    never from per-sink fields; an upstream `set` supplies them. It drops and counts points older
    than Datadog's documented windows (1 h for metrics, 18 h for logs, 10 min for checks) before
    sending. The metrics window is the documented one and stricter than the intake
    ([Timestamp windows and what a `202` drops](#timestamp-windows-and-what-a-202-drops) has
    what the intake stored).

11. **`datadog_trace_in` shares decision 5's bounded-wait-then-`503` mechanism, but a `503` there
    is deferral for about 15 s, not minutes, and the bound is shorter.** Delivery is the same
    `Fanout::send_with_deadline` all-edges-or-nothing wait, under `BUSY_AFTER` at 2 s rather than
    5 s: a dd-trace tracer writes with a short timeout and gives up on a payload quickly.
    dd-trace-py 4.15.2, observed once (amendment below), retries a `503` five times, waiting 100,
    200, 400, 800, and 1,600 ms between attempts (3.1 s), then drops the payload. Each of its six
    attempts waits up to `BUSY_AFTER` for its `503`, so against this listener the window is
    6 × 2 s + 3.1 s, about 15 s. A stall shorter than that defers delivery; a longer one loses the
    payload, counted `logit.input.batches.dropped{reason="busy"}` rather than assumed recovered.
    The 2 s bound is sized to answer before the tracer gives up on its own, which would
    lose the payload the same way with nothing counted. The operator's lever against the loss is
    downstream capacity, not a retry: a `buffer:` (memory or disk) on the sinks behind this
    listener, sized to absorb a stall so the channel keeps draining. The
    `datadog_trace_in -> datadog_trace_out` pair's lossless-relay contract (decision 1) holds only
    while that channel drains; the counter makes a stall long enough to break it visible.

12. **`statsd_in` and `statsd_out` gain `transport: unix` and `transport: unix_stream`, the
    Agent's `dogstatsd_socket` and `dogstatsd_stream_socket`, with the socket path in the existing
    `bind`/`endpoint` field.** There's no separate `path:` field. One `statsd_in` listens on one
    socket, so an Agent's UDP port plus its socket is two components, as a TCP and a UDP listener
    already are; and a client names the socket the same way, one address under a scheme
    (`DD_DOGSTATSD_URL=unix:///var/run/datadog/dsd.socket`). Rule 65 requires an absolute path and
    rejects `tls:` under either Unix transport. The rest of the decision:
    - **`unix_stream` frames each packet as a 4-byte little-endian length, then one datagram's
      worth of newline-separated lines**: what the Agent's `pkg/dogstatsd/listeners/uds_stream.go`
      reads and `datadog-go`'s stream writer sends. It's a separate framing mode from carbon's
      big-endian `LengthPrefixed`, not a flag on it. Recorded from the `datadog` Python client, and
      accepted by a real Agent 7.83 from `statsd_out` (amendment below).
    - **`statsd_in` and `datadog_trace_in` make their sockets mode `0722`**, the Agent's own mode
      for its DogStatsD and APM sockets (both recorded, amendment below). It's enough because a
      datagram sender or a stream client needs only write permission on the socket file; the
      directory's permissions are the access control.
    - **`statsd_out`'s `unix` sender connects its datagram socket to the path**, as `datadog-go`
      does with `net.Dial("unixgram", path)`, rather than calling `send_to(path)` on an unbound
      socket. Linux parks a sender on a full receiver queue, and wakes it when the receiver
      drains, only when the sender is connected to that receiver. An unconnected sender is
      reported writable again right after each `EAGAIN`, so behind other clients' datagrams (the
      usual DogStatsD fan-in) it retries in a busy loop until the queue has room. A connected
      socket follows the receiver's socket, not the path, so the sink drops it on a send timeout,
      `ECONNREFUSED`, or `ENOTCONN`, and the next send connects to whatever is at the path then.
      The first datagram of a batch gets one immediate reconnect-and-retry on `ECONNREFUSED` or
      `ENOTCONN`, as the stream transports get one reconnect: nothing of the batch has left, so a
      receiver restart between batches costs no batch and no duplicate. Each connect after the
      first counts `logit.output.reconnects`.

13. **`datadog_trace_out` speaks the tracer API to one Agent, over TCP or the Agent's Unix
    socket, and restores the tracer's request headers.** It sends `PUT /v0.4/traces` (or
    `/v0.7/traces` under `version: v0.7`) and `POST /v0.6/stats`, msgpack, with no sampler: the
    Agent's `rate_by_service` reply is ignored. The rest of the decision:
    - **v0.4 is the default form**, because most tracers send it and every Agent takes it. The
      tracer carriers a v0.4 request keeps only in headers (`_logit.datadog.tracer.language_name`
      and the rest `datadog_trace_in` reads) go back out as those headers under either form, so a
      v0.4-origin batch relays with nothing lost; a tracer header's carrier is never written into a
      span's `meta`, on any trace form. Under v0.4 the chunk carriers and the payload carriers no
      header has are dropped and counted `no_wire_form`; v0.7 carries them.
    - **`socket:` is an HTTP/1.1 client over the Unix socket**, a pooled `hyper_util` client whose
      connector dials the path for each new connection, beside `reqwest` for `endpoint:`; the two
      share request building and fault classification. A missing socket file or a refused connect
      is `Fault::Clean`, as a refused TCP connect is. A real Agent 7.83's socket accepted it
      (amendment below), and the trial-org run's Agent 7.83.3 took traces and `/v0.6/stats` alike
      through it.
    - **Requests are cut by trace**, at most 1,000 traces and 25 MiB (the Agent's
      `max_request_bytes`) on the wire, through `datadog_out`'s splitter. The Agent has no count
      limit; the 1,000 bounds one request's encode and send.
    - **`duplicate_safe()` is `false`**: an Agent dedupes nothing, and one batch is up to two
      requests.

14. **`datadog_out` sends events uncompressed and stays not duplicate-safe** (amendment,
    2026-09-24, from the trial-org run). Datadog's `/api/v1/events` answers any
    gzip or deflate body `400 Invalid JSON structure`, so that route goes out with no
    `Content-Encoding` whatever `compression:` says. The intake stores a resent series point once
    (the last write wins at its `(series, timestamp)`) but a resent log twice, and a batch is
    several requests, so `duplicate_safe()` stays `false`. And `datadog_in` decodes a JSON metrics
    body with no `series` member as an empty request: the Agent posts `{}` to each route when it
    starts, and Datadog answers that `202`.

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
- A merge across mappings is bounded-error, not exact; it only happens when a relayed stats
  sketch shares a series with a locally built one. `aggregate` passes APM stats through whole
  ([ADR `aggregation-window-semantics`](aggregation-window-semantics.md)'s 2026-10-19
  amendment), so that takes a stage upstream that removes the stats marker as well.
- The Agent-equivalent trace processor (normalization, `_top_level`, sampler tags, a stats
  concentrator) for the no-Agent topology is a follow-up plan, not this one; until it exists the
  tracer-direct topology runs through `datadog_trace_out` and a real Agent.
- The recorded corpus settled the survey facts a real Agent and tracer could (amendment below),
  and the trial-org run the ones that needed Datadog's backend: sketches, distribution points (gzip or zlib, not raw
  deflate), `/api/v0.2/traces` and `/api/v0.2/stats` from a sender that isn't an Agent, the
  series size cap, deduplication, and the stale windows. One changed a decision (decision 14).
  Whether the intake accepts a stats sketch whose gamma isn't 1.0202 stays open, because nothing
  in `logit` sends one yet.
- The hand-rolled `DdSketch` store measures +4.2% CPU on `aggregate` and +4.4% on `json-parse` on
  the perf VM, both sketch-heavy paths, and no measurable cost anywhere else
  ([`docs/design/performance.md`](../design/performance.md) §9). Accepted: bin-for-bin Datadog
  parity needs the Agent's own bin mapping, not a cheaper store that doesn't match it.
- `datadog_trace_in`'s `503` is deferral only across a tracer's retries, and loss after them
  (`logit.input.batches.dropped{reason="busy"}`), not the minutes-long deferral `datadog_in`'s is.
  Decision 11 gives the window.
- A `unix` `statsd_out` whose receiver restarts mid-batch fails that batch under the sink's usual
  rules; only a restart between batches is absorbed.

## Verified destination behavior

What Datadog accepts and emits, surveyed on 2026-09-23 from Datadog's OpenAPI specs
(`github.com/DataDog/datadog-api-client-go/.generator/schemas/{v1,v2}/openapi.yaml`), the
`datadog-agent` and `agent-payload` sources on `main`, and `docs.datadoghq.com`. Recorded traffic
from Agent 7.83.3 and dd-trace-py 4.15.2 ([`testdata/interop/datadog/`](../../testdata/interop/datadog/README.md))
and two trial-org runs, on 2026-09-24 and 2026-10-08, then checked it; a "trial org" note below
names what a run measured. An item marked UNVERIFIED wasn't confirmed by a current official page,
the corpus, or a trial org.

### Public intake API (no Agent)

| Endpoint | Body and limits | Notes |
|---|---|---|
| `POST api.<site>/api/v2/series` | JSON `{series:[{metric, type, points:[{timestamp s, value}], interval, unit, tags[], resources:[{name,type}], source_type_name, metadata.origin}]}`; 512,000 B compressed, 5,242,880 B decompressed; `Content-Encoding: deflate \| zstd1 \| gzip`; header `DD-API-KEY` | `type`: 0 unspecified, 1 count, 2 rate, 3 gauge. No histogram, set, or distribution type. Points must be no more than 1 h in the past or 10 min in the future ([Timestamp windows and what a `202` drops](#timestamp-windows-and-what-a-202-drops)). A series with more than 100 tags is dropped the same way. A body over 512 kB compressed gets `413`. A resent point overwrites: one value per `(series, timestamp)`, the last write winning (trial org) |
| `POST /api/v1/distribution_points` | JSON `{series:[{metric, host, tags, type:"distribution", points:[[ts,[v…]]]}]}`; `deflate` only | Raw values; Datadog sketches them server side. Limits undocumented; the trial org took 1,052,533 B gzip (150,000 values) with `202` and every value counted. gzip and zlib deflate are both accepted, raw deflate is `400` |
| `POST /api/beta/sketches` | protobuf `SketchPayload` | What Agents send. Not in the public spec; Vector's `datadog_metrics` sink sends it with an API key, and so does `datadog_out`: in the trial org, `aggregate`'s locally built sketches are queryable (`avg`, `count`, `max`) |
| `POST http-intake.logs.<site>/api/v2/logs` | JSON array of `{message, ddsource, ddtags, hostname, service, status, …}`; 1,000 entries, 5 MB decompressed, 1 MB per log (cut, still 2xx); up to 18 h in the past; `gzip`/`deflate`/`identity`; 202 accepted, retry 408/429/500/503 | Other keys are attributes, nested maps included. Trace correlation auto-detects OTel `trace_id`/`span_id` (32-/16-char lowercase hex) and Datadog `dd.trace_id`/`dd.span_id` (decimal). A resent log is stored twice (trial org) |
| `POST /api/v1/events` | `title`, `text` (≤4,000 chars), `date_happened` (≤18 h old), `priority: normal \| low`, `alert_type`, `aggregation_key` (≤100), `host`, `tags`, `source_type_name`; uncompressed only (trial org: gzip and deflate get `400 Invalid JSON structure`) | v2 events (`event-management-intake.<site>`) are a different product surface; v1 is what DogStatsD events map to |
| `POST /api/v1/check_run` | array of `{check, host_name, status 0–3, tags, message?, timestamp?}`; ≤10 min old; message cut at 500 chars, discarded on OK | |
| Traces | No documented native-span API, but the Agent's own outbound protocol is open source (`pkg/trace/writer`) and Vector's `datadog_traces` sink is a working third-party sender of it: see [Agent outbound surfaces](#agent-outbound-surfaces-what-agents-send-to-datadog) | Trace metrics (`trace.*`, service pages) are not derived from `/api/v0.2/traces`; the sender must also send `/api/v0.2/stats` (inferred from `metrics_namespace` docs and the Agent's concentrator; the backend is closed source) |
| `POST otlp.<site>/v1/{traces,metrics,logs}` | OTLP/HTTP protobuf (traces and metrics also JSON); header `dd-api-key`; 512 KiB compressed metrics, 5.1 MiB logs, 15 MiB traces; 413 when over | HTTP only, no gRPC. Metrics must be delta temporality; cumulative is an error. Optional `dd-otel-metric-config` (`histograms.mode`, `summaries.mode`, `resource_attributes_as_tags`) and `dd-otel-span-mapping` headers; `compute_stats=true` to get trace metrics. No Preview banner on any of the three pages, no explicit GA statement either |

Sites: `datadoghq.com`, `datadoghq.eu`, `us3.datadoghq.com`, `us5.datadoghq.com`,
`ap1.datadoghq.com`, `ap2.datadoghq.com`, `uk1.datadoghq.com`, `ddog-gov.com`,
`us2.ddog-gov.com`; every host above is `<prefix>.<site>`.

### Agent listening surfaces (what apps send to an Agent)

- **DogStatsD**: UDP `:8125`, a datagram Unix socket at `dogstatsd_socket`
  (default `/var/run/datadog/dsd.socket`; what Kubernetes clients use), and an optional stream
  Unix socket (`dogstatsd_stream_socket`; a 4-byte little-endian length per packet, recorded in
  `testdata/interop/datadog/README.md`). Buffer 8,192 B. Grammar as
  [`telemetry-landscape.md`](../design/telemetry-landscape.md)'s DogStatsD section, plus two
  suffixes: `|e:<external data>` (v1.5, Agent 7.57+) and `|card:none|low|orchestrator|high`
  (v1.6, 7.64+). Agent semantics: `c` is time-normalized into a rate over the 10 s flush; `s`
  becomes a gauge of distinct values; `h`/`ms` become
  `.avg`/`.count`/`.median`/`.95percentile`/`.max` per `histogram_aggregates` and
  `histogram_percentiles`; `d` goes to Datadog as a sketch.
- **APM on `:8126`** (`pkg/trace/api/endpoints.go`): `/v0.3/traces` (msgpack or JSON),
  `/v0.4/traces` (msgpack, array of arrays of span maps), `/v0.5/traces` (msgpack, a string table
  plus 12-element span arrays), `/v0.7/traces` (msgpack `TracerPayload`, not protobuf),
  `/v1.0/traces` (msgpack, streaming string table), `/info`, `/v0.6/stats`,
  `/v0.1/pipeline_stats`, `/telemetry/proxy/`, `/v0.7/config`, `/evp_proxy/v1`–`v4`,
  `/profiling/v1/input`, `/debugger/*`, `/tracer_flare/v1`, and an optional
  `receiver_socket` (`/var/run/datadog/apm.socket`). Response `{"rate_by_service": {...}}`.
  Request headers: `X-Datadog-Trace-Count`, `Datadog-Meta-Lang`, `Datadog-Meta-Tracer-Version`,
  `Datadog-Container-ID`, `Datadog-Entity-ID`, `Datadog-External-Env`, `Datadog-Client-Computed-Stats`,
  `Datadog-Client-Computed-Top-Level`, `Datadog-Client-Dropped-P0-{Traces,Spans}`. Span schema
  (`span.proto`): `service`, `name`, `resource`, `type` strings; `trace_id`, `span_id`,
  `parent_id` uint64; `start`, `duration` int64 ns; `error` int32; `meta` string→string;
  `metrics` string→f64; `meta_struct` string→bytes; `span_links`; `span_events`. 128-bit trace
  ids carry their high 64 bits as 16 lowercase hex chars in `meta["_dd.p.tid"]` on the first
  span of a chunk. Limits: `service`/`name` ≤100 chars, `resource` ≤5,000, meta key ≤200, meta
  value ≤25,000.
- **OTLP receiver**: gRPC `:4317`, HTTP `:4318`; `metrics` and `traces` enabled, `logs`
  disabled by default. Metric options: `histograms.mode: distributions | counters | nobuckets`,
  `sums.cumulative_monotonic_mode: to_delta | raw_value`, `summaries.mode: gauges | noquantiles`,
  `resource_attributes_as_tags` (false), `instrumentation_scope_metadata_as_tags` (true). Span
  mapping honors the attributes `operation.name`, `resource.name`, `span.type`, and
  `service.name`; without them it derives operation and resource names from semconv.
- **dd-trace SDKs can export OTLP** (Preview): `DD_TRACE_OTEL_ENABLED=true` with
  `OTEL_TRACES_EXPORTER=otlp` (Java 1.62+, Python 4.8+, Node 5.98+, Go 2.8+, .NET 3.41+) and
  `DD_METRICS_OTEL_ENABLED=true`. Spans keep Datadog semantics.
- **Logs**: a `logs` integration `type: tcp | udp` listener taking raw, JSON, or syslog lines,
  one per `\n`. No HTTP logs endpoint other than OTLP (UNVERIFIED as a negative).

### Agent outbound surfaces (what Agents send to Datadog)

Redirected by `dd_url` (metrics, events, checks, metadata only), `logs_config.logs_dd_url`,
`apm_config.apm_dd_url`, or dual-shipped with `additional_endpoints`,
`logs_config.additional_endpoints`, and `apm_config.additional_endpoints`.

| Route | Body | Notes |
|---|---|---|
| `/api/v2/series` | protobuf `MetricPayload` (`agent-payload` `metrics/agent_payload.proto`); `Content-Type: application/x-protobuf` | zstd level 1 by default (`serializer_compressor_kind`), one setting for every endpoint. 512,000 B compressed, 5 MiB decompressed, 10,000 points. v1 JSON only if `use_v2_api.series: false`. v3 columnar (`/api/intake/metrics/v3/series`) only to Datadog URLs (`use_v3_api.series.enabled: datadog_only`); a recorded 7.83 Agent redirected elsewhere sent v2, and the trial org's Agent 7.83.3, with `dd_url` at a `datadog_in`, sent v2 protobuf only |
| `/api/beta/sketches` | protobuf `SketchPayload`: `{metric, host, tags, dogsketches:[{ts, cnt, min, max, avg, sum, k: sint32[], n: uint32[]}]}` | DDSketch with `eps = 1/128` (gamma 1.015625), `min = 1e-9`, bias, 4,096-bin collapsing, int16 keys (`pkg/util/quantile`). No mapping parameters on the wire: the receiver assumes them |
| `/api/v1/check_run` | JSON | |
| `/intake/` | JSON: events and host metadata in one route | |
| `agent-http-intake.logs.<site>/api/v2/logs` | JSON array, zstd (the Agent's one compression setting; the recorded corpus shows `content-encoding: zstd`); ≤1 MB and ≤1,000 logs per batch; headers `DD-API-KEY`, `DD-PROTOCOL: agent-json`, `DD-EVP-ORIGIN: agent`, `dd-message-timestamp` | Legacy TCP `agent-intake.logs.<site>:10516`: `<api-key> <json>\n`, or 4-byte length-prefixed protobuf `Log` |
| `trace.agent.<site>/api/v0.2/traces` | protobuf `AgentPayload{hostName, env, tracerPayloads, tags, agentVersion, targetTPS, errorTPS, rareSamplerEnabled}`; headers `DD-Api-Key`, `Content-Type: application/x-protobuf`, `Content-Encoding` (zstd `BestSpeed` by default; gzip in other builds), `User-Agent: Datadog Trace Agent/<ver>/<commit>`, `X-Datadog-Reported-Languages`; 3,200,000 B uncompressed max; flush every 5 s; retries 408, 429, 5xx, and transport errors with full-jitter backoff, 4 retries (`pkg/trace/writer/{trace,sender}.go`) | Vector sends the same payload with `DD-API-KEY` and gzip only, empty `tags`, and its own `hostName`/`env`/`agentVersion` (`src/sinks/datadog/traces/request_builder.rs`) |
| `trace.agent.<site>/api/v0.2/stats` | msgpack + gzip `StatsPayload{agentHostname, agentEnv, agentVersion, clientComputed, stats: [ClientStatsPayload{hostname, env, version, containerID, lang, …, stats: [bucket{start, duration, stats: [group{service, name, resource, type, spanKind, HTTPStatusCode, synthetics, peerTags, hits, errors, duration, topLevelHits, okSummary, errorSummary}]}]}]}`; ≤4,000 groups per payload; 10 s buckets | `okSummary`/`errorSummary` are protobuf DDSketches from `sketches-go` (`LogCollapsingLowestDenseDDSketch(0.01, 2048)`, gamma 1.0202) and carry their mapping on the wire, unlike the metrics sketches. The Agent computes them from every span before sampling (`pkg/trace/stats/concentrator.go`): eligible spans are `_top_level == 1`, `_dd.measured == 1`, or server/client/producer/consumer `span.kind`; weight `1/_sample_rate` |
| `/api/v1/validate`, `/api/v2/host_metadata`, `/api/v1/metadata` | | Answered 200/204, contents dropped and counted |

Vector's `datadog_agent` source (`src/sources/datadog_agent`) implements the same set minus
`/api/v1/check_run` and `/intake/`. Vector drops `/api/v0.2/stats` on receipt and recomputes
stats in its sink, a partial reimplementation of the Agent's concentrator (no `span.kind`, peer
tags, or gRPC/HTTP keys; "beta"; requires Agent sampling to be off). `logit` relays the stats
instead (decision 2). What the Agent does to a span between the tracer and the intake is in
[`docs/known-gaps/datadog.md`](../known-gaps/datadog.md)'s Agent-equivalent trace processor
entry.

### Timestamp windows and what a `202` drops

`datadog_out`'s 1 h series window (decision 10) is the documented one, and stricter than the
intake: the 2026-09-24 trial-org run stored gauge points 2 h and 3 h old, and none 6 h or older.
A point more than 10 min ahead was dropped alone, with `202` and an `errors` entry naming it,
while the rest of its request was stored.

The 2026-10-08 run measured what the intake drops from a request it answers `202`:

- **Series, over protobuf** (what `datadog_out` sends): always `202` with
  `Content-Type: application/x-protobuf`. A clean request's body is empty. Otherwise it's a
  protobuf whose field 1, `repeated string errors`, holds one entry per dropped series, naming the
  metric and never a tag: `Payload validation failed: too many tags in series <metric>: limit=100
  tags=101`, or `Payload validation failed: series <metric> contains N data points too far in the
  future`, where N counts that series' future points and a series with other valid points keeps
  them. Over JSON the same strings come as `{"errors": [...]}`.
- **The tag limit** counts only the `tags` strings, repeats included; `resources` (host, device,
  and others) don't count. 100 tags were stored and 101 dropped.
- **Distribution points and sketches** drop a series with more than 100 tags, or a point too far
  in the future, silently, answering `202 {"status": "ok"}`. Distribution points answer
  `400 Payload is empty` when every series in the request was dropped.
- **Check runs and logs** with 101 tags drew `202` (`{"status": "ok"}` and `{}`); whether either
  stored them wasn't checked.
- **A point 2 h old** drew no `errors` entry.

"Amendment: what an accepted request drops (2026-10-08)" has what `datadog_out` does with it.

## Amendment: what W7a's recorded traffic settled

`testdata/interop/datadog/` holds traffic from a real Agent 7.83, dd-trace-py 4.15, and the
`datadog` DogStatsD client 0.54; its README lists each finding with the file that proves it. The
ones that bear on these decisions:

- **Decision 11:** the tracer retries a `503` five times, then drops the payload. This was one
  observation, not a recorded fixture (the corpus README's "What this settled" describes it).
  The mechanism stands; decision 11 changed from "no retry" to the retry window it now states.
- **Decision 12:** the stream socket is 4-byte little-endian length-prefixed, as decided. Both of
  the Agent's sockets are `0722`, so `datadog_trace_in` changed from `0666` to `0722`. And the
  Python client writes a service check's `c:` and `card:` after `m:`, which the Agent reads by
  ending `m:` at the next `|`; `statsd_in` changed to read it the same way, so a service-check
  message can no longer carry `|`, and `statsd_out` substitutes `_` for one.
- **Decision 13:** a real Agent's `receiver_socket` took `datadog_trace_out`'s requests.
- **`datadog_trace_in`'s `/info`:** libdatadog, under current tracers, rejects the whole document
  over one field of the wrong JSON type, and did over `"redis": false`. The document now has the
  recorded Agent's field types, held by a test. With `client_drop_p0s: false`, such a tracer
  computes no client stats, so the downstream Agent computes them. `/v0.6/stats` now takes a
  tracer's language, version, and container id from headers where the payload leaves them empty,
  as the Agent does, and `Datadog-External-Env` is a tracer header carrier.
- **`datadog_in`:** the Agent's key check sends its key only as `?api_key=`, and it probes
  `/api/v2/validate` and `/_health` too; all three are served, and a probe reads the query key.
  The logs sender's `{}` connectivity check is no log rather than a skipped one.

The survey facts about Datadog's backend (sketches and traces from a sender that isn't an Agent,
zlib on distribution points, size limits, dedupe, URL classification for v3 series) are the
trial-org run's; Consequences and decision 14 have what it settled.

## Amendment: the default delivery posture is `at_least_once` (2026-09-30)

`Output::duplicate_safe()` is gone, and `at_least_once` is every sink's default posture. `datadog_out` and `datadog_trace_out` now retry an `Ambiguous` fault for up to `buffer.retry_budget` by default, and a resend adds to the kinds Datadog aggregates (distribution points, sketches, APM stats) with no upstream remedy. The statements above that `duplicate_safe()` is `false` describe the earlier default; `buffer.delivery: at_most_once` restores it. See [`delivery-semantics.md`](delivery-semantics.md) item 5.

## Amendment: response classes (2026-10-04)

Under [ADR `sink-fault-classes`](sink-fault-classes.md), each sink records how it classifies a
destination's answers. The Datadog sinks' tables are the "Faults, retries, and duplicate safety"
sections of `crates/logit-outputs/src/datadog.rs` and `crates/logit-outputs/src/datadog_trace.rs`.
The decisions:

- **`datadog_out`.** A `401` or `403` is `Refused`: the routes share one org-wide key, so every
  request would get it. A `404` is `Refused` too: it names a route the configured `site` or
  `endpoints:` base doesn't serve, which every batch would hit, and the Agent's forwarder
  reschedules a `404` rather than drop it. A `405` or `407` is `Refused` (the path to the intake).
  A `400`, a `413`, and any other `4xx` are `Rejected`, as the Agent drops `400` and `413`; `408`,
  `429`, and any `5xx`, `501` included, are `Ambiguous`.
- **`datadog_trace_out`.** A `404` is `Refused`: the Agent doesn't serve the sink's `version:`
  route, so every batch gets it (a tracer answers the same `404` by downgrading its API version).
  A `415` is `Refused` for the same reason: the Agent doesn't take that version's body format. A
  `401`, `403`, `405`, or `407` is `Refused`: the Agent has no credential, so a proxy or a wrong
  path answered. An Agent's `429` is `Clean`, because
  the receiver turns the payload away before reading it ("trace-agent is overwhelmed, a payload has
  been rejected"). A `400` and a `413` are `Rejected`.
- **A route refused after another was accepted holds the batch and resends only the unsettled
  requests.** The refusal keeps its own class under both postures, and each retry sends the refused
  route's requests alone ([ADR `sink-fault-classes`](sink-fault-classes.md), "Amendment: the
  Datadog sinks retry per request (2026-10-09)"). Reading the refused route as `Rejected` instead
  would drop that route's records from every batch until the operator fixed the key, the base URL,
  or the `version:`, the loss `Refused` exists to prevent.

## Amendment: the socket mode is configurable (2026-10-05)

Decision 12's mode is now each listener's default rather than a fixed value. `statsd_in`,
`lines_in`, and `datadog_trace_in` take an optional `socket_mode:`, a quoted octal string such as
`"0660"`, and omitting it keeps `0722`, the Agent's own mode. An operator whose clients share a
group, or who wants the socket closed to other users without a dedicated directory, sets it
instead of changing the directory.

- **The value is a quoted string of three octal digits, with an optional leading `0`.** A YAML
  number is rejected with a hint to quote it, because YAML 1.1 reads `0660` as an octal number and
  YAML 1.2 as a string, so only a string means the same mode to every tool that reads the file.
- **Permission bits only.** Setuid, setgid, and the sticky bit mean nothing on a socket file, so a
  value above `0777` is rejected when the config is parsed.
- **Graph rule 78 rejects `socket_mode` where there's no socket file**: a `statsd_in` or `lines_in`
  under `transport: tcp` or `udp`, and a `datadog_trace_in` without `socket`.

## Amendment: what an accepted request drops (2026-10-08)

The intake drops part of a request it answers `202`. A trial-org run on 2026-10-08 measured two
causes on the metrics routes: a series, distribution point, or sketch with more than 100 `tags`
strings (repeats counted, `resources` not), and a point more than 10 minutes in the future. The
series route names each dropped series in its `202` body, a protobuf `repeated string errors`; the
distribution-points and sketches routes drop silently and answer `202 {"status": "ok"}`. The run's
check and logs requests drew `202` with no sign of a drop, so what those routes drop is unmeasured.
`datadog_out` now handles both:

- **A measured destination limit the sink can check for itself is enforced before sending.** The
  sink drops a metrics record whose rendered `tags` list is over 100 strings and counts it
  `records.dropped{reason="too_many_tags"}`, once per batch. On two of the three routes this is
  the only count of the loss anyone gets. The cost is that the sink copies Datadog's limit: if
  Datadog raises it, the sink drops records Datadog would store
  ([`docs/known-gaps/datadog.md`](../known-gaps/datadog.md)). The future-point limit already had
  this treatment as part of the stale filter.
- **A series `2xx` body's `errors` count as rejected records, and the verdict stays `Ok`.** Each
  entry counts one `records.rejected{route="series"}`, at most the request's entries, and comes
  out of `records`. The rest of the request was stored, so a retry would resend it, and a named
  series over the tag limit would be dropped again: the response class table's `2xx` row stays
  `Ok`, and the named series are counted, not retried. This
  is the backstop for what the sink can't predict, such as a sender's clock running ahead of the
  intake's. It follows `otlp_out`'s partial success
  ([ADR `sink-fault-classes`](sink-fault-classes.md)), and counts once per batch for the request that drew it, like the other
  answers a destination gives.

## Amendment: loss counter names (2026-10-13)

[ADR `telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md) renames the counters this record
names, so that any nonzero `*.dropped` is loss. The decisions above stand; only the names change:

- **A busy `503` is `logit.input.batches.deferred`**, untagged, on both listeners, in place of
  `logit.input.batches.dropped{reason="busy"}`. The Agent keeps the payload and retries it.
  Decision 11 still holds for `datadog_trace_in`: a tracer gives up after about 15 s, so a
  sustained deferred rate there is loss the counter can't tell from deferral.
- **A series named in a `202` body is
  `logit.output.records.dropped{route="series", reason="rejected"}`**, in place of
  `records.rejected{route="series"}`. It still comes out of `records`, and a request is either
  answered `2xx` or rejected, so no entry counts under both.
- **The codec's malformed and unsupported input is `dropped`.** A bad series, point, sketch,
  check, event, log, span, or stats element, a legacy `distributions` entry, an
  `idxTracerPayloads` entry, and an interpolated stats summary count `logit.input.*.dropped`.
  Nothing to carry (a `null` value, an empty distribution, sketch, or stats bucket) stays
  `skipped`.
- **On encode,** an unresolved `GaugeDelta` is
  `logit.output.metrics.dropped{reason="unresolved_gauge_delta"}`; a non-finite value, an
  oversized sketch, and a check with no valid status are `metrics.dropped`; and a stats record of
  no known shape is `logit.output.stats.dropped{reason="unrecognized_record"}`. A kind the series
  route has no form for stays `metrics.skipped{metric_kind}`. A tag left off a delivered record is
  `logit.output.tags.omitted`, every reason unchanged.

`crates/logit-proto/src/datadog/mod.rs`'s module doc has every counter, and
[internal-telemetry.md](../design/internal-telemetry.md) has the operator-facing tables.

## Amendment: the carriers are private (2026-10-13)

[ADR `private-attribute-namespace`](private-attribute-namespace.md) moves every `datadog.*`
carrier this record names under the private `_logit.` prefix, protocol segment kept:
`_logit.datadog.interval`, `_logit.datadog.tracer.*`, `_logit.datadog.chunk.*`, and the rest. The
decisions above use the new spelling. Four things follow for the Datadog pairs:

- **Datadog's own names stay public.** `_dd.*`, `_top_level`, and `_sampling_priority_v1` are
  span data a tracer or the Agent wrote, and Datadog shows them; they aren't carriers. The APM
  stats metric names (`datadog.stats.hits` and the rest) are metric names, not attributes.
- **Every other sink ignores the carriers.** A `datadog_trace_in -> otlp_out` pipeline sends no
  tracer header as an OTLP resource attribute, and a `datadog_in -> influxdb_out` pipeline writes
  no `_logit.datadog.interval` tag.
- **The Datadog sinks follow the same rule.** `datadog_out` and `datadog_trace_out` read their own
  carriers by exact name, and skip every other `_logit.` key when they render tags, log fields,
  or span `meta` and `metrics`, without counting it. That includes their own carriers a route
  doesn't read: a stray `_logit.datadog.interval` on a log never becomes a log field, and the
  v0.6 stats route, which has no envelope, skips the Agent carriers uncounted.
- **A tag can't spell a carrier.** `datadog_in` drops a series tag under `_logit.` and counts it
  `logit.input.tags.omitted{reason="reserved_prefix"}`, so a `_logit.datadog.type:rate` tag never
  turns a `GAUGE` into a `RATE`. An operator who wants a carrier on a series from another
  protocol stamps it with `set`, as `fixtures/datadog-direct.yaml` stamps
  `_logit.datadog.interval`.

## Amendment: the process hostname fills a missing host (2026-10-14)

`datadog_out` writes the process hostname ([ADR `process-hostname`](process-hostname.md)) as the
host of a series, distribution point, sketch, log, event, or service check that carries none of
its own. Decision 10 stands: there's no per-sink host field, and a host the event or its resource
carries always wins. Trace and stats payloads keep their tracer and Agent hostnames verbatim, and
`datadog_trace_out` is unchanged.

- **A DogStatsD `host:` tag is a host.** `statsd_in` decodes `#host:web1` to the attribute
  `host`. With no `host.name`, or an empty one, a `Str` `host` is the host and isn't sent as a tag, as the Agent's
  `extractTagsMetadata` (datadog-agent 7.83.3, `comp/dogstatsd/server/impl/enrich.go`) reads it.
  An empty one marks a sample hostless, and it is never filled. With `host.name`
  present, `host` stays an ordinary tag.
- **Events and service checks read `host.name` and `host`.** Their host is `_logit.statsd.event.host` or
  `_logit.statsd.service_check.host` (the `h:` field), else `host.name`, else `host`, else the process
  hostname. A `host.name` or `host` used this way leaves as the host and not as a tag.
- **A log with a host Datadog reads isn't filled.** Datadog's log preprocessing takes the host
  from `host`, `hostname`, or `syslog.hostname`. A log carrying `hostname`, `host.name`,
  `syslog.hostname`, or `host` gets no default `hostname`, and the last two leave as the
  attributes they were.
- **An Agent's hostless sample stays hostless.** On v2 the Agent writes a hostless series as a
  `host` resource with no name, which decodes into `_logit.datadog.resources`. A series carrying
  such an entry is never filled, and the entry is kept, so the relay stays a fixed point. The v1,
  distribution-point, and sketch routes spell the same thing `"host":""`, which decodes to the
  private carrier `_logit.datadog.hostless` (`Bool(true)`, an event attribute like the route's
  other carriers), distinct from an absent `host`. A sample carrying it is never filled. It
  leaves as `""` on those three routes and as a `host` resource with no name on v2, as does a
  sample with an empty `host:` tag, so a later hop keeps it hostless too. To keep an absent
  `host` absent, the v1 and distribution-point encoders omit `host` when there's no host, no
  hostless mark, and no default host, or when `host.name` isn't a string, which is never
  replaced.
- **An empty sketch host is hostless.** Protobuf can't tell an empty sketch `host` from an
  absent one, so `logit` reads every empty one as hostless. That's what it means from every
  sender found, in datadog-agent 7.83.3:
  - DogStatsD sets a sample's host to the Agent's hostname unless it has a `host:` tag
    (`comp/dogstatsd/server/impl/enrich.go`, `extractTagsMetadata`), and clears it in serverless
    mode (`enrichMetricSample`).
  - A check's empty hostname gets the Agent's unless the check turned that off, as
    `empty_default_hostname: true` and the KSM check do
    (`pkg/aggregator/sender.go`, `sendMetricSample`; `pkg/collector/corechecks/checkbase.go`).
  - The aggregator copies the sample's host into the sketch without defaulting
    (`pkg/aggregator/time_sampler.go`, `newSketchSeries`).
  - The Agent's own hostname is empty only in sidecar mode (Fargate or managed instances),
    where "the task/pod is the unit of identity, not the host" (`pkg/util/hostname/common.go`,
    `fromFargate`). It exits at startup when it can't resolve one
    (`cmd/agent/subcommands/run/command.go`).

  The OpenTelemetry Collector's Datadog exporter sends an empty host for a source that isn't a
  host, such as an ECS Fargate task (`pkg/opentelemetry-mapping-go/otlp/metrics`,
  `metrics_translator.go`). Filling any of these with the relay's hostname would invent a host
  the sender declined to name. What the intake does with an empty sketch host isn't documented.
- **One `logit` is one Datadog host for hostless senders.** A metric with no `host:` tag, or an
  event or check with no `h:` field and no `host:` tag, relayed through `logit` reports under the
  relay's hostname, as it would through one Agent. Datadog bills per host, so this matches what
  an Agent costs. To keep a host per sender, stamp `host.name` upstream with `set`.

A relayed item that arrived with no host now leaves with the relay's hostname, which joins both
of the codec's permitted-normalization lists (`crates/logit-proto/src/datadog/mod.rs`).

## Amendment: `statsd_out` writes the DogStatsD host (2026-10-19)

`statsd_out` under `format: dogstatsd` now writes the host as an Agent reads it, so a sample sent
through a local Agent keeps its host. With no `host` attribute and no non-empty `h:` field, a non-empty
`host.name` leaves as the `host:` tag, and `_logit.datadog.hostless` as an empty `host:` tag. A
`host` attribute wins over `host.name` there, the reverse of `datadog_out`'s order above.
[ADR `statsd-output`](statsd-output.md)'s amendment of the same date records the rule and why.
