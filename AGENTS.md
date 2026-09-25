# AGENTS.md

Guidance for AI coding agents working in this repo. Humans: see [README.md](README.md).

## What this is

`logit` is a logging/metrics/tracing multiplexer written in Rust, with user transforms in
LuaJIT. Read these in order before changing anything:

1. [docs/OVERVIEW.md](docs/OVERVIEW.md) (~1 page): scope and positioning.
2. [docs/adr/](docs/adr): *why* the stack is what it is.
3. [docs/design/](docs/design): the internal event model, the Lua scripting API, the pipeline
   component graph, the native wire protocol, and internal telemetry. These five design docs are
   load-bearing; don't improvise around them without reading them first.
4. [docs/known-gaps.md](docs/known-gaps.md): check it before "fixing" something that looks
   broken. It's likely a documented, deliberate gap, not an oversight.

[docs/deploying.md](docs/deploying.md) is the operator-facing doc for running any of this outside
the dev stack.

### Invariants a reviewer checks first

These are easy to break in a routine-looking change, and each one is the reason its component
exists or a contract other `logit` processes depend on:

- **`shape` emits counts and lengths only**: never an observed key, value, body, or metric name,
  in a metric, tag, diagnostic, or telemetry point. That property is the point of the component,
  and it's what a reviewer checks first on any change to it.
- **`sample`'s hash is a frozen cross-version contract.** XXH64 seed 0 (`twox-hash`) over a fixed
  canonical byte form, in `logit_core::sampling`, pinned by test vectors. Changing it is a
  wire-breaking change that needs its own ADR, because every `logit` process must reach the same
  verdict for the same key with nothing propagated.
- **`prometheus_in`'s TLS keys are prefixed by the mode they serve**: `scrape_tls:` for the scrape
  client, `bind_tls:` for the remote-write server. The old bare `tls:` is gone (pre-release) with
  no alias; don't add one back.
- **`flatten` never deletes an attribute.** It removes a source attribute only once its leaves are
  written. Last write wins on a key collision, silently, and there is deliberately no cap on how
  many keys one value can expand into beyond a fixed internal recursion-depth bound. That's a
  settled, documented gap (`docs/known-gaps.md`), not a missing guard.
- **`http_access` derives every value from config or a built-in table, never a capture.** It's
  best-effort per field, never drops an event, and emits no metrics (a stock `kv_metrics` + `keep`
  does that).
- **`prometheus_out` never accumulates a delta.** That summarization stays `aggregate`'s job, per
  `lossless-transit`'s "summarization is opt-in and named" rule.
- **`shape` and `aggregate` split the work**: `shape` emits raw `Samples`, never a sketch; an
  `aggregate` downstream summarizes, per `lossless-transit`.

The lossless-relay, Lua-VM, mergeable-sketch, and memory-pin rules are in
[Design constraints that aren't optional](#design-constraints-that-arent-optional).

## Current state

Every kind in `logit_config`'s `ComponentKind` (`crates/logit-config/src/lib.rs`) is a real,
implemented component. `logit run` rejects a config referencing any unimplemented kind with a
clear error.

The v0.1 statsd/InfluxDB slice is complete: statsd in, a 10s `aggregate` window, a Lua
enrichment stage, and InfluxDB 2.x out, via `logit run <config>`
([fixtures/statsd-to-influxdb.yaml](fixtures/statsd-to-influxdb.yaml), `script/server`).
Everything below has landed on top of it.

### Inputs

Listeners live in `crates/logit-inputs`, codecs in `crates/logit-proto`.

| Kind | Code | What it does | Decision record |
|---|---|---|---|
| `statsd_in` | `crates/logit-inputs/src/statsd.rs` | statsd/DogStatsD over UDP (default), `transport: tcp`, or a Unix socket (`unix`, `unix_stream`) | [ADR `decoupled-listener-io`](docs/adr/decoupled-listener-io.md) |
| `syslog_in` | `crates/logit-inputs/src/syslog.rs` | syslog over UDP (default) or `transport: tcp`, optional TLS (RFC 5425) | [ADR `syslog-tcp-ingress-and-tls`](docs/adr/syslog-tcp-ingress-and-tls.md) |
| `graphite_in` | `crates/logit-inputs/src/graphite/` | carbon plaintext and pickle, UDP or TCP | [ADR `graphite-carbon-relay`](docs/adr/graphite-carbon-relay.md) |
| `collectd_in` | `crates/logit-inputs/src/collectd.rs` | collectd's binary `network` protocol, unicast or multicast | [ADR `collectd-binary-relay`](docs/adr/collectd-binary-relay.md) |
| `otlp_in` | `crates/logit-inputs/src/otlp.rs` | OTLP logs, metrics, and traces over OTLP/HTTP (protobuf and OTLP/JSON) and OTLP/gRPC | [ADR `otlp-json-decoding`](docs/adr/otlp-json-decoding.md) |
| `datadog_in` | `crates/logit-inputs/src/datadog.rs` | Datadog's intake API over HTTP (series, sketches, checks, events, logs, APM traces and stats), gzip/deflate/zstd, `503` when busy | [ADR `datadog-agent-and-intake-relay`](docs/adr/datadog-agent-and-intake-relay.md) |
| `datadog_trace_in` | `crates/logit-inputs/src/datadog_trace.rs` | the Datadog Agent's APM API for dd-trace tracers (`/v0.3`–`/v0.7/traces` msgpack, `/v0.6/stats`, `/info`) over TCP and/or a Unix socket; keeps every span, `503` (a tracer's loss) when busy | [ADR `datadog-agent-and-intake-relay`](docs/adr/datadog-agent-and-intake-relay.md) |
| `prometheus_in` | `crates/logit-inputs/src/prometheus.rs` | scrapes `/metrics` targets, or receives remote-write | [ADR `prometheus-scrape-and-exposition`](docs/adr/prometheus-scrape-and-exposition.md), [ADR `prometheus-remote-write`](docs/adr/prometheus-remote-write.md) |
| `tail_in` | `crates/logit-inputs/src/tail/` | rotation- and checkpoint-aware file tailing | [ADR `file-tailing-and-docker-json-logs`](docs/adr/file-tailing-and-docker-json-logs.md) |
| `docker_in` | `crates/logit-inputs/src/docker.rs` | Docker json-file container logs, enriched from a sibling `config.v2.json`; no docker socket | same ADR as `tail_in` |
| `logit_in` | `crates/logit-inputs/src/logit.rs` | the native `logit`-to-`logit` transport | [ADR `native-transport-handshake-and-ack`](docs/adr/native-transport-handshake-and-ack.md) |
| `internal` | `crates/logit-inputs/src/internal.rs` | `logit` observing itself: its own telemetry as ordinary events | [ADR `internal-telemetry-as-pipeline-events`](docs/adr/internal-telemetry-as-pipeline-events.md) |
| `generate_in` | `crates/logit-inputs/src/generate.rs` | declarative event generator for load tests | [ADR `load-test-harness`](docs/adr/load-test-harness.md) |

### Outputs

Sinks live in `crates/logit-outputs`.

| Kind | Code | What it does | Decision record |
|---|---|---|---|
| `influxdb_out` | `crates/logit-outputs/src/influxdb.rs` | InfluxDB 2.x, with bounded output retry | [ADR `service-lifecycle-and-output-retry`](docs/adr/service-lifecycle-and-output-retry.md) |
| `stdio_out` | `crates/logit-outputs/src/stdio.rs` | human-readable render (default) or `format: native`; its file target is `file_out` with an empty rotation policy | [ADR `file-output-native-format`](docs/adr/file-output-native-format.md) |
| `file_out` | `crates/logit-outputs/src/file.rs` | rotating file sink sharing `stdio_out`'s implementation | [ADR `rotating-file-output`](docs/adr/rotating-file-output.md) |
| `syslog_out` | `crates/logit-outputs/src/syslog.rs` | RFC 3164/5424 over UDP, TCP, or TLS (RFC 5425) | [ADR `syslog-output`](docs/adr/syslog-output.md) |
| `statsd_out` | `crates/logit-outputs/src/statsd.rs` | statsd/DogStatsD over UDP, TCP (optionally TLS), or a Unix socket (`unix`, `unix_stream`) | [ADR `statsd-output`](docs/adr/statsd-output.md) |
| `otlp_out` | `crates/logit-outputs/src/otlp.rs` | OTLP logs, metrics, and traces over OTLP/HTTP and OTLP/gRPC | [ADR `otlp-tls-and-pooled-grpc-client`](docs/adr/otlp-tls-and-pooled-grpc-client.md) |
| `datadog_out` | `crates/logit-outputs/src/datadog.rs` | Datadog's intake API: series, distribution points, sketches, service checks, events, logs, and Agent-processed APM traces and stats, one request per route; drops stale points and unprocessed traces, counted | [ADR `datadog-agent-and-intake-relay`](docs/adr/datadog-agent-and-intake-relay.md) |
| `datadog_trace_out` | `crates/logit-outputs/src/datadog_trace.rs` | a Datadog Agent's APM API (traces and `/v0.6/stats`), v0.4 or v0.7, over TCP or the Agent's Unix socket, restoring the tracer's request headers | [ADR `datadog-agent-and-intake-relay`](docs/adr/datadog-agent-and-intake-relay.md) |
| `prometheus_out` | `crates/logit-outputs/src/prometheus.rs` | serves an exposition endpoint, or sends remote-write | [ADR `prometheus-scrape-and-exposition`](docs/adr/prometheus-scrape-and-exposition.md), [ADR `prometheus-remote-write`](docs/adr/prometheus-remote-write.md) |
| `collectd_out` | `crates/logit-outputs/src/collectd.rs` | collectd's binary `network` protocol | [ADR `collectd-binary-relay`](docs/adr/collectd-binary-relay.md) |
| `graphite_out` | `crates/logit-outputs/src/graphite.rs` | carbon plaintext and pickle | [ADR `graphite-carbon-relay`](docs/adr/graphite-carbon-relay.md) |
| `logit_out` | `crates/logit-outputs/src/logit.rs` | the native `logit`-to-`logit` transport | [ADR `native-transport-handshake-and-ack`](docs/adr/native-transport-handshake-and-ack.md) |
| `null_out` | `crates/logit-outputs/src/null.rs` | discards everything; a load-test sink | [ADR `load-test-harness`](docs/adr/load-test-harness.md) |

### Transforms and routing

Native transforms live in `crates/logit-transforms`; `lua`/`lua_file` live in `crates/logit-script`.

| Kind | What it does | Decision record |
|---|---|---|
| `lua`, `lua_file` | a user script, inline or from a `.lua` file path relative to the config file; an optional `interval` runs its `flush()` | [ADR `lua-flush-root-context`](docs/adr/lua-flush-root-context.md), [ADR `lua-event-constructor`](docs/adr/lua-event-constructor.md) |
| `aggregate` | windowed aggregation into mergeable metric kinds | [ADR `aggregation-window-semantics`](docs/adr/aggregation-window-semantics.md) |
| `json` | parses JSON into attributes | [ADR `json-parsing-into-attributes`](docs/adr/json-parsing-into-attributes.md) |
| `csv` | delimiter-separated columns into attributes from a config-declared, positional schema; no header-row mode | [ADR `csv-positional-columns`](docs/adr/csv-positional-columns.md) |
| `logfmt`, `kv` | the de-facto `key=value` parsers | [ADR `logfmt-and-kv-parsing`](docs/adr/logfmt-and-kv-parsing.md) |
| `regex` | named capture groups into attributes | [ADR `regex-transform`](docs/adr/regex-transform.md) |
| `kv_metrics` | turns attributes already on an event into metrics on that same event | [ADR `kv-metrics-semantics`](docs/adr/kv-metrics-semantics.md) |
| `keep` | retains only the named attributes (an allowlist); place it before `aggregate` to bound series cardinality | [ADR `kv-metrics-semantics`](docs/adr/kv-metrics-semantics.md) |
| `remove` | drops the named attributes | — |
| `set` | stamps constant values onto event attributes and/or the batch resource | [ADR `operator-declared-resource-attributes`](docs/adr/operator-declared-resource-attributes.md) |
| `scale` | multiplies named numeric attributes by a constant factor (unit conversion) | [ADR `scale-transform`](docs/adr/scale-transform.md) |
| `keep_values` | clamps attribute values to a per-field allow-list | [ADR `value-allowlist-cardinality-clamp`](docs/adr/value-allowlist-cardinality-clamp.md) |
| `trace_context` | gives a `LogRecord` a native application trace/span reference, from W3C hex ids or, under `format: datadog`, a dd-trace tracer's decimal and 128-bit `dd.trace_id`/`dd.span_id`; an opt-in `span:` block turns an access log line into a real `SpanRecord` on the same event | [ADR `log-record-trace-context`](docs/adr/log-record-trace-context.md), [ADR `trace-context-span-lifting`](docs/adr/trace-context-span-lifting.md) |
| `http_access` | normalizes web-server access logs onto OTel semconv | [ADR `http-access-normalization`](docs/adr/http-access-normalization.md) |
| `flatten` | rewrites a nested attribute into flat, dot-joined keys | [ADR `flatten-transform`](docs/adr/flatten-transform.md) |
| `has_signal`, `keep_signals`, `drop_signals` | forward an event carrying a listed signal; keep, or clear, the listed signals' payloads | [ADR `signal-filtering-components`](docs/adr/signal-filtering-components.md) |
| `has_attributes`, `drop_attributes` | forward, or drop, an event whose resource and/or attributes match every configured pair | [ADR `attribute-filtering-components`](docs/adr/attribute-filtering-components.md) |
| `has_provenance`, `drop_provenance` | forward, or drop, an event whose batch `origin`/`previous` match a configured list | [ADR `provenance-filtering-components`](docs/adr/provenance-filtering-components.md) |
| `sample` | consistent, keyed sampling | [ADR `consistent-sampling-component`](docs/adr/consistent-sampling-component.md) |
| `shape` | an observer that measures each event's shape | [ADR `shape-observer-component`](docs/adr/shape-observer-component.md) |
| `route`, `target` | native equality-only router, and the named destinations it fills | [ADR `target-components`](docs/adr/target-components.md) |

Details an agent needs beyond the table:

- **`aggregate`**: `temporality: cumulative` keeps a delta `Sum`/`Histogram` accumulator across
  flushes, summing instead of resetting. That's what lets `statsd_in -> aggregate ->
  prometheus_out` and `internal -> aggregate -> prometheus_out` expose real running counters. See
  [ADR `prometheus-scrape-and-exposition`](docs/adr/prometheus-scrape-and-exposition.md)'s
  "Temporality is `aggregate`'s job" section and the
  [ADR `aggregation-window-semantics`](docs/adr/aggregation-window-semantics.md) cumulative
  amendment. Its raw-retention modes keep exact values, with a real HyperLogLog backing
  `sets: estimate` and the overflow fallback.
- **`lua`**: `Event.new(t)` makes every payload constructible from Lua as the inverse of
  `event:to_table()`, with `flush(now)` supplying a flush-driven emission's timestamp. The Lua
  surface covers `event.metrics`, `event.span`, `scope`, and the log/resource fields. See
  [ADR `lua-flush-root-context`](docs/adr/lua-flush-root-context.md) for the root context a Lua
  `flush()` runs in.
- **`json`**: `invalid_utf8: replace` retries a parse that failed on invalid UTF-8 (nginx's
  `escape=json` passes high bytes raw) on a lossy copy, on the failure path only.
- **`regex`** replaced a `lua` component in `demo/logit.yaml`'s postgres tier.
- **`keep_values`** is `keep`'s value-side sibling. It clamps attribute (and/or resource-attribute)
  values to a per-field allow-list: a tag whose valid set the operator knows but the producer
  doesn't enforce (nginx's `$host` against a handful of real vhosts, say) becomes that field's
  `other` (or is removed) rather than an unbounded new series. An optional ordered `normalize:`
  step (today just ASCII-lowercasing) is applied and written back before the allow test.
  [fixtures/nginx-to-influxdb.yaml](fixtures/nginx-to-influxdb.yaml) runs one, clamping `$host`
  ahead of `aggregate`.
- **`http_access`** is placed once between `json` and `trace_context`, replacing the hundred
  lines of per-server `map` blocks it used to take. The web server logs its access line under raw
  OTel semconv attribute names with the untouched value (`url.original`,
  `http.response.status_code`, `user_agent.original`, plus a few `logit`-own names:
  `http.request.line`, the unit-suffixed `http.request.duration_{s,ms,us}`, `upstream.*`).
  `http_access` then:
  - decomposes the composites, coerces numerics (nginx's `"000"` status to `0`), and merges
    durations into `_s`;
  - normalizes method (`_OTHER` plus `http.request.method_original`) and protocol version;
  - redacts semconv's sensitive query keys, then caps every free-text field per `CAPPED_FIELDS`;
  - derives a bounded set: `user_agent.class` from an ordered regex table, `http.route` from
    operator `routes:` plus three built-in sets, `error.type`, `span.name`, `span.status` (never
    `ok`), and a `span.duration_s` mirror that `trace_context` resolves a span from.

  Every canonical name is also accepted with each `.` replaced by `-`
  (`http-response-status_code`; only the dots change). That fixed alias table exists for
  HAProxy's `%{+json}o`, whose item names can't contain a dot, and it's how
  `demo/haproxy/haproxy.cfg` logs. See [docs/http-access-logs.md](docs/http-access-logs.md) (the
  operator-facing schema, with a snippet per server) and
  [fixtures/nginx-to-influxdb.yaml](fixtures/nginx-to-influxdb.yaml).
- **`flatten`** is an opt-in, operator-placed rewrite of a nested `Value::Map`/`Value::Array`
  attribute into flat, dot-joined keys (`foo.key`, `tags.0`, composing as `items.0.name`). It
  exists because `influxdb_out`, `statsd_out`, `prometheus_out`, `graphite_out`, and
  `collectd_out` each drop a nested attribute outright; none of their wire formats has anywhere
  to put one. The event model still nests and every decoder still produces nesting (`json`,
  `otlp_in`, `syslog_in`'s `syslog.sd`, Lua); `flatten` is a rewrite for the one pipeline leg that
  needs a flat shape, not a codec-wide convention. It confronts head-on the three ADRs that
  rejected dotted flattening as *implicit* decoder/matcher behavior. A leaf is any non-container
  value or an empty `Map`/`Array`. See the invariants above and
  [fixtures/nested-json-to-influxdb.yaml](fixtures/nested-json-to-influxdb.yaml).
- **`sample`** hashes `key: trace_id` (a span's, else a log's `TraceRef`), `{attribute: ..}`, or
  `{resource: ..}` and compares it against `rate`, so every event sharing a key gets the same
  verdict in every `logit` process with nothing propagated. An `always_keep:` override pins
  flagged events through, and `missing:` decides the fate of an event the key isn't on. It's the
  one kind `routing-by-condition-is-lua` retired that came back, because keyed consistency is the
  thing a `lua` component can't express. `trace_is_sampled` shares its 53-bit compare in
  `logit_core::sampling`. Graph rule 61 validates it. See
  [fixtures/sample-traces.yaml](fixtures/sample-traces.yaml).
- **`shape`** is tapped off a flow by ordinary fan-out and never placed in it. It rewrites every
  event it sees into a measurement of that event's own shape: attribute and nested-map counts,
  key/value byte lengths, a per-type value count, and metric and span widths, tagged
  `signal`/`source`/`tap`. On its `interval` it adds per-batch measurements (events,
  resource/scope attribute counts, distinct key-sets per batch) and cumulative gauges (distinct
  keys, distinct key-sets, top-1/top-5 key-set share, an overflow flag). See
  [fixtures/shape-tap.yaml](fixtures/shape-tap.yaml) and
  [docs/plans/data-shape-survey.md](docs/plans/data-shape-survey.md) (W1 of the survey this
  instrument exists to collect).
- **`route`/`target`**: a `target` is a named, zero-cost destination a router directs events into,
  and `route` is the native equality-only router that fills it. Together they remove two
  structural costs the central-collector split-apart topology had no better answer for: a filter
  chain paid by every branch, and the deep clone an all-mutating fan-out can't avoid.
- **`generate_in`**: `event:` templates via `logit_core::template`, with `{seq}`/`{seq%N}`
  placeholders. `generate_in` and `null_out` ship unconditionally but exist only to drive the
  load-test harness (see [Harnesses](#harnesses)).

### Lossless like-protocol relays

Eight like-protocol pairs must each relay losslessly, modulo a named list of permitted
normalizations ([ADR `lossless-transit`](docs/adr/lossless-transit.md); the rule itself is under
[Design constraints that aren't optional](#design-constraints-that-arent-optional)).
[docs/plans/lossless-transit.md](docs/plans/lossless-transit.md) has the closing assessment for
the first three pairs, and [docs/plans/datadog-relay.md](docs/plans/datadog-relay.md) for the two
Datadog pairs. What the model and codecs carry for them:

- **Model v2**: `Sum`/`Samples`/`SetMembers`/`ExponentialHistogram`, `SpanExt`, batch-level
  `Scope`, `MetricRecord.flags`.
- **OTLP**: scope grouping, start time, description, exemplars, a `NO_RECORDED_VALUE` point
  round-tripped flagged, `event_name`/`observed_timestamp`, span `flags`/`trace_state`/a real
  status-message field, and dropped-attribute counts.
- **syslog**: structured data as `syslog.sd`, timestamp precedence, bytes MSG, and opt-in
  structured-data emission.
- **statsd**: raw timers/sets, `|c:`/`|e:`/`|card:`/`|T`, and events/service checks.
- **Datadog**: `datadog.*` carriers for every raw field without a typed home (a `rate`'s type, a
  count's interval, resources, origin, trace chunk, tracer, and Agent fields), `DdSketch` under
  `Mapping::agent` for the Agent's metrics sketches and `Mapping::logarithmic` for APM stats
  sketches, 128-bit trace ids through `_dd.p.tid`, and APM stats as metric events.

Residual debt lives in `docs/known-gaps.md`: post-sketch metric kinds at `statsd_out`;
`statsd_out` carrying no `unit` and no native rename/prefix and stamping an egress timestamp only
on a `|T`-marked line; and syslog's `event.timestamp` staying receipt time while the wire
TIMESTAMP follows the precedence table. The Datadog pairs' residual debt is in the same file's
"Datadog" section, listed by the closing assessment.

Per pair:

- **`statsd_in -> statsd_out`**: `statsd_out` is the mirror of `statsd_in`, over UDP, TCP
  (optionally TLS, the `syslog_out` arrangement ported verbatim), or the Datadog Agent's two Unix
  sockets (`transport: unix`/`unix_stream`, graph rule 65), with DogStatsD tags
  round-tripped through the real decoder. It encodes `Sum` (delta, monotonic),
  `Gauge`/`GaugeDelta`, `Samples`, `SetMembers`, and DogStatsD events/service checks. A relay with
  no `aggregate` in between, or one configured `distributions: samples`/`sets: members`,
  round-trips timers and sets byte-for-byte under `format: dogstatsd`. Under `format: statsd` it's
  lossless modulo the ADR's permitted normalizations: multi-value lines split, and `h`/`d`
  normalize to `ms`. Only post-sketch kinds
  (`Distribution`/`Set`/`Histogram`/`ExponentialHistogram`/`Summary`, and a cumulative or
  non-monotonic `Sum`) are dropped and counted (`docs/known-gaps.md`).
- **`otlp_in -> otlp_out`**: logs, metrics, and traces, over both OTLP/HTTP and a hand-rolled
  OTLP/gRPC transport. `otlp_in`'s HTTP side accepts OTLP/JSON as well as protobuf
  ([ADR `otlp-json-decoding`](docs/adr/otlp-json-decoding.md)). The protobuf types are committed
  pregenerated ([ADR `committed-pregenerated-otlp-protobuf`](docs/adr/committed-pregenerated-otlp-protobuf.md)).
  gRPC framing is a hand-rolled service over `hyper` rather than `tonic`
  ([ADR `hand-rolled-grpc-over-hyper`](docs/adr/hand-rolled-grpc-over-hyper.md)), but client-side
  connection management is a pooled, TLS-capable `hyper-util`/`hyper-rustls` client, not a
  per-request hand-rolled connect. Both `otlp_out` and `otlp_in` support TLS (`tls:` in config,
  private CAs and mutual TLS included) on both transports, selected by the endpoint's scheme
  ([ADR `otlp-tls-and-pooled-grpc-client`](docs/adr/otlp-tls-and-pooled-grpc-client.md)).
- **`syslog_in -> syslog_out`**: `syslog_out` speaks RFC 3164/5424 over UDP, TCP, or TLS (RFC
  5425), round-tripping header fields from an event's `syslog.*` attributes
  ([ADR `syslog-output`](docs/adr/syslog-output.md),
  [ADR `syslog-tcp-ingress-and-tls`](docs/adr/syslog-tcp-ingress-and-tls.md)).
- **`prometheus_in -> prometheus_out`**: built lossless against the model from its first PR
  rather than retrofitted. It's a fixed point modulo a short, named list of normalizations
  ([ADR `prometheus-scrape-and-exposition`](docs/adr/prometheus-scrape-and-exposition.md),
  [fixtures/prometheus-relay.yaml](fixtures/prometheus-relay.yaml)). Both dialects (Prometheus
  text 0.0.4 and OpenMetrics 1.0) are negotiated on `Accept`/`Content-Type`. `prometheus` is
  the one pair outside the three encoder shapes (see [Where things live](#where-things-live)), by
  design. Both components are **two-mode**, each mode chosen by which config field is set and
  enforced by graph rules 55 and 56 rather than by a new kind, so a scrape or exposition config
  needs no edit ([ADR `prometheus-remote-write`](docs/adr/prometheus-remote-write.md)):
  - `prometheus_in` either scrapes `scrape_targets:` on an interval, or binds a remote-write
    **receiver** on `bind:`. The receiver accepts 1.0 and 2.0 on one listener, chosen per
    request from its own `Content-Type`, with `bind_tls:` for server TLS. It decodes
    `Content-Encoding: snappy` or `zstd` (the VictoriaMetrics remote write protocol, vmagent's
    default, through `ruzstd`), so vmagent stays on zstd with no configuration. A bounded
    `metadata_cache:` (`max_families`/`ttl`) is what makes 1.0 typed at all, because
    Prometheus's own 1.0 sender ships `metadata[]` in separate requests from the samples it
    describes.
  - `prometheus_out` either exposes a registry on `bind:`, or **sends** remote-write to an
    `endpoint:` under an explicit `version: 1 | 2`. There's no negotiation and no fallback: the
    operator picks the one their receiver speaks, as they already pick an exposition dialect.
    `compression: snappy | zstd` is chosen the same way: `zstd` is the VictoriaMetrics remote
    write protocol, `ruzstd`-encoded, `version: 1` only (graph rule 56), with no fallback to
    Snappy. It sends one `POST` per batch with no retry in the sink; `duplicate_safe()` is `true` because
    a sample's identity at a receiver is `(label set, timestamp)`.
  - Native histograms are skipped and counted in both directions, pending their own follow-up.

  Examples: [fixtures/prometheus-remote-write-receive.yaml](fixtures/prometheus-remote-write-receive.yaml),
  [fixtures/prometheus-remote-write-send.yaml](fixtures/prometheus-remote-write-send.yaml).
  VictoriaMetrics, VictoriaLogs, and VictoriaTraces are reached through these and the other
  standard-protocol components, with no kind of their own
  ([ADR `victoriametrics-interop`](docs/adr/victoriametrics-interop.md)).
- **`collectd_in -> collectd_out`**: `crates/logit-proto`'s `collectd` codec holds both
  directions in one module whose doc is the mapping table. It covers both kinds collectd's binary
  `network` protocol carries: value lists, and notifications as a log event with a
  `collectd.severity` attribute. A sticky-identity
  decoder turns one datagram into one event per Values part, with its N data sources as N
  `MetricRecord`s in wire order. A multicast `bind:` is detected from the address and joined with
  no extra field. An optional `types_db:` names those data sources without ever changing what
  goes back on the wire. `CollectdEncoder` implements
  [`FramedEncoder`](docs/adr/framed-encoder.md) (a `MessageBuf<usize>` of already-packed
  datagrams, because the receiver resets its sticky state at every datagram edge and
  `max_packet_bytes` decides where those edges fall) rather than the one-blob-per-batch
  `Encoder`. It's a fixed point modulo its own named normalization list
  ([ADR `collectd-binary-relay`](docs/adr/collectd-binary-relay.md),
  [fixtures/collectd-relay.yaml](fixtures/collectd-relay.yaml)).
- **`graphite_in -> graphite_out`**: `crates/logit-proto`'s `graphite` codec covers both of
  carbon's wire protocols, plaintext and pickle (a hand-rolled writer and a restricted,
  opcode-allowlisted reader, no new crate dependency). `multi_value: skip | expand` decides the
  metric kinds carbon's one-number-per-datapoint wire can't carry natively, and
  `tags: carbon | drop` decides whether attributes render as carbon's own `;k=v` segment. It's a
  fixed point modulo its own named normalization list
  ([ADR `graphite-carbon-relay`](docs/adr/graphite-carbon-relay.md),
  [fixtures/graphite-relay.yaml](fixtures/graphite-relay.yaml)).
- **`datadog_in -> datadog_out`** (Datadog's intake API): `crates/logit-proto/src/datadog/` holds
  one codec module per payload family, and `mod.rs`'s doc is the mapping table: series v1 and v2
  (JSON and protobuf), distribution points, sketches bin-for-bin, service checks, events (the
  Agent's `/intake/` envelope and public v1), logs, `AgentPayload` traces, and `StatsPayload` APM
  stats, relayed rather than recomputed. `datadog_in` decodes gzip, deflate, and zstd (`ruzstd`)
  and answers a full pipeline `503` after a bounded wait. `datadog_out` sends a trace chunk only
  when its root carries the Agent's `_top_level` mark, so `datadog_trace_in` must not feed it;
  it drops stale points before sending, and isn't duplicate-safe. Normalizations include
  whole-second metric timestamps, one point per series, `avg` recomputed from `sum`/`cnt`, and a
  batch per `TracerPayload`
  ([ADR `datadog-agent-and-intake-relay`](docs/adr/datadog-agent-and-intake-relay.md),
  [fixtures/datadog-intake-standin.yaml](fixtures/datadog-intake-standin.yaml),
  [fixtures/datadog-direct.yaml](fixtures/datadog-direct.yaml)).
- **`datadog_trace_in -> datadog_trace_out`** (a Datadog Agent's APM API): v0.3, v0.4, v0.5,
  and v0.7 msgpack traces and `/v0.6/stats` over TCP or the Agent's Unix socket, the tracer's
  request headers carried as `datadog.tracer.*` resource attributes and restored as headers. v0.4
  egress, the default, relays a v0.4 tracer losslessly; a v0.7 tracer's chunk fields need
  `version: v0.7`. A `503` from `datadog_trace_in` defers a payload only for the tracer's short
  retry window
  ([fixtures/datadog-agent-standin.yaml](fixtures/datadog-agent-standin.yaml),
  [fixtures/datadog-agent-relay.yaml](fixtures/datadog-agent-relay.yaml)).

  [docs/datadog.md](docs/datadog.md) is the operator-facing account of both pairs: the four
  topologies (direct, through a local Agent, and standing in for an Agent or for the intake),
  best practice in each direction, and the rules that lose data when missed.

### Listener I/O

- **UDP**: `statsd_in` and `syslog_in` are thin wrappers over a shared
  `logit-inputs::udp::UdpListener` driver for their (default) UDP transport. A UDP listener's
  socket read and its decode/batch-assembly loop run decoupled through a `ReceiveQueue`, the
  listener-side mirror of `SinkQueue`'s sink-side decoupling, so a stalled downstream doesn't
  stop the socket being read. It's configured by the `receive:` block
  ([ADR `decoupled-listener-io`](docs/adr/decoupled-listener-io.md)).
- **Linux batching and kernel counters**: `read_loop` takes up to `receive.read_batch` datagrams
  (default 64) per `recvmmsg(2)` call and hands the batch to `BoundedQueue::push_many`/`pop_many`,
  which update the queue's telemetry gauges once per batch instead of once per datagram on both
  the push and pop side. `logit_pipeline::sockstat` reads the kernel's own per-socket counters
  straight off a listener's fd (`getsockopt(SO_MEMINFO)` for a UDP socket's drops/receive-buffer
  fill, `TCP_INFO` for a `LISTEN` socket's accept-queue depth/backlog), once a second and once
  more after the read loop stops. So `logit.input.kernel.drops` and the
  `receive_buffer.*`/`accept_queue.*` gauges attribute to a component a loss no other layer could
  see ([ADR `udp-intake-batching-and-socket-visibility`](docs/adr/udp-intake-batching-and-socket-visibility.md)).
- **TCP**: `syslog_in`, `graphite_in`, and `statsd_in` can each run `transport: tcp` on a
  generic stream driver, `logit-inputs::tcp::TcpListener` (`crates/logit-inputs/src/tcp.rs`),
  which provides:
  - an accept loop and a connection cap;
  - per-listener framing: RFC 6587's auto-detecting pair for `syslog_in`, LF-delimited lines for
    `statsd_in` and carbon plaintext, and carbon's 4-byte length prefix for pickle;
  - `handshake_timeout:`, bounding each pre-message phase;
  - an opt-in `idle_timeout:`, bounding the quiet gaps after them (off by default;
    [ADR `idle-connection-timeout`](docs/adr/idle-connection-timeout.md));
  - a `tls:` block: RFC 5425 for syslog, a `logit`-to-`logit` or stunnel-shaped relay hop for the
    other two ([ADR `syslog-tcp-ingress-and-tls`](docs/adr/syslog-tcp-ingress-and-tls.md) and its
    amendment).

  A TCP listener has no receive queue at all: TCP's own flow control is the backpressure, unlike
  the UDP-only `decoupled-listener-io` queue every datagram listener shares. So a TCP
  `graphite_in` takes the same `tls:` block a TCP `syslog_in` does.

### Native wire format and transport

- **Encoding**: `logit_proto::native` (`crates/logit-proto/src/frame.rs` + `src/native/`) is a
  tested `Encoder`/`Decoder`: dictionary-first, hand-rolled, framed by a 24-byte header with
  CRC-32C and optional lz4. A four-arm bake-off against `rkyv`, `postcard`, and OTLP itself
  decided it ([ADR `native-wire-format-encoding`](docs/adr/native-wire-format-encoding.md)).
- **On disk**: `stdio_out`/`file_out` can write it as `format: native` alongside their default
  human-readable render ([ADR `file-output-native-format`](docs/adr/file-output-native-format.md)).
- **Disk buffer**: any sink can opt into `buffer.disk:`, a crash-recoverable disk spool over these
  same frames that replaces that sink's in-memory delivery queue
  ([ADR `disk-backed-sink-buffer`](docs/adr/disk-backed-sink-buffer.md)).
- **Connection**: `logit_in`/`logit_out` (`crates/logit-inputs/src/logit.rs`/
  `crates/logit-outputs/src/logit.rs`) use one TCP (optionally TLS) connection, a `Hello`/`HelloAck`
  version/codec/compression handshake, and one native frame per batch, acknowledged before the
  next is sent ([ADR `native-transport-handshake-and-ack`](docs/adr/native-transport-handshake-and-ack.md)).

### Runtime and pipeline

- **Config graph**: config is a flat graph of named components
  (ADR `component-graph-configuration`, [pipeline-graph.md](docs/design/pipeline-graph.md)),
  resolved and validated by `logit-pipeline::graph`, then run by `logit-pipeline::run`'s node
  runtime. `logit-cli::pipeline` is only the kind → implementation registry.
  `crates/logit-pipeline/src/runtime.rs` has the orchestration and the per-node flush-tick timer.
  `logit graph <config>` prints the resolved graph as graphviz DOT (`crates/logit-cli/src/dot.rs`).
- **Config loading**: config files are read and parsed exclusively through
  `logit_cli::config::load` (`crates/logit-cli/src/config.rs`), which also resolves
  `!env VAR_NAME`. Any field on any component can pull its value from the environment this way
  ([ADR `env-yaml-tag`](docs/adr/env-yaml-tag.md)), which is why `influxdb_out`'s `token` is a
  plain string, not an env-specific field.
- **Lifecycle**: [ADR `service-lifecycle-and-output-retry`](docs/adr/service-lifecycle-and-output-retry.md)
  covers signal-driven shutdown and `influxdb_out`'s bounded output retry.
  `crates/logit-inputs/src/statsd.rs` and `crates/logit-outputs/src/influxdb.rs` are the
  reference listener and sink.
- **Trace propagation**: every `Delivered` (one `Fanout` edge's channel payload) carries a real
  `TraceContext`, propagated as a child of its parent for the two node kinds with an unambiguous
  one to propagate: `Transform::process`/`ScriptWorker::process`'s non-flush path, and
  `run_output`. See
  [ADR `trace-context-propagation-on-delivered`](docs/adr/trace-context-propagation-on-delivered.md)
  and [pipeline-graph.md](docs/design/pipeline-graph.md)'s "Trace context propagation" section.
- **Internal telemetry**: `internal` drains `logit_core::telemetry`'s per-component buffers into
  ordinary events on its own `interval`
  ([ADR `internal-telemetry-as-pipeline-events`](docs/adr/internal-telemetry-as-pipeline-events.md),
  [internal-telemetry.md](docs/design/internal-telemetry.md),
  [fixtures/internal-telemetry.yaml](fixtures/internal-telemetry.yaml) is a runnable config).
  `otlp_out` carries those spans and metrics out over the wire.
- **Internal spans**: every node visit (a listener's send, a transform's process/flush, a sink's
  deliver) mints exactly one real `SpanRecord` from that context. Spans are deterministically
  sampled on `trace_id` (`span_sample_rate`, default `0.1`, `1.0` in the demo), so every `logit`
  process in a split-collection topology reaches the same keep/drop verdict independently with no
  propagated bit. See
  [ADR `internal-span-emission-and-deterministic-sampling`](docs/adr/internal-span-emission-and-deterministic-sampling.md)
  and `internal-telemetry.md`'s "Spans" section. `docs/known-gaps.md`'s internal-spans entry
  tracks what's still open: the listener span's window, and Lua `flush()`'s link-less root.

### Operator surface

[docs/deploying.md](docs/deploying.md)'s "Probes and exit codes" and "Self-logging" sections are
the operator-facing account of all of this.

- **Self-logging**: leveled, structured, through `tracing` (`--log-level`/`LOGIT_LOG`,
  `--log-format text|json`; [ADR `tracing-for-self-logging`](docs/adr/tracing-for-self-logging.md)).
  `internal`'s own `logs:` setting (`warn` by default, `error`, or `off`) captures `logit`'s own
  `warn`-and-above self-diagnostics into the pipeline as ordinary log events through
  `logit_core::telemetry::TelemetryLayer` (`internal-telemetry.md`'s "Logs" section).
- **Readiness**: a top-level `admin:` block serves `/readyz`/`/healthz` and the `logit ready`
  probe helper that `Dockerfile`'s `HEALTHCHECK` uses
  ([ADR `admin-readiness-endpoint`](docs/adr/admin-readiness-endpoint.md)).
- **Startup binding**: `Input::bind` opens every listener's socket in a pre-pass *before* any
  task is spawned, so a bind failure fails startup with nothing else running.
- **Exit codes**: `1` for a startup failure, `2` for a runtime failure after the process reported
  ready.
- **Release image**: `ghcr.io/ross/logit:latest`, pushed by hand via `workflow_dispatch` rather
  than on every merge
  ([ADR `publish-release-image-to-ghcr`](docs/adr/publish-release-image-to-ghcr.md)).

### Demo and examples

- **`fixtures/`** is contributor-facing fixtures the dev stack (`script/server`) runs against. Keep
  them real: other things in the repo depend on them (`compose.yaml`'s `nginx` service,
  `crates/logit-bench/src/fixtures.rs`'s `NGINX_SYSLOG_LINE`).
  [fixtures/nginx-to-influxdb.yaml](fixtures/nginx-to-influxdb.yaml) exercises the
  syslog/InfluxDB side against a real nginx (`fixtures/nginx/`).
- **`demo/`** is the answer to "let me see this work" for anyone else: a self-contained
  `docker compose up` against the release image. Logs, metrics, and traces all flow through it
  end to end, into Loki, VictoriaMetrics, and Tempo respectively
  ([docs/plans/demo-stack.md](docs/plans/demo-stack.md),
  [docs/plans/otlp-end-to-end.md](docs/plans/otlp-end-to-end.md)). In `demo/logit.yaml`:
  - `loki_out` is `otlp_out` over HTTP straight to Loki
    ([docs/plans/otlp-logs-and-resource-identity.md](docs/plans/otlp-logs-and-resource-identity.md)'s
    workstream B); `tempo_out` is `otlp_out` over gRPC to Tempo, proving the internal-span chain
    against a real Tempo.
  - `victoria_out` is `prometheus_out` sending remote-write 1.0 (zstd) to VictoriaMetrics, so both
    `aggregate`s feeding it run `temporality: cumulative`. The Grafana dashboard's metric panels
    are PromQL against the Prometheus-sanitized names (`web_requests_total`).
  - `nginx_in` is `docker_in`, tailing that tier's container directly instead of receiving a
    `syslog:` stream; `postgres_in` is `tail_in`, tailing Postgres's own rotating jsonlog
    directory (`docs/plans/demo-richer-traces.md`'s workstream C).
  - `browser_in` is `otlp_in` over HTTP; the landing page's browser OTel SDK sends it a
    `documentLoad` span on every page load (`demo/app/browser/telemetry.js`).
  - `syslog_out` isn't in the demo; its own unit/integration tests cover it fully. The demo isn't
    meant to stay exhaustive over every component.

### Harnesses

- **Load testing**: `crates/logit-perf` (bin `logit-perf`,
  `script/perf run|compare|attribute|flamegraph|list`) is the out-of-CI load-test harness. It
  spawns the real release `logit run <config>` process against `perf/scenarios/*.yaml`, driven by
  `generate_in` into `null_out`, and measures events/s, CPU µs/event (the regression gate), and
  peak RSS. `attribute` decodes a temporary `internal` telemetry leg into a per-node time
  breakdown; `flamegraph` drives `perf`/`inferno` in a throwaway image. It's built and runnable by
  hand, deliberately not wired into `script/cibuild` or any schedule
  ([ADR `load-test-harness`](docs/adr/load-test-harness.md),
  [docs/plans/load-test-harness.md](docs/plans/load-test-harness.md)).
- **Recorded numbers**: [docs/design/performance.md](docs/design/performance.md), kept current as
  the harness and the code evolve, measured on the disposable perf VM
  ([ADR `disposable-azure-perf-vm`](docs/adr/disposable-azure-perf-vm.md)) since 2026-09-20.
- **Data-shape survey**: `script/shape-survey` drives real traffic through `shape`; see
  [Where things live](#where-things-live).
- **Victoria interop**: `script/victoria-interop` checks the standard-protocol components against
  real VictoriaMetrics, VictoriaLogs, VictoriaTraces, and vmagent; see
  [Where things live](#where-things-live).

### Not yet built

- Credit-based flow control beyond one frame in flight, and QUIC, for the native transport
  (`docs/known-gaps.md`).
- Prometheus native histograms, skipped and counted in both directions.
- An Agent-equivalent Datadog trace processor (normalization, `_top_level` marking, sampling, a
  stats concentrator), so tracer spans could reach Datadog with no real Agent in the path
  ([docs/plans/datadog-relay.md](docs/plans/datadog-relay.md) §14).

## Environment

Everything runs in a container — **do not** assume Rust, LuaJIT, or `cargo` are on the host; they
usually aren't. Use `script/*`, not bare `cargo`:

| Command | What it does |
|---|---|
| `script/bootstrap` | Build the dev container image (run first, or after touching `Dockerfile.dev`) |
| `script/test [args]` | `cargo nextest run --workspace` |
| `script/lint` | `cargo clippy --workspace --all-targets -- -D warnings` |
| `script/format [--check]` | `cargo fmt --all` |
| `script/check [test args]` | Routine format-check + lint + workspace tests, in one dev container |
| `script/schema` | Regenerate `schema/logit.schema.json` — run after any `logit-config` type change, and commit the result |
| `script/validate` | Manually run `logit validate` over every shipped config (`demo/`, `fixtures/`, `perf/scenarios/`, `tools/shape-survey/configs/`, `tools/victoria-interop/logit-*.yaml`); ordinary tests enforce this too |
| `script/bench [filter]` | `cargo bench -p logit-bench` — throughput + per-benchmark allocation counts. Not part of `cibuild` |
| `script/perf run\|compare\|attribute\|flamegraph\|list` | Out-of-CI load-test harness (`crates/logit-perf`, `docs/adr/load-test-harness.md`) — spawns the real `logit` binary against `perf/scenarios/*.yaml`. A `udp-statsd*` scenario is instead driven over a real socket from its `perf/load/` sidecar spec, needs `--pin-sender`/`--pin-child`, is denominated over events *delivered*, and takes `--verify` (a strict zero-drop self-check) / `--rate-scale` (moves the operating point without editing a spec) ([ADR `udp-intake-batching-and-socket-visibility`](docs/adr/udp-intake-batching-and-socket-visibility.md)). `attribute` decodes a temporary `internal` dump into a per-node time breakdown; `flamegraph` runs `perf record` in its own throwaway image (`crates/logit-perf/Dockerfile`, not `Dockerfile.dev`). Not part of `cibuild` |
| `script/shape-survey [producer ...]` | Out-of-CI data-shape capture harness (`tools/shape-survey/`, [docs/plans/data-shape-survey.md](docs/plans/data-shape-survey.md)) — drives real traffic through the `shape` component and summarizes what the events look like. Producers are discovered by globbing `tools/shape-survey/producers/*.sh`, one file each, six today: `interop` replays `testdata/interop/` and is the instrument's acceptance test (it must reproduce the statsd corpus's independently-counted numbers), `exporters` scrapes six official Prometheus exporters in default configuration through one `prometheus_in` per target (where `logit.shape.attributes` reads as labels per series and `logit.shape.batch.events` as series per scrape; cAdvisor is deliberately not among them — it needs `--privileged`), `applogs` runs eight real logging libraries at pinned versions in tiny HTTP apps through `tail_in` plus one auto-instrumented Django exporting OTLP straight to `otlp_in`, `oteldemo` runs the OpenTelemetry Demo at a pinned tag through its own Collector (~10 SDK languages at once, and ~20 GB of RAM), `hostagents` runs collectd and Telegraf in default configuration over five wires at once (collectd binary, carbon ×2, a scrape, OTLP/gRPC), and `demo` taps `demo/`'s own stack without modifying it. Each states its own representativeness line and its own caveats — `tools/shape-survey/README.md`'s "Producers" table has all six side by side. Every run is namespaced `shape-survey-<producer>-…`, so **two invocations can run concurrently** on one daemon (`SHAPE_SURVEY_SKIP_IMAGE=1` for the second). `tools/shape-survey/combine.py` folds several runs into one cross-producer table set. Runs on the host and drives docker, like `script/record-fixtures`. Not part of `cibuild` |
| `script/victoria-interop` | Out-of-CI interop harness (`tools/victoria-interop/`, [docs/plans/victoriametrics-interop.md](docs/plans/victoriametrics-interop.md)) — runs pinned VictoriaMetrics, VictoriaLogs, VictoriaTraces, and vmagent in compose beside one `logit` per leg (`tools/victoria-interop/logit-*.yaml`), queries each backend for what arrived, and prints one `PASS`/`GAP`/`FAIL` row per leg: remote-write 1.0 (Snappy and zstd) and 2.0, vmagent scraping `prometheus_out`, `influxdb_out`, `graphite_out`, `otlp_out` over HTTP and gRPC, `syslog_out`, `prometheus_in` scraping `/federate`, and vmagent remote-writing into `prometheus_in`. The plan's "Findings" section records a run. One compose project, `victoria-interop`, so one run at a time per daemon (`VICTORIA_INTEROP_SKIP_IMAGE=1` reuses the image). Runs on the host and drives docker, like `script/shape-survey`. Not part of `cibuild` |
| `script/audit` | `cargo-deny` + `cargo-audit` |
| `script/cibuild` | The exact sequence CI runs, in order — run this before opening a PR |
| `script/console` | Interactive shell in the dev container, for anything not covered above |
| `script/image [tag]` | Build the production runtime image (`Dockerfile`, not `Dockerfile.dev`) |
| `script/demo [compose args]` | Run the self-contained demo stack (`demo/`) — the release image, no dev container |
| `script/vm up\|shell\|status\|down\|build\|push\|pull` | Create, use, and destroy a disposable Azure VM for perf measurement (`docs/adr/disposable-azure-perf-vm.md`). `down` deletes the resource group — the only way back to $0. **The operator runs `up` and `down`**; an agent does the measuring in between — `build <ref\|dir\|tarball>...` stashes another binary to measure (`perf/bins/<slug>/logit`, fed to `logit-perf run --logit-bin`), `push`/`pull` `scp` files to/from it |
| `script/unsafe-check miri\|careful\|inject\|all\|shell` | Out-of-CI, nightly-only verification of the codebase's three raw-`libc` `unsafe` call sites (`crates/logit-inputs/src/udp.rs`'s `recvmmsg`, `crates/logit-pipeline/src/sockstat.rs`'s `getsockopt`, `crates/logit-inputs/src/tail/watch.rs`'s hand-rolled inotify — [ADR `out-of-ci-unsafe-verification`](docs/adr/out-of-ci-unsafe-verification.md)) in its own throwaway image (`tools/unsafe-check/Dockerfile`, not `Dockerfile.dev`). `miri` runs the pure-helper tests miri can actually execute (it has no shims for `recvmmsg`/`inotify`); `careful` runs the real crates under `cargo-careful`'s debug-assertion std; `inject <strace-inject-spec> [-- <cargo test args>]` forces an errno (EINTR/EAGAIN/…) via `strace -e inject=`, needing `--cap-add SYS_PTRACE`. Not part of `cibuild` |

All default to `sudo docker`; `DOCKER=docker` or `DOCKER=podman` overrides — except `script/vm`,
which talks to `az` and `ssh` on the host and never to a local Docker daemon. See
[ADR `containerized-development`](docs/adr/containerized-development.md) and
[ADR `scripts-to-rule-them-all`](docs/adr/scripts-to-rule-them-all.md).

## Workflow

Work happens on a branch, landed via pull request — never commit straight to `main`. Run
`script/cibuild` locally before opening one; it's the same sequence `.github/workflows/ci.yml`
runs, so a clean local run means a clean CI run.

Use `script/check` for the ordinary edit/verify loop. Cargo downloads and compiled artifacts use
shared project-wide Docker volumes across worktrees; do not remove them as part of routine cleanup.

**The shared `logit_target_cache` volume can serve one worktree a crate compiled from another.**
Every worktree mounts at `/work` inside the dev container, so cargo's fingerprints can't tell two
checkouts of the same crate apart, and a test run in one worktree can link a `logit-proto` or
`logit-core` built from a sibling branch. The symptom is a failure no change on the branch
explains: an allocation pin off by one on a docs-only branch, an interop test rejecting a fixture
the branch never touched. Before treating such a failure as real, rerun with a private target
dir, `CARGO_TARGET_DIR=/work/perf/results/<slug>/target` (gitignored), through a plain
`docker run` of the dev image with the worktree bound at `/work`; if that run passes, the shared
cache was stale. CI builds from a clean cache and doesn't have the problem.

**Before opening a PR, sweep for the two things review catches most often**: grep every comment
line the branch added for the words the comment rule bans (`exactly`, `actually`, `genuinely`,
`deliberately`, `simply`, `just`, `on purpose`, `load-bearing`), and ask whether the branch made
a decision a maintainer would look for in an ADR (a wire form, a transport, a mode, a loss
semantic) and recorded it only in a module doc. Both rules are in
[Conventions to hold to](#conventions-to-hold-to); they are the two a reviewer flags on most
PRs that skip this pass.

**To bring a branch with an open PR up to date with `main`, `git merge origin/main` — don't
rebase.** A rebase rewrites the branch's commits, which means a force-push to update the PR; that's
disruptive for an open PR (review-comment associations, anyone else with the branch checked out)
for no real benefit here. A merge commit costs nothing extra and pushes normally.

### Branches and PR titles

Work that lands as a series of related PRs (a workstream) is grouped by a **stream key**: one
short lowercase token (`[a-z0-9-]`, abbreviations welcome) picked when the work is planned, before
the first branch is cut, and not already in use by another stream. It lives in the branch names
and PR titles themselves; nothing else has to record it (a plan in `docs/plans/` may mention it,
but most workstreams won't have one). The key and the workstream number then appear identically
in the branch and the PR title:

| Kind | Branch | PR title |
|---|---|---|
| Workstream PR | `<key>/w<N>[<letter>]` | `<branch>: <summary>` |
| Follow-up to a finished stream | `<key>/<slug>` | `<branch>: <summary>` |
| One-off outside any stream | `<type>/<slug>` | `<type>(<scope>): <summary>` |

```
graphite/w0                graphite/w0: ADR and plan for a lossless Graphite/Carbon relay
graphite/w2                graphite/w2: graphite_in — carbon plaintext/pickle over UDP and TCP
graphite/w4a               graphite/w4a: recorded Graphite interop fixtures
targets/review-followups   targets/review-followups: route/target review follow-ups
fix/ci-test-flakes         fix(cli): remove two CI-only test races
```

- **Workstream branches carry no `feat/` prefix and no trailing slug** — the PR summary already
  says what W2 is, and `<key>/` is the namespace: `git branch --list 'graphite/*'` lists the
  stack, and a PR list sorted by title reads as one. Letters (`w4a`, `w4b`) are sibling PRs meant
  to land in parallel off the same parent. Don't repeat the number at the end of the title
  (`… (W2)`) — it's already the prefix.
- **The PR title is the branch name, a colon, and the summary** — no conventional-commit type.
  The type still goes on every commit message (`feat(inputs): …`); merges to `main` are real merge
  commits, so the PR title never becomes a commit subject.
- **One-off work keeps conventional-commit style throughout.** `type` is one of
  `feat|fix|docs|test|chore|perf`; untyped branches (`dev-loop-speed`, `worktree-…`) are out.
- **Stacked PRs:** `<key>/w<N>` branches from its parent workstream's branch and its PR targets
  that branch; retarget to `main` once the parent merges. Stack-internal merge commits are
  `merge <key>/w<N> into <key>/w<M>`.

Merged branches and PRs are never renamed to fit.

## Conventions to hold to

- **A new design decision worth remembering gets an ADR** (`docs/adr/<slug>.md`, copied from
  [`docs/adr/TEMPLATE.md`](docs/adr/TEMPLATE.md)) — not just a comment or a PR description. Name
  the file after the decision, not a number: parallel branches racing for "the next number" was a
  recurring source of merge churn (see [`docs/adr/README.md`](docs/adr/README.md) for the full
  index and the `created`/`updated` frontmatter that orders it). Check the existing ADRs before
  re-deciding something they already settled. `docs/plans/` follows the same convention.
- **`rustfmt.toml`/`clippy.toml` are enforced**, not advisory — `script/cibuild` fails the build on
  either. Run `script/format` before committing rather than hand-formatting.
- **Every config type derives `Serialize + Deserialize + JsonSchema` together**
  ([ADR `config-yaml-jsonschema`](docs/adr/config-yaml-jsonschema.md)) — the published schema is generated from
  the Rust types specifically so it can't drift. If `schemars` needs a hint `serde` doesn't give it
  (as with the hand-rolled `Duration` codec in `logit-config`), add `#[schemars(with = "...")]`
  alongside `#[serde(with = "...")]` rather than dropping the derive.
- **Stub code says so.** Unimplemented pieces are `todo!()` with a comment pointing at the design
  doc section and, where relevant, what to build next — see `logit-script`, `logit-proto`, and the
  `statsd`/`influxdb` stubs. Follow that pattern for new stubs rather than silently returning a
  default.
- **A config file is always read through `logit_cli::config::load`**, never a bare
  `std::fs::read_to_string` + `serde_norway::from_str` — that's what resolves `!env` and rejects an
  unknown YAML tag (ADR `env-yaml-tag`); a call site that bypasses it silently loses both.
- **A comment says what a maintainer would otherwise get wrong**: an invariant, a hidden
  constraint, a workaround, a wire fact. It explains why, and what only when the code can't.
  Concretely:
  - No history. Nothing about what the code used to do, or which workstream, PR, review, or
    date changed it. Git has that; an ADR link carries a long why.
  - No hedges or intensifiers ("exactly", "actually", "genuinely", "deliberately", "simply",
    "just", "on purpose", "load-bearing").
  - Cite a doc by path and heading, never by line number, and code by item name.
  - One copy of a list. A module doc that describes behavior (validation rules, a codec's
    mapping table and permitted normalizations) is the canonical copy; docs, tests, and examples
    summarize it and point at it.
  - A module doc opens with what the module is for in a sentence or two, then the facts a
    maintainer needs at the code. The rest is a pointer.
  - `// SAFETY:` comments are required by clippy. Config field docs in `logit-config` are
    operator docs rendered into `schema/logit.schema.json`, so they're written for an operator:
    no rule numbers, no ADR paths, no cross-references to other doc comments.
  - Wire samples, protocol grammar, and error strings are verbatim material. Leave them as is.

## Design constraints that aren't optional

These come directly out of [docs/design/lua-api.md](docs/design/lua-api.md) and
[docs/design/data-model.md](docs/design/data-model.md) — violating them means redoing work later,
not a style preference:

- **`mlua::Lua` is neither `Send` nor `Sync`.** One Lua VM per pipeline worker, no implicit shared
  mutable state across workers. `ScriptWorker` in `logit-script` enforces this with a
  `PhantomData<*const ()>` marker — don't remove it to make something compile.
- **Events reach Lua through a proxy (`EventProxy`, userdata + metamethods), not a converted
  table.** The whole point is avoiding a full table conversion on every stage for every event —
  don't "simplify" this back into `event:to_table()`-by-default. `Event.new(t)` is the one
  opt-in full-table path, in the other direction: a script pays for it only where it calls it
  (`docs/design/memory.md` §2's `Event.new` rows), and every other `lua:` allocation pin is
  unchanged by its existence.
- **Metric kinds must stay mergeable.** `Distribution` needs a sketch with a real error bound
  (`DDSketch`, not a naive percentile), `Set` needs a real union (`HyperLogLog`) — this is what
  makes the split-collection topology in `docs/OVERVIEW.md` correct rather than approximate.
  `logit-core::sketch::DdSketch` is a hand-rolled DDSketch with a working `merge`
  (`crates/logit-transforms`' `aggregate` is its first caller) that keys bins exactly as the
  Datadog Agent does, so a sketch relays to and from Datadog bin-for-bin
  ([ADR `datadog-agent-and-intake-relay`](docs/adr/datadog-agent-and-intake-relay.md)); its
  mapping is part of the wire, so don't change `Mapping::agent`'s constants. `HyperLogLog` wraps
  the `cardinality-estimator` crate, also a real, mergeable sketch — don't replace either with a
  non-mergeable shortcut.
- **`statsd_in -> statsd_out`, `otlp_in -> otlp_out`, `syslog_in -> syslog_out`, `prometheus_in ->
  prometheus_out`, `collectd_in -> collectd_out`, `graphite_in -> graphite_out`, `datadog_in ->
  datadog_out`, and `datadog_trace_in -> datadog_trace_out` must each be a lossless relay**,
  modulo a named list of permitted normalizations (batching, tag reordering, a sink-configured
  dialect change) — [ADR `lossless-transit`](docs/adr/lossless-transit.md). A
  decoder never pre-summarizes what an explicit `aggregate`/Lua stage should decide about, and a
  field a protocol can carry that `Event` can't represent is tracked debt
  ([`docs/plans/lossless-transit.md`](docs/plans/lossless-transit.md)), not an accepted codec
  limitation — don't add a new lossy mapping without checking that plan and the survey it's built
  on ([`docs/design/telemetry-landscape.md`](docs/design/telemetry-landscape.md)) first.
- **The wire encoding is decided: hand-rolled, shipped as `logit_proto::native`** — a four-arm
  bake-off (`crates/logit-bench/src/bakeoff/`) settled it against `rkyv`, `postcard`, and OTLP
  itself; see [ADR `native-wire-format-encoding`](docs/adr/native-wire-format-encoding.md) and
  `docs/design/wire-protocol.md`. The disk-backed sink buffer over these frames
  ([ADR `disk-backed-sink-buffer`](docs/adr/disk-backed-sink-buffer.md)), built directly rather
  than through `logit_proto::buffer::Buffer<T>` -- that trait's role narrowed to `InMemoryBuffer`
  alone, since its sync/`&mut self`/generic shape turned out to be the wrong seam for an async,
  file-backed implementation. The `logit_out`/`logit_in` connection/handshake state machine is
  built ([ADR `native-transport-handshake-and-ack`](docs/adr/native-transport-handshake-and-ack.md));
  still open: credit-based flow control beyond one frame in flight, and QUIC -- don't design those
  in passing; they're real future work, not yet started.
- **Memory behavior is measured, not assumed** — `docs/design/memory.md` records what every
  pipeline stage allocates and what `Event` costs to move, and both are enforced by tests:
  `crates/logit-core/tests/type_sizes.rs` asserts exact `size_of`s, and
  `crates/logit-bench/tests/allocations.rs` asserts exact allocation counts per stage. They're
  exact equality on purpose. They are tripwires, not a score: the one attempt to *optimize* an
  allocation count — pre-sizing `AttrMap`'s spill — won its micro-benchmark and ran 8–17% slower
  end to end on `json`, so a sizing or allocation-strategy change needs a real binary measured per
  signal class on the perf VM before it is believed
  ([ADR `event-sizing-and-allocation-strategy`](docs/adr/event-sizing-and-allocation-strategy.md)).
  **When one fails, that's the test working** — decide whether the
  change is worth it, then update the constant *and* `docs/design/memory.md`'s table in the same
  commit. Don't relax an assertion to a `<=` bound to make it pass; that removes the only thing
  stopping `Event` from quietly growing.
- **Benchmark and test fixtures never depend on a running service.** No nginx, no InfluxDB, no
  container — `crates/logit-bench/src/fixtures.rs` holds `const` wire-format literals and
  directly-constructed events, and components are called directly rather than through the runtime
  (`docs/design/memory.md`'s "Fixtures" section has the pattern, including why a literal should
  carry provenance). Standing up a service against real software to *inform* a fixture is fine and
  encouraged; committing a fixture that needs one is not.
- **Don't generalize a measurement from one event shape.** `Event` carries any combination of log,
  metrics, and span, and `logit` targets logs-only, metrics-only, traces-only, and mixed pipelines
  alike (`docs/OVERVIEW.md`). The fixtures cover logs-only, wide-JSON, distribution-heavy, and
  span shapes alongside the original mixed one, but that closes the *measurement* gap, not the
  sizing *decisions* those numbers feed — see `docs/design/memory.md` §0 and §8 before treating any
  one number as settled across workloads. [`docs/design/data-shapes.md`](docs/design/data-shapes.md)
  is what real producers actually send — a desk survey plus live captures measured by the `shape`
  component — and its headline is that per-event width is **bimodal by signal** (metric events at
  0–6 attributes, parsed structured logs at 9 and up, spans across both), so a number that is right
  for one leg is wrong for another. Reach for it before picking a "representative" shape, and mind
  its own §7: none of it is production traffic.

## Where things live

```
crates/
  logit-core        internal event model: Event, Value, Resource, metric kinds, interner, self-telemetry
  logit-config      YAML config types + generated JSON Schema
  logit-script      LuaJIT embedding (mlua), the Event proxy
  logit-proto       codec traits, native wire format, output buffering
  logit-pipeline    Input/Output/Transform/Router traits, Fanout, graph resolution+validation, node runtime, sockstat (per-socket kernel counters)
  logit-inputs      per-protocol listeners implementing logit-pipeline::Input; statsd (v0.1 target), syslog, graphite, collectd, otlp, datadog (datadog_in), datadog_trace (datadog_trace_in), prometheus, tail (tail_in/docker_in), logit (logit_in), internal (self-telemetry), generate_in (load-test event generator), shared udp/tcp/unix drivers
  logit-outputs     per-protocol sinks implementing logit-pipeline::Output; InfluxDB (v0.1 target), stdio, file, syslog, statsd, otlp, prometheus, collectd, graphite, datadog (datadog_out), datadog_trace (datadog_trace_out), logit (logit_out), null_out (load-test discard sink)
  logit-transforms  native transforms implementing logit-pipeline::Transform; aggregate (v0.1 target), json, csv, kv_metrics, keep, remove, set, trace_context, scale, has_signal, keep_signals, drop_signals, has_attributes, drop_attributes, has_provenance, drop_provenance, keep_values, logfmt, kv, regex, shape (the fan-out-tapped shape observer), flatten (dotted-key expansion of a nested attribute), http_access (access-log normalization onto OTel semconv), sample (consistent, keyed sampling on a frozen XXH64 hash), route (implements logit-pipeline::Router)
  logit-cli         the `logit` binary: the kind → implementation registry, `Command::{Schema,Validate,Run,Graph}`
  logit-bench       dev-only: allocation-count tests + divan throughput benches (docs/design/memory.md)
  logit-perf        dev-only, publish = false: the load-test harness binary (`logit-perf`, `script/perf`) -- spawns the real logit-cli binary against perf/scenarios/*.yaml (docs/adr/load-test-harness.md, docs/design/performance.md)
```

`perf/scenarios/*.yaml` are the harness's own shipped configs (ordinary `logit` YAML, a
`generate_in` listener into `null_out` or a real sink), covered by `script/validate` and
`every_shipped_config_loads_and_validates` alongside `demo/`/`fixtures/`; `perf/results/` is
where `script/perf run`/`attribute`/`flamegraph` write their (gitignored) output.

`tools/shape-survey/` is the data-shape capture harness `script/shape-survey` drives ([docs/plans/data-shape-survey.md](docs/plans/data-shape-survey.md)): `lib.sh` (shared docker plumbing, nothing producer-specific), stdlib-only `replay.py`/`summarize.py` (which parses `stdio_out`'s human render — the whole `render_value` grammar, arrays and maps included, under `--self-test`)/`check_interop.py`/`combine.py` (the cross-run report), one file per producer under `producers/` (`interop`, `exporters`, `applogs`, `oteldemo`, `hostagents`, `demo` — see that directory's README for what each runs and what its numbers are worth), and capture configs under `configs/` — which join `script/validate` and `every_shipped_config_loads_and_validates` alongside `demo/`/`fixtures/`/`perf/scenarios/`. Runs land in `perf/results/shape-survey/<producer>/<timestamp>/` (gitignored); raw traffic never enters the repo and nothing there writes under `testdata/`. Everything a run creates is namespaced by producer (`shape-survey-<producer>-net`, `shape-survey-<producer>-<suffix>` containers and compose projects), so two producers can be captured at the same time on one shared daemon and cleanup can only ever touch its own. Every producer states a one-line **representativeness** in `provenance.txt`, which `summarize.py` prints as the banner above every table and `combine.py` repeats on every row — the `demo` producer's numbers in particular are a harness exercise, not evidence of production shape, and some of the formats it measures were authored in this repo.

`tools/victoria-interop/` is the interop harness `script/victoria-interop` drives: `compose.yaml` (the pinned Victoria images, vmagent, and one `logit` service per leg), one `logit-<leg>.yaml` per leg (which join `script/validate` and `every_shipped_config_loads_and_validates`), `vmagent-scrape.yaml`, and the stdlib-only `check.py` that queries each backend and replays committed `testdata/interop/prometheus/` captures at VictoriaMetrics. Runs land in `perf/results/victoria-interop/<timestamp>/` (gitignored); [its README](tools/victoria-interop/README.md) has the rest.

`perf/load/*.yaml` are the **sidecar load specs** for real-socket scenarios
([ADR `udp-intake-batching-and-socket-visibility`](docs/adr/udp-intake-batching-and-socket-visibility.md)):
a `udp-statsd*` scenario has no `generate_in` at all, and `logit-perf` sends it real UDP traffic
from the matching `perf/load/<scenario>.yaml` instead. They are a separate directory, not more
files under `perf/scenarios/`, because both `script/validate` and the shipped-config test glob that
directory unconditionally — anything in it has to be a valid `logit` config. `perf/load/README.md`
has the spec format and how its traffic model was calibrated against the real-client capture under
`testdata/interop/statsd/`.

`logit-inputs`/`logit-outputs`/`logit-transforms` depend on `logit-pipeline` for their trait, not
the other way around (`docs/design/pipeline-graph.md`'s "Crate layout" section) -- this is what
keeps the pipeline runtime from having to know about any concrete protocol or transform. A new
protocol implements `logit_proto::Decoder` on the listener side and, on the sink side, whichever
of the three encoder shapes its wire format is: `logit_proto::Encoder` (one opaque blob per
batch — `influxdb` is the template), `logit_proto::FramedEncoder` (one framed message per
record into a `logit_proto::MessageBuf`, with per-message drop accounting — `syslog`/`statsd`,
[ADR `framed-encoder`](docs/adr/framed-encoder.md)), or `logit_proto::SignalEncoder` (one
payload per signal — `otlp`); plus `logit_pipeline::Input` or `logit_pipeline::Output`, and a
variant in `logit_config`'s `ComponentKind`. (`prometheus` is the one pair outside all of these,
by design — its ADR says why.) A new native transform implements `logit_pipeline::Transform`,
following `logit-transforms::Aggregator`. A new router implements `logit_pipeline::Router`
(`crates/logit-pipeline/src/router.rs`), following `logit-transforms::Route`
([ADR `target-components`](docs/adr/target-components.md)).
