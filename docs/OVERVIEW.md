# logit overview

## What it is

`logit` is a single binary that receives logs, metrics, and traces over many wire protocols,
converts them into one internal event model, transforms them, and sends the results to many
destinations. The same binary runs as a sidecar next to one workload, as a host agent for
everything on a machine, or as a central aggregator that other `logit` nodes forward to. The config
file decides which; there is no separate build.

## Why

Telemetry pipelines usually force a choice between fast but rigid tools, such as statsd and
Telegraf, and flexible but heavy ones, such as a full stream-processing engine. `logit` aims between
them: a lightweight Rust core with an efficient event model, plus an embedded LuaJIT scripting layer
fast enough to reshape, enrich, aggregate, and route events directly in config, without a separate
processing tier.

The second problem is **splitting collection from processing**. Running a thin collector at the
edge and a heavier processor centrally is common, but it usually means gluing two different tools
together through a lossy intermediate format. `logit` has a first-class, efficient, lossless
transport between nodes: a native wire protocol that carries the internal event model as is.
OpenTelemetry Protocol (OTLP) is the interoperable option at the edges.

Relaying a protocol to itself is lossless. Each of these pairs is a transparent relay:

- `statsd_in` to `statsd_out`
- `otlp_in` to `otlp_out`
- `syslog_in` to `syslog_out`
- `prometheus_in` to `prometheus_out`
- `collectd_in` to `collectd_out`
- `graphite_in` to `graphite_out`

A relay can regroup events into different batches and apply a short, named list of normalizations,
but it drops no information. Summarizing, such as summing counters over a window, happens only in a
stage you add on purpose. See [ADR `lossless-transit`](adr/lossless-transit.md).

## Scope

- **Ingest:** statsd and DogStatsD, collectd, Graphite (plaintext and pickle), syslog (RFC 3164
  and 5424), OTLP logs, metrics, and traces, Prometheus scraping and remote-write, and the native
  `logit` protocol. Listeners use UDP, TCP, or TLS, depending on the protocol. File tailing handles
  rotation and keeps a checkpoint (`tail_in`), including Docker's json-file container logs,
  enriched with each container's identity without access to the Docker socket (`docker_in`). See
  [ADR `file-tailing-and-docker-json-logs`](adr/file-tailing-and-docker-json-logs.md).
- **Transform:** built-in parsers for common line formats (JSON, logfmt, `key=value`, CSV, regular
  expressions, and web-server access logs) run ahead of user logic. User logic is Lua, written
  inline in the YAML or loaded from a `.lua` file. Built-in components also cover stateful metric
  aggregation (counters, gauges, sets, and distributions with percentiles), consistent sampling,
  filtering, and routing. Every stage is opt-in.
- **Emit:** the same protocols as ingest, except file tailing, plus InfluxDB 2.x, standard output,
  and rotating files. The native `logit` protocol forwards events between nodes.
- **Configuration:** YAML, validated against a JSON Schema generated from the Rust config types, so
  the schema can't drift from what the binary accepts. A config is one flat graph of named
  **components**, not a fixed inputs, transforms, and outputs structure. Each component has a
  `type` and a `sources` list naming the components it reads from. A listener has no sources, a
  sink is nobody's source, and any component in between can feed as many downstream components as
  need it. See [docs/design/pipeline-graph.md](design/pipeline-graph.md) and
  [ADR `component-graph-configuration`](adr/component-graph-configuration.md).

## Non-goals

`logit` is not a storage engine, a query layer, or a dashboarding or alerting tool. It moves and
reshapes telemetry, then hands the result to systems that do those jobs, such as InfluxDB,
Prometheus, Grafana, a security information and event management (SIEM) system, or another
`logit`.

## Positioning

The closest prior art is Vector, the OpenTelemetry Collector, Fluent Bit, and Telegraf. `logit`
makes a narrower, more opinionated bet than any of them:

- A fast, general-purpose scripting language, instead of a custom DSL or Collector-style Go plugins
  that require a rebuild.
- Built-in parsers for the common cases, in front of that language.
- An event model and wire protocol designed from the start for efficient splitting of collection
  and processing across nodes.

`logit` runs on a private network, behind a trust boundary the operator owns. It is built to
survive accidental data, such as a misconfigured sender or a corrupt file, not a malicious peer;
see [ADR `deployment-threat-model`](adr/deployment-threat-model.md).
