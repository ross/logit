# logit

## What it is

`logit` is a single binary that ingests logs, metrics, and traces over many wire protocols,
normalizes them into one internal event representation, runs user-defined transforms over them, and
emits the results to many destinations. The same binary runs as a sidecar next to one workload, as a
host-level agent collecting for everything on a machine, or as a central aggregator that other `logit`
nodes forward to — the deployment shape is a config file, not a different build.

## Why

Telemetry pipelines tend to force a choice: fast and rigid (statsd, Telegraf), or flexible and heavy
(a full stream-processing engine). `logit` targets the middle — a lightweight Rust core with an
efficient built-in event model, plus a genuinely fast embedded scripting layer (LuaJIT) so users can
express real logic — reshaping, enriching, aggregating, routing — directly in config, without
standing up a separate processing tier.

The other piece existing tools handle awkwardly is **splitting collection from processing**. Running a
thin collector at the edge and a heavier processor centrally is common in practice, but usually means
gluing two different tools together with a lossy intermediate format. `logit` is designed so that a
`logit` talking to a `logit` is a first-class, efficient path — the same event model on both ends, an
internal wire protocol designed for it, and OpenTelemetry (OTLP) as the interoperable option at the
edges.

## Scope (v1 direction)

- **Ingest:** UDP/TCP listeners for statsd and DogStatsD-style tagged metrics, collectd, syslog
  (RFC 3164/5424), OTLP (logs/metrics/traces). File tailing for logs (rotation- and
  checkpoint-aware). More protocols added incrementally behind the same input trait.
- **Transform:** built-in parsers for common line shapes (JSON, logfmt, key=value, CSV, regex/grok),
  chainable in front of user logic. User logic is Lua, loaded inline from YAML or from referenced
  `.lua` files, run per pipeline stage. A built-in stateful aggregation processor for metrics
  (counters, gauges, sets, distributions/percentiles) that users opt into per pipeline.
- **Emit:** the mirror of ingest — the same protocols available as outputs, plus the native
  `logit`-to-`logit` protocol for forwarding between nodes.
- **Configuration:** YAML, validated against a JSON Schema published alongside the binary and
  generated directly from the Rust config types, so it cannot drift from what the binary actually
  accepts.

## What this is not (for now)

Not a storage engine, not a query layer, not a dashboarding or alerting tool. `logit` moves and
reshapes telemetry; it hands the result to systems (InfluxDB, Prometheus, Grafana, a SIEM, another
`logit`) that do those jobs.

## Positioning

Closest prior art is Vector, the OpenTelemetry Collector, Fluent Bit, and Telegraf. `logit`'s bet is
narrower and more opinionated than any of them: a real, fast, general-purpose scripting language
(rather than a bespoke DSL or Collector-style Go plugins that require a rebuild) sitting behind
built-in parsers for the 90% case, over an event model and wire protocol designed from the start for
efficient node-to-node splitting of collection and processing.
