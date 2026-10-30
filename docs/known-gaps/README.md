# Known gaps

Known rough edges in things already built. Check here before "fixing" something that looks
broken. Each entry either has a matching `todo!()` or doc-comment pointer in the code, or is small
enough to describe fully here. This isn't a roadmap; the [project overview](../OVERVIEW.md) has
the planned scope.

Each entry starts with the gap in bold, then its consequence, then the workaround or revisit
trigger. When a gap closes, delete its entry and fix any doc or code comment that points at it;
git history keeps the old text. When only part of a gap closes, rewrite the entry to state what's
still open.

A limit that has a decision and a doc isn't a known gap, even when it costs an operator something.
Its ADR is the record, and the operator's workaround goes in [Deploying `logit`](../deploying.md)
or the matching guide ([Datadog](../datadog.md), [Splunk](../splunk.md),
[HTTP access logs](../http-access-logs.md)); a non-goal under the threat model is listed in
[ADR `deployment-threat-model`](../adr/deployment-threat-model.md). An entry here is work still
open: a deferred fix, a revisit trigger, unverified behavior, or residual lossless-transit debt.

| File | Covers |
|---|---|
| [runtime.md](runtime.md) | the pipeline runtime and graph, the event model and interner, config and CLI, and the admin endpoint and release image |
| [native-hop.md](native-hop.md) | the native wire format, `logit_in`/`logit_out`, and sink buffering |
| [intake.md](intake.md) | UDP intake, TLS, and connection lifecycle across listeners and sinks |
| [mappings.md](mappings.md) | what each sink does with each metric kind and payload it can't carry natively |
| [statsd.md](statsd.md) | `statsd_in` and `statsd_out` |
| [datadog.md](datadog.md) | `datadog_in`, `datadog_out`, `datadog_trace_in`, and `datadog_trace_out` |
| [splunk.md](splunk.md) | `splunk_hec_in` and `splunk_hec_out` |
| [influxdb.md](influxdb.md) | `influxdb_in` and `influxdb_out`: unsupported routes and what a carried bucket costs |
| [kafka.md](kafka.md) | `kafka_out` and `kafka_in` |
| [syslog.md](syslog.md) | `syslog_in` and `syslog_out` |
| [otlp.md](otlp.md) | `otlp_in` and `otlp_out` |
| [prometheus.md](prometheus.md) | `prometheus_in` and `prometheus_out`, scrape and remote-write |
| [sinks.md](sinks.md) | `file_out` and `stdio_out`, the stream sinks' uncounted unacknowledged bytes, the multi-request sinks' retry memory, and kept-alive connections a destination closed first; `influxdb_out`'s gaps are in [influxdb.md](influxdb.md), [mappings.md](mappings.md), and [prometheus.md](prometheus.md) |
| [tailing.md](tailing.md) | `tail_in`, `docker_in`, and `replay_in` |
| [transforms.md](transforms.md) | predicates and `sample`, `shape`, `aggregate`, `cumulative_to_delta`, `http_access` and the access-log servers, and Lua |
| [telemetry.md](telemetry.md) | internal telemetry, internal spans, self-logging, and the load-test harness |
