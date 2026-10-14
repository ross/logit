---
created: 2026-10-14
updated: 2026-10-14
---

# Process hostname: one resolved name, `host.name` on `internal`'s resource

## Status
Accepted

## Context

Several `logit` processes writing into one metrics backend were indistinguishable. `internal`'s
resource carried only `service.name = logit`, so every instance's `logit.*` series shared one label
set (`docs/known-gaps/telemetry.md` listed this as a residual). Nothing in the service read the
hostname, and the sinks that need one (`syslog_out`, `splunk_hec_out`, `datadog_out`, `collectd_out`)
each take a `hostname:` of their own or none.

[ADR `operator-declared-resource-attributes`](operator-declared-resource-attributes.md) says
`logit`'s own code may not invent an identity. A hostname isn't invented: it is a fact about where
the process runs, discovered the way `docker_in` discovers `container.*`
([internal-telemetry.md](../design/internal-telemetry.md)'s "Resource identity").

## Decision

`logit run` resolves one process hostname at startup and passes it to every component's build. It
is the top-level `hostname:` if set, otherwise the kernel hostname (`uname(2)`'s nodename, through
`rustix`'s safe wrapper, the value `gethostname(2)` returns). `internal` stamps it as `host.name` on
its resource beside `service.name`.

- **`$HOSTNAME` isn't read.** In Docker and Kubernetes the runtime sets it to the kernel hostname,
  so it adds nothing. On a host under systemd it's usually unset, and when it differs from the
  kernel value it's stale or inherited (`sudo -E`, an image `ENV`).
- **No DNS lookup.** `gethostbyname`/`getaddrinfo` canonicalization would make startup depend on a
  resolver and return different names on different networks.
- **A non-UTF-8 nodename converts lossily.** An empty result fails startup (exit 1) with a message
  that names `hostname:`.
- **`logit validate` doesn't resolve it.** Only `run` needs the value. A `hostname:` that is empty
  or whitespace-only is a graph error (rule 88), so `validate` still catches that.
- **No process-global.** `logit_pipeline::host::resolve` runs in `prepare` and the result is a
  parameter to `build_spec`, so tests choose their own hostname.

`logit` claims `host.name` only for its own telemetry (`internal`). A follow-up makes
`syslog_out`, `splunk_hec_out`, and `datadog_out` fall back to the process hostname for events that
carry no host of their own (see the amendment below). That fallback fills a missing value and never
overwrites a host an event carries. `collectd_out` keeps its `no_host` drop: its ADR's stance that inventing a host
merges every unlabeled sender into one host's metrics stands. A relay's events reach `collectd_out`
with the origin's `collectd.host`, and only `internal`'s events, which now carry `host.name`, gain a
host.

## Alternatives considered
- **Read `$HOSTNAME` first.** Rejected: see above.
- **Resolve through DNS to a fully qualified name.** Rejected: see above.
- **`libc::gethostname` directly.** Rejected: the codebase caps raw-`libc` `unsafe` call sites
  ([ADR `out-of-ci-unsafe-verification`](out-of-ci-unsafe-verification.md)), and `rustix` is
  already in the dependency graph.
- **A `set` transform on `internal`'s output.** Works today, but every config repeats it and none
  of them can read the kernel hostname.
- **`service.instance.id` instead.** A random id per start re-keys every series on each restart.
  A hostname is stable across restarts of the same host.

## Consequences

- Every `logit.*` series from `internal` gains one constant `host.name` label in `prometheus_out` and
  `influxdb_out`, because both fold resource attributes into labels and tags.
- In Kubernetes a pod name changes on each rollout, so series re-key per pod. That's inherent to
  telling instances apart; set `hostname: !env NODE_NAME` to use the node name instead.
- A config that relied on `internal` events having no `host.name` changes behavior at three sinks,
  because each reads `host.name` from the event or its resource:
  - `collectd_out` tries `collectd.host`, then `host.name`, before its configured `hostname:`. So
    `internal -> collectd_out` sends the process hostname even when the sink sets
    `hostname: logit-relay`, and a sink with no `hostname:` sends instead of dropping with
    `no_host`.
  - `datadog_out` maps `host.name` to the series and sketch `host`, so `internal`'s metrics now
    carry a host, which can register a new host in Datadog.
  - `splunk_hec_out` sets the envelope `host` from the resource's `host.name`, where it used to
    leave Splunk's default.

  To get a different name, set the top-level `hostname:`. To keep `internal`'s events host-less,
  place a `remove` of `host.name` after `internal`.
- `rustix` joins `logit-pipeline`'s direct dependencies (the `system` feature). It was already
  transitive.

## Amendment: three sinks fall back to the process hostname (2026-10-14)

`build_spec` passes the process hostname to `syslog_out`, `splunk_hec_out`, and `datadog_out`.
Each sends it as the host of an event that carries none, in this order: the event's own host, then
the sink's `hostname:` (`syslog_out` only), then the process hostname.

| Sink | The event's own host | Where the process hostname goes |
|---|---|---|
| `syslog_out` | `syslog.hostname` | HOSTNAME, unless the sink sets `hostname:` |
| `splunk_hec_out` | resource `host.name` | the envelope `host` |
| `datadog_out` | `host.name`; for an event or service check, `statsd.*.host` first; for a log, `hostname` first | a series' `host` resource, a v1, distribution, or sketch `host`, a log's `hostname`, an event's `host`, a check's `host_name` |

Each relay pair gains a permitted normalization: an item that arrived with no host leaves with
the relay's hostname. The Consequences' recipe for host-less `internal` events, a `remove` of
`host.name` after `internal`, now holds only for `collectd_out`: the other three sinks fill the
removed host back in with the same name. Trace and stats hostnames in `datadog_out` and everything in
`datadog_trace_out` stay verbatim relay metadata. `collectd_out` is unchanged, for the reason in
the Decision above.
