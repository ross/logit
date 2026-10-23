---
created: 2026-10-14
updated: 2026-10-23
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
with the origin's host as `host.name`, and only `internal`'s events, which now carry `host.name`,
gain a host.

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
  - `collectd_out` tries `host.name` before its configured `hostname:`. So
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
| `datadog_out` | `host.name`, else a DogStatsD `host:` tag; for an event or service check, `statsd.*.host` first; for a log, `hostname` first, and `syslog.hostname` or `host` also count | a series' `host` resource, a v1, distribution, or sketch `host`, a log's `hostname`, an event's `host`, a check's `host_name` |

Each relay pair gains a permitted normalization: an item that arrived with no host leaves with
the relay's hostname. A Datadog metric its sender marked hostless isn't filled; the process
hostname amendment of [ADR `datadog-agent-and-intake-relay`](datadog-agent-and-intake-relay.md)
lists the hostless spellings. The Consequences' recipe for host-less `internal` events, a `remove` of
`host.name` after `internal`, now holds only for `collectd_out`: the other three sinks fill the
removed host back in with the same name. Trace and stats hostnames in `datadog_out` and everything in
`datadog_trace_out` stay verbatim relay metadata. `collectd_out` is unchanged, for the reason in
the Decision above.

## Amendment: `datadog_out` reads `collectd.host` (2026-10-22)

**Superseded** by "Amendment: `host.name` is the standard host key (2026-10-23)" below.
`collectd_in` writes its host as `host.name`, and no sink reads `collectd.host`.

`collectd_in` stamps each value list's and notification's host as `collectd.host`, and
`datadog_out` didn't read it, so every collectd sender behind a relay reported under the relay's
process hostname. collectd identifies a value by host, plugin, and type, so its host is the Datadog
host of the series it sends.

`datadog_out` now ranks a non-empty `Str` `collectd.host` as an event's own host, after the
others in the table above and before the process hostname: for a metric, after `host.name`, a
DogStatsD `host:` tag, and the Datadog hostless marks (an empty `host:` tag, a v2 `host` resource
with no name, or `_logit.datadog.hostless`), since a sender that marked a sample hostless chose no
host; for an event or service check, after `statsd.*.host`, `host.name`, and `host:`; for a log,
after `hostname` and `host.name`, and only when the log carries neither `syslog.hostname` nor
`host`. Read as the host, it leaves as the host and not as a tag or a log attribute; when another
host or a hostless mark wins, it stays a tag.

`host.name` stays first, ahead of `collectd.host`, because it's how an operator overrides a host
with `set`. `collectd_out` reads the two the other way round (`collectd.host`, then `host.name`),
because there `collectd.host` is the wire's own field.

## Amendment: `host.name` is the standard host key (2026-10-23)

The 2026-10-22 amendment made `datadog_out` read another family's host key, `collectd.host`. That
was the one read left that broke
[ADR `protocol-knowledge-stays-in-family`](protocol-knowledge-stays-in-family.md): a Lua script or
`set` that wrote `collectd.host` steered `datadog_out`, and `splunk_hec_out` never read it, so one
relay sent collectd's host to Datadog and not to Splunk. One shared key ends the cross-family read;
`splunk_hec_out` still takes only a resource `host.name`, so collectd's and syslog's per-event
hosts don't reach its envelope (the known gap "`splunk_hec_out` writes the envelope `host` from the batch resource only" in [`docs/known-gaps/splunk.md`](../known-gaps/splunk.md)).

`host.name`, the OpenTelemetry semantic-conventions name, is the one documented host key, spelled
`logit_core::attrs::HOST_NAME`:

- **A source** that learns a sender's host writes it as `host.name`, at the scope where it is
  identity. `collectd_in` writes it on each event, because collectd's host varies per value list,
  and `syslog_in` writes each line's HOSTNAME on its event for the same reason; `internal` writes
  the process hostname on its resource.
- **A sink** that needs a host reads `host.name` off the event, else the resource, beside its own
  family's wire forms, and falls back to the process hostname or its own `hostname:`. The table
  gives each sink's order.
- **No sink reads another family's host key**, and no sink ranks a foreign key against its own.

| Sink | Host, in order |
|---|---|
| `collectd_out` | `host.name`, then `hostname:`; with neither, the list drops (`no_host`) |
| `datadog_out` | for a metric, `host.name`, then a DogStatsD `host:` tag; for an event or service check, `statsd.*.host` first; for a log, `hostname` first; then the process hostname. A hostless mark is never filled. A log carrying `syslog.hostname` or `host` gets no default: both are Datadog's own reserved log attributes, which its log preprocessing reads as the host, not syslog-family keys |
| `splunk_hec_out` | the resource's `host.name`, then the process hostname: the envelope is per batch (known gap: [`docs/known-gaps/splunk.md`](../known-gaps/splunk.md)) |
| `statsd_out` (`format: dogstatsd`) | an event's or check's `h:` field; else a `host` attribute, then a non-empty `host.name`, written as the `host:` tag |
| `syslog_out` | `host.name` (event, else resource), then `hostname:`, then the process hostname |

Where a sink's own wire carries a host field, that field ranks first: a Datadog log's `hostname`,
an event's or check's `h:` field, and at `statsd_out` a `host` attribute, `statsd_in`'s reading
of a DogStatsD `host:` tag, so a DogStatsD relay keeps its tag. None reads another family's key.

`syslog_in` writes HOSTNAME as `host.name` and no longer writes `syslog.hostname`, and
`syslog_out` reads `host.name` as HOSTNAME
([ADR `syslog-output`](syslog-output.md)'s "Amendment: HOSTNAME is `host.name` (2026-10-23)").
That changes `syslog_out`'s row of the 2026-10-14 table above: its own host is now `host.name`.
`datadog_out`'s log row there still lists `syslog.hostname`, as Datadog's reserved attribute.

`collectd_in` no longer writes `collectd.host`, and nothing reads it: an attribute of that name
is an ordinary attribute. Other sinks render collectd's host under the standard name, so
`prometheus_out` labels it `host_name`, and `influxdb_out` and `graphite_out` tag it `host.name`.
