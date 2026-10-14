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

`logit` claims `host.name` only for its own telemetry (`internal`). The follow-up workstream makes
`syslog_out`, `splunk_hec_out`, and `datadog_out` fall back to the process hostname when an event
carries no host of its own. That fallback fills a missing value and never overwrites a host an
event carries. `collectd_out` keeps its `no_host` drop: its ADR's stance that inventing a host
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
- A config that relied on `internal` events having no `host.name` changes behavior in one place:
  `internal -> collectd_out` now sends the process hostname instead of dropping with `no_host`.
- `rustix` joins `logit-pipeline`'s direct dependencies (the `system` feature). It was already
  transitive.
