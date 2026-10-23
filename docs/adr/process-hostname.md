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
relay sent collectd's host to Datadog and not to Splunk. One shared key ends the cross-family
read, and every sink that writes a host takes a per-event `host.name`, `splunk_hec_out` included.

`host.name`, the OpenTelemetry semantic-conventions name, is the one documented host key, spelled
`logit_core::attrs::HOST_NAME`:

- **A source** that learns a sender's host writes it as `host.name`, at the scope where it is
  identity. `collectd_in` writes it on each event, because collectd's host varies per value list,
  and `syslog_in` writes each line's HOSTNAME on its event for the same reason; `internal` writes
  the process hostname on its resource.
- **A sink** that needs a host reads `host.name` off the event, else the resource, first, and
  falls back to its own family's wire forms, then the process hostname or its own `hostname:`
  ("Amendment: `host.name` ranks first everywhere (2026-10-23)" below). Superseded (2026-10-23):
  no sink reads its family's wire forms as a fallback; after `host.name` comes only its own
  hostless mark, where the family has one, then the process hostname or `hostname:` ("Amendment:
  a source fills the common keys (2026-10-23)" below).
- **No sink reads another family's host key**, and no sink ranks a foreign key against its own.

`syslog_in` writes HOSTNAME as `host.name` and no longer writes `syslog.hostname`, and
`syslog_out` reads `host.name` as HOSTNAME
([ADR `syslog-output`](syslog-output.md)'s "Amendment: HOSTNAME is `host.name` (2026-10-23)").
That changes `syslog_out`'s row of the 2026-10-14 table above: its own host is now `host.name`.
`datadog_out`'s log row there still lists `syslog.hostname`, as Datadog's reserved attribute.

`collectd_in` no longer writes `collectd.host`, and nothing reads it: an attribute of that name
is an ordinary attribute. Other sinks render collectd's host under the standard name, so
`prometheus_out` labels it `host_name`, and `influxdb_out` and `graphite_out` tag it `host.name`.

## Amendment: `host.name` ranks first everywhere (2026-10-23)

Before this amendment, a sink's own wire host form ranked ahead of `host.name`: a Datadog log's
`hostname`, a Datadog event's or service check's `h:` field (`_logit.statsd.*.host`), and at
`statsd_out` a `host` attribute. So an operator's `set` of `host.name`, or a resource `host.name`
from `internal` or an OTLP sender, reached some records and not others, by protocol. One rule
replaces that order: `host.name` (event, else resource), when present and not empty, is the host at
every sink. A sink's own wire forms apply only when there is none. This also changes `datadog_out`'s
row of the 2026-10-14 table above. Superseded (2026-10-23): the Datadog family's sources now write
every wire host as `host.name`, so `datadog_out` and `statsd_out` read no wire form after it,
only the hostless mark ("Amendment: a source fills the common keys (2026-10-23)" below, whose
table replaces this one's two rows).

| Sink | Host, in order |
|---|---|
| every sink that writes a host | `host.name` (event, else resource), then the sink's own forms below, then its fallback |
| `datadog_out` | then a log's `hostname`, an event's or check's `h:` field, a DogStatsD `host:` tag, and the hostless marks; then the process hostname. A hostless mark is never filled by the process hostname, and a `host.name` fills it. A log carrying `syslog.hostname` or `host` gets no default: both are Datadog's own reserved log attributes |
| `statsd_out` (`format: dogstatsd`) | then an event's or check's non-empty `h:` field, written as its own field, and a `host` attribute, written as the `host:` tag, both written when present; then, with no `host` attribute and no non-empty `h:` field, the hostless mark as an empty `host:` tag; no fallback, since the Agent stamps its own |
| `collectd_out` | then `hostname:`; with neither, the list drops (`no_host`) |
| `syslog_out` | then `hostname:`, then the process hostname |
| `splunk_hec_out` | then the process hostname |

Where `host.name` wins, the form it outranks doesn't reach the wire. A private carrier (`h:`, the
hostless mark) is left off, uncounted, as every private key is. A public attribute that would put a
second host on the wire is omitted and counted: a Datadog log's `hostname` attribute
(`tags.omitted{reason="reserved_key"}`, as a `message` attribute is), and at `statsd_out` a `host`
attribute, which would be a second `host:` tag that a DogStatsD Agent consumes
(`tags.omitted{reason="unrepresentable"}`).

A Datadog-family source writes `host.name` only from a sender's own key spelled that way: a
DogStatsD `host.name:` tag, a Datadog API event's or check's `host.name:` tag, or a Datadog log's
`host.name` attribute. A relay of such a record beside the protocol's own host form now sends
`host.name` as the host. Each like pair names that a permitted normalization
(`crates/logit-cli/tests/statsd_round_trip.rs`'s (10), and `logit_proto::datadog`'s permitted
normalizations for logs, events, and service checks). Every other relay round trip is unchanged.

## Amendment: a source fills the common keys (2026-10-23)

The ranks-first amendment left each Datadog-family sink reading its family's wire host forms
after `host.name`: a log's `hostname` attribute, an event's or service check's `h:` field (the
`_logit.statsd.event.host` and `_logit.statsd.service_check.host` carriers), and a DogStatsD
`host:` tag (a `host` attribute). Each existed only because the source that decoded the wire host
didn't write `host.name`. So one host could reach the model in four spellings, and every sink that
cared reread all four. The fix belongs at the source.

**A source fills the common keys; a sink uses them.** A source (a listener's decoder, `internal`,
`replay_in`, or `generate_in`'s template) is responsible for writing the common keys from its
incoming data, with logic appropriate to its protocol, at the scope where the value is identity.
A sink is responsible for using them. A sink never reconstructs a common key from a protocol's
wire form: a wire form that names the same fact is decoded onto the common key, and the wire form
doesn't survive as a carrier. Where the protocol has its own rule for which of several wire forms
names the host, the source applies it, so a sink sees one answer.

For the host, the Datadog family's decoders now apply that rule:

| Wire host | Decodes to |
|---|---|
| `datadog_in` series, distribution point, or sketch `host` | `host.name`; `""` is the hostless mark (unchanged) |
| `datadog_in` log `hostname` | `host.name`; `""` is no host |
| `datadog_in` event `host` (the Agent's `/intake/` envelope and public v1) | `host.name`; `""` is the hostless mark, as for a series |
| `datadog_in` service check `host_name` | `host.name`; `""` is the hostless mark, as for a series |
| `statsd_in` metric line, event, or service check `host:` tag | `host.name`, the last one when there are several, never a tag; an empty one is the hostless mark |
| `statsd_in` event or service check `h:` field | `host.name`, ahead of a `host:` tag; an empty one is no host, and the tag applies |

That is how a DogStatsD Agent reads a line (datadog-agent 7.83.3,
`comp/dogstatsd/server/impl/enrich.go`, `extractTagsMetadata` and `enrichEvent`). A record's own
host displaces a `host.name` the sender spelled as a tag or a log key, counted
`logit.input.tags.omitted{reason="reserved_key"}`; with no host of its own, such a tag is the
record's `host.name`. The two `_logit.statsd.*.host` carriers are retired. A `host` attribute is
an ordinary tag at every sink: no decoder writes one now, so it comes from an operator's `set`, a
script, or another protocol. A Datadog log's `hostname` attribute, from any of those, would be a
second `hostname` key in the object, so `datadog_out` omits it, counted.

After `host.name`, a sink reads only its family's hostless mark, then its fallback:

| Sink | Host, in order |
|---|---|
| `datadog_out` | `host.name` (event, else resource) in each route's own field; then the hostless mark (`_logit.datadog.hostless`, or a v2 `host` resource with no name), never filled; then the process hostname. A log carrying `syslog.hostname` or `host` gets no default: both are Datadog's own reserved log attributes |
| `statsd_out` (`format: dogstatsd`) | `host.name` as a metric line's `host:` tag and an event's or service check's `h:` field; then the hostless mark as an empty `host:` tag; no fallback, since the Agent stamps its own. A valued `host` attribute beside either is omitted, counted, since the Agent would consume it as a second `host:` tag; a bare `host` token stays |

The other rows of the ranks-first table are unchanged. The like-pair relays stay fixed points
modulo their lists; `crates/logit-cli/tests/statsd_round_trip.rs`'s (10) and
`logit_proto::datadog`'s permitted normalizations name what changed: a DogStatsD `host.name:` tag
beside a `host:` tag no longer relays as the host, reversing the ranks-first amendment's reading,
since the source now decides and the wire's own host is the Agent's.

The common keys today, and who writes each:

| Key | Written by, from |
|---|---|
| `host.name` | `collectd_in` from each value list's host part, on the event; `syslog_in` from HOSTNAME, on the event; `statsd_in` and `datadog_in` from the wire hosts above, on the event; `splunk_hec_in` from each object's `host`, on the resource; `internal` from the process hostname, on the resource; `otlp_in` passes the sender's resource and record attributes through, which already use the standard key |
| `service.name` | `internal`, on the resource; `datadog_in` (its traces route) and `datadog_trace_in` from each span's `service`, on the span's event, and their APM stats routes (`datadog_in`'s stats route, `datadog_trace_in`'s `/v0.6/stats`) from each `ClientGroupedStats`' `Service`, on the group's event; `otlp_in` passes it through; otherwise an operator's `set`. A Datadog log's `service` stays the attribute `service`, which `datadog_out` writes back as the log's `service` |
| `network.peer.*`, `client.*` | every network listener but `logit_in`, opt-in ([ADR `listener-peer-address`](listener-peer-address.md)) |
| the `_logit.metrics.*` flags | decoders, where the flag says the whole fact ([ADR `metric-flags`](metric-flags.md)) |

`replay_in` and `logit_in` deliver whatever a source upstream wrote; `generate_in` writes what
its template says.

**Not yet applied.** These sources have a host-like wire form and don't write `host.name`; each
is its own follow-up, not part of this amendment:

- `influxdb_in`: a line-protocol tag named `host`, which Telegraf adds to every point by default,
  stays the tag `host`.
- `prometheus_in`: the `instance` and `job` labels are Prometheus target identity, not a host,
  and stay labels. That's settled, not a follow-up.
- `graphite_in`, `lines_in`, `tail_in`: their wires carry no host field; a host inside a metric
  path or a line is a transform's to extract.
- `docker_in`: a container's `config.v2.json` names its `Config.Hostname`, which isn't read.
- `datadog_in`'s and `datadog_trace_in`'s trace and APM stats payloads: the `TracerPayload` and
  `ClientStatsPayload` `hostname` (`_logit.datadog.tracer.hostname`) and the Agent's own
  `hostName`/`internalHostname` (`_logit.datadog.agent.hostname`, also an events envelope's) are
  payload metadata the trace sinks restore verbatim for the Agent's trace processing, and the
  Agent's is the relaying Agent's identity, not a sender's host. They stay carriers.

