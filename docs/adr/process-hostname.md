---
created: 2026-10-14
updated: 2026-10-24
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
`datadog_trace_out` stay verbatim relay metadata. Superseded (2026-10-23): a trace or stats
payload's hostname is the resource's `host.name`, and `datadog_out` fills a missing one only where
no Agent hostname rides beside it ("Amendment: a source fills the common keys (2026-10-23)"
below). `collectd_out` is unchanged, for the reason in the Decision above.

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

**Every other host is the operator's to put in `host.name`.** An input whose protocol defines a
host field writes `host.name` from it. For every other way a host reaches an event (a parsed log
field, a carbon path segment, a line's text, a Lua-built event, a relay from a protocol with no
host field), an operator's transform puts it in `host.name`, and no sink reads any other key for
it. That's settled behavior, not a gap: only the protocol's own definition tells an input which
field, if any, names a host. For example, a pino or bunyan log tailed by `tail_in` and parsed by
`json` carries the host as a `hostname` attribute; a `lua` stage that copies it to `host.name`
(or a `set` of a constant) is what makes it the host at every sink.

For the host, the Datadog family's decoders now apply that rule:

| Wire host | Decodes to |
|---|---|
| `datadog_in` series, distribution point, or sketch `host` | `host.name`; `""` is the hostless mark (unchanged) |
| `datadog_in` log `hostname` | `host.name`; `""` is no host |
| `datadog_in` event `host` (the Agent's `/intake/` envelope and public v1) | `host.name`; `""` is the hostless mark, as for a series |
| `datadog_in` service check `host_name` | `host.name`; `""` is the hostless mark, as for a series |
| `statsd_in` metric line, event, or service check `host:` tag | `host.name`, the last one when there are several, never a tag; an empty one is the hostless mark |
| `statsd_in` event or service check `h:` field | `host.name`, ahead of a `host:` tag; an empty one is no host, and the tag applies |
| `datadog_in` traces and `datadog_trace_in` `/v0.7/traces`: a `TracerPayload`'s `hostname` | `host.name` on the batch resource, one payload per batch; `""` is no host. v0.3 to v0.5 have no such field |
| `datadog_in` and `datadog_trace_in` APM stats: a `ClientStatsPayload`'s `Hostname` | `host.name` on the batch resource, one payload per batch; `""` is no host |

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
| `datadog_out` | `host.name` (event, else resource) in each route's own field; then the hostless mark (`_logit.datadog.hostless`, or a v2 `host` resource with no name), never filled; then the process hostname. A log carrying `syslog.hostname` or `host` gets no default: both are Datadog's own reserved log attributes. A trace or APM stats payload reads the resource's `host.name` only, has no hostless mark, and gets the process hostname only with no `_logit.datadog.agent.hostname` beside it |
| `statsd_out` (`format: dogstatsd`) | `host.name` as a metric line's `host:` tag and an event's or service check's `h:` field; then the hostless mark as an empty `host:` tag; no fallback, since the Agent stamps its own. A valued `host` attribute beside either is omitted, counted, since the Agent would consume it as a second `host:` tag; a bare `host` token stays |

The other rows of the ranks-first table are unchanged. The like-pair relays stay fixed points
modulo their lists; `crates/logit-cli/tests/statsd_round_trip.rs`'s (10) and
`logit_proto::datadog`'s permitted normalizations name what changed: a DogStatsD `host.name:` tag
beside a `host:` tag no longer relays as the host, reversing the ranks-first amendment's reading,
since the source now decides and the wire's own host is the Agent's.

The common keys today, and who writes each:

| Key | Written by, from |
|---|---|
| `host.name` | `collectd_in` from each value list's host part, on the event; `syslog_in` from HOSTNAME, on the event; `statsd_in` and `datadog_in` from the wire hosts above, on the event; `datadog_in`'s traces and stats routes and `datadog_trace_in` from each tracer or stats payload's hostname, on the resource; `splunk_hec_in` from each object's `host`, on the resource; `docker_in` under `host_name: container` from the container's `Config.Hostname`, on the resource, and otherwise not at all; `internal` from the process hostname, on the resource; `otlp_in` passes the sender's resource and record attributes through, which already use the standard key |
| `service.name` | `internal`, on the resource; `datadog_in` (its traces route) and `datadog_trace_in` from each span's `service`, on the span's event, and their APM stats routes (`datadog_in`'s stats route, `datadog_trace_in`'s `/v0.6/stats`) from each `ClientGroupedStats`' `Service`, on the group's event; `otlp_in` passes it through; otherwise an operator's `set`. A Datadog log's `service` stays the attribute `service`, which `datadog_out` writes back as the log's `service`. W4a will add `statsd_in`'s and `datadog_in`'s `service:` tags, on the event; W4b a Datadog log's `service` field and `ddtags` `service:`, on the event; W4c a stats payload's `Service`, on the resource |
| `deployment.environment.name` | today, `otlp_in` passes it through, and otherwise an operator's `set`. W4a will add `env:` tags, on the event; W4b a log's `ddtags` `env:`, on the event; W4c a span's `env` meta, on the event, and a `TracerPayload`'s `env` and a stats payload's `Env`, on the resource |
| `service.version` | today, `otlp_in` passes it through, and otherwise an operator's `set`. W4a will add `version:` tags, W4b `ddtags` `version:`, and W4c a span's `version` meta, on the event, and a `TracerPayload`'s `app_version` and a stats payload's `Version`, on the resource |
| `container.id` | today, `docker_in` on each container's resource, and `otlp_in` passes it through. W1 will add DogStatsD `\|c:`, on the event, and the `Datadog-Container-ID` header, a `TracerPayload`'s `container_id`, and a stats payload's `ContainerID`, on the resource |
| `container.image.tags` | today, `otlp_in` passes it through, and otherwise an operator's `set`. W1 will add a stats payload's `ImageTag` and `docker_in`'s image tag, each as a one-element array on the resource |
| `process.pid` | today, `otlp_in` passes it through, and otherwise an operator's `set`. W2 will add `syslog_in`'s canonical decimal PROCID or `tag[pid]`, on the event |
| `network.peer.*`, `client.*` | every network listener but `logit_in`, opt-in ([ADR `listener-peer-address`](listener-peer-address.md)) |
| the `_logit.metrics.*` flags | decoders, where the flag says the whole fact ([ADR `metric-flags`](metric-flags.md)) |

`replay_in` and `logit_in` deliver whatever a source upstream wrote; `generate_in` writes what
its template says.

Settled, and not follow-ups:

- `prometheus_in`'s `instance` and `job` labels are Prometheus target identity, not a host, and
  stay labels.
- `graphite_in`, `lines_in`, and `tail_in` have no protocol host field, so any host they carry is
  the operator's to put in `host.name`, as the rule above says.
- `influxdb_in` writes no `host.name`. Line protocol defines no host field: Telegraf's `host` tag,
  which it adds to every point by default, is one producer's convention, and `influxdb_in` can't
  know what every producer that speaks line protocol means by its tags. The tag stays the tag
  `host`; an operator who wants it as the host copies it to `host.name` with a `lua` stage, as
  for any other host.
- `docker_in` writes no `host.name` by default. A container's `Config.Hostname` in
  `config.v2.json` is the container's own hostname, usually its short id, so writing it by
  default would make every container its own host, and at `datadog_out` a new Datadog host per
  container. With none, a sink's process-hostname fallback names the host `logit` runs on, the
  Docker host. `host_name: container` writes it, on the resource beside `container.id`, for an
  operator who wants each container as a host
  ([ADR `file-tailing-and-docker-json-logs`](file-tailing-and-docker-json-logs.md)'s
  "Amendment: `host.name` is opt-in (2026-10-23)").
- A tracer or stats payload's hostname is the host of its spans or stats, so `datadog_in` and
  `datadog_trace_in` write it as the batch resource's `host.name`, and
  `_logit.datadog.tracer.hostname` is retired. A datadog-agent 7.83.3 Agent sets
  `TracerPayload.Hostname` from the tracer's payload, else from the root span's `_dd.hostname`
  (`pkg/trace/agent/agent.go`, `setPayloadAttributes`), and never from its own hostname: it sends
  its own beside the payloads, as `AgentPayload.HostName` (`pkg/trace/writer/trace.go`). So an
  empty one has no hostless reading; it is no host. The Agent's own hostname
  (`_logit.datadog.agent.hostname`, an `AgentPayload`'s `hostName`, a `StatsPayload`'s
  `AgentHostname`, or an events envelope's `internalHostname`) is the relaying Agent's identity,
  not a sender's host, and stays a carrier
  ([ADR `datadog-agent-and-intake-relay`](datadog-agent-and-intake-relay.md)'s "Amendment: the
  tracer hostname is `host.name` (2026-10-23)").

Every source with a protocol host field now writes it, or is settled above as not having one.


## Amendment: the remaining common keys (2026-10-24)

The rule of "Amendment: a source fills the common keys (2026-10-23)" covers every common key,
not only `host.name`. This amendment names the rest and how each family writes them. The
[`ckeys` plan](../plans/common-keys.md) has the per-input mapping, read from the code, and the
workstreams that carry it out; until each lands, the facts below keep their current spellings.

**The keys.** Each is semconv's current name, and none has an alias. Each joins
[data-model.md](../design/data-model.md)'s "Well-known attribute names" when its first writer
lands.

| Key | Type | Written from (protocol-defined fields only) |
|---|---|---|
| `service.name` | string | a Datadog log's `service`; a `service:` tag on a DogStatsD line and on a Datadog series, sketch, event, or service check; an APM stats payload's `Service`. Spans and stats groups already write it |
| `deployment.environment.name` | string | an `env:` tag (DogStatsD, Datadog metrics, events, and checks); a Datadog log's `ddtags` `env:`; a span's `env` meta; a `TracerPayload`'s `env`; a stats payload's `Env` |
| `service.version` | string | a `version:` tag; a `ddtags` `version:`; a span's `version` meta; a `TracerPayload`'s `app_version`; a stats payload's `Version` |
| `container.id` | string | DogStatsD `\|c:` on metrics, events, and checks; the `Datadog-Container-ID` header and a `TracerPayload`'s `container_id`; a stats payload's `ContainerID` |
| `container.image.tags` | array of string | a stats payload's `ImageTag`, as one element; `docker_in`'s image reference tag, in place of `container.image.tag` |
| `process.pid` | integer | a syslog PROCID or RFC 3164 `tag[pid]` that is canonical decimal |

**Scope.** A tag or field on one line, record, or span is identity for that event, so the source
will write the key there. A field of a whole payload (a `TracerPayload`, a `ClientStatsPayload`, a
tracer's request headers) is identity for the batch, which is one payload, so the source will
write it on the resource. A sink will read the event's value, else the resource's. A sink that
writes a per-payload field will read the resource only, as the Datadog encoder that `datadog_out`
and `datadog_trace_out` share does for a `TracerPayload`'s `hostname`
([ADR `datadog-agent-and-intake-relay`](datadog-agent-and-intake-relay.md)'s "Amendment: the
tracer hostname is `host.name` (2026-10-23)"): an event's own value is an ordinary span tag, and
in a form with no payload field (`datadog_trace_out`'s v0.4) the resource's value rides as a span
tag that a differing event value shadows, counted.

**Collision at decode: the protocol's form wins.** When a protocol's own field and a sender's tag
name the same fact (a span's `env` meta beside a `deployment.environment.name` meta key, `|c:`
beside a `container.id:` tag, `env:` beside `deployment.environment.name:` in one tag list), the
key will take the protocol's value, and the tag will be dropped, counted
`logit.input.tags.omitted{reason="reserved_key"}`, as a `host.name:` tag beside a line's own host
is.

**Collision at encode: the key wins.** A bare `env`, `version`, or `service` attribute beside its
key comes only from an operator's transform or a source in another family. A sink will omit it and
count `logit.output.tags.omitted`, as `statsd_out` omits a valued `host` beside `host.name`
(`reason="unrepresentable"` in a tag list, and `reason="reserved_key"` for a key that would collide
with a Datadog log's own field, as a log's `hostname` attribute does).

**Datadog's spellings.** A Datadog-family sink will write each key under Datadog's name for it,
whatever source wrote it:

- On metrics, events, and checks, `service.name` as a `service` tag,
  `deployment.environment.name` as `env`, `service.version` as `version`, and `container.id` as
  `container_id`: the tag spellings of Datadog's OTLP mapping (datadog-agent 7.83.3,
  `pkg/opentelemetry-mapping-go/otlp/attributes/attributes.go`, `coreMapping` and
  `ContainerMappings`).
- On traces, `container.id` as the `TracerPayload`'s `container_id`, which the Agent's OTLP trace
  receiver fills from `container.id` (`pkg/trace/api/otlp.go`, `receiveResourceSpansV1` and
  `receiveResourceSpansV2`), and as the tracer API's `Datadog-Container-ID` header at
  `datadog_trace_out`.
- On traces and APM stats, each key in the payload's own field: `service.name` as a span's or
  stats group's `service` and a stats payload's `Service`; `deployment.environment.name` as a
  span's `env` meta, the `TracerPayload`'s `env`, and a stats payload's `Env`; `service.version`
  as a span's `version` meta, the `TracerPayload`'s `app_version`, and a stats payload's
  `Version`; `container.id` as a stats payload's `ContainerID`; and `container.image.tags` as a
  stats payload's `ImageTag` (the first-element rule below). A span or group value is read
  from the event, and a payload field from the resource, by the scope rule above.
- On a log, `container.id` appended to `ddtags` as `container_id:<id>`, as the Agent tags a
  container's logs. A `container_id:` entry already in the log's `ddtags` loses to the key,
  removed and counted `reason="reserved_key"`, as a log's `hostname` beside `host.name` is.
- `container.image.tags`' first element as the `image_tag` tag or stats `ImageTag`.
  `ContainerMappings` maps the deprecated singular `container.image.tag` to `image_tag` and reads
  nothing for the array, so this is `logit`'s rule, by analogy. `image_tag` holds one value, so
  further elements will be omitted, counted `reason="unrepresentable"`, as for any value the wire
  has no place for.

`statsd_out` under `format: dogstatsd` will write `env:`, `version:`, and `service:` tags and
`|c:`. `otlp_out` and `splunk_hec_out` pass the keys through as attributes.

**DogStatsD `|c:`.** The Agent reads `|c:` as origin-detection local data (datadog-agent 7.83.3,
`comp/core/tagger/origindetection/origindetection.go`, `ParseLocalData`), and a single value reads
differently from a comma list:

- A single value that is bare or prefixed `ci-` or `cid-` is a container id, so `statsd_in` will
  write it as `container.id` with the prefix stripped. A single `in-<inode>` is a cgroup v2 inode,
  which no neutral key names, so it will decode to the carrier `_logit.statsd.cgroup_inode`.
- In a comma list, the Agent reads only `ci-` and `in-` items, so `statsd_in` will too: a `ci-`
  item is `container.id`, an `in-` item is the carrier, and any other item will be dropped,
  counted `logit.input.fields.dropped{reason="malformed"}`. It's loss, and none of the decoder's
  counters counts part of a line, so W1 will add this per-field counter and document it in
  [internal-telemetry.md](../design/internal-telemetry.md).

`statsd_out` will write a container id alone as `|c:<id>`, bare, and with the carrier as
`|c:ci-<id>,in-<inode>`, the only list form in which the Agent reads the id; the carrier alone
leaves as `|c:in-<inode>`. A single `ci-` or `cid-` value relaying as the bare id will be a named
normalization of the DogStatsD pair: a bare single value is the form every Agent version reads.

**Datadog logs: `ddtags` is lifted, not split.** `datadog_in` will lift a log's `ddtags` `env:`,
`version:`, and `service:` onto the three keys and keep every other tag in the one `ddtags`
string. A log's own `service` field wins over a `ddtags` `service:`, and the loser will be dropped
and counted as a decode collision. `datadog_out` will write `env:` and `version:` back into
`ddtags` from the keys for any log, an OTLP log included, and `service.name` as the log's
`service`. A full split was rejected: a tag and a top-level field are different things in Datadog,
and `datadog_out` writes every attribute as a field, so a relay would turn each split tag into a
field. The three lifted tags are Unified Service Tagging's, which every route has a typed home
for.

**What stays as it is**, each for the reason given:

- An `AgentPayload`'s `env` and `agentVersion`, and a `StatsPayload`'s `agentEnv` and
  `agentVersion`, stay the `_logit.datadog.agent.env` and `.version` carriers. They're the relaying
  Agent's identity, as `_logit.datadog.agent.hostname` is, not the application's.
- A stats payload's `GitCommitSha` stays `_logit.datadog.stats.git_commit_sha`. semconv's `vcs.*`
  keys describe a repository change, not a deployed build, and only the Datadog family reads it.
- Syslog APP-NAME stays `syslog.tag`. It names a program, not a service; as `service.name` it would
  make every daemon on a host its own service.
- A tracer's `language_name`, `tracer_version`, and `runtime_id` stay `_logit.datadog.tracer.*`
  carriers. They describe the tracing library, and only the Datadog family reads them.
- A Datadog log's `ddsource` stays `ddsource`. It names the log's integration, and semconv has no
  key for it.
- `influxdb_in`, `graphite_in`, `lines_in`, and `tail_in` write none of these keys, because their
  protocols define no such field. A value they carry is an operator's to move with a transform.

The inventory of common keys in the 2026-10-23 amendment above gains a row for each new key, and
its `service.name` row names the writers this stream adds.

