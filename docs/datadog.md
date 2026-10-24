# Datadog and `logit`

`logit` speaks Datadog's own protocols in both directions. It can send logs, metrics, traces,
events, and service checks to Datadog, straight to the intake API or through a local Datadog
Agent. It can also receive what Datadog-instrumented systems produce, losslessly, from either side
of an Agent: in front of the applications, where it answers dd-trace tracers and DogStatsD clients
as a local Agent would; and behind a fleet of Agents, where it answers them as Datadog's intake
would. Both stand-ins need nothing changed on the sender but a URL, which is what makes a migration
to or from Datadog a configuration change rather than an application change.

This doc helps you pick a topology and avoid the mistakes that lose data. Each component's
configuration and telemetry are in [`docs/deploying.md`](deploying.md):
[`datadog_in`](deploying.md#datadog_in-standing-in-for-datadogs-intake),
[`datadog_trace_in`](deploying.md#datadog_trace_in-standing-in-for-the-agents-apm-api),
[`datadog_out`](deploying.md#datadog_out-sending-straight-to-datadog),
[`datadog_trace_out`](deploying.md#datadog_trace_out-sending-to-an-agents-apm-api), and
`statsd_in`/`statsd_out` over the Agent's
[DogStatsD socket](deploying.md#statsd_in-dogstatsd-over-a-unix-socket).

Related docs:

- [ADR `datadog-agent-and-intake-relay`](adr/datadog-agent-and-intake-relay.md): the design
  decisions, including why Datadog-origin traces never go to Datadog as OTLP.
- [The ADR's "Verified destination behavior"](adr/datadog-agent-and-intake-relay.md#verified-destination-behavior):
  the protocol survey, what Datadog accepts and emits. `crates/logit-proto/src/datadog/mod.rs`'s
  module doc maps each Datadog concept onto `logit`'s event model.
- [`docs/known-gaps/datadog.md`](known-gaps/datadog.md): what isn't built or isn't verified.

## Topologies

Four topologies cover the ways `logit` sits next to Datadog. Each has a runnable example.

- **Direct to Datadog's API.** `logit` is the host collector and posts to Datadog's intake with no
  Agent in the path: [`fixtures/datadog-direct.yaml`](../fixtures/datadog-direct.yaml) runs
  DogStatsD through `aggregate` into `datadog_out`.
- **Through a local Agent.** `logit` hands data to an Agent on the host, which sends it on:
  [`fixtures/datadog-via-agent.yaml`](../fixtures/datadog-via-agent.yaml) sends OTLP traces and
  metrics to the Agent's OTLP receiver and DogStatsD to its `:8125`.
- **Standing in for an Agent.** Applications point their Datadog libraries at `logit` instead of an
  Agent: [`fixtures/datadog-agent-standin.yaml`](../fixtures/datadog-agent-standin.yaml) listens
  for DogStatsD on `:8125` (`statsd_in`) and for traces and client stats on `:8126`
  (`datadog_trace_in`), optionally on the Agent's Unix sockets too.
  [`fixtures/datadog-agent-relay.yaml`](../fixtures/datadog-agent-relay.yaml) relays both on to a
  real Agent, which is where tracer spans must go next (see
  [Rules that lose data when missed](#rules-that-lose-data-when-missed)).
- **Standing in for the intake.** Agents send `logit` what they'd send Datadog, over Datadog's
  intake API: [`fixtures/datadog-intake-standin.yaml`](../fixtures/datadog-intake-standin.yaml)
  runs `datadog_in` and has the Agent-side settings for replacing Datadog and for dual-shipping.
  This is the topology a migration away from Datadog runs through
  ([From Datadog](#from-datadog-dual-ship-tee-then-cut-over)).

Which component carries each signal in each topology:

| Signal | Direct | Through a local Agent | Agent stand-in | Intake stand-in |
|---|---|---|---|---|
| Metrics | `datadog_out` (series, sketches, distribution points); `otlp_out` agentless, delta temporality only | `statsd_out format: dogstatsd`, over UDP or either Agent Unix socket; `otlp_out` | `statsd_in`, over UDP or either Agent Unix socket; `otlp_in` | `datadog_in` |
| Logs | `datadog_out`; `otlp_out` agentless | `otlp_out`, with the Agent's OTLP logs turned on | `syslog_in`; `otlp_in` | `datadog_in` |
| Traces | `datadog_out` for Agent-processed spans with their APM stats; `otlp_out` agentless for OTel spans | `otlp_out` for OTel spans; `datadog_trace_out` for tracer spans | `datadog_trace_in`; `otlp_in` | `datadog_in` (spans and APM stats) |
| Events | `datadog_out` | `statsd_out` (`_e{}`) | `statsd_in` (`_e{}`) | `datadog_in` |
| Service checks | `datadog_out` | `statsd_out` (`_sc`) | `statsd_in` (`_sc`) | `datadog_in` |

An application that writes JSON lines to an Agent's TCP `logs` port can point at `lines_in` in
`logit` instead: `lines_in` (`transport: tcp`) into `json`, then a `set` stage for the host and
service the Agent would have stamped
([`lines_in`](deploying.md#lines_in-plain-newline-delimited-lines)). A multi-line message arrives
as several events.

One log path is untested. An Agent's TCP `logs` listener takes syslog-formatted lines from
`syslog_out`, but whether the Agent parses a JSON body in one into attributes is unverified. Use
`otlp_out` for logs through an Agent.

## Which way to send

### To Datadog: direct or through an Agent

**Send directly when `logit` is already the collector on the host**: `datadog_out` for metrics,
logs, events, service checks, and Datadog-origin traces with their APM stats, and `otlp_out`
agentless for OTel-origin traces. **Put an Agent in front when you want Datadog's host and APM
features**, or when OTel-origin traces need the Agent's sampling and ingestion controls. Whichever
you pick, **don't send one signal both ways**: Datadog stores both copies, so the same data
arrives twice.

A local Agent gives you:

- host and container metadata, Live Containers, Datadog's integrations, and the Agent's
  out-of-the-box tags;
- APM trace metrics and Remote Configuration;
- its own retry and buffering;
- Unix-socket locality for DogStatsD and APM;
- Agent-side aggregation of `h` and `ms` into `.avg`/`.count`/`.median`/`.95percentile`/`.max`,
  and of `d` into sketches. That needs the raw lines: with `aggregate`'s defaults, `statsd_out`
  turns each timer into `.count`/`.sum`/`.min`/`.max`/`.q*` counter and gauge lines the Agent
  stores as they are. Use `distributions: samples` and `sets: members`, or no `aggregate`, in front of a
  `statsd_out` to an Agent.

It costs you:

- one more process on every host;
- OTLP logs, which are off in the Agent by default;
- HTTP log intake, which the Agent doesn't have: logs reach it as TCP lines or OTLP only;
- a Datadog dependency on every host for the whole of a migration.

Sending directly needs no Agent: it's one hop, and it works wherever HTTPS does. It costs you:

- the 1-hour metric window, which drops a disk-buffer replay after a long outage
  ([below](#disk-buffer-replay-meets-the-1-hour-metric-window));
- the Agent's `h`/`ms` aggregates, which nothing in `logit` produces. `aggregate` sends timers and
  histograms as sketches, and Datadog computes quantiles from those;
- distributions go only as raw points (`/api/v1/distribution_points`) or through the sketches
  route, which Datadog doesn't document for third parties but accepts;
- the intake's payload limits, which `datadog_out` splits requests under;
- for traces, a choice of protocol. The native protocol carries Datadog-origin spans losslessly
  with the stats their Agent computed. OTLP carries OTel-origin spans, but Datadog computes
  their trace metrics (`compute_stats=true`) from the spans that arrive, so after any sampling,
  and Datadog's OTLP mapping loses parts of a Datadog span
  ([the ADR's "Context"](adr/datadog-agent-and-intake-relay.md#context)).

### A series' `rate` type and `interval`

`datadog_out` has no field for either. A `Gauge` whose event or batch resource sets
`_logit.metrics.type: rate` leaves as a Datadog `rate`, and `_logit.metrics.interval`, in whole
seconds, leaves as the `interval` of a series of any type. `datadog_in` sets both from what it
receives, so a relayed `rate` stays a `rate`. For the `count` points an `aggregate` window
produces, stamp the window's length with a `set` after it, as
[fixtures/datadog-direct.yaml](../fixtures/datadog-direct.yaml) does. A Lua script that computes a
per-second rate sets both keys to send a `rate`; the same flag makes `dedup_timestamps` sum two
rates of one interval instead of keeping the later
([ADR `metric-flags`](adr/metric-flags.md)'s "The flags").

### OTel metrics to Datadog: `otlp_in` into `datadog_out`

An OpenTelemetry SDK or Collector can send its metrics to `otlp_in`, and `datadog_out` writes
each kind as the Collector's `datadogexporter` does by default, so a metric reaches the same
Datadog series whichever way it went
([ADR `datadog-otlp-metric-mapping`](adr/datadog-otlp-metric-mapping.md)):

| OTLP kind | What Datadog gets |
|---|---|
| delta `Sum`, monotonic or not | a `count` |
| cumulative non-monotonic `Sum` (an up-down counter) | a `gauge` |
| `Gauge` | a `gauge` |
| delta `Histogram` | a distribution, bin for bin as the exporter builds it; under `histograms.mode: counters`, a `<name>.bucket` count per bucket |
| delta `ExponentialHistogram` | a distribution |
| `Summary` | `<name>.quantile` gauges; for a delta one, also `<name>.count` and `<name>.sum` counts |

`histograms.send_aggregation_metrics: true` adds `<name>.count`, `<name>.sum`, `<name>.min`, and
`<name>.max` series for each delta histogram and exponential histogram, as the exporter's setting
of the same name does. A series carries its unit as the exporter writes it: an OTLP (UCUM) unit
under Datadog's name for it (`ms` as `millisecond`, `By/s` as `byte/second`), and no unit where
Datadog has none, counted `logit.output.fields.omitted{reason="unit_unmapped"}`. A unit that
arrived through `datadog_in` is already Datadog's and leaves as it is. One difference
from the exporter remains: tags come from the event's attributes and resource as for every other
`datadog_out` metric, not from the exporter's resource-attribute tables
(`docs/known-gaps/datadog.md`).

**Convert cumulative metrics to delta first.** Most SDKs default to cumulative temporality, and
Datadog needs a monotonic `Sum`, a `Histogram`, an `ExponentialHistogram`, and a summary's
`.count` and `.sum` as per-interval deltas. The exporter differences them against a per-series
cache; in `logit`, place a `cumulative_to_delta` between `otlp_in` and `datadog_out`, which keeps
that state ([ADR `cumulative-to-delta-transform`](adr/cumulative-to-delta-transform.md),
[fixtures/otlp-to-datadog.yaml](../fixtures/otlp-to-datadog.yaml)). Without it, `datadog_out`
drops each, counted `logit.output.metrics.dropped{reason="unresolved_cumulative"}` or, for a
summary's two series, `logit.output.fields.dropped{reason="unresolved_cumulative"}`, with a
throttled `unresolved_cumulative` warning. The other remedy is to configure the SDK for delta
temporality, with the `OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE=delta` environment
variable.

`cumulative_to_delta` keeps its state in memory. After a restart of `logit`, each series' first
point only records a baseline under the default `initial_value: auto`, so the counts between the
last point before the restart and the first after it are lost. A delta point resent after a
transport error double-counts at Datadog's sketch route, though a series `count` point
overwrites at its series and timestamp.

### collectd to Datadog: `collectd_in` into `datadog_out`

A collectd fleet's `network` plugin can send to `collectd_in`, which writes each value list's and
notification's host as `host.name`. `datadog_out` sends each one under that host, so every
collectd sender keeps its own Datadog host, and writes each data-source type as follows
([fixtures/collectd-to-datadog.yaml](../fixtures/collectd-to-datadog.yaml)):

| collectd type | `logit` model | What Datadog gets |
|---|---|---|
| GAUGE | `Gauge` | a `gauge` |
| ABSOLUTE | delta monotonic `Sum` | a `count` |
| COUNTER | cumulative monotonic `Sum` | dropped, unless a `cumulative_to_delta` upstream makes it a `count` |
| DERIVE | cumulative non-monotonic `Sum` | a `gauge` of the running total, unless a `cumulative_to_delta` with `non_monotonic:` set makes it a `count` |

COUNTER and DERIVE are running totals, and a Datadog `count` is a per-interval delta, so place a
`cumulative_to_delta` between `collectd_in` and `datadog_out`. It converts COUNTER as it is,
with one difference from collectd: collectd reads a COUNTER that went down as a wrap and adds
2^32 or 2^64, but `cumulative_to_delta` reads it as a reset and sends the new value, so the
increase between the previous value and the wrap point is lost.
DERIVE converts only under `non_monotonic:`, because collectd marks it non-monotonic: collectd
can't detect a DERIVE reset, so it can't promise the value never goes down.

- **`non_monotonic: reset_on_decrease`** reads a DERIVE that went down as a counter restart and
  sends the new value, as for COUNTER. Use it for the stock collectd types, whose DERIVE sources
  have a minimum of 0 (`if_octets`, `disk_octets`, `cpu`), so collectd itself discards a negative
  rate as a reset.
- **`non_monotonic: difference`** sends a decrease as a negative `count`. Use it only when every
  DERIVE that reaches it can go down for real. A collectd value list has no start time, so under
  `difference` a counter reset after a host reboot reads as one large negative `count`: the
  previous total subtracted from the new one.

Without `cumulative_to_delta`, `datadog_out` drops COUNTER, counted
`logit.output.metrics.dropped{reason="unresolved_cumulative"}`, and writes DERIVE as a `gauge` of
its running total, counted `logit.output.metrics.degraded{metric_kind="non_monotonic_sum"}`. A
converted DERIVE stays non-monotonic, so its `count` is counted under that name too.

A value list carries no start time, so under the default `initial_value: auto` each series'
first point only records a baseline, at startup and after a restart of `logit`. Set
`max_staleness` above collectd's longest `Interval`.

### From Datadog: dual-ship, tee, then cut over

A migration away from Datadog runs through the intake stand-in in three stages:

1. **Dual-ship.** Add `logit`'s `datadog_in` to the Agents' `additional_endpoints`, and to the
   `logs_config` and `apm_config` equivalents. Datadog keeps receiving everything, and no
   application or host changes. Undoing it is one Agent config edit.
2. **Tee.** Fan `datadog_in` out to the new backend while Datadog keeps receiving its own copy,
   and compare the two.
3. **Cut over.** Point `dd_url`, `logs_config.logs_dd_url`, and `apm_config.apm_dd_url` at
   `logit`, or remove the Agent.

Use the Agent stand-in (`statsd_in`, `datadog_trace_in`, and `otlp_in` on the Agent's ports) only
where there's no Agent to redirect, such as containers without a sidecar or serverless functions,
or in stage 3, once the Agent itself is going away.

## Rules that lose data when missed

### `datadog_trace_in` must not feed `datadog_out`

Datadog's trace intake expects spans an Agent has normalized, obfuscated, and marked, with the
Agent's APM stats beside them. `datadog_trace_in` does none of that processing: it relays spans as
the tracer wrote them. `datadog_out` sends a trace chunk only when its root span carries the
Agent's `_top_level` mark, so every span from `datadog_trace_in` is dropped and counted
`logit.output.records.dropped{reason="needs_agent_processing"}`. An OTel span with no Datadog
fields at all is counted `reason="not_datadog_origin"`.

Send `datadog_trace_in`'s output to `datadog_trace_out` in front of a real Agent, as
`datadog-agent-relay.yaml` does, or to an OTLP backend. Send OTel spans to Datadog with
`otlp_out`. What `datadog_in` receives on `/api/v0.2/traces` came from an Agent, so `datadog_in`
into `datadog_out` relays traces unchanged.

### Disk-buffer replay meets the 1-hour metric window

`datadog_out` drops data outside Datadog's documented windows before sending, counted
`logit.output.records.dropped{reason="stale"}`: metrics older than 1 hour or more than 10 minutes
ahead, logs and events older than 18 hours, and service checks older than 10 minutes. A
`buffer.disk:` holds batches through a Datadog outage, but on replay after an outage longer than
an hour, the metrics are dropped as stale, not delivered late. The 1-hour window is the documented
one and stricter than what the intake stored in testing
([the ADR's "Timestamp windows and what a `202` drops"](adr/datadog-agent-and-intake-relay.md#timestamp-windows-and-what-a-202-drops)). The windows are
measured from one send time per batch, read when the batch is first tried, so every retry of it
drops the same points and each counts once. A batch held across retries can therefore
send a point that far past the end of its window.

### A metric with more than 100 tags is dropped

Datadog drops a series, distribution point, or sketch whose `tags` list holds more than 100
strings, and only the series route says so. `datadog_out` drops such a record before sending and
counts it `logit.output.records.dropped{reason="too_many_tags"}`, with a throttled `too_many_tags`
diagnostic naming its metric. Every attribute the encoder renders as a tag counts, resource
attributes and repeated values included, but `host.name`, the device, and the other fields with
their own place on the wire don't. A histogram's `.bucket` series carries two more tags, its
bounds, so on a metric with 99 or 100 tags it's dropped; a summary's `.quantile` series carries
one more, its quantile, so on a metric with 100 tags it's dropped. Each counts one, and the
metric's other series go out. To keep a metric, cut
its tags upstream with `keep` or `remove`.

### Two points of one series in one second leave as one

Datadog keeps one point per series per second: the higher of two in one request, the later
across requests, and a `count` isn't summed. So of each batch's series points, `datadog_out`
sends the latest point of a series in one second and leaves the others off, counted
`logit.output.metrics.dropped{reason="duplicate_timestamp"}` with a throttled
`duplicate_timestamp` diagnostic. A gauge (not a `rate`) or a cumulative kind equal to the point
sent (the same value, type, unit, interval, source type, and origin) is counted
`logit.output.metrics.normalized{reason="duplicate_timestamp"}` instead, since Datadog stores the
same point either way: an Agent relay or a retry resending identical gauges raises no loss alarm.
Two equal delta counts or `rate`s are two increments, and only one is sent, so they count
`dropped`. A series is the metric name, the tag set, and the host, device, and resources.
Distribution points aren't collapsed, because Datadog keeps both. A relay that
keeps source timestamps from a source stamping several readings of one series in a second (an
Agent's unaggregated `|T` gauges, `statsd_in` gauges, `internal`) shows a nonzero dropped count.
Put a `dedup_timestamps` with `precision: 1s` last before the sink: it keeps the latest gauge and
sums deltas and `rate`s of one interval. Two points of one series in one second that arrive in two batches are both sent, and
Datadog keeps the later, uncounted (`docs/known-gaps/datadog.md`).

The series route also names what it dropped from an accepted request in its `202` body, such as
a point more than 10 minutes in the future when the sender's clock runs ahead. `datadog_out`
counts each named series `logit.output.records.dropped{route="series", reason="rejected"}`, leaves it out of
`logit.output.records`, and logs `series_rejected` with the first entry. The batch isn't retried:
the rest of the request was stored.

### Hostless senders report as one Datadog host

`datadog_out` sends `logit`'s process hostname
([deploying.md's "Hostname"](deploying.md#hostname)) as the host of anything that carries none of
its own:

- a metric, event, or service check with no `host.name` and no hostless mark;
- a log with no `host.name`, `syslog.hostname`, or `host`;
- a trace or APM stats payload with no `host.name` on its batch resource and no relaying Agent's
  hostname beside it. An Agent sends an empty tracer hostname beside its own, so an Agent's
  payload keeps it empty.

That's what an Agent does for a DogStatsD client that names no host, so a `logit` on each host
behaves as the Agent it replaces. A central `logit` receiving from many hosts is different: every
hostless sender reports under the relay's one host, their series merge under it, and the per-host
view of them is lost. Datadog bills per host, so the cost matches one Agent.

`logit`'s inputs read each sender's own host into `host.name`: `statsd_in` a DogStatsD `host:`
tag (the last one) or an event's or check's `h:` field, `datadog_in` a series' or sketch's
`host`, a log's `hostname`, an event's `host`, or a check's `host_name`, `datadog_in` and
`datadog_trace_in` a tracer or stats payload's `hostname` (onto the batch resource), and
`collectd_in` each value list's and notification's host. A `host:` tag is the host, and isn't kept as a tag, as the
Agent reads it. So to keep a host per sender, have each client send one, or stamp `host.name`
upstream with `set`, which overrides the sender's. A host the data carries is never replaced.

A `host` attribute that reaches `datadog_out`, from a `set`, a script, or another protocol such as
an InfluxDB `host` tag, is an ordinary `host:` tag, not the host. A log's `hostname` attribute
from such a source is left off, counted `logit.output.tags.omitted{reason="reserved_key"}`,
because it would be a second `hostname` key. To make either the host, copy it to `host.name` upstream, as
[deploying.md's "Hostname"](deploying.md#hostname) shows.

A sample a client made hostless with an empty `host:` tag stays hostless, and so does a sample an
Agent sends hostless: a v2 `host` resource with no name, or `"host":""` on the v1,
distribution-point, and sketch routes. An empty sketch `host` always reads as hostless, because
protobuf can't tell it from a missing one. A non-empty `host.name` fills a hostless sample.

Through a local Agent, `statsd_out` under `format: dogstatsd` sends the host where the Agent
reads it: a non-empty `host.name` as a metric line's `host:` tag and an event's or check's `h:`
field, else an empty `host:` tag for a hostless sample. A line with neither gets the Agent's own
hostname. Beside either, a `host` attribute is left off, counted
`logit.output.tags.omitted{reason="unrepresentable"}`, since the Agent consumes every `host:` tag
and keeps only the last.

### A container id reaches Datadog as `container_id`, never resolved

`logit` writes a sender's container as `container.id`: `statsd_in` from a DogStatsD `|c:` field's
container id, `datadog_in` and `datadog_trace_in` from a tracer or stats payload's container id or
the `Datadog-Container-ID` header, and `docker_in` from each container it tails. `datadog_out`
sends it as a `container_id` tag on a metric, event, or service check, a `container_id` entry in a
log's `ddtags`, and the `container_id` field of a trace or stats payload; `container.image.tags`'
first element goes out as `image_tag`. A `container_id` or `image_tag` attribute beside the key
is left off, counted `logit.output.tags.omitted{reason="unrepresentable"}`.

An Agent does more: it resolves a container id through its tagger into the container's tags
(image, name, orchestrator labels), and on a DogStatsD sample adds `container_id` itself only at
high tag cardinality. `logit` has no tagger, so `statsd_in -> datadog_out` sends the bare
`container_id` tag for a line that names a container id, and nothing for one that names only a
cgroup inode (`|c:in-<inode>`, what most clients send), which only the sender's host can resolve.
To get the container's tags, send DogStatsD through a local Agent with `statsd_out`, which writes
`|c:` back in the form the Agent reads.

### A log's service, `env`, and `version` ride on the standard keys

`datadog_in` writes a log's `service` as `service.name`, and moves its `ddtags` `env:`,
`version:`, and `service:` entries onto `deployment.environment.name`, `service.version`, and
`service.name`. Every other `ddtags` entry stays in the one string, in order. `datadog_out` writes
them back for any log, whatever source wrote the keys: `service.name` as the log's `service`, and
the other two as `env:` and `version:` entries at the end of `ddtags`. So an OTLP log whose
resource carries `deployment.environment.name` reaches Datadog tagged `env:`.

Two cases lose a value, counted `logit.input.tags.omitted{reason="reserved_key"}` at
`datadog_in`:

- A log carrying its service twice, as the `service` field and a `service:` entry (an Agent
  tailing a container that sets Unified Service Tagging sends both), keeps the field. The relay
  sends no `service:` entry back.
- A second valued `env:`, `version:`, or `service:` entry loses to the first.

A `service` attribute on a log reaching `datadog_out`, from a `json` transform's parse or a
`set`, is left off, counted `logit.output.tags.omitted{reason="reserved_key"}`, since it would be
a second `service` key in the object. Put the value in `service.name` instead, with a `lua` stage
or a `set`.

### `at_least_once` can duplicate a request, and everything but series is stored twice

The default posture is `at_least_once`, so a `5xx` or a timeout is retried. One batch goes out
over up to eight routes, each as one or more requests (one per event, and a route over its size
cap is split). The sink remembers which requests of the batch Datadog settled, and a retry sends
only the rest, so a request Datadog accepted isn't resent. A route Datadog rejects for its
content (a `400`, or a `413`, counted `oversize`) is counted `records.dropped{reason="rejected"}`
and the send goes on to the next request, so one rejected route doesn't fail the batch or stop the
others. A `403` (an invalid API key, or one sent to another `site`) logs `api_key_rejected`; a
`401` (a missing key) or a `404` (a route the configured `site` doesn't serve) logs
`request_refused`. Each would answer every batch the same way, so the sink holds the batch and
retries it until the key or the site is fixed, sending only the requests not yet settled; watch
`logit.component.retrying` for it. A hold applies under `at_most_once` too.

Two cases still send a request twice. The request that drew the `5xx` or timeout is resent, and
it may have been applied. A `buffer.disk:` batch replayed after a crash is a new batch, so every
request of it is sent again. Datadog stores a resent series point once, the last write winning at
its `(series, timestamp)`, but stores a resent log twice. Every other route is assumed to
duplicate too: expect duplicate logs, spans, events, and checks, and inflated distribution point,
sketch, and APM stats counts, which no upstream `aggregate` setting prevents. `datadog_trace_out`
is the same: an Agent dedupes nothing.

To drop the batch on an ambiguous failure instead, set `buffer: {delivery: at_most_once}` on the
sink. It then drops only what Datadog didn't accept, and counts those entries
`records.dropped{reason="ambiguous_at_most_once"}` per route. The count for a route the batch never
reached is an upper bound.

### Events go uncompressed

Datadog's `/api/v1/events` answers any gzip or deflate body `400 Invalid JSON structure`, so
`datadog_out` sends events uncompressed whatever `compression:` says. Every other route follows
`compression:` (gzip by default; zlib deflate for distribution points).

### Size limits

`datadog_out` cuts each route's events into requests under Datadog's limits, and an event too
large to send alone is dropped and counted `records.dropped{reason="oversize"}`. The series limit
(512,000 bytes compressed, 10,000 points) is the intake's own. The limits on distribution points,
sketches, and logs are tighter than the intake's, so they cost extra requests but never a `413`.
`datadog_in` caps a request at 5 MiB compressed and 5,242,880 bytes decompressed (16 MiB for
traces), above what a conforming Agent sends. Both components' `deploying.md` sections have the
tables.

### A `503` is deferral for an Agent, and loss for a tracer

When the pipeline doesn't take a request's batches in time, both listeners answer `503` with
`Retry-After: 1` rather than holding the connection, and count
`logit.input.batches.deferred`. What that costs depends on the sender:

- **`datadog_in`** waits 5 seconds. An Agent's forwarder keeps the payload in its retry queue and
  retries for minutes, so a `503` defers delivery until that queue fills.
- **`datadog_trace_in`** waits 2 seconds. dd-trace-py 4.15.2 retries a `503` five times, waiting
  100 ms and doubling, then drops the payload. Against this listener that's a window of about
  15 seconds; a longer stall loses spans.

Give the sinks behind `datadog_trace_in` a `buffer:` (memory, or `disk:` for a long outage) large
enough to absorb a stall, so the channel it sends into keeps draining. A `buffer:` on
`datadog_trace_out` also keeps an unreachable Agent from turning into `503`s at the tracers.

### Unix sockets: create the directory, mind the mode

`statsd_in` (`transport: unix` or `unix_stream`) and `datadog_trace_in` (`socket:`) bind the
Agent's socket paths. `logit` never creates the socket's directory, because its owner and mode are
the access control. It makes the socket file mode `0722` by default, the Agent's own mode: a
client needs only write permission to send, so any user's process can. Restrict senders with the
directory's permissions, or set `socket_mode:` to a quoted octal string such as `"0660"`. The path must be absolute, and neither Unix transport takes `tls:`. On the sending
side, raise `statsd_out`'s `max_packet_bytes:` to `8192` for a Unix socket.

### Log correlation: `trace_context` with `format: datadog`

A dd-trace tracer with log injection on writes the active trace and span into each log line.
`trace_context`'s default `otel` format reads only W3C hex ids, so those lines stay uncorrelated
until you set `format: datadog`, which reads `dd.trace_id` and `dd.span_id`:

- **dd-trace-py** (4.15.2, in the trial-org run) writes flat dotted keys: `dd.trace_id` as 32 hex
  characters when the tracer generates 128-bit ids, else decimal, and `dd.span_id` as decimal,
  beside `dd.service`, `dd.env`, and `dd.version`.
- **dd-trace-js** with pino or winston, and **dd-trace-rb** with lograge, nest the same fields
  under a `dd` object (`"dd":{"trace_id":"...","span_id":"..."}`). Put a `flatten` with
  `attributes: [dd]` ahead of `trace_context` to turn that into the dotted names. It's a no-op on
  a line with no `dd` object. No real output from these two libraries has been checked.

Three details catch people out:

- **A line that carries both forms loses one silently.** `flatten` writes `dd.trace_id` over a
  flat `dd.trace_id` already on the event, last write winning, with no counter.
- **A 16-digit id is decimal under `datadog`.** The format decides; nothing is guessed from the
  value. A decimal trace id fills the low 64 bits; name an attribute holding `_dd.p.tid`'s high
  half in `trace_id_high:` if your logs carry it.
- **`datadog_out` sends the log body as the Datadog `message`**, and drops an attribute named
  `message` or `timestamp`, counted `logit.output.tags.omitted{reason="reserved_key"}`. After
  `json`, the body is still the original line. The lifted ids go out as hex `trace_id` and
  `span_id`, which Datadog's log intake correlates on, unless the log already has an attribute of
  either name.

[`fixtures/datadog-logs-correlation.yaml`](../fixtures/datadog-logs-correlation.yaml) runs
`tail_in`, `json`, `flatten`, and `trace_context` into both `datadog_out` and `otlp_out`.

### Credentials

Take `datadog_out`'s `api_key` and `datadog_in`'s `api_keys` from the environment with
`!env DD_API_KEY`, as every example does. `datadog_out` rejects a key with leading or trailing
whitespace at startup, since a key file read with its trailing newline would reach Datadog as a
different key, and it never logs the key. `datadog_in` with `api_keys` empty accepts any key,
including on the Agent's key check, so an Agent can't detect a mistyped key; set `api_keys`. It's
a shared secret, not transport security: add `tls:` before binding beyond loopback.

### Sites

`datadog_out`'s `site` defaults to `datadoghq.com`. Set it to your organization's site:
`datadoghq.eu`, `us3.datadoghq.com`, `us5.datadoghq.com`, `ap1.datadoghq.com`,
`ap2.datadoghq.com`, `uk1.datadoghq.com`, `ddog-gov.com`, or `us2.ddog-gov.com`. A key from one
site gets `403` on another. For `otlp_out` agentless, the endpoint is `https://otlp.<site>` with a
`dd-api-key` header, over HTTP only; Datadog's OTLP intake takes delta metrics, not cumulative.

## What's verified

The codecs and listeners were checked against real traffic recorded from Datadog Agent 7.83.3,
dd-trace-py 4.15.2 (libdatadog 9.0.0) with Flask 3.1.3, and the `datadog` DogStatsD client 0.54.0
over both Unix sockets ([`testdata/interop/datadog/`](../testdata/interop/datadog/README.md)).
Every recorded request decodes and gets its route's `2xx` from the listeners, and a recorded
series request and the recorded tracer requests relay through the two Datadog pairs unchanged.

A Datadog trial org then received, and showed, what `logit` sent it:

- `datadog_out` direct: series, sketches, distribution points, logs, and events, with the stale
  window, the series dedupe, and the size caps above measured against the intake. Service checks
  got `202`, but Datadog has no API to query them back.
- A real Agent pointed at `datadog_in` with `dd_url`, `logs_config.logs_dd_url`, and
  `apm_config.apm_dd_url`, relayed by `datadog_out`: its metrics, logs, and events arrived; its
  spans arrived with their 128-bit trace ids and resource names; and its relayed APM stats
  populated the service's `trace.*` hits, errors, and latency.
- `datadog_trace_out` and `statsd_out` into a real Agent over TCP, UDP, and its Unix
  sockets, with the tracer's request headers accepted.
- A real dd-trace-py log line lifted by `trace_context` under `format: datadog`, found by its
  trace id in Datadog's log search.
- `otlp_out` agentless: a span, a delta sum, and a log.

The Agent's dual-shipping (`additional_endpoints`) and TLS settings against `datadog_in` are
untested. `docs/known-gaps/datadog.md` lists everything else that isn't built or
isn't verified.
