---
created: 2026-10-23
updated: 2026-10-23
---

# Protocol knowledge stays in family: a component keys behavior on another component's internals only within a lossless family

## Status
Accepted

## Context

A component's **internals** are what it writes for its own use or its own pair's, with a meaning
only its own code defines:

- a protocol's private carriers under `_logit.<protocol>.`
  ([ADR `private-attribute-namespace`](private-attribute-namespace.md));
- a protocol's wire-specific public attribute names, such as `collectd.plugin_instance`;
- a native hop's sender identity and sequence
  ([ADR `native-hop-identity-and-sequence`](native-hop-identity-and-sequence.md));
- a batch's provenance, the `origin` and `previous` components it passed through.

Reading another component's internals to decide what to do is an anti-pattern. `aggregate` showed
the cost before [ADR `metric-flags`](metric-flags.md) removed it: four probes for four protocols'
carriers ran on every event with metrics, to answer a question that almost never answered yes.
They coupled a shared transform to four codecs, and they let no other producer ask for the same
handling: a Lua script that built a closed bucket, or a `set` that stamped one, had no way to say
so. A read keyed on one producer's key is also wrong the moment another producer writes that key.
`set` and Lua may write any private key, and the parsing transforms and the unguarded decoders
admit one uncounted (the private-namespace ADR's "Transforms" and "The inbound guard").

[ADR `private-attribute-namespace`](private-attribute-namespace.md)'s "Any component may read the
private namespace" reads as permission for this. Its intent was visibility: the keys
aren't hidden from Lua, `set`, `keep`, `route`, or a whole-event render. Driving logic off a
foreign key wasn't the case it considered.

The lossless pairs are the exception. [ADR `lossless-transit`](lossless-transit.md)'s rule (b)
exists so that a protocol's own sink reads what its decoder stamped, and
[ADR `datadog-agent-and-intake-relay`](datadog-agent-and-intake-relay.md)'s decision 8 shares
`_logit.statsd.event.*` and `_logit.statsd.service_check.*` between DogStatsD and the intake API,
because they are the same Datadog concepts on two wires.

## Decision

A component may key behavior on another component's internals only within a lossless family.

**A family** is one like-protocol pair of [ADR `lossless-transit`](lossless-transit.md)
(`statsd`, `otlp`, `syslog`, `prometheus`, `collectd`, `graphite`, `splunk_hec`, `influxdb`), or
the **Datadog family**: `statsd_in` and `statsd_out` (DogStatsD), `datadog_in` and `datadog_out`,
and `datadog_trace_in` and `datadog_trace_out`. The `statsd` pair belongs to both. `logit_out` and
`logit_in`, with the `buffer.disk:` spool that records hop frames, are the native hop's family:
`logit_in` keys its high-water marks and acknowledgments on `logit_out`'s sender identity and
sequence
([ADR `native-hop-identity-and-sequence`](native-hop-identity-and-sequence.md)). Inside a family
a component reads another member's carriers, wire-specific attribute names, and shared codec
constants freely.

**Outside a family**, a shared transform, a router, an input, or another protocol's sink reads
only:

1. **Neutral meaning segments**: `_logit.metrics.`, `_logit.field.`, and any later segment that
   names a meaning rather than a protocol. The event's creator sets them, per
   [ADR `metric-flags`](metric-flags.md)'s "the creator decides, and the consumer does what the
   flags say".
2. **Public vocabulary**: OpenTelemetry semantic conventions (`host.name`, `http.*`), and a
   destination's own documented attribute names (`com.splunk.*` for Splunk, Datadog's reserved
   `host` and `syslog.hostname` for `datadog_out`).
3. **What its own config names**: a key in `route`, `has_attributes`, `remove`, or `set`, a
   `timestamp` source attribute, a `trace_context` `format:`, or a component id in
   `has_provenance`, `drop_provenance`, or a `route` on `by: {provenance: origin | previous}`.

**"Any component may read a private key" holds for visibility and not for logic.** Lua, `set`,
`keep`, `route`, and whole-event renders (`stdio_out`'s `format: json`, the native wire) see every
private key. Rendering or reporting a value without branching on it, such as `shape`'s `source`
tag, isn't keying behavior on it.

**An exception needs its own ADR** that names it as one, and a section in
[the in-family plan](../plans/protocol-knowledge-stays-in-family.md), which lists every read that
crosses a family.

## Alternatives considered

- **A single like pair as the unit.** Rejected: DogStatsD, Datadog's intake API, and the Agent's APM
  API share events, service checks, hostless marks, and APM stats between them, and the Datadog
  codec defines each once.
  Splitting it by pair would make `datadog_out` define its own copy of `statsd_in`'s event carriers
  for Datadog's own vocabulary.
- **A graph-validation rule pairing input kinds with sink kinds.** Rejected: the event carries the
  knowledge, not the topology. A Lua script, a `set`, or a parsing transform can produce any key
  between any input and any sink, and a native hop or a `replay_in` hides the original input from
  the graph.
- **Leave the existing permission as written.** Rejected: it invites the next probe in a shared
  transform, and each one is invisible to every producer but the one it was written for.

## Consequences

- **No cross-family read remains.** [The in-family plan](../plans/protocol-knowledge-stays-in-family.md)
  found four, and each is closed by a neutral key ("Amendment: move, don't mirror
  (2026-10-23)"): C1 and C2, the Datadog rate carrier `dedup_timestamps` and `prometheus_out`
  read ("The next applications"); C3, the Prometheus family-type carrier `otlp_out` read ("The
  third application"); and C6, `datadog_out` taking `collectd.host` as the host ("The fourth
  application").
- **Four reads are in family and allowed:** C4, `statsd_out` writing an empty DogStatsD `host:` tag
  for `_logit.datadog.hostless`; C5, `datadog_out` and `statsd_out` reading the
  `_logit.statsd.event.*` and `_logit.statsd.service_check.*` carriers `statsd_in` and
  `datadog_in` both write (a DogStatsD `|T` or `d:` timestamp rides as the neutral flag, under
  "Amendment: move, don't mirror (2026-10-23)"); C7, `datadog_out`'s trace readiness reading
  `datadog_trace_in`'s tracer and Agent carriers; C8, `datadog_out` and `datadog_trace_out`
  routing on `_logit.datadog.stats.name`.
- **A new special case goes to the creator.** A shared transform or a foreign sink that needs to
  treat an event specially asks for a neutral flag set by the component that made the event, the
  shape [ADR `metric-flags`](metric-flags.md) took for `aggregate`, so Lua and `set` can ask too.
- [ADR `private-attribute-namespace`](private-attribute-namespace.md) and
  [data-model.md](../design/data-model.md)'s "Well-known attribute names" state the rule in place
  of the old permission.

## Amendment: move, don't mirror (2026-10-23)

**A consumer of private logic prefers the shared neutral flag over a carrier in its own
namespace.** When a neutral flag fully expresses a fact, the decoder writes the flag and no
carrier beside it, and the protocol's own sink reads the flag. A protocol carrier stays only for a
fact with no neutral meaning, such as a Datadog `unspecified` type, an APM stats name, or a
container id.

[ADR `metric-flags`](metric-flags.md) took the mirror form: a decoder wrote a neutral flag beside
its protocol carrier, and the protocol's own sink kept reading the carrier. Where the two say the
same thing, that leaves two sources of truth for one fact. A `remove` or a script that touches one
and not the other splits them, and the protocol's sink keeps a private reading of a fact every
other component reads from the flag.

### The first application: the two timestamp carriers

Both carriers said only "this sample carried its own timestamp", which
`_logit.metrics.closed: "timestamped"` already says. Both are retired:

- **`_logit.prometheus.timestamp`.** A scrape writes only the flag, on a sample with its own
  timestamp. The remote-write receiver writes neither, as before
  (`PrometheusDecoder::with_timestamp_marker(false)`). `prometheus_out` writes a sample's
  timestamp for an event flagged `timestamped`.
- **`_logit.statsd.timestamp`.** `statsd_in` writes only the flag for a `|T` line or an event's or
  service check's `d:` field, and sets `Event::timestamp` from it as before. `statsd_out` under
  `format: dogstatsd` writes `|T<secs>` on a metric line, and `d:<secs>` on an event or a service
  check, for an event flagged `timestamped`, with `secs` the whole seconds of `Event::timestamp`,
  truncated. Under `format: statsd` the omission is counted as before,
  `logit.output.fields.omitted{reason="dialect"}`, once for the timestamp.

Each sink reads the flag as every flag is read: the event's value, else the batch resource's
(`logit_core::attrs::is_closed_timestamped`). **Only the value `timestamped` drives timestamp
emission.** `bucketed`, any other `Str`, and `Bool(true)` close a span for `aggregate`, but none
says the event's timestamp is its source's own, so neither sink writes a timestamp for them.

**The timestamp written is `Event::timestamp`, not a value saved at decode.** The retired statsd
carrier held the wire seconds so that a stage rewriting `Event::timestamp` couldn't move or
fabricate a `|T`. That protection goes, under the creator-decides rule: the flag says the
timestamp is the source's, and the event's timestamp is the point's time. A `timestamp` transform
or a Lua script that rewrites `Event::timestamp` now moves the `|T` with it, and a script that sets
the flag gets the `|T` it asked for.

**The cross-protocol consequence is intended.** A `prometheus_in` scrape sample with its own
timestamp now leaves `statsd_out` with a `|T`, and a `statsd_in` `|T` line now leaves
`prometheus_out` with a sample timestamp. Before, neither did, since each sink read only its own
protocol's carrier. Both are the same fact on two wires, which is what a neutral flag is for. The
transit spec's `attributes/timestamped` row in `spec/transit/mapping.yaml` states it per sink.

### The next applications: the Datadog rate and interval carriers

[The in-family plan](../plans/protocol-knowledge-stays-in-family.md)'s C1 and C2 remedy takes the
same form, and closes both reads. `datadog_in` writes the neutral `_logit.metrics.type: rate` and
`_logit.metrics.interval` instead of `_logit.datadog.type: rate` and `_logit.datadog.interval`,
and `_logit.datadog.type` keeps only `unspecified`, the one value with no neutral meaning
([ADR `metric-flags`](metric-flags.md)'s "The flags" defines both keys).

- **`dedup_timestamps` and `prometheus_out`** read the flags through `logit_core::attrs::is_rate`
  and `logit_core::attrs::interval` and import nothing from `logit_proto::datadog`
  ([ADR `dedup-timestamps-transform`](dedup-timestamps-transform.md)'s "Amendment: a rate is a
  neutral flag (2026-10-23)").
- **`datadog_out`** reads the flags too: a `Gauge` flagged `rate` leaves as a `rate`, and
  `_logit.metrics.interval` as the series `interval` of any type. A rate a Lua script or a `set`
  flags therefore reaches Datadog as a `rate`, which is the creator-decides rule working, not a
  leak ([ADR `datadog-agent-and-intake-relay`](datadog-agent-and-intake-relay.md)'s "Amendment:
  the rate and the interval are neutral flags (2026-10-23)").
- **A retired carrier says nothing.** A `Gauge` carrying only `_logit.datadog.type: rate` and
  `_logit.datadog.interval` is a state at `dedup_timestamps` and `prometheus_out`, and leaves
  `datadog_out` as a `gauge` with no interval.

### The third application: the Prometheus family types

[The in-family plan](../plans/protocol-knowledge-stays-in-family.md)'s C3 remedy takes the same
form. `prometheus_in` writes `_logit.metrics.type: info`, `stateset`, or `gaugehistogram` for
those three OpenMetrics family types, instead of `_logit.prometheus.type`, and
`_logit.prometheus.type` keeps only `untyped` and `unknown`: "the family declared no type" has
no neutral meaning. No event carries both keys. An attribute, not a field on `MetricRecord`, so
`Event` doesn't grow and Lua and `set` can write it
([ADR `metric-flags`](metric-flags.md)'s "The flags" defines the values).

- **`otlp_out`**, and `kafka_out`'s OTLP formats through the same encoder, read the flag through
  `logit_core::attrs::metric_type` and import nothing from `logit_proto::prometheus`: a
  `Histogram` flagged `gaugehistogram` is skipped, counted, and a `Gauge` flagged `info` or
  `stateset` is a cumulative non-monotonic `Sum`, `info` named `<name>_info`
  ([ADR `prometheus-scrape-and-exposition`](prometheus-scrape-and-exposition.md)'s "Amendment
  (2026-10-23): the family types are a neutral flag").
- **`prometheus_out`** reads the flag for the three types and its own carrier for `untyped` and
  `unknown`, so an info or stateset gauge a Lua script or a `set` flags exposes with its
  `# TYPE` line.
- **Each reader keeps its kind rule.** `info` and `stateset` act on a `Gauge` and
  `gaugehistogram` on a `Histogram`; a value on another kind is ignored.
- **A retired value says nothing.** A record carrying only `_logit.prometheus.type: info`,
  `stateset`, or `gaugehistogram` is an ordinary gauge or histogram at `otlp_out` and
  `prometheus_out`.

### The fourth application: the host

[The in-family plan](../plans/protocol-knowledge-stays-in-family.md)'s C6 remedy moves collectd's
host to a public key every family shares rather than to a flag: a host is identity, not a
directive. `collectd_in` writes a value list's or notification's Host part as `host.name`, the
standard host key, instead of `collectd.host`, and `collectd_out` reads `host.name` as its Host.
`datadog_out` reads no `collectd.host` on any route, and `host.name` reaches it as every other
producer's does ([ADR `process-hostname`](process-hostname.md)'s "Amendment: `host.name` is the
standard host key (2026-10-23)").

- **No sink reads another family's host key.** Each reads `host.name` first, then its own
  family's wire forms, and falls back to the process hostname or its own `hostname:`
  ([ADR `process-hostname`](process-hostname.md)'s "Amendment: `host.name` ranks first
  everywhere (2026-10-23)").
- **A retired key says nothing.** An attribute named `collectd.host` is an ordinary attribute at
  every sink, `collectd_out` and `datadog_out` included.

The syslog family follows on the same key. `syslog_in` writes each line's HOSTNAME as the event
attribute `host.name`, and `syslog_out` reads it off the event, else the batch resource, as
HOSTNAME ([ADR `syslog-output`](syslog-output.md)'s "Amendment: HOSTNAME is `host.name`
(2026-10-23)"). A syslog line's host reaches every other sink as any producer's does, and any
producer's host reaches `syslog_out` with no syslog key set. An attribute named `syslog.hostname`
is ordinary at `syslog_out`. `datadog_out` still reads one on a log, because `syslog.hostname` is
Datadog's own reserved log attribute: public vocabulary under the Decision's second item, not a
syslog-family key.
