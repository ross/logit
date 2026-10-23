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
- a protocol's wire-specific public attribute names, such as `collectd.host`;
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

- **Four cross-family reads are recorded debt**, each with a remedy in
  [the in-family plan](../plans/protocol-knowledge-stays-in-family.md):
  - C1: `dedup_timestamps` sums a `Gauge` carrying `_logit.datadog.type: rate` as an increment
    ([ADR `dedup-timestamps-transform`](dedup-timestamps-transform.md)'s "Which reading survives").
  - C2: `prometheus_out` counts a same-millisecond Datadog rate as an increment
    (the same ADR's "The sink-side counter").
  - C3: `otlp_out`, and `kafka_out`'s OTLP formats through the same encoder, read
    `_logit.prometheus.type` to skip a `gaugehistogram` and to write `info` and `stateset` as a
    non-monotonic `Sum`
    ([ADR `prometheus-scrape-and-exposition`](prometheus-scrape-and-exposition.md)'s 2026-10-21
    amendment).
  - C6: `datadog_out` takes `collectd.host` as the host
    ([ADR `process-hostname`](process-hostname.md)'s "Amendment: `datadog_out` reads
    `collectd.host` (2026-10-22)").

  `docs/known-gaps/runtime.md` lists them as one gap until the plan closes them.
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

### The next applications

[The in-family plan](../plans/protocol-knowledge-stays-in-family.md)'s C1 and C2 remedy takes the
same form: `datadog_in` writes the neutral `_logit.metrics.type: rate` and
`_logit.metrics.interval` instead of mirroring them, and `_logit.datadog.type` keeps only
`unspecified`, the one value with no neutral meaning.
