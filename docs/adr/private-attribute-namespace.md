---
created: 2026-10-13
updated: 2026-10-23
---

# Private attribute namespace: protocol carriers live under `_logit.<protocol>.`, and every other protocol's sink ignores them

## Status
Accepted. On 2026-10-19, [ADR `influxdb-line-protocol-relay`](influxdb-line-protocol-relay.md)'s
"Amendment: string fields are private (2026-10-19)" moved InfluxDB string fields from the public
`influxdb.field.<key>` to `_logit.influxdb.field.<key>`; the lists below are amended to match. On 2026-10-22, "Amendment: a segment that names the key's
meaning (2026-10-22)" moved that key to the protocol-free `_logit.field.<key>`, which the lists
below don't cover. On 2026-10-23, "Amendment: reading a private key is in-family (2026-10-23)"
narrowed "Any component may read the private namespace" to visibility, rewritten below to match.

## Context

[ADR `lossless-transit`](lossless-transit.md)'s rule (b) lets a decoder stamp a protocol's raw
encoding of something the model normalizes as a protocol-namespaced attribute, so the same
protocol's sink can reproduce the original wire form: `statsd.type`, `prometheus.timestamp`,
`collectd.plugin`, `influxdb.measurement`, `datadog.interval`, `datadog.tracer.*`, and others.
Each like-protocol pair is a fixed point because of them.

At every other sink they're ordinary attributes, and which ones a sink renders is decided sink by
sink:

- `influxdb_out` skips its own carriers and `statsd.*`.
- `graphite_out` skips `statsd.*` and `collectd.*`.
- `prometheus_out` skips only `prometheus.*`.
- `datadog_out`'s series encoder skips a fixed list of 15 keys.
- `otlp_out` skips only its own `otel.severity_*` carriers.

So the input ahead of a sink changes that sink's series identity. A `statsd_in -> collectd_out`
pipeline counts `statsd.type` as a tag with no wire form, a `collectd_in -> prometheus_out`
pipeline grows a `collectd_plugin` label, and an `influxdb_in -> otlp_out` pipeline sends
`influxdb.measurement` as an OTLP attribute. The documented workaround, a `remove` ahead of each
foreign sink, needs the operator to know every carrier of every protocol upstream, and has to be
revisited whenever a codec adds one.

Rule (c) of the same ADR reserves `logit.*` for the opposite case: a `logit`-specific concept that
has to cross a foreign wire because it has nowhere else to go (`logit.body_format`). Nothing named
the case of a key that must never cross one.

## Decision

**Carriers live under one private prefix, `_logit.`, with the protocol segment kept:
`_logit.statsd.type`, `_logit.prometheus.type`, `_logit.influxdb.measurement`.** Every
protocol sink ignores `_logit.*` when it renders tags, labels, fields, or attributes, except its
own `_logit.<protocol>.*` keys, which it reads by exact name. There is no list to maintain.

`logit_core::attrs::PRIVATE_PREFIX` and `logit_core::attrs::is_private` are the one definition of
the prefix. Every check in the codebase calls `is_private`; nothing spells the prefix out again.
`logit_core::attrs::merged` doesn't filter, because each sink reads its own carriers through it.

### What is private

A key is private when it's one of these:

- A protocol's raw wire spelling of something the model already normalizes into a typed field: a
  severity, a metric kind's wire type, the fact that the wire carried its own timestamp, a name
  the model folds into `MetricRecord.name`.
- A payload or envelope field that only that protocol's family reads: a tracer's request
  headers, an InfluxDB bucket.

Everything else a decoder stamps is public data that any sink may render.

By protocol, these move under `_logit.`:

| Protocol | Private keys |
|---|---|
| statsd | every `statsd.*` attribute: `type`, `container_id` (since `container.id` and `cgroup_inode`, [ADR `process-hostname`](process-hostname.md)'s "Amendment: container identity (2026-10-24)"), `external_data`, `cardinality`, `timestamp`, `event.*`, `service_check.*` |
| Prometheus | every `prometheus.*` attribute: `type`, `timestamp`, and the resource attribute `target` |
| collectd | `plugin`, `type`, `interval`, and `severity` |
| Datadog | every `datadog.*` attribute: series, events, service checks, Agent fields, trace chunk, stats, tracer headers, `span.error`, and `source` |
| InfluxDB | `influxdb.measurement` and the resource attributes `influxdb.org`, `influxdb.org_id`, and `influxdb.bucket`; string fields live under `field.<key>`, no longer a protocol segment (the 2026-10-22 amendment) |
| syslog | `syslog.severity` |
| OTLP | `otel.severity_number` and `otel.severity_text` |

These stay public:

- collectd's host, `collectd.plugin_instance`, and `collectd.type_instance`: identity with no
  other home. The record name holds only the plugin and type, and the host stays off the
  resource, so hiding them would merge different hosts' and instances' lists into one series.
  The host rides on the standard `host.name`
  ([ADR `process-hostname`](process-hostname.md)'s "Amendment: `host.name` is the standard host
  key (2026-10-23)").
- `syslog.timestamp`: the sender's own stamp. `Event.timestamp` is receipt time until a
  `timestamp` component resolves it, so the attribute duplicates nothing, and an operator names it
  in that component's config.
- `syslog.tag`, `syslog.pid`, `syslog.msgid`, `syslog.sd`, and `syslog.facility`: header facts
  with no typed home that any destination can use. HOSTNAME rides on the standard `host.name`
  ([ADR `syslog-output`](syslog-output.md)'s "Amendment: HOSTNAME is `host.name` (2026-10-23)").
- `com.splunk.*` and `otel.log.*`: the OpenTelemetry `splunk_hec` exporter's public vocabulary,
  which [ADR `splunk-hec-relay`](splunk-hec-relay.md) adopts.
- `http.*`: OpenTelemetry semantic conventions.
- `logit.body_format`, and any other rule (c) key: public by design, because it exists to cross a
  foreign wire. `_logit.` is its opposite.
- `_dd.*`: Datadog's own span meta and metrics keys, which Datadog shows as data.

Datadog metric names under `datadog.stats.*` and protobuf package names aren't attributes, and
this decision doesn't touch them.

### The ignore rule

When a protocol sink renders an event's or a resource's attributes as tags, labels, fields, or
attributes, it skips every top-level key `is_private` accepts, except its own
`_logit.<protocol>.*` keys. The skip is silent and uncounted: no `omitted`, `no_wire_form`, or
`dialect` counter ever counts a private key, and no diagnostic names one. A private key isn't a
tag the sink failed to write; it was never a tag.

The rule applies to top-level attribute keys only. A `_logit.` key nested inside a `Value::Map`
attribute is data and renders like any other map key.

Renders whose purpose is the whole event carry everything, private keys included:

- `stdio_out` and `file_out`, under the human render, `format: json`, and `format: native`.
- `kafka_out` under `format: json`, `msgpack`, and `native`.
- The native wire: `logit_out`, `logit_in`, and the `buffer.disk:` spool.

`kafka_out`'s `otlp_proto` and `otlp_json` formats render attributes as OTLP does, and follow the
rule as `otlp_out` does.

### Any component may read the private namespace

The ignore rule governs generic rendering only. No component is kept from seeing a private key:
Lua, `set`, `keep`, `route`, and the whole-event renders above see every one. Keying behavior on
another component's carrier is narrower, and is allowed only within a lossless family ("Amendment:
reading a private key is in-family (2026-10-23)").

### Transforms

- `keep` drops `_logit.*` like any key not in `fields`, unless `private: keep`, which retains the
  whole private namespace alongside `fields`. A private key retained this way counts as kept in
  `logit.transform.attributes.kept`. The default is `drop`, so a `keep` ahead of `aggregate`
  bounds series cardinality the same way whatever the input.
- `flatten`'s `all` (for `attributes` or `resource`) never selects a private key. A private key
  named in a list still flattens.
- `aggregate` keys series on private keys unchanged, as on any attribute.
- `remove`, `route`, `has_attributes`, `drop_attributes`, `set`, and Lua name private keys
  explicitly, with no prefix matching. A private key may therefore appear in operator config:
  `set` stamping `_logit.metrics.interval`, or a `route` on `_logit.influxdb.bucket`.

### The inbound guard

The decoders for the five tag-bearing metric wires drop a key under `_logit.` and count it as
`logit.input.tags.omitted{reason="reserved_prefix"}`; the point itself is kept:

- DogStatsD `|#` tags, on metrics, events, and service checks.
- Datadog series, sketch, event, and service-check tags.
- InfluxDB line-protocol tags.
- Prometheus remote-write labels.
- carbon `;k=v` tags.

Without the guard, a sender's tag would be read back as carrier data at that carrier's own sink.

Every other decoder admits the key uncounted: OTLP metric data-point attributes and Splunk HEC
metric `fields` included, the log and span decoders, and the parsing transforms (`json`,
`logfmt`, `kv`, `csv`, `regex`). A top-level `_logit.*` key they admit is taken as the carrier
it names by that carrier's family. The guard generalizes a counter `influxdb_in` already keeps
for its own prefix; the other decoders have no tag-omission counter, and a producer has to choose
the `_logit.` prefix to reach a carrier. Under [ADR `deployment-threat-model`](deployment-threat-model.md),
that makes it a documented gap rather than a guard to add.

## Alternatives considered

- **A dedicated carrier field on `Event`.** Either a boxed map, which costs an allocation on every
  carrier-bearing event, or inline storage, which adds bytes to every event whether it carries
  one or not. [ADR `minimize-allocations-over-event-size`](minimize-allocations-over-event-size.md)
  ranks allocations first, `docs/design/memory.md`'s "What was reclaimed, and what was
  deliberately not" records two boxing changes measured and reverted on those grounds, and
  [ADR `event-sizing-and-allocation-strategy`](event-sizing-and-allocation-strategy.md) requires a
  perf VM measurement per signal class before any sizing change. A field would also hide carriers
  from Lua, `route`, `set`, and `remove`, which address attributes.
- **Bits in `MetricRecord.flags`.** `flags` is OTLP's `DataPointFlags`, written to the wire as
  is, and `aggregate` zeroes it on a merge. Most carriers aren't booleans (a measurement name, a
  five-tuple, a tracer header), log and span carriers have no `MetricRecord`, and a bit is
  invisible to Lua, `route`, and `remove`.
- **Per-sink skip lists (the status quo).** Each sink knows a different subset of foreign
  carriers, so the output depends on the input, and every new carrier has to be added to every
  other sink's list. The `remove` workaround moves the same list into every operator's config.
- **Prefix matching on `remove`.** It makes the workaround shorter but leaves it the operator's
  job, at every foreign sink, in every config.
- **Filtering inside `attrs::merged`.** Every sink reads its own carriers through `merged`, so a
  filter there hides them from the one sink that needs them; it would need an exception per sink,
  which is the ignore rule written somewhere less visible.
- **Reusing `logit.*`.** Rule (c) keys are meant to cross a foreign wire, and carriers never
  should. One prefix for both would need a second list to tell them apart.
- **One private segment with no protocol (`_logit.type`).** statsd, Prometheus, and collectd each
  carry a `type`, with different meanings. The protocol segment keeps them apart and gives each
  sink an exact own-prefix to read.

## Consequences

- Every carrier listed above is renamed in its decoder, its own sink, tests, fixtures, fuzz seeds,
  and docs. `logit` is pre-release, so there are no aliases and no decode path for the old names.
- Each protocol sink's render site gets one `is_private` check after its own-prefix arm and before
  any counter. The per-sink foreign skip lists (`influxdb_out`'s `statsd.` skip, `graphite_out`'s
  `FOREIGN_CARRIER_PREFIXES`) go away.
- `docs/design/data-model.md`'s "Well-known attribute names" holds the per-key carrier tables
  and the canonical statement of these rules; module docs and other docs point at it.
- `docs/known-gaps/mappings.md` closes its rows that describe a carrier rendered as an ordinary
  attribute at a foreign sink, and gains one for the unguarded decoders above.
- A relay that runs `keep` between a like-protocol pair needs `private: keep` to stay a fixed
  point.
- A new decoder that stamps a carrier spells it `_logit.<protocol>.<name>`, and a new sink calls
  `is_private` at its render site. Neither needs a change anywhere else.

## Amendment: a segment that names the key's meaning (2026-10-22)

The namespace has a second kind of segment. `_logit.field.<key>` names what the key is, data about
a point and not identity of its series, instead of a protocol. Any sink that can carry a
non-identity string may read it: `influxdb_out` writes a `Str` under it as a string field. The
ignore rule covers it like any private key, so every other protocol sink skips it silently, and
a key under it isn't a carrier of any protocol. The prefix has one definition,
`logit_core::attrs::FIELD_PREFIX`, tested by `logit_core::attrs::is_field`. See
[ADR `metric-flags`](metric-flags.md) and
[ADR `influxdb-line-protocol-relay`](influxdb-line-protocol-relay.md)'s 2026-10-22 amendment.

## Amendment: the `_logit.metrics.` flag segment (2026-10-22)

`_logit.metrics.` joins `_logit.field.` as a segment that names the signal a key describes rather
than a protocol. Its keys are flags: the component that creates an event sets one, and a reader
such as `aggregate` acts on it instead of on a protocol carrier. `_logit.metrics.closed` and
`_logit.metrics.fields` were the first two; every flag's values and readers are
[ADR `metric-flags`](metric-flags.md)'s "The flags". The ignore rule covers them like any private
key, so no protocol sink renders a flag, and `keep` under its default `private: drop` strips them.
The constants live in `logit_core::attrs` beside `PRIVATE_PREFIX` and `FIELD_PREFIX`.

## Amendment: reading a private key is in-family (2026-10-23)

"Any component may read the private namespace" meant that the keys aren't protected or hidden:
Lua, `set`, `keep`, `route`, and a whole-event render see every private key. It didn't mean that a
component may drive logic off another protocol's carrier, and its two examples read as if it did.

[ADR `protocol-knowledge-stays-in-family`](protocol-knowledge-stays-in-family.md) states the rule.
A component keys behavior on another component's carriers only within a lossless family: one
like-protocol pair, or the Datadog family of DogStatsD (`statsd_in`, `statsd_out`), `datadog_in`
and `datadog_out`, and `datadog_trace_in` and `datadog_trace_out`. `datadog_out` reading the
`_logit.statsd.event.*` carriers that `statsd_in` and `datadog_in` both write is in family.
Outside a family, a component reads the neutral segments (`_logit.metrics.`, `_logit.field.`),
public vocabulary, and its own config. `collectd_out` reading `_logit.prometheus.type` would be
outside one, and the reads of that kind that exist today are listed as debt in
[the in-family plan](../plans/protocol-knowledge-stays-in-family.md).
