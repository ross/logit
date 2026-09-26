# Pipeline graph

How config wires components together and how the runtime executes the result. Decision record:
[ADR `component-graph-configuration`](../adr/component-graph-configuration.md). This document is load-bearing per
`AGENTS.md` — read it before touching `logit-config`'s component types or the pipeline runtime.

## Config shape

One flat map. Every component has an id (the map key), a `type`, and a `sources` list naming the
other components it reads from:

```yaml
components:
  metrics_in:
    type: statsd_in
    bind: 0.0.0.0:8125

  windowed:
    type: aggregate
    sources: [metrics_in]
    interval: 10s

  enrich:
    type: lua
    sources: [windowed]
    script: |
      function process(event)
        event.attributes.env = event.attributes.env or "dev"
        return event
      end

  influx:
    type: influxdb_out
    sources: [enrich]
    url: http://influxdb:8086
    org: logit
    bucket: metrics
    token: !env INFLUXDB_TOKEN
```

This is the statsd → aggregate → Lua → InfluxDB shape of
[fixtures/statsd-to-influxdb.yaml](../../fixtures/statsd-to-influxdb.yaml). There is no `inputs`/
`outputs`/`pipelines` split and no separate `transforms:` chain: `sources` carries all the wiring.
A "pipeline" is whatever subgraph is reachable from a listener; config has no notion of one.

In Rust (a sketch; `crates/logit-config/src/lib.rs` has the full `ComponentKind`):

```rust
pub struct Config {
    #[schemars(schema_with = "non_empty_components_schema")]
    pub components: HashMap<String, Component>,
}

pub struct Component {
    #[serde(default)]
    pub sources: Vec<String>,
    // The `target` components a router directs events into, in slot order
    // (docs/adr/target-components.md). Legal only on `lua`/`lua_file`.
    #[serde(default)]
    pub targets: Vec<String>,
    #[serde(flatten)]
    pub kind: ComponentKind,
}

#[serde(tag = "type", rename_all = "snake_case")]
pub enum ComponentKind {
    StatsdIn { bind: String },
    SyslogIn { bind: String },
    OtlpIn { bind: String },
    TailIn { paths: Vec<String>, #[serde(flatten)] tail: TailOptions },
    DockerIn { root: String, containers: Vec<String>, discover: bool, labels: Vec<String>, #[serde(flatten)] tail: TailOptions },
    LogitIn { bind: String },

    Lua { script: String, interval: Option<Duration> },
    LuaFile { lua_file: String, interval: Option<Duration> },
    Aggregate { interval: Duration },
    Json { skip_to_brace: bool },
    // Turns attributes into metrics on the same event (docs/adr/kv-metrics-semantics.md).
    // Deliberately no `tags:` field -- tag selection is `Keep`'s job.
    KvMetrics { counters: Vec<MetricSpec>, gauges: Vec<MetricSpec>, distributions: Vec<MetricSpec> },
    // An allowlist: retains only the named attributes. Place before `aggregate` -- its
    // `SeriesKey` includes the whole attribute set, so pruning first bounds cardinality.
    Keep { fields: Vec<String> },
    // A denylist: drops the named attributes, keeping the rest.
    Remove { fields: Vec<String> },
    // Drops an event that doesn't carry a wanted signal -- never mutates a forwarded event
    // (docs/adr/signal-filtering-components.md).
    HasSignal { signals: Vec<Signal>, mode: MatchMode },
    // Retains only the listed signals' payloads, clearing the rest -- an allowlist, `has_signal`'s
    // mutating counterpart.
    KeepSignals { signals: Vec<Signal> },
    // A denylist: clears the listed signals' payloads, keeping the rest.
    DropSignals { signals: Vec<Signal> },
    // Forwards an event whose resource/attributes match every configured pair -- config is
    // `Set`'s, field for field; never mutates (docs/adr/attribute-filtering-components.md).
    HasAttributes { resource: BTreeMap<String, SetValue>, attributes: BTreeMap<String, SetValue> },
    // Drops an event matching every configured pair -- has_attributes' exact complement on the
    // identical config, taken at the top level (an event matching some-but-not-all pairs forwards).
    DropAttributes { resource: BTreeMap<String, SetValue>, attributes: BTreeMap<String, SetValue> },
    // Forwards an event whose batch's origin/previous match every configured field -- each field
    // is a list of alternatives (OR'd within the field), the two fields AND together; never
    // mutates (docs/adr/provenance-filtering-components.md).
    HasProvenance { origin: Vec<String>, previous: Vec<String> },
    // Drops an event whose batch's origin/previous match every configured field -- has_provenance's
    // exact complement on the identical config, taken at the top level.
    DropProvenance { origin: Vec<String>, previous: Vec<String> },
    // Clamps attribute/resource-attribute values to a per-field allow-list -- `keep`'s value-side
    // sibling. A disallowed value becomes that field's `other` (or is removed if absent); an
    // optional, ordered `normalize` step (today just lowercasing) runs before the allow test and
    // is written back (docs/adr/value-allowlist-cardinality-clamp.md).
    KeepValues { resource: BTreeMap<String, ValueAllowList>, attributes: BTreeMap<String, ValueAllowList> },
    // Matches a pattern against a log message (or a named attribute), turning named capture
    // groups into attributes (docs/adr/regex-transform.md).
    Regex { pattern: String, field: Option<String> },
    // Rewrites each event into a measurement of its own shape -- counts and lengths only, never a
    // key or a value -- with per-batch and cumulative measurements on `interval`. Belongs on its
    // own fan-out branch, never in the flow it measures (docs/adr/shape-observer-component.md).
    Shape { interval: Duration, resource: ShapeResource, max_tracked_keys: usize,
            max_tracked_keysets: usize },
    // Rewrites a nested Map/Array attribute into flat, dot-joined keys (foo.key, tags.0) -- an
    // opt-in, operator-placed rewrite, never a decoder or sink behavior
    // (docs/adr/flatten-transform.md). `attributes`/`resource` are each `all`/`none`/a named list.
    Flatten { attributes: FlattenFields, resource: FlattenFields, arrays: FlattenArrays },
    // Normalizes a web server's access line, logged under raw OTel semconv names, into its
    // conformant form plus a bounded derived set (user_agent.class, http.route, span.name, ...);
    // placed between `json` and `trace_context` (docs/adr/http-access-normalization.md).
    HttpAccess { routes: Vec<HttpRouteRule>, route_other: Option<String>,
                 user_agent_rules: Vec<UserAgentRule>, max_length: BTreeMap<String, usize>,
                 redact_query: Vec<String>, forwarded: Option<ForwardedConfig> },
    // Splits each row of a delimited line into positional attributes named by a configured
    // `columns` list (docs/adr/csv-positional-columns.md).
    Csv { columns: Vec<String>, delimiter: char },
    // Equality-only routing: one key read per event, one target per matching value
    // (docs/adr/target-components.md). An unrouted event goes to this component's ordinary
    // consumers.
    Route { by: RouteBy, routes: BTreeMap<String, String> },
    // Keeps a fraction of events by a hashed key (trace_id, an attribute, or a resource
    // attribute), so every event sharing a key gets the same verdict in every process;
    // `always_keep` pins flagged events through (docs/adr/consistent-sampling-component.md).
    Sample { rate: f64, key: Option<SampleKey>, missing: Option<SampleMissing>,
             always_keep: Option<SampleOverride> },
    // as each lands in logit-transforms, same shape: a `ComponentKind` variant, no `sources`
    // opinion of its own (that lives on `Component`, uniformly). `rename`/`filter`/`throttle`/
    // `dedup` used to be sketched here too -- retired before landing, not merely deferred: each is
    // already expressible as a `lua` component, and `docs/adr/routing-by-condition-is-lua.md`
    // records why a native equivalent wasn't worth building yet. `sample` was retired with them
    // and came back once keyed, cross-process consistency turned out to be the part `lua` can't
    // express.

    InfluxDbOut { url: String, org: String, bucket: String, token: String },
    OtlpOut { endpoint: String },
    LogitOut { endpoint: String },
    // A named destination a router directs events into (docs/adr/target-components.md). No
    // fields, and no `sources` -- fed by direction, never by naming anything itself.
    Target {},
}
```

**Naming: every protocol kind is suffixed `_in`/`_out`.** One internally-tagged enum can't hold
both `Otlp { bind }` (a listener) and `Otlp { endpoint }` (a sink) as `type: otlp`, and the two
`Logit` variants collide the same way. Every protocol kind takes the suffix, not only the ones that
collide, so the rule stays predictable when a protocol gains its second side later — as
`syslog_out` (RFC 3164/5424 over UDP or TCP, `docs/adr/syslog-output.md`) did after `SyslogIn`.
Transform kinds — `lua`, `lua_file`, `aggregate`, `json`, `csv`, `kv_metrics`, `keep`,
`remove`, `set`, `trace_context`, `scale`, `has_signal`, `keep_signals`, `drop_signals`,
`has_attributes`, `drop_attributes`, `has_provenance`, `drop_provenance`, `keep_values`, `logfmt`,
`kv`, `regex`, `shape`, `flatten`, `http_access`, `sample`, `route`, and any future native
transform — take no suffix, because a transform has only one direction.

**`interval` is a per-kind field.** `lua`/`lua_file` carry an optional flush interval
(`docs/adr/aggregation-window-semantics.md`); `aggregate`, `internal`, `shape`, and
`prometheus_in` require one. Rule 9 rejects a zero `interval` on any kind that has one, via
`graph.rs`'s `interval()` table.

**`Component` flattens `ComponentKind` with `#[serde(flatten)]`.** `schemars` 0.8 (pinned in
`Cargo.toml`) emits that as an `allOf` composition in `schema/logit.schema.json`. Every shipped
config loads through it with `serde_norway`, but no test validates those configs against the
emitted schema itself. If `flatten` ever misbehaves, the fallback is
repeating `sources: Vec<String>` on every `ComponentKind` variant instead of factoring it onto
`Component`.

## Environment substitution

`!env VAR_NAME` is a YAML tag, valid as the value of any field on any component. It resolves
against the process environment when the config loads (`crates/logit-cli/src/config.rs`,
[ADR `env-yaml-tag`](../adr/env-yaml-tag.md)). `influxdb_out`'s `token` above uses it. There is no
dedicated `token_env` field (the ADR records why that design was rejected); any secret or
deployment-specific field is written the same way:

```yaml
url: !env INFLUXDB_URL
token: !env INFLUXDB_TOKEN
```

Resolution happens on the parsed YAML tree before serde sees it, so `Config`'s types carry no
trace of it: to serde, a `!env`-tagged field is identical to one written with the value inline.
The substituted value is re-parsed as a YAML scalar: `8125` becomes an integer and `true` a bool;
anything else, including a value that looks like a mapping or sequence, stays a string. That lets
`!env` fill a non-string field. The cost: a secret that looks like a number or bool must be quoted
at the source.

Every `!env` reference must resolve for `run`, `validate`, and `graph` alike — including `logit
graph`, which never reads a component's field values (only `sources` and `type`). Any tag other
than `!env` is a hard error, because a typo'd tag would otherwise deserialize silently as its
literal argument string.

## Roles come from kind, not topology

| Kind class | `sources` | May be another component's source |
|---|---|---|
| Listener (`statsd_in`, `collectd_in`, `graphite_in`, `syslog_in`, `otlp_in`, `tail_in`, `docker_in`, `logit_in`, `internal`, `prometheus_in`, `generate_in`) | must be empty | required (≥1 consumer) |
| Transform (`lua`, `lua_file`, `aggregate`, `json`, `csv`, `kv_metrics`, `keep`, `remove`, `set`, `trace_context`, `scale`, `has_signal`, `keep_signals`, `drop_signals`, `has_attributes`, `drop_attributes`, `has_provenance`, `drop_provenance`, `keep_values`, `logfmt`, `kv`, `regex`, `shape`, `flatten`, `http_access`, `sample`, `route`) | ≥1 required | required (≥1 consumer) |
| Sink (`influxdb_out`, `stdio_out`, `file_out`, `otlp_out`, `syslog_out`, `logit_out`, `statsd_out`, `collectd_out`, `graphite_out`, `prometheus_out`, `null_out`) | ≥1 required | must not be |
| Target (`target`) | must be empty | required (≥1 consumer), and ≥1 directing router (rule 49) |

Rule 50 relaxes rule 7's "≥1 consumer" for *routers* only. A router's ordinary consumers receive
only its *unrouted* events, so a router may have none; its unrouted events are then dropped and
counted, never lost silently ([ADR `target-components`](../adr/target-components.md)).

Role is never derived from topology ("no sources → listener", "nothing reads it → sink"). ADR
`component-graph-configuration` rejected that because a typo'd source reference would silently
turn a real sink into an orphaned transform instead of failing with "did you mean". The kind
already knows its arity; config only states the edges.

## Routing: `route` and `target`

A router sends each event to a named `target` component, so "send these events here and those
there" needs no filter per branch and no consumer sees batches it doesn't want
([ADR `target-components`](../adr/target-components.md)):

```yaml
components:
  central_in:
    type: logit_in
    bind: 0.0.0.0:5150

  split:
    type: route
    sources: [central_in]
    by: {attribute: stream}
    routes:                  # value -> target id
      host: host_stream
      app: app_stream

  host_stream: {type: target}
  app_stream:  {type: target}

  untagged_out:
    type: stdio_out
    sources: [split]        # the router's own consumers: everything no route claimed
    target: stderr
```

`route` reads one key per event. `by:` is exactly one of `{provenance: origin}`,
`{provenance: previous}`, `{attribute: <key>}`, or `{resource: <key>}`, and `routes:` maps that
key's values onto `target` ids; several values may map to one target. An event whose value no
route names, or that lacks the key, is *unrouted*: it goes to the router's own consumers
(`untagged_out` above, which names the router in `sources:`), not through a chain of complementary
filters. A `target` has no fields and no `sources:`. A router points at it, and downstream
components read it like any other source (for example, `windowed: {sources: [host_stream]}`).
`lua`/`lua_file` is the other router kind: it picks a target per event with `event:to("id")`
instead of an equality table (see `docs/design/lua-api.md`'s "Routing to a target."). A complete,
runnable version of the config above is `fixtures/fan-out-central.yaml`.

## Validation

`graph::resolve` (`crates/logit-pipeline/src/graph.rs`) enforces these rules; `logit-cli`'s
`validate_semantics` (`crates/logit-cli/src/pipeline.rs`) is a thin wrapper around it. The
canonical list, with each rule's reasoning, is `graph.rs`'s module doc; each rule is enforced at
its `// Rule N` label in `resolve`. Each entry below names what is rejected; the numbers are
identifiers, not execution order. Several rules share one principle: **a config that can only be
a no-op, a black hole, or an impossible bound is an error**, and so is a setting that would be
silently ignored. `0` for a count or duration bound is usually impossible, not small.

1. No components at all.
2. A `sources` id naming no defined component.
3. A component listing itself as a source.
4. A repeated id in one `sources` list, which would deliver every batch twice.
5. A cycle, through `sources` or router -> target edges; the error names one concrete cycle.
6. Wrong arity for the kind's role (the table above), or a sink named as a source.
7. A non-sink component with no consumer.
8. A kind that isn't implemented.
9. A zero `interval` on a kind that has one.
10. A `kv_metrics` with no counters, gauges, or distributions.
11. A `kv_metrics` distribution with no `field`, or an entry with an empty `name`.
12. A `set` with neither `resource` nor `attributes`, or with an empty key.
13. More than one `internal` component.
14. A non-default `buffer:` on a non-sink.
15. A sink `buffer.max_batches` or `buffer.max_bytes` of `0`.
16. An `internal` `span_sample_rate` that is non-finite or outside `[0, 1]`.
17. A non-default `receive:` outside a datagram, stream, or tail listener, or a receive-queue field
    on a stream or tail listener.
18. A `0` receive-queue or batch-assembly bound (`batch_flush_interval: 0s` is legal).
19. An empty `trace_context` `trace_id`, `span_id`, or `flags` field name, after `format`'s
    defaults.
20. A `scale` with no `fields`, an empty field name, or a non-finite factor.
21. An empty `signals:` list, or all three signals on `keep_signals`/`drop_signals`.
22. An `otlp_out` header the transport sets itself, or two that differ only in case.
23. An `otlp_out` `paths:` under `protocol: grpc`.
24. An inconsistent `otlp_out` `tls:`, or one under a non-`https://` endpoint.
25. A `trace_context` `span:` with an empty `name` or a `0s` `max_skew`.
26. A `tail_in` with no `paths`, an empty entry, or a `*` outside the final path component.
27. A `docker_in` that would tail nothing, or with an empty or duplicate entry or an empty `root`.
28. A `tail_in`/`docker_in` `poll_interval`, `checkpoint_interval`, or `max_line_bytes` of `0`.
29. A `file_out` that would never rotate, a `rotate.max_bytes`/`max_files` of `0`, or a
    `max_files` above 1000 (`logit_config::MAX_ROTATE_FILES`).
30. A `kv` with an empty, identical, or overlapping `pair_sep`/`kv_sep`.
31. A `regex` with an empty `field`, or a `pattern` that fails to compile or has no named group.
32. A `csv` with no `columns`, an empty or duplicate column name, or an unusable `delimiter`.
33. A `stdio_out`/`file_out` `compression:` outside `format: native`.
34. An inconsistent `logit_out` `tls:`, or a `logit_in` `max_frame_bytes` of `0` or above 64 MiB.
35. A `buffer.disk:` beside a non-default in-memory bound, a bad disk bound, or a shared
    `disk.path`.
36. A `has_attributes`/`drop_attributes` with nothing configured, an empty key, or a non-finite
    value.
37. A `has_provenance`/`drop_provenance` with nothing configured, or an empty or repeated entry.
    An id isn't checked against this graph: it may name a component in another process.
38. A `statsd_out`/`collectd_out`/`graphite_out` `max_packet_bytes` of `0`, or a `collectd_out`
    value outside `1024..=65535`.
39. A cumulative `aggregate` with a `series_retention` or `max_retained_series` of `0`.
40. A scrape-mode `prometheus_in` with a bad target URL, `timeout: 0s`, a bad `scrape_tls:`, or a
    reserved or colliding header.
41. A registry-mode `prometheus_out` `path` not starting with `/`, or `max_series: 0`.
42. A `generate_in` count of `0`, an empty key or metric name, a non-finite metric value, or an
    unknown or unbounded template placeholder.
43. A `tls:` on a UDP `syslog_in`, `graphite_in`, or `statsd_in`.
44. An inconsistent `syslog_out` `tls:`, or one under `transport: udp`.
45. A `0s` `handshake_timeout`, or a non-default one on a UDP listener.
46. A `graphite_in`/`graphite_out` pickle over UDP, a zero size or timeout bound, or an
    out-of-range `max_frame_bytes`.
47. A `targets:` list on anything but `lua`/`lua_file`.
48. A router target that is unresolved, the router itself, or not a `target`, or a repeated `lua`
    target.
49. A `target` with `sources`, or one no router directs to.
50. Not a rejection: a router is exempt from rule 7.
51. A `route` with no `routes:`, an empty key or value, or an empty `by:` key.
52. An inconsistent `statsd_out` `tls:`, or one under `transport: udp`.
53. A `0s` `idle_timeout`, or any `idle_timeout` on a UDP listener.
54. A `keep_values` with nothing configured, an empty name or `allow`, a non-finite literal, a
    literal `normalize: [lower]` can't produce, or a repeated step.
55. A `prometheus_in` with both or neither mode, or a field set for the other mode.
56. A `prometheus_out` with both or neither mode, a field set for the other mode, or a bad sender
    setting.
57. A datagram listener's `receive.read_batch` above 1024.
58. A `shape` `max_tracked_keys` or `max_tracked_keysets` of `0`.
59. A `flatten` that selects nothing, or has an empty list or an empty or repeated name.
60. An `http_access` with an empty or invalid pattern, an empty label, a malformed or repeated
    route rule, a bad `max_length`, or `forwarded: {trust: false}`.
61. A `sample` rate outside `[0, 1)` (`0` needs `always_keep`), an empty field name, a malformed
    `always_keep`, or `missing:` without `key:`.
62. Two `tail_in`/`docker_in` components sharing a `checkpoint_path`, or one whose
    `checkpoint_path` is another's `<checkpoint_path>.tmp`.
63. A `datadog_in` with an empty `bind`, or an `api_keys` entry that is empty or has surrounding
    whitespace.
64. A `datadog_trace_in` with neither `bind` nor `socket`, an empty `bind`, a relative `socket`
    path, or `tls` without `bind`.
65. A `statsd_in` `bind` or `statsd_out` `endpoint` that isn't an absolute path under
    `transport: unix`/`unix_stream`, or `tls:` under either Unix transport.
66. A `datadog_out` with an empty or whitespace-padded `api_key`, an empty `site` or one with a
    scheme or `/`, an `endpoints` entry that isn't an absolute `http://`/`https://` URL,
    `timeout: 0s`, a reserved or colliding header, or a bad `tls`.
67. A `datadog_trace_out` with both or neither of `endpoint`/`socket`, an `endpoint` that isn't an
    absolute `http://`/`https://` URL, a relative `socket` path, `timeout: 0s`, a reserved
    (including any `datadog-*`/`x-datadog-*`) or colliding header, or a bad `tls` (including any
    `tls` with `socket`).
68. A `trace_context` `trace_id_high` outside `format: datadog`, or an empty one.
69. A `splunk_hec_in` with an empty `bind`, a `tokens` entry that is empty or has surrounding
    whitespace, or a `max_request_bytes` of `0`.
70. A `splunk_hec_out` whose `endpoint` isn't an absolute `http://`/`https://` URL, carries a query
    or fragment, or ends in a HEC route rather than the `/services/collector` base, an empty or
    whitespace-padded `token`, `timeout: 0s`, an `ack_timeout` without `ack: true` or of `0s`, a
    `max_body_bytes` of `0`, or a bad `tls` (including any `tls` with an `http://` endpoint).
71. A `lua`/`lua_file` `max_memory` of `0`: an empty Lua VM already holds more than that.

**Deliberately not validated:** that a `by: {provenance: ..}` route key names a component in *this*
graph — rule 37's reasoning; the key is as likely to name a component relayed from another process.

**Sink reachability from a listener needs no separate rule.** It follows from 2 + 5 + 7: every
acyclic chain of ≥1-source components ends somewhere, and rule 7 requires every non-terminal
component in it to have a consumer, so the chain can only end at a sink.

**Sharing needs no rule either.** The pre-graph validator rejected an input or output claimed by
more than one pipeline, because that runtime couldn't express sharing. In the graph model a shared
component is one that appears in several `sources` lists, with nothing to special-case or restrict.

## Runtime model

- **Each component is a node** with one inbox (`mpsc::Receiver`, capacity `CHANNEL_CAPACITY`, 64)
  and a `Fanout`: one `mpsc::Sender` per consumer, resolved from the inverted `sources` relation.
- **Fan-in is free**: N sources into one component is N cloned `Sender`s feeding the same inbox. A
  `target` that several routers direct at is fan-in by the same mechanism: each router holds a
  clone of that target's senders ([ADR `target-components`](../adr/target-components.md)).
- **Fan-out shares the batch instead of cloning it per consumer.** An edge with one consumer moves
  the batch through as `Delivered::Owned`. With more than one, `Fanout` wraps it in an `Arc` and
  sends each consumer a `Delivered::Shared` clone, iterating its consumers with `split_last()`
  (`crates/logit-pipeline/src/fanout.rs`). A consumer that mutates may still pay a deep clone; see
  "Backpressure" below for the cost by shape.
- **Bind before spawning anything.** Before any task starts, the runtime walks every component in
  **sorted id order** and calls `Input::bind` on each listener and `Output::bind` on each sink,
  marking each `NodeState::Bound` (`crates/logit-pipeline/src/readiness.rs`). Both default to a
  no-op, so only a component that opens something overrides one. A failure is a *startup* failure
  naming that component (exit code 1) with nothing else running, rather than the first `JoinSet`
  error once every sibling is live. Sinks are included because a sink can listen too:
  `prometheus_out` serves an exposition endpoint, so an address already in use is a startup
  failure, not a delivery failure for `write_loop`'s retry to absorb
  (`docs/adr/prometheus-scrape-and-exposition.md`'s "`Output::bind`"). Sorted, sequential order
  makes "which one failed" reproducible instead of a race between binds.
- **Inboxes exist before any node is spawned**, so spawn order doesn't matter. The runtime creates
  every component's inbox channel up front, and a `Fanout` is only cloned `Sender`s into inboxes
  that already exist. No topological build order is needed. `Graph::topological_order`, a
  byproduct of rule 5's Kahn pass, has no production reader: the runtime spawns in sorted id order,
  and `logit graph` renders straight off the raw `Config`.
- **A `target` is not a node.** It gets no task, no inbox, and no channel. (A listener's inbox is
  created but never read, because nothing can name a listener as a source; a target's is never
  created, because a target declares no `sources:` and nothing may name it as one either.) At
  runtime a target is **one `Fanout`**, built in a pass *before* the spawn loop — ids are sorted,
  so a router can precede its own targets. It is wired to the target's consumers' inboxes and
  carries the target's own id (`with_component`) and telemetry handle (`with_telemetry`). Each of
  its routers gets a clone, and its readiness state is `NodeState::Alias` for the whole run. The
  map of target `Fanout`s is **dropped alongside the construction-only `senders` map**. A clone
  left behind would be an extra `Sender` on each of the target's consumers' channels, so the
  shutdown cascade could never pass the target. That failure is a hang, not an assertion, which is
  why `a_router_exiting_closes_its_targets_consumers_inboxes` pins it under a timeout
  ([ADR `target-components`](../adr/target-components.md)).
- **A router is an ordinary node with one extra edge set.** It owns its own `Fanout` (slot 0, the
  unrouted/forward edge) plus a slot-ordered `Vec<Fanout>`, one per `graph::targets_of` entry. For
  each incoming batch it routes every event by *borrowing* it, counts per destination,
  `reserve_exact`s, moves each event into its destination's buffer (`route_batch`), and sends
  **one batch per non-empty destination under one child `BatchContext` and one span**. One
  incoming batch is one hop however many ways it forks, the same rule ordinary fan-out follows. A
  router with targets and no ordinary consumers is legal; its forward partition is dropped and
  counted `logit.component.events.dropped{reason="unrouted"}`, never silently.
- **Shutdown cascades by channel closure** from listeners toward sinks. A node whose inbox closes
  drains it and exits, which drops its `Fanout`'s senders and closes its consumers' inboxes in
  turn.

### Thread model: only Lua needs its own OS thread

`mlua::Lua` is `!Send`/`!Sync` (`docs/design/lua-api.md`'s concurrency section; `AGENTS.md` lists
this as non-optional), so a Lua node can't be *moved* into an async task and runs on a dedicated
`std::thread`. **Each Lua component gets its own thread**, talking to its neighbors over the same
`mpsc` channels every other node uses. The pre-graph runtime ran a whole pipeline's transform chain
on one thread, because `PipelineConfig.transforms` guaranteed the stages were adjacent; in a graph,
a Lua component's sources and consumers can be any components. The thread's exit (a normal return
once its inbox closes, or a panic caught at the top of the thread) is reported over a oneshot that
a small `JoinSet` task awaits for the node (`runtime.rs`'s `watch_lua_thread`), so readiness, the
failure-triggered drain, and the exit code treat a Lua node as they treat any task.

That watcher task also bounds a script that never returns
([ADR `lua-runaway-script-bounds`](../adr/lua-runaway-script-bounds.md)):

- **Heartbeat.** The thread shares a `logit_script::Heartbeat` with its watcher: bit 0 is set
  while it is inside a `process()` or `flush()` call, and a count above it advances on each call,
  each `Event.new`, and each event taken from a returned table. The thread marks itself idle before
  it sends, so a thread parked on a full downstream inbox is backpressure, never a stall.
- **Stall.** Busy with the value unchanged for `stall_after` (10 s) sets the node to
  `NodeState::Stalled` and logs `script_stalled`; `/readyz` reads `503 stalled` until the next
  change sets `Running` again. The phase never moves, so a stall recovers on its own.
- **Revocable I/O.** The thread's inbox, own `Fanout`, and target `Fanout`s live in one
  `Arc<Mutex<Option<LuaIo>>>`, locked by the thread only around a receive and around a send,
  never while busy.
- **Wedge.** Once shutdown has begun, busy with the value unchanged for `shutdown_grace` (2 s),
  measured from the later of the signal and the last change, is a wedge. A node already `Stalled`
  is measured from its last change alone; with the defaults (10 s to stall, 2 s of grace) that
  means the next tick after the signal. The grace is shorter than a sink's 5 s
  `buffer.shutdown_grace`, so a downstream window flushed after the revocation still reaches its
  sink. The watcher takes the `LuaIo` out of the mutex, counts the batches still in its inbox as
  dropped with reason `shutdown`, drops it, and returns `Err` naming the node. Dropping it closes
  every downstream inbox, so those nodes drain on their own graces and flush their own windows as
  on any shutdown, and every upstream send fails as `closed_consumer`.
  Nothing is aborted: the join loop's first-error path marks the node `Failed` and the run exits
  `2`. The thread itself is left running until `main` exits; if its call ever returns, it finds
  `None` and stops. A node that isn't busy is never blamed, whatever its downstream is doing.

There is no wall-clock bound on the drain as a whole. A slow `flush()` that keeps producing events
is progress however long it takes, and graces don't add up along a chain of Lua nodes, because
each wedge is judged on its own node's heartbeat. The cost: a script looping over `Event.new`
forever is progress too, and is never stalled or wedged.

Everything else runs as an ordinary tokio task: listeners, sinks, native `Send` transforms
(`logit-transforms::Aggregator` and every other native transform in that crate), and **native
`Router`s** (`route`, [ADR `target-components`](../adr/target-components.md)). A `Router` is `Send`
for the same reason a `Transform` is, and `run_router` is `run_transform` minus the flush-deadline
race. A `target` runs as nothing at all (see "Runtime model"). A **Lua router** (a `lua`/`lua_file`
component with `targets:`) is still one OS thread: it partitions each batch by the `event:to(..)`
mark its script set and sends one batch per destination from that thread.

**Fusing a linear run of adjacent Lua nodes back onto one thread** would save a thread and a
channel hop per hop. It isn't built: thread count is bounded by config size (at most one per
component), which hasn't needed it.

### Flush ticks are per node

Each flush-bearing node — an `aggregate` or `shape` component, or a `lua`/`lua_file` component
with `interval` set — owns a single flush deadline. `run_transform` (for Lua, `run_lua_loop`
through `Handle::block_on`, since its thread has no async context) races it against the inbox
with `tokio::time::timeout` around `recv`, so a flush fires on schedule even when no events arrive, and
advances it with `advance_flush_deadline`, which is constant-time however many ticks were missed
(`crates/logit-pipeline/src/runtime.rs`). There is no shared cross-node schedule: the pre-graph
worker kept a `Vec` of deadlines only because a pipeline's stages shared one thread and one receive
loop.

A flushed event leaves through the node's own `Fanout`, like a normally processed batch. So the
"flushed output isn't exempt from downstream processing" property
(`docs/adr/aggregation-window-semantics.md`) holds by construction: downstream processing is
"send to the node's consumers," the path every event takes. `docs/design/lua-api.md`'s `flush()`
contract and `docs/adr/aggregation-window-semantics.md`'s windowing semantics apply unchanged; the
graph model changes only *where* the flush timer lives (per node instead of per pipeline chain),
not what it does.

### Node kinds and the transform trait question

`Input`, `Output`, `Transform`, and `Router` are all traits in `logit-pipeline`, and the node
runtime holds `Box<dyn Transform + Send>` next to `Box<dyn Input + Send>`/`Box<dyn Output + Send>`.
The pre-graph runtime dispatched transforms through a closed, hand-written `Stage` enum, which
worked only because the whole chain lived on one thread with no `Send`/object-safety pressure. A
graph node's per-kind dispatch (arity, thread or task, flush or not) needs the trait instead of a
parallel hand-written enum per node kind. Lua nodes stay the one special-cased kind, for the
`!Send` reason above; a trait object doesn't fix that and shouldn't try to.

Besides `process`, `Transform` has a second per-batch hook, `map_resource`, called once per
incoming batch before any event reaches `process`. It lets a transform substitute the batch's
resource (`logit-transforms::Set` was the first implementer); see
[ADR `operator-declared-resource-attributes`](../adr/operator-declared-resource-attributes.md).

### Trace context propagation

Every `Delivered` (the channel payload one `Fanout` edge carries, `crates/logit-pipeline/src/fanout.rs`)
carries a `TraceContext { trace_id: [u8; 16], span_id: [u8; 8] }`, the substrate for internal
spans. [ADR `trace-context-propagation-on-delivered`](../adr/trace-context-propagation-on-delivered.md) decided it, on
the measured evidence [ADR `minimize-allocations-over-event-size`](../adr/minimize-allocations-over-event-size.md) required.
[ADR `internal-span-emission-and-deterministic-sampling`](../adr/internal-span-emission-and-deterministic-sampling.md) turns
it into a `SpanRecord`-carrying `Event`; see `docs/design/internal-telemetry.md`'s "Spans" section
for the emit API, the sampler, and the bound.

**Whether a node propagates a real parent or mints a fresh root depends on its kind.** It follows
the 1-to-1 versus *n*-to-1 distinction between a node's per-batch processing and its flush:

| Node kind | Context of what it emits | Span recorded |
|---|---|---|
| A listener's own batches | Always a fresh root. `Input::run` never receives a `Delivered` (arity rules out a `sources` entry pointing at a listener), so there's no parent to inherit. | `SpanKind::Producer`, in `Fanout::send`/`send_blocking` |
| `Transform::process`/`ScriptWorker::process` (the non-flush path) | A [`TraceContext::child`] of the one incoming batch that produced it: 1-to-1, unambiguous. | `SpanKind::Internal`, in `run_transform`/`run_lua` |
| `Transform::flush`/Lua's timer-driven `flush()` | A fresh root, deliberately. A flush is *n*-to-1 (however many batches arrived since the last tick), with no single correct parent. This is a tracked gap, not an approximation; see ADR `trace-context-propagation-on-delivered`'s "What this doesn't do." One root covers *every* resource group a flush emits, not one root per group (ADR `internal-span-emission-and-deterministic-sampling`). | `SpanKind::Internal`, in `run_flush`/`run_lua`'s `flush_now` |
| `run_output` | Borrows the incoming `Delivered` without unwrapping it (`Output::send(&EventBatch)`, [ADR `arc-eventbatch-copy-on-write`](../adr/arc-eventbatch-copy-on-write.md)), so the context is there to read. There's nothing downstream to propagate *to*: the sink span mints `ctx.child()` as its own identity and discards it (ADR `internal-span-emission-and-deterministic-sampling`). | `SpanKind::Client`, in `write_loop` |

Mechanically:

- `Fanout::send`/`send_blocking` mint a root, open the listener's span, and delegate to
  `Fanout::send_with_own_context` (ADR `internal-span-emission-and-deterministic-sampling`). A
  flush-driven emission also mints its own root, but calls `send_with_own_context` directly so it
  can record its own span around that context.
- `Fanout::send_with_context`/`send_blocking_with_context` mint a child of a given parent, record
  no span of their own, and are defined in terms of `send_with_own_context`.
- `Delivered::context()` is a cheap `&self` accessor. Read it *before* `unwrap_batch`, which
  consumes the batch and discards the context. Returning the context from `unwrap_batch` would
  force every caller, most of which propagate nothing, to thread an unused value through.
- `SinkQueue`'s entries carry the context (`push`/`peek`, ADR
  `internal-span-emission-and-deterministic-sampling`), because the `drain_inbox` → `write_loop`
  path needs it to parent the sink's own span.

A fan-out (one batch, several downstream branches) gives every branch the *identical* child
context. One emission forking into several consumers is one hop, and ADR
`internal-span-emission-and-deterministic-sampling` records exactly one span for it, not one per
branch.

### Provenance propagation

Alongside `TraceContext`, every `Delivered`'s `BatchContext` carries a
`Provenance { origin: Option<Symbol>, previous: Option<Symbol> }`: which component created the
batch, and which component the current node received it from
([ADR `batch-provenance-on-delivered`](../adr/batch-provenance-on-delivered.md)). Every component
can read it (a native `Transform::observe_provenance`, an `Output::observe_batch`, a Lua script's
`provenance` global) and none can write it: a component never constructs a `Delivered`, so it
can't forge or drop what it carries.

**`Fanout`, the only writer, applies one stamping rule to every node kind with no special cases**
— unlike trace context above, which needs one row per node kind:

```
origin   = origin.or(Some(self.component))   // set once, on this Fanout's first send
previous = Some(self.component)              // rewritten on every send
```

- A listener's first send has empty incoming provenance, so it sets *both* fields: the listener is
  its own origin and previous. `internal` needs no special case, because it's a listener by role.
- Every later hop finds `origin` already set and rewrites only `previous`.
- A flush emission (`Transform::flush`/Lua's `flush()`) mints a fresh `BatchContext` with empty
  provenance, as it mints a fresh `TraceContext` root. The flushing component becomes *both*
  `origin` and `previous`, which is accurate for a batch built from accumulated state rather than
  passed through.
- A fan-out gives every branch the identical provenance, as it does for trace context.
- **`previous` downstream of a `target` is the target's id, never the router's**, and `origin` is
  untouched. No special case does this: a target's one `Fanout` is built
  `with_component(<target id>)` like every other node's, so the rule above applies and a
  `has_provenance{previous: [host_stream]}` reads naturally
  ([ADR `target-components`](../adr/target-components.md)). A router's own forward edge stamps the
  *router's* id, like any other node. Provenance deliberately doesn't record which router fed a
  target; if that ever matters, it belongs in a router-side metric, not in provenance.

`logit_in`'s relay (`Fanout::send_relayed`) uses a different rule, `stamp_relayed`: it back-fills
(`or`, not overwrite) only what the wire didn't carry, so a v2 `logit_out` peer's own
`origin`/`previous` survive the hop untouched. That is what lets a split-collection deployment
read as one graph across `logit_out -> logit_in`. See the ADR for the wire-format decision
(`CODEC_NATIVE_V2`) this relies on.

## Backpressure: diamonds are the normal shape now

The usual way to route by condition is a set of sibling branches off one upstream, each a
transform that drops what its branch doesn't want, reconverging on shared sinks (ADR
`component-graph-configuration`; a `lua` component per branch, per
`docs/adr/routing-by-condition-is-lua.md`, or a router and targets). Diamonds are the expected
topology, not a rare one, with these consequences:

- **Backpressure crosses branches.** A stalled sink backs up through every branch that shares an
  upstream with it, not only its own path. That is correct bounded-channel behavior, but one slow
  destination can head-of-line-block telemetry bound for an unrelated, healthy one.
- **Fan-out cost depends on shape.** Without a routing primitive (which ADR
  `component-graph-configuration` ruled out), every extra consumer once cloned the outgoing
  `EventBatch` — a deep `Vec<Event>` clone. That clone was load-bearing, not incidental: it makes
  branch isolation free. Two branches never share an `Event` value, so a mutation on one is
  structurally invisible to the other (see [ADR `multi-payload-events`](../adr/multi-payload-events.md)'s
  branch-isolation note; `crates/logit-pipeline/src/runtime.rs`'s
  `a_mutation_on_one_fan_out_branch_is_invisible_to_the_sibling_branch` proves it).

  `Arc<EventBatch>` copy-on-write (`docs/adr/arc-eventbatch-copy-on-write.md`, which records the
  three rounds of measurement behind these numbers) keeps that isolation and changes the cost:
  - A single-consumer edge (most edges in the shipped config) and an all-`Output` fan-out are free
    or near-free.
  - A fan-out mixing an `Output` branch with a mutating branch is racy: 1 or 6 allocations,
    decided by real scheduling, never a fixed number.
  - A fan-out with no `Output` branch still pays the full clone (6 allocations, one worse than the
    pre-`Arc` code), with no path to improvement under the current design.

  There is no single "fan-out cost"; `docs/design/memory.md` §3 has the account by shape.
- **A router and targets (ADR `target-components`) is the cheap form of this diamond**: one
  partition pass and no clone. It doesn't change backpressure: a stalled consumer of one target
  backs up through its router into every other target's flow, the same head-of-line blocking as
  the first bullet, paid by a router instead of a filter chain.

**Open question: a closed downstream.** `Fanout` skips a closed consumer and counts the batch in
`logit.component.events.dropped{reason="closed_consumer"}`. In a DAG that closure should arguably
propagate as a shutdown signal instead. A per-edge `on_full: block | drop` policy is one plausible
answer; neither is built.

**Sink-side buffering decouples a sink's inbox from its delivery**
(`docs/adr/buffered-sink-delivery.md`). `run_output` splits into a drain half that moves batches
off the inbox into a `SinkQueue` and a writer half that delivers from that queue independently
(`crates/logit-pipeline/src/queue.rs`). A slow or backing-off sink therefore keeps draining its
inbox, instead of pushing backpressure upstream as soon as one `Output::send` is slow. Backpressure
still exists: a `SinkQueue` under `Block` applies it once the queue fills. It surfaces later and
deeper than the inbox's `CHANNEL_CAPACITY=64`, and `logit.component.buffer.utilization` shows it
coming rather than only a stalled inbox.

**Listener-side receive decoupling does the same one hop earlier**
(`docs/adr/decoupled-listener-io.md`). Without it, a UDP listener's `recv_from`, decode, and
`Fanout::send` share one loop, so downstream backpressure stops the socket being read and the
kernel drops datagrams, silently and uncounted. `logit-inputs::udp::UdpListener` splits into a read
half that moves datagrams off the socket into a `ReceiveQueue` (`BoundedQueue<Datagram>`, the
generalized type `SinkQueue` is also an instance of, `crates/logit-pipeline/src/queue.rs`) and a
decode half that pops, decodes, accumulates, and sends independently. Unlike `SinkQueue`, the
receive queue defaults to `drop_oldest`, not `Block`: a UDP reader's producer is the kernel socket
buffer, which can't be asked to wait, so blocking would only move the loss into the kernel. See
that ADR for the field research behind the default.

## `logit graph`: visualizing the resolved DAG

`logit graph <config>` prints the component graph as graphviz DOT to stdout, answering "what does
this config actually do" for a graph that's hard to read from YAML.

- It renders unconditionally, straight off the raw `Config` rather than a resolved `Graph`. An edge
  only needs its `sources` id written as a target, and graphviz auto-creates a bare node for an
  edge whose target was never declared, so an unresolved source (rule 2) renders as a visibly
  dangling edge instead of blocking output.
- It then runs full validation (every rule) and reports failures to stderr with a non-zero exit,
  without suppressing the DOT output. `graph` is most useful on configs that fail validation: a
  cycle, or a typo'd source now visibly dangling, is far easier to see rendered than to decode
  from an error naming two component ids.
- It styles nodes by role (listener, transform, sink), so where data enters, forks, and lands reads
  at a glance without the arity table.
- It renders a `target` as a dashed box, and every router → target edge dashed, labeled with the
  `routes:` key that directs an event down it. A `lua`/`lua_file` `targets:` edge has no label,
  because the script picks the destination with `event:to("..")`. A targeting node's ordinary
  edges to its consumers, which carry the events no target took, are labeled `[else]`. The target
  edges come from `graph::target_edges`, which reads the raw `Config` too, so a router whose
  target id resolves to nothing renders as a dangling dashed edge rather than blocking output
  ([ADR `target-components`](../adr/target-components.md)).
- Every `!env` reference must still resolve ("Environment substitution" above). A missing variable
  fails the load before `render` is called, as with `run`/`validate`, even for a field this command
  never reads.

It's a `Command::Graph` arm in `logit-cli`, alongside `Schema`/`Validate`/`Run`
(`crates/logit-cli/src/main.rs`), and it's synchronous like `Schema`/`Validate`: it needs only the
graph structure, with no I/O and no tokio runtime.

## Crate layout

The obvious arrangement is circular: the pipeline runtime builds inputs, outputs, and transforms,
but `Input::run`/`Output::send`/a `Transform` implementation need the `Fanout` type the runtime
defines. The fix is to invert it: the trait definitions and the runtime live in a crate that the
impl crates depend on, not the other way around:

```
logit-core   logit-config   logit-script
        \         |         /
          logit-pipeline          — Input/Output/Transform traits, Fanout, graph, node runtime
           /            |            \
   logit-inputs   logit-transforms   logit-outputs   — impls only
           \            |            /
                    logit-cli                        — CLI + the kind → impl registry
```

Not drawn above: `logit-transforms` also depends on `logit-config` directly, for `route`'s
`RouteBy` type (`docs/adr/target-components.md`). An impl crate reading a config type it needs is
unremarkable; only `logit-pipeline` is barred from depending on the impl crates.

`logit-pipeline`:
- Defines the `Input`, `Output`, and `Transform` traits discussed above (and `Router`), so
  `logit-inputs`/`logit-outputs` hold only impls.
- Owns `Fanout`, the graph resolution/validation module (pure; see below), and the node runtime.
- Depends on `logit-core` (for `EventBatch`), `logit-config` (for `ComponentKind`), `logit-script`
  (for the Lua node's `ScriptWorker`), and `logit-proto` (for `Buffer`/`InMemoryBuffer`, which
  `SinkQueue` wraps — `docs/adr/buffered-sink-delivery.md`), but *not* on
  `logit-inputs`/`logit-transforms`/`logit-outputs`, which depend on it for the trait definitions.
  `logit-cli` is the one crate that depends on everything, and it holds the kind-to-trait-object
  registry (`build_spec`, which replaced the pre-graph `build_input`/`build_output`).
- Keeps the channel type out of `logit-core`, whose doc comment states "no I/O, no pipeline".
  Weakening that would blur the boundary that crate exists to hold.
- Owns `BoundedQueue<T: Queued>` and `BatchAccumulator` (`docs/adr/decoupled-listener-io.md`)
  alongside `Fanout` and the node runtime, because neither type mentions a socket or a decoder. The
  UDP socket bind, `SO_RCVBUF` setsockopt, and `recv_from` loop that *use* them
  (`logit-inputs::udp::UdpListener`) stay in `logit-inputs`: generic machinery here, concrete
  protocol impls there, as everywhere else.
- Owns `sockstat` (`docs/adr/udp-intake-batching-and-socket-visibility.md`) on the same principle.
  It is a `getsockopt`-level *reading* of a file descriptor the caller already holds — the kernel's
  per-socket drop counter and receive-buffer fill (`SO_MEMINFO`), a listening socket's accept-queue
  depth (`TCP_INFO`) — with no notion of a listener, a datagram, or a node. It isn't in
  `logit-inputs` because `logit-outputs` is the foreseeable second consumer and must not depend on
  an input crate. It isn't in `logit-core` because a raw syscall and a `libc` dependency would
  break that crate's "no I/O" boundary (the bullet above); this crate already does real I/O
  (`disk_queue.rs`). Everything that *operates* a socket — binding, sizing, reading — stays in
  `logit-inputs`.

`graph.rs` (resolution, the validation rules, and the topological sort) is a **pure function over
`Config`**: no channels, threads, or tokio, so it's unit-testable without real I/O, as the
pre-graph `apply_transforms` was. `logit run`, `logit validate`, and `logit graph` layer on the
same resolution: `run` executes it, `validate` checks it and stops, `graph` renders it.
