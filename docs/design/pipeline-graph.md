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
[examples/statsd-to-influxdb.yaml](../../examples/statsd-to-influxdb.yaml). There is no `inputs`/
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
runnable version of the config above is `examples/fan-out-central.yaml`.

## Validation

`graph::resolve` (`crates/logit-pipeline/src/graph.rs`) runs these rules in order; `logit-cli`'s
`validate_semantics` (`crates/logit-cli/src/pipeline.rs`) is a thin wrapper around it. Several
rules share one principle, stated once here and cited by number below: **a config that can only
be a no-op, a black hole, or an impossible bound is an error**, and so is a setting that would be
silently ignored. `0` for a count or duration bound is usually impossible, not small.

1. At least one component.
2. Every id in any `sources` list resolves to a defined component.
3. No self-reference (a component listing itself as a source). This is a special case of 5, with
   its own message because it's the most common typo shape.
4. No duplicate source within one component's `sources` list. A repeated id would push the same
   consumer onto that source's outbound edge list twice, giving its `Fanout` two live `Sender`
   clones into the same inbox. Every batch would arrive twice, doubling telemetry and every count
   through an `aggregate` component, instead of being rejected as the typo it almost certainly is.
5. **No cycles.** A cycle plus bounded `mpsc` channels is a deadlock, not a slow pipeline. The
   check is Kahn's algorithm; the nodes left unresolved when it runs out of zero-indegree
   candidates are the cycle *plus* everything downstream of it. The error walks that set back to
   one concrete cycle path before reporting it, so a downstream victim is never named as part of
   the cycle.
6. Arity per kind, per the table above: a listener with `sources`, a sink with none, or a sink
   named as another component's source is rejected.
7. Every non-sink component has ≥1 consumer. This catches the silent black hole of a transform
   whose only consumer was renamed or deleted, still accumulating state or running Lua for
   nothing.
8. The kind is implemented — the pre-graph `require_implemented_input`/
   `require_implemented_output`/`require_implemented_transform` trio, unified into one check over
   `ComponentKind`.
9. No zero-length `interval` on any kind that has one (`lua`, `lua_file`, `aggregate`, `internal`,
   `shape`, `prometheus_in`; `graph.rs`'s `interval()` table).
10. A `kv_metrics` with `counters`, `gauges`, and `distributions` all empty is rejected: it can only
    be a no-op, the same silent black hole rule 7 catches.
11. A `kv_metrics` distribution entry with no `field` is rejected: a distribution of nothing is
    meaningless (`docs/adr/kv-metrics-semantics.md`).
12. A `kv_metrics` counter, gauge, or distribution entry with an empty `name` is rejected, because
    `influxdb_out` can't encode a metric with no measurement name
    (`docs/adr/kv-metrics-semantics.md`). (Numbering note: `crates/logit-pipeline/src/graph.rs`'s
    rule comments no longer match this list one-to-one. The code folds this check under one
    "Rules 10 + 11" comment with the two above, and reuses the number 12 for `set`'s no-op check.
    The rules themselves are unchanged.)
13. At most one `internal` component. Two would each drain, and so split, the same process-wide
    telemetry `Registry`, silently halving whichever one a consumer wasn't reading.
14. A non-default `buffer:` block on a non-sink component is rejected. `buffer:`
    (`docs/adr/buffered-sink-delivery.md`) configures a sink's delivery queue, which only a sink
    has, so on a listener or transform it is a misplaced block, not a setting to ignore silently.
15. A sink's `buffer.max_batches` or `buffer.max_bytes` of `0` is rejected: no batch could ever be
    queued.
16. `internal`'s `span_sample_rate` must be finite and within `[0, 1]`. Out-of-range is a config
    error, not something to clamp silently.
17. A non-default `receive:` block is rejected on any kind that is not one of these:
    - a **datagram listener** (`docs/adr/decoupled-listener-io.md`): `collectd_in`, and
      `statsd_in`/`syslog_in`/`graphite_in` under `transport: udp`;
    - a **stream listener** (one shared driver, `docs/adr/syslog-tcp-ingress-and-tls.md` plus
      `docs/adr/graphite-carbon-relay.md`'s 2026-09-14 amendment): `syslog_in`/`graphite_in`/
      `statsd_in` under `transport: tcp`;
    - a **tail listener** (`docs/adr/file-tailing-and-docker-json-logs.md`): `tail_in`/`docker_in`.

    The rule is deliberately not "any non-listener": `internal` and `generate_in` are listeners by
    role but have no socket, queue, or decoder, so `receive:` on either would be exactly the
    silently ignored setting rule 14 guards against on the sink side.

    A tail listener has no receive *queue* (the tailed file is its own durable buffer), so only
    `receive.batch_max_events`, `batch_max_bytes`, `batch_flush_interval`, and `shutdown_grace`
    apply. The queue fields (`max_datagrams`, `max_bytes`, `overflow`, `receive_buffer_bytes`) and
    `read_batch` — which sizes one `recvmmsg(2)` read and the matching `pop_many` off that queue
    ([ADR `udp-intake-batching-and-socket-visibility`](../adr/udp-intake-batching-and-socket-visibility.md))
    — are rejected by name. A stream listener has no receive queue either, for a different reason:
    the TCP connection's own flow control *is* the backpressure, so a blocked `Fanout::send` stops
    the socket being read and the peer's window closes. The same five queue fields are rejected by
    name, with a message that says so, and the four batch/shutdown fields apply **per connection**
    rather than per listener (N live connections can hold up to N × `batch_max_events` in flight).
18. A datagram listener's `receive.max_datagrams`, `receive.max_bytes`, `receive.read_batch`, or
    `receive.batch_max_events` of `0` is rejected — the twin of rule 15. (`read_batch: 0` is a
    `recvmmsg` `vlen` of zero, which reads nothing, forever; rule 57 owns its upper end.)
    `receive.batch_flush_interval: 0s` is **not** rejected: it means "no flush timer." A tail or
    stream listener's `receive.batch_max_events`/`batch_max_bytes` of `0` is rejected the same way;
    the queue-only bounds don't apply to either (rule 17).
19. (The code's numbering has drifted here too; see the note on 12.) A `set` with both `resource`
    and `attributes` empty is rejected, as is an empty key in either map. A `trace_context` with an
    empty `trace_id` field name is rejected. Both apply rules 10-12's no-op reasoning to later
    components (`docs/adr/operator-declared-resource-attributes.md`,
    `docs/adr/log-record-trace-context.md`).
20. A `scale` with an empty `fields` map, an empty field name, or a non-finite factor is rejected
    (`docs/adr/scale-transform.md`).
21. An empty `signals:` list on `has_signal`, `keep_signals`, or `drop_signals` is rejected, and
    `keep_signals`/`drop_signals` also reject naming all three signals. Which of those two shapes is
    the black hole (no event gets through) and which is the no-op (every event forwarded untouched)
    is *opposite* between the two kinds: an allowlist naming nothing keeps nothing, and naming
    everything keeps everything; a denylist is the mirror. Both shapes are rejected, and the error
    names the right one. `keep`'s empty `fields` list stays legal: "drop every attribute" is a real
    operation, "drop every event" is not. See `docs/adr/signal-filtering-components.md`.
22. An `otlp_out` `headers:` entry may not name a header the protocol sets itself (`content-type`,
    `content-length`, `content-encoding`, `host`, `te`, `transfer-encoding`, `connection`, any
    `grpc-*` header, an empty name, or an HTTP/2 pseudo-header starting with `:`), compared
    case-insensitively.
23. `otlp_out`'s `paths:` is HTTP-only. gRPC method names are fixed by the `.proto` service
    definitions, so a non-empty `paths:` under `protocol: grpc` is rejected rather than ignored
    (rule 14's reasoning).
24. An `otlp_out` `tls:` block: `cert_file`/`key_file` must be set together (mutual TLS needs
    both); `insecure_skip_verify` together with `ca_file` is contradictory; and a non-empty `tls:`
    under a plain `http://`/`grpc://` endpoint is rejected. TLS is selected by `endpoint`'s scheme
    (`docs/adr/otlp-tls-and-pooled-grpc-client.md`), so that block would be silently ignored.
25. A `trace_context` `span:` block with an empty `name` (OTLP requires every span to have one) or a
    `max_skew` of `0s` (every span would be rejected as skewed) is rejected
    (`docs/adr/trace-context-span-lifting.md`). Rule 19 also covers an empty `span_id`/`flags`
    field name on the same component, since those default to `span.id`/`trace.flags`: `null`
    disables a lookup, `""` is a typo.
26. `tail_in`'s `paths:` must name at least one file, no entry may be empty, and a `*` wildcard is
    allowed only in the final path component (`/var/log/app/*.log`, not `/var/*/app.log`). That is
    the glob subset `PathPattern` implements; a wildcard in a directory position would silently
    never match (`docs/adr/file-tailing-and-docker-json-logs.md`).
27. `docker_in`'s `containers:` must be non-empty or `discover: true` must be set. Explicit
    selection is the default, so a config with neither would silently tail nothing (rule 7's black
    hole). `containers:`/`labels:` may not contain an empty entry, `containers:` may not repeat
    one, and `root:` must be non-empty.
28. A `tail_in`/`docker_in` `poll_interval`, `checkpoint_interval`, or `max_line_bytes` of `0` is
    rejected: either interval at `0s` would busy-loop (rule 9's reasoning), and
    `max_line_bytes: 0` would drop every line (`docs/adr/file-tailing-and-docker-json-logs.md`).
29. A `file_out` whose `rotate:` block sets neither `max_bytes` nor `interval` is rejected: it would
    never rotate, and `stdio_out` already covers deliberate never-rotate. `rotate.max_bytes: 0`
    (every batch would rotate) and `rotate.max_files: 0` (would delete the file it just rotated)
    are impossible bounds, like rules 9/15/18/28 (`docs/adr/rotating-file-output.md`).
30. A `kv` with an empty `pair_sep` or `kv_sep`, with `pair_sep == kv_sep`, or with a `kv_sep` that
    *contains* `pair_sep` is rejected: each is a certain no-op or a certain garbage result
    (`docs/adr/logfmt-and-kv-parsing.md`). `logfmt` needs no rule: past `bare_keys`, its only field
    is a `bool`, which can't be malformed.
31. A `regex` with an empty `field` name, a `pattern` that fails to compile, or a `pattern` with no
    named capture group is rejected. The first is useless, the second a config mistake, the third a
    certain no-op (`docs/adr/regex-transform.md`).
32. A `csv` with an empty `columns` list, an empty column name, or a duplicate column name is
    rejected (the no-op rule, and rule 4's "a repeated entry silently doubles" applied to columns).
    So is a `delimiter` that is `"` (RFC 4180's quote character), `\n`/`\r` (already consumed as
    line framing by every input), or non-ASCII (`docs/adr/csv-positional-columns.md`).
33. `stdio_out`/`file_out`'s `compression:` is rejected when set to anything but `none` under the
    default `format: human`. Compression is `NativeEncoder`'s knob, so it would do nothing
    (`docs/adr/file-output-native-format.md`).
34. A `logit_out` `tls:` block: `cert_file`/`key_file` must be set together, and
    `insecure_skip_verify` together with `ca_file` is contradictory — rule 24's first two checks.
    There is no scheme check: `logit_out`'s `endpoint` is a bare `host:port`, so the presence of
    `tls:` is the only signal and always turns TLS on. Separately, a `logit_in` `max_frame_bytes`,
    when set, must be nonzero and at most 64 MiB — `logit_proto::frame::MAX_SANE_UNCOMPRESSED_LEN`,
    the ceiling `read_frame`/`read_frame_with_header` enforce whatever a listener configures.
35. A sink's `buffer.disk:` block (`docs/adr/disk-backed-sink-buffer.md`) is rejected alongside a
    non-default `buffer.max_batches`/`max_bytes`: disk replaces the in-memory bound rather than
    sizing alongside it, so the set value would be ignored (rule 33's reasoning).
    `buffer.disk.segment_bytes`/`max_bytes` of `0` are impossible bounds, and `segment_bytes` may
    not exceed `max_bytes`. Two sinks may not declare the same literal `buffer.disk.path`. The
    comparison is on the path as written, not resolved against the config directory;
    `DiskQueue`'s own exclusive lock catches an aliased path the comparison can't see.
36. `has_attributes`/`drop_attributes`: at least one of `resource`/`attributes` must be non-empty,
    every key must be non-empty, and every value must be a finite number
    (`docs/adr/attribute-filtering-components.md`). Which empty config is the black hole and which
    the no-op is *inverted* from rule 21: `resource:`/`attributes:` is a map of conjunctions, not a
    list of alternatives, so zero pairs is vacuously true. An empty `has_attributes` matches, and
    so forwards, every event (a no-op); its exact complement `drop_attributes` also matches every
    event, and so drops them all (a black hole). The same key in both `resource:` and `attributes:`
    is legal, because they address different objects.
37. `has_provenance`/`drop_provenance`: at least one of `origin`/`previous` must be non-empty, and
    neither list may contain an empty or duplicate entry
    (`docs/adr/provenance-filtering-components.md`). The empty-config orientation matches rule 36,
    not rule 21, even though each field is a list of alternatives: an empty `origin:`/`previous:`
    means "this field isn't part of the match" (vacuously true), not "match zero alternatives". The *field*, not the list,
    decides. So with both empty, `has_provenance` matches every batch (a no-op) and
    `drop_provenance` drops every one (a black hole). Deliberately *not* validated: that a
    configured id names a component in this graph. `origin`/`previous` are as likely to name a
    component in another process's graph, relayed unchanged across `logit_out`/`logit_in`.
38. A `statsd_out`, `collectd_out`, or `graphite_out` `max_packet_bytes: 0` is rejected, like rule
    15: every metric line or value list would overflow and be dropped whole
    (`docs/adr/statsd-output.md`, `docs/adr/collectd-binary-relay.md`,
    `docs/adr/graphite-carbon-relay.md`). `collectd_out` also rejects any value outside
    `1024..=65535`, collectd's own `MaxPacketSize` range (`docs/adr/collectd-binary-relay.md`).
    Above it every datagram fails `EMSGSIZE` at the socket, which `collectd_out` counts as a
    per-datagram drop rather than a `Fault`, so it would report `requests{class="ok"}` while
    delivering nothing. `statsd_out` and `graphite_out` make no range claim in their ADRs, so they
    keep only the zero check.
39. An `aggregate` with `temporality: cumulative` requires `series_retention >= 1` (a count of
    windows, not a duration) and `max_retained_series >= 1`. Those bounds keep a running total
    alive across the window boundary; with either at `0`, no accumulator survives a flush and each
    window would emit its own increment labeled as a cumulative total. `series_retention: 0` stays
    legal under the default `temporality: delta`, where it is the documented opt-out from gauge
    retention (`docs/adr/aggregation-window-semantics.md`'s cumulative amendment).
40. A **scrape-mode** `prometheus_in`'s scrape settings. The rule applies only when
    `scrape_targets` is non-empty and says nothing about a `bind:` receiver; rule 55 owns the mode
    itself, including the neither-mode case
    ([ADR `prometheus-remote-write`](../adr/prometheus-remote-write.md)).
    - Each `scrape_targets` entry must be an absolute `http://`/`https://` URL with a non-empty
      authority. `logit-pipeline` doesn't depend on `reqwest`/`url` (see "Crate layout"), so this
      is a small hand-rolled scheme/authority check, not a full URL parse.
    - `timeout: 0s` is rejected (rule 9's reasoning; rule 9 itself covers `interval: 0s`).
    - A `scrape_tls:` block must have `cert_file`/`key_file` together and no
      `insecure_skip_verify` alongside `ca_file` (rule 24's checks, as rule 34 makes for
      `logit_out`). It is rejected outright unless at least one target is `https://`: TLS is
      selected per target by scheme, so the block would otherwise be silently ignored (rule 24's
      third check).
    - `headers` may not name a header this input sets itself (`accept`, `user-agent`, and the
      other protocol-owned names) or collide with another entry, compared case-insensitively —
      rule 22's check for `otlp_out` (`docs/adr/prometheus-scrape-and-exposition.md`).

    `receive:` on `prometheus_in` is rejected by rule 17: it's a listener by role, but uses neither
    driver that rule permits.
41. A `prometheus_out` `path:` must start with `/`, and `max_series` must be ≥ 1
    (`docs/adr/prometheus-scrape-and-exposition.md`). A request URI's path is always absolute, so
    a relative or empty `path:` could never match and every scrape would 404 against an endpoint
    that looks configured. `max_series: 0` would evict every series on arrival, leaving the
    endpoint always empty (rule 38's impossible bound).
42. A `generate_in`'s bounds and templates (`docs/plans/load-test-harness.md`):
    - `count`, `batch`, and `rate` must each be at least 1 where set. `0` generates nothing;
      omitting `count`/`rate` is how "unbounded"/"unthrottled" is written.
    - A `metric` must have a non-empty `name` and a finite `value`. No `event.attributes` or
      `resource` key may be empty.
    - Every template string — `event.log`, every `event.attributes` value, every `resource`
      value, and `event.metric.name` — must parse as a `logit_core::template` and may name only the
      placeholders `generate_in` substitutes: `seq`, or `seq%N` with `N >= 1`. An unknown
      placeholder is rejected rather than rendered literally or as nothing, because a mistyped
      `{seg}` would silently collapse a scenario's cardinality to one series — the difference
      between measuring an aggregation window and measuring nothing.
    - **`event.metric.name` allows only `{seq%N}`, never a bare `{seq}`.** A metric name is
      *interned*, and `logit_core::interner` never removes a `Symbol` (`docs/design/memory.md`
      §4), so an unbounded name would leak one never-reclaimed entry per generated event. A log
      body or attribute value is copied onto the event and freed with it, so `{seq}` is legal
      there.

    The placeholder check lives in a small pure `generate_var_is_valid` helper in `graph.rs`, so
    `logit-inputs`' own `compile` resolver can mirror it exactly without depending on
    `logit-config` (see "Crate layout"). `receive:` on a `generate_in` is rejected by rule 17: it
    has no socket, queue, or decoder for `receive:` to configure.
43. A listener with a `tls:` block must use a stream transport: `transport: tcp` on a `syslog_in`
    ([ADR `syslog-tcp-ingress-and-tls`](../adr/syslog-tcp-ingress-and-tls.md)), a `graphite_in`
    ([ADR `graphite-carbon-relay`](../adr/graphite-carbon-relay.md)'s 2026-09-14 amendment, which
    put it on the same driver), or a `statsd_in` (the syslog ADR's own amendment, the driver's
    third listener). TLS needs a reliable, ordered byte stream, and its datagram sibling DTLS is
    out of scope throughout this project (RFC 6012 for syslog; neither carbon nor statsd has a DTLS
    receiver), so a `tls:` block under `transport: udp` could never take effect. It is rejected,
    as rule 24 rejects a `tls:` block under a plaintext `otlp_out` endpoint: an operator who wrote
    one meant the connection encrypted, and running it in the clear anyway is the worst outcome.
    A plaintext TCP listener is unaffected.

    **One rule, not one per listener.** The check, message, and reasoning are identical on every
    kind; only the kind's `transport` spelling differs. A listener that gains a stream transport
    joins by adding an arm to this rule's match, not by taking a new rule number. The *sink* rules
    (24/34/44/52) are the opposite, one per sink, because each also checks that sink's own `tls:`
    internals.
44. A `syslog_out` `tls:` block must have `cert_file`/`key_file` together and no
    `insecure_skip_verify` alongside `ca_file` — rule 34's two checks for `logit_out` (and rule
    24's for `otlp_out`). As with `logit_out`, the endpoint is a bare `host:port`, so the presence
    of `tls:` is the only "TLS is wanted" signal and there's no wrong-scheme case. One check is
    unique to this rule: `tls:` together with `transport: udp` is rejected
    ([ADR `syslog-tcp-ingress-and-tls`](../adr/syslog-tcp-ingress-and-tls.md)). Syslog over TLS is
    RFC 5425, TLS over *TCP*, and DTLS is out of scope, so accepting the block would leave an
    operator who asked for encryption on a plaintext datagram socket.
    `logit_outputs::syslog::SyslogOutput::with_tls` re-checks that last one itself, since
    `graph::resolve` isn't the only possible caller.

45. A `handshake_timeout` must be greater than `0s` on every kind that has one — `syslog_in`,
    `graphite_in`, `statsd_in`, `logit_in`, `otlp_in` — and must stay at its default where it can
    never take effect: a `syslog_in`, `graphite_in`, or `statsd_in` with `transport: udp`.
    - `0s` is impossible, not tight: no TLS accept, first-byte read, or `Hello` read completes in
      zero time, so the listener would close every connection immediately and receive nothing
      (rules 9/15/18/28).
    - The UDP check is rule 43's reasoning applied to this field, in rule 33's "only means
      anything under X" shape: a UDP listener has no connection to hand shake, so a set value
      would be silently ignored.
    - Only a *non-default* value is rejected, so the default stays legal everywhere. `graph.rs`
      imports `logit_config::default_handshake_timeout` to tell the two apart rather than
      mirroring the number, and `a_udp_syslog_in_at_the_default_handshake_timeout_resolves_fine`
      deserializes a real config rather than constructing the variant, so it exercises the same
      `serde` defaulting path the comparison must agree with.

    **`otlp_in` appears only in the `0s` check.** Its budget bounds both the TLS accept and the
    wait for a plaintext connection's first byte — a `TcpStream::peek` under the same budget,
    which consumes nothing and so leaves hyper's own version sniff untouched
    (`crates/logit-inputs/src/otlp.rs`,
    [ADR `otlp-tls-and-pooled-grpc-client`](../adr/otlp-tls-and-pooled-grpc-client.md)'s
    2026-09-14 amendment). The value is live with or without `tls:`, so there's no context to
    reject it in.

46. A `graphite_in`'s and `graphite_out`'s protocol/transport pair and size bounds
    ([ADR `graphite-carbon-relay`](../adr/graphite-carbon-relay.md)):
    - `protocol: pickle` requires `transport: tcp` on both kinds. Carbon frames a pickle batch
      with a 4-byte big-endian length prefix (Twisted's `Int32StringReceiver`), which has no
      meaning inside a self-delimiting datagram, so the combination could only mis-frame.
    - A zero `max_line_bytes` (`graphite_in`, plaintext+tcp), `max_frame_bytes` (either kind,
      pickle), or `connect_timeout` (`graphite_out`, tcp) is rejected. `max_line_bytes: 0` would
      drain every byte as one endless oversize line; `max_frame_bytes: 0` couldn't fit even
      carbon's two-opcode empty-list pickle frame; `connect_timeout: 0s` could never connect.
    - `max_frame_bytes` must be within `1024..=16 MiB` on both kinds. Below 1024 no real carbon
      batch fits; above 16 MiB a frame's *declared* length is a larger allocation than any sender
      has reason to ask for. This is rule 38's "a bound the transport can't honestly carry is a
      silent failure, not a generous setting," applied to a length-prefixed frame.
    - `graphite_out`'s `max_packet_bytes: 0` is rule 38's zero check. There is no collectd-style
      range clamp: an oversize packed datagram is already counted `oversize_datagram` and skipped,
      as in `statsd_out`'s `EMSGSIZE` handling.
47. A non-empty `targets:` is legal only on a `lua`/`lua_file` component
    ([ADR `target-components`](../adr/target-components.md)); on any other kind it is rejected by
    name (rule 14's shape). A `route` declares its targets through `routes:`' values, and no other
    kind can direct an event anywhere, so the list would be silently ignored.
48. Every id in `graph::targets_of` — a `lua`/`lua_file`'s `targets:` entries, a `route`'s
    `routes:` values — must resolve to a defined component, must not be the router itself, and must
    name a `target` kind. Directing at an ordinary component would be a `sources:` entry written on
    the wrong side of the edge (the inversion ADR `component-graph-configuration`'s "named outlets"
    rejection was about), and the message says so. A `lua`/`lua_file` `targets:` list may not
    repeat an id (rule 4, one hop over: two `Fanout`s into one target would deliver every routed
    batch twice). A `route` mapping several `routes:` values onto one target is legal and collapses
    to one slot; that many-to-one is what the kind is for. Rule 51's `routes:` shape checks run
    *before* this rule, so an empty `routes:` value is reported as empty rather than as an
    unresolved target id.
49. A `target` declares no `sources`: routers feed it, and it never names anything itself (checked
    in rule 6's arity match). Rule 7 still requires at least one consumer, and at least one router
    must direct at it — rule 7's mirror, since a target nothing routes to is the same black hole
    from the other end, and its consumers would wait forever.
50. Rule 7 is relaxed for routers only: a component with a non-empty `graph::targets_of` may have
    no consumers. A router's ordinary consumers receive its *unrouted* events, so without any those
    events are dropped and counted (`logit.component.events.dropped{reason="unrouted"}`), never
    silently.
51. A `route` needs a non-empty `routes:` map (rules 10/20's no-op reasoning), with no empty key
    (it could never match a real value) and no empty value (it could never name a real target).
    Under `by: {attribute: k}`/`{resource: k}`, `k` must be non-empty (rules 19/20's empty field
    name, applied to the one key a `route` reads).
52. A `statsd_out` `tls:` block gets rule 44's three checks, with its messages verbatim:
    `cert_file`/`key_file` together, no `insecure_skip_verify` alongside `ca_file`, and no `tls:`
    together with `transport: udp`, since DTLS is out of scope here too
    ([ADR `statsd-output`](../adr/statsd-output.md)'s TLS amendment). This sink also dials a bare
    `host:port`, where `tls:`'s presence is the only "TLS is wanted" signal. It is one rule per
    *sink* (24/34/44/52), unlike rule 43, because each sink also checks its own `tls:` internals.
    `logit_outputs::statsd::StatsdOutput::with_tls` re-checks the `transport: udp` case itself,
    since `graph::resolve` isn't the only possible caller.
53. An `idle_timeout`, where set, must be greater than `0s` on every kind that has one —
    `syslog_in`, `graphite_in`, `statsd_in`, `logit_in`, `otlp_in`, `prometheus_in` — and must not
    be set at all where it can never take effect: a `syslog_in`, `graphite_in`, or `statsd_in` with
    `transport: udp`, which has no connection to time out
    ([ADR `idle-connection-timeout`](../adr/idle-connection-timeout.md)). `0s` is impossible for a
    different reason than in rule 45: a connection is idle whenever the listener waits for its next
    byte, so a zero budget would close each one the moment it paused. The UDP check is rule 43's
    reasoning in rule 45's shape.

    **One rule, six kinds, and no default to exempt.** Like rule 43, and unlike the per-sink rules
    24/34/44/52, the check, message, and reasoning are identical on every kind, so a listener joins
    by adding an arm to the match. Unlike rule 45's `handshake_timeout`, this field is an `Option`:
    absent *is* "no idle timeout", so every `Some` is a set value and the UDP check rejects any of
    them. That is also why the zero message names the fix — "omit the field to disable the idle
    timeout" — rather than a legal value. `prometheus_in` has the field for its `bind:` receiver
    only. In *scrape* mode there is no connection to time out either, but that is a wrong-*mode*
    field rather than a wrong-*transport* one, so rule 55 rejects it instead of this rule growing
    a second axis.
54. A `keep_values` with neither `resource` nor `attributes` configured is rejected (rule 12's
    no-op reasoning), as is an empty field name in either map (rules 19/20). An empty `allow` list
    is rejected, and the message names the alternatives: `set` (with `other:`) or `remove`
    (without it) already mean "clamp everything on this field." A non-finite `F64` literal in
    `allow`/`other` is rejected (rule 36), since it could never compare equal under the coercing
    matcher. Under a field's `normalize: [lower]`, a `Str` literal in `allow`/`other` that isn't
    already ASCII-lowercase is rejected by name, because `lower` could never produce it; a
    duplicate step in one `normalize:` list is rejected as a no-op. An *empty* `normalize:` list is
    legal: it's the default, meaning no normalization
    ([ADR `value-allowlist-cardinality-clamp`](../adr/value-allowlist-cardinality-clamp.md)).
55. A `prometheus_in` is in exactly one mode, and every field must belong to the mode it's written
    under ([ADR `prometheus-remote-write`](../adr/prometheus-remote-write.md)). A non-empty
    `scrape_targets:` makes it a scrape client; `bind:` makes it a remote-write receiver accepting
    1.0 and 2.0 on one listener. Both is two components' config in one, and neither is a listener
    that can never produce an event (rules 7/12's no-op reasoning); each fails with a message
    rather than a process listening on nothing.
    - A non-default `interval`, `timeout`, `headers`, or `scrape_tls` alongside `bind:` is
      rejected, because a receiver performs no scrape.
    - A non-default `path`, `bind_tls`, `idle_timeout`, or `metadata_cache` alongside
      `scrape_targets:` is rejected: a scrape client binds nothing, and it reads the `# TYPE` line
      in every response instead of remembering one.

    This is rules 45 and 53's shape one kind over, for their reason: a setting that silently does
    nothing is worse than a startup failure naming it. Only *non-default* values are rejected,
    which lets `interval` keep its default in bind mode and satisfy rule 9's `interval: 0s`
    rejection without a mode-specific carve-out. In bind mode, `path` must start with `/` (rule
    41's check and reason: a relative or empty path could never match, and every write would `404`
    against a listener that looks configured). `metadata_cache.ttl` must be greater than `0s` (rule
    9's reasoning: an entry that expires as it's written makes a cache that does nothing but still
    sweeps on every request). `metadata_cache: {max_families: 0}` is how the cache is turned off,
    so that pairing, where `ttl` governs nothing, is the one case the zero check allows.

56. A `prometheus_out` has exactly one of `bind:` (serve an exposition) and `endpoint:` (write to a
    remote-write receiver) — never both, never neither
    ([ADR `prometheus-remote-write`](../adr/prometheus-remote-write.md)). A **non-default** value
    of a field that belongs to the other mode is rejected rather than ignored: `path`,
    `expire_after`, or `max_series` alongside `endpoint:`; `version`, `timeout`, `headers`, or
    `endpoint_tls` alongside `bind:`. This is rules 45 and 53's shape and reason, and like rule 45
    it compares against `logit_config`'s own default functions rather than mirroring their values,
    so the default stays legal in both modes. Rule 41's `path`/`max_series` checks run in registry
    mode only, for the same reason: two rules with one gate each, rather than rule 41 taking on a
    second job. In sender mode the remaining checks are rule 40's, restated for this kind:
    - `endpoint` must be an absolute `http://`/`https://` URL, path included (a remote-write
      receiver's write path, typically `/api/v1/write`, goes there, not in `path:`).
    - `timeout: 0s` is rejected (rule 9's reasoning).
    - `headers` may not be empty-named, `:`-prefixed (an HTTP/2 pseudo-header), collide with
      another entry once case is ignored, or name one this output sets itself: `content-type`,
      `content-encoding`, `content-length`, `x-prometheus-remote-write-version`, `user-agent`, the
      five in `graph::RESERVED_REMOTE_WRITE_HEADERS`.
    - The `endpoint_tls:` block must have `cert_file`/`key_file` together and no
      `insecure_skip_verify` alongside `ca_file` (rules 24/34/44/52's two checks, since it is a
      sink's own TLS block).
    - A non-default `endpoint_tls:` under a plain `http://` endpoint is rejected — rule 40's
      *scheme* check, since the endpoint's scheme selects TLS and the block would be ignored.
57. A datagram listener's `receive.read_batch` above `1024` is rejected
    ([ADR `udp-intake-batching-and-socket-visibility`](../adr/udp-intake-batching-and-socket-visibility.md)).
    `read_batch` is `recvmmsg(2)`'s `vlen`, and `1024` is `UIO_MAXIOV`'s value, but the ceiling is
    `logit`'s, not the kernel's: `do_recvmmsg` clamps no `vlen` at all (`UIO_MAXIOV` bounds
    `msg_iovlen` within one `msghdr`, which this read path sets to 1). The ceiling bounds the
    per-listener receive slab and the loss on the shutdown path, which both grow linearly with
    `read_batch`. Rule 18 owns the `0` end, the same split those two rules have for
    `max_datagrams`/`max_bytes`. A `read_batch` *larger than* `max_datagrams` is deliberately
    **legal**: `push_many` handles a batch bigger than the whole queue (evict or block per policy,
    per item, exactly as a sequence of single `push` calls would), so a rule against it would only
    refuse a configuration that works. The rule covers datagram listeners only: rule 17 already
    rejects a non-default `read_batch` on every other kind, so any other kind reaching this check
    has the default and passes.
58. A `shape`'s `max_tracked_keys` or `max_tracked_keysets` of `0` is rejected
    ([ADR `shape-observer-component`](../adr/shape-observer-component.md)), the impossible-bound
    shape of rules 9/15/18/28/45. A cap of `0` tracks nothing, so `logit.shape.distinct_keys` and
    `.distinct_keysets` would read `0`, and `logit.shape.tracking_overflow` `1`, forever. There is
    deliberately no way to turn the table off: the cumulative gauges are half of what `shape` is
    for, so an operator who doesn't want them removes the component. `shape`'s `interval` is
    covered by rule 9.
59. A `flatten` with `attributes: none` and `resource: none` is rejected (rules 7/12/54's no-op
    reasoning). An empty named list on either field is rejected, and the message names both
    keywords: `none` means nothing, `all` means every nested attribute. An empty field name in a
    named list is rejected (rules 19/20/54), and so is a name repeated within one list (no-op).
    `attributes: all` (the default) and a field name containing `.` are deliberately legal: the
    former is the useful default, and the latter names a literal attribute, as rule 54's
    `keep_values` fields may ([ADR `flatten-transform`](../adr/flatten-transform.md)).
60. `http_access` validation
    ([ADR `http-access-normalization`](../adr/http-access-normalization.md)):
    - Every `routes[].match` and `user_agent_rules[].match` must compile as a regex (rule 31).
    - An empty `match`, `route`, `class`, `route_other`, or `redact_query` entry is rejected (rules
      19/20/54). An empty `match` would match every path and hide every rule after it.
    - Each `routes` entry must be exactly `builtin`, or `match` together with `route`. The error
      names the missing half or the extra key, which is why an entry is one flat struct rather
      than an untagged enum (whose failure names no key). A repeated `builtin:` set is rejected,
      because the second can never match anything the first didn't.
    - A `max_length` key must name a field in `logit_config::CAPPED_FIELDS`, and the error lists
      them; a limit of `0` is rejected (rules 9/15/18/58).
    - `forwarded: {trust: false}` is rejected in favor of omitting the block, so "don't trust
      `X-Forwarded-For`" has one spelling.

    There is deliberately **no** "nothing configured" clause: a bare `type: http_access` still
    coerces, caps, derives, and classifies with the built-in tables.
61. `sample` validation
    ([ADR `consistent-sampling-component`](../adr/consistent-sampling-component.md)):
    - `rate` must be finite and within `[0, 1]` (rule 16's reasoning, since `sampling::keep`
      shares `trace_is_sampled`'s "NaN keeps everything" fallback).
    - `rate: 1` is rejected (it keeps every event), and so is `rate: 0` without `always_keep` (it
      keeps nothing — that's `null_out`); rules 7/12/54/59's no-op reasoning. `rate: 0` *with*
      `always_keep` is the "only flagged events" debugging mode and is allowed.
    - An empty `key:` or `always_keep:` field name is rejected (rules 19/20).
    - `always_keep` must name exactly one of `attribute`/`resource`, and a non-finite
      `always_keep.value` is rejected, since it can never match anything (rules 36/54).
    - `missing:` without `key:` is rejected: there's no key to be missing.

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
failure-triggered drain, and the exit code treat a Lua node exactly like any task.

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
  because the script picks the destination with `event:to("..")`. These edges come from
  `graph::target_edges`, which reads the raw `Config` too, so a router whose target id resolves to
  nothing renders as a dangling dashed edge rather than blocking output
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
