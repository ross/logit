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

This is the same statsd → aggregate → Lua → InfluxDB shape as today's
[examples/statsd-to-influxdb.yaml](../../examples/statsd-to-influxdb.yaml), reshaped: no `inputs`/
`outputs`/`pipelines` split, no separate `transforms:` chain — `sources` carries all the wiring, and
a "pipeline" is just whatever subgraph is reachable from a listener. There is no config-level notion
of a pipeline at all.

In Rust:

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
    // Splits each row of a delimited line into positional attributes named by a configured
    // `columns` list (docs/adr/csv-positional-columns.md).
    Csv { columns: Vec<String>, delimiter: char },
    // Equality-only routing: one key read per event, one target per matching value
    // (docs/adr/target-components.md). An unrouted event goes to this component's ordinary
    // consumers.
    Route { by: RouteBy, routes: BTreeMap<String, String> },
    // as each lands in logit-transforms, same shape: a `ComponentKind` variant, no `sources`
    // opinion of its own (that lives on `Component`, uniformly). `rename`/`filter`/`sample`/
    // `throttle`/`dedup` used to be sketched here too -- retired before landing, not merely
    // deferred: each is already expressible as a `lua` component, and
    // `docs/adr/routing-by-condition-is-lua.md` records why a native equivalent wasn't worth
    // building yet.

    InfluxDbOut { url: String, org: String, bucket: String, token: String },
    OtlpOut { endpoint: String },
    LogitOut { endpoint: String },
    // A named destination a router directs events into (docs/adr/target-components.md). No
    // fields, and no `sources` -- fed by direction, never by naming anything itself.
    Target {},
}
```

**Naming: every protocol kind is suffixed `_in`/`_out`.** Merging `InputConfig` and `OutputConfig`
into one tagged enum creates real collisions — `Otlp { bind }` (a listener) and `Otlp { endpoint }`
(a sink) can't both be `type: otlp` in an internally-tagged enum, and the same is true of the two
`Logit` variants. Suffixing *every* protocol kind uniformly, not just the two that collide today,
keeps the rule predictable as more protocols gain a second side — `syslog_out` (RFC 3164/5424 over
UDP or TCP, `docs/adr/syslog-output.md`) is exactly that case, landing well after `SyslogIn`.
Transform kinds — `lua`, `lua_file`, `aggregate`, `json`, `csv`, `kv_metrics`, `keep`,
`remove`, `set`, `trace_context`, `scale`, `has_signal`, `keep_signals`, `drop_signals`,
`has_attributes`, `drop_attributes`, `has_provenance`, `drop_provenance`, `logfmt`, `kv`, `regex`,
`shape`, `flatten`, and any future native transform — take no suffix; there's only ever one direction
for a transform to be.

**`interval` stays a per-kind optional field, unchanged from today.** `lua`/`lua_file` already carry
an optional flush interval (`docs/adr/aggregation-window-semantics.md`); `aggregate` requires
one. That doesn't change here — only where the field lives changes (on the component's `type`-tagged
kind, same as today) — and the existing "a zero interval is rejected" rule (`require_nonzero_interval`,
`crates/logit-cli/src/pipeline.rs`) carries over as-is, generalized to any kind with an `interval`.

**Implementation risk to verify early:** `schemars` 0.8 (pinned in `Cargo.toml`) generating
`#[serde(flatten)]` over an internally-tagged enum produces an `allOf` composition in the emitted
JSON Schema. Confirm `schema/logit.schema.json` still validates real configs and that `serde_norway`
round-trips it before committing to this exact shape; if `flatten` misbehaves, the fallback is
repeating `sources: Vec<String>` on every `ComponentKind` variant instead of factoring it onto
`Component`.

## Environment substitution

`!env VAR_NAME` is a YAML tag, valid as the value of any field on any component, resolved against
the process environment when the config is loaded (`crates/logit-cli/src/config.rs`) --
[ADR `env-yaml-tag`](../adr/env-yaml-tag.md). It's what `influxdb_out`'s `token` above is for: rather
than a dedicated `token_env` field (an earlier, rejected design -- see the ADR), any field that's
secret or deployment-specific spells it the same way:

```yaml
url: !env INFLUXDB_URL
token: !env INFLUXDB_TOKEN
```

Resolution happens on the parsed YAML tree, before serde ever sees it, so `Config`'s types carry no
trace of it: a `!env`-tagged field looks, to serde, exactly like a field written with the
substituted value inline. The substituted value is re-parsed as a YAML scalar (`8125` becomes an
integer, `true` a bool; anything else, including a value that happens to look like a mapping or
sequence, stays a string) -- this is what lets `!env` work on a non-string field, at the cost of a
secret that happens to look like a number or bool needing to be quoted at the source.

Every `!env` reference must resolve, unconditionally, for all three commands -- `logit graph`
included, even though it never reads a component's field values (only `sources` and `type`). Any
tag other than `!env` is a hard error too -- a typo'd tag would otherwise silently deserialize as
the tag's literal argument string instead of failing.

## Roles come from kind, not topology

| Kind class | `sources` | May be another component's source |
|---|---|---|
| Listener (`statsd_in`, `collectd_in`, `graphite_in`, `syslog_in`, `otlp_in`, `tail_in`, `docker_in`, `logit_in`, `prometheus_in`, `generate_in`) | must be empty | required (≥1 consumer) |
| Transform (`lua`, `lua_file`, `aggregate`, `json`, `csv`, `kv_metrics`, `keep`, `remove`, `set`, `trace_context`, `scale`, `has_signal`, `keep_signals`, `drop_signals`, `has_attributes`, `drop_attributes`, `has_provenance`, `drop_provenance`, `keep_values`, `logfmt`, `kv`, `regex`, `shape`, `flatten`, `route`) | ≥1 required | required (≥1 consumer) |
| Sink (`influxdb_out`, `stdio_out`, `file_out`, `otlp_out`, `syslog_out`, `logit_out`, `statsd_out`, `collectd_out`, `graphite_out`, `prometheus_out`, `null_out`) | ≥1 required | must not be |
| Target (`target`) | must be empty | required (≥1 consumer), and ≥1 directing router (rule 49) |

Rule 7's "≥1 consumer" column is relaxed for *routers* only (rule 50): a component that directs at
a target may have no ordinary consumers at all, since its consumers are only where its *unrouted*
events go — those are dropped and counted rather than silently lost
([ADR `target-components`](../adr/target-components.md)).

Deriving role from topology instead ("no sources → listener", "nothing reads it → sink") was
considered and rejected (ADR `component-graph-configuration`): a typo'd source reference would silently turn a real sink into
an orphaned transform, with no error, rather than a clear "did you mean" failure. The kind already
knows its own arity — config just states the edges.

## Routing: `route` and `target`

A router directs each event at a named `target` component instead of every consumer of a shared
upstream seeing every batch — the graph's answer to "send these events here and those there"
without a filter per branch ([ADR `target-components`](../adr/target-components.md)):

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

`route` reads one key per event — `by:` is exactly one of `{provenance: origin}`,
`{provenance: previous}`, `{attribute: <key>}`, `{resource: <key>}` — and `routes:` maps the values
that key can take onto `target` ids; several values may map to one target. A value the key holds
that no route names, or a missing key, is *unrouted* and falls through to the router's own
`sources:` edge (`untagged_out` above), not a chain of complementary filters. A `target` has no
fields and no `sources:` of its own: it's fed by direction, named by whichever router points at it,
and read by downstream components exactly like any other source (`windowed: {sources: [host_stream]}`,
say). `lua`/`lua_file` is the other router kind, choosing a target per event with `event:to("id")`
instead of an equality table — see `docs/design/lua-api.md`'s "Routing to a target." A complete,
runnable version of the config above is `examples/fan-out-central.yaml`.

## Validation

Replaces `validate_semantics` (`crates/logit-cli/src/pipeline.rs`). In order:

1. At least one component.
2. Every id appearing in any `sources` list resolves to a defined component.
3. No self-reference (a component listing itself as a source) — a special case of 5, but worth its
   own message since it's the most common typo shape.
4. No duplicate source within one component's `sources` list — a repeated id would otherwise push
   the same consumer onto that source's outbound edge list twice, giving its `Fanout` two live
   `Sender` clones into the same inbox and silently delivering every batch twice (doubling
   telemetry, and doubling every count through an `aggregate` component) rather than being
   rejected as the config typo it almost certainly is.
5. **No cycles.** DFS with a recursion-stack set, or Kahn's algorithm falling back to "nodes remain
   with no zero-indegree candidate." This is the one genuinely new must-have: a cycle plus bounded
   `mpsc` channels is a deadlock, not a slow pipeline, and today's linear-pipeline shape made cycles
   structurally impossible — the graph model makes them a real config mistake to guard against. The
   nodes still unresolved once Kahn's algorithm runs out of zero-indegree candidates are the cycle
   *plus* everything downstream of it, not the cycle alone — the error walks that set back down to
   one concrete cycle path before reporting it, so a downstream victim is never named as if it were
   part of the cycle.
6. Arity per kind, per the table above — a listener with `sources`, a sink with none, a sink named as
   another component's source.
7. Every non-sink component has ≥1 consumer — replaces today's "pipeline has no outputs" check and
   catches the same silent-black-hole failure (a transform whose only consumer was renamed or
   deleted, still accumulating state or running Lua for nothing).
8. Kind is actually implemented — the direct generalization of `require_implemented_input`/
   `require_implemented_output`/`require_implemented_transform` into one check over `ComponentKind`.
9. No zero-length `interval` on any kind that has one (`lua`, `lua_file`, `aggregate`) — unchanged
   from `require_nonzero_interval` today.
10. A `kv_metrics` with `counters`, `gauges`, and `distributions` all empty is rejected — it can
    only ever be a no-op, the same silent-black-hole failure rule 7 exists to catch.
11. A `kv_metrics` distribution entry with no `field` is rejected — a distribution of nothing is
    meaningless (`docs/adr/kv-metrics-semantics.md`).
12. A `kv_metrics` counter, gauge, or distribution entry with an empty `name` is rejected — the
    implemented `influxdb_out` sink can't encode a metric with no measurement name
    (`docs/adr/kv-metrics-semantics.md`). (Numbering note: `crates/logit-pipeline/src/graph.rs`'s
    own rule comments have drifted from this list since `set` landed -- its code folds this
    `kv_metrics` check under one "Rules 10 + 11" comment alongside the two above it, and reuses
    the number 12 for `set`'s own no-op check instead. The rules themselves are unchanged; only
    the comment numbering no longer lines up one-to-one with this list.)
13. At most one `internal` component — two would each drain (and so split) the same process-wide
    telemetry `Registry`, silently halving whichever one a downstream consumer happened not to be
    reading from rather than failing clearly.
14. A non-default `buffer:` block on a non-sink component is rejected — `buffer:`
    (`docs/adr/buffered-sink-delivery.md`) configures a sink's delivery queue, which only a
    sink has, so a listener or transform carrying one is almost certainly a misplaced block rather
    than a meaningful setting silently ignored.
15. A sink's `buffer.max_batches` or `buffer.max_bytes` of `0` is rejected — an impossible bound
    (no batch could ever be queued) rather than a small one.
16. `internal`'s `span_sample_rate` must be finite and within `[0, 1]` — a config error, not
    something to clamp silently.
17. A non-default `receive:` block is rejected on any kind that is not a **datagram listener**
    (`docs/adr/decoupled-listener-io.md`, `collectd_in`, and `statsd_in`/`syslog_in`/`graphite_in`
    under `transport: udp`), a **stream listener** (one shared driver,
    `docs/adr/syslog-tcp-ingress-and-tls.md` plus `docs/adr/graphite-carbon-relay.md`'s 2026-09-14
    amendment: `syslog_in`/`graphite_in`/`statsd_in` under `transport: tcp`), or a **tail
    listener** (`docs/adr/file-tailing-and-docker-json-logs.md`, `tail_in`/`docker_in`). Deliberately not
    "any non-listener": `internal` and `generate_in` are listeners by role but have no socket, no
    queue, and no decoder, so `receive:` on either would be exactly the silently-ignored-setting
    failure rule 14 guards against on the sink side. A tail listener has no receive *queue* at all (the tailed
    file is its own durable buffer) — only `receive.batch_max_events`, `batch_max_bytes`,
    `batch_flush_interval`, and `shutdown_grace` are meaningful on one; a queue-bounding field
    (`max_datagrams`, `max_bytes`, `overflow`, `receive_buffer_bytes`), or `read_batch` — which
    sizes one `recvmmsg(2)` read and the matching `pop_many` off that same queue
    ([ADR `udp-intake-batching-and-socket-visibility`](../adr/udp-intake-batching-and-socket-visibility.md))
    — is rejected by name. A
    stream listener has no receive queue either, for a different reason — the TCP connection's own
    flow control *is* the backpressure, so a blocked `Fanout::send` just stops the socket being
    read and the peer's window closes — so the same five queue fields are rejected by name on one,
    with a message that says so, and the same four batch/shutdown fields apply, scoped **per
    connection** rather than per listener (N live connections can hold up to N ×
    `batch_max_events` in flight).
18. A datagram listener's `receive.max_datagrams`, `receive.max_bytes`, `receive.read_batch`, or
    `receive.batch_max_events`
    of `0` is rejected — the twin of rule 15. (`read_batch: 0` is `recvmmsg`'s `vlen` of zero, which
    reads nothing, forever; rule 57 owns its upper end.) `receive.batch_flush_interval: 0s` is **not**
    rejected — it means "no flush timer," a meaningful setting, unlike the count bounds. A tail or
    stream listener's `receive.batch_max_events`/`batch_max_bytes` of `0` is rejected the same way
    (the queue-only bounds don't apply to either at all — see rule 17).
19. (Also since drifted into the code's numbering, see the note on 12 above.) A `set` with both
    `resource` and `attributes` empty is rejected, as is an empty key in either map (added
    alongside `has_attributes`/`drop_attributes` below, so `set`'s own validation matches what its
    config actually allows) — and a `trace_context` with an empty `trace_id` field name is
    rejected — all the same "can only ever be a no-op" reasoning rules 10-12 apply to `kv_metrics`,
    extended to the components that landed after this list was written
    (`docs/adr/operator-declared-resource-attributes.md`,
    `docs/adr/log-record-trace-context.md`).
20. A `scale` with an empty `fields` map is rejected (the same no-op reasoning again), as is an
    empty field name within it (the same reasoning rule 19 applies to `trace_context`'s `trace_id`)
    or a non-finite factor (`docs/adr/scale-transform.md`).
21. An empty `signals:` list on `has_signal`, `keep_signals`, or `drop_signals` is rejected.
    `keep_signals`/`drop_signals` additionally reject naming all three signals. Which of the two
    shapes is the silent black hole (rule 7's "no consumer" failure, recast here as "no event
    ever gets through") and which is the no-op (every event forwarded untouched) is *opposite*
    between the two kinds — an allowlist naming nothing keeps nothing (black hole), naming
    everything keeps everything (no-op); a denylist is the mirror. Both shapes are rejected
    either way, but the error message names the right one. `keep`'s empty `fields` list stays
    legal by contrast — "drop every attribute" is a real operation, "drop every event" is not.
    See `docs/adr/signal-filtering-components.md`.
22. An `otlp_out` `headers:` entry may not name a header the protocol itself sets (`content-type`,
    `content-length`, `content-encoding`, `host`, `te`, `transfer-encoding`, `connection`, any
    `grpc-*` header, an empty name, or an HTTP/2 pseudo-header starting with `:`) — checked
    case-insensitively.
23. `otlp_out`'s `paths:` is HTTP-only — gRPC method names are fixed by the `.proto` service
    definitions, not a mount point an operator can move, so a non-empty `paths:` under
    `protocol: grpc` is rejected rather than silently ignored (the same instinct as rule 14's
    `buffer:` on a non-sink).
24. An `otlp_out` `tls:` block: `cert_file`/`key_file` must be set together (mutual TLS needs
    both, not one alone); `insecure_skip_verify` together with `ca_file` is contradictory and
    rejected; and a non-empty `tls:` under a plain `http://`/`grpc://` endpoint is rejected —
    TLS is selected by `endpoint`'s scheme
    (`docs/adr/otlp-tls-and-pooled-grpc-client.md`), so a `tls:` block with nothing to tune would
    otherwise be silently ignored rather than caught as a likely mistake.
25. A `trace_context` `span:` block with an empty `name` (OTLP requires every span to have a
    name) or a `max_skew` of `0s` (an impossible window — every span would be rejected as skewed,
    the same instinct as rule 9's zero `interval`) is rejected
    (`docs/adr/trace-context-span-lifting.md`). Rule 19 also covers an empty `span_id`/`flags`
    field name on the same component now that those default to `span.id`/`trace.flags` — `null`
    disables a lookup, `""` is a typo.
26. `tail_in`'s `paths:` must name at least one file, no entry may be empty, and a `*` wildcard is
    permitted only in the final path component (`/var/log/app/*.log`, not `/var/*/app.log`) —
    the minimal glob subset `PathPattern` implements; a directory-position wildcard would silently
    never match anything (`docs/adr/file-tailing-and-docker-json-logs.md`).
27. `docker_in`'s `containers:` must be non-empty or `discover: true` must be set — explicit
    selection is the default, so a config with neither would silently tail nothing, the same
    black-hole reasoning rule 7 exists to catch. No empty entry in `containers:`/`labels:`, no
    duplicate `containers:` entry, and `root:` must be non-empty. Not reachable until `docker_in`
    itself is implemented (rule 8 rejects it first until then).
28. A `tail_in`/`docker_in` `poll_interval`, `checkpoint_interval`, or `max_line_bytes` of `0` is
    rejected — `poll_interval`/`checkpoint_interval` at `0s` would busy-loop (the same reasoning
    as rule 9's zero `interval`), and `max_line_bytes: 0` would drop every line
    (`docs/adr/file-tailing-and-docker-json-logs.md`).
29. A `file_out` whose `rotate:` block sets neither `max_bytes` nor `interval` is rejected — that
    would silently never rotate at all, and `stdio_out` already covers the deliberate never-rotate
    case, so this is a config error rather than a quiet no-op. `rotate.max_bytes: 0` (every batch
    would rotate) and `rotate.max_files: 0` (would delete the file it just rotated) are each an
    impossible bound, the same "0 is impossible, not just small" instinct as rules 9/15/18/28
    (`docs/adr/rotating-file-output.md`).
30. A `kv` with an empty `pair_sep` or `kv_sep`, with `pair_sep == kv_sep`, or with a `kv_sep`
    that *contains* `pair_sep`, is rejected — each is a certain no-op or a certain garbage result
    (`docs/adr/logfmt-and-kv-parsing.md`). `logfmt` needs no rule of its own: past `bare_keys`,
    its only field is a `bool`, which can't be malformed.
31. A `regex` with an empty `field` name, a `pattern` that fails to compile, or a `pattern` that
    declares no named capture group, is rejected — the first is the usual "empty is useless" case,
    the second can only ever be a config mistake, and the third can only ever be a no-op
    (`docs/adr/regex-transform.md`).
32. A `csv` with an empty `columns` list, an empty column name, or a duplicate column name is
    rejected (the "can only ever be a no-op" and "a repeated entry silently doubles" rules again,
    the latter applied to columns instead of sources), as is a `delimiter` that is `"` (RFC
    4180's quote character), `\n`/`\r` (already consumed as line framing by every input), or
    non-ASCII (`docs/adr/csv-positional-columns.md`).
33. `stdio_out`/`file_out`'s `compression:` is rejected whenever set to anything but `none` under
    the default `format: human` — it would silently do nothing, since compression is
    `NativeEncoder`'s own knob (`docs/adr/file-output-native-format.md`).
34. A `logit_out` `tls:` block: `cert_file`/`key_file` must be set together, and
    `insecure_skip_verify` together with `ca_file` is contradictory — the same two checks rule 24
    makes for `otlp_out`'s `tls:` block, minus its third (scheme-based) check: `logit_out`'s
    `endpoint` is a bare `host:port` with no scheme to read a TLS signal from, so `tls:`'s mere
    presence is the only signal and always turns TLS on. And a `logit_in` `max_frame_bytes`, when
    set, must be nonzero and at or under 64 MiB — `logit_proto::frame::MAX_SANE_UNCOMPRESSED_LEN`,
    the ceiling `read_frame`/`read_frame_with_header` themselves enforce regardless of what a
    listener configures.
35. A sink's `buffer.disk:` block (`docs/adr/disk-backed-sink-buffer.md`) is rejected alongside a
    non-default `buffer.max_batches`/`max_bytes` — disk replaces the in-memory bound rather than
    sizing alongside it, so a set-but-ignored value is a config error, the same reasoning as 33.
    `buffer.disk.segment_bytes`/`max_bytes` of `0` are each an impossible bound (the "0 is
    impossible, not just small" instinct of 9/15/18/28/29), and `segment_bytes` may not exceed
    `max_bytes`. Two sinks may not declare the same literal `buffer.disk.path` (compared as
    written, not resolved against the config directory — `DiskQueue`'s own exclusive lock catches
    an aliased path this comparison can't see).
36. `has_attributes`/`drop_attributes`: at least one of `resource`/`attributes` must be non-empty,
    every key must be non-empty, and every value must be a finite number
    (`docs/adr/attribute-filtering-components.md`). The empty-config black-hole/no-op assignment is
    *inverted* from rule 21's: `resource:`/`attributes:` is a map of conjunctions, not a list of
    alternatives, so zero configured pairs is vacuously true — `has_attributes` with nothing
    configured matches (and so forwards) every event, a no-op, while `drop_attributes`, being its
    exact complement, matches every event too but that means dropping every one of them, a black
    hole. The same key appearing in both `resource:` and `attributes:` is deliberately legal — they
    address different objects.
37. `has_provenance`/`drop_provenance`: at least one of `origin`/`previous` must be non-empty, no
    entry in either list may be empty, and no list may contain a duplicate entry
    (`docs/adr/provenance-filtering-components.md`). The empty-config black-hole/no-op assignment
    lines up with rule 36's, not rule 21's, despite each field's own contents being a list of
    alternatives (`signals:`'s own shape): an empty `origin:`/`previous:` means "this field isn't
    part of the match" (vacuously true), not "match against zero alternatives" (vacuously false) —
    it's the *field*, not the list, that decides which orientation applies. So both fields empty
    means `has_provenance` matches every batch, a no-op, and `drop_provenance`, its exact
    complement, drops every one of them, a black hole. Deliberately *not* validated: that a
    configured id names a component present in this graph — `origin`/`previous` are exactly as
    likely to name a component in a different process's graph, relayed unchanged across
    `logit_out`/`logit_in`.
38. A `statsd_out`, `collectd_out`, or `graphite_out` `max_packet_bytes: 0` is rejected, the same
    shape as rule 15's `buffer.max_batches`/`max_bytes: 0` — an impossible bound (every metric
    line/value list would overflow it and be dropped whole), not a small one
    (`docs/adr/statsd-output.md`, `docs/adr/collectd-binary-relay.md`,
    `docs/adr/graphite-carbon-relay.md`). `collectd_out` additionally rejects any value outside
    `1024..=65535` — collectd's own `MaxPacketSize` range (`docs/adr/collectd-binary-relay.md`):
    above it, every datagram fails `EMSGSIZE` at the socket (no UDP payload is that large), which
    `collectd_out` counts as a per-datagram drop rather than surfacing as a `Fault` — so an
    unbounded value would silently report `requests{class="ok"}` while delivering nothing.
    `statsd_out` and `graphite_out` make no such range claim in their own ADRs, so they keep only
    the zero check.
39. An `aggregate` with `temporality: cumulative` requires `series_retention >= 1` (a count of
    windows, not a duration) and `max_retained_series >= 1` — those two bounds are what keeps a running total alive
    across the window boundary, so with either at `0` no accumulator survives a flush and every
    window would emit its own increment labelled as a cumulative total, a silently wrong number for
    the consumer that mode exists for. `series_retention: 0` stays legal under the default
    `temporality: delta`, where it is the documented opt-out from gauge retention
    (`docs/adr/aggregation-window-semantics.md`'s cumulative amendment).
40. A **scrape-mode** `prometheus_in`'s scrape settings. Every check in this rule is a statement
    about an outbound scrape, so the whole rule is gated on a non-empty `scrape_targets` and says
    nothing at all about a `bind:` receiver; rule 55 owns the mode itself, including the
    neither-mode case this rule's empty-list bail used to catch by accident
    ([ADR `prometheus-remote-write`](../adr/prometheus-remote-write.md)). Each `scrape_targets`
    entry must be an absolute `http://`/`https://` scrape URL with
    a non-empty authority — `logit-pipeline` doesn't depend on `reqwest`/`url` (this document's own
    "Crate layout" section), so this is a small hand-rolled scheme/authority check, not a full URL
    parse. `timeout: 0s` is rejected, the same "0 is impossible" reasoning as rule 9's `interval`
    (which also covers `prometheus_in`'s own `interval: 0s`, via the same generic check). A
    `scrape_tls:`
    block must be internally consistent — `cert_file`/`key_file` set together, no
    `insecure_skip_verify` alongside `ca_file` — the same two checks rule 24 makes for `otlp_out`'s
    own `tls:` block (and rule 34 for `logit_out`'s) — and is rejected outright unless at least one
    target is `https://` — TLS is selected per-target by its own scheme, so a `scrape_tls:` block
    with
    nothing to tune would otherwise be silently ignored, the same reasoning as rule 24's third
    check. `headers` may not name a header this input sets
    itself (`accept`, `user-agent`, and the other protocol-owned names) or collide with another
    entry once case is ignored, checked case-insensitively — the same shape rule 22 already checks
    for `otlp_out`'s own `headers:` (`docs/adr/prometheus-scrape-and-exposition.md`). `receive:` on
    `prometheus_in` stays rejected via rule 17's own allowlist — it's a listener by role, but isn't
    wired to either of the two drivers that rule actually permits a non-default block on.
41. A `prometheus_out` `path:` must start with `/`, and `max_series` must be ≥ 1
    (`docs/adr/prometheus-scrape-and-exposition.md`). A request URI's path is always absolute, so a
    relative or empty `path:` could never match one — every scrape would 404 against an endpoint
    that looks configured. `max_series: 0` is rule 38's impossible bound in another shape: every
    series would be evicted the instant it arrived, so the endpoint would always be empty.
42. A `generate_in`'s bounds and templates (`docs/plans/load-test-harness.md`). `count`, `batch`,
    and `rate` must each be at least 1 where set — `0` generates nothing at all, the impossible
    bound of rules 9/15/18/38 rather than a small one, and omitting `count`/`rate` is already how
    "unbounded"/"unthrottled" is spelled. A `metric` must carry a non-empty `name` and a finite
    `value`. No `event.attributes` or `resource` key may be empty. And every template string —
    `event.log`, every `event.attributes` value, every `resource` value, and `event.metric.name` —
    must parse as a `logit_core::template` and may name only the placeholders `generate_in`
    actually substitutes: `seq`, or `seq%N` with `N >= 1`. An unknown placeholder is rejected here
    rather than rendered literally or as nothing: a mistyped `{seg}` would otherwise silently
    collapse a scenario's intended cardinality to a single series, which is the difference between
    measuring an aggregation window and measuring nothing. **`event.metric.name` is narrower
    still: only `{seq%N}`, never a bare `{seq}`.** A metric name is *interned*, and
    `logit_core::interner` is monotonic — a `Symbol` is never removed (`docs/design/memory.md`
    §4) — so an unbounded metric name would intern a fresh, never-reclaimed name for every event a
    run generates: a process-lifetime leak wearing a cardinality knob's clothes. A log body or an
    attribute value is copied onto the event and freed with it, so `{seq}` stays legal there.
    The var-name check lives in a small
    pure `generate_var_is_valid` helper in `graph.rs` so that `logit-inputs`' own `compile`
    resolver can mirror it exactly without depending on `logit-config` (this document's own
    "Crate layout" section). `receive:` on a `generate_in` is rejected by rule 17's own allowlist
    — it is a listener by role, with no socket, queue, or decoder for `receive:` to configure.
43. A listener carrying a `tls:` block must be on a stream transport — `transport: tcp` on a
    `syslog_in` ([ADR `syslog-tcp-ingress-and-tls`](../adr/syslog-tcp-ingress-and-tls.md)), a
    `graphite_in` ([ADR `graphite-carbon-relay`](../adr/graphite-carbon-relay.md)'s 2026-09-14
    amendment, which put that listener on the same driver) or a `statsd_in` (that same syslog ADR's
    own amendment, the driver's third listener). TLS is defined over a reliable ordered
    byte stream, and its datagram sibling DTLS is out of scope throughout this project (RFC 6012
    for syslog; neither carbon nor statsd has a DTLS receiver at all) — so a `tls:` block under
    `transport: udp`
    could never take effect. Rejected rather than ignored, the same call rule 22 makes for a
    `tls:` block under a plaintext `otlp_out` endpoint: an operator who wrote one meant the
    connection encrypted, and running it in the clear anyway is the worst of the available
    outcomes. Nothing here constrains a plaintext TCP listener, which stays perfectly ordinary.

    **One rule, not one per listener.** The check, the message and the reasoning are identical on
    every kind it covers; only the kind's own `transport` spelling differs. So a listener that
    grows a stream transport joins this rule by adding an arm to its match — three have done so
    now — rather than by claiming another rule number: the opposite convention from the *sink* rules
    (24/34/44/52), which stay one per sink because each also checks that sink's own `tls:`
    internals.
44. A `syslog_out` `tls:` block must be internally consistent — `cert_file`/`key_file` set
    together, no `insecure_skip_verify` alongside `ca_file` — the same two checks rule 34 makes
    for `logit_out`'s own `tls:` block (and rule 24 for `otlp_out`'s), for the same reason: both
    of those sinks dial a bare `host:port` endpoint, so `tls:`'s mere presence is the only signal
    that TLS is wanted and there is no "wrong scheme" case to catch. Plus one check this rule
    alone makes: a `tls:` block together with `transport: udp` is rejected
    ([ADR `syslog-tcp-ingress-and-tls`](../adr/syslog-tcp-ingress-and-tls.md)). Syslog over TLS is
    RFC 5425 — TLS over *TCP* — and DTLS is out of scope, so accepting the block and ignoring it
    would leave an operator who asked for encryption on a plaintext datagram socket.
    `logit_outputs::syslog::SyslogOutput::with_tls` re-checks that last one itself, since
    `graph::resolve` isn't the only possible caller.

45. A `handshake_timeout` must be greater than `0s` on every kind that has one — `syslog_in`,
    `graphite_in`, `statsd_in`, `logit_in`, `otlp_in` — and must be left at its default where it
    could never take effect: a `syslog_in`, a `graphite_in` or a `statsd_in` with `transport: udp`.
    `0s` is an impossible budget rather than a tight one: no TLS accept, first-byte read,
    or `Hello` read completes in zero time, so a listener configured with it would accept
    connections only to close each one immediately and would receive nothing at all — the same
    "0 is impossible, not just small" call rules 9/15/18/28 make for a flush interval, a queue
    bound, and a poll interval. The context check is rule 43's reasoning applied to this
    field instead of `tls:`, in rule 33's "only means anything under X" shape: a UDP `syslog_in`,
    `graphite_in` or `statsd_in` has no connection to hand shake at all, so an operator who set a value there
    meant it to take effect and set-but-ignored is an error rather than a silent no-op. Only a *non-default* value
    is rejected, so the field's own default stays legal everywhere and no pre-existing config
    becomes invalid; `graph.rs` imports `logit_config::default_handshake_timeout` to make that
    distinction rather than mirroring the number, and
    `a_udp_syslog_in_at_the_default_handshake_timeout_resolves_fine` deserializes a real
    config rather than constructing the variant, so it exercises the `serde` defaulting path this
    comparison has to agree with.

    **A plaintext `otlp_in` was the second such case and no longer is.** That listener's budget
    once bounded its TLS accept alone, so with no `tls:` block it was inert and a set value was
    rejected. It now also bounds the wait for a plaintext connection's very first byte — a
    `TcpStream::peek` under the same budget, which consumes nothing and so leaves hyper's own
    version sniff untouched (`crates/logit-inputs/src/otlp.rs`, and
    [ADR `otlp-tls-and-pooled-grpc-client`](../adr/otlp-tls-and-pooled-grpc-client.md)'s 2026-09-14
    amendment) — so the value is live with or without `tls:` and this rule no longer names
    `otlp_in` in its context check at all, only in the `0s` one.

46. A `graphite_in`'s and a `graphite_out`'s protocol/transport pair and size bounds
    ([ADR `graphite-carbon-relay`](../adr/graphite-carbon-relay.md)). `protocol: pickle` requires
    `transport: tcp` on both kinds: carbon frames a pickle batch with a 4-byte big-endian length
    prefix (Twisted's `Int32StringReceiver`), which has no meaning in a datagram that already
    delimits itself, so the combination could only ever mis-frame rather than work slightly worse.
    A zero `max_line_bytes` (`graphite_in`, plaintext+tcp), `max_frame_bytes` (either kind,
    pickle), or `connect_timeout` (`graphite_out`, tcp) is rejected — the impossible bound of
    rules 9/15/18/38 again (`max_line_bytes: 0` would drain every byte as one endless oversize
    line; `max_frame_bytes: 0` could never fit even carbon's own two-opcode empty-list pickle
    frame; `connect_timeout: 0s` could never establish a TCP connection at all). `max_frame_bytes`
    is additionally held to `1024..=16 MiB` on both kinds: below 1024 no real carbon batch fits,
    and above 16 MiB a frame's *declared* length is a larger allocation than any sender has a
    reason to ask for — rule 38's "a bound the transport cannot honestly carry is a silent
    failure, not a generous setting" applied to a length-prefixed frame. `graphite_out`'s
    `max_packet_bytes: 0` is rule 38's own zero check, extended (no collectd-style range clamp: an
    oversize packed datagram is already counted `oversize_datagram` and skipped, `statsd_out`'s
    own `EMSGSIZE` handling).
47. A non-empty `targets:` is legal only on a `lua`/`lua_file` component
    ([ADR `target-components`](../adr/target-components.md)). On any other kind it is rejected by
    name — rule 14's shape: a `route` declares its targets through `routes:`' values, and no other
    kind has any way to direct an event anywhere, so a set-but-ignored list is a config error
    rather than a setting silently doing nothing.
48. Every id in `graph::targets_of` — a `lua`/`lua_file`'s `targets:` entries, a `route`'s
    `routes:` values — must resolve to a defined component, must not be the router itself, and must
    name a `target` kind: a router may never direct at an ordinary component, which would be a
    `sources:` entry written on the wrong side of the edge (the inversion ADR
    `component-graph-configuration`'s "named outlets" rejection was about), so the message says so.
    A `lua`/`lua_file` `targets:` list may not repeat an id — rule 4's reasoning, one hop over: two
    `Fanout`s into the same target would deliver every routed batch to it twice. A `route` mapping
    several `routes:` values onto one target is legal by contrast and collapses to one slot — that
    is the many-to-one the kind is for. Rule 51's `routes:` shape checks deliberately run *before*
    this rule, so an empty `routes:` value is reported as the empty value it is rather than as an
    unresolved target id.
49. A `target` declares no `sources` — it is fed by direction, from a router that names it, never
    by naming anything itself (checked in rule 6's own arity match, where the rest of the table
    lives). Rule 7 still requires it to have at least one consumer, and it must also be directed to
    by at least one router: rule 7's mirror, since a target nothing routes to is the same black
    hole seen from the other end — its consumers would wait on it forever.
50. Rule 7 is relaxed for routers only: a component with a non-empty `graph::targets_of` is exempt
    from the "no consumers" rejection. A router's ordinary consumers are where its *unrouted*
    events go, so a router without any is a legal config — those events are dropped and counted
    (`logit.component.events.dropped{reason="unrouted"}`), never silently.
51. A `route` needs a non-empty `routes:` map — an empty one can only ever be a no-op, rules
    10/20's reasoning — with no empty key (it could never match a real value) and no empty value
    (it could never name a real target), and, under `by: {attribute: k}`/`{resource: k}`, a
    non-empty `k`: rule 19/20's empty-field-name rejection, applied to the one key a `route` reads
    per event.
52. A `statsd_out` `tls:` block must be internally consistent — `cert_file`/`key_file` set
    together, no `insecure_skip_verify` alongside `ca_file` — rule 44's three checks with its
    messages verbatim, since this sink dials the same bare `host:port` where `tls:`'s mere
    presence is the only "TLS is wanted" signal there is. Plus that rule's own third check: `tls:`
    together with `transport: udp` is rejected, since DTLS is out of scope here too
    ([ADR `statsd-output`](../adr/statsd-output.md)'s TLS amendment). One rule per *sink*
    (24/34/44/52), unlike rule 43's one-rule-for-every-listener, because each sink also checks its
    own `tls:` internals. `logit_outputs::statsd::StatsdOutput::with_tls` re-checks the
    `transport: udp` one itself, since `graph::resolve` isn't the only possible caller.
53. An `idle_timeout`, where set, must be greater than `0s` on every kind that has one —
    `syslog_in`, `graphite_in`, `statsd_in`, `logit_in`, `otlp_in`, `prometheus_in` — and must not
    be set at all
    where it could never take effect: a `syslog_in`, a `graphite_in` or a `statsd_in` with
    `transport: udp`, which has no connection to time out
    ([ADR `idle-connection-timeout`](../adr/idle-connection-timeout.md)). `0s` is rule 45's
    impossible bound for a different reason than rule 45's own: every connection is momentarily
    idle whenever this listener is waiting on its next byte, so a zero budget would close each one
    the instant it stopped sending — rules 9/15/18/28/45's "0 is impossible, not just small" call
    either way. The context check is rule 43's reasoning and rule 45's shape, one field over.

    **One rule, six kinds — and no default to exempt.** Like rule 43 (and unlike the per-sink
    rules 24/34/44/52) the check, the message and the reasoning are identical on every kind it
    covers, so a listener joins by adding an arm to its match. Each kind's arm landed in the same
    PR that made that listener *honour* the field, so no released state ever accepted a
    set-but-ignored `idle_timeout`. Where rule 45 has to tell a defaulted `handshake_timeout` from a set one,
    this field is an `Option`: absent *is* "no idle timeout", so every `Some` is a set value and
    the UDP check rejects any of them rather than only a non-default one. That is also why the
    zero message names the fix — "omit the field to disable the idle timeout" — instead of a legal
    value to use instead. `prometheus_in` carries the field for its `bind:` receiver only; in
    *scrape* mode there is no connection to time out either, but that is a wrong-*mode* field
    rather than a wrong-*transport* one, so rule 55 rejects it instead of this rule growing a
    second axis.
54. A `keep_values` with neither `resource` nor `attributes` configured is rejected — rule 12's
    "can only ever be a no-op" reasoning. An empty field name in either map is rejected — rules
    19/20's reasoning. A field's `allow` list being empty is rejected too, naming the alternative:
    `set` (with `other:`) or `remove` (without it) already say "clamp everything on this field."
    A non-finite `F64` literal in `allow`/`other` is rejected — rule 36's finiteness reasoning,
    since it could never compare equal to anything under the coercing matcher. Under a field's own
    `normalize: [lower]`, a `Str` literal in `allow`/`other` that isn't already ASCII-lowercase is
    rejected by name, since a `lower` step could never produce it; a duplicate step within one
    field's `normalize:` list is rejected too, the same no-op reasoning once more. An *empty*
    `normalize:` list is legal — it's the default, meaning no normalization at all
    ([ADR `value-allowlist-cardinality-clamp`](../adr/value-allowlist-cardinality-clamp.md)).
55. A `prometheus_in` is in exactly one mode, and every field belongs to the mode it is written
    under ([ADR `prometheus-remote-write`](../adr/prometheus-remote-write.md)). A non-empty
    `scrape_targets:` is a scrape client; `bind:` is a remote-write receiver accepting 1.0 and 2.0
    on one listener. Both together is two components' worth of config in one, and neither is a
    listener that could never produce an event — rules 7/12's "can only ever be a no-op" instinct,
    answered with a message rather than a process that starts up listening on nothing. Then a
    non-default `interval`, `timeout`, `headers` or `scrape_tls` alongside `bind:` is rejected (a
    receiver performs no scrape), and a non-default `path`, `bind_tls`, `idle_timeout` or
    `metadata_cache` alongside `scrape_targets:` is rejected (a scrape client binds nothing, and
    reads a `# TYPE` line in every response it scrapes rather than remembering one). That is rule
    45's and rule 53's shape one kind over, and it exists for their reason: a setting silently
    doing nothing is worse than a startup failure naming it. Only *non-default* values are
    rejected — which is what lets `interval` keep its default in bind mode, so rule 9's
    `interval: 0s` rejection stays satisfied there with no mode-specific carve-out. In bind mode
    the `path` itself must also start with `/` — rule 41's check for `prometheus_out`, for rule
    41's reason: a request URI's path is always absolute, so a relative or empty one could never
    match, and every write would `404` against a listener that looks configured — and
    `metadata_cache.ttl` must be greater than `0s`, rule 9's zero-interval reasoning: an entry
    expiring the instant it is written is a cache that does nothing while still sweeping on every
    request, and `metadata_cache: {max_families: 0}` is the spelling that turns it off — which is
    why that pairing, where the `ttl` governs nothing at all, is the one case the zero check lets
    through.

56. A `prometheus_out` has exactly one of `bind:` (serve an exposition) and `endpoint:` (write to
    a remote-write receiver) — never both, never neither
    ([ADR `prometheus-remote-write`](../adr/prometheus-remote-write.md)). A **non-default** value
    of a field belonging to the mode that isn't set is rejected rather than silently ignored:
    `path`, `expire_after` or `max_series` alongside `endpoint:`; `version`, `timeout`, `headers`
    or `endpoint_tls` alongside `bind:`. That's rule 45's and rule 53's shape and it exists for
    their reason — a setting that quietly does nothing is worse than a startup failure naming it
    — and like rule 45 it compares against `logit_config`'s own default functions rather than
    mirroring their values, so the default stays legal in both modes. Rule 41's `path`/`max_series`
    checks are registry-mode-only for the same reason: two rules, one gate each, rather than rule
    41 silently acquiring a second job. In sender mode the remaining checks are rule 40's, restated
    against this kind's fields: `endpoint` must be an absolute `http://`/`https://` URL, path
    included (a remote-write receiver's write path — typically `/api/v1/write` — lives there, not
    in `path:`); `timeout: 0s` is rejected, the same "0 is impossible" reasoning as rule 9's
    `interval`; `headers` may not be empty-named, `:`-prefixed (an HTTP/2 pseudo-header), collide
    with another entry once case is ignored, or name one this output sets itself — `content-type`,
    `content-encoding`, `content-length`, `x-prometheus-remote-write-version`, `user-agent`, the
    five in `graph::RESERVED_REMOTE_WRITE_HEADERS`; the `endpoint_tls:` block must be internally
    consistent (`cert_file`/`key_file` together, no `insecure_skip_verify` alongside `ca_file` —
    rules 24/34/44/52's two checks, since this is a sink's own TLS block); and a non-default
    `endpoint_tls:` under a plain `http://` endpoint is rejected — a *scheme* check, exactly rule
    40's third TLS check, since TLS is selected by the endpoint's own scheme and a block under
    `http://` could only ever be ignored.
57. A datagram listener's `receive.read_batch` above `1024` is rejected
    ([ADR `udp-intake-batching-and-socket-visibility`](../adr/udp-intake-batching-and-socket-visibility.md)).
    `read_batch` is `recvmmsg(2)`'s `vlen`, and `1024` is `UIO_MAXIOV`, the kernel's own hard
    ceiling on how many `iovec`s any one vectored I/O call may carry — above it the kernel clamps
    or refuses depending on call path, which is a runtime surprise whose cause is nowhere near the
    config that set it. Rule 18 owns the `0` end, the same split those two rules already have for
    `max_datagrams`/`max_bytes`. A `read_batch` *larger than* `max_datagrams` is deliberately
    **legal**: `push_many` has a defined answer for a batch bigger than the whole queue (evict or
    block per policy, per item, exactly as a sequence of single `push` calls would have), so a rule
    against it would only refuse a configuration that works. Datagram listeners only — rule 17 has
    already rejected a non-default `read_batch` on every other kind, so anything reaching this
    check carries the default and cannot fail it.
58. A `shape`'s `max_tracked_keys` or `max_tracked_keysets` of `0` is rejected
    ([ADR `shape-observer-component`](../adr/shape-observer-component.md)) — rules 9/15/18/28/45's
    impossible-bound shape again. A cap of `0` tracks nothing at all, so `logit.shape.distinct_keys`
    and `.distinct_keysets` would read `0` and `logit.shape.tracking_overflow` `1` forever, from a
    component that looks configured. There is deliberately no "turn the table off" spelling: the
    cumulative gauges are half of what `shape` is for, so an operator who doesn't want them removes
    the component. `shape`'s `interval` needs no rule of its own — it joins `aggregate`'s and
    `internal`'s in rule 9's `interval()` table.
59. A `flatten` with `attributes: none` and `resource: none` together is rejected — rules
    7/12/54's "can only ever be a no-op" instinct again. An empty named list on either field is
    rejected, naming both keywords: write `none` to mean nothing, `all` to mean every nested
    attribute. An empty field name within a named list is rejected — rules 19/20/54's reasoning;
    a field name repeated within one named list is rejected too, the same no-op reasoning once
    more. `attributes: all` (the default) and a field name containing `.` are both deliberately
    legal — the former is the useful default, the latter names a literal attribute exactly as
    rule 54's `keep_values` fields already may
    ([ADR `flatten-transform`](../adr/flatten-transform.md)).

**Deliberately not validated:** that a `by: {provenance: ..}` route key names a component in *this*
graph — rule 37's reasoning; the key is as likely to name a component relayed from another process.

**Sink reachability from a listener needs no separate rule.** It's implied by 2 + 5 + 7: every
acyclic chain of ≥1-source components terminates somewhere, and every non-terminal component in that
chain is required (by 7) to have a consumer, so the chain can only terminate at a sink.

**What disappears:** "input/output claimed by more than one pipeline is not yet supported" — today's
`validate_semantics` rejects this outright because the runtime had no way to express sharing. Under
the graph model it's simply a component with more than one entry in another component's `sources`;
no special-casing needed, and no restriction to state.

## Runtime model

- Each component is a node: one inbox (`mpsc::Receiver<EventBatch>`, capacity `CHANNEL_CAPACITY`,
  unchanged from today) and a `Fanout` — one `mpsc::Sender` per consumer, resolved from the inverted
  `sources` relation.
- **Fan-in is free**: N sources into one component is N cloned `Sender`s feeding the same inbox. A
  `target` several routers direct at is fan-in at the target by exactly the same mechanism — each
  router holds a clone of that target's senders ([ADR `target-components`](../adr/target-components.md)).
- **Fan-out costs a clone per extra consumer**: exactly the `output_txs.split_last()` pattern
  `send_batch` already uses (`crates/logit-cli/src/pipeline.rs`), generalized from "per output" to
  "per downstream consumer of any node."
- **Bind before spawning anything.** Before the first channel exists, let alone the first task,
  the runtime walks every component in **sorted id order** and calls `Input::bind` on each listener
  and `Output::bind` on each sink, marking each `NodeState::Bound`
  (`crates/logit-pipeline/src/readiness.rs`). Both default to a no-op, so only a component that
  actually opens something overrides one; a failure is a *startup* failure naming that component
  (exit code 1) with nothing else running yet, rather than the first `JoinSet` error once every
  sibling is already live. Sinks are in this pass because a sink can listen too — `prometheus_out`
  serves an exposition endpoint, so an address already in use is its startup failure mode, not a
  delivery one for `write_loop`'s retry to absorb
  (`docs/adr/prometheus-scrape-and-exposition.md`'s "`Output::bind`"). Sorted, sequential order is
  what makes "which one failed" reproducible instead of a race between binds.
- **A `target` is not a node.** It gets no task, no inbox, and no channel at all — a listener's
  inbox exists but is dropped unread (nothing can name a listener as a source), while a target's is
  never created, one step stronger: a target declares no `sources:` and nothing may name *it* as a
  source either, so there is nothing to create. What a target actually is at runtime is **one
  `Fanout`**, built in a pass *before* the spawn loop (ids are sorted, so a router can precede its
  own targets), wired to that target's consumers' inboxes and carrying the target's own id
  (`with_component`) and telemetry handle (`with_telemetry`). Each of its routers gets a clone, and
  its readiness state is `NodeState::Alias` for the life of the run. That map of target `Fanout`s is
  **dropped alongside the construction-only `senders` map**: a live clone left behind would be an
  extra outstanding `Sender` on every one of that target's consumers' channels, so the shutdown
  cascade below could never reach past the target — a hang, not a failed assertion, which is why
  `a_router_exiting_closes_its_targets_consumers_inboxes` pins it under a timeout.
  ([ADR `target-components`](../adr/target-components.md)).
- **A router is an ordinary node with one extra edge set.** It owns its own `Fanout` (slot 0, the
  unrouted/forward edge) plus a slot-ordered `Vec<Fanout>`, one per `graph::targets_of` entry. Per
  incoming batch it routes every event *borrowing* it, counts per destination, `reserve_exact`s,
  moves each event into its destination's buffer (`route_batch`), and sends **one batch per
  non-empty destination under one child `BatchContext` and one span** — one incoming batch is one
  hop however many ways it forks, exactly the rule an ordinary fan-out already follows. A router
  with targets and no ordinary consumers is legal; its forward partition is dropped and counted
  `logit.component.events.dropped{reason="unrouted"}`, never silently.
- **Build in reverse topological order** — from sinks back toward listeners — so every node's
  outbound `Fanout` is fully wired (every consumer's inbox already exists) before that node can
  start producing. This generalizes what `run_config` already does today (build outputs, then the
  transform-worker thread, then inputs).
- Shutdown cascades by channel closure, propagating from listeners toward sinks in topological
  order — the same "closed inbox → drain and exit" shape `run_pipeline_worker` already implements
  for one chain, now per node.

### Thread model: only Lua needs its own OS thread

`mlua::Lua` is `!Send`/`!Sync` (`docs/design/lua-api.md`'s concurrency section; `AGENTS.md` lists
this as non-optional) — a Lua node cannot be *moved* into an async task at all, so it needs a
dedicated `std::thread`, same as today. What changes is the granularity: today one thread runs an
entire pipeline's transform chain serially, because chain adjacency was guaranteed by
`PipelineConfig.transforms`. In the graph model, adjacency isn't guaranteed — a Lua component's
sources and consumers can be arbitrary other components — so **each Lua component gets its own
thread**, communicating with its neighbors over the same `mpsc` channels every other node uses.
The thread's exit (a normal return once its inbox closes, or a panic caught at the top of the
thread) is reported over a oneshot that a small `JoinSet` task awaits on the node's behalf
(`runtime.rs`'s `watch_lua_thread`), so readiness, the failure-triggered drain and the exit code
treat a Lua node exactly as they treat any task.

Everything else — listeners, sinks, native `Send` transforms (`aggregate` today via
`logit-transforms::Aggregator`; `json`/`filter`/etc. as they land in the same crate), and **native
`Router`s** (`route`, [ADR `target-components`](../adr/target-components.md); a `Router` is `Send`
for the same reason a `Transform` is, and `run_router` is `run_transform` minus the flush-deadline
race) — runs as an ordinary tokio task, no dedicated thread required. A `target` runs as nothing at
all — see the runtime model above. A **Lua router** (a `lua`/`lua_file` component with `targets:`)
is not a further exception: it is the same one OS thread it would be without them, partitioning
each batch by the `event:to(..)` mark its script set and sending one batch per destination from
that thread. This is a strict generalization of today's split
(input/output tasks vs. one worker thread per pipeline), not a new idea — it just now applies per
node instead of per pipeline.

**Fusing a linear run of adjacent Lua nodes back onto one thread** (avoiding a thread and a channel
hop per hop) is a real, identifiable future optimization once thread count in practice warrants it —
explicitly not v1. Thread count is bounded by config size (one component, one thread at most), which
is enough to start from.

### Flush ticks become per-node, not per-pipeline

Today's `run_pipeline_worker` owns one `Vec<Option<Instant>>` deadline schedule across a whole
chain's stages (`next_flush`, `advance_flush_deadline`, `flush_due_stages` in
`crates/logit-cli/src/pipeline.rs`) because every stage in a pipeline shares one thread and one
receive loop. In the graph model each flush-bearing node (an `aggregate` component, or a `lua`/
`lua_file` component with `interval` set) owns its *own* single-entry version of that same
schedule — one deadline, advanced by the same constant-time `advance_flush_deadline` logic, raced
against its own inbox receive via the same `tokio::time::timeout`-around-`recv` pattern already
proven out today. No shared cross-node schedule is needed, and no change to the deadline-advancement
math itself — it was already correct per-stage, just iterated over a `Vec` that no longer needs to
exist.

A flushed event runs through that node's own `Fanout`, exactly like a normally-processed batch —
`flush_stage`'s "flushed output isn't exempt from downstream processing" property
(`docs/adr/aggregation-window-semantics.md`) holds automatically here, because downstream
processing is just "send to the node's consumers," the same path every event takes.

### Node kinds and the transform trait question

`Input`/`Output` (`logit-inputs`/`logit-outputs`) are already traits; native transforms
(`logit-transforms::Aggregator`) are not — today's `Stage` enum in `pipeline.rs` dispatches on a
closed, hand-written set because the whole chain lives on one thread with no `Send`/object-safety
pressure. The graph model's per-kind dispatch (arity, thread-vs-task, flush-or-not) is a strong
signal to give native transforms the same trait treatment `Input`/`Output` already have — a
`Transform` trait in `logit-pipeline` that `logit-transforms::Aggregator` and future native
transforms implement, letting the node runtime hold `Box<dyn Transform + Send>` next to
`Box<dyn Input + Send>`/`Box<dyn Output + Send>` rather than growing a parallel hand-written enum
per node kind. Lua nodes stay the one hand-special-cased kind, for the `!Send` reason above — a
trait object doesn't fix that, and shouldn't try to.

`Transform` later grew a second per-batch hook alongside `process`: `map_resource`, called once per
incoming batch before any event reaches `process`, letting a transform substitute the batch's
resource (`logit-transforms::Set` is the first implementer) — see
[ADR `operator-declared-resource-attributes`](../adr/operator-declared-resource-attributes.md).

### Trace context propagation

Every `Delivered` (the channel payload one `Fanout` edge carries, `crates/logit-pipeline/src/fanout.rs`)
carries a `TraceContext { trace_id: [u8; 16], span_id: [u8; 8] }` — the substrate for internal
spans, decided and built per [ADR `trace-context-propagation-on-delivered`](../adr/trace-context-propagation-on-delivered.md) on
the measured evidence [ADR `minimize-allocations-over-event-size`](../adr/minimize-allocations-over-event-size.md) required.
[ADR `internal-span-emission-and-deterministic-sampling`](../adr/internal-span-emission-and-deterministic-sampling.md) is what actually turns
this plumbing into a `SpanRecord`-carrying `Event` — see `docs/design/internal-telemetry.md`'s
"Spans" section for the emit API, the sampler, and the bound.

**Which node kinds propagate a real parent, and which mint a fresh root, is not uniform — it's
exactly the 1-to-1-vs-*n*-to-1 distinction the rest of this doc already draws between a node's
per-batch processing and its flush:**

| Node kind | Context of what it emits | Span recorded |
|---|---|---|
| A listener's own batches | Always a fresh root — `Input::run` never receives a `Delivered` (arity rules out a `sources` entry pointing at one), so there is no parent to inherit. | `SpanKind::Producer`, in `Fanout::send`/`send_blocking` |
| `Transform::process`/`ScriptWorker::process` (the non-flush path) | A [`TraceContext::child`] of the one incoming batch that produced it — 1-to-1, unambiguous. | `SpanKind::Internal`, in `run_transform`/`run_lua` |
| `Transform::flush`/Lua's timer-driven `flush()` | A fresh root, deliberately — an *n*-to-1 relationship (however many batches were absorbed since the last tick), with no single correct parent to propagate. Tracked as an open gap, not silently approximated; see ADR `trace-context-propagation-on-delivered`'s "What this doesn't do." One root now covers *every* resource group a flush emits, not one root per group (ADR `internal-span-emission-and-deterministic-sampling`). | `SpanKind::Internal`, in `run_flush`/`run_lua`'s `flush_now` |
| `run_output` | Already borrows the incoming `Delivered` without unwrapping (`Output::send(&EventBatch)`, [ADR `arc-eventbatch-copy-on-write`](../adr/arc-eventbatch-copy-on-write.md)), so the context is there to read. Nothing further downstream to propagate *to* — the sink span mints `ctx.child()` as its own identity and then discards it (ADR `internal-span-emission-and-deterministic-sampling`). | `SpanKind::Client`, in `write_loop` |

Mechanically: `Fanout::send`/`send_blocking` mint a root, open the listener's own span, and
delegate to `Fanout::send_with_own_context` (new, ADR `internal-span-emission-and-deterministic-sampling`) — the *only* remaining caller of
`send`/`send_blocking`, now that a flush-driven emission (which used to call `send` directly) also
mints its own root and calls `send_with_own_context` instead, so it can record its own span around
the same context. `Fanout::send_with_context`/`send_blocking_with_context` (mint a child of a given
parent, no span of their own) are defined in terms of `send_with_own_context` too — additive
methods, no existing signature changed. `Delivered::context()` is a cheap `&self` accessor — read
it *before* `unwrap_batch` consumes the batch, since `unwrap_batch` itself still discards the
context (changing its return type to include one would force every existing caller, most of which
don't propagate anything, to thread a value through unused). `SinkQueue`'s entries carry the
context too now (`push`/`peek`, ADR `internal-span-emission-and-deterministic-sampling`) — the last place it was still being discarded, on the
one path (`drain_inbox` → `write_loop`) that needs it to parent the sink's own span.

A fan-out (one batch, several downstream branches) gives every branch the *identical* child
context — one emission forking into several consumers is still one hop, not several, and (per ADR
0022) records exactly one span for it, not one per branch.

### Provenance propagation

Alongside `TraceContext`, every `Delivered`'s second element (`BatchContext`) also carries a
`Provenance { origin: Option<Symbol>, previous: Option<Symbol> }` — which component created a
batch, and which component the current node received it from — decided in
[ADR `batch-provenance-on-delivered`](../adr/batch-provenance-on-delivered.md). Readable by every
component (a native `Transform::observe_provenance`, an `Output::observe_batch`, a Lua script's
`provenance` global) and writable by none: a component never constructs a `Delivered`, so it has
no way to forge or drop what it carries.

**One stamping rule, applied uniformly by `Fanout` (the sole writer), covers every node kind with
no special-casing** — a deliberate contrast with the trace-context table above, which genuinely
does need one row per node kind:

```
origin   = origin.or(Some(self.component))   // set once, on this Fanout's first send
previous = Some(self.component)              // rewritten on every send
```

- A listener's first send has empty incoming provenance, so it sets *both* fields — the listener
  is its own origin and previous. `internal` needs no special case: it's a listener by role.
- Every later hop finds `origin` already set and only rewrites `previous`.
- A flush emission (`Transform::flush`/Lua's `flush()`) mints a fresh `BatchContext` with empty
  provenance, the same way it mints a fresh `TraceContext` root — so the flushing component
  becomes *both* `origin` and `previous`, the honest statement for a batch built from accumulated
  state rather than a re-emission of anything that passed through unchanged.
- A real fan-out gives every branch the identical provenance, same as it does for trace context.
- **`previous` downstream of a `target` is the target's id, never the router's** — and `origin` is
  untouched. No special case makes this true: a target's one `Fanout` is built
  `with_component(<target id>)` like every other node's, so the rule above applies unchanged and a
  `has_provenance{previous: [host_stream]}` reads naturally
  ([ADR `target-components`](../adr/target-components.md)). A router's own forward edge stamps the
  *router's* id, as any other node would. Which router fed a target is deliberately not recoverable
  from provenance; if that ever matters it is a router-side metric, not a provenance change.

`logit_in`'s relay (`Fanout::send_relayed`) uses a different rule, `stamp_relayed`: it back-fills
(`or`, not overwrite) only whatever the wire didn't carry, so a v2 `logit_out` peer's own
`origin`/`previous` survive the hop completely untouched — the property the `logit_out -> logit_in`
special case exists for, letting a split-collection deployment read as one graph. See the ADR for
the wire-format decision (`CODEC_NATIVE_V2`) this relies on.

## Backpressure: diamonds are the normal shape now

With a chain of ordinary transform components as the only branching mechanism (ADR
`component-graph-configuration` — one component drops what a branch doesn't want, a sibling drops
the rest, downstream components choose a branch by naming it as a source; today that's a `lua`
component per branch, per `docs/adr/routing-by-condition-is-lua.md`), a config where one listener
feeds several such branches that reconverge on shared sinks isn't a rare topology — it's the
*expected* way to express "route by condition." Two consequences worth stating rather than
discovering in production:

- **Backpressure crosses branches.** A stalled sink backs up through every branch sharing an
  upstream with it, not just its own path — this is correct bounded-channel behavior, but it means
  one slow destination can head-of-line-block telemetry destined for an unrelated, healthy one.
- **Fan-out used to pay a flat clone cost; it now depends on shape.** Originally: every extra
  consumer of a node cloned the outgoing `EventBatch` — a deep `Vec<Event>` clone, incurred
  unconditionally wherever a filter fanned out. A routing primitive would have avoided this by
  construction; having ruled that out (ADR `component-graph-configuration`), the clone was load-bearing, not incidental — and
  it's also what makes branch isolation free: two branches of a fan-out never share the same
  `Event` value, so a mutation on one is structurally invisible to the other, with nothing extra to
  design or maintain for that guarantee (see [ADR `multi-payload-events`](../adr/multi-payload-events.md)'s
  branch-isolation note, proven by `crates/logit-pipeline/src/runtime.rs`'s
  `a_mutation_on_one_fan_out_branch_is_invisible_to_the_sibling_branch` — a test three rounds of
  the fix below never touched, only its doc comment).

  **`Arc<EventBatch>` copy-on-write landed** (`docs/adr/arc-eventbatch-copy-on-write.md`),
  after three rounds of measurement correcting an increasingly specific overclaim each time — worth
  reading end to end for that alone. The settled, shape-dependent result: a single-consumer edge
  (most edges in the shipped config) and an all-`Output` fan-out are both now unconditionally free
  or near-free. A fan-out mixing an `Output` branch with a mutating branch is genuinely racy — 1 or
  6 allocations, decided by real scheduling, never a fixed number. A fan-out with no `Output`
  branch at all still pays the full clone (6, one allocation worse than the pre-`Arc` code), with
  no path to improvement under the current design. "Load-bearing" was right, but there is no single
  number for "the fan-out cost" any more — see `docs/design/memory.md` §3 for the complete,
  shape-by-shape account.
- **A router + targets split (ADR `target-components`) is the cheap form of this diamond** — one
  partition pass and no clone, against the branches-share-a-clone accounting just above — but it
  doesn't change the backpressure story: a stalled consumer of one target still backs up through
  its router into every other target's flow, the same head-of-line blocking the first bullet
  describes, just paid by a router instead of a filter chain.

Also worth carrying forward as an open question, not a decision: today's `send_batch` silently drops
a send on a closed downstream (`let _ = tx.blocking_send(...)`). Under a DAG that closure should
really propagate as a shutdown signal rather than vanish. A per-edge `on_full: block | drop` policy
is a plausible future answer; out of scope for the initial graph implementation.

**Sink-side buffering decouples a sink's own inbox from its delivery**
(`docs/adr/buffered-sink-delivery.md`). `run_output` used to await `Output::send` inline, so a
slow or backing-off sink stopped draining its own inbox for as long as delivery took — backpressure
from that sink reached its upstream almost immediately. It now splits into a drain half that moves
batches off the inbox into a `SinkQueue` and a writer half that delivers from that queue
independently (`crates/logit-pipeline/src/queue.rs`), so a slow sink no longer stalls its own
inbox just because delivery is slow. Backpressure doesn't disappear — a `SinkQueue` under `Block`
still applies it once the queue itself fills — it just surfaces later and deeper than the inbox's
`CHANNEL_CAPACITY=64`, and it's now visible ahead of time via
`logit.component.buffer.utilization` rather than only as a stalled inbox.

**Listener-side receive decoupling does the same thing one hop earlier**
(`docs/adr/decoupled-listener-io.md`). A UDP listener's `recv_from`, decode, and
`Fanout::send` used to share one loop, so downstream backpressure stopped the socket being read and
the kernel dropped datagrams silently and uncounted. `logit-inputs::udp::UdpListener` splits into a
read half that moves datagrams off the socket into a `ReceiveQueue` (`BoundedQueue<Datagram>`, the
same generalized type `SinkQueue` is an instance of, `crates/logit-pipeline/src/queue.rs`) and a
decode half that pops, decodes, accumulates, and sends independently. Unlike `SinkQueue`, the
receive queue defaults to `drop_oldest`, not `Block` — a UDP reader's producer is the kernel socket
buffer, which cannot be asked to wait, so blocking here would relocate loss into the kernel instead
of preventing it. See that ADR for the field research behind the default.

## `logit graph`: visualizing the resolved DAG

`logit graph <config>` prints the resolved component graph as graphviz DOT to stdout — the natural
answer to "what does this config actually do," which gets harder to eyeball by reading YAML once
config is a graph rather than a list of linear pipelines.

- Renders unconditionally, straight off the raw `Config` rather than a resolved `Graph` — it needs
  only that a `sources` id can be written as an edge target, which is true even when that id names
  no defined component: graphviz auto-creates a bare node for an edge whose target was never
  otherwise declared, so an unresolved source (rule 2) still renders as a visibly dangling edge
  rather than blocking output. (An earlier version of this section reasoned "an edge to an
  undefined component can't be drawn at all" and required rule 2 to pass first — that premise was
  simply wrong once actually tried; corrected here rather than left as a stated constraint the
  implementation quietly didn't follow.)
- Runs the full validation (every rule) after rendering and reports any failures to stderr with a
  non-zero exit — without suppressing the DOT output. This is deliberate: `graph` is most useful on
  exactly the configs that fail validation, since a cycle — or a typo'd source, now visibly
  dangling — is far easier to see rendered than to parse out of an error message naming two
  component ids.
- Styles nodes by role (listener / transform / sink) so the shape of the data flow — where it
  enters, where it forks, where it lands — reads at a glance without cross-referencing the arity
  table.
- Renders a `target` as a dashed box, and every router → target edge dashed too, labelled with the
  `routes:` key that directs an event down it (a `lua`/`lua_file` `targets:` entry carries no
  label — its destination is chosen in the script, by `event:to("..")`). These edges come from
  `graph::target_edges`, which reads the raw `Config` like everything else here, so a router whose
  target id resolves to nothing still renders as a visibly dangling dashed edge rather than
  blocking output ([ADR `target-components`](../adr/target-components.md)).
- Still needs every `!env` reference in the config to resolve, though ("Environment substitution"
  above) — a missing variable fails to load before `render` is ever called, same as `run`/
  `validate`, even for a field this command never reads.

Lives in `logit-cli` as a `Command::Graph` arm alongside `Schema`/`Validate`/`Run`
(`crates/logit-cli/src/main.rs`); stays synchronous like `Schema`/`Validate` — it only needs the
resolved graph structure, no I/O, no tokio runtime.

## Crate layout

The obvious arrangement is circular: a pipeline runtime needs to build inputs/outputs/transforms,
but `Input::run`/`Output::send`/a `Transform` implementation need the `Fanout` type the runtime
defines. Invert it — trait definitions and the runtime move into a new crate that the impl crates
don't depend on:

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
`RouteBy` type (`docs/adr/target-components.md`) — an impl crate reading a config type it needs is
unremarkable; only `logit-pipeline` itself is barred from depending on the impl crates.

`logit-pipeline`:
- Moves `Input` and `Output` out of `logit-inputs`/`logit-outputs` (which then hold only impls), and
  adds the `Transform` trait discussed above.
- Owns `Fanout` and the graph resolution/validation module (pure, no channels or threads — see
  below) and the node runtime.
- Depends on `logit-core` (for `EventBatch`), `logit-config` (for `ComponentKind`), and `logit-proto`
  (for `Buffer`/`InMemoryBuffer`, which `SinkQueue` wraps — `docs/adr/buffered-sink-delivery.md`),
  but *not* on `logit-inputs`/`logit-transforms`/`logit-outputs` — those depend on it instead, for
  the trait definitions. `logit-cli` is the one place that depends on everything and holds the
  kind-to-trait-object registry (today's `build_input`/`build_output`, generalized).
- Keeps the channel type out of `logit-core`, whose doc comment states "no I/O, no pipeline" —
  weakening that would blur a boundary the crate exists to hold.
- Owns `BoundedQueue<T: Queued>` and `BatchAccumulator` (`docs/adr/decoupled-listener-io.md`)
  alongside `Fanout` and the node runtime — both are transport-agnostic (nothing in either type
  mentions a socket or a decoder). The UDP socket bind, `SO_RCVBUF` setsockopt, and `recv_from` loop
  that *uses* them (`logit-inputs::udp::UdpListener`) stay in `logit-inputs`, following the same
  "traits and generic machinery here, concrete protocol impls there" split the crate already
  applies everywhere else.
- Owns `sockstat` (`docs/adr/udp-intake-batching-and-socket-visibility.md`) on the same line: a
  `getsockopt`-level *reading* of a file descriptor the caller already holds — the kernel's
  per-socket drop counter and receive-buffer fill (`SO_MEMINFO`), a listening socket's accept-queue
  depth (`TCP_INFO`) — with no notion of a listener, a datagram or a node. It is here rather than in
  `logit-inputs` because `logit-outputs` is the foreseeable second consumer and must not depend on
  an input crate, and rather than in `logit-core` because that crate's "no I/O" boundary (two
  bullets up) is exactly what a raw syscall and a `libc` dependency would have broken; this crate
  already does real I/O (`disk_queue.rs`) without claiming otherwise. Everything that *operates* a
  socket — binding it, sizing it, reading from it — still stays in `logit-inputs`.

`graph.rs` (resolution + the validation rules + topo-sort) is a **pure function over
`Config`** — no channels, no threads, no tokio — mirroring how `apply_transforms` in today's
`pipeline.rs` was deliberately kept pure specifically so it's unit-testable without spinning up
real I/O. `logit run`, `logit validate`, and `logit graph` are three different things layered on
top of the same pure resolution: run executes it, validate checks it and stops, graph renders it.

## `flush()` needs no new design

Restating for confirmation, not introducing anything: a stateful component (`aggregate`, or a Lua
component with `interval` set) is a node whose loop races its inbox receive against its own flush
deadline (previous section), emitting into its `Fanout` on that schedule independent of inbound
traffic. `docs/design/lua-api.md`'s `flush()` contract and
`docs/adr/aggregation-window-semantics.md`'s windowing semantics both apply unchanged — the
graph model changes *where* the flush timer lives (per-node instead of per-pipeline-chain), not what
it does.
