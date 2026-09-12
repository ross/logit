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
    // Matches a pattern against a log message (or a named attribute), turning named capture
    // groups into attributes (docs/adr/regex-transform.md).
    Regex { pattern: String, field: Option<String> },
    // Splits each row of a delimited line into positional attributes named by a configured
    // `columns` list (docs/adr/csv-positional-columns.md).
    Csv { columns: Vec<String>, delimiter: char },
    // as each lands in logit-transforms, same shape: a `ComponentKind` variant, no `sources`
    // opinion of its own (that lives on `Component`, uniformly). `rename`/`filter`/`sample`/
    // `throttle`/`dedup` used to be sketched here too -- retired before landing, not merely
    // deferred: each is already expressible as a `lua` component, and
    // `docs/adr/routing-by-condition-is-lua.md` records why a native equivalent wasn't worth
    // building yet.

    InfluxDbOut { url: String, org: String, bucket: String, token: String },
    OtlpOut { endpoint: String },
    LogitOut { endpoint: String },
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
and any future native transform — take no suffix; there's only ever one direction for a transform
to be.

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
| Listener (`statsd_in`, `syslog_in`, `otlp_in`, `tail_in`, `docker_in`, `logit_in`, `prometheus_in`) | must be empty | required (≥1 consumer) |
| Transform (`lua`, `lua_file`, `aggregate`, `json`, `csv`, `kv_metrics`, `keep`, `remove`, `set`, `trace_context`, `scale`, `has_signal`, `keep_signals`, `drop_signals`, `has_attributes`, `drop_attributes`, `has_provenance`, `drop_provenance`, `logfmt`, `kv`, `regex`) | ≥1 required | required (≥1 consumer) |
| Sink (`influxdb_out`, `stdio_out`, `file_out`, `otlp_out`, `syslog_out`, `logit_out`, `statsd_out`, `prometheus_out`) | ≥1 required | must not be |

Deriving role from topology instead ("no sources → listener", "nothing reads it → sink") was
considered and rejected (ADR `component-graph-configuration`): a typo'd source reference would silently turn a real sink into
an orphaned transform, with no error, rather than a clear "did you mean" failure. The kind already
knows its own arity — config just states the edges.

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
    (`docs/adr/decoupled-listener-io.md`, `statsd_in`/`syslog_in`) or a **tail listener**
    (`docs/adr/file-tailing-and-docker-json-logs.md`, `tail_in`/`docker_in`). Deliberately not
    "any non-listener": `internal` is a listener by role but has no socket, no queue, and no
    decoder, so `receive:` on it would be exactly the silently-ignored-setting failure rule 14
    guards against on the sink side. A tail listener has no receive *queue* at all (the tailed
    file is its own durable buffer) — only `receive.batch_max_events`, `batch_max_bytes`,
    `batch_flush_interval`, and `shutdown_grace` are meaningful on one; a queue-bounding field
    (`max_datagrams`, `max_bytes`, `overflow`, `receive_buffer_bytes`) is rejected by name.
18. A datagram listener's `receive.max_datagrams`, `receive.max_bytes`, or `receive.batch_max_events`
    of `0` is rejected — the twin of rule 15. `receive.batch_flush_interval: 0s` is **not**
    rejected — it means "no flush timer," a meaningful setting, unlike the count bounds. A tail
    listener's `receive.batch_max_events`/`batch_max_bytes` of `0` is rejected the same way (the
    queue-only bounds don't apply to it at all — see rule 17).
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
38. A `statsd_out` `max_packet_bytes: 0` is rejected, the same shape as rule 15's
    `buffer.max_batches`/`max_bytes: 0` — an impossible bound (every metric line would overflow it
    and be dropped whole), not a small one (`docs/adr/statsd-output.md`).
39. An `aggregate` with `temporality: cumulative` requires `series_retention >= 1` (a count of
    windows, not a duration) and `max_retained_series >= 1` — those two bounds are what keeps a running total alive
    across the window boundary, so with either at `0` no accumulator survives a flush and every
    window would emit its own increment labelled as a cumulative total, a silently wrong number for
    the consumer that mode exists for. `series_retention: 0` stays legal under the default
    `temporality: delta`, where it is the documented opt-out from gauge retention
    (`docs/adr/aggregation-window-semantics.md`'s cumulative amendment).
40. A `prometheus_in` `targets` must name at least one absolute `http://`/`https://` scrape URL with
    a non-empty authority — `logit-pipeline` doesn't depend on `reqwest`/`url` (this document's own
    "Crate layout" section), so this is a small hand-rolled scheme/authority check, not a full URL
    parse. `timeout: 0s` is rejected, the same "0 is impossible" reasoning as rule 9's `interval`
    (which also covers `prometheus_in`'s own `interval: 0s`, via the same generic check). A `tls:`
    block must be internally consistent — `cert_file`/`key_file` set together, no
    `insecure_skip_verify` alongside `ca_file` — the same two checks rule 24 makes for `otlp_out`'s
    own `tls:` block (and rule 34 for `logit_out`'s) — and is rejected outright unless at least one
    target is `https://` — TLS is selected per-target by its own scheme, so a `tls:` block with
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
- **Fan-in is free**: N sources into one component is N cloned `Sender`s feeding the same inbox.
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

Everything else — listeners, sinks, and native `Send` transforms (`aggregate` today via
`logit-transforms::Aggregator`; `json`/`filter`/etc. as they land in the same crate) — runs as an
ordinary tokio task, no dedicated thread required. This is a strict generalization of today's split
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
- Runs the full validation (all eighteen rules) after rendering and reports any failures to stderr with a
  non-zero exit — without suppressing the DOT output. This is deliberate: `graph` is most useful on
  exactly the configs that fail validation, since a cycle — or a typo'd source, now visibly
  dangling — is far easier to see rendered than to parse out of an error message naming two
  component ids.
- Styles nodes by role (listener / transform / sink) so the shape of the data flow — where it
  enters, where it forks, where it lands — reads at a glance without cross-referencing the arity
  table.
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

`graph.rs` (resolution + the eighteen validation rules + topo-sort) is a **pure function over
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
