# Known gaps: Pipeline runtime, event model, config, and admin

Entry format and the other areas: [the known-gaps index](README.md).

## Pipeline runtime and graph

- **An unconditional fan-out to several mutating branches pays a full `EventBatch` clone**
  ([ADR `arc-eventbatch-copy-on-write`](../adr/arc-eventbatch-copy-on-write.md)):
  - A single-consumer edge (most edges in the shipped config) costs 0 allocations; an all-`Output`
    fan-out costs 1.
  - A fan-out mixing one `Output` branch with one mutating branch costs 1 *or* 4 allocations,
    depending on scheduling.
  - A fan-out with no `Output` branch costs a full clone (4), with no path to improvement under the
    current design.

  No single number says what fan-out costs; [memory.md](../design/memory.md) §3 has the
  shape-by-shape account. **Workaround:** split by destination to avoid the clone. A router plus
  `target` components (ADR [`target-components`](../adr/target-components.md)) costs
  `1 + used destinations` allocations per batch, against 194 for the fan-out-plus-filters shape
  `memory.md` §3 measures for the same split. Any destination split, whether named by an
  attribute, provenance, or resource value or by a Lua script's own decision, has that cheap,
  non-cloning answer.

- **Channel depth is bounded in batches, not bytes or events** (`CHANNEL_CAPACITY`,
  `crates/logit-pipeline/src/runtime.rs`) — 64 batches per edge, whatever each batch weighs, so
  total in-flight memory scales with edge count times batch size.
  - Listeners that assemble batches bound them by config through a `BatchAccumulator` under
    `receive.batch_max_events`/`batch_max_bytes`: a UDP listener
    ([ADR `decoupled-listener-io`](../adr/decoupled-listener-io.md)), each connection on the TCP
    stream driver (`crates/logit-inputs/src/tcp.rs`), and each file `tail_in`/`docker_in` tracks.
  - A listener that receives a whole batch per request or frame (`otlp_in`, `logit_in`) passes on
    what the sender sent.
  - A transform's outbound batch has no bound at all, and nothing in the config says so.

  [memory.md](../design/memory.md) §5 has the account.

- **Every `Output::send` call allocates a boxed future, on every batch, for every sink.** `Output`
  is `#[async_trait]` (`crates/logit-pipeline/src/output.rs`), which desugars `async fn send` into
  a fn returning `Pin<Box<dyn Future<...>>>`: 1 allocation (16 bytes) per call, the same through
  `&mut dyn Output` (the shape `run_output` has) or on a concrete type
  (`crates/logit-bench/tests/allocations.rs`'s
  `send_batch_through_a_noop_output_disabled_telemetry`). It's the hottest schedule on the output
  side: once per batch, every sink, every pipeline. `Input::run` is called once per process, and
  `Transform`/`ScriptWorker` aren't `#[async_trait]`.

  A hand-written method returning `Pin<Box<dyn Future<...>>>` wouldn't fix it: the box is how a
  `dyn Trait` object returns a future of unknown, implementer-varying size, whoever writes the
  method. A fix gives up `dyn Output` for this call, through either:
  - enum dispatch over the small, closed set of shipped `Output` kinds
    (`StreamOutput`/`InfluxDbOutput`/...), so each variant's `async fn` compiles to its own unboxed
    future; or
  - a runtime generic per node over a concrete `Output` type, which loses the config-driven dynamic
    construction (`Box<dyn Output + Send>` built from a running config,
    `crates/logit-cli/src/pipeline.rs`) the pipeline relies on.

  Either is real work. **Revisit** when the output path's allocation cost becomes worth chasing.

- **A datagram in flight when SIGTERM/SIGINT lands is lost**
  ([ADR `service-lifecycle-and-output-retry`](../adr/service-lifecycle-and-output-retry.md)).
  Cancelling a listener's `run` future drops whatever it was mid-`recv_from`/decode on. The loss
  is accepted, because UDP is lossy by contract already. A UDP listener counts those datagrams as
  `datagrams.dropped{reason="shutdown"}`; the events it had already decoded are the uncounted
  remainder under [UDP intake](intake.md#udp-intake).

  The rest of shutdown is protected. A signal handler closes every listener's inbox normally
  (`logit_pipeline::run_with_shutdown`, `crates/logit-pipeline/src/runtime.rs`), triggering the
  same close-time flush a listener's natural completion has, so the aggregation window survives,
  and each sink's `Output::flush` runs once `write_loop` stops delivering
  ([ADR `buffered-sink-delivery`](../adr/buffered-sink-delivery.md)).
- **A batch parked in `Fanout::send` when a listener's future is dropped is lost, already counted
  `sent`.** `Fanout::deliver` counts `batches.sent`/`events.sent` before it awaits the first
  consumer's channel. A send dropped while a full downstream parks it reaches no consumer, or only
  a prefix of them, and no counter records the difference. A listener's future is dropped in two
  places:
  - `Input::run_until_shutdown`'s default races `run` against the signal with no grace, so the
    drop comes the instant shutdown fires. That covers `prometheus_in` in scrape mode (the scrape
    in flight, and each target's batch still to send) and `generate_in`. The HTTP listeners'
    accept loops also use the default, but their connections run on spawned tasks the drop
    doesn't reach.
  - `run_input`'s grace backstop drops a listener still draining after its grace: the UDP
    listeners (under [UDP intake](intake.md#udp-intake)), `internal` (under
    [Internal telemetry and self-logging](telemetry.md#internal-telemetry-and-self-logging)), and
    `tail_in`, whose restart replays the batch from the last checkpoint (under
    [File tailing and Docker logs](tailing.md)).

  Both need a downstream that stays full at shutdown. Counting the loss needs `Fanout` to record a
  delivery per consumer, the fix the UDP entry names. `docs/design/pipeline-graph.md`'s
  "Cancellation points" table lists each site.
- **A batch sent into a closed sink inbox after the sweep's bound runs out is lost uncounted.**
  `run_output` closes its inbox before its shutdown sweep, so a later send fails upstream as
  `closed_consumer`, and a listener with an acknowledgement also refuses that batch to its client.
  A producer that reserved its channel permit before the close can still send. The sweep receives
  until `recv` returns `None`, but only for `SWEEP_DRAIN_TIMEOUT` (250 ms). A permit holder still
  blocked on another consumer of a fan-out when that runs out sends into a channel nobody reads.
  The batch dies with the `Receiver`: counted `sent` upstream, and nothing at the sink.
  [ADR `shutdown-accounting-and-cancellation-safety`](../adr/shutdown-accounting-and-cancellation-safety.md)
  names it as an exception in decision 1. **Revisit** if a reconciliation shows a sink's
  `received` short of its producers' `sent` after a shutdown with no `closed_consumer` drops.
- **The `fault` seam's rules on one point don't each see every hit.** `logit_pipeline::fault` (a
  test-only seam) checks a scope's rules on a point in the order they were added, and a rule that
  fails an operation returns before any later rule counts it. So a rule counts only the hits no
  earlier rule on that point failed. `scope.fail_nth(p, 1, E).fail_nth(p, 2, E)` fails the first
  and the third operation at `p`, not the first two: the second rule never sees hit 1, lets hit 2
  through as its first, and fails hit 3 as its second.
  - **Workaround:** to fail the first two, add two `fail_nth(p, 1, E)` rules, as
    `stdio::tests::a_rotation_whose_reopen_fails_counts_once_and_the_retries_count_no_bytes_twice`
    does. Nothing shipped is affected.
  - **Revisit** when a test needs rules on one point to count the same hits, such as `n`th
    operations named by their absolute position whatever earlier rules failed.
- **A test that captures `tracing` output fails under plain `cargo test` beside tests that emit
  diagnostics.** `splunk::tests::a_code_6_out_of_range_is_diagnosed` installs a thread-local
  subscriber with `set_default` and asserts the `request_rejected` report reached it.
  - **Verified:** run as `cargo test -p logit-outputs --lib splunk::tests::`, which runs the
    module's tests on threads of one process, it failed twice in a row with nothing captured. It
    passes run alone and under `cargo nextest run`, which runs each test in its own process, so
    `script/test`, `script/check`, and CI are unaffected.
  - **Suspected, not verified:** `tracing` caches each callsite's interest process-wide. When
    another test's thread reaches the `warn!` inside `Diagnostics::warn_throttled` with no
    subscriber interested, that callsite can be cached as never enabled, and this test's
    thread-local subscriber then never sees the event. The module's attempt-accounting tests
    emit more such diagnostics concurrently, which would make the race more likely.
  - **Revisit:** if a documented script runs `logit-outputs` tests under plain `cargo test`. The
    fix then is a test that doesn't depend on a thread-local subscriber, such as one reading
    `Diagnostics::occurrences`.
- **`logit_in` doesn't record the sending peer's address.** Every other network listener takes
  `peer:` and, on TCP, `proxy_protocol:`: those on the shared socket drivers and the five HTTP
  listeners (`otlp_in`, `datadog_in`, `datadog_trace_in`, `splunk_hec_in`, and `prometheus_in`'s
  remote-write receiver), which also take `forwarded:`
  ([ADR `listener-peer-address`](../adr/listener-peer-address.md)). `logit_in` accepts its
  connections itself, takes none of the three, and stamps no `network.peer.*` or `client.*`.
  - **Consequence:** a pipeline can't tell which of several `logit_out` senders on one `logit_in`
    wrote an event, or route on it, unless the sender's events carry its identity.
  - **Workaround:** stamp each sender's identity upstream of its `logit_out`, with a `set` stage
    writing a resource attribute, or run one `logit_in` per sender (or per group of senders) and
    stamp each with a `set` stage.
  - **Revisit trigger:** work on the native protocol, or a deployment that can't do either
    workaround.

## Event model and interner

- **The attribute/metric-name interner never frees** (`crates/logit-core/src/interner.rs`) —
  `lasso::ThreadedRodeo` has no eviction, so the process retains every distinct string it ever
  interned, at a measured ~94-124 bytes each.

  **Accepted, not planned work.** Re-interning a string the table already holds allocates nothing,
  so a fixed schema reaches steady state and stays flat.
  - **What's interned:** attribute and resource keys, metric names, the symbol-typed metric/log
    fields (`unit`, `description`, `event_name`), and `telemetry`'s tag keys and values. An
    attribute's *value* never is, so the usual cardinality explosion (host, request id, user
    agent, path) never touches it.
  - **What's left:** a real metric name that never repeats, from a user who embedded an id in a
    metric name. That namespace is user-controlled, not attacker-controlled, because `logit`'s
    listeners are private by deployment shape
    ([ADR `deployment-threat-model`](../adr/deployment-threat-model.md)). The anti-pattern is well
    known, and `logit` isn't what breaks first. The metric store fails well before: a million
    distinct measurement names is a million-plus series, against 94 MB here. Even inside `logit`,
    `aggregate`'s window costs ~600 bytes per series *per window* against the interner's ~94
    bytes once, so it fails ~6× harder and sooner, and putting `keep` in front of it already
    mitigates that.

  Growth is observable: `internal`'s `logit.process.interner.strings` gauge
  ([internal-telemetry.md](../design/internal-telemetry.md)) samples `interner::len()` on every
  drain tick. Attach any sink to `internal` to watch it.

  **Revisit trigger — re-check the premise, not the conclusion:** if a listener ever stops being
  private (a public or multi-tenant ingest endpoint, a hosted aggregator). The retrofit is
  expensive: `Symbol` is `Copy` and `resolve` panics on an unknown symbol, so `AttrMap`,
  `MetricRecord`, `SeriesKey`, the Lua proxy, and the native wire dictionary all assume symbols are
  eternal. See [memory.md](../design/memory.md)'s interner section.

  These listeners and transforms feed it unbounded keys:
  - **Bounded:** keys from `logit`'s own config or a fixed protocol grammar (statsd's `#tag:value`).
  - **`syslog.sd` is not bounded.** `syslog.sd`
    (`Value::Map { "<SD-ID>" -> Value::Map { "<PARAM-NAME>" -> ... } }`,
    [ADR `syslog-structured-data-convention`](../adr/syslog-structured-data-convention.md)) interns
    every SD-ID and PARAM-NAME a peer sends, at both map levels; nesting doesn't bound that. RFC
    5424's grammar is the bound: 1 to 32 PRINTUSASCII bytes, excluding `=`, SP, `]`, and `"`. It's
    the same exposure the `json` transform has for an arbitrary JSON object's keys
    ([ADR `json-parsing-into-attributes`](../adr/json-parsing-into-attributes.md)), with a length
    cap `json` doesn't have.
  - **`otlp_in` is the sharpest form.** `crates/logit-proto/src/otlp/common.rs`'s
    `key_values_into_attrs` interns every OTLP `KeyValue.key`, and those keys are arbitrary
    peer-supplied strings with no `logit`-side grammar and no length cap. It's the listener where
    a never-repeating key (the retrofit trigger) could plausibly come from something other than a
    user's own naming mistake. **Mitigation:** [`docs/deploying.md`](../deploying.md) recommends
    `keep` in front of `otlp_in` specifically, beyond the general `aggregate`-cardinality
    recommendation [`fixtures/nginx-to-influxdb.yaml`](../../fixtures/nginx-to-influxdb.yaml)
    demonstrates.
  - **`logit_in`'s native dictionary.** `crates/logit-proto/src/native/dict.rs`'s `Dict::read`
    interns every dictionary string a `logit_in` peer sends before the rest of the batch
    validates, so its strings stay in the interner, even from a batch the decoder then rejects.
    That's not defended, per the threat model in
    [ADR `deployment-threat-model`](../adr/deployment-threat-model.md). Nothing budgets dictionary
    strings across frames. The dictionary entry cap and the frame size bound each frame; the
    process-lifetime bound is the same premise as every other feeder: `logit_in`'s peers are other
    `logit` processes the operator runs.
  - **`flatten` adds no new bound** (`crates/logit-transforms/src/flatten.rs`,
    [ADR `flatten-transform`](../adr/flatten-transform.md)). Its marginal exposure over
    `json`/`syslog_in`/`otlp_in` is twofold:
    - path *combinations* of already-interned keys, a product bounded by the fixed internal
      recursion-depth wall rather than a key-count cap;
    - array indices, a new key axis no other component mints: a 10,000-element array attribute
      flattens into `tags.0`..`tags.9999`, ten thousand symbols interned forever.

    There is no `max_keys`-style cap (settled; see the ADR's Alternatives). The operator's levers
    are a narrowed `attributes:`/`resource:` list and `arrays: skip`. A rotating key space in
    *value* position that `flatten` promotes to *key* position (a map keyed by request or user
    IDs) is the exposure `json`/`otlp_in` already have one level shallower, multiplied by every
    distinct path above it.
  - **`lua`/`lua_file` feed it from five feeders, one guarded.** `telemetry`'s tag keys/values
    and metric names (`crates/logit-script/src/telemetry.rs`'s `static_str`/`static_metric_name`/
    `read_tags`) are the guarded case: `install`'s `count`/`gauge` closures check
    `Telemetry::is_enabled` before calling them, so a disabled handle costs nothing whatever a
    script passes. The other four are unguarded, per this entry's accepted posture:
    - `MetricProxy`'s `__newindex` on `name`/`unit`/`description`;
    - `LogProxy`'s `__newindex` on `event_name`;
    - `Event.new`'s symbol fields: `construct::metric_from_table`'s `name` directly, and its
      `unit`/`description` and `construct::log_from_table`'s `event_name` through
      `symbol_field`;
    - every attribute key a script writes, top-level through `AttrsProxy::__newindex` and nested
      through `value.rs`'s `lua_table_to_attrmap`.

    See [ADR `lua-runaway-script-bounds`](../adr/lua-runaway-script-bounds.md) and the
    [Lua](transforms.md#lua) section.

- **`HyperLogLog::from_bytes` (`crates/logit-core/src/metric.rs`) works around an upstream
  allocation-layout bug in `cardinality-estimator` 1.0.3, not only a byte-shape mismatch.**
  - **The bug:** that crate's `Array::from_vec` rounds a deserialized `Vec<u32>`'s length up to
    the next power of two, `resize`s to it, then frees the representation with a `Box::from_raw`
    sized to that rounded length. If the `Vec` carries more spare capacity than the rounded length,
    the dealloc uses the wrong `Layout`: undefined behavior. That's routine when serde's blanket
    `Vec<T>` deserializer allocates with the *unrounded* count as its `Vec::with_capacity` hint, so
    ordinary native `METRIC_SET` decoding reaches it, not only a crafted blob.
  - **Our fix:** `HllBytesReader` reports the already-rounded capacity as
    `serde::de::SeqAccess::size_hint` for the members list, so the first allocation is the size
    the crate settles on, and it bounds the claimed member count before allocating at all.
    `HyperLogLog`'s and `HllBytesReader`'s doc comments (same file) have the full mechanism.
  - **Pinning tests:** `hyperloglog_round_trips_non_power_of_two_member_counts`, which Miri fails
    on a regressed size hint only under
    `-Zmiri-disable-stacked-borrows -Zmiri-permissive-provenance` (`script/unsafe-check miri`
    passes both), and
    `a_members_vec_deserialized_through_the_hll_reader_has_the_capacity_upstream_frees`, which
    pins serde's `Vec` preallocation on stable.
  - **Revisit** on any `cardinality-estimator` upgrade; it's pinned to 1.0.3. The upstream fix
    would be `into_boxed_slice`/`shrink_to_fit` in `Array::from_vec`, so the freed layout always
    matches the `Vec`'s capacity by construction.
- **A decoded sketch's or HyperLogLog's summary fields are taken as written**
  (`crates/logit-core/src/sketch.rs`'s module doc, `HyperLogLog::from_bytes`). `from_bytes`
  bounds what a blob can allocate or make later operations cost, and rejects a zero-register
  count past the register count, but trusts the rest of a peer's summary. That's a non-goal under
  [ADR `deployment-threat-model`](../adr/deployment-threat-model.md) (accidental data from private
  peers), and [ADR `untrusted-input-bounds`](../adr/untrusted-input-bounds.md) adds a check only
  where it's free and would catch an accident. None of these panics:
  - A `DdSketch` with `min > max` answers non-monotonic quantiles.
  - A `DdSketch` with an infinite or `NaN` `min` or `max` hands it to `quantile`'s clamp, so a
    decoded sketch can answer `±∞`.
  - `merge` skips a `DdSketch` with a `count` of 0 over populated bins, because it returns early
    on an empty incoming sketch.
  - A `HyperLogLog` whose harmonic sum (`data[1]`) is wrong estimates wrong. The sum is an `f32`
    accumulated per register update, so recomputing it on decode wouldn't reproduce the bytes.

## Config and CLI

- **`logit graph` can't render a config with any secret left unset** — every command that loads a
  config must resolve every `!env` reference (ADR `env-yaml-tag`), including `graph`, though it
  reads only a component's `sources`/`type` to render topology and style nodes by role. A lenient
  mode that substituted a placeholder for a missing variable was tried and reverted (ADR
  `env-yaml-tag`'s Alternatives). **Workaround:** render a copy of the config with placeholder
  values filled in.
- **`!env` is invisible to `schema/logit.schema.json`**
  ([ADR `env-yaml-tag`](../adr/env-yaml-tag.md)) — resolution happens on the parsed YAML tree
  before serde sees it (`crates/logit-cli/src/config.rs`), so the schema describes the
  substituted shape, never the tag. A schema-aware YAML editor flags a `!env`-tagged value it
  can't resolve against the schema.
- **Config deserialization errors lose line/column information** once `!env` is in the picture
  (`crates/logit-cli/src/config.rs`) — resolving the tag means parsing to `serde_norway::Value`
  first and deserializing from that, and `serde_norway::from_value` carries no source location
  the way `serde_norway::from_str` on the raw file does. Two things partly offset it: `!env`'s own
  errors name a config path (`components.influx_out.token`), and a note is appended when a
  substitution's resolved type likely caused the failure.
- **No config hot reload on SIGHUP.** A config change means a restart; SIGHUP gets no special
  handling. It's out of scope for `docs/plans/operator-surface.md`, because it needs its own
  design (diffing the old and new resolved `Graph`, deciding which components to reuse versus
  tear down and rebuild), not a small addition to the readiness/exit-code work.
- **No `logit stats` command reading a live `Registry` out-of-process.** You can see `internal`'s
  telemetry only *through* the pipeline, with a sink attached downstream. There's no out-of-band
  read path like `/readyz`/`/healthz` for lifecycle state. It's set aside alongside the admin
  endpoint (`docs/plans/operator-surface.md`) as speculative until an operator asks, for the
  reason ADR `internal-telemetry-as-pipeline-events` gives for not building a `Registry`
  addressable outside the pipeline.

## Admin endpoint, readiness, and release image

- **The admin endpoint has no TLS and no auth** (`docs/plans/operator-surface.md`, ADR
  `admin-readiness-endpoint`) — anyone who can reach `admin.bind` can read the pipeline's
  lifecycle phase and every component's coarse state. It's not deferred: `/readyz`/`/healthz` are
  loopback/pod-local by design, not meant to cross a real network boundary. Either feature would
  guard against a threat model this endpoint doesn't have, for a caller already inside the
  process's own network namespace. `prometheus_out`'s exposition endpoint is the *deferred*
  version of this gap; see "`prometheus_out` has no TLS and no auth either" under
  [Prometheus](prometheus.md).
- **Readiness is per-process, not per-sink.** A single sink holding its queue while its
  destination refuses it doesn't flip `/readyz` to unready; the sink's `buffer:` block bounds what
  the hold accumulates, and the `logit.component.retrying` gauge and the paced `retrying` error
  line are what an alert watches ([ADR `sink-fault-classes`](../adr/sink-fault-classes.md)).
  `/readyz`'s `degraded` is reserved for a node that has exited with an error, not one that's
  behind.
  - The one non-exited not-ready answer is `503 stalled`, for a `lua`/`lua_file` component inside
    a script call with no progress
    ([ADR `lua-runaway-script-bounds`](../adr/lua-runaway-script-bounds.md)). It clears when the
    script resumes, with no phase change.
  - A richer per-sink probe would be additive to `PipelineState.components`, already keyed by
    component id. It isn't built because nothing has asked for it.
- **The published `ghcr.io/ross/logit` image is `latest` only, amd64 only, unsigned, and
  unattested.** It has no version tags (the workspace version is still a pre-release
  placeholder), no arm64 build, no cosign signature, and no SBOM. Nothing in CI builds or
  smoke-tests the production `Dockerfile` beyond the manual `workflow_dispatch` publish itself.
  [ADR `publish-release-image-to-ghcr`](../adr/publish-release-image-to-ghcr.md) names each as a
  follow-up.
