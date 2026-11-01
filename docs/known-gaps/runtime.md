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
  attribute, provenance, or resource value, or by a Lua script's own decision, has that cheap,
  non-cloning answer. An event a `route` sends to several targets is the exception: it costs one
  clone per extra destination, unless every event in its batch goes to the same set. A
  `route_signals` split by signal moves the payloads but clones the attributes once per part past
  the first.

- **A router's copies can reconverge downstream undetected.** Graph rule 51 rejects a component
  that lists two destinations one event can reach together in its `sources:`, but not one that
  reads them further downstream: `route` sends an event to targets `a` and `b`, `a -> x`,
  `b -> y`, and `c` lists `x` and `y`. `c` then receives that event twice. Detecting it means
  tracing every path from each pair of co-marked destinations, which an ordinary diamond would
  trip on too. **Workaround:** none in the graph; keep a component that reads several of one
  router's destinations behind one of them, or give it one target that every relevant value
  lists ([ADR `target-components`](../adr/target-components.md)'s 2026-10-16 amendment). A
  `route_signals` with `strip: false` has the same gap past rule 92. Under `strip: true` its
  targets carry disjoint payloads, so reading several, directly or downstream, delivers no payload
  twice, but each part is its own event: an event-level count over them counts a mixed event
  once per part. To count it once, send its signals to one target
  ([ADR `route-signals`](../adr/route-signals.md)).

- **Channel depth is bounded in batches, not bytes or events** (`CHANNEL_CAPACITY`,
  `crates/logit-pipeline/src/runtime.rs`) — 64 batches per edge, whatever each batch weighs, so
  total in-flight memory scales with edge count times batch size.
  - Listeners that assemble batches bound them by config through a `BatchAccumulator` under
    `receive.batch_max_events`/`batch_max_bytes`: a UDP listener
    ([ADR `decoupled-listener-io`](../adr/decoupled-listener-io.md)), each connection on the TCP
    stream driver (`crates/logit-inputs/src/tcp.rs`), and each file `tail_in`/`docker_in`/`cri_in`
    tracks.
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

- **A batch parked in `Fanout::send` when a listener's future is dropped is lost, already counted
  `sent`.** `Fanout::deliver` counts `batches.sent`/`events.sent` before it awaits the first
  consumer's channel. A send dropped while a full downstream parks it reaches no consumer, or only
  a prefix of them, and no counter records the difference. A listener's future is dropped in two
  places:
  - `Input::run_until_shutdown`'s default races `run` against the signal with no grace, so the
    drop comes the instant shutdown fires. That covers the scrape driver's kinds,
    `prometheus_in` in scrape mode and `nginx_stub_status_in` (the scrape in flight, and each
    target's batch still to send), and `generate_in`.
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
- **A signal that arrives as startup reports ready can still wait `shutdown.delay`.**
  The shutdown driver parks on the signal almost at once and, once woken, decides whether to skip
  the delay by reading the readiness phase (`run_with_options`,
  `crates/logit-pipeline/src/runtime.rs`). A SIGTERM or SIGINT that arrives in the last scheduler
  turns before `Readiness::ready` wakes the driver, but if `ready()` runs before the woken driver
  reads the phase, the driver sees `Ready` and waits the full delay before it drains.
  - **Consequence:** a pod stopped during its own startup can hold its ports for the delay, which
    the skip exists to avoid. The window is the few scheduler turns between the signal's wakeup
    and the driver's read of the phase.
  - **Revisit trigger:** a rollout seen waiting the delay on a pod that was signalled while
    starting. The fix decides the skip atomically with the phase transition, such as a
    pending-signal mark that `ready()` honors.
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
- **Tests that pin the interner's size fail intermittently under plain `cargo test`.** They read
  `interner::len()` before and after a region and assert it didn't move, to show a cache hit or a
  missed lookup interned nothing: `crates/logit-core/src/interner.rs` (the `KeyCache` hit tests),
  `crates/logit-core/src/attrs.rs` (a missed `get`/`remove`, and `get_sym`),
  `crates/logit-transforms/src/json.rs` and `logfmt.rs`, `crates/logit-proto/src/graphite/decode.rs`
  (`repeat_tag_keys_are_cache_hits_and_paths_are_not_cached`), and
  `crates/logit-bench/tests/allocations.rs`'s `flatten_pino_http_event_cold_key_cache`.
  - **Cause:** the interner is process-global and never frees. Plain `cargo test` runs a crate's
    tests on threads of one process, so another test interning a new key between the two reads
    moves `len()`. Each test's comment says it relies on `nextest` running every test in its own
    process. `script/test`, `script/check`, and CI use `nextest`, so they're unaffected.
  - **Not reproduced for this entry:** the cause is read from the tests, not from a failing run.
  - **Workaround:** run these crates under `cargo nextest run`. Plain `cargo test` is also
    unsupported for the allocation pins, which count per process.
  - **Revisit:** if a documented script runs these crates under plain `cargo test`. The fix then
    is a test that doesn't read the global count, such as one asserting on the `KeyCache`'s own
    entries.
- **`logit_in` doesn't record the sending peer's address.** Every other network listener takes
  `peer:` and, on TCP, `proxy_protocol:`: those on the shared socket drivers and the six HTTP
  listeners (`otlp_in`, `datadog_in`, `datadog_trace_in`, `splunk_hec_in`, `influxdb_in`, and
  `prometheus_in`'s remote-write receiver), which also take `forwarded:`
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
- **Nothing in the event model keeps a `Histogram` well formed, so each producer and each sink
  decides alone.** `logit_core::Histogram`'s `buckets` field is public, and the type promises no
  order, no single `+Inf` bucket, and no finite bound. A well-formed histogram has finite bounds in
  strictly increasing order, at most one `+Inf` bound and it last, and `(bound, count)` pairs
  that are each bucket's own count.
  - **Producers:** the Lua constructor `Event.new` checks the rule (strictly increasing bounds,
    `math.huge` only last, no `NaN` or `-math.huge`) and appends a missing `+Inf` bucket
    ([ADR `lua-event-constructor`](../adr/lua-event-constructor.md)). `prometheus_in`'s text decoder
    sorts bounds and drops a family whose cumulative counts decrease. `otlp_in` takes
    `explicit_bounds` as the wire has them: it checks neither their order nor that they're finite,
    and a `bucket_counts` longer than `explicit_bounds` plus one reads as several `+Inf`
    buckets.
  - **Sinks:** `otlp_out` sorts and merges equal finite bounds and folds `-Inf`, `NaN`, and
    repeated `+Inf` into the first and overflow buckets. `prometheus_out` and `influxdb_out` share
    `cumulative_counts`, which sorts, merges, and drops a `NaN` bound's bucket from the
    cumulative series. Only a `NaN` or `-Inf` bound is counted
    (`degraded{reason="non_finite_component"}`); a reorder or merge is silent. Every other
    histogram encoder (`statsd_out`, `graphite_out`, and `splunk_hec_out` under
    `multi_value: expand`, through `logit_proto::multi_value`) writes one series per stored
    bucket in stored order, so a repeated bound writes the same name twice. `collectd_out` skips
    the kind.
  - **Consequence:** one malformed histogram reads differently per destination, and the repair is
    copied into each encoder rather than stated once. This is separate from the
    [`histogram_count_mismatch`](otlp.md) entry, which is about a point's `count`, not its bounds.
  - **To close:** a `Histogram` check-and-normalize helper called at egress by every encoder, with
    one counter for what it repaired. It changes what every sink writes for a malformed input, so
    it needs its own ADR under [`lossless-transit`](../adr/lossless-transit.md). Revisit when a
    producer is found sending unordered or repeated bounds.

## Config and CLI

- **No config hot reload on SIGHUP.** A config change means a restart. A SIGHUP reopens
  `stdio_out`'s and `file_out`'s file targets and logs that the config wasn't reloaded
  ([ADR `signal-handling`](../adr/signal-handling.md)); `types_db:` and `lua_file` scripts are
  read once at startup too. TLS certificate, key, and CA files do reload
  ([ADR `tls-certificate-reload`](../adr/tls-certificate-reload.md)). Hot reload is out of scope
  for the readiness and exit-code work, because it needs its own design (diffing the old and new
  resolved `Graph`, deciding which components to reuse versus tear down and rebuild; see
  [the live-reload plan](../plans/live-reload-and-socket-handover.md)).
- **No `logit stats` command reading a live `Registry` out-of-process.** You can see `internal`'s
  telemetry only *through* the pipeline, with a sink attached downstream. There's no out-of-band
  read path like `/readyz`/`/healthz` for lifecycle state. It's set aside alongside the admin
  endpoint ([ADR `admin-readiness-endpoint`](../adr/admin-readiness-endpoint.md)) as speculative until an operator asks, for the
  reason ADR `internal-telemetry-as-pipeline-events` gives for not building a `Registry`
  addressable outside the pipeline.

## Admin endpoint, readiness, and release image

- **The published `ghcr.io/ross/logit` image is `latest` only, amd64 only, unsigned, and
  unattested.** It has no version tags (the workspace version is still a pre-release
  placeholder), no arm64 build, no cosign signature, and no SBOM. Nothing in CI builds or
  smoke-tests the production `Dockerfile` beyond the manual `workflow_dispatch` publish itself.
  [ADR `publish-release-image-to-ghcr`](../adr/publish-release-image-to-ghcr.md) names each as a
  follow-up.
