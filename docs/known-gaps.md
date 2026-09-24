# Known gaps

Deliberate, known rough edges in things already built. Check here before "fixing" something that
looks broken. Each entry either has a `todo!()` or doc-comment pointer at its location in the code
too, or is small enough to describe fully here. This is not a roadmap; see
[OVERVIEW.md](OVERVIEW.md) for planned scope.

Entries are grouped by area. Each starts with the gap in bold, then its consequence, then the
workaround or revisit trigger. Closed entries stay, struck through and marked **Closed**, so a
search for an old symptom still finds what fixed it and what, if anything, is still open.

## Pipeline runtime and graph

- **Fan-out/fan-in is unbuffered/uncoordinated** — a stalled sink backs up every branch that
  shares an upstream with it, not just its own. The component graph (ADR
  `component-graph-configuration`, [pipeline-graph.md](design/pipeline-graph.md)) makes arbitrary
  fan-out/fan-in the normal case (a sink shared by two branches, one listener feeding several
  filters). A per-edge `on_full: block | drop` backpressure policy is an open question, not yet
  designed. A router plus `target` components doesn't change this: a stalled consumer of one target
  backs up through its router into every other target's flow, like any other shared upstream.

  ~~Each extra consumer of a node costs a full `EventBatch` clone.~~ **Closed, with a real residual
  gap:** `Arc<EventBatch>` copy-on-write landed (`docs/adr/arc-eventbatch-copy-on-write.md`, three
  rounds, each correcting an overclaim of the last — worth reading for that alone). This is about
  clone cost, not backpressure. As measured:
  - A single-consumer edge (most edges in the shipped config) costs 0 allocations; an all-`Output`
    fan-out costs 1.
  - A fan-out mixing one `Output` branch with one mutating branch costs 1 *or* 6 allocations,
    depending on real scheduling.
  - A fan-out with no `Output` branch still costs a full clone (6, one worse than the original
    code), with no path to improvement under the current design.

  [memory.md](design/memory.md) §3 has the shape-by-shape account; there is no single number for
  "what fan-out costs now." A split by destination avoids the clone: a router plus `target`
  components (ADR [`target-components`](adr/target-components.md)) costs `1 + used destinations`
  allocations per batch, against 324 for the fan-out-plus-filters shape `memory.md` §3 measures for
  the identical split. So the residual gap is specifically "an unconditional fan-out to several
  mutating branches." Any destination split, named by an attribute/provenance/resource value or a
  Lua script's own decision, has a cheap, non-cloning answer.

- **Channel depth is bounded in batches, not bytes or events** (`CHANNEL_CAPACITY`,
  `crates/logit-pipeline/src/runtime.rs`) — 64 batches per edge, with unbounded batch size. A
  transform's outbound batch size is still unbounded: a 65 KB syslog datagram parsed into hundreds of
  events and re-batched by a downstream transform can produce an oversized batch with nothing in the
  config saying so, and total in-flight memory scales with edge count. Becomes real with a TCP input
  feeding a transform directly, where nothing caps how many events one read produces. Already
  bounded by config:
  - A UDP listener's own outbound edge ([ADR `decoupled-listener-io`](adr/decoupled-listener-io.md)):
    `BatchAccumulator` merges datagrams into one batch under `receive.batch_max_bytes` (default
    1MiB), so a `statsd_in`/`syslog_in` edge is bounded by config, not just by datagram size.
  - `tail_in`/`docker_in` (`docs/adr/file-tailing-and-docker-json-logs.md`) turned out *not* to be
    the TCP case: each tracked file gets its own `BatchAccumulator` under the same
    `receive.batch_max_events`/`batch_max_bytes` bound.
- **Every `Output::send` call allocates a boxed future, on every batch, for every sink, unrelated
  to telemetry or anything else in this file's other entries.** `Output` is `#[async_trait]`
  (`crates/logit-pipeline/src/output.rs`), which desugars `async fn send` into a fn returning
  `Pin<Box<dyn Future<...>>>`, so each call heap-allocates its future: 1 allocation (16 bytes) per
  call, identical through `&mut dyn Output` (the shape `run_output` has) or on a concrete type
  (`crates/logit-bench/tests/allocations.rs`'s `send_batch_through_a_noop_output_disabled_telemetry`,
  found by chance while adding `run_output` coverage for the internal-spans costing exercise — see
  the "Internal spans" item under
  [Internal telemetry and self-logging](#internal-telemetry-and-self-logging)). This is specific to
  the output side, on the hottest schedule (once per batch, every sink, every pipeline): `Input::run`
  is called once per process, and `Transform`/`ScriptWorker` aren't `#[async_trait]`.

  **A hand-written method returning `Pin<Box<dyn Future<...>>>` would not fix it** (an earlier
  suggestion of this entry, corrected in review): the box *is* how a `dyn Trait` object returns a
  future of unknown, implementer-varying size, whoever writes the method — not an artifact of
  `async_trait`'s codegen. A real fix gives up
  `dyn Output` for this call, either:
  - enum dispatch over the small, closed set of shipped `Output` kinds
    (`StreamOutput`/`InfluxDbOutput`/...), so each variant's `async fn` compiles to its own unboxed
    future; or
  - a runtime generic per node over a concrete `Output` type, which loses the config-driven dynamic
    construction (`Box<dyn Output + Send>` built from a running config,
    `crates/logit-cli/src/pipeline.rs`) the pipeline relies on.

  Real work either way, with no forcing function yet. This entry is that forcing function, for
  whenever the output path's allocation cost becomes worth chasing.

- **Closed for SIGTERM/SIGINT** ([ADR
  `service-lifecycle-and-output-retry`](adr/service-lifecycle-and-output-retry.md)) **— a datagram
  in flight when the signal lands is still lost.** A signal handler closes every listener's inbox
  normally (`logit_pipeline::run_with_shutdown`, `crates/logit-pipeline/src/runtime.rs`), triggering
  the same close-time flush a listener's natural completion has, so the aggregation window is
  protected. The in-flight loss is accepted: cancelling a listener's `run` future drops whatever it
  was mid-`recv_from`/decode on, and UDP is lossy by contract already.

  ~~`Output` still has no close/flush hook of its own.~~ **Closed**
  ([ADR `buffered-sink-delivery`](adr/buffered-sink-delivery.md)): `Output` gains
  `async fn flush(&mut self)` (default no-op, so no sink needed to change), called once `write_loop`
  (`crates/logit-pipeline/src/runtime.rs`) stops delivering — its queue drained to closed-and-empty,
  or a bounded shutdown grace (default 5s) expired with batches still undelivered. Load-bearing now
  that a sink can hold unwritten data at shutdown (see "Output buffering" under
  [Native wire format, `logit_in`/`logit_out`, and buffering](#native-wire-format-logit_inlogit_out-and-buffering)).

## Event model and interner

- **The attribute/metric-name interner never frees** (`crates/logit-core/src/interner.rs`) —
  `lasso::ThreadedRodeo` has no eviction, so every distinct string ever interned is retained for the
  life of the process, at a measured ~94-124 bytes each.

  **Accepted, not planned work.** Measured bounds: re-interning a string the table already holds
  allocates *nothing*, so a fixed schema reaches steady state and stays flat. Only keys and metric
  names are interned, never values, so the usual cardinality explosion (host, request id, user
  agent, path) never touches it. What's left is a real metric name that never repeats: a user who
  embedded an id in a metric name. That namespace is user-controlled, not attacker-controlled,
  because `logit`'s listeners are private by deployment shape ([OVERVIEW.md](OVERVIEW.md)); the
  anti-pattern is well known; and `logit` isn't what breaks first. The metric store fails well
  before (a million distinct measurement names is a million-plus series, against 94 MB here), and
  even inside `logit`, `aggregate`'s window costs ~600 bytes per series *per window* against the
  interner's ~94 bytes once — ~6× harder, sooner, and already mitigated by putting `keep` in front
  of it.

  **Revisit trigger — re-check the premise, not the conclusion:** if a listener ever stops being
  private (a public or multi-tenant ingest endpoint, a hosted aggregator). The retrofit is
  expensive: `Symbol` is `Copy` and `resolve` *panics* on an unknown symbol, so `AttrMap`,
  `MetricRecord`, `SeriesKey`, the Lua proxy, and the planned wire dictionary all assume symbols are
  eternal. See [memory.md](design/memory.md)'s interner section.

  Growth is observable: ~~If the `tracing` migration lands anyway, an `interner::len()` gauge is
  nearly free at that point and would make this observable rather than silent.~~ **Closed, ahead of
  that migration.** `internal`'s process-level gauges (`logit.process.interner.strings`,
  [internal-telemetry.md](design/internal-telemetry.md)) sample `interner::len()` on every drain
  tick; attach any sink to `internal` to watch it.

  Which listeners and transforms feed it unbounded keys:
  - **Bounded:** keys from `logit`'s own config or a fixed protocol grammar (statsd's `#tag:value`).
  - **`syslog.sd` — not in the bounded set (correction).** `syslog.sd`
    (`Value::Map { "<SD-ID>" -> Value::Map { "<PARAM-NAME>" -> ... } }`,
    [ADR `syslog-structured-data-convention`](adr/syslog-structured-data-convention.md)) interns
    every SD-ID and PARAM-NAME a peer sends, at both map levels; nesting doesn't bound that. The
    bound is RFC 5424's grammar (1 to 32 PRINTUSASCII bytes, excluding `=`, SP, `]`, `"`) — the same
    exposure the `json` transform has for an arbitrary JSON object's keys
    ([ADR `json-parsing-into-attributes`](adr/json-parsing-into-attributes.md)), with a length cap
    `json` doesn't have.
  - **`otlp_in` — the sharpest form yet (landed in PR3).**
    `crates/logit-proto/src/otlp/common.rs`'s `key_values_into_attrs` interns every OTLP
    `KeyValue.key`, and those keys are arbitrary peer-supplied strings with no `logit`-side grammar
    and no length cap — sharper than `syslog.sd`'s 32-byte-capped tokens. It is the first listener
    where "a metric name that never repeats" (the retrofit trigger) could plausibly come from
    something other than a user's own naming mistake. Mitigation: [`docs/deploying.md`](deploying.md)
    recommends `keep` in front of `otlp_in` specifically, beyond the general
    `aggregate`-cardinality recommendation
    [`examples/nginx-to-influxdb.yaml`](../examples/nginx-to-influxdb.yaml) demonstrates.
  - **`flatten` adds no new bound, by design** (`crates/logit-transforms/src/flatten.rs`,
    [ADR `flatten-transform`](adr/flatten-transform.md)). Its marginal exposure over
    `json`/`syslog_in`/`otlp_in`: path *combinations* of already-interned keys (a product, bounded by
    the fixed internal recursion-depth wall, not a key-count cap), and array indices, a new key axis
    no other component mints — a 10,000-element array attribute flattens into `tags.0`..`tags.9999`,
    ten thousand symbols interned forever. There is deliberately no `max_keys`-style cap (settled;
    see the ADR's Alternatives); the operator's levers are a narrowed `attributes:`/`resource:` list
    and `arrays: skip`. A rotating key space in *value* position that `flatten` promotes to *key*
    position (a map keyed by request or user IDs) is the exposure `json`/`otlp_in` already have one
    level shallower, multiplied by every distinct path above it.

  Unrelated to growth, fixed: **`AttrMap::get` used to intern rather than probe** (`attrs.rs`). All
  three production call sites were keyed by config strings or Lua literals, so it was a wasted hash
  plus concurrent-map probe on the hot path, not a leak. `get`/`remove` now use `interner::lookup`, a
  non-interning probe, falling through to the existing search only on a hit.

- **`HyperLogLog::from_bytes` (`crates/logit-core/src/metric.rs`) works around an upstream
  allocation-layout bug in `cardinality-estimator` 1.0.3, not just a byte-shape mismatch.** That
  crate's `Array::from_vec` rounds a deserialized `Vec<u32>`'s length up to the next power of two,
  `resize`s to it, then frees the representation with a `Box::from_raw` sized to that rounded
  length. If the `Vec` carries more spare capacity than the rounded length (routine when serde's
  blanket `Vec<T>` deserializer allocates with the *unrounded* count as its `Vec::with_capacity`
  hint), the dealloc uses the wrong `Layout`: undefined behavior, reachable through ordinary native
  `METRIC_SET` decoding, not just a crafted blob. `HllBytesReader` fixes it on our side: it reports
  the already-rounded capacity as `serde::de::SeqAccess::size_hint` for the members list (so the
  first allocation is the size the crate settles on), and bounds the claimed member count before
  allocating at all. Full mechanism: `HyperLogLog`'s and `HllBytesReader`'s doc comments (same file);
  pinning test: `hyperloglog_round_trips_non_power_of_two_member_counts`. Pinned to
  `cardinality-estimator` 1.0.3; the upstream fix would be `into_boxed_slice`/`shrink_to_fit` in
  `Array::from_vec`, so the freed layout always matches the `Vec`'s capacity by construction.
- ~~**`HyperLogLog` is real now; statsd still has no producer for it.**~~ **Closed, both halves.**
  - **Real implementation** ([`docs/plans/lossless-transit.md`](plans/lossless-transit.md)):
    `HyperLogLog` (`crates/logit-core/src/metric.rs`) wraps the `cardinality-estimator` crate —
    merge (union), `estimate()`, and a canonical `to_bytes`/`from_bytes` pinned to that crate's
    version. `logit-transforms::Aggregator` merges `MetricKind::SetMembers` into a `Set`
    (`sets: estimate`, the default) or retains an exact deduplicated member set (`sets: members`,
    bounded by `max_set_members_per_series`, falling back to an estimate on overflow); see
    [ADR `aggregation-window-semantics`](adr/aggregation-window-semantics.md)'s amendment.
    `logit-outputs::influxdb` renders a `Set`'s estimate as a `value=` field instead of erroring,
    and `logit-outputs::stdio` renders `set=<estimate>`.
  - **statsd producer**: statsd's `s` type is no longer a decode error.
    `crates/logit-inputs/src/statsd.rs` decodes `s` to `MetricKind::SetMembers`, one event per line,
    every member a zero-copy datagram slice; `crates/logit-outputs/src/statsd.rs` encodes one
    `name:<member>|s` line per member — `SetMembers`/`Set`'s own producer, the same way `ms`/`h`/`d` produce
    `Samples`/`Distribution`. See [ADR `statsd-output`](adr/statsd-output.md)'s amendment.

## Config and CLI

- **`logit graph` can't render a config with any secret left unset** — every `!env` reference must
  resolve for all three commands (ADR `env-yaml-tag`), including `graph`, though it only reads a
  component's `sources`/`type` to render topology and style nodes by role. A lenient mode that
  substituted a placeholder for a missing variable was tried and reverted (ADR `env-yaml-tag`'s
  Alternatives). Workaround: render a copy of the config with dummy values filled in.
- **`graph::is_implemented`'s error Debug-prints a whole `ComponentKind`**
  (`"kind {:?} is not implemented yet"`, `crates/logit-pipeline/src/graph.rs`) — harmless today,
  because no *unimplemented* kind carries a secret field. Since `!env` inlines secrets directly into
  fields (ADR `env-yaml-tag`) rather than referencing them by name, this becomes a real leak the
  moment an unimplemented kind gains one. Fix before that happens: redact, or list fields, instead of
  a blanket `{:?}`.
- **`!env` is invisible to `schema/logit.schema.json`** ([ADR `env-yaml-tag`](adr/env-yaml-tag.md)) —
  resolution happens on the parsed YAML tree before serde sees it
  (`crates/logit-cli/src/config.rs`), so the schema describes the substituted shape, never the tag.
  A schema-aware YAML editor flags a `!env`-tagged value it can't resolve against the schema.
- **Config deserialization errors lose line/column information** once `!env` is in the picture
  (`crates/logit-cli/src/config.rs`) — resolving the tag means parsing to `serde_norway::Value`
  first and deserializing from that, and `serde_norway::from_value` carries no source location the
  way `serde_norway::from_str` on the raw file does. Partly offset by `!env`'s own errors naming a
  config path (`components.influx_out.token`) and by the note appended when a substitution's
  resolved type likely caused the failure.
- **No config hot reload on SIGHUP.** A config change means a restart; SIGHUP gets no special
  handling. Explicitly out of scope for `docs/plans/operator-surface.md`: it needs its own design
  (diffing the old and new resolved `Graph`, deciding which components to reuse versus tear down and
  rebuild), not a small addition to the readiness/exit-code work.
- **No `logit stats` command reading a live `Registry` out-of-process.** The only way to see
  `internal`'s telemetry is *through* the pipeline (a sink attached downstream); there's no
  out-of-band read path like `/readyz`/`/healthz` for lifecycle state. Set aside alongside the admin
  endpoint (`docs/plans/operator-surface.md`) as speculative until an operator asks — the reasoning
  ADR `internal-telemetry-as-pipeline-events` gives for not building a `Registry` addressable outside
  the pipeline.

## Admin endpoint, readiness, and release image

- **The admin endpoint has no TLS and no auth** (`docs/plans/operator-surface.md`, ADR
  `admin-readiness-endpoint`) — anyone who can reach `admin.bind` can read the pipeline's lifecycle
  phase and every component's coarse state. Deliberate, not deferred: `/readyz`/`/healthz` are
  loopback/pod-local by design, not meant to cross a real network boundary, and either feature
  would guard against a threat model this endpoint doesn't have, for a caller already inside the
  process's own network namespace. (`prometheus_out`'s exposition endpoint is the *deferred* version
  of this gap; see "`prometheus_out` has no TLS and no auth either" under [Prometheus](#prometheus).)
- **Readiness is per-process, not per-sink.** A single sink stuck retrying (`degraded`, in the
  self-logging sense) does not flip `/readyz` to unready; a sink's own `buffer:` block (retry
  budget, queue depth) exists to absorb that. `/readyz`'s `degraded` phase is reserved for a node
  that has exited with an error, not one that's merely behind. A richer per-sink probe would be
  additive to `PipelineState.components` (already keyed by component id); not built because nothing
  has asked for it.
- **The published `ghcr.io/ross/logit` image is `latest` only, amd64 only, unsigned, and
  unattested.** No version tags (the workspace version is still a pre-release placeholder), no
  arm64 build, no cosign signature, no SBOM, and nothing in CI builds or smoke-tests the production
  `Dockerfile` beyond the manual `workflow_dispatch` publish itself
  ([ADR `publish-release-image-to-ghcr`](adr/publish-release-image-to-ghcr.md)). All named there as
  deliberate follow-ups.
- ~~**A Lua node's post-startup failure is invisible to `/readyz`.**~~ **Closed**
  (`crates/logit-pipeline/src/runtime.rs`'s `watch_lua_thread`). The Lua thread reports its exit
  over a second oneshot (`done_tx`, beside the ready handshake), and a `JoinSet` task awaiting that
  report is the node's entry in the join loop. A thread that panics after reporting ready is treated
  like any failing task: `NodeState::Failed`, `/readyz` `503 degraded`, the same graceful drain
  SIGTERM drives, exit code `2` with the component named in the `exiting` line, and a
  `thread_panicked` diagnostic in the self-log stream. A thread that returns on its own (inbox
  closed) reports `Finished` instead of staying `Running`. No in-process restart, deliberately: the
  fail-fast-for-the-supervisor posture ADR `service-lifecycle-and-output-retry` takes for every
  node. Unchanged: a script's *own* `process()`/`flush()` errors are logged and counted, never
  fatal, so only a Rust panic can kill the thread; a `lua_file`/`Lua` script that fails to *load* is
  still a startup failure (exit `1`), caught by the ready handshake.

## Native wire format, `logit_in`/`logit_out`, and buffering

- **Native wire protocol: the format and the transport are both done; credit-based flow control,
  QUIC, and an OTLP passthrough codec aren't.** The codec (`crates/logit-proto/src/frame.rs`/
  `src/native/`, [ADR `native-wire-format-encoding`](adr/native-wire-format-encoding.md)) and the
  connection layer (`logit_in`/`logit_out`, [ADR
  `native-transport-handshake-and-ack`](adr/native-transport-handshake-and-ack.md)) are real,
  tested `ComponentKind`s. Still open:
  - **Credit-based flow control (`window` > 1).** `Hello`/`HelloAck` negotiate and record a
    `window`, but the sender only ever has one frame outstanding. Several in-flight frames
    acknowledged out of order need `logit-pipeline`'s `SinkQueue` to track more than one
    outstanding batch: a real queue-shape change, not designed yet.
  - **QUIC.** TCP only today; a plausible later transport upgrade, not attempted.
  - **An OTLP passthrough codec.** Whether the native protocol should carry OTLP-encoded payloads
    unmodified (a relay forwarding OTLP without re-encoding into native) is an open question in
    `docs/design/wire-protocol.md`'s "Open question" section.
  - **`cargo-fuzz` targets over the decoders.** `crates/logit-proto/tests/robustness.rs`'s seeded
    mutation suite (truncation, bit flips, inflated lengths, over-depth nesting) covers the ground a
    corpus-driven fuzzer would, but `cargo-fuzz` needs nightly Rust, and the dev toolchain is
    stable-only (`docs/adr/containerized-development.md`), so fuzz targets are deferred.
    [ADR `out-of-ci-unsafe-verification`](adr/out-of-ci-unsafe-verification.md)'s throwaway
    nightly image serves a different, narrower need (miri/`cargo-careful`/fault injection over the
    raw-`libc` `unsafe`) and defers `cargo-fuzz` again in its "Alternatives considered". Closing
    this gap still means writing `cargo-fuzz` targets, not just pointing them at that image.
  - **`logit_in`'s and `internal`'s shutdown grace is fixed at 5s, not operator-tunable.** Graph
    validation's rule 17 rejects a `receive:` block on both (neither is a datagram or tail
    listener), so both always get `ReceiveConfig::default().shutdown_grace`. Both use that grace:
    `LogitInput` to close idle connections cleanly, `InternalInput` for its final drain of buffered
    self-telemetry (`crates/logit-inputs/src/internal.rs`). A real gap if a deployment needs a
    different number; no `receive:`-shaped knob exists yet.
  - **`otlp_in` can hold the graph open past shutdown.** Each connection `OtlpInput::run` spawns
    holds its own `Fanout` clone, and the input doesn't override `Input::run_until_shutdown` the way
    `logit_in` does (`crates/logit-inputs/src/logit.rs`'s module doc comment). An idle keep-alive
    HTTP/gRPC connection at shutdown can hold its `Fanout` clone open indefinitely, but the
    cancel-by-drop shutdown ([ADR
    `service-lifecycle-and-output-retry`](adr/service-lifecycle-and-output-retry.md)) depends on
    every listener releasing its clone. Follow `logit_in`'s design to fix it. Since 2026-09-14 a
    connection already closed by an operator-set `idle_timeout:` holds nothing open, so this covers
    only a connection still within its idle budget (or with none configured) when shutdown begins.
  - ~~**`otlp_in`'s TLS accept has no timeout.**~~ **Closed (2026-09-13)** for the TLS accept,
    which is all this item claimed. `crate::otlp::run` wraps `acceptor.accept(stream)` in
    `tokio::time::timeout` against `OtlpInput::handshake_timeout`, the pattern `logit_in` already
    used. A timeout and a handshake failure both surface through the per-connection
    `connection_error` diagnostic, and the permit returns when the task ends. `handshake_timeout:`
    is an operator field on `syslog_in`, `logit_in`, and `otlp_in` (5s default, non-zero per graph
    rule 45). The plaintext arm has a bound too (closed 2026-09-14, [ADR
    `otlp-tls-and-pooled-grpc-client`](adr/otlp-tls-and-pooled-grpc-client.md)'s amendment): a
    `TcpStream::peek` under the same `handshake_timeout` before the stream reaches `hyper`, so a
    connection that sends zero bytes closes within the budget on both arms. **Still open, by
    construction:** on the TLS arm the timeout bounds the TLS accept and nothing after it. `hyper`'s
    `hyper_util::server::conn::auto::Builder` reads the first bytes itself to tell HTTP/1.1 from an
    h2 preface, a read this module can't wrap without reimplementing that sniff.
    `http1().header_read_timeout(..)` doesn't cover it either (it starts only after the version is
    decided), and `protocol: grpc`'s `hyper::server::conn::http2::Builder` has no equivalent knob.
    A TLS connection that handshakes and then goes silent, or a plaintext one that sends its one
    peeked byte and goes silent, is handled by `idle_timeout:` (opt-in, closed 2026-09-14; see "No
    idle-connection timeout on a TCP listener" under
    [TLS and connection lifecycle](#tls-and-connection-lifecycle)), not by this pre-message bound.

- **Output buffering: closed for the sink side, in-memory only.** `crates/logit-proto/src/buffer.rs`'s
  `Buffer`/`InMemoryBuffer` are implemented (`push`/`peek`/`commit`, `DropOldest`/`DropNewest`).
  Every sink sits behind a bounded, byte-aware `SinkQueue` (`crates/logit-pipeline/src/queue.rs`)
  that keeps accepting while a delivery attempt is in flight or backing off, with retry
  (`RetryConfig`, up to 60s by default) and fault-classification-driven duplicate-safety
  (`Fault`/`DeliveryPosture`, `crates/logit-pipeline/src/output.rs`) behind that boundary ([ADR
  `buffered-sink-delivery`](adr/buffered-sink-delivery.md)). A persistent failure no longer ends
  `logit run` by default: it drops the offending batch and continues, exiting only after a
  sustained ~60s window of nothing but configuration-error (`Fault::Permanent`) failures. Still
  open:
  - **No durable (disk-backed) buffering on the receive side.** A UDP listener's `ReceiveQueue`
    ([ADR `decoupled-listener-io`](adr/decoupled-listener-io.md)) is in-memory only, so a restart,
    or a shutdown grace that expires mid-drain, loses what it held. The sink side is closed: an
    opt-in `buffer.disk:` block replaces a sink's in-memory `SinkQueue` with a crash-recoverable
    spool over `logit_proto::native` frames (`crates/logit-pipeline/src/disk_queue.rs`, [ADR
    `disk-backed-sink-buffer`](adr/disk-backed-sink-buffer.md)); a restart or `SIGKILL` resumes
    from the last persisted read cursor, replaying at most the batches committed since the last
    checkpoint. The same frames would serve the receive side, but its design isn't started: a
    listener has no equivalent of a sink's "haven't delivered yet" boundary to resume from.
  - **The disk-backed sink spool has a real, accepted power-loss window.** Every cursor write is
    `fsync`ed (tmp file, then directory); a segment is `fsync`ed only when it rotates away and at
    shutdown, not per push (the ADR's "Durability" section and its 2026-09-24 amendment). A power
    loss (not a process crash) can lose the active segment's most recent un-`fsync`ed writes. A `disk.sync: every_push` knob that closes the window at a real
    throughput cost is a plausible follow-up, not built.
  - **`logit_proto::buffer::Buffer<T>`'s role narrowed to `InMemoryBuffer` alone.** Written ahead
    of its caller ([ADR `buffered-sink-delivery`](adr/buffered-sink-delivery.md)), the trait's
    sync/`&mut self`/generic shape was the wrong seam once a disk-backed implementation existed:
    `DiskQueue` is async and concrete over `(Arc<EventBatch>, TraceContext)` and implements its own
    surface ([ADR `disk-backed-sink-buffer`](adr/disk-backed-sink-buffer.md)).
  - **No spool sharing, compaction, or out-of-order replay for the disk-backed sink buffer.** One
    spool directory per sink; no rewriting of written segments to reclaim space early (a segment is
    deleted whole once the read cursor crosses it); no encryption at rest. None blocks the
    at-least-once contract; each is narrower future work if a deployment needs it.
  - **No end-to-end acknowledgement — one hop further than before, still not the whole path.**
    `logit_out`'s `send` returns success only after `logit_in`'s own `Fanout::send` has accepted
    the batch into every one of its downstream inboxes ([ADR
    `native-transport-handshake-and-ack`](adr/native-transport-handshake-and-ack.md)'s "Ack point"
    decision), a real acknowledgement rather than "the write succeeded". But it covers only the next
    hop: if `logit_in` forwards to a further sink (another `logit_out`, an `influxdb_out`, ...),
    nothing tracks whether the data survives that delivery, and any non-`logit_out` sink's `send`
    still means only "the immediate destination accepted the write". The receive side is
    attributable now: a UDP listener counts every datagram it drops
    (`logit.component.datagrams.dropped`, ADR `decoupled-listener-io`), and the per-socket kernel
    counters count drops before `logit` sees the datagram (`logit.input.kernel.drops`; see "No
    visibility into the kernel's own UDP receive-buffer drops" under [UDP intake](#udp-intake)).
    What remains is the delivery side past the first hop.
  - **No out-of-order/credit-based acknowledgement.** `SinkQueue` is deliberately in-order and
    single-in-flight (one queue, one writer, `peek`-then-`commit`-the-head only) until credit-based
    flow control lands; see the "Credit-based flow control" item of the native wire protocol entry
    in this section.

- ~~**`logit_proto::Encoder`'s single-`Bytes`-per-batch contract doesn't fit a sink that needs
  per-message framing**~~ **Closed (2026-09-12).** `syslog_out` needs one UDP datagram or one
  octet-counted TCP frame per message, and `statsd_out` one statsd line per metric packed up to a
  datagram size cap; one opaque `Bytes` per batch can express neither, so both used to bypass the
  trait with a bespoke `encode_into`. Two sinks needing the shape was the awaited signal; a third
  codec (collectd) re-copying the buffer ended the deferral. [ADR
  `framed-encoder`](adr/framed-encoder.md) adds `logit_proto::FramedEncoder`, the third encoder
  shape beside `Encoder` (one blob per batch) and `SignalEncoder` (one blob per signal): N framed
  messages per batch into a shared, generic `logit_proto::MessageBuf<M>`, never failing, with a
  per-sink `Stats` for drop accounting. `syslog_out` and `statsd_out` implement it (statsd's
  per-call datagram cap became encoder state set once per transport). The collectd codec
  (`crates/logit-proto/src/collectd/encode.rs`) adopted it in `collectd_out`: its
  `Packets` buffer became `MessageBuf<usize>`, whose per-datagram `usize` meta is the value-list
  count `EMSGSIZE` accounting needs. `prometheus_out` stays outside all three traits by design.

## UDP intake

- **A UDP listener's read and decode loops share one task.** `read_loop` and `decode_loop`
  (`crates/logit-inputs/src/udp.rs`) run under `run_until_shutdown`'s one two-arm `select!`, so
  they interleave, yielding to each other on the coop budget, but never run on two cores at once.
  The coop-budget analysis found no measurable cost from that sharing
  (`docs/design/performance.md` §7). A report-only experiment alongside it, not shipped, found real
  headroom in splitting them: `decode_loop` spawned onto its own task, pinned to cores 2, 3, 14, 15
  (two fast physical cores plus their SMT siblings), against the same branch and pins otherwise.
  **These numbers are laptop-provisional and not migrated to the reference VM.** The branch
  (`udp/w4-scratch-spawn-experiment`) was local-only and report-only, and the 2026-09-20 VM
  measurement session that replaced every other number in this document didn't rebuild or re-run it
  (out of scope for that session; it would need `script/vm build` from a local directory source).
  Read them as this-laptop-that-day, like every pre-2026-09-20 figure this document used to carry
  uncaveated:

  | | single task | decode spawned |
  |---|---|---|
  | `udp-statsd-small` CPU µs/event | 1.229 | 1.082 (−12%) |
  | `udp-statsd-small` peak RSS | 22.0 MiB | 38.0 MiB |
  | `udp-statsd` CPU µs/event | 0.659 | 0.696 (+5.6%) |
  | `udp-statsd` kernel drop % | 0.69 | 0.00 |
  | `udp-statsd` mean fill | 22.8 | 2.6 |
  | `udp-statsd` peak RSS | 86.4 MiB | 264.8 MiB |

  Splitting took `udp-statsd`'s kernel drops to zero and its mean fill from ~23 to ~2.6 (the reader
  no longer waits behind decode and keeps the socket drained), and cut `udp-statsd-small`'s
  CPU/event ~12%. The cost: +5.6% CPU/event on `udp-statsd` (the cross-core handoff) and roughly
  **3× peak RSS**, because nothing paces the reader against the decoder once they stop sharing a
  poll budget.

  Shipping it needs four things designed together, three from the ADR's "Consequences" section and
  the fourth from PR #254's report of the experiment:
  1. A join-handle-plus-cancellation story to replace `run_until_shutdown`'s two-arm `select!`,
     which is load-bearing for shutdown/drain ordering: read finishing closes the queue, which lets
     decode discover closed-and-empty and flush its accumulator.
  2. Moving the decoder out of `&mut self` so it can live on a `'static` task (`D: 'static`).
  3. A `Fanout` ownership answer: dropping the decode future, which a caller can't do directly once
     it's on a task, is what closes every downstream inbox today.
  4. `receive.max_bytes`'s default revisited against real measurements, since nothing bounds the
     reader once it's decoupled from decode's pace.

  This overlaps heavily with "One reader per UDP listener" (the `SO_REUSEPORT` entry, next), which
  needs answers to the same shutdown-cascade and `Fanout`-ownership questions for N readers each
  with their own `Fanout` clone. Design the two together.

- **One reader per UDP listener.** A single read loop is one core's worth of read capacity.
  `SO_REUSEPORT` lets several sockets share one port, with the kernel load-balancing datagrams
  across them: gostatsd's `--max-readers` (default `min(8, NumCPU)`), rsyslog's per-listener thread
  count (capped at 32). Not built. The prerequisite is done: a batched read was supposed to raise
  the single-reader ceiling first, and `recvmmsg(2)` with `read_batch: 64` did, substantially
  cutting CPU per event on the single-datagram-per-packet workload (see "A UDP listener reads one
  datagram per syscall", closed, below, and
  [ADR `udp-intake-batching-and-socket-visibility`](adr/udp-intake-batching-and-socket-visibility.md)'s
  sweep), so whether one reader is still the bottleneck is now measurable rather than assumed. The
  cost of building it hasn't changed: N readers each holding their own `Fanout` clone need their own
  answer to the cancel-by-drop shutdown cascade
  ([ADR `service-lifecycle-and-output-retry`](adr/service-lifecycle-and-output-retry.md)), which
  assumes exactly one `Fanout` per listener. The same work would have to settle "A UDP listener's
  read and decode loops share one task" (previous entry).
- **No runtime `recvmmsg(2)` → `recvmsg(2)` fallback when a sandbox blocks the syscall.** On Linux a
  UDP listener always calls `recvmmsg(2)`. A seccomp profile (or an LSM) that refuses it returns
  `ENOSYS`/`EPERM` on the first call, which `read_loop` treats as fatal: the listener fails and the
  process exits with the runtime-failure code (`2`, not the bind-time `1`, because the socket bound
  fine). That is the correct shape — quinn hit the same wall on Android x86 (quinn#1947), and bun
  hit a worse one where the refusal produced no datagrams and a 100% CPU spin (bun#42678). Since
  `libc/w1` the message names the syscall, the bound socket, and the fact that
  `receive.read_batch: 1` won't help, instead of a bare `Function not implemented (os error 38)`.
  Not built, by design: bun's and quinn's other half, a one-shot `AtomicBool` latch that on the
  first `ENOSYS`/`EPERM` falls back to per-datagram `recvmsg(2)` for the life of the process
  (quinn#2079's pattern for its own `sendmsg` `EINVAL` fallback). That means carrying a second Linux
  read path forever for an environment `logit` has never been reported to run in, and a listener
  silently running `read_batch` times slower than configured is arguably worse than one that
  refuses to start. Revisit if a real deployment asks.
- **`received_at`'s strict ordering survives a wall-clock *step* only within one read batch.**
  `now_nanos()` is `SystemTime::now()` on purpose: `received_at` is the event's wall-clock
  timestamp, which a monotonic instant can't be. Within a batch, ordering doesn't depend on the
  clock (one read gives `base`; datagram `i` gets `base + i`), but across batches it does. A
  backwards `clock_settime` between two batches (chrony's `makestep`, an NTP correction after a long
  outage, a VM suspend/restore or live migration) can move the clock back far more than the
  `≤ read_batch` nanoseconds of offset, so two datagrams in consecutive batches can share a
  `received_at` — the `(series, timestamp)` collision the `+ i` offset exists to prevent (ADR
  `udp-intake-batching-and-socket-visibility`'s "One `received_at` per syscall batch"). The damage
  is bounded: `decode_loop`'s latency computation clamps with `.max(0)`, and `influxdb_out`'s
  `allocate_timestamp` disambiguates within an output batch by design. Not closable from the
  listener, because the fix, a monotonic clock, would be the wrong timestamp.
  `every_datagram_in_a_batch_gets_its_own_received_at`'s doc comment states the same distinction
  next to the assertion.
- **`ReceiveBufferSampler` still gauges a descriptor it captured at construction, rather than one
  taken from the socket at each sample.** Nothing is wrong today — the compiler just isn't asked to
  prove it. The TCP twin, `AcceptQueueSampler`, now reads the fd off the `listener` argument it is
  handed, so the gauged socket and the accepting socket are the same by construction; that closed a
  class where `sampler.accept(&other_listener)` compiled and silently reported the wrong socket's
  queue ([ADR `udp-intake-batching-and-socket-visibility`](adr/udp-intake-batching-and-socket-visibility.md)'s
  2026-09-21 amendment explains why `BorrowedFd<'_>` fixes the lifetime but not the identity). The
  UDP sampler can't get the same treatment without changing `sample_while`'s signature, since that
  function holds the sampler and the read future, not the socket. The lifetime is enforced
  indirectly: the combined future carries `&socket` through its sibling `read_loop` arm, so the
  borrow checker won't let it outlive the socket, and there is one `ReceiveBufferSampler` per
  `read_loop_sampled` per socket with no way to reach a second. Left deliberately: it is a signature
  change to a function whose arm ordering is load-bearing.
- **A UDP sink's send failures are not counted by cause.** `statsd_out`, `syslog_out`,
  `graphite_out` and `collectd_out` treat every failed datagram send the same way, so an operator
  can't tell `ENOBUFS` (local socket-buffer pressure: a tuning problem) from `EMSGSIZE` (a datagram
  past the path MTU: a configuration problem) from `ECONNREFUSED` (an ICMP port-unreachable from a
  missing receiver: a deployment problem). The fix is a `logit.output.send.errors{errno="..."}`
  count at those four send sites, with the errno set bounded by the handful a UDP `sendmsg` can
  return; only the call site can see the errno. The receive side's kernel counters (see "No
  visibility into the kernel's own UDP receive-buffer drops", closed, below) have no useful
  send-side twin: `SO_MEMINFO`'s `wmem_alloc` is ~always 0 on a UDP socket because a datagram is
  charged and uncharged inside one `sendmsg`, so a send-buffer gauge would be a flat zero. It was
  deliberately not built; `SockMeminfo` carries the field only because the option returns it.
- **Netns-wide UDP counters (`/proc/net/snmp`, `netstat -su`) are deliberately not collected.**
  `Udp: InErrors` / `RcvbufErrors` / `NoPorts` and the `UdpLite` block answer questions the
  per-socket counters can't — most usefully `NoPorts`, datagrams for a port nothing listens on,
  which is what a misconfigured sender looks like from the receiver. But they are totals for the
  whole network namespace, every process and socket in it, and `logit`'s telemetry is per component
  (`docs/design/internal-telemetry.md`: every point carries the `component`/`kind`/`role` of what
  recorded it). Publishing a namespace-wide number under one listener's identity would mislead in
  exactly the deployments where it matters, such as a host agent sharing a netns with everything
  else on the box. If wanted, they belong in a process-level scope beside `logit.process.*`, which
  `internal` already samples, not on any listener.
- ~~**No visibility into the kernel's own UDP receive-buffer drops.**~~ **Closed** (ADR
  [`udp-intake-batching-and-socket-visibility`](adr/udp-intake-batching-and-socket-visibility.md)).
  A listener's `ReceiveQueue` ([ADR `decoupled-listener-io`](adr/decoupled-listener-io.md); see
  "Output buffering" under
  [Native wire format, `logit_in`/`logit_out`, and buffering](#native-wire-format-logit_inlogit_out-and-buffering))
  always counted the datagrams *it* dropped, but a datagram the kernel discarded before
  `recv_from` returned it was invisible — the one loss path nothing could attribute to a component.
  `logit_pipeline::sockstat` now reads the kernel's per-socket counters off the listener's fd, once
  a second and once more after the read loop stops, reporting `logit.input.kernel.drops` (a count)
  and `logit.input.receive_buffer.used.bytes` / `.utilization` (gauges: the fill level that says
  whether more drops are coming).

  Two details worth keeping. It uses **`getsockopt(SO_MEMINFO)`, not procfs** (the original
  proposal): the drop counter is byte-for-byte `/proc/net/udp[6]`'s `drops` column, but needs no
  parse of a netns-wide table and no matching of *our* socket by address or inode — a match
  `SO_REUSEADDR` and multicast binds make ambiguous — and the same call returns the receive buffer's
  fill. **The same helper covers TCP listeners**: `getsockopt(TCP_INFO)` on a `LISTEN` socket
  aliases `tcpi_unacked`/`tcpi_sacked` onto the accept queue's depth and backlog ceiling, reported as
  `logit.input.accept_queue.depth` / `.limit` / `.utilization` by every stream input (`syslog_in`,
  `graphite_in`, TCP `statsd_in`, `logit_in`, `otlp_in`, `prometheus_in`'s remote-write receiver).
  This is ahead of the field: syslog-ng, rsyslog, Telegraf and gostatsd all tell operators to run
  `netstat -su`/`ss -u` themselves.

- ~~**A UDP listener reads one datagram per syscall**~~ **Closed on Linux**
  ([ADR `udp-intake-batching-and-socket-visibility`](adr/udp-intake-batching-and-socket-visibility.md)).
  `read_loop` (`logit-inputs::udp`) takes up to `receive.read_batch` datagrams per `recvmmsg(2)`
  call (default 64, ceiling 1024 — `UIO_MAXIOV`'s number, but `logit`'s own limit on the slab and
  the shutdown-path loss; the kernel clamps no `vlen` on the receive side), through
  `tokio::net::UdpSocket::async_io`, the raw-fd seam this work needed and the one
  `crates/logit-inputs/src/tail/watch.rs`'s `inotify` backend already uses. The `mmsghdr`/`iovec`
  arrays are rebuilt inside the readiness closure on every call over `Vec<u64>` backing storage, so
  no raw pointer is held across an `.await` and the read future stays `Send` with no `unsafe impl`.
  `read_batch` also sizes the decode half's `pop_many`, so one knob governs both ends of the receive
  queue, and `logit.input.reads` beside `logit.input.datagrams` exposes the mean fill of a syscall
  batch, the number that says whether the knob does anything. **Still open:** Linux only.
  `recvmmsg` has no portable equivalent worth a second implementation; other targets keep one
  `recv_from` per datagram behind the same interface, and `read_batch` is parsed and ignored there.
- ~~**A `ReceiveQueue`'s depth/bytes/utilization gauges update on every datagram, on both sides of
  the queue**~~ **Closed, both halves.** `BoundedQueue::push`/`pop`
  (`crates/logit-pipeline/src/queue.rs`) call `update_gauges` — three `Telemetry::gauge` calls,
  each locking `ComponentBuffer`'s `Mutex<HashMap>` (`crates/logit-core/src/telemetry.rs`) — on
  every accepted item. On a `SinkQueue` that's once per *batch*, an accepted cost; on a
  `ReceiveQueue` it was once per *datagram* on both sides, with the same listener's `read_loop`
  (pushing) and `decode_loop` (popping) contending on one lock. The pop side closed when
  `BoundedQueue` gained `push_many`/`pop_many`
  ([ADR `udp-intake-batching-and-socket-visibility`](adr/udp-intake-batching-and-socket-visibility.md)),
  in `BoundedQueue` itself with `push`/`pop` and every sink-side caller untouched: `decode_loop`
  pops up to 64 datagrams per call and updates the three gauges once per popped batch. Per-item
  admission, drop counting and `Block` waiting are unchanged. The push side closed with the
  `recvmmsg` read ("A UDP listener reads one datagram per syscall", previous entry): batching the
  push without batching the read would have been the same gauge update around a one-item `Vec`, but
  with `recvmmsg` at `vlen = read_batch` datagrams now arrive batched and `read_loop` hands each batch to `push_many` in one call. Both
  loops now touch the telemetry mutex once per batch.

## TLS and connection lifecycle

- **Every TLS-capable component's certificates are loaded once at startup; rotation needs a
  restart.** `otlp_in`/`otlp_out`, `logit_in`/`logit_out`, `syslog_in`/`syslog_out`, and
  `prometheus_in` (`with_tls`, one per component, all built on
  `crates/logit-inputs/src/tls.rs::build_server_config`/`crates/logit-outputs/src/tls.rs::
  build_client_config`) read every PEM file at construction (`logit run` startup) into a static
  `rustls::ClientConfig`/`ServerConfig` (or `prometheus_in`'s `reqwest` client equivalent). A
  renewed certificate (a 90-day Let's Encrypt cert, a `cert-manager`-issued one) has no effect
  until restart. Filed first against `otlp_in`/`otlp_out` as out of scope ([ADR
  `otlp-tls-and-pooled-grpc-client`](adr/otlp-tls-and-pooled-grpc-client.md)); every later TLS
  component shares the shape, so the gap generalizes ([ADR
  `syslog-tcp-ingress-and-tls`](adr/syslog-tcp-ingress-and-tls.md)). Fix: `rustls::ServerConfig`'s
  `ResolvesServerCert` (a file-watcher hook) on the server side and an equivalent reload on the
  client side, behind a SIGHUP or a poll.
- **No TLS client on any sink has a `server_name` override.** `otlp_out`, `logit_out`, and
  `syslog_out` check the peer's certificate against the configured `endpoint`'s own host, so an
  endpoint reached by IP, or through a proxy whose certificate names something else, can't be
  verified by name (OTel's equivalent knob is `tls.server_name_override`). Cheap to add, left out to
  keep the initial TLS work small ([ADR
  `otlp-tls-and-pooled-grpc-client`](adr/otlp-tls-and-pooled-grpc-client.md), [ADR
  `syslog-tcp-ingress-and-tls`](adr/syslog-tcp-ingress-and-tls.md)): `hyper-rustls`'s
  `HttpsConnectorBuilder::with_server_name_resolver` for `otlp_out`, an equivalent `reqwest`
  override for `prometheus_in` (the one TLS *client* on the input side), and a plain `ServerName`
  override ahead of `host_only(endpoint)` for `logit_out`/`syslog_out` (both on
  `crates/logit-outputs/src/tls.rs`).
- **A write-only TLS sink (`syslog_out`, and `logit_out` before its per-batch ack) cannot observe a
  peer's post-handshake rejection.** Under TLS 1.3 the server sends its whole handshake flight,
  `Finished` included, before it sees the client's certificate, so a client-cert rejection (a
  `client_ca_file`-requiring collector, no matching cert presented) arrives as an alert after
  `TlsConnector::connect` has already succeeded here. `syslog_out` flushes before reporting a batch
  delivered ([ADR `syslog-tcp-ingress-and-tls`](adr/syslog-tcp-ingress-and-tls.md)'s 2026-09-13
  amendment), but a flush only proves the bytes left this process, not that the peer accepted them.
  `send` still reports the batch delivered, and the sink never reads from the connection again to
  learn otherwise (PR #159's finding). `logit_out` is exposed only until it reads the batch's ack.
  A *server*-certificate rejection is unaffected: it happens inside the client's own handshake,
  before any write, and always surfaces as `Fault::Clean` (`crates/logit-cli/tests/syslog_round_trip.rs`'s
  `mod tls`). Closing the client-cert case means reading and interpreting TLS alerts (or
  application-level acks) the sink never looks at, out of scope for both ADRs that introduced
  these sinks.
- ~~**No idle-connection timeout on a TCP listener after a successful handshake (or, on plaintext,
  after the first byte).**~~ **Closed (2026-09-14).** All five TCP-capable listener kinds
  (`syslog_in`, `graphite_in`, `statsd_in` each with `transport: tcp`, `logit_in`, `otlp_in`) take
  an opt-in `idle_timeout:`, off by default. The clock runs only while the listener waits on the
  socket, so a connection blocked handing a batch to a full downstream is never taken for a silent
  peer; `logit_in` measures idle from its last `Ack` written, not bytes read (a peer waiting on a
  delayed ack isn't idle); `otlp_in` tracks idleness at the service level (an in-flight counter,
  not an IO-level timer) instead of wrapping hyper's read loop. See [ADR
  `idle-connection-timeout`](adr/idle-connection-timeout.md) and [`docs/deploying.md`'s
  "`idle_timeout` on a TCP listener"](deploying.md#idle_timeout-on-a-tcp-listener), which
  recommends enabling it wherever consistent traffic is expected. **Still open:** the client-side
  complement (`logit_out`/`syslog_out`/`statsd_out`/`graphite_out` probe a reused pooled connection
  before writing) closes the common case but not the race of a peer's FIN arriving *during* a
  write. That is `Fault::Ambiguous` on `logit_out` and a silent, unclassified loss on the three
  plaintext sinks, whose wire protocols give the sender no way to learn a write failed. And
  `otlp_in`'s idle clock resets on request completion, not bytes; see the next entry.
- ~~**An `otlp_in` connection that sends its *first* byte and then goes silent holds a
  connection-cap permit indefinitely.**~~ **Closed (2026-09-14)** by `idle_timeout:` (previous
  entry): once set, such a connection closes like any idle one (`graceful_shutdown()`, a bounded
  grace reusing `handshake_timeout`, then drop). An idle-timed-out connection also no longer holds
  the graph open past shutdown, which narrows but doesn't close "`otlp_in` can hold the graph open
  past shutdown" under [Native wire format, `logit_in`/`logit_out`, and
  buffering](#native-wire-format-logit_inlogit_out-and-buffering): a connection still within
  `idle_timeout` at shutdown is untouched. **Still open: the reset-on-request-completion
  narrowing.** `hyper` owns this listener's bytes, so `idle_timeout` sees only requests starting
  and finishing, not bytes read. A request head that dribbles in more slowly than `idle_timeout` on
  an otherwise-quiet keep-alive connection is still closed: a documented cost, not a bug. A request
  whose head arrives right at the idle deadline is not a further gap: it is served to completion
  inside the bounded grace (`graceful_shutdown` then poll for up to `handshake_timeout`), and the
  grace runs again after it completes so the response reaches the wire, because dropping it
  mid-flight would discard a batch already handed to `Fanout::send`. The cost is at most a
  reconnect for the next request, never a lost response or batch. A silent peer can't exploit this:
  with nothing in flight the drop still happens at the end of the grace, and a stalled body is
  bounded by the per-frame stall timeout.

## Cross-protocol mappings

- **Cross-protocol semantic gaps.** Each row below is a place where `logit`'s internal model and a
  peer wire model can't express each other cleanly. Every mapping is deliberate, counted, and
  documented at its own call site; this entry collects them in one place so nobody has to grep
  encoder doc comments to find them. Tracked as debt against
  [ADR `lossless-transit`](adr/lossless-transit.md); see
  [`docs/plans/lossless-transit.md`](plans/lossless-transit.md) for the closing assessment's
  residual-debt list. The table grows as codecs land:
  - OTLP (`crates/logit-proto/src/otlp/`) was `logit`'s first *second* wire model and started
    this list.
  - The `encode (Prometheus)` rows are the Prometheus exposition/OpenMetrics codec
    (`crates/logit-proto/src/prometheus/`,
    [ADR `prometheus-scrape-and-exposition`](adr/prometheus-scrape-and-exposition.md)). Its
    *decode* direction is lossless enough that every row is on the way out.
  - The `encode (collectd)`/`decode (collectd)` rows are the collectd binary-protocol codec
    (`crates/logit-proto/src/collectd/`, [ADR `collectd-binary-relay`](adr/collectd-binary-relay.md)),
    whose module doc is the authority for each. collectd has *fewer* numeric kinds than the model,
    so its rows are mostly "no wire form exists" rather than "the nearest shape loses something."
  - The `encode (Graphite)` rows are the Graphite/Carbon codec
    (`crates/logit-proto/src/graphite/`, [ADR `graphite-carbon-relay`](adr/graphite-carbon-relay.md)),
    whose module doc is likewise the authority. It is the narrowest wire model: a carbon datapoint
    is one untyped number at one whole second.

  | Direction | Mapping | Counter | Why |
  |---|---|---|---|
  | encode | `MetricKind::Distribution` (a `DDSketch`) → OTLP `Summary` of 5 fixed quantiles (p50/p75/p90/p95/p99) | `logit.output.metrics.degraded{metric_kind="distribution"}` | OTLP has no mergeable-sketch metric type. `ExponentialHistogram` is the nearest shape, but `DDSketch` exposes no bin iteration to convert from (`crates/logit-core/src/metric.rs`), and fabricating one would repeat the "non-mergeable HyperLogLog" mistake AGENTS.md warns against (`crates/logit-proto/src/otlp/metrics.rs`'s module doc). |
  | encode | `MetricKind::Set` (a `HyperLogLog`) → skipped entirely | `logit.output.metrics.skipped{metric_kind="set"}` | OTLP has no cardinality-estimate wire type. The gap is OTLP's, not this crate's: `HyperLogLog` is real (see "`HyperLogLog` is real now" under [Event model and interner](#event-model-and-interner)). `crates/logit-outputs/src/influxdb.rs` no longer shares this precedent; it renders a `Set`'s estimate as a `value=` field. |
  | encode | `Value::U64` above `i64::MAX` → OTLP `AnyValue.DoubleValue` | none (numeric, not a metric point) | OTLP's only integer type is signed 64-bit; exact up to `f64`'s 2^53 range, approximate above. Any `Value::U64` (even in range) also decodes back as `Value::I64`, losing "unsigned"; `otlp/common.rs`'s module doc has the full case list. |
  | encode | `Value::Timestamp` → OTLP `AnyValue.IntValue` | none | OTLP's `AnyValue` has no timestamp variant; decodes back as `Value::I64`, indistinguishable from an integer. |
  | encode | `MetricKind::Samples` (raw statsd `ms`/`h`/`d` observations) → OTLP `Summary` of 5 fixed quantiles (p50/p75/p90/p95/p99), sketched into a temporary `DdSketch` first | `logit.output.metrics.degraded{metric_kind="samples"}` | Same as the `Distribution` row: OTLP has no raw-sample-list type, so `otlp_out` sketches first (`add_weighted` per value, weighted by `(1/sample_rate).round()` clamped to `[1, 1000]`) and takes the degraded path ([ADR `metrics-model-v2`](adr/metrics-model-v2.md)). |
  | encode | `MetricRecord.exemplars` on a `Summary` point → dropped | none (documented) | `SummaryDataPoint` has no `exemplars` field (OTLP spec). `Sum`/`Gauge`/`Histogram`/`ExponentialHistogram` carry them; a `Samples`/`Distribution` degraded into a `Summary` loses them for the same reason (`crates/logit-proto/src/otlp/metrics.rs`'s module doc). |
  | encode/decode | `Exemplar`'s trace context (`TraceRef.flags`) → dropped on encode, hardcoded `0` on decode | none (documented) | OTLP's `Exemplar` has no trace-flags field: a permanent lossy mapping, not a decode shortcut (`crates/logit-proto/src/otlp/metrics.rs`'s `encode_exemplar`/`decode_exemplar`). |
  | sinks with no no-value wire form / `aggregate` | A `MetricRecord` flagged `NO_RECORDED_VALUE` → skipped at `influxdb_out`/`statsd_out`, rendered as `no_recorded_value` at `stdio_out` (never dropped — a debug sink must show it), passed through unmerged at `aggregate` | `logit.output.messages.dropped{reason="no_recorded_value"}` (statsd) / throttled diagnostic key `no_recorded_value` (influxdb) / `logit.transform.metrics.passed_through{reason="no_recorded_value"}` (aggregate) | `otlp_out` re-encodes a flagged point unchanged, the fixed point `docs/adr/lossless-transit.md` requires for `otlp_in -> otlp_out`. `collectd_out` is the one other wire with its own concept: a flagged `Gauge` is written as a GAUGE `NaN` ("no reading this interval") and decodes back flagged (`crates/logit-proto/src/collectd/mod.rs`'s module doc); any other flagged kind at `collectd_out` is skipped and counted. No other sink or transform has a "no value here" concept, so using the flag's default numeric payload would fabricate a sample (`crates/logit-core/src/metric.rs`'s `flags` doc). |
  | encode (Prometheus) | A delta `Sum`/`Histogram` → **skipped** | `logit.output.metrics.skipped{metric_kind="delta_sum"\|"delta_histogram"}`, throttled diagnostic key `delta_temporality_unresolved` | Exposition has no delta temporality: every counter and histogram is a running total since a start time. Resolving one in the sink means per-series state and an invented window, which [ADR `aggregation-window-semantics`](adr/aggregation-window-semantics.md) makes an explicit stage. The diagnostic names the fix: `aggregate` with `temporality: cumulative`. |
  | encode/decode (Prometheus) | Native histograms are **skipped in both directions** — `MetricKind::ExponentialHistogram` on the way out of either `prometheus_out` mode, a `TimeSeries.histograms[]` entry on the way into `prometheus_in(bind)` | `logit.output.metrics.skipped{metric_kind="exponential_histogram"}` on send; `logit.input.metrics.skipped{reason="native_histogram"}` on receive (also reported per request as `Decoded::histograms_skipped`, which is why a 2.0 response's `X-Prometheus-Remote-Write-Histograms-Written` is always `0`). **A 1.0 sender gets no signal at all** — 1.0 defines none of the `-Written` headers, so a Prometheus configured with `protobuf_message: prometheus.WriteRequest` and native histograms enabled sees `204`s for requests whose histograms were dropped, and only this receiver's own counter says otherwise. A 2.0 sender at least reads the zero (and Prometheus's own queue manager treats a zero against a non-zero send as a failure, loudly) | Deferred, not rejected; [ADR `prometheus-remote-write`](adr/prometheus-remote-write.md)'s "Native histograms now" alternative has the scope: a `Point::NativeHistogram` for the sparse shape, a mapping between Prometheus's `schema` and OTLP's `scale` (both base-2 exponential, but they differ on sign and zero-bucket treatment), the positive/negative span-and-delta encoding, and a decision on the gauge-vs-counter `reset_hint`. Remote-write 2.0 carries them, so the wire exists; the mapping doesn't. (Text 0.0.4 and OpenMetrics 1.0 have no syntax for them — sparse buckets live only in Prometheus's protobuf exposition and remote-write, `docs/design/telemetry-landscape.md`.) Materializing explicit buckets would be the lossy conversion `MetricKind::ExponentialHistogram` exists to avoid. |
  | encode (Prometheus remote-write) | A record flagged `FLAG_NO_RECORDED_VALUE` whose kind expands to several derived series — `Histogram`, `Summary`, `Distribution`/`Samples` (a sketch) — → **skipped**, where a flagged `Gauge`/`Sum`/marker-untyped record is written as Prometheus's own stale marker (the NaN bit pattern `0x7ff0000000000002`) | `logit.output.metrics.skipped{reason="no_recorded_value"}` | The flag says a series stopped reporting, not *which* of `_bucket{le}`/`_sum`/`_count` existed, and a stale marker must name a series by its full label set. A marker on the bare family name would mark a series that never existed (`crates/logit-proto/src/prometheus/mod.rs`'s `stale_point`). The exposition path differs: `with_stale_markers` is off there and *every* flagged record is skipped under the same counter. |
  | encode (Prometheus remote-write) | Two readings of one series whose nanosecond timestamps truncate to the same millisecond → the **later** reading wins, the earlier is dropped | `logit.output.metrics.degraded{reason="sub_ms_collapsed"}`, once per dropped reading | The wire carries milliseconds, the model nanoseconds, and one label set can't carry two samples at one timestamp: Prometheus and Mimir answer `400 duplicate sample for timestamp`, which this sink classifies `Fault::Permanent`, so sending both would cost the whole request. Real data loss — the one entry on `remote_write.rs`'s permitted-normalization list that loses a *reading* — with no fix short of a sub-millisecond wire. Reachable only from a sub-millisecond source writing one series more than once per millisecond. |
  | encode (Prometheus) | `MetricKind::Distribution`/`Samples` → a `summary` of 5 fixed quantiles (p50/p75/p90/p95/p99) with a `_count` and **no `_sum`** | `logit.output.metrics.degraded{metric_kind="distribution"\|"samples"}` | Same shape and the same shared `DISTRIBUTION_QUANTILES` constant as the OTLP `Distribution` row, so one metric describes itself identically at `otlp_out` and `prometheus_out`. The missing `_sum` was justified as "a `DDSketch` has no sum to report" (OpenMetrics permits omitting it); that claim is now stale — see "`prometheus_out`'s "a sketch has no sum" claim is stale" under [Prometheus](#prometheus). |
  | encode (Prometheus) | `MetricKind::Set`/`SetMembers` → a `gauge` of `estimate()` / of the distinct member count | `logit.output.metrics.degraded{metric_kind="set"\|"set_members"}` | Prometheus has no cardinality-estimate type, but unlike OTLP (which skips) it has a plain gauge, and a cardinality number is a valid gauge reading. Lost: mergeability; two relays' gauges can't combine the way their `HyperLogLog`s could. |
  | encode (Prometheus) | `Sum{Cumulative, !monotonic}` → `gauge` | `logit.output.metrics.degraded{metric_kind="non_monotonic_sum"}` | A Prometheus `counter` is monotonic by definition; exposing a decreasing one breaks every `rate()`. A gauge carries the value and loses only "this is a sum," which no exposition type expresses. |
  | encode (Prometheus) | A `Histogram`'s `min`/`max` → dropped | none (documented) | Neither exposition format has per-histogram min/max, only buckets, `_sum`, and `_count`. OTLP does, so `otlp_in -> prometheus_out` loses them and `otlp_in -> otlp_out` doesn't. Tracked follow-up (a `_min`/`_max` convention would be an invention, not a format feature). |
  | encode (Prometheus) | Label values: `Value::Str/I64/U64/F64/Bool` stringified; a multi-valued `Array` (a repeated DogStatsD tag key) renders its last representable element; `Null/Bytes/Timestamp/Map`, and an `Array` with no representable element, dropped | `logit.output.labels.normalized{reason="multi_value"}` (lossy: the non-last elements are discarded) / `logit.output.labels.dropped{reason="unrepresentable"}` | Labels are strings, so every kind with a faithful string form gets one and loses its type (`Value::I64(3)` and `Value::Str("3")` become the same label). A label set is a map, so a multi-value `Array` has no faithful form; last-value-wins, counted, mirrors `influxdb_out` (ADR `statsd-output`'s amendment). The dropped kinds have no honest string form: a `Bytes` need not be UTF-8, and flattening a `Map` would invent unparseable syntax. |
  | encode (Prometheus) | Name/label sanitization: every byte outside `[a-zA-Z0-9_:]` (metric) / `[a-zA-Z0-9_]` (label) → `_`, a leading digit → `_` prefix; two labels colliding after that keep the one whose original name sorts first, and an attribute colliding with a generated `le`/`quantile` is dropped | `logit.output.labels.dropped{reason="collision"\|"reserved"}` | Substitution rather than deletion follows `statsd_out` (`crates/logit-outputs/src/statsd.rs`'s `sanitize_into`), so distinct inputs usually stay distinct. Collisions are the residue (`a.b` and `a-b` are one wire name); the *metric*-name case is resolved the same way and counted in its own row below. Prometheus 3's quoted UTF-8 names would remove most of this; supporting them is a tracked follow-up. |
  | encode (Prometheus) | `EventBatch::scope`, `Resource::schema_url`, and every `dropped_attributes_count` → dropped | none (documented) | Exposition has no scope, schema, or dropped-count concept: a family is a name, a type, two metadata strings, and labelled samples. `otlp_in -> prometheus_out` loses instrumentation-scope identity; `otlp_in -> otlp_out` doesn't. Rendering scope as `otel_scope_name`/`otel_scope_version` labels (OTel's Prometheus convention) is a tracked follow-up, not a silent default. |
  | encode (Prometheus) | Two model names sanitizing onto one wire name → the family whose model name sorts first is exposed, the rest **skipped** | `logit.output.metrics.skipped{reason="name_collision"}` | Exposing both would be *invalid*: a second `# TYPE` line for one name (or a duplicate series) makes Prometheus reject the whole scrape. Resolved deterministically on model names, not arrival order, like the label collision in the sanitization row. |
  | encode (Prometheus) | An OpenMetrics `# UNIT` whose unit is not the family name's `_<unit>` suffix, or carries anything outside `[a-zA-Z0-9_]` → dropped | `logit.output.metrics.degraded{reason="unit_not_suffix"}` | OpenMetrics 1.0 requires "an underscore and the unit MUST be the suffix of the MetricFamily name", and Prometheus fails the *entire* body otherwise (`unit %q not a suffix of metric %q`). So an OTLP-sourced `MetricRecord { name: "request_duration", unit: "s" }` loses its unit rather than taking every other family down. Appending the unit to the name (Prometheus's own OTLP translation) is a tracked follow-up. |
  | encode (Prometheus) | An exemplar with no OpenMetrics line to sit on → dropped | `logit.output.metrics.degraded{reason="exemplar_dropped"}` | OpenMetrics allows one exemplar per `_total`/`_bucket` line ("a bucket MUST NOT have more than one exemplar") and caps its label set at 128 code points. A counter with N exemplars keeps one, two exemplars in one bucket keep one, and an over-budget label set keeps none (truncating a trace id would make it a lie). Text 0.0.4 drops every exemplar *uncounted*: that's the operator's dialect choice, on the permitted-normalization list. |
  | encode (Prometheus) | Two records sharing one name but disagreeing on family type → the first type wins, the rest **skipped** | `logit.output.metrics.skipped{reason="type_conflict"}` | One `# TYPE` line per name, so a `Gauge` and a `Sum` of one name can't both be exposed, and Prometheus rejects a body that tries. `prometheus_out`'s registry resolves the same conflict across scrapes (a series changing type) by replacing the family, counted separately. |
  | encode (OTLP + Prometheus) | `MetricKind::GaugeDelta` → **skipped** at every sink | `logit.output.metrics.skipped{metric_kind="gauge_delta"}`, throttled diagnostic key `gauge_delta_unresolved` | A relative gauge adjustment is *unresolved* ([ADR `relative-gauge-adjustments`](adr/relative-gauge-adjustments.md)): only `aggregate` holds the running value it applies against, and no wire has an "adjust the previous value by" concept. Every sink uses the one diagnostic key, so a missing `aggregate` is findable with one grep. |
  | decode (collectd) | A `0x0200` Signature part → skipped, **unverified**; a `0x0210` Encryption part → the rest of the datagram dropped | none (Signature) / `logit.component.diagnostics{key="encrypted_packet_dropped"}` (Encryption) | collectd's `SecurityLevel Sign`/`Encrypt` are deliberately deferred (`docs/plans/collectd-binary-relay.md`'s settled decisions). A signed packet's payload is plaintext, so it decodes, unauthenticated; keep the listener on a trusted network if that matters. An encrypted one has no plaintext, so the tail is dropped and counted. Verification needs a shared-secret config surface (`AuthFile`) and real HMAC-SHA-256/AES-256, its own piece of work. |
  | decode (collectd) | A COUNTER/ABSOLUTE above 2⁵³ → `Sum { value: f64 }`, approximate | none | The `Value::U64` int/double row, one layer down: `logit_core::Sum.value` is an `f64`, exact only to 2⁵³ (~9.0e15). Re-encoding is stable (the same `f64` gives the same `u64`; the top of the range round-trips through a saturating cast), so `collectd_in -> collectd_out` is still a fixed point, but above 2⁵³ the number isn't the sender's. A typed integer metric value is a core-model question. |
  | encode (collectd) | A `Sum` that is non-finite, has a fractional part, or falls outside its target integer range → **dropped** | `logit.output.metrics.skipped{reason="unencodable_value"}`, throttled diagnostic key `unencodable_value` | COUNTER, DERIVE, and ABSOLUTE are wire integers with no fractional type to degrade into. Rounding fabricates a value: statsd's `page.views:2\|c\|@0.3` reaches a sink as `6.666…`, and both `6` and `7` are wrong. `GAUGE` is collectd's only float type, and relabelling a counter as a gauge loses "this is a sum" (as the Prometheus non-monotonic row does) without even preserving the value. |
  | encode (collectd) | `Samples`, `Distribution`, `SetMembers`, `Set`, `Histogram`, `ExponentialHistogram`, `Summary`, and a delta non-monotonic `Sum` → **skipped**, one exhaustive `match` arm each | `logit.output.metrics.skipped{metric_kind="samples"\|"distribution"\|"set_members"\|"set"\|"histogram"\|"exponential_histogram"\|"summary"\|"non_monotonic_delta_sum"}` | collectd has four scalar data-source types (COUNTER, GAUGE, DERIVE, ABSOLUTE): no bucket, quantile, sketch, or member-set form, and no delta type that can decrease (ABSOLUTE is delta-*monotonic*). Unlike `prometheus_out`, there is no plain gauge to render an estimate onto without inventing a convention. `Summary` is the arguable case (quantiles as N gauges under synthetic type instances); deliberately not done, because a receiver's `types.db` knows nothing of that naming. |
  | encode (collectd) | `MetricRecord`'s `unit`, `description`, `start_timestamp` and `exemplars`; `EventBatch::scope`; `Resource::schema_url`; every `dropped_attributes_count`; and every attribute outside the `collectd.` namespace | none for the first group (documented); `logit.output.tags.dropped{reason="no_wire_form"}` for the attributes | A value list is an identity five-tuple, a time, an interval, and N numbers. **collectd has no tag concept at all**, so a DogStatsD tag or OTLP resource attribute reaching `collectd_out` is counted rather than folded into the type instance (which would collide with the real one and change the series identity a receiver keys on). `host.name` is counted too: host resolution reads it, but the attribute has no wire form. |
  | encode/decode (collectd) | A value list of more than `MAX_VALUES_PER_LIST` (64) data sources → on decode the part is malformed and the rest of the datagram is abandoned; on encode the list is dropped whole | `logit.component.diagnostics{key="bad_part"}` or `CodecError::Malformed` (decode) / `logit.output.metrics.skipped{reason="too_many_values"}` + diagnostic key `too_many_values` (encode) | The wire allows `(65535 - 6) / 9 = 7281`; nothing real comes close (`load` has 3, `if_octets` 2, `disk_io_time` 2). The cap applies to both directions on purpose: an over-long list fits under `max_packet_bytes`, so without the encode half a relay would emit lists any receiver running this codec rejects, abandoning every unrelated list packed behind them — and `aggregate`/`kv_metrics` can put far more than 64 records on one event. The constant bounds per-part decode work and the per-list record-name suffix fan-out (`<plugin>.<type>.<i>`); it does **not** bound interner growth, whose unbounded axis is distinct `<plugin>`/`<type>` strings, the same exposure as `statsd_in`'s wire-chosen metric names, accepted on `docs/design/memory.md` §4's "listeners are private" premise. Raise the constant if a legitimate producer ever hits it; none known does. |
  | encode (collectd) | A `log`-only event's `collectd.severity` outside `{1, 2, 4}` (present but the wrong `Value` type, or `Value::U64` out of that set) → the notification is **dropped** whole | `logit.output.metrics.skipped{reason="notification_dropped"}`, throttled diagnostic key `notification_dropped` | collectd's Severity part carries only `1` (FAILURE), `2` (WARNING), or `4` (OKAY), with no "unknown"; clamping or defaulting would report a severity nobody sent. An event with **no** `collectd.severity` isn't a notification attempt and is counted `skipped_no_metrics` instead; this row is the present-but-unusable case. |
  | encode (Graphite) | `MetricKind::Samples`/`Distribution`/`Histogram`/`ExponentialHistogram`/`Summary`/`Set`/`SetMembers` → **skipped** under `multi_value: skip` (the default), or **expanded** into dotted sub-paths (`.count`, `.sum`, `.q0_5`…`.q0_99`, `.bucket_<b>`, `.zero_count`) under `multi_value: expand` | `logit.output.metrics.skipped{metric_kind="samples"\|"distribution"\|"histogram"\|"exponential_histogram"\|"summary"\|"set"\|"set_members"}` / `logit.output.metrics.degraded{metric_kind=…}` once per record | A carbon datapoint is **one number at one second**: no bucket, quantile, sketch, or member-set form, and no typed gauge for an estimate. Skip is the default because the alternative is a *naming convention* the far end doesn't know: `x.q0_99` is a series called `x.q0_99`, not a quantile of `x`. `expand` is opt-in and named (ADR `lossless-transit`'s "summarization is opt-in and named" rule, applied to a *rendering*), and loses mergeability: two relays' `.count` series can't recombine like their `DdSketch`es. An `ExponentialHistogram`'s buckets are **not** expanded even under `expand`: materializing `base^i` bounds is the lossy conversion that kind avoids, and would mint unbounded wire paths from one record. |
  | encode (Graphite) | A `Sum`'s `temporality` and `monotonic` → **dropped**; the value goes on the wire bare | none (a named normalization, not a skip) | Carbon has no opinion on either. Unlike `prometheus_out`, which *skips* a delta `Sum` because exposition's cumulative meaning would break every `rate()`, nothing here can misread the value; only the model's extra facts are lost, so this is normalization 12 in the codec's list rather than a drop. Consequence: `otlp_in -> graphite_out -> graphite_in` turns a cumulative counter into a gauge. |
  | encode (Graphite) | A non-finite value (NaN, ±inf) → **dropped** | `logit.output.metrics.skipped{reason="unencodable_value"}`, throttled diagnostic key `unencodable_value` | Carbon drops a NaN on receipt, and has no spelling for infinity (`inf` passes carbon's `float()` but whisper can't store it). Substituting zero fabricates a reading. Decode rejects the same values (`logit.input.metrics.skipped{reason="non_finite_value"}`), so a relay never emits one. |
  | encode (Graphite) | A `MetricRecord` flagged `NO_RECORDED_VALUE` → **skipped** | `logit.output.metrics.skipped{reason="no_recorded_value"}` | The rule for every sink with no no-value wire form (the `sinks with no no-value wire form / `aggregate`` row above). Carbon has no "no reading this interval" marker (unlike collectd's GAUGE `NaN`); Graphite's "no data" is an absent datapoint, and writing the default payload would report a sample nobody sent. |
  | encode (Graphite) | `MetricRecord`'s `unit`, `description`, `start_timestamp` and `exemplars`; `EventBatch::scope`; `Resource::schema_url`; every `dropped_attributes_count` | none (documented) | A datapoint is a path, an optional tag set, a number, and a second. `otlp_in -> graphite_out` loses scope identity and unit metadata; `otlp_in -> otlp_out` doesn't. There is no comment syntax to hang them on either: plaintext has no metadata channel, and pickle is a list of three-tuples. |
  | encode (Graphite) | **Resource** attributes are rendered as carbon tags, indistinguishable from event ones | none (documented) | The `influxdb_out`/`statsd_out` rule: one wire tag set, and dropping the resource half would lose `service.name`/`host.name`. Invisible within the pair (a bare `graphite_in` resource is empty, so `graphite_in -> graphite_out` stays a fixed point), but cross-protocol `otlp_in -> graphite_out -> graphite_in` returns every resource attribute as an *event* attribute. Carbon has no second tag scope. |
  | encode (Graphite) | A path component longer than **255 bytes** → written unchanged, and rejected by whisper | none (documented) | Carbon has no path length bound, and the codec deliberately does **not** truncate: a truncated path is a *different, silently wrong* series, while an over-long one fails visibly at storage. 255 bytes is a filesystem limit (whisper stores `a.b.c` as `a/b/c.wsp`), binding only whisper-backed Graphites, not `go-carbon` with a ClickHouse backend, which is why the codec shouldn't enforce it. `/` and `\` *are* substituted with `_`, since they'd create nested directories. Put a length check in an operator-side `lua` stage if needed. |
  | encode (Prometheus) | A `MetricRecord` flagged `NO_RECORDED_VALUE` → **skipped** | `logit.output.metrics.skipped{reason="no_recorded_value"}` | The rule for every sink with no no-value wire form (the `sinks with no no-value wire form / `aggregate`` row above): exposition has no "no value here" marker, so emitting the default payload fabricates a reading. Prometheus staleness is scrape-level (a series stops appearing), which a relay can't synthesize from one flagged point. |

  **Still open, too narrow for a row:** `BodyFormat` has no OTLP field and round-trips through a
  reserved attribute (`logit.body_format`), lossless but attribute-shaped (`otlp/logs.rs`'s module
  doc). [ADR `lossless-transit`](adr/lossless-transit.md) rule (c) names it the standing example of
  a `logit`-only concept with nowhere else on the wire to go, so it stays. **Closed, formerly
  filed here:** a bare `LogRecord`'s OTLP `trace_id`/`span_id`/`flags` (now
  `logit_core::LogRecord::trace`, [ADR `log-record-trace-context`](adr/log-record-trace-context.md));
  a span's `Status.message` (`SpanRecord.ext`'s boxed `SpanExt.status_message`,
  [ADR `metrics-model-v2`](adr/metrics-model-v2.md), so `otlp/traces.rs` no longer stamps or reads
  `otel.status_message`); and a `NO_RECORDED_VALUE`-flagged point skipped on decode (same
  amendment: `MetricRecord.flags` carries the bit and the point round-trips).

  **Qualification of an ADR:** `Distribution`→`Summary` and `Set`→skip narrow
  [ADR `native-wire-format-with-otlp-bridge`](adr/native-wire-format-with-otlp-bridge.md)'s claim
  that the internal model "must be a superset of what OTLP can express, or the OTLP codec becomes
  lossy". Here `logit`'s own model (a mergeable sketch, a mergeable cardinality estimator) can't be
  losslessly re-expressed *as* OTLP, the direction ADR `native-wire-format-with-otlp-bridge` didn't
  anticipate. See
  [ADR `committed-pregenerated-otlp-protobuf`](adr/committed-pregenerated-otlp-protobuf.md)'s
  Consequences section, and `crates/logit-proto/src/otlp/metrics.rs`'s module doc for the full
  encode/decode tables this summarizes.

## statsd

- **`statsd_out` drops post-sketch metric kinds — `Distribution`/`Set`/`Histogram`/
  `ExponentialHistogram`/`Summary`/a cumulative or non-monotonic `Sum`, counted
  (`unsupported_metric_kind`).** These kinds exist only after some stage has summarized, and a
  merged `DdSketch`/`HyperLogLog` has no lossless statsd rendering; `docs/adr/statsd-output.md`'s
  original Decision section explains why that mapping needs its own design rather than a guess.
  Narrowed: `crates/logit-outputs/src/statsd.rs` now encodes `MetricKind::Samples`/`SetMembers`,
  the raw shapes `statsd_in` decodes `ms`/`h`/`d`/`s` to losslessly
  ([ADR `lossless-transit`](adr/lossless-transit.md)), back to statsd lines: `name:v1:v2|<type>|@rate` under
  `format: dogstatsd`, one line per value under `format: statsd`, and `name:m|s` one line per set
  member. Before that, `ms`/`h`/`d` decoded to `MetricKind::Distribution` and `s` was a decode error,
  so a `statsd_in -> aggregate -> statsd_out` relay dropped every timer/set metric.
  - **What round-trips:** a `statsd_in -> statsd_out` relay with no `aggregate`, or one configured
    `distributions: samples`/`sets: members`, relays a timer or set line intact.
  - **What still drops:** `aggregate`'s default summarizing config (`distributions: sketch`/`sets:
    estimate`) drops every timer/set metric. The `samples`/`members` config keeps a window's raw
    shape only while every sample in it shares one sample rate and the window stays under
    `max_samples_per_series`/`max_set_members_per_series`. Past either limit, `aggregate` falls
    back to a sketch/estimate for that window, which this sink drops and counts the same way
    (`docs/adr/statsd-output.md`'s amendment).

  Tracked as debt against [ADR `lossless-transit`](adr/lossless-transit.md); see
  [`docs/plans/lossless-transit.md`](plans/lossless-transit.md) for the closing assessment's
  residual-debt list.

- **`statsd_out` has no `unit` and no metric renaming/prefixing; egress timestamp is now carried,
  but only on a `|T`-marked line.**
  - **Timestamp:** any event without a `statsd.timestamp` `U64` carrier (everything but a relayed
    `|T`-carrying line) is stamped with the receiver's receipt time, like `syslog_out` (see
    "`event.timestamp` is still receipt time" under [syslog](#syslog)). Narrowed:
    DogStatsD's `|T<unix-seconds>` segment (`format: dogstatsd` only) round-trips. `statsd_in` sets
    `Event::timestamp` from an incoming `|T<secs>` and stamps a `statsd.timestamp: Value::U64(secs)`
    per-line carrier holding the raw wire value, not a marker bit (`docs/adr/statsd-output.md`'s
    amendment). `statsd_out` re-emits `|T<secs>` from that carrier, never from `Event::timestamp`,
    so a stage that rebuilds `Event::timestamp` after decode (notably `aggregate`'s flush) can't
    fabricate or collapse a `|T`. The classic grammar has no timestamp segment: `format: statsd`
    drops `|T` and counts it (`dropped_dialect_fields`).
  - **Unit:** `MetricRecord::unit` has no statsd wire representation and is dropped the same way.
  - **Renaming:** no native, sink-level way to rename or namespace a metric on egress. A sink-side
    `prefix` field was considered and rejected for `statsd_out` (`docs/adr/statsd-output.md`'s
    Alternatives) in favor of a future general metric-rename *transform* (native, not Lua), which
    doesn't exist yet. Workaround: a `lua` component ahead of `statsd_out` can rename or retag,
    `event.metrics[i].name = "..."`. ~~`docs/design/lua-api.md` notes a metric's value/fields are
    unexposed to Lua~~ — narrowed: `event.metrics` exposes every metric field for reading and
    `name`/`unit`/`description`/`start_timestamp` for writing on every kind. There is still no way to
    *construct or append* a metric from Lua, or to write any field besides
    `value`/`temporality`/`monotonic` on kinds other than `sum`/`gauge` (see
    `docs/design/lua-api.md`'s "Reading and writing `event.metrics`").

  Tracked as debt against [ADR `lossless-transit`](adr/lossless-transit.md); see
  [`docs/plans/lossless-transit.md`](plans/lossless-transit.md) for the closing assessment's
  residual-debt list.

- ~~**Relative gauge adjustment (`+`/`-`) and sample-rate extrapolation for distributions**~~
  **Closed, both halves** (`docs/adr/relative-gauge-adjustments.md`); three by-design residuals
  remain (below). The two halves landed as independently reviewed branches with no code dependency
  on each other, merged here once both were on `main`.

  **Relative gauge adjustments.** `statsd_in` decodes a leading `+`/`-` on a `g` value into
  `MetricKind::GaugeDelta`, explicitly *unresolved*: it must never reach a sink. `aggregate`
  resolves it against the running gauge value. An absolute keeps the last-write-wins-by-source-
  timestamp rule; a delta applies in arrival order and never advances the LWW timestamp
  (asymmetric on purpose, since mixing the two orderings is undefined once they interleave). A
  delta resolving in a *later* window than its absolute needs the gauge value to survive a flush,
  so `aggregate` retains gauge series under two independent bounds
  (`docs/adr/aggregation-window-semantics.md`'s amendment): `series_retention`, a windows-count TTL
  per series (default `5`; `0` restores strictly tumbling behavior), and `max_retained_series`, a
  hard cardinality cap (the TTL bounds only the retained set's tail, not its peak).

  **Sample-rate extrapolation.** `DdSketch::add_weighted(value, count)`
  (`crates/logit-core/src/metric.rs`) delegates to `sketches_ddsketch::DDSketch::add_with_count`,
  an O(1) native weighted add. A repeated-`add` loop and a binary-doubling `merge` were rejected;
  `merge` specifically because it is O(log count) allocations on `statsd_decode_one_line`'s
  exact-equality allocation path, which this project doesn't relax. `100|ms|@0.1` extrapolates into
  10 weighted samples, as a `c` (counter) already extrapolates via `value / sample_rate`. Weight is
  `(1.0 / sample_rate).round().max(1.0)`, **clamped** at `MAX_SAMPLE_WEIGHT` (1000, i.e. `@0.001`):
  a bound on the population estimate, not a CPU-loop guard, matching `aggregate.rs`'s
  `MAX_CONTRIBUTING_CONTEXTS_PER_SERIES` stance on fixed constants. A clamp is throttle-reported
  (`sample_rate_clamped`, mirrored into `logit.component.diagnostics{key="sample_rate_clamped"}`
  by `Diagnostics`), never silent. A sample rate on `g` (gauge) or `s` (set) stays ignored.

  **Where it lives now (updated 2026-09-12):** not in `statsd_in`.
  [`docs/plans/lossless-transit.md`](plans/lossless-transit.md) moved the sketch-and-clamp
  step verbatim (including `MAX_SAMPLE_WEIGHT`/`sample_rate_clamped`) into `aggregate`'s default
  `distributions: sketch` absorb path (`Samples::sketch`/`Samples::MAX_WEIGHT`,
  `crates/logit-core/src/metric.rs`), and `statsd_in`'s copy was deleted: `ms`/`h`/`d` decode to a
  raw `MetricKind::Samples` with `sample_rate` carried verbatim. A `statsd_in -> aggregate`
  pipeline reports `sample_rate_clamped` once, not twice. See
  [ADR `statsd-output`](adr/statsd-output.md)'s amendment and
  [ADR `aggregation-window-semantics`](adr/aggregation-window-semantics.md)'s amendment.

  **Still open, by design:**
  - **Retention is on by default (`series_retention: 5`, `max_retained_series: 10,000`), so
    upgrading with no config change turns it on for every existing `aggregate` component.** Both
    fields are additive, so no config fails to validate, but a config with high-cardinality,
    slowly churning gauge tags can grow steady-state memory from the upgrade alone (up to
    `max_retained_series` idle series held for up to `series_retention` extra windows per
    `aggregate`). `logit.transform.series.retained` shows the number; `series_retention: 0` opts
    back out to the exact pre-upgrade behavior. Deliberate: a feature whose point is resolving
    deltas correctly shouldn't ship opt-in.
  - **A delta after eviction (the cardinality cap) or after a process restart resolves against
    0.0.** Eviction is counted (`logit.transform.gauge.delta.unseeded`,
    `logit.transform.series.evicted{reason="cardinality"}`), never silent. The restart case needs
    durable aggregator state, deliberately not built: ADR `aggregation-window-semantics`'s original
    objection to cumulative counters ("state grows unbounded with series cardinality and a process
    restart resets every series to zero with no way to detect that from the emitted stream")
    applies to a retained gauge too. Retention narrows the window; it doesn't close it. A
    *cumulative* series (`temporality: cumulative`) has the same exposure but not the same
    blindness: every point carries a `start_timestamp`, so a consumer can see the restart even
    though `logit` can't prevent it; a gauge has no such field, and inventing one is not on
    the table.
  - **A `GaugeDelta` reaching a sink with no `aggregate` on its path degrades to a throttled,
    per-metric drop, not a config-time error.** `influxdb_out` reports it under its own
    `gauge_delta_unresolved` diagnostic key (not `encode_error`) and skips that metric, as with
    `Set`. A `logit validate` graph check ("a statsd input reaches an output with no `aggregate` on
    the path") is implementable in `logit-pipeline::graph`, but has a real false positive
    (resolving downstream in another collector is legitimate), and `logit validate` has no warning
    channel, only pass/fail. Deferred.

- **Closed: a repeated DogStatsD tag key decodes to a `Value::Array`, not a collapse to its last
  value.** `#team:a,team:b` is legal DogStatsD (a list, not a map). `insert_tags`
  (`crates/logit-inputs/src/statsd.rs`) folds a repeated key into a `Value::Array` in wire order,
  where a plain `AttrMap::insert` per token used to let the last token win. An *exact* duplicate
  token still dedupes at decode (`#team:a,team:a` -> `Str("a")`), matching the Datadog agent, and a
  one-element `Array` is never produced, so a non-repeated tag's shape is unchanged. `statsd_out`'s
  `build_tag_suffix` expands an `Array` back into one tag per element in order, no dedupe, so
  `x:1|c|#team:a,team:b` relays byte-for-byte instead of collapsing to `x:1|c|#team:b`. **Still
  lossy elsewhere, counted:** sinks whose wire can't carry a multi-value tag fall back to
  last-value-wins — `influxdb_out` and `prometheus_out` render an `Array`'s last representable
  element and count `logit.output.{tags,labels}.normalized{reason="multi_value"}` once per
  attribute. See [ADR `statsd-output`](adr/statsd-output.md)'s amendment and
  [`docs/plans/lossless-transit.md`](plans/lossless-transit.md) for the closing assessment.
- ~~**`statsd_in` copies tag values instead of slicing them**~~ **Closed.** It built attribute
  values with `attributes.insert(k, v)` on a `&str`, routing through `Value::str` →
  `Bytes::from(String)` (a copy of bytes already in the datagram buffer), then `build_event`'s
  `attributes.clone()` promoted each to a shared `Bytes`, copying again. It now uses the
  pointer-arithmetic `slice_of` reconstruction `syslog.rs` already had: 8 allocations per line down
  to 2, the same irreducible pair `syslog_in` pays (one `Vec<Event>` per line, one for the batch,
  split across two `Vec`s here because of statsd's multi-value grammar). `crates/logit-bench`'s
  `statsd_tag_values_share_the_datagram_allocation` (formerly
  `statsd_tag_values_are_copied_not_sliced`, inverted as its doc comment said it would be) asserts
  the zero-copy property structurally. See [memory.md](design/memory.md).

## syslog

- **Narrowed: `event.timestamp` is still receipt time, not the sender's — but that's no longer the
  only place the sender's own clock can land.** Every event is stamped with the instant its datagram
  came off the socket (`received_at`, captured by the read half and passed to
  `Decoder::decode_into` explicitly since [ADR `decoupled-listener-io`](adr/decoupled-listener-io.md)
  decoupled decode from the read loop, rather than a fresh clock read at decode time, which could
  run arbitrarily behind arrival under backlog). The sender's own timestamp is kept separately as
  the `syslog.timestamp` attribute: a `Value::Timestamp` for RFC 5424's RFC 3339 form, a raw
  `Value::Str` for RFC 3164's, or `Value::Null` for a nil 5424 TIMESTAMP. The two diverge by network
  and queueing delay always, and arbitrarily when the sender's clock is skewed or messages are
  replayed or relayed. **Consequence:** everything keyed on time — `aggregate`'s tumbling window,
  the point timestamp `influxdb_out` writes — uses `event.timestamp`, so a delayed or replayed
  message lands in the window it *arrived* in, not the one it *happened* in. **What changed:**
  `syslog_out`'s emitted TIMESTAMP follows the precedence rule in
  [ADR `syslog-structured-data-convention`](adr/syslog-structured-data-convention.md), so a
  `syslog_in -> syslog_out` relay's *wire* timestamp can reflect the origin even though
  `event.timestamp` does not. Tracked as debt against
  [ADR `lossless-transit`](adr/lossless-transit.md); see
  [`docs/plans/lossless-transit.md`](plans/lossless-transit.md) for the closing assessment's
  residual-debt list.

  **Why not derive it from the sender:** RFC 3164's timestamp has no year and no timezone, so
  resolving it to an instant means guessing both, and doing it only for RFC 5424 would give two
  senders on one listener different timestamp semantics with nothing in the config saying so.

  **Worth exploring: an optional `syslog_timestamp` transform**, added to a flow explicitly, that
  replaces `event.timestamp` with a resolved `syslog.timestamp` and makes the guesswork
  configurable. It would need:
  - RFC 5424: parse the RFC 3339 timestamp directly; no inference.
  - RFC 3164: fill in year and timezone. Default year: the one that puts the message closest to
    receipt time (handles a New Year's Eve rollover both ways). An explicit `timezone:` field,
    defaulting to UTC — never the host's local zone, which would make behavior depend on an
    environment variable.
  - A bounded **sanity window** (`max_skew:`, say): a resolved timestamp further from receipt time
    than the window is rejected, keeping receipt time, with a throttled diagnostic. Without it, one
    sender with a badly wrong clock can write points years away and quietly poison a dashboard.
  - The skip rule every other transform follows: no `syslog.timestamp`, or one that doesn't
    resolve, passes the event through with `event.timestamp` untouched — never dropped.

  Being a separate, opt-in component rather than a `syslog_in` flag is the point: it keeps the
  listener's contract simple and makes "we trust our senders' clocks" a visible line in the config
  graph rather than a default nobody remembers choosing.

- **Narrowed: `syslog_out` only re-stamps a relayed timestamp when the origin's own can't be
  rendered on the configured output format.** Per the timestamp-precedence rule in
  [ADR `syslog-structured-data-convention`](adr/syslog-structured-data-convention.md)
  (`write_5424_timestamp`/`write_3164_timestamp`, `crates/logit-outputs/src/syslog.rs`), a resolved
  `syslog.timestamp` renders directly: a `Value::Timestamp` on either output format, a `Value::Str`
  verbatim only when the output is also 3164. Still falls through to `event.timestamp` (receipt
  time): a 3164-origin `Value::Str` relayed onto a 5424 output (no year or timezone to build an RFC
  3339 stamp from), a nil `Value::Null` relayed onto a 3164 output (3164 has no NILVALUE), and an
  absent attribute. The opt-in `syslog_timestamp` transform sketched in "`event.timestamp` is still
  receipt time" (this section) would be the way to resolve `event.timestamp` itself, either
  direction.
- **`syslog_out`'s control-character escaping is ambiguous with a message that already contained
  the escape sequence literally.** The encoder escapes an embedded newline as the two characters
  `\`/`n` (similarly `\r`/NUL) so it can't forge a second syslog message downstream, but leaves a
  literal backslash untouched, because escaping it would double every backslash in a JSON message
  body and break a `| json` LogQL filter on every line. Consequence: a message that contained the
  literal two characters `\`/`n` is indistinguishable on the wire from one with a real newline.
  Accepted in `docs/adr/syslog-output.md`.
- **SD-ELEMENT/SD-PARAM order is canonicalized by name, not by wire position.**
  `write_structured_data`/`write_sd_element` (`crates/logit-outputs/src/syslog.rs`) sort SD-IDs and
  PARAM-NAMEs by name bytes rather than reproducing `AttrMap`/attribute iteration order
  (process-global intern order, not wire order). A repeated PARAM-NAME's occurrences are emitted
  grouped, so a wire `a b a` interleaving is re-emitted as `a a b`. Permitted under
  [ADR `lossless-transit`](adr/lossless-transit.md)'s attribute-reordering normalization; recorded
  in [ADR `syslog-structured-data-convention`](adr/syslog-structured-data-convention.md)'s
  Consequences and `crates/logit-cli/tests/syslog_round_trip.rs`'s normalization list.
- **Closed: `syslog_out` now emits RFC 5424 STRUCTURED-DATA.** Every `syslog.sd` element an event
  carries round-trips (`write_structured_data`, `crates/logit-outputs/src/syslog.rs`), and an
  opt-in `structured_data: { sd_id: "<name>@<PEN>" }` element carries every non-`syslog.*`
  attribute (what a `json`/`kv_metrics` stage merged into `event.attributes`) once an operator
  picks a private-enterprise-number-qualified SD-ID (RFC 5424 §7.2.2; no default ships). See
  [ADR `syslog-structured-data-convention`](adr/syslog-structured-data-convention.md). **Still
  open:** a log's native trace context (`log.trace`,
  [ADR `log-record-trace-context`](adr/log-record-trace-context.md)) isn't an `event.attribute`, so
  the opt-in element can't carry it.
- ~~**`syslog_out` has no TLS**~~ **Closed (2026-09-13)** for RFC 5425 (syslog over TLS over TCP):
  `syslog_out` gains `tls:` (`TlsClientConfig`) and `syslog_in` the matching `transport: tcp`/`tls:`
  (`TlsServerConfig`), both on the `logit_out`/`otlp_in`-shaped config plumbing this entry named as
  the fix; see [ADR `syslog-tcp-ingress-and-tls`](adr/syslog-tcp-ingress-and-tls.md). **Still open:
  RFC 6012 (DTLS, syslog over TLS over UDP)**, out of scope for that ADR (see its Alternatives). A
  `tls:` block under `transport: udp` is a config error on both `syslog_in` and `syslog_out`, not
  silently ignored, so this is a real gap, not a documentation one.
- ~~**`syslog_in` is UDP-only**~~ **Closed (2026-09-13).** `syslog_in` gains `transport: tcp` on
  the generic stream driver (`logit-inputs::tcp::TcpListener`) that `syslog_out`'s TCP transport
  already used from the egress side: RFC 6587 framing (octet-counting or non-transparent,
  auto-detected per connection), plus `tls:` for RFC 5425 syslog over TLS. The asymmetry this entry
  and [ADR `syslog-output`](adr/syslog-output.md)'s "that asymmetry is deliberate" note called out
  no longer holds; see [ADR `syslog-tcp-ingress-and-tls`](adr/syslog-tcp-ingress-and-tls.md).
  **Also closed: `syslog_in` no longer skips RFC 5424 STRUCTURED-DATA** — `parse_structured_data`
  (`crates/logit-inputs/src/syslog.rs`) is a real, quote-aware parser into `syslog.sd`; see
  [ADR `syslog-structured-data-convention`](adr/syslog-structured-data-convention.md).
- **Closed: a non-UTF-8 syslog MSG decodes to a `Value::Bytes` event instead of being rejected.**
  RFC 5424's `MSG-ANY` permits arbitrary octets, and `logit-core::Value`'s `Bytes` variant carries
  them. `parse_line`/`parse_5424`/`parse_3164` (`crates/logit-inputs/src/syslog.rs`) parse header
  fields off the raw bytes and validate each as PRINTUSASCII; only the MSG slice is UTF-8-validated
  (`message_value`), so a clean header with a non-UTF-8 MSG decodes with a `Value::Bytes` message.
  `syslog_out` writes a `Value::Bytes` message raw, sanitized at the byte level
  (`sanitize_msg_bytes`), never lossy-decoded. See
  [ADR `syslog-structured-data-convention`](adr/syslog-structured-data-convention.md).

  **Still open on UDP: framing splits a binary MSG.** UTF-8 rejection was never the only obstacle
  to an arbitrary-binary payload. `SyslogDecoder::decode_into` (`crates/logit-inputs/src/syslog.rs`)
  splits on `\n` *before* any UTF-8 check on `syslog_in`'s UDP transport, so a binary payload with
  a `0x0A` byte is cut mid-value (see the HAProxy "CBOR" entry under
  [HTTP access logs](#http-access-logs-nginx-haproxy-and-http_access)). Over `transport: tcp` this
  is solved: `SyslogInput::tcp` turns line splitting off
  (`SyslogDecoder::with_line_splitting(false)`) and hands framing to
  `logit-inputs::tcp::TcpListener`'s octet-counting `Framer`
  ([ADR `syslog-tcp-ingress-and-tls`](adr/syslog-tcp-ingress-and-tls.md)), which delimits by
  declared length, so a `0x0A` inside an octet-counted MSG survives end to end. `Value::Bytes` MSG
  plus this framing clear the "reachable" bar on TCP; the HAProxy CBOR entry records why the
  decoder that would consume it was measured and not built.

## OTLP

- **`otlp_out` aborts an entire batch's `send` on the first signal request that fails -- pointed at
  a signal-partial backend fed by a mixed-signal source, that's not just noise, it can end the
  process.** `OtlpOutput::send` (`crates/logit-outputs/src/otlp.rs`) issues one request per
  non-empty signal, sequentially (traces before metrics, per `encode_signals`' fixed ordering), and
  `?`-propagates the first failure without attempting the rest. Found running `demo/`'s
  `tempo_out` against Tempo ([docs/plans/otlp-end-to-end.md](plans/otlp-end-to-end.md)), not
  anticipated by that plan.

  **Consequence.** `internal` (`self`) doesn't distinguish signals: every drain carries spans plus
  `logit`'s own `logit.*` metrics (all `Sum`/`Gauge`/`Distribution`, all mergeable). Tempo is
  traces-only (a `TraceService`, no `MetricsService`), so a mixed batch's traces request succeeds
  and its metrics request fails with `grpc-status: 12` (`UNIMPLEMENTED`, correctly classified
  `Fault::Permanent`, not retried). `write_loop` sees one failed `send` and drops the whole batch,
  though its traces already reached Tempo (confirmed via Tempo's `/api/search`/`/api/traces`).
  That alone is recoverable noise. Pointed straight at `self`, it isn't: with a 10s drain interval
  *every* `tempo_out` batch mixed both signals, `send` never returned `Ok`, `last_success` never
  advanced, and `write_loop`'s ~60s sustained-permanent-failure guard
  ([ADR `service-lifecycle-and-output-retry`](adr/service-lifecycle-and-output-retry.md), revised
  by [ADR `buffered-sink-delivery`](adr/buffered-sink-delivery.md)) killed the whole process about a
  minute after startup, taking the InfluxDB metrics path with it. That guard exists to end a
  process stuck on a real misconfiguration (a bad token, a bad bucket); two signals correctly
  reaching a backend that wants one is the false positive it can't distinguish.

  **Workaround.** Filter at the config layer. `demo/logit.yaml` puts `trace_only`
  (`type: has_signal`, `signals: [traces]`) between `self` and `tempo_out`, dropping every
  metric-only drain and forwarding span-only ones untouched
  ([ADR `signal-filtering-components`](adr/signal-filtering-components.md)). Unlike the
  `aggregate`-based workaround it replaced, `has_signal` never mutates a forwarded event and never
  lets a metrics-only batch reach `tempo_out`, so the guard has nothing to trip on.
  `demo/logit.yaml`'s `trace_only`/`tempo_out` components carry this explanation inline.
  `has_signal` and its payload-stripping siblings `keep_signals`/`drop_signals` are the general
  fix. A production `otlp_out` whose source only carries signals its destination accepts never hits
  this. **Still open (unfiled):** a per-signal partial-failure mode on `OtlpOutput::send` that
  doesn't abort sibling signals already in flight, and doesn't let one incompatible signal trip
  the sustained-failure guard for signals that are succeeding.

- **`otlp_in`'s `partial_success` response is always empty.** OTLP's
  `Export*ServiceResponse.partial_success` lets a receiver accept most of a request while
  reporting rejected records. `otlp_out` (`crates/logit-outputs/src/otlp.rs`) implements the
  *reading* half (its `a_partial_success_response_is_counted_not_failed` tests), but
  `logit_proto::SignalDecoder::decode_signal` returns no per-call skip/reject count, only a
  self-telemetry counter (`logit.input.metrics.skipped{metric_kind, reason}`). So every successful
  decode replies with an empty (all-default, "fully accepted") `partial_success`, even when a point
  was skipped. The one remaining skip case is a `Metric` whose `data` oneof isn't set
  (`crates/logit-proto/src/otlp/metrics.rs::decode_metric`'s `None => Vec::new()` arm); an over-cap
  exponential histogram and a `NO_RECORDED_VALUE`-flagged point both round-trip in full now
  ([ADR `metrics-model-v2`](adr/metrics-model-v2.md)). A fully malformed request (bad protobuf, an
  invalid span id) still fails the *whole* request (`400`/`grpc-status: 3`), the one shape the
  response does reflect. **To close:** thread a per-call count through `SignalDecoder` (a
  `crates/logit-proto` API change, out of scope for the PR that added `otlp_in`), when OTLP input
  volume makes it worth it. On the JSON path the count renders as
  `rejectedSpans`/`rejectedLogRecords`/`rejectedDataPoints`, a different key per [`Signal`], where
  protobuf shares one tag number across all three `Export*ServiceResponse` messages
  (`export_response_json`'s doc comment, `crates/logit-inputs/src/otlp.rs`). (The entry's other
  half, compression, is closed: `otlp_in` decodes gzip on both transports, bounded the way
  `otlp_out` bounds encode;
  [ADR `otlp-compression-and-decompression-bounds`](adr/otlp-compression-and-decompression-bounds.md).)

- **`otlp_in` has no CORS support — `OPTIONS` 404s, no `Access-Control-Allow-Origin`.** A browser
  exporter posting cross-origin fails at preflight: `handle_http`
  (`crates/logit-inputs/src/otlp.rs`) answers any non-`POST` method, `OPTIONS` included, with
  `404`. **Workaround (the supported path):** same-origin export through a reverse proxy in front
  of both the page and `otlp_in`, e.g. `demo/haproxy/haproxy.cfg` routing `/v1/traces` to `logit`;
  see `docs/plans/browser-tracing.md`. A real `cors:` config surface (allowed origins, an `OPTIONS`
  handler, response headers) is unbuilt; it is a config/security design of its own (the
  allowed-origins list, whether a reflexive `*` is ever appropriate), not part of the OTLP/JSON
  decoding work that surfaced it.
- **`otlp_in` answers every 4xx/5xx with `text/plain`, on both encodings — the spec wants a
  protobuf-encoded `Status`.** The spec: *"The response body for all HTTP 4xx and HTTP 5xx
  responses MUST be a Protobuf-encoded Status message"*. `text_response`
  (`crates/logit-inputs/src/otlp.rs`) always builds a plain-text body, on the protobuf and JSON
  paths alike. Present since `otlp_in` first shipped, not introduced by OTLP/JSON. Left alone
  because a `google.rpc.Status` encoder is orthogonal to decoding, and every real client checked
  (including `opentelemetry-js`) reads only the HTTP status code on error, never the body's
  content-type.
- **An OTLP/JSON request costs more peak memory per byte than a same-sized protobuf one, under the
  same `MAX_REQUEST_BYTES` cap.** The JSON path parses into a `serde_json::Value` tree
  (`crates/logit-proto/src/otlp/json/`) first, one `Map`/`Vec`/`String`/`Number` allocation per
  node, where `prost::Message::decode` builds the target structs directly. The bound still holds:
  `MAX_CONCURRENT_CONNECTIONS`'s doc comment (`crates/logit-inputs/src/otlp.rs`) states the
  worst case across all connections is a finite multiple of the existing 4 GiB figure, but no one
  has measured the multiplier. **Revisit:** profile it before OTLP/JSON sees production volume.
- ~~**`otlp_in` only accepted OTLP/protobuf, not OTLP/JSON**~~ **Closed.** `otlp_in`
  (`crates/logit-inputs/src/otlp.rs`) accepts `Content-Type: application/json` alongside protobuf
  on the HTTP transport, through a hand-written dialect layer (`crates/logit-proto/src/otlp/json/`)
  onto the same generated types and decode path. [ADR `otlp-json-decoding`](adr/otlp-json-decoding.md)
  has the design, including why `pbjson`/generated `serde::Deserialize` impls were rejected: OTLP's
  hex trace/span ids deviate from proto3 JSON's bytes-as-base64 rule, which those generators
  implement faithfully and can't skip for one field type without hand-editing generated code.
  **Still open** (entries above, and that ADR's Consequences): CORS, the `text/plain` error body,
  and the JSON path's bounded but higher memory cost.

## Prometheus

- **`prometheus_out`'s "a sketch has no sum" claim is stale.** The `encode (Prometheus)`
  `MetricKind::Distribution`/`Samples` row in the table under
  [Cross-protocol mappings](#cross-protocol-mappings) says the OpenMetrics `summary` omits `_sum`
  because "a `DDSketch` has no sum to report". That is no longer true:
  `logit_core::DdSketch::sum` (`crates/logit-core/src/metric.rs`) is exact (the inner crate
  accumulates a plain `f64` alongside the bins and adds the two sums on `merge`), and
  `graphite_out`'s `multi_value: expand` already emits `.sum` for the identical sketch
  (`crates/logit-proto/src/graphite/mod.rs`'s module doc). `prometheus_out` could emit a real `_sum`
  line today with only a changed `write!`. Noticed during the Graphite/Carbon relay effort
  (`docs/plans/graphite-carbon-relay.md`'s W4b closeout) and left out of its scope; a follow-up for
  whoever next touches `crates/logit-proto/src/prometheus/mod.rs`.
- **`prometheus_out` has no TLS and no auth either** (ADR `prometheus-scrape-and-exposition`'s
  "Security posture"). Anyone who can reach its `bind:` reads the entire registry: every label on
  every series the sink holds. Unlike the admin endpoint's deliberate non-goal (see "The admin
  endpoint has no TLS and no auth" under
  [Admin endpoint, readiness, and release image](#admin-endpoint-readiness-and-release-image)), this
  is a deferred gap: `bind:` is required, so every `prometheus_out` is listening, and the payload is
  the full metric surface, which reveals far more than a lifecycle phase. Workaround:
  `examples/prometheus-expose.yaml` binds `127.0.0.1`, and the field's doc comment says to keep it
  loopback or pod-local behind something with both. Server-side TLS would reuse `logit-inputs`'
  existing builder (`otlp_in`'s `tls:`); auth has no in-tree precedent on any listener, so it needs a
  decision on the kind (bearer, mTLS) before code.
- **The remote-write receiver has TLS but no authentication** ([ADR
  `prometheus-remote-write`](adr/prometheus-remote-write.md)'s "Security posture";
  `crates/logit-inputs/src/prometheus.rs`'s module doc). `prometheus_in`'s `bind_tls:` is transport
  security, not identity: no bearer token, no basic auth, and no mutual-TLS identity check beyond
  `rustls` accepting whatever chain a client presents when `client_ca_file` is set. Anything that
  can reach the socket can write series into the pipeline. Same gap as `admin:` and
  `prometheus_out`'s exposition `bind:` ("The admin endpoint has no TLS and no auth" and
  "`prometheus_out` has no TLS and no auth either"), for the same reason: auth has no in-tree precedent, so the kind (bearer,
  mTLS subject matching) needs deciding first. Until then, bind loopback or pod-local and front it
  with something that authenticates
  ([`examples/prometheus-remote-write-receive.yaml`](../examples/prometheus-remote-write-receive.yaml);
  `docs/deploying.md`'s "Prometheus remote-write" section).
- **1.0 remote-write typing depends on the metadata cache, which is bounded and therefore lapses**
  ([ADR `prometheus-remote-write`](adr/prometheus-remote-write.md)'s "The receiver is stateless";
  `prometheus_in`'s "Metadata cache" module-doc section). An expired family decodes untyped again —
  its samples are `unknown` and its derived series come apart — until the sender's next metadata
  request re-declares it.

  Why a cache at all: remote-write 1.0 carries a family's type, `# HELP` and `# UNIT` in
  `WriteRequest.metadata[]`, and Prometheus's 1.0 sender ships those in **separate requests** on its
  own schedule (`metadata_config`, once a minute by default). Without a cache nearly every family
  decodes as `unknown`, and `http_request_duration_seconds_bucket`/`_sum`/`_count` arrive as three
  unrelated series instead of one histogram. No sample is *lost* either way — only a **declared**
  family name claims a suffix, so an undeclared `foo_sum` is its own family
  (`crates/logit-proto/src/prometheus/assemble.rs`); the cache buys typing, not data.
  `metadata_cache:` (`max_families: 10000`, `ttl: 10m`, least-recently-seen evicted first over the
  cap; `max_families: 0` turns it off, the setting for a pure-2.0 fleet) closes that. The default
  TTL is ten times Prometheus's metadata cadence, so a live sender must miss ten refreshes running
  to lapse; a sender with a longer `metadata_config` interval, or one that went quiet and came back,
  will. Watch `logit.input.metadata_cache.size` (gauge), `.evicted{reason="expired"|"cardinality"}`
  and `.replaced`; a steady `expired` stream against a live sender means `ttl` is under that
  sender's cadence. 2.0 needs none of this: it is fully typed on every request.

  Two smaller bounds in the same cache:
  - A remembered `# HELP`/`# UNIT` is **cut to a fixed byte cap** (`MAX_METADATA_TEXT_BYTES`,
    counted `logit.input.metadata_cache.truncated`), because the table outlives the request whose
    size cap bounded it. The *type* is remembered exactly; only description text is affected.
  - A remembered type is **advisory, never authoritative**: where a declaration carried in the
    request itself would make the assembler discard a sample, a remembered one yields and the
    sample opens an implicit family of its own, counted
    `logit.input.metrics.degraded{reason="seed_mismatch"}`
    (`crates/logit-proto/src/prometheus/assemble.rs`'s "A seeded type is advisory" table). A stale
    memory costs typing, never data — but a steady `seed_mismatch` stream means the table and the
    senders disagree about a family's shape; chase it rather than tune it.

- **The remote-write receiver's metadata table is shared by every sender that can reach it, and
  peers can evict each other's entries.** One table per `prometheus_in(bind)` component, not per
  peer, evicted strictly by `last_seen` (`crates/logit-inputs/src/prometheus.rs`'s "Metadata cache"
  module-doc section). The sharing is deliberate — it lets a 2.0 sender's inline declarations type
  a 1.0 sender's series — but a peer declaring many families pushes others out of `max_families`,
  counted `.evicted{reason="cardinality"}` with **no attribution**. Repeated faster than the
  victims' `metadata_config` cadence, that keeps well-behaved senders permanently untyped; their
  samples still arrive as flat families (the remembered type is advisory; see "1.0 remote-write
  typing depends on the metadata cache"). No sender is authenticated, so the conclusion is that of
  "The remote-write receiver has TLS but no authentication": don't point a `bind:` at senders you don't control. `max_families: 0` turns off the sharing along with
  the typing. The default is on, so every existing `bind:` config acquires this on upgrade.
- **The remote-write sender does no cross-batch reordering, so a fan-in topology can draw
  out-of-order `400`s.** `prometheus_out(endpoint)` sends samples in batch order ([ADR
  `prometheus-remote-write`](adr/prometheus-remote-write.md)'s "Sender behaviour"; the sink's module
  doc). Both specs require one series' samples in timestamp order, so two upstream branches writing
  the same series into one `prometheus_out` can send an older sample after a newer one. A receiver
  with no out-of-order window (stock Prometheus, Mimir without `out_of_order_time_window`) answers
  `400`, classified `Fault::Permanent` and dropped. A single chain into one `prometheus_out` can't
  hit this. Fix: a topology that doesn't split one series across branches, or a receiver with an
  out-of-order window. The sink won't buffer its way out: a reorder window is `aggregate`-shaped
  state a sink deliberately doesn't hold.
- **`prometheus_in`'s `tls:` key is gone, and writing it is silently ignored rather than rejected.**
  TLS keys are now prefixed by mode: `scrape_tls:` (client TLS for scrapes) and `bind_tls:` (server
  TLS for the remote-write receiver) — [ADR `prometheus-remote-write`](adr/prometheus-remote-write.md)'s
  "mode-prefixed TLS keys"; pre-release, so no alias or deprecation window. The gap: **no
  `ComponentKind` variant carries `#[serde(deny_unknown_fields)]`**, so an old `tls:` under a
  `prometheus_in` is dropped at parse time with no error, the component starts with default TLS
  settings, and a scrape that should present a client certificate quietly doesn't. `logit validate`
  can't catch it either. The same silence covers any misspelled key on any `ComponentKind` variant;
  this rename just makes it likely. `TailOptions`' doc comment in `crates/logit-config/src/lib.rs`
  states the general rule, and its reason (serde refuses the attribute alongside
  `#[serde(flatten)]`) is narrower than the rule.
- **`influxdb_out` and `prometheus_in`'s scrape client still follow HTTP redirects.** Both build
  their own `reqwest::Client` with no redirect policy (`crates/logit-outputs/src/influxdb.rs`'s
  `build_client`, `crates/logit-inputs/src/prometheus.rs`'s scrape client), inheriting `reqwest`'s
  `limited(10)`. Consequence: a `301`/`302`/`303` is replayed as a body-less `GET`, so whatever
  answers it becomes the verdict on a batch never written; a `307`/`308` replays the body *and* the
  operator's `headers:` at the `Location` host, past a config-time `https://` check that has no say
  at runtime. `reqwest` strips only `Authorization`/`Cookie`, and only on a host or port change, so
  a tenant header always travels — as would `influxdb_out`'s token and a scrape URL's basic-auth
  credential. `otlp_out` and `prometheus_out`'s remote-write sender already share
  `crates/logit-outputs/src/http.rs`'s `build_client`, which turns redirects off; that helper's doc
  comment has the reasoning, and `http.rs`'s module doc names the influxdb half as this gap. Not a
  one-line flip: `influxdb_out` keeps its own client and its own
  `status_class`/`is_retryable_status`/`classify_transport_error` (the same table as `http.rs`'s
  today, as its module doc says), so closing this means moving it onto the shared client and
  classifier.
- **`logit.input.samples` means two different things depending on `prometheus_in`'s mode.** Scrape
  mode counts the *series* a scrape decoded (`events.len()`, one event per series,
  `crates/logit-inputs/src/prometheus.rs`'s `tick`); bind mode counts **wire samples** reaching the
  `Fanout` — for a classic histogram, one per `_bucket` plus `_sum` plus `_count`. One counter name,
  two units, on one `ComponentKind`, so summing `logit.input.samples` across a deployment running
  both modes adds series to samples. Deliberate: the bind-mode number matches the 2.0
  `X-Prometheus-Remote-Write-Samples-Written` header, and a counter disagreeing with that header
  would have no right answer. A mode tag was considered and not added: the modes are already told
  apart by which of `logit.input.scrapes`/`logit.input.writes` the component reports.
- **A `prometheus_in(bind)` whose downstream is already closed still answers `204`.** `Fanout::send`
  silently skips a closed consumer (counted `logit.component.events.dropped{reason=
  "closed_consumer"}`, `crates/logit-pipeline/src/fanout.rs`), and the receiver hands its batch to
  the `Fanout` *before* building the response — `otlp_in`'s ordering, which lets channel
  backpressure throttle the sender's queue. So during a shutdown that has already torn down the
  downstream half of the graph, a sender gets `204` (and, on 2.0, a non-zero `Samples-Written`) for
  a batch nothing kept. A remote-write sender treats `204` as "stored, drop it from my WAL" and never
  resends; an OTLP client's retry after `otlp_in`'s success is its own business, so this listener is
  the first where the inherited fanout behaviour breaks a *durable* promise. The fanout behaviour is
  [`docs/design/pipeline-graph.md`](design/pipeline-graph.md)'s own open question — *"today's
  `send_batch` silently drops a send on a closed downstream; under a DAG that closure should really
  propagate as a shutdown signal rather than vanish"* — and the general statement of the limit is
  "No end-to-end acknowledgement" under
  [Native wire format, `logit_in`/`logit_out`, and buffering](#native-wire-format-logit_inlogit_out-and-buffering).
  Narrow in practice (shutdown is per-connection and the window is the drain); closes when that open
  question does.

## File, stdio, and InfluxDB sinks

- **`file_out` rotates and retains by count, but has no SIGHUP/external-rotator reopen, no
  compression, no `max_age`, no timestamped rotated-file naming, and its time-based rotation is
  write-triggered rather than boundary-triggered** (ADR `rotating-file-output`).
  - *Reopen:* `file_out` only rotates a file it opened itself and never re-checks whether its path
    still names the same inode, so an external rotator leaves it writing to the unlinked inode —
    the same gap `stdio_out` has.
  - *Naming and timing:* rotated files always get a numbered suffix (`.1`, `.2`, ...), never a
    timestamp. An idle sink under a calendar `interval` rolls on its *next* write after the
    boundary, not at the boundary; the rolled file's *contents* are still exactly the previous
    period's, only its on-disk appearance is late.
  - *Retention and compression:* retention is a plain `max_files` count, with no age-based
    eviction. Compression is `format: native`'s `compression: lz4` or nothing: `format: human`'s
    text render has no compression option, and nothing compresses already-rotated files after the
    fact. Both are left to an external tool.
  - *Format:* `format:` (`human`, the human-readable render, or `native`, `logit_proto::native`'s
    wire format, ADR `file-output-native-format`) is shared with `stdio_out`. A `format:`
    *template* over `human` is the one unbuilt extension point (`Format::Ndjson` is named only as
    an aspiration, not code). Reading a `format: native` file back — a decoder-side
    reader/verifier, or wiring `NativeDecoder` into `tail_in` — is real, unblocked, undesigned
    follow-up work.
  - *Restart:* `FileTarget::open` seeds `RotationState`'s calendar period from an existing file's
    mtime (not just `written` from its length), so a restart under an `interval` policy resumes
    mid-period instead of merging two periods into one file or never rotating. Residual: an
    unreadable mtime (a failed `metadata()` call) or a backwards clock jump across the restart
    falls back to learning the period fresh on the first write after open.
  - *Crash safety (not a gap, recorded for context):* rotation is commit-point-first. The active
    file is renamed to a transient staging path before any retained file is touched, so a rename
    failure leaves the active file unrotated-and-growing and every retained file untouched (unlike
    the destructive cascade-before-rename order this replaced). A `.rotating` staging file orphaned
    by a kill between that rename and its promotion is promoted to `.1` on the next rotation, never
    silently lost.

- **`file_out` never fsyncs, by design.** Nothing in `crates/logit-outputs/src/file.rs` fsyncs
  the active file, the `.rotating` staging file, or the directory after a rename, so a power loss
  (not a process crash) can lose the most recent writes or leave a rotation half-applied. A
  log-file sink doesn't pay per-batch fsyncs for a guarantee few deployments need; see the
  "`file_out` makes no durability promise" amendment to
  [ADR `rotating-file-output`](adr/rotating-file-output.md#amendment-file_out-makes-no-durability-promise-2026-09-24).
  No revisit trigger short of a deployment that needs a power-loss-safe log file.
- **`stdio_out` has no reopen** — a file target is opened once, in append mode, and held for the
  process's lifetime, so an external log rotator that moves the file leaves `logit` writing to the
  unlinked inode until restart (there is no SIGHUP-reopen). Acceptable for a debugging/dev-loop
  sink. When a file target needs bounding, use `file_out` (ADR `rotating-file-output`), which
  shares `stdio_out`'s implementation and adds a rotation policy. The format is no longer fixed
  (`format: human | native`, ADR `file-output-native-format`); a user-supplied `format:`
  *template* over the human-readable render is designed for (the encoder is built around a
  `Format` enum with room for it) but not implemented.
- ~~**`influxdb_out`'s line encoder allocates ~180 times per event**~~ **Closed.** It was the
  largest single cost in the pipeline, roughly twice the end-to-end cost of ingesting an event. Now
  30 allocations per 100-event batch (from 18,024) and 2.6× faster: escaping and formatting go
  straight into buffers reused on the encoder, the resource and event attribute maps are
  merge-joined instead of cloned and re-inserted, the series key is borrowed for lookup and
  allocated only on a miss, and `allocate_timestamp`'s path-compression scratch is reused. Output is
  byte-for-byte unchanged, pinned by the existing format tests. **Still open:** a per-batch (not
  per-event) residue, guarded by `crates/logit-bench`'s `influx_encode_100_events`.

  **`stdio_out` got the same treatment shortly after** (`crates/logit-outputs/src/stdio.rs`): ~18
  allocations per event down to ~1 (1801 → 101 per 100 events), via the same merge-join and
  reused-buffer mechanism. It had briefly been the more wasteful encoder once `influxdb_out` was
  fixed; both are now in the same range. See [memory.md](design/memory.md)'s recommendations.

## File tailing and Docker logs

- **`docker_in`'s identity refresh, and its offset retention across a de-selecting rename, are
  both bounded by `poll_interval`, and the retention doesn't survive a `logit` restart.** A
  `config.v2.json` change (a rename, or a metadata read recovering from an earlier failure) is
  picked up on the next poll tick; a rename and a rename back within one tick is never observed. A
  container renamed out of `containers:` keeps its offset in memory so a rename back resumes
  instead of replaying, but only within this process: if `logit` restarts between the two renames,
  the container falls back to `read_from` like any file the process has never seen. See [ADR
  `docker-container-identity-and-minimal-watches`](adr/docker-container-identity-and-minimal-watches.md).
- **`docker_in` only watches `root` and the files it currently has open, so a log file's own
  first appearance inside an already-existing container directory, a rotation, and a
  `config.v2.json` change are all discovered on the next `poll_interval` tick, not instantly.**
  Only a container directory arriving or leaving under `root` is `inotify`-fast: Docker's
  per-container state directories are direct children of `root`, so that needs no per-container
  watch. None of the three poll-bound cases lose data, only latency; a short `poll_interval` is the
  only way to tighten them. This deliberately reverses the earlier per-container-directory-watch
  design, which caught all four near-instantly but cost O(containers on the host) work per log line
  written anywhere on the host. See [ADR
  `docker-container-identity-and-minimal-watches`](adr/docker-container-identity-and-minimal-watches.md).
- **`docker_in`'s timestamps are the one deliberate exception among the tailing decoders to
  "stamp receipt time."** It uses the json-file envelope's own `time` field (the daemon's
  same-host clock), because replaying a backlog (`read_from: beginning`, or a fresh container's
  already-written history) as "now" would misstate when those lines happened — see the "docker_in
  timestamps" section of [ADR
  `file-tailing-and-docker-json-logs`](adr/file-tailing-and-docker-json-logs.md). `tail_in` keeps
  the general rule (read time, matching `syslog_in`), since a plain text line carries no timestamp
  to trust. Receipt time isn't a repo-wide invariant either: `otlp_in` prefers a record's own
  `time_unix_nano` when set, falling back to `observed_time_unix_nano` only for the zero "unknown"
  sentinel. `observed_time_unix_nano` is preserved both ways: decode copies it onto
  `LogRecord.observed_timestamp` verbatim (`0` stays `0`), and encode prefers that field over the
  wall clock whenever it is non-zero, which makes `otlp_in -> otlp_out` a fixed point for it
  (`otlp/logs.rs`'s module doc, [ADR `metrics-model-v2`](adr/metrics-model-v2.md)).
- **`tail_in`/`docker_in`'s checkpoint identity is `(dev, ino)`, which doesn't survive a bind
  mount or filesystem migration that preserves content but not inode numbers.** A restored backup,
  a volume moved to different storage, or a bind mount re-created from a snapshot resumes from the
  beginning instead of the checkpointed offset. Safe (at-least-once still holds), just not the
  seamless resume of the common case.
- **`inotify` doesn't reliably fire over network or FUSE-backed mounts** (NFS chief among them) —
  and `watch: auto` falls back to polling only on outright setup failure, not on a mount type it
  can't detect in advance. For a config on such a mount, set `watch: poll` explicitly rather than
  relying on `auto` to notice.
  `poll_interval` is the only mechanism proven to work everywhere.
- **`tail_in`/`docker_in`'s `inotify` wake source is Linux-only** — every other platform runs
  `watch: poll` regardless of config, and an explicit `watch: inotify` is a startup error, not a
  silent downgrade.
- **A *file* watch that fails to register is never retried for that file.** `Watcher::watch_file`
  runs once per tracked inode, when `Tailer::open_tracked` opens it. A failure (realistically
  `ENOSPC` against `fs.inotify.max_user_watches` on a host tailing many files) is diagnosed
  `watch_error` with the errno, and that file then relies on `poll_interval` for data wakes —
  exactly `watch: poll`'s behavior — until it is rotated or re-opened. The *directory* watch
  self-heals: it is re-armed on every `scan`
  ([ADR `file-tailing-and-docker-json-logs`](adr/file-tailing-and-docker-json-logs.md)'s
  2026-09-21 amendment). Not retried per file because that would re-attempt one syscall per
  unwatched file per scan with nothing suggesting the limit moved; the diagnostic points at the
  sysctl instead.
- **Two spellings of one directory (a symlink, a `.` component) share a single kernel watch, and
  `logit.input.watch.watches` counts them twice.** `inotify_add_watch` follows symlinks and the
  desired set is keyed on the configured path string, so `paths: [/var/log/app/*.log,
  /srv/app/logs/*.log]` over a symlink registers one watch and reports two. Harmless: a
  `Wake::Discover` may name the other spelling, but the driver discards its payload before
  rescanning, and the `IN_IGNORED` purge drops both entries together. Only the gauge over-reports,
  and `docs/deploying.md`'s "What to watch for file tailing" says so. Normalizing the desired set
  (or passing `IN_DONT_FOLLOW`) would change which paths a config can name — a config-surface
  decision, not a bug fix.
- **`parse_events` discards the rest of a `read` buffer after a malformed event**, rather than
  resynchronizing. Unreachable from a real inotify fd (the kernel never returns a partial event,
  and `len` is always 0 or a multiple of 16 — both pinned in the ADR), and acceptable because the
  poll tick and the unconditional `drain` reconcile whatever a discarded event would have said.
- **`docker_in` only speaks the json-file log driver.** Docker's other logging drivers (`local`,
  `journald`, `syslog`, and more) don't write a per-container file this driver could tail. Each
  would be separate work, not a parameter on this one.
- **No per-input stream filter on `docker_in`** — to keep only `stdout` (or only `stderr`), add a
  downstream stage that reads `log.iostream`; `demo/logit.yaml`'s `nginx_stdout`, an inline `lua`
  component, is the worked example. A config field on `docker_in` was considered and set aside
  together with "Named output ports on a component" (the next entry) — see [ADR
  `file-tailing-and-docker-json-logs`](adr/file-tailing-and-docker-json-logs.md)'s "Alternatives
  considered".
- **Named output ports on a component (a listener publishing separate named streams other
  components subscribe to individually, e.g. `docker_in` publishing `stdout`/`stderr` as two
  distinct sources) don't exist.** They touch the component graph's core arity/wiring model
  broadly enough to need their own design, not a `docker_in`-sized increment, so they were deferred
  when scoping the file-tailing work. Revisit if a second, unrelated need for the same shape shows
  up; the other motivating case raised and set aside at the same time was a future
  `splitter`-style component fanning a multi-signal event out into separate logs/metrics/traces
  streams. See [ADR `file-tailing-and-docker-json-logs`](adr/file-tailing-and-docker-json-logs.md)'s
  "Alternatives considered".
- **`config.v2.json` is an internal Docker daemon format, not a documented public API** —
  `docker_in` reads it directly (no socket, no HTTP client) because it sits next to the log file
  already being read, but a Docker version bump could change its shape without notice. A missing
  or unparseable file degrades gracefully (a `container.id`-only resource, diagnosed
  `metadata_error`). The residual risk is a *silently reshaped* file that still parses but means
  something different.

## Transforms: predicates and sampling

- **Predicate-shaped work (routing by condition, sampling, throttling, dedup) costs a Lua VM, an OS
  thread, and roughly 9× the per-event allocations of a native transform, because `logit` has no
  native predicate language** — a deliberate choice, not an oversight. Today this applies to
  throttling, dedup, and anything needing an actual operator (`>=`, `contains`, cross-attribute
  comparison); the rest now has native answers (narrowed 2026-09-10, 2026-09-13, 2026-09-22):
  - Equality-only filtering, such as splitting a `logit_in` fan-out back apart by an attribute an
    upstream `set` stamped: `has_attributes`/`drop_attributes`
    ([ADR `attribute-filtering-components`](adr/attribute-filtering-components.md)).
  - Destination selection (splitting one flow into named streams): `route` (equality on
    provenance/attribute/resource) and `target` components (ADR
    [`target-components`](adr/target-components.md)); `lua` can also route with `event:to`.
  - Sampling: `sample` ([ADR `consistent-sampling-component`](adr/consistent-sampling-component.md)).
    It supersedes the routing ADR's `sample` clause on different grounds than the revisit trigger:
    keyed, cross-process-consistent sampling is something a `lua` component can't express at all,
    not just a cost.

  The cost, measured: **9** allocations / **1.61 µs** per event through a `lua` component versus
  **1** allocation / **525 ns** through a native `Transform` (`docs/design/memory.md`), plus one
  dedicated OS thread and one LuaJIT VM per `lua` node
  (`crates/logit-pipeline/src/runtime.rs`'s `run_with_telemetry`) versus an ordinary tokio task.
  Noise below roughly tens of thousands of events/sec (sidecar/host-agent volume); real at
  central-collector volume, where a multi-branch routing diamond can cost a measurable fraction of
  a core for what a native transform would answer in a third of that.
  [ADR `routing-by-condition-is-lua`](adr/routing-by-condition-is-lua.md) has the full account.
  **Revisit trigger:** sustained, *measured* central-collector throughput pressure against a real
  config, not a hunch. The ADR records a substantially designed native predicate grammar
  (total-by-construction, so it can't fail at runtime) as where to resume.
- **`sample`'s consistency is `logit`'s own, not OTel's, and stops at the key.** Four deliberate
  edges ([ADR `consistent-sampling-component`](adr/consistent-sampling-component.md)):
  1. **No OTEP 235 / W3C `tracestate` `th:` interop.** An OTel SDK or collector sampling by
     threshold compares the trace id's *low 56 bits* against a `th:` value it also writes into
     `tracestate`; `logit` hashes the id's hex text with XXH64 and compares the hash's *top 53
     bits*. So `logit`'s `sample` and an upstream OTel sampler at the same rate keep *different*
     traces, and because `logit` neither reads nor writes `th:`, downstream can't recover the
     sampling probability from a `logit`-sampled trace. Adopting OTEP 235 would mean a second bit
     convention for `key: trace_id` only (no other key has a `tracestate`) and, since the hash is a
     frozen cross-version contract, its own ADR.
  2. **`always_keep` is per leg.** Nothing about an override hit propagates: a flagged event kept
     by one sampler is effectively unflagged at the next sampler unless that one has the same
     `always_keep`. Same "no propagated bit" reasoning as the rate itself.
  3. **Two spellings of one trace id that aren't lowercase hex don't agree.** `key: trace_id`
     hashes a lifted id as its 32 lowercase hex characters, and `{attribute: ..}` hashes a `Str`
     as-is, so an *uppercase*-hex attribute (off-spec — W3C mandates lowercase) or a trace id
     carried as a 16-byte `Value::Bytes` gets a different verdict than the same id lifted by
     `trace_context`. Case is not folded and raw bytes are not hex-encoded, on purpose: the
     canonicalization table stays one rule per `Value` variant.
  4. **A resource key is all-or-nothing per resource.** `key: {resource: ..}` gives every event of
     one resource the same verdict — the point of keying on one — but at a low resource count the
     kept fraction is lumpy, not `rate` (two services at `rate: 0.5` keep zero, one, or both).

## `shape` observer

- **`shape`'s cumulative gauges are since process start, not windowed.**
  `logit.shape.distinct_keys`, `.distinct_keysets`, `.keyset_share.top1`/`.top5` and
  `.tracking_overflow` (`crates/logit-transforms/src/shape.rs`,
  [ADR `shape-observer-component`](adr/shape-observer-component.md)) accumulate from startup, are
  re-reported unchanged on every flush, and never reset. That suits the survey the instrument was
  built for (a capture's whole key-set population, not the last ten seconds), but a long-lived
  tap's distinct-key count only rises, so it can't show that a producer *stopped* emitting a key,
  and `tracking_overflow` latches at `1` for the life of the process once either cap is hit.
  Restarting the process is the only reset; a windowed variant (a second set of gauges reset per
  flush, or a decaying table) is real future work.
- **`shape` tracks top-level attribute keys only.** A nested `Value::Map`'s keys count toward that
  map's width (`logit.shape.nested_map_width`) and its values toward the per-type counters, but
  they never enter the distinct-key set or the key-set hash. Two events with matching top-level
  keys and entirely different nested maps are one key-set to `logit.shape.distinct_keysets`.
  Deliberate: the key-set identity is what `AttrMap`'s sorted `Symbol` sequence gives for free, and
  the sizing questions the survey feeds (`docs/design/memory.md` §8) are about the top-level map.
  Consequence: a shop whose width lives under a `k8s`/`labels` map reads as narrow on the
  distinct-key gauges and wide only on the nested ones — read the two together.
- **A batch's `Scope` passes through `shape` untouched, unlike its `Resource`.** `resource: drop`
  substitutes an empty `Resource` so no resource attribute value leaves the tap, but `Scope` has no
  equivalent: `Transform` has no scope-substitution hook, and adding one that every implementer
  must carry, for this one component, wasn't judged worth it
  ([ADR `shape-observer-component`](adr/shape-observer-component.md)). A scope names an
  instrumentation library rather than carrying payload, so this is a narrow exception to the
  counts-only property, not a hole in it — but an `otlp_in` whose senders put identifying
  information in `Scope.attributes` should know it rides through. `shape`'s *flush* output carries
  no scope at all (a window spans many batches, so there is no single one to keep).
- **Nothing bounds a single `shape` measurement event.** `logit.shape.key_bytes` and
  `.value_bytes` carry one value per top-level key and per string leaf, so an event with ten
  thousand attributes yields a ten-thousand-value `Samples`, bounded only by whatever bounded the
  source event. Every *table* in the component is capped and counted (`max_tracked_keys`,
  `max_tracked_keysets`, the per-window batch cap); the per-event vectors are exempt because
  truncating a width measurement at exactly the widths worth knowing about defeats the instrument.
  The obvious fix, if a tap ever meets a genuinely pathological producer: a per-event value cap
  with a drop counter.
- **`shape` measures `Resource`/`Scope` width as a count only.** `logit.shape.batch.resource_attributes`
  and `.scope_attributes` are attribute counts per batch, with no resource-side equivalent of
  `key_bytes`/`value_bytes`/`nested_maps`. So the per-batch cost `docs/design/memory.md` cares
  about is only half visible: a 20-attribute resource of short enums reads the same as one of long
  ARNs and a nested label map.

## HTTP access logs: nginx, HAProxy, and `http_access`

- **`http_access` has no per-server presets** (2026-09-22). It never learns a server's native
  variable names: the operator maps them onto the canonical names in the server's own log-format
  language, using `docs/http-access-logs.md` as the reference
  ([ADR `http-access-normalization`](adr/http-access-normalization.md) rejected `preset: nginx`).
  Caddy and Traefik are the two servers whose JSON key names can't be chosen at all; for them the
  doc's recipe is a `lua` rename stage (Caddy with a `flatten` ahead of it), which costs a Lua VM
  per worker and a few allocations per event, because `logit` has no native rename component (`set`
  only stamps constants). Revisit: build a native rename, or a preset, if either server turns out
  to matter.
- **`http_access`'s `forwarded: {trust: true}` is all-or-nothing** (2026-09-22). It overwrites
  `client.address` with the *first* hop of `http.request.header.x-forwarded-for`, with no
  trusted-proxy list and no hop count. That's right behind one proxy you control that sets the
  header. Behind several, or behind one that appends to a client-supplied header, the first hop is
  whatever the client wrote. Picking the right hop needs to know which proxies are yours (the
  `set_real_ip_from`/`real_ip_recursive` shape of nginx's realip module), config this component
  doesn't carry yet.
- **`http_access`'s route rules are regex-only, and matched O(rules) per event with no prefilter**
  (2026-09-22). Each `routes:` entry is a regex (or a built-in set, itself one regex) tried in list
  order until one matches: no path-template syntax (`/users/:id`), no prefix trie, no `RegexSet`
  prefilter. For a real deployment's handful of rules this is noise next to `json`'s own parse; for
  dozens of rules on a busy tier it is linear in the rule count on every unmatched path, which is
  exactly the scanner traffic that falls through to `route_other`. No `script/perf` scenario yet
  finds where it starts to matter.
- **`http_access`'s user-agent table is a heuristic bucket classifier, not a parser** (2026-09-22).
  It writes one of `scanner`/`tool`/`crawler`/`browser`/`other`/`none` (or a configured class),
  never `user_agent.name`/`user_agent.version`. `user_agent_rules:` (tried first) or an upstream
  `user_agent.class` (trusted as-is) can pre-empt the built-in table, but it can't be turned off.
  It only sees what the client sent: nikto 2.6.1+ defaults to a browser UA from its own bundled
  list, nuclei randomizes a real browser UA for ordinary HTTP requests, and Nessus uses a real
  Chrome UA matching the scan host's platform. Their unconfigured traffic classifies `browser`, not
  `scanner`, however the table is tuned; the `scanner` row only catches a scanner that identifies
  itself.
- **`http_access` never percent-decodes a path** (2026-09-22). `http.route` is matched against the
  capped `url.path` exactly as it arrived, so a rule matches the encoded form only: `/api/x` and
  `/%61pi/x` are two different paths, and the second may land in `route_other`. Decoding safely
  (overlong sequences, encoded `/`, invalid UTF-8 after decoding) is real code, and would also
  change the recommended `url.original` contract of "the raw request target".
- **A non-UTF-8 value reaching `http_access` is capped in bytes and never classified**
  (2026-09-22). A field arriving as `Value::Bytes` (from any stage that keeps raw bytes, including a
  `lua` stage writing a non-UTF-8 string) has no text to run a regex over. It is still capped (by
  byte length, not characters) and control-byte cleaned, but a bytes `user_agent.original`
  classifies `other` and a bytes `url.path` gets `route_other` (or no route). Best-effort, visible,
  and counted like any other field; never matched.
- **The built-in `crawler` pattern trades `\bbot\b`'s precision for bare `bot/`'s recall**
  (2026-09-22). `\bbot\b` alone misses every crawler whose token runs the word into `Bot/`
  (`DotBot/1.2`, `Discordbot/2.0`, `YandexMobileBot/3.0`), which is what most of the unlisted long
  tail looks like. So bare `bot/` sits beside it (`\bbot/` would add nothing, since a `/` is always
  a word boundary). Cost: anything with `bot/` mid-word classifies as a crawler. `UptimeRobot/2.0`
  was the corpus's one false positive, and `tool` catches it first. A non-crawler product whose name
  ends in `bot` will be called a crawler; a `user_agent_rules:` entry pre-empts it.
- **Not a gap, a consequence: an nginx line with no `$http_user_agent` gets no
  `user_agent.class`** (2026-09-22). By `http_access`'s absent rule, an absent header writes no
  class: absence is silence, not `none` (which a logged-but-empty header gets).
  `demo/nginx/nginx.conf`'s format doesn't log the user agent, so the demo's nginx events carry no
  class while its HAProxy events do. To get one, log `"user_agent.original":"$http_user_agent"`.
- **A pathological Host header can truncate the syslog-bound JSON line -- but nginx's own header-size
  limit turns out to make that hard to actually trigger.** `$host` is unbounded and
  attacker-controlled behind a public IP, while the example's lean `log_format`
  (`examples/nginx/nginx.conf`, originally `access_json_syslog`, now `access_semconv`) sizes its
  fixed fields well under nginx's syslog message cap. Measured against nginx 1.31.4 (workstream F,
  `docs/plans/nginx-integration.md`): under default settings an oversized `Host` never reaches
  nginx's syslog writer. `large_client_header_buffers` (4 8k by default) rejects any request whose
  request line plus headers exceed ~8180 bytes with a 400 *before* nginx builds a log line, and
  every `Host` value nginx will log (measured up to 8180 bytes) produced a complete, untruncated
  datagram that parsed cleanly. That contradicts the assumption, from older nginx source naming a
  1024-byte `NGX_SYSLOG_MAX_STR`, that a several-KB `Host` would truncate the line: whatever nginx's
  current per-datagram cap is, it sits above the request-header limit that gates this vector.

  nginx's defaults close this door, not `logit`. A line can still arrive truncated (a larger
  `large_client_header_buffers`, a different unbounded field, a different syslog client), so the
  degradation was verified with a hand-crafted truncated datagram sent straight to `syslog_in`:
  `syslog_in` accepts the truncated-but-valid-UTF-8 datagram; `json` fails to parse the body,
  reports a throttled `parse_failure` diagnostic (`crates/logit-transforms/src/json.rs`), and passes
  the event through with `attributes` unchanged (only `syslog.*` metadata survives);
  `nginx_metrics` derives nothing field-based (only the fieldless `nginx.requests` counter, which
  always fires, increments); sibling requests are unaffected. No nginx-side mitigation (such as
  capping `$host`'s logged length) is added: the pipeline already degrades gracefully, and capping
  a field nginx allows up to 8KB solves a problem the design doesn't have.

  The other two halves of the `$host` exposure are closed (2026-09-16, 2026-09-22). *Cardinality:*
  `nginx.conf` serves exactly two vhosts, but a junk `Host` used to become its own unbounded series
  in `aggregate` and InfluxDB; `examples/nginx-to-influxdb.yaml`'s `bounded` component
  (`keep_values`, `docs/adr/value-allowlist-cardinality-clamp.md`) now clamps it to the two real
  vhosts ahead of `aggregate`, folding anything else into one `other`-tagged series. It clamps
  `server.address` now, not `host`. *Length:* the lean format logs `$host` as `server.address`, and
  `http_access` ([ADR `http-access-normalization`](adr/http-access-normalization.md)) caps it at 253
  characters (`max_length: {server.address: 253}`, `CAPPED_FIELDS`' default), so an over-long value
  never reaches a tag or span attribute at full size, whatever let it past nginx. The truncation
  finding is unchanged: the cap runs after `json` parses the line, so a datagram truncated in
  transit still fails `json` as described.

- **HAProxy's native CBOR log output (`%{+cbor}o` / `%{+cbor,+bin}o`) was investigated twice and
  deliberately not pursued — a closed door now, not a "not now", recorded so it isn't reopened
  without new evidence.** Net: a second decoder to build and maintain, a `syslog_in` framing knob,
  and a rename stage, for a ~15% wire saving and no parse-time win. The first pass (2026-09-13)
  deferred it on framing grounds; the second (2026-09-17) captured real HAProxy 3.0.27 output,
  built a throwaway decoder, and measured. Findings, most surprising first:
  - **Binary CBOR *is* reachable on both transports, once the flags are spelled right.** HAProxy's
    log-format options are comma-separated: `%{+cbor,+bin}o`. `%{+cbor+bin}o` applies only the last
    flag (`bin` alone, plain unencoded output) and `%{+bin+cbor}o` only `cbor` (the hex form):
    `parse_logformat_node_args` in HAProxy's `src/log.c` resets its start pointer at every `+`, and
    the manual never says so. With the comma form, HAProxy 3.0.27 emitted raw binary CBOR in the
    syslog MSG both to a plain UDP `log` target and to a `ring` with
    `server ... log-proto octet-count`. Over TCP octet-counting that MSG already reaches `logit`
    intact as a `Value::Bytes` (see "a non-UTF-8 syslog MSG decodes to a `Value::Bytes` event" under
    [syslog](#syslog)); over UDP it would need a `syslog_in` opt-out of newline splitting, a
    one-field change that was never the hard part.
  - **Wire shape**, should anything decode it: an indefinite-length map (`BF … FF`) with definite
    text keys; *untyped* string items such as `%HM` come out as indefinite-length *chunked* text
    strings (`7F 63 'GET' FF`), `:str`-typed ones as definite strings; `:sint` non-negatives are
    major type 0; `:bool` is simple true/false; no tags.
  - **Size: 13–19% smaller than JSON, not more.** The demo's 20-item HAProxy access line, same items
    from one HAProxy run: 588 bytes as `%{+json}o` (HAProxy pads after `:` and `,`), 549 bytes as
    the compact hand-written JSON `demo/haproxy/haproxy.cfg` emits, 476 bytes as `%{+cbor,+bin}o`.
    The hex form is 2× the binary, so larger than JSON.
  - **Parse speed: no faster, measured.** A hand-rolled CBOR-to-attributes prototype (a twin of
    `json`: zero-copy `Bytes` slices for definite strings, the same `KeyCache`, indefinite
    maps/strings, an explicit depth bound) benched against `JsonParser::process` on those two
    payloads, pinned to one core with divan's allocation profiler on, three runs: JSON 881–921
    ns/event, CBOR 1030–1049 ns/event, 1.1–1.2× *slower*. Allocations were equal (one: the
    `AttrMap` spilling past its 8-entry inline capacity at 20 attributes) once the chunked `%HM`
    string was typed `:str`; as HAProxy emits it, CBOR costs two more for the chunk concatenation.
    The per-entry budget is dominated by what both formats share (key-cache lookup, `Value`
    construction, sorted `insert_sym`, UTF-8 validation, refcount bumps); the syntax scanning CBOR
    saves is a small slice that `serde_json`'s tuned scanner already spends well. A tuned decoder
    could plausibly close the gap; nothing in the profile suggested a meaningful lead. The
    prototype was not kept in tree.
  - **The item-name grammar limitation is shared with `%{+json}o`**, and is why the demo
    hand-writes its JSON: HAProxy rejects a literal `.` in a custom item name (confirmed with
    `haproxy -c`, `demo/haproxy/haproxy.cfg:99-117`). A CBOR-sourced tier would still need a rename
    stage for its `span.*`/`trace.*` keys (`SpanLiftConfig` in `crates/logit-config` has no
    source-field override, so `trace_context` can't absorb it), and CBOR has no hand-written escape
    hatch because binary can't be typed into a `log-format` string. That is HAProxy's grammar, not
    CBOR's; CBOR text keys take dots fine.

  A `cbor_in`/`cbor_out` listener/sink pair was weighed in the same pass and rejected outright:
  nothing in the telemetry landscape speaks CBOR over a socket (Fluent forward is msgpack, Vector's
  native wire is protobuf, syslog is text), so it would be a second native wire beside
  `logit_in`/`logit_out` with no producer or consumer. If new evidence reopens this, the first
  pass's decoder constraints still hold, and the prototype confirmed each costs real code:
  `Value::as_str` **panics** on an invalid-UTF-8 `Value::Str` (`crates/logit-core/src/value.rs`),
  so CBOR's only-nominally-UTF-8 text strings need validation first; a hand-rolled reader needs an
  explicit recursion bound, which `json`'s `serde_json`-based one inherits for free; a length
  header must never size an allocation directly; and tag 1 (epoch time), decodable straight into
  `Value::Timestamp`, is the one thing the format offers that JSON doesn't. HAProxy doesn't emit it.

- ~~**An nginx access line carrying invalid UTF-8 was lost whole — at `json`,
  not at `syslog_in`.**~~ **Closed (2026-09-22).** nginx's `escape=json` escapes `"`, `\`, and
  control bytes but passes bytes `>= 0x80` raw, so a Latin-1 `User-Agent`, or a percent-*decoded*
  path logged via `$uri`, puts invalid UTF-8 into an otherwise valid-looking JSON line. `syslog_in`
  never dropped it: it decodes the MSG as `Value::Bytes` (see "a non-UTF-8 syslog MSG decodes to a
  `Value::Bytes` event" under [syslog](#syslog)). `json`'s strict parse failed it (one throttled
  `parse_failure`, the event passed through with none of its fields), the one failure `http_access`
  can't reach from behind `json`. `json` now takes `invalid_utf8: replace`
  ([ADR `http-access-normalization`](adr/http-access-normalization.md)): on the failure path only,
  a parse that failed on invalid UTF-8 is retried on a copy with every invalid sequence replaced by
  U+FFFD, and a rescued line reports `logit.component.diagnostics {key="invalid_utf8"}`. **Still
  open:** the default stays `reject`, so a pipeline must opt in; the replacement is lossy (the
  original bytes don't survive); and `docs/http-access-logs.md` tells nginx users to log
  `$request_uri`, never `$uri`, which removes the decoded-path source but not a client's own
  non-ASCII header bytes.

## Lua

- ~~**Lua has no span API at all**~~ — ~~**narrowed to span writes/minting from Lua.**~~ —
  **narrowed again (2026-09-15) to in-place span mutation from Lua.** A script can't *mutate* an
  existing `event.span` field by field; the documented way is `Event.new(event:to_table())` with
  the table edited. Reading and minting both work: `event.span` (`docs/design/lua-api.md`'s
  "Reading `event.span`") is a read-only proxy over every `SpanRecord` field, `events`/`links`
  tables included, and `Event.new` ([ADR `lua-event-constructor`](adr/lua-event-constructor.md), the
  `span` table in `lua-api.md`'s "Constructing events") builds a whole span, so `trace_context`'s
  `span:` block ([ADR `trace-context-span-lifting`](adr/trace-context-span-lifting.md)) is no
  longer the only way to turn a log line into a span. An in-place write path would share the
  constructor's parsers; it's a small follow-up, not designed yet, the same posture as in-place
  `event.log.message`/`severity`/`body_format` writes.
- ~~**A Lua component's `flush()` has no resource or scope of its own at a timer tick**, and sees
  a stale trace context and stale provenance, all for the same reason: its globals kept whatever
  the most recently processed batch set.~~ **Closed** ([ADR
  `lua-flush-root-context`](adr/lua-flush-root-context.md)). A Lua `flush()` runs in a root
  context: before every call, `trace` is the fresh root the emission is sent under, `provenance` is
  this component as both `origin` and `previous`, `resource` is empty, and `scope` is none. A
  `resource`/`scope` write inside `flush()` is the one way a flush-driven emission carries either.
  **Still open, by the ADR's design:** `logit` never attributes a flush to the batches that fed it
  (there's no accumulator to inspect, unlike `Transform::flush`'s linking — see "Internal spans"
  under [Internal telemetry and self-logging](#internal-telemetry-and-self-logging)), so a script
  that wants that relationship tracks contributing contexts itself inside `process()`.
- ~~**A benchmark of the event proxy against plain table conversion is still outstanding**~~ —
  **Closed.** `crates/logit-bench/benches/pipeline.rs` (`lua::proxy` vs `lua::to_table`) shows the
  proxy is faster, more so for scripts that read few attributes, since `to_table` converts
  everything. The design commitment in [lua-api.md](design/lua-api.md) stands, now with a number
  behind it.

  The same measurement found the boundary costing 21 allocations per event (a `_G` lookup of
  `process` per event, a fresh `AttrsProxy` userdata per attribute access, a Rust `String` per
  metamethod key); **also closed**, 21 → 9, by caching `process`/`flush` as an
  `mlua::RegistryKey` resolved once at load, caching the `AttrsProxy` userdata per event, and
  taking `mlua::String` instead of an owned `String` in both metamethods. Two edge cases the
  caching opened are closed too: a script that stashes `event.attributes` past the point its event
  is returned fails loudly in this crate's own voice (not mlua's raw error), and a `flush` global
  that isn't a function is a load-time error, matching `process`'s `MissingProcess` treatment,
  instead of silently meaning "no `flush()`". Both are documented in
  [lua-api.md](design/lua-api.md); [memory.md](design/memory.md)'s recommendations have the full
  write-up.

## Internal telemetry and self-logging

- **Internal telemetry ([internal-telemetry.md](design/internal-telemetry.md),
  [ADR `internal-telemetry-as-pipeline-events`](adr/internal-telemetry-as-pipeline-events.md))
  covered metrics only in its first cut; of the three extensions deferred then, spans and logs have
  landed and `host_metrics` hasn't.** The framework (the `internal` component, the per-component
  buffer, the emit API) is built to extend.
  - **Internal spans — emission, sampling, and export are built and proven end to end; five
    narrower residuals remain (below).** `internal`'s name (not `internal_metrics`) left room for
    this without a rename. How it got here:
    - *Costing.* Node-runtime coverage in `crates/logit-bench/tests/allocations.rs`/
      `benches/pipeline.rs` measures what `run_transform`/`run_output`/`Fanout::send` cost,
      including the *first* call after every `internal` drain (`ComponentBuffer::drain`'s
      `mem::take` re-populates the buffer rather than updating it, a recurring cost the first pass
      missed). A throwaway `TraceContext` prototype on `Delivered`, measured against
      [ADR `minimize-allocations-over-event-size`](adr/minimize-allocations-over-event-size.md)'s
      gate, showed zero allocation change, `size_of::<Delivered>()` 32 → 56, and no attributable
      throughput regression (`docs/design/memory.md`'s "Runtime" and "Costing internal spans").
    - *Propagation.* [ADR
      `trace-context-propagation-on-delivered`](adr/trace-context-propagation-on-delivered.md):
      `Delivered` always carries a `TraceContext`. `Transform::process`/`ScriptWorker::process`
      (non-flush path, one incoming batch per emission) propagate via `Fanout::send_with_context`,
      and `run_output` already borrows the incoming `Delivered`. `Transform::flush`/`Aggregator`
      keep a bounded, best-effort `ContributingContexts` set per series
      (`MAX_CONTRIBUTING_CONTEXTS_PER_SERIES`, 8; overflow dropped and counted as
      `logit.transform.links.dropped{reason="cardinality"}`) and pair each flushed `Event` with the
      resulting `SpanLink`s. Lua's `flush()` has no inspectable accumulator, so it runs in a
      link-less root context ([ADR `lua-flush-root-context`](adr/lua-flush-root-context.md)), with
      `trace.trace_id`/`trace.span_id` (`docs/design/lua-api.md`) exposed to the script's own
      `process()`. Picking an arbitrary contributing batch as "the" parent was rejected: silently
      wrong is worse than visibly incomplete.
    - *Emission and sampling.* [ADR
      `internal-span-emission-and-deterministic-sampling`](adr/internal-span-emission-and-deterministic-sampling.md):
      `Telemetry::span`/`SpanGuard` (mirroring `Timer`'s disabled-is-free shape) turn a
      `(context, node, batch)` visit into a `SpanRecord`-carrying `Event`, drained by
      `ComponentBuffer::drain`'s span pass beside the metric pass — the "`ComponentBuffer`/drain
      turns counters into events" shape ADR `internal-telemetry-as-pipeline-events` named. `run_flush`
      now mints **one** root before `transform.flush(now)` and sends every resource group under it
      (`Fanout::send_with_own_context`): one flush is one unit of work, not *N* hops. Sampling is
      deterministic on `trace_id` (`trace_is_sampled`, `ComponentKind::Internal::span_sample_rate`,
      default 0.1), so every node reaches the same verdict with no propagated bit and no growth to
      `TraceContext`/`Delivered`. See `docs/design/internal-telemetry.md`'s "Spans" section and
      `docs/design/pipeline-graph.md`'s "Trace context propagation" table.
    - *Export proof.* [docs/plans/otlp-end-to-end.md](plans/otlp-end-to-end.md) (the OTLP series'
      fourth PR), not ADR `internal-span-emission-and-deterministic-sampling` alone, closes the
      item: `otlp_out` (ADR `committed-pregenerated-otlp-protobuf`'s codec, ADR
      `hand-rolled-grpc-over-hyper`'s gRPC transport) exports `internal`'s spans from
      `demo/logit.yaml`'s `tempo_out` to a real Tempo over OTLP/gRPC, and Grafana shows the
      listener-root, transform/sink-child tree matching `pipeline-graph.md`'s table: a span leaves
      the process, decodes correctly, and reconstructs the right shape.

    **What `span_sample_rate` does** (default `0.1`, `1.0` in the demo): decides once per
    `trace_id` whether that trace's internal spans exist inside `logit` at all. An unsampled trace
    never becomes a `SpanRecord`, never takes a slot in the bounded per-component buffer, and costs
    only the sampler's branch (`Telemetry::span`'s doc comment). It's a volume control on `logit`'s
    self-observability, independent of the traffic. **What it doesn't do:** sample events (a
    dropped trace's events still reach every sink untouched; traffic sampling is the `sample`
    transform's job, ADR `consistent-sampling-component`, which hashes its key rather than reading
    raw trace-id bits and so owes this sampler no agreement); propagate across a peer (no `sampled`
    flag crosses `otlp_in`/`otlp_out`, so a downstream `logit` or other OTLP consumer decides
    independently on the same `trace_id`, per ADR
    `internal-span-emission-and-deterministic-sampling`'s "no propagated bit"); or thin metrics
    (`internal`'s point-side buffer and `otlp_out`'s metrics encoding ignore it, which is why the
    demo's InfluxDB dashboard looks the same at `0.1` or `1.0`).

    **Still open, deliberately:**
    1. **The listener span's window is the `send` call only, not decode-to-send.** `Fanout::send`
       can't see how long a listener spent building the batch (`Input::run` is a free-form loop) —
       the still-open listener-side half of "delivery I/O is not decoupled from event processing".
    2. **Lua `flush()` still gets a link-less root.** It gets a real span (ADR
       `internal-span-emission-and-deterministic-sampling`) but no links, since the Lua side has no
       accumulator to inspect. Its script-visible side is settled
       ([ADR `lua-flush-root-context`](adr/lua-flush-root-context.md)).
    3. **A `SinkQueue` entry is 24 bytes larger.** `TraceContext` rides inline in every queue entry
       (`push`/`peek`) so `write_loop` can parent its sink span on the context the batch arrived
       under — the same size-for-a-span trade `Delivered` made and measured
       (`docs/design/memory.md`'s "Costing internal spans").
    4. **`service.name` is the only resource identity `internal` sets**
       (`docs/design/internal-telemetry.md`'s "Resource identity"). Its `Resource` has no
       `host.name`/`service.instance.id`, the semconv-correct way to tell `logit` instances apart
       in Tempo, because nothing in the workspace reads the OS hostname
       (`SyslogEncoder::default_hostname`, `crates/logit-outputs/src/syslog.rs`, is
       config-supplied). Deferred until that dependency exists, rather than added as a one-off.
    5. **The demo's Tempo service graph panel is empty.** It needs Tempo's `metrics_generator`
       (`service-graphs`/`span-metrics` processors) with `remote_write` to a Prometheus-compatible
       store, that store as a Grafana datasource, and `serviceMap.datasourceUid` on the Tempo
       datasource; `demo/compose.yaml`/`demo/tempo/tempo.yaml` have none of it. A real
       multi-service trace now exists to draw (`docs/plans/demo-tracing-stack.md`'s HAProxy →
       nginx → app chain plus [ADR `trace-context-span-lifting`](adr/trace-context-span-lifting.md)'s
       `span:` block: `haproxy`/`nginx` access spans and `demo-app`'s own OTel span). Deferred as
       extra stack pieces for one panel, not a `logit`-side gap, but worth doing now. Whether
       `logit` should compute a service graph itself as a component is unexplored, no decision made.
  - ~~**Internal logs** — routing `Diagnostics`' stderr output into the graph as `LogRecord` events
    is the natural next layer, and what the still-deferred `tracing` migration should build on
    rather than duplicate.~~ **Closed** (`docs/plans/operator-surface.md`, workstream D).
    `logit_core::telemetry::TelemetryLayer`, a `tracing_subscriber::Layer`, captures every
    `logit`-targeted `tracing` event at or above `internal.logs`'s threshold (`warn` by default,
    `error`, or `off`) into the same per-component buffer as points and spans, emitted as ordinary
    `LogRecord`-carrying `Event`s. See `docs/design/internal-telemetry.md`'s "Logs" section for the
    emit path and the bound (`MAX_LOGS_PER_COMPONENT`, 256, dropped and counted past the cap like
    spans).
  - **`host_metrics`** — facts about the machine (CPU, disks, NICs) aren't built. They're a
    different kind of source than `internal`: read from the OS rather than `logit`'s own counters,
    with their own config and failure modes an in-process atomic read never has. A separate
    component kind when it lands, not a field on `internal`.

- **A component's internal-telemetry buffer caps distinct `(name, tags)` keys at 1024**
  (`MAX_KEYS_PER_COMPONENT`, `crates/logit-core/src/telemetry.rs`), and the cap isn't
  configurable. It bounds a component that ignores the tag-cardinality convention (`&'static str`
  values only) instead of letting it grow the process-wide interner. A dropped key is counted
  (`logit.internal.points.dropped{reason="cardinality"}`), never silent. Revisit if a legitimate
  component ever needs more than 1024 distinct points between drains.
- **Lua-authored telemetry (`crates/logit-script/src/telemetry.rs`,
  [ADR `lua-authored-telemetry-cardinality`](adr/lua-authored-telemetry-cardinality.md)) trades the
  type-system cardinality guarantee the rest of `internal-telemetry.md` relies on for a
  convention-enforced one.** A script's metric name/tag value goes through the process interner
  instead of being a Rust `&'static str`, so a script that builds one from per-event data leaks the
  interner one entry at a time. Accepted for the same reason as "The attribute/metric-name interner
  never frees" (under [Event model and interner](#event-model-and-interner)): bounded in the
  intended, documented use (a fixed literal in the script's source). The fix if that stops holding
  (a bounded per-`ScriptWorker` cache instead of the process-wide interner) is recorded in the ADR
  as a considered-and-deferred alternative.
- **The internal-telemetry component survey (`docs/design/internal-telemetry.md`'s worked-examples
  list) found several more candidates not built yet** — each needs more than a
  `telemetry.count(...)` call:
  - **Process-level facts beyond what `internal` already samples.** `logit.process.memory.*` via
    jemalloc heap stats needs a new `tikv-jemalloc-ctl` dependency plus cross-crate plumbing, since
    `crates/logit-inputs` (home of `internal`) doesn't depend on `crates/logit-cli` (home of the
    `jemalloc` feature, `docs/adr/jemalloc-global-allocator.md`). `logit.process.threads`/
    `.fds`/`.cpu.seconds` need Linux-specific `/proc` parsing. Candidate names:
    `logit.process.memory.allocated`/`.resident`, `.threads`, `.fds`, `.cpu.seconds`.
  - **`json`'s parse-outcome counts.** Its two failure modes (`no_brace`, `parse_failure`) already
    reach `logit.component.diagnostics{key=...}` through the `Diagnostics` bridge, so a dedicated
    metric would mostly restate them.
  - **`logit-proto`'s `frame.rs` metrics** — listed as still a stub with nothing to instrument (see
    [Native wire format, `logit_in`/`logit_out`, and
    buffering](#native-wire-format-logit_inlogit_out-and-buffering)). Candidate names, pre-committed
    so the builder needn't re-derive them: `logit.proto.frames{direction,codec,compression}`,
    `logit.proto.frame.bytes`, `logit.proto.errors{reason="magic"|"version"|"crc"|"truncated"}`.
    (`buffer.rs`'s metrics are done, at the `SinkQueue` layer, `docs/adr/buffered-sink-delivery.md`:
    `logit.component.buffer.batches`/`.bytes`/`.utilization`/`.push.blocked.duration` and new
    `reason` values on `batches.dropped`/`events.dropped`, per
    `docs/design/internal-telemetry.md`'s catalog.)
  - **Lua per-call latency, error classification, flush-tick-empty tracking.** A per-event
    `ScriptWorker::process` timing distribution would isolate one pathological event in a big
    batch (`logit.component.process.duration` is whole-batch) at the cost of a clock read per
    event. `ScriptError`'s `MissingProcess`/`Lua(...)`/malformed-return cases collapse into one
    `errors{reason="process"}`, though a script bug (a malformed `flush()` return) is a different
    signal than a runtime error. Each is real; none was a default yes.
- **Internal-log sampling is the existing per-key occurrence throttle, nothing finer.**
  `Diagnostics::warn_throttled`'s powers-of-two throttle bounds a chatty diagnostic before it
  reaches `tracing`; `TelemetryLayer` adds no sampling or rate limit after that. A component that
  logs at `warn`/`error` outside the throttle (a lifecycle event, an unthrottled
  `Diagnostics::error`) is bounded only by `MAX_LOGS_PER_COMPONENT`'s bound-and-drop. Not built:
  nothing shipped needs it, and the throttle already covers the hot path (a malformed line, a parse
  failure).
- ~~**`eprintln!` instead of a real diagnostics facility** — every component's diagnostic now goes
  through `logit_core::diag::Diagnostics`, which closes the two concrete hazards this entry used to
  name: every message is prefixed with its component's id, and a message that can fire once per
  event under normal operation is throttled by occurrence count rather than printed unbounded.
  What's still missing is the real thing: severity levels, structured fields, filtering — a full
  `tracing` migration, deliberately kept as separate, later work rather than folded into this
  narrower fix.~~ **Closed** ([ADR `tracing-for-self-logging`](adr/tracing-for-self-logging.md)).
  `Diagnostics::warn`/`warn_throttled` emit through `tracing::warn!` with `component` and `key` as
  structured fields; new `info`/`error` cover unthrottled lifecycle messages; `logit run` gains
  `--log-level`/`LOGIT_LOG` and `--log-format text|json`. `grep -rn 'eprintln!' crates/*/src` names
  only `logit-cli/src/main.rs` — `Command::Run`'s exit-error printer, `Command::Graph`'s
  validation warning, and `Command::Ready`'s probe failure: a CLI's own stderr on its own error
  paths, not a running service's self-log.

## Load-test harness and perf tooling

- **`script/perf compare` has no cross-run noise model.** It diffs two results files' medians
  directly against `--threshold`, ignoring the run-to-run variance each file's own `repeats:`
  already show. A scenario whose repeats spread more than `--threshold` can trip a "regression" on
  scheduling luck, and a real regression smaller than its noise floor can pass silently. `compare`
  warns on a host/CPU-model mismatch but not on "this scenario's own repeats disagree by more than
  the threshold you're gating on". Still a gap in principle, but its motivating case is gone: on the
  laptop, [`docs/design/performance.md`](design/performance.md) §1's noise sub-section found
  `aggregate` spreading roughly ±25% between repeats even solo on an idle machine (`buffered` in
  the same ballpark; see the `buffered` entry in this section), while `passthrough`, `json-parse`,
  and `lua` stayed far tighter — so a 5% threshold flagged `aggregate` on its ordinary variance. **That does
  not reproduce on the disposable perf VM** (2026-09-20): two independent 5-repeat samples of
  `aggregate` agree within 0.7%, within one run and across runs an hour apart. Candidate fixes
  (gate `compare` on each file's `min`, or a per-scenario threshold) stay unbuilt until a scenario
  needs them.
- **`buffered`'s events/s was the least reproducible number this harness reported; the harness-side
  fix has landed (#165) and is now confirmed on a quiet machine — resolved, with one
  product-side question left open, tracked below.** **Still open:** whether `DiskQueue::open`'s
  unchanged double-read startup scan (`crates/logit-pipeline/src/disk_queue.rs`) accounts for any
  remaining spread. Nobody has picked it up, and on the VM's tighter numbers it matters less than it
  looked on the quiet laptop.

  The scan: at every startup `DiskQueue::open` reads and CRC-walks the *active* segment in full to
  check for a torn tail (bounded by the default `segment_bytes` rotation threshold, 64MiB — not
  obviously enough alone to explain a multi-second swing), then reads every segment at or after the
  read cursor a *second* time to count what's left to replay — real work whenever the cursor lags
  the end of what's on disk, and a first-open cost even on a *cleared* spool.

  The cause that was fixed: `perf/scenarios/buffered.yaml`'s spool (`perf/results/spool/`,
  gitignored) accumulated uncleared across every repeat and invocation. A solo `script/perf run
  --repeat 5 --scenario buffered` against a leftover spool degraded monotonically, 134k → 75k → 53k
  → 38k → 27k events/s, with peak RSS climbing 56 → 110 MiB; deleting `perf/results/spool/` first
  gave the same shape from a higher start (615k → 939k → 289k → 126k → 80k), because repeats shared
  one spool directory too. Full account, with this run's numbers, in
  [`docs/design/performance.md`](design/performance.md) §3.

  The fix (#165): `script/perf run`/`attribute`/`flamegraph` clear every `buffer.disk.path`
  directory a scenario declares before each spawn — every repeat, not once per invocation
  (`crates/logit-perf/src/spool.rs`) — and refuse to remove anything outside `perf/results/`.
  Measured after the fix, solo `--repeat 5` (`startup_s` is spawn → `ready`):

  | Box | events/s | CPU µs/event | Peak RSS | `startup_s` |
  |---|---|---|---|---|
  | Busy laptop | 885k → 510k → 838k → 863k → 792k | — | 24.8–30.6 MiB | 2.6–4.4 ms |
  | Quiet, idle laptop | 632,897 → 659,892 → 641,560 → 780,888 → 873,406 | 2.63 → 2.53 → 2.57 → 2.04 → 1.84 | 25–28 MiB | ~4 ms |
  | Perf VM (2026-09-20) | 745,047 → 757,703 → 802,478 → 780,068 → 801,518 | 1.720 → 1.710 → 1.710 → 1.709 → 1.708 | 70.7–79.0 MiB | — |

  No monotonic decay and no RSS climb on any of them; the VM run is the tightest this scenario has
  measured (about 7% events/s spread, under 1% on CPU µs/event). `script/perf attribute --scenario
  buffered` on the quiet-laptop build put the constraint downstream of `gen`, in the disk queue's
  write/read path, not the harness: `gen` sent and `out` received all 1,200,000 events, `gen` spent
  1.3959s blocked in `send`, and neither the sink nor the listener show process time of their own.

- **`generate_in`'s `rate:` pacing is millisecond-granular above roughly 1k batches/s.** The
  wall-clock catch-up loop (`due = elapsed * rate`; sleep until `start + (sent+n)/rate` when ahead)
  can't subdivide one OS sleep below about 1ms, so a `rate` above roughly 1,000 batches/s (above
  ~100k events/s at the default `batch: 100`) is accurate on average but bursty within each
  millisecond. Named as a risk at design time ([ADR `load-test-harness`](adr/load-test-harness.md),
  `docs/plans/load-test-harness.md`). Nothing shipped is affected: no `perf/scenarios/*.yaml` sets
  `rate:` (every scenario measures unthrottled, backpressure-only throughput).
- **`RunReport.box_state` records nothing on the disposable perf VM.** `logit-perf run`'s
  best-effort governor/EPP/platform-profile/AC-power probe (`crates/logit-perf/src/result.rs`)
  returns an empty `{}` on every Azure guest, where that sysfs surface doesn't exist — correct for
  what it checks, but the results JSON, now the primary provenance record for every recorded
  number, captures nothing about the box beyond `hostname`/`cpu_model`/`nproc`. With the VM as the
  reference box (`docs/adr/disposable-azure-perf-vm.md`), `BoxState` should also record THP setting
  (`/sys/kernel/mm/transparent_hugepage/enabled`), `net.core.rmem_max`/`rmem_default`
  (`/proc/sys/net/core/`), and vCPU topology (`vCPUsPerCore` — from IMDS, or `nproc` alongside
  `/proc/cpuinfo`'s core-id fields). Each changed a finding this effort measured (THP flips the
  `read_batch` RSS story; `rmem_max` decides whether a receive buffer clamps), and `compare` could
  then warn on them as it does for a hostname/CPU-model mismatch. Needs a code change
  (`crates/logit-perf/src/result.rs`'s `BoxState`, plus a `compare.rs` warning); not built.
- **When and how the load-test harness runs in the ongoing development process is deliberately
  undecided.** Nightly, manually triggered, a PR gate on a `compare --threshold` regression, or
  another cadence is open future work ([ADR `load-test-harness`](adr/load-test-harness.md)'s "Open
  question" section). The harness is built and runnable by hand; nothing wires it into CI, a
  pre-merge gate, or a schedule.
