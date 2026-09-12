# Known gaps

Deliberate, already-identified gaps that don't block whatever they were found alongside, tracked
here so they don't only live in an untracked scratch file or someone's memory. Each one either has
a `todo!()`/doc-comment pointer at its actual location too, or is cheap enough to describe fully
here. Not a roadmap — see [OVERVIEW.md](OVERVIEW.md) for planned scope; this is specifically things
already built that have a known, accepted rough edge.

- **Predicate-shaped work (routing by condition, sampling, throttling, dedup) costs a Lua VM, an OS
  thread, and roughly 9× the per-event allocations of a native transform, because `logit` has no
  native predicate language** — a deliberate choice, not an oversight;
  [ADR `routing-by-condition-is-lua`](adr/routing-by-condition-is-lua.md) has the full account and
  the measured numbers. Concretely: **9** allocations / **1.07 µs** per event through a `lua`
  component versus **1** allocation / **360 ns** through a native `Transform`
  (`docs/design/memory.md`), and one dedicated OS thread plus one LuaJIT VM per `lua` node
  (`crates/logit-pipeline/src/runtime.rs`'s `run_with_telemetry`) versus an ordinary tokio task.
  Fine at sidecar/host-agent volume — the delta is noise below roughly tens of thousands of
  events/sec — and real at central-collector volume, where a multi-branch routing diamond can cost
  a measurable fraction of a core answering what a native transform would answer for a third of
  that. The ADR names the explicit revisit trigger: sustained, *measured* central-collector
  throughput pressure against a real config, not a hunch — and records a substantially-designed
  native predicate grammar (total-by-construction, so it can't fail at runtime) as where to resume
  if that trigger fires. **Narrowed on 2026-09-10:** that trigger fired for the equality-only
  subcase of routing by condition (splitting a `logit_in` fan-out back apart by an attribute an
  upstream `set` stamped) — `has_attributes`/`drop_attributes`
  ([ADR `attribute-filtering-components`](adr/attribute-filtering-components.md)) answer exactly
  that shape natively. Sampling, throttling, dedup, and anything needing an actual operator
  (`>=`, `contains`, cross-attribute comparison) remain Lua-only; the gap above still applies to
  them unchanged.
- ~~**`HyperLogLog` is real now; statsd still has no producer for it.**~~ — **closed, both halves,
  as of W3.** [`docs/plans/lossless-transit.md`](plans/lossless-transit.md)'s W2 gave `HyperLogLog`
  (`crates/logit-core/src/metric.rs`) a real implementation wrapping the `cardinality-estimator`
  crate — merge (union), `estimate()`, and a canonical `to_bytes`/`from_bytes` pinned to that
  crate's version, no longer a method-less placeholder. `logit-transforms::Aggregator` really
  merges `MetricKind::SetMembers` into a `Set` (`sets: estimate`, the default) or retains an exact
  deduplicated member set (`sets: members`, bounded by `max_set_members_per_series`, falling back
  to an estimate on overflow) — see [ADR `aggregation-window-semantics`](adr/aggregation-window-semantics.md)'s
  amendment for the full design; `logit-outputs::influxdb` renders a `Set`'s estimate as a `value=`
  field instead of erroring, and `logit-outputs::stdio` renders `set=<estimate>`. W3 closed the
  other half: statsd's `s` (set) metric type is no longer a decode error —
  `crates/logit-inputs/src/statsd.rs` decodes `s` straight to `MetricKind::SetMembers`, one event
  per line, every member a zero-copy datagram slice, and `crates/logit-outputs/src/statsd.rs`
  encodes it back as one `name:<member>|s` line per member — `SetMembers`/`Set`'s own producer, the
  same way `ms`/`h`/`d` already produce `Samples`/`Distribution`. See
  [ADR `statsd-output`](adr/statsd-output.md)'s amendment.
- **`HyperLogLog::from_bytes` (`crates/logit-core/src/metric.rs`) works around an upstream
  allocation-layout bug in `cardinality-estimator` 1.0.3, not just a byte-shape mismatch.** That
  crate's `Array::from_vec` rounds a deserialized `Vec<u32>`'s length up to the next power of two,
  `resize`s to it, then later frees the representation with a `Box::from_raw` sized to that rounded
  length -- if the `Vec` handed in carries more spare capacity than the rounded length (routine when
  serde's blanket `Vec<T>` deserializer allocates with the *unrounded* element count as its
  `Vec::with_capacity` hint), the eventual dealloc uses the wrong `Layout`: undefined behavior,
  reachable through ordinary native `METRIC_SET` decoding, not just a crafted blob. `HllBytesReader`
  fixes this on our side by reporting the already-rounded capacity as `serde::de::SeqAccess::size_hint`
  for the members list (so the initial allocation is already the size the crate will settle on) and
  by bounding the claimed member count before allocating at all. See `HyperLogLog`'s own doc comment
  and `HllBytesReader`'s (same file) for the full mechanism, and
  `hyperloglog_round_trips_non_power_of_two_member_counts` for the pinning test. Pinned to
  `cardinality-estimator` 1.0.3; the upstream fix would be `into_boxed_slice`/`shrink_to_fit` in
  `Array::from_vec` so the freed layout always matches the `Vec`'s own capacity by construction.
- **Native wire protocol: the format and the transport are both done; credit-based flow control,
  QUIC, and an OTLP passthrough codec aren't.** `crates/logit-proto/src/frame.rs`/`src/native/`
  (the codec, [ADR `native-wire-format-encoding`](adr/native-wire-format-encoding.md)) and
  `logit_in`/`logit_out` (the connection layer, [ADR
  `native-transport-handshake-and-ack`](adr/native-transport-handshake-and-ack.md)) are real,
  tested, running `ComponentKind`s now. What's left, genuinely open:
  - **Credit-based flow control (`window` > 1)** — `Hello`/`HelloAck` already negotiate and record
    a `window`, but the sender only ever has one frame outstanding; several in-flight frames
    acknowledged out of order needs `logit-pipeline`'s `SinkQueue` to track more than one
    outstanding batch, a real queue-shape change, not designed yet.
  - **QUIC** — TCP only today; a plausible later transport upgrade, not attempted.
  - **An OTLP passthrough codec** — whether the native protocol should carry OTLP-encoded payloads
    unmodified (a relaying node forwarding OTLP without re-encoding into native) is still an open
    question, `docs/design/wire-protocol.md`'s own "Open question" section.
  - **`cargo-fuzz` targets over the decoders** — `crates/logit-proto/tests/robustness.rs`'s seeded
    mutation suite (truncation, bit flips, inflated lengths, over-depth nesting) covers the same
    ground a corpus-driven fuzzer would, but needs nightly Rust to build at all
    (`docs/adr/containerized-development.md`'s stable-only toolchain), so real fuzz targets are
    deferred, not built.
  - **`logit_in`'s shutdown grace is fixed at 5s, not operator-tunable** — graph validation's rule
    17 rejects a `receive:` block on `logit_in` (it isn't a datagram or tail listener), so it
    always gets `ReceiveConfig::default().shutdown_grace` with no config-level way to change it,
    even though `LogitInput` (unlike `internal`) genuinely uses that grace to close idle
    connections cleanly on shutdown. A real gap if a deployment ever needs a different number, not
    yet a `receive:`-shaped knob.
  - **`otlp_in` can hold the graph open past shutdown.** Every connection `OtlpInput::run` spawns
    holds its own `Fanout` clone but the input never overrides `Input::run_until_shutdown` the way
    `logit_in` now does (`crates/logit-inputs/src/logit.rs`'s own module doc comment) — an idle
    keep-alive HTTP/gRPC connection at shutdown time can leave its `Fanout` clone open
    indefinitely, which the cancel-by-drop shutdown mechanism ([ADR
    `service-lifecycle-and-output-retry`](adr/service-lifecycle-and-output-retry.md)) depends on
    every listener eventually releasing. Not fixed here — `logit_in`'s design is the pattern to
    follow when this is addressed.
  - **`otlp_in`'s TLS accept has no timeout.** `crate::otlp::run`'s `acceptor.accept(stream).await`
    is unbounded, same gap `logit_in` had until this was fixed there: a client that completes TCP
    connect and then sends nothing pins a connection-limit permit forever. `logit_in`'s pattern
    (`LogitInput::handshake_timeout`, wrapping the TLS accept itself in
    `tokio::time::timeout` in its accept loop, not just the post-TLS `Hello`/request read) is the
    one to follow here too. Not fixed for `otlp_in` in the same change — out of scope for the
    finding that fixed it for `logit_in`.
- **Output buffering: closed for the sink side, in-memory only.** `crates/logit-proto/src/buffer.rs`'s
  `Buffer`/`InMemoryBuffer` are implemented (`push`/`peek`/`commit`, `DropOldest`/`DropNewest`), and
  every sink now sits behind a bounded, byte-aware `SinkQueue`
  (`crates/logit-pipeline/src/queue.rs`) that keeps accepting while a delivery attempt is in
  flight or backing off, with retry (`RetryConfig`, up to 60s by default) and fault-classification-
  driven duplicate-safety (`Fault`/`DeliveryPosture`, `crates/logit-pipeline/src/output.rs`) moved
  behind that boundary ([ADR `buffered-sink-delivery`](adr/buffered-sink-delivery.md)). A persistent failure no
  longer ends `logit run` by default — it degrades to dropping the offending batch and continuing,
  exiting only after a sustained ~60s window of nothing but configuration-error (`Fault::Permanent`)
  failures. What's left, genuinely open:
  - **No durable (disk-backed) buffering on the receive side.** Closed for the sink side: an opt-in
    `buffer.disk:` block replaces a sink's in-memory `SinkQueue` with a crash-recoverable spool over
    `logit_proto::native` frames (`crates/logit-pipeline/src/disk_queue.rs`,
    [ADR `disk-backed-sink-buffer`](adr/disk-backed-sink-buffer.md)) — a process restart or `SIGKILL`
    resumes delivery from the last persisted read cursor, replaying at most the batches committed
    since the last checkpoint. A UDP listener's `ReceiveQueue` (since
    [ADR `decoupled-listener-io`](adr/decoupled-listener-io.md)) is still in-memory only; a restart
    or a shutdown grace that expires mid-drain still loses whatever it was holding. The same
    `logit_proto::native` frames this closed the sink side with are available for the receive side
    too, but the design (what a listener resumes *from* has no equivalent of a sink's "haven't
    delivered yet" boundary) isn't started.
  - **The disk-backed sink spool has a real, accepted power-loss window.** Durability is
    `fdatasync` on segment rotation, on the cursor file, and at shutdown — not per push (see the
    ADR's "Durability" section). A power loss (not a process crash) can lose the tail of the active
    segment's most recent, not-yet-`fsync`ed writes. A `disk.sync: every_push` knob to close that
    window at a real throughput cost is a plausible follow-up, not built.
  - **`logit_proto::buffer::Buffer<T>`'s role narrowed to `InMemoryBuffer` alone.** Deliberately
    written ahead of its caller ([ADR `buffered-sink-delivery`](adr/buffered-sink-delivery.md)), the
    trait's sync/`&mut self`/generic shape turned out to be the wrong seam once a second, disk-backed
    implementation actually needed to exist: `DiskQueue` is async and concrete over
    `(Arc<EventBatch>, TraceContext)`, and implements its own surface directly rather than that
    trait ([ADR `disk-backed-sink-buffer`](adr/disk-backed-sink-buffer.md)).
  - **No spool sharing, compaction, or out-of-order replay for the disk-backed sink buffer.** One
    spool directory per sink, no rewriting of already-written segments to reclaim space early
    (deletion only happens whole-segment, once the read cursor has fully crossed it), and no
    encryption at rest. None of these block the at-least-once contract the feature ships with; each
    is real, narrower future work if a deployment needs it.
  - **No end-to-end acknowledgement — one hop further than before, still not the whole path.**
    `logit_out`'s `send` only returns success once `logit_in`'s own `Fanout::send` has accepted
    the batch into every one of *its* downstream inboxes ([ADR
    `native-transport-handshake-and-ack`](adr/native-transport-handshake-and-ack.md)'s "Ack
    point" decision) — a real acknowledgement, not just "the write succeeded" the way every other
    sink here still means it. But that's still only as far as the *next* hop: if `logit_in`
    forwards on to a further sink (another `logit_out`, an `influxdb_out`, ...), nothing tracks
    whether the data survives *that* delivery, and a non-`logit_out` sink's own `send` succeeding
    is still only "the immediate destination accepted the write," never more. The receive-side loss
    this entry used to also name (a UDP listener losing datagrams before anything reaches a
    buffer) narrowed with ADR `decoupled-listener-io`: a listener now counts every datagram it
    drops itself (`logit.component.datagrams.dropped`); what remains uncounted is the kernel's own
    drop, before `logit` ever sees the datagram — see the kernel-drop-visibility entry below.
  - **No out-of-order/credit-based acknowledgement** — see the native wire protocol entry above's
    "Credit-based flow control" bullet; `SinkQueue` is deliberately in-order and single-in-flight
    (one queue, one writer, `peek`-then-`commit`-the-head only) until that lands.
- **No visibility into the kernel's own UDP receive-buffer drops.** A listener's `ReceiveQueue`
  ([ADR `decoupled-listener-io`](adr/decoupled-listener-io.md), directly above) counts every datagram *it* drops,
  but a datagram the kernel discards before `recv_from` ever returns it is invisible to `logit`
  entirely. Linux exposes this per-socket in `/proc/net/udp[6]`'s `drops` column; sampling it (on a
  timer, keyed by the listener's own bound address) as `logit.input.kernel.drops` would close the
  last uncounted loss path, and is Linux-only with no new dependency. Worth noting almost nothing in
  the field does this in-process — syslog-ng, rsyslog, Telegraf, and gostatsd all tell operators to
  run `netstat -su`/`ss -u` themselves — so building it would put `logit` ahead of the field, not
  merely at parity.
- **A UDP listener reads one datagram per syscall.** `read_loop` (`logit-inputs::udp`,
  [ADR `decoupled-listener-io`](adr/decoupled-listener-io.md)) calls `recv_from` once per datagram. `recvmmsg(2)`
  amortizes that across a batch — rsyslog's own high-throughput reference config sets `batchSize`
  to 128, gostatsd's `--receive-batch-size` defaults to 50 — and syscall overhead is the read half's
  dominant remaining cost now that a stalled downstream no longer stops it running. Not built:
  `tokio::net::UdpSocket` doesn't expose `recvmmsg`, so this needs raw-fd work via `try_io` plus a
  `libc` binding.
- **One reader per UDP listener.** A single `recv_from` loop is one core's worth of read capacity.
  `SO_REUSEPORT` lets multiple sockets share one port with the kernel load-balancing datagrams
  across them — gostatsd's `--max-readers` (default `min(8, NumCPU)`), rsyslog's per-listener
  thread count (capped at 32). Not built, partly because it interacts with the previous entry (a
  batched read raises the single-reader ceiling before more readers are worth adding) and partly
  because N readers each holding their own `Fanout` clone would need its own answer to the
  cancel-by-drop shutdown cascade ([ADR `service-lifecycle-and-output-retry`](adr/service-lifecycle-and-output-retry.md)) that
  today assumes exactly one `Fanout` per listener.
- **A `ReceiveQueue`'s depth/bytes/utilization gauges update on every datagram, not every batch.**
  `BoundedQueue::push`/`pop` (`crates/logit-pipeline/src/queue.rs`) call `update_gauges` — three
  `Telemetry::gauge` calls, each locking `ComponentBuffer`'s `Mutex<HashMap>`
  (`crates/logit-core/src/telemetry.rs`) — unconditionally on every accepted item. On a `SinkQueue`
  that's once per *batch*, an already-accepted cost; on a `ReceiveQueue` it's once per *datagram*,
  and the same listener's `read_loop` (pushing) and `decode_loop` (popping) run concurrently against
  the identical lock, so this is genuine cross-task contention on the receive side's two hottest
  loops, not just added per-call overhead. Deliberately not changed here: `BoundedQueue` is one
  implementation serving both queues by design ([ADR `decoupled-listener-io`](adr/decoupled-listener-io.md),
  workstream A), and coalescing or sampling the receive side's gauge updates without also touching
  the sink side would split that implementation's behavior back apart along exactly the seam it was
  built to erase. If this ever shows up as a measured bottleneck (`script/bench`, the same evidence
  bar `docs/design/memory.md`'s "Costing internal spans" section sets for a similar hot-path
  tradeoff), the fix belongs in `BoundedQueue` itself — e.g. gauging on a sampled/coalesced cadence
  for every caller — not as a receive-only special case.
- ~~**Relative gauge adjustment (`+`/`-`) and sample-rate extrapolation for distributions**~~ —
  **closed, both halves** (`docs/adr/relative-gauge-adjustments.md`). Landed as two
  independently-reviewed branches — relative gauge adjustment and sample-rate extrapolation had no
  code dependency on each other — merged together here now that both are on `main`.

  **Relative gauge adjustments.** `statsd_in` decodes any leading `+`/`-` on a `g` value into
  `MetricKind::GaugeDelta` — explicitly *unresolved*; it must never reach a sink. `aggregate`
  resolves it against the running gauge value: an absolute keeps today's last-write-wins-by-
  source-timestamp rule, a delta applies in arrival order and never advances the LWW timestamp
  (asymmetric on purpose — mixing the two orderings is undefined the moment they interleave
  otherwise). Resolving a delta in a *later* window than the absolute it should apply against
  needs the gauge's value to survive a flush, which `aggregate` now does for gauge series
  specifically, bounded by two independent mechanisms
  (`docs/adr/aggregation-window-semantics.md`'s amendment): `series_retention` (a windows-count
  TTL per series, on by default at `5` windows — a feature whose entire point is making "resolves
  against 0.0" rare shouldn't default to guaranteeing it; `0` opts out entirely, reproducing the
  strictly-tumbling behavior every config had before this existed) and `max_retained_series`
  (a hard cardinality cap, since the TTL alone bounds only the tail of the retained set, not its
  peak).

  What's left open, by design, not oversight:
  - **Retention is on by default (`series_retention: 5`, `max_retained_series: 10,000`), so
    upgrading with no config change turns it on for every existing `aggregate` component.**
    Deliberate — a feature whose entire point is resolving deltas correctly shouldn't ship
    opt-in, and both fields are additive to the schema so no config fails to validate — but it is
    a real behavior change: a config with high-cardinality, slowly-churning gauge tags can see its
    steady-state memory grow purely from the upgrade (up to `max_retained_series` idle series
    held for up to `series_retention` extra windows per `aggregate` component), with no line in the
    config saying so. `logit.transform.series.retained` makes the actual number visible;
    `series_retention: 0` opts back out to the exact pre-upgrade behavior.
  - **A delta after eviction (the cardinality cap) or after a process restart resolves against
    0.0.** The eviction case is counted and reported (`logit.transform.gauge.delta.unseeded`,
    `logit.transform.series.evicted{reason="cardinality"}`) — never silent. The restart case is
    unfixable without durable aggregator state, which this project has deliberately not built:
    ADR `aggregation-window-semantics`'s original objection to cumulative counters ("state grows
    unbounded with series cardinality and a process restart resets every series to zero with no
    way to detect that from the emitted stream") applies just as much to a retained gauge.
    Retention narrows the window this can happen in; it does not close it. A *cumulative* series
    (`temporality: cumulative`, that ADR's later amendment) has the same exposure but not the same
    blindness: every emitted point carries a `start_timestamp`, so a consumer can see the restart
    even though `logit` can't prevent it. A gauge has no such field, and inventing one for it is
    not on the table.
  - **A `GaugeDelta` reaching a sink with no `aggregate` on its path degrades to a throttled,
    per-metric drop, not a config-time error.** `influxdb_out`'s encoder reports it under its own
    `gauge_delta_unresolved` diagnostic key (not the generic `encode_error`) and skips just that
    metric, same as `Set`. A `logit validate` graph check ("a statsd input reaches an output with
    no `aggregate` on the path") is implementable — `logit-pipeline::graph` already walks the
    resolved graph — but has a real false-positive case (resolving downstream in a separate
    collector this instance forwards to is legitimate) and `logit validate` has no warning channel
    today, only pass/fail. Deferred, not silently skipped.

  **Sample-rate extrapolation for distributions.** `DdSketch::add_weighted(value, count)`
  (`crates/logit-core/src/metric.rs`) delegates to `sketches_ddsketch::DDSketch::add_with_count`
  (an O(1) native weighted add, not a repeated-`add` loop or a binary-doubling `merge` — both were
  considered and rejected: the crate does have a native weighted add, and even a repeated-`add`
  fallback would have been chosen over `merge` specifically because `merge` is O(log count)
  allocations on `statsd_decode_one_line`'s exact-equality allocation path, which this project's
  own convention forbids relaxing). `statsd_in`'s `ms`/`h`/`d` decoding now extrapolates
  `100|ms|@0.1` into 10 weighted samples instead of one unweighted one, the same way a `c` (counter)
  already extrapolates via `value / sample_rate`. Weight is
  `(1.0 / sample_rate).round().max(1.0)`, **clamped** at `MAX_SAMPLE_WEIGHT` (1000, i.e. `@0.001`)
  rather than extrapolated without bound — a bound on the resulting population estimate now that
  the add itself is O(1), not a CPU-loop guard, matching `aggregate.rs`'s
  `MAX_CONTRIBUTING_CONTEXTS_PER_SERIES` stance on fixed, non-configurable constants. A clamp is
  throttle-reported (`sample_rate_clamped`, mirrored into
  `logit.component.diagnostics{key="sample_rate_clamped"}` by `Diagnostics` for free — no separate
  counter), never silent. A sample rate on `g` (gauge) or `s` (set) stays ignored — extrapolating
  an absolute or a cardinality-estimator value is meaningless, unlike a count.

  **Updated 2026-09-12 (W3):** the mechanism described in the paragraph above no longer lives in
  `statsd_in` at all. [`docs/plans/lossless-transit.md`](plans/lossless-transit.md)'s W2 moved the
  sketch-and-clamp step (verbatim, including `MAX_SAMPLE_WEIGHT`/`sample_rate_clamped`) into
  `aggregate`'s default `distributions: sketch` absorb path (`Samples::sketch`/`Samples::MAX_WEIGHT`,
  `crates/logit-core/src/metric.rs`), and W3 deleted `statsd_in`'s own copy entirely: `ms`/`h`/`d`
  now decode straight to a raw `MetricKind::Samples` with `sample_rate` carried verbatim and no
  sketching or extrapolation at decode time at all. A `statsd_in -> aggregate` pipeline reports
  `sample_rate_clamped` exactly once now, not twice. See [ADR `statsd-output`](adr/statsd-output.md)'s
  amendment and [ADR `aggregation-window-semantics`](adr/aggregation-window-semantics.md)'s own
  amendment.
- ~~**`eprintln!` instead of a real diagnostics facility** — every component's diagnostic now goes
  through `logit_core::diag::Diagnostics`, which closes the two concrete hazards this entry used to
  name: every message is prefixed with its component's id, and a message that can fire once per
  event under normal operation is throttled by occurrence count rather than printed unbounded.
  What's still missing is the real thing: severity levels, structured fields, filtering — a full
  `tracing` migration, deliberately kept as separate, later work rather than folded into this
  narrower fix.~~ **Closed** ([ADR `tracing-for-self-logging`](adr/tracing-for-self-logging.md)):
  `Diagnostics::warn`/`warn_throttled` emit through `tracing::warn!`, carrying `component` and
  `key` as structured fields; new `info`/`error` cover unthrottled lifecycle messages. `logit run`
  gains `--log-level`/`LOGIT_LOG` and `--log-format text|json`. `grep -rn 'eprintln!'
  crates/*/src` now names only `logit-cli/src/main.rs` — `Command::Run`'s exit-error printer,
  `Command::Graph`'s validation warning, and `Command::Ready`'s probe failure — a CLI's own
  stderr on its own error paths, not a running service's self-log.
- **Closed for SIGTERM/SIGINT** ([ADR `service-lifecycle-and-output-retry`](adr/service-lifecycle-and-output-retry.md)) — a
  signal handler now closes every listener's inbox normally
  (`logit_pipeline::run_with_shutdown`, `crates/logit-pipeline/src/runtime.rs`), triggering the
  same close-time flush a listener's own natural completion always has. One residual gap left open
  by that fix, not an oversight (the other, `Output`'s missing close/flush hook, is now closed too
  — see below):
  - **A datagram in flight when the signal lands is lost.** Cancelling a listener's `run` future
    drops whatever it was mid-`recv_from`/decode on. Accepted: UDP is lossy by contract already;
    the aggregation window (which this fix does protect) is not.

  ~~`Output` still has no close/flush hook of its own.~~ **Closed**
  ([ADR `buffered-sink-delivery`](adr/buffered-sink-delivery.md)): `Output` gains `async fn flush(&mut self)`
  (default no-op, so no existing sink needed to change), called once `write_loop`
  (`crates/logit-pipeline/src/runtime.rs`) stops delivering — either because its queue drained to
  closed-and-empty, or because a bounded shutdown grace (default 5s) expired first with batches
  still undelivered. Now load-bearing rather than purely aspirational, since a sink can genuinely
  hold unwritten data at shutdown once buffering exists (the entry above).
- **Fan-out/fan-in is unbuffered/uncoordinated** — the component graph (ADR `component-graph-configuration`,
  [pipeline-graph.md](design/pipeline-graph.md)) makes arbitrary fan-out/fan-in the normal case (a
  sink shared by two branches, one listener feeding several filters), but a stalled sink backs up
  every branch sharing an upstream with it, not just its own. A per-edge `on_full: block | drop`
  policy for the backpressure question is an open one, not yet designed — unaffected by the
  allocation work below, which is about the clone cost, not the backpressure semantics.

  ~~Each extra consumer of a node costs a full `EventBatch` clone.~~ **Closed, with a real residual
  gap.** `Arc<EventBatch>` copy-on-write landed (`docs/adr/arc-eventbatch-copy-on-write.md`,
  three rounds, each correcting an overclaim the last one made — worth reading for that alone). A
  single-consumer edge (most edges in the shipped config) and an all-`Output` fan-out are both now
  unconditionally free or near-free (0 and 1 allocations). What's left, exactly as measured, not as
  originally hoped: a fan-out mixing one `Output` branch with one mutating branch costs 1 *or* 6
  allocations depending on real scheduling, never a fixed number; a fan-out with no `Output` branch
  at all still costs a full clone (6, one worse than the original code), with no path to
  improvement under the current design. See [memory.md](design/memory.md) §3 for the complete,
  shape-by-shape account — there is no single number for "what fan-out costs now."
- **A Lua component's `flush()` has no resource or scope of its own at a timer tick** — unlike an
  `aggregate` component, which tracks its own per-resource windows, a Lua component's flushed
  events default to whichever resource (and, since W7, scope) it most recently saw on a real batch
  (`crates/logit-pipeline/src/runtime.rs`'s `last_resource`/`last_scope`, see [ADR
  `aggregation-window-semantics`](adr/aggregation-window-semantics.md)). A script can now override
  either default explicitly by writing `resource`/`scope` inside `flush()` itself
  (`crates/logit-script/src/resource.rs`/`scope.rs`, [ADR
  `operator-declared-resource-attributes`](adr/operator-declared-resource-attributes.md)) — a
  workaround available to the script author, not a fix to the underlying gap: `logit` still has no
  way to attribute a flush-driven emission to a *specific* one of several upstream resources (or
  scopes) on its own. Fine for every config today (one listener, one resource/scope); would need a
  real answer once a component has more than one upstream resource/scope and no script-side
  override.
- **A Lua component's `flush()` sees a stale trace context, for the same reason.** `trace.trace_id`/
  `trace.span_id` (`docs/design/lua-api.md`'s "Reading trace context") reflect whichever batch
  `process()` most recently saw, not any single batch a flush-driven emission could correctly
  attribute itself to — the same *n*-to-1 problem `Transform::flush`'s linking solves for native
  transforms (below), deliberately not solved the same way here: a Lua component has no
  accumulator `logit` can inspect, so there's no state to track contributing contexts *into*. A
  script that wants better than "stale" can read `trace.trace_id`/`trace.span_id` inside its own
  `process()` and do its own bookkeeping — the values are genuinely there to use, just not
  aggregated by `logit` on the script's behalf.
- **A Lua component's `flush()` sees stale provenance, for the same reason.**
  `provenance.origin`/`.previous` (`docs/design/lua-api.md`'s "Reading provenance") reflect
  whichever batch `process()` most recently saw, not the flushing component itself — the same
  *n*-to-1 gap as the trace-context entry just above, with one difference: outside a Lua `flush()`,
  `Fanout` (`crates/logit-pipeline/src/fanout.rs`) resolves this correctly for the batch it actually
  sends (a flush-driven emission is stamped with the flushing component as both `origin` and
  `previous`, [ADR `batch-provenance-on-delivered`](adr/batch-provenance-on-delivered.md)) — only
  the Lua globals a script reads *during* `flush()`, before that stamp is applied, are stale. A
  script reading `provenance.origin` inside `flush()` to decide what to do sees the last processed
  batch's value, not what the emitted batch will actually be stamped with.
- **No native way to stamp `logit`'s own pipeline trace context onto a log's `LogRecord.trace`** —
  a script can already do this by hand (`event.log.trace_id = trace.trace_id`,
  [ADR `log-record-trace-context`](adr/log-record-trace-context.md)), but the `trace_context`
  native transform has no equivalent opt-in mode, only attribute-lifting. Deliberately deferred,
  not designed around yet: it stamps *logit's* identity onto *application* data, which must stay
  strictly opt-in (never a default, same posture as everything else on this page), and no concrete
  consumer has needed it yet. Revisit once one does.
- ~~**Lua has no span API at all**~~ — **narrowed to span writes/minting from Lua.** `event.span`
  (`docs/design/lua-api.md`'s "Reading `event.span`") is now a real, read-only proxy — a script can
  read every field a `SpanRecord` carries, including its `events`/`links` tables, once one exists.
  What's left: there is still no way for a script to *create* or *mutate* a span.
  `trace_context`'s `span:` block ([ADR
  `trace-context-span-lifting`](adr/trace-context-span-lifting.md)) is still the *only* way to
  turn a log line into a span today. A script ahead of it can still prepare the convention
  attributes (compute `span.start` from whatever the line actually carries, say) for
  `trace_context` to consume — genuinely useful, just not a substitute for write access. Span
  *write* access is its own design pass, the same posture typed record access on `event.log`
  already took before its own read/write half landed.
- **A haproxy/nginx access line derives only its own server span, not the CLIENT-side child span
  for the hop to its upstream** — `trace_context`'s `span:` block mints one `SpanRecord` per
  event, and `Transform::process` is one-in-one-out, so there's nowhere to put a second span for
  the same line. The math is fully available on the wire already: for haproxy (`docs/adr/
  trace-context-span-lifting.md`'s "Producer timing model"), a CLIENT span to the upstream would
  be `start = request_date + TR + Tw`, `duration = Tc + Tr + Td` (`%TR`/`%Tw`/`%Tc`/`%Tr`/`%Td`,
  logged as plain `haproxy.timer.*` attributes for exactly this reason); for nginx, the
  `$upstream_connect_time`/`$upstream_header_time`/`$upstream_response_time` triple gives the
  equivalent breakdown. Building this needs either a `Transform` that can emit more than one event
  per input (a trait change) or a second, explicitly upstream-flavored lift mode — not designed,
  since no config needs it yet and the attributes needed to build it later are already on the
  event.
- ~~**A benchmark of the event proxy against plain table conversion is still outstanding**~~ —
  **closed.** Measured in `crates/logit-bench/benches/pipeline.rs` (`lua::proxy` vs
  `lua::to_table`): the proxy is faster, widening in its favour for scripts that read few
  attributes, since `to_table` converts everything regardless. The design commitment in
  [lua-api.md](design/lua-api.md) stands, now with a number behind it.

  What the same measurement turned up — the boundary costing 21 allocations per event (a `_G`
  lookup of `process` per event, a fresh `AttrsProxy` userdata per attribute access, a Rust
  `String` per metamethod key) — is **also now closed**: 21 → 9, via caching `process`/`flush` as
  an `mlua::RegistryKey` (resolved once at load, not looked up from `_G` per call), caching the
  `AttrsProxy` userdata per event instead of rebuilding it per access, and taking `mlua::String`
  instead of an owned `String` in both metamethods. Two edge cases the caching opened were closed
  rather than left as caveats: a script that stashes `event.attributes` past the point its event is
  returned now fails loudly in this crate's own voice (not mlua's raw error), and a `flush` global
  that exists but isn't a function is now a load-time error, matching `process`'s existing
  `MissingProcess` treatment, instead of silently behaving as "no `flush()`" forever. Both are
  documented in [lua-api.md](design/lua-api.md); see [memory.md](design/memory.md)'s recommendations
  for the full write-up.
- **The attribute/metric-name interner never frees** (`crates/logit-core/src/interner.rs`) —
  `lasso::ThreadedRodeo` has no eviction, so every distinct string ever interned is retained for the
  life of the process, at a measured ~94-124 bytes each.

  **Accepted, not planned work.** The bounds, measured: re-interning a string the table already
  holds allocates *nothing*, so a fixed schema reaches steady state and stays flat; and only keys
  and metric names are interned, never values — so the usual telemetry cardinality explosion (host,
  request id, user agent, path) never touches it. What's left is a metric name that is real and
  never repeats, which means a user who embedded an id in a metric name. `logit`'s listeners are
  private by deployment shape ([OVERVIEW.md](OVERVIEW.md)), so that namespace is user-controlled
  rather than attacker-controlled; the anti-pattern is well known; and `logit` isn't what breaks
  first. The metric store goes well before (a million distinct measurement names is a million-plus
  series, against 94 MB here), and even inside `logit`, `aggregate`'s window costs ~600 bytes per
  series *per window* against the interner's ~94 bytes once — ~6× harder, sooner, and already
  mitigated by putting `keep` in front of it.

  **The premise is the thing to re-check, not the conclusion:** if a listener ever stops being
  private — a public or multi-tenant ingest endpoint, a hosted aggregator — revisit this. The
  retrofit is expensive: `Symbol` is `Copy` and `resolve` *panics* on an unknown symbol, so
  `AttrMap`, `MetricRecord`, `SeriesKey`, the Lua proxy, and the planned wire dictionary all assume
  symbols are eternal. See [memory.md](design/memory.md)'s interner section.

  ~~If the `tracing` migration lands anyway, an `interner::len()` gauge is nearly free at that
  point and would make this observable rather than silent.~~ **Closed, ahead of that migration.**
  `internal`'s process-level gauges (`logit.process.interner.strings`,
  [internal-telemetry.md](design/internal-telemetry.md)) sample `interner::len()` on every drain
  tick — growth is now observable by attaching any sink to `internal`, no `tracing` migration
  required first.

  Separately and unrelated to growth: **`AttrMap::get` used to intern rather than probe**
  (`attrs.rs`) — fixed. All three production call sites were keyed by config strings or Lua
  literals (a bounded set), so this was a wasted hash plus concurrent-map probe on the hot path,
  not a leak, but it's gone now regardless: `get`/`remove` use `interner::lookup`, a non-interning
  probe, falling through to the existing search only on a hit.

  **`otlp_in` is the sharpest form of the growth premise yet, now that it's landed (PR3).**
  Every earlier listener's attribute *keys* come from `logit`'s own config or a fixed protocol
  grammar (statsd's `#tag:value`) — a bounded set by construction.
  **Correction: syslog's structured-data field names are not actually in that bounded-set
  category.** `syslog.sd` (`Value::Map { "<SD-ID>" -> Value::Map { "<PARAM-NAME>" -> ... } }`,
  [ADR `syslog-structured-data-convention`](adr/syslog-structured-data-convention.md)) interns
  every SD-ID and PARAM-NAME a peer sends, at both map levels, exactly like any other `AttrMap`
  key — nesting doesn't bound that. The real bound is RFC 5424's own grammar (1 to 32 PRINTUSASCII
  bytes, excluding `=`, SP, `]`, `"`), not a fixed set `logit` defines — the same interner exposure
  the `json` transform already has for an arbitrary JSON object's keys
  ([ADR `json-parsing-into-attributes`](adr/json-parsing-into-attributes.md)), just with a length
  cap `json` doesn't have. `crates/logit-proto/src/otlp/common.rs`'s `key_values_into_attrs` interns
  every OTLP `KeyValue.key` it decodes, and OTLP attribute keys are arbitrary peer-supplied strings
  with no `logit`-side grammar bounding them at all — including no length cap, which is what makes
  OTLP the sharpest form of this yet, sharper even than `syslog.sd`'s 32-byte-capped tokens — the
  first listener where "a metric name that never repeats" (this entry's stated retrofit trigger,
  above) could plausibly come from something other than a user's own naming mistake. The mitigation
  this entry already names is documented for real now: [`docs/deploying.md`](deploying.md) has a
  `keep`-in-front recommendation specifically for `otlp_in`, not just the general
  `aggregate`-cardinality one
  [`examples/nginx-to-influxdb.yaml`](../examples/nginx-to-influxdb.yaml) already demonstrates.
- ~~**`statsd_in` copies tag values instead of slicing them**~~ — **closed.** It used to build
  attribute values with `attributes.insert(k, v)` on a `&str`, routing through
  `Value::str` → `Bytes::from(String)` (copying bytes already in the datagram buffer), then
  `build_event`'s `attributes.clone()` promoted each to a shared `Bytes`, copying a second time.
  Now uses the same pointer-arithmetic `slice_of` reconstruction `syslog.rs` already had: 8
  allocations per line down to 2, the same irreducible pair `syslog_in` pays (one `Vec<Event>` per
  line, one for the batch, split across two `Vec`s here due to statsd's multi-value grammar).
  `crates/logit-bench`'s `statsd_tag_values_share_the_datagram_allocation` (formerly
  `statsd_tag_values_are_copied_not_sliced`, inverted exactly as that test's own doc comment said
  it would be) asserts the zero-copy property structurally now. See [memory.md](design/memory.md).
- ~~**`influxdb_out`'s line encoder allocates ~180 times per event**~~ — **closed.** Was the largest
  single cost in the pipeline, roughly twice what ingesting an event cost end to end. Now 30
  allocations per 100-event batch (from 18,024) and 2.6× faster, by escaping and formatting
  straight into buffers reused on the encoder, merge-joining the resource and event attribute maps
  instead of cloning and re-inserting, borrowing the series key for its lookup and allocating it
  only on a miss, and reusing `allocate_timestamp`'s path-compression scratch. Output is
  byte-for-byte unchanged, which the existing format tests pin. What remains is per-batch rather
  than per-event; `crates/logit-bench`'s `influx_encode_100_events` guards that.

  **`stdio_out` got the same treatment shortly after** (`crates/logit-outputs/src/stdio.rs`):
  ~18 allocations per event down to ~1 (1801 → 101 per 100 events), via the identical merge-join
  and reused-buffer mechanism. It had briefly become the more wasteful of the two encoders once
  `influxdb_out` was fixed first; both are now in the same range. See
  [memory.md](design/memory.md)'s recommendations.
- **Channel depth is bounded in batches, not bytes or events**
  (`CHANNEL_CAPACITY`, `crates/logit-pipeline/src/runtime.rs`) — 64 batches per edge, with
  unbounded batch size. Narrowed by [ADR `decoupled-listener-io`](adr/decoupled-listener-io.md) for a UDP
  listener's own outbound edge specifically: `BatchAccumulator` now merges many datagrams into one
  batch under an explicit, config-visible byte bound (`receive.batch_max_bytes`, default 1MiB), so
  what a `statsd_in`/`syslog_in` edge can hold is bounded by config, not just by datagram size. What
  remains open is every *other* edge — a transform's outbound batch size is still unbounded, so a
  65 KB syslog datagram parsed into hundreds of events and then re-batched by a downstream transform
  can still produce an oversized batch with nothing in the config saying so, and total in-flight
  memory still scales with edge count. Becomes real with a TCP input feeding a transform directly,
  where nothing caps how many events one read produces -- `tail_in`/`docker_in`
  (`docs/adr/file-tailing-and-docker-json-logs.md`) turned out *not* to be this case after all:
  each tracked file gets its own `BatchAccumulator` under the identical config-visible
  `receive.batch_max_events`/`batch_max_bytes` bound `statsd_in`/`syslog_in` already use, so a
  busy file's outbound batch is bounded the same way a UDP listener's already is.
- **`!env` is invisible to `schema/logit.schema.json`** ([ADR `env-yaml-tag`](adr/env-yaml-tag.md)) —
  resolution happens on the parsed YAML tree before serde ever sees it
  (`crates/logit-cli/src/config.rs`), so the schema describes the substituted shape, never the tag
  itself. A schema-aware YAML editor will flag a `!env`-tagged value it can't resolve against the
  schema.
- **Config deserialization errors lose line/column information** once `!env` is in the picture
  (`crates/logit-cli/src/config.rs`) — resolving the tag requires parsing to
  `serde_norway::Value` first and deserializing from that, and `serde_norway::from_value` carries
  no source location the way `serde_norway::from_str` does directly on the raw file. Partly offset
  by `!env`'s own errors naming a config path (`components.influx_out.token`) and by the note
  appended when a substitution's resolved type likely caused the failure.
- **`graph::is_implemented`'s error Debug-prints a whole `ComponentKind`**
  (`"kind {:?} is not implemented yet"`, `crates/logit-pipeline/src/graph.rs`) — harmless today,
  since no *unimplemented* kind carries a secret field, but with `!env` now used to inline secrets
  directly into fields (ADR `env-yaml-tag`) rather than referencing them by name, this becomes a real leak
  the moment an unimplemented kind gains one. Fix before that happens: redact or field-list instead
  of a blanket `{:?}`.
- **`logit graph` can't render a config with any secret left unset** — every `!env` reference must
  resolve for all three commands (ADR `env-yaml-tag`), including `graph`, even though it only ever reads a
  component's `sources`/`type` to render topology and style nodes by role. A lenient mode that
  substituted a placeholder for a missing variable was tried and reverted (ADR `env-yaml-tag`'s
  Alternatives) — visualizing a config's shape without its production secrets set needs a copy of
  the config with dummy values filled in, not a feature of `logit graph` itself.
- **`syslog_in` is UDP-only** — nginx's `syslog:` writer is UDP-only, so a TCP accept loop would
  buy the driving integration nothing. `syslog_out` (the egress side, `docs/adr/syslog-output.md`)
  supports both UDP and TCP, and that asymmetry is deliberate, not a sign this entry needs closing
  to match. Stays additive-later on the *input* side specifically. **Closed: `syslog_in` no longer
  skips RFC 5424 STRUCTURED-DATA** — `parse_structured_data`
  (`crates/logit-inputs/src/syslog.rs`) is a real, quote-aware parser into `syslog.sd`; see
  [ADR `syslog-structured-data-convention`](adr/syslog-structured-data-convention.md).
- **Closed: `syslog_out` now emits RFC 5424 STRUCTURED-DATA** — every `syslog.sd` element an
  event carries round-trips (`write_structured_data`,
  `crates/logit-outputs/src/syslog.rs`), and an opt-in `structured_data: { sd_id: "<name>@<PEN>" }`
  element carries every non-`syslog.*` attribute — what a `json`/`kv_metrics` stage merged into
  `event.attributes` — once an operator picks an SD-ID (a private-enterprise-number-qualified one,
  RFC 5424 §7.2.2; no default is shipped). See
  [ADR `syslog-structured-data-convention`](adr/syslog-structured-data-convention.md). Still open: a
  log's native trace context (`log.trace`, [ADR `log-record-trace-context`](adr/log-record-trace-context.md))
  isn't an `event.attribute` at all, so the opt-in element still can't carry it.
- **SD-ELEMENT/SD-PARAM order is canonicalized by name, not by wire position** — `write_structured_data`/
  `write_sd_element` (`crates/logit-outputs/src/syslog.rs`) sort SD-IDs and PARAM-NAMEs by name
  bytes rather than reproducing `AttrMap`/attribute iteration order (process-global intern order,
  not wire order); a repeated PARAM-NAME's occurrences are emitted grouped under one name, so a
  wire `a b a` interleaving (the same PARAM-NAME appearing, another PARAM-NAME, then the first
  again) is re-emitted as `a a b`, not preserved. Permitted under
  [ADR `lossless-transit`](adr/lossless-transit.md)'s attribute-reordering normalization; recorded
  in [ADR `syslog-structured-data-convention`](adr/syslog-structured-data-convention.md)'s
  Consequences and `crates/logit-cli/tests/syslog_round_trip.rs`'s normalization list.
- **Narrowed: `syslog_out` only re-stamps a relayed timestamp when the origin's own can't be
  rendered on the configured output format** — per the timestamp-precedence rule in
  [ADR `syslog-structured-data-convention`](adr/syslog-structured-data-convention.md)
  (`write_5424_timestamp`/`write_3164_timestamp`, `crates/logit-outputs/src/syslog.rs`), a resolved
  `syslog.timestamp` now renders directly (a `Value::Timestamp` on either output format; a
  `Value::Str` verbatim only when the output is also 3164). What still falls through to
  `event.timestamp` (receipt time): a 3164-origin `Value::Str` token relayed onto a 5424 output (no
  year or timezone to build an RFC 3339 stamp from), a nil `Value::Null` relayed onto a 3164 output
  (3164 has no NILVALUE), and an absent attribute. The opt-in `syslog_timestamp` transform sketched
  below would still be the way to resolve `event.timestamp` itself, for either direction.
- **`syslog_out`'s control-character escaping is ambiguous with a message that already contained
  the escape sequence literally** — the encoder escapes an embedded newline as the two characters
  `\`/`n` (and similarly for `\r`/NUL) so it can't forge a second syslog message downstream, but
  deliberately leaves a literal backslash untouched (escaping it would double every backslash in a
  JSON message body and break a `| json` LogQL filter on every line). Consequence: a message that
  genuinely contained the literal two characters `\`/`n` is indistinguishable on the wire from one
  that contained a real newline. Accepted in `docs/adr/syslog-output.md`.
- **`syslog_out` has no TLS** — plaintext UDP/TCP only; RFC 5425 (syslog over TLS) and RFC 6012
  (DTLS) are both out of scope. A `logit -> remote collector` hop over an untrusted network has no
  transport security today. `otlp_out`/`otlp_in` gained `tls:` config
  ([ADR `otlp-tls-and-pooled-grpc-client`](adr/otlp-tls-and-pooled-grpc-client.md)) via a
  `TlsClientConfig`/`TlsServerConfig` pair in `logit-config` designed to be reusable by any other
  protocol — `syslog_out`'s own TLS support, if it lands, is a config-plumbing exercise against
  those same types, not a design decision to redo.
- ~~**`logit_proto::Encoder`'s single-`Bytes`-per-batch contract doesn't fit a sink that needs
  per-message framing**~~ — **closed as of 2026-09-12.** `syslog_out` needs one UDP datagram or
  one octet-counted TCP frame per *message*, and `statsd_out` needs one statsd line per metric
  packed up to a datagram size cap, neither of which one opaque `Bytes` per *batch* can express,
  so both used to bypass the trait entirely with a bespoke `encode_into`. Two sinks independently
  needing the shape was the signal being waited for; a third codec (collectd) re-copying the
  buffer was what ended the deferral. [ADR `framed-encoder`](adr/framed-encoder.md) adds
  `logit_proto::FramedEncoder` -- the third encoder shape beside `Encoder` (one blob per batch)
  and `SignalEncoder` (one blob per signal): N framed messages per batch into a shared, generic
  `logit_proto::MessageBuf<M>`, never failing, a per-sink `Stats` for drop accounting -- which
  `syslog_out` and `statsd_out` now implement (statsd's per-call datagram cap became encoder
  state set once per transport). Follow-up, not done here: the collectd codec
  (`crates/logit-proto/src/collectd/encode.rs`) adopting it in place of its own `Packets` buffer,
  before `collectd_out` is built on `Packets`. `prometheus_out` stays outside all three traits by
  design.
- **`statsd_out` drops post-sketch metric kinds — `Distribution`/`Set`/`Histogram`/
  `ExponentialHistogram`/`Summary`/a cumulative or non-monotonic `Sum`, counted
  (`unsupported_metric_kind`).** **Narrowed by W3** — the original v1 deferral covered every
  timer/set metric outright: `ms`/`h`/`d` on the statsd wire all decoded to
  `MetricKind::Distribution` and `s` was a hard decode error, so a `statsd_in -> aggregate ->
  statsd_out` relay dropped every timer/set metric regardless of `aggregate`'s config.
  `crates/logit-outputs/src/statsd.rs` now encodes `MetricKind::Samples`/`SetMembers` — the raw
  shapes `statsd_in` decodes `ms`/`h`/`d`/`s` to losslessly (`docs/adr/lossless-transit.md`'s W3) —
  back to real statsd lines (`name:v1:v2|<type>|@rate` under `format: dogstatsd`, one line per
  value under `format: statsd`; `name:m|s` one line per member for sets). **This means a
  `statsd_in -> statsd_out` relay with no `aggregate` in between, or one configured
  `distributions: samples`/`sets: members`, now round-trips a timer or set line intact; only
  `aggregate`'s *default* summarizing config (`distributions: sketch`/`sets: estimate`) still drops
  every timer/set metric** — the kinds still dropped above only ever exist *after* some stage has
  already summarized, and a merged `DdSketch`/`HyperLogLog` has no lossless statsd rendering (see
  `docs/adr/statsd-output.md`'s original Decision section for why that mapping still deserves its
  own design, not a guess made in passing). **The `samples`/`members` config keeps a window's raw
  shape only while every sample landing in it shares one sample rate and the window stays under
  `max_samples_per_series`/`max_set_members_per_series`** — once either limit is crossed,
  `aggregate` falls back to a sketch/estimate for that window regardless of the config, and this
  sink has no lossless rendering for that fallback either; it drops and counts it exactly like the
  default-summarized case (`docs/adr/statsd-output.md`'s amendment). Tracked as debt against
  [ADR `lossless-transit`](adr/lossless-transit.md); see [`docs/plans/lossless-transit.md`](plans/lossless-transit.md) for the closing assessment's residual-debt list.
- **`statsd_out` has no `unit` and no metric renaming/prefixing; egress timestamp is now carried,
  but only on a `|T`-marked line.** **Narrowed by W3** — DogStatsD's own `|T<unix-seconds>` segment
  (`format: dogstatsd` only) now round-trips: `statsd_in` sets `Event::timestamp` from an incoming
  `|T<secs>` and stamps a `statsd.timestamp: Value::U64(secs)` per-line carrier holding the raw
  wire value itself, not just a marker bit (`docs/adr/statsd-output.md`'s amendment), and
  `statsd_out` re-emits `|T<secs>` from that carrier's own value — never derived from
  `Event::timestamp`, so a stage that rebuilds `Event::timestamp` after decode (`aggregate`'s
  flush, notably) can't fabricate or collapse a `|T` on the way back out. The classic grammar still
  has no timestamp segment at all (`format: statsd` drops `|T` and counts it,
  `dropped_dialect_fields`), and any event with no `U64` carrier set — everything that isn't a
  relayed `|T`-carrying line — is still stamped with the receiver's own receipt time, exactly like
  `syslog_out`'s receipt-time entry above. `MetricRecord::unit` still has no statsd wire
  representation and is dropped the same way. There is still no *native*, sink-level way to rename
  or namespace a metric on egress (~~`docs/design/lua-api.md` notes a metric's value/fields are
  unexposed to Lua~~ — narrowed by W7: `event.metrics` now exposes every metric field for reading
  and `name`/`unit`/`description`/`start_timestamp` for writing on every kind, so a `lua` component
  placed ahead of `statsd_out` *can* rename or retag a metric today, `event.metrics[i].name =
  "..."`; what W7 didn't add is a way to *construct or append* a new metric from Lua, or to write
  any field besides `value`/`temporality`/`monotonic` on kinds other than `sum`/`gauge` — see
  `docs/design/lua-api.md`'s "Reading and writing `event.metrics`") — a sink-side `prefix` field
  was considered and rejected for `statsd_out` specifically (`docs/adr/statsd-output.md`'s
  Alternatives) in favor of a future general metric-rename *transform* (a native component, not
  Lua), which still doesn't exist. Tracked as debt against [ADR `lossless-transit`](adr/lossless-transit.md); see [`docs/plans/lossless-transit.md`](plans/lossless-transit.md) for the closing assessment's residual-debt list.
- **A repeated DogStatsD tag key collapses to its last value.** `#team:a,team:b` is legal
  DogStatsD, but `AttrMap` is a map, not a multiset, so `statsd_in` overwrites the first `team:a`
  with `team:b` while building the event's attributes, before any sink ever sees the line —
  `x:1|c|#team:a,team:b` relays through `statsd_out` as `x:1|c|#team:b`, silently dropping the
  first value rather than the whole tag. This is a model gap (`AttrMap` itself, not a decoder or
  sink bug), tracked as debt against [ADR `lossless-transit`](adr/lossless-transit.md); see
  [`docs/plans/lossless-transit.md`](plans/lossless-transit.md) for the closing assessment's residual-debt list.
- **`statsd_out` has no TLS/DTLS** — plaintext UDP/TCP only, same gap as `syslog_out`'s above, and
  the same `TlsClientConfig`/`TlsServerConfig` pair would be the config-plumbing exercise if it
  lands.
- **Closed: a non-UTF-8 syslog MSG decodes to a `Value::Bytes` event instead of being rejected** —
  RFC 5424's `MSG-ANY` permits arbitrary octets, and `logit-core::Value`'s `Bytes` variant now
  carries it. `parse_line`/`parse_5424`/`parse_3164` (`crates/logit-inputs/src/syslog.rs`) parse
  header fields directly off the line's raw bytes and validate each individually as PRINTUSASCII;
  only the MSG slice is UTF-8-validated (`message_value`), so a line whose header parses cleanly
  while MSG isn't valid UTF-8 now decodes with a `Value::Bytes` message instead of failing outright.
  `syslog_out` writes a `Value::Bytes` message raw, sanitized at the byte level
  (`sanitize_msg_bytes`), never lossy-decoded. See
  [ADR `syslog-structured-data-convention`](adr/syslog-structured-data-convention.md).

  **UTF-8 rejection was never the only thing standing between a syslog line and an arbitrary-binary
  payload, and closing it above doesn't change that.** `SyslogDecoder::decode_into`
  (`crates/logit-inputs/src/syslog.rs`) still splits a datagram on `\n` *before* any UTF-8 check
  runs, so a binary payload containing a `0x0A` byte is still cut mid-value by the framing — see the
  HAProxy CBOR entry below, where this framing gap is what actually blocks the case that motivated
  writing it down. `Value::Bytes` MSG closes the UTF-8 half of the gap; a binary payload that isn't
  newline-safe by construction (nginx's `escape=json` output happens to be; not every binary format
  is) still needs an escaped-binary encoding or an opt-out of `syslog_in`'s newline splitting to
  round-trip.
- **HAProxy's native CBOR log output (`%{+cbor}o`/`%{+cbor+bin}o`) was evaluated as a cheaper way to
  source its access logs and deliberately not pursued** — a considered "not now," not an
  unexplored idea, recorded here so the investigation doesn't get redone. Three findings, each
  independently sufficient to defer it:
  - **The reachable mode is bigger than JSON, not smaller.** HAProxy's default CBOR encoding
    (`%{+cbor}o`, no `+bin`) is hex-encoded ASCII — a line like `BF69636C69656E745F6970…`, an
    indefinite-length map rendered as hex text, ~2 bytes on the wire per payload byte. Only
    `%{+cbor+bin}o` emits raw binary, which is the mode that would actually be more compact than
    the demo's hand-rolled JSON — but see the next point.
  - **Binary CBOR cannot reach `logit` over any transport it has today.** Beyond the non-UTF-8
    rejection above, `syslog_in` splits every datagram on `\n` before any UTF-8 check runs at all
    (`crates/logit-inputs/src/syslog.rs:190-197`), and `0x0A` occurs freely inside CBOR — it's the
    encoding of the integer 10, and turns up throughout length headers and float payloads — so a
    binary payload is chopped mid-value by the framing itself, independent of the UTF-8 question.
    `tail_in`/`docker_in` are line-framed too, and Docker's json-file driver wraps each line in a
    JSON string that can't carry arbitrary octets at all. Nothing in the tree offers
    length-delimited framing, which is the actual prerequisite; a `cbor_in` listener, a unix-socket
    input, or an opt-out of `syslog_in`'s newline splitting would each qualify.
  - **HAProxy's log-format item-name grammar rejects a literal `.` in a custom name, and `%{+json}o`
    and `%{+cbor}o` share that grammar** (already recorded at `demo/haproxy/haproxy.cfg:99-117`,
    confirmed empirically against `haproxy -c`) — but the two encodings aren't equally stuck by it.
    JSON has an escape hatch: `demo/haproxy/haproxy.cfg:140` hand-writes the JSON text itself, with
    per-value `json(ascii)` escaping, to get its dotted `span.*`/`trace.*` keys past the grammar.
    CBOR has no equivalent, because binary can't be typed into a `log-format` string — `%{+cbor}o`
    is the only way to emit it, so a CBOR-sourced HAProxy tier is stuck with undotted keys and would
    need a rename stage the JSON tier doesn't. `SpanLiftConfig`
    (`crates/logit-config/src/lib.rs:806-828`) has no source-field override for `span.status`,
    `span.start_us`, or `span.duration_ms`, so `trace_context` can't absorb that rename on its own
    either. Worth being precise about *whose* limitation this is: CBOR's own text-string keys are
    arbitrary UTF-8 and handle dots fine — every constraint above belongs to HAProxy's log-format
    grammar or to `logit`'s current transports, not to CBOR as a format.

  If length-delimited framing ever lands and this is revisited, three design constraints are
  already known and don't need rediscovering: `Value::as_str` **panics** on an invalid-UTF-8
  `Value::Str` (`crates/logit-core/src/value.rs:33-41`), so CBOR's only-nominally-UTF-8 text-string
  type would need validation before becoming one; a hand-rolled decoder needs an explicit recursion
  depth bound, since `json`'s `serde_json`-based one inherits a limit for free that a hand-rolled
  CBOR reader would not; and a length header must never size an allocation directly (an attacker can
  claim a multi-gigabyte array in a handful of bytes). CBOR tag 1 (epoch time), decodable straight
  into `Value::Timestamp`, is the one thing the format would offer that JSON doesn't — the reason
  it's worth this entry rather than a closed door.
- **Narrowed: `event.timestamp` is still receipt time, not the sender's — but that's no longer the
  only place the sender's own clock can land.** Every event is still stamped with the instant its
  datagram came off the socket (`received_at`, captured by the read half and threaded through to
  `Decoder::decode_into` explicitly since [ADR `decoupled-listener-io`](adr/decoupled-listener-io.md)
  decoupled decode from the read loop — not a fresh clock read at decode time, which could
  otherwise run arbitrarily behind arrival under backlog), and preserves the sender's own timestamp
  separately, as the `syslog.timestamp` attribute (a `Value::Timestamp` for RFC 5424's RFC 3339
  form, a raw `Value::Str` for RFC 3164's, or `Value::Null` for a nil 5424 TIMESTAMP). The two can
  diverge: by network and queueing delay always, and by an arbitrary amount when the sender's clock
  is skewed or when messages are replayed or forwarded through a relay. Everything downstream keyed
  on time — `aggregate`'s tumbling window, the point timestamp `influxdb_out` writes — still uses
  `event.timestamp` unconditionally, so today a delayed or replayed message still lands in the
  window it *arrived* in, not the one it *happened* in; that part of this entry is unchanged. What
  *has* changed: `syslog_out`'s own emitted TIMESTAMP field now follows the precedence rule in
  [ADR `syslog-structured-data-convention`](adr/syslog-structured-data-convention.md), so a
  `syslog_in -> syslog_out` relay's *wire* timestamp can reflect the origin again even though
  `event.timestamp` itself does not. Tracked as debt against [ADR `lossless-transit`](adr/lossless-transit.md); see [`docs/plans/lossless-transit.md`](plans/lossless-transit.md) for the closing assessment's residual-debt list.

  Deriving `event.timestamp` from the sender instead was considered and deliberately not done here:
  RFC 3164's timestamp carries no year and no timezone, so resolving it to an instant means guessing
  both, and doing it only for RFC 5424 would give two senders on one listener different timestamp
  semantics with nothing in the config saying so.

  Worth exploring: an **optional `syslog_timestamp` transform** — a component an operator adds to a
  flow explicitly, which replaces `event.timestamp` with a resolved `syslog.timestamp` and makes the
  guesswork configurable rather than implicit. The pieces it would need:

  - RFC 5424: parse the RFC 3339 timestamp directly; no inference needed.
  - RFC 3164: fill in the missing year and timezone. A reasonable default is "the year that puts the
    message closest to receipt time" (which handles a New Year's Eve rollover in both directions)
    plus an explicit `timezone:` field defaulting to UTC — never the host's local zone, which would
    make behavior depend on an environment variable.
  - A bounded **sanity window** (`max_skew:`, say): a resolved timestamp further from receipt time
    than the window is rejected and receipt time kept, with a throttled diagnostic. Without it, one
    sender with a badly wrong clock can write points years away and quietly poison a dashboard.
  - A skip rule matching every other transform here: no `syslog.timestamp`, or one that doesn't
    resolve, means the event passes through with `event.timestamp` untouched — never dropped.

  Being a separate, opt-in component (rather than a flag on `syslog_in`) is the point: it keeps the
  listener's contract simple and honest, and makes "we trust our senders' clocks" a visible line in
  the config graph rather than a default nobody remembers choosing.
- **`stdio_out` has no reopen** — a file target is opened once, in append mode, and held for the
  process's lifetime: an external log rotator that moves the file leaves `logit` writing to the
  unlinked inode until restart (there is no SIGHUP-reopen). Acceptable for a debugging/dev-loop
  sink, which is what this is for — `file_out` (ADR `rotating-file-output`) is the sink to reach
  for when a file target needs to be bounded, sharing `stdio_out`'s implementation but adding a
  rotation policy. The output format is no longer fixed (`format: human | native`,
  ADR `file-output-native-format`) — a user-supplied `format:` *template* over the human-readable
  render specifically is still designed for (the encoder is built around a `Format` enum with room
  for it) but not implemented.
- **`file_out` rotates and retains by count, but has no SIGHUP/external-rotator reopen, no
  compression, no `max_age`, no timestamped rotated-file naming, and its time-based rotation is
  write-triggered rather than boundary-triggered** (ADR `rotating-file-output`). An external tool
  rotating a `file_out`-managed file out from under it hits the same unlinked-inode gap `stdio_out`
  already has — `file_out` only ever rotates a file itself opened, never re-checks whether the path
  it holds still names the same inode. Rotated files are always named with a numbered suffix
  (`.1`, `.2`, ...), never a timestamp, and an idle sink under a calendar `interval` rolls on its
  *next* write after the boundary, not at the boundary itself (the rolled file's *contents* are
  still exactly the previous period's, only its on-disk appearance is delayed). Retention is a
  plain `max_files` count; there is no age-based eviction, and rotated-file compression is
  `format: native`'s `compression: lz4` or nothing — `format: human`'s text render has no
  compression option of its own, and there is no built-in compression of already-rotated files
  after the fact, both left to an external tool. `format:` (`human`, the same human-readable
  render, or `native`, `logit_proto::native`'s wire format, ADR `file-output-native-format`) is
  shared with `stdio_out`; a `format:` *template* over `human` specifically remains the one
  unbuilt extension point (`Format::Ndjson` is named only as an aspiration, not code). Reading a
  `file_out`-written `format: native` file back — a decoder-side reader/verifier, or wiring
  `NativeDecoder` into `tail_in` — is real, unblocked follow-up work, not designed yet.
  `FileTarget::open` seeds `RotationState`'s calendar period from an existing file's own mtime (not
  just `written` from its length), so a restart under an `interval` policy correctly picks up
  mid-period rather than merging two periods' events into one file or silently never rotating —
  the residual is an unreadable mtime (a failed `metadata()` call) or a backwards clock jump across
  the restart, either of which falls back to the pre-seeding behavior (period learned fresh on the
  first write after open). Rotation is commit-point-first: the active file is renamed to a
  transient staging path before anything retained is touched, so a rename failure leaves the active
  file unrotated-and-growing with every retained file completely untouched (not the destructive
  cascade-before-rename order this replaced), and a `.rotating` staging file orphaned by a process
  killed between that rename and its promotion is picked up and promoted to `.1` on the very next
  rotation, never silently lost.
- **A pathological Host header can truncate the syslog-bound JSON line -- but nginx's own header-size
  limit turns out to make that hard to actually trigger.** The example's lean `access_json_syslog`
  `log_format` (`examples/nginx/nginx.conf`) sizes its fixed fields well under nginx's syslog
  message cap, but `$host` itself is unbounded and attacker-controlled behind a public IP.
  Measuring this for real (workstream F, `docs/plans/nginx-integration.md`) against nginx
  1.31.4 turned up a more reassuring result than expected: an oversized `Host` header never reaches
  nginx's syslog writer at all under default settings. `large_client_header_buffers` (4 8k by
  default) rejects any request whose request line plus headers exceed ~8180 bytes with a 400
  *before* nginx builds a log line for it -- every `Host` value nginx will actually log (measured up
  to 8180 bytes) produced a complete, untruncated syslog datagram and parsed cleanly, contradicting
  the assumption (based on older nginx source naming a 1024-byte `NGX_SYSLOG_MAX_STR`) that a
  several-KB `Host` header would truncate the line. Whatever nginx's current per-datagram syslog
  cap actually is, it sits above the request-header limit that already gates this specific vector.

  That doesn't mean the failure mode isn't real, just that this particular door is closed by
  nginx's own defaults, not by anything `logit` does. A syslog line *can* still end up truncated --
  a larger `large_client_header_buffers`, a different unbounded field, or a different syslog client
  entirely could all produce one -- so the pipeline's degradation was verified directly: a
  hand-crafted, deliberately truncated syslog datagram sent straight to `syslog_in` (bypassing
  nginx) confirmed the *documented* consequence exactly: `syslog_in` accepts the truncated-but-valid-
  UTF-8 datagram without error; `json` fails to parse the truncated body and reports a throttled
  `parse_failure` diagnostic (`crates/logit-transforms/src/json.rs`) while passing the event through
  with `attributes` unchanged (only `syslog.*` metadata survives); `nginx_metrics` derives nothing
  field-based for that event (only the fieldless `nginx.requests` counter, which always fires
  regardless of attributes, still increments); sibling requests before and after are unaffected. No
  nginx-side mitigation (e.g. capping `$host`'s logged length) is added here: the pipeline already
  degrades gracefully on a truncated line by whatever means it happens, and capping a field nginx
  itself allows up to 8KB would be solving a problem the design doesn't actually have.
- **Internal telemetry ([internal-telemetry.md](design/internal-telemetry.md),
  [ADR `internal-telemetry-as-pipeline-events`](adr/internal-telemetry-as-pipeline-events.md)) covers metrics only** — the
  framework (the `internal` component, the per-component buffer, the emit API) is built to extend,
  but three extensions are deliberately not part of this first cut:
  - **Internal spans — emission, sampling, and export are now built and proven end to end; three
    narrower residuals remain (below).**
    `internal`'s name (not `internal_metrics`) deliberately left room for this without a rename.
    History: `crates/logit-bench/tests/allocations.rs`/`benches/pipeline.rs`'s node-runtime
    coverage closed the gap where nothing measured what `run_transform`/`run_output`/`Fanout::send`
    themselves cost (including the *first* call after every `internal` drain,
    `ComponentBuffer::drain`'s `mem::take` re-populating the buffer rather than updating it — a
    real, recurring cost the first pass at this measurement missed); a throwaway `TraceContext`
    prototype on `Delivered` was built, measured against that coverage per
    [ADR `minimize-allocations-over-event-size`](adr/minimize-allocations-over-event-size.md)'s gate, and reverted — zero
    allocation change, `size_of::<Delivered>()` 32 → 56, no attributable throughput regression.
    See `docs/design/memory.md`'s "Runtime" and "Costing internal spans" sections for that account.

    **On that evidence, [ADR `trace-context-propagation-on-delivered`](adr/trace-context-propagation-on-delivered.md) built real
    propagation** — `Delivered` permanently carries a `TraceContext`, and the two node kinds with an
    unambiguous parent propagate a real one: `Transform::process`/`ScriptWorker::process` (the
    non-flush path, one incoming batch per emission) via `Fanout::send_with_context`, and
    `run_output` (already borrows the incoming `Delivered` without unwrapping, so nothing further
    to wire). A follow-up gave `Transform::flush`/`Aggregator` a bounded, best-effort
    `ContributingContexts` set per series (`MAX_CONTRIBUTING_CONTEXTS_PER_SERIES`, 8 — dropped and
    counted past the cap, `logit.transform.links.dropped{reason="cardinality"}`) and paired each
    flushed `Event` with the `SpanLink`s that set produced. Lua's `flush()` got no equivalent — no
    accumulator `logit` can inspect — left as an accepted stale-context limitation (next to the
    identical `Resource`-staleness gap), with `trace.trace_id`/`trace.span_id`
    (`docs/design/lua-api.md`) exposed to a script's own `process()` instead. Picking an arbitrary
    contributing batch as "the" parent, for either case, was considered and rejected (silently
    wrong is worse than visibly incomplete).

    **[ADR `internal-span-emission-and-deterministic-sampling`](adr/internal-span-emission-and-deterministic-sampling.md) closed both items
    this entry used to list as open: emission and sampling.** `Telemetry::span`/`SpanGuard` (mirroring
    `Timer`'s disabled-is-free shape) turn a `(context, node, batch)` visit into a real
    `SpanRecord`-carrying `Event`, drained by `ComponentBuffer::drain`'s new span pass alongside the
    existing metric pass — exactly the "`ComponentBuffer`/drain turns counters into events" shape
    this entry once named as the expected home, per ADR `internal-telemetry-as-pipeline-events`. `run_flush` also changed shape here:
    it now mints **one** root before `transform.flush(now)` and sends every resource group under it
    (`Fanout::send_with_own_context`), rather than minting a fresh root per group as it used to — one
    flush is one unit of work, not *N* hops. Sampling is deterministic on `trace_id`
    (`trace_is_sampled`, `ComponentKind::Internal::span_sample_rate`, default 0.1) — every node
    reaches the same keep/drop verdict independently, with no propagated bit and no growth to
    `TraceContext`/`Delivered`. See `docs/design/internal-telemetry.md`'s "Spans" section for the
    full account, and `docs/design/pipeline-graph.md`'s "Trace context propagation" table for the
    resulting per-node-kind span record.

    **[docs/plans/otlp-end-to-end.md](plans/otlp-end-to-end.md) (the OTLP series' fourth
    PR) is what actually closes this item, not ADR `internal-span-emission-and-deterministic-sampling` alone.** Everything above built a real
    `SpanRecord` inside `logit`'s own process; nothing tested whether the result was a span *any
    other system would recognize*. `otlp_out` (ADR `committed-pregenerated-otlp-protobuf`'s codec, ADR `hand-rolled-grpc-over-hyper`'s hand-rolled gRPC
    transport) is that proof: `demo/logit.yaml`'s `tempo_out` exports `internal`'s spans to a real
    Tempo over OTLP/gRPC, and Grafana's Tempo panel shows the actual parent/child tree a config's
    topology produces -- a listener root with transform/sink children, matching
    `pipeline-graph.md`'s table exactly. That is the end-to-end proof this entry was missing: not
    "a span exists," but "a span leaves the process, decodes correctly on the wire, and reconstructs
    the right shape on the other end."

    **What sampling does and doesn't do, now that it's exercised for real.** `span_sample_rate`
    (default `0.1`, `1.0` in the demo) decides, once per `trace_id`, whether *this* trace's internal
    spans exist at all inside `logit` -- an unsampled trace never becomes a `SpanRecord`, never
    occupies a slot in the bounded per-component buffer, and costs nothing beyond the sampler's own
    branch (`Telemetry::span`'s doc comment). It is a volume control on `logit`'s own
    self-observability, deliberately independent of the traffic it's observing: raising or lowering
    it changes how much of the internal pipeline you can see, never what the pipeline does to an
    event. What it does *not* do: it doesn't sample the events themselves (a dropped trace's events
    still flow through the pipeline and reach every configured sink, untouched); it doesn't
    propagate to or from a peer (no `sampled` flag crosses `otlp_in`/`otlp_out`'s wire boundary, so
    a `logit` downstream of another `logit` -- or of any other OTLP producer -- makes its own
    independent keep/drop decision on the same `trace_id`, per ADR `internal-span-emission-and-deterministic-sampling`'s "no propagated bit"
    decision); and it doesn't thin the *metrics* signal at all -- `internal`'s point-side buffer and
    `otlp_out`'s metrics encoding are entirely unaffected by this knob, which is why the demo's
    InfluxDB dashboard populates identically whether `span_sample_rate` is `0.1` or `1.0`.

    **What's still open, deliberately, not oversights:**
    1. **The listener span's window is the `send` call only, not decode-to-send.** `Fanout::send`
       has no visibility into how long a listener spent building the batch it's about to send
       (`Input::run` is a free-form loop) — the still-open listener-side half of "delivery I/O is
       not decoupled from event processing" (below).
    2. **Lua `flush()` still gets a link-less root.** It gets a real span now (ADR `internal-span-emission-and-deterministic-sampling`), but no
       links — there is still no accumulator on the Lua side to inspect, same limitation as the
       `Resource`-staleness gap above.
    3. **A `SinkQueue` entry is 24 bytes larger.** `TraceContext` now rides inline in every queue
       entry (`push`/`peek`) so `write_loop` can parent its own sink span on the context a batch
       actually arrived under — the same size-for-a-span trade `Delivered` itself already made and
       measured (`docs/design/memory.md`'s "Costing internal spans" section).
    4. **`service.name` is the only resource identity `internal` sets** (`docs/design/internal-telemetry.md`'s
       "Resource identity" section) — its `Resource` has no `host.name`/`service.instance.id`,
       which would be the semconv-correct next attributes for disambiguating multiple `logit`
       instances in Tempo. Neither is added yet because there is no OS-hostname source anywhere in
       the workspace (`SyslogEncoder::default_hostname`, `crates/logit-outputs/src/syslog.rs`, is
       config-supplied, not OS-derived) — deferred pending that dependency rather than added as a
       one-off.
    5. **The demo's Tempo service graph panel has nothing to show.** It needs Tempo's
       `metrics_generator` (`service-graphs`/`span-metrics` processors) enabled with
       `remote_write` to a Prometheus-compatible store, that store added as a Grafana datasource,
       and `serviceMap.datasourceUid` set on the Tempo datasource — none of which
       `demo/compose.yaml`/`demo/tempo/tempo.yaml` have. This is no longer blocked on the demo
       lacking a real cross-service trace to draw: `docs/plans/demo-tracing-stack.md`'s HAProxy →
       nginx → app chain and [ADR `trace-context-span-lifting`](adr/trace-context-span-lifting.md)'s
       `span:` block together give Tempo a genuine multi-service trace (`haproxy`/`nginx`'s access
       spans plus `demo-app`'s own OTel span). Still deferred — it's added stack pieces
       (`metrics_generator`, a Prometheus-compatible store) for one dashboard panel, not a
       `logit`-side gap — but worth doing now that there's something real to draw. Worth exploring
       later whether `logit` itself should compute a service graph as a component, rather than
       depending on external `metrics_generator` infrastructure to do it — unexplored, no decision
       made.
  - ~~**Internal logs** — routing `Diagnostics`' stderr output into the graph as `LogRecord` events
    is the natural next layer, and what the still-deferred `tracing` migration (above) should build
    on rather than duplicate.~~ **Closed** (`docs/plans/operator-surface.md`, workstream D):
    `logit_core::telemetry::TelemetryLayer` — a `tracing_subscriber::Layer` — captures every
    `logit`-targeted `tracing` event at or above `internal.logs`'s threshold (`warn` by default,
    `error`, or `off`) into the same per-component buffer points and spans already drain from,
    emitted as ordinary `LogRecord`-carrying `Event`s alongside them. See
    `docs/design/internal-telemetry.md`'s "Logs" section for the emit path and the bound
    (`MAX_LOGS_PER_COMPONENT`, 256, dropped and counted past the cap like spans).
  - **`host_metrics`** — facts about the machine itself (CPU, disks, NICs) are a different kind of
    source than `internal`: read from the OS rather than from `logit`'s own counters, need their
    own config, and can fail in ways an in-process atomic read never does. A separate component
    kind when it lands, not a field on `internal`.
- **A component's internal-telemetry buffer caps distinct `(name, tags)` keys at 1024**
  (`MAX_KEYS_PER_COMPONENT`, `crates/logit-core/src/telemetry.rs`) — bounds a component that
  ignores the tag-cardinality convention (`&'static str` values only) rather than letting it grow
  the process-wide interner unbounded. A dropped key is counted
  (`logit.internal.points.dropped{reason="cardinality"}`), never silent, but the cap itself is a
  fixed constant, not configurable — revisit if a legitimate component ever needs more than 1024
  distinct points between drains.
- **Lua-authored telemetry (`crates/logit-script/src/telemetry.rs`,
  [ADR `lua-authored-telemetry-cardinality`](adr/lua-authored-telemetry-cardinality.md)) trades the type-system cardinality
  guarantee the rest of `internal-telemetry.md` relies on for a convention-enforced one** — a
  script's metric name/tag value is round-tripped through the process interner rather than
  required to be a Rust `&'static str`, so nothing stops a script from building one out of per-event
  data and leaking the interner one entry at a time. Accepted for the same reason the interner's
  own never-evicting design is accepted (above): bounded in the intended, documented use (a fixed
  literal in the script's own source), and the fix if it ever isn't (a bounded per-`ScriptWorker`
  cache instead of the process-wide interner) is recorded as a considered-and-deferred alternative
  in the ADR, not undesigned.
- **The internal-telemetry component survey (`docs/design/internal-telemetry.md`'s worked-examples
  list) found several more candidates not built yet** — real, but each needs more than a
  `telemetry.count(...)` call:
  - **Process-level facts beyond what `internal` already samples** — `logit.process.memory.*` via
    jemalloc heap stats needs a new `tikv-jemalloc-ctl` dependency, and cross-crate plumbing since
    `crates/logit-inputs` (where `internal` lives) doesn't depend on `crates/logit-cli` (where the
    `jemalloc` feature is, `docs/adr/jemalloc-global-allocator.md`). `logit.process.threads`/
    `.fds`/`.cpu.seconds` would need Linux-specific `/proc` parsing. Candidate names:
    `logit.process.memory.allocated`/`.resident`, `.threads`, `.fds`, `.cpu.seconds`.
  - **`json`'s parse-outcome counts** — its two real failure modes (`no_brace`, `parse_failure`)
    already ride the `Diagnostics` bridge for free (`logit.component.diagnostics{key=...}`), so a
    dedicated metric would mostly restate what's already visible.
  - **`logit-proto`'s `frame.rs` metrics** — still a stub (see the entries above), nothing to
    instrument until an implementation exists. Candidate names, pre-committed so whoever builds it
    doesn't have to re-derive them: `logit.proto.frames{direction,codec,compression}`,
    `logit.proto.frame.bytes`, `logit.proto.errors{reason="magic"|"version"|"crc"|"truncated"}`.
    (`buffer.rs`'s own metrics are no longer on this list — implemented at the `SinkQueue` layer,
    `docs/adr/buffered-sink-delivery.md`, as `logit.component.buffer.batches`/`.bytes`/
    `.utilization`/`.push.blocked.duration` and new `reason` values on `batches.dropped`/
    `events.dropped`, per `docs/design/internal-telemetry.md`'s catalog.)
  - **Lua per-call latency, error classification, flush-tick-empty tracking** — a per-event
    `ScriptWorker::process` timing distribution would isolate one pathological event from a big
    batch (today's `logit.component.process.duration` is whole-batch), but costs a clock read per
    event; `ScriptError`'s `MissingProcess`/`Lua(...)`/malformed-return cases collapse into one
    `errors{reason="process"}` today, when a script-bug class (a malformed `flush()` return) is a
    different signal than a runtime error. Each is real; none was a default yes.
- **Every `Output::send` call allocates a boxed future, on every batch, for every sink, unrelated
  to telemetry or anything else in this file's other entries.** `Output` is `#[async_trait]`
  (`crates/logit-pipeline/src/output.rs`); the macro desugars `async fn send` into a fn returning
  `Pin<Box<dyn Future<...>>>`, so calling it — through `&mut dyn Output`, the shape `run_output`
  actually has, or even on a concrete type directly — heap-allocates its future every time.
  Measured at 1 allocation (16 bytes) per call, confirmed identical whether the call goes through a
  trait object or not (`crates/logit-bench/tests/allocations.rs`'s
  `send_batch_through_a_noop_output_disabled_telemetry`, found while adding `run_output` allocation
  coverage for the internal-spans costing exercise above — a coincidental discovery, not something
  that exercise was looking for). `Input::run` and `Transform`'s Lua-adjacent paths don't have this
  problem the same way (`Input::run` is called once per process; `Transform`/`ScriptWorker` aren't
  `#[async_trait]` at all), so this is specific to the output side, on the hottest possible
  schedule (once per batch, every sink, every pipeline). Not fixed here — and **a hand-written
  method returning `Pin<Box<dyn Future<...>>>` would not fix it either**, an earlier version of
  this entry's own suggestion, corrected in review: that return type requires exactly the same
  heap allocation to construct, whether a macro or a person wrote the method, because the box
  *is* the mechanism a `dyn Trait` object uses to return a future of unknown, implementer-varying
  size — not an artifact of `async_trait`'s codegen specifically. A real fix means giving up `dyn
  Output` for this call: either enum dispatch over the small, closed set of concrete `Output`
  kinds this project ships (`StreamOutput`/`InfluxDbOutput`/...), matched rather than boxed, so
  each variant's `async fn` compiles to its own real, unboxed future; or making the runtime
  generic per node over a concrete `Output` type, which loses the config-driven dynamic
  construction (`Box<dyn Output + Send>` built from a running config, `crates/logit-cli/src/pipeline.rs`)
  the pipeline currently relies on. Real work either way, with no forcing function yet — this
  entry is that forcing function, for whenever the output path's allocation cost becomes worth
  chasing.
- **Cross-protocol semantic gaps.** OTLP (`crates/logit-proto/src/otlp/`) is `logit`'s first
  *second* wire model, and its codec is the first place "our internal model can't cleanly express
  what a peer protocol expects" shows up as more than a one-line doc-comment footnote. Filed as its
  own entry, meant to grow as more codecs and more of OTLP's own surface (profiles, ...) get real
  mappings, rather than re-discovered by grepping doc comments across encoders each time. It has
  grown once already: the `encode (Prometheus)` rows below are the Prometheus exposition/OpenMetrics
  codec's half of the same list (`crates/logit-proto/src/prometheus/`,
  [ADR `prometheus-scrape-and-exposition`](adr/prometheus-scrape-and-exposition.md)) — a second wire
  model with its own idea of what a metric is, and the first one whose *decode* direction is
  lossless enough that every row here is on the way out. It has grown again since: the
  `encode (collectd)`/`decode (collectd)` rows are the collectd binary-protocol codec's half
  (`crates/logit-proto/src/collectd/`, [ADR `collectd-binary-relay`](adr/collectd-binary-relay.md)),
  whose module doc is the authority for every one of them — the first wire model here with *fewer*
  numeric kinds than this one rather than a differently-shaped set, which is why its rows are mostly
  "no wire form exists" rather than "the nearest shape loses something." Tracked as debt against [ADR `lossless-transit`](adr/lossless-transit.md);
  see [`docs/plans/lossless-transit.md`](plans/lossless-transit.md) for the closing assessment's residual-debt list. Every
  mapping below is deliberate, counted, and documented at its own call site — this entry exists so
  the list is in one place too:

  | Direction | Mapping | Counter | Why |
  |---|---|---|---|
  | encode | `MetricKind::Distribution` (a `DDSketch`) → OTLP `Summary` of 5 fixed quantiles (p50/p75/p90/p95/p99) | `logit.output.metrics.degraded{metric_kind="distribution"}` | OTLP has no mergeable-sketch metric type; `ExponentialHistogram` is the nearest shape, but `DDSketch` exposes no bin iteration to convert from (`crates/logit-core/src/metric.rs`), and fabricating one would repeat the "non-mergeable HyperLogLog" mistake AGENTS.md already warns against (`crates/logit-proto/src/otlp/metrics.rs`'s module doc). |
  | encode | `MetricKind::Set` (a `HyperLogLog`) → skipped entirely | `logit.output.metrics.skipped{metric_kind="set"}` | OTLP has no cardinality-estimate wire type to encode a `HyperLogLog` into (`HyperLogLog` is real now, this file's own first entry — the gap is OTLP's, not this crate's); `crates/logit-outputs/src/influxdb.rs` no longer shares this precedent, since it now renders a `Set`'s estimate as a `value=` field instead of erroring. |
  | encode | `Value::U64` above `i64::MAX` → OTLP `AnyValue.DoubleValue` | none (numeric, not a metric point) | OTLP's only integer type is signed 64-bit; exact up to `f64`'s 2^53 range, approximate above it. Any `Value::U64` (even in range) also loses the "this was unsigned" fact on decode, coming back as `Value::I64` — `otlp/common.rs`'s module doc has the full case list. |
  | encode | `Value::Timestamp` → OTLP `AnyValue.IntValue` | none | OTLP's `AnyValue` has no timestamp variant at all; decodes back as `Value::I64`, indistinguishable from a value that was always an integer. |
  | encode | `MetricKind::Samples` (raw statsd `ms`/`h`/`d` observations) → OTLP `Summary` of 5 fixed quantiles (p50/p75/p90/p95/p99), sketched into a temporary `DdSketch` first | `logit.output.metrics.degraded{metric_kind="samples"}` | Same shape as the `Distribution` row above — OTLP has no raw-sample-list metric type either, so `otlp_out` sketches first (`add_weighted` per value, weighted by `(1/sample_rate).round()` clamped to `[1, 1000]`) and takes the same degraded path ([ADR `metrics-model-v2`](adr/metrics-model-v2.md)). |
  | encode | `MetricRecord.exemplars` on a `Summary` point → dropped | none (documented) | `SummaryDataPoint` has no `exemplars` field on the wire at all (OTLP spec) — every other kind (`Sum`/`Gauge`/`Histogram`/`ExponentialHistogram`) carries them onto the wire; a `Samples`/`Distribution` degrading into a `Summary` loses its exemplars for the same structural reason (`crates/logit-proto/src/otlp/metrics.rs`'s module doc). |
  | encode/decode | `Exemplar`'s trace context (`TraceRef.flags`) → dropped on encode, hardcoded `0` on decode | none (documented) | OTLP's `Exemplar` message has no trace-flags field at all — a real, permanent lossy mapping, not a decode shortcut (`crates/logit-proto/src/otlp/metrics.rs`'s `encode_exemplar`/`decode_exemplar`). |
  | sinks with no no-value wire form / `aggregate` | A `MetricRecord` flagged `NO_RECORDED_VALUE` → skipped at `influxdb_out`/`statsd_out`, rendered as `no_recorded_value` at `stdio_out` (never dropped — a debug sink must show it), passed through unmerged at `aggregate` | `logit.output.messages.dropped{reason="no_recorded_value"}` (statsd) / throttled diagnostic key `no_recorded_value` (influxdb) / `logit.transform.metrics.passed_through{reason="no_recorded_value"}` (aggregate) | `otlp_out` keeps a flagged point on the wire and re-encodes it unchanged — that's the fixed point `docs/adr/lossless-transit.md` requires for `otlp_in -> otlp_out` — and `collectd_out` is the one other wire with a concept of its own for this: a flagged `Gauge` is written as a GAUGE `NaN`, collectd's own "no reading this interval," and decodes back flagged (`crates/logit-proto/src/collectd/mod.rs`'s module doc); every *other* kind flagged at `collectd_out` is still skipped and counted. No remaining sink or transform has a wire/model concept of "no value here," so treating the flag's default numeric payload as a genuine reading would fabricate a sample the producer never sent (`crates/logit-core/src/metric.rs`'s `flags` doc). |
  | encode (Prometheus) | A delta `Sum`/`Histogram` → **skipped** | `logit.output.metrics.skipped{metric_kind="delta_sum"\|"delta_histogram"}`, throttled diagnostic key `delta_temporality_unresolved` | Prometheus exposition has no delta temporality at all: every counter and histogram on the wire is a running total since a start time. Resolving one in the sink would mean the sink keeping per-series state and inventing a window, which is exactly what [ADR `aggregation-window-semantics`](adr/aggregation-window-semantics.md) makes an explicit, named stage — so `prometheus_out` skips and the diagnostic names the fix (`aggregate` with `temporality: cumulative`). |
  | encode (Prometheus) | `MetricKind::ExponentialHistogram` → **skipped** | `logit.output.metrics.skipped{metric_kind="exponential_histogram"}` | Neither Prometheus text 0.0.4 nor OpenMetrics 1.0 has native-histogram syntax — sparse exponential buckets exist only in Prometheus's protobuf exposition and remote-write 2.0 (`docs/design/telemetry-landscape.md`). Materializing explicit buckets from one would be the lossy conversion `MetricKind::ExponentialHistogram` exists to avoid; the gap closes with remote-write, not with a bucket-fabricating shim. |
  | encode (Prometheus) | `MetricKind::Distribution`/`Samples` → a `summary` of 5 fixed quantiles (p50/p75/p90/p95/p99) with a `_count` and **no `_sum`** | `logit.output.metrics.degraded{metric_kind="distribution"\|"samples"}` | Same shape as the OTLP `Distribution` row above, and the same shared `DISTRIBUTION_QUANTILES` constant, so one metric describes itself identically at `otlp_out` and `prometheus_out`. The missing `_sum` is honest rather than lossy: a `DDSketch` has no sum to report, and OpenMetrics permits omitting it. |
  | encode (Prometheus) | `MetricKind::Set`/`SetMembers` → a `gauge` of `estimate()` / of the distinct member count | `logit.output.metrics.degraded{metric_kind="set"\|"set_members"}` | Prometheus has no cardinality-estimate type, but unlike OTLP (which skips) it does have a plain gauge, and a cardinality *number* is a perfectly good gauge reading — the estimate is the whole point of a `Set`. What's lost is mergeability: two relays' gauges can't be combined the way their `HyperLogLog`s could. |
  | encode (Prometheus) | `Sum{Cumulative, !monotonic}` → `gauge` | `logit.output.metrics.degraded{metric_kind="non_monotonic_sum"}` | Prometheus has no non-monotonic counter: a `counter` is monotonic by definition, and exposing a decreasing one would make every `rate()` over it wrong. A gauge carries the value correctly and loses only the "this is a sum" fact, which no exposition type can express. |
  | encode (Prometheus) | A `Histogram`'s `min`/`max` → dropped | none (documented) | Neither exposition format has a per-histogram minimum or maximum field; only buckets, `_sum` and `_count`. OTLP does, so this is lost on `otlp_in -> prometheus_out` but not on `otlp_in -> otlp_out`. Tracked as a follow-up (a `_min`/`_max` convention would be an invention, not a format feature). |
  | encode (Prometheus) | Label values: `Value::Str/I64/U64/F64/Bool` stringified; `Null/Bytes/Timestamp/Array/Map` dropped | `logit.output.labels.dropped{reason="unrepresentable"}` | Prometheus labels are always strings, so every kind that has a faithful string form gets one and the type is lost (a `Value::I64(3)` and a `Value::Str("3")` become the same label). The five dropped kinds have no honest string form: a `Bytes` need not be UTF-8, and flattening an `Array`/`Map` into one label value would invent a syntax nothing parses back. |
  | encode (Prometheus) | Name/label sanitization: every byte outside `[a-zA-Z0-9_:]` (metric) / `[a-zA-Z0-9_]` (label) → `_`, a leading digit → `_` prefix; two labels colliding after that keep the one whose original name sorts first, and an attribute colliding with a generated `le`/`quantile` is dropped | `logit.output.labels.dropped{reason="collision"\|"reserved"}` | Substitution, not deletion, is the `statsd_out` precedent (`crates/logit-outputs/src/statsd.rs`'s `sanitize_into`): distinct inputs stay distinct in the common case. Collisions are the residue — `a.b` and `a-b` are one label name on the wire; the *metric*-name case is resolved the same way and counted in its own row below. Prometheus 3's quoted UTF-8 names would remove the need for most of this; supporting them is a tracked follow-up. |
  | encode (Prometheus) | `EventBatch::scope`, `Resource::schema_url`, and every `dropped_attributes_count` → dropped | none (documented) | The exposition format has no scope, schema or dropped-count concept — a family is a name, a type, two metadata strings and a set of labelled samples, full stop. `otlp_in -> prometheus_out` therefore loses instrumentation-scope identity; `otlp_in -> otlp_out` does not. Rendering a scope as `otel_scope_name`/`otel_scope_version` labels (OTel's own Prometheus convention) is a tracked follow-up, not a silent default. |
  | encode (Prometheus) | Two model names sanitizing onto one wire name → the family whose model name sorts first is exposed, the rest **skipped** | `logit.output.metrics.skipped{reason="name_collision"}` | Exposing both would be *invalid*, not merely lossy: a second `# TYPE` line for one name (or the same series twice) makes Prometheus reject the whole scrape, so one naming clash would poison every other metric in the body. Resolved deterministically on the model names rather than on arrival order, the same way a post-sanitization *label* collision is (the sanitization row above). |
  | encode (Prometheus) | An OpenMetrics `# UNIT` whose unit is not the family name's `_<unit>` suffix, or carries anything outside `[a-zA-Z0-9_]` → dropped | `logit.output.metrics.degraded{reason="unit_not_suffix"}` | OpenMetrics 1.0 requires "an underscore and the unit MUST be the suffix of the MetricFamily name", and Prometheus's parser fails the *entire* body when it isn't (`unit %q not a suffix of metric %q`) — so an OTLP-sourced `MetricRecord { name: "request_duration", unit: "s" }` has to lose its unit rather than take every other family down with it. Appending the unit to the name instead (what Prometheus's own OTLP translation does) is a tracked follow-up, not something to do silently. |
  | encode (Prometheus) | An exemplar with no OpenMetrics line to sit on → dropped | `logit.output.metrics.degraded{reason="exemplar_dropped"}` | OpenMetrics allows one exemplar per `_total`/`_bucket` line ("a bucket MUST NOT have more than one exemplar") and caps its label set at 128 code points. So a counter carrying N exemplars keeps one, two exemplars whose values fall in one bucket's range keep one, and an over-budget label set keeps none — truncating a trace id would make it a lie. Text 0.0.4 drops every exemplar *uncounted*: that is the operator's dialect choice, on the permitted-normalization list rather than here. |
  | encode (Prometheus) | Two records sharing one name but disagreeing on family type → the first type wins, the rest **skipped** | `logit.output.metrics.skipped{reason="type_conflict"}` | The wire has exactly one `# TYPE` line per name, so a `Gauge` and a `Sum` of the same name cannot both be exposed — and Prometheus rejects a body that tries. `prometheus_out`'s registry has the same conflict at a longer time scale (a series changing type between scrapes) and resolves it by replacing the family, counted separately. |
  | encode (OTLP + Prometheus) | `MetricKind::GaugeDelta` → **skipped** at every sink | `logit.output.metrics.skipped{metric_kind="gauge_delta"}`, throttled diagnostic key `gauge_delta_unresolved` | A relative gauge adjustment is explicitly *unresolved* ([ADR `relative-gauge-adjustments`](adr/relative-gauge-adjustments.md)): only `aggregate` carries the running gauge value a delta applies against. No wire format has a "adjust the previous value by" concept, so encoding one as an absolute reading would silently invent a value. Every sink reports it under the one greppable diagnostic key, so a missing `aggregate` is findable with a single grep regardless of which output noticed. |
  | decode (collectd) | A `0x0200` Signature part → skipped, **unverified**; a `0x0210` Encryption part → the rest of the datagram dropped | none (Signature) / `logit.component.diagnostics{key="encrypted_packet_dropped"}` (Encryption) | collectd's `SecurityLevel Sign`/`Encrypt` are deferred, deliberately (`docs/plans/collectd-binary-relay.md`'s settled decisions): a signed packet's payload is plaintext, so it still decodes — it just isn't authenticated, and an operator who needs that today should keep the listener on a trusted network. An encrypted one has no plaintext at all, so there is nothing to skip *to*; the tail goes, counted. Signature verification means a shared-secret config surface (`AuthFile`) and a real HMAC-SHA-256/AES-256 implementation, which is its own piece of work, not a line in the codec. |
  | decode (collectd) | A COUNTER/ABSOLUTE above 2⁵³ → `Sum { value: f64 }`, approximate | none | Exactly the `Value::U64`/int-double row above, one layer down: `logit_core::Sum.value` is an `f64`, so a 64-bit wire counter is exact only to 2⁵³ (~9.0e15). Re-encoding is stable — the same `f64` produces the same `u64` every time, and the top of the range round-trips through a saturating cast — so `collectd_in -> collectd_out` is still a fixed point; the number simply isn't the one the sender had above 2⁵³. A typed integer metric value is a core-model question, outside this codec. |
  | encode (collectd) | A `Sum` that is non-finite, has a fractional part, or falls outside its target integer range → **dropped** | `logit.output.metrics.skipped{reason="unencodable_value"}`, throttled diagnostic key `unencodable_value` | COUNTER, DERIVE and ABSOLUTE are integers on the wire; there is no fractional data-source type to degrade into. Rounding would fabricate a value nobody sent — statsd's `page.views:2\|c\|@0.3` reaches a sink as `6.666…`, and both `6` and `7` are wrong. `GAUGE` is the only floating-point type collectd has, and relabelling a counter as a gauge would lose the "this is a sum" fact the way the Prometheus non-monotonic row above does, without even the excuse that the value survives. |
  | encode (collectd) | `Samples`, `Distribution`, `SetMembers`, `Set`, `Histogram`, `ExponentialHistogram`, `Summary`, and a delta non-monotonic `Sum` → **skipped**, one exhaustive `match` arm each | `logit.output.metrics.skipped{metric_kind="samples"\|"distribution"\|"set_members"\|"set"\|"histogram"\|"exponential_histogram"\|"summary"\|"non_monotonic_delta_sum"}` | collectd has exactly four data-source types (COUNTER, GAUGE, DERIVE, ABSOLUTE), all scalar: no bucket, quantile, sketch or member-set wire form exists to degrade into, and no delta type that can decrease (ABSOLUTE is delta-*monotonic*). Unlike `prometheus_out`, which at least has a plain gauge to render a cardinality estimate onto, there is nothing here that would not be an invented convention. `Summary` is the one arguable case — its quantiles could be N gauges under synthetic type instances — and it is deliberately not done: that is a naming convention a receiver's `types.db` knows nothing about. |
  | encode (collectd) | `MetricRecord`'s `unit`, `description`, `start_timestamp` and `exemplars`; `EventBatch::scope`; `Resource::schema_url`; every `dropped_attributes_count`; and every attribute outside the `collectd.` namespace | none for the first group (documented); `logit.output.tags.dropped{reason="no_wire_form"}` for the attributes | The protocol has no field for any of them: a value list is an identity five-tuple, a time, an interval and N numbers, full stop. The attribute case is the one worth stating plainly — **collectd has no tag concept at all**, so a DogStatsD tag or an OTLP resource attribute reaching `collectd_out` has nowhere to go and is counted rather than folded into the type instance (which would collide with the real one and change the series identity a receiver keys on). `host.name` is counted here too: the host resolution reads it, but the attribute itself still has no wire form. |
  | encode/decode (collectd) | A value list of more than `MAX_VALUES_PER_LIST` (64) data sources → on decode the part is malformed and the rest of the datagram is abandoned; on encode the list is dropped whole | `logit.component.diagnostics{key="bad_part"}` or `CodecError::Malformed` (decode) / `logit.output.metrics.skipped{reason="too_many_values"}` + diagnostic key `too_many_values` (encode) | The wire allows up to `(65535 - 6) / 9 = 7281`, but nothing real comes close (`load` has 3, `if_octets` 2, `disk_io_time` 2). The cap is deliberately pair-wide rather than decode-only: an over-long list fits easily under `max_packet_bytes`, so without the encode-side half a relay would emit lists that any receiver running this codec rejects — abandoning every unrelated list packed behind them in the same datagram — and `aggregate`/`kv_metrics` can both put far more than 64 records on one event. What the constant bounds is the decoder's per-part work and the per-list record-name suffix fan-out (`<plugin>.<type>.<i>`); it is **not** a bound on interner growth, whose unbounded axis is distinct `<plugin>`/`<type>` strings — the same exposure `statsd_in`'s wire-chosen metric names have, accepted on `docs/design/memory.md` §4's "listeners are private" premise. A legitimate producer hitting the cap would be a real gap worth raising the constant for; nothing known does. |
  | encode (Prometheus) | A `MetricRecord` flagged `NO_RECORDED_VALUE` → **skipped** | `logit.output.metrics.skipped{reason="no_recorded_value"}` | The same rule every sink with no no-value wire form follows (the `sinks with no no-value wire form / `aggregate`` row above): exposition has no "no value here" marker, so emitting the flag's default numeric payload would fabricate a reading the producer never sent. Prometheus's own staleness handling is a scrape-level concept (a series that stops appearing), which a relay cannot synthesize from one flagged point. |

  One residual, narrower gap in the same codec, not yet worth its own table row: `BodyFormat` has
  no OTLP field of its own and round-trips through a reserved attribute (`logit.body_format`)
  instead — lossless, just an attribute-shaped workaround, documented in `otlp/logs.rs`'s own module
  doc; [ADR `lossless-transit`](adr/lossless-transit.md) rule (c) names it the standing example of a
  `logit`-only concept with nowhere else on the wire to go, so unlike the rows closed below this one
  stays. (A bare `LogRecord`'s OTLP `trace_id`/`span_id`/`flags` fields used to be filed here too —
  closed, `logit_core::LogRecord::trace` now carries them,
  [ADR `log-record-trace-context`](adr/log-record-trace-context.md). A span's `Status.message` was
  the same shape — closed in W4 too: `SpanRecord.ext`'s boxed `SpanExt.status_message`
  ([ADR `metrics-model-v2`](adr/metrics-model-v2.md)) is a real field now, and `otlp/traces.rs` no
  longer stamps or reads `otel.status_message`. A `NO_RECORDED_VALUE`-flagged data point used to be
  skipped on decode too — closed the same amendment: `MetricRecord.flags` carries the bit forward
  and the point round-trips instead of being dropped.)

  Both `Distribution`→`Summary` and `Set`→skip are a real, if narrow, qualification of
  [ADR `native-wire-format-with-otlp-bridge`](adr/native-wire-format-with-otlp-bridge.md)'s claim that the internal model "must
  be a superset of what OTLP can express, or the OTLP codec becomes lossy": here it's `logit`'s own
  model — a mergeable sketch, a mergeable cardinality estimator — that can't be losslessly re-expressed *as* OTLP,
  the direction ADR `native-wire-format-with-otlp-bridge` didn't anticipate. See
  [ADR `committed-pregenerated-otlp-protobuf`](adr/committed-pregenerated-otlp-protobuf.md)'s Consequences section for that
  qualification stated plainly, and `crates/logit-proto/src/otlp/metrics.rs`'s module doc for the
  full encode/decode tables this summarizes.

- **`otlp_in`'s `partial_success` response is always empty.** OTLP's
  `Export*ServiceResponse.partial_success` field exists so a receiver can accept most of a request
  while reporting which records it rejected — `otlp_out` (`crates/logit-outputs/src/otlp.rs`) fully
  implements the *reading* half of this (see its `a_partial_success_response_is_counted_not_failed`
  tests). But `logit_proto::SignalDecoder::decode_signal` doesn't return a per-call skip/reject
  count today — only a self-telemetry counter (`logit.input.metrics.skipped{metric_kind, reason}`)
  — so there's nothing for `otlp_in` to echo back into the wire response yet: every successful
  decode replies with an empty (all-default, meaning "fully accepted") `partial_success`, even when
  the request silently skipped a metric point internally — the one remaining case is a `Metric`
  whose `data` oneof isn't set at all (`crates/logit-proto/src/otlp/metrics.rs::decode_metric`'s
  `None => Vec::new()` arm); an over-cap exponential histogram and a `NO_RECORDED_VALUE`-flagged
  point both round-trip in full now and are no longer examples of this (W4,
  [ADR `metrics-model-v2`](adr/metrics-model-v2.md)). A fully malformed request (bad protobuf, an invalid span id)
  still correctly fails the *whole* request (`400`/`grpc-status: 3`), which is the one shape
  `otlp_in`'s response *does* reflect today. Threading a real per-call count through would be a
  `SignalDecoder` API change (`crates/logit-proto`), out of scope for the PR that added `otlp_in`
  itself — a natural next step whenever OTLP input volume makes the gap worth closing. Now that
  `otlp_in` speaks two wire encodings (below), closing this means rendering the per-signal reject
  count as `rejectedSpans`/`rejectedLogRecords`/`rejectedDataPoints` on the JSON path — the JSON key
  differs per [`Signal`], where the protobuf field shares one tag number across all three
  `Export*ServiceResponse` messages (`export_response_json`'s doc comment,
  `crates/logit-inputs/src/otlp.rs`).
  (Compression was the other half of this entry — `otlp_in` now decodes gzip on both transports,
  bounded the same way `otlp_out` bounds it on encode; see
  [ADR `otlp-compression-and-decompression-bounds`](adr/otlp-compression-and-decompression-bounds.md).)

- **`otlp_in` only accepted OTLP/protobuf, not OTLP/JSON — closed.** `otlp_in`
  (`crates/logit-inputs/src/otlp.rs`) now accepts `Content-Type: application/json` alongside
  protobuf on the HTTP transport, decoding through a hand-written dialect layer
  (`crates/logit-proto/src/otlp/json/`) onto the same generated types and decode path the protobuf
  side already used. See [ADR `otlp-json-decoding`](adr/otlp-json-decoding.md) for the design (and
  for why `pbjson`/generated `serde::Deserialize` impls were rejected — OTLP's hex trace/span ids
  are exactly where OTLP deviates from proto3 JSON's own bytes-as-base64 rule, which those
  generators implement faithfully and can't be told to skip for one field type without hand-editing
  generated code). What's still open, tracked below and in that ADR's Consequences: CORS, the
  `text/plain` error-body deviation, and the JSON path's real (if still bounded) memory cost
  relative to protobuf.

- **`otlp_in` has no CORS support — `OPTIONS` 404s, no `Access-Control-Allow-Origin`.** A browser
  exporter posting cross-origin to `otlp_in` fails at preflight: `handle_http` answers any
  non-`POST` method, `OPTIONS` included, with a `404`
  (`crates/logit-inputs/src/otlp.rs`). Same-origin export (a reverse proxy in front of both the
  page and `otlp_in`, e.g. `demo/haproxy/haproxy.cfg` routing `/v1/traces` to `logit`) sidesteps
  this entirely and is the supported path today — see `docs/plans/browser-tracing.md`. A real
  `cors:` config surface (allowed origins, an `OPTIONS` handler, response headers) is unbuilt; it's
  a config/security surface in its own right (an allowed-origins list, whether a reflexive `*` is
  ever appropriate) rather than something to fold into the OTLP/JSON decoding work that made it
  worth naming.

- **`otlp_in` answers every 4xx/5xx with `text/plain`, on both encodings — the spec wants a
  protobuf-encoded `Status`.** *"The response body for all HTTP 4xx and HTTP 5xx responses MUST be
  a Protobuf-encoded Status message"* — `text_response` (`crates/logit-inputs/src/otlp.rs`) always
  builds a plain-text body instead, for both the protobuf and the JSON request path. Pre-existing
  on the protobuf side since `otlp_in` first shipped, not something OTLP/JSON support introduced;
  left alone when JSON support landed since building a `google.rpc.Status` encoder is orthogonal to
  decoding and every real client checked (including `opentelemetry-js`) only reads the HTTP status
  code on error, never the error body's content-type.

- **An OTLP/JSON request costs more peak memory per byte than a same-sized protobuf one, under the
  same `MAX_REQUEST_BYTES` cap.** The JSON path parses into a `serde_json::Value` tree
  (`crates/logit-proto/src/otlp/json/`) before any of it reaches the decoded event model — a
  `Map`/`Vec`/`String`/`Number` allocation per JSON node — where `prost::Message::decode` builds
  the target structs directly with no such intermediate tree. `MAX_CONCURRENT_CONNECTIONS`'s doc
  comment (`crates/logit-inputs/src/otlp.rs`) states the bound this doesn't break (worst case
  across all connections is still a real, finite multiple of the existing 4 GiB figure, not
  unbounded) without asserting a measured multiplier — nobody has profiled one yet. Worth doing
  before OTLP/JSON sees production volume.

- **`otlp_out` aborts an entire batch's `send` on the first signal request that fails -- pointed at
  a signal-partial backend fed by a mixed-signal source, that's not just noise, it can end the
  process.** Discovered running `demo/`'s `tempo_out` against Tempo
  ([docs/plans/otlp-end-to-end.md](plans/otlp-end-to-end.md)), not anticipated by that
  plan. `internal` (`self`, observing `logit`'s own pipeline) doesn't distinguish signals -- every
  drain carries both spans and this process's own `logit.*` metrics (all `Sum`/`Gauge`/
  `Distribution`, all mergeable). `OtlpOutput::send` (`crates/logit-outputs/src/otlp.rs`) issues
  one request per non-empty signal, sequentially (traces before metrics, per `encode_signals`'
  fixed ordering), and `?`-propagates the first failure without attempting the rest. Tempo is a
  traces-only OTLP receiver -- it registers a `TraceService` but no `MetricsService` -- so a batch
  mixing both sees its traces request succeed and its metrics request that follows fail with
  `grpc-status: 12` (`UNIMPLEMENTED`, correctly classified `Fault::Permanent`, correctly not
  retried). `write_loop` sees one failed `send` and drops the whole batch -- a batch whose trace
  payload had already, successfully, separately reached Tempo moments earlier, confirmed directly
  against Tempo's `/api/search`/`/api/traces` endpoints.

  **That alone is recoverable noise. Pointed straight at `self` with nothing in between, it is not
  recoverable at all.** `self`'s 10s drain interval meant *every* `tempo_out` batch mixed both
  signals, so `send` never once returned `Ok`, `last_success` never advanced, and `write_loop`'s
  ~60s sustained-permanent-failure guard
  ([ADR `service-lifecycle-and-output-retry`](adr/service-lifecycle-and-output-retry.md), revised by
  [ADR `buffered-sink-delivery`](adr/buffered-sink-delivery.md)) killed the entire `logit` process about a minute
  after startup -- taking the InfluxDB metrics path down with it, not just Tempo. That guard exists
  specifically to end a process stuck on a genuine misconfiguration (a bad token, a bad bucket); a
  demo whose "misconfiguration" is actually two signals correctly reaching a backend that only
  wants one is exactly the false-positive case it wasn't built to distinguish. `demo/logit.yaml`
  fixes this at the config layer: `trace_only` (`type: has_signal`, `signals: [traces]`) sits
  between `self` and `tempo_out`, dropping every metric-only drain and forwarding every span-only
  one untouched ([ADR `signal-filtering-components`](adr/signal-filtering-components.md)) -- unlike
  the `aggregate`-based workaround this replaced, `has_signal` never mutates a forwarded event and
  never lets a metrics-only batch reach `tempo_out` at all, so the guard's streak never resets from
  a near-miss; there's simply nothing left for it to trip on.

  This is specific to pointing `otlp_out` at a mixed-signal source feeding a signal-partial
  backend -- a production `otlp_out` scoped to a source that only ever carries the signals its
  destination accepts would never hit either half of this. `has_signal` (and its
  payload-stripping siblings `keep_signals`/`drop_signals`) is the general config-layer fix; a
  per-signal partial-failure mode on `OtlpOutput::send` that doesn't abort sibling signals already
  in flight and doesn't let one incompatible signal alone trip the sustained-failure guard for
  signals that are succeeding remains a separate, unfiled possible improvement to `otlp_out`
  itself. `demo/logit.yaml`'s `trace_only`/`tempo_out` components carry this same explanation
  inline.

- **No mechanism exists anywhere in `logit` to attach a static attribute to a batch's resource** —
  found in the same investigation
  ([docs/plans/otlp-logs-and-resource-identity.md](plans/otlp-logs-and-resource-identity.md),
  workstream A). Not config (no `attributes`/`labels`/`tags`/`resource` field on any input), not any
  transform (`keep`/`json`/`kv_metrics`/`aggregate` only filter or derive), and Lua can mutate only
  *event* attributes (`crates/logit-script/src/proxy.rs`'s `AttrsProxy`), never a resource. This is
  what blocks giving `syslog_in`/`statsd_in` traffic a real `service.name` for OTLP-native backends
  (Loki's index labels among them) without the just-landed rule that `logit`'s own code must not
  invent one. The plan's workstream A sketches the fix — an operator-declared `resource:` config
  field, landing on a new ADR distinguishing "code invents an identity" (still forbidden) from "an
  operator configuring the pipeline declares one" (fine, same category as `syslog_out`'s existing
  `hostname`/`app_name` fields) — plus the demo-stack workstreams (B, C, D) it would unblock.

- **TLS certificates (`otlp_out`'s `tls:`, `otlp_in`'s `tls:`) are loaded once at startup; rotation
  needs a restart.** `OtlpOutput::with_tls`/`OtlpInput::with_tls`
  (`crates/logit-outputs/`/`crates/logit-inputs/src/otlp.rs`) read every PEM file at construction
  time (`logit run` startup) and build a static `rustls::ClientConfig`/`ServerConfig` from it — a
  renewed certificate (a 90-day Let's Encrypt cert, a `cert-manager`-issued one) has no effect until
  the process restarts. [ADR `otlp-tls-and-pooled-grpc-client`](adr/otlp-tls-and-pooled-grpc-client.md)
  files this as deliberately out of scope; closing it means `rustls::ServerConfig`'s
  `ResolvesServerCert` (a file-watcher hook) on the server side, or an equivalent reload on the
  client side, either behind a SIGHUP or a poll.
- **`otlp_out`'s TLS client has no `server_name` override.** Useful when an endpoint is reached by
  IP or through a proxy whose certificate names something else (OTel's own
  `tls.server_name_override` knob). Cheap to add via `hyper-rustls`'s
  `HttpsConnectorBuilder::with_server_name_resolver` and an equivalent override on the `reqwest`
  side; left out of the initial TLS work to keep it small
  ([ADR `otlp-tls-and-pooled-grpc-client`](adr/otlp-tls-and-pooled-grpc-client.md)).

- **`docker_in` never notices a `docker rename` after a container's log file is first opened.**
  `container.name` is read once, from `config.v2.json`, at open time, and never re-read for the
  life of that file handle — a rename after that point is invisible until the container restarts
  (a new inode, a fresh `open`). Closing this would mean either watching `config.v2.json` itself
  (a second file per container to track) or moving to the docker socket/API, which reports renames
  as events. See [ADR `file-tailing-and-docker-json-logs`](adr/file-tailing-and-docker-json-logs.md).
- **`docker_in` only speaks the json-file log driver.** Docker also supports `local`,
  `journald`, `syslog`, and others as the configured logging driver; none of the others write a
  per-container file this driver could tail at all. Genuinely different work per driver, not a
  parameter on this one.
- **`tail_in`/`docker_in`'s checkpoint identity is `(dev, ino)`, which doesn't survive a bind
  mount or filesystem migration that preserves content but not inode numbers.** A restored backup,
  a volume moved to different storage, or a bind mount re-created from a snapshot all resume from
  the beginning rather than the checkpointed offset — safe (at-least-once still holds), just not
  the seamless resume the common case gets.
- **`inotify` doesn't reliably fire over network or FUSE-backed mounts** (NFS chief among them) —
  `watch: auto` falls back to polling only on outright setup failure, not on a mount type it can't
  detect in advance, so a config on such a mount should set `watch: poll` explicitly rather than
  relying on `auto` to notice. `poll_interval` is the only mechanism proven to work everywhere.
- **`tail_in`/`docker_in`'s `inotify` wake source is Linux-only** — every other platform runs
  `watch: poll` unconditionally regardless of config, and an explicit `watch: inotify` is a startup
  error rather than a silent downgrade.
- **`docker_in`'s timestamps are the one deliberate exception among the tailing decoders to
  "stamp receipt time."** It uses the json-file envelope's own `time` field (the daemon's
  same-host clock) instead, since replaying a backlog (`read_from: beginning`, or a fresh
  container's already-written history) as "now" would misrepresent when those lines actually
  happened — see the ADR's "docker_in timestamps" section. `tail_in` itself still follows the
  general rule (read time, matching `syslog_in`'s own precedent) — a plain text line carries no
  timestamp of its own to trust. Receipt time isn't a repo-wide invariant either: `otlp_in`
  independently prefers a record's own `time_unix_nano` when the sender set one, falling back to
  `observed_time_unix_nano` only for the zero "unknown" sentinel — a wire format that carries an
  origin timestamp is trusted for it. `observed_time_unix_nano` itself is preserved the same way,
  both directions: decode copies it onto `LogRecord.observed_timestamp` verbatim (`0` stays `0`),
  and encode prefers that field over the wall clock whenever it is non-zero — what makes
  `otlp_in -> otlp_out` a fixed point for this field too (`otlp/logs.rs`'s module doc,
  [ADR `metrics-model-v2`](adr/metrics-model-v2.md)'s W4 amendment).
- **No per-input stream filter on `docker_in`** — an operator who wants only `stdout` (or only
  `stderr`) needs a downstream stage reading `log.iostream` themselves (`demo/logit.yaml`'s
  `nginx_stdout`, an inline `lua` component, is the worked example), not a config field on
  `docker_in` itself. Considered and set aside alongside named output ports (next entry) — see the
  ADR's "Alternatives considered".
- **Named output ports on a component (a listener publishing separate named streams other
  components subscribe to individually, e.g. `docker_in` publishing `stdout`/`stderr` as two
  distinct sources) don't exist.** Touches the component graph's core arity/wiring model broadly
  enough to be its own design, not a `docker_in`-sized increment — deferred when scoping the
  file-tailing work, revisit if a second, unrelated need for the same shape shows up (a future
  `splitter`-style component fanning a multi-signal event out into separate logs/metrics/traces
  streams was the other motivating case raised and set aside at the same time). See the ADR's
  "Alternatives considered".
- **`config.v2.json` is an internal Docker daemon format, not a documented public API** — `docker_in`
  reads it directly (no socket, no HTTP client) because it's already sitting right next to the log
  file it's already reading, but a Docker version bump could change its shape with no deprecation
  notice. A missing or unparseable file already degrades gracefully (a `container.id`-only
  resource, diagnosed `metadata_error`); a *silently reshaped* file that still parses but means
  something different is the residual risk this doesn't catch.
- **The admin endpoint has no TLS and no auth** (`docs/plans/operator-surface.md`, ADR
  `admin-readiness-endpoint`) — `/readyz`/`/healthz` are loopback/pod-local by design, not meant to
  cross a real network boundary; anyone who can reach `admin.bind` can read the pipeline's
  lifecycle phase and every component's coarse state. Deliberate, not deferred: adding either would
  protect against a threat model this endpoint doesn't have, for a caller that's already inside
  the process's own network namespace.
- **`prometheus_out` has no TLS and no auth either** (ADR `prometheus-scrape-and-exposition`'s
  "Security posture") — anyone who can reach its `bind:` reads the entire registry: every label on
  every series the sink currently holds. Two things make this a *deferred* gap rather than the
  deliberate non-goal the admin endpoint's is. Its `bind:` is required, not off-by-default, so
  every `prometheus_out` in existence is listening; and the payload is the full metric surface,
  which can carry far more about a deployment than a lifecycle phase. `examples/prometheus-expose.yaml`
  therefore binds `127.0.0.1`, and the field's own doc comment says to keep it loopback or pod-local
  and front it with something that has both. Real server-side TLS would reuse `logit-inputs`'
  existing builder (`otlp_in`'s `tls:`); auth has no in-tree precedent on any listener yet, so it
  needs a decision about what kind (bearer, mTLS) before it needs code.
- **Prometheus remote-write is not built** — `prometheus_in`/`prometheus_out` cover scrape and
  exposition only (ADR `prometheus-scrape-and-exposition`'s "Transports" section). The seam is
  already there for it: the wire↔model mapping lives behind `MetricFamily`
  (`logit_proto::prometheus::mod`), independent of `text.rs`'s syntax, so a `remote_write.rs`
  mapping prompb messages to and from that same type adds no change to the "Model mapping" tables
  above; `prometheus_in` would gain an optional `bind:` (a receiver) and `prometheus_out` an
  optional `endpoint:` (a sender), each mutually exclusive with today's field by a graph rule —
  additive, not a breaking change to a config already shipped. Deferred because every existing
  Prometheus deployment already scrapes/exposes text, and remote-write needs vendored protobuf
  types (`crates/logit-proto/proto/prometheus/`, regenerated by `tools/protogen` per ADR
  `committed-pregenerated-otlp-protobuf`) plus Snappy framing (`snap`, already allowed in
  `deny.toml`) before anything at all can round-trip.
- **Readiness is per-process, not per-sink.** A single sink stuck retrying (`degraded`, in the
  self-logging sense) does not flip `/readyz` to unready — that's what a sink's own `buffer:`
  block (retry budget, queue depth) exists to absorb. `/readyz`'s `degraded` phase is reserved for
  a node that has actually exited with an error, not one that's merely behind. A richer per-sink
  probe is additive to `PipelineState.components` (already keyed by component id) should a real
  need for it show up — not built now because nothing has asked for it yet.
- **A Lua node's post-startup failure is invisible to `/readyz`.** A Lua component runs on a raw
  `std::thread`, not inside the `JoinSet` `run_with_telemetry` otherwise watches every task
  through — its `NodeState` reaches `Running` once it reports ready and stays there for the rest
  of the run, even if the thread later panics or its script errors out unrecoverably. True before
  workstream B (`docs/plans/operator-surface.md`) and unchanged by it; the readiness side simply
  can't see what the join loop never sees either. A bad `lua_file`/`Lua` script that fails to
  *load* is still caught (the startup handshake `run_with_telemetry`'s spawn loop already does) —
  this gap is specifically about a failure *after* that handshake succeeds.
- **No config hot reload on SIGHUP.** A config change means a restart; SIGHUP gets no special
  handling today. Explicitly out of scope for `docs/plans/operator-surface.md` — it needs its own
  design (diffing the old and new resolved `Graph`, deciding which components can be reused versus
  torn down and rebuilt), not a small addition to the readiness/exit-code work.
- **No `logit stats` command reading a live `Registry` out-of-process.** Every current way to see
  `internal`'s telemetry is *through* the pipeline (a sink attached downstream) — there's no
  separate out-of-band read path the way `/readyz`/`/healthz` are for lifecycle state. Considered
  and set aside alongside the admin endpoint (`docs/plans/operator-surface.md`): building a second
  read path before an operator has actually asked for one would be speculative, the same reasoning
  ADR `internal-telemetry-as-pipeline-events` already gives for not building a `Registry` addressable
  outside the pipeline.
- **Internal-log sampling is the existing per-key occurrence throttle, nothing finer.**
  `Diagnostics::warn_throttled`'s powers-of-two throttle is what bounds a chatty diagnostic's
  volume before it ever reaches `tracing`; `TelemetryLayer` itself applies no further sampling or
  rate limiting once an event is emitted. A component that logs at `warn`/`error` outside that
  throttle (a lifecycle event, an unthrottled `Diagnostics::error`) has no rate limit at all beyond
  `MAX_LOGS_PER_COMPONENT`'s bound-and-drop. Not built now — nothing shipped needs it, and the
  throttle already covers the actual hot path (a malformed line, a parse failure) this would
  otherwise protect.
