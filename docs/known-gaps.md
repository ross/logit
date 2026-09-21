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
  the measured numbers. Concretely: **9** allocations / **1.61 µs** per event through a `lua`
  component versus **1** allocation / **525 ns** through a native `Transform`
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
  them unchanged. **Narrowed again on 2026-09-13:** destination selection — splitting one flow into
  named streams — is native now: `route` (equality on provenance/attribute/resource) and `target`
  components (ADR [`target-components`](adr/target-components.md)), and `lua` can route with
  `event:to`. Sampling, throttling, dedup, and anything needing an operator remain Lua-only.
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
  - **`logit_in`'s and `internal`'s shutdown grace is fixed at 5s, not operator-tunable** — graph
    validation's rule 17 rejects a `receive:` block on either (neither is a datagram or tail
    listener), so both always get `ReceiveConfig::default().shutdown_grace` with no config-level
    way to change it, even though both genuinely use that grace: `LogitInput` to close idle
    connections cleanly on shutdown, `InternalInput` for its final drain of buffered self-telemetry
    (`crates/logit-inputs/src/internal.rs`). A real gap if a deployment ever needs a different
    number, not yet a `receive:`-shaped knob.
  - **`otlp_in` can hold the graph open past shutdown.** Every connection `OtlpInput::run` spawns
    holds its own `Fanout` clone but the input never overrides `Input::run_until_shutdown` the way
    `logit_in` now does (`crates/logit-inputs/src/logit.rs`'s own module doc comment) — an idle
    keep-alive HTTP/gRPC connection at shutdown time can leave its `Fanout` clone open
    indefinitely, which the cancel-by-drop shutdown mechanism ([ADR
    `service-lifecycle-and-output-retry`](adr/service-lifecycle-and-output-retry.md)) depends on
    every listener eventually releasing. Not fixed here — `logit_in`'s design is the pattern to
    follow when this is addressed. **Narrowed as of 2026-09-14:** a connection an operator-configured
    `idle_timeout:` has already closed no longer holds anything open at shutdown time — this row
    is now only about a connection still within its idle budget (or with none configured) when
    shutdown begins.
  - ~~**`otlp_in`'s TLS accept has no timeout.**~~ — **closed as of 2026-09-13** for the TLS
    accept itself, which is all this row ever claimed. `crate::otlp::run`'s
    `acceptor.accept(stream)` is now wrapped in `tokio::time::timeout` against an
    `OtlpInput::handshake_timeout` field, exactly the pattern `logit_in` already used; the timeout
    and a handshake failure both surface through the same per-connection `connection_error`
    diagnostic, and the permit comes back because the task ends. `handshake_timeout:` is an
    operator-facing field on `syslog_in`, `logit_in`, and `otlp_in` alike now, 5s by default,
    non-zero per graph rule 45. **What it does not close, on `otlp_in`:** on the *TLS* arm it
    bounds the TLS accept and nothing after it, because this listener hands each accepted stream
    straight to `hyper`, whose `hyper_util::server::conn::auto::Builder` reads the connection's
    first bytes itself to tell HTTP/1.1 from an h2 preface — a read this module never sees and
    cannot wrap without reimplementing that sniff, and one `http1().header_read_timeout(..)` does
    not cover either (that starts only once the version is already decided; `protocol: grpc`, on
    `hyper::server::conn::http2::Builder`, has no equivalent knob at all). The *plaintext* arm
    also has a bound now (closed 2026-09-14, [ADR
    `otlp-tls-and-pooled-grpc-client`](adr/otlp-tls-and-pooled-grpc-client.md)'s amendment): a
    `TcpStream::peek` under this same `handshake_timeout` before the stream ever reaches `hyper`,
    so a connection that sends zero bytes is closed on both arms within the budget. What happens
    to a *TLS* `otlp_in` connection that finishes its handshake and then says nothing, or a
    *plaintext* one that sends its one peeked byte and then says nothing, is the
    idle-connection-timeout row below's `idle_timeout:` field, opt-in and closed 2026-09-14 — not
    something this row (a pre-message bound) ever covered.
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
    drops itself (`logit.component.datagrams.dropped`). It narrowed the rest of the way with the
    per-socket kernel counters below — the kernel's own drop, before `logit` ever sees the
    datagram, is counted too now (`logit.input.kernel.drops`). Every receive-side loss path on a
    UDP listener is therefore attributable to a component; what this entry still names is the
    *delivery* side, past the first hop.
  - **No out-of-order/credit-based acknowledgement** — see the native wire protocol entry above's
    "Credit-based flow control" bullet; `SinkQueue` is deliberately in-order and single-in-flight
    (one queue, one writer, `peek`-then-`commit`-the-head only) until that lands.
- ~~**No visibility into the kernel's own UDP receive-buffer drops.**~~ — **closed** (ADR
  [`udp-intake-batching-and-socket-visibility`](adr/udp-intake-batching-and-socket-visibility.md)).
  A listener's `ReceiveQueue` ([ADR `decoupled-listener-io`](adr/decoupled-listener-io.md),
  directly above) always counted every datagram *it* dropped; a datagram the kernel discarded
  before `recv_from` could return it was invisible to `logit` entirely, which left the one loss
  path nothing could attribute to a component. `logit_pipeline::sockstat` now reads the kernel's own
  per-socket counters straight off the listener's fd, once a second and once more after the read
  loop stops, and reports `logit.input.kernel.drops` (a count) alongside
  `logit.input.receive_buffer.used.bytes` / `.utilization` (gauges) — the fill level that says
  whether more drops are coming.

  Two things about how it landed are worth keeping. **`getsockopt(SO_MEMINFO)`, not procfs**,
  which is what this entry originally proposed: the drop counter it returns is byte-for-byte
  `/proc/net/udp[6]`'s `drops` column, but reading it needs no parse of a netns-wide table and no
  matching of *our* socket in it by address or inode — a match `SO_REUSEADDR` and multicast binds
  make genuinely ambiguous — and it returns the receive buffer's fill in the same call. **And the
  same helper covers TCP listeners**: `getsockopt(TCP_INFO)` on a socket in `LISTEN` aliases
  `tcpi_unacked`/`tcpi_sacked` onto the accept queue's depth and its backlog ceiling, reported as
  `logit.input.accept_queue.depth` / `.limit` / `.utilization` by every stream input (`syslog_in`,
  `graphite_in`, TCP `statsd_in`, `logit_in`, `otlp_in`, `prometheus_in`'s remote-write receiver).
  The note this entry ended on still holds: almost nothing in the field does either in-process —
  syslog-ng, rsyslog, Telegraf and gostatsd all tell operators to run `netstat -su`/`ss -u`
  themselves — so this is ahead of the field rather than at parity with it.
- **A UDP sink's send failures are not counted by cause.** The receive side's kernel counters
  (directly above) have no useful send-side twin: `SO_MEMINFO`'s `wmem_alloc` is ~always 0 when
  sampled on a UDP socket, because a datagram is charged and uncharged inside one `sendmsg`, so a
  send-buffer gauge would be a flat zero dressed up as a signal — it was deliberately not built,
  and `SockMeminfo` carries the field only because the option returns it anyway. What *would* be
  worth having is the thing the call site can see and nothing else can: the errno. `statsd_out`,
  `syslog_out`, `graphite_out` and `collectd_out` all send datagrams and all treat a failed send
  the same way regardless of why it failed, so an operator cannot today tell `ENOBUFS` (local
  socket-buffer pressure, a tuning problem) from `EMSGSIZE` (a datagram past the path MTU, a
  configuration problem) from `ECONNREFUSED` (an ICMP port-unreachable from a receiver that isn't
  there, a deployment problem) — three different faults with three different fixes, currently one
  undifferentiated failure. A `logit.output.send.errors{errno="..."}` count at those four send
  sites would separate them, with the errno set bounded by the handful a UDP `sendmsg` can
  actually return.
- **Netns-wide UDP counters (`/proc/net/snmp`, `netstat -su`) are deliberately not collected.**
  `Udp: InErrors` / `RcvbufErrors` / `NoPorts` and the `UdpLite` block alongside them answer real
  questions the per-socket counters cannot — most usefully `NoPorts`, datagrams that arrived for a
  port nothing was listening on, which is what a misconfigured sender looks like from the
  receiver's side. They are not collected because they are not attributable: they are totals for
  the whole network namespace, covering every process and every socket in it, and `logit`'s
  telemetry model is per component (`docs/design/internal-telemetry.md` — every point carries the
  `component`/`kind`/`role` identity of the thing that recorded it). Publishing a namespace-wide
  number under one listener's identity would be actively misleading in exactly the deployments
  where it matters, a host agent sharing a netns with everything else on the box. If these are
  ever wanted, they belong to a process-level scope — alongside `logit.process.*`, which `internal`
  already samples for itself — and not to any listener.
- ~~**A UDP listener reads one datagram per syscall**~~ — **closed** on Linux
  ([ADR `udp-intake-batching-and-socket-visibility`](adr/udp-intake-batching-and-socket-visibility.md)).
  `read_loop` (`logit-inputs::udp`) now takes up to `receive.read_batch` datagrams per `recvmmsg(2)`
  call (default 64, ceiling `UIO_MAXIOV`), through `tokio::net::UdpSocket::async_io` — which is the
  raw-fd seam this entry said the work would need, and the same one
  `crates/logit-inputs/src/tail/watch.rs`'s `inotify` backend already uses. The `mmsghdr`/`iovec`
  arrays are rebuilt inside the readiness closure on every call over `Vec<u64>` backing storage, so
  no raw pointer is ever held across an `.await` and the read future stays ordinarily `Send` with no
  `unsafe impl` behind it.

  `read_batch` also sizes the decode half's `pop_many`, so one knob governs both ends of the receive
  queue, and `logit.input.reads` alongside `logit.input.datagrams` makes the mean fill of a syscall
  batch — the number that says whether the knob is doing anything — directly observable.

  **Linux only, and that is the whole of it.** `recvmmsg` is a Linux syscall with no portable
  equivalent worth a second implementation; every other target keeps the one-`recv_from`-per-datagram
  loop behind the same interface, and `read_batch` is documented as parsed-and-ignored there.
- **One reader per UDP listener.** A single read loop is one core's worth of read capacity.
  `SO_REUSEPORT` lets multiple sockets share one port with the kernel load-balancing datagrams
  across them — gostatsd's `--max-readers` (default `min(8, NumCPU)`), rsyslog's per-listener
  thread count (capped at 32). Still not built, and the reasoning has moved on rather than
  disappeared. **The prerequisite is done**: the entry above used to say a batched read should raise
  the single-reader ceiling before more readers are worth adding, and it has —
  `recvmmsg(2)` with `read_batch: 64` cut the CPU cost per event on the single-datagram-per-packet
  workload substantially, so the question "is one reader still the bottleneck?" now has measurements
  behind it rather than an assumption
  ([ADR `udp-intake-batching-and-socket-visibility`](adr/udp-intake-batching-and-socket-visibility.md)'s
  sweep). What has not changed is the cost of building it: N readers each holding their own `Fanout`
  clone would need its own answer to the cancel-by-drop shutdown cascade
  ([ADR `service-lifecycle-and-output-retry`](adr/service-lifecycle-and-output-retry.md)) that today
  assumes exactly one `Fanout` per listener. A related, smaller question that same work would have
  to settle is its own entry, directly below: the read and decode halves currently share one task.
- **A UDP listener's read and decode loops share one task.** `read_loop` and `decode_loop`
  (`crates/logit-inputs/src/udp.rs`) are driven by `run_until_shutdown`'s one two-arm `select!`, so
  the two interleave — cooperatively yielding to each other on the coop budget — but never run on two
  cores at once. W4's coop-budget analysis found nothing currently costing anything measurable from
  that sharing (`docs/design/performance.md` §7), but a report-only experiment run alongside it, not
  shipped, found real headroom if it were split: spawning `decode_loop` onto its own task, pinned to
  cores 2, 3, 14, 15 (two fast physical cores plus their SMT siblings), against the same branch and
  pins otherwise — **laptop-provisional, and not migrated to the reference VM**: the branch
  (`udp/w4-scratch-spawn-experiment`) was local-only and report-only from the start, and the
  2026-09-20 VM measurement session that replaced every other number in this document with current
  VM figures didn't rebuild or re-run it (out of scope for that session; would need `script/vm
  build` from a local directory source). Read the numbers below as this-laptop-that-day, same as
  every pre-2026-09-20 figure this document used to carry uncaveated:

  | | single task | decode spawned |
  |---|---|---|
  | `udp-statsd-small` CPU µs/event | 1.229 | 1.082 (−12%) |
  | `udp-statsd-small` peak RSS | 22.0 MiB | 38.0 MiB |
  | `udp-statsd` CPU µs/event | 0.659 | 0.696 (+5.6%) |
  | `udp-statsd` kernel drop % | 0.69 | 0.00 |
  | `udp-statsd` mean fill | 22.8 | 2.6 |
  | `udp-statsd` peak RSS | 86.4 MiB | 264.8 MiB |

  Splitting the two took `udp-statsd`'s kernel drops to zero and its mean fill from ~23 to ~2.6 (the
  reader stops waiting behind decode at all and keeps the socket continuously drained), and cut
  `udp-statsd-small`'s CPU/event ~12% — at a cost of +5.6% CPU/event on `udp-statsd` itself (the
  cross-core handoff) and roughly **3× peak RSS**, because nothing paces the reader against the
  decoder any more once they're not sharing a poll budget.

  Shipping it, not attempted here, needs four things designed together — three named in the ADR's
  "Consequences" section, the fourth from PR #254's own report of the experiment: a
  join-handle-plus-cancellation story to replace `run_until_shutdown`'s two-arm `select!`, which is
  load-bearing for shutdown/drain ordering today (read finishing closes the queue, which is what lets
  decode discover closed-and-empty and flush its accumulator); moving the decoder out of `&mut self`
  so it can live on a `'static` task (`D: 'static`); a `Fanout` ownership answer, since dropping the
  decode future — not something a caller does directly once it's on a task — is what closes every
  downstream inbox today; and `receive.max_bytes`'s default revisited against real measurements,
  since nothing bounds the reader once it's decoupled from decode's pace. **This overlaps heavily
  with the `SO_REUSEPORT` entry above**, which needs answers to
  the same shutdown-cascade and `Fanout`-ownership questions for its own, larger reason (N readers
  each with their own `Fanout` clone) — the two should be designed together rather than separately.
- ~~**A `ReceiveQueue`'s depth/bytes/utilization gauges update on every datagram, on both sides of
  the queue**~~ — **closed, both halves.** `BoundedQueue::push`/`pop` (`crates/logit-pipeline/src/queue.rs`)
  call `update_gauges` — three `Telemetry::gauge` calls, each locking `ComponentBuffer`'s
  `Mutex<HashMap>` (`crates/logit-core/src/telemetry.rs`) — unconditionally on every accepted item.
  On a `SinkQueue` that's once per *batch*, an already-accepted cost; on a `ReceiveQueue` it was
  once per *datagram* on both sides at once, with the same listener's `read_loop` (pushing) and
  `decode_loop` (popping) contending on the identical lock.

  **The decode side is now batched.** `BoundedQueue` grew `push_many`/`pop_many`
  ([ADR `udp-intake-batching-and-socket-visibility`](adr/udp-intake-batching-and-socket-visibility.md)) —
  in `BoundedQueue` itself, as this entry required, with `push`/`pop` and every sink-side caller
  untouched — and `decode_loop` pops up to 64 datagrams per call, so the pop side now updates the
  three gauges once per popped batch instead of once per datagram. Per-item admission, drop counting
  and `Block` waiting are unchanged; only the bookkeeping around them batches.

  **And the push side closed with the `recvmmsg` read.** The condition this entry set was that
  batching the push without batching the read would just be the same gauge update with a one-item
  `Vec` around it. The read path is now `recvmmsg` with `vlen = read_batch` (the entry above), so
  the datagrams arrive already batched and `read_loop` hands the whole batch to `push_many` in one
  call. Both of the two loops that used to contend on the component's telemetry mutex once per
  datagram now touch it once per batch, which is what this entry asked for.
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

  That clone is still what an *ordinary* fan-out with no `Output` branch costs — a router plus
  `target` components (ADR [`target-components`](adr/target-components.md)) expresses a split
  without one: `1 + used destinations` allocations per batch, against 324 for the fan-out-plus-
  filters shape `memory.md` §3 measures for the identical split. So the residual gap above is now
  specifically "an unconditional fan-out to several mutating branches," not "splitting a flow" —
  anything that *is* a destination split, named by an attribute/provenance/resource value or a Lua
  script's own decision, has a cheap, non-cloning answer now. The backpressure caveat two
  paragraphs up is unchanged either way: a stalled consumer of one target still backs up through
  its router into every other target's flow, the same as any other shared upstream.
- ~~**A Lua component's `flush()` has no resource or scope of its own at a timer tick**, and sees
  a stale trace context and stale provenance, all for the same reason: its globals kept whatever
  the most recently processed batch set.~~ **Closed** ([ADR
  `lua-flush-root-context`](adr/lua-flush-root-context.md)): a Lua `flush()` runs in a root
  context. Before every call, `trace` is the fresh root the emission is sent under, `provenance`
  is this component as both `origin` and `previous` (what the batch is stamped with), `resource`
  is empty and `scope` is none; a `resource`/`scope` write inside `flush()` is the one way a
  flush-driven emission carries either. What remains is not a gap but the ADR's stated boundary:
  `logit` never attributes a flush to any of the batches that fed it (no accumulator to inspect,
  unlike `Transform::flush`'s linking below), so a script that wants that relationship tracks
  contributing contexts itself inside `process()`.
- ~~**Lua has no span API at all**~~ — ~~**narrowed to span writes/minting from Lua.**~~ —
  **narrowed again (2026-09-15) to in-place span mutation from Lua.** `event.span`
  (`docs/design/lua-api.md`'s "Reading `event.span`") is a real, read-only proxy — a script can
  read every field a `SpanRecord` carries, including its `events`/`links` tables, once one exists
  — and `Event.new` ([ADR `lua-event-constructor`](adr/lua-event-constructor.md), the `span` table
  in `lua-api.md`'s "Constructing events") now builds a whole span, `events` and `links` included,
  so `trace_context`'s `span:` block ([ADR
  `trace-context-span-lifting`](adr/trace-context-span-lifting.md)) is no longer the only way to
  turn a log line into a span. What's left: a script cannot *mutate* an existing `event.span`
  field by field; the documented way is `Event.new(event:to_table())` with the table edited. An
  in-place write path shares this constructor's parsers and is a small follow-up, not designed
  yet — the same posture in-place `event.log.message`/`severity`/`body_format` writes take.
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

  **`flatten` (`crates/logit-transforms/src/flatten.rs`,
  [ADR `flatten-transform`](adr/flatten-transform.md)) sits downstream of all of the above and adds
  no new bound by design.** Its marginal exposure over what `json`/`syslog_in`/`otlp_in` already
  accept is two things: path *combinations* of already-interned keys (a product, not a sum — bounded
  by the fixed internal recursion-depth wall, not a key-count cap), and array indices, which are a
  genuinely new key axis no existing component mints — a 10,000-element array attribute flattens
  into `tags.0`..`tags.9999`, ten thousand new symbols, interned forever. There is deliberately no
  `max_keys`-style cap (a settled decision, not an oversight — see the ADR's Alternatives); the
  operator's levers are a narrowed `attributes:`/`resource:` list and `arrays: skip`. A rotating key
  space in *value* position that `flatten` promotes to *key* position (a map keyed by request or
  user IDs, say) is the same exposure `json`/`otlp_in` already have one level shallower, now
  multiplied by every distinct path above it.
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
- ~~**`syslog_in` is UDP-only**~~ — **closed as of 2026-09-13.** `syslog_in` gains
  `transport: tcp`, running on the same generic stream driver (`logit-inputs::tcp::TcpListener`)
  `syslog_out`'s own TCP transport already used from the egress side — RFC 6587 framing
  (octet-counting or non-transparent, auto-detected per connection), and `tls:` on top of it for
  RFC 5425 syslog over TLS. The asymmetry this entry and
  [ADR `syslog-output`](adr/syslog-output.md)'s "that asymmetry is deliberate" note both called
  out no longer holds; see
  [ADR `syslog-tcp-ingress-and-tls`](adr/syslog-tcp-ingress-and-tls.md). **Closed: `syslog_in` no
  longer skips RFC 5424 STRUCTURED-DATA** — `parse_structured_data`
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
- ~~**`syslog_out` has no TLS**~~ — **closed as of 2026-09-13** for RFC 5425 (syslog over TLS over
  TCP): `syslog_out` gains `tls:` (`TlsClientConfig`), and `syslog_in` gains the matching
  `transport: tcp`/`tls:` (`TlsServerConfig`) on the ingress side, both against the same
  `logit_out`/`otlp_in`-shaped config-plumbing this entry already named as the fix; see
  [ADR `syslog-tcp-ingress-and-tls`](adr/syslog-tcp-ingress-and-tls.md). **Still open: RFC 6012
  (DTLS, syslog over TLS over UDP)** — out of scope for that ADR (see its Alternatives); a `tls:`
  block under `transport: udp` is a config error on both `syslog_in` and `syslog_out` rather than
  silently ignored, so this remains a real gap, not a documentation one.
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
  state set once per transport). The collectd codec (`crates/logit-proto/src/collectd/encode.rs`)
  adopted it too, in `collectd_out`'s own PR (W3): its `Packets` buffer is gone, replaced by
  `MessageBuf<usize>` -- the per-datagram `usize` meta is the value-list count `EMSGSIZE`
  accounting needs, which is exactly what `Packets` existed to carry. `prometheus_out` stays
  outside all three traits by design.
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
- **Closed: a repeated DogStatsD tag key decodes to a `Value::Array`, not a collapse to its last
  value.** `#team:a,team:b` is legal DogStatsD — a list, not a map — and `insert_tags`
  (`crates/logit-inputs/src/statsd.rs`) now folds a repeated key into a `Value::Array` in wire
  order instead of letting a plain `AttrMap::insert` per token let the last token win; an *exact*
  duplicate token still dedupes at decode (`#team:a,team:a` -> `Str("a")`), matching the Datadog
  agent's own behaviour, and a one-element `Array` is never produced, so a non-repeated tag's
  decoded shape is unchanged. `statsd_out`'s `build_tag_suffix` expands an `Array`-valued attribute
  back into one tag per element in array order, with no dedupe on encode, so
  `x:1|c|#team:a,team:b` now relays byte-for-byte instead of collapsing to `x:1|c|#team:b`.
  Sinks whose wire can't carry a multi-value tag fall back to last-value-wins, counted rather than
  silent: `influxdb_out` and `prometheus_out` each render an `Array`'s last representable element
  and count `logit.output.{tags,labels}.normalized{reason="multi_value"}` once per attribute. See
  [ADR `statsd-output`](adr/statsd-output.md)'s amendment and
  [`docs/plans/lossless-transit.md`](plans/lossless-transit.md) for the closing assessment.
- **Closed: a non-UTF-8 syslog MSG decodes to a `Value::Bytes` event instead of being rejected** —
  RFC 5424's `MSG-ANY` permits arbitrary octets, and `logit-core::Value`'s `Bytes` variant now
  carries it. `parse_line`/`parse_5424`/`parse_3164` (`crates/logit-inputs/src/syslog.rs`) parse
  header fields directly off the line's raw bytes and validate each individually as PRINTUSASCII;
  only the MSG slice is UTF-8-validated (`message_value`), so a line whose header parses cleanly
  while MSG isn't valid UTF-8 now decodes with a `Value::Bytes` message instead of failing outright.
  `syslog_out` writes a `Value::Bytes` message raw, sanitized at the byte level
  (`sanitize_msg_bytes`), never lossy-decoded. See
  [ADR `syslog-structured-data-convention`](adr/syslog-structured-data-convention.md).

  **Narrowed: UTF-8 rejection was never the only thing standing between a syslog line and an
  arbitrary-binary payload, and closing it above didn't fully close this one either — though the
  framing half has since caught up on one transport.** `SyslogDecoder::decode_into`
  (`crates/logit-inputs/src/syslog.rs`) still splits on `\n` *before* any UTF-8 check runs on
  `syslog_in`'s UDP transport, so a binary payload containing a `0x0A` byte is still cut mid-value
  by the framing there — see the HAProxy CBOR entry below. Over `transport: tcp`, though, this is
  no longer true: `SyslogInput::tcp` turns line splitting off
  (`SyslogDecoder::with_line_splitting(false)`) and hands framing to
  `logit-inputs::tcp::TcpListener`'s octet-counting `Framer`
  ([ADR `syslog-tcp-ingress-and-tls`](adr/syslog-tcp-ingress-and-tls.md)), which delimits by
  declared length, not `\n` — a `0x0A` inside an octet-counted MSG now survives intact end to end.
  `Value::Bytes` MSG (closed above) plus this framing clear the "reachable" bar on TCP; the HAProxy
  CBOR entry below records why the decoder that would consume it was measured and not built.
- **HAProxy's native CBOR log output (`%{+cbor}o` / `%{+cbor,+bin}o`) was investigated twice and
  deliberately not pursued — a closed door now, not a "not now", recorded so it isn't reopened
  without new evidence.** The first pass (2026-09-13) deferred it on framing grounds; the second
  (2026-09-17) captured real HAProxy 3.0.27 output, built a throwaway decoder, and measured. What
  that found, in decreasing order of surprise:
  - **Binary CBOR *is* reachable, on both transports, once the flags are spelled right.** HAProxy's
    log-format options are comma-separated: `%{+cbor,+bin}o`. `%{+cbor+bin}o` applies only the
    last flag (`bin` alone, so plain unencoded output) and `%{+bin+cbor}o` only `cbor` (the hex
    form) — `parse_logformat_node_args` in HAProxy's `src/log.c` resets its start pointer at every
    `+`, and the manual never says so. With the comma form, HAProxy 3.0.27 emitted raw binary CBOR
    in the syslog MSG both to a plain UDP `log` target and to a `ring` with
    `server ... log-proto octet-count`. Over TCP octet-counting that MSG already lands in `logit`
    intact as a `Value::Bytes` (the narrowed entry above); over UDP it would need a `syslog_in`
    opt-out of newline splitting, a one-field change that was never the hard part.
  - **Wire shape**, should anything ever decode it: an indefinite-length map (`BF … FF`) with
    definite text keys; *untyped* string items such as `%HM` come out as indefinite-length
    *chunked* text strings (`7F 63 'GET' FF`), `:str`-typed ones as definite strings; `:sint`
    non-negatives are major type 0; `:bool` is simple true/false; no tags.
  - **Size: 13–19% smaller than JSON, not more.** The demo's 20-item HAProxy access line, same
    items from one HAProxy run: 588 bytes as `%{+json}o` (HAProxy pads after `:` and `,`), 549
    bytes as the compact hand-written JSON `demo/haproxy/haproxy.cfg` actually emits, 476 bytes as
    `%{+cbor,+bin}o`. The hex form is 2× the binary, i.e. larger than JSON.
  - **Parse speed: no faster, measured.** A hand-rolled CBOR-to-attributes prototype (a twin of
    `json`: zero-copy `Bytes` slices for definite strings, the same `KeyCache`, indefinite
    maps/strings, an explicit depth bound) benched against `JsonParser::process` on those two
    payloads, pinned to one core with divan's allocation profiler on, three runs: JSON 881–921
    ns/event, CBOR 1030–1049 ns/event — 1.1–1.2× *slower*. Allocations were equal (one: the
    `AttrMap` spilling past its 8-entry inline capacity at 20 attributes) once the chunked `%HM`
    string was typed `:str`; as HAProxy emits it, CBOR costs two more for the chunk concatenation.
    The per-entry budget is dominated by what both formats share — key-cache lookup, `Value`
    construction, sorted `insert_sym`, UTF-8 validation, refcount bumps — and the syntax scanning
    CBOR saves is a small slice of it that `serde_json`'s tuned scanner already spends well. A
    tuned decoder could plausibly close the gap; nothing in the profile suggested a meaningful
    lead. The prototype was not kept in tree.
  - **The item-name grammar limitation is shared with `%{+json}o`**, and is why the demo
    hand-writes its JSON: HAProxy rejects a literal `.` in a custom item name (confirmed against
    `haproxy -c`, `demo/haproxy/haproxy.cfg:99-117`), so a CBOR-sourced tier would still need a
    rename stage for its `span.*`/`trace.*` keys (`SpanLiftConfig` in `crates/logit-config` has no
    source-field override, so `trace_context` can't absorb it), and CBOR has no hand-written escape
    hatch because binary can't be typed into a `log-format` string. That is HAProxy's grammar, not
    CBOR's; CBOR text keys take dots fine.

  Net: a second decoder to build and maintain, a `syslog_in` framing knob, and a rename stage, for
  a ~15% wire saving and no parse-time win. Not built. A `cbor_in`/`cbor_out` listener/sink pair
  was weighed in the same pass and rejected outright: nothing in the telemetry landscape speaks
  CBOR over a socket (Fluent forward is msgpack, Vector's native wire is protobuf, syslog is text),
  so it would be a second native wire beside `logit_in`/`logit_out` with no producer or consumer.
  If new evidence ever reopens this, the decoder constraints from the first pass still hold and
  the prototype confirmed each one costs real code: `Value::as_str` **panics** on an invalid-UTF-8
  `Value::Str` (`crates/logit-core/src/value.rs`), so CBOR's only-nominally-UTF-8 text strings need
  validation before becoming one; a hand-rolled reader needs an explicit recursion bound, which
  `json`'s `serde_json`-based one inherits for free; a length header must never size an allocation
  directly; and tag 1 (epoch time), decodable straight into `Value::Timestamp`, is the one thing
  the format offers that JSON doesn't — HAProxy doesn't emit it.
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

  **Narrowed on 2026-09-16:** the *cardinality* half of the same `$host` exposure -- distinct from
  the truncation risk above -- is now closed by example. `nginx.conf` serves exactly two vhosts,
  but `$host` itself stays attacker-controlled, so a junk `Host` header used to become its own
  unbounded series in `aggregate` and in InfluxDB. `examples/nginx-to-influxdb.yaml`'s `bounded`
  component (`keep_values`, `docs/adr/value-allowlist-cardinality-clamp.md`) now clamps `host` to
  the two real vhosts ahead of `aggregate`, folding anything else into one `other`-tagged series.
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
    accumulator `logit` can inspect — and runs in a link-less root context instead
    ([ADR `lua-flush-root-context`](adr/lua-flush-root-context.md)), with `trace.trace_id`/
    `trace.span_id` (`docs/design/lua-api.md`) exposed to a script's own `process()` for its own
    bookkeeping. Picking an arbitrary
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
       links — there is still no accumulator on the Lua side to inspect. The script-visible side
       of that root is settled ([ADR `lua-flush-root-context`](adr/lua-flush-root-context.md)).
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
  "no wire form exists" rather than "the nearest shape loses something." It has grown once more: the
  `encode (Graphite)` rows are the Graphite/Carbon codec's half
  (`crates/logit-proto/src/graphite/`, [ADR `graphite-carbon-relay`](adr/graphite-carbon-relay.md)),
  whose module doc is likewise the authority — the narrowest wire model of the lot, since a carbon
  datapoint is one untyped number at one whole second and nothing else. Tracked as debt against [ADR `lossless-transit`](adr/lossless-transit.md);
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
  | encode/decode (Prometheus) | Native histograms are **skipped in both directions** — `MetricKind::ExponentialHistogram` on the way out of either `prometheus_out` mode, a `TimeSeries.histograms[]` entry on the way into `prometheus_in(bind)` | `logit.output.metrics.skipped{metric_kind="exponential_histogram"}` on send; `logit.input.metrics.skipped{reason="native_histogram"}` on receive (also reported per request as `Decoded::histograms_skipped`, which is why a 2.0 response's `X-Prometheus-Remote-Write-Histograms-Written` is always `0`). **A 1.0 sender gets no signal at all** — 1.0 defines none of the `-Written` headers, so a Prometheus configured with `protobuf_message: prometheus.WriteRequest` and native histograms enabled sees `204`s for requests whose histograms were dropped, and only this receiver's own counter says otherwise. A 2.0 sender at least reads the zero (and Prometheus's own queue manager treats a zero against a non-zero send as a failure, loudly) | Deferred to a follow-up plan, not rejected — [ADR `prometheus-remote-write`](adr/prometheus-remote-write.md)'s "Native histograms now" alternative has the scope: a `Point::NativeHistogram` carrying the sparse shape, a mapping between Prometheus's `schema` field and OTLP's `scale` (they agree on base-2 exponential bucketing but not on sign or on the zero-bucket treatment), the positive/negative span-and-delta encoding, and a decision about the gauge-vs-counter `reset_hint`. Remote-write 2.0 *does* carry them, so this is no longer "the text format has no syntax for it" (text 0.0.4 and OpenMetrics 1.0 genuinely don't — sparse buckets live only in Prometheus's protobuf exposition and in remote-write, `docs/design/telemetry-landscape.md`): the wire is there now and the mapping is what isn't. Materializing explicit buckets from one would still be the lossy conversion `MetricKind::ExponentialHistogram` exists to avoid. |
  | encode (Prometheus remote-write) | A record flagged `FLAG_NO_RECORDED_VALUE` whose kind expands to several derived series — `Histogram`, `Summary`, `Distribution`/`Samples` (a sketch) — → **skipped**, where a flagged `Gauge`/`Sum`/marker-untyped record is written as Prometheus's own stale marker (the NaN bit pattern `0x7ff0000000000002`) | `logit.output.metrics.skipped{reason="no_recorded_value"}` | One flag says a series stopped reporting; it says nothing about *which* of `_bucket{le}`/`_sum`/`_count` existed while it did, and a stale marker has to name a series by its full label set to mean anything. There is nothing to reconstruct the stale set from, and writing a marker on the bare family name would mark a series that never existed (`crates/logit-proto/src/prometheus/mod.rs`'s `stale_point`). Distinct from the exposition path, where `with_stale_markers` is off and *every* flagged record is skipped under the same counter. |
  | encode (Prometheus remote-write) | Two readings of one series whose nanosecond timestamps truncate to the same millisecond → the **later** reading wins, the earlier is dropped | `logit.output.metrics.degraded{reason="sub_ms_collapsed"}`, once per dropped reading | The wire carries milliseconds and the model carries nanoseconds, and one label set may not carry two samples at one timestamp: Prometheus and Mimir answer `400 duplicate sample for timestamp`, which this sink classifies `Fault::Permanent`, so emitting both would cost the whole request rather than the one reading. Real data loss rather than a rendering difference — the one entry on `remote_write.rs`'s permitted-normalization list that loses a *reading* — and the only fix is a sub-millisecond wire, which remote-write does not have. Reachable in practice only from a source with sub-millisecond resolution writing one series more than once per millisecond. |
  | encode (Prometheus) | `MetricKind::Distribution`/`Samples` → a `summary` of 5 fixed quantiles (p50/p75/p90/p95/p99) with a `_count` and **no `_sum`** | `logit.output.metrics.degraded{metric_kind="distribution"\|"samples"}` | Same shape as the OTLP `Distribution` row above, and the same shared `DISTRIBUTION_QUANTILES` constant, so one metric describes itself identically at `otlp_out` and `prometheus_out`. The missing `_sum` is honest rather than lossy: a `DDSketch` has no sum to report, and OpenMetrics permits omitting it. |
  | encode (Prometheus) | `MetricKind::Set`/`SetMembers` → a `gauge` of `estimate()` / of the distinct member count | `logit.output.metrics.degraded{metric_kind="set"\|"set_members"}` | Prometheus has no cardinality-estimate type, but unlike OTLP (which skips) it does have a plain gauge, and a cardinality *number* is a perfectly good gauge reading — the estimate is the whole point of a `Set`. What's lost is mergeability: two relays' gauges can't be combined the way their `HyperLogLog`s could. |
  | encode (Prometheus) | `Sum{Cumulative, !monotonic}` → `gauge` | `logit.output.metrics.degraded{metric_kind="non_monotonic_sum"}` | Prometheus has no non-monotonic counter: a `counter` is monotonic by definition, and exposing a decreasing one would make every `rate()` over it wrong. A gauge carries the value correctly and loses only the "this is a sum" fact, which no exposition type can express. |
  | encode (Prometheus) | A `Histogram`'s `min`/`max` → dropped | none (documented) | Neither exposition format has a per-histogram minimum or maximum field; only buckets, `_sum` and `_count`. OTLP does, so this is lost on `otlp_in -> prometheus_out` but not on `otlp_in -> otlp_out`. Tracked as a follow-up (a `_min`/`_max` convention would be an invention, not a format feature). |
  | encode (Prometheus) | Label values: `Value::Str/I64/U64/F64/Bool` stringified; a multi-valued `Array` (a repeated DogStatsD tag key) renders its last representable element; `Null/Bytes/Timestamp/Map`, and an `Array` with no representable element, dropped | `logit.output.labels.normalized{reason="multi_value"}` (lossy: the non-last elements are discarded) / `logit.output.labels.dropped{reason="unrepresentable"}` | Prometheus labels are always strings, so every kind that has a faithful string form gets one and the type is lost (a `Value::I64(3)` and a `Value::Str("3")` become the same label). A label set is a map, so a multi-value `Array` has no faithful form either; last-value-wins, counted, mirrors `influxdb_out` (ADR `statsd-output`'s amendment). The dropped kinds have no honest string form: a `Bytes` need not be UTF-8, and flattening a `Map` into one label value would invent a syntax nothing parses back. |
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
  | encode (collectd) | A `log`-only event's `collectd.severity` outside `{1, 2, 4}` (present but the wrong `Value` type, or `Value::U64` out of that set) → the notification is **dropped** whole | `logit.output.metrics.skipped{reason="notification_dropped"}`, throttled diagnostic key `notification_dropped` | collectd's own wire only ever carries `1` (FAILURE), `2` (WARNING) or `4` (OKAY) in a Severity part; there is no "unknown severity" value to fall back to, and inventing one (clamping to the nearest, or defaulting to WARNING) would put a severity on the wire nothing upstream actually reported. Distinct from an event with **no** `collectd.severity` attribute at all, which is not a notification attempt in the first place and is counted `skipped_no_metrics` instead — this row is specifically the case where the attribute is present but unusable. |
  | encode (Graphite) | `MetricKind::Samples`/`Distribution`/`Histogram`/`ExponentialHistogram`/`Summary`/`Set`/`SetMembers` → **skipped** under `multi_value: skip` (the default), or **expanded** into dotted sub-paths (`.count`, `.sum`, `.q0_5`…`.q0_99`, `.bucket_<b>`, `.zero_count`) under `multi_value: expand` | `logit.output.metrics.skipped{metric_kind="samples"\|"distribution"\|"histogram"\|"exponential_histogram"\|"summary"\|"set"\|"set_members"}` / `logit.output.metrics.degraded{metric_kind=…}` once per record | A carbon datapoint is **one number at one second** — there is no bucket, quantile, sketch or member-set wire form to degrade into, and unlike `prometheus_out` there is not even a typed gauge to render a cardinality estimate onto honestly. Skipping is the default because the alternative is a *naming convention* nothing at the far end knows about: `x.q0_99` is a series called `x.q0_99`, not a quantile of `x`, and Graphite cannot tell the two apart. `expand` is therefore opt-in and named (ADR `lossless-transit`'s "summarization is opt-in and named" rule applied to a *rendering* rather than a summarization), and what it loses is mergeability: two relays' `.count` series cannot be recombined the way their `DdSketch`es could. An `ExponentialHistogram`'s buckets are deliberately **not** expanded even under `expand` — materializing `base^i` bounds would be exactly the lossy conversion that kind exists to avoid, and would mint an unbounded number of wire paths from one record. |
  | encode (Graphite) | A `Sum`'s `temporality` and `monotonic` → **dropped**; the value goes on the wire bare | none (a named normalization, not a skip) | Carbon's wire has no opinion about either — every datapoint is just a number at a second — so unlike `prometheus_out`, which *skips* a delta `Sum` because exposition has a competing cumulative meaning that would make every `rate()` wrong, there is nothing here for the value to be misread as. The number is carried faithfully and only the model's extra facts are lost, which is why this is normalization 12 in the codec's own list rather than a drop. It does mean `otlp_in -> graphite_out -> graphite_in` turns a cumulative counter into a gauge. |
  | encode (Graphite) | A non-finite value (NaN, ±inf) → **dropped** | `logit.output.metrics.skipped{reason="unencodable_value"}`, throttled diagnostic key `unencodable_value` | Carbon's own receiver drops a NaN on receipt, and there is no wire spelling for an infinity at all — `inf` in the value field is a string carbon's `float()` would accept but whisper cannot store. Substituting zero would fabricate a reading. The decode side rejects the same values symmetrically (`logit.input.metrics.skipped{reason="non_finite_value"}`), so a relay never emits one either. |
  | encode (Graphite) | A `MetricRecord` flagged `NO_RECORDED_VALUE` → **skipped** | `logit.output.metrics.skipped{reason="no_recorded_value"}` | The rule every sink with no no-value wire form follows (the `sinks with no no-value wire form / `aggregate`` row above). Carbon has no marker for "no reading this interval" — unlike collectd, whose GAUGE `NaN` means exactly that — so writing the flag's default numeric payload would report a sample the producer never sent, and Graphite's own "no data" is the absence of a datapoint. |
  | encode (Graphite) | `MetricRecord`'s `unit`, `description`, `start_timestamp` and `exemplars`; `EventBatch::scope`; `Resource::schema_url`; every `dropped_attributes_count` | none (documented) | The protocol has no field for any of them: a datapoint is a path, an optional tag set, a number and a second, full stop. `otlp_in -> graphite_out` therefore loses instrumentation-scope identity and unit metadata; `otlp_in -> otlp_out` does not. Unlike the Prometheus rows, there is not even a comment syntax to hang them on — carbon's plaintext line has no metadata channel, and the pickle batch protocol is a list of three-tuples. |
  | encode (Graphite) | **Resource** attributes are rendered as carbon tags, indistinguishable from event ones | none (documented) | The same rule `influxdb_out` and `statsd_out` follow: a sink's wire has one tag set, and dropping the resource half would lose `service.name`/`host.name` entirely. Within the pair this is invisible — a bare `graphite_in` resource is empty, so `graphite_in -> graphite_out` stays a fixed point — but cross-protocol it is real: `otlp_in -> graphite_out -> graphite_in` returns every resource attribute as an *event* attribute, and the resource/event distinction is gone. Carbon has no second tag scope to put them in. |
  | encode (Graphite) | A path component longer than **255 bytes** → written unchanged, and rejected by whisper | none (documented) | Carbon itself has no path length bound, and the codec deliberately does **not** truncate: a truncated path is a *different, silently wrong* series, where an over-long one fails visibly at the storage layer. The 255 bytes is a filesystem limit (whisper stores `a.b.c` as `a/b/c.wsp`), so it binds only whisper-backed Graphites and not, say, `go-carbon` with a ClickHouse backend — which is exactly why enforcing it in the codec would be wrong. `/` and `\` *are* substituted with `_`, since those would create a nested directory rather than a series segment. A length check belongs in an operator-side `lua` stage if a deployment needs one. |
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

- **`prometheus_out`'s "a sketch has no sum" claim is stale.** The `encode (Prometheus)`
  `MetricKind::Distribution`/`Samples` row above says the rendered OpenMetrics `summary` omits
  `_sum` because "a `DDSketch` has no sum to report" — that was true when the row was written, but
  `logit_core::DdSketch::sum` (`crates/logit-core/src/metric.rs`) is exact now (the inner crate
  accumulates it as a plain `f64` alongside the bins, and adds the two sums on `merge`), a fact
  `crates/logit-proto/src/graphite/mod.rs`'s module doc leans on directly: `graphite_out`'s own
  `multi_value: expand` **does** emit `.sum` for the identical sketch. So `prometheus_out` could
  emit a real `_sum` line for the same summary today with no new computation, only a changed
  `write!`. Filed here rather than fixed as part of the Graphite/Carbon relay effort that noticed
  it (`docs/plans/graphite-carbon-relay.md`'s W4b closeout) — a candidate follow-up for whoever
  next touches `crates/logit-proto/src/prometheus/mod.rs`, not a bug in this effort's own scope.
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

- **Every TLS-capable component's certificates are loaded once at startup; rotation needs a
  restart.** `otlp_in`/`otlp_out`, `logit_in`/`logit_out`, `syslog_in`/`syslog_out`, and
  `prometheus_in` alike (`with_tls`, one per component, all built on
  `crates/logit-inputs/src/tls.rs::build_server_config`/`crates/logit-outputs/src/tls.rs::
  build_client_config`) read every PEM file at construction time (`logit run` startup) and build a
  static `rustls::ClientConfig`/`ServerConfig` (or, for `prometheus_in`'s `reqwest` client, its
  equivalent) from it — a renewed certificate (a 90-day Let's Encrypt cert, a `cert-manager`-issued
  one) has no effect until the process restarts. Originally filed against `otlp_in`/`otlp_out` alone
  ([ADR `otlp-tls-and-pooled-grpc-client`](adr/otlp-tls-and-pooled-grpc-client.md)) as deliberately
  out of scope; every TLS component built since shares the same construction-time-only shape, so the
  gap generalizes rather than needing a fresh entry per component
  ([ADR `syslog-tcp-ingress-and-tls`](adr/syslog-tcp-ingress-and-tls.md)). Closing it means
  `rustls::ServerConfig`'s `ResolvesServerCert` (a file-watcher hook) on the server side, or an
  equivalent reload on the client side, either behind a SIGHUP or a poll.
- **No TLS client on any sink has a `server_name` override.** `otlp_out`, `logit_out`, and
  `syslog_out` alike derive the `ServerName` a peer's certificate is checked against from the
  configured `endpoint`'s own host — useful to override when an endpoint is reached by IP or
  through a proxy whose certificate names something else (OTel's own `tls.server_name_override`
  knob). Cheap to add per sink via `hyper-rustls`'s
  `HttpsConnectorBuilder::with_server_name_resolver` (`otlp_out`), an equivalent override on the
  `reqwest` side (`prometheus_in`, the one TLS *client* on the input side), and a plain
  `ServerName` override ahead of `host_only(endpoint)` (`logit_out`/`syslog_out`, both built on
  `crates/logit-outputs/src/tls.rs`); left out of the initial TLS work on each to keep it small
  ([ADR `otlp-tls-and-pooled-grpc-client`](adr/otlp-tls-and-pooled-grpc-client.md),
  [ADR `syslog-tcp-ingress-and-tls`](adr/syslog-tcp-ingress-and-tls.md)).
- ~~**No idle-connection timeout on a TCP listener after a successful handshake (or, on plaintext,
  after the first byte).**~~ — **closed 2026-09-14.** All five TCP-capable listener kinds
  (`syslog_in`, `graphite_in`, `statsd_in` each `transport: tcp`, `logit_in`, `otlp_in`) now take
  an opt-in `idle_timeout:` bounding exactly this gap — off by default, so nothing about the
  three design questions this row used to pose was skirted: a connection blocked handing a batch
  to a full downstream is never mistaken for a silent peer (the clock only runs while the listener
  is actually waiting on the socket), `logit_in` measures idle from its last `Ack` written rather
  than from bytes read (a peer waiting on a delayed ack is not idle), and `otlp_in` tracks idleness
  at the service level (an in-flight counter, not an IO-level timer) rather than trying to wrap
  hyper's own read loop. See [ADR `idle-connection-timeout`](adr/idle-connection-timeout.md) for
  the full design and [`docs/deploying.md`'s "`idle_timeout` on a TCP
  listener"](deploying.md#idle_timeout-on-a-tcp-listener) for the operator-facing account,
  including the recommendation to enable it wherever consistent traffic is expected.

  **What is still open.** The client-side complement (`logit_out`/`syslog_out`/`statsd_out`/
  `graphite_out` each probe a reused pooled connection before writing to it) closes the common
  case but not the point-in-time race: a peer's FIN arriving *while* the sink is writing is still
  today's `Fault::Ambiguous` on `logit_out`, and a silent, unclassified loss on the three plaintext
  sinks, whose wire protocols give the sender no way to learn the write failed at all. And
  `otlp_in`'s own idle clock resets on request *completion*, not on bytes — see the row below.
- ~~**An `otlp_in` connection that sends its *first* byte and then goes silent holds a
  connection-cap permit indefinitely.**~~ — **closed 2026-09-14** by the same `idle_timeout:` field
  above: once set, a connection that produced one byte and nothing else is closed the same way any
  other idle connection is (`graceful_shutdown()`, a bounded grace reusing `handshake_timeout`,
  then drop). An idle-timed-out connection no longer holds the graph open past shutdown either —
  see the `otlp_in`-can-hold-the-graph-open-past-shutdown row above, which this narrows but does
  not close: a connection still *within* `idle_timeout` at shutdown time is untouched by this.

  **What is still open: the reset-on-request-completion narrowing.** `hyper` owns this listener's
  bytes, so the finest grain `idle_timeout` can see is a request starting and finishing, not
  individual bytes read. A request head that dribbles in more slowly than `idle_timeout` on an
  otherwise-quiet keep-alive connection is therefore still closed — a documented cost, not a bug,
  and distinct from the case this row used to track. A request whose head arrives right at the
  idle deadline is not a further gap: it is served to completion inside the bounded grace
  (`graceful_shutdown` then poll for up to `handshake_timeout`) rather than dropped underneath it
  — the connection is kept open while that request is in flight, and the grace runs again once it
  completes so the response actually reaches the wire, since dropping it mid-flight would discard
  a batch already handed to `Fanout::send`. The cost is at most a reconnect for the *next* request,
  never a lost response or a lost batch, and a silent peer cannot exploit this to hold the
  connection open indefinitely: with nothing in flight the drop still happens at the end of the
  grace, and a stalled body is bounded by the same per-frame stall timeout regardless.
- **A write-only TLS sink (`syslog_out`, and `logit_out` before its per-batch ack) cannot observe a
  peer's post-handshake rejection.** Under TLS 1.3 the server sends its entire handshake flight,
  `Finished` included, before it ever sees the client's certificate message — so a client-cert
  rejection (a `client_ca_file`-requiring collector, no matching cert presented) arrives as an
  alert *after* `TlsConnector::connect` has already returned success on this side. `syslog_out`
  now flushes before a batch may be reported delivered
  ([ADR `syslog-tcp-ingress-and-tls`](adr/syslog-tcp-ingress-and-tls.md)'s 2026-09-13 amendment),
  so the bytes are genuinely off this host by the time `send` reports success — but a local flush
  only proves the write left this process, never that the peer accepted it: the server's rejection
  alert is independent of, and unaffected by, whether this side has flushed. `send` still reports
  the batch delivered, and this sink never reads from the connection again to learn otherwise (PR
  #159's finding). `logit_out` is exposed to the same window only up to its own ack read — once a
  batch's ack has actually been read back, a rejection can no longer hide behind it. A
  *server*-certificate rejection is unaffected: that verification happens inside the client's own
  handshake, before any write is attempted, so it always surfaces as `Fault::Clean` (see
  `crates/logit-cli/tests/syslog_round_trip.rs`'s `mod tls`). Closing the client-cert case would
  mean this sink reading and interpreting TLS alerts (or application-level acks) it currently
  never looks at — out of scope for either ADR that introduced these sinks.

- **`docker_in`'s identity refresh, and its offset retention across a de-selecting rename, are
  both bounded by `poll_interval`, and the retention doesn't survive a `logit` restart.** A
  `config.v2.json` change (a rename, a metadata read recovering from an earlier failure) is
  picked up on the next poll tick, not instantly — a rename and a rename back within one tick is
  never observed at all. A container renamed out of `containers:` retains its offset in memory so
  a rename back resumes rather than replaying, but that retention is process-local: `logit`
  restarting between the two renames falls back to `read_from` for it, the same as any file this
  process has never seen before. See [ADR
  `docker-container-identity-and-minimal-watches`](adr/docker-container-identity-and-minimal-watches.md).
- **`docker_in` only watches `root` and the files it currently has open, so a log file's own
  first appearance inside an already-existing container directory, a rotation, and a
  `config.v2.json` change are all discovered on the next `poll_interval` tick, not instantly.**
  Only a container's own directory arriving or leaving under `root` is `inotify`-fast — Docker's
  per-container state directories are direct children of `root`, so that much needs no per-
  container watch at all. A short `poll_interval` is the only way to tighten the other three; none
  of the three lose data by being poll-bound, only latency. This deliberately reverses the
  previous per-container-directory-watch design, which caught all four near-instantly but cost
  O(containers on the host) work per log line written anywhere on the host. See [ADR
  `docker-container-identity-and-minimal-watches`](adr/docker-container-identity-and-minimal-watches.md).
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
- **The remote-write receiver has TLS but no authentication** ([ADR
  `prometheus-remote-write`](adr/prometheus-remote-write.md)'s "Security posture", and
  `crates/logit-inputs/src/prometheus.rs`'s own module doc) — `prometheus_in`'s `bind_tls:` gives
  the receiver real server TLS, and that is transport security, not identity: there is no bearer
  token, no basic auth, and no mutual-TLS identity check beyond `rustls` accepting whatever chain a
  client presents when `client_ca_file` is set. Anything that can reach the socket can write series
  into the pipeline. Exactly the gap the two rows above already carry for `admin:` and
  `prometheus_out`'s exposition `bind:`, and it stays one for the same reason: auth has no in-tree
  precedent on any listener yet, so it needs a decision about what kind (bearer, mTLS subject
  matching) before it needs code. Until then the posture is the shipped example's — bind loopback or
  pod-local and front it with something that authenticates
  ([`examples/prometheus-remote-write-receive.yaml`](../examples/prometheus-remote-write-receive.yaml),
  and `docs/deploying.md`'s "Prometheus remote-write" section).
- **1.0 remote-write typing depends on the metadata cache, which is bounded and therefore lapses**
  ([ADR `prometheus-remote-write`](adr/prometheus-remote-write.md)'s "The receiver is stateless"
  section, and `prometheus_in`'s "Metadata cache" module-doc section). Remote-write 1.0 carries a
  family's type, `# HELP` and `# UNIT` in `WriteRequest.metadata[]`, and Prometheus's own 1.0 sender
  ships those in **separate requests** on its own schedule (`metadata_config`, once a minute by
  default) rather than attached to the samples they describe — so without a cache every family in
  nearly every 1.0 request decodes as `unknown`, and `http_request_duration_seconds_bucket`/`_sum`/
  `_count` arrive as three unrelated series instead of one histogram. No sample is *lost* either
  way — only a **declared** family name claims a suffix, so an undeclared `foo_sum` is a family of
  its own rather than a sample thrown away
  (`crates/logit-proto/src/prometheus/assemble.rs`) — what the cache buys is typing, not data.
  `metadata_cache:` closes that
  (`max_families: 10000`, `ttl: 10m`; `max_families: 0` turns it off entirely, which is the setting
  for a pure-2.0 fleet), least-recently-seen evicted first over the cap. What remains a gap is what
  the bound *means*: **an expiry puts a family back to decoding untyped** — its next samples are
  `unknown` and its derived series come apart again — until the sender's next metadata request
  re-declares it. The default TTL is an order of magnitude over Prometheus's own metadata cadence,
  so a live sender has to miss ten refreshes running to lapse, but a sender with a longer
  `metadata_config` interval, or one that has gone quiet and come back, will. Watch
  `logit.input.metadata_cache.size` (gauge), `.evicted{reason="expired"|"cardinality"}` and
  `.replaced`; a steady `expired` stream against a live sender means the `ttl` is under that
  sender's cadence. 2.0 needs none of this — it is fully typed on every request. Two smaller
  bounds sit inside the same row. A remembered `# HELP`/`# UNIT` is **cut to a fixed byte cap**
  (`MAX_METADATA_TEXT_BYTES`, counted `logit.input.metadata_cache.truncated`), because what is
  remembered outlives the request that carried it and the request's own size cap does not bound a
  table that keeps entries — the *type* is remembered exactly, so only the description text is
  affected. And a remembered type is **advisory, never authoritative**: where a declaration the
  request itself carried would make the assembler throw a sample away, a remembered one gives way
  instead and the sample opens an implicit family of its own, counted
  `logit.input.metrics.degraded{reason="seed_mismatch"}`
  (`crates/logit-proto/src/prometheus/assemble.rs`'s "A seeded type is advisory" table). So a stale
  or wrong memory costs typing, never data — but a steady `seed_mismatch` stream means the table
  and the senders disagree about a family's shape, which is worth chasing rather than tuning.
- **The remote-write receiver's metadata table is shared by every sender that can reach it, and
  peers can evict each other's entries.** There is one table per `prometheus_in(bind)` component,
  not one per peer, and eviction is strictly by `last_seen`
  (`crates/logit-inputs/src/prometheus.rs`'s "Metadata cache" module-doc section). That sharing is
  the point — it is what lets a 2.0 sender's inline declarations type a 1.0 sender's series — but
  it cuts both ways: a peer that declares a great many families pushes other peers' entries out of
  `max_families`, counted `.evicted{reason="cardinality"}` with **no attribution** to who caused
  it. Repeated faster than the victims' own `metadata_config` cadence, that keeps well-behaved
  senders permanently untyped. Their samples still arrive — the remembered type is advisory, see
  the row above — as flat families. Nothing on this listener authenticates a sender, so the
  conclusion is the no-auth row's rather than a new one: do not point a `bind:` at senders you do
  not control, and `max_families: 0` turns the sharing off along with the typing. The default is
  on, so every existing `bind:` config acquires this on upgrade.
- **The remote-write sender does no cross-batch reordering, so a fan-in topology can draw
  out-of-order `400`s.** `prometheus_out(endpoint)` sends samples in batch order and nothing in the
  sink reorders across batches ([ADR `prometheus-remote-write`](adr/prometheus-remote-write.md)'s
  "Sender behaviour", and the sink's own module doc). Both specs require a sender to write one
  series' samples in timestamp order, so two upstream branches that both write the same series into
  one `prometheus_out` can present a receiver with an older sample after a newer one — which a
  receiver with no out-of-order window (a stock Prometheus, Mimir without
  `out_of_order_time_window`) answers `400`, classified `Fault::Permanent` and dropped. This is a
  property of the pipeline that was built, not a bug in the sink: a single chain into one
  `prometheus_out` cannot have it, and the fix is a topology that doesn't split one series across
  branches, or a receiver configured with an out-of-order window. Not something the sink can buffer
  its way out of without inventing a reorder window of its own, which is the `aggregate`-shaped
  state a sink deliberately doesn't hold.
- **`prometheus_in`'s `tls:` key is gone, and writing it is silently ignored rather than rejected.**
  The kind has two TLS-shaped roles now, so every TLS key is prefixed by the mode it serves:
  `scrape_tls:` (client TLS for outbound scrapes) and `bind_tls:` (server TLS for the remote-write
  receiver) — [ADR `prometheus-remote-write`](adr/prometheus-remote-write.md)'s "mode-prefixed TLS
  keys". Pre-release, so the rename ships with no alias and no deprecation window. The residual gap
  is that **no `ComponentKind` variant carries `#[serde(deny_unknown_fields)]`** — `TailOptions`'
  own doc comment in `crates/logit-config/src/lib.rs` states the general rule, and the reason it
  can't have one there (serde refuses the attribute alongside `#[serde(flatten)]`) is narrower than
  the rule it names. So an operator who writes the old `tls:` under a `prometheus_in` gets no error
  at all: the block is dropped at parse time, the
  component starts with default TLS settings, and a scrape that should have presented a client
  certificate quietly doesn't. The same silence covers any misspelled key on any `ComponentKind`
  variant; this row names the one rename that makes it likely to be hit in practice. `logit
  validate` cannot catch it either, for the same reason.
- **`influxdb_out` and `prometheus_in`'s scrape client still follow HTTP redirects.** Both build
  their own `reqwest::Client` without a redirect policy (`crates/logit-outputs/src/influxdb.rs`'s
  `build_client`, `crates/logit-inputs/src/prometheus.rs`'s scrape client), so they inherit
  `reqwest`'s `limited(10)` default. `otlp_out` and `prometheus_out`'s remote-write sender do not:
  they share `crates/logit-outputs/src/http.rs`'s `build_client`, which turns the policy off, and
  that helper's own doc comment is where the reasoning lives — a `301`/`302`/`303` is replayed as a
  body-less `GET`, so whatever answers it becomes the sink's verdict on a batch that was never
  written, and a `307`/`308` replays the body *and* the operator's `headers:` at the `Location`
  host, past a config-time `https://` check that has no say at runtime. `reqwest` strips only
  `Authorization`/`Cookie`, and only on a host or port change, so a tenant header always travels.
  The same argument applies to `influxdb_out`'s token and to a scrape URL's basic-auth credential;
  `crates/logit-outputs/src/http.rs`'s module doc names the influxdb half explicitly as a gap worth
  closing separately rather than in passing. Not closed here because `influxdb_out` keeps its own
  `status_class`/`classify_transport_error` pair on purpose (its module doc argues that sink's
  classification is its own to evolve), and moving it onto the shared client is that change, not a
  one-line policy flip.
- **`logit.input.samples` means two different things depending on `prometheus_in`'s mode.** In
  scrape mode it counts the *series* a scrape decoded (`events.len()` — one event per series,
  `crates/logit-inputs/src/prometheus.rs`'s `tick`); in bind mode it counts the **wire samples**
  that reached the `Fanout`, which for a classic histogram is one per `_bucket` plus `_sum` plus
  `_count` rather than one per record. One counter name, two units, on one `ComponentKind`. It is
  deliberate on both sides — the bind-mode number is the same one the 2.0
  `X-Prometheus-Remote-Write-Samples-Written` header reports, and a counter and a header disagreeing
  about one request would be a puzzle with no right answer — but it means summing
  `logit.input.samples` across a deployment running both modes adds series to samples. A
  mode-distinguishing tag was considered and not added: the two modes are already distinguishable by
  which of `logit.input.scrapes`/`logit.input.writes` the same component reports.
- **A `prometheus_in(bind)` whose downstream is already closed still answers `204`.** `Fanout::send`
  silently skips a closed consumer (counted `logit.component.events.dropped{reason=
  "closed_consumer"}`, `crates/logit-pipeline/src/fanout.rs`), and the receiver hands its batch to
  the `Fanout` *before* building the response — the ordering `otlp_in` already uses, and the one
  that makes channel backpressure throttle the sender's own queue. So during a shutdown that has
  already torn the downstream half of the graph down, a sender is told `204` (and, on 2.0, a
  non-zero `Samples-Written`) for a batch nothing kept. Inherited, not introduced here: this is
  [`docs/design/pipeline-graph.md`](design/pipeline-graph.md)'s own named open question — *"today's
  `send_batch` silently drops a send on a closed downstream; under a DAG that closure should really
  propagate as a shutdown signal rather than vanish"* — reaching a transport that has a wire
  acknowledgement to be wrong about. **That last part is what is new in this stack**: the fanout
  behaviour is inherited and unchanged, but until `prometheus_in`'s `bind:` there was no listener on
  it whose protocol made a *durable* promise back to the sender. `otlp_in` answers a success too,
  but an OTLP client's retry is its own business; a remote-write sender treats `204` as "this is
  stored, drop it from my WAL" and will not send those samples again. The no-end-to-end-acknowledgement
  entry above is the general statement of the same limit. Narrow in practice (shutdown is
  per-connection and the window is the drain), and it closes when that open question does, not
  before.
- **Readiness is per-process, not per-sink.** A single sink stuck retrying (`degraded`, in the
  self-logging sense) does not flip `/readyz` to unready — that's what a sink's own `buffer:`
  block (retry budget, queue depth) exists to absorb. `/readyz`'s `degraded` phase is reserved for
  a node that has actually exited with an error, not one that's merely behind. A richer per-sink
  probe is additive to `PipelineState.components` (already keyed by component id) should a real
  need for it show up — not built now because nothing has asked for it yet.
- ~~**A Lua node's post-startup failure is invisible to `/readyz`.**~~ **Closed**
  (`crates/logit-pipeline/src/runtime.rs`'s `watch_lua_thread`): the Lua thread now reports its
  exit over a second oneshot (`done_tx`, beside the existing ready handshake), and a `JoinSet`
  task awaiting that report is the node's entry in the join loop — so a thread that panics after
  reporting ready is treated exactly like any task failing: `NodeState::Failed`, `/readyz`
  `503 degraded`, the same graceful drain SIGTERM drives, exit code `2` with the component named
  in the `exiting` line, plus a `thread_panicked` diagnostic in the self-log stream. A thread
  that returns on its own (inbox closed) reports `Finished` instead of staying `Running`. No
  in-process restart, deliberately — the same fail-fast-for-the-supervisor posture ADR
  `service-lifecycle-and-output-retry` takes for every other node. Unchanged and worth restating:
  a script's *own* `process()`/`flush()` errors are logged and counted, never fatal, so the only
  thing that can kill the thread is a Rust panic; a bad `lua_file`/`Lua` script that fails to
  *load* is still a startup failure (exit `1`), caught by the ready handshake as before.
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

- **`generate_in`'s `rate:` pacing is millisecond-granular above roughly 1k batches/s.** The
  wall-clock catch-up loop (`due = elapsed * rate`; sleep until `start + (sent+n)/rate` when
  ahead) can't subdivide a single OS sleep below about 1ms, so a configured `rate` above roughly
  1,000 batches/s (the default `batch: 100`, so above ~100k events/s) is accurate on average but
  bursty within any one millisecond rather than smooth. Named as a risk at design time
  ([ADR `load-test-harness`](adr/load-test-harness.md), `docs/plans/load-test-harness.md`), not
  fixed: none of `perf/scenarios/*.yaml` sets `rate:` at all (every scenario measures unthrottled,
  backpressure-only throughput), so nothing shipped is affected by it today.
- **`script/perf compare` has no cross-run noise model.** It diffs two results files' medians
  directly against `--threshold`, with no notion of how much run-to-run variance either file's own
  `repeats:` already show. A scenario whose own repeats already spread more than `--threshold`
  (`aggregate` far more than most, `buffered` in the same ballpark, see below) can trip a
  "regression" on nothing but scheduling luck, and a real regression smaller than that scenario's
  noise floor can pass silently. `compare` already warns on a host/CPU-model mismatch between the
  two files; it has no equivalent warning for "this scenario's own repeats disagree by more than the
  threshold you're gating on" — still a real gap in principle, but its own motivating case is gone.
  On the laptop, [`docs/design/performance.md`](design/performance.md) §1's noise sub-section found
  `aggregate` spreads roughly ±25% between repeats even solo on an idle machine, while
  `passthrough`, `json-parse`, and `lua` stayed far tighter on the same run — so a single 5%
  threshold flagged `aggregate` on nothing but its own ordinary variance. **That does not reproduce
  on the disposable perf VM** (2026-09-20): two independent 5-repeat samples of `aggregate` agree
  with each other to within 0.7%, both within one run and across runs an hour apart. Candidate
  fixes remain unbuilt (gate `compare` on each file's `min`, or a per-scenario threshold) in case a
  future scenario needs them, but nothing in the current suite does.
- **`buffered`'s events/s was the least reproducible number this harness reported; the harness-side
  fix has landed (W8, #165) and is now confirmed on a quiet machine — resolved, with one
  product-side question left open, tracked below.**
  `crates/logit-pipeline/src/disk_queue.rs`'s `DiskQueue::open` pays an un-cleared spool's cost twice
  at every startup: it reads and CRC-walks the *active* segment in full to validate it for a torn
  tail (a cost bounded by the default `segment_bytes` rotation threshold, 64MiB — on its own, not
  obviously large enough to explain a multi-second swing), then reads every segment at or after the
  read cursor a *second* time to count what's left to replay — real work whenever the cursor hasn't
  caught up to the end of what's on disk. `perf/scenarios/buffered.yaml`'s spool
  (`perf/results/spool/`, gitignored) used to accumulate across every repeat and every invocation
  that reused it, uncleared: a solo `script/perf run --repeat 5 --scenario buffered` against a spool
  already left over from a prior run degraded monotonically, 134k → 75k → 53k → 38k → 27k events/s,
  with peak RSS climbing 56 → 110 MiB alongside it; deleting `perf/results/spool/` first and
  re-running showed the same shape from a higher starting point (615k → 939k → 289k → 126k → 80k) —
  still degrading within the one invocation, because the harness's own repeats shared the same spool
  directory and never reset it either. See [`docs/design/performance.md`](design/performance.md) §3
  for the full account, including this run's own numbers.

  **Harness-side fix, landed (#165):** `script/perf run`/`attribute`/`flamegraph` now clear every
  `buffer.disk.path` directory a scenario declares before each spawn — every repeat, not just once
  per invocation (`crates/logit-perf/src/spool.rs`), refusing to remove anything outside
  `perf/results/`. A post-fix solo `--repeat 5` on a busy machine: 885k → 510k → 838k → 863k → 792k
  events/s, peak RSS flat at 24.8–30.6 MiB and `startup_s` (spawn → `ready`) a small 2.6–4.4 ms
  throughout — no monotonic decay, no RSS climb, in contrast to every pre-fix sequence above. A
  follow-up solo `--repeat 5` on a quiet, idle machine confirmed the same signature with nothing else
  on the box to blame for any remaining spread either: 632,897 → 659,892 → 641,560 → 780,888 →
  873,406 events/s (2.63 → 2.53 → 2.57 → 2.04 → 1.84 µs/event), peak RSS 25–28 MiB, startup ~4 ms —
  still no decay, no climb. `script/perf attribute --scenario buffered` on the same build put the
  constraint downstream of `gen` (the disk queue's own write/read path), not the harness: `gen` sent
  and `out` received all 1,200,000 events, `gen` spent 1.3959s blocked in `send`, and neither the
  sink nor the listener show any process time of their own. **A 2026-09-20 solo `--repeat 5` on the
  disposable perf VM narrowed the remaining spread further still:** 745,047 → 757,703 → 802,478 →
  780,068 → 801,518 events/s (1.720 → 1.710 → 1.710 → 1.709 → 1.708 µs/event), peak RSS a tight
  70.7–79.0 MiB band — about 7% events/s spread and under 1% on CPU µs/event, the tightest this
  scenario has ever measured. **What remains open, narrower than before:** whether
  `DiskQueue::open`'s still-unchanged double-read startup scan (the active-segment validation pass,
  and the second pass counting what's left to replay against a *cleared* spool's own first-open
  cost) accounts for any of the remaining spread — a product-side item
  (`crates/logit-pipeline/src/disk_queue.rs`) nobody has picked up, and, on the VM's own tighter
  numbers, less consequential than it looked even on the quiet laptop.
- **When and how the load-test harness runs in the ongoing development process is deliberately
  undecided.** [ADR `load-test-harness`](adr/load-test-harness.md)'s own "Open question" section:
  nightly, manually-triggered, gating a PR on a `compare --threshold` regression, or some other
  cadence entirely is real future work this effort didn't answer, not an oversight — the harness is
  built and runnable by hand, and nothing wires it into CI, a pre-merge gate, or a schedule yet.
- **`RunReport.box_state` records nothing on the disposable perf VM.** `logit-perf run`'s
  best-effort governor/EPP/platform-profile/AC-power probe (`crates/logit-perf/src/result.rs`)
  comes back an empty `{}` on every Azure guest, since that sysfs surface doesn't exist there —
  correct behavior for what it checks, but it means the results JSON that's now this project's
  primary provenance record for every recorded number captures nothing about the box beyond
  `hostname`/`cpu_model`/`nproc`. Now that the VM is the reference box (`docs/adr/disposable-azure-
  perf-vm.md`), the reproducibility story would be stronger if `BoxState` also recorded THP setting
  (`/sys/kernel/mm/transparent_hugepage/enabled`), `net.core.rmem_max`/`rmem_default`
  (`/proc/sys/net/core/`), and vCPU topology (`vCPUsPerCore` — from IMDS, or `nproc` alongside
  `/proc/cpuinfo`'s core-id fields) — every one of which turned out to change a finding this effort
  measured (THP flips the `read_batch` RSS story; `rmem_max` decides whether a receive buffer
  clamps). `compare` would then have something to warn on for these too, the same way it already
  does for a hostname/CPU-model mismatch. Not built here — flagged as a real code change
  (`crates/logit-perf/src/result.rs`'s `BoxState`, plus a `compare.rs` warning), out of scope for a
  docs-only rewrite.

- **`shape`'s cumulative gauges are since process start, not windowed.**
  `logit.shape.distinct_keys`, `.distinct_keysets`, `.keyset_share.top1`/`.top5` and
  `.tracking_overflow` (`crates/logit-transforms/src/shape.rs`,
  [ADR `shape-observer-component`](adr/shape-observer-component.md)) accumulate from startup and
  are re-reported unchanged on every flush; they never reset. For the survey this instrument was
  built for that is what's wanted — a capture's whole key-set population, not the last ten seconds'
  — but it means a long-lived tap's distinct-key count only ever rises, so it cannot show that a
  producer *stopped* emitting a key, and `tracking_overflow` latches at `1` for the life of the
  process once either cap is hit. A windowed variant (a second set of gauges reset per flush, or a
  decaying table) is real future work; restarting the process is the only reset today.

- **`shape` tracks top-level attribute keys only.** A nested `Value::Map`'s keys are counted in
  that map's width (`logit.shape.nested_map_width`) and its values in the per-type counters, but
  they never enter the distinct-key set or the key-set hash. Two events whose top-level keys match
  and whose nested maps differ entirely are one key-set as far as `logit.shape.distinct_keysets` is
  concerned. This is deliberate — the key-set identity is what `AttrMap`'s own sorted `Symbol`
  sequence gives for free, and the sizing questions the survey feeds
  (`docs/design/memory.md` §8) are about the top-level map — but it means a shop whose width lives
  under a `k8s`/`labels` map reads as narrow on the distinct-key gauges and wide only on the nested
  ones. Read the two together.

- **A batch's `Scope` passes through `shape` untouched, unlike its `Resource`.** `resource: drop`
  substitutes an empty `Resource` so no resource attribute value leaves the tap, but there is no
  equivalent for `Scope`: `Transform` has no scope-substitution hook, and adding one every
  implementer would have to carry, for this one component, wasn't judged worth it
  ([ADR `shape-observer-component`](adr/shape-observer-component.md)). A scope names an
  instrumentation library rather than carrying payload, so this is a narrow exception to the
  counts-only property rather than a hole in it — but an `otlp_in` whose senders put identifying
  information in `Scope.attributes` should know that it rides through. `shape`'s *flush* output
  carries no scope at all (a window spans many batches, so there is no single one to keep).

- **Nothing bounds a single `shape` measurement event.** `logit.shape.key_bytes` and
  `.value_bytes` carry one value per top-level key and per string leaf, so an event with ten
  thousand attributes produces a ten-thousand-value `Samples` — the only bound is whatever bounded
  the event that produced it. Every *table* in the component is capped and counted
  (`max_tracked_keys`, `max_tracked_keysets`, the per-window batch cap); the per-event vectors are
  the one place that discipline isn't applied, on the reasoning that truncating a measurement of
  width at exactly the widths worth knowing about defeats the instrument. A per-event value cap
  with a drop counter is the obvious fix if a tap ever meets a genuinely pathological producer.

- **`shape` measures `Resource`/`Scope` width as a count only.** `logit.shape.batch.resource_attributes`
  and `.scope_attributes` are attribute counts per batch; there is no resource-side equivalent of
  `key_bytes`/`value_bytes`/`nested_maps`. The per-batch cost `docs/design/memory.md` cares about
  is therefore only half visible — a 20-attribute resource of short enums and one of long ARNs and
  a nested label map read the same.
