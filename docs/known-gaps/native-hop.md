# Known gaps: Native wire format, `logit_in`/`logit_out`, and buffering

Entry format and the other areas: [the known-gaps index](README.md).

- **The native hop has one transport, TCP (optionally TLS).** `logit_in`/`logit_out` cross L4
  infrastructure (a TCP proxy, a network load balancer, a stateful firewall) and not L7 (an HTTP
  ingress, a service mesh in HTTP mode, an application load balancer, an HTTP `CONNECT` egress
  proxy, or TLS termination that routes on HTTP).
  - **Consequence:** a deployment that can only open an HTTP path between two `logit` processes
    has no native hop. The fallback is `otlp_out` to `otlp_in`, at OTLP's fidelity and encode
    cost. A lossy or migrating WAN hop gets TCP's reconnect and loss recovery, with no 0-RTT
    resume and no connection migration.
  - **Revisit trigger:** a deployment that needs one of them. The frames and the session protocol
    carry unchanged over any ordered, reliable byte stream, so the remedy is a second transport
    selected per component (gRPC for L7 infrastructure, QUIC for the WAN case), not a second
    protocol. The 2026-10-05 amendment of
    [ADR `native-transport-handshake-and-ack`](../adr/native-transport-handshake-and-ack.md)
    names the candidates and the first design question of each. Neither is started.
- **No OTLP passthrough codec.** Whether the native protocol should carry OTLP-encoded payloads
  unmodified, so a relay forwards OTLP without re-encoding it into native, is undecided and
  undesigned; see [`design/wire-protocol.md`](../design/wire-protocol.md)'s "Open question".
- **Nothing builds `fuzz/` in `script/check` or CI.** `fuzz/` is its own cargo workspace, so a
  signature change in `logit-proto` or `logit-core` breaks the targets without failing any check,
  and the break shows only at the next campaign's `cargo fuzz build`. For example, a
  decode-budget argument added to the native decoders broke both native batch targets this way.
  See [ADR `out-of-ci-fuzzing`](../adr/out-of-ci-fuzzing.md).
  - **Revisit:** add a `cargo check --manifest-path fuzz/Cargo.toml` step to `script/check` if it
    works on the stable toolchain (`libfuzzer-sys` compiles on stable; only `cargo fuzz run`
    needs nightly), or else a build step in the nightly image, run by hand.
- **`logit_in`'s, `internal`'s, and the five HTTP listeners' shutdown grace is fixed at 5s, not
  operator-tunable.** Graph rule 17 rejects a `receive:` block on all seven (`otlp_in`,
  `datadog_in`, `datadog_trace_in`, `splunk_hec_in`, and `prometheus_in`'s remote-write receiver
  are the five), because none is a datagram, stream, or tail listener, so each always gets
  `ReceiveConfig::default().shutdown_grace`. `LogitInput` uses it to close connections cleanly,
  the HTTP listeners as the deadline at which `run_input` aborts any connection still open, and
  `InternalInput` for its final drain of buffered self-telemetry
  (`crates/logit-inputs/src/internal.rs`). It's a gap if a deployment needs a different number;
  no `receive:`-shaped knob exists.
- **`otlp_in`'s TLS-arm `handshake_timeout` bounds the TLS accept and nothing after it.** `hyper`'s
  `hyper_util::server::conn::auto::Builder` reads the first bytes itself to tell HTTP/1.1 from an
  h2 preface, a read `crate::otlp` can't wrap without reimplementing that sniff. The plaintext arm
  bounds its first byte with a `TcpStream::peek` instead. `http1().header_read_timeout(..)` doesn't
  cover the read either, because it starts only after the version is decided, and
  `protocol: grpc`'s `hyper::server::conn::http2::Builder` has no equivalent knob.
  - **Consequence:** only the opt-in `idle_timeout:`
    ([ADR `idle-connection-timeout`](../adr/idle-connection-timeout.md)), not this pre-message
    bound, closes a TLS connection that handshakes and then goes silent, or a plaintext one that
    sends its one peeked byte and goes silent.

- **A native attribute map with keys in descending dictionary order inserts in quadratic time.**
  `read_attr_map` inserts each key into `AttrMap`'s sorted storage (`AttrMap::insert_sym`, a
  binary search and a `Vec::insert`) as it reads it, so a map whose keys arrive in descending
  order of the receiver's interned symbols shifts every earlier entry on each insert. Measured:
  80,000 keys take 3.4 s in descending order against 20 ms ascending. Real maps are far too small
  for this to show ([`design/data-shapes.md`](../design/data-shapes.md)); only a map with tens of
  thousands of keys in the worst order pays it. It's a non-goal under
  [ADR `deployment-threat-model`](../adr/deployment-threat-model.md): the fix (collect, then sort
  once) changes the ordinary decode path for a shape only crafted input produces.
- **The native decode budget bounds only what arrives over `logit_in`.** The `buffer.disk:` spool
  decodes its records with no budget (`parse_record` in
  `crates/logit-pipeline/src/disk_queue.rs`), because each spooled batch was already that size in
  memory when `DiskQueue::push` wrote it, and a budget refusal there would discard the batch as
  corrupt. `NativeDecoder` (the `Decoder` seam) uses the 256 MiB default. See
  [`design/wire-protocol.md`](../design/wire-protocol.md)'s "Decode amplification".
  - **Consequence:** a sender learns only `max_frame_bytes` from `HelloAck`, not the budget, so
    `logit_in` can refuse a stock `logit_out` batch between roughly 10% and 100% of the cap. The
    refusal is deterministic: `logit_in` answers it with `Ack{rejected(decode_budget)}` naming the
    frame, so the sender drops that batch alone as rejected and diagnoses it rather than retrying,
    and the frames behind it go on over the same connection
    ([ADR `native-hop-ack-status`](../adr/native-hop-ack-status.md)).
  - **Workaround:** change the sender's batching.
- **An oversize frame from an uncompressed sender is refused by name only within a sliver over
  the cap.** `logit_out` defaults to `compression: none`, so a frame's `compressed_len` equals its
  `uncompressed_len`. `logit_in` reads the body and answers `Ack{rejected(too_large)}` only for a
  frame within `frame::compressed_bound(max_frame_bytes)` (`n + n/255 + 16`), through
  `frame::read_frame_prefix`. Past that it answers `Reject{FRAME_TOO_LARGE}` with nothing of the
  body read and closes the connection.
  - **Consequence:** the sender drops the head as `rejected` on the `Reject` and reconnects, and
    the frames behind it in the window are resent on the new connection, where `logit_in`'s marks
    recognize any it already handled. A stock `logit_out` checks the cap before it writes, so only a
    sender that doesn't reaches this path.
  - **To close:** a streaming drain that reads the frame through a fixed scratch buffer with a
    running CRC, up to a drain cap, so `logit_in` can refuse an uncompressed frame by name without
    buffering it ([ADR `native-hop-ack-status`](../adr/native-hop-ack-status.md),
    "Consequences").

- **No durable (disk-backed) buffering on the receive side.** A UDP listener's `ReceiveQueue`
  ([ADR `decoupled-listener-io`](../adr/decoupled-listener-io.md)) is in-memory only, so a
  restart, or a shutdown grace that expires mid-drain, loses what it held. The sink side's
  `buffer.disk:` spool ([ADR `disk-backed-sink-buffer`](../adr/disk-backed-sink-buffer.md)) uses
  frames that would serve the receive side too, but its design isn't started: a listener has no
  equivalent of a sink's "haven't delivered yet" boundary to resume from.
- **The disk-backed sink spool has an accepted power-loss window.** Every cursor write is
  `fsync`ed (tmp file, then directory); a segment is `fsync`ed only when it rotates away and at
  shutdown, not per push (the ADR's "Durability" section and its amendment). A power loss, though
  not a process crash, can lose the active segment's most recent un-`fsync`ed writes. A
  `disk.sync: every_push` knob that closes the window at a throughput cost is a plausible
  follow-up, not built.
- **No spool sharing, compaction, or encryption for the disk-backed sink buffer.** Each sink gets
  one spool directory. Nothing rewrites written segments to reclaim space early: a segment is
  deleted whole once the read cursor crosses it. Nothing encrypts at rest. None of these blocks the
  at-least-once contract; each is narrower future work if a deployment needs it.
- **No out-of-order acknowledgment on the native hop.** `logit_in` forwards a connection's frames
  in order, and `logit_out` commits its store's head on each `Ack`
  ([ADR `native-hop-send-window`](../adr/native-hop-send-window.md)), so a batch slow to forward
  holds up the ones behind it. Out-of-order acknowledgment is out of scope by decision: nothing
  could be acknowledged out of order, and the spool would need a persisted acknowledged bit per
  record.
- **No end-to-end acknowledgment past the first hop.** `logit_out`'s `send` succeeds only after
  `logit_in`'s own `Fanout::send` has accepted the batch into every one of its downstream inboxes
  ([ADR `native-transport-handshake-and-ack`](../adr/native-transport-handshake-and-ack.md)'s "Ack
  point" decision).
  - **Consequence:** if `logit_in` forwards to a further sink (another `logit_out`, an
    `influxdb_out`, and so on), nothing tracks whether the data survives that delivery, and any
    non-`logit_out` sink's `send` means only "the immediate destination accepted the write". The
    receive side is attributable (`logit.component.datagrams.dropped`,
    `logit.input.kernel.drops`;
    [ADR `udp-intake-batching-and-socket-visibility`](../adr/udp-intake-batching-and-socket-visibility.md)).
  - [ADR `delivery-semantics`](../adr/delivery-semantics.md), item 3, makes end-to-end
    acknowledgment a non-goal: an input's acknowledgment means accepted into the pipeline, and
    this entry states that limit.

- **A TLS `logit_out` that dies inside the first record of a frame reads, at `logit_in`, as a clean
  close.** The frame's header travels in its first TLS record. If the sender's connection fails
  before that record is complete, `logit_in` has decrypted zero bytes of the header when the
  stream ends, so `read_header` takes the end as a close between frames and counts nothing, where
  `logit.proto.errors{reason="truncated_header"}` would be truthful.
  - **Consequence:** only the listener's count is off. Nothing is lost or duplicated: the
    sender's write fails `Clean`, and the batch is resent on a new connection.
  - **Why it isn't fixed:** the listener can't tell the two apart. rustls 0.23.45 marks
    `has_seen_eof` on the transport EOF whatever its deframer still holds, so a record that never
    completed and no record at all both reach `read_header` as the same `UnexpectedEof` with no
    plaintext read
    ([ADR `sink-send-path-and-attempt-accounting`](../adr/sink-send-path-and-attempt-accounting.md),
    decision 12).

- **A shutdown grace that cuts a `logit_out` `send` mid-write commits, and counts as dropped, a
  batch that provably never landed.** `write_loop` reads any cut-off send as `Fault::Ambiguous`,
  because it can't know how far the send got. On `logit_out`, a cut inside the frame's write or
  flush leaves `logit_in` holding a truncated frame it never forwards, so the batch didn't arrive.
  Under `buffer.delivery: at_most_once`, the runtime then commits it and counts it
  `logit.component.batches.dropped{reason="shutdown"}`; under the default, `at_least_once`, it
  stays queued.
  - **Why it isn't fixed:** the runtime sees a cancelled future, not where in the send it
    stopped, and a cut inside the ack wait, after the frame landed, is truly ambiguous.
    [`design/pipeline-graph.md`](../design/pipeline-graph.md)'s "Cancellation points" table has
    the row.
  - **Workaround:** to keep a queued batch across the restart, set `buffer.disk:` on the
    `logit_out` component, which persists it at the read cursor.

- **A shutdown with a `logit_out` window in flight can count as dropped batches that `logit_in`
  then forwards.** At a grace cut, each frame in flight is counted
  `logit.component.batches.dropped{reason="shutdown"}`: under `at_most_once` the cut commits it,
  and under the default `at_least_once` it stays reserved and a memory store's `finish` counts
  it. Those frames were written whole, and when the connection is still pooled,
  `Output::flush`'s `shutdown()` closes it cleanly, so `logit_in` reads and forwards them. The
  over-count is at most `window` batches.
  - **Why it isn't fixed:** the sink can't learn which of them `logit_in` forwarded without
    waiting for `Ack`s past the grace.
  - **Workaround:** under `at_least_once`, a `buffer.disk:` store doesn't count them. It replays
    them on the next start, and `logit_in` acknowledges any it already forwarded without
    forwarding them again ([ADR `native-hop-send-window`](../adr/native-hop-send-window.md),
    decision 6).

- **The native hop still forwards a duplicate after a `logit_in` restart, an evicted sender, or a
  load balancer.** `logit_in` deduplicates a resend or a `buffer.disk:` replay against a
  high-water mark per sender identity, held in memory and bounded at
  `max_connections + max_connections / 4` identities, evicting the least recently seen. A
  restarted `logit_in`, an identity evicted from a full table, and a second `logit_in` behind a
  load balancer hold no mark for the sender, so each forwards the resend.
  [ADR `native-hop-identity-and-sequence`](../adr/native-hop-identity-and-sequence.md), decisions
  5 and 6, accepts this: the at-least-once target prefers the duplicate to a loss.

- **A resend can race the frames an ended connection still holds, and be forwarded twice.**
  `logit_in` holds no lock per sender identity across a forward, and it raises a sender's mark
  only once a consumer takes the frame or once it refuses the frame by name.
  - **The race:** a fault that ends a `logit_out` connection mid-window (a reset, a read error, a
    message other than `Ack`, an ack timeout) can leave that connection's task holding whole frames
    in its socket buffer. The task reads and forwards them while the sender resends the same window
    on a new connection. For each sequence, whichever task checks it against the mark first
    forwards it, and the other skips it as a resend once that forward raises the mark. Both forward
    it when both check before either forward lands, most often when the old task's forward is
    parked on a full inbox past the sender's ack timeout.
  - **The bound:** each task forwards a given frame at most once per connection that held it, so
    twice per fault, and one more time for each further connection that times out on the same
    inbox. The old task ends at its first `Ack` write after the reset arrives: the sender has
    closed that socket, so the write meets a reset within about a round trip. With acks coalesced,
    that write comes when the task's next read would wait, at an identity change, or at the
    32-frame cap. Before it, the task can forward the frame it was parked on plus every frame still
    buffered on the closed socket, up to the cap. The hard bound is the window the old connection
    held, and the cap keeps a run under 32. Under the sustained backpressure that parks a forward,
    each of those forwards can race the resend of the same frame.
  - **Consequence:** nothing is lost. A duplicate copy can reach the consumers after later
    batches; a batch's only copy never does, because the mark reaches a sequence only after a copy
    of it landed or `logit_in` refused it by name, and a refused frame is never forwarded by any
    connection, since it fails the same way on each.
  - [ADR `native-hop-identity-and-sequence`](../adr/native-hop-identity-and-sequence.md), decision
    7, accepts the race: a per-sender lock held across the forward would close it at the cost of a
    lock per frame, to prevent a duplicate the at-least-once target tolerates.
    [ADR `native-hop-send-window`](../adr/native-hop-send-window.md), decision 6, keeps that with a
    window.

- **A refused frame whose rejected `Ack` is lost is reported delivered by the sender.**
  `logit_in` raises a refused frame's mark before the sender reads the rejected `Ack`
  ([ADR `native-hop-ack-status`](../adr/native-hop-ack-status.md), decision 4). If the connection
  fails in between (a write failure, a stall, the sender's ack timeout), `logit_out` reads it as
  `Ambiguous`, and the next `HelloAck.marks` covers the sequence, so the sink commits the frame
  without resending it.
  - **Consequence:** the batch was dropped, never forwarded, but the two ends disagree on why. To
    reconcile, compare `logit_in`'s `logit.input.batches.dropped{reason="rejected"}` with the
    sending sink's `logit.component.batches.dropped{reason="rejected"}`: the excess at the input is
    batches the sink counted under `logit.output.batches.resumed` and
    `logit.component.batches.delivered`. Nothing is lost beyond the drop itself.
  - **Possible fix, out of scope:** a bounded per-identity set of refused sequences that
    `HelloAck` names, so a resume commits them as dropped.

- **A reconnect resumes at most 16 sender identities.** After a fault, `logit_out`'s next `Hello`
  lists the identities of the frames it will resend, so `logit_in` can answer their marks and the
  frames at or below them are committed without a send (`logit.output.batches.resumed`). The list
  holds at most 16 identities (`MAX_HELLO_SENDERS`). A window whose frames span more than 16 store
  opens, which takes a spool replaying records from that many earlier opens, resends the frames of
  the 17th identity on.
  - **Consequence:** `logit_in` recognizes each of those by its mark and doesn't forward it again,
    so the cost is the encode and the bytes. The previous entry's duplicate cases are unchanged.
    [ADR `native-hop-named-acks`](../adr/native-hop-named-acks.md), decision 4, accepts this.
  - **Revisit trigger:** `logit.input.batches.resends` climbing after reconnects that the resume
    should have absorbed.

- **Coalesced acks cost about 2 points of a window's ceiling under 10 ms RTT.** At `window: 32`
  and a 10 ms round trip (`tc qdisc ... netem delay 5ms`), `logit_in`'s named acks
  ([ADR `native-hop-named-acks`](../adr/native-hop-named-acks.md)) reach 97.2% of the 3,174
  batches/s ceiling against 99.5% for per-frame acks, over 8 repeats each, and every named-ack
  repeat is below every per-frame repeat. The same acks use 12.8% less CPU per event, and on
  loopback they're 10.4% cheaper.
  - **Cause:** burst handling before the coalesced `Ack`, not the coalescing cap. An `Ack` covers
    about 4 frames under latency and about 2.5 on loopback, far below `ACK_COALESCE_MAX` (32).
    Caps of 8 and 16 reach 98.5% and 97.6%: cap 8 recovers 1.3 points, outside M's repeat range,
    and cap 16's +0.4 is inside it. The cap stays at 32 because the whole gap is about 2 points.
  - **Unexplained:** `native-relay-window1`, where nothing coalesces, also reads +2.3% from the
    pre-named-ack binary to this one (1.150 → 1.177 µs/event), under the 5% gate.
  - **Numbers:** [`design/performance.md`](../design/performance.md) §1, "`native-relay` under a
    10 ms round trip"; the plan is
    [`plans/native-hop-named-acks.md`](../plans/native-hop-named-acks.md) "Findings".

- **A disk-backed sink replays delivered and dropped batches after a crash, under either
  posture.** `commit` moves the read cursor in memory. The cursor reaches disk on a commit once
  `checkpoint_interval` (1 s by default) has passed since the last write, on a segment roll, or at
  shutdown, with no timer.
  - **Consequence:** after a crash, the spool replays the batches in flight and every batch
    committed since the last cursor write, which after an idle period can be far older than the
    interval. `logit.component.buffer.disk.replayed` counts every record resumed after the cursor,
    so it can't separate those re-deliveries from the backlog that was never sent. Under
    `at_most_once`, the re-deliveries include a batch the sink dropped as `Ambiguous` to avoid a
    duplicate. A `statsd_out`, or a sink whose destination aggregates a resend, with
    `buffer.disk:` can replay counters from that window after a crash.
  - On the native hop, a `logit_in` that still holds the replayed records' sender identity acks
    them without forwarding them
    ([ADR `native-hop-identity-and-sequence`](../adr/native-hop-identity-and-sequence.md)).
  - It isn't a gap to close: [ADR `delivery-semantics`](../adr/delivery-semantics.md), item 8,
    keeps the combination valid and states the window.
