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
- **`internal`'s and the six HTTP listeners' shutdown grace is fixed at 5s, not operator-tunable.**
  Graph rule 17 rejects a `receive:` block on these seven and on `logit_in` (the six are
  `otlp_in`, `datadog_in`, `datadog_trace_in`, `splunk_hec_in`, `influxdb_in`, and
  `prometheus_in`'s remote-write receiver), because none is a datagram, stream, or tail
  listener, so each always gets `ReceiveConfig::default().shutdown_grace`. The HTTP listeners use
  it as the deadline at which `run_input` aborts any connection still open, and `InternalInput`
  for its final drain of buffered self-telemetry (`crates/logit-inputs/src/internal.rs`). It's a
  gap if a deployment needs a different number; no `receive:`-shaped knob exists. `logit_in` gets
  the same default but doesn't use it: its accept loop returns when shutdown fires, and each
  connection, a task of its own, answers `GOING_AWAY` and closes at the next frame boundary (see
  the module doc of `crates/logit-inputs/src/logit.rs`, "Shutdown").
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
  not a process crash, can lose the active segment's most recent un-`fsync`ed writes. Of those,
  the next open counts only a torn final record, as `batches.dropped{reason="disk_torn"}`; records
  lost whole leave no bytes and aren't counted. The same count can also include a batch already
  counted: a write that failed and was counted `disk_full` or `disk_io_error`, if no later push
  repaired its bytes before the process exited, or a record delivered and committed after the
  last cursor persist (every `checkpoint_interval`) whose bytes the power loss tore. A
  `disk.sync: every_push` knob that closes the window at a throughput cost is a plausible
  follow-up, not built.
- **A disk-backed sink's shutdown `fsync`s have no bound.** `DiskQueue::finish` runs after the
  shutdown grace has expired and waits for the persist worker, flushes and `fsync`s the active
  segment, then `fsync`s the spool directory, with no timeout on any of them
  (`crates/logit-pipeline/src/disk_queue.rs`; `finish_and_flush` in
  `crates/logit-pipeline/src/runtime.rs` adds none). On a slow or stalled disk the process
  outlives the grace by that long. Skipping the remaining `fsync`s after a bound would be safe:
  a stale cursor costs duplicates on the next open, and the spool keeps what it wrote.
  - **Revisit trigger:** a shutdown that runs past the grace on a slow disk, such as on the perf
    VM.
- **No spool sharing, compaction, or encryption for the disk-backed sink buffer.** Each sink gets
  one spool directory. Nothing rewrites written segments to reclaim space early: a segment is
  deleted whole once the read cursor crosses it. Nothing encrypts at rest. None of these blocks the
  at-least-once contract; each is narrower future work if a deployment needs it.
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

- **Coalesced acks cost about 2 points of a window's ceiling under 10 ms RTT.** At `concurrency: 32`
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
    10 ms round trip".
