---
created: 2026-09-29
updated: 2026-09-29
---

# Sink send path and attempt accounting: counters that say what they count, one pooled-stream driver, and TLS writes that are flushed

## Status
Accepted

## Context

`docs/plans/critical-sections-inventory.md` groups nine entries as cluster 6, "Sink send path":

- SINK-01, SINK-02, and SINK-03 cover the pooled-TCP send machine that `statsd_out`,
  `syslog_out`, and `graphite_out` each carry a copy of, its dial, and the half-open probe.
- SINK-04 covers UDP datagram packing and `EMSGSIZE` handling.
- SINK-05 and SINK-06 cover the `Output` trait contract and encode-side counters emitted per
  `send`.
- WIRE-08 and WIRE-09 cover `logit_out`'s send path and the close probe.
- RT-05 covers `deliver_with_retry` and `backoff_for`.

Top leads 12 and 14 sit on the same code. Read-only passes checked the entries against `main`
and against the pinned sources of tokio-rustls 0.26.5, rustls 0.23.45, and tokio 1.53.1. They
found the following.

- **A TLS write can return before its bytes reach the socket (WIRE-08).**
  tokio-rustls's `poll_write` returns `Ok(n)` with up to 64 KiB of ciphertext still queued in
  the session whenever a socket write goes `Pending`, and a `poll_read` that processes its
  records cleanly drives no writes.
  `logit_out` writes a frame and then waits for an `Ack` without calling `flush()`. Under TLS the
  peer never receives the queued tail, so the wait ends in a timeout, classified `Ambiguous`.
  Under the default `at_most_once` posture that drops a batch the peer never saw. The same shape
  exists at `Hello`/`HelloAck`. `logit_in`'s unflushed `Ack` and `Reject` writes have the same
  shape but weren't traced.
- **The `logit_out` first-write verdict has the wrong premise and the right answer (WIRE-08).**
  `send` classifies a first-write `Err` as `Fault::Clean` on the premise that nothing left the
  host, and discards the `io::Error`. Under TLS an `Err` can follow bytes of this frame reaching
  the wire. The verdict is still safe, because the peer then holds a truncated frame it can't
  forward.
- **The pooled-TCP send machine exists in three drifting copies (SINK-01, SINK-02).**
  `graphite_out` has no `flush()`, no `is_tls` guard, no `logit.output.reconnects`, and a
  concrete `TcpStream` with no injection point for a test. `statsd_out` and `syslog_out` classify
  an invalid TLS server name as `Fault::Clean`, so a bad endpoint retries to budget exhaustion on
  every batch.
- **`logit.output.requests` means different things per sink (WIRE-08).** The three line sinks
  use `class=ok|error` and count connect failures. `logit_out` uses four fault classes and skips
  connect, handshake, frame-encode, and all three too-large returns.
- **Encode-side counters repeat on every retry (SINK-06, lead 12).** Every sink re-encodes per
  attempt and re-emits its drop and normalization counters. Codecs that emit their own counters
  (graphite, collectd, OTLP, Prometheus, Datadog, Splunk) do the same through the `Telemetry`
  handle. `datadog_out` also reads the clock per attempt, so its stale-point drops can differ
  between attempts of one batch.
- **`retry_max_delay: 0s` spins (RT-05).** It's operator-reachable and unvalidated.
  `backoff_for` returns 0 and `deliver_with_retry` loops until the budget ends.
- **The UDP packer trusts the encoder's cap (SINK-04).** It appends an entry of any length into
  an empty buffer. `is_message_too_large` is four copies whose `InvalidInput` fallback swallows
  errors that aren't `EMSGSIZE`. `collectd_out` accepts `max_packet_bytes` up to 65535, but a UDP
  payload can't exceed 65507. The four UDP sinks bind `0.0.0.0:0` and use the first resolved
  address, so an IPv6 endpoint fails `Clean` on every batch.
- **Prose that is wrong (SINK-03, WIRE-09).** `poll_pending_close`'s doc says a `Pending` poll
  consumes nothing and that a cancelled read loses data. A `Pending` poll can move a partial
  record from the socket into the TLS session, and a cancelled read loses nothing. The behavior
  is sound: the session keeps the record, and the probe never drops the stream on `Pending`.
  The statsd and syslog claim that each record is a run of complete lines is wrong for the same
  reason.
- **`observe_batch` runs once per batch (SINK-05).** The trait doc and `LogitOutput`'s doc say
  once per attempt. `write_loop` calls it before `deliver_with_retry`.

The transport counters, and the drop counters a peer's verdict produces, are per attempt by
nature. The encode-side counters are the only ones that measure the batch and not the attempt.

## Decision

1. **Sink counters fall into three classes, and each class has one rule.**
   - **Encode-side** counters describe the batch: `logit.output.messages.dropped` for a reason
     the encoder decided, `logit.output.tags.dropped`, `logit.output.batch.bytes`, normalization
     counts, and the counters a codec emits inside `encode_into`. They count once per batch,
     however many attempts it takes.
   - **Transport** counters describe an attempt: `logit.output.requests`,
     `logit.output.request.duration`, `logit.output.ack.duration`, and
     `logit.output.request.bytes`. They count once per attempt.
   - **Server-verdict and kernel drops** count what a peer or the kernel refused on an attempt:
     a Splunk code 6 and the oversize split, an HTTP 413, an OTLP `partial_success`, and
     `EMSGSIZE`. They count per attempt. A batch retried after such a verdict counts the verdict
     again, because each attempt got its own answer. Counting them once would need the sink to
     remember what an earlier attempt learned, and a retry might get a different answer. An
     `EMSGSIZE` drop repeats in two sequences: drops followed by a failure before any datagram of
     the attempt was sent, under either posture, since that failure is `Clean` and `Clean` retries
     under both; and any `Ambiguous` retry under `at_least_once`, which is `graphite_out`'s
     default.

   `docs/design/internal-telemetry.md` and `docs/deploying.md` state the third class's
   repetition, so an operator reading a drop counter on an unhealthy sink knows what it measures.
2. **A gate in `Telemetry`, armed once per batch, makes encode-side counters count once per
   batch.** (Amended by `sink/w5`: an armed gate in `Telemetry`, not a sink-owned one that
   defaults open.)
   - `logit_core::CountGate` is a shared switch. `Telemetry::gated` and `Diagnostics::gated`
     build a new handle over the same buffer and the same throttle. While the gate is muted, the
     gated `Telemetry` drops `count` calls, and the gated `Diagnostics::warn_throttled` returns
     before it counts or bumps the throttle, so the next unmuted report is numbered as if the
     muted ones never happened. A gauge or timing isn't gated: a gauge is last-write-wins, and a
     timing measures an attempt. The gate attaches to a disabled handle too, so the throttle is
     muted when the config has no `internal` component.
   - The gate is never state in `ComponentBuffer`: the runtime holds a clone of the same handle,
     and every ungated handle shares the buffer.
   - Each sink holds a `BatchAccounting` (`crates/logit-outputs/src/accounting.rs`) and two sets
     of handles. Its encoder gets views gated by the sink's gate, in every builder order. The sink
     keeps the ungated originals for its transport counters (`requests`, `request.duration`,
     `reconnects`, `messages`, `datagrams`, `datapoints`) and for kernel and peer verdicts
     (`oversize_datagram` from `EMSGSIZE`).
   - `Output::observe_batch`, which `write_loop` calls once per batch before the batch's first
     attempt, arms the gate and clears the units counted. Each encode runs as a synchronous
     closure through `BatchAccounting::encode(unit, ..)`: an encode of a unit the armed batch has
     already encoded runs muted, and the gate unmutes when the closure returns, so nothing awaits
     while it's muted. An `Ok` send disarms the gate.
   - An unarmed gate never mutes. A caller of `send` that never calls `observe_batch` (a unit
     test, a benchmark, `logit_pipeline::send_batch`) sees every encode counted.
   - The encode-side counts a sink emits itself (`statsd_out`'s and `syslog_out`'s `EncodeStats`,
     `influxdb_out`'s `tags.normalized`, every sink's `batch.bytes`, the datagram packer's
     over-cap skip) are skipped when `encode` reports a repeat.
   - A unit is a batch, or a route for `datadog_out` and `datadog_trace_out`, which encode each
     route lazily and can encode one route after an await that another attempt already passed.
   - `Output` gains no method and no parameter.

   The bisection in `split_encode` (`crates/logit-outputs/src/http.rs`), which `datadog_out` and
   `datadog_trace_out` both call, re-encodes each half of an over-limit request. Those
   re-encodes count through the same gated view, so they are muted twice over: a bisection
   triggered by a retry doesn't count again, and, because the gate closes after a unit's first
   encode, bisection re-encodes inside one attempt are muted too. Today they count the same
   records more than once within a single `send`, which `datadog.rs`'s module doc records.
3. **`datadog_out` fixes `now` once per batch, in `observe_batch`.** Staleness isn't monotonic
   in `now`: a point is stale when it's older than `METRIC_MAX_AGE` or more than
   `METRIC_MAX_AHEAD` in the future. With a clock read per attempt, a point that was ahead of the
   window on attempt 1 can fall inside it on attempt 2, so the sink counts it dropped and later
   delivers it. With one `now`, every attempt reaches the same verdict for every point. The
   trade-off is that a batch retried for `retry_budget` can send a point up to that long past the
   end of its window.
4. **`logit.output.requests` counts every attempt that returns, tagged
   `class=ok|clean|ambiguous|permanent`.** This applies to `statsd_out`, `syslog_out`,
   `graphite_out`, `collectd_out`, and `logit_out`, so the label set is the one `Fault` defines.
   - A connect failure, a handshake failure, and a too-large return are attempts and count.
     `logit_out`'s pre-connect too-large `Permanent` counts as `class=permanent`, and the docs
     say so.
   - An empty batch that returns before any I/O isn't an attempt and doesn't count.
   - A cancelled attempt (a budget timeout or the shutdown grace dropping the future) is
     uncounted at the sink, because a dropped future can't run code after its last await.
     `logit.component.errors` and the drop counters cover it.
   - `logit.output.request.duration` records on a cancelled attempt, because its timer records
     in `Drop`. `requests` doesn't. The docs say so, and the code doesn't change.

   The HTTP sinks' `requests` vocabulary differs from this one. Aligning it is follow-up work
   outside this decision.
5. **`statsd_out`, `syslog_out`, and `graphite_out` share one pooled-stream driver.** The
   callers build the frame and count their own encode-side and per-message results. The driver
   takes `&[u8]` and owns:
   - the dial (TCP, TCP with TLS, and Unix stream), through a free `connect` function;
   - the probe on a reused connection (`poll_pending_close`);
   - the first `write` followed by `write_all` for the remainder;
   - the `flush` after the write;
   - one reconnect on a plaintext first-write failure;
   - fault classification;
   - `logit.output.reconnects` and `logit.output.requests`.

   `graphite_out` gains the `flush` and `logit.output.reconnects` it lacks. The driver's
   connection holds a `has_connected_once` flag so the first dial doesn't count as a reconnect.
   `logit_out` keeps its own `send` and calls the shared `connect`. Its protocol is framed and
   acked: it must interleave a `Hello` exchange, read a reply before it can classify a write's
   outcome, and check `Ack.seq`. The driver's one-write, one-flush shape would need those
   hooks, and they'd be a second protocol inside a shared component. Sharing the dial without
   sharing the send keeps the drift risk in the part that was copied, which is the dial.
6. **The fault rule for a write depends on the transport and on the protocol's framing.**
   - On a plaintext line or message stream, a first-write `Err` is `Fault::Clean`. Nothing of
     this frame reached the peer, so the driver reconnects once and retries the frame itself,
     and a second failure returns `Clean` to `write_loop`.
   - Under TLS, a write `Err` on a line or message stream is `Fault::Ambiguous`. The session may
     have put a record on the wire before the error, and the peer may forward the complete lines
     inside it.
   - On `logit_out`, every failure before the frame is completely written and flushed is
     `Fault::Clean`, under TLS and plaintext alike: a write `Err`, a write `Ok(0)`, and a flush
     `Err`, wherever in the frame they fall. `logit_in` fills the whole frame, checks its CRC,
     decodes, forwards, and only then acks, and it has no partial decode path, so a peer that
     didn't receive every byte of the frame can't hold the batch. Bytes of the frame may have
     left the host; `Clean` says what the peer holds, not what left. The sink keeps the
     `io::Error` in the chain and drops the connection. The one `Ambiguous` window is the ack
     wait.

     One residual: a TLS 1.3 `KeyUpdate` queued behind the frame. In rustls 0.23.45,
     `PlaintextSink::write` buffers the plaintext and then calls `maybe_refresh_traffic_keys`.
     When a record's sequence number reaches the suite's confidentiality limit
     (`RecordLayer::pre_encrypt_action`), `send_single_fragment` sets
     `refresh_traffic_keys_pending`, and `maybe_refresh_traffic_keys` queues a `KeyUpdate` record
     behind the records of the same `write` call. If that call carried the frame's last record,
     a write or flush can fail on the `KeyUpdate` after the whole frame reached the socket. The
     attempt is then `Clean` although the peer holds the frame and forwards it, and a resend
     duplicates the batch. The limit is 2^24 records under one traffic key for the AES-GCM suites
     (`TLS13_AES_128_GCM_SHA256` and `TLS13_AES_256_GCM_SHA384` in the `ring` provider), so the
     case is reachable only on a connection that has carried 16 777 216 records, at least one per
     frame; for `TLS13_CHACHA20_POLY1305_SHA256` the limit is unreachable. It isn't fixed: the
     write and flush can't tell a `KeyUpdate` failure from a frame failure, and the window is one
     record in 2^24.

   The difference between the line sinks and `logit_out` is framing. A line stream has no frame
   boundary the peer waits for: any prefix that ends at a newline is a complete record it
   forwards. A `logit_out` frame carries a length and a CRC-32C, so a prefix is never forwarded.
7. **`logit_out` calls `flush()` after the frame write and after the `Hello` write, before it
   waits for a reply.** Without the flush, the failure in the Context section follows. tokio-rustls
   0.26.5's `poll_write` can return `Ok(n)` with ciphertext still queued in the session, and a
   `poll_read` that processes its records cleanly doesn't drive the writes that queued it. So the
   peer never receives the tail of the frame or the `Hello`, and the sink waits for a reply the
   peer can't send. The timeout is `Ambiguous`, and under `at_most_once` it drops a batch the
   peer never received. The handshake shape is the same: a `Hello` split across a `Pending`
   socket write stalls the `HelloAck` wait. A flush after the write closes both. Plaintext
   `TcpStream::flush` is a no-op, so the flush costs nothing off TLS. A read that fails on a bad
   record does write (decision 8), but the stall is on the clean path, where a waiting read
   drives nothing, so the flush is needed either way.

   The flush follows the whole write, before `conn.seq` advances and before
   `logit.proto.frames` and `logit.proto.frame.bytes` count the frame, and it isn't under the
   sink's `request_timeout`: a large frame on a slow link can outlast that timeout, and the retry
   budget bounds the flush as it bounds the write. `logit_in` flushes its own control writes
   (`HelloAck`, `Ack`, and every `Reject`) inside the `handshake_timeout` bound its
   `write_control` already applies. With one frame in flight, a control message fits the socket
   of a peer that is waiting for it, so the listener side is a contract fix rather than an
   observed stall; a tokio-rustls pair over `tokio::io::duplex(16)` reaches it.
8. **Third-party semantics are pinned by tests, and a dependency bump re-verifies them.** The
   design rests on facts about tokio-rustls 0.26.5, rustls 0.23.45, and tokio 1.53.1. Tests run
   against a real TLS pair over `tokio::io::duplex` with a small buffer, which makes a mid-record
   `Pending` deterministic. A bump of any of the three crates is a trigger to re-run them and
   re-read the sources. The pinned facts are:
   - `poll_write` returns `Ok(n)` with ciphertext still queued whenever the socket write goes
     `Pending`, up to 64 KiB.
   - `poll_flush` drives that queue to the socket.
   - A `poll_read` that processes its records cleanly drives no writes, so a reader waiting for
     a reply doesn't push out an unflushed request.
   - A `poll_read` whose record processing fails (a bad record, or a peer's fatal alert) makes
     one last-gasp write for the alert, which sends queued ciphertext first, and then returns an
     error (`ErrorKind::InvalidData` for a bad record).
   - A `Pending` `poll_read` can move a partial record into the session. The session keeps it,
     and a later read completes the record.
   - Dropping a read future loses nothing the session already holds.
   - A peer's `close_notify` reads as `Ready(Ok)` with an empty buffer.
   - A peer close without `close_notify` reads as `ErrorKind::UnexpectedEof`.
   - A poll of an idle TLS 1.3 connection with post-handshake tickets in flight is `Pending`, not
     an empty `Ready(Ok)`.
   - A write `Err` can follow ciphertext from the same call reaching the peer.
   - `write_all` maps an `Ok(0)` write to `ErrorKind::WriteZero`.
   - tokio-rustls treats an IO `Ok(0)` as would-block: a write still returns `Ok(n)` for what the
     session took, a write into a full session returns `Pending` with no waker held, and a flush
     fails with `WriteZero`. So the plaintext seam takes a `FakeStream` and TLS tests take the
     real pair.

   Each fact gets a test.
9. **The four UDP sinks and `statsd_out`'s `transport: unix` share one datagram send path,
   `crates/logit-outputs/src/datagram.rs`.**
   - **One packer.** `send_datagrams` is generic over an entry's weight: `1` for `statsd_out` and
     `syslog_out`, datapoints for `graphite_out`, value lists for `collectd_out`. Its framing is
     `Packed` (entries joined by `\n` up to the cap: statsd and graphite) or `OnePerEntry` (each
     entry its own datagram: syslog, which never packs, and collectd, whose encoder chose every
     boundary). One loop holds the fault rule, the `EMSGSIZE` handling, and the counts for all four
     sinks, and it builds each packed datagram in the sink's own buffer.
   - **A destination enum, not a trait.** `DatagramDest` is `Udp`, `Unix` (`UnixDest`, with its
     reconnect-once rule on a batch's first datagram and its `send_timeout`), and a
     `#[cfg(test)]` `Scripted` variant, as the stream driver has `Target::Scripted`. `UnixDest`'s
     connect target has a scripted variant too, so its reconnect and timeout rules run under a
     script.
   - **No over-cap pre-pass.** Every encoder caps its entries at the value its sink passes the
     packer, on every builder path and transport, and `MessageBuf` has no remove API, so a
     pre-pass would scan for something the code can't produce. The packer has one branch, a
     comparison per entry, that skips an over-cap entry and counts it
     `messages.dropped{reason="oversize_datagram"}` in the weight unit. It has no
     `debug_assert!` in front of it, so a debug build's tests reach the branch too.
     `GraphiteOutput::with_encoder` refuses a pickle encoder on UDP, as rule 46 does in config: it
     was the one builder path that could put an entry past the cap, since a pickle frame is bounded
     by `max_frame_bytes`, not by the datagram cap.
   - **`is_message_too_large` tests `EMSGSIZE` only**, as a named constant holding Linux's value.
     The `InvalidInput` fallback is removed. It caught more than `EINVAL`: std raises
     `InvalidInput` itself for a Unix socket path of 108 bytes or more, a NUL in a path, and a name
     that resolves to nothing, and the kernel's `EINVAL` for a UDP send to port 0 maps to it too.
     Each of those fails every datagram alike, so the fallback turned a bad endpoint into every
     batch counted `oversize_datagram` under `requests{class="ok"}`, a silent loss under the wrong
     reason. They are faults now, `Clean` when nothing of the batch was sent.
   - **Two graph rules** reject the two reachable cases at config time: rule 65 rejects a
     `statsd_in`/`statsd_out` Unix socket path of 108 bytes or more, and rule 73 rejects port 0 on
     a UDP sink endpoint. The port is readable at validation even when the host is a name, and
     `!env` is resolved by then.
   - **The ceiling is per transport.** `logit_proto::MAX_UDP_PAYLOAD_BYTES` (65507, the largest UDP
     payload over IPv4) bounds `max_packet_bytes` on `collectd_out`, whose range becomes
     `1024..=65507`, and on `statsd_out` and `graphite_out` under `transport: udp` (rule 38). It
     doesn't bound statsd's Unix transports, whose limit is the socket's send buffer. IPv6 allows
     20 more bytes, but validation can't know a hostname's family, so the bound is the same. A
     `syslog_out` over UDP caps its encoder at `min(max_message_bytes, 65507)`, so a longer message
     is truncated by the encoder's existing truncation rather than refused by the kernel.
   - **IPv4 first, IPv6 when the endpoint has nothing else.** Each UDP sink keeps its IPv4 socket
     bound at construction, so a bad local bind stays a startup error, and binds an IPv6 socket
     the first time a batch needs one; a failed IPv6 bind is `Fault::Clean`. Per batch, the sink
     sends to the first IPv4 address the endpoint resolves to, else the first IPv6 one
     (`pick_addr`).
   - **Partial sends are counted.** `send_datagrams` returns what it sent on every exit, so a sink
     counts `logit.output.messages`, `logit.output.datagrams`, and `graphite_out`'s
     `logit.output.datapoints` for the datagrams that reached the kernel before a failure. They
     are transport facts and count per attempt. A cancelled send returns nothing, so its counts
     are lost, which `docs/known-gaps.md` records.
   - **`count_request` moves to crate level** (`crates/logit-outputs/src/lib.rs`), shared by the
     stream driver, `logit_out`, and the datagram sinks. `collectd_out` counts through it, which
     completes decision 4.
10. **Config validation rejects the values that make retry spin or a bad endpoint retry forever.**
    - Graph rule 15, which already rejects a zero `buffer.max_batches` or `buffer.max_bytes` on a
      sink, rejects `buffer.retry_budget: 0s` and `buffer.retry_max_delay: 0s`, as the graph does
      for other durations where zero breaks the component. A zero budget times every attempt out
      before it starts; a zero delay retries with no pause until the budget ends.
      `deliver_with_retry` `debug_assert!`s both are nonzero and counts attempts with
      `saturating_add`. `backoff_for` isn't floored at `base_delay`: a floor would override a
      `retry_max_delay` an operator set below 200 ms, which is a valid choice.
    - Each TLS sink parses the endpoint's server name once, in `with_tls`, and stores the
      parsed target. A bad endpoint fails startup, not every batch. If a name
      still fails to parse at the send path, the fallback classification is `Fault::Permanent`,
      not `Clean`, so it doesn't retry to budget exhaustion.
11. **`logit_out` refuses a `HelloAck` that doesn't answer its `Hello`, as `Fault::Permanent`.**
    A `HelloAck` whose `version` differs from the `Hello`'s, whose `codec` the `Hello` didn't
    offer, or whose `compression` is unknown or wasn't offered fails the handshake. The peer
    answers an identical `Hello` the same way, which is the reason `REJECT_VERSION_MISMATCH` and
    `REJECT_NO_COMMON_CODEC` are `Permanent`, so this is too. An unknown compression byte no
    longer falls back to none. `logit.output.reconnects` and the first-connection flag change
    only after the `HelloAck` passes, so a refused handshake is not a connection. `write_loop`
    then treats a peer that keeps answering this way like one that keeps sending a version
    reject: each attempt is explicitly `Permanent`, the batch is dropped, and after
    `PERMANENT_FAILURE_WINDOW` (60 s) of nothing but such outcomes the pipeline ends.
12. **`logit_out` sends `close_notify`, and `logit_in` reads a close between frames as a close
    under TLS too.** Before this, `logit_out` never shut its stream down, so under TLS every
    disconnect reached `logit_in` as `UnexpectedEof`, which `serve_connection` returned as an
    error and the accept loop logged as `connection_error`; the listener's clean-close path was
    unreachable under TLS.
    - `Output::flush`, which `run_output` calls once when the sink stops, shuts the pooled
      connection down within the sink's `request_timeout` and drops it. The shutdown sends
      `close_notify` under TLS and a FIN under both. A failure isn't reported, since every frame
      on a pooled connection was acked.
    - A connection dropped after a failed or cancelled attempt can't send `close_notify` without
      an await its drop doesn't have. The listener rule covers it.
    - `logit_in`'s header read treats `UnexpectedEof` with no byte of the next header read as the
      same close as an `Ok(0)`: no frame is in flight. A close or read error part-way through a
      header stays an error and counts `logit.proto.errors{reason="truncated_header"}`, which
      was uncounted; part-way through a body it stays `reason="truncated"`.

13. **`stdio_out` and `file_out` count a batch's bytes after its write.** `StreamOutput::send`
    counts `logit.output.batch.bytes` and calls `FileTarget::note_written` once the write and
    flush succeeded, not before. A re-open after a rotation can fail `Fault::Clean`, which
    retries, so counting first counted the batch once per attempt and grew the size the rotation
    policy tracks, rotating the next file early. `logit.output.file.rotations` counts a rotation
    whose commit-point rename landed even when the re-open after it fails
    (`FileTarget::awaiting_reopen`). `StreamOutput` needs no gate: its encoders count nothing.

## Alternatives considered

- **A gate that defaults open and closes after the first encode.** Rejected. A caller that never
  calls `observe_batch` (a test, a tool, `send_batch`) would find it closed after its first
  `send` and count nothing for every later batch. Arming it in `observe_batch` and never muting an
  unarmed one keeps such a caller on today's behavior.
- **The gate in `ComponentBuffer`.** Rejected. The runtime and every ungated handle of the
  component share that buffer, so muting it would mute the transport counters and the runtime's
  own counts with the codec's.
- **Gating the sink's own handles too.** Not needed: the gate is muted only inside the encode
  closure, where a sink counts nothing, so a sink's transport counters would read the same through
  a gated handle. The sinks keep ungated handles so that holds without reasoning about timing.
- **A runtime-set attempt number on the component's `Telemetry` handle, with encode-side counts
  muted after attempt 1.** Rejected. It loses counts wherever encoding follows an await:
  `datadog_out` and `datadog_trace_out` encode per route, lazily, so a route first encoded on
  attempt 2 would be muted although nothing counted it on attempt 1. Its appeal, no per-sink
  state, doesn't survive that case.
- **An attempt parameter on `Output::send`.** Rejected. It changes every implementation and every
  call site, including tests that call `send` directly, for a fact only the sinks with
  encode-side counters need.
- **Memoizing the encoded bytes per batch.** Rejected. It moves the counting problem to a cache
  that must be invalidated per batch and held across a `Delivered` that may be shared. It also
  doesn't cover codec counters that are emitted as a side effect of encoding and never appear in
  the bytes.
- **Counting server-verdict drops once per batch.** Rejected. See decision 1: each attempt gets
  its own verdict, and a later attempt can get a different one.
- **A per-attempt clock in `datadog_out`, with the gated view alone covering counters.**
  Rejected. The stale verdict can flip between attempts, so a point can be counted dropped and
  still be sent.
- **Merging `logit_out` into the shared driver.** Rejected. See decision 5.
- **Treating a TLS write-phase `Err` on `logit_out` as `Ambiguous`, as the line sinks do.**
  Rejected. The peer can't forward a truncated frame, so the batch was never received, and
  `Ambiguous` would drop it under `at_most_once` for no reason.
- **Keeping `logit_out`'s single first `write` ahead of `write_all`.** It existed to tell "nothing
  left" (`Clean`) from "something left" (`Ambiguous`). Once every write-phase failure is `Clean`,
  it tells nothing apart, so the frame goes out through one `write_all` and a flush.
- **`logit_out` answering a bad `HelloAck` `Clean` or `Ambiguous`.** Rejected. Nothing of a batch
  was sent, so `Ambiguous` is wrong, and `Clean` retries to budget exhaustion on every batch
  against a peer that will answer the same way every time.
- **Flooring `backoff_for` at `base_delay`.** Rejected. See decision 10.
- **Scanning a batch for over-cap entries before any datagram is sent.** Rejected. No encoder can
  produce such an entry once `GraphiteOutput::with_encoder` refuses pickle on UDP, and dropping one
  would need a remove API `MessageBuf` doesn't have. A skip-and-count branch in the packing loop
  covers the invariant for one comparison per entry.
- **A destination trait for the packer.** Rejected for an enum: the destinations are a closed set
  of two plus a test double, as the stream driver's `Target` is, and an enum keeps the send
  future's type concrete.
- **Binding the UDP socket by the family of the first resolved address.** Rejected. Where
  `localhost` resolves to `::1` first and the receiver listens on `127.0.0.1` only, the send to
  `::1` succeeds and nothing receives it: a loud `Clean` failure on every batch becomes silent loss.
  Preferring IPv4 keeps every endpoint that works today working, and reaches IPv6 when the
  endpoint has no IPv4 address.
- **Keeping `is_message_too_large`'s `InvalidInput` fallback for platforms that report
  `EMSGSIZE` differently.** Rejected. `logit` builds and runs in Linux containers, and the fallback
  counted a misconfigured endpoint as oversize data on every batch.
- **Counting each datagram as it's sent, so a cancelled send keeps its counts.** Not taken: it
  costs a telemetry call per datagram for a loss already bounded to the attempt the runtime
  cancelled, which `logit.component.errors` records.

## Consequences

- One pooled-stream driver and one datagram packer replace three and two copies. A fix to a fault
  arm or to packing lands once, and each driver test covers three sinks.
- A sink with encode-side counters owns a `BatchAccounting`, arms it in `observe_batch`, and runs
  every encode through it. A new sink or codec that counts encode-side must count through the gated
  view, or skip its own counts on a repeat encode. A test that runs a sink through `write_loop`
  (`logit_pipeline::test_util::drive_write_loop`) with a first attempt that fails `Clean` must
  match the counters of a single-attempt run, and each sink gets one.
- `Telemetry` grows from one pointer to two words, and `Telemetry::count` on a gated handle pays one
  relaxed atomic load. No allocation pin moves.
- `logit.output.requests` gains the classes `clean`, `ambiguous`, and `permanent` on the line
  sinks and `collectd_out`, and loses `error`. A dashboard or alert on `class="error"` needs to
  change. This is a pre-release break with no alias.
- `logit_out` gains a `flush()` after two writes, and `logit_in` after each control write. Under
  TLS that closes a stall that surfaced as an `Ambiguous` timeout and a dropped batch.
- A `logit_out` write-phase failure that used to be `Ambiguous` (a `write_all` remainder) is now
  `Clean`, so under `at_most_once` the batch is retried where it was dropped.
- A `logit_out` whose peer answers a `HelloAck` it didn't ask for now fails `Permanent` where it
  used to connect (a bad compression byte) or fail `Ambiguous` (a codec it never offered).
- A TLS `logit_in` no longer logs `connection_error` for every `logit_out` disconnect between
  frames; a truncated header now counts `truncated_header`.
- `retry_budget: 0s` and `retry_max_delay: 0s` become validation errors. A config that sets
  either fails to load.
- `collectd_out` rejects `max_packet_bytes` above 65507 where it accepted up to 65535, and a UDP
  `statsd_out` or `graphite_out` rejects the same values. A Unix socket path of 108 bytes or more
  and a UDP sink endpoint on port 0 become validation errors; built past validation, both are
  faults on every batch where they were silent drops counted `oversize_datagram`.
- A UDP sink endpoint that resolves only to IPv6 addresses is reached, where it failed `Clean` on
  every batch. A name that resolves to both still goes to IPv4.
- A `syslog_out` message over 65507 bytes on UDP is truncated where the kernel refused it.
- `collectd_out`'s `logit.output.requests` loses `class="error"` for the four fault classes, and
  the UDP sinks count the messages and datagrams a failed attempt did send.
- Encode-side counts lose their inflation on retry, and the drop counters read while a sink is
  unhealthy stop growing by the attempt count. A server-verdict drop still grows with retries,
  and decision 1 says so.
- `datadog_out` can send a point up to `retry_budget` past its window, which decision 3 accepts.
- The pinned third-party facts cost a re-verification on each bump of tokio-rustls, rustls, or
  tokio.
- Left open for later workstreams: the HTTP sinks' `requests` vocabulary.

## Running it

Each workstream fills in its subsection in the PR that lands it, and updates its inventory rows.
No code from this record exists until a workstream lands it.

### `sink/w1`: pinned TLS semantics and one fake stream (SINK-03, WIRE-09)

`sink/w1` changes no production behavior. It adds tests and corrects prose.

- **Pinned facts.** `crates/logit-outputs/src/stream_pins.rs` pins decision 8's facts against
  tokio-rustls 0.26.5, rustls 0.23.45, and tokio 1.53.1. Each test names the source function it
  pins. The TLS tests run a real client and server over `tokio::io::duplex(4096)`, except
  `a_tls_write_error_can_follow_a_whole_record_reaching_the_peer`, whose 65536-byte pipe holds a
  whole record before its armed failure. Each pair is handshake complete, with a counting wrapper (`TapIo`) under each end. No test sleeps or reads a clock:
  "nothing more is available" is a one-poll read that answers `Pending` on an in-memory pipe,
  and each body runs under `tokio::task::unconstrained` so the cooperative budget can't produce
  that `Pending` on its own.
  - `a_tls_write_returns_ok_with_ciphertext_still_queued_in_the_session`: a 100 000-byte write
    returns `Ok(65536)`, rustls's buffer limit, with 4096 bytes on the pipe and `wants_write()`
    still set.
  - `a_tls_read_that_succeeds_leaves_queued_ciphertext_queued`: after the server drains the
    pipe, a client read that processes its records cleanly moves no ciphertext, and the server
    receives nothing more.
  - `a_tls_read_failing_on_a_bad_record_sends_queued_ciphertext`: a bad record written toward
    the client makes its read fail with `InvalidData`, and that read's last-gasp write puts queued
    ciphertext on the pipe.
  - `a_tls_flush_drives_every_queued_byte_to_the_peer`: a flush delivers every accepted byte.
  - `a_peer_close_notify_reads_as_an_empty_ready` and
    `a_peer_transport_close_without_close_notify_reads_as_unexpected_eof`: a `close_notify` reads
    as `Ok(0)`, and a transport close without one reads as `ErrorKind::UnexpectedEof`.
  - `one_poll_of_an_idle_tls_13_connection_after_the_handshake_is_pending`: the poll reads the
    server's session tickets into the session and answers `Pending`, not an empty `Ready(Ok)`.
  - `a_partial_tls_record_survives_a_dropped_read`: a read polled once over part of a record,
    then dropped, loses nothing, and the rest of the record completes it.
  - `a_tls_write_error_can_follow_a_whole_record_reaching_the_peer`: with the IO failing after
    20 000 bytes, a 40 000-byte write returns `Err`, and the peer decrypts the first 16 384 bytes.
  - `an_io_ok_zero_under_tokio_rustls_parks_a_full_session_write_with_no_waker`: an IO `Ok(0)`
    counts as would-block. The first write still returns `Ok(65536)`. Once the session is full,
    a write answers `Pending` and nothing holds its waker. A flush over the same IO fails with
    `WriteZero` and doesn't park.
  - `write_all_maps_an_ok_zero_write_to_write_zero`: tokio's `write_all` fails an `Ok(0)` write
    with `WriteZero`.
- **One fake stream.** `FakeStream` in `crates/logit-outputs/src/test_support.rs` replaces the two
  `FakeTlsStream` copies in `statsd.rs` and `syslog.rs`, and the eight tests that used them keep
  their names and assertions. It scripts short, `Ok(0)`, and failing writes by call number, a
  failing flush, and a read that is `Pending`, EOF, an error, or unsolicited bytes. It records
  what was written apart from what was flushed. Its doc says it never goes under tokio-rustls.
- **`tls.rs` tests.** `poll_pending_close`'s four arms over `FakeStream`
  (`a_pending_poll_is_open`, `an_empty_ready_is_eof`, `a_read_error_is_eof`,
  `unsolicited_bytes_are_counted_up_to_the_probe_buffer`), and three over a real TLS pair
  (`a_partial_tls_record_probes_open_and_is_still_readable`, `a_tls_close_notify_probes_eof`,
  `a_tls_transport_close_without_close_notify_probes_eof`). `host_only_takes_the_host_of_a_bare_endpoint`
  is a table over `host:port`, bracketed IPv6, a bare host, the empty string, and bare
  unbracketed IPv6. The last yields a truncated host, as the doc's "the brackets are the
  operator's to write" allows.
- **Prose.** `poll_pending_close`'s doc now says a `Pending` poll can move a partial record or
  whole post-handshake records into the session, that the kept session loses nothing, and that
  the probe is one poll because it must not wait. The `Ready(Err)` arm says why it reads as
  `Eof`. `send_tcp` in `statsd_out` and `syslog_out`, and ADRs `statsd-output` and
  `syslog-output`, no longer say each TLS record holds complete lines or messages: rustls splits
  what each session write accepted into records of at most 16384 bytes of plaintext, with no
  regard for line or message boundaries, and the complete lines or messages in what arrived are
  what a receiver keeps.

Run them with `script/test -p logit-outputs stream_pins tls::tests`. A bump of tokio-rustls,
rustls, or tokio re-runs `stream_pins` and re-reads the functions each test names.

### `sink/w2`: the pooled-stream driver (SINK-01, SINK-02)

`sink/w2` lands decision 5's driver in `crates/logit-outputs/src/stream.rs`, moves `statsd_out`,
`syslog_out`, and `graphite_out` onto it, and lands decision 4's counter classes and decision
10's server-name parsing for those sinks.

- **The driver.** `PooledStream::send(&Dial, frame, telemetry)` and `PooledStream::flush`, with a
  free `connect(&Dial)` that counts nothing, for `logit_out` to share in `sink/w3`. A caller
  builds its frame into its own buffer (statsd's LF lines or `unix_stream` length prefixes,
  syslog's octet counting, graphite's lines or pickle frames) and counts its own messages and
  datapoints. `stream.rs`'s module doc lists the fault rules. The three `send_tcp` copies, both
  `TcpDial` copies, and graphite's `connect` are gone. `transport: unix` keeps its own
  reconnect-once datagram path and its own `has_connected_once`.
- **What an operator sees change.**
  - `logit.output.requests` on the three sinks is `class=ok|clean|ambiguous|permanent` on every
    transport, one per attempt that returns. `class="error"` is gone. A failed connect or handshake
    counts `clean`; an empty batch counts nothing. `collectd_out` keeps `ok|error` until `sink/w4`.
  - `graphite_out` flushes before it calls a TCP batch delivered, counts
    `logit.output.reconnects`, and holds a `Box<dyn AsyncStream>`. It still has no `tls:` option.
  - `with_tls` on `statsd_out` and `syslog_out` parses the endpoint's server name once, into a
    `TlsTarget`. A host that is neither an IP literal nor a DNS name (an empty host, a scoped IPv6
    literal such as `[fe80::1%eth0]:514`) fails startup with an error naming the component and the
    endpoint. The send path no longer parses a name, so it has no `Permanent` fallback to take.
  - Dial errors name the sink and the endpoint or socket path.
- **Driver tests** (`stream::tests`), each asserting the `Fault`, what reached the peer, the pool
  afterwards, and the counters through `TelemetryProbe`:
  - the dial: `a_refused_dial_is_clean_leaves_the_pool_empty_and_counts_no_reconnect` (a redial
    after the probe finds the pooled connection closed),
    `a_stalled_tls_handshake_times_out_clean_within_twice_the_connect_timeout` (a peer that
    accepts TCP and never answers the ClientHello, the handshake timed out on a paused clock),
    and `a_tls_target_needs_a_server_name_in_the_endpoint_host`;
  - the probe: `a_reused_connection_that_probes_eof_is_redialed_and_the_retry_survives` and
    `a_reused_connection_that_probes_unsolicited_bytes_is_redialed_and_the_retry_survives`, where
    the redialed connection's first write fails and the one retry still delivers;
  - write faults: `a_plaintext_first_write_error_is_retried_once_then_clean`,
    `a_plaintext_first_write_of_zero_bytes_is_write_zero_retried_once_then_clean`,
    `a_remainder_failure_after_a_short_first_write_is_ambiguous_and_never_resent`,
    `a_flush_failure_after_a_complete_write_is_ambiguous_and_drops_the_connection`, and
    `a_real_reset_mid_frame_is_ambiguous` (a loopback RST inside `write_all` of a 32 MiB frame);
  - TLS over a real tokio-rustls pair: `a_tls_write_error_is_ambiguous_and_never_retried` and
    `a_tls_send_returns_only_once_the_peer_can_read_the_whole_frame` (a 100 000-byte frame over a
    4096-byte pipe);
  - cancellation: `a_send_dropped_inside_write_all_leaves_the_pool_empty_and_the_next_send_dials_fresh`,
    `a_send_dropped_in_the_redial_after_a_probe_leaves_the_pool_empty`, and
    `a_send_dropped_while_dialing_counts_nothing_and_the_next_send_dials_again`. The probe itself
    never suspends, so there is no await inside it to drop at;
  - bounds and accounting: `one_send_dials_at_most_the_probe_redial_and_one_retry` (a table over
    every probe answer, plaintext and TLS) and
    `reconnects_count_every_successful_dial_after_the_first`;
  - Unix streams, on real sockets:
    `unix_stream_redials_when_the_probe_finds_the_pooled_connection_closed` (the peer
    half-closes, the test waits until the pooled stream probes `Eof`, and the second frame
    arrives on a new connection with nothing more on the first, which only the probe's redial
    produces), `unix_stream_retries_a_first_write_the_peer_refused` (the peer's `SHUT_RD` makes
    the first write fail with `EPIPE`), and `unix_stream_dial_failures_are_clean`.

  On a current-thread runtime the probe sees a peer's close only once the I/O driver has run
  since it arrived. The real-socket tests of the probe path wait for it:
  `unix_stream_redials_when_the_probe_finds_the_pooled_connection_closed` on an observable, and
  the three sinks' `a_pooled_connection_the_peer_closed_is_reconnected_before_writing_and_the_message_is_not_lost`
  by awaiting the collector's message. Each fails when the driver ignores the probe's answer.

  `ScriptedDial` in `test_support.rs` hands the driver scripted fresh connections, including one
  that never completes, and counts dials.
- **Per-sink wiring tests.**
  `statsd::tests::the_stream_transports_report_their_counts_through_the_driver`
  (TCP and `unix_stream`), `syslog::tests::tcp_and_tls_report_their_counts_through_the_driver`,
  `graphite::tests::plaintext_and_pickle_over_tcp_report_their_own_counts`,
  `graphite::tests::tcp_counts_every_connect_after_the_first_as_a_reconnect`,
  `graphite::tests::a_tcp_batch_is_flushed_before_it_is_reported_delivered`,
  `with_tls_rejects_an_endpoint_with_no_valid_server_name` in `statsd` and `syslog`, and
  `logit-cli`'s `a_tls_statsd_output_endpoint_with_no_valid_server_name_fails_startup`. The tests
  that called `send_tcp` directly keep their names and now drive each sink's `send` with a
  `FakeStream` pooled behind the driver.

Run them with `script/test -p logit-outputs stream:: statsd:: syslog:: graphite::`.

### `sink/w3`: `logit_out` (WIRE-08)

`sink/w3` lands decisions 4, 6, 7, 11, and 12 for `logit_out` and `logit_in`. The frame-encode
failure after a connection is taken keeps no connection, as before: `write_frame_with_flags`
fails only on a payload over the sanity cap, which the bound check before it excludes, or on a
zstd frame, which `compression_from_u8` never yields, so the failure is unreachable.

- **Flushes.** `logit_out` writes each frame with one `write_all` and a flush, and
  `write_control` flushes the `Hello`. `logit_in`'s `write_control` flushes inside its bound.
  `LogitOutput::connect_and_handshake` is `dial` then `handshake(stream)`, so a test hands the
  handshake a stream it built.
- **Shared dial.** `dial` is `crate::stream::connect` with `request_timeout` as its
  `connect_timeout`: the TCP connect and the TLS handshake are each bounded by it, as before, and
  every failure is `Clean`. `with_tls` builds a `TlsTarget`, so an endpoint with no valid server
  name fails startup (decision 10), and `send` counts `requests` through `count_request` (at crate
  level since `sink/w4`).
- **Faults.** Every write-phase failure is `Clean`, with the `io::Error` in the chain. A
  `HelloAck` that doesn't answer the `Hello` is `Permanent`, counted as no connection.
- **`requests`.** `Output::send` wraps one inner attempt and counts `logit.output.requests`
  once, from `classify` of its result. Connect, handshake, and too-large returns now count.
- **Close.** `Output::flush` shuts the pooled connection down; `logit_in`'s header read takes
  `UnexpectedEof` at a frame boundary as a close and counts a truncated header.
- **Smaller.** `read_control` bounds a control frame at `control::MAX_CONTROL_MESSAGE_BYTES`
  (4096; the largest message this version writes is a 1033-byte `Reject`), not the 64 MiB data
  cap, and `logit_in` reads a `Hello` against the same cap. An over-cap `Hello` header closes the
  connection before any body is read, counted `logit.proto.errors{reason="handshake"}` like any
  other bad `Hello`. A control frame is never compressed, so both sides cap its compressed length
  at the same 4096, not lz4's worst case over it. `LogitOutput::observe_batch`'s doc says once per batch.

The tests, in `crates/logit-outputs/src/logit.rs` unless named otherwise:

- Flushes over a real tokio-rustls pair on `tokio::io::duplex`:
  `a_tls_frame_larger_than_the_socket_buffer_is_flushed_before_the_ack_wait` (a 32 KiB frame
  over a 4 KiB pipe), `the_hello_is_flushed_before_the_hello_ack_wait` (a 16-byte pipe), and in
  `logit_in`, `hello_ack_and_ack_reach_a_tls_client_over_a_pipe_smaller_than_one_record` and
  `a_reject_reaches_a_tls_client_over_a_pipe_smaller_than_one_record`. Each timed out before the
  fix.
- Write-phase faults: `a_write_that_fails_part_way_through_the_frame_is_clean_and_keeps_the_io_error`,
  `a_first_write_of_zero_bytes_is_clean_and_keeps_the_io_error`, and
  `a_failed_flush_is_clean_and_drops_the_connection` over `FakeStream`; and
  `a_tls_write_error_after_a_whole_record_left_is_clean_and_logit_in_forwards_nothing`, where
  20 000 bytes of ciphertext reach a real TLS `logit_in` before the socket fails, and the
  listener counts `truncated` and forwards nothing.
- `HelloAck` validation: `a_hello_ack_naming_a_codec_never_offered_is_permanent`,
  `..._an_unknown_compression_...`, `..._a_compression_never_offered_...`, and
  `a_hello_ack_with_another_protocol_version_is_permanent`, each followed by a good handshake
  that counts no reconnect.
- `requests`: `a_refused_connect_counts_one_clean_request`,
  `a_handshake_reject_counts_one_request_of_its_class`,
  `each_too_large_return_counts_one_permanent_request_and_keeps_the_connection`, and
  `every_returned_send_counts_one_request` (five sends, one of each outcome and a second `ok`).
  The compressed-frame too-large return has no test: `frame::compressed_bound` is lz4's worst
  case over a payload the check before it already bounded, so lz4 can't exceed it.
- Close: `flush_sends_close_notify_on_the_pooled_tls_connection`,
  `logit_in_reads_a_tls_connection_ended_after_an_ack_as_a_clean_close` (flushed, and dropped
  without `close_notify`), and in `logit_in`,
  `a_tls_client_gone_without_close_notify_between_frames_is_a_clean_close` and
  `a_client_gone_mid_header_is_an_error_counted_as_a_truncated_header`.
- TLS twins of the probe tests (WIRE-09):
  `a_pooled_tls_connection_the_peer_closed_is_replaced_before_the_next_write_with_no_batch_lost`
  and `a_pooled_tls_connection_with_an_unsolicited_reject_is_replaced`; and
  `a_tls_peer_gone_between_the_frame_and_its_ack_is_ambiguous_and_the_next_send_reconnects`.
- The control cap: `read_control_accepts_a_message_at_the_control_message_cap_and_refuses_one_over`,
  and `the_largest_message_of_each_type_fits_the_control_message_cap` in
  `crates/logit-proto/src/native/control.rs`, and in `logit_in`,
  `a_hello_is_bounded_by_the_control_message_cap` (a `Hello` at the cap is answered, and one
  over it is closed on its header; before the fix, the connection waited for the body).
  `a_hello_whose_compressed_length_is_over_the_control_message_cap_is_refused` pins the
  compressed-length half.
- The shared dial: `with_tls_rejects_an_endpoint_with_no_valid_server_name`, and
  `a_tls_logit_output_endpoint_with_no_valid_server_name_fails_startup` in
  `crates/logit-cli/src/pipeline.rs`.

Run them with `script/test -p logit-outputs -p logit-inputs -p logit-proto logit:: control::`.

### `sink/w4`: the datagram packer (SINK-04)

`sink/w4` lands decision 9 and completes decision 4 for `collectd_out`.

- **The module.** `crates/logit-outputs/src/datagram.rs` holds `send_datagrams`,
  `is_message_too_large`, `pick_addr`, `UdpDest` (the eager IPv4 socket and the lazy IPv6 one),
  `DatagramDest`, and `UnixDest`, moved out of `statsd.rs`. `statsd_out` (UDP and Unix datagram)
  and `graphite_out` pack through it; `syslog_out` and `collectd_out` send one datagram per entry
  through it. Both old packers, both `flush_datagram`s, the four `is_message_too_large` copies,
  `syslog_out`'s `udp_send_fault`, and `collectd_out`'s own send loop are gone.
  `ScriptedDest` in `crates/logit-outputs/src/test_support.rs` scripts each send (accept,
  `EMSGSIZE`, an error of a given kind, or never completing) and records each accepted datagram
  with its entry count.
- **The two old packers differed only in what they counted and where they sent.** Same greedy
  test (`len + 1 + entry > cap` on a non-empty buffer), same separator guard, same
  `Clean`-until-the-first-sent-datagram rule, same resets on every exit. `graphite_out` summed
  datapoints and counted an `EMSGSIZE` drop in datapoints; `statsd_out` counted entries. `statsd_out`
  sent through `DatagramDest` (UDP or Unix) with a first-of-batch flag, `graphite_out` straight to a
  `UdpSocket`, so the Unix reconnect rule was statsd's alone. The diagnostic text named each sink.
  The shared packer keeps the arithmetic, takes the unit as the weight function, takes the sink
  name for its diagnostics, and gives both sinks the destination enum. Neither old packer checked
  an entry against the cap, and both discarded their counts on an error.
- **What an operator sees change.**
  - New validation errors: a `statsd_in`/`statsd_out` Unix socket path of 108 bytes or more (rule
    65), a UDP sink endpoint on port 0 (rule 73), and `max_packet_bytes` above 65507 on a UDP
    `statsd_out`/`graphite_out` or on `collectd_out` (rule 38).
  - An endpoint that resolves only to IPv6 addresses works on all four UDP sinks.
  - A `syslog_out` message longer than 65507 bytes on UDP is truncated to fit a datagram.
  - `collectd_out` counts `logit.output.requests{class="ok"|"clean"|"ambiguous"|"permanent"}`.
  - A failed UDP attempt counts the messages, datagrams, and (graphite) datapoints it did send.
- **Tests**, each shown to fail on a planted bug:
  - `datagram::tests`: `only_emsgsize_is_a_message_too_large`,
    `pick_addr_takes_the_first_ipv4_address_else_the_first_ipv6_one`,
    `resolution_selects_the_socket_of_the_chosen_address_family` (never skipped: it binds the IPv6
    slot, or fails `Clean` naming it on a host with no IPv6),
    `emsgsize_then_a_failure_with_nothing_sent_is_clean`,
    `sent_then_emsgsize_then_a_failure_is_ambiguous`,
    `a_failure_after_two_datagrams_returns_what_the_two_carried`, `one_per_entry_never_packs`,
    `an_entry_over_the_cap_is_dropped_and_counted_and_its_neighbours_are_sent`,
    `the_reconnect_once_rule_survives_an_emsgsize_dropped_first_datagram`,
    `a_timed_out_unix_send_drops_the_socket_and_the_next_send_reconnects`,
    `a_unix_send_parked_past_send_timeout_times_out_and_drops_the_socket` (paused clock),
    `a_send_dropped_mid_batch_leaves_a_clean_start_and_a_usable_unix_socket`, and the proptest
    `packing_never_exceeds_the_cap_never_splits_an_entry_and_reconciles`: every datagram at most the
    cap, non-empty, with no leading or trailing `\n`; the datagrams joined by `\n` equal the
    entries joined by `\n`; each datagram is the run of whole entries the packer reports it holds,
    and the next entry wouldn't have fit; entries, weight, and datagrams reconcile. Entries hold
    embedded `\n`s, so splitting a datagram on `\n` isn't used to recover them. The cap is drawn
    apart from the entries, so some cases hold entries over it: each is absent from every
    datagram and counted `oversize_datagram` in the weight unit, and the rest satisfy the above.
  - A real kernel `EMSGSIZE` through each sink's builders, with exact drop counts in the sink's
    unit, `send` returning `Ok`, `requests{class="ok"}`, sent plus dropped equal to what was
    encoded, and the datagrams around the refused one arriving:
    `statsd::tests::a_real_udp_emsgsize_drops_one_datagram_and_the_ones_around_it_arrive`,
    `statsd::tests::a_real_unix_emsgsize_drops_one_datagram_and_the_ones_around_it_arrive` (sized
    from `/proc/sys/net/core/wmem_default`),
    `graphite::tests::a_real_emsgsize_drops_one_datagram_and_the_ones_around_it_arrive`, and
    `collectd::tests::a_real_emsgsize_drops_one_datagram_and_the_one_after_it_arrives`. Over IPv4
    `syslog_out`'s encoder cap keeps every message inside a datagram, so its `EMSGSIZE` test is
    scripted: `syslog::tests::an_emsgsize_message_is_dropped_and_counted_and_the_rest_are_sent`.
  - Each sink's `a_udp_endpoint_with_port_zero_fails_clean_and_counts_no_oversize` (the kernel's
    `EINVAL`, asserted by errno), `an_ipv6_udp_endpoint_is_delivered` (a collector on `[::1]:0`,
    skipped with a printed reason where IPv6 loopback is unavailable), and its partial-send test
    (`a_udp_failure_after_two_datagrams_counts_what_reached_the_wire` in statsd and graphite,
    `a_udp_failure_after_two_messages_counts_what_reached_the_wire` in syslog,
    `a_failure_after_two_datagrams_counts_what_reached_the_wire` in collectd); and
    `statsd::tests::a_unix_socket_path_too_long_for_sockaddr_un_fails_clean_and_counts_no_oversize`,
    `syslog::tests::a_message_longer_than_a_udp_datagram_is_truncated_to_fit_one` (a real 70 000
    byte message), and `graphite::tests::with_encoder_refuses_pickle_on_udp`.
  - `logit-pipeline`'s graph tests: `a_udp_max_packet_bytes_above_the_udp_payload_ceiling_is_rejected`,
    `the_udp_payload_ceiling_binds_only_the_udp_transports`,
    `a_max_packet_bytes_above_the_udp_payload_ceiling_is_rejected_for_collectd_out`,
    `a_unix_socket_path_too_long_for_sockaddr_un_is_rejected_on_both_statsd_kinds`,
    `a_udp_sink_endpoint_with_port_zero_is_rejected`, and
    `a_nonzero_port_and_the_stream_transports_pass_rule_73`.

Run them with `script/test -p logit-outputs -p logit-pipeline datagram:: statsd:: syslog::
graphite:: collectd:: graph::`.

### `sink/w5`: attempt accounting and backoff (SINK-05, SINK-06, RT-05)

`sink/w5` lands decisions 1 and 2 for `statsd_out`, `syslog_out`, `graphite_out`,
`collectd_out`, and `influxdb_out`, decision 13 for `stdio_out` and `file_out`, and decision 10's
graph rule. The multi-request HTTP sinks wait for `sink/w6`.

- **The gate.** `CountGate`, `Telemetry::gated`/`is_muted`, and `Diagnostics::gated` in
  `logit-core`; `BatchAccounting` in `crates/logit-outputs/src/accounting.rs`. The five sinks
  override `observe_batch`, run their encode through `BatchAccounting::encode`, hand their encoder
  gated views in every builder order, and keep ungated handles. `null_out`, `logit_out`, and
  `prometheus_out` count nothing encode-side that a retry repeats, and have no gate.
- **The packer.** `datagram::Report::count_local_drops` carries the batch's first-encode flag, so
  the over-cap skip counts once per batch while `EMSGSIZE` counts per attempt. Both keep the reason
  `oversize_datagram`.
- **`StreamOutput`.** Decision 13.
- **The runtime.** Rule 15 rejects the two zero durations; `deliver_with_retry` asserts them
  nonzero and saturates its attempt count. `logit_pipeline::test_util::drive_write_loop` runs the
  real `write_loop`, with its `observe_batch` call site, over a sink and a Registry-backed handle.
- **Docs.** The `Output` trait: `observe_batch` once per batch, `send`'s contract (one attempt,
  cancellable at every await, a synchronous encode, a `Fault` on failure), the bounded
  verdict-driven resend inside one attempt, and `flush` on a grace expiry. Every `duplicate_safe`
  doc names the `buffer.delivery` override.
- **Tests**, each shown to fail on a planted bug:
  - `accounting::tests`: an unarmed gate never mutes, a repeat of a unit is muted, `observe`
    resets, `delivered` disarms, and units are independent. `telemetry::tests` and `diag::tests`:
    a gated handle shares the buffer and the throttle, a gate mutes only the handles built over it,
    a disabled handle can be gated, and a muted `warn_throttled` leaves no trace in the throttle.
  - Per sink and transport, a batch with encode-side drops, normalizations, and a diagnostic,
    delivered on its second attempt through `drive_write_loop`, against a single-attempt run of
    the same batch: every series but `requests`, `reconnects`, `component.errors`, and
    `component.retries` must match, and the transport counters show both attempts.
    `statsd::tests::a_{udp,unix_datagram,tcp,unix_stream}_retry_counts_encode_side_counters_once`,
    `syslog::tests::a_{udp,tcp}_retry_...`, `graphite::tests::a_{udp,tcp}_retry_...` and
    `an_encoder_installed_after_the_handles_counts_encode_side_once_too`,
    `collectd::tests::a_retry_counts_encode_side_counters_once` (the stream sinks' first attempt
    is a refused `ScriptedDial`, the datagram sinks' a failing `ScriptedDest`), and
    `influxdb::tests::a_retry_counts_encode_side_counters_once` (a `503`, then a `204`). With the
    gate never armed, every one reads each encode-side counter and diagnostic twice.
  - `statsd::tests`: a second batch through the loop counts again, a batch after one the budget
    cut off counts, and direct `send`s with no `observe_batch` count every time.
  - `stdio::tests::a_rotation_whose_reopen_fails_counts_once_and_the_retries_count_no_bytes_twice`,
    over the `fault` seam.
  - `datagram::tests::a_repeat_encode_skips_an_over_cap_entry_uncounted_and_counts_an_emsgsize`.
  - `runtime::tests`: `backoff_for_doubles_from_base_and_is_capped_at_max_for_every_attempt`,
    `every_attempt_records_one_send_duration_sample_and_every_retry_one_error`, and
    `an_attempt_cut_off_by_the_budget_is_ambiguous`; `graph::tests`'s two zero-duration rejects.

Run them with `script/test -p logit-core -p logit-outputs -p logit-pipeline accounting:: telemetry::
diag:: retry_counts stdio:: datagram:: runtime::tests graph::tests::a_sinks_buffer`.

### `sink/w6`: the multi-request HTTP sinks

`sink/w6` lands decisions 2 and 3 for `otlp_out`, `prometheus_out`'s remote-write mode,
`datadog_out`, `datadog_trace_out`, and `splunk_hec_out`: per-route units and `split_encode`'s
bisection for the two Datadog sinks, and `datadog_out`'s per-batch `now`.

### `sink/w7`: close-out

`sink/w7` adds the "Cancellation points" rows in `docs/design/pipeline-graph.md` for a dropped
`write_all` on the pooled sinks and the datagram loop, updates `docs/known-gaps.md`,
`docs/design/internal-telemetry.md`, and `docs/deploying.md`, and closes the inventory rows.
