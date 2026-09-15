---
created: 2026-09-14
updated: 2026-09-14
---

# Idle-connection timeouts on TCP listeners: an opt-in `idle_timeout`, a next-byte deadline, and a client-side pooled-connection probe

## Status
Accepted

## Context

Every TCP listener in the tree bounds its *pre*-message phases and nothing past them.
`handshake_timeout:` (`crates/logit-config/src/lib.rs`) is operator-tunable on all five TCP-capable
listener kinds -- `syslog_in`, `graphite_in`, `statsd_in` (each `transport: tcp`), `logit_in`, and
`otlp_in` -- and bounds each pre-message phase separately: the TLS accept when `tls:` is set, then
the wait for the connection's first byte (a `Hello` for `logit_in`, the framer's first byte for
`syslog_in`/`graphite_in`/`statsd_in`, a peeked first byte for `otlp_in`). What none of the five
bounds is what happens *after* that: a connection that completes its
handshake (or, on a plaintext listener, delivers at least one byte) and then goes silent holds its
connection-cap permit -- 1024 on every one of the five -- indefinitely, right up to the cap itself. A
slow-loris-shaped client can exhaust that cap with connections that will never send another byte.
`docs/known-gaps.md:1291-1332` names this gap and records that it is deliberately not a second use
of `handshake_timeout:` -- that field landed on 2026-09-13 with this held out explicitly, because an
idle bound is a different shape from a pre-message one and raises three design questions a knob
can't answer on its own:

1. **A stalled downstream must not look like a silent peer.** A connection task blocked handing a
   batch to `Fanout::send` (`crates/logit-pipeline/src/fanout.rs:311-332`) stops reading its socket
   for exactly the reason TCP backpressure is supposed to work: the peer feels its own write block.
   A timer that can't tell "the peer sent nothing" from "we haven't read yet" would turn every
   downstream stall into a wave of dropped connections and lost data -- the opposite of the
   no-receive-queue design [ADR `syslog-tcp-ingress-and-tls`](syslog-tcp-ingress-and-tls.md) and
   [ADR `native-transport-handshake-and-ack`](native-transport-handshake-and-ack.md) both rest on.
2. **`logit_in`'s per-batch ack makes "idle" a protocol state, not just a read gap.** A `logit_out`
   peer legitimately waits for an `Ack` before sending its next frame
   ([ADR `native-transport-handshake-and-ack`](native-transport-handshake-and-ack.md)), and that ack
   is deliberately delayed by a slow downstream. Whether "idle" should be measured from the last
   frame read, the last ack written, or something else is a wire-protocol question this ADR has to
   settle, not a detail a generic timer can guess at.
3. **`otlp_in`'s read loop belongs to `hyper`, not to a loop in this codebase.** Its connections are
   driven by `hyper_util::server::conn::auto::Builder`
   ([ADR `otlp-tls-and-pooled-grpc-client`](otlp-tls-and-pooled-grpc-client.md)), so whatever "idle"
   means there has to work with hyper's own state machine rather than a socket-level timer this
   listener would have to invent around it.

This decision answers all three, adds one opt-in `idle_timeout` field to all five listener kinds,
and adds the client-side complement so a server-side idle close costs a sender as little as
possible. That complement matters because a server-initiated close is not free for every peer: a
sink holding a pooled connection can write into a socket the peer has already half-closed.
`logit_out` (`crates/logit-outputs/src/logit.rs:340-507`) pools one TCP connection per remote and
reuses it across batches; if the peer's FIN arrives before `logit_out`'s next write, that write
still leaves the host, the ack read then fails, and the outcome is classified `Fault::Ambiguous` --
`duplicate_safe()` is false for the native protocol, so the default at-most-once delivery posture
drops the batch rather than risk a duplicate (`crates/logit-pipeline/src/runtime.rs:4468-4478`).
Plaintext `syslog_out`/`statsd_out`/`graphite_out` senders have it worse: nothing in those wire
protocols tells the sender its peer closed, so a write into a FIN'd socket is silently lost with no
ambiguity classification to even name the loss. An idle-timeout feature that only closes the server
side and does nothing about this would make every one of those pools measurably lossier the moment
an operator turns it on.

## Decision

### One optional field, absent by default, on all five listener kinds

`SyslogIn`, `GraphiteIn`, `StatsdIn`, `LogitIn`, and `OtlpIn` each gain
`idle_timeout: Option<Duration>` (`crates/logit-config/src/lib.rs`, the same
`humantime_serde_duration::option` + `#[schemars(with = "Option<String>")]` shape `handshake_timeout`
already uses). Absent means today's behaviour, unchanged: no idle bound, a quiet connection is never
closed for silence alone. A new graph rule (53) enforces the field's only two constraints, sharing
one rule body across all five match arms rather than five near-duplicate checks: `Some(Duration::ZERO)`
is rejected with "omit the field to disable the idle timeout" (rule 45 also rejects zero, but
`handshake_timeout` has no off state, so the wording is new here), and a `Some(_)` value under
`transport: udp` on `syslog_in`, `graphite_in`, or `statsd_in` is rejected too, because a UDP
listener has no connection to time out.

### The reset rule: the clock runs only while the listener is waiting on the socket

One semantic covers every listener. The idle clock is armed the moment a connection has finished
whatever work it was doing and starts waiting on the peer again, and it is reset by two things only:
bytes actually read from the peer, and the listener's own work on the connection returning -- an
`emit`/`Fanout::send` completing, an `Ack` written, a response completed. Time spent blocked inside
`Fanout::send` itself never counts against the clock, because the clock has not yet been re-armed
when that block starts; it only starts running again once that send returns. This is the direct
answer to question 1 above: "the peer is silent" and "we are still busy with the last thing it sent"
are now structurally different states, not two readings of the same missing byte.

### An idle close is policy, not a fault

Closing an idle connection is an ordinary, expected outcome, never routed through the
`connection_error` diagnostic or counted as one. All five listeners count a new
`logit.input.connections.closed{reason="idle"}` and return `Ok(())` from the connection task, the
same success path a graceful shutdown takes; the connection-cap permit is released the same way it
always is, by the task ending. Whatever the connection had already buffered is not silently
discarded: any complete, accumulated batch is flushed with `FlushReason::Closed`
(`crates/logit-pipeline/src/accumulator.rs:26-34`, which gains "or closed as idle" to its doc), and
a partial frame still sitting in the framer is reported through `report_buffered_tail`, counted
`truncated` -- exactly the accounting a `Failed` or `Shutdown` close already gets, so an idle close
introduces no new kind of silent loss.

### The shared driver gets one next-byte deadline, not two timers

`crates/logit-inputs/src/tcp.rs`'s `serve_connection` (the driver behind `syslog_in`, `graphite_in`,
and `statsd_in`) already tracks a `first_byte_deadline` for `handshake_timeout`. Rather than run a
second, independent idle timer alongside it, the driver computes one `next_byte_deadline`: while the
connection hasn't seen its first byte yet, that deadline is `first_byte_deadline`
(`accept + handshake_timeout`) exactly as today; once a byte has been seen, it becomes
`last_progress + idle_timeout` when `idle_timeout` is set, or a deadline that never fires (via
`Instant::checked_add`'s `far_future()` fallback) when it isn't. `last_progress` is updated on the
same two events the reset rule above names: after bytes are read, and after the inner frame loop's
own `emit`/`Fanout::send` returns. A periodic flush tick that finds nothing to emit touches neither
event, so `a_flush_tick_does_not_reset_the_idle_clock` is the pin against that. One deadline, computed
fresh each iteration, means the elapsed branch only has to ask which case it is (still waiting on the
first byte, or idle past an established connection) rather than reconcile two independently-armed
timers.

### `logit_in`: idle measured from the last `Ack` written

This is the answer to question 2. A `logit_out` peer waiting on a delayed ack is, by definition, not
idle -- the listener itself is the one doing the work that delays it, per
[ADR `native-transport-handshake-and-ack`](native-transport-handshake-and-ack.md)'s own ack-as-
backpressure design. So `logit_in`'s idle clock resets on the handshake completing and on every `Ack`
write, not merely on bytes read; a peer that has sent a frame and is patiently waiting for its ack
while a slow downstream drains is never closed as idle no matter how long that ack takes. What *is*
bounded is the read itself: `read_frame_body`'s fill loop gets a per-`read` stall bound (not a total
one, since a large, slowly-arriving frame that keeps making incremental progress is not idle either).
A frame header whose first byte has already arrived is progress, not silence: the absolute idle
deadline bounds only the wait for that first byte, and the rest of the header -- like the body -- is
read under the per-`read` stall bound, so a frame that starts arriving right at the deadline is read
and acked rather than rejected after the peer has already written it. Either an idle gap between
frames or a stalled body read ends the same way: a `Reject{GOING_AWAY,
"idle for <dur>"}` control message, written before the connection closes -- the same signal
`logit_in` already sends on ordinary shutdown, so a `logit_out` peer needs no new case to handle it.

### `otlp_in`: a service-level in-flight tracker, not an IO wrapper

This is the answer to question 3. `otlp_in`'s connections are driven by hyper's own H1/H2 server
loop, and hyper 1.11.1's H1 implementation polls the underlying socket for a read *mid-message* --
`mid_message_detect_eof`'s `force_io_read` path -- specifically so it can notice a peer closing while
a handler is still working. An IO-level idle timer wrapped around that socket would see those polls
as "activity" and tick down anyway during ordinary backpressure, the same failure mode question 1
warns about, just relocated into hyper's internals instead of this codebase's. So idle tracking for
`otlp_in` lives one layer up, at the service: an `Activity` handle (an in-flight request counter plus
a last-progress instant) that `service_fn` updates as requests start and finish, independent of
anything hyper's own read loop is doing underneath.

When that tracker's deadline fires with no request in flight, the connection is closed by asking
hyper to close it, not by dropping the socket out from under it: `graceful_shutdown()` is called,
the connection is polled for up to `handshake_timeout` (reused as the grace period -- no new knob).
If that grace elapses with nothing in flight, the connection is dropped regardless of what the poll
returned; if a request arrived inside the grace and is now being served -- on HTTP/1, inside the
connection future itself -- the connection is kept until that request completes rather than dropped
out from under it, since dropping it would discard a batch already blocked in `Fanout::send`. A
stalled body is still bounded by the per-frame stall timeout below, so this wait can never be held
open by a silent peer. This two-step shape is deliberate and was verified against the pinned
`hyper 1.11.1`/`hyper-util 0.1.20` sources, not assumed:
`graceful_shutdown` closes an idle keep-alive H1 connection promptly (`disable_keep_alive` calls
`state.close()` immediately when the connection's `KA` state is `Idle`) and sends a GOAWAY on an H2
connection -- both the common case for a connection this tracker considers idle. But a *fresh* H1
connection stopped mid-head has state `KA::Busy`, not `Idle`, and keeps waiting regardless;
hyper-util's own pre-sniff `ReadVersion` future resolves to `Err("Cancelled")` on a graceful shutdown
signal, and an H2 connection still mid-handshake only sets an internal `close_pending` flag rather
than closing outright. The bounded grace-then-drop step exists precisely for those three cases,
where `graceful_shutdown` alone would leave the connection parked. This is also the narrowing
`docs/known-gaps.md:1333-1359`'s residual row already anticipated: `otlp_in` resets its idle clock on
request *completion*, not on individual bytes, so a request head that dribbles in more slowly than
`idle_timeout` on an otherwise-idle keep-alive connection is still closed -- a documented cost, not a
bug. A body that stalls mid-request gets a narrower, per-frame bound instead (`collect_with_stall_bound`
over `BodyExt::frame`) and ends in a 408 (HTTP) or gRPC status 4, with the connection closed after
the handler returns, rather than waiting for the whole-connection idle deadline.

### The client-side probe: one non-cancellable `poll_read` before the first write

Every pooled TCP sink -- `logit_out`, `syslog_out`, `statsd_out`, `graphite_out` -- reuses a
connection across batches, which is exactly the shape that turns a server-side idle close into a
silent loss if the client writes into a socket its peer already closed. Before the first write of
each send attempt on a *reused* pooled stream, the sink polls it once: a single `poll_read` via
`std::future::poll_fn`, never a cancellable `tokio::time::timeout(read)`, because a timeout on a real
read could cancel mid-TLS-record and discard bytes that had already arrived. `Poll::Pending` (nothing
to read yet) means the connection is still open and the write proceeds normally. An immediate EOF, or
unsolicited bytes (the only thing a peer would ever send unprompted is `logit_in`'s own
`Reject{GOING_AWAY}`), means the pooled connection is dropped and a fresh one is opened before
anything is written -- the existing `Clean`/reconnect path, since nothing has left the host yet. This
closes the common case for free: a peer that idle-timed-out and closed cleanly some time ago is
caught before the write that would otherwise race its FIN. The residual case is exactly the FIN
racing the probe itself, at the point the peer closes *while* this sink is writing -- that is today's
`Fault::Ambiguous`, unchanged and still documented as such.

### Default and recommendation

**Default: opt-in.** A server-side close has an inherent loss window on any sender that cannot detect
it -- plaintext `syslog_in`/`graphite_in`/`statsd_in` senders write into a FIN'd socket with no error
at all -- and, even with the probe above, an unlucky interleaving can still cost `logit_out` one
`Ambiguous` batch. Enabling the feature by default would introduce that cost on every deployment
whether or not it has a connection-exhaustion problem to solve, which is the wrong trade to make
unilaterally.

**But operators should enable it wherever consistent traffic is expected.** On a listener receiving
steady traffic, a connection quiet for longer than the timeout is by definition an anomaly -- a dead
peer, a half-open socket, or a slow-loris attempt -- so closing it costs nothing real and returns the
permit. The value should be set comfortably above the sender's longest normal gap (several flush
intervals, for instance), so the timeout never fires against legitimate traffic. Leave it unset only
for genuinely sparse or bursty senders where a long quiet period is expected and normal, and think
twice about enabling it at all on plaintext `syslog_in`/`graphite_in`/`statsd_in`, where the sender
has no way to learn its connection was closed. This recommendation is recorded here in full and
carried verbatim into `docs/deploying.md`'s operator-facing section and each field's config doc
comment.

## Alternatives considered

- **hyper's `header_read_timeout` + `TokioTimer` for `otlp_in`.** Verified against the pinned hyper
  1.11.1 source (`src/proto/h1/conn.rs`): the timer arms at the top of `poll_read_head`, before any
  header byte is parsed, and `State::idle` re-arms it on every idle keep-alive gap -- it is an idle
  timeout wearing a first-head name, and `docs/known-gaps.md`'s residual row already named this same
  finding when rejecting it as the fix for the narrower first-byte gap. It also only covers H1 (H2
  has no equivalent knob), and bounds nothing about a request body once headers are read.
- **An IO-level wrapper around the socket, ticking a timer on every poll.** Rejected for `otlp_in`
  specifically: hyper's H1 server polls the raw socket read mid-message (`mid_message_detect_eof`'s
  `force_io_read`) to notice a peer closing while a handler runs, so a wrapper timer at that layer
  would tick during ordinary backpressure and misread it as idleness -- the same failure this ADR's
  service-level tracker was built to avoid.
- **An idle timer that ignores backpressure and fires on elapsed wall-clock time regardless of what
  the listener is doing.** Rejected outright: it would turn a downstream stall into dropped
  connections and lost data, exactly the outcome the no-receive-queue design
  ([ADR `syslog-tcp-ingress-and-tls`](syslog-tcp-ingress-and-tls.md)) exists to prevent.
- **A second use of `handshake_timeout`.** Rejected, and already rejected once before this ADR
  existed: `docs/known-gaps.md`'s idle row records that decision from 2026-09-13, when
  `handshake_timeout` landed. A pre-message bound and an idle bound are different shapes -- one
  covers "never sent anything," the other "stopped sending" -- and conflating them into one knob
  would make either semantics wrong for the other case.
- **A wire-level ping/keepalive frame in the native `logit` protocol.** Rejected: it would need a
  protocol version or capability bump to solve a problem the read-side timeout plus the client-side
  probe and `GOING_AWAY` signal already solve without touching the wire format at all.
- **On by default.** Rejected for the loss reasons the recommendation above spells out: plaintext
  senders that cannot detect a server close, and the residual `Ambiguous`-batch cost on `logit_out`,
  are both real costs that should be an operator's deliberate choice, not a default applied to every
  deployment regardless of its traffic shape.

## Consequences

- **W1** (`syslog_in`, `graphite_in`, `statsd_in`): `idle_timeout` on `SyslogIn`/`GraphiteIn`/
  `StatsdIn` (`crates/logit-config/src/lib.rs`), rule 53's three match arms and its full five-kind
  module-doc entry in `crates/logit-pipeline/src/graph.rs` (written once, in final form, even though
  two more arms land later, so the module doc never has to be revisited for this rule again), the
  next-byte-deadline change to `crates/logit-inputs/src/tcp.rs::serve_connection` and its module doc,
  the `FlushReason::Closed` doc update in `crates/logit-pipeline/src/accumulator.rs`, `with_idle_timeout`
  wiring through `syslog.rs`/`statsd.rs`/`graphite/mod.rs` and `crates/logit-cli/src/pipeline.rs`,
  `docs/design/pipeline-graph.md`'s rule list, and a regenerated `schema/logit.schema.json`. Each
  landed PR's own rule-53 arms are the only ones honoured at that point -- no landed state ever
  accepts a set-but-ignored `idle_timeout` on a kind whose arm hasn't shipped yet, since the field
  and its arm land in the same PR.
- **W2** (`logit_in`, and the client-side probe on all four pooled sinks): `idle_timeout` on
  `LogitIn`, rule 53's fourth arm, the ack-driven idle clock and per-read body stall bound in
  `crates/logit-inputs/src/logit.rs` (including its `going_away` helper and module doc), and the
  `poll_pending_close` probe added to `crates/logit-outputs/src/tls.rs` and called from
  `crates/logit-outputs/src/{logit,syslog,statsd,graphite}.rs` before each reused pooled connection's
  first write.
- **W3** (`otlp_in`): `idle_timeout` on `OtlpIn`, rule 53's fifth and final arm, and the `Activity`
  tracker, `drive_with_idle` driver, and `collect_with_stall_bound` body-read change in
  `crates/logit-inputs/src/otlp.rs`, replacing that file's current idle-related module-doc section
  with the service-level-tracker rationale and the hyper evidence above.
- **W4** (docs closeout): a new `docs/deploying.md` section covering the semantics, the reset rule,
  and the enable-it-wherever-consistent-traffic-is-expected recommendation verbatim; both
  `docs/known-gaps.md` rows this ADR answers (`:1291-1359`, both the idle-timeout row and the
  `otlp_in` residual row that pointed at it) struck or narrowed to what remains open; the counter
  added to `docs/design/internal-telemetry.md`'s bullet for each of the five listeners, and that
  doc's `connection_error` wording gaining "never an idle close, which is counted, not diagnosed";
  amendments or one-line pointers in the four related ADRs this Context section links
  (`syslog-tcp-ingress-and-tls`, `native-transport-handshake-and-ack`, `otlp-tls-and-pooled-grpc-client`,
  `graphite-carbon-relay`); `AGENTS.md`'s current-state sentence; and a commented
  `# idle_timeout: 5m` example in each of `syslog-relay.yaml`, `forwarder-central.yaml`,
  `statsd-to-influxdb.yaml`, and `graphite-relay.yaml`.
- The new counter, `logit.input.connections.closed{reason="idle"}`, is additive telemetry on all
  five listener kinds; `connection_error`'s documented meaning gains one clause it did not have
  before -- an idle close is never routed through it, by design, so a jump in the new counter with no
  corresponding movement in `connection_error` is the expected, healthy signature of the feature
  doing its job rather than something to investigate.
- No new crate dependency: `Notify`, `poll_fn`, `ReadBuf`, `Instant::checked_add`/`far_future`, and
  `BodyExt::frame` are all already in the dependency tree the workstreams above build on.
