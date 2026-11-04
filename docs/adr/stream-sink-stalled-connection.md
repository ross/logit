---
created: 2026-11-03
updated: 2026-11-03
---

# Stream sinks keep a stalled connection and count the unacknowledged bytes a destroyed one held

## Status
Accepted

## Context

`syslog_out`, `statsd_out`, and `graphite_out` over TCP or TLS, and `statsd_out` over a Unix
stream, share one pooled-stream driver (`crates/logit-outputs/src/stream.rs`, [ADR
`sink-send-path-and-attempt-accounting`](sink-send-path-and-attempt-accounting.md), decisions 4
to 6). A line protocol has no acknowledgment, so a receiver that dies with bytes in either socket
buffer loses them, and that bound is decided ([ADR `delivery-semantics`](delivery-semantics.md),
item 6).

How much is lost depended on the driver, not only on the wire. A write that accepted nothing for
`connect_timeout` failed the attempt and dropped the connection, and the next attempt redialed.
A receiver that stopped reading still has a listening socket, so the kernel completes the
handshake and parks the new connection in the accept backlog, where it takes a second pair of
full buffers. Each stall did that again. The soak harness measured the result
(`docs/known-gaps/sinks.md`): a receiver killed 90 s into a stall lost 4.5 MB with 256 KiB
buffers and 7.2 MB under Linux's default autotuning, across 13 parked connections, and none of
it was counted.

The kernel knows part of that loss. `SIOCOUTQ` on a TCP socket reads `write_seq - snd_una`, the
bytes sent but unacknowledged plus the bytes not yet sent, and it still reads that after the peer
resets the connection, because `tcp_reset` purges the write queue but leaves both sequence
numbers. Measured on Linux 7.2 against a loopback receiver that never read: 2,720,256 bytes
written, 128,512 acknowledged into the receiver's buffer, and 2,591,744 in `SIOCOUTQ` both before
and after the receiver's reset (`sockstat`'s `send_queue_still_reads_what_a_reset_destroyed`
pins the sum). A receiver that read everything and closed leaves 0. On `AF_UNIX` the same ioctl
reads `sk_wmem_alloc`, the memory charged to the receiver's queue (238,336 for 233,152 bytes
written), and 0 once the receiver closes, because the close frees it.

## Decision

1. **A stalled connection is kept.** A write that accepts nothing for `connect_timeout` fails
   the attempt, and the driver pools the connection instead of dropping it, whether the stall
   came on the frame's first write, a later write, or the flush. A first write that accepted
   nothing is `Clean`, plaintext or TLS: tokio-rustls answers `Pending` only when its session
   took none of the bytes (`crate::stream_pins`'
   `a_pending_tls_write_accepted_nothing_and_queued_ciphertext_leaves_first`). A stall after part
   of the frame was accepted is `Ambiguous`, as before.
2. **A frame cut mid-write is finished first.** The connection keeps the frame's unwritten tail,
   or an owed flush, and the next attempt writes it before its own frame, under the same progress
   bound. A tail that stalls again is `Clean` for the new frame, which never started, and the
   connection stays. A tail whose write fails ends the connection (decision 4), and the new frame
   goes on a redial that doesn't consume the plaintext retry. A receiver never sees a frame cut
   and another begun on one connection.
3. **A TCP peer that isn't answering is closed instead.** Before keeping a stalled TCP
   connection, the driver reads `TCP_INFO` (`sockstat::tcp_peer_answering`): it keeps the
   connection only if `tcpi_retransmits == 0` and `tcpi_probes < 2`. `tcp_ack` zeroes
   `tcpi_probes` on any ACK that isn't old, so a receiver that stopped reading but still ACKs
   the zero-window probes stays answering however long it stalls; measured on loopback over 8 s,
   `tcpi_probes` never passed 1 and `tcpi_retransmits` stayed 0 while `tcpi_backoff` reached 5.
   `tcpi_retransmits` resets only on an ACK that advances `snd_una`, so a live receiver that
   answers a retransmission timeout's resend with a zero-window ACK still reads as not
   answering, and its connection is closed, uncounted. A vanished or partitioned host fails a
   probe or a retransmission and is closed, as before, uncounted (decision 5). A Unix
   stream is always kept, and so is a connection whose `TCP_INFO` can't be read: a dead peer
   still ends in a write error once the kernel gives up.
4. **The bytes a destroyed connection held are counted.** When a pooled connection ends with its
   data certainly destroyed, the driver reads `SIOCOUTQ` before closing it, and counts the bytes
   of earlier batches it held as `logit.output.bytes.dropped{reason="unacknowledged"}`, with a
   throttled `warn` keyed `unacknowledged`. Certainly destroyed means: the probe of a reused
   connection reads an EOF or a reset (the receiver closed with data unread, which resets), reads
   unsolicited bytes (closing a socket with unread data sends a reset and purges the queue), or
   reads a TLS alert; or a write or flush fails. The count is in bytes, since the queue doesn't
   know message boundaries, as `input.bytes.dropped{reason="truncated"|"gone"}` already counts a
   tailed file's loss. Under TLS it is wire bytes, record framing included.
5. **Nothing is counted where the data may still arrive.** A stall's closed connection (decision
   3) and a connection the shutdown flush gives up on become orphans the kernel keeps sending,
   so a receiver that resumes still gets the bytes, and a count would break "any nonzero
   `*.dropped` is loss". A Unix stream is never counted: its queue reads 0 once the receiver
   closes.
6. **Every batch already accounted for is left out of the count.** The bytes of each frame an
   attempt returned `Ambiguous` for, and its finished tail, belong to a batch that is resent
   under `at_least_once` or counted `batches.dropped{reason="ambiguous_at_most_once"}`, so they
   aren't counted again. The connection keeps every such frame's stream offsets, not only the
   last one's, because an earlier frame's bytes can still be queued behind a later one, and
   forgets a frame once a queue reading shows it has left. On plaintext the queue is the last
   `SIOCOUTQ` bytes the stream accepted, and each frame's overlap with it is computed. Under TLS
   the queue is ciphertext, so each frame is taken out whole with the most framing it could
   carry (29 bytes per 16 KiB record and per write call), unless the bytes after it can't fit
   in the queue and a full session together.
   The count is a floor: it leaves out up to 64 KiB of ciphertext queued in the rustls session,
   which a boxed stream doesn't expose.
7. **The count is on the sink's ungated handles.** It is the destination's verdict on batches an
   earlier attempt already called delivered, met once per connection, not an encode-side
   outcome a retry repeats ([ADR
   `sink-send-path-and-attempt-accounting`](sink-send-path-and-attempt-accounting.md), decisions
   1 and 2).
8. **The shutdown flush is bounded.** `PooledStream::flush` finishes a kept tail and flushes
   under `connect_timeout` per progress step, and drops the connection on a stall or an error,
   uncounted. Without the bound, a kept TLS connection to a frozen receiver would hold
   `finish_and_flush`, which runs outside the shutdown grace.

## Alternatives considered

- **Keep only a connection whose stalled write accepted nothing.** Under a frozen receiver the
  stall is almost always mid-frame: the first write returns a short count and the rest stalls,
  or under TLS the flush stalls. Keeping only the zero-progress case would redial in almost every
  real stall.
- **Count at the stall, when the connection is given up.** A closed connection with data queued
  is an orphan that keeps retransmitting, and the receiver may resume and read it, so that count
  could report loss that never happened.
- **`TCP_USER_TIMEOUT` to end a dead peer's connection.** Current kernels apply it to
  zero-window probing too (`tcp_probe_timer`), so it would abort a live receiver that stopped
  reading, which is the case this keeps.
- **`SO_KEEPALIVE` to detect a dead peer.** Keepalive skips a socket with data in flight, which
  a stalled connection always has.
- **`TCP_INFO`'s `tcpi_bytes_sent - tcpi_bytes_acked` instead of `SIOCOUTQ`.** It counts only
  bytes that left the socket, not bytes queued behind a zero window: on the frozen receiver above
  it read 47,615 where `SIOCOUTQ` read 2,591,744. `SIOCOUTQ` held after the reset, so no fallback
  was needed.
- **Resume a kept frame instead of writing the batch again.** Under `at_least_once` the retried
  batch arrives twice: once as the kept frame finishes, once whole in the resend. The driver
  could recognize the resend's frame as the one it holds and write only the tail. That needs
  frames to encode byte-identically across attempts, which no stream sink promises. It is
  bounded at one batch per stall, inside the `Ambiguous` allowance the posture already accepts,
  and left as a follow-up.
- **RELP for `syslog_out`.** The only way to see the receiver's share of the loss. It is a new
  transport with its own ADR, recorded in `docs/known-gaps/syslog.md`.

## Consequences

- A receiver that stops reading puts one buffer pair at risk, whatever the stall's length, and
  `logit.output.reconnects` stays flat through the stall. Each attempt in the stall waits
  `connect_timeout`, as a redial's stalled write did.
- Under `at_least_once` the whole stalled batch is delivered twice once the receiver reads
  again: the kept connection finishes the frame, and the retry sends the frame whole.
- A live receiver with a closed window whose kernel answered a retransmission timeout's resend
  with a zero-window ACK reads as not answering (decision 3). Its connection is closed and
  redialed, uncounted, as a stall's connection was before this decision. The measurements above
  didn't produce that case, so how often a real stall meets it is unmeasured.
- The sender's share of a receiver's death is counted. The receiver's share, acknowledged by its
  kernel and never read, isn't, and neither is anything on a Unix stream, a stall's closed
  connection, or shutdown (`docs/known-gaps/sinks.md`). A receiver that half-closes and keeps
  reading is overcounted: its FIN reads as a close.
- `sockstat` gains `send_queue` (an `ioctl`) and `tcp_peer_answering` (a third `getsockopt`), so
  [ADR `out-of-ci-unsafe-verification`](out-of-ci-unsafe-verification.md) covers them. Off Linux
  both are unavailable, logged once at `debug`; the connection is kept and nothing is counted.
- `logit_out` keeps its own fault rules: `write_with_progress` still reports a stall the same
  way, and `connect` still returns the stream alone.
