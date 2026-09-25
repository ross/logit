---
created: 2026-09-25
updated: 2026-09-25
---

# Untrusted-input bounds: one set of rules for every decoder and listener a peer can reach

## Status
Accepted

## Context

`docs/plans/critical-sections-inventory.md` groups the code an unauthenticated peer feeds bytes to
as its first cluster, "Remote-reachable crash/DoS": CORE-05, CORE-06, WIRE-01..03, WIRE-06,
WIRE-10/11/15, CODEC-16, and CODEC-17, plus WIRE-07 and NET-10 for top lead 13 (the accept loop).
Reading that code found each decoder and listener defending itself in its own way, some well and
some not at all:

- **Depth.** Neither OTLP `AnyValue` walk (`otlp/json/mod.rs`'s `any_value`,
  `otlp/common.rs`'s `any_value_to_value`) has a depth cap of its own. They rely on serde_json's
  parser limit of 128 and prost's decode limit of 100, each one feature flag away from gone. The
  native `Value` decoder caps itself at 128 (CODEC-16).
- **Timestamps.** 17 OTLP decode sites cast a wire `u64` timestamp with `as i64`, so a value past
  `i64::MAX` wraps negative: 3 in `logs.rs`, 3 in `traces.rs`, and 11 in `metrics.rs`. The encode
  side clamps with `.max(0)` (CODEC-17).
- **Declared counts.** The native metric-kind list arms bound allocation only by remaining wire
  bytes, so a 1-byte set member becomes a `Bytes` (about 32 times its wire size) and an
  exponential bucket becomes 8 times its wire size. `read_metric_kind` and `for_each_field` ignore
  bytes left over in a carved body, where `read_record_list_into` rejects them. `read_uvarint`
  accepts a 10th byte with bits above bit 0 set and discards them. `write_frame_with_flags` casts
  the payload length to `u32` unchecked (WIRE-01, WIRE-02, WIRE-03).
- **Interning.** `Dict::read` interns every dictionary entry into the never-evicting process
  interner before the batch validates (WIRE-02).
- **Sketches.** `DdSketch::from_bytes` and `HyperLogLog::from_bytes` bound their counts before
  allocating, but no robustness harness covers either, and nothing pins the serde behavior the
  `HyperLogLog` codec's soundness depends on (CORE-05, CORE-06).
- **HTTP/2 streams.** No hyper listener sets `max_concurrent_streams`. hyper 1.11.1's h2 server
  default is 200, not unlimited, so each listener's documented worst case (1024 connections times
  one body) is low by a factor of 200. The rapid-reset defaults exist but aren't pinned (WIRE-10,
  WIRE-11, WIRE-15).
- **`logit_in` reads.** `logit_in` allocates the whole declared body, up to 64 MiB, before a body
  byte arrives, then copies header and body into a second buffer. Its `Hello` read is bounded by
  `max_frame_bytes`. Six control writes have no timeout, so a peer that never reads pins the
  connection (WIRE-06).
- **The connection gauge.** `logit.input.connections` is a bare `fetch_add`/`fetch_sub` pair
  around `serve_connection` in six listeners. A panic in between leaks the count (WIRE-06, WIRE-11,
  WIRE-15).
- **Accept errors.** Eight accept loops propagate any `accept()` error with `?`. tokio retries only
  `EAGAIN`, so one `EMFILE` or `ENOBUFS` stops the listener for the life of the process (WIRE-07,
  NET-10).
- **Encoding headers.** `otlp_in` matches `content-encoding` and `grpc-encoding` by exact bytes,
  so `Gzip` gets a `415`, and a unary gRPC body carrying a second frame is decoded as its first
  frame alone (WIRE-10).

Some of the code checked out and needs only a test to pin it: `frame.rs` checks both declared
lengths before allocating; `prometheus_in`'s snappy and zstd paths are bounded; and
`drive_with_idle`'s wait-out loop has no ceiling by design.


## Decision

Every decoder and listener that reads peer bytes follows the rules below. Each rule is stated once
here, and the code that enforces it points at this ADR.

### Threat model

The peer these rules defend against is a misbehaving or compromised peer on a private network:
another `logit`, an agent, an SDK, or an exporter the operator runs, sending malformed or hostile
bytes. The bar is that no input crashes the process or makes it allocate without bound, and that
nothing a peer sends corrupts what `logit` relays. Surviving direct exposure to the open internet
is not the bar. `docs/known-gaps.md`'s "Event model and interner" section already relies on the
same premise for the never-evicting interner, and its revisit trigger (a listener that stops being
private) applies to these rules too.

### Decoders

- **A declared length or count never sizes an allocation.** A decoder checks a declared length
  against both a fixed cap and the bytes remaining before it allocates, and lets a collection grow
  from what it reads. Where one wire byte legitimately becomes more than one heap byte, the
  measured wire-to-heap expansion ratio for that decoder is recorded next to its caps in
  [`docs/design/wire-protocol.md`](../design/wire-protocol.md). A new count cap is added only when
  a measured ratio makes the frame cap unsafe, and the measurement is recorded either way.
- **One `Value` depth cap across `logit-proto`.** A single crate-level `MAX_VALUE_DEPTH` (128)
  bounds the native `Value` decoder and both OTLP `AnyValue` walks. Each walk threads a depth and
  returns `CodecError::Malformed` past the cap. A third-party parser's own limit is a second line
  of defense, never the only one.
- **Varints are canonical.** A 10-byte varint whose last byte has any bit above bit 0 set is
  malformed. No writer produces one.
- **A carved body has no trailing bytes.** When a decoder carves a length-prefixed body or field
  and parses it, bytes left over are malformed. Before release there is no forward-compatibility
  padding to preserve.
- **A writer refuses what a reader would reject.** `write_frame` returns an error for a payload
  over `MAX_SANE_UNCOMPRESSED_LEN` instead of truncating its length, so the cap is enforced once.
- **Out-of-range OTLP timestamps saturate.** An OTLP wire timestamp past `i64::MAX` nanoseconds
  decodes as `i64::MAX` through one helper, applied at all 17 decode sites: `logs.rs`'s `decode_log_record` (`time_unix_nano`, the `observed_time_unix_nano` fallback, and `observed_timestamp`); `traces.rs`'s `decode_span_event` and `decode_span` (start and end); and `metrics.rs`'s `decode_exemplar` plus `time_unix_nano` and `start_time_unix_nano` for each of `Sum`, `Gauge`, `Histogram`, `Summary`, and `ExponentialHistogram`.
  This is a permitted normalization under
  [ADR `lossless-transit`](lossless-transit.md): a saturated timestamp relays as
  2262-04-11T23:47:16.854775807Z, not the original value.

### `logit_in`

- **The frame body is read incrementally.** One buffer starts at the header plus
  `min(compressed_len, 64 KiB)` and grows by doubling as bytes arrive, never past the declared
  end. A peer that declares 64 MiB and sends nothing holds 64 KiB, not 64 MiB.
- **The `Hello` has its own cap.** `MAX_HELLO_BYTES` (4 KiB) bounds the handshake read instead of
  `max_frame_bytes`.
- **Every control write is bounded.** `HelloAck`, `Ack`, and every `Reject`, including
  `GOING_AWAY`, is written under `handshake_timeout`. A write that times out ends the connection.

### HTTP and gRPC listeners

- **An explicit stream cap.** Every hyper listener (`otlp_in` HTTP and gRPC, `prometheus_in`'s
  remote-write receiver, `datadog_in`, `datadog_trace_in`) builds its connections through one
  shared builder that sets `max_concurrent_streams` to 32 and pins
  `max_pending_accept_reset_streams` (20) and `max_header_list_size` (16 KiB) explicitly, so a
  hyper upgrade can't move them. The per-listener worst case is
  `MAX_CONCURRENT_CONNECTIONS × MAX_CONCURRENT_STREAMS × 2 × MAX_REQUEST_BYTES`: a compressed body
  and its decompressed copy on every stream of every connection. For `otlp_in` that is
  1024 × 32 × 2 × 4 MiB = 256 GiB, a bound on what a peer can make the process try to allocate,
  not a memory budget.
- **Encoding names are case-insensitive.** `otlp_in` matches `content-encoding` and
  `grpc-encoding` the way `http.rs`'s `Encoding::from_headers` already does for the Datadog
  listeners.
- **A unary gRPC body carries one message.** A second gRPC frame in the body is
  `INVALID_ARGUMENT`, not ignored.

### Every listener

- **The connection gauge is a drop guard.** `LiveConnections` hands out one guard per connection
  that increments `logit.input.connections` on creation and decrements it on drop, so a panicking
  connection task still returns the gauge to its true value. tokio drops a task's future after a
  panic, and no build profile sets `panic = "abort"`.
- **An accept error is classified, not propagated.** Each accept loop passes the error to one
  shared classifier and acts on its class:

  | Class | errno | Action |
  |---|---|---|
  | Connection | `ECONNABORTED`, `ECONNRESET`, `EINTR`, `EPERM`, `EPROTO`, `EHOSTDOWN`, `ENONET`, `EHOSTUNREACH`, `EOPNOTSUPP`, `ENETDOWN`, `ENETUNREACH` | Retry at once: the failure belongs to one connection. |
  | Resource | `EMFILE`, `ENFILE`, `ENOBUFS`, `ENOMEM` | Back off 100 ms, then continue. |
  | Fatal | `EBADF`, `EINVAL`, `ENOTSOCK`, `EFAULT`, and tokio's runtime-shutdown error | Return the error: the listening socket itself is unusable. |
  | Other | anything else | Back off 100 ms, then continue. |

  100 ms is the backoff `prometheus_out`'s exposition server and the admin server already use.
  Every accept error counts `logit.input.accept.errors{reason}` and is diagnosed under the key
  `accept_error`.

## Alternatives considered

- **A per-listener in-flight byte budget.** A semaphore over bytes held in request bodies would
  bound the product in the worst-case formula, where the stream cap bounds only one of its
  factors. It is the right next step for a public listener. It is recorded as a follow-up in
  `docs/known-gaps.md` rather than built here, because it changes how every HTTP listener reads a
  body.
- **Reject an out-of-range OTLP timestamp, or treat it as unset.** Rejecting fails a whole export
  request over one field the sender can't correct. Treating it as unset makes it look like a real
  zero, which OTLP gives a meaning (for example, "use the observed time"). Saturating keeps the
  event and its ordering, and is a named, testable normalization.
- **A total body deadline.** A deadline on the whole body would close the dribbled-body gap. It
  was declined: a slow link sending a large legitimate body looks the same. The body read has a
  per-frame (per-`read` on `logit_in`) stall bound, and that bound is `idle_timeout`, which is off
  by default. With `idle_timeout` unset, a stalled or dribbled body is unbounded in time. With it
  set, a peer sending one byte per frame, each slightly under the bound, holds a request for up to
  `MAX_REQUEST_BYTES × idle_timeout` on an HTTP listener and `max_frame_bytes × idle_timeout` on
  `logit_in`. Both costs are recorded in `docs/known-gaps.md`.
- **A lower default `max_frame_bytes`.** Declined. Incremental reads remove the up-front
  allocation that made 64 MiB costly, and a relay that batches aggressively needs the headroom.
- **A listener-wide request semaphore.** Declined in favor of the per-connection stream cap. A
  shared count of in-flight requests lets one connection starve the others, and it counts requests
  where the resource at risk is bytes. The byte budget above is the better form of the same idea.

## Consequences

- A peer that sends any of the following now gets a rejection where it used to get silent
  acceptance: a non-canonical varint, trailing bytes in a native field or metric body, a unary
  gRPC body with a second frame, OTLP nesting past 128, a `Hello` over 4 KiB, or more than 32
  concurrent streams on one connection. No conforming sender produces any of these.
- An OTLP sender that spells its encoding `Gzip` or `GZIP` now interoperates with `otlp_in`.
- An OTLP timestamp past 2262-04-11 relays as 2262-04-11T23:47:16.854775807Z.
- `docs/design/wire-protocol.md` gains each native decoder's measured expansion ratio, and
  `docs/known-gaps.md` gains the dribbled-body cost, the in-flight byte budget follow-up, and the
  native dictionary as an interner feeder.
- Unchanged: `drive_with_idle`'s wait-out loop still has no ceiling. It waits for an in-flight
  request to finish before closing an idle connection, and a request blocked in `Fanout::send` is
  backpressure, not idleness ([ADR `idle-connection-timeout`](idle-connection-timeout.md)).
- A new decoder or listener of peer bytes is held to these rules in review, and gets a fuzz target
  ([ADR `out-of-ci-fuzzing`](out-of-ci-fuzzing.md)).
