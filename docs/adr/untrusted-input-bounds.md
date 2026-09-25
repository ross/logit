---
created: 2026-09-25
updated: 2026-09-25
---

# Untrusted-input bounds: one set of rules for every decoder and listener a peer can reach

## Status
Accepted

## Context

`docs/plans/critical-sections-inventory.md` groups the code that decodes bytes from a peer as its
first cluster, "Remote-reachable crash/DoS": CORE-05, CORE-06, WIRE-01..03, WIRE-06,
WIRE-10/11/15, CODEC-16, and CODEC-17, plus WIRE-07 and NET-10 for top lead 13 (the accept loop).
Reading that code found each decoder and listener handling unexpected input in its own way:

- **Depth.** Neither OTLP `AnyValue` walk (`otlp/json/mod.rs`'s `any_value`,
  `otlp/common.rs`'s `any_value_to_value`) has a depth cap of its own. They rely on serde_json's
  recursion limit and prost's decode limit, and no test pins either. The native `Value` decoder
  caps itself at 128 (CODEC-16).
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
- **`logit_in` reads.** `logit_in` allocates the whole declared body before a body byte arrives,
  then copies header and body into a second buffer. Six control writes have no timeout, so a peer
  that stops reading pins the connection task (WIRE-06).
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

The bar is accidental data: a misconfigured or buggy sender, an unexpected producer, a wedged
peer, or a corrupt file. A problem only crafted input can trigger is defended only when the
defense is free, and is otherwise a documented non-goal (listed below). The decision of record is
[ADR `deployment-threat-model`](deployment-threat-model.md).

### Decoders

- **A per-frame decode budget.** Decoding a frame may allocate at most 4 × the listener's
  effective `max_frame_bytes` of estimated heap, charged per element as it is decoded. The reason
  is a misconfigured sender's giant batch, not a bomb. The per-element charges are the measured
  wire-to-heap expansion ratios, recorded next to the caps in
  [`docs/design/wire-protocol.md`](../design/wire-protocol.md).
- **OTLP nesting keeps its parsers' limits.** OTLP nesting is bounded by serde_json's recursion
  limit (128 JSON levels, about 41 `AnyValue` levels) and prost's (100 messages, about 49 levels),
  both under native's 128. Tests pin both limits so a dependency bump can't remove them silently.
  No local cap is added.
- **Varints are canonical.** A 10-byte varint whose last byte has any bit above bit 0 set is
  malformed. No writer produces one, so this also surfaces an encoder bug instead of hiding it.
- **A carved body has no trailing bytes.** When a decoder carves a length-prefixed body or field
  and parses it, bytes left over are malformed. This also surfaces an encoder bug instead of
  hiding it. Before release there is no forward-compatibility padding to preserve.
- **A writer refuses what a reader would reject.** `write_frame` returns an error for a payload
  over `MAX_SANE_UNCOMPRESSED_LEN` instead of truncating its length, so the cap is enforced once.
- **Out-of-range OTLP timestamps saturate.** An OTLP wire timestamp past `i64::MAX` nanoseconds
  decodes as `i64::MAX` through one helper, applied at all 17 decode sites: `logs.rs`'s
  `decode_log_record` (`time_unix_nano`, the `observed_time_unix_nano` fallback, and
  `observed_timestamp`); `traces.rs`'s `decode_span_event` and `decode_span` (start and end); and
  `metrics.rs`'s `decode_exemplar` plus `time_unix_nano` and `start_time_unix_nano` for each of
  `Sum`, `Gauge`, `Histogram`, `Summary`, and `ExponentialHistogram`. [ADR
  `lossless-transit`](lossless-transit.md)'s "Permitted normalizations" list records this as a
  permitted normalization.

### `logit_in`

- **The body is copied once.** The body read stays pre-sized from the frame header, but it fills
  the frame's final buffer directly, so the body is copied once instead of twice.
- **Every control write is bounded.** `HelloAck`, `Ack`, and every `Reject`, including
  `GOING_AWAY`, is written under `handshake_timeout`. A write that times out ends the connection.
  The reason is a wedged peer: a stopped process, or a full receive buffer, pins the connection
  task in an unbounded write today.

### HTTP and gRPC listeners

- **The stream cap is pinned.** Every hyper listener (`otlp_in` HTTP and gRPC, `prometheus_in`'s
  remote-write receiver, `datadog_in`, `datadog_trace_in`) builds its connections through one
  shared builder that sets `max_concurrent_streams` to hyper's own default of 200, and
  `max_pending_accept_reset_streams` (20) and `max_header_list_size` (16 KiB) to theirs,
  explicitly, so a hyper upgrade can't move them. The per-listener worst case is
  `MAX_CONCURRENT_CONNECTIONS × MAX_CONCURRENT_STREAMS × 2 × MAX_REQUEST_BYTES`: a compressed body
  and its decompressed copy on every stream of every connection. For `otlp_in` that is
  1024 × 200 × 2 × 4 MiB = 1.6 TiB, a bound on what peers could make the process try to allocate,
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
- **An accept error is classified, not propagated.** File-descriptor exhaustion is an operational
  accident, not an attack, and it must not stop a listener for the life of the process. Each
  accept loop passes the error to one shared classifier and acts on its class:

  | Class | errno | Action |
  |---|---|---|
  | Connection | `ECONNABORTED`, `ECONNRESET`, `EINTR`, `EPERM`, `EPROTO`, `EHOSTDOWN`, `ENONET`, `EHOSTUNREACH`, `EOPNOTSUPP`, `ENETDOWN`, `ENETUNREACH` | Retry at once: the failure belongs to one connection. |
  | Resource | `EMFILE`, `ENFILE`, `ENOBUFS`, `ENOMEM` | Back off 100 ms, then continue. |
  | Fatal | `EBADF`, `EINVAL`, `ENOTSOCK`, `EFAULT`, and tokio's runtime-shutdown error | Return the error: the listening socket itself is unusable. |
  | Other | anything else | Back off 100 ms, then continue. |

  100 ms is the backoff `prometheus_out`'s exposition server and the admin server already use.
  Every accept error counts `logit.input.accept.errors{reason}` and is diagnosed under the key
  `accept_error`.

### Documented non-goals

Each of these needs crafted input, and none has a free defense. Each is recorded in
`docs/known-gaps.md`:

- **Interning a rejected batch's dictionary.** CRC-32C already rejects accidental corruption
  before decode, so only a crafted frame interns strings from a batch that later fails ("Event
  model and interner").
- **Descending-key attribute-map inserts.** A native attribute map whose keys arrive in descending
  dictionary order inserts in quadratic time ("Native wire format, `logit_in`/`logit_out`, and
  buffering").
- **OTLP/JSON peak heap under crafted tiny objects.** A body of tiny objects under an unknown key
  peaks at about 98 bytes of heap per input byte, against about 16 for ordinary structure
  ("OTLP").
- **Compression-ratio amplification in general.** Each decompressed body is capped, but nothing
  bounds many connections each inflating a small body to its cap at once (the in-flight byte
  budget entry under "TLS and connection lifecycle").

## Alternatives considered

- **A per-listener in-flight byte budget.** A semaphore over bytes held in request bodies would
  bound the product in the worst-case formula, where the stream cap bounds only one of its
  factors. It is recorded as a follow-up in `docs/known-gaps.md` rather than built here, because
  it changes how every HTTP listener reads a body.
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
- **A lower default `max_frame_bytes`.** Declined. A relay that batches aggressively needs the
  headroom, and the per-frame decode budget scales with the configured value.
- **A listener-wide request semaphore.** Declined. A shared count of in-flight requests lets one
  connection starve the others, and it counts requests where the resource at risk is bytes. The
  byte budget above is the better form of the same idea.
- **Declined because each needs a crafted peer, and the defense is not free:**
  - a local `Value` depth cap shared by the OTLP walks;
  - an incremental `logit_in` body read with a handshake-sized `Hello` cap;
  - a body cap specific to OTLP/JSON;
  - lazy dictionary interning, after the batch validates;
  - a stream cap of 32 instead of hyper's 200.

## Consequences

- A peer that sends any of the following now gets a rejection where it used to get silent
  acceptance: a non-canonical varint, trailing bytes in a native field or metric body, a frame
  whose decode exceeds the per-frame budget, or a unary gRPC body with a second frame. No
  conforming sender produces any of these, so each rejection points at a sender bug.
- An OTLP sender that spells its encoding `Gzip` or `GZIP` now interoperates with `otlp_in`.
- An OTLP timestamp past 2262-04-11 relays as 2262-04-11T23:47:16.854775807Z.
- `docs/design/wire-protocol.md` gains each native decoder's measured expansion ratio, and
  `docs/known-gaps.md` gains the dribbled-body cost, the in-flight byte budget follow-up, and the
  non-goals above.
- Unchanged: `drive_with_idle`'s wait-out loop still has no ceiling. It waits for an in-flight
  request to finish before closing an idle connection, and a request blocked in `Fanout::send` is
  backpressure, not idleness ([ADR `idle-connection-timeout`](idle-connection-timeout.md)).
- A new decoder or listener of peer bytes is held to these rules in review, and gets a fuzz target
  ([ADR `out-of-ci-fuzzing`](out-of-ci-fuzzing.md)).
