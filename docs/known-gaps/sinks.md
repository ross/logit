# Known gaps: File and stdio sinks

Entry format and the other areas: [the known-gaps index](README.md).

- **`file_out` rotates and retains by count, but has no compression, no `max_age`, no
  timestamped rotated-file naming, and its time-based rotation is write-triggered rather than
  boundary-triggered** (ADR `rotating-file-output`).
  - **Reopen:** a SIGHUP reopens a `file_out` or `stdio_out` file target before its next write
    ([ADR `signal-handling`](../adr/signal-handling.md)), not at the signal. A sink that receives
    no batch keeps the renamed inode open, and its disk space allocated, until its next one, and a
    batch mid-write when the signal arrives finishes into the old inode.
  - **Naming and timing:** rotated files always get a numbered suffix (`.1`, `.2`, and so on),
    never a timestamp. An idle sink under a calendar `interval` rolls on its *next* write after
    the boundary, not at the boundary. The rolled file still holds the previous period's contents;
    only its on-disk appearance is late.
  - **Retention and compression:** retention is a plain `max_files` count, with no age-based
    eviction. Compression is `format: native`'s `compression: lz4` or nothing: `format: human` and
    `format: json` have no compression option, and nothing compresses already-rotated files after
    the fact. Both are left to an external tool. A gzip or zstd archive replays through `replay_in`
    ([`replay_in`'s gaps](tailing.md#replay_in)).
  - **Format:** `stdio_out` shares `format:` (`human`, ADR `human-render-block-format`; `json`,
    ADR `stream-json-format`; or `native`, ADR `file-output-native-format`) and `message:`. A
    `format:` *template* over `human` is the one unbuilt extension point. `replay_in` reads a
    `format: json` or `format: native` file back once ([ADR `replay-input`](../adr/replay-input.md));
    nothing follows a growing `format: native` file.
  - **Restart:** `FileTarget::open` seeds the calendar period from an existing file's mtime
    (`RotationState::seed_period`). If the mtime is unreadable (a failed `metadata()` call) or the
    clock jumps backward across the restart, the sink learns the period fresh on the first write
    after open.

- **`stdio_out` to a stdout or stderr pipe has no write bound.** A pipe reader that stops
  reading (a stalled log shipper, a paused `less`) fills the pipe, and the write never returns.
  tokio writes stdout and stderr on a blocking thread: a timeout around the write would return,
  but it can't cancel the write on that thread, and every later write queues behind it, so the
  sink can't bound the attempt as `Output::send`'s contract asks.
  - **Consequence:** the sink holds until shutdown with no failed attempt, so
    `logit.component.retrying` never reads `1` and no `retrying` line is logged. The queue fills
    behind it under its `buffer:` bounds, and then the default `overflow: block` stops the
    sink's sources: every other sink fed from them stops with it. A stream or HTTP listener
    upstream then pushes back on its clients, and a UDP listener's receive queue drops. On the
    sink, the stall shows first as `logit.component.buffer.utilization` at `1`,
    `logit.component.buffer.push.blocked` at `1`, and a `buffer_full` warning repeated once a
    minute, then as `logit.component.inbox.full` and `inbox.blocked.duration` once its inbox
    fills. Until the queue fills, nothing reports the stall.
  - **Workaround:** when the output matters, write it with `file_out` and have the reader follow
    the file. When `stdio_out` is only for watching, set `buffer.overflow: drop_oldest` on it, so
    a stalled reader costs that sink's batches and nothing else.
  - **Why it stays a gap:** bounding the write needs a nonblocking stdout, and `O_NONBLOCK` is a
    flag on the file description `logit` shares with its parent and siblings, so setting it can
    break their writes.
- **The human render shows everything on the event but the batch's provenance.** `stdio_out`'s
  block (ADR `human-render-block-format`) is exhaustive over `Event`, `Resource`, and `Scope`.
  A batch's `origin`/`previous` reach a sink only through `Output::observe_batch`, which
  `StreamOutput` doesn't implement, and ADR `batch-provenance-on-delivered` keeps them off the
  event. `format: json` has the same gap.
  - **To close it:** implement that hook and thread the two names into the render ahead of
    `EventBatch`.
  - **Workaround:** a script that wants them in the data copies them into an attribute.
- **A send the shutdown grace cuts off can leave a torn line in a `stdio_out` or `file_out`
  file.** Both write a batch in place with one `write_all`. When `write_loop`'s grace drops that
  `send` part-way, the part already handed to the file stays, and the sink's `flush()` then
  completes the write in flight, so the file can hold a torn line or native frame. The grace
  decides how the batch is counted (ADR
  [`shutdown-accounting-and-cancellation-safety`](../adr/shutdown-accounting-and-cancellation-safety.md),
  decision 3), but nothing marks the torn record, and the next run appends after it. The other
  sink families' dropped-`send` behavior is in
  [ADR `sink-send-path-and-attempt-accounting`](../adr/sink-send-path-and-attempt-accounting.md).
  - **Related:** a write that succeeds followed by a flush that fails leaves
    `FileTarget::note_written` uncalled for bytes that may have reached the file, so a size
    rotation can come late. The error carries no `Fault`, so the batch drops as `Rejected`
    instead of retrying.

- **The per-request retry memory of `otlp_out` and the Datadog sinks is in-process.**
  `otlp_out`, `datadog_out`, and `datadog_trace_out` each send one batch as several requests and
  remember per batch which the destination settled, so a retry resends only the rest ([ADR
  `sink-fault-classes`](../adr/sink-fault-classes.md), "Amendment: `otlp_out` retries per signal
  (2026-10-05)" and "Amendment: the Datadog sinks retry per request (2026-10-09)"). The memory
  lives in `BatchAccounting` (`crates/logit-outputs/src/accounting.rs`), not on disk.
  - **Consequence:** a `buffer.disk:` replay after a crash is a new batch and resends every
    request, accepted ones included ([Deploying `logit`](../deploying.md#durable-buffering)'s
    "Durable buffering"). Under `at_least_once`, the request that drew an
    `Ambiguous` answer is resent too, and may have been applied. Whether either duplicates depends
    on the destination: Datadog stores a resent series point once and a resent log twice
    (`docs/known-gaps/datadog.md`).
  - **Revisit trigger:** an operator who needs a crash replay to skip delivered requests.

- **A `Rejected` batch has no dead-letter path, and no sink splits a batch in reaction to a
  `413`.** A batch the destination rejects for its own content drops at once
  ([ADR `sink-fault-classes`](../adr/sink-fault-classes.md)) and is visible only in
  `batches.dropped{reason="rejected"}` and a throttled log event carrying the destination's text.
  Nothing keeps the batch for inspection or replay. A `413` or equivalent drops the whole request,
  where Telegraf and libbeat halve the batch and retry; `datadog_out` and `datadog_trace_out`
  count it `oversize`. Three sinks do split, none of them on a `413`: `datadog_out` and
  `datadog_trace_out` cut a batch into requests before sending, under the destination's entry
  count and body size (`split_encode` in `crates/logit-outputs/src/http.rs`), and
  `splunk_hec_out` splits a body once when Splunk Cloud answers it with its oversize code 6. In
  `otlp_out`, `influxdb_out`, and the other HTTP sinks, a batch over the destination's limit
  drops whole.
  - **Workaround:** size the listener's batch bounds below the destination's limit, and read the
    diagnostic for the rejection text.
  - **To close:** a dead-letter sink for rejected batches and a split-on-`413` retry step in the
    shared HTTP driver, each with its own ADR.

- **A stream sink doesn't count the bytes its peer never acknowledged.** A stream with no
  application acknowledgment is best-effort by decision
  ([ADR `delivery-semantics`](../adr/delivery-semantics.md), item 6, "A wire that can't confirm
  delivery bounds its hop"), and [Deploying `logit`](../deploying.md#a-stream-sink-has-no-acknowledgment)
  has the operator's workaround. What's open is counting and measuring that loss.
  `syslog_out`, `statsd_out`, and `graphite_out` over TCP or TLS, and `statsd_out` over a Unix
  stream, count a batch's messages in `logit.output.messages` once the frame's write and flush
  return (`crates/logit-outputs/src/stream.rs`). When the receiver ends with bytes in either
  socket buffer, the next send's probe finds the connection closed and redials, and nothing
  reports the frames already counted.
  - **To close, the sender's part:** on a failed connection, `SIOCOUTQ` (or `TCP_INFO`'s
    unacknowledged bytes) says how much the peer never acknowledged, and the driver could count
    that as loss. The receiver's part, acknowledged by its kernel and never read, can't be seen
    from the sender without an application acknowledgment such as RELP's.
  - **Unmeasured, a stalled receiver:** against a reader that keeps up, the loss is too small to
    see. In `syslog-tcp-ingress` and `syslog-tls-ingress`, at 2,000 lines a second with each
    buffer capped at 256 KiB, four kills' gaps of 9,800 to 9,916 lines each fell within about
    400 lines, the error of the estimate, of what the listener read after its last export
    ([plan `soak-harness`](../plans/soak-harness.md)'s "Findings"). A receiver stalled behind a
    full receive buffer when it's killed would lose the whole buffer, up to several MiB under
    Linux's default autotuning, and no scenario measures that.

- **A kept-alive connection a destination closed first reads as `Ambiguous`.** An HTTP sink
  closes a pooled connection idle past its `idle_timeout:` (30 s by default) before reusing it,
  so a destination that closes idle connections later doesn't race a write
  ([ADR `sink-request-concurrency`](../adr/sink-request-concurrency.md), decision 7). A
  destination that closes a kept-alive connection for another reason still can: a restart, a
  per-connection request cap, or an idle timeout shorter than the sink's `idle_timeout:`. A write
  the sink sends as that close crosses it is reset, and the sink reads `Ambiguous` ("connection
  closed before message completed"), so `at_most_once` drops the batch, counted
  `batches.dropped{reason="ambiguous_at_most_once"}`, and `at_least_once` resends it. Delay on
  the link widens the window to the delay. Over h2 a graceful `GOAWAY` tells the client which
  requests the server never saw, and those are resent unseen; HTTP/1.1 has no such signal.
  - **Measured:** under a 1 s delay on the sink's egress at `concurrency: 8`, a destination with
    a 5 s idle timeout reset 12 writes no fault caused in about 2,300 batches under a 90 s pool
    idle timeout, and none at `idle_timeout: 1s`
    ([plan `sink-request-concurrency`](../plans/sink-request-concurrency.md)'s "Findings").
  - **Workaround:** set `idle_timeout:` below the destination's idle timeout less the link's
    delay, or keep `at_least_once` so the batch is resent.
  - **Not pursued:** resending a write that failed on a reused connection with no byte received,
    as Chromium does, which decision 7 rejects: `reqwest` doesn't say whether a failed request's
    connection was reused.
