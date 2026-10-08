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
    eviction. Compression is `format: native`'s `compression: lz4` or nothing: `format: human`'s
    text render has no compression option, and nothing compresses already-rotated files after the
    fact. Both are left to an external tool.
  - **Format:** `stdio_out` shares `format:` (`human`, ADR `human-render-block-format`; `json`,
    ADR `stream-json-format`; or `native`, ADR `file-output-native-format`) and `message:`. A
    `format:` *template* over `human` is the one unbuilt extension point. Reading a
    `format: native` file back, with a decoder-side reader/verifier or by wiring `NativeDecoder`
    into `tail_in`, is unblocked, undesigned follow-up work.
  - **Restart:** `FileTarget::open` seeds the calendar period from an existing file's mtime
    (`RotationState::seed_period`). If the mtime is unreadable (a failed `metadata()` call) or the
    clock jumps backward across the restart, the sink learns the period fresh on the first write
    after open.

- **`file_out` never fsyncs, by design.** Nothing in `crates/logit-outputs/src/file.rs` fsyncs
  the active file, the `.rotating` staging file, or the directory after a rename, so a power loss
  (not a process crash) can lose the most recent writes or leave a rotation half-applied. A
  log-file sink doesn't pay per-batch fsyncs for a guarantee few deployments need; see the
  "`file_out` makes no durability promise" amendment to
  [ADR `rotating-file-output`](../adr/rotating-file-output.md#amendment-file_out-makes-no-durability-promise-2026-09-24).
  There's no revisit trigger short of a deployment that needs a power-loss-safe log file.
- **`stdio_out` to a stdout or stderr pipe has no write bound.** A pipe reader that stops
  reading (a stalled log shipper, a paused `less`) fills the pipe, and the write never returns.
  tokio writes stdout and stderr on a blocking thread: a timeout around the write would return,
  but it can't cancel the write on that thread, and every later write queues behind it, so the
  sink can't bound the attempt as `Output::send`'s contract asks.
  - **Consequence:** the sink holds silently until shutdown. No attempt fails, so
    `logit.component.retrying` never reads `1` and no `retrying` line is logged. The queue fills
    behind it under its `buffer:` bounds, and then the default `overflow: block` stops the
    sink's sources: every other sink fed from them stops with it, and a listener upstream pushes
    back on its clients. The stall shows only as `logit.component.inbox.full` and
    `inbox.blocked.duration` on the sink.
  - **Workaround:** when the output matters, write it with `file_out` and have the reader follow
    the file. When `stdio_out` is only for watching, set `buffer.overflow: drop_oldest` on it, so
    a stalled reader costs that sink's batches and nothing else.
  - **Why it stays a gap:** bounding the write needs a nonblocking stdout, and `O_NONBLOCK` is a
    flag on the file description `logit` shares with its parent and siblings, so setting it can
    break their writes.
- **No runtime bound on a sink's attempt; each sink bounds its own.** The runtime wraps
  `Output::send` in no timeout ([ADR `sink-fault-classes`](../adr/sink-fault-classes.md), "A
  retryable fault retries until it succeeds"). The HTTP and gRPC sinks bound each request with
  `request_timeout`, the stream sinks bound each write's progress with `connect_timeout`, the Unix
  datagram send has its `send_timeout`, and `logit_out` bounds each write and acknowledgment wait
  with `request_timeout`. `stdio_out` to a pipe has no bound (the entry above).
  - **Consequence:** a sink whose write or acknowledgment wait has no transport timeout holds its
    queue silently when the destination stops answering: no attempt fails, so
    `logit.component.retrying` stays `0` and no `retrying` line is logged. Only the shutdown grace
    cuts it.
  - **Rule for a sink author:** give every write and acknowledgment wait a sink makes its own
    timeout, and classify a timeout that fires as `Ambiguous` (or `Clean` when nothing left the
    process).
  - **Alternative declined:** a runtime backstop timeout above every sink's own. Each sink's
    timeout is configurable per component, so a backstop set above it needs a graph rule tying the
    two, for a case the per-sink rule already covers.
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
