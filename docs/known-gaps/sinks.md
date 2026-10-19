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
    the fact. Both are left to an external tool. A gzipped JSON archive also has no reader
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
    sink's sources: every other sink fed from them stops with it. A stream or HTTP listener
    upstream then pushes back on its clients, and a UDP listener's receive queue drops. On the
    sink, the stall shows first as `logit.component.buffer.utilization` at `1` with
    `buffer.push.blocked.duration` samples, then as `logit.component.inbox.full` and
    `inbox.blocked.duration` once its inbox fills.
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

- **The per-request retry memory of `otlp_out` and the Datadog sinks is in-process.**
  `otlp_out`, `datadog_out`, and `datadog_trace_out` each send one batch as several requests and
  remember per batch which the destination settled, so a retry resends only the rest ([ADR
  `sink-fault-classes`](../adr/sink-fault-classes.md), "Amendment: `otlp_out` retries per signal
  (2026-10-05)" and "Amendment: the Datadog sinks retry per request (2026-10-09)"). The memory
  lives in `BatchAccounting` (`crates/logit-outputs/src/accounting.rs`), not on disk.
  - **Consequence:** a `buffer.disk:` replay after a crash is a new batch and resends every
    request, accepted ones included (`docs/known-gaps/native-hop.md`, "A disk-backed sink replays
    delivered and dropped batches after a crash"). Under `at_least_once`, the request that drew an
    `Ambiguous` answer is resent too, and may have been applied. Whether either duplicates depends
    on the destination: Datadog stores a resent series point once and a resent log twice
    (`docs/known-gaps/datadog.md`).
  - **Revisit trigger:** an operator who needs a crash replay to skip delivered requests.

- **Every sink that waits for a response, but `logit_out`, sends one request at a time, so over a
  slow link its throughput is one batch per round trip.** `write_loop`
  (`crates/logit-pipeline/src/runtime.rs`) takes the head batch, calls the sink's `send`, and
  takes the next only after `send` returns. It submits
  several batches together only for a sink whose `Output::window` reads above 1, and the default
  is 1. Only `logit_out` overrides it (`window:`, [ADR
  `native-hop-send-window`](../adr/native-hop-send-window.md)). No other sink has a concurrency or
  pipelining field. The limit holds for `otlp_out` over OTLP/HTTP and OTLP/gRPC alike (the pooled
  gRPC client could multiplex, but `send` never has two requests outstanding), `influxdb_out`,
  `datadog_out`, `datadog_trace_out`, `splunk_hec_out`, and `prometheus_out`'s remote-write mode.
  A batch that needs several requests sends them in turn: `otlp_out` one per signal, and the
  Datadog and Splunk sinks one per route or body.
  - **Bound:** a sink moves at most (events per batch) ÷ (round-trip time to a response, plus the
    destination's processing time) events a second. At 50 events per batch and a 200 ms round
    trip that is 250 events a second, with the server's own time on top. A batch is whatever the
    input assembled, because the sink doesn't coalesce.
  - **Symptom:** once the offered rate passes the bound, the sink's buffer fills. Under
    `buffer.overflow: drop_newest` (or `drop_oldest`) the sink counts
    `logit.component.batches.dropped{reason="overflow_newest"}` (`overflow_oldest`), and nothing
    else reports it. Under the default `block` the sources stall instead, and the loss moves to
    whatever feeds them.
  - **Evidence:** the `otlp-fan-out` soak scenario (`docs/plans/soak-harness.md`, "Findings",
    "W13b: fan-out transforms", run `20261018T043911Z`). The generator's
    `otlp_out` offered 500 records a second in batches of 50 over OTLP/HTTP to the system under
    test, with `buffer: { max_batches: 50, overflow: drop_newest }`, and the harness delayed the
    SUT's egress, which carries the responses, by 200 ms ± 50 ms. The generator counted 2,033
    batches (101,650 records) as `overflow_newest` over the run, a fifth of the 483,150 it
    offered, none in the ledger's G. The 10 batches a second offered is twice the 5 a second a
    200 ms round trip allows. Not all of the 101,650 is the delay: the run's pause, stop, and
    partition of the SUT also overflowed the 50-batch buffer, and the plan does not split the
    count by fault.
  - **Workaround:** put the long link under `logit_out` to a `logit_in`, which keeps up to
    `window:` frames in flight, and send from the receiving `logit` to the destination over a
    short hop. Otherwise, move the sink nearer to its destination. Size `buffer.max_batches` and
    `buffer.max_bytes` to ride out a burst, and pick `overflow` for the loss you accept; none
    raises the sustained bound. Fewer, larger batches raise it: a listener that takes
    `receive.batch_max_events` and `batch_max_bytes` assembles them, and an `aggregate` stage
    sends one batch per window.
  - **Revisit trigger:** a destination that is a long round trip away and needs more than a
    batch per round trip, which would mean a window on the shared HTTP driver
    (`crates/logit-outputs/src/http.rs`) and the `submit`/`await_ack` split for it. Concurrent
    requests can arrive out of order, so each sink's posture and each destination's tolerance for
    that need checking first.
- **A `Rejected` batch has no dead-letter path, and no sink splits an oversize batch.** A batch
  the destination rejects for its own content drops at once
  ([ADR `sink-fault-classes`](../adr/sink-fault-classes.md)) and is visible only in
  `batches.dropped{reason="rejected"}` and a throttled log event carrying the destination's text.
  Nothing keeps the batch for inspection or replay. A `413` or equivalent drops the whole batch,
  where Telegraf and libbeat halve it and retry. Only `splunk_hec_out` splits, and only for Splunk
  Cloud's oversize answer.
  - **Workaround:** size the listener's batch bounds below the destination's limit, and read the
    diagnostic for the rejection text.
  - **To close:** a dead-letter sink for rejected batches and a split-and-retry step in the shared
    HTTP driver, each with its own ADR.
