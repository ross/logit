# Known gaps: File and stdio sinks

Entry format and the other areas: [the known-gaps index](README.md).

- **`file_out` rotates and retains by count, but has no SIGHUP/external-rotator reopen, no
  compression, no `max_age`, no timestamped rotated-file naming, and its time-based rotation is
  write-triggered rather than boundary-triggered** (ADR `rotating-file-output`).
  - **Reopen:** `file_out` rotates only a file it opened itself and never re-checks whether its
    path still names the same inode, so an external rotator leaves it writing to the unlinked
    inode, the same gap `stdio_out` has.
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
- **`stdio_out` has no reopen** — it opens a file target once, in append mode, and holds it for
  the process's lifetime. An external log rotator that moves the file leaves `logit` writing to
  the unlinked inode until restart; there's no SIGHUP reopen. That's acceptable for a debugging
  and dev-loop sink.
  - **Workaround:** when a file target needs bounding, use `file_out` (ADR
    `rotating-file-output`), which shares `stdio_out`'s implementation and adds a rotation policy.

  A user-supplied `format:` *template* over the human render has room in the `Format` enum but
  isn't implemented.
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
    rotation can come late. The error carries no `Fault`, so the batch isn't retried.
