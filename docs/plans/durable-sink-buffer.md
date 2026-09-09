---
created: 2026-09-09
updated: 2026-09-09
---

# Closing plan: durable, disk-backed sink buffering

## Context

[ADR `buffered-sink-delivery`](../adr/buffered-sink-delivery.md) put every sink behind a bounded,
in-memory `SinkQueue` (`crates/logit-pipeline/src/queue.rs`) and explicitly rejected disk backing
*for that ADR* because it needed an `EventBatch` serialization that hadn't been decided. That
blocker is gone: [ADR `native-wire-format-encoding`](../adr/native-wire-format-encoding.md)
shipped `logit_proto::native`, whose module doc says outright "this is the same format for a
socket and a file … a file is a plain concatenation of frames — append, sequential read,
`frame::resync` past a torn write," and the in-flight `format: native` work on
`stdio_out`/`file_out` (ADR `file-output-native-format`) is already writing those frames to disk.
`docs/known-gaps.md`'s "No durable (disk-backed) buffering" names this as "real, unblocked
follow-up work, not designed yet." This plan designs and closes it for the sink side.

**Scope.** An opt-in `buffer.disk:` block on any sink. When set, the sink's queue *is* a
disk-backed spool: every batch is appended as a native frame to a segment file before it is
eligible for delivery, the writer reads from a persisted cursor, and a process restart (clean or
SIGKILL) resumes delivery from the last committed cursor, replaying at most the batches committed
since the last cursor checkpoint (at-least-once, the same trade `tail_in`'s checkpoint already
makes). The receive side (`ReceiveQueue`) stays in-memory — a UDP listener's producer is the
kernel, and spilling there is a different design.

## Decisions already settled

| Question | Decision |
|---|---|
| Memory vs disk | **Disk replaces memory for that sink, not a spill tier.** A two-tier "memory until full, then disk" design has to answer how ordering survives a batch sitting in memory while older ones are on disk; it also makes "what survives a crash" depend on timing. Vector's model (a buffer is memory *or* disk) is the precedent. `max_batches`/`max_bytes` (the memory bounds) are rejected when non-default alongside `disk:` (rule-33-style: a knob that would silently do nothing is a config error) |
| What is persisted | One **record** per batch: `[TraceContext: 16-byte trace_id, 8-byte span_id][native frame]`. The context rides inline so a batch replayed within the same process keeps its sink-span parent; after a restart it is still valid (a trace id is just bytes). `frame::resync` still works because it scans for `MAGIC` |
| File layout | `<path>/` is a directory: `segment-<seq:016>.lgit` files, `cursor.json` (`{version, segment, offset, committed_seq}`), written tmp+rename exactly like `crates/logit-inputs/src/tail/checkpoint.rs`. A new segment starts when the active one exceeds `segment_bytes` (default 64 MiB). A segment is deleted once the cursor has passed its end |
| Bound and overflow | `disk.max_bytes` (default 1 GiB) over the sum of segment sizes. `buffer.overflow` keeps its meaning: `block` (default) blocks the push; `drop_oldest` advances the read cursor past whole head frames (counted per frame/event as `batches.dropped{reason="overflow_oldest"}`, exactly the in-memory reason) until it fits; `drop_newest` rejects the push |
| Durability level | `fdatasync` on segment rotation, on cursor write, and on shutdown; **not per push.** Power loss can lose the tail of the active segment; process death cannot lose anything already `write`n (the kernel page cache survives the process). Stated in the ADR and `deploying.md`; per-push fsync is a possible later `disk.sync: every_push` knob, not built |
| Recovery | On open: list segments, load cursor (missing/corrupt → start at the oldest segment, offset 0, diagnosed `cursor_error`, never fatal), validate the active segment frame by frame; a torn tail (`CodecError::Truncated`) is truncated at the last good frame boundary and counted `logit.component.buffer.disk.truncated`; a mid-file CRC failure resyncs forward and counts `batches.dropped{reason="disk_corrupt"}`. Replayed batches count `logit.component.buffer.disk.replayed` |
| Push cost | Push = encode (`NativeEncoder`, the cost `crates/logit-bench` already pins at 23 allocs/event-batch fixture) + `write_all` to the active segment. This breaks the ADR's zero-clone `Arc<EventBatch>` property for disk-backed sinks *by design* — durability is what the operator opted into. Measured and recorded in `memory.md` §2 |
| Peek cost | Peek = read + decode the head frame, **cached** until `commit()` so retries don't re-decode. `write_loop`'s retry loop calls `peek` per attempt; the cache makes that free |
| Runtime seam | `pub enum SinkStore { Memory(SinkQueue), Disk(DiskQueue) }` in `queue.rs` with the four methods `drain_inbox`/`write_loop`/`finish_and_flush` use (`push`, `peek`, `commit`, `close`, plus `len`/`is_empty`). Enum dispatch, no `dyn`, no generic over `run_output`. `NodeSpec::Output` carries a `SinkStoreConfig` |
| Shutdown | With disk, `finish_and_flush` persists the cursor and closes files — it drops **nothing** and counts nothing as `reason="shutdown"`; the shutdown grace only bounds how long `write_loop` keeps *delivering*. The abandoned-inbox sweep in `run_output` still applies (those batches never reached the spool) — unless `disk:` is on, in which case the sweep appends them to the spool instead |
| Frame compression | `disk.compression: none \| lz4` (reuse `logit_config::Compression`), default `none` — disk is cheap, decode latency on the hot path matters more |
| Where | `crates/logit-pipeline/src/disk_queue.rs` (tokio `fs` is already enabled workspace-wide; the crate owns `SinkQueue`, telemetry names, and the runtime). Frame reading helpers (`FrameHeader::read` public, `CodecError::Truncated`) come from the native-transport plan's workstream A — if that hasn't landed, this plan's workstream A makes the same two changes |
| Not a `Buffer<T>` impl | `logit_proto::buffer::Buffer<T>` is sync, `&mut self`, and generic over `T`; a disk queue is async (file I/O) and concrete over `(Arc<EventBatch>, TraceContext)`. `DiskQueue` implements the async `SinkStore` surface directly. The trait stays for `InMemoryBuffer`; the ADR records why the "trait boundary that was cheap to add" turned out to be the wrong seam for disk |

## The constraint everything is designed around

`write_loop` is single-in-flight, in-order, `peek`-then-`commit`-the-head, and every attempt is
raced against a timeout that can cancel the future mid-await. `DiskQueue` must therefore be
cancellation-safe at every `await`: a cancelled `peek` must not leave a half-read cache; a
cancelled `push` must not leave a half-written record. Concretely: `push` encodes fully in memory
first, then does one `write_all` under the queue's mutex and records the new segment length only
after it returns (a torn write on cancellation is exactly what recovery's truncation handles);
`peek` reads into a local buffer and installs the decoded cache in one assignment.

## Reference architecture

```
run_output ── drain_inbox ──push──▶ SinkStore::Disk(DiskQueue)
                                     ├─ active segment (append: [ctx][frame])
                                     ├─ older segments (read-only, deleted after cursor passes)
                                     └─ cursor.json (tmp+rename, on interval + rotation + shutdown)
             write_loop ──peek──▶ head record (cached decode) ──deliver──▶ commit (advance cursor)
```

## Workstream dependency graph

```
A (proto: Truncated + public header read) ── B (DiskQueue) ── C (SinkStore + runtime) ── D (config/graph/build_spec) ── E (restart test, allocations, docs)
```

Strictly sequential; B is the bulk. Land A alone (tiny), B+C together, D, E.

## A. `logit-proto` prerequisites

`CodecError::Truncated { needed: usize }` returned by `read_frame` for a short header/body
instead of `Malformed`; `FrameHeader::read` and `MAX_SANE_UNCOMPRESSED_LEN` `pub`. Shared with
`docs/plans/native-transport.md` workstream A — whichever lands first carries it; the other
rebases. **Test list:** truncated-by-one header and body yield `Truncated` with the right
`needed`; all existing frame tests unchanged.

## B. `DiskQueue`

**Goal:** a cancellation-safe, crash-recoverable spool with the `push`/`peek`/`commit`/`close`
surface, unit-tested with no tokio time and no runtime beyond `#[tokio::test]`.

- `DiskQueueConfig { dir: PathBuf, max_bytes: u64, segment_bytes: u64, overflow: OverflowPolicy,
  compression: Compression, checkpoint_interval: Duration }`.
- `DiskQueue::open(config, telemetry, diag) -> io::Result<Self>` runs recovery.
- Internals: `Mutex<State>` (std mutex, never held across `.await` — file I/O happens on a
  `tokio::fs::File` owned by the state but the lock is released around awaits by taking the
  file out, `Option::take`, the same shape `syslog_out`'s `Conn` uses), `Notify` pair
  (`not_empty`/`not_full`) copied from `BoundedQueue`, `closed: AtomicBool`.
- Segment naming/rotation, cursor persistence (`checkpoint.rs`'s tmp+rename idiom, plus
  `sync_data` on the new file and the directory), deletion of passed segments, `drop_oldest` by
  cursor advance, `drop_newest` rejection, the `Truncated`/resync recovery paths.
- Telemetry (compile-time names, a second `QueueMetrics`-style static): reuse
  `logit.component.buffer.batches`/`.bytes`/`.utilization`/`.push.blocked.duration` with the
  same meanings (bytes = on-disk bytes), plus `logit.component.buffer.disk.segments` (gauge),
  `.disk.replayed` (count, at open), `.disk.truncated` (count), `batches.dropped{reason=
  "disk_corrupt"|"disk_full"}` (`disk_full` = an I/O `ENOSPC` on push under `block`, which
  cannot block: counted and rejected).

**Test list (scratch dir per test via the `file.rs` `scratch_dir` idiom, no `tempfile`):** push
→ peek → commit FIFO across a segment boundary; peek is cached across repeated peeks and
invalidated by commit; drop after reopen replays exactly the uncommitted records; a segment
whose tail is cut mid-record (truncate the file by hand) is recovered to the last good frame and
counted; a corrupted CRC mid-segment is skipped via resync and counted; `drop_oldest` advances
past whole records and reports the right units; `drop_newest` rejects; cursor missing → starts
at oldest; cursor pointing past the end (segment deleted externally) → diagnosed, restarts at
oldest surviving segment; passed segments are deleted after commit crosses them; `max_bytes`
utilization gauge; a `push` future dropped mid-await leaves the spool readable (recovery test
proves it); `close` then `peek` returns `None` when empty.

**Done:** every recovery branch has a test that constructs the on-disk state by hand.

## C. `SinkStore` and the runtime

- `queue.rs`: `pub enum SinkStore { Memory(SinkQueue), Disk(DiskQueue) }` with inherent
  `push/peek/commit/close/len/is_empty` delegating by match; `pub enum SinkStoreConfig {
  Memory(SinkQueueConfig), Disk(DiskQueueConfig) }`.
- `runtime.rs`: `NodeSpec::Output(Box<dyn Output + Send>, SinkStoreConfig, WriteLoopConfig)`;
  `run_output` builds the store (`DiskQueue::open` errors are startup errors, `component '{id}'`
  context); `drain_inbox`/`write_loop`/`finish_and_flush` take `Arc<SinkStore>`; the
  `finish_and_flush` and abandoned-inbox behaviours per the decisions table.
- `logit-bench` callers of `SinkQueue`/`drain_inbox` updated (`Memory` arm).

**Test list:** every existing `runtime.rs` sink test passes with `Memory` unchanged; a
`Disk`-backed `run_output` under `tokio::time::pause()` delivers, commits, and leaves an empty
spool; shutdown mid-backlog persists the cursor and drops nothing (assert
`batches.dropped{reason="shutdown"}` is never emitted for a disk-backed sink and the spool
reopens with the backlog intact); the abandoned-inbox sweep appends to the spool.

## D. Config, graph, `build_spec`

- `crates/logit-config/src/lib.rs`: `BufferConfig.disk: Option<DiskBufferConfig>`;
  `DiskBufferConfig { path: String (required; relative to the config dir like every other path),
  max_bytes: u64 = 1GiB (human_bytes), segment_bytes: u64 = 64MiB, compression: Compression =
  none, checkpoint_interval: Duration = 1s }` with `deny_unknown_fields, default` on the
  optional fields (`path` has no default, so the struct derives `Default` only via a manual
  impl or drops `default` — follow `TlsServerConfig`'s "presence is the on-switch" precedent).
- `graph.rs`: rule 34: with `disk:` set, `max_batches`/`max_bytes` must be default (they are
  ignored); `segment_bytes ≤ max_bytes`; `segment_bytes ≤ 64 MiB` (a frame is ≤
  `MAX_SANE_UNCOMPRESSED_LEN`, and a segment must hold at least one); two sinks may not share a
  `disk.path` (resolve against the config dir and compare canonical paths). Documented in
  `pipeline-graph.md`'s list.
- `pipeline.rs`: `queue_config` returns `SinkStoreConfig`, resolving `path` against `base_dir`.

**Test list:** config round-trips; each rule-34 case; `build_spec` produces a `Disk` store.
**Done:** `script/schema` diff is the new struct only; `script/validate` clean.

## E. Restart test, allocations, docs

- `crates/logit-cli/tests/durable_buffer_restart.rs`: run a graph `fixture input → file_out
  (disk buffer)` where the output is a test `Output` that fails until a flag flips; drop the
  whole runtime mid-backlog (simulating SIGKILL: no shutdown signal, just drop the future); run
  a second graph over the same directory with a succeeding output; assert every batch pushed
  before the drop is delivered exactly once *or* twice only within the last
  `checkpoint_interval` window, never lost, never out of order.
- `crates/logit-bench/tests/allocations.rs`: `disk_queue_push_one_batch` (encode + write) and
  `disk_queue_peek_cached_costs_nothing`; `docs/design/memory.md` §2 rows and §5 rewritten from
  "in-flight memory" to memory *or* disk per sink.
- Docs: ADR `disk-backed-sink-buffer.md` (decisions above; alternatives: spill tier — rejected
  for ordering/crash-semantics ambiguity; a `Buffer<T>` impl — rejected, wrong seam; per-push
  fsync — deferred as a knob; memmap — rejected, no new dependency and no benefit for
  append/sequential-read); `docs/deploying.md` "Sink delivery buffering" gains a "Durable
  buffering" subsection (when to use it, the sizing sentence becomes RAM-or-disk per sink, the
  fsync statement, what to watch, "put it on a volume that survives the container");
  `docs/known-gaps.md`: rewrite the durable-buffering entry to receive-side-only, note the
  power-loss window and the `Buffer<T>` trait's narrowed role; `docs/design/internal-telemetry.md`
  catalog rows; `docs/design/wire-protocol.md` "Buffering" section updated; `AGENTS.md` current
  state; `examples/` — add a commented `disk:` block to `examples/statsd-to-influxdb.yaml`'s
  existing commented `buffer:` reference block.

## Verification, across the whole plan

- `script/cibuild` clean at every step.
- The restart integration test runs 5× consecutively without flakes (it touches real files).
- Manual soak on `compose.yaml`: `influxdb_out` with `disk:`, stop InfluxDB for two minutes,
  `docker kill -s KILL` the `logit` container, start both again → every window's points land
  in InfluxDB (compare counts), `logit.component.buffer.disk.replayed` > 0 once, then 0.
- `script/audit` unchanged (no new dependencies).

## Explicitly out of scope (file in `known-gaps.md`)

Receive-side (`ReceiveQueue`) disk backing; per-push fsync; encryption at rest; a shared spool
across sinks; compaction/rewrite of segments; out-of-order acks (window > 1) — all future.
