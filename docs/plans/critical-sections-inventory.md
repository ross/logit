---
created: 2026-09-20
updated: 2026-09-24
---

# Verification plan: critical sections inventory

Sensitive sections of `logit`: hot paths, hand-rolled logic where no established third-party crate
is doing the work (or one is used in a non-trivial way), `unsafe`/raw syscalls, concurrency and
shutdown ordering, durability, parsers of untrusted input, and accounting that must reconcile.
This is a **work list for future deep-dive verification sessions**, not a list of findings.

- **Surveyed at:** `main` @ `2f387ee` (2026-09-20). Line numbers are pinned to that commit;
  entries also name functions/types — trust the names over the numbers once the code drifts.
- **Method:** nine read-only survey passes, one per area, each reading the code (not just docs)
  and writing entries to a fixed template. Nothing was built, run, or modified. Two areas (CODEC,
  XFORM) got a lighter-weight pass than the concurrency-, durability-, and soundness-heavy ones —
  weight their "none spotted" accordingly.
- **Scope exclusions:** `logit-perf`, `logit-bench` (except its counting allocator's `unsafe`),
  `logit-config` plumbing, generated protobuf, test code. `graph.rs` is covered only where the
  runtime *assumes* a rule holds (RT-14).
- **"Observed concerns" are unverified.** They are leads a surveyor noticed while reading. Some
  will be wrong. A deep-dive session's first job is to refute or confirm them.
- **Totals:** 135 entries — 43 P0, 62 P1, 30 P2. P0 = custom logic on the main data path where
  being wrong means silent loss/duplication/corruption, a crash, a hang, or a remote DoS.

Two corrections to assumptions going in: `graphite/pickle.rs` and `logit-cli/src/pipeline.rs`
contain **no** `unsafe` (grep hits were comments/tests). Production `unsafe` lives in exactly three
places: `logit-inputs/src/udp.rs` (`recvmmsg`, NET-01), `logit-pipeline/src/sockstat.rs`
(`getsockopt`, NET-11), and `logit-inputs/src/tail/watch.rs` (hand-rolled `inotify`, TAIL-07) —
one shared `libc` surface, worth reviewing in one session.

## Top leads (unverified — start here)

The surveyors' highest-value suspicions, roughly by blast radius. Each is detailed under its entry.

| # | Lead | Entry |
|---|---|---|
| 1 | `DdSketch::merge` `.expect()`s matching configs, but sketches arrive decoded from peer bytes over `logit_in` / disk spool — a remote-reachable panic | CORE-05, WIRE-03 |
| 2 | `HyperLogLog::from_bytes` reaches an upstream allocation-layout UB (per `known-gaps.md`) from untrusted native-frame bytes | CORE-06, WIRE-03 |
| 3 | No `http2_max_concurrent_streams` on `otlp_in` or `prometheus_in`'s h2c receiver — per-listener memory worst case is under-estimated by the stream count | WIRE-10, WIRE-11, WIRE-15 |
| 4 | `logit_in` eagerly allocates `vec![0u8; compressed_len]` from the header (64 MiB × 1024 conns, `idle_timeout` off by default) | WIRE-06 |
| 5 | Unbounded recursion: OTLP/JSON `AnyValue` decode (network), and `lua_to_value` / `value_heap_bytes` (script-built nested table; the heap walk runs on queue push) | CODEC-16, CORE-17 |
| 6 | No instruction-count or memory ceiling on a `ScriptWorker` VM — `used_memory()` is observed, never enforced | CORE-15 |
| 7 | A transient `read_dir` failure makes the tail scan return empty → every file `Draining` → re-opened at byte 0: full-file duplicate burst, untested | TAIL-01 |
| 8 | Tail checkpoints and the disk-spool cursor are tmp+rename with **no fsync** (file or directory); a corrupt tail checkpoint falls back to `read_from` (default `End`) → silent *loss* on power failure, contradicting the ADR's "strictly duplicates" | TAIL-05, DISK-06 |
| 9 | `write_record`'s torn-write repair ignores `set_len`'s result yet rewinds in-memory lengths — a failed truncate desynchronizes `len` from the `O_APPEND` file | DISK-03 |
| 10 | Every spool `fsync` and the rotation `create` are `let _ =` — the durability policy is unobservable when it fails | DISK-04 |
| 11 | `drain_inbox` cancelled while parked in `store.push` under `overflow: block` loses one in-hand batch **uncounted**; shutdown's `batches_dropped` log ignores `finish_and_flush` drops | RT-03 |
| 12 | `deliver_with_retry` re-calls `send`, so every sink re-encodes and **re-emits its drop/normalization counters on each retry** — inflating exactly the counters read when a sink is unhealthy | SINK-06, RT-05 |
| 13 | TCP accept loop's `accepted?` makes any `accept()` error (`EMFILE`, `ECONNABORTED`, `ENOBUFS`) fatal to the listener; `logit_in`/`otlp_in` likely share the shape | NET-10, WIRE-07 |
| 14 | One hand-rolled pooled-TCP send machine in three drifting copies (statsd/syslog/graphite): graphite lacks the pre-delivery `flush()`, the `is_tls` guard, and `logit.output.reconnects` | SINK-01 |
| 15 | OTLP decode casts every wire `u64` timestamp `as i64` unguarded — ≥2^63 silently wraps negative (encode side has `.max(0)`) | CODEC-17 |
| 16 | `parse_traceparent` slices a `str` at fixed byte offsets after only a length check — non-ASCII input can panic | CORE-11 |
| 17 | The process-wide interner never evicts and is fed from the network (native dictionary entries, trailer strings, Lua `telemetry` names) | CORE-01, WIRE-02, CORE-19 |
| 18 | `influxdb_out` keeps its own `reqwest` client: default redirect policy (credential-carrying 307/308 replay) and an unbounded error-body read | SINK-08 |
| 19 | Aggregate has two "kept in sync by comment, not compiler" pairs guarded by `unreachable!` (`passes_through`/`Accumulator::new_for`; `flush`'s `retain`/`kind_for_retained`) | XFORM-02, XFORM-03 |
| 20 | Under `DropOldest`, one spool `push` can decode-and-evict a whole segment in one loop, because `total_bytes` shrinks only on segment deletion | DISK-05 |

Repo-wide gaps that cut across entries:

- **No `cargo-fuzz` target exists anywhere in the workspace.** `logit-proto/tests/robustness.rs`
  mutates native/control/graphite/collectd only — statsd, syslog, both Prometheus paths, OTLP/JSON,
  the TCP `Framer`, json/logfmt/csv tokenizers, disk-spool segments, and tail checkpoints have none.
- **Cancellation is the least-tested axis.** The runtime drops `send`/`push` futures mid-flight by
  design; no test drops one inside `write_all`, the UDP datagram loop, or a parked `store.push`.
- **Dependency bumps are re-verification triggers**: `logit-inputs/src/http.rs`'s idle/graceful
  shutdown driver is pinned by reference to hyper 1.11.1 / hyper-util 0.1.20 internals;
  `BoundedQueue::close` leans on a tokio `notify_waiters` internal; `logit_out`'s `Clean` vs
  `Ambiguous` fault split rests on an unverified `tokio-rustls` write-semantics assumption; the HLL
  codec's soundness rests on serde's `with_capacity(size_hint)` behaviour.
- **Connection gauges are decremented by a bare statement, not a drop guard**, in all three stream
  listeners — leaks on panic.
- No TLS certificate reload exists anywhere, and it is not recorded in `docs/known-gaps.md`.

## Suggested session clusters

Entries that share a mechanism and should be verified together, in suggested order:

1. **Remote-reachable crash/DoS** — CORE-05, CORE-06, WIRE-01..03, WIRE-06, WIRE-10/11/15,
   CODEC-16, CODEC-17. Mostly fuzz targets + size/depth caps; highest severity, most mechanical.
2. **Durability** — DISK-01..06, DISK-09, DISK-13, TAIL-05, DISK-10. One crash-injection harness
   serves all of it; settle the fsync policy (tmp file + directory) once for spool *and* checkpoints.
3. **Shared queue + shutdown** — NET-06, NET-07, RT-07, DISK-08, RT-02..04, NET-02/03, TAIL-06/08.
   One loom/shuttle model of `BoundedQueue` (incl. the `peek`/`commit` head reservation), then a
   cancellation-safety audit of every `select!`.
4. **Tail bookkeeping** — TAIL-01..04, TAIL-09, TAIL-10. Proptest state machine against a model
   filesystem (rename/copytruncate/delete/transient-error), then a real `logrotate` run.
5. **`libc` surface** — NET-01, NET-11, NET-12, TAIL-07. miri where possible, strace otherwise.
6. **Sink send path** — SINK-01..06, WIRE-08/09, RT-05. Mechanical diff of the three copies first,
   then fault injection (RST mid-write, blackhole, close_notify), then the retry-counter question.
7. **Lua boundary** — CORE-15..19, RT-11. Adversarial scripts: re-entrancy under a held `RefCell`
   borrow, a proxy held past its scope, infinite loop, deep/huge table.
8. **Aggregate** — XFORM-01..04, CORE-07. Proptest against a naive reference aggregator, merge
   laws (associativity/commutativity) for every mergeable kind, cardinality-cap soak.
9. **Untrusted-input parsers** — NET-08, CODEC-01..03/05/07/10/12/13, XFORM-06/08. One fuzz target
   each; differential tests against reference implementations to inform committed fixtures.
10. Everything else P1, then P2.

## How to use this list

Each entry below is a starting point for one deep-dive verification session. The survey that
produced it *identified and characterized* the code; it did not verify it. "Observed concerns" are
leads a surveyor noticed while reading, explicitly unverified — treat them as hypotheses to refute
or confirm, not findings.

### Status tracking

The index table carries a **Status** column. Values:

| Status | Meaning |
|---|---|
| `unreviewed` | Surveyed only; nobody has deep-dived it |
| `in-progress (<branch>)` | A session/branch is on it |
| `reviewed @<sha>` | Deep-dived at that commit, no findings that needed a change |
| `findings → <PR/issue>` | Deep-dived, produced changes; link them |
| `stale` | Code under the entry changed materially since its last review — re-review |

Update the row in the same PR that lands the session's artifact. A `reviewed @<sha>` row goes
`stale` when `git log <sha>..HEAD -- <entry's paths>` shows a non-trivial change.

### Running a deep-dive session

- **One session per entry or small cluster, not per file.** The entries are split by mechanism on
  purpose; a session that tries to cover all of `runtime.rs` will skim.
- **Every session ends in a committed artifact** — a new test, a fuzz target, a proptest, a
  fault-injection harness, an ADR amendment, or a `docs/known-gaps.md` entry. A prose-only
  "looked fine" rots; the only acceptable prose-only outcome is the `reviewed @<sha>` status row
  plus a sentence on *what was checked and how*.
- **Verify adversarially, in a fresh context.** The reviewer's brief is to *refute* the entry's
  invariants, not confirm them, and it should not be the session (or ideally the model) that wrote
  the code. Past experience in this repo: a recorded real-client corpus found a Prometheus
  assembler bug that the proptests written alongside the code could not.
- **Prefer executable verification to reading.** In rough order of value per hour:
  - `cargo-fuzz` targets for every decoder of untrusted input (statsd, syslog, collectd, carbon
    plaintext + pickle, Prometheus text + remote-write, OTLP/JSON, native frames, disk-spool
    segments, tail checkpoints).
  - A crash-injection harness for the disk spool, tail checkpoints, and file rotation: kill -9 at
    every write/fsync/rename boundary, restart, assert the no-loss / bounded-duplication contract.
  - Differential tests against the reference implementation (real collectd, carbon, Python
    `pickle`, Prometheus `expfmt`, an OTel collector, `grpcurl`) — informing a committed fixture,
    never a test that needs the service running (AGENTS.md's fixture rule).
  - A mechanical diff of the "ported verbatim" TCP/TLS arrangements across sinks; divergence
    between copies is either a bug in one or an undocumented decision.
  - `miri` over every `unsafe` site that can run under it; `loom`/`shuttle` or tokio paused-time
    tests for the queue and shutdown orderings; a cancellation-safety audit of every `select!`.
  - Fault injection at the socket layer: peer RST mid-write, blackholed peer, slowloris, truncated
    frame, oversized length prefix, TLS close_notify.
- **Reconcile the counters.** For any path with drop accounting, a verification run should end
  with `received == delivered + Σ(counted drops)`; an unexplained remainder is a finding even if
  no test failed.

### Keeping the list current

- Line numbers are pinned to the commit in the header. Entries also name functions/types so they
  survive drift; trust the names over the numbers.
- After each large workstream lands, re-run the survey scoped to the touched paths (one sub-agent,
  same entry template) and merge: new entries arrive `unreviewed`, touched entries go `stale`.
- A new component that adds a hand-rolled codec, a new socket driver, persistence, or `unsafe`
  should add its own entry in the PR that introduces it.

## Index

Sorted by priority, then area. Update **Status** in the PR that lands a session's artifact.

| ID | Pri | Section | Primary location | Status |
|---|---|---|---|---|
| [NET-01](#net-01--recvmmsg2-batched-udp-read-hand-built-mmsghdriovec-arrays-over-vecu64-storage) | P0 | `recvmmsg(2)` batched UDP read: hand-built `mmsghdr`/`iovec` arrays over `Vec<u64>` storage | `crates/logit-inputs/src/udp.rs` (`BatchReader`, `build_headers`/`recvmmsg_into`/`harvest_headers`) | findings → libc/w1 |
| [NET-02](#net-02--udp-read_loop-shutdown-race-queue-close-contract-and-per-batch-telemetry) | P0 | UDP `read_loop`: shutdown race, queue-close contract, and per-batch telemetry | `crates/logit-inputs/src/udp.rs:556-592` | unreviewed |
| [NET-03](#net-03--udp-decode_loop-pop_many-batching-interval-flush-deadline-race-and-final-flush-ordering) | P0 | UDP `decode_loop`: `pop_many` batching, interval-flush deadline race, and final flush ordering | `crates/logit-inputs/src/udp.rs:1119-1217` | unreviewed |
| [NET-06](#net-06--boundedqueuepush_many-batched-admission-control-the-pre-wait-notify-and-cancellation) | P0 | `BoundedQueue::push_many`: batched admission control, the pre-wait notify, and cancellation | `crates/logit-pipeline/src/queue.rs:382-469` | unreviewed |
| [NET-07](#net-07--boundedqueuepop_many--pop--close-cancellation-safety-and-the-closed-and-empty-signal) | P0 | `BoundedQueue::pop_many` / `pop` / `close`: cancellation safety and the closed-and-empty signal | `crates/logit-pipeline/src/queue.rs:497-522` | unreviewed |
| [NET-08](#net-08--tcp-framer-rfc-6587-auto-detect-latch-lf-lines-with-drain-resync-and-the-4-byte-length-prefix) | P0 | TCP `Framer`: RFC 6587 auto-detect latch, LF lines with drain-resync, and the 4-byte length prefix | `crates/logit-inputs/src/tcp.rs:295-710` | unreviewed |
| [TAIL-01](#tail-01--rotation--truncation--removal-reconciliation-in-scan) | P0 | Rotation / truncation / removal reconciliation in `scan` | `crates/logit-inputs/src/tail/driver.rs:350-424` | unreviewed |
| [TAIL-02](#tail-02--start-offset-selection-inode-rebinding-and-the-resume-map) | P0 | Start-offset selection, inode rebinding, and the `resume` map | `crates/logit-inputs/src/tail/driver.rs:525-628` | unreviewed |
| [TAIL-03](#tail-03--read--split--decode--batch-hot-loop-and-its-backpressure-contract) | P0 | Read → split → decode → batch hot loop, and its backpressure contract | `crates/logit-inputs/src/tail/driver.rs:636-661` | unreviewed |
| [TAIL-04](#tail-04--linesplitter-framing-partial-carry-over-and-max_line_bytes-drop-semantics) | P0 | `LineSplitter`: framing, partial carry-over, and `max_line_bytes` drop semantics | `crates/logit-inputs/src/tail/line.rs:77-162` | unreviewed |
| [TAIL-05](#tail-05--checkpoint-persistence-atomicity-durability-and-the-corrupt-file-fallback) | P0 | Checkpoint persistence: atomicity, durability, and the corrupt-file fallback | `crates/logit-inputs/src/tail/checkpoint.rs:59-158` | findings → dur/w6 |
| [TAIL-09](#tail-09--docker-json-file-envelope-decode-and-16-kib-partial-line-reassembly) | P0 | Docker json-file envelope decode and 16 KiB partial-line reassembly | `crates/logit-inputs/src/docker.rs:141-154` | unreviewed |
| [DISK-01](#disk-01--diskqueueopen--crash-recovery-torn-tail-truncation-cursor-reconciliation) | P0 | DiskQueue::open — crash recovery, torn-tail truncation, cursor reconciliation | `crates/logit-pipeline/src/disk_queue.rs:378-559` | in-progress (dur/w3) |
| [DISK-02](#disk-02--record-format-parse_record-and-walk_segments-resync-scan) | P0 | Record format, `parse_record`, and `walk_segment`'s resync scan | `crates/logit-pipeline/src/disk_queue.rs:54-63` | in-progress (dur/w3) |
| [DISK-03](#disk-03--diskqueuepush--write_record--torn-write-repair-write_in_flight-cancellation-safety) | P0 | `DiskQueue::push` / `write_record` — torn-write repair, `write_in_flight`, cancellation safety | `crates/logit-pipeline/src/disk_queue.rs:597-729` | in-progress (dur/w4) |
| [DISK-06](#disk-06--read-cursor-rollover-segment-deletion-and-checkpoint-cadence) | P0 | Read cursor rollover, segment deletion, and checkpoint cadence | `crates/logit-pipeline/src/disk_queue.rs:1009-1062` | in-progress (dur/w5) |
| [DISK-09](#disk-09--sink-shutdown-ordering-run_outputs-close-then-sweep-sinkstorefinish-and-the-at-least-once-window) | P0 | Sink shutdown ordering: `run_output`'s close-then-sweep, `SinkStore::finish`, and the at-least-once window | `crates/logit-pipeline/src/runtime.rs:588-744` | in-progress (dur/w5) |
| [RT-01](#rt-01--startup-orchestration-bind-pre-pass-channelfanout-construction-spawn-loop-scaffolding-drop) | P0 | Startup orchestration: bind pre-pass, channel/Fanout construction, spawn loop, scaffolding drop | `crates/logit-pipeline/src/runtime.rs:174-536` | unreviewed |
| [RT-02](#rt-02--shutdown-signalling-grace-anchoring-and-the-join-loops-first-error-cascade) | P0 | Shutdown signalling, grace anchoring, and the join loop's first-error cascade | `runtime.rs:198-232` | unreviewed |
| [RT-03](#rt-03--run_outputs-drainwrite-join-the-abandoned-inbox-sweep-and-finish_and_flush-ordering) | P0 | `run_output`'s drain/write join, the abandoned-inbox sweep, and `finish_and_flush` ordering | `runtime.rs:573-744` | unreviewed |
| [RT-04](#rt-04--write_loop-peekcommit-delivery-permanent-failure-window-degradedrecovered-edges) | P0 | `write_loop`: peek/commit delivery, permanent-failure window, degraded/recovered edges | `runtime.rs:1042-1218` | unreviewed |
| [RT-11](#rt-11--lua-node-hosting-os-thread-two-oneshot-handshake-catch_unwind-handleblock_on) | P0 | Lua node hosting: OS thread, two-oneshot handshake, `catch_unwind`, `Handle::block_on` | `runtime.rs:1673-1779` | unreviewed |
| [WIRE-01](#wire-01--frame-envelope-24-byte-header-crc-32c-over-compressed-bytes-lz4-bounds-resync) | P0 | Frame envelope: 24-byte header, CRC-32C over compressed bytes, lz4 bounds, resync | `crates/logit-proto/src/frame.rs:24-55` | unreviewed |
| [WIRE-02](#wire-02--dictionary-first-symbol-table-and-value-tlv-decode-untrusted-counts-depth-interning) | P0 | Dictionary-first symbol table and `Value` TLV decode (untrusted counts, depth, interning) | `crates/logit-proto/src/native/dict.rs:18-52` | unreviewed |
| [WIRE-03](#wire-03--record-tlv-decode-default-elision-encoding-required-fields-and-opaque-sketch-blobs) | P0 | Record TLV decode: default-elision encoding, required fields, and opaque sketch blobs | `crates/logit-proto/src/native/record.rs:49-101` | unreviewed |
| [WIRE-05](#wire-05--control-message-tlv-and-the-hellohelloack-negotiation-state-machine) | P0 | Control-message TLV and the `Hello`/`HelloAck` negotiation state machine | `crates/logit-proto/src/native/control.rs:24-49` | unreviewed |
| [WIRE-06](#wire-06--logit_in-per-connection-frame-loop-eager-body-allocation-idle-bounds-ack-as-backpressure) | P0 | `logit_in` per-connection frame loop: eager body allocation, idle bounds, ack-as-backpressure | `crates/logit-inputs/src/logit.rs:446-600` | unreviewed |
| [WIRE-08](#wire-08--logit_out-send-path-one-frame-in-flight-partial-write-semantics-fault-classification) | P0 | `logit_out` send path: one-frame-in-flight, partial-write semantics, fault classification | `crates/logit-outputs/src/logit.rs:78-122` | unreviewed |
| [WIRE-10](#wire-10--hand-rolled-grpc-server-framing-length-prefixed-messages-trailers-gzip-bounds) | P0 | Hand-rolled gRPC server framing: length-prefixed messages, trailers, gzip bounds | `crates/logit-inputs/src/otlp.rs:741-816` | unreviewed |
| [WIRE-11](#wire-11--shared-hyper-connection-lifecycle-idle-tracking-graceful-shutdown-body-stall-bounds) | P0 | Shared hyper connection lifecycle: idle tracking, graceful shutdown, body stall bounds | `crates/logit-inputs/src/http.rs:33-94` | unreviewed |
| [WIRE-15](#wire-15--prometheus_in-remote-write-receiver-ingress-permits-deadlines-body-limits-snappy-bounds-version-dispatch) | P0 | `prometheus_in` remote-write receiver ingress: permits, deadlines, body limits, snappy bounds, version dispatch | `crates/logit-inputs/src/prometheus.rs:800-816` | unreviewed |
| [CODEC-16](#codec-16--otlpjson-anyvalue-decode--unbounded-recursion-on-attacker-controlled-nesting) | P0 | OTLP/JSON `AnyValue` decode — unbounded recursion on attacker-controlled nesting | `crates/logit-proto/src/otlp/json/mod.rs:309-347` | unreviewed |
| [CORE-05](#core-05--ddsketch-wrapper-merge-panics-on-a-config-mismatch-reachable-from-the-wire) | P0 | `DdSketch` wrapper: `merge` panics on a config mismatch reachable from the wire | `crates/logit-core/src/metric.rs:302-410` | unreviewed |
| [CORE-06](#core-06--hyperloglog-hand-rolled-serde-byte-codec-working-around-an-upstream-allocation-layout-ub) | P0 | `HyperLogLog`: hand-rolled serde byte codec working around an upstream allocation-layout UB | `crates/logit-core/src/metric.rs:412-516` | unreviewed |
| [CORE-15](#core-15--scriptworker-vm-lifecycle-the-luajit-sandbox-and-return-value-validation) | P0 | `ScriptWorker`: VM lifecycle, the LuaJIT sandbox, and return-value validation | `crates/logit-script/src/lib.rs:44-46` | unreviewed |
| [CORE-16](#core-16--eventproxy-handle-lifetime-registry-caches-the-no-clone-fast-path-and-metricproxys-weak) | P0 | `EventProxy` handle lifetime: registry caches, the no-clone fast path, and `MetricProxy`'s `Weak` | `crates/logit-script/src/proxy.rs:99-173` | unreviewed |
| [CORE-17](#core-17--lua-attribute-writes-refcell-borrow-discipline-value-identity-preservation-and-unbounded-table-recursion) | P0 | Lua attribute writes: `RefCell` borrow discipline, value-identity preservation, and unbounded table recursion | `crates/logit-script/src/proxy.rs:481-524` | unreviewed |
| [XFORM-02](#xform-02--aggregate-per-event-merge-dispatch-process) | P0 | Aggregate: per-event merge dispatch (`process`) | `crates/logit-transforms/src/aggregate.rs:473-914` | unreviewed |
| [XFORM-03](#xform-03--aggregate-flush-series-retention-and-the-cardinality-cap) | P0 | Aggregate: flush, series retention, and the cardinality cap | `crates/logit-transforms/src/aggregate.rs:946-1159` | unreviewed |
| [SINK-01](#sink-01--the-copied-pooled-tcp-send-path-statsd--syslog--graphite--probe-one-write-then-write_all-one-reconnect) | P0 | The copied pooled-TCP send path (statsd / syslog / graphite) — probe, one-write-then-write_all, one reconnect | `crates/logit-outputs/src/statsd.rs:2068-2211` | unreviewed |
| [SINK-04](#sink-04--udp-datagram-packing-emsgsize-handling-and-partial-batch-fault-classification) | P0 | UDP datagram packing, `EMSGSIZE` handling, and partial-batch fault classification | `crates/logit-outputs/src/statsd.rs:1963-2066` | unreviewed |
| [SINK-05](#sink-05--the-output-trait-contract-each-sink-relies-on-retry-posture-cancellation-shutdown) | P0 | The `Output` trait contract each sink relies on (retry, posture, cancellation, shutdown) | `crates/logit-pipeline/src/output.rs:18-104` | unreviewed |
| [SINK-09](#sink-09--allocate_timestamp--the-per-series-union-find-collision-allocator-behind-duplicate_safe--true) | P0 | `allocate_timestamp` — the per-series union-find collision allocator behind `duplicate_safe() == true` | `crates/logit-outputs/src/influxdb.rs:465-577` | unreviewed |
| [NET-04](#net-04--udplistenerrun_until_shutdown-the-readdecode-two-future-select-and-double-poll-guard) | P1 | `UdpListener::run_until_shutdown`: the read/decode two-future select and double-poll guard | `crates/logit-inputs/src/udp.rs:305-363` | unreviewed |
| [NET-09](#net-09--tcp-serve_connection-the-shared-next-byte-deadline-idle-close-policy-and-end-of-connection-flushes) | P1 | TCP `serve_connection`: the shared next-byte deadline, idle-close policy, and end-of-connection flushes | `crates/logit-inputs/src/tcp.rs:1264-1491` | unreviewed |
| [NET-10](#net-10--tcp-accept-loop-connection-cap-permit-lifetime-per-connection-spawn-and-the-live-connections-gauge) | P1 | TCP accept loop: connection cap, permit lifetime, per-connection spawn, and the live-connections gauge | `crates/logit-inputs/src/tcp.rs:1077-1205` | unreviewed |
| [NET-11](#net-11--sockstat-raw-getsockoptso_meminfo--getsockopttcp_info-and-the-wrapping-drop-counter) | P1 | `sockstat`: raw `getsockopt(SO_MEMINFO)` / `getsockopt(TCP_INFO)` and the wrapping drop counter | `crates/logit-pipeline/src/sockstat.rs` (`meminfo`/`listen_queue`) | findings → libc/w2 |
| [NET-12](#net-12--the-two-kernel-samplers-coop-budget-arm-ordering-self-disable-and-the-guaranteed-final-sample) | P1 | The two kernel samplers: coop-budget arm ordering, self-disable, and the guaranteed final sample | `crates/logit-inputs/src/udp.rs` (`sample_while`, `ReceiveBufferSampler`), `crates/logit-inputs/src/tcp.rs` (`AcceptQueueSampler`) | findings → libc/w1, libc/w2 |
| [TAIL-06](#tail-06--shutdown-ordering-and-final-flush-of-held-state) | P1 | Shutdown ordering and final flush of held state | `crates/logit-inputs/src/tail/driver.rs:312-321` | unreviewed |
| [TAIL-07](#tail-07--hand-rolled-inotify-backend-every-unsafesyscall-site-in-this-area) | P1 | Hand-rolled `inotify` backend: every `unsafe`/syscall site in this area | `crates/logit-inputs/src/tail/watch.rs:236-501` | findings → libc/w3 |
| [TAIL-08](#tail-08--the-runtime-select-wake-routing-timers-and-cancellation-safety) | P1 | The runtime `select!`: wake routing, timers, and cancellation safety | `crates/logit-inputs/src/tail/driver.rs:229-321` | unreviewed |
| [TAIL-10](#tail-10--configv2json-identity-cache-refresh-and-de-selection) | P1 | `config.v2.json` identity cache, refresh, and de-selection | `crates/logit-inputs/src/docker.rs:325-346` | unreviewed |
| [DISK-04](#disk-04--segment-rotation-fsync-policy-and-finish) | P1 | Segment rotation, fsync policy, and `finish` | `crates/logit-pipeline/src/disk_queue.rs:298-300` | findings → #324 |
| [DISK-05](#disk-05--overflow-policy-eviction-and-drop-accounting-on-the-spool) | P1 | Overflow policy, eviction, and drop accounting on the spool | `crates/logit-pipeline/src/disk_queue.rs:616-705` | in-progress (dur/w4) |
| [DISK-07](#disk-07--peek--read_record_at--read_at--the-delivery-read-path-and-live-corruption-resync) | P1 | `peek` / `read_record_at` / `read_at` — the delivery read path and live corruption resync | `crates/logit-pipeline/src/disk_queue.rs:1085-1140` | unreviewed |
| [DISK-08](#disk-08--notifyclosed-wakeup-protocol-and-the-mutex-poison-posture) | P1 | `Notify`/`closed` wakeup protocol and the `Mutex`-poison posture | `crates/logit-pipeline/src/disk_queue.rs:352-370` | unreviewed |
| [DISK-10](#disk-10--file_out-rotation-commit-point-first-rename-staging-recovery-retention-cascade) | P1 | `file_out` rotation: commit-point-first rename, staging recovery, retention cascade | `crates/logit-outputs/src/file.rs:281-297` | in-progress (dur/w7) |
| [DISK-13](#disk-13--logit_protoframe-as-the-disk-record-envelope--sanity-caps-crc-lz4-resync) | P1 | `logit_proto::frame` as the disk record envelope — sanity caps, CRC, lz4, `resync` | `crates/logit-proto/src/frame.rs:24-62` | in-progress (dur/w2) |
| [RT-05](#rt-05--deliver_with_retry-and-backoff_for-budget-enforcement-and-doubling-schedule) | P1 | `deliver_with_retry` and `backoff_for`: budget enforcement and doubling schedule | `runtime.rs:874-928` | unreviewed |
| [RT-06](#rt-06--fanout-clone-vs-move-on-the-last-edge-provenance-stamping-closed-consumer-accounting) | P1 | `Fanout`: clone-vs-move on the last edge, provenance stamping, closed-consumer accounting | `crates/logit-pipeline/src/fanout.rs:167-414` | unreviewed |
| [RT-07](#rt-07--sinkqueue--boundedqueue-the-notify-condvar-pattern-blocking-push-close-semantics) | P1 | `SinkQueue` / `BoundedQueue`: the `Notify` condvar pattern, blocking push, close semantics | `crates/logit-pipeline/src/queue.rs:147-182` | unreviewed |
| [RT-08](#rt-08--run_transform-flush-deadline-race-close-time-flush-and-cadence-math) | P1 | `run_transform`: flush-deadline race, close-time flush, and cadence math | `runtime.rs:1255-1333` | unreviewed |
| [RT-10](#rt-10--run_router--route_batch-the-four-pass-partition-and-routerscratch-reuse) | P1 | `run_router` / `route_batch`: the four-pass partition and `RouterScratch` reuse | `runtime.rs:1389-1468` | unreviewed |
| [RT-12](#rt-12--batchaccumulator-incremental-weight-tracking-and-the-resource-scope-key) | P1 | `BatchAccumulator`: incremental weight tracking and the `(resource, scope)` key | `crates/logit-pipeline/src/accumulator.rs:53-259` | unreviewed |
| [RT-14](#rt-14--graph-rules-the-runtime-assumes-cycle-detection-target-arity-slot-order) | P1 | Graph rules the runtime *assumes* (cycle detection, target arity, slot order) | `crates/logit-pipeline/src/graph.rs:2936-3032` | unreviewed |
| [WIRE-04](#wire-04--batch-framing-v1v2-and-the-mandatory-provenance-trailer) | P1 | Batch framing v1/v2 and the mandatory provenance trailer | `crates/logit-proto/src/native/mod.rs:49-67` | unreviewed |
| [WIRE-07](#wire-07--logit_in-accept-loop-connection-cap-bounded-tls-accept-live-connection-accounting) | P1 | `logit_in` accept loop: connection cap, bounded TLS accept, live-connection accounting | `crates/logit-inputs/src/logit.rs:244-361` | unreviewed |
| [WIRE-09](#wire-09--pooled-connection-close-probe-stream-erasure-and-sni-derivation) | P1 | Pooled-connection close probe, stream erasure, and SNI derivation | `crates/logit-outputs/src/tls.rs:29-30` | unreviewed |
| [WIRE-12](#wire-12--otlp_out-grpc-round-trip-over-a-pooled-hyper-utilhyper-rustls-client-and-the-fault-table) | P1 | `otlp_out` gRPC round trip over a pooled hyper-util/hyper-rustls client, and the fault table | `crates/logit-outputs/src/otlp.rs:304-379` | unreviewed |
| [WIRE-13](#wire-13--tls-configuration-construction-private-ca-mtls-and-insecure_skip_verify) | P1 | TLS configuration construction: private CA, mTLS, and `insecure_skip_verify` | `crates/logit-inputs/src/tls.rs:49-90` | unreviewed |
| [WIRE-14](#wire-14--prometheus_in-scrape-loop-per-tick-fan-out-per-target-body-cap-outcome-bookkeeping) | P1 | `prometheus_in` scrape loop: per-tick fan-out, per-target body cap, outcome bookkeeping | `crates/logit-inputs/src/prometheus.rs:347` | unreviewed |
| [WIRE-16](#wire-16--prometheus_in-metadata-cache-one-blocking-mutex-an-arc-seed-an-expiry-watermark-lru-cap) | P1 | `prometheus_in` metadata cache: one blocking mutex, an `Arc` seed, an expiry watermark, LRU cap | `crates/logit-inputs/src/prometheus.rs:824-840` | unreviewed |
| [WIRE-17](#wire-17--prometheus_in--written--logitinputsamples-reconciliation) | P1 | `prometheus_in` `-Written` / `logit.input.samples` reconciliation | `crates/logit-inputs/src/prometheus.rs:1653-1694` | unreviewed |
| [WIRE-18](#wire-18--prometheus_out-exposition-registry-upsert-type-conflict-expiry-sweep-one-pass-cap) | P1 | `prometheus_out` exposition registry: upsert, type conflict, expiry sweep, one-pass cap | `crates/logit-outputs/src/prometheus.rs:346-379` | unreviewed |
| [WIRE-19](#wire-19--prometheus_out-exposition-http-server-two-deadlines-and-synchronous-render--gzip-on-the-runtime) | P1 | `prometheus_out` exposition HTTP server: two deadlines, and synchronous render + gzip on the runtime | `crates/logit-outputs/src/prometheus.rs:276-313` | unreviewed |
| [CODEC-01](#codec-01--hand-rolled-restricted-pickle-stack-machine-reader-opcode-allowlist) | P1 | Hand-rolled restricted pickle stack-machine reader (opcode allowlist) | `crates/logit-proto/src/graphite/pickle.rs:258-542` | unreviewed |
| [CODEC-02](#codec-02--historical-pickle-memo-growth-dos-fixed-regression-sensitive) | P1 | Historical pickle memo-growth DoS (fixed, regression-sensitive) | `crates/logit-proto/src/graphite/pickle.rs:667-694` | unreviewed |
| [CODEC-03](#codec-03--carbon-plaintextpickle-decode-entry-point-and-timestamp-arithmetic) | P1 | Carbon plaintext/pickle decode entry point and timestamp arithmetic | `crates/logit-proto/src/graphite/decode.rs:147-297` | unreviewed |
| [CODEC-05](#codec-05--dogstatsdstatsd-line-decoder--per-line-dispatch-and-event-text-unescaping) | P1 | DogStatsD/statsd line decoder — per-line dispatch and event-text unescaping | `crates/logit-inputs/src/statsd.rs:538-576` | unreviewed |
| [CODEC-07](#codec-07--rfc-31645424-syslog-parser--pritimestamp-framing-and-dialect-sniffing) | P1 | RFC 3164/5424 syslog parser — PRI/TIMESTAMP framing and dialect sniffing | `crates/logit-inputs/src/syslog.rs:590-672` | unreviewed |
| [CODEC-10](#codec-10--collectd-binary-decoder--tlv-part-framing-and-the-values-part-lengthcount-gate) | P1 | collectd binary decoder — TLV part framing and the Values-part length/count gate | `crates/logit-proto/src/collectd/part.rs:139-153` | unreviewed |
| [CODEC-12](#codec-12--prometheus-textopenmetrics-decoder--line-grammar-family-assembler-and-cumulative-bucket-reconstruction) | P1 | Prometheus text/OpenMetrics decoder — line grammar, family assembler, and cumulative-bucket reconstruction | `crates/logit-proto/src/prometheus/text.rs:185-356` | unreviewed |
| [CODEC-13](#codec-13--prometheus-remote-write-decoder--snappy-decompression-bomb-guard-and-the-20-symbol-table-indirection) | P1 | Prometheus remote-write decoder — Snappy decompression-bomb guard and the 2.0 symbol-table indirection | `crates/logit-inputs/src/prometheus.rs:1581-1615` | unreviewed |
| [CODEC-14](#codec-14--influxdb-line-protocol-encoder--collision-avoiding-timestamp-allocator-and-fieldtag-escaping) | P1 | InfluxDB line-protocol encoder — collision-avoiding timestamp allocator and field/tag escaping | `crates/logit-outputs/src/influxdb.rs:579-617` | unreviewed |
| [CODEC-17](#codec-17--otlp-decode--unguarded-u64-as-i64-timestamp-cast-on-every-wire-timestamp-field-logstracesmetrics) | P1 | OTLP decode — unguarded `u64 as i64` timestamp cast on every wire timestamp field (logs/traces/metrics) | `crates/logit-proto/src/otlp/logs.rs:256-259` | unreviewed |
| [CORE-01](#core-01--process-wide-symbol-interner-unbounded-growth-and-per-call-shard-contention) | P1 | Process-wide symbol interner: unbounded growth and per-call shard contention | `crates/logit-core/src/interner.rs:12-51` | unreviewed |
| [CORE-02](#core-02--keycache-hand-rolled-cursor-scan-memo-in-front-of-the-interner) | P1 | `KeyCache`: hand-rolled cursor-scan memo in front of the interner | `crates/logit-core/src/interner.rs:86-146` | unreviewed |
| [CORE-03](#core-03--attrmap-sorted-inline-smallvec-and-the-resourceevent-merge-join) | P1 | `AttrMap`: sorted inline `SmallVec` and the resource⊕event merge-join | `crates/logit-core/src/attrs.rs:16-89` | unreviewed |
| [CORE-07](#core-07--samples-attacker-influenced-sample-rate-extrapolation) | P1 | `Samples`: attacker-influenced sample-rate extrapolation | `crates/logit-core/src/metric.rs:178-249` | unreviewed |
| [CORE-08](#core-08--telemetry-component-buffers-locks-bounded-caps-and-drop-accounting) | P1 | Telemetry component buffers: locks, bounded caps, and drop accounting | `crates/logit-core/src/telemetry.rs:41-99` | unreviewed |
| [CORE-09](#core-09--telemetrylayer-capturing-tracing-back-into-the-pipeline-feedback-loop-and-field-extraction) | P1 | `TelemetryLayer`: capturing `tracing` back into the pipeline (feedback loop and field extraction) | `crates/logit-core/src/telemetry.rs:832-891` | unreviewed |
| [CORE-10](#core-10--deterministic-span-sampling-and-spanguard-span-minting) | P1 | Deterministic span sampling and `SpanGuard` span minting | `crates/logit-core/src/telemetry.rs:94-128` | unreviewed |
| [CORE-12](#core-12--hand-rolled-rfc-3339-formattingparsing-and-exact-decimal-to-nanos) | P1 | Hand-rolled RFC 3339 formatting/parsing and exact decimal-to-nanos | `crates/logit-core/src/time.rs:21-39` | unreviewed |
| [CORE-18](#core-18--eventnewt-building-a-whole-event-from-an-untrusted-shape-lua-table) | P1 | `Event.new(t)`: building a whole `Event` from an untrusted-shape Lua table | `crates/logit-script/src/construct.rs:53-126` | unreviewed |
| [CORE-19](#core-19--the-lua-telemetry-global-script-strings-into-the-process-interner) | P1 | The Lua `telemetry` global: script strings into the process interner | `crates/logit-script/src/telemetry.rs:33-42` | unreviewed |
| [XFORM-01](#xform-01--aggregate-serieskey-identity-hashing-and-grouping) | P1 | Aggregate: SeriesKey identity, hashing, and grouping | `crates/logit-transforms/src/aggregate.rs:1320-1469` | unreviewed |
| [XFORM-04](#xform-04--aggregate-cumulative-temporality-and-counter-reset-semantics) | P1 | Aggregate: cumulative temporality and counter-reset semantics | `crates/logit-transforms/src/aggregate.rs:1-47` | unreviewed |
| [XFORM-06](#xform-06--jsonrs-zero-copy-json-into-attributes-parsing) | P1 | json.rs: zero-copy JSON-into-attributes parsing | `crates/logit-transforms/src/json.rs:65-129` | unreviewed |
| [XFORM-08](#xform-08--logfmtrs--kv-parsing-hand-rolled-tokenizers) | P1 | logfmt.rs / kv parsing: hand-rolled tokenizers | `crates/logit-transforms/src/logfmt.rs:107-129` | unreviewed |
| [XFORM-09](#xform-09--trace_contextrs-timing-resolution-and-skew-arithmetic) | P1 | trace_context.rs: timing resolution and skew arithmetic | `crates/logit-transforms/src/trace_context.rs:166-225` | unreviewed |
| [SINK-02](#sink-02--tcpdialconnect--per-phase-connecthandshake-timeouts-and-reconnect-accounting) | P1 | `TcpDial::connect` — per-phase connect/handshake timeouts and reconnect accounting | `crates/logit-outputs/src/statsd.rs:2214-2279` | unreviewed |
| [SINK-03](#sink-03--poll_pending_close--the-one-poll-half-open-probe-shared-by-every-pooled-sink) | P1 | `poll_pending_close` — the one-poll half-open probe shared by every pooled sink | `crates/logit-outputs/src/tls.rs:48-113` | unreviewed |
| [SINK-06](#sink-06--encode-side-stats-emitted-per-send-attempt--retry-inflation-and-the-cancelled-attempt-hole) | P1 | Encode-side stats emitted per `send` attempt — retry inflation and the cancelled-attempt hole | `crates/logit-outputs/src/statsd.rs:1820-1900` | unreviewed |
| [SINK-07](#sink-07--statsd-line-level-drop-rules-indivisible-entries-oversize-whole-drop-and-the-multi-value-timer) | P1 | statsd line-level drop rules: indivisible entries, oversize-whole-drop, and the multi-value timer | `crates/logit-outputs/src/statsd.rs:1162-1185` | unreviewed |
| [SINK-08](#sink-08--influxdb_outsend--one-shot-http-attempt-fault-classification-and-its-own-reqwest-client) | P1 | `influxdb_out::send` — one-shot HTTP attempt, fault classification, and its own `reqwest` client | `crates/logit-outputs/src/influxdb.rs:26-37` | unreviewed |
| [NET-05](#net-05--udp-bind-path-socket2-socket-creation-so_rcvbuf-multicast-join-address-fallback) | P2 | UDP bind path: `socket2` socket creation, `SO_RCVBUF`, multicast join, address fallback | `crates/logit-inputs/src/udp.rs:383-397` | unreviewed |
| [NET-13](#net-13--listener-wrappers-framingtransport-selection-and-the-decoder-clone-per-connection-contract) | P2 | Listener wrappers: framing/transport selection and the `Decoder: Clone` per-connection contract | `crates/logit-inputs/src/statsd.rs:243-244, 261-266, 301-315, 463-491` | unreviewed |
| [NET-14](#net-14--listener-tls-termination-rustlsserverconfig-construction-from-operator-pem) | P2 | Listener TLS termination: `rustls::ServerConfig` construction from operator PEM | `crates/logit-inputs/src/tls.rs:49-90` | unreviewed |
| [TAIL-11](#tail-11--pattern-discovery-hand-rolled-glob-and-dockers-two-position-walk) | P2 | Pattern discovery: hand-rolled glob and Docker's two-position walk | `crates/logit-inputs/src/tail/pattern.rs:37-93` | unreviewed |
| [TAIL-12](#tail-12--telemetry-and-diagnostic-accounting-across-the-tail-driver) | P2 | Telemetry and diagnostic accounting across the tail driver | `crates/logit-inputs/src/tail/driver.rs:274` | unreviewed |
| [DISK-11](#disk-11--rotationstate--rotation-trigger-bookkeeping-and-open-time-seeding) | P2 | `RotationState` — rotation-trigger bookkeeping and open-time seeding | `crates/logit-outputs/src/file.rs:60-170` | unreviewed |
| [DISK-12](#disk-12--streamoutputsend--encoderotatewriteflush-ordering-and-error-posture) | P2 | `StreamOutput::send` — encode/rotate/write/flush ordering and error posture | `crates/logit-outputs/src/stdio.rs:713-787` | unreviewed |
| [DISK-14](#disk-14--config--spool-wiring-path-resolution-graph-rule-35-and-the-exclusive-lock) | P2 | Config → spool wiring: path resolution, graph rule 35, and the exclusive lock | `crates/logit-cli/src/pipeline.rs:1001-1024` | unreviewed |
| [RT-09](#rt-09--process_batch-in-place-retain_mut-per-event-loop-and-absorbed-accounting) | P2 | `process_batch`: in-place `retain_mut` per-event loop and absorbed accounting | `runtime.rs:1335-1387` | unreviewed |
| [RT-13](#rt-13--readiness-monotonic-phase-transitions-under-concurrent-writers) | P2 | `Readiness`: monotonic phase transitions under concurrent writers | `crates/logit-pipeline/src/readiness.rs:110-211` | unreviewed |
| [RT-15](#rt-15--logit-clipipeline-process-lifecycle-the-double-signal-kill-switch-and-configruntime-knob-mapping) | P2 | `logit-cli::pipeline`: process lifecycle, the double-signal kill switch, and config→runtime knob mapping | `crates/logit-cli/src/pipeline.rs:105-185` | unreviewed |
| [WIRE-20](#wire-20--prometheus_out-remote-write-sender-timestamp-partition-one-post-per-batch-duplicate-safety) | P2 | `prometheus_out` remote-write sender: timestamp partition, one POST per batch, duplicate safety | `crates/logit-outputs/src/prometheus.rs:823-844` | unreviewed |
| [CODEC-04](#codec-04--carbon-plaintextpickle-encoder-tag-sanitization-multi-value-expansion-frame-packing) | P2 | Carbon plaintext/pickle encoder: tag sanitization, multi-value expansion, frame packing | `crates/logit-proto/src/graphite/encode.rs:350-475` | unreviewed |
| [CODEC-06](#codec-06--statsddogstatsd-encoder--service-check-status-coercion-and-multi-value-rendering) | P2 | statsd/DogStatsD encoder — service-check status coercion and multi-value rendering | `crates/logit-outputs/src/statsd.rs:1095-1129` | unreviewed |
| [CODEC-08](#codec-08--rfc-5424-structured-data-parser--bounded-loop-no-recursion-param-folding) | P2 | RFC 5424 STRUCTURED-DATA parser — bounded loop, no recursion, param folding | `crates/logit-inputs/src/syslog.rs:901-923` | unreviewed |
| [CODEC-09](#codec-09--syslog-encoder--structured-data-escaping-header-field-sanitization-and-oversizetruncation-handling) | P2 | syslog encoder — structured-data escaping, header-field sanitization, and oversize/truncation handling | `crates/logit-outputs/src/syslog.rs:397-552` | unreviewed |
| [CODEC-11](#codec-11--collectd-binary-encoder--identity-sanitizationtruncation-and-the-write_string_part-panic-contract) | P2 | collectd binary encoder — identity sanitization/truncation and the write_string_part panic contract | `crates/logit-proto/src/collectd/encode.rs:848-882` | unreviewed |
| [CODEC-15](#codec-15--messagebufm--reusable-framed-message-buffer-low-sensitivity) | P2 | `MessageBuf<M>` — reusable framed-message buffer (low sensitivity) | `crates/logit-proto/src/msgbuf.rs:1-180` | unreviewed |
| [CORE-04](#core-04--estimated_heap_bytes-the-admission-control-accounting-that-must-reconcile) | P2 | `estimated_heap_bytes`: the admission-control accounting that must reconcile | `crates/logit-core/src/event.rs:53-61` | unreviewed |
| [CORE-11](#core-11--tracespan-id-minting-per-thread-splitmix64-and-hex-parsing) | P2 | Trace/span id minting (per-thread SplitMix64) and hex parsing | `crates/logit-core/src/trace.rs:31-47` | unreviewed |
| [CORE-13](#core-13--diagnostics-shared-power-of-two-throttle-and-its-telemetry-mirror) | P2 | `Diagnostics`: shared power-of-two throttle and its telemetry mirror | `crates/logit-core/src/diag.rs:33-56` | unreviewed |
| [CORE-14](#core-14--template-the-name-parser-and-per-event-renderer) | P2 | `template`: the `{name}` parser and per-event renderer | `crates/logit-core/src/template.rs:39-71` | unreviewed |
| [CORE-20](#core-20--countingalloc-the-dev-only-counting-global-allocator) | P2 | `CountingAlloc`: the dev-only counting global allocator | `crates/logit-bench/src/alloc.rs:22-30` | unreviewed |
| [XFORM-05](#xform-05--aggregate-contributing-context-span-link-bookkeeping) | P2 | Aggregate: contributing-context span-link bookkeeping | `crates/logit-transforms/src/aggregate.rs:109-168` | unreviewed |
| [XFORM-07](#xform-07--csvrs-hand-rolled-rfc-4180-row-splitter) | P2 | csv.rs: hand-rolled RFC 4180 row splitter | `crates/logit-transforms/src/csv.rs:179-242` | unreviewed |
| [XFORM-10](#xform-10--regexrs-capture-group-extraction) | P2 | regex.rs: capture-group extraction | `crates/logit-transforms/src/regex.rs:35-54` | unreviewed |
| [XFORM-11](#xform-11--small-filtermutate-transforms-combined) | P2 | Small filter/mutate transforms (combined) | `crates/logit-transforms/src/keep.rs` | unreviewed |
| [SINK-10](#sink-10--build_client_config--insecure_skip_verify--shared-client-tls-construction) | P2 | `build_client_config` / `insecure_skip_verify` — shared client TLS construction | `crates/logit-outputs/src/tls.rs:115-196` | unreviewed |
| [SINK-11](#sink-11--internal_ins-drain-loop-and-its-final-drain-on-shutdown) | P2 | `internal_in`'s drain loop and its final drain on shutdown | `crates/logit-inputs/src/internal.rs:75-143` | unreviewed |
| [SINK-12](#sink-12--builder-order-wiring-of-the-encoder-cap--diagnostics--telemetry) | P2 | Builder-order wiring of the encoder cap / diagnostics / telemetry | `crates/logit-outputs/src/statsd.rs:1739-1815` | unreviewed |


---

## NET — Network intake (UDP/TCP listeners, receive queue, sockstat)

Area: `crates/logit-inputs/src/{udp,tcp,tls,statsd,syslog,collectd,graphite/}.rs`,
`crates/logit-pipeline/src/{sockstat,queue}.rs`.
Governing ADRs: `docs/adr/decoupled-listener-io.md`,
`docs/adr/udp-intake-batching-and-socket-visibility.md`,
`docs/adr/syslog-tcp-ingress-and-tls.md`, `docs/adr/idle-connection-timeout.md`.
Third-party in play: `tokio` 1.53.1 (`net`, `sync::watch`, `sync::Notify`, `sync::Semaphore`,
`time`), `socket2` 0.6.5 (bind/`SO_RCVBUF`/multicast join), `libc` (Linux-only: `recvmmsg`,
`getsockopt`), `bytes` 1, `rustls` 0.23 / `tokio-rustls` 0.26 / `rustls-pki-types` 1.

All line numbers verified against the worktree at
`/home/ross/lib/logit/.claude/worktrees/vectorized-sparking-origami` (HEAD `2f387ee`).

---

### NET-01 — `recvmmsg(2)` batched UDP read: hand-built `mmsghdr`/`iovec` arrays over `Vec<u64>` storage
- **Location:** `crates/logit-inputs/src/udp.rs` — `struct BatchReader`, `BatchReader::new`,
  `BatchReader::read_batch`, `BatchReader::truncated`, and (since `libc/w1`) the three functions
  the closure was split into: `build_headers`, `recvmmsg_into`, `harvest_headers`, with the
  module-scope `HDR_WORDS`/`IOV_WORDS` and the `const _: () = { … }` layout-assert block beside
  them. Plus the compile-time `Send` pin (`assert_batch_read_future_is_send`), the non-Linux twin,
  and the constants `MAX_DATAGRAM_BYTES` / `MAX_READ_BATCH`. (Line numbers dropped: this entry's
  originals were already stale after the split, and the names are stable.)
- **What it does:** Waits for socket readiness via `tokio::net::UdpSocket::async_io(READABLE |
  ERROR)`, then, inside the synchronous closure, re-materializes `vlen` `iovec`s and `vlen`
  `mmsghdr`s into `Vec<u64>` word buffers pointed at a single `vlen * 65_507`-byte slab, issues one
  `libc::recvmmsg(fd, hdrs, vlen, MSG_DONTWAIT, NULL)`, and lifts each returned `msg_len` and
  `msg_hdr.msg_flags` into plain integer vectors. Back in async context it copies each datagram out
  as a right-sized `Bytes`, stamps `received_at = now_nanos() + i`, and counts `MSG_TRUNC`
  occurrences.
- **Why sensitive:** unsafe/syscall (three `unsafe` blocks: pointer writes of C structs into `u64`
  storage, the raw `recvmmsg`, and the post-call `(*hdrs.add(i))` reads); hot-path (this is the
  per-datagram ingress path for every UDP listener); custom (no crate does the vectored receive —
  `tokio` exposes none); cancellation (the future is dropped by `read_loop`'s shutdown race);
  accounting (`logit.input.datagrams.truncated`, and the `+ i` timestamp offset feeding downstream
  (series, timestamp) identity); untrusted-input (lengths and flags come from the kernel but the
  payload boundaries drive the slicing).
- **Invariants to verify:** *(all nine verified in `libc/w1` — see the Verified paragraph below)*
  - ✅ `iov_words`/`hdr_words` really are ≥ `vlen * IOV_WORDS` / `vlen * HDR_WORDS` `u64`s for every
    `vlen` the clamp in `BatchReader::new` can produce, and `div_ceil(8)` never under-allocates on
    any supported target. **Holds**, and the reason originally given was incomplete: `div_ceil`
    guards the per-slot *capacity*, but the hazard it does not address is the *stride* —
    `hdrs.add(i)` steps by `size_of::<mmsghdr>()`, not by the reserved `HDR_WORDS * 8`. That case
    is impossible because Rust guarantees `size_of` is a multiple of `align_of`, which the new
    `const` block now asserts alongside everything else.
  - ✅ The `align_of` assert pair is the only alignment guarantee. **Holds**: `Vec<u64>` allocates
    through `Layout::array::<u64>()`, whose alignment is `align_of::<u64>()`. The asserts are now a
    module-scope `const` block that also covers capacity, stride, and the two harvested field
    offsets.
  - ✅ Every `mmsghdr` is fully zeroed before its two fields are set, so `msg_name`/`msg_namelen`/
    `msg_control`/`msg_controllen` are NULL/0 and the kernel writes no source address. **Holds**,
    and the reason it *matters* is the musl `__pad1`/`__pad2` one (rust-lang/libc#2344,
    libuv#3419), not a kernel one — the kernel's own per-call writeback of
    `msg_flags`/`msg_controllen` is overwritten by the rebuild before anything could read it.
    Pinned by `a_rebuild_after_a_kernel_writeback_fully_reinitialises_every_header`.
  - ✅ `slots` slot `i` is wholly inside the allocation and disjoint per `i`; `self.lens[i].min(…)`
    can never index past a slot. **Holds**, and the `.min()` is now *provably* dead code:
    `udp_recvmsg` returns `copied`, not `ulen`, unless `MSG_TRUNC` is an **input** flag, which this
    call never passes. Kept as defence in depth with an accurate comment; the `debug_assert_eq!`
    beside it is what keeps the claim honest. Disjointness and containment are pinned by
    `every_header_describes_its_own_slot_and_asks_for_nothing_else` under `miri`.
  - ✅ The read future is `Send` with no `unsafe impl`; nothing holds a raw pointer across an
    `.await`. **Holds** — `assert_batch_read_future_is_send` is a real type-check (it is
    `#[allow(dead_code)]`, not `#[cfg(test)]`, so it is checked in every Linux build), and after
    the `libc/w1` split every raw pointer is a local of one of the three synchronous helpers.
  - ✅ Cancel-safety. **Holds**, now proven from tokio source rather than asserted: `async_io` has
    exactly two suspension points and both precede the closure call. The code comment that said
    "the one `.await`" was wrong on its face (there are two, and the second can return `Pending`)
    and is corrected.
  - ✅ `EINTR` retries; `EAGAIN` → `WouldBlock`; every other errno fatal. **Policy holds and is
    right**, but the reachable-errno set is not what the comment implied: `EINTR` is unreachable on
    a non-blocking socket, and the transients a retry would be for are unreachable too. The ADR
    amendment carries the full table. Comment corrected; arm kept.
  - ✅ `received_at` strictly increasing **within** a batch, `saturating_add` cannot saturate.
    **Holds** (`base ≈ 1.8e18`, `i ≤ 1023`). The *cross-batch* claim the test's doc made is weaker
    than it read — `now_nanos()` is the wall clock — and is now stated precisely there and tracked
    in `docs/known-gaps.md`.
  - ✅ `MSG_TRUNC` accounting is per call, reset per call, read once per batch. **Holds**;
    `an_oversized_ipv6_datagram_is_delivered_truncated_and_counted` drives the real case.
- **Observed concerns:**
  - ~~*Low confidence, spin risk:* if `recvmmsg` ever returned `0`…~~ — **retired.** `do_recvmmsg`
    cannot return `0` for `vlen > 0`: its loop is `while (datagrams < vlen)` and every exit with
    `datagrams == 0` returns a negative `err`. `vlen` is clamped ≥ 1 twice over. Unreachable by
    construction, not merely unobserved.
  - ~~*Medium confidence, memory:* the "only faulted pages count" claim is an allocator-behavior
    assumption, not asserted anywhere.~~ — **downgraded to informational.** It *is* measured, two
    independent ways, in `docs/design/memory.md` §5: a direct allocation-side probe plus a
    whole-process peak-RSS check flat at 21.7 MiB across a sixteenfold change in nominal slab size,
    with an explicit, confirmed `THP=always` counter-case where it does not hold. jemalloc is the
    shipped allocator (`logit-cli`'s `#[global_allocator]`, `default = ["jemalloc"]`). One
    unstated precondition remains: memory.md says the slab is allocated "at startup", but it is
    allocated in `read_loop`, i.e. after bind, so jemalloc has had a chance to accumulate dirty
    extents it could recycle-and-memset instead of `mmap`ing fresh. Immaterial at 4 MiB; worth
    knowing.
  - ~~*Low confidence:* the "error after ≥1 message is reported on the next call" quirk is
    unmentioned; can it drop an already-received batch?~~ — **answered: it cannot.** `do_recvmmsg`
    returns the positive count and stashes the error into `sk_err` for the next call's pre-loop
    `sock_error` check. The quirk is real and was undocumented; it is now in the ADR amendment.
    For this call shape the only stashable mid-batch errors need an invalid buffer.
- **Existing coverage:** `udp.rs` tests
  `a_two_hundred_datagram_burst_is_delivered_complete_and_in_order`,
  `read_batch_one_and_sixty_four_yield_identical_event_streams`,
  `datagrams_of_mixed_sizes_including_empty_and_near_maximum_survive_byte_exact`,
  `the_read_counter_never_exceeds_the_datagram_counter_and_both_are_exact`,
  `every_datagram_in_a_batch_gets_its_own_received_at`,
  `an_oversized_ipv6_datagram_is_delivered_truncated_and_counted`. **Added in `libc/w1`:**
  `mod batch_reader_helpers` (six pure tests over `build_headers`/`harvest_headers`, `vlen ∈
  {1, 2, 63, 64, 1024}`, runnable under `miri` and in ordinary CI),
  `a_fatal_read_error_closes_the_queue_and_names_the_syscall_and_the_socket`, and
  `a_sandbox_blocked_syscall_is_named_along_with_why_read_batch_one_would_not_help`.
  Perf: `perf/scenarios/udp-statsd{,-small,-packed}.yaml` + `perf/load/*.yaml` (`script/perf run
  --verify`). ADR: `udp-intake-batching-and-socket-visibility` (see its 2026-09-21 amendment).
- **Suggested verification approach:** *(as executed in `libc/w1`, with two of the original
  suggestions corrected.)* Line-by-line review of the `unsafe` blocks against the `mmsghdr` ABI —
  done, and the blocks were restructured into three named functions so that the two pure ones run
  under `miri` after all: "miri is not usable (real syscalls)" is true of the closure as a whole
  and false of its halves, which is the whole point of the split. `strace -e inject=` for the errno
  paths, now a named, repeatable set (`script/unsafe-check inject-all`: `EINTR:when=2+3` retries
  and loses nothing; `EPERM:when=3` stops on the third call with no retry and no spin;
  `ENOSYS:when=1` makes exactly one traced call, which is the bun#42678 shape's absence).
  **"Real-socket fault-injection test forcing `EINTR` (send a
  signal to the reading thread)" cannot work** — `EINTR` is unreachable on a non-blocking
  `recvmmsg`; only `strace -e inject=recvmmsg:error=EINTR` produces it. A readable *non-socket*
  descriptor (a pipe) does give a real, unprivileged, deterministic fatal errno with no injection
  at all, which is what the new fatal-path test uses.
- **Priority:** P0 — three `unsafe` blocks on the main per-datagram ingress path, hand-rolled
  against a C ABI, with no crate doing the work.
- **Verified 2026-09-21** (`libc/w1`, atop `libc/w0` `c063bc2`, parent `main` `af2ef65`): all nine
  invariants above re-derived against `torvalds/linux` master (`net/socket.c`, `net/ipv4/udp.c`,
  `net/ipv6/udp.c`, `net/core/datagram.c`, `net/ipv4/datagram.c`) and tokio tag `tokio-1.53.1`, the
  version in `Cargo.lock` — every claim in the ADR amendment carries its source. All three observed
  concerns resolved (one retired as unreachable by construction, one downgraded to an
  already-measured fact, one answered and then documented). The closure was split into
  `build_headers` / `recvmmsg_into` / `harvest_headers` with identical behaviour, so the pure
  halves run under `miri` (`script/unsafe-check miri`, Stacked **and** Tree Borrows, clean) —
  including a test that writes through each header's own stored `iov_base`, which is the provenance
  chain production actually depends on. `cargo careful test -p logit-inputs` clean. Three doc/code
  claims were factually wrong and are fixed: `MAX_READ_BATCH`'s `UIO_MAXIOV` justification (there
  is no recv-side `vlen` clamp; the bound is ours), `async_io`'s "one `.await`" (there are two),
  and the coop-budget mechanism (a `WouldBlock` `async_io` spends nothing, and `watch::wait_for`
  has no coop call at all). The fatal path had **no** test of any kind before this; it has two now,
  and the error message names the syscall and the bound socket instead of `Function not
  implemented (os error 38)`.

---

### NET-02 — UDP `read_loop`: shutdown race, queue-close contract, and per-batch telemetry
- **Location:** `crates/logit-inputs/src/udp.rs:556-592` (`read_loop`), reached through
  `read_loop_sampled` (`udp.rs:943-954`).
- **What it does:** Loops: clear the reusable `Vec<Datagram>`, race `BatchReader::read_batch`
  against `shutdown.wait_for`, emit three (sometimes four) per-batch counters, then race
  `queue.push_many(&mut batch)` against `shutdown`. On every exit path — shutdown, or a fatal
  socket error — it calls `queue.close()`, which is the *only* signal `decode_loop` has that
  nothing more will arrive.
- **Why sensitive:** hot-path; cancellation (both `select!` arms can drop a future mid-flight);
  backpressure (`overflow: block` is the one configuration where this genuinely stops reading);
  data-loss (a cancelled `push_many` discards up to `read_batch` datagrams *uncounted*);
  accounting (`logit.input.reads`/`.datagrams`/`.datagram.bytes`/`.datagrams.truncated` must
  reconcile with what `decode_loop` and the queue's own drop counters report); concurrency
  (`queue.close()` is the sole liveness signal for the decode half).
- **Invariants to verify:**
  - `queue.close()` is unconditionally reached on *every* return path, including a panic-free early
    `break Err` (`udp.rs:574-576`) — verify no future edit can `return` past line 590.
  - `logit.input.datagrams` (counted before the push, `udp.rs:579`) plus the queue's
    `logit.component.datagrams.dropped` reconcile with events actually delivered, modulo the named
    shutdown loss.
  - Cancelling the `push_many` arm leaves the accepted prefix queued *and announced* (see the
    `push_many` entry) so the decode side is never parked behind an abandoned prefix.
  - `batch.clear()` at the top (`udp.rs:569`) means no datagram is ever double-pushed after a
    cancelled `push_many` (the `Drain` already emptied it).
  - `shutdown.wait_for(|&due| due)` (not `changed()`) correctly handles "shutdown already fired
    before this loop iteration".
  - The reused `Vec` keeps its capacity across iterations (allocation pin).
- **Observed concerns (unverified):**
  - *Documented, not a surprise:* a `push_many` cancelled by the shutdown arm (`udp.rs:585-588`)
    drops its remainder uncounted — named in ADR `udp-intake-batching-and-socket-visibility` and in
    `push_many`'s own doc. Listed here only so a verifier knows it is intentional.
  - *Low confidence:* `udp.rs` races `shutdown.wait_for` inside a future that must be `Send`
    (`#[async_trait]` `run_until_shutdown`), while `tcp.rs:1224-1228` states that `wait_for`'s
    `Ref` guard makes a combined future `!Send` and therefore uses `changed()` + an explicit
    `*shutdown.borrow()` check. The two drivers reach the same behavior by different means; worth
    confirming the UDP side really is immune (it compiles, so it is — but the asymmetry suggests one
    of the two comments is imprecise).
- **Existing coverage:** `udp.rs` tests `shutdown_drains_the_queue_and_delivers_every_already_queued_datagram`
  (1400), `a_backlog_queued_before_shutdown_is_still_decoded_and_delivered` (1514),
  `shutdown_while_a_batch_is_mid_push_exits_promptly_and_closes_the_queue` (2488),
  `a_block_queue_smaller_than_the_read_batch_still_delivers_every_datagram` (2557),
  `the_reader_keeps_reading_while_the_downstream_fanout_is_never_drained` (1334),
  `a_disabled_sampler_still_reads_and_closes_the_queue` (2058). ADRs: `decoupled-listener-io`,
  `udp-intake-batching-and-socket-visibility`.
- **Suggested verification approach:** targeted review plus a fault-injection test that fires
  shutdown at randomized offsets during a sustained `overflow: block` load and reconciles
  `datagrams sent == delivered + dropped + (bounded shutdown loss)`; `--verify` perf scenario.
- **Priority:** P0 — the close contract is the only thing keeping `decode_loop` from hanging, and
  the accounting is the basis for every loss claim the ADR makes.

---

### NET-03 — UDP `decode_loop`: `pop_many` batching, interval-flush deadline race, and final flush ordering
- **Location:** `crates/logit-inputs/src/udp.rs:1119-1217` (`decode_loop`), with `emit`
  `udp.rs:1229-1232`, `now_nanos` `udp.rs:1236-1238`, and `BatchingConfig` `udp.rs:166-175`.
  Depends on `crates/logit-pipeline/src/accumulator.rs:149-248` (`BatchAccumulator::absorb`/`take`/
  `next_deadline`).
- **What it does:** Owns the `Fanout`. Each iteration: fires an interval flush if the deadline
  passed, then `pop_many(&mut popped, pop_batch)` wrapped in `tokio::time::timeout(wait, …)` when a
  flush interval is configured; a `0` return means closed-and-empty, at which point it flushes
  `FlushReason::Shutdown` and returns. For each popped datagram it records a *per-datagram*
  `logit.component.receive.latency`, clears the reused `scratch`, calls `decoder.decode_into`, and
  hands the result to the accumulator.
- **Why sensitive:** hot-path (per datagram, and the only place the clock is read twice per
  datagram); cancellation (this whole future is dropped by `run_input`'s grace backstop, losing up
  to `read_batch` popped-but-undecoded datagrams); data-loss; backpressure (`emit` awaits
  `Fanout::send`, which can park arbitrarily long and defer the interval flush); accounting
  (`receive.latency`, `receive.flushed{reason}`); custom (hand-rolled deadline math instead of an
  interval timer).
- **Invariants to verify:**
  - `pop_many` returning `0` genuinely implies closed-and-empty, never a live queue (see the
    `pop_many` entry) — a spurious `0` would flush and terminate the listener early.
  - The `timeout(wait, pop_many(...))` at `udp.rs:1168-1173`: on `Err(_elapsed)`, `popped` is
    provably untouched (`pop_many` only awaits on iterations that removed nothing), so `continue` →
    `popped.clear()` cannot discard items.
  - `wait == 0` (deadline already passed) cannot spin: the top-of-loop flush must advance
    `next_flush` via `BatchAccumulator::next_deadline` every time.
  - Arrival order is preserved: `pop_many` appends FIFO and `popped.drain(..)` preserves it
    (`udp.rs:1189`).
  - `scratch.clear()` (not `mem::take`) and `popped.clear()` keep capacity — the allocation pin.
  - Final `FlushReason::Shutdown` fires only after nothing can race it (`count == 0`).
  - Latency `(now_nanos() - received_at).max(0)` (`udp.rs:1190`) is correct given `received_at =
    base + i` can be *ahead* of a subsequent `now_nanos()` read.
  - A decode error drops exactly one datagram and never the rest of the batch (`udp.rs:1210-1213`).
- **Observed concerns (unverified):**
  - *Documented:* one `TraceContext::new_root` per *accumulated* batch, so N unrelated datagrams
    share a root (`udp.rs:1219-1228`); tracked in `docs/known-gaps.md`'s internal-spans entry.
  - *Low confidence:* `now_nanos()` is called once per datagram here *and* once per read batch in
    `BatchReader`; on a high-rate listener that is two `SystemTime::now()` syscalls/vDSO calls per
    datagram's worth of work. Not a correctness issue; worth a perf look.
- **Existing coverage:** `udp.rs` tests `a_backlog_deeper_than_the_pop_batch_is_fully_decoded_in_arrival_order`
  (1568), `a_malformed_datagram_is_skipped_without_stopping_the_decode_loop` (1618),
  `a_two_hundred_datagram_burst_is_delivered_complete_and_in_order` (2274). Accumulator tests in
  `crates/logit-pipeline/src/accumulator.rs` (`mod tests`, from line 263). Integration:
  `crates/logit-inputs/tests/statsd_to_aggregate.rs`. Alloc pins:
  `crates/logit-bench/tests/allocations.rs`.
- **Suggested verification approach:** targeted review of the deadline arithmetic; a
  `#[tokio::test(start_paused = true)]` proptest over (interval, arrival pattern, batch bounds)
  asserting no event is lost, no batch is split across resources, and the flush cadence never
  drifts; fault injection dropping the future mid-decode to bound the loss.
- **Priority:** P0 — a wrong `0`-means-closed reading or a lost `popped` vec is silent data loss on
  the main path, and the deadline math is hand-rolled.

---

### NET-04 — `UdpListener::run_until_shutdown`: the read/decode two-future select and double-poll guard
- **Location:** `crates/logit-inputs/src/udp.rs:305-363` (`Input::run_until_shutdown` for
  `UdpListener`), plus `Input::bind` `udp.rs:272-295`.
- **What it does:** Takes the socket left by `bind()`, builds the shared `Arc<ReceiveQueue>`, pins
  `read_loop_sampled` and `decode_loop` as two futures, and races them with `tokio::select!`. If
  `read` wins it then drives `decode` to completion (draining what was already queued); if `decode`
  somehow wins first it awaits `read` and never touches `decode` again — the explicit guard against
  polling an already-resolved future.
- **Why sensitive:** concurrency (two cooperatively-scheduled halves on one task — see
  `docs/known-gaps.md`'s "read and decode loops share one task"); shutdown ordering (the decode half
  owns the `Fanout`, so its drop is what cascades the shutdown downstream); cancellation.
- **Invariants to verify:**
  - `decode` is never polled after it resolved (the `Option<result>` dance at `udp.rs:352-362`).
  - `read` finishing always precedes `decode` finishing in practice, because only `read_loop` calls
    `queue.close()`.
  - `self.socket.take()` at `udp.rs:311` + `bind()` idempotence means a second `run` rebinds rather
    than panicking.
  - The socket outlives `ReceiveBufferSampler`'s captured raw fd (`udp.rs:1018-1022`) — it is owned
    by this function, which outlives both futures.
- **Observed concerns (unverified):** none spotted. The guard and its rationale are unusually well
  argued in the comment at `udp.rs:336-351`.
- **Existing coverage:** `udp.rs` tests `bind_then_run_delivers_a_real_datagram` (1440),
  `a_second_bind_is_a_no_op` (1468), `run_until_shutdown_binds_when_the_caller_did_not` (1494),
  `bind_reports_an_unbindable_address` (1483). ADR: `decoupled-listener-io`,
  `service-lifecycle-and-output-retry`.
- **Suggested verification approach:** targeted review; optionally a loom/shuttle model of the
  close/park/wake handshake between the two halves (they share one task today, so loom adds little
  unless `decode_loop` moves onto its own task; see the "A UDP listener's read and decode loops
  share one task" entry in [`docs/known-gaps.md`](../known-gaps.md#udp-intake)).
- **Priority:** P1 — correct today and well documented, but it is the hinge the whole listener's
  shutdown ordering swings on.

---

### NET-05 — UDP bind path: `socket2` socket creation, `SO_RCVBUF`, multicast join, address fallback
- **Location:** `crates/logit-inputs/src/udp.rs:383-397` (`bind_socket`), `403-423`
  (`bind_first_available`), `426-485` (`Bound`, `bind_one`), `489-523` (`finish_bind`).
- **What it does:** Resolves `bind:` asynchronously via `tokio::net::lookup_host`, then tries each
  resolved address in turn. Per candidate it creates a `socket2::Socket`, optionally sets
  `SO_RCVBUF`, sets non-blocking, and — if the address is a multicast group — sets `SO_REUSEADDR`,
  binds the *unspecified* address on that port, and joins the group on the default interface
  (`INADDR_ANY` / ifindex 0). `finish_bind` gauges the granted receive buffer, warns when the
  kernel clamped it below `2 × requested` on Linux, and converts to a `tokio::net::UdpSocket`.
- **Why sensitive:** nontrivial-3p-use(socket2) (multicast requires a three-step sequence that
  `tokio::net::UdpSocket::bind` cannot express); accounting (`logit.input.receive_buffer.bytes` /
  `.requested.bytes` and the `rmem_max` warning); a failed join is deliberately fatal because a
  bound-but-unjoined listener looks healthy and receives nothing forever.
- **Invariants to verify:**
  - The ordering `set_recv_buffer_size` → `set_nonblocking` → (`set_reuse_address` →) `bind` →
    `join` is the one the kernel requires for each path; `SO_RCVBUF` before `bind` is what Linux
    wants.
  - IPv4 vs IPv6 group detection (`is_multicast()` covers `224.0.0.0/4` and `ff00::/8`) and the
    matching `join_multicast_v4`/`v6` call.
  - `finish_bind`'s `effective_minimum = requested.saturating_mul(2)` on Linux matches
    `sock_setsockopt`'s `sk_rcvbuf = max(2*requested, SOCK_MIN_RCVBUF)` and does not false-warn at
    small values.
  - `bind_first_available` really tries every candidate, and the "no addresses" path bails cleanly.
- **Observed concerns (unverified):**
  - *Medium confidence, minor:* at `udp.rs:413`, if `bind_one` succeeds but `finish_bind` fails
    (only the `from_std` conversion can), the `?` returns immediately instead of falling through to
    the next candidate — a small inconsistency with the "try every resolved address" contract this
    function is explicitly documented to have. Practically unreachable.
  - *Low confidence:* the multicast join uses interface index `0`/`INADDR_ANY` with no way for an
    operator to pick an interface on a multi-homed host; deliberate, but there is no config escape
    hatch and no diagnostic if the kernel picks the wrong one.
- **Existing coverage:** `udp.rs` tests `bind_socket_reports_the_granted_receive_buffer_even_when_unset`
  (1673), `bind_first_available_falls_through_to_a_later_candidate` (1692),
  `a_multicast_bind_joins_the_group_and_receives_a_datagram_sent_to_it` (1727),
  `bind_first_available_with_every_candidate_failing_reports_the_last_error` (2609). Consumer:
  `crates/logit-inputs/src/collectd.rs:162-179` (`collectd_in`'s standard group). ADR:
  `decoupled-listener-io`, `collectd-binary-relay`.
- **Suggested verification approach:** targeted review; a real-socket test on a host with two
  interfaces; `strace` the setsockopt sequence.
- **Priority:** P2 — startup-only, well covered, and a failure is loud rather than silent.

---

### NET-06 — `BoundedQueue::push_many`: batched admission control, the pre-wait notify, and cancellation
- **Location:** `crates/logit-pipeline/src/queue.rs:382-469` (`push_many`), against
  `queue.rs:256-321` (`push`, the single-item reference semantics),
  `queue.rs:630-641` (`would_overflow`, `count_dropped`), `queue.rs:648-658` (`update_gauges`).
  UDP-side types: `crates/logit-inputs/src/udp.rs:35-63` (`Datagram`, `Queued` impl,
  `RECEIVE_QUEUE_METRICS`, `ReceiveQueue`).
- **What it does:** Drains a `Vec<T>` into the queue through a `Peekable<Drain>`, applying the same
  per-item admission control `push` applies to one: per-item `Queued::weight`, the
  "impossible to ever fit" bypass, `Block` waiting on `not_full`, `DropOldest` eviction /
  `DropNewest` rejection with per-item `count_dropped`. The bookkeeping is batched: **one**
  `update_gauges` per call, at most one `push_blocked` timing sample per call, one
  `not_empty.notify_one()` at the end plus one immediately *before* every suspend.
- **Why sensitive:** hot-path (one call per `recvmmsg` batch); concurrency (the `Notify` condvar
  pattern, permit vs. broadcast semantics); cancellation (the `Drain` is held across every
  `.await`); backpressure (`Block` is the operator-selectable mode that makes the reader stop);
  data-loss (the cancelled remainder is dropped uncounted, by design); accounting
  (`logit.component.datagrams.dropped` / `.bytes.dropped` must reconcile);
  nontrivial-3p-use(tokio::sync::Notify — the code depends on documented-but-subtle
  `notified()`-constructed-before-state-check ordering and on `notify_one` storing a permit where
  `notify_waiters` does not).
- **Invariants to verify:**
  - The `Notified` future is constructed *before* the state check on every loop iteration
    (`queue.rs:402`), so no `commit`/`pop`/`close` landing after construction is missed.
  - The pre-wait `not_empty.notify_one()` (`queue.rs:457`) can never be spurious — the comment's
    proof that `must_wait ⇒ queue non-empty` (`queue.rs:450-456`) needs checking against
    `would_overflow`'s `>=`/`>` mix at `queue.rs:631`.
  - No deadlock for `max_items: 2` + a 5-item batch under `Block` (the mutual-wait case the pre-wait
    notify exists for), including when a consumer parked *before* any of the prefix landed.
  - `head_weight` is computed exactly once per item across however many wait iterations re-examine
    it (`queue.rs:398`, `409`, `420`).
  - `items` is always left empty with capacity intact, on the ordinary path *and* on cancellation.
  - The cancelled-mid-wait case leaves: accepted prefix queued, fully accounted, and announced;
    remainder dropped uncounted; caller's `Vec` empty.
  - `dropped` is drained and counted *outside* the lock, per iteration, so evicted datagrams are
    freed promptly rather than held for the whole `Block` wait (`queue.rs:433-435`).
  - Mutex poisoning is swallowed (`unwrap_or_else(|p| p.into_inner())`) at every lock site — verify
    a poisoned queue can't serve corrupted state.
- **Observed concerns (unverified):**
  - *Low confidence:* `would_overflow` (`queue.rs:631`) is `inner.len() >= max_items || inner.weight()
    + weight > max_weight`; `inner.weight() + weight` is a plain `u64` add with no overflow guard.
    `Datagram::weight()` is ≤ ~65 KB so unreachable today, but the type is generic.
  - *Documented:* the uncounted cancellation remainder (`queue.rs:368-381`) is an accepted,
    shutdown-only loss; listed for context, not as a finding.
- **Existing coverage:** `queue.rs` tests 1286-1531 (eleven `push_many_*` tests including
  `a_cancelled_push_many_leaves_the_prefix_queued_the_vec_empty_and_accounting_exact` (1490)),
  `a_consumer_parked_before_a_blocking_push_many_is_woken_by_the_prefix_it_admits` (1688),
  `a_push_many_cancelled_mid_wait_still_wakes_a_consumer_parked_behind_its_prefix` (1731),
  `under_block_with_a_running_consumer_batched_and_single_pushes_agree` (1765),
  `push_many_and_pop_many_each_update_the_gauges_exactly_once_per_call` (1860),
  `a_push_many_that_waits_records_exactly_one_push_blocked_sample` (1899),
  `any_interleaving_of_batched_and_single_calls_agrees_with_the_single_call_sequence` (1973).
  ADR: `udp-intake-batching-and-socket-visibility` ("`push_many`/`pop_many` live on `BoundedQueue`
  itself"), `decoupled-listener-io`.
- **Suggested verification approach:** loom or shuttle model of push/push_many/pop/pop_many/close
  interleavings with 1–2 producers and 1–2 consumers — the `Notify` permit reasoning is exactly
  what a model checker is for; supplement with a proptest comparing batched vs. single-call
  sequences (one already exists at 1973 — extend it to cover `close()` racing).
- **Priority:** P0 — a lost wakeup here wedges a listener permanently, and the drop accounting is
  the source of truth for every "logit counted the loss" claim.

---

### NET-07 — `BoundedQueue::pop_many` / `pop` / `close`: cancellation safety and the closed-and-empty signal
- **Location:** `crates/logit-pipeline/src/queue.rs:497-522` (`pop`), `546-589` (`pop_many`),
  `624-628` (`close`), `661-…` (`peek`, the `T: Clone` sibling used by `SinkQueue` — another
  surveyor's area but it shares this lock and these `Notify`s).
- **What it does:** `pop_many` awaits at least one item, then removes up to `max` under **one** lock
  acquisition with no suspend point, appends FIFO to a caller-reused `Vec`, issues one
  `not_full.notify_one()` *per item removed*, and updates gauges once. A return of `0` means
  closed-and-empty and nothing else. `close()` sets the flag with `Release` and calls
  `notify_waiters()` on both `Notify`s.
- **Why sensitive:** concurrency; cancellation (the grace backstop drops this future);
  data-loss (an item removed into a half-filled `out` that is then dropped would vanish silently);
  backpressure (the per-item `not_full` wakeups are what let a batched pop release multiple blocked
  pushers); accounting; nontrivial-3p-use(tokio Notify — `close()` relies on `notified()` snapshotting
  the `notify_waiters` counter at *construction*, a tokio-1.53.1 internal behavior the code pins
  with a test).
- **Invariants to verify:**
  - `pop_many` only awaits on an iteration that removed nothing, so a dropped future can never lose
    items into a partially-filled `out` (`queue.rs:570-587`).
  - `commit()` clears any stale head reservation, so `pop_many` can't inherit one left by a
    cancelled `peek` (`queue.rs:555-561`).
  - `return 0` at `queue.rs:565-566` happens with the lock guard alive inside the block — verify the
    guard drop and that `out` is genuinely untouched on that path.
  - `max == 0` is `debug_assert`ed and clamped to 1 in release (`queue.rs:547-548`).
  - `close()`'s permit-less `notify_waiters()` is never lost: every wait loop constructs its
    `Notified` before its state check; re-verify against the pinned tokio version if it is bumped.
  - `closed` uses `Release` on store and `Acquire` on every load (`queue.rs:278`, `415`, `509`,
    `565`, `625`) — confirm no load uses `Relaxed`.
  - The per-item `not_full.notify_one()` loop (`queue.rs:581-583`) matches what `popped` sequential
    `pop`s would have done.
- **Observed concerns (unverified):** none spotted. The `notify_one` vs. `notify_waiters` choice is
  argued explicitly and pinned by a test.
- **Existing coverage:** `queue.rs` tests `pop_many_is_fifo_across_push_and_push_many_interleavings_and_never_exceeds_max`
  (1533), `pop_many_returns_zero_only_once_closed_and_empty` (1554),
  `pop_many_awaits_on_an_empty_open_queue_and_resolves_once_a_push_many_lands` (1566),
  `a_pop_many_dropped_mid_wait_leaves_no_reservation_a_later_push_can_still_evict` (1593),
  `a_notify_waiters_call_between_constructing_a_notified_and_polling_it_is_never_lost` (1622),
  `pop_many_with_max_zero_trips_a_debug_assert` (1938) / `…_is_clamped_to_one_rather_than_hanging`
  (1947), plus `pop_is_fifo_and_returns_none_once_closed_and_empty` (1161).
- **Suggested verification approach:** loom/shuttle (same model as `push_many`); re-run the
  tokio-internals pin test on any `tokio` bump; targeted review of the `Ordering` choices.
- **Priority:** P0 — same lock/notify protocol as `push_many`; a lost `not_empty` wakeup hangs a
  listener's decode loop with a non-empty queue.

---

### NET-08 — TCP `Framer`: RFC 6587 auto-detect latch, LF lines with drain-resync, and the 4-byte length prefix
- **Location:** `crates/logit-inputs/src/tcp.rs:295-710` — `struct Framer` (301-324),
  `Framer::new` (333-348), `push`/latch (373-385), `next_frame` (394-410), `finish` (430-515),
  `oversize_policy` (519-524), `next_line` (528-593), `next_length_prefixed` (603-624),
  `next_octet_counted` (634-700), `strip_cr` (705-710). Supporting types: `FramingMode` (169-183),
  `Oversize` (186-196), `Framing` (211-223), `FrameError` (241-293). Constants: `MAX_FRAME_BYTES`
  (143), `READ_BUFFER_BYTES` (148), `LENGTH_PREFIX_BYTES` (155) with a cross-crate assert (157).
- **What it does:** A pure, socket-free byte-stream framer. Under `Rfc6587Auto` it latches
  octet-counting vs. LF framing from the connection's very first byte and never re-evaluates. Under
  `Lines` it scans for `LF` with an incremental `scanned` cursor (so a long line is not rescanned
  O(n²)), strips at most one preceding `CR`, and — under `Oversize::DrainToNextLine` — abandons an
  over-bound line, counts it once, and resynchronizes at the next `LF` via a `draining` latch.
  `LengthPrefixed` reads a big-endian `u32`, rejects a declared length over the bound as fatal, and
  hands the payload out unframed. `finish()` decides what a terminator-less remainder means per
  framing.
- **Why sensitive:** untrusted-input (this is the sole parser standing between a hostile peer and
  the decoders — unbounded buffering, integer overflow, index panics, and infinite loops all live
  here); hot-path (per frame, per read); custom (entirely hand-rolled — no `tokio_util::codec`);
  data-loss / duplication (a mis-framed stream silently corrupts every subsequent message);
  accounting (`logit.input.frames.dropped{reason}` must be emitted exactly once per lost frame,
  and the RST path must agree with the FIN path about identical bytes).
- **Invariants to verify:**
  - **Buffer is bounded for every hostile input.** Octet counting: digits capped at 9
    (`tcp.rs:644-648`), declared `len` capped at `max_frame_bytes` (685-690), so `buf` ≤ `header +
    max_frame_bytes`. Lines: the no-`LF` path checks `buf.len() > bound` (550) — but the check only
    fires *after* the whole read has been appended, so the true ceiling is `bound +
    READ_BUFFER_BYTES`. Length prefix: the declared length is checked before waiting (610-616).
    Confirm each path and that `draining` (560-562) cannot be defeated.
  - **No index panic.** `self.buf[self.scanned..]` (547) requires `scanned <= buf.len()` on every
    path, including after the `draining` block consumed bytes (531-543) and after every
    `split_to`. `self.buf[0]` (671) requires `digits >= 1`, which 664-666 guarantees.
  - **No integer overflow.** `idx = self.scanned + offset` (572); `header + len` (693);
    `LENGTH_PREFIX_BYTES + payload_len` (617) where `payload_len` is a `u32` widened to `usize`
    (fine on 64-bit, check 32-bit targets).
  - **No infinite loop.** `next_frame`'s `continue` on an empty line (405) must always make progress
    — verify an all-`\n` stream terminates each `next_frame` call.
  - **The latch is per connection and never re-evaluated**, including a `push` of an empty slice
    (375: `seen_bytes |= !bytes.is_empty()`).
  - **`scanned` is reset on every path that mutates `buf`** (439, 452, 458, 466, 479, 536, 541, 561,
    582, 591, 622, 698).
  - **`finish()` and `report_buffered_tail` agree**: identical buffered bytes followed by FIN vs.
    RST produce the same counter, except the one documented case (`Rfc6587Auto` LF arm emits the
    remainder as a real final message).
  - **`OversizeSkipped` is the only non-fatal variant** (277-279) and the framer is genuinely
    resynchronized when it is returned (the drain latch or the consumed terminator).
  - Leading-zero octet counts, zero counts, and non-digit-before-SP are all rejected rather than
    silently reinterpreted (664-679).
- **Observed concerns (unverified):**
  - *Medium confidence, bound slack:* under `Lines`, `next_line`'s ceiling test at `tcp.rs:550` runs
    only after `serve_connection` pushed a whole read. With `READ_BUFFER_BYTES = 8 KiB` and a
    `graphite_in` `max_line_bytes`, peak buffered bytes is `max_line_bytes + 8 KiB - 1`, not
    `max_line_bytes`. Benign at these sizes; worth confirming it is intended and documented.
  - *Low confidence:* `next_line`'s post-drain path sets `self.buf.clear()` when no `LF` is present
    (539-542) — a peer that never sends `LF` after crossing the bound will have every subsequent
    byte silently discarded with no further counter and no connection close. With no `idle_timeout`
    configured that connection holds a permit indefinitely while consuming bytes. The skip was
    counted once, by design; the indefinite silent-discard state is the part worth re-reading.
  - *Low confidence, 32-bit only:* `u32::from_be_bytes(prefix) as usize` (609) is lossless on 64-bit
    but the comparison at 610 against `max_frame_bytes: usize` would behave differently on a 32-bit
    target. No such target ships today.
- **Existing coverage:** `tcp.rs` framer tests 1629-2011 (~20 tests: latch, split pushes, embedded
  newline, CR stripping, empty lines, oversize under both policies, drain-to-next-line, ten-digit
  and leading-zero counts, length-prefix assembly and over-bound), plus the recorded-interop replay
  `interop_fixture_rsyslog_tcp_non_transparent_frame` (2022). ADRs: `syslog-tcp-ingress-and-tls`,
  `graphite-carbon-relay`, `idle-connection-timeout`.
- **Suggested verification approach:** **a fuzz target** — `Framer` is pure, synchronous, and takes
  arbitrary bytes, which makes it the single best fuzzing candidate in this area (drive
  `push`/`next_frame`/`finish` over `cargo-fuzz` or `arbitrary`-driven proptest with randomized
  chunk boundaries, asserting: never panics, buffered bytes stay bounded, every byte is either
  emitted in a frame or accounted by exactly one `FrameError`, and chunking does not change the
  frame sequence). Add a differential proptest asserting FIN and RST agree.
- **Priority:** P0 — the only untrusted-input parser on the stream path, entirely hand-rolled, and a
  framing mistake silently corrupts every downstream message rather than failing.

---

### NET-09 — TCP `serve_connection`: the shared next-byte deadline, idle-close policy, and end-of-connection flushes
- **Location:** `crates/logit-inputs/src/tcp.rs:1264-1491` (`serve_connection`), with
  `read_step` (1229-1242), `ReadStep` (1207-1216), `absorb_frame` (1497-1522),
  `report_frame_error` (1528-1531), `report_buffered_tail` (1545-1557), `emit` (1563-1566),
  `far_future` (1586-1588). Module contract: `tcp.rs:49-94`.
- **What it does:** Per connection: races the interval-flush deadline and a single *next-byte*
  deadline against `read_step` using `timeout_at`. The next-byte deadline is the absolute first-byte
  deadline while `!framer.first_byte_seen()`, and `last_progress + idle_timeout` afterwards (or
  `far_future()` when no idle timeout is set). `last_progress` advances on exactly two things: bytes
  read (stamped *after* the frame loop, so time blocked in `Fanout::send` is not charged to the
  peer) and an interval flush that actually emitted. Every termination path (shutdown, EOF, read
  error, idle, fatal frame error) flushes the accumulator with a specific `FlushReason` and
  optionally reports a buffered partial frame.
- **Why sensitive:** hot-path (per read, per frame); cancellation (`read_buf` cancel-safety is what
  lets both the flush tick and the shutdown arm drop the read mid-await without losing stream
  bytes); concurrency/shutdown ordering; backpressure (`emit` parks on `Fanout::send`, and the idle
  clock must not run while it does — a design requirement the ADR states explicitly); data-loss (an
  end-of-connection path that skips the accumulator flush silently drops decoded events);
  accounting (`connections.closed{reason="idle"}`, `frames.dropped{reason="truncated"}`, and the
  `Ok(())`-vs-`Err` split that decides whether a `connection_error` diagnostic fires).
- **Invariants to verify:**
  - `AsyncReadExt::read_buf` is cancellation-safe for both `TcpStream` and
    `tokio_rustls::server::TlsStream` — the TLS case is the one worth checking, since
    `tokio-rustls` buffers plaintext internally.
  - Every `return` path flushes `accumulator.take()` — enumerate all six (1332, 1389, 1403, 1430,
    1439, 1479) and confirm none can drop decoded events.
  - `report_buffered_tail` is called on exactly the non-clean paths (shutdown 1328, idle 1384,
    read-error 1435) and *not* on the EOF path (where `framer.finish()` handles it) — no double
    counting, no silent gap.
  - `first_byte_deadline` is absolute (computed once at 1288) and survives arbitrarily many flush
    ticks; `timeout_at` (not `timeout`) is what preserves that.
  - The post-timeout discriminator `now >= next_byte_deadline` (1371) cannot misclassify a flush
    tick as an idle close, and cannot miss an idle close that coincides with a flush tick.
  - `last_progress.checked_add(idle)` (1357) handles an absurd-but-legal `idle_timeout` by falling
    back to `far_future()`.
  - A flush tick that emits nothing must **not** reset the idle clock (1307-1314 — only inside the
    `if let Some(batch)`).
  - Bytes that complete no frame still reset the clock (1489 runs on every `ReadStep::Bytes`).
  - An idle close returns `Ok(())` so it never becomes a `connection_error`.
  - `received_at` is stamped once per read (1445), before framing, matching `Decoder::decode_into`'s
    contract.
- **Observed concerns (unverified):**
  - *Low confidence:* `far_future()` is recomputed on every loop iteration (1357 →
    `unwrap_or_else(far_future)`), so the "never arrives" deadline slides forward each pass. Correct
    behaviorally, but it means the arithmetic is redone per read; also, when there is *no* flush
    interval and *no* idle timeout, `timeout_at` arms a 30-year timer per read rather than skipping
    the timeout wrapper.
  - *Low confidence:* a fatal `FrameError` inside the inner frame loop (1476-1479) flushes the
    accumulator but does **not** call `report_buffered_tail` — arguably correct (the framer cleared
    its buffer as part of raising the error on most paths), but `Oversize` raised at `tcp.rs:574-577`
    (the terminator-already-buffered, `Fatal` arm) returns *without* clearing `buf`, so those bytes
    are discarded uncounted. Worth checking against the "FIN and RST agree" claim.
  - *Documented:* a connection still within its idle budget at shutdown holds things open until the
    grace backstop — [`docs/known-gaps.md`](../known-gaps.md#native-wire-format-logit_inlogit_out-and-buffering)'s
    "`otlp_in` can hold the graph open past shutdown" entry.
- **Existing coverage:** `tcp.rs` tests 2295-3262: `a_clean_close_flushes_whatever_is_accumulated`
  (2375), `an_abrupt_close_with_a_buffered_partial_frame_counts_it_truncated` (2562),
  `shutdown_mid_message_counts_the_buffered_partial_frame` (2600),
  `a_silent_plaintext_connection_releases_its_permit_after_the_handshake_timeout` (2789),
  `the_first_byte_deadline_does_not_apply_once_the_framing_has_latched` (2818),
  `the_first_byte_deadline_applies_under_every_framing_mode` (2857),
  `an_idle_connection_is_closed_after_the_idle_timeout_and_releases_its_permit` (2964),
  `a_connection_blocked_on_a_full_downstream_is_not_closed_as_idle` (3019),
  `an_idle_close_flushes_the_accumulated_batch_and_counts_a_buffered_partial_frame_truncated`
  (3054), `a_flush_tick_does_not_reset_the_idle_clock` (3106),
  `bytes_that_complete_no_frame_still_reset_the_idle_clock` (3133),
  `the_idle_timeout_applies_under_every_framing_mode` (3161),
  `no_idle_timeout_means_a_quiet_connection_is_never_closed` (3210). ADR:
  `idle-connection-timeout`, `syslog-tcp-ingress-and-tls`.
- **Suggested verification approach:** targeted review of the six return paths against an
  "every decoded event is delivered exactly once" checklist; `start_paused = true` proptest over
  (flush interval, idle timeout, arrival schedule, close mode) asserting deliver-once and correct
  close-reason attribution; a real-socket slow-loris test.
- **Priority:** P1 — very well tested and argued; the risk is a future edit adding a seventh return
  path that forgets the flush, and the `Oversize`-fatal buffered-byte accounting above.

---

### NET-10 — TCP accept loop: connection cap, permit lifetime, per-connection spawn, and the live-connections gauge
- **Location:** `crates/logit-inputs/src/tcp.rs:1077-1205` (`Input::run_until_shutdown` for
  `TcpListener`), with `Input::bind` (1060-1068), `MAX_CONCURRENT_CONNECTIONS` (118),
  `HANDSHAKE_TIMEOUT` (129), and the module's "Connection limit" contract (39-47).
- **What it does:** Binds once in a pre-pass, then loops: sample-and-accept (via
  `AcceptQueueSampler`) raced against `shutdown`; `try_acquire_owned` a semaphore permit or drop the
  stream immediately and count `connections.rejected{reason="limit"}`; clone the `Fanout`,
  `Diagnostics`, `Telemetry`, `TlsAcceptor` and decoder; `tokio::spawn` a task that bumps the
  `AtomicI64` live-connections gauge, bounds the TLS accept with `handshake_timeout`, runs
  `serve_connection`, decrements the gauge, and reports any `Err` as `connection_error`.
- **Why sensitive:** concurrency (unjoined spawned tasks, an `AtomicI64` gauge, a shared
  `Diagnostics` throttle); shutdown ordering (the accept loop returns immediately on shutdown while
  connection tasks keep running, each holding a `Fanout` clone — that is what keeps the cascade
  open); backpressure (the cap is a hard reject, not a queue); accounting
  (`connections`/`connections.rejected`/`connections.closed`); nontrivial-3p-use(tokio Semaphore
  owned permits, tokio-rustls `TlsAcceptor`).
- **Invariants to verify:**
  - A permit is released on *every* task exit — including a TLS accept that errors or times out
    (1167-1170) and a panicking `serve_connection` (the `_permit` binding at 1138 handles the
    unwind, assuming `panic = unwind`).
  - The live-connections gauge converges to 0 after every connection ends; the `fetch_add`/
    `fetch_sub` are balanced across all exit paths.
  - Shutdown: the accept loop's `return Ok(())` at 1107 does not orphan in-flight connection tasks
    in a way that loses their accumulated batches — each has its own `conn_shutdown` receiver and
    flushes on it, but nothing joins them.
  - `Diagnostics` clones share throttle counts listener-wide (relied on by
    `the_per_frame_diagnostic_throttle_is_shared_not_per_connection`).
  - A past-the-cap connection is closed *before* any TLS accept, so a flood costs no handshake CPU.
  - `TlsAcceptor::from` is built once outside the loop; the per-connection clone is an `Arc` bump.
- **Observed concerns (unverified):**
  - *Medium confidence:* `accepted?` at `tcp.rs:1106` makes **any** `accept()` error fatal to the
    whole listener. Transient, recoverable errnos — `EMFILE`/`ENFILE` (fd exhaustion, plausible at a
    1024-connection cap plus sinks), `ECONNABORTED`, `ENOBUFS` — would take the listener down
    permanently rather than backing off and retrying. The conventional accept loop distinguishes
    these. No test covers it.
  - *Medium confidence, telemetry only:* the live-connections gauge (1145-1146, 1193-1194) publishes
    from the RMW's return value, but the `fetch_add` and the `gauge()` write are not atomic together,
    so two tasks can still interleave and leave a stale value published until the next transition.
    The comment claims this pattern avoids that; it narrows the window but does not close it.
  - *Low confidence:* nothing bounds how long a connection task may outlive the accept loop after
    shutdown other than `run_input`'s grace backstop, and the tasks are never joined — so
    `run_until_shutdown` returning `Ok(())` does not mean the listener is quiescent.
- **Existing coverage:** `tcp.rs` tests `the_connection_cap_drops_a_connection_past_the_limit_and_counts_it`
  (2434), `three_connections_report_their_framing_errors_through_one_throttle` (2511),
  `an_oversize_frame_closes_only_that_connection` (2636),
  `shutdown_returns_promptly_with_an_idle_connection_still_open` (2672),
  `a_tls_connection_round_trips_a_decoded_frame` (2698),
  `a_client_trusting_the_wrong_ca_is_refused_and_the_listener_keeps_serving` (2719),
  `mutual_tls_accepts_a_client_certificate_and_refuses_a_client_without_one` (2746),
  `a_silent_connection_releases_its_permit_after_the_handshake_timeout` (2905),
  `bind_makes_the_port_live_and_local_addr_reports_it_before_run` (2278). `graphite/mod.rs`
  `two_concurrent_tcp_connections_both_deliver` (689). ADR: `syslog-tcp-ingress-and-tls`,
  `idle-connection-timeout`, `service-lifecycle-and-output-retry`.
- **Suggested verification approach:** targeted review of the accept-error classification (compare
  against `otlp_in`/`logit_in`'s own accept loops for consistency); fault injection with `ulimit -n`
  lowered to force `EMFILE`; a shutdown test that asserts every spawned task has exited before the
  input is considered done.
- **Priority:** P1 — the fatal-accept-error path is a real availability hazard on the main data
  path, but it degrades to "listener stops loudly" rather than silent corruption.

---

### NET-11 — `sockstat`: raw `getsockopt(SO_MEMINFO)` / `getsockopt(TCP_INFO)` and the wrapping drop counter
- **Location:** `crates/logit-pipeline/src/sockstat.rs` — `meminfo`/`parse_meminfo`,
  `listen_queue`/`parse_listen_queue`, `DropCounter`, `SockMeminfo::receive_utilization`,
  `Unavailable`, the module-scope ABI `const _: () = assert!(…)` block, and `RawFd`/`fd_of` with
  their non-unix twins. (Line numbers dropped: `libc/w2` moved everything in this file. The item
  names are stable, the numbers were not.)
- **What it does:** Two `unsafe` `libc::getsockopt` calls against a raw fd this process owns.
  `meminfo` reads up to 9 `u32`s, verifies the kernel wrote at least through `SK_MEMINFO_DROPS`, and
  returns five named fields. `listen_queue` zeroes a `libc::tcp_info`, reads it, verifies the reply
  reached `tcpi_sacked`, verifies `tcpi_state == TCP_LISTEN` (a hardcoded `10`), and returns the
  kernel's aliased accept-queue depth/backlog. `DropCounter::delta` turns the free-running, wrapping
  `sk_drops` into a per-interval delta, treating the first sample as an absolute from a known zero.
- **Why sensitive:** unsafe/syscall (two raw `getsockopt` calls with caller-supplied `socklen_t`
  in/out); accounting (`logit.input.kernel.drops` is the only visibility into loss no other layer
  can see, and it is claimed to reconcile byte-for-byte with `/proc/net/udp`'s `drops` column);
  custom (deliberately not procfs); nontrivial-3p-use(libc — `SK_MEMINFO_*` indices and the
  `TCP_LISTEN` constant are copied from kernel UAPI, not from `libc`).
- **Invariants to verify:**
  - `raw` is `[u32; 9]` and every index read (`SK_MEMINFO_RMEM_ALLOC`=0, `RCVBUF`=1, `WMEM_ALLOC`=2,
    `SNDBUF`=3, `DROPS`=8) is in range; `needed` (158) covers the largest index actually read.
  - `len` is initialized to the buffer size and the kernel's written-back value is the only thing
    the length check trusts (159-161).
  - `std::mem::zeroed::<libc::tcp_info>()` is a valid value (216) — the SAFETY comment asserts no
    niches; verify against `libc`'s definition.
  - `offset_of!(tcp_info, tcpi_sacked) + size_of::<u32>()` (237) is the correct "filled through"
    threshold, and `tcpi_unacked` really precedes `tcpi_sacked`.
  - `TCP_LISTEN = 10` matches `include/net/tcp_states.h`, and the state check genuinely prevents
    reporting real unacked/SACKed segment counts as an accept queue.
  - `DropCounter::delta`'s `wrapping_sub` is correct for any interval below 2³² drops, and the
    "first sample is absolute" choice is safe for every caller today (both samplers open their own
    socket).
  - Both functions return `None` (never garbage) for a non-socket fd, a UDP fd passed to
    `listen_queue`, an established socket, and an old kernel.
  - `receive_utilization` may legitimately exceed 1.0 and must not be clamped.
- **Observed concerns (unverified):** ~~none spotted. The Linux-only twins, the length checks and the
  state check are all present and tested against real sockets.~~ Substantially right about the
  `unsafe`, wrong about the coverage. Verified 2026-09-21 (see below): every ABI claim holds, but
  two documented *mechanisms* were wrong, and three of the module's own branches were untested.
  - **Confirmed, fixed:** both wrappers discarded `errno`, so an `EBADF` — a stale or reused
    descriptor, the only cause that would be a real bug — was reported to the operator as "your
    kernel is too old". They now return `Unavailable`, which carries the `io::Error`.
  - **Confirmed, fixed:** `receive_utilization`'s doc explained readings above 1.0 as a
    charge-then-uncharge race window. It is not a window: the kernel admits on the *pre-charge*
    total and then charges the whole `truesize`, so a saturated queue settles at up to
    `rcvbuf + truesize`. Checked in v5.10, v6.6 and v6.12 `__udp_enqueue_schedule_skb`. The "never
    clamp" conclusion is unchanged and better supported.
  - **Confirmed, fixed:** both length checks were unreachable on any kernel that has the option at
    all, so neither they nor their constants were exercised by anything. Parsing is now split out
    (`parse_meminfo`/`parse_listen_queue`) and unit-tested at the boundaries.
  - **Confirmed, fixed:** nothing in the workspace read `wmem_alloc`/`sndbuf`, and nothing pinned
    the `SK_MEMINFO_DROPS` index against an independently-produced number — a mutant landing on
    `BACKLOG` (7) or `OPTMEM` (6) passed every test in the tree. Both closed, the second by the
    `/proc/net/udp` cross-check.
  - **Refuted:** nothing about the two `unsafe` blocks themselves. Every index, offset, constant,
    `socklen_t` in/out contract and kernel-version claim checked out against UAPI headers.
- **Existing coverage:** `sockstat.rs`'s own test module —
  `the_first_sample_reports_its_absolute_value`,
  `a_wrap_past_u32_max_reports_the_true_delta_not_a_huge_one`,
  `meminfo_reads_a_real_bound_udp_socket`, `meminfo_of_a_non_socket_fd_reports_enotsock`,
  `listen_queue_reads_a_real_tcp_listener`, `listen_queue_of_a_connected_socket_reports_the_state_it_saw`,
  `listen_queue_of_a_udp_socket_reports_the_syscall_failure`, and (added by `libc/w2`)
  `a_full_length_meminfo_reply_is_read_field_by_field`,
  `a_meminfo_reply_short_of_the_drop_counter_is_refused`,
  `the_listen_queue_is_read_only_from_a_listening_socket_with_both_fields_filled`,
  `only_a_refused_option_is_reported_as_an_unsupported_one`. Consumers' tests: `udp.rs`'s sampler
  block (including `the_kernels_drop_counter_agrees_with_proc_net_udp_to_the_packet`), `tcp.rs`'s
  accept-queue block. ADR: `udp-intake-batching-and-socket-visibility`, plus its 2026-09-21
  amendment.
- **Suggested verification approach:** ~~targeted review against the kernel UAPI headers; a
  real-socket test that cross-checks `logit.input.kernel.drops` against `/proc/net/udp`'s `drops`
  column under a perf-VM `udp-statsd --verify` run; run the unit tests under an older kernel
  container to exercise the short-reply path.~~ Done, except the older-kernel container — which is
  now unnecessary: the short-reply paths are unit-testable directly, and no kernel that has the
  options at all can produce one.
- **Verified 2026-09-21** (at `libc/w0` `c063bc2`, parent `main` `af2ef65`): every invariant above
  checked against primary sources rather than recalled — `include/uapi/linux/sock_diag.h`,
  `include/uapi/linux/tcp.h`, `include/net/tcp_states.h`, `include/net/sock.h`, `net/core/sock.c`
  (v4.11/v4.12/v6.12), `net/ipv4/udp.c` (v5.10/v6.6/v6.12), `net/ipv4/tcp.c` (v2.6.24),
  `net/socket.c` and `net/ipv4/inet_connection_sock.c` (v6.12), and `libc` 0.2.189's gnu/musl/arch
  modules. All hold. The findings are the four "confirmed, fixed" items above; the full citation
  set, the `libc`-bump review notes (gnu's `tcp_info` omits the kernel's eighth `__u8` and relies
  on `repr(C)` padding) and the reachable-errno enumeration are in the ADR's 2026-09-21 amendment.
  Layout and index constants are now module-scope `const _: () = assert!(…)` tripwires, so a `libc`
  bump that moves any of them is a build failure. `parse_meminfo`/`parse_listen_queue` are pure and
  run under `miri` via `script/unsafe-check`; both `getsockopt` sites run under `cargo-careful`.
- **Priority:** P1 — `unsafe` and hand-rolled, but both calls are read-only, bounded, well guarded,
  and a mistake degrades telemetry rather than the data path.

---

### NET-12 — The two kernel samplers: coop-budget arm ordering, self-disable, and the guaranteed final sample
- **Location:** `crates/logit-inputs/src/udp.rs` (`KERNEL_SAMPLE_INTERVAL`, `read_loop_sampled`,
  `sample_while`, `ReceiveBufferSampler`) and `crates/logit-inputs/src/tcp.rs:712-846`
  (`ACCEPT_QUEUE_SAMPLE_INTERVAL`, `AcceptQueueSampler`). The UDP half was verified in `libc/w1`;
  the TCP half belongs to `libc/w2`.
- **What it does:** Both wrap the driver's own blocking operation in a `select! { biased; sleep =>
  …, work => … }` so the kernel counters are sampled once a second *while* the work arm is parked
  or starved. The UDP one re-polls the *same* pinned `read_loop` future each tick (never dropping
  it, which would lose a datagram), and guarantees one final sample after `read_loop` returns. Both
  disable themselves permanently on the first failed read, after one `warn`, and then arm no timer
  at all.
- **Why sensitive:** concurrency/cancellation (the UDP sampler's correctness rests on *not*
  cancelling the read future, and on tokio's cooperative-scheduling budget semantics — a `Pending`
  caused by an exhausted coop budget is not a park, so an arm placed behind it never runs);
  hot-path-adjacent (the biased timer arm costs one `Sleep::poll` per wake on the read path);
  accounting (this is the only visibility into kernel-level loss, and the final sample is what
  attributes drops in the last second before a fatal error).
- **Invariants to verify:**
  - ✅ *(UDP, `libc/w1`)* `sample_while` never drops and re-creates the `read` future. **Holds** —
    `std::pin::pin!` gives a `Pin<&mut F>`, which is `Unpin`, so both the disabled-break arm and
    the `select!` arm take `&mut read`, borrowing rather than moving. The future is never dropped
    before the loop ends, including on the `enabled == false` path this entry calls out.
  - ❌ *(UDP, `libc/w1`)* ~~The final `sampler.sample_once()` runs on **every path**~~ — **false as
    written.** It runs on every path `sample_while` itself *returns* on; a future that is
    **dropped** runs nothing, and `run_input`'s grace backstop (`logit_pipeline::runtime`) drops
    this whole future when `shutdown_grace` expires. Production never reaches it: `read_loop` races
    `shutdown` in both of its own `select!`s so it returns within microseconds, and
    `input_runtime_config` supplies `ReceiveConfig::default()`'s **5 s** for every listener,
    including one with no `receive:` block. `InputRuntimeConfig::default()`'s `Duration::ZERO` is
    a test-only value, and at ZERO both arms are ready at once and `select!`'s rotation drops the
    listener roughly half the time. Claim corrected in the code, the ADR, and here; `runtime.rs`'s
    own stale comment (which said production used the ZERO default) is fixed too. **No runtime
    behaviour changed.**
  - ✅ *(UDP, `libc/w1`)* The `biased;` ordering is preserved. **Holds**, and the tokio source makes
    the argument stronger than the measurement alone did — `select!` gates on the coop budget
    before polling *any* arm, and `Sleep`'s `poll_elapsed` consults coop before its deadline, so
    the read-arm-first ordering leaves the sleep permanently unregistered with the timer driver.
    The existing behavioural pin is genuinely order-sensitive (reversed, `ticks` would be 0 against
    `>= 3` of 6). Two mechanism errors in the code comment are corrected; see the ADR amendment.
  - ✅ *(UDP, `libc/w1`)* A disabled sampler arms no timer. **Holds**; covered by
    `a_disabled_sampler_still_reads_and_closes_the_queue`.
  - *(UDP: ✅ `libc/w1`; TCP: `libc/w2`)* `ReceiveBufferSampler` re-emits `receive_buffer.bytes`
    every sample because `ComponentBuffer::drain` `mem::take`s its point map — a gauge written once
    would vanish from the series; same reasoning for `accept_queue.limit`.
  - ✅ *(UDP, `libc/w1`)* `kernel.drops` is emitted only when nonzero, matching every other loss
    counter. **Holds.**
  - ✅ *(UDP, `libc/w1`)* The bare fd held by the sampler cannot outlive its socket. **Holds at
    compile time**, more strongly than "cannot outlive" suggests: the sampler is a local of
    `sample_while`, itself a local of `read_loop_sampled(socket: &UdpSocket, …)` — a future that
    *borrows* the socket — and `run_until_shutdown` declares `socket` before `read`, so reverse
    drop order drops the sampler first on every exit including unwind and the `Err` path. (TCP half
    to `libc/w2`.)
  - `AcceptQueueSampler::accept` is cancellation-safe: losing the arm to `shutdown` costs at most one
    sample and never a connection (`TcpListener::accept` takes nothing off the queue unless it
    returns).
  - *(TCP half, added by `libc/w2`)* The interval tick actually fires under a steady accept rate —
    `AcceptQueueSampler::accept`'s loop turns once per accepted connection, so a timer rebuilt per
    turn never comes due; and the socket gauged is the socket accepted on.
- **Observed concerns (unverified):**
  - *Low confidence:* the coop-budget argument is specific to tokio's current 128-unit budget and to
    `Sleep::poll` calling `coop::poll_proceed` first. A tokio bump could change either; the pin is a
    behavioral test (`the_sampler_keeps_ticking_while_the_read_future_burns_its_whole_coop_budget`)
    rather than a version assertion, which is the right shape but easy to miss in a bump review.
    — **Confirmed** (`libc/w1`), and addressed as far as it can be without a version assertion:
    `sample_while`'s doc now names `tokio-1.53.1` and enumerates the four exact source facts
    (`Budget::initial() == 128`; `async_io`'s success-vs-`WouldBlock` budget spend; `poll_elapsed`'s
    `poll_proceed`-before-deadline; `select!`'s `poll_budget_available`), and the ADR amendment
    carries the same list under a "Re-verify on a dependency bump" heading. Note that the coop
    consult is in `Sleep::poll_elapsed`, not `Sleep::poll` — this entry's own wording was one frame
    off.
  - *Low confidence:* ~~`tcp.rs`'s sampler is biased-first "for uniformity" although the comment
    states either ordering is correct there — the uniformity argument is sound, but it means the
    TCP listener pays a `Sleep::poll` per accept for no measured benefit.~~ **Confirmed and worse
    than stated, fixed in `libc/w2`** (TCP half only; the UDP half of this entry is `libc/w1`'s).
    It was not a `Sleep::poll` per accept but a timer-wheel insert *and* a lock-taking cancel per
    accept, on every stream listener in the process — tokio 1.53.1 registers a `Sleep`'s
    `TimerEntry` lazily on first poll (`init` → `reregister`, driver lock) and cancels it on drop
    (`PinnedDrop` → `cancel` → `clear_entry`, driver lock again, unconditionally). And the fresh
    `sleep(interval)` re-anchored its deadline to `Instant::now()` every turn, so a listener
    accepting faster than once per interval **never got an interval sample at all** — a real
    cadence bug in exactly the busy-listener case the interval sample exists for, pinned now by
    `a_steady_stream_of_accepts_does_not_starve_the_interval_tick` (it saw 20 samples for 20
    accepts and zero ticks before the fix). One `Pin<Box<Sleep>>` per sampler, `reset()` only when
    it fires. `biased;` and sample-before-each-accept are unchanged; the latter is now pinned by
    `the_accept_queue_is_sampled_before_the_accept_not_after_it`, which nothing did before.
  - *Confirmed, fixed in `libc/w2` (TCP half):* `ACCEPT_QUEUE_SAMPLE_INTERVAL`'s doc claimed the
    sampler costs "one `getsockopt` per listener per second". It is that plus one per accepted
    connection.
  - *Confirmed, fixed in `libc/w2` (TCP half):* `the_kernel_accept_queue_gauges_are_reported_for_a_running_listener`
    asserted `utilization` lives in `[0, 1]` against a real socket — false, since
    `sk_acceptq_is_full` is strictly greater-than and a `listen(1)` socket settles at depth 2. It
    did not flake as written (backlog in the hundreds) but was wrong as a statement about the
    metric, and `internal-telemetry.md`'s row was off by one in the same way.
  - *Confirmed (the fd), fixed differently than suggested:* the stored `fd` was never a lifetime
    risk — the sampler is a local declared after the listener at all four call sites — but it made
    `sampler.accept(&other_listener)` compile and silently gauge the wrong socket. `BorrowedFd<'_>`
    would have fixed the lifetime, not the identity. The TCP sampler now reads the fd off the
    `listener` argument at each sample; `ReceiveBufferSampler` keeps its stored fd
    (`docs/known-gaps.md`).
- **Existing coverage:** `udp.rs` tests `the_kernels_own_drops_and_receive_buffer_fill_are_reported`
  (1866), `a_full_receive_buffer_is_reported_as_used_bytes_and_a_utilization_ratio` (1930),
  `the_final_sample_reports_drops_that_happened_just_before_shutdown` (1979),
  `a_sampler_that_cannot_read_the_counters_disables_itself_on_the_first_sample` (2037),
  `a_disabled_sampler_still_reads_and_closes_the_queue` (2058),
  `the_sampler_keeps_ticking_while_the_read_future_burns_its_whole_coop_budget` (2121, with the
  `burns_its_whole_coop_budget_forever` helper at 2099). `tcp.rs` tests 3263, 3303, 3321. ADR:
  `udp-intake-batching-and-socket-visibility`.
- **Suggested verification approach:** targeted review; re-run the coop-budget pin on every tokio
  bump; a perf-VM run at ~90% kernel loss confirming per-window `kernel.drops` still appear (the
  ADR's own measurement).
- **Verified 2026-09-21, TCP half only** (at `libc/w0` `c063bc2`, parent `main` `af2ef65`; the UDP
  half is `libc/w1`'s): `AcceptQueueSampler`, `ACCEPT_QUEUE_SAMPLE_INTERVAL` and the accept loop
  checked against tokio 1.53.1's own `time/sleep.rs`, `runtime/time/entry.rs` and
  `net/tcp/listener.rs`, and against `include/net/sock.h` / `net/ipv4/inet_connection_sock.c` at
  v6.12 for the queue semantics. Cancel safety, the disabled-sampler no-timer rule, the
  `accept_queue.limit` re-emission and the "losing the arm costs at most one sample" claim all
  hold. The findings are the four confirmed items above; see the ADR's 2026-09-21 amendment for the
  quoted tokio internals. `crate::udp::sample_while` was checked for the same timer problem and
  deliberately left alone — its loop turns once per tick, not once per read.
- **Priority:** P1 — telemetry-only, but this is the *evidence path* for every loss claim, and the
  arm-ordering bug it fixes was invisible until measured.
- **Verified 2026-09-21 — UDP half only** (`libc/w1`, atop `libc/w0` `c063bc2`, parent `main`
  `af2ef65`; the TCP half and this entry's index Status wording belong to `libc/w2`): the four
  UDP-side invariants above re-checked against tokio tag `tokio-1.53.1` — `pin!`'s `Unpin`
  guarantee, `select!`'s `poll_budget_available` gate, `Sleep::poll_elapsed`'s `poll_proceed`,
  `Budget::initial() == 128` — and against the code for the drop-order and enabled-flag claims. One
  invariant ("the final sample runs on every path") is **false as written** and is corrected in all
  three places it was stated; no runtime behaviour was changed, because the path that skips it is
  unreachable in production and changing it would mean weakening the grace backstop. The
  coop-budget concern is confirmed and mitigated with a named, version-pinned re-check list rather
  than a new assertion.

---

### NET-13 — Listener wrappers: framing/transport selection and the `Decoder: Clone` per-connection contract
- **Location:** `crates/logit-inputs/src/statsd.rs:243-244, 261-266, 301-315, 463-491` (`Inner`,
  `StatsdInput::tcp`, `Input` impl); `crates/logit-inputs/src/syslog.rs:124-125, 144-147, 182-190,
  338-366`; `crates/logit-inputs/src/graphite/mod.rs:133-134, 163-166, 383-392, 395-430`
  (`framing()` and the `bind()`-time `set_framing`); `crates/logit-inputs/src/collectd.rs:94-179`.
  Driver side: `crates/logit-inputs/src/tcp.rs:1008-1020` (`with_framing`/`set_framing`),
  `tcp.rs:20-25` (why `D: Clone` is load-bearing).
- **What it does:** Each protocol wrapper is a thin `enum Inner { Udp(UdpListener<D>),
  Tcp(TcpListener<D>) }` that picks the driver, the framing mode, and the frame bound, and forwards
  `bind`/`run`/`run_until_shutdown`. `statsd_in` fixes `FramingMode::Lines { DrainToNextLine }` at
  construction; `syslog_in` keeps the driver default `Rfc6587Auto` and turns *off* the decoder's own
  line splitting on TCP; `graphite_in` defers the framing decision to `bind()` because
  `max_line_bytes`/`max_frame_bytes` are builder fields that may be set in any order;
  `collectd_in` is UDP-only and leans on `bind_one`'s multicast detection.
- **Why sensitive:** data-loss/duplication (the wrong framing mode silently mis-frames a whole
  stream — a statsd line beginning with a digit read as an RFC 6587 octet count is the concrete
  hazard the explicit `FramingMode` exists to prevent); concurrency (every accepted connection gets
  its own decoder clone, so any decoder state that must *not* be shared, and any that must, depends
  on the `Clone` impl being right); accounting (a `Diagnostics` clone shares throttle counts, so
  `with_diagnostics` must reach both the driver and the decoder).
- **Invariants to verify:**
  - `graphite_in`'s deferred `set_framing` runs before any connection is served on *every* entry
    path — `run_until_shutdown` calls `self.bind()` explicitly for exactly this reason
    (`graphite/mod.rs:416-430`); a caller reaching the driver's own `run_until_shutdown` directly
    would get `Rfc6587Auto`.
  - No wrapper can reach `FramingMode::Rfc6587Auto` for a protocol whose lines may start with a
    digit (statsd, carbon plaintext).
  - `with_diagnostics` reaches both the listener's own `diag` and the wrapped decoder's (the
    regression each wrapper has a named test for).
  - Each decoder's `Clone` impl produces the per-connection independence the driver assumes —
    especially `GraphiteDecoder` (which the module doc says preserves interner state) and
    `SyslogDecoder::with_line_splitting(false)` on TCP.
  - `statsd_in`/`graphite_in` TCP have **no** receive queue, so `receive:` fields that only make
    sense for UDP are either mapped onto `TcpListenerConfig`'s four batching fields or rejected by
    graph validation.
- **Observed concerns (unverified):** none spotted in the glue itself. The `graphite_in`
  deferred-framing arrangement is the one place a future refactor could regress silently.
- **Existing coverage:** per-wrapper tests: `statsd.rs:1147` / `1163`
  (`with_diagnostics_reaches_the_wrapped_decoder_too`, `…_a_tcp_connections_decoder`),
  `graphite/mod.rs:616` / `635` (`with_receive_round_trips_…_on_both_transports`,
  `with_diagnostics_reaches_the_wrapped_decoder_on_both_transports`), `689`
  (`two_concurrent_tcp_connections_both_deliver`), `collectd.rs:315` / `321` / `332`.
  Driver-level: `tcp.rs:2857` / `3161` (framing-mode matrices). ADRs: `graphite-carbon-relay`,
  `syslog-tcp-ingress-and-tls`, `collectd-binary-relay`.
- **Suggested verification approach:** targeted review; a table-driven test asserting every shipped
  `ComponentKind`/`transport`/`protocol` combination resolves to the expected `(FramingMode,
  max_frame_bytes)` pair.
- **Priority:** P2 — thin plumbing, but the framing-mode choice is the high-consequence bit and it
  is centralized in four small functions.

---

### NET-14 — Listener TLS termination: `rustls::ServerConfig` construction from operator PEM
- **Location:** `crates/logit-inputs/src/tls.rs:49-90` (`build_server_config`), `29-39`
  (`TlsServerSettings`); used by `crates/logit-inputs/src/tcp.rs:974-981` (`TcpListener::with_tls`)
  and the accept-loop TLS arm `tcp.rs:1148-1171`.
- **What it does:** Reads a PEM certificate chain and private key (and an optional client-CA
  bundle) relative to the config file's directory, builds a `rustls::ServerConfig` on the `ring`
  provider with safe default protocol versions, installs a `WebPkiClientVerifier` when a client CA
  is configured (mutual TLS) or `with_no_client_auth` otherwise, and sets the ALPN list (empty for
  the stream driver).
- **Why sensitive:** nontrivial-3p-use(rustls — `builder_with_provider` +
  `with_safe_default_protocol_versions` + `add_parsable_certificates`, rather than the ordinary
  `ServerConfig::builder()`); untrusted-input (PEM paths come from config, but the handshake itself
  faces the network); the mTLS branch is a security boundary.
- **Invariants to verify:**
  - `add_parsable_certificates` (`tls.rs:80`) *silently skips* unparsable certs and returns counts
    that are discarded — an operator's typo'd CA bundle could produce an empty or partial root store
    and therefore a verifier that trusts less (or, if empty, `WebPkiClientVerifier::builder(...)
    .build()` should error — confirm it does rather than accepting everyone).
  - `with_safe_default_protocol_versions().expect(...)` (65-66) can genuinely never fail with the
    `ring` provider.
  - The ALPN list is empty for `syslog_in`/`graphite_in`/`statsd_in` and that is correct for a
    non-HTTP protocol.
  - Paths resolve against the config file's directory consistently with every other `*_file` field.
  - The `TlsAcceptor` is built once and only `Arc`-cloned per connection (`tcp.rs:1087`).
  - The TLS accept is bounded by `handshake_timeout` and the permit is released on failure
    (`tcp.rs:1150-1171`).
- **Observed concerns (unverified):**
  - *Medium confidence:* `roots.add_parsable_certificates(ca_certs)` discards its `(added, ignored)`
    return. A bundle where some certs fail to parse yields a quietly smaller trust set with no
    diagnostic. Worth either logging the ignored count or erroring.
- **Existing coverage:** `tcp.rs` tests `a_tls_connection_round_trips_a_decoded_frame` (2698),
  `a_client_trusting_the_wrong_ca_is_refused_and_the_listener_keeps_serving` (2719),
  `mutual_tls_accepts_a_client_certificate_and_refuses_a_client_without_one` (2746), with fixtures
  from `testdata_dir()` (2205) and the client-side helpers at 2212-2277. ADR:
  `syslog-tcp-ingress-and-tls`, `otlp-tls-and-pooled-grpc-client`.
- **Suggested verification approach:** targeted review of the `add_parsable_certificates` return
  handling; a negative test with a CA bundle containing one valid and one corrupt PEM block.
- **Priority:** P2 — startup-time construction with real mTLS tests behind it; the silent-skip is the
  one thing worth closing.

---

### NET — Cross-cutting notes

**Shared helpers other areas depend on.**
- `logit_pipeline::BoundedQueue` (`crates/logit-pipeline/src/queue.rs:156-659`) is shared with the
  *sink* side: `SinkQueue` (`queue.rs:698-806`) is a `BoundedQueue<(Arc<EventBatch>, BatchContext)>`
  using `peek`/`commit` rather than `pop`/`pop_many`, over the same `Mutex`, the same two `Notify`s
  and the same `close()` semantics. Any loom/shuttle model built for the intake side should cover
  the `peek`-reservation path too, since the head reservation is exactly what makes `peek` +
  `commit` cancellation-*unsafe* while `pop` is safe. Coordinate with whoever owns
  `SinkQueue`/`runtime`.
- `logit_pipeline::BatchAccumulator` (`crates/logit-pipeline/src/accumulator.rs:62-259`) is used
  identically by the UDP decode loop and by every TCP connection task, and its
  `next_deadline` delegates to `runtime::advance_flush_deadline` — shared with `run_transform`'s
  flush timer. Its incremental `estimated_heap_bytes` arithmetic is exactness-critical for
  `batch_max_bytes` and is pinned by `crates/logit-bench/tests/allocations.rs`.
- `logit_pipeline::sockstat` is deliberately protocol-free and is expected to gain a
  `logit-outputs` consumer (send-side `sk_wmem_alloc`). Its `None`-means-"stop asking" contract is
  what both samplers' self-disable logic rests on.
- `crate::udp::now_nanos` (`udp.rs:1236-1238`) is `pub(crate)` and is the single clock both drivers
  stamp `received_at` from (`tcp.rs:1570-1572`).
- `crate::tcp::far_future` (`tcp.rs:1586-1588`) is `pub(crate)` and is also used by `logit_in` and
  `otlp_in`'s idle deadlines — those two listeners are outside my area but share this and the
  `idle_timeout` semantics.
- `crate::tls::TlsServerSettings` / `build_server_config` is shared by `tcp.rs`, `logit.rs` and
  `otlp.rs`; `apply_client_tls` is `prometheus_in`'s client side and belongs to whoever surveys that.

**Noticed outside my area, for another surveyor.**
- `crates/logit-pipeline/src/accumulator.rs:261-262` has a duplicated `#[cfg(test)]` attribute on
  the test module. Harmless, cosmetic.
- `crate::logit` (`crates/logit-inputs/src/logit.rs`) and `crate::otlp`
  (`crates/logit-inputs/src/otlp.rs`) each have their own accept loop, connection cap, handshake and
  idle handling that deliberately diverge from `tcp.rs`'s (the "Connection limit" doc section at
  `tcp.rs:39-47` names one such divergence). They should be surveyed as a unit with this entry's
  accept-loop findings — in particular the fatal-`accept()`-error question, which likely applies to
  all three.
- The codecs themselves (`StatsdDecoder::decode_into` `statsd.rs:538+`, `SyslogDecoder`
  `syslog.rs:438+`, `logit_proto::graphite`/`collectd`) are another agent's area, but note that
  `SyslogDecoder::decode_into` is documented as *infallible* — so the TCP driver's `bad_frame`
  counter is never reachable for `syslog_in`, and all syslog decode failures surface as the
  decoder's own `bad_line`. That asymmetry between drivers and decoders is worth one shared check.
- `docs/known-gaps.md` documents, as deliberate: one reader per UDP listener (no `SO_REUSEPORT`
  fan-out); read and decode sharing one task; the `ReceiveQueue` being in-memory only (no
  crash-recovery for intake, unlike the disk-backed *sink* buffer); `read_batch` being Linux-only;
  and a connection still within its idle budget at shutdown. None of these should be reported as
  surprises.


---

## TAIL — File tailing and Docker json-file ingestion

Area: `crates/logit-inputs/src/tail/{driver,watch,checkpoint,line,pattern,mod}.rs`,
`crates/logit-inputs/src/docker.rs`.
Governing ADRs: [`docs/adr/file-tailing-and-docker-json-logs.md`](../adr/file-tailing-and-docker-json-logs.md)
and its partial supersession [`docs/adr/docker-container-identity-and-minimal-watches.md`](../adr/docker-container-identity-and-minimal-watches.md).
Telemetry contract: [`docs/design/internal-telemetry.md`](../design/internal-telemetry.md#tail_in-and-docker_in).
Documented deliberate gaps: [`docs/known-gaps.md`](../known-gaps.md#file-tailing-and-docker-logs).

Third-party crates actually in play here: `tokio` (fs, `AsyncFd`, `select!`, `watch`), `bytes`,
`serde`/`serde_json` (checkpoint file, `config.v2.json`, json-file envelope), and **`libc` only**
(Linux-only dep, `crates/logit-inputs/Cargo.toml:55-60`). There is deliberately **no** `notify`,
no `inotify` crate, no `glob`, no `tempfile` — inotify, globbing, line splitting, and the
scratch-dir test helper are all hand-rolled (ADR "Alternatives considered").

---

### TAIL-01 — Rotation / truncation / removal reconciliation in `scan`
- **Location:** `crates/logit-inputs/src/tail/driver.rs:350-424` (`Tailer::scan`), `:430-449`
  (`refresh_identity`), `:456-482` (`reconcile_truncation`), `:507-514` (`on_data_wake`); state
  enum at `:79-92` (`FileState`), maps at `:132-156` (`files`, `by_path`, `resume`).
- **What it does:** Every poll tick (and on every `Wake::Discover`/`Wake::Overflow`) it re-lists
  matching paths, stats each one, and classifies: same path + same `(dev,ino)` → truncation check
  plus a factory `refresh`; same path + new inode → mark the old entry `Draining`, count
  `files.rotated`, open the new inode at offset 0; tracked path no longer discovered → mark
  `Draining`. Truncation is inferred purely from `len < tracked.offset`, and resets the file
  offset, the `LineSplitter`, and the decoder's own cross-line state.
- **Why sensitive:** custom (inode-identity rotation detection written from scratch); data-loss
  (a wrongly-classified rotation loses the tail of the old inode); duplication (a false
  truncation re-reads from 0); concurrency/TOCTOU (path is `read_dir`'d, then separately
  `metadata`'d, then separately `open`'d — three races with logrotate/dockerd);
  accounting (`files.rotated`/`files.truncated` must match reality for an operator to trust it).
- **Invariants to verify:**
  - A rename-then-recreate rotation always drains the old inode to real EOF before closing it
    (`reap_drained` only reaps ids in the current pass's `at_eof`, `:748-762`).
  - A `copytruncate` rotation (same inode, length reset) is detected and never splices the
    pre-truncation partial onto the new generation (`:477-478` rebuilds the splitter and calls
    `decoder.reset()`).
  - `by_path` and `files` stay mutually consistent: every `Active` inode is reachable from exactly
    one `by_path` entry, and no `by_path` entry points at an id absent from `files`
    (the ownership check at `:558-560` is the subtle one).
  - A path discovered twice in one scan (two patterns) and a rotation pair discovered in either
    HashMap iteration order converge on the same tracked set.
  - `Draining` is never reaped before EOF, and reaping removes the file from the next checkpoint
    write only after its accumulator was flushed.
- **Observed concerns (unverified):**
  - **Transient discovery failure looks identical to removal.** `PathPattern::scan` returns an
    empty `Vec` when `read_dir` fails for *any* reason (`pattern.rs:108-110`, `:124-126`), and
    `scan`'s stale loop (`driver.rs:363-371`) then marks **every** tracked file `Draining`. Those
    files drain, close, and are dropped from `files`; on the next successful scan they re-enter
    via the `None` arm with `first == false` → `StartOffset::Beginning` (`:398-403`) → the entire
    file is re-emitted. One EMFILE/ENFILE, a permissions flap, or an NFS hiccup on `read_dir`
    therefore replays every tailed file from byte 0. Medium-high confidence from reading; the
    `resume` map is deliberately *not* populated for the `Draining` reap path (`:773-784`).
  - **Truncation detection can be missed when the writer immediately refills.** `len >=
    tracked.offset` short-circuits (`:459-461`); a `copytruncate` followed quickly by enough
    writes to exceed the old offset before the next tick leaves the offset pointing into the new
    generation — lines are then silently skipped/garbled with no diagnostic. Inherent to
    size-based detection; not listed in `known-gaps.md`. Medium confidence.
  - `scan` does blocking `read_dir`/`metadata` (and, for `docker_in`, `read` of every
    `config.v2.json`) directly on the async worker thread; `pattern.rs:96-102` argues this is
    cheap, but it is O(containers) per tick on a busy host. Low severity, high confidence.
- **Existing coverage:** `driver.rs` tests `rotation_by_rename_drains_the_old_inode_then_follows_the_new_one`
  (:1227), `a_draining_file_with_more_than_one_chunk_of_backlog_is_fully_read_before_close` (:2015),
  `a_truncation_discards_the_partial_line_held_from_the_previous_generation` (:2061),
  `a_wildcard_matching_both_a_rotated_file_and_its_replacement_keeps_one_entry_per_inode` (:2098),
  `rebinding_a_renamed_inode_never_removes_a_by_path_entry_another_inode_now_owns` (:2152),
  `a_data_wake_for_a_path_a_new_inode_now_owns_is_not_a_truncation` (:1961). No test covers a
  failing `read_dir`.
- **Suggested verification approach:** proptest/state-machine against a model filesystem driving
  (append, rename+create, copytruncate, delete, read_dir-fails) sequences and asserting the emitted
  line multiset equals the written one modulo permitted duplicates; plus a real `logrotate` run
  (both `rename` and `copytruncate` modes) in the dev container.
- **Priority:** P0 — custom rotation bookkeeping on the main data path; a misclassification is
  silent loss or a full-file duplicate burst.

### TAIL-02 — Start-offset selection, inode rebinding, and the `resume` map
- **Location:** `crates/logit-inputs/src/tail/driver.rs:525-628` (`open_tracked`), `:105-111`
  (`StartOffset`), `:139-147` (`resume` field), `:388-405` (the `None` arm that picks the start),
  `:773-784` (`reap_drained`'s `Deselected` retention).
- **What it does:** Decides where a newly discovered inode starts reading — a checkpoint/de-selection
  `resume` entry wins, else `read_from` on the very first scan, else byte 0. An inode already
  tracked under a different name is *rebound* (path swapped) rather than reopened, preserving
  offset/splitter/decoder/accumulator. A `Resume(off)` past current length falls back to 0.
- **Why sensitive:** duplication (reopening instead of rebinding replays a whole file); data-loss
  (a wrong `End`/resume offset skips content); custom (all of it); bookkeeping whose error silently
  orphans an entry from `by_path`.
- **Invariants to verify:**
  - `resume` entries are consumed exactly once and only after `accept` succeeded (`:568-573`) —
    the "peeked, not removed" comment at `:389-395` is the argument.
  - `read_from` applies only on the first scan, never to a later discovery.
  - A `Deselected` entry is never revived by the rebind branch (`:532-541`).
  - The rebind branch's `by_path` ownership check (`:558-560`) never evicts another inode's
    binding, in either HashMap iteration order.
  - A `Draining` entry revived by the rebind branch keeps its offset and continues, not restarts.
- **Observed concerns (unverified):**
  - **`resume` is keyed on `(dev, ino)` with the stored path never compared** (`:396-403`,
    `checkpoint.rs:66-105`). An inode number recycled by the filesystem between the checkpoint
    write (or a de-selection) and rediscovery would apply a stale byte offset to unrelated
    content — skipping data rather than duplicating it. The stored `path` is explicitly described
    as "for a human reading the file, never used to match" (`checkpoint.rs:17-25`). Plausible on a
    churning Docker host where containers are created and removed constantly. Medium confidence
    that it's reachable, high confidence the guard is absent.
  - `resume` entries for inodes never rediscovered are never evicted; the map grows to the size of
    the checkpoint plus every de-selection for the life of the process. Low severity.
- **Existing coverage:** `checkpoint_is_written_on_interval_only_when_dirty_and_resumes_by_inode`
  (:1498), `checkpoint_with_an_offset_past_the_file_size_restarts_at_zero` (:1543),
  `a_reselected_file_resumes_at_the_retained_offset_rather_than_replaying` (:1458),
  `read_from_end_skips_preexisting_lines_and_read_from_beginning_replays_them` (:1164),
  `a_file_created_after_startup_is_discovered_and_read_from_the_beginning` (:1204).
- **Suggested verification approach:** targeted review plus a fault-injection test that recycles an
  inode (delete + create in a loop until `ino` repeats on tmpfs) and asserts the new file is read
  from 0.
- **Priority:** P0 — start offset is exactly the loss/duplication knob, and the identity key has a
  known blind spot.

### TAIL-03 — Read → split → decode → batch hot loop, and its backpressure contract
- **Location:** `crates/logit-inputs/src/tail/driver.rs:636-661` (`drain`), `:669-734` (`read_one`),
  `:900-903` (`emit`); accumulator contract in `crates/logit-pipeline/src/accumulator.rs:149-180`.
- **What it does:** Round-robins over every tracked file, reading at most one 64 KiB chunk each,
  splitting into lines, decoding each line, absorbing into that file's own `BatchAccumulator`, and
  `Fanout::send`-ing whenever a bound is hit. It repeats until no file made progress. There is no
  receive queue by design — the file is the buffer and the loop simply stops advancing `offset`
  when downstream is slow.
- **Why sensitive:** hot-path (per byte and per line); backpressure (the whole "no drop policy"
  claim rests on this loop's shape); data-loss (offset advances per *chunk* at `:698`, before the
  lines it contains have been delivered); cancellation (shutdown is only checked between files,
  `:647-649`).
- **Invariants to verify:**
  - `offset` never advances past bytes that have been split into lines and absorbed — and the
    checkpoint's `offset - pending_bytes` subtraction (`:826-830`) is the only thing making the
    per-chunk advance safe.
  - A slow/full `Fanout` stalls the loop without dropping anything and resumes at the same offset.
  - The round-robin genuinely prevents one busy file starving others (one chunk per file per pass).
  - A read error (`:677-681`, returns `false`) can never cause a non-`Draining` file to be reaped.
  - Shutdown observed mid-drain (`:647`) returns before any further read, and everything already
    absorbed is still flushed by the shutdown path.
- **Observed concerns (unverified):**
  - **Timer starvation.** `drain`'s inner `loop` (`:642-660`) only exits when *no* file made
    progress; a continuously-written file keeps `any_progress` true, so the `select!` (and hence
    the flush timer, the checkpoint timer, and the poll tick) is never reached while writes keep
    up with reads. Batches still flush on `max_events`/`max_bytes`, but `checkpoint_interval` and
    `flush_interval` can both be starved indefinitely under sustained load. Medium-high confidence.
  - **Per-read allocation and an extra copy on the hot path.** `read_one` allocates and zeroes a
    fresh 64 KiB `vec![0u8; READ_CHUNK_BYTES]` on *every* call for *every* file (`:673`) and then
    copies the used prefix again into `Bytes::copy_from_slice(&chunk[..n])` (`:689`). Under
    `watch: inotify` with frequent small writes this is 64 KiB of zeroing per wake per file. The
    "zero-copy" claim in `line.rs:70-76` is relative to this already-copied chunk. High confidence
    it's real; severity is perf, not correctness. No allocation-count test covers `tail`
    (`crates/logit-bench/tests/allocations.rs` has no tail stage).
  - `drain` allocates a `Vec<FileId>` of all tracked ids on every inner iteration (`:645`).
- **Existing coverage:** `downstream_backpressure_pauses_reading_without_loss` (:1682),
  `two_files_are_read_round_robin_so_a_busy_file_cannot_starve_the_other` (:1747),
  `a_draining_file_with_more_than_one_chunk_of_backlog_is_fully_read_before_close` (:2015). No
  bench in `crates/logit-bench`, no perf scenario in `perf/scenarios/` for tailing.
- **Priority:** P0 — this is the per-byte data path and the entire backpressure argument; the timer
  starvation interacts directly with the checkpoint window.

### TAIL-04 — `LineSplitter`: framing, partial carry-over, and `max_line_bytes` drop semantics
- **Location:** `crates/logit-inputs/src/tail/line.rs:77-162` (`LineSplitter`, `push`,
  `take_partial`, `pending_bytes`), `:164-170` (`strip_cr`).
- **What it does:** Splits read chunks on `\n`, strips a trailing `\r`, carries an unterminated
  line across chunks in a `BytesMut`, drops (never truncates) any line exceeding `max_line_bytes`
  and keeps a `dropping` flag until that line's newline arrives. `pending_bytes()` is what the
  checkpoint subtracts so a persisted offset never covers an unemitted line.
- **Why sensitive:** hot-path (per byte); custom (hand-rolled framing, no `memchr`); data-loss
  (a mis-accounted `pending_bytes` makes a restart skip a real line); untrusted-input
  (`max_line_bytes` is the only bound on memory held for one line).
- **Invariants to verify:**
  - Every byte fed in is either emitted in exactly one line, held in `partial`, or attributed to a
    counted drop — no byte silently vanishes and none is emitted twice.
  - `pending_bytes()` equals exactly the bytes consumed-but-not-emitted, including in the
    `dropping` state (where it is 0 by design — see `driver.rs:818-825`).
  - The boundary is consistent: a line of exactly `max_line_bytes` is kept in both the
    single-chunk (`:112`) and spanning (`:123`) branches.
  - `dropped_lines` counts each oversized line exactly once, not once per chunk (`:117-126`).
  - `take_partial` clears `dropping` and yields `None` when the held content was already dropped.
  - A `\r` that is real data at end-of-line is indistinguishable from CRLF framing — accepted.
- **Observed concerns (unverified):** the newline search is a byte-at-a-time
  `iter().position()` (`:100`) rather than `memchr`; on a 64 KiB chunk of long lines this is the
  dominant per-byte cost. Perf only, high confidence. Nothing else spotted; the drop-accounting
  branches read correct.
- **Existing coverage:** eight unit tests at `line.rs:240-339` including
  `a_line_over_the_limit_spanning_chunks_is_dropped_whole_and_resumes_after_it` (:291),
  `zero_max_line_bytes_drops_every_line` (:302),
  `a_fully_contained_lines_message_is_a_zero_copy_slice_of_the_chunk` (:329); plus
  `a_checkpoint_offset_never_covers_a_line_still_held_as_a_partial` (`driver.rs:2232`).
- **Suggested verification approach:** proptest — feed an arbitrary byte stream in arbitrary chunk
  splits and assert (emitted lines ++ partial ++ dropped bytes) reconstructs the input exactly, and
  that `pending_bytes()` matches the model at every step.
- **Priority:** P0 — pure, self-contained, per-byte, and the source of the checkpoint's safety
  margin; an off-by-one here is silent loss on restart.

### TAIL-05 — Checkpoint persistence: atomicity, durability, and the corrupt-file fallback
- **Location:** `crates/logit-inputs/src/tail/checkpoint.rs:59-158` (`CheckpointStore::load`,
  `mark_dirty`, `write`), `crates/logit-inputs/src/tail/driver.rs:816-831` (`write_checkpoint`),
  `:299-309` (interval tick: flush-then-write), `:317-319` (shutdown: close, flush, forced write).
- **What it does:** A JSON `{version, files:[{dev,ino,path,offset}]}` document written to a `.tmp`
  sibling and renamed over the real path, only when dirty (or forced), always preceded by a flush
  of every accumulator, always with each file's held partial subtracted from its offset. Pruning is
  implicit: only currently-tracked files are written. Load treats missing, unreadable, malformed,
  and wrong-version alike — diagnosed, then ignored.
- **Why sensitive:** durability/crash-consistency (this is the restart contract); data-loss vs
  duplication (which way a partial failure falls); custom (hand-rolled atomic-write); accounting
  (`offset - pending_bytes`).
- **Invariants to verify:**
  - The persisted offset is always ≤ "bytes whose lines are already with `Fanout`" — i.e. the
    flush-before-write ordering at `:299-309` and `:317-319` is never reordered.
  - A crash at any instant leaves either the old or the new checkpoint parseable, never a torn one.
  - A failed write leaves `dirty` set so the next tick retries (`checkpoint.rs:146-157`).
  - The forced shutdown write runs after both `close_all_for_shutdown` and `flush_all`, so
    `pending_bytes()` is 0 by then.
  - Pruning never drops an entry for a file still being tailed.
- **Observed concerns (unverified):**
  - **No `fsync`.** `std::fs::write` + `std::fs::rename` (`checkpoint.rs:144-145`) with no
    `sync_all` on the tmp file and no directory fsync. On power loss (not a process crash) the
    rename can be durable while the data is not, yielding an empty or truncated checkpoint. Load
    then treats it as missing (`:89-95`) and **every file falls back to `read_from`, whose default
    is `End`** — i.e. the corrupt-checkpoint path fails towards *silent loss* of everything written
    while down, not towards duplication. The ADR's "strictly duplicates, never loss" claim
    ([`file-tailing-and-docker-json-logs.md`](../adr/file-tailing-and-docker-json-logs.md#checkpoints-optional-written-on-an-interval-only-when-dirty)) does not cover this case. High confidence the
    fsync is absent; medium on how often it matters. Compare `disk_queue.rs`, which does fsync
    ([`known-gaps.md`](../known-gaps.md#native-wire-format-logit_inlogit_out-and-buffering)).
  - **Blocking I/O in an `async fn`.** `write_checkpoint` is `async` but performs `std::fs::write`
    + `rename` synchronously on the runtime worker; likewise `CheckpointStore::load` inside
    `bind()`. Low severity, high confidence.
  - **`tmp` path derivation.** `self.path.with_extension("tmp")` (`:144`) *replaces* the existing
    extension; two tail listeners with checkpoint paths differing only in extension would collide.
    No graph rule appears to enforce checkpoint-path uniqueness across components (grep of
    `crates/logit-pipeline/src/graph.rs` finds no `checkpoint_path` rule). Low likelihood.
- **Existing coverage:** `checkpoint.rs:161-262` (round-trip, no-op-when-clean, forced, prune,
  bad-version); `driver.rs` `an_interval_checkpoint_flushes_before_writing_its_offset` (:2205),
  `a_checkpoint_offset_never_covers_a_line_still_held_as_a_partial` (:2232),
  `checkpoint_prunes_entries_for_missing_paths_on_write` (:1577),
  `shutdown_flushes_every_accumulator_and_writes_the_checkpoint_within_grace` (:1613). No
  fault-injection test.
- **Suggested verification approach:** fault injection — `kill -9` between write and rename, and a
  simulated torn/zero-length checkpoint on load; decide explicitly whether the corrupt-file
  fallback should be `Beginning` rather than `read_from`.
- **Priority:** P0 — this is the durability boundary, and its failure mode currently points at loss
  rather than duplication.
- **Verified 2026-09-24** (dur/w6): the loss lead is confirmed and fixed. Against the old code, an
  empty, truncated, wrong-version, or stray-tmp checkpoint under `read_from: end` delivered none of
  the pre-existing lines
  (`an_unusable_checkpoint_starts_every_preexisting_file_at_the_beginning_even_under_read_from_end`
  timed out); `CheckpointStore::load` now returns `Loaded::Unusable` for each, and the first scan
  starts every file at 0, counted `logit.input.checkpoint.errors{op="load"}`. Every write goes
  through `logit_pipeline::atomic_write::write_file_durably` on the blocking pool, so the missing
  `fsync`s, the runtime-thread I/O, and the `with_extension` tmp collision are gone; a freeze or
  an `EIO` at each of its four steps leaves the old (or, after the rename, the new) checkpoint
  loadable and the store dirty for the next tick (`a_crash_at_any_step_of_a_write_leaves_the_previous_checkpoint_loadable`,
  `a_failed_write_at_any_step_leaves_the_store_dirty_and_the_next_write_lands`). Graph rule 62
  rejects two tailing listeners sharing a literal `checkpoint_path`. The flush-before-write and
  close-flush-write shutdown orderings are unchanged and still covered by the driver tests above;
  `load` stays a blocking read at bind, an accepted startup cost.

### TAIL-06 — Shutdown ordering and final flush of held state
- **Location:** `crates/logit-inputs/src/tail/driver.rs:312-321` (loop exit, then close/flush/
  checkpoint), `:748-786` (`reap_drained`), `:797-804` (`close_all_for_shutdown`), `:806-814`
  (`flush_all`), `:840-872` (`close_decoder`).
- **What it does:** On shutdown, every still-tracked decoder is given a chance to emit its held
  unterminated line (`LineSplitter::take_partial`) and whatever `TailDecoder::close` produces
  (`docker_in`'s dangling reassembly), then every accumulator is flushed
  (`FlushReason::Shutdown`), then the checkpoint is force-written. A per-file close on rotation/
  removal/de-selection runs the same `close_decoder` with `FlushReason::Closed`.
- **Why sensitive:** cancellation (the runtime aborts this task once `shutdown_grace` expires —
  `crates/logit-pipeline/src/runtime.rs:345`); data-loss (the held partial exists nowhere else once
  its bytes are inside `offset`); shutdown ordering (checkpoint must be last).
- **Invariants to verify:**
  - Ordering close → flush → checkpoint is preserved, so an abort mid-sequence can only cost
    duplicates, never a persisted offset ahead of delivery.
  - `close_all_for_shutdown` deliberately does *not* remove entries from `files`, so
    `write_checkpoint` still sees every offset (`:788-796`).
  - An unterminated final line is emitted exactly once, and not also re-read after a restart
    (its bytes *are* included in the shutdown checkpoint offset because `pending_bytes()` is 0 by
    then — verify the two halves agree).
  - `reap_drained` flushes before dropping the entry, so a rotated-away file's last partial batch
    is never lost.
  - `emit`'s `sink.send().await` inside the shutdown path can block; confirm what the runtime does
    when the grace expires there, and that nothing is half-sent.
- **Observed concerns (unverified):** `TailBatching::shutdown_grace` (`tail/mod.rs:68`) is carried
  into `TailConfig` but never read inside the tail driver — the grace is enforced only externally
  by the runtime. That's consistent with other listeners, but means the driver has no internal
  bound on how long the final flush may block. Low-medium confidence that it matters.
- **Existing coverage:** `shutdown_flushes_every_accumulator_and_writes_the_checkpoint_within_grace`
  (`driver.rs:1613`), `an_unterminated_last_line_is_held_until_its_newline_arrives_and_emitted_on_close`
  (:1651), `a_partial_entry_is_emitted_on_close_rather_than_lost` (`docker.rs:729`).
- **Priority:** P1 — correct in the tested paths; the untested interaction is an abort during a
  blocked final send.

### TAIL-07 — Hand-rolled `inotify` backend: every `unsafe`/syscall site in this area
- **Location:** `crates/logit-inputs/src/tail/watch.rs`, `mod inotify` (`:337-` after `libc/w3`;
  the line refs below are the post-`libc/w3` ones, since the module roughly doubled in size).
  The `unsafe` sites, exhaustively — still six, unchanged in kind: `:515` `inotify_add_watch`,
  `:531` `inotify_rm_watch`, `:648` `libc::read` inside `AsyncFd::try_io`, `:688`
  `inotify_init1`, `:692` `OwnedFd::from_raw_fd`, `:740` `ptr::read_unaligned` of
  `libc::inotify_event`. (Plus test-only `write_unaligned` at `:859`/`:874` and a `pipe2` in the
  dead-fd test.) Masks at `:405-419`; bookkeeping at `:428-`; `parse_events` at `:724-`.
- **What it does:** Opens one non-blocking, cloexec inotify fd wrapped in `tokio::io::unix::AsyncFd`;
  registers `DIR_MASK` watches on pattern directories and `FILE_MASK` (`IN_MODIFY`) watches on each
  open file; reads raw event batches into a reused 64 KiB buffer and decodes them into
  `Wake::{Discover,Data,Overflow}`, purging a watch descriptor on `IN_IGNORED`.
- **Why sensitive:** unsafe/syscall (six sites); untrusted-input in the parsing sense (the buffer
  is kernel-supplied but is walked with manual offset arithmetic and an unaligned struct read);
  concurrency (fd ownership across `AsyncFd`); accounting (~~`wd` reuse — a stale map entry
  misattributes a later unrelated watch~~ — refuted, see below; the real accounting hazard was a
  reverse index that outlived its descriptor).
- **Invariants to verify:**
  - `parse_events`' loop arithmetic never reads out of bounds: header check at `:450`, name check
    at `:460-462`, advance at `:499`; `event.len` is attacker-irrelevant but must still be handled
    when 0 or larger than the remaining buffer.
  - `read_unaligned` is genuinely required and the `inotify_event` layout matches the kernel's on
    every supported target (the flexible `name[]` member is *not* part of the Rust struct).
  - The fd in `AsyncFd<OwnedFd>` outlives every `as_raw_fd()` use, including `rm_watch` during drop
    ordering.
  - `watches`/`by_path` never leak: `IN_IGNORED` purge (`:474`) plus explicit `unwatch`/
    `unwatch_dir` (`:375-384`) cover every removal path, and a reused `wd` can never resolve to a
    stale path.
  - `watch_file` has no path dedup by design (`:365-369`) — verify `open_tracked` really calls it
    exactly once per tracked file and `reap_drained` unwatches exactly once (`driver.rs:610`,
    `:765-767`).
  - A short `read` that splits an event across two reads: does the kernel guarantee this can't
    happen for a buffer ≥ one event? The `break` at `:461` silently discards the remainder if it
    ever does.
- **Observed concerns (verified 2026-09-21 — see below):**
  - ~~`next_wake` returns `std::future::pending()` forever if `AsyncFd::readable()` errors
    (`:391-396`)~~ — **CONFIRMED and fixed.** It did, silently. Reachable only on runtime
    shutdown (tokio's `Registration::readiness` returns `Err(gone())` solely when
    `ev.is_shutdown`), so benign as liveness, but invisible as observability.
  - ~~A read error other than would-block is swallowed with an empty arm (`:415`)… this is a busy
    loop~~ — **CONFIRMED, and worse than "busy":** tokio's `AsyncFdReadyGuard::try_io` clears
    cached readiness *only* on `WouldBlock`, and `AsyncFd::readable()`'s path
    (`Registration::readiness` → `ScheduledIo::readiness`) carries no `coop::poll_proceed` budget
    check, so the loop never returns `Ready` *or* `Pending` — the driver's whole task wedges,
    taking the poll, flush and checkpoint ticks with it. Demonstrated live: reverting the fix made
    `a_watcher_whose_fd_reads_as_broken_dies_once_and_then_parks` run past 600s without even its
    own `tokio::time::timeout` firing. Both arms now retire the wake source as `Wake::Dead`.
  - ~~`logit.input.watch.watches` counts *intended* watches~~ — **CONFIRMED**, and
    `docs/deploying.md` asserted the opposite ("Under `inotify`/`auto` the two coincide"), now
    corrected. A failed directory watch no longer inflates it; a draining file whose inode is gone
    and an aliased directory still do. The leak detector it removes is replaced by
    `the_live_kernel_watch_count_matches_this_watchers_own_bookkeeping`, which reads
    `/proc/self/fdinfo/<inotify fd>`.
  - **Not listed here, and the top finding: a pattern directory was armed exactly once, ever.**
    `reconcile_watches` (`driver.rs:334-346`) armed only the set difference and recorded
    `watched_dirs = desired` regardless of the syscall's result; `Tailer::patterns` never mutates,
    so every later scan iterated two empty differences. Missing at `bind`, deleted-and-recreated,
    or renamed-away all lost the watch permanently. Two independent further causes: `watch_dir`'s
    `by_path` short-circuit against an entry `IN_IGNORED` never purged, and no `IN_MOVE_SELF` in
    `DIR_MASK`. All three verified by reverting each fix separately and watching the new tests
    fail.
  - **Refuted, and worth not re-litigating:** *`wd` reuse* — the kernel allocates cyclically from
    1 (`idr_alloc_cyclic`, v3.10 commit `a66c04b4534f`) and before that with a `*last_wd + 1`
    cursor that never wrapped, so reuse needs a process to cycle `1..INT_MAX` (`inotify(7)` BUGS);
    "a stale map entry misattributes a later unrelated watch" is not a practical concern on any
    kernel, and the purge earns its place by bounding the maps instead. *Unbounded `pending`
    growth* — `next_wake` reads only when `pending` is empty, so it is bounded at
    `EVENT_BUF_BYTES / 16` = 4096 entries at all times. *Overflow losing tracked files' writes* —
    `Wake::Overflow` rescans and `drain` (unconditional after every `select!` iteration) re-reads
    every tracked file in the same iteration. *inotify-rs#156 (one read per readiness edge)* —
    does not apply: tokio's readiness is cached and cleared only by a `WouldBlock` `try_io`, which
    is level-trigger emulation, so the `loop { readable().await; try_io(..) }` shape is correct
    (verified in tokio 1.53.1 `io/async_fd.rs`, `runtime/io/registration.rs`).
- **Existing coverage (as surveyed):** `watch.rs:503-689` — **ten** tests, not nine (the survey
  undercounted by one), including two real-fd integration tests
  (`inotify_watcher_watch_dir_wakes_discover_on_a_child_file_created` :635,
  `inotify_watcher_watch_file_wakes_data_on_its_own_write` :653) and
  `parse_events_purges_an_ignored_watch_and_emits_no_wake_for_it` (:593). Driver-level:
  `under_inotify_*` tests at `driver.rs:1811`, `:1878`, `:1916` and `docker.rs:1513`, `:1573`.
  **Now 34** across `tail::watch` (28 in the `inotify` submodule), plus four new driver-level ones.
- **Suggested verification approach:** targeted review of the six `unsafe` blocks against
  `inotify(7)`; miri is not applicable (real syscalls) but `parse_events` alone is pure and could
  be fuzzed with arbitrary byte buffers; an fd/watch-leak soak test (`ls /proc/self/fd`,
  `/proc/self/fdinfo/<inotify fd>` watch count) under repeated rotation.
- **Priority:** P1 — the only `unsafe` in the area, but memory-safety-wise it is small, well
  commented, and the parse half is unit-tested; failure mode is mostly degraded wakes rather than
  corruption.
- **Verified 2026-09-21 @`c063bc2` (parent `main` @`af2ef65`), branch `libc/w3`.** All six
  `unsafe` sites reviewed against v6.12 `fs/notify/inotify/inotify_user.c`,
  `inotify_fsnotify.c`, `include/linux/fsnotify.h`, `inotify(7)`/`inotify_add_watch(2)`/
  `inotify_rm_watch(2)`, and tokio 1.53.1's `AsyncFd`/`Registration`/`ScheduledIo` — every kernel
  or tokio claim now in a comment was fetched from the source, not recalled, and the guarantees
  relied on are written down in [ADR `file-tailing-and-docker-json-logs`](../adr/file-tailing-and-docker-json-logs.md)'s
  2026-09-21 amendment. **Invariants I1–I6 as worded:** I1 holds on 64-bit and was a real 32-bit
  hardening gap (`event.len as usize` widening a `u32`), now closed with `checked_add`; I2 holds,
  and `size_of`/`align_of`/every field offset of `libc::inotify_event` is now a const assertion;
  I3 holds (no `Drop` impl, nothing calls `rm_watch` from drop glue, and tokio's `AsyncFd::drop`
  discards its deregister result and never calls `Handle::current()`); **I4 was broken** — the
  `IN_IGNORED` purge covered `watches` only, and `by_path` was not even passed to `parse_events`;
  I5 holds (`open_tracked` returns early on both rebind branches before `watch_file`, and
  `reap_drained` removes from `self.files` before `unwatch`); I6 holds — the kernel never returns
  a partial event. The test fixture `raw_event` was **kernel-infidel** (padding to 4, and `len == 4`
  for a nameless event where the kernel emits 0), which is why the mutant that advances by
  `header_len` alone survived every test; it now reproduces `round_event_name_len` exactly.
  Coverage added: a mixed named/nameless multi-event buffer, both fit-check boundaries, an
  overflowing `len`, `IN_IGNORED` ORed with another bit, a seeded truncation/bit-flip/inflated-len
  sweep (`parse_events_survives_seeded_truncation_and_bit_flips`, in the style of
  `logit-proto/tests/robustness.rs`, miri-friendly via `cfg!(miri)`), the three directory re-arm
  scenarios against a real fd, a `with_init`-over-a-pipe test for the dead-fd arm, and the
  `/proc/self/fdinfo` watch-leak soak. `script/unsafe-check`'s existing `logit-inputs|parse_events`
  filter matches every new pure test by name, so `MIRI_TARGETS` needed no change.

### TAIL-08 — The runtime `select!`: wake routing, timers, and cancellation safety
- **Location:** `crates/logit-inputs/src/tail/driver.rs:229-321` (`run_until_shutdown`), `:96-103`
  (`Outcome`), `:909-914` (`sleep_until_opt`), `:207-227` (`bind`); the awaited wake future is
  `watch.rs:386-419` (`InotifyWatcher::next_wake`).
- **What it does:** Races shutdown, the inotify wake, the poll tick, the flush tick, and the
  checkpoint tick; each arm yields a plain `Outcome` with no `.await` inside it (deliberately, so
  the `select!` future stays `Send` under `#[async_trait]`). All real async work happens after the
  `select!` resolves, and `drain` runs unconditionally after every iteration.
- **Why sensitive:** cancellation (every non-selected branch's future is dropped mid-poll each
  iteration); concurrency (`bind` vs lazy-bind, the `watcher.take()` dance at `:242`); data-loss
  (a wake dropped at the wrong moment means bytes wait for the poll tick — or forever, if state
  was lost with the future).
- **Invariants to verify:**
  - `next_wake` is cancel-safe: dropping it must not lose already-read-and-parsed events. It looks
    safe because parsed events land in `self.pending` (`watch.rs:413`) and the only `.await` is
    `readable()`, but confirm `try_io`'s readiness-clearing semantics across cancellation.
  - `shutdown.wait_for(|&due| due)` being dropped repeatedly does not lose the shutdown edge.
  - Timers re-arm correctly: `next_poll` only in the `Poll` arm (`:291`), `next_flush`/
    `next_checkpoint` only in theirs; a long `drain` leaves a past deadline that fires immediately
    (not drift-accumulating), which is intended but should be confirmed.
  - `bind()` idempotency (`:208-210`) and `run_until_shutdown`'s `expect("bind() leaves a watcher
    behind")` (`:242`) can never panic on any real call path.
  - A `Wake::Data` for an untracked or rotated path is a no-op (`:507-514`) — the ADR's argument
    for why the stale-inode check is load-bearing.
- **Observed concerns (unverified):** the flush/checkpoint timers can be starved by `drain` (see
  the hot-loop entry). Otherwise none spotted; the `Outcome`-value design is unusually carefully
  argued (`:254-262`).
- **Existing coverage:** `under_inotify_a_new_file_is_discovered_well_before_the_poll_interval`
  (:1811), `under_poll_a_new_file_is_discovered_only_after_the_poll_interval` (:1844),
  `a_data_wake_for_a_path_a_new_inode_now_owns_is_not_a_truncation` (:1961), `bind_*` tests
  (:1068, :1089, :1110).
- **Suggested verification approach:** targeted review for cancel-safety; a `tokio::time::pause`
  test that drives a long drain and asserts the checkpoint tick still lands.
- **Priority:** P1 — correctness of the loop shape is well argued; the residual risk is timer
  starvation and cancel-safety of the hand-rolled wake future.

### TAIL-09 — Docker json-file envelope decode and 16 KiB partial-line reassembly
- **Location:** `crates/logit-inputs/src/docker.rs:141-154` (`PartialEntry`), `:163-218`
  (`DockerDecoder`, `emit`), `:220-228` (`JsonFileLine` borrowed deserialize), `:231-301`
  (`decode_line`), `:303-309` (`close`), `:311-318` (`reset`).
- **What it does:** Parses each line as Docker's json-file envelope, maps `stream` to a `'static`
  str, parses the envelope's own RFC-3339 `time` (falling back to read time with a `bad_time`
  diagnostic), reassembles entries whose `log` doesn't end in `\n` into one logical line, enforces
  `max_line_bytes` over the reassembled message with a `dropping` flag, and copies the envelope's
  `attrs` map onto the event verbatim.
- **Why sensitive:** hot-path (per line, plus a `serde_json` parse per line); untrusted-input
  (the `log` field is entirely container-controlled; `attrs` keys become interned symbols);
  data-loss/duplication (reassembly state spanning lines); custom (the reassembly and the
  drop-flag protocol, mirroring `LineSplitter`'s).
- **Invariants to verify:**
  - Every complete logical line is emitted exactly once, with the *closing* entry's
    timestamp/stream/attrs (`:289-299`), and a held `PartialEntry` is emitted on `close` (`:303-309`)
    but discarded on `reset` (`:311-318`).
  - `dropping` clears on exactly the entry that closes the oversized line, and never swallows the
    following good entry (the truncation interaction is the case `reset` exists for).
  - Memory held for one reassembly is bounded: the check at `:278-285` happens *after* the append,
    so the bound can be overshot by at most one entry (~16 KiB) — confirm that's the intent.
  - `max_line_bytes` is applied twice (per-fragment at `:278`, on the completed message at `:292`)
    and the two agree.
  - `strip_suffix('\n')` at `:291` is total given `is_complete` (`:263`).
  - Unknown `stream` returns `Malformed` (`:243-248`) and emits nothing — the driver counts it as
    `bad_line` and clears scratch (`driver.rs:724-728`).
- **Observed concerns (unverified):**
  - **A malformed entry in the middle of a reassembly silently splices across the gap.**
    `decode_line` returns `Err` before touching `self.partial`/`self.dropping` (`:237-248`), so a
    held fragment survives and the next closing entry joins it to content from a different logical
    line, with only a throttled `bad_line` to show for it. Medium confidence it is reachable (a
    torn read of the json-file, or a `stream` value Docker adds later).
  - **Interner growth from container-supplied `attrs` keys.** `emit` interns every key of the
    envelope's `attrs` object (`:199-203` → `AttrMap::insert`), and the interner is process-wide
    and permanent (`docs/design/memory.md` §4 is the reason `labels:` is opt-in for the *resource*
    side). `attrs` is daemon-written from `--log-opt labels/env`, so it should be bounded by
    container configuration rather than log content — worth confirming that's actually true for
    every Docker version. Medium confidence.
  - Every reassembled message is an owned `String` (`:289-291`) by design (documented in the ADR's
    Consequences) — so `docker_in` pays a copy per line that `tail_in` doesn't. Expected, noted for
    the perf picture.
- **Existing coverage:** `docker.rs:604-790` — envelope/stream/time tests, reassembly (:665),
  oversized reassembly (:690), close-emits-partial (:729), never-parses-inner-line (:765); plus
  driver-level truncation interactions
  `a_truncation_discards_a_docker_partial_entry_held_from_the_previous_generation` (:1624) and
  `a_truncation_clears_a_docker_dropping_state_so_the_next_generation_is_not_swallowed` (:1676).
- **Suggested verification approach:** proptest over fragment sequences (including interleaved
  malformed entries and truncations) against a model reassembler; a real `docker run` emitting
  >16 KiB lines on both streams.
- **Priority:** P0 — per-line parsing of container-controlled input with cross-line state; a
  reassembly bug silently corrupts message content.

### TAIL-10 — `config.v2.json` identity cache, refresh, and de-selection
- **Location:** `crates/logit-inputs/src/docker.rs:325-346` (`ConfigStat`), `:348-384`
  (`Identity`, `CachedMeta`, `id_only_resource`), `:406-450` (`refresh_cache`, `cached`), `:452-520`
  (`accept`/`open`/`refresh`/`end_scan`), `:25-53` (`ContainerFilter`, `is_id_prefix`), `:82-122`
  (`ContainerMeta::read`, `resource`), `:129-139` (`split_image_ref`); driver side at
  `driver.rs:430-449` and `:58-77` (`Refresh`).
- **What it does:** Caches each container directory's parsed identity keyed on `config.v2.json`'s
  `(dev, ino, len, mtime)`, re-reading only on a stat change; rebuilds the `Arc<Resource>` and keeps
  the old `Arc` when the new one compares equal (so `BatchAccumulator`'s `ptr_eq` check doesn't
  split batches spuriously); reports `Identity` or `Deselected` back to the driver; evicts entries
  no path reached this scan via a generation counter.
- **Why sensitive:** accounting/bookkeeping (the `Arc` identity *is* the batch boundary);
  data-loss (a de-selection closes a live file); untrusted-input (`config.v2.json` is an
  undocumented daemon-internal format — [`known-gaps.md`](../known-gaps.md#file-tailing-and-docker-logs)); custom (stat-cache protocol,
  generation eviction, image-ref splitting).
- **Invariants to verify:**
  - An unchanged `config.v2.json` costs exactly one `stat` per container per scan and no read.
  - `stat == None` (missing file) is never treated as "unchanged" (`:418`) so a not-yet-written
    config is retried every tick.
  - `metadata_error` fires on the transition into failure only (`:437-443`), and a recovery clears
    `failed`.
  - Value-equality keeps the same `Arc` (`:431-434`), so an unrelated daemon rewrite does not
    force a `ResourceChange` flush.
  - `Refresh::Deselected` never swaps the decoder's resource (the selection check precedes the
    identity check, `:488-500`) so the final flush carries the identity the lines were read under.
  - `end_scan`'s generation eviction (`:515-519`) cannot evict an entry still in use.
  - `split_image_ref` handles `registry:5000/app`, `app@sha256:…`, and `app:tag` correctly
    (`:129-139`).
  - `is_id_prefix`'s ≥12-hex-char rule (`:51-53`) cannot make a *name* accidentally match an id.
- **Observed concerns (unverified):**
  - **A `Deselected` file's container is evicted from the metadata cache while it waits to be
    reaped.** `refresh_identity` removes the `by_path` binding (`driver.rs:445`), so on the next
    scan the path takes the `None` arm and `open_tracked` returns early at `:532-541` *before*
    calling `accept` — meaning `refresh_cache` is not called for that directory and `end_scan`
    evicts its `CachedMeta`. Harmless in effect (the next `accept` re-reads) but it resets `failed`
    and costs a re-read + re-parse; worth confirming it can't oscillate. Medium confidence.
  - `discover: true` short-circuits `accept` before any `refresh_cache` (`:453-457`), so under
    `discover` a container's cache entry is first populated by `open`/`refresh` — verify the
    `id_only_resource` degradation window is only the first scan.
  - `ContainerFilter::matches` (`:41-48`) can match a *name* entry against a container whose
    metadata has never been readable only via the id-prefix path; a name-only selection is
    therefore invisible until `config.v2.json` parses. Documented behavior, but the consequence
    (a selected container silently not tailed) deserves an explicit check.
  - The doc comment at `:38-40` is garbled ("config-validated by graph rule 27 doesn't check this
    specifically") — cosmetic, but it obscures what is actually validated.
- **Existing coverage:** `docker.rs` `a_recreated_container_with_a_new_id_is_picked_up_by_name`
  (:1051), `a_rewritten_config_v2_json_changes_the_name_on_a_fresh_batch_boundary` (:1101),
  `a_metadata_read_that_fails_then_succeeds_recovers_the_full_resource` (:1178),
  `a_stat_changing_rewrite_that_reproduces_the_same_resource_keeps_the_same_arc` (:1254),
  `a_container_renamed_out_of_the_explicit_selection_stops_flowing` (:1318),
  `a_container_renamed_back_into_the_selection_resumes_without_replaying` (:1372),
  `a_short_or_non_hex_entry_never_matches_by_id_prefix` (:894).
- **Suggested verification approach:** targeted review plus a real `docker run`/`docker rename`/
  `docker rm` sequence against a live daemon with both `containers:` and `discover: true`.
- **Priority:** P1 — well tested and the failure modes degrade rather than corrupt, but the
  `Arc`-identity/batch-boundary coupling is subtle and the input format is undocumented.

### TAIL-11 — Pattern discovery: hand-rolled glob and Docker's two-position walk
- **Location:** `crates/logit-inputs/src/tail/pattern.rs:37-93` (`new`, `docker_containers`,
  `matches_name`), `:95-139` (`scan`, `scan_docker_containers`).
- **What it does:** A deliberately minimal glob (literal name, or one `*` in the final component,
  anchored prefix/suffix), and `docker_in`'s non-glob two-position walk that only accepts
  `<root>/<id>/<id>-json.log`. Both are non-recursive, synchronous, and treat any error as "no
  matches, try again next tick".
- **Why sensitive:** custom (a glob crate was explicitly rejected); the anchoring is what stops
  `access.log.1` matching `*.log`, i.e. what stops a rotated file being re-tailed and duplicated;
  the silent-empty-on-error behavior is the input to the mass-`Draining` concern above.
- **Invariants to verify:**
  - `x*y` matches `xy` (zero-width wildcard, `:87`) and never matches shorter names.
  - A rotated `app.log.1` can never satisfy `app.log` or `*.log`.
  - `scan_docker_containers` never accepts a mismatched `<root>/foo/bar-json.log` pair (`:133`).
  - Non-UTF-8 directory entries are skipped, not panicked on (`:112`, `:132`).
  - Symlinked paths/directories behave sanely (`metadata` in `driver.rs:355` follows symlinks,
    `entry.file_type()` at `pattern.rs:128` does not).
- **Observed concerns (unverified):** the error-to-empty conversion (`:108-110`, `:124-126`) is
  indistinguishable from "directory is empty" at the call site — see the `scan` entry's first
  concern. Symlink handling is untested either way. Low-medium confidence.
- **Existing coverage:** `pattern.rs:142-271` — eight tests covering literal/prefix/suffix
  matching, missing directory, docker two-position walk, and `dir()` behavior.
- **Priority:** P2 — small, pure, and well tested in isolation; its risk is concentrated in how
  `scan` interprets an empty result.

### TAIL-12 — Telemetry and diagnostic accounting across the tail driver
- **Location:** `crates/logit-inputs/src/tail/driver.rs:274` and `:290`
  (`logit.input.watch.wakes`), `:284` (`watch.overflows`), `:384` (`files.rotated`), `:410-423`
  (`files.open`, `watch.watches`), `:435`/`:446` (`files.identity_changed`, `files.deselected`),
  `:481` (`files.truncated`), `:712-713` (`logit.input.lines`, `.line.bytes`), `:900-902`
  (`logit.component.receive.flushed{reason}`), `checkpoint.rs:149`
  (`logit.input.checkpoint.writes`); diagnostics keys throughout.
- **What it does:** The only externally visible account of what this listener read, dropped, and
  rotated. Contract documented at [`docs/design/internal-telemetry.md`](../design/internal-telemetry.md#tail_in-and-docker_in).
- **Why sensitive:** accounting (these counters are how an operator detects the loss modes the
  other entries describe); hot-path (two counter calls per line at `:712-713`).
- **Invariants to verify:**
  - `logit.input.lines` counts lines *offered to the decoder*, so it excludes lines dropped by
    `LineSplitter` for `max_line_bytes` and includes lines the decoder then rejects — confirm the
    doc says exactly that, since `lines` ≠ events emitted in both directions.
  - Oversized dropped lines have a diagnostic (`:701-706`) but no dedicated counter; `bad_line`
    likewise only surfaces via `logit.component.diagnostics{key}`. Verify that's sufficient to
    reconcile input bytes against emitted events.
  - `files.open` (`:410`) counts `Draining`/`Deselected` entries too.
  - `watch.watches` reports intended, not live, watches (`:411-423`) — verify the doc's claim and
    that it can't mask a descriptor leak.
  - `watch.wakes{source="inotify"}` is incremented for `Overflow` as well as real wakes (`:274`).
  - `checkpoint.writes` increments only on a successful write, so a persistently failing write is
    visible only as a `checkpoint_error` diagnostic.
- **Observed concerns (unverified):** none beyond the reconciliation gaps listed above; the
  counters look consistent with `internal-telemetry.md`. Low confidence anything is wrong.
- **Existing coverage:** `driver.rs`'s `gauge_value` helper (:1048) and the `under_inotify_*`/
  watch-count assertions; no test reconciles `lines` against emitted events.
- **Priority:** P2 — no data-path risk, but it is the detector for every P0 above.

---

### TAIL — Cross-cutting notes

- **Shared helpers other areas depend on:** `logit_pipeline::BatchAccumulator`
  (`crates/logit-pipeline/src/accumulator.rs:149-180`) — its `Arc::ptr_eq` resource-change flush is
  load-bearing for `docker_in`'s identity refresh *and* for every UDP/TCP listener, so verifying it
  once covers several areas; `FlushReason::Closed` was added for this driver. `Fanout::send` is the
  entire backpressure story here (no receive queue by design — ADR "No receive queue"), so the
  tail driver's loss behavior is inherited from `Fanout`/`runtime.rs`'s channel capacity.
  `logit_core::parse_rfc3339_to_nanos` (`crates/logit-core/src/time.rs:96-101`) is hand-rolled and
  is on `docker_in`'s per-line path — it belongs to whoever surveys `logit-core`, but a panic or
  overflow there lands on this data path.
- **Overlap with the UDP-intake area:** `libc` is shared between `tail/watch.rs`'s inotify backend
  and `udp.rs`'s `recvmmsg`/`sockstat` work ([`known-gaps.md`](../known-gaps.md#udp-intake)'s closed `recvmmsg` entry cross-references them); whoever
  reviews `unsafe` should do both together.
- **Runtime-side dependency:** `shutdown_grace` is enforced by `crates/logit-pipeline/src/runtime.rs:345`
  and `:548-563`, not by this driver — the "what is lost when the grace expires mid-flush" question
  can only be answered in that area.
- **Possibly missing graph validation:** no rule appears to reject two tail listeners sharing a
  `checkpoint_path` (they would clobber each other's file and each other's `.tmp`). Worth a look by
  whoever surveys `crates/logit-pipeline/src/graph.rs`.
- **No bench or perf-scenario coverage at all for tailing** — `crates/logit-bench` and
  `perf/scenarios/` have nothing for `tail_in`/`docker_in`, so the per-read 64 KiB zeroed
  allocation and the byte-at-a-time newline scan are unmeasured.


---

## DISK — Disk spool, rotating file sinks, on-disk frames

Scope: `crates/logit-pipeline/src/disk_queue.rs` (the `buffer.disk:` spool), `crates/logit-outputs/src/file.rs` +
`stdio.rs` (rotating file sink), `crates/logit-proto/src/frame.rs` *as consumed by the disk paths*,
`crates/logit-pipeline/src/queue.rs`'s `SinkStore` seam and `runtime.rs`'s `run_output`/`write_loop` shutdown
ordering, and the `cursor.json` checkpoint. Third-party crates in play on these paths: `tokio` (`fs` feature),
`lz4_flex` (safe-encode/safe-decode/checked-decode), `crc32c`, `serde_json` (cursor file only), `bytes`, `anyhow`.
Everything else on these paths is hand-rolled. `crates/logit-inputs/src/tail` (the other checkpoint) is another
surveyor's.

---

### DISK-01 — DiskQueue::open — crash recovery, torn-tail truncation, cursor reconciliation
- **Location:** `crates/logit-pipeline/src/disk_queue.rs:378-559` (`DiskQueue::open`), plus its helpers
  `load_cursor` `:245-278`, `persist_cursor` `:281-296`, `list_segments` `:115-129`, and the replay-count block
  `:481-497`
- **What it does:** Creates/locks the spool directory (`std::fs::File::try_lock` on `lock`), lists
  `segment-<seq:016>.lgit` files, reads and frame-walks the **highest-numbered (active)** segment in full,
  `set_len`-truncates it back to the last good record boundary, then loads `cursor.json` and clamps it (missing /
  wrong version / malformed / references a vanished segment → oldest surviving segment at offset 0; offset past the
  recovered length → clamped). Finally it re-reads every segment at or after the cursor a **second** time purely to
  count replayable records, emits `disk.truncated` / `disk.replayed` / `batches.dropped{reason="disk_corrupt"}`, and
  force-persists the (possibly clamped) cursor.
- **Why sensitive:** durability — this is the only code that decides what survives a `SIGKILL`; data-loss — a
  wrong `good_len` truncates real records away permanently; untrusted-input — it parses bytes that may be torn or
  arbitrarily corrupt; accounting — `replayed`/`truncated`/`disk_corrupt` are the operator's only signal that
  recovery did anything; custom — the whole scan/resync/clamp algorithm is hand-written.
- **Invariants to verify:**
  - Only the highest-`seq` segment is ever truncated; every lower segment's `len` is trusted from `metadata()` with
    no validation (the "single producer, one write in flight" premise `:23-27` must actually hold).
  - `walk_segment`'s `good_len` never exceeds the last byte of a fully-parsed record, and truncation to it never
    removes a record that would have parsed.
  - A corrupt record mid-active-segment resyncs forward instead of truncating everything after it (this is exactly
    what `MAX_SANE_COMPRESSED_LEN` buys — see the frame entry).
  - Cursor clamping never moves the cursor *forward* past undelivered records, and never lands mid-record.
  - `queued_records` seeded from `replayed` (`:532`) equals the number of records actually readable from the cursor
    onward; `total_bytes` (`:523`) is the sum of *all* segment lengths including already-consumed bytes before the
    cursor — verify that `buffer.bytes`/`utilization` are documented as that (whole-segment granularity), not as
    "bytes still to deliver".
  - Re-opening the same directory from a second process fails at the lock rather than corrupting.
- **Observed concerns (unverified):**
  - The double read of every segment at/after the cursor (validate pass `:428-441` then count pass `:481-497`) is a
    known, still-open startup cost (the `buffered` entry under `docs/known-gaps.md`'s
    [Load-test harness and perf tooling](../known-gaps.md#load-test-harness-and-perf-tooling) section, not its
    buffering section); at the default `segment_bytes` of 64 MiB plus a backlog, `open` reads the whole backlog
    into `Vec<u8>` with `std::fs::read` — peak RSS is proportional to the largest segment. High confidence this is real; it is documented, not a surprise.
  - `persist_cursor` (`:292`) does `std::fs::write(tmp)` + `rename` with **no fsync of the tmp file and no fsync of
    the directory** at that point (only `finish` fsyncs the cursor). On a power loss the rename may land with
    stale-or-empty content. Medium confidence this matters in practice (ext4 `data=ordered` mostly saves it), but
    the ADR's durability claim doesn't cover it.
  - A persistently failing `persist_cursor` is only `warn_throttled` — replay grows without bound and nothing
    counts it. Low-medium.
  - *Closed by #324:* both `persist_cursor` concerns above. The cursor now goes through
    `atomic_write::write_file_durably` (tmp `fsync`, rename, directory `fsync`), and every failure counts
    `buffer.disk.errors{op="cursor"}` (`a_cursor_persist_is_fsynced_before_its_rename_and_the_directory_after`,
    `a_persistently_failing_cursor_write_is_counted_every_time`). The rest of this entry is `dur/w3`'s.
  - `list_segments` (`:121-125`) silently skips anything not matching the pattern, including a file whose seq
    parses but whose name isn't zero-padded; harmless today but the sort is on the parsed `u64`, not the name, so
    keep that the invariant.
- **Existing coverage:** `disk_queue.rs` tests
  `a_segment_truncated_mid_record_recovers_to_the_last_good_frame_and_counts_truncated` (`:1426`),
  `a_crc_corrupted_record_mid_segment_is_skipped_via_resync_and_counted` (`:1458`),
  `a_trace_id_containing_magic_does_not_derail_recovery` (`:1489`), `a_missing_cursor_starts_at_the_oldest_segment`
  (`:1591`), `a_cursor_past_the_end_restarts_at_the_oldest_surviving_segment` (`:1605`),
  `reopen_after_uncommitted_pushes_replays_exactly_those` (`:1401`),
  `a_corrupted_length_field_does_not_silently_discard_the_rest_of_the_segment` (`:1897`);
  integration: `crates/logit-cli/tests/durable_buffer_restart.rs:170`
  (`a_disk_backed_sink_survives_a_simulated_sigkill_and_redelivers_only_what_it_must`).
  ADR: `docs/adr/disk-backed-sink-buffer.md` ("Recovery").
- **Suggested verification approach:** corrupt-segment fuzzing (arbitrary byte mutations over a real multi-record
  segment, asserting "never loses a record that a clean reader would have found before the first mutated byte" and
  "never truncates past `good_len`"); crash-injection harness killing the process at each write/rename/fsync
  boundary and diffing delivered-set against sent-set; a model-based proptest over (push, peek, commit, crash,
  reopen) sequences.
- **Priority:** P0 — custom recovery logic on the main data path; wrong here is silent permanent loss of spooled
  batches.

---

### DISK-02 — Record format, `parse_record`, and `walk_segment`'s resync scan
- **Location:** `crates/logit-pipeline/src/disk_queue.rs:54-63` (`CONTEXT_LEN`), `:92-105`
  (`encode_context`/`decode_context`), `:140-162` (`parse_record`), `:183-232` (`walk_segment`)
- **What it does:** A record is 24 raw bytes (`trace_id` + `span_id`, unframed, unversioned) followed by one
  `logit_proto::frame`. `parse_record` dispatches on the frame's codec byte (`CODEC_NATIVE_V1` → `decode_batch` with
  empty provenance, `CODEC_NATIVE_V2` → `decode_batch_v2`), returning the consumed length. `walk_segment` iterates
  records; a non-`Truncated` error triggers a forward scan for `frame::MAGIC`, backing up `CONTEXT_LEN` bytes to
  find the candidate record start, trying it, and continuing the scan past a spurious match.
- **Why sensitive:** untrusted-input — corrupt bytes reach a hand-rolled scanner and then the native decoder;
  data-loss — a wrong `consumed` desynchronizes the cursor and discards or re-reads everything after;
  duplication — resyncing to a candidate *before* the current position would replay records; custom — the
  `magic_at - CONTEXT_LEN` back-off is bespoke and relies on a documented `frame::resync` caveat.
- **Invariants to verify:**
  - `consumed` returned by `parse_record` is exactly `CONTEXT_LEN + frame bytes`; `before - rest.len()` (`:159`) can
    never over- or under-count given `read_frame`'s advance semantics.
  - The resync loop always makes forward progress (`scan_from = magic_at + 1`, `:218`) and can never set `pos`
    backwards: note `candidate = magic_at - CONTEXT_LEN` (`:210`) can be **less than `pos`** when a spurious MAGIC
    sits within 24 bytes after `pos`; verify that `parse_record(candidate)` succeeding in that case cannot rewind
    the walk (it would double-count/replay).
  - `Truncated` is only ever produced by a genuine short buffer, never by a corrupt length field (this is what
    `MAX_SANE_COMPRESSED_LEN` guarantees — the two files must stay in step).
  - A `CODEC_NATIVE_V1` record written by an older build still decodes (forward/backward compat across the codec
    byte), and an unknown codec byte resyncs rather than aborting the walk.
  - `CONTEXT_LEN` is never widened (`:54-63` says so explicitly) — any change silently misparses every spooled
    record.
- **Observed concerns (unverified):**
  - The `candidate < pos` rewind above is the one I'd check first. Medium confidence it is reachable only with
    adversarial/corrupt bytes; the consequence would be an infinite loop or replay, not silent loss.
  - When nothing is recoverable, `walk_segment` sets `pos = bytes.len()` and counts exactly **one**
    `corrupt_skipped` (`:223-227`) regardless of how many records' worth of bytes were discarded — the drop counter
    under-reports. High confidence; may be deliberate.
- **Existing coverage:** the corruption tests listed in the previous entry, plus
  `a_v1_codec_record_spooled_before_this_change_still_replays_with_empty_provenance` (`:1369`) and
  `provenance_survives_a_spool_round_trip` (`:1345`). No fuzz target.
- **Suggested verification approach:** proptest/fuzz `walk_segment` against a reference model (a list of
  known-good record offsets + an arbitrary mutation set), asserting monotonic `pos`, no duplicate offsets, and
  `good_len` monotonicity. Targeted review of the `candidate < pos` case.
- **Priority:** P0 — it is the parser between corrupt disk bytes and the delivery path, and it is entirely custom.

---

### DISK-03 — `DiskQueue::push` / `write_record` — torn-write repair, `write_in_flight`, cancellation safety
- **Location:** `crates/logit-pipeline/src/disk_queue.rs:597-729` (`push`), `:731-734`
  (`last_write_error_was_disk_full`), `:739-835` (`write_record`), `:837-843` (`open_append`), and the
  `write_in_flight`/`write_len_before_flight`/`last_write_error_disk_full` fields at `:334-344`
- **What it does:** Encodes the batch (`encode_batch_v2` + `write_frame`), rejects anything over
  `MAX_SANE_UNCOMPRESSED_LEN`, decides an overflow action, then appends. Before appending it repairs a tail left by
  a previously *cancelled or failed* write by `set_len`-ing the active segment back to `write_len_before_flight`.
  `write_in_flight` is set before the single `.await`ed `write_all` (+ `flush`) and cleared only on success, so a
  dropped `push` future (`run_output`'s `select!` can cancel `drain_inbox` mid-push) is repaired by the next call.
  On failure the batch is dropped and counted `disk_full` (errno 28) or `disk_io_error`.
- **Why sensitive:** cancellation — correctness rests entirely on "exactly one `.await` in the write path, guarded
  by a flag"; data-loss/corruption — a missed repair leaves garbage bytes mid-segment and an in-memory `len` that
  disagrees with the file; concurrency — `State` is a `std::sync::Mutex` whose guard must never be held across an
  `.await` (clippy-enforced), so every step re-locks and re-reads; accounting — every failure path must count
  exactly one dropped item; hot-path — this is the per-batch cost of a disk-backed sink
  (`disk_queue_push_one_batch` pins 34 allocations).
- **Invariants to verify:**
  - After any `push` returns, `segments.back().len` equals the active segment's real on-disk length.
  - A `push` future dropped at the `write_all`/`flush` await leaves *only* trailing garbage, never a partially
    updated `total_bytes`/`queued_records` (those are updated after the write, `:720-726` — verify).
  - Exactly one of {record durably appended + counters incremented} and {record dropped + counted once} happens per
    `push`; no path both counts a drop and appends.
  - `write_all` + `flush` really does hand the bytes to the kernel (the F2 fix) — `a_pushed_record_is_on_disk_before_push_returns`
    (`:1811`) checks this via an independent fd.
  - `open_append` uses `O_APPEND`, so the repair `set_len` must have succeeded for the next append to land at the
    right offset.
  - `last_write_error_disk_full` is only read while `write_in_flight` is meaningful (`:343` says it is meaningless
    otherwise, but `push` reads it at `:715` after `write_record` returned false — verify both failure paths set it,
    including the `open_append` failure at `:802` which *clears* `write_in_flight`).
- **Observed concerns (unverified):**
  - The repair path ignores both the open and the `set_len` result (`:750-752`, `let Ok(f) = … { let _ = f.set_len(…) }`)
    but unconditionally sets `active.len = repair_len`, `write_in_flight = false` and rewinds `total_bytes`
    (`:753-763`). If the truncate silently fails, the in-memory length diverges from the file, the next `O_APPEND`
    write lands past the torn bytes, and the read cursor will later walk into the gap. Medium confidence
    (needs an EIO/EACCES to trigger), high impact — this is the one place I'd fault-inject first.
  - `open_append` failure sets `write_in_flight = false` (`:801`) *before* `last_write_error_disk_full` (`:802`);
    since `push` then calls `last_write_error_was_disk_full()` under a fresh lock, an interleaved second producer
    could read a stale flag. Low confidence (there is exactly one producer by design), but the flag's "meaningless
    when not in flight" contract is being leaned on across a lock release.
  - `write_frame`'s `.expect(...)` at `:608-609` is justified by "config can't produce Zstd" — a real coupling
    between `logit_config::Compression` and `frame::Compression` that nothing enforces mechanically.
- **Existing coverage:** `an_oversized_batch_is_dropped_and_counted_rather_than_written` (`:1669`),
  `a_failed_write_drops_and_counts_the_batch_rather_than_silently_counting_it_queued` (`:1697`, uses an EISDIR
  trick), `a_pushed_record_is_on_disk_before_push_returns` (`:1811`); allocation pin
  `crates/logit-bench/tests/allocations.rs:1845` (`disk_queue_push_one_batch`). **No test covers the
  cancelled-mid-write repair path** as far as I can see.
- **Suggested verification approach:** fault injection via a FUSE/LD_PRELOAD or a temp filesystem sized to trigger
  real ENOSPC and a read-only remount for EIO; a targeted test that drops a `push` future at the write await (e.g.
  `tokio::time::timeout` around `push` with a paused clock, or a `poll_fn` driving one poll) and then asserts the
  next push repairs; strace to confirm the `set_len`/`write`/`fsync` ordering.
- **Priority:** P0 — the write path for every batch on a disk-backed sink, with hand-rolled cancellation recovery.

---

### DISK-04 — Segment rotation, fsync policy, and `finish`
- **Location:** `crates/logit-pipeline/src/disk_queue.rs:298-300` (`fsync_path`), `:766-772` (the rotate trigger
  inside `write_record`), `:847-867` (`rotate_segment`), `:1166-1183` (`finish`)
- **What it does:** Before a write, if the active segment's length is already `>= segment_bytes`, the current
  handle is flushed, the old segment file is `fdatasync`'d, a new `segment-<seq+1>.lgit` is created and the
  directory is `fdatasync`'d. `finish` force-persists the cursor, flushes the write handle, and `fdatasync`s the
  cursor file, the active segment, and the directory.
- **Why sensitive:** durability — this is the *entire* fsync policy (ADR: no per-push fsync, accepted power-loss
  window); unsafe/syscall — `sync_data` via a freshly opened fd rather than the write handle; data-loss — the
  claim "every segment but the active one is complete and durable" is what lets `open` trust their lengths
  unvalidated; concurrency — the flush must happen outside the `std::sync::Mutex` guard.
- **Invariants to verify:**
  - Directory entry durability: the new segment's `create` is fsync'd via the *directory* (`:863`), and the old
    segment's data via its own fd (`:859`) — verify the ordering is create-then-dirsync-then-record-in-memory.
  - `rotate_segment` only appends to `segments` if the create succeeded (`:862-866`); if it failed, the next
    `write_record` still appends to the over-sized old segment and retries rotation later — verify that
    self-healing actually happens and that `open` copes with an over-`segment_bytes` segment.
  - `fsync_path` opens the path fresh — for the active segment this is a *different* fd from the one that wrote;
    on Linux that still flushes the inode's dirty pages, but confirm that's the intended semantic (and that the
    active segment hasn't been renamed/deleted in between).
  - `finish` drops nothing and returns `(0,0)` (`queue.rs:799-802`), so the cursor it persists must point at the
    first undelivered record, including the one currently held in `head_cache` (peeked but not committed).
  - Segment sequence numbers are strictly increasing and never reused after deletion.
- **Observed concerns (unverified):**
  - Every fsync and the segment `create` are `let _ =` / `.is_ok()` — a failing fsync is completely silent, no
    diagnostic, no counter. The durability claim is therefore unobservable in production. High confidence;
    `:859`, `:862-863`, `:1178-1182`.
  - A failed `rotate_segment` create is silent too (`:862`), so a directory that has gone read-only degrades into
    "one segment grows forever" with no signal. High confidence.
- **Existing coverage:** `push_then_peek_then_commit_round_trips_fifo_across_a_segment_boundary` (`:1316`),
  `a_fully_consumed_segment_is_deleted_once_commit_crosses_it` (`:1620`),
  `the_cursor_rolls_forward_when_a_segment_the_reader_caught_up_to_later_rotates_away` (`:1748`). Nothing asserts
  fsync happened. ADR: `docs/adr/disk-backed-sink-buffer.md` ("Durability"); gap:
  [`docs/known-gaps.md`](../known-gaps.md#native-wire-format-logit_inlogit_out-and-buffering) (accepted power-loss window).
- **Suggested verification approach:** `strace -e trace=fdatasync,fsync,openat,renameat,ftruncate` on a real run to
  confirm the ordering matches the ADR; a power-loss simulation (`dm-flakey` or a qemu drive with write-cache
  reordering) to confirm only the active segment's tail can be lost.
- **Priority:** P1 — the policy is documented and the loss window is accepted, but the silent failure of every
  fsync/create means a real durability regression would be invisible.
- **Verified 2026-09-24** (#324): both concerns confirmed and fixed. Every rotation, `finish`, and unlink
  fs call is now preceded by a `logit_pipeline::fault` check, and `disk_queue.rs`'s tests inject `EIO`/`ENOSPC`/
  `EACCES` at each one and assert `logit.component.buffer.disk.errors{op}` plus a `disk_fs_error` diagnostic
  (`a_failed_segment_fsync_at_rotation_is_counted_and_diagnosed`, `a_failed_directory_fsync_is_counted`,
  `a_failed_segment_unlink_is_counted`); `a_failed_rotation_create_is_counted_and_the_next_push_retries_rotation`
  confirms the self-heal. The fresh-fd `sync_data` holds (Linux `fsync(2)` flushes the inode, not the fd); moving it
  to the retained write handle is `dur/w4`.

---

### DISK-05 — Overflow policy, eviction, and drop accounting on the spool
- **Location:** `crates/logit-pipeline/src/disk_queue.rs:616-705` (the decision loop in `push`, incl. the
  three-way `DropOldest` arm `:651-679`), `:874-897` (`evict_oldest`), `:561-581`
  (`update_gauges`/`count_dropped`/`after_change`)
- **What it does:** Enforces `disk.max_bytes` over the sum of segment lengths. `Block` awaits `not_full`;
  `DropNewest` rejects and counts; `DropOldest` advances the read cursor past whole head records
  (`overflow_oldest`), except that a **reserved (peeked) head** makes it reject the new push instead
  (`overflow_newest`), and a genuinely empty spool accepts over-bound rather than dropping forever. A failed write
  drops under `disk_full`/`disk_io_error` regardless of policy.
- **Why sensitive:** data-loss — every arm here deliberately destroys data and must count it exactly once;
  backpressure — `Block` is the default and parks the producer; accounting — `items_dropped`/`units_dropped` must
  reconcile with what actually left the spool; concurrency — the decision is taken under the lock and then acted on
  after releasing it, with a documented race re-check in `evict_oldest`.
- **Invariants to verify:**
  - Every dropped batch increments `items_dropped` exactly once and `units_dropped` by its event count, under
    exactly one `reason`.
  - `evict_oldest`'s re-check (`:883-888`) genuinely prevents evicting a record a concurrent `peek` reserved, and
    a lost race returns `false` → `push` accepts over-bound rather than looping.
  - `total_bytes` only shrinks on whole-segment deletion, so the `Block` arm cannot deadlock: verify that
    `not_full` is notified on every path that can free space (`roll_read_cursor:1059`, `evict_oldest:895`) and that
    a blocked producer is always woken (including by `close()`, `:1157-1161`, via `notify_waiters`).
  - `notified = self.not_full.notified()` is created *before* the state check (`:635`) — the standard
    lost-wakeup-avoidance ordering; verify it is respected on every loop iteration.
  - `impossible_to_ever_fit` (`:616`) admits a record larger than `max_bytes` rather than blocking forever.
- **Observed concerns (unverified):**
  - Under `DropOldest`, each `Action::Evict` iteration costs a real async read + full native decode of the head
    record just to learn its length (`evict_oldest` → `read_record_at`), and `total_bytes` doesn't shrink until a
    whole segment is deleted — so one `push` against a full spool can evict (and decode) an entire segment's worth
    of records in a loop before the `read_offset < len` check finally lets it write. That's an unbounded amount of
    work and an unbounded burst of `overflow_oldest` drops inside a single `push` call. High confidence this is the
    mechanical behavior; whether it's acceptable is the open question. Not called out in the ADR.
  - `roll_read_cursor` is only called from `push` when the policy is `DropOldest` (`:632`, the "F1" gating) — the
    `Block` path relies on `peek` doing it. Verify there is no state where the producer is blocked, the reader is
    parked, and neither calls `roll_read_cursor`. Low-medium confidence of a gap; the F1 comment at `:994-1002`
    suggests this class of bug has bitten once already.
  - `after_change()` re-locks to read three fields right after `push` already held the lock (`:720-728`) — a
    second lock acquisition per push on the hot path.
- **Existing coverage:** `drop_oldest_advances_past_whole_records_and_reports_units` (`:1509`),
  `drop_oldest_drops_the_newest_rather_than_growing_past_max_bytes_while_the_head_is_peeked` (`:1534`),
  `drop_newest_rejects_the_push` (`:1571`), `the_utilization_gauge_tracks_max_bytes` (`:1647`). ADR:
  `docs/adr/disk-backed-sink-buffer.md` ("Bound and overflow", plus the rejected deferred-skip-cursor alternative).
- **Suggested verification approach:** a model-based proptest (random push/peek/commit/close interleavings against
  a reference FIFO + byte-budget model) asserting `delivered ∪ dropped == pushed` and that every drop is counted;
  a stress test with `max_bytes` just above one segment under `DropOldest` measuring worst-case work per push.
- **Priority:** P1 — deliberate data destruction whose accounting is the only audit trail; the unbounded
  evict-loop is the concrete thing to size.

---

### DISK-06 — Read cursor rollover, segment deletion, and checkpoint cadence
- **Location:** `crates/logit-pipeline/src/disk_queue.rs:1009-1062` (`roll_read_cursor`), `:1068-1080`
  (`advance_read_cursor`), `:1145-1155` (`commit`)
- **What it does:** Advances `read_offset` past a committed/evicted record, then loops forward across any fully
  consumed, non-active segments — persisting the cursor once per roll, removing the left-behind segment files
  (blocking `std::fs::remove_file`, outside the lock), invalidating the cached read fd, and notifying `not_full`.
  Separately, `advance_read_cursor` persists the cursor whenever `checkpoint_interval` has elapsed.
- **Why sensitive:** data-loss — deleting a segment the cursor hasn't truly crossed loses records permanently;
  duplication — a cursor persisted too early replays on restart, too late duplicates; concurrency — `commit` is
  synchronous but performs blocking file I/O from an async task; accounting — `total_bytes`/`segments` gauges are
  maintained here.
- **Invariants to verify:**
  - `read_offset = offset - len` (`:1027`) is only correct if `offset >= len`; the guard at `:1019` must make that
    unconditional. A multi-segment overshoot must land at the right offset in the right segment.
  - A segment is deleted **only** after the cursor has moved to a strictly larger `seq` *and* the new cursor has
    been persisted (`:1031` persists before the deletion loop at `:1033`/`:1056` — verify the cursor write actually
    reached disk before the unlink, otherwise a crash in between loses records with a cursor still pointing at a
    now-missing segment; `open`'s fallback then jumps to the *oldest surviving* segment, which replays but could
    also skip).
  - The active segment is never deleted (`is_active` check `:1015`, `:1019`).
  - `next_seq` is found by "next larger seq", not `seq + 1` (`:1022`) — tolerates a gap from a failed delete.
  - The cached `read_file` is invalidated whenever its segment is removed (`:1039-1043`).
  - `checkpoint_interval` bounds worst-case replay: a crash replays at most the records committed since the last
    persist.
- **Observed concerns (unverified):**
  - `persist_cursor` here is a blocking `std::fs::write` + `rename`, and the deletions are blocking
    `std::fs::remove_file`, both called from `commit()` on a tokio worker thread. Documented as an accepted
    trade-off (`:29-33`), but under `DropOldest` eviction it can fire in a tight loop. Medium confidence this
    matters; worth measuring.
  - `roll_read_cursor` persists the cursor **before** removing files but does not fsync it (see the open entry);
    a crash between the rename and the unlink is the interesting window.
  - `commit()` calls `after_change()` (`:1153`) which re-locks — third lock acquisition per commit.
- **Existing coverage:** `a_fully_consumed_segment_is_deleted_once_commit_crosses_it` (`:1620`),
  `the_cursor_rolls_forward_when_a_segment_the_reader_caught_up_to_later_rotates_away` (`:1748`),
  integration `crates/logit-cli/tests/durable_buffer_restart.rs:390`. ADR: "Read cursor rollover".
- **Suggested verification approach:** crash injection at each of {cursor rename, each unlink} with a subsequent
  reopen, asserting no record is lost (duplicates allowed); proptest over multi-segment overshoot.
- **Priority:** P0 — this is the commit point of the at-least-once contract; an off-by-one deletes undelivered data.

---

### DISK-07 — `peek` / `read_record_at` / `read_at` — the delivery read path and live corruption resync
- **Location:** `crates/logit-pipeline/src/disk_queue.rs:1085-1140` (`peek`), `:905-960` (`read_record_at`),
  `:962-985` (`read_at`), `:320-324` (`HeadCache`)
- **What it does:** `peek` rolls the cursor, returns the cached head if any, otherwise reads and decodes the record
  at `(read_seq, read_offset)` and caches it (`record_len` is the delta the eventual `commit` advances by).
  `read_at` reuses a cached per-segment `tokio::fs::File`, seeks, and fills a buffer, growing the buffer by
  `CodecError::Truncated`'s `needed` hint. On live corruption it re-reads the remainder of the segment and
  `walk_segment`s it, returning `pos + len` so the cursor skips the corrupt bytes too (the "F4" fix).
- **Why sensitive:** hot-path — one peek per delivery attempt (the cached hit is pinned at 0 allocations);
  data-loss/duplication — `record_len` is a cursor delta, so an error here mis-advances; untrusted-input — the same
  corrupt bytes as recovery, now on the live path; concurrency — the cursor can move between the read and the cache
  install (guarded at `:1128`); cancellation — `peek` is one arm of `write_loop`'s `select!` and is dropped
  routinely.
- **Invariants to verify:**
  - `record_len` in `HeadCache` is the delta from the *cursor* (not the record's own size) in both the clean and
    the resync path — this is the exact F4 bug; `:940-948` documents it.
  - The cache is only installed when the cursor hasn't moved (`:1128`); a dropped `peek` future must leave no
    partial state (it holds no lock across an await — verify).
  - `read_at`'s cached fd is always for the segment being read and is dropped when that segment is deleted.
  - `buf.len() < chunk_len` → "real end of segment" (`:922-924`) is sound given `read_at` short-reads only at EOF.
  - Repeated `peek` before `commit` returns the identical `Arc` (retry costs nothing).
- **Observed concerns (unverified):**
  - The live-resync branch counts `outcome.corrupt_skipped.max(1)` (`:955`) where `outcome` is a walk of the
    **entire remainder of the segment** — so every corrupt record later in the segment is counted on *this* peek,
    and counted again on each subsequent peek that hits corruption. `disk_corrupt` can substantially over-report.
    Also the same corrupt record is counted once at `open` and again live (the test at `:1850-1854` explicitly
    drains the open-time count to work around this). High confidence on the mechanism; medium on whether anyone
    considers it a bug.
  - That same branch walks the whole rest of the segment on every corrupt hit → O(n²) over a badly corrupted
    segment. Medium confidence, low practical likelihood.
  - When `read_record_at` returns `None` but the accounting says data is available, `peek` sleeps 1 ms and loops
    **forever** (`:1132-1137`) — it never re-checks `closed()` on that branch (the `closed()` check at `:1108` is
    only reachable when `has_data` is false). If a segment file is removed out of band, `write_loop` never sees
    `Closed` and shutdown depends entirely on the grace timer. Medium confidence, liveness only.
- **Existing coverage:** `peek_is_cached_across_repeated_calls_until_commit` (`:1385`),
  `live_resync_past_corruption_advances_the_cursor_past_the_skipped_bytes` (`:1830`),
  `a_corrupted_length_field_does_not_silently_discard_the_rest_of_the_segment` (`:1897`);
  allocation pin `crates/logit-bench/tests/allocations.rs:1889` (`disk_queue_peek_cached_costs_nothing`).
- **Suggested verification approach:** targeted review of the counting in the resync branch; a test that deletes /
  truncates a segment under a live queue and asserts `peek` still terminates on `close()`; proptest that the sum of
  committed `record_len`s equals the total bytes of the records actually delivered.
- **Priority:** P1 — the correctness-critical part (`pos + len`) is fixed and tested; the residual issues are
  accounting over-count and a liveness edge.

---

### DISK-08 — `Notify`/`closed` wakeup protocol and the `Mutex`-poison posture
- **Location:** `crates/logit-pipeline/src/disk_queue.rs:352-370` (`DiskQueue` fields), `:583-585` (`closed`),
  `:1157-1161` (`close`), plus every `self.inner.lock().unwrap_or_else(|p| p.into_inner())` call site (≈20, e.g.
  `:577`, `:637`, `:721`, `:741`, `:820`, `:1010`, `:1094`, `:1147`, `:1168`)
- **What it does:** One `std::sync::Mutex<State>` guards all cursor/segment/file state; `tokio::sync::Notify`
  pairs (`not_empty`/`not_full`) coordinate the single producer and single consumer; an `AtomicBool` `closed`
  (Acquire/Release) plus `notify_waiters()` releases both sides at shutdown.
- **Why sensitive:** concurrency — a lost wakeup hangs the pipeline; cancellation — both waiters live inside
  `select!` arms in `runtime.rs` and are dropped routinely; data-loss — `unwrap_or_else(|p| p.into_inner())`
  deliberately ignores mutex poisoning, so state observed after a panic mid-mutation is used as if valid.
- **Invariants to verify:**
  - `notified()` is always registered before the state re-check on both the `not_full` (`:635`) and `not_empty`
    (`:1111-1121`) sides; the `peek` side does a double re-check (`roll_read_cursor` then re-read) before awaiting.
  - `close()` uses `notify_waiters()` (wakes all currently registered) while the steady-state paths use
    `notify_one()` — verify a producer registered *after* `close()` still escapes (it does via the `self.closed()`
    short-circuit at `:639`, but the reader side at `:1119` needs the same reasoning).
  - Poison-ignoring is safe here only if no code path can panic while `State` is half-updated; `expect("always at
    least one segment")` appears at `:722`, `:745`, `:768`, `:777`, `:788`, `:850`, `:525` — any of those firing
    poisons the mutex and the next lock proceeds on inconsistent state.
  - `Ordering::Acquire`/`Release` on `closed` is sufficient given the `Mutex` also synchronizes.
- **Observed concerns (unverified):** the `expect("always at least one segment")` family is the invariant a
  poisoned-mutex-ignored design leans on hardest; `rotate_segment` can fail to create a new segment, and
  `roll_read_cursor` can remove entries — worth proving `segments` is never emptied. Medium confidence it holds.
- **Existing coverage:** `close_then_peek_returns_none_when_empty` (`:1734`), and the two roll-forward tests that
  assert `peek` doesn't hang (`:1782`, `:1862`, with explicit `tokio::time::timeout`). No dedicated concurrency
  stress test.
- **Suggested verification approach:** a loom-style or high-iteration randomized concurrency test (producer +
  consumer + close) under `tokio::time::pause`, plus targeted review of the notify/re-check ordering.
- **Priority:** P1 — a lost wakeup is a hang, not corruption, and the roll-forward tests already caught one.

---

### DISK-09 — Sink shutdown ordering: `run_output`'s close-then-sweep, `SinkStore::finish`, and the at-least-once window
- **Location:** `crates/logit-pipeline/src/runtime.rs:588-744` (`run_output`, esp. the `select!` at `:657-667`,
  `store.close()` at `:690`, the abandoned-inbox sweep at `:703-739`), `:1008-1040` (`finish_and_flush`),
  `:1101-1167` (`write_loop`'s peek/deliver/commit, incl. `store.commit()` after a `Dropped` outcome at `:1167`),
  `crates/logit-pipeline/src/queue.rs:776-804` (`SinkStore::finish`)
- **What it does:** `write_loop` and `drain_inbox` race in a `select!`; whichever finishes first, the store is
  closed **before** the abandoned-inbox sweep, and batches still sitting in the mpsc buffer are appended to the
  disk spool (memory stores count them `reason="shutdown"` instead). `finish_and_flush` then calls
  `SinkStore::finish`, which for `Disk` persists+fsyncs and reports `(0,0)` — dropping nothing.
- **Why sensitive:** shutdown ordering — the close-before-sweep ordering exists specifically to avoid a
  `not_full` deadlock; cancellation — `drain_inbox` is dropped mid-`push` by design; duplication — the window
  between `output.send` succeeding and `store.commit()` + cursor persist is the at-least-once window and interacts
  with `Output::duplicate_safe()`; accounting — `reason="shutdown"` must fire for memory and never for disk.
- **Invariants to verify:**
  - No batch is both appended to the spool by the sweep and counted `reason="shutdown"`.
  - `store.close()` before the sweep really does make `DiskQueue::push` non-blocking (the `self.closed()` arm at
    disk_queue `:639`) so the sweep cannot hang against a full spool.
  - A `push` cancelled by dropping `drain` leaves only a torn tail, repaired either by the sweep's next push or by
    the next `open`.
  - `write_loop` returning on shutdown-grace expiry leaves the peeked head *uncommitted*, so `finish` persists a
    cursor pointing at it and it replays (at-least-once, not loss).
  - A `Delivery::Dropped` also commits (`:1167`) — so a batch dropped after a failed send is removed from the
    spool; verify that's intended for a *disk* buffer (it means a destination outage longer than the retry budget
    permanently discards spooled data despite durability being on).
  - The spool may briefly exceed `disk.max_bytes` during the sweep, bounded by the channel capacity and reclaimed
    at next `open`.
- **Observed concerns (unverified):**
  - The `Dropped ⇒ commit` behavior above is the one I'd raise with the author: an operator who enabled
    `buffer.disk:` for durability may not expect a 60 s retry-budget exhaustion to discard a durably spooled batch
    (counted `send_failed`). High confidence on the mechanism; it may well be the intended, documented posture from
    `buffered-sink-delivery`, but the disk ADR doesn't restate it.
  - `DiskQueue::finish` is `async` and awaits fsyncs while shutdown grace has already expired — nothing bounds how
    long `finish` itself takes.
- **Existing coverage:** `crates/logit-cli/tests/durable_buffer_restart.rs` (both tests, incl. the simulated
  SIGKILL at `:170` and the rotation/catch-up case at `:390`); `runtime.rs`'s own "F3 shutdown-sweep" test (uses
  `disk_queue::test_support::encoded_record_len`, `disk_queue.rs:1208`). ADRs:
  `docs/adr/disk-backed-sink-buffer.md` ("Shutdown"), `docs/adr/service-lifecycle-and-output-retry.md`,
  `docs/adr/buffered-sink-delivery.md`.
- **Suggested verification approach:** targeted code review of the four exit paths (drain-first, write-first,
  fatal error, grace expiry) × {Memory, Disk}; a test that asserts exactly-once accounting across a shutdown with
  a full spool under `overflow: block`.
- **Priority:** P0 — the shutdown path decides whether spooled data survives, and the ordering here has already
  been fixed once (F3).

---

### DISK-10 — `file_out` rotation: commit-point-first rename, staging recovery, retention cascade
- **Location:** `crates/logit-outputs/src/file.rs:281-297` (`rotated_path`/`staging_path`), `:299-344`
  (`promote_staged`), `:346-455` (`rotate`/`rotate_inner`), `:184-199` (`open_active`), `:238-269`
  (`ensure_open`/`write_all`/`flush`)
- **What it does:** Flushes the active handle, then (for `max_files == 1`) truncates in place, else: promotes any
  orphaned `.rotating` staging file from a previous crash, renames the active file to `<path>.rotating` (the commit
  point), drops the handle and resets rotation state, re-opens `path` fresh, and promotes the staged file to `.1`
  after cascading `.1→.2→…` and deleting the oldest. Failure policy is deliberately asymmetric (flush → fatal,
  commit rename → `NotRotated`, re-open → `Err(Fault::Clean)`, retention → warn and continue).
- **Why sensitive:** data-loss — a wrong ordering can delete retained history or write new events into an
  already-rotated file; durability — rename atomicity and crash-in-the-window recovery are the whole design;
  custom — a hand-rolled logrotate; concurrency/cancellation — `rotate` is awaited inside `Output::send`, which
  `write_loop` can cancel on shutdown-grace expiry.
- **Invariants to verify:**
  - Nothing retained is touched until the commit-point rename succeeds (`:428`); a failed rename leaves the active
    file, the handle, and every `.N` untouched, and returns `NotRotated` (never counted as a rotation).
  - After the commit point, `self.file = None` (`:436`) before any write can land, so no event is ever appended to
    an already-renamed file.
  - `promote_staged`'s loop `for n in (1..max_files - 1).rev()` (`:324`) produces the right cascade for
    `max_files` 2, 3, and N, and never leaves a gap or overwrites a file it should have cascaded.
  - A `.rotating` orphan from a killed process is promoted exactly once, on the next rotation (`:420`), and the
    just-staged file is promoted unconditionally even if the re-open failed (`:452`).
  - Cancelling `rotate` mid-`await` (only the flush at `:396-400` awaits) cannot leave a staged file unpromoted
    beyond the next rotation.
  - `Fault::Clean` on the re-open (`:444-446`, `:412-414`) never counts toward `write_loop`'s
    sustained-permanent-failure exit window.
- **Observed concerns (unverified):**
  - `max_files` has no upper bound (`logit-config/src/lib.rs:2602`; graph rule only rejects `0`,
    `graph.rs:1483-1486`). `promote_staged` then does `max_files - 2` `exists()`+`rename()` syscall pairs on
    **every** rotation — `max_files: 4294967295` is a config-driven multi-billion-syscall stall. High confidence
    mechanically; operator-error only.
  - Asymmetry under `max_files == 1`: a failed truncate returns `Err` and leaves `state` **un**reset (`:406-415`,
    test at `:982`), so every subsequent `send` re-attempts the rotation and fails → every batch errors. Under
    `max_files >= 2` the same class of failure degrades to `NotRotated` and keeps writing. Medium confidence this
    asymmetry is deliberate; it isn't stated in the failure-policy table at `:365-372`.
  - No directory fsync after any rename, and the active file is never fsync'd — `file_out` offers no durability
    guarantee at all (the ADR doesn't claim one, but a reader may assume rotation is crash-atomic).
  - `promote_staged`'s `staging.exists()` / `oldest.exists()` / `from.exists()` are TOCTOU-shaped; single-writer
    makes it fine in practice.
- **Existing coverage:** `file.rs` tests `:626-1002` — notably
  `a_failed_active_file_rename_leaves_every_retained_file_completely_untouched` (`:791`),
  `repeated_failed_rotations_never_delete_retained_history` (`:836`),
  `a_stale_staging_file_from_a_killed_process_is_promoted_on_the_next_rotation` (`:870`),
  `a_failed_reopen_after_a_committed_rename_never_writes_into_the_rotated_file` (`:906`),
  `a_failed_reopen_is_classified_clean_so_the_batch_is_retried_rather_than_dropped` (`:938`),
  `max_files_three_keeps_exactly_the_active_file_and_two_rotated_ones` (`:719`),
  `max_files_one_truncates_in_place_rather_than_ever_creating_a_dot_1` (`:744`). ADR:
  `docs/adr/rotating-file-output.md`; gaps: [`docs/known-gaps.md`](../known-gaps.md#file-stdio-and-influxdb-sinks).
- **Suggested verification approach:** kill -9 injection at each of {rename to staging, each cascade rename, the
  oldest unlink, the re-open} followed by a restart, asserting no retained file is lost or duplicated; a bound on
  `max_files` (graph rule) if the syscall-storm concern is confirmed.
- **Priority:** P1 — custom retention logic with real delete/rename ordering, but the commit-point-first redesign
  is already well covered by tests.

---

### DISK-11 — `RotationState` — rotation-trigger bookkeeping and open-time seeding
- **Location:** `crates/logit-outputs/src/file.rs:60-170` (`unix_seconds`, `now_unix`, `RotationState` incl.
  `seed_period`, `should_rotate`, `note_written`, `period_for`), `:217-236` (`FileTarget::open`'s seeding)
- **What it does:** Tracks bytes written into the active file (seeded from its length at open) and which
  hourly/daily UTC bucket it was last written in (seeded from its mtime at open, only when `written > 0`).
  `should_rotate` fires when the calendar period changed or when this write would cross `max_bytes` — and never on
  an empty file, so an oversized batch lands whole.
- **Why sensitive:** data-loss adjacency — a spurious rotation under `max_files: 1` truncates real data; bookkeeping
  — `written` drifting from the real file length means `max_bytes` silently stops bounding; hot-path — evaluated
  once per batch in `StreamOutput::send`.
- **Invariants to verify:**
  - `written` is seeded from the file's length at open and incremented by exactly the bytes `send` wrote
    (`stdio.rs:747` calls `note_written(now, bytes.len())` **before** the write at `:766-769` — verify a failed
    write doesn't leave `written` over-counted).
  - `period` stays `None` for a freshly created or empty file, so the first batch never rotates.
  - `period_for` uses `div_euclid` so negative (pre-epoch) timestamps bucket correctly; `now_unix` falls back to 0
    on a pre-epoch clock (`:73`).
  - `reset()` after a rotation clears both fields, and a failed rotation leaves them in a state that doesn't
    re-trigger every write.
- **Observed concerns (unverified):**
  - `note_written` is called before the write succeeds (`stdio.rs:747` vs `:766`); on a write error `send`
    propagates `Err`, so the count is over-stated for a file that didn't receive the bytes. Low impact (the error
    is fatal-ish), high confidence.
  - A backwards wall-clock jump across a period boundary causes one spurious rotation (`should_rotate` compares
    buckets for inequality, not ordering, `:138`). Low confidence anyone cares; worth a line in the ADR.
- **Existing coverage:** the pure unit tests at `file.rs:534-620` (nine of them, no files or clocks involved) plus
  the restart-seeding tests at `:687-716`.
- **Suggested verification approach:** targeted review; a proptest over (open length, mtime, sequence of writes,
  clock) asserting `written` equals the real file length after every operation.
- **Priority:** P2 — pure, well-tested, and a mistake here costs an early/late rotation rather than corruption.

---

### DISK-12 — `StreamOutput::send` — encode/rotate/write/flush ordering and error posture
- **Location:** `crates/logit-outputs/src/stdio.rs:713-787` (`Output::send`, `Output::flush`), `:609-620`
  (`Target`), `:144-167` (`StreamEncoder`)
- **What it does:** Short-circuits empty batches **before** encoding (so `format: native` doesn't write a header-only
  frame), encodes, decides rotation *before* the write so a batch is never split across a rotation boundary, counts
  `logit.output.file.rotations` only on `RotateOutcome::Rotated`, then does one `write_all` + one `flush` per batch.
  Any write error propagates as a fatal `anyhow::Error` with no retry inside the sink.
- **Why sensitive:** hot-path — per batch; data-loss — a batch torn across a rotation would produce an unparseable
  native file; durability — there is a `flush` but no `fsync`, so a machine crash loses the page cache; accounting
  — the rotations counter must not count a `NotRotated`; cancellation — `send` is raced against the shutdown grace
  in `write_loop`, so `write_all` can be dropped mid-write (blocking I/O behind `tokio::fs`).
- **Invariants to verify:**
  - Rotation is always decided before the write; a batch is written whole into exactly one file.
  - Under `format: native`, each file (rotated and active) contains only whole, independently decodable frames
    *except* possibly a torn tail from a crash — and nothing reads these files back today, so the torn tail is
    unrecoverable by any shipped tool.
  - `bytes.len()` is counted in `logit.output.batch.bytes` exactly once even when the rotation path errors.
  - A cancelled `send` (shutdown grace) can leave a partial `write_all` on disk; verify the batch is then
    replayed/redelivered or counted, not silently both.
  - `Output::flush` at shutdown reaches the same handle `send` used, including after a failed re-open left
    `FileTarget::file == None` (flush is then a documented no-op, `file.rs:264-269`).
- **Observed concerns (unverified):**
  - `send` returns `Err` from `file.rotate(...)?` at `:735` — a fatal-classified rotate flush failure aborts the
    batch *after* nothing has been written, which is correct, but the `note_written` at `:747` is skipped in that
    path while the earlier `should_rotate` already fired; verify there's no state where rotation is attempted every
    batch forever (see the `max_files == 1` note in the previous entry). Medium confidence.
  - No `fsync` anywhere in the file sink; `flush()` on `tokio::fs::File` only reaches the page cache. Documented
    implicitly, never stated. High confidence, low surprise.
- **Existing coverage:** `stdio.rs` tests `rotating_via_stream_output_rotates_and_counts_the_rotation` (`:1488`),
  `a_rotation_that_could_not_rename_the_active_file_is_never_counted_as_a_rotation` (`:1543`),
  `send_on_an_empty_batch_writes_nothing_under_native_format_either` (`:1628`),
  `rotating_under_native_format_leaves_both_files_independently_decodable` (`:1650`),
  `send_appends_across_multiple_batches_rather_than_truncating` (`:1370`),
  `send_records_batch_bytes_matching_the_actual_encoded_length` (`:1423`). ADRs:
  `docs/adr/rotating-file-output.md`, `docs/adr/file-output-native-format.md`.
- **Priority:** P2 — the ordering is simple and well tested; the residual risk (no fsync, torn native tail with no
  reader) is documented-by-omission rather than a bug.
- **Suggested verification approach:** targeted review; if `format: native` ever gains a reader, add a
  torn-tail-resync test mirroring `DiskQueue::open`'s.

---

### DISK-13 — `logit_proto::frame` as the disk record envelope — sanity caps, CRC, lz4, `resync`
- **Location:** `crates/logit-proto/src/frame.rs:24-62` (`MAX_SANE_UNCOMPRESSED_LEN`, `MAX_SANE_COMPRESSED_LEN`,
  `HEADER_LEN`), `:131-162` (`FrameHeader::read`), `:221-271` (`read_frame`/`read_frame_with_header`), `:276-299`
  (`lz4_compress`/`lz4_decompress`), `:301-310` (`resync`)
- **What it does:** 24-byte header (magic/version/flags/codec/compression/reserved/uncompressed_len/
  compressed_len/crc32c), CRC-32C over the *compressed* bytes (checked before decompression), lz4 via `lz4_flex`'s
  `compress_into`/`decompress_into` with an exact-size output buffer truncated to what was actually written, and a
  linear `MAGIC` scan for resync. *Another surveyor covers the native codec itself; this entry is only the disk
  consumption.*
- **Why sensitive:** untrusted-input — every length field is attacker-/corruption-controlled and sizes an
  allocation; data-loss — the `Truncated` vs `Malformed` distinction decides whether `DiskQueue::open` truncates
  the segment or resyncs past the bad record (getting it wrong silently discards everything after);
  nontrivial-3p-use(lz4_flex) — the `decompress_into` + truncate + length-recheck dance exists specifically because
  the crate can write fewer bytes than the buffer holds; nontrivial-3p-use(crc32c) — CRC over compressed bytes, by
  design.
- **Invariants to verify:**
  - `Truncated` is produced **only** for a genuine short buffer; every corrupt-length case is `Malformed`. Both
    `uncompressed_len` (`:230`) and `compressed_len` (`:236`) are capped before use; `MAX_SANE_COMPRESSED_LEN`
    (`:54-55`) is wide enough that `write_frame` can never emit a frame its own `read_frame` rejects.
  - CRC is verified *before* `lz4_flex` sees the bytes (`:246` precedes `:252`).
  - The post-decompress length check (`:263-269`) is not a tautology — depends on `lz4_decompress`'s
    `out.truncate(written)` at `:297`.
  - `resync` is a plain `windows(4).position(…)` — O(n) per call, and `DiskQueue`'s callers must tolerate a
    spurious hit inside a `trace_id` (tested).
  - `HEADER_LEN` and the field offsets used by disk_queue's tests (`CONTEXT_LEN + 16` for `compressed_len`,
    disk_queue `:1908`) stay in sync with `FrameHeader::write` (`:103-113`).
- **Observed concerns (unverified):** none spotted in the disk-facing behavior. `resync`'s linear scan is the
  performance term in `walk_segment`'s worst case (see the parse entry), not a correctness issue.
- **Existing coverage:** `frame.rs:316-514` — 16 unit tests, including
  `a_header_truncated_by_one_byte_is_truncated_not_malformed` (`:393`),
  `a_body_truncated_by_one_byte_is_truncated_not_malformed` (`:400`),
  `rejects_a_frame_that_decompresses_shorter_than_its_header_declares` (`:456`),
  `rejects_an_uncompressed_len_over_the_sanity_cap` (`:467`),
  `rejects_a_compressed_len_over_the_sanity_cap` (`:502`, the F5 fix with an explicit comment naming
  `DiskQueue::read_record_at`'s `walk_segment` as the victim), `rejects_corrupt_crc` (`:381`),
  `resync_finds_the_next_frame_start_after_garbage` (`:434`). ADRs:
  `docs/adr/native-wire-format-encoding.md`, `docs/design/wire-protocol.md`.
- **Suggested verification approach:** a `cargo-fuzz` target over `read_frame` (no fuzz targets exist in this repo
  today) asserting no panic and no allocation over the caps; a property test that `write_frame ∘ read_frame` is
  total for every payload up to the cap under both compressions.
- **Priority:** P1 — the caps and CRC are correct and tested, but this is the one decoder standing between corrupt
  disk bytes and an allocation, and the `Truncated`/`Malformed` distinction is load-bearing for disk recovery.

---

### DISK-14 — Config → spool wiring: path resolution, graph rule 35, and the exclusive lock
- **Location:** `crates/logit-cli/src/pipeline.rs:1001-1024` (`queue_config`),
  `crates/logit-pipeline/src/graph.rs:1654-1711` (rule 35), `crates/logit-pipeline/src/disk_queue.rs:388-405`
  (the `lock` file), `crates/logit-pipeline/src/queue.rs:703-742` (`SinkStore`/`SinkStoreConfig`/`open`)
- **What it does:** `disk.path` is resolved against the config file's `base_dir`; rule 35 rejects
  `max_batches`/`max_bytes` set alongside `disk:`, rejects zero/inverted `segment_bytes`/`max_bytes`, and rejects
  two components declaring the same *literal* `disk.path`. An aliased path (`./spool` vs `spool`) is caught at
  runtime by an exclusive `flock` on `<dir>/lock` held for the queue's lifetime.
- **Why sensitive:** data-loss/corruption — two sinks sharing a spool interleave records and corrupt each other's
  cursors; the lock is the only runtime defense; this is otherwise plain config plumbing.
- **Invariants to verify:**
  - `std::fs::File::try_lock` maps to `flock(LOCK_EX|LOCK_NB)` (per-open-file-description), so two opens **within
    the same process** also conflict — that's what makes the aliased-path case fail rather than corrupt.
  - The lock file handle (`_lock`) outlives every segment/cursor operation and is released on `SIGKILL` without
    leaving a stale marker (no lock file content is ever read).
  - Rule 35's literal-string comparison plus the lock together cover every aliasing case a config can express
    (symlinks, bind mounts, `..` segments — the lock is the backstop).
  - `base_dir.join(&disk.path)` handles an absolute `disk.path` correctly (`join` replaces).
- **Observed concerns (unverified):** none spotted. Worth noting only that a *third-party* process holding the
  directory (e.g. a stale container) produces a startup failure with a message that blames another `logit`
  component (`:397-401`).
- **Existing coverage:** `graph.rs` rule-35 tests (search `buffer.disk`), `pipeline.rs:3092`
  (`build_spec_wires_a_disk_buffer_into_a_sinkstoreconfig_disk_with_the_path_resolved_against_base_dir`),
  `crates/logit-perf/src/spool.rs`'s own containment tests. ADR: `docs/adr/disk-backed-sink-buffer.md`
  ("File layout", "Corrections to the sketch" #3).
- **Suggested verification approach:** targeted review + one integration test opening the same spool twice
  (aliased path) and asserting the second fails at `open`, not later.
- **Priority:** P2 — thin, and the failure mode is a loud startup error.

---

### DISK — Cross-cutting notes

**Shared helpers other areas depend on:**
- `logit_proto::frame` (`write_frame`/`read_frame`/`FrameHeader::read`/`resync`/`MAX_SANE_*`) is shared verbatim by
  the disk spool, `file_out`'s `format: native`, and `logit_in`/`logit_out`'s socket transport. The
  `Truncated` vs `Malformed` classification and `MAX_SANE_COMPRESSED_LEN` exist *because of* the disk walker
  (frame.rs:494-514 says so) — a change made for the network surveyor's benefit can silently break disk recovery.
- `logit_proto::native::{encode_batch_v2, decode_batch, decode_batch_v2, CODEC_NATIVE_V1/V2}` is the payload codec
  for spooled records; the codec byte is the *only* versioning a disk record has (`CONTEXT_LEN` is fixed forever).
- `Diagnostics::warn_throttled` keys used on these paths: `cursor_error`, `disk_io_error` (disk_queue),
  `rotate_failure`, `retention_failure` (file.rs). `SINK_QUEUE_METRICS` (`queue.rs`) is shared between the
  in-memory and disk stores, so `buffer.depth`/`bytes`/`utilization` mean different things per variant (disk's
  `bytes` is on-disk segment bytes including already-consumed-but-undeleted ones).
- The tmp+rename checkpoint idiom is duplicated, not shared, between `disk_queue::persist_cursor` and
  `crates/logit-inputs/src/tail/checkpoint.rs` — the tail surveyor should compare the two (same fsync gap?).

**Things I noticed outside my area worth handing on:**
- `crates/logit-pipeline/src/runtime.rs`'s `write_loop` commits a batch off the queue on `Delivery::Dropped`
  (`:1167`) — relevant to whoever covers the sink/retry path, and it interacts with durability as flagged above.
- `crates/logit-pipeline/src/queue.rs`'s `BoundedQueue`/`InMemoryBuffer` (`logit-proto/src/buffer.rs:38-130`) is
  the memory twin of the spool with the same peek/commit reservation contract; the reserved-head semantics differ
  deliberately between the two (`InMemoryBuffer` can evict behind a reserved head, `DiskQueue` cannot) — a
  queue-area surveyor should pin that difference.
- `crates/logit-perf/src/spool.rs` deletes directories (`clear`), guarded by a lexical containment check against
  `<root>/perf/results/` — dev-only, but it is the one place in the repo that `remove_dir_all`s a config-derived
  path.
- `logit-config` puts no upper bound on `rotate.max_files` (only graph rule `!= 0`), which `promote_staged`
  iterates over — a config-surface surveyor may want to add a ceiling.
- No `cargo-fuzz` targets exist anywhere in the workspace; the two decoders most deserving one (frame + native)
  both sit on the disk recovery path.


---

## RT — Pipeline node runtime

Scope: `crates/logit-pipeline/src/{runtime,fanout,queue,accumulator,router,transform,output,input,readiness}.rs`,
`graph.rs` only where it has runtime-correctness consequences, and `crates/logit-cli/src/pipeline.rs`.
`disk_queue.rs` and `sockstat.rs` are other surveyors' — only their seams are noted.

**Test/production boundary in `runtime.rs`:** production code is **lines 1–2280**; `#[cfg(test)] mod tests`
runs **2281–8322** (~72% of the file is tests). Every line reference below was read in the source.

**Third-party crates in play (from `crates/logit-pipeline/Cargo.toml`):** `tokio` (mpsc / watch / oneshot /
`Notify` / `JoinSet` / `time`), `anyhow` (used *non-trivially*: `Fault` is smuggled as `anyhow` context and
read back with `anyhow::Error::downcast_ref`, not the `std::error::Error` method), `async-trait`, `tracing`,
`serde`/`serde_json`/`bytes` (disk queue only), `libc` (Linux-only, `sockstat` only — out of scope).
Dev-only: `tokio` `test-util` for paused-clock tests.

**Correction to the brief:** there is **no `unsafe` anywhere in `crates/logit-cli/src/`** (verified by grep over
the whole crate). The only `unsafe` in `logit-cli` is in a *test*: `crates/logit-cli/tests/admin_ready.rs:175`
(`libc::kill(pid, SIGTERM)`), which is test code and out of scope. The only `unsafe` in `logit-pipeline` is in
`sockstat.rs:146,216,221` (another surveyor's area).

---

### RT-01 — Startup orchestration: bind pre-pass, channel/Fanout construction, spawn loop, scaffolding drop
- **Location:** `crates/logit-pipeline/src/runtime.rs:174-536` (`run_with_telemetry`); bind pre-pass `234-253`;
  inbox/sender construction `255-271`; target-`Fanout` pre-pass `273-298`; spawn loop `312-445`;
  `drop(senders)` / `drop(target_fanouts)` `447-463`; `resolve_target_fanouts` `1578-1603`.
  Also `crates/logit-pipeline/src/input.rs:32-34` and `output.rs:53-55` (`bind` contracts).
- **What it does:** Sorts every component id, seeds `Readiness`, spawns the shutdown driver task, binds every
  `Input` **and** `Output` sequentially before any task exists, creates one bounded `mpsc` inbox per non-target
  component, builds each `target`'s single `Fanout` before the spawn loop (because a router can sort before its
  targets), then spawns one tokio task per node (or an OS thread + a `watch_lua_thread` stand-in task for Lua).
  Finally it drops the construction-only `senders` and `target_fanouts` maps.
- **Why sensitive:** concurrency (spawn/abort ordering, a `JoinSet` plus raw threads); data-loss and **hang**
  (the two `drop`s at `447-463` are load-bearing: a surviving `Sender` clone means no inbox ever closes, so the
  shutdown cascade never fires and `run` hangs forever); accounting (`Readiness` transitions); custom (the
  target-alias `Fanout` scheme is bespoke).
- **Invariants to verify:**
  - `drop(senders)` and `drop(target_fanouts)` happen on **every** path that reaches the join loop; nothing else
    retains a `Sender` clone or a target `Fanout` clone beyond its owning node.
  - A `target` gets no channel (`265-267`), no spec removal, no task, and exactly one `Fanout` reachable only
    through its routers.
  - `senders[c]` (`291`, `323`) can never index-panic: every consumer id must have a channel, i.e. rule 49
    ("nothing may name a `target` as a source") must actually hold for **every** kind.
  - `resolve_target_fanouts`'s `panic!` (`1596`) is unreachable given rules 48/49.
  - Bind order is the sorted id order and the *first* failure by that order is the one reported.
  - `readiness.begin` is provably the first write on the signal (the comment at `181-193` argues this against an
    already-resolved `shutdown`).
  - Slot order is derived in exactly one place (`graph::targets_of`) and agrees between `Router` slots and the
    Lua name→slot table.
- **Observed concerns (unverified):**
  - **Early-return teardown is abrupt and unjoined, unlike the graceful path.** A Lua script load failure
    (`432-441`), a thread-spawn failure (`419-420`) or a missing spec (`327-330`) returns `RunError::Startup`
    while earlier nodes are already live. Dropping `tasks` aborts the tokio tasks mid-flight (no drain, no
    `finish_and_flush`, no flush of an already-buffered sink), and **already-started Lua OS threads are not in
    the `JoinSet` at all** — they are detached and merely happen to exit once the locals drop their senders.
    `readiness.failed()` is also never called on this path, so `/readyz` reports `starting`, not `degraded`.
    Medium confidence this is a real, if narrow, gap; it is startup-only.
  - `NodeSpec::Target` registered for a *non*-target kind is silently a no-op (`381-388`, deliberate per the
    comment) — a registry bug there produces a node that exists in the graph but never runs. Low severity, but
    nothing detects it.
- **Existing coverage:** `runtime.rs` tests `a_failing_bind_returns_startup_and_spawns_nothing` (5903),
  `a_failing_output_bind_returns_startup_and_spawns_nothing` (5966),
  `the_first_failing_bind_by_sorted_id_is_the_one_reported` (6012),
  `a_router_exiting_closes_its_targets_consumers_inboxes` (7365, under a `tokio::time::timeout` specifically to
  catch the hang), `two_routers_directing_at_one_target_both_deliver` (7443),
  `phase_reaches_ready_then_draining_on_a_normal_run` (6043). Governed by
  `docs/adr/target-components.md`, `docs/adr/admin-readiness-endpoint.md`, `docs/design/pipeline-graph.md`.
- **Suggested verification approach:** targeted code review of every early-`return` path in `run_with_telemetry`
  for scaffolding-drop and `Readiness` consistency; a test that fails a Lua script load *after* a sink has
  buffered batches and asserts what happens to them; a "no `Sender` outlives construction" audit.
- **Priority:** **P0** — a retained `Sender` is a permanent hang of the whole process on the main data path, and
  the logic is entirely custom.

---

### RT-02 — Shutdown signalling, grace anchoring, and the join loop's first-error cascade
- **Location:** `runtime.rs:198-232` (watch channel, shutdown driver, `drain_started`,
  `shutdown_dropped_batches`), `471-535` (join loop, `shutdown_driver.abort()`, `drain complete`),
  `961-982` (`shutdown_grace_expired`), `538-571` (`run_input`), `input.rs:38-60`
  (`Input::run_until_shutdown` default), `input.rs:63-80` (`InputRuntimeConfig`, grace defaults to
  `Duration::ZERO`).
- **What it does:** A `watch<bool>` carries shutdown to every node. The driver task flips
  `Readiness::draining()` *before* `send(true)`, and the join loop's first error flips the same signal so a
  failure drains the rest gracefully instead of aborting siblings. `shutdown_grace_expired` is a reusable future
  that resolves `grace` after the *first* time shutdown fired, with the deadline stored in a caller-owned
  `&mut Option<Instant>` so it survives the future being dropped by a losing `select!` arm. `run_input` races the
  listener against that backstop rather than against `shutdown` itself.
- **Why sensitive:** cancellation (the whole design hinges on a dropped `select!` arm not losing the grace
  anchor); concurrency; data-loss (cancel-by-drop of a listener mid-decode); accounting (`drain complete`).
- **Invariants to verify:**
  - `*deadline` is set synchronously at `979` before any further `.await`, so the anchor survives cancellation of
    *that specific* `shutdown_grace_expired` call; and a second call never re-anchors.
  - `shutdown.wait_for(|&due| due)` being dropped mid-poll leaves the `watch::Receiver` in a state where the next
    call still observes `true` (tokio's `wait_for` marks-seen semantics).
  - Only the *first* error becomes `result`; later errors are observed and discarded but still mark
    `NodeState::Failed` and `Readiness::failed()`.
  - The `while let Some(joined) = tasks.join_next_with_id()` loop runs to exhaustion — never `break`s — so no task
    is aborted by an early `JoinSet` drop.
  - `shutdown_driver.abort()` at `516` can never race a still-needed `send(true)`.
- **Observed concerns (unverified):**
  - **`drain complete` under-reports dropped batches.** `shutdown_dropped_batches` is incremented *only* by
    `run_output`'s abandoned-**inbox** sweep for a `Memory` store (`722-723`). Batches dropped from the sink's
    **queue** by `finish_and_flush` → `SinkStore::finish` (`runtime.rs:1014-1035`, `queue.rs:788-804`) are counted
    in telemetry but never reach this atomic, because `finish_and_flush` has no handle to it. So a shutdown that
    dropped a thousand queued batches still logs the clean `info!("drain complete")` branch (`531-533`). High
    confidence this is a real accounting hole; low blast radius (a log line), but it is exactly the line an
    operator would trust.
  - `InputRuntimeConfig::shutdown_grace` defaults to `ZERO`, so `run_input`'s two `select!` arms become ready
    simultaneously and tokio picks pseudo-randomly. Both arms map to `Ok(())`/the input's own result here, so it
    looks benign — but an input that returns `Err` at exactly the same instant can have that error swallowed by
    the grace arm. Low confidence this is reachable in practice; worth one look.
  - **Context (documented, not a surprise):** `docs/known-gaps.md` records that a datagram in flight at signal
    time is lost uncounted, that `otlp_in` can hold the graph open past shutdown (connection-spawned `Fanout`
    clones), and that `logit_in`/`internal` grace is fixed at 5s.
- **Existing coverage:** `run_with_shutdown_flushes_an_in_flight_window_before_exiting` (2817),
  `a_non_overriding_input_returns_at_the_instant_shutdown_fires_not_after_the_grace` (2985),
  `an_overriding_input_draining_within_its_grace_completes_and_delivers` (3017),
  `an_overriding_input_that_never_finishes_is_cancelled_at_exactly_the_grace_deadline` (3049),
  `a_healthy_sinks_buffered_batches_are_still_delivered_after_a_sibling_sink_trips_the_permanent_failure_window`
  (5487), `crates/logit-cli/tests/admin_ready.rs` (real SIGTERM drain). Governed by
  `docs/adr/service-lifecycle-and-output-retry.md`, `docs/adr/decoupled-listener-io.md`.
- **Suggested verification approach:** cancellation-safety audit of every `select!` in `runtime.rs` (there are
  four: `566`, `657`, `1101`, `1143`); `tokio::time::pause` tests pinning the grace anchor across a *losing*
  race; a shutdown-under-load test asserting the `drain complete` counter equals the sum of every
  `batches.dropped{reason="shutdown"}`.
- **Priority:** **P0** — this is the drain-ordering machinery for the whole process; wrong here means silent
  loss or a hang at every shutdown.

---

### RT-03 — `run_output`'s drain/write join, the abandoned-inbox sweep, and `finish_and_flush` ordering
- **Location:** `runtime.rs:573-744` (`run_output`), `select!` at `657-667`, `drop(drain)` `673`,
  `store.close()` `690`, abandoned-inbox sweep `692-739`, `finish_and_flush` call `741`;
  `finish_and_flush` itself `984-1040`; `drain_inbox` `746-781`; `unwrap_batch_arc` `783-796`;
  `SinkStore` seam `queue.rs:703-805`.
- **What it does:** Splits a sink node into `drain_inbox` (inbox → store) and `write_loop` (store → wire), joined
  by one `select!`. `inbox` is owned by `run_output` and only *borrowed* by `drain_inbox`, specifically so
  cancelling `drain_inbox` leaves un-`recv`ed batches in the channel buffer for the sweep. After the writer
  finishes, it closes the store, sweeps the inbox (appending to a `Disk` spool, counting-and-dropping for
  `Memory`), then calls `finish_and_flush` exactly once. `finish_and_flush` must not run inside `write_loop` —
  the doc comment at `988-996` records the concrete bug that caused (a producer slipping a batch in between
  "queue observed empty" and `flush()`).
- **Why sensitive:** data-loss (this is the last accounting point before batches vanish); cancellation (a
  `Pin<Box<dyn Future>>` polled by `&mut` in one arm and consumed by `.await` in the other); concurrency;
  accounting (`batches.dropped{reason="shutdown"}`); backpressure (`store.close()` at `690` is what stops the
  sweep's `push` from blocking forever under `overflow: block`).
- **Invariants to verify:**
  - Exactly one of the two `already_finished` arms runs, and `write`'s mutable borrow of `output` is released
    before `finish_and_flush` reclaims it (`661-667`).
  - `store.close()` precedes every sweep `push` (`690` before `711`), and `DiskQueue::push` really does
    short-circuit its overflow policy once closed.
  - `Output::flush` is called **exactly once** on every exit path — happy, fatal, and shutdown-grace.
  - Nothing can push into `store` after `drop(drain)` at `673`.
  - `Disk` sweep appends rather than drops; `Memory` sweep counts both batches and events.
  - `SinkStore::finish` for `Memory` drains via `commit()` (which clears any stale `peek` reservation).
- **Observed concerns (unverified):**
  - **One batch can be lost *uncounted* on the cancellation path.** `drain_inbox` (`769-779`) takes a
    `Delivered` off the inbox and *then* awaits `store.push(...)`. If that `push` is parked on `not_full`
    (`overflow: block`, full queue) when `run_output`'s `select!` resolves via `write`, dropping the `drain`
    future at `673` destroys the in-hand batch. The sweep at `705` only sees what is still in the *channel*, so
    that batch is never counted in `batches.dropped{reason="shutdown"}` and never reaches a `Disk` spool. This is
    the exact loss `queue.rs:368-381` documents for `push_many` on the receive side, but it is **not** documented
    here or in `docs/known-gaps.md`. Medium-high confidence; bounded at one batch per sink.
  - **Sweep-dropped batches were never counted as `received`.** `drain_inbox` is the only place
    `logit.component.batches.received`/`events.received` is incremented (`776-777`); the sweep (`705-713`) skips
    it while still counting `dropped`. So a sink can report `dropped > received`. Deliberate or not, it breaks
    the naive reconciliation `received = delivered + dropped`. Medium confidence it's an oversight.
  - The `write`-finishes-first arm can leave a `peek` reservation standing (see the `write_loop` entry); it is
    cleared later by `SinkStore::finish`'s `commit()` loop, so benign *today* — but only because of that.
- **Existing coverage:** `a_slow_sinks_send_in_flight_does_not_stop_its_inbox_from_draining_into_the_queue`
  (4117), `inbox_close_drains_the_queues_tail_before_run_output_resolves` (4375),
  `shutdown_grace_expiry_ends_write_loop_promptly_leaving_the_remainder_for_run_output` (4997),
  `run_output_flushes_exactly_once_and_never_loses_a_batch_racing_shutdown_grace` (5065),
  `a_disk_backed_sinks_shutdown_sweep_does_not_hang_pushing_into_a_full_spool` (5287);
  `crates/logit-bench/tests/allocations.rs:1577` (`drain_inbox_single_consumer_owned_batch_costs_exactly_the_arc`);
  `crates/logit-cli/tests/durable_buffer_restart.rs`. Governed by `docs/adr/buffered-sink-delivery.md`,
  `docs/adr/disk-backed-sink-buffer.md`.
- **Suggested verification approach:** a paused-time test that wedges `store.push` under `Block` with a full
  queue and then trips shutdown grace, asserting the in-hand batch is either delivered, spooled, or counted;
  a telemetry-reconciliation test (`received == delivered + all dropped reasons`) driven through a full
  `run_output`.
- **Priority:** **P0** — silent, uncounted loss of a batch on the main data path in custom code.

---

### RT-04 — `write_loop`: peek/commit delivery, permanent-failure window, degraded/recovered edges
- **Location:** `runtime.rs:1042-1218` (`write_loop`), `select!`s at `1101-1109` and `1143-1150`,
  sink span `1116-1130`, `observe_batch` `1137`, outcome handling `1156-1215`;
  `PERMANENT_FAILURE_WINDOW` `37`; `Delivery` enum `858-872`; `output.rs:91-191`
  (`Fault`/`classify`/`is_explicitly_permanent`/`is_retryable`).
- **What it does:** Pulls the head via `store.peek()`, mints a child span parented on the batch's own
  `BatchContext`, hands the context to `Output::observe_batch`, delivers via `deliver_with_retry`, then commits —
  **on success and on failure alike**. A failure counts `batches.dropped{reason="send_failed"}`, marks the span,
  and emits `degraded` once (then `recovered` on the next success). Only a run of *explicitly* classified
  `Fault::Permanent` outcomes with no success in between, sustained for 60s, ends the loop with `Err`.
- **Why sensitive:** data-loss (commit-on-failure is a deliberate drop); duplication (`AtLeastOnce` retry of an
  `Ambiguous` fault); cancellation (an in-flight `Output::send` is dropped when the grace arm wins);
  accounting; `nontrivial-3p-use(anyhow)` — `Fault` travels as `anyhow` *context* and is read back with
  `anyhow::Error::downcast_ref`, explicitly **not** the `std::error::Error` trait method, which would silently
  never match (`output.rs:118-133`).
- **Invariants to verify:**
  - Every path through the loop either commits the head or returns; the head is never left permanently reserved
    while the loop continues.
  - `is_explicitly_permanent` is strictly narrower than `classify(..) == Permanent`; an unclassified error resets
    the streak (`1205-1213`).
  - `permanent_streak_since` is set on the *first* explicit-permanent failure and cleared by any success or any
    non-explicit outcome; the 60s window is measured from that first failure.
  - `degraded`/`recovered` fire exactly once per edge, not per batch.
  - `shutdown_grace_expired` returns `Ok(())`, never `Err` — an incomplete shutdown drain is not a pipeline
    failure (`1113`, `1153`).
  - The sink span's parent is `ctx.trace.span_id` (the batch's own), and `ctx.trace.child()` is used only as this
    span's identity, never propagated.
- **Observed concerns (unverified):**
  - **A `peek` reservation can be left standing when the grace arm wins the first `select!`.**
    `BoundedQueue::peek` (`queue.rs:672-686`) calls `InMemoryBuffer::peek`, which sets `head_reserved = true`
    (`crates/logit-proto/src/buffer.rs:145-150`). In `tokio::select!`, a branch whose future completed can still
    lose the race and have its *result* discarded — so `peek` may reserve and then `write_loop` returns
    `Ok(())` at `1113`. Today nothing breaks: `SinkStore::finish` drains with `commit()`, which clears it. But the
    invariant is accidental, not argued anywhere, and `queue.rs:488-496` explicitly names a dangling reservation
    as the hazard `pop` exists to avoid. Medium-high confidence the window is real; low severity today.
  - **Cancelling `deliver_with_retry` mid-`send` at grace expiry is an ambiguous outcome silently recorded as a
    clean shutdown drop.** The batch stays uncommitted, so `finish_and_flush` counts it
    `dropped{reason="shutdown"}` — but the destination may have received it. No `Fault::Ambiguous` is recorded.
    Medium confidence this is worth naming in `known-gaps`.
  - The span recorded when `ShutdownExpired` returns at `1153` drains as a *successful* zero-outcome `deliver`
    span (no `.error()`), unlike the mixed-error case `run_lua_loop` explicitly handles at `2086`. Low confidence
    this matters.
- **Existing coverage:** a dense block of `write_loop` tests at `runtime.rs:4524-5290` —
  `an_ambiguous_fault_is_dropped_immediately_under_at_most_once_with_no_retry` (4587),
  `a_permanent_fault_is_never_retried_under_{at_most,at_least}_once` (4621/4626),
  `budget_exhaustion_on_a_retryable_fault_drops_the_batch_and_write_loop_continues` (4665),
  `sustained_permanent_failures_end_write_loop_once_the_failure_window_elapses` (4693),
  `a_success_inside_the_window_resets_the_permanent_failure_streak` (4774),
  `an_unclassified_error_never_trips_the_permanent_failure_window` (4851),
  `a_budget_exhausted_ambiguous_drop_resets_the_permanent_failure_streak_like_success_does` (4927),
  `a_permanently_failing_sink_with_a_live_input_does_not_end_run` (4304),
  `write_loop_records_a_sink_span_parented_on_the_incoming_batchs_context` (6824);
  `output.rs:193-270` pins the whole `is_retryable` table exhaustively. Governed by
  `docs/adr/buffered-sink-delivery.md`, `docs/adr/service-lifecycle-and-output-retry.md`,
  `docs/adr/internal-span-emission-and-deterministic-sampling.md`.
- **Suggested verification approach:** cancellation-safety audit of both `select!`s with attention to the
  completed-but-discarded branch; a paused-time test that trips grace exactly while `peek` is ready and asserts
  the reservation state; review of whether a grace-cancelled `send` should be recorded as `Ambiguous`.
- **Priority:** **P0** — every batch a sink ever emits goes through here, the drop decision is made here, and
  the classification plumbing is custom.

---

### RT-05 — `deliver_with_retry` and `backoff_for`: budget enforcement and doubling schedule
- **Location:** `runtime.rs:874-928` (`deliver_with_retry`), `930-947` (`backoff_for`), `798-828`
  (`RetryConfig`), `830-856` (`WriteLoopConfig`); config mapping `crates/logit-cli/src/pipeline.rs:1025-1038`
  (`write_config` — note `base_delay` is *not* operator-exposed and always takes the default).
- **What it does:** Races **every** attempt, including the first, against the remaining budget via
  `tokio::time::timeout`, because a sink's own internal timeout can exceed the configured budget. A timeout is
  classified `Fault::Ambiguous`, never `Permanent`. Backoff doubles with `saturating_mul`, bounded at 128
  iterations, then clamped to `max_delay` and to whatever is left of the budget.
- **Why sensitive:** hot-path (per batch on every sink); duplication (retrying an `Ambiguous` fault under
  `AtLeastOnce`); custom (hand-rolled budget/backoff rather than a retry crate); accounting
  (`send.duration`/`errors`/`retries`).
- **Invariants to verify:**
  - `total_budget` is a genuine ceiling across every attempt *and* every sleep — no path can exceed it.
  - `remaining` can reach zero: `tokio::time::timeout(Duration::ZERO, ..)` must not busy-loop or livelock.
  - `backoff_for` is correct for *any* `(base_delay, max_delay)` pair, including `base_delay > max_delay` and
    `base_delay == 0`; `saturating_mul` never wraps.
  - `attempt` (a `u32`) increments once per attempt and cannot overflow inside one budget.
  - A timeout is only ever `Ambiguous`, never `Clean` or `Permanent`.
  - `telemetry.timer("logit.component.send.duration")` is dropped around exactly the `send` call, timeout
    included.
- **Observed concerns (unverified):** none spotted. `base_delay: 0` would make `backoff_for` return `0` forever
  and spin the retry loop at full speed until the budget expires — but `base_delay` is not config-exposed
  (`pipeline.rs:1032`), so it is unreachable today. Worth a `debug_assert` if it ever is exposed. Low confidence
  it matters now.
- **Existing coverage:** `backoff_between_retry_attempts_follows_the_configured_doubling_schedule`
  (`runtime.rs:4634`), plus the budget/fault tests listed in the `write_loop` entry;
  `fast_retry_config` helper at `4524`. Governed by `docs/adr/buffered-sink-delivery.md`.
- **Priority:** **P1** — custom timing logic on the hot path, but well covered and failure modes are bounded
  (too many or too few retries, not corruption).

---

### RT-06 — `Fanout`: clone-vs-move on the last edge, provenance stamping, closed-consumer accounting
- **Location:** `crates/logit-pipeline/src/fanout.rs:167-414` (`Fanout`); `stamp` `202-213`,
  `stamp_relayed` `215-226`; `deliver` `309-332`; `deliver_blocking` `372-395`; `record_send` `397-403`;
  `record_dropped_on_close` `405-413`; `Delivered` `112-152`; `TraceContext` `40-84`.
  Consuming side: `runtime.rs:2196-2247` (`unwrap_batch`), `783-796` (`unwrap_batch_arc`).
- **What it does:** One consumer → `Delivered::Owned` with no `Arc` at all; more than one → one `Arc::new` plus a
  refcount bump per consumer, with the **last** consumer getting the `Arc` moved rather than cloned.
  `stamp` writes provenance (`origin` via `get_or_insert`, `previous` always overwritten) — the single place in
  the pipeline that ever writes it. `stamp_relayed` is `logit_in`'s back-fill-only variant. Closed consumers are
  skipped and counted.
- **Why sensitive:** hot-path (every batch on every edge); custom (the owned/shared split and the last-edge move
  are the whole copy-on-write design); accounting (`batches.sent`/`events.sent`/`events.dropped{closed_consumer}`
  must reconcile with downstream `received`); data-loss (a closed consumer silently drops, by design).
- **Invariants to verify:**
  - `Delivered::Owned` is produced **iff** the edge has exactly one consumer; a fan-out never hands two branches
    the same owned batch.
  - `unwrap_batch`'s `Arc::try_unwrap`-or-clone keeps branch isolation: a sibling's mutation is never visible
    (`runtime.rs:2212-2231` argues the race is genuinely two-sided).
  - `stamp` is idempotent over an already-stamped `origin`, and `previous` after a target hop is the *target's*
    id while `origin` survives untouched.
  - One batch forking N ways is one `batches.sent`, not N (`record_send` is called once, `400-403`).
  - `record_dropped_on_close` per *consumer*, not per batch — so `events.dropped{closed_consumer}` can exceed
    `events.sent` for a fan-out with several closed consumers; downstream dashboards must expect that.
- **Observed concerns (unverified):**
  - **`logit.component.send.blocked.duration` is recorded on *every* send, not only blocked ones.** The timer is
    created unconditionally at `315`/`378` and always dropped, so the metric is really "send duration," and its
    distribution is dominated by near-zero samples. This is the opposite of the convention `BoundedQueue::push`
    deliberately follows (`queue.rs:254-255`, `316-318`: "a push that never had to wait records no sample at all,
    so the metric isn't muddied by a stream of ~0 durations"). High confidence the two differ; medium confidence
    the `Fanout` side is the wrong one rather than intentional.
  - `deliver`'s single-consumer arm returns at `320` before the explicit `drop(timer)` at `331`; the timer still
    drops at scope exit, so the sample *is* recorded — but the asymmetry makes it easy to misread. Cosmetic.
  - A closed consumer is skipped silently as far as the *pipeline* is concerned (no error, no shutdown signal) —
    `docs/design/pipeline-graph.md`'s backpressure section names this as a deliberate open question, so it is
    context, not a surprise.
- **Existing coverage:** `fanout.rs:416+` unit tests; `crates/logit-bench/tests/allocations.rs` —
  `fanout_send_one_consumer_costs_nothing`, `fanout_send_mixed_output_and_transform_consumers[_when_output_finishes_first]`
  (~1336-1568), `a_mutation_on_one_fan_out_branch_is_invisible_to_the_sibling_branch`;
  `runtime.rs:6627` (`a_fan_out_records_exactly_one_span_not_one_per_branch`),
  `runtime.rs:7241` (`previous_downstream_of_a_target_is_the_targets_id_and_origin_is_untouched`).
  Governed by `docs/adr/arc-eventbatch-copy-on-write.md`, `docs/adr/batch-provenance-on-delivered.md`,
  `docs/adr/trace-context-propagation-on-delivered.md`.
- **Priority:** **P1** — hot path and custom, but the allocation behaviour is pinned by exact-equality tests and
  the failure mode (an extra clone, a misnamed timing metric) is not silent corruption.

---

### RT-07 — `SinkQueue` / `BoundedQueue`: the `Notify` condvar pattern, blocking push, close semantics
- **Location:** `crates/logit-pipeline/src/queue.rs:147-182` (type), `184-225` (`with_metrics`, the
  `Block`→`DropOldest` substitution), `234-321` (`push`), `471-486` (`commit`), `591-628` (`close`),
  `630-632` (`would_overflow`), `643-658` (`update_gauges`), `661-687` (`peek`), `689-805` (`SinkQueue`
  alias, `SinkStore`, `SinkStoreConfig`, `finish`). **Shared with the UDP-intake surveyor:**
  `push_many` (`322-469`), `pop` (`488-522`), `pop_many` (`524-589`), `Queued`/`QueueMetrics` (`21-90`) —
  noted here, not duplicated.
- **What it does:** A `std::sync::Mutex<InMemoryBuffer<T>>` plus two `tokio::sync::Notify`s. `push` under
  `Block` awaits room, constructing the `Notified` *before* the guarded state check. An item that could never
  fit falls through to the underlying `DropOldest` rather than wedging forever. `close()` uses
  `notify_waiters()` on both signals and relies on a documented tokio internal (the `notify_waiters` counter
  snapshot taken at `notified()` construction time) for correctness.
- **Why sensitive:** concurrency (a hand-rolled async condition variable); backpressure (`Block` is the default
  for sinks); data-loss (`DropOldest`/`DropNewest`); `nontrivial-3p-use(tokio)` — `close()`'s correctness
  depends on `Notify::notified()`'s construction-time counter snapshot, an implementation detail pinned against
  tokio 1.53.1 by name (`598-614`); accounting (gauges, `items_dropped`/`units_dropped`).
- **Invariants to verify:**
  - Every wait loop constructs its `Notified` before the state check it guards (`push:269`, `pop:499`,
    `pop_many:550`, `peek:674`, `push_many:402`).
  - `close()` can never be missed by a waiter — the "there is no third window" argument at `598-614`.
  - `impossible_to_ever_fit` (`258`, `410-411`) covers `max_items == 0` and an over-weight item, and the
    `DropOldest` fallback never evicts a reserved head.
  - `peek` requires `T: Clone` and a `peek` without a matching `commit` leaves a standing reservation — the
    caller contract `write_loop` must honour.
  - `Mutex` poisoning is recovered everywhere via `unwrap_or_else(|p| p.into_inner())` (`273`, `406`, `479`,
    `501`, `552`, `676`) — consistent, and no invariant is broken by resuming on a poisoned buffer.
  - `update_gauges` guards both denominators against zero.
- **Observed concerns (unverified):** none spotted in the sink-side code; the reasoning at `591-628` is unusually
  careful and `a_notify_waiters_call_between_constructing_a_notified_and_polling_it_is_never_lost` pins the tokio
  dependency. The one cross-cutting worry is the caller-side `peek`-without-`commit` window described in the
  `write_loop` entry.
- **Existing coverage:** `queue.rs:807+` unit tests (including the tokio-behaviour pin and
  `a_pop_dropped_mid_wait_leaves_no_reservation_a_later_push_can_still_evict`);
  `crates/logit-proto/src/buffer.rs:182+` for `InMemoryBuffer`'s reservation semantics. Governed by
  `docs/adr/buffered-sink-delivery.md`, `docs/adr/decoupled-listener-io.md`,
  `docs/adr/udp-intake-batching-and-socket-visibility.md`.
- **Suggested verification approach:** a loom or shuttle model over `push`/`commit`/`close` with one producer and
  one consumer (the sink shape), asserting no lost wakeup and no permanent park; a re-check of the tokio
  `Notify` internals against the currently pinned tokio version.
- **Priority:** **P1** — a lost wakeup here hangs a sink permanently, but the argument is explicit, the pattern is
  standard, and a regression test already guards the tokio dependency.

---

### RT-08 — `run_transform`: flush-deadline race, close-time flush, and cadence math
- **Location:** `runtime.rs:1255-1333` (`run_transform`), `1614-1671` (`run_flush`),
  `2256-2279` (`advance_flush_deadline`); trait contract `crates/logit-pipeline/src/transform.rs:30-143`;
  reused by `crates/logit-pipeline/src/accumulator.rs:238-248`.
- **What it does:** Races the inbox against the transform's own flush deadline with
  `tokio::time::timeout(wait, inbox.recv())`, flushing when the deadline passes and once more when the inbox
  closes so an in-flight window is not lost. `advance_flush_deadline` computes the next point on the cadence in
  constant time via a nanosecond remainder, so a process stalled for hours does not replay every missed tick.
- **Why sensitive:** hot-path; cancellation (`inbox.recv()` inside a `timeout` must be cancel-safe or a batch is
  lost); data-loss (a missed close-time flush silently discards an aggregation window); custom (the cadence math);
  accounting (`flush.events`, `events.dropped{reason="absorbed"}`).
- **Invariants to verify:**
  - `tokio::sync::mpsc::Receiver::recv` is cancel-safe, so a `timeout` elapse never consumes a batch.
  - The close-time flush runs exactly once, only for a transform with an interval (`1292-1294`).
  - `advance_flush_deadline` returns a point *strictly after* `now`, preserves the original cadence phase, and
    never returns a past instant (the `checked_add` fallback chain at `2278`).
  - Its two `debug_assert`s (`deadline <= now`, `!interval.is_zero()`) hold at both call sites
    (`runtime.rs:1276`, `1958`) and from `BatchAccumulator::next_deadline`.
  - `transform.flush_interval()` is stable across the node's life — `1273-1275` `expect`s `Some` after having
    seen `Some` once.
  - `run_flush` mints **one** root per flush and sends every `(resource, scope)` group as a sibling under it, not
    one root per group.
- **Observed concerns (unverified):**
  - `1273-1275` panics if a transform's `flush_interval()` ever transitions `Some -> None` at runtime. No shipped
    transform does; the `expect` message says so. Low severity, worth a trait-contract note.
  - `run_flush` deliberately mints a **root**, not a child — the *n*-to-1 gap. `docs/known-gaps.md`'s
    internal-spans entry records this as deliberate, so it is context, not a finding.
- **Existing coverage:** `advancing_a_missed_flush_deadline_is_constant_time_and_preserves_cadence` (2293),
  `run_with_shutdown_flushes_an_in_flight_window_before_exiting` (2817),
  `run_transform_flush_mints_a_fresh_root_not_either_absorbed_batchs_context` (6509),
  `run_flush_attaches_the_links_the_transform_produced_to_one_flush_span` (6694),
  `run_flush_sends_every_resource_group_under_one_root_context` (6722),
  `run_flush_carries_the_transforms_scope_onto_the_outgoing_batch` (6764);
  `crates/logit-bench/benches/pipeline.rs:457` (`process_batch_through_keep`). Governed by
  `docs/adr/aggregation-window-semantics.md`, `docs/adr/internal-span-emission-and-deterministic-sampling.md`.
- **Priority:** **P1** — a lost flush is a silently lost aggregation window, but the logic is small, the cadence
  math is directly tested, and `recv` cancel-safety is a documented tokio guarantee.

---

### RT-09 — `process_batch`: in-place `retain_mut` per-event loop and absorbed accounting
- **Location:** `runtime.rs:1335-1387` (`process_batch`); trait at `transform.rs:30-116`.
- **What it does:** The per-batch body of `run_transform`: reads `scope` out, offers it to `observe_scope`,
  applies `map_resource`, then `events.retain_mut(|e| transform.process(&resource, e))` in place, calls
  `end_batch`, and counts `before - after` as `events.dropped{reason="absorbed"}`. Returns `None` for an empty
  result so no empty batch is ever sent.
- **Why sensitive:** hot-path (per event, per batch, on every transform node); custom (the in-place
  `retain_mut` shape exists specifically to avoid memcpy'ing an 864-byte `Event`); accounting.
- **Invariants to verify:**
  - `retain_mut` never reorders surviving events.
  - `map_resource`'s `None` moves the incoming `Arc` through with no clone; `Some` substitutes it for *both*
    `process`'s argument and the outgoing batch.
  - `end_batch` is inside the `process.duration` timer and runs exactly once per batch, including when every
    event was absorbed.
  - `absorbed = before - events.len()` cannot underflow — i.e. `retain_mut` can only shrink.
  - An empty outgoing batch is `None`, never a zero-event send.
- **Observed concerns (unverified):** none spotted.
- **Existing coverage:** `crates/logit-bench/tests/allocations.rs` (exact allocation pins),
  `crates/logit-bench/benches/pipeline.rs:457`; `a_transform_that_absorbs_every_event_still_records_a_span`
  (`runtime.rs:6591`). Governed by `docs/design/memory.md` §7.
- **Priority:** **P2** — hot but small, exhaustively pinned by the allocation suite, and a bug here shows up as a
  measurable allocation-count failure rather than silently.

---

### RT-10 — `run_router` / `route_batch`: the four-pass partition and `RouterScratch` reuse
- **Location:** `runtime.rs:1389-1468` (`run_router`), `1470-1576` (`route_batch`), `1605-1612` (`slot_of`);
  `crates/logit-pipeline/src/router.rs:37-143` (`Destination`, `Router`, `RouterScratch`);
  registry arm `crates/logit-cli/src/pipeline.rs:989-995`.
- **What it does:** Per incoming batch: one child context, one `"process"` span, then a four-pass partition
  (route-borrowing → count → `reserve_exact` per used destination → move) into node-owned scratch buffers that
  `std::mem::take` leaves capacity-0 for the next batch. Sends one batch per non-empty destination under the
  *same* context. Slot 0 is the router's own outbound edge; an empty slot-0 destination counts
  `events.dropped{reason="unrouted"}` explicitly, because `Fanout::deliver` returns early and counts nothing.
- **Why sensitive:** hot-path (per event); custom (the four-pass partition and the exact allocation contract:
  `1 + used destinations` per batch, zero per event); accounting (the `unrouted` counter is the *only* thing
  making unrouted events visible); data-loss (an out-of-range slot falls back to unrouted in release).
- **Invariants to verify:**
  - The partition is total: every event in appears in exactly one destination out (no `Destination::Drop`).
  - `scratch.dests` entries are always left empty on return (`1566-1574`), so a stale event can never leak into
    the next batch.
  - `slot_of(Forward) == 0` and `slot_of(To(n)) == n + 1` agree with `resolve_target_fanouts`' ordering and with
    `RouterScratch::new(targets)` sizing `targets + 1`.
  - The out-of-range guard at `1525-1538` is `usize::from(n) + 1 >= slots` (`>=`, not `>`) — verify the boundary.
  - `counts.resize(slots, 0)` after `clear()` re-zeroes every slot each batch.
  - One incoming batch = exactly one span and one context, however many destinations it forks into.
  - `Fanout::deliver`'s zero-consumer early return is compensated by the explicit `unrouted` count at
    `1456-1463`, and by `send_lua_partitions`' identical block at `2180-2187`.
- **Observed concerns (unverified):** none spotted. The `None => continue` arm at `1447-1450` would silently
  discard a partition if a slot had no `Fanout`, but it is genuinely unreachable given the scratch is sized from
  `targets` itself.
- **Existing coverage:** `a_router_partitions_a_batch_across_two_targets` (6993),
  `unrouted_events_reach_the_routers_ordinary_consumers` (7083),
  `unrouted_events_are_counted_when_a_router_has_no_ordinary_consumers` (7149),
  `one_incoming_batch_forks_into_one_span_however_many_destinations` (7308),
  `route_batch_partitions_into_slot_order_and_leaves_its_scratch_empty_for_reuse` (7530);
  `crates/logit-bench/tests/allocations.rs:1648`
  (`route_batch_two_targets_costs_one_vec_per_used_destination`);
  `crates/logit-cli/tests/route_round_trip.rs`. Governed by `docs/adr/target-components.md`.
- **Priority:** **P1** — custom hot-path partition where a slot-arithmetic error routes events to the wrong
  destination (a silent correctness bug), but the slot ordering is single-sourced and well tested.

---

### RT-11 — Lua node hosting: OS thread, two-oneshot handshake, `catch_unwind`, `Handle::block_on`
- **Location:** `runtime.rs:1673-1779` (`run_lua`), `1781-1799` (`thread_outcome`),
  `1801-1817` (`watch_lua_thread`), `1819-2115` (`run_lua_loop`), `2117-2140` (`lua_slot_of`),
  `2142-2194` (`send_lua_partitions`); spawn site `389-443`; registry arms
  `crates/logit-cli/src/pipeline.rs:581-587`.
- **What it does:** `ScriptWorker` is `!Send`, so a Lua node lives on a dedicated `std::thread`. Two oneshots
  carry its lifecycle: `ready_tx` (script-load outcome → `RunError::Startup`) and `done_tx` (post-ready outcome,
  awaited by the `watch_lua_thread` stand-in `JoinSet` entry). The loop body runs under
  `catch_unwind(AssertUnwindSafe(..))` so a panic becomes a message. Timer waits go through
  `runtime.block_on(async { timeout(..).await })`, with the `async` block deliberately deferring `Sleep`
  construction into the runtime context. Sends use `blocking_send`. A Lua node is also a router: events carry an
  `Option<u16>` mark into the same slot scheme `run_router` uses.
- **Why sensitive:** concurrency (an OS thread outside the `JoinSet`, bridged by oneshots and `block_on`);
  cancellation/shutdown ordering (the thread is reached only via its inbox closing, never by the shutdown
  signal — `watch_lua_thread` deliberately does *not* watch it); data-loss (a dropped `done_tx`, a `blocking_send`
  into a full channel); custom (`AssertUnwindSafe` over `Lua`/`Rc<RefCell>` state); accounting
  (`script.vm.memory`, `script.events.emitted`, `errors{reason=process|flush}`).
- **Invariants to verify:**
  - `done_tx` is sent on **every** post-ready exit path (`1773-1778`) and the `watch_lua_thread` task is spawned
    **only** after a successful `ready_rx` (`421-431`), so no watcher is ever left orphaned.
  - `AssertUnwindSafe` is genuinely sound here: nothing captured is used after an unwind.
  - `Handle::block_on` is never called from a runtime worker thread and never nests inside an `.await` — and the
    runtime outlives every Lua thread (a `block_on` against a shut-down runtime panics).
  - `inbox.blocking_recv()` (`1963`) and `Fanout::blocking_send` are only ever reached off-runtime.
  - The four batch-scoped globals (`trace`, `provenance`, `resource`, `scope`) are reset before **every**
    `flush()` (`1885-1893`) and re-set on **every** batch (`2005-2023`), including a batch where every event
    errored — so nothing leaks between batches.
  - `flush_provenance` (`1847`) matches exactly what `Fanout::stamp` would write on this node's own edge.
  - `lua_slot_of`'s release fallback (slot 0) matches `route_batch`'s, and `scratch.dests` is left empty by
    `send_lua_partitions`' `mem::take` on both the batch and flush paths.
  - `span.error()` fires on any batch with at least one script error (`2077-2087`), including the all-errored
    case where `span.events` is never called.
- **Observed concerns (unverified):**
  - **A Lua thread can block the drain on `blocking_send`.** If a downstream inbox is full and shutdown fires,
    the Lua thread parks in `blocking_send` until the downstream actually closes its receiver. That does happen
    (`run_output` drops `inbox` when it returns), so it unblocks — but the ordering is implicit and unargued in
    the code, and `run_with_telemetry`'s join loop waits on `watch_lua_thread` for it. Medium confidence this is
    fine; low confidence it is *guaranteed* for every downstream node kind.
  - A load failure returning `Startup` at `432-441` leaves *earlier* Lua threads detached (see the startup entry).
  - `run_lua_loop`'s `expect` at `1956-1957` mirrors `run_transform`'s and panics if `configured_interval`
    disagrees with `next_flush` — unreachable, both derive from the same immutable value.
- **Existing coverage:** `a_lua_node_processes_events_end_to_end_through_the_graph` (2354),
  `run_lua_records_vm_memory_and_emit_outcome` (3310),
  `run_lua_process_writing_{resource,scope}_re_stamps_the_outgoing_batch` (3445/3546),
  `run_lua_marks_its_process_span_as_error_when_every_event_in_the_batch_errors` (3669) and the mixed-batch
  variant (3776), `a_flush_with_no_batch_ever_received_still_records_vm_memory` (3887),
  `a_lua_thread_panicking_after_ready_flips_failed_and_returns_runtime` (6149),
  `a_lua_node_finishing_on_its_own_reaches_finished` (6224), `watch_lua_thread_maps_each_outcome` (6326),
  `a_lua_router_splits_a_batch_two_ways` (7618), `an_unmarked_event_reaches_the_lua_routers_ordinary_consumers`
  (7706), `an_unknown_target_in_lua_counts_a_script_error_and_does_not_kill_the_node` (7774),
  `lua_flush_output_honours_marks` (7894), and the `lua_flush_*` root-context block (8128-8280).
  Governed by `docs/adr/lua-flush-root-context.md`, `docs/adr/target-components.md`,
  `docs/adr/lua-event-constructor.md`, `docs/design/lua-api.md`.
- **Suggested verification approach:** targeted review of the thread-lifecycle state machine (every exit path
  × every oneshot); a shutdown-under-load test with a Lua node feeding a wedged sink; confirm the runtime cannot
  be dropped while a Lua thread is inside `block_on`.
- **Priority:** **P0** — a thread that never reports, or a `block_on` against a dead runtime, hangs or crashes the
  process; the whole bridge is hand-built.

---

### RT-12 — `BatchAccumulator`: incremental weight tracking and the `(resource, scope)` key
- **Location:** `crates/logit-pipeline/src/accumulator.rs:53-259` — `absorb` `148-213`, `take` `215-225`,
  `current_weight` `227-236`, `next_deadline` `238-248`, `scope_eq` `251-259`.
- **What it does:** Merges many small decoded batches into fewer larger ones under three bounds
  (`max_events`, `max_bytes`, caller-driven interval). Keeps `resource_weight`/`scope_weight`/`events_weight`
  incrementally so `current_weight` is O(1) rather than re-walking every held event, and flushes whenever the
  `(resource, scope)` key changes so events are never relabelled onto the wrong identity.
- **Why sensitive:** hot-path (per decoded datagram/line on every listener); custom (the incremental weight
  decomposition claims to be *exact*, not approximate); data-loss/corruption (merging across distinct resources
  would silently relabel events — "a correctness bug no test would catch since the output stays well-formed",
  its own doc comment at `121-124`); backpressure (the byte bound is what caps a listener's outbound batch).
- **Invariants to verify:**
  - `resource_weight + scope_weight + events_weight + capacity*size_of::<Event>()` equals
    `EventBatch::estimated_heap_bytes` **exactly** for any sequence of absorbs — including after a
    `ResourceChange`/`ScopeChange` flush, where `events_weight` is *assigned* (`188`) rather than accumulated.
  - `take()` resets all three weights to zero (`221-223`) and `resource`/`scope` to `None`.
  - `holding_something` (`159`) correctly suppresses a spurious `ScopeChange` on the very first absorb.
  - Identity comparison is `Arc::ptr_eq`, not value equality, on both terms — so a decoder that builds a fresh
    equal-content `Arc` per batch degenerates to one batch per datagram (a performance cliff, not a correctness
    bug).
  - An empty `events` is a total no-op (`155-157`); `Vec::append` leaves the caller's scratch capacity intact.
  - The `ResourceChange`/`ScopeChange` path defers bound checking by at most one absorb and drops nothing.
- **Observed concerns (unverified):** none spotted. Resource wins over scope when both changed at once (`189-193`)
  — correct, since the held batch is flushed either way and only the reason tag differs.
- **Existing coverage:** `accumulator.rs:261+` unit tests; exercised in anger by `crates/logit-inputs`' UDP and
  TCP drivers. Governed by `docs/adr/decoupled-listener-io.md`, `docs/design/memory.md` §2.
- **Priority:** **P1** — hot path with a bespoke exactness claim, but a weight error only mis-sizes batches; the
  `(resource, scope)` rule is the part whose failure would be silent corruption.

---

### RT-13 — `Readiness`: monotonic phase transitions under concurrent writers
- **Location:** `crates/logit-pipeline/src/readiness.rs:110-211` — `channel`/`disabled` `123-136`,
  `begin` `149-167`, `set_node` `169-177`, `ready` `179-188`, `draining` `190-199`, `failed` `201-210`.
  Writers: `runtime.rs:196, 252, 297, 348, 362, 367, 379, 430, 468, 487, 501, 503` and the shutdown driver at
  `224`. Reader: `crates/logit-cli/src/admin.rs`.
- **What it does:** One `watch<PipelineState>`. Every update goes through `send_modify` (infallible, holds the
  watch's write lock for the closure), which is what makes the monotonicity rules atomic against a second
  concurrent writer — the join loop and the shutdown driver genuinely race.
- **Why sensitive:** concurrency (a genuine race between the driver task and the join loop);
  `nontrivial-3p-use(tokio)` (`send_modify` chosen over `send` precisely so `Readiness::disabled()` — a channel
  with no receivers by construction — never needs a `let _ =` at every call site); accounting (this is what an
  orchestrator routes traffic on).
- **Invariants to verify:**
  - `Starting -> Ready -> Draining` is one-way; `Failed` is reachable from any state and terminal.
  - `draining()` is a no-op from `Failed` (`194`) — a SIGTERM after a failure must not paper over it.
  - `ready()` is a no-op from `Draining`/`Failed` (`184`).
  - `begin()` deliberately does **not** assign `phase` (`160-166`) — the argument at `152-159` is that assigning
    it could move an already-advanced phase backwards.
  - `set_node` on an unknown id is a silent no-op (`173-175`) — verify `begin` really seeds every id the runtime
    will later name, including targets.
  - `since` moves only on a real phase *change*.
  - A `NodeState::Alias` is set once and never transitions.
- **Observed concerns (unverified):** none spotted. The `begin`-before-spawn ordering argument at `181-193` of
  `runtime.rs` is the subtle part and it is explicitly reasoned.
- **Existing coverage:** `readiness.rs:213+`; `runtime.rs` `phase_reaches_ready_then_draining_on_a_normal_run`
  (6043), `a_lua_thread_panicking_after_ready_flips_failed_and_returns_runtime` (6149),
  `a_lua_node_finishing_on_its_own_reaches_finished` (6224),
  `a_sustained_permanent_sink_failure_returns_runtime_not_startup` (6353),
  `readiness_disabled_never_panics_across_a_full_run` (6428); `crates/logit-cli/tests/admin_ready.rs`,
  `crates/logit-cli/tests/exit_codes.rs`. Governed by `docs/adr/admin-readiness-endpoint.md`,
  `docs/plans/operator-surface.md`.
- **Priority:** **P2** — well contained, the monotonicity rules live in the type rather than in caller ordering,
  and a wrong answer misroutes traffic rather than losing it.

---

### RT-14 — Graph rules the runtime *assumes* (cycle detection, target arity, slot order)
- **Location:** `crates/logit-pipeline/src/graph.rs:2936-3032` (`topological_order` — Kahn plus cycle recovery),
  `485-505` (`target_edges`), `507-527` (`targets_of`), `719-...` (`resolve`; rule 5 cycle check at `757`,
  rule 50 router exemption at `799-807`, rule 48/49 target checks around `879-915`).
  Runtime consumers: `runtime.rs:265-267, 291, 323, 376, 397, 1596`.
- **What it does:** Validation, mostly — but three of its outputs are load-bearing at runtime: (1) **no cycles**,
  including router→target edges, which is what stops a runtime deadlock rather than merely an ugly graph;
  (2) **rule 49** (nothing names a `target` as a source), which is what makes `senders[c]` at `runtime.rs:291`
  and `323` non-panicking; (3) **`targets_of`'s order is the slot order** used identically by `Destination::To(n)`,
  the runtime's `Vec<Fanout>`, and the Lua name→slot table.
- **Why sensitive:** the runtime *indexes and panics* on these guarantees; a cycle that slipped through is a
  runtime deadlock, not a validation message.
- **Invariants to verify:**
  - `topological_order` counts **both** edge kinds (`sources` and router→target) in indegree (`2955-2967`), so
    `router -> target -> .. -> router` is rejected.
  - An id naming no defined component is skipped on both sides (`2957`, `2963`) without corrupting indegree.
  - The cycle-recovery walk at `2999-3021` cannot dead-end or loop forever ("a stuck node always has a stuck
    source", `3015`) and the `path[start..]` slice is really the cycle, not its lead-in tail.
  - `targets_of` de-duplicates while `target_edges` does not, and the de-duplicated order is stable and derived
    in exactly one place.
  - Rule 49's coverage is total (every kind, not just the ones with explicit `sources` validation).
- **Observed concerns (unverified):** none spotted in the runtime-facing subset. The self-loop skip at `2963`
  (`target != id`) defers a router naming itself to rule 48 for a better message — verify rule 48 actually runs
  in every path that reaches the runtime.
- **Existing coverage:** `graph.rs:3034+` (`two_node_cycle_is_rejected` 3441, `longer_cycle_is_rejected` 3447,
  `a_cycle_is_reported_as_a_concrete_path` 3457), plus the rule-by-rule suite. Governed by
  `docs/adr/component-graph-configuration.md`, `docs/adr/target-components.md`,
  `docs/design/pipeline-graph.md`.
- **Priority:** **P1** — validation, so a gap surfaces as a hang or panic at startup rather than as data loss,
  but the runtime genuinely indexes on it.

---

### RT-15 — `logit-cli::pipeline`: process lifecycle, the double-signal kill switch, and config→runtime knob mapping
- **Location:** `crates/logit-cli/src/pipeline.rs:105-185` (`run_pipelines`), kill switch `141-145`,
  admin bind `147-169`, `188-246` (`prepare`), `248-267` (`shutdown_signal`),
  `1000-1023` (`queue_config`), `1025-1038` (`write_config`), `1198-1204` (`input_runtime_config`),
  `1134-1143` (`overflow_policy`), `1234-1242` (`delivery_posture`), `531-547` (`internal` arm),
  `582-587` (`lua_file` read), `983-995` (`target`/`route` arms).
- **What it does:** Loads and resolves the config, activates the `TelemetryLayer` once the `internal` component's
  `logs` threshold is known, spawns a detached kill-switch task that `std::process::exit(130)`s on a **second**
  signal, binds the admin listener synchronously (a bind failure is `Startup`), and runs the graph.
  `shutdown_signal()` installs an *independent* listener per call and is called three times, relying on all
  three being notified. `queue_config`/`write_config`/`input_runtime_config` are the only places config values
  become runtime behaviour (`SinkStoreConfig`, `RetryConfig`, `WriteLoopConfig`, `InputRuntimeConfig`).
- **Why sensitive:** concurrency (three independent signal listeners, a detached task calling `process::exit`);
  shutdown ordering (the admin server is deliberately *not* given a shutdown listener, so `/readyz` can answer
  `503 draining` throughout the drain); data-loss via misconfiguration (a wrong `overflow`/`delivery`/
  `shutdown_grace` mapping silently changes drop behaviour). Everything else in this 4k-line file is thin
  `ComponentKind` → constructor plumbing and is **not** sensitive.
- **Invariants to verify:**
  - Multiple `tokio::signal::unix::signal(SignalKind::terminate())` listeners really are all notified — the whole
    kill switch depends on it (`248-251` asserts this in prose).
  - The kill switch's `process::exit(130)` can only fire on a genuine *second* signal, never on a duplicate
    wakeup of the first.
  - `kill_switch.abort()` and `admin_server.abort()` (`174-177`) happen after `run_with_telemetry` returns, on
    both the `Ok` and `Err` paths.
  - `queue_config` resolves `disk.path` against `base_dir`, not the process CWD (`1015`).
  - `write_config` maps `retry_budget`/`retry_max_delay`/`shutdown_grace`/`delivery` faithfully, and leaving
    `base_delay` at the default (`1032`) is intentional.
  - `prepare`'s sorted build order (`227-228`) makes startup failures reproducible and `Registry::drain` order
    deterministic.
  - The `internal` arm's `expect` (`536-538`) is genuinely guaranteed by graph rule 13.
- **Observed concerns (unverified):**
  - `std::process::exit(130)` from a detached task (`144`) skips every destructor — including any disk-queue
    cursor `fsync`. That is the intent of a kill switch, but it means the second signal can cost more replay on
    restart than the first. Documented at `97-99` as deliberate; noting it as context.
  - **No `unsafe` here** (contrary to the survey brief). Confirmed by grep over the whole crate's `src/`.
- **Existing coverage:** `crates/logit-cli/tests/admin_ready.rs` (real SIGTERM + drain probe),
  `exit_codes.rs`, `durable_buffer_restart.rs`, `logging_flags.rs`, `route_round_trip.rs`, and the per-protocol
  round-trip suites; `pipeline.rs`'s own `#[cfg(test)]` block at `1523+` (including `run_config` at `271-283`).
  Governed by `docs/adr/service-lifecycle-and-output-retry.md`, `docs/adr/admin-readiness-endpoint.md`,
  `docs/adr/tracing-for-self-logging.md`, `docs/deploying.md`.
- **Priority:** **P2** — the risky part is small and integration-tested against a real signal; the bulk of the
  file is plain config plumbing.

---

### RT — Cross-cutting notes

**Shared helpers other areas depend on:**
- `advance_flush_deadline` (`runtime.rs:2264-2279`) is `pub(crate)` and reused verbatim by
  `BatchAccumulator::next_deadline` (`accumulator.rs:242-248`), which every listener in `logit-inputs` drives.
  A change to its cadence semantics changes both the transform flush timer and every listener's batch timer.
- `BoundedQueue<T>` (`queue.rs:156-659`) is one implementation serving both `SinkQueue` (this area) and
  `ReceiveQueue` (the UDP-intake surveyor's). `push_many`/`pop_many`/`Queued`/`QueueMetrics` are theirs;
  `push`/`peek`/`commit`/`close`/`update_gauges`/`would_overflow` are shared by both sides. The `Notify`
  construct-before-check ordering and `close()`'s dependence on tokio's `notify_waiters` counter snapshot are
  **shared correctness arguments** — verify once, for both.
- `Fanout` is the single choke point for every producer's send-side telemetry and the **only** place provenance
  is ever written (`stamp`/`stamp_relayed`). Any listener/transform survey that touches provenance should read
  `fanout.rs:202-226` rather than re-deriving it.
- `SinkStore`/`SinkStoreConfig` (`queue.rs:703-805`) is the seam to `disk_queue.rs`: `open`, `push`, `peek`,
  `commit`, `close`, `finish`. `run_output` relies on two `DiskQueue` behaviours it does not own —
  `push` short-circuiting its overflow policy once `closed` (`runtime.rs:675-689`), and `finish()` returning
  `(0, 0)` unconditionally. The disk-queue surveyor should confirm both.

**Things noticed outside my area that another surveyor should pick up:**
- `logit_core::trace_is_sampled` (`crates/logit-core/src/telemetry.rs:110-127`) is the deterministic sampler
  every span in this area is gated on. It uses the top 53 bits of the low 8 `trace_id` bytes and deliberately
  keeps NaN/`>= 1.0` rates (`!(rate < 1.0)`, with a `#[allow(clippy::neg_cmp_op_on_partial_ord)]`). The
  correctness of "every `logit` process reaches the same verdict independently" rests entirely there, not in
  `logit-pipeline`.
- `logit_core::telemetry`'s `MAX_LINKS_PER_SPAN = 32` (`telemetry.rs:92`) silently bounds what `run_flush`
  (`runtime.rs:1659`) attaches; the drop is counted as
  `logit.internal.span.links.dropped{reason="cardinality"}`.
- `InMemoryBuffer::peek` sets `head_reserved` (`crates/logit-proto/src/buffer.rs:145-150`) and only `commit`
  clears it — this reservation lifecycle crosses three crates and is the subtlest shared invariant I found.
- `Output::observe_batch` (`output.rs:69-71`) is implemented only by `logit_out`; whoever surveys
  `logit-outputs` should confirm it tolerates being called once per *attempt* (retries included), as
  `write_loop:1137` does.


---

## WIRE — Native wire format and HTTP/gRPC/TLS transports

Area: `crates/logit-proto/src/frame.rs`, `crates/logit-proto/src/native/*`,
`crates/logit-inputs/src/{logit,otlp,http,tls,prometheus}.rs`,
`crates/logit-outputs/src/{logit,otlp,http,tls,prometheus}.rs`.
Out of scope here (other surveyors): the OTLP `Event` mapping codecs under
`crates/logit-proto/src/otlp/`, the Prometheus text/protobuf codecs under
`crates/logit-proto/src/prometheus/`, `logit-pipeline`'s `SinkQueue`/`write_loop`/`DiskQueue`.
20 entries, ordered wire-format → native transport → OTLP/HTTP/TLS → Prometheus transport. Every
cited line number was read, not inferred; the Prometheus entries came from a dedicated second pass
whose references were sampled and re-verified.

Third-party crates in play (from the three `Cargo.toml`s): `lz4_flex`, `crc32c`, `bytes`, `prost`,
`snap`, `serde_json`, `base64` (logit-proto); `hyper`, `hyper-util`, `http`, `http-body-util`,
`flate2`, `rustls`, `rustls-pki-types`, `tokio-rustls`, `reqwest`, `socket2`, `libc` (inputs);
`reqwest`, `hyper`, `hyper-util`, `hyper-rustls`, `webpki-roots`, `rustls`, `tokio-rustls`,
`flate2`, `snap` (outputs).

---

### WIRE-01 — Frame envelope: 24-byte header, CRC-32C over compressed bytes, lz4 bounds, resync
- **Location:** `crates/logit-proto/src/frame.rs:24-55` (`MAX_SANE_UNCOMPRESSED_LEN`,
  `MAX_SANE_COMPRESSED_LEN`), `:102-163` (`FrameHeader::write`/`FrameHeader::read`),
  `:174-214` (`write_frame`/`write_frame_with_flags`), `:221-271`
  (`read_frame`/`read_frame_with_header`), `:276-299` (`lz4_compress`/`lz4_decompress`),
  `:308-310` (`resync`)
- **What it does:** Wraps one codec payload in a fixed 24-byte little-endian header (magic,
  version, flags, codec, compression, reserved, uncompressed_len, compressed_len, crc32c),
  optionally lz4-block-compressing the payload and checksumming the *compressed* bytes. The read
  side re-checks both declared lengths against sanity caps before allocating, verifies the CRC
  before handing anything to `lz4_flex`, and re-checks that the decompressed length matches what
  the header declared. `resync` scans for the next `LGIT` magic after a torn write.
- **Why sensitive:** hot-path (every native batch, every disk-spool record, every control
  message goes through here); custom (hand-rolled framing, no framing crate); untrusted-input
  (`uncompressed_len`/`compressed_len` are raw `u32`s off a socket used to size allocations, and
  a decompression bomb is the direct threat the caps exist for); data-loss (a wrong
  `Truncated` vs `Malformed` classification makes a disk-spool reader silently discard every
  record after the bad one — see the comment at `:509-513`); nontrivial-3p-use(lz4_flex) (raw
  *block* API with caller-sized buffers, `decompress_into` + truncate, not the framed API).
- **Invariants to verify:**
  - CRC is computed and checked over exactly the bytes on the wire (compressed form), on both
    sides, and always *before* decompression runs.
  - `MAX_SANE_COMPRESSED_LEN = MAX + MAX/255 + 16` genuinely covers lz4 block worst-case
    expansion for a payload at `MAX_SANE_UNCOMPRESSED_LEN`, so `write_frame` can never emit a
    frame `read_frame` refuses.
  - `lz4_decompress` truncates to bytes actually written, so the `payload.len() != uncompressed_len`
    check at `:263` is a real check, not a tautology.
  - "Too few bytes" is `Truncated`; "the bytes present are wrong" is `Malformed`. No path returns
    `Truncated` for a corrupt length field.
  - `FrameHeader::read` consumes exactly `HEADER_LEN` bytes on every path including the error
    ones a caller may retry after (it does not, on the early-return at `:132`; verify no caller
    depends on partial consumption).
  - `write_frame`/`read_frame` reject `Compression::Zstd` symmetrically rather than treating it as
    `None`.
- **Observed concerns (unverified):**
  - `write_frame_with_flags` casts `payload.len() as u32` (`:206`) with no check against
    `MAX_SANE_UNCOMPRESSED_LEN`; a >4 GiB payload would wrap silently. Callers do bound it
    (`logit_out` at `crates/logit-outputs/src/logit.rs:351`, `DiskQueue`), and the doc comment at
    `:31-35` explicitly acknowledges the asymmetry, but the check lives in every caller rather
    than here. **Low confidence this is reachable; medium confidence it's worth centralizing.**
  - `resync` (`:308`) is `O(n)` per call over the remaining buffer and a caller that resyncs
    repeatedly past spurious magics is `O(n²)`; not a concern for a 24-byte-header stream, worth a
    glance at the disk-spool caller. **Low confidence.**
- **Existing coverage:** in-file unit tests `frame.rs:312-515` (round trips, bad magic, unknown
  version, corrupt CRC, both truncation classes, both sanity caps, short-decompress, zstd,
  resync); `crates/logit-proto/tests/robustness.rs` (`read_frame_survives_every_single_byte_truncation`,
  `read_frame_survives_seeded_bit_flips`, `read_frame_rejects_a_{uncompressed,compressed}_len_inflated_to_u32_max`,
  `read_frame_never_allocates_proportionally_to_a_hostile_uncompressed_len`);
  `crates/logit-bench/benches/wire_format.rs`. Governed by
  [ADR `native-wire-format-encoding`](docs/adr/native-wire-format-encoding.md) and
  `docs/design/wire-protocol.md`.
- **Suggested verification approach:** targeted review of the cap arithmetic against `lz4_flex`'s
  documented worst case; a proptest that `read_frame(write_frame(p, c)) == p` for arbitrary
  payload/compression; a `cargo-fuzz` target over `read_frame` (`docs/known-gaps.md` records fuzz
  targets as deliberately deferred for toolchain reasons — this is the highest-value place to
  revisit that); malicious-frame table test (declared lengths at/over each cap, CRC-correct
  garbage lz4).
- **Priority:** P0 — every byte of the native path crosses this, the logic is entirely custom, and
  a length/classification error means silent spool truncation or an allocation DoS.

---

### WIRE-02 — Dictionary-first symbol table and `Value` TLV decode (untrusted counts, depth, interning)
- **Location:** `crates/logit-proto/src/native/dict.rs:18-52` (`DictBuilder`), `:57-100`
  (`Dict::read`/`Dict::get`, `MAX_SANE_DICT_ENTRIES` at `:65`);
  `crates/logit-proto/src/native/value.rs:39` (`MAX_VALUE_DEPTH`), `:41-106` (`write_value`),
  `:108-194` (`read_value_at`/`read_attr_map_at`);
  `crates/logit-proto/src/native/varint.rs:12-69` (`write_uvarint`/`read_uvarint`/zigzag/`read_u8`)
- **What it does:** Every `Symbol` (a process-local `lasso::Spur`) is resolved to a string at
  encode time into a per-batch dictionary written first, and re-interned into the *reader's*
  global interner at decode time; everything else references it by index. `Value`s are
  `tag + uvarint len + payload`, so an unknown tag is skipped by byte count and degrades to
  `Value::Null`; nesting is recursion-bounded at 128 levels on the read side only.
- **Why sensitive:** hot-path (one dictionary lookup per attribute key per event; `read_uvarint`
  is the innermost loop of the whole codec); custom (hand-rolled LEB128 and TLV, no serde);
  untrusted-input (dictionary count, per-entry length, array count, and map count are all
  attacker-controlled and feed `Vec::with_capacity` and `split_to`); data-loss (an unrecognized
  tag silently becomes `Null` — a real value disappears rather than erroring);
  accounting (every decoded string permanently grows the never-evicting global interner).
- **Invariants to verify:**
  - Every declared length is compared against `bytes.len()` *before* `split_to`, and every count
    is `.min(4096)`-clamped before `with_capacity` (dict.rs:75, value.rs:157).
  - The decode-side recursion cap is reached on both the `Array` and `Map` paths and cannot be
    bypassed by alternating them (`read_attr_map_at` passes `depth` unchanged at value.rs:190,
    and `TAG_MAP` adds 1 at `:163` — confirm the combination still increments once per nesting
    level).
  - `read_uvarint`'s 10-byte bound cannot loop forever and cannot produce a value that then
    overflows a downstream `as usize`/`as u32` cast.
  - Re-interning an attacker-supplied dictionary cannot grow the process-global interner without
    bound across many connections (the interner never evicts — this is an unbounded-growth vector
    distinct from per-request memory caps).
  - `Str` is UTF-8-validated (value.rs:150) and `Bytes` deliberately is not.
- **Observed concerns (unverified):**
  - `read_uvarint` (varint.rs:27-40) accepts non-canonical encodings and, on the 10th byte,
    `<< 63` silently discards the byte's upper 6 bits — two distinct byte strings decode to the
    same `u64`. Harmless for correctness of a single decode, but it means `encode(decode(x)) != x`
    at the byte level for a crafted input, which matters if any code ever compares wire bytes.
    **Medium confidence this is real, low confidence it matters today.**
  - `Dict::read` interns every entry into the *global* interner before any of the batch has been
    validated (dict.rs:87). A peer that sends frames whose dictionaries are all-unique random
    strings grows that interner permanently, at up to `MAX_SANE_DICT_ENTRIES` (16M) per frame,
    with no per-connection or global budget. This is the one resource here that survives the
    request. **Medium-high confidence; `docs/known-gaps.md` discusses interner growth for
    `otlp_in`/`json` but I did not find this specific native-path case named.**
  - Unknown `Value` tag → `Value::Null` (value.rs:168) is documented, but it means a lossy-transit
    failure that no counter records — no `logit.proto.errors` or skip counter fires.
    **High confidence it's intentional (module doc argues it), low confidence it's fully benign.**
- **Existing coverage:** `dict.rs:102-185`, `value.rs:196-333` (unknown tag, both depth caps,
  exactly-at-cap round trip), `varint.rs:71-130`; `crates/logit-proto/tests/robustness.rs`
  (`decode_batch_rejects_a_dictionary_count_inflated_far_past_the_sanity_cap`,
  `decode_batch_never_allocates_proportionally_to_a_hostile_dictionary_count`,
  `decode_batch_rejects_value_nesting_past_the_depth_cap`). ADR `native-wire-format-encoding`,
  `docs/design/wire-protocol.md`'s "dictionary-first batches", `docs/design/data-model.md`.
- **Suggested verification approach:** proptest round-trip over arbitrary `Value` trees;
  a targeted memory test that N frames with all-distinct dictionaries do not grow RSS unboundedly
  (measures the interner claim); fuzz target on `decode_batch` with a dictionary-heavy corpus.
- **Priority:** P0 — innermost decode loop, entirely custom, directly fed by untrusted bytes, and
  the interner-growth path has no bound at all.

---

### WIRE-03 — Record TLV decode: default-elision encoding, required fields, and opaque sketch blobs
- **Location:** `crates/logit-proto/src/native/record.rs:49-101` (`write_field`/
  `write_scalar_field`/`for_each_field`), `:106-195` (`write_record_list`/`read_record_list_into`/
  `ListSink`), `:331-536` (`write_metric_kind`/`read_metric_kind`), `:566-582` (`read_exemplar`),
  `:618-646` (`read_metric_record`), `:702-723` (`read_trace_ref`), `:761-795` (`read_log_record`),
  `:902-940` (`read_span_link`), `:1036-1112` (`read_span_record`), `:1146-1167` (`read_event`),
  `:1189-1249` (`read_resource`/`read_scope`)
- **What it does:** Encodes every record type as a TLV field stream, writing a field only when its
  value differs from the decode-side default; lists of records are `count` + per-entry
  length-prefixed bodies. `read_metric_kind` is a closed dispatch (an unknown kind tag is a hard
  error), and `Distribution`/`Set` hand an opaque attacker-controlled blob to
  `DdSketch::from_java_bytes` / `HyperLogLog::from_bytes` in `logit-core`.
- **Why sensitive:** hot-path (per-record, per-event, per-batch); custom (hand-rolled TLV with an
  encode-side default-elision rule that has to mirror the decode-side defaults exactly);
  untrusted-input (every count and length is off the wire; `read_record_list_into` at `:166-182`
  loops `count` times with no cap on `count` itself, only on the `reserve` hint);
  data-loss (an encode-side default-elision mismatch silently changes a value in transit);
  nontrivial-3p-use(cardinality-estimator) — `METRIC_SET` reaches `HyperLogLog::from_bytes`, which
  `docs/known-gaps.md` documents as working around an upstream *undefined-behavior* allocation-layout
  bug reachable through exactly this decode path.
- **Invariants to verify:**
  - For every field, "skipped on encode" ⟺ "decodes to that same default". The elision conditions
    (`!= 0`, `is_some`, `!is_empty`, `!= Raw`, `!= Internal`, `!= Unset`) must each match the
    reader's initializer one-for-one.
  - `write_scalar_field`'s declared length always matches what the closure writes (only a
    `debug_assert` guards it, `:72-76` — a release-mode mismatch desyncs every following field).
  - `read_record_list_into`'s unbounded `count` loop terminates on truncated input via the
    per-entry length check at `:170`, and cannot be made to allocate proportionally to a huge
    `count` before any entry is read.
  - `read_metric_kind`'s per-variant sequential layouts consume exactly their declared `len`
    (nothing checks for trailing bytes inside a kind body, unlike `read_record_list_into`'s
    `:178-180` trailing-byte check).
  - `HyperLogLog::from_bytes`/`DdSketch::from_java_bytes` bound their own claimed member/bucket
    counts before allocating, since the blob here is fully attacker-controlled.
  - `read_trace_ref`'s fixed-width reads (`:703`, `:712`) can never read past the field slice.
- **Observed concerns (unverified):**
  - `read_metric_kind` does not reject trailing bytes inside a kind body (`:434-535`), so a
    crafted frame can carry padding the writer never emits; benign, but it breaks byte-level
    fixed-point for that record. **Medium confidence.**
  - `read_exponential_buckets` (`:315-323`) reads `count` uvarints with the `.min(4096)` reserve
    clamp but no cap on `count`; truncation stops it, but a 64 MiB frame of 1-byte varints
    produces a ~500M-element `Vec<u64>` (4 GiB). The frame cap bounds bytes-in, not
    elements-out — the expansion ratio is ~8×. Same shape at `:445` (`Samples`), `:461`
    (`SetMembers`), `:478` (`Histogram`), `:516` (`Summary`). **Medium-high confidence this is a
    real amplification factor worth measuring.**
  - The `logit-core` sketch readers are outside this area but are reached *only* through here and
    through OTLP; flag to whoever surveys `logit-core`.
- **Existing coverage:** `record.rs:1251-1675` (round trips for every metric kind, fully-populated
  records); `crates/logit-proto/tests/robustness.rs` truncation/bit-flip suites over
  `decode_batch` (which reaches all of this). ADR `native-wire-format-encoding`,
  `docs/design/wire-protocol.md`, `docs/design/data-model.md`.
- **Suggested verification approach:** proptest `decode(encode(record)) == record` over generated
  `MetricRecord`/`SpanRecord`/`LogRecord` (the pattern `crates/logit-proto/tests/otlp_fixed_point.rs`
  already uses for OTLP); a peak-allocation test for the list-expansion ratio above, mirroring
  `robustness.rs`'s `peak_live_bytes` harness; a review pass that diffs each writer's elision
  condition against the matching reader's initializer.
- **Priority:** P0 — silent value change on a default-elision mismatch, and the count-expansion
  paths are an unmeasured memory amplifier on a network-reachable decoder.

---

### WIRE-04 — Batch framing v1/v2 and the mandatory provenance trailer
- **Location:** `crates/logit-proto/src/native/mod.rs:49-67` (`CODEC_NATIVE_V1`/`V2`, trailer tags,
  `MAX_SANE_TRAILER_FIELD_BYTES`), `:82-115` (`encode_batch`), `:120-173` (`decode_batch`,
  `MAX_SANE_EVENT_COUNT`), `:187-209` (`encode_batch_v2`/`write_trailer_field`), `:218-259`
  (`decode_batch_v2`/`trailer_str`), `:290-308` (`NativeDecoder::decode_into`)
- **What it does:** Lays out a payload as dictionary → length-prefixed resource → mandatory scope
  presence byte → event count → length-prefixed events, with v2 appending a mandatory
  length-prefixed provenance trailer. The "no proper prefix of a valid encoding is itself valid"
  property is the stated reason the scope section and the trailer are both mandatory rather than
  optional-trailing.
- **Why sensitive:** hot-path; custom; untrusted-input (`resource_len`, `scope_len`, `event_count`,
  per-event `body_len`, `trailer_len` all off the wire); data-loss/duplication (v1-vs-v2 dispatch
  is decided by the negotiated codec byte, not by the payload — decoding a v2 payload as v1 leaves
  trailing bytes, decoding v1 as v2 errors); accounting (the provenance trailer is what
  `Delivered`'s origin/previous chain is built from downstream).
- **Invariants to verify:**
  - No proper prefix of a valid v1 *or* v2 encoding decodes successfully (the pinned property).
  - `decode_batch` consumes exactly the payload on a valid input — `NativeDecoder::decode_into`
    (`:298-305`) never checks that `payload` is empty afterwards, so trailing bytes in a v1 frame
    are silently ignored.
  - `MAX_SANE_EVENT_COUNT` (16M) times the minimum per-event encoding is still bounded by the
    64 MiB frame cap; the `with_capacity(count.min(4096))` clamp holds.
  - Unknown trailer tags are skipped whole and cannot desync the trailer loop (`:250`).
  - `intern()` on trailer strings is bounded by `MAX_SANE_TRAILER_FIELD_BYTES` (4096) but not by
    a count — a trailer can repeat `TRAILER_TAG_ORIGIN` arbitrarily many times within
    `trailer_len`, interning each one.
- **Observed concerns (unverified):**
  - `NativeDecoder::decode_into` ignores leftover payload bytes after `decode_batch`
    (`mod.rs:304`), unlike `decode_batch_v2`'s own "consumed the whole payload" test at `:539`.
    A frame with a v1 payload plus junk decodes clean. **High confidence it's true; low confidence
    it matters, since `logit_in` does not use `NativeDecoder` but calls `decode_batch` directly.**
  - Repeated trailer tags each intern a new string into the global interner (`:248-249`), bounded
    only by `trailer_len` / 3 bytes per entry. Same unbounded-interner theme as the dictionary.
    **Medium confidence.**
- **Existing coverage:** `mod.rs:310-607` (round trips, empty batch, populated scope, foreign codec
  byte, concatenated frames, v2 provenance both-present/both-absent, the every-proper-prefix test
  at `:565`, v1-payload-rejected-by-v2, unknown trailer tag);
  `crates/logit-proto/tests/robustness.rs` truncation/bit-flip/inflated-count suites.
  ADR `native-wire-format-encoding`, `docs/adr/batch-provenance-on-delivered.md`,
  `docs/design/wire-protocol.md`.
- **Suggested verification approach:** proptest round-trip over generated `EventBatch` +
  `Provenance`; extend the existing every-proper-prefix assertion to v1 `decode_batch` and to
  `NativeDecoder::decode_into`; a differential test that a v2 payload fed to `decode_batch`
  (wrong codec) is detected rather than partially accepted.
- **Priority:** P1 — well covered by the existing robustness suite; the residual risks are the
  trailing-bytes laxity and interner growth rather than corruption.

---

### WIRE-05 — Control-message TLV and the `Hello`/`HelloAck` negotiation state machine
- **Location:** `crates/logit-proto/src/native/control.rs:24-49` (version, reject codes, caps),
  `:53-109` (`write_field`/`read_field`/`read_choice_list`), `:136-178` (`Hello`),
  `:199-236` (`HelloAck`), `:250-273` (`Ack`), `:289-324` (`Reject`), `:339-374`
  (`ControlMessage::decode`/`expect_msg_type`);
  `crates/logit-inputs/src/logit.rs:640-721` (`handshake`, the listener's choice);
  `crates/logit-outputs/src/logit.rs:189-283` (`connect_and_handshake`), `:286-292`
  (`compression_from_u8`), `:326-333` (`reject_is_permanent`)
- **What it does:** A four-message control protocol carried in `FLAG_CONTROL` frames. The listener
  picks the best shared codec (v2 preferred, v1 fallback, otherwise `Reject{NO_COMMON_CODEC}`) and
  the best shared compression (lz4 if offered, otherwise `None`), echoes its own
  `max_frame_bytes`/`window`, and the sink validates that the acked codec is one it offered.
  Reject codes are partitioned into permanent (version/codec/frame-size) and transient
  (internal/going-away/unknown).
- **Why sensitive:** custom (hand-rolled protocol state machine); untrusted-input (this decoder
  runs *before* any peer is trusted — it is the first thing a connecting stranger reaches);
  protocol state machine (a negotiation mismatch means every subsequent frame is
  mis-decoded); duplication (`reject_is_permanent`'s classification decides whether a batch is
  retried, i.e. possibly duplicated, or dropped).
- **Invariants to verify:**
  - A missing field decodes to its zero default with no error (`version: 0`, `codecs: []`,
    `max_frame_bytes: 0`) — confirm every downstream consumer handles those degenerate values.
    In particular `HelloAck.max_frame_bytes == 0` on the sink side yields
    `bound = 0` at `crates/logit-outputs/src/logit.rs:398`, making every batch
    `Fault::Permanent` "too large".
  - An unknown *field* tag is skipped; an unknown *message type* is rejected (`control.rs:361`).
  - `read_choice_list`'s `MAX_CHOICE_LIST_ENTRIES` bound is on the already-`body.len()`-bounded
    field, so no allocation precedes it.
  - The listener's chosen codec is the one enforced per-frame afterwards
    (`crates/logit-inputs/src/logit.rs:547-554`) and the sink frames under exactly the acked codec
    (`crates/logit-outputs/src/logit.rs:392-414`).
  - `Reject{VERSION_MISMATCH|NO_COMMON_CODEC|FRAME_TOO_LARGE}` is permanent and everything else
    transient, at the handshake (`Clean`) and after a data frame (`Ambiguous`) respectively.
  - The listener writes `HelloAck` before any data frame can be read, and a data frame arriving
    first is rejected (`logit.rs:663-665`).
- **Observed concerns (unverified):**
  - The sink offers `compressions: vec![Compression::None as u8, self.compression as u8]`
    (`crates/logit-outputs/src/logit.rs:223`) — when `self.compression` is `None` this sends
    `[0, 0]`, a duplicate entry. Harmless, slightly odd. **High confidence, cosmetic.**
  - `compression_from_u8` (`:286`) maps anything other than 0/1 to `None` via `unwrap_or`, so a
    peer acking `Zstd` silently degrades rather than erroring — unlike the codec check three lines
    later, which is strict. Asymmetric handling of the same class of protocol violation.
    **High confidence it's the code's behavior; medium confidence it's intended.**
  - `logit_in` never uses the client's `Hello.max_frame_bytes` to bound its own writes; only the
    listener's own ceiling is enforced. Irrelevant today (control frames are tiny) but the field
    is negotiated and then unused in one direction. **High confidence, low impact.**
  - `Hello.window`/`HelloAck.window` are negotiated and recorded but never honoured — documented
    in `docs/known-gaps.md` as credit-based flow control being unbuilt. **Context, not a finding.**
- **Existing coverage:** `control.rs:376-559` (round trips including empty lists, at/over both
  caps, unknown field tag skipped, wrong message type, dispatch, `FLAG_CONTROL` framing);
  `crates/logit-proto/tests/robustness.rs` (truncation + bit flips over all four messages);
  `crates/logit-inputs/src/logit.rs` tests at `:1284-1404` (v2/v1 negotiation, no-common-codec,
  version mismatch, data-frame-before-hello); `crates/logit-outputs/src/logit.rs` tests at
  `:901-1032`, `:1151-1229` (codec negotiation, ack naming an unoffered codec, each reject class).
  [ADR `native-transport-handshake-and-ack`](docs/adr/native-transport-handshake-and-ack.md).
- **Suggested verification approach:** a malicious-peer test matrix driving `LogitInput` with
  hand-built `Hello`s (absent fields, `max_frame_bytes: 0`, 16 codecs, unknown message type,
  a `Reject` where a `Hello` belongs); the mirror against `LogitOutput` with a fake listener
  (`HelloAck` with `max_frame_bytes: 0`, an `Ack` before any frame is sent, a `Hello` in reply to a
  `Hello`).
- **Priority:** P0 — pre-authentication attack surface on a listening socket, and the reject
  classification directly drives retry (duplication) versus drop (loss).

---

### WIRE-06 — `logit_in` per-connection frame loop: eager body allocation, idle bounds, ack-as-backpressure
- **Location:** `crates/logit-inputs/src/logit.rs:446-600` (`serve_connection`), `:611-632`
  (`going_away`/`close_idle`), `:729-749` (`IdleBounds`), `:754-835`
  (`HeaderReadError`/`read_header`), `:839-944` (`FrameReadError`/`read_frame_body`),
  `:949-957` (`write_control`)
- **What it does:** After the handshake, loops: race a header read against shutdown; bound-check
  the declared lengths against `max_frame_bytes`; read the body into a pre-sized buffer with a
  per-`read` stall bound; re-verify through `frame::read_frame_with_header`; decode under the
  negotiated codec; `Fanout::send_relayed`; write `Ack{seq}`; restamp the idle clock. The ack is
  written only after the batch is in every downstream inbox — that delay *is* the backpressure,
  and the idle clock is deliberately stamped at the ack rather than at the read.
- **Why sensitive:** hot-path (per frame, per batch); custom; untrusted-input (`compressed_len`
  sizes an allocation at `:907` before any body byte has arrived); concurrency/cancellation (the
  `select!` at `:495-509` cancels a partially-filled header read); backpressure (the ack point is
  the only flow control); data-loss/duplication (a connection dropped after `Fanout::send` but
  before the `Ack` reaches the peer makes the sender retry an already-delivered batch);
  accounting (`logit.proto.frames`, `logit.proto.frame.bytes`, `logit.proto.errors{reason}`,
  `logit.input.connections.closed{reason=idle}` must reconcile with what actually crossed).
- **Invariants to verify:**
  - `read_frame_body` bound-checks *both* declared lengths against
    `min(max_frame_bytes, MAX_SANE_UNCOMPRESSED_LEN)` before `vec![0u8; compressed_len]` at
    `:907`, and the resulting worst case (`max_frame_bytes × max_connections`) is an intended
    number.
  - Cancelling `read_header` at the shutdown arm can only discard bytes on a connection that is
    then closed — never bytes belonging to a frame that will be re-read.
  - `seq` increments exactly once per forwarded batch and matches what the sender counts; an
    `Ack` is never written for a batch `Fanout::send_relayed` did not accept.
  - The idle clock advances only at `:598` (after the ack), never on a read, so a peer waiting on
    a delayed ack can never be closed as idle.
  - `IdleBounds::first_byte` is absolute and `stall` is per-`read`; a header whose first byte
    lands just before the deadline is read to completion rather than rejected.
  - Every `FrameReadError` variant maps to exactly one `logit.proto.errors{reason}` tag and an
    idle close maps to none (it is `Ok(())` policy, not a fault).
  - A control frame after the handshake closes the connection (`:542-546`).
- **Observed concerns (unverified):**
  - **Eager body allocation before any body byte arrives** (`:907`). With the default
    `max_frame_bytes = 64 MiB` and `max_connections = 1024`, 1024 peers each sending a header
    declaring 64 MiB and then nothing reserve 64 GiB of address space. `vec![0u8; n]` uses
    `alloc_zeroed`, so physical pages are likely lazily faulted, which softens this considerably —
    but it is a virtual-commit cliff on a `vm.overcommit_memory=2` host, and `idle_timeout` is
    **off by default**, so nothing reclaims those connections. **Medium-high confidence this is a
    real slowloris amplifier; the mitigation (reading incrementally into a growing buffer, or
    capping the default `max_frame_bytes`) is cheap.**
  - **The connection-limit `Reject` write is unbounded** — `reject_or_serve`'s `:382-388` calls
    `write_control` (→ `write_all`) with no timeout, on a connection holding *no* permit. An
    arbitrary number of peers that never read can each pin a task and a socket indefinitely.
    In practice a ~40-byte control message fits in the kernel send buffer, so this needs a peer
    that has also shrunk its receive window. **Medium confidence; low practical likelihood, but it
    is the one path with no count bound.**
  - `close_idle` (`:629`) and `going_away` (`:613`) likewise `write_all` unbounded — an idle close
    on a peer that has stopped reading could block the very task the idle timeout exists to
    reclaim. **Medium confidence, same low practical likelihood.**
  - `logit.input.connections` gauge is decremented by a plain statement at `:408`, not a guard —
    a panic in `serve_connection` unwinds past it and the gauge leaks upward permanently.
    **High confidence the code path exists; low confidence a panic is reachable.**
- **Existing coverage:** `crates/logit-inputs/src/logit.rs:1161-2044` — ack-after-inbox
  (`:1435`), oversized-frame-rejected-on-the-header-alone (`:1407`), CRC-corrupt frame + counter
  (`:1464`), shutdown with an idle client (`:1502`), and a full idle-timeout suite (`:1759-2043`:
  going-away + permit release, delayed-ack-is-not-idle, clock restarts per ack, header arriving at
  the deadline, header stalling, body stalling, and no-idle-timeout). ADR
  `native-transport-handshake-and-ack`, [ADR `idle-connection-timeout`](docs/adr/idle-connection-timeout.md),
  `docs/design/wire-protocol.md`.
- **Suggested verification approach:** malicious-peer harness — N connections each sending a
  header declaring `max_frame_bytes` and then nothing, measuring RSS and virtual size; a
  slowloris against the past-the-cap reject path; connection-kill fault injection between
  `Fanout::send` and the `Ack` write to confirm the duplicate is the only outcome (never a loss);
  a targeted review of the `select!`/`borrow()` cancellation reasoning at `:480-509`.
- **Priority:** P0 — network-reachable listener, custom read loop, and the allocation and
  unbounded-write paths are DoS-shaped on the main data path.

---

### WIRE-07 — `logit_in` accept loop: connection cap, bounded TLS accept, live-connection accounting
- **Location:** `crates/logit-inputs/src/logit.rs:244-361` (`Input::bind`/`run`/
  `run_until_shutdown`), `:363-412` (`reject_or_serve`), `:112-126` (`HANDSHAKE_TIMEOUT`,
  `MAX_CONCURRENT_CONNECTIONS`), `:196-239` (`with_tls`/`with_max_frame_bytes`/
  `with_handshake_timeout`/`with_idle_timeout`)
- **What it does:** Binds in a pre-pass (`Input::bind`), then accepts in a loop racing against a
  `watch` shutdown, taking a non-blocking semaphore permit per connection. Unlike `otlp_in`, a
  connection past the cap is still TLS-accepted (under `handshake_timeout`) so the `Reject` goes
  out encrypted rather than in the clear. Each connection is spawned with its own `Fanout` clone,
  telemetry handle, and shutdown receiver.
- **Why sensitive:** concurrency (a spawned task per connection, each holding a `Fanout` clone
  that the cancel-by-drop shutdown depends on releasing); cancellation (the accept `select!` at
  `:284-287` returns `Ok(())` on shutdown, dropping the accept future);
  accounting (`logit.input.connections` gauge and
  `logit.input.connections.rejected{reason=limit}` counter); nontrivial-3p-use(tokio-rustls)
  (a `TlsAcceptor` per connection from one shared `Arc<ServerConfig>`).
- **Invariants to verify:**
  - A connection past the cap can never consume a permit, and the permit is always released when
    the task ends (including on the TLS-failure and timeout arms).
  - Past-the-cap connections are bounded *in rate* by nothing — confirm that is the accepted
    trade-off the ADR names, and that the TLS accept for them is timeout-bounded (`:312`).
  - `AcceptQueueSampler::accept` is cancellation-safe against the shutdown arm (the comment at
    `:274-276` asserts this; the implementation lives in `crate::tcp`).
  - The gauge published at `:394-395`/`:408-409` derives from the atomic's own return value, never
    a separate load, on every path.
  - After `run_until_shutdown` returns, every spawned connection task eventually observes its own
    `conn_shutdown` and drops its `Fanout` clone.
  - `with_max_frame_bytes` clamps to `MAX_SANE_UNCOMPRESSED_LEN` (`:202`) so config can only
    lower, never raise, the frame ceiling.
- **Observed concerns (unverified):**
  - Gauge decrement is not a drop guard (`:408`), so a panic in `serve_connection` leaks the gauge
    upward and the `logit.input.connections` reading drifts permanently. **High confidence in the
    code shape; `otlp_in` has the identical pattern at `crates/logit-inputs/src/otlp.rs:426`/`:507`.**
  - Nothing bounds the *number* of past-the-cap TLS handshakes in flight (acknowledged in the
    module doc at `:40-44` and in the ADR), so the cap bounds served connections but not resource
    use under a TLS flood. **Documented trade-off, restated here as context.**
- **Existing coverage:** `:1162-1208` (bind/idempotent bind/port-in-use), `:1524` (cap rejects past
  the cap), `:1617` (TLS client that sends nothing releases its permit), `:1667` (a TLS listener at
  its cap rejects over TLS, not in the clear). ADR `native-transport-handshake-and-ack`,
  `docs/adr/service-lifecycle-and-output-retry.md`, `docs/plans/operator-surface.md` workstream B.
- **Suggested verification approach:** targeted review plus a fault-injection test that panics
  inside `serve_connection` and asserts the gauge returns to zero (it currently will not);
  a connection-flood test measuring task/socket counts past the cap.
- **Priority:** P1 — the cap and the timeouts are tested; the residual is accounting drift and an
  acknowledged unbounded-reject-path trade-off.

---

### WIRE-08 — `logit_out` send path: one-frame-in-flight, partial-write semantics, fault classification
- **Location:** `crates/logit-outputs/src/logit.rs:78-122` (`Conn`, `LogitOutput` state),
  `:341-343` (`observe_batch`), `:345-539` (`Output::send`), `:414-454` (the `write`-then-
  `write_all` sequence), `:456-534` (telemetry, ack wait, `Ack.seq` check), `:548-550`
  (`duplicate_safe`), `:600-639` (`read_control`)
- **What it does:** One attempt per `send` (retry lives in `logit-pipeline`'s `write_loop`). The
  live connection is `take()`n into a local so a cancelled attempt closes it rather than leaving
  a half-written socket; a reused connection is probed before the first write; the batch is
  encoded once under v1 and re-encoded under v2 only on a v2 connection; the frame is written with
  a single `write` first (to learn whether *anything* left) and `write_all` for the remainder;
  then the `Ack` is awaited under the same timeout and its `seq` must equal the frame just sent.
- **Why sensitive:** hot-path (per batch); custom (hand-rolled ack protocol and fault table);
  cancellation (the whole `send` is raced against `tokio::time::timeout` by the caller — dropping
  it mid-write must not leave `self.stream` reusable); duplication (`duplicate_safe() == false`
  plus an `Ambiguous` classification is exactly the at-least-once window); data-loss (a
  `Fault::Permanent` here drops the batch); accounting (`logit.output.requests{class}`,
  `logit.proto.frames`, `logit.proto.frame.bytes`, `logit.output.reconnects`,
  `logit.output.ack.duration` must reconcile with `write_loop`'s own view).
- **Invariants to verify:**
  - `self.stream` is `None` for the whole duration of an attempt, so a cancelled `send` cannot
    leave a partially-written connection in the pool (test at `:1033` pins this).
  - "Nothing left the host" (`Clean`) versus "at least one byte left" (`Ambiguous`) is *actually*
    true through a `tokio_rustls` stream — a `poll_write` on a TLS stream encrypts into the
    session buffer, so `Ok(n)` does not by itself mean `n` plaintext bytes reached the peer, and
    an `Err` on the first write may still have flushed earlier session bytes. This is the single
    most load-bearing assumption in the module and the one most specific to the TLS wrapper.
  - `conn.seq` and the listener's own `seq` stay in lockstep across every early-return path
    (`seq` is incremented at `:456`, after the write succeeded, before the ack wait).
  - No frame is ever written twice on the same connection with the same `seq`.
  - The `bound = peer_max_frame_bytes.min(MAX_SANE_UNCOMPRESSED_LEN)` check at `:398` keeps the
    connection (does not drop it) and writes nothing.
  - `read_control` bounds both declared lengths before `vec![0u8; compressed_len]` at `:626`, and
    does so on the very first call, before any peer is trusted.
  - `flush()` (`:541-546`) is or isn't needed after a write under TLS — the send path never calls
    it, relying on `write_all` having pushed the record out.
- **Observed concerns (unverified):**
  - **`logit.output.requests` is not counted on several failure paths.** The two "batch too large"
    returns (`:361`, `:410`) and every failure inside `connect_and_handshake` (reached via `?` at
    `:383`/`:387`) return without incrementing the counter, while every post-write path does.
    The counter therefore under-reports failures and its `class` distribution does not reconcile
    with `write_loop`'s attempt count. **High confidence — the code paths are plainly visible.**
  - TLS write semantics (above) — the memory of the syslog-TLS review records `tokio-rustls` write
    semantics as a past source of error, so this deserves a re-derivation rather than a re-read of
    the comment at `:419-421`. **Medium confidence there is a real gap; high confidence it is
    worth re-verifying.**
  - `compression_from_u8(...).unwrap_or(Compression::None)` at `:261` silently accepts a
    nonsense compression byte where the adjacent codec check is strict. **High confidence,
    low impact.**
  - `Ack.seq` mismatch is `Ambiguous` and drops the connection (`:522-534`), but nothing bounds
    how often a peer can force that — a hostile listener that always acks the wrong seq turns
    every batch into a reconnect plus an ambiguous retry, i.e. unbounded duplication downstream.
    **Medium confidence; arguably out of threat model, since the peer is a configured `logit_in`.**
- **Existing coverage:** `crates/logit-outputs/src/logit.rs:641-1438` — real-`LogitInput`
  round trips, connection reuse, the pooled-close probe (`:781`, `:827`), v2/v1 negotiation
  (`:901`, `:957`), unoffered-codec ack (`:1007`), cancelled-send (`:1033`), connect-refused
  (`:1059`), each reject class (`:1151-1229`), ack timeout / never-acks (`:1230`, `:1247`),
  compression downgrade (`:1271`), oversized batch (`:1304`), `read_control` sanity cap (`:1333`),
  insecure-skip-verify warning (`:1388`). ADR `native-transport-handshake-and-ack`,
  ADR `idle-connection-timeout`, `docs/adr/buffered-sink-delivery.md`.
- **Suggested verification approach:** targeted review of the write-classification against
  `tokio_rustls`'s pinned `poll_write`/`poll_flush` source (the same method the module already
  used for hyper); a fault-injection test that kills the peer between the write and the ack under
  *TLS* specifically (today's tests for that path are plaintext); an accounting test that asserts
  `logit.output.requests` totals equal the number of `send` calls.
- **Priority:** P0 — this is the sink side of the only lossless-transit-critical native path, the
  classification decides duplicate-versus-drop, and the TLS write assumption is unverified.

---

### WIRE-09 — Pooled-connection close probe, stream erasure, and SNI derivation
- **Location:** `crates/logit-outputs/src/tls.rs:29-30` (`AsyncStream`), `:39-46` (`host_only`),
  `:49-73` (`PendingClose`), `:99-113` (`poll_pending_close`); callers at
  `crates/logit-outputs/src/logit.rs:364-388` and (out of area, same helper)
  `crates/logit-outputs/src/syslog.rs`
- **What it does:** Before the first write of a `send` on a *reused* connection, polls the stream
  for readability exactly once — never a cancellable `timeout(read)`, because dropping a read
  future on a TLS stream can discard a partially-received record that `tokio_rustls` has already
  taken off the socket. `Pending` means open and provably consumed nothing; EOF, an error, or
  unsolicited bytes all mean "drop this connection".
- **Why sensitive:** custom (a hand-rolled one-shot poll, not any crate's API);
  cancellation (the entire rationale is about what a cancelled read destroys);
  data-loss (a false "open" verdict leads to an `Ambiguous` batch; a false "closed" verdict costs
  only a reconnect); nontrivial-3p-use(tokio-rustls) (the reasoning depends on `tokio_rustls`'s
  internal buffering behavior).
- **Invariants to verify:**
  - `Poll::Pending` genuinely implies nothing was consumed from the TLS session *and* from the
    kernel socket, for `tokio_rustls::client::TlsStream` specifically (rustls may have read a
    partial record into its own buffer and returned `Pending` — confirm that buffer survives,
    which it does since the stream itself is kept).
  - Both consuming verdicts (`Eof`, `Bytes`) unconditionally lead to the connection being dropped
    at every call site.
  - `poll_fn` here creates a waker that is never re-polled; confirm no runtime resource leaks from
    registering interest and then never awaiting.
  - `host_only` derives the right SNI for `host:port`, `[v6]:port`, and a bare host with no port;
    a bare unbracketed IPv6 is documented as the operator's responsibility.
- **Observed concerns (unverified):** none spotted. The `Pending`-not-`timeout(read)` argument at
  `:79-90` is unusually well-reasoned; the residual (a FIN arriving between probe and write) is
  stated explicitly.
- **Existing coverage:** `crates/logit-outputs/src/logit.rs:781` (peer-closed pooled connection
  replaced with no batch lost, driven against a real `LogitInput` with a real `idle_timeout`),
  `:827` (unsolicited `Reject` replaces the connection). ADR `idle-connection-timeout`'s
  "client-side probe" decision.
- **Suggested verification approach:** read the pinned `tokio-rustls` `poll_read` source to confirm
  the `Poll::Pending`-consumes-nothing claim holds for a partially-received TLS record (the one
  third-party behaviour the whole design rests on); add TLS twins of the two existing probe tests,
  which are both plaintext today; a fault-injection test that FINs between the probe and the
  write, asserting the outcome is `Ambiguous` and never a silent loss.
- **Priority:** P1 — small, well-argued, and covered end-to-end against a real listener; the
  correctness claim rests on a third-party crate's internal behavior, which is worth one
  source-level confirmation.

---

### WIRE-10 — Hand-rolled gRPC server framing: length-prefixed messages, trailers, gzip bounds
- **Location:** `crates/logit-inputs/src/otlp.rs:741-816` (`handle_grpc`), `:836-857`
  (`grpc_response`), `:865-886` (`GrpcBody`, the hand-rolled `hyper::body::Body`), `:891-897`
  (`grpc_frame`), `:905-916` (`grpc_unframe`), `:921-941` (`InflateError`/`inflate`),
  `:943-998` (`write_varint`/`export_response`/`export_response_json`), `:608-739`
  (`handle_http`, `request_encoding`), `:203` (`MAX_REQUEST_BYTES`)
- **What it does:** Implements gRPC-over-HTTP/2 by hand on top of `hyper::server::conn::http2`:
  routes on `:path`, checks `grpc-encoding`, reads the body under a 4 MiB `Limited` plus a
  per-frame stall bound, strips the 5-byte `[compressed][len:u32be]` header, optionally inflates
  under the same 4 MiB cap, and answers with a one-data-frame-then-one-trailers-frame body
  carrying `grpc-status`/`grpc-message`. The `Export*ServiceResponse` protobuf is hand-emitted.
- **Why sensitive:** hot-path (every OTLP/gRPC request); custom (the ADR budgets this as half the
  PR); untrusted-input (frame length field, gzip bomb, `Content-Type` dispatch, path routing);
  nontrivial-3p-use(hyper) (a hand-written `Body` impl and trailers, no `tonic`);
  nontrivial-3p-use(flate2) (bounded via `Read::take(cap+1)` rather than trusting the stream);
  data-loss (only the first gRPC frame in a body is decoded — anything after it is dropped
  silently).
- **Invariants to verify:**
  - `inflate` (`:932-941`) truly bounds the decompressed size: `take(MAX + 1)` then
    `len() > MAX` catches an input inflating to exactly `MAX + 1` and never silently truncates.
  - `grpc_unframe`'s `bytes.get(5..5 + len)` (`:915`) cannot overflow (`len` is `u32 as usize`,
    safe on 64-bit; confirm the 32-bit target story is a non-goal).
  - A body carrying more than one gRPC frame is either rejected or documented as
    first-frame-only — today it is silently first-frame-only.
  - Every error path answers HTTP 200 with a gRPC status in trailers (or headers), never a bare
    HTTP error, so a gRPC client sees a status rather than a transport failure.
  - `request_encoding` (`:720-739`) treats absent/empty/non-ASCII `Content-Type` as protobuf, and
    matches case-insensitively with parameters stripped.
  - `export_response`'s hand-written protobuf (`:962-981`) is wire-identical to the generated
    `Export*ServiceResponse` for all three signals.
  - The 4 MiB `Limited` bounds the *compressed* body and `MAX_CONCURRENT_CONNECTIONS` bounds the
    multiplier; the JSON path's real multiple of that is explicitly unmeasured
    (`docs/known-gaps.md`).
- **Observed concerns (unverified):**
  - Multi-frame gRPC request bodies are silently truncated to the first message (`:785`).
    Unary OTLP never sends more than one, so this is correct in practice, but it is an unsignalled
    drop rather than an `INVALID_ARGUMENT`. **High confidence in the behavior, low confidence it
    matters.**
  - `req.headers().get("content-encoding")` matches the exact bytes `b"gzip"`/`b"identity"`
    (`:629-639`) — `Gzip`, `GZIP`, or `gzip, identity` all 415. HTTP content-codings are
    case-insensitive. **Medium-high confidence this is a real (if minor) interop gap, and it is
    inconsistent with `request_encoding`'s deliberate case-insensitivity three lines earlier.**
    Same pattern for `grpc-encoding` at `:762-770`.
  - Error responses on the OTLP/HTTP path are `text/plain` rather than a protobuf `Status` —
    already recorded in `docs/known-gaps.md` as a pre-existing deviation. **Context.**
  - `partial_success` is always empty on success — recorded in `docs/known-gaps.md`. **Context.**
- **Existing coverage:** `crates/logit-inputs/src/otlp.rs:1001-2746` (large in-file suite; covers
  both transports, gzip, size limits, TLS, the idle/stall paths, JSON vs protobuf dispatch).
  [ADR `hand-rolled-grpc-over-hyper`](docs/adr/hand-rolled-grpc-over-hyper.md),
  [ADR `otlp-json-decoding`](docs/adr/otlp-json-decoding.md),
  `docs/adr/otlp-compression-and-decompression-bounds.md`.
- **Suggested verification approach:** interop test against a real gRPC client (`grpcurl`, the
  OpenTelemetry Collector's `otlp` exporter, an OTel SDK) covering trailers-only errors,
  gzip, and `grpc-encoding` casing; malicious-request tests (declared frame length far past the
  body, a gzip bomb, a 5-byte body, a `compressed=1` flag with non-gzip bytes); a fuzz target over
  `grpc_unframe` + `inflate`.
- **Priority:** P0 — hand-rolled protocol framing on a network-reachable listener with
  decompression of untrusted input.

---

### WIRE-11 — Shared hyper connection lifecycle: idle tracking, graceful shutdown, body stall bounds
- **Location:** `crates/logit-inputs/src/http.rs:33-94` (`Activity`), `:102-110` (`InFlight`),
  `:125-211` (`drive_with_idle`), `:216-266` (`BodyReadError`/`collect_with_stall_bound`),
  `:277-288` (`body_read_error_message`); users at `crates/logit-inputs/src/otlp.rs:375-519`
  (accept loop, permit, first-byte `peek`, TLS accept bound) and `:531-606` (`serve_connection`),
  plus `crates/logit-inputs/src/prometheus.rs`'s remote-write receiver
- **What it does:** Tracks idleness at the *service* level (requests in flight + the instant the
  last one finished) rather than around the socket, because hyper's h1 server polls the socket
  mid-message and an IO-level timer would read ordinary backpressure as a silent peer. On the
  deadline it calls `graceful_shutdown`, polls for a bounded grace, waits out any request that
  started inside the window (so a handler parked in `Fanout::send` is never dropped), then drops
  the connection. Request bodies get a separate per-frame stall bound.
- **Why sensitive:** concurrency (an `AtomicUsize` + `Mutex<Instant>` + `Notify` shared between a
  service closure and the driver loop); cancellation (three nested `select!`s over a pinned
  connection future — `drive_with_idle:146-149`, `:161-168`, `:197-203`);
  data-loss (dropping a connection whose handler is inside `Fanout::send` discards a batch that
  never reached the fanout — the explicit reason for the wait-out loop at `:195-207`);
  backpressure (the whole design exists so backpressure never looks like idleness);
  nontrivial-3p-use(hyper/hyper-util) — the close sequence is justified against *pinned* hyper
  1.11.1 / hyper-util 0.1.20 internals (`KA::Idle` vs `KA::Busy`, `ReadVersion` resolving
  `Err("Cancelled")`, h2 `close_pending`), so a dependency bump can silently invalidate it.
- **Invariants to verify:**
  - `in_flight` is incremented when hyper *calls* the service (not when the future is first
    polled) and decremented by `InFlight::drop` on every exit including an unwind
    (`otlp.rs:556`, `http.rs:104-110`).
  - `stamp_progress` happens before the decrement, so a waiting driver never sees
    `in_flight == 0` with a stale `last_progress`.
  - The `notified()` arms cannot miss a notification (`Notify::notify_one` stores a permit, so a
    notification racing the `select!` arm's creation is not lost — verify for all three sites).
  - The wait-out loop at `:196-204` cannot spin forever against a peer that keeps starting new
    requests — the comment argues only "continuing to be served" extends it; confirm a pipelined
    h1 client or an h2 client opening streams cannot hold it open indefinitely.
  - `conn` keeps being polled while waiting on `in_flight` (the deadlock the comment at `:120-124`
    names) on both the h1 and h2 arms.
  - The pinned-hyper claims still hold against the version in `Cargo.lock`.
  - `collect_with_stall_bound`'s bound is per-frame, never total, and drops trailers exactly as
    `Collected::to_bytes` does.
- **Observed concerns (unverified):**
  - The wait-out loop (`:177-208`) has no overall ceiling: `grace` restarts each iteration as long
    as `in_flight > 0`. A client that keeps a request in flight indefinitely (a handler blocked on
    a permanently-full downstream) keeps the connection and its permit alive past the idle close.
    That is the intended trade-off (better than losing the batch), but it means `idle_timeout` is
    not an upper bound on connection lifetime. **High confidence in the behavior; it is argued for
    in the comment, so this is a "confirm it's the intended contract" item.**
  - `otlp_in`'s accept loop (`otlp.rs:396-518`) does not race shutdown at all, and connections hold
    `Fanout` clones — already recorded in `docs/known-gaps.md` ("`otlp_in` can hold the graph open
    past shutdown"), narrowed but not closed by `idle_timeout`. **Context, documented.**
  - The `logit.input.connections` gauge decrement at `otlp.rs:507` is a statement, not a guard —
    same panic-leaks-the-gauge shape as `logit_in`. **High confidence in the shape.**
- **Existing coverage:** `crates/logit-inputs/src/otlp.rs:1001-2746`'s idle/stall tests, and the
  equivalents in `crates/logit-inputs/src/prometheus.rs`. ADR `idle-connection-timeout`,
  `otlp_in`'s own module doc (`crates/logit-inputs/src/otlp.rs:72-129`) is the reasoning of record.
- **Suggested verification approach:** targeted review against the pinned hyper sources (re-run the
  original derivation, don't re-read the comment); a soak test opening and abandoning connections
  in each of the three parked states the grace exists for; a test that a handler blocked forever in
  `Fanout::send` does not leak a permit past some bound; slowloris tests (dribbled request head,
  dribbled body) on both h1 and h2.
- **Priority:** P0 — shared by two listeners, mediates loss-versus-leak on shutdown, and its
  correctness is pinned to third-party internals that a routine dependency bump changes.

---

### WIRE-12 — `otlp_out` gRPC round trip over a pooled hyper-util/hyper-rustls client, and the fault table
- **Location:** `crates/logit-outputs/src/otlp.rs:304-379` (`send_http`), `:381-437` (`send_grpc`),
  `:448-457` (`Output::send`, the multi-request loop), `:459-477` (`duplicate_safe`),
  `:487-495` (`default_client_tls_config`), `:504-515` (`build_grpc_client`), `:524-534`
  (`normalize_grpc_endpoint`), `:539-566` (`grpc_status_class`/`grpc_fault`), `:578-653`
  (`grpc_roundtrip`), `:658-691` (`grpc_status_from`/`grpc_frame`/`grpc_unframe`), `:696-701`
  (`gzip`); the hand-rolled protobuf response walker at `:706-721` (`read_varint`), `:733-754`
  (`parse_partial_success`), `:756-784` (`parse_partial_success_message`), `:790-812`
  (`skip_field`); shared client helpers at `crates/logit-outputs/src/http.rs:46-139`
- **What it does:** Issues one request per non-empty signal, sequentially, aborting the rest on the
  first failure. The gRPC transport frames the message by hand but dials through a pooled,
  TLS-capable `hyper_util::client::legacy::Client` over a `hyper-rustls` `HttpsConnector`
  (`http2_only`, `https_or_http`, ALPN filled in by `enable_http2`), reads `grpc-status` from
  response headers first and trailers second, and maps status → `Fault`. Redirects are turned off
  for both transports, and error bodies are read bounded.
- **Why sensitive:** hot-path (per batch, up to 3 requests); custom (gRPC framing, status
  extraction, and a bespoke fault table); duplication (`duplicate_safe() == false`: a mid-batch
  failure retries signals that already landed); data-loss (`Fault::Permanent` drops the batch,
  and every unrecognized gRPC status defaults to `Permanent`);
  nontrivial-3p-use(hyper-util/hyper-rustls) (`http2_only` + `https_or_http` + the
  "`alpn_protocols` must be empty on the way in or `with_tls_config` panics" contract at `:503`);
  accounting (`logit.output.requests{signal,class}`, `logit.output.records.rejected{signal}`).
- **Invariants to verify:**
  - `tls.alpn_protocols` is empty for every `ClientConfig` reaching `build_grpc_client` — both
    `default_client_tls_config` and `crate::tls::build_client_config` must never set it, or the
    process panics at construction.
  - `with_timeout` and `with_tls` compose in any order without losing state (`client` is a pure
    function of `(timeout, tls)`; `headers` is deliberately a separate field for this reason).
  - Header precedence: operator `headers:` are cloned in first and protocol-owned headers
    `insert`ed (replace, not append) after, on both transports (`:312-314`, `:591-609`).
  - `header_status.or(trailer_status)` (`:643`) is the right precedence for a compliant server —
    a Trailers-Only response carries the status in headers, a normal one in trailers; confirm a
    server that (incorrectly) puts `grpc-status: 0` in headers alongside a real error in trailers
    cannot mask the error.
  - The `Fault` table matches the module doc's table exactly, and the "unrecognized status is
    Permanent" default is intended (it drops data rather than retrying it).
  - Redirects stay off (`http.rs:51`), so a `3xx` is `Permanent` and the operator's `headers:`
    never travel to a `Location` host.
  - `read_body_prefix` (`http.rs:78-89`) is a genuinely bounded read, not a bounded message.
  - `normalize_grpc_endpoint` (`:524`) maps `grpc://` and a bare `host:port` to `http://`, and
    cannot produce a URI that silently downgrades an `https://` endpoint.
  - In the hand-rolled protobuf walker: every index advance is checked (`skip_field`'s
    `*pos += 8`/`+= 4` at `:793`/`:807` check `<= bytes.len()` *after* the fact, safe only because
    `pos <= len` holds on entry — confirm that on every call path); `read_varint`'s `shift >= 64`
    bound (`:717`) terminates on any input; group wire types 3/4 abort rather than loop; and every
    `break` path leaves the result at its default rather than a half-parsed value.
- **Observed concerns (unverified):**
  - `send` aborts the remaining signals on the first failure (`:452-454`) and `write_loop` retries
    the whole batch, so a mixed log+metric+trace batch duplicates the signals that already
    succeeded on every retry. Documented in `duplicate_safe`'s own doc comment, but it means a
    3-signal batch has a 3× worse duplication profile than a 1-signal one. **High confidence;
    documented, but worth quantifying.**
  - `grpc_status_from` returns `None` for a non-numeric `grpc-status`, which then falls through to
    the trailers and possibly to "carried no grpc-status" → `Ambiguous` (retry). A server sending
    a malformed status therefore causes retries rather than a permanent failure.
    **Medium confidence it's the right call.**
  - `parse_partial_success` failures are silent by design (`(0, "")`), so a receiver reporting
    rejections in a shape this parser mis-reads under-counts `logit.output.records.rejected`.
    **Medium confidence.**
  - A `rejected` value above `i64::MAX` wraps negative at `:767` and is then silently ignored by
    the `rejected > 0` guard at `:288` — a hostile or buggy server can suppress its own rejection
    reporting. **Medium confidence; trivial impact.**
  - `read_varint` (`:706`) accepts non-canonical varints and drops the 10th byte's high bits, the
    same shape as the native codec's. **High confidence, no impact on a count field.**
  - `prost` is already a dependency; the module doc argues against generating the *collector
    service* types, which is a different question from hand-writing a wire walker for three
    fields. **Observation, not a defect.**
- **Existing coverage:** `crates/logit-outputs/src/otlp.rs:815-1836` (canned HTTP and gRPC servers
  including TLS, the full fault table, partial success, headers, paths, compression).
  [ADR `hand-rolled-grpc-over-hyper`](docs/adr/hand-rolled-grpc-over-hyper.md),
  [ADR `otlp-tls-and-pooled-grpc-client`](docs/adr/otlp-tls-and-pooled-grpc-client.md),
  `docs/adr/buffered-sink-delivery.md`.
- **Suggested verification approach:** interop test against a real OpenTelemetry Collector
  (`otlp` receiver) over both transports, plaintext and TLS, with and without gzip; a
  hostile-server test matrix (status in headers and trailers, malformed status, no status,
  endless error body, a `3xx`); a connection-kill injection mid-request to confirm `Ambiguous`.
- **Priority:** P1 — extensively tested against canned servers and the connection management is
  now a well-known crate; the residual risk is status-precedence and the multi-request duplication
  profile.

---

### WIRE-13 — TLS configuration construction: private CA, mTLS, and `insecure_skip_verify`
- **Location:** `crates/logit-inputs/src/tls.rs:49-90` (`build_server_config`), `:126-175`
  (`apply_client_tls`, the `reqwest` path for `prometheus_in`);
  `crates/logit-outputs/src/tls.rs:145-196` (`build_client_config`), `:204-250`
  (`AcceptAnyServerCert`); callers at `crates/logit-inputs/src/otlp.rs:314-325`,
  `crates/logit-inputs/src/logit.rs:188-195`, `crates/logit-outputs/src/otlp.rs:191-210`,
  `crates/logit-outputs/src/logit.rs:160-173`, and `crates/logit-outputs/src/http.rs:46-56`
- **What it does:** Builds `rustls::ServerConfig`/`ClientConfig` from operator-supplied PEM paths
  resolved against the config file's directory, with optional client-cert verification (mTLS),
  optional private-CA-instead-of-Mozilla-roots, and an `insecure_skip_verify` verifier that skips
  chain and hostname checks but still verifies handshake signatures. `prometheus_in` takes the
  `reqwest`-native route instead, where `ca_file` must also disable the built-in roots to be a
  replacement rather than an addition.
- **Why sensitive:** custom (a hand-written `ServerCertVerifier`); nontrivial-3p-use(rustls) —
  `builder_with_provider(ring)`, `dangerous().with_custom_certificate_verifier`,
  `add_parsable_certificates` (which *silently skips* unparseable certs), and the requirement that
  `alpn_protocols` be empty before `hyper-rustls` fills it in; this is the code that decides
  whether transport security is real.
- **Invariants to verify:**
  - `insecure_skip_verify` always emits the operator warning, on every sink that supports it
    (`otlp_out:199-204`, `logit_out:165-170`, `prometheus_*`).
  - `ca_file` genuinely *replaces* the default roots on both paths: `RootCertStore::empty()` on the
    rustls path (`outputs/tls.rs:167`), `tls_built_in_root_certs(false)` on the reqwest path
    (`inputs/tls.rs:149`). Confirm no third path forgets it.
  - `add_parsable_certificates` (`inputs/tls.rs:80`, `outputs/tls.rs:176`) silently discards
    unparseable entries — a typo'd CA bundle yields an empty trust store, which for the
    *server*-side client verifier means every client is rejected (fail-closed, fine) and for the
    *client* side means every server is rejected (also fail-closed). Verify both are actually
    fail-closed and produce a legible error.
  - `insecure_skip_verify` combined with `ca_file` silently ignores `ca_file`
    (`outputs/tls.rs:162-180`); the comment says graph rule 24 rejects the combination — confirm
    the mirrored rule exists for `logit_out` and `prometheus_*` too.
  - `AcceptAnyServerCert` still verifies TLS 1.2/1.3 handshake signatures and reports
    `supported_verify_schemes` from the same provider.
  - No `ClientConfig` reaching `build_grpc_client` has `alpn_protocols` set (a panic otherwise).
  - There is **no certificate reload** anywhere: a rotated cert requires a process restart. Confirm
    that is intended and documented in `docs/deploying.md`.
  - Paths are resolved against the config file's directory consistently on every field.
- **Observed concerns (unverified):**
  - No cert/key hot reload on any listener or sink. For a long-lived collector with short-lived
    certs (ACME, SPIFFE) this is an operational cliff. I did not find it in
    `docs/known-gaps.md`. **High confidence it is absent; medium confidence it should be a
    recorded gap.**
  - `inputs/tls.rs:151` applies the client identity only when *both* `cert_file` and `key_file`
    are present; one without the other is silently ignored rather than an error (the comment
    defers to graph validation). **Medium confidence a config that sets only one slips through to
    a silently non-mTLS connection.**
  - `apply_client_tls` concatenates cert and key PEMs with a single `\n` (`:163`) — fine for
    well-formed PEM, but a cert file without a trailing newline plus this one produces a valid
    boundary while a file with `\r\n` line endings may not. **Low confidence.**
- **Existing coverage:** `crates/logit-outputs/src/logit.rs:1388`/`:1416` (insecure warning present
  and absent), `crates/logit-inputs/src/logit.rs:1556-1715` (real TLS listener, mTLS-capable test
  fixtures under `testdata/`), the TLS arms of the `otlp_in`/`otlp_out` in-file suites.
  [ADR `otlp-tls-and-pooled-grpc-client`](docs/adr/otlp-tls-and-pooled-grpc-client.md),
  [ADR `syslog-tcp-ingress-and-tls`](docs/adr/syslog-tcp-ingress-and-tls.md).
- **Suggested verification approach:** targeted review plus a fixture matrix — expired cert,
  wrong-hostname cert, CA-not-in-store, client cert required and absent, unparseable CA bundle,
  `cert_file` without `key_file` — asserting each fails closed with a legible error; a review of
  whether cert reload belongs on the roadmap.
- **Priority:** P1 — a mistake here is a silent security downgrade rather than data loss, and the
  fail-closed behavior of `add_parsable_certificates` plus the missing reload story are both
  unverified.

---

<!-- The seven entries below come from a dedicated pass over the two Prometheus transport files;
     a sample of their line references was independently re-verified. -->

### WIRE-14 — `prometheus_in` scrape loop: per-tick fan-out, per-target body cap, outcome bookkeeping
- **Location:** `crates/logit-inputs/src/prometheus.rs:347` (`MAX_SCRAPE_BYTES` = 32 MiB),
  `:385-441` (`instance_of`, `redact_url`, `build_resource`), `:462-530`
  (`status_class`, `ScrapeStatus`, `scrape_target`), `:652-763` (`PrometheusInput::tick`),
  `:768-787` (`Input::run`)
- **What it does:** `run` drives a `tokio::time::interval` with `MissedTickBehavior::Delay`,
  swallows the first tick, and calls `tick`. `tick` spawns one `JoinSet` task per target (all
  targets concurrently, **no cap**), collects `(idx, ScrapeStatus, elapsed)` into a pre-sized
  `outcomes` vec, then sequentially classifies each into a `logit.input.scrapes{class}` count and
  one `EventBatch` per target with three synthetic series (`up`, `scrape_duration_seconds`,
  `scrape_samples_scraped`) appended. Bodies are read incrementally via
  `reqwest::Response::chunk` and aborted at 32 MiB.
- **Why sensitive:** hot-path (one whole `/metrics` page decoded per target per interval);
  untrusted-input (a hostile exporter controls body size, `Content-Type`, and content);
  concurrency (unbounded per-tick fan-out, results reassembled by index); accounting
  (`scrapes{class}`, `samples`, and the synthetic `up`/`scrape_samples_scraped` triple must agree
  per target per tick); custom (hand-rolled redaction of credentials out of the target URL before
  it becomes a resource attribute that reaches every sink).
- **Invariants to verify:**
  - Exactly one `scrapes{class}` count, one `scrape.duration` timing, and one batch with exactly
    three synthetic series per target per tick, on every path including `JoinError`
    (`:686`, `unwrap_or((NetworkError, ZERO))`).
  - `outcomes[idx]` can never be out of range or cross-assign two targets' results (`:673-683`).
  - `redact_url` never leaks `user:pass@` or a `?token=` query into `ATTR_TARGET` or into a
    `scrape_failed` diagnostic, and two unparseable targets never collapse onto one identity
    (`:420-434`).
  - `up == 1.0` iff the body both arrived and parsed; `scrape_samples_scraped == samples` counted
    at `:751`.
  - `MissedTickBehavior::Delay` genuinely holds under a `sink.send` stall (a stalled tick must not
    become N back-to-back rounds).
- **Observed concerns (unverified):**
  - **Peak memory scales with target count, not with the cap.** All targets are scraped
    concurrently and every body is retained in `outcomes` until the join loop finishes
    (`:673-683`), so the worst case is `targets × 32 MiB` resident at once, not 32 MiB. Neither
    `MAX_SCRAPE_BYTES`' own doc (`:342-347`) nor the module doc bounds the fan-out, and there is
    no per-tick concurrency limit. A 50-target config against misbehaving exporters is a 1.6 GiB
    worst case. **Medium confidence.**
  - **Sends are serialized after every scrape completes:** no batch reaches the fanout until the
    slowest target has finished or timed out, so one slow target delays all targets' batches by up
    to `timeout`, and a full downstream then blocks the whole round at `:761`. **High confidence;
    by design, but worth stating.**
  - The scrape client follows redirects — `reqwest::Client::new()` at `:564` keeps `reqwest`'s
    `limited(10)` default, unlike `crate::http::build_client` on the sink side. **Documented in
    `docs/known-gaps.md` and ADR `prometheus-remote-write`'s Consequences; context, not new.**
  - `logit.input.samples` counts *series* here and *wire samples* in bind mode — a documented
    gap ([`docs/known-gaps.md`](../known-gaps.md#prometheus)'s "`logit.input.samples` means two
    different things depending on `prometheus_in`'s mode"). **Context.**
- **Existing coverage:** in-file `crates/logit-inputs/src/prometheus.rs:1909-2384` (dialect
  selection, 4xx/5xx classes, timeout, oversize, refused connection, resource identity, URL
  redaction, `Accept` header, two targets in one tick, telemetry classes, TLS trusted/untrusted
  CA). Integration: `crates/logit-cli/tests/prometheus_round_trip.rs`.
  [ADR `prometheus-scrape-and-exposition`](docs/adr/prometheus-scrape-and-exposition.md).
- **Suggested verification approach:** N canned servers each serving just under 32 MiB, asserting
  peak RSS (or simply adding and testing a `JoinSet` concurrency bound); a one-hangs-one-answers
  test asserting when the fast target's batch reaches the fanout; a proptest over
  `redact_url`/`instance_of` (including `http://[::1/metrics`, `http://h:99999/`, IDN hosts)
  asserting no credential substring ever survives.
- **Priority:** P1 — the redaction path is credential-bearing and the unbounded fan-out is a real
  memory-bound gap, but nothing here is a happy-path correctness bug.

---

### WIRE-15 — `prometheus_in` remote-write receiver ingress: permits, deadlines, body limits, snappy bounds, version dispatch
- **Location:** `crates/logit-inputs/src/prometheus.rs:800-816` (`MAX_REQUEST_BYTES` = 4 MiB,
  `MAX_CONCURRENT_CONNECTIONS` = 1024, `HANDSHAKE_TIMEOUT`), `:1229-1372` (`Input::bind`/`run`),
  `:1378-1441` (`serve_write_connection`), `:1448-1477` (`handle_write`), `:1482-1615`
  (`write_response`: routing, body collection, snappy bounds), `:1696-1746` (`header_str`,
  `no_content`, `text_response`, `with_written_headers`); shared
  `crates/logit-inputs/src/http.rs:33-110`, `:125-211`, `:231-266`
- **What it does:** `bind` opens the `TcpListener` idempotently; `run` accepts, samples
  accept-queue gauges (`crate::tcp::AcceptQueueSampler`), takes a **non-blocking**
  `try_acquire_owned` permit (rejecting past 1024 rather than queueing), then spawns a task that
  bounds the TLS accept or the plaintext first-byte peek at 5s and serves the connection with
  `hyper_util`'s auto builder (HTTP/1.1 **and h2c**). Per request: path → method →
  `Content-Encoding: snappy` → `Content-Type` → version, then `Limited::new(body, 4 MiB)` collected
  with an optional per-frame stall bound, `snap::raw::decompress_len` checked against 4 MiB
  *before* expansion, then `decompress_vec`, decode, `sink.send`, `204`.
- **Why sensitive:** untrusted-input (an unauthenticated peer drives compression ratio, frame
  cadence, header set, stream count); concurrency + cancellation (permits, `drive_with_idle`'s
  select loop, the grace window that deliberately waits out an in-flight `Fanout::send`);
  backpressure (the `204` is issued *after* `sink.send`, so channel pressure becomes sender-side
  flow control); protocol state machine (the route/status table and 2.0's `-Written` contract);
  nontrivial-3p-use(hyper/hyper-util auto, tokio-rustls, snap, http-body-util).
- **Invariants to verify:**
  - The decompressed body can never exceed `MAX_REQUEST_BYTES`: `decompress_len` (`:1583`) is
    authoritative, and a block whose declared length lies about what it actually expands to must
    be a `snap` error rather than a larger allocation at `:1605`.
  - A connection's permit is released on every exit: rejected (`:1279` drop), handshake timeout,
    TLS failure, clean pre-first-byte close (`:1337`), normal exit, panic unwinding through the
    spawned task.
  - `logit.input.connections` (add `:1297`, sub `:1361`) reconciles to 0 when the listener is
    quiet.
  - Exactly one `logit.input.writes{class}` plus one `write.duration` per request, across all
    seven exits (`handle_write` wraps `write_response`, `:1460-1476`).
  - `with_written_headers` emits the three `-Written` headers on 2.0 only, on both 2xx and 4xx,
    and never on a rejection that happened before the version was known (`seen = None`,
    `:1494-1544`).
  - `activity.request_close()` on a stalled body (`:1563`) actually closes the connection after
    the `408`, and only where `idle_timeout:` is configured.
- **Observed concerns (unverified):**
  - **The connection cap does not bound concurrent requests, because h2c is served.** The module
    doc (`:248-257`) and `MAX_CONCURRENT_CONNECTIONS`' own doc (`:802-806`) assert that the
    per-request cap bounds the listener's whole worst case. But `auto::Builder` (`:1430`) serves
    h2c and **no `http2_max_concurrent_streams` is set anywhere in `logit-inputs`** (grepped);
    RFC 7540's default is unlimited. One h2c connection can therefore hold many concurrent
    streams, each with up to 4 MiB compressed + 4 MiB decompressed + a decoded `Vec<Event>`, so
    the worst case is `1024 × streams × ~8 MiB`, not `1024 × 8 MiB`. **Medium-high confidence —
    this is the most consequential finding in the Prometheus half.**
  - **The accept loop terminates the input on any `accept()` error** (`:1266`), where
    `prometheus_out`'s own loop backs off and continues
    (`crates/logit-outputs/src/prometheus.rs:1117-1134`). This is the *consistent* pattern across
    `logit-inputs` (`otlp.rs:397`, `logit.rs:285`, `tcp.rs:1106`), so it is cross-cutting, but an
    `EMFILE` burst kills the listener for the process's life while the sibling sink survives it.
  - A default `bind:` (no `idle_timeout:`) has no bound at all on a half-uploaded request,
    holding a permit until the peer goes away. **Documented deliberate gap; context.**
  - `live_connections` is a manual `fetch_add`/`fetch_sub` pair (`:1297`, `:1361`) rather than a
    drop guard like `InFlight`, so a panic unwinding out of `serve_write_connection` leaks the
    gauge permanently. **Low confidence it's reachable; same shape as `logit_in`/`otlp_in`.**
- **Existing coverage:** in-file `:2529-3307` — 204 on 1.0 and 2.0 with `-Written`, 404/405+Allow,
  415 for missing/other `Content-Encoding` and missing/unrecognised `Content-Type`, 413 for an
  over-cap `decompress_len`, 400 for malformed snappy and for a body that isn't the promised
  message, multi-timestamp ordering, empty resource,
  `backpressure_delays_the_204_until_the_channel_drains` (`:2876`),
  `a_body_that_stops_arriving_is_408_and_closes_the_connection` (`:3103`), TLS (`:3181`), idle
  keep-alive close releasing its permit (`:3227`), silent-connection handshake timeout (`:3287`).
  Integration: `crates/logit-cli/tests/prometheus_remote_write_round_trip.rs`,
  `crates/logit-proto/tests/prometheus_remote_write_interop.rs`.
  [ADR `prometheus-remote-write`](docs/adr/prometheus-remote-write.md),
  [ADR `idle-connection-timeout`](docs/adr/idle-connection-timeout.md).
- **Suggested verification approach:** drive the receiver over h2c (a `hyper` client with
  `http2_only`) opening ~100 concurrent streams each posting a near-4 MiB body, and watch RSS —
  then decide whether to set `http2_max_concurrent_streams` on the auto builder. Separately, a
  fuzz/proptest corpus of snappy frames (truncated, declared length lying about the block,
  zero-length, maximally-ratio'd) through `write_response`, asserting status and that no
  allocation exceeds the cap.
- **Priority:** P0 — the only unauthenticated, network-reachable parser on the pair, and the
  documented DoS bound has a plausible hole in it.

---

### WIRE-16 — `prometheus_in` metadata cache: one blocking mutex, an `Arc` seed, an expiry watermark, LRU cap
- **Location:** `crates/logit-inputs/src/prometheus.rs:824-840` (counter names,
  `MAX_METADATA_TEXT_BYTES`), `:848-857` (`CachedFamily`), `:865-924` (`MetadataCache`,
  `CacheState`, `recompute_expiry`, `note_expiry`, `rebuild_seed`), `:928-940` (`bounded_text`),
  `:962-968` (`seed`), `:972-988` (`sweep`), `:997-1070` (`learn`), `:1081-1102` (`enforce_cap`),
  `:1107-1109` (`lock`); call sites `:1630`, `:1649-1651`
- **What it does:** A per-component `family name -> (type, help, unit, last_seen)` table behind a
  `std::sync::Mutex`, shared by every peer. Each request takes the lock once to clone an
  `Arc<Declarations>` seed (sweeping expired entries first, but only if the `next_expiry`
  watermark has passed), decodes *without* the lock, then takes it again — only if the request
  declared something — to fold in declarations, count retypes, truncate over-long help/unit to
  1 KiB, evict down to `max_families` least-recently-seen (ties by name, one
  `select_nth_unstable` pass), and rebuild the seed only when contents actually changed.
  `max_families: 0` means no table at all and the stateless `remote_write::decode` path.
- **Why sensitive:** concurrency (a blocking mutex on a Tokio worker, taken on every request of a
  concurrently-served connection; the design rests on the held section being O(1) unless something
  really changed); accounting (four counters and a gauge that must reconcile with the table);
  untrusted-input (any peer that can POST writes into the shared table, and help/unit strings
  outlive the request); data-loss-adjacent (an expiry or a cardinality eviction silently flattens
  a family's model kind); custom (hand-rolled watermark, hand-rolled one-pass LRU).
- **Invariants to verify:**
  - **`next_expiry` is always a lower bound, never an over-estimate** — the whole correctness of
    skipping the sweep. `note_expiry` (`:907-913`) only lowers; `recompute_expiry` (`:900-903`)
    recomputes exactly. Verify no path raises it.
  - **`Decoded::declarations` contains only what *this request* carried, never the seed echoed
    back** — otherwise every seeded family's `last_seen` refreshes on every request and nothing
    ever expires. True today
    (`crates/logit-proto/src/prometheus/remote_write.rs:642-740`, `:860-970` build `declarations`
    fresh and pass `seed` separately), but it is a **cross-crate invariant with no test pinning
    it**.
  - `sweep`'s staleness test (`> ttl`, `:978`) and `seed`'s watermark test (`now > earliest`,
    `:964`) must not disagree at the boundary.
  - The lock is never held across an `.await` (`:1630`, `:1649` are the only call sites, both
    fully synchronous).
  - `metadata_cache.size` equals `families.len()` after every transition; the gauge is only
    published when the table changed (`:986`, `:1068`), so no mutating path may leave it stale.
  - `enforce_cap`'s `excess - 1` index is always valid (`total > max_families >= 1`, `:1086-1090`).
- **Observed concerns (unverified):**
  - **A very large `ttl` silently disables expiry for an entry:** `recompute_expiry` uses
    `filter_map(|f| f.last_seen.checked_add(ttl))` (`:902`) and `note_expiry` returns early on
    overflow (`:908`), so an overflowing `Instant + ttl` drops that entry out of the watermark and
    it never expires while `next_expiry` reads `None` over a non-empty table. Only reachable with
    an absurd (but rule-55-legal, non-zero) `ttl`. **Low-medium confidence.**
  - **`metadata_cache.truncated` counts per re-declaration, not per remembered truncation:**
    accumulated at `:1017-1019` before the `get_mut` match, so a sender re-declaring the same
    over-long `# HELP` every minute increments it forever even though nothing new is stored — it
    will read as an ongoing fault. **Medium confidence.**
  - Any peer can evict every other peer's declarations — one table, no attribution,
    `evicted{reason="cardinality"}` unattributed. **Documented in the module doc (`:220-229`) and
    known-gaps; context.**
  - A poisoned lock is recovered (`:1107-1109`) on the argument that nothing inside can panic;
    `bounded_text`'s `while !is_char_boundary(end) { end -= 1 }` (`:936-938`) is the one arithmetic
    loop under it and terminates because index 0 is always a boundary. **Fine today, worth keeping
    true.**
- **Existing coverage:** in-file `:3408-3977` — cross-request typing, disabled cache takes the
  stateless path, request declaration overrides cache, 2.0 fills the cache for a later 1.0 sender,
  TTL expiry, cardinality cap with single-pass tie-break-by-name, re-declaration not counted as a
  replacement, `UNKNOWN` cannot replace a cached type, description bounding on a char boundary,
  seed not rebuilt on an identical re-declaration, the entry's own `Arc` preserved, and
  `a_request_before_the_watermark_does_not_sweep` (`:3953`, via the `#[cfg(test)] sweeps` counter
  at `:874-875`). ADR `prometheus-remote-write` ("The receiver is stateless; 1.0 typing waits for
  a bounded metadata cache"); [`docs/known-gaps.md`](../known-gaps.md#prometheus).
- **Suggested verification approach:** a loom or plain-threads stress test hammering `seed`/`learn`
  from N threads with overlapping and disjoint family sets, asserting
  `families.len() <= max_families` and gauge agreement at quiescence; a test that seeds a family
  then sends only *sample* requests for longer than the TTL, asserting it does expire (this pins
  the cross-crate "declarations never echo the seed" invariant end to end); a property test that
  `next_expiry <= min(last_seen + ttl)` after every operation sequence.
- **Priority:** P1 — a subtle watermark or `declarations` regression becomes "nothing ever expires"
  (unbounded memory) or "everything expires" (silently untyped families), neither of which fails a
  test today.

---

### WIRE-17 — `prometheus_in` `-Written` / `logit.input.samples` reconciliation
- **Location:** `crates/logit-inputs/src/prometheus.rs:1653-1694` (the counting loop, `written`,
  `sink.send`, `no_content`), `:1702-1746` (`no_content`, `with_written_headers`); callback
  contract at `crates/logit-proto/src/prometheus/mod.rs:550-595` (`families_to_events_with`)
- **What it does:** For each decoded timestamp group, sums `remote_write::wire_samples` over every
  series (`total`), then converts the group to events while a `&mut` callback adds the wire
  samples of any series the model mapping *dropped* (`dropped`).
  `written = total.saturating_sub(dropped)` feeds both `logit.input.samples` and 2.0's
  `X-Prometheus-Remote-Write-Samples-Written`, deliberately the same number.
  `Histograms-Written` is a literal `0`; `Exemplars-Written` is `Decoded::exemplars` verbatim.
- **Why sensitive:** accounting (a header the *sender's* queue manager acts on — Prometheus treats
  a zero against a non-zero send as a failure — plus a counter an operator reconciles against
  `prometheus_out`'s `logit.output.samples`); data-loss visibility (this is the only signal a
  sender gets that its series were dropped); hot-path (two walks over every series of every group,
  per request).
- **Invariants to verify:**
  - `dropped <= total` always — the callback and the counting loop walk the same `decoded.groups`,
    so `saturating_sub` (`:1685`) should never actually saturate; a saturation means the two walks
    disagree and is silently swallowed.
  - `written == 0` ⟺ no batch is sent (`:1687`): a request whose every series was dropped must
    answer `204` with `Samples-Written: 0` and put nothing downstream.
  - `wire_samples` reads the `Point` identically on the `total` and `dropped` sides (so a
    quantiles-only summary is 2, not 4).
  - `logit.input.samples` and the header can never disagree for one request.
- **Observed concerns (unverified):**
  - **`Exemplars-Written` is not subjected to the same drop accounting as `Samples-Written`:**
    `decoded.exemplars` (`:1693`) is the count the *decoder* saw, but exemplars belonging to a
    series the model mapping dropped at `:1678-1680` never reached the fanout. The ADR states
    `Exemplars-Written` *is* `Decoded::exemplars`, so it is deliberate — but it is inconsistent
    with the care taken over samples on the very next line, and a 2.0 sender gets an over-count.
    **Medium confidence.**
  - `Histograms-Written: 0` is honest today but becomes wrong silently the moment native
    histograms land. **Documented in known-gaps; flagged here because it is tied to this function
    rather than to the codec.**
- **Existing coverage:** in-file `:2918-3101` —
  `a_request_whose_only_series_is_dropped_reports_zero_samples_written`,
  `a_kept_histogram_reports_every_wire_sample_it_was_spelled_as`,
  `a_quantiles_only_summary_reports_only_its_quantiles`,
  `a_1_0_created_sample_is_reported_as_a_sample_it_kept`, plus
  `telemetry_counts_writes_by_class_and_samples` (`:3155`). ADR `prometheus-remote-write`.
- **Suggested verification approach:** a proptest generating mixed 1.0/2.0 requests (histograms
  with decreasing bucket counts, empty histograms, summaries in every partial spelling, stale
  markers, exemplar-bearing counters) asserting
  `Samples-Written == logit.input.samples == Σ wire_samples of the series that actually became
  events`, and turning the `saturating_sub` into a debug assert.
- **Priority:** P1 — wrong `-Written` numbers change a real Prometheus sender's retry behaviour,
  and the current tests are example-based rather than exhaustive over the drop matrix.

---

### WIRE-18 — `prometheus_out` exposition registry: upsert, type conflict, expiry sweep, one-pass cap
- **Location:** `crates/logit-outputs/src/prometheus.rs:346-379` (`Clock`, `LabelKey`, `Stored`,
  `StoredFamily`, `Registry`), `:381-524` (`len`, `upsert`, `sweep`, `enforce_cap`, `families`,
  `render`), `:752-776` (`ExposeOutput::send`), `:1104-1106` (`lock`), `:1184-1189` (the
  render-side critical section in `handle`)
- **What it does:** `send` converts the batch under the **encoder** lock, releases it, then takes
  the **registry** lock once for the whole batch: upsert every family (latest wins per rendered
  label set; a type change clears the family's series and counts `type_conflict`), sweep anything
  past `expire_after`, then evict least-recently-updated down to `max_series` in one
  `select_nth_unstable_by` pass. A scrape takes encoder-then-registry in the same order, sweeps
  again, and renders.
- **Why sensitive:** concurrency (two `std::sync::Mutex`es with a stated lock order, taken from
  both an async sink task and an async HTTP handler); data-loss (eviction and type conflict
  silently discard series an operator is watching); hot-path (per-batch upsert, per-scrape full
  deep copy); accounting (`series` gauge, `series.evicted{reason}`, `metrics.type_conflict`).
- **Invariants to verify:**
  - **Lock order is always encoder → registry**, on both paths (`:757-760` releases the encoder
    before `:766`; `:1185-1186` takes both in order). Any future third caller must not invert it.
  - A scrape never observes a registry over `max_series` or holding expired series — the point of
    doing upsert + sweep + cap in one critical section (`:765-773`).
  - `Registry::len()` (`:382-384`) equals what the gauge publishes and what `enforce_cap` measures
    against.
  - `enforce_cap`'s `excess - 1` index validity (`:466-471`), and its tie-break
    (`updated_at`, then family name, then label set) being a pure function of the data.
  - A family left with no series is removed, so a bare `# TYPE`/`# HELP` pair never renders.
- **Observed concerns (unverified):**
  - **An empty family can persist and render as a bare `# TYPE`/`# HELP`.** `upsert` (`:391-396`)
    creates the family entry before iterating `series`, so a `MetricFamily` with an empty `series`
    vec leaves an empty entry; `sweep` only prunes empty families *inside* `if evicted > 0`
    (`:432-435`) and returns immediately when `expire_after` is zero (`:419-421`); `enforce_cap`
    only prunes what it touched. So with `expire_after: 0s` an empty family is permanent. Whether
    `events_to_families` can emit one is the codec surveyor's question; the registry does not
    defend against it. **Medium confidence.**
  - **`Registry::families()` deep-copies the entire registry on every scrape, under both locks**
    (`:490-511`): every label key `Vec<(String, String)>`, every `Point`, every `Exemplar` vec is
    cloned into a fresh `Vec<MetricFamily>` before `text::write_with` runs. At the default 100 000
    -series cap that is a large allocation plus memcpy per scrape, holding the lock `send` needs.
    The module doc acknowledges lock contention but not that a render is O(registry) in
    *allocation* as well as in bytes. **High confidence.**
  - `with_diagnostics`/`with_telemetry` call `rebuild_encoder` (`:688-694`), replacing the `Arc`.
    Called after `bind()`, the running `ServerState` keeps the old encoder and renders stop being
    counted under the component. The doc says every call is a builder running before `bind` —
    true today, unenforced. **High confidence in the shape.**
- **Existing coverage:** in-file `:1577-1832` — resend replaces rather than accumulates,
  `expire_after` eviction + counter, `expire_after: 0` disables expiry, `max_series` LRU eviction +
  counter, `a_batch_far_over_the_cap_evicts_exactly_the_oldest_series_in_one_pass`, type conflict
  replaces the family and evicts its old series, delta-`Sum` skip counted through the sink's
  telemetry, exemplar drop counted at render; plus the byte-exact exposition corpus at
  `:1451-1560`. ADR `prometheus-scrape-and-exposition` ("Exposition state and expiry").
- **Suggested verification approach:** a concurrency test hammering `send` from one task while
  scraping from another (10k series, 50 scrapes), asserting every scrape body parses as valid
  exposition and `len()` never exceeds the cap; a micro-benchmark of `families()` + `render()` at
  100k series to decide whether to render straight from the `BTreeMap`s.
- **Priority:** P1 — correctness is well covered; the cost model (deep copy under a contended
  lock) is the load-bearing risk.

---

### WIRE-19 — `prometheus_out` exposition HTTP server: two deadlines, and synchronous render + gzip on the runtime
- **Location:** `crates/logit-outputs/src/prometheus.rs:276-313` (`MAX_CONCURRENT_CONNECTIONS` =
  16, `HEADER_READ_TIMEOUT` = 5s, `RESPONSE_TIMEOUT_BASE`, `RESPONSE_TIMEOUT_PER_1K_SERIES`,
  `ACCEPT_ERROR_BACKOFF`, `response_timeout`), `:722-748` (`bind`), `:783-814` (`flush`, `Drop`),
  `:1112-1157` (`serve`), `:1159-1209` (`handle`), `:1214-1259` (`header_str`, `negotiate`,
  `contains_ignore_ascii_case`, `accepts_gzip`, `gzip_encode`)
- **What it does:** `bind` opens the socket and spawns `serve`, which accepts, classifies
  `accept()` errors (client accidents retried immediately, anything else after a 100 ms backoff),
  acquires a **blocking** semaphore permit (16 max, deliberately after accept so the kernel backlog
  absorbs bursts), and serves one HTTP/1.1 connection with hyper's `header_read_timeout(5s)` plus
  an outer `tokio::time::timeout(response_timeout)` over the whole connection. `handle` is a
  **synchronous** fn: negotiate dialect from `Accept` (substring match), decide gzip from
  `Accept-Encoding` (honouring `q=0`), sweep + render under both locks, gzip, count
  `scrapes{class}` and `scrape.bytes`, build the response with `Vary: Accept, Accept-Encoding`.
- **Why sensitive:** concurrency (blocking render and gzip on a Tokio worker thread inside a
  `service_fn`); cancellation (the outer connection timeout drops hyper mid-flight); shutdown
  ordering (`flush`/`Drop` abort only the accept task, not in-flight connection tasks);
  accounting (`scrapes{class="ok"}` is counted at render, not at delivery); untrusted-input (two
  small hand-rolled header matchers).
- **Invariants to verify:**
  - Exactly one `scrapes{class}` count per request across all three routes (`:1164`, `:1168`,
    `:1195`).
  - `HEAD` builds the identical body to `GET` so `content-length` agrees (RFC 9110 §9.3.2) —
    `handle` does not branch on method past the allow check.
  - `accepts_gzip` treats `gzip;q=0` as a refusal and `x-gzip`/`gzipped` as non-matches
    (`:1237-1250`).
  - `negotiate`'s substring match cannot be tricked by an `Accept` that *excludes* OpenMetrics
    (e.g. `application/openmetrics-text;q=0`) — it currently would select OpenMetrics; the doc says
    `q` weights "are not a thing any real scraper sends".
  - `flush` and `Drop` both close the port exactly once and are safe in either order and twice
    (`:783-788`, `:808-814`).
- **Observed concerns (unverified):**
  - **The whole-connection deadline cuts keep-alive connections and can land mid-response.**
    `tokio::time::timeout(response_deadline, serve)` at `:1154` bounds the *connection*, not the
    response, and Prometheus reuses connections across scrapes by default. At the default cap that
    is ~40 s, so roughly every third scrape interval the connection is torn down; a scrape that
    starts just before the deadline is cut mid-body, handing the scraper a short read against a
    `Content-Length` it already trusted — the exact failure `RESPONSE_TIMEOUT_BASE`'s own doc
    (`:288-293`) says it exists to prevent — while this sink has already counted
    `scrapes{class="ok"}`. The pattern is inherited verbatim from
    `crates/logit-cli/src/admin.rs:92-94`, where the payload is a lifecycle word and reuse doesn't
    matter. **Medium-high confidence.**
  - **Render and gzip run synchronously on the async worker:**
    `service_fn(move |req| async move { handle(req, state) })` at `:1142-1145` — `handle` is a
    plain fn, so `text::write_with` over up to `max_series` series *and* `gzip_encode` (`:1191`,
    `:1252-1259`) block the runtime thread. With 16 permitted connections, 16 workers can be
    blocked in flate2 at once. No `spawn_blocking` anywhere on this path. **High confidence.**
  - A blocking `acquire_owned().await` at `:1136-1137` means a wedged connection stops the accept
    loop at 16 — intended (backlog as buffer), but combined with the connection deadline being the
    *only* release, a slow scraper set can stall accepts for up to `response_timeout`.
  - `flush`/`Drop` abort only the accept task; in-flight connection tasks keep the registry `Arc`
    alive and keep serving until their own deadline. **Documented at `:778-782`; noted for
    shutdown-ordering completeness.**
- **Existing coverage:** in-file `:1451-1576` (byte-exact text and OpenMetrics, `*/*`, HEAD
  headers-no-body, gzip only when asked and inflating to the same bytes, `gzip;q=0`, 404, custom
  path, 405 naming the allowed methods), `:1833-1951` (bind idempotence, address-in-use message,
  `flush` closes the port, drop-without-flush closes the port, empty 200 before any batch,
  per-class counting with bytes, series gauge), plus
  `accept_negotiation_matches_the_module_docs_table` (`:1953`) and
  `gzip_is_accepted_only_on_a_non_zero_weight_offer` (`:1966`). Integration:
  `crates/logit-cli/tests/prometheus_round_trip.rs`. ADR `prometheus-scrape-and-exposition` is
  **silent on timeouts and concurrency** — these constants are the implementation's own.
- **Suggested verification approach:** a test holding one keep-alive connection open past
  `response_timeout` issuing periodic `GET`s, asserting either that requests keep succeeding or
  that the tear-down never lands inside a response; a load test at 100k series with 16 concurrent
  gzip-requesting scrapers measuring runtime-worker starvation (e.g. an unrelated timer's
  latency), to decide on `spawn_blocking` for render + gzip.
- **Priority:** P1 — a mid-response connection cut against a real Prometheus is an
  operator-visible, intermittent scrape failure that the sink's own telemetry reports as success.

---

### WIRE-20 — `prometheus_out` remote-write sender: timestamp partition, one POST per batch, duplicate safety
- **Location:** `crates/logit-outputs/src/prometheus.rs:823-844` (`RemoteWriteOutput`), `:850-948`
  (builders), `:955-961` (`partition`), `:967-983` (`request_headers`), `:991-997`
  (`new_sender_encoder`), `:1009-1081` (`send`), `:1085-1098` (`flush`, `duplicate_safe`); shared
  `crates/logit-outputs/src/http.rs:46-139`
- **What it does:** Partitions the batch by `Event::timestamp` into a `BTreeMap` (ascending), runs
  `events_to_families` per partition, returns early with **no request at all** if every group is
  empty, encodes one request via `remote_write::encode_counted`, Snappy-block-compresses it, and
  POSTs it once with four protocol headers `insert`ed over a clone of the operator's `headers:`.
  Outcome → `Ok` / `Ambiguous` (429, 5xx, non-connect transport errors) / `Permanent` (3xx and
  other 4xx, with a bounded 256-byte body snippet) / `Clean` (connect failure).
  `duplicate_safe()` is `true`.
- **Why sensitive:** per-batch hot path (partition + convert + encode + compress per delivery);
  data-loss/duplication (the `Fault` classification is what `write_loop` retries or drops on, and
  `duplicate_safe` is what selects `AtLeastOnce` at all); untrusted-input (the receiver's rejection
  body is read into a diagnostic — bounded, but attacker-influenced text);
  nontrivial-3p-use(reqwest with redirects disabled, snap, rustls).
- **Invariants to verify:**
  - The four protocol headers always win over `headers:` (`insert`, not append, and one
    `.headers(..)` at the call site — `:967-983`, `:1042`).
  - An empty or all-skipped batch issues **no** request and counts no `requests{class}`
    (`:1024-1026`).
  - `logit.output.samples` is counted only on success (`:1052`) and is the codec's own count, not
    a guess from family counts.
  - Every outcome produces exactly one `requests{class}` and one `request.duration` — note the
    timer is dropped at `:1047` before the match, so it covers the request and not the
    body-snippet read.
  - `duplicate_safe() == true` must stay true: `false` silently turns every 5xx into a dropped
    batch (`:1089-1098`).
  - `with_timeout` and `with_tls` compose in either order (`:875-879` reads `self.tls`, `:914-933`
    reads `self.request_timeout`).
  - Redirects stay off (`crates/logit-outputs/src/http.rs:46-56`) — following one would carry the
    operator's tenant/auth headers to a `Location` host past rule 56's scheme check.
- **Observed concerns (unverified):** none spotted in the transport logic. Two contextual notes:
  (a) `send` re-encodes a retried batch from the same events, so the `(label set, timestamp)`
  idempotence claim holds only as long as encoding is deterministic — worth an explicit test;
  (b) the sink does no cross-batch reordering, so a fan-in topology can draw out-of-order `400`s —
  a documented known-gaps row, not a finding.
- **Existing coverage:** in-file `:2257-2741` — the four protocol headers, `version: 2` switching
  content-type/version header/message, operator header riding along while a protocol-owned one is
  overridden, snappy block decoding as a `WriteRequest`, multi-timestamp batch → one `TimeSeries`
  with ordered samples, stale-NaN gauge, delta-`Sum` skipped with no request left to send, empty
  batch sends nothing, success counts class + duration + samples, 5xx/429 ambiguous, 400 permanent
  carrying the body, endless rejection body read only as far as the snippet needs, redirect not
  followed and permanent, refused connection clean, three TLS cases, bind/flush no-ops,
  `duplicate_safe` selecting `AtLeastOnce`, illegal header name rejected at construction, enum
  delegation. Integration: `crates/logit-cli/tests/prometheus_remote_write_round_trip.rs`,
  `crates/logit-proto/tests/prometheus_remote_write_fixed_point.rs`,
  `prometheus_remote_write_interop.rs`. ADR `prometheus-remote-write` ("Sender behaviour: one
  request per batch, no retry in the sink").
- **Suggested verification approach:** a determinism test — encode the same `EventBatch` twice and
  assert byte-identical compressed bodies (pins the idempotence premise behind `duplicate_safe`);
  a table-driven test over every status class × transport error asserting the exact `Fault`, so the
  shared `http.rs` table cannot drift out from under this sink.
- **Priority:** P2 — the best-covered mechanism of the seven, with a clean shared fault table and
  no state to corrupt.

---

### WIRE — Cross-cutting notes

**Shared helpers other areas depend on.**
- `crates/logit-proto/src/frame.rs` is not only the network path: `crates/logit-pipeline/src/disk_queue.rs`
  (the disk-backed sink buffer) and `stdio_out`/`file_out`'s `format: native` both read and write
  these frames, and the `Truncated`-vs-`Malformed` classification is what keeps a torn spool file
  resyncable instead of truncated. Whoever surveys `logit-pipeline`'s `DiskQueue` should treat
  `frame.rs` as shared, not as transport-only.
- `crates/logit-inputs/src/http.rs` (`Activity`, `InFlight`, `drive_with_idle`,
  `collect_with_stall_bound`, `body_read_error_message`) is shared by `otlp_in` and
  `prometheus_in`'s remote-write receiver. Its correctness is pinned to hyper 1.11.1 /
  hyper-util 0.1.20 internals by explicit reference; **a dependency bump is a re-verification
  trigger for both listeners.**
- `crates/logit-outputs/src/http.rs` (`build_client`, `status_class`, `is_retryable_http_status`,
  `classify_reqwest_error`, `read_body_prefix`, `body_snippet`, `ERROR_BODY_SNIPPET_BYTES`) is
  shared by `otlp_out` and `prometheus_out` *by design* — the ADRs say the two fault tables are one
  table. `influxdb_out` deliberately keeps its own copy and, per the module doc at
  `crates/logit-outputs/src/http.rs:12-15`, **does not get `build_client`'s redirect-off policy** —
  worth a look from whoever surveys `influxdb_out`.
- `crates/logit-outputs/src/tls.rs`'s `AsyncStream`, `host_only`, and `poll_pending_close` are
  shared by `logit_out` and `syslog_out` (and the probe concept by every pooled sink);
  `crates/logit-inputs/src/tls.rs`'s `build_server_config` by `otlp_in` and `logit_in`, and
  `apply_client_tls` by `prometheus_in`.
- `crate::tcp::AcceptQueueSampler` and `crate::tcp::far_future` (out of this area) are used by
  `logit_in`, `otlp_in`, and `prometheus_in`'s receiver accept loops; the sampler's
  cancellation-safety against a shutdown `select!` is asserted in comments here but implemented
  there (`crates/logit-inputs/src/tcp.rs:772-806`).
- Everything *semantic* under `crates/logit-proto/src/prometheus` (`events_to_families`,
  `families_to_events_with`, `text::write_with`, `remote_write::{decode,decode_with,
  encode_counted,wire_samples,Version,Declarations}`) is the codec surveyor's territory — but two
  correctness contracts straddle the crate boundary and belong to both: the `-Written` accounting
  above, and the metadata cache's "`Decoded::declarations` never echoes the seed" invariant, which
  nothing tests today.

**Things noticed outside this area that another surveyor should pick up.**
1. **`logit-core`'s sketch deserializers are reached from untrusted bytes through this area.**
   `record.rs:455` (`DdSketch::from_java_bytes`) and `:472` (`HyperLogLog::from_bytes`) hand an
   attacker-controlled blob straight into `logit-core::metric`. `docs/known-gaps.md` already records
   that `HyperLogLog::from_bytes` works around an upstream **undefined-behavior** allocation-layout
   bug in `cardinality-estimator` 1.0.3 that is "reachable through ordinary native `METRIC_SET`
   decoding". That makes `logit-core::metric`'s `HllBytesReader` a P0 for the `logit-core` surveyor,
   not a P2.
2. **The process-global interner has no eviction and no budget**, and every decoded dictionary
   entry, trailer string, and OTLP attribute key is interned into it permanently. `docs/known-gaps.md`
   discusses this for `otlp_in`/`json` cardinality; I did not find the native-path case named.
   Whoever surveys `logit-core::interner` should treat "a remote peer can grow it without bound"
   as in-scope.
3. **A repeated, mechanical pattern: telemetry gauges decremented by a bare statement rather than a
   drop guard** (`crates/logit-inputs/src/logit.rs:408`, `crates/logit-inputs/src/otlp.rs:507`, and
   per the Prometheus pass likely the receiver too). A panic in a connection task leaks the
   `logit.input.connections` gauge upward permanently. Cheap to fix once, in `crate::tcp`, for all
   of them.
4. **`logit.output.requests` under-counts on `logit_out`'s pre-write failure paths**
   (`crates/logit-outputs/src/logit.rs:361`, `:410`, and the `?` at `:383`/`:387`), so the
   sink-side attempt accounting does not reconcile with `logit-pipeline`'s `write_loop` view.
   Worth checking whether the other sinks have the same gap.
5. **No TLS certificate reload anywhere** — every `ServerConfig`/`ClientConfig` is built once at
   construction from files on disk. Not in `docs/known-gaps.md` as far as I could find; an operator
   surveying `docs/deploying.md` should decide whether that belongs there.
6. **No HTTP/2 concurrent-stream limit is set anywhere in `logit-inputs`** (grepped: no
   `max_concurrent_streams`). Both `otlp_in` (h2 and h2c) and `prometheus_in`'s remote-write
   receiver (h2c via `auto::Builder`) therefore let one connection carry unbounded concurrent
   streams, which makes each listener's documented "`MAX_CONCURRENT_CONNECTIONS ×
   MAX_REQUEST_BYTES`" worst case an under-estimate by a factor of the stream count. This is one
   fix in two places and is the single highest-value item in this whole survey.
7. **Every `logit-inputs` accept loop propagates an `accept()` error out of `run`**
   (`crates/logit-inputs/src/logit.rs:285`, `otlp.rs:397`, `prometheus.rs:1266`, `tcp.rs:1106`),
   ending the input for the process's life on an `EMFILE` burst — while `prometheus_out`'s own
   exposition accept loop (`crates/logit-outputs/src/prometheus.rs:1117-1134`) classifies and
   backs off instead. A cross-cutting lifecycle decision worth making once, deliberately, rather
   than five times by default.
8. **`prometheus_in`'s scrape client is the one HTTP client in the workspace that still follows
   redirects** (`reqwest::Client::new()` at `crates/logit-inputs/src/prometheus.rs:564`), unlike
   `crate::http::build_client` on the sink side. Already documented, listed here so the
   inconsistency is visible next to the sink-side policy.


---

## CODEC — Hand-rolled protocol codecs

Scope: statsd, syslog, collectd, graphite, prometheus, otlp codecs (decoders of untrusted network
input + mirror encoders), influxdb line-protocol encoder, msgbuf/lib.rs codec traits. Listener/
socket/driver glue and the native wire format are out of scope (other surveys cover them).

---

### CODEC-01 — Hand-rolled restricted pickle stack-machine reader (opcode allowlist)
- **Location:** `crates/logit-proto/src/graphite/pickle.rs:258-542` (`PickleReader::parse`, `PickleReader::read_datapoints`), opcode table at `pickle.rs:76-119`
- **What it does:** A from-scratch reader for a ~24-opcode allowlisted subset of Python's pickle protocols 0/2/4/5, built because carbon's batch wire format is pickle. Every opcode not on the allowlist (`GLOBAL`, `REDUCE`, `BUILD`, `STACK_GLOBAL`, dict/set opcodes, all protocol-0 textual opcodes, etc. — the ones that let a general unpickler construct/call arbitrary objects) is rejected with `CodecError::Malformed`. Strings are UTF-8-validated ranges into the caller's buffer (zero-copy); containers (tuples/lists) are index ranges into reusable arenas, not per-item `Vec`s.
- **Why sensitive:** custom (no crate used — `serde-pickle`/`pickle` crates deliberately rejected because they implement the general, dangerous format), untrusted-input (raw bytes off a carbon TCP/UDP listener), hot-path (per-datapoint, reused `PickleReader` state across frames for zero-alloc warm decode).
- **Invariants to verify:**
  - every opcode not in the allowlist is rejected, including protocol-0/1 textual opcodes and every object-construction opcode (`GLOBAL`/`STACK_GLOBAL`/`REDUCE`/`BUILD`/`INST`/`OBJ`/`NEWOBJ*`/`EXT*`/`PERSID`)
  - every declared length (`BINUNICODE`/`BINUNICODE8`/`BINBYTES8`/`SHORT_BIN*`/`FRAME`) is checked against remaining input *before* anything is sliced or sized from it (`slice()` at pickle.rs:709-714 is the single choke point — confirm no length check anywhere bypasses it)
  - `BINSTRING`'s signed 32-bit length rejects negative rather than sign-extending (pickle.rs:474-481)
  - `LONG1`/`LONG4` magnitude capped at 8 bytes (`MAX_LONG_BYTES`) regardless of declared `n`
  - stack size (`MAX_PICKLE_ITEMS`), open-`MARK` depth (`MAX_PICKLE_DEPTH`), and each arena (`tuples`, `lists`) independently bounded — all four caps checked *before* the corresponding `Vec` grows, not after
  - `datapoint()`'s two-level-deep-only traversal (pickle.rs:320-335) genuinely cannot be driven deeper by any accepted opcode sequence — no recursion exists anywhere in `parse`'s opcode loop; the only nesting mechanism is the stack + `APPEND`/`APPENDS`/`TUPLE*` opcodes, none of which recurse
  - final-state check: exactly one stack value at `STOP`, and it must be a `List`
  - a UTF-8 validation failure or wrong-shaped item costs only that datapoint (not the frame), but a top-level opcode/bounds violation fails the whole frame (documented dichotomy, pickle.rs:280-286) — confirm it's actually implemented that way
- **Observed concerns (unverified):** none spotted — the code is unusually well-argued in its own doc comments (module doc pickle.rs:1-71 walks through why each bound exists) and cross-references its own test/robustness coverage. Worth a second look under fuzzing: `append_range` (pickle.rs:650-665) requires the target list to be the arena's *tail* (`start + len == lists.len()`) and rejects otherwise. Low confidence this can be violated by any legal opcode sequence, but it's exactly the kind of state-machine invariant that's easy to get subtly wrong and hard to review by eye — if violated, the failure mode would be silent corruption of an unrelated in-flight list's slice bounds rather than a clean reject.
- **Correction to the survey brief:** the brief states pickle.rs "contains `unsafe`". It does not — `grep -rn "unsafe"` across the whole graphite codec (decode.rs/encode.rs/pickle.rs/outputs graphite.rs) returns exactly one hit, a comment at pickle.rs:339-340 explaining why `unsafe` was deliberately *avoided* ("re-validated here rather than carrying an unsafe 'trust me' across the two"). `git log` on pickle.rs shows only two commits ever (initial add, a memo-bound fix); no unsafe was introduced or removed. Treat "no unsafe in graphite" as verified, not assumed.
- **Existing coverage:** Unusually strong. Unit tests in `pickle.rs:738-1335+` use real CPython-generated fixtures (protocol 2, protocol 5/-1, memoized paths, LONG1 timestamps past 2038, numeric-string coercion, wrong-shaped items, explicit rejection tests for `GLOBAL`/`STACK_GLOBAL`/dict/set/`REDUCE`/`BUILD`/protocol-0). `crates/logit-proto/tests/robustness.rs:525-660+` explicitly calls this "the highest-risk parser in the repo" and runs single-byte-truncation-survival, seeded bit-flip fuzzing, inflated-string-length, 64-bit-length, and nesting-depth-cap tests with a peak-allocation counter. `crates/logit-proto/tests/graphite_fixed_point.rs` proptests round-trip (`plaintext_round_trips_every_generated_batch`, `pickle_round_trips_every_generated_batch`, ~lines 307-330). ADR `graphite-carbon-relay.md`.
- **Suggested verification approach:** port the existing bit-flip/truncation robustness tests to a real `cargo-fuzz` target run for CPU-hours instead of a fixed seed list, targeting `append_range`'s tail invariant and the memo ordinal-key check specifically. A differential test against real CPython `pickle.loads`/`dumps` (already informally done for fixture provenance) formalized as a corpus-based interop test would catch accepted-opcode semantic drift.
- **Priority:** P1 — untrusted network input into fully custom parsing logic, but exceptionally well-bounded and already fuzz-adjacent-tested; no unsafe, no found P0.

### CODEC-02 — Historical pickle memo-growth DoS (fixed, regression-sensitive)
- **Location:** `crates/logit-proto/src/graphite/pickle.rs:667-694` (`PickleReader::memo_put`); fix commit `e821381 fix(proto): bound pickle memo growth by opcodes consumed`
- **What it does:** `BINPUT`/`LONG_BINPUT`/`MEMOIZE` write into `self.memo: Vec<Option<PValue>>` at an attacker-controlled key. The fix enforces the memo key must be *ordinal* (`key <= self.memo.len()` — only overwrite an existing slot or append the next one) rather than trusting the declared key to size/resize the `Vec`.
- **Why sensitive:** untrusted-input, custom, accounting (the cap that bounds memory is itself the property under test) — a single-value-derived-allocation DoS class.
- **Invariants to verify:**
  - a `LONG_BINPUT`/`BINPUT` key greater than `self.memo.len()` is rejected before any `Vec` growth (pickle.rs:678-683)
  - `MEMOIZE`'s self-assigned key (`self.memo.len()`) can never itself trigger the ordinal-skip rejection
  - the memo, `tuples` arena, `lists` arena, and `stack` are all independently capped at `MAX_PICKLE_ITEMS`, so satisfying one cap never lets another grow unbounded
- **Observed concerns (unverified):** none spotted — regression test at pickle.rs:1059-1071 (`a_memo_key_that_skips_ahead_is_rejected`) reproduces the exact 9-byte frame from the original finding (`PROTO 2, EMPTY_LIST, LONG_BINPUT 499999, STOP`, which previously resized the memo to ~8MB before failing or not failing at all), cross-referenced by a named peak-allocation robustness test (`graphite_pickle_never_allocates_from_a_hostile_memo_key`, per the code comment — not independently confirmed by reading the test body in this pass).
- **Existing coverage:** `pickle.rs:1066` unit test; `crates/logit-proto/tests/robustness.rs`'s `graphite_pickle_never_allocates_from_a_hostile_memo_key` (name/location per code comment, not independently re-read).
- **Suggested verification approach:** confirm the robustness test asserts an actual byte-level peak-allocation bound (not just "it errors"), and that this fix's ordinal-key invariant is included in the fuzz corpus above rather than only the one hand-written fixture.
- **Priority:** P1 — already fixed and tested, but memo-key handling is exactly the class of subtle wire-driven-allocation bug worth a dedicated regression/fuzz check to keep fixed.

### CODEC-03 — Carbon plaintext/pickle decode entry point and timestamp arithmetic
- **Location:** `crates/logit-proto/src/graphite/decode.rs:147-297` (`Decoder::decode_into`, `decode_plaintext`, `decode_line`, `decode_pickle`, `push_datapoint`); timestamp handling at `decode.rs:342-381` (`parse_timestamp`, `resolve_timestamp`)
- **What it does:** Entry point for both carbon wire protocols into `Event`s. Plaintext: splits a datagram on `\n`, strips `\r`, validates UTF-8 per-line (non-UTF-8 line skipped, not fatal), splits into exactly 3 whitespace-separated fields. Pickle: delegates to `PickleReader` (above), treats any error as whole-frame-fatal (no resync point in a pickle stream). `resolve_timestamp` widens seconds (`f64`, since carbon allows fractional seconds and a `-1` "now" sentinel) to nanoseconds using **split arithmetic** (whole seconds and sub-second remainder scaled separately) to avoid `f64` precision loss past 2^53 that a naive `seconds * 1e9` would introduce.
- **Why sensitive:** untrusted-input (raw UDP/TCP bytes, non-UTF-8 possible), hot-path (per-line/per-datapoint), lossless-roundtrip (ADR `lossless-transit`'s carbon leg; a timestamp precision bug would silently drift `graphite_in -> graphite_out` fixed points), custom (float-splitting timestamp conversion mirrors `collectd`'s `cdtime_to_nanos` — a repeated hand-written pattern, not a library).
- **Invariants to verify:**
  - a non-UTF-8 byte sequence in one `\n`-delimited line costs only that line, not the datagram (decode.rs:187-190)
  - `-1.0` exactly is carbon's receipt-time sentinel — the float-equality comparison (decode.rs:365) is intentional (this is a "float edge case" per the survey brief) and matches carbon's own bit-exact sentinel check
  - non-finite (`NaN`/`inf`) or non-positive timestamps are rejected, not silently clamped/wrapped (decode.rs:368-377)
  - whole-seconds/sub-nanos split arithmetic (decode.rs:378-380) actually avoids precision loss versus a naive one-step multiply across realistic timestamp ranges (2020s-2100s)
  - both final casts (`whole as i64`, `sub_nanos as i64`) saturate rather than wrap for a timestamp near/past `i64`'s range (documented as intentional/Rust `as` semantics; confirm no wrapping path was reintroduced)
  - non-finite metric *values* are rejected symmetrically in `push_datapoint` (decode.rs:274-283)
- **Observed concerns (unverified):** none spotted — behavior is deliberate and cross-referenced against carbon's real `MetricLineReceiver`/`TaggedSeries.parse` semantics in comments (e.g. `parse_tags`, decode.rs:299-340, reproduces "last value wins" for duplicate tag keys to match CPython's `dict`-based parser). Low-confidence note: float-equality sentinel comparisons are a classic review flag; here it's textually justified, so likely fine but worth a verifier's explicit sign-off.
- **Existing coverage:** `decode.rs` has a `#[cfg(test)] mod tests` (not read in full this pass). `crates/logit-proto/tests/robustness.rs:562-573` (plaintext byte-truncation/bit-flip survival). `crates/logit-proto/tests/graphite_fixed_point.rs` round-trips including a large-timestamp fixed point (~line 163) and proptest generators. ADR `graphite-carbon-relay.md`; [`docs/known-gaps.md`](../known-gaps.md#cross-protocol-mappings)'s `encode (Graphite)` rows document the deliberate lossy cases (multi-value skip, Sum temporality drop, non-finite drop, no metadata channel, 255-byte path limit) — not surprises, accepted normalizations.
- **Suggested verification approach:** targeted code review of the timestamp split-arithmetic against a table of known-tricky `f64` values (sub-second fractions near precision boundaries, values near year-2038/2262 i64-nanosecond overflow); bias the existing proptest generators toward these boundary values.
- **Priority:** P1 — untrusted input parsing with real edge-case density (float sentinels, precision-sensitive arithmetic, lossless-roundtrip claims), heavily tested already.

### CODEC-04 — Carbon plaintext/pickle encoder: tag sanitization, multi-value expansion, frame packing
- **Location:** `crates/logit-proto/src/graphite/encode.rs:350-475` (`encode_record`, `multi`, `expand`); `encode.rs:650-718` (`Sink::emit`/`close_frame`/`payload_len` — pickle frame packing and the 4-byte length-prefix patch); `encode.rs:884-935` (`sanitize_into` and the three `is_forbidden_in_*` predicates); output glue `crates/logit-outputs/src/graphite.rs` (thin transport wrapper, no extra codec logic)
- **What it does:** Renders `Event`/`MetricRecord`s back to carbon plaintext lines or pickle frames. Handles carbon's inability to represent multi-value metric kinds (`Samples`/`Distribution`/`Histogram`/`ExponentialHistogram`/`Summary`/`Set`/`SetMembers`) via `multi_value: skip|expand` — `skip` drops+counts, `expand` renders dotted sub-paths (`.count`, `.sum`, `.q0_5`, `.bucket_<b>`, etc.). Sanitizes path/tag bytes to carbon's allowed character set. Packs pickle datapoints into frames bounded by `max_frame_bytes`, patching a 4-byte big-endian length prefix in place once a frame closes (`payload_len() as u32` cast at encode.rs:716 — sound only because `max_frame_bytes` is graph-rule-capped to ≤16 MiB; verified against `crates/logit-pipeline/src/graph.rs:343`, `GRAPHITE_FRAME_BYTES_RANGE = 1024..=16*1024*1024`, rule 46).
- **Why sensitive:** custom, data-loss (multi-value skip/expand is intentional and named — see known-gaps.md — but still a spot where a config choice or a future metric kind could silently lose data), lossless-roundtrip (frame-packing must keep `graphite_in -> graphite_out` byte-exact), hot-path.
- **Invariants to verify:**
  - `payload_len() as u32` (encode.rs:716) can never truncate — depends entirely on the *external* graph-rule cap holding at every call site, not a local type-level guarantee; confirm no path constructs `GraphiteEncoder`/`Sink` bypassing graph validation
  - a pickle datapoint that can't fit an *empty* frame is dropped, never causes an infinite open/close loop (encode.rs:663-671)
  - `MultiValue::Expand` sub-path suffixes can never collide with a scalar record's own unsuffixed path
  - non-finite values are dropped on encode symmetrically to decode's rejection (encode.rs:380-383)
  - sanitization (`sanitize_into`, encode.rs:884+) never emits a byte outside carbon's accepted set even for adversarial (e.g. OTLP-sourced) attribute names/values; empty-after-sanitize names/values are dropped, not emitted
  - collision handling: two attributes whose *sanitized* names collide resolve deterministically (original-name sort order via `TagSlot.original`, encode.rs:730-738), not by interner/iteration order
  - `Value::Array` renders as its last element only, and this lossy path is counted (`tags_normalized_multi_value`)
- **Observed concerns (unverified):** none at the code level; sanitization/collision logic reads as carefully reasoned. The `u32` cast at encode.rs:716 is sound only by an external invariant (graph validation) rather than a local `debug_assert!` — low-confidence maintainability gap: a future refactor constructing the encoder outside the config-validated path (a test helper, a new relay feature) could silently violate it.
- **Existing coverage:** `crates/logit-proto/tests/graphite_fixed_point.rs` (round-trip, proptest, includes `a_sum_becomes_a_gauge_on_the_first_hop_and_is_then_a_fixed_point`, `tag_order_is_canonical_after_the_first_hop`); `crates/logit-bench/tests/allocations.rs` asserts exact allocation counts for a warm encode. ADR `graphite-carbon-relay.md`; `docs/known-gaps.md` "encode (Graphite)" rows (~1184-1190) enumerate every accepted lossy mapping.
- **Suggested verification approach:** code review of the `u32` cast's external-invariant dependency (consider a cheap `debug_assert!(payload_len <= u32::MAX as usize)` at the cast site); a proptest targeting attribute names crafted to collide post-sanitization (e.g. `"a.b"` and `"a!b"` both sanitizing to `"a_b"`) to confirm the tie-break is actually exercised.
- **Priority:** P2 — mostly intentional, well-documented lossy behavior with strong round-trip coverage; the u32-cast external dependency and collision tie-break are worth a quick look but don't look actively broken.

---

### CODEC-05 — DogStatsD/statsd line decoder — per-line dispatch and event-text unescaping
- **Location:** `crates/logit-inputs/src/statsd.rs:538-576` (`Decoder::decode_into`), `:726-857` (`parse_line`, metric grammar dispatch), `:858-949` (`parse_event`), `:959-974` (`unescape_event_text`), `:1120-1128` (`parse_finite_value`)
- **What it does:** Splits a UDP/TCP-delivered datagram into `\n`-separated lines, trims framing whitespace (preserving trailing whitespace on `_e{`/`_sc|` lines where it can be real payload), and independently parses each line as a metric, DogStatsD event, or service check. `unescape_event_text` unescapes DogStatsD's `\n`-as-backslash-n encoding with an exact-capacity `Vec` (`raw.len() - escapes`, where `escapes` counts non-overlapping 2-byte `"\n"` occurrences) so the zero-copy fast path (no escapes) and the single-allocation slow path both avoid `String::replace`'s extra allocation.
- **Why sensitive:** untrusted-input (raw datagram bytes, arbitrary tag/value content), hot-path (every statsd line, v0.1's target protocol), custom (hand-rolled grammar dispatch, not a parser-combinator crate), lossless-roundtrip (ADR `lossless-transit`'s statsd leg — decoded shape must survive `statsd_in -> statsd_out`).
- **Invariants to verify:**
  - `unescape_event_text`'s `Vec::with_capacity(raw.len() - escapes)` (statsd.rs:964) never underflows: `escapes` is `raw.matches("\\n").count()`, and each match consumes 2 bytes, so `escapes <= raw.len() / 2 < raw.len()` whenever `escapes > 0` (the `escapes == 0` case returns early at :961-963 before the subtraction) — confirm this holds for adversarial input like a string entirely of `\n\n\n...` repeats and for the empty string
  - the `debug_assert_eq!(out.len(), out.capacity())` (statsd.rs:972) is genuinely never wrong — i.e. in release builds an off-by-one here would silently reallocate rather than corrupt, since `debug_assert!` is compiled out; confirm there's no *correctness* dependence on this being exact beyond the performance claim
  - one malformed line never discards other lines in the same datagram (per-line isolation, statsd.rs:562-570)
  - `parse_finite_value` rejects non-finite (`NaN`/`inf`) parsed floats (statsd.rs:1120-1128) wherever it's the value parser
  - `slice_of` (statsd.rs:587-592) pointer-arithmetic reconstruction of a `Bytes` always lands inside the original `bytes` allocation — true by construction (every `sub` is a slice of `text`, which is a `str::from_utf8` view of `bytes`) but this pattern (subtracting raw pointers) is repeated in 3+ files (statsd, syslog, graphite) and any future refactor that hands `slice_of` a `&str` obtained by any other route (e.g. concatenation, `.to_owned()`) would produce undefined pointer arithmetic
- **Observed concerns (unverified):** none spotted in the arithmetic itself. The `sub_start - text_start` (and its syslog/graphite siblings) is not `unsafe`, but it is exactly the kind of pattern that becomes unsound if a well-meaning refactor swaps a borrowed substring for an owned one — worth flagging as a "don't touch without re-reading the doc comment" site for future codebase changes, not a bug today.
- **Existing coverage:** `statsd.rs:1130+` `#[cfg(test)] mod tests` (grammar, event/service-check parsing, tag folding). `crates/logit-inputs/tests/statsd_to_aggregate.rs` (integration through `aggregate`). ADR `statsd-output.md` (mirror), `lossless-transit.md`. Not confirmed in this pass whether `crates/logit-proto/tests/robustness.rs` includes a statsd fuzz/truncation section comparable to graphite's/collectd's — worth checking explicitly (the module doc for statsd wasn't found to make the same "highest-risk parser" claim graphite's does, which may just mean it's simpler grammar, not that it's untested).
- **Suggested verification approach:** a proptest specifically constructing adversarial `_e{...}` event bodies with runs of `\n` escapes (including odd/malformed patterns like a trailing lone backslash) to stress the capacity arithmetic; confirm `crates/logit-proto/tests/robustness.rs` coverage for statsd specifically (grep found graphite/collectd sections but this pass didn't confirm a statsd one).
- **Priority:** P1 — v0.1's target protocol, untrusted input, custom grammar; no bug found but capacity arithmetic and pointer-reconstruction patterns deserve a fuzz pass given the DoS history in the sibling graphite codec.

### CODEC-06 — statsd/DogStatsD encoder — service-check status coercion and multi-value rendering
- **Location:** `crates/logit-outputs/src/statsd.rs:1095-1129` (`render_service_check`, status coercion), `:976-1010` (event line rendering), module-wide `Samples`/`SetMembers` expansion (search `sketch()`/`raw`/`format: dogstatsd` in the same file)
- **What it does:** The mirror of the statsd_in decoder — encodes `Event`/`MetricRecord`s back to statsd/DogStatsD wire lines, including `Sum`(delta,monotonic)/`Gauge`/`GaugeDelta`/`Samples`/`SetMembers`/events/service checks. `render_service_check` must produce a status in `0..=3`: it prefers an explicit `statsd.service_check.status` carrier if present and valid, else coerces the underlying Gauge's float value with `.round()` and a bounds check (`is_finite() && (0.0..=3.0).contains(&rounded)`) before the `rounded as u64` cast (statsd.rs:1114-1117), dropping and counting otherwise.
- **Why sensitive:** custom, lossless-roundtrip (ADR `statsd-output.md`'s round-trip claim: `statsd_in -> statsd_out` must be byte-identical modulo named normalizations), hot-path, data-loss (post-sketch kinds `Distribution`/`Set`/`Histogram`/`ExponentialHistogram`/`Summary` and cumulative/non-monotonic `Sum` are deliberately dropped-and-counted per `docs/known-gaps.md` — confirmed documented gap, not a surprise).
- **Invariants to verify:**
  - the `rounded as u64` cast (statsd.rs:1116) is only reached when `gauge_value.is_finite() && (0.0..=3.0).contains(&rounded)` — confirm this guard can't be bypassed by an intermediate NaN produced by `.round()` itself (NaN's `.round()` is NaN, which fails `is_finite()`, so this should hold, but worth an explicit test with `f64::NAN`/`f64::INFINITY`/`-0.0` as the carried gauge value)
  - a config-declared `statsd.service_check.status` carrier outside `0..=3` correctly falls through to the gauge-coercion path rather than being trusted (statsd.rs:1111-1113 `Some(s) if s <= 3 => s`)
  - the documented lossless round trip actually holds byte-for-byte for `format: dogstatsd` on timers/sets, and holds modulo the ADR's *named* normalizations only (multi-value line split, `h`/`d` -> `ms`) under `format: statsd`
  - dropped post-sketch metric kinds are counted, not silently discarded (accounting invariant, per `docs/known-gaps.md`)
- **Observed concerns (unverified):** none spotted — the status-coercion guard reads correct by inspection.
- **Existing coverage:** `crates/logit-outputs/src/statsd.rs:2288+` extensive `#[cfg(test)] mod tests` (module has 5223 lines total, the bulk apparently tests). ADR `statsd-output.md` claims and documents the round-trip guarantee and its exceptions explicitly; `docs/known-gaps.md` names the dropped-kind list.
- **Suggested verification approach:** targeted unit tests feeding `f64::NAN`, `f64::INFINITY`, `f64::NEG_INFINITY`, `-0.0`, and values just outside `[0,3]` (e.g. `3.4999999`, `-0.0000001`) as the service-check gauge value; a differential round-trip test against a real DogStatsD-speaking client/agent if not already covered by an interop fixture.
- **Priority:** P2 — well-guarded arithmetic, strong existing test file, documented (not surprising) lossy scope; no defect found.

### CODEC-07 — RFC 3164/5424 syslog parser — PRI/TIMESTAMP framing and dialect sniffing
- **Location:** `crates/logit-inputs/src/syslog.rs:590-672` (main `parse_line`-style entry: PRI parsing, dialect sniff between RFC 3164/5424), `:674-704` (`parse_3164_timestamp`, fixed 15-byte timestamp shape), `:706-745` (`is_tag_shaped`, bracketed-PID heuristic)
- **What it does:** Parses the leading `<PRI>` field (validates it's 1-3 ASCII digits, rejects leading zeros except literal `0`, rejects PRI > 191 which would decode to an impossible facility/severity), then sniffs whether what follows is RFC 5424 (a digit followed by a space, i.e. VERSION) or falls back to RFC 3164. `parse_3164_timestamp` checks a fixed 15-byte `Mmm dd hh:mm:ss` shape with an explicit `s.len() < 15` guard before any indexing. The 5424 sniff has an explicit fallback: if the sniffed "version" fails to parse as 5424 but isn't literally `'1'`, it's treated as a false-positive sniff and reparsed as 3164 (with a diagnostic) rather than dropped.
- **Why sensitive:** untrusted-input (raw syslog datagrams/TCP lines from arbitrary senders), custom (no syslog-parsing crate — full hand-rolled grammar for two RFCs at once), lossless-roundtrip (ADR `syslog-output.md`/`syslog-tcp-ingress-and-tls.md`; `syslog_in -> syslog_out` must round-trip per `lossless-transit.md`).
- **Invariants to verify:**
  - every fixed-width slice (`&s[..15]` at syslog.rs:682, `&after_lt[..gt]` at :599) is preceded by a length check that makes the slice always in-bounds (confirmed present for both in this pass: `s.len() < 15` guard at :679, and `gt` is itself bounded by `position()` finding `>` within `after_lt` so `gt <= after_lt.len()`)
  - PRI's leading-zero and >191 rejections (syslog.rs:607-613) correctly prevent an impossible facility (>23) / severity (>7) combination from ever reaching `map_severity`
  - the RFC 5424/3164 dialect sniff's fallback-to-3164 path (syslog.rs:628-668) cannot infinite-loop or double-count a diagnostic, and correctly distinguishes "genuine malformed 5424 line" (version `'1'`, fails outright, syslog.rs:651) from "false-positive sniff of a 3164 line whose MSG starts with a digit-space" (any other version digit, falls back silently-but-logged, syslog.rs:652-667)
  - `is_tag_shaped`'s bracketed-PID logic (syslog.rs:706-745) never indexes past `body`'s bounds when `open` (the position of `[`) is found via `rposition`, and correctly requires the bracket to be closed at the very end (`body.last() != Some(&b']')`)
- **Observed concerns (unverified):** none spotted — every fixed-offset slice found in this pass was preceded by an explicit length check, and the RFC dialect sniff's edge case (a 3164 line whose message happens to start with `"4 requests failed"`-shaped text) is explicitly named and tested for in the comments. Did not exhaustively re-derive every possible slice in the ~600 lines of this file not read in this pass (syslog.rs is 2465 lines with real logic to line 1181) — a full line-by-line audit of every `&s[a..b]` was not completed; the grep found no additional un-guarded fixed-width slices beyond what's discussed here, but that grep is pattern-based, not a proof.
- **Existing coverage:** `syslog.rs:1181+` extensive `#[cfg(test)] mod tests`. Need to confirm (not done this pass) whether `crates/logit-proto/tests/robustness.rs` has a syslog section with the same truncation/bit-flip treatment graphite/collectd get — grep in this pass only searched for "graphite"/"pickle"/"carbon", not "syslog"; a follow-up grep for `syslog` in robustness.rs is a quick, worthwhile check. ADR `syslog-output.md`, `syslog-tcp-ingress-and-tls.md`.
- **Suggested verification approach:** confirm/add a `robustness.rs` syslog section (byte-truncation + bit-flip survival) if missing, given every sibling wire codec in this survey area has one; targeted fuzz on the PRI/dialect-sniff boundary (crafted lines starting with digit-sequences designed to bounce between the 3164/5424 paths).
- **Priority:** P1 — untrusted input, fully custom dual-RFC grammar, no crate; code reads carefully guarded but this pass didn't confirm equivalent fuzz coverage to graphite/collectd, which is itself worth checking before downgrading.

### CODEC-08 — RFC 5424 STRUCTURED-DATA parser — bounded loop, no recursion, param folding
- **Location:** `crates/logit-inputs/src/syslog.rs:901-923` (`parse_sd_name`), `:925-967` (`parse_param_value`, escape handling), `:969-984` (`insert_param`, repeated-PARAM-NAME folding), `:986-1149+` (`parse_structured_data`, the SD-ELEMENT loop)
- **What it does:** Parses RFC 5424's `[SD-ID PARAM-NAME="PARAM-VALUE" ...][...]` structured-data blocks: a `while`/`loop` state machine advancing a `pos` cursor through the byte slice, never recursing. `parse_sd_name` bounds SD-NAME/PARAM-NAME to 1-32 bytes per RFC 5424. `parse_param_value` unescapes `\"`, `\\`, `\]` (keeping any other backslash-prefixed byte literal, rather than erroring or dropping the backslash). Repeated PARAM-NAMEs within one SD-ELEMENT fold into a `Value::Array` in encounter order (syslog.rs:973-984), and duplicate SD-IDs across elements are rejected outright (syslog.rs:1014-1016).
- **Why sensitive:** untrusted-input, custom (hand-rolled recursive-descent-shaped but iterative state machine), lossless-roundtrip (structured data is one of `lossless-transit.md`'s named model-v2 additions — `syslog.sd`).
- **Invariants to verify:**
  - the whole parser is genuinely non-recursive and its `pos` cursor strictly advances every iteration of every `loop`/`while` (confirmed by direct reading in this pass for `parse_param_value`'s loop, syslog.rs:932-966, and the outer SD-ELEMENT loop's structure, syslog.rs:1002-1040+) — so its only bound on work/memory is the overall line-length cap enforced upstream by the listener (not this file), not a hardcoded SD-element or nesting count; confirm the upstream `max_line_bytes` (or equivalent) config actually applies before this parser ever sees the bytes, since this file has no depth/count cap of its own
  - `parse_sd_name`'s 1-32 byte SD-NAME bound is enforced *after* scanning to the first non-SD-NAME-byte, i.e. a 10,000-byte run of valid SD-NAME bytes with no terminator is scanned in full before being rejected for length (syslog.rs:905-920) — O(n) per attempt, not unbounded, but worth confirming this can't be made quadratic by many back-to-back long invalid names within one line (each failure aborts the whole structured-data parse per the error-propagation `?`, so it shouldn't be)
  - `parse_param_value`'s trailing-backslash and unterminated-quote cases are both handled (return `Err`, not panic) even at the very end of input (syslog.rs:953-958, :934-936)
  - `id.get(pos)`/`s.get(pos)` style bounds checks (not raw indexing) are used throughout the loop bodies — confirmed via `.get(` pattern reading; the two `.expect("parse_sd_name only accepts PRINTUSASCII, always valid UTF-8")` calls (syslog.rs:1007, 1029) are sound *only* because `is_sd_name_byte` (syslog.rs:897-899) restricts to PRINTUSASCII, which is a UTF-8 subset — confirm `is_sd_name_byte`'s definition hasn't drifted from ASCII-only since these `expect`s were written
- **Observed concerns (unverified):** none spotted — the loop is genuinely bounded by input length with no separate cap, which is fine given upstream line-length limits but means this parser has *no defense in depth* of its own against a very long single SD-ELEMENT with many params; low-severity since the outer line-length cap is a real, config-validated bound elsewhere, but a verifier should confirm that bound is unconditionally applied (i.e. `syslog_in` over TCP with a large `max_line_bytes` misconfiguration wouldn't let this parser see unboundedly large input).
- **Existing coverage:** `syslog.rs:1181+` test module (not fully read this pass for SD-specific cases). ADR `log-record-trace-context.md`/`trace-context-span-lifting.md` touch adjacent `span:` handling; `lossless-transit.md` for the `syslog.sd` model addition.
- **Suggested verification approach:** proptest generating adversarial SD blocks (deeply repeated PARAM-NAMEs, near-32-byte and over-32-byte names, unterminated quotes, backslash runs) checked against a real syslog-ng/rsyslog parse where feasible for a differential signal; confirm the upstream line-length cap is what actually bounds this parser's worst case, and consider whether an explicit max-SD-element or max-PARAM count would be cheap insurance independent of that external bound.
- **Priority:** P2 — bounded, non-recursive, careful `.get()`-based indexing throughout; the only real question is whether it should have its own defense-in-depth cap rather than relying entirely on an external line-length bound.

### CODEC-09 — syslog encoder — structured-data escaping, header-field sanitization, and oversize/truncation handling
- **Location:** `crates/logit-outputs/src/syslog.rs:397-552` (`encode_event`, `push_message_str`, `push_message_bytes`), `:556-826` (`resolve_facility`/`resolve_severity`/`write_rfc5424_header`/`write_rfc3164_header`/`sanitize_5424_field`/`sanitize_3164_token`/`is_valid_sd_name`), `:868-1053` (`push_sd_escaped`, `write_structured_data`, `write_sd_element`, `write_sd_param`), `:1143-1218` (`sanitize_msg`/`sanitize_msg_bytes`/`truncate_on_char_boundary`/`truncate_bytes`/`frame_octet_counting`) — directly read line-by-line this pass (superseding a prior placeholder in this file that had not been).
- **What it does:** The mirror of the syslog parser above — renders `Event`s back to RFC 3164/5424 wire lines over UDP/TCP/TLS. `resolve_facility`/`resolve_severity` reconstruct PRI from `syslog.facility`/`syslog.severity` attributes (each independently range-checked, `default_facility` clamped to `.min(23)` at construction, `SyslogEncoder::new` line 308), so `pri = facility*8+severity` is arithmetically incapable of exceeding 191 or underflowing — the exact malformed shape the decoder rejects on ingest. Every header field (HOSTNAME/APP-NAME/PROCID/MSGID/TAG) is sanitized to `PRINTUSASCII` with a per-field byte cap (`sanitize_5424_field`/`sanitize_3164_token`, iterating `char`s but only ever pushing single-byte ASCII, so the `scratch.len() >= max_len` cap is exact, never off-by-a-multibyte-char). STRUCTURED-DATA (`write_structured_data`) sorts SD-IDs and PARAM-NAMEs by name bytes before writing (`AttrMap` iteration order is intern order, not wire order — sorting makes output a pure function of the data) and explicitly detects/avoids a collision between an origin's own `syslog.sd` element and the opt-in `structured_data` config block sharing an SD-ID (would otherwise emit a wire line the decoder's own duplicate-SD-ID rule would reject). Message bodies go through `sanitize_msg`/`sanitize_msg_bytes` (byte-level twin for non-UTF-8 `Value::Bytes`, avoiding lossy conversion) which neutralize `\n`/`\r`/NUL/other C0/DEL with backslash mnemonics — the framing-level defense (RFC 6587 octet-counting, `frame_octet_counting`) is a second, independent layer against the same message-forging class, not a redundant one. If the header alone exceeds `max_message_bytes` the whole message is dropped (counted `dropped_oversize_header`) rather than truncating a header field into something a receiver would misparse; the message body is truncated on a UTF-8 char boundary (`truncate_on_char_boundary`) or raw byte boundary (`truncate_bytes`) instead.
- **Why sensitive:** custom, lossless-roundtrip (explicit ADR-level `syslog_in -> syslog_out` round-trip claim), data-loss (a field syslog's wire can carry but `Event` can't represent is tracked debt per AGENTS.md's "lossless-transit" rule; STRUCTURED-DATA's wire *order* for an interleaved repeated PARAM-NAME, e.g. `a b a`, is also normalized to grouped/sorted rather than preserved — documented in `docs/known-gaps.md`, not a surprise).
- **Invariants to verify:**
  - `push_sd_escaped` (syslog.rs:868-883) is the exact inverse of the decoder's `parse_param_value` unescaping: it escapes `"`/`\`/`]` per RFC 5424 §6.3.3, *and additionally* pre-converts `\n`/`\r`/NUL/other-C0/DEL to `sanitize_msg`'s own two-character mnemonics before that escaping pass runs — so a literal newline becomes the three wire bytes `\`,`\`,`n` (verified: Rust source `"\\\\n"` is literally backslash-backslash-n), which the decoder's `\\` rule folds back into two-character text `\n`, never a real newline; confirm this two-step composition is exactly symmetric with the decoder for every one of the 4 special mnemonic cases plus the generic `\xNN` case.
  - `push_message_str`/`push_message_bytes`'s budget arithmetic (`self.max_message_bytes - self.line.len()` after `self.line.push(' ')`, syslog.rs:497/533) can never underflow: the preceding `if self.line.len() >= self.max_message_bytes` branch (485/521) already returned before the push, so post-push `line.len() <= max_message_bytes` always holds — re-derive this by hand rather than trusting the read, since it's a subtraction one line after a mutation.
  - `write_structured_data`'s collision check (syslog.rs:946-949: does the opt-in element's `sd_id` already exist as a key in `syslog.sd`) fires before the opt-in element is ever rendered, and the "nothing to emit anyway" pre-check (937-938) can't cause a real collision to go uncounted.
  - `sanitize_5424_field`/`sanitize_3164_token`'s `scratch.len() >= max_len` cap (checked before each push, not after) always yields `scratch.len() <= max_len` exactly, since every pushed `char` is guaranteed single-byte (either already `PRINTUSASCII`-range ASCII, or replaced with `_`) — confirm no future edit could push a multi-byte replacement character and silently break the byte-length contract the RFC caps depend on.
  - `civil_time_of`'s Hinnant civil-from-days arithmetic and `MONTH_ABBR[(month as usize - 1).min(11)]` indexing (syslog.rs:1078, 1085-1101) can't underflow/panic for any `i64` nanosecond value reachable from `event.timestamp` — checked by hand this pass: since nanosecond-`i64` inherently bounds the representable date range to roughly ±292 years, `days` stays small in magnitude regardless of how extreme the input `i64` is, so `month` is always computed in `1..=12` and the `usize` subtraction never underflows; worth a proptest over `i64::MIN`/`i64::MAX`/near-boundary values to confirm rather than rely on this hand-derivation, especially given the sibling OTLP decoder bug (this file's own entry below) can hand this encoder a corrupted-but-still-in-range `i64` timestamp.
- **Observed concerns (unverified):** none found at high confidence after a direct line-by-line read of every function listed above — the code is as carefully guarded and as thoroughly self-documented (each function's doc comment cross-references the decoder's matching behavior and the module's own "Injection safety"/"Sizing"/"STRUCTURED-DATA" sections) as every other codec in this survey. One low-confidence note: `write_rfc5424_header`'s numeric-PID rendering (`Pid::U64`, syslog.rs:644-646) writes the value via `{p}` with no length cap, unlike the `Pid::Str` arm which routes through `push_5424_field`'s 128-byte cap — not a real bug (`u64::MAX` is 20 ASCII digits, far under any RFC 5424 PROCID-length concern), but it is an asymmetry between the two `Pid` arms worth a verifier's explicit note rather than silent parity assumption.
- **Existing coverage:** `crates/logit-outputs/src/syslog.rs:1774+` extensive `#[cfg(test)]` module (3962 lines total) with dedicated tests for exactly the edge cases above: `an_embedded_newline_cannot_forge_a_second_message`, `embedded_carriage_return_and_nul_are_escaped`, `a_literal_backslash_passes_through_unescaped_so_json_bodies_stay_valid`, `a_hostname_with_space_and_non_ascii_is_sanitized`, `an_app_name_longer_than_48_bytes_is_truncated`, `an_oversize_message_is_truncated_on_a_char_boundary_not_the_header`, `an_oversize_header_drops_the_message_entirely`, `max_message_bytes_exactly_at_the_header_length_never_overflows_the_cap`, `frame_octet_counting_prefixes_each_message_with_its_exact_byte_length`, `sd_param_value_escapes_quote_backslash_and_close_bracket`, `two_sd_elements_concatenate_with_no_separator`. No dedicated fuzz/robustness-harness section (consistent with this survey's repo-wide finding that `robustness.rs` doesn't cover syslog at all, decoder or encoder). ADR `syslog-output.md`, `syslog-tcp-ingress-and-tls.md`; `docs/known-gaps.md`'s residual-debt list names the timestamp-precedence and SD-order-normalization gaps explicitly.
- **Suggested verification approach:** a differential round-trip proptest (`syslog_in -> syslog_out` byte-for-byte modulo the two named normalizations) generating adversarial structured-data param values (mixed control characters, backslashes, embedded quotes) and header fields (non-ASCII, embedded `:`/`[`/`]`); a targeted unit test constructing an `i64` timestamp at/near `i64::MIN`/`i64::MAX` fed through `civil_time_of` to empirically confirm the no-panic claim above rather than rely on hand-derivation; confirmation against a real syslog-ng/rsyslog or Loki/Alloy receiver for structured-data escaping specifically (the module doc already records one real interop bug found and fixed this way — the RFC 5424 §6.4 BOM, syslog.rs:456-462 — showing this class of differential check has already paid off once here).
- **Priority:** P2 — directly read line-by-line this pass and found to be as carefully guarded as its decoder sibling (P1), with strong existing example-based test coverage and a documented history of catching at least one real interop bug (the BOM) through exactly the kind of differential testing recommended above; downgraded from the prior placeholder's provisional P1 now that the code has actually been verified rather than assumed risky by file role alone.

---

### CODEC-10 — collectd binary decoder — TLV part framing and the Values-part length/count gate
- **Location:** `crates/logit-proto/src/collectd/part.rs:139-153` (`read_part`), `crates/logit-proto/src/collectd/decode.rs:171-213` (`decode_into`, per-datagram walk + partial-decode-keeps-what-parsed policy), `:292-429` (`decode_values` — the untrusted-count gate at 300-317, per-value extraction at 363-370)
- **What it does:** Walks a collectd binary `network` protocol datagram as a flat sequence of self-delimiting TLV parts (`read_part`), dispatching each into "sticky" per-datagram identity state (host/plugin/type/etc.) that a Values part is later decoded against — mirroring collectd's own `parse_packet`. `decode_values` reads an attacker-controlled `u16` data-source count and validates it against the part's own declared length *and* a `MAX_VALUES_PER_LIST` (64) cap before indexing into the type-vector or value bytes, or interning any per-value metric name. Fully read and directly verified line-by-line in this pass (not a placeholder).
- **Why sensitive:** untrusted-input (raw UDP bytes, no auth), custom (hand-rolled TLV walker, not a parser-combinator or schema-driven decoder), hot-path (per-datagram, per-value-list), accounting (a malformed part mid-datagram must keep already-decoded lists rather than discard the whole batch — an explicit reconciliation the code calls out).
- **Invariants to verify:**
  - `read_part`'s three checks (`remaining < HEADER_LEN`, `len < HEADER_LEN`, `len > remaining`) are jointly sufficient to make every subsequent `&bytes[at+HEADER_LEN..at+len]` slice in-bounds for any `at`/`bytes` combination, including `at == bytes.len()` and a `len` of exactly `HEADER_LEN`.
  - `decode_values`'s ordering is exactly: length-vs-count cross-check (line 306) -> count range-vs-`MAX_VALUES_PER_LIST` check (309) -> type-vector validation (313-317) -> *only then* any indexing into `payload[values_at + index*8 ..]` or a per-value `intern()` call — i.e. no allocation or interner growth is reachable from an unvalidated count.
  - The two `.expect()`s at decode.rs:368/370 (`try_into()` for the 8-byte value slice, `DsValue::from_wire` for a pre-validated type byte) are genuinely unreachable given the preceding checks — re-derive the arithmetic (`values_at = 2 + count`, slice bounds against the already-verified `expected == len` equality) rather than trusting the comment.
  - A malformed part partway through a multi-list datagram correctly keeps every list decoded *before* it and discards only the remainder, with the right `bad_part`-vs-hard-`CodecError` branch (kept-lists count is 0 vs. >0, `decode_into` lines 193-206).
  - `TYPE_ENCRYPTION` correctly stops the walk (no attempt to interpret ciphertext as further parts); `TYPE_SIGNATURE` is skipped by length with no verification (documented, deliberate — this codec holds no keys).
- **Observed concerns (unverified):** none spotted — this is some of the most carefully pre-validated untrusted-parsing code in the survey (the module's own doc comment states the "every length and type check before a single allocation" invariant explicitly, and a dedicated test proves it via a peak-allocation counter — see below).
- **Existing coverage:** **exceptionally strong** — `crates/logit-proto/tests/robustness.rs` runs `collectd_decode_survives_every_single_byte_truncation` (every prefix-truncation of a real packet), `collectd_decode_survives_seeded_bit_flips` (4000 seeded bit flips), and `collectd_decode_rejects_a_values_count_inflated_far_past_what_the_input_holds` — a hand-crafted part declaring 65535 data sources over 20 bytes, asserting both rejection *and* (via a custom `CountingAlloc` peak-allocation counter) that peak live bytes stays under 4 KiB, directly proving the count is checked before sizing. `crates/logit-proto/tests/collectd_fixed_point.rs` covers wire-level fixed-point round-trips including a proptest over a generated packet grammar. Governed by ADR `collectd-binary-relay`.
- **Suggested verification approach:** given the strength of existing coverage, this is closer to a targeted-code-review item than a new-fuzz-target item — spot-check the `.expect()` arithmetic manually, and confirm the robustness suite's crafted-hostile-count test also exercises a count that's valid-per-length-check but exceeds `MAX_VALUES_PER_LIST` (a boundary between the two distinct checks at lines 306 and 309).
- **Priority:** P1 — untrusted network input with real panic-adjacent arithmetic, but already has some of the best-targeted fuzz/mutation coverage in the codebase (bit-flip, truncation, and a hostile-count allocation-DoS regression test all exist and pass); residual risk is in the unexercised interaction between the two count checks, not in a missing-coverage gap. (Supersedes a prior placeholder entry in this file that had marked collectd as unreviewed/P0-provisional — it has now been read line-by-line and does not warrant that priority.)

### CODEC-11 — collectd binary encoder — identity sanitization/truncation and the write_string_part panic contract
- **Location:** `crates/logit-proto/src/collectd/encode.rs:848-882` (`sanitize_raw`), `:993-1026` (`write_list`), `crates/logit-proto/src/collectd/part.rs:155-218` (`write_string_part`, `write_values_part` — both `.expect()`-panic-on-violated-invariant by design)
- **What it does:** Before any identity field (host/plugin/plugin_instance/type/type_instance) or value list reaches the wire, `sanitize_raw` substitutes NUL/`/` with `_` and truncates to `MAX_IDENTITY_BYTES` (walking back off UTF-8 continuation bytes when the source is known-UTF-8), and the encoder caps every value list to `MAX_VALUES_PER_LIST` (64) before calling `part::write_values_part`. Both `part.rs` writer functions are designed to **panic** (via `u16::try_from(...).expect(...)`) rather than silently truncate a `u16` wire length if that upstream invariant is ever violated — the doc comment for `write_values_part` explicitly argues a silent `as u16` wraparound (producing a shorter declared length than the real payload) would be worse than a crash. Fully read and directly verified in this pass.
- **Why sensitive:** custom (hand-rolled sanitizer + a deliberate crash-over-corruption design choice), lossless-roundtrip (encoder is the write side of the `collectd_in -> collectd_out` lossless-relay pair), accounting (identity truncation/substitution is counted via `ctx.identity_substituted()`/`identity_truncated()`), data-loss-if-violated (a released build where the cap is bypassed doesn't corrupt silently — it panics, a deliberate but still real DoS-shaped failure mode for the whole process, not just the one bad record).
- **Invariants to verify:**
  - Every call site that reaches `part::write_string_part` passes a value that has already gone through `sanitize_raw` (or `with_hostname`'s construction-time sanitize) with `MAX_IDENTITY_BYTES` enforced — i.e., there is no code path (including a future new call site) that hands an unsanitized, unbounded `Value::Str`/`Value::Bytes` straight to `write_string_part`.
  - The `MAX_VALUES_PER_LIST` cap in `encode_into` (encode.rs ~373-380, `drop_too_many_values`) runs before `write_list`/`write_values_part` is ever called for that list, for every code path that constructs a value list (not just the primary one).
  - `sanitize_raw`'s UTF-8-continuation-byte walk-back (`while end > 0 && out[end] & 0xC0 == 0x80`) can't underflow/infinite-loop on a pathological string, and correctly falls back to `end == 0` (empty output) for a string that is entirely continuation bytes up to the cap — should be unreachable given `Value::Str`'s "always valid UTF-8" invariant, but worth confirming for `Value::Bytes` (`is_utf8 = false`) inputs, which take the byte-boundary (no walk-back) path instead.
  - `write_list`'s identity-elision logic (only emit a string part when `cur.x != last.x`) can't desync from what a real collectd receiver's sticky-state reset expects, especially across the "flush and re-encode the same list a second time" packet-boundary case documented at encode.rs ~1050-1057.
- **Observed concerns (unverified):** none spotted in the reasoning itself; the "panic over corruption" design is explicitly and thoughtfully argued in the source rather than an oversight. The genuinely open question is *empirical*, not logical: whether every call site really does route through the cap, which is an invariant a type system can't enforce here (it's an ordering-of-calls contract, not a type-level one) — worth a targeted review pass specifically hunting for a bypass rather than re-deriving the argument already in the comments.
- **Existing coverage:** `crates/logit-proto/src/collectd/encode.rs`'s own unit tests (from line 1243), including a dedicated `dropped_too_many_values`/`too_many_values` counter test at ~1982-1996 proving the encode-side cap fires (one list dropped, not one per record) and a companion test at the boundary (`MAX_VALUES_PER_LIST` exactly, no drop). `crates/logit-proto/tests/collectd_fixed_point.rs` round-trips a message at exactly 255 bytes (`a_message_at_exactly_255_bytes_is_a_fixed_point`) and non-UTF-8 hosts. Governed by ADR `collectd-binary-relay`.
- **Suggested verification approach:** targeted code review enumerating every call site of `write_string_part`/`write_values_part` to confirm each is reachable only after sanitization/capping; a property test constructing `Event`s with adversarially long/unicode-boundary-straddling identity attributes to confirm `sanitize_raw` never panics and always produces valid, correctly-truncated output.
- **Priority:** P2 — not attacker-facing in the usual sense (this is the encode/egress side, fed by already-decoded or Lua-authored internal `Event`s, not raw wire bytes), and the "crash instead of corrupt" design is a deliberate, well-reasoned tradeoff; residual risk is a hard panic (process-wide DoS) if the invariant is ever silently violated by a new call site, not silent corruption. (Supersedes the prior placeholder entry — read line-by-line in this pass.)

---

### CODEC-12 — Prometheus text/OpenMetrics decoder — line grammar, family assembler, and cumulative-bucket reconstruction
- **Location:** `crates/logit-proto/src/prometheus/text.rs:185-356` (`parse_with`, `Parser::line`/`comment`/`sample`, `parse_sample`, `parse_labels`), `crates/logit-proto/src/prometheus/assemble.rs:939-1058` (`finish_series` — histogram/summary bucket-total reconstruction, `count_value`, `created_nanos`)
- **What it does:** A hand-rolled line-oriented parser for both Prometheus text 0.0.4 and OpenMetrics 1.0 exposition formats (no parser-combinator crate), splitting a scrape body on `\n` and dispatching each line as metadata (`# HELP`/`# TYPE`/`# UNIT`/`# EOF`) or a sample. `assemble.rs`'s `Assembler`/`finish_series` is the stateful family router that reconstructs a `Histogram`'s bucket list from individually-scraped `_bucket{le=...}` lines, synthesizing a missing `+Inf` bucket from `_count` when a producer omits it, and rounds fractional OpenMetrics bucket/count values to the model's `u64` counts.
- **Why sensitive:** untrusted-input (scrape response body, or a receiver's line-oriented ingest — attacker-controlled if `prometheus_in` scrapes an untrusted target or receives arbitrary text), custom (hand-rolled dual-dialect grammar), hot-path (per-line, per-scrape), float edge cases (`count_value`/`created_nanos` handle NaN/Inf/negative/huge floats from the wire before casting to `u64`/`i64`).
- **Invariants to verify:**
  - every line-level parse failure (`parse_sample` returning `None`, a malformed metadata line) costs only that one line/sample, never fails the whole body — except the two structural cases documented as hard errors (`# EOF` misuse, content after EOF in OpenMetrics) — confirm the split between "line skip" and "whole-body reject" matches the module doc's table exactly with no third, undocumented path.
  - `count_value`'s `v.round() as u64` (assemble.rs:1033-1038) is only reached after `v.is_finite() && v >= 0.0`, so a `u64` saturation (for an astronomically large but finite float) is the worst case, never UB or a panic — confirm no other cast site in `assemble.rs` skips this pattern.
  - `created_nanos`'s `nanos.abs() >= i64::MAX as f64` guard (assemble.rs:1052) correctly rejects every value that would overflow `i64` on the `as i64` cast, including values just under the boundary where `f64` rounding could tip it over.
  - `finish_series`'s synthesized `+Inf` bucket (assemble.rs:978-983) can't be fooled by a producer sending a *finite* bucket claiming to be `+Inf`-labeled some other way, and the `histogram_count_mismatch` degradation (not a hard failure) is the only consequence of a producer's `_count` disagreeing with the reconstructed total — never a silently wrong total.
  - Label-set validation (empty name/value, non-ascending byte order — referenced from the remote-write doc as shared assembler logic) applies identically to the text-format path, not just remote-write.
- **Observed concerns (unverified):** none spotted — every float-to-integer cast found in `assemble.rs` was preceded by an explicit finiteness/range guard, matching the pattern seen everywhere else in this survey. Did not exhaustively read `parse_labels`' escape-unescaping loop (text.rs:360+) byte-by-byte in this pass — worth a follow-up specifically on backslash-escape handling for label values (`\\`, `\"`, `\n`) at a string's exact end.
- **Existing coverage:** `crates/logit-proto/src/prometheus/text.rs`'s own unit tests (from line 1138) and `mod.rs`'s (from line 1149); `crates/logit-proto/tests/prometheus_fixed_point.rs` (round-trip tests). **No dedicated section in `crates/logit-proto/tests/robustness.rs`** (that file's own module doc only names `native`, `control`, `collectd`, and `graphite` as covered — prometheus is not mentioned). Governed by ADR `prometheus-scrape-and-exposition`.
- **Suggested verification approach:** a proptest generating adversarial exposition bodies (mixed dialects, missing `+Inf` buckets, fractional bucket counts, out-of-order `le` values, huge/negative/NaN sample values) checked against `finish_series` for panics and correct total reconstruction; a differential test against Prometheus's own `expfmt` Go parser for a shared corpus (the interop-style approach `prometheus_remote_write_interop.rs` already uses for remote-write).
- **Priority:** P1 — untrusted text input with real float-edge-case handling, well-guarded by inspection, but (like statsd/syslog) has no dedicated entry in the project's own robustness/fuzz harness despite that harness explicitly existing for "every decoder that will read untrusted bytes off a socket."

### CODEC-13 — Prometheus remote-write decoder — Snappy decompression-bomb guard and the 2.0 symbol-table indirection
- **Location:** `crates/logit-inputs/src/prometheus.rs:1581-1615` (declared-length-then-decompress gate, `snap::raw::decompress_len` before `decompress_vec`), `crates/logit-proto/src/prometheus/remote_write.rs:777-835` (`resolve_symbol`/`resolve_refs`/2.0's `symbols[0]` invariant check)
- **What it does:** Before ever inflating a Snappy-compressed remote-write POST body, `prometheus_in` reads the decompressed length out of the Snappy block header (`snap::raw::decompress_len`) and rejects the request if that declared length exceeds `MAX_REQUEST_BYTES`, *before* calling `decompress_vec` — a textbook compression-bomb defense. Once decompressed and protobuf-decoded (by `prost`, not hand-rolled), remote-write 2.0's request-wide string-interning table (`symbols[]`, referenced by index from every label) is resolved via bounds-checked `symbols.get(reference as usize)`, with `symbols[0] == ""` and even-length `labels_refs` both explicitly validated as structural invariants that reject the whole request (not a per-series skip) when violated.
- **Why sensitive:** untrusted-input (HTTP POST body from any remote-write sender), nontrivial-3p-use(snap) (using the raw/block API's declared-length introspection specifically to gate against decompression bombs, rather than the simpler-but-unsafe "just decompress it" pattern), accounting (the malformed-vs-skip distinction is itself a correctness property — a corrupt `symbols` table must reject the whole request, since every other series' label resolution depends on the same table being trustworthy).
- **Invariants to verify:**
  - `snap::raw::decompress_len`'s declared length is checked against `MAX_REQUEST_BYTES` on every code path before `decompress_vec` is ever called — confirm there's no alternate decode entry point (e.g. a test helper repurposed in production, or a future streaming variant) that skips the pre-check.
  - `resolve_symbol`/`resolve_refs`'s bounds checks (`symbols.get(reference as usize)`) reject out-of-range indices as a hard `CodecError::Malformed` (whole-request failure), not a per-series skip — confirmed by the module doc's own classification table (remote_write.rs:78-93); verify the code matches the doc exactly, including the odd-length `labels_refs` check.
  - The empty-body-vs-wrong-message-type disambiguation (remote_write.rs:83-91 — a 1.0 body posted with a 2.0 `Content-Type` decodes to a syntactically-valid-but-empty `Request`) is actually caught, not silently accepted as "zero series, 204 OK" (the doc explicitly says this was a real bug class the check exists to close).
  - `reference as usize` (remote_write.rs:777) can't wrap or misbehave for a `reference` read from protobuf as some smaller unsigned type — confirm the protobuf field's declared type and that no sign-extension or truncation is possible on 32-bit-index platforms.
- **Observed concerns (unverified):** none spotted — the decompression-bomb guard is exactly the right shape and explicitly comments on why (`crates/logit-inputs/src/prometheus.rs:1581-1583`). Lower confidence than the rest of this survey on the protobuf-message-shape parsing itself, since that's `prost`-generated/handled and wasn't re-audited here (out of scope per the task's "skip generated" guidance, though `remote_write.rs`'s hand-written post-decode validation *was* reviewed).
- **Existing coverage:** `crates/logit-proto/tests/prometheus_remote_write_fixed_point.rs` and `crates/logit-proto/tests/prometheus_remote_write_interop.rs` (the latter explicitly decompressing real Snappy bodies, `crates/logit-proto/tests/prometheus_remote_write_interop.rs:82`). No dedicated compression-bomb regression test was located in this pass (i.e., a test asserting peak-allocation stays bounded for a hostile declared-length, mirroring collectd's/graphite's robustness-suite pattern) — worth confirming one exists under a different name, or adding one. Governed by ADR `prometheus-remote-write`.
- **Suggested verification approach:** a targeted test constructing a Snappy block whose header declares a length just over `MAX_REQUEST_BYTES` (and one just under, plus one at `usize::MAX`-adjacent boundary values) verified to reject without ever calling `decompress_vec`, ideally with a peak-allocation counter like `robustness.rs`'s `CountingAlloc`; a symbol-table fuzz target feeding a valid-shaped but adversarially-indexed `labels_refs`.
- **Priority:** P1 — the compression-bomb gate is correctly implemented and is exactly the kind of check that's easy to silently regress (e.g. someone "simplifying" to a single `decompress_vec` call during a refactor); worth a dedicated regression test given how much damage a silent regression here would do (unbounded memory from a single small HTTP request).

---

### CODEC-14 — InfluxDB line-protocol encoder — collision-avoiding timestamp allocator and field/tag escaping
- **Location:** `crates/logit-outputs/src/influxdb.rs:579-617` (`allocate_timestamp` — a union-find-style "smallest free slot ≥ requested" allocator with path compression), `:505-561` (call site: per-batch `series_allocated_timestamps` map, series-key computation), `:797-827` (`push_escaped_measurement`/`push_escaped_tag`/`push_escaped` — line-protocol special-character escaping), `:659-733` (`render_fields`, non-finite-value guards per metric kind)
- **What it does:** Because an InfluxDB point's identity is `(series key, timestamp)`, two events in the same output batch that reduce to the same measurement+tag-set+timestamp would otherwise silently overwrite each other at the database (the second write wins, the first is gone with no error). `allocate_timestamp` prevents this by finding the smallest free timestamp `>= requested` for that series within the batch, using a per-series `HashMap<i64,i64>` "next free slot" chain with path compression (a union-find pattern) so that repeated collisions on one series cost amortized-constant time rather than the O(k²) a naive linear re-probe would cost — the comment documents two earlier, rejected designs and exactly why each was wrong (arrival-order-dependent, or over-corrective). `push_escaped` implements line protocol's backslash-escaping for measurements/tags in one allocation-free pass over the common (nothing-to-escape) case.
- **Why sensitive:** custom (a hand-rolled union-find variant, not from a crate — this is genuinely novel algorithmic code, not a wrapper), hot-path (every metric line in every batch to `influxdb_out`, v0.1's target sink), data-loss-if-wrong (the entire point of this function is preventing a specific silent-overwrite data-loss mode; a bug in the allocator could itself reintroduce the same silent loss it exists to prevent, or worse, non-deterministically allocate colliding timestamps), accounting (`i64::MAX` reachable only via `checked_add(1)?` returning `None`, which the caller turns into a hard `CodecError::Malformed` rather than a silent collision).
- **Invariants to verify:**
  - the path-compression rewrite (`allocate_timestamp`, influxdb.rs:598-617) preserves the "smallest free slot" property exactly — after any sequence of calls, every timestamp `next_free` maps *from* is genuinely occupied, and the value it maps *to* is either free or itself correctly chained to a free slot (no cycles, no stale pointers into now-freed-then-reoccupied territory — though nothing here ever frees a slot once allocated, so "reoccupied" shouldn't apply, worth confirming)
  - a `HashMap` classic re-entrancy hazard: `visited` (the scratch `Vec` for the walk) is correctly cleared at the start of every call (confirmed: `visited.clear()` at influxdb.rs:603) and every visited slot on a chain is repointed to the *final* free-slot-plus-one, not to an intermediate node (confirmed: `next_free.insert(slot, successor)` for every drained `visited` entry, using the same `successor` computed once) — re-derive this by hand against the doc comment's own worked examples rather than trusting the prose
  - `checked_add(1)?` at influxdb.rs:611 correctly returns `None` (propagated as a hard encode error, not a panic or silent collision) when `cur == i64::MAX`, and this is genuinely unreachable from any realistic batch (documented as requiring 2^63 prior allocations at or after one requested timestamp) — sanity-check that reasoning rather than accept it uncritically, since "not reachable in practice" is exactly the kind of claim that's worth a skeptical second look
  - `series_allocated_timestamps` is correctly scoped to one batch (hoisted at `encode`'s top level per the comment at influxdb.rs:542-543) and never leaks state across batches, which would otherwise nudge an *unrelated* later batch's legitimate timestamp for no reason
  - `render_fields`/`push_float` (influxdb.rs:659-757) — every non-finite (`NaN`/`inf`) float value is filtered before reaching `push_float`'s `debug_assert!(v.is_finite())` (influxdb.rs:755), which is compiled out in release, so a violated invariant here would silently write literal `NaN`/`inf` text into the line protocol output (which the comment says InfluxDB would then reject the whole write for) rather than panicking loudly in production — confirm every `MetricKind` arm in `render_fields` truly guards with `.is_finite()` before calling `push_float`/`push_uint`, with no gap for a newly-added metric kind
- **Observed concerns (unverified):** the `debug_assert!` (not a real assert) backing `push_float`'s non-finite invariant is a soft spot structurally: in a release build, a future code path that fails to filter a NaN/Inf before calling `push_float` would silently emit malformed line-protocol text instead of panicking or erroring — low confidence this currently has a gap (every call site read in `render_fields` does guard explicitly), but the enforcement mechanism itself (debug-only) is weaker than the rest of this survey's typical "hard check, real error" pattern. The union-find allocator's correctness was reasoned through by re-reading rather than by tracing an actual multi-collision example by hand; a skeptical verifier should work at least one concrete 3-4-collision scenario through by hand against the code, not just the prose.
- **Existing coverage:** `crates/logit-outputs/src/influxdb.rs`'s own test module (from line 829) explicitly includes "`allocate_timestamp` regression tests" (per the code's own comment at ~line 852) covering same-series-different-measurement non-collision, same-metric-name-twice-on-one-event collision, and (per a comment at ~line 1433) the out-of-order collision case the doc walks through. ADR references not independently checked for an InfluxDB-specific ADR beyond the general `lossless-transit`/lossless-relay framing (`influxdb_out` predates most per-protocol ADRs as the v0.1 target).
- **Suggested verification approach:** a property test modeling the allocator abstractly (a reference "linear scan for smallest free slot" implementation checked for agreement against `allocate_timestamp` under randomized collision sequences, including adversarial orderings) rather than only the hand-written example-based unit tests; a targeted review converting the `debug_assert!` in `push_float` into a real guard (or confirming via exhaustive `MetricKind` match-arm review that it can never fire) given `debug_assert!`'s release-mode blind spot is otherwise a manual, human-verified invariant.
- **Priority:** P1 — genuinely custom, hot-path algorithmic code whose entire purpose is preventing silent data loss, in the v0.1 target sink; well-tested by example but not proven by property/fuzz testing, and one soft (`debug_assert!`-only) invariant enforcement point.

### CODEC-15 — `MessageBuf<M>` — reusable framed-message buffer (low sensitivity)
- **Location:** `crates/logit-proto/src/msgbuf.rs:1-180` (whole file; real logic ends at line 105, tests from 106)
- **What it does:** A generic, allocation-reused buffer of `(bytes range, per-message metadata)` pairs backing every `FramedEncoder` implementation (syslog/statsd/graphite/collectd output) — one contiguous `Vec<u8>` plus a `Vec<Range<usize>>` plus a `Vec<M>`, cleared-not-freed between batches. No parsing, no untrusted input handled directly (it's a pure output-side accumulator fed already-encoded bytes by each codec).
- **Why sensitive:** hot-path (every framed encoder's per-batch buffer) — otherwise minimal; not untrusted-input-facing itself (the untrusted-input risk lives in whichever codec's encoder decided what bytes to push, not in this generic container), no unsafe, no casts, no recursion.
- **Invariants to verify:**
  - `clear()` genuinely keeps backing capacity across calls (this is a *performance* invariant pinned by `crates/logit-bench/tests/allocations.rs`'s exact allocation-count assertions per AGENTS.md, not a correctness one) — confirmed directly by this file's own `clear_forgets_messages_but_keeps_capacity` test (msgbuf.rs:146-166)
  - `iter`/`iter_with` never desync `ranges`/`meta` (they're pushed in lockstep in `push_with`, msgbuf.rs:60-65 — a single function is the only writer of both, so desync would require a bug local to this one four-line function)
- **Observed concerns (unverified):** none — this is about as low-risk as generic container code gets; included in this survey only because the task brief named it explicitly, not because reading it surfaced anything.
- **Existing coverage:** the file's own test module (msgbuf.rs:106-180) covers push-order, meta-pairing, empty-buffer behavior, capacity retention across `clear()`, and confirms `MessageBuf<()>`'s zero-allocation property. ADR `framed-encoder.md`.
- **Suggested verification approach:** none needed beyond what exists; not worth further verification time relative to everything else in this survey.
- **Priority:** P2 — trivial, well-tested, not itself untrusted-input-facing; lowest priority in this entire survey.

---

### CODEC-16 — OTLP/JSON `AnyValue` decode — unbounded recursion on attacker-controlled nesting
- **Location:** `crates/logit-proto/src/otlp/json/mod.rs:309-347` (`any_value`, recursing into itself once per `arrayValue`/`kvlistValue` nesting level via `array_field`/`key_values`), `:250-291` (`hex_bytes`/`hex_decode`/`base64_bytes`, the sibling bytes-field decoders in the same file); entry points `crates/logit-proto/src/otlp/json/{logs,metrics,traces}.rs:11-14` (`serde_json::from_slice::<JsonValue>(bytes)`)
- **What it does:** OTLP's `AnyValue` protobuf message is recursive by design (a value can itself be an array or map of more `AnyValue`s). This hand-written JSON decoder — chosen over a `pbjson`-generated one specifically because OTLP's JSON deviates from proto3 JSON's bytes-as-base64 rule for trace/span ids (ADR `otlp-json-decoding`) — mirrors that recursive shape as ordinary Rust function-call recursion: `any_value` calls itself once per array element and once per kvlist entry, for every level of nesting a sender's JSON declares, with **no explicit depth counter or cap anywhere in this file**.
- **Why sensitive:** untrusted-input (an `otlp_in` HTTP/JSON request body from any network sender, potentially unauthenticated), custom (hand-rolled per ADR, not generated), recursion (the exact "recursion depth" risk category this survey's brief calls out by name), and — the key comparison point — this codebase demonstrably already treats exactly this bug class as serious elsewhere: `crates/logit-proto/tests/robustness.rs` has a dedicated `decode_batch_rejects_value_nesting_past_the_depth_cap` test for the native wire format's recursive `Value::Array`/`Value::Map`, and `graphite::pickle` enforces `MAX_PICKLE_DEPTH` on open `MARK`s for the identical reason — both exist specifically to stop a crafted, deeply-nested payload from stack-overflowing the process. `any_value` has no analogous cap of its own.
- **Invariants to verify:**
  - whether `serde_json::from_slice::<serde_json::Value>` (the very first parse, before `any_value` ever runs) has built-in recursion-depth protection that transitively bounds how deep the resulting `Value` tree — and therefore `any_value`'s own recursion over it — can ever be. `serde_json` is understood (and [`docs/known-gaps.md`](../known-gaps.md#http-access-logs-nginx-haproxy-and-http_access)'s own unrelated HAProxy CBOR entry independently states the same belief: "`json`'s `serde_json`-based [reader] inherits [a recursion bound] for free") to enforce a default recursion limit; this survey confirmed no `unbounded_depth`/`disable_recursion_limit` feature is enabled anywhere in the workspace's `Cargo.toml` files, which is consistent with that protection being intact — **but this was reasoned about, not empirically reproduced with an actual deeply-nested payload against a running `otlp_in` listener**, and is exactly the kind of assumption a verification session should confirm by construction rather than inherit on faith.
  - if the `serde_json` limit does hold, what depth it actually permits (commonly cited as ~128) and whether that's small enough to guarantee no stack overflow regardless of available stack size, or merely small enough in practice today.
  - whether the equivalent *protobuf*-path `AnyValue` decoding (`crates/logit-proto/src/otlp/common.rs`, not read in depth in this pass) has the same recursive shape with no depth cap — `prost`'s own generated decode may or may not impose a limit independently of anything in this crate; if it doesn't, the protobuf path could be exposed even if the JSON path is protected by `serde_json`'s limit, since the two paths are decoded by entirely different code.
  - `hex_bytes`/`hex_decode` (mod.rs:250-280), read alongside this entry: correctly rejects odd-length hex, non-hex-digit bytes, and any length not exactly matching `expected_len` (16 for trace ids, 8 for span ids), with no out-of-bounds indexing (`hex_decode`'s `bytes[i+1]` access is safe because the odd-length check precedes it and the loop advances by exactly 2) — a clean contrast case showing this file's *other* logic is carefully bounds-checked, which sharpens rather than dulls the concern about the one recursive function having no analogous cap.
- **Observed concerns (unverified but high-confidence as a real gap in defense-in-depth):** the missing local depth cap is real and directly confirmed by reading `any_value`; whether it is *currently exploitable* hinges entirely on the unverified `serde_json` assumption above. If that assumption ever stops holding (a `serde_json` version/feature change, or a future switch to a streaming/SAX-style JSON parser that builds `AnyValue` incrementally instead of via a full `Value` tree), there would be nothing in this crate's own code to catch it — unlike the native format and graphite/pickle, which both defend themselves locally rather than relying on an upstream crate's internal limit.
- **Existing coverage:** `crates/logit-proto/src/otlp/json/{mod,logs,metrics,traces}.rs` each have unit tests, but no recursion/depth-specific test was located for this path (unlike the native wire format, which `robustness.rs` explicitly covers for over-depth nesting). `crates/logit-proto/tests/otlp_fixed_point.rs` is round-trip-focused, not adversarial-input-focused. No `robustness.rs` section exists for OTLP at all. Governed by ADR `otlp-json-decoding`.
- **Suggested verification approach:** (1) first and cheapest: construct a deeply nested (e.g. 10,000–100,000-level) `{"arrayValue":{"values":[{"arrayValue":{"values":[...` JSON payload, well under any configured request-body size cap, and feed it through `otlp_in`'s actual HTTP/JSON path (ideally under a debug build with a deliberately small thread stack to make a real stack overflow reproducible rather than merely plausible) to empirically settle whether `serde_json`'s limit actually protects this code today; (2) if it does not protect it, add an explicit depth counter threaded through `any_value`/`array_field`/`key_values`, mirroring `MAX_PICKLE_DEPTH`'s pattern, with a `CodecError::Malformed` past a small cap (64–128 levels — no real OTLP producer nests attributes anywhere near that deep); (3) either way, add a `robustness.rs`-style depth-cap test for this path so the answer stays pinned regardless of `serde_json`'s own behavior; (4) separately check the protobuf-path `AnyValue` decode in `common.rs` for the same gap, since it's a different code path entirely.
- **Priority:** P0 (provisional pending the empirical check above) — untrusted network input, fully custom recursive decoding logic, and a directly analogous bug class this exact codebase already fuzzes and caps in two sibling codecs (native `decode_batch`, graphite `pickle`) but has not applied here; downgrade to P2 the moment the `serde_json`-recursion-limit assumption is empirically confirmed to hold and a defense-in-depth cap is judged unnecessary, but until that test is run this should be treated as a live, unverified DoS candidate rather than a stylistic nitpick.

### CODEC-17 — OTLP decode — unguarded `u64 as i64` timestamp cast on every wire timestamp field (logs/traces/metrics)
- **Location:** `crates/logit-proto/src/otlp/logs.rs:256-259` (`decode_log_record`'s `time_unix_nano`/`observed_time_unix_nano`), `:276`; `crates/logit-proto/src/otlp/traces.rs:232,236` (`start_time_unix_nano`/`end_time_unix_nano` in span decode); `crates/logit-proto/src/otlp/metrics.rs:161,430,435,445,451,464,470,490,492,507,508` (every data-point/exemplar `time_unix_nano`/`start_time_unix_nano`)
- **What it does:** Every OTLP timestamp field is a wire `fixed64`/`uint64` nanoseconds-since-epoch. Decode converts each to this model's `i64`-nanosecond `Event::timestamp`/`observed_timestamp`/span/exemplar timestamps via a bare `as i64` cast, with **no range check anywhere in this pass of any of the three files** — confirmed by reading `decode_log_record`, `decode_span`, and every metrics data-point branch that reads a `_time_unix_nano` field.
- **Why sensitive:** untrusted-input (every field here comes straight off an OTLP/HTTP or OTLP/gRPC request body — protobuf `fixed64`, or a JSON number/string per `otlp/json`), integer-overflow/truncating-cast (a `u64` with the high bit set — i.e. any timestamp at or past 2262-04-11T00:12:43.685477580Z, `i64::MAX` nanoseconds since epoch — reinterprets as a large **negative** `i64` on the `as` cast, not a saturating clamp: Rust's `as` between same-width integer types of different signedness is a bit-pattern reinterpretation, never a saturate), lossless-roundtrip / data-loss (the corrupted timestamp is silent: no diagnostic, no counter, no rejection — it flows straight into `Event::log`/`Event::span`/`Event::metric`, none of which validate their `timestamp: i64` argument, per `crates/logit-core/src/event.rs:199-221`).
- **Invariants to verify:**
  - construct an OTLP payload (protobuf or JSON, either is equally affected since JSON's `u64_field`/`parse_u64` in `otlp/json/mod.rs` also just parses to a plain `u64`) with a `time_unix_nano` at or above `i64::MAX + 1` (`9_223_372_036_854_775_808`) and confirm the resulting `Event::timestamp` is indeed a large negative number (reproducing the bug) rather than being rejected, clamped, or saturated.
  - check every other `as i64` timestamp-cast site enumerated above for the same gap — this is not one isolated line, it's the same pattern repeated across logs/traces/metrics with no shared helper, so a partial fix (e.g. only in `logs.rs`) would leave the others silently vulnerable.
  - compare against `crates/logit-inputs/src/syslog.rs`'s `parse_5424`'s explicit `TimestampError::OutOfRange` handling (this survey's syslog entry above) — that code path already recognizes exactly this failure mode for RFC 5424 TIMESTAMPs and responds with "omit the field, throttled diagnostic, keep the rest of the record" rather than a silent wraparound; the OTLP path has no equivalent.
  - confirm whether any *downstream* consumer (e.g. `aggregate`'s window bucketing, `influxdb_out`'s point timestamp) would itself panic, misbehave, or silently reorder data given a wildly out-of-range negative timestamp reaching it — i.e. whether the blast radius is "one event has a wrong timestamp" or something wider (e.g. a window keyed on timestamp behaving pathologically for one poisoned event mixed with normal ones).
- **Observed concerns (unverified -> now verified as a real gap):** this is not a hypothetical — it was directly confirmed by reading `decode_log_record` (logs.rs:256-259: `if record.time_unix_nano != 0 { record.time_unix_nano as i64 } else { ... }` with no bounds check), and the equivalent pattern repeats verbatim in `traces.rs` and `metrics.rs`. Confidence: high that the cast is unguarded as read; moderate on real-world exploitability/severity, since it requires an attacker or misbehaving sender to emit a timestamp value that is already nonsensical (>287 years in the future) for it to trigger, and the consequence is a wrong timestamp on one event rather than a crash or unbounded resource use. Checked `docs/known-gaps.md` for an existing acknowledgment of this specific gap — found none; the file does document the *general* principle that receipt-time vs. sender-time handling needs care (syslog's, elsewhere), but nothing calling out OTLP's timestamp casts by name.
- **Existing coverage:** `crates/logit-proto/src/otlp/logs.rs:282+`, `traces.rs:239+`, `metrics.rs:543+` unit tests exist but (based on the test names visible via grep — `unwrap()`-heavy round-trip assertions) appear focused on the normal-range/round-trip cases, not adversarial out-of-range timestamps; no test constructing a `time_unix_nano` >= `i64::MAX` was found. `crates/logit-proto/tests/otlp_fixed_point.rs` likely covers round-trip fixed points for realistic values only (not independently re-read for this specific edge case). No `robustness.rs` coverage for OTLP at all (that file's own module doc doesn't name it). Governed by ADR `otlp-json-decoding`, `docs/design/data-model.md`; `docs/known-gaps.md`'s timestamp-precedence entries address a related-but-distinct concern (receipt vs. sender time) and do not cover this.
- **Suggested verification approach:** a unit test per file (logs/traces/metrics) constructing a wire message with `time_unix_nano = u64::MAX` (and `i64::MAX + 1` exactly, the boundary) and asserting the decoded `Event`'s timestamp is *not* silently negative — then decide and implement the actual desired behavior (reject the record, clamp to `i64::MAX`, or omit the field with a diagnostic, mirroring syslog's `OutOfRange` handling) and apply it uniformly across all enumerated call sites, ideally through one shared helper (`fn wire_time_to_nanos(u64) -> Option<i64>` or similar) rather than three independently-repeated bare casts.
- **Priority:** P1 — genuine, verified, previously-unflagged silent-data-corruption bug reachable from fully untrusted network input (OTLP/HTTP and OTLP/gRPC both accept arbitrary `fixed64` timestamps), inconsistent with the project's own established, more careful handling of the identical failure mode elsewhere (syslog); not P0 only because the trigger condition (a timestamp implying a date past 2262) is unusual enough that it's far more likely to surface from a buggy sender's garbage value than a deliberate attack, and the consequence is corrupted metadata on affected records rather than a crash or resource exhaustion.

---

### CODEC — Cross-cutting notes

- **Consistent quality signal:** every codec read in depth across both survey passes (graphite's pickle reader/decoder/encoder, statsd's decoder/encoder, syslog's PRI/timestamp/structured-data parser, collectd's TLV decoder/encoder) shows the same disciplined pattern: a single length-check choke point per parser, explicit `.get()`-based bounds checks instead of raw indexing, casts guarded by prior range checks, and extremely thorough doc comments that pre-argue the safety case. Prometheus, OTLP, and InfluxDB/msgbuf (below) confirm the same pattern continues to hold there too.
- **The survey brief's `unsafe` claim for graphite/pickle.rs is incorrect** — verified via grep and git log; no `unsafe` exists or ever existed in that module, nor anywhere else in this survey's scope (`crates/logit-proto/src/{statsd,syslog,collectd,graphite,prometheus,otlp}`, `crates/logit-outputs/src/{statsd,syslog,collectd,graphite,influxdb,otlp,prometheus}.rs`) — confirmed by a repo-wide grep restricted to these paths. The only `unsafe` in the two crates that touch this survey's protocols at all lives in `crates/logit-inputs/src/tail/watch.rs` and `crates/logit-inputs/src/udp.rs`, both listener/socket glue explicitly out of this survey's scope.
- **Repeated hand-rolled pattern worth tracking as one item, not many:** the raw-pointer-subtraction `slice_of` "reconstruct a zero-copy `Bytes` from a `&str`/byte-slice" trick appears near-identically in `statsd.rs`, `syslog.rs`, and `graphite/decode.rs` (each with its own independent implementation). It's sound today (every caller only ever slices a substring that was itself validated out of the original `Bytes`), but it's a single conceptual invariant spread across three files with no shared helper enforcing it — a future refactor in any one of them (e.g. introducing an owned/copied substring by mistake) would silently produce unsound pointer arithmetic with no compiler error. Worth one shared, tested utility rather than three independently-trusted copies, or at minimum one cross-file test that would catch a violation.
- **collectd was independently read line-by-line in this pass** (superseding an earlier placeholder from a concurrent pass of this same file) and found to be **exceptionally well-guarded**, with dedicated robustness/fuzz coverage (truncation, bit-flip, and a hostile-count allocation-DoS regression test) matching or exceeding graphite's. Prometheus, OTLP, and InfluxDB/msgbuf entries below are likewise freshly read, not placeholders.
- **Neither statsd nor syslog has a `crates/logit-proto/tests/robustness.rs` section** (confirmed by direct grep: only `native`, `control`, `graphite`, and (as of this pass) implicitly `collectd`-adjacent decoders are exercised there) — this is a genuine, real coverage gap for exactly the two protocols with the most attacker-facing custom parsing surface (DogStatsD's length-prefixed event grammar, syslog's STRUCTURED-DATA parser), not a false alarm. This is this survey's single strongest concrete recommendation: port `robustness.rs`'s truncation/bit-flip/hostile-length-counter harness to `StatsdDecoder`/`SyslogDecoder` before anything else in this list. Prometheus (text/OpenMetrics and remote-write) has no `robustness.rs` section either, per that entry's own grep — the harness's coverage stops at `native`/`control`/`graphite`/`collectd`, i.e. exactly the two oldest wire codecs plus the native format; every codec added since (statsd having predated the harness, syslog, prometheus, otlp) has none. This is a broader, repo-wide gap than any single codec entry captures on its own.
- **Two genuinely new, previously-undocumented findings surfaced in this pass, both in OTLP:** (1) `any_value`'s JSON-decode recursion (`crates/logit-proto/src/otlp/json/mod.rs:309-347`) has no *local* depth bound, unlike the native wire format's `decode_batch` and graphite's `pickle` reader, both of which cap recursion/nesting explicitly — this one relies entirely on `serde_json`'s implicit limit as an unverified backstop, and is a live, provisional P0 pending a one-hour empirical check (send a deeply nested payload, see if it crashes); (2) every OTLP timestamp field's `u64 -> i64` decode cast (`logs.rs`/`metrics.rs`/`traces.rs`, ten-plus call sites) silently wraps to a negative timestamp for any wire value `>= 2^63`, with the *encode* side's `.max(0)` guard confirming the decode side's missing symmetric guard is an asymmetry rather than an intentional design (P1 — silent corruption, not a crash). Neither is in `docs/known-gaps.md`. Both are worth a follow-up session's first attention precisely because they're concrete and testable in under an hour each, unlike most of this survey's "reads fine, wants more fuzzing" findings.


---

## CORE — Core event model and LuaJIT embedding

Area survey for later deep-dive verification sessions. Read-only pass; nothing built or run.
Line numbers verified against the worktree at
`/home/ross/lib/logit/.claude/worktrees/vectorized-sparking-origami` (branch `main`, `2f387ee`).

Third-party crates in play here: `lasso` 0.7 (`multi-threaded` → `ThreadedRodeo`),
`sketches-ddsketch` 0.4, `cardinality-estimator` 1.0.3 (`with_serde`), `smallvec` 1 (`union`),
`bytes`, `serde` (traits only, no data format), `tracing`/`tracing-subscriber` (`Layer` only),
`mlua` 0.9 (`luajit`, `vendored`). No `ahash`/`hashbrown` in either crate — the interner is `lasso`,
the telemetry buffers are `std::collections::HashMap`.

---

### CORE-01 — Process-wide symbol interner: unbounded growth and per-call shard contention
- **Location:** `crates/logit-core/src/interner.rs:12-51` (`Symbol`, `INTERNER: OnceLock<ThreadedRodeo>`, `intern`, `resolve`, `lookup`, `len`); consumers at `crates/logit-core/src/attrs.rs:26-75`, `crates/logit-core/src/provenance.rs:30-37`, `crates/logit-script/src/telemetry.rs:40-42`
- **What it does:** One process-global `lasso::ThreadedRodeo` maps every attribute key, metric name, unit, description and `event_name` to a `Copy` 4-byte `Spur`. `resolve` returns `&'static str` and **panics** on a symbol the table doesn't hold. `lookup` is the non-interning probe `AttrMap::get`/`remove` use so a miss can't grow the table.
- **Why sensitive:** hot-path (every attribute key on every event), concurrency (a sharded `DashMap`-style lock touched by every pipeline worker), unbounded-growth (`ThreadedRodeo` never evicts, ~94-124 B per distinct string, for the life of the process), untrusted-input (keys come from the wire for `json`, syslog SD, OTLP, statsd/collectd names).
- **Invariants to verify:**
  - `intern(s)` on an already-present string allocates nothing and returns the identical `Symbol` (the premise the whole growth argument rests on).
  - `resolve` is only ever reached with symbols this process minted — no `Symbol` ever crosses a process boundary as a raw integer (check the native wire dictionary and disk spool paths especially).
  - `lookup` really is non-interning in `lasso` 0.7 (`ThreadedRodeo::get`), so `AttrMap::get` on an absent key cannot grow the table.
  - `Symbol` ordering (`Spur`'s `Ord`) is *insertion* order, not lexical — anything relying on `AttrMap`'s "sorted" iteration for a stable *wire* ordering across processes is relying on the wrong thing.
  - `interner::len()` is the only growth observable (sampled by `internal`'s `logit.process.interner.strings`); confirm it is actually wired and cheap.
- **Observed concerns (unverified):**
  - `resolve` panicking (`interner.rs:31-33`) plus `Symbol: Copy` means the documented retrofit cost is real; nothing enforces the "symbols are eternal" premise at a type level. Medium confidence this is exactly as documented, not worse.
  - Sorted-`Symbol` iteration order being insertion-order-dependent is stated as "deterministic" in `attrs.rs:85-88`; it is deterministic *within* a process run but not across two processes that interned in different orders. Worth confirming no codec's fixed-point property (lossless-transit) depends on it. Medium confidence this is fine (codecs sort by name), but it is not argued anywhere I found.
- **Existing coverage:** `crates/logit-core/src/interner.rs:148-222` (5 unit tests, all `KeyCache`-focused); `crates/logit-core/tests/type_sizes.rs:29` (`symbol_is_a_niche_optimized_u32`). Governed by `docs/design/memory.md` §4 and [`docs/known-gaps.md`](../known-gaps.md#event-model-and-interner) ("The attribute/metric-name interner never frees" — documented, accepted, not a surprise).
- **Suggested verification approach:** targeted review of every `resolve` call site for a symbol that could have come from outside this process; a long-run soak (`json`/`otlp_in` against high-key-cardinality input) watching `interner::len()` and RSS; a contention microbench of `intern` across N worker threads.
- **Priority:** P1 — documented and accepted gap, but it is global mutable state on the hottest path and the panic-on-unknown-symbol contract is load-bearing.

---

### CORE-02 — `KeyCache`: hand-rolled cursor-scan memo in front of the interner
- **Location:** `crates/logit-core/src/interner.rs:86-146` (`KeyCache`, `MAX_ENTRIES = 64`, `MAX_KEY_LEN = 128`, `get_or_intern`)
- **What it does:** A per-component `Vec<(Box<str>, Symbol)>` kept in first-seen order with a cursor. The steady path compares the entry at the cursor (`memcmp`, no hashing); a miss scans forward with wraparound; a genuine miss falls through to `intern` and is cached if under both caps. Cloned per accepted TCP connection.
- **Why sensitive:** hot-path (one lookup per key per event on `json`/`csv`/`kv` parse), custom (not a hash map — a hand-rolled ordered scan with modular wraparound), untrusted-input (keys come from the parsed document), unbounded-growth (bounded deliberately, and the bound is the whole defense).
- **Invariants to verify:**
  - `cache.get_or_intern(s) == intern(s)` for every `s`, always — including after the cap is hit and for over-long keys (the stated contract, line 80).
  - The wraparound scan (`interner.rs:122-128`, `(start + offset) % len`) visits every entry exactly once and never misses a present key.
  - `self.cursor = len + 1` on the insert path (`interner.rs:133`) can exceed `len` — the `start` clamp at line 115 is what makes that safe; verify no other reader of `cursor` exists.
  - Once at `MAX_ENTRIES`, a never-seen key pays a full 64-entry scan before `intern` — confirm that worst case is acceptable for an adversarial key stream (every key distinct).
  - A cloned cache (per connection) carries entries that remain valid `Symbol`s forever.
- **Observed concerns (unverified):** none spotted. The cap, the length cap, and the "never evicts" choice are all argued in-file.
- **Existing coverage:** `crates/logit-core/src/interner.rs:152-221` — five tests covering agreement with `intern`, zero-intern repeat passes, wraparound resync, cap behaviour, and over-long keys. Governed by `docs/design/memory.md` §4 and `docs/design/performance.md` (the `json-parse` scenario that motivated it).
- **Suggested verification approach:** proptest `get_or_intern` against `intern` over random key sequences including reordering, absences, duplicates, cap overflow and >128-byte keys; a worst-case microbench with 100% distinct keys.
- **Priority:** P1 — custom data structure on the parse hot path; a wrong answer here is a silently mis-keyed attribute, not a crash.

---

### CORE-03 — `AttrMap`: sorted inline `SmallVec` and the resource⊕event merge-join
- **Location:** `crates/logit-core/src/attrs.rs:16-89` (`AttrMap`, `INLINE_CAPACITY = 8`, `get`/`get_sym`/`insert`/`insert_sym`/`remove`/`remove_sym`/`iter`), `crates/logit-core/src/attrs.rs:112-134` (`merged`)
- **What it does:** Attributes are a `SmallVec<[(Symbol, Value); 8]>` kept sorted by `Symbol`, with `binary_search_by_key` for every access and an `insert`-shifting write. `merged` walks a `Resource`'s and an `Event`'s maps in lockstep, the event's value winning on an equal key, without cloning either map.
- **Why sensitive:** hot-path (every attribute read/write, and `merged` runs once per event in `influxdb_out`, `statsd_out` and the Prometheus codec), custom (hand-rolled sorted vector + hand-rolled merge-join), accounting (the merge must yield exactly what a clone-and-override would).
- **Invariants to verify:**
  - The vector is sorted by `Symbol` after every operation, including `insert_sym` on an existing key (overwrite, no reorder) and `remove_sym`.
  - `merged` emits each key exactly once, in ascending `Symbol` order, event value winning on ties — equivalent to `resource.clone().extend(event)` for every input pair, including empty/one-sided maps.
  - `get`/`remove`'s `lookup`-first shortcut is sound: a string never interned cannot be a key (relies on interning being monotonic and never evicting — i.e. coupled to the interner entry above).
  - `insert` at `INLINE_CAPACITY` spills correctly and iteration order is unchanged across the spill (`type_sizes.rs:60` pins the size either way).
  - `Value::Map(Box<AttrMap>)` recursion does not make `PartialEq`/`Clone` unexpectedly deep-expensive on a hot path.
- **Observed concerns (unverified):** `merged` is a `std::iter::from_fn` over two `Peekable`s (`attrs.rs:118-133`); the tie branch advances both but returns `event_attrs.next()` — correct, but the resource value is dropped silently and nothing asserts the two maps were individually sorted to begin with. Low confidence this is a real bug; it is a good proptest target.
- **Existing coverage:** `crates/logit-core/src/attrs.rs:136+` unit tests; `crates/logit-core/tests/type_sizes.rs:60` (`attr_map_pays_its_inline_capacity_whether_or_not_it_spills`); allocation pins throughout `crates/logit-bench/tests/allocations.rs`. Governed by `docs/design/data-model.md` and `docs/design/memory.md`.
- **Suggested verification approach:** proptest `merged` against a naive clone-and-override reference over random key/value pairs; proptest the sortedness invariant over random insert/remove sequences.
- **Priority:** P1 — hot path and custom, but the failure mode is a wrong/duplicated tag rather than unsoundness.

---

### CORE-04 — `estimated_heap_bytes`: the admission-control accounting that must reconcile
- **Location:** `crates/logit-core/src/event.rs:53-61` (`EventBatch::estimated_heap_bytes`), `:71-176` (`span_heap_bytes`, `attr_map_heap_bytes`, `value_heap_bytes`, `metric_record_heap_bytes`, `exp_histogram_heap_bytes`, `ESTIMATED_DISTRIBUTION_HEAP_BYTES = 512`), `:238-251` (`Event::estimated_heap_bytes`), `crates/logit-core/src/resource.rs:19-22,43-48`
- **What it does:** An O(events) walk producing the byte figure that bounds in-memory sink queues and the batch accumulator. Deliberately approximate: interned symbols are excluded on purpose (an earlier version's `resolve`-per-key walk was ~30% of `json-parse` samples), and a `Distribution` is a flat 512-byte guess.
- **Why sensitive:** hot-path (runs on every queue push), accounting (`Event::estimated_heap_bytes` summed over a set plus the batch-level terms must reproduce `EventBatch::estimated_heap_bytes` *exactly*, by construction — `logit_pipeline::BatchAccumulator` depends on it), unbounded-growth (an undercount here is how a bounded queue stops being bounded).
- **Invariants to verify:**
  - Σ `Event::estimated_heap_bytes` + `resource` + `scope` + `events.capacity() * size_of::<Event>()` == `EventBatch::estimated_heap_bytes`, for every batch shape (no metrics, spilled attrs, spans with events/links, exemplars).
  - No path is O(events²) or triggers an interner probe (the regression that was removed).
  - `value_heap_bytes` recursion over `Array`/`Map` terminates and is bounded in practice (see the nesting-depth concern in the Lua conversion entry below — a script-built `Value` can nest arbitrarily deep, and this walk recurses without a depth limit: `event.rs:118-130`).
  - The `Samples` "only counts if spilled" rule (`event.rs:156-162`) and the `Distribution` constant don't systematically undercount a sketch-heavy workload.
- **Observed concerns (unverified):**
  - `value_heap_bytes` (`event.rs:118-130`) and `attr_map_heap_bytes` are mutually recursive with no depth cap, and they run on the queue-push path. Combined with `lua_to_value`'s unbounded nesting (below), a deep `Value::Map` chain is a stack-overflow vector reached from ordinary admission control, not just from Lua. Medium confidence this is reachable; low confidence anyone would hit it accidentally.
  - `ESTIMATED_DISTRIBUTION_HEAP_BYTES = 512` (`event.rs:106`) is explicitly "a guess, not a measurement" — a `Distribution`-heavy queue's real footprint is unpinned.
- **Existing coverage:** `crates/logit-core/src/event.rs:254+` unit tests; the whole of `crates/logit-bench/tests/allocations.rs` for the adjacent allocation discipline. Explicitly *exempted* from the exact-size/exact-allocation discipline (`event.rs:26-27`). Governed by `docs/adr/buffered-sink-delivery.md`, `docs/design/memory.md`.
- **Suggested verification approach:** proptest the sum-equals-whole identity over generated batches; measure a `Distribution`-heavy batch's true RSS against the 512-byte constant.
- **Priority:** P2 — approximate by design and not a correctness surface, except for the reconciliation identity and the recursion depth.

---

### CORE-05 — `DdSketch` wrapper: `merge` panics on a config mismatch reachable from the wire
- **Location:** `crates/logit-core/src/metric.rs:302-410` (`DdSketch`, `new`, `add`, `add_weighted`, `merge`, `quantile`, `count`, `sum`, `to_java_bytes`, `from_java_bytes`, `PartialEq` via bytes); decode call site `crates/logit-proto/src/native/record.rs:454-458`; merge call sites `crates/logit-transforms/src/aggregate.rs:659,671`
- **What it does:** Thin newtype over `sketches_ddsketch::DDSketch` built with `Config::defaults()`. `add_weighted` delegates to `add_with_count` (O(1) in the weight, which is attacker-influenced). `merge` **`.expect()`s** on the inner `Result`, on the stated premise that every sketch in the codebase uses the default config. `PartialEq` compares `to_java_bytes()` output because the crate exposes no bin iteration.
- **Why sensitive:** numeric (relative-error bound, merge associativity/commutativity, exact `sum` alongside approximate quantiles), untrusted-input, data-loss, nontrivial-3p-use(`sketches-ddsketch`) — the java-bytes blob is the *only* lossless in/out path, so the wire format is pinned to it.
- **Invariants to verify:**
  - **The `.expect` at `metric.rs:354` cannot fire.** `DdSketch::from_java_bytes` (`metric.rs:401-403`) is reachable from `logit_proto::native` decode (`record.rs:455`) — i.e. from a `logit_in` peer or a replayed disk-spool file — and from nothing else in-process. If `sketches_ddsketch::DDSketch::from_java_bytes` reconstructs the sketch's *encoded* config rather than forcing `Config::defaults()`, a peer sending a blob with a different relative accuracy produces a sketch whose later `merge` in `aggregate` **panics the node**. Verify by reading the pinned crate's `from_java_bytes`/`merge` source.
  - `merge` is associative and commutative to within the documented error bound; merging is exact for `sum` and `count`.
  - `add_weighted(v, 0)` is a no-op and `add_weighted(v, n)` equals `n` calls to `add` in count, sum and every quantile.
  - `to_java_bytes` is a fixed point across `from_java_bytes` (the `PartialEq` impl's correctness depends on it), and `PartialEq` isn't accidentally load-bearing anywhere that needs *logical* equality rather than byte equality (it is insertion-history dependent).
  - `sum()` returning `0.0` for an empty sketch (`metric.rs:383-385`) is never confused with "no observations" by a caller that needs `count()` instead.
- **Observed concerns (unverified):**
  - The `merge` panic above. High confidence the reasoning in the doc comment (`metric.rs:350-355`, "the mismatched-config failure case can't-actually-happen") does **not** account for the `from_java_bytes` decode path; medium-to-high confidence it is genuinely reachable, contingent on what the upstream decoder does with the config. This is the single most actionable item in this area.
  - `from_java_bytes`'s doc (`metric.rs:397-400`) says it "Fails only on a genuinely malformed blob" — it says nothing about a well-formed blob with a non-default config, which is exactly the gap.
- **Existing coverage:** `crates/logit-core/src/metric.rs:1223-1338` — `add_weighted` × {1, large, 0}, the relative-error-bound test, `sum` exactness across merge and across a java-bytes round trip, and the `PartialEq`-via-bytes test. No test constructs a non-default-config blob. Governed by `docs/design/data-model.md` ("metric kinds must stay mergeable"), `docs/adr/aggregation-window-semantics.md`, [`docs/known-gaps.md`](../known-gaps.md#statsd) ("Sample-rate extrapolation").
- **Suggested verification approach:** read the pinned `sketches-ddsketch` 0.4 source for `from_java_bytes`/`merge`/`Config`; if the config round-trips, write a test that decodes a foreign-config blob and merges it, then change `merge` to return a `Result` (or force a re-sketch on decode). Separately, proptest merge associativity/commutativity and the error bound.
- **Priority:** **P0** — a reachable panic on the main data path from peer-supplied bytes, in custom glue whose safety argument has a hole.

---

### CORE-06 — `HyperLogLog`: hand-rolled serde byte codec working around an upstream allocation-layout UB
- **Location:** `crates/logit-core/src/metric.rs:412-516` (type docs, `insert`, `merge`, `estimate`, `heap_bytes`, `to_bytes` incl. the `data`-word canonicalization at `:499-504`, `from_bytes`), `:538-556` (`PartialEq`), `:558-585` (`HllDecodeError`), `:587-777` (`HllBytesWriter` `Serializer`), `:779-802` (mirrored upstream constants `CE_REPRESENTATION_ARRAY/_HLL`, `CE_ARRAY_MAX_CAPACITY = 128`, `CE_HLL_SLICE_LEN = 771`), `:804-922` (`HllBytesReader` + the UB explanation), `:855-888` (`validate_members_len`), `:924-956` (`HllSeqAccess`, `size_hint` → `cap`), `:958-1162` (`Deserializer`, notably `deserialize_u64` stashing the tag at `:1015-1025` and `deserialize_seq` at `:1098-1110`)
- **What it does:** Wraps `cardinality_estimator::CardinalityEstimator<[u8]>` and drives its `(u64, Option<Vec<u32>>)` serde shape through a purpose-built byte `Serializer`/`Deserializer` pair. `to_bytes` masks the volatile pointer bits out of the leading `data` word so two logically-equal estimators serialize identically. `from_bytes` reports a **rounded** `size_hint` so serde's blanket `Vec<T>` deserializer allocates exactly the capacity the upstream crate will later free — without which `Array::from_vec`'s `mem::forget` + `Box::from_raw`-with-a-rounded-length frees the wrong `Layout` (undefined behavior), and bounds the claimed member count before any allocation.
- **Why sensitive:** unsafe/ffi-adjacent (avoiding UB in a dependency's `unsafe` by construction, from *our* side), untrusted-input (blobs arrive over `logit_in` and from the disk spool), numeric (HLL union semantics, estimate accuracy), nontrivial-3p-use(`cardinality-estimator`) — the byte format is pinned to exactly version 1.0.3's `serde.rs` and to two `pub(crate)` constants mirrored by hand.
- **Invariants to verify:**
  - `deserialize_seq`'s `size_hint` is *the rounded capacity*, never `len` (`metric.rs:1108-1109`, `validate_members_len:874,882`) — the soundness of the whole codec rests on this one value and the doc says so explicitly.
  - `tag` is always set before `deserialize_seq` runs (`metric.rs:846-852` claims the outer tuple always decodes `data` first). A crafted blob cannot reach `deserialize_seq` with `tag == None` — `validate_members_len`'s `other` arm rejects it, so verify the rejection, not just the claim.
  - `CE_HLL_SLICE_LEN = 771` and `CE_ARRAY_MAX_CAPACITY = 128` still match upstream for the pinned version and the default `P = 12, W = 6` (there is a test that recomputes the former).
  - `serde`'s blanket `Vec<T>` deserializer in the resolved `serde` version really does call `Vec::with_capacity(seq.size_hint())` before reading elements — a serde upgrade that changes this silently reinstates the UB.
  - `to_bytes`'s canonicalization (`metric.rs:499-504`) only ever clears bits the decoder discards, for both the array and HLL representations, and never touches the "small" representation where `data` *is* the content.
  - `merge` is a true union (idempotent, commutative, associative) and `estimate` stays within the crate's stated error for realistic cardinalities.
  - `from_bytes` on any byte string — truncated, oversized, wrong tag, absurd length — fails cleanly with `HllDecodeError` and never allocates unboundedly first.
- **Observed concerns (unverified):**
  - The hand-mirrored `pub(crate)` constants and the assumption about serde's allocation strategy are two independent silent-breakage vectors on a dependency bump; only the first has a guard test. Medium confidence this is the weakest link.
  - `PartialEq` is insertion-order-dependent (`metric.rs:544-551`) — documented, but if any equality assertion outside this module compares HLLs built in different orders it will be flaky. Low confidence anything does.
- **Existing coverage:** `crates/logit-core/src/metric.rs:1404-1653` — empty estimate, accuracy on 1k distinct members, merge-is-union, byte round trips, truncated input, independently-decoded byte identity, fixed point for every representation, bad representation tags, non-power-of-two member counts (the UB pinning test), over-max array count, HLL count off-by-one, and `hll_slice_len_matches_upstream_constant`. Documented in [`docs/known-gaps.md`](../known-gaps.md#event-model-and-interner).
- **Suggested verification approach:** **run the HLL tests under Miri and ASan specifically** (this is the one place in the area where UB is the documented failure mode); fuzz `from_bytes` with arbitrary bytes; proptest merge as a set-union law; add a guard test that pins serde's `with_capacity`-from-`size_hint` behaviour if one can be written.
- **Priority:** **P0** — untrusted bytes feeding a codec whose stated purpose is preventing UB in a dependency, with version-pinned constants mirrored by hand.

---

### CORE-07 — `Samples`: attacker-influenced sample-rate extrapolation
- **Location:** `crates/logit-core/src/metric.rs:178-249` (`SAMPLES_INLINE = 19`, `Samples`, `MAX_WEIGHT = 1000`, `weight`, `sketch`, `Default`)
- **What it does:** `weight()` turns a wire-supplied `sample_rate` into `round(1/rate)` clamped to `[1, 1000]`, degrading a non-finite or non-positive rate to `1` rather than `0` (because `NaN as u64 == 0` and `add_weighted(_, 0)` is a silent no-op — every observation would vanish). `sketch()` is the one place three consumers agree on how to summarize raw samples.
- **Why sensitive:** numeric (NaN/overflow handling, extrapolation), untrusted-input (`sample_rate` is a bare `f64` off the native wire with nothing validating it — stated at `metric.rs:213-215`), data-loss (the degenerate-to-1 choice is precisely a silent-drop guard), hot-path.
- **Invariants to verify:**
  - `weight()` ∈ `[1, MAX_WEIGHT]` for every `f64` input including `NaN`, `±inf`, `0.0`, `-0.0`, subnormals and `f64::MIN_POSITIVE`.
  - `sketch()` never drops a value regardless of `sample_rate`, and its count equals `values.len() * weight()`.
  - `SAMPLES_INLINE = 19` still keeps `size_of::<MetricKind>() == 176` (pinned).
  - The `MAX_WEIGHT` clamp is applied consistently with `crates/logit-inputs/src/statsd.rs`'s own `MAX_SAMPLE_WEIGHT` (the doc says W3 folded one into the other — confirm there is now one constant, not two).
- **Observed concerns (unverified):** none spotted; the NaN reasoning is explicit and tested.
- **Existing coverage:** `crates/logit-core/src/metric.rs:1348-1402` (`samples_new_defaults_sample_rate_to_one`, `samples_weight_is_never_zero_and_is_clamped`, `sketch_of_a_nan_rate_samples_still_counts_every_value`, `sketch_weights_values_by_sample_rate`); `crates/logit-core/tests/type_sizes.rs:226`. Governed by [`docs/known-gaps.md`](../known-gaps.md#statsd) ("Sample-rate extrapolation"), `docs/adr/lossless-transit.md`.
- **Suggested verification approach:** proptest `weight()` over arbitrary `f64` bit patterns.
- **Priority:** P1 — numeric edge handling on untrusted input, well argued and tested but easy to regress.

---

### CORE-08 — Telemetry component buffers: locks, bounded caps, and drop accounting
- **Location:** `crates/logit-core/src/telemetry.rs:41-99` (`Tag`, `MAX_KEYS_PER_COMPONENT = 1024`, `RESERVED_TAG_KEYS`, `MAX_SPANS_PER_COMPONENT = 512`, `MAX_LOGS_PER_COMPONENT = 256`, `MAX_LINKS_PER_SPAN = 32`), `:137-153` (`PointKey::new` — reserved-key filter then sort), `:161-221` (`Telemetry::count`/`gauge`/`timing`/`timer`/`is_enabled`), `:494-611` (`ComponentBuffer`, `push_span`, `push_log`, `upsert`), `:631-734` (`drain`), `:743-814` (`Registry`, `telemetry_for`, `push_log`, `drain`)
- **What it does:** Per-component buffers coalesce points by `(name, sorted tags)` between drains, accumulate spans and captured logs in plain `Vec`s, and report their own overflow as `logit.internal.{points,spans,logs}.dropped`. Four `std::sync::Mutex`es plus four `AtomicU64`s per component; a disabled `Telemetry` is `None` and every method is one predictable branch.
- **Why sensitive:** concurrency (shared global-ish state written from every worker task; `Mutex` poisoning tolerated via `into_inner` everywhere), unbounded-growth (three caps are the only bound on buffers that fill between drain ticks), accounting (the dropped counters must reconcile with what was actually rejected), hot-path (a live handle is touched several times per batch per node).
- **Invariants to verify:**
  - No lock is ever held across an `.await` (claimed at `telemetry.rs:599-601`); confirm across every call site, including `SpanGuard::link`'s re-entrant `Telemetry::count` (`:416-427`) which takes the `points` lock while *not* holding `spans`.
  - `drain`'s swap-and-take is atomic enough that a point recorded concurrently is either in this drain or the next, never lost and never double-counted: `points`/`spans`/`logs` are `mem::take`n under their own locks while the `*_dropped` counters are `swap`ped separately (`:631-646`) — there is a window between the two. Verify a drop counted just after its buffer was taken isn't attributed to the wrong drain in a way that breaks reconciliation.
  - Cap enforcement is exact: the `>=` checks at `:541`, `:554`, `:606` plus the reserved-key filter at `:149` mean a bounded key space regardless of caller behaviour.
  - Identity attributes (`component`/`kind`/`role`) always win, inserted last (`:667-669`, `:572-574`), and are never part of the cardinality key.
  - `Registry::telemetry_for` returning the *same* buffer for a repeated id (`:780-788`) keeps `kind`/`role` from the first registration — confirm no caller depends on the later values.
  - `upsert`'s kind-mismatch fallback (`:171-175`, `:198-205`) silently converts a gauge key into a counter (and vice versa) — confirm that's intended and that the Lua-side `logit.` prefix guard (`crates/logit-script/src/telemetry.rs:51-59`) is the only thing preventing a script from doing it to a runtime metric.
- **Observed concerns (unverified):**
  - `Registry::telemetry_for` and `Registry::push_log` both linear-scan `Vec<Arc<ComponentBuffer>>` under a `Mutex` (`:782`, `:798`). `push_log` runs on **every** captured `warn`-and-above event, so its cost is O(components) under a global lock. Low severity at realistic component counts; worth noting.
  - The `*_dropped` swap/take ordering window above. Low confidence it matters; it only skews attribution across a tick boundary.
- **Existing coverage:** `crates/logit-core/src/telemetry.rs:968+` (57 test fns in-file, including the clock-override span tests); live-telemetry allocation rows in `crates/logit-bench/tests/allocations.rs`. Governed by `docs/design/internal-telemetry.md`, `docs/adr/internal-telemetry-as-pipeline-events.md`.
- **Suggested verification approach:** targeted concurrency review plus a loom-style or multi-threaded stress test asserting drop-counter reconciliation (points recorded == points drained + points dropped) across many drains.
- **Priority:** P1 — concurrency plus accounting, bounded by design, no obvious soundness hole.

---

### CORE-09 — `TelemetryLayer`: capturing `tracing` back into the pipeline (feedback loop and field extraction)
- **Location:** `crates/logit-core/src/telemetry.rs:832-891` (`TelemetryLayer`, `ActiveTelemetryLayer`, `activate`, `capture_filter`), `:893-901` (`severity_from_level`), `:903-966` (`Layer::on_event` — target gate at `:917`, threshold at `:921`, the inline `Visitor` at `:930-950`, the two fallbacks at `:952-960`); producer side `crates/logit-core/src/diag.rs:93-181`
- **What it does:** A `tracing_subscriber::Layer` that starts inert, is `activate`d once the config's `internal` component is known, and turns `logit`-target events at or above a threshold into `PendingLog`s in that component's buffer. `capture_filter()` must be installed as a *per-layer* filter, with a static `WARN` cap, or an operator's `--log-level` would permanently disable capture at the callsite level.
- **Why sensitive:** concurrency (an `Arc<RwLock<Option<..>>>` read on every event; `activate` writes it), re-entrancy/feedback (this is `logit` logging about itself into its own pipeline — any warn emitted *from* the capture path would recurse), hot-path-adjacent (fires on every warn), accounting (`logit.internal.logs.dropped`).
- **Invariants to verify:**
  - **No feedback loop.** Nothing on the `on_event` → `Registry::push_log` → `ComponentBuffer::push_log` path emits a `tracing` event (overflow is an `AtomicU64`, not a warning) — confirm, including `Mutex` poisoning handling and any allocation failure path.
  - The `target() != "logit"` gate (`:917`) is the only thing keeping a dependency's instrumentation out; confirm no `logit` code logs with a foreign target and vice versa.
  - `capture_filter`'s static `WARN` cap is never stricter than a reachable `InternalLogs` level (`warn|error|off`), and installing it per-layer (not globally) is enforced somewhere, not just documented at `:864-890`.
  - Severity ordering: `level < inner.threshold` (`:921`) relies on `Severity`'s derived `Ord` matching the intended monotone order (`crates/logit-core/src/lib.rs:44-52`).
  - A log captured before `activate` is dropped silently and that's intended.
- **Observed concerns (unverified):**
  - `Visitor::record_str` does `value.trim_matches('"')` (`telemetry.rs:940`) — `trim_matches` strips **all** leading and trailing `"` characters, not one pair. A self-diagnostic message that legitimately begins or ends with a quote (e.g. a warning quoting a bad config value, `bad key "x"`) is silently mangled in the captured log while the stderr copy is intact. Medium confidence this is real and low severity; worth a targeted test.
  - `record_debug` allocates a `String` per field via `format!("{value:?}")` (`:936-938`) on every captured event, then allocates again in `record_str`. Only on the warn+ path, so cost is bounded — but it is on the path a log flood takes.
  - `push_log`'s silent no-op when `component_id` names no registered buffer (`:796-801`) drops the event with **no counter at all** — the one loss in this subsystem that self-telemetry can't see. Documented as "should not happen"; medium confidence it's genuinely unreachable.
- **Existing coverage:** `crates/logit-core/src/telemetry.rs:968+` test module (includes capture/threshold/fallback tests); `crates/logit-core/src/diag.rs:184+`. Governed by `docs/design/internal-telemetry.md` ("Logs"), `docs/adr/tracing-for-self-logging.md`, `docs/plans/operator-surface.md` workstream D.
- **Suggested verification approach:** targeted review of the whole capture path for any `tracing!` call; a test emitting a warn whose message starts and ends with quotes; a flood test asserting `logit.internal.logs.dropped` reconciles.
- **Priority:** P1 — feedback-loop class plus a concrete (small) message-corruption finding.

---

### CORE-10 — Deterministic span sampling and `SpanGuard` span minting
- **Location:** `crates/logit-core/src/telemetry.rs:94-128` (`DEFAULT_SPAN_SAMPLE_RATE = 0.1`, `trace_is_sampled`), `:235-264` (`Telemetry::span`), `:267-288` (`now_unix_nanos`, with the `#[cfg(test)]` clock override), `:326-368` (`PendingSpan`, `started_at` rationale), `:391-489` (`SpanGuard`, `link` cap, `finish_inner`'s saturating `start + elapsed`), `:562-589` (`span_event`), `:300-324` (`Timer`, record-on-`Drop`)
- **What it does:** Every node visit mints at most one span, kept or dropped by a hash of the `trace_id` alone so every `logit` process in a split topology reaches the same verdict with no propagated bit. The end timestamp is `start + Instant::elapsed()`, never a second wall-clock read, so an NTP correction can't produce `end < start`.
- **Why sensitive:** numeric (a float comparison against `rate * 2^53`, NaN handling, the 53-bit truncation choice), hot-path (called per node visit per batch), accounting (`logit.internal.spans.dropped`, `span.links.dropped`), concurrency (the sample rate is copied into each buffer at construction to avoid a second lock).
- **Invariants to verify:**
  - `trace_is_sampled` keeps approximately `rate` of uniformly distributed trace ids, exactly at `rate == 1.0` (the `!(rate < 1.0)` form at `:119-122` is deliberately NaN-permissive) and none at `rate <= 0.0`.
  - The verdict depends only on `trace_id` — two processes with the same configured rate agree for every id.
  - Trace ids from `random_id_bytes` are uniform enough in `trace_id[8..16]` for this to hold (couples this entry to the id-minting entry below).
  - `finish_inner`'s `min(i64::MAX as u128) as i64` + `saturating_add` (`:479-480`) never panics or wraps.
  - `Timer`/`SpanGuard` recording on `Drop` at a cancelled `.await` (documented at `:292-299`) doesn't corrupt any counter that must reconcile — only distribution shape.
  - A disabled or unsampled guard allocates nothing and reads no clock (`:243-246`).
- **Observed concerns (unverified):** none spotted. The 53-bit choice, the NaN arm and the monotonic-end derivation are each argued in-file with a test.
- **Existing coverage:** `crates/logit-core/src/telemetry.rs:968+`, including `a_wall_clock_moving_backward_between_start_and_finish_cannot_make_end_precede_start` and the `CLOCK_OVERRIDE` machinery. Governed by `docs/adr/internal-span-emission-and-deterministic-sampling.md`, `docs/design/internal-telemetry.md` ("Spans"). Open items tracked in [`docs/known-gaps.md`](../known-gaps.md#internal-telemetry-and-self-logging) (listener span window is `send`-only; Lua `flush()` gets a link-less root) — documented, not surprises.
- **Suggested verification approach:** statistical test of `trace_is_sampled` over many random ids at several rates; property test that two independently-constructed registries agree on every id.
- **Priority:** P1 — custom numeric sampling whose whole value is cross-process agreement.

---

### CORE-11 — Trace/span id minting (per-thread SplitMix64) and hex parsing
- **Location:** `crates/logit-core/src/trace.rs:31-47` (`TraceRef::from_bytes`), `:51-76` (`push_hex`, `to_hex`, `parse_trace_id`, `parse_span_id`), `:90-102` (`parse_traceparent`), `:113-154` (`random_id_bytes`, `initial_seed`), `:156-168` (`parse_hex`)
- **What it does:** A thread-local SplitMix64 mints trace/span ids without a `rand` dependency, seeded per thread from `RandomState::new().hash_one(ThreadId)`. Hex parsing enforces exact length, ASCII hex and the OTLP all-zero-is-invalid rule in one place. `parse_traceparent` accepts only W3C version `00`, exactly 55 bytes.
- **Why sensitive:** custom (hand-rolled PRNG and hand-rolled hex/`traceparent` parsing), untrusted-input (`parse_traceparent`/`parse_*_id` take wire and script data), numeric (id uniformity feeds the deterministic sampler above). Explicitly *not* security-relevant per the module doc.
- **Invariants to verify:**
  - Two fresh threads never produce the same first id (the const-seed bug the comment at `:117-121` records was caught in review — confirm the fix holds, e.g. that `RandomState::new()` really varies per call within a process).
  - `random_id_bytes::<16>()` output is uniform enough in bytes 8..16 that `trace_is_sampled` isn't biased.
  - `parse_hex` rejects non-ASCII (`:157`), wrong length, and non-hex digits; `to_digit(16)` accepts only `[0-9a-fA-F]`.
  - `parse_traceparent`'s fixed offsets (`:92-100`) can't index out of bounds for any 55-byte input, including non-ASCII multibyte content (`&s[3..35]` slicing a `str` at a non-char boundary would **panic** — the length check is on bytes, and `parse_hex`'s `is_ascii` check happens *after* the slice).
  - `TraceRef::from_bytes` never yields a span without a trace and never yields an all-zero trace.
- **Observed concerns (unverified):**
  - `parse_traceparent` (`trace.rs:90-102`) checks `b.len() != 55` and three separator bytes, then slices `&s[3..35]`, `&s[36..52]`, `&s[53..55]` on the **`str`**. If the input contains a multi-byte UTF-8 character positioned so that byte index 3, 35, 36, 52, 53 or 55 falls inside it, the slice panics rather than returning `None`. The separator checks at indices 2/35/52 constrain three of those boundaries but not 3, 36, 53 or 55. Medium confidence this is reachable from a header value; whoever calls `parse_traceparent` (likely `trace_context`) decides whether the input can be non-ASCII. Worth a fuzz run.
- **Existing coverage:** `crates/logit-core/src/trace.rs:170+` unit tests. Governed by `docs/adr/log-record-trace-context.md`, `docs/adr/trace-context-span-lifting.md`.
- **Suggested verification approach:** fuzz `parse_traceparent` with arbitrary 55-byte UTF-8 strings (the panic hypothesis is cheap to confirm or refute); chi-square the PRNG output; a two-thread first-call-divergence test.
- **Priority:** P2 — likely a panic-on-malformed-input bug rather than anything worse, but cheap to settle and it sits on a parse path.

---

### CORE-12 — Hand-rolled RFC 3339 formatting/parsing and exact decimal-to-nanos
- **Location:** `crates/logit-core/src/time.rs:21-39` (`format_rfc3339_utc`), `:48-65` (`civil_from_days`), `:96-104` (`parse_rfc3339_to_nanos`), `:110-200` (`parse_rfc3339_components`), `:204-223` (`is_leap_year`, `days_in_month`), `:229-237` (`days_from_civil`), `:251-281` (`parse_decimal_nanos`)
- **What it does:** Howard Hinnant's civil-date algorithms in both directions with `div_euclid`/`rem_euclid` for pre-epoch correctness, a strict RFC 5424-flavoured RFC 3339 parser (uppercase `T`/`Z`, no leap seconds, real calendar validation, 1-9 fractional digits, `±HH:MM` offsets), and an `f64`-free decimal-to-nanos converter for values like nginx's `$msec`.
- **Why sensitive:** custom (a deliberate hand-roll rather than `chrono`/`time`, flagged TODO at `:14`), numeric (overflow via `checked_mul`/`checked_add`, euclidean vs truncating division, exactness beyond `f64`'s 53 bits), untrusted-input (`syslog_in`'s TIMESTAMP, `trace_context`'s `span.*_rfc3339`, `$msec`), hot-path (per event for syslog).
- **Invariants to verify:**
  - `format_rfc3339_utc` never panics for any `i64`, including `i64::MIN`/`MAX` (claimed at `:18-20`).
  - `parse_rfc3339_to_nanos(format_rfc3339_utc(n)) == n` for every `n` in the representable range; and the inverse for every valid input string.
  - `civil_from_days` / `days_from_civil` are exact inverses over the full `i64` day range (note `days_from_civil` at `:229-237` uses plain `/` with an explicit negative-year adjustment while `civil_from_days` uses `div_euclid` — two different formulations of the same floor, worth checking they agree at negative years).
  - Malformed-vs-out-of-range is classified correctly (`TimestampError`), since callers treat them differently.
  - `parse_rfc3339_components`'s byte indexing (`b[4]`, `b[7]`, `b[10]`, `b[13]`, `b[16]`, and `digits(start, n)` which uses `s.get(..)`) can't panic on non-ASCII input — `digits` uses `s.get()` (safe), but the direct `b[i]` reads are on the byte slice (safe) and the offsets are guarded by `b.len() < 20`. Later indices (`idx + 1`, `idx + 4`) use `digits`, so safe. Confirm there is no `&s[..]` slice at a non-char boundary anywhere.
  - `parse_decimal_nanos` rejects rather than truncates sub-nanosecond digits (`:270-276`) and overflows cleanly.
- **Observed concerns (unverified):** the two civil-date formulations differing in style (`div_euclid` vs the `if y >= 0` adjustment) is a correctness-by-two-different-arguments situation; low confidence there's a real disagreement, but it's exactly what a round-trip proptest settles.
- **Existing coverage:** `crates/logit-core/src/time.rs:283+` unit tests. No governing ADR — the hand-roll decision is recorded in the module doc (`:8-14`) and `AGENTS.md`'s "design constraints".
- **Suggested verification approach:** proptest round trip over the full `i64` nanos range and over the full `i64` day range for the civil-date pair; differential-test against `chrono` in a dev-only test if one is ever acceptable; fuzz the parser.
- **Priority:** P1 — custom date arithmetic on a per-event parse path with untrusted input; wrong here is a silently wrong timestamp.

---

### CORE-13 — `Diagnostics`: shared power-of-two throttle and its telemetry mirror
- **Location:** `crates/logit-core/src/diag.rs:33-56` (type + the shared-`Arc` scope rationale), `:117-140` (`component_id`, `occurrences`, `lock_counts`), `:164-181` (`warn_throttled`)
- **What it does:** Per-`&'static str` key occurrence counts behind an `Arc<Mutex<HashMap<..>>>` shared by every clone of one component's `Diagnostics`, reporting the 1st/2nd/4th/8th… occurrence and suppressing the rest, with no clock. Every occurrence — suppressed or not — increments `logit.component.diagnostics{key}`.
- **Why sensitive:** concurrency (shared mutable counts across per-connection tasks; poisoning tolerated), accounting (the metric must count *every* occurrence while the log counts a subset), hot-path-adjacent (the suppressed path runs once per malformed event and must not allocate beyond the `HashMap::entry`, which allocation pins depend on).
- **Invariants to verify:**
  - Keys are `&'static str` only — no runtime-derived key can grow the map (the map is keyed by `&'static str`, so the type enforces it).
  - The suppressed path never touches `tracing`, never reads a clock, and allocates only what `HashMap::entry` does (`:154-157`).
  - The lock is never held across the `tracing::warn!` (it is explicitly dropped at `:170`).
  - `Telemetry::count` is called *before* the lock (`:165`) — confirm that ordering can't deadlock against the telemetry buffer's own lock.
  - `occurrences()` reflects the shared total across all clones.
- **Observed concerns (unverified):** none spotted.
- **Existing coverage:** `crates/logit-core/src/diag.rs:184+`; live-telemetry allocation rows in `crates/logit-bench/tests/allocations.rs`. Governed by `docs/adr/service-lifecycle-and-output-retry.md` (+ its 2026-09-14 amendment), `docs/adr/tracing-for-self-logging.md`.
- **Priority:** P2 — small, well-argued, and the failure mode is log volume rather than data.

---

### CORE-14 — `template`: the `{name}` parser and per-event renderer
- **Location:** `crates/logit-core/src/template.rs:39-71` (`TemplateError`), `:98-151` (`Template::vars`/`is_literal`/`literal`/`compile`), `:169-185` (`Compiled::render`), `:191-242` (`parse`)
- **What it does:** A name-agnostic `{name}`/`{{`/`}}` parser producing merged literal segments, compiled once into consumer-resolved values and rendered per event into a caller-owned `String` with no allocation of its own. First consumer is `generate_in`'s event template.
- **Why sensitive:** hot-path (`render` runs per generated event), custom (hand-rolled scanner with byte indexing into a `str`).
- **Invariants to verify:**
  - `parse`'s byte scanning never slices a `str` at a non-char boundary — `rest.find('}')` returns a byte offset into a `&str` (char-boundary safe), the `bytes[i..].iter().position(..)` run at `:229-232` relies on `{`/`}` being ASCII (argued at `:226-228`). Confirm the `&input[i + 1..]` at `:206` and `&rest[..end]` at `:210` are always on boundaries.
  - Adjacent literals are always merged, so `literal()` is a reliable "nothing to render per event" test (`:86-88`, `:120-126`).
  - `compile`'s `from_utf8_lossy` (`:143-145`) is exact for any `Template` that came from `parse`; the lossy path is only reachable for a hand-built `Segment::Lit`.
  - `render` allocates nothing beyond growing `out`.
  - The three error offsets are byte offsets into the original input and are accurate.
- **Observed concerns (unverified):** none spotted.
- **Existing coverage:** `crates/logit-core/src/template.rs:244+`. Governed by `docs/plans/load-test-harness.md`, `docs/adr/load-test-harness.md`.
- **Priority:** P2 — small, self-contained, dev/load-test-facing today.

---

### CORE-15 — `ScriptWorker`: VM lifecycle, the LuaJIT sandbox, and return-value validation
- **Location:** `crates/logit-script/src/lib.rs:44-46` (`sandbox_libs` = `TABLE|STRING|MATH`), `:48-72` (`remove_unsandboxed_base_globals` — deletes `loadfile`/`dofile`/`load`/`loadstring`/`getfenv`/`setfenv`), `:76-116` (`ScriptWorker` fields, `PhantomData<*const ()>`), `:149-205` (`new` — install-before-`.exec()` ordering for `trace`/`resource`/`scope`/`provenance`/`Event`), `:212-319` (`set_trace_context`, `set_resource`/`take_resource`, `set_scope`/`take_scope`, `set_provenance`, `with_telemetry`, `with_component`, `with_targets`), `:324-326` (`used_memory`), `:335-390` (`process`, `flush`), `:402-417` (`events_from_table`)
- **What it does:** One `mlua::Lua` per pipeline worker, `!Send` by construction. Loads the script once, resolves `process`/`flush` to `RegistryKey`s, and installs five globals before the script's top-level code runs (a top-level `local x = trace` captures the value at that instant, so late installation would permanently capture `nil` — a bug caught in review). `process` returns `nil` / an event / a table of events; `flush(now)` takes the tick time as a decimal-nanos string.
- **Why sensitive:** unsafe/ffi (the whole Rust↔LuaJIT boundary; `PhantomData<*const ()>` is the only thing enforcing one-VM-per-worker), untrusted-input (the script is operator-supplied but arbitrary), script-triggered unbounded work, concurrency (the `!Send` marker is a hard constraint from the VM, per `AGENTS.md`).
- **Invariants to verify:**
  - **The sandbox is actually closed.** `loadfile`/`dofile` were reachable despite `StdLib::TABLE|STRING|MATH` (reproduced in review, `:48-53`). Re-audit the full `_G` of a constructed worker for anything else that reaches the host: `os`, `io`, `debug`, `require`, `package`, `collectgarbage`, `newproxy`, `rawset`/`rawget` on protected tables, and LuaJIT's `ffi`/`jit`/`bit` in particular. Confirm `ffi` is genuinely absent, not merely not-requested.
  - **There is no instruction-count or memory limit.** Nothing here sets an `mlua` hook, a debug hook, or `Lua::set_memory_limit`. A script with `while true do end` hangs its worker thread forever; a script that accumulates a table across `flush()` calls grows the VM without bound (`used_memory()` at `:324-326` is *observation*, not a limit, and nothing appears to read it). Verify whether the per-`lua`-node dedicated OS thread ([`docs/known-gaps.md`](../known-gaps.md#transforms-predicates-and-sampling)) bounds the blast radius to that node, and whether shutdown can still proceed.
  - A panic inside a Rust callback cannot unwind through the Lua C frames (mlua catches these, but confirm for the `.expect()`s in `LogProxy::with_log`/`SpanProxy::with_span`).
  - `PhantomData<*const ()>` is present and nothing `unsafe impl Send`s around it.
  - `events_from_table` rejects a non-sequence table (the `{[2] = event}` silently-empty bug at `:396-400`) and an empty table means zero events.
  - Reassigning `_G.process`/`_G.flush` after load has no effect (documented narrowing at `:142-148`).
  - `ScriptWorker::process` holds `self.targets.borrow()` (a `Ref`) as a temporary for the *entire* `process.call(..)` expression (`lib.rs:337-339`) — any `borrow_mut` reached during a script call would panic. Only `with_targets` takes `borrow_mut`, and it can't run concurrently; confirm that stays true.
- **Observed concerns (unverified):**
  - No execution-time or memory ceiling of any kind. High confidence this is the state of the code; whether it's acceptable is a design question the ADRs may already answer (I did not find one that does). An operator-supplied infinite loop is a node-level hang.
  - The install-order dependency (`:152-176`) is subtle and load-bearing five times over; a future global installed in the wrong place silently breaks top-level aliasing only for scripts that use that pattern.
- **Existing coverage:** `crates/logit-script/src/lib.rs:419+` (89 test fns in-file), `crates/logit-bench/tests/allocations.rs:3021+` (`lua: process 1 event` = 9 allocations and its variants), `crates/logit-bench/benches/pipeline.rs` (`lua::proxy` vs `lua::to_table`). Governed by `docs/design/lua-api.md`, `docs/adr/lua-flush-root-context.md`, `docs/adr/lua-event-constructor.md`, `docs/adr/routing-by-condition-is-lua.md`.
- **Suggested verification approach:** an adversarial-script test suite — enumerate `_G` and assert an allowlist; attempt `require`/`ffi`/`os.execute`/`io.open`/`debug.getinfo`/`collectgarbage`; an infinite loop with a timeout harness; a memory-growth script watched via `used_memory()`; `return {[2]=e}` and other malformed returns.
- **Priority:** **P0** — sandbox-escape class with a reproduced precedent, plus unbounded script-triggered work on the main data path.

---

### CORE-16 — `EventProxy` handle lifetime: registry caches, the no-clone fast path, and `MetricProxy`'s `Weak`
- **Location:** `crates/logit-script/src/proxy.rs:99-173` (`EventProxy` fields and the "handle is consumed once returned" contract), `:175-251` (`new`/`with_targets`/`cloned_from`, the four lazy `*_userdata` cache accessors), `:271-301` (`into_inner` — tears down all four cached sub-proxies via `AnyUserData::take`, then `Rc::try_unwrap`), `:308-311` (`strong_count`, test-only), `:788-829` (`MetricsProxy`, uncached per-index minting), `:831-898` (`MetricProxy` holding a `Weak`, `with_metric`/`with_metric_mut`), `:907-917` (`metric_handle_consumed_error`, `stale_metric_error`), `:1648-1660` (`take_event`), `:1697-1716` (`clarify_destructed_handle_use`)
- **What it does:** One `Rc<RefCell<Event>>` shared by the event handle and its `attributes`/`log`/`metrics`/`span` sub-proxies, the latter cached as `RegistryKey`s. Returning an event from `process()` calls `AnyUserData::take`, which empties the Lua box (invalidating every Lua alias) and then `into_inner` destructs each cached sub-proxy *before* `Rc::try_unwrap`, so the common path moves the `Event` out with no clone. `MetricProxy` deliberately holds a `Weak` so an uncollected per-index handle can't defeat that fast path or resurrect a returned event.
- **Why sensitive:** unsafe/ffi (userdata lifetimes, destructed-userdata semantics, GC timing), hot-path (`into_inner`'s fast path is the difference between moving and deep-cloning an `Event` per event), concurrency-adjacent (`RefCell` borrow discipline across metamethod re-entry), data-loss (a silently-cloned event mutated through a stale handle is wrong data accepted quietly).
- **Invariants to verify:**
  - After `process()` returns an event, **no** Lua-side handle to it or its sub-objects still works: the `EventProxy`, `AttrsProxy`, `LogProxy`, `MetricsProxy`, `SpanProxy` all go through the destructed-userdata path; `MetricProxy` goes through failed `Weak::upgrade`. Each must produce a clear error, not stale data.
  - `Rc::try_unwrap` succeeds in the ordinary case — including after a script reads `event.attributes`, `event.log`, `event.metrics[1]`, `event.span`, and after a `MetricProxy` that LuaJIT has not yet collected. (`strong_count` at `:308-311` is the test hook for exactly this.)
  - `event:clone()` produces a genuinely independent `Event`, shares the target table by `Rc`, and copies the mark (`:202-206`).
  - `event:to(id)` marks the *handle*, never the `Event` (which `type_sizes.rs` pins), and the mark leaves exactly once, through `take_event`.
  - `into_inner`'s four teardown blocks (`:271-295`) remove the registry entry unconditionally even when `ud.take::<T>()` fails.
  - No `RefCell` double-borrow panic is reachable: `with_metric_mut` holds `borrow_mut` across its closure, `LogProxy::with_log_mut` likewise, `EventProxy::to_table` holds `borrow` across many `lua.create_*` calls. Confirm none of those can re-enter a script (fresh tables have no metatable, so `Table::set` can't dispatch `__newindex` — verify).
  - `LogProxy`/`SpanProxy`'s `.expect(..)` (`:539`, `:544`, `:1469`) can only be violated if the `is_some()` gate in `EventProxy::__index` (`:332-342`) is bypassed; nothing else may construct them.
- **Observed concerns (unverified):**
  - In `into_inner`, if `ud.take::<AttrsProxy>()` fails (e.g. the userdata is currently borrowed), the registry entry is removed anyway but the `AttrsProxy` keeps its `Rc`, so `Rc::try_unwrap` falls back to `rc.borrow().clone()` — the returned event and the still-live proxy then reference **different** events, and a later write through the stale proxy silently affects nothing. Low confidence this is reachable (it requires `take_event` to run while a sub-proxy metamethod is on the stack), but the failure is silent rather than loud.
  - Nothing tears down the cached sub-proxies on the path where a script *drops* an event without returning it (documented trade at `:136-142`) — registry slots are left to mlua's reuse. Documented, low severity.
- **Existing coverage:** `crates/logit-script/src/proxy.rs:1718+` (75 test fns, including stale-index and consumed-handle paths); `crates/logit-bench/tests/allocations.rs:3021-3260` pins exact allocation counts for passthrough (9), spilled-attrs passthrough (5), metric read (11), `#metrics` (8), span name, resource write (7). Governed by `docs/design/lua-api.md`, `docs/design/memory.md` §8, `docs/adr/target-components.md`.
- **Suggested verification approach:** adversarial Lua — stash `event`, `event.attributes`, `event.log`, `event.span`, `event.metrics[1]` in globals/upvalues and use each from a later `process()` and from `flush()`; return the same event twice; return an event from a table *and* keep it; force GC (`collectgarbage` is removed, so exercise via volume) between mint and return. Consider Miri on the pure-Rust portions, though mlua's C FFI limits its reach.
- **Priority:** **P0** — this is the Rust↔Lua object-lifetime boundary on the main data path, and the `Weak`-vs-`Rc` choice is explicitly load-bearing for both correctness and the no-clone fast path.

---

### CORE-17 — Lua attribute writes: `RefCell` borrow discipline, value-identity preservation, and unbounded table recursion
- **Location:** `crates/logit-script/src/proxy.rs:481-524` (`AttrsProxy` `__index`/`__newindex`, the no-op check at `:505-513`), `crates/logit-script/src/value.rs:13-67` (`value_to_lua`), `:69-111` (`MAX_EXACT_F64_INT`, `i64_is_exact_lua_number`, `exact_i64_to_lua`, `exact_u64_to_lua`), `:118-175` (`lua_string_repr`, `lua_value_matches`), `:193-214` (`lua_to_value`), `:232-244` (`validated_sequence_len`), `:246-291` (`lua_table_to_value`, `lua_table_to_attrmap`); same pattern in `crates/logit-script/src/resource.rs:123-162,191-217` and `crates/logit-script/src/scope.rs:169-232,278-308`
- **What it does:** An attribute write first checks whether the incoming Lua value is byte-for-byte what `value_to_lua` would have produced for the current value; if so it's a no-op, which is what makes `Value` variants (`Bytes` vs `Str`, `U64` vs `I64`, `F64(42.0)` vs `I64(42)`) survive an unmodified round trip through a script. Integers outside ±2^53 cross as decimal strings, because Lua's only number is an `f64`. `validated_sequence_len` decides array-vs-map by checking every key rather than trusting `raw_len()` (which is undefined for a table with holes — a reproduced bug).
- **Why sensitive:** hot-path (every attribute read/write from a script), numeric (the 2^53 boundary, the `u64::MAX` round-trip-cast trap documented at `:74-78`), data-loss (variant identity, and `Array([])` vs `Map({})` being genuinely unsolvable), untrusted-input (script-supplied tables of arbitrary shape and depth), re-entrancy (`lua_to_value` on a table can re-enter Lua, so the `RefCell` borrow must be released first).
- **Invariants to verify:**
  - **The borrow-then-release ordering.** `AttrsProxy::__newindex` takes `this.0.borrow()` for the no-op check, drops it, then calls `lua_to_value` (which can re-enter a script `__index` via `Table::get`), then takes `borrow_mut` (`proxy.rs:505-516`). Verify the same ordering holds in `resource.rs:140-159` and `scope.rs:286-306`, and that a re-entrant write during `lua_to_value` can't produce a lost update or a `RefCell` panic.
  - **`lua_to_value` recursion is unbounded.** `lua_to_value` → `lua_table_to_value` → `lua_to_value` (`value.rs:206`, `:261-269`) has no depth limit, and neither does `lua_table_to_attrmap` → `lua_to_value` (`:284-291`) nor `construct::value_at`. A script can build an arbitrarily deep nested table and overflow the Rust stack (abort, not a catchable error). The resulting `Value` then feeds `value_heap_bytes`'s equally unbounded recursion (see the accounting entry) and every codec's own walk.
  - `lua_value_matches` is exactly the inverse of `value_to_lua`'s representation choice for every variant, including the LuaJIT dual-number collapse of integral `F64` onto `LuaValue::Integer` (`value.rs:163-169`).
  - The 2^53 boundary is a magnitude check, never a round-trip cast (`value.rs:69-92`) — the `u64::MAX` saturation trap is the recorded reason.
  - `validated_sequence_len` rejects `{[1]="a",[2]="b",[4]="d",extra="c"}` (the reproduced `raw_len()` bug at `:222-231`) and treats an empty table as `Some(0)`.
  - `lua_table_to_value`'s empty-table-is-`Map` special case (`:260`) doesn't leak into `ScriptWorker::process`/`flush`'s use of the same helper.
  - `resource`/`scope` copy-on-write: `ensure_modified` clones the whole record on the first write so a second field write can't discard the first (`resource.rs:182-186`, `scope.rs:87-94`); `take` commits `modified` as the new `base` so a second `take` returns `None`.
- **Observed concerns (unverified):**
  - The unbounded nesting depth above. High confidence the recursion has no limit; medium confidence a real script could hit it accidentally, high confidence a malicious/buggy one could.
  - `lua_value_matches` is deliberately shallow — `Array([Bytes])` assigned to itself becomes `Array([Str])`. Documented residual (`value.rs:141-154`, `docs/design/lua-value-type-preservation.md`), not a surprise.
  - In `scope.rs`'s `name`/`version` write (`:174-195`), `borrow_mut()` is taken *before* the equality check and held across `Bytes::copy_from_slice` — no re-entry there, but the shape differs from the attribute path's careful ordering. Low confidence this matters.
- **Existing coverage:** `crates/logit-script/src/value.rs:297+`, `proxy.rs:1718+`, `resource.rs:234+`, `scope.rs:335+`; allocation pins `lua: set_resource + process + take_resource, no write` (9) and `.. writing resource` (7) at `crates/logit-bench/tests/allocations.rs:3038-3084`. Governed by `docs/adr/lua-value-identity-preservation.md`, `docs/design/lua-value-type-preservation.md`, `docs/design/lua-api.md`, `docs/adr/operator-declared-resource-attributes.md`.
- **Suggested verification approach:** adversarial Lua — a 100k-deep nested table assigned to an attribute (expect either a clean error or a crash, and decide which is wanted); a metatable whose `__index` writes back into the same event during `lua_to_value`; proptest `lua_value_matches` against `value_to_lua` for every `Value` variant including the 2^53 and `u64::MAX` boundaries.
- **Priority:** **P0** — unbounded script-triggered recursion into Rust stack plus the re-entrancy discipline that keeps a `RefCell` from panicking; both on the main data path.

---

### CORE-18 — `Event.new(t)`: building a whole `Event` from an untrusted-shape Lua table
- **Location:** `crates/logit-script/src/construct.rs:53-126` (the key allowlists), `:135-151` (`install`, reading the targets cell per call), `:155-227` (`event_from_table`), `:231-278` (`log_from_table`), `:282-481` (`metric_from_table`) and `:487-564` (`Kind`), `:591-616` (`bucket_from_row`, `quantile_from_row`), `:635-664` (`validate_buckets`), `:681-701` (`exp_buckets_from_table`), `:705-720` (`exemplar_from_table`), `:727-841` (`span_from_table`, `span_event_from_table`, `span_link_from_table`), `:851-1363` (shared helpers: `expect_keys_of`, `nanos_string`, `u32_field`, `count`, `i32_field`, `scale_field`, `enum_field`, `trace_ref_from_fields`, `hex_id_field`, `finite`, `attributes_from_table`)
- **What it does:** The exact inverse of `event:to_table()`. Strict key allowlists at every level (an unknown key is an error), raw table access (`raw_get`/`pairs`) so no metatable can make the key check and the reads disagree, and defaults only where `logit-core` already documents one. Rejects the two sketch kinds and `gauge_delta` as unconstructible. Normalizes a finite-tailed histogram by appending a `(+inf, 0)` overflow bucket.
- **Why sensitive:** untrusted-input (arbitrary Lua tables, including hostile shapes), numeric (`f64`→`u64`/`i32`/`u32` conversions, finiteness, quantile and scale ranges, bucket monotonicity), data-loss (a wrongly-built metric flows straight to a sink), hot-path (a script may call this per event), unbounded-growth (interning every `name`/`unit`/`description` a script supplies).
- **Invariants to verify:**
  - `Event.new(e:to_table())` is a fixed point for every losslessly-representable event shape (the explicit residuals: a `Value::Null` `log.message`/span `name`, the two sketch kinds).
  - Every numeric conversion is range-checked before the `as` cast: `u32_field:1004-1006`, `count:1033-1037` (note the `18_446_744_073_709_551_616.0` literal bound — `2^64` as an `f64`, exclusive), `i32_field:1071-1075`, `scale_field:1046-1055`. Confirm none can produce a saturating/wrapping cast.
  - `validate_buckets` guarantees strictly increasing bounds with `+inf` only last, and the appended overflow bucket keeps the OTLP `bucket_counts == explicit_bounds + 1` shape the encoder requires (`:618-664`). An empty `buckets` stays empty.
  - `expect_keys_of` really is raw in mlua 0.9 (`Table::pairs` → `lua_next`, claimed at `:16-19`, `:933-934`) — if it isn't, a metatable can hide a key from the check that `raw_get` then reads (or vice versa).
  - `trace_ref_from_fields` rejects `span_id`/`trace_flags` without a `trace_id` (`:1128-1138`) and never produces an all-zero id.
  - `span_from_table` rejects `end_timestamp < start` (`:755-760`).
  - `is_no_recorded_value = true` ORs the flag bit and `false` is a no-op, so `{flags=1, is_no_recorded_value=true}` rebuilds as exactly `1` (`:318-332`).
  - The metric/exemplar/bucket/quantile/span-event/span-link sequences are all bounded only by the script — a `values` list of 10M entries is accepted. Confirm that's intended (it is the same exposure a script writing a big attribute has).
  - `Event.new` interns every `name`/`unit`/`description`/`event_name` a script supplies (`:302`, `:1248`) — the same interner-growth exposure the Lua `telemetry` global documents, but *not* flagged here.
- **Observed concerns (unverified):**
  - Unbounded interning from `Event.new`'s `name`/`unit`/`description` fields is not called out anywhere I found, unlike `crates/logit-script/src/telemetry.rs`'s explicit treatment of the same hazard. Medium confidence this is a real (if accepted-by-analogy) gap in the documentation rather than the code.
  - `attributes_from_table` (`:1343-1362`) and `value_at` inherit `lua_to_value`'s unbounded nesting recursion (previous entry).
- **Existing coverage:** `crates/logit-script/src/construct.rs:1365+` (149 test fns — by far the densest test module in the area); allocation pins `lua: Event.new gauge event ..` in `crates/logit-bench/tests/allocations.rs`. Governed by `docs/adr/lua-event-constructor.md`, `docs/design/lua-api.md` ("Constructing events").
- **Suggested verification approach:** proptest `Event.new(e:to_table()) == e` over generated events; adversarial tables (metatables, huge sequences, deep nesting, `f64` boundary values for every integer field, NaN/±inf everywhere they're rejected); confirm `Table::pairs`'s rawness against the pinned mlua source.
- **Priority:** P1 — large untrusted-input parser, but already exceptionally well tested and every value lands in a checked field.

---

### CORE-19 — The Lua `telemetry` global: script strings into the process interner
- **Location:** `crates/logit-script/src/telemetry.rs:33-42` (`static_str` = `resolve(intern(s))`), `:44-59` (`static_metric_name`, the `logit.` prefix guard), `:61-91` (`read_tags`, reserved-key rejection), `:93-145` (`install`, the `is_enabled`-before-anything ordering and `LuaString` rather than `String` parameters)
- **What it does:** Gives scripts `telemetry.count(name, n, tags?)` and `telemetry.gauge(name, v, tags?)`. A Lua string is turned into the `&'static str` the Rust API demands by round-tripping it through the process interner — genuinely `'static`, and free when the string is already interned. Refuses the `logit.` namespace (a script could otherwise coalesce into and corrupt a runtime metric, flipping its kind via `upsert`'s mismatch fallback) and refuses `component`/`kind`/`role` tags.
- **Why sensitive:** unbounded-growth (a script that builds a metric name or tag value from per-event data leaks the interner one entry per distinct string, for the life of the process), untrusted-input, hot-path (per event if a script calls it), accounting (the `logit.` guard is what keeps runtime counters honest).
- **Invariants to verify:**
  - A disabled handle costs *nothing*: `is_enabled()` is checked before `to_str()`, before `static_str`, before `read_tags` (`:121-125`, `:134-141`). A pipeline without an `internal` component must never grow the interner from script activity.
  - The `logit.` prefix check is applied to the name before interning (it is — `:51-58` returns before `static_str`), so a rejected name doesn't leak either.
  - No other Lua-reachable path interns an arbitrary script string without a guard — `Event.new` and the `MetricProxy`/`LogProxy` `__newindex` `name`/`unit`/`description`/`event_name` writes do exactly that (`proxy.rs:683,1228,1237,1252`, `construct.rs:302,1248`).
  - `read_tags` rejects reserved keys before interning them.
- **Observed concerns (unverified):** the guard here is thorough while the equivalent interning in `proxy.rs`/`construct.rs` has no `is_enabled`-style gate and no documented hazard note. Consistent with the interner's global "accepted" posture, but the asymmetry is worth a deliberate decision. Medium confidence.
- **Existing coverage:** `crates/logit-script/src/telemetry.rs:147+`. Governed by `docs/adr/lua-authored-telemetry-cardinality.md`, `docs/design/lua-api.md`, [`docs/known-gaps.md`](../known-gaps.md#internal-telemetry-and-self-logging).
- **Priority:** P1 — a documented, deliberately accepted leak path whose guards must all stay in the right order.

---

### CORE-20 — `CountingAlloc`: the dev-only counting global allocator
- **Location:** `crates/logit-bench/src/alloc.rs:22-30` (const-initialized, destructor-free thread-locals), `:62-78` (`measure`), `:80-108` (`record_alloc`/`record_dealloc`/`record_realloc`/`bump_live`), `:116-157` (`CountingAlloc`, the `unsafe impl GlobalAlloc`)
- **What it does:** Wraps an inner allocator and counts allocations/reallocs/bytes/peak-live per thread, so allocation counts can be exact-equality assertions in ordinary tests.
- **Why sensitive:** unsafe/ffi (a `GlobalAlloc` impl — allocating from inside the allocator is infinite recursion), concurrency (thread-locals during teardown). Dev-only: `logit-bench` is `publish = false` and this never ships in the production binary.
- **Invariants to verify:** the thread-locals stay `const`-initialized and destructor-free (`Cell<u64>`/`Cell<i64>`); every counter access uses `try_with` so an allocation during thread teardown is dropped rather than panicking; `record_*` never allocates; every method forwards to `inner` unchanged.
- **Observed concerns (unverified):** none spotted. The two load-bearing details are stated in the module doc (`:13-17`) and the `SAFETY` comment (`:132-134`).
- **Existing coverage:** the whole of `crates/logit-bench/tests/allocations.rs` is the consumer.
- **Priority:** P2 — `unsafe`, but dev-only, tiny, and the hazards are documented and handled.

---

### CORE — Cross-cutting notes

**Existing coverage is unusually strong and is itself a control.** `crates/logit-core/tests/type_sizes.rs`
(11 exact `size_of` assertions, including `Symbol`, `Value`, `AttrMap` spilled/unspilled, `MetricKind`,
`MetricRecord`, `Event`, `SpanExt`, `SAMPLES_INLINE`) and `crates/logit-bench/tests/allocations.rs`
(3810 lines of exact allocation-count pins, ~20 of them Lua rows: passthrough 9, spilled passthrough 5,
metric read 11, `#metrics` 8, resource write 7, `Event.new` variants) are exact-equality on purpose —
per `AGENTS.md`, a failure is the test working. A verifier should treat both files as the *specification*
of the memory behaviour, not as tests to relax. `crates/logit-bench/benches/pipeline.rs` carries the
proxy-vs-`to_table` comparison.

**Three items look worth escalating out of this survey:** (1) `DdSketch::merge`'s `.expect()`
(`metric.rs:354`) against sketches decoded from peer bytes at `crates/logit-proto/src/native/record.rs:455`;
(2) unbounded recursion in `lua_to_value`/`lua_table_to_value`/`value_at` and the matching
`value_heap_bytes` walk, reachable from a script-built nested table; (3) the absence of any instruction-count
or memory ceiling on a `ScriptWorker`'s VM. Each is cheap to confirm and each is on the main data path.

**The recurring hazard pattern in `logit-script` is "a `RefCell` borrow held across something that can
re-enter Lua."** Four modules (`proxy::AttrsProxy`, `resource`, `scope`, `construct`) all implement the
same borrow-check-release-convert-reborrow dance, each with a comment pointing at
`AttrsProxy::__newindex` as the canonical one. A verification pass should check all four against that
one reference implementation rather than each in isolation, and should include `LogProxy::with_log_mut`
and `MetricProxy::with_metric_mut`, which *do* hold a borrow across their closures.

**Several safety arguments are "by construction, stated in a doc comment" rather than enforced.** The
interner's eternal-symbols premise, `DdSketch`'s single-config premise, `LogProxy`/`SpanProxy`'s
`is_some()` preconditions behind `.expect()`, `HllBytesReader`'s dependence on serde's
`with_capacity(size_hint)` behaviour, and the install-before-`.exec()` ordering of five Lua globals are
all correct today and all silently breakable by a plausible future change. Where a cheap guard test
exists (as `hll_slice_len_matches_upstream_constant` does), it is worth adding one.

**Nothing in this area is a surprise relative to `docs/known-gaps.md`.** The interner never freeing
(`:499-545`), the `cardinality-estimator` allocation-layout workaround (`:49-64`), the statsd
sample-rate extrapolation (`:373-397`), Lua's cost per event (`:9-15`), the listener span window and
Lua `flush()`'s link-less root (`:1014-1024`), and the Lua interner exposure (`:1062-1074`) are all
documented, deliberate, and listed above as context rather than as findings.


---

## XFORM — Native transforms

Surveyed read-only. No `unsafe` anywhere in this crate. No `cargo`/build/test run performed.
Third-party crates in play (`crates/logit-transforms/Cargo.toml`): `serde_json` (default
features -- no `unbounded_depth`, so its built-in ~128-frame recursion guard applies to nested
JSON), `regex` (automata-based, no catastrophic-backtracking exposure), `smallvec`, `bytes`,
`thiserror`. No hand-rolled crypto, no hand-rolled async I/O in this crate -- all per-event
processing is synchronous CPU work called from the node runtime in `logit-pipeline`.

---

### XFORM-01 — Aggregate: SeriesKey identity, hashing, and grouping
- **Location:** `aggregate.rs:1320-1469` (`SeriesKey`, `PartialEq`/`Hash` impls, `scope_key_eq`,
  `attr_map_key_eq`, `value_key_eq`, `hash_value`); `aggregate.rs:916-935` (`group_for`)
- **What it does:** Defines a metric series' identity as `(name, unit, whole attribute set)` and a
  resource group's identity as `(resource value, scope value)`, both by value rather than `Arc`
  identity. `f64` attribute values are compared/hashed via `to_bits()` so `NaN` is reflexive
  (required for `Eq`/`Hash` correctness) instead of IEEE-754 `NaN != NaN`, which would otherwise
  make every NaN-tagged event open a fresh, never-reclaimed series. `group_for` does a linear scan
  of `self.groups` per event to find/open the matching group.
- **Why sensitive:** custom (hand-rolled `Hash`/`Eq` over a recursive `Value` tree, not derived),
  hot-path (once per absorbed metric), numeric (bitwise float comparison is a deliberate,
  easy-to-get-wrong departure from `==`).
- **Invariants to verify:**
  - `Hash`/`Eq` stay consistent for every `Value` variant, recursively (`Array`/`Map` included) --
    a mismatch is a silent HashMap bug (two "equal" keys hash differently, or vice versa).
  - `AttrMap::iter()` truly yields sorted-by-`Symbol` order unconditionally (both `SeriesKey::hash`
    and `attr_map_key_eq` assume this to justify a zipped walk instead of a true set comparison);
    if that invariant is ever violated by a caller, two events with the same tags in different
    insertion order would wrongly become distinct series.
  - `Array` order-sensitivity is intentional (tests confirm two arrays differing only in element
    order stay distinct series) -- verify this matches `docs/design/data-model.md`'s stated
    semantics, not just this module's own tests.
  - `group_for`'s linear scan is fine only because distinct `(resource, scope)` pairs per
    `Aggregator` are small in practice; there is no cap on `self.groups.len()` itself (only on
    retained *series*), so a source that mints many distinct resources could grow `self.groups`
    within one window.
- **Observed concerns (unverified):** none spotted in the hashing/equality logic itself; the
  absence of any bound on `self.groups.len()` (as opposed to `max_retained_series` for series) is
  worth a deliberate look -- can a real topology, not just a synthetic one, present enough distinct
  `(resource, scope)` values within a single window to grow this list?
- **Existing coverage:** `aggregate.rs` tests `distinct_tag_sets_stay_distinct_series`,
  `identical_multi_valued_array_tags_fold_into_one_series`,
  `multi_valued_array_tags_differing_only_in_element_order_stay_two_series`,
  `same_tags_in_different_insertion_order_collide_into_one_series`,
  `nan_attribute_value_keys_stably_across_events`, `different_resources_do_not_fold_together`.
  No proptest against a naive reference. Governing docs: `docs/adr/aggregation-window-semantics.md`.
- **Suggested verification approach:** targeted code review of `hash_value`/`value_key_eq` against
  every `Value` variant; a proptest comparing this module's grouping against a naive
  `Vec<(Value,Value)>`-based reference for random attribute sets including NaN/nested
  arrays/maps.
- **Priority:** P1 -- wrong here silently fragments or wrongly merges series (a correctness bug,
  not a crash), and the logic is entirely custom, but it's well-reasoned and already has targeted
  tests for the known-tricky cases (NaN, ordering, arrays).

### XFORM-02 — Aggregate: per-event merge dispatch (`process`)
- **Location:** `aggregate.rs:473-914` (`Aggregator::process`), `aggregate.rs:275-336`
  (`Accumulator` enum, `passes_through`), `aggregate.rs:1187-1308` (`Accumulator::new_for`/
  `into_kind`/`kind_for_retained`)
- **What it does:** The main per-event hot path: for every metric on an event, decides whether it
  passes through untouched (cumulative `Sum`, `ExponentialHistogram`, `Summary`, and a
  mode-dependent `Histogram`), opens or reuses a series accumulator, and merges by kind (`Sum`
  add, `Gauge` last-write-wins by timestamp, `GaugeDelta` always-applies-in-arrival-order,
  `Distribution` sketch merge, `Samples` sketch-or-raw-concat with rate/cap fallback, `Set`/
  `SetMembers` HyperLogLog-or-raw-union with cap fallback, `Histogram` bucket-wise
  `saturating_add`). A kind conflict on an existing series leaves that one metric on the event
  rather than corrupting or dropping it.
- **Why sensitive:** hot-path, custom (no crate does mergeable-metric semantics), numeric
  (`saturating_add` on histogram buckets specifically to avoid backward-wrapping on a hostile/
  buggy `u64::MAX` producer; float sums saturate to `inf` on their own), data-loss/duplication
  (every merge-vs-pass-through decision either double-counts, silently drops, or correctly
  forwards a metric -- there is no middle state).
- **Invariants to verify:**
  - `passes_through` (aggregate.rs:326-336) and `Accumulator::new_for`'s `unreachable!` arm
    (aggregate.rs:1200-1263) **must stay in exact sync** -- the module doc says so explicitly and
    a mismatch is a `unreachable!()` panic at runtime, not a compile error. Any future `MetricKind`
    addition or `temporality` mode addition must update both.
  - A `GaugeDelta` never advances `Accumulator::Gauge.at` (aggregate.rs:642-656, `note the ..`) --
    verify this stays true even as merge arms are edited; the module explicitly calls out that
    mixing delta-in-arrival-order with absolute-by-LWW is only well-defined because of this.
  - Histogram bucket merge only proceeds when `bucket_bounds_match` (bitwise, NaN-safe) --
    verify a bounds mismatch always routes to the `histogram_bounds_mismatch` pass-through path,
    never silently misaligned bucket-index addition.
  - `Samples`/`SetMembers` raw-retention fallback (rate mismatch, or `max_samples_per_series`/
    `max_set_members_per_series` overflow) must produce a **union-correct** sketch/HLL of
    held-plus-incoming, not just the incoming record -- verify `held_owned.sketch()` /
    `hll.insert(m)` loops actually include every previously-held raw value/member before the
    incoming one is folded in (aggregate.rs:707-739, 777-796).
  - `was_vacant` + `GaugeDelta` unseeded-at-zero counting (aggregate.rs:808-831) must only fire on
    a genuinely new series, not a retained-but-idle one being re-touched.
- **Observed concerns (unverified):** `Value::I64`/`U64` -> `f64` coercion happens transitively
  through `crate::numeric` for `scale`/`kv_metrics`, not `aggregate` itself, but `aggregate`'s own
  `Sum`/`Gauge` accumulators are pure `f64`, so any producer emitting an `i64`/`u64` sum near or
  past 2^53 already lost precision before reaching this file (out of this file's control, but
  worth confirming at the wire-decode boundary, not assumed fixed here). No other concerns spotted
  in the merge dispatch itself -- it is unusually thoroughly commented and each fallback path has
  an accompanying test.
- **Existing coverage:** extensive -- `aggregate.rs` tests from `counters_sum_within_a_window`
  through `flush_emits_no_empty_resource_events_group` (roughly lines 1537-2693), including
  dedicated tests for kind conflicts, cumulative-vs-delta sum non-merging, gauge-delta semantics,
  and samples/set overflow fallback paths (see e.g. `a_series_fed_by_more_than_the_cap_drops_and_counts_the_rest`,
  `sum_merge_carries_the_first_records_monotonic_flag`). ADRs: `aggregation-window-semantics`
  (base + two amendments), plus `docs/design/data-model.md`'s mergeable-kinds design.
- **Suggested verification approach:** property-based test that feeds a random stream of same-key
  metrics (mixed Sum/Gauge/GaugeDelta/Distribution/Samples/Set/SetMembers/Histogram, including
  cap-crossing sequences) through `process`+`flush` and checks the result against a naive
  reference accumulator (sum-of-values for Sum, last-timestamp-wins for Gauge, exact set union
  capped at HLL error bounds, etc.); a dedicated fuzz/property test specifically for the
  `saturating_add` histogram-bucket path with adversarial `u64::MAX`-heavy inputs.
- **Priority:** P0 -- fully custom, on the primary metrics data path, and a wrong merge here is
  silent numeric corruption (double-counted or dropped metric values) that a consumer has no way
  to detect after the fact.

### XFORM-03 — Aggregate: flush, series retention, and the cardinality cap
- **Location:** `aggregate.rs:946-1159` (`Aggregator::flush`)
- **What it does:** Drains every series each window. Non-retainable accumulators (Distribution/
  Samples/Set/SetMembers, and Sum/Histogram under delta temporality) are always emitted and reset.
  Retainable ones (a Gauge in either mode; a Sum/Histogram under `temporality: cumulative`) either
  emit-and-survive (if updated this window), emit-nothing-and-age (if idle, up to
  `series_retention` windows), or get evicted. After all groups are processed, a single global
  cardinality cap (`max_retained_series`) evicts the least-recently-updated survivors first via a
  stable sort on `idle_windows`, across every resource group at once.
- **Why sensitive:** hot-path (runs every flush interval, typically 10s), custom, unbounded-growth
  guard itself (the cap this function enforces is the thing standing between a high-cardinality
  gauge stream and unbounded memory), time/windowing (idle-window aging, cross-flush survival),
  accounting (every series must end up in exactly one bucket: emitted-and-reset,
  emitted-and-retained, idle-and-retained, idle-evicted, or cardinality-evicted -- never
  double-counted, never lost silently).
- **Invariants to verify:**
  - The `retain` predicate at aggregate.rs:1003-1010 must exactly match `kind_for_retained`'s
    `unreachable!` set (aggregate.rs:1289-1307) -- same paired-exhaustiveness hazard as
    `passes_through`/`new_for`.
  - A retained Gauge's `at` is reset to `i64::MIN` on retention (aggregate.rs:1050-1052) so a
    later, earlier-timestamped absolute gauge in the next window is still accepted -- verify this
    doesn't also let a genuinely stale out-of-order gauge silently overwrite a fresher one across
    the window boundary in some other combination.
  - `total_dropped_links`/`evicted_idle`/`evicted_cardinality` telemetry counters must reflect the
    actual counts, not just be emitted opportunistically -- these are an operator's only signal
    that data is being evicted.
  - `self.groups.retain(|g| !g.series.is_empty())` (aggregate.rs:1120) must run only after
    survivors are re-inserted, and never drop a group that still holds surviving idle series.
  - `series.len()` pre-sizing `events` (aggregate.rs:996) stays a true upper bound as new emission
    paths are added (an idle-but-retained series intentionally contributes zero events -- verify
    a future change can't make it push more than one).
- **Observed concerns (unverified):** none spotted; this function's comments proactively call out
  and justify each of the choices above (worth noting: it is *unusually* well pre-argued for a
  reviewer, which raises the bar for what a review needs to actually add, not lowers it).
- **Existing coverage:** `flush_records_active_series_and_resource_group_counts`,
  `flush_with_nothing_accumulated_records_zero_series_and_zero_groups`,
  `flush_sums_series_across_multiple_resource_groups`,
  `a_delta_in_the_next_windows_resolves_against_the_previous_windows_final_value` [sic name in
  file], `a_retained_idle_gauge_emits_nothing_that_window`,
  `an_idle_gauge_is_evicted_after_series_retention_windows_and_a_later_delta_resolves_against_zero`,
  `series_retention_zero_reproduces_the_strictly_tumbling_output`,
  `the_cardinality_cap_evicts_and_fires_series_evicted_cardinality`,
  `contexts_are_never_carried_across_a_flush_even_for_a_retained_gauge`,
  `flush_emits_no_empty_resource_events_group` (all in `aggregate.rs`, roughly lines 2402-2705).
  Config-level default wiring verified in `crates/logit-config/src/lib.rs` (`series_retention: 5`,
  `max_retained_series: 10_000` by default) and `crates/logit-cli/src/pipeline.rs:591-600`.
- **Suggested verification approach:** a soak/cardinality test that pushes far more than
  `max_retained_series` distinct never-repeating gauge series over many windows and asserts peak
  memory/series-count never exceeds the cap regardless of churn rate; paused-clock window tests
  walking idle-eviction across exactly `series_retention` boundary windows (off-by-one check).
- **Priority:** P0 -- this is the actual cardinality/memory safety valve for the whole aggregate
  path; a bug here (e.g. an off-by-one in idle-window comparison, or the cap not applying
  globally) directly risks unbounded memory in production.

### XFORM-04 — Aggregate: cumulative temporality and counter-reset semantics
- **Location:** `aggregate.rs:1-47` (module doc), `aggregate.rs:565-624` (Sum/Histogram merge under
  cumulative), `aggregate.rs:1023-1041`/`1289-1307` (`start_timestamp` stamping on retained
  records), `aggregate.rs:36-40` (saturating bucket-count doc)
- **What it does:** Under `temporality: cumulative`, a delta Sum/Histogram's accumulator survives
  flushes and keeps summing; the emitted record is stamped `start_timestamp = SeriesState::first_seen`,
  unchanged for the series' lifetime, which is the reset signal OTLP/Prometheus consumers use to
  detect a counter restart. An incoming *cumulative* Sum is always pass-through (re-summing it
  would double-count).
- **Why sensitive:** numeric (this is the exact place a hostile or buggy producer's repeated
  `u64::MAX` bucket counts, or eviction/restart, could misrepresent a monotonic series to a
  downstream consumer that assumes monotonicity for rate() calculations), time/windowing
  (`start_timestamp` is the only signal that ties a "restart" together), custom.
- **Invariants to verify:**
  - `first_seen` is captured once, at series creation, from `event.timestamp` (source clock,
    aggregate.rs:550) and never updated for the life of the series -- a series that's evicted (TTL
    or cardinality cap) and recreated must get a **fresh** `first_seen`, which is the intended
    reset signal; verify no code path updates `first_seen` on an existing `SeriesState`.
  - `record.start_timestamp` is only set for `Sum`/`Histogram` kinds (aggregate.rs:1033), and only
    on the *retained* path -- verify the tumbling (`into_kind`) path for a fresh, non-cumulative
    Sum still reports `start_timestamp: 0` ("unknown") as documented, not a stale value.
  - `graph.rs` rule 39 (referenced in `with_temporality`'s doc comment) is meant to reject
    `temporality: cumulative` configured *without* both retention bounds set -- verify that graph
    rule actually exists and is exercised (this file only asserts the aggregator-level contract,
    not the config-validation guarantee it depends on).
- **Observed concerns (unverified):** none spotted in this file; the interaction with
  `graph.rs` rule 39 is outside this crate and wasn't verified here -- flagged as a cross-crate
  dependency a future reviewer should confirm still holds.
- **Existing coverage:** `a_cumulative_sum_never_merges_into_an_existing_delta_sum_series`,
  cumulative-mode tests interleaved through the retention test group listed above. Governing ADR:
  `aggregation-window-semantics.md`'s "cumulative temporality as an opt-in mode" amendment.
- **Suggested verification approach:** targeted review of the eviction-then-recreate path to
  confirm `first_seen` really resets; a test that evicts a cumulative series via the cardinality
  cap and asserts the next window's re-created series reports a fresh `start_timestamp` rather
  than an inherited one.
- **Priority:** P1 -- wrong here misleads a downstream rate() consumer rather than losing data
  outright, and the mechanism is narrow and already has a first-line test, but the OTLP/Prometheus
  contract it upholds is easy to violate quietly (e.g. by later code that touches `first_seen` for
  an unrelated reason).

### XFORM-05 — Aggregate: contributing-context span-link bookkeeping
- **Location:** `aggregate.rs:109-168` (`ContributingContexts`, `MAX_CONTRIBUTING_CONTEXTS_PER_SERIES`)
- **What it does:** Tracks up to 8 distinct `TraceContext`s per series between flushes (a bounded,
  drop-and-counted set, mirroring `ComponentBuffer::upsert`'s precedent elsewhere in the codebase),
  turned into `SpanLink`s at flush time.
- **Why sensitive:** unbounded-growth guard (small, but the pattern matters), accounting (the
  `dropped` counter must reflect exactly what was rejected by the cap).
- **Invariants to verify:** cap is per-series, never shared/leaked across series in the same
  window (module doc explicitly calls out this would be "silently wrong" per the ADR); `dropped`
  count only increments when the *specific* context wasn't already tracked and the cap was full,
  not on every re-observation.
- **Observed concerns (unverified):** none spotted -- straightforward, small, well-tested.
- **Existing coverage:** `flush_links_every_distinct_context_that_contributed_to_a_series`,
  `repeat_events_under_the_same_context_dont_duplicate_a_link`,
  `a_series_fed_by_more_than_the_cap_drops_and_counts_the_rest`,
  `contributing_contexts_reset_after_a_flush` (aggregate.rs ~2265-2365).
- **Suggested verification approach:** code review only; low complexity, already covered.
- **Priority:** P2 -- small, bounded, already tested; a bug here loses trace-link fidelity, not
  metric data.

### XFORM-06 — json.rs: zero-copy JSON-into-attributes parsing
- **Location:** `json.rs:65-129` (`JsonParser::process`), `json.rs:156-174` (`borrowed_str_bytes`),
  `json.rs:176-361` (custom `serde::de::Visitor`/`DeserializeSeed` impls deserializing straight
  into `Value`/`Symbol` instead of via `serde_json::Value`)
- **What it does:** Parses a log message as one JSON object directly into `event.attributes`,
  using a hand-rolled `Deserializer`-driven visitor (not `serde_json::from_slice::<Value>()`) so
  unescaped strings stay zero-copy slices of the original `Bytes` buffer and keys go through a
  `KeyCache` instead of allocating an owned `String` per key.
- **Why sensitive:** hot-path, custom (nontrivial-3p-use(serde_json): drives the low-level
  `Deserializer`/`Visitor` API directly rather than the ordinary `Deserialize` derive path),
  untrusted-input (this parses attacker/producer-controlled bytes).
- **Invariants to verify:**
  - `borrowed_str_bytes`'s pointer-range check (json.rs:164-174) must correctly detect every case
    where serde_json's `visit_borrowed_str` did *not* actually hand back a genuine sub-slice of
    `base` (it falls back to a copy when the range check fails) -- getting this wrong either
    panics (if it used `Bytes::slice_ref` unguarded, which it deliberately avoids) or, worse,
    could construct a `Bytes` claiming to share `base`'s allocation when it doesn't. Confirm no
    path can produce a false positive (a string that isn't really a subslice passing the range
    check due to pointer-arithmetic edge cases, e.g. a zero-length string at a boundary).
  - `self.scratch.clear()` on both entry and the error path (json.rs:95, 119) -- verify a partial
    parse never leaves stale data in `scratch` to leak into the *next* event's merge (the comment
    explains why but the two clear-sites must both remain).
  - The all-or-nothing merge contract (parse fully into `scratch`, only then drain into
    `event.attributes`) must hold even as new `Value` variants or nesting is added.
  - Recursion depth for nested objects/arrays relies entirely on `serde_json`'s own default
    recursion limit (no `unbounded_depth` feature enabled, confirmed via `Cargo.toml`) -- verify
    this is still true after any dependency-feature change, since this module does not add its own
    depth guard.
- **Observed concerns (unverified):** the pointer-range arithmetic in `borrowed_str_bytes` uses
  plain `usize` pointer casts and addition (`base_start + base.len()`, `s_start + s.len()`) with no
  overflow guard; on ordinary 64-bit targets with realistic buffer sizes this cannot overflow, but
  it's worth an explicit sanity check that these are true "is-a-subslice" checks and not just
  "ranges happen to overlap numerically" (e.g. does it correctly reject a `s` that is a *different*
  live allocation whose address range happens to lie within `base`'s numeric range due to reuse?
  In practice two `Bytes` never coincide like this from one `serde_json::Deserializer::from_slice`
  call, but this is exactly the kind of pointer-provenance reasoning that's easy to get subtly
  wrong and hard to catch by testing).
- **Existing coverage:** `json.rs` unit tests (`#[cfg(test)]` at line 382 onward), plus
  `crates/logit-bench/tests/allocations.rs`'s `json_parse_one_event`/`json_parse_wide_json_event`/
  `json_parse_reordered_keys_event` (allocation-count pins, which indirectly also exercise the
  zero-copy path -- a regression to copying would likely fail these). ADR:
  `json-parsing-into-attributes.md`.
- **Suggested verification approach:** a fuzz target over `parse_object`/`parse_object_prefix`
  feeding arbitrary bytes (checks: never panics, never produces a `Value::Str` that isn't valid
  UTF-8, never OOMs on deeply nested input); targeted review of `borrowed_str_bytes` against
  `bytes::Bytes`'s actual pointer/refcount internals.
- **Priority:** P1 -- untrusted-input parsing with hand-rolled pointer-range reasoning on a hot
  path; a bug is more likely a wrong-value or (unlikely, given the fallback) a panic than silent
  data loss, and the crate already double-checks the risky case rather than trusting serde_json's
  guarantee blindly.

### XFORM-07 — csv.rs: hand-rolled RFC 4180 row splitter
- **Location:** `csv.rs:179-242` (`split_row`), `csv.rs:250-275` (`unescape`)
- **What it does:** A hand-rolled (no `csv` crate dependency) single-pass byte scanner implementing
  RFC 4180 quoting (embedded newlines explicitly out of scope, per the ADR) to split one message
  line into `(start, end, needs_unescape)` byte-offset triples, then unescapes doubled `""` only
  where needed.
- **Why sensitive:** hot-path, custom (no crate used at all, unlike `regex`/`serde_json`),
  untrusted-input.
- **Invariants to verify:**
  - `start`/`end` offsets are stored as `u32` (`csv.rs:33`, the `scratch: Vec<(u32,u32,bool)>`
    field) -- confirm this is safe for any message the pipeline can actually deliver (i.e. that
    something upstream bounds a single log line to well under 4 GiB, since a longer line would
    silently truncate/wrap these offsets via `as u32` casts at csv.rs:228/237/109).
  - `split_row`'s precondition "`!line.is_empty()`" (csv.rs:177-178) is enforced by the caller
    (`process` returns early on an empty message) -- verify every call site still upholds this if
    the function is ever reused elsewhere.
  - The single UTF-8 validity check on the whole message (csv.rs:101-107) is claimed sufficient to
    guarantee every subsequently-sliced field is also valid UTF-8, reasoning that `delimiter`/`"`
    are single ASCII bytes and never fall inside a multi-byte sequence -- verify this reasoning
    against `logit_config`'s actual validation of `delimiter` (rule 32) rather than assuming it
    holds for every configured byte value.
  - `unescape`'s two-pass length-then-fill matches exactly (the `debug_assert_eq!(out.len(),
    out.capacity())` at csv.rs:273 is *not* checked in a release build) -- a mismatch would only
    surface as a silent extra/short allocation in release, not a panic.
- **Observed concerns (unverified):** the `u32` truncation risk above is real but almost certainly
  moot in practice (nothing in this codebase appears to accept multi-gigabyte single log lines);
  worth a one-line confirmation rather than a deep dive. The `debug_assert_eq!` at csv.rs:273
  being release-mode-silent is a minor robustness note, not a correctness bug (if the invariant
  ever broke, `Vec::with_capacity`'s actual capacity would just be larger than needed --
  `Bytes::from` would then take the extra-allocation path the comment says it's trying to avoid,
  a perf regression, not a soundness issue).
- **Existing coverage:** `csv.rs` unit tests for `split_row` (`split_handles_...` tests) plus
  `crates/logit-bench/tests/allocations.rs`'s `csv_parse_one_event` (zero-allocation pin) and its
  escaped-field counterpart. ADR: `csv-positional-columns.md`.
- **Suggested verification approach:** fuzz `split_row` directly (never panics, offsets always in
  bounds, field count matches actual delimiter count); confirm the `u32` offset ceiling against
  any documented/enforced max message size elsewhere in the pipeline.
- **Priority:** P2 -- narrow, single-pass, well-argued, and already fuzzable in principle; the
  `u32` truncation is the only latent numeric concern and is very unlikely to be reachable.

### XFORM-08 — logfmt.rs / kv parsing: hand-rolled tokenizers
- **Location:** `logfmt.rs:107-129` (`scan_quoted`), `logfmt.rs:141-215` (`parse_logfmt`),
  `logfmt.rs:217-338` (`find_bytes`, `parse_kv_segment`, `parse_kv`), `logfmt.rs:81-105`
  (`unescape`)
- **What it does:** Two hand-rolled, allocation-minimal tokenizers over the same message buffer:
  `logfmt` (whitespace-delimited, `"`-quoted-with-backslash-escapes) and `kv` (configurable
  literal separators, no quoting). Both resynchronize past malformed spans (e.g. a leading `=`
  with no key) rather than aborting the whole line.
- **Why sensitive:** hot-path, custom, untrusted-input.
- **Invariants to verify:**
  - `scan_quoted`'s escape handling (`j += 2` on a backslash) never reads past `n` -- verified by
    the `j + 1 >= n` check at logfmt.rs:118, but re-verify against an input ending in a lone
    trailing backslash inside quotes.
  - `find_bytes` (logfmt.rs:221-226) is a byte-substring search (`.windows(needle.len())`); for
    `kv`'s operator-configured `pair_sep`/`kv_sep` this is fine (bounded, non-adversarial needle
    length), but confirm no path ever calls it with an attacker-influenced needle.
  - `parse_kv`'s cursor always advances past each found separator (logfmt.rs:328-331), so the
    total work across one message is linear in message length, not quadratic -- worth a direct
    confirmation since `find_bytes` alone looks like it could be O(n) per call if misused in a loop
    that re-scans from the start.
  - The "resynchronize at next whitespace on an empty key" path (logfmt.rs:168-176) must still
    guarantee forward progress (no infinite loop) on pathological input like a message that is
    entirely `=` characters.
  - `is_blank`-vs-`NoPairs` distinction (logfmt.rs:33-35, used at logfmt.rs:417/490) determines
    whether a "nothing parsed" outcome is silently ignored or surfaced as a throttled diagnostic --
    verify a message of pure barewords-with-`bare_keys`-off still correctly produces a *diagnosed*
    `NoPairs`, not a silent skip (the module doc says a bareword-only line fails as `NoPairs` "the
    same as it would with `bare_keys` off").
- **Observed concerns (unverified):** none spotted that suggest a genuine quadratic blowup or an
  infinite loop; the resynchronization logic is subtle enough (three separate "value" arms in
  `parse_logfmt`, each advancing `i` differently) that a fuzz target would be higher-value here
  than more manual reading.
- **Existing coverage:** `logfmt.rs` has a large test module (from line 503) covering quoting,
  escapes, unterminated quotes, bare keys, and `kv`'s three empty/bareword/no-separator segment
  shapes; `crates/logit-bench/tests/allocations.rs` pins allocation counts for the escaped-value
  case. ADR: `logfmt-and-kv-parsing.md`.
- **Suggested verification approach:** fuzz `parse_logfmt`/`parse_kv` directly (never panics, never
  infinite-loops, `i`/`cursor` always strictly non-decreasing and eventually reaches `n`); a
  proptest comparing output against a simple reference regex-based logfmt parser on generated
  well-formed input.
- **Priority:** P1 -- hand-rolled tokenizer over untrusted bytes on a hot path is exactly the
  shape most likely to hide a panic-on-malformed-input or infinite-loop bug that only a fuzzer
  finds; no such bug was spotted by reading, but reading is not the right tool to rule one out.

### XFORM-09 — trace_context.rs: timing resolution and skew arithmetic
- **Location:** `trace_context.rs:166-225` (`timing_nanos`, `f64_seconds_to_nanos`, `quantity`),
  `trace_context.rs:336-365` (start/end/duration resolution and skew check inside `lift`)
- **What it does:** Resolves an access-log line's start/end/duration attributes (arriving in one of
  five unit forms each) into a single nanosecond `(start, end)` pair, rejecting contradictory
  inputs (two forms of the same quantity present at once, negative duration, end before start,
  arithmetic overflow) and a result too far from receipt time (`max_skew`). All arithmetic goes
  through `checked_mul`/`checked_add`/`checked_sub`; a float-seconds form is converted via
  integer-exact whole-second scaling plus a rounded fractional part, deliberately avoiding a
  single imprecise `f64` multiply.
- **Why sensitive:** custom, numeric, time/windowing, untrusted-input (every value here originates
  from a log producer's attributes).
- **Invariants to verify:**
  - Every arithmetic step that combines two attacker/producer-supplied quantities
    (`s.checked_add(d)`, `e.checked_sub(d)`, `receipt.checked_sub(d)`, the skew
    `instant.checked_sub(receipt)`) must route overflow to `Skip::Timing`/`Skip::Skew`, never a
    panic or a wrapped value -- confirmed present at every call site read; verify none were missed
    if this function is extended.
  - `f64_seconds_to_nanos` (trace_context.rs:197-209): the `as i128` cast on `whole` "saturates
    rather than wraps" per its comment -- confirm this is actually true of Rust's `f64 as i128`
    cast semantics (it is, since Rust 1.45's cast behavior change), not just asserted in a
    comment.
  - `quantity`'s "exactly one form may be present" rule (trace_context.rs:214-225) -- confirm a
    value present in two forms that happen to *agree* (e.g. `span.start` and `span.start_ms` both
    resolving to the same nanosecond value) is still rejected as `Skip::Invalid`, per the stated
    "refuses to resolve by precedence" design, not silently accepted because they happen to match.
  - The skew check's `unwrap_or(true)` on `checked_sub` overflow (trace_context.rs:361) treats an
    unrepresentable skew as "skewed" (safe default) -- verify this can't be reached with a
    *valid* combination of timestamps that should have been accepted.
- **Observed concerns (unverified):** none spotted; this is the most defensively written numeric
  code in the crate (explicit comments justifying each `checked_*` choice and the float-avoidance
  strategy). The one thing not verified here (out of this crate's scope) is `logit_core::trace`'s
  own `parse_traceparent`/`parse_trace_id`/`parse_span_id`/`parse_decimal_nanos`/
  `parse_rfc3339_to_nanos` hex/date parsers that this module calls into -- those live in
  `crates/logit-core/src/trace.rs`, outside this survey's assigned area, but are direct untrusted-
  input dependencies of this file and should be covered by whichever survey owns `logit-core`.
- **Existing coverage:** `trace_context.rs`'s large test module (from line 457) covers traceparent
  precedence, invalid ids, skew rejection, contradictory timing forms, and span minting. ADRs:
  `log-record-trace-context.md`, `trace-context-span-lifting.md`.
- **Suggested verification approach:** proptest over random start/end/duration/unit combinations
  (including deliberately overflow-adjacent `i64` values) checking the function never panics and
  every `Ok` result satisfies `start <= end` and both within `max_skew` of receipt; cross-reference
  with whatever survey covers `crates/logit-core/src/trace.rs`'s parsers.
- **Priority:** P1 -- numeric/time logic entirely custom and directly fed by untrusted producer
  attributes, but unusually well-guarded already (every found arithmetic site is checked); residual
  risk is in combinations not yet in the test suite rather than an obviously missing guard.

### XFORM-10 — regex.rs: capture-group extraction
- **Location:** `regex.rs:35-54` (`RegexParser::new`), `regex.rs:71-110` (`process`)
- **What it does:** Compiles an operator-configured pattern once at construction, reuses a
  `CaptureLocations` buffer per event, and writes each named capture as a zero-copy `Value::Str`
  slice of the matched buffer.
- **Why sensitive:** hot-path, nontrivial-3p-use(regex) only in the sense of reusing
  `CaptureLocations` across calls rather than the ordinary `captures()` API -- otherwise a
  straightforward, idiomatic use of the `regex` crate, whose automata-based engine has no
  catastrophic-backtracking exposure regardless of pattern (unlike `regex` crates in some other
  languages).
- **Invariants to verify:** `self.locs`/`self.names` stay indexed consistently as
  `Regex::capture_names()`'s order (captured once at construction) -- verify a pattern change
  (impossible at runtime here, since `RegexParser` is rebuilt on config reload, but worth
  confirming) can't desync `names[i]` from `locs.get(i)`.
- **Observed concerns (unverified):** none -- this is close to the least risky file in the crate,
  specifically because it delegates all the hard parsing-safety work to the `regex` crate itself.
- **Existing coverage:** `regex.rs` unit tests (from line 113) including telemetry and multiple
  capture-group shapes. ADR: `regex-transform.md`.
- **Suggested verification approach:** code review only; no fuzzing needed given the crate's own
  linear-time guarantee.
- **Priority:** P2 -- thin, safe usage of a crate that already provides the relevant safety
  guarantee (no ReDoS).

### XFORM-11 — Small filter/mutate transforms (combined)
- **Location:** `keep.rs` (whole file, 270 lines), `attributes.rs:1-185` (`has_attributes`/
  `drop_attributes`), `provenance.rs:1-147` (`has_provenance`/`drop_provenance`), `signals.rs`
  (whole file, `has_signal`/`keep_signals`/`drop_signals`), `set.rs` (whole file), `scale.rs`
  (whole file), `route.rs:1-172`, `keep_values.rs:1-197`, `kv_metrics.rs:1-268`, `lib.rs:46-111`
  (`numeric`/`value_matches`, shared coercion helpers)
- **What it does:** Straightforward, mostly-linear-scan filters and mutators over an event's
  attributes/resource/provenance, all operating on small (operator-authored) config-derived lists
  rather than unbounded input-derived structures. `Set`/`KeepValues`/`HasAttributes` each carry a
  one-entry `Arc::ptr_eq`-keyed resource cache -- a deliberate, bounded (size-1) optimization, not
  a growth risk.
- **Why sensitive:** low individually; grouped here because the survey brief calls for treating
  these as at-most-P2 unless something stood out. `numeric()`'s `i64`/`u64 as f64` cast
  (`lib.rs:55-56`) is the one shared numeric-correctness note: values beyond 2^53 silently lose
  precision when compared/scaled through `scale`/`kv_metrics`/`value_matches`'s numeric-coercion
  arm -- an accepted, undocumented-as-such limitation rather than a bug, since nothing in this
  crate claims exact big-integer semantics.
- **Invariants to verify:** the one-entry resource caches never serve a stale mapped resource for
  a *different* input `Arc` (all keyed correctly on `Arc::ptr_eq`, confirmed by reading); `Keep`
  with an empty `fields` list intentionally drops every attribute (documented, tested) rather than
  being a misconfiguration guard.
- **Observed concerns (unverified):** `route.rs:88-101`'s `Route::new` panics (not a `Result`) if
  a configured target isn't in the resolved target list, and again if there are more than 65535
  targets for one router (`try_into::<u16>().expect(...)`) -- both are documented as "should be
  impossible after graph validation," i.e. a construction-time panic guarded entirely by another
  crate's (`logit-pipeline::graph`) validation rules 48/51. This is a real, if narrow,
  cross-crate coupling: a future change to graph validation that weakens those rules would turn
  into a runtime panic here rather than a graceful config-rejection, with no local test catching
  the regression (only an integration-level one would).
- **Existing coverage:** each file has its own unit test module; `kv_metrics.rs`'s per-batch
  `Tally` telemetry-coalescing behavior is tested via `flush`/`end_batch` interaction tests.
  ADRs: `attribute-filtering-components.md`, `provenance-filtering-components.md`,
  `kv-metrics-semantics.md`, `scale-transform.md`, `value-allowlist-cardinality-clamp.md`,
  `target-components.md`.
- **Suggested verification approach:** none warranted beyond ordinary code review; if anything,
  confirm `logit-pipeline::graph`'s rules 48/51 (referenced by `route.rs`) are still enforced
  and tested on that side, since `route.rs` itself has no defense if they aren't.
- **Priority:** P2 -- simple, well-tested, config-bounded; the `route.rs` panic-on-graph-bug
  coupling is the only item worth a cross-crate note rather than a re-review of this file alone.

---

### XFORM — Cross-cutting notes

- **`aggregate.rs` is where essentially all real risk in this crate concentrates** -- it is the
  only stateful, cross-event, cross-flush component, and every "unbounded growth" or "numeric
  correctness" concern in the crate traces back to it. The other transforms are uniformly
  stateless-per-event and either delegate hard safety properties to a well-behaved crate (`regex`,
  `serde_json`'s recursion guard) or operate over small, operator-authored config, not
  attacker/producer-scaled data.
- **The "kept in sync by comment, not by the compiler" pairs are the crate's sharpest edge:**
  `passes_through` <-> `Accumulator::new_for`'s `unreachable!` arm, and `flush`'s `retain`
  predicate <-> `kind_for_retained`'s `unreachable!` arm. Both are called out explicitly in the
  source comments as needing to move together; a future verifier should specifically check any
  diff touching either half of either pair.
- **Untrusted-input parsing (`json`, `csv`, `logfmt`/`kv`) is uniformly hand-rolled for
  performance** (avoiding intermediate allocations/trees) rather than for lack of a suitable
  crate, and each one is already allocation-count-pinned by `crates/logit-bench/tests/
  allocations.rs`. None currently has a fuzz target; that is the single highest-value gap this
  survey found across the "parsing untrusted input" category.
- **No `unsafe` anywhere in this crate** -- every zero-copy trick (`json`'s `borrowed_str_bytes`,
  `csv`/`logfmt`'s `Bytes::slice`) goes through safe `bytes::Bytes` APIs plus ordinary pointer
  *arithmetic for comparison only* (never dereferenced), not raw unsafe slicing.
- **`docs/known-gaps.md` already documents** the aggregate-retention-on-by-default memory-growth
  change, the "A delta after eviction (the cardinality cap) or after a process restart resolves
  against 0.0" behavior ([its "statsd" section](../known-gaps.md#statsd)), and the
  `cardinality-estimator` 1.0.3 allocation-layout bug workaround -- none of these were re-reported
  above as surprises; they're deliberate, tracked gaps, cited here only as context a verifier
  should already know going in.


---

## SINK — Sink connection management and delivery

Area: `crates/logit-outputs` transport halves (statsd, syslog, graphite, collectd, influxdb),
shared helpers (`tls.rs`, `http.rs`, `attrs.rs`, `lib.rs`), the `Output`/`write_loop` seam, plus a
brief look at `logit-inputs`' `internal.rs`/`generate.rs`. Wire encoding (codecs in `logit-proto`,
and the statsd/syslog *grammar* halves) is another agent's area; this file covers the socket,
lifecycle, retry/fault and accounting layers only.

Test-module boundaries (everything below the marker is `#[cfg(test)]`): `statsd.rs:2288`,
`syslog.rs:1763`, `graphite.rs:538`, `collectd.rs:275`, `influxdb.rs:829`, `http.rs:141`;
`tls.rs`/`attrs.rs`/`lib.rs` have no test module.

---

### SINK-01 — The copied pooled-TCP send path (statsd / syslog / graphite) — probe, one-write-then-write_all, one reconnect
- **Location:**
  `crates/logit-outputs/src/statsd.rs:2068-2211` (`StatsdOutput::send_tcp`);
  `crates/logit-outputs/src/syslog.rs:1508-1668` (`SyslogOutput::send_tcp`);
  `crates/logit-outputs/src/graphite.rs:417-512` (`GraphiteOutput::send_tcp`).
  Supporting: `statsd.rs:1667-1682` / `syslog.rs:1220-1236` / `graphite.rs:134-137` (`Conn`),
  and `crate::tls::poll_pending_close` (own entry below).
- **What it does:** Builds one whole-batch byte buffer, takes the pooled connection out of
  `Option<…>` into a local, probes a *reused* connection with one non-consuming `poll_read`,
  issues a single `write()` then `write_all()` for the remainder, and (statsd/syslog only)
  `flush()`es before putting the connection back and returning `Ok`. A plaintext write that
  accepted zero bytes is retried exactly once against a fresh connection; anything after a byte
  has left is `Fault::Ambiguous` and never resent.
- **Why sensitive:** hot-path (one call per batch per sink); custom (hand-rolled partial-write +
  reconnect state machine, no crate doing it); cancellation (`deliver_with_retry` races every
  `send` under `tokio::time::timeout` — `crates/logit-pipeline/src/runtime.rs:902` — and
  `write_all` is explicitly not cancel-safe, so the `stream.take()` discipline is the only thing
  preventing a resumed write into a frame-desynced stream); data-loss / duplication (a
  misclassified `Clean` makes the runtime resend a batch the peer already has; a misclassified
  `Ambiguous` silently drops a batch that never landed); nontrivial-3p-use(tokio-rustls) (the whole
  TLS/plaintext asymmetry rests on `poll_write` returning `Ok(n)` with records still in the rustls
  buffer, and on `Err` not proving a zero-byte attempt).
- **Invariants to verify:**
  - `*stream` is never written through directly; every path writes to a local taken out of it, so a
    dropped future leaves `None` behind (all three copies).
  - The one post-write-failure retry is consumed at most once per `send`; the probe-driven
    reconnect must *not* consume it (comment asserts this at `statsd.rs:2141-2145`,
    `syslog.rs:1592-1596`, `graphite.rs:460-464`).
  - Loop termination is bounded: at most probe-reconnect + one retry connect per call.
  - On TLS (`dial.is_tls()`), the `Err` arm never retries and never yields `Fault::Clean`
    (`statsd.rs:2201-2208`, `syslog.rs:1658-1665`).
  - On success the connection is only re-pooled after `flush()` returned `Ok`
    (`statsd.rs:2179-2187`, `syslog.rs:1635-1643`) — and never after a partial-write error.
  - `Ok(0)` on a non-empty buffer is normalized to `WriteZero` and treated as "nothing written"
    (true only for plaintext; tokio-rustls maps zero progress to `Pending`).
  - The returned message count (`lines.len()` / `messages.len()` / `buf.len()`) equals what was
    actually framed, including the negative-gauge pair counted as one.
- **Observed concerns (unverified):**
  - **Divergence: `graphite_out::send_tcp` never `flush()`es before reporting the batch delivered**
    (`graphite.rs:494-503`), where statsd (`statsd.rs:2179-2182`) and syslog
    (`syslog.rs:1635-1638`) both do and both document the flush as load-bearing. Benign *today*
    because `graphite.rs:136` holds a bare `TcpStream` (whose `poll_flush` is a documented no-op)
    and graphite has no TLS, but the copied family has drifted and the invariant that made the
    flush necessary is not stated in graphite. High confidence on the divergence; low on current
    impact.
  - **Divergence: graphite's `Conn::Tcp` is `Option<TcpStream>`, not `Option<Box<dyn AsyncStream>>`**,
    so it has no `is_tls()` guard on the retry arm (`graphite.rs:505-509`). Correct now; it is the
    exact line that becomes wrong the day graphite gains a `tls:` block. Medium confidence this is
    worth a comment rather than a change.
  - `conn.write_all(&frame_buf[n..])` after a short first write (`statsd.rs:2171`,
    `syslog.rs:1627`, `graphite.rs:490`) is the non-cancel-safe call; if the runtime's budget
    timeout fires inside it the peer holds a *partial* frame. For syslog's octet-counted framing a
    receiver will block on an incomplete length prefix until the connection closes (fine); for
    statsd/graphite plaintext a truncated final line is a corrupt datapoint the receiver may accept.
    No test appears to cover cancellation mid-`write_all`. Medium confidence.
  - The reused-connection probe costs one extra `poll_read` per batch on the hot path; cheap, but
    it is per-`send`, not per-idle-period. Low concern.
- **Existing coverage:** `statsd.rs` tests `tcp_reconnects_exactly_once_after_the_peer_resets_an_inherited_connection`
  (:3762), `a_pooled_connection_the_peer_closed_is_reconnected_before_writing_and_the_message_is_not_lost`
  (:3828), `a_tls_write_failure_is_ambiguous_and_never_retried` (:4303),
  `a_plaintext_zero_byte_write_still_reconnects_once` (:4338),
  `a_tls_batch_is_reported_delivered_only_once_the_stream_has_been_flushed` (:4372);
  `syslog.rs` :2342, :2437, :2947, :2976, :3008, :3073, :3110, :3152; `graphite.rs` :810, :878,
  :922. ADRs: `docs/adr/syslog-output.md`, `docs/adr/syslog-tcp-ingress-and-tls.md`,
  `docs/adr/statsd-output.md`, `docs/adr/graphite-carbon-relay.md`,
  `docs/adr/idle-connection-timeout.md`, `docs/adr/buffered-sink-delivery.md`.
- **Suggested verification approach:** Diff the three copies line-by-line (they are explicitly
  "ported verbatim"; the flush and the `is_tls` guard are the known drifts — look for others).
  Then fault injection against a real loopback peer: peer RST mid-`write_all`, peer half-close
  between probe and write, blackholed peer (no FIN) under a short retry budget, TLS peer sending
  `close_notify` then nothing. Add a cancellation test that drops the `send` future inside
  `write_all` and asserts `*stream` is `None` afterwards.
- **Priority:** P0 — main data path, fully hand-rolled, and a classification or flush mistake is
  silent loss or duplication at the receiver.

---

### SINK-02 — `TcpDial::connect` — per-phase connect/handshake timeouts and reconnect accounting
- **Location:** `crates/logit-outputs/src/statsd.rs:2214-2279` (`TcpDial`, `is_tls`, `connect`);
  `crates/logit-outputs/src/syslog.rs:1671-1738` (identical twin);
  `crates/logit-outputs/src/graphite.rs:515-528` (`connect`, the reduced copy).
- **What it does:** Dials TCP under `connect_timeout`, then — if `tls` is set — races the rustls
  handshake under the *same* `connect_timeout` again, so a TLS connect can take up to 2× the
  configured value (documented). Every failure is `Fault::Clean`. On the second and later
  successful connect it bumps `logit.output.reconnects` via a `&mut bool` borrowed from the sink.
- **Why sensitive:** custom (hand-rolled two-phase dial rather than a connector abstraction);
  concurrency (`has_connected_once` is a `&mut bool` threaded out of the sink per `send`, so its
  update ordering relative to a cancelled attempt matters); accounting (`reconnects` is the only
  operator-visible signal that a pooled connection is churning); nontrivial-3p-use(tokio-rustls,
  rustls) (SNI derived by string surgery, not by a URL parser).
- **Invariants to verify:**
  - `has_connected_once` flips exactly once, on the first *successful* connect, and a failed
    connect never counts a reconnect nor sets the flag (`statsd.rs:2272-2276`,
    `syslog.rs:1731-1735`).
  - A probe-driven reconnect is counted as a reconnect (it goes through `connect`), and a
    first-ever lazy connect is not.
  - The total worst-case connect time a `send` can consume is bounded by 2 × `connect_timeout`
    per dial × at most 2 dials — and must still fit inside `RetryConfig::total_budget`, or the
    runtime's own timeout pre-empts it with `Fault::Ambiguous`
    (`crates/logit-pipeline/src/runtime.rs:900-912`).
  - `host_only` (`tls.rs:39-46`) yields the right SNI for `host:port`, `[::1]:port`, and a bare
    host with no port.
- **Observed concerns (unverified):**
  - `graphite_out` has **no reconnect counter at all** (`graphite.rs:519-521` says so explicitly),
    so the probe-driven reconnect this sink also performs is invisible to an operator. Consistent
    with its ADR, but it makes the one sink whose `duplicate_safe()` is `true` also the one whose
    connection churn cannot be observed. High confidence, design-level.
  - `host_only` on an unbracketed IPv6 endpoint splits on the last colon and produces a wrong SNI;
    documented at `tls.rs:33-38` as the operator's problem, but nothing validates it and a wrong
    SNI surfaces as a confusing handshake failure. Low/medium.
  - A `connect` that succeeds and *then* has its future dropped by the budget timeout has already
    incremented `reconnects` (deliberate, per the comment) — worth confirming that is the intended
    reading of the metric.
- **Existing coverage:** `statsd.rs:3873` (`tcp_connect_refused_is_classified_as_a_clean_fault`),
  `:4136` (`reconnects_are_counted_from_the_second_connect`), `:3983`-`:4122` (TLS round trip,
  untrusted CA, insecure_skip_verify, mTLS); `syslog.rs:2331`, `:2596`-`:2781`, `:3191`;
  `graphite.rs:796`. ADRs: `syslog-tcp-ingress-and-tls`, `statsd-output` (TLS amendment),
  `idle-connection-timeout`.
- **Suggested verification approach:** Targeted review plus a diff of the two `TcpDial` copies;
  tokio paused-time tests for the 2× timeout claim; a TLS server that completes TCP accept and
  then stalls the handshake forever, to confirm the second timeout actually fires and faults
  `Clean`.
- **Priority:** P1 — wrong here means a stalled or mis-budgeted connect and a misleading metric,
  not silent corruption.

---

### SINK-03 — `poll_pending_close` — the one-poll half-open probe shared by every pooled sink
- **Location:** `crates/logit-outputs/src/tls.rs:48-113` (`PendingClose`, `poll_pending_close`);
  callers at `statsd.rs:2146-2156`, `syslog.rs:1597-1607`, `graphite.rs:465-475` (and
  `logit.rs`, another agent's area).
- **What it does:** Wraps a single `AsyncRead::poll_read` in `poll_fn`, mapping `Pending` → still
  open, `Ready(Ok)` with an empty buffer → peer EOF, `Ready(Ok)` with bytes → unsolicited data,
  `Ready(Err)` → treat as closed. Deliberately *not* `timeout(read)`, because a cancelled read on a
  `tokio-rustls` stream can discard a partially received record.
- **Why sensitive:** custom (hand-rolled `poll_fn` over a trait object rather than any crate API);
  cancellation (the entire justification for the shape is cancel-safety of the read);
  data-loss (the answer decides whether a batch is written into a dead socket, which is exactly
  the loss `idle-connection-timeout` closed); nontrivial-3p-use(tokio-rustls) (relies on
  `Pending` provably consuming no plaintext through a `Box<dyn AsyncStream>` the caller can't
  introspect).
- **Invariants to verify:**
  - `PendingClose::Open` is returned only on `Pending`, and only that answer keeps the connection —
    the two answers that may have consumed bytes both end with the connection dropped.
  - A TLS stream that has buffered a partial record when `Pending` is returned keeps that state in
    the retained session (the session object is kept, not just the socket) — verify against
    tokio-rustls' current `poll_read` implementation, not just the comment.
  - A `poll_read` that internally drives a renegotiation/key-update does not lose progress when
    the `poll_fn` context is dropped immediately after.
  - `PendingClose::Bytes` really is impossible in steady state for statsd/syslog/graphite (none of
    those receivers speaks back), so discarding those bytes loses nothing.
  - The probe is only ever applied to a *reused* connection, never a freshly dialled one.
- **Observed concerns (unverified):** The comment at `tls.rs:95-98` states the residual honestly (a
  FIN arriving between probe and write is unchanged). The one thing not argued: `Ready(Err(_)) →
  Eof` (`tls.rs:109`) swallows the error kind entirely, so a genuinely transient read error is
  indistinguishable from a closed peer and silently costs a reconnect. Low confidence this matters
  in practice; worth a one-line justification. No test in `tls.rs` itself (it has no test module) —
  the probe is only covered indirectly through each sink's pooled-connection tests.
- **Existing coverage:** indirect only — `statsd.rs:3828`, `syslog.rs:2437`, `graphite.rs:878`
  (`a_pooled_connection_the_peer_closed_is_reconnected_before_writing_and_the_message_is_not_lost`).
  ADR: `docs/adr/idle-connection-timeout.md` ("The client-side probe").
- **Suggested verification approach:** Direct unit tests on `poll_pending_close` with a mock
  `AsyncRead` covering all four arms; a TLS fault-injection test where the server sends a partial
  record then goes quiet, asserting a subsequent real read still sees the completed record; review
  against the pinned `tokio-rustls` version's `poll_read`.
- **Priority:** P1 — a wrong answer costs one reconnect (cheap) or one dead-socket write (silent
  loss), but the logic is small and the callers' tests exercise the main paths.

---

### SINK-04 — UDP datagram packing, `EMSGSIZE` handling, and partial-batch fault classification
- **Location:** `crates/logit-outputs/src/statsd.rs:1963-2066` (`UdpSendCounts`, `send_udp`,
  `flush_datagram`) and `:2281-2286` (`is_message_too_large`);
  `crates/logit-outputs/src/graphite.rs:308-415` + `:530-536` (the near-verbatim copy, with a
  `datapoints` column added);
  `crates/logit-outputs/src/syslog.rs:1445-1506` + `:1740-1761` (one `send_to` per message,
  deliberately *not* packed, plus `udp_send_fault`);
  `crates/logit-outputs/src/collectd.rs:214-273` (no packing at all — the codec chose the datagram
  boundaries).
- **What it does:** Resolves the endpoint once per batch, then packs/sends datagrams. An
  `EMSGSIZE`-shaped error counts the datagram's contents under
  `logit.output.messages.dropped{reason="oversize_datagram"}` and **continues**, returning `Ok`
  overall; any other error aborts the batch with `Fault::Clean` if nothing has gone out yet and
  `Fault::Ambiguous` otherwise.
- **Why sensitive:** hot-path (per batch, per datagram); custom (the greedy packer and the
  partial-batch fault rule are both hand-rolled); data-loss (an `EMSGSIZE` drop is real, silent
  data loss reported only via a counter and a throttled warning, on a `send` that returns success);
  accounting (three different counting units — entries, datapoints, value lists — must reconcile
  with what actually went on the wire); duplication (an `Ambiguous` mid-batch under an operator-set
  `AtLeastOnce` posture resends datagrams the peer already has).
- **Invariants to verify:**
  - The packer never emits a datagram larger than `max_packet_bytes`: check the `needs_sep`/`extra`
    arithmetic at `statsd.rs:2007-2018` / `graphite.rs:357-369` — the pre-flush check uses `extra`
    computed *before* the flush, and the separator is re-guarded by `!packet_buf.is_empty()` after.
  - A single entry longer than the cap can never reach `send_udp` (the encoder's own line cap must
    always be `max_packet_bytes` on the UDP arm — `statsd.rs:1743-1749`, `graphite.rs:192-198`),
    otherwise the packer emits an oversize datagram unchecked.
  - `counts.messages` counts `MessageBuf` *entries*, not `\n` bytes, so a negative-gauge pair is one
    message on both transports (`statsd.rs:1963-1976`).
  - `counts.entries_in_packet`/`datapoints_in_packet` are reset on **every** exit path of
    `flush_datagram`, including the error return (`statsd.rs:2058-2059`, `graphite.rs:405-407`).
  - `Fault::Clean` is returned only when *no* datagram of this batch has been sent
    (`statsd.rs:2057`, `graphite.rs:404`, `syslog.rs:1744-1750`).
  - Endpoint resolution happens exactly once per batch, never per datagram, and a resolution
    failure is `Clean` (all four sinks).
  - A `send` cancelled mid-datagram-loop leaves no counter at all for the datagrams already sent
    (see the accounting entry below).
- **Observed concerns (unverified):**
  - `EMSGSIZE` returning `Ok(())` from `flush_datagram` means the *whole batch* can be dropped while
    `send` reports success and `logit.output.requests{class="ok"}` increments. Deliberate and
    documented (a per-message data condition, not a sink failure — `syslog.rs:1448-1452`), but it
    is the one path where "delivered" and "dropped" are both true for the same batch. High
    confidence this is intended; worth confirming the counters make it legible.
  - `is_message_too_large` is duplicated verbatim in four files (`statsd.rs:2283`,
    `syslog.rs:1758`, `graphite.rs:533`, `collectd.rs:270`) with a Linux-only errno 90 and an
    `ErrorKind::InvalidInput` fallback; [`docs/known-gaps.md`](../known-gaps.md#udp-intake) already tracks the absence of
    per-errno send accounting (`ENOBUFS` vs `EMSGSIZE` vs `ECONNREFUSED` are one undifferentiated
    failure). Listed as documented context, not a surprise.
  - `collectd_out` has **no `flush()` override** (`collectd.rs:169-212`) — correct for UDP, but it
    is the only sink in the family that doesn't spell the contract out.
- **Existing coverage:** `statsd.rs:3566`, `:3585`, `:3609`, `:3666`, `:3681`, `:4422`
  (`udp_and_tcp_report_the_same_message_count_for_the_same_batch`); `syslog.rs:2234`, `:2283`;
  `graphite.rs:681`, `:788`, `:955` (`an_emsgsize_datagram_is_counted_not_faulted`);
  `collectd.rs` tests below :275. Integration: `crates/logit-cli/tests/statsd_round_trip.rs`,
  `syslog_round_trip.rs`, `graphite_round_trip.rs`, `collectd_round_trip.rs`. ADRs:
  `statsd-output`, `syslog-output`, `graphite-carbon-relay`, `collectd-binary-relay`,
  `framed-encoder`.
- **Suggested verification approach:** Property-test the packer (random entry lengths × caps →
  assert every produced datagram ≤ cap and the concatenation round-trips); force `EMSGSIZE` by
  binding a socket with a tiny `SO_SNDBUF` or sending past the loopback MTU; a blackholed
  destination for the `ECONNREFUSED`-after-first-datagram case; diff statsd's and graphite's
  `send_udp`/`flush_datagram` against each other.
- **Priority:** P0 — the packer is custom, runs on every UDP batch, and both the oversize path and
  the partial-batch fault rule decide between silent loss and duplication.

---

### SINK-05 — The `Output` trait contract each sink relies on (retry, posture, cancellation, shutdown)
- **Location:** `crates/logit-pipeline/src/output.rs:18-104` (trait `Output`: `bind`, `send`,
  `observe_batch`, `flush`, `duplicate_safe`), `:106-270` (`Fault`, `classify`,
  `is_explicitly_permanent`, `DeliveryPosture`, `is_retryable`); consumed at
  `crates/logit-pipeline/src/runtime.rs:889-928` (`deliver_with_retry`) and `:1066+` (`write_loop`).
  Sink-side declarations: `statsd.rs:1953-1960` (`false`), `syslog.rs:1435-1442` (`false`),
  `collectd.rs:206-211` (`false`), `graphite.rs:301-305` (`true`), `influxdb.rs:192-200` (`true`).
- **What it does:** Defines what a sink may assume: `send` is one attempt (no internal retry), the
  runtime owns retry timing and the retryable decision, every attempt is raced against the
  remaining budget and may be **dropped mid-flight**, a `Fault` travels back as anyhow context, an
  unclassified error defaults to `Permanent` but does *not* count toward the sustained-permanent
  exit window, and `flush()` is called once after the last batch.
- **Why sensitive:** concurrency / cancellation (every sink's `send` must be drop-safe at every
  await point, and the sinks encode that assumption in their `stream.take()` discipline);
  duplication (`duplicate_safe()` → `DeliveryPosture` is the single switch deciding whether an
  `Ambiguous` failure is resent); data-loss (`AtMostOnce` + `Ambiguous` = the batch is dropped);
  custom (`classify` deliberately uses `anyhow::Error::downcast_ref`, not the `std::error::Error`
  trait method — `output.rs:118-138` explains that the obvious spelling silently always returns
  `Permanent`).
- **Invariants to verify:**
  - Every `Fault` a sink attaches is attached to the *outermost* error it returns, or at least
    somewhere `anyhow`'s recursive `downcast_ref` will find it (verified for all five sinks by
    reading; re-check after any `.context(...)` reordering).
  - Each sink's `duplicate_safe()` claim matches its destination's real semantics:
    `graphite_out`'s `true` rests on whisper being last-write-wins per `(path, second)` and is
    explicitly **not** a property of the carbon wire (`graphite.rs:92-104`); `influxdb_out`'s
    `true` rests on the encoder being byte-deterministic for a re-encoded batch
    (`influxdb.rs:192-197`) — which in turn rests on `allocate_timestamp` (own entry below).
  - `send` is safe to drop at every `.await`: connect, probe, write, write_all, flush, `send_to`.
  - `flush()` is a no-op-or-better on every state a connection can be in at shutdown
    (`statsd.rs:1946-1951`, `syslog.rs:1428-1433`, `graphite.rs:294-299`).
  - No sink retries internally any more (`influxdb.rs:118-120` says it used to and no longer does).
  - `bind()` is unimplemented (default) for every sink here — only `prometheus_out` overrides it.
- **Observed concerns (unverified):**
  - A budget timeout inside `send` produces `Fault::Ambiguous` (`runtime.rs:908-911`), so for the
    four `duplicate_safe() == false` sinks it is never retried — correct, but it means a slow
    receiver plus a tight `retry_budget` silently drops batches with only
    `logit.component.errors` to show for it.
  - `logit_config::BufferConfig::delivery` can override the posture to `AtLeastOnce` for a sink that
    declared `duplicate_safe() == false`; for `statsd_out`/`collectd_out` that turns a mid-batch
    UDP `Ambiguous` into a resend of already-delivered counter increments. Operator-opt-in and
    documented, but the sinks' own doc comments argue `false` as if it were binding. Medium
    confidence this deserves a cross-reference.
- **Existing coverage:** `crates/logit-pipeline/src/output.rs:205-270` (exhaustive posture/fault
  table test, `classify` context-walk tests); runtime tests in `crates/logit-pipeline/src/runtime.rs`;
  per-sink `duplicate_safe_is_*` tests (`statsd.rs:3884`, `syslog.rs:2268`, `graphite.rs:976`).
  ADRs: `docs/adr/buffered-sink-delivery.md` (the governing one; supersedes
  `service-lifecycle-and-output-retry`'s own retry text), `disk-backed-sink-buffer`.
- **Suggested verification approach:** Targeted review of each sink's `Fault` attachment sites
  against the ADR table; a cancellation harness that drops `send` at each await point and asserts
  the sink's next `send` still works; confirm the config override path is intentional for the
  `false` sinks.
- **Priority:** P0 — this is the contract every other entry in this file depends on, and it decides
  loss vs. duplication for the whole sink side.

---

### SINK-06 — Encode-side stats emitted per `send` attempt — retry inflation and the cancelled-attempt hole
- **Location:** `crates/logit-outputs/src/statsd.rs:1820-1900` (the ~15 `telemetry.count` calls
  before any I/O); `crates/logit-outputs/src/syslog.rs:1357-1377`;
  `crates/logit-outputs/src/graphite.rs:236-246` and `crates/logit-outputs/src/collectd.rs:171-181`
  (where the *codec* emits its own counters inside `encode_into`);
  `crates/logit-outputs/src/influxdb.rs:122-142` (`multi_value_tags`).
  Retry driver: `crates/logit-pipeline/src/runtime.rs:889-928`.
- **What it does:** Each `send` re-encodes the batch from scratch and emits every encode-side drop
  / normalization counter, then does the I/O and emits the transport counters.
- **Why sensitive:** accounting (these counters are the *only* record of dropped metrics/messages —
  `docs/known-gaps.md`'s cross-protocol table cites them by name); hot-path (per batch, per
  attempt).
- **Invariants to verify:**
  - A batch delivered on attempt *k* must report each encode-side drop exactly once, not *k* times.
  - A `send` future cancelled by the budget timeout must not leave the encode counters incremented
    with no matching `logit.output.requests` — or, if it does, that asymmetry must be intended.
  - `influxdb.rs:290` zeroes `multi_value_tags` per `encode`, and `statsd`/`syslog`'s `EncodeStats`
    is `Default`-constructed per call — so the *encoder* does not accumulate; the inflation, if
    real, is purely at the telemetry sink.
  - `graphite`/`collectd` push their counters from inside the codec, so they inflate identically
    but through a different path.
- **Observed concerns (unverified):**
  - **`deliver_with_retry` calls `output.send(batch)` in a loop on the same batch**
    (`runtime.rs:898-902`), and every sink here re-encodes and re-emits its full drop/normalization
    counter set at the top of `send`. A `Fault::Clean` retry (connect refused, DNS failure,
    plaintext zero-byte write) therefore **double-counts every encode-side drop** —
    `logit.output.messages.dropped{reason=…}`, `logit.output.tags.dropped`,
    `logit.output.batch.bytes`. I found no ADR or known-gaps entry acknowledging this; grepping
    `buffered-sink-delivery.md` and `known-gaps.md` for re-encode/double-count turned up nothing.
    High confidence on the mechanism; unverified whether anyone has measured it.
  - Symmetrically, a cancelled attempt records the drops but no `requests{class=…}`, so
    `messages + messages.dropped` need not reconcile against `requests` in either direction.
- **Existing coverage:** per-sink counter tests exist for a *single* `send`
  (e.g. `statsd.rs:4404`, `:4422`), but I found none driving a sink through `write_loop` with a
  forced retry and asserting counters are not doubled.
- **Suggested verification approach:** A `write_loop`-level test with a sink whose first attempt
  fails `Clean` and whose second succeeds, asserting the drop counters match the single-attempt
  case; decide whether encode stats should move after the I/O or be memoized per batch (note
  `observe_batch` already exists as a per-batch, per-attempt hook, `output.rs:66-79`).
- **Priority:** P1 — no data is lost, but the drop accounting an operator uses to reconcile a relay
  is wrong exactly when the sink is unhealthy.

---

### SINK-07 — statsd line-level drop rules: indivisible entries, oversize-whole-drop, and the multi-value timer
- **Location:** `crates/logit-outputs/src/statsd.rs:1162-1185` (`push_line`), `:1257-1287`
  (negative-absolute-gauge two-line pair as one entry), `:1422-1521` (`render_samples`),
  `:1527-1567` (`render_set_members`), `:929-974` (`append_dialect_extras`/`append_container_id`),
  and the cap wiring at `:1739-1766`.
- **What it does:** Every rendered line funnels through `push_line`, which drops a line longer than
  `max_packet_bytes` **whole** (never truncating) and counts `dropped_oversize_line`. A negative
  absolute gauge is pushed as one `MessageBuf` entry containing an embedded `\n`, so the packer can
  never split the `name:0|g` / `name:-5|g` pair across datagrams.
- **Why sensitive:** hot-path (per metric); custom (the indivisible-entry trick and the
  drop-whole-never-truncate rule are both bespoke); data-loss (an oversize drop loses a whole
  metric record silently, counted only); accounting (the "one entry = one message" convention has
  to hold identically on UDP and TCP — see `UdpSendCounts`' doc at `statsd.rs:1963-1967`).
- **Invariants to verify:**
  - Every line-producing arm calls `push_line` — no arm writes into `ctx.out` directly.
  - The negative-gauge pair is exactly one `out.push`, and its *combined* length is what's checked
    against the cap (it is: `push_line` sees the joined string at `:1286`).
  - The cap applied to the encoder always matches the transport (`encoder_cap`, `:1743-1749`), in
    every builder order (`with_encoder` at `:1754`, `with_max_packet_bytes` at `:1761`,
    `with_diagnostics` at `:1806` must not reset it).
  - No entry other than the negative-gauge pair ever contains `\n` — the whole packing-safety
    argument depends on it, and every sanitizer exists for that reason.
  - `dropped_oversize_line` and `dropped_oversize_datagram` are never both counted for the same
    metric.
- **Observed concerns (unverified):**
  - Under `Format::DogStatsd` a `Samples` record renders as **one** multi-value line
    (`statsd.rs:1461-1491`); a `statsd_in → statsd_out` relay of a large timer datagram can produce
    a line well past 1432 bytes, and it is then dropped *whole* — every sample lost — while the
    `Format::Statsd` arm (`:1493-1519`) splits per value and would have survived. The module doc
    explains why truncation is wrong but not why splitting a multi-value line (an explicitly
    permitted normalization per `lossless-transit`) isn't done here under the cap. Medium
    confidence this is a real, reachable loss path; the encoder cap plus `statsd_in`'s per-datagram
    decode bound the size, so it needs a concrete size argument either way.
  - A negative-gauge pair whose two lines individually fit but whose joined length exceeds the cap
    drops a perfectly sendable gauge. Low confidence this is reachable with realistic caps.
- **Existing coverage:** `statsd.rs:3609` (`a_single_line_longer_than_max_packet_bytes_is_dropped_whole`),
  `:3627` (`the_encoder_line_cap_follows_the_transport_at_build_time`),
  `:3666` (`a_negative_absolute_gauges_two_lines_are_never_split_across_datagrams`),
  `:2888`/`:2938` (multi-value samples per dialect), `:3413` (oversize event line).
  Integration: `crates/logit-cli/tests/statsd_round_trip.rs`. ADRs: `statsd-output`,
  `framed-encoder`, `lossless-transit`; [`docs/known-gaps.md`](../known-gaps.md#statsd) for the post-sketch-kind and
  unit/rename debt.
- **Suggested verification approach:** Construct a realistic worst-case `Samples` record from a
  full-size `statsd_in` datagram and measure the rendered line against the default 1432 cap;
  real-receiver interop (gostatsd / Datadog agent) for the packed-datagram and negative-gauge-pair
  forms.
- **Priority:** P1 — real silent loss, but bounded to a specific record shape and already partly
  argued in the module doc.

---

### SINK-08 — `influxdb_out::send` — one-shot HTTP attempt, fault classification, and its own `reqwest` client
- **Location:** `crates/logit-outputs/src/influxdb.rs:26-37` (`DEFAULT_TIMEOUT`,
  `is_retryable_status`), `:39-114` (struct, builders, `build_client`), `:116-201` (`send`,
  `duplicate_safe`), `:203-217` (`classify_transport_error`);
  shared alternative at `crates/logit-outputs/src/http.rs:46-139`.
- **What it does:** Encodes the batch to line protocol, POSTs once to `/api/v2/write` with a
  per-request timeout, buckets the status for telemetry, and maps status/transport error to a
  `Fault`: 429 and 5xx → `Ambiguous`, other 4xx → `Permanent`, `is_connect()` → `Clean`, everything
  else → `Ambiguous`. Declares `duplicate_safe() == true`.
- **Why sensitive:** hot-path; duplication (`duplicate_safe() == true` means `Ambiguous` *is*
  retried, so the idempotency claim is load-bearing); data-loss (`Permanent` ends the batch and
  feeds `write_loop`'s sustained-permanent exit window); nontrivial-3p-use(reqwest) (`is_connect()`
  as the sole `Clean` discriminator, and a client built without the redirect policy the other HTTP
  sinks use).
- **Invariants to verify:**
  - `duplicate_safe() == true` holds: re-encoding the same `EventBatch` must be byte-identical, so
    InfluxDB's `(measurement, tag set, timestamp)` identity makes the rewrite an overwrite. That
    requires `series` and `multi_value_tags` to be cleared per `encode` (`influxdb.rs:287-290`) and
    `allocate_timestamp` to be order-deterministic (next entry).
  - `is_connect()` is genuinely true only for pre-request failures on the pinned `reqwest` version
    (the module claims this is confirmed by a live test rather than assumed, `:206-208`).
  - A 429/5xx is `Ambiguous`, never `Permanent`, so a rate-limited InfluxDB never trips the exit
    window.
  - The response body read on the error path is bounded (it is **not** here — see concerns).
  - The per-request `.timeout()` and the client-wide timeout interact as intended with
    `deliver_with_retry`'s own budget race.
- **Observed concerns (unverified):**
  - **`influxdb.rs:109-114`'s `build_client` does not disable redirects**, so `reqwest`'s default
    `limited(10)` applies — the exact hazard `http.rs:28-45` documents at length for `otlp_out`/
    `prometheus_out` (a 301/302/303 replayed as a body-less GET turning into a bogus verdict on an
    unwritten batch; a 307/308 replaying the body and the `Authorization: Token …` header at the
    `Location` host). `http.rs:12-15` explicitly names this as "a gap worth closing separately".
    High confidence; documented but open.
  - **`influxdb.rs:179` calls `resp.text().await` unbounded** on the error path, where `http.rs`
    grew `read_body_prefix`/`body_snippet` (`:78-104`) precisely because a sink getting 5xx is the
    one that keeps retrying. Same duplicated-classification gap. High confidence.
  - `influxdb.rs` keeps its own `status_class`/`is_retryable_status`/`classify_transport_error`
    (`:35`, `:98`, `:211`) that are byte-for-byte the shared ones in `http.rs` (`:111`, `:125`,
    `:133`). The divergence is deliberate per `http.rs:12-15`, but it is two copies of one table.
  - No gzip/compression on this sink at all (unlike `otlp_out`/`prometheus_out`) — worth confirming
    that's intended rather than an oversight for large batches.
- **Existing coverage:** `influxdb.rs` tests from `:829` (encode-side heavy);
  `connect_refused_is_reliably_classified_as_a_clean_fault` is named at `:207` as the live check.
  ADRs: `service-lifecycle-and-output-retry`, `buffered-sink-delivery`.
- **Suggested verification approach:** Point the sink at a local HTTP server that 302s and then
  307s to a second host and observe what it does with the token header and the batch verdict;
  bound-check the error-body read against a 5xx with a multi-MB body; decide whether to adopt
  `http.rs::build_client`.
- **Priority:** P1 — the redirect and unbounded-body issues are real and already named in-repo; the
  core classification looks sound.

---

### SINK-09 — `allocate_timestamp` — the per-series union-find collision allocator behind `duplicate_safe() == true`
- **Location:** `crates/logit-outputs/src/influxdb.rs:465-577` (`encode_metric_line`, including the
  series-key construction and the three rejected schemes) and `:579-617` (`allocate_timestamp`);
  state at `:250-257` (`visited`, `series`), cleared at `:287`.
- **What it does:** InfluxDB identifies a point by `(measurement, tag set, timestamp)`, so two
  same-series events sharing a timestamp in one batch would silently overwrite each other. A
  per-series "smallest free slot ≥ t" allocator with path compression nudges collisions forward by
  1 ns each, amortized-cheap for the *k* ≈ 30,000 case a single statsd multi-value datagram can
  produce.
- **Why sensitive:** hot-path (per metric line, and quadratic if done naively — the comment at
  `:519-524` costs out the rejected version at ~450M lookups); custom (hand-rolled union-find, no
  crate); data-loss (a collision that isn't disambiguated is a silently overwritten point);
  duplication (the determinism of this function is exactly what makes `duplicate_safe() == true`
  safe — a retry must re-derive the same timestamps or the retry writes *new* points instead of
  overwriting).
- **Invariants to verify:**
  - Re-encoding the same `EventBatch` twice yields byte-identical output (the `duplicate_safe`
    claim) — requires `series.clear()` at `:287` and iteration order over `batch.events` to be
    stable.
  - The allocator is order-sensitive but deterministic: the same arrival order always yields the
    same assignment. (A retry re-encodes the *same* `EventBatch`, so order is preserved.)
  - A timestamp with no prior collision is returned untouched, regardless of arrival order
    (the fix for the two rejected schemes).
  - Path compression cannot create a self-loop or a cycle in `next_free` — the `i64::MAX` guard at
    `:611` (`checked_add`) is the only thing preventing one; `None` must propagate as a
    `CodecError`, not a panic.
  - `visited` is cleared at entry (`:603`) and fully drained (`:613`), so no cross-call leakage.
  - The series key is `measurement + tags` only (`:506`), excluding fields and timestamp.
- **Observed concerns (unverified):** none spotted in the algorithm itself — it reads correctly and
  the reasoning is unusually well argued. One note: `series` is a `HashMap<String, HashMap<i64,i64>>`
  that grows with distinct series per batch and is cleared but not shrunk (`:287`), so a single
  wide batch pins its peak — consistent with the repo's stated buffer-reuse trade, but the inner
  maps are re-allocated per new series. Low concern, allocation only.
- **Existing coverage:** `influxdb.rs:852` onward — the `allocate_timestamp` regression tests are
  called out explicitly at `:852-854`; also `crates/logit-bench/tests/allocations.rs`.
  ADR: `buffered-sink-delivery` (for the `duplicate_safe` linkage).
- **Suggested verification approach:** Property test — random multisets of `(series, timestamp)` →
  assert every output timestamp is distinct within a series, ≥ its request, and that the whole
  assignment is a pure function of the input sequence; a second encode of the same batch must be
  byte-identical.
- **Priority:** P0 — silently overwritten points are unrecoverable data loss, the code is fully
  custom, and it is the foundation of this sink's `duplicate_safe() == true`.

---

### SINK-10 — `build_client_config` / `insecure_skip_verify` — shared client TLS construction
- **Location:** `crates/logit-outputs/src/tls.rs:115-196` (`TlsClientSettings`,
  `build_client_config`) and `:198-250` (`AcceptAnyServerCert`); callers
  `statsd.rs:1768-1804` (`with_tls`) and `syslog.rs:1304-1341` (the twin).
- **What it does:** Builds a `rustls::ClientConfig` from operator settings — bundled Mozilla roots
  or a PEM `ca_file`, optional client cert for mTLS, or a verifier that accepts any certificate.
  Both sinks treat the mere presence of a `tls:` block as "TLS required, no plaintext fallback",
  because their endpoint is a bare `host:port` with no scheme.
- **Why sensitive:** custom (`AcceptAnyServerCert` is a hand-written `ServerCertVerifier` in the
  `dangerous()` API); nontrivial-3p-use(rustls) (`builder_with_provider` +
  `with_safe_default_protocol_versions` + a custom verifier, and both arms must land in the same
  `WantsClientCert` state for the mTLS layering below to be identical).
- **Invariants to verify:**
  - `insecure_skip_verify` still verifies the *handshake signature* (it does — `:219-245` delegates
    to the provider's algorithms) and only skips chain/hostname validation.
  - The warning is emitted exactly once per sink at construction (`statsd.rs:1796-1801`,
    `syslog.rs:1333-1338`), not per connect.
  - `with_tls` on the UDP arm is a hard error in both sinks (belt-and-braces behind graph rules 52
    / 44) — `statsd.rs:1790-1795`, `syslog.rs:1327-1332`.
  - `cert_file`/`key_file` must be supplied together; the `_ =>` arm at `:194` silently falls back
    to `with_no_client_auth` if only one is set — verify config validation rejects that upstream.
  - Paths resolve against the config file's directory, not the process CWD (`base_dir`).
  - `add_parsable_certificates` (`:175`) silently skips unparsable certs — a `ca_file` that is
    entirely garbage yields an empty root store and a confusing handshake failure rather than a
    config error.
- **Observed concerns (unverified):**
  - The `(Some(cert), None)` / `(None, Some(key))` combinations fall through to no client auth with
    no diagnostic (`tls.rs:182-195`). Medium confidence this is caught by `logit-config`/graph
    validation; worth confirming rather than assuming.
  - `roots.add_parsable_certificates` discards its return value (`:175`), so "0 of 5 certificates
    parsed" is not reported. Medium confidence this is worth a warning.
  - `rustls::crypto::ring::default_provider()` is installed per call rather than as a process
    default; harmless but means every `with_tls` builds its own provider clone.
- **Existing coverage:** `statsd.rs:3896-4136` (testdata certs, trusting collector, untrusted CA,
  insecure_skip_verify warning, mTLS, UDP rejection); `syslog.rs:2502-3191` (the same matrix plus
  a plaintext-sink-against-TLS-collector case). `tls.rs` itself has no tests. ADRs:
  `syslog-tcp-ingress-and-tls`, `statsd-output` (TLS amendment), `otlp-tls-and-pooled-grpc-client`.
- **Suggested verification approach:** Targeted review against the pinned rustls version's
  `dangerous()` API contract; add cases for half-specified client-cert material and an unparsable
  `ca_file`.
- **Priority:** P2 — well covered by tests, and the failure mode is a failed connection rather
  than silent loss; the `insecure_skip_verify` verifier is the one piece worth a careful read.

---

### SINK-11 — `internal_in`'s drain loop and its final drain on shutdown
- **Location:** `crates/logit-inputs/src/internal.rs:75-143` (`run`, `run_until_shutdown`),
  `:145-204` (`tick`), `:206-227` (`shutdown_due`, `now_nanos`).
- **What it does:** Every `interval`, samples process gauges, drains `Registry` into events, counts
  them by shape, and sends one batch downstream. Overrides `run_until_shutdown` specifically so a
  SIGTERM triggers **one final drain** before returning — otherwise up to a whole interval of
  buffered self-telemetry is discarded by cancel-by-drop.
- **Why sensitive:** concurrency / cancellation (a `tokio::select!` over `Interval::tick` and a
  `watch` receiver, both of which must be cancel-safe or a tick/edge is lost); backpressure
  (`Fanout::send` at `:197-202` awaits downstream capacity, and the final drain happens *before*
  the `Fanout` is dropped, so the shutdown cascade is gated on it); accounting (the drain/reset
  boundary decides whether a point is emitted once, twice, or never).
- **Invariants to verify:**
  - Both `select!` arms are genuinely cancel-safe: `Interval::tick` consumes no tick when the other
    branch wins, and `wait_for` re-checks on the next call (asserted in the comment at `:131-133`).
  - The `_tx` binding at `:84` is retained — dropping it makes `wait_for` resolve immediately and
    turns `run` into "drain once, then exit".
  - `shutdown_due` returns `()` so no `RwLockReadGuard` is held across the drain `.await`
    (`:206-217`) — this is what keeps the future `Send`.
  - The final drain's own `Fanout::send` completes within `shutdown_grace` (5 s for `internal`,
    per `:106-111`), or the batch is lost to the grace backstop.
  - `Registry::drain` is atomic with respect to concurrent `Telemetry::count` from other tasks — the
    actual drain/reset race lives in `logit-core`, out of scope here, but this is its only caller.
  - The first `ticker.tick()` is consumed and skipped (`:129`), so no drain happens at t=0.
- **Observed concerns (unverified):** The self-counts recorded *after* the drain that produced them
  are never emitted on the final tick — explicitly acknowledged as residual at `:113-118`, not a
  surprise. Nothing else spotted. `generate.rs` (the other file in scope) is a dev/load-test
  generator: its only non-trivial mechanism is wall-clock rate pacing
  (`crates/logit-inputs/src/generate.rs:564-587`, deadline recomputed from `started` rather than
  fixed increments) and the literal-prototype clone path (`:468-498`); neither is a loss/duplication
  hazard and I did not open an entry for it.
- **Existing coverage:** `internal.rs` tests from `:229`; `generate.rs` from `:632` (including
  `rate_paces_batches_against_the_wall_clock` at `:904`). ADRs:
  `internal-telemetry-as-pipeline-events`, `decoupled-listener-io`, `load-test-harness`.
- **Suggested verification approach:** tokio paused-time test driving several intervals then firing
  shutdown mid-interval, asserting exactly one extra drain and no lost tick; a blocked-downstream
  test asserting the grace backstop cuts the final send short rather than hanging.
- **Priority:** P2 — bounded to self-telemetry, the loss mode is observability rather than customer
  data, and the tricky parts are already argued in comments.

---

### SINK-12 — Builder-order wiring of the encoder cap / diagnostics / telemetry
- **Location:** `crates/logit-outputs/src/statsd.rs:1739-1815`
  (`encoder_cap`, `with_encoder`, `with_max_packet_bytes`, `with_diagnostics`, `with_telemetry`);
  `crates/logit-outputs/src/graphite.rs:189-231`;
  `crates/logit-outputs/src/collectd.rs:120-166`;
  `crates/logit-outputs/src/syslog.rs:1299-1352` (the one that does **not** do this).
- **What it does:** Each sink keeps its own copy of `max_packet_bytes` (and diag/telemetry) and
  re-applies them to any encoder installed later, so the builders are order-independent.
  `collectd.rs:120-128` records the concrete bug this prevents: without it,
  `.with_max_packet_bytes(n).with_encoder(e)` silently reverts to an uncapped encoder, every
  datagram packs past what a UDP socket can send, and the sink reports
  `requests{class="ok"}` while delivering nothing.
- **Why sensitive:** data-loss (the documented failure mode is exactly "reports success, delivers
  nothing"); accounting (a dropped diagnostics/telemetry handle kills every counter the codec
  emits, including drop counters).
- **Invariants to verify:**
  - Every builder order produces the same sink for all four sinks that take an encoder.
  - `syslog.rs:1299-1302`'s `with_encoder` is a plain assignment with no re-application — verify
    that is safe, i.e. that `SyslogEncoder` has no sink-owned setting that must survive
    (`max_message_bytes` is set on the encoder directly at `syslog.rs:332`, and `with_diagnostics`
    at `:1343` re-applies to whatever encoder is current — so order `with_encoder` *after*
    `with_diagnostics` drops the diag handle).
  - `logit-cli::pipeline::build_spec` is the only production caller; confirm the order it uses.
- **Observed concerns (unverified):** `SyslogOutput::with_diagnostics(d).with_encoder(e)` appears to
  drop `d` from the encoder (`syslog.rs:1299-1302` overwrites, `:1343-1347` only forwards at the
  time it is called) — the exact hazard `collectd.rs:120-128` and `statsd.rs:1751-1757` were
  written to prevent, with syslog the one sink not hardened. Medium confidence; depends entirely on
  the order `build_spec` uses, which I did not check.
- **Existing coverage:** `statsd.rs:3627` (`the_encoder_line_cap_follows_the_transport_at_build_time`);
  `collectd.rs`/`graphite.rs` tests below their markers. No equivalent syslog test found.
- **Suggested verification approach:** Read `crates/logit-cli/src/pipeline.rs`'s `build_spec` for
  each sink's actual builder order; add an order-independence test per sink (assert the encoder's
  cap and that a diagnostic reaches the configured handle, in both orders).
- **Priority:** P2 — a latent construction-order bug, not a runtime one, and the production call
  site may well already use the safe order.

---

### SINK — Cross-cutting notes

- **One TCP send path, three copies, already drifting.** `statsd_out`, `syslog_out` and
  `graphite_out` each carry a hand-copied `Conn` / lazy-connect / probe / single-write-then-
  `write_all` / one-reconnect / fault-classification machine, with the source file and line range
  cited in the comments (`graphite.rs:26-31` cites `statsd.rs:1638-2045`, which no longer matches
  after edits). The known drifts are graphite's missing pre-delivery `flush()`, its bare
  `TcpStream` instead of `Box<dyn AsyncStream>`, and its absent `logit.output.reconnects`. Any
  verification session should diff the three before reviewing any one of them, and the stale
  line-range citations should be treated as unreliable.
- **`Fault::Clean` is the load-bearing promise.** Every sink's duplicate-safety argument reduces to
  "`Clean` never over-claims": it must mean *nothing of this batch left the host*. The UDP sinks
  enforce it with a `sent > 0` / `datagrams > 0` check, the TCP sinks with the
  plaintext-`write`-returns-`Err`-proves-zero-bytes argument, and TLS is excluded from `Clean`
  entirely after any application write. Four independent implementations of one rule.
- **"Delivered" and "dropped" can both be true.** An `EMSGSIZE` datagram is counted as dropped and
  the `send` still returns `Ok`, incrementing `requests{class="ok"}`. Combined with the
  per-attempt re-emission of encode-side drop counters under retry, the sink-side counters do not
  reconcile in general — worth settling as a deliberate accounting model before treating any
  `messages` vs `messages.dropped` sum as authoritative.
- **Two fault/HTTP-classification tables.** `http.rs` exists to be the single copy, and
  `influxdb.rs` deliberately keeps its own — along with `reqwest`'s default redirect policy and an
  unbounded error-body read that `http.rs` specifically fixed. `http.rs:12-15` names this as open.
- **Cancellation is the least-tested axis.** `deliver_with_retry` drops `send` futures mid-flight
  by design (`runtime.rs:902`), and every sink's correctness rests on `stream.take()` plus
  local-only connection ownership. I found tests for peer-close, zero-byte-write, TLS flush and
  connect-refused, but none that drop a `send` future inside `write_all` or inside the UDP
  datagram loop.

