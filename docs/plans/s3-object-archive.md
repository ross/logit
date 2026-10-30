---
created: 2026-10-30
updated: 2026-10-30
---

# Enabling plan: an S3 object-archive sink — one object per window, delivered at completion

## Context

`logit` has no sink for an object store, the usual place to keep telemetry for months and read
it back later. [ADR `s3-object-archive`](../adr/s3-object-archive.md) decides `s3_out`: one
object per time and size window, each batch held in the sink's store until its object is
complete, under a key its first batch names. [ADR
`collecting-sink-window`](../adr/collecting-sink-window.md) extends the window protocol so a sink
can keep that window open across arrivals. [ADR `sink-fault-classes`](../adr/sink-fault-classes.md)'s
"Amendment: `s3_out`'s departures from the status-only default (2026-10-30)" records where its
response classes leave the shared default.

Stream key: `s3`.

Goals:

- `s3_out` writes `format: json` or `native` objects, gzip, zstd, or uncompressed, to AWS S3 and
  to S3-compatible stores, and no batch counts delivered before its object is stored.
- A retry or a replay never loses data at a key it overwrites, and the duplicates it can write
  are the at-least-once kind.
- `replay_in` reads an archived object back as it was written.
- An interop harness checks the sink against MinIO, Ceph RGW, and real AWS S3.

Non-goals, each the ADR's "Not in v1":

- Native append, conditional writes, and persisted upload ids.
- Several objects or parts in flight at once.
- The OTLP formats and Parquet.
- Partitions by attribute or by event time.
- An object-store reader for `replay_in`.
- A soak stand-in shape for `s3_out`, and Cloudflare R2, Backblaze B2, or Google Cloud Storage
  verification.

## Design

The ADRs are the design; this section doesn't restate their tables. In short:

- **The window** ([ADR `collecting-sink-window`](../adr/collecting-sink-window.md)): a sink
  returns a deadline from `Output::collecting` while it takes more batches, the fill waits on a
  new store primitive, and the window closes at the sink's bound, the deadline, store pressure,
  the staged batches' memory reaching `buffer.max_bytes`, the store closing, or the shutdown
  signal. Until every batch of a closed window is answered, `window()` reads the answers owed, so
  no new window opens. Nothing changes for a sink that keeps the default.
- **The sink** ([ADR `s3-object-archive`](../adr/s3-object-archive.md)): `submit` stages a batch
  into the open object and uploads a part when one fills; `await_ack` on the head completes the
  object, and a failed part poisons it; the key is
  `{prefix}{partition}{object_time}-{store id}-{first seq}.{ext}`, and the head pins its
  `object_time` and batch count so no retry holds more than an attempt that may still land; SigV4 and
  credentials come from `logit-aws`; decision 6 is the response-class table until W3 moves it
  into the module doc.

## Workstreams

| # | Branch | PR title | Size | Depends on | Status |
|---|---|---|---|---|---|
| W0 | `s3/w0` | `s3/w0: ADRs and plan for an S3 object-archive sink` | S | — | open |
| W1 | `s3/w1` | `s3/w1: move SigV4 and the AWS credential chain into logit-aws` | S | — | open (#926) |
| W2 | `s3/w2` | `s3/w2: a collecting sink window` | M | W0 | not started |
| W3 | `s3/w3` | `s3/w3: s3_out` | L | W1, W2 | not started |
| W4 | `s3/w4` | `s3/w4: replay_in reads gzip and zstd files` | S | — | not started |
| W5 | `s3/w5` | `s3/w5: an S3 interop harness` | M | W3, W4 | not started |

### W0: ADRs and plan

The two ADRs, the `sink-fault-classes` amendment, this plan, and the index rows in
[`docs/adr/README.md`](../adr/README.md) and [`README.md`](README.md). Docs only.

### W1: `logit-aws` (#926)

**Scope.** Moves SigV4 (`logit_aws::sigv4`) and the AWS default credential chain
(`logit_aws::creds`) out of `kafka_out`'s SASL module into a new workspace crate, with no
behavior change, so `s3_out` can use them without depending on `kafka_out`. Opened before this
plan, against `main`.

**Files.** `crates/logit-aws/` (new), `crates/logit-outputs/src/kafka/sasl/`,
`crates/logit-outputs/src/kafka/fake_broker.rs`, `Cargo.toml`, `Cargo.lock`.

**Tests.** The moved tests, unchanged; a move needs no regression test.

**Docs.** [ADR `kafka-sasl`](../adr/kafka-sasl.md)'s amendment on the move, `AGENTS.md`'s
"Where things live", and `docs/design/pipeline-graph.md`'s crate layout.

### W2: the collecting window

**Scope.** Everything in [ADR `collecting-sink-window`](../adr/collecting-sink-window.md):
`Output::collecting` and the reached-the-destination count, both with defaults (the count's is
today's `InFlight::left_the_process()`); the store's wait primitive and its pressure signal, on
the memory queue and the disk spool; the fill's wait, its memory bound, and its close causes in
`window_round` and `deliver_window`; `SendLimit` during collection and upload; the shutdown signal
closing the window; the grace count; and graph rule 35 accepting a non-default
`buffer.max_bytes` beside `disk:` on a collecting sink. No sink uses it yet.

**Files.** `crates/logit-pipeline/src/output.rs`, `runtime.rs`, `queue.rs`, `disk_queue.rs`,
`buffer_full.rs`, and `graph.rs` (rule 35), and `crates/logit-proto/src/buffer.rs` if the memory
buffer needs a pressure read.

**Tests**, on a collecting variant of the `WindowedOutput` fake in `runtime.rs`'s window tests,
each waiting on an observable through `logit_pipeline::test_util`:

- a push into an empty store during collection is submitted into the same window, and the head
  is acknowledged once, after the deadline;
- the window closes at the deadline, at `collecting()` returning `None`, at `max_in_flight`, at
  the staged batches' `estimated_heap_bytes` reaching `buffer.max_bytes` on a memory and on a disk
  store, at the store closing, and at the shutdown signal before the grace expires, each counted
  under its `cause`;
- a batch that arrives while a closed window uploads isn't submitted until all N of that
  window's answers are read, and the N - 1 answers behind the head come with no fill or wait
  between them;
- a submit failure past the head makes the head's next `await_ack` fail without `HeadOnly`, and
  the loop resubmits from the head, the failed batch included;
- pressure closes it in a memory store under `block` and under `drop_oldest`, and in a disk store
  at `disk.max_bytes`; under `block` the waiting push completes once the window's batches commit;
- a fault without `HeadOnly` reopens the window from the head without observing any batch again;
- under `at_most_once`, a grace cut commits only the batches the sink says may have reached the
  destination, and the rest stay reserved; a sink keeping the default counts what
  `left_the_process()` counts today, a submit in progress included;
- `send.duration` records one sample per window, from close to answer, and the `buffer_full`
  cause names no limit while collecting;
- the store's wait is cancellation-safe and reserves nothing when dropped.

The existing window tests for `logit_out` and the HTTP slot sinks pass unchanged, which pins
"nothing changes for existing windowed sinks".

**Docs.** `docs/design/internal-telemetry.md` (`logit.component.window.closed`),
`docs/design/pipeline-graph.md`'s "Cancellation points" and rule 35, `docs/deploying.md`'s sink buffering
section (the `drop_oldest` note), and the `Output` trait's doc.

### W3: `s3_out`

**Scope.** The sink as [ADR `s3-object-archive`](../adr/s3-object-archive.md) decides it: the
config variant and its graph rules, the key, the staged object (encode, compress, hash, part
cuts), `PutObject` and the multipart path, the XML extractor, the response classes, the abort of
an upload abandoned in the process, credentials and `ExpiredToken`'s refresh, and the telemetry.
If the diff runs large, it splits into `s3/w3a` (the single-`PutObject` path, objects under one
part) and `s3/w3b` (multipart), stacked.

**Files.** `crates/logit-outputs/src/s3/` (new), `crates/logit-outputs/src/lib.rs`,
`crates/logit-outputs/Cargo.toml` (`logit-aws`), `crates/logit-config/src/lib.rs`,
`schema/logit.schema.json` through `script/schema`, `crates/logit-pipeline/src/graph.rs`, and
`crates/logit-cli/src/pipeline.rs`.

**Tests**, against an in-process HTTP stand-in, never a running service:

- one test per row of the response-class table, which moves into the module doc with each row
  naming its test, `InvalidPart`, `InvalidPartOrder`, `EntityTooSmall`, `InvalidBucketName`, and
  MinIO's `SlowDownWrite` among them;
- a key formats as the template, and a re-staged head keeps its `object_time`;
- a retry of the same head, after any fault class, writes the same key and closes at the pinned
  batch count though more batches wait in the store; a retry that closes earlier lowers the pin;
- a late `CompleteMultipartUpload` from an earlier attempt, landing after its retry committed,
  leaves every committed batch at the key;
- a part upload that fails inside `submit` is never completed: the upload is aborted once, the
  head's `await_ack` fails without `HeadOnly`, and the object starts again in a new upload;
- non-final parts are `part_size` bytes, and `x-amz-content-sha256` matches each body;
- a `200` `CompleteMultipartUpload` with an `<Error>` body isn't delivered;
- `ExpiredToken` calls `Chain::forget` before the retry, and `NoSuchUpload` starts the object
  again, sending `AbortMultipartUpload` for the old upload;
- an `Ambiguous` answer to `UploadPart` reads `Clean`, and to `CompleteMultipartUpload` stays
  `Ambiguous`;
- validation rejects each bound in decision 7 and accepts its defaults;
- an object round-trips through `replay_in`'s decoder for each format and, once W4 lands, each
  compression;
- `crates/logit-bench/tests/allocations.rs` pins the staging path's allocations.

Each regression test is run once with its fix reverted.

**Docs.** `docs/deploying.md` gains an `s3_out` section (the lifecycle rule, IAM permissions, the
dotted-bucket note, shutdown grace and `buffer.disk:`, store pressure, ingest-time partitions);
`AGENTS.md`'s outputs table and "Where things live"; an example under `examples/` with its
rendered graph; `docs/design/internal-telemetry.md`; `docs/design/memory.md`; the
`sink-fault-classes` departures list gains its `s3_out` bullet; and a `docs/known-gaps/sinks.md`
entry for anything the stream leaves open.

### W4: `replay_in` reads gzip and zstd

**Scope.** `replay_in` detects a gzip (`1f 8b`) or zstd (`28 b5 2f fd`) file by its first bytes
and decompresses it as it reads, for either format: every gzip member, and every zstd frame, of a
file. Branches off `main`; independent of the rest.

**Files.** `crates/logit-inputs/src/replay.rs`, with `crates/logit-inputs/src/zstd.rs`'s bounds
where they fit a file, and `crates/logit-config/src/lib.rs`'s `replay_in` doc.

**Tests:** a gzip and a zstd file of each format replay as the uncompressed file does; a file of
concatenated gzip members and one of concatenated zstd frames replay whole; a truncated
compressed file counts its loss under `logit.input.frames.dropped{reason}` and ends.

**Docs.** An amendment to [ADR `replay-input`](../adr/replay-input.md), `replay.rs`'s module doc,
and `AGENTS.md`'s inputs row.

### W5: `script/s3-interop`

**Scope.** An out-of-CI harness, `tools/s3-interop/` and `script/s3-interop`, shaped like
`script/splunk-interop`: pinned MinIO and Ceph RGW containers beside one `logit` per leg, and AWS
S3 legs from a gitignored env file. Each leg lists what arrived, reads each object back through
`replay_in`, and prints a `PASS`, `GAP`, or `FAIL` row: single-`PutObject` and multipart objects,
each compression and format, a forced retry that overwrites a key, a `3xx` from a wrong region,
and a store's answer to `x-amz-content-sha256` signed bodies.

**Files.** `tools/s3-interop/` (compose file, `logit-*.yaml` legs, checker, README),
`script/s3-interop`, and `.gitignore` for the env file.

**Tests.** The legs' configs join `script/validate` and `every_shipped_config_loads_and_validates`.

**Docs.** `tools/s3-interop/README.md` with a recorded run, `AGENTS.md`'s environment table and
harness list, and the ADR's decision 6 rows each run confirms or corrects.

### Mechanics

- **Branches and titles.** Each workstream is a stacked `s3/w<N>` branch whose PR is titled
  `s3/w<N>: <summary>`: W2 on W0, W3 on W2 with W1 merged in, W5 on W3. W1 (#926) targets
  `main`, and W4 branches off `main` and is independent of the stack.
- **Review.** Each PR goes to the reviewer peer session for review before its fix rounds, and
  each fix round is re-reviewed as a delta.
- **Regression tests.** Every regression test is run once with its fix reverted to show it fails,
  and the PR body says so.
- **The pre-PR sweep** from `AGENTS.md`'s "Workflow": prose the diff contradicts, every row of a
  list it touches, a test that can't fail, banned words in comments, and a decision recorded only
  in a module doc.

## Settled questions

- **Delivery model: decided by the maintainer.** One object per window, every batch delivered at
  its object's completion, as Kafka Connect does; no acknowledgment on staging and no sink-owned
  staging directory.
- **Key: decided by the maintainer.** The first batch's store identity, with no conditional write.
- **Scope: decided by the maintainer.** The interop harness covers MinIO, Ceph RGW, and AWS;
  `replay_in` reads gzip and zstd in this stream; the formats, native append, concurrency, and the
  soak stand-in wait.

## Decided in W0, for review

These came out of writing the ADRs; a reviewer may push on any of them:

- `Output::collecting` returns the deadline, and `window()` keeps its meaning, rather than
  `window()` returning `staged + 1` ([ADR `collecting-sink-window`](../adr/collecting-sink-window.md),
  decision 1).
- Store pressure is a push that finds no room: one that waits, one that sheds, or a memory
  `drop_oldest` push accepted over the bound with nothing evictable (decision 3 of that ADR).
- `400` `EntityTooLarge` and `413` are `Refused`, not `Rejected`, since every body is at most
  `part_size` ([ADR `s3-object-archive`](../adr/s3-object-archive.md), decision 6).
- An `Ambiguous` answer to `CreateMultipartUpload` or `UploadPart` is `Clean` (decision 6).
- An upload abandoned in the process is aborted once, best effort, and a failed part poisons its
  object (decision 1).
- The head pins its batch count, which only falls, and `object_time` carries milliseconds, so a
  restart can't repeat a dead process's key (decision 3). With the pin, the maintainer's reason
  against `If-None-Match` no longer arises; the ADR keeps the decision on other grounds.
- `InvalidPart`, `InvalidPartOrder`, and `EntityTooSmall` are `Clean`, holding on a sink defect
  rather than dropping a batch per attempt, and every `503` with an S3 code is `Clean`
  (`s3-object-archive`, decision 6).
- `bucket` is validated as not empty only, since stores' naming rules differ (`s3-object-archive`,
  decision 6).
- A window closes when its decoded batches reach `buffer.max_bytes` on either store, and rule 35
  accepts `buffer.max_bytes` beside `disk:` on a collecting sink ([ADR
  `collecting-sink-window`](../adr/collecting-sink-window.md), decision 2). Dropping read-ahead
  and re-reading on a fault is rejected for now.
- A closed window's answers are all read before the next window opens, through `window()`
  reading the answers owed (decision 1 of that ADR).
- zstd writes one frame per staged batch, since `ruzstd` 0.9 has no streaming encoder (`s3-object-archive`, decision 4).
- A hand-rolled XML extractor, not an XML crate, and a `Content-Type` per format (`s3-object-archive`, decisions 4 and 5).
- `timeout` defaults to `60s` (`s3-object-archive`, decision 7).

## Findings

Recorded per workstream as each lands.
