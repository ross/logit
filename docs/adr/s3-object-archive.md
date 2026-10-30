---
created: 2026-10-30
updated: 2026-10-30
---

# `s3_out`: one object per time and size window, delivered at completion, under a key its first batch names

## Status
Accepted. Built in W3a (the single-`PutObject` path) and W3b (the multipart path) of [the
plan](../plans/s3-object-archive.md). "Implementation notes (2026-10-30)", below, records what
each decided that this record left open. The
window mechanics are [ADR `collecting-sink-window`](collecting-sink-window.md), and the response
classes that depart from the shared default are recorded in [ADR
`sink-fault-classes`](sink-fault-classes.md)'s "Amendment: `s3_out`'s departures from the
status-only default (2026-10-30)".

## Context

An object store is where telemetry goes to be kept: cheap, durable, and read back later by a
query engine or by `logit`'s own `replay_in`. `logit` has no sink for one.

Plain S3 has no append. An object is written whole by `PutObject`, or assembled from parts by a
multipart upload and made visible, all at once, by `CompleteMultipartUpload`. Native append
exists only per provider: S3 Express One Zone directory buckets (`x-amz-write-offset-bytes`),
Ceph RGW (`?append&position=`), Azure Append Blob, and Google Cloud Storage's zonal appendable
objects. So an archiving sink puts many batches into one object, and has to decide three things:
when a batch counts as delivered, where batches wait until their object is stored, and what key
an object gets so that a retry or a crash neither loses data nor silently replaces it.

The shippers split on the first two:

| Shipper | Accumulates in | Acknowledges a batch when | Object closes at | Key, and a retry |
|---|---|---|---|---|
| Fluent Bit `out_s3` | a local `store_dir` file | written to `store_dir` | `upload_timeout` 10m or `total_file_size` 100M; 5M parts streamed as they fill | a template; the upload id and part ETags are persisted and resumed |
| Logstash `s3` | a local temp file, not fsynced | written to the temp file | 5 MiB or 15 min | `restore` re-uploads leftover temp files at start |
| Cribl Stream | local disk staging | staged | 32 MB, 300 s open, or 30 s idle | a template |
| Vector `aws_s3` | an in-memory batch | its one `PutObject` succeeds | 10 MB or 300 s | `date=%F/<unix>-<uuid>.log.gz`; a retry reuses the key; the default memory buffer loses data on a crash |
| Fluentd `out_s3` | a buffer chunk | the chunk's one `PutObject` succeeds | the chunk's limits | `%{index}`, probed for a free key |
| OTel Collector `awss3exporter` | one export request | its `PutObject` succeeds | per request | random, so a retry duplicates |
| Amazon Data Firehose | the service | accepted by the service | 5 MiB or 300 s by default | the service's |
| Kafka Connect S3 sink | one part in memory (`s3.part.size` 25 MiB) | offsets commit only after `CompleteMultipartUpload` | its rotation settings | `<topic>+<partition>+<startOffset>`; a crash abandons the upload and replays from Kafka into the same key |
| Datadog log archives | the service | the service's | the service's | `dt=<YYYYMMDD>/hour=<HH>/archive_<HHmmss.SSSS>.<UUID>.json.zst` |

Those that acknowledge on staging own a durable staging area with its own crash recovery.
Those that acknowledge on `PUT` either write one small object per batch or lose what's in memory
on a crash. Kafka Connect commits nothing upstream until the object is complete, and replays an
interrupted object into the same deterministic key. In `logit`, the sink's store plays Kafka's
part: the memory queue or the `buffer.disk:` spool holds every uncommitted batch and gives it
back after a fault or a restart.

What `logit` already has:

- **The window protocol.** `Output::submit` and `Output::await_ack` keep a reserved prefix of the
  store in flight and commit it head by head ([ADR
  `native-hop-send-window`](native-hop-send-window.md), decision 4). [ADR
  `collecting-sink-window`](collecting-sink-window.md) extends it so the prefix stays open
  across arrivals.
- **A store identity per batch.** Every sink sees each batch's `SeqId` through
  `Output::observe_batch`: a sender id minted at random each time the store opens, and a sequence
  number. A spooled record keeps the pair it was spooled with, so a replay after a restart
  carries the original pair ([ADR
  `native-hop-identity-and-sequence`](native-hop-identity-and-sequence.md)).
- **AWS signing and credentials.** `crates/logit-aws` (PR #926) holds SigV4 on `ring`
  (`logit_aws::sigv4::Signer`) and the AWS default credential chain (`logit_aws::creds::Chain`,
  with `Chain::forget` to drop cached credentials), moved out of `kafka_out`'s `AWS_MSK_IAM` ([ADR
  `kafka-sasl`](kafka-sasl.md)).
- **Encoders and a reader.** `stdio_out`'s `format: json` lines
  (`crates/logit-outputs/src/ndjson.rs`, [ADR `stream-json-format`](stream-json-format.md)) and
  bare native frames ([ADR `file-output-native-format`](file-output-native-format.md)), both of
  which `replay_in` reads ([ADR `replay-input`](replay-input.md)); `flate2` and `ruzstd` are
  already dependencies.

Sources: the [Kafka Connect S3 sink
overview](https://docs.confluent.io/kafka-connectors/s3-sink/current/overview.html) and
`kafka-connect-storage-cloud`'s `TopicPartitionWriter.java` and `S3OutputStream.java`; [Fluent
Bit's S3 output](https://docs.fluentbit.io/manual/pipeline/outputs/s3); [Vector's `aws_s3`
sink](https://vector.dev/docs/reference/configuration/sinks/aws_s3/); [Logstash's `s3`
output](https://www.elastic.co/guide/en/logstash/current/plugins-outputs-s3.html); AWS's [S3
error responses](https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html), [multipart
upload limits](https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html), and
[`CompleteMultipartUpload`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html).

## Decision

`s3_out` will be a sink in `crates/logit-outputs/src/s3/` that keeps one object open over a time
and size window, encodes and compresses each batch into it as the batch arrives, stores it with
one `PutObject` or a multipart upload, and counts a batch delivered only once its object is
complete. Every batch waits in the sink's own store until then. An object's key is named by its
first batch's store identity, so a retry or a replay overwrites only data it writes again.

### 1. An object per window, delivered at completion

The maintainer chose the Kafka Connect model.

- **An object is open over a window.** Each batch is encoded and compressed into the open object
  when it's staged. Once the compressed body passes `part_size`, the sink uploads a part and the
  object becomes a multipart upload; an object that closes under one `part_size` goes up as one
  `PutObject`.
- **What closes it.** `max_bytes` of encoded, uncompressed bytes; `max_age` since its first batch
  was staged; store pressure; the staged batches' memory reaching `buffer.max_bytes`; the store
  closing; or the shutdown signal, not the grace expiry ([ADR
  `collecting-sink-window`](collecting-sink-window.md), decisions 2, 3, and 5). A memory store
  also closes it at `buffer.max_batches`. A retry of an object closes at its pinned batch count at
  the latest (decision 3).
- **Delivered means complete.** Every batch in the object stays in the sink's memory queue or
  `buffer.disk:` spool, uncommitted, until `PutObject` answers `200` or
  `CompleteMultipartUpload` answers `200` without an `<Error>` body. No batch counts delivered
  earlier, so nothing is acknowledged from memory, and the sink owns no staging directory.
- **A crash abandons the open upload.** Its parts are invisible and billed until an
  `AbortIncompleteMultipartUpload` lifecycle rule removes them, and the operator docs will
  require that rule on the bucket. The upload id isn't persisted. A disk spool replays the
  object's batches into a new object; a memory store loses them with the process, as it loses
  everything else it holds.
- **A failed part poisons the object.** A part upload that fails inside `submit`, past the head,
  is left unclassified by the loop, which stops the fill and reads the answers owed. The sink
  records the failure, never completes that upload, and fails the head's next `await_ack` without
  `HeadOnly`, with the failure's class, so the loop resubmits from the head into a new upload.
  The batch whose submit failed is in that resubmit, though its bytes were already in the
  abandoned compressor stream.
- **An upload abandoned in the process is aborted.** When a fault restarts an object that has
  parts, the sink sends `AbortMultipartUpload` for the old upload id once, bounded by `timeout`,
  and counts a failed abort without retrying it; the lifecycle rule is the backstop.
- **Parts.** Every part but the last is `part_size` bytes of compressed body, cut there with the
  remainder carried into the next part, because Cloudflare R2 requires every non-final part to be
  the same size and S3 requires each to be at least 5 MiB. An object holds at most 10,000 parts, S3's limit, which
  the configuration bounds (decision 7).
- **Memory.** Per sink: the staged batches, bounded by `buffer.max_bytes` on either store (a disk
  store holds the batches it has read ahead decoded until they're committed), about one part of
  compressed body plus one batch's compressed overshoot, and the compressor's state. An object
  never holds more than `buffer.max_bytes` of decoded batches, so a larger object means raising
  `buffer.max_bytes`; `buffer.disk:` buys durability across a restart, not size.
  [`docs/design/memory.md`](../design/memory.md) records the measured cost when W3 lands.

### 2. Realized through the collecting window

`s3_out` reports a window of `usize::MAX` and returns its object's `max_age` deadline from
`Output::collecting` while the object is open and under `max_bytes` ([ADR
`collecting-sink-window`](collecting-sink-window.md), decision 1). From the close until every
batch of the object is answered, its `window()` reads the answers it still owes and
`collecting()` reads `None`:

- **`observe_batch`** saves the batch's `SeqId`. The first batch staged into an object fixes the
  object's key.
- **`submit` stages.** It encodes the batch, appends it to the compressor, hashes the compressed
  bytes into the current part as they're produced, and uploads a part when one fills. Each step
  is per batch because `run_output` runs `drain_inbox` and `write_loop` in one task: compressing
  a whole object at its close would stall the inbox for that long. Every request is bounded by
  `timeout`.
- **`await_ack` on the head completes the object**: it flushes the compressor, uploads the last
  part or the single `PutObject`, and sends `CompleteMultipartUpload`. It answers the head with
  the object's outcome, and each later `await_ack` answers the next batch of the object `Ok` at
  once; the owed count in `window()` keeps the loop from submitting or waiting in between.
- **A fault covers the object.** A failed request returns a fault without `HeadOnly`, so the loop
  resubmits from the head and the sink starts the object again under the same key, holding no
  more batches than its pinned count (decision 3).
  A `Rejected` answer drops the head batch alone, as the runtime drops it for any windowed sink,
  and the rest are staged again into a new object.
- **One object at a time.** The next object opens only once every batch of the last one is
  answered, so a batch that arrives during an upload waits in the store. Parts of one object
  upload one at a time, too.

### 3. The key is the first batch's identity, with no conditional write in v1

The maintainer chose this key and kept conditional writes out of v1.

```text
{prefix}{partition formatted at object_time}{object_time}-{store id}-{first seq}.{ext}
logs/dt=2026-10-30/hour=14/20261030T142501.123Z-6f1c...e09a-00000000000000000042.ndjson.gz
```

- **The pieces.** `prefix` and `partition` come from config; `object_time` is the wall clock in
  UTC, compact ISO 8601 to the millisecond (`20261030T142501.123Z`), when the object's first batch
  was staged; the store id is the first batch's 16-byte sender id in lowercase hex; the first seq
  is its sequence number, zero-padded to 20 digits so keys sort by sequence within a millisecond.
- **The head pins `object_time` and the batch count.** The sink remembers, against the head's
  `SeqId`, the object's `object_time` and, from the object's first close, its batch count. Every
  retry of that head in the same process, whatever the fault's class, writes the same key and
  closes at that count at the latest. A retry that closes earlier (store pressure, the memory
  bound, shutdown) lowers the pin to its own count, so the pin only falls. The pinned batches are
  uncommitted, so the store still holds every one of them, reserved, when a retry stages them,
  under store pressure too. A restart stamps a new `object_time`; milliseconds keep it from
  repeating the dead process's stamp, since a restart takes longer than that, unless the wall
  clock steps back across the restart.
- **The default partition** is Hive-style, `dt=%Y-%m-%d/hour=%H/` in UTC. It is the time the
  object was staged, not any event's own time, and the operator doc will say so. Partitioning by
  an attribute isn't in v1; a `route` into several `s3_out`s, each with its own `prefix`, does it.
- **Why an overwrite never loses data.** Two cases.
  - *A request the sink starts.* The sink writes an object at key K, whose first batch is s, only
    while s is uncommitted. The store commits in order, so every batch from s onward is
    uncommitted too, and any earlier object at K, which held only batches from s onward, holds
    nothing the store has let go. Those batches are staged again, into this object or a later
    one, before any of them is committed. So replacing the earlier object at K drops nothing that
    isn't written again.
  - *A request still in flight at the store.* An `Ambiguous` `PutObject` or
    `CompleteMultipartUpload` can land after its retry has completed and committed its batches.
    AWS orders concurrent uploads to one key only on a versioned bucket; otherwise "any other
    request received between the time when the multipart upload is initiated and when it
    completes ... might take precedence" ([Concurrent multipart upload
    operations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html#distributedmpupload)),
    and MinIO and Ceph RGW state nothing. The pin makes the late write harmless: every attempt at
    K holds a prefix s..s+n of the store, and no retry holds more batches than any attempt before
    it, so a late earlier attempt holds every batch the retry committed, and the batches past the
    retry's are uncommitted and staged again. After a restart the replay's `object_time`, and so
    its key, differs from any request the dead process left in flight, and the late write is a
    duplicate.

  A different key comes only from a different first batch (the head changed, by a `Rejected`
  drop or a commit) or a new `object_time` (a restart); regrouping behind the same head writes the
  same key. A different key is a duplicate, which at-least-once allows. The argument doesn't need
  a deterministic encoding, though the `json` and native encoders are deterministic.
- **What an overwrite looks like.** It is the at-least-once duplicate in S3's terms: a second
  `ObjectCreated` event notification for the key, and a second version on a versioned bucket.
- **No `If-None-Match: *` in v1.** The maintainer first ruled it out because a retry could hold
  more batches than the object already at its key, and a `412` would drop the extra ones. The pin
  removes that case: a retry never holds more than an earlier attempt at its key, so a `412` means
  an object at least as complete is already there, and reading it as delivered would be safe. The
  maintainer confirmed it stays out of v1 anyway, since an overwrite is already loss-free; it is a
  follow-up.
- **No secret in a key, and none in a message.** The key holds no config value but `prefix` and
  `partition`. Messages name the bucket and print `endpoint:` only through
  `logit_core::redact::url`.

### 4. Formats and compression

- **`format: json`**, the default, writes `stdio_out`'s `format: json` lines, which `replay_in`
  reads. **`format: native`** writes concatenated bare native frames, as `file_out` does under
  `format: native`.
- **`compression: gzip`**, the default, through `flate2`, as one gzip member per object.
  **`zstd`**, through `ruzstd`, as one zstd frame per staged batch (amended: one per 1 MiB of
  input, in "Implementation notes (2026-10-30)"): `ruzstd` 0.9's
  `FrameCompressor` compresses a whole input into one frame and has no streaming encoder, and a
  zstd stream of concatenated frames is valid and reads back as one. W3 measures what the
  per-batch frames cost in ratio against a single frame. **`none`** writes the encoding as it is.
- **Extensions**, lowercase: `.ndjson.gz`, `.ndjson.zst`, and `.ndjson` for `json`; `.logit.gz`,
  `.logit.zst`, and `.logit` for `native`.
- **No `Content-Encoding` header**, because a client fetching the object would decompress it in
  transit and hand on bytes whose name says they're compressed. `Content-Type` will be
  `application/gzip`, `application/zstd`, `application/x-ndjson`, or `application/octet-stream`.
- **Deferred:** the OTLP formats and Parquet.

### 5. Upload, signing, and credentials

- **Fixed-length bodies, signed by their hash.** `PutObject` and `UploadPart` send a known
  `Content-Length` and the body's hex SHA-256 in `x-amz-content-sha256`, computed while the part
  fills (`ring::digest::Context`). No `aws-chunked` encoding and no trailing checksums: the AWS
  SDKs' 2025 default of trailing checksums broke Ceph RGW and other S3-compatible stores.
- **SigV4 header auth** through `logit_aws::sigv4::Signer`: the `Authorization` header and
  `x-amz-security-token` assembly are new in `s3_out`; the signer and the canonical request
  aren't.
- **Credentials.** A `credentials:` block with `access_key_id` and `secret_access_key` (with
  `!env`, [ADR `env-yaml-tag`](env-yaml-tag.md)) and an optional `session_token`, or, without the
  block, `logit_aws::creds::Chain`. An `ExpiredToken` answer calls `Chain::forget` before the
  retry.
- **Addressing.** `region:` is required. Without `endpoint:`, the sink uses AWS's regional
  endpoint, virtual-hosted (`https://<bucket>.s3.<region>.amazonaws.com`, with the partition's
  domain); with `endpoint:`, path-style (`<endpoint>/<bucket>/<key>`). `path_style:` overrides
  either. A bucket name containing `.` needs `path_style: true` over TLS on AWS, because the
  wildcard certificate covers one label.
- **No redirect following.** A `3xx` is `Refused`, and its diagnostic carries the
  `x-amz-bucket-region` header so the operator can fix `region:` (decision 6).
- **Optional headers.** `storage_class:` sets `x-amz-storage-class`; `sse:` sets
  `x-amz-server-side-encryption` to `AES256` or `aws:kms`, with an optional KMS key id and the
  bucket-key flag. No ACLs and no object tags.
- **XML.** `CreateMultipartUpload`'s `<UploadId>`, an error body's `<Code>` and `<Message>`, and
  `CompleteMultipartUpload`'s answer are read by a small extractor in the sink over a bounded
  read, with no XML crate. `CompleteMultipartUpload` can answer `200` with an `<Error>` body, and
  the sink reads that body before it counts the object complete.

### 6. Response classes

S3 never interprets an object's content, so no S3 answer is about one batch except a size, and
an object's size is set by the sink's configuration, not by a batch: every body is at most
`part_size`, since a batch larger than a part is split across parts. [ADR
`sink-fault-classes`](sink-fault-classes.md)'s rule that an unattributable `4xx` is `Rejected`
stands, so every S3 answer the sink distinguishes is listed. An answer that reaches the "any other
`4xx`" row costs one batch per object attempt: the runtime drops the head alone, counted
`batches.dropped{reason="rejected"}`, and the next attempt uploads the rest of the object again.
That is why every configuration code the sink knows of is read `Refused`. The canonical copy is
`crates/logit-outputs/src/s3/mod.rs`'s module doc, each row backed by a test; this table was its
first copy and isn't updated.

| Answer | Class | Why |
|---|---|---|
| `2xx`; `CompleteMultipartUpload` `200` with no `<Error>` body | delivered | the object is stored |
| `200` from `CompleteMultipartUpload` with an `<Error>` body | the class of its `<Code>` below | AWS documents the error-in-`200` case |
| `301` `PermanentRedirect`, `307` `TemporaryRedirect`, any other `3xx` | `Refused` | the wrong region or endpoint: the same answer for every object; the diagnostic carries `x-amz-bucket-region` |
| `400` `RequestTimeout`, `IncompleteBody`, `BadDigest`, `XAmzContentSHA256Mismatch` | `Clean` | the body didn't arrive whole, so nothing was stored |
| `400` `ExpiredToken`, `TokenRefreshRequired` | `Clean`, after `Chain::forget` | the credentials expired; fresh ones will sign the retry |
| `400` `EntityTooLarge`, `413` | `Refused` | a body is at most `part_size`, so the destination's limit is below it: configuration, not a batch |
| `400` `InvalidArgument`, `InvalidToken`, `AuthorizationHeaderMalformed`, `InvalidBucketName`, `InvalidEncryptionAlgorithmError`, any `KMS.*`, `InvalidStorageClass`, `AccessControlListNotSupported`, `KeyTooLongError`, `InvalidRequest` | `Refused` | configuration: the same answer for every object |
| `400` `InvalidPart`, `InvalidPartOrder`, `EntityTooSmall` | `Clean` | the sink built a bad upload; nothing is visible yet, so the object starts again |
| `403`, any code | `Refused` | credentials or policy |
| `404` `NoSuchBucket`, and any other `404` | `Refused` | the bucket or endpoint is wrong |
| `404` `NoSuchUpload` | `Clean` | the upload was aborted underneath the sink; the object starts again |
| `408` | `Ambiguous` | the shared default ([ADR `sink-fault-classes`](sink-fault-classes.md)'s "Amendment: 408 is `Ambiguous`") |
| `409` `OperationAborted` | `Clean` | a conflicting operation was in progress; nothing was stored |
| `411` `MissingContentLength` | `Refused` | a proxy or store that won't take the request's framing |
| `429`; any `503` with an S3 XML `<Code>` (`SlowDown`, `ServiceUnavailable`, MinIO's `SlowDownWrite` and `SlowDownRead`) | `Clean` | the store refused the request before processing it; `PutObject` and `UploadPart` are atomic |
| `503` with no S3 `<Code>` | `Ambiguous` | a load balancer's answer, which says nothing about the store: the shared default |
| `500` (`InternalError` or any other code), `502`, `504`, and any other `5xx` | `Ambiguous` | the request may have been applied; a retry rewrites the same key; the shared default |
| `501` `NotImplemented` | `Refused` | the store lacks a feature the sink uses |
| any other `4xx` | `Rejected` | [ADR `sink-fault-classes`](sink-fault-classes.md)'s rule for an answer the sink can't attribute |
| connect failure; a dial `connect_timeout:` ended | `Clean` | the shared HTTP driver's default |
| request timeout, or another transport error after the connection opened | `Ambiguous` | the shared HTTP driver's default |

- **Nothing is visible before completion.** An answer the table reads `Ambiguous` to
  `CreateMultipartUpload` or `UploadPart`, transport errors included, is `Clean`: no object is
  visible at the key until `CompleteMultipartUpload` succeeds, so the request couldn't have
  stored a batch. Only `PutObject` and `CompleteMultipartUpload` can be `Ambiguous`, which keeps
  `at_most_once` from dropping an object that was never stored.
- **A bad upload holds, it doesn't drop.** `InvalidPart`, `InvalidPartOrder`, and `EntityTooSmall`
  mean the sink assembled the upload wrong, a defect in `logit`, not in a batch or in the
  configuration. Read `Clean`, they restart the object; if the defect repeats, the sink holds,
  `logit.component.retrying` reads `1`, and `logit.component.errors` climbs, so the defect is
  seen with no data lost. Read `Rejected`, they would drop a batch per object attempt, counted as
  loss, for a fault no batch caused.
- **No bucket-name syntax check.** `bucket` is validated as not empty and nothing more: S3's
  naming rules and those of S3-compatible stores differ, and validation never rejects a name a
  store might accept. A name the store refuses answers `InvalidBucketName`, which holds as
  `Refused`.
- **Posture.** The default, `at_least_once`, because a retry rewrites the same key.
- **Retries are the loop's.** Every retryable class restarts the object from its head through
  the window protocol, holding no more than the head's pinned batch count (decision 3); the sink
  retries no request of its own.

### 7. Configuration

Every field but `bucket:` and `region:` will default. The fields:

| Field | Default | Validation |
|---|---|---|
| `bucket` | required | not empty |
| `region` | required | not empty |
| `endpoint` | AWS's regional endpoint | an `http://` or `https://` URL with no path, query, or userinfo |
| `path_style` | `true` with `endpoint:`, else `false` | |
| `prefix` | empty | the longest key `prefix` and `partition` can produce fits S3's 1,024 bytes |
| `partition` | `dt=%Y-%m-%d/hour=%H/` | a strftime pattern `jiff` can format |
| `format` | `json` | `json` or `native` |
| `compression` | `gzip` | `gzip`, `zstd`, or `none` |
| `max_bytes` | `32MiB` | above `0`, and at most `part_size` × 9,000, which keeps an object under 10,000 parts with room for compression overhead on incompressible data and the batch that crosses `max_bytes` |
| `max_age` | `5m` | above `0s` |
| `part_size` | `8MiB` | 5 MiB to 5 GiB, S3's part bounds |
| `storage_class` | none | not empty |
| `sse` | none | `aes256`, or `aws_kms` with an optional `kms_key_id` and `bucket_key` |
| `credentials` | none: the AWS default chain | `access_key_id` and `secret_access_key` together; an optional `session_token` |
| `timeout` | `60s` | above `0s`; bounds every request, an 8 MiB part included |
| `connect_timeout` | `5s` | above `0s`, as on the HTTP sinks |
| `idle_timeout` | `30s` | as on the HTTP sinks |
| `tls` | none | the HTTP sinks' client `tls:` block, for a private CA or mutual TLS |

- **Why these defaults.** `max_age: 5m` matches Vector's, Cribl's, and Firehose's 300 s.
  `max_bytes: 32MiB` matches Cribl's 32 MB and fits under the memory store's default 64 MiB
  `buffer.max_bytes`, below the memory bound, with room for arrivals to queue in the store while
  an object uploads. `part_size:
  8MiB` is above S3's 5 MiB minimum and keeps one part's buffer small.
- **No `concurrency:`.** One object uploads at a time (decision 2); the field is absent rather
  than fixed at 1.
- **`buffer:`** is the ordinary block. `buffer.max_bytes` bounds an open object's decoded
  batches on either store, so graph rule 35 will accept a non-default `buffer.max_bytes` beside
  `disk:` on `s3_out` ([ADR `collecting-sink-window`](collecting-sink-window.md), decision 2).
  The operator doc will recommend `buffer.disk:` for durability across a restart, a
  `buffer.shutdown_grace` long enough for one object's upload, and a `buffer.max_bytes` above
  `max_bytes`, so the memory bound and store pressure are the exception.
- **Graph rules** for the bounds above join [`docs/design/pipeline-graph.md`](../design/pipeline-graph.md)'s
  rule table in W3.

### 8. Telemetry

- **Requests.** `logit.output.requests{op, class}`, `op` one of `put_object`,
  `create_upload`, `upload_part`, `complete_upload`, or `abort_upload`, and `class` the HTTP
  sinks' status vocabulary; `logit.output.request.duration{op}`.
- **Objects.** `logit.output.objects` per completed object, `logit.output.parts` per uploaded
  part, and `logit.output.bytes` per compressed byte sent, resends included.
  `logit.output.uploads.abandoned{aborted}` counts an upload the sink gave up in the process,
  `aborted="false"` when its abort failed and the lifecycle rule must remove its parts.
- **Loss** is counted by the runtime alone, `batches.dropped{reason}`, under [ADR
  `telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md): no `s3_out` counter says
  `dropped`, since an abandoned upload loses nothing the store doesn't give back.
- **The window's own** `logit.component.window.closed{cause}` and `send.duration` per object come
  from [ADR `collecting-sink-window`](collecting-sink-window.md), decision 4.

### 9. Verification and scope

The maintainer set both.

- **`script/s3-interop`**, an out-of-CI harness: pinned MinIO and Ceph RGW containers, and real
  AWS S3 legs from a gitignored env file, as `script/splunk-interop`'s cloud target reads its
  stack. Not Cloudflare R2, Backblaze B2, or Google Cloud Storage.
- **`replay_in` reads gzip and zstd files**, detected by their magic bytes, so an archived object
  replays as it was written.
- **Not in v1:**
  - a soak stand-in shape for `s3_out`; it isn't among the sinks whose tables `AGENTS.md`
    requires restated as soak steps, and it's a follow-up;
  - the OTLP formats and Parquet;
  - native append, on S3 Express One Zone or Ceph RGW;
  - conditional writes (`If-None-Match: *`), safe under the pin (decision 3) and a follow-up;
  - several objects, or several parts, in flight at once;
  - an object-store reader for `replay_in`;
  - partitions by attribute or by event time;
  - Object Lock buckets, which require `Content-MD5` or an SDK checksum header on every
    `PutObject` (such a bucket answers `400` `InvalidRequest`, `Refused`);
  - following redirects, ACLs, and object tags.


## Implementation notes (2026-10-30)

W3a built the single-`PutObject` path and W3b the multipart path. Between them they decided these
points the record left open.

- **The close picks the request.** The close finishes the compressed stream, which can write
  more than one part's worth: `gzip`'s deflate stream flushes what it held back, and `zstd`
  compresses the chunk not yet compressed. An object whose close leaves one part, with none
  uploaded, is one `PutObject`; any other is a multipart upload, even one whose staging cut no
  part. So every request body is at most `part_size`, which is what decision 6's `EntityTooLarge`
  row rests on.
- **A part failure at the head is the head's own.** Decision 1's poisoning covers a part that
  fails past the head, which the loop leaves unclassified. One that fails inside the head's own
  `submit`, when the head alone fills a part, is the head's fault, which the loop classifies as
  any submit failure with nothing in flight: the sink aborts the upload and fails the submit with
  the part's class, and keeps no poison, so a `Rejected` head is dropped alone and the next batch
  opens its own object.
- **The loop resubmitting an object's first batch starts the object again**, whatever state a
  cancelled `submit` or `await_ack` left, and aborts any upload that call left open.
- **An abandoned upload is aborted at once.** The abort is sent from the call whose request failed,
  before its fault returns, so a held queue doesn't leave uploads open between attempts. It
  follows a failed `CompleteMultipartUpload` too, `Ambiguous` included: an abort after a
  completion that did apply answers `NoSuchUpload`, which counts as `aborted="true"`, since no
  parts are left either way.
- **An `<Error>` in a `200` `CompleteMultipartUpload`** is read at the status S3 answers its code
  with (`InternalError` `500`, `SlowDown` `503`, `NoSuchUpload` `404`, and so on), and a code the
  table has no row for, or none, as `500`: the request may have been applied. The answer is read
  whole, up to 64 KiB, since S3 can send whitespace before it; a body cut by a transport error or
  over that bound is `Ambiguous`.
- **A `200` missing what the sink needs is `Clean`.** A `CreateMultipartUpload` with no
  `<UploadId>` or an `UploadPart` with no `ETag` stored nothing visible; the object starts again.
- **The object's headers go on the request that creates it.** `Content-Type`, `storage_class:`,
  and `sse:` go on `PutObject` or `CreateMultipartUpload`; `UploadPart`, `CompleteMultipartUpload`
  (with `Content-Type: application/xml`), and `AbortMultipartUpload` carry none of them.
- **`logit.output.bytes` counts object bytes**, the `PutObject` and `UploadPart` bodies, not
  `CompleteMultipartUpload`'s part list.
- **No `s3:ListMultipartUploadParts`.** The sink keeps each part's `ETag` from its answer and
  never lists an upload's parts.
- **An encode that fails past the head doesn't poison the object.** The failed batch was never
  staged, so the loop's unclassified stop completes the object without it, and the batch fails
  again at the head of the next object, where it's dropped alone as `Rejected`. Poisoning the
  object would drop the healthy head batches ahead of it one per attempt.
- **AWS addressing without `endpoint:`** is `<bucket>.s3.<region>.amazonaws.com`, or
  `.amazonaws.com.cn` for a `cn-` region. GovCloud's `us-gov-` regions share `amazonaws.com`;
  any other partition needs `endpoint:`.
- **What's signed.** `host`, `content-type`, and every `x-amz-*` header the sink sends;
  `Content-Length` isn't, as AWS's own examples don't sign it.
- **Static credentials and `ExpiredToken`.** With `credentials:` set there's nothing to forget,
  so the answer stays `Clean` and the retry signs with the same token until the operator replaces
  it, announced by `logit.component.retrying`.
- **`partition` validation** formats the pattern at 23:59:59.999 on each month's 20th to 26th
  day in year 9999 (every weekday and month name), at millisecond 999 and at millisecond 0
  (where `%.f` writes nothing), and takes the longest result with the longest
  key tail. `endpoint:` may end in one `/`.
- **`logit.output.parts`** counts `UploadPart` requests only, so a `PutObject` counts none.
- **zstd writes a frame per 1 MiB of encoded input**, not per batch: the encoding collects in a
  reused buffer, a frame is compressed each time it reaches `ZSTD_CHUNK_BYTES` (1 MiB), and the
  close compresses the rest. Compressed bytes, with `ruzstd` at its fastest level, for each
  fixture staged into one object:

  | Fixture, encoding | `gzip` | `zstd`, 64 KiB chunks | `zstd`, 256 KiB | `zstd`, 1 MiB | `zstd`, 4 MiB | `zstd`, one frame |
  |---|---|---|---|---|---|---|
  | 50 nginx batches of 100, `json` (5.4 MB) | 29,401 | 54,559 | 27,800 | 27,605 | 27,553 | 27,540 |
  | 50 nginx batches of 100, `native` (1.9 MB) | 13,399 | 15,826 | 7,983 | 7,905 | 7,892 | 7,892 |
  | 500 nginx batches of 10, `native` (2.0 MB) | 10,515 | 16,554 | 8,605 | 8,527 | 8,514 | 8,514 |
  | 50 statsd batches of 100, `json` (1.2 MB) | 4,861 | 5,831 | 3,081 | 3,042 | 3,029 | 3,029 |

  A frame per batch had cost up to ten times the single frame's size, more than `gzip` in every
  case. At 1 MiB the frames are within 0.5% of one frame for the whole object, and 4 MiB buys under
  0.2% more for four times the buffer, so 1 MiB it is. The fixtures repeat one batch, which
  favors one long frame. The chunk not yet
  compressed lags the part cut by up to 1 MiB of encoded input, as gzip's deflate buffer
  does by less.
- **The module doc's table is canonical, and these notes record each change to it.** The table
  in decision 6 is as first written; the later rows are below and in
  `crates/logit-outputs/src/s3/mod.rs`'s "Response classes".
- **`400` `EntityTooLarge` and `413` rest on every body being at most `part_size`**, as decision
  6's row says, once parts are cut at `part_size` ("The close picks the request", above). While
  every object was one `PutObject`, closed once its body reached `part_size`, a body could be
  `part_size` plus one batch.
- **`401`, `405`, and `407` are `Refused`**, the shared HTTP default, and not decision 6's "any
  other `4xx`": each says the same thing about every object (credentials, a method the store or a
  proxy won't take, a proxy's credentials), and `Rejected` would drain the queue one head batch
  per attempt.
- **A `KMS.*` code is `Refused` under any `4xx`.** MinIO answers
  `KMS.DefaultKeyAlreadyConfiguredException` with `409`.
- **`AccessControlListNotSupported` has no row.** It answers a request carrying an ACL, and the
  sink sends none.
- **A head that fills the object alone bounds the fill.** No collecting window opens then, so
  the sink's `window()` reads the batches staged while the object is full, and the loop's
  ordinary fill stops there instead of submitting every batch the store holds.
- **`prefix` and `partition` may not make a `.` or `..` segment**: a URL drops it, while SigV4
  for S3 signs the path as written, so every request would answer `SignatureDoesNotMatch`.

## Alternatives considered

- **One object per batch** (the OTel exporter, Fluentd's chunk). Rejected: a batch is tens of
  kilobytes, so an archive becomes millions of small objects, with a request charge each and a
  slow listing for every reader.
- **Acknowledge on staging** (Fluent Bit, Logstash, Cribl). Rejected: a batch would be committed
  from the sink's store before it is stored, so a crash or a failed upload loses it uncounted,
  or needs a second durable store with its own recovery to avoid that.
- **Sink-owned disk staging.** Rejected: `buffer.disk:` is already a crash-recoverable spool of
  every uncommitted batch, so a staging directory would be a second one, with its own fsync,
  bound, recovery, and loss accounting, for what the spool already gives back.
- **Native append** (S3 Express One Zone, Ceph RGW). Rejected for v1: each is one provider's API,
  and plain S3 and most S3-compatible stores have none.
- **A content-hash key.** Rejected: a replay that groups batches differently hashes differently,
  so it never overwrites; and the hash is known only once the object closes, so the key couldn't
  be chosen when the upload starts.
- **A random key per object** (the OTel exporter). Rejected: every retry is a new object, a
  duplicate even when the first attempt failed.
- **Conditional writes (`If-None-Match: *`) in v1.** Deferred, as decision 3 says: the pin makes
  them safe, and an overwrite is already loss-free, so v1 doesn't need them.
- **Retry `CompleteMultipartUpload` on the same upload id before starting over.** It would let a
  late Complete and its retry be the same request. Rejected for the pinned count, which covers a
  late `PutObject` as well and keeps every retry on the loop's one path.
- **Persist the upload id and part ETags, and resume** (Fluent Bit). Rejected: resuming needs the
  same bytes in the same parts after a restart, which the spool's regrouping doesn't promise, and
  the lifecycle rule already removes an abandoned upload.
- **The AWS SDK for Rust.** Rejected: it brings its own HTTP stack, TLS configuration, retry
  loop, and credential chain beside `logit`'s, and its retries hide the attempt from the fault
  classes. Its trailing-checksum default is what broke S3-compatible stores in 2025.
  `logit-aws` already has the signer and the chain `kafka_out` uses.

## Consequences

- **Code.** `crates/logit-outputs/src/s3/` (the sink, the key, the staged object, the XML
  extractor, and the response classes), the `S3Out` variant in `crates/logit-config/src/lib.rs`
  with `script/schema`, the graph rules, and the registry entry in
  `crates/logit-cli/src/pipeline.rs`; `logit-outputs` depends on `logit-aws`. The plan's W3.
- **Docs.** A `docs/deploying.md` section with the lifecycle rule, the IAM permissions
  (`s3:PutObject`, `s3:AbortMultipartUpload`, and the KMS key's under `sse: aws_kms`), the
  dotted-bucket note, the shutdown-grace, `buffer.max_bytes`, and disk-buffer advice, and the
  ingest-time partition note; `AGENTS.md`'s outputs table; an example
  under `examples/`; `docs/design/internal-telemetry.md`; `docs/design/memory.md`.
- **Duplicates.** A replay after a restart writes its batches again under a new key, since its
  `object_time` is new; a late `CompleteMultipartUpload` or `PutObject` after its retry rewrites the
  same key with the same batches or more, so the batches past a retry's lowered pin are stored
  both there and in the next object; and a `Rejected` drop moves the head, so when an earlier
  attempt at the old key landed, the rest of that object is stored twice. An operator
  who needs none deduplicates downstream, as for every other at-least-once sink.
- **Billing.** An abandoned multipart upload is billed until the lifecycle rule removes it.
- **Follow-ups:** the items "Not in v1" lists, and an `s3_out` soak stand-in shape.
