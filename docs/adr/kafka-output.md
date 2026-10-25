---
created: 2026-10-11
updated: 2026-10-25
---

# `kafka_out`: a pure-Rust producer on `kafka-protocol`, with the Java producer's semantics and named divergences

## Status
Accepted

## Context

Kafka is a common landing zone for logs and telemetry: Vector, Fluent Bit, and the OpenTelemetry
Collector each ship a Kafka producer. `logit` has none. [ADR `framed-encoder`](framed-encoder.md)
anticipated "a per-record Kafka/NATS producer" as a framed sink, and [ADR
`sink-fault-classes`](sink-fault-classes.md) compared Kafka Connect's error model in its survey.

A Kafka producer is more than a wire codec. It decides which partition each record lands on,
whether a resend is deduplicated, what each broker error means, and how a request is sized, and a
consumer or a second producer on the same topic depends on each of those decisions. The reference
clients disagree on some of them: the Java producer and librdkafka have different defaults for
idempotence, the request size cap, and the unkeyed and keyed partitioners.

The repository is rustls-only, with no OpenSSL and no C toolchain in the build (`Cargo.toml`,
`Dockerfile`), and every TLS component reloads its certificates through `logit_pipeline::tls`
([ADR `tls-certificate-reload`](tls-certificate-reload.md)).

The facts below were checked against the Java client's source (Kafka 4.x trunk), librdkafka's, and
the `kafka-protocol` 0.18.0 crate, and recorded against a real broker:
[testdata/interop/kafka/README.md](../../testdata/interop/kafka/README.md) has the corpus and what
it shows. [tools/kafka-interop/README.md](../../tools/kafka-interop/README.md) has the interop
legs and what each run against Apache Kafka and Redpanda found.

## Decision

`kafka_out` is a pure-Rust Kafka producer that `logit` owns, in `crates/logit-outputs/src/kafka/`,
built on the `kafka-protocol` crate's message codecs. Its reference semantics are the Java
producer 4.x's defaults, and each place it departs from them is named below with its reason.

### 1. Client: `kafka-protocol`, pure Rust, no C

- **Dependency.** `kafka-protocol = { version = "=0.18.0", default-features = false, features =
  ["client", "broker"] }`. The version is pinned with `=` because a 0.x minor release breaks the
  API. The crate is MIT or Apache-2.0, and it adds `crc` and `crc-catalog` (same licenses) to the
  tree.
- **Compression goes through the crate's custom-compression hook, for all five codecs.** The
  crate's `lz4` feature pulls `lz4-sys` and its `zstd` feature pulls `zstd-sys`, both C, and its
  `snappy` feature writes xerial framing. `RecordBatchEncoder::encode_with_custom_compression`
  calls the hook for every `Compression` value, and the crate's built-in codecs run only when no
  hook is passed, so `kafka_out` compresses every codec itself and enables none of the crate's
  codec features:
  - `none` passes the records through;
  - gzip through `flate2`;
  - snappy as one raw block through `snap::raw` (item 2);
  - lz4 as an LZ4 frame through `lz4_flex`, with FLG `0x60` and BD `0x40` (version 1, independent
    blocks, no block or content checksum, no content size, 64 KB blocks), which is what both the
    Java client and librdkafka write;
  - zstd as a standard frame through `ruzstd`.
- **What the crate provides.** Request and response codecs generated from Kafka 4.1.0's schemas
  (Produce v3 to v13, ApiVersions v0 to v4, Metadata v0 to v13, InitProducerId v0 to v5), and record
  batch v2 (magic 2) with CRC-32C. It has no network client and no async runtime: the connection,
  the producer state, and the partitioners are `logit`'s own.
- **The `broker` feature** decodes requests and encodes responses. It exists here so the tests
  can run an in-process fake broker. No test depends on a running Kafka. The fake broker
  decompresses through `RecordBatchDecoder`'s custom-decompression hook, with the same five
  codecs, plus a xerial snappy reader for the Java corpus: a 16-byte header (the magic, a version, and a
  compatible version), then raw blocks, each prefixed by a big-endian `u32` length.

### 2. Reference semantics: the Java producer 4.x

| Behavior | Java producer 4.x default | `kafka_out` |
|---|---|---|
| Acknowledgment | `acks=all` | `acks: all \| leader \| none`, default `all` |
| Idempotence | `enable.idempotence=true` | `idempotent: true` by default (item 4) |
| Keyed partitioning | `toPositive(murmur2(key)) % numPartitions` over all partitions, seed `0x9747b28c`; an empty non-null key is hashed | The same (item 3) |
| Unkeyed partitioning | KIP-794 built-in partitioner: a random available partition, switched after `batch.size` (16,384) bytes | Sticky per `logit` batch (divergence below) |
| In flight | `max.in.flight.requests.per.connection=5` | One produce request per broker per attempt (divergence below) |
| Batching delay | `linger.ms=5` | None (divergence below) |
| Size cap | `max.request.size=1048576`, checked against a record's uncompressed size | `max_request_bytes`, default 1,048,576, the same check (item 7) |
| Request timeout | `request.timeout.ms=30000` | `timeout`, default 30 s, bounds every connect, write, and acknowledgment wait |
| Retry | `retry.backoff.ms` 100 to 1,000 exponential (KIP-580), until `delivery.timeout.ms` (120 s) | `write_loop`'s backoff, until success or shutdown ([ADR `sink-fault-classes`](sink-fault-classes.md)) |
| Metadata age | `metadata.max.age.ms=300000` | A refresh every 5 minutes, plus on the errors item 5 names |
| Compression | `none`; `gzip`, `snappy`, `lz4`, `zstd` | All five, default `none` |
| Record timestamp | `CreateTime`, the producer's clock | `CreateTime`, the event's timestamp (item 6) |

The named divergences:

- **Unkeyed records stick to one partition per `logit` `send()`.** All of a batch's unkeyed
  records go to one partition, picked at random among the topic's partitions that have a leader,
  and the next batch picks again. Java switches partitions after `batch.size` bytes. `logit`'s
  upstream batch is already the unit of accumulation, so a byte count inside it adds a second
  batching rule with no benefit. librdkafka's sticky partitioner is time-based (it re-picks every
  10 ms), so neither reference client defines the switch point in a way a consumer can observe.
- **Snappy is written raw, as librdkafka writes it.** Java writes xerial `SnappyOutputStream`
  framing (magic `82 53 4e 41 50 50 59 00`). Brokers and Java consumers accept both forms, because
  the xerial reader falls back to a raw block; the corpus has one of each.
- **`CORRUPT_MESSAGE` is `Rejected`.** Java retries it (`CorruptRecordException` is retriable).
  `kafka_out` computes its CRC deterministically from the batch, so a resend carries the same bytes
  and gets the same answer.
- **One produce request per broker per attempt.** Java keeps up to five requests in flight per
  connection. Pipelining through `Output::window` is a follow-up.
- **No linger.** The batch `logit` hands the sink is the batch the producer sends.
- **`MESSAGE_TOO_LARGE` halves a partition batch once.** Java cuts the batch into pieces of
  `batch.size` (16,384) bytes, halves only a batch that is already a split, and re-enqueues the
  pieces as new batches that can split and retry again. `logit` has no `batch.size` accumulator to
  size pieces by, so `kafka_out` halves the partition batch, resends each half once, and reads a
  second `MESSAGE_TOO_LARGE` as `Rejected` (item 7).
- **No delivery timeout.** Java fails a batch after `delivery.timeout.ms` (120 s). `kafka_out`
  retries a retryable fault until it succeeds or shutdown cuts it, and `buffer:` bounds what queues
  behind it, as for every sink ([ADR `sink-fault-classes`](sink-fault-classes.md)).

### 3. Keyed partitioning

A record with a key goes to `toPositive(murmur2(key)) % n`, where `n` counts all of the topic's
partitions, available or not, as Java's does. A keyed record whose partition has no leader waits
for one, through the `Clean` class, instead of moving to another partition. The hash is pinned by
[testdata/interop/kafka/murmur2.json](../../testdata/interop/kafka/murmur2.json): 31 keys,
including empty, multibyte, invalid UTF-8, and 1,500-byte keys, each with the Java client's own
`Utils.murmur2` result and its partition for 1, 3, 6, 12, and 100 partitions. A test that
disagrees with the file is a bug in `kafka_out`.

`key:` takes the shape of `sample`'s key ([ADR
`consistent-sampling-component`](consistent-sampling-component.md)): `none` (the default),
`trace_id`, `{attribute: <name>}`, or `{resource: <name>}`.

A key is text, as UTF-8 bytes, so a consumer and the console tools can show it:

| Value at the key | Key bytes |
|---|---|
| `trace_id` (the span's, else the log's `TraceRef`) | 32 lowercase hex characters |
| A string | Its UTF-8 bytes |
| An integer | Its decimal text |
| A float | Its shortest decimal text that round-trips |
| A boolean | `true` or `false` |
| Bytes | As they are |
| A map, an array, or no value | No key: the record is sent unkeyed, sticky with the rest of its batch, and counted as a missing key |

The trace id's form matches the OpenTelemetry Collector's `kafkaexporter`, whose
`partition_traces_by_id` and `partition_logs_by_trace_id` key a record with the trace id as lowercase
hex (`traceutil.TraceIDToHexOrEmptyString`, which calls Go's `hex.EncodeToString`), so the key
bytes are the same. The partition isn't, with the exporter's defaults. Its `record_partitioner`
defaults to `sticky_key` with a Sarama-compatible FNV-1a hasher, where `kafka_out` uses the Java
client's murmur2. Against v0.162.0 on Apache Kafka 4.2.0 and Redpanda, 14 of 16 trace ids landed
on a different partition from the exporter's default than from `kafka_out`, and none did with the
exporter's `record_partitioner: {sticky_key: {hasher: murmur2}}`
(`script/kafka-interop`'s `otlp-exporter-parity` leg;
[known gaps](../known-gaps/kafka.md)). As in the exporter's log path, an all-zero trace id counts
as no value.

librdkafka's default partitioner, `consistent_random` (CRC32), doesn't match Java's: it placed 19
of 23 keys on a different partition in the recorded cross-check. Its `murmur2_random` partitioner
matched on all 23, as did the Java console producer. `kafka_out` matches the Java client, so its
keyed records land where a Java producer's records with the same key land.

### 4. Acknowledgment, idempotence, and delivery posture

- **`acks: all` is the default**, as in Java since 3.0.
- **`idempotent: true` is the default and requires `acks: all`.** A graph rule rejects
  `idempotent: true` with any other `acks`. An explicit conflict is a config error, never a silent
  disable: Java throws on the same explicit conflict, and disables idempotence silently only when
  neither setting was given. `kafka_out` has no unset state to disable from.
- **An idempotent producer** gets a producer id (PID) and epoch from `InitProducerId` and numbers
  each partition's batches with a sequence that advances by the record count and wraps from
  `Int32.MAX` to 0.
- **Within one epoch, a retry reuses the PID, the epoch, and each partition batch's base sequence
  verbatim**, as Java's `RecordAccumulator.drain` does (it assigns a sequence only to a batch that
  has none). The broker keeps the last five batches per PID per partition, so it deduplicates an
  `Ambiguous` resend inside that window and answers with the original offsets.
- **A partition batch that settles without being appended bumps the epoch.** The broker accepts a
  partition's next batch only at the sequence after the last one it appended
  (`ProducerAppendInfo.checkSequence`), so records numbered but never appended leave a gap, and
  that partition's next batch would be answered `OUT_OF_ORDER_SEQUENCE_NUMBER`. The gap follows:
  - a `Rejected` partition batch, whatever its code;
  - the named-record drop and resend for `INVALID_TIMESTAMP` and `INVALID_RECORD` (item 5);
  - the `MESSAGE_TOO_LARGE` split (item 7);
  - a batch that `write_loop` drops after an `Ambiguous` attempt under `at_most_once`, which may
    or may not have been appended.

  Before that partition sends again, `kafka_out` bumps the epoch on the client: the same PID with
  epoch + 1, and no request. Every partition's sequence restarts at 0 under the new epoch
  (`ProducerAppendInfo.checkSequence` requires 0 there). `kafka_out` sends `InitProducerId` only
  when the epoch reaches `Short.MAX_VALUE`. For a producer with no transactional id, the broker
  (`TransactionCoordinator.handleInitProducerId`) ignores the PID and epoch in that request and
  returns a fresh PID at epoch 0, so that request mints a new PID. The resend of the rest, the split
  halves, and every partition batch numbered behind the failure take fresh consecutive sequences
  under the new epoch. Java does the same: `TransactionManager.handleFailedBatch` requests an epoch
  bump for the partition, `TransactionManager.bumpIdempotentProducerEpoch` sets the epoch to
  epoch + 1 on the same PID without a request and resets the producer id only at
  `Short.MAX_VALUE`, and the
  `MESSAGE_TOO_LARGE` branch removes the batch from the in-flight set before it splits, so the
  pieces take fresh sequences. With no gap inside an epoch, `OUT_OF_ORDER_SEQUENCE_NUMBER` means
  only a real one: the broker lost its sequence state.
- **Dedup is lost across a new epoch or a new PID.** A process restart mints a new PID.
  `INVALID_PRODUCER_EPOCH` re-initializes the producer with a new PID, and an epoch bump (above, and
  on `OUT_OF_ORDER_SEQUENCE_NUMBER`) moves the same PID to epoch + 1 on the client. Either way the
  sequences start at 0, and the broker can't match a resend against an append from before. A batch whose first
  attempt landed before one of these events and whose retry was sent after it is stored twice. A
  bump renumbers every unsettled partition batch, so it also costs dedup for one still waiting on an
  `Ambiguous` answer, including the same partition's other batch in the same send, if it has one.
- **The posture stays the runtime default, `at_least_once`** ([ADR
  `delivery-semantics`](delivery-semantics.md), item 5). The broker's dedup removes most
  duplicates, but not those above, so `kafka_out` doesn't claim effectively-once delivery.
- **Settled partitions aren't resent.** A produce request carries several partitions, and each
  partition's result settles on its own. `kafka_out` remembers per batch which partitions the
  broker settled, in `BatchAccounting` (`crates/logit-outputs/src/accounting.rs`), as `otlp_out`
  and the Datadog sinks remember settled requests ([ADR
  `sink-fault-classes`](sink-fault-classes.md), "Amendment: the Datadog sinks retry per request
  (2026-10-09)"). A retry resends only the unsettled partitions, with their sequences from the
  current epoch.
- **What settles a partition.** A partition settles, and is never resent for that batch, when:
  - the broker accepted it (`NONE`, or `DUPLICATE_SEQUENCE_NUMBER` from an older broker);
  - its records are `Rejected`, dropped and counted.
- **The attempt's class covers only the unsettled partitions.** When any remain, the send returns
  the strongest of their classes, in the order `Refused`, `Ambiguous`, `Clean`. A partition held as
  `Refused` keeps the queue held, under both postures, and each retry resends the other unsettled
  partitions without resending a settled one. Under `at_most_once`, an `Ambiguous` partition isn't
  settled inside the sink: the send returns `Ambiguous`, and `write_loop` drops the batch and
  counts it, as it does for `datadog_out`.
- **When none remain, the send succeeds only if a partition was accepted**, on this attempt or an
  earlier one (`BatchAccounting::any_accepted`). Otherwise it fails `Rejected` with the first
  rejection's error, so `write_loop` drops and counts the batch as it does any rejected batch, as
  [ADR `delivery-semantics`](delivery-semantics.md)'s "Amendment: per-request verdicts" sets for
  `datadog_out`.

### 5. Fault classes

Each partition's result in a `Produce` response is classified on its own, by error code. The rows
below are the decision. The `kafka_out` module doc becomes the canonical table when W1 and W2
land, one row per response backed by a fake-broker test, per AGENTS.md's response-class-table
invariant; this table isn't kept in step with it after that.

Transport outcomes, before or without a response:

| Outcome | Class | Why |
|---|---|---|
| Connect failure, DNS failure, TLS handshake failure | `Clean` | No request byte left the process. `otlp_out` reads a handshake failure the same way. |
| A timeout or disconnect before the request is written | `Clean` | The broker can't have the request. |
| A timeout or disconnect after any request byte is written, or while waiting for the response | `Ambiguous` | The broker may have appended the batch. |

Broker error codes:

| Error | Code | Java | `kafka_out` | Why |
|---|---|---|---|---|
| `CORRUPT_MESSAGE` | 2 | retried | `Rejected` | Divergence (item 2): a resend carries the same bytes. |
| `UNKNOWN_TOPIC_OR_PARTITION` | 3 | retried after a metadata refresh | `Refused` after one metadata refresh | Nothing was appended. The refresh asks the broker to create the topic (item 9); a topic still missing after it stays missing until an operator creates it, and the queue holds until then. |
| `LEADER_NOT_AVAILABLE` | 5 | retried after a refresh | `Clean`, metadata refreshed | An election is in progress; nothing was appended. |
| `NOT_LEADER_OR_FOLLOWER` | 6 | retried after a refresh (KIP-951's `currentLeader` from v10) | `Clean`, metadata refreshed | The request went to the wrong broker; nothing was appended. |
| `REQUEST_TIMED_OUT` | 7 | retried | `Ambiguous` | The leader may have appended before replication timed out. |
| `MESSAGE_TOO_LARGE` | 10 | split if the batch holds more than one record, else failed | Split once, else `Rejected` | Item 7. |
| `NETWORK_EXCEPTION` | 13 | retried after a refresh | `Ambiguous` | Java also synthesizes it for a disconnect; the batch may have been appended. |
| `RECORD_LIST_TOO_LARGE` | 18 | failed, no split | `Rejected` | The partition batch exceeds the broker's segment size; a resend gets the same answer. |
| `NOT_ENOUGH_REPLICAS` | 19 | retried | `Clean` | The broker rejects before appending. |
| `NOT_ENOUGH_REPLICAS_AFTER_APPEND` | 20 | retried | `Ambiguous` | The leader appended; replication fell short. |
| `INVALID_REQUIRED_ACKS` | 21 | config error | `Refused` | Every request gets the same answer. |
| `TOPIC_AUTHORIZATION_FAILED` | 29 | failed | `Refused` | Credentials or ACLs: the same for every batch. |
| `CLUSTER_AUTHORIZATION_FAILED` | 31 | fatal, the producer is unusable | `Refused` | As 29. |
| `INVALID_TIMESTAMP` | 32 | failed; `record_errors` names records from v8 | Drop the named records, resend the rest once | Item 6. |
| `UNSUPPORTED_VERSION` | 35 | fatal | `Refused` | A protocol mismatch, the same for every batch. |
| `POLICY_VIOLATION` | 44 | failed | `Rejected` | A broker-side policy refused this batch's content. |
| `OUT_OF_ORDER_SEQUENCE_NUMBER` | 45 | retried, with an epoch bump in some cases | `Ambiguous`, epoch bumped | `kafka_out` leaves no gap inside an epoch (item 4), so the broker's sequence state no longer matches; an earlier attempt may have landed, and the new epoch loses dedup. |
| `DUPLICATE_SEQUENCE_NUMBER` | 46 | success | Success | Current brokers answer `NONE` with the original offsets instead. |
| `INVALID_PRODUCER_EPOCH` | 47 | failed | `Clean`, producer re-initialized | The broker refused the stale epoch before appending. |
| `KAFKA_STORAGE_ERROR` | 56 | retried after a refresh | `Clean`, metadata refreshed | The replica's log directory failed; nothing was appended. |
| `UNKNOWN_PRODUCER_ID` | 59 | retried, possibly with an epoch bump | `Clean` | The broker expired the PID's state; current brokers rarely send it. |
| `UNSUPPORTED_COMPRESSION_TYPE` | 76 | failed | `Refused` | The broker or topic refuses the configured codec for every batch. |
| `INVALID_RECORD` | 87 | failed; `record_errors` | Drop the named records, resend the rest once | As 32. |
| `THROTTLING_QUOTA_EXCEEDED` | 89 | retried | `Clean` | Nothing was appended. Produce quotas otherwise slow a client through `throttle_time_ms`. |
| `UNKNOWN_TOPIC_ID` | 100 | retried after a refresh | `Refused` after one metadata refresh | As 3. Produce v13 only, which `kafka_out` doesn't send (item 8). |
| `INCONSISTENT_TOPIC_ID` | 103 | retried after a refresh | `Clean`, metadata refreshed | The topic was recreated under the same name. Produce v13 only, which `kafka_out` doesn't send (item 8). |
| Any other code | — | — | `Rejected` | The runtime's default for an answer a sink can't attribute ([ADR `sink-fault-classes`](sink-fault-classes.md)). The diagnostic names the code. |

Kafka has no `TOPIC_DELETED` error. A topic deleted under a running producer reads as code 3.

**Records a broker names.** `INVALID_TIMESTAMP` and `INVALID_RECORD` carry `record_errors` from
Produce v8 (KIP-467), naming the records at fault by index. `kafka_out` drops the named records,
counted, bumps the epoch, and resends the rest of that partition batch once under fresh sequences
(item 4). This follows `splunk_hec_out`'s code 6,
which drops the one object HEC names and resends the rest once ([ADR
`splunk-hec-relay`](splunk-hec-relay.md), decision 18). A second rejection of the resend, or a
rejection that names no record, makes the partition batch `Rejected`.

### 6. Record timestamps

A record's timestamp is the event's timestamp, sent as `CreateTime`. On a topic set to
`LogAppendTime`, the broker overwrites it with its own clock.

Kafka 4.x sets `log.message.timestamp.after.max.ms` to 3,600,000 (1 hour) by default; 3.6 to 3.9
had no limit. A relayed event stamped more than an hour ahead of the broker's clock is rejected as
`INVALID_TIMESTAMP`, and the broker names it in `record_errors`. `kafka_out` drops that record,
counted, and delivers the rest of its batch (item 5). The past is unbounded by default
(`log.message.timestamp.before.max.ms`).

### 7. Size

- **`max_request_bytes`** (default 1,048,576, Java's `max.request.size`) is checked per record
  against the record's uncompressed size, as Java's `estimateBatchSizeUpperBound` does. A record
  over the cap is dropped at encode time and counted; it never reaches the broker.
- **A send's records are cut into produce requests** that stay under the cap.
- **`MESSAGE_TOO_LARGE`** on a partition batch with more than one record splits it in two and
  resends both halves once (a divergence, item 2), under fresh sequences (item 4). On a single
  record, or on a half, it's `Rejected`.
- **`RECORD_LIST_TOO_LARGE`** is `Rejected` with no split, as in Java.

### 8. Protocol versions

- **The `Produce` floor is v3.** Kafka 4.0 removed v0 to v2 (KIP-896): `ProduceRequest.json`'s
  `validVersions` is `3-12` in 4.0 and `3-13` from 4.1. v3 is also the first version that carries
  record batch v2 (magic 2), the only format `kafka_out` writes. A 4.x broker still advertises
  `Produce` from v0 in its `ApiVersions` answer
  ([api-versions.txt](../../testdata/interop/kafka/api-versions.txt) shows 0 to 13), for
  librdkafka's sake (KAFKA-18659), and rejects a request below v3. `kafka_out` reads the advertised
  minimum as no constraint and negotiates from v3.
- **zstd needs v7.** A broker that offers less than v7 can't take `compression: zstd`, and the
  sink reports that as `Refused`.
- **`record_errors` needs v8.** Below v8, an `INVALID_TIMESTAMP` or `INVALID_RECORD` names no
  record and the partition batch is `Rejected` (item 5).
- **The ceiling is v12**, the highest version that addresses a topic by name. v13 replaces names
  with topic ids, which would add an id lookup and the `UNKNOWN_TOPIC_ID` and
  `INCONSISTENT_TOPIC_ID` paths for no gain. Every Kafka 4.x broker accepts v12, and v10 to v12
  carry KIP-951's leader hints. librdkafka's own maximum is v10.
- **Each connection negotiates** through `ApiVersions`: the highest version both sides support,
  within v3 to v12.

### 9. Metadata

The producer bootstraps from `brokers:`, a list of `host:port`, and then connects to each
partition leader the cluster advertises. It refreshes metadata after the errors item 5 marks
"metadata refreshed", after `UNKNOWN_TOPIC_OR_PARTITION` before it reads it as `Refused`, and
whenever the cached metadata is older than 5 minutes (Java's `metadata.max.age.ms`).

Its `Metadata` requests set `allow_auto_topic_creation`, as the Java producer's do, so the
broker's `auto.create.topics.enable` decides whether a missing topic is created. On a broker that
won't create it, the topic stays missing and the sink reads it as `Refused` (item 5).

### 10. TLS

A `tls:` block (`TlsClientConfig`) turns TLS on, as it does for the other sinks that take a bare
`host:port`. Each connection's SNI and certificate check use the broker's advertised host, not the
bootstrap address. A `tls.server_name` replaces the name every broker's certificate is checked
against, and leaves SNI alone ([ADR `tls-client-server-name`](tls-client-server-name.md)). Certificates reload through `logit_pipeline::tls` like every other TLS
component. mTLS uses the same block's client certificate.

SASL isn't part of this stream (Consequences).

### 11. Formats and record mapping

`format:` picks the payload, and each format has a default `record:` granularity that
`record: event | batch` overrides.

| `format:` | Default `record:` | One record holds |
|---|---|---|
| `json` | `event` | One object in the NDJSON grammar, without the trailing newline. `crates/logit-outputs/src/ndjson.rs`'s module doc is the grammar. |
| `text` | `event` | The log body as UTF-8. A non-log event is counted and skipped. This is the OTel `kafkaexporter`'s `raw` encoding and Fluent Bit's `raw` format. |
| `msgpack` | `event` | The NDJSON grammar's object, key for key, as MessagePack, written with `logit_proto::msgpack::Writer`, so a consumer can switch codecs without remapping fields. |
| `native` | `batch` | One `CODEC_BATCH` frame ([ADR `file-output-native-format`](file-output-native-format.md)). |

With `record: batch`, `json` writes NDJSON and `text` writes the bodies joined by newlines. With
`record: event`, `native` writes a one-event batch per record, which repeats the frame's
dictionary on every record.

The encoder implements [`FramedEncoder`](framed-encoder.md): each message in its `MessageBuf` is
one record's value, and the message's `Meta` carries the record's key and partition hint. A record
the encoder drops is counted in its `Stats`.

`topic:` and `headers:` are static in this stream: one topic per sink, and the same header map on
every record. The `otlp_proto` and `otlp_json` formats write a topic per signal ([ADR
`kafka-otlp-formats`](kafka-otlp-formats.md)). Kafka headers keep their order and, in `kafka-protocol`'s `Record`, can't repeat a
key.

Each format stays decodable, so a future `kafka_in` can round-trip it.

## Alternatives considered

- **librdkafka through `rdkafka`.** Rejected:
  - it needs OpenSSL for TLS, which breaks the rustls-only build;
  - its internal retries hide which attempt failed and how, which blurs the four fault classes a
    sink must report;
  - it runs its own threads beside tokio;
  - its TLS can't reload through `logit_pipeline::tls`.

  Its defaults also differ from Java's on idempotence (off), the size cap (1,000,000), and the
  keyed partitioner (CRC32), so matching Java would mean overriding each.
- **`krafka` or another pure-Rust client.** Rejected: pre-1.0, with retry, idempotence, and error
  semantics nobody here has verified, and the same problem of hiding the attempt from the fault
  classes.
- **Hand-roll the record batch and the request codecs.** Rejected: `kafka-protocol` already
  encodes record batch v2 with CRC-32C and generates every request and response from Kafka's own
  schemas. Its compression hook removes the one reason to bypass it, its C codecs.
- **Java's byte-based sticky switch for unkeyed records.** Rejected as above: `logit`'s batch is
  already the accumulation unit.
- **Xerial-framed snappy, as Java writes it.** Rejected: brokers and consumers read raw snappy,
  `snap` writes it directly, and librdkafka, the other reference client, writes it.
- **The trace id's 16 raw bytes as the key.** Rejected: no reference producer keys a record that
  way, and the console tools can't show it. Lowercase hex matches the OTel `kafkaexporter`.
- **Topic ids (Produce v13).** Rejected: an id lookup and two more error paths for no gain over
  names.

## Consequences

- W1 adds `kafka-protocol`, `crc`, and `crc-catalog` to `Cargo.lock` and `deny.toml`'s review,
  with none of `kafka-protocol`'s codec features, so no C codec enters the tree. It enables
  `lz4_flex`'s frame support. `flate2` is already a direct dependency of `logit-outputs` and `snap`
  a dev-dependency; W1 makes `snap` a normal dependency, adds `ruzstd` and `lz4_flex`, and widens
  the scope of all four beyond the components they serve today, updating the `Cargo.toml` comments
  that scope them.
- The `kafka_out` module doc carries the canonical fault-class table, each row backed by a
  fake-broker test, and this ADR's table stops being updated once it exists.
- Duplicates are possible across a process restart, a producer re-initialization, and an epoch
  bump, which follows every partition batch that settles without being appended (item 4).
  An operator who needs none must deduplicate downstream.
- An event stamped more than an hour in the future doesn't reach a Kafka 4.x topic with the
  default broker config. It's dropped and counted (item 6).
- Unkeyed records cluster per `logit` batch. A topic fed by one small, steady producer fills
  partitions less evenly over a short window than Java's byte-based switch would.
- Follow-ups, each outside this stream:
  - SASL PLAIN and SCRAM, then OAUTHBEARER and AWS MSK IAM, decided in
    [ADR `kafka-sasl`](kafka-sasl.md);
  - `otlp_proto` and `otlp_json` formats, for parity with the OTel `kafkaexporter`, decided in
    [ADR `kafka-otlp-formats`](kafka-otlp-formats.md);
  - pipelined produce through `Output::window`;
  - a topic chosen per event;
  - `kafka_in`, a consumer-group client;
  - Redpanda and WarpStream specifics beyond what W4's interop run checks.

## Amendment: what W1 settled (2026-10-11)

Building the producer core settled six points the decision above leaves open or states
differently. Where this section and an item above disagree, this section holds.

1. **Settled partitions live in the sink's per-batch `Pending`, not in `BatchAccounting`.**
   Item 4 puts them in `BatchAccounting`. The sink already has to keep one `Pending` per `logit`
   batch across attempts, because it holds each unkeyed record's sticky partition and each
   partition batch's sequence; a retry that re-partitioned or renumbered would lose the broker's
   dedup. `Pending` records which partition batches the broker accepted or rejected, and
   `Producer::attempt` resends only the rest. The send's verdict is item 4's rule unchanged.
2. **One produce request in flight per broker, sent in sequence, with brokers in parallel.**
   Item 2's "one produce request per broker per attempt" can't hold beside item 7: a broker takes
   one record batch per partition per request, and a send is cut into requests under
   `max_request_bytes`, so a broker can need several requests in one attempt. Within an attempt,
   once a partition's batch fails, that partition's later batches aren't sent; they stay
   unsettled as `Clean`. Nothing lands behind a gap, so the broker never answers them
   `OUT_OF_ORDER_SEQUENCE_NUMBER`.
3. **Split halves and the named-record remainder go out on the next attempt.** The attempt that
   gets `MESSAGE_TOO_LARGE`, or `INVALID_TIMESTAMP` or `INVALID_RECORD` naming records, returns
   `Clean` for that partition, and `write_loop`'s retry sends the halves or the remainder under
   the bumped epoch. "Once" is enforced per partition batch: a half or a remainder is marked as a
   resend, and a resend answered `MESSAGE_TOO_LARGE` again, or one with records named again, is
   `Rejected` whole, never split or trimmed a second time.
4. **`UNKNOWN_TOPIC_OR_PARTITION` (3) and `UNKNOWN_TOPIC_ID` (100) are `Refused`, with a metadata
   refresh before the next attempt.** That refresh is item 9's request with
   `allow_auto_topic_creation` set, so a broker that creates topics creates it then.

   *Correction (2026-10-11, W2):* point 4 read code 3 and 100 as `Refused` on their first
   answer, which item 5 doesn't say. A Kafka 4.2 broker that auto-creates a topic answers the
   `Metadata` request that created it with code 3, so every new topic failed its first attempt
   `Refused` and logged an error. Item 5's "`Refused` after one metadata refresh" now holds as
   written: the first code 3 or 100, from `Metadata` or `Produce`, is `Clean` and schedules a
   refresh, and only one that persists after a completed refresh is `Refused`, until a partition
   batch is accepted. A first answer from `Metadata` gets its refresh in the same attempt, which
   polls, from 100 ms doubling to 1 s (Java's `retry.backoff.ms` and `retry.backoff.max.ms`),
   while the topic is still missing or leaderless, up to `timeout`, as Java's `waitOnMetadata`
   polls up to `max.block.ms`. One from `Produce` gets it on the next attempt.
5. **The details item 8 and item 5 leave open:**
   - `Metadata` is negotiated within v4 (the first version with `allow_auto_topic_creation`) to
     v12, and `InitProducerId` within v0 to v5.
   - `ApiVersions` is sent at v3 only, so a broker older than Kafka 2.4 is unsupported. Every 3.x
     and 4.x broker answers v3.
   - When `record_errors` names every record of a partition batch, nothing is left to resend, and
     the partition batch is `Rejected`.
   - Before it writes to a pooled connection, the producer polls it once for a close. A
     connection the broker closed (`connections.max.idle.ms`) is redialed and the request written
     on the new one, so the attempt stays `Clean` instead of failing `Ambiguous`.
6. **`deny.toml` needed no change.** `kafka-protocol`, `crc`, and `crc-catalog` are MIT or
   Apache-2.0, which its allowlist already covers, and `script/audit` passes with them.

The `kafka_out` module doc (`crates/logit-outputs/src/kafka/mod.rs`, "Response classes") now
carries the canonical fault-class table, so item 5's table is no longer updated.

## Amendment: what W2 settled (2026-10-11)

Building the sink settled five points the decision above leaves open. Where this section and an
item above disagree, this section holds.

1. **`record:` waits for W3.** (Settled by "Amendment: what W3 settled".) Item 11 gives every format a `record: event | batch` override.
   `kafka_out` ships `format: json` with one record per event and no `record:` field. A batch
   record needs its own timestamp and its own answer to a batch over `max_request_bytes`, which
   W3 decides with the other formats.
2. **A key's text follows `sample`'s canonical bytes** (`logit_core::sampling::hash_value`), which
   item 3's table matches row for row. The one value that table leaves out, a `Timestamp`, is
   its decimal nanoseconds, as `sample` hashes it, so one attribute keys a record and samples
   an event the same way.
3. **The sink lets go of a `Pending` when a send is final, and when `Output::observe_batch`
   starts a batch while one is still held**, calling `Producer::abandon` either way. The second
   case is a batch `write_loop` stopped retrying without a final send, as after a shutdown cut. A
   caller that never calls `observe_batch` gets a fresh `Pending` on every `send`.
4. **An event whose configured key has no value is counted in `logit.output.records.unkeyed`**,
   not in `records.dropped`: the record is still sent.
5. **Under `at_most_once`, an `Ambiguous` partition is dropped when another partition in the same
   attempt is `Refused`.** Item 4's ranking puts `Refused` over `Ambiguous`, and `write_loop`
   retries `Refused` under both postures, so a held `Ambiguous` partition would be resent with
   the `Refused` one. Without idempotence, which `acks: leader` and `acks: none` allow, that
   resend can store its records twice. So before the retry, the sink settles each `Ambiguous`
   partition as dropped, counts its records in
   `logit.output.records.dropped{reason="ambiguous_at_most_once"}`, and marks the sequence gap
   as `Producer::abandon` does. The send still returns `Refused` for the partitions left. This
   corrects item 4's statement that an `Ambiguous` partition under `at_most_once` always makes
   the send return `Ambiguous`.

A Kafka 4.2 broker with `auto.create.topics.enable` answers the first `Metadata` request for a
missing topic with `UNKNOWN_TOPIC_OR_PARTITION` while it creates the topic. The W1 amendment's
point 4 correction covers how the producer reads that answer, so a new topic's first batch goes
out with no failure.

## Amendment: what W3 settled (2026-10-11)

Building the other formats and `record:` settled nine points item 11 and the W2 amendment's point
1 leave open. Where this section and an item above disagree, this section holds. The encoder's
module doc (`crates/logit-outputs/src/kafka/encode.rs`) has the per-format table.

1. **`record:` is optional, and unset means the format's default**: `event` for `json`, `text`,
   and `msgpack`, `batch` for `native`, as item 11's table gives.
2. **A batch record packs events in order until the next would push it over the cap.** The cap is
   `max_request_bytes` less the overhead the producer adds for the record (the batch header, the
   record framing, and the headers), so a value that fits never trips the producer's own
   per-record check. The event that doesn't fit starts the next record. An event whose own value
   is over the cap is dropped and counted as `records.dropped{reason="oversize"}`, as a single
   oversize record is; the rest of the batch still goes.
3. **`native` finds each record's longest run of events that fits by encoding.** A frame shares
   one dictionary across its events, so its size isn't the sum of theirs. The encoder tries the
   whole batch as one frame first, then, for each record, doubles and bisects over the run
   length. A frame only grows as events are added, so this finds the boundaries the in-order rule
   above would, at the cost of extra encodes when a batch doesn't fit one record
   (`docs/known-gaps/kafka.md`), with one exception: a frame can't skip an event, so an event
   over the cap ends the open record. The other formats drop that event and keep filling the
   record, so `native` can emit one more record than they do for the same batch.
4. **A batch record has no key, and its timestamp is its first event's.** A key belongs to one
   event, and a batch record's events can each have a different one. Graph rule 85 rejects any
   `key` but `none` with a batch record, including `native`'s default, and its message tells the
   operator to set `record: event`. `KafkaOutput::new` refuses the same pair.
5. **Batch record bodies, per format:** NDJSON for `json`, each line newline-terminated;
   the bodies joined by `\n` for `text`, with no newline after the last, so a body that holds a
   newline can't be told from two bodies (`docs/known-gaps/kafka.md`); the MessagePack maps back
   to back for `msgpack`, which a MessagePack reader takes as a stream of values; one
   `CODEC_BATCH` frame for `native`.
6. **`text` writes the log body by type.** A `Str` or `Bytes` body goes as its bytes, a `Map` or
   `Array` as `ndjson.rs`'s JSON value, a timestamp as RFC 3339, another scalar as its decimal or
   `true`/`false` text, and `Null` as an empty value. This is the OTel `kafkaexporter`'s `raw`
   log encoding. An event with no log record is skipped and counted as
   `logit.output.events.skipped{reason="no_log"}`, the counter `syslog_out` and `splunk_hec_out`
   use for an event they have no wire form for. A batch whose every event is skipped sends no
   request and succeeds.
7. **`msgpack` is `json`'s object from one walker.** `ndjson.rs` walks an event through a
   `TreeWriter` trait, and the JSON and MessagePack writers implement it, so the two formats
   can't disagree on which keys appear or their order. MessagePack has types JSON lacks, and the
   writer uses them in two places: `Value::Bytes` is a `bin`, not JSON's `b"..."` text, and a
   non-finite float is a float, not JSON's `"NaN"`, `"inf"`, or `"-inf"`. Timestamps, ids, and
   byte fields that are text by convention stay the strings JSON writes, so a consumer reads the
   same values from either format. `logit_proto::msgpack::Writer` gained `begin_map` and
   `end_map`, which patch a map's header once its length is known and still write the smallest
   header.
8. **A native frame is uncompressed.** `compression:` compresses the record batch it travels in,
   and lz4 inside a compressed batch would add work for no gain. Under `record: event`, each
   record is a one-event frame under the batch's resource and scope, through
   `logit_proto::native::encode_events`, which encodes a slice of a batch's events without
   cloning them.
9. **No setting is ignored by a format.** Every field applies to all four formats, so rule 85
   has no "a setting the format ignores" clause. The one combination that leaves a setting
   unused, a key with a batch record, is rejected under point 4.

Checked against `apache/kafka:4.2.0`: each format under each `record:` mode, read back through
kcat and decoded (`serde_json`, `logit_proto::msgpack::Reader`, `logit_proto::native`), carried
every event sent, with each event record's key and each record's timestamp as above. A native
frame decoded in another process lists an event's attributes in that process's interning order,
not the sender's: `AttrMap` order is process-local, so the comparison is field for field.

**Correction to W1: a loading coordinator is polled inside the attempt.** A fresh broker answers
the first `InitProducerId` with `COORDINATOR_LOAD_IN_PROGRESS` (14) while it loads its
transaction state, and W1's producer returned that as a `Clean` failure, so `write_loop` logged
an `ERROR` and retried. The Java producer re-enqueues the request without failing a send. The
producer now polls `InitProducerId` on 14, `COORDINATOR_NOT_AVAILABLE` (15), and
`NOT_COORDINATOR` (16) with the missing-topic poll's backoff, 100 ms doubling to 1 s, and fails
`Clean` only once `timeout` passes. Against a fresh `apache/kafka:4.2.0`, the first batch
logged that `ERROR` before the change and nothing after it.

## Amendment: `format: text` counts what it leaves out (2026-10-13)

Item 6 of "Amendment: what W3 settled" counts an event with no log record. An event that carries
a log and also metrics or a span now counts what its record leaves out: one
`logit.output.metrics.omitted{reason="format"}` per metric record and one
`logit.output.spans.omitted{reason="format"}` per span, encode-side, once per batch. The pieces
leave a record that's otherwise sent, so the name is `omitted`, not `skipped`
([ADR `telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md)'s amendment of the same date).
Being encode-side, the counts stand when the record is lost later: under `record: event`, a
record the producer drops as oversize, or one the broker rejects or names, has still counted its
omitted pieces beside its `records.dropped`.
`record: batch` counts the same, except for an event over the budget alone, which is counted
`records.dropped{reason="oversize"}` and nothing more.
