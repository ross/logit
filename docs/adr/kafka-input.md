---
created: 2026-10-29
updated: 2026-10-29
---

# `kafka_in`: a pure-Rust consumer-group client on `kafka-protocol`, committing an offset once its records are in every downstream inbox

## Status
Accepted

## Context

`kafka_out` produces to Kafka ([ADR `kafka-output`](kafka-output.md), [ADR `kafka-sasl`](kafka-sasl.md),
[ADR `kafka-otlp-formats`](kafka-otlp-formats.md)), and each of its six formats was written to stay
decodable so a consumer could read it back. `logit` has no consumer. The transit spec reads
`kafka_out`'s records through a driver convention instead of a pair's decoder
(`crates/logit-cli/src/transit/wire.rs`'s module doc), and a pipeline that starts at a Kafka
topic, the usual landing zone for Filebeat, Fluent Bit, Vector, and the OpenTelemetry Collector,
needs another agent in front of `logit`.

A consumer decides three things a downstream system depends on: where a new group starts, when an
offset is committed relative to delivery, and what happens to records in flight when a partition
moves to another member. The surveyed consumers answer them differently:

- **The Java consumer 4.x** ([`ConsumerConfig.java` at 4.1.0](https://raw.githubusercontent.com/apache/kafka/4.1.0/clients/src/main/java/org/apache/kafka/clients/consumer/ConsumerConfig.java))
  starts at `latest`, auto-commits every 5 s whatever the application did with the records, and
  revokes a partition through a callback the application must use to commit.
- **Vector's `kafka` source** ([`src/sources/kafka.rs`](https://raw.githubusercontent.com/vectordotdev/vector/master/src/sources/kafka.rs))
  runs librdkafka with `enable.auto.offset.store=false` and stores each record's offset once its
  sinks finalize it, through an ordered finalizer, then lets librdkafka commit every 5 s. Its
  `drain_timeout_ms` (half of `session_timeout_ms`) bounds a revoke. It starts at `largest`.
- **The Collector's `kafkareceiver`** ([README](https://raw.githubusercontent.com/open-telemetry/opentelemetry-collector-contrib/main/receiver/kafkareceiver/README.md))
  runs franz-go with cooperative-sticky rebalancing, auto-commits every second, and marks a
  record before or after its pipeline call depending on `message_marking.after` and
  `on_error`. It reads one OTLP export request per record, the signal taken from the topic.
- **Redpanda Connect's `kafka_franz` input** ([docs](https://docs.redpanda.com/redpanda-connect/components/inputs/kafka_franz/))
  commits an offset only once every record before it was delivered, the highest contiguous
  delivered offset, every 5 s and at shutdown, with up to `checkpoint_limit` (1,024) records in
  flight per partition.
- **Telegraf's `kafka_consumer`** ([README](https://raw.githubusercontent.com/influxdata/telegraf/master/plugins/inputs/kafka_consumer/README.md))
  starts at `oldest` and bounds in-flight records with `max_undelivered_messages` (1,000).
- **Fluent Bit's `in_kafka`** ([docs](https://raw.githubusercontent.com/fluent/fluent-bit-docs/master/pipeline/inputs/kafka.md))
  commits after each processed batch unless `enable_auto_commit` is set.

[ADR `delivery-semantics`](delivery-semantics.md) item 3 already fixes what an input's
acknowledgment means in `logit`: the batch is in every open downstream inbox. A consumer's
acknowledgment is its committed offset.

The repository is rustls-only with no C toolchain, and `kafka_out` already speaks the protocol on
`kafka-protocol` 0.18.0 with its own connection, SASL, and compression code
(`crates/logit-outputs/src/kafka/`). Two dependency facts shape where shared code goes:
`logit-inputs` never depends on `logit-outputs` as a library (its `Cargo.toml` says so), and
neither crate depends on `logit-config`, because `logit-cli` turns config into settings. The fuzz
crate depends on `logit-core` and `logit-proto` only ([ADR `out-of-ci-fuzzing`](out-of-ci-fuzzing.md),
"The `fuzz/` crate"), so a decoder it fuzzes has to live in `logit-proto`.

The broker facts below were checked against Kafka's message schemas at the 2.4.0 and 4.0.0 tags,
against [testdata/interop/kafka/api-versions.txt](../../testdata/interop/kafka/api-versions.txt)
for Apache Kafka 4.2.0, and against `kafka-broker-api-versions.sh` run on
`redpandadata/redpanda:v26.2.3` on 2026-10-29.

## Decision

`kafka_in` is a pure-Rust Kafka consumer-group client that `logit` owns, in
`crates/logit-inputs/src/kafka/`, on the code `kafka_out` already uses, moved into a shared crate.
It joins a group with the classic protocol, commits a partition's offset only once every record
before it is in a batch `Fanout` accepted, and decodes every format `kafka_out` writes.

### 1. Code layout: `logit-kafka`, and the codec in `logit-proto`

- **A new crate, `crates/logit-kafka`**, depending on `logit-core`, `logit-pipeline`, and
  `logit-proto`, holds what both directions use: the broker connection (`conn.rs`), `sasl/`
  except the AWS pieces below, the settings types `logit-cli` builds from config (today's outputs-side `KafkaSasl`
  mirror), the consumer core (item 4 onward), and the fake broker behind a `test-util` cargo
  feature, so both crates' tests drive it. It depends on neither `logit-config` nor
  `logit-outputs`, and both `logit-inputs` and `logit-outputs` depend on it.
- **The AWS pieces live in `crates/logit-aws`.** SigV4 signing (`sasl/sigv4.rs`) and the AWS
  credential chain (`sasl/aws_creds.rs`) serve an S3 sink too, so they move to a small
  `logit-aws` crate (depending on `ring`, `reqwest`, `logit-core`, and `logit-pipeline`'s
  `Fault`), which the `s3` stream extracts for `s3_out`. `logit-kafka` depends on it and keeps the
  MSK-specific part, `msk_iam.rs`: the presigned payload, the partition domain, and the
  `Authenticator` glue.
- **Version negotiation becomes a per-API table.** `conn.rs` hard-codes a range per API it sends
  today (`PRODUCE_VERSIONS`, `METADATA_VERSIONS`, `INIT_PRODUCER_ID_VERSIONS`). It becomes one
  table keyed by API key, each entry a supported range and whether a broker's advertised minimum
  counts, and `Versions` answers the chosen version for any key. `ApiVersions` stays at v3 only.
- **The record-batch codec and compression move to `logit_proto::kafka`**, because they're a
  wire codec, synchronous and I/O-free, and the fuzz crate can reach `logit-proto` and nothing
  above it. `batch.rs`'s decode and `compression.rs`'s decompressors stop being test-only, and
  decompression is bounded as `logit_proto::otlp::grpc::inflate_bounded` and
  `logit_proto::prometheus::compression::decompress_bounded` bound theirs: never more than the
  bound plus one byte is produced, and the extra byte is the error. `kafka-protocol` becomes a
  `logit-proto` dependency.
- **The TLS helpers `conn.rs` uses** (`AsyncStream`, `PendingClose`, `poll_pending_close`) move to
  `logit_pipeline::tls`, beside `build_client_config`, which already lives there.
  `crates/logit-outputs/src/tls.rs` keeps its other helpers and re-exports these, because
  `stream.rs` and `syslog.rs` use them too.
- **`kafka_out` keeps the producer-only code**: `producer.rs`, `multi.rs`, `sink.rs`, `encode.rs`,
  `partitioner.rs`, and `fault.rs`. `conn.rs`'s `RequestError` keeps its `sent` flag, and the
  mapping from it to a sink `Fault` stays in `fault.rs`, because an input has no fault classes.

### 2. Reference semantics: the Java consumer 4.x, classic protocol

| Behavior | Java consumer 4.x | `kafka_in` |
|---|---|---|
| Group protocol | `group.protocol=classic` | Classic only (item 4); KIP-848 is a follow-up |
| Assignors | `partition.assignment.strategy` = `[range, cooperative-sticky]` | `assignment: range \| roundrobin`, default `range`; eager only |
| Start position with no commit | `auto.offset.reset=latest` | `start: latest \| earliest`, default `latest` |
| Out-of-range position | `auto.offset.reset` again | The log start when the position is below it, the log end when above it (divergence below) |
| Isolation | `isolation.level=read_uncommitted` | `isolation: read_uncommitted \| read_committed`, default `read_uncommitted` |
| Commit | `enable.auto.commit=true`, `auto.commit.interval.ms=5000`, regardless of processing | `commit_interval`, default 5 s, of accepted offsets only (item 6) |
| Session | `session.timeout.ms=45000`, `heartbeat.interval.ms=3000` | `session_timeout: 45s`, `heartbeat_interval: 3s` |
| Rebalance timeout | `max.poll.interval.ms=300000`, sent as JoinGroup's `rebalance_timeout_ms` | `rebalance_timeout: 5m`, sent the same way; no poll-interval liveness check (divergence below) |
| Fetch | `fetch.min.bytes=1`, `fetch.max.bytes=52428800`, `max.partition.fetch.bytes=1048576`, `fetch.max.wait.ms=500` | `fetch: {min_bytes: 1, max_bytes: 50MiB, max_partition_bytes: 1MiB, max_wait: 500ms}` |
| Request timeout | `request.timeout.ms=30000` | `timeout: 30s`, as `kafka_out`'s |
| Metadata age | `metadata.max.age.ms=300000` | A refresh every 5 minutes, plus on the errors item 9 names |
| CRC check | `check.crcs=true` | Always checked |
| Client id | `client.id=""` | `client_id: logit` |
| Topic auto-creation | `allow.auto.create.topics=true` | Never requested (divergence below) |
| Fetch sessions | KIP-227 incremental sessions | Full fetches only (item 7) |

The named divergences:

- **No poll-interval liveness.** Java leaves the group when the application doesn't call `poll`
  within `max.poll.interval.ms`, so a stuck consumer gives up its partitions. `kafka_in` has no
  poll: heartbeats run on their own task (item 7), and a member whose pipeline is blocked keeps
  its partitions. A blocked pipeline is almost always a blocked destination that every member of
  the group shares, and leaving would rebalance the group over and over without moving a record.
  `rebalance_timeout` is only JoinGroup's field: how long the coordinator waits for every member
  to rejoin.
- **An out-of-range position doesn't skip retained records.** Java resets an out-of-range position
  with `auto.offset.reset`, so under `latest` a consumer that fell behind retention jumps to the
  log end and skips every record still on the broker. `start:` decides where a partition with no
  commit begins; it isn't permission to skip retained data. Item 9 has the rule.
- **No topic creation by reading.** `kafka_in`'s Metadata requests clear
  `allow_auto_topic_creation`, so a typo in `topics:` doesn't create a topic on a broker with
  `auto.create.topics.enable`. A missing topic is waited for (item 9).
- **No cooperative rebalancing.** Eager rebalancing revokes every partition of every member at
  each rebalance. It's what both shipped assignors need, and the classic protocol's cooperative
  mode is the part of the Java client KIP-848 replaces.

### 3. Protocol versions

The broker floor stays Kafka 2.4, `kafka_out`'s: `ApiVersions` goes at v3 only, which a 2.4 broker
is the first to answer ([ADR `kafka-output`](kafka-output.md), "Amendment: what W1 settled",
point 5). Each range's minimum is the higher of Kafka 4.0's floor (KIP-896 removed the older
versions) and the first version carrying a field `kafka_in` needs; each maximum is the last version
that names a topic by name, within `kafka-protocol` 0.18.0's range. Every row's minimum is served by
Kafka 2.4, Kafka 4.2, and Redpanda 26.2.

| API | Range | 2.4 serves | Why the bounds |
|---|---|---|---|
| `Fetch` | v4 to v12 | v0 to v11 | v4 adds `isolation_level` and is 4.0's floor; v7 adds session fields, sent as a full fetch (item 7); v13 replaces topic names with ids |
| `ListOffsets` | v2 to v10 | v0 to v5 | v2 adds `isolation_level` |
| `Metadata` | v4 to v12 | v0 to v9 | `kafka_out`'s range |
| `OffsetCommit` | v2 to v9 | v0 to v8 | 4.0's floor; v5 drops `retention_time_ms` |
| `OffsetFetch` | v1 to v9 | v0 to v6 | 4.0's floor; v7 adds `require_stable`, set when offered |
| `FindCoordinator` | v0 to v6 | v0 to v3 | v4 batches keys; both shapes are handled |
| `JoinGroup` | v2 to v9 | v0 to v6 | 4.0's floor; from v4 a first join is answered `MEMBER_ID_REQUIRED` (79) with the id to join under |
| `SyncGroup` | v0 to v5 | v0 to v4 | |
| `Heartbeat` | v0 to v4 | v0 to v4 | |
| `LeaveGroup` | v0 to v5 | v0 to v4 | v3 addresses members by list |

Sources: `clients/src/main/resources/common/message/<Api>Request.json` at
[2.4.0](https://github.com/apache/kafka/tree/2.4.0/clients/src/main/resources/common/message) and
[4.0.0](https://github.com/apache/kafka/tree/4.0.0/clients/src/main/resources/common/message).

**KIP-848 isn't available on Redpanda.** Apache Kafka 4.2.0 serves `ConsumerGroupHeartbeat` (68)
v0 to v1; Redpanda v26.2.3 answers it `UNSUPPORTED`, so a KIP-848-only client couldn't consume
from Redpanda at all.

### 4. Group membership: the classic protocol, eager, with Java's embedded format

- **Coordinator.** `FindCoordinator` for the group key, then JoinGroup, SyncGroup, Heartbeat, and
  LeaveGroup to that broker.
- **Join.** JoinGroup carries protocol type `consumer` and one protocol, named `range` or
  `roundrobin` as Java names them, whose metadata is a `ConsumerProtocolSubscription`: a 2-byte
  version, then the subscribed topics. `kafka_in` writes v0 (topics and null user data) and reads
  v0 to v3, the versions Java 4.x writes, through `kafka-protocol`'s generated
  `ConsumerProtocolSubscription`. The join request's own timeout is `rebalance_timeout` plus
  `timeout`, because the coordinator holds the answer until every member rejoins, as Java's does.
- **Assignment.** The member the coordinator names leader runs the assignor over every member's
  subscription and returns each member's `ConsumerProtocolAssignment` v0 through SyncGroup:
  - `range`, per topic: partitions in numeric order, members in member-id order, each member
    `partitions / members` of them, the first `partitions % members` one more
    ([`RangeAssignor`](https://raw.githubusercontent.com/apache/kafka/4.1.0/clients/src/main/java/org/apache/kafka/clients/consumer/RangeAssignor.java)).
  - `roundrobin`: every subscribed topic-partition in topic then partition order, dealt to
    members in member-id order, skipping a member not subscribed to the topic
    ([`RoundRobinAssignor`](https://raw.githubusercontent.com/apache/kafka/4.1.0/clients/src/main/java/org/apache/kafka/clients/consumer/RoundRobinAssignor.java)).
- **Mixed groups.** Because the format and the names are Java's, a Java consumer and `kafka_in` can
  share a group. Java 4.x offers `[range, cooperative-sticky]` by default, so a group of both
  picks `range`, eagerly. A Java member configured with `cooperative-sticky` alone shares no
  protocol with `kafka_in`, and the join fails `INCONSISTENT_GROUP_PROTOCOL` (item 9).
- **Rejoin triggers.** A Heartbeat, OffsetCommit, or SyncGroup answered `REBALANCE_IN_PROGRESS`;
  lost membership (item 9); and a metadata refresh that shows a subscribed topic's partition count
  differing from the one the assignment was built on, or a subscribed topic appearing, as Java's
  `ConsumerCoordinator` rejoins on a changed metadata snapshot.
- **Every rebalance revokes every partition** (item 8), then rejoins.
- **Leave.** At shutdown, after the final commit, LeaveGroup, so the group rebalances at once
  rather than after `session_timeout`.

### 5. The fetch path and backpressure

- **No `receive:` queue.** Kafka holds the backlog, so a full pipeline stops fetching, as TCP flow
  control stops a stream listener. Graph rule 17 treats `kafka_in` as it treats a stream or tail
  listener: `receive:` takes `batch_max_events`, `batch_max_bytes`, and `shutdown_grace`, and
  rejects the queue fields by name. `batch_flush_interval` is rejected too, because a fetch's
  records are delivered when they arrive, with nothing to flush.
- **A fetch loop per leader broker.** Each sends `Fetch` for the assigned partitions it leads whose
  buffer is empty, with the `fetch:` bounds. A partition that still holds undelivered records is
  left out of the next request, as Java pauses a partition with buffered records.
- **One delivery loop.** It turns each partition's buffered records into `EventBatch`es, in offset
  order, cut at `batch_max_events` and `batch_max_bytes`, and sends them through `Fanout`. A batch
  covers a contiguous offset range of one partition, so each partition's batches are accepted in
  offset order.
- **Memory bound.** A partition buffers at most one fetch's data for it: `max_partition_bytes`
  compressed, or one record batch larger than that, which a broker returns whole so a consumer
  can't stall (KIP-74). Batches decompress one at a time, each bounded by `max_batch_bytes`
  (default 64 MiB, item 10). One fetch request is bounded by `max_bytes`.
- **Every send goes through `Fanout::send_with_deadline`**, which reserves a slot on every consumer
  before it sends, so a send that times out has sent nothing and none is dropped part way
  ([`fanout.rs`](../../crates/logit-pipeline/src/fanout.rs)'s module doc). While no revoke is
  pending, a timeout retries the same batch; once one is pending, the deadline is the drain
  deadline (item 8).
- **A closed consumer** ends the input as it ends `tail_in`: the batch no consumer took is counted
  `logit.input.batches.dropped{reason="closed_consumer"}` and not committed, the commit before it
  is made, and the member leaves the group so another member reads on.

### 6. Offset commit is the input's acknowledgment

[ADR `delivery-semantics`](delivery-semantics.md) item 3 applies as written: `kafka_in` commits a
partition's offset only once every record before it is in a batch `Fanout` accepted into every open
downstream inbox, and at least one. It never waits for a sink, and never commits before
acceptance.

- **The committed offset is the highest contiguous accepted offset**, plus one, per partition,
  Redpanda Connect's rule. A record that needs no delivery counts as done once it's read: a record
  skipped or dropped by item 10, a control record, and an aborted transaction's record under
  `read_committed`. A batch not accepted never lets the position pass it.
- **When commits go out:** on `commit_interval` (5 s) for each partition whose position moved, on
  every revoke before the rejoin (item 8), and at shutdown before LeaveGroup. A commit carries the
  generation and member id, so a member that lost its membership can't commit over the next
  owner.
- **Starting position.** OffsetFetch for each assigned partition; a partition with no commit
  starts at `start:`, through ListOffsets (`-1` latest, `-2` earliest), counted
  `logit.input.offsets.reset{reason="no_commit"}`.
- **At-least-once.** A crash, a lost membership, or a revoke whose drain misses its deadline
  replays every record after the last commit; the next owner reads them again. These duplicates
  are accepted under [ADR `delivery-semantics`](delivery-semantics.md) item 2 and absorbed or not
  as item 5 sets out per destination. `kafka_in` can't recognize a replay, so item 11's replay
  count doesn't apply to it.
- **Against the surveyed consumers.** Java's auto-commit can commit records the application never
  finished, losing them on a crash. Vector stores each finalized offset, and an offset stored
  after a gap can carry the committed position past a record whose delivery failed. The
  Collector's default marks a record before its pipeline call. The contiguous rule is the only one
  of these under which a committed offset never covers an undelivered record.

### 7. Heartbeats, fetch sessions, and timeouts

- **Heartbeats run on their own task**, every `heartbeat_interval`, independent of fetch and
  delivery, so a slow pipeline never expires the session.
- **No fetch sessions.** Every Fetch from v7 sends session id 0 and epoch -1, a full request that
  opens no session (KIP-227). An incremental session saves request bytes when a member owns many
  partitions; it's a follow-up once a measurement shows that cost.
- **`timeout`** bounds every connect, write, and response wait, as `kafka_out`'s does, except a
  JoinGroup (item 4) and a Fetch, whose wait is `max_wait` plus `timeout`.

### 8. Rebalance and shutdown

A revoke, at every rebalance and at shutdown, runs in this order:

1. Every fetch loop stops, and buffered records not yet in a batch are discarded; the next owner
   reads them.
2. The batch in flight gets the drain deadline, `drain_timeout` (default 5 s, kept below
   `rebalance_timeout` by graph rule 101; at shutdown also bounded by `shutdown_grace`). A batch
   accepted by then advances the position. One that isn't is not sent, not committed, and counted
   `logit.input.batches.deferred`, with the batches decoded behind it.
3. A synchronous commit of every revoked partition's position.
4. JoinGroup, or at shutdown LeaveGroup.

A missed drain deadline isn't loss: the records stay in Kafka, uncommitted, and the next owner
reads them, so `logit.input.batches.deferred` keeps its meaning of data the source still holds
and offers again. The record [ADR `telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md) keeps
of that word gains `kafka_in`'s use in the stream that builds it.

A lost membership (item 9) skips step 3: the coordinator refuses the commit, and the partitions
already belong to the next generation.

### 9. Consumer outcomes

An input has no batch to hold, so `write_loop`'s four fault classes don't apply. Every outcome is
one of five actions: **rejoin**, **refresh** metadata and retry, **rediscover** the coordinator and
retry, **back off** and retry with no end, or **skip** and count. A backoff starts at 100 ms and
doubles to 10 s, reset by a success. No broker outcome ends the input or the process; only the
generic rules do, such as a panicking task. The `kafka_in` module doc becomes the canonical table
when the stream builds it, one row per outcome backed by a fake-broker test, and this table isn't
kept in step after that.

Every outcome below counts `logit.input.broker.errors{api, error}`, `error` the lowercase Kafka
error name or `transport`, `tls`, or `sasl`, besides the counter in its row.

| Outcome | Code | Action | Also counted |
|---|---|---|---|
| DNS, connect, or TLS handshake failure; a timeout or disconnect | — | Back off; a coordinator connection rediscovers, a leader connection refreshes | `logit.input.reconnects` on the redial |
| SASL refused (`UNSUPPORTED_SASL_MECHANISM`, `ILLEGAL_SASL_STATE`, `SASL_AUTHENTICATION_FAILED`), or a token endpoint's `4xx` | 33, 34, 58 | Back off, no fallback to another mechanism or to no authentication ([ADR `kafka-sasl`](kafka-sasl.md)) | Diagnostic `auth_failed` |
| `OFFSET_OUT_OF_RANGE` | 1 | ListOffsets earliest and latest; a position below the log start moves to the log start, one above the log end moves to the log end | `logit.input.offsets.reset{reason="out_of_range"}`; below the start, `logit.input.records.dropped{reason="retention"}` for the offsets between |
| `CORRUPT_MESSAGE`, or a batch whose CRC-32C doesn't match | 2 | Refetch the partition once from the same offset; a second mismatch skips the batch | `logit.input.records.dropped{reason="corrupt"}`, the batch's record count |
| `UNKNOWN_TOPIC_OR_PARTITION` on Metadata | 3 | The topic isn't subscribed until it exists; refresh every 5 minutes, and rejoin when it appears | Diagnostic `topic_missing` |
| `UNKNOWN_TOPIC_OR_PARTITION` on Fetch, `LEADER_NOT_AVAILABLE`, `NOT_LEADER_OR_FOLLOWER`, `REPLICA_NOT_AVAILABLE`, `KAFKA_STORAGE_ERROR`, `FENCED_LEADER_EPOCH`, `UNKNOWN_LEADER_EPOCH`, `OFFSET_NOT_AVAILABLE` | 3, 5, 6, 9, 56, 74, 75, 78 | Refresh, then fetch the partition from its leader after a backoff | — |
| `COORDINATOR_LOAD_IN_PROGRESS` | 14 | Back off, same coordinator | — |
| `COORDINATOR_NOT_AVAILABLE`, `NOT_COORDINATOR` | 15, 16 | Rediscover | — |
| `REBALANCE_IN_PROGRESS` | 27 | Revoke (item 8), commit, rejoin | `logit.input.rebalances{reason="rebalance"}` |
| `ILLEGAL_GENERATION` | 22 | Revoke without the commit, rejoin under the same member id | `logit.input.rebalances{reason="membership_lost"}` |
| `UNKNOWN_MEMBER_ID`, a session that expired while the coordinator was unreachable | 25 | Revoke without the commit, rejoin with an empty member id | `logit.input.rebalances{reason="membership_lost"}` |
| `FENCED_INSTANCE_ID` | 82 | As 25. Only static membership, which `kafka_in` doesn't send, earns it | As 25 |
| `MEMBER_ID_REQUIRED` | 79 | JoinGroup again with the id the answer carries: the ordinary first join from v4 | Not counted as an error |
| `INCONSISTENT_GROUP_PROTOCOL`, `INVALID_SESSION_TIMEOUT`, `GROUP_MAX_SIZE_REACHED` | 23, 26, 81 | Back off and rejoin; another member's assignor, the broker's `group.min.session.timeout.ms` or `group.max.session.timeout.ms`, or the group's size limit | Diagnostic `group_refused`, naming the code |
| `TOPIC_AUTHORIZATION_FAILED` | 29 | Back off; the partitions stay assigned and unread | Diagnostic `not_authorized` |
| `GROUP_AUTHORIZATION_FAILED` | 30 | Back off on whichever group request got it | Diagnostic `not_authorized` |
| `UNSUPPORTED_VERSION`, or no version in item 3's range on a broker | 35 | Back off; a rolling broker upgrade can end it | Diagnostic `unsupported_version` |
| `UNSTABLE_OFFSET_COMMIT` on OffsetFetch | 88 | Back off; a transaction holding the offset is pending | — |
| An OffsetCommit that fails with a transport error or a retriable code | — | The positions stay pending and go with the next commit | `logit.input.commits{class="error"}` |
| An OffsetCommit refused for generation or membership (22, 25, 82) | — | As the row for the code; the positions are dropped from the commit | `logit.input.commits{class="fenced"}` |
| A `throttle_time_ms` above 0 in any response | — | No request to that broker before it passes, as Java mutes the connection | — |
| A record batch whose decompressed size passes `max_batch_bytes` | — | Skip the batch | `logit.input.records.dropped{reason="oversize"}`, its record count |
| A batch that doesn't decompress, or whose record framing doesn't parse | — | Skip the batch | `logit.input.records.dropped{reason="corrupt"}` |
| A message set of magic 0 or 1 | — | Skip it; Kafka 4.0 removed those formats (KIP-724), and only a 2.x or 3.x broker's old segment holds one | `logit.input.records.dropped{reason="legacy_format"}` |
| A record whose value doesn't decode in `format:` | — | Skip the record and commit past it (item 10) | `logit.input.records.dropped{reason="undecodable"}` |

`OFFSET_METADATA_TOO_LARGE` (12) can't occur: `kafka_in` commits no metadata string. Any other code
backs off with a diagnostic naming it, and counts under its own name.

### 10. A record that won't decode is skipped and committed past

A record, or a batch, that fails a bound or doesn't decode fails the same way on every read, so
retrying it would wedge the partition forever. It's skipped, counted
`logit.input.records.dropped{reason}` with a throttled diagnostic naming the topic, partition, and
offset, and the position moves past it, as Vector commits past a decompression failure and the
Collector's `message_marking.on_permanent_error` does. `max_batch_bytes` bounds one batch's
decompressed size; a native frame is also bounded by `NativeDecoder`'s per-frame
`DecodeBudget`.

A record with a null value, a compaction tombstone, has nothing to carry and is counted
`logit.input.records.skipped{reason="tombstone"}`. A record whose value decodes to no event, an
empty NDJSON value say, is `logit.input.records.skipped{reason="empty"}`. Under `read_committed`, a
record from an aborted transaction, found through each partition's `aborted_transactions` list and
the abort markers, is `logit.input.records.skipped{reason="aborted"}`: the config asked not to see
it. Control records aren't counted at all.

### 11. Formats

`format:` is required, with no default: a topic's producer decides its payload, and no guess is
safe. Each format reads back what `kafka_out` writes under both `record: event` and `record:
batch`.

| `format:` | One record decodes as |
|---|---|
| `json` | `logit`'s event JSON, through `logit_proto::event_json::EventJsonDecoder::decode_batches` on the value: one object, or NDJSON under a batch record |
| `msgpack` | The same objects as MessagePack, one map or several back to back, through a new reader that mirrors `event_json.rs` onto `logit_proto::msgpack::Reader`, with `crates/logit-outputs/src/msgpack.rs`'s value forms (a `bin` is `Bytes`, a non-finite float is a float) |
| `native` | One `CODEC_BATCH` frame through `NativeDecoder` |
| `text` | One log per body: the value as its message, `Str` when it's UTF-8 and `Bytes` otherwise, no severity, the `raw` body format, the record timestamp, and an empty resource. Under `record: batch`, one log per line (below) |
| `otlp_proto`, `otlp_json` | One OTLP export request through `OtlpDecoder::decode_signal` or `decode_signal_json`, its signal taken from the topic |

- **`record:` exists only under `format: text`**, default `event`, because only `text` can't tell a
  batch record from an event record by its bytes. The other formats read both forms.
- **The `text` batch form changes in `kafka_out`.** Today it joins bodies with `\n` unescaped, so a
  body that holds a newline reads as two ([ADR `kafka-output`](kafka-output.md), "Amendment: what
  W3 settled", point 5). `logit` is pre-release, so the encoder changes rather than the pair
  listing a normalization: under `record: batch`, each body escapes `\` as `\\` and LF as `\n`,
  and the bodies are joined by LF with none after the last. `kafka_in` splits on LF and unescapes.
  The escape works on bytes, so a body's UTF-8 validity and its `Str` or `Bytes` type survive. An
  escaped form stays one line per body for a console consumer; a length prefix wouldn't. The
  stream records it as a `kafka-output` amendment. `record: event` text is unchanged: the OTel
  `kafkaexporter`'s `raw` encoding.
- **`text` reads a third-party producer's lines.** Filebeat, Fluent Bit, and Vector write
  application JSON. That arrives as `format: text` plus a `json` transform, the stance of
  [ADR `plain-lines-listener`](plain-lines-listener.md): a listener parses nothing, so one parser
  serves every input. This departs from Vector's `kafka` source, which decodes in the source.
  `format: json` is `logit`'s own event JSON, never an arbitrary object.
- **The OTLP formats take `topics:` as a map**, `{logs: <topic>, metrics: <topic>, traces:
  <topic>}`, the keys `kafka_out`'s `topics:` uses. A record's signal is its topic's key. The
  Collector's exporter writes no `otelcol.signal` header by default and `kafka_out` none, so the
  topic is the only signal there is. Unlike `kafka_out`'s, the map has no defaults: a key left out
  isn't subscribed, because `kafka_in` can't tell which signals a pipeline wants, and subscribing
  a missing `otlp_metrics` would hold a missing-topic diagnostic forever. Two signals on one topic
  are rejected (graph rule 102), since a protobuf request doesn't name its signal. The other
  formats take `topics:` as a list of names.
- **Timestamps.** Under `text`, the event's timestamp is the record's, at millisecond precision;
  a record with no timestamp (`-1`) takes the read time. Under every other format, the payload's
  timestamps stand, and the record's rides in a carrier (item 12).

### 12. Record metadata rides as private carriers

Every event `kafka_in` emits carries its record's metadata as private attributes, always, with no
opt-in and no public copy ([ADR `private-attribute-namespace`](private-attribute-namespace.md)):

| Attribute | Value |
|---|---|
| `_logit.kafka.topic` | `Str` |
| `_logit.kafka.partition` | `I64` |
| `_logit.kafka.offset` | `I64` |
| `_logit.kafka.key` | `Str` when UTF-8, else `Bytes`; absent for a null key, `Str("")` for an empty one |
| `_logit.kafka.headers` | `Map` of header name to value: `Str` when UTF-8, else `Bytes`, `Null` for a null value; a name that repeats holds an `Array` of its values in wire order |
| `_logit.kafka.timestamp` | `Timestamp`, the record's milliseconds; absent for `-1` |
| `_logit.kafka.timestamp_type` | `Str`, `create_time` or `log_append_time` |

- **Repeated header names.** Kafka allows them, and `kafka-protocol`'s `Record` keeps headers in an
  `IndexMap<StrBytes, Option<Bytes>>`, which collapses a repeat and requires a UTF-8 name. So
  `logit_proto::kafka` decodes records itself, magic 2 only, and the crate's record types stay on
  the encode side. A header whose name isn't UTF-8 is left out, counted
  `logit.input.headers.omitted{reason="invalid_name"}`.
- **Every event of a record shares its carriers**, so the events of a batch record or an OTLP
  request carry one offset.
- **The kafka pair is a lossless family** under
  [ADR `protocol-knowledge-stays-in-family`](protocol-knowledge-stays-in-family.md), so
  `kafka_out` may key behavior on `_logit.kafka.*`. Two uses follow:
  - `kafka_out` leaves `_logit.kafka.*` out of every payload it writes, `json`, `msgpack`, and
    `native` included, which otherwise carry every private key. The carriers describe the record
    an event arrived in, not the event, and a relay that wrote them into its own payload would
    nest one hop's envelope inside the next. The `stdio_out` and `file_out` renders keep them.
  - Restoring a relayed record's key and headers from the carriers is a follow-up; today `key:`
    and `headers:` are config.

### 13. `kafka_out -> kafka_in` is the eleventh lossless pair

Under [ADR `lossless-transit`](lossless-transit.md), `kafka_in -> kafka_out` relays losslessly in
`json`, `msgpack`, `native`, and the two OTLP formats, modulo these permitted normalizations:

- Record boundaries: a relay re-packs events into its own records, by its own `record:` and
  `max_request_bytes`, and an OTLP request by its own split.
- Partition and offset: a relayed record lands where the relay's partitioner puts it.
- The carriers of item 12, added on the way in and never written into a payload.
- The record timestamp: a relay stamps a record with its event's timestamp, or its first event's,
  as `CreateTime`, so a source record's `LogAppendTime`, or a `CreateTime` that differed from its
  payload's timestamp, isn't kept.
- Compression: a relay writes its own `compression:`, and a xerial-framed snappy batch leaves as
  raw snappy.
- A native frame's attribute order, which is process-local interning order
  ([ADR `kafka-output`](kafka-output.md), "Amendment: what W3 settled").
- An aborted transaction's records under `read_committed`, which the config asked not to read.
- The OTLP formats' own normalizations, as `otlp_in -> otlp_out` lists them.

`text` carries only a log body, so the pair is lossless under `text` for a log whose body is
`Str` or `Bytes` and that carries nothing else. A record key and headers are data on Kafka's wire
that the relay doesn't write until `kafka_out` restores them from the carriers; until then they're
residual debt in `docs/known-gaps/kafka.md`, not a normalization.

The transit spec gains `kafka_in` as a source, and its `kafka_out` read-back convention becomes the
pair's decoder.

### 14. Config

`ComponentKind::KafkaIn`, `deny_unknown_fields`:

```yaml
kafka:
  type: kafka_in
  brokers: ["kafka-1:9092", "kafka-2:9092"]
  topics: [logs]                   # under otlp_proto | otlp_json: {logs: .., metrics: .., traces: ..}
  group: logit-logs                # required
  format: json                     # required: json | text | msgpack | native | otlp_proto | otlp_json
  # record: event                  # format: text only: event (the default) | batch
  # client_id: logit
  # start: latest                  # latest | earliest
  # isolation: read_uncommitted    # read_uncommitted | read_committed
  # assignment: range              # range | roundrobin
  # session_timeout: 45s
  # heartbeat_interval: 3s
  # rebalance_timeout: 5m
  # drain_timeout: 5s
  # commit_interval: 5s
  # fetch: {min_bytes: 1, max_bytes: 50MiB, max_partition_bytes: 1MiB, max_wait: 500ms}
  # max_batch_bytes: 64MiB
  # timeout: 30s
  # tls: {...}                     # TlsClientConfig, as kafka_out's
  # sasl: {...}                    # KafkaSasl, as kafka_out's: every mechanism
```

Graph rules, numbered from the next free one:

- **Rule 100**: a `kafka_in` with an empty `brokers` or an entry that isn't `host:port`, a bad
  `tls` or `sasl` (rule 85's helpers, shared), an empty `group` or `client_id`, an empty `topics`,
  a topic that isn't a legal Kafka name or that repeats, a `fetch` bound of 0, a `min_bytes` over
  `max_bytes`, a `max_wait` not below `timeout`, or a `max_batch_bytes`, `commit_interval`,
  `drain_timeout`, or `timeout` of 0.
- **Rule 101**: a `kafka_in` whose `heartbeat_interval` isn't below `session_timeout`, whose
  `session_timeout` is over `rebalance_timeout`, or whose `drain_timeout` isn't below
  `rebalance_timeout`.
- **Rule 102**: a `kafka_in` whose `topics:` shape doesn't match its `format:` (a map under the OTLP
  formats, a list under the others), an OTLP map with a key other than `logs`, `metrics`, or
  `traces`, no key, or one topic under two keys, or a `record:` under any format but `text`.
- **Rule 17** gains `kafka_in` beside the stream and tail listeners (item 5).

No error, log, or diagnostic prints the `sasl:` or `tls:` block (AGENTS.md's config-printing rule).

### 15. Telemetry

Names follow [internal-telemetry.md](../design/internal-telemetry.md)'s "Naming": component detail
under `logit.input.*`, at the protocol's own unit, with no protocol segment, as `prometheus_in`'s
`logit.input.scrapes` and `logit_in`'s `logit.input.acks` have none. A Kafka record is the unit.

| Name | Kind | Meaning |
|---|---|---|
| `logit.input.records` / `.record.bytes` | count / sum | data records decoded, and their value bytes; a dropped or skipped one included, a control record and a buffered record a revoke discards before decoding not |
| `logit.input.records.dropped{reason}` | count | `undecodable`, `corrupt`, `oversize`, `legacy_format`, or `retention` (items 9 and 10) |
| `logit.input.records.skipped{reason}` | count | `tombstone`, `empty`, or `aborted` (item 10) |
| `logit.input.headers.omitted{reason="invalid_name"}` | count | item 12 |
| `logit.input.batches.dropped{reason="closed_consumer"}` | count | item 5 |
| `logit.input.batches.deferred` | count | a batch a revoke or shutdown left unsent and uncommitted (item 8) |
| `logit.input.partitions` | gauge | partitions assigned, published on every assignment |
| `logit.input.lag{topic, partition}` | gauge | the high watermark, or under `read_committed` the last stable offset, less the position, from each Fetch answer |
| `logit.input.rebalances{reason}` | count | each completed join: `start`, `rebalance`, `membership_lost`, or `metadata` |
| `logit.input.commits{class}` | count | each OffsetCommit: `ok`, `error`, or `fenced` |
| `logit.input.offsets.reset{reason}` | count | `no_commit` or `out_of_range` |
| `logit.input.broker.errors{api, error}` | count | item 9 |
| `logit.input.reconnects` | count | a redial of a connection slot dialed before, as `kafka_out` counts `logit.output.reconnects` |

With these, `logit.input.records` equals the records in batches `Fanout` accepted, plus every
`records.dropped` and `records.skipped` reason, plus the records of the batches counted
`batches.deferred` or `batches.dropped{reason="closed_consumer"}`, which the next owner reads and
counts again.

## Alternatives considered

- **librdkafka through `rdkafka`.** Rejected for the reasons [ADR `kafka-output`](kafka-output.md)
  gives: OpenSSL, its own threads, and a TLS stack `logit_pipeline::tls` can't reload. Its consumer
  also commits from its own timer, so the commit rule of item 6 would sit on top of
  `enable.auto.offset.store=false` and an offset store `logit` doesn't own.
- **KIP-848 first.** The server-side assignor removes the client's rebalance state machine, but
  Redpanda 26.2 doesn't serve `ConsumerGroupHeartbeat`, and the protocol needs Kafka 4.0, far above
  the 2.4 floor. It's an opt-in follow-up.
- **Cooperative-sticky rebalancing.** It keeps unaffected partitions flowing through a rebalance,
  at the cost of the two-phase revoke protocol and owned-partition bookkeeping, which KIP-848
  replaces. Eager is enough while a revoke drains in `drain_timeout`.
- **Commit, then forward.** Loses every record between the commit and a crash, which no `logit`
  input accepts.
- **Commit each accepted offset as Vector stores it.** An accepted offset after a refused one would
  commit past the refused batch.
- **A `receive:` queue, as the UDP listeners have.** It decouples a socket from a stalled pipeline,
  which a pull-based consumer doesn't need: Kafka holds the backlog, and a queue would only add
  records read but uncommitted.
- **Leave the group when the pipeline is blocked, as Java's poll interval does.** Rejected under
  item 2's divergence: the next owner meets the same blocked destination, and the group rebalances
  for nothing.
- **A `format: json` that parses an arbitrary object.** Duplicates the `json` transform, as
  [ADR `plain-lines-listener`](plain-lines-listener.md) rejected for `lines_in`.
- **Public record metadata (`kafka.topic`, `messaging.kafka.*`).** A partition and an offset as
  public attributes become tags at every metric sink and a series per offset. A pipeline that wants
  one public copies it with `lua` or `set`.
- **A length prefix for `text` batch records.** Unambiguous, but unreadable in a console consumer,
  and a reader splitting on LF would see one garbled body.
- **A header marking a `text` batch record.** `kafka_in` would trust a
  producer's header over its own config, and a third-party producer writes none.

## Consequences

- W1 moves code across five crates (`logit-kafka`, `logit-aws`, `logit-proto`, `logit-pipeline`,
  `logit-outputs`) and changes no behavior; `kafka_out`'s tests pass unchanged.
- `logit-proto` gains `kafka-protocol`, and the fuzz crate gains a record-batch target and a
  MessagePack event target.
- Duplicates follow a crash, a lost membership, and a missed drain deadline, each bounded by what was read since the last commit: about `commit_interval` of
  traffic per partition, plus one fetch. An operator who needs none deduplicates downstream.
- Every rebalance stops every member until the group rejoins, for up to `drain_timeout` plus the
  join. A rolling restart of `n` members rebalances `n` times, twice each without static
  membership.
- A blocked pipeline holds its partitions, so lag grows on them and no other member takes over.
- An event from `kafka_in` carries seven private attributes, and an event that also carries its
  own attributes spills `AttrMap`'s inline capacity sooner than it would from another input.
- `kafka_out`'s `text` batch records change form. A consumer that split them on LF reads the same
  bodies, now escaped.
- Follow-ups, each outside this stream:
  - KIP-848, opt-in by `group_protocol: consumer`, for Kafka 4.0 and later;
  - fetch sessions (KIP-227), if a measurement shows full fetches cost enough to matter;
  - static membership (`group.instance.id`), so a restart doesn't rebalance;
  - rack-aware fetch from followers (KIP-392);
  - regular-expression topic subscriptions;
  - `kafka_out` restoring a relayed record's key and headers from `_logit.kafka.key` and
    `_logit.kafka.headers`;
  - Azure Event Hubs' Kafka endpoint as an interop target: SASL PLAIN with the username
    `$ConnectionString` and the namespace's connection string as the password, on port 9093
    ([Microsoft's overview](https://learn.microsoft.com/en-us/azure/event-hubs/azure-event-hubs-apache-kafka-overview));
  - consumer lag beyond the per-partition gauge, such as a group-wide total or lag in time;
  - a topic chosen per event on `kafka_out`, already listed in
    [ADR `kafka-output`](kafka-output.md)'s Consequences.
