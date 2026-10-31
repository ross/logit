---
created: 2026-10-29
updated: 2026-10-29
---

# Enabling plan: `kafka_in` — a consumer-group client and the eleventh lossless pair

## Context

`logit` produces to Kafka and can't consume from it. [ADR `kafka-input`](../adr/kafka-input.md)
decides the consumer: a pure-Rust classic-protocol group member on the code `kafka_out` already
uses, committing an offset only once its records are in every downstream inbox, decoding all six
of `kafka_out`'s formats, and carrying record metadata as `_logit.kafka.*` carriers. That ADR is
the decision record; this plan cuts it into workstreams.

Stream key: `kin`.

Goals:

- `kafka_in` consumes a topic in a group alongside other `kafka_in` and Java members, survives
  broker restarts and rebalances, and never commits past a record no consumer took.
- `kafka_in -> kafka_out` relays losslessly in every format but `text`, modulo the ADR's item 13,
  and the transit spec reads `kafka_out` through the pair's decoder.
- Every claim about a broker is checked against Apache Kafka 4.2.0 and Redpanda v26.2.3 by
  `script/kafka-interop`, and the commit rule under faults by a soak scenario.

Non-goals:

- KIP-848, fetch sessions, static membership, rack-aware fetch, and regular-expression topics
  (the ADR's follow-ups).
- `kafka_out` restoring a relayed record's key and headers from the carriers.
- Parsing third-party payloads inside `kafka_in`; that's `format: text` plus a transform.
- Azure Event Hubs, MSK, or any hosted broker in the interop runs.

## Design

### W1: the `logit-kafka` crate, with no behavior change

W1 cuts after the `s3` stream's `logit-aws` extraction,
[#926](https://github.com/ross/logit/pull/926), merges. If it hasn't, W1 does that extraction
itself in the same shape: `crates/logit-aws` holding `sasl/sigv4.rs` and `sasl/aws_creds.rs`
(SigV4 signing, the AWS credential chain, and the partition-domain lookup it calls), depending
on `ring`, `reqwest`, `logit-core`, and `logit-pipeline`'s `Fault`, with the AWS endpoints' HTTP
stub as `logit_aws::test_stub` behind a `test-util` feature.

- **`crates/logit-kafka`**, a workspace member depending on `logit-core`, `logit-pipeline`,
  `logit-proto`, `logit-aws`, `kafka-protocol`, and the TLS crates. It receives from
  `crates/logit-outputs/src/kafka/`:
  - `conn.rs`, with version negotiation generalized from the three range constants to a table
    keyed by API key (a range and whether the broker's advertised minimum counts), and `Versions`
    answering any key. `PRODUCE_VERSIONS`, `METADATA_VERSIONS`, and `INIT_PRODUCER_ID_VERSIONS`
    become rows. `RequestError::fault()` and the `Fault` contexts on connect, TLS, and
    ApiVersions move with the file, as in the ADR's item 1.
  - `sasl/` except `sigv4.rs` and `aws_creds.rs`, which `logit-aws` holds; `msk_iam.rs` (the
    presigned payload and the `Authenticator` glue) stays here.
  - `logit_kafka::error`, holding `fault.rs`'s `code` table, `name`, and `classify_sasl`.
    `kafka_out`'s `fault.rs` keeps the per-code produce classes and imports the table from here;
    its tests' `fault::code` paths change to match.
  - The HTTP helpers `sasl/oauth.rs` uses, as a TLS-less copy in `logit-kafka`'s own `http.rs`
    (the ADR's item 1 says why neither `logit-outputs::http`, whose `build_client` takes the
    sinks' `rustls::ClientConfig`, nor `logit-aws`'s private copy is its home). `logit-outputs::http`
    keeps its TLS-taking `build_client` and `client_builder` and loses `try_read_body_bytes`;
    `logit-aws` is unchanged by this bullet.
  - `FakeStream`, `ReadStep`, and `WriteStep` from `crates/logit-outputs/src/test_support.rs`,
    behind `test-util`; `test_support` re-exports them so `stream.rs`' and the other sinks' tests
    resolve unchanged.
  - The outputs-side `KafkaSasl` and `OAuthToken` settings types, which `logit-cli` builds from
    `logit_config::KafkaSasl`.
  - `fake_broker` (now the `fake_broker/` directory), behind a `test-util` cargo feature, with `TokenEndpoint` and the other
    stubs; it re-exports `logit_aws::test_stub::HttpStub` as it does after #926.
- **`logit_proto::kafka`** (`crates/logit-proto/src/kafka/`) receives `batch.rs` and
  `compression.rs`. `next_sequence` moves here from `producer.rs`, and `decode`, `decompress`, and the xerial
  snappy reader become public, and every
  decompressor is bounded as `inflate_bounded` and `decompress_bounded` are: at most the bound plus
  one byte produced, the extra byte an error. `kafka-protocol` joins `logit-proto`'s dependencies.
  The fake broker decodes through it. No production path calls `decode` yet.
- **`logit_pipeline::tls`** receives `AsyncStream`, `PendingClose`, and `poll_pending_close`;
  `crates/logit-outputs/src/tls.rs` re-exports them for `stream.rs` and `syslog.rs`.
- **`kafka_out`** keeps `producer.rs`, `multi.rs`, `sink.rs`, `encode.rs`, `partitioner.rs`,
  `fault.rs`, and its three test modules, importing the rest from `logit-kafka`.
- **Dependencies.** `logit-inputs` and `logit-outputs` depend on `logit-kafka`. `logit-kafka`
  depends on neither `logit-config` nor `logit-outputs`; `logit-inputs` still never depends on
  `logit-outputs` as a library.
- **Tests.** `kafka_out`'s tests pass unchanged; the PR body says so, and names any test that
  moved crates. A test for each bounded decompressor: a payload one byte over its bound fails, one
  at the bound decodes. `script/audit` and `script/lint` pass; `deny.toml` changes only if
  `logit-aws` brings a license it lacks.
- **Docs.** `crates/logit-kafka/src/lib.rs`'s module doc names what lives there and why.
  AGENTS.md's "Where things live" gains `logit-kafka` and `logit-aws`, and the `kafka_out` row's
  code column stays `crates/logit-outputs/src/kafka/`. [ADR `kafka-output`](../adr/kafka-output.md)
  and [ADR `kafka-sasl`](../adr/kafka-sasl.md) keep their paths in history; a module path they
  give as current is updated where it would mislead.

### W2: the consumer core in `logit-kafka`

The protocol client, with no `Input` and no config: everything W3 drives.

- **Records.** `logit_proto::kafka::decode` becomes a hand-rolled magic-2 decoder: the 61-byte
  header, CRC-32C, the attributes (codec, transactional, control), bounded decompression, and
  each record's varint framing, with repeated header names kept in order and a non-UTF-8 header
  name reported. A magic 0 or 1 message set is reported as such, not decoded. `kafka-protocol`'s
  `RecordBatchEncoder` stays on the encode side. Tests: every batch in
  `testdata/interop/kafka/batches/` (Java and librdkafka, both snappy framings) decodes to the
  records the corpus README lists, and a batch with a flipped CRC byte, a truncated record, and a
  decompressed size one over the bound each fail as the ADR's item 9 rows say.
- **Modules** under `crates/logit-kafka/src/consumer/`:
  - `metadata.rs`: the cluster view, leaders per partition, refreshed every 5 minutes and on the
    item 9 codes, with `allow_auto_topic_creation` cleared.
  - `group.rs`: FindCoordinator, the JoinGroup and `MEMBER_ID_REQUIRED` round trip, SyncGroup,
    LeaveGroup, and the membership state; `heartbeat.rs`, its own task, reporting
    `REBALANCE_IN_PROGRESS` and lost membership.
  - `assignor.rs`: `range` and `roundrobin` over `ConsumerProtocolSubscription` v0 to v3 read and
    v0 written, and `ConsumerProtocolAssignment` v0, through `kafka-protocol`'s generated types.
  - `offsets.rs`: OffsetFetch (`require_stable` from v7), OffsetCommit with generation and member
    id, ListOffsets earliest and latest under the configured isolation, and the
    contiguous-position tracker of the ADR's item 6.
  - `fetch.rs`: one loop per leader broker, full fetches only (session id 0, epoch -1), a
    partition left out while its buffer holds records, `aborted_transactions` filtering under
    `read_committed`, `throttle_time_ms`, and the out-of-range rule.
  - `client.rs`: the API W3 drives: assignment changes, buffered records per partition,
    `mark_accepted`, `commit`, and the revoke and shutdown sequence of the ADR's item 8.
  - `pool.rs`: one connection per role (metadata, the coordinator, heartbeats, each leader's
    fetch loop), dialed on demand, with throttle muting and a per-request answer cap.
- **Fake broker.** Gains FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup,
  OffsetCommit, OffsetFetch, ListOffsets, and Fetch, served from the batches its Produce path
  stores. A group coordinator model: generations, eager rebalance on a join or a leave, session
  expiry on tokio's paused clock, committed offsets per group, and a log start offset a test can
  raise to model retention. Scripted transactions (control batches and an aborted list) and
  scripted errors through the existing `Step` and `Action`.
- **Tests**, against the fake broker, one or more per row of the ADR's item 9 table, and for:
  `range` and `roundrobin` against the Java javadoc examples; a two-member group where one member
  joins late and one leaves; a commit refused after lost membership never landing; the
  contiguous position never passing an unaccepted batch; a slow consumer of records keeping its
  session; an out-of-range position below the log start counting the gap; and versions
  negotiated within the ADR's item 3 table against a broker advertising Kafka 2.4's ranges.

### W3: `kafka_in`, the `Input`

- **Config.** `ComponentKind::KafkaIn` in `crates/logit-config/src/lib.rs`, every field of the
  ADR's item 14 with its operator doc comment, `deny_unknown_fields`, the `topics:` list-or-map
  type, `record:` reused from `kafka_out`, `TlsClientConfig` and `KafkaSasl` shared;
  `script/schema`.
- **Graph rules.** Rules 100, 101, and 102 in `crates/logit-pipeline/src/graph.rs` (its module
  doc's "Validation rules" is the canonical list), reusing rule 85's broker, topic, TLS, and SASL
  helpers, and rule 17's predicates extended to `kafka_in`. Rule 100 also rejects a
  `fetch.max_bytes` plus `max_batch_bytes` that doesn't fit Fetch's i32 `max_bytes` field: the
  consumer reads a Fetch answer under that sum plus 1 MiB.
  [docs/design/pipeline-graph.md](../design/pipeline-graph.md)'s "Validation" mirrors them. A test
  per rejection.
- **Registry.** `crates/logit-cli/src/pipeline.rs` builds `KafkaInSettings`.
- **The input.** `crates/logit-inputs/src/kafka/`: `mod.rs`, whose module doc holds the input's
  own outcomes (an undecodable record, a tombstone, an empty value, an omitted header, a deferred
  or dropped batch), each with its test, beside a pointer to `logit_kafka::consumer`'s table of
  broker outcomes (the ADR's item 9), the carriers, and the commit rule; `input.rs`, the `Input` with `run_until_shutdown` overridden for the item 8
  sequence; `decode.rs`, the `json`, `text` (both `record:` modes), and `native` decoders and the
  carriers. Batches go through `Fanout::send_resumable` as item 5 describes.
- **`kafka_out` changes.**
  - `format: text` under `record: batch` escapes `\` and LF, in `encode.rs` and its module doc.
    [ADR `kafka-output`](../adr/kafka-output.md) gets an amendment superseding "Amendment: what W3
    settled" point 5, and `docs/known-gaps/kafka.md` loses nothing, since the ambiguity was never
    listed there.
  - The payload leaves out `_logit.kafka.*`: the `ndjson.rs` walker gets a key filter that
    `kafka_out`'s `json` and `msgpack` pass and `stdio_out` doesn't, and
    `logit_proto::native::encode_events` a matching one. `crates/logit-bench/tests/allocations.rs`
    shows whether the native filter moves a pin; if it does, the constant and
    `docs/design/memory.md` change in the same commit.
- **Telemetry.** Every name in the ADR's item 15, and a `##### kafka_in` section in
  [internal-telemetry.md](../design/internal-telemetry.md) with its diagnostic keys. Its
  "Loss vocabulary" row for `logit.input.batches.deferred` widens to "the source keeps the data and
  offers it again", with an amendment to
  [ADR `telemetry-loss-vocabulary`](../adr/telemetry-loss-vocabulary.md) and a row in its
  migration record.
- **Amendments.** [ADR `delivery-semantics`](../adr/delivery-semantics.md) item 3's list of what
  an acknowledgment is gains `kafka_in`'s committed offset, and a new amendment names its commit
  past a record dropped under the ADR's item 9 or 10 as a third exception to item 3, beside the two
  native-hop ones (the at-or-below-the-mark acknowledgment and the rejected `Ack`). AGENTS.md's
  "Delivery targets at-least-once per hop" bullet, which lists those exceptions, gains it. [ADR
  `private-attribute-namespace`](../adr/private-attribute-namespace.md)'s table gains a Kafka row,
  and its whole-event render list says `kafka_out` leaves out its own family's carriers. [ADR
  `protocol-knowledge-stays-in-family`](../adr/protocol-knowledge-stays-in-family.md) gains the
  Kafka family.
- **Tests.** Against the fake broker through the real `Input`: each format under each `record:`
  mode round-trips `kafka_out`'s encoder; carriers on every event, a repeated header as an
  `Array`, a null key absent; a closed consumer stops, commits the batch before, and leaves; a
  revoke whose drain deadline passes counts `batches.deferred` and commits nothing past it;
  shutdown commits and leaves within `shutdown_grace`; a tombstone, an empty value, an
  undecodable value, and an oversize batch each counted and committed past. Each regression test
  is run once with its fix reverted.
- **Docs.** A `## kafka_in: consuming from Kafka` section in [docs/deploying.md](../deploying.md):
  topologies, the commit rule and what it costs at a crash, sizing `drain_timeout` and
  `rebalance_timeout`, the blocked-pipeline consequence, `format: text` plus `json` for
  third-party producers, mixed groups with Java consumers, and no topic auto-creation.
  `fixtures/kafka-in.yaml` against the dev stack's broker, reading `fixtures/kafka-out.yaml`'s
  topic. AGENTS.md's inputs table gains `kafka_in`; its `kafka_out` row and the transit
  paragraph's "since there's no `kafka_in`" follow in W5.
- **Known gaps** in [docs/known-gaps/kafka.md](../known-gaps/kafka.md), its index row widened to
  "`kafka_out` and `kafka_in`":
  - no KIP-848, and no cooperative rebalancing: every rebalance stops the group;
  - no static membership: a rolling restart rebalances twice per member;
  - no fetch sessions;
  - no rack-aware fetch;
  - no regular-expression topics;
  - a relayed record's key and headers aren't restored, which is also the pair's open lossless
    debt;
  - full fetches only, unmeasured against a member with many partitions.

### W4: `msgpack` and the OTLP formats

- **`logit_proto::msgpack_event`**, a reader mirroring `event_json.rs` onto
  `logit_proto::msgpack::Reader`: the object grammar of `crates/logit-outputs/src/ndjson.rs`'s
  module doc, with `crates/logit-outputs/src/msgpack.rs`'s value forms (a `bin` as `Bytes`, a
  non-finite float as a float, timestamps and ids as their text), several maps back to back as
  one batch record, and the same resource and scope grouping as `decode_batches`. Its module doc
  names what it shares with `event_json.rs` and where it differs.
- **The OTLP formats.** The `topics:` map turned into a topic-to-signal table, each record through
  `OtlpDecoder::decode_signal` or `decode_signal_json`; a request that decodes to several batches
  (one per resource) sends each, and the record is accepted once every batch is.
- **Tests.** Every event shape in the transit cases through `kafka_out`'s `msgpack` encoder under
  both `record:` modes and back through the reader, equal field for field; a truncated map, a
  wrong type for a known key, and a nested depth past the bound each refused. `otlp_proto` and
  `otlp_json` round-trip `kafka_out`'s encoder for each signal, including a request split by
  `key: trace_id` and by size.
- **Docs.** `docs/deploying.md`'s section gains the OTLP topology (the Collector's
  `kafkaexporter` into `kafka_in`), and `fixtures/kafka-otlp.yaml` a consumer leg or a sibling
  `fixtures/kafka-in-otlp.yaml`.

### W5: the transit spec, the lossless pair, and fuzzing

- **Transit.** `kafka_in` becomes a transit source: cases under `spec/transit/cases/` are record
  batch files, written by `kafka_out`'s encoder for the event shapes and taken from
  `testdata/interop/kafka/batches/` for third-party framing, decoded by the pair's decoder.
  `crates/logit-cli/src/transit/wire.rs`'s `kafka_out` row replaces its read-back convention with
  `kafka_in`'s decoder, which covers every format, so the `kafka_out` column gains `msgpack`,
  `native`, and the OTLP formats. Like-pair flows `kafka-relay-<format>` for each format but
  `text`, and `kafka-relay-text` for the log-body case. `script/transit gen`; the generated
  regions in `crates/logit-outputs/src/kafka/encode.rs` and
  [docs/known-gaps/mappings.md](../known-gaps/mappings.md) follow.
- **The pair's docs.** [ADR `lossless-transit`](../adr/lossless-transit.md) gets "Amendment: an
  eleventh like pair", pointing at the ADR's item 13 for the normalizations, and
  `crates/logit-inputs/src/kafka/mod.rs`'s module doc carries the same list. AGENTS.md's
  "Lossless like-protocol relays" (ten to eleven pairs, a `kafka_in -> kafka_out` bullet) and
  "Design constraints that aren't optional" list follow, as do the transit paragraph and the
  `kafka_out` row. The open key-and-headers debt stays in `docs/known-gaps/kafka.md`.
- **Fuzzing**, per [ADR `out-of-ci-fuzzing`](../adr/out-of-ci-fuzzing.md): a `kafka_batch` target
  over `logit_proto::kafka::decode` with its decompression bound, and a `msgpack_event` target
  over the W4 reader; a row each in `script/unsafe-check`'s `FUZZ_TARGETS`; seeds from
  `testdata/interop/kafka/batches/` and encoder round trips in `fuzz/seedgen`; a bullet each in
  the ADR's amendment listing the targets. A short `fuzz-all` run's result goes in the PR body.

### W6a: interop legs

`script/kafka-interop` gains consumer legs on both brokers, each a `logit-in-*-{kafka,redpanda}.yaml`
config and a `check.py` row, documented in
[tools/kafka-interop/README.md](../../tools/kafka-interop/README.md):

- `consume-console`: `kafka-console-producer.sh` with keys and headers into `kafka_in`
  (`format: text`), every line, key, and header carried.
- `consume-relay-<format>-<codec>`: `kafka_out` into `kafka_in` for all six formats, both `record:`
  modes where a format has them, and all five codecs, each event equal field for field.
- `consume-collector-proto`, `consume-collector-json`: the pinned Collector's `kafkaexporter` into
  `kafka_in` under the OTLP formats, each signal on its own topic.
- `consume-group-join`, `consume-group-kill`: two `kafka_in` members, one started later and one
  killed, with every record read at least once and no partition read by two members in one
  generation. `consume-group-mixed`: `kafka_in` and `kafka-console-consumer.sh` in one group,
  which picks `range`.
- `consume-broker-restart`: a broker restarted mid-stream, offsets resuming from the last commit.
- `consume-tls`, `consume-mtls`, `consume-sasl-plain`, `consume-sasl-scram512`, `consume-oauth-cc`:
  the existing listeners and identity provider, as a consumer.
- `consume-read-committed`: a transactional Java producer that commits some transactions and
  aborts others, with `read_committed` reading only the committed ones and `read_uncommitted`
  reading both. The leg runs it as a single-file Java program if the broker image's runtime
  compiles source, else from a pinned JDK image; W6a decides and the README records which.

The README's "Recorded run" records a run on both brokers.

### W6b: the soak scenario

`tools/soak/scenarios/kafka-consume-broker-restart/`: a generator `logit` (`generate_in ->
kafka_out`, `format: json`, `record: event`) into the stack's broker, and the SUT `kafka_in ->
aggregate (temporality: cumulative) -> prometheus_out` remote-write into VictoriaMetrics.

- **Harness.** A new source `wire = "kafka"`, checked at load against the SUT's `kafka_in` and the
  generator's `kafka_out`. G is the records the generator's sink saw accepted, W the SUT's
  `logit.input.records`. Kafka holds every record, so the wire is judged as a file wire is: the
  run's W − G against a replay band summed over the SUT's lives, each killed life allowing the
  records since its last commit (its commit rate × `commit_interval`, plus one fetch), each
  membership loss the same, and each revoke its `logit.input.batches.deferred` records. A deficit
  FAILs. The end sequence waits for the SUT's `logit.input.lag` to read 0 on every partition
  before stopping it.
- **Rows.** `ledger.wire` (the `kafka` wire), `ledger.intake`, `ledger.aggregate`, `ledger.egress`,
  and `ledger.summary`, the last two taking the surplus `ledger.wire` allowed, since a replayed
  increment reaches VictoriaMetrics twice.
- **Steps.** A broker `restart`, a broker `pause` longer than `session_timeout` (membership lost),
  a SUT `kill`, and a SUT `stop`.
- **`[[expect]]` rows.** `logit.input.records.dropped` max 0; `logit.input.rebalances` min 1 after
  the kill's restart and after the pause; `logit.input.commits{class="fenced"}` min 1 in the pause
  window; `logit.input.lag` returning to 0 within the recovery bound after each step.
- **Docs.** [tools/soak/README.md](../../tools/soak/README.md)'s source and wire sections, its
  scenario list, and AGENTS.md's soak paragraph.

### Mechanics

Per AGENTS.md "Workflow": each workstream is a PR stacked on the one before, review runs on it
before the fix rounds, every regression test is run once with its fix reverted and the PR body
says so, and the prose sweep (docs, ADRs, module docs, config field docs, `docs/known-gaps/`)
precedes each review. A broker fact a test asserts comes from Kafka's source or a recorded run,
never from the code under test.

**Siblings.** W4 and W5 stay sequential: W5's transit source, read-back, and fuzz targets cover
every format, so they read W4's decoders, and splitting them would have two branches regenerate
`spec/transit/`'s generated files. W6a and W6b are siblings off W5: the interop harness and the
soak harness share no file, and neither changes `kafka_in`.

## Workstreams

| # | Branch | PR title | Size | Depends on | Status |
|---|---|---|---|---|---|
| W0 | `kin/w0` | `kin/w0: ADR and plan for kafka_in, a consumer-group client` | S | — | open |
| W1 | `kin/w1` | `kin/w1: the logit-kafka crate, shared by kafka_out and kafka_in` | M | W0, the `s3` stream's `logit-aws` PR | open |
| W2 | `kin/w2` | `kin/w2: the consumer core: group membership, assignors, offsets, and fetch` | L | W1 | open |
| W3 | `kin/w3` | `kin/w3: kafka_in, with json, text, and native, and kafka_out's text batch escape` | L | W2 | open |
| W4 | `kin/w4` | `kin/w4: kafka_in's msgpack and OTLP formats` | M | W3 | open |
| W5 | `kin/w5` | `kin/w5: kafka_in in the transit spec, the eleventh lossless pair, and fuzz targets` | M | W4 | open |
| W6a | `kin/w6a` | `kin/w6a: kafka_in interop legs on Apache Kafka and Redpanda` | M | W5 | open |
| W6b | `kin/w6b` | `kin/w6b: the kafka-consume-broker-restart soak scenario` | M | W5 | open |

## Findings

Recorded per workstream as each lands.

- **W0**: Redpanda v26.2.3 answers `ConsumerGroupHeartbeat` (68) `UNSUPPORTED` and serves the
  classic group APIs at Kafka 2.4's versions or above (JoinGroup v0 to v6, SyncGroup v0 to v4,
  Fetch v4 to v13), checked with `kafka-broker-api-versions.sh`. `kafka-protocol`'s `Record`
  collapses repeated header names, so W2 decodes records itself.
- **W1**: `kafka_out`'s `http::try_read_body_bytes` had no caller once `oauth.rs` moved, so it
  left `logit-outputs`; `build_client` and `client_builder` stay. `prometheus::compression::decompress_bounded`
  refuses a zstd frame whose declared window exceeds the bound, and the reference clients' corpus
  frames declare a 2 MiB window (descriptor `0x58`), so `logit_proto::kafka`'s zstd decompressor
  streams through a `take(max + 1)` under a window limit of `max(max, 8 MiB)` instead of reusing
  it; `ruzstd` decodes until its ring buffer passes the declared window before a reader sees a
  byte, so the window limit, not the `take`, bounds a frame's memory. `Dialer` gained a
  `required` list of API keys a broker must offer or be `Refused`, which the producer sets to
  Produce, Metadata, and InitProducerId; the consumer sets its own.
- **W2**: `kafka-protocol`'s `ListOffsetsRequest` defaults `replica_id` to 0, a follower's id, so
  the consumer sets -1. The consumer tops Fetch at v12, since v13 names topics by id. Four of its
  departures from the ADR's first draft are now in the ADR: `CORRUPT_MESSAGE` on a Fetch answer
  refetches with no end while a bad batch is refetched once and then skipped, only the first
  partition with data gets a batch over `max_partition_bytes` (KIP-74), a partition with no
  commit commits its start position, and `legacy_format` counts offsets. The periodic commit runs
  inside `Consumer::next`, so the input calls it between batches. A dial failure carries the
  stage it failed in (`conn::DialStage`), which tags `broker.errors`. The outcome table is
  `crates/logit-kafka/src/consumer/mod.rs`'s module doc, not the input's.
- **W3**: The input decodes a whole `Fetched`, then cuts it by `receive.batch_max_events` and
  `batch_max_bytes` (record value bytes, a record never split), so every record's outcome is
  counted before any batch goes out. To offer a batch again after a send slice,
  `logit_pipeline::SendTimeout` now hands the batch back. `logit validate` accepts every
  schema value. At shutdown the batch in flight waits the lesser of `drain_timeout` and half of
  `receive.shutdown_grace`, leaving the other half for the commit and LeaveGroup. The batches
  behind a `closed_consumer` drop count `batches.deferred`, since nothing past the dropped one is
  committed. A `text` value is never `empty`: an empty value is a log with an empty message, as
  `kafka_out` writes one. The `fetch` byte bounds are byte-count strings, as every other byte field
  in the config is, so `min_bytes: 1` is written `"1"`. Rule 100 bounds `fetch.max_bytes` plus
  `max_batch_bytes` by Fetch's `i32` `max_bytes` field, not by the connection's 64 MiB response
  cap. A repeated header name can't come from `kafka-protocol`'s producer types, so the carrier
  tests hand-build a record batch.
- **W4**: `logit_proto::msgpack::Reader` already read every format the grammar uses, so it gained
  nothing; the bounds live in `logit_proto::msgpack_event`. `event_json` now parses a line into a
  tree of its own (`event_tree::Node`) rather than `serde_json::Value`, so one walk reads both
  formats and keeps a `bin` and a non-finite float apart. A MessagePack input's trees are charged to
  the default 256 MiB `DecodeBudget`, which JSON has no equivalent of. `logit_proto::otlp` has no
  decode budget to reuse: `otlp_in`'s one bound is its 4 MiB cap on an HTTP body, so an OTLP record
  is bounded by `max_batch_bytes` alone. A request's batches needed no change to the input: a
  record's batches always fall in one cut, which is marked after its last batch. A logs request on
  the metrics topic is `undecodable` under both encodings. OTLP/JSON ignores a field it doesn't
  know, so under `otlp_json` such a request decodes to no event, and the input reads its top-level
  `resourceLogs`, `resourceMetrics`, or `resourceSpans` list, or the snake_case `resource_logs`,
  `resource_metrics`, or `resource_spans` the decoder also reads, to tell it from an empty request.
  A list with no entry carried nothing, and the request stays `empty`. `kafka_out` never writes a
  request of several resources, so the test builds one by concatenating two protobuf requests. The
  OTLP encoder stamps a log with no observed time with the clock, so the split round trip sets one.
- **W5**: `kafka_out` under an OTLP format dropped an event with no log, span, or metric uncounted;
  it now counts `logit.output.events.skipped{reason="no_payload"}`, as `otlp_out` does. `kafka_out`
  under an OTLP format also wrote a `u64` attribute above `i64::MAX` as a double uncounted; it now
  counts `logit.output.attributes.normalized{reason="u64_as_double"}` for each one on an event that
  reaches a request, and on the resource and scope when any record is written, as `otlp_out` does.
  Its records carry the time it encoded them as their timestamp, as the Collector's exporter's do,
  so item 13's record-timestamp normalization now says so, and the transit driver restamps those
  timestamps with the run's clock to keep the expectations stable. The cases are `kafka_out`'s
  records built from the other sources' events, in every format and codec, the Java producer's and
  kcat's segments read in place under `format: text`, and a hand-written OTLP case for the
  `otlp_out` normalizations no producer case reaches (a finite last histogram bound, a `Timestamp`
  attribute, and a log with no trace beside a span). Series that share a name across source cases
  pair wrongly in one case, so `statsd/sub-millisecond`, `sub-millisecond-equal`, and `timestamped`
  are cases of their own. Under `format: text` the class check skips metric records: the log and
  span rows' `beside` cells account for them. The key-and-headers debt is the row
  `attributes/kafka_key_headers`, `lost` and uncounted at `kafka_out`, the eleventh
  `UNCOUNTED_ALLOWED` entry. The `kafka_out` cells no flow reaches are mostly OTLP-format attribute
  rows for other protocols' carriers, listed in `spec/transit/coverage.md`. The `kafka_batch` and
  `msgpack_event` fuzz targets ran 120 s each with no finding: `kafka_batch` 1.93 million runs,
  `msgpack_event` 5.17 million over 14 jobs, with no out-of-memory or timeout.
  `logit_proto::kafka::decode` leaves a tail of fewer than 12 bytes unread without reporting it, so
  the transit read-back fails unless the batch frames end where the bytes do.
- **W6a**: Every consumer leg passed on both brokers, with no `kafka_in` bug and nothing in the
  ADR's protocol table contradicted. A broker restart within the session timeout doesn't cost a
  classic group its members: Apache Kafka 4.2.0 and Redpanda v26.2.3 both reload the group with
  its offsets, so the member read through the restart with no rejoin and committed past the
  topic's end offsets at the restart on every partition before the leg stopped it. The leg then
  restarts the broker a second time with the member stopped to show a later life resuming from
  the commit.
  Java 4.2's console consumer and `kafka_in` settle on `range` on both brokers, and each side's
  revoke commits before the rejoin, so partitions move with nothing read twice. Both brokers name
  the aborted transactions under `read_committed`, Redpanda included. The OTLP formats' relay
  differs only by the two OTLP normalizations, the observed time `kafka_out` stamps and the
  `_logit.otel.severity_*` carriers the decoder writes. The broker image's runtime is a JRE with
  no `jdk.compiler`, so `Txn.java` compiles in a pinned `eclipse-temurin` JDK image and runs on the
  broker image's JRE and client jars. `kafka-console-producer.sh`'s `null.marker` writes a null
  header value. `rpk topic create` creates the harness's 119 topics on each broker in a
  call per partition count, where a `kafka-topics.sh` per topic starts a JVM each time.
- **W6b**: A 10-minute run passed every row (`perf/results/soak/20261030T183229Z`, not
  committed): 605,800 records accepted, 610,400 read, and 605,400 stored. The kill replayed
  4,354 records, about 4.4 s at 1,000 a second, inside one `commit_interval`. The 40 s broker
  pause lost the membership once and fenced one commit, and the rejoined member replayed almost
  nothing, because the paused broker served no records past the last good commit. The broker
  restart caused no rebalance: the member reconnected inside its 10 s session, counting 13
  `logit.input.broker.errors`. A stop left the group cleanly, so the next life joined with
  `reason="start"` and no batch was deferred. The topic must exist before the SUT starts, since
  `kafka_in` rechecks a missing topic every 5 minutes, so the scenario's compose override
  creates it in the broker's healthcheck. A membership loss's allowance uses the life's peak
  read rate, which after a pause is the catch-up rate (8,921 a second here), so the band's upper
  edge, 63,972, is loose; the deficit side is what catches loss.
