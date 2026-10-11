---
created: 2026-10-11
updated: 2026-10-11
---

# Enabling plan: `kafka_out` — a pure-Rust Kafka producer

## Context

`logit` has no Kafka support, and Kafka is where many deployments land logs and telemetry. This
plan builds `kafka_out`, a sink that produces to a Kafka topic, starting with JSON records and
adding `text`, `msgpack`, and `native`. The decisions are [ADR `kafka-output`](../adr/kafka-output.md):
the client, the semantics it matches and where it departs from them, the fault classes, and the
formats. This plan is the build order and how each step is checked.

Goals:

- Produce to Apache Kafka 3.x and 4.x with the Java producer's defaults: `acks: all`, idempotence
  on, murmur2 keyed partitioning that lands where a Java client lands, and every response
  classified as `Clean`, `Ambiguous`, `Rejected`, or `Refused`.
- Stay pure Rust and rustls-only, with certificate reload through `logit_pipeline::tls`.
- Keep each format decodable so a later `kafka_in` can round-trip it.

Non-goals for this stream:

- SASL of any kind (PLAIN, SCRAM, OAUTHBEARER, AWS MSK IAM). TLS and mTLS are the only
  authentication.
- `kafka_in`, a consumer-group client.
- The `otlp_proto` and `otlp_json` formats of the OTel `kafkaexporter`.
- Pipelined produce (more than one request in flight per broker).
- A topic chosen per event. `route`/`target` with one `kafka_out` per topic covers it for now.
- Transactions.

Stream key **`kafka`**: branches `kafka/w0` to `kafka/w4`, stacked. PR stack only: Ross directs
merging.

## Workstreams

| # | Lands | Acceptance |
|---|---|---|
| W0 | This plan, [ADR `kafka-output`](../adr/kafka-output.md), and the recorded corpus: `script/record-fixtures kafka`, `tools/record-fixtures/kafka/`, and [testdata/interop/kafka/](../../testdata/interop/kafka/README.md) | `script/check` passes; the corpus regenerates with `script/record-fixtures kafka` and its partitioner cross-check passes |
| W1 | The producer core in `crates/logit-outputs/src/kafka/`, not a new crate ([ADR `crate-layout-and-build-speed`](../adr/crate-layout-and-build-speed.md)): a tokio connection with rustls from `logit_pipeline::tls`; `ApiVersions`, `Metadata`, `InitProducerId`, and `Produce` v3 to v12; record batch v2 through `kafka-protocol`, with all five codecs through its compression hook (gzip through `flate2`, raw snappy through `snap`, lz4 through `lz4_flex`, zstd through `ruzstd`) and none of its codec features; murmur2 and sticky partitioners; idempotent sequence state; an in-process fake broker on the crate's `broker` feature, decompressing through the same codecs plus a xerial snappy reader | murmur2 matches every vector in `murmur2.json`; each compressed batch decodes to the same records as `batches/*-none.segment`, and a batch `kafka_out` encodes decodes through the crate's decoder; a retry against the fake broker carries the same PID, epoch, and base sequences, and a partition batch that settles without being appended bumps the epoch, after which that partition's next batch starts at sequence 0; `cargo-deny` and `cargo-audit` pass with the new crates |
| W2 | `kafka_out` with `format: json`: `ComponentKind::KafkaOut` (`brokers`, `topic`, `key`, `format`, `compression`, `acks`, `idempotent`, `max_request_bytes`, `timeout`, `headers`, `tls`) and `script/schema`; graph rule 84 (idempotence without `acks: all`, a zero bound, empty `brokers` or one that isn't a `host:port`, an illegal topic name, a bad `tls`), mirrored in `docs/design/pipeline-graph.md`; registration in `crates/logit-cli/src/pipeline.rs`; the module-doc response-class table; `fixtures/kafka-out.yaml` and a KRaft `kafka` service in `compose.yaml`; the AGENTS.md outputs table, `README.md`, `docs/OVERVIEW.md`, `docs/known-gaps/kafka.md`, and `docs/deploying.md` | Each row of the response-class table has a fake-broker test, and each regression test fails with its fix reverted; `logit run fixtures/kafka-out.yaml` against the dev-stack broker writes JSON objects, keys, headers, and partitions that `kafka-console-consumer.sh` and kcat read back as expected |
| W3 | **Done.** `format: text`, `msgpack`, and `native`, and `record: event \| batch`; rule 84's clause rejecting a `key` with a batch record (no setting is ignored by a format, so rule 84 has no clause for one; [ADR `kafka-output`](../adr/kafka-output.md), "Amendment: what W3 settled"); the msgpack value forms in `crate::msgpack`'s module doc; one walker in `ndjson.rs` writing both JSON and MessagePack | Each format's records decode back to the events that went in, in both record modes (the `kafka_in` round-trip property, checked in-process and against `apache/kafka:4.2.0` through kcat); allocation pins for the new encode paths in `crates/logit-bench/tests/allocations.rs` and `docs/design/memory.md` |
| W4 | **Done.** `script/kafka-interop`, out of CI, modeled on `script/victoria-interop`, and one soak scenario in `tools/soak/scenarios/` | Every leg of the interop plan below reports `PASS` or a `GAP` with a known-gaps entry; the soak scenario shows no uncounted loss |

Landing order: W0 → W1 → W2 → W3 → W4, linear. Each PR is based on and targets its parent's
branch and is brought up to date with `git merge origin/main`, never a rebase.

## Interop plan (W4)

`script/kafka-interop` runs pinned Apache Kafka (KRaft) and Redpanda in compose beside one `logit`
per leg, reads each topic back with the reference tools, and prints one `PASS`/`GAP`/`FAIL` row per
leg:

- **Partition parity.** The same keys, produced by `kafka_out` and by `kafka-console-producer.sh`,
  land on the same partitions.
- **Codecs.** A topic written with each of `none`, `gzip`, `snappy`, `lz4`, and `zstd` stores
  batches under that codec and is read by the Java consumer and by kcat.
- **Idempotent dedup.** A `tc netem` drop of the broker's response after the write leads to a
  resend and no duplicate in the topic, while a non-idempotent control sink's resend stores one.
- **TLS and mTLS.** A broker with a private CA, and one that requires a client certificate.
- **Timestamps.** An event stamped more than an hour ahead is dropped and counted; the rest of its
  batch lands.
- **Redpanda.** The legs above against Redpanda, with each difference recorded.

The soak scenario restarts the broker mid-run and checks the ledger for uncounted loss.

## Findings (W4)

`script/kafka-interop` against `apache/kafka:4.2.0` and `redpandadata/redpanda:v26.2.3`, run
2026-10-11 (`perf/results/kafka-interop/20261011T212530Z/`): 21 of 22 rows `PASS`, one `GAP`, no
`FAIL`, and no `kafka_out` bug.

| Leg | Apache Kafka 4.2.0 | Redpanda v26.2.3 |
|---|---|---|
| Partition parity | `PASS`: all 50 keys on the console producer's partition, 6 of 6 partitions used | `PASS`, the same |
| Codecs `none`, `gzip`, `snappy`, `lz4`, `zstd` | `PASS` each: 100 records, each event once, in both the Java consumer and kcat; `kafka-dump-log.sh` shows all 10 stored batches under the named codec | `PASS` each: the same read-back; rpk shows all 100 records' batches under the named codec |
| Idempotent dedup | `PASS`: 800 records for 800 events after one `Ambiguous` attempt; the non-idempotent control stored 10 duplicates | `PASS`, the same |
| TLS, mutual TLS | `PASS`: 50 records each; with no client certificate, 0 records and only `clean` attempts | `PASS`, the same |
| Timestamps | `PASS`: 90 of 100 records landed, the 10 stamped at 2100-01-01 dropped and counted as `invalid_timestamp` | `GAP`: 0 landed; Redpanda answers `INVALID_TIMESTAMP` naming no record, so each batch of 10 was rejected whole, counted as `rejected` |

What the run showed:

- **Each codec row reads the codec the broker stored.** Both consumers decode any codec, so the
  read-back alone can't show that `compression:` reached the wire. On Kafka, `kafka-dump-log.sh`
  reads the topic's segment file. Redpanda's segments aren't in Kafka's file format, so `rpk topic
  consume -f '%a{compression}'` reports each record's batch codec as Redpanda serves it on a
  fetch; with `compression.type: producer`, the default on both brokers, that's the stored
  batch's codec. Forcing one leg's sink to `compression: none` turns its `codec-zstd` row `FAIL`
  on both brokers.

- **The dedup leg isn't vacuous.** For 8 s the broker's packets to the leg were dropped while its
  requests still arrived, so the broker appended a batch whose response never came. Both sinks
  timed out once (`Ambiguous`) and resent after the drop cleared. The idempotent sink's resend
  carried its first attempt's sequence, and the broker discarded it; the control sink's resend
  was appended again, which is the duplicate the idempotent producer prevents. The row `FAIL`s
  unless the control stored a duplicate, because an `Ambiguous` attempt whose first try never
  reached the broker proves nothing about idempotence. With the netem drop skipped
  (`NETEM_FOR=0`), both brokers' rows `FAIL`.
- **Redpanda agrees with Kafka on every leg but one.** It places keyed records where the Java
  partitioner does, takes all five codecs, deduplicates an idempotent resend, and enforces
  mutual TLS. Like Kafka 4.x, it refuses a record more than an hour ahead
  (`message.timestamp.after.max.ms`, 3,600,000 by default), but it fills no `record_errors`, so
  `kafka_out` can't drop the one record and rejects the partition batch under item 5 of
  [ADR `kafka-output`](../adr/kafka-output.md). [docs/known-gaps/kafka.md](../known-gaps/kafka.md)
  records it. Only Redpanda's `timestamp` row can read `GAP`: Apache Kafka is the reference, so
  any outcome but the expected one `FAIL`s its row.
- **The soak scenario lost nothing and stored nothing twice.** `script/soak run
  kafka-broker-restart` (8 minutes 30 seconds, `perf/results/soak/20261011T213158Z/`) restarted,
  paused for 40 s, and stopped for 60 s the broker under a 2,000 lines/s statsd stream: every row
  `PASS`, 1,032,400 increments absorbed and 1,032,400 in the topic's distinct records, and 25,900
  records in the topic, all distinct, for 25,900 accepted. The pause produced one `Ambiguous`
  attempt, and its resend stored no duplicate. The harness deduplicates the topic before it sums
  V, so a duplicate can't hide a loss, and `ledger.topic` `FAIL`s a duplicate in a SUT life with
  no producer epoch bump. See
  [docs/plans/soak-harness.md](soak-harness.md)'s "Findings".

## Verified so far

[testdata/interop/kafka/README.md](../../testdata/interop/kafka/README.md) records what W0 checked
against `apache/kafka:4.2.0` and kcat 1.7.1 (librdkafka): the Java client's murmur2 vectors, the
partitioner cross-check, the stored record batches for each client and codec (Java writes xerial
snappy and is idempotent by default; kcat writes raw snappy and isn't), and the broker's
`ApiVersions` ranges. The ADR's semantics table was checked against the Java client's and
librdkafka's source.

## Verification

- `script/check` on every PR, and `script/cibuild` before each PR opens.
- Fake-broker tests pin every row of the response-class table.
- The W0 vectors are matched byte for byte where the encoding is deterministic: murmur2 results,
  and record bodies once timestamps, producer ids, and CRCs are set aside.
- W2's end-to-end run against the dev-stack broker, and W4's interop and soak runs.
- `pr-review N --comment` on each PR.
