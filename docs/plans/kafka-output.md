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
| W1 | The producer core in `crates/logit-outputs/src/kafka/`, not a new crate ([ADR `crate-layout-and-build-speed`](../adr/crate-layout-and-build-speed.md)): a tokio connection with rustls from `logit_pipeline::tls`; `ApiVersions`, `Metadata`, `InitProducerId`, and `Produce` v3 to v12; record batch v2 through `kafka-protocol` with lz4 (`lz4_flex`) and zstd (`ruzstd`) through its compression hook; murmur2 and sticky partitioners; idempotent sequence state; an in-process fake broker on the crate's `broker` feature | murmur2 matches every vector in `murmur2.json`; each compressed batch decodes to the same records as `batches/*-none.segment`, and a batch `kafka_out` encodes decodes through the crate's decoder; a retry against the fake broker carries the same PID, epoch, and base sequences; `cargo-deny` and `cargo-audit` pass with the new crates |
| W2 | `kafka_out` with `format: json`: `ComponentKind::KafkaOut` (`brokers`, `topic`, `key`, `format`, `record`, `compression`, `acks`, `idempotent`, `max_request_bytes`, `timeout`, `headers`, `tls`) and `script/schema`; graph rule 82 (idempotence without `acks: all`, a zero bound, empty `brokers`, a setting the format ignores), mirrored in `docs/design/pipeline-graph.md`; registration in `crates/logit-cli/src/pipeline.rs`; the module-doc response-class table; `fixtures/kafka-out.yaml` and a KRaft `kafka` service in `compose.yaml`; the AGENTS.md outputs table, `README.md`, `docs/OVERVIEW.md`, `docs/known-gaps/sinks.md`, and `docs/deploying.md` | Each row of the response-class table has a fake-broker test, and each regression test fails with its fix reverted; `logit run fixtures/kafka-out.yaml` against the dev-stack broker writes JSON objects, keys, headers, and partitions that `kafka-console-consumer.sh` and kcat read back as expected |
| W3 | `format: text`, `msgpack`, and `native`, and `record: event \| batch`; the msgpack grammar in its module doc; `ndjson::render_event` shared with the msgpack writer | Each format's records decode back to the events that went in (the `kafka_in` round-trip property, checked in-process); allocation pins for the new encode paths in `crates/logit-bench/tests/allocations.rs` and `docs/design/memory.md` |
| W4 | `script/kafka-interop`, out of CI, modeled on `script/victoria-interop`, and one soak scenario in `tools/soak/scenarios/` | Every leg of the interop plan below reports `PASS` or a `GAP` with a known-gaps entry; the soak scenario shows no uncounted loss |

Landing order: W0 → W1 → W2 → W3 → W4, linear. Each PR is based on and targets its parent's
branch and is brought up to date with `git merge origin/main`, never a rebase.

## Interop plan (W4)

`script/kafka-interop` runs pinned Apache Kafka (KRaft) and Redpanda in compose beside one `logit`
per leg, reads each topic back with the reference tools, and prints one `PASS`/`GAP`/`FAIL` row per
leg:

- **Partition parity.** The same keys, produced by `kafka_out` and by `kafka-console-producer.sh`,
  land on the same partitions.
- **Codecs.** A topic written with each of `none`, `gzip`, `snappy`, `lz4`, and `zstd` is read by
  the Java consumer and by kcat.
- **Idempotent dedup.** A `tc netem` drop of the broker's response after the write leads to a
  resend and no duplicate in the topic.
- **TLS and mTLS.** A broker with a private CA, and one that requires a client certificate.
- **Timestamps.** An event stamped more than an hour ahead is dropped and counted; the rest of its
  batch lands.
- **Redpanda.** The legs above against Redpanda, with each difference recorded.

The soak scenario restarts the broker mid-run and checks the ledger for uncounted loss.

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
