---
created: 2026-10-12
updated: 2026-10-12
---

# `kafka_out` OTLP formats: one export request per signal per record, a topic per signal, and a producer per topic behind a multi-topic facade

## Status
Accepted

## Context

The OpenTelemetry Collector's `kafkaexporter` is the most common way OTLP data reaches Kafka, and
its `kafkareceiver` is the most common way it leaves. Both default to `otlp_proto`: each record
holds one whole `Export*ServiceRequest`, and the receiver decodes one request per record and
ignores the record's headers. `kafka_out` writes `json`, `text`, `msgpack`, and `native` ([ADR
`kafka-output`](kafka-output.md), item 11), none of which a `kafkareceiver` reads, so a Collector
downstream of `logit` can't consume what `kafka_out` produces. That ADR lists the `otlp_proto` and
`otlp_json` formats as a follow-up for parity with the exporter.

The exporter's defaults, checked against `opentelemetry-collector-contrib` v0.162.0:

- One record per `Export*ServiceRequest`, one request per signal, written to a topic per signal:
  `otlp_logs`, `otlp_metrics`, and `otlp_spans`.
- No key by default. `partition_traces_by_id` and `partition_logs_by_trace_id` split the request
  by trace id and key each piece with the trace id as 32 lowercase hex characters.
  `partition_metrics_by_resource_attributes` and `partition_logs_by_resource_attributes` key by a
  hash of the resource's attributes, computed with the Collector's internal `xhash` package.
- No size splitting: a request over the broker's limit fails.
- No record timestamp of its own: the record carries the time the client produced it.
- An opt-in `signal_header` adds an `otelcol.signal` header naming the signal.

`kafka_out`'s producer serves one topic (`crates/logit-outputs/src/kafka/producer.rs`): its
metadata, missing-topic state, idempotent identity, and the sink's per-batch `Pending` are all
scoped to it. `logit_proto::SignalEncoder` already splits an `EventBatch` into one OTLP payload per
non-empty signal for `otlp_out`.

The work lands as stream `kotlp`: W1 adds the facade, `otlp_proto`, `topics:`, and the keys; W2
adds an OTLP/JSON encoder in `logit-proto` and `otlp_json`; W3 adds the interop legs.

## Decision

`kafka_out` gains `format: otlp_proto` and `format: otlp_json`. Each record holds one OTLP export
request for one signal, written to that signal's topic by a producer per topic behind a facade
whose API is the one a future multi-topic producer exposes.

### 1. The record unit

A batch becomes one export request per non-empty signal, as `otlp_out` encodes it. Each request is
one record. `key: trace_id` (item 5) splits a request further, and the size cap (item 6) splits it
further again; nothing else does.

`SignalEncoder::encode_signals` encodes a whole batch only. Splitting needs a request built from a
subset of a batch's events, so `logit-proto` gains a function that builds one signal's prost
request from events picked by reference or index, under the batch's resource and scope. It builds
no sub-`EventBatch`: cloning one per piece would move `logit-bench`'s allocation pins. `otlp_proto`
serializes the prost value, and so does `otlp_json` (item 8). Trace-id grouping and the size
bisect both build their pieces through it.

`record:` is rejected under both OTLP formats. The record unit is fixed, so neither `event` nor
`batch` describes it. Checks that read `RecordFormat::default_record` to learn whether a record
holds many events (`KafkaOutput::new`'s key check, graph rule 85's batch derivation) treat the OTLP
formats as their own record unit, so a key isn't rejected as if the record were a batch.

### 2. Topics

Under `otlp_proto` and `otlp_json`, an optional `topics:` map names each signal's topic, and each
key it leaves out takes the exporter's default:

```yaml
format: otlp_proto
topics: {logs: otlp_logs, metrics: otlp_metrics, traces: otlp_spans}   # the defaults
```

- `topic:` becomes optional in config. The other formats still require it and reject `topics:`.
  Under the OTLP formats, when `topic:` is set, every signal goes to that one topic. Setting both
  `topic:` and `topics:` is a config error.
- Graph rule 85 checks each `topics:` value as it checks `topic:`, and accepts only the keys
  `logs`, `metrics`, and `traces`.
- **Topics resolve lazily.** A topic's producer opens on its first record, so a
  logs-only pipeline never asks the broker about `otlp_metrics` and never causes the broker to
  auto-create it. A topic that's misconfigured or missing surfaces only when its signal first
  arrives.

### 3. A producer per topic behind a multi-topic facade

The sink drives a facade, not a `Producer`. Records carry their topic, and the facade prepares,
attempts, and settles per topic. Inside it, each topic has its own `Producer`, unchanged: its own
connections, metadata, missing-topic state, producer id (PID) and epoch, and `Pending`.

The facade is keyed by topic name, so two signals mapped to one topic share one producer. It holds
only the `ProducerConfig` template, and producers open lazily, including after a rebuild: the
sink's TLS and credential rebuild drops the producers rather than recreating them, and the next
record opens each again.

A future topic chosen per event ([ADR `kafka-output`](kafka-output.md), Consequences) replaces the
facade's internals with one producer that holds several topics' metadata and partition batches
under one PID, and that sends one produce request per broker across topics. The rework boundary is
the facade's API: the sink and the encoder above it don't change, and
nothing outside the facade constructs a `Producer`.

What a producer per topic costs until then:

- A sink writing three signals holds up to three connections to each broker and three PIDs.
- Three produce requests go to a broker where one could carry all three topics.

### 4. Partial success and head-of-line blocking

Each signal's partition batches settle on their own, as partitions do within one topic ([ADR
`kafka-output`](kafka-output.md), item 4). A retry resends only the unsettled partition batches,
so a signal that the broker accepted isn't resent while another signal is retried.

**Prepare is per topic.** A topic whose prepare fails keeps no `Pending`; the topics that did
prepare keep theirs, and the next attempt re-prepares only the failed topic. The attempts of the
topics run concurrently, so one topic's metadata wait, bounded by `timeout`, doesn't stack across
topics.

**One verdict, computed by the facade.** A topic that failed to prepare, or whose attempt failed at
topic level (identity, metadata) before any partition answered, counts as unsettled with that
error's fault class. The facade reads every topic's raw partition states, with one any-accepted
flag shared across topics, and applies the rule:

- If anything is unsettled, the verdict is the strongest class among the unsettled: `Refused` over
  `Ambiguous` over `Clean`.
- If nothing is unsettled, it's success when anything was accepted, else `Rejected`.

The facade doesn't merge per-topic verdicts by rank. `Pending::verdict` ranks `Rejected` and
`Clean` alike, so a merged `Rejected` would drop a batch while another topic is still unsettled and
retryable. The producer therefore exposes its settlements, and not a verdict of its own, to the
facade.

**Delivery postures span every topic.** Under `buffer.delivery: at_most_once`, an `Ambiguous`
fault on any topic drops the batch, so a `Refused` metrics topic triggers the drop on the logs
producer's partition batches. `abandon` at shutdown and the count of records lost likewise cover
every topic.

A `Refused` topic holds the whole sink, as a refused signal holds `otlp_out`. A metrics topic that
the broker won't authorize stops logs and spans behind it, although their own topics accept
writes. The remedy is one `kafka_out` per signal, each behind `has_signal`, and `keep_signals` when
an event carries more than one signal. Because topics resolve lazily, each of those sinks opens a
producer for its own signal only.

### 5. Keys

| `key:` | Under `otlp_proto` and `otlp_json` |
|---|---|
| `none` (the default) | One unkeyed request per signal per batch, as the exporter's default. |
| `trace_id` | Spans and logs split into one request per trace id, each keyed with the trace id as 32 lowercase hex characters: the bytes the exporter's `partition_traces_by_id` and `partition_logs_by_trace_id` write. Metrics stay one unkeyed request. |
| `{resource: NAME}` | The whole request keyed by that resource attribute's value, with [ADR `kafka-output`](kafka-output.md)'s key text. An `EventBatch` has one resource, so every request from the batch has the same key. |
| `{attribute: NAME}` | Rejected by graph rule 85. A request holds many events, and each can have a different value. |

Spans and logs with no trace id, under `key: trace_id`, and a request whose resource lacks the
attribute, under `key: {resource: NAME}`, go unkeyed: one unkeyed request per signal, counted in
`logit.output.records.unkeyed` once per record, as for the other formats. Metrics under
`key: trace_id` aren't counted there: the key doesn't apply to them.

Each signal's key comes from that signal's own field: a span's from `span.trace_id`, a log's from
the log's `TraceRef`, which is what its OTLP log record carries. This differs from the per-event
formats, where `KafkaEncoder::key` prefers the span's id. It is also what the exporter's
`partition_logs_by_trace_id` keys by.

A keyed request goes to the partition the Java client picks for the same key bytes ([ADR
`kafka-output`](kafka-output.md), item 3).

`kafka_out` doesn't port the exporter's `xhash`-based resource key. That key hashes every resource
attribute through a package internal to the Collector, so matching it means porting Go internals
and pinning them against each Collector release, and the only thing the match buys is partition
placement shared with an exporter writing the same topic. `key: {resource: NAME}` gives the
property operators use resource keys for: one source's records on one partition.

### 6. Size

The budget is per record and includes its key: `value_budget` less the key's bytes, since the
producer counts a record's overhead as `5 + key.len()` in `Producer::prepare`'s cap check. A keyed
piece that fits the encoder then fits the producer. A request over the budget is split by events into several records, each the longest run of the remaining
events that fits, found by encoding candidates as `native` does under `record: batch` ([ADR
`kafka-output`](kafka-output.md), "Amendment: what W3 settled", item 3). Each piece keeps the
request's key. A single event whose request is over the budget alone is dropped, and each signal record it would
have formed counts one `logit.output.records.dropped{reason="oversize"}`.

The `records.*` counters count Kafka records, as for the other formats: a rejected record holding
500 spans counts 1. Events per signal aren't a separate counter. The sink's telemetry table states
these units.

The OTLP mapping's side-effect counters (`logit.output.metrics.degraded`, `skipped`, and their
warnings) count once per batch. The encoder counts through a gate, as `otlp_out` does with
`BatchAccounting`, and every candidate encode after the first runs with it muted: bisect probes,
and the re-prepare of a topic whose prepare failed.

This diverges from the exporter, which sends the request whole and loses it when the broker
refuses it. A consumer reading by trace id sees one trace's spans across several records, each a
valid request on its own.

### 7. Record timestamp

A record's timestamp is produce time, the time the encoder builds it, as the exporter's records
carry. This differs from the other formats, whose timestamp is the event's. A request holds many
events, and `INVALID_TIMESTAMP` names records, so one future-stamped event would lose the whole
request under Kafka 4.x's rule against records stamped more than an hour ahead ([ADR
`kafka-output`](kafka-output.md), item 6). Event times stay in the OTLP payload.

### 8. `otlp_json`

`otlp_json` writes what the Collector's pdata `JSONMarshaler` writes, so a `kafkareceiver` with
`encoding: otlp_json` reads it: protojson field names in lowerCamelCase, trace and span ids as
hex, 64-bit integers as strings, and enums as integers.

The encoder is new and hand-rolled in `logit-proto`, beside the OTLP/JSON decoder. `pbjson` can't
write hex ids, for the reason [ADR `otlp-json-decoding`](otlp-json-decoding.md) rejected it for
decoding. `otlp_in`'s decoder reads the encoder's output back in tests.

### 9. Verification

W3 adds legs to `script/kafka-interop` in which a real Collector's `kafkareceiver`, pinned to the
release these facts were checked against, reads `kafka_out`'s `otlp_proto` and `otlp_json` topics
on both Apache Kafka and Redpanda, and the harness compares what the Collector decoded with what
`logit` sent: every signal, the keyed partitions under `key: trace_id`, and an oversize request
split into several records that the receiver still reads.

## Alternatives considered

- **A multi-topic producer now (one PID, one produce request per broker across topics).**
  Rejected for this stream. It reworks the producer's metadata, missing-topic state, sequences,
  and `Pending` for several topics, which a topic per event also needs, and that rework is better
  designed with per-event topics in view. The facade gets exporter parity now without touching the
  producer, and fixes the API that rework keeps.
- **One topic per sink, with `has_signal` routing each signal to its own sink.** Rejected as the
  only way. It needs three sinks and three filters for the exporter's default layout, which one
  exporter config covers. It stays the remedy for head-of-line blocking (item 4).
- **Port `xhash` for byte parity with `partition_*_by_resource_attributes`.** Rejected (item 5).
- **Drop a request over the size cap, as the exporter loses it.** Rejected: `kafka_out` already
  splits a batch record that doesn't fit, and a request split into valid smaller requests reaches
  the consumer where a dropped one doesn't.

## Consequences

- `topic:` is optional in config under the OTLP formats, and `topics:` and `record:` join rule
  85's checks.
- A sink writing several signals holds a producer, connections, and a PID per topic until a
  multi-topic producer replaces the facade's internals.
- Consumer sites of `topic:` need updating. `tools/soak/soaklib/scenario.py` matches `kafka_out`
  by topic, and W1 updates it.
- A refused signal topic holds every signal in the sink. Operators who need signals isolated run a
  sink per signal.
- Known gaps, recorded in `docs/known-gaps/kafka.md` as the stream lands:
  - No `otelcol.signal` header. A consumer that tells signals apart by that header, rather than by
    topic, can't use `topic:` with several signals.
  - No byte parity with the exporter's `partition_*_by_resource_attributes` keys, so a `logit`
    sink and an exporter writing one topic place the same resource on different partitions.
  - Head-of-line blocking across signals (item 4).
  - Per-request loss on `Rejected`: the broker rejects a record whole, so one bad record drops
    every item in its request. The other formats lose one event, or one batch record.
