# Kafka producer reference vectors

Reference output from real Kafka clients, recorded by
[`script/record-fixtures kafka`](../../../script/record-fixtures), so `kafka_out`
([ADR `kafka-output`](../../../docs/adr/kafka-output.md)) can be checked against them byte for
byte. Unlike the other directories here, these aren't captures of a client's bytes on the wire.
They're the Java client's partitioner results, and the record batches a broker stored after each
client produced them. A broker stores a produce request's batch unchanged, so the segment bytes
serve as the expected encoding.

Producers and broker:

| Role | Image | Digest |
|---|---|---|
| Java client (`kafka-console-producer.sh`, `kafka-console-consumer.sh`) and single-node KRaft broker | `apache/kafka:4.2.0` (kafka-clients 4.2.0) | `sha256:9516fb7634bad307d17c33b589fde9023003b0cb761374f500002b980a3149b9` |
| librdkafka client | `edenhill/kcat:1.7.1` | `sha256:8f16a5fed099931ce1122420b7473efe467ff9841d53680b99db25dd1723d711` |
| Compiles `Murmur2Vectors.java` (the Kafka image has no `javac`) | `maven:3.9.16-eclipse-temurin-17` | `sha256:1a352420f7aba21f5ad08df31bab55f74c013fb491f1ae8ab1dd7ff9ed698584`; it only compiles the class, which runs in the Kafka image |

Recorded 2026-10-11. To regenerate, run `script/record-fixtures kafka`. Timestamps, producer ids,
and CRCs change on every run; the key set, records, and partition results don't.

## Files

| File | Source | Contents |
|---|---|---|
| `murmur2.json` | `tools/record-fixtures/kafka/Murmur2Vectors.java`, run with `java -cp '/opt/kafka/libs/*:/tmp'` in the Kafka image | 31 keys: empty, ASCII of 1 to 8 bytes (every tail case of 0 to 3 remaining bytes), keys that share a head and differ in trailing bytes, multibyte UTF-8, invalid UTF-8, and 1,025 to 1,501 byte keys. Each has `Utils.murmur2(key)` as a signed i32 and `Utils.toPositive(h) % n` for n in 1, 3, 6, 12, 100. Keys over 64 bytes are stored as `key_runs` (`[byte hex, count]`). |
| `partitioner-crosscheck.tsv` | `kafka-console-producer.sh` with `parse.key=true`; `kcat -P -K:` with `-X partitioner=murmur2_random` and with the default | The partition each text key landed on in a 6-partition topic, read back with `kafka-console-consumer.sh` (`print.partition=true`) and `kcat -C -f '%p\t%k\n'`. The script fails if either the console producer or `murmur2_random` column differs from the Java computation. |
| `batches/<producer>-<compression>.segment` | The broker's segment `00000000000000000000.log`, copied out | Three records, `key-1` to `key-3`, 55 to 57 byte values, header `h1=hv1` on each. One file per producer (`java` or `kcat`) and `none`, `gzip`, `snappy`, `lz4`, `zstd`. 178 to 288 bytes each. |
| `batches/<producer>-<compression>.txt` | `kafka-dump-log.sh --print-data-log --deep-iteration` on that segment | The broker's reading of the batch. |
| `api-versions.txt` | `kafka-broker-api-versions.sh` | The Produce, Metadata, InitProducerId, and ApiVersions ranges, then the broker's whole answer. |

Invocations for the batches (topic `kafka-record-b-<producer>-<compression>`, one partition):

- Java: `kafka-console-producer.sh --reader-property parse.key=true --reader-property parse.headers=true --compression-codec <c>`, input lines `h1:hv1<TAB>key-N<TAB>value`.
- kcat: `kcat -P -K: -H h1=hv1 -z <c> -X queue.buffering.max.ms=500` (no `-z` for `none`).

## What the dumps show

All ten batches have `magic: 2`, `count: 3`, `isTransactional: false`, `isControl: false`,
`partitionLeaderEpoch: 0`, and `CreateTime` timestamps. `compresscodec` matches the request.

- **Java is idempotent by default.** Every Java batch has a real `producerId` (1 to 5, one per
  producer run), `producerEpoch: 0`, and `baseSequence: 0` (`lastSequence: 2`). The first
  `InitProducerId` on a fresh broker returns id 1, so ids depend on run order.
- **kcat is not.** Every kcat batch has `producerId: -1`, `producerEpoch: -1`,
  `baseSequence: -1`, because librdkafka defaults to `enable.idempotence=false`.
- **Timestamps** are the client's wall clock; neither tool takes a timestamp option. The Java
  records within a batch can carry different timestamps (the batch `baseTimestamp` is the
  first's); kcat's three share one.
- **Snappy framing differs between the clients.** The Java payload begins `82 53 4e 41 50 50 59 00`
  followed by version 1 and compatible version 1: xerial `SnappyOutputStream` framing, as
  snappy-java writes. The kcat payload is a raw Snappy block with no framing.
- **LZ4** is an LZ4 frame in both: magic `04 22 4d 18`, FLG `0x60` (version 1, independent blocks,
  no block checksum, no content size, no content checksum), BD `0x40` (64 KB maximum block).
- **Zstd** is a plain zstd frame (`28 b5 2f fd`) in both. **Gzip** is a standard gzip stream
  (`1f 8b 08`) in both.
- The records inside each compressed payload are the same record encoding as `none`; only the
  record bodies are compressed, never the 61-byte batch header.

## Partitioners

The Java client's default partitioner for a keyed record is `toPositive(murmur2(key)) % n`. The
console producer and `kcat -X partitioner=murmur2_random` reproduce that for every text key in
`partitioner-crosscheck.tsv`. kcat's default partitioner, `consistent_random` (CRC32), does not:
it disagrees with the Java result for 19 of the 23 keys. A producer whose keyed records must
land where a Java client's land has to use murmur2.

The cross-check omits the empty key, which the console tools can't carry, and the non-text keys.
`murmur2.json` alone covers those.
