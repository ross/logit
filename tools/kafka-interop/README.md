# kafka-interop

`script/kafka-interop` checks `kafka_out` and `kafka_in` against a real Apache Kafka and a real
Redpanda. It runs the legs in [What it runs](#what-it-runs) against each broker, reads every
`kafka_out` topic back with the Java client and kcat, reads what each `kafka_in` wrote, and prints
one row per leg and broker. [Recorded runs](#recorded-runs) records what a run showed.

The script runs on the host and drives docker (`$DOCKER`, `sudo docker` by default), like
`script/victoria-interop` and `script/splunk-interop`. It isn't part of `script/cibuild`, and no
test depends on it running. A run takes about 10 minutes once the images are built.

## What it runs

`compose.yaml` starts one stack under the compose project `kafka-interop`, on the network
`kafka-interop-net`, with no host ports:

| Service | Image | Role |
|---|---|---|
| `kafka` | `apache/kafka:4.2.0` | single-node KRaft broker, started on `kafka-server.properties` |
| `redpanda` | `redpandadata/redpanda:v26.2.3` | single-node Redpanda in `dev-container` mode, started on `redpanda.yaml` |
| `oauth` | `ghcr.io/navikt/mock-oauth2-server:6.0.2`, pinned by digest | the OAuth 2.0 server the OAUTHBEARER legs fetch tokens from and both brokers validate them against |
| `collector-<broker>` | `otel/opentelemetry-collector-contrib:0.162.0`, pinned by digest | one per broker, started on `collector.yaml`: `kafkareceiver`s on the OTLP legs' topics, each pipeline ending in a `file` exporter that writes OTLP/JSON to the run directory's `collector/<broker>/`, `kafkaexporter`s that re-key the `keyed` traces, and `kafka/out-proto` and `kafka/out-json`, which write what the Collector decoded back to Kafka as `collector-{proto,json}-{logs,metrics,spans}` for the `consume-collector-*` legs |
| `logit-<leg>-<broker>` | `logit:kafka-interop`, built from the current tree | one per `logit-<leg>-<broker>.yaml`, and one per `logit-in-<leg>-<broker>.yaml` |

Each broker has five client listeners: plain on `:9092`, TLS on `:9093`, mutual TLS, which
requires a client certificate, on `:9094`, SASL_PLAINTEXT on `:9096`, and SASL_SSL on `:9097`.
The SASL listeners offer PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512, and Redpanda's also offers
OAUTHBEARER; Kafka has a sixth listener, SASL_PLAINTEXT with OAUTHBEARER only, on `:9098`. Only
they require authentication, so every read-back uses the plain listener and needs no credentials. The read-back runs `kafka-console-consumer.sh` and
`kafka-get-offsets.sh` inside the `kafka` container, against either broker, and kcat in
`edenhill/kcat:1.7.1` containers on the stack's network.

`AWS_MSK_IAM` has no leg: only Amazon MSK implements its server side, so the harness can't run
it. Its tests pin the signature against AWS's SigV4 test suite and a golden payload from
botocore, and the fake broker checks every payload
([ADR `kafka-sasl`](../../docs/adr/kafka-sasl.md), "Amendment: what W3 settled").

The legs, each run against both brokers:

| Leg | Config | What it checks |
|---|---|---|
| `parity` | `logit-parity-*.yaml` | 200 events keyed by 50 `user` values land on the same partitions of a 6-partition topic as the same keys from `kafka-console-producer.sh` |
| `codec-<c>` | `logit-codecs-*.yaml` | 100 events under each `compression` (`none`, `gzip`, `snappy`, `lz4`, `zstd`) are stored under that codec, and decode in both the Java consumer and kcat, each event once |
| `dedup` | `logit-dedup-*.yaml` | while the broker's packets to this leg are dropped, an idempotent sink's resend after an `Ambiguous` attempt stores no duplicate, and a non-idempotent control sink beside it stores at least one |
| `tls`, `mtls`, `mtls-required` | `logit-tls-*.yaml` | 50 events over server TLS and over mutual TLS from the run's private CA; a sink with no client certificate stores nothing |
| `sasl-plain`, `sasl-scram512`, `sasl-scram256`, `sasl-wrong` | `logit-sasl-*.yaml` | 50 events through `sasl:` with PLAIN over SASL_SSL, SCRAM-SHA-512 over SASL_SSL, and SCRAM-SHA-256 over SASL_PLAINTEXT; a SCRAM-SHA-512 sink with the wrong password stores nothing, counts every attempt as `refused`, holds its queue with `logit.component.retrying` at `1`, and drops no batch as `rejected` |
| `oauth-cc`, `oauth-static`, `oauth-refused` | `logit-oauth-*.yaml` | 60 events at 1/s through the client-credentials grant, past the 45 s life of the first token, with at least two tokens fetched, counted in the `oauth` service's log; 50 events through a fixed token the script fetched, with two SASL extensions on Kafka; and a fixed token that isn't a JWT, which the broker refuses: nothing stored, every attempt `refused`, the queue held, no batch dropped, and, on Kafka, the leg's log carrying the broker's error challenge status |
| `otlp-proto`, `otlp-json` | `logit-otlp-*.yaml` | 160 ticks, each a metrics event with five kinds (delta and cumulative sums, a gauge with unit and description, an explicit histogram, an exponential histogram), a log, and a span, written as `otlp_proto` to the default topics and as `otlp_json` to `otlpjson-*`: the Collector's `kafkareceiver` decodes 160 logs, 160 spans, and 800 data points, and every checked field (resource name, bodies and severity, trace and span ids in hex, span names, kind, status, and events, metric values, temporality, unit, bucket counts) equals what the tick number implies. One record per signal per batch |
| `otlp-keyed`, `otlp-json-keyed` | `logit-otlp-*.yaml` | under `key: trace_id`, every record's key is a 32-character lowercase hex trace id, each of 16 trace ids sits on one partition, and that partition is `murmur2(key) % 6`, the Java client's. Logs without a trace id and all metrics are unkeyed, with a null key (kcat's key length -1), not an empty one. The Collector decodes the same items. `otlp-json-keyed` adds a 4 KiB `max_request_bytes`: no record exceeds it, and `jsonbig-metrics` holds more records than batches |
| `otlp-exporter-parity` | `logit-otlp-*.yaml`, `collector.yaml` | the `keyed` spans, re-keyed by a Collector `kafkaexporter` with `partition_traces_by_id` into 6-partition topics. With the exporter's default partitioner (`sticky_key`, FNV-1a) 14 of 16 trace ids land on a different partition than `kafka_out` puts them on; with `hasher: murmur2` none does. The row passes on the murmur2 match and reports the default's count ([`docs/known-gaps/kafka.md`](../../docs/known-gaps/kafka.md)) |
| `otlp-oversize` | `logit-otlp-*.yaml` | with `max_request_bytes: 2KiB`, each batch's request splits into at least two records per signal, none over the cap and none spanning two batches (checked from the items the Collector decoded in each record), and the Collector still decodes every item |
| `timestamp` | `logit-timestamp-*.yaml` | in each batch of 10, the event stamped at 2100-01-01 is dropped and counted as `invalid_timestamp`, and the other nine land |

The consumer legs, each run against both brokers. Every `kafka_in` starts at `earliest` in a
group of its own, except the group legs' members, which share one, and writes each event it reads, with its `_logit.kafka.*` carriers, to a
`stdio_out` `format: json` file in the run directory, which `check.py` reads:

| Leg | Config | What it checks |
|---|---|---|
| `consume-console` | `logit-in-console-*.yaml` | 50 lines `kafka-console-producer.sh` wrote to a 3-partition topic, read as `format: text`: each line once, its key, its headers (`seq`, `rep` twice, which reads back as an array of both values in order, and `nil` with a null value), and its timestamp equal to the record's, and every offset below the end offsets read |
| `consume-relay-<format>-<codec>` | `logit-in-relay-*.yaml` | for each of the six formats and five codecs, `kafka_out` writes 40 ticks (each a metrics event with five kinds, a log, and a span, with string, integer, float, boolean, array, and map attributes) to their own topics, under both `record:` modes where the format has them, and `kafka_in` reads them back. Every event read, carriers removed, equals one the generator wrote, field for field, each once. The [ADR `kafka-input`](../../docs/adr/kafka-input.md) item 13 normalizations are the only differences allowed: under `text`, which carries a body alone, a body-only log arrives at its record's timestamp (its own, cut to milliseconds, under `record: event`; its record's first event's under `batch`); under the OTLP formats, each data point arrives as an event of its own, a log gains the observed time `kafka_out` stamps it with and the `_logit.otel.severity_*` carriers, as `otlp_in -> otlp_out` does |
| `consume-collector-proto`, `consume-collector-json` | `logit-in-collector-*.yaml`, `collector.yaml` | the Collector's `kafkaexporter` writes what its receivers decoded from the `otlp-proto` and `otlp-json` legs back to Kafka as `otlp_proto` and `otlp_json`, one topic per signal, and `kafka_in` reads 160 logs, 160 spans, and 800 data points, every checked field as the tick number implies |
| `consume-group-join` | `logit-in-group-join-{a,b}-*.yaml` | member `a` writes 400 records keyed by `seq` to a 6-partition topic and reads it; member `b` joins 20 s later. Every offset is read, no offset is read twice, and no partition changes member more than once in offset order |
| `consume-group-kill` | `logit-in-group-kill-{a,b}-*.yaml` | both members start together and the script kills `b` with SIGKILL 20 s in. Every offset is read, `a` reads again at most a commit interval of what `b` had read, and no partition changes member more than twice: to `b` at its join, and back after its session expires |
| `consume-group-mixed` | `logit-in-group-mixed-*.yaml` | `kafka_in` and a `kafka-console-consumer.sh` share one group. `kafka-consumer-groups.sh --describe --state`, read while both are members, shows two members and the `range` strategy, and their reads together cover every offset, with no partition changing member more than twice |
| `consume-broker-restart` | `logit-in-restart-*.yaml`, `logit-in-restart-produce-*.yaml` | runs alone after the other legs: 300 records at 5 a second. The broker restarts 20 s in with the member reading, and the member's group commits past the end offsets the topic has once both brokers are healthy again on every partition, read from `kafka-consumer-groups.sh --describe`, which only a fetch made after the restart can do; then the member stops, which commits and leaves, the broker restarts again, and the member starts once more. Every offset is read, the member counted the outage in `broker.errors` and joined only at its two starts, and `start:` resolved once per partition only (`offsets.reset{reason="no_commit"}` 3), so the second life resumed from the commit that outlived both restarts. Nothing is read twice unless the member counted a rejoin or a deferred batch, and then at most a commit interval |
| `consume-tls`, `consume-mtls`, `consume-sasl-plain`, `consume-sasl-scram512` | `logit-in-auth-*.yaml` | the `tls`, `mtls`, `sasl-plain`, and `sasl-scram512` topics read over the listener and credentials their producing sink used, each record once |
| `consume-oauth-cc` | `logit-in-oauth-*.yaml` | the `oauth-cc` topic read through the client-credentials grant for the whole run, past the first token's life, with at least two tokens fetched for the member's own `scope` |
| `consume-read-committed` | `logit-in-txn-*.yaml`, `Txn.java` | a transactional Java producer writes 10 transactions of 10 records, committing the even ones and aborting the odd ones. Under `read_committed`, `kafka_in` reads the 50 committed records once each and counts the 50 aborted ones `records.skipped{reason="aborted"}`; under `read_uncommitted` it reads all 100 |

The relay legs don't read each topic's stored codec: the `codec-<c>` legs show that `kafka_out`
stores each codec, so the relay rows show that `kafka_in` reads it.

`Txn.java` runs on the broker image's own JRE and client libraries, compiled first in
`eclipse-temurin:21.0.10_7-jdk`, pinned by digest: the broker image's runtime is a JRE without
`jdk.compiler`, so `java Txn.java` can't run there.

The SASL user is `logit` with the password `logit-secret` on Kafka, whose PLAIN entry is a JAAS
`user_logit` in `kafka-server.properties` and whose SCRAM credentials `kafka-storage.sh format
--add-scram` writes. Redpanda keeps one credential per user, so the script creates `logit256`
(SCRAM-SHA-256) and `logit512` (SCRAM-SHA-512) with `rpk security user create`; its PLAIN
login checks the SCRAM credential, so the `sasl-plain` sink signs in as `logit512`. The script sets
Redpanda's `sasl_mechanisms` to `[SCRAM, PLAIN]` after start. The passwords are literals in the leg
configs, like the run's other throwaway material.

Every token from `oauth` names the subject `logit`, the audience `kafka-interop`, and the issuer
`http://oauth/default`, and lives 45 s; it takes any client secret, so there's no leg for a
wrong one. Kafka validates tokens with its own `OAuthBearerValidatorCallbackHandler` against
`oauth`'s JWKS, with the issuer and audience set in `kafka-server.properties`, and a
`connections.max.reauth.ms` of 60 s, so each session ends when its token expires. Redpanda
validates them through its `oidc_*` cluster properties, which the script sets after start, with
a `kafka_sasl_max_reauth_ms` of 40 s, so each session ends before its token expires. Redpanda
has one such bound for every listener, so its SCRAM and PLAIN sinks redial every 40 s too. Each
`oauth_cc` sink asks for a `scope` that names its broker, and `oauth` logs every token request's
body at `DEBUG`, so `check.py` counts each sink's tokens in `logs/oauth.log`.
OAUTHBEARER is a Redpanda Enterprise feature, which a new cluster's 30-day trial license covers;
if Redpanda refuses the settings, `logs/redpanda-oidc-setup.log` says why and its four
OAUTHBEARER rows read `GAP`. The OAUTHBEARER legs run in `oauth`'s network namespace, so their
token endpoint is `http://127.0.0.1`, which `token_endpoint` allows over plain HTTP.

Redpanda v26.2.3 differs from Kafka in three ways the legs work around or allow for:

- It fetches the JWKS from the discovery document's `jwks_uri` without its port, so `oauth`
  listens on port 80.
- It reads everything between `auth=Bearer ` and the closing `\x01\x01` as the token, so a SASL
  extension corrupts the token and Redpanda drops the connection. Its `oauth_static` sink sends
  none ([`docs/known-gaps/kafka.md`](../../docs/known-gaps/kafka.md)).
- It refuses a token with `SASL_AUTHENTICATION_FAILED` at once, without RFC 7628's error
  challenge, so its refusal carries no OAuth status.

Every config passes `logit validate`: `script/validate` and the
`every_shipped_config_loads_and_validates` test both cover `logit-*.yaml` here. Each sink has
its own `generate_in`, so a sink that stalls, as `mtls_nocert` does by design, can't hold back
another sink in the same process. The exception is `logit-otlp-*.yaml`: its five sinks share one
`generate_in` and `lua` source, so every sink sees the same events, and each sets `buffer:
{ overflow: drop_oldest }` so a slow sink drops its oldest batches instead of stalling the others.
At 20 events a second nothing drops, and a drop would show as a missing item.

## A run

1. Builds `logit:kafka-interop` from `Dockerfile`, and `logit-soak-netem:local` from
   `tools/soak/netem/` (set `KAFKA_INTEROP_SKIP_IMAGE=1` to reuse both), and validates every leg
   config with it and `collector.yaml` with the Collector's `validate`.
2. Generates a private CA, a broker certificate for the names `kafka` and `redpanda`, and a
   client certificate into the run directory's `tls/`, with `openssl` in the Kafka image. The
   keys are EC P-256 in PKCS#8, which Kafka's PEM keystore requires.
3. Starts `oauth` and waits for its JWKS, then starts both brokers with
   `docker compose up --wait`, turns on Redpanda's OAUTHBEARER, creates every topic on both, and
   produces the 50 parity keys and the `consume-console` leg's 50 lines with
   `kafka-console-producer.sh`, and the `consume-read-committed` leg's transactions with
   `Txn.java`. Last, it fetches the fixed token the `oauth_static` sinks send and passes it to
   their legs as `KAFKA_INTEROP_OAUTH_TOKEN`. Topics are created with `rpk topic create`, against
   either broker, one call per partition count.
4. Starts every leg but the late and restart ones, and both Collectors. 5 s later it starts
   the `group-mixed` leg's `kafka-console-consumer.sh` for 40 s. 10 s after the start, for 8 s,
   it drops every packet each broker sends to its `dedup` leg, with `netem-drop.sh` run in the broker's network namespace (a `prio` qdisc, a
   `netem loss 100%` band, and a u32 filter on the leg's address). The broker still receives
   the leg's requests and appends them, so their responses are what's lost, and each sink's
   2 s `timeout` reads that as `Ambiguous`. 20 s after the start it starts each
   `group-join` leg's member `b` and kills each `group-kill` leg's member `b`, and at 25 s it
   reads the `group-mixed` group with `kafka-consumer-groups.sh --describe`.
5. Stops each leg with SIGTERM 75 s after the start. `internal` keeps a `logit` process running
   until a signal, so no leg ends on its own; the stop drains its sinks and writes its last
   telemetry. It then stops each Collector, which flushes its file exporters and its `kafka`
   exporters before the topics are read.
6. Reads every topic on both brokers back with `kafka-get-offsets.sh`,
   `kafka-console-consumer.sh`, and kcat. For each codec topic it also reads the codec each
   stored batch carries: `kafka-dump-log.sh` over Kafka's segment file, and
   `rpk topic consume -f '%a{compression}'` against Redpanda, whose segments aren't in Kafka's
   file format, so rpk reports each record's batch codec as Redpanda serves it on a fetch. Both
   brokers keep the producer's codec (`compression.type: producer`, the default).
7. Reads the consumer topics' end offsets, then runs the broker-restart legs alone. 20 s after
   they start it restarts both brokers with `docker restart`, which keeps each container's log
   directory, waits for both to report healthy, and reads the `restart` topic's end offsets. It
   then polls each member's group with `kafka-consumer-groups.sh --describe` until the group has
   committed past those end offsets on every partition, which only a fetch made after the
   restart can do, for up to 90 s, and stops
   the members, recording their exit codes as `<leg>-life1`. It restarts both brokers again,
   starts the members again, and stops the legs 110 s after they started, or 30 s after the
   second start when that's later.
8. Runs `check.py` with `python3 -I` in a `python:3.12-slim` container. `check.py` reads only the
   run directory.
9. Clears the netem rules and tears the project down (`down -v --remove-orphans`) on every
   exit.

A row is `PASS` (the leg did what the tables above say it checks), `GAP` (the broker differs from Apache Kafka
in a way [`docs/known-gaps/kafka.md`](../../docs/known-gaps/kafka.md) records), or `FAIL`. Only
Redpanda's `timestamp` and OAUTHBEARER rows, `consume-oauth-cc` included, can read `GAP`; Apache Kafka is the reference, so any other outcome
there is a `FAIL`. The script exits 1 on any `FAIL`.

Each leg fails when it can't prove what it checks:

- A `codec-<c>` row fails if no stored batch codec was read, or if any stored batch carries a
  codec other than `<c>`. Both consumers decode any codec, so the read-back alone can't show
  that `compression:` reached the wire.
- The `dedup` row fails if the idempotent sink made no `Ambiguous` attempt, or if the control
  stored no duplicate or lost an event. Without an `Ambiguous` attempt nothing was resent; without
  the control's duplicate, the fault never hit a batch the broker had appended, so the
  idempotent sink's resends never reached the broker's duplicate check.

A run writes `perf/results/kafka-interop/<timestamp>/` (gitignored, or under
`KAFKA_INTEROP_OUT`):

| File | Contents |
|---|---|
| `results.md`, `results.json` | one row per leg and broker |
| `readback/<broker>/<topic>.java.tsv` | `kafka-console-consumer.sh` with the timestamp, partition, offset, and key printed (no value for the OTLP topics, whose bytes aren't text) |
| `readback/<broker>/<topic>.kcat.tsv` | kcat's `%p\t%o\t%T\t%k\t%s` (`%S`, the value's size, for the OTLP topics) |
| `readback/<broker>/<topic>.offsets` | `kafka-get-offsets.sh`, one `topic:partition:offset` per partition |
| `collector/<broker>/<name>.jsonl` | what each Collector pipeline decoded, one OTLP/JSON request per line |
| `readback/<broker>/codec-<c>.batches` | each codec topic's stored codec: `kafka-dump-log.sh` output for Kafka, one `offset: <o> compression: <c>` line per record for Redpanda |
| `<leg>-<broker>-telemetry.log` | each leg's `internal` telemetry, `stdio_out` `format: json`; a consumer leg's is `in-<leg>-<broker>-telemetry.log` |
| `consume-<leg>-<broker>.jsonl` | what each consumer leg's `kafka_in` read, `stdio_out` `format: json`, carriers included; `relay-<broker>-sent.jsonl` and `relay-<broker>-text-sent.jsonl` hold what the relay leg's generators wrote |
| `readback/<broker>/consume.offsets`, `restart.offsets`, `restart-after.offsets` | `kafka-get-offsets.sh` for the topics the consumer legs read, and for `restart` once both brokers are healthy after the first restart |
| `readback/<broker>/restart-committed.txt` | the `consume-restart` group's last `kafka-consumer-groups.sh --describe` before the script stopped its first life |
| `readback/<broker>/group-mixed-console.tsv`, `group-mixed.describe` | the `group-mixed` leg's console consumer's records, and `kafka-consumer-groups.sh --describe --state` and `--members` |
| `redpanda-oidc.txt` | `ok` once Redpanda took the OAUTHBEARER settings, else `failed` |
| `exits.txt`, `timeline.log`, `netem.log` | each leg's exit code, the run's phases, and the qdiscs and filters each netem step set |
| `provenance.txt`, `logs/<service>.log`, `tls/` | image digests and versions, every service's log, and the run's certificates |

## Recorded runs

### 2026-10-30: the consumer legs

Against `apache/kafka:4.2.0` and `redpandadata/redpanda:v26.2.3`
(`perf/results/kafka-interop/20261031T041411Z/`), from `kin/w6a` with `kin/w6b` merged: 133 of 134 rows
`PASS`, the one `GAP` Redpanda's `timestamp` row the run below records, no `FAIL`, and no
`kafka_in` bug. The consumer legs' rows:

| Leg | Apache Kafka 4.2.0 | Redpanda v26.2.3 |
|---|---|---|
| `consume-console` | `PASS`: 50 lines on 3 partitions, each once, with its key, its headers (`rep`'s two values as an array in order, `nil` null), and the record timestamp as the event's | `PASS`, the same |
| `consume-relay-<format>-<codec>`, 30 rows | `PASS` each: `json`, `msgpack`, and `native`, 120 events under `record: event` (120 records) and `batch` (4), each equal to what was sent; `text`, 40 bodies under each mode, each at its record's timestamp; `otlp_proto` and `otlp_json`, 40 logs, 200 points, and 40 spans, equal but for the OTLP normalizations | `PASS` each, the same |
| `consume-collector-proto`, `consume-collector-json` | `PASS` each: 160 logs, 160 spans, and 800 data points, none read twice, every field as sent | `PASS`, the same |
| `consume-group-join` | `PASS`: 400 records read by `a` 328 and `b` 72, none twice, three partitions moving once | `PASS`: `a` 320, `b` 80, none twice |
| `consume-group-kill` | `PASS`: 19 offsets read twice, inside the 60 a commit interval allows; three partitions changed member twice, to `b` and back | `PASS`: 12 offsets read twice; every partition changed member once |
| `consume-group-mixed` | `PASS`: two members under `range` while both were in; `kafka_in` 245 and the console consumer 155 records, none twice | `PASS`: `kafka_in` 230, the console consumer 170 |
| `consume-broker-restart` | `PASS`: 300 records, none twice, joins only the two `start`s, `offsets.reset{reason="no_commit"}` 3, no deferred batch; the first life committed 69, 49, and 57 on partitions 0 to 2 before its stop, past the 60, 45, and 45 they ended at once both brokers were healthy again; 10 broker errors and no failed commit | `PASS`: the same, committing 69, 49, and 57 past 62, 45, and 48, with 3 broker errors |
| `consume-tls`, `consume-mtls`, `consume-sasl-plain`, `consume-sasl-scram512` | `PASS` each: 50 records, each once | `PASS` each, the same |
| `consume-oauth-cc` | `PASS`: 60 records over 75 s, 3 tokens fetched | `PASS`, the same |
| `consume-read-committed` | `PASS`: 50 records under `read_committed`, 50 counted `aborted`; 100 under `read_uncommitted` | `PASS`, the same |

What the run showed:

- **The rows can fail.** Changing one attribute in a relay leg's output, one header, or one
  transaction's record, or copying one record into the other member's output, turns the matching
  row `FAIL`, checked offline by rerunning `check.py` over an edited copy of the run directory.
- **A broker restart doesn't cost a member its group.** On both brokers the member read through
  the first restart with no rejoin: its only joins were the two `start` ones, one per process
  life. The restart took 10 s, less than the 45 s session, and within 8 s of both brokers being
  healthy again each group had committed past the end offsets the topic had then, on every
  partition, so the first life fetched after the restart in the group it already had. The second life, started
  after a second restart, read each partition on from the commit its shutdown made, with nothing
  read twice.
- **Java's console consumer and `kafka_in` agree on `range`.** Each broker reported the strategy
  `range` with two members, three partitions each, and the partitions moved to the console
  consumer at its join and back at its leave with nothing read twice, so each side's revoke
  committed before the rejoin.
- **Redpanda agrees with Kafka on every consumer leg**, transactions included: under
  `read_committed`, both name the aborted transactions in each Fetch answer, and `kafka_in`
  skipped and counted their 50 records.

### 2026-10-11: the core legs

Against `apache/kafka:4.2.0` and `redpandadata/redpanda:v26.2.3`
(`perf/results/kafka-interop/20261011T212530Z/`), before the SASL and OTLP legs existed: 21 of
22 rows `PASS`, one `GAP`, no `FAIL`, and no `kafka_out` bug.

| Leg | Apache Kafka 4.2.0 | Redpanda v26.2.3 |
|---|---|---|
| Partition parity | `PASS`: all 50 keys on the console producer's partition, 6 of 6 partitions used | `PASS`, the same |
| Codecs `none`, `gzip`, `snappy`, `lz4`, `zstd` | `PASS` each: 100 records, each event once, in both the Java consumer and kcat; `kafka-dump-log.sh` shows all 10 stored batches under the named codec | `PASS` each: the same read-back; rpk shows all 100 records' batches under the named codec |
| Idempotent dedup | `PASS`: 800 records for 800 events after one `Ambiguous` attempt; the non-idempotent control stored 10 duplicates | `PASS`, the same |
| TLS, mutual TLS | `PASS`: 50 records each; with no client certificate, 0 records and only `clean` attempts | `PASS`, the same |
| Timestamps | `PASS`: 90 of 100 records landed, the 10 stamped at 2100-01-01 dropped and counted as `invalid_timestamp` | `GAP`: 0 landed; Redpanda answers `INVALID_TIMESTAMP` naming no record, so each batch of 10 was rejected whole, counted as `rejected` |

What the run showed:

- **The codec rows read what the broker stored.** Forcing one leg's sink to `compression: none`
  turns its `codec-zstd` row `FAIL` on both brokers.
- **The dedup leg isn't vacuous.** During the 8 s drop the broker appended a batch whose response
  never came. Both sinks timed out once (`Ambiguous`) and resent after the drop cleared. The
  idempotent sink's resend carried its first attempt's sequence, and the broker discarded it; the
  control sink's resend was appended again. With the netem drop skipped (`NETEM_FOR=0`), both
  brokers' rows `FAIL`.
- **Redpanda agrees with Kafka on every leg but one.** It places keyed records where the Java
  partitioner does, takes all five codecs, deduplicates an idempotent resend, and enforces mutual
  TLS. Like Kafka 4.x, it refuses a record more than an hour ahead
  (`message.timestamp.after.max.ms`, 3,600,000 by default), but it fills no `record_errors`, so
  `kafka_out` rejects the whole partition batch
  ([`docs/known-gaps/kafka.md`](../../docs/known-gaps/kafka.md)).

The `kafka-broker-restart` soak scenario's run is in
[`docs/plans/soak-harness.md`](../../docs/plans/soak-harness.md)'s "Findings".

## Cleanup and a shared daemon

Everything the script creates belongs to the `kafka-interop` project, apart from the one-shot
netem and kcat containers, which remove themselves. The project name is fixed, so one run at a
time per daemon: the script refuses to start while the project has containers, and prints the
`down` command for a stack a crashed run left behind.

Don't edit `script/kafka-interop` while it runs. Bash reads a script as it executes, so an edit
can break the run partway through; the cleanup trap still tears the stack down.
