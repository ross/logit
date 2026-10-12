# kafka-interop

`script/kafka-interop` checks `kafka_out` against a real Apache Kafka and a real Redpanda. It runs
the legs in [`docs/plans/kafka-output.md`](../../docs/plans/kafka-output.md)'s "Interop plan
(W4)" against each broker, reads every topic back with the Java client and kcat, and prints one
row per leg and broker. The plan's "Findings" section records what a run showed.

The script runs on the host and drives docker (`$DOCKER`, `sudo docker` by default), like
`script/victoria-interop` and `script/splunk-interop`. It isn't part of `script/cibuild`, and no
test depends on it running. A run takes about four minutes once the images are built.

## What it runs

`compose.yaml` starts one stack under the compose project `kafka-interop`, on the network
`kafka-interop-net`, with no host ports:

| Service | Image | Role |
|---|---|---|
| `kafka` | `apache/kafka:4.2.0` | single-node KRaft broker, started on `kafka-server.properties` |
| `redpanda` | `redpandadata/redpanda:v26.2.3` | single-node Redpanda in `dev-container` mode, started on `redpanda.yaml` |
| `logit-<leg>-<broker>` | `logit:kafka-interop`, built from the current tree | one per `logit-<leg>-<broker>.yaml` |

Each broker has five client listeners: plain on `:9092`, TLS on `:9093`, mutual TLS, which
requires a client certificate, on `:9094`, SASL_PLAINTEXT on `:9096`, and SASL_SSL on `:9097`.
The SASL listeners offer PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512. Only they require
authentication, so every read-back uses the plain listener and needs no credentials. The read-back runs `kafka-console-consumer.sh` and
`kafka-get-offsets.sh` inside the `kafka` container, against either broker, and kcat in
`edenhill/kcat:1.7.1` containers on the stack's network.

The legs, each run against both brokers:

| Leg | Config | What it checks |
|---|---|---|
| `parity` | `logit-parity-*.yaml` | 200 events keyed by 50 `user` values land on the same partitions of a 6-partition topic as the same keys from `kafka-console-producer.sh` |
| `codec-<c>` | `logit-codecs-*.yaml` | 100 events under each `compression` (`none`, `gzip`, `snappy`, `lz4`, `zstd`) are stored under that codec, and decode in both the Java consumer and kcat, each event once |
| `dedup` | `logit-dedup-*.yaml` | while the broker's packets to this leg are dropped, an idempotent sink's resend after an `Ambiguous` attempt stores no duplicate, and a non-idempotent control sink beside it stores at least one |
| `tls`, `mtls`, `mtls-required` | `logit-tls-*.yaml` | 50 events over server TLS and over mutual TLS from the run's private CA; a sink with no client certificate stores nothing |
| `sasl-plain`, `sasl-scram512`, `sasl-scram256`, `sasl-wrong` | `logit-sasl-*.yaml` | 50 events through `sasl:` with PLAIN over SASL_SSL, SCRAM-SHA-512 over SASL_SSL, and SCRAM-SHA-256 over SASL_PLAINTEXT; a SCRAM-SHA-512 sink with the wrong password stores nothing, counts every attempt as `refused`, holds its queue with `logit.component.retrying` at `1`, and drops no batch as `rejected` |
| `timestamp` | `logit-timestamp-*.yaml` | in each batch of 10, the event stamped at 2100-01-01 is dropped and counted as `invalid_timestamp`, and the other nine land |

The SASL user is `logit` with the password `logit-secret` on Kafka, whose PLAIN entry is a JAAS
`user_logit` in `kafka-server.properties` and whose SCRAM credentials `kafka-storage.sh format
--add-scram` writes. Redpanda keeps one credential per user, so the script creates `logit256`
(SCRAM-SHA-256) and `logit512` (SCRAM-SHA-512) with `rpk security user create`; its PLAIN
login checks the SCRAM credential, so the `sasl-plain` sink signs in as `logit512`. The script sets
Redpanda's `sasl_mechanisms` to `[SCRAM, PLAIN]` after start. The passwords are literals in the leg
configs, like the run's other throwaway material.

Every config passes `logit validate`: `script/validate` and the
`every_shipped_config_loads_and_validates` test both cover `logit-*.yaml` here. Each sink has
its own `generate_in`, so a sink that stalls, as `mtls_nocert` does by design, can't hold back
another sink in the same process.

## A run

1. Builds `logit:kafka-interop` from `Dockerfile`, and `logit-soak-netem:local` from
   `tools/soak/netem/` (set `KAFKA_INTEROP_SKIP_IMAGE=1` to reuse both), and validates every leg
   config with it.
2. Generates a private CA, a broker certificate for the names `kafka` and `redpanda`, and a
   client certificate into the run directory's `tls/`, with `openssl` in the Kafka image. The
   keys are EC P-256 in PKCS#8, which Kafka's PEM keystore requires.
3. Starts both brokers with `docker compose up --wait`, creates every topic on both, and produces
   the 50 parity keys with `kafka-console-producer.sh`.
4. Starts every leg. 10 s later, for 8 s, it drops every packet each broker sends to its `dedup`
   leg, with `netem-drop.sh` run in the broker's network namespace (a `prio` qdisc, a
   `netem loss 100%` band, and a u32 filter on the leg's address). The broker still receives
   the leg's requests and appends them, so their responses are what's lost, and each sink's
   2 s `timeout` reads that as `Ambiguous`.
5. Stops each leg with SIGTERM 75 s after the start. `internal` keeps a `logit` process running
   until a signal, so no leg ends on its own; the stop drains its sinks and writes its last
   telemetry.
6. Reads every topic on both brokers back with `kafka-get-offsets.sh`,
   `kafka-console-consumer.sh`, and kcat. For each codec topic it also reads the codec each
   stored batch carries: `kafka-dump-log.sh` over Kafka's segment file, and
   `rpk topic consume -f '%a{compression}'` against Redpanda, whose segments aren't in Kafka's
   file format, so rpk reports each record's batch codec as Redpanda serves it on a fetch. Both
   brokers keep the producer's codec (`compression.type: producer`, the default). It then runs
   `check.py` with `python3 -I` in a `python:3.12-slim` container. `check.py` reads only the
   run directory.
7. Clears the netem rules and tears the project down (`down -v --remove-orphans`) on every
   exit.

A row is `PASS` (the leg did what the plan expects), `GAP` (the broker differs from Apache Kafka
in a way [`docs/known-gaps/kafka.md`](../../docs/known-gaps/kafka.md) records), or `FAIL`. Only
Redpanda's `timestamp` row can read `GAP`; Apache Kafka is the reference, so any other outcome
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
| `readback/<broker>/<topic>.java.tsv` | `kafka-console-consumer.sh` with the timestamp, partition, offset, and key printed |
| `readback/<broker>/<topic>.kcat.tsv` | kcat's `%p\t%o\t%T\t%k\t%s` |
| `readback/<broker>/<topic>.offsets` | `kafka-get-offsets.sh`, one `topic:partition:offset` per partition |
| `readback/<broker>/codec-<c>.batches` | each codec topic's stored codec: `kafka-dump-log.sh` output for Kafka, one `offset: <o> compression: <c>` line per record for Redpanda |
| `<leg>-<broker>-telemetry.log` | each leg's `internal` telemetry, `stdio_out` `format: json` |
| `exits.txt`, `timeline.log`, `netem.log` | each leg's exit code, the run's phases, and the qdiscs and filters each netem step set |
| `provenance.txt`, `logs/<service>.log`, `tls/` | image digests and versions, every service's log, and the run's certificates |

## Cleanup and a shared daemon

Everything the script creates belongs to the `kafka-interop` project, apart from the one-shot
netem and kcat containers, which remove themselves. The project name is fixed, so one run at a
time per daemon: the script refuses to start while the project has containers, and prints the
`down` command for a stack a crashed run left behind.

Don't edit `script/kafka-interop` while it runs. Bash reads a script as it executes, so an edit
can break the run partway through; the cleanup trap still tears the stack down.
