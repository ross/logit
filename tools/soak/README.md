# soak

`script/soak` runs real `logit` containers for minutes or hours under network and lifecycle
faults on a timeline, and reports crashes, restarts, error lines, hangs, memory and file-descriptor
growth, and uncounted data loss as one `PASS`, `WARN`, `FAIL`, or `SKIP` row per check. A check
reports `SKIP` only when it has nothing it can judge, such as a run with no telemetry, or SUT
telemetry whose process lives disagree with the timeline.
[ADR `soak-harness`](../../docs/adr/soak-harness.md) records why it's built this way, and
[`docs/plans/soak-harness.md`](../../docs/plans/soak-harness.md) is the design: the scenario
schema, the driver loop, every check, and a "Findings" section with what runs showed.

The script runs on the host and drives docker (`$DOCKER`, `sudo docker` by default), like
`script/victoria-interop` and `script/shape-survey`. It isn't part of `script/cibuild`, and no
test depends on it running. It needs Python 3.11 or later on the host (`tomllib`); the driver is
standard library only.

```sh
script/soak list                                 # the shipped scenarios
script/soak self-test                            # the driver's pure parts; no docker
script/soak run statsd-vm                        # the scenario's own duration (16 minutes)
script/soak run statsd-vm --duration 5m --keep   # shorter, and leave the stack up afterward
script/soak run random-faults --duration 8h      # a seeded random schedule; see "Long runs"
script/soak run random-faults --seed 7           # the same scenario, another schedule
script/soak run statsd-datadog                   # an external target; needs a key, see "External targets"
script/soak run kafka-broker-restart             # a Kafka target; see "Kafka targets"
script/soak check perf/results/soak/<stamp>      # re-score a run offline
```

## What it runs

A scenario is a directory under `scenarios/`: a `scenario.toml` timeline beside one ordinary
`logit` config per `logit` service. `compose.yaml` starts three services under the compose
project `soak-<scenario>`, or the two `logit` services alone for a scenario with an external
target (see [External targets](#external-targets)), or the two with a Kafka broker in place of
VictoriaMetrics for a Kafka target (see [Kafka targets](#kafka-targets)):

| Service | Image | `soak.role` |
|---|---|---|
| `victoria-metrics` | `victoriametrics/victoria-metrics:v1.152.0` | `backend`: `-retentionPeriod=100y`, published on an ephemeral loopback port the driver queries; in the compose profile `local`, which the driver enables for a local target only |
| `logit` | `logit:soak`, built from the current tree | `sut`: the system under test (SUT), with the scenario's `configs.sut` |
| `generator` | `logit:soak` | `generator`: a load source, a second `logit` with the scenario's `configs.generator`. An override can add more, each with the config `[configs]` names under its service name and its own `[[ledger.source]]` |
| `kafka` | `apache/kafka:4.2.0` | `broker`: a single-node KRaft broker, one partition per topic, for a Kafka target only (the compose profile `kafka`); the driver reads its topic back through `docker exec` |

The driver and the checks know a service by its `soak.role` label, never its name. A scenario
can add services with `compose = "compose.override.yaml"`, a file beside its `scenario.toml`
that compose merges onto `compose.yaml` (`-f compose.yaml -f <override>`). Each service it adds
needs a `soak.role` label, and a backend needs `profiles: [local]`. The two-sink scenarios add
a second VictoriaMetrics this way:

```yaml
services:
  victoria-metrics-b:
    extends: { file: compose.yaml, service: victoria-metrics }   # resolved from tools/soak/
    profiles: [local]
    labels: { soak.role: backend }
```

`extends.file` resolves against the project directory, which is `tools/soak/`, not against the
override's own directory. Before it starts anything, the driver reads the merged services from
`docker compose config` and checks every step, `[[expect]]`, and sink against them. `list` and
the self-test can't run compose, so they check a scenario with no override against the base
roles only.

Both `logit` services run `logit --log-format json run`, so stdout carries the `internal`
telemetry as NDJSON and stderr the JSON self-log. Their configs pass `logit validate`:
`script/validate`, the `every_shipped_config_loads_and_validates` test, and every `run` cover
`scenarios/*/logit-*.yaml`.

`statsd-vm` sends 2,000 statsd lines a second, one per UDP datagram, through `aggregate` into
remote-write 1.0 (zstd) to VictoriaMetrics. Its 13-minute cycle delays and drops the SUT's
remote-write, drops the generator's UDP, stops VictoriaMetrics, and pauses, stops, and
partitions the SUT.

`statsd-vm-lost-total` is a 5-minute negative control for the ledger: the SUT stops while
VictoriaMetrics is down, with a 1 s sink `shutdown_grace`, so its first life's last totals drop at
shutdown. A correct ledger reports that life's `ledger.egress` gap as counted, never as a
`FAIL`.

Four scenarios of about 5 minutes each run one fault and assert its effect with `[[expect]]`
tables (see [Expectations](#expectations)):

| Scenario | Fault | What its expectations assert |
|---|---|---|
| `sink-outage-block` | VictoriaMetrics stopped 90 s; the sink's queue is 2 batches under `overflow: block` | the sink's queue fills, backpressure reaches the listener, whose receive queue drops the oldest datagrams and counts them while the kernel and the sink drop nothing, and the sink drains after |
| `sink-outage-drop-oldest` | VictoriaMetrics stopped 90 s; the sink's queue is 2 batches under `overflow: drop_oldest` | the sink evicts and counts batches while `aggregate` never blocks, the listener drops nothing, and its receive queue stays under 5% full, and the sink drains after |
| `udp-flood-sink-stop` | 20,000 lines/s; VictoriaMetrics stopped 60 s; a 2-batch blocking sink queue, the receive queue at its default | the stop backs up to the listener, which evicts and counts the oldest datagrams and reads datagrams in every drain of the stop, at 90% of the generator's rate, and the kernel drops nothing, including in the drain where the chain unblocks |
| `udp-flood-sink-stop-block` | as `udp-flood-sink-stop`, with `receive: {overflow: block, max_datagrams: 10000}` | the stop backs up to the listener, the kernel drops, `logit` evicts nothing, and every queue drains after |

`spool-kill-replay` (6 minutes 15 seconds) is `statsd-vm` with its sink on a disk spool
(`buffer: {disk: {path: /tmp/soak-spool}}`). VictoriaMetrics stops for 150 s, and 60 s into that
the SUT is killed with SIGKILL and started again 30 s later, while VictoriaMetrics is still down.
Its expectations assert that the spool queues windows during the outage, the restarted process
replays the killed one's records once and never again, the sink drops nothing, and it stops
retrying after the outage, and no torn tail is truncated at the restart. It sets
`[ledger] vm_every_window`, so `ledger.windows` checks that every spooled window reached
VictoriaMetrics, and `ledger.replay` ties the restarted process's replay to what the killed one
had queued. The spool lives in the container's own filesystem, which a `kill` and `start`
keep, because the driver never recreates a container.

`random-faults` (1 hour by default, meant for `--duration 8h`) is `statsd-vm`'s stream with
`spool-kill-replay`'s disk-spooled sink and `vm_every_window`, under a seeded random schedule
(see [Random schedules](#random-schedules)): netem delay, jitter, loss, and rate limits on
`logit` and on `generator`; `stop`, `pause`, `kill`, and `partition` of `logit`; `stop` and
`partition` of VictoriaMetrics; and `partition` of the generator. Each fault lasts 20 s to 3
minutes, one at a time, with 2 to 4 minutes of quiet between them. Over 8 hours that is about 113
faults, and about half the run is judged steady state.

Three scenarios put a `lua` stage between the listener and `aggregate`, with
`[ledger] sut_script` naming it, so `ledger.script` balances the script's hop:

| Scenario | Length | What it drives |
|---|---|---|
| `lua-path` | 16 minutes | `statsd-vm`'s schedule through a script that rewrites every event, drops every 53rd, fails on every 97th, and returns one rollup event from `flush()` each window; its expectations assert all three outcomes |
| `lua-stall` | 8 minutes | a busy loop of about 30 s on the 360,000th event: the node reads stalled, `/readyz` answers `503 stalled`, the receive queue fills and evicts, and the script resumes on its own. A `mark` with `ready = "stalled"` covers it |
| `lua-memory` | 8 minutes 10 seconds | from the 480,000th event the script keeps 1 KB per event until the VM passes `max_memory: 64MiB`: the SUT logs `memory_limit_exceeded`, drains, and exits 2 inside an `exit` step, whose revert starts it again. The run ends before the second life reaches that event |

Three scenarios give the SUT two sinks of one kind, each to its own VictoriaMetrics, with one
`[[ledger.sink]]` table per sink:

| Scenario | Length | What it drives |
|---|---|---|
| `fanout-copy` | 12 minutes 30 seconds | `aggregate` fans out to two sinks, each holding a full copy (`split = "copy"`): the second backend stopped 90 s, netem loss on the first, then both stopped, overlapping. Its expectations assert that the first sink keeps delivering while the second backend is down, the second queues, and neither drops a batch |
| `fanout-block` | 5 minutes 30 seconds | as `fanout-copy` with 500 ms windows, and the second sink's queue at 2 batches under `overflow: block`, through a 90 s stop of its backend. A fan-out offers each batch to its consumers in turn through one send, so once that sink's queue and inbox fill, `aggregate` waits on it and the healthy sink stops delivering too. A `mark` names the blocked span; the expectations assert the stall, the backpressure reaching the listener, and both sinks recovering |
| `route-split` | 9 minutes | the generator tags each line `shard:0` or `shard:1` over DogStatsD, and a `route` on `shard` fills two targets, each feeding one sink (`split = "route"`): one backend stopped 90 s, then netem loss on the other. Each backend holds one shard's series, so the egress gap is judged on their sum |

`fanin` (16 minutes 30 seconds) runs two generators of different kinds into one `aggregate` and
one sink, each with its own `[[ledger.source]]`. `generator` sends `statsd-vm`'s 2,000 statsd
lines a second into `statsd_in`. `generator-lines`, which the scenario's override adds, sends 2,000
syslog lines a second over UDP into `lines_in`, one per datagram; `kv`, `keep`, `kv_metrics`, and
`keep_signals` turn each into one `soak_lines` increment beside statsd's series. Its 13.5-minute
cycle puts netem loss, a partition, and a stop on each generator in turn, and its expectations
assert that a stopped generator's listener reads nothing while the other keeps reading.

`cardinality` (75 minutes) adds a second `generate_in` to `statsd-vm`'s generator that sends 200
metric names a second, none of them repeating, through the same statsd sink. The interner never
frees, so both processes intern every name for the whole run; `aggregate` forgets each name
three windows later, so its state stays flat. `[expected]` sets `rss_slope` to FAIL on `logit`
and on `generator`, and its expectations assert each process's interner slope (720,000 strings
an hour, within 5%), that `aggregate` evicts each name, and that it never reaches its series
cap. A netem delay on `logit` and a VictoriaMetrics stop give `recovery` faults to judge. Run it
at its own duration: under an hour, `rss_slope` reports growth as WARN, and the expected FAIL
FAILs the row.

Four scenarios of 6 to 8 minutes each run one fault of the sink's delivery path and assert its
effect with `[[expect]]` tables:

| Scenario | Length | What it drives |
|---|---|---|
| `spool-full` | 7 minutes 15 seconds | `spool-kill-replay`'s disk-spooled sink with `max_bytes: 128KiB` under `overflow: drop_oldest`, through a 200 s VictoriaMetrics stop. The spool rejects each window that arrives while the sink holds its head and counts it `overflow_newest`, never `disk_full` or `rejected`, and drains after |
| `spool-enospc` | 7 minutes 15 seconds | the same stop against a spool on a 128 KiB tmpfs that a compose override mounts at the spool path, below the spool's own 4 MiB bound. A write fails with `ENOSPC` and the sink counts `disk_full`, never `overflow_oldest`. No step stops or kills the SUT, because Docker discards a tmpfs when its container stops |
| `slow-destination` | 7 minutes 45 seconds | the sink's `timeout` is 2 s. A 3 s netem delay on VictoriaMetrics makes it apply each write whose response arrives late, then a 2 kbit rate limit on the SUT's egress makes a request unable to finish. Each counts `network_error` and a retry, and nothing drops |
| `fanout-stop-outage` | 6 minutes 15 seconds | `fanout-copy`'s two sinks, with the second's queue at 4 batches under `drop_oldest`. VictoriaMetrics B is stopped 150 s and the SUT is stopped for 30 s, 60 s in, so the second sink's queued batches drop at shutdown and are counted while the first sink drops nothing |

The stall and the memory cap depend on the host's speed: `lua-stall`'s loop count and
`lua-memory`'s event count are calibrated on the development host, and the comments in their
`logit-sut.yaml` give the timings. On a much slower or faster host the event falls outside its
step's window, and `ready` or `exit` FAILs.

`statsd-datadog` (14 minutes) sends `statsd-vm`'s stream, renamed `logit.soak.requests_0` to
`logit.soak.requests_99`, through a 10 s delta `aggregate` into `datadog_out` with a disk spool,
straight to Datadog's intake. Its 12-minute cycle delays (300 ms ± 100 ms) and drops (20%) the
SUT's egress, partitions it for 60 s, pauses it for 30 s, and kills it for 30 s. Its
expectations assert that the sink drops no batch through any fault, stops retrying after the
partition, drops nothing as stale through the partition and the kill, and that the restarted
process truncates no torn spool tail. It's an external target, so it needs a Datadog API key;
see [External targets](#external-targets).

`kafka-broker-restart` (8 minutes 30 seconds) sends `statsd-vm`'s stream through a 2 s delta
`aggregate` into `kafka_out` (JSON records, idempotent, a 10 s `timeout`) to the stack's broker,
topic `soak`. Its 6.5-minute cycle restarts the broker, pauses it for 40 s, and stops it for
60 s. Its expectations assert that the sink drops no batch through any fault, the pause leaves at
least one attempt `Ambiguous`, so the broker has resends to deduplicate, and the sink retries
during the stop and stops retrying after it. The topic is read back at the end and judged by
`ledger.egress` and `ledger.topic`, which FAILs a record stored twice; see
[Kafka targets](#kafka-targets).

### Faults

| Action | What it does | Revert |
|---|---|---|
| `netem` | a one-shot `logit-soak-netem:local` container in the target's network namespace sets a root `tc netem` qdisc with the step's `args` | the same container clears it |
| `pause` | `docker pause` | `docker unpause` |
| `stop` | `docker stop -t 30` | `docker start` |
| `kill` | `docker kill -s KILL`, on `logit` only: no shutdown signal, so no final telemetry drain, no shutdown drops, and exit code 137. A killed generator's last sends would reach the SUT with no count behind them | `docker start` |
| `restart` | `docker restart -t 30` | none |
| `partition` | `docker network disconnect` from every network | `docker network connect`, passing back each alias recorded before the disconnect |
| `mark` | nothing: it marks a window for an event the SUT's own config causes, such as a stalled script. An optional `ready` names a readiness word the SUT must report inside it | nothing |
| `exit` | nothing: on `logit` only, it names an exit the SUT makes on its own inside the window, with the `code` it leaves and the `key` of the ERROR line that explains it | `docker start` if the container has exited |

A `mark` or `exit` window is a fault window like any other, so the checks excuse what happens
inside it. Neither can go in a `[random]` schedule.

```toml
[[step]]
at = "2m50s"
action = "exit"
on = "logit"
code = 2
key = "memory_limit_exceeded"
for = "1m30s"
```

A root qdisc shapes the target's egress only: `netem` on `logit` impairs its remote-write (or,
for an external target, all its traffic to the internet), not the statsd arriving at it. To
impair the UDP into the SUT, put `netem` on a generator, which impairs that generator's source
alone. The driver
appends `limit 100000` to `args` unless they set a `limit`, so netem's own queue never drops.

## External targets

A scenario whose sink delivers outside the stack, such as to Datadog, declares it with a
`[target]` table:

```toml
[target]
kind = "external"          # the default, when the table is absent, is "victoria-metrics"
name = "datadog"           # a label for results; nothing queries it
env = ["DD_API_KEY"]       # variables the SUT's config reads with !env
```

To run one, put each listed variable in `perf/results/soak-external.env`, then run the
scenario. Replace `DATADOG_API_KEY` with your key:

```sh
echo 'DD_API_KEY=DATADOG_API_KEY' > perf/results/soak-external.env
script/soak run statsd-datadog
```

- `SOAK_EXTERNAL_ENV=<file>` names another file, such as a shell file of dev secrets. The
  script refuses a file git doesn't ignore, asking the repository the file is in, because it
  holds credentials; everything under `perf/results/` is ignored, and a file in no repository
  passes. Any other git failure is refused: a missing `git` by name, and any other failure,
  such as a repository that `safe.directory` rejects, with git's message.
- The file can be plain or shell-sourceable: `NAME=value` or `export NAME=value` lines, `#`
  comment lines, and blank lines. One pair of matching single or double quotes around a value
  is removed, and nothing inside them is expanded. A `#` after a value is part of the value. A
  name set twice takes its last value.
- The script refuses to start when the file doesn't set a listed variable or sets it empty,
  and names the variable, never its value.
- The script also refuses a listed value that contains `$`, a single or double quote, a
  backtick, `\`, `#`, or whitespace, naming the variable and the kind of character. Compose
  reads the private copy below with dotenv rules, so it would expand `$NAME`, cut a value at
  ` #`, parse quotes and `\`, and fail on an unmatched quote with an error that quotes the
  value. A value without those characters reaches the SUT as written.
- Docker never reads your file. The script writes the listed variables as plain `NAME=value`
  lines to a private copy, mode 0600 in a new temporary directory outside the repo, for
  `logit validate`'s `--env-file`, and removes it before the run starts. The driver makes its
  own copy for compose's second `--env-file` and removes it when the run ends. No value goes
  on a command line, and the run directory never holds one. Only a run killed with SIGKILL
  leaves its copy in the temporary directory (`soak-env-*`); SIGINT, SIGTERM, and SIGHUP
  remove it. Each `inspect/<svc>.json` has every
  `Config.Env` value replaced by `<redacted>`, because the SUT's environment carries the key.
- A variable reaches the SUT only when `compose.yaml` passes it in the `logit` service's
  `environment:`. Today that's `DD_API_KEY`, and the scenario check refuses any other name in
  `env`.
- `statsd-datadog` uses Datadog's default site, US1. For an org on another site, set `site:` on
  the `datadog` sink in the scenario's `logit-sut.yaml`.
- Each run writes 100 custom metrics to the org, named `logit.soak.requests_*`, from the host
  `logit-soak`.

What changes for an external target:

- The stack runs without the `local` profile's backends, so a fault on one is refused as an
  unknown service, and a `[[ledger.sink]]` names its `id` alone, with no `backend` or
  `vm_selector`. `netem` and
  `partition` on `logit` shape or cut its egress to the internet; `kill`, `stop`, and `pause` on
  `logit` work as for a local target.
- The driver makes no VictoriaMetrics readiness check, freshness poll, flush, or export. At the
  end it stops every generator, then waits, up to 120 s, for a SUT drain at least two aggregate
  windows later that shows every sink's `buffer.batches` at 0 and `retrying` at 0 or never set,
  before it stops the SUT. `timeline.jsonl` records the wait as a `sink_drained` phase.
- `ledger.egress` and `ledger.windows` SKIP, and `ledger.summary` shows its Ab − V term as SKIP
  and judges the other three. `ledger.wire`, `ledger.intake`, `ledger.edge`,
  `ledger.aggregate`, `ledger.replay`, and `identity.sink` judge the run as they do a local one.
- `ledger.sent` ends the ledger at each sink's own telemetry, and reads a `PASS` whose detail
  starts with `SENT` when every batch each sink received ended in an accepted response. A batch
  left unsent, a drop or refusal the sink logged, or no `2xx` FAILs it; the plan's
  [External targets](../../docs/plans/soak-harness.md#8-external-targets-w5) section lists
  every verdict.

`SENT` is the end of the evidence. A `2xx` means the intake took the request for processing;
nothing queries the destination for what it stored, so the harness can't see a point the intake
accepted and later discarded, and can't tell a resent point it overwrote from one it stored
twice. `datadog_out` resends a whole batch after an ambiguous failure under `at_least_once`; a
resent series point overwrites the stored one, and a resent log is stored again
([`docs/known-gaps/datadog.md`](../../docs/known-gaps/datadog.md)), which is why
`statsd-datadog` sends series only.

## Kafka targets

A scenario whose sink is `kafka_out` declares the stack's own broker as its target:

```toml
[target]
kind = "kafka"
topic = "soak"             # the topic the SUT's kafka_out writes
```

What changes for a Kafka target:

- The stack runs `kafka` in place of `victoria-metrics`, and needs no env file. A fault may name
  `kafka` (`netem`, `pause`, `stop`, `restart`, `partition`); a `restart` is a clean broker
  shutdown and start, which keeps the log and the producer state. A fault on `kafka` is refused
  for any other target, and one on `victoria-metrics` for this one.
- The scenario has one `[[ledger.sink]]`, its `kafka_out`, with no `backend` or `vm_selector`;
  the topic is that sink's V.
- As for an external target, nothing reads VictoriaMetrics, and the end sequence waits for the
  sink's queue to empty before it stops the SUT.
- After the SUT stops, the driver reads the topic in the broker container: each partition's end
  offset (`kafka-offsets.txt`), then that many records from the beginning
  (`kafka-topic.ndjson`). `timeline.jsonl` records a `kafka_read` phase, or `kafka_read_failed`.
- The SUT's `aggregate` must run `temporality: delta`, because V is the sum of every distinct
  record's `sum` values: each record carries one window's increments. Records are deduplicated
  on the value's raw bytes first, so a duplicate can't cancel a loss of the same size. Each
  record, and each duplicate copy, counts toward the SUT life its timestamp falls in.
- `ledger.egress` and `ledger.summary` judge Ab − V against the topic's distinct records. A
  surplus FAILs, as on any target. `ledger.windows` SKIPs.
- `ledger.sent` judges the sink's batches, drops, and refusals as for an external target, with
  `kafka_out`'s request classes (`ok` accepted).
- `ledger.topic` compares the topic's distinct record count with the records the sink saw
  accepted, and judges duplicate copies per SUT life. Within one producer epoch the broker
  discards a resend, so a duplicate FAILs unless its life ended in a restart or kill, or the
  sink counted a drop that bumps the epoch there (`rejected`, `invalid_timestamp`,
  `invalid_record`, `ambiguous_at_most_once`). Those duplicates WARN, as
  [`docs/known-gaps/kafka.md`](../../docs/known-gaps/kafka.md) permits. A run with a killed SUT
  life WARNs, rather than FAILs, on a T − A difference in either direction, because a kill loses
  that life's last telemetry interval and a replayed spool batch counts twice in A.

## A run

1. `script/soak` runs the self-test, builds `logit:soak` from `Dockerfile` and
   `logit-soak-netem:local` from `netem/` (set `SOAK_SKIP_IMAGE=1` to reuse both), and validates
   the scenario's configs in `logit:soak`. `SOAK_SKIP_IMAGE=1` reuses whatever `logit:soak` is on
   the daemon, whichever tree built it; `provenance.txt` records the image id, so check it before
   reading a run as evidence about a changed binary.
2. The driver refuses to start if the compose project already has containers, then brings the
   stack up with `docker compose up --wait`, which waits on each `logit` service's `logit ready`
   health check. For an external target it brings up no backend.
3. On a 1-second tick it applies each fault and its revert on schedule, probes `logit ready`
   after every start, restart, or unpause of a `logit` service, inspects every container every
   5 s, samples `docker stats` and, for a local target, each sink's newest stored sample every
   30 s, and appends each
   container's new log lines every 5 minutes. A `logit`
   container that exits or restarts with no step behind it ends the schedule at once.
4. At the end it reverts active faults, stops every generator, waits until every sink's stored
   total holds across two aggregate windows, stops the SUT (expecting exit 0), and exports each
   sink's series from its backend. For an external target it waits for every SUT sink's queue
   to empty instead, and exports nothing.
5. It collects the rest of each service's stdout and stderr separately with `docker logs`, and
   its final `docker inspect`, then tears the project down (`down -v --remove-orphans`) unless `--keep`.
   This step runs on every exit but SIGKILL: SIGINT (Ctrl-C), SIGTERM, and SIGHUP each stop the
   schedule and reach it, unless the driver started with that signal ignored, as `nohup` leaves
   SIGHUP. After a SIGHUP the driver drops its terminal output and carries on, because the run
   directory has everything.
6. It scores the run. The script exits 1 on any `FAIL`, including a run the driver aborted or
   hit an error in, and 130 when SIGINT, SIGTERM, or SIGHUP interrupted it, whose `run` row
   FAILs too. `check` on that directory exits 1: only the live run knows it was interrupted.

A run writes `perf/results/soak/<UTC stamp>/` (gitignored, or under `SOAK_OUT`):

| File | Contents |
|---|---|
| `results.md`, `results.json` | one row per check, then each check's detail lines |
| `timeline.jsonl` | every phase, fault, revert, and readiness probe, with planned and actual times, `rc`, `stderr`, and `netem show` output |
| `watchdog.jsonl` | each container's `docker inspect` state every 5 s |
| `stats.ndjson`, `vm-freshness.jsonl` | the 30-second `docker stats` and per-sink freshness samples; no freshness samples for an external target |
| `vm-export/<sink>.jsonl` | each sink's backend's `/api/v1/export` of the sink's `vm_selector` at the end; absent for an external target |
| `logs/<service>.stdout`, `.stderr` | each service's output across every life of its container |
| `log-chunks.jsonl` | one record per `docker logs` call: its `--since`, `--until`, exit code, and bytes, or the end of its error when it failed |
| `inspect/<service>.json` | each container's final `docker inspect` |
| `provenance.txt`, `compose.env`, `scenario.toml`, `scenario.resolved.json`, `configs/` | what ran, on what host, from which commit, the seed of a random schedule, the expanded schedule, the role of each service, and a copy of each `logit` config and of the compose override |

## The checks

Each check skips fault windows, a fault's span plus the scenario's `recovery_bound`, where its
rule only holds in steady state. The plan's "The checks" has the full rules.

The ledger follows each `[[ledger.source]]`, a generator and the SUT listener its datagrams
reach. `ledger.wire` judges each source on its own, against the fault windows of its own
generator and of every service that isn't a generator, and `ledger.intake` judges each
listener's final life; each reports a line per source or listener and takes the worst status.
From the edge into `aggregate` on, the listeners' counts are summed, because their events merge
there. Nodes between a listener and `aggregate` are listed in `sut_transforms`, and each must
forward every event it receives.

```toml
[configs]
sut = "logit-sut.yaml"
generator = "logit-generator.yaml"              # one config per generator, under its service
generator-lines = "logit-generator-lines.yaml"

[[ledger.source]]         # one per generator
service = "generator-lines"                     # a service with soak.role generator
generator_input = "load"                        # its generate_in
generator_sink = "syslog"                       # its UDP sink, G's source
sut_listener = "lines"                          # the SUT listener it reaches
sut_transforms = ["parse", "labels", "count", "metrics_only"]   # optional: nodes to aggregate
```

The ledger follows each `[[ledger.sink]]`. A row about a sink (`progress`, `identity.sink`,
`recovery`, `ledger.edge`, `ledger.egress`, `ledger.windows`, `ledger.replay`, and
`ledger.sent`) judges each sink, reports a line per sink, and takes the worst status. Under
`split = "copy"`, the default, each sink holds all of `aggregate`'s output, so each sink's V is
judged against Ab alone; under `split = "route"` each sink holds its target's share, so their
summed V is.

```toml
[ledger]
split = "route"           # copy (the default) or route
# ...

[[ledger.sink]]           # one per SUT sink
id = "vm_a"               # the sink's component id
target = "tier_a"         # route only: the target feeding it
backend = "victoria-metrics"                                # local only: a backend service
vm_selector = '{__name__=~"soak_requests_[0-9]+_total"}'    # local only: the series V reads
```

| Check | `FAIL`s when |
|---|---|
| `run` | the driver aborted, hit an error, or was interrupted, or the timeline lacks its `start` or `end_end` phase |
| `timeline` | a fault's apply or revert returned nonzero, the schedule failed fast, or a start or unpause of a `logit` service has no readiness probe; a fault whose apply failed gets no window |
| `exit` | a `logit` container exits outside a scheduled stop, restart, kill, or `exit` step, or with a code other than the one that action leaves (0, 137 for a kill, the step's `code` for an `exit`); an `exit` step sees no exit; or any container is OOM-killed |
| `restarts` | a container starts with no scheduled start or restart behind it: the revert of a `stop` or `kill`, an `exit` revert that started it, or a `restart` |
| `self_log` | stderr has an `exiting` line with a nonzero code, a panic, or an `ERROR` line other than a sink's `retrying`; sink fault lines outside a fault window `WARN`. Inside an `exit` step, its `exiting` line and an `ERROR` line keyed its `key` are expected, and FAIL the row when either is missing |
| `ready` | a `logit` container's readiness word, from each image `HEALTHCHECK` result timed at its start, isn't `ok` outside a fault window; a `mark` with `ready` sees no sample with that word; `degraded` appears outside an `exit` step; or a container isn't ready within 30 s of a start. With no `HEALTHCHECK` results recorded, it judges Docker's health status instead |
| `progress` | a SUT sink delivers nothing in a `progress_window` outside fault windows with work queued (a hang), a `logit` container's telemetry goes quiet, or a sink's newest stored sample is 30 s old (a local target only); a sink still retrying past `recovery_bound` `WARN`s, and so do windows covering under 5% of the run after warmup |
| `rss_slope` | a `logit` service's resident memory grows faster than `rss_growth_mib_per_hour` in steady state; a run shorter than an hour `WARN`s instead, and so do slopes covering under 5% of the run after warmup. Each service is judged on its own line, and `[expected]` can set its verdict (see [Expected verdicts](#expected-verdicts)) |
| `fd_slope` | a `logit` service's open file descriptors end, or sit after a recovery, more than `fd_growth` above the warmup median. Each service is judged on its own line, and `[expected]` can set its verdict |
| `ledger.wire` | for any source, its generator's lines minus what its listener read or the kernel dropped (G − (W + K)), loss by design, exceeds `wire_loss_outside_faults` outside the UDP-affecting fault windows of its own generator and of every service that isn't a generator, with one generator batch of tolerance per window edge, each steady run counting at 0 or more; or its generator's sink counts messages and datagrams apart. The detail lists the gap per source and per run of drain intervals, and the generator's own counted `drop_newest` loss beside it |
| `ledger.intake` | for any listener, the final SUT life's datagrams read minus dropped differ from the events sent plus bad lines (B: `messages.dropped{reason="malformed"}`, `frames.dropped{reason="undecodable"}`, and `lines_in`'s oversize lines); or an earlier life's read-but-unabsorbed residual summed over the listeners (W − D − B − X − Ab, X being the listeners' own counted event drops) falls outside [0, R], R being each listener's receive queue plus 2 batches, 65 more batches for each node between it and `aggregate`, and 65 for `aggregate` |
| `ledger.edge`, `ledger.aggregate` | in the final SUT life, the listeners' summed events sent (or, with `sut_script`, the script's) differ from those `aggregate` received; a node in a source's `sut_transforms` received a different number from those sent to it, or sent a different number from those it received; a sink received a different number of events from those sent to it (under `copy` `aggregate`'s events sent, which a fan-out counts once per batch, and under `route` its target's); or `aggregate` received a different number from those it absorbed |
| `ledger.script` | with `[ledger] sut_script` only, else SKIP: in the final SUT life, the listener's events sent differ from those the script received, those it received from those it emitted, discarded (`events.discarded`), and lost to an error (`events.dropped{reason="script_error"}`), or those it sent from those it emitted plus those `flush()` returned |
| `ledger.egress` | for each sink under `copy`, or for the sinks' sum under `route`: the final SUT life's absorbed increments minus the backend's reset-aware total (Ab − V), or a stopped earlier life's datagrams read minus dropped, bad lines, the listener's counted event drops, and that total (W − D − B − X − V), is nonzero (SKIP on an external target; on a Kafka target V is the topic's distinct records, and a negative gap is a surplus), other than a positive gap in a life whose telemetry counts a batch `batches.dropped` under any reason at a node that can explain it: any node but a sink outside the group, so under `copy` another sink's drop doesn't count a sink's gap, while one at the sink or upstream of the fan-out does (every reason counts; see "The ledger and identities" in the plan's [6. The checks](../../docs/plans/soak-harness.md#6-the-checks-w1a-w1b)); a `rejected` or `disk_corrupt` batch counts the gap but FAILs the life whatever its size; a killed life's Ab − V falls outside its band (below, a surplus: the residual plus one drain interval of ingest; above, uncounted: one aggregate interval plus one drain interval of ingest), with the residual the kill lost reported beside it; or a sink's export matched no series. With a script, its `flush()` events leave Ab, its counted drops and errors leave a stopped life's gap, which the final drain exports because `internal` drains last. A series without one segment per SUT life `WARN`s |
| `ledger.windows` | under `[ledger] vm_every_window = true` only, and SKIPs on an external or Kafka target: a series in a SUT life has two samples more than 1.5 aggregate intervals apart, a killed life's last sample is more than 1.1 intervals before the kill, or a later life's first is more than 2 intervals after its start. The interval comes from the SUT config. A wider gap is excused as idle when the `sut_aggregate` component's `batches.sent` is 0 over the drains from a + (interval + p) / 2 to b − (interval − p) / 2, p being the drain period: no window was emitted, so none was lost. A pause, with no drains at all, counts 0. Any batch there FAILs the gap as windows emitted but not stored, whatever fault was running |
| `ledger.replay` | the life after a killed one replays, in its first drain, a different number of batches from the killed life's last `buffer.batches`, beyond one in flight |
| `ledger.summary` | the final life's uncounted loss, (W − D − E − B) + (E − A) + (A − Ab) + (Ab − V), isn't 0, or any one sink's Ab − V isn't, with a script's hop in place of E − A and one Ab − V term per sink under `copy`; a counted egress term is shown and not judged, unless a `rejected` or `disk_corrupt` drop is in its group's credit, which FAILs the row; on an external target the Ab − V term SKIPs |
| `ledger.sent` | an external or Kafka target only, else SKIP: a batch a sink received didn't end in an accepted response; a clean external run's detail starts with `SENT`. See [External targets](#external-targets) |
| `ledger.topic` | a Kafka target only, else SKIP: the topic wasn't read back; its distinct records number fewer than the sink saw accepted, or more with no dropped attempt to explain them (`WARN` with one, and `WARN` either way across a killed life); or a record is stored twice in a SUT life with no epoch bump (`WARN` after one). See [Kafka targets](#kafka-targets) |
| `identity.sink` | at the final life's last drain before its final one, a sink's batches received, plus those a disk spool replayed at open and the one it counts dropped with a torn tail (`batches.dropped{reason="disk_torn"}`, a batch an earlier life received), differ from delivered + dropped + queued by more than one batch in flight |
| `drops` | the SUT counts a `logit.*.dropped` no other row judges with a `rejected` or `disk_corrupt` reason; any other reason `WARN`s. The ledger's own terms (every `batches.dropped`, a node's `events.dropped` beside a batch drop of the same reason, `bytes.dropped`, a listener's D, K, X, and B, and the script's `script_error`) and, on an external or Kafka target, a sink's `records.dropped` and `metrics.dropped` stay with the rows that balance them, so a drop is counted once. On an external or Kafka target, where `ledger.egress` SKIPs, a `disk_corrupt` batch drop, or a `rejected` one at a node other than a sink, stays in this row |
| `recovery` | within `recovery_bound` of a fault's end, for any sink, no drain interval clear of other faults shows that sink not retrying, its buffer under 5% full or holding at most one batch, every listener's receive queue under 5% full, and the listeners' summed ingest at 95% of the generators' summed warmup rate; a generator `rate_behind` diagnostic turns a rate shortfall into a `WARN`, and so does a run where no fault had an eligible interval |
| `expect.<name>` | a scenario's own `[[expect]]` bound doesn't hold; see [Expectations](#expectations) |

The plan's "The ledger and identities" defines each symbol. Every SUT config gives `internal` a
dedicated pipeline (`self -> telemetry`), so its final drain runs after every data node has
exited and exports the shutdown drops: the listener's `datagrams.dropped{reason="shutdown"}`
goes into D, and a node's `batches.dropped`, under any reason, is what makes a positive
`ledger.egress` gap counted. Only the stderr cross-check is limited to `shutdown`. A SUT config that sends `self` anywhere else loses those counts, so
keep its pipeline dedicated. The stderr copies, the listener's `warn` lines and `drain complete`'s
`batches_dropped`, only cross-check telemetry: `ledger.intake` and `ledger.egress` `WARN` when a
life's stderr count exceeds telemetry's, and are never added to it. A telemetry count above
stderr's is not a disagreement: under `receive: { overflow: block }`, a cancelled blocked push
counts its datagrams as shutdown drops without logging a line. `drain complete` also counts the `internal`
pipeline's own shutdown drops, which no drain exports. The per-hop rows are judged with no
tolerance only for the final SUT life, because an earlier life stops under load. A life a
`kill` ended writes no final drain and counts no shutdown drops, so up to one 5 s `internal`
interval of its counters is lost and its D has no shutdown part; `ledger.egress` judges its Ab − V within a band
instead of for equality, and the plan's "Which life a hop is judged in" derives the band. The
band can't see a lost middle window, or a lost last one when the last flush and drain coincide;
`ledger.windows` catches both unless the kill lands within 0.1 × the aggregate interval of the
last flush, and `ledger.replay` catches a replay two or more batches short.

### Expectations

A scenario's `[[expect]]` tables add one `expect.<name>` row each, judging one metric reduced
over a window around one step, or over the whole run. The plan's "The scenario schema" is the
full reference.

```toml
[[expect]]
name = "sink-queue-fills"
service = "logit"                       # logit or a generator
metric = "logit.component.buffer.batches"
component = "victoria_metrics"          # and any other attribute under attrs = { ... }
step = "c0s1"                           # an expanded step id, or a [[step]] index for every cycle
window = "during"                       # the fault's span; `after`, the recovery_bound after it; `through`, both; `run`, with no step
reduce = "max"                          # counters: delta, min_delta; gauges: max, min, last, slope
min = 2                                 # min, max, or both
```

`min_delta` is the smallest single drain's delta, so `min = 1` asserts a counter rose in every
drain of the window. A gauge reducer includes the value in force at the window's start, because
`internal` exports a gauge only in a drain after it was set, taken from the SUT life running at
that start only. A bound that must hold for the whole episode, such as a loss counter that must
stay 0, uses `through`: a chain blocked behind a sink stays blocked until the sink's next retry,
up to its `retry_max_delay` after the revert. A scenario with no `[[expect]]` gets one `expect`
row that SKIPs.

`window = "run"` takes no `step`: it runs from the end of warmup to the start of the end
sequence, fault windows included. `slope` is a gauge's least-squares slope over the window's
samples, in the metric's unit per hour, so a bound on `logit.process.interner.strings` is in
strings an hour and one on `logit.process.memory.resident.bytes` in bytes an hour. A restart
resets a process gauge, so `slope` fits each SUT life on its own and judges each against the
bounds. A life needs at least 6 samples spanning 60 s, as `rss_slope` does, and the row FAILs
when no life in the window has them.

```toml
[[expect]]
name = "interner-grows"
service = "logit"
metric = "logit.process.interner.strings"
window = "run"
reduce = "slope"
min = 300000              # strings an hour
```

### Expected verdicts

An `[expected]` table sets the verdict a scenario expects of `rss_slope` or `fd_slope` for a
service, the two rows that judge each `logit` service apart:

```toml
[expected]
rss_slope = { logit = "FAIL" }   # row id -> service -> PASS, WARN, or FAIL
```

A service whose verdict matches scores PASS, and its line reads `FAIL, as expected: ...`. A
service whose verdict differs FAILs, as in `expected FAIL, got PASS`, so a growth the scenario
exists to show can't go missing unnoticed. Services with no expectation are judged as usual.
`rss_slope` reports growth as WARN under an hour's run, so an expected FAIL needs a run of an
hour or more; a shorter `--duration` FAILs the row.

### Random schedules

A `[random]` table replaces `cycle` and the `[[step]]` tables. The driver draws one fault at a
time from the seed, so faults never overlap: a gap, then a fault template picked by weight, its
`for`, and for netem one of its `args`. The plan's "The scenario schema" has every rule.

```toml
[random]
seed = 20261009                       # --seed overrides
gap = { min = "2m", max = "4m" }      # quiet time before each fault

[[random.fault]]
weight = 3
action = "netem"
on = "logit"
args = ["delay 200ms 50ms", "loss 30%", "delay 1s", "rate 1mbit"]   # one per draw
for = { min = "60s", max = "3m" }
```

- `gap.min` must be at least `recovery_bound + 2 x progress_window`, and `cooldown` at least
  `gap.min`, so every fault's recovery is followed by steady state the checks judge.
- `[[expect]]` can't be used with `[random]`, because its rows name steps, and the steps change
  with the seed. The watchdog, the ledger, and `recovery` judge a random schedule.
- `--seed N` draws another schedule from the same scenario. It's refused for a fixed schedule.
  The seed is in `provenance.txt`, the header of `results.md`, `results.json`, and
  `scenario.resolved.json`, which also holds the drawn steps (`r1`, `r2`, ...), so
  `script/soak run <scenario> --seed N --duration D` reproduces a run's schedule, and `check`
  re-scores it from the directory alone. A shorter `--duration` runs a prefix of the same
  schedule.
- `script/soak list` shows each random scenario's seed and how many faults its own duration
  draws.

## Long runs

To run for hours, keep the driver alive when the terminal closes: a closed terminal sends
SIGHUP, which ends the schedule, collects, and tears down, so an unprotected 8-hour run stops at
the first disconnect. Run it in `tmux` or `screen`:

```sh
tmux new -s soak 'script/soak run random-faults --duration 8h 2>&1 | tee soak-8h.log'
```

`nohup script/soak run random-faults --duration 8h > soak-8h.log 2>&1 &` works too when
`$DOCKER` needs no password (`DOCKER=docker`, or `sudo` without one). Bash resends SIGHUP to its
jobs when the terminal closes, and the driver keeps the ignore `nohup` set, so the run carries
on. The driver runs every docker command as `sudo -n`, which fails instead of prompting, and with
no terminal there's nothing to prompt on; `tmux` keeps the terminal the ticket was primed on.
Follow a run with `tail -f perf/results/soak/<stamp>/timeline.jsonl`.

What an 8-hour run costs, from the recorded runs' rates:

| Resource | 8 hours | 24 hours |
|---|---|---|
| Run directory | about 250 MB: 100 MB SUT stdout, 130 MB generator stdout, 8 MB `watchdog.jsonl`, 6 MB `vm-export/` per sink | about 750 MB |
| Docker's own log files | at most 100 MB per container (5 rotated files of 20 MB) | the same |
| `check` | about 4 s and 250 MB of RAM | about 15 s and 700 MB |
| Containers | three; each `logit` process held about 15 MiB RSS in the recorded runs | the same |

Each container's `json-file` log rotates at 20 MB and keeps 5 files. Telemetry fills one at
about 6.4 KB/s, so the daemon holds about 3.5 hours of each container's log, and the driver
appends the new lines every 5 minutes, well inside that. The plan's "Findings" has the
arithmetic and the `check` timings on a synthetic 8-hour run directory.

## Cleanup and a shared daemon

Everything a run creates belongs to the `soak-<scenario>` project, apart from the one-shot netem
containers, which remove themselves. The script refuses to start while that project has
containers, and prints the `down` command for a stack a crashed or `--keep` run left behind. Two
different scenarios can run at once; it never prunes.
