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
script/soak run random-faults --duration 8h --retry-stalled 2   # rerun if the host sleeps
script/soak run statsd-datadog                   # an external target; needs a key, see "External targets"
script/soak run kafka-broker-restart             # a Kafka target; see "Kafka targets"
script/soak run standin-remote-write             # a stand-in target; see "Stand-in targets"
script/soak run standin-datadog                  # a Datadog-shaped stand-in, no key needed
script/soak run standin-influxdb                 # an InfluxDB-shaped stand-in, no token needed
script/soak check perf/results/soak/<stamp>      # re-score a run offline
```

## What it runs

A scenario is a directory under `scenarios/`: a `scenario.toml` timeline beside one ordinary
`logit` config per `logit` service. `compose.yaml` starts three services under the compose
project `soak-<scenario>`, or the two `logit` services alone for a scenario with an external
target (see [External targets](#external-targets)), or the two with a Kafka broker in place of
VictoriaMetrics for a Kafka target (see [Kafka targets](#kafka-targets)), or the two with a
scripted stand-in in its place for a stand-in target (see [Stand-in targets](#stand-in-targets)):

| Service | Image | `soak.role` |
|---|---|---|
| `victoria-metrics` | `victoriametrics/victoria-metrics:v1.152.0` | `backend`: `-retentionPeriod=100y`, published on an ephemeral loopback port the driver queries; in the compose profile `local`, which the driver enables for a local target only |
| `logit` | `logit:soak`, built from the current tree | `sut`: the system under test (SUT), with the scenario's `configs.sut`. Every generator sends to it. An override can add further SUT services; see [Several SUT services](#several-sut-services) |
| `generator` | `logit:soak` | `generator`: a load source, a second `logit` with the scenario's `configs.generator`. An override can add more, each with the config `[configs]` names under its service name and its own `[[ledger.source]]` |
| `kafka` | `apache/kafka:4.2.0` | `broker`: a single-node KRaft broker, one partition per topic, for a Kafka target only (the compose profile `kafka`); the driver reads its topic back through `docker exec` |
| `standin` | `python:3.13.16-alpine3.24`, running `standin/standin.py` | `standin`: a scripted HTTP destination, for a stand-in target only (the compose profile `standin`); it answers as `respond` steps cue it on its published control port, and its stdout is the record of every request |

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

Every `logit` service runs `logit --log-format json run`, so stdout carries the `internal`
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

`partition-rss` (15 minutes 45 seconds) runs `statsd-vm`'s stack with one fault, a 45 s
partition of the generator 2 minutes in, then 13 minutes with no fault. It asks whether the
generator gives back the memory the partition's backlog took and then holds flat. Its
`rss_settle` of `120s` puts `rss_slope`'s fit on the plateau after the release, and an
expectation bounds the generator's RSS rise over that plateau to 8 MiB. A SIGHUP step on the
generator, which opens no fault window, marks the plateau's span for that expectation.

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

Six scenarios put a `lua` stage between the listener and `aggregate`, with
`[ledger] sut_script` naming it, so `ledger.script` balances the script's hop. A `-jit`
scenario is the scenario of the same name without the suffix, with LuaJIT's trace compiler on
(`jit: true`):

| Scenario | Length | What it drives |
|---|---|---|
| `lua-path` | 16 minutes | `statsd-vm`'s schedule through a script that rewrites every event, drops every 53rd, fails on every 97th, and returns one rollup event from `flush()` each window; its expectations assert all three outcomes |
| `lua-stall` | 8 minutes | a busy loop of about 30 s on the 360,000th event: the node reads stalled, `/readyz` answers `503 stalled`, the receive queue fills and evicts, and the script resumes on its own. A `mark` with `ready = "stalled"` covers it |
| `lua-memory` | 8 minutes 10 seconds | from the 480,000th event the script keeps 1 KB per event until the VM passes `max_memory: 64MiB`: the SUT logs `memory_limit_exceeded`, drains, and exits 2 inside an `exit` step, whose revert starts it again. The run ends before the second life reaches that event |
| `lua-path-jit` | 16 minutes | `lua-path` under the compiler, rewriting in place: two attribute writes, a `pairs` loop over the attribute proxy, and an `= nil` delete |
| `lua-stall-jit` | 8 minutes | `lua-stall` under the compiler: the compiled loop runs about 35 s and still reads `stalled`, since stall detection doesn't depend on a hook |
| `lua-memory-jit` | 8 minutes 10 seconds | `lua-memory` under the compiler: the kept strings are on the Lua heap, which `max_memory` counts either way, so the SUT still exits 2 |

Five scenarios give the SUT two sinks of one kind, each to its own VictoriaMetrics, with one
`[[ledger.sink]]` table per sink:

| Scenario | Length | What it drives |
|---|---|---|
| `fanout-copy` | 12 minutes 30 seconds | `aggregate` fans out to two sinks, each holding a full copy (`split = "copy"`): the second backend stopped 90 s, netem loss on the first, then both stopped, overlapping. Its expectations assert that the first sink keeps delivering while the second backend is down, the second queues, and neither drops a batch |
| `fanout-block` | 5 minutes 30 seconds | as `fanout-copy` with 500 ms windows, and the second sink's queue at 2 batches under `overflow: block`, through a 90 s stop of its backend. A fan-out offers each batch to its consumers in turn through one send, so once that sink's queue and inbox fill, `aggregate` waits on it and the healthy sink stops delivering too. A `mark` names the blocked span; the expectations assert the stall, the backpressure reaching the listener, and both sinks recovering |
| `route-split` | 9 minutes | the generator tags each line `shard:0` or `shard:1` over DogStatsD, and a `route` on `shard` fills two targets, each feeding one sink (`split = "route"`): one backend stopped 90 s, then netem loss on the other. Each backend holds one shard's series, so the egress gap is judged on their sum |
| `route-copy` | 10 minutes | as `route-split`, with the `route` ahead of two aggregates, one per target, and a list-valued `routes:` value that sends each shard-0 line whole to both targets (`split = "target"`): one backend stopped 90 s, the SUT restarted, then netem loss on the generator. Each sink is judged against its own target's sends, and the router against its copies (`events.copied`) |
| `route-signals` | 13 minutes | `fanin`'s syslog lines, each given a `soak_lines` increment by `kv_metrics` so it carries a log and a metric, into a `route_signals` that splits every event into a log part and a metric part, one per target (`split = "target"`). Each target feeds its own aggregate, sink, and VictoriaMetrics; the log branch gives each log a `soak_log_lines` increment, the count V can read. The metric branch's backend stopped 90 s, the SUT stopped, then killed, then netem loss on the generator |

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

The stall and the memory cap depend on the host's speed: the `lua-stall` scenarios' loop count
and the `lua-memory` scenarios' event count are calibrated on the development host, and the
comments in their `logit-sut.yaml` give the timings. On a much slower or faster host the event falls outside its
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
[Kafka targets](#kafka-targets). `kafka-otlp-broker-restart` is the same run with `kafka_out`
under `format: otlp_proto`: one OTLP metrics export request per window, holding every series.

`kafka-sasl-reauth` (8 minutes 30 seconds, up to 8 hours) is `kafka-broker-restart` with ten
series instead of 100, over SCRAM-SHA-512 to a broker that ends every session after 30 s
(`connections.max.reauth.ms`). Its compose override starts the broker from the scenario's
`kafka-server.properties`, with a SASL listener on 9096 beside the unauthenticated 9092 that the
healthcheck and the read-back use, and a SCRAM user written at format time. `kafka_out` doesn't
re-authenticate in band: it stops reusing a connection 20 s after it authenticates and redials,
counting `logit.output.reconnects`. Beside the base scenario's expectations, it bounds that
rate over the run (80 to 300 an hour, derived in its `scenario.toml`), which catches a redial
before every send. A sink that never renewed would still redial, because the broker closes an
expired session's connection on its next request, so a second rate catches that sink: at most
40 `ambiguous` attempts an hour, the faults' own. It also asserts that no attempt was `refused`
and that the sink dropped no batch over the whole run. Ten series keep an 8-hour
topic at 144,000 records, inside the read-back's bound.

`kafka-oauth-reauth` (15 minutes 30 seconds, up to 8 hours) is `kafka-sasl-reauth` over
OAUTHBEARER with the client-credentials grant. Its token endpoint, `idp`, is
`tools/kafka-interop`'s `mock-oauth2-server`, and it and the SUT share a namespace holder (see
[A token endpoint beside the SUT](#a-token-endpoint-beside-the-sut)). Each token lives 60 s and
ends its broker session when it expires, so the sink fetches a token and redials about every
50 s. After the broker's restart, pause, and stop, the cycle stops the endpoint for 2 minutes,
then pauses it for 2 minutes, each longer than a token lives. The stop shows the fallback: a
redial whose refresh fails keeps the cached token, and the sink's `token_refresh_failed`
diagnostic counts it. Once the token expires, both faults fail each dial `Clean`, and the sink
holds its queue and retries until the endpoint is back. Beside W17's rows, it asserts the
diagnostic inside the stop, `clean` attempts and `retrying` inside both endpoint faults, and
`retrying` clearing after each. The reconnect rate is bounded to 25 to 220 an hour and the
`ambiguous` rate to 20 an hour, both derived in its `scenario.toml`. No attempt is `refused`:
the sink never sends a token it knows has expired.

Seven scenarios run a sink against a scripted stand-in in place of VictoriaMetrics (see
[Stand-in targets](#stand-in-targets)):

| Scenario | Length | What it drives |
|---|---|---|
| `standin-remote-write` | 37 minutes 25 seconds | `prometheus_out` (remote-write 1.0, Snappy) through every row of its sender-mode response-class table: twenty 20 s steps, 105 s apart, from `503`, `429`, and each `400` body the sink reads, through `401`, `403`, `404`, `405`, `407`, `409`, `413`, `415`, `422`, `501`, and `302`, to a connection reset, a stop of the stand-in, and a `408`, each step naming the class the sink must read |
| `standin-at-most-once` | 8 minutes 55 seconds | the same sink under `buffer.delivery: at_most_once`: a `503` and a reset drop the batch, a `401` holds it, a `400` drops it |
| `standin-concurrency` | 9 minutes 45 seconds | the same sink and posture at `concurrency: 8`, with a 250 ms `aggregate` window and a 1 s delay on the SUT's egress, so about four batches are in flight. Each step's `count = 1` cues one write among them: a `401` holds that batch when it reaches the head while the batches behind it are answered, and its resend releases it; a `400` drops that batch alone; a reset drops it alone, `ambiguous_at_most_once`. The expectations count one drop for each |
| `standin-datadog` | 30 minutes 25 seconds | `datadog_out` (series, gzip, delta counts) through every row of its response-class table: sixteen 20 s steps, 105 s apart, from a `202` whose body names two series dropped, through `400`, `401`, `403`, `404`, `405`, `408`, `413`, `429`, `500`, `501`, a `503` after storing (its resend a duplicate), `302`, and `422`, to a connection reset and a stop of the stand-in |
| `standin-influxdb` | 35 minutes 40 seconds | `influxdb_out` (line protocol, delta) with `bucket:` and `org:` configured, through every row of its response-class table: nineteen 20 s steps, 105 s apart, from a `204` and each `400` body, through both `401` bodies, `403`, a `404` naming the bucket, the org, an orgID, and neither, `413`, `422`, `429`, `500`, a `503` after storing (its resend a rewrite), `408`, and `302`, to a connection reset and a stop of the stand-in. Every `403` and `404` holds, since every value is the operator's. The request timeout isn't cued: the sink's 10 s timeout isn't configurable |
| `standin-influxdb-carried` | 35 minutes 40 seconds | the same steps with the bucket, org, and orgID carried on each batch by a `set`, as `influxdb_in` would carry them, and none configured: every `403` and `404` drops the batch |
| `standin-influxdb-mixed` | 35 minutes 40 seconds | the same steps with the bucket carried and the org from `default_org:`: a `403`, a `404` naming the bucket, and a `404` naming neither drop the batch, and a `404` naming the org or an orgID holds |

Two scenarios run a second `logit` under test, `collector`, at the far end of the native hop
(see [Several SUT services](#several-sut-services)):

| Scenario | Length | What it drives |
|---|---|---|
| `native-hop` | 17 minutes | `logit` takes statsd and forwards it with `logit_out` from a disk spool to `collector`'s `logit_in`, which aggregates into VictoriaMetrics: a netem delay and then 10% loss on `logit`'s sends, a partition, a stop, a pause, and a kill of `collector`, a kill of `logit` with `collector` up, and a kill of `logit` inside a stop of `collector`. `ledger.hop` judges the hop; its expectations assert the replay after the first `logit` kill is deduplicated, `logit` reconnects after the `collector` kill, and the spool holds the hop through the stop |
| `native-hop-spans` | 17 minutes | `native-hop` with `span_sample_rate` at the product's default, `0.1`, on both SUTs, so internal spans and the hop frame's trace context run under the same faults. The spans go to stdout beside the metrics, where the NDJSON reader skips them; its expectations add that each SUT emitted spans. `internal` buffers at most 512 spans per component between drains and counts the rest `logit.internal.spans.dropped{reason="buffer_full"}`, which `drops` judges like any other drop |

`rolling-overlap` (19 minutes) runs two instances of one SUT, `logit` and `logit-b`, on one UDP
port under `reuse_port`, in the network namespace of a holder, `sut-net` (see
[Rolling overlap](#rolling-overlap)). Its generator sends `statsd-vm`'s 2,000 lines a second as
eight flows, one per `statsd_out`, which the kernel spreads over the instances and moves as they
stop and start. Each instance stamps its name on its series, aggregates, and spools to
VictoriaMetrics. Its 16-minute cycle stops `logit`, then `logit-b`, delays the holder's egress,
stops `logit` inside a stop of VictoriaMetrics, restarts `logit-b`, and stops it again. Its
expectations assert that each instance reads while the other is down and that neither socket
queue overflows; `ledger.overlap` bounds what each close discarded.

`tcp-ingress` (20 minutes 30 seconds) is `statsd-vm`'s stream over one TCP connection:
`statsd_out` with `transport: tcp` into `statsd_in` with `transport: tcp` and a 60 s
`idle_timeout`, its source declared `wire = "tcp"`. Its 17.5-minute cycle delays and then drops
the generator's segments, drops 30% of the SUT's (its ACKs among them), pauses, stops, kills,
and partitions the SUT, and pauses the generator for 90 s. TCP retransmits what netem drops, so
`ledger.wire` judges the generator's netem at 0; the SUT's pause backs up into the generator,
whose overflow and ambiguous drops are counted there; a stop or a kill can reset the connection
with lines in its socket buffers, so the row reports those windows. Its override caps each
end's TCP buffers at 256 KiB (`net.ipv4.tcp_rmem` and `tcp_wmem` in each container's
namespace), because a host default of up to 32 MiB holds minutes of its 42 KB/s and a pause
then never reaches the generator. Its expectations assert the generator's overflow and
ambiguous drops around the SUT's pause, an idle close around the generator's, and none under
the netem delay.

`http-ingress` (18 minutes) sends 2,000 InfluxDB points a second with `influxdb_out`, 100 to a
request, into `influxdb_in`, its source declared `wire = "http"`. Each point carries only a
string field, so the SUT decodes no gauge, and a `kv_metrics` counts one per point ahead of a
500 ms `aggregate` and a two-batch blocking sink queue. G is the points the SUT answered `204`
for, which it does only once their batch is in its pipeline, so every window but a kill's is
judged. Its 15-minute cycle delays the SUT's egress 11 s, past `influxdb_out`'s 10 s timeout,
so the generator resends batches the SUT took; drops 30% of the SUT's egress
and 10% of the generator's; stops VictoriaMetrics for 90 s, which backs up into the listener's
busy `503`s; and stops, restarts, and kills the SUT. Its expectations assert deferred batches
and the generator's `5xx` answers around the backend stop, transport errors under the delay,
and no sink drop.

`http-concurrency` and `http-concurrency-1` (10 minutes 30 seconds each) are one run at two
settings of the generator's `influxdb_out` `concurrency:`, 8 and 1, measuring what the field buys
on a slow link. The generator sends 500 InfluxDB points a second, 50 to a request, into
`http-ingress`'s SUT. The cycle delays the SUT's egress, which carries its answers, by
200 ms ± 50 ms for 3.5 minutes, then stops VictoriaMetrics for 90 s, which backs up into the
listener's busy `503`s. At 1 the generator moves about four batches a second against the ten
offered, and its 50-batch queue overflows, counted; at 8 two or three requests in flight keep up.
At 8, the expectations assert that more than one request was in flight and none overflowed under
the delay, and that all eight slots filled while the listener held its answers during the stop;
at 1, that the queue overflowed under the delay. Both assert the generator's `5xx` answers
around the stop and no sink drop. Neither runs over TLS: `influxdb_out` has no `tls:` field and
trusts only its built-in roots, so it can't reach a listener whose certificate the run's private
CA signs, and over plaintext it speaks HTTP/1.1, one connection per request in flight.

`tail-rotate` (13 minutes 45 seconds) has the generator write 1,000 log lines a second with
`file_out` (`format: json`) into a file rotated about every 44 s, keeping every file, in
`<run>/shared`, which the driver creates and the scenario's override mounts into the writer and
the SUT. The SUT tails `/shared/app.log*` with `tail_in` (`read_from: beginning`, a checkpoint
in its own filesystem) through `json` and a count chain into `aggregate` and a disk-spooled
sink, its source declared `wire = "file"`. Its 10.75-minute cycle pauses the SUT for 60 s,
stops it, restarts it, and kills it for 30 s and then for 150 s, across several rotations.
`ledger.wire` judges the run's W − G against a replay band per SUT life, and the end sequence
counts the writer's lines in the shared directory before it waits for the tailer to reach G.
Its expectations assert that rotations are seen and that no resume is rejected and no
checkpoint fails. The writer's files stay in the run directory, about 80 MB of them. Its
`max_duration` is 25 minutes, and a longer `--duration` is refused: the tailer holds every
rotated file open, about one descriptor more every 44 s, which passes `fd_growth`'s 40 at about
30 minutes, and from about 74 minutes `file_out` deletes the oldest file, whose lines the end
count then misses.

`tail-rotate-stale` (14 minutes 45 seconds) is `tail-rotate` with `timestamp` and `drop_stale`
in the path: `timestamp` gives each line the time the writer stamped on it, and `drop_stale`
(`max_age: 50s`), the node ahead of `aggregate`, discards a line older than that, counted in
its `events.discarded`, which the ledger subtracts. Its 11.75-minute cycle pauses the SUT for
60 s, stops it for 90 s, restarts it, and kills it for 30 s and then for 150 s. Each fault
that keeps the tailer from reading for more than 50 s leaves a backlog whose oldest lines are
stale when the tailer reads them. A 90 s stop discards about 40 s of lines. A 30 s kill
replays the lines read since the last 5 s checkpoint, which are about 40 s old and stay
fresh. Its expectations assert discards after the pause, the stop, and the long kill, none
through the restart or the short kill, and every line's `timestamp` resolved.

`tail-rotate-delete` (4 hours by default) is `tail-rotate` against a writer that keeps eight files
(`max_files: 8`) and deletes the oldest at every rotation from its eighth on, about one every 44 s.
`tail_in` holds a rotated file open until the writer deletes it, so its descriptors must stay
bounded by that retention over hours. Its cycle is an hour long: the same five faults, then about
50 minutes of one SUT life with no fault, where a leak a restart resets still shows. Deleted files
can't be counted at the end, so `ledger.wire` holds the lines left in the shared directory to G
only as a bound, and requires the eight files the writer keeps. It FAILs a SUT stop, kill, restart,
or `exit` during which the writer rotated six times or more, or a pause during which it rotated
eight times or more, enough to delete a file the tailer hadn't read, a loss `tail_in` counts only
as far as the last checkpoint knew. The 150 s kill spans about three rotations. `fd_growth` is 10:
the tailer holds about eight more files than at warmup, plus two to spare. Its expectations bound
`logit.input.files.open` at 10 and the process's descriptors at 26, require eight or more writer
rotations, so the writer deleted files, and assert no checkpoint error and no gone-file bytes
(`no-gone-bytes`, `logit.input.bytes.dropped` at 0). It has no `no-resume-rejected` expectation: an
inode a deletion freed while the SUT was down can be reused by the writer's next file, and the
restart then rejects the checkpoint entry whose head no longer matches.

`otlp-fan-out` (16 minutes) sends 500 OTLP log records a second with `otlp_out` over HTTP, 50
to a request, into `otlp_in`, its source declared `wire = "http"`. Each record's body is four
lines of InfluxDB line protocol, each point with its own tag value so none merge, and the SUT's
`line_protocol` emits one event per point, discarding the record, ahead of a `kv_metrics`
count, a 500 ms `aggregate`, and a two-batch blocking sink queue. The source declares
`fan_out = { points = 4 }`. Its cycle is `statsd-vm`'s, whose 90 s VictoriaMetrics stop backs
up through the inboxes past `line_protocol`, each batch holding four times the events, into
`otlp_in`, which holds each request until the generator's 10 s timeout. The generator then
resends, and its 50-batch queue overflows, counted. Its expectations assert the sink queue
filling and never dropping during that stop, the generator's transport errors through the
SUT's pause, and no line dropped.

`otlp-fan-out-conc` (16 minutes) is `otlp-fan-out` with the generator's `otlp_out` at
`concurrency: 8`, judged by the same ledger. Beside `otlp-fan-out`'s expectations, it asserts
that more than one request was in flight under the 200 ms delay and that the generator's queue
didn't overflow there, where one request at a time overflowed.

`sighup` (14 minutes 30 seconds) runs `statsd-vm`'s stream with a second SUT sink off the
ledger, `archive`: a `stdio_out` writing one `format: json` line per statsd event, straight from
the listener, to `/shared/archive.log` in `<run>/shared`, which the override mounts into the
SUT. Its 11.5-minute cycle sends `logit` a SIGHUP three times in steady state, once 10 s after a
`rename` moves `archive.log` to `archive.log.1`, once during a 90 s VictoriaMetrics stop, and
once after a second rename to `archive.log.2`, and sends one to the generator. Each SIGHUP makes
`archive` reopen its path before its next write; the stdout telemetry stream ignores it, so
every steady-state row judges straight through the signals, which open no fault window.
`file.reopen` judges each rename, and the expectations assert one reopen through each SIGHUP to
`logit`, a generator that keeps sending through its own, and an `archive` that drops nothing.
`archive` is best-effort (`overflow: drop_oldest`), so a stall there can't hold the listener the
ledger follows. Its files stay in the run directory, about 260 MB of them, and its
`max_duration` is its own duration, because a second cycle's rename would find
`archive.log.1` taken. To keep a scored run small, write each file's
`checks.ndjson_file_lines()` into `<run>/shared-counts.json`, keyed by file name, and delete the
files: `check` reads a file `shared/` no longer holds from there.

`tls-reload` (15 minutes 30 seconds) sends `tcp-ingress`'s stream over mutual TLS: the
generator's `statsd_out` presents a client certificate, and the SUT's `statsd_in` requires one
from the run's private CA (see [TLS certificates](#tls-certificates)). The SUT runs with
`tls_reload_interval: 0s`, so only a SIGHUP reloads it, and the generator polls every 10 s. Its
12.5-minute cycle rotates the SUT's server certificate and sends a SIGHUP, then probes what the
listener serves; rotates the generator's client certificate, which its poll reloads, and
restarts the SUT so the generator reconnects after the reload; rotates the server
certificate broken, so the SIGHUP's reload fails and the listener keeps serving the previous
one; rotates it again, good; and rotates it once more during a 90 s VictoriaMetrics stop. Its
expectations assert one reload through each SIGHUP after a good rotation and a `notAfter` a
day later at least, one failed reload and an unchanged `notAfter` through the broken one's,
the generator's own reload, lines read in every drain after the restart, and no
`connection_error` in the run. `tls.served` checks each probe's serial. The SUT trusts any
certificate from the run's CA, so no row tells which generation the generator presents after
the restart; it presents the loaded one because its resolver hands every handshake the current
pair (`ReloadingCert` in `crates/logit-pipeline/src/tls.rs`). Its `max_duration` is
its own duration, because a second cycle's rotations would name the same days. The netem image
carries the certificate tooling, so a run with `SOAK_SKIP_IMAGE` set needs one built from this
tree.

### Several SUT services

A scenario can run a second `logit` under test, at the far end of a native hop. `logit`
stays the ingress SUT, the one every generator sends to and every `[[ledger.source]]` listener is
in. A further SUT service is declared by a `[configs]` key of its own, one that is neither `sut`
nor a source's `service`, and added by the override as a service of that name with the role
`sut`, mounting the config from its own variable, `SOAK_<SERVICE>_CONFIG`:

```toml
[configs]
sut = "logit-sut.yaml"
collector = "logit-collector.yaml"   # a further SUT service
generator = "logit-generator.yaml"

[[ledger.hop]]            # the native hop joining them; one, at most
from = "logit"            # the SUT holding the logit_out
sink = "hop"              # its logit_out component id
to = "collector"          # the SUT holding the logit_in
listener = "hop"          # its logit_in component id
```

```yaml
services:
  collector:
    extends: { file: compose.yaml, service: logit }
    labels: { soak.role: sut }
    volumes: ["${SOAK_COLLECTOR_CONFIG:?set by script/soak}:/config.yaml:ro,z"]
  logit:
    depends_on: { collector: { condition: service_healthy } }
```

The second SUT holds `sut_aggregate` and every `[[ledger.sink]]`. A second SUT needs the hop,
`from` must be `logit` and `to` the SUT `[configs]` declares, and a hop can't be combined with
`sut_script` or a source's `sut_transforms`. A `sut` service the scenario doesn't declare is
refused, and so is a declared one the stack doesn't run. A `kill` or an `exit` may name either
SUT, and a fault on `collector` never counts as affecting UDP ingress. At the end the driver
stops `logit` once its hop sink's queue is empty with no frame in flight, then `collector` once
the backends' totals hold.

Each SUT's process lives are its own, and nothing pairs a life of one with a life of the other.
The listener rows are judged on `logit`'s lives, with its hop sink's events received, S_hop,
where `aggregate` would be; the rows from `aggregate` on are judged on `collector`'s, fed by its
`logit_in`'s events sent, E_B. `ledger.hop` judges the hop on run totals, and `ledger.summary`
adds a bound end to end: V over the run is at most what the generators sent less every counted
loss. A duplicate balances every count after the hop, so the bound catches one only once it
exceeds the run's uncounted loss, the wire loss among it; `ledger.hop` judges a smaller one. The
plan's "The native hop (W10b)" has the rules.

### Rolling overlap

A scenario can run two or more instances of one SUT on one UDP port, as a rolling upgrade
overlaps them. Each instance runs the whole pipeline. The override adds a namespace holder with
the role `netns`, which owns the address, since an instance that owned it would take it along
when it stopped, and puts every instance in it:

```toml
[overlap]
holder = "sut-net"                  # the netns service
instances = ["logit", "logit-b"]    # logit first; each runs [configs] sut unless it has a key
label = "soak_instance"             # the attribute each instance's `set` stamps on its events
```

```yaml
services:
  sut-net:
    image: logit-soak-netem:local
    entrypoint: ["sleep", "infinity"]
    labels: { soak.role: netns }
    stop_signal: SIGKILL
  logit:
    network_mode: "service:sut-net"
    environment: { LOGIT_ADMIN: unix:/tmp/admin.sock, SOAK_INSTANCE: logit }
  logit-b:
    extends: { file: compose.yaml, service: logit }
    labels: { soak.role: sut }
    network_mode: "service:sut-net"
    environment: { LOGIT_ADMIN: unix:/tmp/admin.sock, SOAK_INSTANCE: logit-b }
```

The loader holds each instance's config to the overlap: `reuse_port: true` on every listener a
source names, a `shutdown.delay` that with the drain fits `docker stop -t 30`, an admin
`socket:` and no `bind:` (the driver probes `logit ready` through the container's
`LOGIT_ADMIN`), and a `set` stamping `label` that each source's `sut_transforms` lists. The
generators send to the holder's name, over UDP only. A generator needs several flows to spread:
`generator_input` and `generator_sink` may both list components, as many of each, each input
feeding the sink at its position, one sink per flow; their counts sum. Only `netem` and `partition` go on the holder, neither on an instance, and two
instances may not be held (paused, stopped, killed, restarted, or exited) at once. At the end the
driver waits once on the backends' totals, then stops each instance.

The rows about a process life judge each instance on its own lives, its V the series whose
`label` names it, and FAIL a series that names no instance. The wire, `progress`, `recovery`,
and the new `ledger.overlap` judge the instances together, since flows move between them:
`progress` counts deliveries from the instances not held, and `ledger.overlap` holds the run's
wire loss to what the instances' socket closes could discard. The plan's "Rolling overlap
(W12b)" has the rules.

### A token endpoint beside the SUT

`kafka_out` takes a plain-HTTP `token_endpoint` on loopback only, so a token server in the stack
shares the SUT's network namespace. Its role is `idp`. As in a rolling overlap, a holder with
the role `netns` owns the namespace, so a stop of the `idp` leaves the SUT's network up, and the
override puts `logit` and the `idp` in it:

```yaml
services:
  sut-net:
    image: logit-soak-netem:local
    entrypoint: ["sleep", "infinity"]
    labels: { soak.role: netns }
    stop_signal: SIGKILL
  idp:
    image: ghcr.io/navikt/mock-oauth2-server:6.0.2@sha256:…
    labels: { soak.role: idp }
    network_mode: "service:sut-net"
  logit:
    network_mode: "service:sut-net"
```

The SUT reaches the `idp` at `127.0.0.1`, and every other service reaches either at the holder's
name: the generator sends to `sut-net:8125`, and the broker fetches the JWKS from `sut-net`.
With no `[overlap]`, a `netns` service needs an `idp` and an `idp` needs one, the stack runs no
SUT but `logit`, and `logit`'s and the `idp`'s `network_mode` must name the holder. Put `netem`
and `partition` on the holder, which then shape or cut the SUT's network, and neither on
`logit` or the `idp`; the holder takes no other fault. A `pause`, `stop`, or `restart` of the
`idp` affects egress only. `self_log` counts a `token_refresh_failed` warning inside a fault
window on an `idp` and WARNs one outside every such window. The plan's "A token endpoint in the
SUT's namespace (W19)" has the rules.

### TLS certificates

A `[tls]` table gives a run a private CA and the certificates it names. Before the stack starts,
the driver runs `certs.sh` (in the netem image, as your user) into `<run>/tls`: the CA, a client
certificate the probe presents, and each named certificate's first generation, valid 30 days.
Keys are EC P-256, and every file is 0644, readable by the image's `logit` user.

```toml
[tls.cert.statsd]         # one table per certificate; `probe` is reserved
san = ["logit"]           # its DNS names, the first one the name a probe verifies
usage = "server"          # server or client: its extended key usage
```

Each certificate is `<name>/live/cert.pem` and `<name>/live/key.pem`, and the CA is `ca.pem`.
`live` is a relative symlink to `<name>/gen-N/`, which a `rotate` step repoints in one rename.
The swap is atomic per file, not across a set: a reload reads a set's files one at a time, so
a poll that lands on the rename can read one generation's certificate with the other's key. That
load fails, and the next poll loads the new pair; `self_log` excuses such a torn read on a
service that polls when a reload of the same component follows it inside the rotate. The driver names the
directory in `compose.env` as `SOAK_TLS_DIR`, and the override mounts the whole directory, not
single files, so the repointed link reaches every container:

```yaml
services:
  logit:
    volumes: ["${SOAK_TLS_DIR:?set by script/soak}:/tls:ro,z"]
```

```toml
[[step]]
at = "0s"
action = "rotate"         # issues statsd's next generation and repoints statsd/live
on = "logit"              # the service whose config reads /tls/statsd/live/
cert = "statsd"
days = 31                 # above 30 and every earlier rotation's, so notAfter rises
# broken = true           # a key that doesn't match the certificate: the reload fails
for = "40s"

[[step]]
at = "10s"
action = "signal"
on = "logit"
signal = "HUP"
for = "10s"

[[step]]
at = "25s"
action = "probe"          # s_client from the service's own network namespace
on = "logit"
cert = "statsd"           # a server certificate
port = 8125               # the listener's port
for = "5s"
```

The loader holds a `rotate` to a service whose config reads its certificate's `live/` files, to
days above 30 and above every earlier rotation of that certificate, and to one run of the
cycle, so `max_duration` is one cycle's run. A `probe` starts inside a `rotate` of its
certificate on its own service, after a SIGHUP there, which a listener with the poll off needs
before it serves anything new. On such a service every earlier rotate of a probed certificate
needs a SIGHUP inside it too, or a start of the service before the certificate's next rotate,
since `tls.served` expects the latest rotation that wasn't broken to be the one loaded. A probe presents the probe certificate, so a listener that
requires one completes the handshake and logs no `connection_error`.

### Faults

| Action | What it does | Revert |
|---|---|---|
| `netem` | a one-shot `logit-soak-netem:local` container in the target's network namespace sets a root `tc netem` qdisc with the step's `args` | the same container clears it |
| `pause` | `docker pause` | `docker unpause` |
| `stop` | `docker stop -t 30` | `docker start` |
| `kill` | `docker kill -s KILL`, on a SUT service only: no shutdown signal, so no final telemetry drain, no shutdown drops, and exit code 137. A killed generator's last sends would reach the SUT with no count behind them | `docker start` |
| `restart` | `docker restart -t 30` | none |
| `partition` | `docker network disconnect` from every network, recording the container's address on each (`ips`) | `docker network connect`, passing back each alias recorded before the disconnect, and recording the address after it (`ips_after`), which can differ |
| `mark` | nothing: it marks a window for an event the SUT's own config causes, such as a stalled script. An optional `ready` names a readiness word the SUT must report inside it | nothing |
| `exit` | nothing: on a SUT service only, it names an exit the SUT makes on its own inside the window, with the `code` it leaves and the `key` of the ERROR line that explains it | `docker start` if the container has exited |
| `respond` | on a stand-in only: POSTs the step's cue to its control port, so it answers `status` (with an optional `body`) or `answer = "reset"`, storing the write when `apply` says to or, by default, when the status is `2xx`. With `count = N` the cue answers the next N writes and is then spent, and the stand-in answers its default again, so with several requests in flight a cue lands on one of them. Under the `datadog` shape a `2xx` can carry `series_errors = N`, a body naming the request's first N series dropped | an empty cue, which restores the default answer |
| `signal` | `docker kill -s HUP`, on a service that runs `logit`, with `signal = "HUP"`, the one signal it takes: the process reopens its file targets and reloads its TLS material | none |
| `rename` | on the host, moves `path` to `to` inside `<run>/shared`, both bare names, refusing a `to` that exists. `path` is a `stdio_out` target in `/shared` in the config of the service the step is `on`, and a `signal` step on that service must start inside the rename's `for` | none |
| `rotate` | issues `cert`'s next generation into `<run>/tls`, valid `days`, with a mismatched key under `broken = true`, and repoints its `live` link; records the new serial and `notAfter`. See [TLS certificates](#tls-certificates) | none |
| `probe` | from the network namespace of the service it's `on`, handshakes with `127.0.0.1:<port>`, presenting the probe certificate, and records the served serial and `notAfter` and whether they verify against the CA for `cert`'s first DNS name | none |

A `mark`, `exit`, or `respond` window is a fault window like any other, so the checks excuse
what happens inside it. A `signal`, `rename`, `rotate`, or `probe` window excuses nothing: none
can lose data, so every row judges straight through it, and only `[[expect]]`, `file.reopen`,
`tls.served`, and `self_log`'s excusal of a broken rotation's failed reload read it. A `signal`
or `probe` may start inside a `rename` or `rotate` on its own service, and no other step may
overlap one of these on its service. None of these seven can go in a `[random]` schedule. A `respond` or a `stop` of
a stand-in may name the `class` the sink must read its answers as (`clean`, `ambiguous`,
`rejected`, or `refused`), which `ledger.standin` judges. Under the `datadog` and `influxdb`
shapes every `respond` other than a `2xx` must name one, because its class is the only statement
of what the sink should do with the answer, and a `2xx` takes none.

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
alone. On a TCP source the SUT's egress carries the ACKs, so `netem` on `logit` affects that
wire too, and `netem` on the generator delays or drops segments TCP retransmits. On an HTTP
source the SUT's egress carries its answers, and `netem` on either end can hold a request or its
answer past the writer's timeout, so both affect that wire. The driver
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

- The stack runs `kafka` in place of `victoria-metrics`, and needs no env file. A scenario's
  compose override may replace the broker's entrypoint and configuration, as
  `kafka-sasl-reauth`'s does, as long as it keeps the PLAINTEXT listener on `localhost:9092`:
  the healthcheck and the topic read-back connect there unauthenticated. A fault may name
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
  The consumer gets 180 s (`driver.py`'s `TOPIC_READ_TIMEOUT_S`), so a long run's topic must
  stay small enough to read in that time: `kafka-broker-restart` read 25,900 records in 4 s.
- The SUT's `aggregate` must run `temporality: delta`, because V is the sum of every distinct
  record's `sum` values: each record carries one window's increments. Records are deduplicated
  on the value's raw bytes first, so a duplicate can't cancel a loss of the same size. Each
  record, and each duplicate copy, counts toward the SUT life its timestamp falls in.
- The ledger reads the topic in the sink's `format:`, taken from the SUT config: `json`, the
  default, or `otlp_proto`. Any other format, or a sink that doesn't write the target's
  `topic`, is refused at load. Under `otlp_proto` the consumer prints each value escaped
  (Kafka's `BytesDeserializer`), and `soaklib/otlp.py` walks it as a metrics export request.
  V counts each `Sum` data point once per series and time, so a resend split another way
  can't count twice, and `ledger.topic` judges such a repeated point as it judges a duplicate
  copy. `format: otlp_json` isn't read.
- `ledger.egress` and `ledger.summary` judge Ab − V against the topic's distinct records. A
  surplus FAILs, as on any target. `ledger.windows` SKIPs.
- `ledger.sent` judges the sink's batches, drops, and refusals as for an external target, with
  `kafka_out`'s request classes (`ok` accepted).
- `ledger.topic` compares the topic's distinct record count with the records the sink saw
  accepted, and judges duplicate copies, and under `otlp_proto` repeated data points, per SUT
  life. Within one producer epoch the broker
  discards a resend, so a duplicate FAILs unless its life ended in a restart or kill, or the
  sink counted a drop that bumps the epoch there (`rejected`, `invalid_timestamp`,
  `invalid_record`, `ambiguous_at_most_once`). Those duplicates WARN, as
  [`docs/known-gaps/kafka.md`](../../docs/known-gaps/kafka.md) permits. A run with a killed SUT
  life WARNs, rather than FAILs, on a T − A difference in either direction, because a kill loses
  that life's last telemetry interval and a replayed spool batch counts twice in A.

## Stand-in targets

A scenario can point its one sink at a scripted HTTP destination, so every response its
response-class table distinguishes runs under load, on cue:

```toml
[target]
kind = "standin"
# shape = "datadog"        # or "influxdb": the destination it plays; "remote_write" by default

[[ledger.sink]]
id = "remote_write"        # the sink; no backend or vm_selector

[[step]]
at = "0s"
action = "respond"
on = "standin"
status = 400
body = "out of order sample"
class = "rejected"         # the class the sink must read; optional
for = "20s"
```

What changes for a stand-in target:

- The stack runs `standin` in place of `victoria-metrics`. The SUT writes to `standin:8080`.
  With no cue, the stand-in answers its shape's default: `200` on `/api/v1/write` for
  `remote_write`, `202` on `/api/v2/series` for `datadog`, and `204` with no body on
  `/api/v2/write` for `influxdb`. It compares the path without its query string, which it
  records apart (`query`). It answers `404` on any other path, and whatever the cue in force
  says otherwise. The driver cues it on its control port,
  published on loopback. A fault may name `standin` (`netem`, `pause`, `stop`, `restart`,
  `partition`), and only a stand-in takes `respond`.
- The record is the stand-in's stdout, `logs/standin.stdout`: one JSON line per request, with the
  cue in force, the answer, whether it stored the write, and the raw body. The checks decode it on
  the host with Python's standard library, which has no zstd before 3.14. So a `remote_write`
  sink must send remote-write 1.0 with `compression: snappy`, and `datadog_out` gzip (its
  default) or `none`. Point `datadog_out` at the stand-in with `endpoints: { api:
  http://standin:8080 }`, and `influxdb_out` with `url: http://standin:8080`; it sends no
  compression.
- Under `remote_write`, V is every sample the stand-in stored, a later write of one
  `(label set, timestamp)` replacing an earlier one, as a remote-write receiver overwrites.
  `ledger.sent` SKIPs.
- Under `datadog`, the SUT's `aggregate` runs `temporality: delta`, and V is the sum of every
  point the stand-in stored. Datadog documents no deduplication of a `count`, so a resent point
  is stored twice: V counts each distinct copy once, as a Kafka topic's records are, and
  `ledger.standin` reports each extra copy as a duplicate. A `series_errors` answer stores every
  series but the first N. `ledger.sent` judges the sink as on an external target, except that a
  refusal line inside a step with `class = "refused"`, a `rejected` batch inside one with
  `class = "rejected"`, an `oversize` record drop inside a `413` step classed `rejected`, and a
  `rejected` record drop inside another step classed `rejected` or a `series_errors` step are
  counted as cued instead of FAILing.
- Under `influxdb`, the SUT's `aggregate` runs `temporality: delta`, and each numeric or boolean
  field of a stored point is a sample of `{__name__: measurement, field: key, ...tags}`. InfluxDB
  overwrites an identical `(measurement, tag set, field, timestamp)`, and `influxdb_out`
  re-encodes a retried batch byte for byte, so a resend is a rewrite: V is the sum over distinct
  `(series, timestamp)` of the stored value, and a resend that stored another value FAILs
  `ledger.standin`. To carry a bucket or org as `influxdb_in` would, stamp the
  `_logit.influxdb.bucket`, `_logit.influxdb.org`, and `_logit.influxdb.org_id` resource
  attributes with a `set`. `ledger.sent` SKIPs.
- Under every shape, `ledger.egress` and `ledger.summary` judge V as an export, and so does
  `ledger.windows` under `[ledger] vm_every_window`.
- `ledger.standin` judges the record against the sink: every body decodes; every sample of a write
  answered `2xx` was stored; no resend stored a different value; per SUT life the record's
  requests by status class equal the sink's `logit.output.requests`, and its resets are at most
  the sink's `network_error`; every answer but the default carries a `respond` step's cue inside
  its span; a step with a `count` answered that many writes under its cue; and each step's
  `class` shows at the sink. `rejected` is a batch dropped `rejected`
  and no `retrying` line; `clean`, `refused`, and `ambiguous` are the `retrying` line's class
  word, `send failed (<class>)`, and no drop; under `at_most_once`, `ambiguous` is a batch dropped
  `ambiguous_at_most_once`. Under `datadog`, also: per SUT life the sink's
  `records.dropped{reason="rejected"}` equals the series the `series_errors` answers named plus the
  points of the requests answered under a step classed `rejected`, other than a `413`, and
  `records.dropped{reason="oversize"}` the points of the `413` answers under such a step; and a
  duplicate copy is reported under `at_least_once` and FAILs under `at_most_once`, or when the copy
  it repeats was answered `2xx` in the same life, a point the sink saw accepted and resent. Under
  `influxdb`, also: per SUT life no point of a request answered under a step classed `rejected`
  is stored, and the egress gap equals those points' sum; the writes name one
  `(org, orgID, bucket)`, which the row reports; and a rewrite is reported, and FAILs under
  `at_most_once` or after a `2xx` in the same life, as a duplicate does under `datadog`.
- A batch dropped `rejected` inside a `respond` step with `class = "rejected"`, up to one
  aggregate interval plus `recovery_bound` after it, counts its gap without FAILing
  `ledger.egress` or `ledger.summary`. Outside every such step it FAILs, as on any target.

To check that the ledger catches a destination that acknowledges and stores nothing, cue
`status = 200` with `apply = false`: `ledger.standin` FAILs it. `ledger.egress` can't be relied
on for it: under cumulative temporality the next stored totals overwrite the hole, and a
`rejected` drop in the same life credits any gap. `[ledger] vm_every_window = true` also
makes `ledger.windows` FAIL the windows it lost, but a rejected batch is a lost window too, so a
scenario with a `rejected` step can't set it.

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
   container that exits or restarts with no step behind it ends the schedule at once. So does a
   tick more than 120 s late, which means the host slept or the driver was stopped: the driver
   records a `stalled` phase and an `aborted` one, and applies no further step, since every
   overdue step would otherwise run back to back.
4. At the end it reverts active faults, stops every generator, waits until every sink's stored
   total holds across two aggregate windows, stops the SUT (expecting exit 0), and exports each
   sink's series from its backend. For an external target it waits for every SUT sink's queue
   to empty instead, and exports nothing. With several SUT services it stops each in chain
   order, as [Several SUT services](#several-sut-services) describes. After a stall it runs the
   same sequence, so the run directory is complete and `check` scores it like any other. Each
   wait has its own bound, so an aborted run ends within minutes of the host waking.
5. It collects the rest of each service's stdout and stderr separately with `docker logs`, and
   its final `docker inspect`, then tears the project down (`down -v --remove-orphans`) unless `--keep`.
   This step runs on every exit but SIGKILL: SIGINT (Ctrl-C), SIGTERM, and SIGHUP each stop the
   schedule and reach it, unless the driver started with that signal ignored, as `nohup` leaves
   SIGHUP. After a SIGHUP the driver drops its terminal output and carries on, because the run
   directory has everything.
6. It scores the run and exits with one of these statuses, which `script/soak` passes through:

   | Status | Run |
   |---|---|
   | 0 | every row passes or warns |
   | 1 | any `FAIL`, including a run the driver aborted or hit an error in |
   | 2 | the run never started: a scenario, argument, or setup error, such as a config `logit validate` refuses, a missing env file, a failed self-test, a failed image build, or a compose project that already has containers |
   | 3 | `stalled`: the host slept or the driver was stopped mid-run, and `run` FAILs. Rerun it once the host is awake |
   | 130 | SIGINT, SIGTERM, or SIGHUP interrupted it, and `run` FAILs |

   `check` on a run directory exits 0, 1, or 3 the same way. On an interrupted run it exits 1,
   because only the live run knows it was interrupted. `--retry-stalled N` reruns a stalled run
   up to N times, 30 s apart, each in a new run directory with the same duration and seed; it
   doesn't combine with `--keep`. The [ADR's stall amendment](../../docs/adr/soak-harness.md#amendment-a-run-the-host-slept-through-ends-at-its-first-stall-and-exits-3-2026-10-18)
   says why a stall ends the run and gets its own status.

A run writes `perf/results/soak/<UTC stamp>/` (gitignored, or under `SOAK_OUT`). When another run
in the same second has claimed that name, the directory is `<UTC stamp>-2`, then `-3`, and so on;
the run creates it with `mkdir`, so two runs never share one:

| File | Contents |
|---|---|
| `results.md`, `results.json` | one row per check, then each check's detail lines |
| `timeline.jsonl` | every phase, fault, revert, and readiness probe, with planned and actual times, `rc`, `stderr`, and `netem show` output |
| `watchdog.jsonl` | each container's `docker inspect` state every 5 s |
| `stats.ndjson`, `vm-freshness.jsonl` | the 30-second `docker stats` and per-sink freshness samples; no freshness samples for any target but the local one |
| `vm-export/<sink>.jsonl` | each sink's backend's `/api/v1/export` of the sink's `vm_selector` at the end; absent for any target but the local one |
| `logs/<service>.stdout`, `.stderr` | each service's output across every life of its container |
| `log-chunks.jsonl` | one record per `docker logs` call: its `--since`, `--until`, exit code, and bytes, or the end of its error when it failed |
| `inspect/<service>.json` | each container's final `docker inspect` |
| `provenance.txt`, `compose.env`, `scenario.toml`, `scenario.resolved.json`, `configs/` | what ran, on what host, from which commit, the seed of a random schedule, the expanded schedule, the role of each service, and a copy of each `logit` config and of the compose override |

## The checks

Each check skips fault windows, a fault's span plus the scenario's `recovery_bound`, where its
rule only holds in steady state. `rss_slope` alone skips a further `rss_settle` after each
window, an optional top-level duration (default `0s`). Set it when a service gives back a
fault's backlog memory in steps over minutes, so a fit over those plateaus beside the pre-fault
baseline slopes upward with no leak; `fanin` and `tcp-ingress` set `60s`, and
`partition-rss` `120s`. Keep it short enough
to leave each life a fit, 6 samples over 60 s: a window that swallows a life's every gap leaves
nothing to judge, and the row's coverage `WARN` says so. The plan's "The checks" has the full
rules.

The ledger follows each `[[ledger.source]]`, a generator and the SUT listener its lines reach
over the source's `wire`: `udp`, the default, one line per datagram; `tcp`, LF-terminated lines
on one connection; `file`, lines the SUT tails from a file the generator writes; or `http`,
requests to `influxdb_in` or `otlp_in` from `influxdb_out` or `otlp_out`. The loader checks
the wire against the listener's `type:` and `transport:` in the SUT config, and on a file or
HTTP wire the generator sink's kind. Each wire has its own symbols: G is the generator sink's
`output.messages` (its `events.delivered` on a file or HTTP wire), W the listener's
`input.datagrams`, `input.frames`, `input.lines`, or on HTTP `events.sent`, and on TCP F
counts the lines the listener's framer dropped before W counted them (`frames.dropped`),
counted wire loss. On HTTP G counts only the points the listener answered with a success, so
the row compares it with W less X, the events of a batch refused with `503` because no
consumer was open, which the client resends. A busy `503` takes nothing and counts the batch
in `logit.input.batches.deferred`: a retry, not loss. `influxdb_in` also counts its requests,
so `ledger.intake` holds its `ok` and `closed_consumer` write requests to its `batches.sent`
and its `busy` ones to `batches.deferred` in every SUT life a kill didn't end, and reports a
killed life unjudged; `otlp_in` counts none. A file wire is judged differently, because a tailer reads behind its writer: the run's
W − G against a replay band summed over the SUT's lives, a killed life's replay after its last
checkpoint and its unexported last drain interval, and a stopped life's split line per open
file. While the writer deleted no file, G is held to the lines the driver counts in the shared
directory at the end; once it deleted one, the count must not exceed G, the directory must hold
the files the writer keeps, and no SUT fault may last long enough for the writer to delete a
file the tailer hadn't read.
`ledger.wire` judges each source on its own, against the fault windows of its own
generator and of every service that isn't a generator that affect its wire, and
`ledger.intake` judges each listener's final life; each reports a line per source or listener
and takes the worst status.
From the edge into `aggregate` on, the listeners' counts are summed, because their events merge
there. Nodes between a listener and `aggregate` are listed in `sut_transforms`, and each must
send every event it receives or count it in `logit.component.events.discarded` (T_disc), as
`drop_stale` does. Every later row subtracts those discards, as it subtracts a script's: they
leave the residual, a stopped life's egress gap, `ledger.summary`'s path term, and the
end-to-end bound. A node that sends more events than it receives, such as `line_protocol`,
FAILs `ledger.edge` unless the source names it in `fan_out`:

- `fan_out = { points = 4 }` names a `line_protocol` node and the events it emits per event.
  `ledger.edge` judges its children, `events.sent − (events.received − events.discarded)` (the
  count [ADR `transform-fan-out`](../../docs/adr/transform-fan-out.md) gives), against k ×
  `events.received` less its `logit.transform.lines.dropped`, with no tolerance. Its own
  discards are its parents, which went on as children, so they aren't loss. The term holds only
  when every message yields k distinct points and carries no other payload: a message
  whose points merge, that has no point, or whose parent carries metrics or a span, which the
  node forwards beside its children, reads as a `ledger.edge` FAIL, since no counter counts
  such a message.
- With one, the rows that compare the listener's counts with `aggregate`'s or V read W′: each
  listener's W − D − B − X carried through its chain, k per event past the fan-out, less what
  each node dropped or discarded. R grows by the chain's fan-out, since a batch past the node
  holds k times the events, and so does a killed life's ingest rate.

`load()` refuses a `fan_out` below 2, on a node outside the source's `sut_transforms` or of
another kind than `line_protocol`, and under `[overlap]`.

```toml
[configs]
sut = "logit-sut.yaml"
generator = "logit-generator.yaml"              # one config per generator, under its service
generator-lines = "logit-generator-lines.yaml"

[[ledger.source]]         # one per generator
service = "generator-lines"                     # a service with soak.role generator
generator_input = "load"                        # its generate_in
generator_sink = "syslog"                       # its sink, G's source; see "Rolling overlap" for lists
sut_listener = "lines"                          # the SUT listener it reaches
wire = "udp"                                    # optional: udp (the default), tcp, file, or http
sut_transforms = ["parse", "labels", "count", "metrics_only"]   # optional: nodes to aggregate
# fan_out = { points = 4 }                      # optional: a line_protocol node's events per event
```

The ledger follows each `[[ledger.sink]]`. A row about a sink (`progress`, `identity.sink`,
`recovery`, `ledger.edge`, `ledger.egress`, `ledger.windows`, `ledger.replay`, and
`ledger.sent`) judges each sink, reports a line per sink, and takes the worst status. Under
`split = "copy"`, the default, each sink holds all of `aggregate`'s output, so each sink's V is
judged against Ab alone; under `split = "route"` each sink holds its target's share, so their
summed V is. `load()` refuses `split = "route"` when the SUT config has a `route` with a
list-valued `routes:` value, or any `route_signals`: each can deliver one event,
or parts of it, to several sinks, so their totals don't add up to what `aggregate` absorbed.

Use `split = "target"` for those routers. Each sink then sits behind its own target and its own
`aggregate`, after the router `[ledger] sut_router` names, a `route` or `route_signals`, and
`sut_aggregate` is refused. The listeners' rows end at the router, its received events (R_in).
The router balances as R_in + R_copied − R_disc == R_fwd + ΣT_sent: its own
`events.received`, `events.copied` (each delivery past an event's first, a `route_signals`
part included), and `events.discarded` (an event carrying no listed signal) against its own
`events.sent` (a `route`'s forward edge, its ordinary consumer) and each target's
`events.sent`. A target has no node: its `Fanout` counts its sends under its own id. Each
sink's rows start at its target, T_sent, less the target's closed-consumer drops, T_x. The
balance pools the targets, so `ledger.edge` also bounds each target's T_sent by R_in − R_disc,
and R_fwd by R_in: a target receives at most one delivery of each event the router kept. The
bounds catch a target receiving more than the router took in, not events sent to one target
in place of another below that. The router's targets must be the sinks' targets, every one,
since a target no sink follows would leave the balance short; no other `route`,
`route_signals`, or `lua` or `lua_file` with `targets:` may fill a sink's target, since its
T_sent would count sends the router never took in; and `sut_script`, a hop, `vm_every_window`, `[overlap]`, and any target
but the local one are refused.

```toml
[ledger]
split = "target"
sut_router = "split"      # the route or route_signals filling the targets

[[ledger.sink]]
id = "vm_logs"
target = "tier_logs"      # the target feeding this sink's branch
transforms = ["count_logs", "logs_metrics_only"]   # optional: nodes from the target to aggregate
aggregate = "window_logs" # this sink's own aggregate
backend = "victoria-metrics"
vm_selector = '{__name__="soak_log_lines_total"}'
```

```toml
[ledger]
split = "route"           # copy (the default), route, or target
# ...

[[ledger.sink]]           # one per SUT sink
id = "vm_a"               # the sink's component id
target = "tier_a"         # route and target only: the target feeding it
backend = "victoria-metrics"                                # local only: a backend service
vm_selector = '{__name__=~"soak_requests_[0-9]+_total"}'    # local only: the series V reads
```

| Check | `FAIL`s when |
|---|---|
| `run` | the driver aborted, hit an error, or was interrupted, its tick ran more than 120 s late (`stalled`, as when the host sleeps, which also ends the schedule there and exits 3), or the timeline lacks its `start` or `end_end` phase |
| `timeline` | a fault's apply or revert returned nonzero, the schedule failed fast, or a start or unpause of a `logit` service has no readiness probe; a fault whose apply failed gets no window |
| `exit` | a `logit` container exits outside a scheduled stop, restart, kill, or `exit` step, or with a code other than the one that action leaves (0, 137 for a kill, the step's `code` for an `exit`); an `exit` step sees no exit; or any container is OOM-killed |
| `restarts` | a container starts with no scheduled start or restart behind it: the revert of a `stop` or `kill`, an `exit` revert that started it, or a `restart` |
| `self_log` | stderr has an `exiting` line with a nonzero code, a panic, or an `ERROR` line other than a sink's `retrying`; sink fault lines outside a fault window `WARN`. Inside an `exit` step, its `exiting` line and an `ERROR` line keyed its `key` are expected, and FAIL the row when either is missing. A `tls_reload_failed` line FAILs too, except inside a `rotate` step with `broken = true` on its service, or, on a service that polls its TLS files, inside any other rotate on it with a `tls_reloaded` of the same component after it in that rotate |
| `ready` | a `logit` container's readiness word, from each image `HEALTHCHECK` result timed at its start, isn't `ok` outside a fault window; a `mark` with `ready` sees no sample with that word; `degraded` appears outside an `exit` step; or a container isn't ready within 30 s of a start. With no `HEALTHCHECK` results recorded, it judges Docker's health status instead |
| `progress` | a SUT sink delivers nothing in a `progress_window` outside fault windows with work queued (a hang), a `logit` container's telemetry goes quiet, or a sink's newest stored sample is 30 s old (a local target only); a sink still retrying past `recovery_bound` `WARN`s, and so do windows covering under 5% of the run after warmup |
| `rss_slope` | a `logit` service's resident memory grows faster than `rss_growth_mib_per_hour` in steady state, which here also excludes a scenario's `rss_settle` (default `0s`) past each fault window; a run shorter than an hour `WARN`s instead, and so do slopes covering under 5% of the run after warmup, a service whose every sample sat in a window included. Each service is judged on its own line, and `[expected]` can set its verdict (see [Expected verdicts](#expected-verdicts)) |
| `fd_slope` | a `logit` service's open file descriptors end, or sit after a recovery, more than `fd_growth` above the warmup median. Each service is judged on its own line, and `[expected]` can set its verdict |
| `ledger.wire` | for a UDP or TCP source, its generator's lines minus what its listener read, what the kernel dropped, and what its framer dropped, counted (G − (W + K + F)), exceeds `wire_loss_outside_faults` outside the reported fault windows that affect its wire, with one generator batch of tolerance per window edge, each judged run counting at 0 or more; or, on UDP, its generator's sink counts messages and datagrams apart, and on TCP it counts any datagram. On UDP that loss is by design and every affecting window is reported: netem on a generator, and a lifecycle fault or partition of the SUT or of a generator. On TCP only a stop, kill, restart, or `exit`, and a partition that moved the container's address, are reported; every other TCP-affecting window (netem on the SUT, a pause, a partition that kept the address) is judged, and a surplus in it beyond one generator batch per `ambiguous_at_most_once` drop the generator counted there, plus one per edge, FAILs. Netem on a TCP generator affects nothing and is judged as steady state. On HTTP, G − (W − X), only a kill or an `exit` of the SUT is reported, since either can lose its unexported counts; every other HTTP-affecting window (netem on either end, a pause, a stop, a restart, a partition) is judged, and a surplus in it beyond one generator batch per attempt the generator counted `logit.output.requests{class="network_error"}`, plus one per edge, FAILs: the writer sends one batch per request, one at a time, and resends it after a transport error, when the SUT may have taken it. A backend stop doesn't affect the wire: a busy `503` takes nothing. On TCP and HTTP, a boundary between two judged windows is no edge of a fault: G's interpolation errs there by up to the G that moved in the generator interval straddling it, many batches when acknowledgments bunch under one request in flight or a restart flushes its backlog, and the error shows as an opposite-signed pair in the two runs that touch. So the part of a surplus past its own run's allowance nets with the loss on the other side of that boundary, up to that G, and the detail line shows each transfer; the part inside the allowance is excused already and nets nothing, so it can't hide a loss next door, and runs that don't touch never net. On every wire, and in `ledger.overlap`, G is anchored at every generator drain, a drain with no G point at the running total, since a counter exports no point for a drain it didn't move in. A file source is judged otherwise: its run's W + F − G against a replay band per SUT life, and G against the lines counted in the shared directory when the writer deleted no file; once it deleted one, the count at or below G, the file count at the writer's retention, and a FAIL for any SUT stop, kill, restart, or `exit` during which the writer rotated `max_files − 2` times or more, or pause during which it rotated `max_files` times, since a paused tailer keeps its open files (the plan's §6 `ledger.wire` row). The detail lists the gap per source and per run of drain intervals, and the generator's own counted loss beside it |
| `ledger.overlap` | under `[overlap]` only, else no row: over the run, the generators' lines minus what every instance read or counted dropped (G − Σ(W + K + F)) exceeds, for each stop or restart of an instance, `ceil(used / 512) + 1` datagrams, `used` being the closing life's last `logit.input.receive_buffer.used.bytes`; or falls below 0 past the generators' end allowance. Each close's own gap is reported, not judged. A kill or `exit` of an instance, which leaves no final sample, or another fault that can lose UDP on its way in makes the row report its numbers and SKIP. See [Rolling overlap](#rolling-overlap) |
| `ledger.intake` | for any listener, the final SUT life's lines read minus dropped differ from the events sent plus bad lines (B: `messages.dropped{reason="malformed"}`, `frames.dropped{reason="undecodable"}`, and on UDP `lines_in`'s oversize lines); or an earlier life's read-but-unabsorbed residual summed over the listeners (W − D − B − X − Ab, X being the listeners' own counted event drops, less a script's or the `sut_transforms` nodes' counted discards; with a `fan_out`, W′ − Ab, W′ being each listener's W − D − B − X carried through its chain) falls outside [0, R], R being each UDP listener's receive queue plus 2 batches per listener, 65 more batches for each node between it and `aggregate`, and 65 for `aggregate`, each listener's share times its chain's product of k with a `fan_out`; or, for an `influxdb_in` on an HTTP wire, in any SUT life a kill didn't end, its `ok` and `closed_consumer` write requests don't sum to its `batches.sent`, or its `busy` ones differ from its `batches.deferred`. Under `split = "target"` the residual ends at the router (W − D − B − X − R_in), and a stopped earlier life's must be 0: its final drain runs after every data node has exited, and each sink's egress starts at its target, so nothing else judges it |
| `ledger.edge`, `ledger.aggregate` | in the final SUT life, the listeners' summed events sent (or, with `sut_script`, the script's) differ from those `aggregate` received; a node in a source's `sut_transforms` received a different number from those sent to it, or sent a different number from those it received less those it discarded (T_in − T_disc ≠ T_out), or a `fan_out` node's children, T_out − (T_in − T_disc), differ from k × those it received less the lines it dropped; a sink received a different number of events from those sent to it (under `copy` `aggregate`'s events sent, which a fan-out counts once per batch, and under `route` its target's); or `aggregate` received a different number from those it absorbed. Under `split = "target"` the path runs into the router instead of `aggregate`; the router's R_in + R_copied − R_disc differs from R_fwd + ΣT_sent, a target's T_sent exceeds R_in − R_disc, or R_fwd exceeds R_in, in the final life or any stopped earlier one; and per sink, its target's sends less its closed-consumer drops (T_sent − T_x) differ from those its first branch transform or its `aggregate` received, a branch transform breaks T_in − T_disc = T_out, its `aggregate` sends a different number from those the sink received, or, in `ledger.aggregate`, absorbs a different number from those it received, each on a line naming it |
| `ledger.script` | with `[ledger] sut_script` only, else SKIP: in the final SUT life, the listener's events sent differ from those the script received, those it received from those it emitted, discarded (`events.discarded`), and lost to an error (`events.dropped{reason="script_error"}`), or those it sent from those it emitted plus those `flush()` returned |
| `ledger.egress` | for each sink under `copy`, or for the sinks' sum under `route`: the final SUT life's absorbed increments minus the backend's reset-aware total (Ab − V), or a stopped earlier life's lines read minus dropped, bad lines, the listener's counted event drops, and that total (W − D − B − X − V), is nonzero (SKIP on an external target; on a Kafka target V is the topic's distinct records, and a negative gap is a surplus), other than a positive gap in a life whose telemetry counts a batch `batches.dropped` under any reason at a node that can explain it: any node but a sink outside the group, so under `copy` another sink's drop doesn't count a sink's gap, while one at the sink or upstream of the fan-out does (every reason counts; see "The ledger and identities" in the plan's [6. The checks](../../docs/plans/soak-harness.md#6-the-checks-w1a-w1b)); a `rejected` or `disk_corrupt` batch counts the gap but FAILs the life whatever its size, unless a `respond` step that expects `rejected` explains it; a killed life's Ab − V falls outside its band (below, a surplus: the residual plus one drain interval of ingest; above, uncounted: one aggregate interval plus one drain interval of ingest; with a `fan_out` the ingest is each listener's times its chain's product of k), with the residual the kill lost reported beside it; or a sink's export matched no series. With a script, its `flush()` events leave Ab, and its counted drops and errors leave a stopped life's gap, as the `sut_transforms` nodes' discards do (W − D − B − X − ΣT_disc − V, or W′ − V with a `fan_out`); the final drain exports them because `internal` drains last. Under `split = "target"` each sink is judged alone against its own `aggregate`'s Ab, and a stopped earlier life's gap starts at its target (T_sent − T_x − V, less its branch transforms' discards); a drop on another sink's branch, its transforms, `aggregate`, or sink, doesn't count its gap; and a killed life's band takes the ingest from its target's sends. A series without one segment per SUT life `WARN`s. Under `[overlap]` that is a known `WARN`: an instance the kernel hashes no flow to in a life writes no series in it, which with `rolling-overlap`'s eight flows happens about once in 256 rejoins; a series whose label names no instance `FAIL`s |
| `ledger.windows` | under `[ledger] vm_every_window = true` only, and SKIPs on an external or Kafka target: a series in a SUT life has two samples more than 1.5 aggregate intervals apart, a killed life's last sample is more than 1.1 intervals before the kill, or a later life's first is more than 2 intervals after its start. The interval comes from the SUT config. A wider gap is excused as idle when the `sut_aggregate` component's `batches.sent` is 0 over the drains from a + (interval + p) / 2 to b − (interval − p) / 2, p being the drain period: no window was emitted, so none was lost. A pause, with no drains at all, counts 0. Any batch there FAILs the gap as windows emitted but not stored, whatever fault was running |
| `ledger.replay` | the life after a killed one replays, in its first drain, a different number of batches from the killed life's last `buffer.batches`, beyond one in flight; across a native hop, a killed `logit`'s hop sink replays outside a band set by its received and delivered batch rates over its last drain interval, the time from that drain to the kill, and its spool's `checkpoint_interval` |
| `ledger.hop` | across a native hop only, else no row: over the run, the hop sink's events received, less its counted drops, minus the `logit_in`'s events forwarded isn't 0, beyond the allowance a kill or a pause earns: below, a killed side's last drain interval of counts, one send window of frames for a kill or a pause of the receiving side, and a killed `logit`'s spool replay of the frames it committed after its last cursor persist, when the receiving side restarted between that life's last delivery and the replay; above, a killed `logit`'s one batch in hand and a killed receiving side's last drain interval of counts. The plan's "The native hop (W10b)" has the rules. A `rejected` or `disk_corrupt` batch drop at the hop sink FAILs too |
| `ledger.summary` | the final life's uncounted loss, (W − D − E − B) + (E − A) + (A − Ab) + (Ab − V), isn't 0, or any one sink's Ab − V isn't, with a script's hop in place of E − A, E − ΣT_disc − A with `sut_transforms` (with a `fan_out`, `ledger.edge`'s chain terms summed, and the end-to-end bound's G carried through each chain), either path's discards shown as counted, and one Ab − V term per sink under `copy`; a counted egress term is shown and not judged, unless a `rejected` or `disk_corrupt` drop is in its group's credit, which FAILs the row; on an external target the Ab − V term SKIPs. Across a native hop E − A becomes E − S_hop in `logit`'s final life and E_B − A in `collector`'s, and the row FAILs a sink group whose V over the run exceeds G less every counted loss; so it does from a file source, plus its replay band, and from an HTTP source, plus one generator batch per transport-error attempt and with the listener's X, which the client resends, not counted. Under `split = "target"` the terms are W − D − E − B, every `ledger.edge` term, the router's balance among them, and per sink A − Ab and Ab − V, each judged on its own, so two terms of opposite sign can't net to 0; the router's term pools its targets, which `ledger.edge`'s bounds cover only above R_in − R_disc |
| `ledger.sent` | an external, Kafka, or `datadog` stand-in target only, else SKIP: a batch a sink received didn't end in an accepted response; a clean external run's detail starts with `SENT`. See [External targets](#external-targets) |
| `ledger.standin` | a stand-in target only, else SKIP: the record has a line that isn't JSON or a body that doesn't decode, a sample answered `2xx` wasn't stored, a resend stored a different value, its requests by status class differ from the sink's in a SUT life, an answer other than the default falls outside its `respond` step, a step with a `count` answered another number of writes under its cue, or a step's `class` doesn't show at the sink; under the `datadog` shape also a `records.dropped` count the record doesn't explain, or a duplicate under `at_most_once` or after a `2xx`; under the `influxdb` shape also a stored point of a rejected request, an egress gap other than the rejected requests' points, writes to more than one org and bucket, or a rewrite under `at_most_once` or after a `2xx`. See [Stand-in targets](#stand-in-targets) |
| `ledger.topic` | a Kafka target only, else SKIP: the topic wasn't read back; its distinct records number fewer than the sink saw accepted, or more with no dropped attempt to explain them (`WARN` with one, and `WARN` either way across a killed life); or a record is stored twice in a SUT life with no epoch bump (`WARN` after one). See [Kafka targets](#kafka-targets) |
| `identity.sink` | at the final life's last drain before its final one, a sink's batches received, plus those a disk spool replayed at open and the one it counts dropped with a torn tail (`batches.dropped{reason="disk_torn"}`, a batch an earlier life received), differ from delivered + dropped + queued by more than one batch in flight |
| `drops` | the SUT counts a `logit.*.dropped` no other row judges with a `rejected` or `disk_corrupt` reason; any other reason `WARN`s. The ledger's own terms (every `batches.dropped`, a node's `events.dropped` beside a batch drop of the same reason, `bytes.dropped`, a listener's D, K, X, and B, and the script's `script_error`) and, on an external, Kafka, or `datadog` stand-in target, a sink's `records.dropped` and `metrics.dropped` stay with the rows that balance them, and so does a TCP listener's F, which `ledger.wire` judges, so each drop is counted once. On an external target, where `ledger.egress` SKIPs, a `rejected` or `disk_corrupt` batch drop at a sink is `ledger.sent`'s FAIL, and one at any other node stays in this row |
| `recovery` | within `recovery_bound` of a fault's end, for any sink, no drain interval clear of other faults shows that sink not retrying, its buffer under 5% full or holding at most one batch, every UDP listener's receive queue under 5% full, and the listeners' summed ingest at 95% of the generators' summed warmup rate; a generator `rate_behind` diagnostic turns a rate shortfall into a `WARN`, and so does a run where no fault had an eligible interval |
| `file.reopen.<step>` | one row per `rename` step, else one `file.reopen` row that SKIPs: the rename or the SIGHUP inside it didn't apply; the renamed file is missing or has a line stamped more than 2 s after the signal's apply finished; the new file, the next rename's `to` or `path` after the last, is missing or has a line stamped more than 2 s before the signal started; or the lines in every file of the path's renames differ from the target's `events.delivered` over the service's lives, which a `kill` or an `exit` of it leaves reported, not judged. An empty new file `WARN`s. The plan's [6. The checks](../../docs/plans/soak-harness.md#6-the-checks-w1a-w1b) has the bounds |
| `tls.served.<step>` | one row per `probe` step, else one `tls.served` row that SKIPs: the probe didn't apply; the served serial or `notAfter` isn't the generation in force, the latest rotation of the certificate before the probe that wasn't `broken`, else the first issue; the served chain didn't verify against the run's CA; or the run recorded no first issue |
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
reduce = "max"                          # counters: delta, min_delta, rate; gauges: max, min, last, rise, slope
min = 2                                 # min, max, or both
```

`min_delta` is the smallest single drain's delta, so `min = 1` asserts a counter rose in every
drain of the window. `rate` is the window's delta divided by the window's length in hours, so
its bounds are a count an hour whatever the run's `--duration`. A gauge reducer includes the value in force at the window's start, because
`internal` exports a gauge only in a drain after it was set, taken from the SUT life running at
that start only. A bound that must hold for the whole episode, such as a loss counter that must
stay 0, uses `through`: a chain blocked behind a sink stays blocked until the sink's next retry,
up to its `retry_max_delay` after the revert. A scenario with no `[[expect]]` gets one `expect`
row that SKIPs.

`rise` is a gauge's value at the window's end less the value in force at its start, and the row
FAILs with no value in force there. `logit.tls.certificate.not_after` with `reduce = "rise"`
and `min = 86400` asserts a reload took a certificate valid a day longer at least, and `min = 0,
max = 0` that a failed one changed nothing.

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

A scenario that sets `max_duration` refuses a longer `--duration`, because its thresholds or
configs hold only that long; `tail-rotate`'s is 25 minutes. `tail-rotate-delete` has none:
its writer's retention bounds both the shared directory and the tailer's descriptors.

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

A host that suspends mid-run, such as a laptop with its lid closed, stalls the run. When the
host wakes, the driver ends the schedule, runs the end sequence, and exits 3 with `run` FAILed as
`stalled`. Rerun the scenario once the host is awake, or pass `--retry-stalled N` to rerun it
automatically.

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
