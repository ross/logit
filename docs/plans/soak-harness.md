---
created: 2026-10-08
updated: 2026-10-08
---

# Enabling plan: a soak harness — faults on a timeline, a watchdog, and a loss ledger

## Context

`logit` is measured for throughput (`script/perf`), interop (`script/victoria-interop`,
`script/splunk-interop`), and data shape (`script/shape-survey`), but nothing runs it for long
under faults. [ADR `soak-harness`](../adr/soak-harness.md) records why the harness is real
containers over ordinary config, one-shot netem, a second `logit` as the load source, and a
reset-aware ledger as the loss oracle. This plan is the build-out: the scenario format, the
compose layout, the driver, the checks, and the order the pieces land in. Read the ADR first.

Goals:

- Run real `logit` containers against generated and real sources and real targets, with network
  and lifecycle faults applied on a timeline from a scenario file.
- Report crashes, hangs, memory and file-descriptor growth, and **uncounted** data loss, each as a
  PASS, WARN, or FAIL row a later run can be compared against.
- Make a new scenario a directory of one TOML timeline and ordinary `logit` configs, with no
  driver change.

Non-goals:

- toxiproxy, or any proxy in the data path. Netem and Docker lifecycle actions only.
- High load as a goal. The first scenario runs 2,000 events/s; throughput is `script/perf`'s job.
- A CI cadence. The harness runs by hand, like the other out-of-CI harnesses.
- An external target before W5.
- Seeded random schedules before W4. `--seed` is accepted and recorded, and refused until then.
- Per-kind conservation rules beyond the first scenario's. They come with the scenarios that need
  them.

Stream key **`soak`**: branches `soak/w0`, `soak/w1a`, `soak/w1b`, then `soak/w2` through
`soak/w5`, a linear stack. PR stack only: nothing is merged by this workstream; Ross directs
merging.

Settled with Ross (2026-10-08): faults are an iproute2 image with `NET_ADMIN` applying `tc netem`
inside a target container's network namespace, plus Docker lifecycle actions (`pause`, `stop`,
`start`, `restart`, network disconnect and connect), with no toxiproxy in W1. The generator is a
second `logit` (`generate_in` into `statsd_out` over UDP), so no new Rust. The oracle is a
watchdog, a sent/dropped/stored ledger against VictoriaMetrics, and the three identities
[`internal-telemetry.md`](../design/internal-telemetry.md) documents: sink conservation, edge
conservation, and gauges that recover after a fault. Internal telemetry reaches the harness as
NDJSON on each `logit` container's stdout, and stderr carries the `--log-format json` self-log.
Runs are minutes first (10 to 20 minutes, a dense schedule), with duration a flag, on the local
Docker daemon only. Scenario files are TOML; the `logit` configs beside them stay YAML. W1 splits
into W1a (driver, collector, watchdog) and W1b (ledger, identities, recovery).

## Design

Each item names the workstream that builds it.

### 1. Layout (W1a)

```text
script/soak                      # bash: common.sh, build logit:soak + logit-soak-netem, validate
                                 # the scenario's logit-*.yaml in the image, exec soak.py
tools/soak/
  README.md
  compose.yaml                   # victoria-metrics, logit (SUT), generator; no container_name
  netem/Dockerfile               # debian:bookworm-slim + iproute2, ENTRYPOINT ["/netem.sh"]
  netem/netem.sh                 # set <spec> | clear | show, dev from `ip route show default`
  soak.py                        # CLI: run | list | check | self-test
  soaklib/
    scenario.py    load/validate/expand(duration) -> [Step]
    docker.py      Docker(prefix): compose(), inspect(), logs_to(), exec_(), run_oneshot(), port()
    faults.py      ACTIONS: name -> (apply, revert, affects_udp_ingress, affects_egress)
    driver.py      up, warmup, 1 s scheduler, watchdog polls, end sequence, collect
    collect.py     provenance, logs, inspect, stats, vm export, timeline/watchdog jsonl
    telemetry.py   NDJSON -> points; counter sums per process life; gauge series; stderr lines
    vm.py          export()/force_flush()/freshness() against the published loopback port
    checks.py      one function per check -> Result(id, status, detail)
    report.py      results.md / results.json
    selftest.py    embedded fixtures; validate() over every shipped scenario
  scenarios/statsd-vm/
    scenario.toml
    logit-sut.yaml
    logit-generator.yaml
```

Subcommands: `run <scenario> [--duration 20m] [--seed N] [--keep] [--out DIR]`, `list`,
`check <run-dir>`, and `self-test`, which also runs at the start of `run`, as
`survey_self_test` does for `script/shape-survey`. Environment: `SOAK_SKIP_IMAGE=1` reuses the
images, and `SOAK_OUT=<dir>` moves the results.

A run writes `perf/results/soak/<UTC stamp>/` (gitignored): `compose.env`, a copy of
`scenario.toml`, `scenario.resolved.json`, `provenance.txt`, `timeline.jsonl`,
`watchdog.jsonl`, `stats.ndjson`, `vm-freshness.jsonl`, `vm-export.jsonl`,
`logs/<svc>.stdout` and `logs/<svc>.stderr`, `inspect/<svc>.json`, `results.md`, and
`results.json`. `check <run-dir>` re-scores a run offline from these files alone.

Host prerequisites: Python 3.11 or later (`tomllib`). `Dockerfile.dev` has no Python, so the
self-test doesn't run in CI; only the config globs do.

### 2. The scenario schema (W1a)

The first scenario, `tools/soak/scenarios/statsd-vm/scenario.toml`:

```toml
name = "statsd-vm"
description = "generator -> statsd_in (UDP) -> aggregate 10s cumulative -> prometheus_out RW1 zstd -> VictoriaMetrics"
duration = "16m"          # warmup + one cycle + cooldown; --duration overrides
warmup = "60s"            # no faults: baseline rate, RSS, fds
cooldown = "120s"         # no faults at the end; >= recovery_bound
recovery_bound = "45s"    # retry_max_delay 10s + two aggregate windows + a drain
cycle = "13m"             # >= last recovery (11m45s) + 2 x progress_window

[configs]
sut = "logit-sut.yaml"
generator = "logit-generator.yaml"

[ledger]                  # component ids the checker reads; no rates here
vm_selector = '{__name__=~"soak_requests_[0-9]+_total"}'
generator_input = "load"
generator_sink = "statsd"
sut_listener = "statsd"
sut_aggregate = "window"
sut_sink = "victoria_metrics"
wire_loss_outside_faults = 0.0

[thresholds]
progress_window = "30s"
rss_growth_mib_per_hour = 64   # WARN under 1h, FAIL above
fd_growth = 8

# Step offsets are relative to each cycle's start; the cycle repeats while it fits before
# cooldown.
[[step]]
at = "0s"
action = "netem"
on = "logit"
args = "delay 200ms 50ms"
for = "120s"

[[step]]
at = "3m"
action = "netem"
on = "logit"
args = "loss 30%"
for = "90s"

[[step]]
at = "5m"
action = "netem"
on = "generator"
args = "loss 10%"
for = "60s"

[[step]]
at = "6m30s"
action = "stop"
on = "victoria-metrics"
for = "90s"

[[step]]
at = "8m30s"
action = "pause"
on = "logit"
for = "30s"

[[step]]
at = "9m30s"
action = "stop"
on = "logit"
for = "30s"

[[step]]
at = "10m15s"
action = "partition"
on = "logit"
for = "45s"
```

`validate()` enforces these rules:

- Every fault carries `for`, so its revert is scheduled with it. `netem clear` is a revert, never
  a step.
- No two faults overlap on one container.
- No netem on a container during its `stop`, `pause`, or `partition`, because the qdisc is lost
  with the namespace and `docker run --network container:<id>` needs a running target.
- No fault ends inside `cooldown`, and `cooldown` is at least `recovery_bound`.
- Actions and services are known. W1's actions are `netem`, `pause`, `stop`, `restart`, and
  `partition`, on `logit`, `generator`, or `victoria-metrics`.
- `--seed` is refused until W4 adds a `[random]` table.

The expected rate is never configured. It is measured from the generator's own `events.sent`
over the warmup.

### 3. Compose (W1a)

The project is `soak-<scenario>`, so two scenarios can run at once. The driver refuses a project
that already has containers, as `script/victoria-interop` does, and never prunes. An `x-logit`
anchor sets `image: ${SOAK_IMAGE}`, `restart: "no"` (a crash must stay observable),
`logging: {driver: json-file}`, and
`entrypoint: ["logit", "--log-format", "json", "run", "/config.yaml"]`.

| Service | Image and settings |
|---|---|
| `victoria-metrics` | `victoriametrics/victoria-metrics:v1.152.0`, `-retentionPeriod=100y`, `ports: ["127.0.0.1::8428"]`, so the host driver polls freshness every 30 s and dumps the final export with no checker container. The driver re-reads the port after any start of this service |
| `logit` (the SUT) | `depends_on: [victoria-metrics]` |
| `generator` | `depends_on: {logit: {condition: service_healthy}}` |

Configs mount from absolute paths in `compose.env` (`SOAK_SUT_CONFIG`,
`SOAK_GENERATOR_CONFIG`), which the driver passes with `--env-file`, because `sudo` strips the
environment. `up -d --wait --wait-timeout 120` waits on the image's `logit ready` `HEALTHCHECK`.
Nothing in a container writes to the run directory in W1, so it needs no `chmod 777`.

Collection uses `docker logs <id> >svc.stdout 2>svc.stderr`, not `docker compose logs`, which
merges the two streams. `docker logs` spans every life of a container that is stopped and started
but never recreated.

### 4. Netem scope (W1a)

Each netem change is a one-shot
`docker run --rm --network container:<id> --cap-add NET_ADMIN logit-soak-netem set|clear|show`.
The qdisc outlives the `tc` process, and nothing long-lived joins the target's namespace, so netem
stays out of `compose up --wait`. The driver appends `limit 100000` unless the spec sets a
`limit`, as headroom so netem's own queue never drops a packet: a delay on the generator at
2,000 packets/s, or the burst after an outage, can exceed netem's default `limit 1000`.

A root qdisc on a container's default-route interface shapes that container's egress only:

| `on =` | What it impairs |
|---|---|
| `logit` | the SUT's remote-write to VictoriaMetrics, not the statsd arriving at it |
| `generator` | UDP into the SUT |
| `victoria-metrics` | VictoriaMetrics's responses |

Each container has one peer in this scenario, so there is no `tc filter`. Per-peer `u32` filters
and `ifb` ingress shaping wait for a scenario that needs them.

A `partition` records the container's network `Aliases` before the disconnect and passes them
back with `--alias` on the reconnect, because `docker network connect` drops the service alias.

### 5. The driver loop and end sequence (W1a)

The driver is single-threaded on a 1 s tick. Every subprocess has a timeout: 60 s, and
`docker stop` gets its `-t` plus 15 s. `$DOCKER` is split with `shlex`, and `-n` is inserted
after `sudo` so an expired ticket fails loudly instead of prompting; `script/soak` primes the
ticket with `${DOCKER} version` first.

1. Setup: run the self-test; load, validate, and expand the scenario; create the run directory;
   write `compose.env` and the provenance (git SHA and dirty count, Docker version, image IDs and
   digests, `uname`, `nproc`, `MemTotal`, `net.core.rmem_max`, Python version, argv, seed).
2. Run `compose up -d --wait`, record the container IDs, and poll VictoriaMetrics `/health`.
3. Loop until `duration`:
   - Run due steps and reverts, each appended to `timeline.jsonl` (`planned_offset`,
     `started_at`, `finished_at`, `rc`, `stderr`, and `netem show` output).
   - After a start, restart, or unpause of a `logit` service, time its readiness with
     `docker exec <id> logit ready --admin http://127.0.0.1:9600`, bounded at 60 s, lifted from
     `tools/shape-survey/lib.sh`'s `start_logit`. Never exec into a paused container.
   - Every 5 s, `docker inspect` each container into `watchdog.jsonl`: `State.Status`,
     `State.Paused`, `State.ExitCode`, `State.OOMKilled`, `State.StartedAt`,
     `State.FinishedAt`, `RestartCount`, and `Health.Status` with its last log entry.
   - Every 30 s, `docker stats --no-stream` into `stats.ndjson` (an RSS series that still works
     when `internal` is wedged), and VictoriaMetrics freshness: the newest sample timestamp across
     `vm_selector` from `/api/v1/export` with `start` 120 s back.
   - **Fail fast:** a `logit` container leaving `running`, or a `StartedAt` change, with no step
     behind it ends the timeline at once. The end sequence and collection still run.
4. End: revert active faults; `docker stop -t 30` the generator; wait until the VictoriaMetrics
   total holds across two aggregate windows (90 s bound); `docker stop -t 60` the SUT, expecting
   exit 0; call VictoriaMetrics `/internal/force_flush`; write `vm-export.jsonl` from
   `/api/v1/export` with `match[]=<vm_selector>` and `start=0`.
5. Collect: `docker logs` per service into `.stdout` and `.stderr`, and `inspect/<svc>.json`.
   Then `compose down -v --remove-orphans`, unless `--keep`. A `try`/`finally` makes SIGINT still
   collect, and the driver routes SIGTERM and SIGHUP into SIGINT's `KeyboardInterrupt`, because
   Python's default for both ends the process without running `finally`.
6. Score with `checks.run_all(run_dir)` into `results.md` and `results.json`. Exit 1 on any FAIL
   or an aborted or errored run, and 130 when interrupted.

### 6. The checks (W1a, W1b)

A check reports PASS, WARN, or FAIL; WARN is for a trend a short run can't settle.
`telemetry.py` mirrors the NDJSON grammar in `crates/logit-outputs/src/ndjson.rs`, whose module
doc gains `tools/soak/` in its list of readers. Counters (`kind: sum`, delta) sum per **process
life**, split where `logit.process.uptime` decreases, with the number of lives cross-checked
against the timeline. Gauges are series.

A "fault window" below is a fault's span plus `recovery_bound` after it. A fault whose apply
returned nonzero has none, so nothing after it is excused.

**The watchdog (W1a):**

- `run`: the timeline has `start` and `end_end`, and no `aborted`, `error`, or `interrupted`
  phase. An interrupted run FAILs so its `results.md` can't read as a pass; the driver still
  exits 130 for it.
- `timeline`: every apply and revert returned 0, the schedule didn't fail fast, and every
  start or unpause of a `logit` service has a `ready` record. An early revert in the end
  sequence isn't probed, so it needs none.
- `exit`: every exit falls inside a scheduled stop with code 0, the final exit is 0, and
  `OOMKilled` is false.
- `restarts`: every `StartedAt` change matches a scheduled start or restart.
- `self_log`: FAIL on an `exiting` line with `code != 0`, a `key == "thread_panicked"` line, or
  the raw `thread '…' panicked at` text, which isn't JSON, so stderr is checked line by line.
  An ERROR line keyed `retrying` is expected inside a fault window and WARNs outside one. A
  WARN line keyed `send_failed`, or whose message is `degraded`, does the same. Any other
  ERROR line FAILs.
- `ready`: `Health.Status` is `healthy` outside fault windows, and a container is ready within
  30 s of each start. A paused container fails the image's `HEALTHCHECK`, so health is judged
  only outside fault windows.
- `progress`, which tells a hang from a slow drain. Outside fault windows, the SUT sink's
  `batches.delivered` increases in every `progress_window`; stdout NDJSON never gaps more than
  three `internal` intervals while the container runs unpaused; and every VictoriaMetrics
  freshness sample is under 30 s old. After a fault, `retrying` at 1 with `retries` rising and
  `buffer.batches` falling is a slow drain: PASS within `recovery_bound`, WARN beyond it. No
  deliveries with `retries` and `errors` flat while `buffer.batches` is above 0 or `inbox.full`
  rises is a **hang, FAIL**. Delivery windows covering under 5% of the run after warmup WARN,
  so a PASS that rests on a few windows says so.
- `rss_slope` and `fd_slope`: the least-squares slope of `logit.process.memory.resident.bytes`
  per life, over samples outside fault windows after warmup, against `rss_growth_mib_per_hour`.
  `logit.process.fds` at the end minus the warmup median is at most `fd_growth`, and returns to
  baseline at the first steady-state sample after each recovery. `docker stats` RSS is the
  fallback series. Judged slopes covering under 5% of the run after warmup WARN.

**The ledger and identities (W1b).** One unit runs through the ledger: a datagram is one line is
one event is one increment.

| Symbol | Source |
|---|---|
| G | generator `logit.output.messages{component=statsd}`; precondition: equals `logit.output.datagrams` |
| W | SUT `logit.input.datagrams{component=statsd}` |
| K | SUT `logit.input.kernel.drops` |
| D | SUT `logit.component.datagrams.dropped` under every reason, from telemetry, plus `shutdown` drops from stderr |
| B | SUT `logit.component.diagnostics{key=bad_line}` |
| E | SUT `logit.component.events.sent{component=statsd}` |
| A | aggregate `logit.component.events.received` |
| Ab | aggregate `logit.transform.metrics.absorbed` |
| V | the reset-aware total of `vm-export.jsonl`: add each series' first value, then every non-negative step, and on a decrease add the new value. Resets per series should equal SUT lives minus one, else WARN. The resets also split V per SUT life |

The generator sets `max_packet_bytes: "32"`, so one datagram carries one line of about 20 bytes,
and G uses `output.messages` (what the kernel took), not `events.sent`. While the SUT is down,
`statsd_out`'s per-batch name resolution fails `Clean` and nothing is sent.

**Which life a hop is judged in.** `internal`'s final drain runs when the shutdown signal fires
([`internal-telemetry.md`](../design/internal-telemetry.md), "`internal`: the drain"), and no
drain exports the work after it: the listener decodes what its receive queue still holds and
flushes its accumulator (`FlushReason::Shutdown`), `aggregate` absorbs those events, and its
close-time window still reaches VictoriaMetrics. Shutdown drops land after it too, so they come
from stderr: the listener's `warn` lines go into D, and the `drain complete` line's
`batches_dropped` (batches, across every node) goes beside `ledger.egress`.

- The final SUT life ends after the generator stops and the VictoriaMetrics total holds (§5's
  end sequence), so nothing arrives after its final drain. `ledger.intake`, `ledger.edge`,
  `ledger.aggregate`, and `ledger.egress` are judged with no tolerance for it.
- An earlier SUT life ends under load, at a scheduled `stop` or `restart`. Its hops are compared
  at its last drain and reported, and judged only through `ledger.egress` and the life's sum
  W − D − B − Ab, which must lie between 0 and the residual R that can be in flight at the
  signal: the receive queue (`receive.max_datagrams`) plus 67 batches of at most
  `receive.batch_max_events` each: the accumulator, the batch the listener holds while its send
  waits for a slot on a full inbox, `aggregate`'s 64-slot inbox, and the batch `aggregate` has
  taken off that inbox but not yet absorbed.

The rows:

- `ledger.wire` is G − (W + K). This loss is uncounted **by design**: UDP on the network, netem
  drops, and the kernel receive queue at socket close
  ([`docs/known-gaps/intake.md`](../known-gaps/intake.md), "UDP intake"). It is reported,
  bucketed by drain timestamp, and fails only when the loss outside UDP-affecting windows (netem
  on `generator`; any `logit` stop, pause, restart, or partition) exceeds
  `wire_loss_outside_faults`. At the end, G can fall short of W + K, because what the
  generator's sink sends after the generator's final drain is never exported, and a send
  cancelled at shutdown loses its counts. A negative gap of up to the generator sink's
  `buffer.batches` at its last drain plus one batch (100 lines each) is allowed.
- `ledger.intake`: W − D == E + B, for the final life.
- `ledger.edge`: E == A, a single-consumer edge, for the final life.
- `ledger.aggregate`: Ab == A, for the final life.
- `ledger.egress`, per life: Ab − V, with V split at each series' reset. A gap of 0 passes, and
  so does a gap between −R and 0 for an earlier life, whose close-time window reached
  VictoriaMetrics after its last drain. A positive gap is lost increments. It is reported with
  that life's `drain complete` `batches_dropped` beside it, and is **counted** only when that
  count is above 0. The two are never reconciled: `batches_dropped` counts batches, and under
  `temporality: cumulative` a dropped batch loses only the increments since its series' last
  delivered total, which no log line states. Any other gap is **uncounted loss, FAIL**.
- `identity.sink`: at the last quiet SUT drain before shutdown,
  `batches.received == batches.delivered + batches.dropped (every reason) + buffer.batches`,
  allowing one batch in flight. Checked there, not at exit, because the sink's last delivery and
  its shutdown drops land after the final drain.
- `recovery`: within `recovery_bound` of each fault's end, `retrying` is 0,
  `buffer.utilization` and `receive.utilization` are under 0.05, and the ingest rate is at least
  95% of the warmup baseline. A generator `rate_behind` diagnostic makes the throughput row WARN,
  because the generator limited the rate, not the SUT.
- The summary row, for the final life: uncounted = (W − D − E − B) + (E − A) + (A − Ab) + (Ab − V),
  which must be 0, with each term shown beside it and wire loss beside them as "by design". When
  `ledger.egress` is counted, its term is shown as counted and the row judges the other three.

### 7. The first scenario's configs (W1a)

The SUT:

```yaml
# logit-sut.yaml
admin: { bind: 127.0.0.1:9600 }     # the image HEALTHCHECK's probe address
components:
  statsd: { type: statsd_in, bind: 0.0.0.0:8125 }   # receive: defaults (drop_oldest)
  window:
    type: aggregate
    sources: [statsd]
    interval: 10s
    temporality: cumulative
    series_retention: 360           # a fault must never evict a series
  victoria_metrics:
    type: prometheus_out
    sources: [window]
    endpoint: http://victoria-metrics:8428/api/v1/write
    version: 1
    compression: zstd               # buffer: defaults (block, at_least_once, retry_max_delay 10s)
  self: { type: internal, interval: 5s, span_sample_rate: 0.0, logs: off }
  telemetry: { type: stdio_out, sources: [self], format: json }
```

The generator:

```yaml
# logit-generator.yaml
admin: { bind: 127.0.0.1:9600 }
components:
  load:
    type: generate_in               # unbounded
    rate: 2000
    batch: 100
    event:
      metric: { name: "soak_requests_{seq%100}", kind: sum, value: 1 }
  statsd:
    type: statsd_out
    sources: [load]
    endpoint: logit:8125
    format: statsd
    max_packet_bytes: "32"          # one line per datagram: the ledger's unit
    buffer: { overflow: drop_newest, retry_max_delay: 1s }   # never stall generate_in
  self: { type: internal, interval: 5s, span_sample_rate: 0.0, logs: off }
  telemetry: { type: stdio_out, sources: [self], format: json }
```

`series_retention` counts aggregate windows, so 360 keeps an idle series for an hour, longer than
any fault in the schedule. The SUT's address can change across a stop and start; `statsd_out`
resolves `logit` once per batch, so the generator follows it.

### 8. Not in this stack

toxiproxy and TCP-level faults it alone can express; per-peer `tc filter` and `ifb` ingress
shaping until a scenario needs them (W2 at the earliest); a checker container; any CI job.

## Workstreams

| # | Branch | PR title | Size | Depends on |
|---|---|---|---|---|
| W0 | `soak/w0` | `soak/w0: ADR and plan for a soak harness` | S | — |
| W1a | `soak/w1a` | `soak/w1a: the soak driver, collector, and watchdog, with the statsd-vm scenario` | M | W0 |
| W1b | `soak/w1b` | `soak/w1b: the soak loss ledger, identities, and recovery checks` | M | W1a |
| W2 | `soak/w2` | `soak/w2: sink-outage and UDP-flood soak scenarios` | M | W1b |
| W3 | `soak/w3` | `soak/w3: disk-spool kill -9 replay soak` | M | W2 |
| W4 | `soak/w4` | `soak/w4: seeded random schedules and hours-long soaks` | M | W3 |
| W5 | `soak/w5` | `soak/w5: an external Datadog target for soaks` | M | W4 |

- **W0**: [ADR `soak-harness`](../adr/soak-harness.md), this plan, and a row in each README.
- **W1a**: `script/soak`, `compose.yaml`, the netem image, `scenario.py`, `docker.py`,
  `faults.py`, `driver.py`, `collect.py`, and `report.py`; `telemetry.py` (counter sums per
  process life, gauge series, and the stderr line classifier); `vm.py` (`freshness()`,
  `export()`, and `force_flush()`); `checks.py` with the watchdog checks; `selftest.py` with
  fixtures for NDJSON with two lives and a `panicked at` line; `run`, `list`, `check`, and
  `self-test`; the `statsd-vm` scenario. The globs in `script/validate` and in
  `every_shipped_config_loads_and_validates` (`crates/logit-cli/src/config.rs`, whose
  `["tools/victoria-interop", "tools/splunk-interop"]` loop gains `tools/soak/scenarios/*/`);
  `tools/soak/README.md`; an AGENTS.md "Harnesses" entry and Environment table row; the
  `ndjson.rs` reader list; a first run recorded under "Findings".
- **W1b**: the ledger, `identity.sink`, and `recovery` checks, and the stderr shutdown-drop
  reductions (the listener's `warn` lines and `drain complete`'s `batches_dropped`); self-test
  fixtures for a VictoriaMetrics export with resets and a stderr log with shutdown drops; a
  16-minute run recorded under "Findings".
- **W2**: sink-outage variants, the verification
  [`buffered-sink-delivery.md`](buffered-sink-delivery.md) describes (a 90 s stop under `block`,
  then `drop_oldest` with a small `max_batches`), and a UDP flood with the sink stopped,
  [`decoupled-listener-io.md`](decoupled-listener-io.md)'s deferred soak. Per-peer `tc filter`
  if a scenario needs it.
- **W3**: disk-spool `kill -9` replay, from [`durable-sink-buffer.md`](durable-sink-buffer.md): a
  `kill` action, a named volume for `buffer.disk.path`, SIGKILL lives in the ledger (the last
  5 s or less of telemetry is lost), and `buffer.disk.replayed` above 0 once, then 0.
- **W4**: seeded random schedules (a `[random]` table), hours-long runs (chunked
  `docker logs --since/--until`), and running on the perf VM through `script/vm push`.
- **W5**: an external target (Datadog) with a gitignored env file, as in `script/splunk-interop`'s
  cloud mode. The ledger ends at the sink's telemetry (`SENT`), with no backend query.

Landing order: W0 → W1a → W1b → W2 → W3 → W4 → W5, linear. Each PR is based on and targets its
parent's branch and is brought up to date with `git merge origin/main`, never a rebase.

## Findings

W1a records its first run here, and W1b records a 16-minute run with every check. Each records
the commit, image tags, host, and the verbatim `results.md` table. Known limits that stay true
across runs:

- UDP wire loss is by design and reported, not judged, outside the threshold on windows with no
  UDP-affecting fault.
- The listener's shutdown flush, `aggregate`'s close-time window, and shutdown drops land after
  `internal`'s final drain, so the per-hop ledger is exact only for the final SUT life, and
  shutdown drops come from stderr.
- The self-test doesn't run in CI.

### W1a: the first `statsd-vm` run (2026-10-08)

Run `20261008T202436Z`: `script/soak run statsd-vm` at the scenario's 16 minutes, from
`soak/w1a` at `03724bae` with documentation edits uncommitted. Images: `logit:soak`
(`sha256:78a95c90bb4a`, built from the same Rust), `logit-soak-netem:local`, and
`victoriametrics/victoria-metrics:v1.152.0`. Host: Fedora, Linux 7.1.13, 32 CPUs, 128 GiB,
Docker 29.7.2, Python 3.14.7, `net.core.rmem_max` 4194304.

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 14 apply/revert action(s) returned 0; 2 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 3 exit(s) of the logit services, each a scheduled stop with code 0 |
| `restarts` | PASS | 2 start(s), each a scheduled start or restart |
| `self_log` | PASS | 5 sink fault line(s) inside fault windows, 0 outside, 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 92 health sample(s) healthy outside fault windows; 2 start(s) or unpause(s) ready in time, slowest ready 0.1s |
| `progress` | PASS | 7 30s window(s) with deliveries outside fault windows; 8 freshness sample(s) under 30s; 180s of 900s after warmup judged (20%; WARN under 5%) |
| `rss_slope` | PASS | MiB/h per service/life: logit/1 -15.0, generator/0 -16.7 (limit 64); logit 190s of 900s after warmup judged (21%; WARN under 5%); generator 200s of 900s after warmup judged (22%; WARN under 5%) |
| `fd_slope` | PASS | warmup median -> end: logit 12->12, generator 11.5->11 (limit +8) |
| `ledger.wire` | SKIP | not built yet |
| `ledger.intake` | SKIP | not built yet |
| `ledger.edge` | SKIP | not built yet |
| `ledger.aggregate` | SKIP | not built yet |
| `ledger.egress` | SKIP | not built yet |
| `identity.sink` | SKIP | not built yet |
| `recovery` | SKIP | not built yet |

What the timeline did:

- All seven faults and their reverts ran within 0.4 s of their planned offsets, each with
  `rc == 0`. Each netem `show` matched its step (`limit 100000 delay 200ms 50ms`, `loss 30%`,
  `loss 10%`), and each clear left `noqueue`. The partition recorded and restored the aliases
  `logit` and `soak-statsd-vm-logit-1`.
- After the unpause and after the start, `logit ready` answered on the first probe (0.1 s).
- The SUT's 200 ms delay and 30% loss on its remote-write logged no `retrying` line: TCP
  retransmission absorbed both. The five sink fault lines are the SUT's `retrying` during the
  VictoriaMetrics stop and the partition, and the generator's while the SUT was stopped or
  partitioned (`logit:8125` doesn't resolve then). The SUT logged `recovered` after each.
- The end sequence's quiet wait held after 20 s, the SUT stopped with exit code 0, and the export
  held 100 series.

What was surprising, and how the driver changed for it:

- `aggregate` stops writing a series once it's idle; `series_retention` keeps the series' state,
  not its output. So once the generator stops, no newer samples arrive to mark aggregate windows.
  The quiet wait holds the total for two windows measured from the stored samples' spacing
  (10 s here) instead of waiting for two newer sample timestamps, which a 5-minute smoke run
  showed never arrive.
- Fault windows plus `recovery_bound` cover most of the cycle. At 11 minutes, each cycle left
  15 s of steady state, too short for a `progress_window`, so a long run would have judged
  `progress` only in warmup and cooldown. The 13-minute cycle leaves 75 s after the last
  recovery. In this run `progress` judged 20% of the time after warmup, and `rss_slope` judged
  the SUT only in its second life (190 s); its first life had three steady-state samples.
- A run shorter than warmup, one cycle, and cooldown keeps only the steps that end before
  cooldown starts: the 5-minute smoke run (`20261008T192852Z`) ran the first netem step only,
  and a 4-minute run runs none.

Two negative controls:

- Run `20261008T194140Z` (4 minutes, no faults): `docker kill -s KILL` of the SUT 72 s in ended
  the schedule at the next watchdog poll ("logit is exited (exit code 137) with no step behind
  it"). `exit` FAILed ("logit exited at +72s with code 137, outside every scheduled stop"),
  `timeline` FAILed on the fail-fast stop, and the script exited 1.
- Run `20261008T204136Z` (4 minutes, no faults): SIGTERM to the driver 91 s in recorded
  `interrupted`, collected the logs, and took the project down within 1.4 s, leaving no
  `soak-statsd-vm` container. `results.md` was written, `run` FAILed ("interrupted at +91s; no
  end_end phase"), `exit` FAILed on the SUT left running, and the script exited 130.

## Verification

- **W0** (this PR) is documentation only: `crates/logit-cli/tests/doc_links.rs` passes, and
  `docs/adr/README.md` and `docs/plans/README.md` each gained a row.
- **W1a**:
  - `script/soak self-test` passes, and `script/soak list` shows `statsd-vm`.
  - `script/soak run statsd-vm` at its own duration on a dev machine: the stack comes up under
    `--wait`; every step and revert appears in `timeline.jsonl` with `rc == 0` and a matching
    `netem show`; `docker logs` yields separate NDJSON stdout and JSON stderr; `results.md` has
    every watchdog check.
  - A `docker kill -s KILL` of the SUT mid-run FAILs `exit`. Documented in the PR body.
  - `script/validate` and `script/check` pass with the new globs, and `doc_links.rs` passes for
    the README.
- **W1b**:
  - The self-test fixtures pass, and a 16-minute run's `results.md` has every ledger row.
  - Two negative controls, each run once and documented in the PR body:
    - A final total lost at shutdown, as a control scenario the driver runs: a `stop` on
      `victoria-metrics` spanning a scheduled `stop` on `logit`, with the SUT sink set to
      `buffer: {shutdown_grace: 1s}`, so the driver owns the VictoriaMetrics start and the port
      re-read. The batches holding that life's last totals drop at shutdown, `drain complete`
      reports them, that life's `ledger.egress` reports a positive gap as counted, and its
      W − D − B − Ab stays within [0, R].
    - A wrong `vm_selector` FAILs `ledger.egress` rather than passing with nothing to compare.
- **W2 through W5**: each new scenario passes `self-test` validation and the shipped-config test,
  and a run of it is recorded under "Findings".
