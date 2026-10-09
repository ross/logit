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
- The perf VM. Hours-long runs use the development host (see W4).
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
  scenarios/statsd-vm-lost-total/  # W1b's negative control: a final total lost at shutdown
    scenario.toml
    logit-sut.yaml
    logit-generator.yaml
  scenarios/sink-outage-block/          # W2: each a short cycle with [[expect]] tables
  scenarios/sink-outage-drop-oldest/
  scenarios/udp-flood-sink-stop/
  scenarios/udp-flood-sink-stop-block/
  scenarios/spool-kill-replay/          # W3: a SIGKILL inside a sink outage, a disk spool
  scenarios/random-faults/              # W4: a seeded random schedule, meant for hours
```

Subcommands: `run <scenario> [--duration 20m] [--seed N] [--keep] [--out DIR]`, `list`,
`check <run-dir>`, and `self-test`, which also runs at the start of `run`, as
`survey_self_test` does for `script/shape-survey`. Environment: `SOAK_SKIP_IMAGE=1` reuses the
images, and `SOAK_OUT=<dir>` moves the results.

A run writes `perf/results/soak/<UTC stamp>/` (gitignored): `compose.env`, a copy of
`scenario.toml`, `scenario.resolved.json`, `configs/` (each `logit` config, so the ledger reads
the SUT's `receive:` limits), `provenance.txt`, `timeline.jsonl`,
`watchdog.jsonl`, `stats.ndjson`, `vm-freshness.jsonl`, `vm-export.jsonl`,
`logs/<svc>.stdout` and `logs/<svc>.stderr`, `log-chunks.jsonl` (W4: one record per `docker logs`
chunk), `inspect/<svc>.json`, `results.md`, and `results.json`. `check <run-dir>` re-scores a run offline from these files alone.

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
- No netem on a container during its `stop`, `kill`, `pause`, or `partition`, because the qdisc
  is lost with the namespace and `docker run --network container:<id>` needs a running target.
- No fault ends inside `cooldown`, and `cooldown` is at least `recovery_bound`.
- Actions and services are known. W1's actions are `netem`, `pause`, `stop`, `restart`, and
  `partition`, on `logit`, `generator`, or `victoria-metrics`. W3 adds `kill`, on `logit` only:
  the lines a killed generator sent after its last telemetry drain reach the SUT with no G
  behind them, and `ledger.wire` would net them against real loss.
- `[ledger] vm_every_window`, optional, is `true` or `false`.
- A scenario has `cycle` with `[[step]]` tables or a `[random]` table (W4), never both.
  `--seed` is refused for a fixed schedule, because no seed changes it.

**Random schedules (W4).** A `[random]` table replaces `cycle` and `[[step]]`:

```toml
[random]
seed = 20261009                       # --seed overrides
gap = { min = "2m", max = "4m" }      # from one fault's end, or warmup's, to the next start

[[random.fault]]                      # one table per allowed fault
weight = 3                            # picked in proportion to its weight
action = "netem"
on = "logit"
args = ["delay 200ms 50ms", "loss 30%", "delay 1s", "rate 1mbit"]   # netem only; one per draw
for = { min = "60s", max = "3m" }
```

`expand(scenario, duration, seed)` draws from `random.Random(seed)`, one fault at a time from
the end of warmup: a gap, a template by weight, its `for`, and for netem one of its `args`, each
a whole number of seconds. It stops at the first fault that would end after cooldown starts, so
a shorter run's schedule is a prefix of a longer one's for the same seed. Steps are `r1`, `r2`,
and so on, and are ordinary steps: the driver runs them, `scenario.resolved.json` records them
with the seed, and `check` re-scores the run from them. The draws use `Random.random()` only,
whose sequence for an integer seed Python keeps stable across versions, and the self-test pins
the shipped scenario's first three faults, so `--seed N` reproduces a recorded run.

One fault at a time is how a random schedule meets every rule above by construction: no two
faults overlap on any container, so none overlaps another on its own, and no netem lands in a
namespace fault. `validate()` checks the `[random]` table, then expands the schedule at the
scenario's own duration and the run's and checks each step against the same rules. Its rules
for the table:

- `seed` is an integer 0 or more; `gap` and each `for` are `{ min, max }` durations with min at
  most max, and `for.min` above 0; `weight` is a number above 0; actions and services are known,
  and `kill` is on `logit` only; a netem template has `args`, none of them a clear, and no other
  action has `args`; unknown keys are refused.
- `gap.min` is at least `recovery_bound + 2 x progress_window`, so every fault's recovery is
  followed by at least two judged progress windows, and `cooldown` is at least `gap.min`, for the
  last fault.
- The scenario's own duration fits warmup, `gap.max`, and the longest `for.max`, so every draw
  of the first fault fits.
- `[[expect]]` is refused: a row names a step, and a random schedule's steps change with the
  seed. The watchdog, the ledger, and `recovery` judge a random schedule.

With `random-faults`' ranges, 50 seeds at 8 hours draw 109 to 120 faults, and the time after
warmup outside every fault window is 51% to 56% of the run (45% to 50% in whole progress
windows), far above the 5% at which `progress` and `rss_slope` WARN. `gap.min` alone guarantees
at least `(gap.min − recovery_bound) / (gap.min + the longest for.max)`, 25% here; the self-test
holds every 8- and 24-hour schedule it draws to that.

**A `kill` (W3)** is `docker kill -s KILL` on the SUT, reverted by `docker start` with
the same readiness probe a `stop`'s revert gets. The process gets no shutdown signal, so it
writes no final `internal` drain, no shutdown drops, and no `exiting` or `drain complete` line,
and Docker records exit code 137. `validate()` treats it as a `stop`: it holds the container's
namespace, so no netem on that container during it, it is UDP-affecting on the SUT, and it may
not overlap another fault on its container. The watchdog accepts the exit inside a scheduled
`kill` with code 137 only, and counts the revert as a start for `timeline`, `restarts`, `ready`,
and the lives cross-check. The ledger pays for it: up to one `internal` interval of the killed
life's counters is lost, so that life's egress is judged within a band, not for equality (see "Which
life a hop is judged in").

**`vm_every_window` (W3)**, an optional `[ledger]` key, turns on `ledger.windows`: every series
has a sample in every `aggregate` window of every SUT life. Set it only when every series is
written in every window, because `aggregate` emits a series only in a window that updated it.

The expected rate is never configured. It is measured from the generator's own `events.sent`
over the warmup.

**Expectations (W2).** The watchdog and the ledger judge every scenario alike. A scenario that
asserts something of its own, such as a sink queue filling during an outage, adds `[[expect]]`
tables, each scored as one `expect.<name>` row:

```toml
[[expect]]
name = "sink-queue-fills"               # the row id: expect.sink-queue-fills
service = "logit"                       # whose telemetry: logit or generator
metric = "logit.component.buffer.batches"
component = "victoria_metrics"          # optional; joins `attrs`
attrs = { reason = "overflow_oldest" }  # optional attribute filters, all strings
step = "c0s1"                           # an expanded step id, or a 1-based [[step]] index
window = "during"                       # during | after | through
reduce = "max"                          # delta | min_delta | max | min | last
min = 2                                 # min, max, or both
```

| Field | Meaning |
|---|---|
| `step` | An expanded step id names one occurrence. An integer names that `[[step]]` table's occurrence in every cycle, and the row FAILs if any occurrence does |
| `window` | `during` is the fault's span, from its apply starting to its revert finishing; `after` is the `recovery_bound` after it; `through` is both, from the apply starting to `recovery_bound` after the revert finishes. Each is open at its start, because a drain's deltas cover the interval before its timestamp. A bound that must hold for the whole episode, such as a loss counter that must stay 0, reads `through`: a chain blocked behind a sink stays blocked until the sink's next retry after the revert, up to its `retry_max_delay` later, so `during` misses the tail |
| `reduce` | For a counter (`kind: sum`): `delta` sums the window's drains, and `min_delta` is the smallest single drain, a drain with no point counting 0, because a loss counter isn't emitted while it's 0. For a gauge: `max`, `min`, and `last` over the window's samples and the value in force at its start, because `internal` exports a gauge only in a drain after it was set. The value in force comes only from the SUT life whose drains cover the window's start, so a value an earlier process set doesn't carry into the next; a life that hasn't set the gauge yet leaves it unset. `last` is the value in force at the window's end |

`validate()` refuses an unknown key, a missing field, a `name` that isn't lowercase letters,
digits, `_`, or `-`, a repeated `name`, a `service` other than `logit` or `generator`, an
unknown `window` or `reduce`, a reducer of the wrong kind for a metric whose kind it knows,
no bound, a `min` above `max`, a bound that isn't a number, `component` set both alone and in
`attrs`, a `component` that isn't a string, and a `step` that names no `[[step]]` table or, at the scenario's own duration, isn't
in its schedule. A shorter `--duration` may drop the step; the row then SKIPs.

When the run is scored, the row FAILs when the reduced value is outside its bounds, the window
has no drain (a counter) or the gauge was never set by its end, the run's points for the metric
are of the other kind, or the step was scheduled but its apply didn't return 0. A scenario with
no `[[expect]]` gets one `expect` row that SKIPs.

### 3. Compose (W1a)

The project is `soak-<scenario>`, so two scenarios can run at once. The driver refuses a project
that already has containers, as `script/victoria-interop` does, and never prunes. An `x-logit`
anchor sets `image: ${SOAK_IMAGE}`, `restart: "no"` (a crash must stay observable),
`logging: {driver: json-file}` with `max-size: 20m`, `max-file: "5"`, and `compress: "true"` on every
service (W4; see "The driver loop and end sequence" for the arithmetic), and
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
but never recreated. From W4 the driver appends it in chunks during the run (§5).

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
`docker stop` gets its `-t` plus 15 s. The polls use shorter ones (W4): `docker inspect` and
`docker stats` 15 s, and a `docker logs` chunk 60 s. An inspect round, and a chunk round, stops at
its first failed call, so a daemon that stops answering costs each poll one timeout per round,
not one per container, and the polls can't stack into minutes of stall.

Every deadline is absolute, measured from the timeline's zero (W4): the tick is `t0 + n`, each
step runs at its offset, and each poll's next deadline is the next multiple of its period after
the poll ran. A late tick or a slow poll delays what follows it but never moves a later
deadline, so over hours the periods don't drift. `$DOCKER` is split with `shlex`, and `-n` is inserted
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
     `vm_selector` from `/api/v1/export` with `start` 120 s back. Each query covers those 120 s
     only, so its cost doesn't grow with the run.
   - Every 5 minutes (W4), `docker logs --since <cursor> --until <now − 5 s>` per container,
     appended to `logs/<svc>.stdout` and `.stderr` and recorded in `log-chunks.jsonl`. The
     boundaries follow the daemon's `json-file` reader: `--since` skips lines timestamped before
     it only until the first line at or after it, then passes every later line in file order,
     and `--until` stops at the first line timestamped after it. The next chunk's `--since` is
     the last `--until` plus 1 ns, so it starts at the line the last chunk stopped at, and the
     chunks partition the file by position, with no line repeated or skipped even where
     stdout's and stderr's timestamps interleave out of order. A line the daemon writes more
     than 5 s after its timestamp, behind the next chunk's start, would be lost; the daemon
     writes each line as it reads it. A chunk is written to temporary files and appended only
     when `docker logs` returns 0, so a failed chunk is retried whole from the same cursor.
     The cursor is per container id, which a stop and start keep.
   - **Fail fast:** a `logit` container leaving `running`, or a `StartedAt` change, with no step
     behind it ends the timeline at once. The end sequence and collection still run.
4. End: revert active faults; `docker stop -t 30` the generator; wait until the VictoriaMetrics
   total holds across two aggregate windows (90 s bound); `docker stop -t 60` the SUT, expecting
   exit 0; call VictoriaMetrics `/internal/force_flush`; write `vm-export.jsonl` from
   `/api/v1/export` with `match[]=<vm_selector>` and `start=0`.
5. Collect: the last `docker logs` chunk per service, from its cursor to the end, into
   `.stdout` and `.stderr`, and `inspect/<svc>.json`.
   Then `compose down -v --remove-orphans`, unless `--keep`. A `try`/`finally` makes SIGINT still
   collect, and the driver routes SIGTERM and SIGHUP into SIGINT's `KeyboardInterrupt`, because
   Python's default for both ends the process without running `finally`.
6. Score with `checks.run_all(run_dir)` into `results.md` and `results.json`, whose header
   names the seed of a random schedule. Exit 1 on any FAIL
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
- `exit`: every exit falls inside a scheduled `stop`, `restart`, or `kill` with the code that
  action leaves (0, or 137 for a kill), the final exit is 0, and `OOMKilled` is false.
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
| D | SUT `logit.component.datagrams.dropped` under every reason, from telemetry, plus `shutdown` drops from stderr, except in a killed life, which writes none |
| B | SUT `logit.component.diagnostics{key=bad_line}` |
| E | SUT `logit.component.events.sent{component=statsd}` |
| A | aggregate `logit.component.events.received` |
| Ab | aggregate `logit.transform.metrics.absorbed` |
| V | the reset-aware total of `vm-export.jsonl`: add each series' first value, then every non-negative step, and on a decrease or at a SUT life start add the new value. V splits per SUT life at each series' decreases and at each life start, placing a sample by its timestamp, which is the SUT's own window time; a restart whose first value isn't below the last one shows no decrease. A series should have one segment per SUT life, else WARN |

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
- An earlier SUT life ends under load, at a scheduled `stop`, `restart`, or `kill`. A life a
  `stop` or `restart` ended is compared at its last drain, and its hops are reported. Its residual W − D − B − Ab, what it read but hadn't absorbed
  by that drain, must lie between 0 and the residual R that can be in flight at the signal: the
  receive queue (`receive.max_datagrams`) plus 67 batches of at most
  `receive.batch_max_events` each: the accumulator, the batch the listener holds while its send
  waits for a slot on a full inbox, `aggregate`'s 64-slot inbox, and the batch `aggregate` has
  taken off that inbox but not yet absorbed. The close-time window delivers that residual into
  V, so `ledger.egress` judges the life's W − D − B − V as it judges the final life's Ab − V.
  The [0, R] bound only catches a residual no shutdown could leave.
- An earlier SUT life ended by a scheduled `kill` has no final drain, no close-time window, and
  no stderr shutdown lines. Its hops are compared at its last drain, which is an ordinary one,
  and reported; its residual must still lie in [0, R]. The kill loses that residual from the
  receive queue and inbox, by design, so `ledger.egress` judges Ab − V alone and reports the
  residual beside it, never with R. Two things bound Ab − V. Below: the residual and the reads
  after the last drain, up to one `internal` interval (5 s), are in no Ab, and an `aggregate`
  flush between that drain and the kill can carry them into V. Above: the kill discards
  `aggregate`'s unflushed window, whose events Ab counts up to the last drain and V never
  holds, and queue growth after the last drain can add up to one more drain interval. The band
  is therefore at least −(residual + rate × drain interval) and at most rate × aggregate
  interval + rate × drain interval, where the rate is the life's W over its last drain interval,
  the drain interval is the median of its drain gaps, and the aggregate interval is the SUT
  config's (the export's sample spacing only when the config names none). The detail states the
  band, both of its sides, and the rate. A gap below the band is a **surplus**, above it
  **uncounted**; both FAIL. There's no `drain complete` line, so a gap can't be counted. D takes
  nothing from stderr for this life, because a SIGKILL logs no shutdown drops.
- The band can't tell a lost last spooled window from the unflushed one when the life's last
  flush and its last drain fall together, and a lost middle window changes no total, because
  under `temporality: cumulative` V reads each life's last value. `ledger.windows` and
  `ledger.replay` catch both.
- What a killed life's sink had queued reaches VictoriaMetrics only if a later life sends it,
  as a disk spool's replay does. Those samples carry the killed life's own window timestamps,
  because `aggregate` stamps each flush with its own clock, so V credits them to the killed
  life by timestamp, not to the life that delivered them. `identity.sink` stays on the final
  life, whose shutdown is ordinary.

The number of SUT lives in telemetry must equal 1 plus the timeline's SUT starts: each `revert`
of a `stop` or a `kill` on `logit`, an end-sequence early one included, and each `apply` of a
`restart`, with `rc` 0. An unpause doesn't start a life. A life shorter than `internal`'s interval leaves at most
its shutdown uptime point, so the next life's first uptime isn't lower and the two merge; on a
mismatch every ledger, `identity.sink`, and `recovery` row SKIPs with the reason. They also SKIP when the SUT's telemetry has no
`logit.process.uptime` point.

The rows:

- `ledger.wire` is G − (W + K). This loss is uncounted **by design**: UDP on the network, netem
  drops, and the kernel receive queue at socket close
  ([`docs/known-gaps/intake.md`](../known-gaps/intake.md), "UDP intake"). It is reported,
  bucketed by drain timestamp, and fails only when the loss outside UDP-affecting windows (netem
  on `generator`; any `logit` stop, kill, pause, restart, or partition) exceeds
  `wire_loss_outside_faults` times the lines sent there, beyond a tolerance of one generator
  batch per window edge. The two processes' drains are out of phase, so G is interpolated
  linearly at each SUT drain timestamp; across a run of steady intervals the interpolation
  telescopes, and each edge of the run errs by at most one batch. Each steady run's gap counts
  at 0 when W + K exceeds G in it, so a surplus in one run can't net out loss in another. The
  generator sink's own
  `drop_newest` loss while the SUT is unreachable is counted and never in G; the row shows it
  beside the wire gap. At the end, G can fall short of W + K, because what the
  generator's sink sends after the generator's final drain is never exported, and a send
  cancelled at shutdown loses its counts. A negative gap of up to the generator sink's
  `buffer.batches` at its last drain plus one batch (100 lines each) is allowed.
- `ledger.intake`: W − D == E + B, for the final life.
- `ledger.edge`: E == A, a single-consumer edge, for the final life.
- `ledger.aggregate`: Ab == A, for the final life.
- `ledger.egress`, per life: Ab − V for the final life and a killed one, and W − D − B − V for
  a stopped earlier one, the residual plus Ab − V, because its close-time window delivers the
  residual to VictoriaMetrics after its last drain. The detail shows both parts. A gap of 0 passes. A
  positive gap is lost increments. It is reported with
  that life's `drain complete` `batches_dropped` beside it, and is **counted** only when that
  count is above 0. The two are never reconciled: `batches_dropped` counts batches, and under
  `temporality: cumulative` a dropped batch loses only the increments since its series' last
  delivered total, which no log line states. A killed life's gap passes inside its band (see
  "Which life a hop is judged in"). Any other gap is **uncounted loss, FAIL**.
- `ledger.windows`, only under `[ledger] vm_every_window = true`, else SKIP: per SUT life and
  series, consecutive samples are at most 1.5 × the SUT config's `aggregate` interval apart; a
  killed life's last sample is at most 1.1 × the interval before the kill, because the kill
  discards only the window in progress and 10% covers a flush's own lateness; and a later
  life's first sample is at most 2 × the interval after the life starts, the first window plus
  startup. A series with no sample in a life FAILs too. Each FAIL names the series and the gap.
  The interval comes from the config, never the median spacing, which moves once half the
  windows are missing. A last window lost to a kill that lands within 0.1 × the interval of the
  life's last flush passes, and the kill time is the `docker kill` call's start, which can run
  about a second ahead of the signal. A gap across a fault that silences the SUT is excused
  (W4): a `pause` of `logit` flushes nothing, and a `partition` of `logit`, or a `pause`, `stop`,
  or `partition` of the generator, stops lines arriving, and `aggregate` writes a series only
  in a window that updated it. The gap is excused only when it starts within 1.5 × the interval
  before the fault and ends within 2 × the interval after it, the bound a new life's first
  sample gets, so a window lost beside the fault still FAILs.
- `ledger.replay`, for each killed life followed by another, else SKIP: the next life's
  first-drain `buffer.disk.replayed` equals the killed life's `buffer.batches` at its last
  drain, one batch in flight allowed. `identity.sink` balances on whatever `replayed` reports,
  so only this row sees a replay two or more batches short of what the killed process queued;
  a single lost batch is `ledger.windows`'s to catch, as a lost window. A killed life with no
  disk spool replays 0, so this row FAILs whenever it had batches queued.
- `identity.sink`: at the last quiet SUT drain before shutdown,
  `batches.received + buffer.disk.replayed == batches.delivered + batches.dropped (every
  reason) + buffer.batches`, allowing one batch in flight. A disk spool opens holding the
  batches an earlier process queued, which this life delivers but never received. Checked there, not at exit, because the sink's last delivery and
  its shutdown drops land after the final drain.
- `recovery`: within `recovery_bound` of each fault's end, some SUT drain shows `retrying` at 0,
  `buffer.utilization` under 0.05, or under 1.0 with `buffer.batches` at most 1 (one batch in a
  2-batch queue is half of it, and one that fills its queue isn't drained), `receive.utilization` under 0.05, and an ingest rate over the drain
  interval ending there of at least 95% of the warmup baseline. Only intervals after the fault's
  end, inside one SUT life, and clear of every other fault are eligible, because a dense schedule
  often starts the next fault before `recovery_bound` runs out; a fault with no eligible interval
  is listed as skipped, and with no fault judged the row WARNs. A generator `rate_behind`
  diagnostic makes the throughput part WARN, because the generator limited the rate, not the
  SUT.
- `ledger.summary`, for the final life: uncounted = (W − D − E − B) + (E − A) + (A − Ab) + (Ab − V),
  which must be 0, with each term shown beside it and wire loss beside them as "by design". When
  `ledger.egress` is counted, its term is shown as counted and the row judges the other three.
- `expect.<name>`, one per `[[expect]]` table: a scenario's own bound on one metric over a
  window around one step; see "The scenario schema".

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
| W1b | `soak/w1b` | `soak/w1b: the loss ledger, sink identity, and recovery checks` | M | W1a |
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
  fixtures for a VictoriaMetrics export with resets and a stderr log with shutdown drops; the
  `statsd-vm-lost-total` control scenario; a 16-minute run and both negative controls recorded
  under "Findings".
- **W2**: `[[expect]]` tables in the scenario schema and the `expect` check that scores them
  (§2), with self-test fixtures for every validation rule and reducer; `recovery` accepts a sink
  queue holding one batch. Four scenarios: `sink-outage-block` and `sink-outage-drop-oldest`, the
  verification [`buffered-sink-delivery.md`](buffered-sink-delivery.md) describes (a 90 s stop
  under `block`, then `drop_oldest` with a small `max_batches`), and `udp-flood-sink-stop` and
  `udp-flood-sink-stop-block`, [`decoupled-listener-io.md`](decoupled-listener-io.md)'s deferred
  soak. A run of each is recorded under "Findings". No scenario needed per-peer `tc filter`.
- **W3**: disk-spool `kill -9` replay, the manual soak
  [`durable-sink-buffer.md`](durable-sink-buffer.md)'s "Verification" describes: a `kill` action
  (§2) on the SUT and its watchdog rules; SIGKILL lives in the ledger, judged within a band
  because up to one `internal` interval of a killed life's telemetry is lost; `ledger.windows`
  and `ledger.replay`, which judge every spooled window; self-test fixtures for the
  watchdog around a kill and a three-life NDJSON whose middle life ends by a kill; and the
  `spool-kill-replay` scenario, whose SUT spools to `/tmp/soak-spool` in its own container
  filesystem, which a `kill` and `start` keep, so it needs no volume. It sets `vm_every_window`,
  and its expectations assert `buffer.disk.replayed` above 0 once, at the restart, then 0, and
  no torn tail (`buffer.disk.truncated` 0 through the kill). A run of it, and a negative control
  without the spool, are recorded under "Findings".
- **W4**: seeded random schedules (a `[random]` table and `--seed`; "The scenario schema"),
  hours-long runs (chunked `docker logs --since/--until`, rotated `json-file` logs, absolute
  poll deadlines, bounded poll timeouts; "The driver loop and end sequence"), `check` fast
  enough for an 8-hour run, `ledger.windows` excusing gaps across faults that silence the SUT,
  and the `random-faults` scenario. The perf VM is dropped: long runs happen on the development
  host, which has 32 cores, 125 GB of RAM, 716 GB of free disk, and Docker's default `json-file`
  log driver with no daemon-level rotation, so compose sets each container's rotation.
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

### W1b: the ledger on a 16-minute `statsd-vm` run (2026-10-08)

Run `20261008T211129Z`: `script/soak run statsd-vm` at the scenario's 16 minutes, from
`soak/w1b` at `ba532233` with the ledger checks uncommitted, on W1a's images and host
(`logit:soak` `sha256:78a95c90bb4a`; no Rust changed). Every watchdog row matched W1a's run. The
ledger rows:

| Check | Status | Detail |
|---|---|---|
| `ledger.wire` | PASS | G 1,915,900 = W 1,844,096 + K 59,679 + wire 12,125 (by design: 12,107 in UDP-affecting windows, 31 steady, -13 at the end; steady limit 500); the generator's counted drop_newest loss, not in G: 16,400 event(s) in 164 batch(es) |
| `ledger.intake` | PASS | final life 1: W − D 643,600 vs E + B 643,600; life 0: W − D − B − Ab 0 within [0, R] (R 77,000 = 10,000 + 67 x 1,000) |
| `ledger.edge` | PASS | final life 1: E 643,600 == A 643,600 (listener to aggregate) |
| `ledger.aggregate` | PASS | final life 1: Ab 643,600 == A 643,600 (every event absorbed) |
| `ledger.egress` | PASS | life 0: W − D − B − V 0 = residual 0 + Ab − V 0, ok; life 1 (final): Ab 643,600 − V 643,600 = 0, ok (R 77,000 = 10,000 + 67 x 1,000) |
| `ledger.summary` | PASS | final life 1: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V 0; wire G − (W + K) 12,125 over the run, by design |
| `identity.sink` | PASS | final life 1 at +980s: received 26 vs delivered 26 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `recovery` | PASS | 7 of 7 fault(s) judged, each recovered within recovery_bound 45s of its end; warmup rate 2,000/s |

Where the generated lines went:

- The generator produced 1,932,300 lines. Its `statsd_out` dropped 16,400 of them (164 batches,
  `drop_newest`) while `logit:8125` didn't resolve during the SUT's stop and partition. The
  generator counts that loss, and it never reaches G.
- Of G's 1,915,900 lines, the SUT's kernel dropped 59,679 (K) while the SUT was paused and its
  receive buffer filled. That loss is counted too.
- The wire gap of 12,125 is the loss uncounted by design. 11,922 of it falls in the 10% netem
  loss on the generator, about 10% of that minute's 120,000 lines, and 185 in the SUT's pause,
  stop, and partition windows. Outside UDP-affecting windows the gap was 31, under the 500 that
  five window edges allow.
- Everything the SUT read reached VictoriaMetrics: V equals Ab in both lives, and each of the 100
  series reset once, at the SUT's restart. The first life's residual at its last drain was 0, so
  its close-time window added nothing after that drain.

Re-scored with these checks, W1a's run `20261008T202436Z` gives the same picture: V is 1,842,707,
equal to Ab in both lives, not the 1,722,774 estimated before the ledger existed. Its wire gap is
12,014, and its generator dropped 17,700 lines.

The wire rule as built: G is cumulative at each generator drain and is interpolated linearly at
each SUT drain timestamp, because the two processes' drains are out of phase by up to an
`internal` interval. Across a run of steady drain intervals the interpolation telescopes, so each
edge of the run, against a fault window or the end sequence, errs by at most one generator batch.
`wire_loss_outside_faults = 0.0` therefore means no loss beyond one batch per edge.

`recovery` judges a fault at any SUT drain within `recovery_bound` of its end whose interval lies
after the fault, inside one SUT life, and clear of every other fault. Judging only the drain at
`recovery_bound` would have judged two of this schedule's seven faults, because the next fault
starts before `recovery_bound` runs out after five of them.

Two negative controls:

- Run `20261008T205911Z`, the `statsd-vm-lost-total` control (5 minutes): the SUT stopped 30 s
  into a 90 s VictoriaMetrics stop, with its sink's `shutdown_grace` at 1 s. Its first life's
  `drain complete` logged `batches_dropped` 4, and `ledger.egress` read "life 0: W − D − B − V
  63,300 = residual 0 + Ab − V 63,300, counted (drain complete batches_dropped 4); life 1
  (final): Ab 418,100 − V 418,100 = 0, ok". `ledger.summary` passed with
  every term 0, and so did every other row.
- Run `20261008T210459Z`, `statsd-vm` for 4 minutes from a scratch copy of the scenario whose
  `vm_selector` matched no series: `ledger.egress` FAILed ("no series in vm-export.jsonl match
  vm_selector '{__name__=~"no_such_metric_total"}'; life 0 (final): Ab 492,400 − V 0 = 492,400,
  uncounted"), `ledger.summary` FAILed on the same 492,400, `progress` FAILed on the empty
  freshness samples, and the script exited 1.

### W2: sink outages and UDP floods (2026-10-08)

Each scenario's recorded run below is from `soak/w2` with the review fixes (committed as
`a8af054d`; the `udp-flood-sink-stop` run started before that commit, with the same changes
uncommitted), one at a time at its own duration, on W1a's images and host (`logit:soak`
`sha256:78a95c90bb4a`; no Rust changed). Every watchdog row (`run` through `fd_slope`),
`identity.sink`, and `recovery` PASSed in all four runs. The ledger and expectation rows follow.
Earlier attempts, cited below as evidence, ran from `65745897` with the W2 changes uncommitted.

What the runs showed about `logit`:

- **Under the default `receive.overflow: drop_oldest`, the kernel dropped 79 datagrams as a
  blocked chain unblocked.** Run `20261008T231010Z`, `udp-flood-sink-stop`: the listener evicted
  and counted 532,590 datagrams, 426,090 of them during the stop and 106,500 in the two drains
  after the revert, and read 100,000 datagrams in every drain throughout, but
  `logit.input.kernel.drops` read 79 in the drain at +128.4 s, 8 s after the revert, the drain in
  which the receive queue went from full to empty and the chain unblocked; that drain read 99,921
  datagrams. [ADR `decoupled-listener-io`](../adr/decoupled-listener-io.md) says the socket keeps
  being read while the downstream is stalled, and through the stop it was: the drops came from
  the unblock, not the stall. The loss is counted (K in `ledger.wire`, which PASSed). A pause
  of the read loop of a few milliseconds, while the decode loop drained 10,000 queued datagrams
  into `aggregate`, would explain it at 20,000 datagrams a second; that cause is an inference,
  not verified. [`docs/known-gaps/intake.md`](../known-gaps/intake.md)'s "UDP intake" entry "A
  `drop_oldest` UDP listener takes kernel drops in the drain where a blocked downstream unblocks"
  records it. `expect.no-kernel-drops` FAILs on it by design, and keeps failing until that entry
  closes. The first run of this scenario, `20261008T223523Z` at a 10 s window, read 0, but its
  stop never reached the listener, so it shows nothing about an unblock.
- **A small `max_batches` doesn't make a 90 s outage reach the listener at 10 s windows.**
  `aggregate` sends one batch per window, and the sink's inbox holds 64 batches
  (`CHANNEL_CAPACITY` in `crates/logit-pipeline/src/runtime.rs`) between `aggregate` and the
  sink's queue. Run `20261008T221446Z`, `sink-outage-block` with statsd-vm's 10 s window: the
  queue reached its 2 batches 18 s into the stop, the sink took one more batch off its inbox and
  then nothing until the revert, when it took 7 at once, and nothing upstream blocked:
  `expect.aggregate-inbox-full` and `expect.listener-drops-counted` FAILed at 0, while every
  ledger row PASSed. Run `20261008T223523Z`, `udp-flood-sink-stop` with the defaults: the sink's
  queue peaked at 6 of 1,024 batches, no `inbox.full`, and `receive.utilization` 0 at every
  drain, so its rows held whether or not the read loop was decoupled. Filling the 64-batch inbox
  at one batch per 10 s takes over 10 minutes, so all four scenarios use a 500 ms window and a
  2-batch sink queue. The default `max_batches: 1024` alone would take nearly three hours at
  10 s.
- **`inbox.full` counts once per blocked send, not continuously.** In each blocking run, the
  sink's and `aggregate`'s `inbox.full` read 1 over the whole outage: the producer found the
  inbox full once and then waited in that send, and the outage's length shows in
  `inbox.blocked.duration`, recorded once that send completes. A second `inbox.full` lands in the
  drain where the chain unblocks. [`internal-telemetry.md`](../design/internal-telemetry.md)'s
  "Inbox side" now says so; it used to say a blocking sink "shows sustained `inbox.full` once
  its buffer fills", and the expectations assert `delta >= 1`.
- **A blocked chain stays blocked until the sink's next retry, not until the revert.** In
  `udp-flood-sink-stop-block`, 119,075 of the 516,666 kernel drops landed in the two drains after
  VictoriaMetrics started again, until the sink's backoff (`retry_max_delay`, 10 s) reached it
  (205,000 of 602,465 in the earlier attempt `20261008T224118Z`). `sink-outage-block`'s listener
  likewise counted 6,100 of its 92,100 `overflow_oldest` drops in the drain after the revert, and
  `udp-flood-sink-stop`'s 106,500 of 532,590. This is the documented behavior, and it's why every
  bound that must hold for the whole episode reads the `through` window.
- **`drop_oldest` at the sink never stalled intake.** `sink-outage-drop-oldest`'s sink evicted
  181 batches and kept taking batches off its inbox, so `aggregate`'s `inbox.full` read 0 over
  the stop and its tail (`expect.aggregate-never-blocks`), with the same 500 ms window that
  blocked `aggregate` about 40 s into the stop in `sink-outage-block`; the listener dropped nothing and its
  receive queue stayed empty.
- Everything else matched its documentation: `drop_oldest` at the listener counted every drop;
  `block` at the listener evicted nothing, during the stop or after it, and moved the loss to
  the kernel's counter; `block` at the sink dropped no batch; and every ledger balanced.

What the runs showed about the harness:

- With a 500 ms window and a 5 s `internal` interval, both timers tick in phase, so every drain
  lands right after a push and `buffer.batches` reads 1 in steady state, warmup included. A
  drained queue therefore reads at most 1, and each scenario's `sink-queue-drains` asserts
  `last <= 1`. For the same reason `recovery` failed run `20261008T222147Z` on
  `buffer.utilization 0.500`, one batch in a 2-batch queue, although the sink delivered 73
  batches in the first drain after the revert and then the full arrival rate; `recovery` now
  accepts a queue holding at most one batch that doesn't fill it.
- A `during` window misses the tail of a blocked episode: in `20261008T224118Z` the listener
  stayed blocked about 13 s past the revert, so `through` exists for the bounds that must stay 0.
- `udp-flood-sink-stop` ran at the starting rate of 20,000 lines/s with no `rate_behind` and no
  kernel drops in warmup, so the rate stayed there. Its wire gap was 0 over 6,123,400 lines (8
  steady, −8 at the end, the interpolation's edges), so `wire_loss_outside_faults` stays 0.0 on
  this host: the docker bridge lost nothing at this rate.

**`sink-outage-block`**, run `20261008T232120Z` (5 minutes 30 seconds; 500 ms window; sink
`buffer: {max_batches: 2}`; VictoriaMetrics stopped 90 s):

| Check | Status | Detail |
|---|---|---|
| `ledger.wire` | PASS | G 672,300 = W 672,300 + K 0 + wire 0 (by design: 0 in UDP-affecting windows, -40 steady, 40 at the end; steady limit 100); the generator's counted drop_newest loss, not in G: 0 event(s) in 0 batch(es) |
| `ledger.intake` | PASS | final life 0: W − D 580,200 vs E + B 580,200 (R 77,000 = 10,000 + 67 x 1,000) |
| `ledger.egress` | PASS | life 0 (final): Ab 580,200 − V 580,200 = 0, ok (R 77,000 = 10,000 + 67 x 1,000) |
| `ledger.summary` | PASS | final life 0: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V 0; wire G − (W + K) 0 over the run, by design |
| `expect.sink-queue-fills` | PASS | max of logit.component.buffer.batches{component=victoria_metrics} on logit, during c0s1: c0s1 (+60s, +150s]: 2 (1 sample(s) and the value in force at its start), want >= 2 |
| `expect.sink-inbox-full` | PASS | delta of logit.component.inbox.full{component=victoria_metrics} on logit, during c0s1: c0s1 (+60s, +150s]: 1 (over 18 drain(s)), want >= 1 |
| `expect.aggregate-inbox-full` | PASS | delta of logit.component.inbox.full{component=window} on logit, during c0s1: c0s1 (+60s, +150s]: 1 (over 18 drain(s)), want >= 1 |
| `expect.listener-drops-counted` | PASS | delta of logit.component.datagrams.dropped{component=statsd, reason=overflow_oldest} on logit, during c0s1: c0s1 (+60s, +150s]: 86,000 (over 18 drain(s)), want >= 1 |
| `expect.no-kernel-drops` | PASS | delta of logit.input.kernel.drops{component=statsd} on logit, through c0s1: c0s1 (+60s, +195s]: 0 (over 27 drain(s)), want <= 0 |
| `expect.sink-never-drops` | PASS | delta of logit.component.batches.dropped{component=victoria_metrics} on logit, through c0s1: c0s1 (+60s, +195s]: 0 (over 27 drain(s)), want <= 0 |
| `expect.sink-queue-drains` | PASS | last of logit.component.buffer.batches{component=victoria_metrics} on logit, after c0s1: c0s1 (+150s, +195s]: 1 (9 sample(s) and the value in force at its start), want <= 1 |
| `expect.retrying-clears` | PASS | min of logit.component.retrying{component=victoria_metrics} on logit, after c0s1: c0s1 (+150s, +195s]: 0 (1 sample(s) and the value in force at its start), want <= 0 |

The listener's receive queue filled about 45 s into the stop, after the sink's queue, its
inbox, and `aggregate`'s inbox, and D is the 92,100 datagrams it dropped and counted. The edge
and aggregate rows PASSed with 580,200 on each side.

**`sink-outage-drop-oldest`**, run `20261008T231530Z` (5 minutes 30 seconds; 500 ms window; sink
`buffer: {max_batches: 2, overflow: drop_oldest}`; VictoriaMetrics stopped 90 s):

| Check | Status | Detail |
|---|---|---|
| `ledger.wire` | PASS | G 672,300 = W 672,300 + K 0 + wire 0 (by design: 0 in UDP-affecting windows, -51 steady, 51 at the end; steady limit 100); the generator's counted drop_newest loss, not in G: 0 event(s) in 0 batch(es) |
| `ledger.intake` | PASS | final life 0: W − D 672,300 vs E + B 672,300 (R 77,000 = 10,000 + 67 x 1,000) |
| `ledger.egress` | PASS | life 0 (final): Ab 672,300 − V 672,300 = 0, ok (R 77,000 = 10,000 + 67 x 1,000) |
| `ledger.summary` | PASS | final life 0: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V 0; wire G − (W + K) 0 over the run, by design |
| `identity.sink` | PASS | final life 0 at +333s: received 673 vs delivered 492 + dropped 181 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `expect.sink-evicts-oldest` | PASS | delta of logit.component.batches.dropped{component=victoria_metrics, reason=overflow_oldest} on logit, during c0s1: c0s1 (+60s, +150s]: 175 (over 18 drain(s)), want >= 1 |
| `expect.aggregate-never-blocks` | PASS | delta of logit.component.inbox.full{component=window} on logit, through c0s1: c0s1 (+60s, +195s]: 0 (over 27 drain(s)), want <= 0 |
| `expect.listener-no-drops` | PASS | delta of logit.component.datagrams.dropped{component=statsd} on logit, through c0s1: c0s1 (+60s, +195s]: 0 (over 27 drain(s)), want <= 0 |
| `expect.intake-never-stalls` | PASS | max of logit.component.receive.utilization{component=statsd} on logit, through c0s1: c0s1 (+60s, +195s]: 0 (27 sample(s) and the value in force at its start), want <= 0.05 |
| `expect.sink-queue-drains` | PASS | last of logit.component.buffer.batches{component=victoria_metrics} on logit, after c0s1: c0s1 (+150s, +195s]: 1 (9 sample(s) and the value in force at its start), want <= 1 |

The sink evicted 181 batches, 175 inside the stop and 6 in the drain after the revert, and Ab − V
is 0: under `temporality: cumulative` each evicted total was superseded by a later one that
reached VictoriaMetrics, and the batches delivered after the revert were the newest, so no
series' last total was lost.

**`udp-flood-sink-stop`**, run `20261008T231010Z` (5 minutes; 20,000 lines/s; 500 ms window;
sink `buffer: {max_batches: 2}`; the receive queue at its defaults; VictoriaMetrics stopped
60 s):

| Check | Status | Detail |
|---|---|---|
| `ledger.wire` | PASS | G 6,123,400 = W 6,123,321 + K 79 + wire 0 (by design: 0 in UDP-affecting windows, 8 steady, -8 at the end; steady limit 100); the generator's counted drop_newest loss, not in G: 0 event(s) in 0 batch(es) |
| `ledger.intake` | PASS | final life 0: W − D 5,590,731 vs E + B 5,590,731 (R 77,000 = 10,000 + 67 x 1,000) |
| `ledger.egress` | PASS | life 0 (final): Ab 5,590,731 − V 5,590,731 = 0, ok (R 77,000 = 10,000 + 67 x 1,000) |
| `ledger.summary` | PASS | final life 0: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V 0; wire G − (W + K) 0 over the run, by design |
| `expect.aggregate-inbox-full` | PASS | delta of logit.component.inbox.full{component=window} on logit, during c0s1: c0s1 (+60s, +120s]: 1 (over 12 drain(s)), want >= 1 |
| `expect.listener-evicts` | PASS | delta of logit.component.datagrams.dropped{component=statsd, reason=overflow_oldest} on logit, during c0s1: c0s1 (+60s, +120s]: 426,090 (over 12 drain(s)), want >= 1 |
| `expect.intake-keeps-reading` | PASS | min_delta of logit.input.datagrams{component=statsd} on logit, during c0s1: c0s1 (+60s, +120s]: 100,000 (smallest of 12 drain(s)), want >= 1 |
| `expect.intake-keeps-pace` | PASS | delta of logit.input.datagrams{component=statsd} on logit, during c0s1: c0s1 (+60s, +120s]: 1,200,000 (over 12 drain(s)), want >= 1.08e+06 |
| `expect.no-kernel-drops` | FAIL | delta of logit.input.kernel.drops{component=statsd} on logit, through c0s1: c0s1 (+60s, +165s]: 79 (over 21 drain(s)), want <= 0, FAIL |
| `expect.sink-queue-drains` | PASS | last of logit.component.buffer.batches{component=victoria_metrics} on logit, after c0s1: c0s1 (+120s, +165s]: 1 (8 sample(s) and the value in force at its start), want <= 1 |

The receive queue filled about 38 s into the stop and evicted from then until the chain
unblocked; `expect.no-kernel-drops` is the finding above.

**`udp-flood-sink-stop-block`**, run `20261008T232711Z` (5 minutes; 20,000 lines/s;
`receive: {overflow: block, max_datagrams: 10000}`; 500 ms window; sink
`buffer: {max_batches: 2}`; VictoriaMetrics stopped 60 s):

| Check | Status | Detail |
|---|---|---|
| `ledger.wire` | PASS | G 6,123,300 = W 5,606,634 + K 516,666 + wire 0 (by design: 0 in UDP-affecting windows, 78 steady, -78 at the end; steady limit 100); the generator's counted drop_newest loss, not in G: 0 event(s) in 0 batch(es) |
| `ledger.intake` | PASS | final life 0: W − D 5,606,634 vs E + B 5,606,634 (R 77,000 = 10,000 + 67 x 1,000) |
| `ledger.egress` | PASS | life 0 (final): Ab 5,606,634 − V 5,606,634 = 0, ok (R 77,000 = 10,000 + 67 x 1,000) |
| `ledger.summary` | PASS | final life 0: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V 0; wire G − (W + K) 0 over the run, by design |
| `expect.sink-queue-fills` | PASS | max of logit.component.buffer.batches{component=victoria_metrics} on logit, during c0s1: c0s1 (+60s, +120s]: 2 (1 sample(s) and the value in force at its start), want >= 2 |
| `expect.kernel-drops` | PASS | delta of logit.input.kernel.drops{component=statsd} on logit, during c0s1: c0s1 (+60s, +120s]: 397,591 (over 12 drain(s)), want >= 1 |
| `expect.block-never-evicts` | PASS | delta of logit.component.datagrams.dropped{component=statsd} on logit, through c0s1: c0s1 (+60s, +165s]: 0 (over 21 drain(s)), want <= 0 |
| `expect.sink-queue-drains` | PASS | last of logit.component.buffer.batches{component=victoria_metrics} on logit, after c0s1: c0s1 (+120s, +165s]: 1 (8 sample(s) and the value in force at its start), want <= 1 |
| `expect.retrying-clears` | PASS | min of logit.component.retrying{component=victoria_metrics} on logit, after c0s1: c0s1 (+120s, +165s]: 0 (1 sample(s) and the value in force at its start), want <= 0 |
| `expect.receive-queue-empties` | PASS | min of logit.component.receive.utilization{component=statsd} on logit, after c0s1: c0s1 (+120s, +165s]: 0 (8 sample(s) and the value in force at its start), want <= 0.05 |

The kernel started dropping about 40 s into the stop, once the sink's queue and inbox,
`aggregate`'s inbox, and the receive queue had filled; from then on the listener read nothing
until the chain unblocked. Every datagram the kernel took is in K, so the wire gap stayed 0.

### W3: a SIGKILL replayed from a disk spool (2026-10-09)

Run `20261009T005807Z`: `script/soak run spool-kill-replay` at its own 6 minutes 15 seconds,
from `soak/w3` at `fe825418` with the review fixes uncommitted, on W1a's images and host
(`logit:soak` `sha256:78a95c90bb4a`; no Rust changed). VictoriaMetrics stopped at +60 s for
150 s; the SUT was killed at +120 s and started at +150 s, and answered `logit ready` on the
first probe. Every row passed, `ledger.windows` and `ledger.replay` among them:

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 4 apply/revert action(s) returned 0; 1 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 3 exit(s) of the logit services, each a scheduled stop with code 0 or a scheduled kill with code 137 (1 kill(s)) |
| `restarts` | PASS | 2 start(s), each a scheduled start or restart |
| `self_log` | PASS | 3 sink fault line(s) inside fault windows, 0 outside, 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 64 health sample(s) healthy outside fault windows; 1 start(s) or unpause(s) ready in time, slowest ready 0.1s |
| `progress` | PASS | 4 30s window(s) with deliveries outside fault windows; 5 freshness sample(s) under 30s; 90s of 315s after warmup judged (29%; WARN under 5%) |
| `rss_slope` | PASS | MiB/h per service/life: logit/1 +4.6, generator/0 +6.9 (limit 64); logit 110s of 315s after warmup judged (35%; WARN under 5%); generator 115s of 315s after warmup judged (37%; WARN under 5%) |
| `fd_slope` | PASS | warmup median -> end: logit 15->15, generator 11->12 (limit +8) |
| `ledger.wire` | PASS | G 762,400 = W 759,100 + K 0 + wire 3,300 (by design: 3,204 in UDP-affecting windows, 43 steady, 53 at the end; steady loss judged 43, each run at 0 or more, against limit 300); the generator's counted drop_newest loss, not in G: 0 event(s) in 0 batch(es) |
| `ledger.intake` | PASS | final life 1: W − D 510,300 vs E + B 510,300; life 0: W − D − B − Ab 0 within [0, R] (R 77,000 = 10,000 + 67 x 1,000) |
| `ledger.edge` | PASS | final life 1: E 510,300 == A 510,300 (listener to aggregate) |
| `ledger.aggregate` | PASS | final life 1: Ab 510,300 == A 510,300 (every event absorbed) |
| `ledger.egress` | PASS | life 0 (killed): Ab 248,800 − V 248,800 = 0, ok (band [-10,001, 30,003]: ingest 2,000/s over the last drain interval; below, the residual 0 plus one 4.99985s drain interval of ingest, absorbed after the last drain and flushed before the kill; above, one 10s aggregate window (configured) unflushed at the kill plus one drain interval of queue growth after the last drain); residual W − D − B − Ab 0 at its last drain, what the kill lost from the receive queue and inbox, by design, not judged here; D has no stderr part: a SIGKILL logs no shutdown drops; life 1 (final): Ab 510,300 − V 510,300 = 0, ok (R 77,000 = 10,000 + 67 x 1,000) |
| `ledger.windows` | PASS | 100 series x 2 life/lives, widest gap 10.001s (gaps <= 15s (1.5 x the 10s aggregate interval), a killed life's last sample within 11s (1.1 x) of the kill, a later life's first within 20s (2 x) of its start) |
| `ledger.replay` | PASS | life 1's first drain replayed 6 vs killed life 0's last-drain buffer.batches 5 (one batch in flight allowed) |
| `ledger.summary` | PASS | final life 1: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V 0; wire G − (W + K) 3,300 over the run, by design |
| `identity.sink` | PASS | final life 1 at +400s: received 23 + replayed 6 vs delivered 29 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `recovery` | PASS | 1 of 2 fault(s) judged, each recovered within recovery_bound 45s of its end; warmup rate 2,000/s |
| `expect.spool-holds-the-outage` | PASS | max of logit.component.buffer.disk.segments{component=victoria_metrics} on logit, during c0s1: c0s1 (+60s, +210s]: 1 (13 sample(s) and the value in force at its start), want >= 1 |
| `expect.spool-queues-windows` | PASS | max of logit.component.buffer.batches{component=victoria_metrics} on logit, during c0s1: c0s1 (+60s, +210s]: 12 (13 sample(s) and the value in force at its start), want >= 2 |
| `expect.spool-replayed-at-restart` | PASS | delta of logit.component.buffer.disk.replayed{component=victoria_metrics} on logit, after c0s2: c0s2 (+150s, +195s]: 6 (over 8 drain(s)), want >= 1 |
| `expect.no-torn-tail` | PASS | delta of logit.component.buffer.disk.truncated{component=victoria_metrics} on logit, through c0s2: c0s2 (+120s, +195s]: 0 (over 8 drain(s)), want <= 0 |
| `expect.no-second-replay` | PASS | delta of logit.component.buffer.disk.replayed{component=victoria_metrics} on logit, after c0s1: c0s1 (+210s, +255s]: 0 (over 9 drain(s)), want <= 0 |
| `expect.sink-never-drops` | PASS | delta of logit.component.batches.dropped{component=victoria_metrics} on logit, through c0s1: c0s1 (+60s, +255s]: 0 (over 33 drain(s)), want <= 0 |
| `expect.retrying-clears` | PASS | last of logit.component.retrying{component=victoria_metrics} on logit, after c0s1: c0s1 (+210s, +255s]: 0 (1 sample(s) and the value in force at its start), want <= 0 |

What the run showed about `logit`:

- **The spool kept every window the killed process queued.** The first process spooled the six
  10 s windows it flushed between the VictoriaMetrics stop and the kill (+68 s through +118 s).
  The second process found them at open (`buffer.disk.replayed` 6), held them behind its own
  windows while VictoriaMetrics stayed down (the queue peaked at 12 batches), and delivered all
  of them once VictoriaMetrics started. `ledger.windows` found a sample in every 10 s window of
  each of the 100 series in both lives (widest gap 10.001 s), and no torn tail was truncated at
  open.
- **The replay matched the queue.** The killed life's last drain (+118 s) read
  `buffer.batches` 5, and the next life replayed 6: the +118.4 s window reached the spool after
  that drain, the one batch in flight `ledger.replay` allows.
- **The killed life's egress balanced to zero.** Its last `aggregate` flush (+118.4 s) and its
  last drain fell together, 1.6 s before the kill, and no flush followed, so its Ab − V was 0,
  inside a band of [−10,001, 30,003]. Its residual at the last drain was 0. `drain complete` is
  absent for that life, as a SIGKILL implies, and the replay count stayed 0 after
  VictoriaMetrics returned.
- `buffer.disk.segments` read 1 throughout, warmup included: at the default 64 MiB
  `segment_bytes` the active segment never rotates in this run, so `spool-holds-the-outage` only
  shows the spool is open. `spool-queues-windows` is the expectation that shows it filling.

What the runs showed about the harness:

- **`identity.sink` didn't account for a spool's replay.** The first run of this scenario,
  `20261009T000850Z`, scored it FAIL: "received 23 vs delivered 29 + dropped 0 +
  buffer.batches 0, gap -6". The six replayed batches were delivered in the final life but
  received in the killed one. The identity now adds the life's `buffer.disk.replayed` to the
  received side, and the self-test's three-life fixture pins it. Re-scored by the current
  checks, that run passes every row; `ledger.windows` SKIPs there, because its resolved
  scenario predates `vm_every_window`, and passes with the key set on a copy.
- **The kill band used to hide a lost tail.** It judged W − D − B − V with R (77,000) on top of
  one window of ingest, a high edge of 97,000, so deleting the last one to four of the six
  spooled windows from a copy of `20261009T000850Z` still passed. It now judges Ab − V alone,
  with a high edge of 30,003 here. A lost last window (20,000) still sits inside that band,
  because the life's last flush and its last drain fell together; on the same copy,
  `ledger.windows` FAILs it from the tail rule (last sample 11.6 s before the kill, over 11 s),
  the last three from the same rule, and the five middle windows from a 50 s gap, which no
  total shows. That tail rule passes a lost last window only when the kill lands within 1 s of
  the life's last flush; this run's kill came 1.6 s after it.
- The generator's `statsd_out` dropped nothing while the SUT was down: its queue held the 30 s
  of batches and sent them once `logit` answered again. The 3,204 lines of wire gap in
  UDP-affecting windows are about what the killed process read in the 1.6 s after its last
  drain (3,200 at 2,000 a second), which no counter exported, plus what its kernel receive queue
  held at the kill; the split between the two isn't measured.
- `vm.totals_by_life` credits the replayed samples to the killed life by their own timestamps,
  as "Which life a hop is judged in" says: V for life 0 is 248,800, equal to its W and Ab.

The negative control, run `20261009T002208Z`, is the same scenario from a scratch copy outside
`tools/soak/scenarios/` whose SUT has no `disk:` block, so the sink's queue is in memory. Its
first process queued the same six windows (`buffer.batches` peaked at 6) and lost them with the
kill. Re-scored by the current checks, `ledger.egress` FAILs: "life 0 (killed): Ab 248,800 − V
128,800 = 120,000, uncounted (band [-9,999, 29,998] …)", and `ledger.replay` FAILs: "life 1's
first drain replayed 0 vs killed life 0's last-drain buffer.batches 6, off by -6".
`expect.spool-holds-the-outage` and `expect.spool-replayed-at-restart` FAIL too, and every other
row passes, `ledger.summary` included, because the final life balanced. The 120,000 is the 60 s
of increments between the VictoriaMetrics stop and the kill.

### W4: random schedules and long runs (no run yet)

No `random-faults` run has been made. The first runs, in order, are a 1-hour smoke at the
scenario's own duration and seed (`script/soak run random-faults`, 14 faults), then 8 hours
(`--duration 8h`, 113 faults for seed 20261009). Each is recorded here with the commit, image
tags, host, the seed, the verbatim `results.md` table, and what the schedule did.

What W4 measured without a run:

- **Log rates.** In the recorded W1b, W2, and W3 runs, the SUT's stdout ran 3.4 to 4.2 KB/s and
  the generator's 4.3 to 4.6 KB/s (17 to 23 KB per 5 s drain), stderr under 50 B/s, whatever the
  load: `internal` writes the same points per drain at 2,000 and at 20,000 lines/s. Stored by
  `json-file`, which wraps and escapes each line, that is at most 6.4 KB/s per container.
- **Rotation window.** At 6.4 KB/s a 20 MB file fills in about 52 minutes, and with
  `max-file: 5` the daemon keeps at least the four newest full files, about 3.5 hours, so a
  5-minute chunk interval has a margin of about 40. The daemon holds at most 100 MB per
  container, less with `compress`.
- **Run-directory growth.** An 8-hour run writes about 100 MB of SUT stdout and 130 MB of
  generator stdout, 8 MB of `watchdog.jsonl` (3 records of about 440 bytes every 5 s), 1 MB of
  `stats.ndjson`, and 6 MB of `vm-export.jsonl` (100 series, a sample every 10 s): about
  250 MB. A 24-hour run writes about 750 MB.
- **`check` cost.** A synthetic 8-hour directory (a real `statsd-vm` run's drains replayed
  across the 30 SUT lives seed 20261009's schedule implies, with a matching timeline, watchdog,
  and export; 240 MB) took 17.6 s and 661 MiB to score: `Telemetry.matching` scanned every point
  on each of `progress`'s 458 `counter_in` calls, and `g_at` and `life_drains` rescanned too, so
  the cost grew with the square of the run. Indexing points by name, bisecting, caching each
  life's drains, and interning attribute dicts brought it to 3.3 s and 243 MiB, with identical
  rows; a 24-hour directory went from 135 s and 1,935 MiB to 13 s and 678 MiB. The recorded
  W1b, W2, and W3 runs score the same rows, every detail included, before and after.

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
- **W2**: the self-test passes, and each new expectation fails with its rule reverted. Each new
  scenario passes `self-test` validation and the shipped-config test, and a run of it, every row
  PASS, is recorded under "Findings".
- **W3**: the self-test passes, and each new rule fails it when reverted. `spool-kill-replay`
  passes `self-test` validation and the shipped-config test, and a run of it, every row PASS, is
  recorded under "Findings", with a negative control: the same scenario without the spool, whose
  killed life's `ledger.egress` FAILs as uncounted and whose `ledger.replay` FAILs.
- **W4**: the self-test passes, and each new rule fails it when reverted (46 rules: every
  `[random]` validation and schedule rule, the draw order, the seed's recording, the
  `ledger.windows` excusal, the chunk boundaries and failure handling, and the poll deadlines
  and round timeouts). `random-faults` passes `self-test` validation, the shipped-config test,
  and `logit validate` in `logit:soak`. Re-scoring the recorded W1b, W2, and W3 runs gives the
  same rows, every detail included. `check` on a synthetic 8-hour run directory finishes in
  seconds. A 1-hour `random-faults` run and an 8-hour one are recorded under "Findings".
- **W5**: each new scenario passes `self-test` validation and the shipped-config test, and a
  run of it is recorded under "Findings".
