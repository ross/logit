---
created: 2026-10-08
updated: 2026-10-11
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
`soak/w12`, a linear stack. PR stack only: nothing is merged by this workstream; Ross directs
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
  compose.yaml                   # victoria-metrics, logit (SUT), generator, each labeled soak.role
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
  scenarios/statsd-datadog/             # W5: an external Datadog target, no backend query
  scenarios/kafka-broker-restart/       # a Kafka target: kafka_out to a local broker, read back
```

Subcommands: `run <scenario> [--duration 20m] [--seed N] [--keep] [--out DIR]`, `list`,
`check <run-dir>`, `target <scenario> [--env-file FILE]` (W5: the target's kind, for
`script/soak`), and `self-test`, which also runs at the start of `run`, as
`survey_self_test` does for `script/shape-survey`. Environment: `SOAK_SKIP_IMAGE=1` reuses the
images, and `SOAK_OUT=<dir>` moves the results.

A run writes `perf/results/soak/<UTC stamp>/` (gitignored): `compose.env`, a copy of
`scenario.toml`, `scenario.resolved.json`, `configs/` (each `logit` config, so the ledger reads
the SUT's `receive:` limits), `provenance.txt`, `timeline.jsonl`,
`watchdog.jsonl`, `stats.ndjson`, `vm-freshness.jsonl`, `vm-export/<sink>.jsonl`,
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
sut_aggregate = "window"
wire_loss_outside_faults = 0.0

[[ledger.source]]         # one per generator the ledger follows into the SUT (W8b)
service = "generator"
generator_input = "load"
generator_sink = "statsd"
sut_listener = "statsd"

[[ledger.sink]]           # one per SUT sink the ledger follows to its backend
id = "victoria_metrics"
backend = "victoria-metrics"
vm_selector = '{__name__=~"soak_requests_[0-9]+_total"}'

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
- No netem on a container during its `stop`, `kill`, `pause`, `partition`, or `exit`, because the qdisc
  is lost with the namespace and `docker run --network container:<id>` needs a running target.
- No fault ends inside `cooldown`, and `cooldown` is at least `recovery_bound`.
- Actions and services are known. W1's actions are `netem`, `pause`, `stop`, `restart`, and
  `partition`, on any service of the stack: `logit`, `generator`, `victoria-metrics`, and from
  W7b any service a `compose` override adds. W3 adds `kill`, on the SUT only:
  the lines a killed generator sent after its last telemetry drain reach the SUT with no G
  behind them, and `ledger.wire` would net them against real loss. W7a adds `mark` and `exit`
  (below).
- `[ledger] vm_every_window`, optional, is `true` or `false`; `[ledger] sut_script`, optional,
  is a component id.
- `[[ledger.source]]` (W8b) has the rules under "Sources" below.
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

**Marks and expected exits (W7a)** name an event the SUT's own config causes, such as a Lua
script that stalls or outgrows its memory cap, so the checks excuse it as they excuse a fault:

```toml
[[step]]
at = "1m30s"
action = "mark"           # no Docker call; its window is an ordinary fault window
on = "logit"
ready = "stalled"         # optional: a readiness word `ready` must read inside the window
for = "2m30s"

[[step]]
at = "2m50s"
action = "exit"           # an exit the SUT makes on its own inside the window
on = "logit"              # logit only
code = 2                  # the exit code it leaves
key = "memory_limit_exceeded"   # the self-log key of the ERROR line that explains it
for = "1m30s"
```

- A `mark`'s apply and revert make no Docker call. Its window excuses what any fault window
  does: `progress`, `ready`, sink fault lines in `self_log`, the slopes, and `recovery` judges
  it. A mark on `logit` is UDP-affecting, because a stalled script backs up into the receive
  queue. `ready`, allowed on a `mark` only, is one of `starting`, `stalled`, `draining`, or
  `degraded`.
- An `exit`'s apply makes no Docker call either. Its revert runs `docker start` when the
  container has exited, and records `started`; only a revert that started it is a start, with a
  readiness probe and a new SUT life. `code` and `key` are required on an `exit` and refused
  elsewhere, and an `exit` is on `logit` only. The process can end at any point in the window,
  so a netem on its container may not overlap it. While an `exit` is open the driver's fail-fast
  accepts `running` or `exited`. "The checks" has its rules in `exit`, `self_log`, and `ready`.
- Neither may appear in a `[random]` schedule, which draws faults; an expected event happens
  at a time the scenario's own config sets.

**`sut_script` (W7a)**, an optional `[ledger]` key, names a `lua` component between the
listener and `aggregate`, and adds the script hop to the ledger ("The ledger and identities").

**Compose overrides, service roles, and sinks (W7b).** Every compose service carries a
`soak.role` label: `sut`, `generator`, `backend`, or `broker` (a Kafka target's broker; see
"Kafka targets"). The harness knows a service by its role, never its name, and keeps one SUT,
`logit`. The base stack's generator is `generator`; from W8b an override can add more (see
"Sources"). A scenario adds services with an optional top-level key naming an override file
beside `scenario.toml`:

```toml
compose = "compose.override.yaml"   # merged onto tools/soak/compose.yaml

[ledger]
split = "route"           # copy (the default) or route
# ...

[[ledger.sink]]           # one per SUT sink
id = "vm_a"               # the sink's component id
target = "tier_a"         # route only: the target that feeds it
backend = "victoria-metrics"                                # local only: a backend service
vm_selector = '{__name__=~"soak_requests_[0-9]+_total"}'    # local only: the series V reads
```

- `[[ledger.sink]]` replaces W1's `[ledger] sut_sink` and `vm_selector`. A scenario lists one
  or more sinks with distinct ids. For a local target each sink names a `backend` and a
  `vm_selector`; for an external target it names neither.
- `split` says how the sinks share `aggregate`'s output. Under `copy` each sink, fed by a
  fan-out, holds all of it. Under `route` each sink is fed by one `target` that a `route` fills,
  so the sinks hold disjoint shares; `route` needs two or more sinks, each with a distinct
  `target`, and `copy` refuses `target`.
- `compose` names a file beside `scenario.toml`, never a path. Each service it adds needs a
  `soak.role` label, and a backend needs `profiles: [local]`.
- `load()` checks every rule above that needs no service list. The rest need the merged
  compose file, which only the driver reads (`docker compose config`, §3): every step's `on`
  and every random fault's is a service of the stack, a `kill` or `exit` is on the role `sut`,
  every `[[expect]]` service runs `logit`, every sink's `backend` has the role `backend`, the
  stack has one `sut` named `logit` and at least one `generator`, every source's `service` has
  the role `generator` and every `generator` has a source (W8b), and no service lacks a known
  role. The driver checks them in `prepare()`, before `up`. `list` and the self-test
  can't run compose, so they pass the base role map (`scenario.BASE_ROLES`) for a scenario with
  no override, and the self-test reads a shipped override's labels itself.

**Sources (W8b).** Each `[[ledger.source]]` is one generator and the SUT listener its
datagrams reach. They replace W1's scalar `[ledger]` keys `generator_input`, `generator_sink`,
and `sut_listener`, which `load()` now refuses with a pointer to the table. `[configs]` names the
SUT's config under `sut` and each generator's under its service name:

```toml
compose = "compose.override.yaml"   # adds generator-lines, labeled soak.role: generator

[configs]
sut = "logit-sut.yaml"
generator = "logit-generator.yaml"
generator-lines = "logit-generator-lines.yaml"

[[ledger.source]]
service = "generator"     # a service with the role generator
generator_input = "load"  # its generate_in
generator_sink = "statsd" # its UDP sink, whose output.messages is G
sut_listener = "statsd"   # the SUT listener its datagrams reach

[[ledger.source]]
service = "generator-lines"
generator_input = "load"
generator_sink = "syslog"
sut_listener = "lines"
sut_transforms = ["parse", "labels", "count", "metrics_only"]   # optional, in order
```

- A scenario lists one or more sources. Each names all four keys, each a component id, with a
  `service` that is a slug (lowercase letters, digits, `_`, and `-`) no other source names and
  a `sut_listener` no other source names. Unknown keys are refused. A `service` whose compose
  variable is `SOAK_SUT_CONFIG` or another source's (`generator-lines` and `generator_lines`
  share one) is refused.
- `[configs]` has `sut` and one key per source's `service`, and no other. The driver writes each
  generator's absolute config path into `compose.env` under `scenario.config_env(service)`:
  `SOAK_GENERATOR_CONFIG` for `generator`, `SOAK_GENERATOR_LINES_CONFIG` for
  `generator-lines`. An override's generator mounts its own variable at `/config.yaml`.
- `sut_transforms`, optional, lists the nodes between the listener and `aggregate`, distinct, none
  of them a listener, and none on another source's list. Each must forward every event it
  receives, which `ledger.edge` judges hop by hop, and each adds an inbox and a batch to R ("The
  ledger and identities"). It can't be combined with `[ledger] sut_script`, whose hop
  `ledger.script` judges from every listener's E.
- With the role map: every source's `service` is a `generator`, and every `generator` in the
  stack has a source, because a generator with none would put datagrams into a listener with no
  G behind them.

**`vm_every_window` (W3)**, an optional `[ledger]` key, turns on `ledger.windows`: every series
has a sample in every `aggregate` window of every SUT life that `aggregate` emitted. Set it only
when every series is written in every window, because `aggregate` emits a series only in a
window that updated it, and the row excuses a gap only when the whole component was idle.

The expected rate is never configured. It is measured from the generator's own `events.sent`
over the warmup.

**Expectations (W2).** The watchdog and the ledger judge every scenario alike. A scenario that
asserts something of its own, such as a sink queue filling during an outage, adds `[[expect]]`
tables, each scored as one `expect.<name>` row:

```toml
[[expect]]
name = "sink-queue-fills"               # the row id: expect.sink-queue-fills
service = "logit"                       # whose telemetry: logit or a generator
metric = "logit.component.buffer.batches"
component = "victoria_metrics"          # optional; joins `attrs`
attrs = { reason = "overflow_oldest" }  # optional attribute filters, all strings
step = "c0s1"                           # an expanded step id, or a 1-based [[step]] index
window = "during"                       # during | after | through | run (W8c, no step)
reduce = "max"                          # delta | min_delta | max | min | last | slope (W8c)
min = 2                                 # min, max, or both
```

| Field | Meaning |
|---|---|
| `step` | An expanded step id names one occurrence. An integer names that `[[step]]` table's occurrence in every cycle, and the row FAILs if any occurrence does. `window = "run"` takes none |
| `window` | `during` is the fault's span, from its apply starting to its revert finishing; `after` is the `recovery_bound` after it; `through` is both, from the apply starting to `recovery_bound` after the revert finishes. Each is open at its start, because a drain's deltas cover the interval before its timestamp. A bound that must hold for the whole episode, such as a loss counter that must stay 0, reads `through`: a chain blocked behind a sink stays blocked until the sink's next retry after the revert, up to its `retry_max_delay` later, so `during` misses the tail. `run` (W8c) has no step: it runs from the end of warmup to the start of the end sequence, fault windows included |
| `reduce` | For a counter (`kind: sum`): `delta` sums the window's drains, and `min_delta` is the smallest single drain, a drain with no point counting 0, because a loss counter isn't emitted while it's 0. For a gauge: `max`, `min`, and `last` over the window's samples and the value in force at its start, because `internal` exports a gauge only in a drain after it was set. The value in force comes only from the SUT life whose drains cover the window's start, so a value an earlier process set doesn't carry into the next; a life that hasn't set the gauge yet leaves it unset. `last` is the value in force at the window's end. `slope` (W8c) is the least-squares slope of the window's own samples, without the value in force, in the metric's unit per hour (`checks.slope()`, times 3,600). A restart resets a process gauge, so `slope` fits each SUT life on its own and judges each life's slope against the bounds; a fit across a restart would read the drop as a negative trend. A life needs `rss_slope`'s minimum, 6 samples spanning 60 s, and the row FAILs when no life in the window has it, as a counter's window with no drain does |

`validate()` refuses an unknown key, a missing field (`step` is required for every window but
`run`, which refuses one), a `name` that isn't lowercase letters,
digits, `_`, or `-`, a repeated `name`, a `service` that doesn't run `logit` (checked against
the role map), an
unknown `window` or `reduce`, a reducer of the wrong kind for a metric whose kind it knows,
no bound, a `min` above `max`, a bound that isn't a number, `component` set both alone and in
`attrs`, a `component` that isn't a string, and a `step` that names no `[[step]]` table or, at the scenario's own duration, isn't
in its schedule. A shorter `--duration` may drop the step; the row then SKIPs.

When the run is scored, the row FAILs when the reduced value is outside its bounds, the window
has no drain (a counter) or the gauge was never set by its end, the run's points for the metric
are of the other kind, or the step was scheduled but its apply didn't return 0. A scenario with
no `[[expect]]` gets one `expect` row that SKIPs.

**Expected verdicts (W8c).** A row can be expected to FAIL when the scenario exists to show the
growth it detects, such as `cardinality`'s interner. An optional top-level `[expected]` table
maps a row id to a table of services and the verdict each should get:

```toml
[expected]
rss_slope = { logit = "FAIL" }   # row id -> service -> PASS | WARN | FAIL
```

- Only `rss_slope` and `fd_slope` take one. They are the rows that judge each `logit` service
  apart (§6), so a verdict names what it's about; every other row's FAIL is loss or a hang,
  which no scenario expects.
- Each service must run `logit` (checked against the role map), and each verdict is `PASS`,
  `WARN`, or `FAIL`; a row id outside the two, an empty table, or a value that isn't a table is
  refused.
- When the run is scored, a service whose own status equals its expectation scores PASS, its
  line reading `FAIL, as expected: ...`, and one whose status differs, a SKIP included, FAILs
  with `expected FAIL, got PASS`. A service with no expectation keeps its own status, and the
  row takes the worst.
- `rss_slope` reports growth as WARN under an hour's run, so an expected FAIL holds only at an
  hour or more; a shorter `--duration` FAILs the row as `expected FAIL, got WARN`.

A scenario may name an external target in a `[target]` table (W5); see "External targets". It
may instead name a local Kafka broker as its target; see "Kafka targets".

### 3. Compose (W1a)

The project is `soak-<scenario>`, so two scenarios can run at once. The driver refuses a project
that already has containers, as `script/victoria-interop` does, and never prunes. An `x-logit`
anchor sets `image: ${SOAK_IMAGE}`, `restart: "no"` (a crash must stay observable),
`logging: {driver: json-file}` with `max-size: 20m`, `max-file: "5"`, and `compress: "true"` on every
service (W4; see "The driver loop and end sequence" for the arithmetic), and
`entrypoint: ["logit", "--log-format", "json", "run", "/config.yaml"]`.

| Service | Image and settings |
|---|---|
| `victoria-metrics` (role `backend`) | `victoriametrics/victoria-metrics:v1.152.0`, `-retentionPeriod=100y`, `ports: ["127.0.0.1::8428"]`, so the host driver polls freshness every 30 s and dumps the final export with no checker container. The driver re-reads the port after any start of this service |
| `logit` (the SUT, role `sut`) | `depends_on: [victoria-metrics]`, not required from W5, when `victoria-metrics` moved to the compose profile `local` the driver enables for a local target only; `environment:` passes each variable an external target may list (`DD_API_KEY`), empty when unset |
| `generator` (role `generator`) | `depends_on: {logit: {condition: service_healthy}}`. An override can add more generators (W8b), each with `extends` from this one and its own config volume |
| `kafka` (role `broker`) | `apache/kafka:4.2.0`, a single-node KRaft broker in the compose profile `kafka`, which the driver enables for a Kafka target only; one partition per topic, topics created on first use, a `kafka-broker-api-versions.sh` health check the SUT's optional `depends_on` waits for. No published port: the driver reads the topic through `docker exec` |

Each service's role is its `soak.role` label (W7b). A scenario's `compose` override is passed
after the base file, `-f compose.yaml -f <override>`, on every `compose` call, and copied into
the run directory's `configs/`. Before `up`, `prepare()` runs `docker compose config --format
json`, which merges and interpolates the files with no daemon call, builds `{service: role}`
from the labels, checks the scenario against it (§2), and records it in
`scenario.resolved.json` as `services`. The self-test passes a role map instead, so no part of
it can reach Docker. An override that adds a service with `extends` names the base file as
`file: compose.yaml`: compose resolves the path against the project directory, the first
file's, not the override's own.

Configs mount from absolute paths in `compose.env` (`SOAK_SUT_CONFIG`, and one variable per
generator, `SOAK_GENERATOR_CONFIG` for `generator`; §2's "Sources"), which the driver passes
with `--env-file`, because `sudo` strips the
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
| a `generator` | its own UDP into the SUT, and no other generator's |
| a `backend` | its responses |

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
2. Resolve the service roles (§3), run `compose up -d --wait`, record the container IDs, and
   poll each backend's `/health`.
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
     when `internal` is wedged), and per sink the freshness of its backend: the newest sample
     timestamp across the sink's `vm_selector` from `/api/v1/export` with `start` 120 s back,
     skipped while a step holds that backend. Each backend has its own published port, re-read
     after any revert of a fault on it. Each query covers those 120 s
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
     A failed chunk's record keeps the tail of the CLI's error from its temporary stderr file.
     The cursor is per container id, which a stop and start keep.
   - **Fail fast:** a `logit` container leaving `running`, or a `StartedAt` change, with no step
     behind it ends the timeline at once. The end sequence and collection still run. Inside an
     `exit` step (W7a) the container may be `running` or `exited`.
4. End (for an external target, W5, see "External targets"): revert active faults; `docker stop -t 30` every generator; wait until every sink's stored
   total holds across two aggregate windows (90 s bound); `docker stop -t 60` the SUT, expecting
   exit 0; call each backend's `/internal/force_flush`; write each sink's
   `vm-export/<sink>.jsonl` from its backend's `/api/v1/export` with `match[]=<vm_selector>`
   and `start=0`.
5. Collect: the last `docker logs` chunk per service, from its cursor to the end, into
   `.stdout` and `.stderr`, and `inspect/<svc>.json`.
   Then `compose down -v --remove-orphans`, unless `--keep`. A `try`/`finally` makes SIGINT still
   collect, and the driver routes SIGTERM and SIGHUP into SIGINT's `KeyboardInterrupt`, because
   Python's default for both ends the process without running `finally`. A signal the driver
   inherited as ignored stays ignored, so `nohup` keeps a run alive when bash resends SIGHUP to
   its jobs as the terminal closes.
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
  action leaves (0, or 137 for a kill), or inside an `exit` step (W7a), from 1 s before its
  apply to 1 s after its revert, with the step's `code`; every `exit` step sees its exit, or the
  row FAILs "expected exit did not happen"; the final exit is 0; and `OOMKilled` is false.
- `restarts`: every `StartedAt` change matches a scheduled start or restart, an `exit` revert
  that started the container included.
- `self_log`: FAIL on an `exiting` line with `code != 0`, a `key == "thread_panicked"` line, or
  the raw `thread '…' panicked at` text, which isn't JSON, so stderr is checked line by line.
  An ERROR line keyed `retrying` is expected inside a fault window and WARNs outside one. A
  WARN line keyed `send_failed`, or whose message is `degraded`, does the same. Any other
  ERROR line FAILs. Inside an `exit` step's window (W7a), the `exiting` line with the step's
  `code`, an ERROR line keyed the step's `key`, and the `Error: ` text line `main` prints right
  after that `exiting` line are expected; the step FAILs the row when either of the first two is
  missing.
- `ready`: a container is ready within 30 s of each start, and readiness is judged from the
  status word `logit ready` reports (W7a). The image's `HEALTHCHECK` runs `logit ready` every
  10 s, and `watchdog.jsonl`'s `health_log` records Docker's newest result, whose `Output` is
  `ok`, or a word and the HTTP status, as in `stalled (503 Service Unavailable)`. The row reads
  the first word of each, timed at the result's `Start`, not at the watchdog's poll, and counts
  each `Start` once. Every sample outside fault windows reads `ok`; a `mark` with `ready = W`
  needs at least one sample reading W inside its span; and `degraded`, which means a node has
  exited, is accepted only inside an `exit` step, fault window or not. A paused container fails
  the `HEALTHCHECK`, so `ok` is judged only outside fault windows. A run directory whose
  watchdog recorded no `health_log` falls back to `Health.Status` being `healthy` outside fault
  windows, and a `mark` with `ready` then FAILs, since its word can't be read.
- `progress`, which tells a hang from a slow drain. Outside fault windows, every SUT sink's
  `batches.delivered` increases in every `progress_window`; stdout NDJSON never gaps more than
  three `internal` intervals while the container runs unpaused; and every sink's freshness
  sample is under 30 s old. After a fault, `retrying` at 1 with `retries` rising and
  `buffer.batches` falling is a slow drain: PASS within `recovery_bound`, WARN beyond it. No
  deliveries with `retries` and `errors` flat while `buffer.batches` is above 0 or `inbox.full`
  rises is a **hang, FAIL**. Delivery windows covering under 5% of the run after warmup WARN,
  so a PASS that rests on a few windows says so.
- `rss_slope` and `fd_slope`: the least-squares slope of `logit.process.memory.resident.bytes`
  per life, over samples outside fault windows after warmup, against `rss_growth_mib_per_hour`.
  `logit.process.fds` at the end minus the warmup median is at most `fd_growth`, and returns to
  baseline at the first steady-state sample after each recovery. `docker stats` RSS is the
  fallback series. Judged slopes covering under 5% of the run after warmup WARN. From W8c both
  rows judge each `logit` service apart, the SUT and every generator, with one detail line per
  service and the worst status, as the per-sink rows do; a generator that interns what the SUT
  interns grows beside it, and one verdict for both would say nothing about the SUT. A
  scenario's `[expected]` table can set a service's verdict (§2, "Expected verdicts").

**The ledger and identities (W1b).** One unit runs through the ledger: a datagram is one line is
one event is one increment.

From W8b the ledger also follows each `[[ledger.source]]`, a generator and its SUT listener.
The listener symbols W, K, D, B, E, and X are kept per listener and summed over the listeners;
G is per source. `ledger.wire` judges each source and `ledger.intake` each listener, with one
detail line per source or listener and the worst status. From the edge into `aggregate` on,
every row reads the sums, because the sources' events merge there: `ledger.edge` judges ΣE
against A, and the residual, `ledger.egress`, `ledger.summary`, the kill band, and `recovery`
read summed symbols. Both legs of a fan-in run in one unit, a datagram that is one line, one
event, and one increment, so the sums are of one unit.

From W7b the ledger follows each `[[ledger.sink]]`. A row about a sink (`progress`,
`identity.sink`, `recovery`, `ledger.edge`'s sink edges, `ledger.egress`, `ledger.windows`,
`ledger.replay`, and `ledger.sent`) keeps its id, judges each sink, reports one line per sink,
and takes the worst status; `report.py` is unchanged.

| Symbol | Source |
|---|---|
| G | per source (W8b), its generator's `logit.output.messages{component=<generator_sink>}`; precondition: equals that sink's `logit.output.datagrams` |
| W | per listener (W8b), SUT `logit.input.datagrams{component=<sut_listener>}` |
| K | per listener, SUT `logit.input.kernel.dropped` |
| D | per listener, SUT `logit.component.datagrams.dropped` under every reason, from telemetry; the listener's stderr shutdown lines only cross-check its `shutdown` part, and a killed life counts none |
| B | per listener, SUT `logit.input.messages.dropped{reason=malformed}` plus `logit.input.frames.dropped{reason=undecodable}` plus `logit.input.frames.dropped{reason=oversize}` (W8b): a line the decoder refused, a datagram it refused whole, and a line over `lines_in`'s `max_line_bytes`, which its decoder drops with a throttled diagnostic and no decoder error. Each is 0 on a listener with no such rule |
| E | per listener, SUT `logit.component.events.sent{component=<sut_listener>}` |
| X | per listener, SUT `logit.component.events.dropped{component=<sut_listener>}` under every reason (W7a): events the listener counted as sent and then dropped, such as a batch sent into the inbox of a node that has failed (`closed_consumer`) |
| T_in[node], T_out[node] | with a source's `sut_transforms` (W8b), each node's `logit.component.events.received` and `logit.component.events.sent` |
| A | aggregate `logit.component.events.received` |
| A_sent | aggregate `logit.component.events.sent` (W7b), counted once per send however many consumers a fan-out reaches |
| S_in[sink], T_sent[target] | each sink's `logit.component.events.received`, and under `split = "route"` its target's `logit.component.events.sent` (W7b) |
| Ab | aggregate `logit.transform.metrics.absorbed` |
| L_in, L_emit, L_drop, L_err, L_flush, L_sent | with `[ledger] sut_script` (W7a), the script's `logit.component.events.received`; `logit.script.events.emitted` summed over its `outcome`s, which counts events, one per event an `emit_many` returns; `logit.component.events.discarded`, one per event `process()` returns `nil` for; `logit.component.events.dropped{reason="script_error"}`, one per event a script error loses, counted beside `logit.component.errors{reason="process"}`; `logit.component.flush.events`; and `logit.component.events.sent`, which counts what `process()` and `flush()` send alike |
| V[sink] | per sink (W7b), the reset-aware total of `vm-export/<sink>.jsonl`: add each series' first value, then every non-negative step, and on a decrease or at a SUT life start add the new value. V splits per SUT life at each series' decreases and at each life start, placing a sample by its timestamp, which is the SUT's own window time; a restart whose first value isn't below the last one shows no decrease. A series should have one segment per SUT life, else WARN |

The generator sets `max_packet_bytes: "32"`, so one datagram carries one line of about 20 bytes,
and G uses `output.messages` (what the kernel took), not `events.sent`. A `syslog_out` over UDP
sends one message per datagram, with no packing to bound, and counts both (W8b). While the SUT is down,
`statsd_out`'s per-batch name resolution fails `Clean` and nothing is sent.

**Which life a hop is judged in.** Every SUT config gives `internal` a dedicated pipeline, so
its final drain runs after every data node has exited
([`internal-telemetry.md`](../design/internal-telemetry.md), "`internal`: the drain"): after
the listener decodes what its receive queue still holds and flushes its accumulator
(`FlushReason::Shutdown`), `aggregate` absorbs those events and flushes its close-time window,
and the sink makes its last delivery. Shutdown drops reach that drain too: the listener's
`datagrams.dropped{reason="shutdown"}` goes into D, and a node's
`batches.dropped{reason="shutdown"}` beside `ledger.egress`. The stderr copies, the listener's
`warn` lines and the `drain complete` line's `batches_dropped`, cross-check them: a life whose
stderr count exceeds telemetry's WARNs, and neither is added to telemetry. A datagram count in
telemetry above stderr's doesn't WARN: under `receive: { overflow: block }`, a cancelled blocked
push counts its datagrams as shutdown drops without logging a line.

- The final SUT life ends after the generator stops and the VictoriaMetrics total holds (§5's
  end sequence), so nothing arrives after its final drain. `ledger.intake`, `ledger.edge`,
  `ledger.aggregate`, and `ledger.egress` are judged with no tolerance for it.
- An earlier SUT life ends under load, at a scheduled `stop`, `restart`, or `kill`, or at an
  `exit` step's expected exit (W7a), which drains as a `stop` does. A life a `stop`, `restart`,
  or `exit` ended is compared at its last ordinary drain, before the shutdown drain, and its
  hops are reported. Its residual W − D − B − X − Ab, what it read but had neither absorbed nor
  dropped, counted, by that drain, must lie between 0 and the residual R that can be in flight
  at the signal: the receive queue (`receive.max_datagrams`) plus 67 batches of at most
  `receive.batch_max_events` each: the accumulator, the batch the listener holds while its send
  waits for a slot on a full inbox, `aggregate`'s 64-slot inbox, and the batch `aggregate` has
  taken off that inbox but not yet absorbed. A script between them (W7a) adds its own 64-slot
  inbox and the batch it holds, 65 batches more, and its counted drops and errors leave the
  residual: W − D − B − X − (L_drop + L_err) − (Ab − L_flush). `aggregate`'s close-time flush
  delivers that residual into V, and the final drain runs after that flush, so `ledger.egress`
  judges the life's W − D − B − X − V as it judges the final life's Ab − V.
  The [0, R] bound only catches a residual no shutdown could leave. With several listeners (W8b)
  the residual is summed over them, and R is the sum of each listener's own receive queue and 2
  batches of its own `batch_max_events` (its accumulator and its held batch), 65 more for each
  node of its source's `sut_transforms`, plus `aggregate`'s 65 and a script's 65 once, in
  batches of the largest `batch_max_events`. With one listener and no transforms that is the
  queue plus 67 batches, as above.
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
  **uncounted**; both FAIL. A SIGKILL counts no shutdown drops, so a gap can't be counted, and D
  has no shutdown part for this life.
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
  on a generator; any `logit` stop, kill, pause, restart, or partition) exceeds
  `wire_loss_outside_faults` times the lines sent there, beyond a tolerance of one generator
  batch per window edge. The two processes' drains are out of phase, so G is interpolated
  linearly at each SUT drain timestamp; across a run of steady intervals the interpolation
  telescopes, and each edge of the run errs by at most one batch. Each steady run's gap counts
  at 0 when W + K exceeds G in it, so a surplus in one run can't net out loss in another. The
  generator sink's own
  `drop_newest` loss while the SUT is unreachable is counted and never in G; the row shows it
  beside the wire gap. The generator's `self -> telemetry` pipeline is dedicated too, so its
  final drain runs after its sink's last send and exports it. At the end, G can still fall short
  of W + K, because a send the generator's sink cancels at shutdown loses its counts. A negative
  gap of up to the generator sink's `buffer.batches` at its last drain plus one batch (100 lines
  each) is allowed. From W8b the row judges each source alone: its own G against its listener's
  W + K, with its own generator's batch as the tolerance, its own end allowance, and its own
  datagram precondition. A fault on a generator is UDP-affecting for that generator's source
  only, so its window excuses nothing on another source's wire; a UDP-affecting fault on any
  service that isn't a generator, such as any `logit` stop, excuses every source. One detail
  line per source; the worst status wins.
- `ledger.intake`: W − D == E + B, for the final life, per listener from W8b, one detail line
  each, a listener off its balance first.
- `ledger.edge`: E == A, a single-consumer edge, for the final life; from W8b ΣE == A, E summed
  over the listeners, which every source's events reach. A source's `sut_transforms` (W8b) are
  judged hop by hop: its listener's E == T_in[first node], T_in == T_out at every node (each
  forwards every event it receives), T_out[node] == T_in[next node], and the last node's T_out
  takes that listener's E's place in the sum. With `sut_script` (W7a),
  L_sent == A, the edge from the script to `aggregate`. From W7b, each sink's edge too: under
  `copy`, A_sent == S_in[sink], since a fan-out counts one send per batch; under `route`,
  T_sent[target] == S_in[sink] for the target that feeds it.
- `ledger.script` (W7a), with `sut_script`, else SKIP: for the final life, E == L_in (the
  listener's edge into the script), L_in == L_emit + L_drop + L_err (every event emitted,
  dropped, or failed on), and L_sent == L_emit + L_flush. Earlier lives are reported.
- `ledger.aggregate`: Ab == A, for the final life.
- `ledger.egress`, per life and per egress group (W7b): under `split = "copy"` each sink is a
  group with its own V[sink], since each holds all of `aggregate`'s output; under `route` the
  sinks form one group whose V is ΣV[sink], since each holds its target's share. Per group: Ab −
  V for the final life and a killed one, and W − D − B − X − V for a stopped earlier one, the
  residual plus Ab − V, because `aggregate`'s close-time flush delivers the residual to
  VictoriaMetrics before the final drain. The detail shows both parts. With
  `sut_script` (W7a), Ab becomes Ab − L_flush, because each `flush()` event carries one metric
  whose series `vm_selector` excludes, and a stopped earlier life's gap becomes
  W − D − B − X − (L_drop + L_err) − V. The subtraction holds only while each flush event carries
  one metric, which the scenario's script guarantees. `internal` drains last, so the final drain
  exports the script's drops and errors on a stopped life's residual too, and L_drop and L_err
  include them. A gap of 0 passes. A positive gap is lost increments. It is reported with the
  life's telemetry `batches.dropped` that can explain it beside it, under every reason, and is
  **counted** only when that count is above 0. The reason set is open: `shutdown`, a queue's or
  spool's `overflow_oldest` or `overflow_newest`, a spool's `disk_full`, `disk_io_error`, or
  `frame_too_large`, and any reason the runtime adds, all count, and the detail names each
  reason with its count, such as `counted (batches dropped: overflow_oldest 12, shutdown 2)`.
  The two are never reconciled: the count is of batches, and under `temporality: cumulative` a
  dropped batch loses only the increments since its series' last delivered total, which no log
  line states. A drop can explain a group's gap at any node but a sink outside the group: under
  `copy` (W7b), each sink is its own group, so a drop at the script, `aggregate`, or the sink
  itself counts the sink's gap, and another sink's drop doesn't, since that sink's copy is all
  it lost; under `route`, the group holds every sink and every node's drops count the summed
  gap; with one sink, every node's count. The detail shows the other sinks' drops it left out.
  A `rejected` batch, which the destination refused, or a `disk_corrupt` one, which a spool
  couldn't read back, counts the gap like any other drop, but FAILs the life and group it falls
  in whatever the gap, with the reason named: neither happens against a healthy backend, and
  under cumulative temporality a later total can hide the rejected batch's increments, leaving
  a gap of 0. The stderr `drain complete` line's `batches_dropped` counts shutdown drops only,
  process-wide, and never counts a gap; it only cross-checks telemetry's `shutdown` total. A
  killed life's gap passes inside its band (see "Which life a hop is judged in"); a SIGKILL
  counts no shutdown drops, but the life's drains can carry others, such as a queue's overflow
  during an outage, which the detail reports beside the band without judging them. Any other
  gap is **uncounted loss, FAIL**.
- `ledger.windows`, only under `[ledger] vm_every_window = true`, else SKIP: in every sink's
  export (W7b), per SUT life and series, consecutive samples are at most 1.5 × the SUT
  config's `aggregate` interval apart,
  unless the gap is idle; a killed life's last sample is at most 1.1 × the interval before the
  kill, because the kill discards only the window in progress and 10% covers a flush's own
  lateness; and a later life's first sample is at most 2 × the interval after the life starts,
  the first window plus startup. A series with no sample in a life FAILs too. Each FAIL names
  the series and the gap. The interval comes from the config, never the median spacing, which
  moves once half the windows are missing. A last window lost to a kill that lands within
  0.1 × the interval of the life's last flush passes, and the kill time is the `docker kill`
  call's start, which can run about a second ahead of the signal.

  A gap is idle, and excused, when the `aggregate` the `[ledger] sut_aggregate` id names sent
  no batch in the span its missing windows would have been flushed in. An idle flush sends
  nothing, because `aggregate` emits a series only in a window that updated it, so no window
  existed to store. The row counts `logit.component.batches.sent`, one per send and none for an
  idle flush; `aggregate` emits no `flush.events`. The span follows from the drain cadence. A
  flush at f is reported by the drain at f + d, where d runs from about 0 to one drain period
  p (the median spacing of the SUT's drains), because the `internal` and `aggregate` timers
  share a phase and either can fire first at a shared tick; in recorded runs d is within
  0.13 s of 0 or of p. For a gap between samples at a and b, the missing windows flush at
  a + k × interval for 0 < k < (b − a) / interval, so their drains fall in
  [a + interval, b − interval + p]; a's own drain falls in [a, a + p] and b's in [b, b + p].
  The row counts over [a + (interval + p) / 2, b − (interval − p) / 2), which splits both
  separations in half and leaves a margin of (interval − p) / 2 on each side for timer jitter:
  at a 10 s interval and 5 s drains, [a + 7.5 s, b − 2.5 s). The rule needs p under the
  interval, and with p at or above it no gap is excused. When the count is above 0, windows
  were emitted but are missing from the store, and the gap FAILs with the count. The rule
  doesn't read fault windows: an idle gap is legitimate anywhere, and a window lost beside a
  fault FAILs because its flush is counted. A paused SUT writes no drain and flushes nothing,
  so a gap across a pause counts 0 over no drains and is excused; no window closed during the
  pause, so none could be lost. The count is per component, not per series, so a series idle
  while others flush FAILs; set `vm_every_window` only where every series is written in every
  window. The detail names each excused gap with its offsets, its series count, and the
  batches and drains counted.
- `ledger.replay`, for each killed life followed by another and each sink, else SKIP: the next
  life's
  first-drain `buffer.disk.replayed` equals the killed life's `buffer.batches` at its last
  drain, one batch in flight allowed. `identity.sink` balances on whatever `replayed` reports,
  so only this row sees a replay two or more batches short of what the killed process queued;
  a single lost batch is `ledger.windows`'s to catch, as a lost window. A killed life with no
  disk spool replays 0, so this row FAILs whenever it had batches queued.
- `identity.sink`, per sink: at the last quiet SUT drain before shutdown,
  `batches.received + buffer.disk.replayed == batches.delivered + batches.dropped (every
  reason) + buffer.batches`, allowing one batch in flight. A disk spool opens holding the
  batches an earlier process queued, which this life delivers but never received. Checked at
  the last quiet drain, where the one-batch allowance covers what is in flight, rather than at the
  final drain, whose shutdown drops `ledger.egress` already reads.
- `recovery`, per fault and per sink: within `recovery_bound` of each fault's end, some SUT
  drain shows the sink's `retrying` at 0,
  `buffer.utilization` under 0.05, or under 1.0 with `buffer.batches` at most 1 (one batch in a
  2-batch queue is half of it, and one that fills its queue isn't drained), `receive.utilization` under 0.05 at every listener, and an ingest rate over the drain
  interval ending there of at least 95% of the warmup baseline. From W8b the ingest rate is
  summed over the listeners and the baseline over the generators, each generator's from its own
  warmup drains. Only intervals after the fault's
  end, inside one SUT life, and clear of every other fault are eligible, because a dense schedule
  often starts the next fault before `recovery_bound` runs out; a fault with no eligible interval
  is listed as skipped, and with no fault judged the row WARNs. A `rate_behind` diagnostic from
  any generator makes the throughput part WARN, because the generator limited the rate, not the
  SUT.
- `ledger.summary`, for the final life: uncounted = (W − D − E − B) + (E − A) + (A − Ab) + (Ab − V),
  which must be 0, with each term shown beside it and wire loss beside them as "by design".
  From W8b the first two terms read the listeners' sums, and the wire loss sums every source's.
  With `sut_script` (W7a), E − A becomes (E − L_in) + (L_in − L_emit − L_drop − L_err) +
  (L_emit + L_flush − L_sent) + (L_sent − A), the egress term is Ab − L_flush − V, and the
  script's L_drop and L_err are shown beside them as counted. From W7b there is one egress term
  per egress group, each of which must be 0 or counted on its own, so a surplus at one sink
  can't net out loss at another. When an egress term is counted, it is shown as counted and the
  row judges the rest. A `rejected` or `disk_corrupt` drop in the final life's group FAILs the
  row as it FAILs `ledger.egress`, named beside its term.
- `ledger.sent` (W5), for an external target, else SKIP: the ledger ends at each sink's own
  telemetry; see "External targets".
- `drops` (2026-10-13): every `logit.*.dropped` counter the SUT emitted that no row above
  judges. Under [ADR `telemetry-loss-vocabulary`](../adr/telemetry-loss-vocabulary.md) the name
  alone says a count is loss, so the row reads every such name rather than a list. It leaves
  out the terms the ledger balances (every `batches.dropped`, a node's `events.dropped` beside a
  batch drop of the same reason, `bytes.dropped`, a listener's D, K, X, and B terms, and the
  script's L_err) and, on an external or Kafka target, the sink's `records.dropped` and
  `metrics.dropped`, which `ledger.sent` judges. A `rejected` or `disk_corrupt` reason FAILs;
  any other WARNs. On an external target `ledger.egress` SKIPs, so `ledger.sent` FAILs a
  `rejected` or `disk_corrupt` batch drop at a sink, and one at any other node stays in this
  row.
- `expect.<name>`, one per `[[expect]]` table: a scenario's own bound on one metric over a
  window around one step, or over the whole run (W8c); see "The scenario schema".

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

### 8. External targets (W5)

A scenario whose sink delivers outside the stack declares it, and nothing queries the
destination:

```toml
[target]
kind = "external"          # default when absent: "victoria-metrics" (the local service)
name = "datadog"           # a label for results; no backend query
env = ["DD_API_KEY"]       # variables the run needs, from the external env file
```

`validate()` refuses an unknown key or kind; for an external target, a `name` that isn't
lowercase letters, digits, `_`, or `-`, an `env` that isn't an array of distinct variable names,
or a name compose.yaml doesn't pass to the SUT (`scenario.EXTERNAL_ENV_PASSED`, today
`DD_API_KEY`); a `name` or `env` on the local target; a `[[ledger.sink]]` `backend` or
`vm_selector`, or `[ledger] vm_every_window`, on an external target, whose sinks name only
their `id`; and any fault, fixed or random, on `victoria-metrics`, which an external target's
stack doesn't run (from W7b, refused as an unknown service once the driver has read the stack's
roles).
`scenario.resolved.json` records the target, and a run directory with none is a local one.

**Credentials.** `script/soak` reads the variables from `SOAK_EXTERNAL_ENV`, by default
`perf/results/soak-external.env`, as `script/splunk-interop`'s cloud mode reads its env file.
The file can be in shell form: `scenario.read_env_file`'s docstring is the grammar (`export`
lines, one pair of quotes removed, `#` comments). It refuses a file the repository holding it
doesn't ignore, and a git failure other than "not a git repository" rather than reading it as
no repository. It refuses a file that doesn't set a listed variable or sets it empty, and one
that sets a listed variable to a value containing `$`, `'`, `"`, a backtick, `\`, `#`, or
whitespace, naming the variable (and the kind of character) and never its value
(`soak.py target <scenario> --env-file`, which the driver repeats before any docker command).
Compose reads the private copy below with dotenv rules: it interpolates `$NAME`, cuts a value
at ` #`, parses quotes and `\`, and fails `up` on an unmatched quote with an error that quotes
the value, which the driver would record in `timeline.jsonl`. A value without those characters
stays literal. Docker never reads the operator's file, because neither `docker run --env-file`
nor compose's reliably reads an `export` line or strips quotes. The script writes the listed
variables as plain `KEY=value` lines to a private copy (mode 0600, in a new temporary directory
outside the repo and the run directory) for `logit validate` and removes it before the driver
starts; the driver writes its own for every `compose` call's second `--env-file` and removes it
when `execute()` returns or `prepare()` raises. The driver installs its SIGTERM and SIGHUP
handlers before it writes the copy, so only a SIGKILL leaves one behind. No value is passed as
a command-line argument, and the run directory holds none.

**The driver.** Compose runs without the `local` profile, so without any backend. The
driver skips the backends' `/health` poll, the freshness samples, `force_flush`, the quiet
wait, and the export. At the end it reverts active faults, stops the generator, and waits, up to
120 s, for a SUT drain at least two `aggregate` intervals after the generator stopped in which
every sink's `buffer.batches` reads 0 and `retrying` 0 or unset, read from the newest 400 lines of the
SUT's stdout; then it stops the SUT. The wait is a `sink_drained` phase in `timeline.jsonl`,
`held` false when the bound ran out, with each sink's last state under `sinks`. Faults keep their meaning: `netem` and `partition` on
`logit` shape or cut its egress to the internet, and `kill`, `stop`, and `pause` on `logit`
work as before.

**The checks.** `ledger.egress` and `ledger.windows` SKIP with "external target: no backend
query", `ledger.summary` shows its Ab − V term as SKIP and judges the other three, and
`progress` reports no freshness samples. `ledger.replay` reads only telemetry, so it stays, as
do `ledger.wire`, `ledger.intake`, `ledger.edge`, `ledger.aggregate`, `identity.sink`,
`recovery`, and `self_log`, whose sink fault keys (`retrying`, `send_failed`, `degraded`) are
the runtime's, not a sink's own; `datadog_out`'s diagnostics are WARN lines, which `self_log`
doesn't judge. `ledger.sent` ends the ledger at the sinks, each judged by the verdicts below
and named in its lines. It reads `datadog_out`'s telemetry and
diagnostics as the sink's module doc describes them (`crates/logit-outputs/src/datadog.rs`,
"Faults, retries, and duplicate safety", whose response-class table maps each answer to a class
and a diagnostic, and "Telemetry"). This list is the canonical copy of its verdicts:

- Per SUT life, at its last quiet drain (the final life's second-to-last, an earlier life's
  last), `batches.received + buffer.disk.replayed == delivered + dropped (every reason) +
  buffer.batches`, one batch in flight allowed. Off: FAIL.
- The final life's `buffer.batches` above 0 at that drain, or the last `sink_drained` phase in
  `timeline.jsonl` with `held` false: FAIL, naming the batches left unsent. The identity
  balances a queued batch, so it can't catch one the run ended with.
- A batch dropped `rejected`, or records dropped `rejected` or `oversize`: FAIL, with the
  status and the first stderr line with the key that explains it. A record drop quotes
  `request_rejected` or `oversize` (refused by the destination, or too large to send).
  `records.dropped{reason="rejected"}` also counts the series a `202` body names, so it quotes
  `request_rejected`, `series_rejected`, or both, whichever the sink logged. Under per-request
  verdicts a rejected request drops its records and the batch survives when another request
  was accepted, so both the batch and the record counter are read.
- On an external target, a batch dropped `disk_corrupt` (2026-10-13): FAIL, because
  `ledger.egress`, which FAILs it on a local or Kafka target, SKIPs there.
- An `api_key_rejected` or `request_refused` line from the sink on stderr: FAIL, quoting the
  first. The sink reads that answer as `Refused` and holds its queue with no exit, so the run
  can't deliver until the key or the endpoint is fixed.
- No `logit.output.requests` point, or none of class `2xx`: FAIL.
- Any other counted drop (a batch's `overflow_*` or `shutdown` reason, a record's `stale`, `too_many_tags`, or other pre-send reason, the
  encoder's `metrics.skipped`, and its `metrics.dropped` for an encode fault): WARN.
- A life whose stderr `drain complete` `batches_dropped` differs from its telemetry
  `batches.dropped{reason="shutdown"}`: WARN.
- A request of class other than `2xx` (`4xx` holds a `429` too, `5xx`, `network_error`) in a
  drain whose interval overlaps a fault window is the fault's retry; outside every one it
  WARNs with the count, because the destination has a floor of errors of its own.

The row is a PASS whose detail starts with `SENT` when nothing above fires. `SENT` is a PASS
rather than a fifth status so `results.md` keeps PASS, WARN, FAIL, and SKIP and `worst()` needs
no new rank. A `2xx` is the end of the evidence: the harness can't see a point the intake took
and later discarded, nor tell an overwritten resend from a duplicate. `datadog_out` resends a
whole batch after an ambiguous failure under `at_least_once`; a resent series point overwrites
the stored one and a resent log is stored again
([`docs/known-gaps/datadog.md`](../known-gaps/datadog.md)), so the shipped scenario sends series
only.

**The scenario.** `statsd-datadog` is `statsd-vm`'s generator and topology at 2,000 lines/s,
renamed `logit.soak.requests_{seq%100}`, through a 10 s `aggregate` into `datadog_out` (default
site, `api_key: !env DD_API_KEY`, `buffer: {disk: {path: /tmp/soak-spool}}`), with a `set` that
stamps `host.name` and `datadog.interval`. The aggregate is `temporality: delta`: the series
route skips a cumulative `Sum`, and a statsd counter is a Datadog `count` of each window's
increment. Its 12-minute cycle runs `netem delay 300ms 100ms` (90 s) and `netem loss 20%`
(60 s) on `logit`, a 60 s partition, a 30 s pause, and a 30 s kill, each followed by at least
90 s. Its expectations: `batches.dropped` 0 through each fault, `retrying` 0 after the
partition, `records.dropped{reason="stale"}` 0 through the partition and the kill, and
`buffer.disk.truncated` 0 through the kill. `retrying` is exported only once the sink first
retries, so a row after a netem step, which TCP can absorb, would fail on a gauge never set;
and with the intake reachable the sink's queue is often empty at the kill, so no row asks for a
replay above 0: `ledger.replay` ties the replay to the killed life's last `buffer.batches`.

### 8b. Kafka targets

A scenario whose sink is `kafka_out` names the stack's own broker as its target, and the ledger
reads the topic back as its V:

```toml
[target]
kind = "kafka"
topic = "soak"             # the topic the SUT's kafka_out writes; required
# name = "kafka"           # optional, a label for results
```

`load()` refuses a Kafka target with no `topic` or with one Kafka wouldn't create (ASCII
letters, digits, `.`, `_`, and `-`, up to 249 characters, not `.` or `..`), an `env`, the
VictoriaMetrics ledger key `vm_every_window`, and anything but one `[[ledger.sink]]`, the
`kafka_out` whose topic is V, with no `backend` or `vm_selector`. The `kafka` service carries the
role `broker`, and the `kafka` profile is the only one that runs it, so a fault on `kafka` is
known for a Kafka target only, and one on `victoria-metrics` is refused for it as an unknown
service, as for an external target; `kill` stays the SUT's only. Every fault on the broker
affects egress. A `topic` on any other target is refused.

**The driver.** Compose runs the `kafka` profile in place of `local`, with no env-file copy.
The driver skips everything that reads VictoriaMetrics, and its end sequence waits for the sink's
queue to empty, as for an external target. After the SUT stops it reads the topic in the broker
container with the image's own tools: `kafka-get-offsets.sh` for each partition's end offset
(`kafka-offsets.txt`), then `kafka-console-consumer.sh --from-beginning --max-messages <their
sum>` (`kafka-topic.ndjson`, one record value per line). It records a `kafka_read` phase with
both counts, or `kafka_read_failed` when either read fails or the consumer's count differs from
the offsets.

**V.** The SUT's `aggregate` runs `temporality: delta`, so each record, one event in
`kafka_out`'s `format: json`, carries its window's increments. V is the sum of every distinct
record's `sum` metric values, credited to the SUT life its event `timestamp` falls in
(`topic.py`). Records are deduplicated on the value's raw bytes, because a resend re-encodes the
same event to the same bytes; summing the copies would let a duplicate cancel a loss of the same
size. No reset logic applies.

**The checks.** `ledger.egress` and `ledger.summary` judge Ab − V against the topic as they do
against VictoriaMetrics, with one difference: a negative gap in a life that wasn't killed is a
`surplus`, a FAIL. A topic not read back, an empty one, or a record that isn't a JSON object FAILs
`ledger.egress`. `ledger.windows` and the freshness samples SKIP. `ledger.sent` judges the sink
as for an external target, with `kafka_out`'s request classes: `ok` is accepted, and the
diagnostics it quotes are keyed by the drop reason (`rejected`, `invalid_timestamp`,
`invalid_record`, `oversize`); its PASS doesn't read `SENT`, because the ledger goes on to the
topic. `ledger.topic` compares the topic's distinct record count T with the records the sink
saw accepted, A (`logit.output.records`, every SUT life): T below A FAILs, records the broker
acknowledged and no longer holds; T above A FAILs, unless the sink dropped an attempt it never
saw answered, which may have landed, and then WARNs. Across a killed SUT life a T − A difference
in either direction WARNs and names the life, because the kill loses that life's last telemetry
interval of A and a replayed spool batch counts twice in A. A topic not read back FAILs. It
judges each SUT life's duplicate copies too. Within one producer epoch the broker discards a resend, so a
duplicate FAILs in a life with no epoch bump, and WARNs in a life that ended in a restart or
kill (the next life resends under a new producer id) or whose sink counted a drop that bumps
the epoch (`rejected`, `invalid_timestamp`, `invalid_record`, `ambiguous_at_most_once`;
`checks.py`'s `EPOCH_BUMP_REASONS`), as [`docs/known-gaps/kafka.md`](../known-gaps/kafka.md)
permits. Every other target's `ledger.topic` SKIPs.

**The scenario.** `kafka-broker-restart` is `statsd-vm`'s generator at 2,000 lines/s through a
2 s delta `aggregate` into `kafka_out` (`format: json`, `acks: all`, idempotent, `timeout:
10s`), topic `soak`. Its 6.5-minute cycle restarts the broker, pauses it for 40 s, and stops it
for 60 s, each followed by at least 75 s. The 10 s `timeout`, under the pause, turns the pause
into response timeouts, `Ambiguous` attempts the broker deduplicates on the resend. Its
expectations: `batches.dropped` 0 through each fault, at least one `ambiguous` request through
the pause, so `ledger.topic`'s no-duplicate verdict rests on resends, and `retrying` above 0
during the stop and 0 after it.

### 9. Not in this stack

toxiproxy and TCP-level faults it alone can express; per-peer `tc filter` and `ifb` ingress
shaping until a scenario needs them (W2 at the earliest); a checker container; any CI job.

### 10. Next scenarios

Every shipped scenario drives UDP datagrams into one `aggregate`: `statsd_in`, and from W8b in
`fanin` `lines_in` beside it, on §3's base stack plus any services its `compose` override adds.
`[ledger]` names one `[[ledger.source]]` per generator and its listener, and one
`[[ledger.sink]]` per sink after `aggregate`. The scenarios below widen
that, grouped by cost; the Workstreams table gives the landing order. Each states what it
proves, its faults, its rows, and the harness support it needs; §2's validation, §5's end
sequence, and §6's checks hold unless an item says otherwise.

**Cheap now.** Each is a scenario directory on the base stack, plus an override's services
where it needs them, and a small check extension.

1. **Lua in the path (built in W7a).** `statsd_in → lua → aggregate → prometheus_out`, which
   drives `run_lua` and `/readyz`'s `stalled`, the one not-ready answer for a node that hasn't
   exited ([`docs/known-gaps/runtime.md`](../known-gaps/runtime.md), "Admin endpoint, readiness,
   and release image"). Three scenarios
   ([ADR `lua-runaway-script-bounds`](../adr/lua-runaway-script-bounds.md)):
   - `lua-path`: under the `statsd-vm` schedule in §2, the script rewrites each event, discards every
     53rd (`events.discarded`), fails on every 97th (`events.dropped{reason="script_error"}` beside
     `errors{reason="process"}`), and returns one
     rollup event from `flush()` each window.
   - `lua-stall`: a busy loop after N events reads `stalled`, then `ok`. The sandbox has no
     clock, so the loop is an iteration count sized past the 10 s `stall_after`, and a `mark`
     with `ready = "stalled"` covers it.
   - `lua-memory`: a VM over `max_memory` exits 2 with `memory_limit_exceeded`
     ([`docs/deploying.md`](../deploying.md), "Probes and exit codes"), inside an `exit` step
     whose revert starts the SUT again.

   Rows: `ledger.script` and the script terms in the ledger (§6); `ready` judging the status
   word; `exit` and `self_log` judging the expected exit. Built with `[ledger] sut_script`, the
   `mark` and `exit` actions (§2), and readiness scored from `health_log` (§6).
2. **Two sinks of one kind, failing independently (built in W7b).** `aggregate` fans out to two
   `prometheus_out`, each with its own VictoriaMetrics; faults on one leg, then both overlapping;
   then the same split through `route` and two `target`s
   ([ADR `target-components`](../adr/target-components.md)). A held branch slows the other only
   through its inbox: under `block` its buffer then its inbox fill, the shared send waits, and
   `logit.component.inbox.full` counts under the held consumer; under `drop_oldest` the other
   keeps delivering. Faults: `stop` and netem on one backend, then both. Rows: `ledger.egress`,
   `ledger.windows`, `identity.sink`, and `recovery` per sink; an `[[expect]]` on the healthy
   sink's `batches.delivered` during the outage. Built as `fanout-copy`, `fanout-block`, and
   `route-split` (W7b), and `fanout-stop-outage` (W8a: the SUT stopped inside one backend's
   stop, so that sink's queue drops at shutdown and the other sink's doesn't), with the scenario
   `compose` override, service roles read from the merged compose file, and `[[ledger.sink]]`
   with `split` (§2).
3. **Fan-in (built in W8b).** Two generators of different kinds into one `aggregate` and sink: statsd, plus
   `lines_in` with `kv_metrics` or remote-write into `prometheus_in`. Proves series merging and
   per-source loss attribution. Faults: netem, `partition`, and `stop` on each generator in turn.
   Rows: `ledger.wire` per source in its own unit, `ledger.intake` per listener. A remote-write
   leg carries cumulative totals, so it keeps its own series and balances as V does. Needs: item
   2's override, and `[ledger]` listing generators, G summed where the units meet. Built as
   `fanin`, with the `lines_in` leg: `generator-lines` sends RFC 5424 syslog over UDP
   (`syslog_out`, one message per datagram), and `kv`, `keep`, `kv_metrics`, and
   `keep_signals` turn each line into one `soak_lines` increment, so both legs stay in the
   datagram unit and G and W sum where they meet. `[[ledger.source]]` tables (§2's "Sources")
   list the generators, and the source's `sut_transforms` put the four nodes in `ledger.edge`.
4. **A full spool (built in W8a).** A small `buffer.disk.max_bytes` through a long
   `victoria-metrics` stop. Reaching `max_bytes` is `overflow`'s case (under `drop_oldest` the sink holds its head
   batch reserved while it retries, so the spool rejects each arriving batch and counts it
   `overflow_newest`; `overflow_oldest` needs a head that isn't reserved); `batches.dropped{reason="disk_full"}` counts only an `ENOSPC` write
   ([ADR `disk-backed-sink-buffer`](../adr/disk-backed-sink-buffer.md), "Decision"). Rows:
   `[[expect]]` on the drop reason, `ledger.egress` counted, then `progress` and `recovery`.
   Built as `spool-full` (`max_bytes: 128KiB`, `overflow: drop_oldest`) and `spool-enospc` (a
   128 KiB tmpfs at the spool path from a compose override, below the spool's own bound, and no
   `kill`, because Docker discards a tmpfs when its container stops).
5. **A slow destination (built in W8a).** Netem `delay` on `victoria-metrics`, or `rate` on
   `logit`, past the sink's `timeout:`, so a request VictoriaMetrics applied still times out: `prometheus_out`'s
   `Ambiguous` transport-error row (module doc, "Faults, retries and duplicate safety (sender
   mode)"). The retry rewrites the totals, which `temporality: cumulative` overwrites. Rows:
   `[[expect]]` on `requests{class="network_error"}` and `retries`, `ledger.egress` at 0, and
   `progress` telling a slow drain from a hang. Built as `slow-destination`, with a 2 s sink
   `timeout:`, a 3 s `delay` on `victoria-metrics`, and a `rate` limit on `logit`.
6. **High cardinality over hours (built in W8c).** A second `generate_in` with `{seq%N}` in
   the metric name, N past any count the run reaches, at a low rate (`generate_in` rejects a
   bare `{seq}` in an interned field). The interner never frees
   ([`docs/known-gaps/runtime.md`](../known-gaps/runtime.md), "Event model and interner"), so
   `rss_slope` should FAIL: the harness sees the documented growth and pins its rate. A short
   `series_retention` keeps `aggregate`'s state from masking it. Rows: `rss_slope` and the slope
   of `logit.process.interner.strings` on `logit`. The generator interns every name too, so its
   RSS grows beside the SUT's, and the row judges each service on its own, so a FAIL says which
   process grew. Built as `cardinality`: 200 names a second through the generator's one statsd
   sink, both inputs in the ledger, a 75-minute run, `series_retention: 3`, and
   `[expected] rss_slope` FAIL on both services, with `[[expect]]` rows on each service's
   interner slope over `window = "run"` and on `aggregate`'s evictions. Built with `rss_slope`
   and `fd_slope` judged per service (§6), `[expected]` verdicts, and the `slope` reducer and
   `run` window (§2).

**Medium.** Each adds a service, a topology, or a tool.

7. **The native hop.** Generator → SUT A (`statsd_in → logit_out`, `buffer.disk:`) → SUT B
   (`logit_in → aggregate → prometheus_out`). Raw events cross the hop, so a forwarded resend is a
   surplus at B, not an overwrite in V. Proves [ADR `delivery-semantics`](../adr/delivery-semantics.md),
   "7. The native hop is effectively-once", and the resume in
   [ADR `native-hop-named-acks`](../adr/native-hop-named-acks.md), "4. The handshake carries
   identities out and marks back". Faults on A or the link judge zero surplus; a restart of B
   forgets its marks, so its resend is judged within the send window. Rows: the ledger one hop
   longer, `ledger.replay` on A. Needs: a second SUT service, with `kill` on it.
8. **TCP ingress.** `syslog_in` or `lines_in` over TCP, or `statsd_in` with `transport: tcp`, from
   the generator's TCP sinks. A stalled SUT backs up into the sender
   ([ADR `syslog-tcp-ingress-and-tls`](../adr/syslog-tcp-ingress-and-tls.md), "No receive queue on
   TCP -- the connection itself is the backpressure"), so a `pause` is the generator's counted
   overflow, and a `partition` is a reset losing what sat in the socket buffers, uncounted on both
   sides. Rows: `ledger.wire` judged zero outside resets, and idle closes
   ([ADR `idle-connection-timeout`](../adr/idle-connection-timeout.md)) under `[[expect]]`.
   Needs: per-line wire counters in place of datagram ones.
9. **File tailing.** `tail_in` or `docker_in` behind a rotating writer, with `pause` and `kill` on
   the SUT. Proves checkpoint resume
   ([ADR `file-tailing-and-docker-json-logs`](../adr/file-tailing-and-docker-json-logs.md),
   "Checkpoints: optional, written on an interval, only when dirty"). A replay is at-least-once
   (ADR `delivery-semantics`, "10. A replaying input is at-least-once up to the in-memory
   queues"): a checkpoint advances once lines reach the downstream inboxes, so a kill loses the
   checkpointed lines still in memory (the inboxes, `aggregate`'s open window, and the sink's
   memory queue) and replays the lines since the last checkpoint. The sink runs with
   `buffer.disk:`, as `spool-kill-replay` does, so the sink's queue survives and §6's killed-life
   band ("Which life a hop is judged in"), which covers `aggregate`'s window and not a sink
   queue, applies as written, with the lines the tailer read standing in for W. A killed life is
   judged with that band plus a surplus band of up to one checkpoint interval of lines on top.
   Needs: a volume the writer and SUT share (the generator's `file_out` can write, G its count),
   the tail-side W, and that surplus band.
10. **Deterministic destination errors.** A scripted HTTP stand-in answering `429`, `5xx`, `400`,
    and `413` on cue and recording what it accepts, so every row of a sink's response-class table
    ([ADR `sink-fault-classes`](../adr/sink-fault-classes.md), "Each sink attributes from
    everything its destination gives it") runs under load, and a Datadog-shaped one makes
    `ledger.sent` testable without a real org. It's a destination, not a proxy, so the non-goal
    in "Context" holds; toxiproxy (§9) adds TCP-level faults, not statuses. Rows: `ledger.egress`
    against its record, `[[expect]]` per class. Needs: the stand-in, a cue the driver sets per
    step, and V read from its record. This is the one tooling investment the roadmap recommends.

**Operational.**

11. **Rolling overlap.** Two SUTs on `reuse_port` with `shutdown.delay`, swapped mid-run
    ([ADR `listener-port-sharing-and-shutdown-delay`](../adr/listener-port-sharing-and-shutdown-delay.md),
    "What an overlap means for the data"). Rows: zero uncounted loss; wire loss at the swap within
    the old socket's queue at close. Needs: a namespace-holder service both SUTs join, because a
    SUT owning the namespace takes the port with it when it stops; and a per-instance attribute
    with V summed across instances, because two processes writing one cumulative series overwrite
    each other.
12. **SIGHUP and TLS reload.** A TLS listener and sink, signalled and rotated during faults
    ([ADR `tls-certificate-reload`](../adr/tls-certificate-reload.md), "Trigger: a content poll,
    and SIGHUP"). Rows: `[[expect]]` on `logit.tls.reloads{outcome="reloaded"}` and
    `logit.tls.certificate.not_after`, and the ledger at zero across reconnects. The harness's
    telemetry stream is a stdout `stdio_out`, which ignores SIGHUP, so it's expected to run
    unaffected; a separate row covers the reopen with a SUT `file_out` on a mounted volume, whose
    path a rename moves aside before the signal and whose next batch lands in a fresh file.
    Needs: a `signal` action and a certificate-rotation action writing into a mounted
    directory.

**Items 1 and 2 land first, as two stacked PRs, W7a and W7b.** Both touch code the harness has
never driven, and item 2 forces the compose override, the service list, and the per-sink ledger
that items 3, 7, and 11 need.

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
| W6 | `soak/w6` | `soak/w6: the soak harness's next scenarios` | S | W5 |
| W7a | `soak/w7a` | `soak/w7a: Lua-in-the-path soaks, mark and exit actions, and readiness words` | M | W6 |
| W7b | `soak/w7b` | `soak/w7b: two-sink fan-out and route soaks, compose overrides, and a ledger per sink` | M | W7a |
| W8a | `soak/w8a` | `soak/w8a: counted drops of every reason, full-spool, slow-destination, and stop-during-outage soaks` | M | W7b |
| W8b | `soak/w8b` | `soak/w8b: fan-in from two generators` | M | W8a |
| W8c | `soak/w8c` | `soak/w8c: high-cardinality soaks, a slope reducer, and expected verdicts` | M | W8b |
| W9 | `soak/w9` | `soak/w9: a scripted HTTP destination for every response class` | M | W8c |
| W10 | `soak/w10` | `soak/w10: a native-hop soak between two logit processes` | M | W9 |
| W11 | `soak/w11` | `soak/w11: TCP-ingress and file-tailing soaks` | M | W10 |
| W12 | `soak/w12` | `soak/w12: rolling-overlap, SIGHUP, and TLS-reload soaks` | M | W11 |

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
  enough for an 8-hour run, `ledger.windows` excusing a gap in which `aggregate` sent no batch,
  and the `random-faults` scenario. The perf VM is dropped: long runs happen on the development
  host, which has 32 cores, 125 GB of RAM, 716 GB of free disk, and Docker's default `json-file`
  log driver with no daemon-level rotation, so compose sets each container's rotation.
- **W5**: an external target (Datadog) with a gitignored env file, as in `script/splunk-interop`'s
  cloud mode. The ledger ends at the sink's telemetry (`SENT`), with no backend query. The
  `[target]` table, the `local` compose profile, `SOAK_EXTERNAL_ENV`, the end sequence's wait
  for the sink's queue, `ledger.sent`, the SKIPs of the rows that read V, and the
  `statsd-datadog` scenario ("External targets"); self-test fixtures for every `[target]` rule,
  the wait, and `ledger.sent`'s verdicts.
- **W6**: §10, "Next scenarios", and these rows. Documentation only.
- **W7a**: §10's item 1: the `mark` and `exit` actions, readiness scored from the status word in
  `health_log`, `[ledger] sut_script` and the `ledger.script` row, self-test fixtures for each
  rule, and the `lua-path`, `lua-stall`, and `lua-memory` scenarios, each run recorded under
  "Findings".
- **W7b**: §10's item 2: the scenario compose override, service roles read from the merged
  compose file, and a ledger per sink, with the `fanout-copy`, `fanout-block`, and `route-split`
  scenarios.
- **W8a**: items 4 and 5, and a stop of the SUT inside a backend's stop: the `spool-full`,
  `spool-enospc`, `slow-destination`, and `fanout-stop-outage` scenarios, with the `ledger.egress`
  credit for counted drops of every reason.
- **W8b**: item 3, a ledger over several generators: `[[ledger.source]]` tables replacing the
  scalar `[ledger]` keys, any number of `generator`-role services with a config each, the wire
  judged per source and the intake per listener, `sut_transforms` judged hop by hop, and the
  `fanin` scenario. It needs `syslog_out` to count `logit.output.datagrams` over UDP, which
  `fix/syslog-out-datagrams` adds.
- **W8c**: item 6, a gauge `slope` reducer and a `run` window for `[[expect]]`, `rss_slope`
  and `fd_slope` judged per service, an `[expected]` verdict per row and per service, and the
  `cardinality` scenario.
- **W9**: item 10, the scripted destination, and `ledger.sent` run against it.
- **W10**: item 7, a second SUT service and the ledger one hop longer.
- **W11**: items 8 and 9, per-line wire counters, a shared volume, and the replay surplus band.
- **W12**: items 11 and 12, the namespace-holder service, per-instance totals, and the `signal`
  and certificate-rotation actions.

Landing order: W0 → W1a → W1b → W2 → W3 → W4 → W5 → W6 → W7a → W7b → W8a → W8b → W8c → W9 → W10 → W11 → W12, linear. Each PR is based on and targets its
parent's branch and is brought up to date with `git merge origin/main`, never a rebase.

## Findings

W1a records its first run here, and W1b records a 16-minute run with every check. Each records
the commit, image tags, host, and the verbatim `results.md` table. Known limits that stay true
across runs:

- UDP wire loss is by design and reported, not judged, outside the threshold on windows with no
  UDP-affecting fault.
- An earlier SUT life stops under load, so the per-hop ledger is judged with no tolerance only
  for the final SUT life. Shutdown drops come from telemetry, which a dedicated `internal`
  pipeline exports, and stderr only cross-checks them.
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
  the unblock, not the stall. The loss is counted (K in `ledger.wire`, which PASSed). The first
  run of this scenario, `20261008T223523Z` at a 10 s window, read 0, but its stop never reached
  the listener, so it shows nothing about an unblock.

  Resolved: the decode loop drained the backlog in one poll while the read loop went unpolled,
  and PR #596 makes `decode_loop` yield after 16 consecutive full pops
  ([ADR `udp-intake-batching-and-socket-visibility`](../adr/udp-intake-batching-and-socket-visibility.md)'s
  "Amendment: a backlog drain starves the reader, and the one yield it needed (2026-10-09)").
  Four runs of this scenario against the fixed binary read 0 kernel drops with every row passing:
  `20261009T003527Z` and `20261009T141302Z` from the fix's own branch, and `20261009T152642Z` and
  `20261009T153202Z` from an image rebuilt with no cache from the merged `soak/w2` tree. An
  intermediate run, `20261009T151548Z` (45 drops), ran an image the record can't tie to the fix,
  so it isn't counted.
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

### W4: random schedules and long runs (2026-10-09)

Run `20261009T135848Z`: `SOAK_SKIP_IMAGE=1 script/soak run random-faults`, the scenario's own
1 hour and seed 20261009 (14 faults), from `soak/w4` at `82a0448f` on W1a's images
(`logit:soak` `sha256:78a95c90bb4a`, VictoriaMetrics v1.152.0) on a 32-core host. Overall PASS, and
every row passed. A first attempt died at the signal self-test because `nohup` hands the run
an ignored `SIGHUP`, which the self-test's "default" case assumed was not; `82a0448f` has the
self-test set the inherited dispositions itself.

The drawn schedule (offsets in seconds from the start; `r8` reverted 1.2 s late and `r2` applied
1.2 s late, both while the driver was busy with the previous step):

| Id | Offset | Action | Service | Args | Duration |
|---|---|---|---|---|---|
| r1 | 260 | kill | logit | | 54 s |
| r2 | 451 | netem | logit | `delay 1s` | 140 s |
| r3 | 810 | kill | logit | | 29 s |
| r4 | 1014 | stop | logit | | 75 s |
| r5 | 1240 | netem | generator | `delay 1s` | 113 s |
| r6 | 1520 | kill | logit | | 56 s |
| r7 | 1700 | partition | victoria-metrics | | 60 s |
| r8 | 1934 | netem | generator | `loss 10%` | 77 s |
| r9 | 2210 | netem | generator | `loss 10%` | 80 s |
| r10 | 2433 | stop | victoria-metrics | | 41 s |
| r11 | 2620 | partition | victoria-metrics | | 78 s |
| r12 | 2861 | partition | generator | | 36 s |
| r13 | 3028 | kill | logit | | 25 s |
| r14 | 3222 | netem | logit | `delay 1s` | 111 s |

The rows of `results.md` (the `ledger.egress` detail trimmed to its per-life verdicts and bands,
which are identical in shape for every killed life; `ledger.windows` re-scored by the idle rule
the 8-hour run below led to, where the first scoring excused the same gap, across `r12`, by a
time bound around the fault):

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 28 apply/revert action(s) returned 0; 5 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 7 exit(s) of the logit services, each a scheduled stop with code 0 or a scheduled kill with code 137 (4 kill(s)) |
| `restarts` | PASS | 6 start(s), each a scheduled start or restart |
| `self_log` | PASS | 12 sink fault line(s) inside fault windows, 0 outside, 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 790 health sample(s) healthy outside fault windows; 5 start(s) or unpause(s) ready in time, slowest ready 0.1s |
| `progress` | PASS | 59 30s window(s) with deliveries outside fault windows; 67 freshness sample(s) under 30s; 1740s of 3540s after warmup judged (49%; WARN under 5%) |
| `rss_slope` | PASS | MiB/h per service/life: logit/0 -3.9, logit/1 +6.9, logit/2 -11.4, logit/3 +4.1, logit/4 +0.8, logit/5 -16.2, generator/0 -2.5 (limit 64); logit 1850s of 3540s after warmup judged (52%; WARN under 5%); generator 1855s of 3540s after warmup judged (52%; WARN under 5%) |
| `fd_slope` | PASS | warmup median -> end: logit 15->15, generator 12->12 (limit +8) |
| `ledger.wire` | PASS | G 7,059,278 = W 7,014,509 + K 0 + wire 44,769 (by design: 46,083 in UDP-affecting windows, -1,323 steady, 9 at the end; steady loss judged 75, each run at 0 or more, against limit 1,900); the generator's counted drop_newest loss, not in G: 153,200 event(s) in 1,532 batch(es) |
| `ledger.intake` | PASS | final life 5: W − D 1,144,200 vs E + B 1,144,200; lives 0 to 4: W − D − B − Ab 0 within [0, R] (R 77,000 = 10,000 + 67 x 1,000) |
| `ledger.edge` | PASS | final life 5: E 1,144,200 == A 1,144,200 (listener to aggregate) |
| `ledger.aggregate` | PASS | final life 5: Ab 1,144,200 == A 1,144,200 (every event absorbed) |
| `ledger.egress` | PASS | life 0 (killed): Ab 528,800 − V 528,800 = 0, ok (band [-10,000, 30,001]); life 1 (killed): Ab 1,060,900 − V 1,050,900 = 10,000, within band (band [-9,999, 29,997]); life 2: W − D − B − V 0 = residual 0 + Ab − V 0, ok; life 3 (killed): Ab 929,900 − V 929,900 = 0, ok (band [-10,001, 30,002]); life 4 (killed): Ab 2,942,809 − V 2,942,809 = 0, ok (band [-10,002, 30,006]); life 5 (final): Ab 1,144,200 − V 1,144,200 = 0, ok |
| `ledger.windows` | PASS | 100 series x 6 life/lives, widest gap 10.002s (gaps <= 15s (1.5 x the 10s aggregate interval) or idle, with no window batches.sent from a + 7.5s to b - 2.5s (5s drains); a killed life's last sample within 11s (1.1 x) of the kill, a later life's first within 20s (2 x) of its start; 1 idle gap(s) excused: life 4 +2866s to +2906s (40.001s, 100 series), window sent 0 batch(es) over 6 drain(s) from +2874s to +2904s) |
| `ledger.replay` | PASS | life 1's first drain replayed 0 vs killed life 0's last-drain buffer.batches 1; life 2's first drain replayed 0 vs killed life 1's last-drain buffer.batches 0; life 4's first drain replayed 0 vs killed life 3's last-drain buffer.batches 1; life 5's first drain replayed 0 vs killed life 4's last-drain buffer.batches 1 (one batch in flight allowed) |
| `ledger.summary` | PASS | final life 5: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V 0; wire G − (W + K) 44,769 over the run, by design |
| `identity.sink` | PASS | final life 5 at +3623s: received 55 vs delivered 55 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `recovery` | PASS | 14 of 14 fault(s) judged, each recovered within recovery_bound 45s of its end; warmup rate 2,000/s |
| `expect` | SKIP | the scenario has no [[expect]] tables |

What the run showed:

- **Life 1's Ab − V of 10,000 is inside its band.** The kill came with one 10 s aggregate window
  at 2,000/s unflushed, and the spool holds what reached the sink, not what the aggregate hadn't
  flushed. The band's high edge is 29,997. The other three killed lives balanced to 0.
- **`ledger.replay` replayed 0 at every restart**, against 0 or 1 queued batches at each kill.
  The sink was idle at each kill, so the spool had nothing to replay. The fixed
  `spool-kill-replay` scenario remains the replay test.
- **Steady-state coverage was 49% of the post-warmup time for `progress` and 52% for the RSS
  slopes**, as the schedule promises: faults and their recovery take the rest.
- **Wire loss was 44,769, all inside UDP-affecting windows.** Those windows lost 46,083, and the
  steady buckets summed to −1,323, which the check clamps to 0 per bucket (judged 75 against a
  limit of 1,900). The negative buckets are interpolation error at the bucket edges, the artefact
  the clamp exists for.
- **The generator's own counted `drop_newest` loss was 153,200 events** (1,532 batches) while the
  SUT was down. It is outside G by definition and the run counted it.
- **RSS slopes were −16.2 to +6.9 MiB/h per life** (limit 64) and file descriptors stayed flat
  (logit 15 to 15, generator 12 to 12).
- **The health sampler took 790 samples**, all healthy outside fault windows, and the slowest
  readiness after a start was 0.1 s.

Run `20261009T154710Z` (2026-10-09), the 8-hour run: `SOAK_SKIP_IMAGE=1 script/soak run
random-faults --duration 8h`, seed 20261009 (113 faults), from `soak/w4` at `5de4999f` on
`logit:soak` `sha256:549cfaa6d1ee`,
built `--no-cache` from the merged tree with #596, and VictoriaMetrics v1.152.0. The run
directory is 239 MB. The host was shared for part of the run with a 14-minute `statsd-datadog`
run and with cargo test containers. Overall PASS after a re-score: the first scoring FAILed
`ledger.windows` on one gap, a harness finding described below, and every other row passed both
times.

The rows of the re-scored `results.md` (`rss_slope`, `ledger.intake`, `ledger.egress`, and
`ledger.replay` trimmed to their ranges and counts):

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 226 apply/revert action(s) returned 0; 38 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 31 exit(s) of the logit services, each a scheduled stop with code 0 or a scheduled kill with code 137 (16 kill(s)) |
| `restarts` | PASS | 44 start(s), each a scheduled start or restart |
| `self_log` | PASS | 95 sink fault line(s) inside fault windows, 0 outside, 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 6182 health sample(s) healthy outside fault windows; 38 start(s) or unpause(s) ready in time, slowest ready 0.1s |
| `progress` | PASS | 458 30s window(s) with deliveries outside fault windows; 535 freshness sample(s) under 30s; 13710s of 28740s after warmup judged (48%; WARN under 5%) |
| `rss_slope` | PASS | MiB/h per service/life: logit/0 to logit/29 from -13.4 to +36.4, generator/0 +1.0 (limit 64); logit 14835s of 28740s after warmup judged (52%; WARN under 5%); generator 14830s of 28740s after warmup judged (52%; WARN under 5%) |
| `fd_slope` | PASS | warmup median -> end: logit 15->15, generator 11->11 (limit +8) |
| `ledger.wire` | PASS | G 56,517,865 = W 55,362,360 + K 937,342 + wire 218,163 (by design: 227,626 in UDP-affecting windows, -9,542 steady, 79 at the end; steady loss judged 871, each run at 0 or more, against limit 15,300); the generator's counted drop_newest loss, not in G: 1,094,500 event(s) in 10,945 batch(es) |
| `ledger.intake` | PASS | final life 29: W − D 538,400 vs E + B 538,400; lives 0 to 28: W − D − B − Ab 0 (life 19: 100) within [0, R] (R 77,000 = 10,000 + 67 x 1,000) |
| `ledger.edge` | PASS | final life 29: E 538,400 == A 538,400 (listener to aggregate) |
| `ledger.aggregate` | PASS | final life 29: Ab 538,400 == A 538,400 (every event absorbed) |
| `ledger.egress` | PASS | 16 killed lives: Ab − V 0, ok (10 lives) or 10,000, within band (6 lives: 1, 10, 12, 17, 20, 25), each band about [-10,000, 30,000]; 13 stopped lives: W − D − B − V 0 = residual 0 + Ab − V 0, ok; life 29 (final): Ab 538,400 − V 538,400 = 0, ok |
| `ledger.windows` | PASS | 100 series x 30 life/lives, widest gap 10.002s (gaps <= 15s (1.5 x the 10s aggregate interval) or idle, with no window batches.sent from a + 7.5s to b - 2.5s (5s drains); a killed life's last sample within 11s (1.1 x) of the kill, a later life's first within 20s (2 x) of its start; 23 idle gap(s) excused: life 4 +2866s to +2906s (40s, 100 series), window sent 0 batch(es) over 6 drain(s) from +2874s to +2904s; life 7 +4773s to +4793s (20s, 100 series), window sent 0 batch(es) over 2 drain(s) from +4781s to +4791s; life 7 +4923s to +4964s (40.885s, 100 series), window sent 0 batch(es) over 0 drain(s) from +4931s to +4962s; 20 more in the lines) |
| `ledger.replay` | PASS | 16 killed lives each followed by another: the next life's first drain replayed 0 vs the killed life's last-drain buffer.batches 0 or 1 (one batch in flight allowed) |
| `ledger.summary` | PASS | final life 29: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V 0; wire G − (W + K) 218,163 over the run, by design |
| `identity.sink` | PASS | final life 29 at +28829s: received 24 vs delivered 24 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `recovery` | PASS | 113 of 113 fault(s) judged, each recovered within recovery_bound 45s of its end; warmup rate 2,000/s |
| `expect` | SKIP | the scenario has no [[expect]] tables |

What the run showed:

- **The SUT ran 30 lives**: 16 ended by a `kill`, 13 by a `stop`, and the final one. All 226
  actions returned 0, the health sampler took 6,182 samples, all healthy outside fault windows,
  and the slowest readiness after a start or unpause was 0.1 s.
- **Steady-state coverage was 48% of the post-warmup time for `progress`** (458 windows with
  deliveries) **and 52% for the RSS slopes**, the same shares as the 1-hour run.
- **RSS slopes were −13.4 to +36.4 MiB/h per life** (limit 64), and the generator's was
  +1.0 MiB/h over the whole run. File descriptors stayed
  flat: `logit` 15 to 15, the generator 11 to 11.
- **Wire loss was 218,163, all inside UDP-affecting windows.** Those windows lost 227,626, the
  steady buckets summed to −9,542, clamped to 0 per bucket (judged 871 against a limit of
  15,300), and 79 fell at the end. The kernel counted 937,342 drops (K), during pauses of the
  SUT, and the generator counted 1,094,500 `drop_newest` events in 10,945 batches while the SUT
  was down. Both are counted loss. The final life's uncounted loss was 0.
- **The ledger balanced in every life.** Every earlier life's residual was 0, except killed
  life 19's 100, inside [0, R]. Ten killed lives balanced to 0 and six were 10,000 short, one
  unflushed 10 s window at 2,000/s, inside the band.
- **`ledger.replay` replayed 0 at every one of the 16 restarts after a kill**, against 0 or 1
  queued batches at each kill: the sink was idle at each kill, as in the 1-hour run.
- **`recovery` judged all 113 faults**, and each recovered within the 45 s `recovery_bound`.
- **The sink dropped nothing in the whole run.** During the partition `r43` it held one
  `Ambiguous` batch and delivered it at 18:45:40 when the partition ended. `vm-export.jsonl` holds
  the +10675 s sample twice with the same value, because the timed-out first POST landed too:
  a harmless at-least-once duplicate under `temporality: cumulative`.

One finding, in the harness: the first scoring FAILed `ledger.windows`, because all 100 series
lacked the windows at about +10685 s and +10695 s in life 10. Fault `r43` was a 23 s
`partition` of `logit`, from +10671.00 s to +10694.08 s. The generator's `statsd_out` couldn't
resolve `logit:8125` until +10698.39 s, 4.31 s after the revert. Across the run's six `logit`
partitions this resume lag ran 0.94 to 4.53 s; it's inferred to be Docker's embedded DNS
re-registering the alias after `network connect`. The SUT's listener saw no datagram from
18:45:22 to 18:45:37, and the `window` aggregate reported `series.active` 0 and sent no batch at
those flushes. It then absorbed the 58,100-datagram backlog at 18:45:42 and emitted the +10705 s
window with the full count, so the totals went from 43,820 to 44,620 over four windows, 200 per
window. VictoriaMetrics logged nothing from 18:30 to 18:49. No window was lost: the gap ended
11.084 s after the revert, past the 11 s bound the excusal then allowed after a silencing fault.
Widening that bound would also have excused a window lost right after a fault, so the
excusal now reads the aggregate's own `batches.sent` instead of fault windows (see
`ledger.windows` under "The checks"). Re-scored, the row passes with 23 idle gaps excused, each
across a `pause` or `partition` of `logit` or a `partition` of the generator, `r43`'s among them:
"life 10 +10675s to +10705s (30s, 100 series), window sent 0 batch(es) over 4 drain(s) from
+10683s to +10703s". The 1-hour run and the W1b, W2, and W3 runs keep their statuses.

The 8-hour run meets W4's gate in "Verification".

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

### W5: an external Datadog target (2026-10-09)

Run `20261009T155153Z`: `SOAK_SKIP_IMAGE=1 script/soak run statsd-datadog`, the scenario's own
14 minutes, from `soak/w5` at `7a496043` with no modified paths, against a real org on
Datadog's default site (`datadoghq.com`, US1), on a 32-core host (`logit:soak`
`sha256:549cfaa6d1ee`, `logit-soak-netem:local` `sha256:75afae37640e`). The 8-hour
`random-faults` run was on the same host and daemon throughout. Overall PASS: every row passed
but `ledger.egress` and `ledger.windows`, which an external target SKIPs.

The five faults, as `timeline.jsonl` recorded them (offsets in seconds from the start; each
applied and reverted on schedule with `rc` 0, in under 0.4 s):

| Step | Offset | Action | Service | Args | Duration |
|---|---|---|---|---|---|
| c0s1 | 60 | netem | logit | `delay 300ms 100ms` | 90 s |
| c0s2 | 240 | netem | logit | `loss 20%` | 60 s |
| c0s3 | 390 | partition | logit | | 60 s |
| c0s4 | 540 | pause | logit | | 30 s |
| c0s5 | 660 | kill | logit | | 30 s |

The rows of `results.md` (the `ledger.sent` detail trimmed to its headline numbers and per-life identities):

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 10 apply/revert action(s) returned 0; 2 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 3 exit(s) of the logit services, each a scheduled stop with code 0 or a scheduled kill with code 137 (1 kill(s)) |
| `restarts` | PASS | 1 start(s), each a scheduled start or restart |
| `self_log` | PASS | 3 sink fault line(s) inside fault windows, 0 outside, 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 128 health sample(s) healthy outside fault windows; 2 start(s) or unpause(s) ready in time, slowest ready 0.1s |
| `progress` | PASS | 8 30s window(s) with deliveries outside fault windows; no freshness samples (external target: no backend query); 210s of 780s after warmup judged (27%; WARN under 5%) |
| `rss_slope` | PASS | MiB/h per service/life: logit/0 +10.0, logit/1 -4.5, generator/0 -10.5 (limit 64); logit 260s of 780s after warmup judged (33%; WARN under 5%); generator 260s of 780s after warmup judged (33%; WARN under 5%) |
| `fd_slope` | PASS | warmup median -> end: logit 15->15, generator 11->11 (limit +8) |
| `ledger.wire` | PASS | G 1,645,600 = W 1,582,621 + K 59,779 + wire 3,200 (by design: 3,322 in UDP-affecting windows, -57 steady, -66 at the end; steady loss judged 78, each run at 0 or more, against limit 700); the generator's counted drop_newest loss, not in G: 46,500 event(s) in 465 batch(es) |
| `ledger.intake` | PASS | final life 1: W − D 360,100 vs E + B 360,100; life 0: W − D − B − Ab 0 within [0, R] (R 77,000 = 10,000 + 67 x 1,000) |
| `ledger.edge` | PASS | final life 1: E 360,100 == A 360,100 (listener to aggregate) |
| `ledger.aggregate` | PASS | final life 1: Ab 360,100 == A 360,100 (every event absorbed) |
| `ledger.egress` | SKIP | external target: no backend query; ledger.sent judges the sink |
| `ledger.windows` | SKIP | external target: no backend query |
| `ledger.replay` | PASS | life 1's first drain replayed 0 vs killed life 0's last-drain buffer.batches 1 (one batch in flight allowed) |
| `ledger.summary` | PASS | final life 1: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V SKIP (external target: no backend query); wire G − (W + K) 3,200 over the run, by design |
| `ledger.sent` | PASS | SENT to datadog: every batch received ended in an accepted response; requests 2xx 74, network_error 9; records accepted 7,400; life 0 at +659s: received 60 vs delivered 59 + dropped 0 + buffer.batches 1, gap 0; life 1 at +865s: received 15 vs delivered 15 + dropped 0 + buffer.batches 0, gap 0 |
| `identity.sink` | PASS | final life 1 at +865s: received 15 vs delivered 15 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `recovery` | PASS | 5 of 5 fault(s) judged, each recovered within recovery_bound 45s of its end; warmup rate 2,000/s |
| `expect.no-drop-delay` | PASS | delta of logit.component.batches.dropped{component=datadog} on logit, through c0s1: c0s1 (+60s, +195s]: 0 (over 27 drain(s)), want <= 0 |
| `expect.no-drop-loss` | PASS | delta of logit.component.batches.dropped{component=datadog} on logit, through c0s2: c0s2 (+240s, +345s]: 0 (over 21 drain(s)), want <= 0 |
| `expect.no-drop-partition` | PASS | delta of logit.component.batches.dropped{component=datadog} on logit, through c0s3: c0s3 (+390s, +495s]: 0 (over 21 drain(s)), want <= 0 |
| `expect.no-drop-pause` | PASS | delta of logit.component.batches.dropped{component=datadog} on logit, through c0s4: c0s4 (+540s, +615s]: 0 (over 15 drain(s)), want <= 0 |
| `expect.no-drop-kill` | PASS | delta of logit.component.batches.dropped{component=datadog} on logit, through c0s5: c0s5 (+660s, +735s]: 0 (over 8 drain(s)), want <= 0 |
| `expect.retrying-clears` | PASS | last of logit.component.retrying{component=datadog} on logit, after c0s3: c0s3 (+450s, +495s]: 0 (1 sample(s) and the value in force at its start), want <= 0 |
| `expect.no-stale-partition` | PASS | delta of logit.output.records.dropped{component=datadog, reason=stale} on logit, through c0s3: c0s3 (+390s, +495s]: 0 (over 21 drain(s)), want <= 0 |
| `expect.no-stale-kill` | PASS | delta of logit.output.records.dropped{component=datadog, reason=stale} on logit, through c0s5: c0s5 (+660s, +735s]: 0 (over 8 drain(s)), want <= 0 |
| `expect.no-torn-tail` | PASS | delta of logit.component.buffer.disk.truncated{component=datadog} on logit, through c0s5: c0s5 (+660s, +735s]: 0 (over 8 drain(s)), want <= 0 |

What the run showed:

- **`datadog_out` held through every fault and dropped nothing.** `batches.dropped` stayed 0
  through the netem delay and loss, the 60 s partition, the 30 s pause, and the kill; no record
  dropped `stale` through the partition or the kill; and the spool truncated no torn tail at the
  restart. The sink sent 74 requests that answered `2xx`, 7,400 series records accepted (100
  series per 10 s window), and its per-life identity balanced in both lives: life 0 at +659 s
  received 60 against 59 delivered and 1 queued, and life 1 at +865 s received 15 against 15
  delivered. The end sequence's `sink_drained` phase held with `buffer.batches` 0 after 26.8 s.
- **The nine `network_error` requests were the partition's.** All nine fall in drains reported
  from +413 s to +443 s, inside the partition's window (+390 s to +450 s); the sink logged one
  `retrying` line (`operation timed out`) and one `recovered` line 1.3 s after the revert, and
  `retrying` read 0 afterward. Neither netem step produced a failed request: TCP absorbed the
  delay and the 20% loss.
- **The kill replayed nothing, against one batch queued.** The killed life's last drain read
  `buffer.batches` 1 and the next life replayed 0, inside the one batch in flight
  `ledger.replay` allows: that batch was sent between the drain and the kill. As in W4, the sink
  was idle at the kill, so this run doesn't exercise a non-zero replay; `spool-kill-replay`
  remains the replay test.
- **`ledger.sent`'s evidence ends at the `2xx`.** The harness doesn't query Datadog, so it
  can't see a point the intake accepted and later discarded, and this entry doesn't record what
  the org showed for the `logit.soak.requests_*` series.
- **Wire loss was 3,200, inside UDP-affecting windows.** Those windows lost 3,322, almost all
  of it (3,235) in the kill's window; the steady buckets summed to −57 and the end to −66,
  interpolation error at the bucket edges. The SUT's kernel counted 59,779 receive-buffer drops
  (K), reported at the end of the pause. Separately, the generator's `statsd_out` counted
  46,500 events dropped `overflow_newest` (465 batches) in drains reported from +429 s to
  +454 s: in the partition
  it couldn't resolve `logit`, its queue filled, and it dropped the newest. Both counts are
  outside the uncounted total, which was 0 in both lives.
- **RSS slopes were −10.5 to +10.0 MiB/h per life** (limit 64), over short judged spans (100 s
  to 640 s), and file descriptors stayed flat (logit 15 to 15, generator 11 to 11).
- **Readiness after the unpause and the restart was 0.1 s**, and all 128 health samples outside
  fault windows were healthy.

### W7a: Lua in the path (2026-10-11)

The recorded runs are from `soak/w7b` at `b1e03e31`, which carries this branch merged with
`main`'s shutdown accounting (`internal` drains last), on `logit:soak` `sha256:0e1c5e3109ab`,
rebuilt with `--no-cache` from that commit, on the development host (32 cores). `lua-stall` ran
alone; the others ran two at a time under separate compose projects. Every run passed every
row. The earlier runs, from `soak/w7a` before the merge, found four gaps in the harness and
none in `logit`:

| Run | Scenario | Verdict |
|---|---|---|
| `20261011T220359Z` | `lua-stall` | every row PASS |
| `20261011T221311Z` | `lua-memory` | every row PASS |
| `20261011T221308Z` | `lua-path` | every row PASS |
| `20261011T225118Z` | `statsd-vm` | every row PASS: the regression control |

What the runs showed about `logit`:

- **A stalled script reads `stalled` and recovers on its own.** In `lua-stall` the loop on the
  360,000th event held `process()` past the 10.5 s heartbeat. The HEALTHCHECK read `stalled`
  twice, from +194 s, and `ok` after; `script_stalled` was counted once. The receive queue
  filled (`receive.utilization` 1) and evicted 39,000 datagrams, counted as `overflow_oldest`,
  the kernel dropped none, and the run recovered within `recovery_bound`. Docker's
  `Health.Status` stayed `healthy` throughout, since it needs three failed checks in a row, so
  only the status word in `health_log` shows the stall.
- **`max_memory` ends the process cleanly.** In `lua-memory` the VM held 67,339,919 bytes after
  one full collection at +266 s: one ERROR line keyed `memory_limit_exceeded`, then
  `drain complete` with no batch dropped and `exiting` with code 2, exit code 2, and a restart
  on the step's revert at +320 s that was ready in 0.1 s. The exited life's egress gap was 0,
  with no shutdown drop to count.
- **The script hop balances under faults.** In `lua-path`'s final life, E 646,100 == L_in ==
  L_emit 627,375 + L_drop 12,190 + L_err 6,535, and L_sent 627,409 == L_emit + L_flush 34, the
  34 being one rollup event per 10 s window. The egress gap is 0 once L_flush leaves Ab; with
  the subtraction left out on a copy of the checks, the recorded `20261011T172503Z` FAILs
  `ledger.egress` and `ledger.summary` by 34, the flush events.

What the runs showed about the harness:

- **`progress` read an expected exit's stdout gap as a hang.** The first `lua-memory` run,
  `20261011T171152Z`, FAILed "logit stdout went quiet for 58s from +267s": the drain-gap rule
  excused `stop`, `kill`, `pause`, and `restart` windows only. It now excuses an `exit` step's
  too, and the self-test pins it.
- **The ledger missed a batch counted at the listener.** That run's first life FAILed
  `ledger.egress` with an uncounted gap of 100. After the script node failed, the listener's
  next batch went into its closed inbox and was counted as
  `logit.component.events.dropped{reason="closed_consumer"}` 100 on `statsd`, while
  `events.sent` counted it too. The residual now subtracts the listener's own counted event
  drops, X. Re-scored, the run passes every row; the second run saw no such batch (X 0).
- **A stopped life's script drops after its last drain aren't exported.** The second `lua-path`
  run, `20261011T172503Z`, FAILed `ledger.egress` for its first life with a gap of 3. The life
  stopped with 100 events read but not yet through the script; after the last drain the
  script dropped and failed on about 2.9% of them, which no drain exported then. A script band
  briefly passed such a gap: the life's drop-and-error share of the events the script hadn't
  emitted, rounded up, plus 1 per outcome the life showed. The band was retired once `internal`
  drained last: the final drain now exports those drops and errors, L_drop and L_err include
  them, and the residual subtracts them, so a band could only mask loss. In the recorded
  `lua-path` run, `20261011T221308Z`, life 0's residual and gap were both 0.
- **A drop period that divides the series count empties series.** The first `lua-path` run,
  `20261011T170612Z`, WARNed that two series' resets weren't SUT lives − 1: dropping every 50th
  event of a generator cycling 100 names dropped `soak_requests_49_total` and
  `soak_requests_99_total` almost whole. The script drops every 53rd event instead.
- Two runs started in the same second collide on the run directory: the second fails at once
  with `FileExistsError`. Starting runs a second apart avoids it.
- The stall and the memory trip are timed by the SUT's own event count, so a much slower or
  faster host moves them out of their steps' windows; the calibration is in each scenario's
  `logit-sut.yaml`.

### W7b: two sinks of one kind (2026-10-11)

The recorded runs are from `soak/w7b` at `b1e03e31`, merged with `main`'s shutdown accounting
(`internal` drains last), on `logit:soak` `sha256:0e1c5e3109ab`, rebuilt with `--no-cache` from
that commit, on the development host (32 cores), two scenarios at a time under separate compose
projects. Every run passed every row:

| Run | Scenario | Verdict |
|---|---|---|
| `20261011T223021Z` | `fanout-copy` | every row PASS |
| `20261011T224405Z` | `fanout-block` | every row PASS |
| `20261011T223024Z` | `route-split` | every row PASS |
| `20261011T225118Z` | `statsd-vm` | every row PASS: the regression control |
| `20261011T225122Z` | `statsd-vm-lost-total` | every row PASS: life 0's gap of 63,300 is counted by 4 batches its telemetry exports as `batches.dropped{reason="shutdown"}`, and stderr's `drain complete` agrees |
| `20261011T224408Z` | `spool-kill-replay` | every row PASS: `ledger.replay` 6 replayed vs 6 queued, `ledger.windows` across the kill |

The fan-out scenarios each run one sink under `overflow: drop_oldest`, `vm_b` in `fanout-copy`
and `route-split` and `vm_a` in `fanout-block`, because `logit validate` warns when a listener
reaches two memory-only `block` sinks, and a shipped config validates with no warning. Each
such queue's 1,024 batches hold every fault's windows, so it dropped nothing.

What the runs showed about `logit`:

- **A sink's outage holds its own queue and nothing else, while its queue has room.** In
  `fanout-copy` `vm_b`'s queue held up to 8 batches through its backend's 90 s stop while `vm_a`
  delivered 9, one per window; both backends ended with Ab 1,512,700 each, and neither sink
  dropped a batch through any of the four faults, the overlapping stop of both backends
  included. `route-split` showed the same with each sink holding one shard: V 546,400 at each
  backend, summing to Ab 1,092,800, and nothing unrouted.
- **A blocked sink stalls every sink beside it.** In `fanout-block` `vm_b`'s 2-batch queue
  filled during its backend's stop, then its 64-batch inbox: `inbox.full` counted 1 under `vm_b`
  and 1 under `window`, and `vm_a`, whose backend was up, delivered 0 batches from +110 s to
  +150 s. The listener evicted and counted 86,000 datagrams (`overflow_oldest`), the kernel
  dropped none, `vm_a` delivered 80 batches in the 45 s after the stop, and `vm_b`'s queue was
  back to 1. This is the shared send of a fan-out, which offers a batch to each consumer in
  turn; the scenario pins it rather than treating it as loss, since every event was counted.
- DogStatsD tags reach `prometheus_out` as labels through `aggregate` and `route`: each backend
  stored 50 series, `soak_requests_N_total{shard="N mod 2"}`, and the generator's 29-byte tagged
  lines still went one per datagram under `max_packet_bytes: "32"`.

What the work showed about the harness:

- `extends` in an override names the base file as `file: compose.yaml`: compose resolves the
  path against the project directory, the first file's, so `../../compose.yaml` from the
  scenario's directory fails with "service not found".
- A 10 s window delivers one batch every other 5 s drain, so a `min_delta` of deliveries can't
  assert a sink kept delivering; the fan-out scenarios bound the `delta` over the window
  instead.
- `statsd-vm-lost-total` is a control for the counted rule, not a FAIL: its gap is counted.
- With `internal` draining last, every node's shutdown drops are exported under its own id, so
  `ledger.egress` credits a group's gap with every node's except those of the sinks outside the
  group. None of the recorded fan-out runs stopped the SUT, so the self-test fixtures pin the
  per-sink rule: another sink's drop leaves a short sink uncounted under `copy`, its own drop
  or one at `aggregate` counts it, and either sink's drop counts the summed gap under `route`.

### W8a: counted drops of every reason (2026-10-11)

W8a changes `ledger.egress` to credit a gap with a node's `batches.dropped` under every reason,
and adds `spool-full`, `spool-enospc`, `slow-destination`, and `fanout-stop-outage`. The code is
`soak/w8a` at `6c19a0a0`, on the development host (`sailfish`, 32 cores, 128 GB, Docker 29.7.2).
The four scenario runs used `logit:soak` `sha256:a22432651386`; the control runs used the image
rebuilt afterward, `sha256:d988f51ca940`, because the first image's creation time predates
`main`'s head commit. The branch changes no `logit` source, and every run passed every row:

| Run | Scenario | Verdict |
|---|---|---|
| `20261012T012640Z` | `spool-full` | every row PASS: `overflow_newest` 1 in the stop's window, `disk_full` 0, `rejected` 0, spool drained |
| `20261012T010854Z` | `spool-enospc` | every row PASS: `disk_full` 3, `overflow_oldest` 0, `rejected` 0, spool drained |
| `20261012T014301Z` | `slow-destination` | every row PASS: 8 `network_error` requests and 8 retries in each fault, nothing dropped, `retrying` back to 0 |
| `20261012T013456Z` | `fanout-stop-outage` | every row PASS: `vm_b` life 0's gap of 123,700 is counted by `overflow_oldest` 3 and `shutdown` 4; `vm_a`'s gap is 0 |
| `20261012T015248Z` | `statsd-vm` | every row PASS: the regression control |
| `20261012T015256Z` | `statsd-vm-lost-total` | every row PASS: life 0's gap of 63,300 is counted by `shutdown` 4, as before |
| `20261012T015851Z` | `sink-outage-drop-oldest` | every row PASS: the queue evicted 175 batches during the stop |

What the runs showed about `logit`:

- **A disk spool counts `overflow_newest` during an outage, not `overflow_oldest`.** A sink under
  `overflow: drop_oldest` holds its head batch reserved while it retries, so a full spool rejects
  each arriving batch instead of evicting the head
  ([ADR `disk-backed-sink-buffer`](../adr/disk-backed-sink-buffer.md), "Bound and overflow").
  A spooled batch here is about 6,376 bytes, so the 128 KiB `max_bytes` of `spool-full` holds
  about 20. `disk_full` counts only a write that fails with `ENOSPC`, which `spool-enospc` reaches
  with a 128 KiB tmpfs below the spool's own 4 MiB bound.
- **A dropped batch under cumulative temporality leaves no gap.** In `spool-full` and
  `spool-enospc` the sink dropped 2 and 3 batches in the run, and `ledger.egress` read a gap of 0
  in both, because a later cumulative total overwrote the totals the dropped batch carried.
  `identity.sink` balanced anyway: 45 received were 43 delivered and 2 dropped in `spool-full`, and
  42 delivered and 3 dropped in `spool-enospc`. This is why a `rejected` drop FAILs whatever the
  gap.
- **A response slower than the sink's 2 s `timeout` is retried, never dropped.** A 3 s `delay` at
  VictoriaMetrics lets the destination apply each write while the sink sees a transport error;
  the resend overwrites the same totals, and `ledger.egress` read 0. Throttling the SUT's egress
  at `rate 2kbit` timed requests out the same way.
- **`fanout-stop-outage` is the first real run of the per-sink credit.** Stopping the SUT inside
  `vm_b`'s backend outage made `vm_b`'s life 0 lose 123,700 increments, counted by 3 batches its
  queue evicted and 4 it dropped at shutdown, while `vm_a` delivered to its own backend up to the
  stop and ended with a gap of 0. `recovery` judged 1 of 2 faults: the SUT's stop lies inside
  `vm_b`'s stop, so the SUT stop has no interval clear of other faults, which is the check's
  documented eligibility rule.

What the work showed about the harness:

- A netem `rate` of 64 kbit and 8 kbit didn't time out the sink's requests, because the
  compressed bodies are about 1 KB; 2 kbit did. A throttle scenario must size the rate against
  the body, not the batch's event count.
- A tmpfs for the spool needs `mode=1777` and `labels: { soak.role: sut }` in the compose
  override. `mode=1777` lets the image's unprivileged `logit` user create the spool directory in
  a mount root owned by root, and the self-test reads service roles from the override's labels
  alone.
- No step stops or kills the SUT in `spool-enospc`, because Docker discards a tmpfs when its
  container stops.
- The self-test fixtures failed against `main`'s `checks.py` with the new rule reverted:
  `overflow_oldest`, `disk_full`, a mixed set of reasons, an `overflow` stderr count of 0,
  `rejected` twice, `disk_corrupt`, an overflow in a killed life, an overflow at the sink's own
  copy, an overflow at another sink under `copy` (`sinks-copy-overflow-elsewhere`, which fails
  on its detail wording), and an overflow at either sink under `route`. The existing shutdown
  fixtures failed too, on detail wording alone ("batches dropped for shutdown N" became
  "batches dropped: shutdown N").
- Re-scoring ten preserved runs with `script/soak check` gave no change in any verdict.

### W8b: fan-in from two generators (2026-10-12)

W8b replaces the scalar `[ledger]` source keys with `[[ledger.source]]` tables, allows several
generators, judges the wire per source and the intake per listener, and adds `fanin`. The code
is `soak/w8b` at `06d8d5d7`, merged with `fix/syslog-out-datagrams` (`63a266c5`), which makes
`syslog_out` count `logit.output.datagrams` over UDP, on the development host (`sailfish`, 32
cores, Docker 29.7.2). Every run used `logit:soak` `sha256:11cb9f9847b5`, built with
`--no-cache` from that merge. The tips merged afterward add `main`'s `quinn-proto` bump, which the
image predates. Runs went two at a time under separate compose projects, and
every run passed every row:

| Run | Scenario | Verdict |
|---|---|---|
| `20261012T202959Z` | `fanin` | every row PASS: each source's wire gap lies in its own generator's fault windows, and Ab 3,574,096 = V. The run used a 13-minute cycle and a 16-minute duration; the scenario now has 13m30s and 16m30s, which its last recovery (12m30s) plus two progress windows needs, and wasn't rerun |
| `20261012T203004Z` | `statsd-vm` | every row PASS: the regression control |
| `20261012T204714Z` | `fanout-copy` | every row PASS: two sinks, one source |
| `20261012T210105Z` | `spool-kill-replay` | every row PASS: `ledger.replay` 6 replayed vs 6 queued |
| `20261012T210110Z` | `fanout-stop-outage` | every row PASS: `vm_b` life 0's gap of 123,700 counted by `overflow_oldest` 3 and `shutdown` 4, as in W8a |

`fanin`'s `results.md` table:

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 12 apply/revert action(s) returned 0; 2 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 5 exit(s) of the logit services, each a scheduled stop with code 0 |
| `restarts` | PASS | 2 start(s), each a scheduled start or restart |
| `self_log` | PASS | 2 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 108 readiness sample(s) read ok outside fault windows; 2 start(s) or unpause(s) ready in time, slowest ready 0.1s |
| `progress` | PASS | 5 30s window(s) outside fault windows, judged per sink, each freshness sample under 30s: victoria_metrics: deliveries in 5 of 5, 11 freshness sample(s); 120s of 900s after warmup judged (13%; WARN under 5%) |
| `rss_slope` | PASS | MiB/h per service/life: generator/0 +10.9, generator/1 +7.4, generator-lines/0 +3.2, generator-lines/1 -3.4, logit/0 -1.7 (limit 64); generator 260s of 900s after warmup judged (29%; WARN under 5%); generator-lines 265s of 900s after warmup judged (29%; WARN under 5%); logit 270s of 900s after warmup judged (30%; WARN under 5%) |
| `fd_slope` | PASS | warmup median -> end: generator 12->11, generator-lines 12->12, logit 13->13 (limit +8) |
| `ledger.wire` | PASS | generator -> statsd: G 1,799,400 = W 1,787,299 + K 0 + wire 12,101 (by design: 12,188 in UDP-affecting windows, -64 steady, -23 at the end; steady loss judged 23, each run at 0 or more, against limit 500); the generator's counted drop_newest loss, not in G: 13,400 event(s) in 134 batch(es) \| generator-lines -> lines: G 1,799,200 = W 1,786,797 + K 260 + wire 12,143 (by design: 12,245 in UDP-affecting windows, -136 steady, 34 at the end; steady loss judged 43, each run at 0 or more, against limit 500); the generator's counted drop_newest loss, not in G: 13,800 event(s) in 138 batch(es) |
| `ledger.intake` | PASS | final life 0 statsd: W − D 1,787,299 vs E + B 1,787,299; final life 0 lines: W − D 1,786,797 vs E + B 1,786,797 (R 349,000 = 10,000 + 2 x 1,000 (statsd) + 10,000 + 262 x 1,000 (lines) + 65 x 1,000) |
| `ledger.edge` | PASS | final life 0: E[lines] 1,786,797 == T_in[parse] 1,786,797 (into parse); T_in[parse] 1,786,797 == T_out[parse] 1,786,797 (parse forwards every event); T_out[parse] 1,786,797 == T_in[labels] 1,786,797 (into labels); T_in[labels] 1,786,797 == T_out[labels] 1,786,797 (labels forwards every event); T_out[labels] 1,786,797 == T_in[count] 1,786,797 (into count); T_in[count] 1,786,797 == T_out[count] 1,786,797 (count forwards every event); T_out[count] 1,786,797 == T_in[metrics_only] 1,786,797 (into metrics_only); T_in[metrics_only] 1,786,797 == T_out[metrics_only] 1,786,797 (metrics_only forwards every event); Σ(sent into aggregate) 3,574,096 == A 3,574,096 (listeners to aggregate); A_sent 13,500 == S_in[victoria_metrics] 13,500 (aggregate to victoria_metrics) |
| `ledger.script` | SKIP | no [ledger] sut_script |
| `ledger.aggregate` | PASS | final life 0: Ab 3,574,096 == A 3,574,096 (every event absorbed) |
| `ledger.egress` | PASS | victoria_metrics: life 0 (final): Ab 3,574,096 − V[victoria_metrics] 3,574,096 = 0, ok (split copy; R 349,000 = 10,000 + 2 x 1,000 (statsd) + 10,000 + 262 x 1,000 (lines) + 65 x 1,000) |
| `ledger.windows` | SKIP | not enabled ([ledger] vm_every_window) |
| `ledger.replay` | SKIP | no killed life followed by another |
| `ledger.summary` | PASS | final life 0: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V[victoria_metrics] 0; wire G − (W + K) 24,244 over the run, by design |
| `ledger.sent` | SKIP | local target: ledger.egress judges delivery against the backends |
| `ledger.topic` | SKIP | victoria-metrics target: no topic |
| `identity.sink` | PASS | final life 0 at +988s: victoria_metrics: received 98 vs delivered 98 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `recovery` | PASS | 6 of 6 fault(s) judged at 1 sink(s), each recovered within recovery_bound 45s of its end; warmup rate 4,000/s |
| `expect.statsd-idle-while-generator-stopped` | PASS | min_delta of logit.input.datagrams{component=statsd} on logit, during 5: c0s5 (+570s, +630s]: 0 (smallest of 12 drain(s)), want <= 0 |
| `expect.lines-reads-while-generator-stopped` | PASS | min_delta of logit.input.datagrams{component=lines} on logit, during 5: c0s5 (+570s, +630s]: 10,000 (smallest of 12 drain(s)), want >= 5000 |
| `expect.lines-idle-while-generator-lines-stopped` | PASS | min_delta of logit.input.datagrams{component=lines} on logit, during 6: c0s6 (+705s, +765s]: 0 (smallest of 12 drain(s)), want <= 0 |
| `expect.statsd-reads-while-generator-lines-stopped` | PASS | min_delta of logit.input.datagrams{component=statsd} on logit, during 6: c0s6 (+705s, +765s]: 10,000 (smallest of 12 drain(s)), want >= 5000 |

What the runs showed about `logit`:

- **Two listeners' events merge into one `aggregate` with nothing lost past the wire.** Each
  source sent about 1.8 million lines. `statsd` read 1,787,299 and `lines` 1,786,797, the four
  nodes between `lines` and `window` forwarded every event, `window` received and absorbed
  3,574,096, and VictoriaMetrics held 3,574,096 across 150 series: 100 `soak_requests_N_total`
  and 50 `soak_lines_total{id}`.
- **A fault on one generator touches only its own source.** The 10% netem loss on `generator`
  cost its source 12,187 lines and the same fault on `generator-lines` cost its source 12,230,
  while each source's wire lost under 50 lines in all of its own steady time, the other
  generator's faults included. While either generator was stopped,
  its listener read 0 datagrams in at least one drain and the other read 10,000 in every drain.
- **A partitioned generator's queue costs the SUT's kernel queue after the reconnect.** The 45 s
  partition of `generator-lines` made `syslog_out` drop 13,800 lines as `drop_newest` and queue
  the rest. When the network came back at +495 s, the burst cost 260 datagrams to the kernel at
  the SUT (`logit.input.kernel.drops` on `lines`, in the drain at +498 s), inside the fault's
  window. The same partition of `generator` dropped 13,400 lines at the generator and none in
  the kernel.
- `syslog_out` over UDP sends one message per datagram: G's precondition held on both sources
  (messages equal datagrams) once the fix counted datagrams.

What the work showed about the harness:

- Compose merges `volumes` by mount target, so an override's generator that `extends` the base
  `generator` mounts its own config at `/config.yaml` through its own variable,
  `SOAK_GENERATOR_LINES_CONFIG`, with no `!override` tag.
- `aggregate` passes an event's log through untouched, so a `lines_in` leg needs
  `keep_signals: [metrics]` before it; without it every line's log would reach the sink and its
  event counts.
- Two runs started in the same second collide on the run directory, which is named by the
  second: a second `spool-kill-replay` start failed in `prepare()` with `FileExistsError` and
  started nothing. Starting runs a few seconds apart avoids it.
- `script/soak check` can't re-score a run recorded before W8b: its `scenario.resolved.json`
  carries the scalar keys, so the ledger rows read no source.
- The self-test fixtures failed with each harness change reverted (18 reverts): the per-source
  fault windows, the per-listener intake, the summed E, the transform hops, R per listener and
  from each listener's `receive:`, B's oversize drops, the per-source precondition, the summed
  recovery baseline and ingest, the refused scalar keys, the source rules, a source's service a
  generator, every generator a source's, several generators in the role map, a config per
  generator, `compose.env` per generator, and the end sequence stopping each generator. Two
  rules added in review fail it the same way: a transform on two sources' chains, and a service
  whose compose variable is the SUT's or another source's.

The controls' tables:

`statsd-vm`, `20261012T203004Z`:

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 14 apply/revert action(s) returned 0; 2 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 3 exit(s) of the logit services, each a scheduled stop with code 0 |
| `restarts` | PASS | 2 start(s), each a scheduled start or restart |
| `self_log` | PASS | 5 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 54 readiness sample(s) read ok outside fault windows; 2 start(s) or unpause(s) ready in time, slowest ready 0.1s |
| `progress` | PASS | 7 30s window(s) outside fault windows, judged per sink, each freshness sample under 30s: victoria_metrics: deliveries in 7 of 7, 7 freshness sample(s); 180s of 900s after warmup judged (20%; WARN under 5%) |
| `rss_slope` | PASS | MiB/h per service/life: generator/0 -6.9, logit/1 +10.6 (limit 64); generator 200s of 900s after warmup judged (22%; WARN under 5%); logit 190s of 900s after warmup judged (21%; WARN under 5%) |
| `fd_slope` | PASS | warmup median -> end: generator 12->12, logit 12->12 (limit +8) |
| `ledger.wire` | PASS | G 1,915,600 = W 1,843,541 + K 59,779 + wire 12,280 (by design: 12,167 in UDP-affecting windows, 118 steady, -5 at the end; steady loss judged 118, each run at 0 or more, against limit 500); the generator's counted drop_newest loss, not in G: 16,700 event(s) in 167 batch(es) |
| `ledger.intake` | PASS | final life 1: W − D 643,400 vs E + B 643,400; life 0: W − D − B − X − Ab 0 within [0, R] (R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.edge` | PASS | final life 1: E 643,400 == A 643,400 (listener to aggregate); A_sent 2,700 == S_in[victoria_metrics] 2,700 (aggregate to victoria_metrics) |
| `ledger.script` | SKIP | no [ledger] sut_script |
| `ledger.aggregate` | PASS | final life 1: Ab 643,400 == A 643,400 (every event absorbed) |
| `ledger.egress` | PASS | victoria_metrics: life 0: W − D − B − X − V 0 = residual 0 + Ab − V 0, ok; life 1 (final): Ab 643,400 − V[victoria_metrics] 643,400 = 0, ok (split copy; R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.windows` | SKIP | not enabled ([ledger] vm_every_window) |
| `ledger.replay` | SKIP | no killed life followed by another |
| `ledger.summary` | PASS | final life 1: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V[victoria_metrics] 0; wire G − (W + K) 12,280 over the run, by design |
| `ledger.sent` | SKIP | local target: ledger.egress judges delivery against the backends |
| `ledger.topic` | SKIP | victoria-metrics target: no topic |
| `identity.sink` | PASS | final life 1 at +990s: victoria_metrics: received 27 vs delivered 27 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `recovery` | PASS | 7 of 7 fault(s) judged at 1 sink(s), each recovered within recovery_bound 45s of its end; warmup rate 2,000/s |
| `expect` | SKIP | the scenario has no [[expect]] tables |

`fanout-copy`, `20261012T204714Z`:

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 8 apply/revert action(s) returned 0; 0 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 2 exit(s) of the logit services, each a scheduled stop with code 0 |
| `restarts` | PASS | 3 start(s), each a scheduled start or restart |
| `self_log` | PASS | 6 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 67 readiness sample(s) read ok outside fault windows; 0 start(s) or unpause(s) ready in time |
| `progress` | PASS | 9 30s window(s) outside fault windows, judged per sink, each freshness sample under 30s: vm_a: deliveries in 9 of 9, 9 freshness sample(s); vm_b: deliveries in 9 of 9, 9 freshness sample(s); 240s of 690s after warmup judged (35%; WARN under 5%) |
| `rss_slope` | PASS | MiB/h per service/life: generator/0 -0.4, logit/0 +1.5 (limit 64); generator 270s of 690s after warmup judged (39%; WARN under 5%); logit 270s of 690s after warmup judged (39%; WARN under 5%) |
| `fd_slope` | PASS | warmup median -> end: generator 12->12, logit 13->13 (limit +8) |
| `ledger.wire` | PASS | G 1,512,800 = W 1,512,800 + K 0 + wire 0 (by design: 0 in UDP-affecting windows, -27 steady, 27 at the end; steady loss judged 0, each run at 0 or more, against limit 100); the generator's counted drop_newest loss, not in G: 0 event(s) in 0 batch(es) |
| `ledger.intake` | PASS | final life 0: W − D 1,512,800 vs E + B 1,512,800 (R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.edge` | PASS | final life 0: E 1,512,800 == A 1,512,800 (listener to aggregate); A_sent 7,700 == S_in[vm_a] 7,700 (aggregate to vm_a); A_sent 7,700 == S_in[vm_b] 7,700 (aggregate to vm_b) |
| `ledger.script` | SKIP | no [ledger] sut_script |
| `ledger.aggregate` | PASS | final life 0: Ab 1,512,800 == A 1,512,800 (every event absorbed) |
| `ledger.egress` | PASS | vm_a: life 0 (final): Ab 1,512,800 − V[vm_a] 1,512,800 = 0, ok \| vm_b: life 0 (final): Ab 1,512,800 − V[vm_b] 1,512,800 = 0, ok (split copy; R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.windows` | SKIP | not enabled ([ledger] vm_every_window) |
| `ledger.replay` | SKIP | no killed life followed by another |
| `ledger.summary` | PASS | final life 0: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V[vm_a] 0 + Ab − V[vm_b] 0; wire G − (W + K) 0 over the run, by design |
| `ledger.sent` | SKIP | local target: ledger.egress judges delivery against the backends |
| `ledger.topic` | SKIP | victoria-metrics target: no topic |
| `identity.sink` | PASS | final life 0 at +778s: vm_a: received 77 vs delivered 77 + dropped 0 + buffer.batches 0, gap 0; vm_b: received 77 vs delivered 77 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `recovery` | PASS | 4 of 4 fault(s) judged at 2 sink(s), each recovered within recovery_bound 45s of its end; warmup rate 2,000/s |
| `expect.a-delivers-while-b-stopped` | PASS | delta of logit.component.batches.delivered{component=vm_a} on logit, during c0s1: c0s1 (+60s, +150s]: 9 (over 18 drain(s)), want >= 6 |
| `expect.b-queues-while-stopped` | PASS | max of logit.component.buffer.batches{component=vm_b} on logit, during c0s1: c0s1 (+60s, +150s]: 9 (10 sample(s) and the value in force at its start), want >= 5 |
| `expect.b-drops-nothing` | PASS | delta of logit.component.batches.dropped{component=vm_b} on logit, through 1: c0s1 (+60s, +195s]: 0 (over 27 drain(s)), want <= 0 |
| `expect.a-drops-nothing` | PASS | delta of logit.component.batches.dropped{component=vm_a} on logit, through 3: c0s3 (+390s, +525s]: 0 (over 27 drain(s)), want <= 0 |
| `expect.b-drops-nothing-overlapping` | PASS | delta of logit.component.batches.dropped{component=vm_b} on logit, through 4: c0s4 (+420s, +555s]: 0 (over 27 drain(s)), want <= 0 |

`spool-kill-replay`, `20261012T210105Z`:

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 4 apply/revert action(s) returned 0; 1 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 3 exit(s) of the logit services, each a scheduled stop with code 0 or a scheduled kill with code 137 (1 kill(s)) |
| `restarts` | PASS | 2 start(s), each a scheduled start or restart |
| `self_log` | PASS | 3 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 36 readiness sample(s) read ok outside fault windows; 1 start(s) or unpause(s) ready in time, slowest ready 0.1s |
| `progress` | PASS | 4 30s window(s) outside fault windows, judged per sink, each freshness sample under 30s: victoria_metrics: deliveries in 4 of 4, 5 freshness sample(s); 90s of 315s after warmup judged (29%; WARN under 5%) |
| `rss_slope` | PASS | MiB/h per service/life: generator/0 -2.3, logit/1 -25.0 (limit 64); generator 115s of 315s after warmup judged (37%; WARN under 5%); logit 110s of 315s after warmup judged (35%; WARN under 5%) |
| `fd_slope` | PASS | warmup median -> end: generator 12->11, logit 15->15 (limit +8) |
| `ledger.wire` | PASS | G 762,400 = W 759,100 + K 0 + wire 3,300 (by design: 3,222 in UDP-affecting windows, -18 steady, 96 at the end; steady loss judged 4, each run at 0 or more, against limit 300); the generator's counted drop_newest loss, not in G: 0 event(s) in 0 batch(es) |
| `ledger.intake` | PASS | final life 1: W − D 510,300 vs E + B 510,300; life 0: W − D − B − X − Ab 0 within [0, R] (R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.edge` | PASS | final life 1: E 510,300 == A 510,300 (listener to aggregate); A_sent 2,300 == S_in[victoria_metrics] 2,300 (aggregate to victoria_metrics) |
| `ledger.script` | SKIP | no [ledger] sut_script |
| `ledger.aggregate` | PASS | final life 1: Ab 510,300 == A 510,300 (every event absorbed) |
| `ledger.egress` | PASS | victoria_metrics: life 0 (killed): Ab 248,800 − V[victoria_metrics] 248,800 = 0, ok (band [-9,998, 29,994]: ingest 2,000/s over the last drain interval; below, the residual 0 plus one 4.99993s drain interval of ingest, absorbed after the last drain and flushed before the kill; above, one 10s aggregate window (configured) unflushed at the kill plus one drain interval of queue growth after the last drain); residual W − D − B − X − Ab 0 at its last drain, what the kill lost from the receive queue and inbox, by design, not judged here; D has no shutdown part: a SIGKILL counts no shutdown drops; life 1 (final): Ab 510,300 − V[victoria_metrics] 510,300 = 0, ok (split copy; R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.windows` | PASS | 100 series over 1 sink(s) x 2 life/lives, widest gap 10.001s (gaps <= 15s (1.5 x the 10s aggregate interval) or idle, with no window batches.sent from a + 7.5s to b - 2.5s (5s drains); a killed life's last sample within 11s (1.1 x) of the kill, a later life's first within 20s (2 x) of its start) |
| `ledger.replay` | PASS | victoria_metrics: life 1's first drain replayed 6 vs killed life 0's last-drain buffer.batches 6 (one batch in flight allowed) |
| `ledger.summary` | PASS | final life 1: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V[victoria_metrics] 0; wire G − (W + K) 3,300 over the run, by design |
| `ledger.sent` | SKIP | local target: ledger.egress judges delivery against the backends |
| `ledger.topic` | SKIP | victoria-metrics target: no topic |
| `identity.sink` | PASS | final life 1 at +400s: victoria_metrics: received 23 + replayed 6 vs delivered 29 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `recovery` | PASS | 1 of 2 fault(s) judged at 1 sink(s), each recovered within recovery_bound 45s of its end; warmup rate 2,000/s |
| `expect.spool-holds-the-outage` | PASS | max of logit.component.buffer.disk.segments{component=victoria_metrics} on logit, during c0s1: c0s1 (+60s, +210s]: 1 (14 sample(s) and the value in force at its start), want >= 1 |
| `expect.spool-queues-windows` | PASS | max of logit.component.buffer.batches{component=victoria_metrics} on logit, during c0s1: c0s1 (+60s, +210s]: 12 (14 sample(s) and the value in force at its start), want >= 2 |
| `expect.spool-replayed-at-restart` | PASS | delta of logit.component.buffer.disk.replayed{component=victoria_metrics} on logit, after c0s2: c0s2 (+150s, +195s]: 6 (over 9 drain(s)), want >= 1 |
| `expect.no-torn-tail` | PASS | delta of logit.component.buffer.disk.truncated{component=victoria_metrics} on logit, through c0s2: c0s2 (+120s, +195s]: 0 (over 9 drain(s)), want <= 0 |
| `expect.no-second-replay` | PASS | delta of logit.component.buffer.disk.replayed{component=victoria_metrics} on logit, after c0s1: c0s1 (+210s, +255s]: 0 (over 9 drain(s)), want <= 0 |
| `expect.sink-never-drops` | PASS | delta of logit.component.batches.dropped{component=victoria_metrics} on logit, through c0s1: c0s1 (+60s, +255s]: 0 (over 33 drain(s)), want <= 0 |
| `expect.retrying-clears` | PASS | last of logit.component.retrying{component=victoria_metrics} on logit, after c0s1: c0s1 (+210s, +255s]: 0 (1 sample(s) and the value in force at its start), want <= 0 |

`fanout-stop-outage`, `20261012T210110Z`:

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 4 apply/revert action(s) returned 0; 1 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 3 exit(s) of the logit services, each a scheduled stop with code 0 |
| `restarts` | PASS | 2 start(s), each a scheduled start or restart |
| `self_log` | PASS | 3 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 36 readiness sample(s) read ok outside fault windows; 1 start(s) or unpause(s) ready in time, slowest ready 0.1s |
| `progress` | PASS | 4 30s window(s) outside fault windows, judged per sink, each freshness sample under 30s: vm_a: deliveries in 4 of 4, 5 freshness sample(s); vm_b: deliveries in 4 of 4, 5 freshness sample(s); 90s of 315s after warmup judged (29%; WARN under 5%) |
| `rss_slope` | PASS | MiB/h per service/life: generator/0 -5.3, logit/1 +22.0 (limit 64); generator 115s of 315s after warmup judged (37%; WARN under 5%); logit 110s of 315s after warmup judged (35%; WARN under 5%) |
| `fd_slope` | PASS | warmup median -> end: generator 12->12, logit 13->13 (limit +8) |
| `ledger.wire` | PASS | G 762,900 = W 752,600 + K 0 + wire 10,300 (by design: 10,375 in UDP-affecting windows, -47 steady, -28 at the end; steady loss judged 0, each run at 0 or more, against limit 300); the generator's counted drop_newest loss, not in G: 0 event(s) in 0 batch(es) |
| `ledger.intake` | PASS | final life 1: W − D 500,100 vs E + B 500,100; life 0: W − D − B − X − Ab 0 within [0, R] (R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.edge` | PASS | final life 1: E 500,100 == A 500,100 (listener to aggregate); A_sent 2,300 == S_in[vm_a] 2,300 (aggregate to vm_a); A_sent 2,300 == S_in[vm_b] 2,300 (aggregate to vm_b) |
| `ledger.script` | SKIP | no [ledger] sut_script |
| `ledger.aggregate` | PASS | final life 1: Ab 500,100 == A 500,100 (every event absorbed) |
| `ledger.egress` | PASS | vm_a: life 0: W − D − B − X − V 0 = residual 0 + Ab − V 0, ok; life 1 (final): Ab 500,100 − V[vm_a] 500,100 = 0, ok \| vm_b: life 0: W − D − B − X − V 123,700 = residual 0 + Ab − V 123,700, counted (batches dropped: overflow_oldest 3, shutdown 4); life 1 (final): Ab 500,100 − V[vm_b] 500,100 = 0, ok (split copy; R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.windows` | SKIP | not enabled ([ledger] vm_every_window) |
| `ledger.replay` | SKIP | no killed life followed by another |
| `ledger.summary` | PASS | final life 1: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V[vm_a] 0 + Ab − V[vm_b] 0; wire G − (W + K) 10,300 over the run, by design |
| `ledger.sent` | SKIP | local target: ledger.egress judges delivery against the backends |
| `ledger.topic` | SKIP | victoria-metrics target: no topic |
| `identity.sink` | PASS | final life 1 at +400s: vm_a: received 23 vs delivered 23 + dropped 0 + buffer.batches 0, gap 0; vm_b: received 23 vs delivered 21 + dropped 2 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `recovery` | PASS | 1 of 2 fault(s) judged at 2 sink(s), each recovered within recovery_bound 45s of its end; warmup rate 2,000/s |
| `expect.b-drops-at-shutdown` | PASS | delta of logit.component.batches.dropped{component=vm_b, reason=shutdown} on logit, during c0s2: c0s2 (+120s, +150s]: 4 (over 2 drain(s)), want >= 1 |
| `expect.a-drops-nothing` | PASS | delta of logit.component.batches.dropped{component=vm_a} on logit, through c0s1: c0s1 (+60s, +255s]: 0 (over 35 drain(s)), want <= 0 |
| `expect.b-retrying-clears` | PASS | last of logit.component.retrying{component=vm_b} on logit, after c0s1: c0s1 (+210s, +255s]: 0 (1 sample(s) and the value in force at its start), want <= 0 |

### W8c: high cardinality over hours (2026-10-12)

W8c judges `rss_slope` and `fd_slope` per `logit` service, adds `[expected]` verdicts, a gauge
`slope` reducer and a `run` window for `[[expect]]`, and the `cardinality` scenario. The code is
`soak/w8c` at `acddf1d5`, its merge of `soak/w8b` at `d08d8e09`, on the development host (32
cores). Every run used `logit:soak` `sha256:11cb9f9847b5`, W8b's image, built from `63a266c5`;
the `main` merged since adds Kafka SASL, graph-rule, and HTTP-driver changes that the image
predates, none of them on the scenarios' paths but `prometheus_out`'s shared HTTP driver. The
`cardinality` run went beside the controls: `statsd-vm`, then `fanin` twice, and a full
`script/cibuild` limited to 8 build jobs during the second `fanin`. A third `fanin` ran alone
afterward.

| Run | Scenario | Verdict |
|---|---|---|
| `20261012T215005Z` | `cardinality` | every row PASS, `rss_slope` FAIL on both services as expected: the SUT +73.2 MiB/h and the generator +60.9 MiB/h against a 48 MiB/h limit, each process interning 720,000 strings an hour |
| `20261012T215025Z` | `statsd-vm` | every row PASS: the regression control, its slope rows one line per service |
| `20261012T220732Z` | `fanin` | WARN: `rss_slope` on `generator` life 0, +101.6 MiB/h, below; every other row PASS |
| `20261012T222537Z` | `fanin` | WARN: the same, +105.2 MiB/h; every other row PASS |
| `20261012T230732Z` | `fanin` | every row PASS, alone on the host: `generator` life 0 +39.4 MiB/h |

`cardinality`'s `results.md` table:

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 10 apply/revert action(s) returned 0; 0 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 2 exit(s) of the logit services, each a scheduled stop with code 0 |
| `restarts` | PASS | 2 start(s), each a scheduled start or restart |
| `self_log` | PASS | 4 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 743 readiness sample(s) read ok outside fault windows; 0 start(s) or unpause(s) ready in time |
| `progress` | PASS | 121 30s window(s) outside fault windows, judged per sink, each freshness sample under 30s: victoria_metrics: deliveries in 121 of 121, 121 freshness sample(s); 3540s of 4380s after warmup judged (81%; WARN under 5%) |
| `rss_slope` | PASS | generator FAIL, as expected: MiB/h life 0 +60.9 (limit 48); 3585s of 4380s after warmup judged (82%; WARN under 5%) \| logit FAIL, as expected: MiB/h life 0 +73.2 (limit 48); 3585s of 4380s after warmup judged (82%; WARN under 5%) |
| `fd_slope` | PASS | generator PASS: warmup median 12 -> end 12 (limit +8) \| logit PASS: warmup median 12 -> end 12 (limit +8) |
| `ledger.wire` | PASS | G 9,913,540 = W 9,913,540 + K 0 + wire 0 (by design: 0 in UDP-affecting windows, -49 steady, 49 at the end; steady loss judged 0, each run at 0 or more, against limit 100); the generator's counted drop_newest loss, not in G: 0 event(s) in 0 batch(es) |
| `ledger.intake` | PASS | final life 0: W − D 9,913,540 vs E + B 9,913,540 (R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.edge` | PASS | final life 0: E 9,913,540 == A 9,913,540 (listener to aggregate); A_sent 946,440 == S_in[victoria_metrics] 946,440 (aggregate to victoria_metrics) |
| `ledger.script` | SKIP | no [ledger] sut_script |
| `ledger.aggregate` | PASS | final life 0: Ab 9,913,540 == A 9,913,540 (every event absorbed) |
| `ledger.egress` | PASS | victoria_metrics: life 0 (final): Ab 9,913,540 − V[victoria_metrics] 9,913,540 = 0, ok (split copy; R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.windows` | SKIP | not enabled ([ledger] vm_every_window) |
| `ledger.replay` | SKIP | no killed life followed by another |
| `ledger.summary` | PASS | final life 0: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V[victoria_metrics] 0; wire G − (W + K) 0 over the run, by design |
| `ledger.sent` | SKIP | local target: ledger.egress judges delivery against the backends |
| `ledger.topic` | SKIP | victoria-metrics target: no topic |
| `identity.sink` | PASS | final life 0 at +4588s: victoria_metrics: received 452 vs delivered 452 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `recovery` | PASS | 5 of 5 fault(s) judged at 1 sink(s), each recovered within recovery_bound 45s of its end; warmup rate 2,000/s |
| `expect.sut-interner-grows` | PASS | slope of logit.process.interner.strings on logit, run: run (+120s, +4500s]: life 0 720,002.5/h (life 0 876 samples over 4375s), want >= 684000 and <= 756000 |
| `expect.generator-interner-grows` | PASS | slope of logit.process.interner.strings on generator, run: run (+120s, +4500s]: life 0 720,000.0/h (life 0 876 samples over 4375s), want >= 684000 and <= 756000 |
| `expect.aggregate-evicts-each-name` | PASS | delta of logit.transform.series.evicted{component=window, reason=idle} on logit, run: run (+120s, +4500s]: 876,000 (over 876 drain(s)), want >= 798000 |
| `expect.aggregate-never-hits-its-cap` | PASS | delta of logit.transform.series.evicted{component=window, reason=cardinality} on logit, run: run (+120s, +4500s]: 0 (over 876 drain(s)), want <= 0 |

What the runs showed about `logit`:

- **Each never-repeating name costs each process one interned string and about 120 bytes for
  good.** Over the run window, warmup's end to the end sequence, both interners grew by
  720,000 strings an hour, 200 a second, one per name. `logit.process.memory.allocated.bytes`
  grew 82.4 MiB/h in the SUT and 82.5 MiB/h in the generator, about 120 bytes a name with the
  interner's table growth included, from 16.4 to 126.6 MiB in the SUT and from 6.9 to
  102.6 MiB in the generator; the known gap's measured 94 to 124 bytes a string holds.
  Allocation grows in steps, flat for several minutes and then 10 to 30 MiB at once, as the
  interner's arena and table grow.
- **RSS follows it more slowly.** Over the same window resident memory grew 70.5 MiB/h in the
  SUT, from 47.5 to 149.4 MiB, and 61.3 MiB/h in the generator, from 23.1 to 97.8 MiB; the
  rows' steady-state fits read +73.2 and +60.9. A 15-minute probe at 100 names a second read
  +117.8 MiB/h on the SUT, against 37 MiB/h of allocation, so a short run overstates the slope:
  RSS starts with the allocator's slack and keeps it.
- **`aggregate` state stayed flat.** With `series_retention: 3` it evicted 876,000 idle series
  over the run window, 200 a second, and none for its cap; it held 2,080 to 2,120 active and
  5,980 to 6,020 retained series per window throughout. VictoriaMetrics held 901,340 series at
  the end, the 100 repeating names and 901,240 unique ones of one sample each, and every
  increment balanced: Ab 9,913,540 = V.
- **A generator's RSS after a partition takes longer than `recovery_bound` to come back.** In
  `fanin`, `generator`'s RSS climbs to about 87 MiB while its 45 s partition queues lines, and
  returns to its 19 MiB baseline 50 to 110 s after the revert, in steps, as the drained queue's
  pages go back. In W8b's run it was back within 49 s. The samples just past the 45 s
  `recovery_bound` count as steady state, so on a 16-minute run with 24 samples in that life the
  fit swings: +10.9 MiB/h in W8b, +101.6 and +105.2 here beside `cardinality`, and +39.4 alone.
  Re-scoring the first run with the W8b harness gives the same WARN, so the rows' new shape
  didn't cause it.

What the work showed about the harness:

- Two lines of `soak_card_N` under 10 fit in one 32-byte datagram, so the probe's G
  precondition FAILed on 5 datagrams that carried 2 lines each. The scenario's names are
  `soak_cardinality_N`, whose shortest pair is 45 bytes.
- `rss_slope` reports growth as WARN under an hour, so `[expected] rss_slope` FAIL holds only at
  the scenario's own 75 minutes; the probe's 15 minutes read WARN.
- The generator interns every name it renders, so its expectation is FAIL from the probe's
  evidence (+44.6 MiB/h at 100 names a second) and the run's (+60.9 at 200).
- `script/soak check` reads `scenario.resolved.json`'s `expected` and `expect` as recorded; the
  eviction bound was corrected after the run, from 798,000 to 832,200 (95% of the run window's
  876,000), which the recorded 876,000 meets.
- The self-test fixtures failed with each harness change reverted (22 reverts): the per-hour
  conversion, too few samples, the run window's start and end, the per-life fit, the slope
  reducer's registration, the run window refusing a step, the step required elsewhere, the two
  new metric kinds, the `[expected]` row, verdict, service, and table rules, an expected FAIL
  scoring PASS, an expected FAIL that came out PASS failing, and the per-service rows. Five
  rules added in review fail it the same way: a row with no service to judge SKIPs; a service
  with samples but no fittable life WARNs; `rss_slope` SKIPs when no service has a fittable
  life and none has an expected verdict; `fd_slope` with no timeline start reaches
  `[expected]`; and `fd_slope` with no fd samples reaches `[expected]`.

The controls' tables:

`statsd-vm`, `20261012T215025Z`:

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 14 apply/revert action(s) returned 0; 2 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 3 exit(s) of the logit services, each a scheduled stop with code 0 |
| `restarts` | PASS | 2 start(s), each a scheduled start or restart |
| `self_log` | PASS | 5 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 54 readiness sample(s) read ok outside fault windows; 2 start(s) or unpause(s) ready in time, slowest ready 0.1s |
| `progress` | PASS | 7 30s window(s) outside fault windows, judged per sink, each freshness sample under 30s: victoria_metrics: deliveries in 7 of 7, 7 freshness sample(s); 180s of 900s after warmup judged (20%; WARN under 5%) |
| `rss_slope` | PASS | generator PASS: MiB/h life 0 -11.8 (limit 64); 200s of 900s after warmup judged (22%; WARN under 5%) \| logit PASS: MiB/h life 1 +4.6 (limit 64); 190s of 900s after warmup judged (21%; WARN under 5%) |
| `fd_slope` | PASS | generator PASS: warmup median 11 -> end 12 (limit +8) \| logit PASS: warmup median 12 -> end 12 (limit +8) |
| `ledger.wire` | PASS | G 1,914,800 = W 1,842,728 + K 59,779 + wire 12,293 (by design: 12,322 in UDP-affecting windows, -76 steady, 46 at the end; steady loss judged 1, each run at 0 or more, against limit 500); the generator's counted drop_newest loss, not in G: 17,400 event(s) in 174 batch(es) |
| `ledger.intake` | PASS | final life 1: W − D 642,600 vs E + B 642,600; life 0: W − D − B − X − Ab 0 within [0, R] (R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.edge` | PASS | final life 1: E 642,600 == A 642,600 (listener to aggregate); A_sent 2,600 == S_in[victoria_metrics] 2,600 (aggregate to victoria_metrics) |
| `ledger.script` | SKIP | no [ledger] sut_script |
| `ledger.aggregate` | PASS | final life 1: Ab 642,600 == A 642,600 (every event absorbed) |
| `ledger.egress` | PASS | victoria_metrics: life 0: W − D − B − X − V 0 = residual 0 + Ab − V 0, ok; life 1 (final): Ab 642,600 − V[victoria_metrics] 642,600 = 0, ok (split copy; R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.windows` | SKIP | not enabled ([ledger] vm_every_window) |
| `ledger.replay` | SKIP | no killed life followed by another |
| `ledger.summary` | PASS | final life 1: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V[victoria_metrics] 0; wire G − (W + K) 12,293 over the run, by design |
| `ledger.sent` | SKIP | local target: ledger.egress judges delivery against the backends |
| `ledger.topic` | SKIP | victoria-metrics target: no topic |
| `identity.sink` | PASS | final life 1 at +980s: victoria_metrics: received 26 vs delivered 26 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `recovery` | PASS | 7 of 7 fault(s) judged at 1 sink(s), each recovered within recovery_bound 45s of its end; warmup rate 2,000/s |
| `expect` | SKIP | the scenario has no [[expect]] tables |

`fanin`, `20261012T230732Z`, alone:

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 12 apply/revert action(s) returned 0; 2 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 5 exit(s) of the logit services, each a scheduled stop with code 0 |
| `restarts` | PASS | 2 start(s), each a scheduled start or restart |
| `self_log` | PASS | 2 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 117 readiness sample(s) read ok outside fault windows; 2 start(s) or unpause(s) ready in time, slowest ready 0.1s |
| `progress` | PASS | 6 30s window(s) outside fault windows, judged per sink, each freshness sample under 30s: victoria_metrics: deliveries in 6 of 6, 12 freshness sample(s); 150s of 930s after warmup judged (16%; WARN under 5%) |
| `rss_slope` | PASS | generator PASS: MiB/h life 0 +39.4, life 1 -6.8 (limit 64); 295s of 930s after warmup judged (32%; WARN under 5%) \| generator-lines PASS: MiB/h life 0 +7.0, life 1 +0.5 (limit 64); 295s of 930s after warmup judged (32%; WARN under 5%) \| logit PASS: MiB/h life 0 +3.8 (limit 64); 300s of 930s after warmup judged (32%; WARN under 5%) |
| `fd_slope` | PASS | generator PASS: warmup median 11.5 -> end 11 (limit +8) \| generator-lines PASS: warmup median 12 -> end 11 (limit +8) \| logit PASS: warmup median 13 -> end 13 (limit +8) |
| `ledger.wire` | PASS | generator -> statsd: G 1,859,800 = W 1,847,876 + K 0 + wire 11,924 (by design: 11,847 in UDP-affecting windows, 2 steady, 75 at the end; steady loss judged 75, each run at 0 or more, against limit 500); the generator's counted drop_newest loss, not in G: 13,100 event(s) in 131 batch(es) \| generator-lines -> lines: G 1,859,500 = W 1,847,578 + K 0 + wire 11,922 (by design: 11,837 in UDP-affecting windows, 39 steady, 46 at the end; steady loss judged 39, each run at 0 or more, against limit 500); the generator's counted drop_newest loss, not in G: 13,500 event(s) in 135 batch(es) |
| `ledger.intake` | PASS | final life 0 statsd: W − D 1,847,876 vs E + B 1,847,876; final life 0 lines: W − D 1,847,578 vs E + B 1,847,578 (R 349,000 = 10,000 + 2 x 1,000 (statsd) + 10,000 + 262 x 1,000 (lines) + 65 x 1,000) |
| `ledger.edge` | PASS | final life 0: E[lines] 1,847,578 == T_in[parse] 1,847,578 (into parse); T_in[parse] 1,847,578 == T_out[parse] 1,847,578 (parse forwards every event); T_out[parse] 1,847,578 == T_in[labels] 1,847,578 (into labels); T_in[labels] 1,847,578 == T_out[labels] 1,847,578 (labels forwards every event); T_out[labels] 1,847,578 == T_in[count] 1,847,578 (into count); T_in[count] 1,847,578 == T_out[count] 1,847,578 (count forwards every event); T_out[count] 1,847,578 == T_in[metrics_only] 1,847,578 (into metrics_only); T_in[metrics_only] 1,847,578 == T_out[metrics_only] 1,847,578 (metrics_only forwards every event); Σ(sent into aggregate) 3,695,454 == A 3,695,454 (listeners to aggregate); A_sent 13,950 == S_in[victoria_metrics] 13,950 (aggregate to victoria_metrics) |
| `ledger.script` | SKIP | no [ledger] sut_script |
| `ledger.aggregate` | PASS | final life 0: Ab 3,695,454 == A 3,695,454 (every event absorbed) |
| `ledger.egress` | PASS | victoria_metrics: life 0 (final): Ab 3,695,454 − V[victoria_metrics] 3,695,454 = 0, ok (split copy; R 349,000 = 10,000 + 2 x 1,000 (statsd) + 10,000 + 262 x 1,000 (lines) + 65 x 1,000) |
| `ledger.windows` | SKIP | not enabled ([ledger] vm_every_window) |
| `ledger.replay` | SKIP | no killed life followed by another |
| `ledger.summary` | PASS | final life 0: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V[victoria_metrics] 0; wire G − (W + K) 23,846 over the run, by design |
| `ledger.sent` | SKIP | local target: ledger.egress judges delivery against the backends |
| `ledger.topic` | SKIP | victoria-metrics target: no topic |
| `identity.sink` | PASS | final life 0 at +1018s: victoria_metrics: received 101 vs delivered 101 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `recovery` | PASS | 6 of 6 fault(s) judged at 1 sink(s), each recovered within recovery_bound 45s of its end; warmup rate 4,000/s |
| `expect.statsd-idle-while-generator-stopped` | PASS | min_delta of logit.input.datagrams{component=statsd} on logit, during 5: c0s5 (+570s, +630s]: 0 (smallest of 12 drain(s)), want <= 0 |
| `expect.lines-reads-while-generator-stopped` | PASS | min_delta of logit.input.datagrams{component=lines} on logit, during 5: c0s5 (+570s, +630s]: 10,000 (smallest of 12 drain(s)), want >= 5000 |
| `expect.lines-idle-while-generator-lines-stopped` | PASS | min_delta of logit.input.datagrams{component=lines} on logit, during 6: c0s6 (+705s, +765s]: 0 (smallest of 12 drain(s)), want <= 0 |
| `expect.statsd-reads-while-generator-lines-stopped` | PASS | min_delta of logit.input.datagrams{component=statsd} on logit, during 6: c0s6 (+705s, +765s]: 10,000 (smallest of 12 drain(s)), want >= 5000 |

The two `fanin` runs beside `cardinality` differed only in `rss_slope`:

| Check | Status | Detail |
|---|---|---|
| `rss_slope` | WARN | generator WARN: MiB/h life 0 +101.6, life 1 +0.2 (limit 64); 290s of 930s after warmup judged (31%; WARN under 5%) \| generator-lines PASS: MiB/h life 0 +2.8, life 1 +0.5 (limit 64); 300s of 930s after warmup judged (32%; WARN under 5%) \| logit PASS: MiB/h life 0 -3.1 (limit 64); 300s of 930s after warmup judged (32%; WARN under 5%) |
| `rss_slope` | WARN | generator WARN: MiB/h life 0 +105.2, life 1 -2.9 (limit 64); 290s of 930s after warmup judged (31%; WARN under 5%) \| generator-lines PASS: MiB/h life 0 +4.0, life 1 -2.1 (limit 64); 295s of 930s after warmup judged (32%; WARN under 5%) \| logit PASS: MiB/h life 0 +6.1 (limit 64); 300s of 930s after warmup judged (32%; WARN under 5%) |

### A Kafka target: `kafka-broker-restart` (2026-10-11)

Run `20261011T213158Z`: `script/soak run kafka-broker-restart`, the scenario's own 8 minutes 30
seconds, from `kafka/w4` at `a63ec4d5` with the deduplicating topic checks uncommitted, on a
32-core host (`logit:soak` `sha256:607d55b505bf`, `apache/kafka:4.2.0` `sha256:9516fb7634ba`).
Overall PASS: every row passed but `ledger.windows`, which a Kafka target SKIPs, and
`ledger.replay`, which had no killed life.

- **Nothing was lost or stored twice.** The generator sent 1,032,400 lines, and every one was
  read, sent to `aggregate`, and absorbed. The topic held 25,900 records, all distinct, whose
  increments summed to 1,032,400, so Ab − V was 0, and the sink saw 25,900 records accepted, so
  T − A was 0 with no duplicate copy. The sink delivered all 259 batches it received and dropped
  none.
- **The restart** (+61 s to +90 s) cost four `clean` attempts, each `Connection refused`, while
  the broker came back.
- **The pause** (+165 s to +205 s) left one attempt `ambiguous`: `waiting for a Produce response
  timed out after 10s`, with six batches queued behind it, then two `clean` attempts. The
  resend's sequences were the first attempt's, and the topic holds no duplicate copy. The
  harness can't tell
  whether the broker appended the first attempt after the unpause and discarded the resend, or
  never appended it.
- **The stop** (+285 s to +345 s) cost seven `clean` attempts, the broker's name gone from DNS,
  and the sink stopped retrying within the recovery bound.
- **The end sequence** found the sink's queue empty 8 s after the generator stopped, and read
  25,900 records back, matching the partition's end offset, 4 s after the SUT stopped.
- **RSS slopes** were +1.5 MiB/h (SUT) and +1.1 MiB/h (generator), and file descriptors held at
  13 (SUT) and went from 12 to 11 (generator).

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
- **W4**: the self-test passes, and each new rule fails it when reverted (51 rules: every
  `[random]` validation and schedule rule, the draw order, the seed's recording, the
  `ledger.windows` idle excusal, on time and one drain late, across a pause with no drains,
  and past 11 s after a partition, against a batch sent inside the span, the chunk boundaries
  and failure handling, the poll deadlines and round timeouts, and an inherited SIGHUP ignore
  kept for `nohup`). `random-faults` passes `self-test` validation, the shipped-config test,
  and `logit validate` in `logit:soak`. Re-scoring the recorded W1b, W2, and W3 runs gives the
  same statuses; only `ledger.windows`' statement of its rules changes. `check` on a synthetic
  8-hour run directory finishes in seconds. Met: the 1-hour run `20261009T135848Z` and the
  8-hour run `20261009T154710Z` are recorded under "Findings", both Overall PASS.
- **W5**: the self-test passes, and each new rule fails it when reverted (60 reverts: every
  `[target]` validation rule, the env file check and its parsing (`export` lines and quotes
  included), each kind of character the compose check refuses and that check in both
  `soak.py target` and the driver, the private copy's directory, a mode other than 0600, its
  content, and its removal when `execute()` returns and when `prepare()` raises, the signal
  handlers installed before `prepare()`, the compose arguments and
  profiles, compose.yaml's profile, passthrough, and optional dependency, the sink-drain wait's
  three conditions and interval, each SKIP, and each `ledger.sent` verdict). `statsd-datadog`
  passes `self-test` validation, the shipped-config test, and `logit validate` in `logit:soak`
  with a placeholder key. `docker compose config` renders the local shape with
  `victoria-metrics` and the external shape without it. Re-scoring the recorded W3 run gives the
  same rows, every detail included, plus a `ledger.sent` SKIP. Met: the `statsd-datadog` run
  `20261009T155153Z` is recorded under "Findings", Overall PASS.
- **W6** is documentation only: `crates/logit-cli/tests/doc_links.rs` passes.
- **W8a**: the self-test passes (961 expectations), and each new fixture fails it when the rule
  is reverted (12 fixtures: the `overflow_oldest`, `disk_full`, mixed-reason, stderr-zero,
  `rejected` (two), `disk_corrupt`, killed-life, own-copy, `route` (two), and
  `sinks-copy-overflow-elsewhere` cases). Each new
  scenario passes `self-test` validation, the shipped-config test, and `logit validate` in
  `logit:soak`. Re-scoring ten preserved runs gives the same statuses. Met: the four scenario
  runs and the `statsd-vm`, `statsd-vm-lost-total`, and `sink-outage-drop-oldest` controls are
  recorded under "Findings", every row PASS.
- **W8b**: the self-test passes (1,023 expectations), and each harness change fails it when
  reverted (20 reverts, listed under "Findings"). `fanin` passes `self-test` validation, the
  shipped-config test, and `logit validate` in `logit:soak`. Re-scoring runs recorded before W8b
  isn't possible: their resolved scenarios carry the scalar `[ledger]` keys. Met: the `fanin`
  run and the `statsd-vm`, `fanout-copy`, `spool-kill-replay`, and `fanout-stop-outage` controls
  are recorded under "Findings", every row PASS.
- **W8c**: the self-test passes (1,061 expectations), and each harness change fails it when
  reverted (22 reverts, listed under "Findings"). `cardinality` passes `self-test` validation,
  the shipped-config test, and `script/validate` with no warning. Met: the `cardinality` run
  and the `statsd-vm` and `fanin` controls are recorded under "Findings", `cardinality` and
  `statsd-vm` every row PASS; two `fanin` runs WARN on a generator's RSS after a partition,
  which the W8b harness scores the same.
