---
created: 2026-10-08
updated: 2026-10-25
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
`soak/w12d`, a linear stack. After it come `soak/w13a`, `soak/w16`, and `soak/w17`, which each
branch off `main`, `soak/w14`, `soak/w13b`, and `soak/w15`, stacked in that order, and
`soak/w18`, which branches off `main` after W14 merged. PR stack only: nothing is
merged by this workstream; Ross directs merging.

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

Subcommands: `run <scenario> [--duration 20m] [--seed N] [--keep] [--retry-stalled N]
[--out DIR]`, `list`,
`check <run-dir>`, `target <scenario> [--env-file FILE]` (W5: the target's kind, for
`script/soak`), and `self-test`, which also runs at the start of `run`, as
`survey_self_test` does for `script/shape-survey`. Environment: `SOAK_SKIP_IMAGE=1` reuses the
images, and `SOAK_OUT=<dir>` moves the results.

A run writes `perf/results/soak/<UTC stamp>/` (gitignored; `<UTC stamp>-N` when that name is taken): `compose.env`, a copy of
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

An optional top-level `rss_settle` (default `0s`) is how long past `recovery_bound` the
`rss_slope` row alone keeps excusing each fault (§6). Every other row's recovery is
`recovery_bound`. It's opt-in: most schedules leave so little past `recovery_bound` that a
settle window would leave a life no fit. `fanin` and `tcp-ingress` set `60s`, and
`partition-rss` `120s`.

`validate()` enforces these rules:

- Every fault carries `for`, so its revert is scheduled with it. `netem clear` is a revert, never
  a step.
- No two faults overlap on one container, except a `signal` inside a `rename` (W12c), and a
  `signal` or a `probe` inside a `rotate` (W12d).
- No netem on a container during its `stop`, `kill`, `pause`, `partition`, or `exit`, because the qdisc
  is lost with the namespace and `docker run --network container:<id>` needs a running target.
- No fault ends inside `cooldown`, and `cooldown` is at least `recovery_bound`.
- `rss_settle`, when set, is a duration of `0s` or more.
- Actions and services are known. W1's actions are `netem`, `pause`, `stop`, `restart`, and
  `partition`, on any service of the stack: `logit`, `generator`, `victoria-metrics`, and from
  W7b any service a `compose` override adds. W3 adds `kill`, on the SUT only:
  the lines a killed generator sent after its last telemetry drain reach the SUT with no G
  behind them, and `ledger.wire` would net them against real loss. W7a adds `mark` and `exit`
  (below). W9a adds `respond`, on a stand-in only (below). W12c adds `signal` and `rename`, on
  a service that runs `logit` (below), and W12d `rotate` and `probe` (below).
- `[ledger] vm_every_window`, optional, is `true` or `false`; `[ledger] sut_script`, optional,
  is a component id.
- `[[ledger.source]]` (W8b) has the rules under "Sources" below.
- A scenario has `cycle` with `[[step]]` tables or a `[random]` table (W4), never both.
  `--seed` is refused for a fixed schedule, because no seed changes it.
- `max_duration` (W11b), optional, is the longest run a scenario's thresholds and configs
  hold. A `--duration` past it is refused, and so is a scenario whose own `duration` is.

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
  and `kill` is on a `sut` service only; a netem template has `args`, none of them a clear, and
  no other action has `args`; unknown keys are refused.
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
namespace, so no netem on that container during it, it is UDP-affecting on `logit`, and it may
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
on = "logit"              # a sut service only
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
  elsewhere, and an `exit` is on a `sut` service only (`logit` before W10a). It is UDP-affecting
  on `logit` alone. The process can end at any point in the window,
  so a netem on its container may not overlap it. While an `exit` is open the driver's fail-fast
  accepts `running` or `exited`. "The checks" has its rules in `exit`, `self_log`, and `ready`.
- Neither may appear in a `[random]` schedule, which draws faults; an expected event happens
  at a time the scenario's own config sets.

**Responds and classes (W9a)** script a stand-in target's answers ("Stand-in targets"):

```toml
[[step]]
at = "0s"
action = "respond"        # sets the stand-in's answer for the step's span
on = "standin"            # a service with the role standin
status = 400              # 100 to 599; or answer = "reset"
body = "out of order sample"   # optional, with status only: the response body
apply = false             # optional: store the write anyway (true) or not (false); by default
                          # the stand-in stores what it answers 2xx
class = "rejected"        # optional: the fault class the sink must read
for = "20s"
```

- A `respond`'s apply POSTs `{step, status | answer, body, apply}` to the stand-in's control
  port, and its revert POSTs `{}`, which restores the default answer. A non-200 from the control
  port is the record's `rc` 1, so `timeline` FAILs it and the step has no window.
- A `respond` sets one of `status`, an integer from 100 to 599, and `answer`, which is
  `reset`: the stand-in reads the whole request, then closes the connection with an RST and no
  response. `body` is a string, with `status` only. `apply` is true or false. None of the four
  is allowed on another action.
- `class`, one of `clean`, `ambiguous`, `rejected`, or `refused`, names the class the sink must
  read the step's answers as
  ([ADR `sink-fault-classes`](../adr/sink-fault-classes.md)). It is allowed on a `respond` and
  on a `stop`, whose connect failures are `clean`; `ledger.standin` judges it (§6).
- With the role map, a `respond`, or a `stop` with a `class`, is on a `standin` service. A
  `respond` never affects UDP ingress and always affects egress.
- A `respond` can't appear in a `[random]` schedule, whose faults take no status or answer.
- W9b adds `series_errors = N`, for a stand-in target with `shape = "datadog"` only, on a
  `respond` with a `2xx` `status` and no `body`: the answer's body is the series route's
  `errors` list naming N series dropped, which the checks take to be the request's first N.
  Under that shape every `respond` other than a `2xx` needs a `class`, and a `2xx` takes none.
  The step's `class` is the harness's only copy of `datadog_out`'s response-class table, so the
  loader checks that one is there, not which one it is; `ledger.standin` judges it against the
  sink. W16 holds `shape = "influxdb"` to the same rule, for `influxdb_out`'s table.
- `count = N`, a positive integer on a `respond` (the `conc` stream's W5), spends the cue on
  the next N writes to the stand-in's path, after which it answers the default with no cue
  until the revert. A sink at `concurrency:` above 1 has several requests in flight, so a cue
  in force for the whole span would answer all of them; a counted cue lands on one.
  `ledger.standin` FAILs a counted step whose cue answered another number of writes, since
  that step tested less than it names.

**Signals and renames (W12c)** drive a `logit` process's SIGHUP handling
([ADR `signal-handling`](../adr/signal-handling.md)): the signal bumps the reopen generation,
and each `stdio_out` or `file_out` file target reopens its path before its next write.

```toml
[[step]]
at = "2m"
action = "rename"         # an external rotator's move, on the host
on = "logit"              # the service whose config has the stdio_out
path = "archive.log"      # a stdio_out target in /shared, the mounted <run>/shared
to = "archive.log.1"      # the name it moves to, in the same directory
for = "30s"

[[step]]
at = "2m10s"
action = "signal"         # docker kill -s HUP
on = "logit"              # a service that runs logit
signal = "HUP"
for = "10s"
```

- A `signal`'s apply is `docker kill -s <signal>` through the same CLI path as a `kill`, and
  its revert does nothing. `signal` is required, and `HUP` is its one value. It is on a `sut`
  or `generator` service. Like any step it may not overlap another on its service, a `rename`
  aside, so it never lands in a `stop`, `kill`, `pause`, `restart`, or `exit`, when the process
  may not be running to receive it.
- A `rename`'s apply is an `os.rename` inside `<run>/shared` on the host, which the driver
  creates for a scenario with one, and its revert does nothing. It refuses to replace a file
  already at `to`. `path` and `to` are bare names; `path` is a `stdio_out` with a `target:` in
  `/shared` in the config of the service the step is `on`, and `to` is neither such a target
  nor another rename's `to`. A `signal = "HUP"` step on the same service starts inside its
  `for`, after its start. A rename runs once per run, since a second cycle's would find its
  `to` taken, so a scenario with one sets `max_duration` to one cycle's run.
- Neither opens a fault window: every `affects_*` predicate is false, and the checks keep them
  out of the windows that excuse anything (`RunData.events`). `[[expect]]` reads either's
  window, and `file.reopen` (§6) judges each rename.
- Neither can appear in a `[random]` schedule, whose faults take no `signal`, `path`, or `to`.

**Certificates and rotations (W12d)** drive TLS reload
([ADR `tls-certificate-reload`](../adr/tls-certificate-reload.md)): a `[tls]` table gives the run
a private CA, a `rotate` step issues a certificate's next generation behind a `live` symlink, and
a `probe` reads what a listener serves after the SIGHUP that reloads it.

```toml
[tls.cert.statsd]         # one table per certificate
san = ["logit"]           # its DNS names; a probe verifies the first
usage = "server"          # server or client

[[step]]
at = "0s"
action = "rotate"         # issues gen N + 1 into <run>/tls and repoints statsd/live
on = "logit"              # the service whose config reads /tls/statsd/live/
cert = "statsd"
days = 31                 # above 30 and every earlier rotation's
broken = false            # optional: true writes a key that doesn't match
for = "40s"

[[step]]
at = "25s"
action = "probe"          # s_client in the service's network namespace
on = "logit"
cert = "statsd"           # a server certificate
port = 8125
for = "5s"
```

- `[tls]` takes `cert` only, one table or more. A certificate's name is a slug other than
  `probe`, the probe's own client certificate; `san` is a non-empty list of DNS names; `usage`
  is `server` or `client`. Unknown keys are refused.
- Before the stack starts, `prepare()` runs `certs.sh` (`tools/soak/netem/certs.sh`, in the
  netem image, as the host user) into `<run>/tls`: `init` makes the CA and the probe certificate,
  and `issue` each certificate's first generation, `gen-1`, valid 30 days, recorded in the
  timeline as the `tls_issued` phase. Keys are EC P-256 with v3 extensions, and every file is
  0644, so the image's `logit` user reads it. Each generation is a directory, and `<name>/live`
  is a relative symlink that `issue` repoints with one `rename(2)`, so each file read through it
  is one generation's or the other's. The swap is atomic per file, not across a set: a reload
  reads a set's files one at a time, so a poll that lands on the rename can pair two
  generations, fail, and load the new pair on its next poll. A certs.sh failure stops
  `prepare()`.
- A `rotate`'s apply issues the next generation and records its `gen`, `serial`, and
  `not_after`; a failed issue still uses its number up. `cert` and `days` are required, `days` a
  whole number above 30 and above every earlier rotation of the certificate, so its `notAfter`
  rises; `broken`, optional, writes a key that doesn't match the certificate, which a reload
  refuses. The step's service must read `/tls/<cert>/live/` in its config. A rotation runs once
  per run, since a second cycle's would name the same days, so a scenario with one sets
  `max_duration` to one cycle's run.
- A `probe`'s apply runs `openssl s_client` from the step's service's network namespace against
  `127.0.0.1:<port>`, presenting the probe certificate, so a listener that requires one completes
  the handshake and counts no `connection_error`. It records the served `serial` and `not_after`,
  and `verified`, whether the chain verifies against the CA for the certificate's first DNS
  name. `cert`, a `server` certificate, and `port` are required. A probe starts inside a
  `rotate` of its certificate on its own service, after a `signal = "HUP"` step there that
  starts inside the same rotate, because a listener reloads only on a SIGHUP or its poll. On a
  service with `tls_reload_interval: 0s`, every earlier rotate of a certificate a probe reads
  holds a SIGHUP on that service too, or a start of the service (a `restart`, `stop`, `kill`,
  or `exit`) falls between it and the certificate's next rotate: `tls.served` expects the
  latest rotation that wasn't broken, which only a reload or a start loads.
- Neither opens a fault window, and neither reverts anything. Neither can appear in a
  `[random]` schedule.

**`sut_script` (W7a)**, an optional `[ledger]` key, names a `lua` component between the
listener and `aggregate`, and adds the script hop to the ledger ("The ledger and identities").

**Compose overrides, service roles, and sinks (W7b).** Every compose service carries a
`soak.role` label: `sut`, `generator`, `backend`, `broker` (a Kafka target's broker; see
"Kafka targets"), `standin` (W9a, a stand-in target's destination; see "Stand-in
targets"), `idp` (W19, a token endpoint the SUT reaches on loopback; see "A token endpoint in
the SUT's namespace"), or `netns` (W12b, a namespace holder: a rolling overlap's, see "Rolling
overlap", or from W19 the one `logit` and an `idp` share). The harness knows a service by its role, never its name. Its ingress SUT is `logit`,
and from W10a an override can add further SUT services (see "Several SUT services").
The base stack's generator is `generator`; from W8b an override can add more (see "Sources"). A scenario adds services with an optional top-level key naming an override file
beside `scenario.toml`:

```toml
compose = "compose.override.yaml"   # merged onto tools/soak/compose.yaml

[ledger]
split = "route"           # copy (the default), route, or target (W15)
# ...

[[ledger.sink]]           # one per SUT sink
id = "vm_a"               # the sink's component id
target = "tier_a"         # route and target only: the target that feeds it
backend = "victoria-metrics"                                # local only: a backend service
vm_selector = '{__name__=~"soak_requests_[0-9]+_total"}'    # local only: the series V reads
```

- `[[ledger.sink]]` replaces W1's `[ledger] sut_sink` and `vm_selector`. A scenario lists one
  or more sinks with distinct ids. For a local target each sink names a `backend` and a
  `vm_selector`; for an external target it names neither.
- `split` says how the sinks share `aggregate`'s output. Under `copy` each sink, fed by a
  fan-out, holds all of it. Under `route` each sink is fed by one `target` that a `route` fills,
  so the sinks hold disjoint shares; `route` needs two or more sinks, each with a distinct
  `target`, and `copy` refuses `target`. Under `target` (W15) each sink sits behind its own
  `target` and `aggregate`, filled by a router that can send one event to several of them;
  see "The target split (W15)".
- `compose` names a file beside `scenario.toml`, never a path. Each service it adds needs a
  `soak.role` label, and a backend needs `profiles: [local]`.
- `load()` checks every rule above that needs no service list. The rest need the merged
  compose file, which only the driver reads (`docker compose config`, §3): every step's `on`
  and every random fault's is a service of the stack, a `kill` or `exit` is on the role `sut`,
  a `respond`, or a `stop` with a `class`, is on the role `standin` (W9a),
  every `[[expect]]` service runs `logit`, every sink's `backend` has the role `backend`, the
  stack's `sut` services are `logit` and each further one `[configs]` names (W10a), and none
  other, with at least one `generator`, every source's `service` has
  the role `generator` and every `generator` has a source (W8b), and no service lacks a known
  role. The driver checks them in `prepare()`, before `up`. `list` and the self-test
  can't run compose, so they pass the base role map (`scenario.BASE_ROLES`) for a scenario with
  no override, and the self-test reads a shipped override's labels itself.

**Sources (W8b).** Each `[[ledger.source]]` is one generator and the SUT listener its
lines reach, over the source's wire (W11a). They replace W1's scalar `[ledger]` keys `generator_input`, `generator_sink`,
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
generator_sink = "statsd" # its sink, whose output.messages is G
sut_listener = "statsd"   # the SUT listener its lines reach

[[ledger.source]]
service = "generator-lines"
generator_input = "load"
generator_sink = "syslog"
sut_listener = "lines"
sut_transforms = ["parse", "labels", "count", "metrics_only"]   # optional, in order
```

- A scenario lists one or more sources. Each names all four keys, each a component id (from
  W12b `generator_input` and `generator_sink` may both list components, as many of each, each
  input feeding the sink at its position, one sink per UDP flow; their counts sum), with a
  `service` that is a slug (lowercase letters, digits, `_`, and `-`) no other source names and
  a `sut_listener` no other source names. Unknown keys are refused. A `service` whose compose
  variable is `SOAK_SUT_CONFIG` or another source's (`generator-lines` and `generator_lines`
  share one) is refused.
- `[configs]` has `sut` and one key per source's `service`; any other key names a further SUT
  service (W10a, "Several SUT services" below). The driver writes each generator's absolute
  config path into `compose.env` under `scenario.config_env(service)`:
  `SOAK_GENERATOR_CONFIG` for `generator`, `SOAK_GENERATOR_LINES_CONFIG` for
  `generator-lines`. An override's generator mounts its own variable at `/config.yaml`.
- `wire`, optional (W11a), is `udp` (the default), `tcp`, `file`, or `http` (W14): what the
  source's lines cross into the SUT, which picks the ledger's symbols for it (§6). `load()`
  reads the SUT config and checks the source's `sut_listener` against it: a component of a kind
  and `transport:` the ledger has a wire for (`scenario.LISTENER_WIRES`: `statsd_in` and
  `syslog_in` UDP unless `transport: tcp`, `lines_in` and `graphite_in` TCP unless
  `transport: udp`, `collectd_in` UDP, `tail_in` and `docker_in` file, `influxdb_in` and
  `otlp_in` HTTP), whose wire is the source's. A listener the config lacks, a kind or transport
  with no wire (a Unix socket), and a mismatch are refused. It also reads the generator's
  config: a file wire's `generator_sink` must be a `file_out`, an HTTP wire's an `influxdb_out`
  or an `otlp_out`, and any other wire's neither.
- `sut_transforms`, optional, lists the nodes between the listener and `aggregate`, distinct, none
  of them a listener, and none on another source's list. Each must send every event it
  receives or count it in `logit.component.events.discarded`, which `ledger.edge` judges hop by
  hop (from W13a), and each adds an inbox and a batch to R ("The ledger and identities"). The
  nodes' discards are counted loss everywhere else in the ledger too (§6). It can't be combined with
  `[ledger] sut_script`, whose hop
  `ledger.script` judges from every listener's E.
- `fan_out`, optional (W13b), admits a node that emits several events per event: a table from a
  `line_protocol` node of the source's `sut_transforms` to the events it emits per event, an
  integer of at least 2, such as `fan_out = { points = 4 }`. `load()` checks the node's kind
  against the SUT config, and refuses `fan_out` under `[overlap]`, whose end-to-end bound
  counts the generators' lines. The term needs every message to yield k distinct
  points and carry no other payload; a message whose points merge, that has none, or whose
  parent carries metrics or a span and goes on beside its children reads as a `ledger.edge`
  FAIL, since no counter counts such a message.
- With the role map: every source's `service` is a `generator`, and every `generator` in the
  stack has a source, because a generator with none would put lines into a listener with no
  G behind them.

**Several SUT services (W10a).** The stack can run more than one `logit` under test, such as a
second one at the far end of a native hop. `logit` stays the ingress SUT: every generator sends
to it, every source's `sut_listener` is one of its components, and its config is `[configs]
sut`. A further SUT service is declared by a `[configs]` key of its own, which is neither `sut`
nor a source's `service`, and run by a compose service of that name labeled `soak.role: sut`,
which the scenario's override adds:

```toml
compose = "compose.override.yaml"   # adds collector, labeled soak.role: sut

[configs]
sut = "logit-sut.yaml"              # logit, the ingress SUT
collector = "logit-collector.yaml"  # a further SUT service, mounted from SOAK_COLLECTOR_CONFIG
generator = "logit-generator.yaml"
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

- The SUT services form a chain: `logit`, then each further one in the order `[configs]` lists
  it (`Scenario.suts`, recorded in `scenario.resolved.json` as `suts`). The chain's last service
  holds `sut_aggregate` and every `[[ledger.sink]]`.
- From W10b a second SUT service needs a `[[ledger.hop]]` naming the native hop that joins it to
  `logit`, and a scenario has at most one hop, so at most two SUT services ("The native hop
  (W10b)" in §8d). From W12b an `[overlap]` table instead runs two or more instances of one
  SUT on one port, each a whole pipeline, with no hop ("Rolling overlap (W12b)" in §8e).
- A further SUT's key is a slug, isn't `logit`, and has a compose variable,
  `scenario.config_env(service)`, that no source and no other SUT service shares. With the role
  map, the stack's `sut` services are `logit` and those keys, so a `sut` service the scenario
  doesn't declare is refused, as is a declared one the stack doesn't run as a `sut`.
- A `kill` or an `exit` may name any SUT service. A fault on a further SUT service never affects
  UDP ingress (§4), since no generator sends to it.
- The ledger across the two follows the hop (§8d). In W10a, before the hop ledger, the rows
  that equate a listener's symbols with `aggregate`'s in one life SKIPped across two services.

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
reduce = "max"                          # delta | min_delta | rate (W17) | max | min | last | slope (W8c) | rise (W12d)
min = 2                                 # min, max, or both
```

| Field | Meaning |
|---|---|
| `step` | An expanded step id names one occurrence. An integer names that `[[step]]` table's occurrence in every cycle, and the row FAILs if any occurrence does. `window = "run"` takes none |
| `window` | `during` is the fault's span, from its apply starting to its revert finishing; `after` is the `recovery_bound` after it; `through` is both, from the apply starting to `recovery_bound` after the revert finishes. Each is open at its start, because a drain's deltas cover the interval before its timestamp. A bound that must hold for the whole episode, such as a loss counter that must stay 0, reads `through`: a chain blocked behind a sink stays blocked until the sink's next retry after the revert, up to its `retry_max_delay` later, so `during` misses the tail. `run` (W8c) has no step: it runs from the end of warmup to the start of the end sequence, fault windows included |
| `reduce` | For a counter (`kind: sum`): `delta` sums the window's drains, `min_delta` is the smallest single drain, a drain with no point counting 0, because a loss counter isn't emitted while it's 0, and `rate` (W17) is the `delta` divided by the window's length in hours, so a bound on a steady rate holds at any `--duration`. For a gauge: `max`, `min`, and `last` over the window's samples and the value in force at its start, because `internal` exports a gauge only in a drain after it was set. The value in force comes only from the SUT life whose drains cover the window's start, so a value an earlier process set doesn't carry into the next; a life that hasn't set the gauge yet leaves it unset. `last` is the value in force at the window's end. `slope` (W8c) is the least-squares slope of the window's own samples, without the value in force, in the metric's unit per hour (`checks.slope()`, times 3,600). A restart resets a process gauge, so `slope` fits each SUT life on its own and judges each life's slope against the bounds; a fit across a restart would read the drop as a negative trend. A life needs `rss_slope`'s minimum, 6 samples spanning 60 s, and the row FAILs when no life in the window has it, as a counter's window with no drain does. `rise` (W12d) is the value in force at the window's end less the one in force at its start, and the row FAILs with no value in force at the start |

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
may instead name a local Kafka broker as its target; see "Kafka targets", or a scripted stand-in
destination (W9a); see "Stand-in targets".

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
| `logit` (the ingress SUT, role `sut`; an override can add more, §2's "Several SUT services") | `depends_on: [victoria-metrics]`, not required from W5, when `victoria-metrics` moved to the compose profile `local` the driver enables for a local target only; `environment:` passes each variable an external target may list (`DD_API_KEY`), empty when unset |
| `generator` (role `generator`) | `depends_on: {logit: {condition: service_healthy}}`. An override can add more generators (W8b), each with `extends` from this one and its own config volume |
| `kafka` (role `broker`) | `apache/kafka:4.2.0`, a single-node KRaft broker in the compose profile `kafka`, which the driver enables for a Kafka target only; one partition per topic, topics created on first use, a `kafka-broker-api-versions.sh` health check the SUT's optional `depends_on` waits for. No published port: the driver reads the topic through `docker exec` |
| `standin` (role `standin`, W9a) | `python:3.13.16-alpine3.24` running `tools/soak/standin/standin.py`, mounted read-only, in the compose profile `standin`, which the driver enables for a stand-in target only. Its data port, 8080, isn't published; its control port is, `ports: ["127.0.0.1::8081"]`, which the driver re-reads after any start of the service, as a backend's. A health check against the control port's `/health`, which the SUT's optional `depends_on` waits for. Its stdout is the record |

Each service's role is its `soak.role` label (W7b). A scenario's `compose` override is passed
after the base file, `-f compose.yaml -f <override>`, on every `compose` call, and copied into
the run directory's `configs/`. Before `up`, `prepare()` runs `docker compose config --format
json`, which merges and interpolates the files with no daemon call, builds `{service: role}`
from the labels, checks the scenario against it (§2), and records it in
`scenario.resolved.json` as `services`. The self-test passes a role map instead, so no part of
it can reach Docker. An override that adds a service with `extends` names the base file as
`file: compose.yaml`: compose resolves the path against the project directory, the first
file's, not the override's own.

Configs mount from absolute paths in `compose.env` (`SOAK_SUT_CONFIG`, one variable per
generator, `SOAK_GENERATOR_CONFIG` for `generator`, §2's "Sources", and one per further SUT
service, `SOAK_COLLECTOR_CONFIG` for `collector`, §2's "Several SUT services"), which the
driver passes with `--env-file`, because `sudo` strips the environment.
`up -d --wait --wait-timeout 120` waits on the image's `logit ready` `HEALTHCHECK`.
Nothing in a container writes to the run directory in W1, so it needs no `chmod 777`. From
W11b a scenario with a `file` wire is the exception: the driver creates `<run>/shared` with mode
0777, since the image's unprivileged `logit` user has a uid the host doesn't know, names its
absolute path in `compose.env` as `SOAK_SHARED_DIR`, and the scenario's override mounts it
`:z` at `/shared` in the writer and the SUT. The writer's files stay in the run directory, the
evidence the end sequence counts G from. From W12c a scenario with a `rename` step gets the same
directory, mounted into the service whose `stdio_out` writes there. From W12d a scenario with a
`[tls]` table gets `<run>/tls`, mode 0755 whatever the host umask, named `SOAK_TLS_DIR`, which
the override mounts `:ro,z` at `/tls` as a whole directory, so a repointed `live` symlink reaches
every container that reads it. The symlink swap is atomic per file, not across a certificate's
pair (§2's "Certificates and rotations (W12d)").

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
| `logit` | the SUT's remote-write to VictoriaMetrics, not the statsd arriving at it; on a TCP source (W11a), the ACKs it sends the generator; on an HTTP source (W14), its answers |
| a `generator` | its own UDP into the SUT, and no other generator's; on a TCP source, segments TCP retransmits; on an HTTP source, its requests |
| a further SUT service (W10a) | its own sends, never UDP ingress, which only `logit` takes |
| a `backend` | its responses |

Each container has one peer in this scenario, so there is no `tc filter`. Per-peer `u32` filters
and `ifb` ingress shaping wait for a scenario that needs them.

A `partition` records the container's network `Aliases` before the disconnect and passes them
back with `--alias` on the reconnect, because `docker network connect` drops the service alias.
From W11a it also records the container's address on each network before the disconnect
(`ips`) and after the reconnect (`ips_after`). Docker may assign another on the reconnect, and a
TCP peer's retransmits reach the container only at the address it had; there is no static
address, and `ledger.wire` says which happened.

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
   - **Stall:** a tick more than 120 s late (`STALL_S`, past the slowest thing the loop does)
     means the host slept or the driver was stopped, and nothing was scheduled or polled in that
     span. The loop records a `stalled` phase and an `aborted` phase with that reason, then
     ends the timeline before any further step: every step overdue by then would run back to
     back. The end sequence and collection still run, so `check` can re-score the directory.
4. End (for an external target, W5, see "External targets"): revert active faults; `docker stop -t 30` every generator; with a `file` wire (W11b), count the
   newline-terminated lines in every file of `<run>/shared` (phase `shared_counted`), read G,
   the writers' `events.delivered`, from a log chunk of each writer taken 6 s later, past the
   chunk's 5 s lag, and wait, up to 180 s, until the tailers' W, read from a log chunk the
   driver captures every 5 s, reaches G or holds for four reads (phase `tail_caught_up`, which
   records G and the count); G, not the count, since a writer that deleted a file (W18) left
   fewer lines than it wrote, and W reaches that count before the tailers read to the end; wait until every sink's stored
   total holds across two aggregate windows (90 s bound); `docker stop -t 60` the SUT, expecting
   exit 0; call each backend's `/internal/force_flush`; write each sink's
   `vm-export/<sink>.jsonl` from its backend's `/api/v1/export` with `match[]=<vm_selector>`
   and `start=0`. With several SUT services (W10a), each stops in chain order, one
   `sut_stopped` phase naming each: before an earlier one stops, the driver waits until a drain
   of its own stdout, two aggregate windows after the wait began, shows its `[[ledger.hop]]`
   sink (W10b) with `buffer.batches` 0, and `retrying` and `logit.output.in_flight` 0 or never
   set (120 s bound, a `sink_drained` phase naming the service); the last waits as above, on
   its sinks. Under `[overlap]` (W12b) one wait on the stored totals covers every instance, and
   then each instance stops (§8e). After a stall the sequence is the same: its waits are each
   bounded, so an aborted run ends within minutes of the host waking, and the directory has
   what any run's has.
5. Collect: the last `docker logs` chunk per service, from its cursor to the end, into
   `.stdout` and `.stderr`, and `inspect/<svc>.json`.
   Then `compose down -v --remove-orphans`, unless `--keep`. A `try`/`finally` makes SIGINT still
   collect, and the driver routes SIGTERM and SIGHUP into SIGINT's `KeyboardInterrupt`, because
   Python's default for both ends the process without running `finally`. A signal the driver
   inherited as ignored stays ignored, so `nohup` keeps a run alive when bash resends SIGHUP to
   its jobs as the terminal closes.
6. Score with `checks.run_all(run_dir)` into `results.md` and `results.json`, whose header
   names the seed of a random schedule. Exit 1 on any FAIL
   or an aborted or errored run, 2 when the run never started (a scenario, argument, or setup
   error), 3 when `run` FAILs a stall, and 130 when interrupted; 3 outranks 1 but not 130.
   `check` exits 0, 1, or 3 the same way. `--retry-stalled N` reruns a stalled run up to N times,
   30 s apart, each in a new run directory with the same duration and seed. The reasoning is
   [ADR `soak-harness`](../adr/soak-harness.md)'s stall amendment.

### 6. The checks (W1a, W1b)

A check reports PASS, WARN, or FAIL; WARN is for a trend a short run can't settle.
`telemetry.py` mirrors the NDJSON grammar in `crates/logit-outputs/src/ndjson.rs`, whose module
doc gains `tools/soak/` in its list of readers. Counters (`kind: sum`, delta) sum per **process
life**, split where `logit.process.uptime` decreases, with the number of lives cross-checked
against the timeline. Gauges are series. With several SUT services (W10a) each has its own lives,
cross-checked against the starts the timeline gives that service, and its own killed lives: the
listener symbols (W, K, D, B, E, X) are read on the ingress SUT's lives, and everything from
`aggregate` on (A, Ab, the sinks, V split per life, `ledger.windows`, `ledger.replay`,
`identity.sink`, `progress`, `recovery`, `ledger.sent`) on the lives of the service holding
`aggregate`, from its own telemetry and stderr. `drops` reads every SUT service and names it.
The two are joined by the native hop's ledger (W10b, §8d), never by pairing their lives.

A "fault window" below is a fault's span plus `recovery_bound` after it. A fault whose apply
returned nonzero has none, so nothing after it is excused.

**The watchdog (W1a):**

- `run`: the timeline has `start` and `end_end`, and no `aborted`, `error`, or `interrupted`
  phase. An interrupted run FAILs so its `results.md` can't read as a pass; the driver still
  exits 130 for it. From W12b a run also FAILs as `stalled` when the driver's 1 s tick ran more
  than 120 s late, past anything the loop does, as when the host sleeps: nothing was scheduled
  or polled in that span. The driver records a `stalled` phase; for an older run directory the
  row reads the same from a gap in `watchdog.jsonl`. The driver ends the schedule at the first
  stall, with an `aborted` phase naming it, and exits 3 (§5).
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
  ERROR line FAILs. From W12d a `tls_reload_failed` line FAILs, except inside a `rotate` step
  with `broken = true` on its service, from its apply to its revert, or, on a service whose
  config polls its TLS files (`tls_reload_interval` unset or above 0), inside any other rotate
  on it when a `tls_reloaded` line of the same component follows it in that rotate: a torn
  read of a set mid-rename. Inside an `exit` step's
  window (W7a), the `exiting` line with the step's
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
  fallback series. Judged slopes covering under 5% of the run after warmup WARN.
  `rss_slope` widens each fault window by the scenario's `rss_settle` (default 0 s), because
  a process can return a backlog's memory in steps over minutes: `fanin`'s generator held 62.6,
  then 52.6, then about 32 MiB for over 100 s after a 45 s partition, and a fit over those
  plateaus beside its 18 MiB baseline read +240 MiB/h. `fd_slope` keeps `recovery_bound`: it
  compares single samples with the warmup median rather than fitting a line, and a connection's
  fd closes when the connection does, with no allocator in between. A service with samples
  after warmup but none outside the widened windows WARNs on coverage, and the row SKIPs for
  want of a fit only when the run after warmup is shorter than the 60 s a fit needs. From W8c both
  rows judge each `logit` service apart, the SUT and every generator, with one detail line per
  service and the worst status, as the per-sink rows do; a generator that interns what the SUT
  interns grows beside it, and one verdict for both would say nothing about the SUT. A
  scenario's `[expected]` table can set a service's verdict (§2, "Expected verdicts").

**The ledger and identities (W1b).** One unit runs through the ledger: a line is one event is
one increment, and on a UDP wire one datagram.

From W8b the ledger also follows each `[[ledger.source]]`, a generator and its SUT listener.
The listener symbols W, K, D, B, E, and X are kept per listener and summed over the listeners;
G is per source. `ledger.wire` judges each source and `ledger.intake` each listener, with one
detail line per source or listener and the worst status. From the edge into `aggregate` on,
every row reads the sums, because the sources' events merge there: `ledger.edge` judges ΣE
against A, and the residual, `ledger.egress`, `ledger.summary`, the kill band, and `recovery`
read summed symbols. Both legs of a fan-in run in one unit, a line that is one event and one
increment, so the sums are of one unit.

From W11a each source's `wire` picks its symbols. The table gives the UDP ones. On a TCP wire G
is the same `output.messages`, which `statsd_out` counts only once a batch's write completed,
with the precondition reversed: the sink counts no `output.datagrams`. W is
`logit.input.frames{component=<sut_listener>}`, one per LF-terminated line, K and D are 0, and
F, the lines the listener's framer dropped before W counted them
(`logit.input.frames.dropped`, reasons in the table below), is counted wire loss; only `undecodable`, counted after `input.frames`, stays in B. A TCP listener
has no receive queue, so R holds no queue term for it. On a file wire (W11b) G is the writer
`file_out`'s `logit.component.events.delivered`, since `file_out` counts no `output.messages`,
W is `logit.input.lines{component=<tail_in>}`, K and D are 0, and F is the tailer's
`logit.input.frames.dropped` under `oversize` (a line past `max_line_bytes`, never counted in W)
and `truncated` (a held partial discarded when its file was truncated). `kill_band` and `recovery`
read the wire's W; `_end_to_end` and `ledger.summary` also subtract F.

On an HTTP wire (W14) G is the writer's `logit.component.events.delivered`, the points of the
batches the listener answered with a success, and W is the listener's
`logit.component.events.sent`: neither `influxdb_in` nor `otlp_in` counts points before it sends
them, so W is also E, and K, D, F, and B are 0, since a request the listener refuses decodes to
nothing it sends. The wire compares G with W − X: a batch refused with `503` because every
consumer had closed is in `events.sent` and its events in X, and the client still holds it.
`checks.HTTP_LISTENERS` is the per-listener table of what else each counts. `influxdb_in`
decodes a body whole into one batch, sends it under one deadline, and answers `204` once it is
in every inbox, `503` busy after 5 s with nothing taken and the batch in
`logit.input.batches.deferred`, or `503` `closed_consumer`; it counts each request in
`logit.input.requests{route, class}`. So per life its `ok` and `closed_consumer` write requests
sum to its `logit.component.batches.sent`, and its `busy` ones equal `batches.deferred`, which
`ledger.intake` holds it to in every life a kill didn't end; a SIGKILL loses the killed life's
last interval of counts, so that life is reported unjudged. A busy `503` is a retry the producer makes, not loss. `otlp_in`
counts no requests and never answers busy: a slow downstream holds the request, and a client
that gives up leaves its batches to be delivered, so it has no identity beyond W − D == E + B.
The writer's `logit.component.events.received` is reported beside G as the points attempted,
and its `logit.output.requests{class="5xx"}` as the answers it retried.

From W7b the ledger follows each `[[ledger.sink]]`. A row about a sink (`progress`,
`identity.sink`, `recovery`, `ledger.edge`'s sink edges, `ledger.egress`, `ledger.windows`,
`ledger.replay`, and `ledger.sent`) keeps its id, judges each sink, reports one line per sink,
and takes the worst status; `report.py` is unchanged.

| Symbol | Source |
|---|---|
| G | per source (W8b), its generator's `logit.output.messages{component=<generator_sink>}`; precondition: equals that sink's `logit.output.datagrams` |
| W | per listener (W8b), SUT `logit.input.datagrams{component=<sut_listener>}` |
| K | per listener, SUT `logit.input.kernel.dropped` |
| F | per listener, SUT `logit.input.frames.dropped`: on a TCP wire (W11a) under the framer's reasons, `truncated`, `oversize`, `drained`, and `malformed`; on a file wire (W11b) under `oversize` and `truncated`; else 0 |
| D | per listener, SUT `logit.component.datagrams.dropped` under every reason, from telemetry; the listener's stderr shutdown lines only cross-check its `shutdown` part, and a killed life counts none |
| B | per listener, SUT `logit.input.messages.dropped{reason=malformed}` plus `logit.input.frames.dropped{reason=undecodable}` plus `logit.input.frames.dropped{reason=oversize}` (W8b): a line the decoder refused, a datagram it refused whole, and a line over `lines_in`'s `max_line_bytes`, which its decoder drops with a throttled diagnostic and no decoder error. Each is 0 on a listener with no such rule |
| E | per listener, SUT `logit.component.events.sent{component=<sut_listener>}` |
| X | per listener, SUT `logit.component.events.dropped{component=<sut_listener>}` under every reason (W7a): events the listener counted as sent and then dropped, such as a batch sent into the inbox of a node that has failed (`closed_consumer`) |
| T_in[node], T_out[node] | with a source's `sut_transforms` (W8b), each node's `logit.component.events.received` and `logit.component.events.sent` |
| T_disc[node] | each such node's `logit.component.events.discarded` (W13a), the events it received and sent no part of, which the runtime counts for every Transform node, only when nonzero, so an absent count is 0; ΣT_disc sums every node of every source |
| T_lines[node] | each such node's `logit.transform.lines.dropped` under every reason (W13b); only `line_protocol` counts it, under `malformed`, for a bad line in a message whose log doesn't go on |
| W′ | with a source's `fan_out` (W13b), each listener's W − D − B − X carried through its chain (`Ledger.through`): a `fan_out` node of k turns x events into k × x − T_lines + (T_in − T_disc), its children and the parents it sent on; any other node into x − T_disc. Summed over the listeners, it takes W − D − B − X − ΣT_disc's place in the residual, a stopped life's egress gap, and the end-to-end bound; R and a killed life's ingest rate grow by each chain's product of k. A fan-out node's own T_disc counts its parents, which went on as children, so it is never loss |
| A | aggregate `logit.component.events.received` |
| A_sent | aggregate `logit.component.events.sent` (W7b), counted once per send however many consumers a fan-out reaches |
| S_in[sink], T_sent[target] | each sink's `logit.component.events.received`, and under `split = "route"` or `"target"` its target's `logit.component.events.sent` (W7b), which the target's own `Fanout` counts under the target's id |
| R_in, R_copied, R_disc, R_fwd | under `split = "target"` (W15), the router `sut_router`'s `logit.component.events.received`, `events.copied` (each delivery past an event's first), `events.discarded` (only `route_signals` counts it: an event carrying no listed signal), and `events.sent`, which counts only its forward edge, a `route`'s ordinary consumer |
| T_x[target] | under `split = "target"` (W15), a target's `logit.component.events.dropped`, a closed consumer's drop of a send T_sent already counted |
| A[agg], Ab[agg], A_sent[agg] | under `split = "target"` (W15), A, Ab, and A_sent of each sink's own `aggregate` |
| Ab | aggregate `logit.transform.metrics.absorbed` |
| L_in, L_emit, L_drop, L_err, L_flush, L_sent | with `[ledger] sut_script` (W7a), the script's `logit.component.events.received`; `logit.script.events.emitted` summed over its `outcome`s, which counts events, one per event an `emit_many` returns; `logit.component.events.discarded`, one per event `process()` returns `nil` for; `logit.component.events.dropped{reason="script_error"}`, one per event a script error loses, counted beside `logit.component.errors{reason="process"}`; `logit.component.flush.events`; and `logit.component.events.sent`, which counts what `process()` and `flush()` send alike |
| V[sink] | per sink (W7b), the reset-aware total of `vm-export/<sink>.jsonl`: add each series' first value, then every non-negative step, and on a decrease or at a SUT life start add the new value. V splits per SUT life at each series' decreases and at each life start, placing a sample by its timestamp, which is the SUT's own window time; a restart whose first value isn't below the last one shows no decrease. A series should have one segment per SUT life, else WARN |

The generator sets `max_packet_bytes: "32"`, so one datagram carries one line of about 20 bytes,
and G uses `output.messages` (what the kernel took), not `events.sent`. A `syslog_out` over UDP
sends one message per datagram, with no packing to bound, and counts both (W8b). While the SUT is down,
`statsd_out`'s per-batch name resolution fails `Clean` and nothing is sent. Over TCP (W11a)
`statsd_out` writes each batch as one frame and counts its lines in `output.messages` only once
the write completes, so a batch dropped `ambiguous_at_most_once` is counted there and never in G.

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
  residual: W − D − B − X − (L_drop + L_err) − (Ab − L_flush). From W13a the `sut_transforms`
  nodes' discards leave it the same way: W − D − B − X − ΣT_disc − Ab. A scenario has a script
  or transforms, never both, so no discard counts twice (`Ledger.discarded`). `aggregate`'s
  close-time flush delivers that residual into V, and the final drain runs after that flush, so
  `ledger.egress` judges the life's W − D − B − X − V, less those discards, as it judges the
  final life's Ab − V. The discards are subtracted in every life, whatever ended it: a killed
  life's residual, and so its kill band's lower edge, leaves them out as well. With a source's
  `fan_out` (W13b) the residual is W′ − Ab instead, and the egress gap W′ − V, W′ having
  carried the chain's discards and fan-out already (the symbol table).
  The [0, R] bound only catches a residual no shutdown could leave. With several listeners (W8b)
  the residual is summed over them, and R is the sum of each listener's own receive queue and 2
  batches of its own `batch_max_events` (its accumulator and its held batch), 65 more for each
  node of its source's `sut_transforms`, plus `aggregate`'s 65 and a script's 65 once, in
  batches of the largest `batch_max_events`. With one listener and no transforms that is the
  queue plus 67 batches, as above. With a `fan_out` (W13b) R counts W′'s events: each
  listener's share is multiplied by its chain's product of k, and `aggregate`'s and a script's
  by the largest, since a batch past a fan-out node holds k times the events.
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
  interval + rate × drain interval, where the rate is the life's W over its last drain interval
  (with a `fan_out`, W13b, each listener's W times its chain's product of k),
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

The number of lives in each SUT service's telemetry must equal 1 plus that service's own
starts on the timeline: each `revert` of a `stop` or a `kill` on it, an end-sequence early one
included, and each `apply` of a `restart` of it, with `rc` 0. A start on one SUT service doesn't
count toward another's. An unpause doesn't start a life. A life shorter than `internal`'s interval leaves at most
its shutdown uptime point, so the next life's first uptime isn't lower and the two merge; on a
mismatch every ledger, `identity.sink`, and `recovery` row SKIPs with the reason. They also SKIP when the SUT's telemetry has no
`logit.process.uptime` point.

The rows:

- `ledger.wire` is G − (W + K + F), F being 0 on UDP. On UDP this loss is uncounted **by design**: UDP on the network, netem
  drops, and the kernel receive queue at socket close
  ([`docs/known-gaps/intake.md`](../known-gaps/intake.md), "UDP intake"). It is reported,
  bucketed by drain timestamp, and fails only when the loss outside UDP-affecting windows (netem
  on a generator; any `logit` stop, kill, pause, restart, or partition) exceeds
  `wire_loss_outside_faults` times the lines sent there, beyond a tolerance of one generator
  batch per window edge. The two processes' drains are out of phase, so G is interpolated
  linearly at each SUT drain timestamp, anchored at every generator drain, one with no G point
  at the running total (W13b), since a counter exports no point for a drain it didn't move
  in; across a run of steady intervals the interpolation telescopes, and each edge of the run
  errs by at most one batch while acknowledgments arrive steadily. The anchoring applies on
  every wire and in `ledger.overlap`. On TCP and HTTP a boundary that touches a judged window,
  with a judged or a steady one on its other side, has its own rule (W13b): G's error there is
  bounded by the G that moved in the generator interval straddling it, which is many batches
  when acknowledgments bunch, under one request in flight or a restart's backlog flush, and it
  shows as an opposite-signed pair in the two runs that touch. The pair nets in full, up to that
  G, before the surplus run's allowance (below) applies to what's left, and the detail line
  shows each transfer. The cap keeps a resend's surplus, or a steady run's, from hiding more
  loss next door than interpolation explains; runs that don't touch, and a reported or end run,
  never net. `checks._boundary_nets`' doc is the full rule. A judged run's surplus allowance
  counts every edge it has, a boundary with another judged run included. Apart from that
  netting, each steady run's gap counts at 0 when W + K exceeds G in it, so a surplus in one run
  can't net out loss in another. The
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
  service that isn't a generator, such as any `logit` stop, excuses every source; a fault on a
  further SUT service (W10a) is never UDP-affecting. One detail
  line per source; the worst status wins.

  On a TCP wire (W11a) a fault is TCP-affecting when it's a lifecycle fault (`pause`, `stop`,
  `kill`, `restart`) or a `partition` of the ingress SUT or of a generator, a `mark` or an
  `exit` on the ingress SUT, or netem on the ingress SUT, whose egress carries the ACKs. Netem on
  the generator isn't: TCP retransmits what it delays or drops, so its window is judged as steady
  state. Of the TCP-affecting windows only those that can reset the connection with lines in its
  socket buffers are reported and not judged: a `stop`, `kill`, `restart`, or `exit`, and a
  `partition` whose record shows the container's address changed or doesn't show the address
  after it (`checks.wire_judgment`). Every other TCP-affecting window is **judged**: its loss
  counts with steady state's, against the same limit and edge tolerance, and its surplus, W + F
  above G, may not exceed one generator batch per `ambiguous_at_most_once` drop the generator
  counted inside it, plus one per edge. A write that failed partway leaves whole lines the SUT
  reads, and under `statsd_out`'s `at_most_once` posture the generator drops that batch and
  never counts it in G. A partition's label carries the address before and after it. The detail
  names the generator's counted loss by reason (`overflow_newest`, `ambiguous_at_most_once`)
  beside the gap; none of it is in G.

  On an HTTP wire (W14) the row judges G − (W − X), and every lifecycle fault, `partition`,
  and netem of the ingress SUT or of a generator, and a `mark` or `exit` on the ingress SUT,
  affects the wire: netem on either end can hold a request or its answer past the writer's
  timeout. A backend stop doesn't: under `overflow: block` it backs up into busy `503`s, which
  take nothing. G counts a point only once the listener answered it `204`, after its batch was
  in every inbox and W had counted it, so no window can lose a point G counted, and only a
  `kill` or an `exit` is reported (`checks.HTTP_REPORTED_ACTIONS`): either can lose up to one
  `internal` interval of the SUT's unexported W. A graceful `stop` or `restart` answers what it
  took before it exits and is judged. Every judged window's surplus, W − X above G, after the
  boundary netting above, may not exceed one generator batch per attempt the writer counted
  `logit.output.requests{class="network_error"}` inside it, plus one per edge. The bound comes
  from the writers: `influxdb_out` sends one batch per request with one request in flight
  (`Output::window` is 1; its module doc, "Destination resolution"), as `otlp_out` does for one
  signal, and after a transport error, `Ambiguous` in its response-class table, resends the
  same batch under `at_least_once`. The listener may have taken that batch before its answer was
  lost, so each such attempt leaves at most one batch in W that G never counts. A `503` is
  `Ambiguous` too, but the listener took nothing (busy) or counted what it refused in X
  (`closed_consumer`), so it allows none. A dial that failed, refused, a DNS failure, or a
  failed TLS handshake, counts `class="connect_error"`: it sent nothing, so it allows none
  either, and the detail counts it apart. A dial still pending when the request times out counts
  `network_error`, though it sent nothing ([ADR
  `sink-send-path-and-attempt-accounting`](../adr/sink-send-path-and-attempt-accounting.md)'s
  "Amendment: a failed dial counts `connect_error`"), so the allowance stays an upper bound wherever a dial can stall, as
  in `http-ingress`'s delay past the writer's timeout. A writer built before the HTTP sinks
  split that class out counts every failed dial as `network_error`, looser still across a SUT
  stop.

  On a file wire (W11b) only a lifecycle fault or an `exit` of the ingress SUT affects the wire.
  A tailer reads behind its writer, so a drain interval's gap is lag, not loss: every run of
  drain intervals is reported, and the row judges the run's W + F − G against a **replay band**
  summed over the ingress SUT's lives (`Ledger.replay_band`):
  - a killed life: above, rate × `checkpoint_interval` (the lines read after the last
    checkpoint, which the restart reads again) plus one 64 KiB read's lines for each of two
    files, the writer's active file and the one it rotated away inside that interval (a fully
    read file replays nothing, and rotations tens of seconds apart leave at most one inside an
    interval of seconds); below, rate × the life's drain interval, the counts a SIGKILL never
    exports. The rate is the life's `input.lines` over its last drain interval, the line size
    the shared directory's bytes over its lines, from the files that survive the writer's
    retention. A killed life with one drain has no drain
    interval, and its last checkpoint can be the previous life's, so its allowance above is that
    drain's W, all of which can replay, plus the same reads, and that W below, about one drain
    interval of reads a checkpoint after the drain committed and the kill never exported;
  - a life a stop, restart, or `exit` ended: up to one line above per file open at its last
    drain (`logit.input.files.open`), the unterminated line shutdown emits and the restart reads
    again as its remainder (docs/known-gaps/tailing.md, "`tail_in` splits a line held
    unterminated at a clean stop into two events"); nothing replays, since shutdown writes a
    final checkpoint;
  - the final life adds nothing: the end sequence waits until the tailers have read what the
    writer wrote (§5).

  While the writer deleted no file, G must also equal the lines the driver counted in the
  shared directory at the end (`shared_counted`), else the row FAILs its precondition. From
  W18, `file_out` deletes `.{max_files − 1}` at each rotation, so a writer deleted
  max(0, rotations − (max_files − 1)) files, its `logit.output.file.rotations` read against
  its config's `rotate.max_files`. Once it deleted one, G stands alone: the count must not
  exceed G, and the directory must hold min(rotations + 1, max_files) files, else the
  precondition FAILs. G less the count is reported beside the lines the deleted files'
  `max_bytes` would hold, never judged. A deleted file the tailer hadn't read is lost and
  counted only as far as the last checkpoint knew (docs/known-gaps/tailing.md), so a
  **retention guard** FAILs the row for any pause, stop, kill, restart, or `exit` of the
  ingress SUT, widened by one of the writer's drain intervals on each side, during which the
  writer rotated as often as its limit, which trips one rotation before the first unread
  deletion:
  - a stop, kill, restart, or `exit`: `max(1, max_files − 2)`. The process closes every file,
    so a file rotated to `.1` shortly before the fault, which a tailer one file behind hasn't
    finished, is lost when it's deleted, at the fault's `max_files − 1`th rotation;
  - a pause: `max_files`. The paused tailer keeps its descriptors, so a file it had open stays
    readable after a deletion, and only a file the writer created after the pause began is
    lost: created at the pause's first rotation, it's deleted at its `max_files + 1`th.

  `ledger.summary`'s end-to-end bound
  runs for a file wire too, with the band's upper edge added to its limit, since a replayed line
  is absorbed and stored again. From W13a it counts the path's discards with the counted loss:
  a replayed line a node discards is counted again as its read counts in W again, so the band
  still bounds the surplus.
- `ledger.intake`: W − D == E + B, for the final life, per listener from W8b, one detail line
  each, a listener off its balance first. A TCP listener's F never reached W, so it's
  `ledger.wire`'s, and `drops` leaves it out. On an HTTP wire (W14) W − D == E + B holds by
  construction, and every life of `influxdb_in` a kill didn't end must also show its `ok` and
  `closed_consumer` write requests summing to its `batches.sent`, one request one batch, and
  its `busy` ones equal to its `batches.deferred`, one line per life, a failed identity first;
  a killed life is reported unjudged, and a listener whose kind the SUT config doesn't give is
  reported. Under `split = "target"` (W15) the residual ends at the router, and a stopped
  earlier life's must be 0; see "The target split (W15)".
- `ledger.edge`: E == A, a single-consumer edge, for the final life; from W8b ΣE == A, E summed
  over the listeners, which every source's events reach. A source's `sut_transforms` (W8b) are
  judged hop by hop: its listener's E == T_in[first node], T_in − T_disc == T_out at every node
  (from W13a; each sends every event it receives that it doesn't discard), T_out[node] ==
  T_in[next node], and the last node's T_out takes that listener's E's place in the sum. The
  detail shows each node's T_disc. From W13b a `fan_out` node of k is held instead to
  children == k × T_in − T_lines, with no tolerance, its children being T_out − (T_in − T_disc),
  the count [ADR `transform-fan-out`](../adr/transform-fan-out.md)'s "Telemetry stays as it
  is" gives; with one, `ledger.summary`'s path term becomes the sum of these chain terms and
  Σ(sent into aggregate) − A, since a fan-out doesn't telescope. With `sut_script` (W7a),
  L_sent == A, the edge from the script to `aggregate`. From W7b, each sink's edge too: under
  `copy`, A_sent == S_in[sink], since a fan-out counts one send per batch; under `route`,
  T_sent[target] == S_in[sink] for the target that feeds it. Under `split = "target"` (W15)
  the path runs into the router, whose balance is judged here too; see "The target split
  (W15)".
- `ledger.script` (W7a), with `sut_script`, else SKIP: for the final life, E == L_in (the
  listener's edge into the script), L_in == L_emit + L_drop + L_err (every event emitted,
  dropped, or failed on), and L_sent == L_emit + L_flush. Earlier lives are reported.
- `ledger.aggregate`: Ab == A, for the final life; under `split = "target"` (W15) per sink,
  each `aggregate` on its own line.
- `ledger.egress`, per life and per egress group (W7b): under `split = "copy"` each sink is a
  group with its own V[sink], since each holds all of `aggregate`'s output; under `route` the
  sinks form one group whose V is ΣV[sink], since each holds its target's share; under
  `target` (W15) each sink is a group judged from its own target, as "The target split (W15)"
  says. Per group: Ab −
  V for the final life and a killed one, and W − D − B − X − V for a stopped earlier one, the
  residual plus Ab − V, because `aggregate`'s close-time flush delivers the residual to
  VictoriaMetrics before the final drain. The detail shows both parts. With
  `sut_script` (W7a), Ab becomes Ab − L_flush, because each `flush()` event carries one metric
  whose series `vm_selector` excludes, and a stopped earlier life's gap becomes
  W − D − B − X − (L_drop + L_err) − V; with `sut_transforms` (W13a) it becomes
  W − D − B − X − ΣT_disc − V, and with a `fan_out` (W13b) W′ − V. The subtraction holds only
  while each flush event carries one metric, which the scenario's script guarantees. `internal`
  drains last, so the final drain exports the script's drops and errors on a stopped life's
  residual too, and L_drop and L_err
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
  a gap of 0. A stand-in told to reject (W9a) isn't a healthy backend: a `rejected` drop at
  the sink whose drain falls inside a `respond` step with `class = "rejected"`, from its apply
  to one `aggregate` interval plus `recovery_bound` after its revert, counts the gap and is
  named, `rejected 3 cued by c0s4, counted`, but doesn't FAIL; one outside every such window
  still FAILs. The stderr `drain complete` line's `batches_dropped` counts shutdown drops only,
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
  2-batch queue is half of it, and one that fills its queue isn't drained), `receive.utilization` under 0.05 at every UDP listener (a stream listener has no receive queue), and an ingest rate over the drain
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
  script's L_drop and L_err are shown beside them as counted. With `sut_transforms` (W13a),
  E − A becomes E − ΣT_disc − A, and ΣT_disc is shown beside it as counted. With a `fan_out`
  (W13b), which doesn't telescope, E − A becomes `ledger.edge`'s chain terms summed plus
  Σ(sent into aggregate) − A, and ΣT_disc isn't shown, since W′ carries it. From W7b there is
  one egress term per egress group, each of which must be 0 or counted on its own, so a surplus
  at one sink can't net out loss at another. When an egress term is counted, it is shown as
  counted and the row judges the rest. A `rejected` or `disk_corrupt` drop in the final life's
  group FAILs the row as it FAILs `ledger.egress`, named beside its term, unless a cued step
  explains it (W9a).
  With an HTTP source (W14) the row also runs the end-to-end bound: V over the run may not
  exceed G less every counted loss, with the HTTP listener's X left out, since its client
  resends it, plus one generator batch per transport-error attempt. With a `fan_out` (W13b)
  each source's share of that limit is carried through its chain (`Ledger.through`), since V
  counts k increments per event past the fan-out. Under `split = "target"` (W15) the terms are
  `ledger.edge`'s, each judged on its own; see "The target split (W15)".
- `ledger.sent` (W5), for an external target, else SKIP: the ledger ends at each sink's own
  telemetry; see "External targets". A Kafka target's sink is judged the same way, and so is a
  `datadog` stand-in's (W9b), with what a `respond` step cued excused; a `remote_write` or
  `influxdb` stand-in target's `ledger.sent` SKIPs (W9a, W16), because `ledger.standin` judges the sink against what the
  stand-in recorded. See "Stand-in targets".
- `ledger.standin` (W9a), for a stand-in target, else SKIP: the stand-in's record against the
  sink; see "Stand-in targets".
- `drops` (2026-10-13): every `logit.*.dropped` counter the SUT emitted that no row above
  judges. Under [ADR `telemetry-loss-vocabulary`](../adr/telemetry-loss-vocabulary.md) the name
  alone says a count is loss, so the row reads every such name rather than a list. It leaves
  out the terms the ledger balances (every `batches.dropped`, a node's `events.dropped` beside a
  batch drop of the same reason, `bytes.dropped`, a listener's D, K, F, X, and B terms, and the
  script's L_err) and, on an external, Kafka, or `datadog` stand-in target, the sink's
  `records.dropped` and
  `metrics.dropped`, which `ledger.sent` judges. A `rejected` or `disk_corrupt` reason FAILs;
  any other WARNs. On an external target `ledger.egress` SKIPs, so `ledger.sent` FAILs a
  `rejected` or `disk_corrupt` batch drop at a sink, and one at any other node stays in this
  row.
- `ledger.overlap` (W12b), under `[overlap]` only, else no row: the run's G − Σ(W + K + F)
  against what the instances' socket closes could discard. Under `[overlap]` the per-life rows
  judge each instance on its own lives and the wire, `progress`, and `recovery` judge them
  together; §8e has the rules.
- `file.reopen.<step>` (W12c), one row per `rename` step, else one `file.reopen` row that
  SKIPs: a rename moves a `stdio_out` file target's `path` to `to`, and the first SIGHUP on
  its service starting inside its window makes the target reopen. The reopen is lazy, at the
  target's next send, and a line's `timestamp` is when its listener read it, so every line in
  the renamed file is stamped by the signal's apply finishing, and every line in the new file
  (the next rename's `to`, or `path` after the last) from the signal's start, each within 2 s:
  `statsd_in` sends a batch at least every 100 ms and the file target writes it as it arrives.
  The row FAILs when the rename or that SIGHUP didn't apply, either file is missing or breaks
  its bound, an earlier rename's file holds a line from before that rename's SIGHUP, a line has
  no `timestamp`, or the lines in every file of the path's renames differ from the target's
  `events.delivered` summed over the service's lives, one line per event under
  `format: json`. A `kill` or an `exit` of the service loses its last drain's counts, so the
  count is then reported, not judged. A new file with no lines WARNs.
- `tls.served.<step>` (W12d), one row per `probe` step, else one `tls.served` row that SKIPs:
  the generation in force for the probe's certificate is the latest rotation of it that started
  before the probe and wasn't `broken`, else the first issue, because a failed reload keeps the
  previous files. The row FAILs when the probe didn't apply, its served serial or `notAfter`
  isn't that generation's, the served chain didn't verify against the run's CA, or the run has
  no `tls_issued` phase.
- `expect.<name>`, one per `[[expect]]` table: a scenario's own bound on one metric over a
  window around one step, or over the whole run (W8c); see "The scenario schema".

**The target split (W15).** A `route` with a list-valued `routes:` value sends one event whole
to several targets, and a `route_signals` under `strip: true` sends each of an event's signals
to its own target as a part of its own; `split = "route"` refuses both, since the sinks' V no
longer add up. Under `split = "target"` the router sits between the listeners and the
aggregates, one per sink: `[ledger] sut_router` names it in place of `sut_aggregate`, and each
`[[ledger.sink]]` names its `target`, its own `aggregate`, and optionally the `transforms`
between them. `load()` holds the router's targets to the sinks' targets, every one, since the
router's balance sums them all; refuses any other `route` or `route_signals`, or `lua` or
`lua_file` with `targets:`, that fills a sink's target, since a target's T_sent counts every
component that fills it; and refuses the split beside `sut_script`, a hop,
`vm_every_window`, `[overlap]`, and any target but the local one. The rows change as follows:

- The listeners' rows end at the router. `ledger.edge` judges Σ(sent into router) == R_in, and
  `ledger.intake`'s residual is W − D − B − X (less ΣT_disc) − R_in: in [0, R] for a killed
  life, and 0 for a stopped one, whose final drain runs after every data node has exited.
  Nothing else judges a stopped life's ingress, since each sink's egress starts at its target.
- The router balances as R_in + R_copied − R_disc == R_fwd + ΣT_sent, in `ledger.edge`, judged
  in the final life and in every stopped earlier one. A target's T_sent counts every
  component that fills it, which `load()` holds to the router. Under `strip: true` each part
  past an event's first counts one copy, so a log-and-metric event reads R_in 1, R_copied 1, and T_sent 1 at
  each target; an event no listed signal claims reads R_disc 1. A `route`'s unrouted event
  reads R_fwd 1. The balance pools the targets, so events sent to one target in place of
  another leave it 0, and the same lives judge two bounds beside it: each target's
  T_sent ≤ R_in − R_disc, since a target receives at most one delivery of each event the
  router kept (graph rule 51 for a `route`, one part per target for a `route_signals`), and
  R_fwd ≤ R_in. The router counts R_in and R_disc before a target's `Fanout` counts T_sent, so
  a drain can't split them the wrong way. The bounds catch a target receiving more than the
  router took in, not a shift between targets below that: `route-copy`'s `tier_a` carries
  about half of R_in.
- Each sink's branch is judged hop by hop in `ledger.edge`: T_sent − T_x == the first branch
  transform's T_in or the sink's A, every branch transform T_in − T_disc == T_out, the last
  T_out == A, and A_sent == S_in[sink]. `ledger.aggregate` judges each sink's Ab == A on a line
  of its own.
- `ledger.egress` judges each sink alone against its own Ab. A stopped earlier life's gap is
  T_sent − T_x − ΣT_disc over its branch − V, the branch residual plus Ab − V, and a killed
  life's band takes its ingest from T_sent and its window from the sink's `aggregate`. A drop
  on another sink's branch, at its transforms, `aggregate`, or sink, doesn't count a gap.
- `ledger.summary` lists W − D − E − B, every `ledger.edge` term, each A − Ab, and each Ab − V,
  and judges each on its own: two terms of opposite sign never net to 0, and one sink's surplus
  never covers another's loss downstream of its target. The router's term pools the targets,
  which only its bounds in `ledger.edge` partly cover. From a file or HTTP wire, each sink's
  V over the run is bounded by G less counted loss, since a target receives at most one part of
  each event.

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
  was accepted, so both the batch and the record counter are read. On a `datadog` stand-in
  target (W9b), a drop inside a `respond` step that explains it is counted as cued instead: a
  `rejected` batch inside a step with `class = "rejected"`, an `oversize` record drop inside a
  `413` step classed `rejected`, and a `rejected` record drop inside another step classed
  `rejected` or a `series_errors` step, each through one aggregate interval and `recovery_bound` after the step. Outside every
  matching step it FAILs. See "Stand-in targets".
- On an external target, a batch dropped `disk_corrupt` (2026-10-13): FAIL, because
  `ledger.egress`, which FAILs it on a local or Kafka target, SKIPs there.
- An `api_key_rejected` or `request_refused` line from the sink on stderr: FAIL, quoting the
  first. The sink reads that answer as `Refused` and holds its queue with no exit, so the run
  can't deliver until the key or the endpoint is fixed. On a `datadog` stand-in target (W9b), a
  line inside a step with `class = "refused"`, through the same widened window, is counted as
  cued instead. See "Stand-in targets".
- No `logit.output.requests` point, or none of class `2xx`: FAIL.
- Any other counted drop (a batch's `overflow_*` or `shutdown` reason, a record's `stale`, `too_many_tags`, or other pre-send reason, the
  encoder's `metrics.skipped`, and its `metrics.dropped` for an encode fault): WARN.
- A life whose stderr `drain complete` `batches_dropped` differs from its telemetry
  `batches.dropped{reason="shutdown"}`: WARN.
- A request of class other than `2xx` (`4xx` holds a `429` too, `5xx`, `network_error`) in a
  drain whose interval overlaps a fault window is the fault's retry; outside every one it
  WARNs with the count, because the destination has a floor of errors of its own.

On an external target, the row is a PASS whose detail starts with `SENT` when nothing above
fires; on a Kafka or stand-in target the PASS doesn't, because the ledger goes on to the topic
or the record. `SENT` is a PASS
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
`checks.py`'s `EPOCH_BUMP_REASONS`), as [`docs/deploying.md`](../deploying.md#kafka_out-producing-to-kafka)
permits. Every other target's `ledger.topic` SKIPs.

**An `otlp_proto` topic.** `load()` reads the `format:` of the `kafka_out` the sink names
from the SUT config, `json` when it sets none, and records it as the resolved target's `format`.
It refuses a format other than `json` or `otlp_proto`, and a sink that doesn't write the
target's `topic`: its `topic:`, or under `otlp_proto` with none, the `metrics:` topic of
`topics:`, else `otlp_metrics`. The consumer prints a value as text, so under `otlp_proto` it
reads through `--value-deserializer org.apache.kafka.common.serialization.BytesDeserializer`,
whose `Bytes.toString` writes each printable ASCII byte but the backslash as itself and every
other byte as `\xHH`, one record per line. `otlp.py` unescapes each line and walks it as an
`ExportMetricsServiceRequest`, as prost reads protobuf: unknown fields are skipped, a scalar
or a oneof keeps its last value, and a truncated field, an over-long varint, or a known field
under the wrong wire type is a record that doesn't decode. Each record holds every series of a
batch, so T and the duplicate copies stay counted on raw bytes, the unit of
`logit.output.records`, but V counts each `Sum` data point once per series (the resource, the
metric name, and the attributes) and `time_unix_nano`. A point a byte-distinct record repeats,
a resend split another way, adds nothing to V, and `ledger.topic` judges it per life as it
judges a duplicate copy. Each record is credited to the life its latest point time falls in.
The walker is checked against `tools/soak/vectors/kafka-otlp-metrics.hex`, a record of int,
double, and negative `Sum` points, a `Gauge`, and a histogram with packed fields, which a test
in `crates/logit-outputs/src/kafka/encode.rs` holds equal to what the encoder writes; the
self-test derives its variants from those bytes. `format: otlp_json` isn't read.

**The scenario.** `kafka-broker-restart` is `statsd-vm`'s generator at 2,000 lines/s through a
2 s delta `aggregate` into `kafka_out` (`format: json`, `acks: all`, idempotent, `timeout:
10s`), topic `soak`. Its 6.5-minute cycle restarts the broker, pauses it for 40 s, and stops it
for 60 s, each followed by at least 75 s. The 10 s `timeout`, under the pause, turns the pause
into response timeouts, `Ambiguous` attempts the broker deduplicates on the resend. Its
expectations: `batches.dropped` 0 through each fault, at least one `ambiguous` request through
the pause, so `ledger.topic`'s no-duplicate verdict rests on resends, and `retrying` above 0
during the stop and 0 after it. `kafka-otlp-broker-restart` is the same run under `format:
otlp_proto`, `topics: {metrics: soak}`, with the same expectations: one record per window.

### 8c. Stand-in targets (W9a, W9b, W16)

A `[target]` table with `kind = "standin"` runs a scripted HTTP destination in place of
VictoriaMetrics, so every row of a sink's response-class table
([ADR `sink-fault-classes`](../adr/sink-fault-classes.md)) runs under load, on cue:

```toml
[target]
kind = "standin"          # name defaults to "standin"
shape = "remote_write"    # or "datadog" (W9b) or "influxdb" (W16); "remote_write" by default

[[ledger.sink]]           # one sink, with no backend or vm_selector
id = "remote_write"
```

**httpbin, considered.** `httpbin`'s `/status/{code}` answers a status fixed by the URL, so
changing the answer mid-run would mean changing the sink's endpoint and restarting the SUT; it
keeps no record of what it received, can't store a write and then answer `5xx`, can't reset a
connection, and decodes no request `Content-Encoding`. What it would contribute, "answer status
N", is a line of `http.server`, and it would bring Flask and gunicorn into a harness that is
stdlib-only.

**The stand-in.** `tools/soak/standin/standin.py` is about 300 lines of stdlib Python run in a
stock `python:3.13.16-alpine3.24` image, so `script/soak` builds nothing for it. Its data port
takes the sink's writes on the shape's path (`--shape remote_write`: `/api/v1/write`, default
answer `200`; `--shape datadog`, W9b: `/api/v2/series`, default answer `202`; `--shape
influxdb`, W16: `/api/v2/write`, default answer `204` with no body and no `Content-Length`); a
write to any other path answers `404`. The path is compared without its query string, because
`influxdb_out` always sends one (`org`, `orgID`, `bucket`, `precision=ns`); compared whole, every
write would answer `404`, which a carried-mode sink reads as `Rejected`, a silent drop. Under `datadog`, a cue's `series_errors` makes a `2xx` answer's body
the series route's `errors` list, protobuf field 1 (`repeated string`), with N entries; the
stand-in decodes nothing, so the entries name series by their place in the request. Its control port takes the driver's
cues and answers `/health`. It prints one JSON line per request to stdout: the time, the path and the
query string apart, `Content-Encoding`, the cue in force (none once a counted cue is spent), how it answered (`status` or `reset`), whether it stored
the write (`apply`, else whether the status is `2xx`), the cue's `series_errors` when it set one,
and the raw body, base64. The driver's
log capture collects that stdout with every other service's, so a `stop` and `start` of the
stand-in keep one record, and `check` re-scores a run from it. SIGTERM lets each request in
flight finish before the process exits, so a `stop` doesn't cut off an answer the record holds.

**The schema.** The target takes an optional `name` (a slug) and `shape` (`remote_write`,
`datadog`, or `influxdb`), and refuses `env` and `topic`; any other target refuses `shape`. It
needs one `[[ledger.sink]]`, and only one, with no `backend` or `vm_selector`.
`vm_every_window` is allowed, since V has an export's shape.
The stack runs the `standin` profile in place of `local`, and the role map must hold the
`standin` service. Faults on `standin` are known only for this target. A `respond` step (§2)
sets the answer.

**The driver.** It writes the target's `shape` to `compose.env` as `SOAK_STANDIN_SHAPE`, which
compose.yaml passes to `--shape`. It reads the stand-in's published control port after `up` and
waits for its `/health`, re-reads it after any revert of a fault on it, and POSTs each `respond` step's cue.
Like a Kafka target's, the end sequence waits for the sink's queue to empty. It reads nothing
back after the SUT stops: the record is in `logs/standin.stdout`.

**V.** `soaklib/standin.py` decodes each stored request's body on the host. Under
`remote_write` it's a Snappy block (a stdlib decoder, pinned against blocks `snap::raw` wrote, in
`tools/soak/standin/snap-vectors.json`) holding a remote-write 1.0 `WriteRequest`. V is every
stored sample, with a later write of one `(label set, timestamp)` replacing an earlier one, as a
remote-write receiver overwrites. The result has the export's shape, so `ledger.egress`,
`ledger.summary`, and, under `vm_every_window`, `ledger.windows` read it as they read
VictoriaMetrics. Python's stdlib has no zstd before 3.14, so a `remote_write` sink sends Snappy.

Under `datadog` (W9b) a body is a `MetricPayload`
(`crates/logit-proto/proto/datadog/agent-payload/proto/metrics/agent_payload.proto`) under
`datadog_out`'s default `gzip`, or `deflate` or none, decoded with the stdlib and a walker pinned
against a body `datadog_out` sent (`tools/soak/standin/datadog-vectors.json`). The SUT's
`aggregate` runs `temporality: delta`, so each point is one window's increment, and V is their
sum, credited to the SUT life its timestamp falls in. Datadog documents no deduplication of a
`count` point, and [ADR `delivery-semantics`](../adr/delivery-semantics.md) says a kind with no
remedy is double-counted, so a resent point adds rather than overwrites. As for a Kafka topic, V
counts each distinct copy of a `(series, timestamp, value)` once, so a duplicate can't cancel a
loss of the same size, and a negative gap is a `surplus` that FAILs `ledger.egress`; a second
value at one `(series, timestamp)` is summed and FAILs `ledger.standin`. A `series_errors`
answer stores every series of its request but the first N, and the sink's `records.dropped`
counts toward a positive gap, because those series are lost with no batch dropped.

Under `influxdb` (W16) a body is InfluxDB 2.x line protocol, uncompressed, read at the query's
`precision` by a stdlib parser of `influxdb_out`'s escaping
(`crates/logit-proto/src/influxdb/encode.rs`, "Escaping") pinned against a body the sink sent and
hand-built escape cases (`tools/soak/standin/influxdb-vectors.json`). Each numeric or boolean field
of a point is a sample of `{__name__: measurement, field: key, **tags}`; a string field has no
number and isn't in V. The SUT's `aggregate` runs `temporality: delta`. InfluxDB overwrites an
identical `(measurement, tag set, field, timestamp)`, and `influxdb_out`'s retry re-encodes a batch
byte for byte (its module doc's "Delivery posture"), so a resend is a rewrite, not a duplicate: V is
the sum over distinct `(series, timestamp)` of the stored value, credited to the SUT life its
timestamp falls in, and a negative gap is a `surplus`, as under `datadog`. A different value at the
same key FAILs `ledger.standin`, since the sink's resend can't produce one.

**`ledger.sent` against a `datadog` stand-in (W9b)** judges the sink by the verdicts in
"External targets", so they run without a real org; that list says which refusals and drops a
`respond` step's cue excuses. `drops` leaves the sink's `records.dropped` and `metrics.dropped`
to it, as on an external target. Under `remote_write` and `influxdb`, `ledger.sent` SKIPs.

**`ledger.standin`** judges the record against the sink:

- Every record line parses, and every body decodes. A resend that stored a different value for
  a sample already stored FAILs; one that stored the same value is counted and reported.
- Every sample of a write the stand-in answered `2xx` was stored. `ledger.egress` counts any
  positive gap in a life with a batch drop, so on a stand-in run with `rejected` steps a black
  hole passes it; this rule, not `ledger.egress`, is what catches one there.
- Per SUT life, the record's requests by status class equal the sink's
  `logit.output.requests{class}` for `2xx` to `5xx`; its resets are at most the sink's
  `network_error`. A failed dial, `connect_error`, never reaches the record. A killed life's
  mismatch WARNs.
- Every answer other than the default carries the cue of a `respond` step that applied
  cleanly and falls inside its span, and every write went to the stand-in's path.
- Each step with a `class` shows that class at the sink. `rejected`: a batch dropped `rejected`
  from the step's apply to two `internal` intervals after its revert, and no `retrying` line in
  the span. `clean`, `refused`, and `ambiguous`: the `retrying` line's class word, `send failed
  (<class>): ...`, the only one in the span, and no drop; under `buffer.delivery: at_most_once`,
  `ambiguous` drops the batch `ambiguous_at_most_once` instead, with no `retrying` line. The
  write loop logs `retrying` on a held head's first failure and then once a minute, so a step's
  span holds its first line. The row reads the stderr line rather than an `[[expect]]` because
  the class appears nowhere in telemetry: `requests{class}` buckets the status, and `retrying`
  is the same gauge for every held class.
- Under `datadog` (W9b), also: per SUT life, the sink's `records.dropped{reason="rejected"}`
  equals the series the `series_errors` answers named plus the points of the requests answered
  under the cue of a step classed `rejected`, other than a `413`, and
  `records.dropped{reason="oversize"}` the points of the `413` answers under such a step. The
  expected class comes from the step alone; the harness keeps no copy of the sink's table to
  check it against, so a step classed wrong FAILs its signature at the sink.
  Each duplicate copy is reported per life with the amount the destination counted twice:
  permitted under `at_least_once`, and a FAIL under `at_most_once`, or when the copy it repeats
  was answered `2xx` in the same life, a point the sink saw accepted and resent.
- Under `influxdb` (W16), also: per SUT life, no point of a request answered under the cue of a step
  classed `rejected` is stored, and the egress gap (Ab − V, plus a stopped earlier life's residual
  as `ledger.egress` adds it) equals those points' sum, with no allowance; a killed life's is
  reported. A cued rejection's credit lets `ledger.egress` count any positive gap in its life, so
  this rule is what catches a loss beside one. The row reports the distinct `(org, orgID, bucket)`
  the writes' queries name and FAILs on more than one. A point stored again with the same value,
  such as each resend after a `503` with `apply = true`, is reported as a rewrite, and FAILs only
  under `at_most_once`, or when the write it repeats was answered `2xx` in the same life. As under
  `datadog`, every `respond` other than a `2xx` needs a `class`.

**The scenarios.**

- `standin-remote-write`: `statsd-vm`'s generator and SUT, with `prometheus_out` sending
  remote-write 1.0 under `compression: snappy` to the stand-in. Twenty 20 s steps, 105 s apart,
  walk the sink's sender-mode table: `503`, a `503` after storing the write (its resend stores
  the same samples again), `429`, `400` with `out of order sample`, `400` with each
  decompression prefix, `401`, `403`, `407`, `404`, `405`, `501`, `409`, `413`, `415`, `422`,
  `302`, a reset, a `stop` of the stand-in, and a `408`, each with its `class`. Under cumulative
  temporality the next window's totals overwrite a rejected batch, so the egress gap stays 0.
- `standin-at-most-once`: the same SUT with `buffer.delivery: at_most_once`; a `503` and a reset
  drop `ambiguous_at_most_once`, a `401` holds, a `400` drops `rejected`.
- `standin-datadog` (W9b): `statsd-datadog`'s generator and SUT, with `datadog_out` sending
  series under `compression: gzip` to `endpoints: { api: http://standin:8080 }`. Sixteen 20 s
  steps, 105 s apart: a `202` with `series_errors = 2`, then `400`, `401`, `403`, `404`, `405`,
  `408`, `413`, `429`, `500`, `501`, a `503` after storing the write (its resend a duplicate),
  `302`, `422`, a reset, and a `stop` of the stand-in, each but the first with its `class`.
  Under delta temporality a rejected window is a counted gap.
- `standin-influxdb` (W16): `standin-remote-write`'s generator and statsd-vm's SUT under delta
  temporality, with `influxdb_out` writing to `url: http://standin:8080` with `bucket:` and
  `org:` configured. Nineteen 20 s steps, 105 s apart, walk the sink's table: `204`, `400` with
  a parse error and with `Please provide either orgID or org`, `401` with InfluxDB 2's body and
  InfluxDB 3's, `403`, `404` naming the bucket, the org by name, an orgID, and neither, `413`,
  `422`, `429`, `500`, a `503` after storing the write (its resend a rewrite), `408` for "any
  other status", `302`, a reset, and a `stop` of the stand-in, each but the `204` with its
  `class`. With no value carried, every `403` and `404` is `refused`. The table's timeout row
  isn't cued: the sink's 10 s request timeout isn't configurable, and a stand-in that held its
  answer that long would stall the SUT for longer than a step.
- `standin-influxdb-carried` (W16): the same steps with no `bucket:`, `org:`, or `default_*`,
  and a `set` after `aggregate` stamping `_logit.influxdb.bucket`, `_logit.influxdb.org`, and
  `_logit.influxdb.org_id` on the resource, as `influxdb_in` would from a write's query. Every
  `403` and `404` is `rejected`; an `[[expect]]` holds `metrics.dropped{reason="no_bucket"}` at
  0 over the run.
- `standin-influxdb-mixed` (W16): the bucket carried by the `set` and the org from
  `default_org:`, so a `403`, a `404` naming the bucket, and a `404` naming neither are
  `rejected`, and a `404` naming the org or an orgID, the operator's, is `refused`.

The black-hole control is a `2xx` the stand-in doesn't store (`status = 200`, `apply = false`).
`ledger.standin` FAILs it wherever it falls. Under cumulative temporality the next stored totals
overwrite a hole, so `ledger.egress` FAILs it as uncounted loss only when it holds the last
samples and no drop in the life credits the gap (a self-test fixture), and a hole in the middle
shows in `ledger.windows` under `vm_every_window` (a recorded by-hand run). It isn't a shipped
scenario.

### 8d. The native hop (W10b)

A scenario with two SUT services names the hop between them:

```toml
[[ledger.hop]]
from = "logit"            # the SUT holding the logit_out: the ingress SUT
sink = "hop"              # its logit_out component id
to = "collector"          # the SUT holding the logit_in: the further SUT [configs] names
listener = "hop"          # its logit_in component id
```

**The schema.** At most one `[[ledger.hop]]`, with the four keys and no other, `sink` and
`listener` component ids, `from` `logit`, and `to` the one further SUT service `[configs]`
names. Two SUT services with no hop and no `[overlap]` (§8e) are refused, and so is a hop
beside `[ledger] sut_script` or a source's `sut_transforms`, whose rows judge a path into
`aggregate` inside one process.

**The driver.** Before it stops `logit`, the end sequence waits on the hop sink alone: its
`buffer.batches` 0, and `retrying` and `logit.output.in_flight` 0 or never set, so everything it
took has been acknowledged before `logit` stops. `collector` then stops as the last SUT does.

**The symbols.** On `logit`'s lives, the hop sink's S_hop
(`logit.component.events.received{component=<sink>}`), its counted drops
(`logit.component.events.dropped`, every reason), and `logit.output.batches.resumed` and
`logit.output.reconnects`. On `collector`'s lives, the `logit_in`'s E_B
(`logit.component.events.sent{component=<listener>}`, the events of the frames it forwarded, and
none of a resend it acknowledged under a mark), X_B (its `events.dropped`), and
`logit.input.batches.resends`. E_B − X_B stands in for W − D − B − X on the far side: it feeds
`aggregate` there. A frame's events are at most the largest events-over-batches of any hop sink
drain, and a send window is the `logit_out`'s `window` (32 by default) frames of that many.

**The rows.** Each side's rows run on its own lives; nothing pairs a life of one with a life of
the other.

- `ledger.wire` and `ledger.intake`'s balance, `logit`'s final life, as for one SUT.
- `ledger.intake`'s earlier lives: `logit`'s W − D − B − X − S_hop in [0, R], since its hop
  sink's inbox stands where `aggregate`'s would; `collector`'s E_B − X_B − Ab in [0, R_hop], 65
  frames of the largest frame, `aggregate`'s inbox and its taken batch.
- `ledger.edge`: ΣE == S_hop in `logit`'s final life, and E_B == A and each sink's edge in
  `collector`'s.
- `ledger.aggregate`, `ledger.egress`, `ledger.windows`, and `ledger.replay` for the sinks:
  `collector`'s lives, with E_B − X_B − V for a stopped earlier life and the kill band's rate
  read from E_B.
- **`ledger.hop`**, on run totals: gap = ΣS_hop − Σ hop drops − ΣE_B. 0 passes. Each killed
  `logit` life allows − its last drain interval of S_hop, which its telemetry may not have
  counted though its spool forwards it, and + one frame: the hop sink counts a batch received
  before it spools it, and a spooled batch survives a SIGKILL, so only the batch in hand can be
  counted and lost. If `collector` restarted between that life's last delivering drain and the
  next `logit` life's first, it also allows − the frames the killed life committed after its
  last cursor persist (its commit rate, `_commit_rate`'s highest delivered rate over its last
  three delivering drain intervals, × `checkpoint_interval`, plus one, in frames of the largest
  frame): its spool replays them under their old identity, and the restarted `collector` has no
  mark for them ([`docs/deploying.md`](../deploying.md#forwarding-between-logit-nodes)); a `collector` that kept its marks
  acknowledges the replay as resends instead. Each killed `collector` life allows + its last
  drain interval of E_B, which it forwarded but never exported, and − one send window of
  frames: its marks are gone, so it forwards a resend of what it had forwarded and not
  acknowledged. Each `pause` of `collector` allows − one send window: the resend can race the
  frames an ended connection still holds
  ([`docs/deploying.md`](../deploying.md#forwarding-between-logit-nodes)). A partition or packet loss
  allows nothing: the race needs a forward parked on a full inbox. Any other gap FAILs, below
  the allowance as a surplus and above it as uncounted. A `rejected` or `disk_corrupt` batch
  drop at the hop sink FAILs too. Resends, resumed frames, and reconnects are shown beside it,
  and the row's lines give the running gap at each fault's start, the hop's backlog plus any
  surplus so far, which can't be split by fault.
- `ledger.summary`: W − D − E − B and E − S_hop in `logit`'s final life, E_B − A, A − Ab, and
  each egress term in `collector`'s, with `ledger.hop` left to judge the hop. Beside them, one
  bound end to end, which no kill or restart can loosen: each egress group's V over the whole
  run is at most G less every counted loss (K, D, B, X, the hop's drops, and X_B, and on a TCP
  wire F), plus one generator batch per `ambiguous_at_most_once` drop of a TCP source, the
  partial-frame surplus `ledger.wire` allows. A duplicate
  forwarded across the hop adds the same increments to E_B, A, Ab, and V, so every per-life term
  balances around it; this bound and `ledger.hop` are what see it, the bound only once the
  surplus exceeds the run's uncounted loss, the wire loss among it.
- `ledger.replay` for the hop sink, for each killed `logit` life followed by another. Unlike a
  sink behind `aggregate`, which gains one batch a window, the hop sink is fed by the listener,
  so its queue keeps growing between the last drain and the kill, and `collector` may take some
  of it. Its spool commits a frame once `collector` acknowledges it and persists the cursor on
  a commit once `checkpoint_interval` (1 s by default) has passed since the last persist, so up
  to one interval of commits stays unpersisted and replays, even after `collector` stopped and
  commits stopped with it. With the received and delivered batch rates over the life's last
  drain interval, its commit rate (the highest delivered rate over its last three drain
  intervals that delivered anything, since delivery that stopped partway through an interval
  averages low over it), and
  t from its last drain to the `docker kill`'s return, replayed − queued is in
  [−(delivered × t) − 1, received × t + the commit rate × the checkpoint interval + 1].
  The first two `native-hop` runs found both terms: a kill inside `collector`'s stop replayed 55
  batches over the queue, 50 it received in the 5 s before the kill and 5 committed before the
  stop and never persisted.
- `identity.sink` judges the hop sink too, at `logit`'s final life's last quiet drain;
  `recovery` judges each side's sinks on its own drains, the hop sink against the listeners'
  datagrams and `collector`'s sinks against E_B; `drops` keys every term on its service and
  leaves out the hop sink's counted drops, which `ledger.hop` reads, and X_B.

**The scenario.** `native-hop` (17 minutes: warmup 60 s, `recovery_bound` 60 s, a 14-minute
cycle, cooldown 120 s) runs `logit` as `statsd_in → logit_out` (`window: 32`, `request_timeout:
10s`, `buffer.disk`) and `collector` as `logit_in → aggregate` (10 s, cumulative,
`series_retention: 360`) `→ prometheus_out` (remote-write 1.0, zstd, `buffer.disk`). Its steps:
a netem delay on `logit` (200 ms ± 50 ms, 120 s) and then 10% loss (90 s), a 45 s partition of
`collector`, a 30 s stop, a 30 s pause, and a 30 s kill of `collector`, a 30 s kill of `logit`
with `collector` up, and a 30 s kill of `logit` inside a 90 s stop of `collector`. Its
`[[expect]]` tables assert the replay after the first `logit` kill is deduplicated
(`logit.input.batches.resends` above 0), `logit` reconnects after the `collector` kill, and the
spool holds the hop's frames through the stop.

### 8e. Rolling overlap (W12b)

A scenario can run two or more instances of one SUT on one UDP port, as a rolling upgrade
overlaps them ([ADR `listener-port-sharing-and-shutdown-delay`](../adr/listener-port-sharing-and-shutdown-delay.md),
"What an overlap means for the data"):

```toml
compose = "compose.override.yaml"   # adds sut-net (soak.role netns) and logit-b (soak.role sut)

[overlap]
holder = "sut-net"                  # owns the network namespace the instances join
instances = ["logit", "logit-b"]    # each runs the whole pipeline; logit first
label = "soak_instance"             # the attribute each instance's `set` stamps, a label on V
```

**The stack.** A SUT that owned the namespace would take the address with it when it stopped,
so the holder, a service with the role `netns`, owns it and runs nothing: the netem image,
`sleep infinity`, `stop_signal: SIGKILL`. Each instance has `network_mode: "service:<holder>"`
and the generators send to the holder's name. The instances share an address, so each serves its
admin endpoint on a Unix socket and sets `LOGIT_ADMIN` to it, which the image's `HEALTHCHECK`
and the driver's readiness probe, `logit ready` with no `--admin`, both read. A generator needs
several flows for the kernel to spread them: a UDP sink sends from one socket, so one sink is one
flow, and `generator_input` and `generator_sink` may both list components, as many of each, each
input feeding the sink at its position; their counts sum. The wire's edge tolerance is the inputs'
batches summed, since each sends a batch in about the same tick, and the end allowance weights
each sink's last queue plus one by its own input's batch.

**The schema.** `[overlap]` has the three keys and no other; `instances` lists two or more
distinct services, `logit` first, none the holder; `label` is a Prometheus label name. With the
config: no `[[ledger.hop]]`; every source on a `udp` wire; a `[configs]` key other than `sut` and
the generators only for an instance, which then runs its own config, else `sut`'s; and in each
instance's config `reuse_port: true` on every source's listener, a `shutdown.delay` above 0 that
with a 10 s drain (the listeners' 5 s `shutdown_grace` and `internal`'s 5 s second stage) is
under `docker stop -t 30`, `admin.socket` with no `admin.bind`, and a `set` stamping `label`
that every source's `sut_transforms` lists. Two instance configs may differ only in the lines
that set `label`. With the steps: only `netem` and `partition` on the holder, neither on an
instance, and no two of `pause`, `stop`, `kill`, `restart`, or `exit` on different instances
overlapping. With the role map: the holder is the stack's one `netns` service, a `netns` service
needs an `[overlap]` or, from W19, an `idp` ("A token endpoint in the SUT's namespace"), and
each instance's `network_mode` names the holder (read from `docker compose config` beside the
roles).

**The faults.** A netem on the holder shapes every instance's egress, and a partition of it cuts
their ingress too. Every instance is an ingress SUT, so a lifecycle fault on any of them is
UDP-affecting.

**The driver.** The end sequence stops the generators, waits once until every sink's stored
total holds, which covers every instance, and then stops each instance.

**The rows.** A row about a process life (`ledger.intake`, `ledger.edge`, `ledger.script`,
`ledger.aggregate`, `ledger.egress`, `ledger.windows`, `ledger.replay`, `identity.sink`,
`drops`, and `ledger.summary`'s per-life terms) judges each instance on its own lives, V being
the series whose `label` names it, with one detail per instance and the worst status. A series
whose `label` names no instance FAILs `ledger.egress`. The flows move between the instances, so
the rest judge them together:

- `ledger.wire`: G against W + K + F summed over the instances, each instance's running total
  interpolated at every instance's drains, since they drain out of phase. An instance's stop is
  UDP-affecting, so its window is reported.
- `ledger.overlap`, on run totals: G − Σ(W + K + F) ≤ Σ over the instances' stops and restarts
  of `ceil(used / 512) + 1` datagrams, `used` being the closing life's last
  `logit.input.receive_buffer.used.bytes`, which the listener samples once more after its read
  loop stops, and 512 B the smallest truesize the kernel charges a small datagram. The bound is
  loose by up to a quarter of `SO_RCVBUF`, since `udp_rmem_release` frees a socket's charge in
  batches. Below 0 it allows the generators' end allowance. Each stop's own gap is reported, not
  judged: interpolating the out-of-phase drains errs by about a generator batch at each edge. A
  kill or an `exit` of an instance closes with no final sample, and a partition, a netem, or a
  lifecycle fault elsewhere that can lose UDP adds loss the closes don't explain, so a run with
  either is reported and not judged. A pause of an instance loses only what its kernel counts in
  K.
- `ledger.summary` adds the wire over the run and a bound end to end: each egress group's V
  summed over the instances is at most G less the instances' counted loss, from W13a their
  `sut_transforms` nodes' discards included.
- `progress`: a fault that holds an instance opens no window, since the others carry its flows;
  each window is judged on the deliveries summed over the instances no such fault holds in it,
  and skipped when every instance is held.
- `recovery`: each instance's sinks and receive queue on its own drains, so only while it runs,
  against the ingest summed over the instances.

**The scenario.** `rolling-overlap` (19 minutes: warmup 60 s, a 16-minute cycle, cooldown
120 s) runs `statsd_in` (`reuse_port`) → `set` → `aggregate` (10 s, cumulative) →
`prometheus_out` (remote-write 1.0, zstd, `buffer.disk`) in both instances, `shutdown.delay:
5s`, one config with `soak_instance: !env SOAK_INSTANCE`, and a generator of eight
`generate_in` inputs of 250 lines a second, each into its own `statsd_out`. Its steps stop
`logit` for 60 s, then `logit-b`, delay the holder's egress (200 ms ± 50 ms, 120 s), stop
`logit` inside a 90 s stop of VictoriaMetrics, restart `logit-b`, and stop it again. No step
partitions the holder or drops packets, whose loss `ledger.overlap` couldn't tell from a close's.
Its `[[expect]]` tables assert that each instance reads while the other is stopped and that
neither counts a kernel drop.

### 8f. A token endpoint in the SUT's namespace (W19)

`kafka_out` takes an OAUTHBEARER `token_endpoint` over plain HTTP on loopback only, and over
HTTPS only with a certificate from a public CA, so a token server in the stack must share the
SUT's network namespace. `tools/kafka-interop` puts the SUT in the token server's namespace,
which a soak can't: a stop of the server would take the SUT's network with it.

**The stack.** An override adds a namespace holder with the role `netns`, as in "Rolling
overlap", and a token server with the role `idp`, and puts `logit` and the `idp` in the holder
with `network_mode: "service:<holder>"`. The SUT reaches the `idp` at `127.0.0.1`. Every other
service reaches either at the holder's name: the generator sends there, and the broker fetches
the JWKS there. The SUT keeps its own container, so its restarts, RSS, descriptors, and logs
resolve as for any SUT.

**The schema.** With no `[overlap]`, the role map holds one `netns` service only beside an
`idp`, and an `idp` only beside one, with no SUT but `logit`; an `idp` and `[overlap]` can't be
combined. `logit`'s and each `idp`'s `network_mode` names the holder. The holder takes `netem`
and `partition` only, and `logit` and the `idp` take neither, as for an overlap: a netem or a
partition of a member is one of the holder, and a lifecycle fault of the holder takes every
member's network away.

**The faults.** A netem or a partition of the holder is one of the SUT's network, ingress and
egress, as on `logit` itself in any other stack; the `idp`'s loopback traffic isn't shaped. A
lifecycle fault on the `idp` affects egress only: the sink holds its queue while it can't
authenticate.

**The rows.** `self_log` counts a `token_refresh_failed` warning inside a fault window on an
`idp`, `recovery_bound` included, and WARNs one outside every such window. A failed refresh that
keeps a valid token loses nothing, so it has no `*.dropped` counter, and none is added: it is
counted in `logit.component.diagnostics{key="token_refresh_failed"}`
([ADR `kafka-sasl`](../adr/kafka-sasl.md)'s fallback amendment), which an `[[expect]]` reads like
any other counter.

### 9. Not in this stack

toxiproxy and TCP-level faults it alone can express; per-peer `tc filter` and `ifb` ingress
shaping until a scenario needs them (W2 at the earliest); a checker container; any CI job.

### 10. Next scenarios

Every shipped scenario drives UDP datagrams into one `aggregate`: `statsd_in`, and from W8b in
`fanin` `lines_in` beside it, on §3's base stack plus any services its `compose` override adds;
from W11a `tcp-ingress` drives TCP into `statsd_in` instead, from W11b `tail-rotate` tails
files, from W14 `http-ingress` drives HTTP into `influxdb_in`, and from W13b `otlp-fan-out`
drives HTTP into `otlp_in` and through `line_protocol`.
`[ledger]` names one `[[ledger.source]]` per generator and its listener, and one
`[[ledger.sink]]` per sink after `aggregate`. The scenarios below widen
that, grouped by cost; the Workstreams table gives the landing order. Each states what it
proves, its faults, its rows, and the harness support it needs; §2's validation, §5's end
sequence, and §6's checks hold unless an item says otherwise.

**Cheap now.** Each is a scenario directory on the base stack, plus an override's services
where it needs them, and a small check extension.

1. **Lua in the path (built in W7a).** `statsd_in → lua → aggregate → prometheus_out`, which
   drives `run_lua` and `/readyz`'s `stalled`, the one not-ready answer for a node that hasn't
   exited ([`docs/deploying.md`](../deploying.md#what-to-watch-on-readyz)). Three scenarios
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

   W12a adds each under LuaJIT's trace compiler ([ADR `lua-jit-opt-in`](../adr/lua-jit-opt-in.md)),
   with no harness change: `lua-path-jit` rewrites in place
   ([ADR `lua-in-place-edits`](../adr/lua-in-place-edits.md)), with two attribute writes, a
   `pairs` loop over the attribute proxy, and an `= nil` delete, and no log message to write,
   since a statsd event carries none. The script fails an event whose `pairs` loop misses a
   written attribute or whose deleted attribute still reads back, and its `[[expect]]` rows pin
   the script's failures in one step to every 97th event's share, so either fault FAILs them; `lua-stall-jit` and `lua-memory-jit` keep their verdicts.
   The compiler adds no count hook, so a compiled loop is caught only by stall detection, as an
   interpreted one is, and the kept strings are on the Lua heap, which `max_memory` counts with
   the compiler on; only compiled machine code is outside it.
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
   ([`docs/design/memory.md`](../design/memory.md#4-interning-the-bargain-and-its-bounds)), so
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
   identities out and marks back". Faults on A or the link judge zero surplus, and so does a
   graceful `stop` or `restart` of B: `logit_in` writes the accepted `Ack` for every frame it
   forwarded before it answers `GOING_AWAY`, so nothing it forwarded is resent. Only a `kill` of
   B, which forgets its marks, or a `pause` long enough to end A's connection, whose frames a
   resend then races, can forward a frame twice, and the send window bounds that surplus; and a
   kill of A replays the frames its spool committed after its last cursor persist, which a B
   restarted since, gracefully or not, forwards again.
   Rows: the ledger one hop longer, `ledger.replay` on A. Built in W10a (a second SUT service,
   §2's "Several SUT services") and W10b (the hop ledger and the `native-hop` scenario, §8d).
   Every shipped SUT config set `span_sample_rate: 0.0` until W12a, so neither internal span
   emission nor the hop frame's trace context
   ([ADR `native-hop-trace-context`](../adr/native-hop-trace-context.md)) ran under faults.
   W12a adds `native-hop-spans`, `native-hop` at the product's default rate, `0.1`, on both
   SUTs. The spans reach each SUT's NDJSON stdout as events with no `metrics`, which
   `telemetry.py` skips, so the ledger reads the same points; `[[expect]]` rows on
   `logit.internal.spans.emitted` show both sides record spans, and `drops` judges a span lost
   from `internal`'s buffer as it judges any other drop. No row yet checks that the hop carries
   the trace: a future row will join `collector`'s `logit_in send` parent span ids to the span
   ids `logit` exported, with a miss bound covering `logit`'s counted
   `logit.internal.spans.dropped{reason="buffer_full"}` and a killed life's last drain interval
   of spans.
8. **TCP ingress (built in W11a).** `statsd_in` with `transport: tcp` from the generator's
   `statsd_out` over TCP; a `syslog_in` TCP source and a TCP reset counter are deferred. A
   stalled SUT backs up into the sender
   ([ADR `syslog-tcp-ingress-and-tls`](../adr/syslog-tcp-ingress-and-tls.md), "No receive queue on
   TCP -- the connection itself is the backpressure"): the SUT's kernel keeps ACKing until its
   receive buffer fills, then the generator's write makes no progress for `connect_timeout`.
   A first write that took nothing fails `Clean` and is retried; a write that took part of a
   frame and then stalls fails `Ambiguous`
   (`crates/logit-outputs/src/stream.rs`'s module table), and under `statsd_out`'s
   `at_most_once` posture drops the batch, counted `ambiguous_at_most_once`. Meanwhile its queue
   overflows, counted `overflow_newest`. A write that failed partway leaves whole lines the SUT reads and G never counted, so a pause is counted
   overflow and ambiguous drops plus a surplus of up to one batch per ambiguous drop. A 45 s
   partition produces no RST: with no send stall the connection survives and delivers; with one,
   the generator counts an ambiguous drop, and the abandoned socket's retransmits deliver what it
   held if the SUT kept its address. Uncounted loss can come only from an address change, or from
   a stop or a kill, which can reset the connection with lines in its socket buffers. Rows:
   `ledger.wire` judged at 0 outside those reported windows, with the surplus allowance in the
   judged ones (§6), and `[[expect]]` on the generator's overflow and ambiguous drops around the
   pause and on idle closes ([ADR `idle-connection-timeout`](../adr/idle-connection-timeout.md))
   around a generator pause past `idle_timeout`. Needs: a wire per source with its own symbols
   (the product already counts per line, `input.frames`), per-wire fault attribution, and a
   partition's addresses. Built as `tcp-ingress`, with an override capping each end's TCP
   buffers so a 30 s pause stalls a write (see "Findings").
9. **File tailing (built in W11b).** `tail_in` behind a rotating writer, with `pause`, `stop`,
   `restart`, and `kill` on the SUT. Proves checkpoint resume
   ([ADR `file-tailing-and-docker-json-logs`](../adr/file-tailing-and-docker-json-logs.md),
   "Checkpoints: optional, written on an interval, only when dirty"). The writer is a
   `generator` service, `generate_in → file_out` with `format: json`. `file_out` rotates by
   renaming the active file to `app.log.rotating`, shifting each `app.log.N` to `N + 1`, removing
   `app.log.{max_files − 1}`, and renaming the staged file to `app.log.1`; `max_files` counts the
   active file. The scenario keeps every file (`max_files: 100`, about 74 minutes of files), so
   nothing unread is deleted and the harness can count every line at the end. Its
   `max_duration` of 25 minutes holds that, and holds the descriptor growth below inside
   `fd_growth`. `file_out` counts no `output.messages`, so G is
   its `logit.component.events.delivered` (§6's file wire), held to the lines the driver counts
   across the shared directory, a G from outside both processes. The SUT tails
   `paths: ["/shared/app.log*"]`, a glob that matches the rotated names, so a renamed file is
   rebound by inode and read to its end; it matches `app.log.rotating` too, which is harmless,
   since that is the same inode. It runs `read_from: beginning`, a `checkpoint_path` in its own
   filesystem (`/var/lib/logit`, which a stop and a kill keep), and `checkpoint_interval: 5s`,
   then `json` and `fanin`'s count chain into `aggregate` and a `prometheus_out` with
   `buffer.disk:`. W is `logit.input.lines`, which the product already counts per line. Both
   mount `<run>/shared` (§3). A replay is at-least-once (ADR `delivery-semantics`, "10. A
   replaying input is at-least-once up to the in-memory queues"): a checkpoint covers only lines
   already in a downstream inbox, so a kill replays what was read after the last checkpoint. A
   clean stop doesn't read to the end of its files: shutdown emits each file's held unterminated
   line, flushes, and writes a final checkpoint, so nothing replays but the split line, which a
   64 KiB read ending mid-line holds as often as a writer caught mid-line does. The replay's
   surplus is in W, so it belongs on `ledger.wire` and on `ledger.summary`'s end-to-end bound,
   not on the killed life's egress band, which §6 bounds from `aggregate`'s window and stays as
   written, with its rate read from `input.lines`; §6's `ledger.wire` row has the band. A file
   wire's fault windows are the SUT's lifecycle faults only. `logit.input.files.rotated` counts
   every renamed name a scan finds, so one rotation of `file_out`'s cascade counts once per file
   it renamed: an `[[expect]]` can ask only that it's above 0. Rows: `ledger.wire` with the band,
   and `[[expect]]` on `logit.input.files.rotated` above 0, `logit.input.files.resume_rejected`
   at 0, and `logit.input.checkpoint.errors` at 0. A tailer keeps every file its pattern still
   matches open at its end, so with every file kept its descriptors grow by about one per
   rotation, and the scenario's `fd_growth` allows for it. Built as `tail-rotate`: 1,000 lines a
   second rotating about every 44 s, a 60 s pause, a 30 s stop, a restart, a 30 s kill, and a
   150 s kill across about three rotations.
10. **Deterministic destination errors (built in W9a, for `prometheus_out`, and W9b, for
    `datadog_out`).** A scripted HTTP stand-in answering `429`, `5xx`, `400`,
    and `413` on cue and recording what it accepts, so every row of a sink's response-class table
    ([ADR `sink-fault-classes`](../adr/sink-fault-classes.md), "Each sink attributes from
    everything its destination gives it") runs under load, and a Datadog-shaped one makes
    `ledger.sent` testable without a real org. It's a destination, not a proxy, so the non-goal
    in "Context" holds; toxiproxy (§9) adds TCP-level faults, not statuses. Rows: `ledger.egress`
    against its record, `[[expect]]` per class. Needs: the stand-in, a cue the driver sets per
    step, and V read from its record. This is the one tooling investment the roadmap recommends.

**Operational.**

11. **Rolling overlap (built in W12b).** Two SUTs on `reuse_port` with `shutdown.delay`, swapped
    mid-run ([ADR `listener-port-sharing-and-shutdown-delay`](../adr/listener-port-sharing-and-shutdown-delay.md),
    "What an overlap means for the data"). Rows: zero uncounted loss in each instance's own
    lives; the run's wire loss within what the instances' socket closes could discard, each close's
    last `receive_buffer.used.bytes` converted to datagrams at a floor of 512 B each and judged on
    run totals (`ledger.overlap`). Built with an `[overlap]` table in place of a hop, a
    namespace holder with the role `netns` that the instances join, because a SUT owning the
    namespace takes the port with it when it stops; an `admin.socket` on each instance, reached
    through `LOGIT_ADMIN`; list-valued `generator_input` and `generator_sink`, because
    `reuse_port` hashes a flow to one socket and one `statsd_out` sink is one flow; and a `set`
    stamping each instance's name, so V splits by instance, because two processes writing one
    cumulative series overwrite each other. Built as `rolling-overlap` (§8e).
12. **SIGHUP and TLS reload.** A TLS listener and sink, signalled and rotated during faults
    ([ADR `tls-certificate-reload`](../adr/tls-certificate-reload.md), "Trigger: a content poll,
    and SIGHUP"), in two halves:
    - **SIGHUP and reopen (W12c).** The harness's telemetry stream is a stdout `stdio_out`,
      which ignores SIGHUP, so it's expected to run unaffected. A separate row covers the
      reopen with a SUT `stdio_out` writing to a file target (`/shared/archive.log`,
      `format: json`), whose path a rename moves aside before the signal and whose next batch
      lands in a fresh file. Not `file_out`: `FileOut.path`'s doc leaves its file to
      `rotate:` and points a file renamed from outside at `stdio_out`, since `file_out`'s own
      `.1` retention collides with an external rename. Built as the `signal` and `rename`
      actions, `file.reopen`, and the `sighup` scenario.
    - **TLS reload (built in W12d).** A private CA per run, a TLS listener and sink, and
      rotations during faults. The SUT runs `tls_reload_interval: 0s`, so every reload comes
      from a SIGHUP and is attributable to one. The certificates mount as a directory, because a
      single-file bind mount keeps the old inode and hides a rotation. Rows: `[[expect]]` on
      `logit.tls.reloads{outcome="reloaded"}` and `logit.tls.certificate.not_after`, which is
      absolute and so needs the `rise` reducer, relative to its value before the step;
      `tls.served`, matching a probe's served serial to the generation in force; and the ledger
      at zero across reconnects. No leg to VictoriaMetrics over TLS: its open-source build
      doesn't verify client certificates. Built as the `[tls]` table, the `rotate` and `probe`
      actions, and the `tls-reload` scenario, whose generator reloads a rotated client
      certificate on its poll and reconnects cleanly after a SUT restart, and whose SUT
      reloads a rotated server certificate on each SIGHUP and keeps the previous one through a
      broken rotation.

**Later additions.** Items 13 to 18 cover what merged after the items above were written, on
2026-10-09.

13. **Fan-out and discarding transforms in the ledger (built in W13a and W13b).**
    `line_protocol` (63fd8238) emits one event per point through `Transform::process_into`, so
    its `events.sent` can exceed its `events.received`, and `drop_stale` and `has_stale`
    (e3a2649b) discard. Before W13a, `ledger.edge` required T_in == T_out at every
    `sut_transforms` node, so neither could be in a scenario's path.
    - **Discards (built in W13a).** The runtime counts `logit.component.events.discarded` for
      every Transform node in `process_batch`, so the ledger reads each node's T_disc with no
      per-node key, and `ledger.edge` judges T_in − T_disc == T_out with no tolerance. A
      discard is counted loss, so the edge alone isn't enough: the residual, a stopped life's
      egress gap, a killed life's band, `ledger.summary`'s path term, and the end-to-end bound
      each take ΣT_disc out, as they take a script's discards (§6). Built with the
      `tail-rotate-stale` scenario: `tail-rotate` with `timestamp` and `drop_stale`
      (`max_age: 50s`) after `json`, and its 30 s stop lengthened to 90 s. `tail-rotate`'s 30 s
      stop is too short: `max_age` must clear what a 30 s kill replays, lines about 40 s old,
      so no 30 s fault can leave a stale line. Only the lines older than `max_age` when the
      restarted tailer reads them are discarded, about 40 s of the stop's backlog, not every
      line read after the restart. `[[expect]]` tables bound `drop_stale`'s `events.discarded`
      around each fault.
    - **Fan-out (built in W13b).** The product adds no counter:
      [ADR `transform-fan-out`](../adr/transform-fan-out.md), "Telemetry stays as it is", gives
      a node's children as `events.sent − (events.received − events.discarded)`, and the
      harness owns that arithmetic. A source's `fan_out = { points = 4 }` judges that node's
      children with no tolerance against k × parents − the lines it dropped,
      `logit.transform.lines.dropped{reason="malformed"}`, and every row after it reads the
      listener's counts carried through the chain, W′ (§6).
    - **The generator.** Points of one message with the same measurement, tag set, and
      timestamp merge into one event (`crates/logit-transforms/src/line_protocol.rs`'s module
      doc), so each of a message's k points carries its own tag value, and none merge.
      `syslog_out` can't carry such a message: it escapes every newline in MSG as `\n` on every
      transport (`crates/logit-outputs/src/syslog.rs`, "Injection safety"), and `line_protocol`
      reads that as one line. So the generator puts k lines in one OTLP log body, a `|-`
      block in its `generate_in` template, which keeps the newlines as literal text, and sends
      it with `otlp_out` over HTTP into `otlp_in`, which carries the body unchanged, on item
      14's `http` wire with `otlp_in`'s rows. `syslog_in`'s
      `framing: datagram` (fdd5b039) stays unsoaked until a sender can write a multi-line
      datagram.
    - **Topology.** `otlp_in → line_protocol → count chain → aggregate → prometheus_out`.
    - **Faults.** `statsd-vm`'s schedule, plus a backend stop under `overflow: block` with
      `rss_slope` watched: an inbox's bound counts batches, not events, and each batch past
      `line_protocol` holds k times the events.
    - Rows: `ledger.edge` with the fan-out term, then the rows as written, each reading W′.
      Built as `otlp-fan-out`, k = 4.
14. **HTTP ingress, starting with `influxdb_in` (built in W14).** Generator `influxdb_out` →
    SUT `influxdb_in` → `kv_metrics` → `aggregate` → `prometheus_out`. Proves that a listener
    answering `503` when busy loses nothing it doesn't count, across the SUT's own faults. A
    `wire = "http"` source has its own symbols (§6): G the writer's `events.delivered`, the
    points the listener answered `204` for; W per point, from the listener's `events.sent`,
    since `influxdb_in` counts requests, not points; X, a batch refused because no consumer was
    open, taken off W, since the client resends it. A `503` and
    `logit.input.batches.deferred` are a retry the producer makes, not loss, and a client's
    resend of a request the SUT took before the answer was lost is a surplus, allowed one
    batch per transport-error attempt, as a TCP window's is per ambiguous drop. `otlp_in` has
    rows of its own in `checks.HTTP_LISTENERS`, with W per log record (one `Event` per
    `LogRecord`) and no request identity, for W13b's generator, which `otlp-fan-out` runs.
    Built as `http-ingress`: each point carries a string field alone, so the SUT decodes no
    gauge, which V can't count, and `kv_metrics` counts one per point. Faults: netem delaying
    the SUT's egress past `influxdb_out`'s 10 s timeout, which carries the answers, and
    dropping 30% of it; 10% loss on the generator's; a backend stop under `overflow: block`
    with a 500 ms window, which backs up into `503`s; and a SUT `stop`, `restart`, and `kill`.
    Rows: `ledger.wire` with the surplus allowance, `ledger.intake` with `influxdb_in`'s request
    identities, `ledger.summary`'s end-to-end bound, and `[[expect]]` on deferred batches and
    the generator's `5xx` answers during the backend stop and its transport errors under the
    delay. `datadog_in`, `splunk_hec_in`, and `prometheus_in`'s remote-write receiver come
    later.
15. **Multi-destination `route` and `route_signals` (81b0befa, 4b86ccc7), built in W15.**
    `scenario.py`'s `_route_problems` refuses a list-valued `routes:` and any `route_signals`
    under `split = "route"`, because one event can reach several sinks and their V no longer
    add up. The third split mode, `split = "target"`, takes each sink's expected V from its
    target's own `events.sent`, with the router balanced as received + copied − discarded
    (`logit.component.events.copied`, `events.discarded`); §6's "The target split (W15)" has
    the rules. Scenario: `generate_in` sending mixed
    log and metric events → `route_signals` → two targets, each into its own `aggregate` and
    backend, with one backend stopped. Rows: `ledger.edge` through the router, and
    `ledger.egress` per sink from its target. Needs: the split mode, a per-target `aggregate`
    in `[[ledger.sink]]`, and self-test fixtures. Built as `route-signals`: each syslog line
    gets a `soak_lines` increment from `kv_metrics` ahead of the router, so it carries a log
    and a metric, and the log branch's own `kv_metrics` gives each log part a
    `soak_log_lines` increment, the one count V can read on that branch with the scenario's
    two VictoriaMetrics; the SUT is also stopped and killed, for the stopped-life and
    killed-life rules. A list-valued `route` is a second scenario, `route-copy`, which reuses
    `route-split`'s generator and sends each shard-0 line to both targets, so `events.copied`
    moves under a `route` too.
16. **Kafka SASL re-authentication over hours (5da5ab8c), built in W17 for SCRAM.**
    `kafka-sasl-reauth` is `kafka-broker-restart` over SCRAM-SHA-512 to a broker whose SASL
    listener sets `connections.max.reauth.ms` to 30 s. Each session's end retires a connection
    (`crates/logit-outputs/src/kafka/conn.rs`'s module doc), so the run redials at a steady
    rate, which `kafka_out` counts in `logit.output.reconnects` from W17 on and a `rate`
    expectation bounds. Rows: `ledger.topic`, `fd_slope`, duplicates per life, and
    `[[expect]]` on the redial rate, the `ambiguous` rate, `refused` attempts, and dropped
    batches. The two halves after SCRAM:
    - **OAUTHBEARER with short-lived tokens (built in W19).** `kafka-oauth-reauth` is
      `kafka-sasl-reauth` over the client-credentials grant, with `tools/kafka-interop`'s
      `mock-oauth2-server` in the stack under the `idp` role, in a namespace holder it shares
      with `logit` ("A token endpoint in the SUT's namespace"). Each token lives 60 s and the
      broker's session ends when it expires, so the sink fetches a token and redials about
      every 50 s. Beside the broker's faults, the endpoint is stopped and paused for 2 minutes
      each, longer than a token lives. Rows: W17's, and `[[expect]]` on the
      `token_refresh_failed` diagnostic inside the stop, which shows the fallback to the cached
      token, `clean` attempts and `retrying` inside both endpoint faults, and `retrying`
      clearing after each. Two corrections to the item as first planned:
      - The expired-token phase is `Clean`, not `Refused`. An endpoint that refuses the
        connection or times out is `Clean`
        (`crates/logit-outputs/src/kafka/sasl/oauth.rs`'s module doc), and the sink never
        sends a token it knows has expired, so the broker refuses nothing, and the run asserts
        no `refused` attempt.
      - The fallback covers a refused endpoint, not a hung one. A redial comes `timeout` before
        its token expires and fetches the next token then, since nothing fetches on a timer.
        Against a hung endpoint, the fetch waits that same `timeout` and almost always returns
        with the token expired. The sink's expiry, the request's start plus `expires_in`, can
        sit up to a second past the JWT's whole-second `exp` that sets the deadline, so a fetch
        can still fail with under a second left and fall back. That is rare, so the pause step
        asserts the hold alone.
    - **An `otlp_proto` topic (2969c24e), built in W20.** A stdlib protobuf walker reads each
      record's metrics export request, and V counts each `Sum` data point once per series and
      time. Built as `kafka-otlp-broker-restart`; §8b's "An `otlp_proto` topic" has the rules.
17. **`influxdb_out`'s response-class table on a stand-in (ffe33654, b93cb23a), built in W16.**
    W9's stand-in design with InfluxDB's shape, cueing the `403` and `404` rows that split on
    whether the bucket or org was carried or configured: `Rejected` for a carried value,
    `Refused` for the operator's (`crates/logit-outputs/src/influxdb.rs`'s module doc). Three
    runs: one with the sink's `bucket:` and `org:` set, one carrying the bucket, org, and orgID
    from a `set` after `aggregate` in place of `influxdb_in`, so the SUT needs no HTTP generator,
    and one carrying the bucket beside `default_org:`. Rows: `ledger.standin` and `[[expect]]`
    per class.
18. **`tail_in` descriptors over hours under a deleting rotation (built in W18).** `tail-rotate`
    with `max_files: 8`, run for hours. A matched rotated file stays open until the writer
    deletes it ([`docs/known-gaps/tailing.md`](../known-gaps/tailing.md)), so the open count
    should stay at about `max_files`. Built as `tail-rotate-delete`, with these corrections to
    the item as first written:
    - `fd_slope` isn't a slope: it compares the warmup median with the end. The pass is
      `fd_growth` of 10: the files the writer keeps plus a deleted one draining and the
      staged `app.log.rotating`, eight more than the warmup's two, plus two to spare, with `[[expect]]`
      maxima on `logit.input.files.open` and `logit.process.fds` over the whole run, a last
      `files.open` of 7 or more, and eight or more writer rotations, so it deleted.
    - The end count isn't replaced: it still gives the replay band its line size, and it
      cross-checks G as an equality while the writer deleted no file and as a bound with the file
      count after (§6's `ledger.wire` row), beside a retention guard for a SUT down long enough
      to lose an unread file.
    - The end sequence's wait needed G: against the count it returned at once once the writer
      had deleted a file (§5).
    - A short repeating cycle hides a leak, since a restart resets one, so the cycle is an hour
      with about 50 minutes of one quiet life in it, run for 4 hours with no `max_duration`.
    - An inode a deletion freed while the SUT was down can be reused by the writer's next file,
      and the restart then counts `resume_rejected` correctly, so the scenario drops
      `tail-rotate`'s expectation of none.

Two follow-ups have no item yet. An overlap of TCP listeners, item 11 over `transport: tcp`,
will need `net.ipv4.tcp_migrate_req`, without which the connections still in the old socket's
accept queue at its close are reset rather than moved to the new one. A CA rotation, item 12's
TLS half carried further, will rotate to a bundle of the old and new CAs, then to a leaf the new
CA signed.

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
| W9a | `soak/w9a` | `soak/w9a: a scripted HTTP stand-in for every remote-write response class` | M | W8c |
| W9b | `soak/w9b` | `soak/w9b: a Datadog-shaped stand-in and ledger.sent against it` | M | W9a |
| W10a | `soak/w10a` | `soak/w10a: several SUT services, each scored on its own lives` | M | W9b |
| W10b | `soak/w10b` | `soak/w10b: a native-hop soak between two logit processes` | M | W10a |
| W11a | `soak/w11a` | `soak/w11a: a TCP-ingress soak, with a wire per source` | M | W10b |
| W11b | `soak/w11b` | `soak/w11b: a file-tailing soak, with a shared volume and the replay band` | M | W11a |
| W12a | `soak/w12a` | `soak/w12a: plan for W13 onward, a tail_in fd gap, and span and Lua-JIT soak variants` | S | W11b |
| W12b | `soak/w12b` | `soak/w12b: a rolling-overlap soak, a namespace holder, and per-instance totals` | M | W12a |
| W12c | `soak/w12c` | `soak/w12c: a SIGHUP soak, with a signal action and a stdio_out reopen check` | M | W12b |
| W12d | `soak/w12d` | `soak/w12d: a TLS-reload soak, with a private CA and a rotate action` | M | W12c |
| W13a | `soak/w13a` | `soak/w13a: discarding transforms in the soak ledger, with a drop_stale tail soak` | M | W12d |
| W13b | `soak/w13b` | `soak/w13b: fan-out transforms in the soak ledger, with a line_protocol soak` | M | W14, W13a |
| W14 | `soak/w14` | `soak/w14: an HTTP-ingress soak into influxdb_in, with an http wire for it and otlp_in` | M | W12d |
| W15 | `soak/w15` | `soak/w15: multi-destination route and route_signals soaks, with a target split` | M | W13b |
| W16 | `soak/w16` | `soak/w16: an InfluxDB-shaped stand-in for every influxdb_out response class` | M | W12d |
| W17 | `soak/w17` | `soak/w17: a Kafka SASL re-authentication soak` | M | W16 |
| W18 | `soak/w18` | `soak/w18: a tail_in descriptor soak under a deleting rotation` | M | W14 |
| W19 | `soak/w19` | `soak/w19: a Kafka OAUTHBEARER soak with short-lived tokens` | M | W17 |
| W20 | `soak/w20` | `soak/w20: an otlp_proto Kafka topic in the soak ledger` | M | W17 |

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
  verification [ADR `buffered-sink-delivery`](../adr/buffered-sink-delivery.md) calls for (a 90 s
  stop under `block`, then `drop_oldest` with a small `max_batches`), and `udp-flood-sink-stop` and
  `udp-flood-sink-stop-block`, [ADR `decoupled-listener-io`](../adr/decoupled-listener-io.md)'s
  deferred soak. A run of each is recorded under "Findings". No scenario needed per-peer `tc filter`.
- **W3**: disk-spool `kill -9` replay, the manual soak
  [ADR `disk-backed-sink-buffer`](../adr/disk-backed-sink-buffer.md) calls for: a `kill` action
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
- **W9a**: item 10 for `prometheus_out`: the stand-in, the `respond` action and the `class` key,
  the `standin` target, role, and profile, V from the stand-in's record, the `ledger.standin`
  row, the cued `rejected` excusal, and the `standin-remote-write` and `standin-at-most-once`
  scenarios ("Stand-in targets").
- **W9b**: item 10 for `datadog_out`: a Datadog-shaped stand-in (`[target] shape = "datadog"`,
  series v2, a `202` whose body names rejected series through `series_errors`), V as the stored
  delta points' sum, `ledger.sent` and `drops` against it with cued refusals and drops excused,
  duplicates reported per life as for a Kafka target, and the `standin-datadog` scenario
  ("Stand-in targets").
- **W10a**: the first half of item 7, a second SUT service: further SUT services declared by
  their own `[configs]` keys (§2's "Several SUT services"), a config variable each, the end
  sequence stopping each in chain order, each service's lives, kills, and stderr read on their
  own, the rows from `aggregate` on judged on the lives of the service holding it, UDP
  attribution on the ingress SUT alone, and self-test fixtures for each. No scenario runs two
  SUTs yet.
- **W10b**: the rest of item 7, the ledger one hop longer (§8d): `[[ledger.hop]]` and its
  rules, the end sequence waiting on the hop sink, each side's rows on its own lives, the
  `ledger.hop` row, the end-to-end bound in `ledger.summary`, the hop sink's replay band,
  identity, and recovery, `drops` keyed on service and component, the `native-hop` scenario,
  and a run of it under "Findings".
- **W11a**: item 8: `[[ledger.source]] wire` (`udp`, `tcp`, or `file`) cross-checked at load
  against the listener's kind and `transport:`, a symbol table per wire (§6; the `file` row's
  symbols included), each hardcoded `logit.input.datagrams` read through the wire, fault
  attribution per wire with the TCP windows judged or reported, the partition's addresses
  before and after, self-test fixtures for each rule, and the `tcp-ingress` scenario with its
  `[[expect]]` rows and a run under "Findings".
- **W11b**: item 9: the `file` wire's scenario, `tail-rotate`, a shared directory the writer
  and the SUT mount, the harness's own line count across `app.log*`, and the replay band on
  `ledger.wire` and the end-to-end bound.
- **W12a**: §10's items 13 to 18 and these rows; the `docs/known-gaps/tailing.md` entry for the
  descriptors a matching glob keeps open; and four variants with no harness change:
  `native-hop-spans` (item 7 with internal spans at the default rate) and `lua-path-jit`, `lua-stall-jit`,
  and `lua-memory-jit` (item 1 under `jit: true`), each run recorded under "Findings".
- **W12b**: item 11, the namespace-holder service and per-instance totals.
- **W12c**: item 12's SIGHUP half, the `signal` action and the `stdio_out` reopen row.
- **W12d**: item 12's TLS half: a private CA per run, the `[tls]` table, the `rotate` and `probe`
  actions, `tls.served`, the `rise` reducer, the broken-rotation excusal in `self_log`, and the
  `tls-reload` scenario.
- **W13a**: item 13's discards: T_disc per `sut_transforms` node in `ledger.edge`, and ΣT_disc
  as counted loss in the residual, the egress gap, `ledger.summary`, and the end-to-end bound,
  with self-test fixtures and the `tail-rotate-stale` scenario.
- **W13b**: item 13's fan-out: a source's `fan_out`, its term in `ledger.edge`, W′ in every row
  after it, `ledger.wire`'s G anchored at every generator drain and its boundary rule between
  two judged windows, self-test fixtures for each rule, and the `otlp-fan-out` scenario. Its
  generator sends over OTLP/HTTP, so it lands after W14's `http` wire and its `otlp_in` rows.
- **W14**: item 14, the `http` wire, with rows for `influxdb_in`, which its scenario runs, and
  for `otlp_in`, which W13b's generator needs; the generator sink's kind checked at load, the
  wire's fault predicates and judgments, `influxdb_in`'s request identities in
  `ledger.intake`, the end-to-end bound in `ledger.summary`, self-test fixtures for each rule,
  and the `http-ingress` scenario.
- **W15**: item 15, a third `split` mode, `target`, for multi-destination `route` and
  `route_signals`: `[ledger] sut_router`, each sink's own `aggregate` and branch `transforms`,
  the router's balance, the per-sink rows, self-test fixtures for each rule, and the
  `route-signals` and `route-copy` scenarios, each run recorded under "Findings".
- **W16**: item 17: the stand-in's `influxdb` shape (the query split off the path, a `204`
  with no body), a stdlib line-protocol parser and its vectors, V as one stored point per
  series and timestamp, `ledger.standin`'s rules for the shape, the class required on its
  `respond` steps, and the `standin-influxdb`, `standin-influxdb-carried`, and
  `standin-influxdb-mixed` scenarios, each run recorded under "Findings".
- **W17**: item 16's SCRAM half: `kafka_out` counts `logit.output.reconnects`, `[[expect]]`
  gains the `rate` reducer, and the `kafka-sasl-reauth` scenario, whose compose override starts
  the broker from its own `kafka-server.properties` with a SCRAM user, with a run recorded under
  "Findings".
- **W18**: item 18: the writer's deletions in `ledger.wire` (the end count held to G only
  while the writer deleted no file, then as a bound with the writer's file count) and its
  retention guard, the end sequence's wait for G, self-test fixtures for each rule, and the
  `tail-rotate-delete` scenario, a 20-minute and a 1-hour run recorded under "Findings".
- **W19**: item 16's OAUTHBEARER half: the `idp` role and a `netns` holder outside
  `[overlap]` ("A token endpoint in the SUT's namespace"), with their rules in `scenario.py`,
  `faults.py`, and `self_log`, self-test fixtures for each, and the `kafka-oauth-reauth`
  scenario, a 20-minute and a 1-hour run recorded under "Findings".
- **W20**: item 16's `otlp_proto` half: the target's format read from the sink's config, the
  consumer's `BytesDeserializer`, a stdlib protobuf walker (`otlp.py`) checked against a vector
  the Rust encoder pins, V per data point and the repeated points `ledger.topic` judges,
  self-test fixtures for each rule, and the `kafka-otlp-broker-restart` scenario, a 20-minute
  and a 1-hour run recorded under "Findings".

§10's follow-ups to items 11 and 12 have no workstream yet.

Landing order: W0 → W1a → W1b → W2 → W3 → W4 → W5 → W6 → W7a → W7b → W8a → W8b → W8c → W9a → W9b → W10a → W10b → W11a → W11b → W12a → W12b → W12c → W12d → W14 → W13b → W15, linear; W13a, W16, and W17 land off `main` beside W14, and W18, W19, and W20 off `main` after it. Each PR is based on and targets its
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

### W9a: a scripted stand-in for every remote-write response class (2026-10-15)

W9a adds the stand-in target, the `respond` action and `class` key, `ledger.standin`, and the
cued `rejected` excusal, with `standin-remote-write` and `standin-at-most-once`. The runs used
`logit:soak` `sha256:ac527cd4206d`, built from `847f57c8` (`main` at `8ea44976` plus W9a, which
changes no Rust), `logit-soak-netem:local` `sha256:2396ab48f179`, and
`python:3.13.16-alpine3.24` `sha256:2d9aefe2fef0`, on the development host (32 cores, Docker
29.8.2). `standin-remote-write` ran at `df6a9f95`, which spaces its steps 105 s apart; the other
two ran at `847f57c8`, beside a first `standin-remote-write` at 95 s spacing (`20261015T160441Z`),
whose every row also passed, `progress` judging only 6% of the run.

| Run | Scenario | Verdict |
|---|---|---|
| `20261015T163904Z` | `standin-remote-write` | every row PASS: each of the 19 steps read as its class, 10 batches dropped `rejected` (2 per rejected step), all cued, and Ab − V 0 |
| `20261015T160610Z` | `standin-at-most-once` | every row PASS: the `503` and the reset each dropped 2 batches `ambiguous_at_most_once`, the `401` held, the `400` dropped 2 `rejected` |
| `20261015T160615Z` | `standin-black-hole`, by hand | FAIL, as the control should: `ledger.windows` finds every series' 30 s hole; `ledger.egress` passes |

`standin-remote-write`'s `results.md` table:

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 38 apply/revert action(s) returned 0; 0 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 2 exit(s) of the logit services, each a scheduled stop with code 0 |
| `restarts` | PASS | 1 start(s), each a scheduled start or restart |
| `self_log` | PASS | 23 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 180 readiness sample(s) read ok outside fault windows; 0 start(s) or unpause(s) ready in time |
| `progress` | PASS | 23 30s window(s) outside fault windows, judged per sink (standin target: no VictoriaMetrics): remote_write: deliveries in 23 of 23, no freshness samples; 660s of 2080s after warmup judged (32%; WARN under 5%) |
| `rss_slope` | PASS | generator PASS: MiB/h life 0 +0.2 (limit 64); 750s of 2080s after warmup judged (36%; WARN under 5%) \| logit PASS: MiB/h life 0 +1.8 (limit 64); 750s of 2080s after warmup judged (36%; WARN under 5%) |
| `fd_slope` | PASS | generator PASS: warmup median 11 -> end 11 (limit +8) \| logit PASS: warmup median 11.5 -> end 11 (limit +8) |
| `ledger.wire` | PASS | G 4,291,300 = W 4,291,300 + K 0 + wire 0 (by design: 0 in UDP-affecting windows, -14 steady, 14 at the end; steady loss judged 0, each run at 0 or more, against limit 100); the generator's counted drop_newest loss, not in G: 0 event(s) in 0 batch(es) |
| `ledger.intake` | PASS | final life 0: W − D 4,291,300 vs E + B 4,291,300 (R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.edge` | PASS | final life 0: E 4,291,300 == A 4,291,300 (listener to aggregate); A_sent 21,600 == S_in[remote_write] 21,600 (aggregate to remote_write) |
| `ledger.script` | SKIP | no [ledger] sut_script |
| `ledger.aggregate` | PASS | final life 0: Ab 4,291,300 == A 4,291,300 (every event absorbed) |
| `ledger.egress` | PASS | remote_write: life 0 (final): Ab 4,291,300 − V[remote_write] 4,291,300 = 0, ok; rejected 2 cued by c0s4, 2 cued by c0s13, 2 cued by c0s14, 2 cued by c0s16, 2 cued by c0s17, counted (split copy; R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.windows` | SKIP | not enabled ([ledger] vm_every_window) |
| `ledger.replay` | SKIP | no killed life followed by another |
| `ledger.summary` | PASS | final life 0: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V[remote_write] 0; wire G − (W + K) 0 over the run, by design |
| `ledger.sent` | SKIP | standin target: ledger.egress and ledger.standin judge delivery against the stand-in's record |
| `ledger.topic` | SKIP | standin target: no topic |
| `ledger.standin` | PASS | 300 request(s) recorded, 213 stored, 36 cue(s); 700 resent sample(s) stored again with the same value; 19 classed step(s) (at_least_once); record, request classes, cues, and each step's class agree with the sink |
| `identity.sink` | PASS | final life 0 at +2164s: remote_write: received 216 vs delivered 206 + dropped 10 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `drops` | PASS | no *.dropped count outside the ledger and the sink rows |
| `recovery` | PASS | 19 of 19 fault(s) judged at 1 sink(s), each recovered within recovery_bound 45s of its end; warmup rate 2,000/s |
| `expect.ambiguous-retries` | PASS | delta of logit.component.retries{component=remote_write} on logit, during c0s1: c0s1 (+60s, +80s]: 6 (over 4 drain(s)), want >= 1 |
| `expect.rejected-drops` | PASS | delta of logit.component.batches.dropped{component=remote_write, reason=rejected} on logit, through c0s4: c0s4 (+375s, +440s]: 2 (over 13 drain(s)), want >= 1 |
| `expect.refused-holds` | PASS | max of logit.component.retrying{component=remote_write} on logit, during c0s7: c0s7 (+690s, +710s]: 1 (1 sample(s) and the value in force at its start), want >= 1 |
| `expect.refused-drops-nothing` | PASS | delta of logit.component.batches.dropped{component=remote_write} on logit, through c0s7: c0s7 (+690s, +755s]: 0 (over 13 drain(s)), want <= 0 |
| `expect.clean-holds` | PASS | max of logit.component.retrying{component=remote_write} on logit, during c0s19: c0s19 (+1950s, +1970s]: 1 (1 sample(s) and the value in force at its start), want >= 1 |
| `expect.retrying-clears` | PASS | last of logit.component.retrying{component=remote_write} on logit, after c0s19: c0s19 (+1970s, +2015s]: 0 (1 sample(s) and the value in force at its start), want <= 0 |

`standin-at-most-once`'s:

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 8 apply/revert action(s) returned 0; 0 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 2 exit(s) of the logit services, each a scheduled stop with code 0 |
| `restarts` | PASS | 0 start(s), each a scheduled start or restart |
| `self_log` | PASS | 7 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 55 readiness sample(s) read ok outside fault windows; 0 start(s) or unpause(s) ready in time |
| `progress` | PASS | 5 30s window(s) outside fault windows, judged per sink (standin target: no VictoriaMetrics): remote_write: deliveries in 5 of 5, no freshness samples; 120s of 475s after warmup judged (25%; WARN under 5%) |
| `rss_slope` | PASS | generator PASS: MiB/h life 0 +0.5 (limit 64); 195s of 475s after warmup judged (41%; WARN under 5%) \| logit PASS: MiB/h life 0 -3.4 (limit 64); 195s of 475s after warmup judged (41%; WARN under 5%) |
| `fd_slope` | PASS | generator PASS: warmup median 12 -> end 12 (limit +8) \| logit PASS: warmup median 11.5 -> end 12 (limit +8) |
| `ledger.wire` | PASS | G 1,081,300 = W 1,081,300 + K 0 + wire 0 (by design: 0 in UDP-affecting windows, -34 steady, 34 at the end; steady loss judged 0, each run at 0 or more, against limit 100); the generator's counted drop_newest loss, not in G: 0 event(s) in 0 batch(es) |
| `ledger.intake` | PASS | final life 0: W − D 1,081,300 vs E + B 1,081,300 (R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.edge` | PASS | final life 0: E 1,081,300 == A 1,081,300 (listener to aggregate); A_sent 5,500 == S_in[remote_write] 5,500 (aggregate to remote_write) |
| `ledger.script` | SKIP | no [ledger] sut_script |
| `ledger.aggregate` | PASS | final life 0: Ab 1,081,300 == A 1,081,300 (every event absorbed) |
| `ledger.egress` | PASS | remote_write: life 0 (final): Ab 1,081,300 − V[remote_write] 1,081,300 = 0, ok; rejected 2 cued by c0s3, counted (split copy; R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.windows` | SKIP | not enabled ([ledger] vm_every_window) |
| `ledger.replay` | SKIP | no killed life followed by another |
| `ledger.summary` | PASS | final life 0: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V[remote_write] 0; wire G − (W + K) 0 over the run, by design |
| `ledger.sent` | SKIP | standin target: ledger.egress and ledger.standin judge delivery against the stand-in's record |
| `ledger.topic` | SKIP | standin target: no topic |
| `ledger.standin` | PASS | 62 request(s) recorded, 49 stored, 8 cue(s); 0 resent sample(s) stored again with the same value; 4 classed step(s) (at_most_once); record, request classes, cues, and each step's class agree with the sink |
| `identity.sink` | PASS | final life 0 at +559s: remote_write: received 55 vs delivered 49 + dropped 6 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `drops` | PASS | no *.dropped count outside the ledger and the sink rows |
| `recovery` | PASS | 4 of 4 fault(s) judged at 1 sink(s), each recovered within recovery_bound 45s of its end; warmup rate 2,000/s |
| `expect.ambiguous-drops` | PASS | delta of logit.component.batches.dropped{component=remote_write, reason=ambiguous_at_most_once} on logit, through c0s1: c0s1 (+60s, +125s]: 2 (over 13 drain(s)), want >= 1 |
| `expect.ambiguous-never-retries` | PASS | delta of logit.component.retries{component=remote_write} on logit, through c0s1: c0s1 (+60s, +125s]: 0 (over 13 drain(s)), want <= 0 |
| `expect.refused-holds` | PASS | max of logit.component.retrying{component=remote_write} on logit, during c0s2: c0s2 (+155s, +175s]: 1 (1 sample(s)), want >= 1 |

The black-hole control ran from a scenario directory outside the repository, through
`soak.py run <path>`: `standin-remote-write`'s configs, `[ledger] vm_every_window = true`, and one
step, `respond` with `status = 200` and `apply = false` for 20 s. Its table:

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 2 apply/revert action(s) returned 0; 0 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 2 exit(s) of the logit services, each a scheduled stop with code 0 |
| `restarts` | PASS | 0 start(s), each a scheduled start or restart |
| `self_log` | PASS | 0 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 36 readiness sample(s) read ok outside fault windows; 0 start(s) or unpause(s) ready in time |
| `progress` | PASS | 5 30s window(s) outside fault windows, judged per sink (standin target: no VictoriaMetrics): remote_write: deliveries in 5 of 5, no freshness samples; 120s of 190s after warmup judged (63%; WARN under 5%) |
| `rss_slope` | PASS | generator PASS: MiB/h life 0 -2.4 (limit 64); 120s of 190s after warmup judged (63%; WARN under 5%) \| logit PASS: MiB/h life 0 +32.1 (limit 64); 120s of 190s after warmup judged (63%; WARN under 5%) |
| `fd_slope` | PASS | generator PASS: warmup median 11 -> end 12 (limit +8) \| logit PASS: warmup median 11.5 -> end 11 (limit +8) |
| `ledger.wire` | PASS | G 511,300 = W 511,300 + K 0 + wire 0 (by design: 0 in UDP-affecting windows, 65 steady, -65 at the end; steady loss judged 65, each run at 0 or more, against limit 100); the generator's counted drop_newest loss, not in G: 0 event(s) in 0 batch(es) |
| `ledger.intake` | PASS | final life 0: W − D 511,300 vs E + B 511,300 (R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.edge` | PASS | final life 0: E 511,300 == A 511,300 (listener to aggregate); A_sent 2,700 == S_in[remote_write] 2,700 (aggregate to remote_write) |
| `ledger.script` | SKIP | no [ledger] sut_script |
| `ledger.aggregate` | PASS | final life 0: Ab 511,300 == A 511,300 (every event absorbed) |
| `ledger.egress` | PASS | remote_write: life 0 (final): Ab 511,300 − V[remote_write] 511,300 = 0, ok (split copy; R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.windows` | FAIL | missing windows, 100 finding(s), first: remote_write soak_requests_0_total: life 0 gap 30s from +59s to +89s, windows emitted but not stored: window sent 2 batch(es) over 4 drain(s) from +67s to +87s (gaps <= 15s (1.5 x the 10s aggregate interval) or idle, with no window batches.sent from a + 7.5s to b - 2.5s (5s drains); a killed life's last sample within 11s (1.1 x) of the kill, a later life's first within 20s (2 x) of its start) |
| `ledger.replay` | SKIP | no killed life followed by another |
| `ledger.summary` | PASS | final life 0: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V[remote_write] 0; wire G − (W + K) 0 over the run, by design |
| `ledger.sent` | SKIP | standin target: ledger.egress and ledger.standin judge delivery against the stand-in's record |
| `ledger.topic` | SKIP | standin target: no topic |
| `ledger.standin` | PASS | 27 request(s) recorded, 25 stored, 2 cue(s); 0 resent sample(s) stored again with the same value; 0 classed step(s) (at_least_once); record, request classes, cues, and each step's class agree with the sink |
| `identity.sink` | PASS | final life 0 at +274s: remote_write: received 27 vs delivered 27 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `drops` | PASS | no *.dropped count outside the ledger and the sink rows |
| `recovery` | PASS | 1 of 1 fault(s) judged at 1 sink(s), each recovered within recovery_bound 45s of its end; warmup rate 2,000/s |
| `expect` | SKIP | the scenario has no [[expect]] tables |

What the runs showed about `logit` and the harness:

- **`prometheus_out` reads every row of its table as documented, under load.** Each step's first
  `retrying` line named the class its `class` key expected (`ambiguous` for `503`, `429`, and a
  reset; `refused` for both decompression bodies, `401`, `403`, `404`, `405`, `407`, `415`, and
  `501`; `clean` for the stopped stand-in), and each `rejected` step (`400` with `out of order
  sample`, `409`, `413`, `422`, `302`) dropped its two windows with no retry.
- **A stopped stand-in is a connect failure.** Docker's DNS stops answering for a stopped
  container, and `reqwest` reports the failed lookup as a connect error (`client error
  (Connect): dns error`), which the sink reads as `clean`. The sink counted 10
  `network_error` requests against the record's 7 resets; the other 3 are those lookups, which
  never reached the stand-in.
- **A resend after a `5xx` stores the same samples.** The `503` with `apply = true` stored each
  attempt, and the sink's 7 resends wrote 700 samples again, each with the value already stored.
- **A rejected window costs a cumulative series nothing.** Each rejected step lost two windows,
  and the next stored totals overwrote them, so Ab − V stayed 0; `ledger.egress` names the 10
  drops as cued.
- **A transient black hole is invisible to `ledger.egress` under cumulative temporality.** The
  control's 20 s of `200`s that stored nothing left Ab − V at 0, because the next window's
  totals overwrite the hole. `ledger.windows` caught it, with one finding per series. A
  stand-in scenario that needs to see a lost window sets `vm_every_window`, which a scenario
  with a `rejected` step can't, since a rejected window is lost too. `ledger.standin`'s check
  that every sample answered `2xx` was stored, added after review, FAILs the same run when it is
  re-scored: 200 samples, 2 windows of 100 series.
- **Self-test**: 1,185 expectations, from 1,074 on `main`. Reverting the cued excusal in
  `Ledger.fault_drops` FAILs four of them: a cued `rejected` drop then FAILs `ledger.egress` and
  `ledger.summary`, and the drop past the step's window reads 3 batches instead of 1.

### W9b: a Datadog-shaped stand-in for every `datadog_out` response class (2026-10-15)

W9b adds the stand-in's `datadog` shape, `series_errors`, V as the stored delta points' sum with
duplicates reported, and `ledger.sent` against a stand-in, with `standin-datadog`. Run
`20261015T180002Z` used `logit:soak` `sha256:4bc148b6d2f5`, rebuilt by `script/soak` from the
working tree at `45fdb280` plus W9b's harness changes (W9b changes no Rust, so the binary is
W9a's), `logit-soak-netem:local` `sha256:ca9f9da8292d`, and `python:3.13.16-alpine3.24`
`sha256:2d9aefe2fef0`, on the development host (32 cores, Docker 29.8.2). Its code is
`19ec65f1`'s but for comments; `results.md` below is its re-score with `soak.py check` at
`d71e2db4`, which orders `ledger.sent`'s cued drops by step and changes no verdict. Every row
passed.

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 32 apply/revert action(s) returned 0; 0 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 2 exit(s) of the logit services, each a scheduled stop with code 0 |
| `restarts` | PASS | 1 start(s), each a scheduled start or restart |
| `self_log` | PASS | 19 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 156 readiness sample(s) read ok outside fault windows; 0 start(s) or unpause(s) ready in time |
| `progress` | PASS | 20 30s window(s) outside fault windows, judged per sink (standin target: no VictoriaMetrics): datadog: deliveries in 20 of 20, no freshness samples; 570s of 1765s after warmup judged (32%; WARN under 5%) |
| `rss_slope` | PASS | generator PASS: MiB/h life 0 +0.0 (limit 64); 645s of 1765s after warmup judged (37%; WARN under 5%) \| logit PASS: MiB/h life 0 +0.8 (limit 64); 645s of 1765s after warmup judged (37%; WARN under 5%) |
| `fd_slope` | PASS | generator PASS: warmup median 11 -> end 12 (limit +8) \| logit PASS: warmup median 11.5 -> end 12 (limit +8) |
| `ledger.wire` | PASS | G 3,661,200 = W 3,661,200 + K 0 + wire 0 (by design: 0 in UDP-affecting windows, -87 steady, 87 at the end; steady loss judged 0, each run at 0 or more, against limit 100); the generator's counted drop_newest loss, not in G: 0 event(s) in 0 batch(es) |
| `ledger.intake` | PASS | final life 0: W − D 3,661,200 vs E + B 3,661,200 (R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.edge` | PASS | final life 0: E 3,661,200 == A 3,661,200 (listener to aggregate); A_sent 18,400 == S_in[datadog] 18,400 (aggregate to datadog) |
| `ledger.script` | SKIP | no [ledger] sut_script |
| `ledger.aggregate` | PASS | final life 0: Ab 3,661,200 == A 3,661,200 (every event absorbed) |
| `ledger.egress` | PASS | datadog: life 0 (final): Ab 3,661,200 − V[datadog] 3,500,400 = 160,800, counted (batches dropped: rejected 8; records dropped: oversize 200, rejected 604); rejected 2 cued by c0s2, 2 cued by c0s8, 2 cued by c0s13, 2 cued by c0s14, counted (split copy; R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.windows` | SKIP | not enabled ([ledger] vm_every_window) |
| `ledger.replay` | SKIP | no killed life followed by another |
| `ledger.summary` | PASS | final life 0: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V[datadog] 160,800 counted; wire G − (W + K) 0 over the run, by design |
| `ledger.sent` | PASS | to standin: every batch received ended in an accepted response; datadog: requests 2xx 176, 3xx 2, 4xx 44, 5xx 20, network_error 10; records accepted 17,596; cued, excused: api_key_rejected line(s) 3 by c0s4; request_refused line(s) 3 by c0s3, 1 by c0s5, 1 by c0s6; batch(es) dropped rejected 2 by c0s2, 2 by c0s8, 2 by c0s13, 2 by c0s14; record(s) dropped oversize 200 by c0s8; record(s) dropped rejected 4 by c0s1, 200 by c0s2, 200 by c0s13, 200 by c0s14; datadog life 0 at +1849s: received 184 vs delivered 176 + dropped 8 + buffer.batches 0, gap 0 (one batch in flight allowed); ledger.egress and ledger.standin read the stand-in's record |
| `ledger.topic` | SKIP | standin target: no topic |
| `ledger.standin` | PASS | 248 request(s) recorded, 183 stored, 30 cue(s); 700 duplicate copies of stored points (life 0 700), 140,000 counted twice by the destination, reported; 4 series named by series_errors; 15 classed step(s) (at_least_once); record, request classes, cues, and each step's class agree with the sink |
| `identity.sink` | PASS | final life 0 at +1849s: datadog: received 184 vs delivered 176 + dropped 8 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `drops` | PASS | no *.dropped count outside the ledger and the sink rows |
| `recovery` | PASS | 16 of 16 fault(s) judged at 1 sink(s), each recovered within recovery_bound 45s of its end; warmup rate 2,000/s |
| `expect.series-errors-drop-records` | PASS | delta of logit.output.records.dropped{component=datadog, reason=rejected} on logit, through c0s1: c0s1 (+60s, +125s]: 4 (over 13 drain(s)), want >= 1 |
| `expect.series-errors-drop-no-batch` | PASS | delta of logit.component.batches.dropped{component=datadog} on logit, through c0s1: c0s1 (+60s, +125s]: 0 (over 13 drain(s)), want <= 0 |
| `expect.rejected-drops` | PASS | delta of logit.component.batches.dropped{component=datadog, reason=rejected} on logit, through c0s2: c0s2 (+165s, +230s]: 2 (over 13 drain(s)), want >= 1 |
| `expect.refused-holds` | PASS | max of logit.component.retrying{component=datadog} on logit, during c0s4: c0s4 (+375s, +395s]: 1 (1 sample(s) and the value in force at its start), want >= 1 |
| `expect.refused-drops-nothing` | PASS | delta of logit.component.batches.dropped{component=datadog} on logit, through c0s4: c0s4 (+375s, +440s]: 0 (over 13 drain(s)), want <= 0 |
| `expect.ambiguous-retries` | PASS | delta of logit.component.retries{component=datadog} on logit, during c0s7: c0s7 (+690s, +710s]: 6 (over 4 drain(s)), want >= 1 |
| `expect.oversize-drops-records` | PASS | delta of logit.output.records.dropped{component=datadog, reason=oversize} on logit, through c0s8: c0s8 (+795s, +860s]: 200 (over 13 drain(s)), want >= 1 |
| `expect.oversize-drops-batches` | PASS | delta of logit.component.batches.dropped{component=datadog, reason=rejected} on logit, through c0s8: c0s8 (+795s, +860s]: 2 (over 13 drain(s)), want >= 1 |
| `expect.applied-5xx-retries` | PASS | delta of logit.component.retries{component=datadog} on logit, during c0s12: c0s12 (+1215s, +1235s]: 7 (over 4 drain(s)), want >= 1 |
| `expect.clean-holds` | PASS | max of logit.component.retrying{component=datadog} on logit, during c0s16: c0s16 (+1635s, +1655s]: 1 (1 sample(s) and the value in force at its start), want >= 1 |
| `expect.retrying-clears` | PASS | last of logit.component.retrying{component=datadog} on logit, after c0s16: c0s16 (+1655s, +1700s]: 0 (1 sample(s) and the value in force at its start), want <= 0 |

What the run showed about `logit` and the harness:

- **`datadog_out` reads every row of its table as documented, under load.** Each held step's
  first `retrying` line named its class: `refused` for `401`, `403`, `404`, and `405`;
  `ambiguous` for `408`, `429`, `500`, `501`, the `503`, and the reset; `clean` for the stopped
  stand-in. Each `rejected` step (`400`, `413`, `302`, `422`) dropped its two windows with no
  retry, and the `413` counted its 200 records `oversize`. The sink logged `api_key_rejected`
  for the `403` and `request_refused` for the other refusals, all inside their steps.
- **A `202` naming series counts them and drops no batch.** The `series_errors = 2` step's two
  requests each named their first 2 series, and the sink counted 4 records dropped `rejected`
  with no batch dropped.
- **The egress gap is the cued loss, to the event.** Ab − V was 160,800: the 8 rejected windows
  (4 steps, 2 windows each, 100 series at 200 increments) are 160,000, and the 4 named series
  800.
- **A resend after an applied `503` is stored again.** The `503` with `apply = true` stored 7
  attempts of the held batch before the eighth was accepted, so the stand-in holds 700
  duplicate copies, 140,000 increments a Datadog org would count twice. V counts each point
  once, and `ledger.standin` reports the copies, as `at_least_once` permits.
- **The sink counted 10 `network_error` requests against the record's 6 resets**; the other 4
  were connect failures to the stopped stand-in, which never reached it.
- **Self-test**: 1,286 expectations, from 1,187 at W9a. Each new rule was reverted in turn and
  failed at least one expectation; the PR lists them.

### W10b: the native hop (2026-10-15)

W10b adds the hop ledger and `native-hop` (§8d). Every run used `logit:soak`
`sha256:0592417ac86a`, built with `--no-cache` from `soak/w10b` (no Rust change from `main`
at `e77a86fd`), `logit-soak-netem:local` `sha256:ca9f9da8292d`, and
`victoriametrics/victoria-metrics:v1.152.0`, on the development host (32 cores, Docker 29.8.2).

The first two runs, `20261015T234302Z` and `20261016T000317Z`, FAILed `ledger.replay` on the
hop sink with every other row passing: the kill of `logit` inside `collector`'s stop replayed
306 and 307 batches over a last-drain queue of 251 and 252, which the first band, exact with
nothing delivered, read as 55 too many. Both were harness bugs. 50 of the 55 were the queue's
growth in the 5 s between the last drain and the kill: unlike a sink behind `aggregate`, the
hop sink is fed by the listener, at 10 batches a second. The other 5 were frames `collector`
acknowledged in the last second before its stop: a spool persists its cursor on a commit once
`checkpoint_interval` has passed, so when commits stop, up to an interval of them stays
unpersisted and replays (`crates/logit-pipeline/src/disk_queue.rs`, `after_cursor_advance`).
The band now covers both, and re-scoring both runs with `soak.py check` passes every row.

The third run, `20261016T002412Z` at `c3050457`, passed every row. `results.md` below is its
re-score with `soak.py check` after review tightened `ledger.hop`: a killed `logit` life
allows one frame above 0 rather than its last drain interval, and its replay of unpersisted
frames only when `collector` restarted before the replay, as it did before the second kill
and not the first. Re-scored the same way, all three runs pass every row, with `ledger.hop`
at −2,400, −2,600, and −2,400 inside allowances of about [−76,000, 11,500].

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 18 apply/revert action(s) returned 0; 6 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 8 exit(s) of the logit services, each a scheduled stop with code 0 or a scheduled kill with code 137 (3 kill(s)) |
| `restarts` | PASS | 5 start(s), each a scheduled start or restart |
| `self_log` | PASS | 9 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 48 readiness sample(s) read ok outside fault windows; 6 start(s) or unpause(s) ready in time, slowest ready 0.1s |
| `progress` | PASS | 3 30s window(s) outside fault windows, judged per sink, each freshness sample under 30s: victoria_metrics: deliveries in 3 of 3, 4 freshness sample(s); 60s of 960s after warmup judged (6%; WARN under 5%) |
| `rss_slope` | PASS | collector PASS: MiB/h life 3 -137.9 (limit 64); 70s of 960s after warmup judged (7%; WARN under 5%) \| generator PASS: MiB/h life 0 +0.7 (limit 64); 95s of 960s after warmup judged (10%; WARN under 5%) \| logit PASS: MiB/h life 2 +25.1 (limit 64); 70s of 960s after warmup judged (7%; WARN under 5%) |
| `fd_slope` | PASS | collector PASS: warmup median 16 -> end 16 (limit +8) \| generator PASS: warmup median 12 -> end 12 (limit +8) \| logit PASS: warmup median 15 -> end 15 (limit +8) |
| `ledger.wire` | PASS | G 2,051,600 = W 2,040,000 + K 0 + wire 11,600 (by design: 11,718 in UDP-affecting windows, -121 steady, 3 at the end; steady loss judged 0, each run at 0 or more, against limit 500); the generator's counted drop_newest loss, not in G: 0 event(s) in 0 batch(es) |
| `ledger.intake` | PASS | final life 2: W − D 390,100 vs E + B 390,100; logit life 0: W − D − B − X − S_hop 0 within [0, R]; logit life 1: W − D − B − X − S_hop 0 within [0, R]; collector life 0: E_B − X_B − Ab 0 within [0, R_hop]; collector life 1: E_B − X_B − Ab 0 within [0, R_hop]; collector life 2: E_B − X_B − Ab 0 within [0, R_hop] (R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000; R_hop 48,925 = 65 x 753 events per frame) |
| `ledger.edge` | PASS | logit final life 2: E 390,100 == S_hop[hop] 390,100 (listeners to hop); collector final life 3: E_B[hop] 451,100 == A 451,100 (hop to aggregate); collector final life 3: A_sent 1,400 == S_in[victoria_metrics] 1,400 (aggregate to victoria_metrics) |
| `ledger.hop` | PASS | hop to hop: ΣS_hop 2,040,000 − hop dropped 0 − ΣE_B 2,042,400 = -2,400, within allowance (allowance [-77,207, 11,506]: logit life 0 killed: −9,999 (2,000/s over its last drain interval), +753 (the batch in hand); logit life 1 killed: −10,000 (2,000/s over its last drain interval), +753 (the batch in hand), −9,036 (its replay of frames committed after its last cursor persist, which collector, restarted at +885s, forwards again); collector life 1 killed: −24,086 (its marks lost), +10,000 (2,000/s over its last drain interval); c0s5 pause on collector: −24,086 (a resend racing an ended connection's frames)); resends 3 at collector, resumed 1 and reconnects 4 at logit; a send window is 32 frames of at most 753 events (24,086) |
| `ledger.script` | SKIP | no [ledger] sut_script |
| `ledger.aggregate` | PASS | final life 3: Ab 451,100 == A 451,100 (every event absorbed) |
| `ledger.egress` | PASS | victoria_metrics: life 0: E_B − X_B − V 0 = residual 0 + Ab − V 0, ok; life 1 (killed): Ab 590,100 − V[victoria_metrics] 579,900 = 10,200, within band (band [-10,000, 30,001]: ingest 2,000/s over the last drain interval; below, the residual 0 plus one 4.99986s drain interval of ingest, absorbed after the last drain and flushed before the kill; above, one 10s aggregate window (configured) unflushed at the kill plus one drain interval of queue growth after the last drain); residual E_B − X_B − Ab 0 at its last drain, what the kill lost from the receive queue and inbox, by design, not judged here; D has no shutdown part: a SIGKILL counts no shutdown drops; life 2: E_B − X_B − V 0 = residual 0 + Ab − V 0, ok; life 3 (final): Ab 451,100 − V[victoria_metrics] 451,100 = 0, ok (split copy; R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.windows` | SKIP | not enabled ([ledger] vm_every_window) |
| `ledger.replay` | PASS | victoria_metrics: life 2's first drain replayed 0 vs killed life 1's last-drain buffer.batches 0; logit hop: life 1's first drain replayed 2 vs killed life 0's last-drain buffer.batches 1, band [-10, 20]: 10.0 batches/s received and 10.0 delivered over its last drain interval, 0.9s from its last drain to the kill, and a 1s checkpoint of its last delivering 10.0/s; logit hop: life 2's first drain replayed 305 vs killed life 1's last-drain buffer.batches 251, band [-1, 62]: 10.0 batches/s received and 0.0 delivered over its last drain interval, 5.0s from its last drain to the kill, and a 1s checkpoint of its last delivering 10.0/s (one batch in flight allowed) |
| `ledger.summary` | PASS | logit final life 2, collector final life 3; the hop itself is ledger.hop's: uncounted 0 = W − D − E − B 0 + E − S_hop 0 + E_B − A 0 + A − Ab 0 + Ab − V[victoria_metrics] 0; wire G − (W + K) 11,600 over the run, by design; end to end over the run, G 2,051,600 less counted loss 0: victoria_metrics: V 2,032,200 <= G − counted 2,051,600 |
| `ledger.sent` | SKIP | local target: ledger.egress judges delivery against the backends |
| `ledger.topic` | SKIP | victoria-metrics target: no topic |
| `ledger.standin` | SKIP | victoria-metrics target: no stand-in |
| `identity.sink` | PASS | collector final life 3 at +1065s: victoria_metrics: received 14 vs delivered 14 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed); logit hop: final life 2 at +1045s: received 1,694 + replayed 305 + torn 0 vs delivered 1,999 + dropped 0 + buffer.batches 0, gap 0 |
| `drops` | PASS | no *.dropped count outside the ledger and the sink rows |
| `recovery` | PASS | 9 of 9 fault(s) judged at 1 sink(s), each recovered within recovery_bound 60s of its end; warmup rate 2,000/s |
| `expect.replay-deduplicated` | PASS | delta of logit.input.batches.resends{component=hop} on collector, after c0s6: c0s6 (+630s, +690s]: 2 (over 12 drain(s)), want >= 1 |
| `expect.reconnects-after-collector-kill` | PASS | delta of logit.output.reconnects{component=hop} on logit, through c0s7: c0s7 (+720s, +810s]: 1 (over 19 drain(s)), want >= 1 |
| `expect.spool-holds-the-stop` | PASS | max of logit.component.buffer.batches{component=hop} on logit, during c0s4: c0s4 (+420s, +450s]: 293 (6 sample(s) and the value in force at its start), want >= 2 |

- `ledger.hop` read a gap of −2,400 events, a surplus of about three frames, in this run, and
  −2,400 and −2,600 in the first two. The surplus is within the allowance, but the run can't say
  which fault forwarded it. The running gap (the row's lines) is the hop's backlog plus any
  surplus so far, and each killed life's uncounted tail shifts it by up to 10,000 events, so it
  can't be split by fault; it settled at −2,241 after the last fault. Two mechanisms fit: `collector`'s kill, which forgot its marks, and
  `logit`'s replay, after its kill inside `collector`'s stop, of frames committed in the second
  before the stop and never persisted, to a `collector` whose restart had forgotten their mark.
  Both are documented duplicates (`docs/deploying.md`'s "Forwarding between `logit` nodes"). An event-level reconciliation
  would need a per-frame count at `logit_in` of frames forwarded above a restarted mark, which
  `logit` doesn't export; this stops here rather than adding one.
- `logit.input.batches.resends` was 3 (6 and 10 in the first two runs), all in `collector`'s
  life after its kill, and `replay-deduplicated` saw 2 after the first `logit` kill:
  `collector` acknowledged `logit`'s replay under the old sender identity without forwarding
  it. `logit.output.batches.resumed` was 1 and `logit.output.reconnects` 4.
- While `collector` was partitioned or stopped, the hop's backlog grew at the full 2,000 events
  a second, to about 47,000 by the kill of `logit` inside the stop, and drained within
  `recovery_bound` once `collector` returned; in the second run `recovery` read `collector`'s
  ingest at 1,180% of the warmup rate after the partition while it did.
- `progress` judged only 60 s of 960 after warmup, three 30 s windows: the schedule leaves
  little steady state between faults and their 60 s recovery.

### W11a: TCP ingress (2026-10-15)

`tcp-ingress` ran twice from `soak/w11a` at `7bddbbc6`, the second run with the override and
the generator's queue size the next commit adds, on the development host
(`sailfish`, 32 cores, Linux 7.2.8), `logit:soak` built with `--no-cache` after `origin/main`'s
head (`3c1ab0f1b2ea`). The final run is `perf/results/soak/20261016T020310Z`, exit 0:

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 16 apply/revert action(s) returned 0; 4 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 4 exit(s) of the logit services, each a scheduled stop with code 0 or a scheduled kill with code 137 (1 kill(s)) |
| `restarts` | PASS | 2 start(s), each a scheduled start or restart |
| `self_log` | PASS | 11 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 67 readiness sample(s) read ok outside fault windows; 4 start(s) or unpause(s) ready in time, slowest ready 0.1s |
| `progress` | PASS | 6 30s window(s) outside fault windows, judged per sink, each freshness sample under 30s: victoria_metrics: deliveries in 6 of 6, 8 freshness sample(s); 150s of 1170s after warmup judged (13%; WARN under 5%) |
| `rss_slope` | WARN | generator PASS: MiB/h life 0 +60.6 (limit 64); 245s of 1170s after warmup judged (21%; WARN under 5%) \| logit WARN: MiB/h life 0 +98.8, life 2 -3.3 (limit 64); 230s of 1170s after warmup judged (20%; WARN under 5%) |
| `fd_slope` | PASS | generator PASS: warmup median 11 -> end 11 (limit +8) \| logit PASS: warmup median 13 -> end 13 (limit +8) |
| `ledger.wire` | PASS | G 2,026,700 = W 2,017,076 + K 0 + counted 4 (truncated 4) + gap 9,620 (9,806 in reported windows, -182 in judged TCP-affecting windows, 72 steady, -76 at the end; loss judged 74, each run at 0 or more, against limit 2,100); the generator's counted loss, not in G: 444,600 event(s) in 4,446 batch(es) (ambiguous_at_most_once 4, overflow_newest 4,442) |
| `ledger.intake` | PASS | final life 2: W − D 770,342 vs E + B 770,342; life 0: W − D − B − X − Ab 0 within [0, R]; life 1: W − D − B − X − Ab 0 within [0, R] (R 67,000 = 0 + 2 x 1,000 + 65 x 1,000) |
| `ledger.edge` | PASS | final life 2: E 770,342 == A 770,342 (listener to aggregate); A_sent 3,700 == S_in[victoria_metrics] 3,700 (aggregate to victoria_metrics) |
| `ledger.script` | SKIP | no [ledger] sut_script |
| `ledger.aggregate` | PASS | final life 2: Ab 770,342 == A 770,342 (every event absorbed) |
| `ledger.egress` | PASS | victoria_metrics: life 0: W − D − B − X − V 0 = residual 0 + Ab − V 0, ok; life 1 (killed): Ab 108,400 − V[victoria_metrics] 98,400 = 10,000, within band (band [-9,999, 29,997]: ingest 2,000/s over the last drain interval; below, the residual 0 plus one 4.99999s drain interval of ingest, absorbed after the last drain and flushed before the kill; above, one 10s aggregate window (configured) unflushed at the kill plus one drain interval of queue growth after the last drain); residual W − D − B − X − Ab 0 at its last drain, what the kill lost from the receive queue and inbox, by design, not judged here; D has no shutdown part: a SIGKILL counts no shutdown drops; life 2 (final): Ab 770,342 − V[victoria_metrics] 770,342 = 0, ok (split copy; R 67,000 = 0 + 2 x 1,000 + 65 x 1,000) |
| `ledger.windows` | SKIP | not enabled ([ledger] vm_every_window) |
| `ledger.replay` | PASS | victoria_metrics: life 2's first drain replayed 0 vs killed life 1's last-drain buffer.batches 0 (one batch in flight allowed) |
| `ledger.summary` | PASS | final life 2: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V[victoria_metrics] 0; wire G − (W + K + F) 9,620 over the run, F 4 counted; ledger.wire judges the rest |
| `ledger.sent` | SKIP | local target: ledger.egress judges delivery against the backends |
| `ledger.topic` | SKIP | victoria-metrics target: no topic |
| `ledger.standin` | SKIP | victoria-metrics target: no stand-in |
| `identity.sink` | PASS | final life 2 at +1260s: victoria_metrics: received 37 vs delivered 37 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `drops` | PASS | no *.dropped count outside the ledger and the sink rows |
| `recovery` | PASS | 8 of 8 fault(s) judged at 1 sink(s), each recovered within recovery_bound 45s of its end; warmup rate 2,000/s |
| `expect.no-idle-close-under-delay` | PASS | delta of logit.input.connections.closed{component=statsd, reason=idle} on logit, through c0s1: c0s1 (+60s, +225s]: 0 (over 33 drain(s)), want <= 0 |
| `expect.generator-overflows` | PASS | delta of logit.component.batches.dropped{component=statsd, reason=overflow_newest} on generator, through c0s4: c0s4 (+540s, +615s]: 153 (over 15 drain(s)), want >= 1 |
| `expect.generator-ambiguous` | PASS | delta of logit.component.batches.dropped{component=statsd, reason=ambiguous_at_most_once} on generator, through c0s4: c0s4 (+540s, +615s]: 3 (over 15 drain(s)), want >= 1 |
| `expect.idle-close` | PASS | delta of logit.input.connections.closed{component=statsd, reason=idle} on logit, through c0s8: c0s8 (+915s, +1050s]: 1 (over 27 drain(s)), want >= 1 |

- **The first run, `20261016T013950Z`, FAILed `expect.generator-overflows` and
  `expect.generator-ambiguous` with nothing lost.** The host's `net.ipv4.tcp_rmem` allows a
  receive buffer of 32 MiB, and the generator's writes all completed through the SUT's 30 s
  pause: 160,000 lines went into the socket buffers and W read every one after the unpause. A
  scenario sized against the host default never stalls a write, so the scenario's override caps
  `tcp_rmem` on `logit` and `tcp_wmem` on `generator` at 256 KiB, and the generator's queue at 50
  batches. With them the pause stalled the write three times: 3 `ambiguous_at_most_once` drops,
  153 batches `overflow_newest`, a surplus of 134 lines against an allowance of 500, and 3
  partial lines counted `truncated`, with no uncounted loss.
- **A graceful stop lost nothing in either run.** At the stop the generator's G was 1,180,900
  (first run) and 1,138,200 plus the pause's 134-line surplus (final run), and the stopped life's
  W equaled it: the listener read until its shutdown, and the generator's next write failed
  `Clean` on a refused dial and stayed queued. No known-gaps entry is needed for
  `ReadStep::Shutdown`'s undrained socket at this rate; the window stays reported.
- **A kill's window shows 9,799 lines (9,779 in the first run) it can't split.** The killed
  life's last drain was 5 s before the kill, and a SIGKILL loses up to one `internal` interval of
  counters, about 9,700 lines at 2,000 a second, so the gap is mostly lines the killed process
  read and never exported. The generator counted 900 lines in the drain after the kill, most
  written before it, then failed `Clean`. What sat in the socket buffers at the kill is inside
  that gap and below what telemetry can resolve.
- **The partition kept the SUT's address** (172.28.0.3 before and after, both runs), so its
  window was judged: one ambiguous drop, one partial line `truncated`, and a 45-line surplus
  against an allowance of 300. The abandoned connection's retransmits delivered after the
  reconnect, and the SUT read about 28,000 lines in the drain after it. No RST.
- **Netem on the generator lost nothing**: both runs' steady windows, which include its 120 s
  delay and 90 s of 10% loss, gap by at most 72 lines, under the edge tolerance; TCP retransmitted
  every segment. 30% loss on the SUT's ACKs stalled nothing and dropped nothing.
- **The generator's 90 s pause made one idle close** 60 s in, as `idle_timeout` sets, and the
  generator redialed on its next write with nothing lost; none closed under the netem delay.
- `rss_slope` WARNed on `logit`'s first life (+98.8 MiB/h over 230 s judged), as any growth
  under an hour's run does; its final life read −3.3 MiB/h, and `fd_slope` ended at the warmup
  median on both services.
- Re-scoring every earlier run directory (52, the three `native-hop` runs among them) with this
  branch's harness gives `results.md`, `results.json`, and exit codes identical to `origin/main`'s.

### W11b: file tailing (2026-10-15)

`tail-rotate` ran twice on the development host (`sailfish`), against `logit:soak` built with
`--no-cache` after `origin/main`'s head (`3c1ab0f1b2ea`). The final run is
`perf/results/soak/20261016T025252Z`, from `soak/w11b` at `f369b3c8` with no modified path,
exit 0, shown here as `check` re-scores it at the branch's head, which merges the file wire's
drain-interval rows into runs:

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 10 apply/revert action(s) returned 0; 5 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 5 exit(s) of the logit services, each a scheduled stop with code 0 or a scheduled kill with code 137 (2 kill(s)) |
| `restarts` | PASS | 4 start(s), each a scheduled start or restart |
| `self_log` | PASS | 0 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 59 readiness sample(s) read ok outside fault windows; 5 start(s) or unpause(s) ready in time, slowest ready 0.1s |
| `progress` | PASS | 6 30s window(s) outside fault windows, judged per sink, each freshness sample under 30s: victoria_metrics: deliveries in 6 of 6, 7 freshness sample(s); 150s of 765s after warmup judged (20%; WARN under 5%) |
| `rss_slope` | PASS | generator PASS: MiB/h life 0 -0.4 (limit 64); 215s of 765s after warmup judged (28%; WARN under 5%) \| logit PASS: MiB/h life 4 -1033.2 (limit 64); 175s of 765s after warmup judged (23%; WARN under 5%) |
| `fd_slope` | PASS | generator PASS: warmup median 11 -> end 11 (limit +40) \| logit PASS: warmup median 16 -> end 34 (limit +40) |
| `ledger.wire` | PASS | G 830,700 (shared directory: 830,700 lines in 19 files) vs W 830,500 + counted 0: W + F − G -200 against the replay band [-9,999, 12,776]; the tailers stopped short at the end with W 830,500 |
| `ledger.intake` | PASS | final life 4: W − D 379,900 vs E + B 379,900; life 0: W − D − B − X − Ab 0 within [0, R]; life 1: W − D − B − X − Ab 0 within [0, R]; life 2: W − D − B − X − Ab 100 within [0, R]; life 3: W − D − B − X − Ab 100 within [0, R] (R 327,000 = 0 + 262 x 1,000 + 65 x 1,000) |
| `ledger.edge` | PASS | final life 4: E[tail] 379,900 == T_in[parse] 379,900 (into parse); T_in[parse] 379,900 == T_out[parse] 379,900 (parse forwards every event); T_out[parse] 379,900 == T_in[labels] 379,900 (into labels); T_in[labels] 379,900 == T_out[labels] 379,900 (labels forwards every event); T_out[labels] 379,900 == T_in[count] 379,900 (into count); T_in[count] 379,900 == T_out[count] 379,900 (count forwards every event); T_out[count] 379,900 == T_in[metrics_only] 379,900 (into metrics_only); T_in[metrics_only] 379,900 == T_out[metrics_only] 379,900 (metrics_only forwards every event); Σ(sent into aggregate) 379,900 == A 379,900 (listeners to aggregate); A_sent 23 == S_in[victoria_metrics] 23 (aggregate to victoria_metrics) |
| `ledger.script` | SKIP | no [ledger] sut_script |
| `ledger.aggregate` | PASS | final life 4: Ab 379,900 == A 379,900 (every event absorbed) |
| `ledger.egress` | PASS | victoria_metrics: life 0: W − D − B − X − V 0 = residual 0 + Ab − V 0, ok; life 1: W − D − B − X − V 0 = residual 0 + Ab − V 0, ok; life 2 (killed): Ab 85,100 − V[victoria_metrics] 80,100 = 5,000, within band (band [-5,099, 14,998]: ingest 1,000/s over the last drain interval; below, the residual 100 plus one 4.99971s drain interval of ingest, absorbed after the last drain and flushed before the kill; above, one 10s aggregate window (configured) unflushed at the kill plus one drain interval of queue growth after the last drain); residual W − D − B − X − Ab 100 at its last drain, what the kill lost from the receive queue and inbox, by design, not judged here; D has no shutdown part: a SIGKILL counts no shutdown drops; life 3 (killed): Ab 89,700 − V[victoria_metrics] 84,700 = 5,000, within band (band [-5,100, 15,000]: ingest 1,000/s over the last drain interval; below, the residual 100 plus one 4.99982s drain interval of ingest, absorbed after the last drain and flushed before the kill; above, one 10s aggregate window (configured) unflushed at the kill plus one drain interval of queue growth after the last drain); residual W − D − B − X − Ab 100 at its last drain, what the kill lost from the receive queue and inbox, by design, not judged here; D has no shutdown part: a SIGKILL counts no shutdown drops; life 4 (final): Ab 379,900 − V[victoria_metrics] 379,900 = 0, ok (split copy; R 327,000 = 0 + 262 x 1,000 + 65 x 1,000) |
| `ledger.windows` | PASS | 1 series over 1 sink(s) x 5 life/lives, widest gap 10.001s (gaps <= 15s (1.5 x the 10s aggregate interval) or idle, with no window batches.sent from a + 7.5s to b - 2.5s (5s drains); a killed life's last sample within 11s (1.1 x) of the kill, a later life's first within 20s (2 x) of its start; 1 idle gap(s) excused: life 0 +59s to +120s (60.629s, 1 series), window sent 0 batch(es) over 0 drain(s) from +67s to +118s) |
| `ledger.replay` | PASS | victoria_metrics: life 3's first drain replayed 0 vs killed life 2's last-drain buffer.batches 0; victoria_metrics: life 4's first drain replayed 0 vs killed life 3's last-drain buffer.batches 0 (one batch in flight allowed) |
| `ledger.summary` | PASS | final life 4: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V[victoria_metrics] 0; wire G − (W + K + F) 200 over the run, F 0 counted; ledger.wire judges the rest; end to end over the run, G 830,700 less counted loss 0 plus the replay band's 12,776: victoria_metrics: V 820,300 <= G − counted 843,476 |
| `ledger.sent` | SKIP | local target: ledger.egress judges delivery against the backends |
| `ledger.topic` | SKIP | victoria-metrics target: no topic |
| `ledger.standin` | SKIP | victoria-metrics target: no stand-in |
| `identity.sink` | PASS | final life 4 at +865s: victoria_metrics: received 23 vs delivered 23 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `drops` | PASS | no *.dropped count outside the ledger and the sink rows |
| `recovery` | PASS | 5 of 5 fault(s) judged at 1 sink(s), each recovered within recovery_bound 45s of its end; warmup rate 1,000/s |
| `expect.files-rotated` | PASS | delta of logit.input.files.rotated{component=tail} on logit, run: run (+60s, +825s]: 130 (over 109 drain(s)), want >= 1 |
| `expect.no-resume-rejected` | PASS | delta of logit.input.files.resume_rejected{component=tail} on logit, run: run (+60s, +825s]: 0 (over 109 drain(s)), want <= 0 |
| `expect.no-checkpoint-errors` | PASS | delta of logit.input.checkpoint.errors{component=tail} on logit, run: run (+60s, +825s]: 0 (over 109 drain(s)), want <= 0 |

- **The first run, `20261016T023523Z`, FAILed `ledger.egress` on a harness bug.** The restarted
  life read W − D − B − X − V of −4,900. A `docker restart` started the next process 0.2 s after
  the old one's final drain (+270.07 s against a start of +270.26 s), and `telemetry.py` placed a
  drain in the newest life that started at most 1 s after it, so the old life's final drain, with
  its close-time counts, landed in the new life while V placed the same flush in the old one by
  timestamp. A point at a drain now takes that drain's life from the uptime sequence. No earlier
  run had a restart that fast, and every one re-scores identically; the first run re-scores to
  PASS.
- **The wire: G 830,700 lines in 19 files, W 830,500.** The shared directory's count equals the
  writer's `events.delivered`. W − G is −200, inside the band [−9,999, 12,776]. Each kill's window
  shows about 100 lines fewer in W than G (109 and 101): the killed life never exported its last
  drain interval of counts, about 5,000 lines, and the restart re-read about as many from its
  last checkpoint, since drains and checkpoints both run every 5 s. No replay surplus showed
  beyond that offset, and the end sequence's wait stopped with W 200 short, held.
- **A clean stop and a restart split no line.** `json` reported no parse failure, and the stop's
  and the restart's windows gap by −55 and −2, inside drain-interval interpolation. The band
  allowed 5 and 7 split lines for the files open at those stops.
- **The 150 s kill spanned three rotations** (the writer rotated at +485 s, +525 s, and +570 s).
  The restarted tailer resumed every checkpointed inode under its new name, read the three new
  files from their start, and read 159,900 lines in its first drain: no `resume_rejected`, no
  checkpoint error, nothing lost. The 30 s kill fell between two rotations.
- **`files.rotated` counted 130 against the writer's 18 rotations**, because each scan counts
  every renamed name in `file_out`'s cascade.
- **The tailer kept every file open.** `logit.input.files.open` rose with each rotation to 19,
  and the SUT's descriptors from 16 at warmup to 34 at the end, inside the scenario's
  `fd_growth` of 40. `tail_in` closes a file only when no pattern matches it any more or a new
  inode takes its path; under a pattern that matches every rotated name and a writer that keeps
  every file, nothing closes one.
- The pause bunched the SUT's drains at the unpause, and the tailer read 61,083 lines in the
  first drain interval after it: a drain interval's G − W on a file wire is lag, which is why
  only the run's total is judged.

### W12a: spans across the native hop, and Lua under the trace compiler (2026-10-17)

The first four runs used `logit:soak` `sha256:99edd881460f`, built with `--no-cache` from `origin/main` at
`4b86ccc7` (W12a changes no Rust but a doc comment), and `logit-soak-netem:local`
`sha256:25de6298bd24`, on the development host. The scenarios ran two at a time from `soak/w12a`'s
working tree, at their own durations. No run found a harness bug or a `logit` defect. The
first `native-hop-spans` run kept every span (`span_sample_rate: 1.0`); review then set both SUTs
to the product's default, `0.1`, and tightened `lua-path-jit`'s checks, and both were run again
(below).

| Run | Scenario | Verdict |
|---|---|---|
| `20261017T165556Z` | `native-hop-spans` | WARN: `drops` (spans past the buffer) and `rss_slope` on `collector`; every other row PASS |
| `20261017T165602Z` | `lua-path-jit` | every row PASS |
| `20261017T171456Z` | `lua-stall-jit` | every row PASS |
| `20261017T171459Z` | `lua-memory-jit` | WARN: `rss_slope` on `generator`; every other row PASS |
| `20261017T201853Z` | `native-hop-spans` at `0.1` | every row PASS |
| `20261017T201907Z` | `lua-path-jit`, `--duration 5m`, the delete removed | FAIL, as intended: `script-errors` and `script-error-loss` |
| `20261017T202633Z` | `lua-path-jit`, `--duration 5m`, `pairs` made to miss `scratch` | FAIL, as intended: `script-errors` and `script-error-loss` |
| `20261017T203350Z` | `lua-path-jit` with the pinned expectations | every row PASS |

The last four ran on `logit:soak` `sha256:75c60bba2ff8`, rebuilt at 10:34 local from a later
branch of this stack, which changes no Rust either, and `logit-soak-netem:local`
`sha256:094e63b23016`. All four started after the host's 10:54 to
13:15 local suspend, which none of the W12a runs spans.

What the runs showed:

- **Spans cost the ledger nothing.** At `1.0`, `native-hop-spans` passed every `native-hop` row: the hop's
  `ledger.hop` at −4,000 inside [−77,036, 11,495], both replays in band, and no uncounted loss.
  Each SUT's stdout carried about 19,000 span events, which `telemetry.py` skipped, and
  `logit.internal.spans.emitted` counted 17,595 on `logit` and 17,004 on `collector`.
- **The hop carries the trace, measured by hand.** Joining the two stdouts after the run, 9,026
  of `collector`'s 9,087 `logit_in send` spans name a parent span recorded in `logit`'s stdout.
  The other 61 name spans `logit` never exported: dropped from its buffer, or in a killed life's
  last drain interval. No scenario row performs this join; §10's item 7 plans one.
- **At `1.0`, a backlog drops spans, counted.** This is a stress observation, not the shipped
  scenario's rate. `internal` buffers at most 512 spans per component between
  drains (`docs/design/internal-telemetry.md`, "Spans"). Two drains passed it on both sides: the
  one right after the 200 ms netem delay was reverted, and the one where the hop drained after
  `collector`'s 90 s stop. `logit`'s `hop` counted
  `logit.internal.spans.dropped{reason="buffer_full"}` 644, and `collector`'s `hop` and `window`
  624 and 626. `drops` reported them as WARN, which `[expected]` can't excuse, since it takes
  only `rss_slope` and `fd_slope`; at the default rate the same backlog keeps about a tenth of
  those spans.
- **At the default rate, nothing drops.** The `0.1` rerun passed every row, `drops` included:
  `logit` emitted 1,752 spans and `collector` 3,389, which adds roots of its own for frames
  `logit` didn't sample, as a receiver at the sender's rate does. By hand again, 933 of
  `collector`'s 1,814 `logit_in send` spans carried a parent, and 925 of those name a span in
  `logit`'s stdout; the other 881 are those roots.
- **`collector`'s `rss_slope` WARN at `1.0` is a short fit.** +69.0 MiB/h over its final life's 65 s, the
  only judged span long enough; `logit`'s final life fit −135.3 MiB/h over the same span.
- **`lua-path-jit` balances the script hop under the compiler.** E 646,255 == L_in ==
  L_emit 627,525 + L_drop 12,193 + L_err 6,537, and L_sent == L_emit + L_flush 34. That first
  run's expectations were `min = 1` only, so a failed in-place check would have passed as
  counted script errors. By hand, L_err is every 97th event not also a 53rd one (about 6,536),
  and every series in VictoriaMetrics carries `stage` alone. The scenario now fails an event
  whose deleted attribute still reads back, and pins `script-errors` and `script-error-loss`
  to [2,200, 2,650] over the first step's 120 s, around the 2,427 every-97th failures expected
  there, so a check that fails on more than about 1 event in 1,000 FAILs both rows. Two
  throwaway edits showed it: with the delete removed, the script failed every event ("soak: a
  deleted attribute still reads back"), and with the `pairs` loop made to miss `scratch`, it
  failed every event ("soak: pairs found 1 of 2 written attributes"). Each run read 235,472 for
  both rows, which FAILed against [2,200, 2,650]. The rerun with the script restored read 2,428
  for both, and its L_err of 6,500 over L_in 642,700 is the every-97th count with no extra
  failure.
- **A compiled loop still reads stalled.** In `lua-stall-jit`, the loop on the 360,000th event
  read `stalled` three times from +185 s, `script_stalled` counted once, the receive queue
  filled and evicted 48,000 datagrams, counted as `overflow_oldest`, and the script recovered with
  no error. Stall detection reads a heartbeat, not a hook, so the compiler doesn't change it.
- **The loop is calibrated for the compiler.** In the image on the development host, 1e10
  iterations took 35 s compiled and 78 s interpreted, measured from the first event's output of
  a one-node config. `lua-stall`'s own comment had 31.5 s interpreted on 2026-10-11, so the host
  ran it more than twice as slowly this time; its stall still ends inside its `mark`, and its
  comment now gives both.
- **`max_memory` still trips under the compiler.** In `lua-memory-jit` the VM held 70,046,127
  bytes and the SUT exited 2 at +267 s inside the `exit` step, started again, and every ledger row
  passed with no script loss.
- **The generator's RSS rose with its own send queue.** In `lua-memory-jit`, once the SUT
  exited, compose's DNS stopped resolving `logit`, and the generator's `statsd_out` failed
  `Clean` ("resolving statsd_out endpoint logit:8125") and held its queue. The queue grew 100
  batches per 5 s drain to 776, then counted `overflow_newest`, and its allocated bytes rose
  from 2.2 to 75 MiB with it; resident memory went from 18 to 86 MiB. When the SUT came back
  the queue drained in one drain and allocated memory fell to 2.3 MiB, but resident stayed near
  78 to 85 MiB for about 55 s before the allocator returned it, falling to 32 MiB. The fit over
  the generator's judged spans read that as +199.5 MiB/h. It's the generator's own sending,
  not the compiler: nothing in the generator runs Lua. No `lua-memory` run data survives to
  compare, since W7a's run directories were removed with their worktree.

### W12b: rolling overlap (2026-10-17)

`rolling-overlap` ran three times on the development host (`trout`, 24 cores, 64 GB, Linux
7.2.9, Docker 29.8.2), one of them stopped early, against `logit:soak` `sha256:75c60bba2ff8`
built from `soak/w12b` after the merge of `soak/w12a`, beside other soak scenarios. The final
run is `perf/results/soak/20261017T202637Z`, from `soak/w12b` at `e0236ed4` with no modified
path, exit 0, shown here as `check` re-scores it at the branch's head, whose end allowance
weights each generator sink by its own input's batch:

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 14 apply/revert action(s) returned 0; 5 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 7 exit(s) of the logit services, each a scheduled stop with code 0 |
| `restarts` | PASS | 6 start(s), each a scheduled start or restart |
| `self_log` | PASS | 4 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 136 readiness sample(s) read ok outside fault windows; 5 start(s) or unpause(s) ready in time, slowest ready 0.2s |
| `progress` | PASS | 26 30s window(s) outside fault windows but an instance's own hold, judged per sink on the instances running through each (0 with none running skipped), each freshness sample under 30s: victoria_metrics: deliveries in 26 of 26, 26 freshness sample(s); 750s of 1080s after warmup judged (69%; WARN under 5%) |
| `rss_slope` | PASS | generator PASS: MiB/h life 0 +1.8 (limit 64); 360s of 1080s after warmup judged (33%; WARN under 5%) \| logit PASS: MiB/h life 1 +7.1, life 2 +7.9 (limit 64); 340s of 1080s after warmup judged (31%; WARN under 5%) \| logit-b PASS: MiB/h life 1 +8.7, life 3 +5.1 (limit 64); 260s of 1080s after warmup judged (24%; WARN under 5%) |
| `fd_slope` | PASS | generator PASS: warmup median 26 -> end 26 (limit +8) \| logit PASS: warmup median 15 -> end 15 (limit +8) \| logit-b PASS: warmup median 15 -> end 15 (limit +8) |
| `ledger.wire` | PASS | G 2,293,600 = W 2,293,600 + K 0 + wire 0 (by design: -127 in UDP-affecting windows, 117 steady, 10 at the end; steady loss judged 246, each run at 0 or more, against limit 2,200); the generator's counted drop_newest loss, not in G: 0 event(s) in 0 batch(es); W by instance: logit 1,177,000, logit-b 1,116,600 |
| `ledger.overlap` | PASS | G 2,293,600 − Σ(W + K + F) 2,293,600 (logit 1,177,000, logit-b 1,116,600) = 0, within [-200, 5]: 5 close(s) of an instance's socket, each at most ceil(used / 512) + 1 datagrams, loose by up to a quarter of its SO_RCVBUF; below, the generators' end allowance |
| `ledger.intake` | PASS | logit: final life 2: W − D 605,500 vs E + B 605,500; life 0: W − D − B − X − Ab 0 within [0, R]; life 1: W − D − B − X − Ab 0 within [0, R] (R 142,000 = 10,000 + 67 x 1,000 + 65 x 1,000) \| logit-b: final life 3: W − D 240,200 vs E + B 240,200; life 0: W − D − B − X − Ab 0 within [0, R]; life 1: W − D − B − X − Ab 0 within [0, R]; life 2: W − D − B − X − Ab 0 within [0, R] (R 142,000 = 10,000 + 67 x 1,000 + 65 x 1,000) |
| `ledger.edge` | PASS | logit: final life 2: E[statsd] 605,500 == T_in[instance] 605,500 (into instance); T_in[instance] 605,500 == T_out[instance] 605,500 (instance forwards every event); Σ(sent into aggregate) 605,500 == A 605,500 (listeners to aggregate); A_sent 5,575 == S_in[victoria_metrics] 5,575 (aggregate to victoria_metrics) \| logit-b: final life 3: E[statsd] 240,200 == T_in[instance] 240,200 (into instance); T_in[instance] 240,200 == T_out[instance] 240,200 (instance forwards every event); Σ(sent into aggregate) 240,200 == A 240,200 (listeners to aggregate); A_sent 2,475 == S_in[victoria_metrics] 2,475 (aggregate to victoria_metrics) |
| `ledger.script` | SKIP | logit: no [ledger] sut_script \| logit-b: no [ledger] sut_script |
| `ledger.aggregate` | PASS | logit: final life 2: Ab 605,500 == A 605,500 (every event absorbed) \| logit-b: final life 3: Ab 240,200 == A 240,200 (every event absorbed) |
| `ledger.egress` | PASS | logit: victoria_metrics: life 0: W − D − B − X − V 0 = residual 0 + Ab − V 0, ok; life 1: W − D − B − X − V 0 = residual 0 + Ab − V 0, ok; life 2 (final): Ab 605,500 − V[victoria_metrics] 605,500 = 0, ok (split copy; R 142,000 = 10,000 + 67 x 1,000 + 65 x 1,000) \| logit-b: victoria_metrics: life 0: W − D − B − X − V 0 = residual 0 + Ab − V 0, ok; life 1: W − D − B − X − V 0 = residual 0 + Ab − V 0, ok; life 2: W − D − B − X − V 0 = residual 0 + Ab − V 0, ok; life 3 (final): Ab 240,200 − V[victoria_metrics] 240,200 = 0, ok (split copy; R 142,000 = 10,000 + 67 x 1,000 + 65 x 1,000) |
| `ledger.windows` | SKIP | logit: not enabled ([ledger] vm_every_window) \| logit-b: not enabled ([ledger] vm_every_window) |
| `ledger.replay` | SKIP | logit: no killed life followed by another \| logit-b: no killed life followed by another |
| `ledger.summary` | PASS | logit: final life 2: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V[victoria_metrics] 0 \| logit-b: final life 3: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V[victoria_metrics] 0; wire G − Σ(W + K + F) 0 over the run, ledger.overlap's; end to end over the run, G 2,293,600 less the instances' counted loss 0: victoria_metrics: ΣV 2,293,600 <= G − counted 2,293,600 |
| `ledger.sent` | SKIP | local target: ledger.egress judges delivery against the backends |
| `ledger.topic` | SKIP | victoria-metrics target: no topic |
| `ledger.standin` | SKIP | victoria-metrics target: no stand-in |
| `identity.sink` | PASS | logit: final life 2 at +1175s: victoria_metrics: received 56 + replayed 4 vs delivered 60 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed) \| logit-b: final life 3 at +1180s: victoria_metrics: received 25 vs delivered 25 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `drops` | PASS | logit: no *.dropped count outside the ledger and the sink rows \| logit-b: no *.dropped count outside the ledger and the sink rows |
| `recovery` | PASS | 6 of 7 fault(s) judged at 1 sink(s), each recovered within recovery_bound 45s of its end; warmup rate 2,000/s |
| `expect.logit-b-carries-c0s1` | PASS | min_delta of logit.input.datagrams{component=statsd} on logit-b, during c0s1: c0s1 (+60s, +120s]: 5,000 (smallest of 12 drain(s)), want >= 1 |
| `expect.logit-carries-c0s2` | PASS | min_delta of logit.input.datagrams{component=statsd} on logit, during c0s2: c0s2 (+210s, +270s]: 5,000 (smallest of 13 drain(s)), want >= 1 |
| `expect.logit-kernel-drops` | PASS | delta of logit.input.kernel.dropped{component=statsd} on logit, run: run (+60s, +1140s]: 0 (over 201 drain(s)), want <= 0 |
| `expect.logit-b-kernel-drops` | PASS | delta of logit.input.kernel.dropped{component=statsd} on logit-b, run: run (+60s, +1140s]: 0 (over 194 drain(s)), want <= 0 |

- **The first run, `20261017T173458Z`, FAILed `progress` because the host slept, not because
  of `logit`.** Its schedule ran, and every fault had recovered by +945 s. From +1120 s, about
  100 s into the cooldown, the laptop froze the driver and every container on its way to a
  suspend (`systemd-sleep` suspended at 10:54:54 local, +1182 s, and resumed at 13:15:40), for
  2 h 22 min. On resume the loop was past its duration and ran
  the end sequence at once; every wait in it stayed inside its bound, though `quiet` ran its full
  90 s, since no sample was newer than the export's 300 s lookback. `progress` then read the
  sleep as 30-second windows with nothing delivered. Every other row passed, `ledger.overlap`
  among them: a gap of 5 against an allowance of 15. The driver now records a `stalled` phase
  when its tick runs more than 120 s late, and `run` FAILs it, reading it from the watchdog's gap
  for an older run like this one: re-scored, the first run's `run` reads `stalled: the driver's
  tick ran 8,508s late from +1120s`. The second run, `20261017T201857Z`, was stopped 3 minutes
  in to keep the host to two soaks at once, and the third, above, ran under
  `systemd-inhibit --what=sleep:idle`.
- **A swap lost nothing.** In the final run G and Σ(W + K + F) both read 2,293,600, and in the
  first they differed by 5. Every close's final `receive_buffer.used.bytes` read 0 but one, the
  first run's last stop of `logit-b`, at 4,800 bytes, about 6 datagrams at a truesize of 768
  bytes. That fits that run's gap of 5: the reader was keeping up, and the closing socket held
  a few datagrams when its read loop stopped. Each stop's own gap, read from interpolated drain
  intervals, ranged from −87 to +78, the out-of-phase interpolation error the row reports
  without judging.
- **The kernel moved every flow and dropped none.** Each instance read every drain while the
  other was down (`expect.logit-b-carries-c0s1` and `expect.logit-carries-c0s2`, at least
  5,000 lines a drain), and neither counted a kernel drop over the run. The eight flows split
  1,177,000 to 1,116,600 over the run, and unevenly from life to life: `logit-b`'s final life
  read 240,200, `logit`'s 605,500.
- **Each instance's own ledger balanced**, every life of every instance at 0, with V split by
  the `soak_instance` label. `logit`, stopped inside VictoriaMetrics' stop, replayed 4 spooled
  batches into its next life, which `identity.sink` balanced.
- `recovery` judged 6 of 7 faults; `c0s5`, `logit`'s stop inside VictoriaMetrics' stop, had no
  drain interval clear of the other fault.

### W12c: SIGHUP and reopen (2026-10-17)

`sighup` ran once on the development host (`sailfish`), against `logit:soak` built from
`soak/w12c` at `082443e0`, which then held `soak/w12b`'s head. The run is
`perf/results/soak/20261017T173412Z`, exit 0, every row PASS or SKIP:

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 20 apply/revert action(s) returned 0; 0 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 2 exit(s) of the logit services, each a scheduled stop with code 0 |
| `restarts` | PASS | 1 start(s), each a scheduled start or restart |
| `self_log` | PASS | 2 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 146 readiness sample(s) read ok outside fault windows; 0 start(s) or unpause(s) ready in time |
| `progress` | PASS | 23 30s window(s) outside fault windows, judged per sink, each freshness sample under 30s: victoria_metrics: deliveries in 23 of 23, 23 freshness sample(s); 660s of 810s after warmup judged (81%; WARN under 5%) |
| `rss_slope` | PASS | generator PASS: MiB/h life 0 -0.5 (limit 64); 665s of 810s after warmup judged (82%; WARN under 5%) \| logit PASS: MiB/h life 0 +2.0 (limit 64); 665s of 810s after warmup judged (82%; WARN under 5%) |
| `fd_slope` | PASS | generator PASS: warmup median 12 -> end 11 (limit +8) \| logit PASS: warmup median 13 -> end 13 (limit +8) |
| `ledger.wire` | PASS | G 1,752,300 = W 1,752,300 + K 0 + wire 0 (by design: 0 in UDP-affecting windows, -53 steady, 53 at the end; steady loss judged 0, each run at 0 or more, against limit 100); the generator's counted drop_newest loss, not in G: 0 event(s) in 0 batch(es) |
| `ledger.intake` | PASS | final life 0: W − D 1,752,300 vs E + B 1,752,300 (R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.edge` | PASS | final life 0: E 1,752,300 == A 1,752,300 (listener to aggregate); A_sent 8,900 == S_in[victoria_metrics] 8,900 (aggregate to victoria_metrics) |
| `ledger.script` | SKIP | no [ledger] sut_script |
| `ledger.aggregate` | PASS | final life 0: Ab 1,752,300 == A 1,752,300 (every event absorbed) |
| `ledger.egress` | PASS | victoria_metrics: life 0 (final): Ab 1,752,300 − V[victoria_metrics] 1,752,300 = 0, ok (split copy; R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.windows` | SKIP | not enabled ([ledger] vm_every_window) |
| `ledger.replay` | SKIP | no killed life followed by another |
| `ledger.summary` | PASS | final life 0: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V[victoria_metrics] 0; wire G − (W + K) 0 over the run, by design |
| `ledger.sent` | SKIP | local target: ledger.egress judges delivery against the backends |
| `ledger.topic` | SKIP | victoria-metrics target: no topic |
| `ledger.standin` | SKIP | victoria-metrics target: no stand-in |
| `identity.sink` | PASS | final life 0 at +899s: victoria_metrics: received 89 vs delivered 89 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `drops` | PASS | no *.dropped count outside the ledger and the sink rows |
| `recovery` | PASS | 1 of 1 fault(s) judged at 1 sink(s), each recovered within recovery_bound 45s of its end; warmup rate 2,000/s |
| `file.reopen.c0s3` | PASS | logit: archive.log -> archive.log.1 with c0s4's SIGHUP; lines in archive.log.1, archive.log.2, archive.log: 1,752,300, archive's events.delivered over logit's lives: 1,752,300; archive.log.1: 391,800 line(s), the latest at +190s; c0s4's SIGHUP finished at +190s; archive.log.2: 600,000 line(s), the earliest at +190s; c0s4's SIGHUP started at +190s |
| `file.reopen.c0s7` | PASS | logit: archive.log -> archive.log.2 with c0s8's SIGHUP; lines in archive.log.1, archive.log.2, archive.log: 1,752,300, archive's events.delivered over logit's lives: 1,752,300; archive.log.2: 600,000 line(s), the latest at +490s; c0s8's SIGHUP finished at +490s; archive.log: 760,500 line(s), the earliest at +490s; c0s8's SIGHUP started at +490s |
| `expect.reopen-steady-1` | PASS | delta of logit.output.file.reopens{component=archive} on logit, through 1: c0s1 (+60s, +115s]: 1 (over 11 drain(s)), want >= 1 |
| `expect.reopen-steady-2` | PASS | delta of logit.output.file.reopens{component=archive} on logit, through 2: c0s2 (+120s, +175s]: 1 (over 11 drain(s)), want >= 1 |
| `expect.reopen-rename-1` | PASS | delta of logit.output.file.reopens{component=archive} on logit, through 4: c0s4 (+190s, +245s]: 1 (over 11 drain(s)), want >= 1 |
| `expect.reopen-backend-stopped` | PASS | delta of logit.output.file.reopens{component=archive} on logit, through 6: c0s6 (+330s, +385s]: 1 (over 11 drain(s)), want >= 1 |
| `expect.reopen-rename-2` | PASS | delta of logit.output.file.reopens{component=archive} on logit, through 8: c0s8 (+490s, +545s]: 1 (over 11 drain(s)), want >= 1 |
| `expect.reopen-steady-3` | PASS | delta of logit.output.file.reopens{component=archive} on logit, through 10: c0s10 (+630s, +685s]: 1 (over 11 drain(s)), want >= 1 |
| `expect.generator-keeps-sending` | PASS | min_delta of logit.output.messages{component=statsd} on generator, through 9: c0s9 (+570s, +625s]: 10,000 (smallest of 11 drain(s)), want >= 1 |
| `expect.archive-never-drops` | PASS | delta of logit.component.batches.dropped{component=archive} on logit, run: run (+60s, +870s]: 0 (over 162 drain(s)), want <= 0 |

- **Each reopen landed between two batches, well inside the 2 s tolerance.** `docker kill -s
  HUP` took 94 ms and 101 ms. The renamed file's last line was stamped 50 ms and 56 ms before
  the signal's apply finished, and the new file's first line 93 ms and 94 ms after it started:
  `archive` switched files within one 100 ms listener batch of the signal.
- **Every event is in one file, and in one only.** The three files hold 391,800, 600,000, and 760,500
  lines, 1,752,300 in all, equal to `archive`'s `events.delivered`, to G, and to V. No line
  lacked a timestamp.
- **One reopen per SIGHUP to `logit`, none from the generator's.** `logit` logged six `reopen
  signal received` lines at info, which `self_log` ignores, and `logit.output.file.reopens`
  rose by 1 through each. The generator logged one and kept sending 10,000 lines per drain.
- **The signals opened no fault window, and the steady-state rows judged straight through
  them.** `progress` judged 81% of the run after warmup, the VictoriaMetrics stop and its
  recovery being the only window; `ledger.wire` judged steady-state loss at 0 across all seven
  signals; the stdout telemetry stream kept its 5 s drains.
- **The off-ledger sink changed no ledger row.** `ledger.edge` read E == A, since a fan-out
  counts one send per batch, and `drops` found nothing, with `archive` under
  `overflow: drop_oldest`.
- After scoring, the three archive files (260 MB) were deleted from the run's `shared/`, and
  their line counts and first and last timestamps kept in `shared-counts.json`, which
  `file.reopen` reads for a file `shared/` no longer holds. A `check` before that fallback
  existed FAILed both rows on the missing files; at the branch's head the run re-scores as
  above.

### W12d: TLS reload (2026-10-17)

`tls-reload` ran once on the development host (`trout`), against `logit:soak` built from
`soak/w12d` at `894e5364` with no modified path, and a netem image rebuilt with `openssl` and
`certs.sh`. The run is `perf/results/soak/20261017T204712Z`, exit 0, every row PASS or SKIP:

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 30 apply/revert action(s) returned 0; 1 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 2 exit(s) of the logit services, each a scheduled stop with code 0 |
| `restarts` | PASS | 2 start(s), each a scheduled start or restart |
| `self_log` | PASS | 3 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON, 1 tls_reload_failed line(s) inside broken rotations |
| `ready` | PASS | 143 readiness sample(s) read ok outside fault windows; 1 start(s) or unpause(s) ready in time, slowest ready 0.2s |
| `progress` | PASS | 22 30s window(s) outside fault windows, judged per sink, each freshness sample under 30s: victoria_metrics: deliveries in 22 of 22, 22 freshness sample(s); 630s of 870s after warmup judged (72%; WARN under 5%) |
| `rss_slope` | PASS | generator PASS: MiB/h life 0 +0.6 (limit 64); 645s of 870s after warmup judged (74%; WARN under 5%) \| logit PASS: MiB/h life 0 +21.0, life 1 +5.4 (limit 64); 645s of 870s after warmup judged (74%; WARN under 5%) |
| `fd_slope` | PASS | generator PASS: warmup median 11 -> end 11 (limit +8) \| logit PASS: warmup median 13 -> end 13 (limit +8) |
| `ledger.wire` | PASS | G 1,869,200 = W 1,869,200 + K 0 + counted 0 + gap 0 (-74 in reported windows, 0 in judged TCP-affecting windows, 82 steady, -8 at the end; loss judged 84, each run at 0 or more, against limit 300); the generator's counted loss, not in G: 3,100 event(s) in 31 batch(es) (overflow_newest 31) |
| `ledger.intake` | PASS | final life 1: W − D 1,377,300 vs E + B 1,377,300; life 0: W − D − B − X − Ab 0 within [0, R] (R 67,000 = 0 + 2 x 1,000 + 65 x 1,000) |
| `ledger.edge` | PASS | final life 1: E 1,377,300 == A 1,377,300 (listener to aggregate); A_sent 6,900 == S_in[victoria_metrics] 6,900 (aggregate to victoria_metrics) |
| `ledger.script` | SKIP | no [ledger] sut_script |
| `ledger.aggregate` | PASS | final life 1: Ab 1,377,300 == A 1,377,300 (every event absorbed) |
| `ledger.egress` | PASS | victoria_metrics: life 0: W − D − B − X − V 0 = residual 0 + Ab − V 0, ok; life 1 (final): Ab 1,377,300 − V[victoria_metrics] 1,377,300 = 0, ok (split copy; R 67,000 = 0 + 2 x 1,000 + 65 x 1,000) |
| `ledger.windows` | SKIP | not enabled ([ledger] vm_every_window) |
| `ledger.replay` | SKIP | no killed life followed by another |
| `ledger.summary` | PASS | final life 1: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V[victoria_metrics] 0; wire G − (W + K + F) 0 over the run, F 0 counted; ledger.wire judges the rest |
| `ledger.sent` | SKIP | local target: ledger.egress judges delivery against the backends |
| `ledger.topic` | SKIP | victoria-metrics target: no topic |
| `ledger.standin` | SKIP | victoria-metrics target: no stand-in |
| `identity.sink` | PASS | final life 1 at +950s: victoria_metrics: received 69 vs delivered 69 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `drops` | PASS | no *.dropped count outside the ledger and the sink rows |
| `recovery` | PASS | 2 of 2 fault(s) judged at 1 sink(s), each recovered within recovery_bound 45s of its end; warmup rate 2,000/s |
| `file.reopen` | SKIP | the scenario has no rename step |
| `tls.served.c0s3` | PASS | logit:8125 served statsd serial A65314DC0EBDF8D2, notAfter 1794948508; want c0s1's gen 2, serial A65314DC0EBDF8D2, notAfter 1794948508 |
| `tls.served.c0s8` | PASS | logit:8125 served statsd serial A65314DC0EBDF8D2, notAfter 1794948508; want c0s1's gen 2, serial A65314DC0EBDF8D2, notAfter 1794948508, still in force after broken c0s6 |
| `tls.served.c0s11` | PASS | logit:8125 served statsd serial 4A464C057819C968, notAfter 1795121728; want c0s9's gen 4, serial 4A464C057819C968, notAfter 1795121728, still in force after broken c0s6 |
| `tls.served.c0s15` | PASS | logit:8125 served statsd serial C578081A2CA1DC67, notAfter 1795208268; want c0s13's gen 5, serial C578081A2CA1DC67, notAfter 1795208268, still in force after broken c0s6 |
| `expect.server-reload-1` | PASS | delta of logit.tls.reloads{component=statsd, outcome=reloaded} on logit, through 2: c0s2 (+70s, +125s]: 1 (over 11 drain(s)), want >= 1 and <= 1 |
| `expect.server-expiry-rises-1` | PASS | rise of logit.tls.certificate.not_after{component=statsd, side=server} on logit, through 2: c0s2 (+70s, +125s]: 86,474 (11 sample(s) and the value in force at its start), want >= 86400 |
| `expect.server-reload-2` | PASS | delta of logit.tls.reloads{component=statsd, outcome=reloaded} on logit, through 10: c0s10 (+490s, +545s]: 1 (over 11 drain(s)), want >= 1 and <= 1 |
| `expect.server-expiry-rises-2` | PASS | rise of logit.tls.certificate.not_after{component=statsd, side=server} on logit, through 10: c0s10 (+490s, +545s]: 173,220 (11 sample(s) and the value in force at its start), want >= 86400 |
| `expect.server-reload-backend-stopped` | PASS | delta of logit.tls.reloads{component=statsd, outcome=reloaded} on logit, through 14: c0s14 (+630s, +685s]: 1 (over 11 drain(s)), want >= 1 and <= 1 |
| `expect.server-expiry-rises-backend-stopped` | PASS | rise of logit.tls.certificate.not_after{component=statsd, side=server} on logit, through 14: c0s14 (+630s, +685s]: 86,540 (11 sample(s) and the value in force at its start), want >= 86400 |
| `expect.broken-reload-fails` | PASS | delta of logit.tls.reloads{component=statsd, outcome=failed} on logit, through 7: c0s7 (+370s, +425s]: 1 (over 11 drain(s)), want >= 1 and <= 1 |
| `expect.broken-reloads-nothing` | PASS | delta of logit.tls.reloads{component=statsd, outcome=reloaded} on logit, through 7: c0s7 (+370s, +425s]: 0 (over 11 drain(s)), want <= 0 |
| `expect.broken-expiry-holds` | PASS | rise of logit.tls.certificate.not_after{component=statsd, side=server} on logit, through 7: c0s7 (+370s, +425s]: 0 (11 sample(s) and the value in force at its start), want >= 0 and <= 0 |
| `expect.server-reloads-total` | PASS | delta of logit.tls.reloads{component=statsd, outcome=reloaded} on logit, run: run (+60s, +930s]: 3 (over 174 drain(s)), want >= 3 and <= 3 |
| `expect.server-failures-total` | PASS | delta of logit.tls.reloads{component=statsd, outcome=failed} on logit, run: run (+60s, +930s]: 1 (over 174 drain(s)), want >= 1 and <= 1 |
| `expect.client-reload` | PASS | delta of logit.tls.reloads{component=statsd, outcome=reloaded} on generator, through 4: c0s4 (+180s, +265s]: 1 (over 17 drain(s)), want >= 1 and <= 1 |
| `expect.client-expiry-rises` | PASS | rise of logit.tls.certificate.not_after{component=statsd, side=client} on generator, through 4: c0s4 (+180s, +265s]: 86,594 (17 sample(s) and the value in force at its start), want >= 86400 |
| `expect.lines-after-restart` | PASS | min_delta of logit.input.frames{component=statsd} on logit, after 5: c0s5 (+270s, +315s]: 10,000 (smallest of 9 drain(s)), want >= 1 |
| `expect.no-connection-errors` | PASS | delta of logit.component.diagnostics{component=statsd, key=connection_error} on logit, run: run (+60s, +930s]: 0 (over 174 drain(s)), want <= 0 |

- **Each SIGHUP reloaded within a millisecond.** `docker kill -s HUP` took 97 ms to 105 ms, and
  `tls_reloaded` (or, for the broken rotation, `tls_reload_failed`) followed `reopen signal
  received` by 0.3 ms to 0.8 ms. Every probe 15 s later read the generation the rotation issued,
  and the one after the broken rotation read the generation before it.
- **The broken rotation cost nothing but the counted failure.** The reload counted
  `outcome="failed"` once and logged one WARN keyed `tls_reload_failed`, which `self_log` excused
  inside the rotation; `not_after` held, and the next good rotation reloaded as usual.
- **The generator's poll reloaded its client certificate 3.5 s after the rotation**, inside its
  10 s `tls_reload_interval`, with no signal, and its `not_after{side="client"}` rose by a day.
  After the SUT's restart a minute later it reconnected cleanly: the SUT read 10,000 lines or
  more in every drain of the recovery window, and counted no `connection_error` all run, the
  four probes' handshakes included. The SUT trusts any certificate from the run's CA, so no
  row shows which generation the generator presented; it presents the loaded one because
  `ReloadingCert`'s resolver returns `current()` (`crates/logit-pipeline/src/tls.rs`).
- **The restart lost nothing on the wire.** The generator counted 31 batches `overflow_newest`
  while the SUT was down, outside G; W equaled G over the run.
- The SUT's reopen generation restarts at 1 in a new process life, so `reopen signal received`
  reads generation 1 twice in its stderr.
- The run started before the host was told not to sleep; a `systemd-inhibit --what=sleep:idle`
  lock was held from about 9 minutes in to the end, and the timeline shows no gap.

### W16: an InfluxDB-shaped stand-in for every `influxdb_out` response class (2026-10-17)

W16 adds the stand-in's `influxdb` shape, the line-protocol parser, V as one stored point per
series and timestamp, `ledger.standin`'s rules for the shape, and the three
`standin-influxdb*` scenarios. The runs used `logit:soak-w16` `sha256:be9724584652`, built from
`9bfc434f`'s `Dockerfile` with W16's harness changes (W16 changes no Rust), and
`python:3.13.16-alpine3.24` `sha256:2d9aefe2fef0`, on the development host (Docker 29.8.2). They
ran at `3a816a20`, all three at once, 20 s apart under `systemd-inhibit --what=sleep:idle`, each
from `soak.py run` with `SOAK_IMAGE=logit:soak-w16`, after its self-test and a `logit validate`
of each config, as `script/soak run` does: other sessions on the host rebuild the shared
`logit:soak` tag from their own trees. Every row of each run passed.

| Run | Scenario | `ledger.egress` | `ledger.standin` |
|---|---|---|---|
| `20261018T024144Z` | `standin-influxdb` | Ab 4,292,600 − V 4,052,600 = 240,000, counted: 12 batches dropped `rejected`, 2 cued by each of `c0s2`, `c0s3`, `c0s11`, `c0s12`, `c0s16`, `c0s17` | 288 requests, 210 stored; 600 points rewritten with the same value; 240,000 in rejected requests' points, the life's whole gap; destination `org='soak-org', bucket='soak'` |
| `20261018T024204Z` | `standin-influxdb-carried` | Ab 4,292,300 − V 3,852,300 = 440,000, counted: 22 batches dropped `rejected`, 2 by each of the 11 rejected steps | 255 requests, 200 stored; 600 rewritten; 440,000 in rejected requests' points; destination `org='soak-org', orgID='0123456789abcdef', bucket='soak-carried'` |
| `20261018T024224Z` | `standin-influxdb-mixed` | Ab 4,292,300 − V 3,932,300 = 360,000, counted: 18 batches dropped `rejected`, 2 by each of the 9 rejected steps | 268 requests, 204 stored; 600 rewritten; 360,000 in rejected requests' points; destination `org='soak-org', bucket='soak-carried'` |

`standin-influxdb`'s class signatures, from `ledger.standin`'s lines:

| Step | Answer | Read as |
|---|---|---|
| `c0s2`, `c0s3` | `400`, a parse error and `Please provide either orgID or org` | `rejected`: no `retrying` line, 2 batches dropped each |
| `c0s4`, `c0s5` | `401`, InfluxDB 2's body and InfluxDB 3's | `refused` |
| `c0s6` to `c0s10` | `403`; `404` naming the bucket, the org, an orgID, and neither | `refused`, nothing carried |
| `c0s11`, `c0s12` | `413`, `422` | `rejected` |
| `c0s13` to `c0s15` | `429`, `500`, `503` after storing | `ambiguous` |
| `c0s16`, `c0s17` | `408`, `302` | `rejected` |
| `c0s18` | a reset | `ambiguous` |
| `c0s19` | a stop of the stand-in | `clean` |

In `standin-influxdb-carried`, `c0s6` to `c0s10` each read `rejected` and dropped 2 batches,
and `[[expect]]` read `retrying` at 0 during each. In `standin-influxdb-mixed`, `c0s6`, `c0s7`,
and `c0s10` read `rejected`, and `c0s8` and `c0s9`, the `404`s naming the org, `refused`. Every
`[[expect]]` passed, `no-bucket-drops-nothing` over 416 drains in both carried runs.

What the runs showed about `logit` and the harness:

- **`influxdb_out` reads every cued row of its table as documented, in all three
  configurations.** Each `403` and `404` split on where the value it names came from, as the
  module doc's "Response classes" says, and `408`, the "any other status" row, read `rejected`,
  unlike `datadog_out`, which retries it.
- **The egress gap is the rejected steps' points, to the increment.** Each rejected step lost
  two 10 s windows of 100 series at 200 increments, 40,000, and each run's gap is its count of
  rejected steps times that. No point of a rejected request is in the store.
- **A resend after an applied `503` rewrites the same points.** The `503` with `apply = true`
  stored 6 attempts of the held batch before the seventh was accepted, so each run holds 600
  rewrites, each with the value first stored, and V counts each point once.
- **A `204` with no `Content-Length` is accepted.** Every default answer was counted `2xx` by
  the sink, on keep-alive connections.
- **The sink counted 10 `network_error` requests against the record's 7 resets**; the other 3
  are connect failures to the stopped stand-in, as in W9a and W9b.
- **The query reached the stand-in in the form the module doc names**: `org=` alone with
  `org:` configured, `org=` and `orgID=` when both are carried, and `default_org:`'s `org=`
  beside a carried bucket, each with `bucket=` and `precision=ns`.
- **The table's timeout row isn't cued.** `influxdb_out`'s 10 s request timeout
  (`DEFAULT_TIMEOUT`) isn't configurable, and a stand-in that held its answer that long would
  stall the sink past a step. Its "DNS failure" row is exercised only by the stopped stand-in's
  lookups.
- **Self-test**: 1,773 expectations, from 1,718 on `main` at `9bfc434f`. Each new rule was reverted in turn
  and failed at least one expectation; the PR lists them.

### W13a: discarding transforms (2026-10-18)

`tail-rotate-stale` ran twice and `tail-rotate` once on the development host (`trout`), against
the `logit:soak` image built at 2026-10-17 19:29 PDT, after `origin/main`'s head (`9bfc434f`);
the branch changes no Rust, so the image needs only `timestamp` and `drop_stale`, both on
`main`. The first run, `perf/results/soak/20261018T023724Z`, passed every row under the design's
first `stop-discards` bounds, `[36,000, 50,000]`; the bounds were then set from the derivation
in the scenario's comment, `[38,000, 46,000]`. The second run, `perf/results/soak/20261018T030955Z`,
from `soak/w13a` at `7359c9e2` with only documentation modified, exit 0, every row PASS or
SKIP:

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 10 apply/revert action(s) returned 0; 5 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 5 exit(s) of the logit services, each a scheduled stop with code 0 or a scheduled kill with code 137 (2 kill(s)) |
| `restarts` | PASS | 4 start(s), each a scheduled start or restart |
| `self_log` | PASS | 0 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 58 readiness sample(s) read ok outside fault windows; 5 start(s) or unpause(s) ready in time, slowest ready 0.2s |
| `progress` | PASS | 6 30s window(s) outside fault windows, judged per sink, each freshness sample under 30s: victoria_metrics: deliveries in 6 of 6, 7 freshness sample(s); 150s of 825s after warmup judged (18%; WARN under 5%) |
| `rss_slope` | PASS | generator PASS: MiB/h life 0 +1.9 (limit 64); 215s of 825s after warmup judged (26%; WARN under 5%) \| logit PASS: MiB/h life 4 -863.1 (limit 64); 175s of 825s after warmup judged (21%; WARN under 5%) |
| `fd_slope` | PASS | generator PASS: warmup median 11 -> end 11 (limit +40) \| logit PASS: warmup median 16 -> end 36 (limit +40) |
| `ledger.wire` | PASS | G 891,300 (shared directory: 891,300 lines in 21 files) vs W 891,100 + counted 0: W + F − G -200 against the replay band [-10,001, 12,777]; the tailers stopped short at the end with W 891,100 |
| `ledger.intake` | PASS | final life 4: W − D 379,900 vs E + B 379,900; life 0: W − D − B − X − ΣT_disc − Ab 0 within [0, R]; life 1: W − D − B − X − ΣT_disc − Ab 0 within [0, R]; life 2: W − D − B − X − ΣT_disc − Ab 0 within [0, R]; life 3: W − D − B − X − ΣT_disc − Ab 100 within [0, R] (R 457,000 = 0 + 392 x 1,000 + 65 x 1,000) |
| `ledger.edge` | PASS | final life 4: E[tail] 379,900 == T_in[parse] 379,900 (into parse); T_in[parse] 379,900 − T_disc[parse] 0 = 379,900 == T_out[parse] 379,900 (parse sends what it doesn't discard); T_out[parse] 379,900 == T_in[stamp] 379,900 (into stamp); T_in[stamp] 379,900 − T_disc[stamp] 0 = 379,900 == T_out[stamp] 379,900 (stamp sends what it doesn't discard); T_out[stamp] 379,900 == T_in[labels] 379,900 (into labels); T_in[labels] 379,900 − T_disc[labels] 0 = 379,900 == T_out[labels] 379,900 (labels sends what it doesn't discard); T_out[labels] 379,900 == T_in[count] 379,900 (into count); T_in[count] 379,900 − T_disc[count] 0 = 379,900 == T_out[count] 379,900 (count sends what it doesn't discard); T_out[count] 379,900 == T_in[metrics_only] 379,900 (into metrics_only); T_in[metrics_only] 379,900 − T_disc[metrics_only] 0 = 379,900 == T_out[metrics_only] 379,900 (metrics_only sends what it doesn't discard); T_out[metrics_only] 379,900 == T_in[fresh] 379,900 (into fresh); T_in[fresh] 379,900 − T_disc[fresh] 105,200 = 274,700 == T_out[fresh] 274,700 (fresh sends what it doesn't discard); Σ(sent into aggregate) 274,700 == A 274,700 (listeners to aggregate); A_sent 23 == S_in[victoria_metrics] 23 (aggregate to victoria_metrics) |
| `ledger.script` | SKIP | no [ledger] sut_script |
| `ledger.aggregate` | PASS | final life 4: Ab 274,700 == A 274,700 (every event absorbed) |
| `ledger.egress` | PASS | victoria_metrics: life 0: W − D − B − X − ΣT_disc − V 0 = residual 0 + Ab − V 0, ok; life 1: W − D − B − X − ΣT_disc − V 0 = residual 0 + Ab − V 0, ok; life 2 (killed): Ab 85,400 − V[victoria_metrics] 80,300 = 5,100, within band (band [-5,001, 15,002]: ingest 1,000/s over the last drain interval; below, the residual 0 plus one 5.00007s drain interval of ingest, absorbed after the last drain and flushed before the kill; above, one 10s aggregate window (configured) unflushed at the kill plus one drain interval of queue growth after the last drain); residual W − D − B − X − ΣT_disc − Ab 0 at its last drain, what the kill lost from the receive queue and inbox, by design, not judged here; D has no shutdown part: a SIGKILL counts no shutdown drops; life 3 (killed): Ab 89,600 − V[victoria_metrics] 84,700 = 4,900, within band (band [-5,100, 15,000]: ingest 1,000/s over the last drain interval; below, the residual 100 plus one 5.00001s drain interval of ingest, absorbed after the last drain and flushed before the kill; above, one 10s aggregate window (configured) unflushed at the kill plus one drain interval of queue growth after the last drain); residual W − D − B − X − ΣT_disc − Ab 100 at its last drain, what the kill lost from the receive queue and inbox, by design, not judged here; D has no shutdown part: a SIGKILL counts no shutdown drops; life 4 (final): Ab 274,700 − V[victoria_metrics] 274,700 = 0, ok (split copy; R 457,000 = 0 + 392 x 1,000 + 65 x 1,000) |
| `ledger.windows` | PASS | 1 series over 1 sink(s) x 5 life/lives, widest gap 10.001s (gaps <= 15s (1.5 x the 10s aggregate interval) or idle, with no window batches.sent from a + 7.5s to b - 2.5s (5s drains); a killed life's last sample within 11s (1.1 x) of the kill, a later life's first within 20s (2 x) of its start; 1 idle gap(s) excused: life 0 +59s to +120s (61.313s, 1 series), window sent 0 batch(es) over 0 drain(s) from +66s to +118s) |
| `ledger.replay` | PASS | victoria_metrics: life 3's first drain replayed 0 vs killed life 2's last-drain buffer.batches 0; victoria_metrics: life 4's first drain replayed 0 vs killed life 3's last-drain buffer.batches 0 (one batch in flight allowed) |
| `ledger.summary` | PASS | final life 4: uncounted 0 = W − D − E − B 0 + E − ΣT_disc − A 0 + A − Ab 0 + Ab − V[victoria_metrics] 0; the transforms' ΣT_disc 105,200, counted; wire G − (W + K + F) 200 over the run, F 0 counted; ledger.wire judges the rest; end to end over the run, G 891,300 less counted loss 155,700 plus the replay band's 12,777: victoria_metrics: V 725,300 <= G − counted + replay 748,377 |
| `ledger.sent` | SKIP | local target: ledger.egress judges delivery against the backends |
| `ledger.topic` | SKIP | victoria-metrics target: no topic |
| `ledger.standin` | SKIP | victoria-metrics target: no stand-in |
| `identity.sink` | PASS | final life 4 at +925s: victoria_metrics: received 23 vs delivered 23 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `drops` | PASS | no *.dropped count outside the ledger and the sink rows |
| `recovery` | PASS | 5 of 5 fault(s) judged at 1 sink(s), each recovered within recovery_bound 45s of its end; warmup rate 1,000/s |
| `file.reopen` | SKIP | the scenario has no rename step |
| `tls.served` | SKIP | the scenario has no probe step |
| `expect.files-rotated` | PASS | delta of logit.input.files.rotated{component=tail} on logit, run: run (+60s, +885s]: 131 (over 109 drain(s)), want >= 1 |
| `expect.no-resume-rejected` | PASS | delta of logit.input.files.resume_rejected{component=tail} on logit, run: run (+60s, +885s]: 0 (over 109 drain(s)), want <= 0 |
| `expect.no-checkpoint-errors` | PASS | delta of logit.input.checkpoint.errors{component=tail} on logit, run: run (+60s, +885s]: 0 (over 109 drain(s)), want <= 0 |
| `expect.timestamps-resolved` | PASS | delta of logit.transform.timestamp.resolved{component=stamp} on logit, run: run (+60s, +885s]: 821,300 (over 109 drain(s)), want >= 1 |
| `expect.no-timestamp-invalid` | PASS | delta of logit.transform.timestamp.skipped{component=stamp, reason=invalid} on logit, run: run (+60s, +885s]: 0 (over 109 drain(s)), want <= 0 |
| `expect.pause-discards` | PASS | delta of logit.component.events.discarded{component=fresh} on logit, after c0s1: c0s1 (+120s, +165s]: 10,100 (over 9 drain(s)), want >= 1 |
| `expect.stop-discards` | PASS | delta of logit.component.events.discarded{component=fresh} on logit, after c0s2: c0s2 (+270s, +315s]: 40,300 (over 8 drain(s)), want >= 38000 and <= 46000 |
| `expect.restart-keeps-fresh` | PASS | delta of logit.component.events.discarded{component=fresh} on logit, through c0s3: c0s3 (+330s, +405s]: 0 (over 15 drain(s)), want <= 0 |
| `expect.short-kill-keeps-fresh` | PASS | delta of logit.component.events.discarded{component=fresh} on logit, through c0s4: c0s4 (+420s, +495s]: 0 (over 8 drain(s)), want <= 0 |
| `expect.long-kill-discards` | PASS | delta of logit.component.events.discarded{component=fresh} on logit, after c0s5: c0s5 (+660s, +705s]: 105,200 (over 8 drain(s)), want >= 1 |

`drop_stale` discarded what the derivation gives, a rate of 1,000 lines a second times the time
each fault kept the tailer from reading, less `max_age`, plus what it replays:

| Fault | Unread | Expected | Run 1 | Run 2 |
|---|---|---|---|---|
| pause, 60 s | 60 s | 10,000 | 10,000 | 10,100 |
| stop, 90 s | 90 s plus the start | 40,000 plus 1,000 a second of start and catch-up | 40,300 | 40,300 |
| restart | a few seconds | 0 | 0 | 0 |
| kill, 30 s | 30 s, plus up to 5 s since the last checkpoint replayed | 0: the oldest replayed line is about 36 s old | 0 | 0 |
| kill, 150 s | 150 s plus up to 5 s replayed | about 105,000 | 105,200 | 105,200 |

So the 90 s stop's start and its catch-up past the backlog's last stale line took about 0.3 s.
Every other row read the discards as the design expected. `ledger.windows` excused only the
pause's gap, since every window after a fault held a backlog's fresh lines; `self_log` counted
no ERROR line, the throttled `stale_event` lines being WARN; and `drops` found nothing, since
`events.discarded` isn't a `*.dropped` count. The final life's ΣT_disc is the 150 s kill's
105,200, and the end-to-end bound counts all three faults' 155,700 with the counted loss.

`tail-rotate`, `perf/results/soak/20261018T025421Z`, passed every row under the changed ledger:
its nodes discard nothing, so ΣT_disc is 0 in every life and each row reads as in W11b's run.

### W14: HTTP ingress (2026-10-17)

`http-ingress` ran twice on the development host (`trout`, 24 cores, Linux 7.2.9), against
`logit:soak` built with `--no-cache` from `soak/w14`, whose diff touches no product code, over
`origin/main`'s head (`9bfc434f`). The first run, at `3f1407ac`, delayed the SUT's egress 6 s;
the second, at `a4051d77`, 11 s. The second is `perf/results/soak/20261018T025154Z`, exit 0,
every row PASS or SKIP, re-scored offline with `script/soak check` after `ledger.intake` came
to judge `influxdb_in`'s identities in every life, which changed only that row:

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 14 apply/revert action(s) returned 0; 3 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 4 exit(s) of the logit services, each a scheduled stop with code 0 or a scheduled kill with code 137 (1 kill(s)) |
| `restarts` | PASS | 4 start(s), each a scheduled start or restart |
| `self_log` | PASS | 8 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 78 readiness sample(s) read ok outside fault windows; 3 start(s) or unpause(s) ready in time, slowest ready 0.2s |
| `progress` | PASS | 9 30s window(s) outside fault windows, judged per sink, each freshness sample under 30s: victoria_metrics: deliveries in 9 of 9, 9 freshness sample(s); 240s of 1020s after warmup judged (24%; WARN under 5%) |
| `rss_slope` | PASS | generator PASS: MiB/h life 0 -0.5 (limit 64); 300s of 1020s after warmup judged (29%; WARN under 5%) \| logit PASS: MiB/h life 0 +54.4, life 3 +7.7 (limit 64); 285s of 1020s after warmup judged (28%; WARN under 5%) |
| `fd_slope` | PASS | generator PASS: warmup median 11 -> end 11 (limit +8) \| logit PASS: warmup median 13 -> end 13 (limit +8) |
| `ledger.wire` | PASS | G 1,745,200 (acknowledged of 2,172,600 attempted) = W 1,736,000 − X 0 + gap 9,200 (9,264 in reported windows, -92 in judged HTTP-affecting windows, 36 steady, -8 at the end; loss judged 99, each run at 0 or more, against limit 2,100); the generator's requests: 23 transport-error attempt(s), 9 answered 5xx and retried; the listener's busy 503s 9 and batches.deferred 9, a retry, not loss; its counted loss, not in G: 427,400 event(s) in 4,274 batch(es) (overflow_newest 4,274) |
| `ledger.intake` | PASS | final life 3: W − D 477,900 vs E + B 477,900; life 0: logit.input.requests{route=write} ok 10,325 + closed_consumer 0 == batches.sent 10,325; life 0: busy 9 == batches.deferred 9; life 1: logit.input.requests{route=write} ok 1,177 + closed_consumer 0 == batches.sent 1,177; life 1: busy 0 == batches.deferred 0; life 2 (killed): request identities not judged; final life 3: logit.input.requests{route=write} ok 4,779 + closed_consumer 0 == batches.sent 4,779; final life 3: busy 0 == batches.deferred 0; life 0: W − D − B − X − Ab 0 within [0, R]; life 1: W − D − B − X − Ab 0 within [0, R]; life 2: W − D − B − X − Ab 0 within [0, R] (R 132,000 = 0 + 67 x 1,000 + 65 x 1,000) |
| `ledger.edge` | PASS | final life 3: E[influx] 477,900 == T_in[count] 477,900 (into count); T_in[count] 477,900 == T_out[count] 477,900 (count forwards every event); Σ(sent into aggregate) 477,900 == A 477,900 (listeners to aggregate); A_sent 47,400 == S_in[victoria_metrics] 47,400 (aggregate to victoria_metrics) |
| `ledger.script` | SKIP | no [ledger] sut_script |
| `ledger.aggregate` | PASS | final life 3: Ab 477,900 == A 477,900 (every event absorbed) |
| `ledger.egress` | PASS | victoria_metrics: life 0: W − D − B − X − V 0 = residual 0 + Ab − V 0, ok; life 1: W − D − B − X − V 0 = residual 0 + Ab − V 0, ok; life 2 (killed): Ab 107,900 − V[victoria_metrics] 116,900 = -9,000, within band (band [-10,000, 11,000]: ingest 2,000/s over the last drain interval; below, the residual 0 plus one 4.99996s drain interval of ingest, absorbed after the last drain and flushed before the kill; above, one 0.5s aggregate window (configured) unflushed at the kill plus one drain interval of queue growth after the last drain); residual W − D − B − X − Ab 0 at its last drain, what the kill lost from the receive queue and inbox, by design, not judged here; D has no shutdown part: a SIGKILL counts no shutdown drops; life 3 (final): Ab 477,900 − V[victoria_metrics] 477,900 = 0, ok (split copy; R 132,000 = 0 + 67 x 1,000 + 65 x 1,000) |
| `ledger.windows` | SKIP | not enabled ([ledger] vm_every_window) |
| `ledger.replay` | PASS | victoria_metrics: life 3's first drain replayed 0 vs killed life 2's last-drain buffer.batches 1 (one batch in flight allowed) |
| `ledger.summary` | PASS | final life 3: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V[victoria_metrics] 0; wire G − (W + K + F) 9,200 over the run, F 0 counted; ledger.wire judges the rest; end to end over the run, G 1,745,200 less counted loss 0 plus a resend allowance 2,300: victoria_metrics: V 1,745,000 <= G − counted + resent 1,747,500 |
| `ledger.sent` | SKIP | local target: ledger.egress judges delivery against the backends |
| `ledger.topic` | SKIP | victoria-metrics target: no topic |
| `ledger.standin` | SKIP | victoria-metrics target: no stand-in |
| `identity.sink` | PASS | final life 3 at +1080s: victoria_metrics: received 473 vs delivered 472 + dropped 0 + buffer.batches 1, gap 0 (one batch in flight allowed) |
| `drops` | PASS | no *.dropped count outside the ledger and the sink rows |
| `recovery` | PASS | 7 of 7 fault(s) judged at 1 sink(s), each recovered within recovery_bound 45s of its end; warmup rate 2,000/s |
| `file.reopen` | SKIP | the scenario has no rename step |
| `tls.served` | SKIP | the scenario has no probe step |
| `expect.listener-defers` | PASS | delta of logit.input.batches.deferred{component=influx} on logit, through c0s4: c0s4 (+480s, +615s]: 9 (over 27 drain(s)), want >= 1 |
| `expect.generator-retries-503s` | PASS | delta of logit.output.requests{class=5xx, component=influx} on generator, through c0s4: c0s4 (+480s, +615s]: 9 (over 27 drain(s)), want >= 1 |
| `expect.generator-resends` | PASS | delta of logit.output.requests{class=network_error, component=influx} on generator, through c0s1: c0s1 (+60s, +166s]: 5 (over 21 drain(s)), want >= 1 |
| `expect.sink-never-drops` | PASS | delta of logit.component.batches.dropped{component=victoria_metrics} on logit, through c0s4: c0s4 (+480s, +615s]: 0 (over 27 drain(s)), want <= 0 |

- **The first run, `20261018T023228Z`, FAILed only `expect.generator-resends`, with nothing
  lost.** A 6 s delay holds `influxdb_out`'s answers under its 10 s timeout on the keep-alive
  connection it reuses, so no attempt ended in a transport error. At 11 s the request in flight
  timed out after the SUT took it, and the generator's next attempts each opened a connection
  whose handshake timed out first: 5 transport errors, a surplus of 102 points, one batch,
  against an allowance of 700. Every other row of the first run read as the second's.
- **A busy `503` lost nothing.** The 90 s VictoriaMetrics stop backed the sink's two-batch
  queue, its inbox, `aggregate`'s, and `count`'s up to the listener, which answered 9 busy
  `503`s, each one `batches.deferred` (8 and 8 in the first run). The generator counted 9
  `5xx` answers and resent each batch. A backend stop doesn't affect the wire, so the stop sat
  in a steady run, which gapped by −2 points, and the sink dropped nothing.
- **A graceful stop and a restart lost nothing, and their surplus stayed inside the
  allowance.** The stop's window gapped by 38 points and the restart's by −27, against
  allowances of 1,000 and 400, which its refused dials, counted `network_error`, inflate. The
  listener answered what it took before it exited, so G never exceeded W.
- **A kill's window shows 9,268 points it can't split** (9,185 in the first run): a SIGKILL
  loses up to one `internal` interval of the SUT's counters, about 10,000 points at 2,000 a
  second, so the killed life's W misses points the generator saw acknowledged. The same
  unexported interval shows in `ledger.egress` as V 9,000 above the killed life's Ab, inside
  its band.
- **`influxdb_in`'s request identities held in every life a kill didn't end**: `ok` write
  requests equal to `batches.sent` (10,325, 1,177, and 4,779 in lives 0, 1, and 3), and the
  backend stop's 9 busy `503`s equal to life 0's 9 `batches.deferred`, with no
  `closed_consumer` answer in either run. The killed life 2 is reported unjudged.
- 30% loss on the SUT's egress and 10% on the generator's made no transport error: TCP
  retransmitted inside the writer's timeout.
- `ledger.summary`'s end-to-end bound held: V 1,745,000 against G 1,745,200 plus a resend
  allowance of 2,300.
- No product finding.

### W13b: fan-out transforms (2026-10-18)

`otlp-fan-out` ran three times on the development host (`trout`, 24 cores, Linux 7.2.9), against
the `logit:soak` image `sha256:68c3d292…` built 2026-10-17 19:29 PDT from `soak/w14`; no product
code changed since `origin/main`'s `9bfc434f`, and `line_protocol`, `otlp_in`, and `otlp_out` are
all on `main`. The third run, `perf/results/soak/20261018T043911Z`, from `soak/w13b` at
`fd3cab7c`, exit 0, every row PASS or SKIP. The table is that run re-scored offline with
`script/soak check` after `ledger.wire`'s boundary rule replaced a merge of adjacent judged
windows; only `ledger.wire`'s detail changed, from a loss judged of 82 to 582 against a limit
of 650:

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 14 apply/revert action(s) returned 0; 2 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 3 exit(s) of the logit services, each a scheduled stop with code 0 |
| `restarts` | PASS | 2 start(s), each a scheduled start or restart |
| `self_log` | PASS | 7 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 53 readiness sample(s) read ok outside fault windows; 2 start(s) or unpause(s) ready in time, slowest ready 0.2s |
| `progress` | PASS | 7 30s window(s) outside fault windows, judged per sink, each freshness sample under 30s: victoria_metrics: deliveries in 7 of 7, 7 freshness sample(s); 180s of 900s after warmup judged (20%; WARN under 5%) |
| `rss_slope` | PASS | generator PASS: MiB/h life 0 +1.6 (limit 64); 200s of 900s after warmup judged (22%; WARN under 5%) \| logit PASS: MiB/h life 1 +3.4 (limit 64); 190s of 900s after warmup judged (21%; WARN under 5%) |
| `fd_slope` | PASS | generator PASS: warmup median 11 -> end 11 (limit +8) \| logit PASS: warmup median 13 -> end 14 (limit +8) |
| `ledger.wire` | PASS | G 381,500 (acknowledged of 483,150 attempted) = W 381,650 − X 0 + gap -150 (0 in reported windows, -5 in judged HTTP-affecting windows, -114 steady, -31 at the end; loss judged 582, each run at 0 or more, against limit 650); the generator's requests: 22 transport-error attempt(s), 0 answered 5xx and retried; its counted loss, not in G: 101,650 event(s) in 2,033 batch(es) (overflow_newest 2,033) |
| `ledger.intake` | PASS | final life 1: W − D 128,700 vs E + B 128,700; final life 1: otlp_in counts no requests: W − D == E + B alone; life 0: W′ − Ab 0 within [0, R] (R 788,000 = (0 + 132 x 1,000) x 4 + 65 x 1,000 x 4; W′ is each listener's W − D − B − X carried through its chain's fan_out and discards) |
| `ledger.edge` | PASS | final life 1: E[otlp] 128,700 == T_in[points] 128,700 (into points); children[points] = T_out 514,800 − (T_in 128,700 − T_disc 128,700) = 514,800 == 4 x T_in − T_lines[points] 514,800 (points emits 4 per event); T_out[points] 514,800 == T_in[count] 514,800 (into count); T_in[count] 514,800 − T_disc[count] 0 = 514,800 == T_out[count] 514,800 (count sends what it doesn't discard); Σ(sent into aggregate) 514,800 == A 514,800 (listeners to aggregate); A_sent 19,840 == S_in[victoria_metrics] 19,840 (aggregate to victoria_metrics) |
| `ledger.script` | SKIP | no [ledger] sut_script |
| `ledger.aggregate` | PASS | final life 1: Ab 514,800 == A 514,800 (every event absorbed) |
| `ledger.egress` | PASS | victoria_metrics: life 0: W′ − V 0 = residual 0 + Ab − V 0, ok; life 1 (final): Ab 514,800 − V[victoria_metrics] 514,800 = 0, ok (split copy; R 788,000 = (0 + 132 x 1,000) x 4 + 65 x 1,000 x 4; W′ is each listener's W − D − B − X carried through its chain's fan_out and discards) |
| `ledger.windows` | SKIP | not enabled ([ledger] vm_every_window) |
| `ledger.replay` | SKIP | no killed life followed by another |
| `ledger.summary` | PASS | final life 1: uncounted 0 = W − D − E − B 0 + ledger.edge's chain terms 0 + Σ(sent into aggregate) − A 0 + A − Ab 0 + Ab − V[victoria_metrics] 0; wire G − (W + K + F) -150 over the run, F 0 counted; ledger.wire judges the rest; end to end over the run, G 381,500 less counted loss 0 plus a resend allowance 1,100, carried through the chains' fan_out and discards to 1,530,400: victoria_metrics: V 1,526,600 <= (G − counted + resent) carried through the chains 1,530,400 |
| `ledger.sent` | SKIP | local target: ledger.egress judges delivery against the backends |
| `ledger.topic` | SKIP | victoria-metrics target: no topic |
| `ledger.standin` | SKIP | victoria-metrics target: no stand-in |
| `identity.sink` | PASS | final life 1 at +960s: victoria_metrics: received 496 vs delivered 495 + dropped 0 + buffer.batches 1, gap 0 (one batch in flight allowed) |
| `drops` | PASS | no *.dropped count outside the ledger and the sink rows |
| `recovery` | PASS | 7 of 7 fault(s) judged at 1 sink(s), each recovered within recovery_bound 45s of its end; warmup rate 500/s |
| `file.reopen` | SKIP | the scenario has no rename step |
| `tls.served` | SKIP | the scenario has no probe step |
| `expect.sink-queue-fills` | PASS | max of logit.component.buffer.batches{component=victoria_metrics} on logit, during c0s4: c0s4 (+450s, +540s]: 2 (1 sample(s) and the value in force at its start), want >= 2 |
| `expect.sink-never-drops` | PASS | delta of logit.component.batches.dropped{component=victoria_metrics} on logit, through c0s4: c0s4 (+450s, +585s]: 0 (over 24 drain(s)), want <= 0 |
| `expect.generator-resends` | PASS | delta of logit.output.requests{class=network_error, component=otlp} on generator, through c0s5: c0s5 (+570s, +645s]: 6 (over 15 drain(s)), want >= 1 |
| `expect.no-lines-dropped` | PASS | delta of logit.transform.lines.dropped{component=points} on logit, run: run (+60s, +960s]: 0 (over 174 drain(s)), want <= 0 |

- **The fan-out term held to the event.** `points` received 128,700 records in the final life
  and discarded each once its four points were out, so its children, T_out − (T_in − T_disc),
  were 514,800, 4 × T_in with no line dropped, and `aggregate` absorbed and VictoriaMetrics
  stored the same 514,800. The stopped life 0 drained to a residual of 0. End to end, V was
  1,526,600 against G 381,500 plus a resend allowance of 1,100, carried through the fan-out to
  1,530,400.
- **The first two runs found two `ledger.wire` defects, both in the HTTP wire's judgment, fixed
  here.** `perf/results/soak/20261018T034333Z`, from a prototype of this branch, FAILed 1,775
  points lost inside the SUT's partition and a 1,713-point surplus after it: the generator's
  `events.delivered` exported no point for the drains the partition stalled, so the backlog
  it delivered after the revert was interpolated back across the stall. G is now anchored at
  every generator drain. `perf/results/soak/20261018T041409Z`, at `c242f67e`, FAILed a
  140-point surplus in a 20 s run between two overlapping netem windows: the 30% loss on the
  SUT's egress delays answers in bursts, and a run boundary between two judged windows had no
  edge to allow the interpolation's error there. A boundary between two judged runs now nets
  the part of a surplus past its run's own allowance with the loss across it, up to the G that
  moved over the generator interval straddling it, and every edge of a judged run allows a
  batch of surplus. In the third run the SUT's restart at +660 shows the pair at its largest:
  −1,890 before the +665 boundary and +1,830 after it. Of the 1,390 past that run's allowance of
  500, 51 netted at +629 and the remaining 1,339 at +665, against 4,950 of G in the straddling
  interval; the 492 of loss left counts in the pool. All three runs, and `http-ingress`'s
  `20261018T025154Z`, re-score PASS on every row with `script/soak check`.
- **The boundary rule leaves phantom loss in the pool, so a correct rerun can FAIL
  `ledger.wire`.** A pure interpolation pair, surplus S beside loss L in a run with allowance
  A, nets only S − A, so about A − (S − L) of loss stays in the pool: 492 in this run, at 582 of
  650. Each extra transport-error attempt in the surplus run adds a batch to A without raising
  the limit, so a rerun can FAIL by a batch or two with nothing lost; read such a FAIL against
  its boundary detail line before calling it loss.
- **The generator lost a fifth of its records, counted.** `otlp_out` sends one request at a
  time, so its rate is a batch per round trip: under the SUT's 200 ms egress delay it
  dropped about half its batches (`overflow_newest`), fewer under the 30% egress loss, and
  through every outage all of them, 101,650 records over the run, none of them in G. A known-gaps candidate
  for `otlp_out` on a slow link, not a ledger finding.
- **The backend stop backed up to the generator, as `otlp_in` has no busy answer.** The sink's
  two-batch queue filled, the inboxes behind it held batches of four times the events, and
  `otlp_in` held each request past `otlp_out`'s 10 s timeout: 3 transport errors and 337
  batches' overflow on the generator through the stop, no sink drop, and `rss_slope` flat at +3.4 MiB/h.
- The pause, the stop, and the partition gave 19 transport-error attempts between them, and
  each judged run's surplus stayed inside its allowance once the boundaries netted.
- No product finding.

### W17: Kafka SASL re-authentication (2026-10-18)

Run `20261018T051509Z`: `kafka-sasl-reauth` at its own 8 minutes 30 seconds, from `soak/w17` at
`de32105b` with the scenario and its documentation uncommitted, on the development host, against
`logit:soak-w17` (`sha256:ff0effb78cba`, built from that tree, since the branch changes
`kafka_out`) and `apache/kafka:4.2.0` (`sha256:9516fb7634ba`). Overall PASS: every row passed
but `ledger.windows` and `ledger.replay`, which SKIP on a Kafka target with no kill.

- **Nothing was lost or stored twice.** The generator sent 1,032,000 lines, all absorbed, and the
  topic held 2,590 distinct records, every one the sink saw accepted, whose increments summed to
  1,032,000. The sink delivered all 259 batches and dropped none.
- **The sink redialed 17 times in the 450 s run window, 136 an hour.** While the broker
  answered, a redial came every 20 to 25 s at the 5 s drain resolution, about 23 s on average:
  the 20 s deadline, then the next 2 s produce slot and the dial. Each fault ended the connection
  and restarted the clock: the restart's redial at +94 s, the pause's at +209 s, and the stop's at
  +359 s, two in one drain, the second most likely the bootstrap connection on the metadata
  refresh the stop's `NotLeaderOrFollower` asked for.
  The three up-segments, 71 s, 76 s, and 151 s, predict 4, 4, and 8 dials with the segment model
  in the scenario's comment, which is what the run counted.
- **The bounds were widened after the run.** It ran under `[120, 300]`, from a first derivation
  that added the forced redials to the renewals. A forced redial restarts the renewal clock
  instead, so the scenario's comment re-derives the floor from the up-segments, 96 an hour, and
  ships `[80, 300]`. That band catches a sink that redials before every send, about 1,800 an
  hour, but not one that never renews: the broker closes an expired session's connection on its
  next request without answering it (Kafka 4.0.0's `SocketServer`
  `Processor.processCompletedReceives()`), the sink reads that as `Ambiguous` and redials, and
  those redials come every 30 to 33 s of broker uptime, 90 to 120 an hour. So the scenario adds
  `no-expired-session-sends`: the `ambiguous` rate over the run at most 40 an hour, against at
  most one per fault, 28 an hour, for a sink that renews, and 83 or more for one that doesn't.
  Re-scored offline (`soak.py check` on a copy of the run directory carrying the new
  expectations), it reads 1 `ambiguous` attempt, the pause's, 8 an hour, and every row still
  passes.
- **No attempt was `refused`.** The restart kept the SCRAM credential, which
  `kafka-storage.sh format --ignore-formatted` leaves in the metadata log. Of 274 attempts, 14
  were `clean` and 1 `ambiguous`; the self-log's throttled lines name a connection refused in the
  restart, a response timeout in the pause, a `NotLeaderOrFollower` as the stop began, and the
  broker's name gone from DNS during it.
- **The read-back took 5 s** after the SUT stopped, for 2,590 records. At ten series an 8-hour
  run holds 144,000 records, which at `kafka-broker-restart`'s observed rate reads well inside
  the driver's 180 s bound, so the scenario's `max_duration` is 8 hours.
- **RSS slopes** were +9.8 MiB/h (SUT) and −0.2 MiB/h (generator) over 370 s, and file
  descriptors went from 13 to 12 (SUT) and 12 to 11 (generator), so no redial left a
  descriptor behind.

Run `20261018T153250Z`: `kafka-sasl-reauth` at `--duration 1h`, from `main` at `c3ca0430` on
`logit:soak-long` (`sha256:f962a632…`), beside the `tail-rotate-delete` run in W18, on the
development host. Overall PASS, with only inapplicable SKIPs.

- **The sink renewed on schedule.** `reauth-rate` read 135.3 an hour (133 reconnects over
  3,540 s), inside [80, 300], and `no-expired-session-sends` read 9.2 an hour against a limit
  of 40. No attempt was `refused` and no batch was dropped.
- **Nothing was lost or stored twice.** G, W, and V are each 7,212,100. `ledger.topic` read
  18,040 distinct records, 0 duplicate copies, and T − A of 0.
- **Memory and descriptors stayed flat.** The SUT's `rss_slope` read +0.8 MiB/h and its
  descriptors went from 13 to 13. All 27 faults recovered within the 45 s bound, the slowest
  in 19 s.
- **`rss_slope` judged 29% of the time after warmup.** A slope needs steady samples, and the
  scenario's 9 cycles of 3 faults each hold the broker away or restart it, so the faults and the
  45 s recovery after each leave about 113 s of every 390 s cycle. The SUT is never restarted,
  so its one life carries the slope. Coverage is above the 5% WARN floor.

The 1 h runs, here and in W18, are shorter than the 4-hour or 8-hour length the scenarios allow.
A leak in either path grows per redial or per deletion, not with wall time, and an hour is the
shortest run whose `rss_slope` gives a verdict instead of a WARN.

### W18: a deleting rotation (2026-10-18)

`tail-rotate-delete` ran for 20 minutes on the development host, against `logit:soak-w18`
built from `soak/w18`, whose Rust is `main`'s at `8b99d6db`. The run is
`perf/results/soak/20261018T141700Z`, exit 0, shown here as `check` re-scores it after the
merge of `main` at `fa26e870`, which changed nothing in it. A 1-hour run follows the 20-minute
run's table; the scenario's default is 4 hours.

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 10 apply/revert action(s) returned 0; 5 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 5 exit(s) of the logit services, each a scheduled stop with code 0 or a scheduled kill with code 137 (2 kill(s)) |
| `restarts` | PASS | 4 start(s), each a scheduled start or restart |
| `self_log` | PASS | 0 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 134 readiness sample(s) read ok outside fault windows; 5 start(s) or unpause(s) ready in time, slowest ready 0.2s |
| `progress` | PASS | 19 30s window(s) outside fault windows, judged per sink, each freshness sample under 30s: victoria_metrics: deliveries in 19 of 19, 19 freshness sample(s); 540s of 1140s after warmup judged (47%; WARN under 5%) |
| `rss_slope` | PASS | generator PASS: MiB/h life 0 +0.6 (limit 64); 590s of 1140s after warmup judged (52%; WARN under 5%) \| logit PASS: MiB/h life 4 -265.1 (limit 64); 545s of 1140s after warmup judged (48%; WARN under 5%) |
| `fd_slope` | PASS | generator PASS: warmup median 11 -> end 11 (limit +10) \| logit PASS: warmup median 16 -> end 23 (limit +10) |
| `ledger.wire` | PASS | G 1,206,200 (shared directory: 324,200 lines in 8 files, the writer having deleted 20) vs W 1,206,000 + counted 0: W + F − G -200 against the replay band [-10,000, 12,777]; the tailers stopped short at the end with W 1,206,000 |
| `ledger.intake` | PASS | final life 4: W − D 754,900 vs E + B 754,900; life 0: W − D − B − X − ΣT_disc − Ab 0 within [0, R]; life 1: W − D − B − X − ΣT_disc − Ab 0 within [0, R]; life 2: W − D − B − X − ΣT_disc − Ab 0 within [0, R]; life 3: W − D − B − X − ΣT_disc − Ab 100 within [0, R] (R 327,000 = 0 + 262 x 1,000 + 65 x 1,000) |
| `ledger.edge` | PASS | final life 4: E[tail] 754,900 == T_in[parse] 754,900 (into parse); T_in[parse] 754,900 − T_disc[parse] 0 = 754,900 == T_out[parse] 754,900 (parse sends what it doesn't discard); T_out[parse] 754,900 == T_in[labels] 754,900 (into labels); T_in[labels] 754,900 − T_disc[labels] 0 = 754,900 == T_out[labels] 754,900 (labels sends what it doesn't discard); T_out[labels] 754,900 == T_in[count] 754,900 (into count); T_in[count] 754,900 − T_disc[count] 0 = 754,900 == T_out[count] 754,900 (count sends what it doesn't discard); T_out[count] 754,900 == T_in[metrics_only] 754,900 (into metrics_only); T_in[metrics_only] 754,900 − T_disc[metrics_only] 0 = 754,900 == T_out[metrics_only] 754,900 (metrics_only sends what it doesn't discard); Σ(sent into aggregate) 754,900 == A 754,900 (listeners to aggregate); A_sent 61 == S_in[victoria_metrics] 61 (aggregate to victoria_metrics) |
| `ledger.script` | SKIP | no [ledger] sut_script |
| `ledger.aggregate` | PASS | final life 4: Ab 754,900 == A 754,900 (every event absorbed) |
| `ledger.egress` | PASS | victoria_metrics: life 0: W − D − B − X − ΣT_disc − V 0 = residual 0 + Ab − V 0, ok; life 1: W − D − B − X − ΣT_disc − V 0 = residual 0 + Ab − V 0, ok; life 2 (killed): Ab 85,300 − V[victoria_metrics] 80,300 = 5,000, within band (band [-5,000, 15,001]: ingest 1,000/s over the last drain interval; below, the residual 0 plus one 4.99957s drain interval of ingest, absorbed after the last drain and flushed before the kill; above, one 10s aggregate window (configured) unflushed at the kill plus one drain interval of queue growth after the last drain); residual W − D − B − X − ΣT_disc − Ab 0 at its last drain, what the kill lost from the receive queue and inbox, by design, not judged here; D has no shutdown part: a SIGKILL counts no shutdown drops; life 3 (killed): Ab 89,700 − V[victoria_metrics] 84,800 = 4,900, within band (band [-5,100, 15,001]: ingest 1,000/s over the last drain interval; below, the residual 100 plus one 4.99989s drain interval of ingest, absorbed after the last drain and flushed before the kill; above, one 10s aggregate window (configured) unflushed at the kill plus one drain interval of queue growth after the last drain); residual W − D − B − X − ΣT_disc − Ab 100 at its last drain, what the kill lost from the receive queue and inbox, by design, not judged here; D has no shutdown part: a SIGKILL counts no shutdown drops; life 4 (final): Ab 754,900 − V[victoria_metrics] 754,900 = 0, ok (split copy; R 327,000 = 0 + 262 x 1,000 + 65 x 1,000) |
| `ledger.windows` | PASS | 1 series over 1 sink(s) x 5 life/lives, widest gap 10.003s (gaps <= 15s (1.5 x the 10s aggregate interval) or idle, with no window batches.sent from a + 7.5s to b - 2.5s (5s drains); a killed life's last sample within 11s (1.1 x) of the kill, a later life's first within 20s (2 x) of its start; 1 idle gap(s) excused: life 0 +59s to +120s (61.137s, 1 series), window sent 0 batch(es) over 0 drain(s) from +66s to +118s) |
| `ledger.replay` | PASS | victoria_metrics: life 3's first drain replayed 0 vs killed life 2's last-drain buffer.batches 0; victoria_metrics: life 4's first drain replayed 0 vs killed life 3's last-drain buffer.batches 0 (one batch in flight allowed) |
| `ledger.summary` | PASS | final life 4: uncounted 0 = W − D − E − B 0 + E − ΣT_disc − A 0 + A − Ab 0 + Ab − V[victoria_metrics] 0; the transforms' ΣT_disc 0, counted; wire G − (W + K + F) 200 over the run, F 0 counted; ledger.wire judges the rest; end to end over the run, G 1,206,200 less counted loss 0 plus the replay band's 12,777: victoria_metrics: V 1,196,000 <= G − counted + replay 1,218,977 |
| `ledger.sent` | SKIP | local target: ledger.egress judges delivery against the backends |
| `ledger.topic` | SKIP | victoria-metrics target: no topic |
| `ledger.standin` | SKIP | victoria-metrics target: no stand-in |
| `identity.sink` | PASS | final life 4 at +1240s: victoria_metrics: received 61 vs delivered 61 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `drops` | PASS | no *.dropped count outside the ledger and the sink rows |
| `recovery` | PASS | 5 of 5 fault(s) judged at 1 sink(s), each recovered within recovery_bound 45s of its end; warmup rate 1,000/s |
| `file.reopen` | SKIP | the scenario has no rename step |
| `tls.served` | SKIP | the scenario has no probe step |
| `expect.files-rotated` | PASS | delta of logit.input.files.rotated{component=tail} on logit, run: run (+60s, +1200s]: 114 (over 184 drain(s)), want >= 1 |
| `expect.no-checkpoint-errors` | PASS | delta of logit.input.checkpoint.errors{component=tail} on logit, run: run (+60s, +1200s]: 0 (over 184 drain(s)), want <= 0 |
| `expect.files-open-bounded` | PASS | max of logit.input.files.open{component=tail} on logit, run: run (+60s, +1200s]: 9 (173 sample(s) and the value in force at its start), want <= 10 |
| `expect.fds-bounded` | PASS | max of logit.process.fds on logit, run: run (+60s, +1200s]: 24 (184 sample(s) and the value in force at its start), want <= 26 |
| `expect.files-open-plateau` | PASS | last of logit.input.files.open{component=tail} on logit, run: run (+60s, +1200s]: 8 (173 sample(s) and the value in force at its start), want >= 7 |
| `expect.writer-deletes` | PASS | delta of logit.output.file.rotations{component=file} on generator, run: run (+60s, +1200s]: 26 (over 228 drain(s)), want >= 8 |
| `expect.watches-bounded` | PASS | max of logit.input.watch.watches{component=tail} on logit, run: run (+60s, +1200s]: 10 (173 sample(s) and the value in force at its start), want <= 11 |

- **The writer deleted 20 files, and nothing unread.** It rotated 27 times, 26 after warmup,
  and kept eight files. G is 1,206,200 and the shared directory held 324,200 lines in 8 files,
  so 882,000 lines went with the deleted files, against about 883,942 that 20 files of 4 MiB
  hold at the surviving files' 94.9 bytes a line. W − G is −200, as in W11b's runs: each kill
  loses about one drain interval of counts and replays about as much. The 150 s kill spanned
  three rotations, half the retention guard's six.
- **Descriptors level off at the writer's retention.** `logit.input.files.open` rose from 2
  at the end of warmup to 9 at most and ended at 8; `logit.input.watch.watches` peaked at 10;
  the SUT's `logit.process.fds` went from a warmup median of 16 to 24 at most and 23 at the
  end. `fds` less `files.open` was 15 at nearly every drain, so the process holds 15
  descriptors besides its files, and `fds-bounded`'s 26 is 15 plus `files-open-bounded`'s 10
  plus one.
- **The wait for G held 200 short.** The end sequence read G from the writer's chunk, and the
  tailers' W stopped at 1,206,000 after four reads, the kills' net −200. Against the shared
  count of 324,200 it would have returned at its first read.
- **No `resume_rejected` in this run**: no inode was freed and reused while the SUT was down.
  The scenario still drops `tail-rotate`'s expectation of none, since a deletion during a
  longer fault can free one.
- **An earlier attempt, `20261018T053350Z`, ran while the host suspended** on a lid close for
  8.5 hours: `systemd-inhibit --what=sleep:idle` doesn't block `handle-lid-switch`. `run`
  FAILed it as stalled. A run the host slept through ends at its first stall and exits 3,
  and is rerun once the host is awake rather than kept awake (§5).

Run `20261018T153308Z`: `tail-rotate-delete` at `--duration 1h`, from `main` at `c3ca0430` on
`logit:soak-long` (`sha256:f962a632…`), beside the `kafka-sasl-reauth` run in W17, on the
development host. Overall PASS, with only inapplicable SKIPs. It confirms the 20-minute run's
limits.

- **The writer deleted 74 files, and nothing unread.** G is 3,606,200 and the shared directory
  held 342,800 lines in 8 files. W + F − G is −100, inside the replay band
  [−9,999, 12,775]. The 5 SUT exits were all scheduled: 3 stops and 2 kills.
- **Descriptors stayed at the writer's retention.** `logit.input.files.open` peaked at 9 and
  ended at 8, `logit.input.watch.watches` peaked at 10, and `logit.process.fds` peaked at 24
  again, under `fds-bounded`'s 26, which the 20-minute run set. The SUT's `fd_slope` row reads
  16 to 23 against a limit of +10.
- **RSS fell.** `rss_slope` read −3.9 MiB/h over the SUT's last life, with 83% of the time after
  warmup judged.

### W15: multi-destination routers under a target split (2026-10-18)

`route-copy` and `route-signals` ran once each, side by side, on the development host (`trout`,
24 cores, Linux 7.2.9), against the `logit:soak` image `sha256:30b450da18b2…` built
2026-10-18 08:07 PDT with `--no-cache` from `soak/w15` after its merge of `origin/main`
(`fdc4eb8c`), so the binary holds every product change through W17; `route`, `route_signals`,
and `target` are unchanged since `4b86ccc7`. Both ran from `soak/w15` at `e824297f`, exit 0,
Overall PASS, every row PASS or SKIP.

- **The router balances with no tolerance.** `route-copy`'s router read R_in 768,525 and
  R_copied 384,269, one copy per shard-0 line, against ΣT_sent 1,152,794, and `route-signals`'s
  read R_in 828,381 and R_copied 828,381, one copy per line since every event carries a log and
  a metric, against ΣT_sent 1,656,762. Neither discarded an event, and `route-copy`'s unrouted
  consumer received none.
- **Each sink balances from its own target.** In the stopped life each sink's
  T_sent − T_x − V was 0, and so was the listeners' side up to the router. `route-signals`' kill
  left each branch inside its own band, 10,200 at `vm_logs`, one aggregate window.
- **A stop of one backend didn't hold the other branch.** With `vm_metrics`' backend down 90 s,
  `vm_logs` delivered every window: the stopped sink's queue held its windows, so nothing backed
  up to the router, which waits on a full inbox at any one target before it serves the next.

`route-copy`, `perf/results/soak/20261018T150812Z`:

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 6 apply/revert action(s) returned 0; 1 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 2 exit(s) of the logit services, each a scheduled stop with code 0 |
| `restarts` | PASS | 2 start(s), each a scheduled start or restart |
| `self_log` | PASS | 3 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 56 readiness sample(s) read ok outside fault windows; 1 start(s) or unpause(s) ready in time, slowest ready 0.2s |
| `progress` | PASS | 7 30s window(s) outside fault windows, judged per sink, each freshness sample under 30s: vm_a: deliveries in 7 of 7, 7 freshness sample(s); vm_b: deliveries in 7 of 7, 7 freshness sample(s); 180s of 540s after warmup judged (33%; WARN under 5%) |
| `rss_slope` | PASS | generator PASS: MiB/h life 0 +7.6 (limit 64); 210s of 540s after warmup judged (39%; WARN under 5%) \| logit PASS: MiB/h life 1 +1.8 (limit 64); 200s of 540s after warmup judged (37%; WARN under 5%) |
| `fd_slope` | PASS | generator PASS: warmup median 12 -> end 11 (limit +8) \| logit PASS: warmup median 13 -> end 13 (limit +8) |
| `ledger.wire` | PASS | G 1,213,300 = W 1,201,225 + K 0 + wire 12,075 (by design: 12,095 in UDP-affecting windows, -76 steady, 56 at the end; steady loss judged 1, each run at 0 or more, against limit 500); the generator's counted drop_newest loss, not in G: 0 event(s) in 0 batch(es) |
| `ledger.intake` | PASS | final life 1: W − D 768,525 vs E + B 768,525; life 0 (stopped): W − D − B − X − R_in 0 (R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.edge` | PASS | final life 1: E 768,525 == R_in[split] 768,525 (listeners to router split); R_in 768,525 + R_copied 384,269 − R_disc 0 = 1,152,794 == R_fwd + ΣT_sent 1,152,794 (router split sends each event to every destination it names); T_sent[tier_a] − T_x[tier_a] 384,269 == A[window_a] 384,269 (into window_a); A_sent[window_a] 2,000 == S_in[vm_a] 2,000 (window_a to vm_a); T_sent[tier_b] − T_x[tier_b] 768,525 == A[window_b] 768,525 (into window_b); A_sent[window_b] 4,000 == S_in[vm_b] 4,000 (window_b to vm_b) |
| `ledger.script` | SKIP | no [ledger] sut_script |
| `ledger.aggregate` | PASS | final life 1: window_a: Ab 384,269 == A 384,269; window_b: Ab 768,525 == A 768,525 (every event absorbed) |
| `ledger.egress` | PASS | vm_a: life 0: T_sent[tier_a] − T_x − V 0 = residual 0 + Ab[window_a] − V 0, ok; life 1 (final): Ab[window_a] 384,269 − V[vm_a] 384,269 = 0, ok \| vm_b: life 0: T_sent[tier_b] − T_x − V 0 = residual 0 + Ab[window_b] − V 0, ok; life 1 (final): Ab[window_b] 768,525 − V[vm_b] 768,525 = 0, ok (split target; R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.windows` | SKIP | not enabled ([ledger] vm_every_window) |
| `ledger.replay` | SKIP | no killed life followed by another |
| `ledger.summary` | PASS | final life 1: uncounted 0, each term judged on its own: Ab[window_a] − V[vm_a] 0; Ab[window_b] − V[vm_b] 0; the other 9 term(s) 0; wire G − (W + K + F) 12,075 over the run; ledger.wire's |
| `ledger.sent` | SKIP | local target: ledger.egress judges delivery against the backends |
| `ledger.topic` | SKIP | victoria-metrics target: no topic |
| `ledger.standin` | SKIP | victoria-metrics target: no stand-in |
| `identity.sink` | PASS | final life 1 at +630s: vm_a: received 40 vs delivered 40 + dropped 0 + buffer.batches 0, gap 0; vm_b: received 40 vs delivered 40 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `drops` | PASS | no *.dropped count outside the ledger and the sink rows |
| `recovery` | PASS | 3 of 3 fault(s) judged at 2 sink(s), each recovered within recovery_bound 45s of its end; warmup rate 2,000/s |
| `file.reopen` | SKIP | the scenario has no rename step |
| `tls.served` | SKIP | the scenario has no probe step |
| `expect.b-delivers-while-a-stopped` | PASS | delta of logit.component.batches.delivered{component=vm_b} on logit, during c0s1: c0s1 (+60s, +150s]: 9 (over 18 drain(s)), want >= 6 |
| `expect.a-queues-while-stopped` | PASS | max of logit.component.buffer.batches{component=vm_a} on logit, during c0s1: c0s1 (+60s, +150s]: 9 (10 sample(s) and the value in force at its start), want >= 5 |
| `expect.a-drops-nothing` | PASS | delta of logit.component.batches.dropped{component=vm_a} on logit, through c0s1: c0s1 (+60s, +195s]: 0 (over 27 drain(s)), want <= 0 |
| `expect.router-copies-shard-0` | PASS | delta of logit.component.events.copied{component=split} on logit, during c0s1: c0s1 (+60s, +150s]: 90,000 (over 18 drain(s)), want >= 50000 |
| `expect.nothing-unrouted` | PASS | delta of logit.component.events.received{component=unrouted} on logit, run: run (+60s, +600s]: 0 (over 108 drain(s)), want <= 0 |

`route-signals`, `perf/results/soak/20261018T150821Z`:

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 8 apply/revert action(s) returned 0; 2 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 4 exit(s) of the logit services, each a scheduled stop with code 0 or a scheduled kill with code 137 (1 kill(s)) |
| `restarts` | PASS | 3 start(s), each a scheduled start or restart |
| `self_log` | PASS | 4 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 77 readiness sample(s) read ok outside fault windows; 2 start(s) or unpause(s) ready in time, slowest ready 0.2s |
| `progress` | PASS | 10 30s window(s) outside fault windows, judged per sink, each freshness sample under 30s: vm_logs: deliveries in 10 of 10, 10 freshness sample(s); vm_metrics: deliveries in 10 of 10, 10 freshness sample(s); 270s of 720s after warmup judged (37%; WARN under 5%) |
| `rss_slope` | PASS | generator PASS: MiB/h life 0 +1.8 (limit 64); 310s of 720s after warmup judged (43%; WARN under 5%) \| logit PASS: MiB/h life 2 +22.6 (limit 64); 225s of 720s after warmup judged (31%; WARN under 5%) |
| `fd_slope` | PASS | generator PASS: warmup median 11 -> end 12 (limit +8) \| logit PASS: warmup median 13 -> end 13 (limit +8) |
| `ledger.wire` | PASS | G 1,573,400 = W 1,551,381 + K 0 + wire 22,019 (by design: 21,992 in UDP-affecting windows, -64 steady, 91 at the end; steady loss judged 1, each run at 0 or more, against limit 700); the generator's counted drop_newest loss, not in G: 0 event(s) in 0 batch(es) |
| `ledger.intake` | PASS | final life 2: W − D 828,381 vs E + B 828,381; life 0 (stopped): W − D − B − X − ΣT_disc − R_in 0; life 1: W − D − B − X − ΣT_disc − R_in 0 within [0, R] (R 272,000 = 10,000 + 197 x 1,000 + 65 x 1,000) |
| `ledger.edge` | PASS | final life 2: E[lines] 828,381 == T_in[parse] 828,381 (into parse); T_in[parse] 828,381 − T_disc[parse] 0 = 828,381 == T_out[parse] 828,381 (parse sends what it doesn't discard); T_out[parse] 828,381 == T_in[labels] 828,381 (into labels); T_in[labels] 828,381 − T_disc[labels] 0 = 828,381 == T_out[labels] 828,381 (labels sends what it doesn't discard); T_out[labels] 828,381 == T_in[count] 828,381 (into count); T_in[count] 828,381 − T_disc[count] 0 = 828,381 == T_out[count] 828,381 (count sends what it doesn't discard); Σ(sent into router) 828,381 == R_in[split] 828,381 (listeners to router split); R_in 828,381 + R_copied 828,381 − R_disc 0 = 1,656,762 == R_fwd + ΣT_sent 1,656,762 (router split sends each event to every destination it names); T_sent[tier_logs] − T_x[tier_logs] 828,381 == T_in[count_logs] 828,381 (into count_logs); T_in[count_logs] 828,381 − T_disc[count_logs] 0 = 828,381 == T_out[count_logs] 828,381 (count_logs sends what it doesn't discard); T_out[count_logs] 828,381 == T_in[logs_metrics_only] 828,381 (into logs_metrics_only); T_in[logs_metrics_only] 828,381 − T_disc[logs_metrics_only] 0 = 828,381 == T_out[logs_metrics_only] 828,381 (logs_metrics_only sends what it doesn't discard); T_out[logs_metrics_only] 828,381 == A[window_logs] 828,381 (into window_logs); A_sent[window_logs] 2,000 == S_in[vm_logs] 2,000 (window_logs to vm_logs); T_sent[tier_metrics] − T_x[tier_metrics] 828,381 == A[window_metrics] 828,381 (into window_metrics); A_sent[window_metrics] 2,000 == S_in[vm_metrics] 2,000 (window_metrics to vm_metrics) |
| `ledger.script` | SKIP | no [ledger] sut_script |
| `ledger.aggregate` | PASS | final life 2: window_logs: Ab 828,381 == A 828,381; window_metrics: Ab 828,381 == A 828,381 (every event absorbed) |
| `ledger.egress` | PASS | vm_logs: life 0: T_sent[tier_logs] − T_x − ΣT_disc − V 0 = residual 0 + Ab[window_logs] − V 0, ok; life 1 (killed): Ab[window_logs] 230,200 − V[vm_logs] 220,000 = 10,200, within band (band [-10,001, 30,002]: ingest 2,000/s over the last drain interval; below, the residual 0 plus one 5.00004s drain interval of ingest, absorbed after the last drain and flushed before the kill; above, one 10s aggregate window (configured) unflushed at the kill plus one drain interval of queue growth after the last drain); residual T_sent[tier_logs] − T_x − ΣT_disc − Ab 0 at its last drain, what the kill lost from the receive queue and inbox, by design, not judged here; D has no shutdown part: a SIGKILL counts no shutdown drops; life 2 (final): Ab[window_logs] 828,381 − V[vm_logs] 828,381 = 0, ok \| vm_metrics: life 0: T_sent[tier_metrics] − T_x − V 0 = residual 0 + Ab[window_metrics] − V 0, ok; life 1 (killed): Ab[window_metrics] 230,200 − V[vm_metrics] 220,000 = 10,200, within band (band [-10,001, 30,002]: ingest 2,000/s over the last drain interval; below, the residual 0 plus one 5.00004s drain interval of ingest, absorbed after the last drain and flushed before the kill; above, one 10s aggregate window (configured) unflushed at the kill plus one drain interval of queue growth after the last drain); residual T_sent[tier_metrics] − T_x − Ab 0 at its last drain, what the kill lost from the receive queue and inbox, by design, not judged here; D has no shutdown part: a SIGKILL counts no shutdown drops; life 2 (final): Ab[window_metrics] 828,381 − V[vm_metrics] 828,381 = 0, ok (split target; R 272,000 = 10,000 + 197 x 1,000 + 65 x 1,000) |
| `ledger.windows` | SKIP | not enabled ([ledger] vm_every_window) |
| `ledger.replay` | PASS | vm_logs: life 2's first drain replayed 0 vs killed life 1's last-drain buffer.batches 0; vm_metrics: life 2's first drain replayed 0 vs killed life 1's last-drain buffer.batches 0 (one batch in flight allowed) |
| `ledger.summary` | PASS | final life 2: uncounted 0, each term judged on its own: Ab[window_logs] − V[vm_logs] 0; Ab[window_metrics] − V[vm_metrics] 0; the other 19 term(s) 0; wire G − (W + K + F) 22,019 over the run; ledger.wire's |
| `ledger.sent` | SKIP | local target: ledger.egress judges delivery against the backends |
| `ledger.topic` | SKIP | victoria-metrics target: no topic |
| `ledger.standin` | SKIP | victoria-metrics target: no stand-in |
| `identity.sink` | PASS | final life 2 at +810s: vm_logs: received 40 vs delivered 40 + dropped 0 + buffer.batches 0, gap 0; vm_metrics: received 40 vs delivered 40 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `drops` | PASS | no *.dropped count outside the ledger and the sink rows |
| `recovery` | PASS | 4 of 4 fault(s) judged at 2 sink(s), each recovered within recovery_bound 45s of its end; warmup rate 2,000/s |
| `file.reopen` | SKIP | the scenario has no rename step |
| `tls.served` | SKIP | the scenario has no probe step |
| `expect.logs-deliver-while-metrics-backend-stopped` | PASS | delta of logit.component.batches.delivered{component=vm_logs} on logit, during c0s1: c0s1 (+60s, +150s]: 9 (over 18 drain(s)), want >= 6 |
| `expect.metrics-queue-while-stopped` | PASS | max of logit.component.buffer.batches{component=vm_metrics} on logit, during c0s1: c0s1 (+60s, +150s]: 9 (10 sample(s) and the value in force at its start), want >= 5 |
| `expect.metrics-drop-nothing` | PASS | delta of logit.component.batches.dropped{component=vm_metrics} on logit, through c0s1: c0s1 (+60s, +195s]: 0 (over 27 drain(s)), want <= 0 |
| `expect.router-copies-every-event` | PASS | delta of logit.component.events.copied{component=split} on logit, during c0s1: c0s1 (+60s, +150s]: 180,000 (over 18 drain(s)), want >= 100000 |
| `expect.router-discards-nothing` | PASS | delta of logit.component.events.discarded{component=split} on logit, run: run (+60s, +780s]: 0 (over 131 drain(s)), want <= 0 |

### W19: Kafka OAUTHBEARER with short-lived tokens (2026-10-19)

Run `20261019T004958Z`: `kafka-oauth-reauth` at `--duration 20m`, from `soak/w19` at
`e0372670`, on the development host, against `logit:soak` (`sha256:10a901b97a16`, built from
that tree), `apache/kafka:4.2.0` (`sha256:9516fb7634ba`), and `mock-oauth2-server` 6.0.2 at its
pinned digest. Overall PASS: every row passed but `ledger.windows` and `ledger.replay`, which
SKIP on a Kafka target with no kill, and the rows for steps the scenario doesn't have.

- **Nothing was lost or stored twice.** G, W, and V are each 2,413,200. `ledger.topic` read
  6,040 distinct records, 0 duplicate copies, and T − A of 0. No batch was dropped.
- **The fallback worked as predicted.** The endpoint stopped at +450 s. 12.7 s later a redial's
  refresh failed and kept the cached token, "valid for another 8.998s", and the token's
  diagnostic counted 1. 10 s after that, with the token expired, the dials began to fail
  `Clean`: 15 `clean` attempts in the stop, `retrying` at 1, and the sink caught up 8 s after the
  endpoint came back.
- **The hung endpoint left nothing to fall back to.** In the pause, the first failure came
  26.7 s in: a redial's token request timed out after the sink's 10 s `timeout`, with the
  token expired by then. No `token_refresh_failed` was written, 7 attempts were `clean`, and the
  sink caught up 14 s after the endpoint resumed.
- **The sink renewed on schedule.** `reauth-rate` read 69.5 an hour (22 over 1,140 s), inside
  [25, 220], and `no-expired-session-sends` read 6.3 an hour (2, the broker pauses'), under 20.
  No attempt was `refused`.
- **Every fault recovered within the 45 s bound**, the slowest the broker stop at 18 s. RSS
  slopes were +0.3 MiB/h (SUT) and −0.3 MiB/h (generator), and descriptors went from 14 to 13
  (SUT).

Run `20261019T011140Z`: `kafka-oauth-reauth` at `--duration 1h`, from the same tree and images.
Overall PASS, with only inapplicable SKIPs.

- **Nothing was lost or stored twice.** G, W, and V are each 7,213,200, and `ledger.topic`
  read 18,040 distinct records with 0 duplicate copies. Of 1,967 attempts, 158 were `clean`, 5
  `ambiguous`, and none `refused`.
- **Each of the 4 endpoint stops fell back once.** The diagnostic counted 1 in each. The
  self-log shows 3 of them, 4.7 s to 8.7 s after the stop began, each with 9 s of the token
  left: the warning is throttled to its 1st, 2nd, and 4th occurrence, which is why the scenario
  reads the counter. The 4 pauses wrote none.
- **The rates held.** `reauth-rate` read 66.1 an hour (65 over 3,540 s), and
  `no-expired-session-sends` 5.1 an hour.
- **Memory and descriptors stayed flat.** The SUT's `rss_slope` read +0.2 MiB/h over 25% of
  the time after warmup, and its descriptors went from 14 to 14. All 22 faults recovered within
  the bound, each endpoint fault in 8 s to 13 s.

### `partition-rss`: the generator's RSS after a partition (2026-10-18)

After a 45 s partition, the generator's RSS fell back to its pre-partition baseline within 44 s
of the revert and held flat there for the remaining 12 minutes: no plateau above baseline, and
no growth. Run `20261019T002831Z`: `script/soak run partition-rss --retry-stalled 1` at the
scenario's 15 minutes 45 seconds, from `soak/partition-rss` at `b6154b9b`, with `logit:soak`
`sha256:18799368c79d`, on host `trout` (24 cores, Docker 29.8.2). Overall PASS:

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 4 apply/revert action(s) returned 0; 0 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 2 exit(s) of the logit services, each a scheduled stop with code 0 |
| `restarts` | PASS | 0 start(s), each a scheduled start or restart |
| `self_log` | PASS | 1 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 170 readiness sample(s) read ok outside fault windows; 0 start(s) or unpause(s) ready in time |
| `progress` | PASS | 27 30s window(s) outside fault windows, judged per sink, each freshness sample under 30s: victoria_metrics: deliveries in 27 of 27, 28 freshness sample(s); 780s of 885s after warmup judged (88%; WARN under 5%) |
| `rss_slope` | PASS | generator PASS: MiB/h life 0 +0.5 (limit 64); 665s of 885s after warmup judged (75%; WARN under 5%) \| logit PASS: MiB/h life 0 +1.3 (limit 64); 665s of 885s after warmup judged (75%; WARN under 5%) |
| `fd_slope` | PASS | generator PASS: warmup median 11 -> end 11 (limit +8) \| logit PASS: warmup median 12 -> end 12 (limit +8) |
| `ledger.wire` | PASS | G 1,888,800 = W 1,888,800 + K 0 + wire 0 (by design: 2 in UDP-affecting windows, -53 steady, 51 at the end; steady loss judged 1, each run at 0 or more, against limit 300); the generator's counted drop_newest loss, not in G: 13,600 event(s) in 136 batch(es) |
| `ledger.intake` | PASS | final life 0: W − D 1,888,800 vs E + B 1,888,800 (R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.edge` | PASS | final life 0: E 1,888,800 == A 1,888,800 (listener to aggregate); A_sent 9,300 == S_in[victoria_metrics] 9,300 (aggregate to victoria_metrics) |
| `ledger.script` | SKIP | no [ledger] sut_script |
| `ledger.aggregate` | PASS | final life 0: Ab 1,888,800 == A 1,888,800 (every event absorbed) |
| `ledger.egress` | PASS | victoria_metrics: life 0 (final): Ab 1,888,800 − V[victoria_metrics] 1,888,800 = 0, ok (split copy; R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.windows` | SKIP | not enabled ([ledger] vm_every_window) |
| `ledger.replay` | SKIP | no killed life followed by another |
| `ledger.summary` | PASS | final life 0: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V[victoria_metrics] 0; wire G − (W + K) 0 over the run, by design |
| `ledger.sent` | SKIP | local target: ledger.egress judges delivery against the backends |
| `ledger.topic` | SKIP | victoria-metrics target: no topic |
| `ledger.standin` | SKIP | victoria-metrics target: no stand-in |
| `identity.sink` | PASS | final life 0 at +969s: victoria_metrics: received 93 vs delivered 93 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `drops` | PASS | no *.dropped count outside the ledger and the sink rows |
| `recovery` | PASS | 1 of 1 fault(s) judged at 1 sink(s), each recovered within recovery_bound 45s of its end; warmup rate 2,000/s |
| `file.reopen` | SKIP | the scenario has no rename step |
| `tls.served` | SKIP | the scenario has no probe step |
| `expect.generator-rss-plateau` | PASS | rise of logit.process.memory.resident.bytes on generator, during 2: c0s2 (+330s, +720s]: -196,608 (78 sample(s) and the value in force at its start), want <= 8.38861e+06 |

The generator's `logit.process.memory.resident.bytes`, at the run's offsets. The partition ran
from +120 s to +165 s; during it the sink failed to resolve `logit:8125`, queued, and dropped
13,600 events under `drop_newest`, counted.

| When | RSS |
|---|---|
| Warmup and before the partition, +4 s to +119 s | 17.1 to 18.7 MiB, 18.2 MiB at +119 s |
| Partition, +124 s to +159 s | up about 8.7 MiB per 5 s drain |
| Peak, +164 s | 85.6 MiB |
| +169 s | 83.0 MiB |
| +174 s | 62.2 MiB |
| +184 s | 56.4 MiB |
| +189 s | 51.5 MiB |
| +194 s | 48.5 MiB |
| +199 s | 40.7 MiB |
| +209 s, 44 s after the revert | 18.3 MiB |
| +269 s, +329 s, +389 s, +449 s | 18.8, 19.0, 18.3, 18.8 MiB |
| +509 s, +569 s, +629 s, +689 s | 18.8, 19.0, 18.3, 19.1 MiB |
| +749 s, +809 s, +869 s, +929 s | 18.5, 18.5, 18.7, 18.7 MiB |
| +944 s, the last sample before the end sequence | 18.5 MiB |

From +209 s to the end the samples stay between 18.25 and 19.16 MiB. `rss_slope` fit +0.5
MiB/h over 135 samples, the warmup's tail and the plateau after +330 s, and the plateau
expectation read a rise of −0.19 MiB (−196,608 bytes) from +330 s to +720 s, against a bound of
8 MiB.

`docker stats` and the host show no contention. The generator's CPU averaged 1% to 3% in each
of minutes 0 to 3, which hold warmup, the partition, and the release, 4% to 8% in minutes 4 to
8, and 2% to 3% after. The host's 1-minute load average, sampled every 15 s from minute 4 to the
end, stayed between 14.6 and 15.9, and read 14.8 at minute 3. It came from 14 idle `ioq-*` kernel
threads in uninterruptible sleep, with 2 or 3 runnable tasks and CPU pressure at 0.

In this run the partition's memory came back whole, which suggests the 32 MiB plateau in
`fanin`'s `20261018T190542Z` was a slower release, not a floor; one run of each doesn't show why
they differ. Here the release ended inside `recovery_bound`, so this run didn't need
`rss_settle`; `fanin` keeps its 60 s because its own release took about 104 s.

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
- **W12a** changes no harness code: the self-test passes (1,478 expectations), and the four new
  scenarios pass `self-test` validation, the shipped-config test, and `script/validate`. Met:
  a run of each is recorded under "Findings", every ledger row PASS.
- **W15**: the self-test passes (1,990 expectations), and each new rule fails it when
  reverted (ten reverts, listed in the PR). `route-signals` and `route-copy` pass `self-test`
  validation, the shipped-config test, and `logit validate` in `logit:soak`. Met: a run of each
  is recorded under "Findings", Overall PASS.
- **W17**: `kafka_out`'s tests pass, and the two reconnect assertions fail with the count
  reverted. The self-test passes (1,797 expectations), and the `rate` fixtures fail with the
  reducer reverted. `kafka-sasl-reauth` passes `self-test` validation, the shipped-config test,
  and `logit validate` in its image. Met: its run is recorded under "Findings", Overall PASS.
- **W19**: the self-test passes (2,088 expectations), and the `idp` namespace and
  `token_refresh_failed` fixtures fail with their rules reverted. `kafka-oauth-reauth` passes
  `self-test` validation and the shipped-config test. Met: a 20-minute and a 1-hour run are
  recorded under "Findings", each Overall PASS.

### `rss_settle`: a settle window for `rss_slope` (2026-10-18)

A solo `fanin` run on a quiet host, `20261018T190542Z`, WARNed `rss_slope` on `generator` at
+240.4 MiB/h. Its own `logit.process.memory.resident.bytes` around the 45 s partition `c0s3`,
from +330.6 s to +375.5 s:

| When | RSS |
|---|---|
| Before the partition | about 18 MiB |
| End of the partition | 86 MiB |
| From +14 s after the partition | 62.6 MiB |
| From +84 s | 52.6 MiB |
| From +104 s | about 32 MiB |
| The next life | about 18 MiB |

The allocator returns the backlog's memory in steps; nothing leaks. `recovery_bound` ends the
excusal 45 s after the revert, so the plateaus sat in the fit beside the baseline. A
`tcp-ingress` run, `20261018T184246Z`, WARNed `logit` life 0 at +74.0 MiB/h over 12 samples in
400 s, which reads as the same effect. `rss_settle` (§2, §6) excuses more after each fault,
for `rss_slope` only. It's opt-in, 0 s by default, and these two scenarios set 60 s.

Re-scored with `soak.py check` on copies with `"rss_settle": 60` added to each
`scenario.resolved.json`, as a run of the scenario now records it, in MiB/h per life with the
share of the run after warmup judged:

| Run | Service | Before | After |
|---|---|---|---|
| `fanin` `20261018T190542Z` | `generator` | life 0 +240.4, life 1 -4.4, 31% (WARN) | life 1 -9.6, 12% (PASS) |
| | `generator-lines` | life 0 +4.0, life 1 -4.3, 32% | life 1 -15.2, 12% (PASS) |
| | `logit` | life 0 +0.2, 32% | life 0 +3.5, 12% (PASS) |
| `tcp-ingress` `20261018T184246Z` | `generator` | life 0 +30.5, 21% | life 0 +14.0, 10% (PASS) |
| | `logit` | life 0 +74.0, life 2 -5.3, 20% (WARN) | life 2 -17.2, 10% (PASS) |

Both rows PASS, and every other row of both runs re-scores unchanged. Without the key, each
copy scores as it did on `main`. A life missing from the After column has no sample outside
the widened windows: neither schedule leaves more than 75 s between one fault's end and the
next fault's start, under `recovery_bound` plus `rss_settle`. In `fanin` the generator's RSS
stays near 32 MiB until its next life, so a leftover step can stay in a life's fit; in this run
the 60 s settle covers every gap in life 0, so that step reaches no fit, and a schedule with
longer gaps could still fit it.

Counting only time that can hold a fit, 6 samples over 60 s per life, at each scenario's own
duration: `fanin` keeps a fit on every service with about 11% judged, against 25% with
`recovery_bound` alone, and `tcp-ingress` about 9%, against 14%. A 60 s default would have left
seven other scenarios with a service and no fittable life (`kafka-broker-restart`,
`kafka-sasl-reauth`, `statsd-vm-lost-total`, `fanout-stop-outage`, `spool-kill-replay`,
`standin-at-most-once`, and `statsd-datadog`), and a recorded `kafka-sasl-reauth` run,
`20261018T153250Z`, went from PASS at 29% judged to none. An earlier estimate that counted all
time outside the windows missed them. Every other shipped scenario keeps `recovery_bound` alone.

What the work showed about the harness:

- With every sample excused, `rss_slope` SKIPped: a service with no steady sample read SKIP and
  the row SKIPped with no life fitted. A service with samples after warmup now WARNs on
  coverage, and the row SKIPs for want of a fit only when the run after warmup is under 60 s.
- The self-test fixtures failed with each change reverted (five reverts): with the settle window
  off, the stepped-recovery fixture WARNed at +71.0 MiB/h against 64; with a window swallowing
  the whole run, the `cardinality`-shaped leak fixture WARNed instead of FAILing; with the old
  SKIP rule, the all-excused fixture SKIPped; with a missing key read as 60 s, the unkeyed run
  directory fixture PASSed; and with `fanin`'s key removed, its load expectation failed.

### W20: an `otlp_proto` topic (2026-10-19)

`kafka-otlp-broker-restart` ran for 20 minutes on the development host, against `logit:soak`
(`sha256:10a901b9…`) built from `soak/w20` at `3c370071`, whose release binary is `main`'s at
`33da7d62`: the branch's Rust change is a test. The run is
`perf/results/soak/20261019T010146Z`, exit 0. A 1-hour run follows the table.

| Check | Status | Detail |
|---|---|---|
| `run` | PASS | ran from start through end_end |
| `timeline` | PASS | 18 apply/revert action(s) returned 0; 0 start(s) or unpause(s) each probed; no fail fast |
| `exit` | PASS | 2 exit(s) of the logit services, each a scheduled stop with code 0 |
| `restarts` | PASS | 6 start(s), each a scheduled start or restart |
| `self_log` | PASS | 9 sink fault line(s) inside fault windows, 0 outside, 0 expected-exit line(s), 0 other ERROR, 0 non-JSON |
| `ready` | PASS | 80 readiness sample(s) read ok outside fault windows; 0 start(s) or unpause(s) ready in time |
| `progress` | PASS | 7 30s window(s) outside fault windows, judged per sink (kafka target: no VictoriaMetrics): kafka: deliveries in 7 of 7, no freshness samples; 180s of 1140s after warmup judged (16%; WARN under 5%) |
| `rss_slope` | PASS | generator PASS: MiB/h life 0 +3.7 (limit 64); 300s of 1140s after warmup judged (26%; WARN under 5%) \| logit PASS: MiB/h life 0 -0.6 (limit 64); 300s of 1140s after warmup judged (26%; WARN under 5%) |
| `fd_slope` | PASS | generator PASS: warmup median 11 -> end 12 (limit +8) \| logit PASS: warmup median 13 -> end 13 (limit +8) |
| `ledger.wire` | PASS | G 2,412,100 = W 2,412,100 + K 0 + wire 0 (by design: 0 in UDP-affecting windows, -1 steady, 1 at the end; steady loss judged 0, each run at 0 or more, against limit 100); the generator's counted drop_newest loss, not in G: 0 event(s) in 0 batch(es) |
| `ledger.intake` | PASS | final life 0: W − D 2,412,100 vs E + B 2,412,100 (R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.edge` | PASS | final life 0: E 2,412,100 == A 2,412,100 (listener to aggregate); A_sent 60,400 == S_in[kafka] 60,400 (aggregate to kafka) |
| `ledger.script` | SKIP | no [ledger] sut_script |
| `ledger.aggregate` | PASS | final life 0: Ab 2,412,100 == A 2,412,100 (every event absorbed) |
| `ledger.egress` | PASS | kafka: life 0 (final): Ab 2,412,100 − V[kafka] 2,412,100 = 0, ok (split copy; R 77,000 = 10,000 + 2 x 1,000 + 65 x 1,000) |
| `ledger.windows` | SKIP | kafka target: no VictoriaMetrics |
| `ledger.replay` | SKIP | no killed life followed by another |
| `ledger.summary` | PASS | final life 0: uncounted 0 = W − D − E − B 0 + E − A 0 + A − Ab 0 + Ab − V[kafka] 0; wire G − (W + K) 0 over the run, by design |
| `ledger.sent` | PASS | to kafka: every batch received ended in an accepted response; kafka: requests ambiguous 3, clean 40, ok 604; records accepted 604; kafka life 0 at +1209s: received 604 vs delivered 604 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed); ledger.egress and ledger.topic read the topic back |
| `ledger.topic` | PASS | T 604 distinct records in the topic (604 read, 0 duplicate copies) vs A 604 records the sink saw accepted, T − A 0 |
| `ledger.standin` | SKIP | kafka target: no stand-in |
| `identity.sink` | PASS | final life 0 at +1209s: kafka: received 604 vs delivered 604 + dropped 0 + buffer.batches 0, gap 0 (one batch in flight allowed) |
| `drops` | PASS | no *.dropped count outside the ledger and the sink rows |
| `recovery` | PASS | 9 of 9 fault(s) judged at 1 sink(s), each recovered within recovery_bound 45s of its end; warmup rate 2,000/s |
| `file.reopen` | SKIP | the scenario has no rename step |
| `tls.served` | SKIP | the scenario has no probe step |
| `expect.no-drop-restart` | PASS | delta of logit.component.batches.dropped{component=kafka} on logit, through c0s1: c0s1 (+60s, +135s]: 0 (over 15 drain(s)), want <= 0 |
| `expect.no-drop-pause` | PASS | delta of logit.component.batches.dropped{component=kafka} on logit, through c0s2: c0s2 (+165s, +250s]: 0 (over 17 drain(s)), want <= 0 |
| `expect.no-drop-stop` | PASS | delta of logit.component.batches.dropped{component=kafka} on logit, through c0s3: c0s3 (+285s, +390s]: 0 (over 21 drain(s)), want <= 0 |
| `expect.pause-is-ambiguous` | PASS | delta of logit.output.requests{class=ambiguous, component=kafka} on logit, through c0s2: c0s2 (+165s, +250s]: 1 (over 17 drain(s)), want >= 1 |
| `expect.retrying-during-stop` | PASS | max of logit.component.retrying{component=kafka} on logit, during c0s3: c0s3 (+285s, +345s]: 1 (1 sample(s) and the value in force at its start), want >= 1 |
| `expect.retrying-clears` | PASS | last of logit.component.retrying{component=kafka} on logit, after c0s3: c0s3 (+345s, +390s]: 0 (1 sample(s) and the value in force at its start), want <= 0 |

- **The escaped read-back holds.** The consumer printed 604 records on 604 lines through
  `BytesDeserializer`, every one unescaped and decoded, none unparsed. Each line averaged
  11,593 characters for a 4,600-byte request: the escaped form is 2.5 times the request,
  because most of a protobuf request's bytes aren't printable.
- **V matched to the event.** The topic's `Sum` points summed to 2,412,100, Ab, with no
  duplicate copy and no repeated point. One record per 2 s window, holding all 100 series,
  so T is 604 records against `kafka-broker-restart`'s one record per series per window.
- **The pauses still left resends.** The run counted three `ambiguous` attempts over its three
  pauses, and the topic held no copy of any of them.

Run `20261019T012708Z`: the same scenario at `--duration 1h`, on the same image, on the
development host. Overall PASS, with only inapplicable SKIPs. It confirms the 20-minute run.

- **No loss and no duplicate across 27 faults.** Ab and V are both 7,212,200, and T and A both
  1,804 records. Nine `ambiguous` attempts over nine pauses left no duplicate copy and no
  repeated point.
- **The read-back is fast at this size.** 1,804 records, 20.9 MB of escaped text, read in about
  6 s after the SUT stopped. At the escaped size an 8-hour run's topic would be about 167 MB,
  which the 180 s consumer bound should still cover, but no run has measured it.
- **RSS and descriptors were flat.** `rss_slope` read +1.7 MiB/h on the SUT and −0.0 on the
  generator, and both kept their descriptor count.
