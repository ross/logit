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
script/soak check perf/results/soak/<stamp>      # re-score a run offline
```

## What it runs

A scenario is a directory under `scenarios/`: a `scenario.toml` timeline beside one ordinary
`logit` config per `logit` service. `compose.yaml` starts three services under the compose
project `soak-<scenario>`:

| Service | Image | Role |
|---|---|---|
| `victoria-metrics` | `victoriametrics/victoria-metrics:v1.152.0` | the backend, `-retentionPeriod=100y`, published on an ephemeral loopback port the driver queries |
| `logit` | `logit:soak`, built from the current tree | the system under test (SUT), with the scenario's `configs.sut` |
| `generator` | `logit:soak` | the load source, a second `logit` with the scenario's `configs.generator` |

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

### Faults

| Action | What it does | Revert |
|---|---|---|
| `netem` | a one-shot `logit-soak-netem:local` container in the target's network namespace sets a root `tc netem` qdisc with the step's `args` | the same container clears it |
| `pause` | `docker pause` | `docker unpause` |
| `stop` | `docker stop -t 30` | `docker start` |
| `kill` | `docker kill -s KILL`, on `logit` only: no shutdown signal, so no final telemetry drain, no shutdown drops, and exit code 137. A killed generator's last sends would reach the SUT with no count behind them | `docker start` |
| `restart` | `docker restart -t 30` | none |
| `partition` | `docker network disconnect` from every network | `docker network connect`, passing back each alias recorded before the disconnect |

A root qdisc shapes the target's egress only: `netem` on `logit` impairs its remote-write, not
the statsd arriving at it. To impair the UDP into the SUT, put `netem` on `generator`. The driver
appends `limit 100000` to `args` unless they set a `limit`, so netem's own queue never drops.

## A run

1. `script/soak` runs the self-test, builds `logit:soak` from `Dockerfile` and
   `logit-soak-netem:local` from `netem/` (set `SOAK_SKIP_IMAGE=1` to reuse both), and validates
   the scenario's configs in `logit:soak`.
2. The driver refuses to start if the compose project already has containers, then brings the
   stack up with `docker compose up --wait`, which waits on each `logit` service's `logit ready`
   health check.
3. On a 1-second tick it applies each fault and its revert on schedule, probes `logit ready`
   after every start, restart, or unpause of a `logit` service, inspects every container every
   5 s, samples `docker stats` and VictoriaMetrics' newest sample every 30 s, and appends each
   container's new log lines every 5 minutes. A `logit`
   container that exits or restarts with no step behind it ends the schedule at once.
4. At the end it reverts active faults, stops the generator, waits until VictoriaMetrics' totals
   hold across two aggregate windows, stops the SUT (expecting exit 0), and exports every stored
   series.
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
| `stats.ndjson`, `vm-freshness.jsonl` | the 30-second `docker stats` and VictoriaMetrics freshness samples |
| `vm-export.jsonl` | VictoriaMetrics' `/api/v1/export` of the scenario's `vm_selector` at the end |
| `logs/<service>.stdout`, `.stderr` | each service's output across every life of its container |
| `log-chunks.jsonl` | one record per `docker logs` call: its `--since`, `--until`, exit code, and bytes, or the end of its error when it failed |
| `inspect/<service>.json` | each container's final `docker inspect` |
| `provenance.txt`, `compose.env`, `scenario.toml`, `scenario.resolved.json`, `configs/` | what ran, on what host, from which commit, the seed of a random schedule, the expanded schedule, and a copy of each `logit` config |

## The checks

Each check skips fault windows, a fault's span plus the scenario's `recovery_bound`, where its
rule only holds in steady state. The plan's "The checks" has the full rules.

| Check | `FAIL`s when |
|---|---|
| `run` | the driver aborted, hit an error, or was interrupted, or the timeline lacks its `start` or `end_end` phase |
| `timeline` | a fault's apply or revert returned nonzero, the schedule failed fast, or a start or unpause of a `logit` service has no readiness probe; a fault whose apply failed gets no window |
| `exit` | a `logit` container exits outside a scheduled stop, restart, or kill, or with a code other than the one that action leaves (0, or 137 for a kill), or any container is OOM-killed |
| `restarts` | a container starts with no scheduled start or restart behind it: the revert of a `stop` or `kill`, or a `restart` |
| `self_log` | stderr has an `exiting` line with a nonzero code, a panic, or an `ERROR` line other than a sink's `retrying`; sink fault lines outside a fault window `WARN` |
| `ready` | a `logit` container is unhealthy outside a fault window, or isn't ready within 30 s of a start |
| `progress` | the SUT's sink delivers nothing in a `progress_window` outside fault windows with work queued (a hang), a `logit` container's telemetry goes quiet, or VictoriaMetrics' newest sample is 30 s old; a sink still retrying past `recovery_bound` `WARN`s, and so do windows covering under 5% of the run after warmup |
| `rss_slope` | resident memory grows faster than `rss_growth_mib_per_hour` in steady state; a run shorter than an hour `WARN`s instead, and so do slopes covering under 5% of the run after warmup |
| `fd_slope` | open file descriptors end, or sit after a recovery, more than `fd_growth` above the warmup median |
| `ledger.wire` | the generator's lines minus what the SUT read or the kernel dropped (G − (W + K)), loss by design, exceeds `wire_loss_outside_faults` outside UDP-affecting fault windows, with one generator batch of tolerance per window edge, each steady run counting at 0 or more; the detail lists the gap per run of drain intervals, and the generator's own counted `drop_newest` loss beside it |
| `ledger.intake` | the final SUT life's datagrams read minus dropped differ from the events sent plus bad lines; or an earlier life's read-but-unabsorbed residual (W − D − B − Ab) falls outside [0, R], R being the receive queue plus 67 batches |
| `ledger.edge`, `ledger.aggregate` | in the final SUT life, the listener's events sent differ from those `aggregate` received, or those it received from those it absorbed |
| `ledger.egress` | the final SUT life's absorbed increments minus VictoriaMetrics' reset-aware total (Ab − V), or a stopped earlier life's datagrams read minus dropped, bad lines, and that total (W − D − B − V), is nonzero, other than a positive gap that life's `drain complete` line counts in `batches_dropped`; a killed life's Ab − V falls outside its band (below, a surplus: the residual plus one drain interval of ingest; above, uncounted: one aggregate interval plus one drain interval of ingest), with the residual the kill lost reported beside it; or the export matched no series. A series without one segment per SUT life `WARN`s |
| `ledger.windows` | under `[ledger] vm_every_window = true` only: a series in a SUT life has two samples more than 1.5 aggregate intervals apart, a killed life's last sample is more than 1.1 intervals before the kill, or a later life's first is more than 2 intervals after its start. The interval comes from the SUT config. A wider gap is excused as idle when the `sut_aggregate` component's `batches.sent` is 0 over the drains from a + (interval + p) / 2 to b − (interval − p) / 2, p being the drain period: no window was emitted, so none was lost. A pause, with no drains at all, counts 0. Any batch there FAILs the gap as windows emitted but not stored, whatever fault was running |
| `ledger.replay` | the life after a killed one replays, in its first drain, a different number of batches from the killed life's last `buffer.batches`, beyond one in flight |
| `ledger.summary` | the final life's uncounted loss, (W − D − E − B) + (E − A) + (A − Ab) + (Ab − V), isn't 0; a counted egress term is shown and not judged |
| `identity.sink` | at the final life's last drain before shutdown, the sink's batches received, plus those a disk spool replayed at open, differ from delivered + dropped + queued by more than one batch in flight |
| `recovery` | within `recovery_bound` of a fault's end, no drain interval clear of other faults shows the sink not retrying, its buffer under 5% full or holding at most one batch, the listener's receive queue under 5% full, and ingest at 95% of the warmup rate; a generator `rate_behind` diagnostic turns a rate shortfall into a `WARN`, and so does a run where no fault had an eligible interval |
| `expect.<name>` | a scenario's own `[[expect]]` bound doesn't hold; see [Expectations](#expectations) |

The plan's "The ledger and identities" defines each symbol. Shutdown-time drops land after
`internal`'s final drain, so the ledger reads them from stderr: the listener's `warn` lines into
D, and `drain complete`'s `batches_dropped` beside `ledger.egress`. The per-hop rows are exact
only for the final SUT life, because an earlier life stops under load. A life a `kill` ended
writes no final drain and no stderr shutdown lines, so up to one 5 s `internal` interval of its
counters is lost and its D has no stderr part; `ledger.egress` judges its Ab − V within a band
instead of for equality, and the plan's "Which life a hop is judged in" derives the band. The
band can't see a lost middle window, or a lost last one when the last flush and drain coincide;
`ledger.windows` catches both unless the kill lands within 0.1 × the aggregate interval of the
last flush, and `ledger.replay` catches a replay two or more batches short.

### Expectations

A scenario's `[[expect]]` tables add one `expect.<name>` row each, judging one metric reduced
over a window around one step. The plan's "The scenario schema" is the full reference.

```toml
[[expect]]
name = "sink-queue-fills"
service = "logit"                       # logit or generator
metric = "logit.component.buffer.batches"
component = "victoria_metrics"          # and any other attribute under attrs = { ... }
step = "c0s1"                           # an expanded step id, or a [[step]] index for every cycle
window = "during"                       # the fault's span; `after`, the recovery_bound after it; `through`, both
reduce = "max"                          # counters: delta, min_delta; gauges: max, min, last
min = 2                                 # min, max, or both
```

`min_delta` is the smallest single drain's delta, so `min = 1` asserts a counter rose in every
drain of the window. A gauge reducer includes the value in force at the window's start, because
`internal` exports a gauge only in a drain after it was set, taken from the SUT life running at
that start only. A bound that must hold for the whole episode, such as a loss counter that must
stay 0, uses `through`: a chain blocked behind a sink stays blocked until the sink's next retry,
up to its `retry_max_delay` after the revert. A scenario with no `[[expect]]` gets one `expect`
row that SKIPs.

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
| Run directory | about 250 MB: 100 MB SUT stdout, 130 MB generator stdout, 8 MB `watchdog.jsonl`, 6 MB `vm-export.jsonl` | about 750 MB |
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
