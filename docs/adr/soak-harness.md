---
created: 2026-10-08
updated: 2026-10-08
---

# A soak harness: real containers over ordinary config, one-shot netem, and a reset-aware ledger as the loss oracle

## Status
Accepted

## Context

`logit` has harnesses for throughput (`script/perf`,
[ADR `load-test-harness`](load-test-harness.md)), interop (`script/victoria-interop`,
`script/splunk-interop`, [ADR `victoriametrics-interop`](victoriametrics-interop.md)), and data
shape (`script/shape-survey`). None runs `logit` for long under faults. Three plans describe
soaks that were never run: stopping a sink for 90 s
([`buffered-sink-delivery`](../plans/buffered-sink-delivery.md)), a `kill -9` with a disk spool
and a replay check ([`durable-sink-buffer`](../plans/durable-sink-buffer.md)), and flooding
`statsd_in` while its sink is stopped
([`decoupled-listener-io`](../plans/decoupled-listener-io.md)). `tc netem` appears in the repo
only as a hand-run latency measurement
([`native-send-window`](../plans/native-send-window.md)).

The goal is robustness: a repeatable way to run real `logit` containers against real or
generated sources and real targets, inject network and lifecycle faults on a timeline, and
report crashes, hangs, memory and file-descriptor growth, and **uncounted** data loss. Counted
loss is a policy outcome (`drop_oldest`, a rejected batch, a shutdown drop); uncounted loss is a
bug. The first scenario is a generated statsd stream into `logit` and out to VictoriaMetrics.

Four facts about the running system shape the design:

- `docker compose logs` merges a container's stdout and stderr, and `docker logs` keeps them
  apart across every life of a container that is stopped and started but never recreated.
- `internal`'s final drain runs when the shutdown signal fires, and no drain exports what
  happens after it ([`internal-telemetry.md`](../design/internal-telemetry.md), "`internal`: the
  drain", and the `shutdown` row of "UDP listeners: `ReceiveQueue` and the kernel socket"):
  - the listener decodes what its receive queue still holds and flushes its accumulator;
  - `aggregate` absorbs those events and flushes its close-time window, which still reaches the
    backend;
  - the sink makes its last delivery;
  - `datagrams.dropped{reason="shutdown"}` and a sink's `batches.dropped{reason="shutdown"}` are
    counted. These reach stderr only, as the listener's `warn` lines and the `drain complete`
    line's `batches_dropped`.

  So per-hop counts are exact only for a process life whose input stopped before the signal:
  the final life, after the generator stops and the backend's total holds. A life that ends
  under load leaves a bounded residual that no drain reports.
- `/readyz` stays `200 ok` while a sink retries and holds its queue
  ([`deploying.md`](../deploying.md), "Probes and exit codes"), so readiness can't detect a sink
  that stops making progress.
- Some UDP loss is uncounted by design: datagrams lost on the network or to netem, the kernel
  receive queue at the instant a socket closes, and the counts of a datagram send cancelled
  mid-batch ([`docs/known-gaps/intake.md`](../known-gaps/intake.md), "UDP intake").

## Decision

**The harness runs the real release image in compose over ordinary YAML configs, outside
`script/cibuild`.**

- `script/soak` builds the image from the working tree, validates each scenario's `logit`
  configs inside that image, and runs the driver. No test depends on it running, as with the
  other harnesses.
- A scenario is a directory under `tools/soak/scenarios/`: a `scenario.toml` timeline beside
  ordinary `logit` YAML configs, one per `logit` service. The configs join `script/validate` and
  `every_shipped_config_loads_and_validates`.
- The timeline is TOML because the driver reads it with Python's standard-library `tomllib`, and
  the standard library has no YAML parser. The `logit` configs stay YAML because they are `logit`
  configs.
- The driver is standard-library Python (3.11 or later) on the host, not in a container, because
  it drives the Docker daemon: lifecycle actions, one-shot fault containers, `docker inspect`,
  `docker stats`, and `docker logs`. It uses `$DOCKER` as the other harnesses do.
- Each scenario runs as compose project `soak-<scenario>`, so two scenarios can run at once on one
  daemon. The driver refuses a project that already has containers and never prunes.

**Network faults are one-shot `tc netem` runs; lifecycle faults are Docker actions.**

- Each netem change is a `docker run --rm --network container:<id> --cap-add NET_ADMIN` of a small
  iproute2 image that applies, clears, or shows a root qdisc on the target's default-route
  interface and exits. The qdisc outlives the `tc` process.
- A root qdisc shapes the target container's egress only. Netem on the `logit` under test impairs
  its sends to the backend, not the traffic arriving at it.
- Lifecycle faults are `pause`, `stop`, `restart`, a network `partition` (disconnect, then
  reconnect with the service aliases recorded before the disconnect, because a reconnect drops
  them), and a `kill` of the SUT with SIGKILL. A `kill` tests what the process left on disk,
  such as a disk spool, because nothing in the process runs after the signal.
- Every fault has a duration, so its revert is scheduled with it.

**The load source is a second `logit`: `generate_in` into `statsd_out` over UDP, one line per
datagram.**

- No new Rust, and the generator reports what it sent through the same telemetry the system under
  test (SUT) uses.
- `max_packet_bytes` is set so each datagram carries one line, which makes a datagram the ledger's
  unit at every hop. The sender's count is `logit.output.messages`, what the kernel took, not
  `events.sent`, because while the SUT is down `statsd_out` fails each batch's name resolution as
  `Clean` and sends nothing.
- The trade-offs: packing is uniform, every datagram leaves one source port, and the generator
  shares the SUT's binary, so a bug in shared code can hide on both sides. That is acceptable for
  fault testing, where the question is what `logit` does to traffic under faults, not how it
  handles a varied client population. Recorded-client traffic is the interop harnesses' job.

**The oracle is `logit`'s own telemetry plus the backend's stored totals, and the boundary of
"uncounted by design" is stated, not judged.**

- Every `logit` container writes `internal` telemetry to stdout as NDJSON (`stdio_out`,
  `format: json`) and its self-log to stderr (`--log-format json`). The driver collects both
  through `docker logs`, kept separate.
- Counters sum per process life, split where `logit.process.uptime` decreases.
- The egress oracle is the backend's cumulative totals. `aggregate` runs
  `temporality: cumulative`, so a duplicate or a lost-then-retried remote-write batch changes
  nothing, and egress loss shows only as a lost final total. Each SUT restart resets the series,
  so the total is summed reset-aware: the first value, plus each non-negative step, plus the new
  value after each decrease or SUT restart, since a restart whose first total isn't below the
  last one shows no decrease. `series_retention` is raised so a fault never evicts a series.
- Loss the system can't count by design is reported and never fails a run: UDP loss between the
  generator and the SUT, the kernel queue at socket close, and a cancelled generator send, each
  linked to its entry in [`docs/known-gaps/intake.md`](../known-gaps/intake.md). The exception is
  wire loss outside every window that affects UDP ingress, which fails above the scenario's
  threshold.
- Shutdown-time drops are read from stderr, and the sink conservation identity is checked at the
  last quiet telemetry drain before shutdown, not at exit.
- The per-hop ledger is judged with no tolerance for the final SUT life only. A life that ends
  under load is compared at its last drain and judged within a bound: what the receive queue,
  one accumulator batch, and `aggregate`'s inbox can hold at the signal.
- A hang is the absence of sink progress (`batches.delivered` flat with work queued) outside fault
  windows and their recovery bound, not a readiness failure.

The plan, [`docs/plans/soak-harness.md`](../plans/soak-harness.md), holds the scenario schema,
the compose layout, the driver loop, the full list of checks, and the workstreams.

## Alternatives considered

- **toxiproxy.** TCP only, so it can't impair the statsd leg, and it adds a hop to the data path
  that changes connection behavior. A candidate later for TCP-level faults (partial writes,
  slow close) that netem can't express.
- **pumba.** A wrapper around the same netem and Docker actions, adding a dependency and its own
  scheduling where the driver already keeps the timeline.
- **A long-lived netem sidecar sharing the target's namespace (`network_mode: service:...`).** It
  keeps the old namespace across a `stop` and `start` of the target and loses its interface on a
  network disconnect, so it fails during the faults it exists to apply.
- **`logit-perf` as the sender.** Its UDP sender is finite, reads its target from a scenario
  file, stops through the harness's own channel, and reports nothing about what it sent. Making it
  a standalone paced sender with telemetry is a Rust change and an image before any scenario runs.
- **A Python producer.** It would need its own counting and pacing, and its sent count would
  share no unit or code path with the SUT's telemetry.
- **A checker container for VictoriaMetrics queries.** One more image and lifecycle to manage; a
  loopback-only ephemeral published port lets the host driver query directly.
- **Scraping `prometheus_out` exposition for telemetry.** A scrape returns current totals only and
  stops when the process stops; stdout NDJSON keeps every drain, survives a stopped container,
  and reuses the grammar other harnesses already read.

## Consequences

- A new `tools/soak/` (driver package, compose file, netem image, scenarios) and `script/soak`.
  `tools/soak/` reads the NDJSON grammar, so `crates/logit-outputs/src/ndjson.rs`'s module doc
  gains it in its list of readers that mirror the grammar.
- `script/validate` and `every_shipped_config_loads_and_validates`
  (`crates/logit-cli/src/config.rs`) gain the `tools/soak/scenarios/*/` configs.
- The host needs Python 3.11 or later for `tomllib`. `Dockerfile.dev` has no Python, so the
  driver's self-test runs at the start of every `run` and by hand, not in CI. Only the config
  globs are checked in CI.
- Shared-daemon etiquette: a fixed project name per scenario, refusal of a live project, and no
  prune.
- The ledger's "uncounted by design" rows stay reported until the gaps in
  `docs/known-gaps/intake.md` close. Closing one turns its row into a judged check.
