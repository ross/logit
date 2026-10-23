---
created: 2026-10-23
updated: 2026-10-23
---

# Runtime worker count: a top-level `runtime.workers` sets the Tokio worker threads

## Status
Accepted

## Context
A batch of 864-byte `Event`s written on one core and read on another costs about as much CPU as the
work done on it. On the perf VM, a router scenario ran at 0.58 µs/event on one worker and at 1.1 to
2.3 µs/event free-running across 8 workers. Throughput fell only 10 to 20% with one worker, and
resident memory went from 80 MiB to 36 MiB. Which side of that trade suits a deployment is the
operator's call: an edge agent that generates a trickle wants one worker, and a central collector
aggregating many sources wants cores. The worker count was not configurable except through
`TOKIO_WORKER_THREADS`.

## Decision
A top-level `runtime:` block takes one field, `workers`, a count of at least 1. `logit run` builds
its Tokio multi-thread runtime with that many workers.

- The default is unchanged: one worker per CPU in the process's affinity mask
  (`std::thread::available_parallelism`).
- `TOKIO_WORKER_THREADS` still applies, and only when `workers` is unset.
- `workers: 1` is a multi-thread runtime with one worker, not a current-thread runtime. Tasks that
  block a thread (`block_in_place`) and the runtime's other machinery behave as they do today.
- `logit` never picks a runtime shape from the config or the host. Operators get knobs.
- `logit run` loads the config once, before it builds the runtime, and passes the loaded config to
  the pipeline. A load failure still exits `1`. The signal handlers still go in before the load,
  on a one-thread runtime of their own, so a signal during the load is held as it was.

## Alternatives considered
- **Fit the worker count to the graph.** Whether one worker is enough depends on traffic, which the
  graph doesn't describe, so a guess would be wrong in both directions.
- **Default to one worker now.** It would cap every existing deployment at one core on upgrade,
  and no real-traffic measurement supports the change.
- **A runtime per pipeline.** Nodes of one pipeline would pay the cross-core cost at each
  runtime boundary, and the graph's resolution doesn't produce isolated pipelines to size.

## Consequences
- The `runtime:` block is in `schema/logit.schema.json`. A config file is still read only through
  `logit_cli::config::load`.
- The process has one extra thread, `logit-signals`, which serves the signal handlers. It is
  outside the count, as a `lua` component's thread is.
- Open: the default is revisited only with measurements from real traffic.
