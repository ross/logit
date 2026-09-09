---
created: 2026-09-09
updated: 2026-09-09
---

# A top-level `admin:` block, not a component, for readiness/liveness

## Status
Accepted

## Context

`logit` has no health or readiness signal. Nothing listens for a probe; `demo/compose.yaml`'s
four services that `depends_on: logit` all say `condition: service_started` because there is no
`healthcheck` to wait on, and `Dockerfile` has no `HEALTHCHECK`. Every listener binds lazily
inside `Input::run`, so there is no "all sockets bound" moment even in-process. An orchestrator
(Kubernetes, Docker Compose, a systemd unit with a restart policy) needs a cheap, standard way to
ask "is this process ready to accept traffic" and "is this process alive at all" — two different
questions with two different failure semantics.

## Decision

**A top-level `admin:` block, not a component kind.** `logit_config::Config` gains `admin:
AdminConfig { bind: Option<String> }`, off unless `bind` is set. This is process-level state — one
admin server per `logit run`, not one per graph node — and it fits nothing in the component
model: it would have no `sources` and no consumers, tripping graph rule 7 ("every non-sink
component needs at least one consumer" — `crates/logit-pipeline/src/graph.rs`), and modeling a
health probe as something that reads from or writes to the pipeline graph would be fiction.

**Two routes, nothing else.** `GET /readyz` reports the pipeline's own lifecycle phase (`200 ok`
once every listener is bound and every node task is running; `503 starting` before that; `503
draining` after a shutdown signal; `503 degraded` if any node has exited with an error while the
process is still draining). `GET /healthz` reports only whether the admin task itself can still
answer — `200 ok` whenever the tokio runtime is alive, regardless of the pipeline's own state.
`?format=json` returns `{status, since, components}` on either route for a caller that wants the
detail; `HEAD` mirrors `GET`. No `/metrics`, no config dump — see this ADR's own "Alternatives"
below and `docs/design/internal-telemetry.md`'s "What this is not".

**HTTP/1.1 only, no TLS, no auth.** This is a loopback/pod-local endpoint: a orchestrator's kubelet
or Docker's own health-check daemon speaks to it inside the same network namespace or the same
pod, never across a real network boundary. Adding TLS or auth would protect against a threat model
this endpoint doesn't have and complicate exactly the deployment shapes it exists to serve — see
`docs/known-gaps.md` for the explicit call-out.

**Readiness is per-process, not per-sink.** A single degraded sink does not flip `/readyz` to
unready — that is what a sink's own `buffer:` block (retry budget, queue depth) already exists to
absorb, and conflating "one sink is behind" with "this process cannot do its job" would make an
orchestrator restart a pod that is otherwise healthy and draining fine. `/readyz`'s `degraded`
state is reserved for a node that has actually exited with an error, not merely one that is
retrying.

## Alternatives considered

- **A component kind (e.g. `admin_in`).** Rejected — see the Decision above: it fits no arity rule
  in the graph model, and giving it fake `sources`/consumers just to satisfy rule 7 would be a
  worse fiction than a top-level config block.
- **A `/metrics` scrape endpoint, bundled into the same server.** Rejected, restating ADR
  `internal-telemetry-as-pipeline-events`'s decision: a scrape endpoint means a second
  representation of a metric kept in sync with `logit_core::MetricKind`, and a second serving
  path with nothing to do with the pipeline. A readiness/liveness endpoint carries no metrics and
  answers a different question ("can this process do its job right now," not "what are its
  numbers") — this ADR does not reopen that decision, it restates it for a reader who might
  otherwise assume `admin:` is the door left open for one.
- **Readiness via a file (`sd_notify`-style) or a Unix socket instead of HTTP.** Deferred, not
  rejected outright: `sd_notify` is systemd-specific, and a file-based liveness check needs its own
  polling convention. An HTTP probe is what every orchestrator this project targets already
  speaks natively (Kubernetes' `httpGet` probe, Docker's `HEALTHCHECK`), so it is the one that
  needs no adapter.

## Consequences

- `logit-cli/src/admin.rs` is a small, standalone HTTP server with no dependency on the pipeline
  graph beyond a `watch::Receiver<PipelineState>` — it can be read, tested, and reasoned about
  without touching `logit-pipeline` at all.
- `Dockerfile`'s `HEALTHCHECK` and `demo/compose.yaml`'s `depends_on: service_healthy` both depend
  on `admin.bind` being set in the target config — a config that omits `admin:` gets no health
  check, exactly as it does not exist today.
- `docs/known-gaps.md` gains the explicit no-TLS/no-auth note, and the readiness-is-per-process
  note, as permanent, intentional limitations rather than gaps to eventually close.
- A future per-sink readiness surface (e.g. per-component status in a richer probe) is additive to
  the existing `PipelineState.components` map — no schema break — should an operator's real need
  for it ever show up.
