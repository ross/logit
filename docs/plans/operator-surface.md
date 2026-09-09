---
created: 2026-09-09
updated: 2026-09-09
---

# Enabling plan: operator surface — readiness/liveness, leveled structured self-logging, internal logs

## Context

Three things an orchestrator or an on-call engineer expects from a long-running collector are
absent, and each is named in `docs/known-gaps.md` or discoverable in five minutes of running it:

- **No health or readiness signal.** Nothing listens for a probe; `demo/compose.yaml`'s four
  services that `depends_on: logit` all say `condition: service_started` because there is no
  `healthcheck` to wait on; `Dockerfile` has no `HEALTHCHECK`. Every listener binds lazily inside
  `Input::run`, so even in-process there is no "all sockets bound" moment
  (`crates/logit-cli/tests/otlp_round_trip.rs` sleeps 50 ms for exactly this reason).
- **Self-logging is `eprintln!`.** `logit_core::diag::Diagnostics` prefixes a component id and
  throttles by occurrence count, and that's all: no severity, no structured fields, no filtering,
  no timestamps, no lifecycle messages (startup, bound, shutdown, sink degraded) at all. Three raw
  `eprintln!`s remain in `run_lua` (`crates/logit-pipeline/src/runtime.rs`), one per failing
  event, unthrottled. `known-gaps.md` files the `tracing` migration as "deliberately kept as
  separate, later work." This is that work.
- **Exit codes don't distinguish outcomes.** Clean shutdown is 0, the double-signal kill is 130,
  and everything else — a bad config, an unbound port, a sustained permanent sink failure after
  an hour of running — is 1 via `anyhow`'s `Termination`.

**What this is not.** Not a `/metrics` scrape endpoint —
[ADR `internal-telemetry-as-pipeline-events`](../adr/internal-telemetry-as-pipeline-events.md)
rejected a pull path *for metrics* (a second `MetricKind` representation, a second serving path)
and `docs/design/internal-telemetry.md`'s "What this is not" restates it. A readiness/liveness
endpoint carries no metrics and is not that thing; the ADR written here says so explicitly. And
the `tracing` adoption is the ADR-blessed shape: "a future `tracing` subscriber could itself feed
`Diagnostics`/`Telemetry`, same as any other producer" — workstream D builds exactly that.

## Decisions already settled

| Question | Decision |
|---|---|
| Config surface | A **top-level `admin:` block**, not a component kind: `Config { components, admin: AdminConfig }` with `AdminConfig { bind: Option<String> }`, `#[serde(deny_unknown_fields, default)]`. Off unless `bind` is set. A component would have no `sources` and no consumers and trip rule 7; and the server is process-level, not part of the graph |
| Endpoints | `GET /readyz` → `200 ok` once every listener has bound and every node task is running; `503 starting` before that; `503 draining` after a shutdown signal; `503 degraded` if any node has exited with an error while the process is still draining. `GET /healthz` → `200 ok` whenever the admin task can answer (tokio runtime alive). Body: `text/plain` status word, plus `?format=json` returning `{status, since, components: {id: "running"\|"bound"\|"failed"}}`. Nothing else — no `/metrics`, no config dump |
| Readiness signal | `Input` trait gains a defaulted `async fn bind(&mut self) -> anyhow::Result<()> { Ok(()) }` called by `run_input` **before** `run`. `otlp_in`, `statsd_in`/`syslog_in` (`UdpListener`), `tail_in`/`docker_in`, and `logit_in` move their bind/open into it and keep the bound socket in `self`. A bind failure therefore fails startup before any other node starts (today it surfaces as the first `JoinSet` error while siblings are already running). `run_with_telemetry` binds *all* inputs first (sequentially, sorted ids), then spawns everything — the Lua `ready_rx` handshake precedent, generalized |
| Readiness state | `logit_pipeline::Readiness` — `Arc<watch::Sender<PipelineState>>` with `PipelineState { phase: Starting\|Ready\|Draining\|Failed, components: HashMap<String, NodeState> }`, updated by the runtime; the admin server holds a `watch::Receiver`. `run_with_telemetry` gains a `readiness: Readiness` parameter (a `Readiness::disabled()` for tests and the existing entry points) |
| Server | `crates/logit-cli/src/admin.rs`: HTTP/1.1 only, `hyper` + `hyper-util` `auto::Builder` copied from `otlp_in`'s `serve_connection`, connection limit 16, 5 s per-connection read timeout, no TLS (loopback/pod-local by design; say so in `deploying.md`). `hyper`, `hyper-util`, `http`, `http-body-util` promoted to direct `logit-cli` dependencies (same versions already in `Cargo.lock`) |
| Probe helper | `logit ready [--admin http://127.0.0.1:9600]` subcommand: GET `/readyz`, exit 0 on 200, 1 otherwise, prints the status word. Uses `hyper`'s client (feature already on) — no `reqwest` in `logit-cli`. Used by `Dockerfile`'s `HEALTHCHECK` and `demo/compose.yaml` (the image is `bookworm-slim` with no `curl`) |
| Exit codes | `0` clean shutdown; `1` config/validation error or startup failure (bind, TLS file, Lua load); `2` runtime failure after ready (sustained permanent sink failure, a listener's accept loop dying); `130` second signal. Implemented as `enum RunError { Startup(anyhow::Error), Runtime(anyhow::Error) }` returned by `run_pipelines`, mapped in `main` via `std::process::exit`. Table goes in `deploying.md` |
| Logging facade | Adopt **`tracing`** (already in `Cargo.lock` at 0.1.44 transitively; promoted to a direct dependency of `logit-core`, `logit-pipeline`, `logit-cli`, with the usual "already in the lock at this version" comment). `Diagnostics` keeps its API and throttling but emits `tracing::warn!(target: "logit", component = %id, key = key, "{msg}")` instead of `eprintln!`; gains `info`/`error` (unthrottled) for the rare lifecycle messages a component itself owns (a listener's bound address, a file rotated) |
| Subscriber | `tracing-subscriber` (new subtree: `fmt`, `env-filter`, `json` features — `sharded-slab`, `thread_local`, `matchers`, `nu-ansi-term`; all MIT/Apache; `script/audit` must pass with no `deny.toml` change or the ADR records the addition) installed in `main` for `run` only. `--log-level <directive>` / `LOGIT_LOG` env (EnvFilter syntax, default `info`), `--log-format text\|json` (default `text`; `json` is one event per line with `timestamp`, `level`, `target`, `component`, `key`, `message`). `schema`/`validate`/`graph` stay print-only |
| Lifecycle log events (all `info` unless noted) | `starting` (config path, component count, version); per listener `bound` (id, address); `ready`; `shutdown signal received`; `drain complete` (duration, batches dropped on shutdown as `warn` if > 0); per sink `degraded` (`warn`, first failed delivery after a success) / `recovered` (`info`); `exiting` (code, reason, `error` for 2). Names are `&'static str` event fields, stable for log-based alerting |
| Remaining `eprintln!` | The three in `run_lua` become `Diagnostics` calls (`script_error` throttled per component, `flush_error`, `trace_context_error`); `main.rs`'s `logit graph` warning becomes `eprintln!`-as-is (it's a CLI's stderr, not the service) |
| Internal logs into the pipeline | A `tracing_subscriber::Layer` (`logit_core::telemetry::TelemetryLayer`) that, for events at `warn` or above carrying a `component` field, pushes a `PendingLog { ts, level, key, message }` into that component's `ComponentBuffer.logs` (bounded `MAX_LOGS_PER_COMPONENT = 256`, overflow counted `logit.internal.logs.dropped{reason="buffer_full"}`); unattributed events go to the `internal` component's own buffer. `ComponentBuffer::drain` gains a third pass emitting `Event::log(ts, base_attrs + key, LogRecord { message, severity: Severity::from(level), body_format: Raw, trace: None })`. `internal`'s tick fold gains a `logs_emitted` arm and counter. Config: `internal.logs: warn \| error \| off` (default `warn`). The layer is installed only when the config has an `internal` component (the `Registry` exists) |
| Where it lands in the demo | `demo/compose.yaml`: `logit` gains `healthcheck: ["CMD", "logit", "ready"]`; the four `depends_on: logit` entries flip to `service_healthy`; `demo/logit.yaml` gains `admin: { bind: 0.0.0.0:9600 }` and routes `internal`'s log events into `logs_only → loki_out` for free (they are ordinary `LogRecord`s) |

## The constraint everything is designed around

Readiness must be **true only when the pipeline can accept and forward**, and false as soon as
it can't — never a static "process is up." Two consequences: (1) binds move ahead of task
spawning (the `Input::bind` decision), so `Ready` is asserted only after every socket/file is
open and every task exists; (2) the first `JoinSet` error and the shutdown signal both flip
`phase` immediately, *before* the drain, so an orchestrator stops routing to a draining pod.
Liveness deliberately stays trivial — a wedged drain is already covered by the second-signal
kill and by readiness going false.

The other constraint is `Diagnostics`'s determinism: components are tested by asserting on
throttle counts without a clock. The `tracing` migration must not add a clock read or an
allocation to a *suppressed* `warn_throttled` call — the suppressed path returns before touching
`tracing`. `crates/logit-bench/tests/allocations.rs`'s stage counts must not move.

## Workstream dependency graph

```
A (tracing adoption, Diagnostics, CLI flags, lifecycle events) ──┐
B (Input::bind, Readiness state, exit codes)                      ──┼── C (admin: config + server + `logit ready`) ── E (docs, demo, Dockerfile)
                                                                   └── D (TelemetryLayer → internal logs) ────────────┘
```

A and B are independent. C needs B (and uses A's lifecycle events). D needs A. E needs all.
Suggested landing: A; B; C; D; E — five PRs, each with its own tests, or A+B, C+D, E.

## A. `tracing` adoption

**Goal:** every self-diagnostic goes through `tracing` with a level, a component, a key, and
consistent fields; the operator picks level and format.

- `crates/logit-core/src/diag.rs`: `warn`/`warn_throttled` emit via `tracing::warn!`; add
  `info(&self, key, msg)` and `error(&self, key, msg)`; `warn_throttled`'s suppressed path
  unchanged (count, telemetry, return `false` — no `tracing` call). Keep `component_id()`.
- `crates/logit-cli/src/main.rs`: `--log-level` (`String`, default `"info"`, also `LOGIT_LOG`),
  `--log-format` (`text|json`); `init_logging()` builds the subscriber (`fmt` with
  `with_target(false)`, `EnvFilter::try_new`, `json()` when asked) before the runtime; on a bad
  directive, exit 1 with a clear message.
- Lifecycle events per the decisions table: `starting`/`ready`/`shutdown`/`drain complete`/
  `exiting` in `crates/logit-cli/src/pipeline.rs::run_pipelines` and `run_with_telemetry`;
  `bound` in each `Input::bind` (workstream B — until then, in `run`); `degraded`/`recovered`
  in `write_loop` where `last_success` is tracked.
- `run_lua`'s three `eprintln!`s → `Diagnostics`.

**Test list:** `Diagnostics` tests unchanged (they assert return values and counts, not
stderr); a `tracing_test`-free assertion using `tracing::subscriber::with_default` and a
capturing layer that `warn_throttled` emits on powers of two only and carries
`component`/`key` fields; `--log-level nonsense` exits 1; `--log-format json` produces
parseable lines (spawn the binary in a `logit-cli` integration test with an invalid config so it
exits fast). **Done:** `grep -rn 'eprintln!' crates/*/src` returns only `main.rs`'s `graph`
warning; allocation counts unchanged.

## B. `Input::bind`, readiness state, exit codes

- `crates/logit-pipeline/src/input.rs`: `async fn bind(&mut self) -> anyhow::Result<()>`
  defaulted no-op, doc: "open sockets/files here; `run` may assume they exist."
- `crates/logit-inputs`: `OtlpInput` (`TcpListener::bind` → `self.listener: Option<TcpListener>`),
  `UdpListener` (`bind_socket` → `self.socket`), tail/docker driver (initial directory scan +
  checkpoint load), `InternalInput` (no-op). Each emits `bound` via `Diagnostics::info`.
- `crates/logit-pipeline/src/runtime.rs`: `pub struct Readiness` (`watch`), `PipelineState`,
  `NodeState`; `run_with_telemetry(graph, specs, telemetry, readiness, shutdown)`: phase
  `Starting` → bind every input in sorted order (first error returns
  `RunError::Startup`) → spawn all → `Ready` → on shutdown signal `Draining` → on first task
  error `Failed` + component marked → after join, return `Ok` or `RunError::Runtime`.
  `run`/`run_with_shutdown` pass `Readiness::disabled()`.
- `crates/logit-cli/src/pipeline.rs`: `run_pipelines -> Result<(), RunError>`; `main` maps to
  exit 0/1/2; the double-signal task keeps 130.
- `crates/logit-cli/tests/otlp_round_trip.rs`: replace the 50 ms sleep with `input.bind().await`
  before spawning `run` — the readiness primitive doing its job in-tree.

**Test list (`runtime.rs`, `tokio::time::pause()`):** phase sequence `Starting → Ready →
Draining` observed through the watch on a normal run; a failing `bind` returns
`RunError::Startup` and no task was spawned (a spy `Output` never sees `send`); a listener
erroring after ready flips `Failed` with its id and returns `RunError::Runtime`; sustained
permanent sink failure → `Runtime`. `main` exit-code mapping tested by spawning the binary with
a config whose listener port is already held (expect 1) and — via a `#[cfg(test)]`-only fail-fast
knob or an unreachable sink with `retry_budget: 1s` and the permanent window shortened for tests
— expect 2. **Done:** the sleep is gone from `otlp_round_trip.rs`.

## C. `admin:` block, server, `logit ready`

- `crates/logit-config/src/lib.rs`: `Config.admin: AdminConfig` (`#[serde(default)]`),
  `AdminConfig { bind: Option<String> }`; schema regenerated; graph untouched (nothing to
  validate beyond a parseable socket address — do that in `build`/`main`).
- `crates/logit-cli/src/admin.rs`: `pub async fn serve(bind: String, readiness:
  watch::Receiver<PipelineState>, shutdown: watch::Receiver<bool>) -> anyhow::Result<()>`;
  routes per the decisions table; 404 otherwise; `HEAD` mirrors `GET`. Spawned by
  `run_pipelines` alongside the kill switch, aborted after the pipeline returns. Its own bind
  failure is `RunError::Startup`.
- `logit ready` subcommand (`Command::Ready { admin: String }`).
- `Dockerfile`: `HEALTHCHECK --interval=10s --timeout=2s --start-period=5s CMD ["logit", "ready"]`
  — only effective when the config sets `admin.bind` on `0.0.0.0:9600`; document.

**Test list:** `/readyz` returns 503 before `Ready`, 200 after, 503 during `Draining` (drive
the watch by hand); `/healthz` always 200; `?format=json` shape; unknown path 404; a 17th
concurrent connection waits (permit) rather than errors; `logit ready` exit codes against a
stub server. **Done:** `script/schema` diff is `admin` only; `script/validate` passes every
shipped config (none set `admin`, so no behaviour change).

## D. Internal logs into the pipeline

- `crates/logit-core/src/telemetry.rs`: `PendingLog`, `ComponentBuffer.logs`,
  `MAX_LOGS_PER_COMPONENT`, the third `drain` pass (timestamp = emit time, like spans),
  `logit.internal.logs.dropped{reason="buffer_full"}`; `Registry::buffer_for(id)` lookup;
  `pub struct TelemetryLayer(Arc<Registry>, Severity)` implementing
  `tracing_subscriber::Layer` (reads the `component`/`key` fields and the message via a
  `Visit` impl; anything without `component` → the `internal` component's buffer under key
  `"process"`).
- `crates/logit-inputs/src/internal.rs`: `logs_emitted` fold arm + `logit.internal.logs.emitted`.
- `crates/logit-config`: `Internal { interval, span_sample_rate, logs: InternalLogs = Warn }`;
  `crates/logit-cli/src/main.rs::init_logging` takes the optional `Registry` and level and
  stacks the layer (the registry is created in `prepare`; reorder so the subscriber can be built
  after config load but before the runtime — `Schema`/`Validate`/`Graph` never install it).

**Test list:** a `warn!` with `component="x"` lands in `x`'s buffer and drains as an
`Event::log` with `severity: Warn`, attrs `component`/`kind`/`role`/`key`; `info!` is not
captured at the default level; 257th pending log is dropped and counted; an unattributed
`error!` lands under `internal`; `logs: off` installs no layer; a suppressed `warn_throttled`
produces no log event (the throttle is upstream of `tracing`). **Done:** `examples/
internal-telemetry.yaml` shows a log event reaching `stdio_out` when a malformed statsd line is
sent.

## E. Docs, demo, Dockerfile

- ADR `admin-readiness-endpoint.md` (decisions; alternatives: a component kind — rejected, rule 7
  and process-level nature; `/metrics` — restated as rejected per the earlier ADR; readiness via
  a file/`sd_notify` — deferred, an HTTP probe is what every orchestrator speaks) and ADR
  `tracing-for-self-logging.md` (decisions; alternatives: the `log` crate — rejected, no
  structured fields; keep `eprintln!` + JSON by hand — rejected, no filtering; `tracing`
  spans for pipeline stages — rejected, `internal` spans already exist and are the pipeline's
  own tracing).
- `docs/deploying.md`: new "Probes and exit codes" section (the code table, `admin:` example,
  Kubernetes `readinessProbe`/`livenessProbe` snippet, `HEALTHCHECK`), new "Self-logging"
  section (`--log-level`, `--log-format json`, the lifecycle event names as stable strings, and
  that `internal` can carry the same lines into any sink).
- `docs/design/internal-telemetry.md`: "Logs" section beside "Spans"; "What this is not"
  amended (the readiness endpoint is not a scrape endpoint; the `tracing` migration has landed
  as a producer); catalog rows for `logit.internal.logs.*`.
- `docs/known-gaps.md`: `eprintln!` entry and "Internal logs" entry closed; new entries: admin
  server has no TLS/auth (loopback by design), readiness is per-process not per-sink (a
  degraded sink doesn't flip readiness — deliberate, it's what `buffer:` is for).
- `AGENTS.md` current state; `demo/compose.yaml` healthcheck + `service_healthy` flips;
  `demo/logit.yaml` `admin:` block; `docs/plans/README.md` row.

## Verification, across the whole plan

- `script/cibuild` clean; `script/audit` clean or the ADR names every new crate and license.
- `script/demo`: `docker compose ps` shows `logit` `healthy`; `app`/`worker`/`haproxy` start only
  after; `docker compose logs logit` shows `starting`/`bound ×5`/`ready` as JSON when
  `--log-format json` is set in the compose command; Grafana's Loki explore shows `service.name=
  logit` warn lines after sending a malformed line to `:5142`.
- Exit codes by hand: bad token to InfluxDB with `retry_budget: 5s` → exits 2 after ~60 s; port
  already in use → exits 1 immediately; `kill -TERM` → 0; `kill -TERM` twice → 130.

## Explicitly out of scope (file in `known-gaps.md`)

TLS/auth on the admin endpoint; a `/metrics` scrape path (still rejected); config hot reload on
SIGHUP (separate design: needs graph diffing); `logit stats` CLI reading the `Registry`
out-of-process; per-sink readiness; log sampling/rate limiting beyond the existing occurrence
throttle; `tracing` spans for pipeline stages.
