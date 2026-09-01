# Enabling plan: a user-facing demo stack

`logit`'s examples are developer scratch material — `examples/statsd-to-influxdb.yaml`,
`examples/nginx-to-influxdb.yaml`, each landed alongside the feature it exercises, all of them
pointed at the *dev* stack (`compose.yaml`, `script/server`), whose entrypoint execs `cargo run`
inside a bind-mounted contributor container. None of that is something to hand a stranger.

This plan builds a self-contained `demo/` directory: `docker compose up`, watch telemetry move
through `logit` and land in Grafana. No Rust toolchain, no `script/*`, no knowledge of this repo.
It is also the forcing function for what's missing, the same way
[0002-nginx-integration.md](0002-nginx-integration.md) used a real nginx to schedule `syslog_in`,
`stdio_out`, and `kv_metrics`: standing Loki and Tempo up now, provisioned and empty, gives the
eventual `syslog_out` and `otlp_out` work somewhere to land on day one.

## The target, generically

- Someone with no Rust toolchain and no familiarity with this repo, running `docker compose up` in
  a directory they cloned, sees `logit` ingest synthetic traffic, transform it, and land it in a
  working Grafana dashboard within a few minutes.
- The three signals `logit` is meant to carry — logs, metrics, traces — are all representable in
  the stack even where `logit` itself can't produce them yet, so the demo doubles as a checklist
  of what's left.

## Decisions already settled

| Question | Decision |
|---|---|
| Where the demo lives | A new, self-contained `demo/` directory with its own `compose.yaml`. Root `compose.yaml` (the contributor dev stack) is untouched — see [ADR 0020](../adr/0020-demo-stack-separate-from-dev-stack.md). |
| What runs `logit` | The production image (`Dockerfile`), built by compose — no published image exists yet ([docs/deploying.md](../deploying.md)). |
| Data source | A trivial writer container looping synthetic syslog datagrams. No nginx in the demo — `examples/nginx/` stays a dev-stack fixture. |
| Log line shape | The same RFC 3164 + JSON-body shape `crates/logit-bench/src/fixtures.rs`'s `NGINX_SYSLOG_LINE` already measures. |
| Log backend | Loki, up and provisioned — no data through `logit` until `syslog_out` exists. |
| Trace backend | Tempo, up and provisioned — no data until `otlp_out` and a span producer both exist. |
| syslog → Loki shim | Grafana Alloy (`loki.source.syslog`, confirmed to accept UDP and RFC 3164). Loki has no syslog receiver of its own; promtail is EOL. |
| This plan writes no Rust | No new `ComponentKind`, no new sink, no span emission. The log and trace legs ship as commented-out config plus a documented pointer to what has to be built. |

## Gaps this plan exists to schedule

| Gap | Consequence |
|---|---|
| No `syslog_out` | Not implemented, not even a declared `ComponentKind` — anticipated as precedent in `docs/design/pipeline-graph.md`'s naming rationale, never built. Logs can't leave `logit` at all. |
| `otlp_out` rejected at validation | Declared in `logit-config`, but `graph::is_implemented` rejects it. No OTLP code, no wire protocol chosen (ADR 0004 leaves gRPC-vs-HTTP open). |
| No span producer | `bench/internal-spans-costing` (PR #39, draft) added a `TraceContext` prototype to `Fanout`'s `Delivered`, measured its cost, and reverted it in full — only the size-guard test and `docs/design/memory.md`'s "Costing internal spans" section remain. Nothing emits a span anywhere. |
| `examples/` doubles as both dev fixtures and the onboarding story | Someone trying `logit` for the first time hits `script/server`'s dev-container dependency before seeing anything work. |
| No config-drift guard | A component field rename can silently break every shipped example; nothing runs `logit validate` over them. |

## Reference topology

```
writer --syslog/UDP--> access_in (syslog_in)
                            |
                            v
                        access_json (json)     <- merges the JSON body into attributes
                           / \
                          v   v
                 tap (stdio_out)   access_metrics (kv_metrics) <- adds metrics to the event
                                        |
                                        v
                                  trimmed (keep)   <- drops high-cardinality attrs
                                        |
                                        v
                                  windowed (aggregate)
                                        |
self (internal) --> self_windowed (aggregate) ------+
                                                      v
                                                influx_out --> InfluxDB --> Grafana

writer --syslog/UDP--> alloy (loki.source.syslog) --> Loki --> Grafana   [scaffolding, see below]

                                                        Tempo --> Grafana   [empty, provisioned]
```

## Workstream dependency graph

A (stack) → B (pipeline config) → C (writer), D (Grafana provisioning) → E (reset `examples/`) → F (guard + docs)

## A. The demo stack

**Goal:** `demo/compose.yaml` brings up InfluxDB, Grafana, Loki, Tempo, and Alloy, all healthy,
with `logit` built from the release `Dockerfile`.

**Files:** `demo/compose.yaml`, `demo/loki/loki.yaml`, `demo/tempo/tempo.yaml`,
`demo/alloy/config.alloy`.

**Notes from getting this working:**

- Loki and Tempo's images are distroless and run as a fixed non-root UID — their data directories
  are named volumes, not bind mounts (config files are the only bind mounts, `:z,ro`). This is the
  same class of fix as root `compose.yaml`'s existing SELinux `:z` comment, one step further.
- Tempo is pinned to `2.10.8`, not the current `3.x` line — 3.0 removed the legacy ingester write
  path and reworked config wholesale. 2.10's monolithic mode is the well-trodden shape.
- Loki's healthcheck is the native `loki -health` probe (backported to 3.6.5+), not the classic
  `wget --spider` form most examples use — there is no shell in the image to run `wget` with.
- The one healthcheck that matters for the demo's first impression is `influxdb`'s, gated in front
  of `logit` — `influxdb_out` retries transient failures (ADR 0013) regardless, but a demo spending
  its first several seconds visibly retrying reads as broken.

**Done when:** `docker compose up --build` in `demo/` brings up every service healthy with no
manual steps.

## B. The pipeline config

**Goal:** `demo/logit.yaml` runs every implemented component kind against the writer's traffic,
visibly, plus `logit` observing itself.

**Files:** `demo/logit.yaml` — `syslog_in → json → {stdio_out, kv_metrics → keep → aggregate} →
influxdb_out`, plus `internal → aggregate → influxdb_out` (ADR 0018). The commented-out `log_out`
(`syslog_out`, pending) and `trace_out` (`otlp_out`, pending) live here too, each with a one-line
pointer to what has to land first — an uncommented reference to either fails `logit validate`
today, so they can't ship live by accident.

**Done when:** `docker compose logs -f logit` shows a `stdio_out` block per synthetic line, and
Grafana's InfluxDB datasource returns `web.*` series tagged exactly `host`/`request_method`/
`status` — no `syslog.*` or `request_id` attribute, which is what `keep`-before-`aggregate` exists
to prevent.

## C. The traffic writer

**Goal:** synthetic access-log traffic, varied enough that `web.request_time`'s distribution and
the `keep` tag set are non-degenerate, with no custom image to build or maintain.

**Files:** `demo/compose.yaml`'s `writer` service — `debian:13-slim` (bash built with
`--enable-net-redirections`, unlike some Alpine builds) plus an inline loop writing to
`/dev/udp/logit/5140`. No `logger`: busybox's applet can't send to a remote host at all (`-n`/`-P`
are util-linux flags it doesn't have), and installing util-linux at container start makes every
`docker compose up` depend on network access, which breaks the "survives restarts" bar.

Also writes the same line to `alloy:5141` — temporary scaffolding so the Loki side of the demo has
data before `syslog_out` exists (see workstream D and `demo/README.md`). Delete that second write
the day `syslog_out` lands and `log_out` in `demo/logit.yaml` is real.

UDP is fire-and-forget, so a writer starting before `logit`'s listener binds just loses those
lines silently — `depends_on: service_started` is the honest limit; there's no way to know a UDP
socket is actually bound short of a manual probe (`docs/deploying.md`'s "ordering rule").

**Done when:** the writer survives `docker compose down && up` repeatedly with no image builds and
no network access beyond talking to `logit`/`alloy`.

## D. Grafana provisioning

**Goal:** InfluxDB, Loki, and Tempo all provisioned as datasources with no manual clicking, plus
one dashboard built on `logit`'s own internal metrics — the part that demonstrates the product,
not the backends.

**Files:** `demo/grafana/provisioning/datasources/datasources.yaml` (InfluxDB, matching
`deploy/grafana/provisioning/datasources/influxdb.yaml`'s shape; Loki with a `derivedFields` link
to Tempo; Tempo with `tracesToLogsV2` back to Loki — correlation that costs nothing to declare now
and works the day either sink actually has data), `demo/grafana/provisioning/dashboards/
dashboards.yaml` (the file provider), `demo/grafana/dashboards/logit-internal.json` (panels over
`logit.component.events.{sent,received,dropped}`, `.process.duration`, `logit.output.requests
{class}`, `logit.process.uptime`/`.interner.strings` — names and tags from
[docs/design/internal-telemetry.md](../design/internal-telemetry.md) — plus the demo's own
`web.requests`/`web.request_time` to show the pipeline working end to end).

The syslog → Loki path itself is `demo/alloy/config.alloy`: Alloy's `loki.source.syslog` (promtail
is EOL) listening on UDP with `syslog_format = "rfc3164"` (matching `syslog_in`'s own format),
promoting the `__syslog_*` labels Alloy strips by default (`host`, `app`) so Loki always has a
stream label, and `loki.write` to Loki's push API.

**Done when:** all three datasources show healthy in Grafana, the dashboard renders real series,
and a log line written by the writer is queryable in Loki via Grafana Explore.

## E. Reset `examples/`

**Goal:** `demo/` becomes the front door; `examples/` stops trying to double as one.

**Disposition of each file:**

| File | Disposition |
|---|---|
| `examples/statsd-to-influxdb.yaml` | Keep — `script/server`'s default config, the v0.1 slice. |
| `examples/internal-telemetry.yaml` | Keep — the canonical `internal → aggregate → sink` shape ADR 0018 references. |
| `examples/syslog-with-telemetry.yaml` | Removed — superseded by `demo/logit.yaml`, which does strictly more. |
| `examples/nginx-to-influxdb.yaml`, `examples/nginx/**` | Keep, marked as a dev-stack fixture, not a starting point — root `compose.yaml`'s `nginx` service and `crates/logit-bench/src/fixtures.rs`'s `NGINX_SYSLOG_LINE` provenance both depend on it staying real and runnable. |

**Files:** `README.md` (a "Try it" section pointing at `demo/`; the status paragraph re-pointed),
`AGENTS.md` ("Current state"), `docs/deploying.md` (a pointer at the top), header comments on
`examples/nginx-to-influxdb.yaml`.

**Deliberately not doing:** deleting `examples/nginx/`. That would mean editing root
`compose.yaml` and `script/server` too (out of scope — the dev stack stays as it is) and would
strand `fixtures.rs`'s claim that its benchmark workload is "the repo's own reference example, not
a synthetic shape." If `examples/nginx/` should go too, it's a clean follow-up once the demo's
writer has proven out the same line shape: `fixtures.rs` needs only its comments rewritten to be
self-contained, no re-measurement, no allocation-count churn.

**Done when:** nothing in the repo points at `examples/syslog-with-telemetry.yaml`, and every
remaining doc cross-reference resolves.

## F. Guard against rot, and document

**Goal:** a config drifting out of sync with the types it's validated against fails loudly, in CI,
not silently in someone's demo.

**Files:**

- `script/validate` — `logit validate` over `demo/logit.yaml` and every `examples/*.yaml`; wired
  into `script/cibuild` (and therefore CI) right after `script/test`.
- `script/demo` — thin wrapper (`${DOCKER} compose -f demo/compose.yaml "$@"`, default `up
  --build`), for `$DOCKER`/podman/sudo parity with every other `script/*` entrypoint (ADR 0006).
  `demo/README.md`'s headline instruction stays plain `docker compose up` — `script/demo` is a
  contributor convenience, not the primary path.
- `demo/README.md` — quick start, the URL table, the first-build-is-slow warning (a full release
  build, no dependency-layer caching by design — `Dockerfile`'s own comment), and an explicit
  "what isn't wired yet" section naming `syslog_out` and `otlp_out`.
- This plan, committed as `docs/plans/0003-demo-stack.md`.

**Done when:** `script/cibuild` fails if a shipped config stops validating, and someone unfamiliar
with the repo can follow `README.md` alone to a working Grafana dashboard.

---

## Verification, across the whole plan

- `cd demo && docker compose up --build` (or `script/demo`) — every service healthy, no manual
  steps.
- `docker compose logs -f logit` — a `stdio_out` block per synthetic line, JSON fields merged into
  attributes.
- Grafana at `localhost:3000`: the shipped dashboard populates; a Flux query against bucket
  `metrics` returns `web.requests`/`web.request_time` tagged exactly `host`/`request_method`/
  `status`.
- Loki and Tempo show healthy in Grafana's datasource check; Loki actually has lines (via the
  Alloy shim); Tempo is empty but reachable.
- `docker compose down && up` twice over — the writer keeps writing, no volume permission errors
  from Loki's or Tempo's data directories.
- `script/cibuild` passes, including `script/validate`.
- `script/server` and `make up` still work unchanged against the dev stack.
