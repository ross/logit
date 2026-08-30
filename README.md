# logit

A logging, metrics, and tracing multiplexer: ingest over many protocols, transform with
user-defined Lua and built-in parsers, emit to many destinations. Runs as a sidecar or a host
agent — same binary, different config. See [docs/OVERVIEW.md](docs/OVERVIEW.md) for the full
scope, [docs/adr/](docs/adr) for why the stack is what it is, [docs/design/](docs/design) for
the internal event model, the Lua scripting API, the pipeline component graph, and the native wire
protocol, and [docs/known-gaps.md](docs/known-gaps.md) for already-identified rough edges in what's
built so far.

**Status:** v0.1 is complete — statsd in, a 10s `aggregate` window, a Lua enrichment stage, InfluxDB
2.x out, via `logit run <config>`. Config is a flat graph of named components, each declaring its
own `sources` ([ADR 0009](docs/adr/0009-component-graph-configuration.md),
[docs/design/pipeline-graph.md](docs/design/pipeline-graph.md)) — `logit graph <config>` prints the
resolved graph as graphviz DOT. [examples/statsd-to-influxdb.yaml](examples/statsd-to-influxdb.yaml)
is a working example; `script/server` runs it against the local test stack below. `aggregate` is the
only built-in transform implemented so far — `logit run` rejects a config referencing any other
unimplemented kind with a clear error; see [ADR 0008](docs/adr/0008-aggregation-window-semantics.md)
for its windowing semantics. Any field on any component can pull its value from the environment
with `!env VAR_NAME` (e.g. `token: !env INFLUXDB_TOKEN`) — see
[ADR 0011](docs/adr/0011-env-yaml-tag.md).

## Development

Everything runs in a container — no Rust or LuaJIT toolchain needs to be installed on the host.
Common tasks are `script/*` commands
([the "Scripts to Rule Them All" pattern](https://github.blog/engineering/scripts-to-rule-them-all/) —
see [ADR 0006](docs/adr/0006-scripts-to-rule-them-all.md)); `make <name>` is a thin alias for anyone
who reaches for `make` out of habit.

| Command | What it does |
|---|---|
| `script/bootstrap` | Build the dev container image. Run this first. |
| `script/setup` | One-time setup for a fresh checkout: bootstrap + start the local test stack. |
| `script/update` | Run after pulling changes: rebuild the dev image, refresh the test stack. |
| `script/test` | `cargo nextest run --workspace` |
| `script/lint` | `cargo clippy`, warnings denied |
| `script/format [--check]` | `cargo fmt` |
| `script/schema` | Regenerate `schema/logit.schema.json` from the config types |
| `script/audit` | Supply-chain checks (`cargo-deny`, `cargo-audit`) |
| `script/cibuild` | The full check sequence CI runs — the real preflight check |
| `script/console` | Interactive shell in the dev container |
| `script/server [config]` | Start the test stack and run `logit` against a config file |

All of these use `sudo docker` by default. Override with `DOCKER=docker script/...` if your
account is in the `docker` group (`sudo usermod -aG docker $USER`, then re-login removes the need
for `sudo` entirely), or `DOCKER=podman script/...` for rootless Podman — both are drop-in
compatible with the plain `Dockerfile.dev`/`compose.yaml` here. See
[ADR 0005](docs/adr/0005-containerized-development.md) for why and how.

## Local test stack

`script/setup` (or `make up`) starts InfluxDB 2.x (seeded with a `logit`/`metrics` org/bucket and
a dev token) and Grafana (anonymous admin access, with the InfluxDB datasource pre-provisioned) at
`localhost:8086` and `localhost:3000`. [examples/statsd-to-influxdb.yaml](examples/statsd-to-influxdb.yaml)
is the config the v0.1 slice targets against this stack.

## Contributing

Work happens on branches, landed via pull request — nothing is pushed straight to `main`.
`script/cibuild` is what CI runs; it's the thing to run locally before opening a PR. See
[AGENTS.md](AGENTS.md) if you're an AI coding agent working in this repo.

## Repo layout

```
crates/
  logit-core        internal event model: Event, Value, Resource, metric kinds, interner
  logit-config      YAML config types + generated JSON Schema
  logit-script      LuaJIT embedding (mlua), the Event proxy
  logit-proto       codec traits, native wire format, output buffering
  logit-pipeline    Input/Output/Transform traits, Fanout, graph resolution, the node runtime
  logit-inputs      per-protocol listeners; statsd
  logit-outputs     per-protocol sinks; InfluxDB
  logit-transforms  built-in native transform components; aggregate, json
  logit-cli         the `logit` binary
docs/
  OVERVIEW.md       project scope, ~1 page
  adr/              architecture decision records
  design/           the event model, Lua API, pipeline component graph, and wire protocol design docs
  plans/            staged implementation plans for larger, multi-session pieces of work
```
