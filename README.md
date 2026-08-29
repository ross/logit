# logit

A logging, metrics, and tracing multiplexer: ingest over many protocols, transform with
user-defined Lua and built-in parsers, emit to many destinations. Runs as a sidecar or a host
agent — same binary, different config. See [docs/OVERVIEW.md](docs/OVERVIEW.md) for the full
scope, [docs/adr/](docs/adr) for why the stack is what it is, and [docs/design/](docs/design) for
the internal event model, the Lua scripting API, and the native wire protocol.

**Status:** early design/scaffolding. The workspace compiles and the config schema pipeline works
end to end; the actual pipeline (inputs → transforms → outputs) is not implemented yet. The v0.1
target is statsd in, one Lua enrichment stage, InfluxDB 2.x out.

## Development

Everything runs in a container — no Rust or LuaJIT toolchain needs to be installed on the host.

```sh
make build       # cargo build --workspace
make test        # cargo nextest run --workspace
make lint        # cargo clippy, warnings denied
make fmt          # cargo fmt
make schema       # regenerate schema/logit.schema.json
make shell        # interactive shell in the dev container
make up           # start InfluxDB 2.x + Grafana for local testing
make down         # stop them
```

By default these use `sudo docker`. Override with `make DOCKER=docker ...` if your account is in
the `docker` group (`sudo usermod -aG docker $USER`, then re-login removes the need for `sudo`
entirely), or `make DOCKER=podman ...` to use rootless Podman — both are drop-in compatible with
the plain `Dockerfile.dev`/`compose.yaml` here. See
[ADR 0005](docs/adr/0005-containerized-development.md) for why and how.

## Local test stack

`make up` starts InfluxDB 2.x (seeded with an `logit`/`metrics` org/bucket and a dev token) and
Grafana (anonymous admin access, with the InfluxDB datasource pre-provisioned) at
`localhost:8086` and `localhost:3000`. [examples/statsd-to-influxdb.yaml](examples/statsd-to-influxdb.yaml)
is the config the v0.1 slice targets against this stack.

## Repo layout

```
crates/
  logit-core      internal event model: Event, Value, Resource, metric kinds, interner
  logit-config    YAML config types + generated JSON Schema
  logit-script    LuaJIT embedding (mlua), the Event proxy
  logit-proto     codec traits, native wire format, output buffering
  logit-inputs    the Input trait; statsd
  logit-outputs   the Output trait; InfluxDB
  logit-cli       the `logit` binary
docs/
  OVERVIEW.md     project scope, ~1 page
  adr/            architecture decision records
  design/         the event model, Lua API, and wire protocol design docs
```
