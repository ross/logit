# logit

`logit` is a single binary that receives logs, metrics, and traces over many protocols, transforms
them with built-in components and LuaJIT scripts, and sends them to many destinations. The same
binary runs as a sidecar, a host agent, or a central aggregator; the config file decides which.

Use it when you want one lightweight process to parse, enrich, aggregate, sample, and route
telemetry, with real scripting instead of a configuration DSL. Relaying a protocol to itself (for
example, `statsd_in` to `statsd_out`) is lossless by design. For scope and positioning against
Vector, the OpenTelemetry Collector, Fluent Bit, and Telegraf, see
[docs/OVERVIEW.md](docs/OVERVIEW.md).

`logit` is pre-release. Config, wire formats, and behavior can change without a compatibility path.

## Try the demo

The demo runs `logit` against a small web stack and sends its logs, metrics, and traces to Loki,
VictoriaMetrics, and Tempo, with Grafana dashboards already provisioned. It needs only Docker with
the Compose plugin.

```sh
cd demo
docker compose up --build
```

The first run builds the images from source and takes several minutes. When it's up, open
http://localhost:8080. That page generates traffic, links to Grafana, and shows the demo's own
pipeline graph. The demo takes shortcuts that don't belong in production, such as hardcoded
secrets and anonymous Grafana admin access. See [demo/README.md](demo/README.md) for what flows
where.

## Run logit

Published images are at `ghcr.io/ross/logit`. Only the `latest` tag exists, it's amd64 only, and
it moves whenever a maintainer publishes a new build, so don't treat it as a pinned version.

1. Save this config as `logit.yaml`. It reads statsd metrics, sums them over 10-second windows,
   adds an `env` tag in Lua, and prints the result:

   ```yaml
   components:
     statsd_in:
       type: statsd_in
       bind: 0.0.0.0:8125
     windowed:
       type: aggregate
       sources: [statsd_in]
       interval: 10s
     enrich:
       type: lua
       sources: [windowed]
       script: |
         function process(event)
           event.attributes.env = event.attributes.env or "dev"
           return event
         end
     out:
       type: stdio_out
       sources: [enrich]
   ```

2. Check the config. The container runs as a non-root user, so the file must be world-readable.
   On an SELinux host such as Fedora or RHEL, add `,z` after `:ro` in each `-v` option below, or
   the container gets `Permission denied` reading the file.

   ```sh
   docker run --rm -v "$PWD/logit.yaml:/config.yaml:ro" ghcr.io/ross/logit:latest validate /config.yaml
   ```

3. Run it. Publish every port your listeners bind, with the right protocol, because Docker
   doesn't publish anything by default. statsd uses UDP:

   ```sh
   docker run --rm -p 8125:8125/udp -v "$PWD/logit.yaml:/config.yaml:ro" \
     ghcr.io/ross/logit:latest run /config.yaml
   ```

4. From another terminal, send a metric. Within 10 seconds, `logit` prints the aggregated event:

   ```sh
   echo "requests:1|c" | nc -u -w1 localhost 8125
   ```

To keep secrets out of config files, write `token: !env INFLUXDB_TOKEN` and pass the variable with
`docker run -e`. Any field on any component accepts `!env`.

For production use, including health probes, exit codes, buffering, TLS, and forwarding between
`logit` nodes, see [docs/deploying.md](docs/deploying.md).

## Components

A config is a flat graph of named components. Each component has a `type` and lists the
components it reads from in `sources`. `logit graph <config>` prints the resolved graph in Graphviz
DOT format. [examples/](examples) has a runnable config for most components.

| Role | Types |
|---|---|
| Inputs | `statsd_in` (statsd and DogStatsD), `syslog_in`, `otlp_in`, `prometheus_in` (scrape or remote-write), `datadog_in` (Datadog's intake API), `datadog_trace_in` (a Datadog Agent's APM API), `splunk_hec_in` (Splunk's HTTP Event Collector), `collectd_in`, `graphite_in`, `tail_in`, `docker_in`, `logit_in`, `internal` (`logit`'s own telemetry), `generate_in` |
| Parsers | `json`, `csv`, `logfmt`, `kv`, `regex`, `http_access` |
| Reshaping | `set`, `remove`, `keep`, `keep_values`, `flatten`, `scale`, `kv_metrics`, `trace_context` |
| Filtering and sampling | `has_signal`, `keep_signals`, `drop_signals`, `has_attributes`, `drop_attributes`, `has_provenance`, `drop_provenance`, `sample` |
| Aggregation and routing | `aggregate`, `route`, `target` |
| Scripting | `lua`, `lua_file` |
| Observation | `shape` |
| Outputs | `influxdb_out`, `otlp_out`, `prometheus_out` (exposition or remote-write), `datadog_out` (Datadog's intake API), `datadog_trace_out` (a Datadog Agent's APM API), `splunk_hec_out` (Splunk's HTTP Event Collector), `statsd_out`, `syslog_out`, `collectd_out`, `graphite_out`, `logit_out`, `stdio_out`, `file_out`, `null_out` |

`logit_in` and `logit_out` speak `logit`'s own wire protocol, for forwarding between `logit` nodes.
VictoriaMetrics, VictoriaLogs, and VictoriaTraces need no component of their own; see
[docs/deploying.md](docs/deploying.md#victoriametrics-victorialogs-and-victoriatraces) for which
standard one reaches each.
The editor-ready JSON Schema for the config is [schema/logit.schema.json](schema/logit.schema.json).

## Documentation

- [docs/OVERVIEW.md](docs/OVERVIEW.md): what `logit` is for, and what it isn't.
- [docs/deploying.md](docs/deploying.md): running `logit` in production.
- [docs/datadog.md](docs/datadog.md): sending to Datadog, and standing in for a Datadog Agent or
  Datadog's intake.
- [docs/splunk.md](docs/splunk.md): sending to Splunk over HEC, and standing in for Splunk's HTTP
  Event Collector.
- [docs/http-access-logs.md](docs/http-access-logs.md): the access-log schema for nginx, HAProxy,
  and other web servers.
- [docs/design/](docs/design): the event model, Lua API, pipeline graph, wire protocol, internal
  telemetry, memory, and performance.
- [docs/adr/](docs/adr): architecture decision records, one per decision.
- [docs/known-gaps.md](docs/known-gaps.md): known limitations. Check it before reporting a bug.

## Development

Everything builds and runs in a container, so you don't need Rust or LuaJIT on the host. You need
Docker or Podman.

1. Build the dev image and start the local test stack:

   ```sh
   script/setup
   ```

2. Run the formatter check, linter, and tests:

   ```sh
   script/check
   ```

The scripts run `sudo docker` by default. If your account is in the `docker` group, run them with
`DOCKER=docker`. For rootless Podman, use `DOCKER=podman`. See
[ADR `containerized-development`](docs/adr/containerized-development.md).

| Command | What it does |
|---|---|
| `script/bootstrap` | Build the dev container image. |
| `script/setup` | Set up a fresh checkout: bootstrap, then start the local test stack. |
| `script/update` | Rebuild the dev image and refresh the test stack after you pull changes. |
| `script/check [test args]` | Check formatting, lint, and run the workspace tests in one container. |
| `script/test [args]` | Run `cargo nextest run --workspace`. |
| `script/lint` | Run `cargo clippy` with warnings denied. |
| `script/format [--check]` | Run `cargo fmt`. |
| `script/schema` | Regenerate `schema/logit.schema.json` from the config types. |
| `script/validate` | Run `logit validate` on every shipped config. |
| `script/audit` | Run supply-chain checks (`cargo-deny`, `cargo-audit`). |
| `script/cibuild` | Run the full CI sequence. |
| `script/console` | Open a shell in the dev container. |
| `script/server [config]` | Start the local test stack and run `logit` against a config. |
| `script/image [tag]` | Build the production image. |
| `script/demo [compose args]` | Run the demo stack. |
| `script/bench [filter]` | Run throughput and allocation benchmarks. |
| `script/perf <command>` | Run the load-test harness ([docs/design/performance.md](docs/design/performance.md)). |

`make <name>` is an alias for most of these. [AGENTS.md](AGENTS.md) lists the remaining
specialized scripts.

Cargo downloads and build artifacts live in the shared `logit_cargo_home` and `logit_target_cache`
Docker volumes, so a new git worktree starts with warm dependencies. Don't remove these volumes
during routine cleanup.

### Local test stack

`script/setup` starts InfluxDB and Grafana. `script/server` also starts nginx.

| Service | Address | Notes |
|---|---|---|
| InfluxDB 2.x | http://localhost:8086 | Org `logit`, bucket `metrics`, with a dev token. |
| Grafana | http://localhost:3000 | Anonymous admin access, with InfluxDB already provisioned as a datasource. |
| nginx | http://localhost:8080 | Sends access logs to `logit` over syslog, for [examples/nginx-to-influxdb.yaml](examples/nginx-to-influxdb.yaml). |

`script/server` runs [examples/statsd-to-influxdb.yaml](examples/statsd-to-influxdb.yaml) unless
you pass another config.

## Contributing

Work on a branch and open a pull request; nobody commits to `main` directly. Run `script/cibuild`
before you open the pull request, because it runs the same sequence as CI. A significant design
decision gets an ADR in [docs/adr/](docs/adr), copied from
[docs/adr/TEMPLATE.md](docs/adr/TEMPLATE.md). [AGENTS.md](AGENTS.md) has the branch and PR naming
conventions and the constraints that tests enforce, such as exact allocation counts.

## Repo layout

```text
crates/
  logit-core        event model: Event, Value, Resource, metric kinds, interner, self-telemetry
  logit-config      YAML config types and the generated JSON Schema
  logit-script      LuaJIT embedding and the Event proxy
  logit-proto       codecs and the native wire format
  logit-pipeline    Input/Output/Transform/Router traits, graph validation, node runtime
  logit-inputs      input components
  logit-outputs     output components
  logit-transforms  built-in transforms and the route router
  logit-cli         the `logit` binary
  logit-bench       dev only: allocation-count tests and throughput benchmarks
  logit-perf        dev only: the load-test harness
demo/               self-contained demo stack
examples/           example configs, also used by the local test stack
docs/               overview, deployment guide, ADRs, design docs, and plans
perf/               load-test scenarios; results are gitignored
schema/             generated JSON Schema for the config
tools/              data-shape survey, unsafe-code checks, fixture recording
```

## License

[MIT](LICENSE)
