# victoria-interop

`script/victoria-interop` checks `logit` against real VictoriaMetrics, VictoriaLogs,
VictoriaTraces, and vmagent. It runs the ten legs in
[`docs/plans/victoriametrics-interop.md`](../../docs/plans/victoriametrics-interop.md)'s "W1's
legs", queries each backend for what arrived, and prints one row per leg. The plan's "Findings"
section records what a run showed.

The script runs on the host and drives docker (`$DOCKER`, `sudo docker` by default), like
`script/shape-survey` and `script/record-fixtures`. It isn't part of `script/cibuild`, and no test
depends on it running.

## What it runs

`compose.yaml` starts one stack under the compose project `victoria-interop`, on the network
`victoria-interop-net`, with no host ports:

| Service | Image | Role |
|---|---|---|
| `victoria-metrics` | `victoriametrics/victoria-metrics:v1.152.0` | `-graphiteListenAddr=:2003`; OTLP over HTTP is on by default |
| `victoria-logs` | `victoriametrics/victoria-logs:v1.52.0` | `-syslog.listenAddr.tcp=:514` |
| `victoria-traces` | `victoriametrics/victoria-traces:v0.11.1` | `-otlpGRPCListenAddr=:4317`, `-otlpGRPC.tls=false` |
| `vmagent` | `victoriametrics/vmagent:v1.152.0` | scrapes `logit-expose` (`vmagent-scrape.yaml`), remote-writes to VictoriaMetrics and to `logit-vmagent-in` |
| `logit-<leg>` | `logit:victoria-interop`, built from the current tree | one per `logit-<leg>.yaml` |

Each `logit-<leg>.yaml` says at its top which leg it is. The legs that need synthetic traffic use
`generate_in`, and the OTLP legs build their metrics, logs, and spans with a `lua` stage's
`Event.new`. Every config passes `logit validate`: `script/validate` and the
`every_shipped_config_loads_and_validates` test both cover `logit-*.yaml` here.

## A run

1. Builds `logit:victoria-interop` from `Dockerfile` (set `VICTORIA_INTEROP_SKIP_IMAGE=1` to reuse
   it) and validates every leg config with it.
2. Brings the stack up with `docker compose up --wait`, which waits on each `logit` service's
   `logit ready` health check.
3. Lets traffic flow for `VICTORIA_INTEROP_WINDOW` seconds (default 45).
4. Copies every service's log into the run directory and runs `check.py` in a
   `python:3.12-slim` container on the stack's network. `check.py` queries VictoriaMetrics
   (`/api/v1/series`, `/api/v1/export`), VictoriaLogs (`/select/logsql/query`), and
   VictoriaTraces (`/select/jaeger/api/traces`), and reads the files the `federate` and
   `vmagent-in` legs write. It also replays committed captures from
   `testdata/interop/prometheus/` at VictoriaMetrics to record how it answers a remote-write 2.0
   request and each zstd header variant.
5. Tears the project down (`down -v --remove-orphans`) on every exit.

Each leg's row is `PASS` (the leg did what the plan expects), `GAP` (the backend or `logit` behaves
in a way the plan records as a gap), or `FAIL`. The script exits 1 on any `FAIL`.

A run writes `perf/results/victoria-interop/<timestamp>/` (gitignored, or under
`VICTORIA_INTEROP_OUT`): `results.md` and `results.json`, `provenance.txt` with the image tags,
`logs/<service>.log`, and the `logit` file outputs.

## Cleanup and a shared daemon

Everything the script creates belongs to the `victoria-interop` project, so cleanup can't touch
another session's containers. The project name is fixed, so one run at a time per daemon: the
script refuses to start while the project has containers, and prints the `down` command for a
stack a crashed run left behind.
