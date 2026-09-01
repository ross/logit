# `logit` demo stack

A self-contained stack for trying `logit`: no Rust toolchain, no `script/*`, no knowledge of the
rest of this repo. See [docs/plans/0003-demo-stack.md](../docs/plans/0003-demo-stack.md) for why it
exists and what's next.

## Quick start

```sh
cd demo
docker compose up --build
```

The first run builds `logit`'s production image from source (vendored LuaJIT, no dependency-layer
caching by design — see `../Dockerfile`'s comment) — expect several minutes before anything
appears. Every run after that reuses the built image.

Once it's up:

| Service | URL | What it's for |
|---|---|---|
| Grafana | http://localhost:3000 | Anonymous admin access. Open the "logit" folder for the pre-built dashboard. |
| InfluxDB | http://localhost:8086 | `logit`/`logit-demo-password`. Bucket `metrics`, org `logit`. |
| Loki | http://localhost:3100 | Provisioned as a Grafana datasource; see below for what's actually in it. |
| Tempo | http://localhost:3200 (query), :4317/:4318 (OTLP) | Provisioned as a Grafana datasource; empty. |

`docker compose logs -f logit` shows every decoded event as a `stdio_out` block — the fastest way
to see the pipeline doing something.

## What's actually flowing

A `writer` service emits a few synthetic access-log lines a second — the same RFC 3164 + JSON-body
shape `../crates/logit-bench/src/fixtures.rs` measures — to `logit`'s `syslog_in` listener.
[`logit.yaml`](logit.yaml) runs it through `json` → `kv_metrics` → `keep` → `aggregate` →
`influxdb_out`, plus `logit` observing its own pipeline via `internal`
(`../docs/design/internal-telemetry.md`) into the same InfluxDB bucket. **Metrics are the one
signal that works end to end today** — that's what the shipped Grafana dashboard shows.

## What isn't wired yet

Loki and Tempo are up, healthy, and provisioned as Grafana datasources — but `logit` can't write
to either of them yet:

- **Logs → Loki** need a `syslog_out` output, which doesn't exist (not implemented, not even a
  declared config kind). As a stand-in, the `writer` service also sends its lines straight to
  `alloy`, which forwards them into Loki (`alloy/config.alloy`) — so the Loki datasource isn't
  simply empty, but that's `alloy` filling in for `logit`, not `logit` actually emitting logs. Once
  `syslog_out` lands, `logit.yaml`'s commented-out `log_out` component points straight at `alloy`'s
  listener, and that direct `writer` → `alloy` write goes away.
- **Traces → Tempo** need both an `otlp_out` output (declared in config, rejected at validation
  today — no OTLP code exists) and something that actually produces spans (`bench/internal-spans-
  costing`, PR #39, measured the cost of carrying trace context through the pipeline and reverted
  the prototype — nothing emits a span anywhere yet). Tempo's OTLP receiver is up and will accept
  data the moment both exist; `logit.yaml`'s commented-out `trace_out` is the shape it'll take.

## Stopping

```sh
docker compose down        # stop, keep data
docker compose down -v     # stop, wipe all volumes (InfluxDB/Grafana/Loki/Tempo/Alloy state)
```
